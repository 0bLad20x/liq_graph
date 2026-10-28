//! Post-run comparison of frozen expectations and generic RPC facts. No RPC,
//! protocol instruction decoding, replay, or assumption of the same bank state.
use std::collections::{BTreeMap,HashMap,HashSet};
use serde_json::{Value,json};
use super::{Selection,ExecutionReport,amounts};

fn amount(info:&Value)->Option<u64>{info["amount"].as_str().and_then(|s|s.parse().ok())
    .or_else(||info["tokenAmount"]["amount"].as_str().and_then(|s|s.parse().ok()))}
fn balances(sim:&Value,key:&str,wallet:&str)->BTreeMap<String,i128>{
    let mut out=BTreeMap::new();
    for b in sim[key].as_array().into_iter().flatten().filter(|b|b["owner"]==wallet){
        if let (Some(m),Some(n))=(b["mint"].as_str(),b["uiTokenAmount"]["amount"].as_str().and_then(|s|s.parse::<i128>().ok())){
            *out.entry(m.into()).or_default()+=n;
        }
    }out
}
pub(super) fn compare(s:&Selection,r:&ExecutionReport)->Value{
    let Some(sim)=&r.simulation else{return json!({"id":r.id,"comparison":"simulation_missing"});};
    let wallet=s.request.wallet.to_string();let settlement=s.request.settlement.to_string();
    let pre=balances(sim,"preTokenBalances",&wallet);let post=balances(sim,"postTokenBalances",&wallet);
    let mints=pre.keys().chain(post.keys()).cloned().collect::<HashSet<_>>();
    let delta=mints.iter().map(|m|(m.clone(),post.get(m).copied().unwrap_or(0)-pre.get(m).copied().unwrap_or(0)))
        .filter(|(_,n)|*n!=0).collect::<BTreeMap<_,_>>();
    let settlement_delta=delta.get(&settlement).copied().unwrap_or(0);
    let other_inventory_consumed=delta.iter().any(|(m,n)|m!=&settlement&&*n<0);
    let fee=sim["fee"].as_u64();
    let native_delta=sim["postBalances"][0].as_u64().zip(sim["preBalances"][0].as_u64()).map(|(a,b)|i128::from(a)-i128::from(b));
    let instructions=sim["innerInstructions"].as_array().into_iter().flatten()
        .flat_map(|g|g["instructions"].as_array().into_iter().flatten()).collect::<Vec<_>>();
    let mut account_mints=HashMap::<String,String>::new();let mut owned=HashSet::<String>::new();
    for x in &instructions {
        let info=&x["parsed"]["info"];
        if let (Some(account),Some(mint))=(info["account"].as_str(),info["mint"].as_str()){
            account_mints.insert(account.into(),mint.into());
            if info["owner"]==wallet||info["wallet"]==wallet{owned.insert(account.into());}
        }
        if let Some(mint)=info["mint"].as_str(){for field in ["source","destination"]{
            if let Some(account)=info[field].as_str(){account_mints.insert(account.into(),mint.into());}
        }}
        if info["authority"]==wallet{if let Some(source)=info["source"].as_str(){owned.insert(source.into());}}
    }
    let mut deposits=0_u64;
    for x in &instructions {let info=&x["parsed"]["info"];
        if x["parsed"]["type"]=="createAccount"&&info["source"]==wallet
            &&info["newAccount"].as_str().is_some_and(|a|owned.contains(a)){
            deposits=deposits.saturating_add(info["lamports"].as_u64().unwrap_or(0));
        }
    }
    let mut actual=Vec::<Value>::new();let mut current=None::<(String,Vec<&Value>)>;
    let mut outside_swap_outflows=Vec::new();
    let flush=|pool:String,transfers:Vec<&Value>,actual:&mut Vec<Value>|{
        let mut debits=BTreeMap::<String,u64>::new();let mut credits=BTreeMap::<String,u64>::new();let mut unresolved=false;
        for x in transfers {let info=&x["parsed"]["info"];let Some(n)=amount(info)else{unresolved=true;continue;};
            let source=info["source"].as_str().unwrap_or("");let destination=info["destination"].as_str().unwrap_or("");
            let mint=info["mint"].as_str().or_else(||account_mints.get(source).map(String::as_str))
                .or_else(||account_mints.get(destination).map(String::as_str));
            let debit=owned.contains(source);let credit=owned.contains(destination);
            if let Some(m)=mint {
                if debit{let entry=debits.entry(m.into()).or_default();*entry=entry.saturating_add(n);}
                if credit{let entry=credits.entry(m.into()).or_default();*entry=entry.saturating_add(n);}
            }else if debit||credit{unresolved=true;}
        }
        let mut row=json!({"pool":pool,"wallet_debits_raw":debits,"wallet_credits_raw":credits,"unresolved_transfers":unresolved});
        if debits.len()==1&&!unresolved {
            let (mint,&n)=debits.first_key_value().unwrap();
            if let Some(e)=s.pools.iter().find(|p|p.pool==pool){
                let direction=if e.observation.base_mint==*mint{Some(0)}else if e.observation.quote_mint==*mint{Some(1)}else{None};
                if let Some(direction)=direction {
                    let output=if direction==0{&e.observation.quote_mint}else{&e.observation.base_mint};
                    let observed=credits.get(output).copied().unwrap_or(0);
                    let input=amounts::Input{slot:e.slot,observation:e.observation.clone(),manifest_matches:e.liquidity_window_checked};
                    row["input_mint"]=json!(mint);row["output_mint"]=json!(output);row["input_raw"]=json!(n);row["output_raw"]=json!(observed);
                    row["frozen_model_output_at_actual_input_raw"]=json!(amounts::quote(&input,direction,n).ok());
                    row["observation_slot_min"]=json!(e.observation.component_slot_min);
                    row["observation_slot_max"]=json!(e.observation.component_slot_max);
                }
            }
        }
        actual.push(row);
    };
    for x in instructions {
        if x["stackHeight"]==2 {
            if let Some((pool,transfers))=current.take(){flush(pool,transfers,&mut actual);}
            if let Some(pool)=s.pools.iter().find(|p|x["accounts"].as_array().is_some_and(|v|v.iter().any(|a|a==&p.pool))){
                current=Some((pool.pool.clone(),Vec::new()));
            }
        }
        if matches!(x["parsed"]["type"].as_str(),Some("transfer"|"transferChecked")){
            if let Some((_,transfers))=current.as_mut(){transfers.push(x);}
            else if x["parsed"]["info"]["authority"]==wallet {
                let info=&x["parsed"]["info"];
                let mint=info["mint"].as_str().or_else(||info["source"].as_str()
                    .and_then(|a|account_mints.get(a)).map(String::as_str));
                outside_swap_outflows.push(json!({"mint":mint,"amount_raw":amount(info),
                    "source":info["source"],"destination":info["destination"]}));
            }
        }
    }
    if let Some((pool,transfers))=current{flush(pool,transfers,&mut actual);}
    let native_settlement=settlement=="So11111111111111111111111111111111111111112";
    let after_network=if native_settlement{fee.map(|n|settlement_delta-i128::from(n))}else{None};
    let liquid=native_delta.filter(|_|native_settlement).map(|n|n+settlement_delta);
    json!({"id":r.id,"simulation_success":sim["err"].is_null(),"simulation_error":sim["err"],
        "expected_primary":s.amount_assessment.estimate,"expected_cost_assumptions":s.cost_assumptions,
        "actual_legs":actual,"wallet_token_outflows_outside_swaps":outside_swap_outflows,
        "actual_fee_lamports":fee,"wallet_token_delta_raw":delta,
        "settlement_delta_raw":settlement_delta,"other_inventory_consumed":other_inventory_consumed,
        "native_delta_lamports":native_delta,"wallet_owned_account_deposits_lamports":deposits,
        "native_settlement_profit_after_network_raw":after_network,
        "liquid_native_plus_wsol_delta_lamports":liquid,
        "liquid_delta_excluding_recorded_wallet_deposits_lamports":liquid.map(|n|n+i128::from(deposits)),
        "positive_after_network_without_other_token_inventory":after_network.map(|p|p>0&&!other_inventory_consumed&&sim["err"].is_null()
            &&liquid.is_some_and(|n|n+i128::from(deposits)>0)),
        "simulation_context_slot":r.context_slot,
        "comparison_scope":"frozen_observation_vs_simulation_bank_not_same_state_math_proof"})
}
