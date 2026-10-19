//! Complete typed execution job -> signed V10/Solana-v1 simulation -> typed report.
//! No DB, Registry, graph lookup, pool selection, protocol decoding or broadcasting.
use std::{collections::{HashMap, HashSet}, path::Path};
use anyhow::{Context, Result, bail, ensure};
use ring::signature::{self, Ed25519KeyPair, KeyPair};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::sync::mpsc;
use crate::core::{AccountMeta, execution::{ExecutionGroup, ExecutionMetrics,
    ExecutionOutcome, ExecutionPool, ExecutionPoolUse, ExecutionReport, ExecutionRequest}};
use super::rpc_transport::{AccountView, RpcAnswer, RpcHandle, RpcOperation};

const EXECUTOR: &str = "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz";
const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const MAINNET: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

fn key(value: &str) -> Pubkey { value.parse().expect("fixed public key") }
fn ata(wallet: Pubkey, mint: Pubkey, program: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[wallet.as_ref(), program.as_ref(), mint.as_ref()], &key(ASSOCIATED)).0
}

/// Signer is a startup resource, never part of an execution request or its logs.
pub struct Settings { signer: Ed25519KeyPair }
impl Settings {
    pub fn load(path: &Path) -> Result<Self> {
        let mut encoded=std::fs::read(path).context("read V10 wallet keypair")?;
        let decoded=serde_json::from_slice::<Vec<u8>>(&encoded);
        encoded.fill(0);
        let mut bytes=decoded.context("wallet must be a Solana JSON keypair byte array")?;
        let result=if bytes.len()==64 {
            Ed25519KeyPair::from_seed_and_public_key(&bytes[..32], &bytes[32..])
                .map_err(|_|anyhow::anyhow!("wallet seed/public key mismatch"))
        } else { Err(anyhow::anyhow!("wallet keypair must contain 64 bytes")) };
        bytes.fill(0);
        Ok(Self { signer:result? })
    }
    pub fn wallet(&self) -> Pubkey {
        Pubkey::new_from_array(self.signer.public_key().as_ref().try_into().expect("Ed25519 public key"))
    }
}

struct Meta { address: Pubkey, writable: bool, signer: bool }
struct Instruction {
    data: Vec<u8>, metas: Vec<Meta>, programs: HashSet<Pubkey>,
    projections: Vec<Pubkey>, metrics: ExecutionMetrics,
}
fn push(metas: &mut Vec<Meta>, address: Pubkey, writable: bool, signer: bool) {
    metas.push(Meta { address, writable, signer });
}
fn mint_program(pool: &ExecutionPool, mint: Pubkey) -> Result<Pubkey> {
    let proof=pool.mints.iter().find(|p|p.address==mint).context("mint proof missing for group role")?;
    ensure!(proof.unsupported.is_empty(), "input contains an unsupported mint proof");
    Ok(key(if proof.token_2022 { TOKEN_2022 } else { TOKEN }))
}
fn append_mint(metas: &mut Vec<Meta>, projections: &mut Vec<Pubkey>,
    wallet: Pubkey, mint: Pubkey, program: Pubkey) {
    push(metas,mint,false,false);
    push(metas,program,false,false);
    let address=ata(wallet,mint,program);
    push(metas,address,true,false);
    if !projections.contains(&address) { projections.push(address); }
}
fn block<'a>(pool: &'a ExecutionPool, quote: Pubkey, x: Pubkey, wallet: Pubkey)
    -> Result<&'a [AccountMeta]> {
    let m=&pool.manifest;
    ensure!(m.pool!=Pubkey::default() && m.mint_a!=m.mint_b && m.validated_slot>0,
        "invalid manifest identity or slot");
    ensure!(pool.mints[0].address==m.mint_a && pool.mints[1].address==m.mint_b,
        "manifest/mint proof identity mismatch");
    ensure!(m.bound_signer.is_none_or(|bound|bound==wallet), "manifest belongs to another signer");
    let accounts=if m.mint_a==quote && m.mint_b==x { &m.base_a }
        else if m.mint_b==quote && m.mint_a==x { &m.base_b }
        else { bail!("pool {} does not match its declared V10 role",m.pool) };
    ensure!((3..=24).contains(&accounts.len()), "pool account block must contain 3..=24 entries");
    ensure!(accounts[0].address!=Pubkey::default() && accounts[1].address==quote
        && !accounts[0].writable && !accounts[1].writable
        && accounts.iter().any(|a|a.address==m.pool), "invalid pool program/quote/account block");
    if accounts[0].address==key(PUMP) {
        ensure!(m.bound_signer==Some(wallet), "PumpSwap manifest requires explicit signer binding");
    }
    let _=mint_program(pool,quote)?;
    let _=mint_program(pool,x)?;
    Ok(accounts)
}
fn append_pool(ix: &mut Instruction, pool: &ExecutionPool, quote: Pubkey, x: Pubkey,
    wallet: Pubkey) -> Result<u16> {
    let accounts=block(pool,quote,x,wallet)?;
    ix.programs.insert(accounts[0].address);
    for account in accounts { push(&mut ix.metas,account.address,account.writable,false); }
    ix.metrics.pools.push(ExecutionPoolUse { pool:pool.manifest.pool,quote,
        validated_slot:pool.manifest.validated_slot });
    Ok(accounts.len() as u16)
}
fn instruction(request: &ExecutionRequest, wallet: Pubkey) -> Result<Instruction> {
    ensure!(!request.id.trim().is_empty() && request.id.len()<=128, "request ID must contain 1..=128 bytes");
    ensure!(request.wallet==wallet, "request wallet differs from configured signer");
    let s=request.settlement;
    ensure!([key(SOL),key(USDC)].contains(&s), "settlement must be WSOL or USDC");
    let q=if s==key(SOL) { key(USDC) } else { key(SOL) };
    let p=&request.parameters;
    ensure!((1..=1_400_000).contains(&p.executor_compute_unit_limit)
        && (1..=1_400_000).contains(&p.transaction_compute_unit_limit), "invalid compute limits");
    ensure!(p.loaded_accounts_data_size_limit>0 && p.loaded_accounts_data_size_limit<=64*1024*1024,
        "invalid loaded account data limit");
    ensure!(p.additional_fee_bp<=8500
        && (p.additional_fee_bp>0)==p.additional_fee_collector.is_some(), "additional fee/collector mismatch");
    ensure!(!p.constant_conversion || request.conversion.is_some(), "constant conversion requires a converter");
    ensure!((1..=4).contains(&request.groups.len()), "V10 requires 1..=4 groups");
    let bridge_mode=matches!(&request.groups[0],ExecutionGroup::Bridge{..});
    let mut all_pools:Vec<&ExecutionPool>=Vec::new();
    for group in &request.groups {
        match group {
            ExecutionGroup::Direct{pools,..}=>{
                ensure!(!bridge_mode && pools.len()>=2, "direct groups require two pools and cannot mix with bridge groups");
                all_pools.extend(pools);
            }
            ExecutionGroup::Bridge{intermediate,bridges,direct,..}=>{
                ensure!(bridge_mode && !bridges.is_empty() && !direct.is_empty(),
                    "bridge groups require intermediate/bridge/direct pools and cannot mix with direct groups");
                all_pools.push(intermediate); all_pools.extend(bridges); all_pools.extend(direct);
            }
        }
    }
    if let Some(conversion)=&request.conversion { all_pools.push(conversion); }
    ensure!(all_pools.len()<=16, "V10 exceeds 16 declarations including conversion; producer must choose a smaller job");
    // A pool may be shared by groups. Every occurrence must use the same frozen proof.
    let mut versions:HashMap<Pubkey,&ExecutionPool>=HashMap::new();
    let mut mint_programs=HashMap::new();
    for pool in &all_pools {
        if let Some(old)=versions.insert(pool.manifest.pool,pool) {
            ensure!(old.manifest==pool.manifest && old.mints==pool.mints,
                "conflicting versions of a shared pool");
        }
        for proof in &pool.mints {
            let program=mint_program(pool,proof.address)?;
            if let Some(old)=mint_programs.insert(proof.address,program) {
                ensure!(old==program,"conflicting token programs for the same mint");
            }
        }
    }
    for settlement in [s,q] {
        if let Some(program)=mint_programs.get(&settlement) {
            ensure!(*program==key(TOKEN),"settlement mint proof has an incorrect token program");
        }
    }
    let mut ix=Instruction { data:Vec::new(),metas:Vec::new(),programs:HashSet::new(),
        projections:vec![wallet,ata(wallet,s,key(TOKEN))],
        metrics:ExecutionMetrics { groups:request.groups.len(),pool_declarations:all_pools.len(),
            unique_accounts:0,account_references:0,instruction_bytes:0,transaction_bytes:0,pools:Vec::new() } };
    let collector=if s==key(USDC) { key("GzVRuLF349u78FHpr8KbqMhrZ1aDxnhSF59JWiZ6tbgt") }
        else if p.use_flashloan { key("6AGB9kqgSp2mQXwYpdrV4QVV8urvCaDS35U1wsLssy6H") }
        else { key("GPpkDpzCDmYJY5qNhYmM14c7rct1zmkjWc2CjR5g7RZ1") };
    push(&mut ix.metas,wallet,true,true); push(&mut ix.metas,s,false,false);
    push(&mut ix.metas,collector,true,false);
    if let Some(collector)=p.additional_fee_collector { push(&mut ix.metas,collector,true,false); }
    push(&mut ix.metas,ata(wallet,s,key(TOKEN)),true,false);
    push(&mut ix.metas,key(TOKEN),false,false);
    push(&mut ix.metas,Pubkey::default(),false,false);
    push(&mut ix.metas,key(ASSOCIATED),false,false);
    if p.use_flashloan {
        let program=key(EXECUTOR);
        let authority=Pubkey::find_program_address(&[b"vault_authority"],&program).0;
        let vault=Pubkey::find_program_address(&[b"vault_token_account",s.as_ref()],&program).0;
        push(&mut ix.metas,authority,false,false); push(&mut ix.metas,vault,true,false);
        ix.projections.push(vault);
    }
    let conversion_count=if let Some(pool)=&request.conversion {
        ensure!(mint_program(pool,q)?==key(TOKEN) && mint_program(pool,s)?==key(TOKEN),
            "invalid settlement token program");
        append_mint(&mut ix.metas,&mut ix.projections,wallet,q,key(TOKEN));
        Some(append_pool(&mut ix,pool,s,q,wallet)?)
    } else { None };
    let mut groups_data=Vec::new();
    for group in &request.groups {
        let mut used=HashSet::new();
        let mut add=|ix:&mut Instruction,pool:&ExecutionPool,quote:Pubkey,x:Pubkey|->Result<u16>{
            ensure!(used.insert(pool.manifest.pool),"duplicate pool within a group");
            append_pool(ix,pool,quote,x,wallet)
        };
        let target=match group {ExecutionGroup::Direct{target,..}|ExecutionGroup::Bridge{target,..}=>*target};
        ensure!(target!=Pubkey::default() && target!=s
            && (request.conversion.is_none() || target!=q),"invalid target role");
        let target_program=*mint_programs.get(&target).context("target token program missing")?;
        append_mint(&mut ix.metas,&mut ix.projections,wallet,target,target_program);
        match group {
            ExecutionGroup::Direct{pools,..}=>{
                groups_data.extend([0,pools.len() as u8]);
                let mut settlement_quoted=false;
                for pool in pools {
                    let m=&pool.manifest;
                    let quote=if m.mint_a==target {m.mint_b} else if m.mint_b==target {m.mint_a}
                        else {bail!("direct pool does not contain target")};
                    ensure!(quote==s || (request.conversion.is_some() && quote==q),"invalid direct quote");
                    settlement_quoted|=quote==s;
                    groups_data.extend(add(&mut ix,pool,quote,target)?.to_le_bytes());
                }
                ensure!(settlement_quoted,"direct group requires a settlement-quoted pool");
            }
            ExecutionGroup::Bridge{bridge,intermediate,bridges,direct,..}=>{
                ensure!(*bridge!=Pubkey::default() && *bridge!=s && *bridge!=target
                    && (request.conversion.is_none() || *bridge!=q),"invalid bridge mint role");
                let program=*mint_programs.get(bridge).context("bridge token program missing")?;
                append_mint(&mut ix.metas,&mut ix.projections,wallet,*bridge,program);
                groups_data.extend([bridges.len() as u8,direct.len() as u8]);
                groups_data.extend(add(&mut ix,intermediate,*bridge,target)?.to_le_bytes());
                for pool in bridges {
                    groups_data.extend(add(&mut ix,pool,if conversion_count.is_some(){q}else{s},*bridge)?.to_le_bytes());
                }
                for pool in direct { groups_data.extend(add(&mut ix,pool,s,target)?.to_le_bytes()); }
            }
        }
    }
    ix.data.push(61);
    ix.data.extend(p.minimum_profit.to_le_bytes());
    ix.data.extend(p.executor_compute_unit_limit.to_le_bytes());
    ix.data.push(u8::from(p.no_failure));
    ix.data.extend(p.additional_fee_bp.to_le_bytes());
    ix.data.push(u8::from(p.use_flashloan));
    ix.data.extend(p.trade_size.to_le_bytes());
    ix.data.push(u8::from(conversion_count.is_some()) | (u8::from(p.constant_conversion)<<1));
    ix.data.push(request.groups.len() as u8);
    if let Some(count)=conversion_count { ix.data.extend(count.to_le_bytes()); }
    ix.data.extend(groups_data);
    ix.metrics.instruction_bytes=ix.data.len();
    ix.metrics.account_references=ix.metas.len();
    Ok(ix)
}

struct Signed { bytes:Vec<u8>, keys:Vec<Pubkey>, signature:String }
fn compile(ix: &mut Instruction, request: &ExecutionRequest, settings: &Settings,
    blockhash: &str) -> Result<Signed> {
    let mut merged:Vec<Meta>=Vec::new();
    let program_meta=Meta{address:key(EXECUTOR),writable:false,signer:false};
    for meta in ix.metas.iter().chain(std::iter::once(&program_meta)) {
        if let Some(old)=merged.iter_mut().find(|a|a.address==meta.address) {
            old.writable|=meta.writable; old.signer|=meta.signer;
        } else { merged.push(Meta{address:meta.address,writable:meta.writable,signer:meta.signer}); }
    }
    ensure!(merged.iter().filter(|a|a.signer).count()==1 && merged[0].address==request.wallet,
        "only the configured wallet may sign");
    let mut keys=vec![request.wallet];
    keys.extend(merged.iter().filter(|a|!a.signer && a.writable).map(|a|a.address));
    keys.extend(merged.iter().filter(|a|!a.signer && !a.writable).map(|a|a.address));
    ensure!(keys.len()<=64,"transaction exceeds configured 64-account boundary; producer must reduce the job");
    ensure!(ix.metas.len()<=255,"instruction exceeds 255 account references");
    let index=|address:Pubkey|->u8{keys.iter().position(|k|*k==address).expect("compiled key") as u8};
    let readonly=merged.iter().filter(|a|!a.signer && !a.writable).count() as u8;
    let hash:Pubkey=blockhash.parse().context("invalid blockhash bytes")?;
    let p=&request.parameters;
    let mask:u32=12 | if p.priority_fee_lamports>0 {3} else {0};
    let mut message=vec![0x81,1,0,readonly];
    message.extend(mask.to_le_bytes()); message.extend(hash.to_bytes());
    message.extend([1,keys.len() as u8]);
    for address in &keys {message.extend(address.to_bytes());}
    if p.priority_fee_lamports>0 {message.extend(p.priority_fee_lamports.to_le_bytes());}
    message.extend(p.transaction_compute_unit_limit.to_le_bytes());
    message.extend(p.loaded_accounts_data_size_limit.to_le_bytes());
    message.extend([index(key(EXECUTOR)),ix.metas.len() as u8]);
    message.extend(u16::try_from(ix.data.len())?.to_le_bytes());
    for meta in &ix.metas {message.push(index(meta.address));}
    message.extend(&ix.data);
    ensure!(message.len()+64<=4096,"v1 transaction exceeds 4096 bytes; producer must reduce the job");
    let signature=settings.signer.sign(&message);
    signature::UnparsedPublicKey::new(&signature::ED25519,settings.signer.public_key().as_ref())
        .verify(&message,signature.as_ref()).map_err(|_|anyhow::anyhow!("local signature verification failed"))?;
    let signature_text=bs58::encode(signature.as_ref()).into_string();
    message.extend(signature.as_ref());
    ix.metrics.unique_accounts=keys.len(); ix.metrics.transaction_bytes=message.len();
    Ok(Signed{bytes:message,keys,signature:signature_text})
}

fn observed_dex_calls(value: &Value, keys: &[Pubkey], ix: &Instruction) -> Vec<String> {
    let pools:HashSet<Pubkey>=ix.metrics.pools.iter().map(|p|p.pool).collect();
    let resolve=|v:&Value|->Option<Pubkey>{v.as_str().and_then(|s|s.parse().ok())
        .or_else(||v.as_u64().and_then(|i|keys.get(i as usize).copied()))};
    let mut observed=Vec::new();
    if let Some(groups)=value.get("innerInstructions").and_then(Value::as_array) {
        for instruction in groups.iter().filter_map(|g|g.get("instructions").and_then(Value::as_array)).flatten() {
            let program=instruction.get("programId").or_else(||instruction.get("programIdIndex")).and_then(resolve);
            if let Some(program)=program.filter(|p|ix.programs.contains(p)) {
                if instruction.get("accounts").and_then(Value::as_array)
                    .is_some_and(|accounts|accounts.iter().filter_map(resolve).any(|p|pools.contains(&p))) {
                    observed.push(program.to_string());
                }
            }
        }
    }
    observed
}
async fn execute(settings: &Settings, rpc: &RpcHandle, request: &ExecutionRequest,
    report: &mut ExecutionReport) -> Result<()> {
    let mut ix=instruction(request,settings.wallet())?;
    // Check exact account/byte limits before performing any RPC or signing real blockhash data.
    let _=compile(&mut ix,request,settings,"11111111111111111111111111111111")?;
    report.metrics=Some(ix.metrics.clone());
    report.phase="chain_preflight".into(); report.outcome=ExecutionOutcome::RpcFailure;
    match rpc.call(RpcOperation::GenesisHash).await?.answer {
        RpcAnswer::GenesisHash(hash)=>ensure!(hash==MAINNET,"V10 worker expects mainnet genesis"),
        _=>bail!("unexpected genesis response"),
    }
    match rpc.call(RpcOperation::Account{address:key(EXECUTOR),view:AccountView::Slice{offset:0,length:0}}).await?.answer {
        RpcAnswer::Account{account:Some(account),..}=>ensure!(account.executable,"V10 program is not executable"),
        _=>bail!("V10 executor program account missing"),
    }
    let min_slot=ix.metrics.pools.iter().map(|p|p.validated_slot).max().context("no manifest slots")?;
    for attempt in 0..2 {
        report.phase="latest_blockhash".into();
        let (slot,blockhash,last_valid_block_height)=match rpc.call(
            RpcOperation::LatestBlockhash{min_context_slot:Some(min_slot)}).await?.answer {
            RpcAnswer::LatestBlockhash{slot,blockhash,last_valid_block_height}=>(slot,blockhash,last_valid_block_height),
            _=>bail!("unexpected blockhash response"),
        };
        report.phase="sign".into(); report.outcome=ExecutionOutcome::Rejected;
        let signed=compile(&mut ix,request,settings,&blockhash)?;
        report.simulated_signature=Some(signed.signature.clone()); report.blockhash=Some(blockhash);
        report.last_valid_block_height=Some(last_valid_block_height);
        report.phase="simulate".into(); report.outcome=ExecutionOutcome::RpcFailure;
        let response=rpc.call(RpcOperation::Simulate{transaction:signed.bytes,
            min_context_slot:Some(min_slot.max(slot)),accounts:ix.projections.clone()}).await?;
        let RpcAnswer::Simulation{slot,value}=response.answer else {bail!("unexpected simulation response")};
        report.context_slot=Some(slot); report.provider=Some(response.provider);
        if attempt==0 && value.get("err").and_then(Value::as_str)==Some("BlockhashNotFound") {
            report.blockhash_retried=true; continue;
        }
        report.executor_invoked=value.get("logs").and_then(Value::as_array).is_some_and(|logs|
            logs.iter().filter_map(Value::as_str).any(|s|s.starts_with(&format!("Program {EXECUTOR} invoke ["))));
        report.dex_invocations=observed_dex_calls(&value,&signed.keys,&ix);
        report.outcome=if value.get("err").is_some_and(|e|!e.is_null()) {
            ExecutionOutcome::SimulationFailed
        } else if report.dex_invocations.is_empty() {
            ExecutionOutcome::SimulatedNoDexCallObserved
        } else { ExecutionOutcome::SimulatedDexCallsObserved };
        report.simulation=Some(value); report.phase="complete".into();
        return Ok(());
    }
    bail!("simulation retry exhausted")
}

pub async fn run(settings: Settings, rpc: RpcHandle, mut input: mpsc::Receiver<ExecutionRequest>,
    output: mpsc::Sender<ExecutionReport>) -> Result<()> {
    crate::log_event!(Info,"v10 started wallet={} mode=simulation broadcasts=false",settings.wallet());
    let mut count=0_u64;
    while let Some(request)=input.recv().await {
        let mut report=ExecutionReport{id:request.id.clone(),outcome:ExecutionOutcome::Rejected,
            phase:"validate".into(),error:None,metrics:None,simulated_signature:None,
            blockhash:None,last_valid_block_height:None,context_slot:None,provider:None,
            blockhash_retried:false,executor_invoked:false,dex_invocations:Vec::new(),simulation:None};
        if let Err(error)=execute(&settings,&rpc,&request,&mut report).await {
            report.error=Some(format!("{error:#}"));
        }
        crate::log_event!(Info,"v10 result request_id={:?} outcome={:?} phase={} context_slot={:?} dex_calls={}",
            report.id,report.outcome,report.phase,report.context_slot,report.dex_invocations.len());
        output.send(report).await.context("V10 report consumer stopped")?;
        count+=1;
    }
    crate::log_event!(Info,"v10 complete requests={count} broadcasts=0");
    Ok(())
}
