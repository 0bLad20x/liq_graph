//! Complete typed execution job -> signed V10/Solana-v1 simulation -> typed report.
//! No DB, Registry, graph lookup, pool selection, protocol decoding or broadcasting.
use std::{cell::RefCell, collections::{HashMap, HashSet}, path::Path, time::Instant, sync::{Arc, LazyLock, atomic::{AtomicU64, Ordering}}};
use anyhow::{Context, Result, bail, ensure};
use ring::signature::{self, Ed25519KeyPair, KeyPair};
use serde_json::Value;
use solana_pubkey::Pubkey;
use tokio::{sync::mpsc,task::JoinSet};
use crate::core::{AccountMeta, execution::{ExecutionGroup, ExecutionMetrics,
    ExecutionOutcome, ExecutionPool, ExecutionPoolUse, ExecutionReport, ExecutionRequest,
    ExecutionTrace,ExecutionSwapCall,ExecutionAssetAmount,ExecutionBalanceChanges}};
use crate::core::rpc::{AccountView, RpcAnswer, RpcHandle, RpcOperation};

/// Compact observed changes from the executor's own successful RPC result.
/// The evidence thread calls this; no RPC, pricing or on-chain action is involved.
pub fn balance_changes(request:&ExecutionRequest,report:&ExecutionReport)->Result<Option<ExecutionBalanceChanges>> {
    let Some(sim)=report.simulation.as_ref().filter(|s|s["err"].is_null()) else{return Ok(None);};
    let wallet=request.wallet.to_string();let settlement=request.settlement.to_string();
    let tokens=|field:&str|->Result<Option<i128>> {
        let Some(entries)=sim.get(field).and_then(Value::as_array) else{return Ok(None);};
        let mut total=0_i128;
        for balance in entries {
            if balance["owner"].as_str()!=Some(wallet.as_str())||balance["mint"].as_str()!=Some(settlement.as_str()){continue;}
            let amount=balance["uiTokenAmount"]["amount"].as_str().context("simulation token amount missing")?
                .parse::<u64>().context("invalid simulation token amount")?;
            total=total.checked_add(i128::from(amount)).context("simulation token balance overflow")?;
        }
        Ok(Some(total))
    };
    let gain=tokens("preTokenBalances")?.zip(tokens("postTokenBalances")?).map(|(a,b)|b-a);
    let settlement_gain_raw=gain.map(|n|n.to_string());
    let above_network_fee=(settlement==SOL).then_some(gain).flatten()
        .zip(sim.get("fee").and_then(Value::as_u64)).map(|(gain,fee)|gain>i128::from(fee));
    let pre=sim.get("preBalances").and_then(Value::as_array);
    let post=sim.get("postBalances").and_then(Value::as_array);
    let mut native_wallet_change_lamports=None;let mut new_account_lamports=None;
    if let Some((pre,post))=pre.zip(post).filter(|(a,b)|!a.is_empty()&&a.len()==b.len()) {
        native_wallet_change_lamports=pre[0].as_u64().zip(post[0].as_u64())
            .map(|(a,b)|(i128::from(b)-i128::from(a)).to_string());
        let mut funded=0_u64;
        for (a,b) in pre.iter().zip(post) {
            let a=a.as_u64().context("invalid pre-simulation lamports")?;
            let b=b.as_u64().context("invalid post-simulation lamports")?;
            if a==0&&b>0{funded=funded.checked_add(b).context("account funding overflow")?;}
        }
        new_account_lamports=Some(funded);
    }
    Ok(Some(ExecutionBalanceChanges{settlement_gain_raw,native_wallet_change_lamports,new_account_lamports,above_network_fee}))
}

const EXECUTOR: &str = "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz";
const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const ASSOCIATED: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const ORCA: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const MAINNET: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

// CP10: decode fixed program addresses exactly once. Do not parse base58 on every
// capacity trial. Unknown values remain validated by the original parser.
static EXECUTOR_KEY: LazyLock<Pubkey> = LazyLock::new(|| EXECUTOR.parse().expect("executor key"));
static SOL_KEY: LazyLock<Pubkey> = LazyLock::new(|| SOL.parse().expect("WSOL key"));
static USDC_KEY: LazyLock<Pubkey> = LazyLock::new(|| USDC.parse().expect("USDC key"));
static TOKEN_KEY: LazyLock<Pubkey> = LazyLock::new(|| TOKEN.parse().expect("token program"));
static TOKEN_2022_KEY: LazyLock<Pubkey> = LazyLock::new(|| TOKEN_2022.parse().expect("token-2022 program"));
static ASSOCIATED_KEY: LazyLock<Pubkey> = LazyLock::new(|| ASSOCIATED.parse().expect("ATA program"));
static PUMP_KEY: LazyLock<Pubkey> = LazyLock::new(|| PUMP.parse().expect("PumpSwap program"));
static ORCA_KEY: LazyLock<Pubkey> = LazyLock::new(|| ORCA.parse().expect("Orca program"));
static MAINNET_KEY: LazyLock<Pubkey> = LazyLock::new(|| MAINNET.parse().expect("mainnet genesis"));
const COLLECTOR_USDC:&str="GzVRuLF349u78FHpr8KbqMhrZ1aDxnhSF59JWiZ6tbgt";
const COLLECTOR_SOL_FLASH:&str="6AGB9kqgSp2mQXwYpdrV4QVV8urvCaDS35U1wsLssy6H";
const COLLECTOR_SOL_OTHER:&str="GPpkDpzCDmYJY5qNhYmM14c7rct1zmkjWc2CjR5g7RZ1";
static COLLECTOR_USDC_KEY: LazyLock<Pubkey> = LazyLock::new(|| COLLECTOR_USDC.parse().expect("USDC collector"));
static COLLECTOR_SOL_FLASH_KEY: LazyLock<Pubkey> = LazyLock::new(|| COLLECTOR_SOL_FLASH.parse().expect("flashloan collector"));
static COLLECTOR_SOL_OTHER_KEY: LazyLock<Pubkey> = LazyLock::new(|| COLLECTOR_SOL_OTHER.parse().expect("SOL collector"));
fn key(value: &str) -> Pubkey {
    match value {
        EXECUTOR => *EXECUTOR_KEY, SOL => *SOL_KEY, USDC => *USDC_KEY,
        TOKEN => *TOKEN_KEY, TOKEN_2022 => *TOKEN_2022_KEY,
        ASSOCIATED => *ASSOCIATED_KEY, PUMP => *PUMP_KEY, ORCA => *ORCA_KEY,
        MAINNET => *MAINNET_KEY,
        COLLECTOR_USDC => *COLLECTOR_USDC_KEY,
        COLLECTOR_SOL_FLASH => *COLLECTOR_SOL_FLASH_KEY,
        COLLECTOR_SOL_OTHER => *COLLECTOR_SOL_OTHER_KEY,
        _ => value.parse().expect("fixed public key"),
    }
}
// Bound the per-worker-thread ATA cache: no global lock on 436k capacity trials,
// no unbounded growth, and no cached pool/bin-array state.
const ATA_CACHE_LIMIT:usize=4096;
std::thread_local! {
    static ATA_CACHE: RefCell<HashMap<(Pubkey,Pubkey,Pubkey),Pubkey>> =
        RefCell::new(HashMap::new());
}
fn ata(wallet: Pubkey, mint: Pubkey, program: Pubkey) -> Pubkey {
    ATA_CACHE.with(|cache| {
        if let Some(&address)=cache.borrow().get(&(wallet,mint,program)) {return address;}
        let address=Pubkey::find_program_address(
            &[wallet.as_ref(), program.as_ref(), mint.as_ref()], &key(ASSOCIATED)).0;
        let mut cache=cache.borrow_mut();
        if cache.len()>=ATA_CACHE_LIMIT {cache.clear();}
        cache.insert((wallet,mint,program),address);
        address
    })
}
static FLASH_AUTHORITY:LazyLock<Pubkey> = LazyLock::new(||
    Pubkey::find_program_address(&[b"vault_authority"], &key(EXECUTOR)).0);
static FLASH_VAULT_SOL:LazyLock<Pubkey> = LazyLock::new(||
    Pubkey::find_program_address(&[b"vault_token_account", key(SOL).as_ref()], &key(EXECUTOR)).0);
static FLASH_VAULT_USDC:LazyLock<Pubkey> = LazyLock::new(||
    Pubkey::find_program_address(&[b"vault_token_account", key(USDC).as_ref()], &key(EXECUTOR)).0);

/// Signer is a startup resource, never part of an execution request or its logs.
pub struct Settings { signer: Ed25519KeyPair, fast: bool }
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
        let fast=match std::env::var("V10_SIMULATION_FAST").as_deref() {
            Ok("false"|"0")=>false, Ok("true"|"1")|Err(_)=>true,
            Ok(_)=>bail!("V10_SIMULATION_FAST must be true or false"),
        };
        Ok(Self { signer:result?, fast })
    }
    pub fn wallet(&self) -> Pubkey {
        Pubkey::new_from_array(self.signer.public_key().as_ref().try_into().expect("Ed25519 public key"))
    }
}

struct Meta { address: Pubkey, writable: bool, signer: bool }
struct Instruction {
    data: Vec<u8>, metas: Vec<Meta>, programs: HashSet<Pubkey>,
    projections: Vec<Pubkey>, metrics: ExecutionMetrics,
    wallet_tokens:HashMap<Pubkey,Pubkey>,
}
fn push(metas: &mut Vec<Meta>, address: Pubkey, writable: bool, signer: bool) {
    metas.push(Meta { address, writable, signer });
}
fn mint_program(pool: &ExecutionPool, mint: Pubkey) -> Result<Pubkey> {
    let proof=pool.mints.iter().find(|p|p.address==mint).context("mint proof missing for group role")?;
    ensure!(proof.unsupported.is_empty(), "input contains an unsupported mint proof");
    Ok(key(if proof.token_2022 { TOKEN_2022 } else { TOKEN }))
}
fn append_mint(ix:&mut Instruction,wallet:Pubkey,mint:Pubkey,program:Pubkey) {
    push(&mut ix.metas,mint,false,false);
    push(&mut ix.metas,program,false,false);
    let address=ata(wallet,mint,program);
    push(&mut ix.metas,address,true,false);
    if !ix.projections.contains(&address) {ix.projections.push(address);}
    ix.wallet_tokens.insert(address,mint);
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
    // A one-sided Prepared Orca pool deliberately carries no arrays on its
    // unsupported side. Never let a malformed seven-account V10 leg through.
    if accounts.first().is_some_and(|m|m.address==*ORCA_KEY){
        ensure!((8..=24).contains(&accounts.len()),"Orca direction has no validated initial TickArray");
    }
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
    ensure!(p.use_flashloan,"V10 simulation requires use_flashloan=true; trading capital must come from the settlement loan vault");
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
    let settlement_ata=ata(wallet,s,key(TOKEN));
    let mut ix=Instruction { data:Vec::new(),metas:Vec::new(),programs:HashSet::new(),
        wallet_tokens:HashMap::from([(settlement_ata,s)]),
        projections:vec![wallet,settlement_ata],
        metrics:ExecutionMetrics { groups:request.groups.len(),pool_declarations:all_pools.len(),
            unique_accounts:0,account_references:0,instruction_bytes:0,transaction_bytes:0,pools:Vec::new() } };
    let collector=if s==key(USDC) { key(COLLECTOR_USDC) }
        else if p.use_flashloan { key(COLLECTOR_SOL_FLASH) }
        else { key(COLLECTOR_SOL_OTHER) };
    push(&mut ix.metas,wallet,true,true); push(&mut ix.metas,s,false,false);
    push(&mut ix.metas,collector,true,false);
    if let Some(collector)=p.additional_fee_collector { push(&mut ix.metas,collector,true,false); }
    push(&mut ix.metas,settlement_ata,true,false);
    push(&mut ix.metas,key(TOKEN),false,false);
    push(&mut ix.metas,Pubkey::default(),false,false);
    push(&mut ix.metas,key(ASSOCIATED),false,false);
    if p.use_flashloan {
        let authority=*FLASH_AUTHORITY;
        let vault=if s==key(SOL){*FLASH_VAULT_SOL}else{*FLASH_VAULT_USDC};
        push(&mut ix.metas,authority,false,false); push(&mut ix.metas,vault,true,false);
        ix.projections.push(vault);
    }
    let conversion_count=if let Some(pool)=&request.conversion {
        ensure!(mint_program(pool,q)?==key(TOKEN) && mint_program(pool,s)?==key(TOKEN),
            "invalid settlement token program");
        append_mint(&mut ix,wallet,q,key(TOKEN));
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
        append_mint(&mut ix,wallet,target,target_program);
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
                append_mint(&mut ix,wallet,*bridge,program);
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
fn message(ix: &mut Instruction, request: &ExecutionRequest,
    blockhash: &str) -> Result<(Vec<u8>,Vec<Pubkey>)> {
    let mut merged:Vec<Meta>=Vec::with_capacity(ix.metas.len()+1);
    // CP10: O(1) expected lookup; Vec retains the original first-seen
    // account ordering, and writable/signer promotion remains unchanged.
    let mut merged_positions=HashMap::<Pubkey,usize>::with_capacity(ix.metas.len()+1);
    let program_meta=Meta{address:key(EXECUTOR),writable:false,signer:false};
    for meta in ix.metas.iter().chain(std::iter::once(&program_meta)) {
        if let Some(&position)=merged_positions.get(&meta.address) {
            let old=&mut merged[position];
            old.writable|=meta.writable; old.signer|=meta.signer;
        } else {
            merged_positions.insert(meta.address,merged.len());
            merged.push(Meta{address:meta.address,writable:meta.writable,signer:meta.signer});
        }
    }
    ensure!(merged.iter().filter(|a|a.signer).count()==1 && merged[0].address==request.wallet,
        "only the configured wallet may sign");
    let mut keys=vec![request.wallet];
    keys.extend(merged.iter().filter(|a|!a.signer && a.writable).map(|a|a.address));
    keys.extend(merged.iter().filter(|a|!a.signer && !a.writable).map(|a|a.address));
    ensure!(keys.len()<=64,"transaction exceeds configured 64-account boundary; producer must reduce the job");
    ensure!(ix.metas.len()<=255,"instruction exceeds 255 account references");
    let positions:HashMap<Pubkey,u8>=keys.iter().enumerate()
        .map(|(index,&address)|(address,index as u8)).collect();
    let index=|address:Pubkey|->u8{*positions.get(&address).expect("compiled key")};
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
    ix.metrics.unique_accounts=keys.len(); ix.metrics.transaction_bytes=message.len()+64;
    Ok((message,keys))
}

/// CP10 phases are aggregate, bounded atomic counters; no per-request event buffer.
#[derive(Default,Debug,serde::Serialize)]
pub struct CapacityPhaseProfile {
    pub calls:u64, pub instruction_ns:u64, pub message_ns:u64,
    pub instruction_failures:u64, pub message_failures:u64,
}
static CAP_PROFILE_ENABLED: LazyLock<bool> = LazyLock::new(||
    matches!(std::env::var("GRAPH_SIGNALS_PROFILE").as_deref(),
        Ok("true"|"1"|"yes")));
static CAP_CALLS:AtomicU64=AtomicU64::new(0);
static CAP_INSTRUCTION_NS:AtomicU64=AtomicU64::new(0);
static CAP_MESSAGE_NS:AtomicU64=AtomicU64::new(0);
static CAP_INSTRUCTION_FAILURES:AtomicU64=AtomicU64::new(0);
static CAP_MESSAGE_FAILURES:AtomicU64=AtomicU64::new(0);

pub fn capacity_phase_profile()->CapacityPhaseProfile {
    CapacityPhaseProfile {
        calls:CAP_CALLS.load(Ordering::Relaxed),
        instruction_ns:CAP_INSTRUCTION_NS.load(Ordering::Relaxed),
        message_ns:CAP_MESSAGE_NS.load(Ordering::Relaxed),
        instruction_failures:CAP_INSTRUCTION_FAILURES.load(Ordering::Relaxed),
        message_failures:CAP_MESSAGE_FAILURES.load(Ordering::Relaxed),
    }
}
/// Exact existing packing/validation boundary. No signing, RPC or pool selection.
/// The timed phases are recorded only when GRAPH_SIGNALS_PROFILE is enabled.
pub fn validate(request: &ExecutionRequest) -> Result<ExecutionMetrics> {
    let measure=*CAP_PROFILE_ENABLED;
    let started=Instant::now();
    let constructed=instruction(request,request.wallet);
    if measure {
        CAP_CALLS.fetch_add(1,Ordering::Relaxed);
        CAP_INSTRUCTION_NS.fetch_add(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed);
        if constructed.is_err() {CAP_INSTRUCTION_FAILURES.fetch_add(1,Ordering::Relaxed);}
    }
    let mut ix=constructed?;
    let started=Instant::now();
    let checked=message(&mut ix,request,"11111111111111111111111111111111");
    if measure {
        CAP_MESSAGE_NS.fetch_add(started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed);
        if checked.is_err(){CAP_MESSAGE_FAILURES.fetch_add(1,Ordering::Relaxed);}
    }
    let _=checked?;
    Ok(ix.metrics)
}

fn compile(ix: &mut Instruction, request: &ExecutionRequest, settings: &Settings,
    blockhash: &str) -> Result<Signed> {
    let (mut message,keys)=message(ix,request,blockhash)?;
    let signature=settings.signer.sign(&message);
    signature::UnparsedPublicKey::new(&signature::ED25519,settings.signer.public_key().as_ref())
        .verify(&message,signature.as_ref()).map_err(|_|anyhow::anyhow!("local signature verification failed"))?;
    let signature_text=bs58::encode(signature.as_ref()).into_string();
    message.extend(signature.as_ref());
    Ok(Signed{bytes:message,keys,signature:signature_text})
}

/// Generic SPL token flows under a declared DEX CPI; no DEX instruction decoding.
/// Refunds are netted per mint. Missing scope/amounts stay explicit, never guessed.
fn observed_trace(value:&Value,keys:&[Pubkey],ix:&Instruction)->ExecutionTrace {
    let Some(groups)=value.get("innerInstructions").and_then(Value::as_array) else {
        return ExecutionTrace{status:"inner_instructions_unavailable".into(),calls:Vec::new()};
    };
    let resolve=|v:&Value|->Option<Pubkey>{v.as_str().and_then(|s|s.parse().ok())
        .or_else(||v.as_u64().and_then(|i|keys.get(i as usize).copied()))};
    let pools=ix.metrics.pools.iter().map(|p|p.pool).collect::<HashSet<_>>();
    let token_programs=[key(TOKEN),key(TOKEN_2022)];
    let mut trace=ExecutionTrace{status:"available".into(),calls:Vec::new()};
    let mut moves=Vec::<HashMap<Pubkey,(u128,u128)>>::new();
    for group in groups {
        let mut current:Option<usize>=None;
        for instruction in group.get("instructions").and_then(Value::as_array).into_iter().flatten() {
            let height=instruction.get("stackHeight").and_then(Value::as_u64);
            if let Some(i)=current {
                if height.zip(trace.calls[i].stack_height).is_none_or(|(h,parent)|h<=parent) {current=None;}
            }
            let program=instruction.get("programId").or_else(||instruction.get("programIdIndex")).and_then(resolve);
            if let Some(program)=program.filter(|p|ix.programs.contains(p)) {
                let found=instruction.get("accounts").and_then(Value::as_array).into_iter().flatten()
                    .filter_map(resolve).filter(|p|pools.contains(p)).collect::<HashSet<_>>();
                if !found.is_empty() {
                    let pool=(found.len()==1).then(||found.iter().next().expect("nonempty").to_string());
                    let mut issues=Vec::new();
                    if pool.is_none(){issues.push("ambiguous_pool_accounts".into());}
                    if height.is_none(){issues.push("stack_height_unavailable".into());}
                    trace.calls.push(ExecutionSwapCall{program:program.to_string(),pool,stack_height:height,
                        status:"no_complete_swap".into(),issues,inputs:Vec::new(),outputs:Vec::new()});
                    moves.push(HashMap::new());current=height.map(|_|trace.calls.len()-1);
                }
            }
            let Some(i)=current else{continue;};
            if !program.is_some_and(|p|token_programs.contains(&p)){continue;}
            let parsed=instruction.get("parsed").filter(|p|p.is_object());
            let mut transfer:Option<(Pubkey,Pubkey,Option<Pubkey>,u64)>=None;
            if let Some(parsed)=parsed {
                if matches!(parsed["type"].as_str(),Some("transfer"|"transferChecked")) {
                    let info=&parsed["info"];
                    let amount=info.get("amount").or_else(||info.pointer("/tokenAmount/amount"))
                        .and_then(|n|n.as_str().and_then(|n|n.parse().ok()).or_else(||n.as_u64()));
                    transfer=resolve(&info["source"]).zip(resolve(&info["destination"])).zip(amount)
                        .map(|((source,destination),amount)|(source,destination,resolve(&info["mint"]),amount));
                    if transfer.is_none(){trace.calls[i].issues.push("parsed_transfer_unresolved".into());}
                } else if parsed["type"].as_str().is_some_and(|t|t.starts_with("transfer")) {
                    trace.calls[i].issues.push("unsupported_token_transfer".into());
                }
            } else if let Some(encoded)=instruction.get("data").and_then(Value::as_str) {
                if let Ok(data)=bs58::decode(encoded).into_vec() {
                    if matches!(data.first(),Some(3|12)) {
                        let checked=data[0]==12;
                        let accounts=instruction.get("accounts").and_then(Value::as_array);
                        let account=|index:usize|accounts.and_then(|a|a.get(index)).and_then(resolve);
                        let amount=data.get(1..9).and_then(|v|v.try_into().ok()).map(u64::from_le_bytes);
                        transfer=account(0).zip(account(if checked{2}else{1})).zip(amount)
                            .map(|((source,destination),amount)|(source,destination,if checked{account(1)}else{None},amount));
                        if transfer.is_none(){trace.calls[i].issues.push("compiled_transfer_unresolved".into());}
                    } else if data.first()==Some(&26) {
                        trace.calls[i].issues.push("token_transfer_extension_unresolved".into());
                    }
                } else {trace.calls[i].issues.push("token_instruction_encoding_unresolved".into());}
            }
            let Some((source,destination,mint,amount))=transfer else{continue;};
            let sent=ix.wallet_tokens.get(&source);let received=ix.wallet_tokens.get(&destination);
            if sent.is_none()&&received.is_none(){continue;}
            let Some(mint)=mint.or_else(||sent.or(received).copied())else{continue;};
            if [sent,received].into_iter().flatten().any(|m|*m!=mint) {
                trace.calls[i].issues.push("wallet_transfer_mint_mismatch".into());continue;
            }
            let flow=moves[i].entry(mint).or_default();
            if sent.is_some(){flow.0+=u128::from(amount);}
            if received.is_some(){flow.1+=u128::from(amount);}
        }
    }
    for (call,flows) in trace.calls.iter_mut().zip(moves) {
        for (mint,(sent,received)) in flows {
            let (target,amount,gross)=if sent>received{(&mut call.inputs,sent-received,sent)}else{(&mut call.outputs,received-sent,received)};
            if amount==0{continue;}
            if let (Ok(amount_raw),Ok(gross_amount_raw))=(u64::try_from(amount),u64::try_from(gross)){
                target.push(ExecutionAssetAmount{mint:mint.to_string(),amount_raw,gross_amount_raw});}
            else{call.issues.push("flow_amount_overflow".into());}
        }
        call.inputs.sort_by(|a,b|a.mint.cmp(&b.mint));call.outputs.sort_by(|a,b|a.mint.cmp(&b.mint));
        if call.pool.is_some()&&call.issues.is_empty()&&call.inputs.len()==1&&call.outputs.len()==1
            &&call.inputs[0].mint!=call.outputs[0].mint {call.status="complete".into();}
    }
    trace
}
async fn execute(settings: &Settings, rpc: &RpcHandle, request: &ExecutionRequest,
    report: &mut ExecutionReport) -> Result<()> {
    let mut ix=instruction(request,settings.wallet())?;
    // Check exact account/byte limits before performing any RPC or signing real blockhash data.
    let _=message(&mut ix,request,"11111111111111111111111111111111")?;
    report.metrics=Some(ix.metrics.clone());
    let min_slot=ix.metrics.pools.iter().map(|p|p.validated_slot).max().context("no manifest slots")?;
    // In fast mode the RPC node substitutes its own valid blockhash. No serial
    // genesis/program/blockhash HTTP lookup is allowed on this signal hotpath.
    for attempt in 0..if settings.fast {1}else{2} {
        report.phase="latest_blockhash".into();
        let (slot,blockhash,last_valid_block_height)=if settings.fast {
            (min_slot,"11111111111111111111111111111111".to_owned(),None)
        } else {
            match rpc.call(RpcOperation::LatestBlockhash{min_context_slot:Some(min_slot)}).await?.answer {
                RpcAnswer::LatestBlockhash{slot,blockhash,last_valid_block_height}=>
                    (slot,blockhash,Some(last_valid_block_height)),
                _=>bail!("unexpected blockhash response"),
            }
        };
        report.phase="sign".into(); report.outcome=ExecutionOutcome::Rejected;
        let build_started=Instant::now();
        let signed=compile(&mut ix,request,settings,&blockhash)?;
        let build_us=build_started.elapsed().as_micros();
        report.build_us=Some(report.build_us.unwrap_or(0).saturating_add(build_us.min(u128::from(u64::MAX)) as u64));
        // A fast simulation uses a replaced blockhash: the local signature is
        // not a signature of the executed message and must not be advertised.
        if !settings.fast {
            report.simulated_signature=Some(signed.signature.clone());
            report.blockhash=Some(blockhash);
            report.last_valid_block_height=last_valid_block_height;
        }
        report.phase="simulate".into(); report.outcome=ExecutionOutcome::RpcFailure;
        let operation=if settings.fast {
            RpcOperation::SimulateFast{transaction:signed.bytes,min_context_slot:Some(min_slot),
                accounts:ix.projections.clone()}
        } else {
            RpcOperation::Simulate{transaction:signed.bytes,min_context_slot:Some(min_slot.max(slot)),
                accounts:ix.projections.clone()}
        };
        let simulation_started=Instant::now();
        let response=rpc.call(operation).await;
        let simulation_ms=simulation_started.elapsed().as_millis();
        report.simulation_rpc_ms=Some(report.simulation_rpc_ms.unwrap_or(0).saturating_add(simulation_ms.min(u128::from(u64::MAX)) as u64));
        let response=response?;
        crate::log_event!(Debug,"v10 timing request_id={:?} fast={} build_us={build_us} simulation_rpc_ms={simulation_ms} provider={}",
            request.id,settings.fast,response.provider);
        let RpcAnswer::Simulation{slot,value}=response.answer else {bail!("unexpected simulation response")};
        report.context_slot=Some(slot); report.provider=Some(response.provider);
        if !settings.fast && attempt==0 && value.get("err").and_then(Value::as_str)==Some("BlockhashNotFound") {
            report.blockhash_retried=true; continue;
        }
        report.executor_invoked=value.get("logs").and_then(Value::as_array).is_some_and(|logs|
            logs.iter().filter_map(Value::as_str).any(|s|s.starts_with(&format!("Program {EXECUTOR} invoke ["))));
        let trace_started=Instant::now();
        report.trace=observed_trace(&value,&signed.keys,&ix);
        report.dex_invocations=report.trace.calls.iter().map(|c|c.program.clone()).collect();
        report.trace_us=trace_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
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

/// Single-worker compatibility path for standalone V10 probes.
pub async fn run(settings: Settings, rpc: RpcHandle, input: mpsc::Receiver<ExecutionRequest>,
    output: mpsc::Sender<ExecutionReport>) -> Result<()> {
    run_parallel(settings,rpc,input,output,1).await.map(|_|())
}

/// CP9B: bounded concurrent read-only V10 simulations, sharing the single RPC
/// scheduler and signer. Completion order is intentionally not request order;
/// correlation is by request id. No transaction is ever broadcast.
#[derive(Debug,serde::Serialize)]
pub struct ParallelReport {
    pub configured_parallelism:usize,
    pub maximum_in_flight:usize,
    pub completed_requests:u64,
}
pub async fn run_parallel(settings: Settings,rpc:RpcHandle,mut input:mpsc::Receiver<ExecutionRequest>,
    output:mpsc::Sender<ExecutionReport>,parallelism:usize)->Result<ParallelReport>{
    ensure!((1..=64).contains(&parallelism),"GRAPH_V10_SIMULATION_PARALLELISM must be 1..64");
    // Check static chain/program identity once at worker startup only in signed mode.
    if !settings.fast {
        match rpc.call(RpcOperation::GenesisHash).await?.answer {
            RpcAnswer::GenesisHash(hash)=>ensure!(hash==MAINNET,"V10 worker expects mainnet genesis"),
            _=>bail!("unexpected genesis response"),
        }
        match rpc.call(RpcOperation::Account{address:key(EXECUTOR),view:AccountView::Slice{offset:0,length:0}}).await?.answer {
            RpcAnswer::Account{account:Some(account),..}=>ensure!(account.executable,"V10 program is not executable"),
            _=>bail!("V10 executor program account missing"),
        }
    }
    crate::log_event!(Info,"v10 started wallet={} mode={} parallelism={} broadcasts=false",
        settings.wallet(),if settings.fast {"simulation_fast"}else{"simulation_signed"},parallelism);
    let settings=Arc::new(settings);
    let mut tasks=JoinSet::<Result<()>>::new();
    let mut count=0_u64;
    let mut peak=0_usize;
    let mut input_closed=false;
    loop {
        if input_closed && tasks.is_empty(){break;}
        tokio::select! {
            completed=tasks.join_next(),if !tasks.is_empty()=>{
                completed.context("V10 concurrent task completion missing")?
                    .context("V10 concurrent simulation task panicked")??;
                count+=1;
            },
            request=input.recv(),if !input_closed && tasks.len()<parallelism=>{
                if let Some(request)=request{
                    let settings=settings.clone();
                    let rpc=rpc.clone();
                    let output=output.clone();
                    tasks.spawn(async move{
                        let started=Instant::now();
                        let mut report=ExecutionReport{id:request.id.clone(),outcome:ExecutionOutcome::Rejected,
                            phase:"validate".into(),error:None,metrics:None,simulated_signature:None,
                            blockhash:None,last_valid_block_height:None,context_slot:None,provider:None,
                            blockhash_retried:false,executor_invoked:false,dex_invocations:Vec::new(),simulation:None,
                            build_us:None,simulation_rpc_ms:None,elapsed_ms:0,trace_us:0,trace:ExecutionTrace::default()};
                        if let Err(error)=execute(&settings,&rpc,&request,&mut report).await {
                            report.error=Some(format!("{error:#}"));
                        }
                        report.elapsed_ms=started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                        crate::log_event!(Debug,"v10 result request_id={:?} outcome={:?} phase={} context_slot={:?} dex_calls={} elapsed_ms={} broadcasts=false",
                            report.id,report.outcome,report.phase,report.context_slot,
                            report.dex_invocations.len(),report.elapsed_ms);
                        output.send(report).await.context("V10 report consumer stopped")?;
                        Ok(())
                    });
                    peak=peak.max(tasks.len());
                }else{
                    input_closed=true;
                }
            }
        }
    }
    drop(output);
    crate::log_event!(Info,"v10 complete requests={count} peak_in_flight={peak} parallelism={parallelism} broadcasts=0");
    Ok(ParallelReport{configured_parallelism:parallelism,
        maximum_in_flight:peak,completed_requests:count})
}

// The pre-CP10 byte compiler is retained *only in tests*. This is an
// independent reference for ordering, duplicate promotion and exact v1 bytes.
#[cfg(test)]
fn legacy_message(ix: &mut Instruction, request: &ExecutionRequest,
    blockhash: &str) -> Result<(Vec<u8>,Vec<Pubkey>)> {
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
    ix.metrics.unique_accounts=keys.len(); ix.metrics.transaction_bytes=message.len()+64;
    Ok((message,keys))
}

#[cfg(test)]
mod cp10_tests {
    use super::*;
    use crate::core::{ExecutionManifest, MintProof};
    use crate::core::execution::ExecutionParameters;

    fn sample_pool(number:u8, account_count:usize, wallet:Pubkey,
        settlement:Pubkey, target:Pubkey, pump:bool) -> ExecutionPool {
        let pool=Pubkey::new_from_array([number;32]);
        let program=if pump {key(PUMP)} else {Pubkey::new_from_array([77;32])};
        let mut account_block=vec![
            AccountMeta{address:program,writable:false},
            AccountMeta{address:settlement,writable:false},
            AccountMeta{address:pool,writable:true},
        ];
        for i in 3..account_count {
            // Several shared accounts exercise dedup and writable promotion.
            account_block.push(AccountMeta{
                address:Pubkey::new_from_array([((number as usize+i)%220+30) as u8;32]),
                writable:i%3==0,
            });
        }
        let inverse=vec![
            AccountMeta{address:program,writable:false},
            AccountMeta{address:target,writable:false},
            AccountMeta{address:pool,writable:true},
        ];
        ExecutionPool {
            manifest:Arc::new(ExecutionManifest{pool,mint_a:settlement,mint_b:target,
                validated_slot:42,bound_signer:pump.then_some(wallet),
                base_a:Arc::from(account_block),base_b:Arc::from(inverse)}),
            mints:[
                MintProof{address:settlement,token_2022:false,extensions:vec![],unsupported:vec![]},
                MintProof{address:target,token_2022:false,extensions:vec![],unsupported:vec![]},
            ],
        }
    }
    fn request(n:usize,account_count:usize,settlement:Pubkey,pump:bool)->ExecutionRequest {
        let wallet=Pubkey::new_from_array([99;32]);
        let target=Pubkey::new_from_array([88;32]);
        let pools=(0..n).map(|i|sample_pool((i+1) as u8,account_count,wallet,
            settlement,target,pump)).collect();
        ExecutionRequest{id:"cp10-equivalence".into(),wallet,settlement,
            groups:vec![ExecutionGroup::Direct{target,pools}],
            conversion:None,parameters:ExecutionParameters::default()}
    }

    #[test]
    fn exact_legacy_transaction_compilation() {
        for settlement in [key(SOL),key(USDC)] {
            for pump in [false,true] {
                for count in [2_usize,3,4,8,12,16] {
                    for width in [3_usize,6,12,24] {
                        let request=request(count,width,settlement,pump);
                        let mut old=instruction(&request,request.wallet).unwrap();
                        let mut new=instruction(&request,request.wallet).unwrap();
                        let old_result=legacy_message(&mut old,&request,
                            "11111111111111111111111111111111");
                        let new_result=message(&mut new,&request,
                            "11111111111111111111111111111111");
                        assert_eq!(old_result.is_ok(),new_result.is_ok(),
                            "settlement={settlement} pump={pump} n={count} width={width}");
                        match (old_result,new_result) {
                            (Ok((old_bytes,old_keys)),Ok((new_bytes,new_keys)))=>{
                                assert_eq!(old_bytes,new_bytes);
                                assert_eq!(old_keys,new_keys);
                                let normal=validate(&request).unwrap();
                                assert_eq!(serde_json::to_value(&normal).unwrap(),
                                    serde_json::to_value(&new.metrics).unwrap());
                            },
                            (Err(old_err),Err(new_err))=>assert_eq!(old_err.to_string(),new_err.to_string()),
                            _=>unreachable!(),
                        }
                        assert_eq!(old.data,new.data);
                        assert_eq!(serde_json::to_value(&old.metrics).unwrap(),
                            serde_json::to_value(&new.metrics).unwrap());
                    }
                }
            }
        }
    }

    #[test]
    fn cached_addresses_equal_cryptographic_reference() {
        for name in [EXECUTOR,SOL,USDC,TOKEN,TOKEN_2022,ASSOCIATED,PUMP,MAINNET,
            COLLECTOR_USDC,COLLECTOR_SOL_FLASH,COLLECTOR_SOL_OTHER] {
            assert_eq!(key(name),name.parse::<Pubkey>().unwrap());
        }
        for settlement in [key(SOL),key(USDC)] {
            let raw=Pubkey::find_program_address(
                &[b"vault_token_account",settlement.as_ref()],&key(EXECUTOR)).0;
            let cached=if settlement==key(SOL){*FLASH_VAULT_SOL}else{*FLASH_VAULT_USDC};
            assert_eq!(raw,cached);
        }
        assert_eq!(*FLASH_AUTHORITY,Pubkey::find_program_address(
            &[b"vault_authority"],&key(EXECUTOR)).0);
        for seed in 0..64_u8 {
            let wallet=Pubkey::new_from_array([seed;32]);
            let mint=Pubkey::new_from_array([seed.wrapping_add(100);32]);
            let prog=if seed%2==0{key(TOKEN)}else{key(TOKEN_2022)};
            let original=Pubkey::find_program_address(
                &[wallet.as_ref(),prog.as_ref(),mint.as_ref()],&key(ASSOCIATED)).0;
            assert_eq!(ata(wallet,mint,prog),original);
            assert_eq!(ata(wallet,mint,prog),original);
        }
    }

    #[test]
    #[ignore = "local CPU microbenchmark only; time is not a correctness assertion"]
    fn compilation_benchmark() {
        let request=request(8,12,key(SOL),true);
        let repetitions=20_000;
        let started=Instant::now();
        for _ in 0..repetitions {
            let mut ix=instruction(&request,request.wallet).unwrap();
            let _=legacy_message(&mut ix,&request,"11111111111111111111111111111111");
        }
        let legacy=started.elapsed();
        let started=Instant::now();
        for _ in 0..repetitions {
            let mut ix=instruction(&request,request.wallet).unwrap();
            let _=message(&mut ix,&request,"11111111111111111111111111111111");
        }
        let optimized=started.elapsed();
        println!("CP10 legacy={legacy:?} optimized={optimized:?} repetitions={repetitions}");
    }
}

#[cfg(test)]
mod cp14_one_sided_tests {
    use super::*;
    use crate::core::{ExecutionManifest,MintProof};
    #[test]
    fn cp14_v10_only_selects_validated_orca_direction() {
        let wallet=Pubkey::new_from_array([98;32]);
        let a=Pubkey::new_from_array([12;32]);
        let b=Pubkey::new_from_array([13;32]);
        let pool=Pubkey::new_from_array([14;32]);
        let program=key("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
        let mut base_a=vec![
            AccountMeta{address:program,writable:false},
            AccountMeta{address:a,writable:false},
            AccountMeta{address:Pubkey::new_from_array([20;32]),writable:false},
            AccountMeta{address:pool,writable:true},
            AccountMeta{address:Pubkey::new_from_array([21;32]),writable:true},
            AccountMeta{address:Pubkey::new_from_array([22;32]),writable:true},
            AccountMeta{address:Pubkey::new_from_array([23;32]),writable:true},
        ];
        let mut base_b=base_a.clone();
        base_b[1].address=b;
        base_b.push(AccountMeta{address:Pubkey::new_from_array([24;32]),writable:true});
        let execution=ExecutionPool {
            manifest:Arc::new(ExecutionManifest{
                pool,mint_a:a,mint_b:b,validated_slot:42,bound_signer:None,
                base_a:Arc::from(base_a.clone()),base_b:Arc::from(base_b)}),
            mints:[
                MintProof{address:a,token_2022:false,extensions:vec![],unsupported:vec![]},
                MintProof{address:b,token_2022:false,extensions:vec![],unsupported:vec![]},
            ],
        };
        assert!(block(&execution,a,b,wallet).is_err());
        assert_eq!(block(&execution,b,a,wallet).unwrap().len(),8);
        base_a.push(AccountMeta{address:Pubkey::new_from_array([25;32]),writable:true});
    }
}
