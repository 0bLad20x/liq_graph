//! Registry snapshots/updates + explicit executor responses -> scheduled checks.
//! The Registry remains the only pool-state writer. No protocol evidence decoding,
//! RPC, database access, price sourcing or graph policy lives in this worker.
use std::{collections::{BTreeSet, HashMap, HashSet}, env, sync::Arc, time::Duration};
use anyhow::{anyhow, bail, ensure, Context, Result};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc, watch};
use crate::core::{now_unix_ms, CompletionRequest, CompletionRequestBatch,
    CompletionResponseBatch, CompletionOutcome, CompletionResult, CompletionResultBatch,
    CompletionReason, CompletionState, CompletionStatus, CompletionWork,
    PoolState, PoolUpdateBatch, Venue};

pub struct Executor {
    pub venue: Venue,
    pub requests: mpsc::Sender<CompletionRequestBatch>,
    pub batch_size: usize,
    pub max_in_flight: usize,
    /// Required executor configuration is present in this process. This can
    /// reactivate historical ConfigurationRequired proofs without decoding evidence.
    pub configuration_ready: bool,
}

#[derive(Clone, Copy)]
pub struct Settings {
    refresh_ms: u64,
    recheck_ms: u64,
    full_refresh_ms: u64,
    recheck_paused_once: bool,
    retry_ms: u64,
    retry_max_ms: u64,
    acknowledgement_ms: u64,
    attempt_timeout: Duration,
    /// A finite standalone pass stops after source drain or this attempt limit.
    pub finite: bool,
    pub attempt_limit: usize,
    /// Optional fixed real-pool cohort per executor for live inspection. Zero: all.
    pub pool_limit: usize,
}
fn seconds(key:&str,default:u64)->Result<u64> {
    let value=env::var(key).map_or(Ok(default),|v|v.parse::<u64>())
        .with_context(||format!("invalid {key}"))?;
    ensure!((1..=86400).contains(&value),"{key} must be 1..86400");
    Ok(value*1000)
}
impl Settings {
    pub fn from_env(finite:bool,attempt_limit:usize)->Result<Self> {
        let retry_ms=seconds("COMPLETION_LIFECYCLE_RETRY_SECONDS",60)?;
        let retry_max_ms=seconds("COMPLETION_LIFECYCLE_RETRY_MAX_SECONDS",900)?;
        ensure!(retry_max_ms>=retry_ms,"lifecycle retry maximum is below initial retry");
        let pool_limit=env::var("COMPLETION_LIFECYCLE_POOL_LIMIT")
            .unwrap_or_else(|_|"0".into()).parse::<usize>()
            .context("invalid COMPLETION_LIFECYCLE_POOL_LIMIT")?;
        ensure!(pool_limit<=100000,"lifecycle pool limit must be 0..100000");
        let recheck_paused_once=match env::var("COMPLETION_LIFECYCLE_RECHECK_PAUSED_ONCE") {
            Err(env::VarError::NotPresent)=>false,
            Ok(v) if v=="true"||v=="1"=>true,
            Ok(v) if v=="false"||v=="0"=>false,
            _=>bail!("COMPLETION_LIFECYCLE_RECHECK_PAUSED_ONCE must be true or false"),
        };
        if recheck_paused_once {
            ensure!(pool_limit>0 && env::var("COMPLETION_LIFECYCLE_LIVE_VENUE").ok().as_deref()==Some("byreal"),
                "paused legacy Byreal recheck is allowed only in bounded Byreal live mode");
        }
        Ok(Self {
            recheck_paused_once,
            refresh_ms:seconds("COMPLETION_LIFECYCLE_REFRESH_SECONDS",900)?,
            recheck_ms:seconds("COMPLETION_LIFECYCLE_RECHECK_SECONDS",300)?,
            full_refresh_ms:seconds("COMPLETION_LIFECYCLE_FULL_REFRESH_SECONDS",3600)?,
            retry_ms,retry_max_ms,
            acknowledgement_ms:seconds("COMPLETION_LIFECYCLE_ACK_SECONDS",120)?,
            attempt_timeout:Duration::from_millis(seconds("COMPLETION_LIFECYCLE_ATTEMPT_SECONDS",180)?),
            finite,attempt_limit,pool_limit,
        })
    }
}

struct Flight {
    id:u64,
    work:CompletionWork,
    previous_checked:Option<u64>,
    previous_slot:Option<u64>,
    sent_at:u64,
    /// Some means the executor finished; await the Registry's accepted update.
    terminal:Option<CompletionResult>,
}
struct Entry {
    pool:PoolState,
    due:Option<u64>,
    work:CompletionWork,
    failures:u32,
    flight:Option<Flight>,
    // Most recent timed-out request for this pool; older executor replies must not overwrite proofs.
    last_expired:Option<u64>,
    last_light_checked_at_ms:Option<u64>,
}
#[derive(Default)]
struct Counters {
    dispatched:u64,
    accepted:u64,
    prepared:u64,
    incomplete:u64,
    unchanged_prepared:u64,
    unchanged_incomplete:u64,
    retryable:u64,
    non_advancing:u64,
    refreshes:u64,
    repeated_refreshes:u64,
    withdrawals:u64,
    withdrawn_in_flight:u64,
    timeouts:u64,
    late_responses:u64,
}
struct Scheduler {
    entries:HashMap<Pubkey,Entry>,
    due:BTreeSet<(u64,Pubkey)>,
    selected:Vec<(Venue,HashSet<Pubkey>)>,
    refreshed:HashSet<Pubkey>,
    next_id:u64,
    counters:Counters,
}
impl Scheduler {
    fn new()->Self {
        Self{entries:HashMap::new(),due:BTreeSet::new(),selected:Vec::new(),
            refreshed:HashSet::new(),next_id:0,counters:Counters::default()}
    }
    fn recheckable(reason:Option<CompletionReason>,configuration_ready:bool)->bool {
        reason.is_none_or(|reason|reason.recheckable()
            || (reason==CompletionReason::ConfigurationRequired && configuration_ready))
    }
    fn plan(pool:&PoolState,configuration_ready:bool,settings:Settings,now:u64)->(CompletionWork,Option<u64>) {
        if !pool.completion_admission.is_admitted() {
            return (CompletionWork::Recheck,None);
        }
        match pool.completion_state {
            CompletionState::Unknown=>(CompletionWork::Initial,Some(now)),
            CompletionState::Prepared=>(CompletionWork::Refresh,
                Some(pool.completion_checked_at_ms.map_or(now,|at|at.saturating_add(settings.refresh_ms)))),
            CompletionState::Incomplete=>{
                if !Self::recheckable(pool.completion_reason_code,configuration_ready) {
                    (CompletionWork::Recheck,None)
                } else {
                    (CompletionWork::Recheck,Some(pool.completion_checked_at_ms
                        .map_or(now,|at|at.saturating_add(settings.recheck_ms))))
                }
            },
        }
    }
    fn observe(&mut self,pool:&PoolState,executor:Option<&Executor>,settings:Settings,now:u64) {
        let managed=executor.is_some();
        let configuration_ready=executor.is_some_and(|executor|executor.configuration_ready);
        let key=pool.address;
        if !self.entries.contains_key(&key) {
            if !pool.completion_admission.is_admitted(){return;}
            let legacy_byreal=settings.recheck_paused_once && pool.venue==Venue::ByrealClmm
                && pool.completion_state==CompletionState::Incomplete
                && pool.completion_reason_code==Some(CompletionReason::UnsupportedConfiguration);
            if managed && settings.pool_limit>0 {
                if !self.selected.iter().any(|(venue,_)|*venue==pool.venue) {
                    self.selected.push((pool.venue,HashSet::new()));
                }
                let selected=&mut self.selected.iter_mut()
                    .find(|(venue,_)|*venue==pool.venue).expect("registered cohort").1;
                // A paused unsupported pool must not consume the finite cohort.
                if !selected.contains(&key) {
                    if (!legacy_byreal && !Self::recheckable(pool.completion_reason_code,configuration_ready))
                        || selected.len()>=settings.pool_limit{return;}
                    selected.insert(key);
                }
            }
            let (work,due)=if legacy_byreal{(CompletionWork::Recheck,Some(now))}
                else{Self::plan(pool,configuration_ready,settings,now)};
            crate::log_event!(Debug,"lifecycle plan pool={} venue={:?} executing={managed} work={work:?} due_at_ms={due:?}",key,pool.venue);
            self.entries.insert(key,Entry{pool:pool.clone(),due,work,failures:0,flight:None,last_expired:None,last_light_checked_at_ms:None});
            if managed {if let Some(at)=due{self.due.insert((at,key));}}
            return;
        }
        let entry=self.entries.get_mut(&key).expect("known lifecycle pool");
        if let Some(at)=entry.due {self.due.remove(&(at,key));}
        let proof_changed=entry.pool.completion_checked_at_ms!=pool.completion_checked_at_ms
            || entry.pool.completion_validated_slot!=pool.completion_validated_slot
            || entry.pool.completion_state!=pool.completion_state;
        let policy_changed=entry.pool.completion_admission!=pool.completion_admission
            || entry.pool.completion_reason_code!=pool.completion_reason_code;
        let reactivated=!entry.pool.completion_admission.is_admitted()
            && pool.completion_admission.is_admitted();
        if entry.pool.completion_admission.is_admitted() && !pool.completion_admission.is_admitted() {
            self.counters.withdrawals+=1;
            crate::log_event!(Info,"lifecycle admission_withdrawn pool={} venue={:?} in_flight={}",
                key,pool.venue,entry.flight.is_some());
        }
        entry.pool=pool.clone();
        if proof_changed {entry.last_light_checked_at_ms=None;}
        let acknowledged=entry.flight.as_ref().and_then(|f|f.terminal.as_ref())
            .is_some_and(|result|result.checked_at_ms==pool.completion_checked_at_ms
                && result.validated_slot==pool.completion_validated_slot
                && match result.status {
                    CompletionStatus::Prepared=>pool.completion_state==CompletionState::Prepared,
                    CompletionStatus::Incomplete=>pool.completion_state==CompletionState::Incomplete,
                    CompletionStatus::Retryable=>false,
                });
        if acknowledged {
            let flight=entry.flight.take().expect("acknowledged flight");
            let result=flight.terminal.expect("acknowledged result");
            self.counters.accepted+=1;
            match result.status {
                CompletionStatus::Prepared=>{
                    self.counters.prepared+=1;
                    if flight.work==CompletionWork::Refresh {
                        self.counters.refreshes+=1;
                        if !self.refreshed.insert(key){self.counters.repeated_refreshes+=1;}
                    }
                },
                CompletionStatus::Incomplete=>self.counters.incomplete+=1,
                CompletionStatus::Retryable=>unreachable!(),
            }
            crate::log_event!(Info,
                "lifecycle accepted request_id={} pool={} pool_hex={} venue={:?} work={:?} status={:?} previous_checked_at_ms={:?} checked_at_ms={:?} previous_slot={:?} slot={:?} admission={:?}",
                flight.id,key,key.to_bytes().iter().map(|b|format!("{b:02x}")).collect::<String>(),
                pool.venue,flight.work,result.status,flight.previous_checked,
                pool.completion_checked_at_ms,flight.previous_slot,pool.completion_validated_slot,pool.completion_admission);
            entry.failures=0;
        }
        if entry.flight.is_none() {
            if reactivated || proof_changed {entry.failures=0;}
            if acknowledged || proof_changed || reactivated || (policy_changed && entry.failures==0) {
                (entry.work,entry.due)=Self::plan(pool,configuration_ready,settings,now);
                if let Some(last)=entry.last_light_checked_at_ms {
                    let interval=if pool.completion_state==CompletionState::Prepared {settings.refresh_ms}else{settings.recheck_ms};
                    entry.due=entry.due.map(|due|due.max(last.saturating_add(interval)));
                }
            }
            if !pool.completion_admission.is_admitted() {
                self.entries.remove(&key);
                return;
            }
            if managed {if let Some(at)=entry.due{self.due.insert((at,key));}}
        }
    }
    fn retry(entry:&mut Entry,settings:Settings,now:u64) {
        entry.flight=None;
        entry.failures=entry.failures.saturating_add(1);
        entry.work=CompletionWork::Retry;
        let delay=settings.retry_ms.saturating_mul(1_u64<<entry.failures.saturating_sub(1).min(16))
            .min(settings.retry_max_ms);
        // Stable per-pool jitter spreads retries without external randomness.
        let jitter=u64::from(entry.pool.address.to_bytes()[0])*delay/2560;
        entry.due=entry.pool.completion_admission.is_admitted()
            .then_some(now.saturating_add(delay).saturating_add(jitter));
    }
    fn responses(&mut self,batch:CompletionResponseBatch,settings:Settings,now:u64)
        ->Result<CompletionResultBatch> {
        let mut forward=Vec::with_capacity(batch.responses.len());
        for response in batch.responses {
            let pool=response.outcome.pool();
            let Some(entry)=self.entries.get_mut(&pool) else {
                // A pool can lose admission while its executor request is still running.
                self.counters.late_responses+=1;
                crate::log_event!(Warn,
                    "lifecycle orphan_response_dropped request_id={} pool={} reason=pool_no_longer_tracked",
                    response.id,pool);
                continue;
            };
            if entry.last_expired.is_some_and(|expired|response.id<=expired) {
                // Its timeout was already accounted for and retried. Never forward an
                // expired completion result to the sole Registry writer.
                self.counters.late_responses+=1;
                crate::log_event!(Warn,
                    "lifecycle late_response_dropped request_id={} pool={} latest_expired_id={:?} proof_retained=true",
                    response.id,pool,entry.last_expired);
                continue;
            }
            let flight=entry.flight.as_ref().context("response for idle lifecycle pool")?;
            ensure!(flight.id==response.id && flight.terminal.is_none(),
                "duplicate or mismatched lifecycle response pool={}",pool);
            // Admission can be withdrawn while RPC reads are in flight.
            // Never advance or persist a completion for a withdrawn pool.
            if !entry.pool.completion_admission.is_admitted() {
                self.counters.withdrawn_in_flight+=1;
                self.entries.remove(&pool);
                crate::log_event!(Info,
                    "lifecycle withdrawn_response_dropped request_id={} pool={} proof_retained=true",
                    response.id,pool);
                continue;
            }
            if let CompletionOutcome::Unchanged(check)=&response.outcome {
                let baseline_matches=entry.pool.completion_validated_slot==Some(check.baseline_validated_slot)
                    && entry.pool.completion_checked_at_ms==Some(check.baseline_checked_at_ms)
                    && entry.pool.completion_state==check.baseline_state
                    && check.baseline_state!=CompletionState::Unknown
                    && check.observed_slot>check.baseline_validated_slot
                    && check.checked_at_ms>=check.baseline_checked_at_ms
                    && check.checked_at_ms<=now.saturating_add(5000)
                    && (check.baseline_state!=CompletionState::Prepared
                        || (entry.pool.execution_manifest.is_some() && entry.pool.mint_proofs.is_some()));
                if !baseline_matches {
                    self.counters.non_advancing+=1;
                    Self::retry(entry,settings,now);
                    if let Some(at)=entry.due{self.due.insert((at,pool));}
                    crate::log_event!(Warn,"lifecycle unchanged_baseline_mismatch request_id={} pool={} retry_at_ms={:?}",response.id,pool,entry.due);
                    continue;
                }
                let work=entry.flight.as_ref().expect("checked flight").work;
                entry.flight=None;
                entry.failures=0;
                entry.last_light_checked_at_ms=Some(check.checked_at_ms);
                self.counters.accepted+=1;
                match check.baseline_state {
                    CompletionState::Prepared=>{
                        self.counters.unchanged_prepared+=1;
                        if work==CompletionWork::Refresh {
                            self.counters.refreshes+=1;
                            if !self.refreshed.insert(pool){self.counters.repeated_refreshes+=1;}
                        }
                    },
                    CompletionState::Incomplete=>self.counters.unchanged_incomplete+=1,
                    CompletionState::Unknown=>unreachable!(),
                }
                let (next_work,normal_due)=Self::plan(&entry.pool,true,settings,now);
                entry.work=next_work;
                let interval=if check.baseline_state==CompletionState::Prepared {settings.refresh_ms}else{settings.recheck_ms};
                entry.due=normal_due.map(|due|due.max(check.checked_at_ms.saturating_add(interval)));
                if let Some(at)=entry.due{self.due.insert((at,pool));}
                crate::log_event!(Info,"lifecycle unchanged request_id={} pool={} venue={:?} work={:?} state={:?} baseline_slot={} observed_slot={} checked_at_ms={} next_due_ms={:?} db_write=false",
                    response.id,pool,entry.pool.venue,work,check.baseline_state,
                    check.baseline_validated_slot,check.observed_slot,check.checked_at_ms,entry.due);
                continue;
            }
            let CompletionOutcome::Proof(result)=response.outcome else{unreachable!()};
            if result.status==CompletionStatus::Retryable {
                ensure!(result.reason_code==Some(CompletionReason::TransientFailure)
                    && result.checked_at_ms.is_none() && result.manifest.is_none(),
                    "invalid transient lifecycle response");
                self.counters.retryable+=1;
                Self::retry(entry,settings,now);
                crate::log_event!(Info,"lifecycle retry request_id={} pool={} next_due_ms={:?} failures={} proof_retained=true",
                    response.id,pool,entry.due,entry.failures);
                if let Some(at)=entry.due{self.due.insert((at,pool));}
            } else if result.validated_slot.is_some_and(|slot|
                entry.pool.completion_validated_slot.is_some_and(|old|slot<=old)) {
                // Same-slot re-reads cannot renew the accepted proof. Wait for a
                // newer slot, without sending a known-stale write to the Registry.
                self.counters.non_advancing+=1;
                Self::retry(entry,settings,now);
                if let Some(at)=entry.due{self.due.insert((at,pool));}
                crate::log_event!(Info,"lifecycle non_advancing request_id={} pool={} slot={:?} next_due_ms={:?}",
                    response.id,pool,result.validated_slot,entry.due);
                continue;
            } else {
                let flight=entry.flight.as_mut().expect("validated lifecycle response");
                flight.sent_at=now;
                flight.terminal=Some(result.clone());
            }
            forward.push(result);
        }
        Ok(CompletionResultBatch{source:batch.source,results:forward})
    }
    fn dispatch(&mut self,executors:&[Executor],settings:Settings,now:u64)->Result<()> {
        for executor in executors {
            loop {
                let active=self.entries.values().filter(|e|
                    e.pool.venue==executor.venue && e.flight.is_some()).count();
                let remaining=if settings.attempt_limit==0 {usize::MAX}
                    else {settings.attempt_limit.saturating_sub(self.counters.dispatched as usize)};
                let capacity=executor.batch_size.min(executor.max_in_flight.saturating_sub(active)).min(remaining);
                if capacity==0{break;}
                let keys=self.due.iter().take_while(|(at,_)|*at<=now)
                    .filter_map(|(_,key)|self.entries.get(key)
                        .filter(|e|e.pool.venue==executor.venue).map(|_|*key))
                    .take(capacity).collect::<Vec<_>>();
                if keys.is_empty(){break;}
                let permit=match executor.requests.try_reserve() {
                    Ok(permit)=>permit,
                    Err(mpsc::error::TrySendError::Full(_))=>break,
                    Err(mpsc::error::TrySendError::Closed(_))=>bail!("lifecycle executor closed venue={:?}",executor.venue),
                };
                let mut requests=Vec::with_capacity(keys.len());
                for key in keys {
                    let entry=self.entries.get_mut(&key).expect("due lifecycle entry");
                    ensure!(entry.flight.is_none() && entry.pool.completion_admission.is_admitted(),
                        "lifecycle attempted duplicate or unqualified dispatch");
                    if let Some(at)=entry.due.take(){self.due.remove(&(at,key));}
                    self.next_id=self.next_id.checked_add(1).context("lifecycle request id overflow")?;
                    let id=self.next_id;
                    entry.flight=Some(Flight{id,work:entry.work,
                        previous_checked:entry.pool.completion_checked_at_ms,
                        previous_slot:entry.pool.completion_validated_slot,sent_at:now,terminal:None});
                    crate::log_event!(Debug,"lifecycle dispatch request_id={id} pool={key} venue={:?} work={:?}",
                        entry.pool.venue,entry.work);
                    let force_full=entry.pool.completion_state==CompletionState::Unknown
                        || entry.pool.completion_validated_slot.is_none()
                        || entry.pool.completion_checked_at_ms.is_none_or(|at|
                            now.saturating_sub(at)>=settings.full_refresh_ms);
                    requests.push(CompletionRequest{id,work:entry.work,force_full,pool:entry.pool.clone()});
                }
                self.counters.dispatched+=requests.len() as u64;
                permit.send(CompletionRequestBatch{requests,timeout:settings.attempt_timeout});
            }
        }
        Ok(())
    }
    fn report(&self,executors:&[Executor],now:u64,final_report:bool) {
        let in_flight=self.entries.values().filter(|e|e.flight.is_some()).count();
        let overdue=self.due.iter().take_while(|(at,_)|*at<=now).count();
        let oldest=self.due.first().map_or(0,|(at,_)|now.saturating_sub(*at));
        let c=&self.counters;
        crate::log_event!(Info,
            "lifecycle stats final={final_report} tracked={} in_flight={in_flight} overdue={overdue} oldest_overdue_ms={oldest} dispatched={} accepted={} prepared={} incomplete={} retryable={} non_advancing={} timeouts={} late_responses={} prepared_refreshes={} repeated_prepared_refreshes={} unchanged_prepared={} unchanged_incomplete={} admission_withdrawals={} withdrawn_in_flight={}",
            self.entries.len(),c.dispatched,c.accepted,c.prepared,c.incomplete,c.retryable,c.non_advancing,
            c.timeouts,c.late_responses,c.refreshes,c.repeated_refreshes,c.unchanged_prepared,c.unchanged_incomplete,c.withdrawals,c.withdrawn_in_flight);
        let mut venues:Vec<Venue>=self.entries.values().map(|e|e.pool.venue).collect();
        venues.sort_unstable_by_key(|v|format!("{v:?}"));venues.dedup();
        for venue in venues {
            let entries=self.entries.values().filter(|e|e.pool.venue==venue).collect::<Vec<_>>();
            crate::log_event!(Info,"lifecycle plan_summary venue={venue:?} executing={} pools={} initial={} refresh={} recheck={} retry={} paused={}",
                executors.iter().any(|e|e.venue==venue),entries.len(),
                entries.iter().filter(|e|e.due.is_some()&&e.work==CompletionWork::Initial).count(),
                entries.iter().filter(|e|e.due.is_some()&&e.work==CompletionWork::Refresh).count(),
                entries.iter().filter(|e|e.due.is_some()&&e.work==CompletionWork::Recheck).count(),
                entries.iter().filter(|e|e.due.is_some()&&e.work==CompletionWork::Retry).count(),
                entries.iter().filter(|e|e.due.is_none()&&e.flight.is_none()).count());
        }
    }
}

/// Subscribe before taking the seed snapshot in the orchestrator. Replaying an
/// unchanged proof never postpones a due refresh or resets transient backoff.
pub async fn run(seed:Vec<PoolState>,mut input:broadcast::Receiver<Arc<PoolUpdateBatch>>,
    mut responses:mpsc::Receiver<CompletionResponseBatch>,executors:Vec<Executor>,
    output:broadcast::Sender<Arc<CompletionResultBatch>>,source_done:watch::Receiver<bool>,
    mut stop:watch::Receiver<bool>,settings:Settings)->Result<()> {
    ensure!(!executors.is_empty(),"lifecycle requires at least one executor");
    ensure!(executors.iter().all(|e|e.batch_size>0&&e.max_in_flight>0),"invalid lifecycle capacity");
    let mut registered=Vec::new();
    for e in &executors {
        ensure!(!registered.contains(&e.venue),"duplicate lifecycle venue");registered.push(e.venue);
    }
    let mut scheduler=Scheduler::new();
    let now=now_unix_ms()?;
    for pool in seed {
        scheduler.observe(&pool,executors.iter().find(|e|e.venue==pool.venue),settings,now);
    }
    let configuration_ready=executors.iter().filter(|e|e.configuration_ready)
        .map(|e|e.venue).collect::<Vec<_>>();
    crate::log_event!(Info,"lifecycle started managed={registered:?} configuration_ready={configuration_ready:?} refresh_ms={} recheck_ms={} retry_ms={} retry_max_ms={} full_refresh_ms={} pool_limit={} attempt_limit={} finite={} max_age_enforced=false",
        settings.refresh_ms,settings.recheck_ms,settings.retry_ms,settings.retry_max_ms,
        settings.full_refresh_ms,settings.pool_limit,settings.attempt_limit,settings.finite);
    let mut input_open=true;
    let mut stopping=*stop.borrow();
    let mut pulse=tokio::time::interval(Duration::from_millis(250));
    pulse.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut report=tokio::time::interval(Duration::from_secs(15));
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let now=now_unix_ms()?;
        stopping|=*stop.borrow();
        if !stopping {scheduler.dispatch(&executors,settings,now)?;}
        let active=scheduler.entries.values().filter(|e|e.flight.is_some()).count();
        let limit=settings.attempt_limit>0 && scheduler.counters.dispatched>=settings.attempt_limit as u64;
        let finite_drained=settings.finite && (*source_done.borrow()||!input_open) && input.is_empty()
            && scheduler.due.first().is_none_or(|(at,_)|*at>now);
        if active==0 && (stopping||limit||finite_drained){break;}
        // A single slow executor or Registry acknowledgement is a pool-local
        // failure, not a reason to kill discovery, pricing, graph and execution.
        // Preserve the last accepted proof and use the existing bounded backoff.
        let mut expired=Vec::new();
        for (pool,entry) in &mut scheduler.entries {
            if let Some(flight)=&entry.flight {
                let timeout=if flight.terminal.is_some(){settings.acknowledgement_ms}
                    else{settings.attempt_timeout.as_millis() as u64+30000};
                let age=now.saturating_sub(flight.sent_at);
                if age>timeout {
                    let id=flight.id;
                    let awaiting_registry=flight.terminal.is_some();
                    entry.last_expired=Some(id);
                    Scheduler::retry(entry,settings,now);
                    expired.push((*pool,id,entry.pool.venue,age,awaiting_registry,entry.due));
                }
            }
        }
        for (pool,id,venue,age,awaiting_registry,due) in expired {
            scheduler.counters.timeouts+=1;
            if let Some(at)=due {scheduler.due.insert((at,pool));}
            crate::log_event!(Warn,
                "lifecycle request_timeout request_id={id} pool={pool} venue={venue:?} elapsed_ms={age} awaiting_registry={awaiting_registry} retry_at_ms={due:?} proof_retained=true");
        }
        // A withdrawn pool whose final RPC/ack timeout elapsed must not remain
        // as an idle entry until a further external update happens.
        scheduler.entries.retain(|_,entry|
            entry.flight.is_some() || entry.pool.completion_admission.is_admitted());
        tokio::select! {
            update=input.recv(),if input_open=>match update {
                Ok(batch)=>{
                    let now=now_unix_ms()?;
                    for pool in &batch.pools {
                        scheduler.observe(pool,executors.iter().find(|e|e.venue==pool.venue),settings,now);
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n))=>bail!("lifecycle lost {n} Registry batches"),
                Err(broadcast::error::RecvError::Closed)=>{
                    input_open=false;
                    ensure!(scheduler.entries.values().all(|e|e.flight.is_none()),
                        "Registry closed with lifecycle requests in flight");
                    stopping=true;
                },
            },
            batch=responses.recv(),if active>0=>{
                let batch=batch.ok_or_else(||anyhow!("lifecycle executor response channel closed"))?;
                let results=scheduler.responses(batch,settings,now_unix_ms()?)?;
                if !results.results.is_empty() {
                    output.send(Arc::new(results)).map_err(|_|anyhow!("lifecycle has no Registry consumer"))?;
                }
            },
            changed=stop.changed(),if !stopping=>{
                stopping=changed.is_err()||*stop.borrow();
            },
            _=pulse.tick()=>{},
            _=report.tick()=>scheduler.report(&executors,now_unix_ms()?,false),
        }
    }
    ensure!(scheduler.counters.dispatched==scheduler.counters.accepted
        +scheduler.counters.retryable+scheduler.counters.non_advancing
        +scheduler.counters.timeouts+scheduler.counters.withdrawn_in_flight,
        "lifecycle final accounting has unacknowledged requests");
    scheduler.report(&executors,now_unix_ms()?,true);
    Ok(())
}
