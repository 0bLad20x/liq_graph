//! CP20S: bounded advisory simulation scheduler; only the candidate worker owns
//! this state. No cross-worker messages, RPC, background task, or persistent log.
//! A frozen fingerprint is not a bank identity: short cooldowns permit rechecks.
use std::{collections::{HashMap,VecDeque},time::{Duration,Instant}};
use serde_json::{json,Value};
use crate::core::market_state::PoolPriceObservation;

pub(super) const COOLDOWN:Duration=Duration::from_secs(30);
const MAX_RECENT_KEYS:usize=8_192;
const MAX_HISTORY:usize=16_384;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(super) enum Familiarity {New, Cooldown, Recheck, Unverifiable}
impl Familiarity {
    pub(super) fn deferred(self)->bool{self==Self::Cooldown}
}

#[derive(Default)]
pub(super) struct Recent {
    last:HashMap<String,Instant>,
    trail:VecDeque<(String,Instant)>,
}
impl Recent {
    pub(super) fn classify(&self,key:Option<&str>,now:Instant)->Familiarity {
        let Some(key)=key else{return Familiarity::Unverifiable};
        match self.last.get(key) {
            None=>Familiarity::New,
            Some(last) if now.saturating_duration_since(*last)<COOLDOWN=>Familiarity::Cooldown,
            Some(_)=>Familiarity::Recheck,
        }
    }
    pub(super) fn record(&mut self,key:Option<String>,now:Instant){
        let Some(key)=key else{return};
        self.last.insert(key.clone(),now);
        self.trail.push_back((key,now));
        while self.last.len()>MAX_RECENT_KEYS || self.trail.len()>MAX_HISTORY {
            let Some((key,at))=self.trail.pop_front()else{break};
            if self.last.get(&key).is_some_and(|seen|*seen==at){
                self.last.remove(&key);
            }
        }
    }
    #[cfg(test)]
    fn len(&self)->usize {self.last.len()}
}

#[derive(Default)]
pub(super) struct Scheduler {
    pub(super) sources:Recent,
    pub(super) packs:Recent,
}
pub(super) fn sha256(data:&Value)->Option<String>{
    let bytes=serde_json::to_vec(data).ok()?;
    Some(ring::digest::digest(&ring::digest::SHA256,&bytes).as_ref().iter()
        .map(|b|format!("{b:02x}")).collect())
}
/// Direction-sensitive price/fee/account facts. Observation timestamps and
/// Solana slots are intentionally excluded. No known-on-chain state is invented.
pub(super) fn frozen_leg(obs:&PoolPriceObservation,
    role:&str,input:&str,output:&str)->Option<Value> {
    let l=obs.liquidity.as_ref()?;
    let v=obs.valuation.as_ref()?;
    Some(json!([
        obs.pool,role,input,output,obs.venue,
        l.content_digest,l.account_set_digest,
        v.dex_fee_rates,v.fees_on_input,v.base_fee_rate,
        v.variable_fee_rate,v.lp_fee_bps,v.protocol_fee_bps,
        v.creator_fee_bps,v.fee_schedule,v.token_fee_status,
    ]))
}
pub(super) fn fingerprint(kind:&str,settlement:&str,target:&str,
    legs:Vec<Value>,parameters:&crate::core::execution::ExecutionParameters)->Option<String>{
    sha256(&json!([kind,settlement,target,parameters,legs]))
}

#[cfg(test)]
mod cp20s_tests{
    use super::*;
    #[test]
    fn cp20s_frozen_keys_are_skipped_within_cooldown_then_rechecked(){
        let mut recent=Recent::default();
        let start=Instant::now();
        assert_eq!(recent.classify(Some("a"),start),Familiarity::New);
        recent.record(Some("a".into()),start);
        assert_eq!(recent.classify(Some("a"),start+Duration::from_secs(29)),Familiarity::Cooldown);
        assert_eq!(recent.classify(Some("a"),start+Duration::from_secs(30)),Familiarity::Recheck);
        assert_eq!(recent.classify(Some("b"),start),Familiarity::New);
        assert_eq!(recent.classify(None,start),Familiarity::Unverifiable);
    }
    #[test]
    fn cp20s_changed_snapshot_is_not_blocked_by_other_snapshot(){
        let mut recent=Recent::default();
        let now=Instant::now();
        recent.record(Some("pool-A-state-1".into()),now);
        assert_eq!(recent.classify(Some("pool-A-state-2"),now),Familiarity::New);
    }
    #[test]
    fn cp20s_bounded_memory_and_reinsertion_preserve_recent_keys(){
        let mut recent=Recent::default();
        let now=Instant::now();
        for i in 0..(MAX_RECENT_KEYS+100){
            recent.record(Some(format!("key-{i}")),now);
        }
        assert!(recent.len()<=MAX_RECENT_KEYS);
        assert_eq!(recent.classify(Some("key-0"),now),Familiarity::New);
        assert_eq!(recent.classify(Some(&format!("key-{}",MAX_RECENT_KEYS+99)),now),
            Familiarity::Cooldown);
        recent.record(Some("last".into()),now);
        recent.record(Some("last".into()),now+Duration::from_secs(31));
        assert_eq!(recent.classify(Some("last"),now+Duration::from_secs(31)),
            Familiarity::Cooldown);
    }
    #[test]
    fn cp20s_snapshot_key_is_order_and_fee_sensitive(){
        let params=crate::core::execution::ExecutionParameters::default();
        let a=vec![json!(["A","direct","S","T",0.01]),json!(["B","partner","T","S",0.02])];
        let b=vec![json!(["B","partner","T","S",0.02]),json!(["A","direct","S","T",0.01])];
        let c=vec![json!(["A","direct","S","T",0.03]),json!(["B","partner","T","S",0.02])];
        let fa=fingerprint("direct2","S","T",a,&params).unwrap();
        assert_ne!(fa,fingerprint("direct2","S","T",b,&params).unwrap());
        assert_ne!(fa,fingerprint("direct2","S","T",c,&params).unwrap());
        assert_ne!(fa,fingerprint("direct2","T","S",vec![],&params).unwrap());
    }
}