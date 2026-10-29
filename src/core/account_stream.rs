//! Source-independent account bytes and watch control. No pool or price interpretation.
use std::{collections::HashSet,sync::{Arc,atomic::{AtomicU64,Ordering}}};
use anyhow::{Result,ensure};
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast,watch};

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum AccountView { Full, Slice {offset:usize,length:usize} }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct AccountWatch {pub address:Pubkey,pub view:AccountView}
#[derive(Clone,Debug)]
pub struct AccountData {
    pub address:Pubkey,pub owner:Pubkey,pub executable:bool,pub bytes:Vec<u8>,
    pub view:AccountView,pub original_len:Option<usize>,
}

/// Account bytes are immutable and shared; receiving an event never clones its byte buffer.
/// Source gap and per-account slot are explicit; timestamps measure local processing only.
#[derive(Clone,Debug)]
pub enum AccountEvent {
    /// Desired source assignment, before its subscription is acknowledged.
    Assigned {provider:String,addresses:Arc<[Pubkey]>},
    Subscribed {provider:String,address:Pubkey},
    Unsubscribed {provider:String,address:Pubkey},
    Account {provider:String,address:Pubkey,slot:u64,
        frame_received_at_us:u64,decoded_at_us:u64,
        value:Option<Arc<AccountData>>},
    Gap {provider:String,reason:&'static str},
    Rejected {provider:String,address:Pubkey,code:i64},
}
pub type AccountEventStream=broadcast::Receiver<AccountEvent>;

/// The single desired watch set. Commitment and endpoint credentials are source configuration.
#[derive(Clone)]
pub struct AccountStreamControl {
    pub(crate) targets:watch::Sender<Vec<AccountWatch>>,
    pub(crate) capacity:usize,
    pub(crate) messages:Arc<AtomicU64>,pub(crate) text_bytes:Arc<AtomicU64>,
}
impl AccountStreamControl {
    pub fn capacity(&self)->usize {self.capacity}
    pub fn replace(&self,watches:Vec<AccountWatch>)->Result<()> {
        ensure!(watches.len()<=self.capacity,"account watch capacity exceeded: required={} configured={}",watches.len(),self.capacity);
        let mut seen=HashSet::with_capacity(watches.len());
        for w in &watches {
            ensure!(seen.insert(w.address),"duplicate watched account");
            if let AccountView::Slice{offset,length}=w.view {
                ensure!(length>0 && offset.checked_add(length).is_some(),"invalid watch slice");
            }
        }
        self.targets.send(watches).map_err(|_|anyhow::anyhow!("account stream stopped"))
    }
    /// Counts source text messages (including ACKs), not just account notifications.
    pub fn stream_counts(&self)->(u64,u64) {
        (self.messages.load(Ordering::Relaxed),self.text_bytes.load(Ordering::Relaxed))
    }
}
