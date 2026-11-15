//! Write leases: in memory, exclusive per (root, node), die with the
//! holder's last connection.

use cairn_proto::{HostId, NodeId};
use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug)]
pub struct Lease {
    pub host: HostId,
    pub conn: u64,
    /// Root handle on `conn` (for Recall pushes).
    pub handle: u32,
}

#[derive(Default)]
pub struct Leases {
    map: Mutex<HashMap<(i64, NodeId), Lease>>,
    pub released: Notify,
}

pub enum Try {
    Granted,
    HeldBy(Lease),
}

impl Leases {
    pub fn holder(&self, root: i64, node: NodeId) -> Option<HostId> {
        self.map.lock().get(&(root, node)).map(|l| l.host)
    }
    /// Take the lease if free or already ours.
    pub fn try_acquire(&self, root: i64, node: NodeId, me: Lease) -> Try {
        let mut m = self.map.lock();
        match m.get(&(root, node)) {
            Some(l) if l.host != me.host => Try::HeldBy(*l),
            _ => {
                m.insert((root, node), me);
                Try::Granted
            }
        }
    }
    /// Take the lease whatever its state (after the break timeout).
    pub fn force(&self, root: i64, node: NodeId, me: Lease) {
        self.map.lock().insert((root, node), me);
    }
    pub fn release(&self, root: i64, node: NodeId, host: HostId) {
        let mut m = self.map.lock();
        if m.get(&(root, node)).map(|l| l.host) == Some(host) {
            m.remove(&(root, node));
            drop(m);
            self.released.notify_waiters();
        }
    }
    pub fn drop_host(&self, host: HostId) {
        self.map.lock().retain(|_, l| l.host != host);
        self.released.notify_waiters();
    }
    pub fn count(&self) -> usize {
        self.map.lock().len()
    }
}
