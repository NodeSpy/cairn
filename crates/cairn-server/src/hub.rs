//! Change fan-out to subscribers. Publish happens only after the SQLite
//! commit, in seq order, from the single writer thread.

use cairn_proto::Change;
use parking_lot::Mutex;
use std::collections::HashMap;
use tokio::sync::mpsc;

struct Sub {
    id: u64,
    tx: mpsc::Sender<Vec<Change>>,
}

#[derive(Default)]
struct RootHub {
    head: u64,
    subs: Vec<Sub>,
}

#[derive(Default)]
pub struct Hub {
    inner: Mutex<(u64, HashMap<i64, RootHub>)>,
}

pub const SUB_QUEUE: usize = 4096;

impl Hub {
    pub fn publish(&self, root: i64, head: u64, changes: Vec<Change>) {
        let mut g = self.inner.lock();
        let r = g.1.entry(root).or_default();
        r.head = r.head.max(head);
        if changes.is_empty() {
            return;
        }
        // A subscriber whose queue is full (or gone) is dropped; its
        // connection notices the closed channel and pushes ResyncRequired.
        r.subs.retain(|s| s.tx.try_send(changes.clone()).is_ok());
    }

    /// Register a subscriber. Returns (from, id, rx): the receiver yields
    /// every change with seq > `from` (possibly with a few duplicates at the
    /// start, which the consumer filters by seq); the backlog up to `from`
    /// must be read from the log.
    pub fn subscribe(&self, root: i64, db_head: u64) -> (u64, u64, mpsc::Receiver<Vec<Change>>) {
        let mut g = self.inner.lock();
        g.0 += 1;
        let id = g.0;
        let r = g.1.entry(root).or_default();
        let from = r.head.max(db_head);
        r.head = from;
        let (tx, rx) = mpsc::channel(SUB_QUEUE);
        r.subs.push(Sub { id, tx });
        (from, id, rx)
    }

    pub fn unsubscribe(&self, root: i64, id: u64) {
        if let Some(r) = self.inner.lock().1.get_mut(&root) {
            r.subs.retain(|s| s.id != id);
        }
    }
}
