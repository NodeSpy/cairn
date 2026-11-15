//! The kernel-side inode table: which inodes the kernel currently holds (lookup count > 0), and
//! the NodeId -> Ino mapping the applier needs, because remote changes name nodes by `NodeId`.
//!
//! Core owns all file state; this table only mirrors what the kernel may have cached, which is
//! also exactly the set of objects that can have inotify watchers.

use cairn_client::api::{Ino, ROOT_INO};
use cairn_proto::NodeId;
use parking_lot::Mutex;
use std::collections::HashMap;

struct Entry {
    nlookup: u64,
    node: NodeId,
}

#[derive(Default)]
struct Inner {
    by_ino: HashMap<Ino, Entry>,
    by_node: HashMap<NodeId, Ino>,
}

pub struct KTable(Mutex<Inner>);

impl Default for KTable {
    fn default() -> Self {
        Self::new()
    }
}

impl KTable {
    pub fn new() -> KTable {
        let mut i = Inner::default();
        // The root is never forgotten by the kernel.
        i.by_ino.insert(ROOT_INO, Entry { nlookup: 1 << 40, node: NodeId::ROOT });
        i.by_node.insert(NodeId::ROOT, ROOT_INO);
        KTable(Mutex::new(i))
    }

    /// The kernel got one more reference (lookup / create / mkdir / readdirplus entry ...).
    pub fn inc(&self, ino: Ino, node: NodeId) {
        let mut g = self.0.lock();
        let e = g.by_ino.entry(ino).or_insert(Entry { nlookup: 0, node });
        e.nlookup += 1;
        if e.node != node {
            // The ino was reused for another node.
            let old = e.node;
            e.node = node;
            if g.by_node.get(&old) == Some(&ino) {
                g.by_node.remove(&old);
            }
        }
        g.by_node.insert(node, ino);
    }

    pub fn forget(&self, ino: Ino, n: u64) {
        if ino == ROOT_INO {
            return;
        }
        let mut g = self.0.lock();
        let Some(e) = g.by_ino.get_mut(&ino) else { return };
        e.nlookup = e.nlookup.saturating_sub(n);
        if e.nlookup == 0 {
            let node = e.node;
            g.by_ino.remove(&ino);
            if g.by_node.get(&node) == Some(&ino) {
                g.by_node.remove(&node);
            }
        }
    }

    pub fn contains(&self, ino: Ino) -> bool {
        self.0.lock().by_ino.contains_key(&ino)
    }

    /// The ino of `node` if the kernel holds it.
    pub fn ino_of(&self, node: NodeId) -> Option<Ino> {
        self.0.lock().by_node.get(&node).copied()
    }

    pub fn len(&self) -> usize {
        self.0.lock().by_ino.len()
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts() {
        let t = KTable::new();
        let n = NodeId(7);
        assert_eq!(t.ino_of(n), None);
        t.inc(5, n);
        t.inc(5, n);
        assert_eq!(t.ino_of(n), Some(5));
        t.forget(5, 1);
        assert!(t.contains(5));
        t.forget(5, 1);
        assert!(!t.contains(5));
        assert_eq!(t.ino_of(n), None);
        assert_eq!(t.ino_of(NodeId::ROOT), Some(1));
        t.forget(1, 1 << 41);
        assert!(t.contains(1));
    }
}
