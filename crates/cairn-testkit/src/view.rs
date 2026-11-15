//! Namespace views, replay of change logs, normalization and diffing.

use cairn_proto::*;
use std::collections::{BTreeMap, HashMap};

/// Comparable attrs: no times (server/clients stamp them independently).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NAttrs {
    pub kind: Kind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub rdev: u64,
    pub content: Option<ContentId>,
    pub symlink_target: Option<Vec<u8>>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

pub fn nattrs(a: &Attrs) -> NAttrs {
    NAttrs { kind: a.kind, mode: a.mode, uid: a.uid, gid: a.gid, size: a.size, rdev: a.rdev, content: a.content, symlink_target: a.symlink_target.clone(), xattrs: a.xattrs.clone() }
}

/// (parent, name) -> (node, attrs)
pub type Ns = BTreeMap<(NodeId, Vec<u8>), (NodeId, NAttrs)>;

pub fn from_rows(rows: &[SnapshotRow]) -> Ns {
    rows.iter().map(|r| ((r.parent, r.name.0.clone()), (r.node, nattrs(&r.attrs)))).collect()
}

/// Apply a change log in seq order to an empty root (the core invariant).
pub fn replay(changes: &[Change]) -> Ns {
    let mut attrs: HashMap<NodeId, Attrs> = HashMap::new();
    let mut dirents: BTreeMap<(NodeId, Vec<u8>), NodeId> = BTreeMap::new();
    for c in changes {
        match &c.effect {
            Effect::Upsert { parent, name, node, attrs: a } => {
                dirents.insert((*parent, name.0.clone()), *node);
                attrs.insert(*node, a.clone());
            }
            Effect::Attrs { node, attrs: a } => {
                attrs.insert(*node, a.clone());
            }
            Effect::Remove { parent, name, node, node_deleted } => {
                dirents.remove(&(*parent, name.0.clone()));
                if *node_deleted {
                    attrs.remove(node);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                dirents.remove(&(*src_parent, src_name.0.clone()));
                dirents.insert((*dst_parent, dst_name.0.clone()), *node);
            }
        }
    }
    dirents.into_iter().filter_map(|(k, n)| attrs.get(&n).map(|a| (k, (n, nattrs(a))))).collect()
}

/// Strip the timestamp (and collision suffix) from conflict names:
/// `a.conflict-h3-20261113T101010[-2]` -> `a.conflict-h3`. Servers may
/// format the host part differently, so normalize to `.conflict-`.
pub fn norm_name(n: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(n);
    match s.find(".conflict-") {
        Some(i) => format!("{}.conflict-", &s[..i]).into_bytes(),
        None => n.to_vec(),
    }
}

/// Canonical form for comparing namespaces across implementations:
/// conflict names normalized, server-minted (HostId 0) node ids renamed to
/// their order of first appearance in `order` (the server-id list).
pub fn canon(ns: &Ns, minted_order: &[NodeId]) -> BTreeMap<(NodeId, Vec<u8>), Vec<(NodeId, NAttrs)>> {
    let map: HashMap<NodeId, NodeId> = minted_order.iter().enumerate().map(|(i, n)| (*n, NodeId::new(HostId(0), i as u64 + 1))).collect();
    let f = |n: NodeId| map.get(&n).copied().unwrap_or(n);
    let mut out: BTreeMap<(NodeId, Vec<u8>), Vec<(NodeId, NAttrs)>> = BTreeMap::new();
    for ((p, name), (n, a)) in ns {
        out.entry((f(*p), norm_name(name))).or_default().push((f(*n), a.clone()));
    }
    for v in out.values_mut() {
        v.sort_by_key(|(n, _)| *n);
    }
    out
}

/// Server-minted node ids in order of first appearance in a change log.
pub fn minted_order(changes: &[Change]) -> Vec<NodeId> {
    let mut v = vec![];
    for c in changes {
        if let Effect::Upsert { node, .. } = &c.effect {
            if node.host() == HostId(0) && *node != NodeId::ROOT && !v.contains(node) {
                v.push(*node);
            }
        }
    }
    v
}

/// Human-readable diff of two namespaces; None if equal.
pub fn diff(a_name: &str, a: &Ns, a_min: &[NodeId], b_name: &str, b: &Ns, b_min: &[NodeId]) -> Option<String> {
    let (ca, cb) = (canon(a, a_min), canon(b, b_min));
    if ca == cb {
        return None;
    }
    let mut s = String::new();
    let keys: std::collections::BTreeSet<_> = ca.keys().chain(cb.keys()).collect();
    for k in keys {
        if ca.get(k) != cb.get(k) {
            s += &format!("  {:?}/{:?}:\n    {a_name}: {:?}\n    {b_name}: {:?}\n", k.0, String::from_utf8_lossy(&k.1), ca.get(k), cb.get(k));
        }
    }
    Some(s)
}
