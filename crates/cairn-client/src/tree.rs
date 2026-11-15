//! The namespace tree and the deterministic, I/O-free planning core:
//! validation of local ops (errno), application of ops and server effects,
//! pending-op index, and reconciliation ("corrections") after a conflicted
//! commit result. Used for both the *synced* tree (server state at
//! `applied_seq`) and the *visible* tree (synced + pending ops).

use crate::api::{Errno, NewNode, XattrFlags};
use cairn_proto::*;
use std::collections::{BTreeMap, HashMap, HashSet};

pub type NameB = Vec<u8>;

#[derive(Clone, Debug)]
pub struct Node {
    pub attrs: Attrs,
    /// Node version (seq of last attr/content change) as last seen from the server.
    pub version: Seq,
    /// Content version (seq of last SetContent) as last seen from the server.
    pub cversion: Seq,
}

/// What changed since the last `take_dirty` (for persistence).
#[derive(Default, Clone, Debug)]
pub struct Dirty {
    pub nodes: HashSet<NodeId>,
    pub dirents: HashSet<(NodeId, NameB)>,
    pub listed: HashSet<NodeId>,
}

#[derive(Clone, Default)]
pub struct Tree {
    pub nodes: HashMap<NodeId, Node>,
    children: HashMap<NodeId, BTreeMap<NameB, NodeId>>,
    back: HashMap<NodeId, Vec<(NodeId, NameB)>>,
    /// Directories whose children are complete.
    pub listed: HashSet<NodeId>,
    orphans: Vec<NodeId>,
    pub dirty: Dirty,
}

pub fn now_ns() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

pub fn dir_attrs(now: i64) -> Attrs {
    Attrs { kind: Kind::Dir, mode: 0o755, uid: 0, gid: 0, size: 0, mtime_ns: now, ctime_ns: now, atime_ns: now, rdev: 0, content: None, symlink_target: None, xattrs: vec![] }
}

pub fn validate_name(name: &[u8]) -> Result<(), Errno> {
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') || name.contains(&0) {
        return Err(Errno::EINVAL);
    }
    if name.len() > 255 {
        return Err(Errno::ENAMETOOLONG);
    }
    Ok(())
}

/// Attribute equality ignoring atime/ctime (which servers and hosts stamp independently).
pub fn attrs_eq(a: &Attrs, b: &Attrs) -> bool {
    a.kind == b.kind
        && a.mode == b.mode
        && a.uid == b.uid
        && a.gid == b.gid
        && a.size == b.size
        && a.mtime_ns == b.mtime_ns
        && a.rdev == b.rdev
        && a.content == b.content
        && a.symlink_target == b.symlink_target
        && a.xattrs == b.xattrs
}

impl Tree {
    pub fn new(now: i64) -> Tree {
        let mut t = Tree::default();
        t.nodes.insert(NodeId::ROOT, Node { attrs: dir_attrs(now), version: Seq(0), cversion: Seq(0) });
        t
    }

    // ------------------------------------------------------------ queries
    pub fn node(&self, n: NodeId) -> Option<&Node> {
        self.nodes.get(&n)
    }
    pub fn lookup(&self, parent: NodeId, name: &[u8]) -> Option<NodeId> {
        self.children.get(&parent).and_then(|c| c.get(name)).copied()
    }
    pub fn children(&self, parent: NodeId) -> Option<&BTreeMap<NameB, NodeId>> {
        self.children.get(&parent)
    }
    pub fn is_empty_dir(&self, d: NodeId) -> bool {
        self.children.get(&d).map(|c| c.is_empty()).unwrap_or(true)
    }
    pub fn is_dir(&self, n: NodeId) -> bool {
        self.nodes.get(&n).map(|x| x.attrs.kind == Kind::Dir).unwrap_or(false)
    }
    pub fn dirents_of(&self, n: NodeId) -> &[(NodeId, NameB)] {
        self.back.get(&n).map(|v| v.as_slice()).unwrap_or(&[])
    }
    /// Reported link count: 2 + subdirectories for directories (POSIX), dirent count for others.
    pub fn nlink(&self, n: NodeId) -> u32 {
        if self.is_dir(n) {
            let subdirs = self.children.get(&n).map(|c| c.values().filter(|x| self.is_dir(**x)).count()).unwrap_or(0);
            2 + subdirs as u32
        } else {
            self.dirents_of(n).len().max(1) as u32
        }
    }
    /// Root-relative path components via the first dirent chain.
    pub fn path_of(&self, n: NodeId) -> Option<Vec<NameB>> {
        let mut out = vec![];
        let mut cur = n;
        for _ in 0..4096 {
            if cur == NodeId::ROOT {
                out.reverse();
                return Some(out);
            }
            let (p, name) = self.dirents_of(cur).first()?;
            out.push(name.clone());
            cur = *p;
        }
        None
    }
    pub fn is_ancestor_or_self(&self, anc: NodeId, n: NodeId) -> bool {
        let mut cur = n;
        for _ in 0..4096 {
            if cur == anc {
                return true;
            }
            if cur == NodeId::ROOT {
                return false;
            }
            match self.dirents_of(cur).first() {
                Some((p, _)) => cur = *p,
                None => return false,
            }
        }
        false
    }

    // ----------------------------------------------------------- mutation
    pub fn insert_node(&mut self, id: NodeId, node: Node) {
        self.nodes.insert(id, node);
        self.dirty.nodes.insert(id);
    }
    pub fn node_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.dirty.nodes.insert(id);
        self.nodes.get_mut(&id)
    }
    pub fn set_listed(&mut self, d: NodeId, v: bool) {
        let ch = if v { self.listed.insert(d) } else { self.listed.remove(&d) };
        if ch {
            self.dirty.listed.insert(d);
        }
    }
    /// Make `(parent,name)` point at `node` (replacing any occupant).
    pub fn set_dirent(&mut self, parent: NodeId, name: &[u8], node: NodeId) {
        let old = self.children.entry(parent).or_default().insert(name.to_vec(), node);
        self.dirty.dirents.insert((parent, name.to_vec()));
        if let Some(o) = old {
            if o == node {
                return;
            }
            self.drop_back(o, parent, name);
        }
        let b = self.back.entry(node).or_default();
        if !b.iter().any(|(p, n)| *p == parent && n == name) {
            b.push((parent, name.to_vec()));
        }
    }
    fn drop_back(&mut self, node: NodeId, parent: NodeId, name: &[u8]) {
        if let Some(b) = self.back.get_mut(&node) {
            b.retain(|(p, n)| !(*p == parent && n == name));
            if b.is_empty() {
                self.back.remove(&node);
                self.orphans.push(node);
            }
        } else {
            self.orphans.push(node);
        }
    }
    pub fn remove_dirent(&mut self, parent: NodeId, name: &[u8]) -> Option<NodeId> {
        let n = self.children.get_mut(&parent)?.remove(name)?;
        self.dirty.dirents.insert((parent, name.to_vec()));
        self.drop_back(n, parent, name);
        Some(n)
    }
    /// Delete a node, all dirents pointing at it, and (for directories) its subtree.
    pub fn remove_node(&mut self, n: NodeId) {
        if n == NodeId::ROOT {
            return;
        }
        let backs = self.back.remove(&n).unwrap_or_default();
        for (p, name) in backs {
            if let Some(c) = self.children.get_mut(&p) {
                c.remove(&name);
            }
            self.dirty.dirents.insert((p, name));
        }
        if let Some(ch) = self.children.remove(&n) {
            for (name, c) in ch {
                self.dirty.dirents.insert((n, name.clone()));
                if let Some(b) = self.back.get_mut(&c) {
                    b.retain(|(p, nm)| !(*p == n && *nm == name));
                    if b.is_empty() {
                        self.back.remove(&c);
                        self.orphans.push(c);
                    }
                }
            }
        }
        self.set_listed(n, false);
        if self.nodes.remove(&n).is_some() {
            self.dirty.nodes.insert(n);
        }
    }
    /// Nodes that lost their last dirent since the last `gc`.
    pub fn orphan_candidates(&self) -> Vec<NodeId> {
        self.orphans.iter().copied().filter(|n| *n != NodeId::ROOT && !self.back.contains_key(n) && self.nodes.contains_key(n)).collect()
    }
    /// Structural invariants (tests): children and back-references agree.
    pub fn check_invariants(&self) -> Result<(), String> {
        for (p, ch) in &self.children {
            for (name, c) in ch {
                if !self.back.get(c).map(|b| b.iter().any(|(bp, bn)| bp == p && bn == name)).unwrap_or(false) {
                    return Err(format!("missing back ref {p:?}/{name:?} -> {c:?}"));
                }
            }
        }
        for (n, b) in &self.back {
            for (p, name) in b {
                if self.lookup(*p, name) != Some(*n) {
                    return Err(format!("dangling back ref {n:?} <- {p:?}/{name:?}"));
                }
            }
        }
        Ok(())
    }
    /// Remove nodes left without any dirent (call after a complete batch of mutations).
    pub fn gc(&mut self, protect: &dyn Fn(NodeId) -> bool) {
        let cands = std::mem::take(&mut self.orphans);
        for n in cands {
            if n != NodeId::ROOT && !self.back.contains_key(&n) && self.nodes.contains_key(&n) && !protect(n) {
                self.remove_node(n);
            }
        }
    }

    fn update_attrs(&mut self, node: NodeId, attrs: &Attrs, seq: Seq) {
        if let Some(n) = self.nodes.get_mut(&node) {
            if n.attrs.content != attrs.content {
                n.cversion = seq;
            }
            n.attrs = attrs.clone();
            n.version = seq;
            self.dirty.nodes.insert(node);
        }
    }

    // ------------------------------------------------------ server effects
    /// Would applying `eff` change what a local observer sees (ignoring atime/ctime)?
    pub fn would_change(&self, eff: &Effect) -> bool {
        match eff {
            Effect::Upsert { parent, name, node, attrs } => {
                let known = self.nodes.contains_key(node);
                if !self.is_dir(*parent) {
                    return known && self.nodes.get(node).map(|n| !attrs_eq(&n.attrs, attrs)).unwrap_or(false);
                }
                if !known && !self.listed.contains(parent) {
                    return false;
                }
                self.lookup(*parent, &name.0) != Some(*node) || self.nodes.get(node).map(|n| !attrs_eq(&n.attrs, attrs)).unwrap_or(true)
            }
            Effect::Remove { parent, name, node, node_deleted } => self.lookup(*parent, &name.0) == Some(*node) || (*node_deleted && self.nodes.contains_key(node)),
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                self.lookup(*src_parent, &src_name.0) == Some(*node) || (self.nodes.contains_key(node) && self.is_dir(*dst_parent) && self.lookup(*dst_parent, &dst_name.0) != Some(*node))
            }
            Effect::Attrs { node, attrs } => self.nodes.get(node).map(|n| !attrs_eq(&n.attrs, attrs)).unwrap_or(false),
        }
    }

    /// Apply a server fact (idempotent). Lazy namespace: changes inside
    /// unlisted directories only touch nodes we already know.
    pub fn apply_effect(&mut self, eff: &Effect, seq: Seq) {
        match eff {
            Effect::Upsert { parent, name, node, attrs } => {
                let known = self.nodes.contains_key(node);
                if !self.is_dir(*parent) {
                    if known {
                        self.update_attrs(*node, attrs, seq);
                    }
                    return;
                }
                if !known && !self.listed.contains(parent) {
                    return;
                }
                self.set_dirent(*parent, &name.0, *node);
                if known {
                    self.update_attrs(*node, attrs, seq);
                } else {
                    self.insert_node(*node, Node { attrs: attrs.clone(), version: seq, cversion: seq });
                    if attrs.kind == Kind::Dir && seq.0 != 0 {
                        self.set_listed(*node, true);
                    }
                }
            }
            Effect::Remove { parent, name, node, node_deleted } => {
                if self.lookup(*parent, &name.0) == Some(*node) {
                    self.remove_dirent(*parent, &name.0);
                }
                if *node_deleted {
                    self.remove_node(*node);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                if self.lookup(*src_parent, &src_name.0) == Some(*node) {
                    self.remove_dirent(*src_parent, &src_name.0);
                }
                if self.nodes.contains_key(node) {
                    if self.is_dir(*dst_parent) {
                        self.set_dirent(*dst_parent, &dst_name.0, *node);
                    }
                } else if self.is_dir(*dst_parent) && self.listed.contains(dst_parent) {
                    // A node we never saw appeared in a listed dir: our listing is incomplete.
                    self.set_listed(*dst_parent, false);
                }
            }
            Effect::Attrs { node, attrs } => self.update_attrs(*node, attrs, seq),
        }
    }

    // ------------------------------------------------------- local ops
    fn touch_dir(&mut self, d: NodeId, now: i64) {
        if let Some(n) = self.node_mut(d) {
            n.attrs.mtime_ns = now;
            n.attrs.ctime_ns = now;
        }
    }

    /// Apply an op to this tree (the *visible* tree). Tolerant: used for
    /// live local ops (already validated) and for replaying the journal.
    pub fn apply_op(&mut self, op: &Op, now: i64) {
        match op {
            Op::Create { parent, name, node, attrs } => {
                if !self.is_dir(*parent) {
                    return;
                }
                self.set_dirent(*parent, &name.0, *node);
                self.insert_node(*node, Node { attrs: attrs.clone(), version: Seq(0), cversion: Seq(0) });
                if attrs.kind == Kind::Dir {
                    self.set_listed(*node, true);
                }
                self.touch_dir(*parent, now);
            }
            Op::Link { parent, name, node } => {
                if !self.is_dir(*parent) || !self.nodes.contains_key(node) {
                    return;
                }
                self.set_dirent(*parent, &name.0, *node);
                if let Some(n) = self.node_mut(*node) {
                    n.attrs.ctime_ns = now;
                }
                self.touch_dir(*parent, now);
            }
            Op::Unlink { parent, name, node, .. } => {
                if self.lookup(*parent, &name.0) == Some(*node) {
                    self.remove_dirent(*parent, &name.0);
                    if !self.back.contains_key(node) {
                        self.remove_node(*node);
                    } else if let Some(n) = self.node_mut(*node) {
                        n.attrs.ctime_ns = now;
                    }
                    self.touch_dir(*parent, now);
                }
            }
            Op::Rename { src_parent, src_name, node, dst_parent, dst_name, mode, .. } => {
                if !self.is_dir(*dst_parent) {
                    return;
                }
                let occ = self.lookup(*dst_parent, &dst_name.0);
                match mode {
                    RenameMode::Exchange => {
                        if let Some(x) = occ {
                            self.set_dirent(*src_parent, &src_name.0, x);
                            self.set_dirent(*dst_parent, &dst_name.0, *node);
                        }
                    }
                    _ => {
                        if occ == Some(*node) && self.lookup(*src_parent, &src_name.0) == Some(*node) {
                            // rename onto another link of itself: nothing happens
                            if (*src_parent, &src_name.0) != (*dst_parent, &dst_name.0) {
                                return;
                            }
                        }
                        if self.lookup(*src_parent, &src_name.0) == Some(*node) {
                            self.remove_dirent(*src_parent, &src_name.0);
                        }
                        if let Some(o) = occ {
                            if o != *node {
                                self.remove_dirent(*dst_parent, &dst_name.0);
                                if !self.back.contains_key(&o) {
                                    self.remove_node(o);
                                } else if let Some(x) = self.node_mut(o) {
                                    x.attrs.ctime_ns = now;
                                }
                            }
                        }
                        self.set_dirent(*dst_parent, &dst_name.0, *node);
                    }
                }
                if let Some(n) = self.node_mut(*node) {
                    n.attrs.ctime_ns = now;
                }
                self.touch_dir(*src_parent, now);
                self.touch_dir(*dst_parent, now);
            }
            Op::SetAttr { node, .. } | Op::SetXattr { node, .. } | Op::SetContent { node, .. } => {
                if let Some(n) = self.node_mut(*node) {
                    attr_op(&mut n.attrs, op, now);
                }
            }
        }
    }

    // ---------------------------------------------------------- planning
    pub fn plan_create(&self, parent: NodeId, name: &[u8], new: &NewNode, node: NodeId, now: i64) -> Result<Op, Errno> {
        validate_name(name)?;
        if !self.is_dir(parent) {
            return Err(if self.nodes.contains_key(&parent) { Errno::ENOTDIR } else { Errno::ENOENT });
        }
        if self.lookup(parent, name).is_some() {
            return Err(Errno::EEXIST);
        }
        let mut xattrs = new.xattrs.clone();
        xattrs.sort();
        let (size, content, target) = match new.kind {
            Kind::File => (0, Some(ContentId::Empty), None),
            Kind::Symlink => {
                let t = new.symlink_target.clone().ok_or(Errno::EINVAL)?;
                (t.len() as u64, None, Some(t))
            }
            _ => (0, None, None),
        };
        let attrs = Attrs { kind: new.kind, mode: new.mode & 0o7777, uid: new.uid, gid: new.gid, size, mtime_ns: now, ctime_ns: now, atime_ns: now, rdev: new.rdev, content, symlink_target: target, xattrs };
        Ok(Op::Create { parent, name: Name_(name), node, attrs })
    }

    pub fn plan_link(&self, node: NodeId, new_parent: NodeId, new_name: &[u8]) -> Result<Op, Errno> {
        validate_name(new_name)?;
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        if n.attrs.kind == Kind::Dir {
            return Err(Errno::EPERM);
        }
        if !self.is_dir(new_parent) {
            return Err(Errno::ENOTDIR);
        }
        if self.lookup(new_parent, new_name).is_some() {
            return Err(Errno::EEXIST);
        }
        if self.dirents_of(node).len() >= 65000 {
            return Err(Errno::EMLINK);
        }
        Ok(Op::Link { parent: new_parent, name: Name_(new_name), node })
    }

    pub fn plan_unlink(&self, parent: NodeId, name: &[u8]) -> Result<Op, Errno> {
        validate_name(name)?;
        if !self.is_dir(parent) {
            return Err(Errno::ENOTDIR);
        }
        let node = self.lookup(parent, name).ok_or(Errno::ENOENT)?;
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        if n.attrs.kind == Kind::Dir {
            return Err(Errno::EISDIR);
        }
        Ok(Op::Unlink { parent, name: Name_(name), node, base: n.version })
    }

    pub fn plan_rmdir(&self, parent: NodeId, name: &[u8]) -> Result<Op, Errno> {
        validate_name(name)?;
        if !self.is_dir(parent) {
            return Err(Errno::ENOTDIR);
        }
        let node = self.lookup(parent, name).ok_or(Errno::ENOENT)?;
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        if n.attrs.kind != Kind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if !self.is_empty_dir(node) {
            return Err(Errno::ENOTEMPTY);
        }
        Ok(Op::Unlink { parent, name: Name_(name), node, base: n.version })
    }

    /// `Ok(None)`: succeeds without doing anything (rename to itself).
    pub fn plan_rename(&self, sp: NodeId, sn: &[u8], dp: NodeId, dn: &[u8], mode: RenameMode) -> Result<Option<Op>, Errno> {
        validate_name(sn)?;
        validate_name(dn)?;
        if !self.is_dir(sp) || !self.is_dir(dp) {
            return Err(Errno::ENOTDIR);
        }
        let node = self.lookup(sp, sn).ok_or(Errno::ENOENT)?;
        let kind = self.nodes.get(&node).ok_or(Errno::ENOENT)?.attrs.kind;
        let occ = self.lookup(dp, dn);
        if occ == Some(node) && mode == RenameMode::NoReplace {
            return Err(Errno::EEXIST);
        }
        if occ == Some(node) && mode == RenameMode::Replace {
            return Ok(None);
        }
        if kind == Kind::Dir && self.is_ancestor_or_self(node, dp) {
            return Err(Errno::EINVAL);
        }
        match mode {
            RenameMode::NoReplace => {
                if occ.is_some() {
                    return Err(Errno::EEXIST);
                }
            }
            RenameMode::Replace => {
                if let Some(o) = occ {
                    let ok = self.nodes.get(&o).ok_or(Errno::ENOENT)?.attrs.kind;
                    match (kind == Kind::Dir, ok == Kind::Dir) {
                        (true, false) => return Err(Errno::ENOTDIR),
                        (false, true) => return Err(Errno::EISDIR),
                        (true, true) if !self.is_empty_dir(o) => return Err(Errno::ENOTEMPTY),
                        _ => {}
                    }
                }
            }
            RenameMode::Exchange => {
                let o = occ.ok_or(Errno::ENOENT)?;
                if self.is_dir(o) && self.is_ancestor_or_self(o, sp) {
                    return Err(Errno::EINVAL);
                }
                if o == node {
                    return Ok(None);
                }
            }
        }
        Ok(Some(Op::Rename { src_parent: sp, src_name: Name_(sn), node, dst_parent: dp, dst_name: Name_(dn), replaces: occ, mode }))
    }

    pub fn plan_setattr(&self, node: NodeId, changes: AttrChanges) -> Result<Op, Errno> {
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        Ok(Op::SetAttr { node, base: n.version, changes })
    }

    pub fn plan_setxattr(&self, node: NodeId, name: &[u8], value: &[u8], flags: XattrFlags) -> Result<Op, Errno> {
        if name.is_empty() {
            return Err(Errno::EINVAL);
        }
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        let has = n.attrs.xattrs.iter().any(|(k, _)| k == name);
        match flags {
            XattrFlags::Create if has => return Err(Errno::EEXIST),
            XattrFlags::Replace if !has => return Err(Errno::ENODATA),
            _ => {}
        }
        Ok(Op::SetXattr { node, name: name.to_vec(), value: Some(value.to_vec()) })
    }

    pub fn plan_removexattr(&self, node: NodeId, name: &[u8]) -> Result<Op, Errno> {
        let n = self.nodes.get(&node).ok_or(Errno::ENOENT)?;
        if !n.attrs.xattrs.iter().any(|(k, _)| k == name) {
            return Err(Errno::ENODATA);
        }
        Ok(Op::SetXattr { node, name: name.to_vec(), value: None })
    }
}

#[allow(non_snake_case)]
fn Name_(b: &[u8]) -> cairn_proto::Name {
    cairn_proto::Name(b.to_vec())
}

/// Apply an attribute-only op to `a`. Returns whether it was one.
pub fn attr_op(a: &mut Attrs, op: &Op, now: i64) -> bool {
    match op {
        Op::SetAttr { changes, .. } => {
            if let Some(m) = changes.mode {
                a.mode = m & 0o7777;
            }
            if let Some(u) = changes.uid {
                a.uid = u;
            }
            if let Some(g) = changes.gid {
                a.gid = g;
            }
            if let Some(t) = changes.atime_ns {
                a.atime_ns = t;
            }
            if let Some(t) = changes.mtime_ns {
                a.mtime_ns = t;
            }
            a.ctime_ns = now;
            true
        }
        Op::SetXattr { name, value, .. } => {
            a.xattrs.retain(|(k, _)| k != name);
            if let Some(v) = value {
                a.xattrs.push((name.clone(), v.clone()));
                a.xattrs.sort();
            }
            a.ctime_ns = now;
            true
        }
        Op::SetContent { content, size, mtime_ns, .. } => {
            a.content = Some(*content);
            a.size = *size;
            a.mtime_ns = *mtime_ns;
            a.ctime_ns = now;
            true
        }
        _ => false,
    }
}

// ------------------------------------------------------------ pending index

/// Which dirents / nodes are touched by pending (unacknowledged) ops. Remote
/// changes to those are not shown (the local op wins until the server decides).
#[derive(Default, Clone)]
pub struct PendingIndex {
    dirents: HashMap<(NodeId, NameB), u32>,
    attr_nodes: HashMap<NodeId, u32>,
    /// nodes whose `Create` is still pending
    creates: HashMap<NodeId, u32>,
}

impl PendingIndex {
    fn bump(&mut self, op: &Op, d: i32) {
        if let Op::Create { node, .. } = op {
            let c = self.creates.entry(*node).or_insert(0);
            *c = (*c as i32 + d).max(0) as u32;
            if *c == 0 {
                self.creates.remove(node);
            }
        }
        let (keys, attr) = op_keys(op);
        for k in keys {
            let c = self.dirents.entry(k.clone()).or_insert(0);
            *c = (*c as i32 + d).max(0) as u32;
            if *c == 0 {
                self.dirents.remove(&k);
            }
        }
        if let Some(n) = attr {
            let c = self.attr_nodes.entry(n).or_insert(0);
            *c = (*c as i32 + d).max(0) as u32;
            if *c == 0 {
                self.attr_nodes.remove(&n);
            }
        }
    }
    pub fn add(&mut self, op: &Op) {
        self.bump(op, 1)
    }
    pub fn remove(&mut self, op: &Op) {
        self.bump(op, -1)
    }
    pub fn has_create(&self, n: NodeId) -> bool {
        self.creates.contains_key(&n)
    }
    pub fn touches_dirent(&self, p: NodeId, name: &[u8]) -> bool {
        !self.dirents.is_empty() && self.dirents.contains_key(&(p, name.to_vec()))
    }
    pub fn has_attr_ops(&self, n: NodeId) -> bool {
        self.attr_nodes.contains_key(&n)
    }
    pub fn is_empty(&self) -> bool {
        self.dirents.is_empty() && self.attr_nodes.is_empty()
    }
}

fn op_keys(op: &Op) -> (Vec<(NodeId, NameB)>, Option<NodeId>) {
    match op {
        Op::Create { parent, name, .. } | Op::Link { parent, name, .. } | Op::Unlink { parent, name, .. } => (vec![(*parent, name.0.clone())], None),
        Op::Rename { src_parent, src_name, dst_parent, dst_name, .. } => (vec![(*src_parent, src_name.0.clone()), (*dst_parent, dst_name.0.clone())], None),
        Op::SetAttr { node, .. } | Op::SetXattr { node, .. } | Op::SetContent { node, .. } => (vec![], Some(*node)),
    }
}

/// The part of the namespace an op touches (for reconciliation).
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Footprint {
    pub keys: Vec<(NodeId, NameB)>,
    pub nodes: Vec<NodeId>,
}

pub fn footprint(op: &Op) -> Footprint {
    let (keys, attr) = op_keys(op);
    let mut nodes: Vec<NodeId> = attr.into_iter().collect();
    match op {
        Op::Create { node, .. } | Op::Link { node, .. } | Op::Unlink { node, .. } => nodes.push(*node),
        Op::Rename { node, replaces, .. } => {
            nodes.push(*node);
            nodes.extend(replaces.iter().copied());
        }
        _ => {}
    }
    Footprint { keys, nodes }
}

/// Re-apply pending attribute ops for `node` on top of server attrs.
pub fn overlay_attrs(attrs: &mut Attrs, node: NodeId, pending: &mut dyn Iterator<Item = &Op>, now: i64) {
    for op in pending {
        let target = match op {
            Op::SetAttr { node, .. } | Op::SetXattr { node, .. } | Op::SetContent { node, .. } => *node,
            _ => continue,
        };
        if target == node {
            let ct = attrs.ctime_ns;
            attr_op(attrs, op, now);
            attrs.ctime_ns = ct.max(attrs.ctime_ns);
        }
    }
}

/// Effects that move `visible` to what the server decided for the footprint
/// of an op whose result was not "applied exactly". Remaining pending ops
/// win for the dirents/nodes they touch. Pure: neither tree is modified.
pub fn reconcile(visible: &Tree, synced: &Tree, pidx: &PendingIndex, pending: &[&Op], fp: &Footprint, now: i64) -> Vec<Effect> {
    let mut out = vec![];
    let mut done: HashSet<(NodeId, NameB)> = HashSet::new();
    let mut moved: HashSet<NodeId> = HashSet::new();
    let mut nodes: Vec<NodeId> = fp.nodes.clone();
    for (p, n) in &fp.keys {
        nodes.extend(visible.lookup(*p, n));
        nodes.extend(synced.lookup(*p, n));
    }
    nodes.sort();
    nodes.dedup();

    // moves of single-link nodes
    for &node in &nodes {
        if !visible.nodes.contains_key(&node) || !synced.nodes.contains_key(&node) {
            continue;
        }
        let (vb, sb) = (visible.dirents_of(node), synced.dirents_of(node));
        if vb.len() == 1 && sb.len() == 1 && vb[0] != sb[0] && !pidx.touches_dirent(vb[0].0, &vb[0].1) && !pidx.touches_dirent(sb[0].0, &sb[0].1) && synced.is_dir(sb[0].0) {
            if let Some(occ) = visible.lookup(sb[0].0, &sb[0].1) {
                if occ != node {
                    out.push(Effect::Remove { parent: sb[0].0, name: cairn_proto::Name(sb[0].1.clone()), node: occ, node_deleted: visible.dirents_of(occ).len() <= 1 });
                }
            }
            out.push(Effect::Move { src_parent: vb[0].0, src_name: cairn_proto::Name(vb[0].1.clone()), dst_parent: sb[0].0, dst_name: cairn_proto::Name(sb[0].1.clone()), node });
            moved.insert(node);
            done.insert(sb[0].clone());
        }
    }
    // dirents
    let mut keys = fp.keys.clone();
    for &n in &nodes {
        keys.extend(visible.dirents_of(n).iter().cloned());
        keys.extend(synced.dirents_of(n).iter().cloned());
    }
    keys.sort();
    keys.dedup();
    for (p, name) in keys {
        if done.contains(&(p, name.clone())) || pidx.touches_dirent(p, &name) || !synced.is_dir(p) || !visible.is_dir(p) {
            continue;
        }
        let (v, s) = (visible.lookup(p, &name), synced.lookup(p, &name));
        if v == s {
            continue;
        }
        match (v, s) {
            (_, Some(sn)) => {
                let mut attrs = synced.nodes[&sn].attrs.clone();
                if pidx.has_attr_ops(sn) {
                    overlay_attrs(&mut attrs, sn, &mut pending.iter().copied(), now);
                }
                out.push(Effect::Upsert { parent: p, name: cairn_proto::Name(name), node: sn, attrs });
            }
            (Some(vn), None) if !moved.contains(&vn) => {
                if synced.listed.contains(&p) {
                    out.push(Effect::Remove { parent: p, name: cairn_proto::Name(name), node: vn, node_deleted: visible.dirents_of(vn).len() <= 1 });
                }
            }
            _ => {}
        }
    }
    // attrs
    for &node in &nodes {
        if let (Some(vn), Some(sn)) = (visible.nodes.get(&node), synced.nodes.get(&node)) {
            let mut exp = sn.attrs.clone();
            if pidx.has_attr_ops(node) {
                overlay_attrs(&mut exp, node, &mut pending.iter().copied(), now);
            }
            if !attrs_eq(&vn.attrs, &exp) {
                out.push(Effect::Attrs { node, attrs: exp });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};

    /// Random namespace ops: structural invariants hold at every step, and
    /// replaying the planned op list on a copy of the start state (journal
    /// replay after a crash) yields the identical tree.
    #[test]
    fn random_ops_keep_invariants_and_replay_identically() {
        for seed in 0..30u64 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut t = Tree::new(1);
            t.set_listed(NodeId::ROOT, true);
            let start = t.clone();
            let mut ops: Vec<Op> = vec![];
            let mut dirs = vec![NodeId::ROOT];
            let mut next = 1u64;
            let names = ["a", "b", "c", "d", "e"];
            for step in 0..300 {
                let now = 10 + step as i64;
                let p = dirs[rng.random_range(0..dirs.len())];
                let n1 = names[rng.random_range(0..names.len())].as_bytes();
                let n2 = names[rng.random_range(0..names.len())].as_bytes();
                let q = dirs[rng.random_range(0..dirs.len())];
                let op = match rng.random_range(0..7) {
                    0 | 1 => {
                        let kind = if rng.random_bool(0.3) { Kind::Dir } else { Kind::File };
                        let id = nid(next);
                        let r = t.plan_create(p, n1, &nn(kind), id, now);
                        if r.is_ok() {
                            next += 1;
                            if kind == Kind::Dir {
                                dirs.push(id);
                            }
                        }
                        r.ok()
                    }
                    2 => t.plan_unlink(p, n1).ok(),
                    3 => t.plan_rmdir(p, n1).ok(),
                    4 => {
                        let mode = [RenameMode::Replace, RenameMode::NoReplace, RenameMode::Exchange][rng.random_range(0..3)];
                        t.plan_rename(p, n1, q, n2, mode).ok().flatten()
                    }
                    5 => t.lookup(p, n1).and_then(|n| t.plan_link(n, q, n2).ok()),
                    _ => t.lookup(p, n1).and_then(|n| t.plan_setattr(n, AttrChanges { mode: Some(0o600), ..Default::default() }).ok()),
                };
                if let Some(op) = op {
                    t.apply_op(&op, now);
                    t.gc(&|_| false);
                    ops.push(op);
                }
                dirs.retain(|d| t.is_dir(*d));
                if let Err(e) = t.check_invariants() {
                    panic!("seed {seed} step {step}: {e}");
                }
            }
            // no cycles: every node reaches the root
            for n in t.nodes.keys() {
                if *n != NodeId::ROOT && t.dirents_of(*n).is_empty() {
                    continue;
                }
                assert!(t.path_of(*n).is_some(), "seed {seed}: {n:?} unreachable");
            }
            let mut replay = start;
            for (i, op) in ops.iter().enumerate() {
                replay.apply_op(op, 10 + i as i64);
            }
            replay.gc(&|_| false);
            let dump = |t: &Tree| {
                let mut v: Vec<_> = t.nodes.iter().map(|(k, n)| (*k, n.attrs.kind, n.attrs.mode)).collect();
                v.sort_by_key(|x| x.0);
                let mut d: Vec<_> = t.children.iter().flat_map(|(p, c)| c.iter().map(move |(n, c)| (*p, n.clone(), *c))).collect();
                d.sort();
                (v, d)
            };
            assert_eq!(dump(&t), dump(&replay), "seed {seed}: replay diverged");
        }
    }

    const H: HostId = HostId(7);
    fn nid(n: u64) -> NodeId {
        NodeId::new(H, n)
    }
    fn nn(kind: Kind) -> NewNode {
        NewNode { kind, mode: 0o644, uid: 1, gid: 1, rdev: 0, symlink_target: None, xattrs: vec![] }
    }
    fn mk(t: &mut Tree, p: NodeId, name: &str, kind: Kind, id: NodeId) -> Op {
        let op = t.plan_create(p, name.as_bytes(), &nn(kind), id, 10).unwrap();
        t.apply_op(&op, 10);
        op
    }

    #[test]
    fn create_validate_and_nlink() {
        let mut t = Tree::new(1);
        mk(&mut t, NodeId::ROOT, "d", Kind::Dir, nid(1));
        mk(&mut t, nid(1), "f", Kind::File, nid(2));
        assert_eq!(t.plan_create(nid(1), b"f", &nn(Kind::File), nid(3), 1), Err(Errno::EEXIST));
        assert_eq!(t.plan_create(nid(2), b"x", &nn(Kind::File), nid(3), 1), Err(Errno::ENOTDIR));
        assert_eq!(t.plan_create(nid(1), b"a/b", &nn(Kind::File), nid(3), 1), Err(Errno::EINVAL));
        assert_eq!(t.plan_create(nid(1), &[b'x'; 256], &nn(Kind::File), nid(3), 1), Err(Errno::ENAMETOOLONG));
        let l = t.plan_link(nid(2), NodeId::ROOT, b"g").unwrap();
        t.apply_op(&l, 11);
        assert_eq!(t.nlink(nid(2)), 2);
        assert_eq!(t.plan_link(nid(1), NodeId::ROOT, b"h"), Err(Errno::EPERM));
        assert_eq!(t.plan_unlink(NodeId::ROOT, b"d"), Err(Errno::EISDIR));
        assert_eq!(t.plan_rmdir(NodeId::ROOT, b"d"), Err(Errno::ENOTEMPTY));
        assert_eq!(t.plan_rmdir(nid(1), b"f"), Err(Errno::ENOTDIR));
        let u = t.plan_unlink(nid(1), b"f").unwrap();
        t.apply_op(&u, 12);
        assert_eq!(t.nlink(nid(2)), 1);
        assert_eq!(t.nlink(nid(1)), 2);
        assert!(t.nodes.contains_key(&nid(2)));
        let u = t.plan_unlink(NodeId::ROOT, b"g").unwrap();
        t.apply_op(&u, 12);
        assert!(!t.nodes.contains_key(&nid(2)));
        let r = t.plan_rmdir(NodeId::ROOT, b"d").unwrap();
        t.apply_op(&r, 13);
        assert!(t.children(NodeId::ROOT).unwrap().is_empty());
    }

    #[test]
    fn rename_rules() {
        let mut t = Tree::new(1);
        let r = NodeId::ROOT;
        mk(&mut t, r, "a", Kind::Dir, nid(1));
        mk(&mut t, nid(1), "b", Kind::Dir, nid(2));
        mk(&mut t, r, "f", Kind::File, nid(3));
        mk(&mut t, r, "g", Kind::File, nid(4));
        mk(&mut t, r, "e", Kind::Dir, nid(5));
        mk(&mut t, nid(5), "x", Kind::File, nid(6));
        // cycle
        assert_eq!(t.plan_rename(r, b"a", nid(2), b"z", RenameMode::Replace), Err(Errno::EINVAL));
        assert_eq!(t.plan_rename(r, b"a", nid(1), b"z", RenameMode::Replace), Err(Errno::EINVAL));
        // type mismatches
        assert_eq!(t.plan_rename(r, b"f", r, b"a", RenameMode::Replace), Err(Errno::EISDIR));
        assert_eq!(t.plan_rename(r, b"a", r, b"f", RenameMode::Replace), Err(Errno::ENOTDIR));
        assert_eq!(t.plan_rename(r, b"a", r, b"e", RenameMode::Replace), Err(Errno::ENOTEMPTY));
        assert_eq!(t.plan_rename(r, b"f", r, b"g", RenameMode::NoReplace), Err(Errno::EEXIST));
        assert_eq!(t.plan_rename(r, b"f", r, b"nope", RenameMode::Exchange), Err(Errno::ENOENT));
        assert_eq!(t.plan_rename(r, b"nope", r, b"f", RenameMode::Replace), Err(Errno::ENOENT));
        assert_eq!(t.plan_rename(r, b"f", r, b"f", RenameMode::Replace), Ok(None));
        // replace file
        let op = t.plan_rename(r, b"f", r, b"g", RenameMode::Replace).unwrap().unwrap();
        assert!(matches!(&op, Op::Rename { replaces: Some(x), .. } if *x == nid(4)));
        t.apply_op(&op, 5);
        assert_eq!(t.lookup(r, b"g"), Some(nid(3)));
        assert!(t.lookup(r, b"f").is_none());
        assert!(!t.nodes.contains_key(&nid(4)));
        // exchange
        let op = t.plan_rename(r, b"g", r, b"e", RenameMode::Exchange).unwrap().unwrap();
        t.apply_op(&op, 6);
        assert_eq!(t.lookup(r, b"g"), Some(nid(5)));
        assert_eq!(t.lookup(r, b"e"), Some(nid(3)));
        assert_eq!(t.path_of(nid(6)).unwrap(), vec![b"g".to_vec(), b"x".to_vec()]);
    }

    #[test]
    fn xattr_rules() {
        let mut t = Tree::new(1);
        mk(&mut t, NodeId::ROOT, "f", Kind::File, nid(1));
        assert_eq!(t.plan_removexattr(nid(1), b"user.a"), Err(Errno::ENODATA));
        assert_eq!(t.plan_setxattr(nid(1), b"user.a", b"1", XattrFlags::Replace), Err(Errno::ENODATA));
        let op = t.plan_setxattr(nid(1), b"user.a", b"1", XattrFlags::Create).unwrap();
        t.apply_op(&op, 3);
        assert_eq!(t.plan_setxattr(nid(1), b"user.a", b"2", XattrFlags::Create), Err(Errno::EEXIST));
        assert_eq!(t.nodes[&nid(1)].attrs.xattrs, vec![(b"user.a".to_vec(), b"1".to_vec())]);
    }

    fn attrs_of(kind: Kind) -> Attrs {
        let mut a = dir_attrs(1);
        a.kind = kind;
        if kind == Kind::File {
            a.content = Some(ContentId::Empty);
        }
        a
    }
    fn nm(s: &str) -> cairn_proto::Name {
        cairn_proto::Name(s.as_bytes().to_vec())
    }

    #[test]
    fn effects_are_idempotent_and_lazy() {
        let mut t = Tree::new(1);
        let r = NodeId::ROOT;
        t.set_listed(r, true);
        let up = Effect::Upsert { parent: r, name: nm("d"), node: nid(1), attrs: attrs_of(Kind::Dir) };
        assert!(t.would_change(&up));
        t.apply_effect(&up, Seq(1));
        assert!(!t.would_change(&up));
        t.apply_effect(&up, Seq(1));
        assert!(t.listed.contains(&nid(1)), "new dirs are complete");
        // inside an unlisted dir with unknown node: ignored
        t.set_listed(nid(1), false);
        let up2 = Effect::Upsert { parent: nid(1), name: nm("f"), node: nid(2), attrs: attrs_of(Kind::File) };
        assert!(!t.would_change(&up2));
        t.apply_effect(&up2, Seq(2));
        assert!(!t.nodes.contains_key(&nid(2)));
        // parent unknown: ignored
        let up3 = Effect::Upsert { parent: nid(99), name: nm("f"), node: nid(3), attrs: attrs_of(Kind::File) };
        t.apply_effect(&up3, Seq(3));
        assert!(!t.nodes.contains_key(&nid(3)));
        // move, remove
        t.set_listed(nid(1), true);
        t.apply_effect(&up2, Seq(2));
        let mv = Effect::Move { src_parent: nid(1), src_name: nm("f"), dst_parent: r, dst_name: nm("g"), node: nid(2) };
        t.apply_effect(&mv, Seq(4));
        t.apply_effect(&mv, Seq(4));
        assert_eq!(t.lookup(r, b"g"), Some(nid(2)));
        assert!(t.lookup(nid(1), b"f").is_none());
        let rm = Effect::Remove { parent: r, name: nm("g"), node: nid(2), node_deleted: true };
        t.apply_effect(&rm, Seq(5));
        t.apply_effect(&rm, Seq(5));
        assert!(!t.nodes.contains_key(&nid(2)));
        // exchange as two upserts (server model)
        t.apply_effect(&Effect::Upsert { parent: r, name: nm("a"), node: nid(10), attrs: attrs_of(Kind::File) }, Seq(6));
        t.apply_effect(&Effect::Upsert { parent: r, name: nm("b"), node: nid(11), attrs: attrs_of(Kind::File) }, Seq(7));
        t.apply_effect(&Effect::Upsert { parent: r, name: nm("a"), node: nid(11), attrs: attrs_of(Kind::File) }, Seq(8));
        t.apply_effect(&Effect::Upsert { parent: r, name: nm("b"), node: nid(10), attrs: attrs_of(Kind::File) }, Seq(8));
        t.gc(&|_| false);
        assert_eq!(t.lookup(r, b"a"), Some(nid(11)));
        assert_eq!(t.lookup(r, b"b"), Some(nid(10)));
        assert_eq!(t.nlink(nid(10)), 1);
    }

    /// visible = synced + pending
    fn setup() -> (Tree, Tree, Vec<Op>) {
        let mut s = Tree::new(1);
        s.set_listed(NodeId::ROOT, true);
        s.apply_effect(&Effect::Upsert { parent: NodeId::ROOT, name: nm("f"), node: nid(1), attrs: attrs_of(Kind::File) }, Seq(1));
        s.apply_effect(&Effect::Upsert { parent: NodeId::ROOT, name: nm("d"), node: nid(2), attrs: attrs_of(Kind::Dir) }, Seq(2));
        (s.clone(), s, vec![])
    }

    #[test]
    fn reconcile_rejected_rename_and_delete_lost() {
        let (mut v, s, _) = setup();
        let r = NodeId::ROOT;
        // local rename f -> d/g, rejected by the server (RenameLost): synced unchanged
        let op = v.plan_rename(r, b"f", nid(2), b"g", RenameMode::Replace).unwrap().unwrap();
        v.apply_op(&op, 5);
        let eff = reconcile(&v, &s, &PendingIndex::default(), &[], &footprint(&op), 6);
        assert_eq!(eff.len(), 1);
        assert!(matches!(&eff[0], Effect::Move { dst_parent, .. } if *dst_parent == r));
        for e in &eff {
            v.apply_effect(e, Seq(0));
        }
        assert_eq!(v.lookup(r, b"f"), Some(nid(1)));
        assert!(v.lookup(nid(2), b"g").is_none());
        // delete lost: entry comes back
        let op = v.plan_unlink(r, b"f").unwrap();
        v.apply_op(&op, 7);
        let eff = reconcile(&v, &s, &PendingIndex::default(), &[], &footprint(&op), 8);
        assert!(matches!(&eff[..], [Effect::Upsert { node, .. }] if *node == nid(1)));
    }

    #[test]
    fn reconcile_conflict_copy_create() {
        let (mut v, mut s, _) = setup();
        let r = NodeId::ROOT;
        // local create x (node 50); server had another x (node 60) and put ours at x.conflict
        let op = mk(&mut v, r, "x", Kind::File, nid(50));
        s.apply_effect(&Effect::Upsert { parent: r, name: nm("x"), node: nid(60), attrs: attrs_of(Kind::File) }, Seq(3));
        s.apply_effect(&Effect::Upsert { parent: r, name: nm("x.conflict-h7-1"), node: nid(50), attrs: attrs_of(Kind::File) }, Seq(4));
        let eff = reconcile(&v, &s, &PendingIndex::default(), &[], &footprint(&op), 6);
        // node 50 moves to the conflict name; the other host's x appears
        assert!(eff.iter().any(|e| matches!(e, Effect::Move { node, dst_name, .. } if *node == nid(50) && dst_name.0 == b"x.conflict-h7-1")));
        assert!(eff.iter().any(|e| matches!(e, Effect::Upsert { node, name, .. } if *node == nid(60) && name.0 == b"x")));
        for e in &eff {
            v.apply_effect(e, Seq(0));
        }
        v.gc(&|_| false);
        assert_eq!(v.lookup(r, b"x"), Some(nid(60)));
        assert_eq!(v.lookup(r, b"x.conflict-h7-1"), Some(nid(50)));
    }

    #[test]
    fn pending_ops_win_in_reconcile() {
        let (mut v, s, _) = setup();
        let r = NodeId::ROOT;
        // two pending ops on the same dirent: the first one's reconcile leaves it alone
        let op1 = v.plan_unlink(r, b"f").unwrap();
        v.apply_op(&op1, 5);
        let op2 = mk(&mut v, r, "f", Kind::File, nid(70));
        let mut pidx = PendingIndex::default();
        pidx.add(&op2);
        let eff = reconcile(&v, &s, &pidx, &[&op2], &footprint(&op1), 6);
        assert!(eff.iter().all(|e| !matches!(e, Effect::Upsert { name, .. } if name.0 == b"f")));
    }

    #[test]
    fn attr_overlay_keeps_pending_setattr() {
        let (mut v, s, _) = setup();
        let op = v.plan_setattr(nid(1), AttrChanges { mode: Some(0o600), ..Default::default() }).unwrap();
        v.apply_op(&op, 9);
        let mut pidx = PendingIndex::default();
        pidx.add(&op);
        let mut a = s.nodes[&nid(1)].attrs.clone();
        a.uid = 42;
        overlay_attrs(&mut a, nid(1), &mut [&op].into_iter(), 10);
        assert_eq!((a.mode, a.uid), (0o600, 42));
    }
}
