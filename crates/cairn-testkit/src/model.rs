//! Reference model: a pure, deterministic namespace + merge engine written
//! directly from docs/PROTOCOL.md's merge table. No I/O, independent of the
//! server implementation. Used as an oracle: feed it the server's committed
//! order of commits and it predicts the namespace, the changes and the
//! `CommitResult`s.

use cairn_proto::*;
use std::collections::{BTreeMap, HashMap};

/// Result class used for comparison: ignores `seq` and the
/// Transformed/Rejected split, keeps the resolution kind.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Class {
    Applied,
    Adopted,
    ConflictCopy,
    DeleteLost,
    RenameLost,
    DirRecreated,
    CycleRejected,
    ContentConflictCopy,
    Failed(ErrCode),
}

pub fn classify(r: &CommitResult) -> Class {
    match r {
        CommitResult::Applied { .. } => Class::Applied,
        CommitResult::Transformed { conflict, .. } | CommitResult::Rejected { conflict } => match conflict.resolution {
            Resolution::Adopted { .. } => Class::Adopted,
            Resolution::ConflictCopy { .. } => Class::ConflictCopy,
            Resolution::DeleteLost => Class::DeleteLost,
            Resolution::RenameLost => Class::RenameLost,
            Resolution::DirRecreated { .. } => Class::DirRecreated,
            Resolution::CycleRejected => Class::CycleRejected,
            Resolution::ContentConflictCopy { .. } => Class::ContentConflictCopy,
        },
        CommitResult::Failed { error } => Class::Failed(error.code),
    }
}

#[derive(Clone, Debug)]
pub struct Predicted {
    pub result: CommitResult,
    /// Spec is ambiguous on the exact resolution; the server may also answer
    /// `Applied` or any `Transformed`.
    pub lax: bool,
    pub changes: Vec<Change>,
}

impl Predicted {
    pub fn matches(&self, actual: &CommitResult) -> bool {
        let (a, p) = (classify(actual), classify(&self.result));
        (matches!(a, Class::Failed(_)) && matches!(p, Class::Failed(_))) || a == p || (self.lax && !matches!(a, Class::Failed(_) | Class::Adopted | Class::RenameLost | Class::CycleRejected | Class::DeleteLost))
    }
}

#[derive(Clone, Debug)]
struct Node {
    attrs: Attrs,
    /// Seq of last change to attrs or content.
    version: Seq,
    content_version: Seq,
    /// (seq, host) of every modification, for the delete-vs-modify rule.
    mods: Vec<(Seq, HostId)>,
    /// Not yet announced by an Upsert: the first Upsert stamps its versions.
    fresh: bool,
}

impl Node {
    /// A node resurrected from its tombstone is announced anew (fresh versions).
    fn revived(mut self) -> Node {
        self.version = Seq(0);
        self.content_version = Seq(0);
        self.mods.clear();
        self.fresh = true;
        self
    }
}

#[derive(Clone, Debug)]
struct Tomb {
    parent: NodeId,
    name: Name,
    node: Node,
}

#[derive(Clone, Default)]
pub struct Model {
    nodes: HashMap<NodeId, Node>,
    dirents: BTreeMap<(NodeId, Name), NodeId>,
    tombs: HashMap<NodeId, Tomb>,
    alias: HashMap<NodeId, NodeId>,
    leases: HashMap<NodeId, HostId>,
    results: HashMap<OpId, CommitResult>,
    pub conflicts: Vec<ConflictRecord>,
    pub changes: Vec<Change>,
    seq: u64,
    minted: u64,
    /// Mimic an implementation that tracks only the last modifier of a node\n    /// for the delete-vs-modify rule (the server does). Spec-exact when false.
    pub last_mod_only: bool,
}

pub struct Ctx<'a> {
    /// UTC timestamp `yyyymmddThhmmss` for conflict names.
    pub ts: &'a str,
}

pub fn dir_attrs() -> Attrs {
    Attrs { kind: Kind::Dir, mode: 0o755, uid: 0, gid: 0, size: 0, mtime_ns: 0, ctime_ns: 0, atime_ns: 0, rdev: 0, content: None, symlink_target: None, xattrs: vec![] }
}

fn err(code: ErrCode, msg: &str) -> CommitResult {
    CommitResult::Failed { error: ErrorReply { code, msg: msg.into() } }
}

pub fn valid_name(n: &Name) -> bool {
    let b = &n.0;
    !b.is_empty() && b.len() <= 255 && !b.contains(&b'/') && !b.contains(&0) && b != b"." && b != b".."
}

impl Model {
    pub fn new() -> Model {
        let mut m = Model::default();
        m.minted = 1; // NodeId(HostId(0), 1) == ROOT, so server-minted ids start at 2
        m.nodes.insert(NodeId::ROOT, Node { attrs: dir_attrs(), version: Seq(0), content_version: Seq(0), mods: vec![], fresh: false });
        m
    }

    pub fn head(&self) -> Seq {
        Seq(self.seq)
    }

    pub fn set_lease(&mut self, node: NodeId, holder: Option<HostId>) {
        match holder {
            Some(h) => self.leases.insert(node, h),
            None => self.leases.remove(&node),
        };
    }

    fn r(&self, n: NodeId) -> NodeId {
        self.alias.get(&n).copied().unwrap_or(n)
    }

    /// Namespace rows: (parent, name) -> (node, attrs).
    pub fn rows(&self) -> Vec<SnapshotRow> {
        self.dirents
            .iter()
            .map(|((p, n), id)| SnapshotRow { parent: *p, name: n.clone(), node: *id, attrs: self.nodes[id].attrs.clone() })
            .collect()
    }

    fn is_dir(&self, n: NodeId) -> bool {
        self.nodes.get(&n).map_or(false, |x| x.attrs.kind == Kind::Dir)
    }

    fn children(&self, d: NodeId) -> bool {
        self.dirents.range((d, Name(vec![]))..).next().map_or(false, |((p, _), _)| *p == d)
    }

    fn dirent_count(&self, n: NodeId) -> usize {
        self.dirents.values().filter(|x| **x == n).count()
    }

    fn parent_of(&self, n: NodeId) -> Option<NodeId> {
        self.dirents.iter().find(|(_, v)| **v == n).map(|((p, _), _)| *p)
    }

    fn conflict_name(&self, parent: NodeId, base: &Name, host: HostId, ctx: &Ctx) -> Name {
        let mut s = base.0.clone();
        s.extend_from_slice(format!(".conflict-h{}-{}", host.0, ctx.ts).as_bytes());
        let mut cand = Name(s.clone());
        let mut i = 2;
        while self.dirents.contains_key(&(parent, cand.clone())) {
            let mut t = s.clone();
            t.extend_from_slice(format!("-{i}").as_bytes());
            cand = Name(t);
            i += 1;
        }
        cand
    }

    fn emit(&mut self, host: HostId, commit: &Commit, effect: Effect, out: &mut Vec<Change>) -> Seq {
        self.seq += 1;
        let c = Change { seq: Seq(self.seq), origin: Some(commit.op_id), host, cred: commit.cred, effect };
        out.push(c.clone());
        self.changes.push(c);
        Seq(self.seq)
    }

    fn touch(&mut self, n: NodeId, host: HostId, seq: Seq) {
        if let Some(x) = self.nodes.get_mut(&n) {
            x.version = seq;
            x.mods.push((seq, host));
        }
    }

    fn upsert(&mut self, host: HostId, c: &Commit, parent: NodeId, name: Name, node: NodeId, out: &mut Vec<Change>) -> Seq {
        self.upsert_t(host, c, parent, name, node, out, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_t(&mut self, host: HostId, c: &Commit, parent: NodeId, name: Name, node: NodeId, out: &mut Vec<Change>, touch: bool) -> Seq {
        // Repointing an occupied dirent orphans the previous node if it has no other dirent.
        if let Some(old) = self.dirents.insert((parent, name.clone()), node) {
            if old != node && self.dirent_count(old) == 0 {
                self.bury(old, parent, name.clone());
            }
        }
        let attrs = self.nodes[&node].attrs.clone();
        let s = self.emit(host, c, Effect::Upsert { parent, name, node, attrs }, out);
        if let Some(n) = self.nodes.get_mut(&node) {
            if n.fresh {
                n.fresh = false;
                n.content_version = s;
            }
        }
        if touch {
            self.touch(node, host, s);
        }
        s
    }

    fn bury(&mut self, n: NodeId, parent: NodeId, name: Name) {
        if let Some(node) = self.nodes.remove(&n) {
            self.tombs.insert(n, Tomb { parent, name, node });
        }
    }

    /// Make sure `d` exists as a directory, resurrecting it (and ancestors)
    /// from tombstones if needed. Err = not found / not a dir.
    fn ensure_dir(&mut self, host: HostId, c: &Commit, d: NodeId, ctx: &Ctx, out: &mut Vec<Change>, recreated: &mut Option<NodeId>) -> Result<(), CommitResult> {
        if let Some(n) = self.nodes.get(&d) {
            return if n.attrs.kind == Kind::Dir { Ok(()) } else { Err(err(ErrCode::NotDir, "parent not a dir")) };
        }
        let Some(t) = self.tombs.get(&d).cloned() else { return Err(err(ErrCode::NotFound, "no such parent")) };
        if t.node.attrs.kind != Kind::Dir {
            return Err(err(ErrCode::NotDir, "parent not a dir"));
        }
        self.ensure_dir(host, c, t.parent, ctx, out, recreated)?;
        self.tombs.remove(&d);
        let name = if self.dirents.contains_key(&(t.parent, t.name.clone())) { self.conflict_name(t.parent, &t.name, host, ctx) } else { t.name.clone() };
        self.nodes.insert(d, t.node.revived());
        self.upsert(host, c, t.parent, name, d, out);
        recreated.get_or_insert(d);
        Ok(())
    }

    fn mint(&mut self) -> NodeId {
        self.minted += 1;
        NodeId::new(HostId(0), self.minted)
    }

    fn equivalent(a: &Attrs, b: &Attrs) -> bool {
        a.kind == b.kind
            && match a.kind {
                Kind::Dir => true,
                Kind::File => a.content == b.content,
                Kind::Symlink => a.symlink_target == b.symlink_target,
                _ => a.rdev == b.rdev,
            }
    }

    pub fn apply(&mut self, host: HostId, commit: &Commit, ctx: &Ctx) -> Predicted {
        if let Some(r) = self.results.get(&commit.op_id) {
            return Predicted { result: r.clone(), lax: false, changes: vec![] };
        }
        let before = self.changes.len();
        let (result, lax) = self.apply_inner(host, commit, ctx);
        let result = match result {
            // fill in the seq now that we know the changes
            CommitResult::Applied { .. } => CommitResult::Applied { seq: self.head() },
            CommitResult::Transformed { conflict, .. } => {
                let conflict = ConflictRecord { seq: self.head(), ..conflict };
                self.conflicts.push(conflict.clone());
                CommitResult::Transformed { seq: self.head(), conflict }
            }
            CommitResult::Rejected { conflict } => {
                let conflict = ConflictRecord { seq: self.head(), ..conflict };
                self.conflicts.push(conflict.clone());
                CommitResult::Rejected { conflict }
            }
            f => f,
        };
        self.results.insert(commit.op_id, result.clone());
        Predicted { result, lax, changes: self.changes[before..].to_vec() }
    }

    fn conflict(&self, c: &Commit, host: HostId, res: Resolution, detail: &str) -> ConflictRecord {
        ConflictRecord { seq: self.head(), op_id: c.op_id, host, resolution: res, detail: detail.into() }
    }

    fn transformed(&self, c: &Commit, host: HostId, res: Resolution, detail: &str) -> CommitResult {
        CommitResult::Transformed { seq: self.head(), conflict: self.conflict(c, host, res, detail) }
    }

    fn apply_inner(&mut self, host: HostId, c: &Commit, ctx: &Ctx) -> (CommitResult, bool) {
        let mut out = vec![];
        let mut recreated = None;
        let mut lax = false;
        let mut res = match &c.op {
            Op::Create { parent, name, node, attrs } => self.create(host, c, ctx, self.r(*parent), name, *node, attrs, &mut out, &mut recreated),
            Op::Link { parent, name, node } => {
                let (node, parent) = (self.r(*node), self.r(*parent));
                self.link(host, c, ctx, parent, name, node, &mut out, &mut recreated, &mut lax)
            }
            Op::Unlink { parent, name, node, base } => {
                let (node, parent) = (self.r(*node), self.r(*parent));
                self.unlink(host, c, parent, name, node, *base, &mut out)
            }
            Op::Rename { src_parent, src_name, node, dst_parent, dst_name, replaces, mode } => {
                let (node, replaces) = (self.r(*node), replaces.map(|r| self.r(r)));
                let (src_parent, dst_parent) = (self.r(*src_parent), self.r(*dst_parent));
                self.rename(host, c, ctx, src_parent, src_name, node, dst_parent, dst_name, replaces, *mode, &mut out, &mut recreated, &mut lax)
            }
            Op::SetAttr { node, changes, .. } => {
                let node = self.r(*node);
                match self.nodes.get_mut(&node) {
                    None => err(ErrCode::NotFound, "setattr: gone"),
                    Some(n) => {
                        let a = &mut n.attrs;
                        if let Some(v) = changes.mode { a.mode = v & 0o7777 }
                        if let Some(v) = changes.uid { a.uid = v }
                        if let Some(v) = changes.gid { a.gid = v }
                        if let Some(v) = changes.atime_ns { a.atime_ns = v }
                        if let Some(v) = changes.mtime_ns { a.mtime_ns = v }
                        let attrs = a.clone();
                        let s = self.emit(host, c, Effect::Attrs { node, attrs }, &mut out);
                        self.touch(node, host, s);
                        CommitResult::Applied { seq: s }
                    }
                }
            }
            Op::SetXattr { node, name, value } => {
                let node = self.r(*node);
                match self.nodes.get_mut(&node) {
                    None => err(ErrCode::NotFound, "setxattr: gone"),
                    Some(n) => {
                        n.attrs.xattrs.retain(|(k, _)| k != name);
                        if let Some(v) = value {
                            n.attrs.xattrs.push((name.clone(), v.clone()));
                            n.attrs.xattrs.sort();
                        }
                        let attrs = n.attrs.clone();
                        let s = self.emit(host, c, Effect::Attrs { node, attrs }, &mut out);
                        self.touch(node, host, s);
                        CommitResult::Applied { seq: s }
                    }
                }
            }
            Op::SetContent { node, base, content, size, mtime_ns } => {
                let node = self.r(*node);
                self.set_content(host, c, ctx, node, *base, *content, *size, *mtime_ns, &mut out, &mut lax)
            }
        };
        if let (Some(d), CommitResult::Applied { .. }) = (recreated, &res) {
            res = self.transformed(c, host, Resolution::DirRecreated { dir: d }, "dir recreated");
        }
        (res, lax)
    }

    #[allow(clippy::too_many_arguments)]
    fn create(&mut self, host: HostId, c: &Commit, ctx: &Ctx, parent: NodeId, name: &Name, node: NodeId, attrs: &Attrs, out: &mut Vec<Change>, rec: &mut Option<NodeId>) -> CommitResult {
        if !valid_name(name) {
            return err(ErrCode::Invalid, "bad name");
        }
        if let Err(e) = self.ensure_dir(host, c, parent, ctx, out, rec) {
            return e;
        }
        let node = self.r(node);
        if let Some(existing) = self.dirents.get(&(parent, name.clone())).copied() {
            if existing == node {
                return CommitResult::Applied { seq: self.head() }; // idempotent
            }
            if Self::equivalent(attrs, &self.nodes[&existing].attrs) {
                self.alias.insert(node, existing);
                return self.transformed(c, host, Resolution::Adopted { node: existing }, "adopted");
            }
            let cn = self.conflict_name(parent, name, host, ctx);
            self.nodes.insert(node, Node { attrs: attrs.clone(), version: Seq(0), content_version: Seq(0), mods: vec![], fresh: true });
            let s = self.upsert(host, c, parent, cn.clone(), node, out);
            self.nodes.get_mut(&node).unwrap().content_version = s;
            return self.transformed(c, host, Resolution::ConflictCopy { parent, name: cn }, "create conflict");
        }
        if self.nodes.contains_key(&node) {
            return err(ErrCode::Exists, "node id in use");
        }
        self.nodes.insert(node, Node { attrs: attrs.clone(), version: Seq(0), content_version: Seq(0), mods: vec![], fresh: true });
        let s = self.upsert(host, c, parent, name.clone(), node, out);
        self.nodes.get_mut(&node).unwrap().content_version = s;
        CommitResult::Applied { seq: s }
    }

    #[allow(clippy::too_many_arguments)]
    fn link(&mut self, host: HostId, c: &Commit, ctx: &Ctx, parent: NodeId, name: &Name, node: NodeId, out: &mut Vec<Change>, rec: &mut Option<NodeId>, lax: &mut bool) -> CommitResult {
        if !valid_name(name) {
            return err(ErrCode::Invalid, "bad name");
        }
        let alive = self.nodes.contains_key(&node);
        if !alive && !self.tombs.contains_key(&node) {
            return err(ErrCode::NotFound, "link: no such node");
        }
        let kind = self.nodes.get(&node).map(|n| n.attrs.kind).unwrap_or_else(|| self.tombs[&node].node.attrs.kind);
        if kind == Kind::Dir {
            return err(ErrCode::IsDir, "link to dir");
        }
        if let Err(e) = self.ensure_dir(host, c, parent, ctx, out, rec) {
            return e;
        }
        if let Some(e) = self.dirents.get(&(parent, name.clone())).copied() {
            if e == node {
                return CommitResult::Applied { seq: self.head() };
            }
            if !alive {
                let t = self.tombs.remove(&node).unwrap();
                self.nodes.insert(node, t.node.revived());
            }
            let cn = self.conflict_name(parent, name, host, ctx);
            self.upsert_t(host, c, parent, cn.clone(), node, out, !alive);
            return self.transformed(c, host, Resolution::ConflictCopy { parent, name: cn }, "link conflict");
        }
        if !alive {
            let t = self.tombs.remove(&node).unwrap();
            self.nodes.insert(node, t.node.revived());
            *lax = true;
        }
        let s = self.upsert_t(host, c, parent, name.clone(), node, out, !alive);
        CommitResult::Applied { seq: s }
    }

    fn remove_dirent(&mut self, host: HostId, c: &Commit, parent: NodeId, name: &Name, node: NodeId, out: &mut Vec<Change>) -> Seq {
        self.dirents.remove(&(parent, name.clone()));
        let deleted = self.dirent_count(node) == 0;
        let s = self.emit(host, c, Effect::Remove { parent, name: name.clone(), node, node_deleted: deleted }, out);
        if deleted {
            self.bury(node, parent, name.clone());
        }
        s
    }

    #[allow(clippy::too_many_arguments)]
    fn unlink(&mut self, host: HostId, c: &Commit, parent: NodeId, name: &Name, node: NodeId, base: Seq, out: &mut Vec<Change>) -> CommitResult {
        let Some(cur) = self.dirents.get(&(parent, name.clone())).copied() else {
            return CommitResult::Applied { seq: self.head() };
        };
        let last = self.dirent_count(cur) <= 1;
        let lost = cur != node
            || last && if self.last_mod_only {
                self.nodes[&cur].mods.last().map_or(false, |(s, h)| *s > base && *h != host)
            } else {
                self.nodes[&cur].mods.iter().any(|(s, h)| *s > base && *h != host)
            }
            || (self.is_dir(cur) && self.children(cur));
        if lost {
            // Like the server, re-announce the surviving dirent so the loser's client gets it back.
            if self.nodes.contains_key(&cur) {
                self.upsert_t(host, c, parent, name.clone(), cur, out, false);
            }
            return self.transformed(c, host, Resolution::DeleteLost, "delete lost");
        }
        let s = self.remove_dirent(host, c, parent, name, node, out);
        CommitResult::Applied { seq: s }
    }

    fn is_ancestor(&self, anc: NodeId, mut n: NodeId) -> bool {
        // is `anc` equal to n or an ancestor of n
        for _ in 0..10_000 {
            if n == anc {
                return true;
            }
            let p = if self.nodes.contains_key(&n) { self.parent_of(n) } else { self.tombs.get(&n).map(|t| t.parent) };
            match p {
                Some(p) => n = p,
                None => return false,
            }
        }
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn rename(&mut self, host: HostId, c: &Commit, ctx: &Ctx, sp: NodeId, sn: &Name, node: NodeId, dp: NodeId, dn: &Name, replaces: Option<NodeId>, mode: RenameMode, out: &mut Vec<Change>, rec: &mut Option<NodeId>, lax: &mut bool) -> CommitResult {
        if !valid_name(dn) || !valid_name(sn) {
            return err(ErrCode::Invalid, "bad name");
        }
        let alive = self.nodes.contains_key(&node);
        let dead = self.tombs.contains_key(&node);
        if !alive && !dead {
            return err(ErrCode::NotFound, "rename: no such node");
        }
        let src_ok = self.dirents.get(&(sp, sn.clone())) == Some(&node);
        if !src_ok && alive {
            return self.transformed(c, host, Resolution::RenameLost, "rename lost");
        }
        let kind = self.nodes.get(&node).map(|n| n.attrs.kind).unwrap_or_else(|| self.tombs[&node].node.attrs.kind);
        if kind == Kind::Dir && self.is_ancestor(node, dp) {
            return self.transformed(c, host, Resolution::CycleRejected, "cycle");
        }
        if let Err(e) = self.ensure_dir(host, c, dp, ctx, out, rec) {
            return e;
        }
        let cur = self.dirents.get(&(dp, dn.clone())).copied();
        if cur == Some(node) && src_ok {
            return CommitResult::Applied { seq: self.head() }; // rename to itself
        }
        let mut conflict_dest = false;
        match mode {
            RenameMode::NoReplace => conflict_dest = cur.is_some(),
            RenameMode::Replace => {
                // An absent destination is free whatever the client expected.
                conflict_dest = cur.is_some() && cur != replaces;
                if let (false, Some(r)) = (conflict_dest, cur) {
                    if self.is_dir(r) && self.children(r) {
                        conflict_dest = true;
                    }
                    if self.is_dir(r) != (kind == Kind::Dir) {
                        conflict_dest = true;
                    }
                }
            }
            RenameMode::Exchange if !src_ok => {
                conflict_dest = cur.is_some() && cur != replaces;
            }
            RenameMode::Exchange => {
                // Exchange with an expected-absent destination degrades to a plain move.
                if replaces.is_none() {
                    conflict_dest = cur.is_some();
                } else if cur.is_some() && (!src_ok || cur != replaces) {
                    conflict_dest = true;
                }
            }
        }
        if mode == RenameMode::Exchange && !conflict_dest && cur.is_some() && src_ok {
            let x = cur.unwrap();
            if kind == Kind::Dir && self.is_ancestor(node, dp) || self.is_dir(x) && self.is_ancestor(x, sp) {
                return self.transformed(c, host, Resolution::CycleRejected, "cycle");
            }
            self.dirents.insert((sp, sn.clone()), x);
            self.dirents.insert((dp, dn.clone()), node);
            let (ax, an) = (self.nodes[&x].attrs.clone(), self.nodes[&node].attrs.clone());
            let _ = an;
            self.emit(host, c, Effect::Remove { parent: dp, name: dn.clone(), node: x, node_deleted: false }, out);
            self.emit(host, c, Effect::Move { src_parent: sp, src_name: sn.clone(), dst_parent: dp, dst_name: dn.clone(), node }, out);
            let s = self.emit(host, c, Effect::Upsert { parent: sp, name: sn.clone(), node: x, attrs: ax }, out);
            return CommitResult::Applied { seq: s };
        }
        if conflict_dest {
            let cn = self.conflict_name(dp, dn, host, ctx);
            self.finish_move(host, c, sp, sn, node, dp, &cn, src_ok, out);
            return self.transformed(c, host, Resolution::ConflictCopy { parent: dp, name: cn }, "rename dest conflict");
        }
        if !src_ok {
            *lax = true;
        }
        if let Some(r) = cur {
            self.remove_dirent(host, c, dp, dn, r, out);
        }
        let s = self.finish_move(host, c, sp, sn, node, dp, dn, src_ok, out);
        CommitResult::Applied { seq: s }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_move(&mut self, host: HostId, c: &Commit, sp: NodeId, sn: &Name, node: NodeId, dp: NodeId, dn: &Name, src_ok: bool, out: &mut Vec<Change>) -> Seq {
        if src_ok {
            self.dirents.remove(&(sp, sn.clone()));
            self.dirents.insert((dp, dn.clone()), node);
            self.emit(host, c, Effect::Move { src_parent: sp, src_name: sn.clone(), dst_parent: dp, dst_name: dn.clone(), node }, out)
        } else {
            if let Some(t) = self.tombs.remove(&node) {
                self.nodes.insert(node, t.node.revived());
            }
            self.upsert(host, c, dp, dn.clone(), node, out)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn set_content(&mut self, host: HostId, c: &Commit, ctx: &Ctx, node: NodeId, base: Seq, content: ContentId, size: u64, mtime: i64, out: &mut Vec<Change>, lax: &mut bool) -> CommitResult {
        if !self.nodes.contains_key(&node) {
            if let Some(t) = self.tombs.get(&node) {
                if t.node.attrs.kind != Kind::File {
                    return err(ErrCode::Invalid, "setcontent: not a file");
                }
            }
            let Some(t) = self.tombs.remove(&node) else { return err(ErrCode::NotFound, "setcontent: no such node") };
            // resurrect (modify wins) then apply
            let mut rec = None;
            if self.ensure_dir(host, c, t.parent, ctx, out, &mut rec).is_err() {
                self.tombs.insert(node, t);
                return err(ErrCode::NotFound, "setcontent: no parent");
            }
            let name = if self.dirents.contains_key(&(t.parent, t.name.clone())) { self.conflict_name(t.parent, &t.name, host, ctx) } else { t.name.clone() };
            self.nodes.insert(node, t.node.revived());
            self.upsert(host, c, t.parent, name, node, out);
            *lax = true;
            self.write_content(host, c, node, content, size, mtime, out);
            return CommitResult::Applied { seq: self.head() };
        }
        if self.nodes[&node].attrs.kind != Kind::File {
            return err(ErrCode::Invalid, "setcontent: not a file");
        }
        let n = self.nodes[&node].clone();
        let ok = match self.leases.get(&node) {
            Some(h) => *h == host,
            None => base >= n.content_version,
        };
        if ok {
            let s = self.write_content(host, c, node, content, size, mtime, out);
            return CommitResult::Applied { seq: s };
        }
        // conflict copy: new sibling with conflict name
        let (parent, name) = self.dirents.iter().find(|(_, v)| **v == node).map(|((p, nm), _)| (*p, nm.clone())).unwrap_or((NodeId::ROOT, Name(b"orphan".to_vec())));
        let cn = self.conflict_name(parent, &name, host, ctx);
        let id = self.mint();
        let mut attrs = n.attrs.clone();
        attrs.content = Some(content);
        attrs.size = size;
        attrs.mtime_ns = mtime;
        self.nodes.insert(id, Node { attrs, version: Seq(0), content_version: Seq(0), mods: vec![], fresh: true });
        let s = self.upsert(host, c, parent, cn.clone(), id, out);
        self.nodes.get_mut(&id).unwrap().content_version = s;
        self.transformed(c, host, Resolution::ContentConflictCopy { parent, name: cn }, "content conflict")
    }

    fn write_content(&mut self, host: HostId, c: &Commit, node: NodeId, content: ContentId, size: u64, mtime: i64, out: &mut Vec<Change>) -> Seq {
        let n = self.nodes.get_mut(&node).unwrap();
        n.attrs.content = Some(content);
        n.attrs.size = size;
        n.attrs.mtime_ns = mtime;
        let attrs = n.attrs.clone();
        let s = self.emit(host, c, Effect::Attrs { node, attrs }, out);
        self.touch(node, host, s);
        self.nodes.get_mut(&node).unwrap().content_version = s;
        s
    }

    /// Content ids reachable from live nodes.
    pub fn live_contents(&self) -> Vec<ContentId> {
        self.nodes.values().filter_map(|n| n.attrs.content).collect()
    }
}
