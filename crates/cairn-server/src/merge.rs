//! The merge engine: a pure, synchronous implementation of the PROTOCOL.md
//! merge table. State comes in through the [`Tree`] trait (SQLite in the
//! server, [`mem::MemTree`] in tests); results and the recorded changes
//! come out. Every namespace mutation goes through `Eng::emit`, which both
//! records the `Change` and applies it to the tree, so "replaying the change
//! log reproduces the namespace" holds by construction.

use cairn_proto::*;

pub const NOBODY: u32 = 65534;

#[derive(Clone, Debug, PartialEq)]
pub struct NodeRec {
    pub attrs: Attrs,
    /// Seq of the last change to attrs or content.
    pub version: Seq,
    /// Seq of the last SetContent (or creation).
    pub cver: Seq,
    pub mod_host: HostId,
    pub content_host: HostId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Tomb {
    pub parent: NodeId,
    pub name: Name,
    pub rec: NodeRec,
}

/// uid/gid translation `from..from+len -> to..to+len`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapRange {
    pub from: u32,
    pub to: u32,
    pub len: u32,
}

/// Per-host credential rules (DESIGN §5.1).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Rules {
    /// None = any.
    pub allowed_uids: Option<Vec<(u32, u32)>>,
    pub allowed_gids: Option<Vec<(u32, u32)>>,
    pub root_squash: bool,
    pub uid_map: Vec<MapRange>,
    pub gid_map: Vec<MapRange>,
}

fn map_id(map: &[MapRange], allowed: &Option<Vec<(u32, u32)>>, squash: bool, id: u32) -> Result<u32, ()> {
    let id = map.iter().find(|m| id >= m.from && id - m.from < m.len).map(|m| m.to + (id - m.from)).unwrap_or(id);
    if let Some(a) = allowed {
        if !a.iter().any(|(lo, hi)| *lo <= id && id <= *hi) {
            return Err(());
        }
    }
    if squash && id == 0 {
        return Ok(NOBODY);
    }
    Ok(id)
}

impl Rules {
    pub fn uid(&self, id: u32) -> Result<u32, ErrorReply> {
        map_id(&self.uid_map, &self.allowed_uids, self.root_squash, id).map_err(|_| denied(format!("uid {id} not allowed for this host")))
    }
    pub fn gid(&self, id: u32) -> Result<u32, ErrorReply> {
        map_id(&self.gid_map, &self.allowed_gids, self.root_squash, id).map_err(|_| denied(format!("gid {id} not allowed for this host")))
    }
    pub fn cred(&self, c: Cred) -> Result<Cred, ErrorReply> {
        Ok(Cred { uid: self.uid(c.uid)?, gid: self.gid(c.gid)? })
    }
    /// "0,1000-1999" -> ranges.
    pub fn parse_ranges(s: &str) -> Result<Vec<(u32, u32)>, String> {
        let mut v = vec![];
        for p in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let r = match p.split_once('-') {
                Some((a, b)) => (a.parse().map_err(|e| format!("{p}: {e}"))?, b.parse().map_err(|e| format!("{p}: {e}"))?),
                None => {
                    let a: u32 = p.parse().map_err(|e| format!("{p}: {e}"))?;
                    (a, a)
                }
            };
            v.push(r);
        }
        Ok(v)
    }
    pub fn fmt_ranges(r: &[(u32, u32)]) -> String {
        r.iter().map(|(a, b)| if a == b { a.to_string() } else { format!("{a}-{b}") }).collect::<Vec<_>>().join(",")
    }
    /// "from:to:len,from:to:len"
    pub fn parse_map(s: &str) -> Result<Vec<MapRange>, String> {
        let mut v = vec![];
        for p in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let f: Vec<&str> = p.split(':').collect();
            if f.len() != 3 {
                return Err(format!("bad map entry {p} (want from:to:len)"));
            }
            let n = |x: &str| x.parse::<u32>().map_err(|e| format!("{p}: {e}"));
            v.push(MapRange { from: n(f[0])?, to: n(f[1])?, len: n(f[2])? });
        }
        Ok(v)
    }
    pub fn fmt_map(m: &[MapRange]) -> String {
        m.iter().map(|r| format!("{}:{}:{}", r.from, r.to, r.len)).collect::<Vec<_>>().join(",")
    }
}

fn err(code: ErrCode, msg: impl Into<String>) -> ErrorReply {
    ErrorReply { code, msg: msg.into() }
}
fn denied(msg: String) -> ErrorReply {
    err(ErrCode::PermissionDenied, msg)
}

/// Storage the engine runs on. Mutators are infallible: implementations
/// record the first failure and the caller checks it afterwards.
pub trait Tree {
    fn head(&self) -> Seq;
    fn bump_seq(&mut self) -> Seq;
    fn node(&self, n: NodeId) -> Option<NodeRec>;
    fn dirent(&self, parent: NodeId, name: &Name) -> Option<NodeId>;
    fn has_children(&self, dir: NodeId) -> bool;
    /// All dirents of `node`, sorted by (parent, name).
    fn dirents_of(&self, node: NodeId) -> Vec<(NodeId, Name)>;
    fn tomb(&self, n: NodeId) -> Option<Tomb>;
    fn alias(&self, n: NodeId) -> Option<NodeId>;
    fn lease_holder(&self, n: NodeId) -> Option<HostId>;
    fn has_content(&self, c: &ContentId) -> bool;
    fn op_result(&self, op: OpId) -> Option<CommitResult>;
    /// Next server-minted node id (HostId(0), counter starting at 2; 1 is ROOT).
    fn next_server_node(&mut self) -> NodeId;

    fn put_node(&mut self, n: NodeId, rec: &NodeRec);
    fn del_node(&mut self, n: NodeId);
    fn put_dirent(&mut self, parent: NodeId, name: &Name, node: NodeId);
    fn del_dirent(&mut self, parent: NodeId, name: &Name);
    fn put_tomb(&mut self, n: NodeId, t: &Tomb);
    fn del_tomb(&mut self, n: NodeId);
    fn put_alias(&mut self, from: NodeId, to: NodeId);
    fn add_pending(&mut self, c: ContentId, host: HostId);
    fn log_change(&mut self, c: &Change);
    fn put_op_result(&mut self, op: OpId, r: &CommitResult);
    fn log_conflict(&mut self, rec: &ConflictRecord, cred: Cred);
}

pub struct Ctx<'a> {
    pub host: HostId,
    pub host_name: &'a str,
    pub rules: &'a Rules,
    pub now_secs: u64,
}

pub struct Outcome {
    pub result: CommitResult,
    pub changes: Vec<Change>,
    /// True if this was a duplicate op_id (stored result returned, nothing applied).
    pub replay: bool,
}

/// Apply one commit. Idempotent on `op_id`.
pub fn apply<T: Tree>(t: &mut T, ctx: &Ctx, commit: &Commit) -> Outcome {
    if let Some(r) = t.op_result(commit.op_id) {
        return Outcome { result: r, changes: vec![], replay: true };
    }
    let mut e = Eng { t, ctx, cred: commit.cred, op: commit.op_id, changes: vec![], recreated: None };
    let verdict = match ctx.rules.cred(commit.cred) {
        Ok(c) => {
            e.cred = c;
            e.run(&commit.op)
        }
        Err(er) => Err(er),
    };
    let head = e.t.head();
    let result = match verdict {
        Err(error) => CommitResult::Failed { error },
        Ok(v) => {
            let (res, detail) = match v {
                V::Applied => match e.recreated {
                    Some(dir) => (Some((true, Resolution::DirRecreated { dir })), "directory recreated".to_string()),
                    None => (None, String::new()),
                },
                V::Transformed(r, d) => (Some((true, r)), d),
                V::Rejected(r, d) => (Some((false, r)), d),
            };
            match res {
                None => CommitResult::Applied { seq: head },
                Some((applied, resolution)) => {
                    let conflict = ConflictRecord { seq: head, op_id: commit.op_id, host: ctx.host, resolution, detail };
                    e.t.log_conflict(&conflict, e.cred);
                    if applied {
                        CommitResult::Transformed { seq: head, conflict }
                    } else {
                        CommitResult::Rejected { conflict }
                    }
                }
            }
        }
    };
    let changes = std::mem::take(&mut e.changes);
    t.put_op_result(commit.op_id, &result);
    Outcome { result, changes, replay: false }
}

enum V {
    Applied,
    Transformed(Resolution, String),
    Rejected(Resolution, String),
}
type R = Result<V, ErrorReply>;

#[derive(Clone, Copy, PartialEq)]
enum Touch {
    No,
    Meta,
    Content,
}

struct Eng<'a, T: Tree> {
    t: &'a mut T,
    ctx: &'a Ctx<'a>,
    cred: Cred,
    op: OpId,
    changes: Vec<Change>,
    recreated: Option<NodeId>,
}

pub fn check_name(n: &Name) -> Result<(), ErrorReply> {
    let b = &n.0;
    if b.is_empty() || b.len() > 255 || b.contains(&b'/') || b.contains(&0) || b == b"." || b == b".." {
        return Err(err(ErrCode::Invalid, "invalid name"));
    }
    Ok(())
}

/// `yyyymmddThhmmss` (UTC).
pub fn utc_stamp(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    // civil_from_days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}", rem / 3600, (rem % 3600) / 60, rem % 60)
}

fn norm_content(c: Option<ContentId>) -> ContentId {
    c.unwrap_or(ContentId::Empty)
}

fn equivalent(a: &Attrs, b: &Attrs) -> bool {
    a.kind == b.kind
        && match a.kind {
            Kind::Dir => true,
            Kind::File => norm_content(a.content) == norm_content(b.content),
            Kind::Symlink => a.symlink_target == b.symlink_target,
            _ => a.rdev == b.rdev,
        }
}

impl<'a, T: Tree> Eng<'a, T> {
    fn resolve(&self, mut n: NodeId) -> NodeId {
        for _ in 0..32 {
            match self.t.alias(n) {
                Some(m) if m != n => n = m,
                _ => break,
            }
        }
        n
    }

    fn now_ns(&self) -> i64 {
        (self.ctx.now_secs as i64).saturating_mul(1_000_000_000)
    }

    /// Record and apply one effect.
    fn emit(&mut self, effect: Effect, touch: Touch) -> Seq {
        let seq = self.t.bump_seq();
        let host = self.ctx.host;
        match &effect {
            Effect::Upsert { parent, name, node, attrs } => {
                match self.t.node(*node) {
                    Some(mut r) => {
                        if touch != Touch::No {
                            r.attrs = attrs.clone();
                        }
                        self.t.put_node(*node, &r);
                    }
                    None => {
                        self.t.put_node(*node, &NodeRec { attrs: attrs.clone(), version: seq, cver: seq, mod_host: host, content_host: host });
                        self.t.del_tomb(*node);
                    }
                }
                self.t.put_dirent(*parent, name, *node);
            }
            Effect::Remove { parent, name, node, node_deleted } => {
                self.t.del_dirent(*parent, name);
                if *node_deleted {
                    if let Some(rec) = self.t.node(*node) {
                        self.t.put_tomb(*node, &Tomb { parent: *parent, name: name.clone(), rec });
                    }
                    self.t.del_node(*node);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                self.t.del_dirent(*src_parent, src_name);
                self.t.put_dirent(*dst_parent, dst_name, *node);
            }
            Effect::Attrs { node, attrs } => {
                if let Some(mut r) = self.t.node(*node) {
                    r.attrs = attrs.clone();
                    match touch {
                        Touch::No => {}
                        Touch::Meta => {
                            r.version = seq;
                            r.mod_host = host;
                        }
                        Touch::Content => {
                            r.version = seq;
                            r.cver = seq;
                            r.mod_host = host;
                            r.content_host = host;
                        }
                    }
                    self.t.put_node(*node, &r);
                }
            }
        }
        let change = Change { seq, origin: Some(self.op), host, cred: self.cred, effect };
        self.t.log_change(&change);
        self.changes.push(change);
        seq
    }

    fn note_content(&mut self, c: Option<ContentId>) {
        if let Some(c) = c {
            if c != ContentId::Empty && !self.t.has_content(&c) {
                self.t.add_pending(c, self.ctx.host);
            }
        }
    }

    fn conflict_name(&self, parent: NodeId, base: &Name) -> Name {
        let suffix = format!(".conflict-{}-{}", self.ctx.host_name, utc_stamp(self.ctx.now_secs));
        let mut n = 1u32;
        loop {
            let tail = if n == 1 { suffix.clone() } else { format!("{suffix}-{n}") };
            let keep = 255usize.saturating_sub(tail.len()).min(base.0.len());
            let mut v = base.0[..keep].to_vec();
            v.extend_from_slice(tail.as_bytes());
            let cand = Name(v);
            if self.t.dirent(parent, &cand).is_none() {
                return cand;
            }
            n += 1;
        }
    }

    /// Make `dir` a live directory, resurrecting it (and its ancestors) from
    /// tombstones if needed.
    fn ensure_dir(&mut self, dir: NodeId, depth: u32) -> Result<(), ErrorReply> {
        if depth > 4096 {
            return Err(err(ErrCode::Invalid, "path too deep"));
        }
        if let Some(r) = self.t.node(dir) {
            return if r.attrs.kind == Kind::Dir { Ok(()) } else { Err(err(ErrCode::NotDir, "not a directory")) };
        }
        if dir == NodeId::ROOT {
            return Err(err(ErrCode::Internal, "root node missing"));
        }
        let tomb = self.t.tomb(dir).ok_or_else(|| err(ErrCode::NotFound, "no such directory"))?;
        if tomb.rec.attrs.kind != Kind::Dir {
            return Err(err(ErrCode::NotDir, "not a directory"));
        }
        self.ensure_dir(tomb.parent, depth + 1)?;
        let name = if self.t.dirent(tomb.parent, &tomb.name).is_none() { tomb.name.clone() } else { self.conflict_name(tomb.parent, &tomb.name) };
        self.emit(Effect::Upsert { parent: tomb.parent, name, node: dir, attrs: tomb.rec.attrs.clone() }, Touch::Content);
        self.recreated = Some(dir);
        Ok(())
    }

    /// Resurrect a deleted non-directory node at (parent,name) if free, else
    /// at a conflict name. Returns the name used.
    fn resurrect_at(&mut self, node: NodeId, tomb: &Tomb, parent: NodeId, name: &Name) -> Name {
        let n = if self.t.dirent(parent, name).is_none() { name.clone() } else { self.conflict_name(parent, name) };
        self.emit(Effect::Upsert { parent, name: n.clone(), node, attrs: tomb.rec.attrs.clone() }, Touch::Content);
        n
    }

    fn fix_attrs(&self, mut a: Attrs) -> Result<Attrs, ErrorReply> {
        a.uid = self.ctx.rules.uid(a.uid)?;
        a.gid = self.ctx.rules.gid(a.gid)?;
        a.xattrs.sort();
        Ok(a)
    }

    fn is_ancestor(&self, anc: NodeId, mut cur: NodeId) -> bool {
        for _ in 0..4096 {
            if cur == anc {
                return true;
            }
            if cur == NodeId::ROOT {
                return false;
            }
            let parent = match self.t.node(cur) {
                Some(_) => self.t.dirents_of(cur).first().map(|(p, _)| *p),
                None => self.t.tomb(cur).map(|t| t.parent),
            };
            match parent {
                Some(p) => cur = p,
                None => return false,
            }
        }
        true
    }

    fn reannounce(&mut self, parent: NodeId, name: &Name, node: NodeId) {
        if let Some(r) = self.t.node(node) {
            self.emit(Effect::Upsert { parent, name: name.clone(), node, attrs: r.attrs }, Touch::No);
        }
    }

    fn run(&mut self, op: &Op) -> R {
        match op {
            Op::Create { parent, name, node, attrs } => self.create(*parent, name, *node, attrs.clone()),
            Op::Link { parent, name, node } => self.link(*parent, name, *node),
            Op::Unlink { parent, name, node, base } => self.unlink(*parent, name, *node, *base),
            Op::Rename { src_parent, src_name, node, dst_parent, dst_name, replaces, mode } => {
                self.rename(*src_parent, src_name, *node, *dst_parent, dst_name, *replaces, *mode)
            }
            Op::SetAttr { node, changes, .. } => self.set_attr(*node, changes),
            Op::SetXattr { node, name, value } => self.set_xattr(*node, name, value.clone()),
            Op::SetContent { node, base, content, size, mtime_ns } => self.set_content(*node, *base, *content, *size, *mtime_ns),
        }
    }

    fn create(&mut self, parent: NodeId, name: &Name, node: NodeId, attrs: Attrs) -> R {
        check_name(name)?;
        let parent = self.resolve(parent);
        let attrs = self.fix_attrs(attrs)?;
        if let Some(cur) = self.t.dirent(parent, name) {
            if cur == node {
                return Ok(V::Applied);
            }
            if self.t.alias(node) == Some(cur) {
                return Ok(V::Transformed(Resolution::Adopted { node: cur }, "name already adopted".into()));
            }
        }
        if self.t.node(node).is_some() || self.t.alias(node).is_some() {
            return Err(err(ErrCode::Exists, "node id already in use"));
        }
        self.ensure_dir(parent, 0)?;
        match self.t.dirent(parent, name) {
            None => {
                self.note_content(attrs.content);
                self.emit(Effect::Upsert { parent, name: name.clone(), node, attrs }, Touch::Content);
                Ok(V::Applied)
            }
            Some(ex) => {
                let exa = self.t.node(ex).map(|r| r.attrs);
                if exa.as_ref().map(|a| equivalent(a, &attrs)).unwrap_or(false) {
                    self.t.put_alias(node, ex);
                    return Ok(V::Transformed(Resolution::Adopted { node: ex }, "equivalent node already present".into()));
                }
                let cn = self.conflict_name(parent, name);
                self.note_content(attrs.content);
                self.emit(Effect::Upsert { parent, name: cn.clone(), node, attrs }, Touch::Content);
                Ok(V::Transformed(Resolution::ConflictCopy { parent, name: cn }, "name taken by different node".into()))
            }
        }
    }

    fn link(&mut self, parent: NodeId, name: &Name, node: NodeId) -> R {
        check_name(name)?;
        let parent = self.resolve(parent);
        let node = self.resolve(node);
        let live = self.t.node(node);
        let tomb = if live.is_none() { self.t.tomb(node) } else { None };
        let attrs = match (&live, &tomb) {
            (Some(r), _) => r.attrs.clone(),
            (None, Some(t)) => t.rec.attrs.clone(),
            _ => return Err(err(ErrCode::NotFound, "no such node")),
        };
        if attrs.kind == Kind::Dir {
            return Err(err(ErrCode::IsDir, "cannot hardlink a directory"));
        }
        self.ensure_dir(parent, 0)?;
        let cur = self.t.dirent(parent, name);
        if live.is_some() && cur == Some(node) {
            return Ok(V::Applied);
        }
        let (use_name, taken) = match cur {
            None => (name.clone(), false),
            Some(_) => (self.conflict_name(parent, name), true),
        };
        match tomb {
            Some(t) => {
                self.emit(Effect::Upsert { parent, name: use_name.clone(), node, attrs: t.rec.attrs.clone() }, Touch::Content);
            }
            None => {
                self.emit(Effect::Upsert { parent, name: use_name.clone(), node, attrs }, Touch::No);
            }
        }
        if taken {
            Ok(V::Transformed(Resolution::ConflictCopy { parent, name: use_name }, "link name taken".into()))
        } else {
            Ok(V::Applied)
        }
    }

    fn unlink(&mut self, parent: NodeId, name: &Name, node: NodeId, base: Seq) -> R {
        check_name(name)?;
        let parent = self.resolve(parent);
        let node = self.resolve(node);
        let cur = match self.t.dirent(parent, name) {
            None => return Ok(V::Applied),
            Some(c) => c,
        };
        if cur != node {
            self.reannounce(parent, name, cur);
            return Ok(V::Transformed(Resolution::DeleteLost, "dirent now points elsewhere".into()));
        }
        let rec = match self.t.node(node) {
            Some(r) => r,
            None => return Ok(V::Applied),
        };
        let links = self.t.dirents_of(node).len();
        let last = rec.attrs.kind == Kind::Dir || links <= 1;
        if last {
            if rec.attrs.kind == Kind::Dir && self.t.has_children(node) {
                self.reannounce(parent, name, node);
                return Ok(V::Transformed(Resolution::DeleteLost, "directory not empty".into()));
            }
            if rec.version > base && rec.mod_host != self.ctx.host {
                self.reannounce(parent, name, node);
                return Ok(V::Transformed(Resolution::DeleteLost, "node modified by another host".into()));
            }
        }
        self.emit(Effect::Remove { parent, name: name.clone(), node, node_deleted: last }, Touch::No);
        Ok(V::Applied)
    }

    #[allow(clippy::too_many_arguments)]
    fn rename(&mut self, sp: NodeId, sn: &Name, node: NodeId, dp: NodeId, dn: &Name, replaces: Option<NodeId>, mode: RenameMode) -> R {
        check_name(sn)?;
        check_name(dn)?;
        let sp = self.resolve(sp);
        let dp = self.resolve(dp);
        let node = self.resolve(node);
        let replaces = replaces.map(|r| self.resolve(r));
        let live = self.t.node(node);
        let src_cur = self.t.dirent(sp, sn);
        let mut tomb = None;
        match (&live, src_cur) {
            (Some(_), Some(c)) if c == node => {}
            (Some(_), _) => return Ok(V::Rejected(Resolution::RenameLost, "source moved by another host".into())),
            (None, _) => match self.t.tomb(node) {
                Some(t) => tomb = Some(t),
                None => return Err(err(ErrCode::NotFound, "no such node")),
            },
        }
        let attrs = match (&live, &tomb) {
            (Some(r), _) => r.attrs.clone(),
            (_, Some(t)) => t.rec.attrs.clone(),
            _ => unreachable!(),
        };
        if attrs.kind == Kind::Dir && self.is_ancestor(node, dp) {
            return Ok(V::Rejected(Resolution::CycleRejected, "rename would create a cycle".into()));
        }
        self.ensure_dir(dp, 0)?;
        let dst = self.t.dirent(dp, dn);
        if live.is_some() && dst == Some(node) {
            return Ok(V::Applied);
        }
        let resurrect = tomb.is_some();
        let mut verdict = V::Applied;
        let target_name: Name;
        match dst {
            None => target_name = dn.clone(),
            Some(ex) => {
                let exr = self.t.node(ex);
                let expected = mode != RenameMode::NoReplace && replaces == Some(ex);
                let compatible = match &exr {
                    None => false,
                    Some(r) => {
                        if mode == RenameMode::Exchange && !resurrect {
                            !(r.attrs.kind == Kind::Dir && self.is_ancestor(ex, sp))
                        } else if attrs.kind == Kind::Dir {
                            r.attrs.kind == Kind::Dir && !self.t.has_children(ex)
                        } else {
                            r.attrs.kind != Kind::Dir
                        }
                    }
                };
                if expected && compatible {
                    let exr = exr.unwrap();
                    if mode == RenameMode::Exchange && !resurrect {
                        self.emit(Effect::Remove { parent: dp, name: dn.clone(), node: ex, node_deleted: false }, Touch::No);
                        self.emit(Effect::Move { src_parent: sp, src_name: sn.clone(), dst_parent: dp, dst_name: dn.clone(), node }, Touch::No);
                        self.emit(Effect::Upsert { parent: sp, name: sn.clone(), node: ex, attrs: exr.attrs }, Touch::No);
                        return Ok(self.done(verdict));
                    }
                    let last = exr.attrs.kind == Kind::Dir || self.t.dirents_of(ex).len() <= 1;
                    self.emit(Effect::Remove { parent: dp, name: dn.clone(), node: ex, node_deleted: last }, Touch::No);
                    target_name = dn.clone();
                } else {
                    target_name = self.conflict_name(dp, dn);
                    verdict = V::Transformed(Resolution::ConflictCopy { parent: dp, name: target_name.clone() }, "rename destination occupied".into());
                }
            }
        }
        match tomb {
            Some(t) => {
                self.emit(Effect::Upsert { parent: dp, name: target_name, node, attrs: t.rec.attrs.clone() }, Touch::Content);
            }
            None => {
                self.emit(Effect::Move { src_parent: sp, src_name: sn.clone(), dst_parent: dp, dst_name: target_name, node }, Touch::No);
            }
        }
        Ok(self.done(verdict))
    }

    fn done(&self, v: V) -> V {
        v
    }

    fn set_attr(&mut self, node: NodeId, ch: &AttrChanges) -> R {
        let node = self.resolve(node);
        let mut rec = self.t.node(node).ok_or_else(|| err(ErrCode::NotFound, "no such node"))?;
        let mut a = rec.attrs.clone();
        if let Some(m) = ch.mode {
            a.mode = m & 0o7777;
        }
        if let Some(u) = ch.uid {
            a.uid = self.ctx.rules.uid(u)?;
        }
        if let Some(g) = ch.gid {
            a.gid = self.ctx.rules.gid(g)?;
        }
        if let Some(t) = ch.atime_ns {
            a.atime_ns = t;
        }
        if let Some(t) = ch.mtime_ns {
            a.mtime_ns = t;
        }
        a.ctime_ns = self.now_ns();
        rec.attrs = a.clone();
        self.emit(Effect::Attrs { node, attrs: a }, Touch::Meta);
        Ok(V::Applied)
    }

    fn set_xattr(&mut self, node: NodeId, name: &[u8], value: Option<Vec<u8>>) -> R {
        let node = self.resolve(node);
        let rec = self.t.node(node).ok_or_else(|| err(ErrCode::NotFound, "no such node"))?;
        if name.is_empty() || name.len() > 255 {
            return Err(err(ErrCode::Invalid, "bad xattr name"));
        }
        let mut a = rec.attrs;
        a.xattrs.retain(|(n, _)| n != name);
        if let Some(v) = value {
            a.xattrs.push((name.to_vec(), v));
        }
        a.xattrs.sort();
        a.ctime_ns = self.now_ns();
        self.emit(Effect::Attrs { node, attrs: a }, Touch::Meta);
        Ok(V::Applied)
    }

    fn set_content(&mut self, node: NodeId, base: Seq, content: ContentId, size: u64, mtime_ns: i64) -> R {
        let node = self.resolve(node);
        let rec = match self.t.node(node) {
            Some(r) => r,
            None => {
                // Deleted: modify wins. Resurrect at the old location, then apply.
                let tomb = self.t.tomb(node).ok_or_else(|| err(ErrCode::NotFound, "no such node"))?;
                if tomb.rec.attrs.kind != Kind::File {
                    return Err(err(ErrCode::Invalid, "not a regular file"));
                }
                self.ensure_dir(tomb.parent, 0)?;
                self.resurrect_at(node, &tomb, tomb.parent, &tomb.name);
                let mut attrs = tomb.rec.attrs.clone();
                self.apply_content(&mut attrs, content, size, mtime_ns);
                self.note_content(Some(content));
                self.emit(Effect::Attrs { node, attrs }, Touch::Content);
                return Ok(V::Applied);
            }
        };
        match rec.attrs.kind {
            Kind::File => {}
            Kind::Dir => return Err(err(ErrCode::IsDir, "is a directory")),
            _ => return Err(err(ErrCode::Invalid, "not a regular file")),
        }
        let ok = match self.t.lease_holder(node) {
            Some(h) => h == self.ctx.host,
            None => base >= rec.cver,
        };
        let mut attrs = rec.attrs.clone();
        self.apply_content(&mut attrs, content, size, mtime_ns);
        self.note_content(Some(content));
        if ok {
            self.emit(Effect::Attrs { node, attrs }, Touch::Content);
            return Ok(V::Applied);
        }
        let (parent, name) = self.t.dirents_of(node).into_iter().next().ok_or_else(|| err(ErrCode::Internal, "live node without dirent"))?;
        let cn = self.conflict_name(parent, &name);
        let new = self.t.next_server_node();
        self.emit(Effect::Upsert { parent, name: cn.clone(), node: new, attrs }, Touch::Content);
        Ok(V::Transformed(Resolution::ContentConflictCopy { parent, name: cn }, "content written without lease on stale base".into()))
    }

    fn apply_content(&self, a: &mut Attrs, content: ContentId, size: u64, mtime_ns: i64) {
        a.content = Some(content);
        a.size = size;
        a.mtime_ns = mtime_ns;
        a.ctime_ns = self.now_ns();
    }
}

/// In-memory `Tree` for tests (and for model checking).
pub mod mem {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

    #[derive(Default)]
    pub struct MemTree {
        pub head: u64,
        pub nodes: BTreeMap<NodeId, NodeRec>,
        pub dirents: BTreeMap<(NodeId, Name), NodeId>,
        pub tombs: HashMap<NodeId, Tomb>,
        pub aliases: HashMap<NodeId, NodeId>,
        pub leases: HashMap<NodeId, HostId>,
        pub content: HashSet<ContentId>,
        pub pending: BTreeSet<(Vec<u8>, HostId)>,
        pub pending_list: Vec<(ContentId, HostId)>,
        pub ops: HashMap<OpId, CommitResult>,
        pub conflicts: Vec<ConflictRecord>,
        pub log: Vec<Change>,
        pub srv_counter: u64,
    }

    impl MemTree {
        pub fn new() -> MemTree {
            let mut t = MemTree::default();
            t.nodes.insert(
                NodeId::ROOT,
                NodeRec { attrs: dir_attrs(), version: Seq(0), cver: Seq(0), mod_host: HostId(0), content_host: HostId(0) },
            );
            t
        }
        /// Namespace as (parent, name, node, attrs) rows.
        pub fn rows(&self) -> Vec<(NodeId, Name, NodeId, Attrs)> {
            self.dirents.iter().map(|((p, n), id)| (*p, n.clone(), *id, self.nodes[id].attrs.clone())).collect()
        }
    }

    pub fn dir_attrs() -> Attrs {
        Attrs {
            kind: Kind::Dir,
            mode: 0o755,
            uid: 1000,
            gid: 1000,
            size: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            atime_ns: 0,
            rdev: 0,
            content: None,
            symlink_target: None,
            xattrs: vec![],
        }
    }

    impl Tree for MemTree {
        fn head(&self) -> Seq {
            Seq(self.head)
        }
        fn bump_seq(&mut self) -> Seq {
            self.head += 1;
            Seq(self.head)
        }
        fn node(&self, n: NodeId) -> Option<NodeRec> {
            self.nodes.get(&n).cloned()
        }
        fn dirent(&self, parent: NodeId, name: &Name) -> Option<NodeId> {
            self.dirents.get(&(parent, name.clone())).copied()
        }
        fn has_children(&self, dir: NodeId) -> bool {
            self.dirents.range((dir, Name(vec![]))..).next().map(|((p, _), _)| *p == dir).unwrap_or(false)
        }
        fn dirents_of(&self, node: NodeId) -> Vec<(NodeId, Name)> {
            self.dirents.iter().filter(|(_, n)| **n == node).map(|((p, nm), _)| (*p, nm.clone())).collect()
        }
        fn tomb(&self, n: NodeId) -> Option<Tomb> {
            self.tombs.get(&n).cloned()
        }
        fn alias(&self, n: NodeId) -> Option<NodeId> {
            self.aliases.get(&n).copied()
        }
        fn lease_holder(&self, n: NodeId) -> Option<HostId> {
            self.leases.get(&n).copied()
        }
        fn has_content(&self, c: &ContentId) -> bool {
            *c == ContentId::Empty || self.content.contains(c)
        }
        fn op_result(&self, op: OpId) -> Option<CommitResult> {
            self.ops.get(&op).cloned()
        }
        fn next_server_node(&mut self) -> NodeId {
            self.srv_counter = self.srv_counter.max(1) + 1; // HostId(0)/1 is ROOT
            NodeId::new(HostId(0), self.srv_counter)
        }
        fn put_node(&mut self, n: NodeId, rec: &NodeRec) {
            self.nodes.insert(n, rec.clone());
        }
        fn del_node(&mut self, n: NodeId) {
            self.nodes.remove(&n);
        }
        fn put_dirent(&mut self, parent: NodeId, name: &Name, node: NodeId) {
            self.dirents.insert((parent, name.clone()), node);
        }
        fn del_dirent(&mut self, parent: NodeId, name: &Name) {
            self.dirents.remove(&(parent, name.clone()));
        }
        fn put_tomb(&mut self, n: NodeId, t: &Tomb) {
            self.tombs.insert(n, t.clone());
        }
        fn del_tomb(&mut self, n: NodeId) {
            self.tombs.remove(&n);
        }
        fn put_alias(&mut self, from: NodeId, to: NodeId) {
            self.aliases.insert(from, to);
        }
        fn add_pending(&mut self, c: ContentId, host: HostId) {
            self.pending_list.push((c, host));
        }
        fn log_change(&mut self, c: &Change) {
            self.log.push(c.clone());
        }
        fn put_op_result(&mut self, op: OpId, r: &CommitResult) {
            self.ops.insert(op, r.clone());
        }
        fn log_conflict(&mut self, rec: &ConflictRecord, _cred: Cred) {
            self.conflicts.push(rec.clone());
        }
    }
}
