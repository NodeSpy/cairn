//! `RootCore`: the per-root state machine implementing [`Core`].
//!
//! State: `synced` (server state at `applied_seq`), `visible` (synced +
//! pending ops, what FUSE sees), the `pending` journal, ino tables, per-node
//! file state (live files, leases) and the applier thread that replays
//! remote changes. See docs/CLIENT.md.

use crate::api::*;
use crate::cas::object_hash;
use crate::client::Shared;
use crate::config::{PolicyMode, RootConfig};
use crate::conn::{fetch_listing, CallErr};
use crate::store::{DbOp, LoadedRoot};
use crate::tree::*;
use cairn_proto::*;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Notify};

pub mod ckpt;
mod fold;
mod pipeline;
use ckpt::*;
use pipeline::*;

/// `base` sentinel: "content version unknown yet, patch from the previous result".
const BASE_DEP: Seq = Seq(u64::MAX);
/// Ops per `Commit` request (the server accepts 4096) and a rough cap on its size in bytes.
const MAX_BATCH: usize = 4000;
const MAX_BATCH_BYTES: usize = 4 << 20;

fn op_size(op: &Op) -> usize {
    let attrs = |a: &Attrs| 160 + a.xattrs.iter().map(|(k, v)| k.len() + v.len() + 16).sum::<usize>() + a.symlink_target.as_ref().map(|t| t.len()).unwrap_or(0);
    match op {
        Op::Create { name, attrs: a, .. } => 64 + name.0.len() + attrs(a),
        Op::Link { name, .. } | Op::Unlink { name, .. } => 64 + name.0.len(),
        Op::Rename { src_name, dst_name, .. } => 96 + src_name.0.len() + dst_name.0.len(),
        Op::SetXattr { name, value, .. } => 64 + name.len() + value.as_ref().map(|v| v.len()).unwrap_or(0),
        _ => 96,
    }
}
/// Truncate beyond this fails with EFBIG (hashing a huge sparse file would stall the daemon).
const MAX_FILE_SIZE: u64 = 64 << 30;

/// One unacknowledged commit in the outbound journal.
#[derive(Clone, Debug)]
pub struct Pend {
    pub n: u64,
    pub op: Op,
    pub cred: Cred,
    pub ts: i64,
    pub sent: bool,
}

#[derive(Default, Clone, Debug)]
pub struct FileState {
    pub ino: u64,
    pub live: bool,
    pub opens: u32,
    pub writers: u32,
    pub dirty: bool,
    /// Visible content changed remotely: the live file must be refreshed before use.
    pub stale: bool,
    pub idle_seq: u64,
    pub last_cred: Option<(u32, u32)>,
    /// (size, mtime_ns, ctime_ns) of the live file when the first writer opened it clean.
    pub open_stat: Option<(u64, i64, i64)>,
    /// Bumped whenever the live file may start changing (write open, truncate): a hash taken
    /// before is void.
    pub wgen: u64,
    /// In the pipeline queue or being processed.
    pub queued: bool,
    /// A save event arrived while it was being processed.
    pub rehash: bool,
    /// Next pass must hash a snapshot (the file has writers).
    pub hot: bool,
    /// A `DbOp::File` row exists for this node.
    pub row_dirty: bool,
    /// recovered `published` row: the content the journal says was published (checked in a worker)
    pub recover_pub: Option<ContentId>,
    /// Pending commit job (stage "published").
    pub job: Option<u64>,
    /// Last checkpoint of this open file: (when, stat).
    pub ckpt: Option<(Instant, Stat)>,
    /// When the file was first opened for write in the current dirty period.
    pub write_since: Option<Instant>,
    /// Last time a checkpoint of this open file was published.
    pub last_pub: Option<Instant>,
}

struct OpenRec {
    node: NodeId,
    write: bool,
    remote: bool,
}

pub enum Work {
    Changes(Vec<Change>),
    Resync { listings: Vec<(NodeId, Vec<DirEntry>)>, applied: Seq, epoch: Option<Epoch>, done: oneshot::Sender<()> },
    Reconcile { fp: Footprint, seq: Seq },
    Barrier(std::sync::mpsc::SyncSender<()>),
    Stop,
}

pub struct RootState {
    name: String,
    db: Arc<crate::store::Db>,
    pub visible: Tree,
    pub synced: Tree,
    pub pending: VecDeque<Pend>,
    pub pidx: PendingIndex,
    ino_of: HashMap<NodeId, (u64, u64)>,
    node_of: HashMap<u64, NodeId>,
    next_ino: u64,
    pub files: HashMap<NodeId, FileState>,
    opens: HashMap<u64, OpenRec>,
    next_token: u64,
    pub applied_seq: Seq,
    pub epoch: Option<Epoch>,
    recent: VecDeque<Change>,
    inflight: Option<Change>,
    pub leases: HashSet<NodeId>,
    local_only: HashSet<NodeId>,
    local_keys: HashSet<(NodeId, NameB)>,
    release_after: HashMap<u64, NodeId>,
    idle: VecDeque<(NodeId, u64)>,
    idle_ctr: u64,
    zombies: HashMap<NodeId, Node>,
    /// insertion order of `zombies` (FIFO eviction; may hold stale ids)
    zombie_order: VecDeque<NodeId>,
    /// Set while processing a change that is followed by more changes of the same commit.
    defer_gc: bool,
    /// dirents/nodes of remote changes withheld from the visible tree because a pending op touched them
    skipped: Footprint,
    /// Highest seq produced by one of our own commits (we know that state without the stream).
    own_seq: Seq,
}

const RECENT_MAX: usize = 20_000;

/// Writes the op log out (`write(2)`) when dropped. Declared before the state lock in a handler, so
/// the syscall runs after the lock is released: a write that waits behind a concurrent fsync of the
/// log file (btrfs inode lock) must not stall every other operation on the root.
pub(crate) struct SyncOnDrop<'a>(pub &'a crate::store::Db);
impl Drop for SyncOnDrop<'_> {
    fn drop(&mut self) {
        self.0.sync_point();
    }
}

impl RootState {
    fn ino(&mut self, node: NodeId) -> (u64, u64) {
        if let Some(x) = self.ino_of.get(&node) {
            return *x;
        }
        let ino = self.next_ino;
        self.next_ino += 1;
        self.ino_of.insert(node, (ino, 0));
        self.node_of.insert(ino, node);
        self.db.send(DbOp::Ino { root: self.name.clone(), node, ino, gen: 0 });
        (ino, 0)
    }

    fn persist(&mut self) {
        let root = self.name.clone();
        let d = std::mem::take(&mut self.synced.dirty);
        for n in d.nodes {
            let row = self.synced.nodes.get(&n).map(|x| (x.version, x.cversion, x.attrs.clone()));
            self.db.send(DbOp::Node { root: root.clone(), node: n, row });
        }
        for (p, name) in d.dirents {
            let node = self.synced.lookup(p, &name);
            self.db.send(DbOp::Dirent { root: root.clone(), parent: p, name, node });
        }
        for n in d.listed {
            let listed = self.synced.listed.contains(&n);
            self.db.send(DbOp::Listed { root: root.clone(), node: n, listed });
        }
        let d = std::mem::take(&mut self.visible.dirty);
        if self.local_only.is_empty() && self.local_keys.is_empty() {
            return;
        }
        for n in d.nodes {
            if self.local_only.contains(&n) {
                match self.visible.nodes.get(&n) {
                    Some(x) => self.db.send(DbOp::LocalNode { root: root.clone(), node: n, attrs: Some(x.attrs.clone()) }),
                    None => {
                        self.db.send(DbOp::LocalNode { root: root.clone(), node: n, attrs: None });
                        self.local_only.remove(&n);
                    }
                }
            }
        }
        for (p, name) in d.dirents {
            let key = (p, name.clone());
            match self.visible.lookup(p, &name) {
                Some(o) if self.local_only.contains(&o) => {
                    self.db.send(DbOp::LocalDirent { root: root.clone(), parent: p, name, node: Some(o) });
                    self.local_keys.insert(key);
                }
                _ => {
                    if self.local_keys.remove(&key) {
                        self.db.send(DbOp::LocalDirent { root: root.clone(), parent: p, name, node: None });
                    }
                }
            }
        }
    }

    fn stash_zombie(&mut self, node: NodeId) {
        if let Some(n) = self.visible.nodes.get(&node) {
            // directories may be held open by the kernel/FUSE layer too: keep them reachable by ino
            // any removed node stays reachable by ino (kernel file handles, open_by_handle): bounded table
            {
                if self.zombies.len() > 10_000 {
                    // oldest first; ones with an open handle or unpublished writes stay
                    let mut budget = 3 * self.zombies.len();
                    while self.zombies.len() > 5_000 && budget > 0 {
                        budget -= 1;
                        let Some(k) = self.zombie_order.pop_front() else { break };
                        if self.zombies.contains_key(&k) {
                            if self.files.get(&k).is_some_and(|f| f.opens > 0 || f.writers > 0 || f.dirty) {
                                self.zombie_order.push_back(k);
                            } else {
                                self.zombies.remove(&k);
                            }
                        }
                    }
                }
                if self.zombie_order.len() > 4 * self.zombies.len() + 1_000 {
                    let z = &self.zombies;
                    self.zombie_order.retain(|k| z.contains_key(k));
                }
                if self.zombies.insert(node, n.clone()).is_none() {
                    self.zombie_order.push_back(node);
                }
            }
        }
    }

    fn gc_visible(&mut self) {
        // keep nodes that are still open (unlinked-but-open files) reachable by ino
        for n in self.visible.orphan_candidates() {
            self.stash_zombie(n);
        }
        self.visible.gc(&|_| false);
    }

    /// Visible-tree decision for a server fact: apply the pending-ops-win
    /// rules; `None` = nothing to show.
    fn rebase(&self, eff: &Effect) -> Option<Effect> {
        let now = now_ns();
        let keep_content = |node: NodeId, a: &mut Attrs| {
            if self.files.get(&node).map(|f| f.writers > 0 || f.dirty).unwrap_or(false) {
                if let Some(v) = self.visible.node(node) {
                    a.content = v.attrs.content;
                    a.size = v.attrs.size;
                    a.mtime_ns = v.attrs.mtime_ns;
                }
            }
        };
        let overlay = |node: NodeId, a: &mut Attrs| {
            if self.pidx.has_attr_ops(node) {
                overlay_attrs(a, node, &mut self.pending.iter().map(|p| &p.op), now);
            }
        };
        match eff {
            Effect::Upsert { parent, name, node, attrs } => {
                if self.pidx.touches_dirent(*parent, &name.0) {
                    return None;
                }
                let mut a = attrs.clone();
                overlay(*node, &mut a);
                keep_content(*node, &mut a);
                Some(Effect::Upsert { parent: *parent, name: name.clone(), node: *node, attrs: a })
            }
            Effect::Remove { parent, name, .. } => {
                if self.pidx.touches_dirent(*parent, &name.0) {
                    None
                } else {
                    Some(eff.clone())
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, .. } => {
                if self.pidx.touches_dirent(*src_parent, &src_name.0) || self.pidx.touches_dirent(*dst_parent, &dst_name.0) {
                    None
                } else {
                    Some(eff.clone())
                }
            }
            Effect::Attrs { node, attrs } => {
                let mut a = attrs.clone();
                overlay(*node, &mut a);
                keep_content(*node, &mut a);
                Some(Effect::Attrs { node: *node, attrs: a })
            }
        }
    }

    /// Apply an effect to the visible tree (idempotent) and its side effects.
    fn apply_visible(&mut self, c: &Change) {
        let nodes: Vec<NodeId> = match &c.effect {
            Effect::Upsert { node, .. } | Effect::Attrs { node, .. } | Effect::Move { node, .. } => vec![*node],
            Effect::Remove { node, .. } => vec![*node],
        };
        let before: Vec<Option<ContentId>> = nodes.iter().map(|n| self.visible.node(*n).and_then(|x| x.attrs.content)).collect();
        match &c.effect {
            Effect::Remove { node, node_deleted, .. } if *node_deleted => self.stash_zombie(*node),
            Effect::Upsert { parent, name, .. } => {
                if let Some(o) = self.visible.lookup(*parent, &name.0) {
                    if self.visible.dirents_of(o).len() <= 1 {
                        self.stash_zombie(o);
                    }
                }
            }
            _ => {}
        }
        let newborn_dir = match &c.effect {
            Effect::Upsert { node, attrs, .. } if attrs.kind == Kind::Dir && !self.visible.nodes.contains_key(node) => Some(*node),
            _ => None,
        };
        self.visible.apply_effect(&c.effect, c.seq);
        if let Some(d) = newborn_dir {
            self.adopt_dir_from_synced(d);
        }
        if !self.defer_gc && !keeps_orphan(&c.effect) {
            self.gc_visible();
        }
        for (i, n) in nodes.iter().enumerate() {
            // versions track the synced tree
            if let Some(s) = self.synced.nodes.get(n).map(|s| (s.version, s.cversion)) {
                if let Some(v) = self.visible.node_mut(*n) {
                    v.version = s.0;
                    v.cversion = s.1;
                }
            }
            let after = self.visible.node(*n).and_then(|x| x.attrs.content);
            if after != before[i] {
                if let Some(f) = self.files.get_mut(n) {
                    f.stale = true;
                }
            }
        }
        self.persist();
    }

    /// A directory (re)appeared in the visible tree (e.g. a delete lost against
    /// concurrent creates inside it): take its children and listed state from
    /// the synced tree instead of assuming it is empty.
    fn adopt_dir_from_synced(&mut self, d: NodeId) {
        if !self.synced.nodes.contains_key(&d) {
            return; // brand new on the server: empty and complete
        }
        let listed = self.synced.listed.contains(&d);
        self.visible.set_listed(d, listed);
        if !listed {
            return;
        }
        let kids: Vec<(NameB, NodeId)> = self.synced.children(d).map(|c| c.iter().map(|(k, v)| (k.clone(), *v)).collect()).unwrap_or_default();
        for (name, c) in kids {
            if self.pidx.touches_dirent(d, &name) {
                continue;
            }
            if !self.visible.nodes.contains_key(&c) {
                if let Some(n) = self.synced.nodes.get(&c).cloned() {
                    self.visible.insert_node(c, n);
                }
            }
            self.visible.set_dirent(d, &name, c);
        }
    }

    fn copy_versions(&mut self, eff: &Effect) {
        let node = match eff {
            Effect::Upsert { node, .. } | Effect::Attrs { node, .. } | Effect::Move { node, .. } | Effect::Remove { node, .. } => *node,
        };
        if let Some(s) = self.synced.nodes.get(&node).map(|s| (s.version, s.cversion)) {
            if let Some(v) = self.visible.node_mut(node) {
                v.version = s.0;
                v.cversion = s.1;
            }
        }
    }

    fn push_pending(&mut self, n: u64, op: Op, cred: Cred, ts: i64) {
        self.db.send(DbOp::Outbound { root: self.name.clone(), n, row: Some((op.clone(), cred, ts)) });
        self.pidx.add(&op);
        self.pending.push_back(Pend { n, op, cred, ts, sent: false });
    }

    fn rebuild_pidx(&mut self) {
        self.pidx = PendingIndex::default();
        for p in &self.pending {
            self.pidx.add(&p.op);
        }
    }

    fn is_local(&self, n: NodeId) -> bool {
        self.local_only.contains(&n)
    }

    /// Replace node `x` by `y` everywhere (server adopted our node into an existing one).
    fn remap_node(&mut self, x: NodeId, y: NodeId) {
        if x == y {
            return;
        }
        if let Some(xn) = self.visible.nodes.get(&x).cloned() {
            let backs = self.visible.dirents_of(x).to_vec();
            if !self.visible.nodes.contains_key(&y) {
                let n = self.synced.nodes.get(&y).cloned().unwrap_or(xn);
                self.visible.insert_node(y, n);
            }
            if let Some(ch) = self.visible.children(x).cloned() {
                for (name, c) in ch {
                    self.visible.set_dirent(y, &name, c);
                }
            }
            for (p, name) in backs {
                self.visible.set_dirent(p, &name, y);
            }
            if self.visible.listed.contains(&x) {
                self.visible.set_listed(y, true);
            }
            self.visible.remove_node(x);
        }
        if let Some((ino, gen)) = self.ino_of.remove(&x) {
            self.db.send(DbOp::InoDel { root: self.name.clone(), node: x });
            self.node_of.insert(ino, y);
            if !self.ino_of.contains_key(&y) {
                self.ino_of.insert(y, (ino, gen));
                self.db.send(DbOp::Ino { root: self.name.clone(), node: y, ino, gen });
            }
        }
        if let Some(f) = self.files.remove(&x) {
            self.files.insert(y, f);
        }
        if self.leases.remove(&x) {
            self.leases.insert(y);
        }
        if self.local_only.remove(&x) {
            self.local_only.insert(y);
        }
        for p in self.pending.iter_mut() {
            if rewrite_node(&mut p.op, x, y) {
                self.db.send(DbOp::Outbound { root: self.name.clone(), n: p.n, row: Some((p.op.clone(), p.cred, p.ts)) });
            }
        }
        for v in self.release_after.values_mut() {
            if *v == x {
                *v = y;
            }
        }
        self.rebuild_pidx();
    }
}

/// Dirent keys of a remote effect that `rebase` would withhold.
fn skipped_keys(st: &RootState, eff: &Effect) -> Option<Vec<(NodeId, NameB)>> {
    let mut ks = vec![];
    match eff {
        Effect::Upsert { parent, name, .. } | Effect::Remove { parent, name, .. } => ks.push((*parent, name.0.clone())),
        Effect::Move { src_parent, src_name, dst_parent, dst_name, .. } => {
            ks.push((*src_parent, src_name.0.clone()));
            ks.push((*dst_parent, dst_name.0.clone()));
        }
        Effect::Attrs { .. } => return None,
    }
    if ks.iter().any(|(p, n)| st.pidx.touches_dirent(*p, n)) {
        Some(ks)
    } else {
        None
    }
}

fn rewrite_node(op: &mut Op, x: NodeId, y: NodeId) -> bool {
    let mut ch = false;
    let mut f = |n: &mut NodeId| {
        if *n == x {
            *n = y;
            ch = true;
        }
    };
    match op {
        Op::Create { parent, node, .. } => {
            f(parent);
            f(node)
        }
        Op::Link { parent, node, .. } | Op::Unlink { parent, node, .. } => {
            f(parent);
            f(node)
        }
        Op::Rename { src_parent, node, dst_parent, replaces, .. } => {
            f(src_parent);
            f(node);
            f(dst_parent);
            if let Some(r) = replaces {
                f(r)
            }
        }
        Op::SetAttr { node, .. } | Op::SetXattr { node, .. } | Op::SetContent { node, .. } => f(node),
    }
    ch
}

fn errno_of(e: &CallErr) -> Errno {
    match e {
        CallErr::Offline => Errno::EIO,
        CallErr::Server(er) => match er.code {
            ErrCode::NotFound => Errno::ENOENT,
            ErrCode::PermissionDenied => Errno::EACCES,
            ErrCode::ReadOnly => Errno::EROFS,
            _ => Errno::EIO,
        },
    }
}

pub struct RootCore {
    pub name: String,
    pub server_name: String,
    cfg: RootConfig,
    sh: Arc<Shared>,
    st: Mutex<RootState>,
    work_tx: Mutex<Sender<Work>>,
    work_rx: Mutex<Option<Receiver<Work>>>,
    pub wake: Notify,
    pub online: AtomicBool,
    writable: AtomicBool,
    applier: Mutex<Option<Arc<dyn KernelApplier>>>,
    node_locks: Mutex<HashMap<NodeId, Arc<Mutex<()>>>>,
    list_lock: Mutex<()>,
    resyncs: std::sync::atomic::AtomicU64,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    pipe: Mutex<Pipe>,
    ckpts: Mutex<HashMap<NodeId, CkptBase>>,
    pipe_cv: parking_lot::Condvar,
    reaper: Reaper,
    boot_files: Mutex<Vec<(NodeId, crate::store::FileRow)>>,
    boot_reaps: Mutex<Vec<u64>>,
}

impl RootCore {
    pub fn new(sh: Arc<Shared>, name: &str, cfg: RootConfig, lr: LoadedRoot) -> anyhow::Result<Arc<RootCore>> {
        let now = now_ns();
        let mut synced = Tree::new(now);
        for (n, v, c, a) in lr.nodes {
            synced.nodes.insert(n, Node { attrs: a, version: v, cversion: c });
        }
        for (p, nm, c) in lr.dirents {
            if synced.nodes.contains_key(&c) {
                synced.set_dirent(p, &nm, c);
            }
        }
        for n in lr.listed {
            synced.listed.insert(n);
        }
        let mut visible = synced.clone();
        let mut local_only = HashSet::new();
        let mut local_keys = HashSet::new();
        for (n, a) in lr.local_nodes {
            if a.kind == Kind::Dir {
                visible.listed.insert(n);
            }
            visible.nodes.insert(n, Node { attrs: a, version: Seq(0), cversion: Seq(0) });
            local_only.insert(n);
        }
        for (p, nm, c) in lr.local_dirents {
            if visible.nodes.contains_key(&c) && visible.nodes.contains_key(&p) {
                visible.set_dirent(p, &nm, c);
                local_keys.insert((p, nm));
            }
        }
        let mut pending: VecDeque<Pend> = VecDeque::new();
        let mut pidx = PendingIndex::default();
        for (n, mut op, cred, ts) in lr.outbound {
            if let Op::SetContent { node, base, .. } = &mut op {
                if *base == BASE_DEP && !pending.iter().any(|p| matches!(&p.op, Op::Create { node: m, .. } | Op::SetContent { node: m, .. } if m == node)) {
                    *base = synced.nodes.get(node).map(|x| x.cversion).unwrap_or(Seq(0));
                }
            }
            visible.apply_op(&op, ts);
            pidx.add(&op);
            pending.push_back(Pend { n, op, cred, ts, sent: false });
        }
        let mut ino_of = HashMap::new();
        let mut node_of = HashMap::new();
        let mut next_ino = 2;
        ino_of.insert(NodeId::ROOT, (ROOT_INO, 0));
        node_of.insert(ROOT_INO, NodeId::ROOT);
        for (n, i, g) in lr.inos {
            ino_of.insert(n, (i, g));
            node_of.insert(i, n);
            next_ino = next_ino.max(i + 1);
        }
        let mut st = RootState {
            name: name.to_string(),
            db: sh.db.clone(),
            visible,
            synced,
            pending,
            pidx,
            ino_of,
            node_of,
            next_ino,
            files: HashMap::new(),
            opens: HashMap::new(),
            next_token: 1,
            applied_seq: lr.applied_seq,
            epoch: lr.epoch,
            recent: VecDeque::new(),
            inflight: None,
            leases: HashSet::new(),
            local_only,
            local_keys,
            release_after: HashMap::new(),
            idle: VecDeque::new(),
            idle_ctr: 0,
            zombies: HashMap::new(),
            zombie_order: VecDeque::new(),
            defer_gc: false,
            skipped: Footprint::default(),
            own_seq: Seq(0),
        };
        // replay intents (the kernel is not mounted yet: direct application)
        let mut maxseq = st.applied_seq;
        for (_, c) in lr.intents {
            st.synced.apply_effect(&c.effect, c.seq);
            st.synced.gc(&|_| false);
            st.copy_versions(&c.effect);
            if let Some(e) = st.rebase(&c.effect) {
                st.visible.apply_effect(&e, c.seq);
                st.gc_visible();
                st.copy_versions(&e);
            }
            maxseq = maxseq.max(c.seq);
        }
        if maxseq > st.applied_seq {
            st.applied_seq = maxseq;
            st.db.send(DbOp::Meta(format!("r/{name}/applied_seq"), maxseq.0.to_le_bytes().to_vec()));
            st.db.send(DbOp::Intent { root: name.to_string(), seq: maxseq.0, change: None });
        }
        st.visible.dirty = Dirty::default();
        st.synced.dirty = Dirty::default();
        let boot_files = lr.files;
        let boot_reaps = lr.reaps;
        let (tx, rx) = std::sync::mpsc::channel();
        let server_name = cfg.server_name.clone().unwrap_or_else(|| name.to_string());
        Ok(Arc::new(RootCore {
            name: name.to_string(),
            server_name,
            cfg,
            sh,
            st: Mutex::new(st),
            work_tx: Mutex::new(tx),
            work_rx: Mutex::new(Some(rx)),
            wake: Notify::new(),
            online: AtomicBool::new(false),
            writable: AtomicBool::new(true),
            applier: Mutex::new(None),
            node_locks: Mutex::new(HashMap::new()),
            list_lock: Mutex::new(()),
            resyncs: std::sync::atomic::AtomicU64::new(0),
            threads: Mutex::new(vec![]),
            pipe: Mutex::new(Pipe::default()),
            ckpts: Mutex::new(HashMap::new()),
            pipe_cv: parking_lot::Condvar::new(),
            reaper: Reaper::default(),
            boot_files: Mutex::new(boot_files),
            boot_reaps: Mutex::new(boot_reaps),
        }))
    }

    pub fn start_threads(self: &Arc<Self>) {
        let rx = self.work_rx.lock().take().expect("started twice");
        let me = self.clone();
        let h = std::thread::Builder::new().name(format!("cairn-apply-{}", self.name)).spawn(move || me.applier_loop(rx)).expect("spawn applier");
        self.threads.lock().push(h);
        self.start_pipeline();
    }

    pub fn stop(&self) {
        self.stop_pipeline();
        let _ = self.work_tx.lock().send(Work::Stop);
        let hs: Vec<_> = self.threads.lock().drain(..).collect();
        for h in hs {
            let _ = h.join();
        }
    }

    // ------------------------------------------------------------ plumbing
    pub fn send_work(&self, w: Work) {
        let _ = self.work_tx.lock().send(w);
    }
    pub fn set_writable(&self, w: bool) {
        self.writable.store(w, Ordering::SeqCst);
    }
    pub fn on_disconnect(&self) {
        self.online.store(false, Ordering::SeqCst);
        let mut st = self.st.lock();
        st.leases.clear();
        for p in st.pending.iter_mut() {
            p.sent = false;
        }
    }
    pub fn epoch_and_applied(&self) -> (Option<Epoch>, Seq) {
        let st = self.st.lock();
        (st.epoch, st.applied_seq)
    }
    pub fn set_epoch_applied(&self, epoch: Epoch, seq: Seq) {
        let mut st = self.st.lock();
        st.epoch = Some(epoch);
        st.applied_seq = seq;
        let n = self.name.clone();
        st.db.send(DbOp::Meta(format!("r/{n}/epoch"), epoch.0.to_le_bytes().to_vec()));
        st.db.send(DbOp::Meta(format!("r/{n}/applied_seq"), seq.0.to_le_bytes().to_vec()));
    }
    pub fn set_root_attrs(&self, attrs: Attrs, version: Seq) {
        self.send_work(Work::Changes(vec![Change { seq: Seq(0), origin: None, host: HostId(0), cred: Cred { uid: 0, gid: 0 }, effect: Effect::Attrs { node: NodeId::ROOT, attrs } }]));
        let _ = version;
    }
    pub fn listed_dirs(&self) -> HashSet<NodeId> {
        self.st.lock().synced.listed.clone()
    }
    /// How many resyncs (epoch change / pruned log) this root has performed.
    pub fn resync_count(&self) -> u64 {
        self.resyncs.load(Ordering::SeqCst)
    }
    pub fn pending_len(&self) -> usize {
        self.st.lock().pending.len()
    }
    pub fn applied_seq(&self) -> Seq {
        self.st.lock().applied_seq
    }
    pub fn is_quiescent(&self) -> bool {
        if !self.pipeline_idle() {
            return false;
        }
        let st = self.st.lock();
        st.pending.is_empty() && (self.online.load(Ordering::SeqCst) || self.sh.cfg.offline || self.sh.cfg.server.is_empty())
    }
    pub fn barrier(&self) {
        let (tx, rx) = std::sync::mpsc::sync_channel(0);
        if self.work_tx.lock().send(Work::Barrier(tx)).is_ok() {
            let _ = rx.recv();
        }
    }
    /// Inspect a (visible) path, for tests: `a/b/c` -> attrs.
    pub fn stat_path(&self, path: &str) -> Option<Attrs> {
        let st = self.st.lock();
        let mut cur = NodeId::ROOT;
        for c in path.split('/').filter(|c| !c.is_empty()) {
            cur = st.visible.lookup(cur, c.as_bytes())?;
        }
        st.visible.node(cur).map(|n| n.attrs.clone())
    }

    fn node_lock(&self, n: NodeId) -> Arc<Mutex<()>> {
        self.node_locks.lock().entry(n).or_default().clone()
    }

    fn block<F: std::future::Future>(&self, f: F) -> F::Output {
        self.sh.rt.block_on(f)
    }

    fn check_writable(&self) -> FsResult<()> {
        if !self.writable.load(Ordering::SeqCst) {
            return Err(Errno::EROFS);
        }
        Ok(())
    }

    fn node_of_ino(&self, st: &RootState, ino: Ino) -> FsResult<NodeId> {
        st.node_of.get(&ino).copied().ok_or(Errno::ESTALE)
    }

    fn policy_of(&self, st: &RootState, parent: NodeId, name: Option<&[u8]>) -> PolicyMode {
        if self.cfg.policy.is_empty() {
            return PolicyMode::Sync;
        }
        let Some(mut p) = st.visible.path_of(parent) else { return PolicyMode::Sync };
        if let Some(n) = name {
            p.push(n.to_vec());
        }
        let s: Vec<String> = p.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
        self.cfg.policy_for(&s.join("/"))
    }

    fn policy_of_node(&self, st: &RootState, node: NodeId) -> PolicyMode {
        if self.cfg.policy.is_empty() {
            return PolicyMode::Sync;
        }
        let Some(p) = st.visible.path_of(node) else { return PolicyMode::Sync };
        let s: Vec<String> = p.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
        self.cfg.policy_for(&s.join("/"))
    }

    fn is_local_target(&self, st: &RootState, parent: NodeId, name: &[u8]) -> bool {
        st.is_local(parent) || self.policy_of(st, parent, Some(name)) == PolicyMode::LocalOnly
    }

    fn info(&self, st: &mut RootState, node: NodeId) -> FsResult<NodeInfo> {
        let (ino, gen) = st.ino(node);
        let (mut attrs, nlink) = match st.visible.node(node) {
            Some(n) => (n.attrs.clone(), st.visible.nlink(node)),
            None => (st.zombies.get(&node).ok_or(Errno::ENOENT)?.attrs.clone(), 0),
        };
        if let Some(f) = st.files.get(&node) {
            if f.live && (f.writers > 0 || f.dirty) {
                if let Ok(m) = fs::metadata(self.sh.cache.live_path(&self.name, f.ino)) {
                    attrs.size = m.len();
                    attrs.mtime_ns = m.mtime() * 1_000_000_000 + m.mtime_nsec();
                    attrs.ctime_ns = attrs.ctime_ns.max(m.ctime() * 1_000_000_000 + m.ctime_nsec());
                }
            }
        }
        Ok(NodeInfo { ino, node, generation: gen, nlink, attrs })
    }

    /// Journal a validated op: apply to the visible tree and queue it for the server.
    fn journal(&self, st: &mut RootState, op: Op, cred: Cred, local: bool) -> Option<u64> {
        let now = now_ns();
        let mut gone = None;
        match &op {
            Op::Unlink { node, .. } if st.visible.dirents_of(*node).len() <= 1 => {
                st.stash_zombie(*node);
                gone = Some(*node);
            }
            Op::Rename { replaces: Some(r), mode, .. } if *mode != RenameMode::Exchange && st.visible.dirents_of(*r).len() <= 1 => {
                st.stash_zombie(*r);
                gone = Some(*r);
            }
            _ => {}
        }
        if let Some(n) = gone {
            // an idle live file of a node that just lost its last name is garbage: drop it now
            // (otherwise it lingers in the idle LRU, 64k of them)
            // (not with a commit pending: the server may still reference those bytes - the delete
            // can lose a conflict, or the create was already sent. The commit moves them into the
            // CAS and uploads them; only folding the create away cancels it, see fold_away.)
            if st.files.get(&n).is_some_and(|f| f.opens == 0 && !f.dirty && f.writers == 0 && !f.queued && f.job.is_none()) {
                if let Some(f) = st.files.remove(&n) {
                    if f.live {
                        self.reap_ino(f.ino);
                    }
                    if f.row_dirty {
                        st.db.send(DbOp::File { root: self.name.clone(), node: n, row: None });
                    }
                }
            }
        }
        st.visible.apply_op(&op, now);
        st.gc_visible();
        let mut op = op;
        let n = if local || self.fold_op(st, &mut op, gone) {
            None
        } else {
            let n = self.sh.next_op();
            st.push_pending(n, op, cred, now);
            Some(n)
        };
        st.persist();
        if n.is_some() {
            self.wake.notify_one();
        }
        n
    }

    // ------------------------------------------------------------- listing
    fn ensure_listed(&self, dir: NodeId) -> FsResult<()> {
        {
            let st = self.st.lock();
            if st.visible.listed.contains(&dir) {
                return Ok(());
            }
            if !st.visible.is_dir(dir) {
                return Err(if st.visible.nodes.contains_key(&dir) { Errno::ENOTDIR } else { Errno::ENOENT });
            }
        }
        let _g = self.list_lock.lock();
        {
            if self.st.lock().visible.listed.contains(&dir) {
                return Ok(());
            }
        }
        if self.sh.cfg.offline || self.sh.cfg.server.is_empty() {
            // standalone (no server at all): every directory is complete by definition
            let mut st = self.st.lock();
            st.visible.set_listed(dir, true);
            return Ok(());
        }
        if !self.sh.link.is_connected() {
            return Err(Errno::EIO);
        }
        let link = self.sh.link.clone();
        let sname = self.server_name.clone();
        let res = self.block(async move {
            let h = link.handle(&sname).ok_or(CallErr::Offline)?;
            fetch_listing(&link, h, dir).await
        });
        let (ver, entries) = res.map_err(|e| errno_of(&e))?;
        let mut st = self.st.lock();
        self.apply_listing(&mut st, dir, ver, entries);
        Ok(())
    }

    fn apply_listing(&self, st: &mut RootState, dir: NodeId, ver: Seq, entries: Vec<DirEntry>) {
        let names: HashSet<&[u8]> = entries.iter().map(|e| e.name.0.as_slice()).collect();
        // synced
        if st.synced.is_dir(dir) {
            for e in &entries {
                st.synced.set_dirent(dir, &e.name.0, e.node);
                match st.synced.nodes.get_mut(&e.node) {
                    Some(n) => {
                        if n.attrs.content != e.attrs.content {
                            n.cversion = ver;
                        }
                        if !attrs_eq(&n.attrs, &e.attrs) {
                            n.version = ver;
                        }
                        n.attrs = e.attrs.clone();
                        st.synced.dirty.nodes.insert(e.node);
                    }
                    None => st.synced.insert_node(e.node, Node { attrs: e.attrs.clone(), version: ver, cversion: ver }),
                }
            }
            let stale: Vec<NameB> = st.synced.children(dir).map(|c| c.keys().filter(|k| !names.contains(k.as_slice())).cloned().collect()).unwrap_or_default();
            for s in stale {
                st.synced.remove_dirent(dir, &s);
            }
            st.synced.set_listed(dir, true);
            let replay: Vec<Change> = st.recent.iter().filter(|c| c.seq > ver && mentions(&c.effect, dir)).cloned().collect();
            for c in replay {
                st.synced.apply_effect(&c.effect, c.seq);
            }
            st.synced.gc(&|_| false);
        }
        // visible
        let now_seq = ver;
        st.visible.set_listed(dir, true);
        for e in &entries {
            let eff = Effect::Upsert { parent: dir, name: e.name.clone(), node: e.node, attrs: e.attrs.clone() };
            let known = st.visible.nodes.contains_key(&e.node);
            if let Some(eff) = st.rebase(&eff) {
                st.visible.apply_effect(&eff, now_seq);
            }
            if !known && e.attrs.kind == Kind::Dir {
                st.visible.set_listed(e.node, false);
            }
            st.copy_versions(&Effect::Attrs { node: e.node, attrs: e.attrs.clone() });
        }
        let stale: Vec<(NameB, NodeId)> = st
            .visible
            .children(dir)
            .map(|c| c.iter().filter(|(k, n)| !names.contains(k.as_slice()) && !st.pidx.touches_dirent(dir, k) && !st.local_only.contains(n)).map(|(k, n)| (k.clone(), *n)).collect())
            .unwrap_or_default();
        for (k, n) in stale {
            st.visible.remove_dirent(dir, &k);
            let _ = n;
        }
        st.visible.set_listed(dir, true);
        let replay: Vec<Change> = st.recent.iter().filter(|c| c.seq > ver && mentions(&c.effect, dir)).cloned().collect();
        for c in replay {
            if let Some(e) = st.rebase(&c.effect) {
                st.visible.apply_effect(&e, c.seq);
            }
        }
        st.gc_visible();
        st.persist();
    }

    // ----------------------------------------------------- applier thread
    fn wait_applier(&self) -> Option<Arc<dyn KernelApplier>> {
        loop {
            if let Some(a) = self.applier.lock().clone() {
                return Some(a);
            }
            if self.sh.shutdown.load(Ordering::SeqCst) {
                return None;
            }
            self.sh.wait_event(Duration::from_millis(50));
        }
    }

    fn applier_loop(self: Arc<Self>, rx: Receiver<Work>) {
        let mut deferred: Vec<(Seq, Footprint)> = vec![];
        while let Ok(w) = rx.recv() {
            match w {
                Work::Stop => break,
                Work::Barrier(tx) => {
                    let _ = tx.send(());
                }
                Work::Changes(cs) => {
                    if self.wait_applier().is_none() {
                        break;
                    }
                    for i in 0..cs.len() {
                        let defer = cs.get(i + 1).map(|n| n.origin.is_some() && n.origin == cs[i].origin).unwrap_or(false);
                        self.process_change(cs[i].clone(), defer);
                    }
                    self.run_deferred(&mut deferred);
                }
                Work::Resync { listings, applied, epoch, done } => {
                    if self.wait_applier().is_none() {
                        break;
                    }
                    self.process_resync(listings, applied, epoch);
                    let _ = done.send(());
                }
                Work::Reconcile { fp, seq } => {
                    if self.wait_applier().is_none() {
                        break;
                    }
                    if seq.0 == 0 || self.st.lock().applied_seq >= seq {
                        self.do_reconcile(&fp);
                    } else {
                        deferred.push((seq, fp));
                    }
                }
            }
        }
    }

    fn run_deferred(&self, deferred: &mut Vec<(Seq, Footprint)>) {
        if deferred.is_empty() {
            return;
        }
        let applied = self.st.lock().applied_seq;
        let (ready, rest): (Vec<_>, Vec<_>) = deferred.drain(..).partition(|(s, _)| *s <= applied);
        *deferred = rest;
        for (_, fp) in ready {
            self.do_reconcile(&fp);
        }
    }

    fn process_change(&self, c: Change, defer_gc: bool) {
        let mut st = self.st.lock();
        st.defer_gc = defer_gc;
        if c.seq.0 != 0 && c.seq <= st.applied_seq {
            st.defer_gc = false;
            return;
        }
        let name = self.name.clone();
        if c.seq.0 != 0 {
            st.db.send(DbOp::Intent { root: name.clone(), seq: c.seq.0, change: Some(c.clone()) });
        }
        st.synced.apply_effect(&c.effect, c.seq);
        if !st.defer_gc && !keeps_orphan(&c.effect) {
            st.synced.gc(&|_| false);
        }
        if c.seq.0 != 0 {
            st.recent.push_back(c.clone());
            if st.recent.len() > RECENT_MAX {
                st.recent.pop_front();
            }
        }
        st.copy_versions(&c.effect);
        st.persist();
        if let Some(k) = skipped_keys(&st, &c.effect) {
            if st.skipped.keys.len() < 10_000 {
                st.skipped.keys.extend(k);
            }
        }
        let decision = st.rebase(&c.effect).and_then(|e| {
            if st.visible.would_change(&e) {
                Some(e)
            } else {
                st.apply_visible(&Change { effect: e.clone(), ..c.clone() });
                None
            }
        });
        drop(st);
        if let Some(e) = decision {
            self.deliver(Change { effect: e, ..c.clone() });
        }
        self.st.lock().defer_gc = false;
        if c.seq.0 != 0 {
            let mut st = self.st.lock();
            st.applied_seq = st.applied_seq.max(c.seq);
            let s = st.applied_seq;
            st.db.send(DbOp::Meta(format!("r/{name}/applied_seq"), s.0.to_le_bytes().to_vec()));
            st.db.send(DbOp::Intent { root: name, seq: c.seq.0, change: None });
            drop(st);
            self.sh.bump_event();
        }
    }

    /// Replay a change through the kernel (no locks held) and into the visible tree.
    fn deliver(&self, c: Change) {
        {
            self.st.lock().inflight = Some(c.clone());
        }
        let applier = self.applier.lock().clone();
        if let Some(a) = applier {
            if let Err(e) = a.apply(&c) {
                tracing::debug!("applier failed ({e:?}) for {:?}; applying directly", c.effect);
            }
        }
        let mut st = self.st.lock();
        st.apply_visible(&c);
        st.inflight = None;
    }

    fn do_reconcile(&self, fp: &Footprint) {
        let effects = {
            let st = self.st.lock();
            let ops: Vec<&Op> = st.pending.iter().map(|p| &p.op).collect();
            reconcile(&st.visible, &st.synced, &st.pidx, &ops, fp, now_ns())
        };
        let host = self.sh.host_id();
        for e in effects {
            self.deliver(Change { seq: Seq(0), origin: None, host, cred: Cred { uid: 0, gid: 0 }, effect: e });
        }
    }

    fn process_resync(&self, listings: Vec<(NodeId, Vec<DirEntry>)>, applied: Seq, epoch: Option<Epoch>) {
        self.resyncs.fetch_add(1, Ordering::SeqCst);
        let mut effects: Vec<Effect> = vec![];
        {
            let st = self.st.lock();
            let seen: HashSet<NodeId> = listings.iter().flat_map(|(_, es)| es.iter().map(|e| e.node)).collect();
            let listed_now: HashSet<NodeId> = listings.iter().map(|(d, _)| *d).collect();
            for (dir, entries) in &listings {
                let names: HashSet<&[u8]> = entries.iter().map(|e| e.name.0.as_slice()).collect();
                if let Some(ch) = st.synced.children(*dir) {
                    for (name, node) in ch {
                        if !names.contains(name.as_slice()) {
                            effects.push(Effect::Remove { parent: *dir, name: Name(name.clone()), node: *node, node_deleted: !seen.contains(node) });
                        }
                    }
                }
                for e in entries {
                    match st.synced.lookup(*dir, &e.name.0) {
                        Some(n) if n == e.node => {
                            if st.synced.node(n).map(|x| !attrs_eq(&x.attrs, &e.attrs)).unwrap_or(false) {
                                effects.push(Effect::Attrs { node: n, attrs: e.attrs.clone() });
                            }
                        }
                        _ => effects.push(Effect::Upsert { parent: *dir, name: e.name.clone(), node: e.node, attrs: e.attrs.clone() }),
                    }
                }
            }
            // directories that vanished from the server: forget their cached listing state
            let _ = listed_now;
        }
        let host = self.sh.host_id();
        for e in effects {
            self.process_change(Change { seq: Seq(0), origin: None, host, cred: Cred { uid: 0, gid: 0 }, effect: e }, false);
        }
        let mut st = self.st.lock();
        st.applied_seq = applied;
        if let Some(e) = epoch {
            st.epoch = Some(e);
            st.db.send(DbOp::Meta(format!("r/{}/epoch", self.name), e.0.to_le_bytes().to_vec()));
        }
        st.db.send(DbOp::Meta(format!("r/{}/applied_seq", self.name), applied.0.to_le_bytes().to_vec()));
        st.recent.clear();
        drop(st);
        self.sh.bump_event();
    }

    // ------------------------------------------------------------- sender
    pub fn take_batch(&self) -> Option<(Vec<Commit>, Vec<u64>)> {
        if !self.online.load(Ordering::SeqCst) || !self.writable.load(Ordering::SeqCst) || !self.sh.host_known() {
            return None;
        }
        let host = self.sh.host_id();
        let mut st = self.st.lock();
        let mut commits = vec![];
        let mut ns = vec![];
        let synced_cv: Vec<(usize, Seq)> = vec![];
        let _ = synced_cv;
        let mut bytes = 0usize;
        let idxs: Vec<usize> = st
            .pending
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.sent)
            .take_while(|(_, p)| {
                bytes += op_size(&p.op);
                bytes <= MAX_BATCH_BYTES
            })
            .map(|(i, _)| i)
            .take(MAX_BATCH)
            .collect();
        for i in idxs {
            let dep = matches!(&st.pending[i].op, Op::SetContent { base, .. } if *base == BASE_DEP);
            if dep && !commits.is_empty() {
                break;
            }
            let mut op = st.pending[i].op.clone();
            if let Op::SetContent { node, base, .. } = &mut op {
                if *base == BASE_DEP {
                    *base = st.synced.nodes.get(node).map(|x| x.cversion).unwrap_or(Seq(0));
                }
            }
            let p = &mut st.pending[i];
            p.sent = true;
            commits.push(Commit { op_id: OpId { host, n: p.n }, cred: p.cred, op });
            ns.push(p.n);
        }
        if commits.is_empty() {
            None
        } else {
            Some((commits, ns))
        }
    }

    pub fn unsend(&self, ns: &[u64]) {
        let mut st = self.st.lock();
        for p in st.pending.iter_mut() {
            if ns.contains(&p.n) {
                p.sent = false;
            }
        }
    }

    /// Process commit results. Returns nodes whose lease should be released.
    pub fn handle_results(&self, ns: &[u64], results: Vec<CommitResult>) -> Vec<NodeId> {
        let mut release = vec![];
        let mut work = vec![];
        {
            let mut st = self.st.lock();
            for (n, res) in ns.iter().zip(results) {
                let Some(pos) = st.pending.iter().position(|p| p.n == *n) else { continue };
                let p = st.pending.remove(pos).unwrap();
                st.pidx.remove(&p.op);
                st.db.send(DbOp::Outbound { root: self.name.clone(), n: p.n, row: None });
                let mut fp = footprint(&p.op);
                let mut applied_seq_of_op: Option<Seq> = None;
                if let CommitResult::Applied { seq } | CommitResult::Transformed { seq, .. } = &res {
                    st.own_seq = st.own_seq.max(*seq);
                }
                let mut good = false;
                match &res {
                    CommitResult::Applied { seq } => {
                        applied_seq_of_op = Some(*seq);
                        good = true;
                    }
                    CommitResult::Transformed { seq, conflict } => {
                        applied_seq_of_op = Some(*seq);
                        if let Resolution::Adopted { node } = &conflict.resolution {
                            if let Op::Create { node: x, .. } = &p.op {
                                st.remap_node(*x, *node);
                                self.remap_pipeline(&mut st, *x, *node);
                                for fnode in fp.nodes.iter_mut() {
                                    if fnode == x {
                                        *fnode = *node;
                                    }
                                }
                                applied_seq_of_op = None;
                            }
                        }
                        tracing::info!("commit {} transformed: {:?} ({})", p.n, conflict.resolution, conflict.detail);
                        work.push((fp.clone(), *seq));
                    }
                    CommitResult::Rejected { conflict } => {
                        tracing::info!("commit {} rejected: {:?} ({})", p.n, conflict.resolution, conflict.detail);
                        work.push((fp.clone(), Seq(0)));
                    }
                    CommitResult::Failed { error } => {
                        tracing::warn!("commit {} failed: {:?} {}", p.n, error.code, error.msg);
                        work.push((fp.clone(), Seq(0)));
                    }
                }
                // content version bookkeeping for dependent SetContent ops
                if let Op::Create { node, .. } | Op::SetContent { node, .. } = &p.op {
                    let nd = fp.nodes.iter().copied().next().unwrap_or(*node);
                    let nd = if matches!(&res, CommitResult::Transformed { conflict, .. } if matches!(conflict.resolution, Resolution::Adopted { .. })) { nd } else { *node };
                    let base = applied_seq_of_op.or_else(|| st.synced.nodes.get(&nd).map(|x| x.cversion));
                    if let Some(b) = base {
                        let mut patched = None;
                        for q in st.pending.iter_mut() {
                            if let Op::SetContent { node: m, base: qb, .. } = &mut q.op {
                                if *m == nd && *qb == BASE_DEP {
                                    *qb = b;
                                    patched = Some((q.n, q.op.clone(), q.cred, q.ts));
                                    break;
                                }
                            }
                        }
                        if let Some((qn, op, cred, ts)) = patched {
                            st.db.send(DbOp::Outbound { root: self.name.clone(), n: qn, row: Some((op, cred, ts)) });
                        }
                    }
                }
                if let Some(node) = st.release_after.remove(&p.n) {
                    if good && st.leases.contains(&node) && st.files.get(&node).map(|f| f.writers == 0).unwrap_or(true) {
                        release.push(node);
                    }
                }
            }
            st.persist();
            // remote changes held back by now-acked ops: bring the visible tree in line with the server
            if !st.skipped.keys.is_empty() {
                let mut keys = std::mem::take(&mut st.skipped.keys);
                keys.sort();
                keys.dedup();
                let (still, ready): (Vec<_>, Vec<_>) = keys.into_iter().partition(|(p, n)| st.pidx.touches_dirent(*p, n));
                st.skipped.keys = still;
                if !ready.is_empty() {
                    // not before the acked ops' own changes are in the synced tree: reconciling
                    // earlier would see the server without them and undo them locally
                    work.push((Footprint { keys: ready, nodes: vec![] }, st.own_seq));
                }
            }
        }
        for (fp, seq) in work {
            self.send_work(Work::Reconcile { fp, seq });
        }
        release
    }

    pub fn lease_released(&self, node: NodeId) {
        self.st.lock().leases.remove(&node);
    }

    // -------------------------------------------------------------- leases
    fn acquire_lease(&self, node: NodeId) {
        if !self.sh.link.is_connected() || !self.online.load(Ordering::SeqCst) {
            return;
        }
        {
            let st = self.st.lock();
            if st.leases.contains(&node) {
                return;
            }
            // the server does not know a node whose Create is still queued: nothing to lease
            if st.pidx.has_create(node) {
                return;
            }
        }
        let link = self.sh.link.clone();
        let sname = self.server_name.clone();
        let r = self.block(async move { link.root_call(&sname, |h| Request::AcquireWrite { root: h, node }).await });
        match r {
            Ok(Response::Granted { version, .. }) => {
                self.st.lock().leases.insert(node);
                // wait until we have seen everything up to the version the server granted at
                let end = Instant::now() + Duration::from_secs(5);
                while {
                    let st = self.st.lock();
                    st.applied_seq < version && st.own_seq < version
                } && Instant::now() < end
                {
                    self.sh.wait_event(Duration::from_millis(20));
                }
            }
            Ok(_) => {}
            Err(e) => tracing::debug!("AcquireWrite {node:?}: {e:?}"),
        }
    }

    /// Server asked for the lease back: publish and release.
    pub fn recall(&self, node: NodeId) {
        let lock = self.node_lock(node);
        let _g = lock.lock();
        let (dirty, cred) = {
            let st = self.st.lock();
            let f = st.files.get(&node);
            (f.map(|f| f.writers > 0 || f.dirty).unwrap_or(false), f.and_then(|f| f.last_cred).unwrap_or((0, 0)))
        };
        if dirty {
            if let Ok(Some(n)) = self.publish_sync(node, Cred { uid: cred.0, gid: cred.1 }, false) {
                self.wait_until(Duration::from_secs(25), |st| st.pending.front().map(|p| p.n > n).unwrap_or(true));
            }
        }
        let link = self.sh.link.clone();
        let sname = self.server_name.clone();
        let _ = self.block(async move { link.root_call(&sname, |h| Request::ReleaseWrite { root: h, node }).await });
        self.st.lock().leases.remove(&node);
    }

    fn wait_until(&self, timeout: Duration, cond: impl Fn(&RootState) -> bool) -> bool {
        let end = Instant::now() + timeout;
        loop {
            if cond(&self.st.lock()) {
                return true;
            }
            if Instant::now() >= end || !self.sh.link.is_connected() {
                return false;
            }
            self.sh.wait_event(Duration::from_millis(50));
        }
    }

    // ------------------------------------------------------------- content
    fn fetch_content(&self, content: ContentId) -> FsResult<()> {
        let Some(h) = object_hash(&content) else { return Ok(()) };
        if self.sh.cache.has(&h) {
            self.sh.cache.touch(&h);
            return Ok(());
        }
        let lock = self.sh.fetch_locks.lock().entry(h).or_default().clone();
        let _g = lock.lock();
        if self.sh.cache.has(&h) {
            return Ok(());
        }
        if !self.sh.link.is_connected() {
            return Err(Errno::EIO);
        }
        let sh = self.sh.clone();
        let r = self.block(async move { fetch_async(&sh, content).await });
        self.sh.fetch_locks.lock().remove(&h);
        match r {
            Ok(()) => {
                self.sh.maybe_evict();
                Ok(())
            }
            Err(e) => {
                tracing::warn!("fetch {content:?} failed: {e}");
                Err(Errno::EIO)
            }
        }
    }

    /// Make sure the node's live file exists and matches its visible content.
    /// Caller holds the node lock.
    fn ensure_live(&self, node: NodeId, write: bool) -> FsResult<(PathBuf, Option<fs::File>)> {
        let gone = {
            // unlinked but still reachable (open("/proc/self/fd/N"), open_by_handle): reuse its live
            // file, or rebuild one from the content the node had when it was removed
            let st = self.st.lock();
            if st.visible.node(node).is_none() {
                match st.files.get(&node) {
                    Some(f) if f.live && st.zombies.contains_key(&node) => return Ok((self.sh.cache.live_path(&self.name, f.ino), None)),
                    _ if !st.zombies.contains_key(&node) => return Err(Errno::ENOENT),
                    _ => {}
                }
                true
            } else {
                false
            }
        };
        if write && !gone {
            // may block on a recall; afterwards our view includes the previous holder's content
            self.acquire_lease(node);
        }
        let (content, mtime, ino, live, stale, opens, local, pinned) = {
            let mut st = self.st.lock();
            let n = match st.visible.node(node) {
                Some(n) => n,
                None => st.zombies.get(&node).ok_or(Errno::ENOENT)?,
            };
            match n.attrs.kind {
                Kind::File => {}
                Kind::Dir => return Err(Errno::EISDIR),
                _ => return Err(Errno::EINVAL),
            }
            let content = n.attrs.content.unwrap_or(ContentId::Empty);
            let mtime = n.attrs.mtime_ns;
            let (ino, _) = st.ino(node);
            let f = st.files.entry(node).or_default();
            if f.ino == 0 {
                f.ino = ino;
            }
            let local = st.is_local(node);
            let pinned = self.policy_of_node(&st, node) == PolicyMode::Pinned;
            let f = &st.files[&node];
            (content, mtime, f.ino, f.live, f.stale, f.opens, local, pinned)
        };
        let _ = local;
        let path = self.sh.cache.live_path(&self.name, ino);
        if live && !stale {
            return Ok((path, None));
        }
        if let Some(h) = object_hash(&content) {
            if pinned {
                self.sh.pin(h);
            }
        }
        let mut fresh = None;
        if live && stale {
            // the live file is about to be replaced: keep a published version's bytes first
            self.settle_commit(node);
        }
        if live && opens > 0 {
            self.fetch_content(content)?;
            // refresh in place: open handles share the inode
            let mut d = fs::OpenOptions::new().write(true).open(&path).map_err(|_| Errno::EIO)?;
            match object_hash(&content) {
                Some(h) => {
                    let mut s = self.sh.cache.open_cas(&h).map_err(|_| Errno::EIO)?;
                    self.sh.cache.clone_into(&mut s, &mut d).map_err(|_| Errno::EIO)?;
                }
                None => d.set_len(0).map_err(|_| Errno::EIO)?,
            }
        } else {
            if live {
                let _ = fs::remove_file(&path);
            }
            self.settle_reap(ino);
            let f = match object_hash(&content) {
                Some(h) => {
                    let cas = self.sh.cache.cas_path(&h);
                    // the common case is a resident object: try the clone first, fetch only if missing
                    let r = match self.sh.cache.clone_file_fd(&cas, &path) {
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !cas.exists() => {
                            self.fetch_content(content)?;
                            self.sh.cache.clone_file_fd(&cas, &path)
                        }
                        r => r,
                    };
                    r.map_err(|e| {
                        tracing::warn!("clone to live failed: {e}");
                        Errno::EIO
                    })?
                }
                None => {
                    let mut oo = fs::OpenOptions::new();
                    oo.read(true).write(true).create(true).truncate(true).mode(0o600);
                    match oo.open(&path) {
                        Ok(f) => f,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            fs::create_dir_all(path.parent().unwrap()).map_err(|_| Errno::EIO)?;
                            oo.open(&path).map_err(|_| Errno::EIO)?
                        }
                        Err(_) => return Err(Errno::EIO),
                    }
                }
            };
            fresh = Some(f);
        }
        match &fresh {
            Some(f) => set_mtime_fd(f, mtime),
            None => set_mtime(&path, mtime),
        }
        let mut st = self.st.lock();
        let f = st.files.entry(node).or_default();
        f.live = true;
        f.stale = false;
        Ok((path, fresh))
    }

    fn cleanup_file(&self, st: &mut RootState, node: NodeId) {
        if let Some(f) = st.files.get(&node) {
            let gone = st.visible.node(node).is_none();
            if f.opens == 0 && f.writers == 0 && (gone || (!f.dirty && !f.queued)) {
                if gone || f.stale {
                    // an idle file with an unfinished commit is renamed into the CAS, not deleted
                    self.retire_uncommitted(st, node);
                    if let Some(f) = st.files.remove(&node) {
                        self.reap_ino(f.ino);
                        if f.row_dirty {
                            st.db.send(DbOp::File { root: self.name.clone(), node, row: None });
                        }
                    }
                    st.zombies.remove(&node);
                } else {
                    st.idle_ctr += 1;
                    let c = st.idle_ctr;
                    st.files.get_mut(&node).unwrap().idle_seq = c;
                    st.idle.push_back((node, c));
                    while st.idle.len() > self.sh.cfg.idle_live_files {
                        let (n, s) = st.idle.pop_front().unwrap();
                        let ok = st.files.get(&n).map(|f| f.idle_seq == s && f.opens == 0 && !f.dirty && !f.queued).unwrap_or(false);
                        if ok {
                            self.retire_uncommitted(st, n);
                            let f = st.files.remove(&n).unwrap();
                            self.reap_ino(f.ino);
                            if f.row_dirty {
                                st.db.send(DbOp::File { root: self.name.clone(), node: n, row: None });
                            }
                        }
                    }
                }
            }
        }
    }

    fn truncate(&self, node: NodeId, size: u64, cred: Cred) -> FsResult<()> {
        if size > MAX_FILE_SIZE {
            return Err(Errno(libc::EFBIG));
        }
        let lock = self.node_lock(node);
        let _g = lock.lock();
        {
            let mut st = self.st.lock();
            let n = st.visible.node(node).ok_or(Errno::ENOENT)?;
            match n.attrs.kind {
                Kind::File => {}
                Kind::Dir => return Err(Errno::EISDIR),
                _ => return Err(Errno::EINVAL),
            }
            let live = st.files.get(&node).map(|f| f.live).unwrap_or(false);
            if !live && n.attrs.size == size {
                return Ok(());
            }
            let f = st.files.entry(node).or_default();
            f.last_cred = Some((cred.uid, cred.gid));
        }
        let (path, _) = self.ensure_live(node, true)?;
        self.settle_commit(node);
        let f = fs::OpenOptions::new().write(true).open(&path).map_err(|_| Errno::EIO)?;
        f.set_len(size).map_err(|_| Errno::EIO)?;
        drop(f);
        let mut st = self.st.lock();
        let f = st.files.get_mut(&node).unwrap();
        f.dirty = true;
        f.open_stat = None;
        f.wgen += 1;
        let idle = f.writers == 0;
        let hot = !idle;
        if idle {
            // a save event (truncate without an open fd): the pipeline publishes it
            self.enqueue_save(&mut st, node, hot);
        }
        Ok(())
    }

    /// Re-enter the pipeline for files left behind by a crash (journaled state first, the scan of
    /// `live/` is the backstop for anything the journal does not know), and finish deferred deletes.
    pub fn recover_live_files(&self) {
        let t0 = Instant::now();
        let rows = std::mem::take(&mut *self.boot_files.lock());
        let nrows = rows.len();
        self.recover_pipeline(rows);
        tracing::info!("startup: pipeline rows {nrows} in {:?}", t0.elapsed());
        for ino in std::mem::take(&mut *self.boot_reaps.lock()) {
            if self.reaper.pending.lock().insert(ino) {
                self.reaper.q.lock().push_back(ino);
            }
        }
        self.reaper.cv.notify_all();
                let dir = self.sh.cache.live_dir(&self.name);
        let Ok(rd) = fs::read_dir(&dir) else { return };
        for e in rd.flatten() {
            let Ok(ino) = e.file_name().to_string_lossy().parse::<u64>() else { continue };
            let path = e.path();
            if self.reaper.pending.lock().contains(&ino) {
                continue;
            }
            let node = self.st.lock().node_of.get(&ino).copied();
            let Some(node) = node else {
                let _ = fs::remove_file(&path);
                continue;
            };
            if self.st.lock().files.get(&node).map(|f| f.live).unwrap_or(false) {
                continue; // recovered from the journal
            }
            let (attrs, ok) = {
                let st = self.st.lock();
                match st.visible.node(node) {
                    Some(n) if n.attrs.kind == Kind::File => (Some(n.attrs.clone()), true),
                    _ => (None, false),
                }
            };
            if !ok {
                let _ = fs::remove_file(&path);
                continue;
            }
            let attrs = attrs.unwrap();
            let Ok(m) = fs::metadata(&path) else { continue };
            let mt = m.mtime() * 1_000_000_000 + m.mtime_nsec();
            if m.len() == attrs.size && mt == attrs.mtime_ns {
                let _ = fs::remove_file(&path);
                continue;
            }
            // stat differs from the node: let a pipeline worker hash it (cold reads are disk-latency
            // bound, 1000s of them must not delay the mount); an unchanged file ends up clean and idle
            tracing::debug!("recovering live file for {node:?}");
            let mut st = self.st.lock();
            let f = st.files.entry(node).or_default();
            f.ino = ino;
            f.live = true;
            f.last_cred = Some((attrs.uid, attrs.gid));
            self.enqueue_save(&mut st, node, false);
        }
    }

    fn remote_apply(&self, st: &mut RootState) {
        if let Some(c) = st.inflight.clone() {
            st.apply_visible(&c);
        }
    }
}

/// A dirent removal that leaves the node alive (RENAME_EXCHANGE emits
/// Remove, Move, Upsert): the node may be re-linked by the next change, so it
/// (and its subtree) must not be collected yet.
fn keeps_orphan(e: &Effect) -> bool {
    matches!(e, Effect::Remove { node_deleted: false, .. })
}

fn mentions(e: &Effect, dir: NodeId) -> bool {
    match e {
        Effect::Upsert { parent, .. } | Effect::Remove { parent, .. } => *parent == dir,
        Effect::Move { src_parent, dst_parent, .. } => *src_parent == dir || *dst_parent == dir,
        Effect::Attrs { .. } => false,
    }
}

fn set_mtime_fd(f: &fs::File, mtime_ns: i64) {
    use std::os::unix::io::AsRawFd;
    let ts = [libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT }, libc::timespec { tv_sec: mtime_ns.div_euclid(1_000_000_000), tv_nsec: mtime_ns.rem_euclid(1_000_000_000) }];
    unsafe {
        libc::futimens(f.as_raw_fd(), ts.as_ptr());
    }
}

fn set_mtime(path: &std::path::Path, mtime_ns: i64) {
    if let Ok(c) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) {
        let ts = [libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT }, libc::timespec { tv_sec: mtime_ns.div_euclid(1_000_000_000), tv_nsec: mtime_ns.rem_euclid(1_000_000_000) }];
        unsafe {
            libc::utimensat(libc::AT_FDCWD, c.as_ptr(), ts.as_ptr(), 0);
        }
    }
}

async fn fetch_async(sh: &Arc<Shared>, content: ContentId) -> Result<(), String> {
    let tmp = sh.cache.tmp_path("dl");
    let res = fetch_to(sh, content, &tmp).await;
    if let Err(e) = res {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    let h = object_hash(&content).unwrap();
    let dst = sh.cache.cas_path(&h);
    if dst.exists() {
        let _ = fs::remove_file(&tmp);
        return Ok(());
    }
    fs::create_dir_all(dst.parent().unwrap()).map_err(|e| e.to_string())?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o444)).map_err(|e| e.to_string())?;
    fs::rename(&tmp, &dst).map_err(|e| e.to_string())
}

async fn fetch_to(sh: &Arc<Shared>, content: ContentId, tmp: &std::path::Path) -> Result<(), String> {
    let ce = |e: CallErr| format!("{e:?}");
    match content {
        ContentId::Empty => File::create(tmp).map(|_| ()).map_err(|e| e.to_string()),
        ContentId::Blob(h) => match sh.link.call(Request::GetBlob { hash: h }).await.map_err(ce)? {
            Response::Blob { data } => {
                if Hash::of(&data) != h {
                    return Err("blob hash mismatch".into());
                }
                let mut f = File::create(tmp).map_err(|e| e.to_string())?;
                f.write_all(&data).map_err(|e| e.to_string())
            }
            r => Err(format!("unexpected {r:?}")),
        },
        ContentId::Chunked(h) => {
            let m = match sh.link.call(Request::GetManifest { id: h }).await.map_err(ce)? {
                Response::Manifest { manifest } => manifest,
                r => return Err(format!("unexpected {r:?}")),
            };
            if m.id() != content {
                return Err("manifest hash mismatch".into());
            }
            let f = Arc::new(File::create(tmp).map_err(|e| e.to_string())?);
            f.set_len(m.size).map_err(|e| e.to_string())?;
            // group the chunks into frames of a few MiB: one GetBlobs per group
            let mut groups: Vec<Vec<(Hash, u32, u64)>> = vec![];
            let mut off = 0u64;
            let mut cur: Vec<(Hash, u32, u64)> = vec![];
            let mut cur_bytes = 0usize;
            for (h, l) in &m.chunks {
                if !cur.is_empty() && cur_bytes + *l as usize > (4 << 20) {
                    groups.push(std::mem::take(&mut cur));
                    cur_bytes = 0;
                }
                cur.push((*h, *l, off));
                cur_bytes += *l as usize;
                off += *l as u64;
            }
            if !cur.is_empty() {
                groups.push(cur);
            }
            let mut set = tokio::task::JoinSet::new();
            let mut it = groups.into_iter();
            let mut inflight = 0;
            loop {
                while inflight < 6 {
                    let Some(g) = it.next() else { break };
                    let (link, f2) = (sh.link.clone(), f.clone());
                    set.spawn(async move {
                        let hashes: Vec<Hash> = g.iter().map(|c| c.0).collect();
                        let got = match link.call(Request::GetBlobs { hashes }).await {
                            Ok(Response::Blobs { blobs }) if blobs.len() == g.len() => blobs,
                            Ok(_) | Err(CallErr::Server(_)) => vec![None; g.len()],
                            Err(e) => return Err(format!("{e:?}")),
                        };
                        for ((ch, len, o), data) in g.into_iter().zip(got) {
                            // anything not returned (pending on another host, over the frame cap) goes the relaying way
                            let data = match data {
                                Some(d) => d,
                                None => match link.call(Request::GetBlob { hash: ch }).await {
                                    Ok(Response::Blob { data }) => data,
                                    Ok(r) => return Err(format!("unexpected {r:?}")),
                                    Err(e) => return Err(format!("{e:?}")),
                                },
                            };
                            if Hash::of(&data) != ch || data.len() != len as usize {
                                return Err("chunk hash mismatch".to_string());
                            }
                            f2.write_all_at(&data, o).map_err(|e| e.to_string())?;
                        }
                        Ok(())
                    });
                    inflight += 1;
                }
                if inflight == 0 {
                    break;
                }
                match set.join_next().await {
                    Some(Ok(Ok(()))) => inflight -= 1,
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => return Err(e.to_string()),
                    None => break,
                }
            }
            Ok(())
        }
    }
}

// ======================================================================
// Core trait
// ======================================================================

fn cred_of(o: Origin) -> Cred {
    match o {
        Origin::Local { uid, gid, .. } => Cred { uid, gid },
        Origin::Remote => Cred { uid: 0, gid: 0 },
    }
}

impl Core for RootCore {
    fn lookup(&self, parent: Ino, name: &[u8]) -> FsResult<NodeInfo> {
        if name.len() > 255 {
            return Err(Errno::ENAMETOOLONG);
        }
        // fast path: one lock acquisition when the directory is already listed (the common case)
        let pn = {
            let mut st = self.st.lock();
            let pn = self.node_of_ino(&st, parent)?;
            if st.visible.listed.contains(&pn) {
                let n = st.visible.lookup(pn, name).ok_or(Errno::ENOENT)?;
                return self.info(&mut st, n);
            }
            pn
        };
        self.ensure_listed(pn)?;
        let mut st = self.st.lock();
        let n = st.visible.lookup(pn, name).ok_or(Errno::ENOENT)?;
        self.info(&mut st, n)
    }

    fn getattr(&self, ino: Ino) -> FsResult<NodeInfo> {
        let mut st = self.st.lock();
        let n = self.node_of_ino(&st, ino)?;
        self.info(&mut st, n)
    }

    fn readdir(&self, dir: Ino) -> FsResult<Arc<DirSnapshot>> {
        let dn = {
            let st = self.st.lock();
            self.node_of_ino(&st, dir)?
        };
        self.ensure_listed(dn)?;
        let mut st = self.st.lock();
        if !st.visible.is_dir(dn) {
            return Err(Errno::ENOTDIR);
        }
        let kids: Vec<(NameB, NodeId)> = st.visible.children(dn).map(|c| c.iter().map(|(k, v)| (k.clone(), *v)).collect()).unwrap_or_default();
        let mut entries = Vec::with_capacity(kids.len());
        for (name, n) in kids {
            let kind = match st.visible.node(n) {
                Some(x) => x.attrs.kind,
                None => continue,
            };
            let (ino, _) = st.ino(n);
            entries.push(DirEntryInfo { name, ino, kind });
        }
        Ok(Arc::new(DirSnapshot { entries }))
    }

    fn getxattr(&self, ino: Ino, name: &[u8]) -> FsResult<Vec<u8>> {
        let st = self.st.lock();
        let n = self.node_of_ino(&st, ino)?;
        let node = st.visible.node(n).ok_or(Errno::ENOENT)?;
        node.attrs.xattrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone()).ok_or(Errno::ENODATA)
    }

    fn listxattr(&self, ino: Ino) -> FsResult<Vec<Vec<u8>>> {
        let st = self.st.lock();
        let n = self.node_of_ino(&st, ino)?;
        let node = st.visible.node(n).ok_or(Errno::ENOENT)?;
        Ok(node.attrs.xattrs.iter().map(|(k, _)| k.clone()).collect())
    }

    fn path_of(&self, ino: Ino) -> Option<Vec<u8>> {
        let st = self.st.lock();
        let n = *st.node_of.get(&ino)?;
        let p = st.visible.path_of(n)?;
        Some(p.join(&b'/'))
    }

    fn create(&self, origin: Origin, parent: Ino, name: &[u8], new: NewNode) -> FsResult<NodeInfo> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            let p = self.node_of_ino(&st, parent)?;
            self.remote_apply(&mut st);
            let n = st.visible.lookup(p, name).ok_or(Errno::ENOENT)?;
            return self.info(&mut st, n);
        }
        validate_name(name)?;
        self.check_writable()?;
        let pn = {
            let st = self.st.lock();
            self.node_of_ino(&st, parent)?
        };
        self.ensure_listed(pn)?;
        let mut st = self.st.lock();
        let local = self.is_local_target(&st, pn, name);
        let node = self.sh.mint_node();
        let op = st.visible.plan_create(pn, name, &new, node, now_ns())?;
        if local {
            st.local_only.insert(node);
        }
        self.journal(&mut st, op, cred_of(origin), local);
        self.info(&mut st, node)
    }

    fn link(&self, origin: Origin, ino: Ino, new_parent: Ino, new_name: &[u8]) -> FsResult<NodeInfo> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            let p = self.node_of_ino(&st, new_parent)?;
            self.remote_apply(&mut st);
            let n = st.visible.lookup(p, new_name).ok_or(Errno::ENOENT)?;
            return self.info(&mut st, n);
        }
        self.check_writable()?;
        let (node, pn) = {
            let st = self.st.lock();
            (self.node_of_ino(&st, ino)?, self.node_of_ino(&st, new_parent)?)
        };
        self.ensure_listed(pn)?;
        let mut st = self.st.lock();
        if st.is_local(node) != self.is_local_target(&st, pn, new_name) {
            return Err(Errno::EXDEV);
        }
        let local = st.is_local(node);
        let op = st.visible.plan_link(node, pn, new_name)?;
        self.journal(&mut st, op, cred_of(origin), local);
        self.info(&mut st, node)
    }

    fn unlink(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            self.node_of_ino(&st, parent)?;
            self.remote_apply(&mut st);
            return Ok(());
        }
        self.check_writable()?;
        let pn = {
            let st = self.st.lock();
            self.node_of_ino(&st, parent)?
        };
        self.ensure_listed(pn)?;
        let mut st = self.st.lock();
        let op = st.visible.plan_unlink(pn, name)?;
        let local = matches!(&op, Op::Unlink { node, .. } if st.is_local(*node));
        self.journal(&mut st, op, cred_of(origin), local);
        Ok(())
    }

    fn rmdir(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            self.node_of_ino(&st, parent)?;
            self.remote_apply(&mut st);
            return Ok(());
        }
        self.check_writable()?;
        let pn = {
            let st = self.st.lock();
            self.node_of_ino(&st, parent)?
        };
        self.ensure_listed(pn)?;
        // the child must be listed to know it is empty
        let child = self.st.lock().visible.lookup(pn, name);
        if let Some(c) = child {
            if self.st.lock().visible.is_dir(c) {
                self.ensure_listed(c)?;
            }
        }
        let mut st = self.st.lock();
        let op = st.visible.plan_rmdir(pn, name)?;
        let local = matches!(&op, Op::Unlink { node, .. } if st.is_local(*node));
        self.journal(&mut st, op, cred_of(origin), local);
        Ok(())
    }

    fn rename(&self, origin: Origin, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8], mode: RenameMode) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            self.node_of_ino(&st, parent)?;
            self.remote_apply(&mut st);
            return Ok(());
        }
        self.check_writable()?;
        let (sp, dp) = {
            let st = self.st.lock();
            (self.node_of_ino(&st, parent)?, self.node_of_ino(&st, new_parent)?)
        };
        self.ensure_listed(sp)?;
        self.ensure_listed(dp)?;
        let occ = self.st.lock().visible.lookup(dp, new_name);
        if let Some(o) = occ {
            if self.st.lock().visible.is_dir(o) {
                self.ensure_listed(o)?;
            }
        }
        let mut st = self.st.lock();
        let Some(op) = st.visible.plan_rename(sp, name, dp, new_name, mode)? else { return Ok(()) };
        let Op::Rename { node, replaces, .. } = &op else { unreachable!() };
        let src_local = st.is_local(*node);
        let dst_local = self.is_local_target(&st, dp, new_name);
        if src_local != dst_local {
            // TODO: crossing a local_only boundary should become create/delete toward the server
            return Err(Errno::EXDEV);
        }
        if let Some(r) = replaces {
            if st.is_local(*r) != src_local {
                return Err(Errno::EXDEV);
            }
        }
        let moved = *node;
        self.journal(&mut st, op, cred_of(origin), src_local);
        // atomic-save rename: a temp file renamed over a name while its writer is still open is a
        // save event (a version is published now, not at the eventual close)
        if st.files.get(&moved).is_some_and(|f| f.writers > 0 && f.dirty) {
            self.enqueue_save(&mut st, moved, true);
        }
        Ok(())
    }

    fn setattr(&self, origin: Origin, ino: Ino, changes: AttrChanges, size: Option<u64>) -> FsResult<NodeInfo> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            let n = self.node_of_ino(&st, ino)?;
            self.remote_apply(&mut st);
            return self.info(&mut st, n);
        }
        self.check_writable()?;
        let node = {
            let st = self.st.lock();
            self.node_of_ino(&st, ino)?
        };
        let cred = cred_of(origin);
        if let Some(sz) = size {
            self.truncate(node, sz, cred)?;
        }
        let mut st = self.st.lock();
        if changes != AttrChanges::default() {
            let op = st.visible.plan_setattr(node, changes.clone())?;
            let local = st.is_local(node);
            self.journal(&mut st, op, cred, local);
            // keep the live file's mtime coherent with what we told the kernel
            if let (Some(mt), Some(f)) = (changes.mtime_ns, st.files.get(&node)) {
                if f.live {
                    set_mtime(&self.sh.cache.live_path(&self.name, f.ino), mt);
                }
            }
        }
        self.info(&mut st, node)
    }

    fn setxattr(&self, origin: Origin, ino: Ino, name: &[u8], value: &[u8], flags: XattrFlags) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            self.node_of_ino(&st, ino)?;
            self.remote_apply(&mut st);
            return Ok(());
        }
        self.check_writable()?;
        let mut st = self.st.lock();
        let node = self.node_of_ino(&st, ino)?;
        let op = st.visible.plan_setxattr(node, name, value, flags)?;
        let local = st.is_local(node);
        self.journal(&mut st, op, cred_of(origin), local);
        Ok(())
    }

    fn removexattr(&self, origin: Origin, ino: Ino, name: &[u8]) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        if origin == Origin::Remote {
            let mut st = self.st.lock();
            self.node_of_ino(&st, ino)?;
            self.remote_apply(&mut st);
            return Ok(());
        }
        self.check_writable()?;
        let mut st = self.st.lock();
        let node = self.node_of_ino(&st, ino)?;
        let op = st.visible.plan_removexattr(node, name)?;
        let local = st.is_local(node);
        self.journal(&mut st, op, cred_of(origin), local);
        Ok(())
    }

    fn open(&self, origin: Origin, ino: Ino, write: bool, truncate: bool) -> FsResult<OpenFile> {
        let write = write || truncate;
        let node = {
            let st = self.st.lock();
            self.node_of_ino(&st, ino)?
        };
        let remote = origin == Origin::Remote;
        if remote {
            // The applier only opens to generate inotify events: hand back a
            // throw-away file; the live file is refreshed lazily (stale flag).
            let mut st = self.st.lock();
            self.remote_apply(&mut st);
            if st.visible.node(node).is_none() && !st.zombies.contains_key(&node) {
                return Err(Errno::ENOENT);
            }
            let t = self.sh.cache.tmp_path("remote");
            let file = fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).mode(0o600).open(&t).map_err(|_| Errno::EIO)?;
            let _ = fs::remove_file(&t);
            let token = st.next_token;
            st.next_token += 1;
            st.opens.insert(token, OpenRec { node, write, remote: true });
            return Ok(OpenFile { file, token });
        }
        if write {
            self.check_writable()?;
        }
        let lock = self.node_lock(node);
        let _g = lock.lock();
        let (path, fresh) = self.ensure_live(node, write)?;
        if write {
            // the live file is about to change: a published version still waiting for its CAS
            // commit must be committed first
            self.settle_commit(node);
        }
        // a live file just created for this open is already open read-write: reuse that fd
        let file = match fresh {
            Some(f) => {
                if truncate {
                    f.set_len(0).map_err(|_| Errno::EIO)?;
                }
                f
            }
            None => {
                if truncate {
                    let f = fs::OpenOptions::new().write(true).open(&path).map_err(|_| Errno::EIO)?;
                    f.set_len(0).map_err(|_| Errno::EIO)?;
                }
                fs::OpenOptions::new().read(true).write(write).open(&path).map_err(|_| Errno::EIO)?
            }
        };
        let mut st = self.st.lock();
        let token = st.next_token;
        st.next_token += 1;
        st.opens.insert(token, OpenRec { node, write, remote: false });
        let f = st.files.entry(node).or_default();
        f.opens += 1;
        f.idle_seq = 0;
        if write {
            if !f.dirty {
                f.open_stat = if truncate { None } else { fs::metadata(&path).ok().map(|m| (m.len(), m.mtime() * 1_000_000_000 + m.mtime_nsec(), m.ctime() * 1_000_000_000 + m.ctime_nsec())) };
            }
            f.writers += 1;
            f.dirty = true;
            f.wgen += 1;
            f.write_since.get_or_insert_with(Instant::now);
            if let Origin::Local { uid, gid, .. } = origin {
                f.last_cred = Some((uid, gid));
            }
        }
        Ok(OpenFile { file, token })
    }

    fn release(&self, ino: Ino, token: u64) -> FsResult<()> {
        let _sp = SyncOnDrop(&self.sh.db); // op log write(2) after the state lock is released
        let rec = {
            let mut st = self.st.lock();
            match st.opens.remove(&token) {
                Some(r) => r,
                None => return Ok(()),
            }
        };
        let _ = ino;
        if rec.remote {
            return Ok(());
        }
        let node = rec.node;
        let lock = self.node_lock(node);
        let _g = lock.lock();
        let (publish, cred) = {
            let mut st = self.st.lock();
            let Some(f) = st.files.get_mut(&node) else { return Ok(()) };
            f.opens = f.opens.saturating_sub(1);
            if rec.write {
                f.writers = f.writers.saturating_sub(1);
                if f.writers == 0 {
                    f.write_since = None;
                    f.ckpt = None;
                    f.last_pub = None;
                }
            }
            let cred = f.last_cred.unwrap_or((0, 0));
            let mut publish = f.writers == 0 && f.dirty;
            if publish && st.visible.node(node).is_none() {
                // unlinked while open: nothing to publish
                let f = st.files.get_mut(&node).unwrap();
                f.dirty = false;
                publish = false;
            }
            (publish, Cred { uid: cred.0, gid: cred.1 })
        };
        let mut res = Ok(());
        if publish {
            let sync_close = {
                let st = self.st.lock();
                self.policy_of_node(&st, node) == PolicyMode::SyncOnClose
            };
            if !sync_close {
                // save event: last writer closed. The pipeline hashes and publishes in the
                // background; the reply does not wait for any of it.
                let mut st = self.st.lock();
                self.enqueue_save(&mut st, node, false);
                return Ok(());
            }
            match self.publish_sync(node, cred, true) {
                Ok(Some(n)) => {
                    let content = self.st.lock().visible.node(node).and_then(|x| x.attrs.content);
                    if sync_close && self.sh.link.is_connected() {
                        let ok = self.wait_until(Duration::from_secs(120), |st| st.pending.front().map(|p| p.n > n).unwrap_or(true))
                            && self.wait_until(Duration::from_secs(120), |_| !content.map(|c| self.sh.uploads.contains(&c)).unwrap_or(false));
                        if !ok {
                            res = Err(Errno::EIO);
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => res = Err(e),
            }
        }
        let mut st = self.st.lock();
        self.cleanup_file(&mut st, node);
        res
    }

    fn fsync(&self, ino: Ino, _datasync: bool) -> FsResult<()> {
        let (node, live) = {
            let st = self.st.lock();
            let n = self.node_of_ino(&st, ino)?;
            (n, st.files.get(&n).filter(|f| f.live).map(|f| f.ino))
        };
        if let Some(i) = live {
            if let Ok(f) = fs::OpenOptions::new().read(true).open(self.sh.cache.live_path(&self.name, i)) {
                let _ = f.sync_data();
            }
        }
        {
            let st = self.st.lock();
            if self.policy_of_node(&st, node) == PolicyMode::SyncOnClose && st.files.get(&node).map(|f| f.writers > 0).unwrap_or(false) {
                let cred = st.files[&node].last_cred.unwrap_or((0, 0));
                drop(st);
                let lock = self.node_lock(node);
                let _g = lock.lock();
                if let Ok(Some(n)) = self.publish_sync(node, Cred { uid: cred.0, gid: cred.1 }, false) {
                    if self.sh.link.is_connected() {
                        self.wait_until(Duration::from_secs(120), |st| st.pending.front().map(|p| p.n > n).unwrap_or(true));
                    }
                }
            } else if st.files.get(&node).map(|f| f.writers > 0 && f.dirty).unwrap_or(false) {
                // save event: fsync/fdatasync of a file being written publishes a version
                drop(st);
                let mut st = self.st.lock();
                self.enqueue_save(&mut st, node, true);
            }
        }
        self.sh.db.sync_log();
        Ok(())
    }

    fn sync_all(&self) -> FsResult<()> {
        self.sh.db.sync_log();
        Ok(())
    }

    fn set_applier(&self, applier: Arc<dyn KernelApplier>) {
        *self.applier.lock() = Some(applier);
        self.sh.bump_event();
    }

    fn apply_direct(&self, change: &Change) -> FsResult<()> {
        let mut st = self.st.lock();
        st.apply_visible(change);
        Ok(())
    }
}
