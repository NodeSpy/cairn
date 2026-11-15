//! The staged write pipeline, the deferred-delete reaper and their recovery (docs/CLIENT.md
//! "Staged write pipeline", "Deferred deletes").
//!
//! A file that has seen a *save event* (last writer closed, fsync/fdatasync, atomic-save rename)
//! goes through `dirty -> hashed -> published -> committed -> uploaded`:
//!
//! - **dirty**: journaled in the op log (`DbOp::File`) when the event happens; the FUSE reply does
//!   not wait for anything below.
//! - **hashed**: a worker hashes the live file straight from the page cache (BLAKE3 / FastCDC, no
//!   copy). `stat` before and after (plus the per-node write generation) detects a write during
//!   hashing: the result is discarded and the file requeued (a stale upload would be harmless
//!   anyway, the server verifies content hashes). A file that still has writers (fsync saves) is
//!   hashed from a reflink snapshot instead, which is a point-in-time view.
//! - **published**: the `SetContent` is journaled (or folded into the unsent `Create`), so other
//!   hosts see the new version. Journaled as `Published{content}` so recovery can re-enter.
//! - **committed**: after the upload hold (default 1 s, so short-lived files fold away) the live
//!   file is `FICLONE`d into the CAS (extents shared: the data hits the disk once), or, if the
//!   object is already in the CAS, the idle live file is re-cloned from it (its own extents
//!   freed). An idle live file that must go before that is renamed into the CAS instead.
//! - **uploaded**: the CAS object is queued for the uploader.
//!
//! (The spec order was uploaded -> committed; committing first gives the uploader a stable
//! point-in-time object to read, and `open(write)` settles a pending commit anyway.)

use super::*;
use crate::store::FileRow;
use parking_lot::Condvar;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Stat {
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

pub fn stat_of(m: &fs::Metadata) -> Stat {
    Stat { size: m.len(), mtime_ns: m.mtime() * 1_000_000_000 + m.mtime_nsec(), ctime_ns: m.ctime() * 1_000_000_000 + m.ctime_nsec() }
}

/// Persisted stage numbers (`FileRow::stage`).
pub const STAGE_DIRTY: u8 = 0;
pub const STAGE_PUBLISHED: u8 = 2;

pub struct CommitJob {
    pub id: u64,
    pub node: NodeId,
    pub content: ContentId,
    /// Stat of the live file when it was hashed; `None` = the object is already in the CAS.
    pub stat: Option<Stat>,
    pub upload: bool,
    pub due: Instant,
}

#[derive(Default)]
pub struct Pipe {
    pub q: VecDeque<NodeId>,
    /// commit jobs, ordered by `due` (a rush moves a job to the front)
    pub jobs: VecDeque<CommitJob>,
    /// saves held back (hot-file rate limit)
    pub delayed: Vec<(Instant, NodeId)>,
    pub active: usize,
    pub next_job: u64,
    pub stop: bool,
    pub rush_all: bool,
    /// next scan of open-for-write files for checkpoints
    pub next_ckpt: Option<Instant>,
    /// a checkpoint scan is running (they must not overlap: each takes a diff base)
    pub ckpt_running: bool,
}

#[derive(Default)]
pub struct Reaper {
    pub q: Mutex<VecDeque<u64>>,
    pub pending: Mutex<HashSet<u64>>,
    pub cv: Condvar,
    pub stop: AtomicBool,
}

pub struct Prepared {
    pub h: crate::cas::Hashed,
    pub mtime: i64,
    pub stat: Option<Stat>,
}

pub enum Prep {
    Clean,
    Retry,
    Ready(Prepared),
}

impl RootCore {
    // ------------------------------------------------------------ queueing
    /// A save event for `node` (caller holds the root lock). Journals `Dirty` and wakes a worker.
    pub(super) fn enqueue_save(&self, st: &mut RootState, node: NodeId, hot: bool) {
        let name = st.name.clone();
        let Some(f) = st.files.get_mut(&node) else { return };
        f.dirty = true;
        f.hot |= hot;
        let ino = f.ino;
        let log_dirty = !f.row_dirty;
        f.row_dirty = true;
        if log_dirty {
            st.db.send(DbOp::File { root: name, node, row: Some(FileRow { ino, stage: STAGE_DIRTY, content: None, size: 0, mtime_ns: 0 }) });
        }
        let f = st.files.get_mut(&node).unwrap();
        if f.queued {
            f.rehash = true;
            return;
        }
        f.queued = true;
        let mut p = self.pipe.lock();
        p.q.push_back(node);
        drop(p);
        self.pipe_cv.notify_one();
    }

    pub fn rush_content(&self, h: &Hash) {
        let mut p = self.pipe.lock();
        let mut i = 0;
        let mut found = false;
        while i < p.jobs.len() {
            if object_hash(&p.jobs[i].content) == Some(*h) {
                let mut j = p.jobs.remove(i).unwrap();
                j.due = Instant::now();
                p.jobs.push_front(j);
                found = true;
            }
            i += 1;
        }
        drop(p);
        if found {
            self.pipe_cv.notify_all();
        }
    }

    /// Make every held job due now (used when waiting for quiescence and at shutdown).
    pub fn rush_all(&self) {
        let mut p = self.pipe.lock();
        p.rush_all = true;
        drop(p);
        self.pipe_cv.notify_all();
    }

    pub fn pipeline_idle(&self) -> bool {
        let p = self.pipe.lock();
        p.q.is_empty() && p.jobs.is_empty() && p.delayed.is_empty() && p.active == 0
    }

    pub(super) fn start_pipeline(self: &Arc<Self>) {
        let n = self.sh.cfg.pipeline_threads.max(1);
        let mut ths = self.threads.lock();
        for i in 0..n {
            let me = self.clone();
            ths.push(std::thread::Builder::new().name(format!("cairn-pipe-{}-{i}", self.name)).spawn(move || me.pipe_loop()).expect("spawn pipeline"));
        }
        let me = self.clone();
        ths.push(std::thread::Builder::new().name(format!("cairn-reap-{}", self.name)).spawn(move || me.reap_loop()).expect("spawn reaper"));
    }

    /// The server adopted node `x` into the existing node `y`: queued saves and commit jobs of `x`
    /// (and their uploads) now belong to `y`.
    pub(super) fn remap_pipeline(&self, st: &mut RootState, x: NodeId, y: NodeId) {
        let mut p = self.pipe.lock();
        for n in p.q.iter_mut() {
            if *n == x {
                *n = y;
            }
        }
        for (_, n) in p.delayed.iter_mut() {
            if *n == x {
                *n = y;
            }
        }
        for j in p.jobs.iter_mut() {
            if j.node == x {
                j.node = y;
            }
        }
        drop(p);
        st.db.send(DbOp::File { root: self.name.clone(), node: x, row: None });
    }

    pub(super) fn stop_pipeline(&self) {
        {
            let mut p = self.pipe.lock();
            p.stop = true;
        }
        self.pipe_cv.notify_all();
        self.reaper.stop.store(true, Ordering::SeqCst);
        self.reaper.cv.notify_all();
    }

    fn pipe_loop(self: Arc<Self>) {
        enum W {
            Save(NodeId),
            Commit(CommitJob),
            Ckpt,
        }
        loop {
            let w = {
                let mut p = self.pipe.lock();
                loop {
                    let now = Instant::now();
                    // shutdown drains: queued saves and commit jobs run immediately, so nothing
                    // pending is left only in the journal
                    // held-back saves that became due
                    let mut i = 0;
                    while i < p.delayed.len() {
                        if p.delayed[i].0 <= now || p.stop || p.rush_all {
                            let (_, n) = p.delayed.swap_remove(i);
                            p.q.push_back(n);
                        } else {
                            i += 1;
                        }
                    }
                    if let Some(n) = p.q.pop_front() {
                        p.active += 1;
                        break W::Save(n);
                    }
                    if p.jobs.front().is_some_and(|j| j.due <= now || p.stop || p.rush_all) {
                        let j = p.jobs.pop_front().unwrap();
                        p.active += 1;
                        break W::Commit(j);
                    }
                    if p.jobs.is_empty() && p.delayed.is_empty() {
                        p.rush_all = false;
                    }
                    if p.stop && p.jobs.is_empty() && p.delayed.is_empty() {
                        return;
                    }
                    let ckpt_every = Duration::from_millis(self.sh.cfg.checkpoint_scan_ms.max(100));
                    if !p.stop && self.sh.cfg.checkpoint_secs > 0 {
                        let due = *p.next_ckpt.get_or_insert(now + ckpt_every);
                        if due <= now && !p.ckpt_running {
                            p.ckpt_running = true;
                            p.next_ckpt = Some(now + ckpt_every);
                            p.active += 1;
                            break W::Ckpt;
                        }
                    }
                    let mut wake = now + Duration::from_secs(1);
                    if let Some(j) = p.jobs.front() {
                        wake = wake.min(j.due);
                    }
                    for (d, _) in &p.delayed {
                        wake = wake.min(*d);
                    }
                    if let Some(c) = p.next_ckpt.filter(|_| !p.ckpt_running) {
                        wake = wake.min(c);
                    }
                    let _ = self.pipe_cv.wait_until(&mut p, wake);
                }
            };
            match w {
                W::Save(n) => self.run_save(n),
                W::Commit(j) => self.run_commit(j),
                W::Ckpt => {
                    self.checkpoint_scan();
                    self.pipe.lock().ckpt_running = false;
                }
            }
            self.pipe.lock().active -= 1;
            self.sh.bump_event();
        }
    }

    // --------------------------------------------------- hash + publish
    /// One pass of the pipeline for a dirty node: hash, publish, schedule the commit.
    fn run_save(&self, node: NodeId) {
        let mut tries = 0u32;
        loop {
            let (hot, cred, local, wgen0) = {
                let mut st = self.st.lock();
                let local = st.is_local(node);
                let Some(f) = st.files.get_mut(&node) else { return };
                f.rehash = false;
                let hot = std::mem::take(&mut f.hot) || f.writers > 0 || tries >= 3;
                let c = f.last_cred.unwrap_or((0, 0));
                (hot, Cred { uid: c.0, gid: c.1 }, local, f.wgen)
            };
            if !local {
                self.acquire_lease(node);
            }
            let mut retry = false;
            match self.hash_phase(node, hot) {
                Ok(Prep::Clean) => {}
                Ok(Prep::Retry) => retry = true,
                Ok(Prep::Ready(p)) => {
                    let lock = self.node_lock(node);
                    let _g = lock.lock();
                    match self.publish_phase(node, cred, !hot, p, wgen0, false) {
                        Err(Errno::EAGAIN) => retry = true,
                        Ok(_) => {}
                        Err(e) => tracing::warn!("{node:?}: publish failed: {e:?}"),
                    }
                }
                Err(e) => tracing::warn!("{node:?}: hash failed: {e:?} (file stays dirty)"),
            }
            if retry {
                tries += 1;
                if tries > 6 {
                    tracing::warn!("{node:?}: file keeps changing while hashing; leaving it dirty");
                } else {
                    continue;
                }
            }
            let mut st = self.st.lock();
            let again = match st.files.get_mut(&node) {
                Some(f) if f.rehash && !retry => true,
                Some(f) => {
                    f.queued = false;
                    false
                }
                None => false,
            };
            if again {
                // a save event arrived meanwhile: rate-limit hot files, otherwise go straight on
                let min = Duration::from_millis(self.sh.cfg.hot_save_min_ms);
                let hot_again = st.files.get(&node).map(|f| f.hot || f.writers > 0).unwrap_or(false);
                if hot_again && !min.is_zero() {
                    drop(st);
                    let mut p = self.pipe.lock();
                    p.delayed.push((Instant::now() + min, node));
                    return;
                }
                drop(st);
                tries = 0;
                continue;
            }
            drop(st);
            break;
        }
        let lock = self.node_lock(node);
        let _g = lock.lock();
        let mut st = self.st.lock();
        if st.files.get(&node).map(|f| f.writers == 0).unwrap_or(true) {
            self.drop_ckpt(node);
        }
        self.cleanup_file(&mut st, node);
    }

    /// Stat + hash without any locks held. `hot`: hash a reflink snapshot (a file with writers).
    fn hash_phase(&self, node: NodeId, hot: bool) -> FsResult<Prep> {
        let (ino, gone) = {
            let st = self.st.lock();
            let Some(f) = st.files.get(&node) else { return Ok(Prep::Clean) };
            (f.ino, st.visible.node(node).is_none())
        };
        let live = self.sh.cache.live_path(&self.name, ino);
        let rp = if gone { None } else { self.st.lock().files.get_mut(&node).filter(|f| f.writers == 0).and_then(|f| f.recover_pub.take()) };
        if let Some(c) = rp {
            if matches!(crate::cas::Cache::hash_file(&live), Ok(h) if h.content == c) {
                let stat = fs::metadata(&live).ok().map(|m| stat_of(&m));
                let mut st = self.st.lock();
                let upload = !st.is_local(node);
                let mut p = self.pipe.lock();
                p.next_job += 1;
                let id = p.next_job;
                p.jobs.push_back(CommitJob { id, node, content: c, stat, upload, due: Instant::now() });
                drop(p);
                if let Some(f) = st.files.get_mut(&node) {
                    f.job = Some(id);
                    f.dirty = false;
                }
                return Ok(Prep::Clean);
            }
        }
        if gone {
            let mut st = self.st.lock();
            if let Some(f) = st.files.get_mut(&node) {
                f.dirty = false;
                if f.row_dirty {
                    f.row_dirty = false;
                    st.db.send(DbOp::File { root: self.name.clone(), node, row: None });
                }
            }
            return Ok(Prep::Clean);
        }
        let meta = fs::metadata(&live).map_err(|_| Errno::EIO)?;
        if meta.len() > MAX_FILE_SIZE {
            tracing::warn!("{node:?}: live file is {} bytes, over the {} limit; not publishing", meta.len(), MAX_FILE_SIZE);
            return Err(Errno(libc::EFBIG));
        }
        let s0 = stat_of(&meta);
        {
            let mut st = self.st.lock();
            let name = self.name.clone();
            if let Some(f) = st.files.get_mut(&node) {
                if f.open_stat == Some((s0.size, s0.mtime_ns, s0.ctime_ns)) {
                    // writers wrote nothing: no hash, no commit
                    f.dirty = f.writers > 0;
                    f.open_stat = None;
                    if f.row_dirty && f.writers == 0 {
                        f.row_dirty = false;
                        st.db.send(DbOp::File { root: name, node, row: None });
                    }
                    self.kick_reconcile(&st, node);
                    return Ok(Prep::Clean);
                }
            }
        }
        let mtime = s0.mtime_ns;
        if (hot || self.ckpts.lock().contains_key(&node)) && meta.len() > INCR_MIN && self.sh.cache.reflinks() {
            // big file with writers: snapshot, rehash only what changed since the last snapshot
            return self.snapshot_prepared(node, &live, true).map(Prep::Ready);
        }
        if hot {
            // point-in-time view: reflink snapshot, hash it, move it into the CAS
            let snap = self.sh.cache.tmp_path("snap");
            self.sh.cache.clone_file(&live, &snap).map_err(|e| {
                tracing::warn!("snapshot failed: {e}");
                Errno::EIO
            })?;
            let h = self.sh.cache.ingest(&snap).map_err(|e| {
                tracing::warn!("ingest failed: {e}");
                Errno::EIO
            })?;
            return Ok(Prep::Ready(Prepared { h, mtime, stat: None }));
        }
        let h = crate::cas::Cache::hash_file(&live).map_err(|_| Errno::EIO)?;
        let s1 = fs::metadata(&live).map(|m| stat_of(&m)).map_err(|_| Errno::EIO)?;
        if s1 != s0 {
            return Ok(Prep::Retry);
        }
        Ok(Prep::Ready(Prepared { h, mtime, stat: Some(s0) }))
    }

    /// Journal the `SetContent` (or fold it into an unsent `Create`/`SetContent`) and schedule the
    /// commit. Caller holds the node lock. `sync`: no upload hold, and the commit runs before
    /// returning. Returns the journal op number, if any.
    pub(super) fn publish_phase(&self, node: NodeId, cred: Cred, want_release: bool, prep: Prepared, wgen0: u64, sync: bool) -> FsResult<Option<u64>> {
        let _sp = SyncOnDrop(&self.sh.db); // after the state lock is released
        let h = prep.h.clone();
        let mtime = prep.mtime;
        let mut st = self.st.lock();
        let Some(f) = st.files.get(&node) else { return Ok(None) };
        let (ino, live_ok) = (f.ino, f.live);
        if st.visible.node(node).is_none() || !live_ok {
            if let Some(f) = st.files.get_mut(&node) {
                f.dirty = false;
            }
            return Ok(None);
        }
        if let Some(s) = prep.stat {
            // the live file must still be exactly what we hashed
            let cur = fs::metadata(self.sh.cache.live_path(&self.name, ino)).map(|m| stat_of(&m));
            if st.files[&node].wgen != wgen0 || cur.ok() != Some(s) {
                return Err(Errno::EAGAIN);
            }
        }
        let local = st.is_local(node);
        {
            let f = st.files.get_mut(&node).unwrap();
            f.dirty = f.writers > 0;
            f.stale = false;
            f.open_stat = None;
        }
        let name = self.name.clone();
        let clear_row = |st: &mut RootState| {
            if let Some(f) = st.files.get_mut(&node) {
                if f.writers == 0 && f.job.is_none() && f.row_dirty {
                    f.row_dirty = false;
                    st.db.send(DbOp::File { root: name.clone(), node, row: None });
                }
            }
        };
        let Some((ncontent, nsize, ncver)) = st.visible.node(node).map(|n| (n.attrs.content, n.attrs.size, n.cversion)) else { return Ok(None) };
        if ncontent == Some(h.content) && nsize == h.size {
            // Same content as already published. If an earlier commit did not complete (the live
            // file's stat moved under it, e.g. a chmod/utimens after close) the bytes may still be
            // only in the live file: commit (and upload) them now.
            if let Some(oh) = object_hash(&h.content) {
                if !self.sh.cache.has(&oh) && prep.stat.is_some() && st.files.get(&node).is_some_and(|f| f.job.is_none()) {
                    let upload = !local;
                    self.schedule_commit(&mut st, node, ino, &h, &prep, upload, sync);
                    return Ok(None);
                }
            }
            clear_row(&mut st);
            self.kick_reconcile(&st, node);
            return Ok(None);
        }
        let policy = self.policy_of_node(&st, node);
        if local || policy == PolicyMode::Pinned {
            if let Some(oh) = object_hash(&h.content) {
                self.sh.pin(oh);
            }
        }
        // An unsent Create/SetContent for this node is superseded in place (the server then sees
        // the final content with the Create, so identical files from two hosts can merge).
        let mut num = None;
        let mut done = false;
        if !local {
            let now = now_ns();
            let mut folded = None;
            for p in st.pending.iter_mut().rev() {
                if p.sent {
                    break;
                }
                match &mut p.op {
                    Op::Create { node: m, attrs, .. } if *m == node => {
                        attrs.content = Some(h.content);
                        attrs.size = h.size;
                        attrs.mtime_ns = mtime;
                        attrs.ctime_ns = now;
                        folded = Some((p.n, p.op.clone(), p.cred, p.ts));
                        break;
                    }
                    Op::SetContent { node: m, content, size, mtime_ns, .. } if *m == node => {
                        *content = h.content;
                        *size = h.size;
                        *mtime_ns = mtime;
                        folded = Some((p.n, p.op.clone(), p.cred, p.ts));
                        break;
                    }
                    _ => {}
                }
            }
            if let Some((n, op, c, ts)) = folded {
                st.db.send(DbOp::Outbound { root: self.name.clone(), n, row: Some((op, c, ts)) });
                st.visible.apply_op(&Op::SetContent { node, base: Seq(0), content: h.content, size: h.size, mtime_ns: mtime }, now);
                st.persist();
                num = Some(n);
                done = true;
                // the superseded version was never sent: its bytes need never leave this host
                self.cancel_jobs_locked(&mut st, node, Some(h.content));
            }
        }
        if !done {
            let dep = st.pending.iter().any(|p| matches!(&p.op, Op::Create { node: m, .. } | Op::SetContent { node: m, .. } if *m == node));
            let base = if dep { BASE_DEP } else { ncver };
            let op = Op::SetContent { node, base, content: h.content, size: h.size, mtime_ns: mtime };
            num = self.journal(&mut st, op, cred, local);
            if let Some(num) = num {
                let writers = st.files.get(&node).map(|f| f.writers).unwrap_or(0);
                if want_release && writers == 0 && st.leases.contains(&node) {
                    st.release_after.insert(num, node);
                }
            }
        }
        // stage: published -> schedule the commit (+ upload) after the hold
        let upload = !local && num.is_some();
        self.schedule_commit(&mut st, node, ino, &h, &prep, upload, sync);
        if object_hash(&h.content).is_none() {
            clear_row(&mut st);
        }
        Ok(num)
    }

    /// The file is no longer dirty: server state that was withheld from the visible tree while it
    /// was (remote content change) must be applied now.
    pub(super) fn kick_reconcile(&self, st: &RootState, node: NodeId) {
        if st.files.get(&node).map(|f| f.writers > 0 || f.dirty).unwrap_or(false) {
            return;
        }
        let (Some(v), Some(s)) = (st.visible.node(node), st.synced.node(node)) else { return };
        if v.attrs.content != s.attrs.content && !st.pidx.has_attr_ops(node) {
            self.send_work(Work::Reconcile { fp: Footprint { keys: vec![], nodes: vec![node] }, seq: Seq(0) });
        }
    }

    /// Remove commit jobs of `node` (superseded content), except one for `keep`.
    pub(super) fn cancel_jobs_locked(&self, st: &mut RootState, node: NodeId, keep: Option<ContentId>) {
        let mut p = self.pipe.lock();
        p.jobs.retain(|j| !(j.node == node && Some(j.content) != keep));
        let still = p.jobs.iter().find(|j| j.node == node).map(|j| j.id);
        drop(p);
        if let Some(f) = st.files.get_mut(&node) {
            f.job = still;
        }
    }

    fn schedule_commit(&self, st: &mut RootState, node: NodeId, ino: u64, h: &crate::cas::Hashed, prep: &Prepared, upload: bool, sync: bool) {
        if object_hash(&h.content).is_none() {
            return;
        }
        let hold = if sync { Duration::from_secs(3600) /* the caller runs it inline */ } else { Duration::from_millis(self.sh.cfg.upload_hold_ms) };
        let mut p = self.pipe.lock();
        // a newer version supersedes an older job of the same node that did not need to go out
        if st.files.get(&node).is_some_and(|f| f.job.is_some()) {
            p.jobs.retain(|j| !(j.node == node && j.content != h.content && j.stat.is_some()));
        }
        p.next_job += 1;
        let id = p.next_job;
        let job = CommitJob { id, node, content: h.content, stat: prep.stat, upload, due: Instant::now() + hold };
        p.jobs.push_back(job);
        drop(p);
        if let Some(f) = st.files.get_mut(&node) {
            f.job = Some(id);
            f.row_dirty = true;
        }
        st.db.send(DbOp::File { root: self.name.clone(), node, row: Some(FileRow { ino, stage: STAGE_PUBLISHED, content: Some(h.content), size: h.size, mtime_ns: prep.mtime }) });
        self.pipe_cv.notify_one();
    }

    // ----------------------------------------------------------- commit
    fn run_commit(&self, job: CommitJob) {
        let lock = self.node_lock(job.node);
        let _g = lock.lock();
        self.run_commit_locked(job, true);
    }

    /// Commit a job: the live file's bytes into the CAS, then queue the upload. Node lock held.
    pub(super) fn run_commit_locked(&self, job: CommitJob, allow_move: bool) {
        let _sp = SyncOnDrop(&self.sh.db); // after the state lock is released
        let (ino, opens, current) = {
            let st = self.st.lock();
            match st.files.get(&job.node) {
                Some(f) if f.job == Some(job.id) => (f.ino, f.opens, true),
                _ => (0, 0, false),
            }
        };
        if !current {
            return; // superseded or the file is gone
        }
        let live = self.sh.cache.live_path(&self.name, ino);
        let mut committed = true;
        // An idle file (nobody has it open, nothing newer pending) is *moved* into the CAS: no
        // FICLONE (on btrfs that forces the dirty pages of a fresh file to disk and waits), no
        // second copy; a later open recreates the live file as a reflink of the object. If the
        // object already exists the live file is redundant and goes to the reaper.
        if let (Some(h), Some(s)) = (object_hash(&job.content), job.stat) {
            let idle = {
                let st = self.st.lock();
                st.files.get(&job.node).is_some_and(|f| f.opens == 0 && f.writers == 0 && !f.dirty && !f.queued && f.live)
            };
            if allow_move && idle && fs::metadata(&live).map(|m| stat_of(&m)).ok() == Some(s) {
                // (`adopt` also covers the dedup case: the redundant live file is just removed)
                if self.sh.cache.adopt(&live, &h).is_ok() {
                    let mut st = self.st.lock();
                    if let Some(f) = st.files.remove(&job.node) {
                        if f.row_dirty {
                            st.db.send(DbOp::File { root: self.name.clone(), node: job.node, row: None });
                        }
                    }
                    if job.upload && self.sh.uploads.add(job.content) {
                        st.db.send(DbOp::Upload { content: job.content, add: true });
                    }
                    drop(st);
                    self.sh.maybe_evict();
                    return;
                }
            }
        }
        if let Some(h) = object_hash(&job.content) {
            if !self.sh.cache.has(&h) {
                committed = match job.stat {
                    Some(s) => self.clone_into_cas(&live, &h, s),
                    None => false,
                };
            } else if let (Some(s), 0) = (job.stat, opens) {
                // content already in the CAS: the idle live file's own extents are redundant
                if fs::metadata(&live).map(|m| stat_of(&m)).ok() == Some(s) {
                    self.reclone_live(&live, &h);
                }
            }
        }
        let mut st = self.st.lock();
        if let Some(f) = st.files.get_mut(&job.node) {
            if f.job == Some(job.id) {
                f.job = None;
                if f.writers == 0 && !f.dirty && f.row_dirty {
                    f.row_dirty = false;
                    st.db.send(DbOp::File { root: self.name.clone(), node: job.node, row: None });
                }
            }
        }
        if !committed {
            // changed behind our back (cannot happen with the write-open settle): publish again
            tracing::warn!("{:?}: live file changed before its commit; republishing", job.node);
            self.enqueue_save(&mut st, job.node, false);
            return;
        }
        if job.upload && self.sh.uploads.add(job.content) {
            st.db.send(DbOp::Upload { content: job.content, add: true });
        }
        drop(st);
        self.sh.maybe_evict();
    }

    /// `FICLONE` the live file into a new CAS object (verified against `s` before and after).
    fn clone_into_cas(&self, live: &std::path::Path, h: &Hash, s: Stat) -> bool {
        let cur = fs::metadata(live).map(|m| stat_of(&m));
        if cur.ok() != Some(s) {
            return false;
        }
        let tmp = self.sh.cache.tmp_path("cas");
        if self.sh.cache.clone_file(live, &tmp).is_err() {
            let _ = fs::remove_file(&tmp);
            return false;
        }
        if fs::metadata(live).map(|m| stat_of(&m)).ok() != Some(s) {
            let _ = fs::remove_file(&tmp);
            return false;
        }
        self.sh.cache.adopt(&tmp, h).is_ok()
    }

    /// Replace the live file's extents with a reflink of the CAS object (same bytes).
    fn reclone_live(&self, live: &std::path::Path, h: &Hash) {
        if !self.sh.cache.reflinks() {
            return;
        }
        let (Ok(mut d), Ok(mut s)) = (fs::OpenOptions::new().write(true).open(live), self.sh.cache.open_cas(h)) else { return };
        let _ = self.sh.cache.clone_into(&mut s, &mut d);
    }

    /// An idle live file whose commit is pending must go (eviction): rename it into the CAS.
    pub(super) fn retire_uncommitted(&self, st: &mut RootState, node: NodeId) {
        let Some(f) = st.files.get(&node) else { return };
        let Some(id) = f.job else { return };
        let ino = f.ino;
        let job = {
            let mut p = self.pipe.lock();
            p.jobs.iter().position(|j| j.id == id).and_then(|i| p.jobs.remove(i))
        };
        let Some(job) = job else { return };
        if st.visible.node(node).is_none() && !job.upload {
            // the node was deleted and nothing sent refers to its bytes
            if let Some(f) = st.files.get_mut(&node) {
                f.job = None;
            }
            return;
        }
        let live = self.sh.cache.live_path(&self.name, ino);
        if let (Some(h), Some(s)) = (object_hash(&job.content), job.stat) {
            if !self.sh.cache.has(&h) && fs::metadata(&live).map(|m| stat_of(&m)).ok() == Some(s) {
                let _ = self.sh.cache.adopt(&live, &h);
            }
        }
        if let Some(f) = st.files.get_mut(&node) {
            f.job = None;
        }
        if job.upload && self.sh.uploads.add(job.content) {
            st.db.send(DbOp::Upload { content: job.content, add: true });
        }
    }

    /// Before the live file of `node` is modified or replaced: finish a pending commit so the
    /// published version's bytes are kept. Node lock held.
    pub(super) fn settle_commit(&self, node: NodeId) {
        let id = {
            let st = self.st.lock();
            st.files.get(&node).and_then(|f| f.job)
        };
        let Some(id) = id else { return };
        let job = {
            let mut p = self.pipe.lock();
            p.jobs.iter().position(|j| j.id == id).and_then(|i| p.jobs.remove(i))
        };
        if let Some(mut j) = job {
            // upload later with the normal hold; only the bytes must be pinned now
            let up = j.upload;
            j.upload = false;
            let (content, node) = (j.content, j.node);
            let stat = j.stat;
            let id = j.id;
            // (the caller is about to reuse the live file: never move it away)
            self.run_commit_locked(j, false);
            if up {
                // keep the upload of the settled version scheduled (object is in the CAS now)
                let mut st = self.st.lock();
                let mut p = self.pipe.lock();
                p.next_job += 1;
                let nid = p.next_job;
                p.jobs.push_back(CommitJob { id: nid, node, content, stat: None, upload: true, due: Instant::now() + Duration::from_millis(self.sh.cfg.upload_hold_ms) });
                drop(p);
                if let Some(f) = st.files.get_mut(&node) {
                    f.job = Some(nid);
                }
                self.pipe_cv.notify_one();
            }
            let _ = (stat, id);
        }
    }

    // ----------------------------------------------------------- sync API
    /// Hash + publish + commit inline (recall, `sync_on_close`). Node lock held by the caller.
    pub(super) fn publish_sync(&self, node: NodeId, cred: Cred, want_release: bool) -> FsResult<Option<u64>> {
        let local = self.st.lock().is_local(node);
        if !local {
            self.acquire_lease(node);
        }
        for attempt in 0..4 {
            let (hot, wgen0) = {
                let st = self.st.lock();
                let Some(f) = st.files.get(&node) else { return Ok(None) };
                (f.writers > 0 || attempt >= 2, f.wgen)
            };
            match self.hash_phase(node, hot)? {
                Prep::Clean => return Ok(None),
                Prep::Retry => continue,
                Prep::Ready(p) => match self.publish_phase(node, cred, want_release, p, wgen0, true) {
                    Err(Errno::EAGAIN) => continue,
                    r => {
                        // run the commit now (no hold)
                        let id = self.st.lock().files.get(&node).and_then(|f| f.job);
                        if let Some(id) = id {
                            let job = {
                                let mut p = self.pipe.lock();
                                p.jobs.iter().position(|j| j.id == id).and_then(|i| p.jobs.remove(i))
                            };
                            if let Some(j) = job {
                                self.run_commit_locked(j, true);
                            }
                        }
                        return r;
                    }
                },
            }
        }
        Err(Errno::EIO)
    }

    // --------------------------------------------------------- recovery
    /// Re-enter the pipeline at the journaled state of every file that was not committed.
    pub fn recover_pipeline(&self, rows: Vec<(NodeId, FileRow)>) {
        for (node, row) in rows {
            let path = self.sh.cache.live_path(&self.name, row.ino);
            let exists = path.exists();
            let visible = self.st.lock().visible.node(node).map(|n| n.attrs.kind == Kind::File).unwrap_or(false);
            if !exists || !visible {
                if row.stage == STAGE_PUBLISHED && exists {
                    // the node is gone but its published bytes may be wanted by the server's pending record
                }
                self.sh.db.send(DbOp::File { root: self.name.clone(), node, row: None });
                if exists && !visible {
                    self.reap_ino(row.ino);
                }
                continue;
            }
            {
                let mut st = self.st.lock();
                let f = st.files.entry(node).or_default();
                f.ino = row.ino;
                f.live = true;
                f.row_dirty = true;
            }
            // a published row only needs its commit if the bytes are still the published ones; that
            // is checked by a pipeline worker (hashing 1000s of files here would delay the mount)
            if let (STAGE_PUBLISHED, Some(c)) = (row.stage, row.content) {
                if let Some(f) = self.st.lock().files.get_mut(&node) {
                    f.recover_pub = Some(c);
                }
            }
            let mut st = self.st.lock();
            self.enqueue_save(&mut st, node, false);
        }
        self.pipe_cv.notify_all();
    }

    // ------------------------------------------------- deferred deletes
    /// Queue live file `ino` for the reaper (it is garbage). Logged, so a crash does not leak it.
    pub(super) fn reap_ino(&self, ino: u64) {
        if self.reaper.pending.lock().insert(ino) {
            self.sh.db.send(DbOp::Reap { root: self.name.clone(), ino, add: true });
            self.reaper.q.lock().push_back(ino);
            self.reaper.cv.notify_one();
        }
    }

    /// A new live file for `ino` is about to be created: finish its pending delete first.
    pub(super) fn settle_reap(&self, ino: u64) {
        // the unlink happens under the `pending` lock, like the reaper's, so a reaper that already
        // took the ino finishes unlinking before the caller creates the new file
        let mut p = self.reaper.pending.lock();
        if p.remove(&ino) {
            let _ = fs::remove_file(self.sh.cache.live_path(&self.name, ino));
        }
    }

    pub fn reaper_idle(&self) -> bool {
        self.reaper.pending.lock().is_empty()
    }

    fn reap_loop(self: Arc<Self>) {
        loop {
            let ino = {
                let mut q = self.reaper.q.lock();
                loop {
                    if let Some(i) = q.pop_front() {
                        break Some(i);
                    }
                    if self.reaper.stop.load(Ordering::SeqCst) {
                        break None;
                    }
                    self.reaper.cv.wait_for(&mut q, Duration::from_millis(500));
                }
            };
            let Some(ino) = ino else { return };
            {
                let mut p = self.reaper.pending.lock();
                if p.remove(&ino) {
                    let _ = fs::remove_file(self.sh.cache.live_path(&self.name, ino));
                }
            }
            self.sh.db.send(DbOp::Reap { root: self.name.clone(), ino, add: false });
        }
    }
}

