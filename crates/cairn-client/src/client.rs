//! `Client`: shared state (state DB, local CAS, server link, runtime) and
//! one [`Core`](crate::api::Core) per configured root.

use crate::api::Core;
use crate::cas::Cache;
use crate::config::Config;
use crate::conn::{self, Link, Uploads};
use crate::core::RootCore;
use crate::store::{Db, DbOp};
use anyhow::{anyhow, Context, Result};
use cairn_proto::*;
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Counter {
    next: u64,
    /// ids below this are durably reserved
    limit: u64,
    /// reservation queued to the DB writer but not yet known durable (0 = none)
    pending: u64,
}

pub struct Shared {
    pub cfg: Config,
    pub db: Arc<Db>,
    pub cache: Cache,
    pub rt: tokio::runtime::Handle,
    pub link: Arc<Link>,
    host: AtomicU64,
    host_known: AtomicBool,
    node_ctr: Mutex<Counter>,
    op_ctr: Mutex<Counter>,
    pub uploads: Uploads,
    pub pins: Mutex<HashSet<Hash>>,
    events: (Mutex<u64>, Condvar),
    pub shutdown: AtomicBool,
    pub fetch_locks: Mutex<HashMap<Hash, Arc<Mutex<()>>>>,
}

const RESERVE: u64 = 1 << 16;

impl Shared {
    pub fn host_id(&self) -> HostId {
        HostId(self.host.load(Ordering::SeqCst))
    }
    pub fn host_known(&self) -> bool {
        self.host_known.load(Ordering::SeqCst)
    }
    pub fn set_host(&self, h: HostId) {
        self.host.store(h.0, Ordering::SeqCst);
        self.host_known.store(true, Ordering::SeqCst);
        self.db.meta_u64("host_id", h.0);
    }
    fn take(&self, c: &Mutex<Counter>, key: &str) -> u64 {
        let mut c = c.lock();
        // Persist the next block of ids ahead of need, asynchronously (halfway through the current
        // one), so the hot path never waits on a state-DB commit; only running a whole block dry
        // before the write landed has to wait (flush).
        if c.pending == 0 && c.next + RESERVE / 2 >= c.limit {
            c.pending = c.limit.max(c.next) + RESERVE;
            self.db.meta_u64(key, c.pending);
        }
        if c.next >= c.limit {
            self.db.sync_log();
            c.limit = c.pending;
            c.pending = 0;
        }
        let v = c.next;
        c.next += 1;
        v
    }
    pub fn mint_node(&self) -> NodeId {
        NodeId::new(self.host_id(), self.take(&self.node_ctr, "node_ctr"))
    }
    pub fn next_op(&self) -> u64 {
        self.take(&self.op_ctr, "op_ctr")
    }
    /// Wake everyone waiting for acks / uploads / applied_seq.
    pub fn bump_event(&self) {
        *self.events.0.lock() += 1;
        self.events.1.notify_all();
    }
    /// Block until an event happens or `d` passes.
    pub fn wait_event(&self, d: Duration) {
        let mut g = self.events.0.lock();
        let _ = self.events.1.wait_for(&mut g, d);
    }
    pub fn pin(&self, h: Hash) {
        if self.pins.lock().insert(h) {
            self.db.send(DbOp::Pin { hash: h, add: true });
        }
    }
    pub fn is_pinned(&self, h: &Hash) -> bool {
        self.pins.lock().contains(h)
    }
    /// Evict cold CAS objects when free space is below the low watermark.
    pub fn maybe_evict(&self) {
        let free = self.cache.free_bytes();
        if free >= self.cfg.evict_below_free {
            return;
        }
        let need = self.cfg.evict_to_free.saturating_sub(free);
        let pins = self.pins.lock().clone();
        let freed = self.cache.evict(need, &|h| pins.contains(h) || self.uploads.contains_hash(h));
        tracing::info!("cache eviction freed {freed} bytes");
    }
}

pub struct Client {
    sh: Arc<Shared>,
    roots: HashMap<String, Arc<RootCore>>,
    rt: Option<tokio::runtime::Runtime>,
}

impl Client {
    pub fn open(cfg: Config) -> Result<Client> {
        static DETECTOR: std::sync::Once = std::sync::Once::new();
        DETECTOR.call_once(|| {
            // report mutex deadlocks (parking_lot) instead of hanging silently
            let _ = std::thread::Builder::new().name("cairn-deadlock".into()).spawn(|| loop {
                std::thread::sleep(Duration::from_secs(10));
                for (i, threads) in parking_lot::deadlock::check_deadlock().iter().enumerate() {
                    for t in threads {
                        eprintln!("cairn-client: DEADLOCK #{i} thread {:?}\n{:?}", t.thread_id(), t.backtrace());
                        tracing::error!("deadlock #{i} thread {:?}: {:?}", t.thread_id(), t.backtrace());
                    }
                }
            });
        });
        std::fs::create_dir_all(&cfg.cache_dir).with_context(|| format!("create {}", cfg.cache_dir.display()))?;
        let cache = Cache::open(&cfg.cache_dir)?;
        let names: Vec<String> = cfg.roots.keys().cloned().collect();
        let t_open = std::time::Instant::now();
        let (db, loaded) = Db::open(&cfg.cache_dir.join("state.db"), &names, Duration::from_millis(cfg.commit_interval_ms), cfg.commit_max_ops, Duration::from_millis(cfg.log_sync_ms))?;
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("cairn-rt").enable_all().build()?;
        let (link, link_rx) = Link::new();
        let (host, known) = match (cfg.host_id, loaded.host_id) {
            (Some(h), _) => (h, true),
            (None, Some(h)) => (h, true),
            // never connected: provisional random prefix for NodeIds (OpIds are built at send time)
            (None, None) => (rand::random::<u64>() | (1 << 63), false),
        };
        let sh = Arc::new(Shared {
            cfg: cfg.clone(),
            db: Arc::new(db),
            cache,
            rt: rt.handle().clone(),
            link,
            host: AtomicU64::new(host),
            host_known: AtomicBool::new(known),
            node_ctr: Mutex::new(Counter { next: loaded.node_ctr, limit: loaded.node_ctr, pending: 0 }),
            op_ctr: Mutex::new(Counter { next: loaded.op_ctr, limit: loaded.op_ctr, pending: 0 }),
            uploads: Uploads::default(),
            pins: Mutex::new(loaded.pins.iter().copied().collect()),
            events: (Mutex::new(0), Condvar::new()),
            shutdown: AtomicBool::new(false),
            fetch_locks: Mutex::new(HashMap::new()),
        });
        if known {
            sh.db.meta_u64("host_id", host);
        }
        for c in &loaded.uploads {
            sh.uploads.restore(*c);
        }
        tracing::info!("startup: state db + op log replay {:?}", t_open.elapsed());
        let mut roots = HashMap::new();
        let mut loaded_roots = loaded.roots;
        for (name, rc) in &cfg.roots {
            let lr = loaded_roots.remove(name).unwrap_or_default();
            let root = RootCore::new(sh.clone(), name, rc.clone(), lr)?;
            roots.insert(name.clone(), root);
        }
        let list: Vec<Arc<RootCore>> = roots.values().cloned().collect();
        for r in &list {
            r.start_threads();
            rt.spawn(conn::sender_task(sh.clone(), r.clone()));
        }
        rt.spawn(conn::upload_task(sh.clone()));
        if !cfg.offline && !cfg.server.is_empty() {
            rt.spawn(conn::connection_task(sh.clone(), link_rx, list));
        } else {
            drop(link_rx);
        }
        tracing::info!("startup: roots built {:?}", t_open.elapsed());
        for r in roots.values() {
            r.recover_live_files();
        }
        tracing::info!("startup: recovery done {:?}", t_open.elapsed());
        Ok(Client { sh, roots, rt: Some(rt) })
    }

    /// The `Core` for a configured root.
    pub fn core(&self, root: &str) -> Result<Arc<dyn Core>> {
        let r = self.roots.get(root).ok_or_else(|| anyhow!("root {root} is not configured"))?;
        Ok(r.clone() as Arc<dyn Core>)
    }

    /// The concrete root (inspection helpers for tests and tools).
    pub fn root(&self, root: &str) -> Option<Arc<RootCore>> {
        self.roots.get(root).cloned()
    }

    pub fn host_id(&self) -> HostId {
        self.sh.host_id()
    }
    pub fn is_connected(&self) -> bool {
        self.sh.link.is_connected()
    }
    /// Test hook: drop the connection and stay offline until `resume`.
    pub fn pause(&self) {
        self.sh.link.paused.store(true, Ordering::SeqCst);
        self.sh.link.changed.notify_waiters();
    }
    pub fn resume(&self) {
        self.sh.link.paused.store(false, Ordering::SeqCst);
        self.sh.link.changed.notify_waiters();
    }
    /// Commit the state DB now.
    pub fn flush(&self) {
        self.sh.db.flush();
    }
    pub fn cache(&self) -> &Cache {
        &self.sh.cache
    }
    /// Contents waiting for upload.
    pub fn uploads_pending(&self) -> usize {
        self.sh.uploads.len()
    }
    /// Wait until every root is online, has nothing pending and all uploads are done.
    pub fn wait_synced(&self, timeout: Duration) -> bool {
        let end = std::time::Instant::now() + timeout;
        for r in self.roots.values() {
            r.rush_all();
        }
        loop {
            if self.roots.values().all(|r| r.is_quiescent()) && self.sh.uploads.len() == 0 {
                return true;
            }
            if std::time::Instant::now() >= end {
                return false;
            }
            self.sh.wait_event(Duration::from_millis(20));
        }
    }
    /// Wait until the write pipeline of every root is idle (everything hashed, published and
    /// committed to the local CAS; uploads are not waited for).
    pub fn wait_pipeline(&self, timeout: Duration) -> bool {
        let end = std::time::Instant::now() + timeout;
        for r in self.roots.values() {
            r.rush_all();
        }
        loop {
            if self.roots.values().all(|r| r.pipeline_idle()) {
                return true;
            }
            if std::time::Instant::now() >= end {
                return false;
            }
            self.sh.wait_event(Duration::from_millis(20));
        }
    }
    /// Wait until the applier has processed everything received so far.
    pub fn settle(&self) {
        for r in self.roots.values() {
            r.barrier();
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.sh.shutdown.store(true, Ordering::SeqCst);
        for r in self.roots.values() {
            r.stop();
        }
        self.sh.db.flush();
        self.sh.link.changed.notify_waiters();
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}
