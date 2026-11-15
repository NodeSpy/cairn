//! Server connection manager: mTLS connect, Hello/Mount/Subscribe, request
//! multiplexing by id, pushes, reconnect with backoff, epoch check and
//! resync; plus the per-root commit sender and the content uploader.

use crate::cas::{object_hash, Cache};
use crate::client::Shared;
use crate::core::{RootCore, Work};
use cairn_proto::aio::{read_envelope, write_envelope};
use cairn_proto::*;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify};

#[derive(Debug)]
pub enum CallErr {
    Offline,
    Server(ErrorReply),
}

const REQ_NAMES: &[&str] = &["Hello", "Mount", "Subscribe", "ReadDir", "GetAttr", "Snapshot", "Commit", "Has", "PutBlob", "PutBlobs", "PutManifest", "GetBlob", "GetBlobs", "GetManifest", "AcquireWrite", "ReleaseWrite", "Conflicts", "Ping"];
static REQ_COUNTS: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];
static COMMIT_OPS: AtomicU64 = AtomicU64::new(0);

fn req_idx(r: &Request) -> usize {
    match r {
        Request::Hello { .. } => 0,
        Request::Mount { .. } => 1,
        Request::Subscribe { .. } => 2,
        Request::ReadDir { .. } => 3,
        Request::GetAttr { .. } => 4,
        Request::Snapshot { .. } => 5,
        Request::Commit { .. } => 6,
        Request::Has { .. } => 7,
        Request::PutBlob { .. } => 8,
        Request::PutBlobs { .. } => 9,
        Request::PutManifest { .. } => 10,
        Request::GetBlob { .. } => 11,
        Request::GetBlobs { .. } => 12,
        Request::GetManifest { .. } => 13,
        Request::AcquireWrite { .. } => 14,
        Request::ReleaseWrite { .. } => 15,
        Request::Conflicts { .. } => 16,
        Request::Ping => 17,
    }
}

/// Requests sent to the server since the process started, by kind, plus the number of ops
/// carried by `Commit` requests (`CommitOps`).
pub fn req_counts() -> Vec<(&'static str, u64)> {
    let mut v: Vec<_> = REQ_NAMES.iter().enumerate().map(|(i, n)| (*n, REQ_COUNTS[i].load(Ordering::Relaxed))).filter(|(_, c)| *c > 0).collect();
    v.push(("CommitOps", COMMIT_OPS.load(Ordering::Relaxed)));
    v
}

pub struct ReqMsg {
    pub req: Request,
    pub resp: oneshot::Sender<Result<Response, CallErr>>,
}

pub struct Link {
    pub tx: mpsc::UnboundedSender<ReqMsg>,
    pub connected: AtomicBool,
    /// Test hook: while set, the manager stays disconnected.
    pub paused: AtomicBool,
    pub gen: AtomicU64,
    handles: Mutex<HashMap<String, RootHandle>>,
    pub changed: Notify,
}

impl Link {
    pub fn new() -> (Arc<Link>, mpsc::UnboundedReceiver<ReqMsg>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Arc::new(Link { tx, connected: AtomicBool::new(false), paused: AtomicBool::new(false), gen: AtomicU64::new(0), handles: Mutex::new(HashMap::new()), changed: Notify::new() }), rx)
    }
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    pub fn handle(&self, server_root: &str) -> Option<RootHandle> {
        self.handles.lock().get(server_root).copied()
    }
    pub async fn call(&self, req: Request) -> Result<Response, CallErr> {
        if !self.is_connected() {
            return Err(CallErr::Offline);
        }
        REQ_COUNTS[req_idx(&req)].fetch_add(1, Ordering::Relaxed);
        if let Request::Commit { commits, .. } = &req {
            COMMIT_OPS.fetch_add(commits.len() as u64, Ordering::Relaxed);
        }
        let (tx, rx) = oneshot::channel();
        self.tx.send(ReqMsg { req, resp: tx }).map_err(|_| CallErr::Offline)?;
        // never block a caller forever on a lost answer
        match tokio::time::timeout(Duration::from_secs(180), rx).await {
            Ok(r) => r.map_err(|_| CallErr::Offline)?,
            Err(_) => Err(CallErr::Offline),
        }
    }
    /// Call with a root handle filled in; Offline if the root is not mounted.
    pub async fn root_call(&self, server_root: &str, f: impl FnOnce(RootHandle) -> Request) -> Result<Response, CallErr> {
        let h = self.handle(server_root).ok_or(CallErr::Offline)?;
        self.call(f(h)).await
    }
}

struct Mounted {
    handle: RootHandle,
    epoch: Epoch,
    head: Seq,
    writable: bool,
}

pub async fn connection_task(sh: Arc<Shared>, mut rx: mpsc::UnboundedReceiver<ReqMsg>, roots: Vec<Arc<RootCore>>) {
    let mut backoff = Duration::from_millis(100);
    loop {
        if sh.shutdown.load(Ordering::SeqCst) {
            return;
        }
        while let Ok(m) = rx.try_recv() {
            let _ = m.resp.send(Err(CallErr::Offline));
        }
        if sh.link.paused.load(Ordering::SeqCst) {
            let _ = tokio::time::timeout(Duration::from_millis(200), sh.link.changed.notified()).await;
            continue;
        }
        let started = Instant::now();
        match run_session(&sh, &mut rx, &roots).await {
            Ok(()) => {}
            Err(e) => tracing::debug!("session ended: {e:#}"),
        }
        sh.link.connected.store(false, Ordering::SeqCst);
        for r in &roots {
            r.on_disconnect();
        }
        sh.bump_event();
        if started.elapsed() > Duration::from_secs(5) {
            backoff = Duration::from_millis(100);
        }
        let jitter = Duration::from_millis(rand::random::<u64>() % 50);
        let _ = tokio::time::timeout(backoff + jitter, sh.link.changed.notified()).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

async fn rpc<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(s: &mut S, id: &mut u64, req: Request) -> anyhow::Result<Response> {
    *id += 1;
    write_envelope(s, &Envelope { id: *id, body: Body::Request(req) }).await?;
    loop {
        let env = read_envelope(s).await?.ok_or_else(|| anyhow::anyhow!("connection closed"))?;
        if env.id != *id {
            continue;
        }
        return match env.body {
            Body::Response(Ok(r)) => Ok(r),
            Body::Response(Err(e)) => Err(anyhow::anyhow!("server error {:?}: {}", e.code, e.msg)),
            _ => continue,
        };
    }
}

async fn run_session(sh: &Arc<Shared>, rx: &mut mpsc::UnboundedReceiver<ReqMsg>, roots: &[Arc<RootCore>]) -> anyhow::Result<()> {
    let tls = tokio_rustls::TlsConnector::from(tls::client_config(&sh.cfg.creds_dir)?);
    let tcp = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(&sh.cfg.server)).await??;
    tcp.set_nodelay(true)?;
    let mut s = tokio::time::timeout(Duration::from_secs(10), tls.connect(tls::server_name(), tcp)).await??;
    let mut id = 0u64;
    match rpc(&mut s, &mut id, Request::Hello { proto: PROTOCOL_VERSION, software: "cairn-client".into() }).await? {
        Response::Welcome { host, .. } => sh.set_host(host),
        r => anyhow::bail!("unexpected hello reply {r:?}"),
    }
    let mut mounted = vec![];
    for r in roots {
        match rpc(&mut s, &mut id, Request::Mount { root: RootName(r.server_name.clone()) }).await {
            Ok(Response::Mounted { root, epoch, head, writable }) => mounted.push(Some(Mounted { handle: root, epoch, head, writable })),
            Ok(r2) => anyhow::bail!("unexpected mount reply {r2:?}"),
            Err(e) => {
                tracing::warn!("mount {} failed: {e:#}", r.server_name);
                mounted.push(None);
            }
        }
    }
    let (mut rd, mut wr) = tokio::io::split(s);
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Envelope>();
    let (in_tx, mut in_rx) = mpsc::unbounded_channel::<Option<Envelope>>();
    let wtask = tokio::spawn(async move {
        while let Some(e) = out_rx.recv().await {
            if write_envelope(&mut wr, &e).await.is_err() {
                break;
            }
        }
    });
    let rtask = tokio::spawn(async move {
        loop {
            match read_envelope(&mut rd).await {
                Ok(Some(e)) => {
                    if in_tx.send(Some(e)).is_err() {
                        break;
                    }
                }
                _ => {
                    let _ = in_tx.send(None);
                    break;
                }
            }
        }
    });
    {
        let mut h = sh.link.handles.lock();
        h.clear();
        for (r, m) in roots.iter().zip(&mounted) {
            if let Some(m) = m {
                h.insert(r.server_name.clone(), m.handle);
            }
        }
    }
    let mut by_handle: HashMap<RootHandle, Arc<RootCore>> = HashMap::new();
    sh.link.gen.fetch_add(1, Ordering::SeqCst);
    sh.link.connected.store(true, Ordering::SeqCst);
    sh.link.changed.notify_waiters();
    let mut bring = vec![];
    for (r, m) in roots.iter().zip(mounted) {
        if let Some(m) = m {
            by_handle.insert(m.handle, r.clone());
            r.set_writable(m.writable);
            let (r2, sh2) = (r.clone(), sh.clone());
            bring.push(tokio::spawn(async move {
                if let Err(e) = bringup(&sh2, &r2, m).await {
                    tracing::warn!("bringup {} failed: {e:#}", r2.name);
                }
            }));
        }
    }

    let mut pending: HashMap<u64, oneshot::Sender<Result<Response, CallErr>>> = HashMap::new();
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.tick().await;
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    let mut last_rx = Instant::now();
    let res: anyhow::Result<()> = loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(m) = m else { break Ok(()) };
                id += 1;
                pending.insert(id, m.resp);
                if out_tx.send(Envelope { id, body: Body::Request(m.req) }).is_err() { break Err(anyhow::anyhow!("writer gone")); }
            }
            e = in_rx.recv() => {
                last_rx = Instant::now();
                match e {
                    Some(Some(env)) => match env.body {
                        Body::Response(r) => {
                            if let Some(tx) = pending.remove(&env.id) {
                                let _ = tx.send(r.map_err(CallErr::Server));
                            }
                        }
                        Body::Push(p) => handle_push(sh, &by_handle, p),
                        Body::Request(_) => {}
                    },
                    _ => break Err(anyhow::anyhow!("connection lost")),
                }
            }
            _ = ping.tick() => {
                if last_rx.elapsed() > Duration::from_secs(45) { break Err(anyhow::anyhow!("ping timeout")); }
                id += 1;
                let (t, _r) = oneshot::channel();
                pending.insert(id, t);
                let _ = out_tx.send(Envelope { id, body: Body::Request(Request::Ping) });
            }
            _ = sh.link.changed.notified() => {
                if sh.link.paused.load(Ordering::SeqCst) || sh.shutdown.load(Ordering::SeqCst) { break Ok(()); }
            }
            _ = poll.tick() => {
                if sh.link.paused.load(Ordering::SeqCst) || sh.shutdown.load(Ordering::SeqCst) { break Ok(()); }
            }
        }
    };
    sh.link.connected.store(false, Ordering::SeqCst);
    for b in bring {
        b.abort();
    }
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(CallErr::Offline));
    }
    wtask.abort();
    rtask.abort();
    res
}

fn handle_push(sh: &Arc<Shared>, by_handle: &HashMap<RootHandle, Arc<RootCore>>, p: Push) {
    match p {
        Push::Changes { root, changes } => {
            if let Some(r) = by_handle.get(&root) {
                r.send_work(Work::Changes(changes));
            }
        }
        Push::Recall { root, node } => {
            if let Some(r) = by_handle.get(&root).cloned() {
                std::thread::spawn(move || r.recall(node));
            }
        }
        Push::NeedContent { hash } => {
            // relay: bypass the upload hold of a published-but-held version
            for r in by_handle.values() {
                r.rush_content(&hash);
            }
            sh.uploads.prioritize(&hash)
        }
        Push::ResyncRequired { root } => {
            if let Some(r) = by_handle.get(&root).cloned() {
                let sh = sh.clone();
                tokio::spawn(async move {
                    r.online.store(false, Ordering::SeqCst);
                    let Some(handle) = sh.link.handle(&r.server_name) else { return };
                    match resync(&sh, &r, handle).await {
                        Ok(since) => {
                            if let Err(e) = subscribe(&sh, &r, handle, since).await {
                                tracing::warn!("resubscribe failed: {e:#}");
                            }
                        }
                        Err(e) => tracing::warn!("resync failed: {e}"),
                    }
                });
            }
        }
    }
}

async fn subscribe(sh: &Arc<Shared>, r: &Arc<RootCore>, handle: RootHandle, since: Seq) -> Result<(), String> {
    match sh.link.call(Request::Subscribe { root: handle, since }).await {
        Ok(Response::Subscribed { .. }) => {
            r.online.store(true, Ordering::SeqCst);
            r.wake.notify_one();
            Ok(())
        }
        Ok(Response::ResyncRequired { .. }) => Err("resync required again".into()),
        Ok(x) => Err(format!("unexpected {x:?}")),
        Err(e) => Err(format!("{e:?}")),
    }
}

async fn bringup(sh: &Arc<Shared>, r: &Arc<RootCore>, m: Mounted) -> Result<(), String> {
    let (stored, applied) = r.epoch_and_applied();
    let mut since = applied;
    let resync_needed = match stored {
        None => {
            r.set_epoch_applied(m.epoch, m.head);
            since = m.head;
            false
        }
        Some(e) => e != m.epoch || m.head < applied,
    };
    if resync_needed {
        since = resync_epoch(sh, r, m.handle, Some(m.epoch)).await?;
    }
    match sh.link.call(Request::Subscribe { root: m.handle, since }).await {
        Ok(Response::Subscribed { .. }) => {}
        Ok(Response::ResyncRequired { .. }) => {
            since = resync_epoch(sh, r, m.handle, Some(m.epoch)).await?;
            subscribe(sh, r, m.handle, since).await?;
            return finish_bringup(sh, r, m.handle).await;
        }
        Ok(x) => return Err(format!("unexpected {x:?}")),
        Err(e) => return Err(format!("{e:?}")),
    }
    r.online.store(true, Ordering::SeqCst);
    r.wake.notify_one();
    finish_bringup(sh, r, m.handle).await
}

async fn finish_bringup(sh: &Arc<Shared>, r: &Arc<RootCore>, handle: RootHandle) -> Result<(), String> {
    if let Ok(Response::NodeAttrs { attrs, version }) = sh.link.call(Request::GetAttr { root: handle, node: NodeId::ROOT }).await {
        r.set_root_attrs(attrs, version);
    }
    Ok(())
}

async fn resync(sh: &Arc<Shared>, r: &Arc<RootCore>, handle: RootHandle) -> Result<Seq, String> {
    resync_epoch(sh, r, handle, None).await
}

/// Fetch one directory listing (paged). Returns (min page version, entries).
pub async fn fetch_listing(link: &Link, handle: RootHandle, dir: NodeId) -> Result<(Seq, Vec<DirEntry>), CallErr> {
    let mut out: Vec<DirEntry> = vec![];
    let mut after: Option<Name> = None;
    let mut ver: Option<Seq> = None;
    loop {
        match link.call(Request::ReadDir { root: handle, dir, after: after.clone(), limit: 1000 }).await? {
            Response::DirListing { version, entries, done } => {
                ver = Some(ver.map(|v| v.min(version)).unwrap_or(version));
                after = entries.last().map(|e| e.name.clone());
                out.extend(entries);
                if done || after.is_none() {
                    break;
                }
            }
            _ => return Err(CallErr::Server(ErrorReply { code: ErrCode::Internal, msg: "bad ReadDir reply".into() })),
        }
    }
    Ok((ver.unwrap_or(Seq(0)), out))
}

/// Re-list every cached directory, diff against the synced tree and hand the
/// net difference to the applier. Returns the seq to subscribe from.
async fn resync_epoch(sh: &Arc<Shared>, r: &Arc<RootCore>, handle: RootHandle, new_epoch: Option<Epoch>) -> Result<Seq, String> {
    let listed = r.listed_dirs();
    let mut listings: Vec<(NodeId, Vec<DirEntry>)> = vec![];
    let mut vmin: Option<Seq> = None;
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    let mut seen: HashSet<NodeId> = HashSet::new();
    if listed.contains(&NodeId::ROOT) {
        queue.push_back(NodeId::ROOT);
    }
    while let Some(d) = queue.pop_front() {
        if !seen.insert(d) {
            continue;
        }
        match fetch_listing(&sh.link, handle, d).await {
            Ok((v, entries)) => {
                vmin = Some(vmin.map(|x: Seq| x.min(v)).unwrap_or(v));
                for e in &entries {
                    if e.attrs.kind == Kind::Dir && listed.contains(&e.node) {
                        queue.push_back(e.node);
                    }
                }
                listings.push((d, entries));
            }
            Err(CallErr::Server(e)) if e.code == ErrCode::NotFound => {}
            Err(e) => return Err(format!("{e:?}")),
        }
    }
    let head_now = match sh.link.call(Request::GetAttr { root: handle, node: NodeId::ROOT }).await {
        Ok(Response::NodeAttrs { version, .. }) => Some(version),
        _ => None,
    };
    let applied = vmin.or(head_now).unwrap_or(Seq(0));
    let (tx, rx) = oneshot::channel();
    r.send_work(Work::Resync { listings, applied, epoch: new_epoch, done: tx });
    // the applier may be parked in a kernel invalidate that needs a handler that needs this session: never wait unboundedly
    let _ = tokio::time::timeout(Duration::from_secs(5), rx).await;
    Ok(applied)
}

// ------------------------------------------------------------------ sender

/// Per-root commit sender: ships the journal in order, batched, and hands
/// results to the root.
pub async fn sender_task(sh: Arc<Shared>, r: Arc<RootCore>) {
    loop {
        if sh.shutdown.load(Ordering::SeqCst) {
            return;
        }
        let Some((commits, ns)) = r.take_batch() else {
            let _ = tokio::time::timeout(Duration::from_secs(1), r.wake.notified()).await;
            continue;
        };
        let handle = match sh.link.handle(&r.server_name) {
            Some(h) => h,
            None => {
                r.unsend(&ns);
                continue;
            }
        };
        // never tell the server about an op that the local log could lose in a power cut: after
        // recovery the client would mint the same op ids and node ids for different operations
        if !sh.db.is_durable() {
            let db = sh.db.clone();
            let _ = tokio::task::spawn_blocking(move || db.sync_log()).await;
        }
        match sh.link.call(Request::Commit { root: handle, commits }).await {
            Ok(Response::Committed { results }) if results.len() == ns.len() => {
                let release = r.handle_results(&ns, results);
                for node in release {
                    let _ = sh.link.call(Request::ReleaseWrite { root: handle, node }).await;
                    r.lease_released(node);
                }
                sh.bump_event();
            }
            Ok(x) => {
                tracing::warn!("bad commit reply: {x:?}");
                r.unsend(&ns);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(CallErr::Offline) => {
                r.unsend(&ns);
                let _ = tokio::time::timeout(Duration::from_secs(1), r.wake.notified()).await;
            }
            Err(CallErr::Server(e)) => {
                tracing::warn!("commit rejected by server: {:?} {}", e.code, e.msg);
                r.unsend(&ns);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// ---------------------------------------------------------------- uploader

#[derive(Default)]
struct UpInner {
    q: VecDeque<ContentId>,
    set: HashSet<ContentId>,
    /// hashes a relay asked for before they were queued: jump the queue when added
    rush: HashSet<Hash>,
}

#[derive(Default)]
pub struct Uploads {
    inner: Mutex<UpInner>,
    pub wake: Notify,
}

impl Uploads {
    pub fn add(&self, c: ContentId) -> bool {
        if c == ContentId::Empty {
            return false;
        }
        let mut g = self.inner.lock();
        if g.set.insert(c) {
            if object_hash(&c).is_some_and(|h| g.rush.remove(&h)) {
                g.q.push_front(c);
            } else {
                g.q.push_back(c);
            }
            drop(g);
            self.wake.notify_one();
            true
        } else {
            false
        }
    }
    /// Load persisted entries without persisting them again.
    pub fn restore(&self, c: ContentId) {
        let mut g = self.inner.lock();
        if g.set.insert(c) {
            g.q.push_back(c);
        }
    }
    pub fn contains(&self, c: &ContentId) -> bool {
        self.inner.lock().set.contains(c)
    }
    pub fn contains_hash(&self, h: &Hash) -> bool {
        self.inner.lock().set.iter().any(|c| object_hash(c) == Some(*h))
    }
    pub fn len(&self) -> usize {
        self.inner.lock().set.len()
    }
    pub fn prioritize(&self, h: &Hash) {
        let mut g = self.inner.lock();
        if let Some(i) = g.q.iter().position(|c| object_hash(c) == Some(*h)) {
            let c = g.q.remove(i).unwrap();
            g.q.push_front(c);
        } else if g.rush.len() < 4096 {
            g.rush.insert(*h);
        }
        drop(g);
        self.wake.notify_one();
    }
    fn pop(&self) -> Option<ContentId> {
        self.inner.lock().q.pop_front()
    }
    fn push_front(&self, c: ContentId) {
        self.inner.lock().q.push_front(c);
    }
    fn push_back(&self, c: ContentId) {
        self.inner.lock().q.push_back(c);
    }
    fn done(&self, c: &ContentId) {
        self.inner.lock().set.remove(c);
    }
}

/// Upload batch: stop adding contents past this many bytes / contents.
const UP_BATCH_BYTES: u64 = 8 << 20;
const UP_BATCH_ITEMS: usize = 1024;
/// One `PutBlobs` frame carries at most this many bytes of blobs.
const PUT_FRAME_BYTES: usize = 4 << 20;

pub async fn upload_task(sh: Arc<Shared>) {
    loop {
        if sh.shutdown.load(Ordering::SeqCst) {
            return;
        }
        if !sh.link.is_connected() {
            let _ = tokio::time::timeout(Duration::from_millis(500), sh.link.changed.notified()).await;
            continue;
        }
        // take a batch: many small blobs share one Has and a few PutBlobs frames
        let mut batch: Vec<ContentId> = vec![];
        let mut bytes = 0u64;
        while batch.len() < UP_BATCH_ITEMS && bytes < UP_BATCH_BYTES {
            let Some(c) = sh.uploads.pop() else { break };
            bytes += object_hash(&c).and_then(|h| std::fs::metadata(sh.cache.cas_path(&h)).ok()).map(|m| m.len()).unwrap_or(0);
            batch.push(c);
        }
        if batch.is_empty() {
            let _ = tokio::time::timeout(Duration::from_secs(1), sh.uploads.wake.notified()).await;
            continue;
        }
        if batch.len() < UP_BATCH_ITEMS / 4 && bytes < UP_BATCH_BYTES / 4 {
            // contents trickle in one by one from the commit workers: gather a few more so that
            // they share a `Has` and a `PutBlobs` (uploads are held for folding anyway)
            tokio::time::sleep(Duration::from_millis(20)).await;
            while batch.len() < UP_BATCH_ITEMS && bytes < UP_BATCH_BYTES {
                let Some(c) = sh.uploads.pop() else { break };
                bytes += object_hash(&c).and_then(|h| std::fs::metadata(sh.cache.cas_path(&h)).ok()).map(|m| m.len()).unwrap_or(0);
                batch.push(c);
            }
        }
        match upload_batch(&sh, &batch).await {
            Ok(()) => {
                for c in &batch {
                    sh.uploads.done(c);
                    sh.db.send(crate::store::DbOp::Upload { content: *c, add: false });
                }
                sh.bump_event();
            }
            Err(CallErr::Offline) => {
                for c in batch.into_iter().rev() {
                    sh.uploads.push_front(c);
                }
            }
            Err(CallErr::Server(e)) => {
                tracing::warn!("upload of {} contents failed: {:?} {}", batch.len(), e.code, e.msg);
                for c in batch {
                    sh.uploads.push_back(c);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Upload `batch`: one `Has` for every blob and chunk, `PutBlobs` frames for the missing ones,
/// then `PutManifest` for each chunked content.
async fn upload_batch(sh: &Arc<Shared>, batch: &[ContentId]) -> Result<(), CallErr> {
    // (hash, source file, offset, len) for every blob/chunk, plus the manifests to put afterwards
    let mut pieces: Vec<(Hash, Arc<std::fs::File>, u64, u32)> = vec![];
    let mut manifests: Vec<Manifest> = vec![];
    for c in batch {
        let Some(h) = object_hash(c) else { continue };
        let path = sh.cache.cas_path(&h);
        let Ok(f) = std::fs::File::open(&path) else {
            tracing::error!("content {c:?} lost locally; cannot upload");
            continue;
        };
        sh.cache.touch(&h);
        let f = Arc::new(f);
        match c {
            ContentId::Empty => {}
            ContentId::Blob(h) => {
                let len = f.metadata().map(|m| m.len()).unwrap_or(0) as u32;
                pieces.push((*h, f, 0, len));
            }
            ContentId::Chunked(_) => {
                let p2 = path.clone();
                let chunks = tokio::task::spawn_blocking(move || Cache::chunks_of(&p2)).await.map_err(|_| CallErr::Offline)?.map_err(|_| CallErr::Offline)?;
                for (ch, off, len) in &chunks {
                    pieces.push((*ch, f.clone(), *off, *len));
                }
                manifests.push(Manifest { size: chunks.iter().map(|c| c.2 as u64).sum(), chunks: chunks.iter().map(|c| (c.0, c.2)).collect() });
            }
        }
    }
    upload_pieces(sh, pieces).await?;
    for manifest in manifests {
        sh.link.call(Request::PutManifest { manifest }).await?;
    }
    Ok(())
}

/// Upload blobs/chunks `(hash, source file, offset, len)`: one `Has`, then `PutBlobs` frames for
/// the ones the server lacks.
pub async fn upload_pieces(sh: &Arc<Shared>, mut pieces: Vec<(Hash, Arc<std::fs::File>, u64, u32)>) -> Result<(), CallErr> {
    use std::os::unix::fs::FileExt;
    if !pieces.is_empty() {
        let mut seen: HashSet<Hash> = HashSet::new();
        pieces.retain(|p| seen.insert(p.0));
        let hashes: Vec<Hash> = pieces.iter().map(|p| p.0).collect();
        let mut missing: HashSet<Hash> = HashSet::new();
        for part in hashes.chunks(20_000) {
            match sh.link.call(Request::Has { hashes: part.to_vec() }).await? {
                Response::Missing { hashes } => missing.extend(hashes),
                _ => missing.extend(part.iter().copied()),
            }
        }
        let todo: Vec<_> = pieces.into_iter().filter(|p| missing.contains(&p.0)).collect();
        let mut i = 0;
        while i < todo.len() {
            let mut j = i;
            let mut sz = 0usize;
            while j < todo.len() && (j == i || sz + todo[j].3 as usize <= PUT_FRAME_BYTES) {
                sz += todo[j].3 as usize;
                j += 1;
            }
            let part: Vec<_> = todo[i..j].iter().map(|(h, f, o, l)| (*h, f.clone(), *o, *l as usize)).collect();
            let blobs = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<(Hash, Vec<u8>)>> {
                let mut out = Vec::with_capacity(part.len());
                for (h, f, o, l) in part {
                    let mut b = vec![0u8; l];
                    f.read_exact_at(&mut b, o)?;
                    out.push((h, b));
                }
                Ok(out)
            })
            .await
            .map_err(|_| CallErr::Offline)?
            .map_err(|_| CallErr::Offline)?;
            match blobs.len() {
                1 => {
                    let (hash, data) = blobs.into_iter().next().unwrap();
                    sh.link.call(Request::PutBlob { hash, data }).await?;
                }
                _ => {
                    sh.link.call(Request::PutBlobs { blobs }).await?;
                }
            }
            i = j;
        }
    }
    Ok(())
}
