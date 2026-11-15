//! In-process test server and a small async test client. Public (not
//! `cfg(test)`) so other crates' tests can use it.
//!
//! ```ignore
//! let mut srv = TestServer::start(tmpdir.path());
//! let creds: PathBuf = srv.add_host("a");                // creds dir for tls::client_config
//! srv.add_root("proj", &[("a", true), ("b", false)]);    // (host, rw)
//! let addr = srv.addr();                                 // 127.0.0.1:<ephemeral>
//! srv.kill();  // or srv.restart() / srv.crash_restart() — same data dir, same port
//! ```
//! Everything on `TestServer` is synchronous (the server runs on its own
//! thread and runtime), so it works from sync tests and from `#[tokio::test]`.

use crate::admin::{self, Admin, HostSet};
use crate::server::{Config, Server, Shared};
use crate::store;
use cairn_proto::aio::{read_envelope, write_envelope};
use cairn_proto::*;
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

enum Cmd {
    Stop,
    Kill,
}

struct Running {
    cmd: std::sync::mpsc::Sender<Cmd>,
    join: std::thread::JoinHandle<()>,
    shared: Arc<Shared>,
}

pub struct TestServer {
    dir: PathBuf,
    cfg: Config,
    addr: SocketAddr,
    run: Option<Running>,
    hosts: HashMap<String, (HostId, PathBuf)>,
}

impl TestServer {
    pub fn start(dir: impl AsRef<Path>) -> TestServer {
        Self::start_with(dir, |_| {})
    }

    /// Like `start`, with a chance to tweak the config (lease_break, relay_timeout, retention, ...).
    pub fn start_with(dir: impl AsRef<Path>, tweak: impl FnOnce(&mut Config)) -> TestServer {
        let dir = dir.as_ref().to_path_buf();
        let data = dir.join("data");
        if !data.join("meta.db").exists() {
            admin::init(&data).expect("init data dir");
        }
        let mut cfg = Config::new(&data, "127.0.0.1:0".parse().unwrap());
        cfg.record_commits = true;
        tweak(&mut cfg);
        let mut s = TestServer { dir, cfg, addr: "127.0.0.1:0".parse().unwrap(), run: None, hosts: HashMap::new() };
        s.spawn();
        s
    }

    fn spawn(&mut self) {
        let cfg = Config { listen: self.addr, ..self.cfg.clone() };
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Cmd>();
        let join = std::thread::Builder::new()
            .name("cairn-testserver".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
                let mut tries = 0;
                let srv = loop {
                    match rt.block_on(Server::start(cfg.clone())) {
                        Ok(s) => break s,
                        Err(e) if tries < 50 => {
                            tries += 1;
                            let _ = e;
                            std::thread::sleep(Duration::from_millis(100));
                        }
                        Err(e) => {
                            let _ = ready_tx.send(Err(format!("{e:#}")));
                            return;
                        }
                    }
                };
                let _ = ready_tx.send(Ok((srv.addr(), srv.shared().clone())));
                match cmd_rx.recv() {
                    Ok(Cmd::Stop) => rt.block_on(srv.shutdown()),
                    _ => srv.abort(),
                }
                rt.shutdown_background();
            })
            .unwrap();
        match ready_rx.recv().expect("server thread died") {
            Ok((addr, shared)) => {
                self.addr = addr;
                self.run = Some(Running { cmd: cmd_tx, join, shared });
            }
            Err(e) => panic!("test server failed to start: {e}"),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn data_dir(&self) -> PathBuf {
        self.cfg.data.clone()
    }
    pub fn shared(&self) -> Arc<Shared> {
        self.run.as_ref().expect("server not running").shared.clone()
    }

    fn admin(&self) -> Admin {
        Admin::open(&self.cfg.data).expect("open admin")
    }

    /// Enroll a host; returns its credentials dir (`ca.pem`, `host.pem`, `host.key`).
    pub fn add_host(&mut self, name: &str) -> PathBuf {
        self.enroll_host(name).1
    }

    pub fn enroll_host(&mut self, name: &str) -> (HostId, PathBuf) {
        let out = self.dir.join("creds").join(name);
        let id = self.admin().add_host(name, &out).expect("add host");
        self.hosts.insert(name.to_string(), (id, out.clone()));
        (id, out)
    }

    pub fn host_id(&self, name: &str) -> HostId {
        self.hosts[name].0
    }
    pub fn creds(&self, name: &str) -> PathBuf {
        self.hosts[name].1.clone()
    }

    /// Create a root (or update its ACL): `(host name, read-write)`.
    pub fn add_root(&self, name: &str, acl: &[(&str, bool)]) {
        let acl: Vec<(String, bool)> = acl.iter().map(|(h, rw)| (h.to_string(), *rw)).collect();
        self.admin().add_root(name, &acl).expect("add root");
    }

    pub fn set_host(&self, name: &str, s: HostSet) {
        self.admin().set_host(name, &s).expect("set host");
    }

    /// Delete the host and drop its live connections right away.
    pub fn revoke_host(&mut self, name: &str) {
        if let Some(id) = self.admin().revoke_host(name).expect("revoke") {
            if let Some(r) = &self.run {
                r.shared.kick_host(id);
            }
        }
    }

    /// Graceful stop (drains the writer).
    pub fn stop(mut self) {
        self.stop_inner();
    }
    fn stop_inner(&mut self) {
        if let Some(r) = self.run.take() {
            let _ = r.cmd.send(Cmd::Stop);
            let _ = r.join.join();
        }
    }
    /// Abrupt death: in-flight work is neither committed nor answered; connections drop.
    pub fn kill(mut self) {
        self.kill_inner();
    }
    fn kill_inner(&mut self) {
        if let Some(r) = self.run.take() {
            let _ = r.cmd.send(Cmd::Kill);
            let _ = r.join.join();
        }
    }
    /// Graceful stop and start again on the same data dir and port.
    pub fn restart(&mut self) {
        self.stop_inner();
        self.spawn();
    }
    /// Abrupt death, then start again on the same data dir and port.
    pub fn crash_restart(&mut self) {
        self.kill_inner();
        self.spawn();
    }
    pub fn is_running(&self) -> bool {
        self.run.is_some()
    }

    /// Delete all but the last `keep_last` changes of `root` and raise the log floor
    /// (a Subscribe with an older `since` then gets ResyncRequired). Effective immediately.
    pub fn truncate_log(&self, root: &str, keep_last: u64) {
        let id = self.root_id(root);
        let a = self.admin();
        let head: i64 = a.conn().query_row("SELECT head FROM roots WHERE id=?", [id], |r| r.get(0)).unwrap();
        let cut = (head - keep_last as i64).max(0);
        a.conn().execute("DELETE FROM changes WHERE root=? AND seq<=?", rusqlite::params![id, cut]).unwrap();
        a.conn().execute("UPDATE roots SET log_floor=MAX(log_floor,?) WHERE id=?", rusqlite::params![cut, id]).unwrap();
    }

    /// Simulate a restore from backup: new epoch, visible to clients after the next start/restart.
    pub fn bump_epoch(&self) -> Epoch {
        self.admin().bump_epoch().expect("bump epoch")
    }

    // ---- inspection (reads meta.db directly; works while stopped) ----

    fn read<T>(&self, f: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>) -> T {
        let c = store::open_ro(&self.cfg.data.join("meta.db")).expect("open db");
        c.execute_batch("BEGIN").unwrap();
        let r = f(&c).expect("query");
        let _ = c.execute_batch("COMMIT");
        r
    }
    fn root_id(&self, root: &str) -> i64 {
        self.read(|c| store::root_by_name(c, root)).unwrap_or_else(|| panic!("no root {root}")).id
    }
    pub fn head(&self, root: &str) -> Seq {
        let id = self.root_id(root);
        Seq(self.read(|c| store::root_head_floor(c, id)).0)
    }
    pub fn epoch(&self) -> Epoch {
        self.read(|c| Ok(store::epoch(c))).expect("epoch")
    }
    /// Whole namespace, ordered by (parent, name).
    pub fn snapshot(&self, root: &str) -> Vec<SnapshotRow> {
        let id = self.root_id(root);
        self.read(|c| store::snapshot(c, id, None, usize::MAX >> 1)).1
    }
    /// Full change log from seq 1 (as long as it has not been pruned).
    pub fn change_log(&self, root: &str) -> Vec<Change> {
        let id = self.root_id(root);
        self.read(|c| store::changes_range(c, id, 0, u64::MAX >> 2, usize::MAX >> 1))
    }
    pub fn conflicts(&self, root: &str) -> Vec<ConflictRecord> {
        let id = self.root_id(root);
        self.read(|c| store::conflicts_since(c, id, 0, usize::MAX >> 1))
    }
    /// Every applied commit in server order (excludes duplicate-op_id replays).
    pub fn commit_log(&self, root: &str) -> Vec<(HostId, Commit, CommitResult)> {
        let id = self.root_id(root);
        self.read(|c| {
            let mut st = c.prepare("SELECT host, commit_, result FROM commit_log WHERE root=? ORDER BY id")?;
            let rows = st.query_map([id], |r| {
                Ok((HostId(r.get::<_, i64>(0)? as u64), store::dec::<Commit>(&r.get::<_, Vec<u8>>(1)?)?, store::dec::<CommitResult>(&r.get::<_, Vec<u8>>(2)?)?))
            })?;
            rows.collect()
        })
    }
    /// Content ids referenced by live nodes and tombstones.
    pub fn live_content_ids(&self, root: &str) -> BTreeSet<String> {
        self.live_content(root).into_iter().map(|c| format!("{c:?}")).collect()
    }
    pub fn live_content(&self, root: &str) -> Vec<ContentId> {
        let id = self.root_id(root);
        let mut out: Vec<ContentId> = vec![];
        let blobs: Vec<Vec<u8>> = self.read(|c| {
            let mut v = vec![];
            for t in ["nodes", "tombs"] {
                let mut st = c.prepare(&format!("SELECT attrs FROM {t} WHERE root=?"))?;
                for r in st.query_map([id], |r| r.get::<_, Vec<u8>>(0))? {
                    v.push(r?);
                }
            }
            Ok(v)
        });
        for b in blobs {
            let a: Attrs = frame::decode(&b).unwrap();
            if let Some(c) = a.content {
                if !out.contains(&c) {
                    out.push(c);
                }
            }
        }
        out
    }
    pub fn has_content(&self, c: ContentId) -> bool {
        match c {
            ContentId::Empty => true,
            ContentId::Blob(h) => crate::cas::Cas::new(&self.cfg.data).unwrap().has(&h),
            ContentId::Chunked(h) => self.read(|c| store::has_manifest(c, &h)),
        }
    }
    /// Contents promised by a SetContent but not yet uploaded.
    pub fn pending_content(&self) -> Vec<(ContentId, HostId)> {
        self.read(|c| {
            let mut st = c.prepare("SELECT content, host FROM pending")?;
            let rows = st.query_map([], |r| Ok((store::dec::<ContentId>(&r.get::<_, Vec<u8>>(0)?)?, HostId(r.get::<_, i64>(1)? as u64))))?;
            rows.collect()
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

// ---------------------------------------------------------------- client

/// Process-wide so several clients of the same host never collide.
static NEXT_OP: AtomicU64 = AtomicU64::new(1);
static NEXT_NODE: AtomicU64 = AtomicU64::new(2);

struct Inner {
    out: mpsc::UnboundedSender<Envelope>,
    pending: parking_lot::Mutex<HashMap<u64, oneshot::Sender<Result<Response, ErrorReply>>>>,
    closed: std::sync::atomic::AtomicBool,
}

/// Minimal async protocol client for tests: one TLS connection, request/response
/// matching by id, pushes queued for `next_push`.
pub struct TestClient {
    pub host: HostId,
    pub host_name: String,
    inner: Arc<Inner>,
    next_id: AtomicU64,
    pushes: tokio::sync::Mutex<mpsc::UnboundedReceiver<Push>>,
    pub cred: Cred,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl Drop for TestClient {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort(); // drops both stream halves: the socket closes
        }
    }
}

fn unavailable(m: &str) -> ErrorReply {
    ErrorReply { code: ErrCode::Unavailable, msg: m.into() }
}

impl TestClient {
    /// TLS connect + Hello. An unknown/revoked certificate yields the server's `PermissionDenied`.
    pub async fn connect(addr: SocketAddr, creds: &Path) -> Result<TestClient, ErrorReply> {
        let cfg = tls::client_config(creds).map_err(|e| unavailable(&format!("{e:#}")))?;
        let tcp = tokio::net::TcpStream::connect(addr).await.map_err(|e| unavailable(&e.to_string()))?;
        let mut s = tokio_rustls::TlsConnector::from(cfg).connect(tls::server_name(), tcp).await.map_err(|e| unavailable(&e.to_string()))?;
        write_envelope(&mut s, &Envelope { id: 1, body: Body::Request(Request::Hello { proto: PROTOCOL_VERSION, software: "cairn-testclient".into() }) })
            .await
            .map_err(|e| unavailable(&e.to_string()))?;
        let r = read_envelope(&mut s).await.map_err(|e| unavailable(&e.to_string()))?.ok_or_else(|| unavailable("closed after Hello"))?;
        let (host, host_name) = match r.body {
            Body::Response(Ok(Response::Welcome { host, host_name, .. })) => (host, host_name),
            Body::Response(Err(e)) => return Err(e),
            other => return Err(unavailable(&format!("unexpected {other:?}"))),
        };
        let (mut rd, mut wr) = tokio::io::split(s);
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Envelope>();
        let (push_tx, push_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner { out: out_tx, pending: Default::default(), closed: Default::default() });
        let t1 = tokio::spawn(async move {
            while let Some(env) = out_rx.recv().await {
                if write_envelope(&mut wr, &env).await.is_err() {
                    break;
                }
            }
        });
        let i2 = inner.clone();
        let t2 = tokio::spawn(async move {
            loop {
                match read_envelope(&mut rd).await {
                    Ok(Some(Envelope { id, body: Body::Response(r) })) => {
                        if let Some(tx) = i2.pending.lock().remove(&id) {
                            let _ = tx.send(r);
                        }
                    }
                    Ok(Some(Envelope { body: Body::Push(p), .. })) => {
                        let _ = push_tx.send(p);
                    }
                    _ => break,
                }
            }
            i2.closed.store(true, Ordering::SeqCst);
            i2.pending.lock().clear(); // wakes callers with Unavailable
        });
        Ok(TestClient {
            host,
            host_name,
            inner,
            next_id: AtomicU64::new(2),
            pushes: tokio::sync::Mutex::new(push_rx),
            cred: Cred { uid: 1000, gid: 1000 },
            tasks: vec![t1.abort_handle(), t2.abort_handle()],
        })
    }

    /// Send a request, wait for its response.
    pub async fn call(&self, req: Request) -> Result<Response, ErrorReply> {
        let rx = self.send(req)?;
        rx.await.unwrap_or_else(|_| Err(unavailable("connection closed")))
    }

    /// Send without waiting (pipelining); await the receiver later.
    pub fn send(&self, req: Request) -> Result<oneshot::Receiver<Result<Response, ErrorReply>>, ErrorReply> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().insert(id, tx);
        if self.inner.closed.load(Ordering::SeqCst) {
            self.inner.pending.lock().remove(&id);
            return Err(unavailable("connection closed"));
        }
        self.inner.out.send(Envelope { id, body: Body::Request(req) }).map_err(|_| unavailable("connection closed"))?;
        Ok(rx)
    }

    /// Next server push, or None after `timeout`.
    pub async fn next_push(&self, timeout: Duration) -> Option<Push> {
        tokio::time::timeout(timeout, self.pushes.lock().await.recv()).await.ok().flatten()
    }

    pub async fn mount(&self, root: &str) -> Result<(RootHandle, Seq, bool), ErrorReply> {
        match self.call(Request::Mount { root: RootName(root.into()) }).await? {
            Response::Mounted { root, head, writable, .. } => Ok((root, head, writable)),
            o => Err(unavailable(&format!("unexpected {o:?}"))),
        }
    }

    pub async fn subscribe(&self, root: RootHandle, since: Seq) -> Result<Response, ErrorReply> {
        self.call(Request::Subscribe { root, since }).await
    }

    pub async fn commit(&self, root: RootHandle, commits: Vec<Commit>) -> Result<Vec<CommitResult>, ErrorReply> {
        match self.call(Request::Commit { root, commits }).await? {
            Response::Committed { results } => Ok(results),
            o => Err(unavailable(&format!("unexpected {o:?}"))),
        }
    }

    pub fn commit_of(&self, op: Op) -> Commit {
        Commit { op_id: OpId { host: self.host, n: NEXT_OP.fetch_add(1, Ordering::SeqCst) }, cred: self.cred, op }
    }

    /// One op as one commit with a fresh op id.
    pub async fn op(&self, root: RootHandle, op: Op) -> CommitResult {
        let c = self.commit_of(op);
        self.commit(root, vec![c]).await.expect("commit").remove(0)
    }

    pub fn new_node(&self) -> NodeId {
        NodeId::new(self.host, NEXT_NODE.fetch_add(1, Ordering::SeqCst))
    }

    /// Upload one small blob; returns its ContentId.
    pub async fn put_blob(&self, data: &[u8]) -> Result<ContentId, ErrorReply> {
        let hash = Hash::of(data);
        self.call(Request::PutBlob { hash, data: data.to_vec() }).await?;
        Ok(ContentId::Blob(hash))
    }

    /// Create a file with `content` (uploaded first) under `parent`; returns the new node.
    pub async fn create_file(&self, root: RootHandle, parent: NodeId, name: &str, data: &[u8]) -> Result<(NodeId, CommitResult), ErrorReply> {
        let content = if data.is_empty() { ContentId::Empty } else { self.put_blob(data).await? };
        let node = self.new_node();
        let r = self.op(root, Op::Create { parent, name: Name(name.as_bytes().to_vec()), node, attrs: file_attrs(content, data.len() as u64) }).await;
        Ok((node, r))
    }

    pub async fn readdir_all(&self, root: RootHandle, dir: NodeId) -> Result<Vec<DirEntry>, ErrorReply> {
        let mut all = vec![];
        let mut after = None;
        loop {
            match self.call(Request::ReadDir { root, dir, after: after.clone(), limit: 100 }).await? {
                Response::DirListing { entries, done, .. } => {
                    after = entries.last().map(|e| e.name.clone());
                    all.extend(entries);
                    if done {
                        return Ok(all);
                    }
                }
                o => return Err(unavailable(&format!("unexpected {o:?}"))),
            }
        }
    }
}

pub fn file_attrs(content: ContentId, size: u64) -> Attrs {
    Attrs {
        kind: Kind::File,
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        size,
        mtime_ns: 1,
        ctime_ns: 1,
        atime_ns: 1,
        rdev: 0,
        content: Some(content),
        symlink_target: None,
        xattrs: vec![],
    }
}

pub fn dir_attrs() -> Attrs {
    Attrs { kind: Kind::Dir, mode: 0o755, content: None, size: 0, ..file_attrs(ContentId::Empty, 0) }
}

pub fn name(s: &str) -> Name {
    Name(s.as_bytes().to_vec())
}
