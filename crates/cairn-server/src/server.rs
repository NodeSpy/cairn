//! Connections, sessions and the request handlers.

use crate::cas::Cas;
use crate::hub::Hub;
use crate::leases::{Lease, Leases, Try};
use crate::store::{self, Db, HostRow, Job, Retention, Writer, WriterDeps};
use anyhow::{Context, Result};
use cairn_proto::aio::write_envelope;
use cairn_proto::frame::decode;
use cairn_proto::*;
use parking_lot::{Mutex, RwLock};
use rusqlite::Connection;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;

pub const SOFTWARE: &str = concat!("cairn-server/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug)]
pub struct Config {
    pub data: PathBuf,
    pub listen: SocketAddr,
    pub retention: Retention,
    /// How long a recalled write lease may stay unreleased before it is broken.
    pub lease_break: Duration,
    /// How long a reader waits for the owner to upload pending content.
    pub relay_timeout: Duration,
    /// Record every applied commit in `commit_log` (test oracle).
    pub record_commits: bool,
    pub max_batch: usize,
}

impl Config {
    pub fn new(data: impl Into<PathBuf>, listen: SocketAddr) -> Config {
        Config {
            data: data.into(),
            listen,
            retention: Retention::default(),
            lease_break: Duration::from_secs(30),
            relay_timeout: Duration::from_secs(60),
            record_commits: false,
            max_batch: 256,
        }
    }
    pub fn db_path(&self) -> PathBuf {
        self.data.join("meta.db")
    }
    pub fn pki_dir(&self) -> PathBuf {
        self.data.join("pki")
    }
}

#[derive(Default)]
struct Relay {
    waiters: Mutex<HashMap<Hash, Vec<(HostId, oneshot::Sender<bool>)>>>,
}

impl Relay {
    fn wait(&self, key: Hash, owner: HostId) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().entry(key).or_default().push((owner, tx));
        rx
    }
    fn landed(&self, key: &Hash) {
        if let Some(v) = self.waiters.lock().remove(key) {
            for (_, tx) in v {
                let _ = tx.send(true);
            }
        }
    }
    fn host_gone(&self, host: HostId) {
        let mut g = self.waiters.lock();
        for v in g.values_mut() {
            let (gone, keep): (Vec<_>, Vec<_>) = std::mem::take(v).into_iter().partition(|(h, _)| *h == host);
            *v = keep;
            for (_, tx) in gone {
                let _ = tx.send(false);
            }
        }
        g.retain(|_, v| !v.is_empty());
    }
}

pub struct Shared {
    pub cfg: Config,
    pub db: Db,
    pub writer: Writer,
    pub hub: Arc<Hub>,
    pub leases: Arc<Leases>,
    pub cas: Cas,
    pub epoch: Epoch,
    conns: Mutex<HashMap<u64, Arc<Session>>>,
    next_conn: AtomicU64,
    relay: Relay,
    shutdown: watch::Sender<bool>,
    live: AtomicUsize,
    pub dead: AtomicBool,
}

impl Shared {
    /// Close every live connection of `host` (revocation).
    pub fn kick_host(&self, host: HostId) {
        for s in self.conns.lock().values() {
            if s.host.read().id == host {
                let _ = s.close.send(true);
            }
        }
    }
    pub fn connected_hosts(&self) -> Vec<HostId> {
        self.conns.lock().values().map(|s| s.host.read().id).collect()
    }
}

#[derive(Clone)]
struct MountInfo {
    root_id: i64,
    writable: bool,
}

struct Session {
    sh: Arc<Shared>,
    id: u64,
    host: RwLock<Arc<HostRow>>,
    can_write: bool,
    out: mpsc::Sender<Envelope>,
    close: watch::Sender<bool>,
    mounts: Mutex<Vec<MountInfo>>,
    subs: Mutex<HashMap<u32, AbortHandle>>,
}

pub struct Server {
    shared: Arc<Shared>,
    addr: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl Server {
    pub async fn start(cfg: Config) -> Result<Server> {
        let acceptor = tokio_rustls::TlsAcceptor::from(tls::server_config(&cfg.pki_dir()).context("load server TLS config (run `cairn-server init` first)")?);
        let hub = Arc::new(Hub::default());
        let leases = Arc::new(Leases::default());
        let cas = Cas::new(&cfg.data)?;
        let writer = Writer::start(WriterDeps {
            db_path: cfg.db_path(),
            hub: hub.clone(),
            leases: leases.clone(),
            cas: cas.clone(),
            retention: cfg.retention.clone(),
            record_commits: cfg.record_commits,
            max_batch: cfg.max_batch,
        })?;
        let db = Db::open(&cfg.db_path())?;
        let epoch = db.read(|c| Ok(store::epoch(c)))?.map_err(|e| anyhow::anyhow!("{e}"))?;
        let listener = TcpListener::bind(cfg.listen).await.with_context(|| format!("bind {}", cfg.listen))?;
        let addr = listener.local_addr()?;
        let (shutdown, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            cfg,
            db,
            writer,
            hub,
            leases,
            cas,
            epoch,
            conns: Mutex::new(HashMap::new()),
            next_conn: AtomicU64::new(1),
            relay: Relay::default(),
            shutdown,
            live: AtomicUsize::new(0),
            dead: AtomicBool::new(false),
        });
        let sh = shared.clone();
        let mut stop = shared.shutdown.subscribe();
        let accept = tokio::spawn(async move {
            // periodic retention pass
            let sh2 = sh.clone();
            let mut stop2 = sh.shutdown.subscribe();
            tokio::spawn(async move {
                let mut iv = tokio::time::interval(Duration::from_secs(600));
                iv.tick().await;
                loop {
                    tokio::select! {
                        _ = iv.tick() => { sh2.writer.submit(Job::Prune); }
                        _ = stop2.changed() => return,
                    }
                }
            });
            loop {
                tokio::select! {
                    r = listener.accept() => match r {
                        Ok((tcp, peer)) => {
                            let sh = sh.clone();
                            let acc = acceptor.clone();
                            sh.live.fetch_add(1, Ordering::SeqCst);
                            tokio::spawn(async move {
                                if let Err(e) = serve_conn(sh.clone(), tcp, acc).await {
                                    tracing::debug!("conn {peer}: {e:#}");
                                }
                                sh.live.fetch_sub(1, Ordering::SeqCst);
                            });
                        }
                        Err(e) => { tracing::warn!("accept: {e}"); tokio::time::sleep(Duration::from_millis(50)).await; }
                    },
                    _ = stop.changed() => return,
                }
            }
        });
        Ok(Server { shared, addr, accept })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Graceful: stop accepting, close connections, drain the writer.
    pub async fn shutdown(self) {
        let _ = self.shared.shutdown.send(true);
        let _ = self.accept.await;
        for _ in 0..250 {
            if self.shared.live.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let sh = self.shared.clone();
        let _ = tokio::task::spawn_blocking(move || sh.writer.shutdown()).await;
    }

    /// Abrupt death: nothing queued is committed or answered.
    pub fn abort(self) {
        self.shared.dead.store(true, Ordering::SeqCst);
        self.shared.writer.kill();
        let _ = self.shared.shutdown.send(true);
        self.accept.abort();
    }
}

async fn read_frame<R: AsyncRead + Unpin>(r: &mut R, cap: usize) -> std::io::Result<Option<Envelope>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > cap {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("frame of {len} bytes exceeds the {cap} byte cap")));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    decode(&body).map(Some)
}

/// `GetBlobs` replies stop adding blobs after this many bytes (the rest come back `None`).
const GET_BLOBS_CAP: usize = 4 << 20;

fn er(code: ErrCode, msg: impl Into<String>) -> ErrorReply {
    ErrorReply { code, msg: msg.into() }
}
fn internal(e: impl std::fmt::Display) -> ErrorReply {
    er(ErrCode::Internal, e.to_string())
}

async fn serve_conn(sh: Arc<Shared>, tcp: TcpStream, acceptor: tokio_rustls::TlsAcceptor) -> Result<()> {
    tcp.set_nodelay(true).ok();
    let tls_stream = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await.context("tls handshake timeout")??;
    let fp = tls::peer_fingerprint(tls_stream.get_ref().1)?;
    let (mut rd, mut wr) = tokio::io::split(tls_stream);

    // ---- Hello ----
    let hello = match tokio::time::timeout(Duration::from_secs(10), read_frame(&mut rd, 64 * 1024)).await.context("hello timeout")?? {
        Some(e) => e,
        None => return Ok(()),
    };
    let reply = |id: u64, r: Result<Response, ErrorReply>| Envelope { id, body: Body::Response(r) };
    let (proto, _software) = match hello.body {
        Body::Request(Request::Hello { proto, software }) => (proto, software),
        _ => {
            write_envelope(&mut wr, &reply(hello.id, Err(er(ErrCode::Invalid, "expected Hello")))).await.ok();
            return Ok(());
        }
    };
    let fp2 = fp;
    let host = sh.db.read(move |c| store::host_by_fp(c, &fp2))?;
    let host = match host {
        Some(h) => h,
        None => {
            write_envelope(&mut wr, &reply(hello.id, Err(er(ErrCode::PermissionDenied, "unknown host certificate")))).await.ok();
            let _ = wr.shutdown().await;
            return Ok(());
        }
    };
    if proto != PROTOCOL_VERSION {
        write_envelope(&mut wr, &reply(hello.id, Err(er(ErrCode::Invalid, format!("protocol version {proto} not supported (server speaks {PROTOCOL_VERSION})"))))).await.ok();
        let _ = wr.shutdown().await;
        return Ok(());
    }
    let h2 = host.clone();
    let can_write = sh.db.read(move |c| store::host_can_write_any(c, &h2))?;
    write_envelope(&mut wr, &reply(hello.id, Ok(Response::Welcome { host: host.id, host_name: host.name.clone(), server_software: SOFTWARE.into() }))).await?;

    let (out_tx, mut out_rx) = mpsc::channel::<Envelope>(1024);
    let (close_tx, mut close_rx) = watch::channel(false);
    let sess = Arc::new(Session {
        sh: sh.clone(),
        id: sh.next_conn.fetch_add(1, Ordering::SeqCst),
        host: RwLock::new(Arc::new(host.clone())),
        can_write,
        out: out_tx,
        close: close_tx,
        mounts: Mutex::new(vec![]),
        subs: Mutex::new(HashMap::new()),
    });
    sh.conns.lock().insert(sess.id, sess.clone());

    // writer task
    let mut wclose = sess.close.subscribe();
    let wtask = tokio::spawn(async move {
        loop {
            tokio::select! {
                m = out_rx.recv() => match m {
                    Some(env) => if write_envelope(&mut wr, &env).await.is_err() { break },
                    None => break,
                },
                _ = wclose.changed() => break,
            }
        }
        let _ = wr.shutdown().await;
    });

    let mut stop = sh.shutdown.subscribe();
    let mut recheck = tokio::time::interval(Duration::from_secs(10));
    recheck.tick().await;
    let result: Result<()> = async {
        loop {
            let cap = sess.host.read().max_frame;
            tokio::select! {
                r = read_frame(&mut rd, cap) => {
                    let env = match r? { Some(e) => e, None => return Ok(()) };
                    if env.id == 0 { anyhow::bail!("request id 0"); }
                    let req = match env.body { Body::Request(r) => r, _ => anyhow::bail!("client sent a non-request") };
                    sess.submit(env.id, req);
                }
                _ = close_rx.changed() => return Ok(()),
                _ = stop.changed() => return Ok(()),
                _ = recheck.tick() => {
                    let id = sess.host.read().id;
                    match sh.db.read(move |c| store::host_by_id(c, id)) {
                        Ok(Some(h)) => *sess.host.write() = Arc::new(h),
                        Ok(None) => return Ok(()), // revoked
                        Err(_) => {}
                    }
                }
            }
        }
    }
    .await;

    // ---- cleanup ----
    let _ = sess.close.send(true);
    for (_, a) in sess.subs.lock().drain() {
        a.abort();
    }
    let host_id = host.id;
    let last = {
        let mut g = sh.conns.lock();
        g.remove(&sess.id);
        !g.values().any(|s| s.host.read().id == host_id)
    };
    if last {
        sh.leases.drop_host(host_id);
        sh.relay.host_gone(host_id);
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), wtask).await;
    result
}

impl Session {
    fn host(&self) -> Arc<HostRow> {
        self.host.read().clone()
    }

    fn mount(&self, h: RootHandle) -> Result<MountInfo, ErrorReply> {
        let g = self.mounts.lock();
        h.checked_sub(1).and_then(|i| g.get(i as usize)).cloned().ok_or_else(|| er(ErrCode::Invalid, "bad root handle"))
    }

    async fn push(&self, p: Push) {
        let _ = self.out.send(Envelope { id: 0, body: Body::Push(p) }).await;
    }

    async fn db<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static) -> Result<T, ErrorReply> {
        let sh = self.sh.clone();
        tokio::task::spawn_blocking(move || sh.db.read(f)).await.map_err(internal)?.map_err(internal)
    }

    /// Called from the read loop, in arrival order. Commits are enqueued
    /// to the writer right here so pipelined commits apply in order.
    fn submit(self: &Arc<Self>, id: u64, req: Request) {
        let me = self.clone();
        if let Request::Commit { root, commits } = req {
            let pre = (|| {
                let m = self.mount(root)?;
                if !m.writable {
                    return Err(er(ErrCode::ReadOnly, "root is mounted read-only"));
                }
                let host = self.host();
                if commits.len() > host.max_commits {
                    return Err(er(ErrCode::TooLarge, format!("too many commits in one request (max {})", host.max_commits)));
                }
                Ok((m, host))
            })();
            match pre {
                Err(e) => {
                    tokio::spawn(async move { me.respond(id, Err(e)).await });
                }
                Ok((m, host)) => {
                    let (tx, rx) = oneshot::channel();
                    let ok = self.sh.writer.submit(Job::Commit(store::CommitJob { root: m.root_id, host: (*host).clone(), commits, reply: tx }));
                    tokio::spawn(async move {
                        let r = if !ok {
                            Err(er(ErrCode::Unavailable, "server shutting down"))
                        } else {
                            match rx.await {
                                Ok(Ok(results)) => Ok(Response::Committed { results }),
                                Ok(Err(e)) => Err(e),
                                Err(_) => Err(er(ErrCode::Unavailable, "server shutting down")),
                            }
                        };
                        me.respond(id, r).await;
                    });
                }
            }
            return;
        }
        if let Request::Subscribe { root, since } = req {
            tokio::spawn(async move { me.do_subscribe(id, root, since).await });
            return;
        }
        let mut closed = self.close.subscribe();
        tokio::spawn(async move {
            let r = tokio::select! {
                r = me.dispatch(req) => r,
                _ = closed.changed() => return,
            };
            me.respond(id, r).await;
        });
    }

    async fn respond(&self, id: u64, r: Result<Response, ErrorReply>) {
        let _ = self.out.send(Envelope { id, body: Body::Response(r) }).await;
    }

    async fn dispatch(self: &Arc<Self>, req: Request) -> Result<Response, ErrorReply> {
        match req {
            Request::Hello { .. } => Err(er(ErrCode::Invalid, "duplicate Hello")),
            Request::Ping => Ok(Response::Ok),
            Request::Mount { root } => self.do_mount(root).await,
            Request::Subscribe { .. } => unreachable!("handled in submit"),
            Request::ReadDir { root, dir, after, limit } => {
                let m = self.mount(root)?;
                let limit = (limit as usize).clamp(1, 4096);
                let (version, entries, done) = self
                    .db(move |c| {
                        let (v, e, d) = store::read_dir(c, m.root_id, dir, after.as_ref(), limit)?;
                        // a missing directory is NotFound, not an empty listing
                        let d_res = store::resolve_alias(c, m.root_id, dir)?;
                        let rec = store::node_rec(c, m.root_id, d_res)?;
                        Ok((rec, v, e, d))
                    })
                    .await
                    .and_then(|(rec, v, e, d)| match rec {
                        None => Err(er(ErrCode::NotFound, "no such directory")),
                        Some(r) if r.attrs.kind != Kind::Dir => Err(er(ErrCode::NotDir, "not a directory")),
                        Some(_) => Ok((v, e, d)),
                    })?;
                Ok(Response::DirListing { version, entries, done })
            }
            Request::GetAttr { root, node } => {
                let m = self.mount(root)?;
                let r = self
                    .db(move |c| {
                        let n = store::resolve_alias(c, m.root_id, node)?;
                        store::node_rec(c, m.root_id, n)
                    })
                    .await?;
                match r {
                    Some(r) => Ok(Response::NodeAttrs { attrs: r.attrs, version: r.version }),
                    None => Err(er(ErrCode::NotFound, "no such node")),
                }
            }
            Request::Snapshot { root, after, limit } => {
                let m = self.mount(root)?;
                let limit = (limit as usize).clamp(1, 4096);
                let (head, rows, done) = self.db(move |c| store::snapshot(c, m.root_id, after.as_ref(), limit)).await?;
                Ok(Response::Snapshot { head, rows, done })
            }
            Request::Commit { .. } => unreachable!("handled in submit"),
            Request::Has { hashes } => {
                let sh = self.sh.clone();
                let (missing, stale) = tokio::task::spawn_blocking(move || -> Result<(Vec<Hash>, Vec<ContentId>), ErrorReply> {
                    let mut miss = vec![];
                    let mut stale = vec![];
                    for h in hashes {
                        let (blob, manifest) = if sh.cas.has(&h) { (true, false) } else { (false, sh.db.read(|c| store::has_manifest(c, &h)).map_err(internal)?) };
                        if !blob && !manifest {
                            miss.push(h);
                            continue;
                        }
                        // The content is here, so nothing may still wait for it: an upload cut
                        // short between storing the bytes and clearing the pending row (client
                        // killed mid-request) would otherwise never be cleared.
                        let id = if blob { ContentId::Blob(h) } else { ContentId::Chunked(h) };
                        if !sh.db.read(|c| store::pending_owners(c, &id)).map_err(internal)?.is_empty() {
                            stale.push(id);
                        }
                    }
                    Ok((miss, stale))
                })
                .await
                .map_err(internal)??;
                if !stale.is_empty() {
                    let (tx, rx) = oneshot::channel();
                    if self.sh.writer.submit(Job::Landed { keys: stale, manifest: None, done: tx }) {
                        let _ = rx.await;
                    }
                }
                Ok(Response::Missing { hashes: missing })
            }
            Request::PutBlob { hash, data } => {
                self.need_write()?;
                if data.len() > CDC_MAX as usize {
                    return Err(er(ErrCode::TooLarge, format!("blob of {} bytes exceeds {CDC_MAX}", data.len())));
                }
                let sh = self.sh.clone();
                tokio::task::spawn_blocking(move || -> Result<(), ErrorReply> {
                    if Hash::of(&data) != hash {
                        return Err(er(ErrCode::Invalid, "blob hash mismatch"));
                    }
                    sh.cas.put(&hash, &data).map_err(internal)
                })
                .await
                .map_err(internal)??;
                self.landed(vec![ContentId::Blob(hash)], None, hash).await?;
                Ok(Response::Ok)
            }
            Request::PutBlobs { blobs } => {
                self.need_write()?;
                if blobs.iter().any(|(_, d)| d.len() > CDC_MAX as usize) {
                    return Err(er(ErrCode::TooLarge, format!("blob exceeds {CDC_MAX}")));
                }
                let keys: Vec<Hash> = blobs.iter().map(|(h, _)| *h).collect();
                let sh = self.sh.clone();
                tokio::task::spawn_blocking(move || -> Result<(), ErrorReply> {
                    // verify everything before writing anything
                    if blobs.iter().any(|(h, d)| Hash::of(d) != *h) {
                        return Err(er(ErrCode::Invalid, "blob hash mismatch"));
                    }
                    sh.cas.put_many(&blobs).map_err(internal)
                })
                .await
                .map_err(internal)??;
                let (tx, rx) = oneshot::channel();
                if !self.sh.writer.submit(Job::Landed { keys: keys.iter().map(|h| ContentId::Blob(*h)).collect(), manifest: None, done: tx }) {
                    return Err(er(ErrCode::Unavailable, "server shutting down"));
                }
                rx.await.map_err(|_| er(ErrCode::Unavailable, "server shutting down"))?;
                for h in &keys {
                    self.sh.relay.landed(h);
                }
                Ok(Response::Ok)
            }
            Request::GetBlobs { hashes } => {
                let sh = self.sh.clone();
                let blobs = tokio::task::spawn_blocking(move || -> Result<Vec<Option<Vec<u8>>>, ErrorReply> {
                    let mut out = Vec::with_capacity(hashes.len());
                    let mut total = 0usize;
                    for h in hashes {
                        if total >= GET_BLOBS_CAP {
                            out.push(None);
                            continue;
                        }
                        let d = sh.cas.get(&h).map_err(internal)?;
                        total += d.as_ref().map(|d| d.len()).unwrap_or(0);
                        out.push(d);
                    }
                    Ok(out)
                })
                .await
                .map_err(internal)??;
                Ok(Response::Blobs { blobs })
            }
            Request::PutManifest { manifest } => {
                self.need_write()?;
                let id = manifest.id();
                let ContentId::Chunked(idh) = id else { unreachable!() };
                let sum: u64 = manifest.chunks.iter().map(|(_, l)| *l as u64).sum();
                if sum != manifest.size || manifest.chunks.iter().any(|(_, l)| *l > CDC_MAX) {
                    return Err(er(ErrCode::Invalid, "manifest sizes inconsistent"));
                }
                let sh = self.sh.clone();
                let chunks: Vec<Hash> = manifest.chunks.iter().map(|(h, _)| *h).collect();
                tokio::task::spawn_blocking(move || -> Result<(), ErrorReply> {
                    for h in chunks {
                        if !sh.cas.has(&h) {
                            return Err(er(ErrCode::NotFound, format!("chunk {h:?} missing")));
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(internal)??;
                let data = frame::encode(&manifest).map_err(internal)?;
                self.landed(vec![id], Some((idh, data)), idh).await?;
                Ok(Response::Ok)
            }
            Request::GetBlob { hash } => {
                let sh = self.sh.clone();
                let get = move || {
                    let sh = sh.clone();
                    async move { tokio::task::spawn_blocking(move || sh.cas.get(&hash)).await.map_err(internal)?.map_err(internal) }
                };
                if let Some(d) = get().await? {
                    return Ok(Response::Blob { data: d });
                }
                self.relay(ContentId::Blob(hash), hash).await?;
                match get().await? {
                    Some(d) => Ok(Response::Blob { data: d }),
                    None => Err(er(ErrCode::NotFound, "no such blob")),
                }
            }
            Request::GetManifest { id } => {
                if let Some(m) = self.db(move |c| store::get_manifest(c, &id)).await? {
                    return Ok(Response::Manifest { manifest: m });
                }
                self.relay(ContentId::Chunked(id), id).await?;
                match self.db(move |c| store::get_manifest(c, &id)).await? {
                    Some(m) => Ok(Response::Manifest { manifest: m }),
                    None => Err(er(ErrCode::NotFound, "no such manifest")),
                }
            }
            Request::AcquireWrite { root, node } => self.acquire(root, node).await,
            Request::ReleaseWrite { root, node } => {
                let m = self.mount(root)?;
                let node = self.db(move |c| store::resolve_alias(c, m.root_id, node)).await?;
                self.sh.leases.release(m.root_id, node, self.host().id);
                Ok(Response::Ok)
            }
            Request::Conflicts { root, since } => {
                let m = self.mount(root)?;
                let conflicts = self.db(move |c| store::conflicts_since(c, m.root_id, since.0, 10_000)).await?;
                Ok(Response::ConflictList { conflicts })
            }
        }
    }

    fn need_write(&self) -> Result<(), ErrorReply> {
        if self.can_write {
            Ok(())
        } else {
            Err(er(ErrCode::ReadOnly, "host has no writable root"))
        }
    }

    async fn landed(&self, keys: Vec<ContentId>, manifest: Option<(Hash, Vec<u8>)>, relay_key: Hash) -> Result<(), ErrorReply> {
        let (tx, rx) = oneshot::channel();
        if !self.sh.writer.submit(Job::Landed { keys, manifest, done: tx }) {
            return Err(er(ErrCode::Unavailable, "server shutting down"));
        }
        rx.await.map_err(|_| er(ErrCode::Unavailable, "server shutting down"))?;
        self.sh.relay.landed(&relay_key);
        Ok(())
    }

    /// Wait until `content` (pending from some owner) has been uploaded.
    async fn relay(&self, content: ContentId, key: Hash) -> Result<(), ErrorReply> {
        let owners = self.db(move |c| store::pending_owners(c, &content)).await?;
        if owners.is_empty() {
            return Err(er(ErrCode::NotFound, "no such content"));
        }
        // choose a connected owner, preferring its subscribed (control) connection
        let target = {
            let g = self.sh.conns.lock();
            let mut cands: Vec<&Arc<Session>> = g.values().filter(|s| owners.contains(&s.host.read().id)).collect();
            cands.sort_by_key(|s| (s.subs.lock().is_empty(), s.id));
            cands.first().map(|s| (*s).clone())
        };
        let target = target.ok_or_else(|| er(ErrCode::Unavailable, "content owner is offline"))?;
        let owner = target.host.read().id;
        let rx = self.sh.relay.wait(key, owner);
        // it may have landed between our first check and registering
        let present = match content {
            ContentId::Blob(h) => self.sh.cas.has(&h),
            ContentId::Chunked(h) => self.db(move |c| store::has_manifest(c, &h)).await?,
            ContentId::Empty => true,
        };
        if present {
            return Ok(());
        }
        target.push(Push::NeedContent { hash: key }).await;
        match tokio::time::timeout(self.sh.cfg.relay_timeout, rx).await {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) | Ok(Err(_)) => Err(er(ErrCode::Unavailable, "content owner went offline")),
            Err(_) => {
                let (tx, rx) = oneshot::channel();
                self.sh.writer.submit(Job::Lost { key: content, host: owner, done: tx });
                let _ = rx.await;
                Err(er(ErrCode::Unavailable, "content owner did not upload in time"))
            }
        }
    }

    async fn do_mount(&self, root: RootName) -> Result<Response, ErrorReply> {
        let host = self.host();
        let name = root.0.clone();
        let row = self.db(move |c| store::root_by_name(c, &name)).await?.ok_or_else(|| er(ErrCode::NotFound, "no such root"))?;
        let writable = if let Some(rest) = row.name.strip_prefix("hosts/") {
            // per-host namespace: only that host, always writable
            if rest.split('/').next() != Some(host.name.as_str()) {
                return Err(er(ErrCode::PermissionDenied, "per-host root belongs to another host"));
            }
            true
        } else {
            let (rid, hid) = (row.id, host.id);
            match self.db(move |c| store::acl_for(c, rid, hid)).await? {
                Some(rw) => rw,
                None => return Err(er(ErrCode::PermissionDenied, "host may not mount this root")),
            }
        };
        let handle = {
            let mut g = self.mounts.lock();
            match g.iter().position(|m| m.root_id == row.id) {
                Some(i) => {
                    g[i].writable = writable;
                    i as u32 + 1
                }
                None => {
                    g.push(MountInfo { root_id: row.id, writable });
                    g.len() as u32
                }
            }
        };
        let rid = row.id;
        let (head, _) = self.db(move |c| store::root_head_floor(c, rid)).await?;
        Ok(Response::Mounted { root: handle, epoch: self.sh.epoch, head: Seq(head), writable })
    }

    /// Sends its own response (so it precedes the first push), then streams.
    async fn do_subscribe(self: &Arc<Self>, id: u64, handle: RootHandle, since: Seq) {
        let m = match self.mount(handle) {
            Ok(m) => m,
            Err(e) => return self.respond(id, Err(e)).await,
        };
        let rid = m.root_id;
        let (head, floor) = match self.db(move |c| store::root_head_floor(c, rid)).await {
            Ok(x) => x,
            Err(e) => return self.respond(id, Err(e)).await,
        };
        if since.0 < floor || since.0 > head {
            return self.respond(id, Ok(Response::ResyncRequired { head: Seq(head) })).await;
        }
        let (from, sub_id, mut rx) = self.sh.hub.subscribe(rid, head);
        struct Guard(Arc<Hub>, i64, u64);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.unsubscribe(self.1, self.2);
            }
        }
        let guard = Guard(self.sh.hub.clone(), rid, sub_id);
        let me = self.clone();
        self.respond(id, Ok(Response::Subscribed { head: Seq(from) })).await;
        let task = tokio::spawn(async move {
            let _g = guard;
            let mut cursor = since.0;
            // backlog from the log, in pages
            while cursor < from {
                let c0 = cursor;
                let page = me.db(move |c| store::changes_range(c, rid, c0, from, 500)).await;
                match page {
                    Ok(p) if !p.is_empty() => {
                        cursor = p.last().unwrap().seq.0;
                        me.push(Push::Changes { root: handle, changes: p }).await;
                    }
                    _ => {
                        me.push(Push::ResyncRequired { root: handle }).await;
                        return;
                    }
                }
            }
            while let Some(mut batch) = rx.recv().await {
                batch.retain(|c| c.seq.0 > cursor);
                if let Some(l) = batch.last() {
                    cursor = l.seq.0;
                    me.push(Push::Changes { root: handle, changes: batch }).await;
                }
            }
            // the hub dropped us (slow consumer): resync
            me.push(Push::ResyncRequired { root: handle }).await;
        });
        if let Some(old) = self.subs.lock().insert(handle, task.abort_handle()) {
            old.abort();
        }
    }

    async fn acquire(&self, handle: RootHandle, node: NodeId) -> Result<Response, ErrorReply> {
        let m = self.mount(handle)?;
        if !m.writable {
            return Err(er(ErrCode::ReadOnly, "root is mounted read-only"));
        }
        let rid = m.root_id;
        let node = self.db(move |c| store::resolve_alias(c, rid, node)).await?;
        let rec = self.db(move |c| store::node_rec(c, rid, node)).await?.ok_or_else(|| er(ErrCode::NotFound, "no such node"))?;
        if rec.attrs.kind == Kind::Dir {
            return Err(er(ErrCode::IsDir, "is a directory"));
        }
        let me = Lease { host: self.host().id, conn: self.id, handle };
        let deadline = tokio::time::Instant::now() + self.sh.cfg.lease_break;
        let mut recalled = false;
        loop {
            let notified = self.sh.leases.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.sh.leases.try_acquire(rid, node, me) {
                Try::Granted => break,
                Try::HeldBy(l) => {
                    if !recalled {
                        recalled = true;
                        self.recall(l, rid, node).await;
                    }
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = tokio::time::sleep_until(deadline) => {
                            tracing::info!("breaking write lease on {node:?} held by {:?}", l.host);
                            self.sh.leases.force(rid, node, me);
                            break;
                        }
                    }
                }
            }
        }
        let rec = self.db(move |c| store::node_rec(c, rid, node)).await?;
        match rec {
            Some(r) => Ok(Response::Granted { version: r.cver, content: r.attrs.content }),
            None => {
                self.sh.leases.release(rid, node, me.host);
                Err(er(ErrCode::NotFound, "node deleted"))
            }
        }
    }

    async fn recall(&self, l: Lease, root: i64, node: NodeId) {
        let target = {
            let g = self.sh.conns.lock();
            let pick = |s: &Arc<Session>| s.mounts.lock().iter().position(|m| m.root_id == root).map(|i| (s.clone(), i as u32 + 1));
            g.get(&l.conn).and_then(pick).or_else(|| g.values().filter(|s| s.host.read().id == l.host).find_map(pick))
        };
        if let Some((s, h)) = target {
            s.push(Push::Recall { root: h, node }).await;
        }
    }
}
