//! The client daemon. It manages a plain local directory (ext4/xfs), and the
//! kernel serves every read and write there directly. The daemon only steps in
//! at these points:
//!
//! * **Hydration**: files whose content isn't local are sparse placeholders
//!   with a FAN_PRE_ACCESS inode mark. The first read or write blocks in the
//!   kernel while we fetch the content and write it through the
//!   (FMODE_NONOTIFY) event fd. That write is invisible to inotify.
//! * **Write gating**: every filled file we don't hold the server write lease
//!   for carries a kernel read lease (F_SETLEASE F_RDLCK) held by us. A local
//!   open-for-write breaks it, which blocks the opener while we get the server
//!   write lease. On placeholders, the hydration handler gates instead: after
//!   filling, a failed F_RDLCK means a writer has the file open.
//! * **Write-back**: FAN_CLOSE_WRITE (FID group) marks a file dirty, and the
//!   uploader pushes it to the server after `flush_delay`.
//! * **Eviction**: clean, cold files are punched back to placeholders when
//!   the cache exceeds `cache_max`. A kernel F_WRLCK lease proves nobody has
//!   the file open before we punch it.
//! * **Remote changes**: applied to the local directory with ordinary
//!   syscalls, so local inotify watchers see real events.

mod conn;

use crate::log;
use crate::proto::*;
use crate::sys::{self, Handle, Info};
use anyhow::{anyhow, bail, Context, Result};
use conn::Conn;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct Opts {
    pub dir: PathBuf,
    pub server: String,
    pub name: String,
    /// Max bytes of clean, filled content kept locally.
    pub cache_max: u64,
    /// How long a file must be quiet after close-for-write before upload.
    pub flush_delay: Duration,
}

const DIR_MASK: u64 = sys::FAN_CREATE
    | sys::FAN_DELETE
    | sys::FAN_RENAME
    | sys::FAN_CLOSE_WRITE
    | sys::FAN_OPEN
    | sys::FAN_EVENT_ON_CHILD
    | sys::FAN_ONDIR;

struct Node {
    kind: Kind,
    version: u64,
    size: u64,
    mtime_ns: i64,
    filled: bool,
    /// Bumped on every local close-for-write; equal to `flushed_gen` when clean.
    dirty_gen: u64,
    flushed_gen: u64,
    dirty_since: Instant,
    /// We hold the server write lease.
    wlease: bool,
    /// Kernel read lease fd (gates local open-for-write).
    klease: Option<OwnedFd>,
    /// FAN_PRE_ACCESS mark present on the inode.
    marked: bool,
    last_access: Instant,
}

impl Node {
    fn new(kind: Kind) -> Node {
        let now = Instant::now();
        Node {
            kind,
            version: 0,
            size: 0,
            mtime_ns: 0,
            filled: true,
            dirty_gen: 0,
            flushed_gen: 0,
            dirty_since: now,
            wlease: false,
            klease: None,
            marked: false,
            last_access: now,
        }
    }
    fn dirty(&self) -> bool {
        self.dirty_gen != self.flushed_gen
    }
}

type SelfOp = Box<dyn FnOnce(BorrowedFd) -> io::Result<()> + Send>;

pub struct Client {
    root: PathBuf,
    opts: Opts,
    conn: Arc<Conn>,
    cid: u64,
    pid: i32,
    /// Pre-content group (fd mode, event fds opened O_RDWR).
    g1: OwnedFd,
    /// Namespace/notification group (FID mode, no event fds).
    g2: OwnedFd,
    nodes: Mutex<HashMap<String, Node>>,
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    lease_fds: Mutex<HashMap<RawFd, String>>,
    dirs: Mutex<HashMap<Handle, String>>,
    /// Work to run on our *own* pre-content events, keyed by (dev, ino):
    /// gives us a FMODE_NONOTIFY writable fd (used for eviction).
    self_ops: Mutex<HashMap<(u64, u64), SelfOp>>,
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

fn under(k: &str, p: &str) -> bool {
    k == p || (k.len() > p.len() && k.starts_with(p) && k.as_bytes()[p.len()] == b'/')
}

fn rekey<V>(m: &mut HashMap<String, V>, from: &str, to: &str) {
    let keys: Vec<String> = m.keys().filter(|k| under(k, from)).cloned().collect();
    for k in keys {
        let v = m.remove(&k).unwrap();
        m.insert(format!("{to}{}", &k[from.len()..]), v);
    }
}

fn mtime_of(md: &fs::Metadata) -> i64 {
    md.mtime() * 1_000_000_000 + md.mtime_nsec()
}

fn is_eagain(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EBUSY))
}

pub fn run(opts: Opts) -> Result<()> {
    // Must happen before any thread is spawned so every thread inherits the mask.
    sys::block_signal(sys::lease_signal())?;
    sys::raise_nofile();

    let root = fs::canonicalize(&opts.dir).with_context(|| format!("{}", opts.dir.display()))?;
    let g1 = sys::fanotify_init(
        sys::FAN_CLOEXEC | sys::FAN_CLASS_PRE_CONTENT | sys::FAN_UNLIMITED_QUEUE | sys::FAN_UNLIMITED_MARKS,
        (libc::O_RDWR | libc::O_LARGEFILE | libc::O_CLOEXEC) as u32,
    )
    .context("fanotify_init(pre-content) — needs CAP_SYS_ADMIN")?;
    let g2 = sys::fanotify_init(
        sys::FAN_CLOEXEC
            | sys::FAN_CLASS_NOTIF
            | sys::FAN_UNLIMITED_QUEUE
            | sys::FAN_UNLIMITED_MARKS
            | sys::FAN_REPORT_DIR_FID
            | sys::FAN_REPORT_NAME,
        libc::O_RDONLY as u32,
    )
    .context("fanotify_init(fid)")?;
    let sfd = sys::signalfd(sys::lease_signal())?;

    let (push_tx, push_rx) = channel();
    let (conn, cid) = Conn::connect(&opts.server, &opts.name, push_tx)?;
    log!("client", "connected to {} as client {cid}; root={}", opts.server, root.display());

    let c = Arc::new(Client {
        root,
        opts,
        conn,
        cid,
        pid: std::process::id() as i32,
        g1,
        g2,
        nodes: Mutex::new(HashMap::new()),
        locks: Mutex::new(HashMap::new()),
        lease_fds: Mutex::new(HashMap::new()),
        dirs: Mutex::new(HashMap::new()),
        self_ops: Mutex::new(HashMap::new()),
    });

    // The pre-content loop must be running before we touch any marked file
    // ourselves (our own accesses generate events we must answer).
    spawn("g1", { let c = c.clone(); move || c.g1_loop() });
    c.materialize()?;
    spawn("g2", { let c = c.clone(); move || c.g2_loop() });
    spawn("leases", { let c = c.clone(); move || c.lease_loop(sfd) });
    spawn("push", { let c = c.clone(); move || c.push_loop(push_rx) });
    spawn("uploader", { let c = c.clone(); move || c.uploader_loop() });
    spawn("evictor", { let c = c.clone(); move || c.evictor_loop() });
    log!("client", "ready");
    loop {
        std::thread::park();
    }
}

fn spawn(name: &str, f: impl FnOnce() + Send + 'static) {
    let n = name.to_string();
    std::thread::Builder::new()
        .name(n.clone())
        .spawn(move || {
            f();
            log!("client", "thread {n} exited");
        })
        .expect("spawn");
}

impl Client {
    // ---------------------------------------------------------------- helpers

    fn abs(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    fn rel_of(&self, p: &Path) -> Option<String> {
        let r = p.strip_prefix(&self.root).ok()?;
        Some(r.to_str()?.to_string())
    }

    fn plock(&self, rel: &str) -> Arc<Mutex<()>> {
        self.locks.lock().unwrap().entry(rel.to_string()).or_default().clone()
    }

    fn with_node<R>(&self, rel: &str, f: impl FnOnce(&mut Node) -> R) -> Option<R> {
        self.nodes.lock().unwrap().get_mut(rel).map(f)
    }

    fn mark(&self, rel: &str) -> io::Result<()> {
        sys::fanotify_mark_path(
            self.g1.as_fd(),
            sys::FAN_MARK_ADD | sys::FAN_MARK_DONT_FOLLOW,
            sys::FAN_PRE_ACCESS,
            &self.abs(rel),
        )?;
        self.with_node(rel, |n| n.marked = true);
        Ok(())
    }

    fn unmark(&self, rel: &str) {
        if self.with_node(rel, |n| std::mem::replace(&mut n.marked, false)) == Some(true) {
            let _ = sys::fanotify_mark_path(
                self.g1.as_fd(),
                sys::FAN_MARK_REMOVE | sys::FAN_MARK_DONT_FOLLOW,
                sys::FAN_PRE_ACCESS,
                &self.abs(rel),
            );
        }
    }

    fn mark_dir(&self, rel: &str) -> io::Result<()> {
        let p = self.abs(rel);
        sys::fanotify_mark_path(self.g2.as_fd(), sys::FAN_MARK_ADD | sys::FAN_MARK_DONT_FOLLOW, DIR_MASK, &p)?;
        let h = sys::path_handle(&p)?;
        self.dirs.lock().unwrap().insert(h, rel.to_string());
        Ok(())
    }

    /// Take a kernel read lease on `rel`. Ok(false) = someone has it open for write.
    fn take_klease(&self, rel: &str) -> io::Result<bool> {
        if self.with_node(rel, |n| n.klease.is_some()) != Some(false) {
            return Ok(true); // already held, or unknown node
        }
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.abs(rel))?;
        let fd: OwnedFd = f.into();
        sys::set_sig(fd.as_fd(), sys::lease_signal())?;
        let raw = fd.as_raw_fd();
        // Register before the lease exists so a break can always be resolved.
        self.lease_fds.lock().unwrap().insert(raw, rel.to_string());
        if let Err(e) = sys::set_lease(fd.as_fd(), sys::LEASE_RD) {
            self.lease_fds.lock().unwrap().remove(&raw);
            return if is_eagain(&e) { Ok(false) } else { Err(e) };
        }
        let mut fd = Some(fd);
        self.with_node(rel, |n| n.klease = fd.take());
        if let Some(fd) = fd {
            // Node vanished meanwhile.
            self.lease_fds.lock().unwrap().remove(&raw);
            let _ = sys::set_lease(fd.as_fd(), sys::LEASE_UN);
        }
        Ok(true)
    }

    fn drop_klease(&self, rel: &str) {
        if let Some(Some(fd)) = self.with_node(rel, |n| n.klease.take()) {
            self.release_fd(fd);
        }
    }

    fn release_fd(&self, fd: OwnedFd) {
        let _ = sys::set_lease(fd.as_fd(), sys::LEASE_UN);
        self.lease_fds.lock().unwrap().remove(&fd.as_raw_fd());
        drop(fd);
    }

    fn fetch(&self, rel: &str) -> Result<(Meta, Vec<u8>)> {
        let f = self.conn.call(Body::Get { path: rel.to_string() }, Vec::new())?;
        match f.body {
            Body::Data(m) => Ok((m, f.payload)),
            b => bail!("unexpected reply {b:?}"),
        }
    }

    /// Write content with ordinary syscalls (local watchers see IN_MODIFY etc.).
    /// Caller must have dropped the kernel lease.
    fn write_local(&self, rel: &str, data: &[u8], mtime_ns: i64) -> io::Result<()> {
        let mut f = OpenOptions::new().write(true).truncate(true).custom_flags(libc::O_NOFOLLOW).open(self.abs(rel))?;
        f.write_all(data)?;
        sys::set_mtime_fd(f.as_fd(), mtime_ns)
    }

    fn make_placeholder(&self, rel: &str, size: u64, mtime_ns: i64, mode: u32) -> io::Result<()> {
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode & 0o7777)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.abs(rel))?;
        f.set_len(size)?;
        sys::set_mtime_fd(f.as_fd(), mtime_ns)
    }

    // ------------------------------------------------------------ startup

    fn materialize(&self) -> Result<()> {
        let r = self.conn.call(Body::List, Vec::new())?;
        let entries = match r.body {
            Body::Listing { entries, .. } => entries,
            b => bail!("unexpected {b:?}"),
        };
        self.mark_dir("")?;
        self.nodes.lock().unwrap().insert(String::new(), Node::new(Kind::Dir));
        let mut known = std::collections::HashSet::new();
        let (mut placeholders, mut cached, mut local_newer) = (0, 0, 0);
        for m in &entries {
            known.insert(m.path.clone());
            let p = self.abs(&m.path);
            let local = fs::symlink_metadata(&p).ok();
            match m.kind {
                Kind::Dir => {
                    if !local.as_ref().is_some_and(|l| l.is_dir()) {
                        let _ = fs::remove_file(&p);
                        fs::create_dir(&p)?;
                        fs::set_permissions(&p, fs::Permissions::from_mode(m.mode))?;
                    }
                    let mut n = Node::new(Kind::Dir);
                    n.version = m.version;
                    self.nodes.lock().unwrap().insert(m.path.clone(), n);
                    self.mark_dir(&m.path)?;
                }
                Kind::Symlink => {
                    let target = m.target.clone().unwrap_or_default();
                    let same = fs::read_link(&p).ok().is_some_and(|t| t.to_str() == Some(&target));
                    if !same {
                        let _ = fs::remove_file(&p);
                        std::os::unix::fs::symlink(&target, &p)?;
                    }
                    let mut n = Node::new(Kind::Symlink);
                    n.version = m.version;
                    self.nodes.lock().unwrap().insert(m.path.clone(), n);
                }
                Kind::File => {
                    let mut n = Node::new(Kind::File);
                    n.version = m.version;
                    n.size = m.size;
                    n.mtime_ns = m.mtime_ns;
                    // Placeholders have size > 0 but no allocated blocks.
                    let local_file = local.as_ref().filter(|l| l.is_file());
                    let has_data = local_file.is_some_and(|l| l.len() == 0 || l.blocks() > 0);
                    match local_file {
                        Some(l) if l.len() == m.size && mtime_of(l) == m.mtime_ns && has_data => {
                            cached += 1;
                        }
                        Some(l) if has_data && mtime_of(l) > m.mtime_ns => {
                            // Modified locally while we were down: keep it, upload it.
                            n.size = l.len();
                            n.mtime_ns = mtime_of(l);
                            n.dirty_gen = 1;
                            n.dirty_since = Instant::now() - self.opts.flush_delay;
                            local_newer += 1;
                        }
                        Some(l) if !has_data && l.len() == m.size && mtime_of(l) == m.mtime_ns => {
                            n.filled = m.size == 0; // existing placeholder
                            placeholders += 1;
                        }
                        _ => {
                            self.make_placeholder(&m.path, m.size, m.mtime_ns, m.mode)?;
                            n.filled = m.size == 0;
                            placeholders += 1;
                        }
                    }
                    let filled = n.filled;
                    self.nodes.lock().unwrap().insert(m.path.clone(), n);
                    if filled {
                        self.take_klease(&m.path)?;
                    } else {
                        self.mark(&m.path)?;
                    }
                }
            }
        }
        // Anything local the server doesn't know about gets adopted (uploaded).
        let mut adopted = 0;
        self.scan_unknown("", &known, &mut adopted)?;
        log!(
            "client",
            "materialized {} entries: {placeholders} placeholders, {cached} cached, {local_newer} locally newer, {adopted} adopted",
            entries.len()
        );
        Ok(())
    }

    fn scan_unknown(&self, rel: &str, known: &std::collections::HashSet<String>, n: &mut usize) -> Result<()> {
        for e in fs::read_dir(self.abs(rel))? {
            let e = e?;
            let Some(name) = e.file_name().to_str().map(String::from) else { continue };
            let child = join(rel, &name);
            if known.contains(&child) {
                if e.file_type()?.is_dir() {
                    self.scan_unknown(&child, known, n)?;
                }
            } else {
                self.adopt(&child)?;
                *n += 1;
            }
        }
        Ok(())
    }

    /// A path appeared locally that the server doesn't have: register it
    /// (recursively for directories) and queue file contents for upload.
    fn adopt(&self, rel: &str) -> Result<()> {
        let p = self.abs(rel);
        let md = match fs::symlink_metadata(&p) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let ft = md.file_type();
        if ft.is_dir() {
            self.conn.call(Body::Mkdir { path: rel.to_string(), mode: md.mode() & 0o7777 }, Vec::new())?;
            self.nodes.lock().unwrap().insert(rel.to_string(), Node::new(Kind::Dir));
            // Mark first, then list: anything created after the mark arrives as an event.
            self.mark_dir(rel)?;
            for e in fs::read_dir(&p)? {
                if let Some(name) = e?.file_name().to_str() {
                    let child = join(rel, name);
                    if !self.nodes.lock().unwrap().contains_key(&child) {
                        self.adopt(&child)?;
                    }
                }
            }
        } else if ft.is_symlink() {
            let target = fs::read_link(&p)?.to_string_lossy().into_owned();
            self.conn.call(Body::Symlink { path: rel.to_string(), target }, Vec::new())?;
            self.nodes.lock().unwrap().insert(rel.to_string(), Node::new(Kind::Symlink));
        } else if ft.is_file() {
            let mut n = Node::new(Kind::File);
            n.size = md.len();
            n.mtime_ns = mtime_of(&md);
            n.dirty_gen = 1;
            n.dirty_since = Instant::now() - self.opts.flush_delay;
            self.nodes.lock().unwrap().insert(rel.to_string(), n);
        }
        Ok(())
    }

    // ------------------------------------------------- pre-content (g1)

    fn g1_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let evs = match sys::fanotify_read(self.g1.as_fd(), &mut buf) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    log!("g1", "read: {e}");
                    return;
                }
            };
            for ev in evs {
                if ev.fd < 0 {
                    continue;
                }
                if ev.pid == self.pid {
                    // Our own access (fill, eviction, remote apply): never block it.
                    self.self_event(ev.fd);
                    continue;
                }
                let c = self.clone();
                std::thread::spawn(move || c.on_pre_access(ev));
            }
        }
    }

    fn self_event(&self, fd: RawFd) {
        let bfd = unsafe { BorrowedFd::borrow_raw(fd) };
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } == 0 {
            let op = self.self_ops.lock().unwrap().remove(&(st.st_dev, st.st_ino));
            if let Some(op) = op {
                if let Err(e) = op(bfd) {
                    log!("g1", "self op failed: {e}");
                }
            }
        }
        let _ = sys::fanotify_respond(self.g1.as_fd(), fd, sys::FAN_ALLOW);
        sys::close_raw(fd);
    }

    fn on_pre_access(&self, ev: sys::Event) {
        let t = Instant::now();
        let range = ev.infos.iter().find_map(|i| match i {
            Info::Range { offset, count } => Some((*offset, *count)),
            _ => None,
        });
        let res = self.hydrate(ev.fd, ev.pid);
        let resp = match &res {
            Ok(_) => sys::FAN_ALLOW,
            Err(e) => {
                log!("hydrate", "pid {} denied with EIO: {e:#}", ev.pid);
                sys::FAN_DENY | ((libc::EIO as u32) << 24)
            }
        };
        if let Err(e) = sys::fanotify_respond(self.g1.as_fd(), ev.fd, resp) {
            log!("hydrate", "respond: {e}");
        }
        sys::close_raw(ev.fd);
        if let Ok(Some(what)) = res {
            log!("hydrate", "{what} pid={} range={range:?} in {:?}", ev.pid, t.elapsed());
        }
    }

    /// Returns Some(description) when work was done (for logging).
    fn hydrate(&self, evfd: RawFd, _pid: i32) -> Result<Option<String>> {
        let path = sys::fd_path(evfd)?;
        let Some(rel) = self.rel_of(&path) else { return Ok(None) };
        let l = self.plock(&rel);
        let _g = l.lock().unwrap();
        let Some((filled, wlease, has_kl)) = self.with_node(&rel, |n| (n.filled, n.wlease, n.klease.is_some())) else {
            return Ok(None);
        };
        let mut did = Vec::new();
        if !filled {
            let (m, data) = self.fetch(&rel)?;
            let efd = unsafe { BorrowedFd::borrow_raw(evfd) };
            sys::pwrite_all(efd, &data, 0)?;
            sys::set_mtime_fd(efd, m.mtime_ns)?;
            self.with_node(&rel, |n| {
                n.filled = true;
                n.version = m.version;
                n.size = data.len() as u64;
                n.mtime_ns = m.mtime_ns;
                n.last_access = Instant::now();
            });
            did.push(format!("filled {rel} ({} bytes, v{})", data.len(), m.version));
        }
        // Drop our writable event fd (keep the number for the response), then
        // check for writers: F_RDLCK fails iff someone has the file open for write.
        sys::neuter_fd(evfd)?;
        if !wlease && !has_kl && !self.take_klease(&rel)? {
            let t = Instant::now();
            self.acquire_write(&rel, false)?;
            did.push(format!("write lease {rel} in {:?}", t.elapsed()));
        }
        self.unmark(&rel);
        Ok(if did.is_empty() { None } else { Some(did.join("; ")) })
    }

    /// Get the server write lease (caller holds the path lock). If the server
    /// has a newer version than ours, refresh local content first.
    fn acquire_write(&self, rel: &str, drop_kl: bool) -> Result<()> {
        let f = self.conn.call(Body::Acquire { path: rel.to_string() }, Vec::new())?;
        let version = match f.body {
            Body::Granted { version } => version,
            b => bail!("unexpected {b:?}"),
        };
        let (ours, filled) = self.with_node(rel, |n| {
            n.wlease = true;
            (n.version, n.filled)
        })
        .ok_or_else(|| anyhow!("{rel} vanished"))?;
        if version > ours && filled {
            let (m, data) = self.fetch(rel)?;
            // Make later opens HSM-visible before dropping the lease (see apply_put).
            self.mark(rel)?;
            if drop_kl {
                self.drop_klease(rel);
            }
            self.write_local(rel, &data, m.mtime_ns)?;
            self.with_node(rel, |n| {
                n.version = m.version;
                n.size = data.len() as u64;
                n.mtime_ns = m.mtime_ns;
            });
            log!("lease", "{rel}: refreshed to v{} before granting local write", m.version);
        } else if drop_kl {
            self.drop_klease(rel);
        }
        Ok(())
    }

    // --------------------------------------------------- lease breaks

    fn lease_loop(self: Arc<Self>, sfd: OwnedFd) {
        loop {
            let fd = match sys::signalfd_read_fd(sfd.as_fd()) {
                Ok(fd) => fd,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    log!("leases", "signalfd: {e}");
                    return;
                }
            };
            let rel = self.lease_fds.lock().unwrap().get(&fd).cloned();
            if let Some(rel) = rel {
                let c = self.clone();
                std::thread::spawn(move || c.on_lease_break(&rel));
            }
        }
    }

    /// A local process is blocked opening `rel` for write.
    fn on_lease_break(&self, rel: &str) {
        let t = Instant::now();
        let l = self.plock(rel);
        let _g = l.lock().unwrap();
        let Some((has_kl, wlease)) = self.with_node(rel, |n| (n.klease.is_some(), n.wlease)) else { return };
        if !has_kl {
            return; // already handled
        }
        if wlease {
            self.drop_klease(rel);
            log!("lease", "{rel}: local write (lease already held)");
            return;
        }
        match self.acquire_write(rel, true) {
            Ok(()) => log!("lease", "{rel}: local write gated {:?} for server lease", t.elapsed()),
            Err(e) => {
                // Fail open: v0 prefers availability over blocking the writer.
                log!("lease", "{rel}: acquire failed ({e:#}); letting write through");
                self.drop_klease(rel);
            }
        }
    }

    // ------------------------------------------- namespace events (g2)

    fn g2_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let evs = match sys::fanotify_read(self.g2.as_fd(), &mut buf) {
                Ok(v) => v,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    log!("g2", "read: {e}");
                    return;
                }
            };
            for ev in evs {
                if ev.mask & sys::FAN_Q_OVERFLOW != 0 {
                    log!("g2", "event queue overflow: local changes may be missed (rescan not implemented)");
                    continue;
                }
                if ev.pid == self.pid {
                    continue;
                }
                if let Err(e) = self.on_ns_event(&ev) {
                    log!("g2", "event {:#x}: {e:#}", ev.mask);
                }
            }
        }
    }

    fn resolve(&self, kind: u8, ev: &sys::Event) -> Option<String> {
        ev.infos.iter().find_map(|i| match i {
            Info::DirName { kind: k, dir, name } if *k == kind => {
                let d = self.dirs.lock().unwrap().get(dir).cloned()?;
                Some(join(&d, sys::os(name).to_str()?))
            }
            _ => None,
        })
    }

    fn on_ns_event(&self, ev: &sys::Event) -> Result<()> {
        let m = ev.mask;
        let is_dir = m & sys::FAN_ONDIR != 0;
        if m & sys::FAN_RENAME != 0 {
            let from = self.resolve(sys::INFO_OLD_DFID_NAME, ev);
            let to = self.resolve(sys::INFO_NEW_DFID_NAME, ev);
            return self.local_rename(from, to);
        }
        let Some(rel) = self.resolve(sys::INFO_DFID_NAME, ev) else { return Ok(()) };
        if m & sys::FAN_CREATE != 0 {
            if !self.nodes.lock().unwrap().contains_key(&rel) {
                self.adopt(&rel)?;
                // A plain file is uploaded on close-write; don't upload an empty
                // half-written file now.
                self.with_node(&rel, |n| {
                    if n.kind == Kind::File {
                        n.flushed_gen = n.dirty_gen;
                    }
                });
                log!("local", "create {rel}");
            }
        }
        if m & sys::FAN_CLOSE_WRITE != 0 && !is_dir {
            let mut nodes = self.nodes.lock().unwrap();
            let n = nodes.entry(rel.clone()).or_insert_with(|| Node::new(Kind::File));
            n.dirty_gen += 1;
            n.dirty_since = Instant::now();
            n.filled = true;
            n.last_access = Instant::now();
        }
        if m & sys::FAN_OPEN != 0 && !is_dir {
            self.with_node(&rel, |n| n.last_access = Instant::now());
        }
        if m & sys::FAN_DELETE != 0 {
            self.local_delete(&rel)?;
        }
        Ok(())
    }

    fn forget_subtree(&self, rel: &str) -> bool {
        let mut known_remote = false;
        let removed: Vec<Node> = {
            let mut nodes = self.nodes.lock().unwrap();
            let keys: Vec<String> = nodes.keys().filter(|k| under(k, rel)).cloned().collect();
            keys.iter().filter_map(|k| nodes.remove(k)).collect()
        };
        for n in removed {
            known_remote |= n.version > 0 || n.kind != Kind::File;
            if let Some(fd) = n.klease {
                self.release_fd(fd);
            }
        }
        self.dirs.lock().unwrap().retain(|_, v| !under(v, rel));
        known_remote
    }

    fn local_delete(&self, rel: &str) -> Result<()> {
        let l = self.plock(rel);
        let _g = l.lock().unwrap();
        if self.forget_subtree(rel) {
            self.conn.call(Body::Remove { path: rel.to_string() }, Vec::new())?;
        }
        log!("local", "delete {rel}");
        Ok(())
    }

    fn local_rename(&self, from: Option<String>, to: Option<String>) -> Result<()> {
        match (from, to) {
            (Some(from), Some(to)) => {
                let (known_remote, existed) = {
                    let nodes = self.nodes.lock().unwrap();
                    match nodes.get(&from) {
                        Some(n) => (n.version > 0 || n.kind != Kind::File, true),
                        None => (false, false),
                    }
                };
                if !existed {
                    return self.adopt(&to);
                }
                // Overwritten destination.
                self.forget_subtree(&to);
                {
                    let mut nodes = self.nodes.lock().unwrap();
                    rekey(&mut nodes, &from, &to);
                }
                for v in self.lease_fds.lock().unwrap().values_mut() {
                    if under(v, &from) {
                        *v = format!("{to}{}", &v[from.len()..]);
                    }
                }
                for v in self.dirs.lock().unwrap().values_mut() {
                    if under(v, &from) {
                        *v = format!("{to}{}", &v[from.len()..]);
                    }
                }
                if known_remote {
                    self.conn.call(Body::Rename { from: from.clone(), to: to.clone() }, Vec::new())?;
                }
                // else: never uploaded (e.g. editor temp file); the pending
                // upload will simply Put under the new name.
                log!("local", "rename {from} -> {to}");
                Ok(())
            }
            (Some(from), None) => self.local_delete(&from), // moved out of the tree
            (None, Some(to)) => self.adopt(&to),            // moved into the tree
            (None, None) => Ok(()),
        }
    }

    // ------------------------------------------------------- write-back

    fn uploader_loop(self: Arc<Self>) {
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let due: Vec<String> = {
                let nodes = self.nodes.lock().unwrap();
                nodes
                    .iter()
                    .filter(|(_, n)| n.kind == Kind::File && n.dirty() && n.dirty_since.elapsed() >= self.opts.flush_delay)
                    .map(|(k, _)| k.clone())
                    .collect()
            };
            for rel in due {
                let l = self.plock(&rel);
                let _g = l.lock().unwrap();
                if let Err(e) = self.flush_locked(&rel) {
                    log!("upload", "{rel}: {e:#}");
                }
            }
        }
    }

    /// Upload `rel` if dirty. Caller holds the path lock.
    fn flush_locked(&self, rel: &str) -> Result<()> {
        let Some(gen) = self.with_node(rel, |n| if n.dirty() { Some(n.dirty_gen) } else { None }).flatten() else {
            return Ok(());
        };
        let p = self.abs(rel);
        let mut f = match OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(&p) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let md = f.metadata()?;
        let mut data = Vec::with_capacity(md.len() as usize);
        f.read_to_end(&mut data)?;
        drop(f);
        let t = Instant::now();
        let r = self.conn.call(
            Body::Put { path: rel.to_string(), mtime_ns: mtime_of(&md), mode: md.mode() & 0o7777 },
            data,
        )?;
        let version = match r.body {
            Body::PutOk { version } => version,
            b => bail!("unexpected {b:?}"),
        };
        self.with_node(rel, |n| {
            n.version = version;
            n.size = md.len();
            n.mtime_ns = mtime_of(&md);
            n.wlease = true; // Put acquires the lease server-side
            n.flushed_gen = gen;
        });
        log!("upload", "{rel}: {} bytes -> v{version} in {:?}", md.len(), t.elapsed());
        // Re-arm the write gate (fails harmlessly if a writer still has it open).
        let _ = self.take_klease(rel);
        Ok(())
    }

    // ------------------------------------------------- server pushes

    fn push_loop(self: Arc<Self>, rx: Receiver<Frame>) {
        for f in rx {
            match f.body {
                Body::Recall { path } => {
                    let c = self.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = c.on_recall(&path) {
                            log!("recall", "{path}: {e:#}");
                        }
                    });
                }
                Body::Changed { seq, from_client, op } => {
                    if let Err(e) = self.apply_remote(&op) {
                        log!("remote", "seq {seq} from {from_client}: {op:?}: {e:#}");
                    }
                }
                b => log!("push", "unexpected {b:?}"),
            }
        }
    }

    fn on_recall(&self, rel: &str) -> Result<()> {
        let t = Instant::now();
        let l = self.plock(rel);
        let _g = l.lock().unwrap();
        if self.with_node(rel, |n| n.wlease) == Some(true) {
            // Re-arm the kernel gate so no new local writer can start; wait
            // (bounded) for current writers to close.
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match self.take_klease(rel) {
                    Ok(true) => break,
                    Ok(false) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
                    Ok(false) => {
                        log!("recall", "{rel}: local writer still open after 10s; releasing anyway");
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                    Err(e) => return Err(e.into()),
                }
            }
            self.flush_locked(rel)?;
            self.with_node(rel, |n| n.wlease = false);
        }
        self.conn.call(Body::Released { path: rel.to_string() }, Vec::new())?;
        log!("recall", "{rel}: released in {:?}", t.elapsed());
        Ok(())
    }

    fn apply_remote(&self, op: &Op) -> Result<()> {
        match op {
            Op::Put(m) => self.apply_put(m),
            Op::Mkdir(m) => {
                let p = self.abs(&m.path);
                match fs::create_dir(&p) {
                    Ok(()) => fs::set_permissions(&p, fs::Permissions::from_mode(m.mode))?,
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e.into()),
                }
                self.nodes.lock().unwrap().entry(m.path.clone()).or_insert_with(|| Node::new(Kind::Dir)).version =
                    m.version;
                self.mark_dir(&m.path)?;
                log!("remote", "mkdir {}", m.path);
                Ok(())
            }
            Op::Symlink(m) => {
                let p = self.abs(&m.path);
                let _ = fs::remove_file(&p);
                std::os::unix::fs::symlink(m.target.as_deref().unwrap_or(""), &p)?;
                self.nodes.lock().unwrap().insert(m.path.clone(), Node::new(Kind::Symlink));
                log!("remote", "symlink {}", m.path);
                Ok(())
            }
            Op::Remove { path } => {
                let l = self.plock(path);
                let _g = l.lock().unwrap();
                self.forget_subtree(path);
                let p = self.abs(path);
                match fs::symlink_metadata(&p) {
                    Ok(md) if md.is_dir() => fs::remove_dir_all(&p)?,
                    Ok(_) => fs::remove_file(&p)?,
                    Err(_) => {}
                }
                log!("remote", "remove {path}");
                Ok(())
            }
            Op::Rename { from, to } => {
                self.forget_subtree(to);
                fs::rename(self.abs(from), self.abs(to))?;
                rekey(&mut self.nodes.lock().unwrap(), from, to);
                for v in self.lease_fds.lock().unwrap().values_mut() {
                    if under(v, from) {
                        *v = format!("{to}{}", &v[from.len()..]);
                    }
                }
                for v in self.dirs.lock().unwrap().values_mut() {
                    if under(v, from) {
                        *v = format!("{to}{}", &v[from.len()..]);
                    }
                }
                log!("remote", "rename {from} -> {to}");
                Ok(())
            }
        }
    }

    fn apply_put(&self, m: &Meta) -> Result<()> {
        let rel = m.path.as_str();
        let l = self.plock(rel);
        let _g = l.lock().unwrap();
        let state = self.with_node(rel, |n| (n.version, n.filled, n.wlease));
        match state {
            Some((v, _, _)) if m.version <= v => return Ok(()), // already have it
            None => {
                // New file: placeholder only; content arrives on first access.
                self.make_placeholder(rel, m.size, m.mtime_ns, m.mode)?;
                let mut n = Node::new(Kind::File);
                n.version = m.version;
                n.size = m.size;
                n.mtime_ns = m.mtime_ns;
                n.filled = m.size == 0;
                self.nodes.lock().unwrap().insert(rel.to_string(), n);
                if m.size == 0 {
                    self.take_klease(rel)?;
                } else {
                    self.mark(rel)?;
                }
                log!("remote", "new {rel} ({} bytes, placeholder)", m.size);
            }
            Some((_, filled, wlease)) => {
                if wlease {
                    log!("remote", "{rel}: remote write while we held the lease (lease was broken)");
                    self.with_node(rel, |n| n.wlease = false);
                }
                if filled {
                    // Hot file: keep it hot. Mark first so any opener that
                    // slips in while the gate is down is HSM-visible and gets
                    // gated by the hydration handler (blocked on our path lock).
                    let (fm, data) = self.fetch(rel)?;
                    self.mark(rel)?;
                    self.drop_klease(rel);
                    self.write_local(rel, &data, fm.mtime_ns)?;
                    self.with_node(rel, |n| {
                        n.version = fm.version;
                        n.size = data.len() as u64;
                        n.mtime_ns = fm.mtime_ns;
                    });
                    if self.take_klease(rel)? {
                        self.unmark(rel);
                    }
                    log!("remote", "update {rel} ({} bytes, v{}, eager)", data.len(), fm.version);
                } else {
                    // Cold file: just resize the placeholder (fires IN_MODIFY).
                    let f = OpenOptions::new().write(true).custom_flags(libc::O_NOFOLLOW).open(self.abs(rel))?;
                    f.set_len(m.size)?;
                    sys::set_mtime_fd(f.as_fd(), m.mtime_ns)?;
                    self.with_node(rel, |n| {
                        n.version = m.version;
                        n.size = m.size;
                        n.mtime_ns = m.mtime_ns;
                    });
                    log!("remote", "update {rel} ({} bytes, v{}, placeholder)", m.size, m.version);
                }
            }
        }
        Ok(())
    }

    // --------------------------------------------------------- eviction

    fn evictor_loop(self: Arc<Self>) {
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let (used, mut cands) = {
                let nodes = self.nodes.lock().unwrap();
                let mut used = 0u64;
                let mut c = Vec::new();
                for (k, n) in nodes.iter() {
                    if n.kind == Kind::File && n.filled {
                        used += n.size;
                        if !n.dirty() && !n.wlease && n.klease.is_some() && n.size > 0 {
                            c.push((n.last_access, k.clone(), n.size));
                        }
                    }
                }
                (used, c)
            };
            if used <= self.opts.cache_max {
                continue;
            }
            let target = self.opts.cache_max / 10 * 9;
            cands.sort();
            let mut cur = used;
            let (mut n, mut freed) = (0, 0u64);
            for (_, rel, size) in cands {
                if cur <= target {
                    break;
                }
                match self.evict(&rel) {
                    Ok(true) => {
                        cur -= size;
                        freed += size;
                        n += 1;
                    }
                    Ok(false) => {}
                    Err(e) => log!("evict", "{rel}: {e:#}"),
                }
            }
            if n > 0 {
                log!("evict", "evicted {n} files ({freed} bytes); cache {cur}/{} bytes", self.opts.cache_max);
            }
        }
    }

    /// Punch `rel` back to a placeholder. Ok(false) if it's in use.
    fn evict(&self, rel: &str) -> Result<bool> {
        let l = self.plock(rel);
        let _g = l.lock().unwrap();
        let Some(Some((size, mtime))) = self.with_node(rel, |n| {
            (n.kind == Kind::File && n.filled && !n.dirty() && !n.wlease && n.klease.is_some())
                .then_some((n.size, n.mtime_ns))
        }) else {
            return Ok(false);
        };
        // 1. Mark first: every open from now on is HSM-enabled.
        self.mark(rel)?;
        // 2. Upgrade to a write lease: succeeds only if nobody (but us) has it open,
        //    so no non-HSM open file can exist.
        let upgraded = {
            let nodes = self.nodes.lock().unwrap();
            let fd = nodes.get(rel).and_then(|n| n.klease.as_ref()).ok_or_else(|| anyhow!("lease gone"))?;
            match sys::set_lease(fd.as_fd(), sys::LEASE_WR) {
                Ok(()) => true,
                Err(e) if is_eagain(&e) => false,
                Err(e) => return Err(e.into()),
            }
        };
        if !upgraded {
            self.unmark(rel);
            return Ok(false);
        }
        // 3. Drop the lease, then punch through a NONOTIFY fd obtained from our
        //    own pre-content event so local watchers see no IN_MODIFY.
        self.drop_klease(rel);
        let f = OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(self.abs(rel))?;
        let md = f.metadata()?;
        self.self_ops.lock().unwrap().insert(
            (md.dev(), md.ino()),
            Box::new(move |efd| {
                sys::punch_hole(efd, size)?;
                sys::set_mtime_fd(efd, mtime)
            }),
        );
        let mut b = [0u8; 1];
        let r = (&f).read(&mut b);
        let leftover = self.self_ops.lock().unwrap().remove(&(md.dev(), md.ino()));
        r?;
        if leftover.is_some() {
            // Our own read produced no pre-content event, so nothing was punched.
            // Content is intact; the mark stays, so the next access re-validates it.
            log!("evict", "{rel}: no self event delivered; nothing freed");
        }
        self.with_node(rel, |n| n.filled = false);
        Ok(true)
    }
}

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<Client>();
}
