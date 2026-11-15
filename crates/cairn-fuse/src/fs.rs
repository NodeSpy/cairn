//! `CairnFs`: the `fuser::Filesystem` that maps kernel requests onto [`Core`] calls.
//!
//! Core owns all state. This layer keeps the kernel inode table (lookup counts), per-open
//! handles (the live-file fd Core returns, shared passthrough backing ids), per-opendir directory
//! snapshots, and computes creator credentials / umask / setgid / default-ACL inheritance for new
//! nodes (the kernel enforces permissions with `default_permissions`; Core does not).

use crate::acl;
use crate::ktable::KTable;
use crate::stats::{self, Op};
use crate::util::*;
use cairn_client::api::*;
use cairn_proto::{AttrChanges, Kind, RenameMode};
use fuser::{
    BackingId, BsdFileFlags, CopyFileRangeFlags, FileAttr, FileHandle, Filesystem, FopenFlags, Generation, INodeNo,
    InitFlags, KernelConfig, LockOwner, Notifier, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyLseek, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, UNIX_EPOCH};

type FErrno = fuser::Errno;

fn fe(e: Errno) -> FErrno {
    FErrno::from_i32(e.0)
}

fn fe_io(e: io::Error) -> FErrno {
    FErrno::from_i32(e.raw_os_error().unwrap_or(libc::EIO))
}

fn last() -> FErrno {
    fe_io(io::Error::last_os_error())
}

const NAME_MAX: usize = 255;

#[derive(Clone, Debug)]
pub struct FsOpts {
    pub ttl: Duration,
    pub neg_ttl: Duration,
    pub passthrough: bool,
    /// `statfs` answers from this path (the cache dir); synthetic numbers when None.
    pub statfs_path: Option<PathBuf>,
    pub max_stack_depth: u32,
}

impl Default for FsOpts {
    fn default() -> Self {
        FsOpts {
            ttl: Duration::from_secs(3600),
            neg_ttl: Duration::from_secs(3600),
            passthrough: true,
            statfs_path: None,
            max_stack_depth: 1,
        }
    }
}

/// State shared between the request handlers, the applier and the mount.
pub struct Shared {
    pub ktable: KTable,
    pub notifier: Mutex<Option<Notifier>>,
    /// tid of the applier thread; requests from it are `Origin::Remote`.
    pub applier_tid: AtomicU32,
    pub opts: FsOpts,
}

impl Shared {
    pub fn new(opts: FsOpts) -> Arc<Shared> {
        Arc::new(Shared { ktable: KTable::new(), notifier: Mutex::new(None), applier_tid: AtomicU32::new(0), opts })
    }

    pub fn inval_inode(&self, ino: Ino, off: i64, len: i64) {
        // Clone out of the lock: the notify write can block in the kernel (parent dir lock), and
        // handler threads call this too; holding the mutex would stall every handler (ABBA).
        let n = self.notifier.lock().clone();
        if let Some(n) = n {
            let _ = n.inval_inode(INodeNo(ino), off, len);
        }
    }

    pub fn inval_entry(&self, parent: Ino, name: &[u8]) {
        let n = self.notifier.lock().clone();
        if let Some(n) = n {
            let _ = n.inval_entry(INodeNo(parent), OsStr::from_bytes(name));
        }
    }
}

struct Handle {
    ino: Ino,
    token: u64,
    /// Core's live file (kept for lseek/fallocate/copy_file_range and the non-passthrough path).
    file: Option<File>,
    _backing: Option<Arc<BackingId>>,
    write: bool,
    /// Opened by the applier: no Core open happened (no lease, no content fetch).
    remote: bool,
}

struct DirHandle {
    ino: Ino,
    snap: Option<Arc<DirSnapshot>>,
    /// Attributes at opendir: fstat on an open directory must work after it was removed or replaced.
    info: Option<NodeInfo>,
}

pub struct CairnFs {
    core: Arc<dyn Core>,
    sh: Arc<Shared>,
    passthrough: AtomicBool,
    next_fh: AtomicU64,
    handles: RwLock<HashMap<u64, Arc<Handle>>>,
    dirs: Mutex<HashMap<u64, Arc<Mutex<DirHandle>>>>,
    /// One backing id per inode, shared by all handles of it (kernel requirement).
    
    backing_by_ino: Mutex<HashMap<Ino, Weak<BackingId>>>,
    /// Inodes with open writers: their attributes are never cached by the kernel (a shared mmap
    /// write changes mtime/ctime/size on the live file without any FUSE request), see `ttl_for`.
    writers: Mutex<HashMap<Ino, u32>>,
    n_writers: AtomicU64,
    /// Runs `Core::release` of writable handles off the request threads (see `ReleasePool`).
    rel_pool: ReleasePool,
}

type Job = Box<dyn FnOnce() + Send>;

/// `CAIRN_SYNC_RELEASE=1` makes RELEASE of writable handles run inline again (debugging).
fn sync_release() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var_os("CAIRN_SYNC_RELEASE").is_some())
}

/// Closing the last writer makes Core hash the file and publish it, which costs milliseconds for
/// a big file, and a process closing its fd can end up waiting for the RELEASE reply (fio's
/// write bandwidth is measured through the close). The reply does not need the result and
/// Core is correct with a release that lands later (a node with a pending release is still dirty
/// and reads its attrs from the live file), so the request thread (or io_uring queue) hands the
/// work to a small pool and replies at once. Dropping the pool drains it: it runs before Core is
/// shut down (the session, and with it the `CairnFs`, is dropped when the mount thread ends).
struct ReleasePool {
    tx: Mutex<Option<std::sync::mpsc::Sender<Job>>>,
    workers: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl ReleasePool {
    fn new() -> ReleasePool {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).clamp(2, 8);
        let workers = (0..n)
            .map(|i| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name(format!("cairn-rel-{i}"))
                    .spawn(move || loop {
                        let job = rx.lock().recv();
                        match job {
                            Ok(j) => j(),
                            Err(_) => break,
                        }
                    })
                    .expect("spawn release worker")
            })
            .collect();
        ReleasePool { tx: Mutex::new(Some(tx)), workers: Mutex::new(workers) }
    }

    fn submit(&self, job: Job) {
        if let Some(tx) = self.tx.lock().as_ref() {
            if let Err(e) = tx.send(job) {
                (e.0)();
            }
        } else {
            job();
        }
    }
}

impl Drop for ReleasePool {
    fn drop(&mut self) {
        self.tx.lock().take();
        for w in self.workers.lock().drain(..) {
            let _ = w.join();
        }
    }
}

macro_rules! tryr {
    ($reply:expr, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return $reply.error(e),
        }
    };
}

fn attr_of(i: &NodeInfo) -> FileAttr {
    let a = &i.attrs;
    let size = a.size;
    FileAttr {
        ino: INodeNo(i.ino),
        size,
        blocks: size.div_ceil(512),
        atime: ts_ns(a.atime_ns),
        mtime: ts_ns(a.mtime_ns),
        ctime: ts_ns(a.ctime_ns),
        crtime: UNIX_EPOCH,
        kind: file_type(a.kind),
        perm: (a.mode & 0o7777) as u16,
        nlink: i.nlink,
        uid: a.uid,
        gid: a.gid,
        rdev: enc_dev(a.rdev),
        blksize: 4096,
        flags: 0,
    }
}

fn negative() -> FileAttr {
    FileAttr {
        ino: INodeNo(0),
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: fuser::FileType::RegularFile,
        perm: 0,
        nlink: 0,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 0,
        flags: 0,
    }
}

fn check_name(n: &OsStr) -> Result<(), FErrno> {
    if n.len() > NAME_MAX {
        Err(FErrno::ENAMETOOLONG)
    } else {
        Ok(())
    }
}

/// Re-open `f` read-write through /proc so one backing file serves readers and writers alike.
fn reopen_rdwr(f: &File) -> io::Result<File> {
    let p = std::ffi::CString::new(format!("/proc/self/fd/{}", f.as_raw_fd())).unwrap();
    let fd = unsafe { libc::open(p.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    if fd < 0 {
        return f.try_clone();
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

impl CairnFs {
    pub fn new(core: Arc<dyn Core>, sh: Arc<Shared>) -> CairnFs {
        let pt = sh.opts.passthrough;
        CairnFs {
            core,
            sh,
            passthrough: AtomicBool::new(pt),
            next_fh: AtomicU64::new(1),
            handles: RwLock::new(HashMap::new()),
            dirs: Mutex::new(HashMap::new()),
            backing_by_ino: Mutex::new(HashMap::new()),
            writers: Mutex::new(HashMap::new()),
            n_writers: AtomicU64::new(0),
            rel_pool: ReleasePool::new(),
        }
    }

    /// Entry/attr TTL for `ino`: zero while it has open writers.
    fn ttl_for(&self, ino: Ino) -> Duration {
        if self.n_writers.load(Ordering::Relaxed) != 0 && self.writers.lock().contains_key(&ino) {
            return Duration::ZERO;
        }
        self.sh.opts.ttl
    }

    fn writer_open(&self, ino: Ino) {
        *self.writers.lock().entry(ino).or_insert(0) += 1;
        self.n_writers.fetch_add(1, Ordering::Relaxed);
    }

    fn writer_close(&self, ino: Ino) {
        let mut w = self.writers.lock();
        if let Some(c) = w.get_mut(&ino) {
            *c -= 1;
            if *c == 0 {
                w.remove(&ino);
            }
            self.n_writers.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn origin(&self, req: &Request) -> Origin {
        let tid = self.sh.applier_tid.load(Ordering::Relaxed);
        if tid != 0 && req.pid() == tid {
            Origin::Remote
        } else {
            Origin::Local { uid: req.uid(), gid: req.gid(), pid: req.pid() }
        }
    }

    fn entry(&self, i: &NodeInfo) -> FileAttr {
        self.sh.ktable.inc(i.ino, i.node);
        attr_of(i)
    }

    fn reply_entry(&self, i: &NodeInfo, reply: ReplyEntry) {
        let a = self.entry(i);
        reply.entry(&self.ttl_for(i.ino), &a, Generation(i.generation));
    }

    /// Resolve the parent directory of `dir` by walking its root-relative path from the root
    /// (export handles: the kernel asks LOOKUP ".." on directories it forgot).
    fn parent_of(&self, dir: Ino) -> FsResult<NodeInfo> {
        if dir == ROOT_INO {
            return self.core.getattr(ROOT_INO);
        }
        let path = self.core.path_of(dir).ok_or(Errno::ESTALE)?;
        let comps: Vec<&[u8]> = path.split(|&b| b == b'/').filter(|c| !c.is_empty()).collect();
        let mut cur = self.core.getattr(ROOT_INO)?;
        for c in &comps[..comps.len().saturating_sub(1)] {
            cur = self.core.lookup(cur.ino, c)?;
        }
        Ok(cur)
    }

    /// Credentials, mode, ACL inheritance for a node about to be created by a local process.
    fn new_node(
        &self,
        req: &Request,
        parent: Ino,
        kind: Kind,
        mode: u32,
        umask: u32,
        rdev: u64,
        target: Option<Vec<u8>>,
    ) -> FsResult<NewNode> {
        let remote = matches!(self.origin(req), Origin::Remote);
        let mut n = NewNode {
            kind,
            mode: mode & 0o7777,
            uid: req.uid(),
            gid: req.gid(),
            rdev,
            symlink_target: target,
            xattrs: vec![],
        };
        if remote {
            return Ok(n); // Core takes everything from the in-flight change
        }
        let p = self.core.getattr(parent)?;
        if p.attrs.kind != Kind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if p.attrs.mode & libc::S_ISGID != 0 {
            n.gid = p.attrs.gid;
            if kind == Kind::Dir {
                n.mode |= libc::S_ISGID;
            } else if n.mode & (libc::S_ISGID | 0o010) == (libc::S_ISGID | 0o010)
                && req.uid() != 0
                && !in_group(req.pid(), req.gid(), n.gid)
            {
                n.mode &= !libc::S_ISGID;
            }
        }
        if kind == Kind::Symlink {
            n.mode = 0o777;
            return Ok(n);
        }
        let def = self.core.getxattr(parent, acl::DEFAULT).ok();
        match def.as_deref().and_then(|d| acl::create(d, n.mode).map(|c| (d, c))) {
            Some((d, c)) => {
                n.mode = (n.mode & !0o777) | (c.mode & 0o777);
                if let Some(a) = c.access {
                    n.xattrs.push((acl::ACCESS.to_vec(), a));
                }
                if kind == Kind::Dir {
                    n.xattrs.push((acl::DEFAULT.to_vec(), d.to_vec()));
                }
            }
            None => n.mode &= !(umask & 0o777),
        }
        Ok(n)
    }

    /// Get (or register) the passthrough backing id for `ino`.
    fn backing_for(
        &self,
        ino: Ino,
        file: &File,
        register: impl FnOnce(&File) -> io::Result<BackingId>,
    ) -> io::Result<Arc<BackingId>> {
        let mut m = self.backing_by_ino.lock();
        if let Some(b) = m.get(&ino).and_then(Weak::upgrade) {
            return Ok(b);
        }
        let f2 = reopen_rdwr(file)?;
        let id = Arc::new(register(&f2)?);
        m.insert(ino, Arc::downgrade(&id));
        Ok(id)
    }

    fn add_handle(&self, h: Handle) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.write().insert(fh, Arc::new(h));
        fh
    }

    fn handle(&self, fh: FileHandle) -> Result<Arc<Handle>, FErrno> {
        self.handles.read().get(&fh.0).cloned().ok_or(FErrno::EBADF)
    }

    fn handle_file(&self, fh: FileHandle) -> Result<Arc<Handle>, FErrno> {
        let h = self.handle(fh)?;
        if h.file.is_none() {
            return Err(FErrno::EBADF);
        }
        Ok(h)
    }

    /// Open `ino`: Core open + handle + passthrough backing. Returns (fh, backing).
    fn do_open(
        &self,
        origin: Origin,
        ino: Ino,
        flags: i32,
        register: impl FnOnce(&File) -> io::Result<BackingId>,
    ) -> Result<(u64, Option<Arc<BackingId>>), FErrno> {
        let write = flags & libc::O_ACCMODE != libc::O_RDONLY;
        if origin == Origin::Remote {
            let fh = self.add_handle(Handle { ino, token: 0, file: None, _backing: None, write: false, remote: true });
            return Ok((fh, None));
        }
        let of = self.core.open(origin, ino, write, flags & libc::O_TRUNC != 0).map_err(fe)?;
        let mut backing = None;
        if self.passthrough.load(Ordering::Relaxed) {
            match self.backing_for(ino, &of.file, register) {
                Ok(b) => backing = Some(b),
                Err(e) => {
                    let _ = self.core.release(ino, of.token);
                    return Err(fe_io(e));
                }
            }
        }
        if write {
            self.writer_open(ino);
        }
        let fh = self.add_handle(Handle {
            ino,
            token: of.token,
            file: Some(of.file),
            _backing: backing.clone(),
            write,
            remote: false,
        });
        Ok((fh, backing))
    }

    fn xattr_reply(&self, r: FsResult<Vec<u8>>, size: u32, reply: ReplyXattr) {
        match r {
            Err(e) => reply.error(fe(e)),
            Ok(v) if size == 0 => reply.size(v.len() as u32),
            Ok(v) if v.len() > size as usize => reply.error(FErrno::ERANGE),
            Ok(v) => reply.data(&v),
        }
    }

    fn do_create_node(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        kind: Kind,
        mode: u32,
        umask: u32,
        rdev: u64,
        target: Option<Vec<u8>>,
    ) -> Result<NodeInfo, FErrno> {
        check_name(name)?;
        let origin = self.origin(req);
        let new = self.new_node(req, parent.0, kind, mode, umask, rdev, target).map_err(fe)?;
        self.core.create(origin, parent.0, name.as_bytes(), new).map_err(fe)
    }
}

impl Filesystem for CairnFs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let mut want = vec![
            InitFlags::FUSE_ASYNC_READ,
            InitFlags::FUSE_PARALLEL_DIROPS,
            InitFlags::FUSE_CACHE_SYMLINKS,
            InitFlags::FUSE_MAX_PAGES,
            InitFlags::FUSE_DO_READDIRPLUS,
            InitFlags::FUSE_READDIRPLUS_AUTO,
            InitFlags::FUSE_ATOMIC_O_TRUNC,
            // NFS-style handles: the nodeid is the persistent local ino, Core resolves it after a restart.
            InitFlags::FUSE_EXPORT_SUPPORT,
            // Kernel evaluates POSIX ACLs; we apply umask ourselves so default ACLs work.
            InitFlags::FUSE_POSIX_ACL,
            InitFlags::FUSE_DONT_MASK,
        ];
        if self.passthrough.load(Ordering::Relaxed) {
            want.push(InitFlags::FUSE_PASSTHROUGH);
        }
        for f in want {
            if let Err(missing) = config.add_capabilities(f) {
                eprintln!("cairnd: kernel lacks {missing:?}");
                if f == InitFlags::FUSE_PASSTHROUGH {
                    self.passthrough.store(false, Ordering::Relaxed);
                }
            }
        }
        // Passthrough needs depth >= 1; it must stay at 1 so overlayfs on top (2) is allowed.
        let _ = config.set_max_stack_depth(self.sh.opts.max_stack_depth);
        let _ = config.set_max_write(1 << 20);
        let _ = config.set_max_readahead(1 << 20);
        let _ = config.set_max_background(128);
        let _ = config.set_congestion_threshold(96);
        eprintln!("cairnd: init passthrough={}", self.passthrough.load(Ordering::Relaxed));
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _st = stats::start(Op::Lookup);
        let r: Result<NodeInfo, FErrno> = (|| {
            check_name(name)?;
            match name.as_bytes() {
                b"." => {
                    let i = self.core.getattr(parent.0).map_err(|e| if e == Errno::ENOENT { FErrno::ESTALE } else { fe(e) })?;
                    // Export-handle resolution of a removed node: only valid while the kernel
                    // still holds the inode (an open fd pins it); otherwise the handle is stale.
                    if i.nlink == 0 && !self.sh.ktable.contains(i.ino) {
                        return Err(FErrno::ESTALE);
                    }
                    Ok(i)
                }
                b".." => self.parent_of(parent.0).map_err(fe),
                n => self.core.lookup(parent.0, n).map_err(fe),
            }
        })();
        match r {
            Ok(i) => self.reply_entry(&i, reply),
            Err(e) if e == FErrno::ENOENT => {
                stats::bump(Op::LookupNeg);
                reply.entry(&self.sh.opts.neg_ttl, &negative(), Generation(0))
            }
            Err(e) => reply.error(e),
        }
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        let _st = stats::start(Op::Forget);
        self.sh.ktable.forget(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let _st = stats::start(Op::Getattr);
        match self.core.getattr(ino.0) {
            Ok(i) => reply.attr(&self.ttl_for(i.ino), &attr_of(&i)),
            Err(Errno::ENOENT) if fh.is_some() => {
                // fstat of an open directory that was removed/replaced meanwhile.
                let d = fh.and_then(|f| self.dirs.lock().get(&f.0).cloned());
                match d.and_then(|d| d.lock().info.clone()) {
                    Some(mut i) => {
                        i.nlink = 0;
                        reply.attr(&Duration::ZERO, &attr_of(&i))
                    }
                    None => reply.error(FErrno::ENOENT),
                }
            }
            Err(e) => reply.error(fe(e)),
        }
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _st = stats::start(Op::Setattr);
        let origin = self.origin(req);
        let tns = |t: TimeOrNow| match t {
            TimeOrNow::Now => now_ns(),
            TimeOrNow::SpecificTime(t) => ns_of(t),
        };
        let ch = AttrChanges {
            mode: mode.map(|m| m & 0o7777),
            uid,
            gid,
            atime_ns: atime.map(tns),
            mtime_ns: mtime.map(tns),
        };
        let r = self.core.setattr(origin, ino.0, ch, size);
        let info = tryr!(reply, r.map_err(fe));
        // chmod rewrites the access ACL (the kernel leaves that to the filesystem).
        if let (Some(m), Origin::Local { .. }) = (mode, origin) {
            if let Ok(v) = self.core.getxattr(ino.0, acl::ACCESS) {
                if let Some(n) = acl::chmod(&v, m) {
                    if n != v {
                        let _ = self.core.setxattr(origin, ino.0, acl::ACCESS, &n, XattrFlags::Any);
                    }
                }
            }
        }
        reply.attr(&self.ttl_for(info.ino), &attr_of(&info));
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _st = stats::start(Op::Readlink);
        match self.core.getattr(ino.0) {
            Ok(i) => match i.attrs.symlink_target {
                Some(t) => reply.data(&t),
                None => reply.error(FErrno::EINVAL),
            },
            Err(e) => reply.error(fe(e)),
        }
    }

    fn mknod(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, rdev: u32, reply: ReplyEntry) {
        let _st = stats::start(Op::Mknod);
        let Some(kind) = kind_of_mode(mode).filter(|k| *k != Kind::Dir && *k != Kind::Symlink) else {
            return reply.error(FErrno::EINVAL);
        };
        match self.do_create_node(req, parent, name, kind, mode, umask, dec_dev(rdev), None) {
            Ok(i) => self.reply_entry(&i, reply),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let _st = stats::start(Op::Mkdir);
        match self.do_create_node(req, parent, name, Kind::Dir, mode, umask, 0, None) {
            Ok(i) => self.reply_entry(&i, reply),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&self, req: &Request, parent: INodeNo, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
        let _st = stats::start(Op::Symlink);
        let t = target.as_os_str().as_bytes().to_vec();
        if t.len() >= libc::PATH_MAX as usize {
            return reply.error(FErrno::ENAMETOOLONG);
        }
        match self.do_create_node(req, parent, link_name, Kind::Symlink, 0o777, 0, 0, Some(t)) {
            Ok(i) => self.reply_entry(&i, reply),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = stats::start(Op::Unlink);
        match check_name(name).and_then(|_| self.core.unlink(self.origin(req), parent.0, name.as_bytes()).map_err(fe)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = stats::start(Op::Rmdir);
        match check_name(name).and_then(|_| self.core.rmdir(self.origin(req), parent.0, name.as_bytes()).map_err(fe)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _st = stats::start(Op::Rename);
        let r: Result<(), FErrno> = (|| {
            check_name(name)?;
            check_name(newname)?;
            let origin = self.origin(req);
            let wh = flags.contains(RenameFlags::RENAME_WHITEOUT);
            let mode = if flags.contains(RenameFlags::RENAME_EXCHANGE) {
                if wh {
                    return Err(FErrno::EINVAL);
                }
                RenameMode::Exchange
            } else if flags.contains(RenameFlags::RENAME_NOREPLACE) {
                RenameMode::NoReplace
            } else {
                RenameMode::Replace
            };
            self.core
                .rename(origin, parent.0, name.as_bytes(), newparent.0, newname.as_bytes(), mode)
                .map_err(fe)?;
            if wh {
                // overlayfs: leave a 0/0 character device where the source was.
                let new = NewNode {
                    kind: Kind::CharDev,
                    mode: 0,
                    uid: req.uid(),
                    gid: req.gid(),
                    rdev: 0,
                    symlink_target: None,
                    xattrs: vec![],
                };
                self.core.create(origin, parent.0, name.as_bytes(), new).map_err(fe)?;
            }
            Ok(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn link(&self, req: &Request, ino: INodeNo, newparent: INodeNo, newname: &OsStr, reply: ReplyEntry) {
        let _st = stats::start(Op::Link);
        match check_name(newname).and_then(|_| {
            self.core.link(self.origin(req), ino.0, newparent.0, newname.as_bytes()).map_err(fe)
        }) {
            Ok(i) => self.reply_entry(&i, reply),
            Err(e) => reply.error(e),
        }
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _st = stats::start(Op::Open);
        let origin = self.origin(req);
        let r = self.do_open(origin, ino.0, flags.0, |f| reply.open_backing(f));
        let counted = r.is_ok() && origin != Origin::Remote && flags.0 & libc::O_ACCMODE != libc::O_RDONLY;
        match r {
            Ok((fh, Some(b))) => reply.opened_passthrough(FileHandle(fh), FopenFlags::empty(), &b),
            Ok((fh, None)) => reply.opened(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => reply.error(e),
        }
        if counted {
            // Attributes cached before this open may predate writes that bypass FUSE (mmap).
            self.sh.inval_inode(ino.0, -1, 0);
        }
    }

    fn create(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        let _st = stats::start(Op::Create);
        let origin = self.origin(req);
        let r: Result<(NodeInfo, u64, Option<Arc<BackingId>>), FErrno> = (|| {
            let info = match self.do_create_node(req, parent, name, Kind::File, mode, umask, 0, None) {
                Ok(i) => i,
                // Raced with another creator: without O_EXCL this is an open of the existing file.
                Err(e) if e == FErrno::EEXIST && flags & libc::O_EXCL == 0 => {
                    let i = self.core.lookup(parent.0, name.as_bytes()).map_err(fe)?;
                    match i.attrs.kind {
                        Kind::File => i,
                        Kind::Dir => return Err(FErrno::EISDIR),
                        _ => return Err(FErrno::EEXIST),
                    }
                }
                Err(e) => return Err(e),
            };
            let (fh, b) = self.do_open(origin, info.ino, flags & !libc::O_CREAT, |f| reply.open_backing(f))?;
            Ok((info, fh, b))
        })();
        match r {
            Ok((i, fh, b)) => {
                let a = self.entry(&i);
                let ttl = self.ttl_for(i.ino);
                match b {
                    Some(b) => reply.created_passthrough(
                        &ttl,
                        &a,
                        Generation(i.generation),
                        FileHandle(fh),
                        FopenFlags::empty(),
                        &b,
                    ),
                    None => reply.created(
                        &ttl,
                        &a,
                        Generation(i.generation),
                        FileHandle(fh),
                        FopenFlags::FOPEN_KEEP_CACHE,
                    ),
                }
            }
            Err(e) => reply.error(e),
        }
    }

    // read/write are only reached without passthrough.
    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _st = stats::start(Op::Read);
        let h = tryr!(reply, self.handle_file(fh));
        let mut buf = vec![0u8; size as usize];
        match h.file.as_ref().unwrap().read_at(&mut buf, offset) {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => reply.error(fe_io(e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _wf: WriteFlags,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _st = stats::start(Op::Write);
        let h = tryr!(reply, self.handle_file(fh));
        match h.file.as_ref().unwrap().write_at(data, offset) {
            Ok(n) => reply.written(n as u32),
            Err(e) => reply.error(fe_io(e)),
        }
    }

    /// ENOSYS makes the kernel stop sending FLUSH (a synchronous round trip per close()).
    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock: LockOwner, reply: ReplyEmpty) {
        let _st = stats::start(Op::Flush);
        reply.error(FErrno::ENOSYS);
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _st = stats::start(Op::Release);
        let h = self.handles.write().remove(&fh.0);
        let Some(h) = h else { return reply.ok() };
        if !h.remote {
            let (hino, token) = (h.ino, h.token);
            if h.write && !sync_release() && !self.core.sync_on_close(hino) {
                let core = self.core.clone();
                self.rel_pool.submit(Box::new(move || {
                    let _ = core.release(hino, token);
                }));
            } else {
                let _ = self.core.release(hino, token);
            }
        }
        let write = h.write;
        drop(h);
        if write {
            self.writer_close(ino.0);
        }
        {
            let mut m = self.backing_by_ino.lock();
            if m.get(&ino.0).is_some_and(|w| w.strong_count() == 0) {
                m.remove(&ino.0);
            }
        }
        reply.ok();
        // Passthrough writes (and shared mmap) change size/mtime on the live file without the kernel
        // noticing, and Core's release may have published new state: drop cached attributes.
        if write {
            self.sh.inval_inode(ino.0, -1, 0);
        }
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let _st = stats::start(Op::Fsync);
        match self.core.fsync(ino.0, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(fe(e)),
        }
    }

    fn fsyncdir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        let _st = stats::start(Op::Fsyncdir);
        match self.core.sync_all() {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(fe(e)),
        }
    }

    fn fallocate(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, length: u64, mode: i32, reply: ReplyEmpty) {
        let _st = stats::start(Op::Fallocate);
        let h = tryr!(reply, self.handle_file(fh));
        let rc = unsafe { libc::fallocate(h.file.as_ref().unwrap().as_raw_fd(), mode, offset as i64, length as i64) };
        if rc < 0 {
            reply.error(last())
        } else {
            self.sh.inval_inode(h.ino, -1, 0);
            reply.ok()
        }
    }

    fn lseek(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: i64, whence: i32, reply: ReplyLseek) {
        let _st = stats::start(Op::Lseek);
        let h = tryr!(reply, self.handle_file(fh));
        let r = unsafe { libc::lseek(h.file.as_ref().unwrap().as_raw_fd(), offset, whence) };
        if r < 0 {
            reply.error(last())
        } else {
            reply.offset(r)
        }
    }

    fn copy_file_range(
        &self,
        _req: &Request,
        _ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        _ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        let _st = stats::start(Op::CopyFileRange);
        let hi = tryr!(reply, self.handle_file(fh_in));
        let ho = tryr!(reply, self.handle_file(fh_out));
        let (mut oi, mut oo) = (offset_in as i64, offset_out as i64);
        let len = len.min(i32::MAX as u64) as usize;
        let n = unsafe {
            libc::copy_file_range(
                hi.file.as_ref().unwrap().as_raw_fd(),
                &mut oi,
                ho.file.as_ref().unwrap().as_raw_fd(),
                &mut oo,
                len,
                flags.bits() as u32,
            )
        };
        if n < 0 {
            reply.error(last())
        } else {
            self.sh.inval_inode(ho.ino, -1, 0);
            reply.written(n as u32)
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let _st = stats::start(Op::Opendir);
        // The snapshot is taken lazily by the first READDIR (opendir for fchdir/openat needs no listing).
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let info = self.core.getattr(ino.0).ok();
        self.dirs.lock().insert(fh, Arc::new(Mutex::new(DirHandle { ino: ino.0, snap: None, info })));
        // The kernel keeps the listing across opendirs (E11/E12 prototype setting): a warm `git status`
        // otherwise costs 4 requests per directory. Local changes invalidate it in the kernel; remote
        // ones are replayed through the mount by the applier, which also invalidates.
        reply.opened(FileHandle(fh), FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE);
    }

    fn readdir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let _st = stats::start(Op::Readdir);
        let r: Result<(), FErrno> = (|| {
            let (ino, snap) = self.dir_snapshot(fh, offset)?;
            let mut idx = offset as usize;
            loop {
                let (eino, kind, name): (u64, _, &[u8]) = match idx {
                    0 => (ino, fuser::FileType::Directory, b"."),
                    1 => (ino, fuser::FileType::Directory, b".."),
                    n => match snap.entries.get(n - 2) {
                        Some(e) => (e.ino, file_type(e.kind), &e.name),
                        None => return Ok(()),
                    },
                };
                if reply.add(INodeNo(eino), idx as u64 + 1, kind, OsStr::from_bytes(name)) {
                    return Ok(());
                }
                idx += 1;
            }
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn readdirplus(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectoryPlus) {
        let _st = stats::start(Op::Readdirplus);
        let r: Result<(), FErrno> = (|| {
            let (ino, snap) = self.dir_snapshot(fh, offset)?;
            let dir = self.core.getattr(ino).map_err(fe)?;
            let mut idx = offset as usize;
            loop {
                let special = idx < 2;
                let ent = if special { None } else { snap.entries.get(idx - 2) };
                if !special && ent.is_none() {
                    return Ok(());
                }
                if special {
                    // The kernel ignores the attributes of "." / ".." and takes no lookup reference.
                    let mut a = attr_of(&dir);
                    a.kind = fuser::FileType::Directory;
                    let name = if idx == 0 { "." } else { ".." };
                    if reply.add(INodeNo(ino), idx as u64 + 1, name, &self.sh.opts.ttl, &a, Generation(0)) {
                        return Ok(());
                    }
                    idx += 1;
                    continue;
                }
                let e = ent.unwrap();
                match self.core.getattr(e.ino) {
                    Ok(info) => {
                        let a = attr_of(&info);
                        if reply.add(
                            INodeNo(e.ino),
                            idx as u64 + 1,
                            OsStr::from_bytes(&e.name),
                            &self.sh.opts.ttl,
                            &a,
                            Generation(info.generation),
                        ) {
                            return Ok(());
                        }
                        self.sh.ktable.inc(info.ino, info.node);
                    }
                    Err(_) => {} // raced with an unlink: skip
                }
                idx += 1;
            }
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn releasedir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) {
        let _st = stats::start(Op::Releasedir);
        self.dirs.lock().remove(&fh.0);
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let _st = stats::start(Op::Statfs);
        if let Some(p) = &self.sh.opts.statfs_path {
            let c = std::ffi::CString::new(p.as_os_str().as_bytes()).unwrap();
            let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
            if unsafe { libc::statvfs(c.as_ptr(), &mut s) } == 0 {
                return reply.statfs(
                    s.f_blocks,
                    s.f_bfree,
                    s.f_bavail,
                    s.f_files,
                    s.f_ffree,
                    s.f_bsize as u32,
                    255,
                    s.f_frsize as u32,
                );
            }
        }
        // The namespace is not bounded by the local disk: report a large, mostly free volume.
        let blocks = 1u64 << 40;
        reply.statfs(blocks, blocks / 2, blocks / 2, 1 << 32, 1 << 31, 4096, 255, 4096)
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let _st = stats::start(Op::Getxattr);
        self.xattr_reply(self.core.getxattr(ino.0, name.as_bytes()), size, reply)
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let _st = stats::start(Op::Listxattr);
        let r = self.core.listxattr(ino.0).map(|names| {
            let mut v = Vec::new();
            for n in names {
                v.extend_from_slice(&n);
                v.push(0);
            }
            v
        });
        self.xattr_reply(r, size, reply)
    }

    fn setxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, value: &[u8], flags: i32, _position: u32, reply: ReplyEmpty) {
        let _st = stats::start(Op::Setxattr);
        let origin = self.origin(req);
        let r: Result<(), FErrno> = (|| {
            let fl = match flags & (libc::XATTR_CREATE | libc::XATTR_REPLACE) {
                libc::XATTR_CREATE => XattrFlags::Create,
                libc::XATTR_REPLACE => XattrFlags::Replace,
                _ => XattrFlags::Any,
            };
            let n = name.as_bytes();
            if origin != Origin::Remote && (n == acl::ACCESS || n == acl::DEFAULT) {
                return self.set_acl(req, origin, ino.0, n, value, fl);
            }
            self.core.setxattr(origin, ino.0, n, value, fl).map_err(fe)
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = stats::start(Op::Removexattr);
        match self.core.removexattr(self.origin(req), ino.0, name.as_bytes()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(fe(e)),
        }
    }
}

impl CairnFs {
    /// The directory snapshot for an open directory handle; a read at offset 0 (first read or
    /// rewinddir) takes a fresh one.
    fn dir_snapshot(&self, fh: FileHandle, offset: u64) -> Result<(Ino, Arc<DirSnapshot>), FErrno> {
        let d = self.dirs.lock().get(&fh.0).cloned().ok_or(FErrno::EBADF)?;
        let mut d = d.lock();
        if offset == 0 || d.snap.is_none() {
            d.snap = Some(self.core.readdir(d.ino).map_err(fe)?);
        }
        Ok((d.ino, d.snap.clone().unwrap()))
    }

    /// `setxattr(system.posix_acl_*)` from a local process: validate, keep mode and ACL consistent
    /// (the kernel leaves both to the filesystem for FUSE).
    fn set_acl(&self, req: &Request, origin: Origin, ino: Ino, name: &[u8], value: &[u8], fl: XattrFlags) -> Result<(), FErrno> {
        let info = self.core.getattr(ino).map_err(fe)?;
        let entries = acl::parse(value).ok_or(FErrno::EINVAL)?;
        if name == acl::DEFAULT {
            if info.attrs.kind != Kind::Dir {
                return if entries.is_empty() { Ok(()) } else { Err(FErrno::EACCES) };
            }
            if entries.is_empty() {
                return match self.core.removexattr(origin, ino, name) {
                    Err(e) if e != Errno::ENODATA => Err(fe(e)),
                    _ => Ok(()),
                };
            }
            if !acl::valid(&entries) {
                return Err(FErrno::EINVAL);
            }
            return self.core.setxattr(origin, ino, name, value, fl).map_err(fe);
        }
        if entries.is_empty() {
            return match self.core.removexattr(origin, ino, name) {
                Err(e) if e != Errno::ENODATA => Err(fe(e)),
                _ => Ok(()),
            };
        }
        if !acl::valid(&entries) {
            return Err(FErrno::EINVAL);
        }
        let (mut mode, equiv) = acl::equiv_mode(&entries, info.attrs.mode);
        if req.uid() != 0 && !in_group(req.pid(), req.gid(), info.attrs.gid) {
            mode &= !libc::S_ISGID;
        }
        if mode != info.attrs.mode {
            let ch = AttrChanges { mode: Some(mode & 0o7777), ..Default::default() };
            self.core.setattr(origin, ino, ch, None).map_err(fe)?;
        }
        if equiv {
            match self.core.removexattr(origin, ino, name) {
                Err(e) if e != Errno::ENODATA => Err(fe(e)),
                _ => Ok(()),
            }
        } else {
            self.core.setxattr(origin, ino, name, value, fl).map_err(fe)
        }
    }
}
