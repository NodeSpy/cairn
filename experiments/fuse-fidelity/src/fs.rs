//! fd-based loopback filesystem over a backing directory (E15, successor of the E11/E12 path-based version).
//!
//! Every inode the kernel knows about is held as an `O_PATH` fd (like libfuse's passthrough_hp). Operations use
//! `*at(parent_fd, name)` and `AT_EMPTY_PATH` / `/proc/self/fd/N`, never a remembered path, so hard links, renames
//! (including RENAME_EXCHANGE / RENAME_WHITEOUT), unlinked-but-open files and rename races resolve to the right
//! inode. The FUSE nodeid is the backing st_ino (fuser ties nodeid to `attr.ino`); the root is nodeid 1.
//!
//! Faithful forwarding: all xattr namespaces, mknod (devices, fifos, sockets, 0/0 whiteouts), renameat2 flags,
//! fallocate modes, copy_file_range, SEEK_DATA/SEEK_HOLE, utimens, chown, statfs. New files are created with the
//! requester's fsuid/fsgid and umask (per-thread), so ownership, setgid-directory inheritance and default ACLs
//! come from the backing filesystem exactly as they would natively.

use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr};
use std::io;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::*;

use crate::{Opts, Tune};

const ROOT: u64 = 1;

struct Inode {
    /// `O_PATH | O_NOFOLLOW` fd: keeps the backing inode alive even if it is unlinked.
    fd: OwnedFd,
    dev: u64,
    nlookup: AtomicU64,
}

impl Inode {
    fn raw(&self) -> i32 {
        use std::os::fd::AsRawFd;
        self.fd.as_raw_fd()
    }
    fn proc_path(&self) -> CString {
        CString::new(format!("/proc/self/fd/{}", self.raw())).unwrap()
    }
}

pub struct Loopback {
    root_real: u64,
    ttl: Duration,
    passthrough: bool,
    writeback: bool,
    tune: Tune,
    is_root: bool,
    /// Set by main once the session exists; used to drop the kernel's cached attributes (see `release`).
    notifier: Arc<Mutex<Option<Notifier>>>,
    table: RwLock<HashMap<u64, Arc<Inode>>>,
    /// nodeid -> backing `file_handle` bytes (type + payload), kept after FORGET so that kernel-held NFS-style handles
    /// (open_by_handle_at, overlayfs index/nfs_export) can be resolved again via LOOKUP "."/"..". Lost on restart.
    handles: Mutex<HashMap<u64, (i32, Vec<u8>)>>,
    root_fd: i32,
    /// One backing id per inode, shared by all open handles of it (kernel requirement).
    backing_by_ino: Mutex<HashMap<u64, Weak<BackingId>>>,
    /// Keeps each handle's backing id alive until release.
    backing_by_fh: Mutex<HashMap<u64, Arc<BackingId>>>,
    dirs: Mutex<HashMap<u64, Arc<Mutex<DirState>>>>,
}

fn last() -> Errno {
    Errno::from_i32(io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO))
}

/// Run a libc call returning <0 on error; evaluates to the (non-negative) result or returns `Err(Errno)`.
macro_rules! sys {
    ($e:expr) => {{
        let r = unsafe { $e };
        if r < 0 {
            return Err(last());
        }
        r
    }};
}

fn cname(n: &OsStr) -> Result<CString, Errno> {
    CString::new(n.as_bytes()).map_err(|_| Errno::EINVAL)
}

/// Backing `(sec, nsec)` (nsec always >= 0) to the `SystemTime` fuser expects. fuser encodes a pre-1970 time with
/// floor semantics (`time_from_system_time`), so build the true value; its *decode* of request times is not the
/// inverse (see `spec`).
fn ts(sec: i64, nsec: i64) -> SystemTime {
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec as u32)
    } else if nsec == 0 {
        UNIX_EPOCH - Duration::from_secs(sec.unsigned_abs())
    } else {
        UNIX_EPOCH - Duration::new(sec.unsigned_abs() - 1, 1_000_000_000 - nsec as u32)
    }
}

fn kind_of(mode: u32) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

/// The kernel's `new_encode_dev` (what FUSE carries in `rdev`).
fn enc_dev(d: u64) -> u32 {
    let (major, minor) = (libc::major(d) as u64, libc::minor(d) as u64);
    ((minor & 0xff) | (major << 8) | ((minor & !0xff) << 12)) as u32
}
/// The kernel's `new_decode_dev` -> glibc dev_t.
fn dec_dev(d: u32) -> libc::dev_t {
    let d = d as u64;
    let major = (d & 0xfff00) >> 8;
    let minor = (d & 0xff) | ((d >> 12) & 0xfff00);
    libc::makedev(major as u32, minor as u32)
}

fn spec(t: Option<TimeOrNow>) -> libc::timespec {
    match t {
        None => libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
        Some(TimeOrNow::Now) => libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_NOW },
        Some(TimeOrNow::SpecificTime(t)) => match t.duration_since(UNIX_EPOCH) {
            Ok(d) => libc::timespec { tv_sec: d.as_secs() as i64, tv_nsec: d.subsec_nanos() as i64 },
            Err(e) => {
                // fuser 0.18 decodes a wire (sec < 0, nsec) as `UNIX_EPOCH - Duration::new(|sec|, nsec)` (a known
                // TODO there, it should subtract only |sec| - nsec/1e9); undo that to recover the wire values.
                let d = e.duration();
                libc::timespec { tv_sec: -(d.as_secs() as i64), tv_nsec: d.subsec_nanos() as i64 }
            }
        },
    }
}

fn stat_fd(fd: i32) -> Result<libc::stat, Errno> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    sys!(libc::fstatat(fd, c"".as_ptr(), &mut st, libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW));
    Ok(st)
}

fn stat_at(dirfd: i32, name: &CStr) -> Result<libc::stat, Errno> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    sys!(libc::fstatat(dirfd, name.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW));
    Ok(st)
}

thread_local! {
    static THREAD_FS_UNSHARED: Cell<bool> = const { Cell::new(false) };
}

/// While alive: this thread has its own umask (CLONE_FS unshared) set to the request's umask and, when the daemon
/// is root, the requester's fsuid/fsgid, with DAC override kept so supplementary-group access (which the FUSE
/// request does not carry) that the kernel already approved is not re-denied by the backing fs.
struct CredGuard {
    old_umask: libc::mode_t,
    creds: bool,
}

impl CredGuard {
    fn new(fs: &Loopback, req: &Request, umask: u32) -> CredGuard {
        THREAD_FS_UNSHARED.with(|u| {
            if !u.get() {
                unsafe { libc::unshare(libc::CLONE_FS) };
                u.set(true);
            }
        });
        let old_umask = unsafe { libc::umask(umask as libc::mode_t) };
        let creds = fs.is_root;
        if creds {
            unsafe {
                libc::setfsuid(req.uid());
                libc::setfsgid(req.gid());
                // setfsuid(non-root) dropped CAP_DAC_OVERRIDE / CAP_DAC_READ_SEARCH from the effective set.
                #[repr(C)]
                struct Hdr { version: u32, pid: i32 }
                #[repr(C)]
                #[derive(Clone, Copy)]
                struct Data { effective: u32, permitted: u32, inheritable: u32 }
                let mut hdr = Hdr { version: 0x20080522, pid: 0 };
                let mut d = [Data { effective: 0, permitted: 0, inheritable: 0 }; 2];
                if libc::syscall(libc::SYS_capget, &mut hdr, d.as_mut_ptr()) == 0 {
                    d[0].effective |= (1 << 1) | (1 << 2);
                    libc::syscall(libc::SYS_capset, &mut hdr, d.as_ptr());
                }
            }
        }
        CredGuard { old_umask, creds }
    }
}

impl Drop for CredGuard {
    fn drop(&mut self) {
        unsafe {
            if self.creds {
                libc::setfsuid(0); // also restores the effective capability set
                libc::setfsgid(0);
            }
            libc::umask(self.old_umask);
        }
    }
}

impl Loopback {
    pub fn new(root: std::path::PathBuf, o: &Opts) -> Self {
        if o.tune.noopen {
            eprintln!("fuse-fidelity: --tune noopen is not supported");
            std::process::exit(2);
        }
        let c = CString::new(root.as_os_str().as_bytes()).unwrap();
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC) };
        if fd < 0 {
            eprintln!("fuse-fidelity: open backing root: {}", io::Error::last_os_error());
            std::process::exit(2);
        }
        let st = stat_fd(fd).unwrap_or_else(|_| {
            eprintln!("fuse-fidelity: stat backing root");
            std::process::exit(2)
        });
        let mut table = HashMap::new();
        table.insert(ROOT, Arc::new(Inode { fd: unsafe { OwnedFd::from_raw_fd(fd) }, dev: st.st_dev, nlookup: AtomicU64::new(1 << 40) }));
        Self {
            root_real: st.st_ino,
            ttl: o.ttl,
            passthrough: o.passthrough,
            writeback: o.writeback,
            tune: o.tune,
            is_root: unsafe { libc::geteuid() } == 0,
            notifier: Arc::new(Mutex::new(None)),
            table: RwLock::new(table),
            handles: Mutex::default(),
            // open_by_handle_at() needs a regular (non-O_PATH) fd on the filesystem as its mount_fd.
            root_fd: unsafe { libc::open(c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC) },
            backing_by_ino: Mutex::default(),
            backing_by_fh: Mutex::default(),
            dirs: Mutex::default(),
        }
    }

    pub fn notifier_slot(&self) -> Arc<Mutex<Option<Notifier>>> {
        self.notifier.clone()
    }

    fn get(&self, ino: INodeNo) -> Result<Arc<Inode>, Errno> {
        self.table.read().unwrap().get(&ino.0).cloned().ok_or(Errno::ENOENT)
    }

    fn node_of(&self, real_ino: u64) -> u64 {
        if real_ino == self.root_real { ROOT } else { real_ino }
    }

    fn attr(&self, st: &libc::stat) -> FileAttr {
        FileAttr {
            ino: INodeNo(self.node_of(st.st_ino)),
            size: st.st_size as u64,
            blocks: st.st_blocks as u64,
            atime: ts(st.st_atime, st.st_atime_nsec),
            mtime: ts(st.st_mtime, st.st_mtime_nsec),
            ctime: ts(st.st_ctime, st.st_ctime_nsec),
            crtime: UNIX_EPOCH,
            kind: kind_of(st.st_mode),
            perm: (st.st_mode & 0o7777) as u16,
            nlink: st.st_nlink as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            rdev: enc_dev(st.st_rdev),
            blksize: st.st_blksize as u32,
            flags: 0,
        }
    }

    fn negative(&self) -> FileAttr {
        FileAttr {
            ino: INodeNo(0), size: 0, blocks: 0, atime: UNIX_EPOCH, mtime: UNIX_EPOCH, ctime: UNIX_EPOCH, crtime: UNIX_EPOCH,
            kind: FileType::RegularFile, perm: 0, nlink: 0, uid: 0, gid: 0, rdev: 0, blksize: 0, flags: 0,
        }
    }

    /// Increment an inode's kernel lookup count unless it already dropped to zero (being forgotten).
    fn try_inc(i: &Inode) -> bool {
        let mut cur = i.nlookup.load(Ordering::Acquire);
        loop {
            if cur == 0 {
                return false;
            }
            match i.nlookup.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(c) => cur = c,
            }
        }
    }

    /// Look `name` up under `parent`, record a kernel lookup of the result and return its attributes.
    fn lookup_name(&self, parent: &Inode, name: &CStr) -> Result<FileAttr, Errno> {
        // LOOKUP "." on any inode (file handles of regular files too) names the inode itself.
        let is_dot = name.to_bytes() == b".";
        let st = if is_dot { stat_fd(parent.raw())? } else { stat_at(parent.raw(), name)? };
        let node = self.node_of(st.st_ino);
        {
            let t = self.table.read().unwrap();
            if let Some(i) = t.get(&node) {
                if i.dev != st.st_dev {
                    return Err(Errno::EIO); // same ino on another backing device: nodeid == ino cannot represent it
                }
                if Self::try_inc(i) {
                    return Ok(self.attr(&st));
                }
            }
        }
        let fd = if is_dot {
            sys!(libc::fcntl(parent.raw(), libc::F_DUPFD_CLOEXEC, 0))
        } else {
            sys!(libc::openat(parent.raw(), name.as_ptr(), libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC))
        };
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let st = stat_fd(std::os::fd::AsRawFd::as_raw_fd(&fd))?;
        let node = self.node_of(st.st_ino);
        let mut t = self.table.write().unwrap();
        match t.get(&node) {
            Some(i) if i.dev != st.st_dev => return Err(Errno::EIO),
            Some(i) if Self::try_inc(i) => {}
            _ => {
                let raw = std::os::fd::AsRawFd::as_raw_fd(&fd);
                t.insert(node, Arc::new(Inode { fd, dev: st.st_dev, nlookup: AtomicU64::new(1) }));
                drop(t);
                self.remember_handle(node, raw);
            }
        }
        Ok(self.attr(&st))
    }

    fn remember_handle(&self, node: u64, fd: i32) {
        // struct file_handle { u32 handle_bytes; i32 handle_type; u8 f_handle[] }
        let mut buf = [0u8; 8 + 128];
        buf[0..4].copy_from_slice(&128u32.to_ne_bytes());
        let mut mnt = 0i32;
        let rc = unsafe { libc::syscall(libc::SYS_name_to_handle_at, fd, c"".as_ptr(), buf.as_mut_ptr(), &mut mnt, libc::AT_EMPTY_PATH) };
        if rc == 0 {
            let n = u32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize;
            let ty = i32::from_ne_bytes(buf[4..8].try_into().unwrap());
            self.handles.lock().unwrap().insert(node, (ty, buf[8..8 + n].to_vec()));
        }
    }

    /// Re-open an inode we already forgot, from its remembered backing file handle (ESTALE if it is gone).
    fn reopen_by_handle(&self, node: u64) -> Result<Arc<Inode>, Errno> {
        let Some((ty, payload)) = self.handles.lock().unwrap().get(&node).cloned() else {
            crate::stats::trace(|| format!("no remembered handle for node {node}"));
            return Err(Errno::ESTALE);
        };
        let mut buf = Vec::with_capacity(8 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
        buf.extend_from_slice(&ty.to_ne_bytes());
        buf.extend_from_slice(&payload);
        let fd = unsafe { libc::syscall(libc::SYS_open_by_handle_at, self.root_fd, buf.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) } as i32;
        if fd < 0 {
            let e = io::Error::last_os_error();
            crate::stats::trace(|| format!("open_by_handle_at(node {node}) failed: {e}"));
            return Err(Errno::ESTALE);
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let st = stat_fd(std::os::fd::AsRawFd::as_raw_fd(&fd))?;
        Ok(Arc::new(Inode { fd, dev: st.st_dev, nlookup: AtomicU64::new(0) }))
    }

    fn forget_n(&self, node: u64, n: u64) {
        if node == ROOT {
            return;
        }
        let Some(i) = self.table.read().unwrap().get(&node).cloned() else { return };
        let mut cur = i.nlookup.load(Ordering::Acquire);
        let left = loop {
            let new = cur.saturating_sub(n);
            match i.nlookup.compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break new,
                Err(c) => cur = c,
            }
        };
        if left == 0 {
            let mut t = self.table.write().unwrap();
            if let Some(cur) = t.get(&node) {
                if Arc::ptr_eq(cur, &i) && i.nlookup.load(Ordering::Acquire) == 0 {
                    t.remove(&node);
                }
            }
            drop(t);
        }
    }

    fn reply_new(&self, parent: &Inode, name: &CStr) -> Result<FileAttr, Errno> {
        self.lookup_name(parent, name)
    }

    /// Open the backing file of `ino` and, for passthrough, register it with the kernel.
    /// Returns (fd as fh, optional backing id).
    fn open_backing(
        &self,
        ino: INodeNo,
        inode: &Inode,
        flags: i32,
        register: impl FnOnce(&OwnedFd) -> io::Result<BackingId>,
    ) -> io::Result<(u64, Option<Arc<BackingId>>)> {
        let mut fl = flags & !(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC | libc::O_NOFOLLOW | libc::O_DIRECTORY) | libc::O_CLOEXEC;
        if self.writeback {
            // The kernel may read pages of write-only files and handles O_APPEND itself.
            if fl & libc::O_ACCMODE == libc::O_WRONLY {
                fl = fl & !libc::O_ACCMODE | libc::O_RDWR;
            }
        }
        if !self.passthrough {
            // The daemon uses pread/pwrite at kernel-supplied offsets, which O_APPEND would override.
            fl &= !libc::O_APPEND;
        }
        if self.passthrough {
            // The backing file is shared by every handle of the inode, so it must allow writes.
            fl = fl & !libc::O_ACCMODE | libc::O_RDWR;
        }
        let c = inode.proc_path();
        let mut fd = unsafe { libc::open(c.as_ptr(), fl) };
        if fd < 0 && self.passthrough && fl & libc::O_ACCMODE == libc::O_RDWR {
            // e.g. ETXTBSY / read-only: fall back to a read-only backing file
            fd = unsafe { libc::open(c.as_ptr(), fl & !libc::O_ACCMODE | libc::O_RDONLY) };
        }
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let backing = if self.passthrough {
            let mut by_ino = self.backing_by_ino.lock().unwrap();
            let id = match by_ino.get(&ino.0).and_then(Weak::upgrade) {
                Some(id) => id,
                None => {
                    let id = Arc::new(register(&fd)?);
                    by_ino.insert(ino.0, Arc::downgrade(&id));
                    id
                }
            };
            Some(id)
        } else {
            None
        };
        let fh = fd.into_raw_fd() as u64;
        if let Some(b) = &backing {
            self.backing_by_fh.lock().unwrap().insert(fh, b.clone());
        }
        Ok((fh, backing))
    }

    /// fd for a request: the handle's own fd, or (no handle) a temporary one opened through /proc.
    fn fd_for(&self, ino: INodeNo, fh: Option<FileHandle>, acc: i32) -> Result<FdRef, Errno> {
        if let Some(fh) = fh {
            if fh.0 != 0 {
                return Ok(FdRef(fh.0 as i32, false));
            }
        }
        let i = self.get(ino)?;
        let fd = sys!(libc::open(i.proc_path().as_ptr(), acc | libc::O_CLOEXEC | libc::O_NONBLOCK | libc::O_NOCTTY));
        Ok(FdRef(fd, true))
    }

    /// Raw `getdents64` over the backing directory, starting at the backing fs' own `d_off` cookie. Stateless
    /// (no snapshot): concurrent readers and concurrent unlinks see exactly what the backing fs gives them.
    /// Calls `emit(ino, d_off, kind, name)` per entry until it returns true (reply full).
    fn getdents(&self, ino: INodeNo, fh: FileHandle, offset: u64, mut emit: impl FnMut(u64, u64, FileType, &OsStr) -> bool) -> Result<(), Errno> {
        let fd = self.fd_for(ino, Some(fh), libc::O_RDONLY | libc::O_DIRECTORY)?;
        let state = if fh.0 != 0 { Some(self.dirs.lock().unwrap().entry(fh.0).or_default().clone()) } else { None };
        let mut local = DirState::default();
        let mut guard = state.as_ref().map(|s| s.lock().unwrap());
        let st: &mut DirState = match guard.as_mut() {
            Some(g) => g,
            None => &mut local,
        };
        if offset != st.next_off {
            sys!(libc::lseek(fd.raw(), offset as i64, libc::SEEK_SET));
            st.buf.clear();
            st.pos = 0;
            st.next_off = offset;
        }
        loop {
            if st.pos >= st.buf.len() {
                st.buf.resize(32 << 10, 0);
                let n = unsafe { libc::syscall(libc::SYS_getdents64, fd.raw(), st.buf.as_mut_ptr(), st.buf.len()) };
                if n < 0 {
                    st.buf.clear();
                    st.pos = 0;
                    st.next_off = u64::MAX; // position unknown: force a seek next time
                    return Err(last());
                }
                st.buf.truncate(n as usize);
                st.pos = 0;
                if n == 0 {
                    st.next_off = u64::MAX; // EOF: any following request (e.g. rewinddir) must seek to refresh the snapshot
                    return Ok(());
                }
            }
            let pos = st.pos;
            let buf = &st.buf;
            // struct linux_dirent64 { u64 d_ino; i64 d_off; u16 d_reclen; u8 d_type; char d_name[]; }
            let d_ino = u64::from_ne_bytes(buf[pos..pos + 8].try_into().unwrap());
            let d_off = i64::from_ne_bytes(buf[pos + 8..pos + 16].try_into().unwrap());
            let reclen = u16::from_ne_bytes(buf[pos + 16..pos + 18].try_into().unwrap()) as usize;
            let d_type = buf[pos + 18];
            let nm = &buf[pos + 19..pos + reclen];
            let nm = &nm[..nm.iter().position(|&b| b == 0).unwrap_or(nm.len())];
            let kind = match d_type {
                libc::DT_DIR => FileType::Directory,
                libc::DT_LNK => FileType::Symlink,
                libc::DT_BLK => FileType::BlockDevice,
                libc::DT_CHR => FileType::CharDevice,
                libc::DT_FIFO => FileType::NamedPipe,
                libc::DT_SOCK => FileType::Socket,
                _ => FileType::RegularFile, // DT_REG (and DT_UNKNOWN: the backing fs always sets d_type)
            };
            if emit(self.node_of(d_ino), d_off as u64, kind, OsStr::from_bytes(nm)) {
                return Ok(()); // reply full: this entry stays pending in the buffer
            }
            st.pos = pos + reclen;
            st.next_off = d_off as u64;
        }
    }

    fn xattr_reply_size(&self, r: isize, buf: &[u8], size: u32, reply: ReplyXattr) {
        if r < 0 {
            return reply.error(last());
        }
        if size == 0 { reply.size(r as u32) } else { reply.data(&buf[..r as usize]) }
    }
}

/// Per-OPENDIR read-ahead state. `lseek` on a btrfs directory re-snapshots its `last_index`, which makes
/// rename-while-readdir loop forever (generic/736), so we avoid seeking when the kernel continues exactly
/// where the previous reply stopped, and keep the unconsumed tail of the last getdents64 buffer.
struct DirState {
    next_off: u64, // u64::MAX = unknown: the next request seeks (this also refreshes btrfs' snapshot after opendir)
    buf: Vec<u8>,
    pos: usize,
}
impl Default for DirState {
    fn default() -> Self {
        DirState { next_off: u64::MAX, buf: Vec::new(), pos: 0 }
    }
}

/// An fd that is closed on drop only if we opened it for this request.
struct FdRef(i32, bool);
impl FdRef {
    fn raw(&self) -> i32 {
        self.0
    }
}
impl Drop for FdRef {
    fn drop(&mut self) {
        if self.1 {
            unsafe { libc::close(self.0) };
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

impl Filesystem for Loopback {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
        let mut want = vec![
            InitFlags::FUSE_ASYNC_READ,
            InitFlags::FUSE_PARALLEL_DIROPS,
            InitFlags::FUSE_CACHE_SYMLINKS,
            InitFlags::FUSE_MAX_PAGES,
        ];
        if self.tune.rdplus {
            want.push(InitFlags::FUSE_DO_READDIRPLUS);
            want.push(InitFlags::FUSE_READDIRPLUS_AUTO);
        }
        if self.tune.killpriv {
            // fuser 0.18 does not expose FATTR_KILL_SUIDGID, so the daemon cannot honour this: leave it off.
            eprintln!("fuse-fidelity: warning: killpriv requested; fuser cannot implement it, suid/sgid clearing on truncate will break");
            want.push(InitFlags::FUSE_HANDLE_KILLPRIV);
            want.push(InitFlags::FUSE_HANDLE_KILLPRIV_V2);
        }
        if self.tune.explicit {
            want.push(InitFlags::FUSE_EXPLICIT_INVAL_DATA);
        }
        // NFS-style file handles: open_by_handle_at() / overlayfs index+nfs_export resolve a nodeid with LOOKUP "."
        // (handled in `lookup`). Handles only resolve while the inode is in our table (no persistence across restarts).
        want.push(InitFlags::FUSE_EXPORT_SUPPORT);
        if self.tune.acl {
            want.push(InitFlags::FUSE_POSIX_ACL);
            // Without DONT_MASK the kernel applies the umask before we see the mode, which defeats default ACLs
            // (generic/099). We apply the request umask ourselves (CredGuard).
            want.push(InitFlags::FUSE_DONT_MASK);
        }
        if self.passthrough {
            want.push(InitFlags::FUSE_PASSTHROUGH);
        }
        if self.writeback {
            want.push(InitFlags::FUSE_WRITEBACK_CACHE);
        }
        for f in want {
            if let Err(missing) = config.add_capabilities(f) {
                eprintln!("fuse-fidelity: kernel lacks {missing:?}");
                if f == InitFlags::FUSE_PASSTHROUGH {
                    self.passthrough = false;
                }
            }
        }
        // Passthrough needs max_stack_depth >= 1. It must stay at 1: the FUSE sb then has stack depth 1 and
        // overlayfs on top of it has 2 (the kernel maximum). The E11 value of 2 made overlayfs refuse to mount
        // with "maximum fs stacking depth exceeded".
        let depth = std::env::var("LFS_STACK_DEPTH").ok().and_then(|v| v.parse().ok()).unwrap_or(1); // test hook
        let _ = config.set_max_stack_depth(depth);
        let _ = config.set_max_write(1 << 20);
        let _ = config.set_max_readahead(1 << 20);
        let _ = config.set_max_background(128);
        let _ = config.set_congestion_threshold(96);
        eprintln!("fuse-fidelity: init, passthrough={} writeback={} tune={:?}", self.passthrough, self.writeback, self.tune);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Lookup);
        crate::stats::trace(|| format!("LOOKUP {parent:?}/{name:?}"));
        let r = self.get(parent).or_else(|e| {
            // kernel resolving a file handle: LOOKUP "." / ".." on an inode we already forgot
            if matches!(name.as_bytes(), b"." | b"..") { self.reopen_by_handle(parent.0) } else { Err(e) }
        }).and_then(|p| {
            let n = cname(name)?;
            // The root has no visible parent: ".." at the root is the root.
            if parent.0 == ROOT && name.as_bytes() == b".." {
                return self.lookup_name(&p, c".");
            }
            self.lookup_name(&p, &n)
        });
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            // Negative entry: ino 0 makes the kernel cache the absence for `ttl`. Anything that creates the
            // name later goes through the mount (replacing the dentry) or must notify_inval_entry.
            Err(e) if e == Errno::ENOENT && self.tune.neg => {
                crate::stats::bump(crate::stats::Op::LookupNeg);
                reply.entry(&self.ttl, &self.negative(), Generation(0))
            }
            Err(e) => reply.error(e),
        }
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        let _st = crate::stats::start(crate::stats::Op::Forget);
        self.forget_n(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let _st = crate::stats::start(crate::stats::Op::Getattr);
        let r = self.get(ino).and_then(|i| stat_fd(i.raw()));
        match r {
            Ok(st) => reply.attr(&self.ttl, &self.attr(&st)),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _st = crate::stats::start(crate::stats::Op::Setattr);
        let inode = tryr!(reply, self.get(ino));
        let fhfd = fh.filter(|f| f.0 != 0).map(|f| f.0 as i32);
        let r: Result<libc::stat, Errno> = (|| {
            if let Some(m) = mode {
                match fhfd {
                    Some(fd) => sys!(libc::fchmod(fd, m & 0o7777)),
                    None => sys!(libc::chmod(inode.proc_path().as_ptr(), m & 0o7777)),
                };
            }
            if uid.is_some() || gid.is_some() {
                sys!(libc::fchownat(inode.raw(), c"".as_ptr(), uid.unwrap_or(u32::MAX), gid.unwrap_or(u32::MAX), libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW));
            }
            if let Some(sz) = size {
                match fhfd {
                    Some(fd) => sys!(libc::ftruncate(fd, sz as i64)),
                    None => {
                        let fd = sys!(libc::open(inode.proc_path().as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NONBLOCK));
                        let r = unsafe { libc::ftruncate(fd, sz as i64) };
                        let e = last();
                        unsafe { libc::close(fd) };
                        if r < 0 {
                            return Err(e);
                        }
                        r
                    }
                };
            }
            if atime.is_some() || mtime.is_some() {
                let times = [spec(atime), spec(mtime)];
                sys!(libc::utimensat(inode.raw(), c"".as_ptr(), times.as_ptr(), libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW));
            }
            stat_fd(inode.raw())
        })();
        match r {
            Ok(st) => reply.attr(&self.ttl, &self.attr(&st)),
            Err(e) => reply.error(e),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _st = crate::stats::start(crate::stats::Op::Readlink);
        let r: Result<Vec<u8>, Errno> = (|| {
            let i = self.get(ino)?;
            let mut buf = vec![0u8; libc::PATH_MAX as usize + 1];
            let n = sys!(libc::readlinkat(i.raw(), c"".as_ptr(), buf.as_mut_ptr().cast(), buf.len()));
            buf.truncate(n as usize);
            Ok(buf)
        })();
        match r {
            Ok(t) => reply.data(&t),
            Err(e) => reply.error(e),
        }
    }

    fn mknod(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, rdev: u32, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Mknod);
        let r = (|| {
            let p = self.get(parent)?;
            let n = cname(name)?;
            {
                let _g = CredGuard::new(self, req, umask);
                sys!(libc::mknodat(p.raw(), n.as_ptr(), mode as libc::mode_t, dec_dev(rdev)));
            }
            self.reply_new(&p, &n)
        })();
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Mkdir);
        let r = (|| {
            let p = self.get(parent)?;
            let n = cname(name)?;
            {
                let _g = CredGuard::new(self, req, umask);
                sys!(libc::mkdirat(p.raw(), n.as_ptr(), mode as libc::mode_t));
            }
            self.reply_new(&p, &n)
        })();
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Unlink);
        let r = (|| {
            let p = self.get(parent)?;
            let n = cname(name)?;
            sys!(libc::unlinkat(p.raw(), n.as_ptr(), 0));
            Ok::<(), Errno>(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Rmdir);
        let r = (|| {
            let p = self.get(parent)?;
            let n = cname(name)?;
            sys!(libc::unlinkat(p.raw(), n.as_ptr(), libc::AT_REMOVEDIR));
            Ok::<(), Errno>(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&self, req: &Request, parent: INodeNo, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Symlink);
        let r = (|| {
            let p = self.get(parent)?;
            let n = cname(link_name)?;
            let t = CString::new(target.as_os_str().as_bytes()).map_err(|_| Errno::EINVAL)?;
            {
                let _g = CredGuard::new(self, req, 0);
                sys!(libc::symlinkat(t.as_ptr(), p.raw(), n.as_ptr()));
            }
            self.reply_new(&p, &n)
        })();
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn rename(&self, _req: &Request, parent: INodeNo, name: &OsStr, newparent: INodeNo, newname: &OsStr, flags: RenameFlags, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Rename);
        let r = (|| {
            let (p, np) = (self.get(parent)?, self.get(newparent)?);
            let (n, nn) = (cname(name)?, cname(newname)?);
            sys!(libc::syscall(libc::SYS_renameat2, p.raw(), n.as_ptr(), np.raw(), nn.as_ptr(), flags.bits()));
            Ok::<(), Errno>(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn link(&self, _req: &Request, ino: INodeNo, newparent: INodeNo, newname: &OsStr, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Link);
        let r = (|| {
            let i = self.get(ino)?;
            let np = self.get(newparent)?;
            let nn = cname(newname)?;
            // AT_EMPTY_PATH links the inode itself (even a symlink); needs CAP_DAC_READ_SEARCH, else use /proc.
            let mut rc = unsafe { libc::linkat(i.raw(), c"".as_ptr(), np.raw(), nn.as_ptr(), libc::AT_EMPTY_PATH) };
            if rc < 0 && matches!(io::Error::last_os_error().raw_os_error(), Some(libc::ENOENT) | Some(libc::EPERM)) {
                rc = unsafe { libc::linkat(libc::AT_FDCWD, i.proc_path().as_ptr(), np.raw(), nn.as_ptr(), libc::AT_SYMLINK_FOLLOW) };
            }
            if rc < 0 {
                return Err(last());
            }
            self.lookup_name(&np, &nn)
        })();
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _st = crate::stats::start(crate::stats::Op::Open);
        let r = self
            .get(ino)
            .map_err(|e| io::Error::from_raw_os_error(e.into()))
            .and_then(|i| self.open_backing(ino, &i, flags.0, |fd| reply.open_backing(fd)));
        match r {
            Ok((fh, Some(b))) => reply.opened_passthrough(FileHandle(fh), FopenFlags::empty(), &b),
            Ok((fh, None)) => reply.opened(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => reply.error(Errno::from_i32(e.raw_os_error().unwrap_or(libc::EIO))),
        }
    }

    fn create(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        let _st = crate::stats::start(crate::stats::Op::Create);
        let r: Result<(FileAttr, u64, Option<Arc<BackingId>>), Errno> = (|| {
            let p = self.get(parent)?;
            let n = cname(name)?;
            // O_EXCL and friends stay as the caller gave them; the access mode is widened (shared backing file).
            let mut fl = (flags & !libc::O_ACCMODE) | libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC;
            if !self.passthrough {
                fl &= !libc::O_APPEND;
            }
            let fd = {
                let _g = CredGuard::new(self, req, umask);
                sys!(libc::openat(p.raw(), n.as_ptr(), fl, mode as libc::mode_t))
            };
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let attr = self.lookup_name(&p, &n)?;
            if self.tune.create1 && self.passthrough && !self.writeback {
                // One open: hand the creating fd itself to the kernel.
                let id = {
                    let mut by_ino = self.backing_by_ino.lock().unwrap();
                    let id = match by_ino.get(&attr.ino.0).and_then(Weak::upgrade) {
                        Some(id) => id,
                        None => {
                            let id = Arc::new(reply.open_backing(&fd).map_err(|e| Errno::from_i32(e.raw_os_error().unwrap_or(libc::EIO)))?);
                            by_ino.insert(attr.ino.0, Arc::downgrade(&id));
                            id
                        }
                    };
                    id
                };
                let fh = fd.into_raw_fd() as u64;
                self.backing_by_fh.lock().unwrap().insert(fh, id.clone());
                return Ok((attr, fh, Some(id)));
            }
            drop(fd);
            let i = self.get(attr.ino)?;
            let oflags = flags & !(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC);
            let (fh, b) = self
                .open_backing(attr.ino, &i, oflags, |fd| reply.open_backing(fd))
                .map_err(|e| Errno::from_i32(e.raw_os_error().unwrap_or(libc::EIO)))?;
            Ok((attr, fh, b))
        })();
        match r {
            Ok((a, fh, Some(b))) => reply.created_passthrough(&self.ttl, &a, Generation(0), FileHandle(fh), FopenFlags::empty(), &b),
            Ok((a, fh, None)) => reply.created(&self.ttl, &a, Generation(0), FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => reply.error(e),
        }
    }

    // read/write are only reached without passthrough.
    fn read(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData) {
        let _st = crate::stats::start(crate::stats::Op::Read);
        let mut buf = vec![0u8; size as usize];
        let tmp = tryr!(reply, self.fd_for(ino, Some(fh), libc::O_RDONLY));
        let n = unsafe { libc::pread(tmp.raw(), buf.as_mut_ptr().cast(), buf.len(), offset as i64) };
        if n < 0 { reply.error(last()) } else { reply.data(&buf[..n as usize]) }
    }

    fn write(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, data: &[u8], _wf: WriteFlags, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyWrite) {
        let _st = crate::stats::start(crate::stats::Op::Write);
        let tmp = tryr!(reply, self.fd_for(ino, Some(fh), libc::O_RDWR));
        let n = unsafe { libc::pwrite(tmp.raw(), data.as_ptr().cast(), data.len(), offset as i64) };
        if n < 0 { reply.error(last()) } else { reply.written(n as u32) }
    }

    /// ENOSYS makes the kernel stop sending FLUSH, saving a synchronous round trip on every close().
    /// Dirty pages are still written back before close returns; locks are kernel-local, so we have nothing to drop.
    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock: LockOwner, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Flush);
        reply.error(Errno::ENOSYS);
    }

    fn release(&self, _req: &Request, ino: INodeNo, fh: FileHandle, flags: OpenFlags, _lock: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Release);
        self.backing_by_fh.lock().unwrap().remove(&fh.0);
        unsafe { libc::close(fh.0 as i32) };
        reply.ok();
        // Passthrough I/O (and above all shared mmap writes) changes mtime/ctime/size on the backing inode without
        // the kernel noticing, and our attributes are cached for `ttl`. Drop the cached attributes when a writable
        // handle goes away so the next stat() sees them (xfstests generic/080, generic/215).
        if self.passthrough && flags.0 & libc::O_ACCMODE != libc::O_RDONLY {
            if let Some(n) = self.notifier.lock().unwrap().as_ref() {
                let _ = n.inval_inode(ino, -1, 0);
            }
        }
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fsync);
        let tmp = tryr!(reply, self.fd_for(ino, Some(fh), libc::O_RDONLY));
        let rc = unsafe { if datasync { libc::fdatasync(tmp.raw()) } else { libc::fsync(tmp.raw()) } };
        if rc < 0 { reply.error(last()) } else { reply.ok() }
    }

    fn fsyncdir(&self, _req: &Request, ino: INodeNo, _fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fsyncdir);
        // With OPENDIR disabled the handle is 0 and fd_for opens the directory through /proc.
        let tmp = tryr!(reply, self.fd_for(ino, None, libc::O_RDONLY | libc::O_DIRECTORY));
        let rc = unsafe { if datasync { libc::fdatasync(tmp.raw()) } else { libc::fsync(tmp.raw()) } };
        if rc < 0 { reply.error(last()) } else { reply.ok() }
    }

    fn fallocate(&self, req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, length: u64, mode: i32, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fallocate);
        let tmp = tryr!(reply, self.fd_for(ino, Some(fh), libc::O_RDWR));
        // As the requester, so the backing fs drops suid/sgid exactly as for a native fallocate (generic/683).
        let _g = CredGuard::new(self, req, 0o000);
        let rc = unsafe { libc::fallocate(tmp.raw(), mode, offset as i64, length as i64) };
        if rc < 0 { reply.error(last()) } else { reply.ok() }
    }

    fn lseek(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: i64, whence: i32, reply: ReplyLseek) {
        let _st = crate::stats::start(crate::stats::Op::Lseek);
        let tmp = tryr!(reply, self.fd_for(ino, Some(fh), libc::O_RDONLY));
        let r = unsafe { libc::lseek(tmp.raw(), offset, whence) };
        if r < 0 { reply.error(last()) } else { reply.offset(r) }
    }

    fn copy_file_range(
        &self,
        req: &Request,
        ino_in: INodeNo,
        fh_in: FileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        fh_out: FileHandle,
        offset_out: u64,
        len: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        let _st = crate::stats::start(crate::stats::Op::CopyFileRange);
        let fin = tryr!(reply, self.fd_for(ino_in, Some(fh_in), libc::O_RDONLY));
        let fout = tryr!(reply, self.fd_for(ino_out, Some(fh_out), libc::O_RDWR));
        let _g = CredGuard::new(self, req, 0o000);
        let (mut oi, mut oo) = (offset_in as i64, offset_out as i64);
        let len = len.min(i32::MAX as u64) as usize;
        let n = unsafe { libc::copy_file_range(fin.raw(), &mut oi, fout.raw(), &mut oo, len, flags.bits() as u32) };
        if n < 0 { reply.error(last()) } else { reply.written(n as u32) }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let _st = crate::stats::start(crate::stats::Op::Opendir);
        if self.tune.nodir {
            // The kernel remembers ENOSYS: no more OPENDIR/RELEASEDIR; directories default to cached readdir.
            return reply.error(Errno::ENOSYS);
        }
        let r = (|| {
            let d = self.get(ino)?;
            let fd = sys!(libc::open(d.proc_path().as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC));
            Ok::<i32, Errno>(fd)
        })();
        match r {
            Ok(fd) => reply.opened(FileHandle(fd as u64), if self.tune.dircache { FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE } else { FopenFlags::empty() }),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let _st = crate::stats::start(crate::stats::Op::Readdir);
        let r = self.getdents(ino, fh, offset, |i, off, kind, name| reply.add(INodeNo(i), off, kind, name));
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn readdirplus(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectoryPlus) {
        let _st = crate::stats::start(crate::stats::Op::Readdirplus);
        let dir = tryr!(reply, self.get(ino));
        let r = self.getdents(ino, fh, offset, |i, off, _kind, name| {
            let n = name.as_bytes();
            if n == b"." || n == b".." {
                // The kernel ignores the attributes of "." / ".." and takes no lookup reference.
                let Ok(st) = stat_at(dir.raw(), if n == b"." { c"." } else { c".." }) else { return false };
                let mut attr = self.attr(&st);
                attr.ino = INodeNo(i);
                return reply.add(attr.ino, off, name, &self.ttl, &attr, Generation(0));
            }
            let Ok(cn) = cname(name) else { return false };
            let Ok(attr) = self.lookup_name(&dir, &cn) else { return false }; // raced with an unlink
            if reply.add(attr.ino, off, name, &self.ttl, &attr, Generation(0)) {
                self.forget_n(attr.ino.0, 1); // did not fit: the kernel will not know about it
                return true;
            }
            false
        });
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn releasedir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Releasedir);
        if fh.0 != 0 {
            self.dirs.lock().unwrap().remove(&fh.0);
            unsafe { libc::close(fh.0 as i32) };
        }
        reply.ok();
    }

    fn statfs(&self, _req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        let _st = crate::stats::start(crate::stats::Op::Statfs);
        let i = tryr!(reply, self.get(ino));
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(i.proc_path().as_ptr(), &mut s) } < 0 {
            return reply.error(last());
        }
        reply.statfs(s.f_blocks, s.f_bfree, s.f_bavail, s.f_files, s.f_ffree, s.f_bsize as u32, s.f_namemax as u32, s.f_frsize as u32)
    }

    fn getxattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let _st = crate::stats::start(crate::stats::Op::Getxattr);
        let i = tryr!(reply, self.get(ino));
        let n = tryr!(reply, cname(name));
        let mut buf = vec![0u8; size as usize];
        let p = i.proc_path();
        let r = unsafe { libc::getxattr(p.as_ptr(), n.as_ptr(), buf.as_mut_ptr().cast(), size as usize) };
        self.xattr_reply_size(r, &buf, size, reply)
    }

    fn listxattr(&self, _req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let _st = crate::stats::start(crate::stats::Op::Listxattr);
        let i = tryr!(reply, self.get(ino));
        let mut buf = vec![0u8; size as usize];
        let p = i.proc_path();
        let r = unsafe { libc::listxattr(p.as_ptr(), buf.as_mut_ptr().cast(), size as usize) };
        self.xattr_reply_size(r, &buf, size, reply)
    }

    fn setxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, value: &[u8], flags: i32, _position: u32, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Setxattr);
        let r = (|| {
            let i = self.get(ino)?;
            let n = cname(name)?;
            // fuser 0.18 does not implement FUSE_SETXATTR_EXT / ACL_KILL_SGID, so the daemon cannot be told to clear
            // SGID when a non-member sets an access ACL. Do the ACL write as the requester instead: the backing fs
            // (posix_acl_update_mode) then applies the kernel's own rule with the requester's credentials.
            let _g = (name.as_bytes() == b"system.posix_acl_access").then(|| CredGuard::new(self, req, 0o000));
            sys!(libc::setxattr(i.proc_path().as_ptr(), n.as_ptr(), value.as_ptr().cast(), value.len(), flags));
            Ok::<(), Errno>(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn removexattr(&self, _req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Removexattr);
        let r = (|| {
            let i = self.get(ino)?;
            let n = cname(name)?;
            sys!(libc::removexattr(i.proc_path().as_ptr(), n.as_ptr()));
            Ok::<(), Errno>(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }
}
