//! Path-based loopback filesystem over a backing directory.
//!
//! Inode numbers are the backing inode numbers (root is 1). Each live inode keeps one path (the one it
//! was last looked up under) and a kernel lookup count. This is deliberately simple: hard links and
//! renames racing with lookups can resolve a stale path. The real client will key by its own inode table.

use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::fs::Metadata;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirEntryExt, FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::*;

use crate::{Opts, Tune};

const ROOT: u64 = 1;
/// Key bit for stateless (no-OPENDIR) directory snapshots, which are keyed by inode instead of file handle.
const STATELESS: u64 = 1 << 63;

struct Inode {
    path: PathBuf,
    nlookup: u64,
}

struct DirEnt {
    ino: u64,
    kind: FileType,
    name: OsString,
}

pub struct Loopback {
    root: PathBuf,
    root_real: u64,
    ttl: Duration,
    passthrough: bool,
    writeback: bool,
    tune: Tune,
    inodes: RwLock<HashMap<u64, Inode>>,
    dirs: Mutex<HashMap<u64, Arc<Vec<DirEnt>>>>,
    next_dir_fh: AtomicU64,
    /// One backing id per inode, shared by all open handles of it (kernel requirement).
    backing_by_ino: Mutex<HashMap<u64, Weak<BackingId>>>,
    /// Keeps each handle's backing id alive until release.
    backing_by_fh: Mutex<HashMap<u64, Arc<BackingId>>>,
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

fn errno(e: io::Error) -> Errno {
    Errno::from_i32(e.raw_os_error().unwrap_or(libc::EIO))
}

fn cstr(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

fn cvt(r: libc::c_int) -> io::Result<()> {
    if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

fn fstat_attr(fs: &Loopback, st: &libc::stat) -> FileAttr {
    let ino = if st.st_ino == fs.root_ino() { ROOT } else { st.st_ino };
    FileAttr {
        ino: INodeNo(ino),
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
        rdev: st.st_rdev as u32,
        blksize: st.st_blksize as u32,
        flags: 0,
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

fn ts(sec: i64, nsec: i64) -> SystemTime {
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec as u32)
    } else {
        UNIX_EPOCH - Duration::from_secs(sec.unsigned_abs())
    }
}

fn spec(t: Option<TimeOrNow>) -> libc::timespec {
    match t {
        None => libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
        Some(TimeOrNow::Now) => libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_NOW },
        Some(TimeOrNow::SpecificTime(t)) => match t.duration_since(UNIX_EPOCH) {
            Ok(d) => libc::timespec { tv_sec: d.as_secs() as i64, tv_nsec: d.subsec_nanos() as i64 },
            Err(_) => libc::timespec { tv_sec: 0, tv_nsec: 0 },
        },
    }
}

impl Loopback {
    pub fn new(root: PathBuf, o: &Opts) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(ROOT, Inode { path: root.clone(), nlookup: 1 });
        Self {
            root_real: std::fs::symlink_metadata(&root).map(|m| m.ino()).expect("stat backing root"),
            root,
            ttl: o.ttl,
            // No-OPEN mode cannot use passthrough: a passthrough-created file makes any later open of the inode
            // (which the kernel now serves without OPEN, as a plain cached file) fail with EIO.
            passthrough: o.passthrough && !o.tune.noopen,
            writeback: o.writeback,
            tune: o.tune,
            inodes: RwLock::new(inodes),
            dirs: Mutex::default(),
            next_dir_fh: AtomicU64::new(1),
            backing_by_ino: Mutex::default(),
            backing_by_fh: Mutex::default(),
        }
    }

    fn path(&self, ino: INodeNo) -> Result<PathBuf, Errno> {
        self.inodes.read().unwrap().get(&ino.0).map(|i| i.path.clone()).ok_or(Errno::ENOENT)
    }

    fn child(&self, parent: INodeNo, name: &OsStr) -> Result<PathBuf, Errno> {
        Ok(self.path(parent)?.join(name))
    }

    fn attr(&self, md: &Metadata) -> FileAttr {
        let ino = if md.ino() == self.root_ino() { ROOT } else { md.ino() };
        FileAttr {
            ino: INodeNo(ino),
            size: md.size(),
            blocks: md.blocks(),
            atime: ts(md.atime(), md.atime_nsec()),
            mtime: ts(md.mtime(), md.mtime_nsec()),
            ctime: ts(md.ctime(), md.ctime_nsec()),
            crtime: UNIX_EPOCH,
            kind: kind_of(md.mode()),
            perm: (md.mode() & 0o7777) as u16,
            nlink: md.nlink() as u32,
            uid: md.uid(),
            gid: md.gid(),
            rdev: md.rdev() as u32,
            blksize: md.blksize() as u32,
            flags: 0,
        }
    }

    fn negative(&self) -> FileAttr {
        FileAttr {
            ino: INodeNo(0), size: 0, blocks: 0, atime: UNIX_EPOCH, mtime: UNIX_EPOCH, ctime: UNIX_EPOCH, crtime: UNIX_EPOCH,
            kind: FileType::RegularFile, perm: 0, nlink: 0, uid: 0, gid: 0, rdev: 0, blksize: 0, flags: 0,
        }
    }

    fn root_ino(&self) -> u64 {
        self.root_real
    }

    /// Record a kernel lookup of `path` and return its attributes.
    fn lookup_path(&self, path: PathBuf) -> Result<FileAttr, Errno> {
        let md = std::fs::symlink_metadata(&path).map_err(errno)?;
        let attr = self.attr(&md);
        self.add_lookup(attr.ino.0, path);
        Ok(attr)
    }

    fn add_lookup(&self, ino: u64, path: PathBuf) {
        if ino == ROOT {
            self.inodes.write().unwrap().get_mut(&ROOT).unwrap().nlookup += 1;
            return;
        }
        let mut t = self.inodes.write().unwrap();
        let e = t.entry(ino).or_insert(Inode { path: PathBuf::new(), nlookup: 0 });
        e.path = path;
        e.nlookup += 1;
    }

    fn chown_new(&self, path: &Path, req: &Request) {
        if let Ok(c) = cstr(path) {
            unsafe { libc::lchown(c.as_ptr(), req.uid(), req.gid()) };
        }
    }

    fn reply_new(&self, path: PathBuf, req: &Request, reply: ReplyEntry) {
        self.chown_new(&path, req);
        match self.lookup_path(path) {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    /// Open the backing file and, for passthrough, register it with the kernel.
    /// Returns (fd as fh, optional backing id).
    fn open_backing(
        &self,
        ino: INodeNo,
        path: &Path,
        flags: i32,
        register: impl FnOnce(&OwnedFd) -> io::Result<BackingId>,
    ) -> io::Result<(u64, Option<Arc<BackingId>>)> {
        let mut fl = flags & !(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC | libc::O_NOFOLLOW) | libc::O_CLOEXEC;
        if self.writeback {
            // The kernel may read pages of write-only files and handles O_APPEND itself.
            if fl & libc::O_ACCMODE == libc::O_WRONLY {
                fl = fl & !libc::O_ACCMODE | libc::O_RDWR;
            }
            fl &= !libc::O_APPEND;
        }
        if self.passthrough {
            // The backing file is shared by every handle of the inode, so it must allow writes.
            fl = fl & !libc::O_ACCMODE | libc::O_RDWR;
        }
        let c = cstr(path)?;
        let mut fd = unsafe { libc::open(c.as_ptr(), fl) };
        if fd < 0 && self.passthrough && fl & libc::O_ACCMODE == libc::O_RDWR {
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

    fn read_dir(&self, path: &Path, ino: u64) -> io::Result<Vec<DirEnt>> {
        let parent_ino = if ino == ROOT {
            ROOT
        } else {
            let md = std::fs::symlink_metadata(path.parent().unwrap_or(path))?;
            if md.ino() == self.root_ino() { ROOT } else { md.ino() }
        };
        let mut v = vec![
            DirEnt { ino, kind: FileType::Directory, name: ".".into() },
            DirEnt { ino: parent_ino, kind: FileType::Directory, name: "..".into() },
        ];
        for e in std::fs::read_dir(path)? {
            let e = e?;
            let ft = e.file_type()?;
            let kind = if ft.is_dir() {
                FileType::Directory
            } else if ft.is_symlink() {
                FileType::Symlink
            } else if ft.is_block_device() {
                FileType::BlockDevice
            } else if ft.is_char_device() {
                FileType::CharDevice
            } else if ft.is_fifo() {
                FileType::NamedPipe
            } else if ft.is_socket() {
                FileType::Socket
            } else {
                FileType::RegularFile
            };
            v.push(DirEnt { ino: e.ino(), kind, name: e.file_name() });
        }
        Ok(v)
    }

    /// fd for a request: the handle's own fd, or (no-OPEN mode, fh 0) a temporary one opened by path.
    fn fd_for(&self, ino: INodeNo, fh: FileHandle, acc: i32) -> io::Result<FdRef> {
        if fh.0 != 0 {
            return Ok(FdRef(fh.0 as i32, false));
        }
        let p = self.path(ino).map_err(|e| io::Error::from_raw_os_error(e.into()))?;
        let c = cstr(&p)?;
        let fd = unsafe { libc::open(c.as_ptr(), acc | libc::O_CLOEXEC | libc::O_NOFOLLOW) };
        if fd < 0 { Err(io::Error::last_os_error()) } else { Ok(FdRef(fd, true)) }
    }

    /// Directory entries for a readdir request. With an OPENDIR handle the snapshot was taken at open; without
    /// one (no-OPENDIR mode) it is taken at offset 0 and kept per inode until EOF or FORGET.
    fn dir_entries(&self, ino: INodeNo, fh: FileHandle, offset: u64) -> Result<Arc<Vec<DirEnt>>, Errno> {
        if fh.0 != 0 {
            return self.dir_snapshot(fh).ok_or(Errno::EBADF);
        }
        let key = ino.0 | STATELESS;
        if offset != 0 {
            let cached = self.dirs.lock().unwrap().get(&key).cloned();
            if let Some(d) = cached {
                if offset as usize >= d.len() {
                    self.dirs.lock().unwrap().remove(&key); // EOF request: snapshot no longer needed
                }
                return Ok(d);
            }
        }
        let p = self.path(ino)?;
        let v = Arc::new(self.read_dir(&p, ino.0).map_err(errno)?);
        self.dirs.lock().unwrap().insert(key, v.clone());
        Ok(v)
    }

    fn dir_snapshot(&self, fh: FileHandle) -> Option<Arc<Vec<DirEnt>>> {
        self.dirs.lock().unwrap().get(&fh.0).cloned()
    }
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
            want.push(InitFlags::FUSE_HANDLE_KILLPRIV);
            want.push(InitFlags::FUSE_HANDLE_KILLPRIV_V2);
        }
        if self.tune.explicit {
            want.push(InitFlags::FUSE_EXPLICIT_INVAL_DATA);
        }
        if self.passthrough {
            want.push(InitFlags::FUSE_PASSTHROUGH);
        }
        if self.writeback {
            want.push(InitFlags::FUSE_WRITEBACK_CACHE);
        }
        for f in want {
            if let Err(missing) = config.add_capabilities(f) {
                eprintln!("fuse-pt: kernel lacks {missing:?}");
                if f == InitFlags::FUSE_PASSTHROUGH {
                    self.passthrough = false;
                }
            }
        }
        // Allow one more stacked fs below us (e.g. overlayfs); btrfs itself has depth 0.
        let _ = config.set_max_stack_depth(2);
        let _ = config.set_max_write(1 << 20);
        let _ = config.set_max_readahead(1 << 20);
        let _ = config.set_max_background(128);
        let _ = config.set_congestion_threshold(96);
        eprintln!("fuse-pt: init, passthrough={} writeback={} tune={:?}", self.passthrough, self.writeback, self.tune);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Lookup);
        crate::stats::trace(|| format!("LOOKUP {:?}/{:?}", self.path(parent), name));
        match self.child(parent, name).and_then(|p| self.lookup_path(p)) {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            // Negative entry: ino 0 makes the kernel cache the absence for `ttl`. Anything that creates the
            // name later goes through the mount (replacing the dentry) or must notify_inval_entry.
            Err(e) if e == Errno::ENOENT && self.tune.neg => { crate::stats::bump(crate::stats::Op::LookupNeg); reply.entry(&self.ttl, &self.negative(), Generation(0)) }
            Err(e) => reply.error(e),
        }
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        let _st = crate::stats::start(crate::stats::Op::Forget);
        if ino.0 == ROOT {
            return;
        }
        let mut t = self.inodes.write().unwrap();
        if let Some(e) = t.get_mut(&ino.0) {
            e.nlookup = e.nlookup.saturating_sub(nlookup);
            if e.nlookup == 0 {
                t.remove(&ino.0);
                drop(t);
                if self.tune.nodir {
                    self.dirs.lock().unwrap().remove(&(ino.0 | STATELESS));
                }
            }
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, fh: Option<FileHandle>, reply: ReplyAttr) {
        let _st = crate::stats::start(crate::stats::Op::Getattr);
        crate::stats::trace(|| format!("GETATTR {:?} fh={}", self.path(ino), fh.is_some()));
        let r = self.path(ino).and_then(|p| std::fs::symlink_metadata(p).map_err(errno));
        match r {
            Ok(md) => reply.attr(&self.ttl, &self.attr(&md)),
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
        let path = match self.path(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let c = match cstr(&path) {
            Ok(c) => c,
            Err(e) => return reply.error(errno(e)),
        };
        let r: io::Result<()> = (|| {
            if let Some(m) = mode {
                cvt(unsafe { libc::chmod(c.as_ptr(), m & 0o7777) })?;
            }
            if uid.is_some() || gid.is_some() {
                cvt(unsafe { libc::lchown(c.as_ptr(), uid.unwrap_or(u32::MAX), gid.unwrap_or(u32::MAX)) })?;
            }
            if let Some(sz) = size {
                match fh {
                    // fh 0 = no-OPEN mode (no handle): truncate by path
                    Some(fh) if fh.0 != 0 => cvt(unsafe { libc::ftruncate(fh.0 as i32, sz as i64) })?,
                    _ => cvt(unsafe { libc::truncate(c.as_ptr(), sz as i64) })?,
                }
            }
            if atime.is_some() || mtime.is_some() {
                let times = [spec(atime), spec(mtime)];
                cvt(unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), libc::AT_SYMLINK_NOFOLLOW) })?;
            }
            Ok(())
        })();
        match r.and_then(|_| std::fs::symlink_metadata(&path)) {
            Ok(md) => reply.attr(&self.ttl, &self.attr(&md)),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _st = crate::stats::start(crate::stats::Op::Readlink);
        match self.path(ino).and_then(|p| std::fs::read_link(p).map_err(errno)) {
            Ok(t) => reply.data(t.as_os_str().as_bytes()),
            Err(e) => reply.error(e),
        }
    }

    fn mknod(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, rdev: u32, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Mknod);
        let r = self.child(parent, name).and_then(|p| {
            let c = cstr(&p).map_err(errno)?;
            cvt(unsafe { libc::mknod(c.as_ptr(), mode & !umask, rdev as libc::dev_t) }).map_err(errno)?;
            Ok(p)
        });
        match r {
            Ok(p) => self.reply_new(p, req, reply),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Mkdir);
        let r = self.child(parent, name).and_then(|p| {
            let c = cstr(&p).map_err(errno)?;
            cvt(unsafe { libc::mkdir(c.as_ptr(), mode & !umask) }).map_err(errno)?;
            Ok(p)
        });
        match r {
            Ok(p) => self.reply_new(p, req, reply),
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Unlink);
        match self.child(parent, name).and_then(|p| std::fs::remove_file(p).map_err(errno)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Rmdir);
        match self.child(parent, name).and_then(|p| std::fs::remove_dir(p).map_err(errno)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&self, req: &Request, parent: INodeNo, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Symlink);
        match self.child(parent, link_name).and_then(|p| std::os::unix::fs::symlink(target, &p).map(|_| p).map_err(errno)) {
            Ok(p) => self.reply_new(p, req, reply),
            Err(e) => reply.error(e),
        }
    }

    fn rename(&self, _req: &Request, parent: INodeNo, name: &OsStr, newparent: INodeNo, newname: &OsStr, flags: RenameFlags, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Rename);
        let r: Result<(), Errno> = (|| {
            let from = self.child(parent, name)?;
            let to = self.child(newparent, newname)?;
            let md = std::fs::symlink_metadata(&from).map_err(errno)?;
            let (moved, moved_is_dir) = (md.ino(), md.is_dir());
            let (cf, ct) = (cstr(&from).map_err(errno)?, cstr(&to).map_err(errno)?);
            let rc = unsafe { libc::syscall(libc::SYS_renameat2, libc::AT_FDCWD, cf.as_ptr(), libc::AT_FDCWD, ct.as_ptr(), flags.bits()) };
            cvt(rc as i32).map_err(errno)?;
            let mut t = self.inodes.write().unwrap();
            let moved = if moved == self.root_ino() { ROOT } else { moved };
            // Re-root the moved inode and, for a directory, anything cached below it. The scan is O(table), so
            // skip it for non-directories (it cost 2-3 ms per rename with 7000 cached inodes).
            if moved_is_dir {
                for e in t.values_mut() {
                    if let Ok(rest) = e.path.strip_prefix(&from) {
                        e.path = to.join(rest);
                    }
                }
            }
            if let Some(e) = t.get_mut(&moved) {
                e.path = to;
            }
            Ok(())
        })();
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn link(&self, _req: &Request, ino: INodeNo, newparent: INodeNo, newname: &OsStr, reply: ReplyEntry) {
        let _st = crate::stats::start(crate::stats::Op::Link);
        let r = self.path(ino).and_then(|old| {
            let new = self.child(newparent, newname)?;
            let (co, cn) = (cstr(&old).map_err(errno)?, cstr(&new).map_err(errno)?);
            cvt(unsafe { libc::link(co.as_ptr(), cn.as_ptr()) }).map_err(errno)?;
            self.lookup_path(new)
        });
        match r {
            Ok(a) => reply.entry(&self.ttl, &a, Generation(0)),
            Err(e) => reply.error(e),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _st = crate::stats::start(crate::stats::Op::Open);
        if self.tune.noopen {
            // The kernel remembers ENOSYS: no more OPEN, and no RELEASE for any file.
            return reply.error(Errno::ENOSYS);
        }
        let r = self.path(ino).map_err(|e| io::Error::from_raw_os_error(e.into())).and_then(|p| {
            self.open_backing(ino, &p, flags.0, |fd| reply.open_backing(fd))
        });
        match r {
            Ok((fh, Some(b))) => reply.opened_passthrough(FileHandle(fh), FopenFlags::empty(), &b),
            Ok((fh, None)) => reply.opened(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn create(&self, req: &Request, parent: INodeNo, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        let _st = crate::stats::start(crate::stats::Op::Create);
        let r: io::Result<(FileAttr, u64, Option<Arc<BackingId>>)> = (|| {
            let p = self.child(parent, name).map_err(|e| io::Error::from_raw_os_error(e.into()))?;
            let c = cstr(&p)?;
            if self.tune.noopen {
                // Kernel no_open mode: no RELEASE ever arrives, so hand out no fd (fh 0; later I/O opens by path).
                let fl = (flags & !libc::O_ACCMODE) | libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC;
                let fd = unsafe { libc::open(c.as_ptr(), fl, mode & !umask) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };
                unsafe { libc::fchown(fd.as_raw_fd(), req.uid(), req.gid()) };
                let mut st: libc::stat = unsafe { std::mem::zeroed() };
                cvt(unsafe { libc::fstat(fd.as_raw_fd(), &mut st) })?;
                let md = fstat_attr(self, &st);
                self.add_lookup(md.ino.0, p);
                return Ok((md, 0, None));
            }
            if self.tune.create1 && self.passthrough && !self.writeback {
                // One open: create, chown and stat through the same fd, then hand that fd to the kernel.
                let fl = (flags & !libc::O_ACCMODE) | libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC;
                let fd = unsafe { libc::open(c.as_ptr(), fl, mode & !umask) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };
                unsafe { libc::fchown(fd.as_raw_fd(), req.uid(), req.gid()) };
                let mut st: libc::stat = unsafe { std::mem::zeroed() };
                cvt(unsafe { libc::fstat(fd.as_raw_fd(), &mut st) })?;
                let md = fstat_attr(self, &st);
                self.add_lookup(md.ino.0, p);
                let id = {
                    let mut by_ino = self.backing_by_ino.lock().unwrap();
                    let id = Arc::new(reply.open_backing(&fd)?);
                    by_ino.insert(md.ino.0, Arc::downgrade(&id));
                    id
                };
                let fh = fd.into_raw_fd() as u64;
                self.backing_by_fh.lock().unwrap().insert(fh, id.clone());
                return Ok((md, fh, Some(id)));
            }
            let fl = (flags & !libc::O_ACCMODE) | libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC;
            let fd = unsafe { libc::open(c.as_ptr(), fl, mode & !umask) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            unsafe { libc::fchown(fd, req.uid(), req.gid()) };
            unsafe { libc::close(fd) };
            let attr = self.lookup_path(p.clone()).map_err(|e| io::Error::from_raw_os_error(e.into()))?;
            let (fh, b) = self.open_backing(attr.ino, &p, flags, |fd| reply.open_backing(fd))?;
            Ok((attr, fh, b))
        })();
        match r {
            Ok((a, fh, Some(b))) => reply.created_passthrough(&self.ttl, &a, Generation(0), FileHandle(fh), FopenFlags::empty(), &b),
            Ok((a, fh, None)) => reply.created(&self.ttl, &a, Generation(0), FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE),
            Err(e) => reply.error(errno(e)),
        }
    }

    // read/write are only reached without passthrough.
    fn read(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, size: u32, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyData) {
        let _st = crate::stats::start(crate::stats::Op::Read);
        let mut buf = vec![0u8; size as usize];
        let tmp = match self.fd_for(_ino, fh, libc::O_RDONLY) {
            Ok(t) => t,
            Err(e) => return reply.error(errno(e)),
        };
        let n = unsafe { libc::pread(tmp.raw(), buf.as_mut_ptr().cast(), buf.len(), offset as i64) };
        if n < 0 {
            reply.error(errno(io::Error::last_os_error()))
        } else {
            reply.data(&buf[..n as usize])
        }
    }

    fn write(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, data: &[u8], _wf: WriteFlags, _flags: OpenFlags, _lock: Option<LockOwner>, reply: ReplyWrite) {
        let _st = crate::stats::start(crate::stats::Op::Write);
        let tmp = match self.fd_for(_ino, fh, libc::O_RDWR) {
            Ok(t) => t,
            Err(e) => return reply.error(errno(e)),
        };
        let n = unsafe { libc::pwrite(tmp.raw(), data.as_ptr().cast(), data.len(), offset as i64) };
        if n < 0 {
            reply.error(errno(io::Error::last_os_error()))
        } else {
            reply.written(n as u32)
        }
    }

    /// ENOSYS makes the kernel stop sending FLUSH, saving a synchronous round trip on every close().
    /// Dirty pages are still written back before close returns; we have no per-close state or locks to drop.
    fn flush(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _lock: LockOwner, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Flush);
        reply.error(Errno::ENOSYS);
    }

    fn release(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, _lock: Option<LockOwner>, _flush: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Release);
        self.backing_by_fh.lock().unwrap().remove(&fh.0);
        unsafe { libc::close(fh.0 as i32) };
        reply.ok();
    }

    fn fsync(&self, _req: &Request, ino: INodeNo, fh: FileHandle, datasync: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fsync);
        let tmp = match self.fd_for(ino, fh, libc::O_RDWR) {
            Ok(t) => t,
            Err(e) => return reply.error(errno(e)),
        };
        let rc = unsafe { if datasync { libc::fdatasync(tmp.raw()) } else { libc::fsync(tmp.raw()) } };
        match cvt(rc) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn fallocate(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, length: u64, mode: i32, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fallocate);
        let tmp = match self.fd_for(ino, fh, libc::O_RDWR) {
            Ok(t) => t,
            Err(e) => return reply.error(errno(e)),
        };
        match cvt(unsafe { libc::fallocate(tmp.raw(), mode, offset as i64, length as i64) }) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn lseek(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: i64, whence: i32, reply: ReplyLseek) {
        let _st = crate::stats::start(crate::stats::Op::Lseek);
        let tmp = match self.fd_for(ino, fh, libc::O_RDONLY) {
            Ok(t) => t,
            Err(e) => return reply.error(errno(e)),
        };
        let r = unsafe { libc::lseek(tmp.raw(), offset, whence) };
        if r < 0 { reply.error(errno(io::Error::last_os_error())) } else { reply.offset(r) }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let _st = crate::stats::start(crate::stats::Op::Opendir);
        if self.tune.nodir {
            // The kernel remembers ENOSYS: no more OPENDIR/RELEASEDIR; directories default to cached readdir.
            return reply.error(Errno::ENOSYS);
        }
        let r = self.path(ino).and_then(|p| self.read_dir(&p, ino.0).map_err(errno));
        match r {
            Ok(v) => {
                let fh = self.next_dir_fh.fetch_add(1, Ordering::Relaxed);
                self.dirs.lock().unwrap().insert(fh, Arc::new(v));
                reply.opened(FileHandle(fh), FopenFlags::FOPEN_CACHE_DIR | FopenFlags::FOPEN_KEEP_CACHE);
            }
            Err(e) => reply.error(e),
        }
    }

    fn readdir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectory) {
        let _st = crate::stats::start(crate::stats::Op::Readdir);
        let d = match self.dir_entries(_ino, fh, offset) {
            Ok(d) => d,
            Err(e) => return reply.error(e),
        };
        for (i, e) in d.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(e.ino), (i + 1) as u64, e.kind, &e.name) {
                break;
            }
        }
        reply.ok();
    }

    fn readdirplus(&self, _req: &Request, ino: INodeNo, fh: FileHandle, offset: u64, mut reply: ReplyDirectoryPlus) {
        let _st = crate::stats::start(crate::stats::Op::Readdirplus);
        let d = match self.dir_entries(ino, fh, offset) {
            Ok(d) => d,
            Err(e) => return reply.error(e),
        };
        let Ok(dir) = self.path(ino) else { return reply.error(Errno::ENOENT) };
        for (i, e) in d.iter().enumerate().skip(offset as usize) {
            let dot = i < 2;
            let path = if i == 0 { dir.clone() } else if i == 1 { dir.parent().unwrap_or(&dir).to_path_buf() } else { dir.join(&e.name) };
            let Ok(md) = std::fs::symlink_metadata(&path) else { continue };
            let attr = self.attr(&md);
            if reply.add(attr.ino, (i + 1) as u64, &e.name, &self.ttl, &attr, Generation(0)) {
                break;
            }
            if !dot {
                self.add_lookup(attr.ino.0, path);
            }
        }
        reply.ok();
    }

    fn releasedir(&self, _req: &Request, _ino: INodeNo, fh: FileHandle, _flags: OpenFlags, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Releasedir);
        self.dirs.lock().unwrap().remove(&fh.0);
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        let _st = crate::stats::start(crate::stats::Op::Statfs);
        let Ok(c) = cstr(&self.root) else { return reply.error(Errno::EIO) };
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        match cvt(unsafe { libc::statvfs(c.as_ptr(), &mut s) }) {
            Ok(()) => reply.statfs(s.f_blocks, s.f_bfree, s.f_bavail, s.f_files, s.f_ffree, s.f_bsize as u32, s.f_namemax as u32, s.f_frsize as u32),
            Err(e) => reply.error(errno(e)),
        }
    }

    fn getxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        let _st = crate::stats::start(crate::stats::Op::Getxattr);
        reply.error(Errno::ENOSYS); // the kernel then stops asking (e.g. security.capability on every write)
    }
    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        let _st = crate::stats::start(crate::stats::Op::Listxattr);
        reply.error(Errno::ENOSYS);
    }

    fn setxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _value: &[u8], _flags: i32, _position: u32, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Setxattr);
        reply.error(Errno::ENOSYS);
    }
    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Removexattr);
        reply.error(Errno::ENOSYS);
    }
    fn fsyncdir(&self, _req: &Request, _ino: INodeNo, _fh: FileHandle, _datasync: bool, reply: ReplyEmpty) {
        let _st = crate::stats::start(crate::stats::Op::Fsyncdir);
        reply.error(Errno::ENOSYS);
    }
}
