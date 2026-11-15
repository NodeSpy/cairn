//! The applier: replays remote changes *through the mount* so local watchers get exactly the
//! inotify events a local operation would produce (DESIGN §6, CLIENT.md "Applying remote changes").
//!
//! A dedicated thread performs the syscalls on the mount path. Their FUSE requests arrive from this
//! thread's tid, which the handlers map to `Origin::Remote`; Core then applies the in-flight change
//! (idempotently) instead of journaling. When the kernel cannot hold a watcher on the object (the
//! parent is not in the kernel inode table) the change goes straight to `Core::apply_direct`.

use crate::fs::Shared;
use crate::util::*;
use cairn_client::api::*;
use cairn_proto::{Attrs, Change, Effect, Kind, NodeId};
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct Job {
    change: Change,
    done: Sender<FsResult<()>>,
}

pub struct Applier {
    tx: Sender<Job>,
}

impl Applier {
    /// Spawn the applier thread for the mount at `mountpoint`.
    pub fn spawn(core: Arc<dyn Core>, sh: Arc<Shared>, mountpoint: PathBuf) -> Arc<Applier> {
        let (tx, rx) = unbounded();
        let (ready_tx, ready_rx) = crossbeam_channel::bounded(1);
        std::thread::Builder::new()
            .name("cairn-applier".into())
            .spawn(move || {
                // Own umask, so replayed creates carry exactly the recorded mode.
                unsafe {
                    libc::unshare(libc::CLONE_FS);
                    libc::umask(0);
                }
                sh.applier_tid.store(gettid(), std::sync::atomic::Ordering::SeqCst);
                let _ = ready_tx.send(());
                let w = Worker { core, sh, mountpoint };
                w.run(rx);
            })
            .expect("spawn applier");
        let _ = ready_rx.recv();
        Arc::new(Applier { tx })
    }
}

impl KernelApplier for Applier {
    fn apply(&self, change: &Change) -> FsResult<()> {
        let (done, wait) = crossbeam_channel::bounded(1);
        self.tx.send(Job { change: change.clone(), done }).map_err(|_| Errno::EIO)?;
        wait.recv().map_err(|_| Errno::EIO)?
    }
}

struct Worker {
    core: Arc<dyn Core>,
    sh: Arc<Shared>,
    mountpoint: PathBuf,
}

fn cstr(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(libc::EIO)
}

fn rc(r: libc::c_int) -> Result<libc::c_int, i32> {
    if r < 0 {
        Err(errno())
    } else {
        Ok(r)
    }
}

fn spec(ns: i64) -> libc::timespec {
    let (s, n) = (ns.div_euclid(1_000_000_000), ns.rem_euclid(1_000_000_000));
    libc::timespec { tv_sec: s, tv_nsec: n }
}

impl Worker {
    fn run(&self, rx: Receiver<Job>) {
        while let Ok(job) = rx.recv() {
            let r = self.replay(&job.change);
            let _ = job.done.send(r);
        }
    }

    fn direct(&self, c: &Change) -> FsResult<()> {
        self.core.apply_direct(c)
    }

    /// Mount path of `ino` (any link), or None if Core cannot place it.
    fn path_of(&self, ino: Ino) -> Option<PathBuf> {
        let p = self.core.path_of(ino)?;
        let rel = Path::new(std::ffi::OsStr::from_bytes(&p));
        let rel = rel.strip_prefix("/").unwrap_or(rel);
        Some(self.mountpoint.join(rel))
    }

    fn known(&self, n: NodeId) -> Option<Ino> {
        self.sh.ktable.ino_of(n)
    }

    fn replay(&self, c: &Change) -> FsResult<()> {
        let r = match &c.effect {
            Effect::Upsert { parent, name, node, attrs } => self.upsert(c, *parent, &name.0, *node, attrs),
            Effect::Remove { parent, name, .. } => self.remove(c, *parent, &name.0),
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                self.mv(c, *src_parent, &src_name.0, *dst_parent, &dst_name.0, *node)
            }
            Effect::Attrs { node, attrs } => self.attrs(c, *node, attrs),
        };
        match r {
            Ok(Replayed::Done) => Ok(()),
            // Not replayable through the mount (or it failed): apply to Core directly and make
            // the kernel re-validate what it may have cached.
            Ok(Replayed::Direct) | Err(_) => {
                let r = self.direct(c);
                self.invalidate(c);
                r
            }
        }
    }

    /// `name` under `pino` already resolves to `node` in Core.
    fn same_entry(&self, pino: Ino, name: &[u8], node: NodeId) -> bool {
        match (self.known(node), self.core.lookup(pino, name)) {
            (Some(i), Ok(info)) => info.ino == i,
            _ => false,
        }
    }

    fn invalidate(&self, c: &Change) {
        match &c.effect {
            Effect::Upsert { parent, name, node, .. } | Effect::Remove { parent, name, node, .. } => {
                let same = matches!(&c.effect, Effect::Upsert { .. }) && self.known(*parent).is_some_and(|p| self.same_entry(p, &name.0, *node));
                if let Some(p) = self.known(*parent).filter(|_| !same) {
                    self.sh.inval_entry(p, &name.0);
                }
                if let Some(i) = self.known(*node) {
                    self.sh.inval_inode(i, 0, 0);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                if let Some(p) = self.known(*src_parent) {
                    self.sh.inval_entry(p, &src_name.0);
                }
                if let Some(p) = self.known(*dst_parent) {
                    self.sh.inval_entry(p, &dst_name.0);
                }
                if let Some(i) = self.known(*node) {
                    self.sh.inval_inode(i, 0, 0);
                }
            }
            Effect::Attrs { node, .. } => {
                if let Some(i) = self.known(*node) {
                    self.sh.inval_inode(i, 0, 0);
                }
            }
        }
    }

    fn child_path(&self, parent: NodeId, name: &[u8]) -> Option<(Ino, PathBuf)> {
        let pino = self.known(parent)?;
        let pp = self.path_of(pino)?;
        Some((pino, pp.join(std::ffi::OsStr::from_bytes(name))))
    }

    fn upsert(&self, _c: &Change, parent: NodeId, name: &[u8], node: NodeId, a: &Attrs) -> Result<Replayed, i32> {
        let Some((pino, path)) = self.child_path(parent, name) else { return Ok(Replayed::Direct) };
        if self.same_entry(pino, name, node) {
            // the name already leads to this node (e.g. the echo of our own create): only its
            // attributes changed. Invalidating the entry would unhash a directory that processes
            // still have as their cwd.
            return Ok(Replayed::Direct);
        }
        self.sh.inval_entry(pino, name);
        let cp = cstr(&path);
        let mode = a.mode & 0o7777;
        // A node the kernel already holds, appearing at another name, is a hard link.
        if a.kind != Kind::Dir {
            if let Some(ino) = self.known(node) {
                if let Some(src) = self.path_of(ino) {
                    let cs = cstr(&src);
                    return match rc(unsafe { libc::link(cs.as_ptr(), cp.as_ptr()) }) {
                        Ok(_) | Err(libc::EEXIST) => Ok(Replayed::Done),
                        Err(e) => Err(e),
                    };
                }
            }
        }
        let r = match a.kind {
            Kind::File => {
                // open(O_CREAT) + (ftruncate) + close: IN_CREATE, [IN_MODIFY], IN_CLOSE_WRITE like a local create.
                let fd = unsafe {
                    libc::open(
                        cp.as_ptr(),
                        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
                        mode,
                    )
                };
                if fd < 0 {
                    Err(errno())
                } else {
                    let r = if a.size > 0 { rc(unsafe { libc::ftruncate(fd, a.size as i64) }).map(|_| ()) } else { Ok(()) };
                    unsafe { libc::close(fd) };
                    r
                }
            }
            Kind::Dir => rc(unsafe { libc::mkdir(cp.as_ptr(), mode) }).map(|_| ()),
            Kind::Symlink => {
                let t = CString::new(a.symlink_target.clone().unwrap_or_default()).map_err(|_| libc::EINVAL)?;
                rc(unsafe { libc::symlink(t.as_ptr(), cp.as_ptr()) }).map(|_| ())
            }
            k => rc(unsafe { libc::mknod(cp.as_ptr(), type_bits(k) | mode, a.rdev as libc::dev_t) }).map(|_| ()),
        };
        match r {
            Ok(()) | Err(libc::EEXIST) => Ok(Replayed::Done),
            Err(e) => Err(e),
        }
    }

    fn remove(&self, _c: &Change, parent: NodeId, name: &[u8]) -> Result<Replayed, i32> {
        let Some((pino, path)) = self.child_path(parent, name) else { return Ok(Replayed::Direct) };
        self.sh.inval_entry(pino, name);
        let cp = cstr(&path);
        let mut r = rc(unsafe { libc::unlink(cp.as_ptr()) });
        if r == Err(libc::EISDIR) {
            r = rc(unsafe { libc::rmdir(cp.as_ptr()) });
        }
        match r {
            Ok(_) | Err(libc::ENOENT) => Ok(Replayed::Done),
            Err(e) => Err(e),
        }
    }

    fn mv(&self, _c: &Change, sp: NodeId, sn: &[u8], dp: NodeId, dn: &[u8], _node: NodeId) -> Result<Replayed, i32> {
        let (Some((spino, src)), Some((dpino, dst))) = (self.child_path(sp, sn), self.child_path(dp, dn)) else {
            // One side is unknown to the kernel: no single rename(2) can be replayed.
            return Ok(Replayed::Direct);
        };
        self.sh.inval_entry(spino, sn);
        self.sh.inval_entry(dpino, dn);
        let (cs, cd) = (cstr(&src), cstr(&dst));
        match rc(unsafe { libc::rename(cs.as_ptr(), cd.as_ptr()) }) {
            Ok(_) => Ok(Replayed::Done),
            // Source gone: already applied (or the view moved on); Core's idempotent apply settles it.
            Err(libc::ENOENT) => Ok(Replayed::Direct),
            Err(e) => Err(e),
        }
    }

    fn attrs(&self, _c: &Change, node: NodeId, new: &Attrs) -> Result<Replayed, i32> {
        let Some(ino) = self.known(node) else { return Ok(Replayed::Direct) };
        let Some(path) = self.path_of(ino) else { return Ok(Replayed::Direct) };
        let Ok(cur) = self.core.getattr(ino) else { return Ok(Replayed::Direct) };
        let old = &cur.attrs;
        let cp = cstr(&path);
        let mut did = false;

        if new.kind == Kind::File && (new.content != old.content || new.size != old.size) {
            // Content: drop the kernel's data/attr cache, then open + ftruncate + close
            // (IN_OPEN, IN_MODIFY, IN_CLOSE_WRITE). The applier's open does not touch Core.
            self.sh.inval_inode(ino, 0, 0);
            let fd = rc(unsafe {
                libc::open(cp.as_ptr(), libc::O_WRONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            })?;
            let r = rc(unsafe { libc::ftruncate(fd, new.size as i64) });
            unsafe { libc::close(fd) };
            r?;
            did = true;
        } else {
            self.sh.inval_inode(ino, -1, 0);
        }
        if new.kind != Kind::Symlink && new.mode != old.mode {
            rc(unsafe { libc::chmod(cp.as_ptr(), new.mode & 0o7777) })?;
            did = true;
        }
        if new.uid != old.uid || new.gid != old.gid {
            rc(unsafe { libc::lchown(cp.as_ptr(), new.uid, new.gid) })?;
            did = true;
        }
        for (k, v) in &new.xattrs {
            if !old.xattrs.iter().any(|(ok, ov)| ok == k && ov == v) {
                let ck = CString::new(k.clone()).map_err(|_| libc::EINVAL)?;
                rc(unsafe { libc::lsetxattr(cp.as_ptr(), ck.as_ptr(), v.as_ptr().cast(), v.len(), 0) })?;
                did = true;
            }
        }
        for (k, _) in &old.xattrs {
            if !new.xattrs.iter().any(|(nk, _)| nk == k) {
                let ck = CString::new(k.clone()).map_err(|_| libc::EINVAL)?;
                match rc(unsafe { libc::lremovexattr(cp.as_ptr(), ck.as_ptr()) }) {
                    Ok(_) | Err(libc::ENODATA) => {}
                    Err(e) => return Err(e),
                }
                did = true;
            }
        }
        // Times last: truncate and chmod above bump mtime/ctime.
        if new.mtime_ns != old.mtime_ns || new.atime_ns != old.atime_ns || (did && new.kind == Kind::File) {
            let t = [spec(new.atime_ns), spec(new.mtime_ns)];
            rc(unsafe { libc::utimensat(libc::AT_FDCWD, cp.as_ptr(), t.as_ptr(), libc::AT_SYMLINK_NOFOLLOW) })?;
            did = true;
        }
        if did {
            Ok(Replayed::Done)
        } else {
            Ok(Replayed::Direct)
        }
    }
}

enum Replayed {
    Done,
    Direct,
}
