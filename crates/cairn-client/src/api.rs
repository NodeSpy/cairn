//! The contract between the client core (this crate) and the FUSE layer
//! (`cairn-fuse`). One `Core` instance serves one mounted root.
//!
//! Division of labour:
//! - **Core** owns all state: the namespace mirror (SQLite WAL + in-memory
//!   cache), the local content cache (`<cache>/cas`), per-file work files,
//!   the outbound journal / offline queue, the server connection, write
//!   leases, conflict handling, recovery. It knows nothing about FUSE.
//! - **FUSE layer** translates kernel requests into `Core` calls, keeps the
//!   kernel-side inode/lookup-count table, hands backing fds to the kernel
//!   for passthrough, and implements `KernelApplier` so remote changes are
//!   replayed *through the mount* (real inotify).
//!
//! Permissions are enforced by the kernel (`default_permissions`), so Core
//! does not re-check mode bits. Core checks namespace validity (EEXIST,
//! ENOTEMPTY, ENOTDIR, EISDIR, EXDEV, cycles, ...).
//!
//! Locking rule: Core must never hold an internal lock while calling into
//! `KernelApplier`, because the applier's syscalls re-enter Core from FUSE
//! worker threads.

use cairn_proto::{AttrChanges, Attrs, Change, Kind, NodeId, RenameMode};
use std::fs::File;
use std::sync::Arc;

/// errno value (libc constants).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Errno(pub i32);

impl Errno {
    pub const ENOENT: Errno = Errno(libc::ENOENT);
    pub const EEXIST: Errno = Errno(libc::EEXIST);
    pub const ENOTDIR: Errno = Errno(libc::ENOTDIR);
    pub const EISDIR: Errno = Errno(libc::EISDIR);
    pub const ENOTEMPTY: Errno = Errno(libc::ENOTEMPTY);
    pub const EINVAL: Errno = Errno(libc::EINVAL);
    pub const EIO: Errno = Errno(libc::EIO);
    pub const EPERM: Errno = Errno(libc::EPERM);
    pub const EACCES: Errno = Errno(libc::EACCES);
    pub const ENODATA: Errno = Errno(libc::ENODATA);
    pub const ENOSPC: Errno = Errno(libc::ENOSPC);
    pub const EROFS: Errno = Errno(libc::EROFS);
    pub const ESTALE: Errno = Errno(libc::ESTALE);
    pub const EXDEV: Errno = Errno(libc::EXDEV);
    pub const EMLINK: Errno = Errno(libc::EMLINK);
    pub const ENAMETOOLONG: Errno = Errno(libc::ENAMETOOLONG);
    pub const ERANGE: Errno = Errno(libc::ERANGE);
    pub const EAGAIN: Errno = Errno(libc::EAGAIN);
}

pub type FsResult<T> = Result<T, Errno>;

/// Local inode number (stable for the life of the node on this host;
/// persisted in the state DB). The root directory is always `ROOT_INO`.
pub type Ino = u64;
pub const ROOT_INO: Ino = 1;

/// Who caused a mutation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Origin {
    /// A local process (uid/gid of the FUSE request). Core applies the op to
    /// the local view immediately and journals it for the server.
    Local { uid: u32, gid: u32, pid: u32 },
    /// The FUSE layer's own applier thread, replaying a remote change through
    /// the mount (see `KernelApplier`). Core does NOT journal it: it makes
    /// sure the in-flight change's effect is applied (idempotent) and returns
    /// the resulting state of the requested object.
    Remote,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NodeInfo {
    pub ino: Ino,
    pub node: NodeId,
    /// Increments if a local ino number is ever reused (export handles).
    pub generation: u64,
    pub nlink: u32,
    /// Current attrs. For a file with open writers, `size`/`mtime_ns` reflect
    /// the work file (Core fstat()s it).
    pub attrs: Attrs,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirEntryInfo {
    pub name: Vec<u8>,
    pub ino: Ino,
    pub kind: Kind,
}

/// A stable snapshot of a directory for one opendir..releasedir session
/// (offsets index into `entries`; "." and ".." are added by the FUSE layer).
#[derive(Clone, Debug)]
pub struct DirSnapshot {
    pub entries: Vec<DirEntryInfo>,
}

/// Fields for creating a new node. The FUSE layer computes uid/gid/mode
/// (umask, setgid directories) exactly as a local filesystem would.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct NewNode {
    pub kind: Kind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u64,
    pub symlink_target: Option<Vec<u8>>,
    /// Initial xattrs (e.g. inherited default ACLs, security.* labels).
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum XattrFlags {
    Any,
    /// XATTR_CREATE: fail with EEXIST if present.
    Create,
    /// XATTR_REPLACE: fail with ENODATA if absent.
    Replace,
}

/// An open file. `file` is the node's **live file**, the one backing file
/// every local opener of this node shares (readers and writers alike), so
/// local semantics are exact (`tail -f`, mmap coherence). Read-only opens
/// get a read-only fd. See docs/CLIENT.md "Content lifecycle".
pub struct OpenFile {
    pub file: File,
    /// Pass back to `release`.
    pub token: u64,
}

/// Filesystem space report, in bytes / inodes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct StatFs {
    /// Logical capacity of the root (server-side quota or NAS free + used).
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub total_files: u64,
    pub free_files: u64,
}

pub trait Core: Send + Sync + 'static {
    // ---------------------------------------------------------------- reads
    /// May block on a server round trip the first time a directory is used
    /// (Core lists it from the server and caches it; afterwards it is kept
    /// fresh by the change stream). Offline + never listed -> EIO.
    fn lookup(&self, parent: Ino, name: &[u8]) -> FsResult<NodeInfo>;
    fn getattr(&self, ino: Ino) -> FsResult<NodeInfo>;
    fn readdir(&self, dir: Ino) -> FsResult<Arc<DirSnapshot>>;
    fn getxattr(&self, ino: Ino, name: &[u8]) -> FsResult<Vec<u8>>;
    fn listxattr(&self, ino: Ino) -> FsResult<Vec<Vec<u8>>>;
    /// Root-relative path of a node (any one of its links), for the applier.
    /// None if the node is not reachable in the local view.
    fn path_of(&self, ino: Ino) -> Option<Vec<u8>>;
    /// Whether the kernel can have this ino cached. Core does not track this;
    /// the FUSE layer does. Listed here only as documentation of the split.
    // (intentionally no method)

    // ------------------------------------------------------------ namespace
    /// create / mkdir / symlink / mknod.
    fn create(&self, origin: Origin, parent: Ino, name: &[u8], new: NewNode) -> FsResult<NodeInfo>;
    fn link(&self, origin: Origin, ino: Ino, new_parent: Ino, new_name: &[u8]) -> FsResult<NodeInfo>;
    /// unlink (files, symlinks, specials); EISDIR for directories.
    fn unlink(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()>;
    /// ENOTDIR for non-directories, ENOTEMPTY if not empty.
    fn rmdir(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()>;
    fn rename(&self, origin: Origin, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8], mode: RenameMode) -> FsResult<()>;
    /// chmod/chown/utimens and truncate (`size`). Truncate of a file with no
    /// open writer acquires the write lease and produces new content.
    fn setattr(&self, origin: Origin, ino: Ino, changes: AttrChanges, size: Option<u64>) -> FsResult<NodeInfo>;
    fn setxattr(&self, origin: Origin, ino: Ino, name: &[u8], value: &[u8], flags: XattrFlags) -> FsResult<()>;
    fn removexattr(&self, origin: Origin, ino: Ino, name: &[u8]) -> FsResult<()>;

    // -------------------------------------------------------------- content
    /// Open for passthrough. Ensures the live file exists (reflink of the
    /// local CAS object, fetching the content first if not resident; blocks).
    /// `write`: acquire the server write lease first (may block on a recall
    /// from the holder; offline -> proceed, conflict rules apply later).
    /// `truncate`: O_TRUNC (implies write).
    fn open(&self, origin: Origin, ino: Ino, write: bool, truncate: bool) -> FsResult<OpenFile>;
    /// An open was released. When the last *writer* goes: snapshot the live
    /// file, hash it, ingest into the local CAS (identical content = one
    /// object), publish metadata, start the upload, release the lease. For
    /// `sync_on_close` paths, returns only after the server has the bytes.
    /// When the last opener goes, the live file may be dropped (it is a
    /// clean reflink of a CAS object again).
    fn release(&self, ino: Ino, token: u64) -> FsResult<()>;
    /// fsync(2) on an open file: make local state durable (state DB commit +
    /// fdatasync of the work file). Does not wait for the server unless the
    /// path is `sync_on_close`.
    fn fsync(&self, ino: Ino, datasync: bool) -> FsResult<()>;
    /// syncfs(2)/sync(2): commit the state DB.
    fn sync_all(&self) -> FsResult<()>;
    /// Whether `release` of this node may block on the server
    /// (`sync_on_close` policy). The FUSE layer uses it to decide whether it
    /// needs FLUSH (otherwise it sets FOPEN_NOFLUSH).
    fn sync_on_close(&self, _ino: Ino) -> bool {
        false
    }
    /// statfs(2). Default: unknown (the FUSE layer reports static values).
    fn statfs(&self) -> FsResult<StatFs> {
        Err(Errno(libc::ENOSYS))
    }

    // --------------------------------------------------- remote application
    /// Register the applier. Core starts applying the change stream only
    /// after this is called.
    fn set_applier(&self, applier: Arc<dyn KernelApplier>);
    /// Apply a change's effect to Core state directly (idempotent). The
    /// applier calls this when replaying through the mount is unnecessary
    /// (no kernel object cached) or impossible.
    fn apply_direct(&self, change: &Change) -> FsResult<()>;
}

/// Implemented by the FUSE layer. Core calls `apply` for every change from
/// the server (and for local corrections after a conflict: a transformed or
/// rejected commit), strictly in order, from one thread, holding no locks.
///
/// Contract:
/// - Before `apply` returns, the change's effect must be in Core state —
///   either via FUSE handlers invoked with `Origin::Remote` while the applier
///   performs syscalls on the mount, or via `Core::apply_direct`. Core calls
///   `apply_direct` again afterwards anyway (idempotent), then advances
///   `applied_seq` durably.
/// - Watchers must see the events a local op would produce (DESIGN §6).
///   Only objects the kernel may have cached can have watchers, so changes
///   whose parent directory is unknown to the kernel can go straight to
///   `apply_direct`.
/// - Kernel caches (dentries, attrs, page cache) must be invalidated for
///   anything the change touched.
/// - Must be idempotent: after a crash Core may re-deliver changes.
pub trait KernelApplier: Send + Sync + 'static {
    fn apply(&self, change: &Change) -> FsResult<()>;
}
