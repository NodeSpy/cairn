//! Wire messages. See docs/PROTOCOL.md.
//!
//! Model: each root has a namespace of **nodes** (inodes, `NodeId`) and
//! **dirents** `(parent NodeId, Name) -> NodeId`. Hardlinks are several
//! dirents pointing at one file node. The server keeps a per-root,
//! strictly ordered **change log**. Clients send `Commit`s (intent plus
//! expectations); the server merges them git-style (DESIGN §13.2) and
//! records the resulting facts as `Change`s. Every client, including the
//! committer, converges by applying the change stream in `Seq` order.

use crate::ids::*;
use serde::{Deserialize, Serialize};

/// A single path component (Linux names are bytes, not UTF-8).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Name(pub Vec<u8>);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Kind {
    File,
    Dir,
    Symlink,
    Fifo,
    Socket,
    CharDev,
    BlockDev,
}

/// Complete attributes of a node, as stored on the server.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Attrs {
    pub kind: Kind,
    /// Permission bits incl. setuid/setgid/sticky (no file-type bits).
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    /// Files: content length. Symlinks: target length. Others: 0.
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub atime_ns: i64,
    /// Device number for CharDev/BlockDev (overlayfs whiteouts are 0/0 chardevs).
    pub rdev: u64,
    /// Files only.
    pub content: Option<ContentId>,
    /// Symlinks only.
    pub symlink_target: Option<Vec<u8>>,
    /// All xattrs (user.*, trusted.*, security.*, system.posix_acl_*), sorted by name.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Partial attribute update (setattr). `None` = unchanged.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct AttrChanges {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime_ns: Option<i64>,
    pub mtime_ns: Option<i64>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum RenameMode {
    /// rename(2): replaces an existing destination (which must be compatible).
    Replace,
    /// RENAME_NOREPLACE: fails with Exists if the destination exists.
    NoReplace,
    /// RENAME_EXCHANGE: atomically swaps source and destination.
    Exchange,
}

/// What a client wants to happen, with the expectations it was based on.
/// Expectations are per dirent (git-style, DESIGN §13.2): ops on different
/// names never conflict.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Op {
    /// New node at (parent, name). Expects the name to be absent.
    Create { parent: NodeId, name: Name, node: NodeId, attrs: Attrs },
    /// Hardlink: new dirent for an existing file node. Expects name absent.
    Link { parent: NodeId, name: Name, node: NodeId },
    /// Remove dirent (parent, name), which the client saw pointing at `node`
    /// at node version `base`. The node itself is deleted when its last
    /// dirent goes (and, for directories, only when empty). If another host
    /// modified the node after `base` (or added children to a directory),
    /// the delete loses (DESIGN §13.2: modify wins).
    Unlink { parent: NodeId, name: Name, node: NodeId, base: Seq },
    /// Move the dirent `(src_parent, src_name) -> node` to `(dst_parent, dst_name)`.
    /// `replaces` is the node the client saw at the destination (None = absent).
    Rename {
        src_parent: NodeId,
        src_name: Name,
        node: NodeId,
        dst_parent: NodeId,
        dst_name: Name,
        replaces: Option<NodeId>,
        mode: RenameMode,
    },
    /// chmod/chown/utimens. `base` = node version the client last saw (used
    /// only for conflict reporting; last writer wins for attrs).
    SetAttr { node: NodeId, base: Seq, changes: AttrChanges },
    /// Set (Some) or remove (None) one xattr.
    SetXattr { node: NodeId, name: Vec<u8>, value: Option<Vec<u8>> },
    /// New file content (metadata-first publish, DESIGN §13.3). The bytes may
    /// still be uploading; the server tracks them as pending from this host.
    /// `base` = content version the client last saw. Requires the write
    /// lease; if the lease was lost and `base` is stale, the conflict-copy
    /// rule applies.
    SetContent { node: NodeId, base: Seq, content: ContentId, size: u64, mtime_ns: i64 },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub op_id: OpId,
    pub cred: Cred,
    pub op: Op,
}

/// Facts recorded in the change log (post-merge). Applying these in order
/// reproduces the server namespace exactly.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Effect {
    /// Dirent (parent, name) now points at `node`, whose full attrs are given.
    /// Covers create, link, and conflict copies.
    Upsert { parent: NodeId, name: Name, node: NodeId, attrs: Attrs },
    /// Dirent removed. If `node_deleted`, the node is gone too.
    Remove { parent: NodeId, name: Name, node: NodeId, node_deleted: bool },
    /// Dirent moved (rename). A replaced destination appears as a preceding Remove.
    Move { src_parent: NodeId, src_name: Name, dst_parent: NodeId, dst_name: Name, node: NodeId },
    /// Node attrs changed (setattr, xattr, content). Full attrs after the change.
    Attrs { node: NodeId, attrs: Attrs },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Change {
    pub seq: Seq,
    /// The commit that caused this change (None for server-internal changes).
    pub origin: Option<OpId>,
    pub host: HostId,
    pub cred: Cred,
    pub effect: Effect,
}

/// How the server resolved a conflicting commit (DESIGN §13.2 table).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Resolution {
    /// Name already taken by an equivalent node (same kind; files: same
    /// content; dirs: always): the client's node is merged into `node`. The
    /// server records `client node -> node` as a permanent alias, so queued
    /// ops that still name the client's node keep working.
    Adopted { node: NodeId },
    /// Name already taken by different content: the commit was applied under a new name.
    ConflictCopy { parent: NodeId, name: Name },
    /// Delete lost against a concurrent modify: the node stays.
    DeleteLost,
    /// Rename lost against an earlier rename of the same source: not applied.
    RenameLost,
    /// Target directory had been deleted concurrently: it was recreated.
    DirRecreated { dir: NodeId },
    /// Would have created a directory cycle: not applied.
    CycleRejected,
    /// Content committed without a valid lease on a stale base: conflict copy.
    ContentConflictCopy { parent: NodeId, name: Name },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub seq: Seq,
    pub op_id: OpId,
    pub host: HostId,
    pub resolution: Resolution,
    pub detail: String,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum CommitResult {
    /// Applied as requested; its changes end at `seq`.
    Applied { seq: Seq },
    /// Applied in a transformed form (see the conflict); changes end at `seq`.
    Transformed { seq: Seq, conflict: ConflictRecord },
    /// Not applied; the client must revert its local view (the change stream
    /// will not contain it).
    Rejected { conflict: ConflictRecord },
    /// Permission/validation failure (not a merge conflict).
    Failed { error: ErrorReply },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: Name,
    pub node: NodeId,
    pub attrs: Attrs,
}

/// One row of a full namespace snapshot (resync).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SnapshotRow {
    pub parent: NodeId,
    pub name: Name,
    pub node: NodeId,
    pub attrs: Attrs,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ErrCode {
    NotFound,
    Exists,
    NotDir,
    IsDir,
    NotEmpty,
    PermissionDenied,
    ReadOnly,
    Stale,
    Busy,
    Invalid,
    Unavailable,
    TooLarge,
    Internal,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ErrorReply {
    pub code: ErrCode,
    pub msg: String,
}

/// Small handle for a mounted root on one connection (avoids resending names).
pub type RootHandle = u32;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Request {
    /// First message. Host identity comes from the TLS client certificate
    /// (its fingerprint is looked up in the server's host table).
    Hello { proto: u32, software: String },
    Mount { root: RootName },
    /// Start receiving `Push::Changes` for changes after `since`.
    Subscribe { root: RootHandle, since: Seq },
    /// Paged by name: pass the last name received as `after`.
    ReadDir { root: RootHandle, dir: NodeId, after: Option<Name>, limit: u32 },
    GetAttr { root: RootHandle, node: NodeId },
    /// Paged full listing for initial sync / resync, ordered by (parent, name).
    Snapshot { root: RootHandle, after: Option<(NodeId, Name)>, limit: u32 },
    /// Ops are applied in order; one result per commit.
    Commit { root: RootHandle, commits: Vec<Commit> },
    // ---- content (content-addressed, root-independent but ACL-checked) ----
    /// Which of these does the server lack?
    Has { hashes: Vec<Hash> },
    /// Upload one blob or chunk (<= CDC_MAX bytes). Server verifies the hash.
    PutBlob { hash: Hash, data: Vec<u8> },
    PutManifest { manifest: Manifest },
    /// Upload many small blobs/chunks in one frame (keep a frame to a few MiB). The server
    /// verifies every hash first and then writes them in one pass; any mismatch fails the whole
    /// request with `Invalid` and stores nothing.
    PutBlobs { blobs: Vec<(Hash, Vec<u8>)> },
    /// Download a blob/chunk. If it is still pending from another host, the
    /// server relays (DESIGN §13.3) and this may take longer.
    GetBlob { hash: Hash },
    GetManifest { id: Hash },
    /// Download many blobs/chunks in one frame. Never relays: a hash the server does not hold
    /// (including content still pending from another host) comes back as `None`; fetch those with
    /// `GetBlob`. The reply is capped at a few MiB, so the tail of `hashes` may come back `None`
    /// too.
    GetBlobs { hashes: Vec<Hash> },
    // ---- write leases (per file node) ----
    AcquireWrite { root: RootHandle, node: NodeId },
    ReleaseWrite { root: RootHandle, node: NodeId },
    Conflicts { root: RootHandle, since: Seq },
    Ping,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Response {
    Welcome { host: HostId, host_name: String, server_software: String },
    Mounted { root: RootHandle, epoch: Epoch, head: Seq, writable: bool },
    Subscribed { head: Seq },
    /// `since` is older than the retained log (or epoch changed): use Snapshot.
    ResyncRequired { head: Seq },
    /// `version` = head seq at listing time; changes after it arrive on the stream.
    DirListing { version: Seq, entries: Vec<DirEntry>, done: bool },
    NodeAttrs { attrs: Attrs, version: Seq },
    Snapshot { head: Seq, rows: Vec<SnapshotRow>, done: bool },
    Committed { results: Vec<CommitResult> },
    Missing { hashes: Vec<Hash> },
    Blob { data: Vec<u8> },
    Blobs { blobs: Vec<Option<Vec<u8>>> },
    Manifest { manifest: Manifest },
    /// `version`/`content` = current server state of the node at grant time.
    Granted { version: Seq, content: Option<ContentId> },
    ConflictList { conflicts: Vec<ConflictRecord> },
    Ok,
}

/// Server-initiated messages (frame id 0).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Push {
    Changes { root: RootHandle, changes: Vec<Change> },
    /// Give back the write lease on `node` (flush first). Reply with ReleaseWrite.
    Recall { root: RootHandle, node: NodeId },
    /// Another host wants content this host published but hasn't uploaded
    /// yet: upload it next (PutBlob/PutManifest).
    NeedContent { hash: Hash },
    /// Epoch changed / log rewound: drop the cursor and resync.
    ResyncRequired { root: RootHandle },
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Body {
    Request(Request),
    Response(Result<Response, ErrorReply>),
    Push(Push),
}

/// One framed message. Requests carry a non-zero id chosen by the client;
/// the matching Response carries the same id. Pushes use id 0. Requests on
/// one connection may complete out of order.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub id: u64,
    pub body: Body,
}
