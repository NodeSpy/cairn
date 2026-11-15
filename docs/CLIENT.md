# cairn client architecture

This document replaces DESIGN §2–§3 and §8 for the client. Those sections
describe the abandoned fanotify design, which is kept for history. The
client is a FUSE filesystem using kernel passthrough (E11–E15). The
contract between the core and the FUSE layer is
`crates/cairn-client/src/api.rs`.

## Processes and crates

`cairnd` is one daemon per host, running as root. It is built from
`cairn-fuse` and links `cairn-client`.

- **`cairn-client` (core).** Owns all state. It has one `Core` per mounted
  root, and those share a `Cache` (the local CAS) and a server connection.
  It has no FUSE dependency. The model and simulation tests drive it
  directly.
- **`cairn-fuse`.** Covers the kernel side:
  - mounting;
  - the request loop (classic `/dev/fuse` threads, or io_uring on VMs);
  - the kernel inode table with lookup counts;
  - passthrough backing ids;
  - cache invalidation;
  - the applier thread that replays remote changes through the mount.

## Local layout

`<cache>` defaults to `/var/lib/cairn`. It must be on btrfs, for reflinks;
elsewhere it falls back to copying.

```
<cache>/oplog           the op log: write-ahead log of every state mutation
<cache>/state.db        SQLite WAL (all roots; tables keyed by root); checkpointed store
<cache>/cas/ab/abcd…    whole-file objects, named by content hash (Blob hash or
                        manifest hash), 0444, immutable, never opened for write
<cache>/live/<root>/<ino>  live files: the backing file of a node that is open
<cache>/tmp/            downloads and snapshots in progress
```

## State DB

The state DB holds the three-tree model (DESIGN §7):

- **synced/remote:**
  - `nodes`: NodeId, local ino, generation, attrs, version, content
    version;
  - `dirents`;
  - `dirs_listed`: which directories have been fully listed from the
    server;
  - `meta`: epoch, `applied_seq`, NodeId and OpId counters.
- **pending:** the `outbound` journal. These are commits not yet
  acknowledged, in order, with their op_ids. The visible view is the
  synced tree plus the pending ops replayed on top.
- **`intents`:** crash-safe application of remote changes.
- **`files`:** pipeline rows, one per node with unpublished or uncommitted
  content: `{ino, stage, content, size, mtime}` (see "Write pipeline").
- **`reaps`:** live files queued for deferred deletion.
- **Write-behind, via the op log.** FUSE calls are answered from an
  in-memory view; every state mutation is first appended to the op log
  (next section). SQLite is the checkpointed store behind it.

## Op log

The model is Postgres: the op log is the WAL, the live files in the cache
are the data WAL, and the CAS is the checkpointed store. Content is written
to disk once.

- **Record.** `len u32 | tag u64 | bincode(DbOp)`; `tag` is the first 8
  bytes of BLAKE3 of the payload. File header `"CAIRNLG1" | base_lsn u64`.
  The LSN is `base_lsn` plus the byte offset after the header.
- **Before the reply.** `journal()` appends and then `write(2)`s the log
  before the FUSE reply. That survives `kill -9` of cairnd. There is no
  fsync on the op path.
- **Group commit.** `fsync`/`fdatasync`/`syncfs` reaching cairnd (FUSE FSYNC
  does reach it under passthrough on kernel 7.2, measured on lfs-a), and a
  timer (`log_sync_ms`, default 100), fsync the log; concurrent callers
  share one fsync. A 2 ms timer writes out buffered records. The sender
  also syncs the log before shipping ops.
- **Power loss.** The guarantee is the ext4/btrfs one: fsynced operations
  survive; the rest is a consistent prefix of the op sequence (records are
  strictly ordered and a torn tail is cut at the first bad checksum).
- **Checkpoint.** A background writer folds records into SQLite in batches
  and stores `ckpt_lsn` in the same transaction. When everything is
  absorbed and `wal_checkpoint(TRUNCATE)` has run (64 MiB of log, or a clean
  shutdown), the log is replaced by a fresh file starting at that LSN.
- **Boot.** SQLite recovers itself, then records at or past `ckpt_lsn` are
  replayed into it, torn tails are truncated, and if SQLite is ahead of the
  whole log the log restarts at its LSN.

## Write pipeline

A save event enqueues the node into a per-root worker pool
(`pipeline_threads`, default 4; `core/pipeline.rs`). Per-file stages, each
journaled as a `File` row in the op log:

`dirty -> hashed -> published -> committed -> uploaded`

- **Save events:** last-writer close, `fsync`/`fdatasync`, atomic-save rename
  onto a name, truncate. None of them waits for hashing (except
  `sync_on_close` paths, and recall, which publish inline).
- **hashed:** the worker hashes the live file straight from disk without
  locks, with a stat before and after. A file that is still open for write
  ("hot") is hashed from a reflink snapshot, at most once per
  `hot_save_min_ms` (1000).
- **published:** under the node lock the worker re-checks the write
  generation and stat. If a write happened in between, the result is
  discarded and the node is requeued (`EAGAIN`). Otherwise the `SetContent`
  commit is recorded, so the version is visible to other hosts.
- **committed:** after `upload_hold_ms` (default 1000, so that folding can
  catch short-lived files) the worker `FICLONE`s the live file into the CAS
  (no second write; if the object already exists the live file is re-cloned
  from it), and hands it to the uploader.
- **uploaded:** the uploader sends chunks and the manifest (see "Sender").
- **Deviation from the first design:** the stage order is *committed before
  uploaded*. The CAS object gives the uploader a stable point-in-time
  source while the writer keeps going.
- **Recovery** re-enters the pipeline at the journaled stage: a published row
  whose content still matches only needs its commit; anything else is
  re-saved. The `live/` scan is a backstop for rows lost with a torn tail.
- **Idle commit moves the file.** If nobody has the file open and nothing
  newer is pending, the commit does not clone: the live file is renamed into
  the CAS (`Cache::adopt`; a duplicate object just drops the live file) and a
  later open re-creates the live file as a reflink of the object. That is what
  keeps a new file at ~1x bytes written to the cache disk. `settle_commit`
  (called by a write-open that is about to reuse the live file) must never move
  it, so it forces the clone path.
- **Eviction and unlink** of an uncommitted idle file: `retire_uncommitted`
  renames the live file into the CAS. A deleted node's pending commit is
  only cancelled when folding dropped its unsent `Create`; otherwise the server
  may still reference the bytes (the delete can lose a conflict, or the create
  was already sent), so the commit and upload still run. A node the server
  adopted into an existing one (`remap_node`) takes its queued saves and jobs
  with it. A new write-open, truncate or stale refresh first settles the commit.
- **Republish with unchanged content.** If a commit finds the live file's stat
  moved (chmod/utimens after close), it republishes; when the hash equals the
  visible content the publish still schedules the commit and upload if the
  object is not in the CAS yet.

## Checkpoints of open files

A file open for write for a long time would otherwise exist only on the
writer's disk. The checkpoint scan (every `checkpoint_scan_ms`, 2000) acts
on files with writers open, bigger than 1 MiB, on a reflink-capable cache:

- Every `checkpoint_secs` (30) or `checkpoint_bytes` (64 MiB) of growth:
  reflink-snapshot the live file into `tmp/`, chunk it, create the CAS
  object as a second reflink, and pre-upload the chunks the server lacks.
  This is **not published**; no commit refers to it.
- **FIEMAP diff.** The previous snapshot is kept as the diff base. Extent
  maps of both snapshots are compared (`changed_ranges`: unchanged only if
  both map a range to the same physical bytes; inline, delalloc and unknown
  extents are never trusted). Only changed ranges, restarted at the first
  affected old chunk boundary, are re-chunked (`rechunk`), stopping at the
  first cut that lands on an old boundary past the last change. The result
  is identical to a full chunking (unit-tested against random edits).
- After `checkpoint_publish_secs` (300) open, the checkpoint is published
  (a crash-consistent version), and again every interval.
- The server never deletes pre-uploaded chunks (it has no GC today; when
  one exists it needs a grace period for chunks without a manifest).

## Sender

- **One batch in flight, no artificial delay.** The sender ships whatever is
  pending when the previous batch is acknowledged: up to 4000 ops or about
  4 MiB (`MAX_BATCH`); the server accepts 4096 commits per request.
- **Folding** (`core/fold.rs`) happens only on ops not yet sent, under the
  root lock:
  - create then delete drops every unsent op naming the node and cancels
    its commit jobs; the server never hears of it;
  - create, write, rename becomes one create at the final name;
  - an atomic save (write temp, rename over the target) replaces the unsent
    ops of the replaced local-only node, inheriting what it replaced on the
    server;
  - repeated unsent versions of a file collapse into the last.
  - Not done: a subtree-delete op for `rm -r` (see E19, "Not done").
- **Content upload hold:** content uploads are held `upload_hold_ms` (1 s).
  Namespace ops are not held. A `NeedContent` relay request bypasses the
  hold and rushes that content.
- **Batched blobs.** The uploader sends one `Has` for a whole group, then
  `PutBlobs` frames of up to 4 MiB, then `PutManifest`. Downloads use
  `GetBlobs` groups of up to 4 MiB. The server clears a stale `pending` row when
  a `Has` finds the content already stored (a client killed between the blob
  write and the pending-row delete would otherwise leave it for ever).
- **Ack ordering.** Remote changes withheld because a pending op touched them
  are reconciled only after the acked ops' own changes (`own_seq`) are in the
  synced tree; reconciling earlier saw the server without them and undid them
  locally (a directory created, then vanishing under a running `git checkout`).
  The FUSE applier does not invalidate a dentry that already resolves to the
  upserted node (it would unhash a directory used as a cwd).

## Deferred deletes

`unlink` returns after the op-log record. The live file is queued
(`DbOp::Reap`) and a reaper thread removes it in the background. The queue
is replayed on boot. Re-creating a live file for the same ino first waits
for its pending reap.

## Lazy namespace

- **Listing on demand.** A directory is listed from the server (paged
  `ReadDir`) on its first `lookup` or `readdir`, then marked listed.
  - Listed directories stay fresh through the change stream; the
    subscription covers the whole root.
  - Changes inside unlisted directories update whatever nodes are known
    and are otherwise ignored, which is cheap.
- **Offline.** Listed directories keep working offline. Unlisted ones
  return EIO.
- **Kernel caching.** The FUSE layer gives the kernel long entry and attr
  TTLs and invalidates them when changes arrive.

## Content lifecycle

- **CAS objects.** Content lives in `cas/` as whole files keyed by
  content hash, one object per distinct content. Every worktree's copy of
  a file shares one object. This dedup comes for free and needs no
  `FIDEDUPERANGE`.
- **Opening.** When a node is opened (read or write), the core makes sure
  the node has a **live file**:
  - a reflink (`FICLONE`) of its CAS object;
  - for a cold file, the content is fetched into `tmp/` first, verified,
    and renamed into `cas/`.

  All local opens of the node share that live file, so readers see
  writers' changes immediately and mmap is coherent. The kernel does the
  I/O through passthrough, at native speed.
- **Saves.** See "Write pipeline": hashing, the `SetContent` commit, the
  FICLONE into the CAS and the upload all run in the background; `release` and
  `fsync` replies do not wait for them. (Before E19: read up to 8 MiB into
  memory, hash, write the object, commit, all inside `release`.)
- **Last opener closes.** The live file is clean again, a reflink of a
  CAS object. Keep it for reuse (bounded LRU) or delete it.
- **Eviction.**
  - Delete CAS objects that no live file uses, that are not pinned, and
    that the server already has, in LRU order.
  - High and low watermarks on `<cache>` free space.
  - On-disk sharing between a CAS object and a live file survives,
    because both are reflinks.
- **Open question, answered (E18).** The per-first-open cost of a live file
  (create + FICLONE, about 80 us on btrfs) stays; two of the candidate
  optimizations are in, one is not possible:
  - done: idle live files are kept in a large LRU (`idle_live_files`, default
    65536), so re-opens cost one `open(2)`;
  - done: the first open creates the file and clones into the same fd (no
    separate stat/unlink/mkdir/utimensat/re-open);
  - **not possible:** passing the CAS object itself through for read-only opens.
    The kernel allows one backing *file* per inode and fails every other open
    of that inode with EIO (also non-passthrough/direct-IO opens), so a writer
    arriving while CAS-backed readers exist would have no file to open.
    Prototype measured: open dropped from ~210 us to ~80 us, but `echo >> f`
    with `exec 3< f` held open fails with EIO. Dropped.

## Writes and leases

- **`open(write)`** acquires the server write lease, blocking on a recall
  if another host holds it. Offline, it proceeds without the lease.
- **Recall while local writers are open.** Publish the current content
  (as on last close) and release. The next last-close re-acquires the
  lease and commits with its `base`. If someone else wrote in between,
  the server makes a conflict copy (PROTOCOL.md).
- **Truncate without an open fd** is lease + new content.

## Applying remote changes (real inotify)

1. The core receives `Push::Changes` and records an intent.
2. It calls `KernelApplier::apply(change)` with no locks held.
3. The FUSE layer's applier thread performs the equivalent syscall on the
   mount path:

   | Effect | Syscall |
   |---|---|
   | Upsert | mknod/mkdir/symlink/link or open(O_CREAT) |
   | Remove | unlink/rmdir |
   | Move | rename |
   | Attrs | chmod/chown/utimens/setxattr; content: open + ftruncate + close for MODIFY/CLOSE_WRITE |

   The resulting FUSE requests arrive from the applier's tid, and the
   handlers call `Core` with `Origin::Remote`. The core applies the
   in-flight effect (idempotent) and returns the new state.
4. If no kernel object can be watching (the parent is not in the kernel
   inode table), the applier calls `apply_direct`.
5. The core calls `apply_direct` again (idempotent), then commits
   `applied_seq` and clears the intent in one transaction.

Results of the applier's own commits:

- `Adopted` / `Transformed` / `Rejected` results produce **corrections**
  (the visible view moves from optimistic to actual). They go through the
  same applier, so local watchers see e.g. the conflict-copy rename or the
  reverted rename.
- Changes whose `origin` is one of this host's own op_ids that applied
  exactly are already in the local view. They update only the synced tree,
  with no kernel replay.

## Recovery

- **Startup.**
  1. SQLite recovery, then replay of the op log tail past `ckpt_lsn`
     (torn tail discarded).
  2. Replay incomplete intents.
  3. Re-enter the write pipeline from the `files` rows, requeue the `reaps`.
  4. Scan `live/` as a backstop and compare each file with its recorded
     state; a writer killed mid-way leaves a dirty live file, which is
     re-saved.
  5. Then mount.
- **Reconnect.**
  1. Hello, Mount (check the epoch), Subscribe from `applied_seq` (or
     resync).
  2. Resend unacknowledged outbound commits in order; op_ids make that
     safe.
  3. Re-upload pending content.

## Policies

- **`sync`** (default): the behaviour described above.
- **`sync_on_close`:** `release` of the last writer waits for upload and
  ack.
- **`pinned`:** never evicted.
- **`local_only`:**
  - content and namespace stay on this host, with no commits;
  - nodes still live in the state DB;
  - other hosts never see them.

  It is used for `target/`, caches and similar. Enforcing a policy on
  rename into or out of a `local_only` path is the core's job: crossing
  the boundary turns into a create or delete toward the server.
