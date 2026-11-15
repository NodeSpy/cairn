# cairn protocol (v2)

The types are in `crates/cairn-proto/src/msg.rs`; this document gives the
semantics. Decisions behind it are in DESIGN §5.1 (security), §13.1
(offline queue), §13.2 (merge rules) and §13.3 (publishing).

## Transport

- TCP plus TLS 1.3 with mutual authentication (`cairn_proto::tls`). Every
  host certificate is signed by the private cairn CA. The server's
  certificate carries the name `cairn-server`.
- **Host identity** is the BLAKE3 fingerprint of the client's certificate.
  The server's host table maps fingerprint → `(HostId, name, ACLs, uid
  rules)`. If the fingerprint is not in the table, the connection is
  closed after `Hello` with `PermissionDenied`. Revocation deletes the row
  and takes effect on the next connect. The server also drops live
  connections of a revoked host.
- **Framing:** `u32 BE length | bincode(Envelope)`, with frames capped at
  16 MiB (`frame.rs` for sync code, `aio.rs` for tokio).
  - Requests carry a client-chosen id, never 0. Responses echo the id and
    may arrive out of order.
  - Pushes use id 0.
  - A client may open several connections, e.g. a bulk-transfer connection
    next to the control connection. Mounts and subscriptions are scoped to
    one connection.

## Session

1. `Hello{proto, software}` → `Welcome{host, host_name, ...}`. A
   protocol-version mismatch is answered with `Invalid` and the connection
   is closed. The client learns its `HostId` here and uses it to mint
   `NodeId`s and `OpId`s.
2. `Mount{root}` → `Mounted{root: handle, epoch, head, writable}`. The ACL
   is checked here. Roots are created by the admin (`cairn-server root add`),
   and a per-host root `hosts/<name>/...` is accessible only to that host.
   If `epoch` differs from the one in the client's state DB, the client
   must resync (below).
3. `Subscribe{root, since}`:
   - normally answered `Subscribed{head}`, after which the server pushes
     every change with `seq > since` as `Push::Changes`, in order, with no
     gaps, forever;
   - answered `ResyncRequired{head}` if `since` is older than the retained
     log.
4. Any number of requests follow, pipelined.

## Namespace model

- A root is a set of **nodes**, keyed by `NodeId`, plus **dirents**
  `(parent, name) → node`.
  - A directory has exactly one dirent (`NodeId::ROOT` has none).
  - A non-directory may have several (hardlinks). `nlink` is the dirent
    count; for directories, clients report it as btrfs does.
- **`Seq`** is per root and strictly increasing. Every change gets one.
  - A node's **version** is the seq of the last change to its attrs or
    content. Its **content version** is the seq of the last `SetContent`.
  - A directory listing's `version` is the head seq at listing time.
- Clients mint `NodeId`s (host id in the high 64 bits, plus a persisted
  counter), so offline creates need no server round trip.
- Names are bytes, 1..=255 long, containing no `/` and no NUL, and never
  `.` or `..`. The server validates them.

## Commits and merging

`Commit{root, commits}` applies the commits in order and returns one
`CommitResult` per commit. The server makes them durable (SQLite commit,
fsync) **before** replying and before pushing the resulting changes.
Batching many commits into one request amortizes the fsync.

**Idempotency.** The server stores `op_id → CommitResult` (keep at least
7 days; the window is configurable). A duplicate `op_id` returns the stored
result and applies nothing. A client retries any commit whose result it
didn't receive, so every commit must stay safe to resend.

**Expectations are per dirent, never per directory.** Two ops on different
names never conflict. A conflict is decided against the server's state at
the moment the commit is applied, in server order:

| Op | Applied as asked when | Otherwise |
|---|---|---|
| `Create` | name absent | name holds an equivalent node (same kind; files: same content; dirs: always; symlinks: same target) → `Adopted{node}` and the server aliases client node → existing node. Different node → `ConflictCopy`: created as `name.conflict-<host>-<UTC yyyymmddThhmmss>`. |
| `Link` | name absent, node exists | name taken → `ConflictCopy`. Node deleted → resurrect it from its tombstone (modify wins). |
| `Unlink` | dirent → `node`, and no other host changed `node` after `base` (dirs: and the dir is empty) | dirent absent: `Applied`, no-op (idempotent). Dirent now points elsewhere, the node was modified by another host, or the dir is not empty → `Transformed{DeleteLost}`: nothing removed, and the client gets the entry back. |
| `Rename` | source dirent → `node` **and** the destination matches `replaces` (`NoReplace`: absent; `Exchange`: both sides as expected) | source already moved elsewhere by another host → `Rejected{RenameLost}`. Source deleted → the rename wins, and the node is resurrected at the destination. Destination occupied by something unexpected → `ConflictCopy` at the destination name. Would create a cycle → `Rejected{CycleRejected}`. |
| any op into a deleted dir | — | the dir is resurrected at its last location (ancestors too, recursively; if a name is taken, the dir gets a conflict name) → `Transformed{DirRecreated}` |
| `SetAttr` / `SetXattr` | node exists | last writer wins (no conflict). Node deleted → `Failed{NotFound}` and the client drops the op (deleting attrs is not data loss). |
| `SetContent` | the host holds the write lease, **or** no host holds it and `base` ≥ current content version (base = the seq at which the client's view of the node was current: a change seq, or a listing's `version`) | otherwise → `Transformed{ContentConflictCopy}`: the content becomes a new sibling file with a conflict name, and the original is untouched. Node deleted → resurrect, then apply (modify wins). |

- **Tombstones.** Deleted nodes and directories keep their last
  `(parent, name, attrs)` for the log retention period, so they can be
  resurrected.
- **Results.** Every result except `Failed` is also written to the conflict
  log (`Conflicts{since}`, inclusive of `since`; clients dedupe by op_id), as are content-lost events. The log records the
  host, uid and op_id.
- **Credentials.**
  - `Commit.cred` is the uid/gid the host asserts. The server applies the
    host's rules: `allowed_uids`/`allowed_gids`, `root_squash`, uid/gid
    maps. A violation is `Failed{PermissionDenied}`.
  - New nodes' `attrs.uid/gid` are checked the same way.
  - The server does not evaluate mode bits; clients enforce them through
    the kernel (`default_permissions`).

**Changes.** The server records the facts that resulted (`Effect`s), not
the request.
- One commit may produce several changes; for example, a rename over an
  existing file is `Remove` then `Move`.
  All changes of one commit are delivered in a single `Push::Changes`
  batch (never split across pushes); clients rely on this to apply
  multi-effect commits such as `RENAME_EXCHANGE` atomically.
- Every change carries `origin` (the op_id that caused it), `host` and
  `cred`.
- Applying all changes in seq order to an empty root reproduces the
  server namespace exactly. This is the core invariant and the model test
  checks it.

## Listing and resync

- **`ReadDir{dir, after, limit}`** pages entries by name.
  - Each page is consistent at `version`.
  - Changes with `seq > version` arrive on the stream. Effects are
    idempotent, so a client that receives a change it has already seen in
    a listing just reapplies it.
- **`GetAttr{node}`** returns the node's attrs and version.
- **Resync** happens when the epoch changes or `ResyncRequired` arrives.
  The client:
  1. re-lists every directory it has cached;
  2. diffs the result against its synced tree, then applies the net
     difference;
  3. re-subscribes from the new head.

  Pending local ops stay queued and rebase normally. `Snapshot` (paged
  full listing) exists for tools and for a client that wants everything.
- **Log retention.** The server keeps the change log for at least 7 days
  and at least 1M changes (configurable). It may truncate older entries.
  Truncation never changes the epoch; restoring the server from a backup
  does.

## Content

- **Content IDs.**
  - `ContentId::Empty` needs no bytes.
  - `Blob(h)`: one blob with hash h (files up to 64 KiB).
  - `Chunked(h)`: h = BLAKE3 of the bincode-encoded `Manifest`, which lists
    the FastCDC chunks (`CDC_*` constants; frozen).
- **Upload.**
  1. `Has{hashes}` → `Missing{hashes}`.
  2. `PutBlob{hash, data}` for each missing blob or chunk, or (v2, preferred)
     `PutBlobs{blobs}` with up to ~4 MiB of blobs per request. The server
     verifies every hash (`Invalid` on any mismatch), writes them all and makes
     them durable with a single `syncfs`, then replies once.
  3. `PutManifest`. The server checks that every chunk is present.
- **Download.** `GetManifest{id}`, then `GetBlob{hash}` per chunk, or (v2)
  `GetBlobs{hashes}` answered by `Blobs(Vec<Option<Vec<u8>>>)`; the server
  stops at 4 MiB of reply and returns `None` for the rest and for blobs it
  does not have; the client falls back to `GetBlob` for those. `GetBlobs`
  never relays pending content (use `GetBlob` for that). The client verifies
  every hash.
- **Pre-uploaded chunks (v2 clients).** A client may upload chunks of a file it
  has not published (checkpoints of open files). The server must not collect
  chunks that no manifest references yet; there is no GC today, and a future
  one needs a grace period for them.
- **Metadata first (DESIGN §13.3).**
  - `SetContent` may reference content the server doesn't have yet. The
    server records `pending(content → host)`.
  - A `GetBlob`/`GetManifest` for pending content makes the server push
    `NeedContent{hash}` to the owner, which uploads that content next. The
    server holds the request until the bytes land; while the owner is
    offline it answers `Unavailable` immediately, and the client returns
    EIO.
  - If the owner reconnects and reports that it no longer has the content
    (its upload fails with "not found locally"), the server logs `content
    lost` and the node stays unreadable until it is overwritten.
- **Request batching (v2).** Clients send up to 4000 commits per `Commit`
  (the server accepts 4096). Clients fold unsent ops before sending (create then
  delete is never sent; create, write, rename is one create at the final name),
  which does not change any rule above.
- **Integrity and GC.** Content is reference-counted from node attrs (plus
  tombstones within retention). Unreferenced content is garbage-collected
  after the retention period.

## Write leases

- **Grants.** `AcquireWrite{node}` returns `Granted{version, content}` at
  once if the lease is free or already held by this host.
- **Recalls.** If another host holds it, the server pushes
  `Recall{node}` to the holder.
  - The holder publishes its current content, with `SetContent` as usual,
    then sends `ReleaseWrite`. Local writers on the holder keep their fds:
    their next close re-acquires the lease and goes through the
    `SetContent` rule.
  - If the holder doesn't release within 30 s (configurable), the lease is
    broken and granted anyway.
- **Disconnects.** Leases die with the holder's last connection.
- **Offline writes.** A client offline writes without a lease. On
  reconnect its `SetContent` is checked against `base` (`base` ≥ content version →
  applied); otherwise → conflict copy.

## Errors

`ErrorReply{code, msg}` is returned for malformed or unauthorized requests,
and for `Unavailable`. It is never used for merge conflicts, which are
`CommitResult`s.
