# cairn — design

> **Build phase (2026-11-13).** Client architecture: `docs/CLIENT.md`
> (supersedes §2–§3 and §8, which describe the abandoned fanotify client).
> Wire protocol and merge semantics: `docs/PROTOCOL.md`.

Status: draft v0.2 (2026-11-12). Kernel behaviour was checked against the
v7.2.15 source (§3.1). Items marked **[verify]** still need a runtime
experiment (§14).

> **2026-11-13: the client layer is being re-evaluated.** The census found
> 3.82M entries (`docs/experiments/namespace-census.md`). Together with E4/E9,
> that makes full-namespace placeholders impractical. E11 benchmarks a FUSE
> client using kernel passthrough (lazy namespace, native data path) against
> native btrfs and NFS. Everything above the client kernel layer stays the
> same either way: the server, CAS/dedup, journal, leases and remote inotify.
>
> **E11 result (`docs/experiments/E11-fuse.md`):** FUSE with passthrough
> matches native on data (sequential I/O, `cargo build`). Metadata-heavy
> operations are about 2–7× slower than local disk and about 8× faster than
> NFS (`git worktree add` takes 0.86 s vs 0.33 s native and 3.9 s on NFS).
> Remote-change inotify through the mount works. The recommendation is FUSE
> with passthrough, pending Daniel's decision.
>
> **E12 (request trimming):** Answering OPENDIR with ENOSYS, fixing the
> rename index and single-open create brought warm `git status` to 1.5×
> native (from 3.5×). Everything left is one kernel-mandated request per
> operation. On bare metal (chase) a round trip costs 12.4 µs, and tuned
> FUSE vs native is: `worktree add` 2.0×, `worktree remove` 4.7×, warm
> status 1.2×, create 10k 1.9×, delete 10k 6.5×.
> **E13 (io_uring):** per-request latency drops 57–66% (≈20 µs → 6.5 µs per
> round trip on the VM). Passthrough still works. This needs
> `fuse.enable_uring=1` and an io_uring transport added to `fuser` (≈500–700
> lines).
> **E14 (io_uring integrated, 450 lines in vendored fuser):** correct
> (inotify, passthrough, kill → ENOTCONN, no wedge). On VMs: −50% per round
> trip, 15–33% faster on workloads. On bare metal (chase): no gain, 0–26%
> slower. The VM win comes from avoiding cross-vCPU wakeups. Use io_uring on
> VM clients (devbox) and classic on bare metal.

## 1. Goals

One shared namespace across machines (devbox VM, Proxmox nodes and their CTs,
later other hosts), backed by the Synology NAS, that:

1. **Runs at local-disk speed.** The kernel serves reads and writes from a
   local filesystem. No FUSE, and no userspace on the data path.
2. **Delivers real inotify for changes made on other machines.** Existing
   tools (editors, vite/webpack, watchman, `inotifywait`) work unchanged.
3. **Keeps local disk usage small.** Local disk is a cache. Clean, cold
   content is evicted and comes back on demand.
4. **Deduplicates everywhere.** Content is stored once, by hash, on the NAS
   and on every client. Worktrees, `node_modules` and repeated checkouts are
   references until something modifies them (copy-on-write).
5. **Is durable and never drifts silently.** The local view is always
   "server state at sequence N + this machine's unacknowledged local
   changes". Both halves are journaled, and divergence is detected and
   reported, never papered over.

Roots to manage first: `~/Projects`, Paseo home, Conductor dirs. Later:
Docker (see §11).

### Non-goals (for now)

- Not a sync tool. Replicating full copies everywhere is out; data is fetched on demand.
- No WAN operation. Offline edits are queued and rebased on reconnect (§13.1).
- No Windows/macOS clients.

## 2. Architecture

```
 client host (devbox VM, PVE node, …)                    NAS (Synology, btrfs)
┌──────────────────────────────────────────────┐        ┌───────────────────────────────┐
│ apps ──read/write──▶ /home/daniel/Projects     │        │ cairn-server (Docker)        │
│                     (bind mount of             │        │  ├─ namespace + change log     │
│                      /cache/roots/projects)    │        │  │   (SQLite, fsync'd)         │
│                          │ kernel (btrfs)      │        │  ├─ lease table + recalls      │
│   fanotify pre-content ──┤ file leases         │        │  ├─ CAS: /volume1/cairn/cas  │
│   fanotify FID notify ───┤ reflink/dedupe      │◀──────▶│  └─ tree view: plain files,    │
│                          ▼                     │  TCP   │      reflinked to CAS (browsable)
│ cairnd (Rust, root)                          │  mux   └───────────────────────────────┘
│  ├─ state DB (SQLite WAL): per-path state,     │
│  │   applied-seq cursor, outbound op queue     │
│  ├─ local CAS cache /cache/cas (btrfs)         │
│  └─ workers: hydrate, gate, upload, apply,     │
│      evict, dedupe, scrub                      │
└──────────────────────────────────────────────┘
```

- **cairnd** (client daemon) owns one btrfs volume, `/cache`. It holds:
  - the backing trees of each root (`/cache/roots/<name>`)
  - the local CAS cache (`/cache/cas`)
  - the state DB

  Each root is bind-mounted at its visible path (e.g. `~/Projects`) only
  while the daemon is healthy. If the daemon stops, the bind mount is
  removed, so nobody can write to an untracked tree.
- **cairn-server** runs on the NAS in Container Manager as a static musl
  binary. It reads and writes `/volume1` at local speed. Running it on a
  machine that mounts the NAS over NFS would bring the NFS latency problem
  back. The server needs only ordinary file I/O plus reflink and dedupe
  ioctls; it uses no fanotify and no leases. That matters because Synology
  runs old vendor kernels: 4.4.x on most DSM 7.2 models, 5.10 on newer
  platforms. At startup the server probes `FICLONERANGE`/`FIDEDUPERANGE`
  inside its data directory and falls back to copying if either is missing.
  Source and destination must stay inside one shared folder, which is one
  btrfs subvolume, and one bind mount.

## 3. Client kernel mechanisms

All of these are in stock kernels ≥ 6.14 (devbox 7.2.15, PVE nodes 7.0.12).

| Need | Mechanism |
|---|---|
| Lazy content | **Placeholder** = sparse file with the right size, mtime and mode, and no data blocks, plus a `FAN_PRE_ACCESS` inode mark. The first read, write or truncate blocks in the kernel while the daemon fills the content through the event fd. The event fd is `FMODE_NONOTIFY`, so filling raises no `IN_MODIFY` (verified, §3.1). |
| Write gating (filled files) | The daemon holds a kernel **read lease** (`F_SETLEASE F_RDLCK`) on each filled file it has no server write lease for. A local open-for-write breaks it: the opener blocks, and we receive a signal (via signalfd, `F_SETSIG`), get the server write lease, then release. |
| Write gating (placeholders) | In the pre-content handler, after filling: drop the event fd, then try `F_RDLCK`. `EAGAIN` means a writer has the file open, so get the server lease before allowing the access. |
| Local change detection | A second fanotify group in FID mode (`FAN_REPORT_DFID_NAME`) watching directories for `CREATE`, `DELETE`, `RENAME`, `CLOSE_WRITE` and `OPEN` (OPEN feeds LRU). FID mode opens no event fds, so it never interacts with leases. |
| Safe eviction | Order: add the mark first, then upgrade to `F_WRLCK` (succeeds only when nobody else has the file open), then punch the data blocks through a NONOTIFY fd obtained from our own pre-content event. The HSM mode of a file is fixed when it is opened (verified), so mark-before-upgrade guarantees no open file can read holes. |
| Remote change → inotify | A metadata-only local operation (§6). |
| Dedup | `FICLONE` reflinks from the local CAS cache, and `FIDEDUPERANGE` after the fact. |

Prototype status: `src/client` (v0, in-memory state) implements placeholders,
both gates, write-back, eviction and remote apply. It has not been run yet.

### 3.1 Verified kernel semantics (v7.2.15 source)

| Behaviour | Result | Design consequence |
|---|---|---|
| HSM mode decided at open, before `break_lease()` | Yes, fixed for the file's lifetime | Mark-before-upgrade eviction is sound. A mark added while an open is blocked in `break_lease` is missed by that open, so add marks before dropping leases. |
| Which marks make an open HSM | Inode, parent directory with `FAN_EVENT_ON_CHILD` (one level), mount, sb. Regular files only. | Option: mark directories instead of every placeholder (far fewer marks, but every child open becomes HSM). Decide with E6/E9. |
| Filesystems allowing pre-content marks | ext4, xfs, btrfs (`SB_I_ALLOW_HSM`). Not tmpfs, overlayfs, nfs, fuse. | `/cache` must be btrfs/xfs/ext4. |
| Ops that emit `FAN_PRE_ACCESS` | read/write variants, copy_file_range (both files), splice/sendfile, fallocate (incl. punch), truncate/ftruncate, `mmap()` (at map time only) | Fill the whole mapped range at `mmap()`; there are no fault-time hooks (removed upstream). |
| `O_TRUNC` on open | **No event** | A writer can truncate a placeholder before any event. The hydrate handler must check the file's current size and blocks; if it was truncated, skip the fill (the writer owns the content) and only run the write gate. |
| Read vs write | Indistinguishable (no `FAN_PRE_MODIFY`) | Keep the `F_RDLCK` writer test. |
| Range info | Present when the caller passes a position, page-aligned; can be absent | Treat a missing range as "whole file". |
| Event fd | Opened `FMODE_NONOTIFY` with our `event_f_flags`, **through `break_lease()`** | An `O_RDWR` event fd breaks our own read lease. Placeholders hold no kernel lease, so fill through the event fd is fine. Files that are marked and leased (remote apply, eviction) must drop the lease before the mark makes them reachable, and the g1 reader must never wait on a lease we hold. |
| NONOTIFY writes | pwrite and fallocate are silent; ftruncate and futimens still emit `IN_MODIFY`/`IN_ATTRIB` | Filling and evicting are silent except for the mtime restore (`IN_ATTRIB`). |
| `ftruncate` | Emits pre-content even through our own NONOTIFY fd (sb-level check) | Never truncate from the g1 reader thread (self-deadlock). |
| overlayfs | Read/write fire on the real inode; mmap fires nothing; mount marks don't reach overlay's private layer mounts (confirmed by E5); event fds on layer files can't be `O_RDWR` (EROFS) | Docker layers need fill-before-mount (§11). Use inode, parent or sb marks only. Have an `O_RDONLY` fill path (§3.3). |
| Lease conflicts | `F_RDLCK` fails if any writer is open (incl. our own fds). Upgrade to `F_WRLCK` only when ours is the only open. Our own opens break our own lease. Only write-opens and path `truncate()` break read leases; unlink and rename don't. Break time 45 s, then forcibly removed. | Matches the design. Never open a leased file for write from the daemon without releasing first. |
| Deny with errno | `FAN_DENY` + EIO/EPERM/EBUSY/ETXTBSY/EAGAIN/ENOSPC | Fetch failure means EIO; a full cache means ENOSPC. |

### 3.2 Experimental results (E1–E4, E6 on kernel 7.2.11; details in `docs/experiments/E1-E4-E6.md`)

- **Filling (E1) works.** Content is correct and filling raises no `IN_MODIFY`. Two corrections:
  - Restore mtime by setting **both** atime and mtime. An mtime-only `futimens` emits `IN_MODIFY`, even through the event fd.
  - A truncate event arrives with range `(newsize, 0)`. Fill only `[0, newsize)`.
- **Write gate (E2) works.** It costs about 25–40 µs with no server delay.
  - `F_SETSIG` resets whenever a lease is removed, so **re-arm it before every new lease**.
  - `ssi_fd` is only an fd number. **Check the inode (fstat dev/ino) before acting on a break signal**, so a stale signal after fd reuse can't unlock the wrong file.
  - A lease that is never released unblocks the writer after 45 s. Openers using `O_NONBLOCK` get EAGAIN while a break is in progress.
- **Writer detection (E3)** produced no false negatives or false positives, including a live shared writable mmap after close. **Hydrate order:**
  1. fill
  2. neuter the event fd
  3. try `F_RDLCK`
  4. remove the mark
  5. keep the lease

  A file that has both a mark and our lease stalls each event for a full break cycle.
- **Eviction (E4) needs one more rule.** As first written, 126 of 7433 concurrent reads saw zeros, because another process's event could be allowed between our upgrade/unlock and the punch.
  - Fix: while an eviction is in progress, track the inode. If any pre-content event from another process arrives for it, **abort the punch**.
  - With the fix: 0 bad reads in 7439. Files held open or mmapped are correctly refused.
- **Mark cost (E6):**
  - Each event adds about 35–45 µs (one per `read()` call). 4 KiB hot reads are about 23× slower, and `cat` of 10k files is about 8× slower.
  - The handler tops out at roughly 30–60k events/s.
  - Inode, directory and sb marks cost the same per event. Files opened before the mark cost nothing.
  - **Rule: marks only on non-resident files, never on hot data.** This rules out directory and sb marks for whole trees, unless E9 makes inode marks impossible at scale.

### 3.3 Experimental results (E5, E7 on kernel 7.2.11; details in `docs/experiments/E5-E7.md`)

- **overlayfs:**
  - Read, write and copy-up of placeholders through overlay fill correctly with inode, directory or sb marks. Exec of a placeholder binary works, because reading the ELF header triggers the fill.
  - A **mount mark never reaches overlay layers**: overlay reads zeros, copy-up persists those zeros, and exec fails. Never rely on mount marks.
  - mmap through overlay gets no event and reads zeros. That's why Docker needs fill-before-mount.
- **Event fds on read-only mounts:** overlay layer mounts are read-only, so an `O_RDWR` event fd fails with EROFS and the reader gets EPERM. Rules:
  - Fill through the event fd when it is writable.
  - Otherwise use an `O_RDONLY` event group and fill through a fresh open of the real path.
  - That fresh write raises a **nested event from our own pid**. The g1 reader therefore must be multi-threaded (at least 2 threads) and auto-allow our own pid; a single-threaded reader deadlocks (reproduced).
  - Event paths from overlay are relative to the layer mount, so key files by handle or inode, never by path.
- **Hydration by reflink:** `FICLONE` into a placeholder through the event fd works, raises no nested or inotify events, and takes about 0.1 ms regardless of size. The first clone of a freshly written blob costs 50–75 ms (btrfs flushes it). Use it as the primary fill path when the content is in the local CAS, with `pwrite` as the fallback. Restore mtime afterwards.
- **Alignment:** clone offsets must be 4 KiB-aligned. A length that isn't aligned is allowed only when the range ends exactly at the end of both files. Chunks from content-defined chunking aren't aligned, so reflink whole blobs only; never assemble a file from chunks by reflink.
- **Dedup on the client (`FIDEDUPERANGE`):**
  - Changes no mtime or ctime, raises no inotify events, and doesn't break our read lease.
  - Safe with open readers, writers and mmaps.
  - About 6 GiB/s while the file is in page cache, but **1.2 MiB/s cold**. So dedupe right after close-write, while the file is still cached.
  - Files under about 2 KiB are stored inline and gain nothing, so skip them.

### 3.4 Scale results (E9; details in `docs/experiments/E9.md`) and the resulting kernel-state model

Measured at 100k, 1M and a partial 3M entries:
- **Materializing the namespace:** 1M entries take 18 s and about 0.4–0.5 KB of btrfs metadata each.
- **Per-file inode marks don't scale.** Each pins about 1.2 KB of unreclaimable inode and dentry memory: 1 GiB at 1M, and a 4 GiB VM died at 1.67M. They also hit `max_user_marks` (default 36k). Directory marks cost 70–210 MiB.
- **One sb mark costs nothing to hold.**
- **Kernel leases cost about 1.9 KB each,** mostly the pinned inode. Taking them is fast (around 800k/s), so memory is the limit.
- **Lease-break signals are silently lost** beyond `RLIMIT_SIGPENDING` (default 15k).
- **FID notifications:** `FAN_UNLIMITED_QUEUE` is mandatory. With the default queue, a slow reader lost 92% of events.
- **Startup scan of 1M entries:** 21 s cold, 3 s warm, with 2 threads.
- **Ignore marks** (`FAN_MARK_IGNORE | IGNORED_SURV_MODIFY`) on a file under an sb `FAN_PRE_ACCESS` mark produce zero events, even after writes.

**Revised model.** Kernel state follows the *hot set*, not the namespace:

| File state | Kernel state | Read cost | Write gate |
|---|---|---|---|
| Placeholder (not resident) | covered by the sb mark | event, then fill | `F_RDLCK` writer test in the handler |
| Resident, cold | covered by the sb mark, then a fast ALLOW | about 35–45 µs per `read()` | writer test in the handler |
| Resident, hot (LRU set, capped at about 100–250k) | ignore mark plus kernel read lease | native | lease break |

- When a cold file is accessed, it is promoted into the hot set, and the least recently used hot file is demoted. Memory for kernel state is then bounded by the hot-set cap (about 3 KB per hot file), whatever the namespace size.
- **One sb mark per managed filesystem,** for both the pre-content group and the FID group. That means managed roots must live on a **dedicated filesystem**. btrfs subvolumes share one sb, so it has to be a separate partition or LV, so that other data (Docker storage, the daemon's own DB) doesn't pay per-read events. The CAS cache and state DB therefore go on a second small filesystem, or are exempted with ignore marks on their directories (to verify).
- Raise `RLIMIT_SIGPENDING` and `RLIMIT_NOFILE` above the hot-set cap. Treat any lost break as possible (`CLOSE_WRITE` is the backstop that catches unsanctioned writes, which are then resolved as conflicts by base version).
- The namespace practically tops out at a few million entries per client. 3M entries needs about 8 GiB of RAM for metadata caching to keep warm scans fast.

### Known kernel limits

- **Directory listings can't be lazy.** No pre-content hook exists for
  lookup or readdir. Upstream patches exist but are stuck on deadlock
  problems (LWN, June 2026). The whole namespace is
  materialized: one inode and dirent per entry, zero data blocks for
  non-resident files.
- **Create, rename and unlink can't be blocked beforehand.** They are
  observed after the fact. Ordering and conflicts are handled through
  conditional operations (§7.3), not kernel blocking.
- **Every read of a still-marked file costs one event.** We remove the mark
  once a file is filled and protected by a lease. An upstream BPF filter is
  planned but not implemented.
- **Fills and evictions change ctime.** ctime can't be set from userspace,
  so git rehashes those files on the next `status`. That's harmless, just
  slower; `core.trustctime=false` avoids it.

## 4. Data model

- **Content**:
  - Small files (≤ 64 KiB) are a single blob, addressed by BLAKE3 hash.
  - Larger files are split with **FastCDC** (`fastcdc::v2020::StreamCDC`,
    normalization level 1; min 16 KiB, average 64 KiB, max 256 KiB) into a
    chunk list. The file's ID is the hash of that list. These parameters and
    the GEAR table are frozen into the format, because the crate's cut points
    differ from other implementations.
  - Identical content anywhere maps to one ID.
- **Entry**: `path → {kind, mode, uid, gid, mtime_ns, size, content_id,
  xattrs, symlink_target, version}`. Device nodes, hardlink groups and
  xattrs come in later milestones (needed for Docker).
- **Version**: the server's global sequence number at the time of the entry's
  last change.
- **Directory hash**: a Merkle hash over sorted child entries. Used by the
  scrub to find drift cheaply (§7.5).

## 5. Server

- **Store**: `/volume1/cairn/` on btrfs.
  - `cas/` holds blobs and chunks.
  - `trees/<root>/` holds a plain-file view of each root. Files there are
    reflinks of CAS content, so they cost no extra space and stay
    browsable over SMB. Large chunked files are materialized as whole
    files, and block-level dedup (§9) shares their extents.
- **E8 (measured on the RS1221+, kernel 4.4.302+)**: `FICLONE` works
  (1 GiB in 92 µs), and `copy_file_range` is a reflink. `FIDEDUPERANGE` works
  but runs at only 29 MiB/s, so the server never dedupes after the fact. New
  content is written once into the CAS and reflinked into the tree view.
  Sequential write with fsync runs at 265 MiB/s. BLAKE3 hashes about
  1 GiB/s per core. See `docs/experiments/E8.md`.
- **Metadata**: SQLite (WAL) holding the entries, an append-only change log
  `(seq, op, client_id, op_id)`, and the lease table.
- **Change log**: every mutation is appended and fsynced, then
  acknowledged, then broadcast. The log is kept for N days or M entries;
  clients that fall behind it resync (§7.4).
- **Leases**:
  - Per-file write leases, exclusive. Recall goes to the holder, who
    flushes and releases. If the holder doesn't answer, the lease is
    broken after a timeout.
  - Directory leases for namespace operations are an open decision (§13).
- **Auth**: see §5.1.

## 5.1 Security model (decided 2026-11-13)

- **Host identity:** each client has its own key pair and a certificate from a
  private cairn CA held by the server. Every connection is mutual TLS
  (rustls; AES-NI on both ends; kTLS optional). IP addresses grant nothing.
- **Enrollment and revocation:** `cairn enroll` creates the key and a CSR,
  which an admin approves on the server (`cairn-server approve <host>`).
  Automated hosts (PVE CTs) use one-time tokens. Revocation takes effect on
  the next connection.
- **Authorization is per root and per host:** which hosts may mount which
  roots, read-write or read-only. A per-host namespace
  (`hosts/<host>/...`) is writable only by that host.
- **Multiple users:** the filesystem is multi-user. Ownership, modes and ACLs
  are stored and enforced, and clients mount with `default_permissions` so the
  local kernel checks access. The authenticated host *asserts* the uid/gid of
  each operation, as NFS AUTH_SYS does but over an authenticated channel.
  Per-host rules limit what a host may assert:
  - `allowed_uids` / `allowed_gids` ranges
  - `root_squash` (uid 0 is mapped to nobody unless allowed)
  - optional uid/gid maps for hosts with different numbering (e.g. idmapped
    CTs)

  Residual risk: a compromised host can act as any uid it is allowed to
  assert, within the roots it can access.
- **Integrity:** everything is content-addressed (BLAKE3). Clients verify
  every fetched chunk, and the server verifies every upload.
- **Audit:** the server's change log records the host and uid for every op.
- **Server hardening:**
  - Path resolution never follows symlinks inside the store. The NAS kernel
    is 4.4, which has no `openat2`, so each component is walked with
    `O_NOFOLLOW`.
  - The container mounts only the cairn share.
  - The admin API is a local unix socket only.
  - Request sizes and rates are capped per host.
- **Client hardening:** the daemon runs as root (passthrough needs it) under a
  systemd sandbox. The host key is `/etc/cairn/host.key` (0600). TPM-backed
  keys are a later option.
- **Optional:** run over Tailscale/WireGuard for network-level defence in
  depth.

## 6. Mapping remote changes to local inotify

No content is transferred to clients that didn't ask for it.

| Remote op | Local action (daemon, metadata only) | Events watchers see |
|---|---|---|
| file content changed | resident copy: invalidate (punch through NONOTIFY fd), then `ftruncate` to new size and set mtime with a normal fd. Placeholder: just `ftruncate` and set mtime. | `IN_MODIFY`, `IN_ATTRIB`, `IN_CLOSE_WRITE` |
| create file | create placeholder (`O_CREAT`, set_len, mtime) and mark it | `IN_CREATE`, `IN_MODIFY`, `IN_CLOSE_WRITE` |
| mkdir / rmdir | same op | `IN_CREATE`/`IN_DELETE` + `IN_ISDIR` |
| unlink | `unlink` | `IN_DELETE` |
| rename | `rename` | `IN_MOVED_FROM`/`IN_MOVED_TO` |
| chmod / xattr | same op | `IN_ATTRIB` |

Remote ops are applied in server sequence order. When a client catches up
after a gap, it applies only the **net difference**, so watchers see one
event per real change instead of a replay of history.

## 7. Consistency and durability

Model: three trees, as in Dropbox's Nucleus sync engine.
- *remote*: server state at `applied_seq`
- *local*: what is on disk
- *synced*: the last state both sides agreed on

All work is planned as idempotent ops computed from the differences between
these trees. The planning core is deterministic and single-threaded, with I/O
pushed to the edges, so the journal can be replayed and the whole thing can
be tested with randomized simulation.

### 7.1 Invariant

`local tree == apply(server state @ applied_seq) + pending local ops`

Both `applied_seq` and the pending queue live in the client state DB.

### 7.2 Client state DB (SQLite WAL)

- `entries(path, kind, server_version, content_id, local_state ∈
  {placeholder, clean, dirty, local_only}, inode, size, mtime, ctime, …)`
- `meta(applied_seq, server_epoch)`
- `outbound(op_id, op, base_version, content_id, state)`
- `intents(seq, op, step)`: crash-safe application of remote ops

Writes are batched: events go into an in-memory queue and are committed every
~5 ms or 500 ops. In WAL mode with `synchronous=NORMAL`, a 6,000-file
`git worktree add` costs a handful of commits.

### 7.3 Ops are conditional and idempotent

- Every outbound op carries a client-generated `op_id`. The server
  deduplicates on it, so retries are safe.
- Every op carries `base_version`, the version the client last saw. If the
  server's current version differs, it returns `Conflict` instead of
  applying the op.
- Conflict handling for content: the loser's data is kept as
  `name.conflict-<host>-<ts>` and both versions are uploaded. With file write
  leases, content conflicts only happen when a lease was broken (timeout or
  partition).

### 7.4 Recovery

- **Remote op application**: record the intent, perform the local op,
  commit `applied_seq` and mark the intent done in one transaction. After a
  crash, replay incomplete intents. Every step is safe to repeat.
- **Reconnect**: request "changes since applied_seq". If the server returns
  "log truncated" or the epoch changed, do a full diff against the server
  listing, apply the net differences, and requeue local differences as
  outbound ops.
- **Daemon start**: scan each root against the DB using
  `(inode, size, mtime, ctime)`. ctime can't be faked, so any change made
  while the daemon was down is detected and becomes an outbound op (or a
  conflict). Mounts appear only after the scan finishes.
- **fanotify queue overflow**: rescan the affected directories (all of them,
  if the event doesn't say which).

### 7.5 Scrub

- Periodically compare Merkle directory hashes with the server's and walk
  down only into subtrees that differ.
- Also verify resident files against their content hash. This is
  rate-limited and reports bit rot or drift.

### 7.6 Local-only window

Dirty data exists only on local disk until the server acknowledges it.
Metadata is committed on every save event and bytes upload after a ~1 s hold (§13.3).
Paths can be configured as `sync_on_close`: with FUSE, the client holds the
`release`/`flush` reply (or the next open) until the upload is
acknowledged.

## 8. Data paths

- **Read, cold**: `FAN_PRE_ACCESS` gives the file and the byte range.
  1. Look up `content_id`.
  2. If it's in the local CAS cache, reflink it into place. Otherwise fetch
     the chunks covering the range first, fill them, and allow the read.
  3. Fetch the remaining chunks in the background, and remove the mark
     once the file is complete.
- **Prefetch**: on a cold access in a directory, queue sibling files by
  recency and size. Paths matching configured patterns (e.g. `.git/objects/pack`)
  are prefetched whole.
- **Write**: always local. Close-write marks the file dirty. The uploader
  hashes it and chunks it if large, then sends only the content IDs the
  server is missing (a `has?` batch), followed by the op.
- **Evict**: an LRU of clean resident files, with a high and a low
  watermark per volume. `pinned` paths are never evicted (§10).

## 9. Dedup

- **Client**: right after close-write, while the file is still in page
  cache (cold dedupe runs at only 1.2 MiB/s, E7), hash it. If its
  `content_id` is already in the local CAS cache and the file is larger
  than about 2 KiB, call `FIDEDUPERANGE`. The kernel compares the
  bytes and shares the extents, so the file stays a normal file and the
  space is reclaimed. Hydration of known content uses `FICLONE` instead of
  a copy.
- **Server**: the tree view is reflinked from the CAS. There is no
  after-the-fact dedup on the NAS (E8: 29 MiB/s). Partial overlap inside
  large files is deduplicated only in transfer (chunks), not in NAS storage.
- **Transfer**: content-defined chunks mean an edit to a large file uploads
  only the changed chunks. Identical content uploads as hashes only.

## 10. Roots and policies

```toml
[root.projects]
path = "/home/daniel/Projects"
[[root.projects.policy]]          # first match wins
match = ["**/node_modules/.cache/**", "**/target/**", "**/*.tmp"]
mode = "local_only"               # never uploaded, never seen remotely
[[root.projects.policy]]
match = ["**/.git/HEAD", "**/.git/refs/**"]
mode = "sync_on_close"
[[root.projects.policy]]
match = ["**"]
mode = "sync"                     # default: write-back + evictable
```

Modes: `sync` (default), `sync_on_close`, `pinned` (sync, never evicted),
`local_only`. Per-host roots, e.g. `hosts/<host>/docker`, are namespaces only
one host mounts, while their content is still deduplicated across all hosts.

## 11. Docker

**E15 result:** stock Docker runs on the FUSE client. overlay2 has its
`data-root` on the mount, there's no fuse-overlayfs and no snapshotter.
Pulls, a multi-stage build, and postgres with its data volume on the mount
all work. They run 0–50% slower than plain btrfs (pull debian 11.4 s vs
7.1 s, build 42.0 s vs 38.8 s, postgres first start 31 s vs 18 s, not
repeated). There is **no Docker-specific code**. Docker works because the
FUSE layer is a faithful POSIX filesystem: xattrs in all namespaces,
whiteouts, rename flags, credential-correct creates, ACLs and export handles.

- Docker state lives in a per-host namespace (`hosts/<host>/docker`), so it
  is never mounted on two hosts at once (an overlayfs requirement). It still
  gets deduplication, offload to the NAS, and portability.
- Configuration: overlayfs `max_stack_depth` must be 1 (our mount can't sit
  on top of another overlay).
- Known gap: inside an overlay whose upper layer is on FUSE, `ln a b; rm a`
  then accessing `b` returns ESTALE. This is a kernel revalidation limit. Fix
  candidates: distinct node ids per hardlink alias, or a kernel patch.
- Lazy image layers and a snapshotter remain optional optimizations (§16).

## 12. Transport

- Plain TCP: a few connections per client, with small framed requests
  multiplexed over them. QUIC (`quinn`) pins about one core per connection at
  10 GbE, and TCP gets kernel TSO/GRO. QUIC only becomes worth it for roaming
  clients. Requests are
  multiplexed and many can be in flight, and the protocol is batch-oriented
  (`has?`, `get_many`, `put_many`).
- Large transfers use parallel streams. zstd is adaptive (on for
  compressible content when the link is the bottleneck).
- The server uses `sendfile` / `copy_file_range` from the CAS where it can.

## 13. Decisions (Daniel, 2026-11-13)

### 13.1 Server unreachable: queue locally
All local changes (content and namespace ops) keep working and queue in the
outbound journal. On reconnect they are rebased onto the server state (§13.2
rules), like `git rebase`.

### 13.2 Namespace concurrency: git-style optimistic merge, no directory locks
- Every namespace op carries the client's base version of the directory (or
  directories) it touches. The server merges ops per directory entry, like a
  git tree merge.
- Ops on different names in the same directory always merge, so many
  processes on many machines can work in one directory at once.
- Conflicts arise only on the same name within the propagation window. They
  are resolved deterministically, in server order, and no data is ever lost:

| Conflict | Resolution |
|---|---|
| Both create the same name | Identical content hash: no conflict. Otherwise the first wins and the other becomes `name.conflict-<host>-<ts>` |
| Delete vs modify | The modification wins (the entry is resurrected) |
| Two renames of the same source | The first wins; the loser's rename is reverted locally |
| Create/rename into a directory deleted concurrently | The directory is recreated |
| Rename that would create a cycle | The later op is rejected and reverted locally |

- Losing clients receive the outcome as ordinary local ops (real inotify).
- Every conflict is written to a conflict log that clients can query
  (`cairn conflicts`).
- File content keeps per-file write leases (single writer). Content conflicts,
  which only happen after a broken lease or offline edits, follow the
  "create twice" rule.

### 13.3 Publishing changes: metadata first, bytes after a short hold

Revised 2026-11-14 (Daniel). Decisions:

- **Content publishes on every save, once hashed:** last-writer close,
  `fsync`/`fdatasync`, and atomic-save rename. The save event does not wait
  for the hash (`sync_on_close` paths excepted).
- **Long-open files publish crash-consistent checkpoints:** a reflink snapshot
  every ~30 s or ~64 MiB of growth is chunked and its chunks pre-uploaded, and
  after 5 minutes open it is published. Only changed ranges are rehashed
  (FIEMAP extent diff).
- **Uploads are held ~1 s** (`upload_hold_ms`) so that temp files, create then
  rename, and repeated saves fold away before any bytes move. Namespace ops are
  not held.

Flow:
1. A save event journals the file as dirty in the op log and returns. A worker
   hashes it (BLAKE3, FastCDC for large files), re-checks the write generation
   (a write during hashing discards the result and requeues), and commits a
   metadata op `{path, size, mtime, mode, content_id}`. Other clients see the
   file at once (placeholder plus inotify). Visibility is close-to-open, like NFS,
   plus every fsync/rename save of a still-open file.
2. If the server already has `content_id` (dedup), that's the end of it.
3. Otherwise, after the hold, the live file is reflinked into the CAS and
   uploaded in batches. A newer version of the file supersedes a queued upload.
4. If another client reads the content before the upload lands, the server
   asks the owning client to prioritize that content (bypassing the hold) and
   relays it.
5. The risk window: until the bytes land, the content exists only on the
   writer. If the writer is lost, readers get EIO and the entry is flagged
   `content lost from <host>` in the conflict log. `sync_on_close` stays
   available per path for anything that must never be in that window.
6. Local durability is a write-ahead op log (`CLIENT.md`, "Op log"): every
   mutation is in the log before the FUSE reply, fsynced on fsync/syncfs and
   on a ~100 ms timer. Power loss keeps fsynced ops and a consistent prefix.

## 14. Test plan

### 14.1 Feasibility experiments (on throwaway VMs)

| ID | Question | Pass criteria |
|---|---|---|
| E1 | Placeholder fill through the event fd | Reader gets correct bytes; no `IN_MODIFY` |
| E2 | Read-lease gate | Open-for-write blocks until released; works on btrfs |
| E3 | Placeholder writer detection | `F_RDLCK` returns `EAGAIN` with a writer open; never a false negative |
| E4 | Eviction safety | Under concurrent readers and writers, nobody ever reads holes |
| E5 | **overlayfs over placeholders** | Confirm in practice: read and write through overlay fill correctly; mmap of a placeholder through overlay reads zeros (expected per source) |
| E6 | Mark cost | Throughput of hot reads with and without a mark; cost per event |
| E7 | reflink / dedupe on btrfs | `FICLONE` and `FIDEDUPERANGE` behaviour, events emitted, ctime effects |
| E8 | Synology | NAS model and kernel; server container runs; `FICLONERANGE`/`FIDEDUPERANGE` work from inside the container on `/volume1`; `copy_file_range` speed |
| E9 | Scale | Materializing 1M entries: time, inode/RAM cost, fanotify mark memory |
| E10 | Crash | Kill -9 / power-off of the client and the server mid-operation; recovery reaches the invariant |

### 14.2 Environment

Throwaway VMs on the Proxmox cluster (VMIDs 9500–9599, tag `cairn`):
an Arch template (kernel matches the devbox), linked clones `lfs-srv`,
`lfs-a`, `lfs-b`, each with a second disk formatted btrfs for `/cache`.
Fault injection: `tc netem` (latency and loss), `kill -9`, VM reset. E8
runs against the real NAS in a scratch share.

### 14.3 Continuous tests

- e2e scenarios (`tests/e2e.sh`, extended): placeholders, gates, remote
  inotify, namespace ops, atomic save, recall, eviction.
- **Model-based fuzz**: random ops across N clients, checked against a
  reference model of the namespace, plus invariant checks after every
  quiesce.
- Benchmarks: `git worktree add` (a large Go monorepo), `git status`, `npm ci`,
  `cargo build`, compared against local btrfs, the current NFS and Ceph RBD.

## 15. Milestones

1. **Feasibility**: E1–E7 and E9 on VMs. Revise this doc.
2. **Core**:
   - client state DB, journal and recovery
   - server SQLite and change log, op_id and base_version
   - single root, placeholders, gates, write-back, remote inotify
   - e2e and fuzz tests green
3. **CAS and dedup**: BLAKE3 + FastCDC, `has?` batching, reflink and dedupe on both ends, eviction with watermarks.
4. **Production path**: server on Synology (E8), bind-mount lifecycle and systemd unit, multiple roots and policies, metrics. Then migrate `paseo-home`, then `~/Projects`.
5. **Fidelity**: xattrs, hardlinks, device nodes, sockets.
6. **Docker**: works unmodified on the FUSE client (E15). Remaining: the hardlink-alias ESTALE gap, then optional lazy layers.
7. **Fleet**: one client per PVE host, bind-mounted into CTs.

## 16. Performance backlog (accepted for now, 2026-11-13)

Daniel accepted the E14 numbers as the baseline: FUSE with passthrough.
Data is at native speed. Metadata costs roughly 1.2× (warm status) to 2× (create, worktree add) native,
and 5–7× native for deletes, worktree remove and cold status, which are
kernel-mandated single requests. Ideas to revisit if this ever hurts:

1. **git fsmonitor integration.** cairnd answers git's fsmonitor hook from
   its own change log, so `git status` skips per-file lstat (fixes cold
   status).
2. **`native` path policy for hot repos.** Serve a subtree directly from the
   backing btrfs, with no FUSE in the path. That closes the gap fully but
   gives up live cross-machine sharing for that subtree.
3. **io_uring transport.** Implemented in the prototype (E14). Enable it on
   VM clients (`fuse.enable_uring=1` on the kernel cmdline); it gives no gain
   on bare metal.
4. **Upstream kernel work to watch:** FUSE request batching and
   BPF-accelerated FUSE (fuse-bpf), which would let the kernel answer
   lookups and getattrs without a round trip.
5. **VM placement.** Round trips cost about 2× more in VMs. Pin the daemon
   and vCPUs, or run the client on the PVE host and bind-mount into guests
   or CTs.

## 17. Versioning (agreed 2026-11-14; next milestone after the write pipeline)

Like S3 object versioning, but chunk-deduplicated. The server's change log
already records every published version (`SetContent` = node → content id,
with host, uid and time), and deleted nodes already leave tombstones.
Versioning makes that retention deliberate and policy-driven. An old
version costs only its changed chunks.

- **Policy:** per root, matched by path; first match wins.
  ```toml
  [[root.projects.versioning]]
  match = ["**/node_modules/**", "**/target/**", "**/.cache/**"]
  keep = "none"
  [[root.projects.versioning]]
  match = ["**"]
  keep_all_for = "24h"                              # every save for a day
  then = ["hourly:7d", "daily:90d", "weekly:1y"]    # thinning
  deleted_for = "90d"                               # undelete window
  max_versions = 500
  ```
  Thinning is needed because content publishes on every save (§13.3). The
  per-host docker root defaults to a short window or none.
- **Restore** is an ordinary commit: `SetContent` with an old content id, or
  re-creating a path. No data moves, and other hosts see it as a normal
  change (inotify included).
- **Phase 1:** a GC that honours the version policy. GC doesn't exist yet,
  so it gets designed together with versioning: a chunk is kept while any
  retained version or tombstone references it.
- **Phase 2:** per-file history and undelete via the CLI:
  - `cairn versions <file>`
  - `cairn restore <file> --at <time>|--version <n>`
  - `cairn deleted <dir>`
  - `cairn undelete <path>`
- **Phase 3:** a read-only point-in-time view of a whole tree, e.g.
  `<root>/.cairn/at/<timestamp>/…`, like ZFS `.zfs/snapshot`. The server
  stores dirents with valid-from/valid-to seqs, so the namespace can be
  queried as of any retained moment.
- **Permissions:** reading a version requires read access to the path;
  restoring requires write access.
- **Not a backup:** versions live on the same NAS. They cover `rm -rf`,
  bad saves and trashed worktrees. NAS-level backup of the cairn store still
  covers loss of the NAS.

## 18. Pluggable storage backends (agreed 2026-11-14; later, after the NAS deployment)

The goal is options. The NAS is the first priority and stays the default
system of record. Later, a root can instead use object storage as its
system of record, or use a bucket as a continuous offsite copy of the NAS
store. Both are the same code, configured differently.

- **The server stays.** It is still needed for commit ordering and merge,
  change push (remote inotify), leases, and relay. It becomes a coordinator
  plus a hot chunk cache, and can run on the NAS, in a PVE CT, or on a
  small cloud VM.
- **Storage trait** in the server, configured per root:
  - local btrfs (today);
  - S3 via the `object_store` crate (AWS, R2, B2, Wasabi, MinIO, GCS,
    Azure);
  - any other backend through an `rclone serve s3` gateway. No FUSE, and
    no per-backend code in cairn.
- **Bucket layout:**
  - chunks packed into ~32 MB pack files with an index;
  - encrypted on the server before upload;
  - the metadata DB's WAL streamed to the bucket, so a new server can
    rebuild from the bucket alone.
- **Durability mode per root:**
  - *strict*: a commit is acknowledged once its WAL segment is in the
    bucket (group-committed, ~20–100 ms per batch);
  - *relaxed*: acknowledged after the local fsync, shipped within ~1 s.
- **Cold reads** cost 20–100 ms to first byte. The client cache, the
  server's hot cache and prefetch absorb this. Writes are unaffected
  (background upload pipeline).
- **Depends on** GC and live-chunk tracking (§17).
