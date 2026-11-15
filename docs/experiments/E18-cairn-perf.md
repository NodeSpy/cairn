# E18 - cairnd per-op latency and create path

Date 2026-11-14. Same setup as E17 (lfs-a client, lfs-srv server over TLS, `/cache` btrfs, 2 vCPU, `tests/bench/run.sh`),
re-run with 3 runs per workload after the fixes below. Raw: `E18-data/lfs-a-all.tsv`. The VM disk is noisy (btrfs
writeback, io pressure 30-50 % during runs): single workloads move by +-30 % between runs, so read the table for the
order of magnitude. "Before" is the E17 table.

## Results (time ratios vs native, lower is better; MiB/s and IOPS rows: lower is worse)

| workload | unit | native (E18) | E17 cairn | E18 cairn | E18 cairn-uring | cairn/native before -> after | uring/native before -> after |
|---|---|---:|---:|---:|---:|---|---|
| wt_add_default | s | 5.48 | 21.75 | 9.62 | 9.71 | 4.35x -> 1.76x | 16.1x -> 1.77x |
| wt_remove_default | s | 0.33 | 1.34 | 1.04 | 0.97 | 4.49x -> 3.18x | 3.66x -> 2.97x |
| wt_add_w16 | s | 5.94 | 21.16 | 9.57 | 9.70 | 3.09x -> 1.61x | 9.05x -> 1.63x |
| wt_remove_w16 | s | 1.15 | 2.94 | 1.03 | 0.99 | 3.92x -> 0.90x | 3.13x -> 0.86x |
| status_warm | s | 0.190 | 0.192 | 0.059 | 0.048 | 1.73x -> 0.31x | 0.84x -> 0.25x |
| statwalk_warm | s | 0.027 | 0.159 | 0.061 | 0.052 | 5.38x -> 2.25x | 3.39x -> 1.89x |
| status_cold | s | 0.166 | 0.359 | 0.306 | 0.226 | 1.41x -> 1.84x | 1.54x -> 1.36x |
| small_create | s | 10.7 | 47.5 | 16.5 | 25.3 (17.9 in the previous run) | 4.21x -> 1.54x | 20.4x -> 2.36x |
| small_delete | s | 0.220 | 0.905 | 0.903 | 0.702 | 3.68x -> 4.10x | 2.14x -> 3.19x |
| seqwrite_fsync_1G | MiB/s | 58.9 | 50.9 | 52.5 | 32.8 (66.6 previous) | 0.70x -> 0.89x | 0.78x -> 0.56x (noise) |
| seqwrite_nosync_1G | MiB/s | 9481 | 576 | 727 | 727 | 0.07x -> 0.08x | 0.87x -> 0.08x |
| seqread_cold_1G | MiB/s | 156 | 107 | 87.9 | 91.6 | 0.78x -> 0.56x | 0.84x -> 0.59x |
| randread4k_warm | IOPS | 744582 | 535877 | 620582 | 652520 | 0.85x -> 0.83x | 0.82x -> 0.88x |
| cargo_build | s | 17.4 | 35.1 | 23.3 | 24.1 | 1.56x -> 1.34x | 1.66x -> 1.39x |
| lat_stat_median | us | 4.2 | 52.6 | 43.3 | 26.4 | 11.9x -> 10.3x | 5.7x -> 6.3x |
| lat_open_cold_median | us | 5.4 | 259 | 140 | 105 | 46x -> 26x | 32x -> 19x |
| lat_open_warm_median | us | 2.5 | 227 | 113 | 84 | 84x -> 45x | 57x -> 34x |

(`lat_open_warm` is "first open of each file after a daemon restart": recovery deletes clean live files, so it still
pays create + reflink per file; an open of a file that already has a live file costs ~22 us, measured on a running
daemon.) `statwalk_cold` and the rest of the E17 rows are unchanged within noise. Native `status_warm` swings between 0.02 and
0.19 s from run to run on this VM (0.020 in the previous E18 run), so its ratio is not meaningful; compare the cairn column
(0.192 -> 0.059 s).

## Where the time went (perf, strace, `/proc/<tid>/stack` sampling on lfs-a)

1. **Create path: 4.5 ms per file inside `release`, ~70 ms with 16 handler threads queued.** `publish` reflink-snapshotted
   the live file before hashing it. On btrfs `FICLONE` of a source with dirty pages calls `btrfs_wait_ordered_range`: it
   forces the file to disk and waits (and waits behind the writeback throttle, `wbt_wait`). That is a synchronous disk
   write per freshly written file, and it stalls every other FICLONE behind it. No server round trip was involved (the
   lease is skipped while the Create is pending; creates were already local + async).
2. **State DB stalls: 0.3-3 s commits blocking the global lock.** `Shared::take` flushed the DB writer every 1024 ids
   (node and op counters) while the caller held the root lock (create, mkdir, rename). The writer's WAL checkpoint (every
   1000 pages, about once a second under load) fsyncs on btrfs, which stalls for 100s of ms, so a handler (and every
   request behind the root lock) sat 0.5-1.8 s in `create`. This is also why `--uring` was 5x worse: one stalled ring
   thread stalls the whole queue of its CPU.
3. **First open per live file:** unlink + stat + mkdir + create + FICLONE + utimensat + re-open (13 syscalls) plus the
   idle live-file LRU of 1024 files thrashing on a 3000-file working set (every open re-created the file).
4. **`git status`/readdir:** `opendir` replied without `FOPEN_CACHE_DIR` (the E11/E12 prototype had it), so a warm
   status cost 4 requests per directory.

## Changes

- `Cache::ingest_live` (cairn-client `cas.rs`): files up to 8 MiB are read into memory, hashed from there and written to
  the CAS as a plain file only if the object is new. No reflink snapshot, no writeback wait. Larger files keep the
  snapshot path. (create-10k loop: 14 s -> 4 s per 3000 files; `small_create` 47.5 -> 16.5 s, worktree add 21.8 -> 9.6 s.)
- `Shared::take` (`client.rs`): the next block of ids (now 65536) is reserved asynchronously at the half-way mark; only
  running a block dry before the write landed flushes. `wal_autocheckpoint` 16384 pages (`store.rs`) so a checkpoint
  fsync is rare. Together with the previous item this is what fixed `--uring` (small_create 230 s -> 18-25 s, worktree add
  80 s -> 9.7 s, now on par with classic; uring stat latency stays about half of classic).
- `ensure_live`: clone straight into the opened fd (resident object tried first, `fetch` only on ENOENT), no pre-unlink,
  no `create_dir_all`, `futimens` on the fd, and `open()` reuses that fd. Idle live file LRU default 1024 -> 65536.
  Warm open 227 -> 113 us, first open of an uncached file 259 -> 140 us (classic).
- `RootState::stash_zombie`: eviction was O(zombies) per unlink once the zombie table passed 10k entries and the idle live
  files (now 64k) kept them from being evictable; repeated create/delete cycles went from 0.5 s to 22 s per `rm -rf`.
  Now FIFO eviction, and a local unlink drops the node's idle live file at once (a zombie without a live file rebuilds one
  from its content when reopened, e.g. open-by-handle).
- cairn-fuse: writable-handle `RELEASE` runs on a small worker pool (the request thread / uring queue replies at once;
  `CAIRN_SYNC_RELEASE=1` disables; `sync_on_close` paths stay synchronous; the pool drains before Core shuts down).
  Worth ~4 % on creates; mostly keeps a ring thread free. `opendir` now sets `FOPEN_CACHE_DIR|FOPEN_KEEP_CACHE`:
  `git status` warm 0.155 -> 0.06 s and no `READDIR` requests when warm.
- `tests/bench/run.sh`: the umount hook waits for cairnd to exit (async release means a SIGTERMed daemon can still be
  hashing, which warmed the page cache under a "cold" read run).

## Tried and rejected

- **CAS object passed through for read-only opens** (CLIENT.md's suggestion). Prototyped: first open 214 -> 80 us. The
  kernel allows one backing file per inode and fails every other open of that inode with EIO, including direct-IO
  non-passthrough ones, so `exec 3< f; echo >> f` fails with EIO. A writer cannot join readers that hold the immutable
  object. Reverted; CLIENT.md updated.
- **Lease round trip on create:** none to remove, `acquire_lease` already returns early while the Create is pending and
  the common create+write+close finishes before the ack; after the ack a later write open does pay the round trip.

## What is left

- `lat_open_*` 112-140 us (prototype 41-76): the remaining cost is btrfs create + FICLONE of the live file (~80 us) on
  the first open after a restart; an open with a live file present is ~22 us. Persisting clean live files across restarts
  (instead of deleting them in `recover_live_files`) would remove it for warm restarts but needs a staleness check.
- `lat_stat_median` 43 us classic vs 26.9 prototype (uring 26 us): classic is two thread wake-ups per request on 2
  vCPUs; Core's `lookup` is ~10 us of that. First lookup in each directory also lists it from the server.
- `seqwrite_nosync` 0.08x: fio lays the file out (write, close), the close publishes (hashes 256 MiB with FastCDC, ~0.3 s),
  and fio's timed pass reopens the file and waits on the node lock held by that publish. Fix would be to not hold the
  node lock across publish (needs a write epoch so a concurrent writer is not marked clean) or to delay publish after
  close until the file is quiet.
- `status_warm`/`statwalk_warm` 2x native: `OPENDIR`/`RELEASEDIR` per directory remain. ENOSYS-opendir (E12) would drop
  them but loses fstat on a removed open directory (generic/035), so not done.
- `small_delete` 0.9 s vs 0.22: one UNLINK per file plus removal of its idle live file (~50 us); `wt_remove_default`
  3x for the same reason.
- `wt_add` 1.6-1.8x: remaining time is btrfs writeback of the live file + CAS copy (two copies of every new file) next to
  the state DB WAL; a 3-file, 157 MiB part of the worktree is still snapshotted by reflink and waits for disk.

## Correctness

cairn-client / cairn-fuse / cairn-testkit `cargo test` (e10 default, `ROUNDS=1 SEEDS=4`, free-running seeds 100-105 and
200-204, differential, server_sim) pass. `sync_on_close_waits_for_server` fails about 3 % of runs under 6x CPU load;
the same rate on the pre-change tree (race in the test: two live contents on the server), not caused here. pjdfstest
8798/8798 on lfs-a, classic and `--uring`. `tests/e2e/run.sh` 38/38. xfstests on lfs-srv with this cairnd: generic
001 006 011 013 023 024 025 035 075 080 091 215 426 467 pass; 020 fails (xattr value size, fails the same with the
pre-change cairnd). Nothing left mounted or running on lfs-a/b/srv; `perf` was installed on lfs-a with pacman.
