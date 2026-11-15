# E19 - op log, staged write pipeline, batched sender

Date 2026-11-15. Setup as E17/E18 (lfs-a client, lfs-srv server over TLS, `/cache` btrfs, 2 vCPU, `tests/bench/run.sh`).
Design: `docs/DESIGN.md` 13.3, `docs/CLIENT.md` (op log, pipeline, checkpoints, sender), `docs/PROTOCOL.md` v2.
Raw: `E19-data/` (`lfs-a-final.tsv` is the final build, 3 runs; `lfs-a-e18bin.tsv` is the E18-era binaries re-run on the
same VM the same day; `lfs-a-new*.tsv` are intermediate builds).

## What changed

Op log written before the FUSE reply (group-commit fsync, SQLite checkpoint + replay); `dirty -> hashed -> published ->
committed -> uploaded` pipeline (publish on every save once hashed); crash-consistent checkpoints of long-open files;
buffered sender (one batch in flight, `PutBlobs`/`GetBlobs`, batched `Has`, folding, ~1 s content hold); deferred deletes
with a reaper; idle commit moves the file into the CAS instead of cloning it.

## Results (cairn/native time ratio; MiB/s and IOPS rows: lower is worse)

E17 and E18 columns are copied from E18 (different days, different native baselines); the same-VM A/B column pair is the
only fair comparison for E19.

| workload | unit | E17 cairn | E18 cairn | E18 bins, same VM today | E19 cairn | E19 uring |
|---|---|---:|---:|---:|---:|---:|
| wt_add_default | s | 21.75 | 9.62 | 8.50 | 10.56 | 13.95 |
| wt_add_w16 | s | 21.16 | 9.57 | 9.29 | 13.04 | 15.01 |
| status_cold | s | 0.359 | 0.306 | 0.320 | 0.507 | 0.454 |
| small_create | s | 47.5 | 16.5 | 17.56 | 17.35 | 20.91 |
| seqwrite_nosync_1G | MiB/s | 576 | 727 | 727 | 6737 | 5818 |
| seqwrite_fsync_1G | MiB/s | 50.9 | 52.5 | 59.4 | 71.1 | 61.9 |
| seqread_cold_1G | MiB/s | 107 | 87.9 | 94.3 | 65.7 | 54.7 |
| randread4k_cold | IOPS | - | - | 972 | 404 | 363 |
| randread4k_mmap_warm | IOPS | - | - | 2378 | 1222 | 1416 |
| cargo_build | s | 35.1 | 23.3 | 26.3 | 35.5 | 31.9 |
| lat_stat_median | us | 52.6 | 43.3 | 43.4 | 43.8 | 43.8 |
| lat_open_warm_median | us | 227 | 113 | 110 | 109 | 106 |

Honest summary: the write side wins (nosync sequential write 727 -> 6700 MiB/s, bytes written per new file halves, server
requests collapse), but **metadata-heavy workloads regress 20-35 % on this 2 vCPU VM** (worktree add, cargo build,
cold status) and cold reads are slower (seqread_cold, randread4k_cold, mmap). The cold-stat target (27 us) was not
reached: `lat_stat_median` is unchanged at ~44 us.

## Numbers asked for

- **Bytes written to the cache disk per new file**: 64 x 4 MiB random files (256 MiB): E18 ~522 MiB (2.04x: live file +
  CAS clone/copy), E19 269-279 MiB (~1.05x), because an idle file is renamed into the CAS (`Cache::adopt`) and the live
  file is recreated lazily.
- **Server requests for one `git worktree add` (6165 files, ~7374 ops)**: E19 133-173 requests in total: `Commit` 11-14
  batches carrying all ~7374 ops, `Has` 4-5, `PutBlobs` 13-14, `PutManifest` 102-136. The E18 binary has no request
  counters; E18-era strace showed ~646 TLS record writes for the same operation. (Under strace the new build is
  polluted by timing - Commit 315, Has 1451 - so strace figures are not comparable.)
- **Cold stat**: a single-lock `lookup()` fast path was added; no measurable change in `lat_stat_median` (47.6 -> 43.8 us
  noise band). The 27 us of the prototype was not reproduced inside the full pipeline.

## Why metadata got slower (partly verified)

FUSE stats means during `cargo_build`, E18 binary vs E19: LOOKUP 10.8 -> 59 us, GETATTR 18.6 -> 47 us, MKDIR 35 ->
461 us, SETATTR 29 -> 477 us, RENAME 54 -> 478 us; CREATE improved 2486 -> 1039 us. perf shows the pipeline threads at
~26 % of cairnd CPU (mostly kernel: reflink/rename/fsync on btrfs) and cairn-db at 11 % on a 2 vCPU VM, so the background
pipeline competes with FUSE handlers for CPU and for the root state lock. Moving the op-log `write(2)` out of the lock
(`SyncOnDrop`) helped LOOKUP/GETATTR but not MKDIR/SETATTR. Ruled out by experiment: `pipeline_threads` 1/2,
`checkpoint_secs=0`, `log_sync_ms=1000`, and disabling adopt. Not verified: whether a larger VM removes the gap; the
remaining stall is suspected to be mutators waiting behind btrfs journal commits inside the lock.

## Findings and deviations

- **FUSE FSYNC reaches cairnd** (measured), so fsync publishes immediately (`fsync_visible` e2e).
- Stage order is `committed` (CAS) before `uploaded`, not the reverse: the upload needs the object in the CAS.
- `UnlinkTree` was not implemented; deletes are deferred per file by the reaper.
- Idle commit moves the file into the CAS (adopt); `settle_commit` (write-open reuse) never moves.
- Incidents: lfs-a was unreachable for a while earlier in the work; an e2e run with `powerloss` in the default list
  hard-reset lfs-a once (it passed). `powerloss` is now excluded from the default list and run on lfs-b with A_IP/B_IP swapped.
- Bugs found and fixed: `clone_into` copy-fallback offset; sender fsync before Commit for non-durable sends; shutdown no
  longer drained the pipeline (cold `git status` stalled 180 s after remount); republish with only ctime drift never
  committed/uploaded (data loss, "content owner did not upload"); applier invalidated dentries of in-use directories on
  echo of our own upsert (worktree add failures); `settle_commit` moved the live file away (e10 "live content missing");
  stale server `pending` rows after a client kill mid-`PutBlobs` (cleared in the `Has` handler); O(n^2) job scans.

## Tests

Cargo (client, server, proto, fuse, testkit, release): pass, including differential, server_sim and e10 with
`ROUNDS=1 BIG=1 SEEDS=4` (438 s; earlier `SEEDS=6` 659 s). pjdfstest 8798/8798 (classic, rerun on the final build; `--uring`
earlier). xfstests generic 001 006 011 013 023 024 025 035 075 080 091 215 426 467 pass, 020 fails as before (run on the
build before the last perf edits). `tests/e2e/run.sh`: all scenarios pass (inotify concurrent conflicts offline
kill_client kill_server kill9_oplog kill9_bulk folding checkpoint fsync_visible; last re-run of kill9_oplog, kill9_bulk,
folding, fsync_visible on the final build 18/0); powerloss on lfs-b 4/4. `kill9_bulk`: 6000 unwritten-to-server files,
kill -9, remount: every file reaches the server, readable from the other client, no EIO or "did not upload".
