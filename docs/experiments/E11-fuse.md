# E11 - FUSE with kernel passthrough vs fanotify placeholders for the client layer

Date 2026-11-13. Code: `experiments/fuse-pt/` (daemon `src/`, benchmark and check scripts `bench/`). Raw data: `docs/experiments/E11-data/`.
Kernel 7.2.11 on both VMs (`lfs-a`, `lfs-b`: 2 vCPU, 4 GiB RAM). Devbox for the NFS baseline: 7.2.15.

## Recommendation

**Use FUSE with passthrough for the client layer.** On the data path it is indistinguishable from native, and on the metadata path it is 2-7x slower than a local disk but about 8x faster than NFS.

- **Data path: native.** Sequential read and write (about 9-10 GB/s, memory-bound on tmpfs), 4 KiB random reads, mmap and `cargo build` are all within about 15% of native. After `open` the daemon is out of the path: the same loopback daemon without passthrough manages only 1.1-1.5 GB/s sequential and 35k cold random IOPS (versus 550-650k), i.e. 6-18x worse.
- **Metadata path: a fixed cost of about 25 us per request that reaches the daemon.** Every lookup, getattr, create, unlink and open that misses the kernel cache is one user/kernel round trip. A cold `stat` costs 26 us (native 1.4 us); an open plus close costs 36-62 us (native 2.5 us).
- **Worktree and small-file workloads.**
  - `git worktree add` (6164 files): 1.39 s against 0.46 s native, 3.0x. NFS took 11.6 s (3.9 s with `checkout.workers=16`; FUSE+passthrough takes 0.86 s with 16 workers).
  - `git worktree remove`: 0.37 s against 0.06 s native (5.9x). NFS: 2.9 s.
  - Creating 10k small files: 1.5 s against 0.74 s (2.1x). NFS: 12.5 s.
  - Deleting 10k files: 0.35 s against 0.05 s (6.8x). NFS: 3.5 s.
- **Where it loses:** metadata-bound bulk operations (`rm -rf`, `git worktree remove`, `git status` after a cold start) at 3-7x native. The absolute numbers are a few tenths of a second per 6k-file tree. Hot reads and writes of file contents have no penalty.
- **Passthrough is what makes this work.** Without it, every read and write goes through the daemon; with it they cost nothing extra. The metadata cost is the same with and without passthrough, so passthrough does not help the metadata gap. What closes that gap is in the last section.

Fanotify placeholders keep native metadata speed but, per DESIGN §3.3-3.4, need the whole namespace materialized, per-file kernel state that does not scale, a dedicated filesystem for the sb mark, and a 35-45 us event on every cold read. FUSE has none of those limits: directory listings and lookups can be lazy, and the fill happens in `open()` before the passthrough fd is handed to the kernel. Its price is the 25 us round trip above, plus having the daemon in the namespace path (if it dies, the mount fails with ENOTCONN; fanotify placeholders would keep serving resident files).

## Prototype

`experiments/fuse-pt/` is a path-based loopback filesystem serving `--backing` at `--mount`.

- **Crate choice: `fuser` 0.18.0** (`default-features = false`, so no libfuse dependency and the musl static build works; it mounts directly with `mount(2)` as root and falls back to `fusermount`).
  - It supports passthrough: `ReplyOpen::open_backing(fd)` issues `FUSE_DEV_IOC_BACKING_OPEN` and returns a `BackingId` (closed on drop), and `opened_passthrough` / `created_passthrough` set `FOPEN_PASSTHROUGH`. `InitFlags::FUSE_PASSTHROUGH` and `KernelConfig::set_max_stack_depth` are there too.
  - It also provides `n_threads` with `FUSE_DEV_IOC_CLONE` fds, `Notifier::{inval_inode, inval_entry}`, readdirplus and `Session::unmount_callable`. No need for another crate or hand-written protocol code.
  - Gotchas found: a backing id must be shared by all handles of an inode (kernel requirement; implemented as a per-inode `Weak<BackingId>` map), and the backing file must be opened read-write if any later open may write.
- **Request loop:** 4 threads (`--threads`), each with its own cloned `/dev/fuse` fd. The sweep below shows 1 to 8 threads make no difference for these single-process workloads.
- **Caching:** entry and attribute TTL 3600 s, negative lookups cached (ino-0 entry), `FOPEN_KEEP_CACHE` on files, `FOPEN_CACHE_DIR` on directories, readdirplus (`DO_READDIRPLUS` + `READDIRPLUS_AUTO`), `PARALLEL_DIROPS`, `ASYNC_READ`, `CACHE_SYMLINKS`, 1 MiB max_write/readahead, `max_stack_depth=2` (so an overlayfs under the backing dir is allowed). `flush` replies ENOSYS, which stops the kernel sending a synchronous FLUSH on every close (about 20% off the small-file workloads; "tuned" below).
- **Writeback cache and passthrough are mutually exclusive.** With both requested, the mount works but `open` of a regular file fails with EPERM (presumably the backing-open ioctl is refused; I did not trace which call returns it). So the passthrough configuration runs without writeback cache; the no-passthrough configuration uses `--writeback`.
- **Simplifications to remove in the real client:** inode table is path-based (hard links and a rename racing a lookup can resolve a stale path), xattrs return ENOSYS, no POSIX locks, a new file is created as root and chowned immediately after (the daemon runs as root), no persistence.

### FUSE over io_uring

Not cheaply usable from Rust today, so not implemented.

- `fuser` 0.18.0 only defines the `FUSE_OVER_IO_URING` init flag; it has no queue implementation.
- Two crates implement it: `fractal-fuse` 0.6.1 (compio-based, one pinned worker per CPU, about 9k downloads, first published March 2026, 7 commits, passthrough and notify_inval not mentioned in its README) and `fuser-iouring` 0.1.2 (published 2026-11-01, 34 downloads). Neither is a fit for a production client yet.
- A hand-rolled version needs registering per-CPU queues with `IORING_OP_URING_CMD` (128-byte SQEs), commit-and-fetch cycles and its own buffer management: well beyond "cheap".
- Kernel side: it needs Linux 6.14+ (satisfied) **and** the module parameter `fuse.enable_uring=1`; the VMs have it at `N`. I did not flip it.

## inotify check

`bench/inotify-check.sh` (run on `lfs-b`, both modes, `inotifywait -m -r /mnt/lfs`). The daemon's `remote-sim` thread (`--simulate-remote`, `src/remote.rs`) runs 40 cycles at 100 ms spacing. Each cycle, through the mount:

1. create `f1` and write 4 KiB (the handle stays open for the rest of the cycle);
2. `notify_inval_inode(f1)`, then `ftruncate` to 8 KiB; checks stat size is 8192 and that reading the extended region through the still-open handle returns zeros;
3. `notify_inval_inode(f1, attrs only)`, then `futimens`; checks the mtime;
4. `notify_inval_entry` for `f1` and `f2`, then `rename f1 f2`;
5. `notify_inval_entry(f2)`, then `unlink f2`.

In parallel, to provoke a self-deadlock: 30 rounds of `find | xargs stat` plus `cat` of 500 files, 30 rounds of `dd` 16 MiB then `rm`, and a tight loop of `ls`/`stat` on the directory being changed.

Result (counts over 40 cycles; same in passthrough and non-passthrough mode):

| event | passthrough | no passthrough | expected |
|---|---:|---:|---|
| CREATE | 40 | 40 | 40 |
| MODIFY | 78 | 77 | 80 (inotify merges adjacent identical events) |
| ATTRIB | 40 | 40 | 40 |
| MOVED_FROM / MOVED_TO | 40 / 40 | 40 / 40 | 40 / 40 |
| DELETE | 40 | 40 | 40 |
| CLOSE_WRITE | 40 | 40 | 40 |

All 40 cycles completed with no failures in both modes (about 1-2 ms each when idle). The daemon never deadlocked on its own requests and exited cleanly on `umount`. The reason it cannot deadlock here: the simulator is a separate thread, the kernel only calls into the daemon from the fuser request threads, and invalidation notifications never wait on a reply from the thread that sends them. Do not send notifications from inside a request handler that holds a lock the notification needs (the kernel may take the parent inode lock for `inval_entry`).

The `ftruncate` gives `IN_MODIFY` and `futimens` gives `IN_ATTRIB`, as DESIGN §6 expects. An earlier run lost the first 5 cycles because the simulator started before `inotifywait` had set up its watch; that is a test-harness race, fixed by `--sim-delay-ms`.

## Benchmark

### Setup

- **Repo:** a large private Go monorepo (≈6k files). A shallow bare clone (`git clone --bare --no-hardlinks --depth 1 file://...`) gives 6164 tracked files, 209 MB; the VMs and the NFS test used that clone, so the original was only read. (I ran `git status -sb` in it once to check it; that can refresh the index's stat cache but changes no content.)
- **Configs:** `native` is a directory on the test filesystem; `nopt` is the loopback daemon with writeback cache and no passthrough; `pt` is the daemon with passthrough. Daemon runs as root with `allow_other,default_permissions`, mount at `/mnt/lfs`.
- **Runs:** 5 per workload (3 for cold random reads, mmap, cargo build and the latency probes). Medians. Scripts: `bench/bench.sh` (VMs), `bench/nfs.sh` (devbox), `bench/sweep.sh`. Cold = `drop_caches=3`; for FUSE the daemon is also restarted (unmount, drop, mount), so the kernel dentry and attribute caches start empty. Per-op latency probes drop only dentries and inodes (`drop_caches=2`), 3000 files, one fresh `stat` or `open`+`close` each.
- **`find | xargs stat` uses `stat -c '%s %Y %i'`.** Plain `stat` resolves uid and gid names for every file; on these VMs that dominated (6 s natively), hiding the filesystem.
- **Two backings.** The VMs' `/cache` (btrfs on `/dev/sdb`) is very slow and throttled here (creating a file costs 2-5 ms, 1 GiB write with fsync runs at 10 MiB/s, 4 KiB random reads about 600-1000 IOPS), and it made single-run results swing by 2x in either direction (see appendix). It cannot show FUSE overhead. So the main table uses a **tmpfs backing (`/mnt/ram`, 3 GiB) on the same VMs**, which isolates the FUSE cost. The btrfs `/cache` results are in the appendix. A real cache disk (NVMe) will sit between the two.
- **Tuned:** the "FUSE pt (tuned)" column is a later run of the same matrix on `lfs-a` with the final binary (flush replying ENOSYS); the other columns ran with the earlier binary that acknowledged flush. The tuned column is a single VM; the other FUSE columns average `lfs-a` and `lfs-b`.
- **NFS:** devbox's real NFS mount (`/home/daniel/Projects`, nfs4.1, `nconnect=8`, `acregmin=600`), scratch dir `~/Projects/.leasefs-bench/`, 3 runs, deleted afterwards. Cold runs were skipped (need root `drop_caches`; I left the devbox state alone), so NFS numbers are warm-attribute-cache numbers and the cold gap is, if anything, worse. Contents were not benchmarked for throughput.

### Results (tmpfs backing, medians; times in seconds unless noted)

| workload | native | FUSE no-pt | FUSE pt | FUSE pt (tuned) | tuned / native | NFS (devbox) |
|---|---:|---:|---:|---:|---:|---:|
| git worktree add | 0.46 | 1.76 | 1.68 | 1.39 | 3.0x | 11.6 |
| git worktree add, checkout.workers=16 | 0.33 | 1.08 | 1.01 | 0.86 | 2.6x | 3.89 |
| git worktree remove | 0.06 | 0.61 | 0.39 | 0.37 | 5.9x | 2.89 |
| git status, warm | 0.02 | 0.07 | 0.07 | 0.07 | 3.4x | 0.28 |
| git status, cold | 0.07 | 0.75 | 0.24 | 0.24 | 3.3x | - |
| find|xargs stat, warm | 0.03 | 0.07 | 0.06 | 0.06 | 2.3x | 0.28 |
| find|xargs stat, cold | 0.06 | 0.16 | 0.16 | 0.17 | 2.8x | - |
| create 10k x 4 KiB files | 0.74 | 2.35 | 2.14 | 1.52 | 2.1x | 12.5 |
| delete 10k files | 0.05 | 0.73 | 0.40 | 0.35 | 6.8x | 3.48 |
| cargo build (debug, 44 vendored crates) | 18.3 | 19.5 | 20.6 | 17.3 | 0.9x | - |
| 1 GiB seq write + fsync (MiB/s) | 8615 | 1146 | 9107 | 8904 | 1.0x | - |
| 1 GiB seq write, no fsync (MiB/s) | 8680 | 1351 | 9144 | 9752 | 1.1x | - |
| 1 GiB seq read, cold (MiB/s) | 9996 | 1530 | 8903 | 10039 | 1.0x | - |
| 1 GiB seq read, warm (MiB/s) | 9945 | 11839 | 9708 | 10139 | 1.0x | - |
| 4 KiB randread pread, warm (kIOPS) | 624 | 666 | 545 | 650 | 1.0x | - |
| 4 KiB randread pread, cold (IOPS) | 618160 | 35357 | 553497 | 650267 | 1.1x | - |
| 4 KiB randread mmap, warm (kIOPS) | 1439 | 1532 | 1386 | 1584 | 1.1x | - |
| cold stat, median (us) | 1.4 | 26.0 | 27.5 | 25.8 | 18.4x | - |
| cold stat, p99 (us) | 11.0 | 97.2 | 93.3 | 87.4 | 7.9x | - |
| cold open+close, median (us) | 2.5 | 78.2 | 91.7 | 62.2 | 24.4x | - |
| open+close after stat, median (us) | 2.9 | 50.4 | 60.0 | 35.7 | 12.5x | - |
| cold open+close, p99 (us) | 18.4 | 174.2 | 209.9 | 191.0 | 10.4x | - |
"tuned / native" is the tuned pt column over the average native. The ratios for latency and the metadata rows are what matters; the throughput rows are memory bandwidth on tmpfs and show only that passthrough adds nothing. Warm random reads and sequential reads hit the page cache, so those rows measure the kernel path, not the daemon.

Per-op latency (tmpfs, median of 3 runs of 3000 files): cold `stat` 26 us (native 1.4 us), cold open+close 62 us tuned / 78-92 us before the flush change (native 2.5 us), open+close of an already-looked-up file 36 us tuned (native 2.9 us). p99 is about 90-190 us. A bare request round trip is therefore about 25 us on a 2 vCPU VM; an open+close of a known file costs 36 us tuned (one round trip plus the backing-open ioctl and the release).

### Thread count (passthrough, tmpfs, medians)

| threads | wt_add (s) | small_create (s) | cold stat (us) | cold open (us) |
|---|---:|---:|---:|---:|
| 1 | 1.39 | 1.46 | 25.4 | 56.6 |
| 2 | 1.40 | 1.45 | 25.3 | 60.3 |
| 4 | 1.41 | 1.47 | 25.3 | 59.9 |
| 8 | 1.39 | 1.54 | 25.5 | 64.4 |

Request latency, not concurrency, sets the cost: these workloads issue one request at a time per process. (The pre-flush-change binary gave the same flat shape: 1.48-1.56 s.)

## What would close the gap

1. **Fewer round trips (done, partly).** NO_FLUSH took small-file creation from 1.93 s to 1.46 s, `worktree add` from 1.56 s to 1.41 s and cold open from 76 us to 60 us (same VM, 4 threads). The remaining cost in `git worktree add` is about 150 us per file, i.e. roughly 4-6 requests per file at 25 us (not profiled; likely a negative lookup, create, a `setattr` for mtime and the release, plus per-directory mkdir). The real client can probably avoid some of them, for example by creating files with the right mtime in one step.
2. **Cache settings.** TTLs are already long, negative entries cached, readdirplus on. After a restart the kernel caches are empty, hence the cold `git status` at 3x native; little further to gain beyond what `readdirplus` and `FOPEN_CACHE_DIR` already do. Cold `git status` without passthrough was 0.75 s against 0.24 s with it; I did not find out why (the only difference should be file reads, which git should not need after the index refresh).
3. **FUSE over io_uring.** The 25 us floor is mostly wake-up and context-switch latency between the kernel and the daemon thread; io_uring removes the `read(/dev/fuse)` wake-up and lets the daemon spin per CPU. I could not measure it here (see above), so its benefit on these workloads is an expectation, not a result. It needs `fuse.enable_uring=1` and a Rust implementation; worth re-checking `fractal-fuse` or a future `fuser` release before the client ships.
4. **Parallelism by the caller.** `checkout.workers=16` brought FUSE+passthrough `worktree add` from 1.39 s to 0.86 s (NFS: 11.6 s to 3.9 s). Round-trip-bound work scales with concurrency; thread count inside the daemon does not matter, concurrency in the callers does.
5. **Writeback cache is not available with passthrough.** If small-file writes ever matter more than large-file I/O, the choice is passthrough (no per-read/write cost) or writeback (batched small writes), not both on this kernel.
6. **Stack depth and overlayfs.** `max_stack_depth=2` allows the backing directory to be on overlayfs; not tested here, and the overlay-over-FUSE case (Docker layers on the mount) was not tested either.

## Not covered

- Real cache disk (NVMe) behaviour; only tmpfs and the throttled VM disk.
- NFS cold, throughput and build numbers.
- `fio` sequential throughput on tmpfs is memory-speed; do not read absolute GB/s as expected.
- Crash behaviour: a killed daemon takes the mount with it (ENOTCONN) and I did not test restart-on-existing-mount recovery.
- Remote-change simulation uses local operations as the stand-in for a sync engine; there is no server.

## Appendix A: per-VM tmpfs results (a = lfs-a, b = lfs-b; "v2" = tuned binary on lfs-a)

| workload | native a | nopt a | pt a | native b | nopt b | pt b | nopt a v2 | pt a v2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| git worktree add | 0.46 | 1.71 | 1.76 | 0.45 | 1.80 | 1.59 | 1.72 | 1.39 |
| git worktree add, checkout.workers=16 | 0.33 | 1.06 | 1.07 | 0.33 | 1.11 | 0.94 | 1.09 | 0.86 |
| git worktree remove | 0.06 | 0.59 | 0.41 | 0.06 | 0.62 | 0.38 | 0.64 | 0.37 |
| git status, warm | 0.02 | 0.06 | 0.06 | 0.02 | 0.08 | 0.07 | 0.07 | 0.07 |
| git status, cold | 0.08 | 1.02 | 0.25 | 0.07 | 0.47 | 0.22 | 1.07 | 0.24 |
| find|xargs stat, warm | 0.02 | 0.06 | 0.07 | 0.03 | 0.07 | 0.06 | 0.06 | 0.06 |
| find|xargs stat, cold | 0.06 | 0.16 | 0.17 | 0.06 | 0.17 | 0.15 | 0.19 | 0.17 |
| create 10k x 4 KiB files | 0.74 | 2.27 | 2.08 | 0.74 | 2.43 | 2.20 | 1.85 | 1.52 |
| delete 10k files | 0.05 | 0.70 | 0.40 | 0.05 | 0.76 | 0.39 | 0.68 | 0.35 |
| cargo build (debug, 44 vendored crates) | 17.9 | 20.0 | 20.5 | 18.8 | 19.0 | 20.8 | 17.9 | 17.3 |
| 1 GiB seq write + fsync (MiB/s) | 8904 | 1073 | 8904 | 8325 | 1219 | 9309 | 1158 | 8904 |
| 1 GiB seq write, no fsync (MiB/s) | 8533 | 1371 | 9062 | 8828 | 1332 | 9225 | 1364 | 9752 |
| 1 GiB seq read, cold (MiB/s) | 9752 | 1499 | 9481 | 10240 | 1561 | 8325 | 1499 | 10039 |
| 1 GiB seq read, warm (MiB/s) | 9752 | 11907 | 9846 | 10139 | 11770 | 9570 | 12047 | 10139 |
| 4 KiB randread pread, warm (kIOPS) | 623 | 676 | 507 | 624 | 657 | 583 | 689 | 650 |
| 4 KiB randread pread, cold (IOPS) | 605494 | 35212 | 520244 | 630827 | 35502 | 586751 | 37014 | 650267 |
| 4 KiB randread mmap, warm (kIOPS) | 1433 | 1615 | 1360 | 1446 | 1448 | 1412 | 1568 | 1584 |
| cold stat, median (us) | 1.4 | 26.1 | 25.4 | 1.4 | 25.8 | 29.7 | 30.9 | 25.8 |
| cold stat, p99 (us) | 5.9 | 98.3 | 82.5 | 16.1 | 96.1 | 104.1 | 100.4 | 87.4 |
| cold open+close, median (us) | 2.5 | 83.6 | 80.3 | 2.6 | 72.8 | 103.1 | 55.9 | 62.2 |
| open+close after stat, median (us) | 3.1 | 49.6 | 52.8 | 2.6 | 51.2 | 67.3 | 31.4 | 35.7 |
| cold open+close, p99 (us) | 17.9 | 202.6 | 179.8 | 19.0 | 145.8 | 240.1 | 133.3 | 191.0 |
## Appendix B: btrfs `/cache` backing (VM disk; noisy, disk-bound)

Same workloads, 5 runs, medians (each VM ran native, nopt, pt in that order; the disk throttled differently over time, e.g. native `worktree add` 15.8 s against 8.2 s for no-passthrough FUSE on the same VM, so differences below about 2x are not meaningful). "Warm" random reads here are only partly cached (the earlier cold phases leave a partial cache), unlike tmpfs. What the table does support: large-file I/O with passthrough tracks native (sequential write with fsync 11 and 20 against 10 MiB/s, warm sequential read 12-13 GB/s against 12-13 GB/s), and the metadata operations stay a small fraction of the disk-bound times.

| workload | native a | nopt a | pt a | native b | nopt b | pt b |
|---|---:|---:|---:|---:|---:|---:|
| git worktree add | 15.8 | 8.22 | 17.6 | 16.2 | 14.1 | 18.4 |
| git worktree add, checkout.workers=16 | 14.5 | 9.49 | 11.4 | 16.5 | 18.5 | 20.7 |
| git worktree remove | 0.24 | 0.88 | 0.67 | 1.10 | 1.13 | 0.65 |
| git status, warm | 0.02 | 0.07 | 0.07 | 0.03 | 0.07 | 0.07 |
| git status, cold | 0.19 | 1.14 | 0.42 | 0.19 | 1.15 | 0.39 |
| find|xargs stat, warm | 0.04 | 0.07 | 0.08 | 0.03 | 0.09 | 0.09 |
| find|xargs stat, cold | 0.46 | 0.59 | 0.59 | 0.47 | 0.90 | 0.59 |
| create 10k x 4 KiB files | 39.6 | 15.8 | 26.3 | 50.9 | 24.0 | 15.8 |
| delete 10k files | 0.23 | 1.09 | 0.73 | 0.23 | 1.09 | 0.73 |
| cargo build (debug, 44 vendored crates) | 23.2 | 20.5 | 19.8 | 20.7 | 22.3 | 21.3 |
| 1 GiB seq write + fsync (MiB/s) | 10 | 21 | 11 | 10 | 19 | 20 |
| 1 GiB seq write, no fsync (MiB/s) | 26 | 50 | 43 | 25 | 23 | 42 |
| 1 GiB seq read, cold (MiB/s) | 54 | 67 | 77 | 65 | 42 | 89 |
| 1 GiB seq read, warm (MiB/s) | 13474 | 9062 | 12337 | 11907 | 10343 | 13128 |
| 4 KiB randread pread, warm (kIOPS) | 661 | 607 | 603 | 647 | 661 | 623 |
| 4 KiB randread pread, cold (IOPS) | 562 | 151 | 880 | 987 | 24 | 1322 |
| 4 KiB randread mmap, warm (kIOPS) | 3 | 1 | 2 | 3 | 1 | 3 |
| cold stat, median (us) | 4.3 | 33.5 | 40.1 | 4.2 | 36.2 | 35.9 |
| cold stat, p99 (us) | 24.0 | 130.8 | 140.3 | 26.0 | 179.7 | 122.2 |
| cold open+close, median (us) | 5.6 | 79.5 | 93.0 | 5.7 | 113.7 | 95.5 |
| open+close after stat, median (us) | 2.6 | 50.2 | 53.7 | 2.9 | 54.2 | 59.8 |
| cold open+close, p99 (us) | 22.0 | 172.4 | 256.2 | 29.1 | 267.8 | 215.0 |
## Reproducing

```
cd experiments/fuse-pt
CARGO_TARGET_DIR=/home/daniel/.cache/leasefs-target/exp-fuse cargo build --release --target x86_64-unknown-linux-musl
# on a VM: fuse-pt and lat in /tmp, bare repo in /tmp/rs.git, vendored build project in /tmp/buildsrc
sudo fuse-pt --backing /cache/backing --mount /mnt/lfs --passthrough    # or --writeback for no passthrough
bench/inotify-check.sh pt
BASE=/mnt/ram SETTLE=0 bench/bench.sh pt lfs-a-ram 5
```

Raw per-run data: `E11-data/{cache,ram}-lfs-{a,b}.tsv`, `ram-lfs-a-v2.tsv`, `nfs-devbox.tsv`, `sweep-threads-v{1,2}.tsv` (columns: VM, config, workload, run, value, unit).
