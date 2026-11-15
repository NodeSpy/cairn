# E14 - FUSE over io_uring integrated into the fuse-pt prototype

Date 2026-11-13. Code: `experiments/fuse-pt/` (`--uring`, `--uring-depth N`, `--uring-fail-test`; vendored `vendor/fuser/`, new `vendor/fuser/src/uring.rs`). Raw data: `docs/experiments/E14-data/{vm-a,vm-b,chase}/results.tsv` (+ request mixes, daemon logs). Built with `CARGO_TARGET_DIR=/home/daniel/.cache/leasefs-target/exp-fuse`, static musl. Not committed.

## Result

- **VMs (2 vCPU, kernel 7.2.11): io_uring is clearly faster than tuned classic.** Cold `stat` 26.9 to 13.5 us (-50%), open+close 41 to 21 us, `worktree remove` -17/-25%, delete 10k -29/-30%, cat-all-cold -30/-33%, `worktree add` -14/-17%. Still 8-16x native per synchronous op.
- **Bare metal (chase, i5-9600T, kernel 7.0.12): no win; slightly slower (0-26%).** Cold `stat` 12.5 us classic vs 14.3 us uring; open+close 16.0 vs 20.2 us; `worktree add` 6% faster, `worktree remove`/delete/create 3-7% slower, within noise on most rows. This confirms the E13 caveat: the VM gain is avoided cross-vCPU wakeups, and on bare metal a classic multi-thread daemon has no such cost to remove, while the ring path adds per-request copying (reassembling the request, copying the reply into the payload buffer).
- Net: the transport pays off only where wakeups are expensive (guests). For a bare-metal client it is not worth the extra complexity on this evidence. Not tried: SQPOLL/thread-per-CPU spinning, zero-copy reply, other CPUs.

## Implementation (fuser 0.18.0 vendored, about 450 lines added, 26 removed)

`vendor/fuser/Cargo.toml`: examples and dev-dependencies removed, `io-uring = "0.7"` added. fuse-pt's `Cargo.toml` uses `fuser = { path = "vendor/fuser" }`. Changes vs upstream (`diff -ru`): new `src/uring.rs` 334 lines; `session.rs` +92/-16; `request.rs` +7/-8; `reply.rs` +7/-2; `lib.rs` +5; `mnt/mount_options.rs` +7; `experiments/fuse-pt/src/main.rs` flags. Nothing had to become `pub` beyond the new `Config` fields (`uring`, `uring_depth`, `uring_fail_test`) and `Session::uring_active()`: the ring code lives inside the crate and uses `AnyRequest`, `RequestWithSender::dispatch` and the `Reply*` types unchanged.

- **Negotiation:** after `Filesystem::init`, the handshake adds `FUSE_OVER_IO_URING` to the requested capabilities, only if `Config::uring` and a probe `io_uring_setup(SQE128)` succeeds (otherwise the kernel would wait forever for queue registrations). INIT itself stays on `/dev/fuse`.
- **Rings:** one thread per possible CPU (`/sys/devices/system/cpu/possible`, matching `num_possible_cpus()`), pinned, one SQE128 ring each, `depth` (default 4) entries. Header buffer 512 B, payload = max(max_write, max_pages*4096) = 1 MiB in fuse-pt, mmap'd lazily (MAP_NORESERVE). REGISTER retries on EAGAIN (INIT race).
- **Request path:** the kernel's `in_header.len` covers header + op args + payload, so `op_in` length = `len - 40 - payload_sz`; no per-opcode table. The request is reassembled into a per-thread scratch buffer (`in_header + op_in + payload`) and fed to the existing decoder and `dispatch`. `RequestWithSender` now holds a `ReplySender` (cloneable), with a new `Ring` variant.
- **Reply path:** `ReplySender::Ring::send` splits the `fuse_out_header` (into `in_out`) and the args (into the payload buffer, `payload_sz` in `ring_ent_in_out`); after dispatch returns the loop pushes `COMMIT_AND_FETCH`, which also re-arms the entry. Backing ids (`open_backing`/`wrap_backing`) go through the `/dev/fuse` channel unchanged. Notifier unchanged (write(2)). Per-opcode stats unchanged (they live in the handlers).
- **Classic reader:** one `/dev/fuse` reader runs for FORGET/INTERRUPT (counted ~1 per inode forget on `fuser-0` via `/proc/<pid>/task/*/io`: 1944 reads for 2000 created+deleted files).
- **Fallback:** (a) kernel does not offer the flag (`enable_uring=0`) or io_uring unavailable: classic loop with the usual thread count (fuse-pt prints `transport=classic (io_uring requested, not available)`); (b) a REGISTER fails: the kernel drops to `/dev/fuse`; the failing ring thread starts the remaining classic readers. Tested with `--uring-fail-test` (queue 0 registers with a bad qid): workload completed, reads spread over 4 classic threads.

Gotchas:
1. Handlers must reply before returning (all of fuse-pt does). A late or cross-thread reply is rejected (epoch check) and the entry is committed with EIO. A blocking handler stalls its CPU's queue (no stealing between queues); a real client with remote round trips needs a worker pool plus a reply path to the owning ring.
2. `UringCmd80` has no setter for `sqe->len`; the SQE byte at offset 24 is written (as in E13).
3. All possible CPUs must register or requests block, so the probe before offering the flag matters. Offline CPUs: pinning fails and is ignored; the ring still registers.
4. `gen` is a reserved word in edition 2024 (rename needed). `Config` is `non_exhaustive`: set fields by assignment.
5. The `log` crate has no logger in fuse-pt, so fuser warnings (fallback reasons) are not printed; only the `transport=` line is.
6. fuse-pt's `e12.sh` needs the binary to be called `fuse-pt` (pgrep -x). Also `/tmp/lat` on lfs-b was the E13 tool, not the E12 one; the lat rows for lfs-b were rerun (`E14-data/vm-b/lat-rerun.tsv`, same method, one tree, 3 runs per phase).
7. The classic baseline is the same binary without `--uring`; it carries the small vendored-fuser changes (cloneable ReplySender), which I did not measure separately.

## Correctness checks

| check | result |
|---|---|
| E11 inotify check (`bench/inotify-check.sh`, now with `BIN`/`EXTRA` env; `EXTRA=--uring`), lfs-a and lfs-b | 40/40 cycles ok, 0 failed; events ATTRIB/MOVED_FROM/MOVED_TO/DELETE/CREATE/CLOSE_WRITE = 40 each, MODIFY 69 (b) and 72 (a) with uring vs 76 and 78 classic (MODIFY count varies run to run under the concurrent load); daemon exited cleanly; `transport=io_uring` in the log |
| unmount (`umount`) | rc 0 in 15 ms, daemon exits, mount gone |
| SIGTERM to the daemon | unmounts and exits |
| SIGKILL under a stat loop | client got an error after 453 iterations, `ls` returns ENOTCONN, `umount` rc 0, nothing wedged. SIGKILL idle: `stat` fails with ENOTCONN at once |
| passthrough | 64 MiB dd 3.1 GB/s, no READ/WRITE reached the daemon; backing ids opened from ring handlers |
| tree equality (lfs-b) | `git worktree add -c checkout.workers=16` through the uring mount: `git status` clean, content md5 and listing md5 identical to the backing dir |
| fallback (enable_uring=0; forced REGISTER failure) | both complete a 300-file workload |

## Method

As E12: tmpfs backing, `bench/e12.sh`, 5 runs and medians (3 for `cargo build`, VMs only; 3 for the lat probes), config `--passthrough --tune default` (classic) and the same plus `--uring`. Order native, classic, uring in one pass per host; the two VMs ran in parallel. lfs-a: `enable_uring` set to 1 via sysfs (lfs-b was already Y); both left as is. Cold = `drop_caches` + daemon restart on VMs; on chase cold is a daemon restart only (no sync or drop_caches). Medians of seconds unless noted. Request counts equal across transports (same handlers). Run-to-run noise on VMs is 10-20% (E12).

## Results (medians; ratios are time ratios, lower is better)

### lfs-a

| workload | native | classic | uring | classic/native | uring/native | uring/classic |
|---|---:|---:|---:|---:|---:|---:|
| git worktree add (16 workers) | 0.319 | 0.942 | 0.785 | 3.0x | 2.5x | 0.83 |
| git worktree remove | 0.063 | 0.404 | 0.335 | 6.4x | 5.3x | 0.83 |
| git status warm | 0.022 | 0.024 | 0.021 | 1.1x | 1.0x | 0.87 |
| git status cold | 0.082 | 0.266 | 0.201 | 3.3x | 2.5x | 0.76 |
| find+stat warm | 0.036 | 0.035 | 0.031 | 1.0x | 0.9x | 0.89 |
| find+stat cold | 0.058 | 0.163 | 0.129 | 2.8x | 2.2x | 0.80 |
| cat all files cold | 0.099 | 0.424 | 0.297 | 4.3x | 3.0x | 0.70 |
| create 10k files | 0.738 | 1.623 | 1.441 | 2.2x | 2.0x | 0.89 |
| delete 10k files | 0.053 | 0.412 | 0.292 | 7.8x | 5.5x | 0.71 |
| cargo build | 19.815 | 20.910 | 20.341 | 1.1x | 1.0x | 0.97 |
| cold stat (us) | 1.400 | 26.900 | 13.500 | 19.2x | 9.6x | 0.50 |
| cold open+close (us) | 2.500 | 76.200 | 40.900 | 30.5x | 16.4x | 0.54 |
| open+close after stat (us) | 2.500 | 41.000 | 21.200 | 16.4x | 8.5x | 0.52 |

### lfs-b

| workload | native | classic | uring | classic/native | uring/native | uring/classic |
|---|---:|---:|---:|---:|---:|---:|
| git worktree add (16 workers) | 0.337 | 0.938 | 0.803 | 2.8x | 2.4x | 0.86 |
| git worktree remove | 0.063 | 0.417 | 0.312 | 6.6x | 4.9x | 0.75 |
| git status warm | 0.019 | 0.028 | 0.026 | 1.4x | 1.3x | 0.95 |
| git status cold | 0.062 | 0.232 | 0.190 | 3.7x | 3.1x | 0.82 |
| find+stat warm | 0.024 | 0.039 | 0.038 | 1.6x | 1.6x | 0.98 |
| find+stat cold | 0.061 | 0.163 | 0.125 | 2.7x | 2.1x | 0.77 |
| cat all files cold | 0.100 | 0.449 | 0.301 | 4.5x | 3.0x | 0.67 |
| create 10k files | 0.790 | 1.744 | 1.483 | 2.2x | 1.9x | 0.85 |
| delete 10k files | 0.052 | 0.431 | 0.303 | 8.2x | 5.8x | 0.70 |
| cargo build | 19.768 | 21.285 | 20.368 | 1.1x | 1.0x | 0.96 |
| cold stat (us) | 1.400 | 26.200 | 13.300 | 18.7x | 9.5x | 0.51 |
| cold open+close (us) | 2.500 | 63.800 | 36.000 | 25.5x | 14.4x | 0.56 |
| open+close after stat (us) | 2.500 | 35.500 | 20.900 | 14.2x | 8.4x | 0.59 |

### chase

| workload | native | classic | uring | classic/native | uring/native | uring/classic |
|---|---:|---:|---:|---:|---:|---:|
| git worktree add (16 workers) | 0.242 | 0.496 | 0.464 | 2.1x | 1.9x | 0.94 |
| git worktree remove | 0.064 | 0.298 | 0.307 | 4.6x | 4.8x | 1.03 |
| git status warm | 0.017 | 0.020 | 0.019 | 1.2x | 1.1x | 0.95 |
| git status cold | 0.017 | 0.109 | 0.117 | 6.2x | 6.7x | 1.08 |
| find+stat warm | 0.025 | 0.027 | 0.027 | 1.1x | 1.1x | 1.00 |
| find+stat cold | 0.024 | 0.117 | 0.115 | 4.9x | 4.8x | 0.98 |
| cat all files cold | 0.129 | 0.317 | 0.363 | 2.5x | 2.8x | 1.14 |
| create 10k files | 0.844 | 1.571 | 1.625 | 1.9x | 1.9x | 1.03 |
| delete 10k files | 0.048 | 0.310 | 0.332 | 6.4x | 6.9x | 1.07 |
| cold stat (us) | 1.500 | 12.500 | 14.300 | 8.3x | 9.5x | 1.14 |
| cold open+close (us) | 2.300 | 30.400 | 35.500 | 13.2x | 15.4x | 1.17 |
| open+close after stat (us) | 2.300 | 16.000 | 20.200 | 7.0x | 8.8x | 1.26 |

## Bare metal run on chase: setup, restore and cleanup

- Original `/sys/module/fuse/parameters/enable_uring` read before anything else: **N**. Set to 1 for the run (log: `enable_uring=Y` during the runs). Restored to **N** afterwards; verified by reading it back: `N`.
- Only `/var/tmp/lfs-bench/` was used: tmpfs (size=3g) mounted under it, FUSE mount at `lfs-bench/mnt`, static `fuse-pt` and `lat` plus the devbox git 2.56.1 bundled with its own glibc (run via a wrapper, `GIT_EXEC_PATH` pointed at the bundle), repo tar of the 6164-file clone. Nothing installed. No `sync`, no `drop_caches`. `cargo build` skipped (no toolchain). Normal node load (load about 0.5, Ceph OSD, VMs) was running.
- Cleanup: FUSE mount and tmpfs unmounted, `/var/tmp/lfs-bench` deleted. Verified afterwards: no `fuse-pt`/lfs processes, `mount | grep lfs-bench` empty, directory gone, no lfs entry in `/var/tmp`, root fs usage unchanged (21 GiB). Nothing else on chase was changed. Kernel 7.0.12-1-pve.
- Local temp files (`/tmp/chase-pack`) and `/tmp/repo.tar` on lfs-b removed. lfs-b got a tmpfs mounted on `/mnt/ram` for the run (throwaway VM).

## Reproducing

```
cd experiments/fuse-pt
CARGO_TARGET_DIR=/home/daniel/.cache/leasefs-target/exp-fuse cargo build --release --target x86_64-unknown-linux-musl
# daemon: --passthrough --tune default [--uring] [--uring-depth N]
echo 1 > /sys/module/fuse/parameters/enable_uring   # before mount
BIN=/tmp/u/fuse-pt FLAGS="--passthrough --tune default --uring" bash bench/e12.sh fuse uring 5
BIN=/tmp/u/fuse-pt EXTRA=--uring bash bench/inotify-check.sh pt
```
