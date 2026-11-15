# E12 - Cutting FUSE requests for metadata-heavy workloads

Date 2026-11-13. Code: `experiments/fuse-pt/` (daemon `src/`, `src/stats.rs` is new; drivers `bench/e12.sh`, `bench/e12-report.py`, probes `bench/probe2.sh`, `bench/pin.sh`, `bench/func.sh`). Raw data: `docs/experiments/E12-data/{vm,chase}/` (`results.tsv`: label, workload, run, value, unit; `mix-<label>-<workload>-<run>.tsv`: one request-mix dump per run).
Test VM `lfs-a` (2 vCPU, kernel 7.2.11, tmpfs backing, as in E11) and, for the last section, bare metal on Proxmox node `chase` (i5-9600T, 6 cores, kernel 7.0.12-1-pve, tmpfs backing).

## Summary

- **The E11 daemon was already close to the minimum number of requests.** Per-opcode counters show that, after the E11 settings (long TTLs, negative entries, readdirplus, `FOPEN_CACHE_DIR`, ENOSYS for flush and xattrs), most of what still reaches the daemon is required by the kernel's protocol: one `LOOKUP`+`CREATE` per new file (two lookups when the caller does `lstat` then `O_EXCL`), one `GETATTR` after a write, one `UNLINK` per file, one `FORGET` per evicted inode, one async `RELEASE` per open.
- **What the counters did find, and what was fixed:**
  - `OPENDIR`/`RELEASEDIR` were 10-65% of the requests on directory-heavy workloads. Replying ENOSYS to the first `OPENDIR` removes all of them: warm `git status` went from 1824 to 78 requests and from 0.059 s to 0.026 s (native 0.017 s), `find | xargs stat` warm from 0.074 to 0.039 s (1811 requests to 1), `git worktree remove` lost 1778 of 17716 requests.
  - A `RENAME` cost 2-3 ms in the daemon (an O(inode table) re-rooting scan, run even for plain files); now 15-110 us (`cargo build` does 86 of them).
  - `CREATE` did open, close, stat, open again; it now does one open and one `fstat` (median daemon time 32 us to 16 us).
- **Measured and rejected:** no-`OPEN` mode (`FUSE_NO_OPEN_SUPPORT` semantics, page-cache serving): open+close of a known file drops from 36 us to 3.4 us, but reading and writing small files gets slower (the data requests come back) and it cannot be combined with passthrough. `HANDLE_KILLPRIV_V2` and `EXPLICIT_INVAL_DATA` change no request count. More daemon threads change nothing.
- **Where the time goes now: latency per synchronous request, not request count.** One round trip costs 26 us on the VM and 12.4 us on bare metal (native `stat`: 1.4-1.5 us). Pinning the daemon and the caller to the same vCPU halves the VM figure (26 to 13 us), which shows that about half of the VM round trip is the cross-vCPU wake-up; on bare metal pinning does nothing. The remaining gap to native is therefore a per-request cost that only a different transport (FUSE over io_uring, per-CPU queues) can reduce.
- **Final numbers (tmpfs, medians, seconds):** native / E11-equivalent daemon / tuned daemon, VM: `git worktree add` (16 workers) 0.32 / 0.89 / 0.84, `worktree remove` 0.061 / 0.38 / 0.36, `git status` warm 0.017 / 0.059 / 0.026, create 10k files 0.66 / 1.49 / 1.43, delete 10k files 0.047 / 0.37 / 0.40. On bare metal the tuned daemon is 2.0x native on `worktree add`, 4.7x on `worktree remove`, 1.2x on warm `git status`, 6.5x on delete 10k.

## 1. Instrumentation

`src/stats.rs`: one atomic counter, one nanosecond sum and a 40-bucket power-of-two latency histogram per opcode, updated from a guard at the top of each `Filesystem` method. The latency is daemon-side handling time including the reply write, not the kernel round trip. A separate `LOOKUP(neg)` counter records lookups answered with a negative entry (a subset of `LOOKUP`, not added to totals). `SIGUSR1` writes the table to `--stats-file` (atomically, with a sequence number), `SIGUSR2` resets, and the table is printed to stderr at unmount when no file is given. `--trace` logs one line per `LOOKUP`/`GETATTR` with the path (used for the probes in section 3). `--stats-off` disables timing; I measured no difference in cold `stat` latency with and without it (24.4 vs 24.6 us median, noise level), so all runs have stats on. `bench/e12.sh` resets before and dumps after every workload run. Unhandled opcodes (`SETXATTR`, `REMOVEXATTR`, `FSYNCDIR`, ...) got explicit ENOSYS handlers so they are counted too; none occurred except one `GETXATTR` per mount.

Workloads: `git worktree add --detach` with `-c checkout.workers=16` (6164 files, 889 directories), `git worktree remove --force`, `git status` warm (second status after the index was refreshed twice, 2 s apart, see Caveats) and cold (daemon restarted, caches dropped), `find | xargs stat` warm and cold, reading every file of the worktree cold (`readall_cold`, 6164 files), create 10k x 4 KiB files (100 dirs x 100), `rm -rf` of them, `cargo build` of the vendored project, and the E11 per-op probes (`lat`). 5 runs, medians (3 for cargo and for ablations). Request counts are the median over runs and are very stable (within 1-2 requests except where stated).

## 2. Request mix per workload

"Before" is the E11 daemon plus counters and nothing else (`--passthrough`, 4 threads). Requests are daemon-side counts: one per request the kernel sent, `FORGET` counted per inode. `LOOKUP` includes the negative ones (last row). Cold-run counts start from a freshly mounted daemon.

### Before

| opcode | wt_add_w16 | wt_remove | status_warm | status_cold | small_create | small_delete | cargo_build | readall_cold |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| LOOKUP | 14287 | 1 | 1 | 7393 | 10101 |  | 2476 | 1 |
| RELEASE | 6401 | 12 | 25 | 25 | 10000 |  | 7140 | 6164 |
| FORGET | 6 | 7060 | 1 | 1 |  | 10101 | 455 |  |
| CREATE | 6177 |  | 1 | 1 | 10000 |  | 818 |  |
| UNLINK | 6 | 6171 | 1 | 1 |  | 10000 | 425 |  |
| OPEN | 224 | 12 | 24 | 24 |  |  | 6322 | 6164 |
| GETATTR | 6738 | 15 | 25 | 12 | 101 |  | 1627 | 10 |
| READDIRPLUS |  | 1777 |  | 1179 |  | 101 | 266 | 885 |
| OPENDIR | 36 | 889 | 873 | 873 |  | 101 | 204 | 905 |
| RELEASEDIR | 36 | 889 | 873 | 873 |  | 101 | 204 | 905 |
| READDIR |  | 1 |  | 567 |  | 101 | 129 | 885 |
| MKDIR | 889 |  |  |  | 101 |  | 247 |  |
| RMDIR |  | 889 |  |  |  | 101 | 150 |  |
| LINK |  |  |  |  |  |  | 131 |  |
| RENAME | 3 |  |  |  |  |  | 86 |  |
| SETATTR |  |  |  |  |  |  | 89 |  |
| STATFS |  |  |  |  |  | 1 | 8 | 1 |
| FLUSH |  |  |  | 1 |  |  |  | 1 |
| GETXATTR |  |  |  |  | 1 |  |  |  |
| **total** | **34803** | **17716** | **1824** | **10950** | **30303** | **20607** | **20772** | **15921** |
| of which negative LOOKUP | 14287 | 1 | 1 | 322 | 10101 | 0 | 2402 | 0 |

### After (`--tune default`: no-OPENDIR, single-open create, rename fix)

| opcode | wt_add_w16 | wt_remove | status_warm | status_cold | small_create | small_delete | cargo_build | readall_cold |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| LOOKUP | 14287 | 1 | 1 | 7393 | 10101 |  | 2476 | 1 |
| RELEASE | 6401 | 12 | 25 | 25 | 10000 |  | 7140 | 6164 |
| FORGET | 6 | 7060 | 1 | 1 |  | 10101 | 455 |  |
| CREATE | 6177 |  | 1 | 1 | 10000 |  | 818 |  |
| UNLINK | 6 | 6171 | 1 | 1 |  | 10000 | 425 |  |
| OPEN | 224 | 12 | 24 | 24 |  |  | 6322 | 6164 |
| GETATTR | 6737 | 15 | 25 | 12 | 101 |  | 1632 | 10 |
| READDIRPLUS |  | 1777 |  | 1179 |  | 101 | 266 | 885 |
| READDIR |  | 1 |  | 567 |  | 101 | 130 | 885 |
| MKDIR | 889 |  |  |  | 101 |  | 247 |  |
| RMDIR |  | 889 |  |  |  | 101 | 150 |  |
| LINK |  |  |  |  |  |  | 131 |  |
| RENAME | 3 |  |  |  |  |  | 86 |  |
| SETATTR |  |  |  |  |  |  | 89 |  |
| STATFS |  |  |  |  |  | 1 | 8 | 1 |
| FLUSH |  |  |  | 1 |  |  |  | 1 |
| OPENDIR |  |  |  | 1 |  |  |  | 1 |
| GETXATTR |  |  |  |  | 1 |  |  |  |
| **total** | **34730** | **15938** | **78** | **9205** | **30303** | **20405** | **20377** | **14112** |
| of which negative LOOKUP | 14287 | 1 | 1 | 322 | 10101 | 0 | 2402 | 0 |

Remarks on the mix:

- `wt_add_w16`: 34.8k requests for 7066 created names = 4.9 per name. All 14287 lookups are negative: 2 per created name (see section 3). Every file gets one `GETATTR` (git stats it after writing).
- `status_cold` / `statwalk_cold`: one `LOOKUP` per file (git `lstat`s in index order, before any directory is read, so `READDIRPLUS` cannot pre-populate the dentries; it only does for the walk). Each directory is read once. In `wt_remove` `READDIRPLUS` is 2 per directory (1777 for 889): the kernel needs an empty reply to see EOF.
- `small_delete`, `wt_remove`: one `UNLINK` per file, and a `FORGET` per inode (7060 and 10101, the majority of what is left in `small_delete`); they are asynchronous but the daemon still has to wake up for them.
- `cargo_build`: `OPEN`/`RELEASE` of 6322 files, 2476 `LOOKUP` (2402 negative, build systems probe a lot), 1632 `GETATTR`.

## 3. What the kernel forces (probes)

`bench/probe2.sh` runs single syscall sequences against a `--trace` daemon:

| sequence | requests seen |
|---|---|
| `open(O_CREAT\|O_EXCL)`, `close`, `stat` | `LOOKUP` (+ `CREATE`, not traced); no `GETATTR`: the create reply's attributes are used |
| the same with a `write` before the `close` | `LOOKUP`, `CREATE`, then **`GETATTR` on the first `stat`**, none on the second |
| `open`, `write`, `close`, `stat` on an existing file | `GETATTR` on the `stat` |
| `chmod`, `utime`, `truncate`, then `stat` | the `SETATTR` reply refreshes the attributes: no `GETATTR` |
| `mkdir` or `unlink`, then `stat` of the parent directory | one `GETATTR` of the parent (its mtime changed, only the daemon knows the new value) |

Consequences, which also explain the counts above:

1. **A write through passthrough invalidates the cached size/mtime of the inode** (the kernel cannot see what the backing fs did), so the next `stat` costs a `GETATTR`. `git checkout` stats every file it writes: 6737 `GETATTR` for 6177 files. No daemon-side setting avoids this. Only the writeback cache (kernel-owned size and times) does, and it is mutually exclusive with passthrough (E11); in the no-OPEN+writeback experiment below `GETATTR` fell from 6737 to 583 but `SETATTR` appeared instead.
2. **Creating a name takes 1 `LOOKUP` + `CREATE`**; if the caller already did an `lstat` of the name (git does) and then opens with `O_EXCL`, it is 2 lookups: the first fills the negative dentry, the `O_EXCL` create revalidates it with the daemon. With negative caching off (`--tune -neg`) `wt_add` needs 21372 lookups, 3 per name, i.e. the negative entry saves exactly the one for a later plain existence check. The second lookup is imposed by the kernel (inferred from counts and the probe; I did not trace the VFS).
3. **`FORGET`, `RELEASE`** are queued by the kernel and cannot be suppressed from the daemon, except `RELEASE` of regular files by the no-OPEN mode (below).
4. **Directory listing**: one extra `READDIR(PLUS)` per directory to get the EOF.
5. **`GETXATTR` (`security.capability`) on write**: the kernel asks once, gets ENOSYS and remembers it for the life of the mount (1 request in 10000 creates). Nothing to gain.

So for `git worktree add`, small-file create and delete, the request count per operation is at the kernel's floor (4.9, 3 and 2.0 per file respectively, where the floor is 4-5, 3 and 2).

## 4. Changes and their correctness argument

The daemon is a loopback stand-in. "Invalidation" below means what the real client must send when a *remote* change arrives (the simulator `remote.rs` already exercises `inval_inode`/`inval_entry` on the E11 settings). Kernel caches rely on TTL 3600 s plus explicit invalidation; local changes go through the mount, so the kernel updates its own view.

| change | requests saved | correctness / invalidation |
|---|---|---|
| **No-OPENDIR**: first `OPENDIR` replies ENOSYS; the kernel stops sending `OPENDIR`/`RELEASEDIR` and treats directories as cached (observed: warm runs show no `READDIR` at all). `readdir`/`readdirplus` get `fh 0` and use a per-inode snapshot taken at offset 0, freed at the EOF request or on `FORGET` of the inode. | 1778 of 17716 (`wt_remove`), 1746 of 1824 (`status_warm`), 1746 of 3592 (`statwalk_cold`) | Cached directory pages are invalidated by the kernel when the directory's mtime changes through the mount; for remote changes the daemon must call `notify_inval_inode(dir)` (drops the page cache of the listing) and `notify_inval_entry(dir, name)` per changed name. The daemon needs the snapshot only between the first and last page of one listing; two concurrent listings of the same directory share it and a listing restarted at offset 0 replaces it, so a concurrent change can show or skip an entry during one listing (same as any readdir without stable cookies; the E11 design had the same property per open handle). The real client should use stable per-entry cookies instead of positions. |
| **Single-open create** (`create1`): `open(O_CREAT)`, `fchown`, `fstat` on one fd, which is then registered as the backing file; E11 did open, close, stat, open. | 0 requests; 3 syscalls less per create, daemon median 32 to 16 us, mean 89 to 77 us | Same semantics: the file is created exclusively with the caller's flags and mode, owned by the caller; the attributes in the reply come from the fd that was created. |
| **Rename fix**: re-rooting cached child paths is only needed when the moved inode is a directory. | 0 requests; `RENAME` 2.1-3.1 ms to 15-110 us (86 in `cargo build`) | Pure daemon-side bookkeeping of the loopback's path table (the real client has its own inode table); the kernel is unaffected. |

Already in place from E11, kept, now with the effect of removing each one (`--tune -neg`, `-rdplus`; 3 runs):

| setting | with | without | why it is correct |
|---|---|---|---|
| negative entries (ino 0 with TTL) | `wt_add` 0.835 s, 14287 lookups | 1.001 s, 21372 lookups | A name created or renamed in by anyone through the mount replaces the negative dentry. For a remote create the daemon calls `notify_inval_entry(parent, name)` (or `notify_inval_inode(parent)` for many). |
| readdirplus (`DO_READDIRPLUS`+`AUTO`) | `statwalk_cold` 0.131 s, 1783 requests; `readall_cold` 0.370 s | 0.273 s, 8832 requests; 0.556 s | Each returned entry is a lookup (attributes and entry TTL). Same invalidation as lookups: `inval_entry` for a changed or removed name, `inval_inode` for changed attributes or data. |
| attr/entry TTL 3600 s, `FOPEN_KEEP_CACHE` (non-passthrough opens) | warm `stat` and `open` take no request | n/a | Safe only with invalidation: `inval_inode(ino, 0, -1)` when remote data changes, `inval_inode(ino, -1, 0)`-style attribute invalidation when only attributes change. A daemon that cannot deliver the notification in time (disconnected from the sync engine) has to lower the TTL for the affected subtree. |
| `flush` and xattr ops ENOSYS | no `FLUSH` per close, 1 `GETXATTR` per mount | E11: about 20% on small-file workloads | The kernel still writes back dirty pages before `close` returns; leases/locks are not implemented by the prototype, so there is nothing to release at close. A real client with locks or close-to-open semantics needs `FLUSH` back (then it costs 1 request per file again). The kernel reads `security.capability` only through xattrs, so a filesystem that does not store file capabilities loses nothing. |
| `CACHE_SYMLINKS`, `PARALLEL_DIROPS`, `ASYNC_READ` | | | `readlink` result cached until the inode is invalidated; the others change concurrency only, not what is cached. |

## 5. Rejected candidates (measured)

All numbers: VM, tmpfs, medians, `before` = E11 settings, `after` = the table above.

| candidate | result | verdict |
|---|---|---|
| `FUSE_HANDLE_KILLPRIV_V2` (`--tune killpriv`) and `FUSE_EXPLICIT_INVAL_DATA` (`explicit`), together = `all` | request counts identical to `after` on every workload; timings equal within noise | no gain; both change who clears suid/cap bits and who drops cached pages, so there is a correctness cost for no benefit. Left off. |
| daemon threads 1 / 4 / 8 | `wt_add` 0.858 / 0.835 / 0.859 s, `wt_remove` 0.324 / 0.356 / 0.362, cold stat 25.2 / 26.9 / 26.3 us | no effect, as in E11: the callers wait for one request at a time. |
| **No-OPEN mode** (`--tune noopen`, kernel `no_open`+no `RELEASE`; no passthrough, page cache and `READ`/`WRITE` through the daemon), also with writeback cache (`noopen_wb`) | open+close after a stat: 3.4 us (native 2.5, passthrough 35.1); cold open+close 31.5 us (61.4). But `readall_cold` 0.518 s (0.370), `wt_add` 1.055 s (0.835), `small_create` 2.02 s (1.43), with writeback 1.78 s; `small_delete` and `wt_remove` +50% with writeback (an extra `SETATTR` per unlink) | **Loses wherever data moves.** Mix with no-OPEN: `readall_cold` 6812 `READ`s replace 6164 `OPEN` + 6164 `RELEASE` (async); `small_create` 20000 `WRITE`s replace 10000 `RELEASE`s. It only wins for code that opens and closes without reading. It also cannot be combined with passthrough: a file created with a passthrough backing makes every later open of the inode (served without `OPEN`, so not passthrough) fail with EIO, and since the kernel sends no `RELEASE` the daemon cannot know when to drop the backing id or fd. Passthrough needs the `OPEN` round trip to hand over the backing id, so the choice is passthrough with `OPEN`, or no `OPEN` without passthrough. Kept as `--tune noopen` for reference. |

Bugs found in the no-OPEN experiment (fixed in the experiment, relevant for any `fh 0` design): `setattr(size)` with `fh 0` called `ftruncate(0)` on the daemon's stdin; fds of created files leaked because no `RELEASE` ever came; `fsync`, `fallocate`, `lseek`, `read`, `write` need a by-path fallback when there is no handle.

## 6. Final results (VM, tmpfs backing)

Times in seconds, medians of 5 runs (3 for cargo); requests are the totals of section 2 (before to after). "tuned/native" is `after` over native.

| workload | native | before | after | tuned/native | requests before to after |
|---|---:|---:|---:|---:|---|
| git worktree add, workers=16 | 0.319 | 0.887 | 0.835 | 2.6x | 34803 to 34730 |
| git worktree remove | 0.061 | 0.381 | 0.356 | 5.8x | 17716 to 15938 |
| git status, warm | 0.017 | 0.059 | 0.026 | 1.5x | 1824 to 78 |
| find + xargs stat, warm | 0.026 | 0.074 | 0.039 | 1.5x | 1811 to 1 |
| git status, cold | 0.077 | 0.244 | 0.214 | 2.8x | 10950 to 9205 |
| find + xargs stat, cold | 0.053 | 0.161 | 0.131 | 2.5x | 3592 to 1783 |
| cat every file, cold (6164 files) | 0.087 | 0.365 | 0.370 | 4.3x | 15921 to 14112 |
| create 10k x 4 KiB files | 0.656 | 1.487 | 1.434 | 2.2x | 30303 to 30303 |
| delete 10k files | 0.047 | 0.373 | 0.397 | 8.4x | 20607 to 20405 |
| cargo build (44 crates), s | 17.2 | 17.7 | 17.9 | 1.04x | 20772 to 20377 |
| cold `stat`, median (us) | 1.4 | 26.3 | 26.9 | 19x | 1 per file |
| cold open+close, median (us) | 2.5 | 66.8 | 61.4 | 25x | 2 per file (`LOOKUP`+`OPEN`) |
| open+close after stat, median (us) | 2.5 | 36.0 | 35.1 | 14x | 1 (`OPEN`) |

Noise: single runs vary by 10-20% (the `all` configuration, request-identical to `after`, measured `wt_add` 1.39 s in one batch), so differences below about 10% are not significant. Significant: warm `status`/`stat` walk (2.3x and 1.9x faster), `RENAME` latency, the request reductions. `delete 10k` shows 0.373 vs 0.397: same requests, noise.

Ablation (3 runs, same session):

| config | wt_add | wt_remove | status warm | status cold | statwalk cold | readall cold | small_create | small_delete |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| before | 0.887 | 0.381 | 0.059 | 0.244 | 0.161 | 0.365 | 1.487 | 0.373 |
| nodir only | 0.854 | 0.362 | 0.025 | 0.216 | 0.143 | 0.360 | 1.655 | 0.360 |
| create1 only | 0.834 | 0.377 | 0.067 | 0.261 | 0.179 | 0.424 | 1.753 | 0.425 |
| after (both) | 0.835 | 0.356 | 0.026 | 0.214 | 0.131 | 0.370 | 1.434 | 0.397 |
| no negative entries | 1.001 | 0.384 | 0.034 | 0.252 | 0.138 | 0.398 | 1.452 | 0.392 |
| no readdirplus | 0.894 | 0.374 | 0.022 | 0.189 | 0.273 | 0.556 | 1.428 | 0.328 |
| noopen | 1.055 | 0.393 | 0.023 | 0.212 | 0.132 | 0.518 | 2.024 | 0.374 |
| noopen + writeback | 0.925 | 0.524 | 0.022 | 0.745 | 0.145 | 0.494 | 1.777 | 0.605 |

`nodir` is the one with a clear effect (warm status/stat walk); `create1` and the rename fix are daemon-side savings that show up in the daemon's own latency (CREATE p50, RENAME) more than in wall time.

## 7. Where the remaining time goes

Tuned daemon, VM: every workload is one synchronous round trip per mutation and per cold lookup, at about 26 us each (native 1.4-2.5 us). Examples: delete 10k files = 10000 `UNLINK` x 26 us = 0.26 s plus the `FORGET` stream (0.40 s measured); `status_cold` = 7393 `LOOKUP` x 26 us = 0.19 s (0.214 s measured).

**CPU placement (VM).** `bench/pin.sh`, 3000 files, medians:

| placement | cold `stat` | cold open+close |
|---|---:|---:|
| unpinned (4 threads) | 24.8 us | 59.9 us |
| 1 thread, unpinned | 24.4 us | 55.4 us |
| daemon on cpu1, caller on cpu0 | 24.4 us | 55.4 us |
| **daemon and caller on cpu0** | **11.2 us** | **31.8 us** |

On the 2-vCPU VM, half of a round trip is waking the other vCPU (idle exit and IPI, both expensive under virtualization); it is gone when both sides share a CPU. The same `e12.sh` matrix run with everything under `taskset -c 0` (3 runs): cold stat 26.9 to 12.9 us, `wt_remove` 0.356 to 0.272 s, `small_delete` 0.397 to 0.241 s, `small_create` 1.43 to 1.25 s, but `wt_add` with 16 workers gets worse (1.15 s against native 0.54 s on one CPU) because it is CPU-bound there. A daemon cannot choose where the caller runs, so this is not a setting; it says that a transport that avoids sleeping-thread wake-ups (io_uring queue per CPU, handler on the submitting CPU, or a polling thread) is where the remaining 2x on the VM and some of the 12 us on bare metal is. `fuser` 0.18 has no io_uring support and `fuse.enable_uring` is `N` on the VM; not tried.

## 8. Bare metal (Proxmox node `chase`)

Setup (all under `/var/tmp/lfs-bench/`, which no longer exists): tmpfs (`size=3g`) mounted there as backing, FUSE mount in the same directory, static musl `fuse-pt` (before and after builds) and `lat` copied in. The node has no `git`, so I copied the devbox's git 2.56.1 with its own glibc and two libraries into the directory and ran it through the bundled `ld-linux` (nothing installed, no host files modified). The repository was a tar of the same 6164-file clone the VM uses, extracted into the mount. Skipped: `cargo build` (no toolchain; not installed), `sync` and `drop_caches` (they act on the host's caches and on production I/O; so "cold" here is only the daemon restart: kernel dentries and attributes of the FUSE mount are gone, but native tmpfs "cold" is the same as warm). The node was running its normal load (load average about 0.6, Ceph OSD, VMs). 5 runs, medians; kernel 7.0.12 instead of 7.2.11.

| workload (s) | native | before | after | tuned/native | VM tuned/native |
|---|---:|---:|---:|---:|---:|
| git worktree add, workers=16 | 0.251 | 0.499 | 0.492 | 2.0x | 2.6x |
| git worktree remove | 0.064 | 0.307 | 0.300 | 4.7x | 5.8x |
| git status, warm | 0.017 | 0.045 | 0.020 | 1.2x | 1.5x |
| find + xargs stat, warm | 0.024 | 0.056 | 0.026 | 1.1x | 1.5x |
| git status, cold (daemon restart only) | 0.017 | 0.124 | 0.123 | 7.2x | n/a |
| find + xargs stat, cold | 0.024 | 0.137 | 0.110 | 4.6x | n/a |
| cat every file | 0.127 | 0.333 | 0.330 | 2.6x | 4.3x |
| create 10k x 4 KiB files | 0.837 | 1.615 | 1.565 | 1.9x | 2.2x |
| delete 10k files | 0.048 | 0.314 | 0.312 | 6.5x | 8.4x |
| cold `stat` median (us) | 1.5 | 12.4 | 12.4 | 8x | 19x |
| cold open+close median (us) | 2.3 | 29.7 | 29.9 | 13x | 25x |
| open+close after stat median (us) | 2.3 | 17.3 | 16.7 | 7x | 14x |

Same workloads pinned to one core (`taskset -c 2`, 3 runs): cold `stat` 13.4 us (unpinned 12.4), `wt_remove` 0.301 s, `small_delete` 0.307 s; no gain, as expected when there is no virtualized wake-up (full table in `E12-data/chase/results.tsv`, labels `*_cpu2`).

What this separates:

- **FUSE overhead proper** (bare metal): 12.4 us per cold lookup against 1.5 us, so 6-8x on pure-metadata operations (`worktree remove` 4.7x, `delete` 6.5x, cold `status` 7x), 2x on `worktree add` and create, no penalty on `cargo build` or file contents (E11).
- **VM overhead**: roughly doubles the round trip (12.4 to 26 us) and moves the ratios on the metadata-bound rows by about 25-60% (e.g. `wt_remove` 4.7x to 5.8x, cold open+close 13x to 25x). Throughput-style rows (`wt_add`, `small_create`) are less affected because concurrent callers overlap round trips. Absolute `small_delete` is almost the same on both (0.31 s vs 0.37-0.40 s): at 31 us per file on bare metal against a 12 us round trip, it is bounded by more than the `UNLINK` latency (the `FORGET` per inode and daemon CPU; I did not profile it further).
- The tuned daemon helps the same two rows as on the VM (warm `status`/stat walk: 2.2x and 2.2x faster, to 1.2x and 1.1x native); everything else is within noise because the request counts did not change.

Cleanup: after the runs I stopped nothing but my own processes, unmounted the FUSE mount and the tmpfs, and deleted `/var/tmp/lfs-bench/` (binaries, git bundle, 260 MB repo tar, results). Checked afterwards: no `fuse-pt` process, no mount containing `lfs-bench`, directory gone, `/var/tmp` listing has no `lfs` entries, root filesystem usage unchanged at 21 GiB. No sysctls, modules, packages or other nodes touched.

## 9. Suggestions for the real client

1. Keep the passthrough + `OPEN` design; answer `OPENDIR` with ENOSYS and serve directories from stable cookies (no per-open state); keep ENOSYS for xattrs unless they are needed. These give the only request cuts that exist (`OPENDIR`/`RELEASEDIR`, up to 2/3 of warm directory traffic).
2. Do not count on fewer requests for create/delete/checkout workloads: they sit at the kernel's floor (`LOOKUP` before `CREATE`, `GETATTR` after a write, `UNLINK`, `FORGET`). To go faster, create files in fewer, larger operations on the caller side (git already parallelizes with `checkout.workers`: 16 workers made `worktree add` 2.6x faster in E11), or reduce the per-request cost.
3. Per-request cost is the lever: FUSE over io_uring (per-CPU queues; the 7.x kernels have it, `fuser` does not yet) or a polling receive thread, then re-run this matrix. On this VM, same-CPU wake-ups halve the round trip, which is what such a transport emulates.
4. Remember `FLUSH` (close-to-open) when real consistency is added: ENOSYS on flush saves one request per close today only because the prototype has nothing to do at close.

## Caveats

- Run-to-run noise is 10-20% on the VM (2 vCPU, shared host), so conclusions rest on request counts, which are exact, and on effects larger than that.
- Git's "racily clean" handling skews `git status` warm: entries whose mtime equals the index's make git re-read the file on every status. An earlier batch with the index refreshed once showed 0.22-0.27 s warm status (hundreds of extra `OPEN`s) for some configurations; the final runs refresh the index twice, 2 s apart, and every config gets the same preparation. The old numbers were discarded.
- Cold numbers on the VM use `drop_caches` plus a daemon restart; bare-metal "cold" is a daemon restart only.
- The loopback daemon is path based and its inode table differs from the real client's; only the kernel-visible request pattern transfers.
- Counters measure daemon-side handling time (including the reply write), so they do not replace the `lat` round-trip probe.

## Reproducing

```
cd experiments/fuse-pt
CARGO_TARGET_DIR=/home/daniel/.cache/leasefs-target/exp-fuse cargo build --release --target x86_64-unknown-linux-musl
# daemon flags: --passthrough [--writeback] [--threads N] [--stats-file F | --stats-off] [--trace]
#               --tune default|none|all|nodir,create1,noopen,killpriv,explicit,neg,rdplus  (prefix - disables)
# VM: fuse-pt/lat in /tmp, bare repo /tmp/rs.git, vendored project /tmp/buildsrc
BIN=/tmp/fuse-pt FLAGS="--passthrough --tune default" OUT=/tmp/e12-out bench/e12.sh fuse after 5
bench/e12.sh native native 5
python3 -I bench/e12-report.py /tmp/e12-out native,before,after
# probes: bench/probe2.sh <bin> --passthrough   bench/pin.sh   bench/func.sh <tune>
```
