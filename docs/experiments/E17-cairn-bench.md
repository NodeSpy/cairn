# E17 - cairnd vs native btrfs benchmark

Date 2026-11-14. Suite: `tests/bench/run.sh a 2` (provisions the server on lfs-srv:7420, mounts cairnd on lfs-a, runs `bench.sh` for native, cairnd classic and `cairnd --uring`; `fuse.enable_uring=1` on the VM). Workloads ported from E11/E12/E14. Native = `/cache` btrfs on lfs-a (2 vCPU, kernel 7.2.11); cairnd = real client core + FUSE passthrough + real server over TLS, data dir on the VM disk. 2 runs per workload (3 for the latency probes), medians, FIOSIZE=256M (rows are still labelled 1G). Raw: `E17-data/lfs-a-all.tsv`.

Note: this run found and drove the fix of a cairnd deadlock (applier blocked in `fuse_reverse_inval_entry` while holding the notifier mutex that handlers needed; fixed in cairn-fuse `fs.rs`). The numbers below are with the fix.

## Results (ratios vs native, time ratios, lower is better; for MiB/s and IOPS rows lower is worse)

| workload | unit | native | cairn | cairn-uring | cairn/native | cairn-uring/native |
|---|---|---:|---:|---:|---:|---:|
| wt_add_default | s | 5.004 | 21.754 | 80.559 | 4.35x | 16.10x |
| wt_remove_default | s | 0.298 | 1.340 | 1.090 | 4.49x | 3.66x |
| wt_add_w16 | s | 6.850 | 21.157 | 61.998 | 3.09x | 9.05x |
| wt_remove_w16 | s | 0.749 | 2.935 | 2.344 | 3.92x | 3.13x |
| status_warm | s | 0.111 | 0.192 | 0.094 | 1.73x | 0.84x |
| statwalk_warm | s | 0.030 | 0.159 | 0.100 | 5.38x | 3.39x |
| status_cold | s | 0.255 | 0.359 | 0.393 | 1.41x | 1.54x |
| statwalk_cold | s | 0.433 | 0.179 | 0.136 | 0.41x | 0.31x |
| small_create | s | 11.280 | 47.545 | 229.6 | 4.21x | 20.35x |
| small_delete | s | 0.246 | 0.905 | 0.525 | 3.68x | 2.14x |
| seqwrite_fsync_1G | MiB/s | 72.650 | 50.872 | 56.488 | 0.70x | 0.78x |
| seqwrite_nosync_1G | MiB/s | 7873.3 | 576.3 | 6827.9 | 0.07x | 0.87x |
| seqread_cold_1G | MiB/s | 137.7 | 106.9 | 115.0 | 0.78x | 0.84x |
| seqread_warm_1G | MiB/s | 8007.8 | 11151.5 | 10741.3 | 1.39x | 1.34x |
| randread4k_warm | IOPS | 633184.7 | 535877.2 | 516093.2 | 0.85x | 0.82x |
| randread4k_warm_lat_p50 | us | 1.064 | 1.216 | 1.232 | 1.14x | 1.16x |
| randread4k_cold | IOPS | 753.6 | 678.5 | 675.0 | 0.90x | 0.90x |
| randread4k_mmap_warm | IOPS | 2594.1 | 1680.3 | 1753.5 | 0.65x | 0.68x |
| cargo_build | s | 22.510 | 35.093 | 37.462 | 1.56x | 1.66x |
| lat_stat_median | us | 4.400 | 52.600 | 25.100 | 11.95x | 5.70x |
| lat_stat_p99 | us | 25.600 | 173.9 | 132.1 | 6.79x | 5.16x |
| lat_open_cold_median | us | 5.600 | 259.4 | 179.8 | 46.32x | 32.11x |
| lat_open_cold_p99 | us | 29.800 | 584.8 | 468.3 | 19.62x | 15.71x |
| lat_open_warm_median | us | 2.700 | 227.2 | 155.0 | 84.15x | 57.41x |
| lat_open_warm_p99 | us | 19.600 | 484.1 | 348.2 | 24.70x | 17.77x |

## Reading

- Namespace-heavy work is 3-5x native on classic, in line with E14's fuse-pt ratios (3.0x worktree add, 6.4x remove) but with far larger absolute times, because every create/rename also commits to the server and the native baseline here is fsync-bound btrfs, not the tmpfs backing used in E14 (E14 native worktree add 0.32 s vs 5.0 s here, so absolute numbers are not comparable).
- Per-op latency is the weak spot: cold stat 52.6 us and open 227-259 us (E14 fuse-pt classic: 26.9 us stat, 41-76 us open). Even warm open costs 227 us vs 2.7 us native, so every open goes through Core (lease/CAS/live-file work), not just FUSE.
- Reads and cached sequential I/O are near native (passthrough works): seq read 0.8-1.4x, 4k random 0.85x, cargo build 1.56x (E14: 1.1x).
- `--uring` is NOT a win here, unlike E14: stat 25 us (better) but file creates are much worse (small_create 230 s vs 48 s classic, worktree add 80 s vs 22 s) and cargo build slightly worse. Likely contention between the per-CPU ring threads and Core's blocking calls on a 2 vCPU guest (a handler blocked on a server round trip stalls its CPU's queue, E14 gotcha 1). Needs investigation before uring is offered for cairnd.
- `seqwrite_nosync` classic 0.07x native: buffered writes appear to be throttled by close/publish work; the uring run does not show it (0.87x), so treat this as noisy until rerun.
