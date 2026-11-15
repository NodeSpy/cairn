# E13 - FUSE over io_uring: per-request latency for the client layer

Date 2026-11-13. Code: `experiments/fuse-uring/` (`src/main.rs` server with both transports, `src/bin/lat.rs` latency client, `bench.sh`). Static musl binaries, `CARGO_TARGET_DIR=/home/daniel/.cache/leasefs-target/exp-uring`.
Test machine: `lfs-b` (2 vCPU i7-8700T guest, kernel 7.2.11, `CONFIG_FUSE_IO_URING=y`, `CONFIG_FUSE_PASSTHROUGH=y`). Only `lfs-b` was touched; the only system change is `echo 1 > /sys/module/fuse/parameters/enable_uring` (not persisted, see Requirements).

## Recommendation

**io_uring is worth building, with a clear caveat about where the win comes from.**

- Against a normal multi-threaded `/dev/fuse` daemon (the shape fuse-pt has), io_uring cuts per-op latency by 57-68% for one client, 20-60% for two, 34-44% for eight, and raises aggregate throughput 25-70%. It is about 6.5 us per round trip, against about 20 us for classic. That is the "25 us per daemon round trip" of E11, so most of it goes away.
- Almost all of the saving is **CPU locality and avoided cross-CPU wakeups**, not syscall count. A classic daemon with a single thread pinned to the same CPU as a single pinned client gets 15.2 us per `stat` against io_uring's 13.2 us (13%). io_uring gets that locality for free because the kernel queues each request on the ring of the CPU that issued it and completes it through task work in the server thread pinned there. A classic daemon cannot, because requests are picked up by whichever thread wins the `read()`.
- These are VM numbers (2 vCPU). Cross-CPU wakeups are probably dearer in a guest than on bare metal, so the gap on real client hardware may be smaller. Re-measure on a target machine before committing to the full integration.
- The server in this experiment does trivial work (in-memory hash map). A real handler that takes a remote round trip adds the same time to both transports, so the saving is the same absolute microseconds but a smaller fraction.
- libfuse's own `passthrough_hp` agrees: `-o io_uring` takes `stat` from 85 to 27 us (-68%), open+close from 126 to 48 (-62%), create+unlink from 297 to 131 (-56%).

Integration plan is at the end. Short version: patch/fork fuser to add an io_uring session loop that reuses its request decoder, keep the `/dev/fuse` fd for init, forget, notifications and the backing-id ioctls, and keep classic as a runtime fallback.

## Protocol (kernel 7.2.11, `fs/fuse/dev_uring.c`, `include/uapi/linux/fuse.h`; libfuse 3.18.3 `lib/fuse_uring.c`)

### Setup order

1. Mount as usual, open `/dev/fuse`, and do **FUSE_INIT over plain read/write on that fd**. The kernel advertises `FUSE_OVER_IO_URING` (bit 41, i.e. `flags2` bit 9, needs `FUSE_INIT_EXT`) in the init request only if `fuse.enable_uring=1` at that moment.
2. The daemon echoes `FUSE_OVER_IO_URING` in its init reply. Only then does the kernel call `fuse_uring_create()` and set `fch->io_uring`. Ring dimensions come from the negotiated values: `nr_queues = num_possible_cpus()`, `max_payload_sz = max(8192, max_write, max_pages * PAGE_SIZE)`.
3. After the reply the daemon creates one `io_uring` per queue with `IORING_SETUP_SQE128` and submits `IORING_OP_URING_CMD` SQEs with `cmd_op = FUSE_IO_URING_CMD_REGISTER` (1), one per ring entry. Until the connection is marked initialised the kernel returns `-EAGAIN` (retry; libfuse does the same).
4. The ring becomes "ready" only when **every** queue (every possible CPU) has at least one registered entry. Until then new requests block in `fuse_block_alloc()`. So a missing queue means a hung mount, not a fallback.
5. If any REGISTER fails, the kernel sets `fch->io_uring = 0` and wakes the waiters, and the connection carries on over classic `/dev/fuse`. If `enable_uring` is off at init, the flag is simply not offered, and a later REGISTER gets `-EOPNOTSUPP`.

### Entries, buffers and commands

- Queue id = CPU number. The kernel picks the queue of `task_cpu(current)` of the **submitting** task (`fuse_uring_task_to_queue`). There is no stealing between queues: if that CPU's queue has no free entry the request waits on that queue's list.
- Each ring entry is a pair of buffers registered by REGISTER via `sqe->addr` = pointer to `struct iovec[2]`, `sqe->len = 2`:
  - iov[0], headers, at least `sizeof(struct fuse_uring_req_header)` = 288 bytes: `in_out[128]` (a `fuse_in_header` on delivery, a `fuse_out_header` on commit), `op_in[128]` (the per-opcode fixed struct, e.g. `fuse_create_in`), and `ring_ent_in_out` (`flags u64`, `commit_id u64`, `payload_sz u32`, pad, reserved).
  - iov[1], payload, at least `max_payload_sz` (>= 128 KiB with default negotiation; I used 256 KiB). Holds the trailing request args (names, write data) on delivery and **all** reply args (`fuse_entry_out`, `fuse_attr_out`, `fuse_open_out`, read data) on commit.
- The 80-byte SQE command area (`sqe128`, the `cmd` bytes) is `struct fuse_uring_cmd_req { u64 flags; u64 commit_id; u16 qid; u8 pad[6]; }`.
- Flow: REGISTER stays pending. When a request is assigned to the entry, the kernel copies header, op header and payload into the user buffers (in the ring's task context) and completes the CQE with `res = 0` (CQE `user_data` identifies the entry). The daemon reads `in_out`, `op_in`, `payload[..payload_sz]`, fills `in_out` with a `fuse_out_header` (error, unique), writes reply args into the payload and `payload_sz` into `ring_ent_in_out`, then submits `COMMIT_AND_FETCH` (2) with `commit_id` = the `commit_id` field it received (equals the request's `unique`). That one SQE both replies and re-arms the entry. Its CQE arrives with the next request.
- `LOOKUP` and `UNLINK` have a zero-size `op_in` and the name is in the payload. `out.error != 0` means no payload is read.
- One `io_uring_enter` per request in the steady state (`submit_and_wait(1)` after pushing the commit).
- Teardown: on unmount or abort pending entries complete with `-ENOTCONN` / `-ECANCELED`; the loop just exits.

### Things that still use `/dev/fuse`

- **FORGET / BATCH_FORGET and INTERRUPT**: `fuse_io_uring_ops.send_forget = fuse_dev_queue_forget`, `send_interrupt = fuse_dev_queue_interrupt`. Measured: about 22k FORGETs per 22k create/unlink iterations arrived on the classic reader while the ring served everything else. The daemon must keep a classic read loop running (libfuse does too).
- **Notifications** (`unique == 0`) are rejected on the ring (`-EINVAL`); send inval/delete notifications with `write(2)` to the fd (fuser's `Notifier` already does). Not tested here.
- The FUSE_INIT exchange and `FUSE_DEV_IOC_*` ioctls.

### Passthrough coexistence

Independent of the ring. `FUSE_DEV_IOC_BACKING_OPEN` / `_CLOSE` are ioctls on the `/dev/fuse` fd (CAP_SYS_ADMIN), and the id goes back in `fuse_open_out.backing_id` with `FOPEN_PASSTHROUGH`, which in the ring case is just bytes in the payload. `FUSE_PASSTHROUGH` and `FUSE_OVER_IO_URING` are separate init flags and can both be set. Verified: with `--passthrough` on both transports a 64 MiB `dd` write/read ran at 3.0 / 9.5 GB/s over io_uring (2.6 / 9.4 classic), the daemon saw no READ/WRITE (it returns ENOSYS for them), and the backing ioctl ran fine from a ring thread inside a handler.

### Requirements

- `fuse.enable_uring=1` (bool, 0644) **before the mount's INIT**; an existing connection is not upgraded. `fuse` is built in (`CONFIG_FUSE_FS=y`) on lfs-b, so persistence is the kernel command line (`fuse.enable_uring=1`), not modprobe.d. I did not persist it (throwaway VM, sysfs write only; lost on reboot).
- `CONFIG_FUSE_IO_URING=y`, `kernel.io_uring_disabled=0` (it is 0 on lfs-b).
- No capability check in `dev_uring.c` itself, so an unprivileged daemon with a `fusermount3` fd should work; I only tested as root. Backing ids need CAP_SYS_ADMIN as before.
- SQE128 rings (`IORING_SETUP_SQE128`), and a thread per queue, pinned to that queue's CPU if you want the locality win.

## Latency results

Method: `lat` loops one syscall sequence on a pre-created flat file, 100k iterations (30k for create+unlink) after 2k warm-up, per-iteration `Instant` timing. Median over 3 runs of the per-run mean (p99 of the same runs in brackets). Server: in-memory flat tree, `entry_valid = attr_valid = 0` so every operation reaches the daemon, FLUSH answered `ENOSYS` (kernel stops sending it). Request mix per iteration: `stat` = LOOKUP + GETATTR (2 trips); open+close = LOOKUP + OPEN + RELEASE (RELEASE is async); create+unlink = about 5 requests incl. a negative LOOKUP.

Configurations: **classic** = 2 reader threads on the shared fd, unpinned; **classic-pin** = the same, thread i pinned to CPU i; **classic-1t** = one reader thread pinned to CPU 0 (best case for classic); **uring** = one ring per CPU, thread pinned, depth 4 (d1 and d16 vary the depth); **hp-classic / hp-uring** = libfuse 3.18.3 `passthrough_hp --nocache --nopassthrough` without and with `-o io_uring` (default libfuse threads and depth; it does real syscalls on a tmpfs backing dir, so its absolute numbers are higher than ours).


**1 client, unpinned**, mean us (p99 us)

| transport | stat | open+close | create+unlink |
|---|---|---|---|
| classic | 39.7 (85) | 49.9 (110) | 105.6 (201) |
| classic-pin | 32.3 (70) | 31.3 (75) | 74.0 (154) |
| classic-1t | 43.4 (99) | 48.4 (104) | 94.7 (208) |
| uring | 13.8 (35) | 17.1 (43) | 45.8 (112) |
| uring-d1 | 13.6 (36) | 17.9 (44) | 44.4 (100) |
| uring-d16 | 13.5 (38) | 17.0 (45) | 41.4 (88) |
| hp-classic | 84.9 (173) | 125.6 (246) | 297.0 (525) |
| hp-uring | 27.3 (93) | 48.3 (156) | 131.4 (233) |

**1 client, pinned to CPU 0**, mean us (p99 us)

| transport | stat | open+close | create+unlink |
|---|---|---|---|
| classic | 44.9 (97) | 50.6 (98) | 99.7 (198) |
| classic-pin | 30.8 (68) | 27.8 (66) | 68.4 (144) |
| classic-1t | 15.2 (43) | 20.2 (55) | 46.6 (115) |
| uring | 13.2 (26) | 16.8 (38) | 43.8 (93) |
| uring-d1 | 13.0 (26) | 18.4 (49) | 42.9 (86) |
| uring-d16 | 13.2 (30) | 16.8 (40) | 41.2 (82) |
| hp-classic | 79.5 (172) | 123.8 (245) | 298.7 (607) |
| hp-uring | 25.1 (58) | 41.8 (92) | 129.4 (219) |

**2 clients, one pinned per CPU (per-client latency; aggregate ops/s in brackets)**, mean us (p99 us)

| transport | stat | open+close | create+unlink |
|---|---|---|---|
| classic | 18.4 (54) [108k/s] | 24.1 (61) [83k/s] | 159.3 (1656) [11k/s] |
| classic-pin | 18.2 (62) [108k/s] | 21.7 (62) [92k/s] | 126.3 (981) [15k/s] |
| classic-1t | 32.7 (77) [49k/s] | 40.6 (97) [42k/s] | 132.9 (1788) [13k/s] |
| uring | 14.7 (42) [135k/s] | 17.9 (43) [111k/s] | 63.2 (270) [21k/s] |
| uring-d1 | 14.5 (41) [136k/s] | 18.9 (48) [104k/s] | 54.9 (164) [22k/s] |
| uring-d16 | 14.1 (38) [140k/s] | 17.7 (45) [112k/s] | 54.5 (154) [22k/s] |
| hp-classic | 89.5 (190) [22k/s] | 147.8 (289) [13k/s] | 426.9 (816) [5k/s] |
| hp-uring | 27.3 (63) [73k/s] | 46.7 (104) [43k/s] | 253.8 (495) [8k/s] |

**8 clients, unpinned on 2 vCPUs**, mean us (p99 us)

| transport | stat | open+close | create+unlink |
|---|---|---|---|
| classic | 80.5 (262) [99k/s] | 99.2 (475) [80k/s] | 700.9 (5112) [11k/s] |
| classic-1t (thread not pinned) | 90.3 (203) [88k/s] | 110.1 (246) [72k/s] | 679.7 (9958) [11k/s] |
| uring | 44.8 (120) [168k/s] | 65.2 (163) [117k/s] | 402.6 (5043) [20k/s] |

### Reading the tables

- Versus a normal threaded classic daemon (**classic**), io_uring changes mean latency by: 1 client unpinned -65% / -66% / -57% (stat / open+close / create+unlink); 2 clients -20% / -26% / -60% with +25% / +34% / +90% aggregate throughput; 8 clients -44% / -34% / -43% with +70% / +46% / +80% throughput. The ≥30% bar is met everywhere except the 2-client stat and open+close rows.
- Versus the **best classic case** (one thread on the client's CPU, one pinned client): -13% / -17% / -6%. That is the floor of what io_uring buys once locality is equal: per-request copy and syscall cost is similar. It is not a configuration a real multi-process client can reach.
- Queue depth does not matter for a synchronous handler (d1, d4, d16 within noise); it would for handlers that block.
- 2-client create+unlink is limited by the root directory's inode lock and my global `Mutex`, which is why classic shows p99 in the milliseconds; the 8-client run is similarly lock-bound (about 20k ops/s on uring, 11k on classic).
- libfuse's classic loop is much slower than our minimal one (it does real backing-dir syscalls and allocates per request); its uring mode cuts 56-68% at one client and 41-70% at two (stat 89 -> 27 us, open+close 148 -> 47 us, create+unlink 427 -> 254 us).
- Passthrough mode (read4k = open + 4 KiB pread + close): 54 us classic against 26 us uring, i.e. the same metadata gap; data is not in the daemon path in either.

## What did not work / caveats

- First builds failed on my own layout bugs (`flags2` offset in `fuse_init_in`, `fuse_init_out` field offsets, `fuse_attr` offsets). With the ring flag not echoed correctly the kernel prints `fuse-io-uring not enabled on this connection` and REGISTER returns `-EOPNOTSUPP` (-95); with a bad `fuse_attr` mode the first CREATE fails with EIO. Neither is a kernel limitation.
- `io-uring` crate 0.7.16 `UringCmd80` has no setter for `sqe->len`; I poke the SQE bytes at offset 24 (`len = 2`). A real implementation should use raw SQE access or a newer crate.
- libfuse on lfs-b: the VM has `fuse3` and `liburing` but no meson and no `passthrough_hp`, so I built libfuse 3.18.3 (`passthrough_hp`, from the release tarball) on the devbox in a throwaway venv and copied the binary and `libfuse3.so` to `/tmp` on lfs-b. Without `--nocache` `passthrough_hp` caches entries for 1 s and `stat` reads 1.3 us, which is not a transport measurement. With `-o io_uring` the process holds io_uring fds (checked via `/proc/<pid>/fd`); classic mode holds none.
- libfuse master's `lib/fuse_uring.c` has a bufpool (zero-copy buffer registration) the 7.2.11 uapi does not have (`fuse_uring_cmd_req` here is only flags/commit_id/qid); I read it for the design but used the 7.2.11 uapi and the released 3.18.3 as the reference.
- Not measured: SQPOLL / `DEFER_TASKRUN` / `COOP_TASKRUN` (libfuse reports they are slower), multiple threads per queue, large-payload throughput through the ring (non-passthrough 128 KiB reads and writes), interrupts and notifications over the ring, the unprivileged mount path, any handler that blocks.
- Numbers come from a 2 vCPU guest with 3 repetitions per cell; run-to-run spread was a few percent for means, more for p99 and for all create+unlink.
- Not persisted on lfs-b. Files left on lfs-b: `/tmp/{fuse-uring,lat,bench.sh,passthrough_hp,libfuse3.so*,bench-*.out}`, `/mnt/t`, `/dev/shm/bk` (empty).

## Integration plan (not implemented; fuse-pt untouched)

fuse-pt implements about 36 `Filesystem` methods (lookup, forget, getattr, setattr, readlink, mknod, mkdir, unlink, rmdir, symlink, rename, link, open, create, read, write, flush, release, fsync, fallocate, lseek, opendir, readdir, readdirplus, releasedir, statfs, xattr x4, fsyncdir, init) and uses `fuser` 0.18 with `n_threads = 4`. fuser's request decoder (`ll::AnyRequest`) and reply types are `pub(crate)`, so reusing them needs a patched or vendored fuser rather than an external transport. Two options:

1. **Fork fuser and add a ring transport (recommended).** About 500-700 lines, all in the transport layer:
   - `RingSession`: after INIT, N threads (N = possible CPUs) each pinned to its CPU, each with an SQE128 ring and `depth` entries (header 512 B, payload >= `max(max_write, max_pages*4096)`, 128-256 KiB each; with 8 entries x CPUs this is a few MiB). Everything above (`dispatch_req`, decoders, the `Reply*` types) stays.
   - Request path: the decoder wants one contiguous slice (`in_header` + op header + args). Rebuild it per request from `in_out`, `op_in` (length from the opcode) and `payload[..payload_sz]`: one small memcpy, no allocation (scratch buffer per entry); WRITE data is copied the same way if passthrough is off.
   - Reply path: fuser's `ReplySender::send(&[IoSlice])` becomes "split first 16 bytes into `in_out`, copy the rest into the payload, set `payload_sz`, push COMMIT_AND_FETCH". For handlers that reply inline (all of fuse-pt today) the ring is thread-local and this is lock-free. If handlers may block or reply from other threads (a remote round trip), the reply must reach the owning ring: either a mutex-guarded SQ per ring or hand the commit back to the ring thread through an eventfd; blocking handlers also need a worker pool, since one slow handler stalls its CPU's queue (no stealing between queues).
   - Keep a classic reader thread running for FORGET / INTERRUPT, and keep `Notifier` on `write(2)`.
   - Fallback: if the init reply lacks `FUSE_OVER_IO_URING` or REGISTER fails, run the stock fuser loop. Gate behind a config flag; default on when `enable_uring` is set.
   - Passthrough: no change. The backing ioctls and `BackingId` handling in `fs.rs` already go through the `/dev/fuse` fd; with the ring they keep working (verified here, from inside a ring handler).
2. **Own minimal protocol layer** (what `experiments/fuse-uring` is). About 350 lines for the 6 opcodes here; for fuse-pt's 36 opcodes and all reply structs (readdir packing, xattr size probing, `setattr` valid bits, `fuse_in_header` ext fields) it is closer to 1500-2000 lines and re-implements what fuser already tests. Only worth it if forking fuser is unacceptable.

Surface that matters: opcodes are the same in both transports (nothing opcode-specific changes except that reply args live in the payload and `LOOKUP`/`UNLINK`-style names come from the payload); the integration risk is in threading (pinning, blocking handlers) and in the "all queues must register or the mount hangs" rule, not in protocol coverage.

Before integrating: (a) repeat the stat / open+close / create+unlink loops on real client hardware with the real fuse-pt daemon behind both transports, since the 6.5-vs-20 us per trip figure is from a 2 vCPU guest and the gain is mostly wakeup cost; (b) persist `fuse.enable_uring=1` on the client machines' kernel command line (and check the distro kernels have `CONFIG_FUSE_IO_URING`, 6.14+).
