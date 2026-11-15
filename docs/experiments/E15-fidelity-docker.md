# E15: POSIX fidelity, overlayfs-on-FUSE, Docker on a FUSE data-root

Crate: `experiments/fuse-fidelity/` (fuser 0.18.0 upstream, derived from `fuse-pt`, which was not touched).
Test host: lfs-srv only (Arch, kernel 7.2.15; it was rebooted from 7.2.11 mid-run because a package upgrade removed
the 7.2.11 modules and `loop` could not load). Backing fs: btrfs `/cache`. Daemon runs as root.

## 1. Overlayfs kernel-source findings (v7.2.11, `fs/overlayfs`, `fs/fuse`)

- FUSE sets `d_revalidate`, so overlayfs classifies it as a "remote" upper (`ovl_dentry_remote`). For a remote upper,
  `super.c` demands d_type, RENAME_WHITEOUT and working xattrs, else "upper fs missing required features". The
  `fuse-pt` prototype (xattr ENOSYS) was refused for exactly this reason (verified: mount refused).
- `ovl_dentry_weird` is not triggered: FUSE has no `d_hash`/`d_compare`/`d_manage`; `d_automount` is only for submounts.
  `DCACHE_OP_REAL` only matters for overlay-on-overlay.
- Missing tmpfile support is only a warning ("upper fs does not support tmpfile"). File-handle support is optional.
- Stack depth: FUSE `s_stack_depth` = the daemon's `max_stack_depth`, overlay adds 1, limit is 2. Depth 2 fails
  with "maximum fs stacking depth exceeded" (verified). The daemon must set `max_stack_depth = 1` in `init`.
- Hardlink aliasing limitation (kernel side, section 7): `ovl_dentry_revalidate_common` revalidates only the ovl
  inode's single upper dentry, shared by all hardlink aliases.

## 2. What was implemented (vs `fuse-pt`)

- fd-based loopback: every inode is an `O_PATH|O_NOFOLLOW` fd; ops use `*at()`, `AT_EMPTY_PATH`, `/proc/self/fd`.
  Fixes hardlink and rename correctness. nodeid = backing st_ino (root = 1); cross-dev collision returns EIO.
  `RLIMIT_NOFILE` raised to the hard limit (EMFILE otherwise).
- Credentials: `CredGuard` for mknod/mkdir/symlink/create/fallocate/copy_file_range/ACL setxattr: per-thread
  `unshare(CLONE_FS)`, umask, `setfsuid/gid`, caps re-raised.
- `FUSE_POSIX_ACL` + `FUSE_DONT_MASK` (default ACLs need the daemon to see the unmasked mode).
- xattrs for all namespaces, mknod (incl. 0/0 whiteouts, high dev numbers), `renameat2` flags, linkat, fallocate
  modes, lseek DATA/HOLE, copy_file_range, fsyncdir, statfs, negative lookup entries.
- Stateless readdir via raw `getdents64` using the backing fs' d_off cookies, plus per-OPENDIR state that avoids
  `lseek` when the kernel continues where the last reply stopped (needed: btrfs `llseek` on a dir re-snapshots
  `last_index`, so seek-per-request makes rename-while-readdir loop forever, generic/736). A fresh handle and
  post-EOF requests always seek (rewinddir semantics, generic/471). OPENDIR is now on by default.
- `FUSE_EXPORT_SUPPORT` with remembered `file_handle`s (`open_by_handle_at`) for LOOKUP of forgotten inodes,
  "." and ".." (generic/426, 467, 756).
- Pre-1970 timestamps handled around fuser's asymmetric encode/decode. `release()` of a writable passthrough
  handle invalidates inode attrs (shared-mmap mtime/ctime with TTL 3600; generic/080, 215).
- Mount options: `--fsname`, `--mntopts atime,nosuid,nodev,noexec,ro`; noatime default. killpriv stays off.

## 3. pjdfstest

| Build | Result |
|---|---|
| fuse-pt-equivalent original (`ff-orig`) | 87 failed subtests, ~20 files (chmod/03,12; chown/03; link/00,03; mkdir/03; mknod/03; open/02,03; rename/02,21,24; rmdir/03; symlink/03; unlink/00,03,14; (f)truncate/03; mkfifo/03) of ~8798 |
| fuse-fidelity (ff-v7) | only rename/24.t (4 subtests): btrfs directories report nlink 1 |
| native btrfs | all pass (8798 tests; rename/24 is marked TODO for btrfs in pjdfstest) |

pjdfstest was not re-run after the final readdir-state change (xfstests generic/011, 006, 013 and the fidelity
readdir test were).

## 4. xfstests `generic/quick` (650 tests, FSTYP=fuse vs btrfs on loop devices)

Not run on FUSE: ~460 (reflink/FICLONE, fiemap, block-device-only, quota, encryption, shutdown, chattr, ...).

| Run | Failures |
|---|---|
| native btrfs baseline | 501, 748 (748 stalled for 18+ min on the loop-backed btrfs and was killed) |
| FUSE run 1 (original fixes missing) | 27: 003 006 011 013 020 080 099 128 215 294 306 319 375 426 444 467 471 477 676 683 684 685 697 736 756 777 798 |
| FUSE run 2 | 14: 003 020 375 426 467 477 676 683 684 685 736 756 777 798 |
| FUSE final (ff-v9) | 003 020 477 683 684 685 777 798 (471 and 736 fixed after the full run, verified individually; 766 was a transient mount timeout and is "not run" on rerun) |

Remaining FUSE-only failures (all pass on btrfs), with cause:
- 003: atime is not propagated (backing is noatime; kernel-side atime only).
- 020: backing btrfs xattr size limit (~16 KB) vs 64 KB expected for FSTYP=fuse.
- 477, 777: cycle-mount, needs persistent file handles across daemon restart (handle map is in memory).
- 683, 684, 685: suid/sgid clearing on fallocate/punch/zero by non-root (needs killpriv: `FATTR_KILL_SUIDGID`,
  not exposed by fuser 0.18).
- 798: cachestat reports 0 for passthrough files.

## 5. Overlayfs (upper and/or lower on the FUSE mount)

`tests/ovl.sh`: 29 checks (copy-up incl. xattr/times/owner, whiteouts, opaque dirs, redirect after dir rename,
hardlinks, xattrs, mknod, mmap, exec, rename-over, NOREPLACE, umount/remount, dmesg). All 29 PASS in four
configs: all on lfs; lower on btrfs + upper on lfs; `redirect_dir,index,metacopy,xino=on`;
`index,nfs_export,redirect_dir`. Only dmesg noise: "upper fs does not support tmpfile" (plus xino/metacopy notices).
The original prototype was refused.

## 6. Docker (overlay2, `data-root` on `/mnt/lfs/docker`)

`tests/dockerbench.sh`, containerd snapshotter disabled (classic graphdriver). `docker info`: Storage Driver
overlay2, Backing Filesystem `fuse`, d_type true. fuse-overlayfs was not needed. Sequential runs on an idle VM, 2 vCPU:

| Step | FUSE (s) | btrfs data-root (s) |
|---|---|---|
| pull alpine | 4.5 | 3.0 |
| pull debian:bookworm-slim | 11.4 | 7.1 |
| pull postgres:17 | 37.6 | 36.5 |
| multi-stage build (gcc) | 42.0 | 38.8 |
| run alpine / debian tools | 2.9 / 2.5 | 5.1 / 4.0 |
| postgres init / ready | 1.8 / 31.0 | 2.6 / 17.9 |
| 100k-row insert | 5.3 | 4.8 |
| restart + ready | 4.1 + 1.4 | 3.9 + 1.4 |
| update | 2.9 | 2.3 |
| `system prune -af --volumes` | 8.8 | 9.0 |

Postgres data volume on the FUSE mount: count and sum verified before and after restart. Pulls, build, run all pass.
One check FAILS on FUSE and passes on btrfs: hardlink-alias-unlink (ESTALE, section 7). The pg-ready 31 s outlier
(first start, initdb on FUSE) was not repeated.

## 7. Remaining gaps and suggested fixes

1. Hardlink alias ESTALE under overlay-on-FUSE: in an overlay with upper on FUSE, `ln a b; ls -li; rm a;` then
   access of `b` (stat/unlink) returns ESTALE (also seen inside Docker: `mv /a /c`, `rm -rf /c`). Cause: overlay
   revalidates the ovl inode's one upper dentry, which FUSE zeroed when the other alias was unlinked. Not fixed by
   index/nfs_export, not fixable in the daemon. Fix: kernel patch to revalidate the dentry's own upper alias, or
   distinct nodeids per hardlink alias.
2. O_TMPFILE: fuser 0.18 has no `FUSE_TMPFILE` (about a 40-line patch to fuser).
3. killpriv / `FATTR_KILL_SUIDGID` / `SETXATTR_EXT` / `ACL_KILL_SGID`: need fuser support (683-685).
4. No ioctl forwarding (FICLONE, FS_IOC_GETFLAGS/chattr) or fiemap, so reflink-class tests do not run.
5. Handle map for forgotten inodes is non-persistent and unbounded (477/777). The real leasefs design should use
   handle-based inodes with an LRU, not one fd per cached inode.
6. Directory nlink is 1 on btrfs (rename/24). Cross-device st_ino collision returns EIO.
7. cachestat reports 0 under passthrough; atime not propagated; locks are local only.
8. btrfs xattr size limit (generic/020) is a backing-fs property.

## 8. E12 tuning notes

- OPENDIR: default now on. With ENOSYS (no OPENDIR) the daemon cannot keep a stable directory fd, so btrfs gets a
  fresh snapshot per request and rename-while-readdir fails (736). Keep OPENDIR enabled; `--tune dircache` adds
  `FOPEN_CACHE_DIR` if wanted.
- Negative-entry and attribute TTL 3600: fine for changes made through the mount; backing-side changes need
  `notify_inval_entry`/`inval_inode`. Attribute TTL breaks shared-mmap mtime, mitigated by invalidation in `release`.
- killpriv must stay off with fuser 0.18; noatime is the default.
- `max_stack_depth` must be 1 for overlay on top.

## 9. Files

- `experiments/fuse-fidelity/{Cargo.toml,src/main.rs,src/fs.rs}`, `tests/{ovl.sh,fidelity.py,dockerbench.sh}`.
- On lfs-srv: `/cache/tools/` (binaries `ff-orig`, `ff-v2`..`ff-v9`; logs `pjd-*.txt`, `xfs-{fuse1,fuse2,fuse3,btrfs}.log`,
  `docker-{fuse,btrfs}.log`). `/usr/local/bin/fuse-fidelity` is ff-v9. Cleaned up: all mounts, dockerd, loop devices,
  `/etc/docker/daemon.json`.
