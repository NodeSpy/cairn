# E16: cairnd (crates/cairn-fuse) fidelity vs E15

Subject: `cairnd` (FUSE layer over `Core`, vendored fuser 0.18 + io_uring), first with `MockCore` (loopback dir), then
the real client core (offline `Client::open`, SQLite state + CAS + live files). Hosts: lfs-srv (xfstests, docker,
overlay, pjd), lfs-a/lfs-b (real server + two clients, inotify). Kernel 7.2.15. Daemon runs as root, passthrough on.

## Results

| Suite | ff-v9 baseline (E15) | cairnd |
|---|---|---|
| pjdfstest (8798 asserts) | 1 known failure | all pass, mock and real Core, also with `--uring` |
| xfstests generic/quick (650) | 8 known FUSE failures | 9 failed (below), no regression |
| overlayfs (ovl.sh) | 29/29 | 29/29 on real Core |
| fidelity.py | n/a | 16/17 (O_TMPFILE unsupported) |
| docker pull/build/postgres, data-root + pgdata on a cairnd mount | passes | all steps pass except `hardlink-alias-unlink` |
| inotify, remote change on a second host | n/a | events arrive through the mount (`inotify-check.sh`, `e2e-inotify.sh`) |

xfstests failures (final run): 003, 683, 684, 685, 798 (killpriv / FUSE limitations, same as ff-v9), 103 (fallocate on
a full fs: no statfs/space accounting from Core), 294 and 306 (`mount -o remount,ro`: FUSE cannot honour it, kernel
sends nothing to the daemon), 732 (needs the same filesystem mounted at two mountpoints; one Core per root cannot).
Fixed during the run: 035 (removed dirs reachable by ino), 080/215/313/423 (ctime/mtime on writer inodes: attr TTL 0
while a writer is open, invalidate on release, Core ctime fix), 467 (unlinked-but-open files stay reachable by ino and
re-openable; "." lookup of a removed node is ESTALE unless the kernel still holds the inode), 524 (see below).

## Findings worth keeping

- fuser unmounts by path when its session ends. After an external `umount` followed by an immediate re-mount at the same
  path, the old daemon's drop tore down the NEW mount (generic/524, flaky mount timeouts). Vendored fuser now detaches
  without unmounting when the kernel ended the session, and cairnd only unmounts on SIGTERM/SIGINT.
- Passthrough + mmap: writes via the backing file bypass FUSE, so attrs of inodes with open writers get TTL 0 and are
  invalidated on release.
- Docker `hardlink-alias-unlink`: overlayfs revalidates a single upper dentry for all aliases (E15 section 7); ESTALE
  from `cat /y` after `rm /x`. Kernel-side, unchanged from ff-v9.
- Overlay upper warns "does not support tmpfile" (no O_TMPFILE in cairnd).

## Gaps

O_TMPFILE, ioctl/fiemap, killpriv, statfs from Core (static values today), flush/`sync_on_close` (no Core hook:
proposed `Core::sync_on_close(ino) -> bool` so the layer can set FOPEN_NOFLUSH otherwise), blocking Core calls stall a
uring queue (io_uring only helps with several queues; Core `lookup`/`open` can block on the server).

## Method

`crates/cairn-fuse/tests/`: `mount.fuse.xfstests` (xfstests helper: `mount -t fuse lfs-<name>` starts cairnd, state per
mountpoint persists across xfstests unmount/mount cycles), `ovl.sh`, `dockerbench.sh`, `fidelity.py`,
`inotify-check.sh`, `e2e-inotify.sh`. Raw logs on lfs-srv: `/cache/tools/res-cairnd{1..11}`, `docker-cairnd.log`.
