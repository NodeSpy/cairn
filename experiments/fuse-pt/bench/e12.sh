#!/bin/bash
# E12 benchmark driver: metadata workloads, native vs FUSE, with per-workload request mix.
# usage: e12.sh <native|fuse> <label> [runs]      (label tags the output; run as a user with sudo)
# env: BASE   dir on the backing fs (default /mnt/ram, tmpfs); native/ and backing/ live under it
#      MNT    mountpoint (default /mnt/lfs)       BIN  daemon binary (default /tmp/fuse-pt)
#      FLAGS  daemon flags (default --passthrough --stats-file $OUT/stats.tsv)
#      RS     bare repo (default /tmp/rs.git)     LAT  latency tool (default /tmp/lat)
#      BUILD  vendored cargo project (default /tmp/buildsrc; set SKIP_CARGO=1 to skip)
#      REPO_TAR  tar of a ready `repo/` clone (use where `git clone` of $RS is not possible)
#      OUT    output dir (default /tmp/e12-out); results: $OUT/results.tsv, request mixes: $OUT/mix-<label>-<workload>-<run>.tsv
#      DROP   how to drop caches (default: sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'); DROP2 for the dentry-only drop; SYNC=true skips sync
set -u
mode=$1; label=$2; N=${3:-5}
BASE=${BASE:-/mnt/ram}; MNT=${MNT:-/mnt/lfs}; BIN=${BIN:-/tmp/fuse-pt}; OUT=${OUT:-/tmp/e12-out}
RS=${RS:-/tmp/rs.git}; LAT=${LAT:-/tmp/lat}; BUILD=${BUILD:-/tmp/buildsrc}
SUDO=${SUDO-sudo}; ME=${ME:-$(id -un)}; SYNC=${SYNC:-sync}   # SYNC=true and DROP=true to leave host caches alone
mkdir -p $OUT
FLAGS=${FLAGS---passthrough}
STATS=$OUT/stats-$label.tsv
if [ $mode = native ]; then D=$BASE/native; else D=$MNT; fi
rec() { echo -e "$label\t$1\t$2\t$3\t${4:-s}" >> $OUT/results.tsv; }
now() { date +%s.%N; }
mix() { # save the request mix of the workload that just ran
  [ $mode = native ] && return
  $SUDO rm -f $STATS; $SUDO kill -USR1 $(pgrep -x fuse-pt) 2>/dev/null; for i in $(seq 1 100); do [ -s $STATS ] && break; sleep 0.02; done
  cp $STATS $OUT/mix-$label-$1-$2.tsv 2>/dev/null
}
mixreset() { [ $mode = native ] || { $SUDO kill -USR2 $(pgrep -x fuse-pt); sleep 0.05; }; }
timeit() { # timeit <workload> <run> <cmd...>
  local w=$1 r=$2; shift 2; local t0 t1; mixreset; t0=$(now); "$@" > /dev/null 2>&1; local rc=$?; t1=$(now)
  mix $w $r
  [ $rc -ne 0 ] && echo "WARN $w run $r rc=$rc" >&2
  rec $w $r $(echo "$t1 - $t0" | bc -l)
}
mount_fuse() {
  $SUDO sh -c "$BIN --backing $BASE/backing --mount $MNT $FLAGS --stats-file $STATS > $OUT/daemon-$label.log 2>&1 &"
  for i in $(seq 1 50); do mountpoint -q $MNT && return; sleep 0.1; done; echo "mount failed" >&2; exit 1
}
umount_fuse() { $SUDO umount $MNT 2>/dev/null; for i in $(seq 1 100); do pgrep -x fuse-pt >/dev/null || return 0; sleep 0.1; done; echo 'umount failed' >&2; exit 1; }
drop() { $SYNC; ${DROP:-$SUDO sh -c 'echo 3 > /proc/sys/vm/drop_caches'}; }
cold() { if [ $mode != native ]; then umount_fuse; drop; mount_fuse; else drop; fi; }
cleanup() { [ $mode != native ] && umount_fuse; $SUDO rm -rf $BASE/native/* $BASE/backing/*; $SYNC; }
trap 'cleanup' EXIT
mkdir -p $BASE/native $BASE/backing; $SUDO chown $ME: $BASE/native $BASE/backing
cleanup
[ $mode != native ] && mount_fuse
$SUDO chown $ME: $D 2>/dev/null
cd /tmp
export GIT_CONFIG_GLOBAL=$OUT/gitconfig; printf '[user]\n name=b\n email=b@b\n[safe]\n directory=*\n' > $OUT/gitconfig
echo "== $label $mode: setup" >&2
if [ -n "${REPO_TAR:-}" ]; then tar -xf $REPO_TAR -C $D; else git clone -q --no-hardlinks $RS $D/repo; fi
git -C $D/repo checkout -q -B bench

for r in $(seq 1 $N); do
  $SYNC
  timeit wt_add_w16 $r git -C $D/repo -c checkout.workers=16 worktree add --detach $D/wt_w16
  timeit wt_remove $r git -C $D/repo worktree remove --force $D/wt_w16
done
git -C $D/repo worktree add -q --detach $D/wt_s
# refresh the index twice, 2 s apart, so no entry is "racily clean" (git would re-read those files on every status)
git -C $D/wt_s status > /dev/null; sleep 2; git -C $D/wt_s status > /dev/null
for r in $(seq 1 $N); do timeit status_warm $r git -C $D/wt_s status; done
for r in $(seq 1 $N); do timeit statwalk_warm $r sh -c "cd $D/wt_s && find . -type f | xargs stat -c '%s %Y %i'"; done
for r in $(seq 1 $N); do
  cold; timeit status_cold $r git -C $D/wt_s status
  cold; timeit statwalk_cold $r sh -c "cd $D/wt_s && find . -type f | xargs stat -c '%s %Y %i'"
done
# read every file of the worktree (open+read+close of ~6k small files), cold cache
for r in $(seq 1 $N); do
  cold; timeit readall_cold $r sh -c "cd $D/wt_s && find . -type f -not -path './.git' | xargs cat"
done

payload=$(head -c 4096 /dev/zero | tr '\0' x)
mk() { mkdir -p $D/small; cd $D/small; for d in $(seq 1 100); do mkdir d$d; for f in $(seq 1 100); do echo "$payload" > d$d/f$f; done; done; }
for r in $(seq 1 $N); do
  timeit small_create $r bash -c "$(declare -f mk); D=$D; payload='$payload'; mk"
  $SYNC
  timeit small_delete $r rm -rf $D/small
done

if [ -z "${SKIP_CARGO:-}" ]; then
  for r in $(seq 1 3); do
    rm -rf $D/buildsrc; cp -r $BUILD $D/buildsrc; $SYNC
    timeit cargo_build $r sh -c "cd $D/buildsrc && cargo build --offline"
  done
fi

(cd $D/wt_s; find . -type f | sed "s|^\.|$D/wt_s|" | sort > $OUT/files.list)
for phase in stat open_cold open_warm; do
  for r in 1 2 3; do
    if [ $mode != native ]; then umount_fuse; ${DROP2:-$SUDO sh -c 'echo 2 > /proc/sys/vm/drop_caches'}; mount_fuse; else ${DROP2:-$SUDO sh -c 'echo 2 > /proc/sys/vm/drop_caches'}; fi
    mixreset
    line=$($LAT $phase $OUT/files.list 3000)
    mix lat_$phase $r
    echo "$line" >&2
    rec lat_${phase}_median $r $(echo $line | sed 's/.*median_us=\([0-9.]*\).*/\1/') us
    rec lat_${phase}_p99 $r $(echo $line | sed 's/.*p99_us=\([0-9.]*\).*/\1/') us
  done
done
echo "== $label done" >&2
