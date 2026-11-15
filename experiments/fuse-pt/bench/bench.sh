#!/bin/bash
# E11 benchmark driver. Runs on a test VM as daniel (uses sudo for mount / drop_caches).
# usage: bench.sh <native|nopt|pt> <vm-label> [runs]
# Inputs in /tmp: fuse-pt, lat, rs.git (bare, 6164 files), buildsrc/ (vendored cargo project).
# Output: TSV lines "vm config workload run value unit" appended to /tmp/results.tsv
set -u
cfg=$1; vm=$2; N=${3:-5}
OUT=${OUT:-/tmp/results.tsv}
BASE=${BASE:-/cache}   # filesystem holding native/ and backing/ (BASE=/mnt/ram for the tmpfs variant)
FIOSIZE=${FIOSIZE:-1G}
case $cfg in
  native) D=$BASE/native ;;
  nopt)   D=/mnt/lfs; DFLAGS="--writeback" ;;
  pt)     D=/mnt/lfs; DFLAGS="--passthrough" ;;
esac
mkdir -p $BASE/native $BASE/backing; sudo chown daniel: $BASE/native $BASE/backing
rec() { echo -e "$vm\t$cfg\t$1\t$2\t$3\t${4:-s}" >> $OUT; }
now() { date +%s.%N; }
timeit() { # timeit <workload> <run> <cmd...>
  local w=$1 r=$2; shift 2; local t0 t1; t0=$(now); "$@" > /dev/null 2>&1; local rc=$?; t1=$(now)
  [ $rc -ne 0 ] && echo "WARN $w run $r rc=$rc" >&2
  rec $w $r $(echo "$t1 - $t0" | bc -l)
}
mount_fuse() {
  sudo sh -c "/tmp/fuse-pt --backing $BASE/backing --mount /mnt/lfs $DFLAGS > /tmp/fuse-$cfg.log 2>&1 &"
  for i in $(seq 1 50); do mountpoint -q /mnt/lfs && return; sleep 0.1; done; echo "mount failed" >&2; exit 1
}
umount_fuse() { sudo umount /mnt/lfs 2>/dev/null; for i in $(seq 1 50); do pgrep -x fuse-pt >/dev/null && mountpoint -q /mnt/lfs || { pgrep -x fuse-pt >/dev/null || return 0; }; sleep 0.1; done; echo 'umount failed' >&2; exit 1; }
drop() { sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null; }
cold() { # drop caches; for FUSE also restart the daemon (fresh inode table, kernel dentries/attrs gone)
  if [ $cfg != native ]; then umount_fuse; drop; mount_fuse; else drop; fi
}
cleanup() { [ $cfg != native ] && umount_fuse; sudo rm -rf $BASE/native/* $BASE/backing/*; sync; sleep ${SETTLE:-20}; }

trap 'cleanup' EXIT
cleanup
[ $cfg != native ] && mount_fuse
sudo chown daniel: $D 2>/dev/null
cd /tmp
export GIT_CONFIG_GLOBAL=/tmp/gitconfig; printf '[user]\n name=b\n email=b@b\n[safe]\n directory=*\n' > /tmp/gitconfig
echo "== $vm $cfg: setup" >&2
rm -rf /tmp/rs.git.tmp
git clone -q --no-hardlinks /tmp/rs.git $D/repo
git -C $D/repo checkout -q -b bench
files=$(git -C $D/repo ls-files | wc -l); echo "tracked files: $files" >&2

# --- worktree add/remove
for variant in default w16; do
  opt=""; [ $variant = w16 ] && opt="-c checkout.workers=16"
  for r in $(seq 1 $N); do
    sync
    timeit wt_add_$variant $r git -C $D/repo $opt worktree add --detach $D/wt_$variant
    timeit wt_remove_$variant $r git -C $D/repo worktree remove --force $D/wt_$variant
  done
done

# --- status / stat-walk on a fresh worktree
git -C $D/repo worktree add -q --detach $D/wt_s
git -C $D/wt_s status > /dev/null   # refresh index once
for r in $(seq 1 $N); do timeit status_warm $r git -C $D/wt_s status; done
for r in $(seq 1 $N); do timeit statwalk_warm $r sh -c "cd $D/wt_s && find . -type f | xargs stat -c '%s %Y %i'"; done
for r in $(seq 1 $N); do
  cold; timeit status_cold $r git -C $D/wt_s status
  cold; timeit statwalk_cold $r sh -c "cd $D/wt_s && find . -type f | xargs stat -c '%s %Y %i'"
done

# --- 10k small files (100 dirs x 100 files, 4 KiB each)
payload=$(head -c 4096 /dev/zero | tr '\0' x)
mk() { mkdir -p $D/small; cd $D/small; for d in $(seq 1 100); do mkdir d$d; for f in $(seq 1 100); do echo "$payload" > d$d/f$f; done; done; }
for r in $(seq 1 $N); do
  timeit small_create $r bash -c "$(declare -f mk); D=$D; payload='$payload'; mk"
  sync
  timeit small_delete $r rm -rf $D/small
done

# --- 1 GiB sequential and 4 KiB random
fio_common="--filename=$D/fio.dat --size=$FIOSIZE --ioengine=psync --group_reporting --output-format=json"
fiojson() { # extract bw MiB/s (or iops) from fio json using jq if present, else grep
  jq -r "$1" 2>/dev/null
}
for r in $(seq 1 $N); do
  rm -f $D/fio.dat; sync; sleep ${SETTLE:-20}
  out=$(fio --name=w --rw=write --bs=1M --end_fsync=1 $fio_common)
  rec seqwrite_fsync_1G $r $(echo "$out" | jq '.jobs[0].write.bw_bytes/1048576') MiB/s
done
for r in $(seq 1 $N); do  # no fsync: measures the write path into the page cache
  rm -f $D/fio.dat; sync; sleep ${SETTLE:-20}
  out=$(fio --name=w --rw=write --bs=1M $fio_common)
  rec seqwrite_nosync_1G $r $(echo "$out" | jq '.jobs[0].write.bw_bytes/1048576') MiB/s
done
sync; fio --name=prep --rw=write --bs=1M --end_fsync=1 $fio_common > /dev/null
for r in $(seq 1 $N); do
  drop; [ $cfg != native ] && { umount_fuse; drop; mount_fuse; }
  out=$(fio --name=r --rw=read --bs=1M --invalidate=1 $fio_common)
  rec seqread_cold_1G $r $(echo "$out" | jq '.jobs[0].read.bw_bytes/1048576') MiB/s
  out=$(fio --name=r --rw=read --bs=1M --invalidate=0 $fio_common)
  rec seqread_warm_1G $r $(echo "$out" | jq '.jobs[0].read.bw_bytes/1048576') MiB/s
done
for r in $(seq 1 $N); do  # warm: file fully in page cache
  out=$(fio --name=rr --rw=randread --bs=4k --runtime=10 --time_based --invalidate=0 $fio_common)
  rec randread4k_warm $r $(echo "$out" | jq '.jobs[0].read.iops') IOPS
  rec randread4k_warm_lat_p50 $r $(echo "$out" | jq '.jobs[0].read.clat_ns.percentile["50.000000"]/1000') us
done
for r in $(seq 1 3); do  # cold: drop caches first; reads pull pages from btrfs on demand
  drop; [ $cfg != native ] && { umount_fuse; drop; mount_fuse; }
  out=$(fio --name=rr --rw=randread --bs=4k --runtime=10 --time_based --invalidate=1 $fio_common)
  rec randread4k_cold $r $(echo "$out" | jq '.jobs[0].read.iops') IOPS
done
for r in $(seq 1 3); do  # mmap engine (page-fault path), warm
  out=$(fio --name=mm --invalidate=0 --rw=randread --bs=4k --runtime=10 --time_based --filename=$D/fio.dat --size=$FIOSIZE --ioengine=mmap --group_reporting --output-format=json)
  rec randread4k_mmap_warm $r $(echo "$out" | jq '.jobs[0].read.iops') IOPS
done
rm -f $D/fio.dat

# --- build: cargo build of experiments/fuse-pt itself (vendored, offline)
for r in $(seq 1 3); do
  rm -rf $D/buildsrc; cp -r /tmp/buildsrc $D/buildsrc
  sync
  timeit cargo_build $r sh -c "cd $D/buildsrc && cargo build --offline"
done

# --- per-op cold latency (stat, open) on 3000 files of the worktree
(cd $D/wt_s; find . -type f | sed "s|^\.|$D/wt_s|" | sort > /tmp/files.list)
for phase in stat open_cold open_warm; do
  for r in $(seq 1 3); do
    if [ $cfg != native ]; then umount_fuse; echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null; mount_fuse; else echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null; fi
    line=$(/tmp/lat $phase /tmp/files.list 3000)
    echo "$line" >&2
    rec lat_${phase}_median $r $(echo $line | sed 's/.*median_us=\([0-9.]*\).*/\1/') us
    rec lat_${phase}_p99 $r $(echo $line | sed 's/.*p99_us=\([0-9.]*\).*/\1/') us
  done
done
echo "== $vm $cfg done" >&2
