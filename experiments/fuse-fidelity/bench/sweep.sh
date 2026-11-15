#!/bin/bash
# E11 thread-count sweep (passthrough, tmpfs backing). usage: sweep.sh <vm-label>; appends to /tmp/results-sweep.tsv
set -u
vm=$1; BASE=/mnt/ram; D=/mnt/lfs; OUT=/tmp/results-sweep.tsv
export GIT_CONFIG_GLOBAL=/tmp/gitconfig
rec() { echo -e "$vm\tpt-threads$T\t$1\t$2\t$3\t${4:-s}" >> $OUT; }
timeit() { local w=$1 r=$2; shift 2; local t0=$(date +%s.%N); "$@" >/dev/null 2>&1; rec $w $r $(echo "$(date +%s.%N) - $t0" | bc -l); }
start() { sudo sh -c "/tmp/fuse-pt --backing $BASE/backing --mount $D --passthrough --threads $T > /tmp/fuse-sweep.log 2>&1 &"; for i in $(seq 50); do mountpoint -q $D && return; sleep 0.1; done; exit 1; }
stop() { sudo umount $D; while pgrep -x fuse-pt >/dev/null; do sleep 0.1; done; }
payload=$(head -c 4096 /dev/zero | tr '\0' x)
mk() { mkdir -p $D/small; cd $D/small; for d in $(seq 1 100); do mkdir d$d; for f in $(seq 1 100); do echo "$payload" > d$d/f$f; done; done; }
for T in 1 2 4 8; do
  sudo rm -rf $BASE/backing/*; mkdir -p $BASE/backing; sudo chown daniel: $BASE/backing
  start
  git clone -q --no-hardlinks /tmp/rs.git $D/repo; git -C $D/repo checkout -q -b bench
  for r in 1 2 3 4 5; do
    timeit wt_add $r git -C $D/repo worktree add --detach $D/wt
    timeit wt_remove $r git -C $D/repo worktree remove --force $D/wt
  done
  for r in 1 2 3; do timeit small_create $r bash -c "$(declare -f mk); D=$D; payload='$payload'; mk"; timeit small_delete $r rm -rf $D/small; done
  git -C $D/repo worktree add -q --detach $D/wt_s
  (cd $D/wt_s; find . -type f | sed "s|^\.|$D/wt_s|" | sort > /tmp/files.list)
  for r in 1 2 3; do
    stop; echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null; start
    rec lat_stat_median $r $(/tmp/lat stat /tmp/files.list 3000 | sed 's/.*median_us=\([0-9.]*\).*/\1/') us
    stop; echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null; start
    rec lat_open_cold_median $r $(/tmp/lat open_cold /tmp/files.list 3000 | sed 's/.*median_us=\([0-9.]*\).*/\1/') us
  done
  stop
done
sudo rm -rf $BASE/backing/*
