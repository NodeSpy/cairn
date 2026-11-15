#!/bin/bash
# E11 NFS baseline on the devbox's real NFS mount. Scratch dir is created and removed by this script.
# Cold (drop_caches) variants are skipped: that needs root and we do not touch the devbox system state.
set -u
N=${1:-3}
D=$HOME/Projects/.leasefs-bench
OUT=${OUT:-/tmp/results-nfs.tsv}
rec() { echo -e "devbox\tnfs\t$1\t$2\t$3\ts" >> $OUT; }
timeit() { local w=$1 r=$2; shift 2; local t0=$(date +%s.%N); "$@" > /dev/null 2>&1; local rc=$?; local t1=$(date +%s.%N); [ $rc -ne 0 ] && echo "WARN $w rc=$rc" >&2; rec $w $r $(echo "$t1 - $t0" | bc -l); }
trap 'rm -rf "$D"' EXIT
rm -rf "$D"; mkdir -p "$D"
export GIT_CONFIG_GLOBAL=/tmp/gitconfig-bench GIT_CONFIG_SYSTEM=/dev/null
printf '[user]\n name=b\n email=b@b\n[safe]\n directory=*\n' > $GIT_CONFIG_GLOBAL
git clone -q --no-hardlinks ~/.cache/leasefs-bench/rs.git $D/repo
git -C $D/repo checkout -q -b bench
for variant in default w16; do
  opt=""; [ $variant = w16 ] && opt="-c checkout.workers=16"
  for r in $(seq 1 $N); do
    timeit wt_add_$variant $r git -C $D/repo $opt worktree add --detach $D/wt_$variant
    timeit wt_remove_$variant $r git -C $D/repo worktree remove --force $D/wt_$variant
  done
done
git -C $D/repo worktree add -q --detach $D/wt_s
git -C $D/wt_s status > /dev/null
for r in $(seq 1 $N); do timeit status_warm $r git -C $D/wt_s status; done
for r in $(seq 1 $N); do timeit statwalk_warm $r sh -c "cd $D/wt_s && find . -type f | xargs stat -c '%s %Y %i'"; done
payload=$(head -c 4096 /dev/zero | tr '\0' x)
mk() { mkdir -p $D/small; cd $D/small; for d in $(seq 1 100); do mkdir d$d; for f in $(seq 1 100); do echo "$payload" > d$d/f$f; done; done; }
for r in $(seq 1 $N); do
  timeit small_create $r bash -c "$(declare -f mk); D=$D; payload='$payload'; mk"
  timeit small_delete $r rm -rf $D/small
done
