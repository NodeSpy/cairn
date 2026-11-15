#!/bin/bash
# Run the benchmark suite on a client VM (default lfs-a): native btrfs vs cairnd mount.
# usage: ./run.sh [--no-build] [vm=a] [runs=3]    Output: results-<vm>-<date>.tsv + .md here.
# Requires the e2e provisioning (tests/e2e/lib.sh) and inputs in the VM's /tmp (rs.git, buildsrc, lat).
source "$(dirname "$0")/../e2e/lib.sh"
trap 'cleanup' EXIT INT TERM
BUILD=1; [ "${1:-}" = "--no-build" ] && { BUILD=0; shift; }
H=${1:-a}; N=${2:-3}
[ $BUILD = 1 ] && { build || exit 2; }
deploy || exit 2
fresh || { echo "provision failed"; exit 2; }
umount_client $H
# mount/umount hooks used by bench.sh (cold runs remount cairnd)
vm $H "cat > /tmp/cairn-mount.sh" <<EOS
mkdir -p $MNT $CACHE; chown daniel: $MNT
nohup cairnd --config /etc/cairn/cairnd.toml > /tmp/cairnd.log 2>&1 < /dev/null &
EOS
vm $H "cat > /tmp/cairn-umount.sh" <<EOS
fusermount3 -uz $MNT 2>/dev/null || umount -l $MNT 2>/dev/null; pkill -f '[c]airnd --config /etc/cairn/cairnd.toml'; for i in \$(seq 1 300); do pgrep -f '[c]airnd --config /etc/cairn/cairnd.toml' >/dev/null || break; sleep 0.1; done
EOS
scp $SSH_OPTS -q "$HERE/../bench/bench.sh" daniel@"$(ip_of $H)":/tmp/cairn-bench.sh
OUTF=/tmp/cairn-results.tsv; vm $H "rm -f $OUTF"
ENVS="SETTLE=${SETTLE:-5} FIOSIZE=${FIOSIZE:-256M}"
echo "== native"; vm $H "$ENVS OUT=$OUTF bash /tmp/cairn-bench.sh native lfs-$H $N" 2>&1 | tail -5
echo "== cairn classic"; vm $H "$ENVS OUT=$OUTF bash /tmp/cairn-bench.sh cairn lfs-$H $N" 2>&1 | tail -5
# uring variant: same, cairnd started with --uring (fuse.enable_uring=1 on the VM)
vm $H "sed -i 's#cairnd --config /etc/cairn/cairnd.toml#cairnd --config /etc/cairn/cairnd.toml --uring#' /tmp/cairn-mount.sh"
echo "== cairn uring"; vm $H "SUFFIX=-uring $ENVS OUT=$OUTF bash /tmp/cairn-bench.sh cairn lfs-$H $N" 2>&1 | tail -5
D=$(dirname "$0"); T=$D/results-$H-$(date +%Y%m%d-%H%M).tsv
vm $H "cat $OUTF" > "$T"
python3 -I "$D/report.py" "$T" | tee "${T%.tsv}.md"
