#!/bin/bash
# Run on the VM as root-capable user (sudo). usage: bench.sh <outfile> ; expects /tmp/fuse-uring and /tmp/lat
# Optional: LIBFUSE_HP=/path/to/passthrough_hp enables the libfuse reference configs.
OUT=${1:-/tmp/bench.out}; MNT=/mnt/t; BK=/dev/shm/bk
: > $OUT
iters() { case $1 in createunlink) echo 30000;; *) echo 100000;; esac; }
run_clients() { # cfg
  IFS="|" read -ra SCS <<< "${SCEN-1 nopin|1 pin|2 pin}"; for sc in "${SCS[@]}"; do
    set -- $1 $sc; cfg=$1; n=$2; p=$3; flag=""; [ $p = pin ] && flag="--pin"
    for op in stat openclose createunlink; do
      for rep in 1 2 3; do
        echo -e "$cfg\t$p\t$(sudo /tmp/lat $MNT $op $(iters $op) $n $flag)" >> $OUT
      done
    done
  done
}
stop() { sudo umount $MNT 2>/dev/null; sleep 0.7; sudo pkill -f "fuse-uring --mode" ; sudo pkill passthrough_hp; sleep 0.3; }
start_ours() { stop; (sudo /tmp/fuse-uring "$@" $MNT > /tmp/srv.log 2>&1 &); sleep 1.5; }
OURS=${OURS-"classic:--mode classic|classic-pin:--mode classic --pin|uring:--mode uring|uring-d1:--mode uring --depth 1|uring-d16:--mode uring --depth 16"}
IFS="|" read -ra CFGS <<< "$OURS"
for cfg in "${CFGS[@]}"; do
  name=${cfg%%:*}; args=${cfg#*:}; start_ours $args; run_clients $name; stop
done
if [ -n "$LIBFUSE_HP" ]; then
  sudo mkdir -p $BK
  for cfg in "hp-classic:" "hp-uring:-o io_uring"; do
    name=${cfg%%:*}; args=${cfg#*:}; stop
    (LD_LIBRARY_PATH=/tmp sudo -E $LIBFUSE_HP $args -o allow_other --nocache --nopassthrough --foreground $BK $MNT > /tmp/hp.log 2>&1 &); sleep 2
    echo "$name io_uring fds: $(sudo ls -l /proc/$(pgrep -f 'passthrough_hp' | tail -1)/fd 2>/dev/null | grep -c io_uring)" >> /tmp/hp.fds
    run_clients $name; stop
  done
fi
