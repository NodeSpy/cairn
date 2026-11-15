#!/bin/bash
# E11 remote-change inotify check. Run on a test VM: ./inotify-check.sh [pt|nopt]
# Starts the daemon with the remote-change simulator, watches the mount with inotifywait -m -r, runs a
# concurrent stat/read/write load in the mount to provoke self-deadlock, then counts events.
set -u
mode=${1:-pt}; cycles=${CYCLES:-40}
BIN=${BIN:-/tmp/fuse-pt}; EXTRA=${EXTRA:-}   # EXTRA="--uring" to run the check over FUSE-over-io_uring
flags=""; [ "$mode" = pt ] && flags=--passthrough || flags=--writeback
sudo umount /mnt/lfs 2>/dev/null; sudo rm -rf /cache/backing/*; mkdir -p /cache/backing/load /cache/backing/remote
for i in $(seq 1 500); do echo $i > /cache/backing/load/f$i; done
sudo sh -c "$BIN --backing /cache/backing --mount /mnt/lfs $flags $EXTRA --simulate-remote remote --sim-cycles $cycles --sim-interval-ms 100 --sim-delay-ms 1500 > /tmp/pt-$mode.log 2>&1 &"
sleep 0.5
inotifywait -m -r -q --format '%w%f %e' /mnt/lfs > /tmp/ino-$mode.log 2>&1 &
ipid=$!
sleep 0.5
# concurrent load while the simulator runs
( for k in $(seq 1 30); do find /mnt/lfs/load -type f | xargs stat > /dev/null; cat /mnt/lfs/load/* > /dev/null; done ) &
( for k in $(seq 1 30); do dd if=/dev/zero of=/mnt/lfs/load/big bs=1M count=16 2>/dev/null; rm -f /mnt/lfs/load/big; done ) &
( while [ -f /tmp/pt-$mode.log ] && ! grep -q "cycle $cycles " /tmp/pt-$mode.log; do ls -l /mnt/lfs/remote > /dev/null 2>&1; stat /mnt/lfs/remote/f1 >/dev/null 2>&1; done ) &
start=$(date +%s)
while ! grep -q "cycle $cycles " /tmp/pt-$mode.log && [ $(( $(date +%s) - start )) -lt 60 ]; do sleep 0.2; done
wait $(jobs -p | grep -v $ipid) 2>/dev/null
sleep 0.5; kill $ipid 2>/dev/null
echo "== mode=$mode"
grep -c 'cycle .* ok' /tmp/pt-$mode.log | sed 's/^/cycles ok: /'
grep -c FAILED /tmp/pt-$mode.log | sed 's/^/cycles failed: /'
for ev in MODIFY ATTRIB MOVED_FROM MOVED_TO DELETE CREATE CLOSE_WRITE; do
  printf '%-12s %s\n' $ev "$(grep '/remote/' /tmp/ino-$mode.log | grep -c "$ev")"
done
sudo umount /mnt/lfs 2>/dev/null; sleep 1.5; pgrep -x $(basename $BIN) > /dev/null && echo "daemon still running" || echo "daemon exited cleanly"
