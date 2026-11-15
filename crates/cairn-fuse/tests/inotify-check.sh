#!/bin/bash
# Remote-change inotify check for cairnd (MockCore + simulator). Run as a sudoer on a test VM:
#   BIN=/cache/tools/cairnd EXTRA=--uring CYCLES=40 ./inotify-check.sh
# The simulator delivers remote create/modify/chmod/xattr/rename/delete cycles through the applier
# while a concurrent stat/read/write load runs; inotifywait -r counts what a watcher sees.
set -u
cycles=${CYCLES:-40}; BIN=${BIN:-/cache/tools/cairnd}; EXTRA=${EXTRA:-}
M=/mnt/ct; BK=/cache/ct-backing; L=/tmp/cairnd-ino.log; E=/tmp/cairnd-ino-events.log
rm -f /tmp/cairnd-sim-go; sudo umount $M 2>/dev/null; sudo rm -rf $BK; sudo mkdir -p $M $BK
sudo sh -c "$BIN --mock $BK --mount $M $EXTRA --simulate-remote remote --sim-cycles $cycles --sim-interval-ms 100 --sim-start-file /tmp/cairnd-sim-go > $L 2>&1 &"
sleep 1
sudo mkdir $M/load; for i in $(seq 1 300); do echo $i | sudo tee $M/load/f$i >/dev/null; done
inotifywait -m -r -q --format '%w%f %e' $M > $E 2>&1 &
ipid=$!
sleep 0.5; touch /tmp/cairnd-sim-go
( for k in $(seq 1 20); do find $M/load -type f | xargs stat > /dev/null; cat $M/load/* > /dev/null; done ) &
( for k in $(seq 1 20); do dd if=/dev/zero of=$M/load/big bs=1M count=16 2>/dev/null; sudo rm -f $M/load/big; done ) &
( while ! grep -q "cycle $cycles " $L; do ls -l $M/remote > /dev/null 2>&1; stat $M/remote/f1 >/dev/null 2>&1; done ) &
start=$(date +%s)
while ! grep -q "cycle $cycles " $L && [ $(( $(date +%s) - start )) -lt 90 ]; do sleep 0.2; done
wait $(jobs -p | grep -v $ipid) 2>/dev/null
sleep 1; kill $ipid 2>/dev/null
echo "== cycles done: $(grep -c 'cycle .* done' $L)  failed changes: $(grep -c 'failed' $L)"
for ev in CREATE MODIFY ATTRIB CLOSE_WRITE MOVED_FROM MOVED_TO DELETE; do
  printf '%-12s %s\n' $ev "$(grep '/remote/' $E | grep -c "$ev")"
done
grep -E 'remote/? ' $E | head -3
sudo pkill -TERM -x $(basename $BIN); sleep 1.5
mount | grep -q " $M " && { echo "STILL MOUNTED"; sudo umount -l $M; } || echo "unmounted cleanly"
pgrep -x $(basename $BIN) >/dev/null && echo "daemon still running" || echo "daemon exited"
