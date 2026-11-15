#!/bin/bash
# Two-host remote-change inotify check with the real Core: a watcher on host B sees what a
# writer on host A does, replayed by B's applier through B's mount. Both hosts run cairnd against
# one cairn-server (root "proj" mounted at /mnt/cfuse). Run from anywhere with ssh access:
#   WRITER=daniel@192.0.2.11 WATCHER=daniel@192.0.2.12 ./e2e-inotify.sh
set -u
WRITER=${WRITER:?}; WATCHER=${WATCHER:?}; M=${M:-/mnt/cfuse}; N=${N:-20}
ssh $WRITER "sudo rm -rf $M/w; sudo mkdir -p $M/w"; sleep 2
ssh $WATCHER "sudo ls $M/w >/dev/null; sudo rm -f /var/tmp/ev.log; (inotifywait -m -r -q --format '%e %w%f' $M/w > /var/tmp/ev.log 2>&1 &); sleep 1"
ssh $WRITER "sudo sh -c 'cd $M/w; for i in \$(seq 1 $N); do echo hello > f1; chmod 600 f1; mkdir sub; echo more >> f1; mv f1 f2; ln -s f2 sl; setfattr -n user.x -v 1 f2; touch -d @1700000000 f2; rm f2 sl; rmdir sub; done'"
sleep 5
echo "== events seen by the watcher over $N cycles (per cycle: CREATE 3, MOVED_FROM/TO 1, DELETE 3 (+1 DELETE_SELF), ATTRIB >=3, MODIFY >=1, CLOSE_WRITE >=1)"
for ev in CREATE MODIFY ATTRIB CLOSE_WRITE MOVED_FROM MOVED_TO DELETE; do
  printf '%-12s %s\n' $ev "$(ssh $WATCHER "grep -c '^$ev\\b\\|^[A-Z_,]*$ev' /var/tmp/ev.log")"
done
ssh $WATCHER "sudo pkill -x inotifywait; sudo ls $M/w | wc -l | sed 's/^/entries left in w (expect 0): /'"
