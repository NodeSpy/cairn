#!/bin/bash
# E5: overlayfs over placeholders. Run as root on the VM. Needs /usr/local/bin/xp.
XP=/usr/local/bin/xp
B=/cache/exp-ovl/e5; STORE=/dev/shm/e5store; OUT=/dev/shm/e5out
mkdir -p $OUT
cleanup() { pkill -x xp 2>/dev/null; sleep 0.3; mountpoint -q $B/merged && umount $B/merged; }
setup() {
  cleanup
  rm -rf $B $STORE; mkdir -p $B/lower/sub $B/upper $B/work $B/merged $STORE/sub
  head -c 20000 /dev/urandom > $STORE/small.txt
  head -c 4194304 /dev/urandom > $STORE/big.bin
  head -c 100000 /dev/urandom > $STORE/sub/child.dat
  cp /usr/bin/true $STORE/true
  for f in small.txt big.bin sub/child.dat true; do
    truncate -r $STORE/$f $B/lower/$f; chmod --reference=$STORE/$f $B/lower/$f; touch -r $STORE/$f $B/lower/$f
  done
  mount -t overlay overlay -o lowerdir=$B/lower,upperdir=$B/upper,workdir=$B/work $B/merged
}
start_hsmd() { # marktype fill evflags
  local m=$1 fill=${2:-pwrite} ev=${3:-rdwr} args=()
  case $m in
    inode) args=(--mark inode:$B/lower) ;;
    dir) args=(--mark dir:$B/lower --mark dir:$B/lower/sub) ;;
    mount) args=(--mark mount:/cache) ;;
    sb) args=(--mark sb:/cache) ;;
  esac
  : > $OUT/hsmd.log
  $XP hsmd --log $OUT/hsmd.log --root $B/lower --store $STORE --fill $fill --evflags $ev --writevia ${WV:-path} "${args[@]}" &
  HP=$!
  for i in $(seq 50); do grep -q 'READY\|MARKFAIL' $OUT/hsmd.log && break; sleep 0.1; done
  grep 'READY\|MARKFAIL' $OUT/hsmd.log | sed 's/^/    /'
}
stop_hsmd() { kill $HP 2>/dev/null; wait $HP 2>/dev/null; }
run_op() { # marktype op [fill] [evflags]
  local m=$1 op=$2 fill=${3:-pwrite} ev=${4:-${EV:-rdonly}}
  setup; start_hsmd $m $fill $ev
  echo "=== mark=$m op=$op fill=$fill evflags=$ev"
  case $op in
    read) timeout 20 $XP sum $B/merged/small.txt; echo "  expected: $(sha_fnv $STORE/small.txt)" ;;
    write) timeout 20 $XP write $B/merged/small.txt
           echo "  differing bytes merged vs store: $(cmp -l $B/merged/small.txt $STORE/small.txt | wc -l) (7 expected if copy-up got real data)"
           echo "  upper copy differing bytes: $(cmp -l $B/upper/small.txt $STORE/small.txt 2>&1| wc -l)" ;;
    mmap) timeout 20 $XP mmapread $B/merged/small.txt ;;
    exec) timeout 20 $B/merged/true; echo "  exec rc=$?" ;;
    stat) stat -c '  stat %n size=%s blocks=%b' $B/merged/small.txt $B/merged/true; ls -l $B/merged $B/merged/sub >/dev/null; echo "  ls ok rc=$?" ;;
    direct-read) timeout 20 $XP sum $B/lower/small.txt; echo "  expected: $(sha_fnv $STORE/small.txt)" ;;
    direct-mmap) timeout 20 $XP mmapread $B/lower/small.txt ;;
    direct-exec) timeout 20 $B/lower/true; echo "  exec rc=$?" ;;
    dir-child-read) timeout 20 $XP sum $B/merged/sub/child.dat ;;
  esac
  sleep 0.3
  echo "  lower/small.txt alloc blocks after: $(stat -c %b $B/lower/small.txt)  lower/true blocks: $(stat -c %b $B/lower/true)"
  echo "  -- events:"; grep -v READY $OUT/hsmd.log | sed 's/^/    /'
  stop_hsmd
}
sha_fnv() { $XP sum $1; }

if [ -n "$1" ]; then run_op "$@"; cleanup; exit; fi
for m in inode dir mount sb; do
  for op in read write mmap stat exec direct-read direct-mmap direct-exec; do run_op $m $op; done
  
done
run_op dir dir-child-read
WV=eventfd EV=rdwr run_op inode read
WV=eventfd EV=rdwr run_op inode direct-read
WV=eventfd EV=rdwr run_op inode exec
WV=path EV=rdwr run_op inode read
run_op inode mmap none
run_op inode write none
cleanup
