#!/bin/bash
# E7: reflink / dedupe on btrfs. Run as root on the VM: e7.sh <section>...  (no args = all but bulk)
XP=/usr/local/bin/xp
D=/cache/exp-ovl/e7; OUT=/dev/shm/e7out; mkdir -p $D $OUT
rnd() { head -c "$1" /dev/urandom > "$2"; }
used() { btrfs filesystem sync /cache >/dev/null; sleep 3; btrfs filesystem usage -b /cache 2>/dev/null | awk '/^ *Used:/{print $2; exit}'; }
watch_run() { # secs path... -- cmd...   (prints inotify events produced by cmd)
  local secs=$1; shift; local paths=(); while [ "$1" != "--" ]; do paths+=("$1"); shift; done; shift
  $XP inw $secs "${paths[@]}" > $OUT/inw.txt & local w=$!
  while ! grep -q INW_READY $OUT/inw.txt; do sleep 0.05; done
  "$@"; wait $w; echo "  inotify events:"; sed 's/^/    /' $OUT/inw.txt | grep -v INW_READY
}
hsm_start() { : > $OUT/hsm.log; $XP hsmd --log $OUT/hsm.log "$@" & HP=$!; for i in $(seq 100); do grep -q 'READY\|MARKFAIL' $OUT/hsm.log && break; sleep 0.1; done; }
hsm_stop() { kill $HP 2>/dev/null; wait $HP 2>/dev/null; }

s_clone() {
  echo "##### FICLONE whole file"
  rm -rf $D/c; mkdir -p $D/c; rnd 1048576 $D/c/src; rnd 100001 $D/c/src_odd; rnd 65536 $D/c/old
  echo "- clone to new file (1 MiB):"; watch_run 2 $D/c -- $XP clone $D/c/src $D/c/new
  echo "- clone odd-size (100001 B) file:"; $XP clone $D/c/src_odd $D/c/new_odd; cmp $D/c/src_odd $D/c/new_odd && echo "  content identical"
  echo "- clone onto existing larger/different dst (64 KiB -> 1 MiB, then 1 MiB -> 100001 B):"
  $XP clone $D/c/src $D/c/old; $XP clone $D/c/src_odd $D/c/old; stat -c '  old size now %s' $D/c/old
  echo "- clone onto itself:"; $XP clone $D/c/src $D/c/src
  echo "- shared extents (filefrag physical of src vs new):"
  filefrag -v $D/c/src | sed -n '4p'; filefrag -v $D/c/new | sed -n '4p'
  echo "- cross-directory/subvolume boundary: skipped (single subvolume)"
}
s_range() {
  echo "##### FICLONERANGE alignment (bsize 4096; src S=16385 bytes, dst D=65536 bytes random)"
  rm -rf $D/r; mkdir -p $D/r; rnd 16385 $D/r/S
  t() { rnd 65536 $D/r/D; echo -n "[$1] "; shift; $XP clonerange "$@" 2>&1 | sed 's/ dst:.*//'; }
  t "aligned 4096@0 -> @0"            $D/r/S 0 4096 $D/r/D 0
  t "aligned 8192@4096 -> @16384"     $D/r/S 4096 8192 $D/r/D 16384
  t "src_off unaligned (1)"           $D/r/S 1 4096 $D/r/D 0
  t "dst_off unaligned (1)"           $D/r/S 0 4096 $D/r/D 1
  t "len unaligned 100, mid-file"     $D/r/S 0 100 $D/r/D 0
  t "len 4097, not reaching src EOF"  $D/r/S 0 4097 $D/r/D 0
  t "len 0 (=to EOF) from 4096"       $D/r/S 4096 0 $D/r/D 4096
  t "tail incl. unaligned EOF 12288+4097 -> dst@12288 (dst longer than end)" $D/r/S 12288 4097 $D/r/D 12288
  rnd 65536 $D/r/D; truncate -s 20000 $D/r/D; echo -n "[tail to dst EOF: dst=20000 B, S[12288..EOF]->dst@12288] "; $XP clonerange $D/r/S 12288 0 $D/r/D 12288 | sed 's/ dst:.*//'
  rnd 65536 $D/r/D; truncate -s 16385 $D/r/D; echo -n "[dst==16385 B (same size as src): tail 12288+4097 -> dst@12288] "; $XP clonerange $D/r/S 12288 4097 $D/r/D 12288 | sed 's/ dst:.*//'
  rnd 65536 $D/r/D; truncate -s 16385 $D/r/D; echo -n "[dst==16385 B: whole file len 0 from 0 (FICLONE-like)] "; $XP clonerange $D/r/S 0 0 $D/r/D 0 | sed 's/ dst:.*//'
  rnd 65536 $D/r/D; truncate -s 16384 $D/r/D; echo -n "[dst==16384 B (aligned, shorter than src's unaligned end): tail 12288+4097] "; $XP clonerange $D/r/S 12288 4097 $D/r/D 12288 | sed 's/ dst:.*//'
  t "range past src EOF 12288+8192"   $D/r/S 12288 8192 $D/r/D 12288
  t "dst offset past dst EOF (aligned 1MiB), len 8192" $D/r/S 0 8192 $D/r/D 1048576
  t "src_off past src EOF"            $D/r/S 32768 4096 $D/r/D 0
  rnd 65536 $D/r/D; echo -n "[self, non-overlapping 0..4096 -> 8192] "; cp $D/r/D $D/r/D2; $XP clonerange $D/r/D2 0 4096 $D/r/D2 8192 | sed 's/ dst:.*//'
  echo -n "[self, overlapping 0..8192 -> 4096] "; $XP clonerange $D/r/D2 0 8192 $D/r/D2 4096 | sed 's/ dst:.*//'
  echo "- full result incl. timestamps for one aligned clonerange into a mid-file dst:"; rnd 65536 $D/r/D; $XP clonerange $D/r/S 0 4096 $D/r/D 8192
  echo "- clonerange with src range lying in a file with FICLONERANGE to a different filesystem: n/a"
}
s_events() {
  echo "##### clone with FAN_PRE_ACCESS marks present (hsmd --fill none = log only)"
  rm -rf $D/f; mkdir -p $D/f; rnd 1048576 $D/f/src
  for case in "dst-marked" "src-marked" "both-marked" "dir-marked(dst dir, ON_CHILD)"; do
    rnd 65536 $D/f/dst; echo "== $case"
    case "$case" in
      dst-marked) args=(--mark inode:$D/f/dst) ;; src-marked) args=(--mark inode:$D/f/src) ;;
      both-marked) args=(--mark inode:$D/f/dst --mark inode:$D/f/src) ;; *) args=(--mark dir:$D/f) ;; esac
    hsm_start --fill none --root $D/f --store $D/f --evflags rdwr "${args[@]}"
    echo "- FICLONE:"; timeout 10 $XP clone $D/f/src $D/f/dst; echo "  rc=$?"
    echo "- FICLONERANGE 4096@0:"; timeout 10 $XP clonerange $D/f/src 0 4096 $D/f/dst 0; echo "  rc=$?"
    echo "- dedupe (ro dst):"; cp --reflink=never $D/f/src $D/f/dst; sleep 0.2; : > $OUT/hsm.log.mark
    timeout 10 $XP dedupe $D/f/src 0 1048576 $D/f/dst 0 ro | cut -c1-200; echo "  rc=$?"
    echo "- events seen by hsmd (excluding the file opens that this tool does not do):"; grep 'comm=xp' $OUT/hsm.log | sed 's/^/    /' | cut -c1-200
    hsm_stop
  done
  echo "== placeholder dst (sparse, size 1 MiB, inode-marked, fill none) cloned from real src"
  rm -f $D/f/ph; truncate -s 1048576 $D/f/ph; hsm_start --fill none --root $D/f --store $D/f --evflags rdwr --mark inode:$D/f/ph
  timeout 10 $XP clone $D/f/src $D/f/ph; cmp $D/f/src $D/f/ph && echo "  content identical after clone"; grep 'comm=xp' $OUT/hsm.log | sed 's/^/    /'; hsm_stop
}
s_ino() {
  echo "##### inotify + mtime/ctime for clone and dedupe"
  rm -rf $D/i; mkdir -p $D/i; rnd 1048576 $D/i/src; cp --reflink=never $D/i/src $D/i/dup
  echo "- FICLONE new file:"; watch_run 2 $D/i -- $XP clone $D/i/src $D/i/cl
  echo "- FICLONERANGE into existing:"; watch_run 2 $D/i $D/i/cl -- $XP clonerange $D/i/src 0 8192 $D/i/cl 0
  echo "- FIDEDUPERANGE dst opened O_RDONLY:"; watch_run 2 $D/i $D/i/dup -- $XP dedupe $D/i/src 0 1048576 $D/i/dup 0 ro
  rnd 1048576 $D/i/src2; cp --reflink=never $D/i/src2 $D/i/dup2
  echo "- FIDEDUPERANGE dst opened O_RDWR:"; watch_run 2 $D/i $D/i/dup2 -- $XP dedupe $D/i/src2 0 1048576 $D/i/dup2 0 rw
}
s_dedupe() {
  echo "##### FIDEDUPERANGE semantics"
  rm -rf $D/d; mkdir -p $D/d; cd $D/d
  rnd 1048576 a; cp --reflink=never a b
  echo "- identical 1 MiB:"; $XP dedupe a 0 1048576 b 0 ro
  filefrag -v a | sed -n '4p'; filefrag -v b | sed -n '4p'
  cp --reflink=never a c; printf 'X' | dd of=c bs=1 seek=500000 conv=notrunc 2>/dev/null
  echo "- differs in one byte (offset 500000), whole range:"; $XP dedupe a 0 1048576 c 0 ro
  echo "- first 4096 identical only, ask for 8192:"; $XP dedupe a 0 8192 c 0 ro
  echo "- differing byte lies outside requested range (ask first 400000-aligned 393216):"; $XP dedupe a 0 393216 c 0 ro
  rnd 100001 e; cp --reflink=never e f
  echo "- odd-size 100001 B file, len 100001 (EOF tail):"; $XP dedupe e 0 100001 f 0 ro
  echo "- odd-size, len 100001 requested with dst opened rw:"; cp --reflink=never e g; $XP dedupe e 0 100001 g 0 rw
  echo "- unaligned off/len mid-file (off 100, len 4096):"; $XP dedupe a 100 4096 b 100 ro
  echo "- len 4097 mid-file (off 0):"; $XP dedupe a 0 4097 b 0 ro
  echo "- inline-sized files (1000 B):"; rnd 1000 h; cp --reflink=never h i; $XP dedupe h 0 1000 i 0 ro
  echo "- src==dst same range:"; $XP dedupe a 0 4096 a 0 ro
  echo "- src==dst different range (identical content? no):"; $XP dedupe a 0 4096 a 4096 ro
  echo "- dst already shares extent with src:"; cp --reflink=always a j; $XP dedupe a 0 1048576 j 0 ro
  echo "- dst range beyond dst EOF:"; $XP dedupe a 0 1048576 e 0 ro
}
s_conc() {
  echo "##### dedupe with dst held open by other processes"
  cd $D; rm -rf k; mkdir k; cd k; rnd 1048576 a
  for mode in r w m mw; do
    cp --reflink=never a b; $XP hold b $mode 3 > $OUT/hold.txt & hp=$!
    while ! grep -q HOLD_READY $OUT/hold.txt; do sleep 0.05; done
    echo "- dst held open mode=$mode:"; timeout 10 $XP dedupe a 0 1048576 b 0 ro; echo "  rc=$?"
    echo "  (and RW dst fd while held:)"; cp --reflink=never a b2; timeout 10 $XP dedupe a 0 1048576 b 0 rw; echo "  rc=$?"
    wait $hp; sed 's/^/  holder: /' $OUT/hold.txt; cmp a b && echo "  content intact after"
    echo "  dst extents shared with src after? $(filefrag -v a | sed -n '4p' | awk '{print $5}') vs $(filefrag -v b | sed -n '4p' | awk '{print $5}')"
  done
  echo "- writer actively writing while dedupe runs: writer rewrites byte 0 repeatedly for 2s, 200 dedupe calls on the rest:"
  cp --reflink=never a b
  ( end=$((SECONDS+2)); while [ $SECONDS -lt $end ]; do printf 'W' | dd of=b bs=1 seek=0 conv=notrunc 2>/dev/null; done ) &
  w=$!; ok=0; same=0; diff=0; err=0
  for i in $(seq 200); do r=$($XP dedupe a 4096 1044480 b 4096 ro); case "$r" in *"status=SAME"*) same=$((same+1));; *"status=DIFFERS"*) diff=$((diff+1));; *) err=$((err+1)); echo "$r";; esac; done
  wait $w; echo "  results same=$same differs=$diff other=$err"
}
s_lease() {
  echo "##### dedupe vs read lease (F_RDLCK) held on dst"
  cd $D; rm -rf l; mkdir l; cd l; rnd 1048576 a; cp --reflink=never a b
  $XP lease b 4 > $OUT/lease.txt & lp=$!; while ! grep -q 'LEASE_READY\|LEASE_FAIL' $OUT/lease.txt; do sleep 0.05; done
  echo "- dedupe with dst opened O_RDONLY (does the ioctl itself break the lease?):"; $XP dedupe a 0 1048576 b 0 ro
  wait $lp; sed 's/^/  holder: /' $OUT/lease.txt
  cp --reflink=never a b
  $XP lease b 4 > $OUT/lease.txt & lp=$!; while ! grep -q 'LEASE_READY\|LEASE_FAIL' $OUT/lease.txt; do sleep 0.05; done
  echo "- dedupe with dst opened O_RDWR (the open breaks it?):"; timeout 60 $XP dedupe a 0 1048576 b 0 rw
  wait $lp; sed 's/^/  holder: /' $OUT/lease.txt
  cp --reflink=never a b
  $XP lease a 4 > $OUT/lease.txt & lp=$!; while ! grep -q 'LEASE_READY\|LEASE_FAIL' $OUT/lease.txt; do sleep 0.05; done
  echo "- lease on SRC, dedupe src(ro) -> dst:"; $XP dedupe a 0 1048576 b 0 ro
  wait $lp; sed 's/^/  holder: /' $OUT/lease.txt
  cp --reflink=never a b
  $XP lease b 4 > $OUT/lease.txt & lp=$!; while ! grep -q 'LEASE_READY\|LEASE_FAIL' $OUT/lease.txt; do sleep 0.05; done
  echo "- FICLONE onto leased dst (needs write open):"; timeout 60 $XP clone a b
  wait $lp; sed 's/^/  holder: /' $OUT/lease.txt
  echo "- F_RDLCK request on a file that is currently a dedupe/clone target but closed: ok if no writer (not tested separately)"
}
s_space() {
  echo "##### space reclaimed"
  cd $D; rm -rf sp; mkdir sp; cd sp; btrfs filesystem sync /cache; sleep 2
  u0=$(used); rnd $((256<<20)) a; cp --reflink=never a b; cp --reflink=never a c; u1=$(used)
  echo "- 3 x 256 MiB identical (separately written): fs used before=$u0 after write=$u1 (+$(( (u1-u0)>>20 )) MiB)"
  compsize -b $D/sp 2>&1 | sed 's/^/  compsize: /'; btrfs filesystem du -s $D/sp | sed 's/^/  fi du: /'
  $XP dedupefile a b 16M; $XP dedupefile a c 16M; u2=$(used)
  echo "- after dedupe b,c against a: fs used=$u2 (reclaimed $(( (u1-u2)>>20 )) MiB)"
  compsize -b $D/sp 2>&1 | sed 's/^/  compsize: /'; btrfs filesystem du -s $D/sp | sed 's/^/  fi du: /'
  filefrag -v a | sed -n '1,6p'; filefrag -v b | sed -n '4,5p'
  echo "- partial overlap: B = first half of A + new random half"
  rm -f b c; head -c $((128<<20)) a > p; rnd $((128<<20)) q; cat q >> p; u3=$(used)
  $XP dedupefile a p 16M; u4=$(used)
  echo "  (dedupefile stops at first differing chunk), fs used before=$u3 after=$u4"
  echo "  now dedupe only first 128 MiB range via chunks:"
  rm -f p; (head -c $((128<<20)) a; cat q) > p; u3=$(used)
  python3 - <<'PY' 2>/dev/null || echo "  (python3 missing, skip)"
PY
  for off in $(seq 0 16 112); do $XP dedupe a $((off<<20)) $((16<<20)) p $((off<<20)) ro | grep -o 'status=[A-Z]*\|bytes_deduped=[0-9]*' | tr '\n' ' '; done; echo
  u4=$(used); echo "  fs used before=$u3 after=$u4 (reclaimed $(( (u3-u4)>>20 )) MiB, expect ~128)"
  compsize -b $D/sp 2>&1 | sed 's/^/  compsize: /'
  echo "- sync fs used reading: $(df -B1 --output=used /cache | tail -1)"
}
s_bulk() {
  echo "##### dedupe throughput"
  cd $D; rm -rf bk; mkdir -p bk/a bk/b; cd bk
  rnd $((160<<20)) blob; split -b 16384 -a 5 -d blob a/f; rm blob
  cp -a --reflink=never a/. b/; sync; echo 3 > /proc/sys/vm/drop_caches
  echo "- 10k x 16 KiB (cold cache):"; ls a | wc -l | sed 's/^/  files: /'
  u0=$(used); $XP dedupebulk a b; u1=$(used); echo "  fs used before=$u0 after=$u1 (reclaimed $(( (u0-u1)>>20 )) MiB of ~156 expected)"
  echo "- same again (already shared, warm):"; $XP dedupebulk a b
  rm -rf a b; mkdir a b; rnd $((10000*4096)) blob; split -b 4096 -a 5 -d blob a/f; rm blob; cp -a --reflink=never a/. b/; sync; echo 3 > /proc/sys/vm/drop_caches
  echo "- 10k x 4 KiB (cold):"; $XP dedupebulk a b
  rm -rf a b; mkdir a b; rnd $((10000*1024)) blob; split -b 1024 -a 5 -d blob a/f; rm blob; cp -a --reflink=never a/. b/; sync
  echo "- 10k x 1 KiB (inline extents):"; $XP dedupebulk a b
  rm -rf a b
}
s_big() {
  echo "##### 1 GiB dedupe"
  cd $D; rm -rf bg; mkdir bg; cd bg; rnd $((1<<30)) big; cp --reflink=never big big2; cp --reflink=never big big3; cp --reflink=never big big4; sync
  echo "- cold cache dedupe (measured once earlier: single call 1 GiB took 851.7 s = 1.2 MiB/s, latency-bound, no readahead). Prefetching first:"
  echo 3 > /proc/sys/vm/drop_caches; t=$(date +%s.%N); cat big big2 > /dev/null; echo "  cat big big2 (2 GiB sequential cold read): $(echo "$(date +%s.%N) - $t" | bc) s"
  u0=$(used)
  echo "  single call for whole 1 GiB (warm):"; $XP dedupe big 0 $((1<<30)) big2 0 ro | cut -c1-110
  echo "  16 MiB chunks, warm:"; cat big3 > /dev/null; $XP dedupefile big big3 16M
  echo "  128 MiB chunks, warm:"; cat big4 > /dev/null; $XP dedupefile big big4 128M
  echo "  16 MiB chunks, warm, already shared:"; $XP dedupefile big big3 16M
  u1=$(used); echo "  fs used before dedupe=$u0 after=$u1 (reclaimed $(( (u0-u1)>>20 )) MiB, expect 3072)"
  cd $D; rm -rf bg
}
s_fill() {
  echo "##### fill path: FICLONE from CAS blob into placeholder inside the pre-content handler"
  F=$D/fill; rm -rf $F; mkdir -p $F/store $F/ph
  rnd 1048576 $F/store/one; touch -d '2020-01-02 03:04:05.123456789' $F/store/one
  truncate -r $F/store/one $F/ph/one; touch -r $F/store/one $F/ph/one
  for cfg in "pwrite rdwr eventfd 4" "clone rdwr eventfd 4" "pwrite rdonly path 4" "clone rdonly path 4" "clone rdonly path 1" "pwrite rdonly path 1"; do
    set -- $cfg; fill=$1; ev=$2; wv=$3; wk=$4
    rm -f $F/ph/one; truncate -r $F/store/one $F/ph/one; touch -r $F/store/one $F/ph/one
    echo "== fill=$fill evflags=$ev writevia=$wv workers=$wk"
    hsm_start --fill $fill --evflags $ev --writevia $wv --workers $wk --root $F/ph --store $F/store --mark inode:$F/ph
    stat -c '  before: blocks=%b mtime=%y ctime=%z' $F/ph/one
    if [ "$fill $wv $wk" = "clone eventfd 4" ]; then watch_run 2 $F/ph -- timeout 10 $XP sum $F/ph/one; rc=0; else timeout 10 $XP sum $F/ph/one; rc=$?; fi; [ $rc = 124 ] && echo "  TIMEOUT (deadlock): reader hung > 10 s"
    echo "  expected $($XP sum $F/store/one)"
    stat -c '  after:  blocks=%b mtime=%y ctime=%z' $F/ph/one
    grep -v READY $OUT/hsm.log | sed 's/^/    /' | cut -c1-230
    hsm_stop; pkill -9 -x xp 2>/dev/null; sleep 0.3
  done
  echo "== does the clone share extents with the CAS blob afterwards?"
  filefrag -v $F/store/one | sed -n '4p'; filefrag -v $F/ph/one | sed -n '4p'
}
s_fillbench() {
  echo "##### fill speed: pwrite vs FICLONE (all via event fd, unmark after fill, cold cache)"
  F=$D/fb; 
  mk() { # n size
    rm -rf $F; mkdir -p $F/store $F/ph; rnd $(( $1 * $2 )) $F/blob; split -b $2 -a 5 -d $F/blob $F/store/f; rm $F/blob
    for f in $F/store/*; do truncate -r $f $F/ph/$(basename $f); done; sync; echo 3 > /proc/sys/vm/drop_caches; }
  run() { # fill label
    hsm_start --fill $1 --evflags rdwr --writevia eventfd --root $F/ph --store $F/store --unmark --mark inode:$F/ph
    head -1 $OUT/hsm.log | sed 's/^/  /'
    echo -n "  $1: "; $XP readall $F/ph
    echo "  events logged: $(grep -c '^EVT' $OUT/hsm.log), fill errors: $(grep -c fillfail $OUT/hsm.log); avg handler us: $(grep fill- $OUT/hsm.log | sed 's/.*us=//' | awk '{s+=$1;n++} END{if(n)printf "%d", s/n}')"
    hsm_stop; }
  for spec in "10000 16384" "1000 1048576" "20 67108864" "1 1073741824"; do
    set -- $spec; echo "== $1 files x $2 bytes"
    mk $1 $2; run pwrite
    for f in $F/ph/*; do truncate -s 0 $f; truncate -r $F/store/$(basename $f) $f; done; sync; echo 3 > /proc/sys/vm/drop_caches
    run clone
    u=$(used); echo "  fs used now (clone left the placeholders sharing extents with store): $u"
  done
  rm -rf $F
}
s_warm() {
  echo "##### warm-cache: handler cost only (store blobs already in page cache) + 128 MiB chunk dedupe redo"
  F=$D/fb
  for spec in "1000 1048576" "1 1073741824"; do
    set -- $spec; echo "== $1 files x $2 bytes"
    for fill in pwrite clone; do
      rm -rf $F; mkdir -p $F/store $F/ph; rnd $(( $1 * $2 )) $F/blob; split -b $2 -a 5 -d $F/blob $F/store/f; rm $F/blob
      for f in $F/store/*; do truncate -r $f $F/ph/$(basename $f); done; sync; cat $F/store/* > /dev/null
      hsm_start --fill $fill --evflags rdwr --writevia eventfd --root $F/ph --store $F/store --unmark --mark inode:$F/ph
      echo -n "  $fill: "; $XP readall $F/ph
      echo "  handler avg us: $(grep fill- $OUT/hsm.log | sed 's/.*us=//' | awk '{s+=$1;n++} END{if(n)printf "%d", s/n}')"
      hsm_stop
    done
  done
  rm -rf $F
  mkdir -p $D/bg2; cd $D/bg2; rnd $((1<<30)) big; cp --reflink=never big big4; sync; cat big big4 > /dev/null
  echo "- 1 GiB, 128 MiB chunks, warm (2 GiB working set):"; $XP dedupefile big big4 128M
  cd $D; rm -rf bg2
  mkdir -p $D/inl; cd $D/inl; mkdir a b; rnd $((10000*1024)) blob; split -b 1024 -a 5 -d blob a/f; rm blob; cp -a --reflink=never a/. b/; sync
  u0=$(used); $XP dedupebulk a b; u1=$(used); echo "- 10k x 1 KiB inline: fs used before=$u0 after=$u1 (reclaimed $(( (u0-u1)>>10 )) KiB of ~10000 KiB)"
  cd $D; rm -rf inl
}
if [ $# -eq 0 ]; then set -- clone range events ino dedupe conc lease space fill; fi
for s in "$@"; do s_$s 2>&1; echo; done
