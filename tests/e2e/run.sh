#!/bin/bash
# VM e2e: ./run.sh [--no-build] [scenario...]   (default: all)
# Scenarios: inotify concurrent conflicts offline kill_client kill_server kill9_oplog kill9_bulk powerloss folding checkpoint fsync_visible  (powerloss: run explicitly; with A_IP/B_IP swapped so the victim is lfs-b)
source "$(dirname "$0")/lib.sh"
trap 'cleanup' EXIT INT TERM

s_inotify() {
  echo "== remote inotify"
  fresh || { bad "fresh"; return; }
  vm b "mkdir -p $MNT/w; rm -f /tmp/ino.out; ( timeout 20 inotifywait -m -r -e create,modify,close_write,moved_from,moved_to,delete --format '%e %f' $MNT/w > /tmp/ino.out 2>&1 & )" ; sleep 1
  vm a "mkdir -p $MNT/w; echo hi > $MNT/w/f1; echo more >> $MNT/w/f1; mv $MNT/w/f1 $MNT/w/f2; rm $MNT/w/f2"
  sleep 3
  for ev in "CREATE f1" "CLOSE_WRITE,CLOSE f1" "MOVED_FROM f1" "MOVED_TO f2" "DELETE f2"; do
    check "lfs-b saw '$ev'" vm b "grep -q '$ev' /tmp/ino.out"
  done
  check "modify seen" vm b "grep -q MODIFY /tmp/ino.out"
}

s_concurrent() {
  echo "== concurrent same-dir creates"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/c"; eventually 10 "dir visible on b" vm b "test -d $MNT/c"
  vm a "for i in \$(seq 1 200); do echo a\$i > $MNT/c/a\$i; done" &
  vm b "for i in \$(seq 1 200); do echo b\$i > $MNT/c/b\$i; done" &
  wait
  eventually 30 "a sees 400 entries" 'test "$(vm a "ls $MNT/c | wc -l")" = 400'
  eventually 30 "b sees 400 entries" 'test "$(vm b "ls $MNT/c | wc -l")" = 400'
  check "contents intact" test "$(vm b "cat $MNT/c/a200 $MNT/c/b200" | tr '\n' ' ')" = "a200 b200 "
  check "no conflict entries" test "$(vm a "ls $MNT/c | grep -c conflict" || true)" = 0
}

s_conflicts() {
  echo "== conflict cases (partition-induced)"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/x; echo base > $MNT/x/mod; echo del > $MNT/x/del; echo mv > $MNT/x/mv; mkdir $MNT/x/dir"
  eventually 10 "synced to b" vm b "test -f $MNT/x/mv"
  vm b "ls -R $MNT/x >/dev/null"   # offline lookups need the directories listed first
  partition b; sleep 1
  # same name created differently on both: conflict copy
  vm a "echo from-a > $MNT/x/new"; vm b "echo from-b > $MNT/x/new"
  # delete (b, offline) vs modify (a): modify wins
  vm a "echo changed > $MNT/x/mod"; vm b "rm $MNT/x/mod"
  # two renames of same source: first wins
  vm a "mv $MNT/x/mv $MNT/x/mv-a"; vm b "mv $MNT/x/mv $MNT/x/mv-b"
  # create into dir deleted concurrently: dir recreated
  vm a "rmdir $MNT/x/dir"; vm b "echo z > $MNT/x/dir/z"
  heal b
  eventually 30 "converged: a and b list the same" 'test "$(vm a "cd $MNT/x && find . | sort")" = "$(vm b "cd $MNT/x && find . | sort")"'
  check "x/mod survived with a's content" test "$(vm b "cat $MNT/x/mod")" = changed
  check "new + conflict copy both exist" test "$(vm a "ls $MNT/x | grep -c '^new'")" -ge 2
  check "both contents preserved" test "$(vm a "cat $MNT/x/new*" | sort | tr '\n' ' ')" = "from-a from-b "
  check "exactly one of mv-a/mv-b" test "$(vm a "ls $MNT/x | grep -c '^mv-'")" = 1
  check "dir recreated with z" vm a "test -f $MNT/x/dir/z"
  check "conflict log non-empty" test "$(conflict_count)" -ge 1
}

s_offline() {
  echo "== offline queue / rebase"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/o; echo v1 > $MNT/o/f"; eventually 10 "synced" vm b "test -f $MNT/o/f"
  vm b "ls -R $MNT/o >/dev/null; cat $MNT/o/f >/dev/null"   # content must be resident to edit offline
  partition b; sleep 1
  vm b "echo offline-edit > $MNT/o/f; echo q > $MNT/o/newoff; mkdir $MNT/o/offdir; mv $MNT/o/newoff $MNT/o/offdir/"
  check "b sees own offline writes" test "$(vm b "cat $MNT/o/offdir/newoff")" = q
  vm a "echo online-edit > $MNT/o/f; echo r > $MNT/o/fromA"
  sleep 2; check "a does not see offline edits" vm a "test ! -e $MNT/o/offdir"
  heal b
  eventually 40 "a sees offline create" vm a "test -f $MNT/o/offdir/newoff"
  eventually 30 "b sees a's new file (rebase)" vm b "test -f $MNT/o/fromA"
  vm a "ls -la $MNT/o; for f in $MNT/o/f*; do echo \$f: \$(cat \$f); done" | sed "s/^/    diag: /"
  check "both edits of f preserved (conflict copy)" test "$(vm a "cat $MNT/o/f $MNT/o/f.conflict-*" | sort | tr '\n' ' ')" = "offline-edit online-edit "
  check "views identical" test "$(vm a "cd $MNT/o && find . | sort")" = "$(vm b "cd $MNT/o && find . | sort")"
  check "conflict log non-empty after offline edit" test "$(conflict_count)" -ge 1
}

s_kill_client() {
  echo "== kill -9 cairnd mid-write"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/k; echo stable > $MNT/k/stable"; eventually 10 "synced" vm b "test -f $MNT/k/stable"
  vm a "( dd if=/dev/urandom of=$MNT/k/big bs=1M count=200 2>/dev/null & ) ; sleep 0.3"
  kill9_client a
  mount_client a || { bad "remount after kill -9"; return; }
  check "a stable file intact after restart" test "$(vm a "cat $MNT/k/stable")" = stable
  check "a mount usable (create)" vm a "echo ok > $MNT/k/after"
  eventually 20 "b sees post-restart file" vm b "test -f $MNT/k/after"
  # whatever happened to 'big' it must be either absent or a consistent file, never a wedge
  check "listing big does not hang" vm a "timeout 10 ls -l $MNT/k"
  check "views identical" test "$(vm a "cd $MNT/k && ls | sort")" = "$(vm b "cd $MNT/k && ls | sort")"
}

s_kill_server() {
  echo "== kill -9 server and restart"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/s; for i in \$(seq 1 50); do echo \$i > $MNT/s/f\$i; done"
  eventually 20 "b sees all 50" 'test "$(vm b "ls $MNT/s | wc -l")" = 50'; vm b "cat $MNT/s/f1 >/dev/null"   # resident before the outage
  kill_server; sleep 1
  vm a "echo during > $MNT/s/during"                       # server down: queued locally
  check "b still reads old data with server down" test "$(vm b "cat $MNT/s/f1" || true)" = 1
  start_server
  eventually 40 "b sees file written during outage" vm b "test -f $MNT/s/during"
  eventually 30 "50 + 1 entries on both" 'test "$(vm a "ls $MNT/s | wc -l")" = 51 -a "$(vm b "ls $MNT/s | wc -l")" = 51'
  check "no stuck mount" vm a "timeout 10 ls $MNT/s"
}

s_kill9_oplog() {
  echo "== kill -9 cairnd under a closing workload: every acked op survives (op log)"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/w"
  vm a "cat > /tmp/wl.py" <<'PY'
import os, sys
d, log = sys.argv[1], sys.argv[2]
a = open(log, 'a', buffering=1)
i = 0
while True:
    p = '%s/f%d' % (d, i)
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
    os.write(fd, (b'data-%d-' % i) * 50)
    os.close(fd)
    if i % 7 == 0:
        os.rename(p, p + '.r')
    a.write('%d\n' % i)      # acked: create+write+close(+rename) all returned
    i += 1
PY
  vm a "rm -f /tmp/acked; ( nohup python3 /tmp/wl.py $MNT/w /tmp/acked >/dev/null 2>&1 & ); sleep 4"
  kill9_client a; vm a "pkill -f '[w]l.py'; true"
  local n; n=$(vm a "wc -l < /tmp/acked")
  echo "    acked ops before kill: $n"
  check "workload ran" test "${n:-0}" -gt 20
  mount_client a || { bad "remount after kill -9"; return; }
  vm a "python3 - <<'PY'
import os
n=0; bad=[]
for i in map(int, open('/tmp/acked').read().split()):
    p='$MNT/w/f%d' % i
    if i % 7 == 0: p += '.r'
    try:
        c=open(p,'rb').read()
    except Exception as e:
        bad.append((i,str(e))); continue
    if c != (b'data-%d-' % i) * 50: bad.append((i,'content'))
print(len(bad), bad[:5])
raise SystemExit(1 if bad else 0)
PY" | sed 's/^/    /'
  check "every acked file present with intact content" test "${PIPESTATUS[0]}" = 0
  check "mount usable" vm a "echo ok > $MNT/w/after"
  eventually 40 "b sees acked files and post-restart file" vm b "test -f $MNT/w/after -a -f $MNT/w/f1"
}

srv_pending_zero() { [ "$(vms srv "sqlite3 -readonly file:$SRVDATA/meta.db?mode=ro 'select count(*) from pending'")" = 0 ]; }
no_fetch_fail_a() { [ "$(vms a "grep -c 'did not upload' /tmp/cairnd.log")" = 0 ]; }
s_kill9_bulk() {
  echo "== kill -9 cairnd right after a bulk write (uploads still pending): all of it reaches the server, b reads it all"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/bulk"
  vm a "cat > /tmp/bulk.py" <<'PY'
import os, sys, hashlib
root, n = sys.argv[1], int(sys.argv[2])
def body(i):
    h = hashlib.sha256(b'bulk%d' % i).digest()
    return (h * (40 + i % 60)) + b'-%d' % i          # 1.3 - 3.3 KiB, unique per file
if sys.argv[3] == 'write':
    for i in range(n):
        d = '%s/d%d' % (root, i % 50)
        os.makedirs(d, exist_ok=True)
        with open('%s/f%d' % (d, i), 'wb') as f:      # close only: no fsync
            f.write(body(i))
else:
    bad = 0
    for i in range(n):
        try:
            c = open('%s/d%d/f%d' % (root, i % 50, i), 'rb').read()
        except Exception as e:
            bad += 1
            if bad <= 5: print('FAIL', i, e)
            continue
        if c != body(i):
            bad += 1
            if bad <= 5: print('FAIL content', i)
    print('bad', bad, 'of', n)
    sys.exit(1 if bad else 0)
PY
  vm a "python3 /tmp/bulk.py $MNT/bulk 6000 write"
  local pend; pend=$(vms srv "sqlite3 -readonly file:$SRVDATA/meta.db?mode=ro 'select count(*) from pending'")
  echo "    server pending contents at kill: ${pend:-?}"
  kill9_client a
  mount_client a || { bad "remount after kill -9"; return; }
  vm a "python3 /tmp/bulk.py $MNT/bulk 6000 read" | sed 's/^/    a: /'
  check "a reads every file back after the restart" test "${PIPESTATUS[0]}" = 0
  eventually 240 "server has no pending content left" srv_pending_zero
  vm a "cat /tmp/bulk.py" | vm b "cat > /tmp/bulk.py"
  eventually 120 "b lists the bulk tree" vm b "test -f $MNT/bulk/d49/f5999"
  vm b "python3 /tmp/bulk.py $MNT/bulk 6000 read" | sed 's/^/    b: /'
  check "b reads every file (no EIO, no missing upload)" test "${PIPESTATUS[0]}" = 0
  check "no failed fetch (\"did not upload\") on a" no_fetch_fail_a
}

s_powerloss() {
  echo "== power loss (sysrq b) on lfs-a: fsynced ops survive, state is a consistent prefix"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/p"
  vms a "echo 1 > /proc/sys/kernel/sysrq; rm -f /cache/acked; sync"   # provisioning files (creds) must survive the reset
  vm a "cat > /tmp/pl.py" <<'PY'
import os, sys
d = sys.argv[1]
ack = os.open('/cache/acked', os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
i = 0
while True:
    p = '%s/f%06d' % (d, i)
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
    os.write(fd, (b'v%d-' % i) * 100)
    if i % 25 == 0:
        os.fsync(fd)
        os.write(ack, b'%d\n' % i)
        os.fsync(ack)
    os.close(fd)
    i += 1
PY
  vm a "( nohup python3 /tmp/pl.py $MNT/p >/dev/null 2>&1 & ); sleep 5; sync -f /cache/acked"
  local n; n=$(vm a "wc -l < /cache/acked")
  echo "    fsynced acks: $n"
  check "workload ran" test "${n:-0}" -ge 3
  vm a "sudo sh -c 'echo b > /proc/sysrq-trigger'" >/dev/null 2>&1 &
  wait_ssh a || { bad "lfs-a did not come back"; return; }
  vm a "pgrep -f '[c]airnd --config' >/dev/null" && vm a "sudo pkill -9 -f '[c]airnd --config'"
  vms a "fusermount3 -uz $MNT 2>/dev/null; true"
  mount_client a || { bad "remount after power loss"; return; }
  vm a "python3 - <<'PY'
import os
names = sorted(os.listdir('$MNT/p'))
extra = [n for n in names if not (len(n) == 7 and n[0] == 'f' and n[1:].isdigit()) and n != 'after']
names = [n for n in names if n not in extra and n != 'after']
if extra: print('unexpected names (conflict copies?)', extra[:3])
idx = [int(n[1:]) for n in names]
acked = [int(x) for x in open('/cache/acked').read().split()]
holes = [i for i in range(len(idx)) if idx[i] != i]
miss = [i for i in acked if i not in set(idx)]
print('present', len(idx), 'acked', len(acked), 'holes', holes[:3], 'missing acked', miss[:3])
bad = []
for i in acked:
    if i in set(idx) and open('$MNT/p/f%06d' % i, 'rb').read() != (b'v%d-' % i) * 100: bad.append(i)
print('bad content', bad[:3])
for i in bad[:2]:
    p = '$MNT/p/f%06d' % i
    c = open(p, 'rb').read()
    print('  f%d: size %d head %r ino %d' % (i, len(c), c[:24], os.stat(p).st_ino))
raise SystemExit(1 if (holes or miss or bad or extra) else 0)
PY" | sed 's/^/    /'
  check "acked files present, intact, names are a gap-free prefix" test "${PIPESTATUS[0]}" = 0
  check "mount usable after power loss" vm a "echo ok > $MNT/p/after"
  eventually 60 "b sees post-recovery file" vm b "test -f $MNT/p/after" || {
    echo "    a count: $(vm a "ls $MNT/p | wc -l") b count: $(vm b "ls $MNT/p | wc -l") a has after: $(vm a "ls $MNT/p/after 2>&1")"
    vm a "pkill -USR1 -f '[c]airnd --config'; sleep 1; true"; vms a "pkill -USR1 -f '[c]airnd --config'; sleep 1; tail -40 /tmp/cairnd.log" | sed 's/^/    a-log: /'
    echo "    server changes: $(srv_changes)"; vm srv "tail -15 /tmp/cairn-server.log" | sed "s/^/    srv-log: /"; sleep 20; echo "    b after 20s more: $(vm b "ls $MNT/p/after 2>&1")"; }
}

s_folding() {
  echo "== folding: temp files never reach the server, atomic save = one version"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/fold; echo base > $MNT/fold/doc"; eventually 10 "synced" vm b "test -f $MNT/fold/doc"
  vm a "ls -R $MNT/fold > /dev/null"
  sleep 2
  local c0 t0 c1 c2; c0=$(srv_changes); t0=$(srv_tombs)
  kill_server; sleep 2   # (a partition would leave one batch in flight, which cannot fold)
  vm a "for i in \$(seq 1 100); do echo tmp\$i > $MNT/fold/.tmp\$i; rm $MNT/fold/.tmp\$i; done; mkdir $MNT/fold/bd; echo x > $MNT/fold/bd/y; rm -r $MNT/fold/bd"
  start_server; sleep 10
  c1=$(srv_changes)
  echo "    create/write/delete x100 + dir tree: server changes +$((c1-c0)), tombs +$(( $(srv_tombs)-t0 ))"
  check "temp files and temp dir never reached the server" test "$c1" = "$c0"
  check "no tombstones for them" test "$(srv_tombs)" = "$t0"
  kill_server; sleep 2
  vm a "for i in 1 2 3 4 5; do echo save\$i > $MNT/fold/.doc.tmp; mv $MNT/fold/.doc.tmp $MNT/fold/doc; done; for i in 1 2 3; do echo new\$i > $MNT/fold/.n.tmp; mv $MNT/fold/.n.tmp $MNT/fold/newdoc; done"
  start_server
  eventually 40 "b sees the last atomic-save content" 'test "$(vm b "cat $MNT/fold/doc")" = save5 -a "$(vm b "cat $MNT/fold/newdoc")" = new3'
  c2=$(srv_changes)
  echo "    8 atomic saves: server changes +$((c2-c1))"
  check "8 atomic saves of 2 files cost <= 5 server changes (one rename+content, one create+content per file)" test "$((c2-c1))" -le 5
  check "no leftover temp names" test "$(vm b "ls -A $MNT/fold | grep -c tmp" || true)" = 0
}

s_checkpoint() {
  echo "== checkpoints: a long-open file is published; only changed ranges are rehashed"
  export CLIENT_TOML_EXTRA=$'checkpoint_secs = 3\ncheckpoint_publish_secs = 14\ncheckpoint_scan_ms = 500'
  export CAIRND_ARGS="--stats-file /tmp/a.stats" CAIRND_ENV="RUST_LOG=cairn_client=debug"
  fresh || { bad "fresh"; unset CLIENT_TOML_EXTRA CAIRND_ARGS CAIRND_ENV; return; }
  unset CLIENT_TOML_EXTRA CAIRND_ARGS CAIRND_ENV
  vm a "mkdir -p $MNT/ck; rm -f /tmp/ck.sha"
  vm a "cat > /tmp/ck.py" <<'PY'
import os, time, hashlib
fd = os.open('/mnt/cairn/ck/big', os.O_CREAT | os.O_RDWR, 0o644)
data = bytearray(os.urandom(32 << 20))
os.write(fd, bytes(data))
time.sleep(7)
data[16 << 20:(16 << 20) + 4096] = os.urandom(4096)
os.pwrite(fd, bytes(data[16 << 20:(16 << 20) + 4096]), 16 << 20)
open('/tmp/ck.sha', 'w').write(hashlib.sha256(data).hexdigest())
time.sleep(70)          # held open: no close, no fsync
os.close(fd)
PY
  vm a "( nohup python3 /tmp/ck.py >/tmp/ck.log 2>&1 & ); sleep 2; sync"
  sleep 6; vm a "sync"
  sleep 6
  vms a "pkill -USR1 -f '[c]airnd --config'; sleep 1"
  vm a "grep '^CKPT_' /tmp/a.stats" | sed 's/^/    /'
  vm a "grep -E 'snapshot of|checkpoint|ckpt scan' /tmp/cairnd.log | head -24" | sed 's/^/    log: /'
  local reh tot snaps
  reh=$(vm a "awk '\$1==\"CKPT_rehashed_bytes\"{print \$2}' /tmp/a.stats"); tot=$(vm a "awk '\$1==\"CKPT_total_bytes\"{print \$2}' /tmp/a.stats")
  snaps=$(vm a "awk '\$1==\"CKPT_snapshots\"{print \$2}' /tmp/a.stats")
  check "at least two checkpoints taken while open" test "${snaps:-0}" -ge 2
  check "FIEMAP diff: rehashed bytes < 75% of the bytes checkpointed ($reh of $tot)" test "${tot:-0}" -gt 0 -a "$((${reh:-0}*100/${tot:-1}))" -lt 75
  local want; want=$(vm a "cat /tmp/ck.sha"); date +%T
  eventually 30 "b sees the published checkpoint of the still-open file" 'test "$(vm b "sha256sum < $MNT/ck/big | cut -d\  -f1")" = "$want"'
  vm a "pgrep -af ck.py; date +%T"
  check "writer still open on a (no close happened)" vm a "pgrep -f '[c]k.py'"
}

s_fsync_visible() {
  echo "== a file saved by fsync + rename, never closed, is visible on the other client"
  fresh || { bad "fresh"; return; }
  vm a "mkdir -p $MNT/fs; echo old > $MNT/fs/doc"; eventually 10 "synced" vm b "test -f $MNT/fs/doc"
  vm a "cat > /tmp/fs.py" <<'PY'
import os, time
def log(m): open('/tmp/fs.log', 'a').write('%.3f %s\n' % (time.time(), m))
log('start')
fd = os.open('/mnt/cairn/fs/.doc.swp', os.O_CREAT | os.O_WRONLY, 0o644)
log('open')
os.write(fd, b'saved-by-fsync\n')
os.fsync(fd)
log('fsync')
os.rename('/mnt/cairn/fs/.doc.swp', '/mnt/cairn/fs/doc')
log('rename')
time.sleep(20)       # editor keeps the descriptor open
os.close(fd)
log('close')
PY
  vm a "rm -f /tmp/fs.log; ( nohup python3 /tmp/fs.py >/dev/null 2>/tmp/fs.err & )"
  eventually 12 "b sees saved-by-fsync while a still holds the fd" 'test "$(vm b "cat $MNT/fs/doc")" = saved-by-fsync'
  check "a's process is still running (fd open)" vm a "pgrep -f '[f]s.py'"
  vms srv "pgrep -af 'cairn-server serve'" 2>&1 | sed 's/^/    dbg srv: /'
  vm a "mount | grep -c cairn; pgrep -af 'cairnd --config'; ls -la $MNT; cat /tmp/fs.err /tmp/fs.log; date +%s.%N; ls -la $MNT/fs; pgrep -af python; grep -av republishing /tmp/cairnd.log | tail -40 | cut -c1-300" 2>&1 | sed 's/^/    dbg: /'
}

BUILD=1; [ "${1:-}" = "--no-build" ] && { BUILD=0; shift; }
[ $BUILD = 1 ] && { build || { echo "build failed"; exit 2; }; }
deploy || { echo "deploy failed"; exit 2; }
SC=("$@"); [ ${#SC[@]} = 0 ] && SC=(inotify concurrent conflicts offline kill_client kill_server kill9_oplog kill9_bulk folding checkpoint fsync_visible)  # powerloss is run separately (it hard-resets the VM)
for s in "${SC[@]}"; do "s_$s"; done
echo; echo "passed=$PASS failed=$FAIL"; for f in "${FAILED[@]:-}"; do [ -n "$f" ] && echo "  - $f"; done
[ $FAIL = 0 ]
