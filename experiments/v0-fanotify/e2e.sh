#!/bin/sh
# End-to-end test: one server, two clients (A, B) on the same box.
# Runs as root in a container (fanotify pre-content needs CAP_SYS_ADMIN).
#   BIN = leasefs binary, W = scratch dir on a real ext4/xfs (not tmpfs).
set -u
BIN=${BIN:-/opt/leasefs/leasefs}
W=${W:-/work}
PASS=0
FAIL=0

ok()   { PASS=$((PASS+1)); echo "PASS  $*"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL  $*"; }
check() { desc=$1; shift; if "$@"; then ok "$desc"; else bad "$desc"; fi; }
now_cs() { cut -d' ' -f1 /proc/uptime | tr -d .; }   # centiseconds
wait_for() { i=0; while [ $i -lt ${2:-60} ]; do sh -c "$1" && return 0; sleep 0.1; i=$((i+1)); done; return 1; }
blocks() { stat -c %b "$1"; }

rm -rf "$W"/store "$W"/a "$W"/b "$W"/logs
mkdir -p "$W"/store/src "$W"/a "$W"/b "$W"/logs
printf 'hello from the server\n' > "$W"/store/hello.txt
dd if=/dev/urandom of="$W"/store/big.bin bs=1M count=8 2>/dev/null
echo 'package main' > "$W"/store/src/main.go
BIGSUM=$(md5sum < "$W"/store/big.bin)

$BIN server --store "$W"/store --listen 127.0.0.1:7070 --grant-delay-ms 1500 > "$W"/logs/server.log 2>&1 &
sleep 0.3
$BIN client --dir "$W"/a --server 127.0.0.1:7070 --name A --cache-max 4M --flush-delay-ms 200 > "$W"/logs/a.log 2>&1 &
$BIN client --dir "$W"/b --server 127.0.0.1:7070 --name B --flush-delay-ms 200 > "$W"/logs/b.log 2>&1 &
wait_for "grep -q ready $W/logs/a.log && grep -q ready $W/logs/b.log" || { echo "clients did not start"; cat "$W"/logs/*.log; exit 1; }

$BIN watch "$W"/a > "$W"/logs/watch-a.txt &
$BIN watch "$W"/b > "$W"/logs/watch-b.txt &
sleep 0.3

echo "== 1. placeholders"
check "big.bin on A has the right size" [ "$(stat -c %s "$W"/a/big.bin)" = 8388608 ]
check "big.bin on A uses no disk blocks" [ "$(blocks "$W"/a/big.bin)" = 0 ]

echo "== 2. lazy fill on first read"
check "cat hello.txt on A returns server content" [ "$(cat "$W"/a/hello.txt)" = "hello from the server" ]
check "big.bin on A reads back identical (8 MiB)" [ "$(md5sum < "$W"/a/big.bin)" = "$BIGSUM" ]
cat "$W"/a/src/main.go > /dev/null
sleep 0.3
check "filling raised no IN_MODIFY on A" sh -c "! grep -E '^MODIFY.* (hello.txt|big.bin|src/main.go)$' $W/logs/watch-a.txt"

echo "== 3. write gate (server grant delay 1500ms)"
t0=$(now_cs); echo "line from A" >> "$W"/a/hello.txt; t1=$(now_cs)
echo "   first write on A took $(( (t1-t0)*10 ))ms"
check "first write blocked until the lease was granted (>=1400ms)" [ $((t1-t0)) -ge 140 ]
t0=$(now_cs); echo "second line from A" >> "$W"/a/hello.txt; t1=$(now_cs)
echo "   second write on A took $(( (t1-t0)*10 ))ms"
check "second write is local speed (lease cached, <300ms)" [ $((t1-t0)) -lt 30 ]

echo "== 4. remote change -> inotify on B"
check "B's watcher saw MODIFY hello.txt" wait_for "grep -q '^MODIFY hello.txt$' $W/logs/watch-b.txt" 50
check "B reads A's content" wait_for "[ \"\$(tail -n1 $W/b/hello.txt)\" = 'second line from A' ]" 50

echo "== 5. namespace ops on A -> inotify on B"
echo "new file" > "$W"/a/new.txt
check "B saw CREATE new.txt" wait_for "grep -q '^CREATE new.txt$' $W/logs/watch-b.txt" 50
check "B reads new.txt" wait_for "[ \"\$(cat $W/b/new.txt 2>/dev/null)\" = 'new file' ]" 50
mv "$W"/a/new.txt "$W"/a/renamed.txt
check "B saw MOVED_TO renamed.txt" wait_for "grep -q '^MOVED_TO renamed.txt$' $W/logs/watch-b.txt" 50
mkdir "$W"/a/dir1
check "B saw CREATE|ISDIR dir1" wait_for "grep -q '^CREATE|ISDIR dir1$' $W/logs/watch-b.txt" 50
echo "deep" > "$W"/a/dir1/deep.txt
check "B reads dir1/deep.txt" wait_for "[ \"\$(cat $W/b/dir1/deep.txt 2>/dev/null)\" = 'deep' ]" 50
rm "$W"/a/renamed.txt
check "B saw DELETE renamed.txt" wait_for "grep -q '^DELETE renamed.txt$' $W/logs/watch-b.txt" 50

echo "== 6. atomic save (write tmp + rename over) on A"
echo "package main // v2" > "$W"/a/src/.main.go.tmp && mv "$W"/a/src/.main.go.tmp "$W"/a/src/main.go
check "B reads the new src/main.go" wait_for "[ \"\$(cat $W/b/src/main.go)\" = 'package main // v2' ]" 50
check "server has no leftover temp file" wait_for "[ ! -e $W/store/src/.main.go.tmp ]" 30

echo "== 7. conflicting writer: B writes a file A holds the lease on (recall)"
t0=$(now_cs); echo "line from B" >> "$W"/b/hello.txt; t1=$(now_cs)
echo "   B's write took $(( (t1-t0)*10 ))ms (recall + grant)"
check "server recalled A's lease" grep -q 'recall hello.txt from client' "$W"/logs/server.log
check "store has all four lines" wait_for "[ \"\$(cat $W/store/hello.txt | wc -l)\" = 4 ]" 50
check "A sees B's line" wait_for "[ \"\$(tail -n1 $W/a/hello.txt)\" = 'line from B' ]" 50

echo "== 8. eviction on A (cache-max 4M; big.bin is 8M)"
check "big.bin evicted to a placeholder" wait_for "[ \$(stat -c %b $W/a/big.bin) = 0 ]" 50
check "size preserved after eviction" [ "$(stat -c %s "$W"/a/big.bin)" = 8388608 ]
check "eviction raised no IN_MODIFY for big.bin" sh -c "! grep -q '^MODIFY big.bin$' $W/logs/watch-a.txt"
check "re-read after eviction is identical" [ "$(md5sum < "$W"/a/big.bin)" = "$BIGSUM" ]

echo "== 9. open file is not evicted"
exec 3< "$W"/a/big.bin
sleep 2.5
check "big.bin still filled while held open" [ "$(blocks "$W"/a/big.bin)" != 0 ]
exec 3<&-

echo
echo "RESULT: $PASS passed, $FAIL failed"
echo "--- server.log";  cat "$W"/logs/server.log
echo "--- a.log";       cat "$W"/logs/a.log
echo "--- b.log";       cat "$W"/logs/b.log
echo "--- watch-b.txt"; cat "$W"/logs/watch-b.txt
chown -R "${HOST_UID:-1000}:${HOST_GID:-1000}" "$W" 2>/dev/null
[ "$FAIL" = 0 ]
