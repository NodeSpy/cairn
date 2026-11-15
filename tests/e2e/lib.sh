#!/bin/bash
# NOTE: the VMs are shared. Only touch our own paths: /mnt/cairn, /etc/cairn/cairnd.toml, /cache/cairn*,
# /tmp/cairn*, server data /cache/cairn-server; kill processes only by our exact command line.
# Shared helpers for VM e2e. Sourced by run.sh. Runs on the devbox, drives the VMs over ssh.
# Provisioning uses the real cairn-server CLI and `cairnd --config` (client toml: server, creds_dir, cache_dir, [root.<name>] path).
set -u
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
SRV_IP=${SRV_IP:-$(awk '$1=="lfs-srv"{print $2}' "$ROOT/tests/hosts.txt")}
A_IP=${A_IP:-$(awk '$1=="lfs-a"{print $2}' "$ROOT/tests/hosts.txt")}
B_IP=${B_IP:-$(awk '$1=="lfs-b"{print $2}' "$ROOT/tests/hosts.txt")}
PORT=${PORT:-7420}
TARGET_DIR=${CARGO_TARGET_DIR:-/home/daniel/.cache/cairn-target/testkit}
MNT=/mnt/cairn
CACHE=/cache/cairn
SRVDATA=/cache/cairn-server
SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10"
ip_of() { case $1 in srv) echo $SRV_IP;; a) echo $A_IP;; b) echo $B_IP;; esac; }
vm() { local h=$1; shift; ssh $SSH_OPTS daniel@"$(ip_of $h)" "$@"; }
vms() { local h=$1; shift; vm $h "sudo bash -c $(printf '%q' "$*")"; }   # as root, quoting-safe

PASS=0; FAIL=0; FAILED=()
ok()   { PASS=$((PASS+1)); echo "  ok   $*"; }
bad()  { FAIL=$((FAIL+1)); FAILED+=("$*"); echo "  FAIL $*"; }
# check <desc> <cmd...> : cmd is run locally (use vm inside)
check() { local d=$1; shift; if "$@" >/dev/null 2>&1; then ok "$d"; else bad "$d"; fi; }
# eventually <timeout-s> <desc> <cmd...>
eventually() { local t=$1 d=$2; shift 2; local i=0; while [ $i -lt $((t*10)) ]; do eval "$*" >/dev/null 2>&1 && { ok "$d"; return 0; }; sleep 0.1; i=$((i+1)); done; bad "$d (timeout ${t}s)"; return 1; }

build() {
  (cd "$ROOT" && CARGO_TARGET_DIR=$TARGET_DIR cargo build --release -p cairn-server -p cairn-fuse) || return 1
}
deploy() {
  # srv: server binary only (lfs-srv may be busy with FUSE tests that use their own cairnd)
  scp $SSH_OPTS -q $TARGET_DIR/release/cairn-server daniel@"$(ip_of srv)":/tmp/cairn-server-e2e || return 1
  vms srv 'install -m755 /tmp/cairn-server-e2e /usr/local/bin/cairn-server'
  for h in a b; do
    scp $SSH_OPTS -q $TARGET_DIR/release/cairnd daniel@"$(ip_of $h)":/tmp/cairnd-e2e || return 1
    vms $h 'install -m755 /tmp/cairnd-e2e /usr/local/bin/cairnd'
  done
}

# ---- server ----
provision() {
  vms srv "pkill -9 -f '[c]airn-server serve --data $SRVDATA'; rm -rf $SRVDATA /tmp/creds-a /tmp/creds-b; mkdir -p $SRVDATA"
  vms srv "cairn-server init $SRVDATA" || return 1
  for h in a b; do vms srv "cairn-server host add lfs-$h --out /tmp/creds-$h --data $SRVDATA" || return 1; done
  vms srv "cairn-server root add projects --acl lfs-a:rw,lfs-b:rw --data $SRVDATA" || return 1
  for h in a b; do   # ship each client's creds dir (ca.pem host.pem host.key) and cairnd config
    vm srv "sudo tar -C /tmp/creds-$h -cf - ." | vm $h "sudo rm -rf /etc/cairn/creds; sudo mkdir -p /etc/cairn/creds; sudo tar -C /etc/cairn/creds -xf -; sudo chmod -R a+rX /etc/cairn"
    vm $h "sudo tee /etc/cairn/cairnd.toml >/dev/null" <<EOC
server = "$SRV_IP:$PORT"
creds_dir = "/etc/cairn/creds"
cache_dir = "$CACHE"
${CLIENT_TOML_EXTRA:-}
[root.projects]
path = "$MNT"
EOC
  done
}
start_server() { vms srv "nohup cairn-server serve --data $SRVDATA --listen 0.0.0.0:$PORT > /tmp/cairn-server.log 2>&1 < /dev/null &"; sleep 1; }
kill_server()  { vms srv "pkill -9 -f '[c]airn-server serve --data $SRVDATA'; true"; }
# ---- clients ----
mount_client() { # mount_client a|b [extra cairnd args]
  local h=$1; shift
  vms $h "rm -f /tmp/cairnd.log; mkdir -p $MNT $CACHE; chown daniel: $MNT; ${CAIRND_ENV:-} nohup cairnd --config /etc/cairn/cairnd.toml $* ${CAIRND_ARGS:-} > /tmp/cairnd.log 2>&1 < /dev/null &"
  local i=0; while [ $i -lt 150 ]; do vm $h mountpoint -q $MNT && return 0; sleep 0.1; i=$((i+1)); done
  echo "mount failed on $h"; vm $h tail -20 /tmp/cairnd.log; return 1
}
umount_client() { local h=$1; vms $h "fusermount3 -uz $MNT 2>/dev/null || umount -l $MNT 2>/dev/null; pkill -f '[c]airnd --config /etc/cairn/cairnd.toml'; true"; }
kill9_client()  { vms $1 "pkill -9 -f '[c]airnd --config /etc/cairn/cairnd.toml'; sleep 0.3; fusermount3 -uz $MNT 2>/dev/null || umount -l $MNT 2>/dev/null; true"; }
wipe_client()   { vms $1 "rm -rf $CACHE"; }

# ---- faults ----
# partition <vm>: cut that VM off from the server in both directions. The VMs' kernel has no
# netfilter/netem modules, so use `unreachable` host routes (on the client and on the server).
partition() { vms $1 "ip route replace unreachable $SRV_IP/32"; vms srv "ip route replace unreachable $(ip_of $1)/32"; }
heal() { vms $1 "ip route del unreachable $SRV_IP/32 2>/dev/null; true"; vms srv "ip route del unreachable $(ip_of $1)/32 2>/dev/null; true"; }
heal_all() { for h in a b; do heal $h; done; }

# Always remove faults and unmount, even on error/interrupt.
cleanup() { heal_all >/dev/null 2>&1; for h in a b; do umount_client $h >/dev/null 2>&1; done; kill_server >/dev/null 2>&1; }

fresh() { # fresh: clean server + clients mounted (FRESH_HOSTS="a" mounts only lfs-a; default "a b")
  local hs=${FRESH_HOSTS:-a b}
  for h in a b; do umount_client $h; done; kill_server; heal_all
  # leftovers of the previous scenario: its helper scripts, and its dirty data (a btrfs fsync of
  # the next scenario's op log would otherwise wait behind it)
  for h in $hs; do vms $h "pkill -f '[c]k.py|[f]s.py|[w]l.py|[b]ulk.py'; sync; true"; done
  provision && start_server && for h in $hs; do wipe_client $h; mount_client $h || return 1; done
  vm a "sudo chmod 777 $MNT" || return 1        # new roots are root:root 0755; let the test user write
  case " $hs " in *" b "*) ;; *) return 0;; esac
  local i=0; while [ $i -lt 100 ]; do [ "$(vm b "stat -c %a $MNT")" = 777 ] && return 0; sleep 0.2; i=$((i+1)); done
  echo "root mode did not propagate to b"; return 1
}
# one line per conflict record: "<resolution-kind-ish blob size>"; counts are what the scenarios use.
# (cairn-server has no `conflicts` subcommand; read the table directly, WAL-safe read-only)
conflict_log() { vms srv "sqlite3 -readonly file:$SRVDATA/meta.db?mode=ro 'select seq, host, length(data) from conflicts order by id'"; }
conflict_count() { conflict_log | wc -l; }

# server-side counters: changes = ops the server merged into its change feed, tombs = tombstones
srv_changes() { vms srv "sqlite3 -readonly file:$SRVDATA/meta.db?mode=ro 'select count(*) from changes'"; }
srv_tombs()   { vms srv "sqlite3 -readonly file:$SRVDATA/meta.db?mode=ro 'select count(*) from tombs'"; }
# wait for the (sysrq-rebooted) VM to answer ssh again
wait_ssh() { local i=0; sleep 8; while [ $i -lt 90 ]; do vm $1 true 2>/dev/null && return 0; sleep 2; i=$((i+1)); done; return 1; }
