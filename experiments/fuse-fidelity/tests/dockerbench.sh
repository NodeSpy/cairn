#!/bin/bash
# dockerbench.sh <data-root> <label> [storage-driver=overlay2] [extra daemon.json fragment]
# Runs on lfs-srv as root. Prints "T <step> <seconds>" lines and PASS/FAIL checks.
ROOT=$1; LABEL=$2; DRV=${3:-overlay2}; EXTRA=${4:-}
PGDIR=${PGDIR:-$ROOT-pgdata}
t() { local n=$1; shift; local s=$(date +%s.%N); "$@" >/tmp/db-$LABEL-$n.log 2>&1; local rc=$?; local e=$(date +%s.%N); printf "T %-22s %7.2f s  rc=%d\n" "$n" "$(echo "$e - $s" | bc)" $rc; return $rc; }
ck() { local n=$1; shift; if "$@" >/tmp/db-$LABEL-$n.log 2>&1; then echo "PASS $n"; else echo "FAIL $n :: $(tail -c 300 /tmp/db-$LABEL-$n.log | tr '\n' ' ')"; fi; }

mkdir -p /etc/docker
systemctl stop docker docker.socket 2>/dev/null; sleep 1
rm -rf "$ROOT" "$PGDIR"; mkdir -p "$ROOT" "$PGDIR"
cat > /etc/docker/daemon.json <<JSON
{ "data-root": "$ROOT", "storage-driver": "$DRV", "features": {"containerd-snapshotter": false}$EXTRA }
JSON
dmesg -C
t daemon-start systemctl start docker || { journalctl -u docker -n 30 --no-pager | tail -20; exit 1; }
sleep 2
docker info 2>/dev/null | grep -E "Storage Driver|Backing Filesystem|Supports d_type|Using metacopy|Native Overlay Diff|userxattr|Docker Root Dir|Logging Driver" 
t pull-alpine   docker pull -q alpine:latest
t pull-debian   docker pull -q debian:bookworm-slim
t pull-postgres docker pull -q postgres:17
mkdir -p /tmp/dbuild-$LABEL && cd /tmp/dbuild-$LABEL
cat > hello.c <<C
#include <stdio.h>
int main(){puts("hello from multi-stage");return 0;}
C
cat > Dockerfile <<'D'
FROM alpine:latest AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY hello.c .
RUN gcc -O2 -o hello hello.c && for i in $(seq 1 2000); do echo $i > /src/f$i; done
FROM alpine:latest
COPY --from=build /src/hello /usr/local/bin/hello
COPY --from=build /src/f1 /f1
RUN chmod 755 /usr/local/bin/hello && ln /usr/local/bin/hello /usr/local/bin/hello2 && echo done > /marker
CMD ["hello"]
D
t build-multistage docker build -q -t ms-test .
ck run-built [ "$(docker run --rm ms-test)" = "hello from multi-stage" ]
t run-alpine-tools docker run --rm alpine:latest sh -c 'ls -la / >/dev/null && cat /etc/os-release >/dev/null && echo hi > /x && wc -c /x && mkdir -p /a/b && cp /bin/busybox /a/b/busybox && /a/b/busybox true && ln /a/b/busybox /a/b/busybox2 && ls -li /a/b && mv /a /c && find / -xdev -type f | wc -l && rm -f /c/b/busybox'
t run-debian-tools docker run --rm debian:bookworm-slim sh -c 'dpkg -l | wc -l; apt-get --version | head -1; tar czf /tmp/t.tgz /usr/share/doc 2>/dev/null; ls -la /tmp/t.tgz; grep -c . /etc/passwd; touch -d "2001-01-01" /tmp/x; stat -c %Y /tmp/x; chown 5:6 /tmp/x; stat -c %u:%g /tmp/x'
ck hardlink-alias-unlink docker run --rm alpine:latest sh -c 'echo a > /x; ln /x /y; ls -li /x /y >/dev/null; rm /x; cat /y'
docker rm -f longrun >/dev/null 2>&1
docker run -d --name longrun debian:bookworm-slim sleep 600 >/dev/null
t exec-tools docker exec longrun sh -c 'echo data > /tmp/e && cat /tmp/e && ls /usr/bin | wc -l && getent passwd root'
docker rm -f longrun >/dev/null 2>&1
# postgres with its data volume on the bind-mounted dir
docker rm -f pg >/dev/null 2>&1
chown 999:999 "$PGDIR"
t pg-init docker run -d --name pg -e POSTGRES_PASSWORD=x -v "$PGDIR":/var/lib/postgresql/data postgres:17
ready() { for i in $(seq 1 120); do docker exec pg pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1 && docker exec pg psql -U postgres -tAc 'select 1' >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
t pg-ready ready
t pg-insert docker exec pg psql -U postgres -c "create table t(id int primary key, v text); insert into t select g, md5(g::text) from generate_series(1,100000) g; checkpoint;"
ck pg-count-before [ "$(docker exec pg psql -U postgres -tAc 'select count(*) from t')" = 100000 ]
t pg-restart docker restart pg
t pg-ready2 ready
ck pg-count-after [ "$(docker exec pg psql -U postgres -tAc 'select count(*), sum(length(v)) from t')" = "100000|3200000" ]
t pg-update docker exec pg psql -U postgres -c "update t set v = upper(v) where id % 2 = 0" -c "checkpoint"
t pg-vacuum docker exec pg psql -U postgres -c "vacuum full t"
docker rm -f pg >/dev/null 2>&1
ck pg-pgdata-on-mount [ -f "$PGDIR/PG_VERSION" ]
t prune docker system prune -af --volumes
echo "-- dmesg"; dmesg | grep -iE "overlay|fuse|EIO|I/O error|BUG|WARN" | head -20
systemctl stop docker docker.socket 2>/dev/null
