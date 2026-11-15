#!/bin/bash
# ovl.sh <lowerparent> <upperparent> <mountopts> : overlayfs exercise. Run as root on the VM.
# lower tree lives under <lowerparent>/l, upper+work under <upperparent>/{u,w}, merged at /mnt/ovl-m.
LP=$1; UP=$2; OPTS=${3:-}
M=/mnt/ovl-m
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "PASS $1"; }
bad() { FAIL=$((FAIL+1)); echo "FAIL $1 :: $2"; }
chk() { local name=$1; shift; if "$@" >/tmp/ovl.out 2>&1; then ok "$name"; else bad "$name" "$(head -c 300 /tmp/ovl.out | tr '\n' ' ')"; fi; }
umount $M 2>/dev/null; rm -rf $LP/l $UP/u $UP/w; mkdir -p $LP/l $UP/u $UP/w $M
cd $LP/l
echo base > a; mkdir dir dir/sub; echo x > dir/f; echo y > dir/sub/g; ln a hard; setfattr -n user.k -v v a
cp /usr/bin/true exe; chmod 755 exe; mkdir ren; echo r > ren/f; ln -s a sl; mkfifo ff; touch -d '2001-02-03 04:05:06' tm tm2; chown 1234:5678 tm
python3 - <<PY
open('mm','wb').write(b'0123456789'*1000)
PY
cd /
dmesg -C
mount -t overlay overlay -o lowerdir=$LP/l,upperdir=$UP/u,workdir=$UP/w$OPTS $M || { echo "MOUNT FAILED"; dmesg | tail -20; exit 1; }
echo "mounted: $(grep ' /mnt/ovl-m ' /proc/mounts)"
cd $M
chk create       bash -c 'echo new > n; [ "$(cat n)" = new ] && [ -f '$UP'/u/n ]'
chk read-lower    bash -c '[ "$(cat a)" = base ] && [ ! -e '$UP'/u/a ]'
chk copyup-write  bash -c 'echo more >> a; [ "$(cat a)" = "base
more" ] && [ -f '$UP'/u/a ] && [ "$(cat '$LP'/l/a)" = base ]'
chk copyup-xattr  bash -c '[ "$(getfattr -n user.k --only-values a)" = v ] && [ "$(getfattr -n user.k --only-values '$UP'/u/a)" = v ]'
chk copyup-hard   bash -c 'echo hh >> hard; [ "$(cat hard)" = "$(cat a)" ] || echo "hardlink not shared after copyup (expected without index=on)"; true'
chk copyup-times  bash -c 'echo z >> tm; true; [ "$(stat -c %u:%g tm)" = 1234:5678 ]'
chk copyup-mtime-preserved bash -c 'chmod 600 tm2; [ "$(stat -c %Y tm2)" = 981173106 ] && [ "$(stat -c %Y '$UP'/u/tm2)" = 981173106 ]'
chk whiteout      bash -c 'rm dir/f; [ ! -e dir/f ] && [ -c '$UP'/u/dir/f ] && [ "$(stat -c %t:%T '$UP'/u/dir/f)" = 0:0 ] && ls dir | grep -qv "^f$"'
chk whiteout-file bash -c 'rm sl; [ ! -e sl ] && [ -c '$UP'/u/sl ]'
chk readdir-d_type bash -c 'ls -la dir > /dev/null; find . -type c | head -1 | grep -q . ; [ "$(find dir -maxdepth 1 -type c | wc -l)" = 0 ]'
chk rename-dir    bash -c 'mv ren ren2 && [ -f ren2/f ] && [ ! -e ren ] && [ "$(cat ren2/f)" = r ] && ls ren2 | grep -q f'
chk redirect-xattr bash -c 'getfattr -d -m trusted.overlay '$UP'/u/ren2 | grep -q redirect || { echo "no redirect xattr"; getfattr -d -m - '$UP'/u/ren2; false; }'
chk opaque        bash -c 'rm -rf dir; mkdir dir; echo fresh > dir/new; [ "$(ls dir)" = new ] && getfattr -d -m trusted.overlay '$UP'/u/dir | grep -q opaque'
chk hardlink-new  bash -c 'echo l > l1; ln l1 l2; [ "$(stat -c %h l1)" = 2 ] && [ "$(stat -c %i l1)" = "$(stat -c %i l2)" ]'
chk xattr-user    bash -c 'echo q > xa; setfattr -n user.a -v 1 xa; [ "$(getfattr -n user.a --only-values xa)" = 1 ] && setfattr -x user.a xa'
chk xattr-trusted-hidden bash -c 'setfattr -n trusted.foo -v 1 xa; [ "$(getfattr -n trusted.foo --only-values xa)" = 1 ]; getfattr -d -m - xa | grep -q "trusted.overlay" && false || true'
chk mknod         bash -c 'mknod nodc c 1 3 && [ -c nodc ] && mkfifo nodp && [ -p nodp ] && echo ok'
chk mmap-lower    python3 -c "
import mmap
f=open('mm','r+b'); m=mmap.mmap(f.fileno(),0); assert m[:10]==b'0123456789'; m[0:4]=b'ABCD'; m.flush(); m.close(); f.close()
assert open('mm','rb').read(10)==b'ABCD456789'
assert open('$LP/l/mm','rb').read(10)==b'0123456789'"
chk exec-lower    ./exe
chk exec-copiedup bash -c 'echo >> exe; ./exe || true; cp exe exe2; ./exe2'
chk exec-new      bash -c 'cp /usr/bin/true nx && ./nx'
chk chown-copyup  bash -c 'chown 4321:8765 dir/new && [ "$(stat -c %u:%g dir/new)" = 4321:8765 ]'
chk symlink       bash -c 'ln -s n sl2; [ "$(readlink sl2)" = n ] && [ "$(cat sl2)" = new ]'
chk sparse-seek   bash -c 'truncate -s 1M sp; echo x | dd of=sp bs=1 seek=500000 conv=notrunc 2>/dev/null; [ "$(stat -c %s sp)" = 1048576 ]'
chk rename-over   bash -c 'echo 1 > r1; echo 2 > r2; mv -f r1 r2; [ "$(cat r2)" = 1 ]'
chk rename-noreplace bash -c 'echo 1 > ra; echo 2 > rb; python3 - <<PY
import ctypes,os
libc=ctypes.CDLL(None,use_errno=True)
r=libc.renameat2(-100,b"ra",-100,b"rb",1)
assert r==-1 and ctypes.get_errno()==17, (r,ctypes.get_errno())
PY'
chk rm-rf-all     bash -c 'rm -rf dir ren2 n a hard l1 l2 xa; true'
cd /
umount $M && ok umount || bad umount ""
chk fsck-rerun-mount  mount -t overlay overlay -o lowerdir=$LP/l,upperdir=$UP/u,workdir=$UP/w$OPTS $M
umount $M
echo "== dmesg"; dmesg | grep -i "overlay" 
echo "RESULT pass=$PASS fail=$FAIL"
