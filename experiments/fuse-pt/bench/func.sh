#!/bin/bash
# func.sh <tune>: functional smoke test of the mount
T=$1
sudo mkdir -p /mnt/ram/fb; sudo chown daniel: /mnt/ram/fb
sudo sh -c "/tmp/fuse-pt --backing /mnt/ram/fb --mount /mnt/lfs --passthrough --tune $T --stats-file /tmp/f.tsv >/tmp/func.log 2>&1 &"
sleep 0.5; cd /mnt/lfs; set -e
mkdir -p a/b; for i in $(seq 1 300); do echo "hello $i" > a/f$i; done
[ "$(ls a | wc -l)" = 301 ]; [ "$(cat a/f77)" = "hello 77" ]
head -c 5000000 /dev/urandom > big; cp big big2; cmp big big2
echo more >> a/f5; [ "$(cat a/f5)" = "hello 5
more" ]
mv a/f6 a/b/g6; [ "$(cat a/b/g6)" = "hello 6" ]; mv a c; [ "$(cat c/b/g6)" = "hello 6" ]; [ -f c/f200 ]
ln -s c/f1 sl; [ "$(cat sl)" = "hello 1" ]
touch -d '2020-01-01' c/f9; [ "$(stat -c %Y c/f9)" = 1577836800 ]
chmod 600 c/f9; [ "$(stat -c %a c/f9)" = 600 ]
ls c | wc -l >/dev/null; touch c/newf; [ -f c/newf ]; ls c | grep -q newf; rm c/newf; ! ls c | grep -q newf
python3 -c "
import os
fd=os.open('c/x',os.O_CREAT|os.O_EXCL|os.O_RDWR,0o644); os.write(fd,b'abc'); os.fsync(fd); os.lseek(fd,0,0); assert os.read(fd,3)==b'abc'; os.close(fd)
try: os.open('c/x',os.O_CREAT|os.O_EXCL|os.O_RDWR); assert 0
except FileExistsError: pass
"
[ "$(stat -c %U c/x)" = daniel ]
rm -rf c; [ ! -e c ]
cd /; sudo umount /mnt/lfs; sleep 0.3; sudo rm -rf /mnt/ram/fb; echo "OK $T"
