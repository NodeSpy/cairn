#!/bin/bash
# probe2.sh <bin> <flags...>: trace of individual syscall sequences
BIN=$1; shift
sudo mkdir -p /mnt/ram/pb; sudo chown daniel: /mnt/ram/pb
sudo sh -c "$BIN --backing /mnt/ram/pb --mount /mnt/lfs $* --trace > /tmp/trace.log 2>&1 &"
sleep 0.5; mkdir -p /mnt/lfs/d; : > /tmp/trace.log
t() { echo "### $1" >> /tmp/trace.log; python3 -c "
import os
$2
"; sleep 0.1; }
t "create,close,stat" 'fd=os.open("/mnt/lfs/d/a",os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o644); os.close(fd); os.stat("/mnt/lfs/d/a")'
t "create,stat,close" 'fd=os.open("/mnt/lfs/d/b",os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o644); os.stat("/mnt/lfs/d/b"); os.close(fd)'
t "create,write,stat,close" 'fd=os.open("/mnt/lfs/d/c",os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o644); os.write(fd,b"x"*100); os.stat("/mnt/lfs/d/c"); os.stat("/mnt/lfs/d/c"); os.close(fd)'
t "create,write,close,stat,stat" 'fd=os.open("/mnt/lfs/d/e",os.O_CREAT|os.O_WRONLY|os.O_EXCL,0o644); os.write(fd,b"x"*100); os.close(fd); os.stat("/mnt/lfs/d/e"); os.stat("/mnt/lfs/d/e")'
t "open,read,close,stat" 'fd=os.open("/mnt/lfs/d/e",os.O_RDONLY); os.read(fd,100); os.close(fd); os.stat("/mnt/lfs/d/e")'
t "open rw,write,close,stat" 'fd=os.open("/mnt/lfs/d/e",os.O_RDWR); os.write(fd,b"y"); os.close(fd); os.stat("/mnt/lfs/d/e")'
t "chmod,stat" 'os.chmod("/mnt/lfs/d/e",0o600); os.stat("/mnt/lfs/d/e")'
t "utime,stat" 'os.utime("/mnt/lfs/d/e",(1,1)); os.stat("/mnt/lfs/d/e")'
t "truncate,stat" 'os.truncate("/mnt/lfs/d/e",5); os.stat("/mnt/lfs/d/e")'
t "mkdir,stat" 'os.mkdir("/mnt/lfs/d/dd"); os.stat("/mnt/lfs/d/dd"); os.stat("/mnt/lfs/d")'
t "unlink,stat dir" 'os.unlink("/mnt/lfs/d/a"); os.stat("/mnt/lfs/d")'
t "rename,stat" 'os.rename("/mnt/lfs/d/b","/mnt/lfs/d/b2"); os.stat("/mnt/lfs/d/b2")'
t "listdir,stat 3" 'import os; [os.stat("/mnt/lfs/d/"+n) for n in os.listdir("/mnt/lfs/d")]'
sudo umount /mnt/lfs; sleep 0.3; grep -E "^(###|T |[A-Z_(a-z)]+\s[0-9]+$)" /tmp/trace.log; sudo rm -rf /mnt/ram/pb
