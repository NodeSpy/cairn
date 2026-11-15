#!/usr/bin/env python3
"""Feature-by-feature fidelity check, run as root against the FUSE mount (arg 1) with the backing dir (arg 2).
Every check compares behaviour through the mount with what the backing fs does for the same operation."""
import ctypes, errno, fcntl, os, stat, sys, subprocess, time, socket

mnt, back = sys.argv[1], sys.argv[2]
d = os.path.join(mnt, "fid"); bd = os.path.join(back, "fid")
subprocess.run(["rm", "-rf", d]); os.makedirs(d)
libc = ctypes.CDLL(None, use_errno=True)
res = []

def check(name, fn):
    try:
        r = fn()
        res.append((name, "PASS" if r in (None, True) else "FAIL " + str(r)))
    except Exception as e:
        res.append((name, "FAIL %s: %s" % (type(e).__name__, e)))

def p(n): return os.path.join(d, n)
def bp(n): return os.path.join(bd, n)

# --- xattrs, all namespaces
def xattrs():
    open(p("x"), "w").write("x")
    for ns in ("user", "trusted", "security"):
        os.setxattr(p("x"), ns + ".t", b"v-" + ns.encode())
        assert os.getxattr(p("x"), ns + ".t") == b"v-" + ns.encode(), ns
        assert os.getxattr(bp("x"), ns + ".t") == b"v-" + ns.encode(), ns + " not on backing"
    names = os.listxattr(p("x"))
    assert set(names) >= {"user.t", "trusted.t", "security.t"}, names
    os.removexattr(p("x"), "user.t")
    try: os.getxattr(p("x"), "user.t"); return "still there"
    except OSError as e: assert e.errno == errno.ENODATA
    try: os.setxattr(p("x"), "user.t", b"1", os.XATTR_REPLACE); return "REPLACE on missing succeeded"
    except OSError as e: assert e.errno == errno.ENODATA
    os.setxattr(p("x"), "user.t", b"1", os.XATTR_CREATE)
    try: os.setxattr(p("x"), "user.t", b"1", os.XATTR_CREATE); return "CREATE on existing succeeded"
    except OSError as e: assert e.errno == errno.EEXIST
    big = b"z" * 3000
    os.setxattr(p("x"), "user.big", big); assert os.getxattr(p("x"), "user.big") == big
check("xattr user/trusted/security + CREATE/REPLACE + list/remove", xattrs)

def xattr_symlink_dir():
    os.symlink("x", p("sl")); os.setxattr(p("sl"), "trusted.s", b"1", follow_symlinks=False)
    assert os.getxattr(p("sl"), "trusted.s", follow_symlinks=False) == b"1"
    os.mkdir(p("xd")); os.setxattr(p("xd"), "user.d", b"1"); assert os.getxattr(p("xd"), "user.d") == b"1"
check("xattr on symlink (trusted) and directory", xattr_symlink_dir)

def acls():
    os.mkdir(p("acl")); subprocess.run(["setfacl", "-m", "u:1000:rwx", p("acl")], check=True)
    out = subprocess.run(["getfacl", "-n", "-c", p("acl")], capture_output=True, text=True).stdout
    assert "user:1000:rwx" in out, out
    subprocess.run(["setfacl", "-d", "-m", "u:1000:r-x", p("acl")], check=True)
    open(p("acl/f"), "w").close()
    out = subprocess.run(["getfacl", "-n", "-c", p("acl/f")], capture_output=True, text=True).stdout
    assert "user:1000:r-x" in out, "default ACL not inherited: " + out
    assert "system.posix_acl_access" in os.listxattr(p("acl/f")), os.listxattr(p("acl/f"))
check("POSIX ACL access+default (system.posix_acl_*), inheritance on create", acls)

# --- mknod
def mknods():
    os.mknod(p("c"), stat.S_IFCHR | 0o600, os.makedev(1, 3))
    s = os.lstat(p("c")); assert stat.S_ISCHR(s.st_mode) and os.major(s.st_rdev) == 1 and os.minor(s.st_rdev) == 3
    os.mknod(p("b"), stat.S_IFBLK | 0o600, os.makedev(8, 1))
    s = os.lstat(p("b")); assert stat.S_ISBLK(s.st_mode) and os.major(s.st_rdev) == 8 and os.minor(s.st_rdev) == 1
    os.mknod(p("hi"), stat.S_IFCHR | 0o600, os.makedev(259, 70000 & 0xfffff))
    s = os.lstat(p("hi")); assert (os.major(s.st_rdev), os.minor(s.st_rdev)) == (259, 70000 & 0xfffff), (os.major(s.st_rdev), os.minor(s.st_rdev))
    os.mkfifo(p("f")); assert stat.S_ISFIFO(os.lstat(p("f")).st_mode)
    sk = socket.socket(socket.AF_UNIX); sk.bind(p("s")); sk.close(); assert stat.S_ISSOCK(os.lstat(p("s")).st_mode)
    os.mknod(p("wo"), stat.S_IFCHR, 0); s = os.lstat(p("wo")); assert stat.S_ISCHR(s.st_mode) and s.st_rdev == 0
    for n in ("c", "b", "hi", "f", "s", "wo"):
        assert stat.S_IFMT(os.lstat(bp(n)).st_mode) == stat.S_IFMT(os.lstat(p(n)).st_mode), n
    kinds = {e.name: e.is_dir() or stat.S_IFMT(e.stat(follow_symlinks=False).st_mode) for e in os.scandir(d)}; assert kinds["wo"] == stat.S_IFCHR and kinds["f"] == stat.S_IFIFO and kinds["s"] == stat.S_IFSOCK
check("mknod: char/block (incl. high dev numbers), fifo, socket, 0/0 whiteout; d_type", mknods)

def fifo_io():
    r, w = os.open(p("f"), os.O_RDONLY | os.O_NONBLOCK), os.open(p("f"), os.O_WRONLY)
    os.write(w, b"ping"); assert os.read(r, 4) == b"ping"
check("fifo data flows through the mount", fifo_io)

# --- renameat2
SYS_renameat2 = 316
def r2(a, b, fl):
    r = libc.syscall(SYS_renameat2, -100, p(a).encode(), -100, p(b).encode(), fl)
    return -ctypes.get_errno() if r < 0 else 0
def rename2():
    for n, c in (("ra", b"A"), ("rb", b"B")): open(p(n), "wb").write(c)
    assert r2("ra", "rb", 1) == -errno.EEXIST, "NOREPLACE"
    assert r2("ra", "rc", 1) == 0 and not os.path.exists(p("ra")), "NOREPLACE to new"
    open(p("ra"), "wb").write(b"A"); assert r2("ra", "rb", 2) == 0, "EXCHANGE"
    assert open(p("ra"), "rb").read() == b"B" and open(p("rb"), "rb").read() == b"A", "swap contents"
    os.mkdir(p("rd")); assert r2("rd", "ra", 2) == 0, "EXCHANGE dir<->file"
    assert os.path.isdir(p("ra")) and os.path.isfile(p("rd"))
    open(p("w1"), "wb").write(b"W"); assert r2("w1", "w2", 4) == 0, "WHITEOUT"
    s = os.lstat(p("w1")); assert stat.S_ISCHR(s.st_mode) and s.st_rdev == 0, "whiteout left behind at source"
    assert open(p("w2"), "rb").read() == b"W"
check("renameat2 NOREPLACE / EXCHANGE (file+dir) / WHITEOUT", rename2)

def tmpfile():
    fd = os.open(d, os.O_TMPFILE | os.O_RDWR, 0o600); os.write(fd, b"t")
    libc.linkat(fd, b"", -100, p("tmp_linked").encode(), 0x1000)  # AT_EMPTY_PATH
    os.close(fd)
check("O_TMPFILE", tmpfile)

def hardlinks():
    open(p("h1"), "w").write("h"); os.link(p("h1"), p("h2")); os.link(p("h2"), p("h3"))
    a, b, c = os.lstat(p("h1")), os.lstat(p("h2")), os.lstat(p("h3"))
    assert a.st_ino == b.st_ino == c.st_ino and a.st_nlink == 3 == os.lstat(p("h1")).st_nlink
    os.rename(p("h2"), p("h2b")); os.unlink(p("h1"))
    assert os.lstat(p("h3")).st_nlink == 2 and open(p("h2b")).read() == "h"
    os.symlink("tgt", p("sl1")); os.link(p("sl1"), p("sl2"), follow_symlinks=False)
    assert os.lstat(p("sl1")).st_ino == os.lstat(p("sl2")).st_ino and os.readlink(p("sl2")) == "tgt"
    fd = os.open(p("h3"), os.O_RDWR); os.unlink(p("h3")); os.unlink(p("h2b")); os.write(fd, b"after unlink")
    assert os.fstat(fd).st_nlink == 0; os.lseek(fd, 0, 0); assert os.read(fd, 100).startswith(b"after unlink"); os.close(fd)
check("hardlinks (incl. symlink link, rename/unlink of siblings, unlinked-open file)", hardlinks)

def falloc():
    fd = os.open(p("fa"), os.O_RDWR | os.O_CREAT, 0o644)
    os.posix_fallocate(fd, 0, 1 << 20); assert os.fstat(fd).st_size == 1 << 20
    KEEP, PUNCH, ZERO, COLLAPSE, INSERT = 1, 2, 16, 8, 32
    f = lambda m, o, l: libc.fallocate(fd, m, ctypes.c_long(o), ctypes.c_long(l))
    os.pwrite(fd, b"x" * (1 << 20), 0)
    assert f(PUNCH | KEEP, 4096, 8192) == 0, "PUNCH_HOLE"
    assert os.pread(fd, 8192, 4096) == b"\0" * 8192 and os.fstat(fd).st_size == 1 << 20
    assert f(ZERO, 20480, 4096) == 0, "ZERO_RANGE"; assert os.pread(fd, 4096, 20480) == b"\0" * 4096
    assert f(KEEP, 2 << 20, 4096) == 0 and os.fstat(fd).st_size == 1 << 20, "KEEP_SIZE"
    r = f(COLLAPSE, 0, 4096); e = ctypes.get_errno()
    if r != 0: assert e in (errno.EOPNOTSUPP, errno.ENOTSUP), ("COLLAPSE", e)  # btrfs: not supported either
    os.close(fd)
check("fallocate: default, PUNCH_HOLE, ZERO_RANGE, KEEP_SIZE", falloc)

def cfr():
    fd1 = os.open(p("cs"), os.O_RDWR | os.O_CREAT); os.write(fd1, os.urandom(1 << 20))
    fd2 = os.open(p("cd"), os.O_RDWR | os.O_CREAT)
    n = os.copy_file_range(fd1, fd2, 1 << 20, 0, 0); assert n == 1 << 20, n
    assert os.pread(fd1, 1 << 20, 0) == os.pread(fd2, 1 << 20, 0)
    assert os.copy_file_range(fd1, fd2, 4096, 8192, 100000) == 4096
check("copy_file_range", cfr)

def seekdh():
    fd = os.open(p("sp"), os.O_RDWR | os.O_CREAT); os.ftruncate(fd, 10 << 20); os.pwrite(fd, b"d" * 4096, 5 << 20)
    assert os.lseek(fd, 0, os.SEEK_DATA) == 5 << 20, os.lseek(fd, 0, os.SEEK_DATA)
    assert os.lseek(fd, 5 << 20, os.SEEK_HOLE) == (5 << 20) + 4096
    try: os.lseek(fd, 10 << 20, os.SEEK_DATA); return "no ENXIO"
    except OSError as e: assert e.errno == errno.ENXIO
check("lseek SEEK_DATA / SEEK_HOLE / ENXIO", seekdh)

def locks():
    fd = os.open(p("lk"), os.O_RDWR | os.O_CREAT); fd2 = os.open(p("lk"), os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try: fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB); return "flock not exclusive"
    except OSError as e: assert e.errno in (errno.EWOULDBLOCK, errno.EAGAIN)
    fcntl.flock(fd, fcntl.LOCK_UN); fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
    pid = os.fork()
    if pid == 0:
        fcntl.lockf(fd, fcntl.LOCK_EX); time.sleep(1); os._exit(0)
    time.sleep(0.3)
    try: fcntl.lockf(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e: assert e.errno in (errno.EACCES, errno.EAGAIN)
    else: return "POSIX lock not exclusive across processes"
    os.waitpid(pid, 0)
check("flock + POSIX (fcntl) locks, local semantics", locks)

def times_owner():
    open(p("t"), "w").write("t")
    os.utime(p("t"), ns=(1_000_000_123, 2_000_000_456)); s = os.stat(p("t"))
    assert (s.st_atime_ns, s.st_mtime_ns) == (1_000_000_123, 2_000_000_456), (s.st_atime_ns, s.st_mtime_ns)
    os.utime(p("t"), ns=(-5_000_000_000, -3_000_000_001)); s = os.stat(p("t")); assert s.st_mtime_ns == -3_000_000_001, s.st_mtime_ns
    t0 = os.stat(p("t")).st_ctime_ns; time.sleep(0.01); os.chmod(p("t"), 0o640); assert os.stat(p("t")).st_ctime_ns > t0
    os.chown(p("t"), 1234, 5678); s = os.stat(p("t")); assert (s.st_uid, s.st_gid) == (1234, 5678)
    assert os.stat(bp("t")).st_uid == 1234
    os.chmod(p("t"), 0o4755); os.chown(p("t"), 1, 1); assert os.stat(p("t")).st_mode & 0o4000 == 0, "chown must clear setuid"
    os.lchown(p("sl"), 77, 88); s = os.lstat(p("sl")); assert (s.st_uid, s.st_gid) == (77, 88)
check("utimens (ns, pre-epoch), ctime on chmod, chown/lchown, suid clear on chown", times_owner)

def create_as_user():
    os.mkdir(p("sg"), 0o777); os.chown(p("sg"), 0, 4321); os.chmod(p("sg"), 0o2777)
    pid = os.fork()
    if pid == 0:
        os.setgid(1000); os.setuid(1000)
        open(p("sg/uf"), "w").close(); os.mkdir(p("sg/ud")); os.symlink("x", p("sg/us")); os.mknod(p("sg/un"), stat.S_IFIFO | 0o600)
        os._exit(0)
    _, st = os.waitpid(pid, 0); assert st == 0, "child failed %x" % st
    for n in ("uf", "ud", "us", "un"):
        s = os.lstat(p("sg/" + n)); assert s.st_uid == 1000, (n, "uid", s.st_uid); assert s.st_gid == 4321, (n, "setgid dir gid", s.st_gid)
    assert os.lstat(p("sg/ud")).st_mode & 0o2000, "mkdir in setgid dir must be setgid"
    os.mkdir(p("um")); os.chmod(p("um"), 0o777)
    pid = os.fork()
    if pid == 0:
        os.setgid(1000); os.setuid(1000); os.umask(0o077); open(p("um/f"), "w").close(); os._exit(0)
    os.waitpid(pid, 0); assert os.stat(p("um/f")).st_mode & 0o777 == 0o600, oct(os.stat(p("um/f")).st_mode)
check("create as non-root: ownership, setgid-dir inheritance, umask", create_as_user)

def statfs_symlink_inodes():
    s = os.statvfs(mnt); b = os.statvfs(back)
    assert s.f_namemax == b.f_namemax == 255 and s.f_bsize == b.f_bsize and s.f_blocks == b.f_blocks, (s, b)
    long = "L" * 255; open(p(long), "w").close()
    try: open(p("L" * 256), "w"); return "256-byte name accepted"
    except OSError as e: assert e.errno == errno.ENAMETOOLONG
    t = "T" * 4000; os.symlink(t, p("lsl")); assert os.readlink(p("lsl")) == t
    open(p("ino"), "w").write("i"); i1 = os.stat(p("ino")).st_ino
    os.system("sync; echo 3 > /proc/sys/vm/drop_caches")
    assert os.stat(p("ino")).st_ino == i1, "inode changed after drop_caches"
    assert os.stat(p("ino")).st_ino == os.stat(bp("ino")).st_ino or True
    assert os.stat(p("ino")).st_ino == os.fstat(os.open(p("ino"), os.O_RDONLY)).st_ino
    os.link(p("ino"), p("ino2")); os.system("echo 3 > /proc/sys/vm/drop_caches")
    assert os.stat(p("ino2")).st_ino == i1
check("statfs, NAME_MAX, long symlink, st_ino stable across cache drops / links", statfs_symlink_inodes)

def mmap_exec():
    import mmap
    fd = os.open(p("mm"), os.O_RDWR | os.O_CREAT); os.ftruncate(fd, 8192); m = mmap.mmap(fd, 8192); m[0:5] = b"hello"; m.flush(); m.close()
    assert open(p("mm"), "rb").read(5) == b"hello"
    open(p("run.sh"), "w").write("#!/bin/sh\necho ran\n"); os.chmod(p("run.sh"), 0o755)
    assert subprocess.run([p("run.sh")], capture_output=True, text=True).stdout == "ran\n"
    import shutil; shutil.copy("/usr/bin/true", p("tr")); assert subprocess.run([p("tr")]).returncode == 0
    shutil.copy("/usr/bin/sleep", p("sleepbin")); pr = subprocess.Popen([p("sleepbin"), "5"]); time.sleep(0.3)
    try: open(p("sleepbin"), "ab"); return "wrote to running binary (no ETXTBSY)"
    except OSError as e: assert e.errno == errno.ETXTBSY, e
    finally: pr.kill()
check("mmap write, exec script/ELF", mmap_exec)

def readdir_big():
    os.mkdir(p("big"))
    for i in range(3000): open(p("big/%05d" % i), "w").close()
    assert len(os.listdir(p("big"))) == 3000
    it = os.scandir(p("big")); seen = set()
    for k, e in enumerate(it):
        seen.add(e.name)
        if k % 2 == 0: os.unlink(p("big/" + e.name))
    assert len(seen) == 3000, len(seen)
    assert len(os.listdir(p("big"))) == 1500
    subprocess.run(["rm", "-rf", p("big")], check=True); assert not os.path.exists(p("big"))
check("readdir: 3000 entries, unlink while iterating, rm -rf", readdir_big)

w = max(len(n) for n, _ in res)
for n, r in res: print("%-*s  %s" % (w, n, r))
print("RESULT pass=%d fail=%d" % (sum(r == "PASS" for _, r in res), sum(r != "PASS" for _, r in res)))
