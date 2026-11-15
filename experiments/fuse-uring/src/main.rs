//! E13: minimal flat in-memory FUSE server with two transports: classic /dev/fuse read/write
//! and FUSE-over-io_uring. Raw protocol, no libfuse/fuser. Run as root (direct mount(2)).
//!
//! usage: fuse-uring --mode classic|uring [--threads N] [--depth D] [--pin] [--passthrough]
//!                   [--attr-valid S] <mountpoint>

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::collections::HashMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

const OP_LOOKUP: u32 = 1;
const OP_FORGET: u32 = 2;
const OP_GETATTR: u32 = 3;
const OP_UNLINK: u32 = 10;
const OP_OPEN: u32 = 14;
const OP_RELEASE: u32 = 18;
const OP_INIT: u32 = 26;
const OP_CREATE: u32 = 35;
const OP_BATCH_FORGET: u32 = 42;

const FUSE_INIT_EXT: u32 = 1 << 30;
const F2_PASSTHROUGH: u32 = 1 << (37 - 32);
const F2_OVER_IO_URING: u32 = 1 << (41 - 32);
const FOPEN_PASSTHROUGH: u32 = 1 << 7;
const IOC_BACKING_OPEN: libc::c_ulong = 0x4010_e501;
const IOC_BACKING_CLOSE: libc::c_ulong = 0x4004_e502;

const MAX_WRITE: usize = 131072;
const PAYLOAD_SZ: usize = 256 * 1024; // must be >= max(8192, max_write, max_pages*4096)
const HDR_SZ: usize = 512; // >= sizeof(fuse_uring_req_header) = 288
const OFF_OP_IN: usize = 128;
const OFF_ENT: usize = 256;

const CMD_REGISTER: u32 = 1;
const CMD_COMMIT_AND_FETCH: u32 = 2;

fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64le(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
fn put32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
fn put64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}

struct Inode {
    mode: u32,
    nlink: u32,
    backing: Option<(i32, i32)>, // (memfd, backing_id)
}
struct Fs {
    names: HashMap<Vec<u8>, u64>,
    inodes: HashMap<u64, Inode>,
    next: u64,
}
struct Shared {
    fs: Mutex<Fs>,
    counts: [AtomicU64; 64],
    passthrough: bool,
    attr_valid: u64,
    fd: i32,
}

fn put_attr(o: &mut [u8], ino: u64, mode: u32, nlink: u32) {
    put64(o, 0, ino);
    put64(o, 8, 0); // size
    put64(o, 16, 0); // blocks
    for t in [24, 32, 40] {
        put64(o, t, 1_700_000_000);
    }
    put32(o, 60, mode);
    put32(o, 64, nlink);
    put32(o, 80, 4096); // blksize
}

fn entry_out(o: &mut [u8], sh: &Shared, ino: u64, mode: u32, nlink: u32) {
    put64(o, 0, ino);
    put64(o, 8, 0);
    put64(o, 16, 0); // entry_valid = 0: every lookup hits the daemon
    put64(o, 24, sh.attr_valid);
    put_attr(&mut o[40..128], ino, mode, nlink);
}

fn passthrough_open(sh: &Shared, ino: &mut Inode) -> i32 {
    if !sh.passthrough {
        return 0;
    }
    if ino.backing.is_none() {
        let name = CString::new("bk").unwrap();
        let mfd = unsafe { libc::memfd_create(name.as_ptr(), 0) };
        if mfd < 0 {
            return 0;
        }
        let mut map = [0u8; 16];
        map[0..4].copy_from_slice(&mfd.to_le_bytes());
        let id = unsafe { libc::ioctl(sh.fd, IOC_BACKING_OPEN as _, map.as_ptr()) };
        if id < 0 {
            eprintln!("backing open failed: {}", std::io::Error::last_os_error());
            unsafe { libc::close(mfd) };
            return 0;
        }
        ino.backing = Some((mfd, id));
    }
    ino.backing.unwrap().1
}

/// op: opcode, hdr: per-op fixed header bytes, rest: trailing args (names). Writes reply body into out.
fn handle(sh: &Shared, op: u32, nodeid: u64, hdr: &[u8], rest: &[u8], out: &mut [u8]) -> Result<usize, i32> {
    sh.counts[(op & 63) as usize].fetch_add(1, Relaxed);
    let name = || rest.split(|&c| c == 0).next().unwrap_or(&[]).to_vec();
    match op {
        OP_LOOKUP => {
            let fs = sh.fs.lock().unwrap();
            let ino = *fs.names.get(&name()).ok_or(libc::ENOENT)?;
            let i = &fs.inodes[&ino];
            entry_out(out, sh, ino, i.mode, i.nlink);
            Ok(128)
        }
        OP_GETATTR => {
            let fs = sh.fs.lock().unwrap();
            let (mode, nl) = if nodeid == 1 { (0o040755, 2) } else {
                let i = fs.inodes.get(&nodeid).ok_or(libc::ENOENT)?;
                (i.mode, i.nlink)
            };
            put64(out, 0, sh.attr_valid);
            put32(out, 8, 0);
            put32(out, 12, 0);
            put_attr(&mut out[16..104], nodeid, mode, nl);
            Ok(104)
        }
        OP_OPEN => {
            let mut fs = sh.fs.lock().unwrap();
            let i = fs.inodes.get_mut(&nodeid).ok_or(libc::ENOENT)?;
            let id = passthrough_open(sh, i);
            put64(out, 0, 0);
            put32(out, 8, if id > 0 { FOPEN_PASSTHROUGH } else { 0 });
            put32(out, 12, id as u32);
            Ok(16)
        }
        OP_CREATE => {
            let _ = hdr; // fuse_create_in: flags, mode, umask, open_flags
            let mut fs = sh.fs.lock().unwrap();
            let n = name();
            let ino = match fs.names.get(&n) {
                Some(&i) => i,
                None => {
                    let i = fs.next;
                    fs.next += 1;
                    fs.inodes.insert(i, Inode { mode: 0o100644, nlink: 1, backing: None });
                    fs.names.insert(n, i);
                    i
                }
            };
            let ent = fs.inodes.get_mut(&ino).unwrap();
            let (mode, nl) = (ent.mode, ent.nlink);
            let id = passthrough_open(sh, ent);
            entry_out(out, sh, ino, mode, nl);
            put64(out, 128, 0);
            put32(out, 136, if id > 0 { FOPEN_PASSTHROUGH } else { 0 });
            put32(out, 140, id as u32);
            Ok(144)
        }
        OP_UNLINK => {
            let mut fs = sh.fs.lock().unwrap();
            let ino = fs.names.remove(&name()).ok_or(libc::ENOENT)?;
            if let Some(i) = fs.inodes.remove(&ino) {
                if let Some((mfd, id)) = i.backing {
                    let id = id as u32;
                    unsafe {
                        libc::ioctl(sh.fd, IOC_BACKING_CLOSE as _, &id as *const u32);
                        libc::close(mfd);
                    }
                }
            }
            Ok(0)
        }
        OP_RELEASE => Ok(0),
        _ => Err(libc::ENOSYS), // includes FLUSH: ENOSYS makes the kernel stop sending it
    }
}

fn op_hdr_len(op: u32) -> usize {
    match op {
        OP_GETATTR => 16,
        OP_OPEN => 8,
        OP_RELEASE => 24,
        OP_CREATE => 16,
        25 => 24, // FLUSH
        OP_FORGET => 8,
        OP_BATCH_FORGET => 8,
        _ => 0,
    }
}

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set);
    }
}

fn init_reply(inb: &[u8], uring: bool, passthrough: bool) -> Vec<u8> {
    let kflags = u32le(inb, 40 + 12);
    let kflags2 = if kflags & FUSE_INIT_EXT != 0 { u32le(inb, 40 + 16) } else { 0 };
    let mut f2 = 0u32;
    if uring && kflags2 & F2_OVER_IO_URING != 0 {
        f2 |= F2_OVER_IO_URING;
    } else if uring {
        eprintln!("kernel did not offer FUSE_OVER_IO_URING (enable_uring=0?)");
        std::process::exit(2);
    }
    if passthrough && kflags2 & F2_PASSTHROUGH != 0 {
        f2 |= F2_PASSTHROUGH;
    }
    let mut r = vec![0u8; 64];
    put32(&mut r, 0, 7);
    put32(&mut r, 4, 45.min(u32le(inb, 40 + 4)));
    put32(&mut r, 8, 131072);
    put32(&mut r, 12, FUSE_INIT_EXT);
    r[16..18].copy_from_slice(&64u16.to_le_bytes()); // max_background
    r[18..20].copy_from_slice(&48u16.to_le_bytes());
    put32(&mut r, 20, MAX_WRITE as u32);
    put32(&mut r, 24, 1);
    put32(&mut r, 32, f2);
    put32(&mut r, 36, if f2 & F2_PASSTHROUGH != 0 { 1 } else { 0 });
    r
}

fn write_reply(fd: i32, unique: u64, err: i32, body: &[u8]) {
    let mut buf = Vec::with_capacity(16 + body.len());
    buf.extend_from_slice(&((16 + body.len()) as u32).to_le_bytes());
    buf.extend_from_slice(&(-err).to_le_bytes());
    buf.extend_from_slice(&unique.to_le_bytes());
    buf.extend_from_slice(body);
    unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
}

fn classic_loop(sh: Arc<Shared>, cpu: Option<usize>) {
    if let Some(c) = cpu {
        pin(c);
    }
    let fd = sh.fd;
    let mut buf = vec![0u8; MAX_WRITE + 8192];
    let mut reply = vec![0u8; 16 + 256];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            match std::io::Error::last_os_error().raw_os_error().unwrap() {
                libc::EINTR | libc::EAGAIN | libc::ENOENT => continue,
                _ => return,
            }
        }
        let n = n as usize;
        let op = u32le(&buf, 4);
        let unique = u64le(&buf, 8);
        let nodeid = u64le(&buf, 16);
        if op == OP_FORGET || op == OP_BATCH_FORGET {
            sh.counts[(op & 63) as usize].fetch_add(1, Relaxed);
            continue;
        }
        let hl = op_hdr_len(op).min(n - 40);
        let (hdr, rest) = buf[40..n].split_at(hl);
        match handle(&sh, op, nodeid, hdr, rest, &mut reply[16..]) {
            Ok(len) => {
                reply[0..4].copy_from_slice(&((16 + len) as u32).to_le_bytes());
                reply[4..8].copy_from_slice(&0i32.to_le_bytes());
                reply[8..16].copy_from_slice(&unique.to_le_bytes());
                unsafe { libc::write(fd, reply.as_ptr().cast(), 16 + len) };
            }
            Err(e) => write_reply(fd, unique, e, &[]),
        }
    }
}

#[repr(C, align(64))]
struct Hdr([u8; HDR_SZ]);
#[repr(C, align(4096))]
struct Payload([u8; PAYLOAD_SZ]);

struct Ent {
    hdr: Box<Hdr>,
    payload: Box<Payload>,
    iov: Box<[libc::iovec; 2]>,
}

fn uring_sqe(fd: i32, cmd_op: u32, qid: u16, commit_id: u64, iov: Option<&[libc::iovec; 2]>, ud: u64) -> squeue::Entry128 {
    let mut cmd = [0u8; 80];
    // struct fuse_uring_cmd_req { u64 flags; u64 commit_id; u16 qid; u8 pad[6]; }
    cmd[8..16].copy_from_slice(&commit_id.to_le_bytes());
    cmd[16..18].copy_from_slice(&qid.to_le_bytes());
    let mut b = opcode::UringCmd80::new(types::Fd(fd), cmd_op).cmd(cmd);
    if let Some(iov) = iov {
        b = b.addr(Some(iov.as_ptr() as u64));
    }
    let mut e = b.build();
    e.set_user_data(ud);
    if iov.is_some() {
        // sqe->len (offset 24) = number of iovecs; UringCmd80 has no setter for it.
        unsafe { *((&mut e as *mut squeue::Entry128 as *mut u8).add(24) as *mut u32) = 2 };
    }
    e
}

fn uring_loop(sh: Arc<Shared>, qid: usize, depth: usize) {
    pin(qid);
    let fd = sh.fd;
    let mut ring: IoUring<squeue::Entry128, cqueue::Entry> =
        IoUring::builder().build(depth as u32 * 2).expect("io_uring_setup (SQE128)");
    let mut ents: Vec<Ent> = (0..depth)
        .map(|_| {
            let mut e = Ent {
                hdr: Box::new(Hdr([0; HDR_SZ])),
                payload: Box::new(Payload([0; PAYLOAD_SZ])),
                iov: Box::new([libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 }; 2]),
            };
            e.iov[0] = libc::iovec { iov_base: e.hdr.0.as_mut_ptr().cast(), iov_len: HDR_SZ };
            e.iov[1] = libc::iovec { iov_base: e.payload.0.as_mut_ptr().cast(), iov_len: PAYLOAD_SZ };
            e
        })
        .collect();
    let mut live = depth;
    for i in 0..depth {
        unsafe { ring.submission().push(&uring_sqe(fd, CMD_REGISTER, qid as u16, 0, Some(&ents[i].iov), i as u64)).unwrap() };
    }
    while live > 0 {
        if let Err(e) = ring.submit_and_wait(1) {
            if e.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            eprintln!("q{qid}: submit_and_wait: {e}");
            return;
        }
        let cqes: Vec<_> = ring.completion().collect();
        for cqe in cqes {
            let i = cqe.user_data() as usize;
            let res = cqe.result();
            if res == -libc::EAGAIN {
                // REGISTER raced FUSE_INIT completion; retry
                std::thread::sleep(std::time::Duration::from_millis(20));
                unsafe { ring.submission().push(&uring_sqe(fd, CMD_REGISTER, qid as u16, 0, Some(&ents[i].iov), i as u64)).unwrap() };
                continue;
            }
            if res < 0 {
                if res != -libc::ENOTCONN && res != -libc::ECANCELED && res != -libc::ECONNABORTED {
                    eprintln!("q{qid} ent{i}: cqe res {res}");
                }
                live -= 1;
                continue;
            }
            let e = &mut ents[i];
            let h = &mut e.hdr.0;
            let op = u32le(h, 4);
            let unique = u64le(h, 8);
            let nodeid = u64le(h, 16);
            let commit_id = u64le(h, OFF_ENT + 8);
            let psz = u32le(h, OFF_ENT + 16) as usize;
            let hl = op_hdr_len(op);
            let mut out_len = 0usize;
            let mut err = 0;
            if op == OP_FORGET || op == OP_BATCH_FORGET {
                err = libc::ENOSYS; // not delivered over the ring in 7.2.11; defensive
            } else {
                let mut rb = [0u8; 512];
                let rl = psz.min(512);
                rb[..rl].copy_from_slice(&e.payload.0[..rl]);
                match handle(&sh, op, nodeid, &h[OFF_OP_IN..OFF_OP_IN + hl], &rb[..rl], &mut e.payload.0[..]) {
                    Ok(l) => out_len = l,
                    Err(x) => err = x,
                }
            }
            // out header lives in the in_out slot; reply args go in the payload buffer
            put32(h, 0, 16 + out_len as u32);
            h[4..8].copy_from_slice(&(-err).to_le_bytes());
            put64(h, 8, unique);
            put32(h, OFF_ENT + 16, out_len as u32);
            unsafe { ring.submission().push(&uring_sqe(fd, CMD_COMMIT_AND_FETCH, qid as u16, commit_id, Some(&e.iov), i as u64)).unwrap() };
        }
    }
}

fn main() {
    let mut mode = "classic".to_string();
    let mut threads = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) } as usize;
    let mut depth = 4usize;
    let (mut do_pin, mut passthrough, mut attr_valid) = (false, false, 0u64);
    let mut mnt = None;
    let mut a = std::env::args().skip(1);
    while let Some(x) = a.next() {
        match x.as_str() {
            "--mode" => mode = a.next().unwrap(),
            "--threads" => threads = a.next().unwrap().parse().unwrap(),
            "--depth" => depth = a.next().unwrap().parse().unwrap(),
            "--pin" => do_pin = true,
            "--passthrough" => passthrough = true,
            "--attr-valid" => attr_valid = a.next().unwrap().parse().unwrap(),
            _ => mnt = Some(x),
        }
    }
    let mnt = mnt.expect("mountpoint");
    let uring = mode == "uring";
    let fd = unsafe { libc::open(c"/dev/fuse".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
    assert!(fd >= 0, "open /dev/fuse");
    let opts = CString::new(format!("fd={fd},rootmode=40000,user_id=0,group_id=0,allow_other")).unwrap();
    let cm = CString::new(mnt).unwrap();
    let r = unsafe { libc::mount(c"fuse-uring".as_ptr(), cm.as_ptr(), c"fuse".as_ptr(), libc::MS_NOSUID | libc::MS_NODEV, opts.as_ptr().cast()) };
    assert!(r == 0, "mount: {}", std::io::Error::last_os_error());

    let sh = Arc::new(Shared {
        fs: Mutex::new(Fs { names: HashMap::new(), inodes: HashMap::new(), next: 2 }),
        counts: std::array::from_fn(|_| AtomicU64::new(0)),
        passthrough,
        attr_valid,
        fd,
    });

    // FUSE_INIT always goes over /dev/fuse read/write, in both modes.
    let mut buf = vec![0u8; MAX_WRITE + 8192];
    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    assert!(n > 0 && u32le(&buf, 4) == OP_INIT, "expected INIT");
    let rep = init_reply(&buf[..n as usize], uring, passthrough);
    write_reply(fd, u64le(&buf, 8), 0, &rep);
    eprintln!("mounted, mode={mode} threads={threads} depth={depth} passthrough={passthrough}");

    let mut hs = vec![];
    if uring {
        for q in 0..threads {
            let s = sh.clone();
            hs.push(std::thread::spawn(move || uring_loop(s, q, depth)));
        }
        // FORGET/INTERRUPT still arrive on /dev/fuse in uring mode: keep one classic reader.
        classic_loop(sh.clone(), None);
    } else {
        for t in 0..threads {
            let s = sh.clone();
            let c = if do_pin { Some(t) } else { None };
            hs.push(std::thread::spawn(move || classic_loop(s, c)));
        }
    }
    for h in hs {
        let _ = h.join();
    }
    let names = [(1, "LOOKUP"), (2, "FORGET"), (3, "GETATTR"), (10, "UNLINK"), (14, "OPEN"), (18, "RELEASE"), (25, "FLUSH"), (35, "CREATE"), (42, "BATCH_FORGET")];
    for (o, n) in names {
        eprintln!("count {n} {}", sh.counts[o].load(Relaxed));
    }
}
