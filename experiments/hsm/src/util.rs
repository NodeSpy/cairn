use crate::sys::*;
use std::ffi::CString;
use std::io::{BufRead, BufReader};
use std::os::fd::*;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub const FAN_MARK_MOUNT: u32 = 0x10;
pub const FAN_MARK_FILESYSTEM: u32 = 0x100;

pub fn now_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
pub fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
pub fn cs(p: &str) -> CString {
    CString::new(p).unwrap()
}
pub fn open_(p: &str, fl: i32) -> i32 {
    unsafe { libc::open(cs(p).as_ptr(), fl | libc::O_CLOEXEC, 0o644) }
}
pub fn close_(fd: i32) {
    unsafe { libc::close(fd) };
}
pub fn fstat_(fd: i32) -> libc::stat {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    unsafe { libc::fstat(fd, &mut st) };
    st
}
pub fn stat_(p: &str) -> Option<libc::stat> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::stat(cs(p).as_ptr(), &mut st) } < 0 {
        None
    } else {
        Some(st)
    }
}
pub fn bfd(fd: i32) -> BorrowedFd<'static> {
    unsafe { BorrowedFd::borrow_raw(fd) }
}
pub fn msleep(ms: u64) {
    thread::sleep(Duration::from_millis(ms))
}
pub fn usleep(us: u64) {
    thread::sleep(Duration::from_micros(us))
}
pub fn lease(fd: i32, kind: i32) -> i32 {
    unsafe { libc::fcntl(fd, libc::F_SETLEASE, kind) }
}
pub fn getlease(fd: i32) -> i32 {
    unsafe { libc::fcntl(fd, libc::F_GETLEASE) }
}
pub fn lease_name(l: i32) -> &'static str {
    match l {
        x if x == libc::F_RDLCK => "RDLCK",
        x if x == libc::F_WRLCK => "WRLCK",
        x if x == libc::F_UNLCK => "none",
        _ => "?",
    }
}
pub fn errname(e: i32) -> String {
    match e {
        0 => "OK".into(),
        libc::EAGAIN => "EAGAIN".into(),
        libc::EIO => "EIO".into(),
        libc::EPERM => "EPERM".into(),
        libc::EBUSY => "EBUSY".into(),
        libc::ENOSPC => "ENOSPC".into(),
        libc::EACCES => "EACCES".into(),
        libc::ENOENT => "ENOENT".into(),
        libc::EINVAL => "EINVAL".into(),
        libc::ETXTBSY => "ETXTBSY".into(),
        x => format!("errno{}", x),
    }
}

pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn pat_byte(k: u32, off: u64) -> u8 {
    (1 + ((off.wrapping_mul(13) + k as u64 * 7 + (off >> 9) * 3) % 251)) as u8
}
pub fn fill_pat(k: u32, off: u64, buf: &mut [u8]) {
    for (i, b) in buf.iter_mut().enumerate() {
        *b = pat_byte(k, off + i as u64);
    }
}
pub fn pat_table(k: u32, size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    fill_pat(k, 0, &mut v);
    v
}
pub fn fnv_update(h: &mut u64, data: &[u8]) {
    for &b in data {
        *h ^= b as u64;
        *h = h.wrapping_mul(0x100000001b3);
    }
}
pub const FNV0: u64 = 0xcbf29ce484222325;
pub fn fnv_pat(k: u32, size: u64) -> u64 {
    let mut h = FNV0;
    let mut off = 0u64;
    let mut buf = vec![0u8; 65536];
    while off < size {
        let n = std::cmp::min(65536, size - off) as usize;
        fill_pat(k, off, &mut buf[..n]);
        fnv_update(&mut h, &buf[..n]);
        off += n as u64;
    }
    h
}

/// Write the pattern for the whole file through fd (chunks of 64K).
pub fn fill_fd(fd: i32, k: u32, size: u64, only: Option<u64>) {
    let mut off = 0u64;
    let lim = only.unwrap_or(size).min(size);
    let mut buf = vec![0u8; 65536];
    while off < lim {
        let n = std::cmp::min(65536, lim - off) as usize;
        fill_pat(k, off, &mut buf[..n]);
        pwrite_all(bfd(fd), &buf[..n], off).unwrap();
        off += n as u64;
    }
}

pub fn set_mtime_sec(fd: i32, sec: i64) {
    set_mtime_fd(bfd(fd), sec * 1_000_000_000).unwrap();
}

/// Create `path` of `size`. filled=true writes pattern; else sparse placeholder.
pub fn make_file(path: &str, size: u64, k: u32, filled: bool, mtime: i64) {
    let _ = std::fs::remove_file(path);
    let fd = open_(path, libc::O_CREAT | libc::O_RDWR | libc::O_EXCL);
    assert!(fd >= 0, "create {} errno {}", path, errno());
    if filled {
        fill_fd(fd, k, size, None);
    } else {
        assert!(unsafe { libc::ftruncate(fd, size as i64) } == 0);
    }
    set_mtime_sec(fd, mtime);
    close_(fd);
}

pub fn mark_add(g: &OwnedFd, path: &str, mask: u64, extra: u32) -> std::io::Result<()> {
    fanotify_mark_path(g.as_fd(), FAN_MARK_ADD | extra, mask, Path::new(path))
}
pub fn mark_rm(g: &OwnedFd, path: &str, mask: u64, extra: u32) -> std::io::Result<()> {
    fanotify_mark_path(g.as_fd(), FAN_MARK_REMOVE | extra, mask, Path::new(path))
}

pub fn new_group(extra: u32) -> Arc<OwnedFd> {
    Arc::new(
        fanotify_init(
            FAN_CLOEXEC | FAN_CLASS_PRE_CONTENT | FAN_UNLIMITED_MARKS | FAN_UNLIMITED_QUEUE | extra,
            (libc::O_RDWR | libc::O_LARGEFILE | libc::O_CLOEXEC) as u32,
        )
        .expect("fanotify_init (need root)"),
    )
}

pub fn range_of(ev: &Event) -> Option<(u64, u64)> {
    ev.infos.iter().find_map(|i| if let Info::Range { offset, count } = i { Some((*offset, *count)) } else { None })
}
pub fn ev_name(fd: i32) -> String {
    fd_path(fd).ok().and_then(|p| p.file_name().map(|s| s.to_string_lossy().into_owned())).unwrap_or_default()
}

pub fn spawn_handler<F>(group: Arc<OwnedFd>, stop: Arc<AtomicBool>, mut f: F) -> thread::JoinHandle<()>
where
    F: FnMut(&Event) -> u32 + Send + 'static,
{
    thread::spawn(move || {
        let mut buf = vec![0u8; 1 << 16];
        while !stop.load(Ordering::Relaxed) {
            let mut p = libc::pollfd { fd: group.as_raw_fd(), events: libc::POLLIN, revents: 0 };
            let r = unsafe { libc::poll(&mut p, 1, 50) };
            if r <= 0 {
                continue;
            }
            let evs = match fanotify_read(group.as_fd(), &mut buf) {
                Ok(e) => e,
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    eprintln!("fanotify_read: {e}");
                    break;
                }
            };
            for ev in evs {
                if ev.fd < 0 {
                    continue;
                }
                let resp = f(&ev);
                let _ = fanotify_respond(group.as_fd(), ev.fd, resp);
                close_raw(ev.fd);
            }
        }
    })
}

pub fn run_self(args: &[&str]) -> String {
    let out = Command::new("/proc/self/exe").args(args).output().unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}
pub fn spawn_self(args: &[&str]) -> Child {
    Command::new("/proc/self/exe").args(args).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap()
}
/// Wait for a line containing `needle` from child stdout; returns the line.
pub fn wait_line(ch: &mut Child, needle: &str) -> String {
    let out = ch.stdout.as_mut().unwrap();
    let mut line = String::new();
    // unbuffered-ish: read byte by byte
    let mut b = [0u8; 1];
    use std::io::Read;
    loop {
        match out.read(&mut b) {
            Ok(1) => {
                if b[0] == b'\n' {
                    if line.contains(needle) {
                        return line;
                    }
                    line.clear();
                } else {
                    line.push(b[0] as char);
                }
            }
            _ => return format!("EOF:{line}"),
        }
    }
}
pub fn finish(mut ch: Child) -> String {
    drop(ch.stdin.take());
    let out = ch.wait_with_output().unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Run inotifywait on `dir` while f runs; return its output lines.
pub fn with_inotify<R>(dir: &str, f: impl FnOnce() -> R) -> (R, Vec<String>) {
    let mut ch = Command::new("inotifywait")
        .args(["-m", "-q", "-e", "modify,attrib,open,access,close_write,close_nowrite,create,delete,moved_from,moved_to", "--format", "%e %f", dir])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("inotifywait");
    msleep(400);
    let r = f();
    msleep(400);
    unsafe { libc::kill(ch.id() as i32, libc::SIGTERM) };
    let out = ch.wait_with_output().unwrap();
    let lines = String::from_utf8_lossy(&out.stdout).lines().map(|s| s.to_string()).collect();
    (r, lines)
}

/// Thread that reads lease-break signals via signalfd, waits `delay_us`, then F_UNLCK the ssi_fd.
pub struct Breaker {
    pub delay_us: Arc<AtomicU64>,
    pub log: Arc<Mutex<Vec<(u64, i32)>>>,
}
pub fn spawn_breaker() -> Breaker {
    block_signal(lease_signal()).unwrap();
    let sfd = signalfd(lease_signal()).unwrap();
    let delay = Arc::new(AtomicU64::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let (d2, l2) = (delay.clone(), log.clone());
    thread::spawn(move || loop {
        match signalfd_read_fd(sfd.as_fd()) {
            Ok(fd) => {
                l2.lock().unwrap().push((now_ns(), fd));
                let d = d2.load(Ordering::SeqCst);
                if d > 0 {
                    usleep(d);
                }
                lease(fd, libc::F_UNLCK);
            }
            Err(_) => break,
        }
    });
    Breaker { delay_us: delay, log }
}
pub fn lease_open(path: &str) -> i32 {
    let fd = open_(path, libc::O_RDONLY);
    assert!(fd >= 0);
    set_sig(bfd(fd), lease_signal()).unwrap();
    let r = lease(fd, libc::F_RDLCK);
    assert!(r == 0, "lease on {path}: errno {}", errno());
    fd
}
