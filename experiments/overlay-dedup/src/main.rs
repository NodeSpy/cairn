#![allow(dead_code)]
//! xp: experiment helper for E5 (overlayfs over placeholders) and E7 (reflink/dedupe).
mod sys;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use sys::*;

const FICLONE: u64 = 0x4004_9409;
const FICLONERANGE: u64 = 0x4020_940d;
const FIDEDUPERANGE: u64 = 0xC018_9436;
const FAN_MARK_MOUNT: u32 = 0x10;
const FAN_MARK_FILESYSTEM: u32 = 0x100;

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
fn ename(e: i32) -> String {
    if e == 0 { "0".into() } else { format!("{}({})", e, std::io::Error::from_raw_os_error(e)) }
}
fn fstat(fd: RawFd) -> libc::stat {
    let mut s: libc::stat = unsafe { std::mem::zeroed() };
    unsafe { libc::fstat(fd, &mut s) };
    s
}
fn fmt_st(s: &libc::stat) -> String {
    format!(
        "size={} bytes_alloc={} mtime={}.{:09} ctime={}.{:09}",
        s.st_size, s.st_blocks * 512, s.st_mtime, s.st_mtime_nsec, s.st_ctime, s.st_ctime_nsec
    )
}
fn changed(a: &libc::stat, b: &libc::stat) -> String {
    format!(
        "mtime_changed={} ctime_changed={} size_changed={}",
        (a.st_mtime, a.st_mtime_nsec) != (b.st_mtime, b.st_mtime_nsec),
        (a.st_ctime, a.st_ctime_nsec) != (b.st_ctime, b.st_ctime_nsec),
        a.st_size != b.st_size
    )
}
fn num(s: &str) -> u64 {
    let s = s.trim();
    if let Some(h) = s.strip_prefix("0x") { return u64::from_str_radix(h, 16).unwrap(); }
    let (n, m) = match s.chars().last() {
        Some('K') => (&s[..s.len() - 1], 1u64 << 10),
        Some('M') => (&s[..s.len() - 1], 1 << 20),
        Some('G') => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    n.parse::<u64>().unwrap() * m
}
fn open_ro(p: &str) -> File { File::open(p).unwrap_or_else(|e| { eprintln!("open {p}: {e}"); std::process::exit(2) }) }
fn open_rw(p: &str) -> File {
    OpenOptions::new().read(true).write(true).open(p).unwrap_or_else(|e| { eprintln!("open rw {p}: {e}"); std::process::exit(2) })
}
fn open_wo_create(p: &str) -> File {
    OpenOptions::new().write(true).create(true).mode(0o644).open(p).unwrap_or_else(|e| { eprintln!("open wo {p}: {e}"); std::process::exit(2) })
}
fn bfd(fd: RawFd) -> BorrowedFd<'static> { unsafe { BorrowedFd::borrow_raw(fd) } }
fn poll_fd(fd: RawFd, ms: i32) -> bool {
    let mut p = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
    unsafe { libc::poll(&mut p, 1, ms) > 0 }
}
fn sleep_ms(ms: u64) { std::thread::sleep(Duration::from_millis(ms)); }

fn fnv(buf: &[u8], mut h: u64) -> u64 {
    for &b in buf { h ^= b as u64; h = h.wrapping_mul(0x100000001b3); }
    h
}

// ---------------- hsmd ----------------
#[derive(PartialEq, Clone, Copy)]
enum Fill { Pwrite, Clone, None, Deny }

fn walk(p: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let ft = e.file_type().unwrap();
            if ft.is_dir() { walk(&e.path(), out) } else if ft.is_file() { out.push(e.path()) }
        }
    }
}

struct Cfg { root: String, store: String, fill: Fill, unmark: bool, via_path: bool }

fn worker(gfd: RawFd, cfg: &Cfg, logf: &std::sync::Mutex<File>) {
    use std::io::Write;
    let say = |s: String| { let mut f = logf.lock().unwrap(); writeln!(f, "{s}").unwrap(); f.flush().unwrap(); };
    let mypid = std::process::id() as i32;
    let mut buf = vec![0u8; 1 << 16];
    let mut chunk = vec![0u8; 1 << 20];
    loop {
        let evs = match fanotify_read(bfd(gfd), &mut buf) { Ok(e) => e, Err(e) => { say(format!("READERR {e}")); std::thread::sleep(Duration::from_millis(50)); continue } };
        for e in evs {
            if e.fd < 0 { say(format!("EVT mask={:#x} nofd", e.mask)); continue }
            let fd = e.fd;
            let path = fd_path(fd).map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| "?".into());
            if e.pid == mypid {
                let _ = fanotify_respond(bfd(gfd), fd, FAN_ALLOW); close_raw(fd);
                say(format!("SELFEVT mask={:#x} path={} (auto-allowed; nested event raised by our own fill)", e.mask, path));
                continue;
            }
            let t = Instant::now();
            let comm = std::fs::read_to_string(format!("/proc/{}/comm", e.pid)).unwrap_or_default().trim().to_string();
            let range = e.infos.iter().find_map(|i| if let Info::Range { offset, count } = i { Some(format!("{offset}+{count}")) } else { None }).unwrap_or("none".into());
            let mut act = String::new();
            let mut resp = FAN_ALLOW;
            let s = fstat(fd);
            let placeholder = (s.st_mode & libc::S_IFMT) == libc::S_IFREG && s.st_size > 0 && s.st_blocks == 0;
            let mut filled = false;
            match cfg.fill {
                Fill::None => act.push_str("allow-nofill"),
                Fill::Deny => { act.push_str("deny"); resp = FAN_DENY | (libc::EIO as u32) << 24 }
                _ if !placeholder => act.push_str("skip-already-filled"),
                _ => {
                    let rel = path.strip_prefix(&cfg.root).unwrap_or(&path).trim_start_matches('/').to_string();
                    let sp = format!("{}/{rel}", cfg.store);
                    let r: Result<(), String> = (|| {
                        let sf = File::open(&sp).map_err(|e| format!("open store {sp}: {e}"))?;
                        let wf; // keeps path-opened fd alive
                        let wfd = if cfg.via_path {
                            let rp = format!("{}/{rel}", cfg.root);
                            wf = OpenOptions::new().write(true).open(&rp).map_err(|e| format!("open-for-write {rp}: {e}"))?;
                            wf.as_raw_fd()
                        } else { fd };
                        if cfg.fill == Fill::Clone {
                            let r = unsafe { libc::ioctl(wfd, FICLONE as _, sf.as_raw_fd()) };
                            if r < 0 { return Err(format!("FICLONE: {}", ename(errno()))) }
                        } else {
                            let mut sf = sf; let mut off = 0u64;
                            loop {
                                let n = sf.read(&mut chunk).map_err(|e| e.to_string())?;
                                if n == 0 { break }
                                pwrite_all(bfd(wfd), &chunk[..n], off).map_err(|e| format!("pwrite: {e}"))?;
                                off += n as u64;
                            }
                        }
                        Ok(())
                    })();
                    match r {
                        Ok(()) => { act.push_str(if cfg.fill == Fill::Clone { "fill-clone" } else { "fill-pwrite" }); filled = true }
                        Err(m) => { act.push_str(&format!("fillfail[{m}]")); resp = FAN_DENY | (libc::EIO as u32) << 24 }
                    }
                }
            }
            if filled && cfg.unmark { if let Err(er) = fanotify_mark_fd(bfd(gfd), FAN_MARK_REMOVE, FAN_PRE_ACCESS, bfd(fd)) { act.push_str(&format!(" unmark-fail[{er}]")) } else { act.push_str(" unmarked") } }
            let _ = fanotify_respond(bfd(gfd), fd, resp);
            close_raw(fd);
            say(format!("EVT pid={} comm={} mask={:#x} path={} range={} act={} us={}", e.pid, comm, e.mask, path, range, act, t.elapsed().as_micros()));
        }
    }
}

fn hsmd(a: &[String]) {
    let (mut log, mut root, mut store) = (String::new(), String::new(), String::new());
    let mut fill = Fill::Pwrite;
    let mut marks: Vec<(String, String)> = vec![];
    let mut rdonly = false;
    let (mut unmark, mut via_path) = (false, false);
    let mut workers = 4usize;
    let mut i = 0;
    while i < a.len() {
        match a[i].as_str() {
            "--log" => { i += 1; log = a[i].clone() }
            "--root" => { i += 1; root = a[i].clone() }
            "--store" => { i += 1; store = a[i].clone() }
            "--fill" => { i += 1; fill = match a[i].as_str() { "pwrite" => Fill::Pwrite, "clone" => Fill::Clone, "none" => Fill::None, "deny" => Fill::Deny, x => panic!("fill {x}") } }
            "--mark" => { i += 1; let (k, v) = a[i].split_once(':').unwrap(); marks.push((k.into(), v.into())) }
            "--evflags" => { i += 1; rdonly = a[i] == "rdonly" }
            "--writevia" => { i += 1; via_path = a[i] == "path" }
            "--unmark" => unmark = true,
            "--workers" => { i += 1; workers = a[i].parse().unwrap() }
            x => panic!("arg {x}"),
        }
        i += 1;
    }
    let logf = std::sync::Mutex::new(File::create(&log).unwrap());
    use std::io::Write;
    let say = |s: String| { let mut f = logf.lock().unwrap(); writeln!(f, "{s}").unwrap(); f.flush().unwrap(); };
    let evf = (if rdonly { libc::O_RDONLY } else { libc::O_RDWR }) | libc::O_CLOEXEC;
    let g = fanotify_init(FAN_CLASS_PRE_CONTENT | FAN_CLOEXEC | FAN_UNLIMITED_MARKS, evf as u32).expect("fanotify_init");
    let gfd = g.as_raw_fd();
    let t0 = Instant::now();
    let mut nmarks = 0;
    for (k, v) in &marks {
        let p = Path::new(v);
        let r = match k.as_str() {
            "inode" => {
                let mut files = vec![];
                if p.is_dir() { walk(p, &mut files) } else { files.push(p.to_path_buf()) }
                let mut res = Ok(());
                for f in files { res = fanotify_mark_path(bfd(gfd), FAN_MARK_ADD | FAN_MARK_DONT_FOLLOW, FAN_PRE_ACCESS, &f); nmarks += 1; if res.is_err() { break } }
                res
            }
            "dir" => { nmarks += 1; fanotify_mark_path(bfd(gfd), FAN_MARK_ADD, FAN_PRE_ACCESS | FAN_EVENT_ON_CHILD, p) }
            "mount" => { nmarks += 1; fanotify_mark_path(bfd(gfd), FAN_MARK_ADD | FAN_MARK_MOUNT, FAN_PRE_ACCESS, p) }
            "sb" => { nmarks += 1; fanotify_mark_path(bfd(gfd), FAN_MARK_ADD | FAN_MARK_FILESYSTEM, FAN_PRE_ACCESS, p) }
            x => panic!("mark kind {x}"),
        };
        if let Err(e) = r { say(format!("MARKFAIL {k}:{v} {e}")); std::process::exit(3) }
    }
    say(format!("READY marks={} mark_ms={}", nmarks, t0.elapsed().as_millis()));
    let cfg = Cfg { root, store, fill, unmark, via_path };
    std::thread::scope(|sc| {
        for _ in 0..workers { sc.spawn(|| worker(gfd, &cfg, &logf)); }
    });
}

// ---------------- inotify watcher ----------------
fn inw(a: &[String]) {
    let secs = num(&a[0]);
    let ino = inotify_init().unwrap();
    let mut wds = vec![];
    for p in &a[1..] { let wd = inotify_add(bfd(ino.as_raw_fd()), Path::new(p), 0xfff).unwrap(); wds.push((wd, p.clone())) }
    println!("INW_READY");
    let end = Instant::now() + Duration::from_secs(secs);
    let mut buf = vec![0u8; 1 << 16];
    let names = [(1, "ACCESS"), (2, "MODIFY"), (4, "ATTRIB"), (8, "CLOSE_WRITE"), (0x10, "CLOSE_NOWRITE"), (0x20, "OPEN"), (0x40, "MOVED_FROM"), (0x80, "MOVED_TO"), (0x100, "CREATE"), (0x200, "DELETE"), (0x400, "DELETE_SELF"), (0x800, "MOVE_SELF")];
    while Instant::now() < end {
        let left = end.saturating_duration_since(Instant::now()).as_millis() as i32;
        if !poll_fd(ino.as_raw_fd(), left.min(200)) { continue }
        for e in inotify_read(bfd(ino.as_raw_fd()), &mut buf).unwrap() {
            let w = wds.iter().find(|(w, _)| *w == e.wd).map(|x| x.1.clone()).unwrap_or_default();
            let f: Vec<&str> = names.iter().filter(|(b, _)| e.mask & b != 0).map(|x| x.1).collect();
            println!("IN {} {} {}", w, String::from_utf8_lossy(&e.name), f.join("|"));
        }
    }
}

// ---------------- reads ----------------
fn sum(a: &[String]) {
    let mut f = match File::open(&a[0]) { Ok(f) => f, Err(e) => { println!("sum open_err={e}"); return } };
    let mut buf = vec![0u8; 1 << 20];
    let (mut n, mut nz, mut h) = (0u64, 0u64, 0xcbf29ce484222325u64);
    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(k) => { n += k as u64; nz += buf[..k].iter().filter(|&&b| b != 0).count() as u64; h = fnv(&buf[..k], h) }
            Err(e) => { println!("sum read_err={e} after={n}"); return }
        }
    }
    println!("sum bytes={n} nonzero={nz} fnv={h:016x}");
}

fn mmapread(a: &[String]) {
    let f = open_ro(&a[0]);
    let len = fstat(f.as_raw_fd()).st_size as usize;
    let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, f.as_raw_fd(), 0) };
    if p == libc::MAP_FAILED { println!("mmapread mmap_err={}", ename(errno())); return }
    let s = unsafe { std::slice::from_raw_parts(p as *const u8, len) };
    let nz = s.iter().filter(|&&b| b != 0).count();
    println!("mmapread len={len} nonzero={nz} fnv={:016x}", fnv(s, 0xcbf29ce484222325));
    // does a subsequent read(2) fill it?
    let mut one = [0u8; 16];
    let r = unsafe { libc::pread(f.as_raw_fd(), one.as_mut_ptr() as *mut _, 16, 0) };
    println!("mmapread then pread n={r} first16={:02x?}", one);
}

fn readall(a: &[String]) {
    let mut files = vec![]; walk(Path::new(&a[0]), &mut files); files.sort();
    let t = Instant::now(); let mut tot = 0u64; let mut bad = 0;
    let mut buf = vec![0u8; 1 << 20];
    let mut lat = vec![];
    for p in &files {
        let t1 = Instant::now();
        let mut f = File::open(p).unwrap();
        loop { match f.read(&mut buf) { Ok(0) => break, Ok(n) => tot += n as u64, Err(_) => { bad += 1; break } } }
        lat.push(t1.elapsed().as_micros());
    }
    lat.sort();
    let el = t.elapsed().as_secs_f64();
    println!("readall files={} bytes={} errors={} secs={:.3} files_per_s={:.0} MiB_per_s={:.1} lat_us_p50={} p99={}", files.len(), tot, bad, el, files.len() as f64 / el, tot as f64 / 1048576.0 / el, lat[lat.len() / 2], lat[lat.len() * 99 / 100]);
}

fn writeat(a: &[String]) {
    let f = open_rw(&a[0]);
    let r = unsafe { libc::pwrite(f.as_raw_fd(), b"WRITTEN".as_ptr() as *const _, 7, 100) };
    println!("write n={r} err={}", ename(if r < 0 { errno() } else { 0 }));
}

// ---------------- clone / dedupe ----------------
fn clone(a: &[String]) {
    let s = open_ro(&a[0]); let d = open_wo_create(&a[1]);
    let (sb, db) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    sleep_ms(20);
    let t = Instant::now();
    let r = unsafe { libc::ioctl(d.as_raw_fd(), FICLONE as _, s.as_raw_fd()) };
    let e = if r < 0 { errno() } else { 0 }; let us = t.elapsed().as_micros();
    let (sa, da) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    println!("FICLONE errno={} us={us} dst_before[{}] dst_after[{}] dst:{} src:{}", ename(e), fmt_st(&db), fmt_st(&da), changed(&db, &da), changed(&sb, &sa));
}
fn clonerange(a: &[String]) {
    let s = open_ro(&a[0]); let (so, len) = (num(&a[1]), num(&a[2])); let d = open_wo_create(&a[3]); let dof = num(&a[4]);
    let (sb, db) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    sleep_ms(20);
    #[repr(C)] struct R { fd: i64, so: u64, len: u64, dof: u64 }
    let r = R { fd: s.as_raw_fd() as i64, so, len, dof };
    let t = Instant::now();
    let rc = unsafe { libc::ioctl(d.as_raw_fd(), FICLONERANGE as _, &r as *const R) };
    let e = if rc < 0 { errno() } else { 0 }; let us = t.elapsed().as_micros();
    let (sa, da) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    println!("FICLONERANGE src_off={so} len={len} dst_off={dof} errno={} us={us} dst_size {}->{} dst:{} src:{}", ename(e), db.st_size, da.st_size, changed(&db, &da), changed(&sb, &sa));
}

#[repr(C)] struct DedupeHdr { so: u64, len: u64, count: u16, r1: u16, r2: u32, fd: i64, dof: u64, done: u64, status: i32, r3: u32 }
fn dedupe_once(sfd: RawFd, so: u64, len: u64, dfd: RawFd, dof: u64) -> (i32, i32, u64) {
    let mut h = DedupeHdr { so, len, count: 1, r1: 0, r2: 0, fd: dfd as i64, dof, done: 0, status: 0, r3: 0 };
    let rc = unsafe { libc::ioctl(sfd, FIDEDUPERANGE as _, &mut h as *mut DedupeHdr) };
    (if rc < 0 { errno() } else { 0 }, h.status, h.done)
}
fn status_name(s: i32) -> String {
    match s { 0 => "SAME".into(), 1 => "DIFFERS".into(), x if x < 0 => format!("errno {}", ename(-x)), x => x.to_string() }
}
fn dedupe(a: &[String]) {
    let s = open_ro(&a[0]); let (so, len) = (num(&a[1]), num(&a[2]));
    let rw = a.get(5).map(|x| x == "rw").unwrap_or(false);
    let d = if rw { open_rw(&a[3]) } else { open_ro(&a[3]) }; let dof = num(&a[4]);
    let (sb, db) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    sleep_ms(20);
    let t = Instant::now();
    let (e, st, done) = dedupe_once(s.as_raw_fd(), so, len, d.as_raw_fd(), dof);
    let us = t.elapsed().as_micros();
    let (sa, da) = (fstat(s.as_raw_fd()), fstat(d.as_raw_fd()));
    println!("FIDEDUPERANGE dstmode={} errno={} status={} bytes_deduped={done} us={us} dst:{} src:{} dst_after[{}]", if rw { "rw" } else { "ro" }, ename(e), status_name(st), changed(&db, &da), changed(&sb, &sa), fmt_st(&da));
}
fn dedupefile(a: &[String]) {
    let s = open_ro(&a[0]); let d = open_ro(&a[1]);
    let chunk = a.get(2).map(|x| num(x)).unwrap_or(16 << 20);
    let size = fstat(s.as_raw_fd()).st_size as u64;
    let t = Instant::now(); let (mut off, mut tot, mut calls, mut differs, mut err) = (0u64, 0u64, 0, 0, 0);
    while off < size {
        let l = chunk.min(size - off);
        let (e, st, done) = dedupe_once(s.as_raw_fd(), off, l, d.as_raw_fd(), off);
        calls += 1;
        if e != 0 { err = e; break }
        if st == 1 { differs += 1; break }
        if st < 0 { err = -st; break }
        if done == 0 { break }
        tot += done; off += done;
    }
    let el = t.elapsed().as_secs_f64();
    println!("dedupefile size={size} chunk={chunk} deduped={tot} calls={calls} differs={differs} err={} secs={el:.3} MiB_per_s={:.1}", ename(err), tot as f64 / 1048576.0 / el);
}
fn dedupebulk(a: &[String]) {
    let mut names: Vec<_> = std::fs::read_dir(&a[0]).unwrap().flatten().map(|e| e.file_name()).collect(); names.sort();
    let t = Instant::now(); let mut ioctl_us = 0u128; let (mut same, mut diff, mut errs, mut bytes) = (0, 0, 0, 0u64);
    for n in &names {
        let s = File::open(Path::new(&a[0]).join(n)).unwrap(); let d = File::open(Path::new(&a[1]).join(n)).unwrap();
        let len = fstat(s.as_raw_fd()).st_size as u64;
        let t1 = Instant::now();
        let (e, st, done) = dedupe_once(s.as_raw_fd(), 0, len, d.as_raw_fd(), 0);
        ioctl_us += t1.elapsed().as_micros();
        if e != 0 || st < 0 { errs += 1 } else if st == 1 { diff += 1 } else { same += 1; bytes += done }
    }
    let el = t.elapsed().as_secs_f64();
    println!("dedupebulk files={} same={same} differs={diff} errors={errs} bytes_deduped={bytes} secs={el:.3} files_per_s={:.0} ioctl_us_avg={}", names.len(), names.len() as f64 / el, ioctl_us / names.len().max(1) as u128);
}

// ---------------- holders ----------------
fn hold(a: &[String]) {
    let mode = a[1].as_str(); let secs = num(&a[2]);
    let f = match mode { "w" => OpenOptions::new().write(true).open(&a[0]).unwrap(), "mw" => open_rw(&a[0]), _ => open_ro(&a[0]) };
    let mut map: Option<&[u8]> = None;
    if mode == "m" || mode == "mw" {
        let len = (fstat(f.as_raw_fd()).st_size as usize).min(1 << 20);
        let prot = if mode == "mw" { libc::PROT_READ | libc::PROT_WRITE } else { libc::PROT_READ };
        let p = unsafe { libc::mmap(std::ptr::null_mut(), len, prot, libc::MAP_SHARED, f.as_raw_fd(), 0) };
        assert!(p != libc::MAP_FAILED);
        map = Some(unsafe { std::slice::from_raw_parts(p as *const u8, len) });
    }
    let h0 = map.map(|m| fnv(m, 0)).unwrap_or(0);
    println!("HOLD_READY mode={mode} map_fnv={h0:016x}");
    sleep_ms(secs * 1000);
    if let Some(m) = map { println!("HOLD_DONE map_fnv_before={h0:016x} map_fnv_after={:016x}", fnv(m, 0)) } else { println!("HOLD_DONE") }
}
fn lease(a: &[String]) {
    let secs = num(&a[1]);
    let sig = lease_signal();
    block_signal(sig).unwrap();
    let sfd = signalfd(sig).unwrap();
    let f = open_ro(&a[0]);
    set_sig(bfd(f.as_raw_fd()), sig).unwrap();
    match set_lease(bfd(f.as_raw_fd()), LEASE_RD) { Ok(()) => println!("LEASE_READY"), Err(e) => { println!("LEASE_FAIL {e}"); return } }
    let t = Instant::now();
    if poll_fd(sfd.as_raw_fd(), (secs * 1000) as i32) {
        let ms = t.elapsed().as_millis();
        let gl = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLEASE) };
        println!("LEASE_BROKEN after_ms={ms} F_GETLEASE={gl}");
        let _ = set_lease(bfd(f.as_raw_fd()), LEASE_UN);
    } else {
        let gl = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLEASE) };
        println!("LEASE_INTACT F_GETLEASE={gl} (F_RDLCK={})", libc::F_RDLCK);
    }
}

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let rest = &a[1..];
    match a[0].as_str() {
        "hsmd" => hsmd(rest), "inw" => inw(rest), "sum" => sum(rest), "mmapread" => mmapread(rest), "readall" => readall(rest),
        "write" => writeat(rest), "clone" => clone(rest), "clonerange" => clonerange(rest), "dedupe" => dedupe(rest),
        "dedupefile" => dedupefile(rest), "dedupebulk" => dedupebulk(rest), "hold" => hold(rest), "lease" => lease(rest),
        x => { eprintln!("unknown {x}"); std::process::exit(1) }
    }
}
