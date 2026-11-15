//! E9 scale experiment driver. Subcommands: gen, scan, marks, leases, fid.
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const FAN_CLOEXEC: u32 = 0x1;
const FAN_NONBLOCK: u32 = 0x2;
const FAN_CLASS_NOTIF: u32 = 0x0;
const FAN_CLASS_PRE_CONTENT: u32 = 0x8;
const FAN_UNLIMITED_QUEUE: u32 = 0x10;
const FAN_UNLIMITED_MARKS: u32 = 0x20;
const FAN_REPORT_DFID_NAME: u32 = 0xC00;
const FAN_MARK_ADD: u32 = 0x1;
const FAN_MARK_DONT_FOLLOW: u32 = 0x4;
const FAN_MARK_FILESYSTEM: u32 = 0x100;
const FAN_CLOSE_WRITE: u64 = 0x8;
const FAN_OPEN: u64 = 0x20;
const FAN_CREATE: u64 = 0x100;
const FAN_DELETE: u64 = 0x200;
const FAN_Q_OVERFLOW: u64 = 0x4000;
const FAN_PRE_ACCESS: u64 = 0x0010_0000;
const FAN_EVENT_ON_CHILD: u64 = 0x0800_0000;
const FAN_RENAME: u64 = 0x1000_0000;
const FAN_ONDIR: u64 = 0x4000_0000;

const GUARD_KB: u64 = 220 * 1024;

fn errno() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
fn cs(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).unwrap()
}
fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn f(&mut self) -> f64 {
        ((self.next() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn normal(&mut self) -> f64 {
        let (a, b) = (self.f(), self.f());
        (-2.0 * a.ln()).sqrt() * (2.0 * std::f64::consts::PI * b).cos()
    }
}

// ---------- memory snapshots ----------
#[derive(Clone, Default)]
struct Snap {
    mi: HashMap<String, u64>,
    caches: HashMap<String, (u64, u64)>, // name -> (num_objs, bytes)
}
fn snap() -> Snap {
    let mut s = Snap::default();
    for l in fs::read_to_string("/proc/meminfo").unwrap().lines() {
        let mut it = l.split_whitespace();
        let k = it.next().unwrap_or("").trim_end_matches(':').to_string();
        let v: u64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
        s.mi.insert(k, v);
    }
    if let Ok(t) = fs::read_to_string("/proc/slabinfo") {
        for l in t.lines().skip(2) {
            let f: Vec<&str> = l.split_whitespace().collect();
            if f.len() > 3 {
                let num: u64 = f[2].parse().unwrap_or(0);
                let sz: u64 = f[3].parse().unwrap_or(0);
                s.caches.insert(f[0].to_string(), (num, num * sz));
            }
        }
    }
    s
}
fn avail_kb() -> u64 {
    for l in fs::read_to_string("/proc/meminfo").unwrap().lines() {
        // MemFree, not MemAvailable: pinned (unreclaimable) inode slabs are
        // counted as "reclaimable" in MemAvailable and would hide an OOM.
        if let Some(r) = l.strip_prefix("MemFree:") {
            return r.split_whitespace().next().unwrap().parse().unwrap();
        }
    }
    0
}
fn drop_caches() {
    unsafe { libc::sync() };
    // WARM=1: keep page cache (btree metadata), drop only dentries/inodes, so a
    // slow cold disk does not dominate the lease/fid runs.
    let lvl = if std::env::var("WARM").is_ok() { "2" } else { "3" };
    let _ = fs::write("/proc/sys/vm/drop_caches", lvl);
    std::thread::sleep(Duration::from_secs(2));
}
fn report(label: &str, a: &Snap, b: &Snap, count: u64) {
    let d = |k: &str| *b.mi.get(k).unwrap_or(&0) as i64 - *a.mi.get(k).unwrap_or(&0) as i64;
    let c = count.max(1) as f64;
    println!(
        "MEM {label} count={count} dSlab={}MiB dSUnreclaim={}MiB dSReclaimable={}MiB dMemAvailable={}MiB dMemFree={}MiB | per_item: slab={:.0}B sunreclaim={:.0}B memfree={:.0}B",
        d("Slab") / 1024, d("SUnreclaim") / 1024, d("SReclaimable") / 1024, d("MemAvailable") / 1024, d("MemFree") / 1024,
        d("Slab") as f64 * 1024.0 / c, d("SUnreclaim") as f64 * 1024.0 / c, -d("MemFree") as f64 * 1024.0 / c
    );
    let mut v: Vec<(String, i64, i64)> = b
        .caches
        .iter()
        .map(|(k, (n, by))| {
            let (n0, b0) = a.caches.get(k).copied().unwrap_or((0, 0));
            (k.clone(), *n as i64 - n0 as i64, *by as i64 - b0 as i64)
        })
        .filter(|x| x.2.abs() > 256 * 1024)
        .collect();
    v.sort_by_key(|x| -x.2.abs());
    for (k, dn, db) in v.iter().take(8) {
        println!("  CACHE {label} {k} dObjs={dn} dKiB={} per_item={:.0}B", db / 1024, *db as f64 / c);
    }
}

fn walk(root: &Path, f: &mut dyn FnMut(&Path, bool) -> bool) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        if !f(&d, true) {
            return;
        }
        let rd = match fs::read_dir(&d) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for e in rd {
            let e = e.unwrap();
            let p = e.path();
            if e.file_type().unwrap().is_dir() {
                stack.push(p);
            } else if !f(&p, false) {
                return;
            }
        }
    }
}

fn setrlim(res: u32, v: u64) {
    let r = libc::rlimit { rlim_cur: v as _, rlim_max: v as _ };
    if unsafe { libc::setrlimit(res as _, &r) } != 0 {
        eprintln!("setrlimit {res} {v} failed errno {}", errno());
    }
}

// ---------- gen ----------
fn cmd_gen(root: &Path, n: u64, j: usize, seed: u64) {
    let d = (n / 15).max(1) as usize;
    let mut rng = Rng(seed);
    let mut paths: Vec<PathBuf> = vec![root.to_path_buf()];
    let mut depth: Vec<u8> = vec![0];
    let mut parent: Vec<usize> = vec![0];
    const NM: [&str; 8] = ["src", "lib", "pkg", "cmd", "internal", "util", "test", "docs"];
    for i in 1..d {
        let b = 4 + rng.below(8) as usize;
        let mut p = (i - 1) / b;
        while depth[p] >= 10 {
            p = parent[p];
        }
        paths.push(paths[p].join(format!("{}{:x}", NM[rng.below(8) as usize], i)));
        depth.push(depth[p] + 1);
        parent.push(p);
    }
    let t0 = Instant::now();
    fs::create_dir_all(root).unwrap();
    for p in &paths[1..] {
        fs::create_dir(p).unwrap();
    }
    let t_dirs = secs(t0.elapsed());
    let mean = (n as f64 - d as f64) / d as f64 + 0.5; // floor() loses ~0.5/dir
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    let t1 = Instant::now();
    let paths = Arc::new(paths);
    let hs: Vec<_> = (0..j)
        .map(|t| {
            let paths = paths.clone();
            std::thread::spawn(move || {
                let (mut files, mut bytes) = (0u64, 0u64);
                let mut samples: Vec<u32> = vec![];
                let mut i = t;
                while i < paths.len() {
                    let mut r = Rng(seed ^ (i as u64).wrapping_mul(0x2545F4914F6CDD1D));
                    let cnt = (-r.f().ln() * mean).floor() as u64;
                    let dfd = unsafe { libc::open(cs(&paths[i]).as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
                    assert!(dfd >= 0);
                    const EX: [&str; 8] = ["rs", "go", "ts", "md", "json", "c", "h", "py"];
                    const ST: [&str; 6] = ["mod", "util", "handler", "config", "index", "types"];
                    for k in 0..cnt {
                        let name = format!("{}_{:x}.{}", ST[r.below(6) as usize], k, EX[r.below(8) as usize]);
                        let sz = ((4096f64).ln() + 2.0 * r.normal()).exp().clamp(1.0, 64.0 * 1048576.0) as u64;
                        let c = CString::new(name).unwrap();
                        unsafe {
                            let fd = libc::openat(dfd, c.as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL, 0o644);
                            assert!(fd >= 0, "openat errno {}", errno());
                            assert_eq!(libc::ftruncate(fd, sz as i64), 0);
                            let ts = [
                                libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
                                libc::timespec { tv_sec: now - r.below(2 * 365 * 86400) as i64, tv_nsec: r.below(1_000_000_000) as i64 },
                            ];
                            assert_eq!(libc::futimens(fd, ts.as_ptr()), 0);
                            libc::close(fd);
                        }
                        files += 1;
                        bytes += sz;
                        if files % 16 == 0 {
                            samples.push(sz.min(u32::MAX as u64) as u32);
                        }
                    }
                    unsafe { libc::close(dfd) };
                    i += j;
                }
                (files, bytes, samples)
            })
        })
        .collect();
    let (mut files, mut bytes, mut samples) = (0u64, 0u64, vec![]);
    for h in hs {
        let (f, b, s) = h.join().unwrap();
        files += f;
        bytes += b;
        samples.extend(s);
    }
    let t_files = secs(t1.elapsed());
    let t2 = Instant::now();
    unsafe { libc::sync() };
    let t_sync = secs(t2.elapsed());
    samples.sort();
    let q = |p: f64| samples[((samples.len() as f64 - 1.0) * p) as usize];
    let avgd = depth.iter().map(|&x| x as f64).sum::<f64>() / d as f64;
    println!(
        "RESULT gen target={n} files={files} dirs={d} entries={} logical_bytes={bytes} max_dir_depth={} avg_dir_depth={avgd:.1} avg_fanout={:.1} secs_dirs={t_dirs:.1} secs_files={t_files:.1} secs_sync={t_sync:.1} entries_per_s={:.0} threads={j}",
        files + d as u64, depth.iter().max().unwrap(), (files as f64 + d as f64 - 1.0) / d as f64,
        (files + d as u64) as f64 / (t_dirs + t_files)
    );
    println!("RESULT gen size_p50={} p90={} p99={} p999={} max={}", q(0.5), q(0.9), q(0.99), q(0.999), q(1.0));
}

// ---------- scan ----------
struct Q {
    st: Mutex<(Vec<PathBuf>, usize)>,
    cv: Condvar,
}
fn scan(root: &Path, j: usize) -> (u64, u64, u64, u64) {
    let q = Arc::new(Q { st: Mutex::new((vec![root.to_path_buf()], 0)), cv: Condvar::new() });
    let files = Arc::new(AtomicU64::new(0));
    let dirs = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let blocks = Arc::new(AtomicU64::new(0));
    let hs: Vec<_> = (0..j)
        .map(|_| {
            let (q, files, dirs, bytes, blocks) = (q.clone(), files.clone(), dirs.clone(), bytes.clone(), blocks.clone());
            std::thread::spawn(move || loop {
                let d = {
                    let mut g = q.st.lock().unwrap();
                    loop {
                        if let Some(d) = g.0.pop() {
                            g.1 += 1;
                            break Some(d);
                        }
                        if g.1 == 0 {
                            break None;
                        }
                        g = q.cv.wait(g).unwrap();
                    }
                };
                let Some(d) = d else { q.cv.notify_all(); break };
                let mut sub = vec![];
                let (mut f, mut nd, mut by, mut bl) = (0, 1, 0, 0);
                if let Ok(rd) = fs::read_dir(&d) {
                    for e in rd {
                        let e = e.unwrap();
                        let m = e.metadata().unwrap(); // lstat semantics
                        if m.is_dir() {
                            sub.push(e.path());
                        } else {
                            f += 1;
                            by += m.len();
                            bl += m.blocks();
                        }
                    }
                }
                files.fetch_add(f, Ordering::Relaxed);
                dirs.fetch_add(nd, Ordering::Relaxed);
                bytes.fetch_add(by, Ordering::Relaxed);
                blocks.fetch_add(bl, Ordering::Relaxed);
                nd = 0;
                let _ = nd;
                let mut g = q.st.lock().unwrap();
                g.0.extend(sub);
                g.1 -= 1;
                q.cv.notify_all();
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    (files.load(Ordering::Relaxed), dirs.load(Ordering::Relaxed), bytes.load(Ordering::Relaxed), blocks.load(Ordering::Relaxed))
}
fn cmd_scan(root: &Path) {
    drop_caches();
    let s0 = snap();
    for (j, warm_first) in [(1usize, false), (2, false)] {
        let _ = warm_first;
        drop_caches();
        for pass in ["cold", "warm", "warm2"] {
            let t = Instant::now();
            let (f, d, b, bl) = scan(root, j);
            let e = secs(t.elapsed());
            println!(
                "RESULT scan j={j} {pass} files={f} dirs={d} secs={e:.2} entries_per_s={:.0} logical_bytes={b} allocated_blocks512={bl}",
                (f + d) as f64 / e
            );
        }
    }
    let s1 = snap();
    report("scan_cache_after_warm", &s0, &s1, 0);
    let cnt = {
        let mut c = 0u64;
        walk(root, &mut |_, _| {
            c += 1;
            true
        });
        c
    };
    report("scan_cache_per_entry", &s0, &s1, cnt);
}

// ---------- marks ----------
fn cmd_marks(root: &Path, mode: &str, limited: bool) {
    drop_caches();
    let s0 = snap();
    let mut flags = FAN_CLOEXEC | FAN_CLASS_PRE_CONTENT;
    if !limited {
        flags |= FAN_UNLIMITED_MARKS;
    }
    let g = unsafe { libc::fanotify_init(flags, libc::O_RDONLY as u32) };
    assert!(g >= 0, "fanotify_init errno {}", errno());
    let (mut n, mut tmark) = (0u64, Duration::ZERO);
    let mut err = 0;
    let mut stopped = "";
    let t0 = Instant::now();
    let mut mark = |p: &Path, fl: u32, mask: u64, n: &mut u64| -> bool {
        let c = cs(p);
        let t = Instant::now();
        let r = unsafe { libc::fanotify_mark(g, FAN_MARK_ADD | fl, mask, libc::AT_FDCWD, c.as_ptr()) };
        tmark += t.elapsed();
        if r != 0 {
            err = errno();
            stopped = "errno";
            return false;
        }
        *n += 1;
        if *n % 4096 == 0 && avail_kb() < GUARD_KB {
            stopped = "memguard";
            return false;
        }
        true
    };
    match mode {
        "sb" => {
            mark(root, FAN_MARK_FILESYSTEM, FAN_PRE_ACCESS, &mut n);
        }
        "dir" => walk(root, &mut |p, isdir| !isdir || mark(p, 0, FAN_PRE_ACCESS | FAN_EVENT_ON_CHILD, &mut n)),
        "inode" => walk(root, &mut |p, isdir| isdir || mark(p, FAN_MARK_DONT_FOLLOW, FAN_PRE_ACCESS, &mut n)),
        _ => panic!("mode"),
    }
    let wall = secs(t0.elapsed());
    println!(
        "RESULT marks mode={mode} limited={limited} marks_added={n} stopped={stopped} errno={err} mark_syscall_secs={:.2} wall_secs={wall:.2} marks_per_s={:.0} max_user_marks={}",
        secs(tmark), n as f64 / secs(tmark).max(1e-9),
        fs::read_to_string("/proc/sys/fs/fanotify/max_user_marks").unwrap().trim()
    );
    let s1 = snap();
    report("marks_live_before_drop", &s0, &s1, n);
    drop_caches();
    let s2 = snap();
    report("marks_pinned_after_drop_caches", &s0, &s2, n);
    unsafe { libc::close(g) };
    drop_caches();
    let s3 = snap();
    report("marks_after_close_and_drop", &s0, &s3, n);
}

// ---------- leases ----------
fn cmd_leases(root: &Path, n: u64, storm: u64) {
    if std::env::var("WARM").is_ok() {
        let t = Instant::now();
        scan(root, 2);
        println!("INFO prewarm scan secs={:.1}", secs(t.elapsed()));
    }
    let mut r: libc::rlimit = unsafe { std::mem::zeroed() };
    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) };
    println!("INFO nofile_before cur={} max={}", r.rlim_cur, r.rlim_max);
    setrlim(libc::RLIMIT_NOFILE as u32, n + 4096);
    drop_caches();
    let s0 = snap();
    let mut fds: Vec<i32> = Vec::with_capacity(n as usize);
    let mut stopped = "";
    let mut err = 0;
    let t0 = Instant::now();
    let mut chunk = Instant::now();
    let step = (n / 10).max(1);
    walk(root, &mut |p, isdir| {
        if isdir {
            return true;
        }
        let fd = unsafe { libc::open(cs(p).as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOATIME) };
        if fd < 0 {
            err = errno();
            stopped = "open_errno";
            return false;
        }
        fds.push(fd);
        let c = fds.len() as u64;
        if c % 4096 == 0 && avail_kb() < GUARD_KB {
            stopped = "memguard";
            return false;
        }
        if c % step == 0 {
            println!("  CHUNK open upto={c} secs={:.2}", secs(chunk.elapsed()));
            chunk = Instant::now();
        }
        c < n
    });
    let nopen = fds.len() as u64;
    let t_open = secs(t0.elapsed());
    println!("RESULT leases open_files={nopen} stopped={stopped} errno={err} open_wall_secs={t_open:.2} (includes tree walk)");
    drop_caches();
    let s1 = snap();
    report("open_only", &s0, &s1, nopen);

    let t1 = Instant::now();
    let mut chunk = Instant::now();
    let (mut ok, mut lerr, mut lerrno) = (0u64, 0u64, 0);
    for (i, &fd) in fds.iter().enumerate() {
        if unsafe { libc::fcntl(fd, libc::F_SETLEASE, libc::F_RDLCK) } == 0 {
            ok += 1;
        } else {
            lerr += 1;
            lerrno = errno();
        }
        if (i as u64 + 1) % step == 0 {
            println!("  CHUNK lease upto={} secs={:.2}", i + 1, secs(chunk.elapsed()));
            chunk = Instant::now();
        }
        if (i + 1) % 4096 == 0 && avail_kb() < GUARD_KB {
            println!("  memguard in lease phase at {}", i + 1);
            break;
        }
    }
    let t_lease = secs(t1.elapsed());
    println!("RESULT leases leased={ok} failed={lerr} errno={lerrno} secs={t_lease:.2} leases_per_s={:.0}", ok as f64 / t_lease);
    drop_caches();
    let s2 = snap();
    report("lease_increment", &s1, &s2, ok);
    report("lease_total_open_plus_lease", &s0, &s2, ok);

    if storm > 0 {
        lease_storm(&fds[..(ok.min(storm)) as usize]);
    }

    let t2 = Instant::now();
    for &fd in &fds {
        unsafe { libc::fcntl(fd, libc::F_SETLEASE, libc::F_UNLCK) };
    }
    let t_un = secs(t2.elapsed());
    let t3 = Instant::now();
    for &fd in &fds {
        unsafe { libc::close(fd) };
    }
    println!("RESULT leases release_secs={t_un:.2} close_secs={:.2}", secs(t3.elapsed()));
    drop_caches();
    report("after_release_close", &s0, &snap(), nopen);
}

fn lease_storm(fds: &[i32]) {
    // Trigger lease breaks on every fd without blocking (O_NONBLOCK write-open
    // returns EWOULDBLOCK but starts the break and sends the lease signal).
    const SIG: i32 = 40; // RT signal
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, SIG);
        libc::sigaddset(&mut set, libc::SIGIO);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        let sfd = libc::signalfd(-1, &set, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
        assert!(sfd >= 0);
        let mut rl: libc::rlimit = std::mem::zeroed();
        libc::getrlimit(libc::RLIMIT_SIGPENDING, &mut rl);
        println!("INFO RLIMIT_SIGPENDING cur={} max={}", rl.rlim_cur, rl.rlim_max);
        for (label, limit) in [("default_sigpending", 0u64), ("raised_sigpending", 4_000_000)] {
            if limit > 0 {
                setrlim(libc::RLIMIT_SIGPENDING as u32, limit);
            }
            // re-arm leases (a previous round leaves them broken)
            for &fd in fds {
                libc::fcntl(fd, libc::F_SETLEASE, libc::F_UNLCK);
                libc::fcntl(fd, libc::F_SETLEASE, libc::F_RDLCK);
                libc::fcntl(fd, 10 /*F_SETSIG*/, SIG);
            }
            let t = Instant::now();
            let (mut wouldblock, mut other) = (0u64, 0u64);
            for &fd in fds {
                let p = CString::new(format!("/proc/self/fd/{fd}")).unwrap();
                let w = libc::open(p.as_ptr(), libc::O_WRONLY | libc::O_NONBLOCK | libc::O_CLOEXEC);
                if w >= 0 {
                    libc::close(w);
                    other += 1;
                } else if errno() == libc::EWOULDBLOCK {
                    wouldblock += 1;
                } else {
                    other += 1;
                }
            }
            let trig = secs(t.elapsed());
            let (mut rt, mut sigio) = (0u64, 0u64);
            let mut seen = std::collections::HashSet::new();
            let mut buf = vec![0u8; 128 * 1024];
            let t = Instant::now();
            loop {
                let r = libc::read(sfd, buf.as_mut_ptr() as *mut _, buf.len());
                if r <= 0 {
                    break;
                }
                for k in 0..(r as usize / 128) {
                    let si = &*(buf.as_ptr().add(k * 128) as *const libc::signalfd_siginfo);
                    if si.ssi_signo as i32 == SIG {
                        rt += 1;
                        seen.insert(si.ssi_fd);
                    } else {
                        sigio += 1;
                    }
                }
            }
            println!(
                "RESULT storm {label} breaks_triggered={wouldblock} other={other} trigger_secs={trig:.2} rt_signals_received={rt} unique_fds={} sigio_fallbacks={sigio} drain_secs={:.2}",
                seen.len(), secs(t.elapsed())
            );
        }
    }
}

// ---------- fid ----------
fn parse_events(buf: &[u8], c: &mut [u64; 6]) {
    let mut off = 0;
    while off + 24 <= buf.len() {
        let len = u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        let mask = u64::from_ne_bytes(buf[off + 8..off + 16].try_into().unwrap());
        c[0] += 1;
        for (i, m) in [FAN_CREATE, FAN_DELETE, FAN_OPEN, FAN_CLOSE_WRITE, FAN_Q_OVERFLOW].iter().enumerate() {
            if mask & m != 0 {
                c[i + 1] += 1;
            }
        }
        if len < 24 {
            break;
        }
        off += len;
    }
}

fn workload(dirs: &[PathBuf], nfiles: usize) -> (f64, f64) {
    let dfds: Vec<i32> = dirs.iter().map(|d| unsafe { libc::open(cs(d).as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) }).collect();
    assert!(dfds.iter().all(|&f| f >= 0));
    let name = |i: usize| CString::new(format!("w{i}")).unwrap();
    let t = Instant::now();
    for i in 0..nfiles {
        unsafe {
            let fd = libc::openat(dfds[i % dfds.len()], name(i).as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL, 0o644);
            assert!(fd >= 0);
            libc::close(fd);
        }
    }
    let c = secs(t.elapsed());
    let t = Instant::now();
    for i in 0..nfiles {
        unsafe { libc::unlinkat(dfds[i % dfds.len()], name(i).as_ptr(), 0) };
    }
    let d = secs(t.elapsed());
    for f in dfds {
        unsafe { libc::close(f) };
    }
    (c, d)
}

fn cmd_fid(root: &Path, mode: &str, reader: &str, qmode: &str, nfiles: usize) {
    if std::env::var("WARM").is_ok() {
        let t = Instant::now();
        scan(root, 2);
        println!("INFO prewarm scan secs={:.1}", secs(t.elapsed()));
    }
    let mut dirs: Vec<PathBuf> = vec![];
    walk(root, &mut |p, isdir| {
        if isdir {
            dirs.push(p.to_path_buf());
        }
        true
    });
    let step = (dirs.len() / 1000).max(1);
    let wdirs: Vec<PathBuf> = dirs.iter().step_by(step).take(1000).cloned().collect();
    let (bc, bd) = workload(&wdirs, nfiles);
    println!("RESULT fid baseline_no_group create_secs={bc:.2} delete_secs={bd:.2} files={nfiles}");
    drop_caches();
    let s0 = snap();
    let mut flags = FAN_CLOEXEC | FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME | FAN_NONBLOCK | FAN_UNLIMITED_MARKS;
    if qmode == "unlimited" {
        flags |= FAN_UNLIMITED_QUEUE;
    }
    let g = unsafe { libc::fanotify_init(flags, libc::O_RDONLY as u32) };
    assert!(g >= 0, "fanotify_init errno {}", errno());
    let base = FAN_CREATE | FAN_DELETE | FAN_RENAME | FAN_CLOSE_WRITE | FAN_OPEN | FAN_ONDIR;
    let t = Instant::now();
    let mut nm = 0u64;
    if mode == "sb" {
        let c = cs(root);
        assert_eq!(unsafe { libc::fanotify_mark(g, FAN_MARK_ADD | FAN_MARK_FILESYSTEM, base, libc::AT_FDCWD, c.as_ptr()) }, 0);
        nm = 1;
    } else {
        for d in &dirs {
            let c = cs(d);
            let r = unsafe { libc::fanotify_mark(g, FAN_MARK_ADD, base | FAN_EVENT_ON_CHILD, libc::AT_FDCWD, c.as_ptr()) };
            assert_eq!(r, 0, "mark errno {}", errno());
            nm += 1;
        }
    }
    println!("RESULT fid mode={mode} marks={nm} mark_secs={:.2}", secs(t.elapsed()));
    let s1 = snap();
    report("fid_marks_live", &s0, &s1, nm);
    drop_caches();
    report("fid_marks_pinned_after_drop", &s0, &snap(), nm);

    let go = Arc::new(AtomicBool::new(reader == "fast"));
    let stop = Arc::new(AtomicBool::new(false));
    let (go2, stop2) = (go.clone(), stop.clone());
    let h = std::thread::spawn(move || {
        let mut c = [0u64; 6];
        let mut buf = vec![0u8; 1 << 20];
        let mut last = Instant::now();
        loop {
            if !go2.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
            let r = unsafe { libc::read(g, buf.as_mut_ptr() as *mut _, buf.len()) };
            if r > 0 {
                parse_events(&buf[..r as usize], &mut c);
                last = Instant::now();
            } else if stop2.load(Ordering::Relaxed) {
                break;
            } else {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        (c, last)
    });
    let t = Instant::now();
    let (wc, wd) = workload(&wdirs, nfiles);
    let wl_end = t.elapsed();
    let sq = snap();
    report("fid_queue_at_end_of_workload", &s1, &sq, 0);
    go.store(true, Ordering::Relaxed);
    let tdrain = Instant::now();
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Relaxed);
    let (c, last) = h.join().unwrap();
    let drain = secs(last.duration_since(tdrain));
    let total = secs(wl_end) + drain.max(0.0);
    println!(
        "RESULT fid run mode={mode} reader={reader} queue={qmode} files={nfiles} create_secs={wc:.2} delete_secs={wd:.2} (baseline {bc:.2}/{bd:.2}) events={} create={} delete={} open={} close_write={} overflow={} drain_after_workload_secs={:.2} events_per_s_overall={:.0}",
        c[0], c[1], c[2], c[3], c[4], c[5], drain.max(0.0), c[0] as f64 / total
    );
    unsafe { libc::close(g) };
}

// Does an sb (or dir) PRE_ACCESS mark honour a per-inode FAN_MARK_IGNORE mask?
fn cmd_ignore(dir: &Path, mode: &str) {
    const FAN_MARK_IGNORE: u32 = 0x400;
    const FAN_MARK_IGNORED_SURV_MODIFY: u32 = 0x40;
    let a = dir.join("A_ignored");
    let b = dir.join("B_plain");
    fs::write(&a, vec![1u8; 8192]).unwrap();
    fs::write(&b, vec![1u8; 8192]).unwrap();
    let g = unsafe { libc::fanotify_init(FAN_CLOEXEC | FAN_CLASS_PRE_CONTENT | FAN_UNLIMITED_MARKS, libc::O_RDONLY as u32) };
    assert!(g >= 0);
    let d = cs(dir);
    let r = if mode == "sb" {
        unsafe { libc::fanotify_mark(g, FAN_MARK_ADD | FAN_MARK_FILESYSTEM, FAN_PRE_ACCESS, libc::AT_FDCWD, d.as_ptr()) }
    } else {
        unsafe { libc::fanotify_mark(g, FAN_MARK_ADD, FAN_PRE_ACCESS | FAN_EVENT_ON_CHILD, libc::AT_FDCWD, d.as_ptr()) }
    };
    println!("INFO base mark ({mode}) rc={r}");
    let ca = cs(&a);
    let r = unsafe { libc::fanotify_mark(g, FAN_MARK_ADD | FAN_MARK_IGNORE | FAN_MARK_IGNORED_SURV_MODIFY, FAN_PRE_ACCESS, libc::AT_FDCWD, ca.as_ptr()) };
    println!("RESULT ignore mark on A rc={r} errno={}", if r != 0 { errno() } else { 0 });
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let (seen2, stop2) = (seen.clone(), stop.clone());
    let h = std::thread::spawn(move || {
        let mut buf = vec![0u8; 8192];
        while !stop2.load(Ordering::Relaxed) {
            let mut p = libc::pollfd { fd: g, events: libc::POLLIN, revents: 0 };
            if unsafe { libc::poll(&mut p, 1, 100) } <= 0 { continue; }
            let r = unsafe { libc::read(g, buf.as_mut_ptr() as *mut _, buf.len()) };
            let mut off = 0usize;
            while r > 0 && off + 24 <= r as usize {
                let len = u32::from_ne_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                let fd = i32::from_ne_bytes(buf[off + 16..off + 20].try_into().unwrap());
                if fd >= 0 {
                    let name = fs::read_link(format!("/proc/self/fd/{fd}")).map(|p| p.display().to_string()).unwrap_or_default();
                    seen2.lock().unwrap().push(name);
                    let resp = [fd, 1i32]; // FAN_ALLOW
                    unsafe { libc::write(g, resp.as_ptr() as *const _, 8); libc::close(fd); }
                }
                off += len;
            }
        }
    });
    for (n, f) in [("A_ignored", &a), ("B_plain", &b)] {
        let before = seen.lock().unwrap().len();
        let fd = unsafe { libc::open(cs(f).as_ptr(), libc::O_RDONLY) };
        let mut x = [0u8; 16];
        unsafe { libc::pread(fd, x.as_mut_ptr() as *mut _, 16, 0); libc::close(fd) };
        std::thread::sleep(Duration::from_millis(300));
        println!("RESULT ignore read {n}: pre-access events = {}", seen.lock().unwrap().len() - before);
    }
    // writes to A: does SURV_MODIFY keep the ignore?
    let before = seen.lock().unwrap().len();
    let fd = unsafe { libc::open(ca.as_ptr(), libc::O_RDWR) };
    unsafe { libc::pwrite(fd, b"x".as_ptr() as *const _, 1, 0); libc::close(fd) };
    std::thread::sleep(Duration::from_millis(300));
    println!("RESULT ignore write A: events = {}", seen.lock().unwrap().len() - before);
    let before = seen.lock().unwrap().len();
    let fd = unsafe { libc::open(ca.as_ptr(), libc::O_RDONLY) };
    let mut x = [0u8; 16];
    unsafe { libc::pread(fd, x.as_mut_ptr() as *mut _, 16, 0); libc::close(fd) };
    std::thread::sleep(Duration::from_millis(300));
    println!("RESULT ignore read A after write: events = {}", seen.lock().unwrap().len() - before);
    stop.store(true, Ordering::Relaxed);
    h.join().unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let root = PathBuf::from(&a[2]);
    let num = |i: usize, d: u64| a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d);
    match a[1].as_str() {
        "gen" => cmd_gen(&root, num(3, 100000), num(4, 2) as usize, num(5, 42)),
        "ignore" => cmd_ignore(&root, &a[3]),
        "scan" => cmd_scan(&root),
        "marks" => cmd_marks(&root, &a[3], a.get(4).map(|s| s == "limited").unwrap_or(false)),
        "leases" => cmd_leases(&root, num(3, 100000), num(4, 0)),
        "fid" => cmd_fid(&root, &a[3], &a[4], &a[5], num(6, 100000) as usize),
        _ => panic!("usage"),
    }
}
