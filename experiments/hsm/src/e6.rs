use crate::sys::*;
use crate::util::*;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::*;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::*;
use std::sync::Arc;
use std::thread;

const HOT_SIZE: usize = 64 << 20;
const NSMALL: usize = 10_000;

/// Child process: set up marks per mode, allow every event immediately, count events.
/// stdin: "snap" -> prints events=N ; EOF -> exits.
pub fn handler(a: &[&str]) {
    let mode = a[0];
    let root = a[1];
    let g = new_group(0);
    let t0 = now_ns();
    let hot = format!("{root}/hot/hot.bin");
    match mode {
        "inode" => {
            mark_add(&g, &hot, FAN_PRE_ACCESS, 0).unwrap();
            for i in 0..NSMALL {
                mark_add(&g, &format!("{root}/small/s{i:05}"), FAN_PRE_ACCESS, 0).unwrap();
            }
        }
        "inode-hot" | "premark" => mark_add(&g, &hot, FAN_PRE_ACCESS, 0).unwrap(),
        "parent" => {
            mark_add(&g, &format!("{root}/hot"), FAN_PRE_ACCESS | FAN_EVENT_ON_CHILD, 0).unwrap();
            mark_add(&g, &format!("{root}/small"), FAN_PRE_ACCESS | FAN_EVENT_ON_CHILD, 0).unwrap();
        }
        "sb" => mark_add(&g, root, FAN_PRE_ACCESS, FAN_MARK_FILESYSTEM).unwrap(),
        "other" => mark_add(&g, &format!("{root}/other/o.bin"), FAN_PRE_ACCESS, 0).unwrap(),
        _ => panic!("mode"),
    }
    let setup_ms = (now_ns() - t0) as f64 / 1e6;
    let count = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (c2, s2) = (count.clone(), stop.clone());
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) if l == "snap" => {
                    println!("events={}", c2.load(Ordering::SeqCst));
                    let _ = std::io::stdout().flush();
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        s2.store(true, Ordering::SeqCst);
    });
    println!("READY setup_ms={:.1}", setup_ms);
    let _ = std::io::stdout().flush();
    let mut buf = vec![0u8; 1 << 16];
    while !stop.load(Ordering::Relaxed) {
        let mut p = libc::pollfd { fd: g.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        let r = unsafe { libc::poll(&mut p, 1, 100) };
        if r <= 0 {
            continue;
        }
        // raw batch loop (avoid per-event allocations beyond the parser)
        let evs = match fanotify_read(g.as_fd(), &mut buf) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for ev in evs {
            if ev.fd >= 0 {
                let _ = fanotify_respond(g.as_fd(), ev.fd, FAN_ALLOW);
                close_raw(ev.fd);
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

struct Handler {
    ch: Child,
    out: BufReader<std::process::ChildStdout>,
    inp: std::process::ChildStdin,
}
impl Handler {
    fn start(mode: &str, root: &str) -> Handler {
        let mut ch = Command::new("/proc/self/exe").args(["h-e6handler", mode, root]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
        let inp = ch.stdin.take().unwrap();
        let mut out = BufReader::new(ch.stdout.take().unwrap());
        let mut l = String::new();
        out.read_line(&mut l).unwrap();
        println!("   handler up: {}", l.trim());
        Handler { ch, out, inp }
    }
    fn snap(&mut self) -> u64 {
        writeln!(self.inp, "snap").unwrap();
        self.inp.flush().unwrap();
        let mut l = String::new();
        self.out.read_line(&mut l).unwrap();
        l.trim().trim_start_matches("events=").parse().unwrap()
    }
    fn stop(mut self) {
        drop(self.inp);
        let _ = self.ch.wait();
    }
}

struct Res {
    ops: u64,
    secs: f64,
    lat: Vec<u32>,
}
fn pct(v: &mut Vec<u32>, p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort();
    v[((v.len() - 1) as f64 * p) as usize] as f64
}

/// pread loop on an open fd for `secs`, `nthreads` threads sharing the fd path (each thread own buffer).
fn pread_loop(fd: i32, bs: usize, secs: f64, nthreads: usize) -> Res {
    let nblocks = HOT_SIZE / bs;
    let mut hs = vec![];
    let t0 = now_ns();
    for t in 0..nthreads {
        hs.push(thread::spawn(move || {
            let mut buf = vec![0u8; bs];
            let mut rng = Rng(0x9E37 + t as u64 * 7919);
            let mut ops = 0u64;
            let mut lat = vec![];
            let deadline = now_ns() + (secs * 1e9) as u64;
            loop {
                for i in 0..64 {
                    let off = (rng.below(nblocks as u64) as usize * bs) as i64;
                    if i % 8 == 0 {
                        let a = now_ns();
                        let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, bs, off) };
                        let b = now_ns();
                        assert!(n == bs as isize, "pread {n} errno {}", errno());
                        if lat.len() < 400_000 {
                            lat.push((b - a) as u32);
                        }
                    } else {
                        let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, bs, off) };
                        assert!(n == bs as isize);
                    }
                    ops += 1;
                }
                if now_ns() > deadline {
                    break;
                }
            }
            (ops, lat)
        }));
    }
    let mut ops = 0;
    let mut lat = vec![];
    for h in hs {
        let (o, l) = h.join().unwrap();
        ops += o;
        lat.extend(l);
    }
    Res { ops, secs: (now_ns() - t0) as f64 / 1e9, lat }
}

fn small_loop(root: &str) -> (f64, u64) {
    let mut buf = vec![0u8; 8192];
    let t0 = now_ns();
    let mut files = 0u64;
    for i in 0..NSMALL {
        let fd = open_(&format!("{root}/small/s{i:05}"), libc::O_RDONLY);
        loop {
            let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut _, 8192) };
            if n <= 0 {
                break;
            }
        }
        close_(fd);
        files += 1;
    }
    ((now_ns() - t0) as f64 / 1e9, files)
}

fn cat_small(root: &str) -> f64 {
    let args: Vec<String> = (0..NSMALL).map(|i| format!("{root}/small/s{i:05}")).collect();
    let t0 = now_ns();
    let st = Command::new("cat").args(&args).stdout(Stdio::null()).status().unwrap();
    assert!(st.success());
    (now_ns() - t0) as f64 / 1e9
}

fn med(v: &mut Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

pub fn run(a: &[&str]) {
    let root = a.get(0).copied().unwrap_or("/cache/exp-hsm/e6");
    let secs: f64 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(3.0);
    let only: Option<&str> = a.get(2).copied();
    println!("=== E6 setup: hot file {} MiB, {} small files of 4 KiB under {root}", HOT_SIZE >> 20, NSMALL);
    if stat_(&format!("{root}/small/s09999")).is_none() {
        let _ = std::fs::remove_dir_all(root);
        std::fs::create_dir_all(format!("{root}/hot")).unwrap();
        std::fs::create_dir_all(format!("{root}/small")).unwrap();
        std::fs::create_dir_all(format!("{root}/other")).unwrap();
        make_file(&format!("{root}/hot/hot.bin"), HOT_SIZE as u64, 1, true, 1_600_000_000);
        make_file(&format!("{root}/other/o.bin"), 1 << 20, 2, true, 1_600_000_000);
        for i in 0..NSMALL {
            make_file(&format!("{root}/small/s{i:05}"), 4096, 3, true, 1_600_000_000);
        }
        unsafe { libc::sync() };
    }
    // warm page cache
    let _ = std::fs::read(format!("{root}/hot/hot.bin")).unwrap();
    for i in 0..NSMALL {
        let _ = std::fs::read(format!("{root}/small/s{i:05}"));
    }
    let hot = format!("{root}/hot/hot.bin");
    let scenarios = ["none", "inode", "parent", "sb", "other", "premark"];
    for sc in scenarios {
        if let Some(o) = only {
            if o != sc {
                continue;
            }
        }
        println!("=== scenario {sc}");
        let mut pre_fd = -1;
        if sc == "premark" {
            pre_fd = open_(&hot, libc::O_RDONLY); // opened BEFORE the mark exists
        }
        let mut h = if sc == "none" { None } else { Some(Handler::start(sc, root)) };
        let mut snap = |h: &mut Option<Handler>| h.as_mut().map(|x| x.snap()).unwrap_or(0);
        let fd = if sc == "premark" { pre_fd } else { open_(&hot, libc::O_RDONLY) }; // opened AFTER mark => HSM mode
        for (label, bs, nth) in [("pread4K", 4096usize, 1usize), ("pread4K-2thr", 4096, 2), ("read1M", 1 << 20, 1)] {
            if label == "pread4K-2thr" && !(sc == "none" || sc == "inode") {
                continue;
            }
            let e0 = snap(&mut h);
            let mut r = pread_loop(fd, bs, secs, nth);
            let e1 = snap(&mut h);
            let ev = e1 - e0;
            println!(
                "E6|{sc}|{label}|ops={} secs={:.2} ops_s={:.0} MiB_s={:.0} mean_ns={:.0} p50_ns={:.0} p99_ns={:.0} events={} events_s={:.0} events_per_op={:.2}",
                r.ops,
                r.secs,
                r.ops as f64 / r.secs,
                r.ops as f64 * bs as f64 / r.secs / 1048576.0,
                r.lat.iter().map(|&x| x as f64).sum::<f64>() / r.lat.len() as f64,
                pct(&mut r.lat, 0.5),
                pct(&mut r.lat, 0.99),
                ev,
                ev as f64 / r.secs,
                ev as f64 / r.ops as f64
            );
        }
        if sc != "premark" {
            let mut t = vec![];
            let e0 = snap(&mut h);
            for _ in 0..5 {
                t.push(small_loop(root).0);
            }
            let ev = snap(&mut h) - e0;
            let m = med(&mut t);
            println!("E6|{sc}|small10k-inproc|median_s={:.4} per_file_us={:.2} events_total_5reps={} events_per_file={:.2}", m, m / NSMALL as f64 * 1e6, ev, ev as f64 / (5 * NSMALL) as f64);
            let mut t = vec![];
            let e0 = snap(&mut h);
            for _ in 0..5 {
                t.push(cat_small(root));
            }
            let ev = snap(&mut h) - e0;
            let m = med(&mut t);
            println!("E6|{sc}|cat10k|median_s={:.4} per_file_us={:.2} events_total_5reps={} events_per_file={:.2}", m, m / NSMALL as f64 * 1e6, ev, ev as f64 / (5 * NSMALL) as f64);
        }
        close_(fd);
        if let Some(hh) = h {
            hh.stop();
        }
    }
}
