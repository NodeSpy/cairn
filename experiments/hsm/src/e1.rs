use crate::sys::*;
use crate::util::*;
use std::os::fd::*;
use std::sync::atomic::*;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Rec {
    t: u64,
    pid: i32,
    mask: u64,
    range: Option<(u64, u64)>,
    name: String,
}

const MARK: u64 = FAN_PRE_ACCESS;

pub fn run(a: &[&str]) {
    let dir = a.get(0).copied().unwrap_or("/cache/exp-hsm/e1");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let g = new_group(0);
    let stop = Arc::new(AtomicBool::new(false));
    let log: Arc<Mutex<Vec<Rec>>> = Arc::new(Mutex::new(vec![]));
    let l2 = log.clone();
    let h = spawn_handler(g.clone(), stop.clone(), move |ev| {
        let name = ev_name(ev.fd);
        l2.lock().unwrap().push(Rec { t: now_ns(), pid: ev.pid, mask: ev.mask, range: range_of(ev), name: name.clone() });
        if name.starts_with("denyplain") {
            return FAN_DENY;
        }
        if name.starts_with("deny") {
            return FAN_DENY | ((libc::EIO as u32) << 24);
        }
        let st = fstat_(ev.fd);
        if st.st_blocks == 0 && st.st_size > 0 {
            if name.starts_with("part") {
                fill_fd(ev.fd, 1, st.st_size as u64, Some(4096));
            } else {
                fill_fd(ev.fd, 1, st.st_size as u64, None);
            }
        }
        if name.contains("touchboth") {
            let ts = [libc::timespec { tv_sec: 1_600_000_000, tv_nsec: 0 }, libc::timespec { tv_sec: 1_600_000_000, tv_nsec: 0 }];
            unsafe { libc::futimens(ev.fd, ts.as_ptr()) };
        } else if name.contains("touch") {
            set_mtime_sec(ev.fd, 1_600_000_000);
        }
        FAN_ALLOW
    });

    let mkph = |name: &str, size: u64| -> String {
        let p = format!("{dir}/{name}");
        make_file(&p, size, 1, false, 1_500_000_000);
        mark_add(&g, &p, MARK, 0).expect("mark");
        let st = stat_(&p).unwrap();
        assert!(st.st_blocks == 0 && st.st_size as u64 == size);
        p
    };
    let evs_for = |name: &str| -> Vec<Rec> { log.lock().unwrap().iter().filter(|r| r.name == name).cloned().collect() };
    let fmt_ev = |r: &Rec, t0: u64| format!("[t+{:.2}ms pid={} mask={:#x} range={:?}]", (r.t as f64 - t0 as f64) / 1e6, r.pid, r.mask, r.range);
    let kv = |s: &str, k: &str| -> String {
        s.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.to_string())).unwrap_or_default()
    };
    let size_a = 1_048_576 + 777u64;

    println!("=== E1.A basic fill through event fd, reader = cat-like process");
    let p = mkph("a1", size_a);
    let (out, ino) = with_inotify(dir, || run_self(&["h-read", &p]));
    let exp = format!("{:x}", fnv_pat(1, size_a));
    println!("reader: {}", out.trim());
    println!("expected fnv={} -> {}", exp, if kv(&out, "fnv") == exp && kv(&out, "bytes") == size_a.to_string() && kv(&out, "zeros") == "0" { "PASS bytes correct" } else { "FAIL" });
    for r in evs_for("a1") {
        println!("event {}", fmt_ev(&r, 0));
    }
    println!("blocks after fill={} (size {})", stat_(&p).unwrap().st_blocks, size_a);
    println!("inotifywait during first read: {:?}", ino);
    println!("IN_MODIFY seen: {}", ino.iter().any(|l| l.contains("MODIFY")));
    let m = stat_(&p).unwrap();
    println!("mtime after fill (no restore) = {} (was 1500000000)", m.st_mtime);
    let n0 = evs_for("a1").len();
    let (out2, ino2) = with_inotify(dir, || run_self(&["h-read", &p]));
    println!("second read (still marked, already filled): {} ; new events={} ; inotify={:?}", out2.trim(), evs_for("a1").len() - n0, ino2);
    // cat for real
    let p3 = mkph("a2", 300_000);
    let o = std::process::Command::new("cat").arg(&p3).output().unwrap();
    let want: Vec<u8> = pat_table(1, 300_000);
    println!("real /usr/bin/cat of a2: {} bytes, equal={}", o.stdout.len(), o.stdout == want);
    // control: inotifywait sees ordinary modify
    let (_, ino_c) = with_inotify(dir, || {
        std::fs::write(format!("{dir}/ctrl"), b"x").unwrap();
    });
    println!("control (plain write by another process) inotify: {:?}", ino_c);

    println!("=== E1.B futimens on the event fd (touch1)");
    let p = mkph("touch1", 100_000);
    let (out, ino) = with_inotify(dir, || run_self(&["h-read", &p]));
    println!("reader: {}", out.trim());
    println!("inotify: {:?}", ino);
    println!("mtime after fill+futimens = {} (restored target 1600000000)", stat_(&p).unwrap().st_mtime);

    {
        let p = mkph("touchboth1", 100_000);
        let (out, ino) = with_inotify(dir, || run_self(&["h-read", &p]));
        println!("touchboth1 (futimens on event fd with BOTH atime+mtime): reader ok={} ; inotify: {:?}", out.contains("zeros=0"), ino);
        // regular fd controls on an unmarked file
        let cp = format!("{dir}/ctl2");
        std::fs::write(&cp, b"x").unwrap();
        let fd = open_(&cp, libc::O_RDWR);
        let (_, i1) = with_inotify(dir, || set_mtime_sec(fd, 1_600_000_001));
        println!("control regular fd futimens(atime=OMIT, mtime) : {:?}", i1);
        let (_, i2) = with_inotify(dir, || {
            let ts = [libc::timespec { tv_sec: 1_600_000_002, tv_nsec: 0 }, libc::timespec { tv_sec: 1_600_000_002, tv_nsec: 0 }];
            unsafe { libc::futimens(fd, ts.as_ptr()) };
        });
        println!("control regular fd futimens(atime, mtime both)  : {:?}", i2);
        let (_, i3) = with_inotify(dir, || set_mtime_path_nofollow(std::path::Path::new(&cp), 1_600_000_003 * 1_000_000_000).unwrap());
        println!("control utimensat(path, atime=OMIT, mtime)      : {:?}", i3);
        let (_, i4) = with_inotify(dir, || { unsafe { libc::fchmod(fd, 0o600) }; });
        println!("control fchmod                                  : {:?}", i4);
        let (_, i5) = with_inotify(dir, || { unsafe { libc::ftruncate(fd, 1) }; });
        println!("control ftruncate(same size)                    : {:?}", i5);
    }

    println!("=== E1.C FAN_DENY");
    for nm in ["deny1", "denyplain1"] {
        let p = mkph(nm, 100_000);
        let out = run_self(&["h-read", &p]);
        println!("{nm}: reader: {} ; blocks={}", out.trim(), stat_(&p).unwrap().st_blocks);
        let o = std::process::Command::new("cat").arg(&p).output().unwrap();
        println!("{nm}: cat stderr: {}", String::from_utf8_lossy(&o.stderr).trim());
    }

    println!("=== E1.D mmap of placeholder");
    let p = mkph("mm1", 1 << 20);
    log.lock().unwrap().clear();
    let out = run_self(&["h-mmap", &p, "0", &(1 << 20).to_string()]);
    println!("mmap reader: {}", out.trim());
    let (t0, t1, t2): (u64, u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap(), kv(&out, "t2").parse().unwrap());
    for r in evs_for("mm1") {
        let when = if r.t >= t0 && r.t <= t1 + 1_000_000 { "DURING mmap()" } else if r.t > t1 && r.t <= t2 { "during page-touch phase" } else { "outside" };
        println!("event {} => {}", fmt_ev(&r, t0), when);
    }
    println!("bytes correct: {}", kv(&out, "fnv") == format!("{:x}", fnv_pat(1, 1 << 20)));
    // partial-fill: event only fills page 0, pages touched later are zeros w/o new events
    let p = mkph("part1", 1 << 20);
    let out = run_self(&["h-mmap", &p, "0", &(1 << 20).to_string()]);
    println!("partial-fill (handler fills only first 4 KiB): {}", out.trim());
    println!("events for part1 = {} (expect 1, no fault-time events)", evs_for("part1").len());
    // offset mmap
    let p = mkph("mm2", 1 << 20);
    let out = run_self(&["h-mmap", &p, "8192", "16384"]);
    println!("mmap off=8192 len=16384: {}", out.trim());
    for r in evs_for("mm2") {
        println!("event {}", fmt_ev(&r, 0));
    }
    // MAP_PRIVATE+PROT_READ & pread range
    let p = mkph("rd1", 1 << 20);
    let out = run_self(&["h-pread", &p, "12345", "100"]);
    println!("pread off=12345 len=100: {}", out.trim());
    for r in evs_for("rd1") {
        println!("event {}", fmt_ev(&r, 0));
    }
    let p = mkph("rd2", 1 << 20);
    let out = run_self(&["h-pread", &p, "0", "1048576"]);
    println!("pread 1MiB: {}", out.trim());
    for r in evs_for("rd2") {
        println!("event {}", fmt_ev(&r, 0));
    }

    println!("=== E1.E O_TRUNC open of placeholder");
    let p = mkph("tr1", 1 << 20);
    let out = run_self(&["h-otrunc", &p]);
    println!("otrunc helper: {}", out.trim());
    let o0: u64 = kv(&out, "open0").parse().unwrap();
    let w0: u64 = kv(&out, "write0").parse().unwrap();
    for r in evs_for("tr1") {
        let when = if r.t < w0 { "BEFORE write (i.e. at open)" } else { "at write" };
        println!("event {} => {}", fmt_ev(&r, o0), when);
    }
    println!("events total for tr1 = {}", evs_for("tr1").len());
    let st = stat_(&p).unwrap();
    println!("final size={} blocks={}", st.st_size, st.st_blocks);
    for (nm, sz) in [("tr2", "0"), ("tr3", "524288")] {
        let p = mkph(nm, 1 << 20);
        let out = run_self(&["h-truncp", &p, sz]);
        println!("truncate(2) path -> {sz}: {} ; events={} {:?}", out.trim(), evs_for(nm).len(), evs_for(nm).iter().map(|r| r.range).collect::<Vec<_>>());
        println!("   size={} blocks={}", stat_(&p).unwrap().st_size, stat_(&p).unwrap().st_blocks);
    }

    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
}
