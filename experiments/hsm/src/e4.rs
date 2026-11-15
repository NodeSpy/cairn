use crate::sys::*;
use crate::util::*;
use std::os::fd::*;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::*;
use std::sync::{Arc, Mutex};

const SIZE: usize = 262144;
const NF: u32 = 16;
const MARK: u64 = FAN_PRE_ACCESS;
fn mtime_of(k: u32) -> i64 {
    1_600_000_000 + k as i64
}

#[derive(Default)]
struct Evict {
    ino: u64,
    active: bool,
    aborted: bool,
    punched: bool,
    v1: bool,
}
#[derive(Default)]
struct Stats {
    events: AtomicU64,
    self_events: AtomicU64,
    fills: AtomicU64,
    punches: AtomicU64,
    interlopers: AtomicU64,
    interlopers_before_punch: AtomicU64,
}

struct Ctx {
    g: Arc<OwnedFd>,
    ev: Arc<Mutex<Evict>>,
    l: Vec<i32>,
    marked: Vec<bool>,
    paths: Vec<String>,
    widen_us: u64,
    rest_us: u64,
    rng: Rng,
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum Out {
    Evicted,
    Refused(i32),
    Aborted,
    NoLease(i32),
}

fn self_trigger(path: &str) {
    let fd = open_(path, libc::O_RDONLY);
    assert!(fd >= 0, "self open {path}: {}", errno());
    let mut b = [0u8; 1];
    unsafe { libc::pread(fd, b.as_mut_ptr() as *mut _, 1, 0) };
    close_(fd);
}

impl Ctx {
    /// refill (if holes), take read lease, drop mark
    fn restore(&mut self, k: usize) -> Result<(), i32> {
        let l = self.l[k];
        if fstat_(l).st_blocks == 0 {
            self_trigger(&self.paths[k]);
        }
        if getlease(l) != libc::F_RDLCK {
            // F_SETSIG is reset when a lease is removed; must be re-armed every time.
            set_sig(bfd(l), lease_signal()).unwrap();
            if lease(l, libc::F_RDLCK) < 0 {
                return Err(errno());
            }
        }
        if self.marked[k] {
            mark_rm(&self.g, &self.paths[k], MARK, 0).unwrap();
            self.marked[k] = false;
        }
        Ok(())
    }
    fn evict(&mut self, k: usize) -> Out {
        let l = self.l[k];
        mark_add(&self.g, &self.paths[k], MARK, 0).unwrap();
        self.marked[k] = true;
        if lease(l, libc::F_WRLCK) < 0 {
            let e = errno();
            mark_rm(&self.g, &self.paths[k], MARK, 0).unwrap();
            self.marked[k] = false;
            return Out::Refused(e);
        }
        {
            let mut ev = self.ev.lock().unwrap();
            ev.ino = fstat_(l).st_ino as u64;
            ev.active = true;
            ev.aborted = false;
            ev.punched = false;
        }
        lease(l, libc::F_UNLCK);
        if self.widen_us > 0 {
            // simulate a descheduled/slow daemon between dropping the lease and the self-triggered punch
            let w = self.rng.below(self.widen_us);
            usleep(w);
        }
        self_trigger(&self.paths[k]);
        let mut ev = self.ev.lock().unwrap();
        ev.active = false;
        if ev.aborted && ev.v1 {
            Out::Aborted
        } else {
            Out::Evicted
        }
    }
}

fn kv(s: &str, k: &str) -> String {
    s.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.to_string())).unwrap_or_default()
}

pub fn run(a: &[&str]) {
    // e4 <dir> <iters> <v0|v1> <widen_us> <rest_us> <readers> <writers> <holders> <label>
    if a.get(0) == Some(&"deterministic") {
        return deterministic(a.get(1).copied().unwrap_or("/cache/exp-hsm/e4d"));
    }
    let dir = a.get(0).copied().unwrap_or("/cache/exp-hsm/e4");
    let iters: usize = a.get(1).map(|s| s.parse().unwrap()).unwrap_or(3000);
    let v1 = a.get(2).copied().unwrap_or("v1") == "v1";
    let widen_us: u64 = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(0);
    let rest_us: u64 = a.get(4).map(|s| s.parse().unwrap()).unwrap_or(500);
    let nread: usize = a.get(5).map(|s| s.parse().unwrap()).unwrap_or(4);
    let nwrite: usize = a.get(6).map(|s| s.parse().unwrap()).unwrap_or(0);
    let nhold: usize = a.get(7).map(|s| s.parse().unwrap()).unwrap_or(0);
    let label = a.get(8).copied().unwrap_or("run");
    println!("=== E4 run '{label}': iters={iters} variant={} widen_us={widen_us} rest_us={rest_us} readers={nread} writers={nwrite} holders={nhold}", if v1 { "v1(abort on interloper)" } else { "v0(naive)" });
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let stopf = format!("{dir}/../STOP-{label}");
    let _ = std::fs::remove_file(&stopf);
    let mut paths = vec![];
    for k in 0..NF {
        let p = format!("{dir}/f{k}");
        make_file(&p, SIZE as u64, k, true, mtime_of(k));
        paths.push(p);
    }
    let _br = spawn_breaker();
    let g = new_group(0);
    let stop = Arc::new(AtomicBool::new(false));
    let evst = Arc::new(Mutex::new(Evict { v1, ..Default::default() }));
    let stats = Arc::new(Stats::default());
    let (e2, s2) = (evst.clone(), stats.clone());
    let pid = unsafe { libc::getpid() };
    let tables: Vec<Vec<u8>> = (0..NF).map(|k| pat_table(k, SIZE)).collect();
    let h = spawn_handler(g.clone(), stop.clone(), move |ev| {
        let st = fstat_(ev.fd);
        let ino = st.st_ino as u64;
        s2.events.fetch_add(1, Ordering::Relaxed);
        let selfev = ev.pid == pid;
        if selfev {
            s2.self_events.fetch_add(1, Ordering::Relaxed);
        }
        let name = ev_name(ev.fd);
        let k: u32 = name[1..].parse().unwrap();
        let mut punch = false;
        {
            let mut es = e2.lock().unwrap();
            if es.active && es.ino == ino {
                if selfev {
                    punch = !(es.v1 && es.aborted);
                } else {
                    es.aborted = true;
                    s2.interlopers.fetch_add(1, Ordering::Relaxed);
                    if !es.punched {
                        s2.interlopers_before_punch.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        if punch {
            punch_hole(bfd(ev.fd), SIZE as u64).unwrap();
            e2.lock().unwrap().punched = true;
            set_mtime_sec(ev.fd, mtime_of(k));
            s2.punches.fetch_add(1, Ordering::Relaxed);
            return FAN_ALLOW;
        }
        if st.st_blocks == 0 && st.st_size > 0 {
            pwrite_all(bfd(ev.fd), &tables[k as usize], 0).unwrap();
            set_mtime_sec(ev.fd, mtime_of(k));
            s2.fills.fetch_add(1, Ordering::Relaxed);
        }
        FAN_ALLOW
    });
    let mut ls = vec![];
    for p in &paths {
        ls.push(lease_open(p));
    }
    let mut ctx = Ctx { g: g.clone(), ev: evst.clone(), l: ls, marked: vec![false; NF as usize], paths: paths.clone(), widen_us, rest_us, rng: Rng(0xdeadbeef12345) };

    let mut kids: Vec<(String, Child)> = vec![];
    let szs = SIZE.to_string();
    let nfs = NF.to_string();
    let mut seed = 1;
    for _ in 0..nread {
        seed += 1;
        kids.push(("reader".into(), Command::new("/proc/self/exe").args(["h-e4reader", dir, &nfs, &szs, &stopf, &seed.to_string(), "all"]).stdout(Stdio::piped()).spawn().unwrap()));
    }
    for _ in 0..nhold {
        seed += 1;
        kids.push(("holder".into(), Command::new("/proc/self/exe").args(["h-e4reader", dir, &nfs, &szs, &stopf, &seed.to_string(), "hold"]).stdout(Stdio::piped()).spawn().unwrap()));
    }
    for _ in 0..nwrite {
        seed += 1;
        kids.push(("writer".into(), Command::new("/proc/self/exe").args(["h-e4writer", dir, &nfs, &szs, &stopf, &seed.to_string()]).stdout(Stdio::piped()).spawn().unwrap()));
    }
    // watchdog
    let progress = Arc::new(AtomicU64::new(0));
    let pg = progress.clone();
    std::thread::spawn(move || {
        let mut last = u64::MAX;
        loop {
            msleep(30_000);
            let cur = pg.load(Ordering::Relaxed);
            if cur == last {
                eprintln!("WATCHDOG: no progress for 30s at iter {cur}; aborting");
                std::process::exit(3);
            }
            last = cur;
        }
    });

    let t_start = now_ns();
    let mut tally = std::collections::BTreeMap::<String, u64>::new();
    let mut rng = Rng(777);
    for i in 0..iters {
        progress.store(i as u64, Ordering::Relaxed);
        let k = rng.below(NF as u64) as usize;
        match ctx.restore(k) {
            Err(e) => {
                *tally.entry(format!("restore-lease-failed:{}", errname(e))).or_default() += 1;
                usleep(200);
                continue;
            }
            Ok(()) => {}
        }
        if ctx.rest_us > 0 {
            let r = ctx.rng.below(ctx.rest_us);
            usleep(r);
        }
        let o = ctx.evict(k);
        let key = match o {
            Out::Evicted => "evicted".to_string(),
            Out::Aborted => "aborted-by-interloper".to_string(),
            Out::Refused(e) => format!("refused({})", errname(e)),
            Out::NoLease(_) => "no-lease".to_string(),
        };
        *tally.entry(key).or_default() += 1;
        if o == Out::Evicted {
            let st = fstat_(ctx.l[k]);
            if st.st_blocks != 0 {
                *tally.entry("evicted-but-blocks-nonzero(raced-by-writer-refill)".into()).or_default() += 1;
            }
        }
    }
    let el = (now_ns() - t_start) as f64 / 1e9;
    // stop children
    std::fs::write(&stopf, b"").unwrap();
    let mut tot_bad = 0u64;
    let mut tot_zero = 0u64;
    let mut tot_reads = 0u64;
    for (role, ch) in kids {
        let out = ch.wait_with_output().unwrap();
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        println!("  {role}: {s}");
        if role != "writer" {
            tot_reads += kv(&s, "reads").parse::<u64>().unwrap_or(0);
            tot_bad += kv(&s, "bad").parse::<u64>().unwrap_or(0);
            tot_zero += kv(&s, "zero_bad").parse::<u64>().unwrap_or(0);
        }
    }
    // final verification: every file fully readable and correct, mtime restored
    let mut final_bad = 0;
    let mut mt_bad = 0;
    for k in 0..NF {
        let out = run_self(&["h-read", &paths[k as usize]]);
        if kv(&out, "fnv") != format!("{:x}", fnv_pat(k, SIZE as u64)) {
            final_bad += 1;
        }
        if stat_(&paths[k as usize]).unwrap().st_mtime != mtime_of(k) {
            mt_bad += 1;
        }
    }
    println!("  elapsed {:.1}s, {:.0} eviction cycles/s", el, iters as f64 / el);
    for (k, v) in &tally {
        println!("  outcome {k}: {v}");
    }
    println!(
        "  handler: events={} self_events={} fills={} punches={} other-process events on the victim inode inside the window={} (of which handled BEFORE the punch={})",
        stats.events.load(Ordering::Relaxed),
        stats.self_events.load(Ordering::Relaxed),
        stats.fills.load(Ordering::Relaxed),
        stats.punches.load(Ordering::Relaxed),
        stats.interlopers.load(Ordering::Relaxed),
        stats.interlopers_before_punch.load(Ordering::Relaxed)
    );
    println!("  RESULT: reader checks={} bad_reads={} (zero_bytes_seen={}) ; final full-file verify bad files={} ; mtime-not-restored files (valid only w/o writers)={}", tot_reads, tot_bad, tot_zero, final_bad, mt_bad);
    println!("  VERDICT: {}", if tot_bad == 0 && final_bad == 0 { "PASS (no bad bytes)" } else { "FAIL (bad bytes observed)" });
    stop.store(true, Ordering::SeqCst);
    let _ = h.join();
    let _ = std::fs::remove_file(&stopf);
}

fn deterministic(dir: &str) {
    println!("=== E4.D deterministic refusal cases (single eviction attempt of one file; holder opened BEFORE the mark)");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let _br = spawn_breaker();
    let g = new_group(0);
    let stop = Arc::new(AtomicBool::new(false));
    let evst = Arc::new(Mutex::new(Evict { v1: true, ..Default::default() }));
    let e2 = evst.clone();
    let pid = unsafe { libc::getpid() };
    let h = spawn_handler(g.clone(), stop.clone(), move |ev| {
        let st = fstat_(ev.fd);
        let selfev = ev.pid == pid;
        let mut punch = false;
        {
            let mut es = e2.lock().unwrap();
            if es.active && es.ino == st.st_ino as u64 {
                if selfev {
                    punch = !es.aborted;
                } else {
                    es.aborted = true;
                }
            }
        }
        if punch {
            punch_hole(bfd(ev.fd), SIZE as u64).unwrap();
            return FAN_ALLOW;
        }
        if st.st_blocks == 0 && st.st_size > 0 {
            fill_fd(ev.fd, 0, st.st_size as u64, None);
        }
        FAN_ALLOW
    });
    let cases = [
        ("none", true),
        ("closed", true),
        ("opath", true),
        ("rdonly", false),
        ("rdwr", false),
        ("wronly", false),
        ("mmapr-closed", false),
        ("mmapp-closed", false),
        ("mmapw-closed", false),
    ];
    for (kind, expect_evict) in cases {
        let p = format!("{dir}/{kind}");
        make_file(&p, SIZE as u64, 0, true, 1_600_000_000);
        let l = lease_open(&p);
        let mut ctx = Ctx { g: g.clone(), ev: evst.clone(), l: vec![l], marked: vec![false], paths: vec![p.clone()], widen_us: 0, rest_us: 0, rng: Rng(5) };
        let mut ch = spawn_self(&["h-hold", &p, kind]);
        let _ = wait_line(&mut ch, "READY");
        let out = ctx.evict(0);
        let blocks = fstat_(l).st_blocks;
        let lease_after = lease_name(getlease(l));
        let ok = (out == Out::Evicted) == expect_evict;
        println!("{:14} holder-open-before-mark: outcome={:?} blocks_after={} lease_after={} marked_after={} expected_evict={} => {}", kind, out, blocks, lease_after, ctx.marked[0], expect_evict, if ok { "PASS" } else { "FAIL" });
        finish(ch);
        close_(l);
    }
    // holders opened AFTER the mark (HSM-mode opens), blocked read never issued
    for kind in ["rdonly", "rdwr"] {
        let p = format!("{dir}/post-{kind}");
        make_file(&p, SIZE as u64, 0, true, 1_600_000_000);
        let l = lease_open(&p);
        mark_add(&g, &p, MARK, 0).unwrap();
        let mut ch = spawn_self(&["h-hold", &p, kind]);
        let _ = wait_line(&mut ch, "READY");
        let mut ctx = Ctx { g: g.clone(), ev: evst.clone(), l: vec![l], marked: vec![true], paths: vec![p.clone()], widen_us: 0, rest_us: 0, rng: Rng(5) };
        let out = ctx.evict(0);
        println!("post-mark {kind:6} holder (HSM-mode open, no read yet): outcome={:?} blocks_after={}", out, fstat_(l).st_blocks);
        finish(ch);
        close_(l);
    }
    stop.store(true, Ordering::SeqCst);
    let _ = h.join();
}
