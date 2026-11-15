use crate::sys::*;
use crate::util::*;
use std::os::fd::*;
use std::process::Command;
use std::sync::atomic::*;

fn kv(s: &str, k: &str) -> String {
    s.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.to_string())).unwrap_or_default()
}
fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p) as usize]
}

pub fn run(a: &[&str]) {
    let dir = a.get(0).copied().unwrap_or("/cache/exp-hsm/e2");
    let long = a.contains(&"--long");
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let br = spawn_breaker();
    let f = format!("{dir}/f");
    let reset = |content: &str| -> i32 {
        let _ = std::fs::remove_file(&f);
        let _ = std::fs::remove_file(format!("{f}.renamed"));
        let _ = std::fs::remove_file(format!("{f}.lnk"));
        std::fs::write(&f, content).unwrap();
        lease_open(&f)
    };
    let nbreaks = || br.log.lock().unwrap().len();

    println!("=== E2.A gate with 1000 ms artificial release delay");
    br.delay_us.store(1_000_000, Ordering::SeqCst);
    for op in ["wronly", "rdwr", "append", "creat_trunc", "truncp", "nonblock"] {
        let l = reset("hello");
        let b0 = nbreaks();
        let out = run_self(&["h-op", op, &f]);
        let (t0, t1): (u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap());
        let bl = br.log.lock().unwrap().clone();
        let sig = if bl.len() > b0 { format!("signal after {:.3} ms, ssi_fd={} (lease fd={})", (bl[b0].0 as f64 - t0 as f64) / 1e6, bl[b0].1, l) } else { "NO SIGNAL".into() };
        println!("{op:12} blocked {:8.3} ms err={} ; breaks={} ; {} ; lease now={}", (t1 - t0) as f64 / 1e6, kv(&out, "err"), bl.len() - b0, sig, lease_name(getlease(l)));
        close_(l);
        msleep(50);
    }
    // nonblock: second try after release
    println!("--- O_NONBLOCK follow-up: re-open after lease released");
    msleep(1200);
    let out = run_self(&["h-op", "nonblock", &f]);
    println!("nonblock open after release: {}", out.trim());

    println!("=== E2.B ops expected NOT to break the lease (delay irrelevant)");
    for op in ["rdonly", "rename", "unlink", "chmod", "utimes", "link"] {
        let l = reset("hello");
        let b0 = nbreaks();
        let out = run_self(&["h-op", op, &f]);
        let (t0, t1): (u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap());
        msleep(100);
        println!("{op:12} took {:8.3} ms err={} ; breaks={} ; lease now={}", (t1 - t0) as f64 / 1e6, kv(&out, "err"), nbreaks() - b0, lease_name(getlease(l)));
        close_(l);
    }
    // rename another file over leased path, sed -i, cp, shell redirects
    {
        let l = reset("hello world");
        let ino0 = stat_(&f).unwrap().st_ino;
        let b0 = nbreaks();
        let t = now_ns();
        let st = Command::new("sed").args(["-i", "s/hello/HELLO/", &f]).status().unwrap();
        let el = (now_ns() - t) as f64 / 1e6;
        msleep(100);
        let content = std::fs::read_to_string(&f).unwrap();
        let ino1 = stat_(&f).unwrap().st_ino;
        println!("sed -i: status={} took {:.1} ms ; breaks={} ; lease on old inode={} ; path inode changed={} ; new content={:?} ; old inode (via lease fd) size={} nlink={}", st, el, nbreaks() - b0, lease_name(getlease(l)), ino0 != ino1, content.trim(), fstat_(l).st_size, fstat_(l).st_nlink);
        close_(l);
    }
    {
        let l = reset("hello world");
        let b0 = nbreaks();
        std::fs::write(format!("{dir}/tmp"), "new").unwrap();
        let t = now_ns();
        std::fs::rename(format!("{dir}/tmp"), &f).unwrap();
        println!("rename(tmp, leased path): took {:.3} ms ; breaks={} ; lease={}", (now_ns() - t) as f64 / 1e6, nbreaks() - b0, lease_name(getlease(l)));
        close_(l);
    }
    for (label, args) in [("sh: echo >> f (O_APPEND)", vec!["-c", "echo more >> $0"]), ("sh: echo > f (O_TRUNC)", vec!["-c", "echo more > $0"]), ("cp tmpfile f (O_TRUNC)", vec!["-c", "echo more > $0.src; cp $0.src $0"]), ("truncate -s 0 f", vec!["-c", "truncate -s 0 $0"])] {
        let l = reset("hello world");
        let b0 = nbreaks();
        let t = now_ns();
        Command::new("sh").arg(args[0]).arg(args[1]).arg(&f).status().unwrap();
        let el = (now_ns() - t) as f64 / 1e6;
        println!("{label}: took {:.1} ms ; breaks={} ; lease={}", el, nbreaks() - b0, lease_name(getlease(l)));
        close_(l);
        msleep(50);
    }

    println!("=== E2.C multiple leased files: ssi_fd identification and concurrent breaks");
    msleep(2500); // let the breaker drain stale signals (see finding: stale ssi_fd after fd reuse)
    br.delay_us.store(200_000, Ordering::SeqCst);
    let names = ["m0", "m1", "m2"];
    let mut fds = vec![];
    for n in names {
        let p = format!("{dir}/{n}");
        std::fs::write(&p, "x").unwrap();
        fds.push(lease_open(&p));
    }
    println!("  leases after setup: {:?}", fds.iter().map(|&f| lease_name(getlease(f))).collect::<Vec<_>>());
    let b0 = nbreaks();
    let c1 = Command::new("/proc/self/exe").args(["h-op", "wronly", &format!("{dir}/m1")]).output().unwrap();
    let bl = br.log.lock().unwrap().clone();
    println!("opened m1 (fd {}) for write: ssi_fd={:?}, correct={}", fds[1], bl[b0..].iter().map(|x| x.1).collect::<Vec<_>>(), bl.len() == b0 + 1 && bl[b0].1 == fds[1]);
    println!("  m1 helper out: {}", String::from_utf8_lossy(&c1.stdout).trim());
    println!("  leases before concurrent test: {:?}", fds.iter().map(|&f| lease_name(getlease(f))).collect::<Vec<_>>());
    let b0 = nbreaks();
    let h0 = Command::new("/proc/self/exe").args(["h-op", "wronly", &format!("{dir}/m0")]).stdout(std::process::Stdio::piped()).spawn().unwrap();
    let h2 = Command::new("/proc/self/exe").args(["h-op", "wronly", &format!("{dir}/m2")]).stdout(std::process::Stdio::piped()).spawn().unwrap();
    let (o0, o2) = (h0.wait_with_output().unwrap(), h2.wait_with_output().unwrap());
    let bl = br.log.lock().unwrap().clone();
    let mut got: Vec<i32> = bl[b0..].iter().map(|x| x.1).collect();
    got.sort();
    let mut want = vec![fds[0], fds[2]];
    want.sort();
    println!("  helper outputs: m0: {} | m2: {}", String::from_utf8_lossy(&o0.stdout).trim(), String::from_utf8_lossy(&o2.stdout).trim());
    println!("  log: {:?}", bl[b0..].iter().map(|x| (x.0 - bl[b0].0, x.1)).collect::<Vec<_>>());
    println!("concurrent writers on m0,m2: ssi_fds={:?} want={:?} ok={}", got, want, got == want);
    for f in fds {
        close_(f);
    }

    println!("=== E2.D gate latency distribution (release delay 0)");
    br.delay_us.store(0, Ordering::SeqCst);
    let n = 300;
    let mut gate = vec![];
    let mut sigl = vec![];
    for _ in 0..n {
        let l = reset("hello");
        let b0 = nbreaks();
        let out = run_self(&["h-op", "wronly", &f]);
        let (t0, t1): (u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap());
        gate.push((t1 - t0) as f64 / 1e3);
        let bl = br.log.lock().unwrap();
        if bl.len() > b0 {
            sigl.push((bl[b0].0 as f64 - t0 as f64) / 1e3);
        }
        drop(bl);
        close_(l);
    }
    let mut base = vec![];
    for _ in 0..n {
        let out = {
            // unleased baseline: new file without lease
            std::fs::write(format!("{dir}/nl"), "hello").unwrap();
            run_self(&["h-op", "wronly", &format!("{dir}/nl")])
        };
        let (t0, t1): (u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap());
        base.push((t1 - t0) as f64 / 1e3);
    }
    println!("n={} leased open(O_WRONLY) us: p50={:.1} p90={:.1} p99={:.1} max={:.1}", n, pct(&mut gate.clone(), 0.5), pct(&mut gate.clone(), 0.9), pct(&mut gate.clone(), 0.99), pct(&mut gate.clone(), 1.0));
    println!("n={} unleased open(O_WRONLY) us: p50={:.1} p90={:.1} p99={:.1} max={:.1}", n, pct(&mut base.clone(), 0.5), pct(&mut base.clone(), 0.9), pct(&mut base.clone(), 0.99), pct(&mut base.clone(), 1.0));
    println!("signal-arrival-at-daemon latency (from before open() in writer) us: p50={:.1} p99={:.1} (signals={})", pct(&mut sigl.clone(), 0.5), pct(&mut sigl.clone(), 0.99), sigl.len());

    if long {
        println!("=== E2.E daemon never releases (lease-break-time)");
        let bt = std::fs::read_to_string("/proc/sys/fs/lease-break-time").unwrap_or_default();
        println!("fs.lease-break-time = {}", bt.trim());
        br.delay_us.store(120_000_000, Ordering::SeqCst);
        let l = reset("hello");
        let out = run_self(&["h-op", "wronly", &f]);
        let (t0, t1): (u64, u64) = (kv(&out, "t0").parse().unwrap(), kv(&out, "t1").parse().unwrap());
        println!("writer unblocked after {:.2} s err={} ; lease now={}", (t1 - t0) as f64 / 1e9, kv(&out, "err"), lease_name(getlease(l)));
    }
}
