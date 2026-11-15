use crate::sys::*;
use crate::util::*;
use std::collections::HashMap;
use std::os::fd::*;
use std::sync::atomic::*;
use std::sync::{Arc, Mutex};

fn probe(path: &str) -> i32 {
    let fd = open_(path, libc::O_RDONLY);
    if fd < 0 {
        return -errno();
    }
    let r = lease(fd, libc::F_RDLCK);
    let e = if r < 0 { errno() } else { 0 };
    if r == 0 {
        lease(fd, libc::F_UNLCK);
    }
    close_(fd);
    e
}

type Probes = Arc<Mutex<HashMap<String, Vec<(i32, i32)>>>>;

pub fn run(a: &[&str]) {
    let dir = a.get(0).copied().unwrap_or("/cache/exp-hsm/e3");
    let trials: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(500);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let br = spawn_breaker();
    br.delay_us.store(300_000, Ordering::SeqCst);
    let g = new_group(0);
    let stop = Arc::new(AtomicBool::new(false));
    let probes: Probes = Arc::new(Mutex::new(HashMap::new()));
    let pr = probes.clone();
    let kept: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(vec![]));
    let kp = kept.clone();
    let g2 = g.clone();
    let h = spawn_handler(g.clone(), stop.clone(), move |ev| {
        let name = ev_name(ev.fd);
        let path = fd_path(ev.fd).unwrap().to_string_lossy().into_owned();
        let st = fstat_(ev.fd);
        if st.st_blocks == 0 && st.st_size > 0 {
            fill_fd(ev.fd, 1, st.st_size as u64, None);
        }
        let before = probe(&path); // event fd still open O_RDWR (control)
        neuter_fd(ev.fd).unwrap();
        let after = probe(&path);
        pr.lock().unwrap().entry(name.clone()).or_default().push((before, after));
        if name.starts_with("keepB") && after == 0 {
            mark_rm(&g2, &path, FAN_PRE_ACCESS, 0).unwrap();
        }
        if name.starts_with("keep") && after == 0 {
            let fd = open_(&path, libc::O_RDONLY);
            set_sig(bfd(fd), lease_signal()).unwrap();
            let r = lease(fd, libc::F_RDLCK);
            if r == 0 {
                kp.lock().unwrap().push(fd);
            } else {
                close_(fd);
            }
        }
        FAN_ALLOW
    });
    let size = 262144u64;
    let mut counter = 0u32;
    // returns (probes, holder output, reader output)
    let mut case = |kind: &str, external: bool, premark: bool| -> (Vec<(i32, i32)>, String, String) {
        counter += 1;
        let name = format!("c{counter}_{kind}");
        let p = format!("{dir}/{name}");
        make_file(&p, size, 1, false, 1_500_000_000);
        let mut holder = None;
        let mut hmsg = String::new();
        if premark {
            let mut ch = spawn_self(&["h-hold", &p, kind]);
            hmsg = wait_line(&mut ch, "READY");
            holder = Some(ch);
            mark_add(&g, &p, FAN_PRE_ACCESS, 0).unwrap();
        } else {
            mark_add(&g, &p, FAN_PRE_ACCESS, 0).unwrap();
            let mut ch = spawn_self(&["h-hold", &p, kind]);
            hmsg = wait_line(&mut ch, "READY");
            holder = Some(ch);
        }
        let mut rout = String::new();
        if external {
            rout = run_self(&["h-read", &p]);
        }
        if let Some(ch) = holder {
            finish(ch);
        }
        let v = probes.lock().unwrap().get(&name).cloned().unwrap_or_default();
        (v, hmsg, rout)
    };
    let fmt = |v: &[(i32, i32)]| -> String {
        v.iter().map(|(b, a)| format!("[unneutered={} neutered={}]", errname(*b), errname(*a))).collect::<Vec<_>>().join(" ")
    };

    println!("=== E3.A deterministic cases (probe = fresh O_RDONLY + F_SETLEASE F_RDLCK inside handler)");
    // kind, external reader triggers, premark, expected_last_neutered (EAGAIN = writer present)
    let cases: Vec<(&str, bool, bool, i32)> = vec![
        ("none", true, false, 0),
        ("rdonly", true, false, 0),
        ("opath", true, false, 0),
        ("wronly", true, false, libc::EAGAIN),
        ("rdwr", true, false, libc::EAGAIN),
        ("trunc", true, false, libc::EAGAIN),
        ("wronly", true, true, libc::EAGAIN), // writer opened BEFORE the mark was added
        ("rdwr-read", false, false, libc::EAGAIN),
        ("wronly-write", false, false, libc::EAGAIN),
        ("trunc-write", false, false, libc::EAGAIN),
        ("trunc-extend", false, false, libc::EAGAIN),
        ("ftrunc", false, false, libc::EAGAIN),
        ("ftrunc-same", false, false, libc::EAGAIN),
        ("mmapw-open", false, false, libc::EAGAIN),
        ("mmapw-closed", true, false, libc::EAGAIN),
        ("mmapr-closed", true, false, 0),
        ("mmapp-closed", true, false, 0),
        ("stale", true, false, 0),
    ];
    for (kind, ext, pre, want) in &cases {
        let (v, hmsg, rout) = case(kind, *ext, *pre);
        let last = v.last().map(|x| x.1);
        let verdict = match last {
            None => "NO EVENT".to_string(),
            Some(l) if l == *want => "PASS".to_string(),
            Some(l) => format!("MISMATCH (want {})", errname(*want)),
        };
        println!("{:14} ext_reader={:5} premark={:5} probes={} ; holder: {} ; reader: {} => {}", kind, ext, pre, fmt(&v), hmsg.trim(), rout.trim(), verdict);
    }

    println!("=== E3.B handler keeps the lease it just won, then a writer opens (full design flow)");
    {
        let name = "keep1";
        let p = format!("{dir}/{name}");
        make_file(&p, size, 1, false, 1_500_000_000);
        mark_add(&g, &p, FAN_PRE_ACCESS, 0).unwrap();
        let rout = run_self(&["h-read", &p]);
        println!("reader: {}", rout.trim());
        let nk = kept.lock().unwrap().len();
        println!("lease kept by handler: {} fd(s); F_GETLEASE={}", nk, lease_name(getlease(*kept.lock().unwrap().last().unwrap_or(&-1))));
        let b0 = br.log.lock().unwrap().len();
        let out = run_self(&["h-op", "wronly", &p]);
        let kvv = |k: &str| -> u64 { out.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.parse().unwrap())).unwrap() };
        println!("writer open blocked {:.1} ms (breaker delay 300 ms), breaks signalled={}", (kvv("t1") - kvv("t0")) as f64 / 1e6, br.log.lock().unwrap().len() - b0);
    }

    {
        let name = "keepB1";
        let p = format!("{dir}/{name}");
        make_file(&p, size, 1, false, 1_500_000_000);
        mark_add(&g, &p, FAN_PRE_ACCESS, 0).unwrap();
        let rout = run_self(&["h-read", &p]);
        println!("variant B (handler removes the mark first, then takes+keeps RDLCK): reader: {}", rout.trim());
        let kvr = |k: &str| -> u64 { rout.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.parse().unwrap())).unwrap() };
        println!("  reader total {:.2} ms; correct={}", (kvr("t1") - kvr("t0")) as f64 / 1e6, rout.contains(&format!("fnv={:x}", fnv_pat(1, size))));
        let b0 = br.log.lock().unwrap().len();
        let out = run_self(&["h-op", "wronly", &p]);
        let kvv = |k: &str| -> u64 { out.split_whitespace().find_map(|w| w.strip_prefix(&format!("{k}=")).map(|x| x.parse().unwrap())).unwrap() };
        println!("  later writer open blocked {:.1} ms (breaker delay 300 ms), breaks signalled={}", (kvv("t1") - kvv("t0")) as f64 / 1e6, br.log.lock().unwrap().len() - b0);
    }

    println!("=== E3.C randomized stress: {trials} trials; expectation = writer present iff holder kind in {{wronly, rdwr, trunc, mmapw-closed}}");
    let kinds = ["none", "rdonly", "wronly", "rdwr", "trunc-extend", "mmapw-closed", "mmapr-closed", "mmapp-closed", "stale", "opath"];
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let mut stats: HashMap<&str, [u32; 4]> = HashMap::new(); // eagain, ok, noevent, total
    let (mut fneg, mut fpos, mut noev) = (0, 0, 0);
    for _ in 0..trials {
        let kind = kinds[rng.below(kinds.len() as u64) as usize];
        let writer = matches!(kind, "wronly" | "rdwr" | "trunc-extend" | "mmapw-closed");
        let (v, _, _) = case(kind, true, false);
        let e = stats.entry(kind).or_insert([0; 4]);
        e[3] += 1;
        match v.last() {
            None => {
                e[2] += 1;
                noev += 1;
            }
            Some((_, a)) => {
                if *a == libc::EAGAIN {
                    e[0] += 1;
                } else if *a == 0 {
                    e[1] += 1;
                }
                if writer && *a == 0 {
                    fneg += 1;
                }
                if !writer && *a == libc::EAGAIN {
                    fpos += 1;
                }
            }
        }
    }
    let mut ks: Vec<_> = stats.iter().collect();
    ks.sort();
    for (k, e) in ks {
        println!("{:14} trials={:4} EAGAIN={:4} OK={:4} no-event={:4}", k, e[3], e[0], e[1], e[2]);
    }
    println!("false negatives (writer present, probe OK) = {fneg} ; false positives = {fpos} ; trials with no event = {noev}");
    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
}
