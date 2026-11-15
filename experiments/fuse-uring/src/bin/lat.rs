//! latency client: lat <dir> <stat|openclose|createunlink|read4k> <iters> <clients> [--pin]
use std::time::Instant;
use std::ffi::CString;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set);
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (dir, op, iters, clients) = (a[1].clone(), a[2].clone(), a[3].parse::<usize>().unwrap(), a[4].parse::<usize>().unwrap());
    let pinned = a.iter().any(|x| x == "--pin");
    let mut hs = vec![];
    let wall = Instant::now();
    for c in 0..clients {
        let (dir, op) = (dir.clone(), op.clone());
        hs.push(std::thread::spawn(move || {
            if pinned { pin(c); }
            let p = CString::new(format!("{dir}/f{c}")).unwrap();
            let mut buf = [0u8; 4096];
            unsafe {
                let fd = libc::open(p.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o644);
                assert!(fd >= 0, "open: {}", std::io::Error::last_os_error());
                if op == "read4k" { libc::pwrite(fd, buf.as_ptr().cast(), 4096, 0); }
                libc::close(fd);
            }
            let mut st: libc::stat = unsafe { std::mem::zeroed() };
            let mut lat = Vec::with_capacity(iters);
            for i in 0..iters + 2000 {
                let t = Instant::now();
                unsafe {
                    match op.as_str() {
                        "stat" => { assert_eq!(libc::stat(p.as_ptr(), &mut st), 0); }
                        "openclose" => { let fd = libc::open(p.as_ptr(), libc::O_RDONLY); assert!(fd >= 0); libc::close(fd); }
                        "createunlink" => {
                            let fd = libc::open(p.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o644); assert!(fd >= 0);
                            libc::close(fd); assert_eq!(libc::unlink(p.as_ptr()), 0);
                        }
                        "read4k" => { let fd = libc::open(p.as_ptr(), libc::O_RDONLY); assert!(fd >= 0);
                            assert_eq!(libc::pread(fd, buf.as_mut_ptr().cast(), 4096, 0), 4096); libc::close(fd); }
                        _ => panic!("op"),
                    }
                }
                if i >= 2000 { lat.push(t.elapsed().as_nanos() as u64); }
            }
            if op != "createunlink" { unsafe { libc::unlink(p.as_ptr()); } }
            lat
        }));
    }
    let mut all: Vec<u64> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let el = wall.elapsed().as_secs_f64();
    all.sort_unstable();
    let n = all.len();
    let mean = all.iter().sum::<u64>() as f64 / n as f64 / 1000.0;
    println!("{op}\tclients={clients}\tmean_us={mean:.1}\tp50_us={:.1}\tp99_us={:.1}\tops_s={:.0}", all[n / 2] as f64 / 1000.0, all[n * 99 / 100] as f64 / 1000.0, (n + clients * 2000) as f64 / el);
}
