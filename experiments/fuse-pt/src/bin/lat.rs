//! Per-op latency of cold stat / open on a file list. Usage: lat <phase> <listfile> [n]
//! phases: stat (fresh lookup+getattr), open_cold (fresh lookup+open+close), open_warm (stat first, time open+close only).
use std::ffi::CString;
use std::time::Instant;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (phase, list) = (a[1].as_str(), &a[2]);
    let n: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let paths: Vec<CString> = std::fs::read_to_string(list)
        .unwrap()
        .lines()
        .take(n)
        .map(|l| CString::new(l).unwrap())
        .collect();
    let stat = |p: &CString| unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        libc::stat(p.as_ptr(), &mut st)
    };
    if phase == "open_warm" {
        paths.iter().for_each(|p| {
            stat(p);
        });
    }
    let mut us: Vec<f64> = Vec::with_capacity(paths.len());
    for p in &paths {
        let t = Instant::now();
        match phase {
            "stat" => {
                stat(p);
            }
            _ => unsafe {
                let fd = libc::open(p.as_ptr(), libc::O_RDONLY);
                libc::close(fd);
            },
        }
        us.push(t.elapsed().as_nanos() as f64 / 1000.0);
    }
    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |f: f64| us[((us.len() as f64 * f) as usize).min(us.len() - 1)];
    println!("{phase} n={} median_us={:.1} p90_us={:.1} p99_us={:.1} mean_us={:.1}", us.len(), q(0.5), q(0.9), q(0.99), us.iter().sum::<f64>() / us.len() as f64);
}
