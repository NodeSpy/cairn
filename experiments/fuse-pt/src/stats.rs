//! Per-opcode request counters and latency histograms (E12).
//!
//! The latency is the daemon-side handling time (from entering the `Filesystem` method until the handler
//! returns, reply included), not the kernel round trip. Buckets are powers of two in ns.
//! SIGUSR1 dumps to `--stats-file` (atomically, with a sequence number), SIGUSR2 resets, and the final
//! dump happens at unmount. `--stats-off` removes the timing cost entirely.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

macro_rules! ops {
    ($($v:ident => $n:expr),* $(,)?) => {
        #[derive(Clone, Copy)]
        #[allow(dead_code)]
        pub enum Op { $($v),* }
        pub const NAMES: &[&str] = &[$($n),*];
    };
}
ops! {
    Lookup => "LOOKUP", LookupNeg => "LOOKUP(neg)", Forget => "FORGET", Getattr => "GETATTR", Setattr => "SETATTR",
    Readlink => "READLINK", Mknod => "MKNOD", Mkdir => "MKDIR", Unlink => "UNLINK", Rmdir => "RMDIR",
    Symlink => "SYMLINK", Rename => "RENAME", Link => "LINK", Open => "OPEN", Read => "READ", Write => "WRITE",
    Flush => "FLUSH", Release => "RELEASE", Fsync => "FSYNC", Opendir => "OPENDIR", Readdir => "READDIR",
    Readdirplus => "READDIRPLUS", Releasedir => "RELEASEDIR", Fsyncdir => "FSYNCDIR", Statfs => "STATFS",
    Setxattr => "SETXATTR", Getxattr => "GETXATTR", Listxattr => "LISTXATTR", Removexattr => "REMOVEXATTR",
    Access => "ACCESS", Create => "CREATE", Fallocate => "FALLOCATE", Lseek => "LSEEK",
    CopyFileRange => "COPY_FILE_RANGE", Lock => "LOCK", Other => "OTHER",
}
const N: usize = NAMES.len();
const B: usize = 40;

pub struct Stats {
    pub on: AtomicBool,
    count: [AtomicU64; N],
    sum_ns: [AtomicU64; N],
    hist: [[AtomicU64; B]; N],
    seq: AtomicU64,
}

pub static STATS: Stats = Stats {
    on: AtomicBool::new(true),
    count: [const { AtomicU64::new(0) }; N],
    sum_ns: [const { AtomicU64::new(0) }; N],
    hist: [const { [const { AtomicU64::new(0) }; B] }; N],
    seq: AtomicU64::new(0),
};

pub static TRACE: AtomicBool = AtomicBool::new(false);

/// `--trace`: one stderr line per traced request (path, not just opcode).
pub fn trace(msg: impl FnOnce() -> String) {
    if TRACE.load(Relaxed) {
        eprintln!("T {}", msg());
    }
}

pub struct Guard(Op, Option<Instant>);

pub fn start(op: Op) -> Guard {
    Guard(op, if STATS.on.load(Relaxed) { Some(Instant::now()) } else { None })
}

/// Count an op without timing (e.g. a lookup that turned out negative: counted in addition to LOOKUP).
pub fn bump(op: Op) {
    STATS.count[op as usize].fetch_add(1, Relaxed);
}

impl Drop for Guard {
    fn drop(&mut self) {
        let Some(t) = self.1 else { return };
        let ns = t.elapsed().as_nanos() as u64;
        let i = self.0 as usize;
        STATS.count[i].fetch_add(1, Relaxed);
        STATS.sum_ns[i].fetch_add(ns, Relaxed);
        let b = (64 - ns.leading_zeros() as usize).min(B - 1);
        STATS.hist[i][b].fetch_add(1, Relaxed);
    }
}

impl Stats {
    pub fn reset(&self) {
        for i in 0..N {
            self.count[i].store(0, Relaxed);
            self.sum_ns[i].store(0, Relaxed);
            for b in 0..B {
                self.hist[i][b].store(0, Relaxed);
            }
        }
    }

    fn quantile_us(&self, i: usize, q: f64) -> f64 {
        let h: Vec<u64> = (0..B).map(|b| self.hist[i][b].load(Relaxed)).collect();
        let tot: u64 = h.iter().sum();
        if tot == 0 {
            return 0.0;
        }
        let want = (tot as f64 * q).ceil() as u64;
        let mut acc = 0;
        for (b, c) in h.iter().enumerate() {
            acc += c;
            if acc >= want {
                return (1u64 << b) as f64 / 1000.0; // bucket upper bound
            }
        }
        0.0
    }

    /// TSV: op, count, mean_us, p50_us, p99_us (bucket upper bounds), total_ms.
    pub fn render(&self, label: &str) -> String {
        let mut s = String::new();
        let seq = self.seq.fetch_add(1, Relaxed) + 1;
        let _ = writeln!(s, "# seq {seq} {label}");
        let mut total = 0;
        for i in 0..N {
            let c = self.count[i].load(Relaxed);
            if c == 0 {
                continue;
            }
            if !matches!(NAMES[i], "LOOKUP(neg)") {
                total += c;
            }
            let sum = self.sum_ns[i].load(Relaxed) as f64;
            let timed: u64 = (0..B).map(|b| self.hist[i][b].load(Relaxed)).sum();
            let mean = if timed > 0 { sum / timed as f64 / 1000.0 } else { 0.0 };
            let _ = writeln!(
                s,
                "{}\t{}\t{:.1}\t{:.1}\t{:.1}\t{:.1}",
                NAMES[i], c, mean, self.quantile_us(i, 0.5), self.quantile_us(i, 0.99), sum / 1e6
            );
        }
        let _ = writeln!(s, "TOTAL\t{total}");
        s
    }

    pub fn dump(&self, path: Option<&str>, label: &str) {
        let out = self.render(label);
        match path {
            Some(p) => {
                let tmp = format!("{p}.tmp");
                if std::fs::write(&tmp, &out).is_ok() {
                    let _ = std::fs::rename(&tmp, p);
                }
            }
            None => eprint!("{out}"),
        }
    }
}
