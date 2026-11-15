//! `cairnd`: the cairn client daemon.
//!
//!   cairnd --config /etc/cairn/cairnd.toml [--uring]
//!
//! Test mode (FUSE layer only, in-memory `MockCore` over a backing directory):
//!
//!   cairnd --mock <backing-dir> --mount <mountpoint> [--simulate-remote <dir> [--sim-cycles N]]

use cairn_client::api::Core;
use cairn_fuse::fs::FsOpts;
use cairn_fuse::mock::{new_attrs, MockCore};
use cairn_fuse::{mount, stats, Mount, MountOpts};
use cairn_proto::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

static STOP: AtomicBool = AtomicBool::new(false);
static DUMP: AtomicBool = AtomicBool::new(false);
static RESET: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}
extern "C" fn on_usr1(_: libc::c_int) {
    DUMP.store(true, Ordering::SeqCst);
}
extern "C" fn on_usr2(_: libc::c_int) {
    RESET.store(true, Ordering::SeqCst);
}

fn die(msg: &str) -> ! {
    eprintln!("cairnd: {msg}");
    std::process::exit(2)
}

#[derive(Default)]
struct Args {
    config: Option<PathBuf>,
    mock: Option<PathBuf>,
    mount: Option<PathBuf>,
    simulate: Option<String>,
    sim_cycles: Option<u32>,
    sim_interval_ms: u64,
    sim_delay_ms: u64,
    sim_start_file: Option<PathBuf>,
    stats_file: Option<String>,
    stats_off: bool,
    statfs_path: Option<PathBuf>,
    o: MountOpts,
}

fn parse_args() -> Args {
    let mut a = Args { sim_interval_ms: 1000, ..Default::default() };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{k} needs a value")));
        match k.as_str() {
            "--config" => a.config = Some(val().into()),
            "--mock" => a.mock = Some(val().into()),
            "--mount" => a.mount = Some(val().into()),
            "--uring" => a.o.uring = true,
            "--uring-depth" => a.o.uring_depth = val().parse().unwrap_or_else(|_| die("bad --uring-depth")),
            "--no-passthrough" => a.o.fs.passthrough = false,
            "--ttl" => {
                let t = Duration::from_secs(val().parse().unwrap_or_else(|_| die("bad --ttl")));
                a.o.fs.ttl = t;
                a.o.fs.neg_ttl = t;
            }
            "--threads" => a.o.threads = val().parse().unwrap_or_else(|_| die("bad --threads")),
            "--fsname" => a.o.fsname = val(),
            "--mntopts" => a.o.mntopts = val().split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
            "--statfs-path" => a.statfs_path = Some(val().into()),
            "--stats-file" => a.stats_file = Some(val()),
            "--stats-off" => a.stats_off = true,
            "--simulate-remote" => a.simulate = Some(val()),
            "--sim-cycles" => a.sim_cycles = Some(val().parse().unwrap_or_else(|_| die("bad --sim-cycles"))),
            "--sim-start-file" => a.sim_start_file = Some(val().into()),
            "--sim-delay-ms" => a.sim_delay_ms = val().parse().unwrap_or_else(|_| die("bad --sim-delay-ms")),
            "--sim-interval-ms" => a.sim_interval_ms = val().parse().unwrap_or_else(|_| die("bad --sim-interval-ms")),
            "-h" | "--help" => {
                println!("cairnd --config FILE [--uring] | --mock DIR --mount MP [--simulate-remote DIR]");
                std::process::exit(0)
            }
            _ => die(&format!("unknown argument {k}")),
        }
    }
    a
}

fn raise_nofile() {
    unsafe {
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 && rl.rlim_cur < rl.rlim_max {
            rl.rlim_cur = rl.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
        }
    }
}

fn main() {
    let a = parse_args();
    unsafe {
        libc::signal(libc::SIGINT, on_stop as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGTERM, on_stop as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGUSR1, on_usr1 as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGUSR2, on_usr2 as extern "C" fn(libc::c_int) as usize);
    }
    raise_nofile();
    let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into())).try_init();
    stats::STATS.on.store(!a.stats_off, Ordering::Relaxed);

    let mut mounts: Vec<Mount> = vec![];
    let mut _client: Box<dyn std::any::Any> = Box::new(());
    let mut sim = None;
    if let Some(backing) = &a.mock {
        let mp = a.mount.clone().unwrap_or_else(|| die("--mock needs --mount"));
        let core = MockCore::new(backing.join("data"));
        let mut o = a.o.clone();
        o.fs = FsOpts { statfs_path: a.statfs_path.clone().or(Some(backing.clone())), ..o.fs };
        let m = mount(core.clone() as Arc<dyn Core>, &mp, &o).unwrap_or_else(|e| die(&format!("mount: {e}")));
        eprintln!("cairnd: mock mounted {} uring={}", m.mountpoint.display(), m.uring_active);
        if let Some(dir) = a.simulate.clone() {
            let (cycles, ms, delay, start) = (a.sim_cycles, a.sim_interval_ms, a.sim_delay_ms, a.sim_start_file.clone());
            sim = Some(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(delay));
                while start.as_ref().is_some_and(|f| !f.exists()) && !STOP.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(50));
                }
                simulate(core, &dir, cycles, ms)
            }));
        }
        mounts.push(m);
    } else if let Some(cfg) = &a.config {
        let (m, c) = real::open(cfg, &a.o).unwrap_or_else(|e| die(&format!("{e:#}")));
        mounts = m;
        _client = c;
    } else {
        die("need --config FILE (or --mock DIR --mount MP)");
    }

    while !STOP.load(Ordering::SeqCst) && mounts.iter().all(|m| !m.is_finished()) {
        std::thread::sleep(Duration::from_millis(20));
        if RESET.swap(false, Ordering::SeqCst) {
            stats::STATS.reset();
        }
        if DUMP.swap(false, Ordering::SeqCst) {
            stats::STATS.dump(a.stats_file.as_deref(), "sigusr1");
        }
    }
    let why = if STOP.load(Ordering::SeqCst) { "signal" } else { "session-ended" };
    STOP.store(true, Ordering::SeqCst);
    // If the session ended by itself (external umount) do NOT unmount by path: a new
    // instance may already be mounted at the same mountpoint and we would tear it down.
    eprintln!("cairnd: exiting ({why})");
    if why == "signal" {
        for m in mounts.iter_mut() {
            let _ = m.unmount();
        }
    }
    for m in mounts.iter_mut() {
        if let Err(e) = m.wait() {
            eprintln!("cairnd: session: {e}");
        }
    }
    if let Some(s) = sim {
        let _ = s.join();
    }
    if !a.stats_off && a.stats_file.is_none() {
        stats::STATS.dump(None, "final");
    }
}

/// One `Client` (state DB, CAS, server link), one `Core` per configured root, one FUSE mount per root.
mod real {
    use super::*;
    use anyhow::Context;
    use cairn_client::client::Client;
    use cairn_client::config::Config;
    use serde::Deserialize;

    /// Optional `[fuse]` table in the same TOML file as the client config.
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct FuseSection {
        uring: Option<bool>,
        uring_depth: Option<usize>,
        threads: Option<usize>,
        ttl_secs: Option<u64>,
        passthrough: Option<bool>,
        mntopts: Vec<String>,
    }

    #[derive(Deserialize, Default)]
    struct FileCfg {
        #[serde(default)]
        fuse: FuseSection,
    }

    pub fn open(path: &std::path::Path, base: &MountOpts) -> anyhow::Result<(Vec<Mount>, Box<dyn std::any::Any>)> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let f: FileCfg = toml::from_str(&text).context("parse [fuse]")?;
        let cfg = Config::from_toml(&text)?;
        let mut o = base.clone();
        o.uring |= f.fuse.uring.unwrap_or(false);
        if let Some(d) = f.fuse.uring_depth {
            o.uring_depth = d;
        }
        if let Some(t) = f.fuse.threads {
            o.threads = t;
        }
        if let Some(t) = f.fuse.ttl_secs {
            o.fs.ttl = Duration::from_secs(t);
            o.fs.neg_ttl = o.fs.ttl;
        }
        if let Some(p) = f.fuse.passthrough {
            o.fs.passthrough &= p;
        }
        o.mntopts.extend(f.fuse.mntopts);
        o.fs.statfs_path = Some(cfg.cache_dir.clone());
        let roots: Vec<(String, std::path::PathBuf)> = cfg
            .roots
            .iter()
            .map(|(n, r)| Ok((n.clone(), r.path.clone().with_context(|| format!("root {n}: no `path` (mountpoint)"))?)))
            .collect::<anyhow::Result<_>>()?;
        let client = Client::open(cfg)?;
        let mut mounts = vec![];
        for (name, mp) in roots {
            std::fs::create_dir_all(&mp).ok();
            let mut ro = o.clone();
            if base.fsname == "cairn" {
                ro.fsname = format!("cairn:{name}");
            }
            let m = mount(client.core(&name)?, &mp, &ro).with_context(|| format!("mount {name} at {}", mp.display()))?;
            eprintln!("cairnd: root {name} mounted at {} uring={}", m.mountpoint.display(), m.uring_active);
            mounts.push(m);
        }
        Ok((mounts, Box::new(client)))
    }
}

/// Stand-in for the change stream: remote create/modify/chmod/rename/delete cycles, delivered
/// through the same path Core uses (in-flight, applier, direct).
fn simulate(core: Arc<MockCore>, rel: &str, cycles: Option<u32>, interval_ms: u64) {
    let ch = |seq: u64, effect: Effect| Change { seq: Seq(seq), origin: None, host: HostId(9), cred: Cred { uid: 0, gid: 0 }, effect };
    let nid = |n: u64| NodeId::new(HostId(9), n);
    let name = |s: &str| Name(s.as_bytes().to_vec());
    let (d, f1) = (nid(1000), nid(1001));
    let mut seq = 1u64;
    let mut cycle = 0u32;
    let mut run = |e: Effect| {
        seq += 1;
        match core.deliver(ch(seq, e)) {
            Ok(()) => {}
            Err(er) => eprintln!("[sim] change {seq} failed: {er:?}"),
        }
    };
    let ts = |s: i64| s * 1_000_000_000;
    let mut da = new_attrs(Kind::Dir, 0o755, 0);
    da.mtime_ns = ts(1_700_000_000);
    run(Effect::Upsert { parent: NodeId::ROOT, name: name(rel), node: d, attrs: da });
    // Give a recursive watcher time to add the new directory.
    std::thread::sleep(Duration::from_millis(500));
    while !STOP.load(Ordering::SeqCst) && cycles.is_none_or(|c| cycle < c) {
        cycle += 1;
        let mut fa = new_attrs(Kind::File, 0o644, 4096);
        fa.mtime_ns = ts(1_700_000_001);
        run(Effect::Upsert { parent: d, name: name("f1"), node: f1, attrs: fa.clone() });
        fa.size = 8192;
        fa.mtime_ns = ts(1_700_000_100 + cycle as i64);
        run(Effect::Attrs { node: f1, attrs: fa.clone() });
        fa.mode = 0o600;
        run(Effect::Attrs { node: f1, attrs: fa.clone() });
        fa.xattrs = vec![(b"user.k".to_vec(), b"v".to_vec())];
        run(Effect::Attrs { node: f1, attrs: fa.clone() });
        run(Effect::Move { src_parent: d, src_name: name("f1"), dst_parent: d, dst_name: name("f2"), node: f1 });
        run(Effect::Remove { parent: d, name: name("f2"), node: f1, node_deleted: true });
        eprintln!("[sim] cycle {cycle} done");
        std::thread::sleep(Duration::from_millis(interval_ms));
    }
    run(Effect::Remove { parent: NodeId::ROOT, name: name(rel), node: d, node_deleted: true });
}
