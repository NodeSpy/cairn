//! Loopback FUSE filesystem with kernel passthrough (E11).
//!
//! Serves `--backing` at `--mount`. With `--passthrough`, `open` registers the backing file with the
//! kernel (FUSE_DEV_IOC_BACKING_OPEN) so read/write/mmap never reach this process.

mod fs;
mod remote;
mod stats;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fuser::{Config, MountOption, Session, SessionACL};

/// Request-reduction switches (E12). `--tune a,b,-c`: `all`/`none` set the base, `x` enables, `-x` disables.
#[derive(Clone, Copy, Debug)]
pub struct Tune {
    /// OPENDIR replies ENOSYS (kernel then skips OPENDIR/RELEASEDIR); readdir is stateless per inode.
    pub nodir: bool,
    /// CREATE opens once and registers that fd (instead of open, close, stat, open).
    pub create1: bool,
    /// Removed in fuse-fidelity (fd-less I/O is incompatible with fd-based handles); accepted and rejected at startup.
    pub noopen: bool,
    /// FUSE_HANDLE_KILLPRIV_V2.
    pub killpriv: bool,
    /// FUSE_EXPLICIT_INVAL_DATA.
    pub explicit: bool,
    /// Cache negative lookups (ino 0 entry with TTL).
    pub neg: bool,
    /// READDIRPLUS (off: plain READDIR).
    pub rdplus: bool,
    /// FUSE_POSIX_ACL: the kernel evaluates POSIX ACLs (system.posix_acl_* are forwarded as xattrs) and does not
    /// apply the umask itself; the daemon applies the request umask so the backing fs can honour default ACLs.
    pub acl: bool,
    /// With OPENDIR enabled (`-nodir`): let the kernel cache directory contents (FOPEN_CACHE_DIR). Without OPENDIR the
    /// kernel always caches (its default for no_opendir).
    pub dircache: bool,
}

impl Tune {
    pub const LEGACY: Tune = Tune { nodir: false, create1: false, noopen: false, killpriv: false, explicit: false, neg: true, rdplus: true, acl: false, dircache: false };
    pub const DEFAULT: Tune = Tune { create1: true, acl: true, ..Tune::LEGACY };
    fn parse(s: &str) -> Tune {
        let mut t = Tune::DEFAULT;
        for w in s.split(',').filter(|w| !w.is_empty()) {
            let (on, name) = match w.strip_prefix('-') { Some(n) => (false, n), None => (true, w) };
            match name {
                "all" => t = Tune { nodir: on, create1: on, noopen: false, killpriv: false, explicit: on, neg: true, rdplus: true, acl: true, dircache: false },
                "none" | "legacy" => t = Tune::LEGACY,
                "default" => t = Tune::DEFAULT,
                "nodir" => t.nodir = on,
                "create1" => t.create1 = on,
                "noopen" => t.noopen = on,
                "killpriv" => t.killpriv = on,
                "explicit" => t.explicit = on,
                "neg" => t.neg = on,
                "rdplus" => t.rdplus = on,
                "acl" => t.acl = on,
                "dircache" => t.dircache = on,
                _ => die(&format!("unknown tune option {name}")),
            }
        }
        t
    }
}

#[derive(Clone)]
pub struct Opts {
    pub backing: PathBuf,
    pub mount: PathBuf,
    pub passthrough: bool,
    pub writeback: bool,
    pub ttl: Duration,
    pub threads: usize,
    /// Directory (relative to the mount) in which the remote-change simulator runs.
    pub simulate: Option<String>,
    pub sim_interval: Duration,
    pub sim_cycles: Option<u32>,
    pub sim_delay: Duration,
    pub stats_file: Option<String>,
    pub stats_off: bool,
    /// Mount source name shown in /proc/mounts; given, the mount also drops the `.fuse-pt` subtype (xfstests wants plain `fuse`).
    pub fsname: Option<String>,
    /// Extra mount options: nosuid,nodev,noexec,ro,atime (atime = drop the default noatime).
    pub mntopts: Vec<String>,
    pub tune: Tune,
}

fn parse_args() -> Opts {
    let mut o = Opts {
        backing: "/cache/backing".into(),
        mount: "/mnt/lfs".into(),
        passthrough: false,
        writeback: false,
        ttl: Duration::from_secs(3600),
        threads: 4,
        simulate: None,
        sim_interval: Duration::from_millis(1500),
        sim_cycles: None,
        sim_delay: Duration::ZERO,
        stats_file: None,
        stats_off: false,
        fsname: None,
        mntopts: vec![],
        tune: Tune::DEFAULT,
    };
    let mut a = std::env::args().skip(1);
    while let Some(k) = a.next() {
        let mut val = || a.next().unwrap_or_else(|| die(&format!("{k} needs a value")));
        match k.as_str() {
            "--backing" => o.backing = val().into(),
            "--mount" => o.mount = val().into(),
            "--passthrough" => o.passthrough = true,
            "--stats-file" => o.stats_file = Some(val()),
            "--tune" => o.tune = Tune::parse(&val()),
            "--trace" => stats::TRACE.store(true, Ordering::Relaxed),
            "--stats-off" => o.stats_off = true,
            "--fsname" => o.fsname = Some(val()),
            "--mntopts" => o.mntopts = val().split(',').filter(|s| !s.is_empty()).map(String::from).collect(),
            "--writeback" => o.writeback = true,
            "--ttl" => o.ttl = Duration::from_secs(val().parse().unwrap_or_else(|_| die("bad --ttl"))),
            "--threads" => o.threads = val().parse().unwrap_or_else(|_| die("bad --threads")),
            "--simulate-remote" => o.simulate = Some(val()),
            "--sim-interval-ms" => o.sim_interval = Duration::from_millis(val().parse().unwrap_or_else(|_| die("bad interval"))),
            "--sim-delay-ms" => o.sim_delay = Duration::from_millis(val().parse().unwrap_or_else(|_| die("bad delay"))),
            "--sim-cycles" => o.sim_cycles = Some(val().parse().unwrap_or_else(|_| die("bad cycles"))),
            _ => die(&format!("unknown argument {k}")),
        }
    }
    o
}

fn die(msg: &str) -> ! {
    eprintln!("fuse-fidelity: {msg}");
    std::process::exit(2)
}

static STOP: AtomicBool = AtomicBool::new(false);
static DUMP: AtomicBool = AtomicBool::new(false);
static RESET: AtomicBool = AtomicBool::new(false);
extern "C" fn on_usr1(_: libc::c_int) {
    DUMP.store(true, Ordering::SeqCst);
}
extern "C" fn on_usr2(_: libc::c_int) {
    RESET.store(true, Ordering::SeqCst);
}
extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

fn main() {
    let opts = parse_args();
    unsafe {
        libc::signal(libc::SIGINT, on_signal as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGTERM, on_signal as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGUSR1, on_usr1 as extern "C" fn(libc::c_int) as usize);
        libc::signal(libc::SIGUSR2, on_usr2 as extern "C" fn(libc::c_int) as usize);
    }
    // Every inode the kernel has cached is held as an O_PATH fd (fs.rs), so raise the soft fd limit to the hard one.
    unsafe {
        let mut rl = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) == 0 && rl.rlim_cur < rl.rlim_max {
            rl.rlim_cur = rl.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl);
        }
    }
    stats::STATS.on.store(!opts.stats_off, Ordering::Relaxed);
    let backing = std::fs::canonicalize(&opts.backing).unwrap_or_else(|e| die(&format!("backing: {e}")));
    let fs = fs::Loopback::new(backing, &opts);
    let notifier_slot = fs.notifier_slot();

    let mut cfg = Config::default();
    cfg.mount_options = vec![
        MountOption::FSName(opts.fsname.clone().unwrap_or_else(|| "leasefs-pt".into())),
        MountOption::DefaultPermissions,
    ];
    let has = |n: &str| opts.mntopts.iter().any(|o| o == n);
    if !has("atime") {
        cfg.mount_options.push(MountOption::NoAtime);
    }
    // Devices (and overlayfs whiteouts) and setuid files must work unless the caller says otherwise.
    cfg.mount_options.push(if has("nodev") { MountOption::NoDev } else { MountOption::Dev });
    cfg.mount_options.push(if has("nosuid") { MountOption::NoSuid } else { MountOption::Suid });
    cfg.mount_options.push(if has("noexec") { MountOption::NoExec } else { MountOption::Exec });
    if has("ro") {
        cfg.mount_options.push(MountOption::RO);
    }
    if opts.fsname.is_none() {
        cfg.mount_options.push(MountOption::Subtype("fuse-pt".into()));
    }
    cfg.acl = SessionACL::All; // allow_other, so non-root users can use the mount of a root daemon
    cfg.n_threads = Some(opts.threads);
    cfg.clone_fd = true;

    let mut session = Session::new(fs, &opts.mount, &cfg).unwrap_or_else(|e| die(&format!("mount: {e}")));
    let notifier = session.notifier();
    *notifier_slot.lock().unwrap() = Some(notifier.clone());
    let mut unmounter = session.unmount_callable();
    eprintln!(
        "fuse-fidelity: mounted {} -> {} passthrough={} writeback={} threads={} ttl={:?} tune={:?}",
        opts.backing.display(), opts.mount.display(), opts.passthrough, opts.writeback, opts.threads, opts.ttl, opts.tune,
    );

    let sim = opts.simulate.clone().map(|dir| {
        let o = opts.clone();
        std::thread::Builder::new()
            .name("remote-sim".into())
            .spawn(move || remote::run(&o, &dir, notifier, &STOP))
            .expect("spawn remote-sim")
    });

    // SIGINT/SIGTERM: unmount, which ends the request loop below.
    let sf = opts.stats_file.clone();
    std::thread::spawn(move || {
        while !STOP.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
            if RESET.swap(false, Ordering::SeqCst) {
                stats::STATS.reset();
            }
            if DUMP.swap(false, Ordering::SeqCst) {
                stats::STATS.dump(sf.as_deref(), "sigusr1");
            }
        }
        let _ = unmounter.unmount();
    });

    let res = session.run(); // blocks until unmounted (by us or externally)
    STOP.store(true, Ordering::SeqCst);
    if !opts.stats_off && opts.stats_file.is_none() {
        stats::STATS.dump(None, "final");
    }
    if let Some(s) = sim {
        let _ = s.join();
    }
    if let Err(e) = res {
        die(&format!("session: {e}"));
    }
}
