//! Mounting one Core as one FUSE filesystem: session, request loop thread, applier.

use crate::applier::Applier;
use crate::fs::{CairnFs, FsOpts, Shared};
use cairn_client::api::{Core, KernelApplier};
use fuser::{Config, MountOption, Session, SessionACL};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MountOpts {
    /// Mount source name (shown in /proc/mounts; xfstests wants plain `fuse` type, so no subtype).
    pub fsname: String,
    pub fs: FsOpts,
    /// Request threads (classic transport).
    pub threads: usize,
    /// FUSE over io_uring (needs `fuse.enable_uring=1`; helps on VMs). Handlers that block on
    /// Core round trips stall one CPU's queue, so keep this off on busy multi-root hosts.
    pub uring: bool,
    pub uring_depth: usize,
    /// nosuid,nodev,noexec,ro,atime
    pub mntopts: Vec<String>,
}

impl Default for MountOpts {
    fn default() -> Self {
        MountOpts {
            fsname: "cairn".into(),
            fs: FsOpts::default(),
            threads: 16,
            uring: false,
            uring_depth: 0,
            mntopts: vec![],
        }
    }
}

pub struct Mount {
    pub mountpoint: PathBuf,
    pub shared: Arc<Shared>,
    pub applier: Arc<dyn KernelApplier>,
    unmounter: fuser::SessionUnmounter,
    thread: Option<JoinHandle<io::Result<()>>>,
    pub uring_active: bool,
}

/// Mount `core` at `mountpoint` and start serving. The applier is registered with the core once
/// the request loop runs (the core starts applying remote changes only then).
pub fn mount(core: Arc<dyn Core>, mountpoint: &Path, o: &MountOpts) -> io::Result<Mount> {
    let mountpoint = std::fs::canonicalize(mountpoint)?;
    let shared = Shared::new(o.fs.clone());
    let fs = CairnFs::new(core.clone(), shared.clone());

    let mut cfg = Config::default();
    cfg.mount_options = vec![MountOption::FSName(o.fsname.clone()), MountOption::DefaultPermissions];
    let has = |n: &str| o.mntopts.iter().any(|x| x == n);
    if !has("atime") {
        cfg.mount_options.push(MountOption::NoAtime);
    }
    cfg.mount_options.push(if has("nodev") { MountOption::NoDev } else { MountOption::Dev });
    cfg.mount_options.push(if has("nosuid") { MountOption::NoSuid } else { MountOption::Suid });
    cfg.mount_options.push(if has("noexec") { MountOption::NoExec } else { MountOption::Exec });
    if has("ro") {
        cfg.mount_options.push(MountOption::RO);
    }
    cfg.acl = SessionACL::All; // allow_other: non-root users use the mount of a root daemon
    cfg.n_threads = Some(o.threads);
    cfg.clone_fd = true;
    cfg.uring = o.uring;
    cfg.uring_depth = o.uring_depth;

    let mut session = Session::new(fs, &mountpoint, &cfg)?;
    *shared.notifier.lock() = Some(session.notifier());
    let unmounter = session.unmount_callable();
    let uring_active = session.uring_active();
    let thread = std::thread::Builder::new().name("cairn-fuse".into()).spawn(move || session.run())?;

    let applier = Applier::spawn(core.clone(), shared.clone(), mountpoint.clone());
    core.set_applier(applier.clone());
    Ok(Mount { mountpoint, shared, applier, unmounter, thread: Some(thread), uring_active })
}

impl Mount {
    pub fn unmount(&mut self) -> io::Result<()> {
        self.unmounter.unmount()
    }

    /// Block until the filesystem is unmounted (by `unmount` or externally).
    pub fn wait(&mut self) -> io::Result<()> {
        match self.thread.take() {
            Some(t) => t.join().unwrap_or_else(|_| Err(io::Error::other("fuse thread panicked"))),
            None => Ok(()),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }
}

/// Convenience for tests and tools.
pub fn default_ttl() -> Duration {
    Duration::from_secs(3600)
}
