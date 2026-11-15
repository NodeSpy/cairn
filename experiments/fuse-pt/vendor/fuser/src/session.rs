//! Filesystem session
//!
//! A session runs a filesystem implementation while it is being mounted to a specific mount
//! point. A session begins by mounting the filesystem and ends by unmounting it. While the
//! filesystem is mounted, the session loop receives, dispatches and replies to kernel requests
//! for filesystem operations under its mount point.

use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::thread::{self};

use log::debug;
use log::error;
use log::info;
use log::warn;
use nix::unistd::Uid;
use nix::unistd::geteuid;
use parking_lot::Mutex;

use crate::Errno;
use crate::Filesystem;
use crate::KernelConfig;
use crate::MountOption;
use crate::ReplyEmpty;
use crate::Request;
use crate::channel::Channel;
use crate::channel::ChannelSender;
use crate::dev_fuse::DevFuse;
use crate::ll;
use crate::ll::Operation;
use crate::ll::ResponseErrno;
use crate::ll::Version;
use crate::ll::flags::init_flags::InitFlags;
use crate::ll::fuse_abi as abi;
use crate::mnt::Mount;
use crate::mnt::mount_options::Config;
use crate::mnt::mount_options::check_option_conflicts;
use crate::notify::Notifier;
use crate::read_buf::FuseReadBuf;
use crate::reply::Reply;
use crate::reply::ReplyRaw;
use crate::reply::ReplySender;
use crate::request::RequestWithSender;

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
pub(crate) const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Default, Debug, Eq, PartialEq, Clone, Copy)]
/// How requests should be filtered based on the calling UID.
pub enum SessionACL {
    /// Allow requests from any user. Corresponds to the `allow_other` mount option.
    All,
    /// Allow requests from root. Corresponds to the `allow_root` mount option.
    RootAndOwner,
    /// Allow requests from the owning UID. This is FUSE's default mode of operation.
    #[default]
    Owner,
}

impl SessionACL {
    /// Returns the mount option string for kernel/fusermount/libfuse paths.
    /// Both `All` and `RootAndOwner` map to `allow_other` - the kernel only
    /// understands `allow_other`, and fuser enforces the root-only restriction internally.
    #[allow(dead_code)]
    pub(crate) fn to_mount_option(self) -> Option<&'static str> {
        match self {
            SessionACL::All | SessionACL::RootAndOwner => Some("allow_other"),
            SessionACL::Owner => None,
        }
    }
}

/// Calls `destroy` on drop.
#[derive(Debug)]
pub(crate) struct FilesystemHolder<FS: Filesystem> {
    pub(crate) fs: Option<FS>,
}

impl<FS: Filesystem> FilesystemHolder<FS> {
    fn destroy(&mut self) {
        if let Some(mut fs) = self.fs.take() {
            fs.destroy();
        }
    }
}

impl<FS: Filesystem> Drop for FilesystemHolder<FS> {
    fn drop(&mut self) {
        self.destroy();
    }
}

#[derive(Debug)]
struct UmountOnDrop {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl UmountOnDrop {
    fn umount(&self) -> io::Result<()> {
        if let Some(mount) = self.mount.lock().take() {
            mount.umount()?;
        }
        Ok(())
    }
}

impl Drop for UmountOnDrop {
    fn drop(&mut self) {
        if let Err(e) = self.umount() {
            warn!("Failed to umount filesystem: {}", e);
        }
    }
}

/// The session data structure
#[derive(Debug)]
pub struct Session<FS: Filesystem> {
    /// Filesystem operation implementations. None after `destroy` called.
    pub(crate) filesystem: FilesystemHolder<FS>,
    /// Communication channel to the kernel driver
    pub(crate) ch: Channel,
    /// Handle to the mount.  Dropping this unmounts.
    mount: UmountOnDrop,
    /// Whether to restrict access to owner, root + owner, or unrestricted
    /// Used to implement `allow_root` and `auto_unmount`
    pub(crate) allowed: SessionACL,
    /// User that launched the fuser process
    pub(crate) session_owner: Uid,
    /// FUSE protocol version, as reported by the kernel.
    /// The field is set to `Some` when the init message is received.
    pub(crate) proto_version: Option<Version>,
    pub(crate) config: Config,
    /// FUSE_OVER_IO_URING was negotiated in FUSE_INIT.
    pub(crate) uring: bool,
    /// Ring payload buffer size: max(max_write, max_pages * page size) as negotiated.
    pub(crate) uring_payload: usize,
}

impl<FS: Filesystem> AsFd for Session<FS> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.ch.as_fd()
    }
}

impl<FS: Filesystem> Session<FS> {
    /// Create a new session by mounting the given filesystem to the given mountpoint
    /// # Errors
    /// Returns an error if the options are incorrect, or if the fuse device can't be mounted.
    pub fn new<P: AsRef<Path>>(
        filesystem: FS,
        mountpoint: P,
        options: &Config,
    ) -> io::Result<Session<FS>> {
        check_option_conflicts(options)?;

        let mountpoint = mountpoint.as_ref();
        info!("Mounting {}", mountpoint.display());
        // If AutoUnmount is requested, but not AllowRoot or AllowOther, return an error
        // because fusermount needs allow_root or allow_other to handle the auto_unmount option
        if options.mount_options.contains(&MountOption::AutoUnmount)
            && options.acl == SessionACL::Owner
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("auto_unmount requires acl != Owner, got: {:?}", options.acl),
            ));
        }
        let (file, mount) = Mount::new(mountpoint, &options.mount_options, options.acl)?;

        let ch = Channel::new(file);

        let mut session = Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(Some(mount))),
            },
            allowed: options.acl,
            session_owner: geteuid(),
            proto_version: None,
            config: options.clone(),
            uring: false,
            uring_payload: 0,
        };

        session.handshake()?;

        Ok(session)
    }

    /// Wrap an existing /dev/fuse file descriptor. This doesn't mount the
    /// filesystem anywhere; that must be done separately.
    pub fn from_fd(
        filesystem: FS,
        fd: OwnedFd,
        acl: SessionACL,
        config: Config,
    ) -> io::Result<Self> {
        let ch = Channel::new(Arc::new(DevFuse(File::from(fd))));
        let mut session = Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(None)),
            },
            allowed: acl,
            session_owner: geteuid(),
            proto_version: None,
            config,
            uring: false,
            uring_payload: 0,
        };

        session.handshake()?;

        Ok(session)
    }

    /// Run the session loop in a background thread. If the returned handle is dropped,
    /// the filesystem is unmounted and the given session ends.
    pub fn spawn(self) -> io::Result<BackgroundSession> {
        let sender = self.ch.sender();
        // Take the fuse_session, so that we can unmount it
        let mount = std::mem::take(&mut *self.mount.mount.lock());
        let guard = thread::Builder::new()
            .name("fuser-bg".to_string())
            .spawn(move || self.run())?;
        Ok(BackgroundSession {
            guard,
            sender,
            mount,
        })
    }

    /// Run the session loop that receives kernel requests and dispatches them to method
    /// calls into the filesystem. This read-dispatch-loop is non-concurrent to prevent
    /// having multiple buffers (which take up much memory), but the filesystem methods
    /// may run concurrent by spawning threads.
    /// # Errors
    /// Returns any final error when the session comes to an end.
    pub fn run(self) -> io::Result<()> {
        let Session {
            filesystem,
            ch,
            mount: _do_not_umount_yet,
            allowed,
            session_owner,
            proto_version: _,
            config,
            uring,
            uring_payload: kernel_payload_cap,
        } = self;

        let n_threads = config.n_threads.unwrap_or(1);

        if !cfg!(target_os = "linux") && n_threads != 1 {
            // TODO: check whether it works on macOS/FreeBSD and enable if it works.
            return Err(io::Error::other(
                "n_threads != 1 is only supported on Linux",
            ));
        }

        let Some(_) = n_threads.checked_sub(1) else {
            return Err(io::Error::other("n_threads"));
        };

        let mut filesystem = Arc::new(filesystem);

        // With io_uring a single classic reader is enough (FORGET, INTERRUPT); the others are only
        // started if ring registration fails and the kernel falls back to /dev/fuse.
        let n_readers = if uring { 1 } else { n_threads };
        let ring_ch = ch.clone();
        let mut channels = Vec::with_capacity(n_readers);

        for _ in 1..n_readers {
            if config.clone_fd {
                #[cfg(target_os = "linux")]
                {
                    channels.push(ch.clone_fd()?);
                    continue;
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(io::Error::other("clone_fd is only supported on Linux"));
                }
            } else {
                channels.push(ch.clone());
            }
        }
        channels.push(ch);

        let mut threads = Vec::with_capacity(n_threads);

        for (i, ch) in channels.into_iter().enumerate() {
            let thread_name = format!("fuser-{i}");
            let event_loop = SessionEventLoop {
                thread_name: thread_name.clone(),
                filesystem: filesystem.clone(),
                ch,
                allowed,
                session_owner,
            };
            threads.push(
                thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || event_loop.event_loop())?,
            );
        }

        let shared = crate::uring::RingShared::new();
        if uring {
            let depth = if config.uring_depth == 0 { crate::uring::DEFAULT_DEPTH } else { config.uring_depth };
            let payload_cap = kernel_payload_cap.max(8192);
            let nq = crate::uring::possible_cpus();
            info!("FUSE over io_uring: {nq} queues x {depth} entries, payload {payload_cap} bytes");
            for q in 0..nq {
                let shared = shared.clone();
                let fs = filesystem.clone();
                let ch = ring_ch.clone();
                let fail_test = config.uring_fail_test;
                let clone_fd = config.clone_fd;
                let extra = n_threads.saturating_sub(1);
                threads.push(
                    thread::Builder::new().name(format!("fuser-ring-{q}")).spawn(move || {
                        let ev = SessionEventLoop {
                            thread_name: format!("ring-{q}"),
                            filesystem: fs.clone(),
                            ch: ch.clone(),
                            allowed,
                            session_owner,
                        };
                        let spawn_readers = || {
                            (0..extra)
                                .filter_map(|i| {
                                    let c = if clone_fd { ch.clone_fd().unwrap_or_else(|_| ch.clone()) } else { ch.clone() };
                                    let ev = SessionEventLoop {
                                        thread_name: format!("fuser-fb-{i}"),
                                        filesystem: fs.clone(),
                                        ch: c,
                                        allowed,
                                        session_owner,
                                    };
                                    thread::Builder::new().name(format!("fuser-fb-{i}")).spawn(move || ev.event_loop()).ok()
                                })
                                .collect()
                        };
                        crate::uring::ring_loop(&ev, q, depth, payload_cap, fail_test, &shared, &spawn_readers)
                    })?,
                );
            }
        }

        let mut reply: io::Result<()> = Ok(());
        let join_all = |threads: Vec<thread::JoinHandle<io::Result<()>>>, reply: &mut io::Result<()>| {
            for thread in threads {
                match thread.join() {
                    Ok(Err(e)) if reply.is_ok() => *reply = Err(e),
                    Ok(_) => {}
                    Err(_) => {
                        if reply.is_ok() {
                            *reply = Err(io::Error::other("event loop thread panicked"));
                        }
                    }
                }
            }
        };
        join_all(threads, &mut reply);
        let extra = std::mem::take(&mut *shared.extra_readers.lock());
        join_all(extra, &mut reply);
        drop(shared);

        let Some(filesystem) = Arc::get_mut(&mut filesystem) else {
            return Err(io::Error::other(
                "BUG: must have one refcount for filesystem",
            ));
        };

        filesystem.destroy();

        reply
    }

    fn handshake(&mut self) -> io::Result<()> {
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();

        loop {
            // Read the init request from the kernel
            let size = match self.ch.receive_retrying(buf) {
                Ok(size) => size,
                Err(nix::errno::Errno::ENODEV) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "FUSE device disconnected during handshake",
                    ));
                }
                Err(err) => return Err(err.into()),
            };

            // Parse the request
            let request = match ll::AnyRequest::try_from(&buf[..size]) {
                Ok(request) => request,
                Err(err) => {
                    error!("{err}");
                    return Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()));
                }
            };

            // Extract the init operation
            let op = match request.operation() {
                Ok(op) => op,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Failed to parse FUSE operation",
                    ));
                }
            };

            let init = match op {
                ll::Operation::Init(init) => init,
                _ => {
                    error!("Received non-init FUSE operation before init: {}", request);
                    // Send error response and return error - non-init during handshake is invalid
                    <ReplyRaw as Reply>::new(
                        request.unique(),
                        ReplySender::Channel(self.ch.sender()),
                    )
                    .send_ll(&ResponseErrno(ll::Errno::EIO));
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Received non-init FUSE operation during handshake",
                    ));
                }
            };

            let v = init.version();
            if v.0 > abi::FUSE_KERNEL_VERSION {
                // Kernel has a newer major version than we support.
                // Send our version and wait for a second INIT request with a compatible version.
                debug!(
                    "INIT: Kernel version {} > our version {}, sending our version and waiting for next init",
                    v.0,
                    abi::FUSE_KERNEL_VERSION
                );
                let response = init.reply_version_only();
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&response);
                continue;
            }

            // We don't support ABI versions before 7.6
            if v < Version(7, 6) {
                error!("Unsupported FUSE ABI version {v}");
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&ResponseErrno(ll::Errno::EPROTO));
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Unsupported FUSE ABI version {v}"),
                ));
            }

            let mut config = KernelConfig::new(init.capabilities(), init.max_readahead(), v);

            // Call filesystem init method and give it a chance to return an error
            let Some(filesystem) = &mut self.filesystem.fs else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Bug: filesystem must be initialized during handshake",
                ));
            };
            let res = filesystem.init(Request::ref_cast(request.header()), &mut config);
            if let Err(error) = res {
                let errno = Errno::from_i32(error.raw_os_error().unwrap_or(0));
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&ResponseErrno(errno));
                return Err(error);
            }

            // Offer FUSE over io_uring only if a ring can be created here (the kernel would otherwise
            // wait for queue registrations that never come).
            if self.config.uring {
                if !crate::uring::probe() {
                    warn!("io_uring unavailable, using /dev/fuse");
                } else if config.add_capabilities(InitFlags::FUSE_OVER_IO_URING).is_err() {
                    warn!("kernel did not offer FUSE_OVER_IO_URING (fuse.enable_uring=0?), using /dev/fuse");
                } else {
                    self.uring = true;
                    self.uring_payload = config.ring_payload();
                }
            }

            // Remember the ABI version supported by kernel and mark the session initialized.
            self.proto_version = Some(v);

            // Log capability status for debugging
            for bit in 0..64 {
                let bitflags = InitFlags::from_bits_retain(1 << bit);
                if bitflags == InitFlags::FUSE_INIT_EXT {
                    continue;
                }
                let bitflag_is_known = InitFlags::all().contains(bitflags);
                let kernel_supports = init.capabilities().contains(bitflags);
                let we_requested = config.requested.contains(bitflags);
                // On macOS, there's a clash between linux and macOS constants,
                // so we pick macOS ones (last).
                let name = if let Some((name, _)) = bitflags.iter_names().last() {
                    Cow::Borrowed(name)
                } else {
                    Cow::Owned(format!("(1 << {bit})"))
                };
                if we_requested && kernel_supports {
                    debug!("capability {name} enabled")
                } else if we_requested {
                    debug!("capability {name} not supported by kernel")
                } else if kernel_supports {
                    debug!("capability {name} not requested by client")
                } else if bitflag_is_known {
                    debug!("capability {name} not supported nor requested")
                }
            }

            // Reply with our desired version and settings.
            debug!(
                "INIT response: ABI {}.{}, flags {:#x}, max readahead {}, max write {}",
                abi::FUSE_KERNEL_VERSION,
                abi::FUSE_KERNEL_MINOR_VERSION,
                init.capabilities() & config.requested,
                config.max_readahead,
                config.max_write
            );

            let response = init.reply(&config);
            <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                .send_ll(&response);

            return Ok(());
        }
    }

    /// True if FUSE over io_uring was negotiated (`Config::uring` and the kernel offered it).
    pub fn uring_active(&self) -> bool {
        self.uring
    }

    /// Unmount the filesystem
    pub fn unmount(&mut self) -> io::Result<()> {
        self.mount.umount()
    }

    /// Returns a thread-safe object that can be used to unmount the Filesystem
    pub fn unmount_callable(&mut self) -> SessionUnmounter {
        SessionUnmounter {
            mount: self.mount.mount.clone(),
        }
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.ch.sender())
    }
}

#[derive(Debug)]
/// A thread-safe object that can be used to unmount a Filesystem
pub struct SessionUnmounter {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl SessionUnmounter {
    /// Unmount the filesystem
    pub fn unmount(&mut self) -> io::Result<()> {
        if let Some(mount) = std::mem::take(&mut *self.mount.lock()) {
            mount.umount()?;
        }
        Ok(())
    }
}

pub(crate) struct SessionEventLoop<FS: Filesystem> {
    /// Cache thread name for faster `debug!`.
    pub(crate) thread_name: String,
    pub(crate) ch: Channel,
    pub(crate) filesystem: Arc<FilesystemHolder<FS>>,
    pub(crate) allowed: SessionACL,
    pub(crate) session_owner: Uid,
}

impl<FS: Filesystem> SessionEventLoop<FS> {
    pub(crate) fn event_loop(&self) -> io::Result<()> {
        // Buffer for receiving requests from the kernel. Only one is allocated and
        // it is reused immediately after dispatching to conserve memory and allocations.
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();
        loop {
            // Read the next request from the given channel to kernel driver
            // The kernel driver makes sure that we get exactly one request per read
            match self.ch.receive_retrying(buf) {
                Ok(size) => match RequestWithSender::new(ReplySender::Channel(self.ch.sender()), &buf[..size]) {
                    // Dispatch request
                    Some(req) => {
                        if let Ok(Operation::Destroy(_)) = req.request.operation() {
                            req.reply::<ReplyEmpty>().ok();
                            return Ok(());
                        } else {
                            req.dispatch(self)
                        }
                    }
                    // Quit loop on illegal request
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid request",
                        ));
                    }
                },
                Err(nix::errno::Errno::ENODEV) => return Ok(()),
                Err(err) => return Err(err.into()),
            }
        }
    }
}

/// The background session data structure
#[derive(Debug)]
pub struct BackgroundSession {
    /// Thread guard of the background session
    pub guard: JoinHandle<io::Result<()>>,
    /// Object for creating Notifiers for client use
    sender: ChannelSender,
    /// Ensures the filesystem is unmounted when the session ends
    mount: Option<Mount>,
}

impl BackgroundSession {
    /// Unmount the filesystem and join the background thread.
    pub fn umount_and_join(mut self) -> io::Result<()> {
        if let Some(mount) = self.mount.take() {
            mount.umount()?;
        }
        self.join()
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.sender.clone())
    }

    /// Join the filesystem thread.
    pub fn join(self) -> io::Result<()> {
        self.guard
            .join()
            .map_err(|_panic: Box<dyn std::any::Any + Send>| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "filesystem background thread panicked",
                )
            })?
    }
}
