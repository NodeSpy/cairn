//! Thin wrappers over the kernel interfaces leasefs is built on:
//! fanotify (pre-content + FID notification groups), file leases
//! (F_SETLEASE), signalfd, file handles, inotify.

use std::ffi::{CStr, CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

// ---- fanotify constants (linux/fanotify.h, kernel 6.14+) ----
pub const FAN_CLOEXEC: u32 = 0x1;
pub const FAN_CLASS_NOTIF: u32 = 0x0;
pub const FAN_CLASS_PRE_CONTENT: u32 = 0x8;
pub const FAN_UNLIMITED_QUEUE: u32 = 0x10;
pub const FAN_UNLIMITED_MARKS: u32 = 0x20;
pub const FAN_REPORT_DIR_FID: u32 = 0x400;
pub const FAN_REPORT_NAME: u32 = 0x800;

pub const FAN_MARK_ADD: u32 = 0x1;
pub const FAN_MARK_REMOVE: u32 = 0x2;
pub const FAN_MARK_DONT_FOLLOW: u32 = 0x4;

pub const FAN_CLOSE_WRITE: u64 = 0x8;
pub const FAN_OPEN: u64 = 0x20;
pub const FAN_CREATE: u64 = 0x100;
pub const FAN_DELETE: u64 = 0x200;
pub const FAN_Q_OVERFLOW: u64 = 0x4000;
pub const FAN_PRE_ACCESS: u64 = 0x0010_0000;
pub const FAN_EVENT_ON_CHILD: u64 = 0x0800_0000;
pub const FAN_RENAME: u64 = 0x1000_0000;
pub const FAN_ONDIR: u64 = 0x4000_0000;

pub const FAN_ALLOW: u32 = 0x1;
pub const FAN_DENY: u32 = 0x2;
pub const FAN_NOFD: i32 = -1;
const FANOTIFY_METADATA_VERSION: u8 = 3;

pub const INFO_DFID_NAME: u8 = 2;
pub const INFO_RANGE: u8 = 6;
pub const INFO_OLD_DFID_NAME: u8 = 10;
pub const INFO_NEW_DFID_NAME: u8 = 12;

fn cvt(r: libc::c_int) -> io::Result<libc::c_int> {
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

fn cstr(p: &Path) -> io::Result<CString> {
    CString::new(p.as_os_str().as_bytes()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
}

pub fn fanotify_init(flags: u32, event_f_flags: u32) -> io::Result<OwnedFd> {
    let fd = cvt(unsafe { libc::fanotify_init(flags, event_f_flags) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Add/remove a mark on the inode `fd` refers to.
pub fn fanotify_mark_fd(group: BorrowedFd, flags: u32, mask: u64, fd: BorrowedFd) -> io::Result<()> {
    cvt(unsafe {
        libc::fanotify_mark(group.as_raw_fd(), flags, mask, fd.as_raw_fd(), std::ptr::null())
    })?;
    Ok(())
}

pub fn fanotify_mark_path(group: BorrowedFd, flags: u32, mask: u64, path: &Path) -> io::Result<()> {
    let c = cstr(path)?;
    cvt(unsafe { libc::fanotify_mark(group.as_raw_fd(), flags, mask, libc::AT_FDCWD, c.as_ptr()) })?;
    Ok(())
}

pub fn fanotify_respond(group: BorrowedFd, fd: RawFd, response: u32) -> io::Result<()> {
    #[repr(C)]
    struct Resp {
        fd: i32,
        response: u32,
    }
    let r = Resp { fd, response };
    let n = unsafe {
        libc::write(group.as_raw_fd(), &r as *const Resp as *const libc::c_void, std::mem::size_of::<Resp>())
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// A file handle as reported by fanotify FID records / name_to_handle_at.
/// Used as a map key, so it is (handle_type, opaque bytes).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Handle {
    pub htype: i32,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Info {
    /// Directory handle + entry name (DFID_NAME / OLD_ / NEW_ variants).
    DirName { kind: u8, dir: Handle, name: Vec<u8> },
    Range { offset: u64, count: u64 },
    Other(u8),
}

#[derive(Debug)]
pub struct Event {
    pub mask: u64,
    /// Event fd (fd-mode groups) or FAN_NOFD. Caller owns it.
    pub fd: RawFd,
    pub pid: i32,
    pub infos: Vec<Info>,
}

/// Blocking read of one batch of fanotify events.
pub fn fanotify_read(group: BorrowedFd, buf: &mut [u8]) -> io::Result<Vec<Event>> {
    let n = unsafe { libc::read(group.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = n as usize;
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 24 <= n {
        let b = &buf[off..];
        let event_len = u32::from_ne_bytes(b[0..4].try_into().unwrap()) as usize;
        let vers = b[4];
        let metadata_len = u16::from_ne_bytes(b[6..8].try_into().unwrap()) as usize;
        let mask = u64::from_ne_bytes(b[8..16].try_into().unwrap());
        let fd = i32::from_ne_bytes(b[16..20].try_into().unwrap());
        let pid = i32::from_ne_bytes(b[20..24].try_into().unwrap());
        if vers != FANOTIFY_METADATA_VERSION || event_len < metadata_len || off + event_len > n {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad fanotify event"));
        }
        let mut infos = Vec::new();
        let mut i = metadata_len;
        while i + 4 <= event_len {
            let h = &b[i..event_len];
            let itype = h[0];
            let ilen = u16::from_ne_bytes(h[2..4].try_into().unwrap()) as usize;
            if ilen < 4 || ilen > h.len() {
                break;
            }
            let rec = &h[..ilen];
            match itype {
                INFO_DFID_NAME | INFO_OLD_DFID_NAME | INFO_NEW_DFID_NAME => {
                    // hdr(4) fsid(8) handle_bytes(4) handle_type(4) f_handle[handle_bytes] name\0
                    let hb = u32::from_ne_bytes(rec[12..16].try_into().unwrap()) as usize;
                    let ht = i32::from_ne_bytes(rec[16..20].try_into().unwrap());
                    let hbytes = rec[20..20 + hb].to_vec();
                    let name_raw = &rec[20 + hb..];
                    let end = name_raw.iter().position(|&c| c == 0).unwrap_or(name_raw.len());
                    infos.push(Info::DirName {
                        kind: itype,
                        dir: Handle { htype: ht, bytes: hbytes },
                        name: name_raw[..end].to_vec(),
                    });
                }
                INFO_RANGE => {
                    // hdr(4) pad(4) offset(8) count(8)
                    let offset = u64::from_ne_bytes(rec[8..16].try_into().unwrap());
                    let count = u64::from_ne_bytes(rec[16..24].try_into().unwrap());
                    infos.push(Info::Range { offset, count });
                }
                t => infos.push(Info::Other(t)),
            }
            i += ilen;
        }
        out.push(Event { mask, fd, pid, infos });
        off += event_len;
    }
    Ok(out)
}

/// name_to_handle_at for a path (does not follow a trailing symlink).
pub fn path_handle(path: &Path) -> io::Result<Handle> {
    const MAX: usize = 128;
    #[repr(C)]
    struct FH {
        handle_bytes: u32,
        handle_type: i32,
        f_handle: [u8; MAX],
    }
    let mut fh = FH { handle_bytes: MAX as u32, handle_type: 0, f_handle: [0; MAX] };
    let mut mount_id: libc::c_int = 0;
    let c = cstr(path)?;
    let r = unsafe {
        libc::syscall(
            libc::SYS_name_to_handle_at,
            libc::AT_FDCWD,
            c.as_ptr(),
            &mut fh as *mut FH,
            &mut mount_id as *mut libc::c_int,
            0,
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(Handle { htype: fh.handle_type, bytes: fh.f_handle[..fh.handle_bytes as usize].to_vec() })
}

// ---- file leases ----

const F_SETSIG: libc::c_int = 10;

pub const LEASE_RD: libc::c_int = libc::F_RDLCK;
pub const LEASE_WR: libc::c_int = libc::F_WRLCK;
pub const LEASE_UN: libc::c_int = libc::F_UNLCK;

pub fn set_lease(fd: BorrowedFd, kind: libc::c_int) -> io::Result<()> {
    cvt(unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETLEASE, kind) })?;
    Ok(())
}

pub fn set_sig(fd: BorrowedFd, sig: libc::c_int) -> io::Result<()> {
    cvt(unsafe { libc::fcntl(fd.as_raw_fd(), F_SETSIG, sig) })?;
    Ok(())
}

/// Signal used for lease-break notifications.
pub fn lease_signal() -> libc::c_int {
    libc::SIGRTMIN() + 2
}

/// Block `sig` in the calling thread (call before spawning threads so all inherit it).
pub fn block_signal(sig: libc::c_int) -> io::Result<()> {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, sig);
        let r = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        if r != 0 {
            return Err(io::Error::from_raw_os_error(r));
        }
    }
    Ok(())
}

pub fn signalfd(sig: libc::c_int) -> io::Result<OwnedFd> {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, sig);
        let fd = cvt(libc::signalfd(-1, &set, libc::SFD_CLOEXEC))?;
        Ok(OwnedFd::from_raw_fd(fd))
    }
}

/// Blocking read of one signal; returns the si_fd field (fd whose lease broke).
pub fn signalfd_read_fd(sfd: BorrowedFd) -> io::Result<RawFd> {
    let mut info: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
    let sz = std::mem::size_of::<libc::signalfd_siginfo>();
    let n = unsafe { libc::read(sfd.as_raw_fd(), &mut info as *mut _ as *mut libc::c_void, sz) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info.ssi_fd)
}

// ---- misc ----

pub fn fd_path(fd: RawFd) -> io::Result<PathBuf> {
    std::fs::read_link(format!("/proc/self/fd/{fd}"))
}

pub fn punch_hole(fd: BorrowedFd, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    cvt(unsafe {
        libc::fallocate(
            fd.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            0,
            len as libc::off_t,
        )
    })?;
    Ok(())
}

pub fn pwrite_all(fd: BorrowedFd, mut data: &[u8], mut off: u64) -> io::Result<()> {
    while !data.is_empty() {
        let n = unsafe {
            libc::pwrite(fd.as_raw_fd(), data.as_ptr() as *const libc::c_void, data.len(), off as libc::off_t)
        };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        data = &data[n as usize..];
        off += n as u64;
    }
    Ok(())
}

pub fn set_mtime_fd(fd: BorrowedFd, mtime_ns: i64) -> io::Result<()> {
    let ts = [
        libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
        libc::timespec { tv_sec: mtime_ns.div_euclid(1_000_000_000), tv_nsec: mtime_ns.rem_euclid(1_000_000_000) },
    ];
    cvt(unsafe { libc::futimens(fd.as_raw_fd(), ts.as_ptr()) })?;
    Ok(())
}

pub fn set_mtime_path_nofollow(path: &Path, mtime_ns: i64) -> io::Result<()> {
    let ts = [
        libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
        libc::timespec { tv_sec: mtime_ns.div_euclid(1_000_000_000), tv_nsec: mtime_ns.rem_euclid(1_000_000_000) },
    ];
    let c = cstr(path)?;
    cvt(unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), ts.as_ptr(), libc::AT_SYMLINK_NOFOLLOW) })?;
    Ok(())
}

/// Replace `fd` with /dev/null, dropping the open file but keeping the
/// descriptor number reserved (fanotify responses are matched by number).
pub fn neuter_fd(fd: RawFd) -> io::Result<()> {
    let devnull = std::fs::File::open("/dev/null")?;
    cvt(unsafe { libc::dup2(devnull.as_raw_fd(), fd) })?;
    Ok(())
}

pub fn close_raw(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

pub fn raise_nofile() {
    unsafe {
        let mut r: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut r) == 0 {
            r.rlim_cur = r.rlim_max;
            libc::setrlimit(libc::RLIMIT_NOFILE, &r);
        }
    }
}

pub fn os(b: &[u8]) -> &OsStr {
    OsStr::from_bytes(b)
}

// ---- inotify (used by the `watch` test tool) ----

pub fn inotify_init() -> io::Result<OwnedFd> {
    let fd = cvt(unsafe { libc::inotify_init1(libc::IN_CLOEXEC) })?;
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub fn inotify_add(fd: BorrowedFd, path: &Path, mask: u32) -> io::Result<i32> {
    let c = cstr(path)?;
    cvt(unsafe { libc::inotify_add_watch(fd.as_raw_fd(), c.as_ptr(), mask) })
}

pub struct InEvent {
    pub wd: i32,
    pub mask: u32,
    pub name: Vec<u8>,
}

pub fn inotify_read(fd: BorrowedFd, buf: &mut [u8]) -> io::Result<Vec<InEvent>> {
    let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut out = Vec::new();
    let mut off = 0usize;
    let n = n as usize;
    while off + 16 <= n {
        let wd = i32::from_ne_bytes(buf[off..off + 4].try_into().unwrap());
        let mask = u32::from_ne_bytes(buf[off + 4..off + 8].try_into().unwrap());
        let len = u32::from_ne_bytes(buf[off + 12..off + 16].try_into().unwrap()) as usize;
        let raw = &buf[off + 16..off + 16 + len];
        let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
        out.push(InEvent { wd, mask, name: raw[..end].to_vec() });
        off += 16 + len;
    }
    Ok(out)
}

#[allow(dead_code)]
pub fn cstr_lossy(c: &CStr) -> String {
    c.to_string_lossy().into_owned()
}
