//! Small conversions shared by the request handlers and the applier.

use cairn_proto::Kind;
use fuser::FileType;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Nanoseconds since the epoch (may be negative) to the true `SystemTime`.
pub fn ts_ns(ns: i64) -> SystemTime {
    if ns >= 0 {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos(ns.unsigned_abs())
    }
}

/// `SystemTime` as fuser decodes it from a request to wire nanoseconds. fuser 0.18 decodes a wire
/// `(sec < 0, nsec)` as `UNIX_EPOCH - Duration::new(|sec|, nsec)` (a known TODO upstream), so undo that.
pub fn ns_of(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64 * 1_000_000_000 + d.subsec_nanos() as i64,
        Err(e) => {
            let d = e.duration();
            -(d.as_secs() as i64) * 1_000_000_000 + d.subsec_nanos() as i64
        }
    }
}

pub fn now_ns() -> i64 {
    ns_of(SystemTime::now())
}

pub fn file_type(k: Kind) -> FileType {
    match k {
        Kind::File => FileType::RegularFile,
        Kind::Dir => FileType::Directory,
        Kind::Symlink => FileType::Symlink,
        Kind::Fifo => FileType::NamedPipe,
        Kind::Socket => FileType::Socket,
        Kind::CharDev => FileType::CharDevice,
        Kind::BlockDev => FileType::BlockDevice,
    }
}

pub fn type_bits(k: Kind) -> u32 {
    match k {
        Kind::File => libc::S_IFREG,
        Kind::Dir => libc::S_IFDIR,
        Kind::Symlink => libc::S_IFLNK,
        Kind::Fifo => libc::S_IFIFO,
        Kind::Socket => libc::S_IFSOCK,
        Kind::CharDev => libc::S_IFCHR,
        Kind::BlockDev => libc::S_IFBLK,
    }
}

pub fn kind_of_mode(mode: u32) -> Option<Kind> {
    Some(match mode & libc::S_IFMT {
        libc::S_IFREG | 0 => Kind::File,
        libc::S_IFDIR => Kind::Dir,
        libc::S_IFLNK => Kind::Symlink,
        libc::S_IFIFO => Kind::Fifo,
        libc::S_IFSOCK => Kind::Socket,
        libc::S_IFCHR => Kind::CharDev,
        libc::S_IFBLK => Kind::BlockDev,
        _ => return None,
    })
}

/// The kernel's `new_encode_dev` (what FUSE carries in `rdev`) from a glibc `dev_t`.
pub fn enc_dev(d: u64) -> u32 {
    let (major, minor) = (libc::major(d) as u64, libc::minor(d) as u64);
    ((minor & 0xff) | (major << 8) | ((minor & !0xff) << 12)) as u32
}

/// The kernel's `new_decode_dev` to a glibc `dev_t`.
pub fn dec_dev(d: u32) -> u64 {
    let d = d as u64;
    let major = (d & 0xfff00) >> 8;
    let minor = (d & 0xff) | ((d >> 12) & 0xfff00);
    libc::makedev(major as u32, minor as u32)
}

/// Whether thread `tid` (the FUSE request "pid" is really the caller's tid) is a member of `gid`
/// (primary or supplementary). The request carries only uid/gid, so supplementary groups come
/// from /proc.
pub fn in_group(tid: u32, primary: u32, gid: u32) -> bool {
    if primary == gid {
        return true;
    }
    let Ok(s) = std::fs::read_to_string(format!("/proc/{tid}/status")) else { return false };
    s.lines()
        .find_map(|l| l.strip_prefix("Groups:"))
        .map(|g| g.split_whitespace().any(|x| x.parse::<u32>().ok() == Some(gid)))
        .unwrap_or(false)
}

pub fn gettid() -> u32 {
    unsafe { libc::syscall(libc::SYS_gettid) as u32 }
}
