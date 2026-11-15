//! FUSE-over-io_uring transport (kernel 6.14+, `fuse.enable_uring=1`).
//!
//! FUSE_INIT is still exchanged on `/dev/fuse`. When the kernel offers `FUSE_OVER_IO_URING` and we echo
//! it, one SQE128 ring is created per possible CPU (queue id = CPU), each with `depth` registered
//! entries. Requests are delivered as CQEs on the ring of the submitting CPU, dispatched inline on
//! the ring's thread through the normal decoder and `Reply*` types, and answered with
//! `COMMIT_AND_FETCH`, which also re-arms the entry. FORGET and INTERRUPT still arrive on `/dev/fuse`
//! (classic readers keep running); notifications keep using `write(2)`.
//!
//! Restriction: handlers must reply before they return (the reply is written into the entry's
//! buffers and committed right after dispatch). A reply sent later or from another thread is
//! rejected: the entry is committed with EIO when dispatch returns.

use std::cell::Cell;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use io_uring::cqueue;
use io_uring::opcode;
use io_uring::squeue;
use io_uring::types;
use io_uring::IoUring;
use log::debug;
use log::error;
use log::warn;
use parking_lot::Mutex;

use crate::channel::ChannelSender;
use crate::ll::Operation;
use crate::ll::fuse_abi::fuse_in_header;
use crate::reply::ReplySender;
use crate::request::RequestWithSender;
use crate::session::SessionEventLoop;
use crate::Filesystem;
use crate::ReplyEmpty;

/// Header buffer: `in_out[128]`, `op_in[128]`, `ring_ent_in_out` (>= 288 bytes required).
const HDR_SZ: usize = 512;
const OFF_OP_IN: usize = 128;
const OFF_ENT: usize = 256;
const OP_IN_MAX: usize = 128;
const IN_HDR: usize = size_of::<fuse_in_header>();
const CMD_REGISTER: u32 = 1;
const CMD_COMMIT_AND_FETCH: u32 = 2;
pub(crate) const DEFAULT_DEPTH: usize = 4;

/// Number of ring queues the kernel expects: `num_possible_cpus()`.
pub(crate) fn possible_cpus() -> usize {
    if let Ok(s) = std::fs::read_to_string("/sys/devices/system/cpu/possible") {
        let max = s
            .trim()
            .split(',')
            .filter_map(|r| r.rsplit('-').next()?.parse::<usize>().ok())
            .max();
        if let Some(m) = max {
            return m + 1;
        }
    }
    (unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) }).max(1) as usize
}

/// True if an SQE128 ring can be created at all (otherwise do not offer the feature to the kernel,
/// which would then wait forever for queues to register).
pub(crate) fn probe() -> bool {
    IoUring::<squeue::Entry128, cqueue::Entry>::builder().build(2).is_ok()
}

/// Per-entry reply slot. Lives in the entry (stable address) for as long as the ring thread runs.
pub(crate) struct Slot {
    hdr: *mut u8,
    payload: *mut u8,
    cap: usize,
    /// Bumped each time a request is delivered and again when its reply is committed.
    epoch: AtomicU64,
    replied: Cell<bool>,
}

/// Reply target handed to the `Reply*` types for one ring request.
#[derive(Debug, Clone)]
pub(crate) struct RingReply {
    pub(crate) ch: ChannelSender,
    slot: *const Slot,
    epoch: u64,
}
// The slot is only written from `send`, which handlers call on the ring thread before returning
// (see module docs); the generation check rejects late replies.
unsafe impl Send for RingReply {}
unsafe impl Sync for RingReply {}
impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Slot")
    }
}

impl RingReply {
    /// Split the reply (a `fuse_out_header` followed by the args) into the entry's `in_out` header
    /// and payload buffer.
    pub(crate) fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let slot = unsafe { &*self.slot };
        if slot.epoch.load(Ordering::Acquire) != self.epoch || slot.replied.get() {
            return Err(io::Error::other("late or duplicate ring reply"));
        }
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total < 16 || total - 16 > slot.cap {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "ring reply does not fit"));
        }
        let mut off = 0usize;
        for b in bufs {
            let mut done = 0;
            while done < b.len() {
                let (dst, n) = if off < 16 {
                    (unsafe { slot.hdr.add(off) }, (16 - off).min(b.len() - done))
                } else {
                    (unsafe { slot.payload.add(off - 16) }, b.len() - done)
                };
                unsafe { std::ptr::copy_nonoverlapping(b.as_ptr().add(done), dst, n) };
                off += n;
                done += n;
            }
        }
        unsafe { std::ptr::write_unaligned(slot.hdr.add(OFF_ENT + 16) as *mut u32, (total - 16) as u32) };
        slot.replied.set(true);
        Ok(())
    }
}

/// Anonymous lazily-committed mapping (payload buffers are up to max_write, mostly untouched).
struct Map(*mut u8, usize);
impl Map {
    fn new(len: usize) -> io::Result<Map> {
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Map(p.cast(), len))
    }
}
impl Drop for Map {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.0.cast(), self.1) };
    }
}

struct Ent {
    _hdr: Map,
    _payload: Map,
    iov: Box<[libc::iovec; 2]>,
    slot: Box<Slot>,
    /// Has this entry ever received a request (i.e. is the registration known to be good)?
    fetched: bool,
    retries: u32,
}

fn sqe(fd: i32, cmd_op: u32, qid: u16, commit_id: u64, iov: Option<&[libc::iovec; 2]>, ud: u64) -> squeue::Entry128 {
    // struct fuse_uring_cmd_req { u64 flags; u64 commit_id; u16 qid; u8 pad[6]; }
    let mut cmd = [0u8; 80];
    cmd[8..16].copy_from_slice(&commit_id.to_le_bytes());
    cmd[16..18].copy_from_slice(&qid.to_le_bytes());
    let mut b = opcode::UringCmd80::new(types::Fd(fd), cmd_op).cmd(cmd);
    if let Some(iov) = iov {
        b = b.addr(Some(iov.as_ptr() as u64));
    }
    let mut e = b.build();
    e.set_user_data(ud);
    if iov.is_some() {
        // sqe->len = number of iovecs; `UringCmd80` has no setter for it (offset 24 in the SQE).
        unsafe { *((&mut e as *mut squeue::Entry128 as *mut u8).add(24) as *mut u32) = 2 };
    }
    e
}

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set);
    }
}

fn is_shutdown(res: i32) -> bool {
    matches!(-res, libc::ENOTCONN | libc::ECANCELED | libc::ECONNABORTED | libc::ENODEV | libc::ENOENT | libc::EINTR)
}

/// State shared by the ring threads of one session.
pub(crate) struct RingShared {
    /// Set when a REGISTER failed: the kernel then falls back to `/dev/fuse` and the classic
    /// readers must take over all the work.
    pub(crate) fallback: AtomicBool,
    pub(crate) extra_readers: Mutex<Vec<std::thread::JoinHandle<io::Result<()>>>>,
}

impl RingShared {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(RingShared { fallback: AtomicBool::new(false), extra_readers: Mutex::new(Vec::new()) })
    }
}

/// Body of one ring thread (queue `qid`).
pub(crate) fn ring_loop<FS: Filesystem>(
    ev: &SessionEventLoop<FS>,
    qid: usize,
    depth: usize,
    payload_cap: usize,
    fail_test: bool,
    shared: &RingShared,
    spawn_readers: &dyn Fn() -> Vec<std::thread::JoinHandle<io::Result<()>>>,
) -> io::Result<()> {
    pin(qid);
    let fd = ev.ch.as_fd_raw();
    let mut ring: IoUring<squeue::Entry128, cqueue::Entry> = IoUring::builder().build((depth * 2) as u32)?;
    let cap = payload_cap.next_multiple_of(4096);
    let mut ents: Vec<Ent> = Vec::with_capacity(depth);
    for _ in 0..depth {
        let hdr = Map::new(HDR_SZ.next_multiple_of(4096))?;
        let payload = Map::new(cap)?;
        let iov = Box::new([
            libc::iovec { iov_base: hdr.0.cast(), iov_len: HDR_SZ },
            libc::iovec { iov_base: payload.0.cast(), iov_len: cap },
        ]);
        let slot = Box::new(Slot { hdr: hdr.0, payload: payload.0, cap, epoch: AtomicU64::new(0), replied: Cell::new(false) });
        ents.push(Ent { _hdr: hdr, _payload: payload, iov, slot, fetched: false, retries: 0 });
    }
    // Contiguous request for the decoder: in_header + op_in + payload (u64 backing for alignment).
    let mut scratch = vec![0u64; (IN_HDR + OP_IN_MAX + cap) / 8 + 1];
    let scratch: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(scratch.as_mut_ptr().cast(), scratch.len() * 8) };

    let (submitter, mut sq, mut cq) = ring.split();
    for (i, e) in ents.iter().enumerate() {
        let q = if fail_test && qid == 0 && i == 0 { 0xfff0 } else { qid as u16 };
        unsafe { sq.push(&sqe(fd, CMD_REGISTER, q, 0, Some(&e.iov), i as u64)).map_err(io::Error::other)? };
    }
    sq.sync();
    let mut live = depth;
    while live > 0 {
        if let Err(e) = submitter.submit_and_wait(1) {
            match e.raw_os_error() {
                Some(libc::EINTR) | Some(libc::EBUSY) => {}
                _ => return Err(e),
            }
        }
        cq.sync();
        while let Some(cqe) = cq.next() {
            let i = cqe.user_data() as usize;
            let res = cqe.result();
            let ent = &mut ents[i];
            if res == -libc::EAGAIN && ent.retries < 500 {
                // REGISTER raced the end of FUSE_INIT processing in the kernel; retry.
                ent.retries += 1;
                std::thread::sleep(Duration::from_millis(20));
                let e = sqe(fd, CMD_REGISTER, qid as u16, 0, Some(&ent.iov), i as u64);
                unsafe { sq.push(&e).map_err(io::Error::other)? };
                continue;
            }
            if res < 0 {
                live -= 1;
                if !ent.fetched && !is_shutdown(res) {
                    // Registration failed: the kernel drops back to /dev/fuse for the whole connection.
                    error!("io_uring queue {qid} entry {i}: register failed ({}), falling back to /dev/fuse", io::Error::from_raw_os_error(-res));
                    if !shared.fallback.swap(true, Ordering::SeqCst) {
                        shared.extra_readers.lock().extend(spawn_readers());
                    }
                } else if !is_shutdown(res) {
                    warn!("io_uring queue {qid} entry {i}: completion error {res}");
                }
                continue;
            }
            ent.fetched = true;
            // Reassemble the request for the decoder.
            let h = unsafe { std::slice::from_raw_parts(ent.slot.hdr, HDR_SZ) };
            let psz = u32::from_ne_bytes(h[OFF_ENT + 16..OFF_ENT + 20].try_into().unwrap()) as usize;
            let commit_id = u64::from_ne_bytes(h[OFF_ENT + 8..OFF_ENT + 16].try_into().unwrap());
            let total = u32::from_ne_bytes(h[0..4].try_into().unwrap()) as usize;
            let unique = u64::from_ne_bytes(h[8..16].try_into().unwrap());
            let oplen = total.wrapping_sub(IN_HDR).wrapping_sub(psz);
            let slot: &Slot = &ent.slot;
            let epoch = slot.epoch.fetch_add(1, Ordering::AcqRel) + 1;
            slot.replied.set(false);
            if oplen > OP_IN_MAX || psz > cap {
                error!("bad ring request: len {total} payload {psz}");
            } else {
                scratch[..IN_HDR].copy_from_slice(&h[..IN_HDR]);
                scratch[IN_HDR..IN_HDR + oplen].copy_from_slice(&h[OFF_OP_IN..OFF_OP_IN + oplen]);
                let payload = unsafe { std::slice::from_raw_parts(slot.payload, psz) };
                scratch[IN_HDR + oplen..total].copy_from_slice(payload);
                let sender = ReplySender::Ring(RingReply { ch: ev.ch.sender(), slot: slot as *const Slot, epoch });
                match RequestWithSender::new(sender, &scratch[..total]) {
                    Some(req) => {
                        if let Ok(Operation::Destroy(_)) = req.request.operation() {
                            req.reply::<ReplyEmpty>().ok();
                        } else {
                            req.dispatch(ev);
                        }
                    }
                    None => error!("undecodable ring request"),
                }
            }
            if !slot.replied.get() {
                // Handler did not reply inline (or the request was bad): fail it with EIO.
                debug!("ring request {unique} not replied inline");
                let out = unsafe { std::slice::from_raw_parts_mut(slot.hdr, 16) };
                out[0..4].copy_from_slice(&16u32.to_ne_bytes());
                out[4..8].copy_from_slice(&(-libc::EIO).to_ne_bytes());
                out[8..16].copy_from_slice(&unique.to_ne_bytes());
                unsafe { std::ptr::write_unaligned(slot.hdr.add(OFF_ENT + 16) as *mut u32, 0) };
            }
            slot.epoch.fetch_add(1, Ordering::AcqRel); // invalidate any outstanding RingReply
            let e = sqe(fd, CMD_COMMIT_AND_FETCH, qid as u16, commit_id, None, i as u64);
            unsafe { sq.push(&e).map_err(io::Error::other)? };
        }
        sq.sync();
    }
    Ok(())
}

impl crate::channel::Channel {
    pub(crate) fn as_fd_raw(&self) -> i32 {
        use std::os::fd::AsFd;
        self.as_fd().as_raw_fd()
    }
}
