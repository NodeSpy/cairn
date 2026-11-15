//! E8 probe: what does the NAS kernel/filesystem support, and how fast is it?
//! usage: nas-probe <scratch-dir>
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Instant;

const FICLONE: libc::c_ulong = 0x40049409;
const FICLONERANGE: libc::c_ulong = 0x4020940d;
const FIDEDUPERANGE: libc::c_ulong = 0xc0189436;

#[repr(C)]
struct CloneRange { src_fd: i64, src_offset: u64, src_length: u64, dest_offset: u64 }
#[repr(C)]
struct DedupeHdr { src_offset: u64, src_length: u64, dest_count: u16, reserved1: u16, reserved2: u32 }
#[repr(C)]
struct DedupeInfo { dest_fd: i64, dest_offset: u64, bytes_deduped: u64, status: i32, reserved: u32 }
#[repr(C)]
struct DedupeArg { hdr: DedupeHdr, info: DedupeInfo }

fn err(r: libc::c_int) -> String {
    if r == 0 { "ok".into() } else { format!("ERR {}", std::io::Error::last_os_error()) }
}

fn mkfile(p: &Path, mib: usize, seed: u8) -> std::io::Result<()> {
    let mut f = File::create(p)?;
    let mut buf = vec![0u8; 1 << 20];
    let mut x: u64 = 0x9E3779B97F4A7C15 ^ seed as u64;
    for _ in 0..mib {
        for b in buf.chunks_mut(8) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; b.copy_from_slice(&x.to_le_bytes()[..b.len()]); }
        f.write_all(&buf)?;
    }
    f.sync_all()
}

fn rate(bytes: u64, t: Instant) -> String {
    let s = t.elapsed().as_secs_f64();
    format!("{:.0} MiB/s ({:.2}s)", bytes as f64 / s / 1048576.0, s)
}

fn main() {
    let dir = std::env::args().nth(1).expect("usage: nas-probe <scratch-dir>");
    let d = Path::new(&dir);
    fs::create_dir_all(d).unwrap();
    let mut u: libc::utsname = unsafe { std::mem::zeroed() };
    unsafe { libc::uname(&mut u) };
    let rel = unsafe { std::ffi::CStr::from_ptr(u.release.as_ptr()) }.to_string_lossy().into_owned();
    println!("kernel: {rel}");

    let a = d.join("a.bin");
    let t = Instant::now();
    mkfile(&a, 1024, 1).unwrap();
    println!("seq write 1 GiB + fsync: {}", rate(1 << 30, t));

    // FICLONE whole file
    let src = File::open(&a).unwrap();
    let b = File::create(d.join("b-clone.bin")).unwrap();
    let t = Instant::now();
    let r = unsafe { libc::ioctl(b.as_raw_fd(), FICLONE as _, src.as_raw_fd()) };
    println!("FICLONE 1 GiB: {} in {:?}", err(r), t.elapsed());

    // FICLONERANGE aligned and unaligned
    let c = OpenOptions::new().create(true).write(true).open(d.join("c-range.bin")).unwrap();
    let mut cr = CloneRange { src_fd: src.as_raw_fd() as i64, src_offset: 1 << 20, src_length: 4 << 20, dest_offset: 0 };
    let r = unsafe { libc::ioctl(c.as_raw_fd(), FICLONERANGE as _, &mut cr) };
    println!("FICLONERANGE aligned 4 MiB: {}", err(r));
    cr.src_offset = 1000;
    let r = unsafe { libc::ioctl(c.as_raw_fd(), FICLONERANGE as _, &mut cr) };
    println!("FICLONERANGE unaligned offset: {} (expected EINVAL)", err(r));

    // FIDEDUPERANGE: plain copy then dedupe against the source
    let dpath = d.join("d-copy.bin");
    let t = Instant::now();
    let rc = fs::copy(&a, &dpath);
    println!("fs::copy 1 GiB (copy_file_range path): {:?} {}", rc.as_ref().err(), rate(1 << 30, t));
    let dst = OpenOptions::new().read(true).write(true).open(&dpath).unwrap();
    let mut done = 0u64;
    let t = Instant::now();
    let mut status = 0;
    while done < (1 << 30) {
        let mut arg = DedupeArg {
            hdr: DedupeHdr { src_offset: done, src_length: 16 << 20, dest_count: 1, reserved1: 0, reserved2: 0 },
            info: DedupeInfo { dest_fd: dst.as_raw_fd() as i64, dest_offset: done, bytes_deduped: 0, status: 0, reserved: 0 },
        };
        let r = unsafe { libc::ioctl(src.as_raw_fd(), FIDEDUPERANGE as _, &mut arg) };
        if r != 0 { println!("FIDEDUPERANGE: {} at {done}", err(r)); status = -1; break; }
        if arg.info.status != 0 || arg.info.bytes_deduped == 0 { println!("FIDEDUPERANGE status={} deduped={}", arg.info.status, arg.info.bytes_deduped); status = arg.info.status; break; }
        done += arg.info.bytes_deduped;
    }
    if status == 0 { println!("FIDEDUPERANGE 1 GiB identical: ok {}", rate(done, t)); }
    // differing content must be refused
    let e = d.join("e-diff.bin");
    mkfile(&e, 16, 2).unwrap();
    let ef = OpenOptions::new().read(true).write(true).open(&e).unwrap();
    let mut arg = DedupeArg {
        hdr: DedupeHdr { src_offset: 0, src_length: 16 << 20, dest_count: 1, reserved1: 0, reserved2: 0 },
        info: DedupeInfo { dest_fd: ef.as_raw_fd() as i64, dest_offset: 0, bytes_deduped: 0, status: 0, reserved: 0 },
    };
    let r = unsafe { libc::ioctl(src.as_raw_fd(), FIDEDUPERANGE as _, &mut arg) };
    println!("FIDEDUPERANGE differing: {} status={} (1 = FILE_DEDUPE_RANGE_DIFFERS)", err(r), arg.info.status);

    // sequential read (page cache dropped not possible unprivileged; read the clone after write)
    let mut f = File::open(&a).unwrap();
    let mut buf = vec![0u8; 4 << 20];
    let t = Instant::now();
    let mut n = 0u64;
    loop { let k = f.read(&mut buf).unwrap(); if k == 0 { break; } n += k as u64; }
    println!("seq read 1 GiB (likely cached): {}", rate(n, t));

    // hashing speed
    let data = fs::read(&a).unwrap();
    let t = Instant::now();
    let h1 = blake3::hash(&data);
    println!("blake3 1 thread: {}", rate(data.len() as u64, t));
    let t = Instant::now();
    let mut hs = blake3::Hasher::new();
    hs.update_rayon(&data);
    let h2 = hs.finalize();
    println!("blake3 rayon ({} threads): {} same={}", std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1), rate(data.len() as u64, t), h1 == h2);
    let t = Instant::now();
    for c in data.chunks(64 << 10) { std::hint::black_box(blake3::hash(c)); }
    println!("blake3 64 KiB chunks 1 thread: {}", rate(data.len() as u64, t));

    for p in ["a.bin", "b-clone.bin", "c-range.bin", "d-copy.bin", "e-diff.bin"] { let _ = fs::remove_file(d.join(p)); }
    println!("cleaned up");
}
