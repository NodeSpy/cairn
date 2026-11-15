use crate::sys::*;
use crate::util::*;
use std::io::{Read, Write};
use std::os::fd::*;
use std::sync::atomic::*;
use std::sync::Arc;

fn flush() {
    let _ = std::io::stdout().flush();
}

pub fn run(cmd: &str, a: &[&str]) {
    match cmd {
        "h-read" => h_read(a[0]),
        "h-pread" => h_pread(a[0], a[1].parse().unwrap(), a[2].parse().unwrap()),
        "h-mmap" => h_mmap(a[0], a[1].parse().unwrap(), a[2].parse().unwrap()),
        "h-otrunc" => h_otrunc(a[0]),
        "h-truncp" => {
            let t0 = now_ns();
            let r = unsafe { libc::truncate(cs(a[0]).as_ptr(), a[1].parse().unwrap()) };
            let e = if r < 0 { errno() } else { 0 };
            println!("t0={} t1={} err={}", t0, now_ns(), e);
        }
        "h-op" => h_op(a[0], a[1]),
        "h-hold" => h_hold(a[0], a[1]),
        "h-e4reader" => h_e4reader(a),
        "h-e4writer" => h_e4writer(a),
        "h-e6handler" => crate::e6::handler(a),
        _ => eprintln!("unknown helper {cmd}"),
    }
}

fn h_read(path: &str) {
    let fd = open_(path, libc::O_RDONLY);
    if fd < 0 {
        println!("open_err={}", errname(errno()));
        return;
    }
    let mut buf = vec![0u8; 65536];
    let (mut tot, mut zeros, mut h, mut err) = (0u64, 0u64, FNV0, 0);
    let t0 = now_ns();
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut _, buf.len()) };
        if n < 0 {
            err = errno();
            break;
        }
        if n == 0 {
            break;
        }
        let s = &buf[..n as usize];
        zeros += s.iter().filter(|&&b| b == 0).count() as u64;
        fnv_update(&mut h, s);
        tot += n as u64;
    }
    println!("bytes={} fnv={:x} zeros={} err={} t0={} t1={}", tot, h, zeros, errname(err), t0, now_ns());
}

fn h_pread(path: &str, off: i64, len: usize) {
    let fd = open_(path, libc::O_RDONLY);
    let mut buf = vec![0u8; len];
    let t0 = now_ns();
    let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, len, off) };
    let e = if n < 0 { errno() } else { 0 };
    let zeros = buf.iter().filter(|&&b| b == 0).count();
    println!("n={} err={} zeros={} t0={} t1={}", n, errname(e), zeros, t0, now_ns());
}

fn h_mmap(path: &str, off: i64, len: usize) {
    let fd = open_(path, libc::O_RDONLY);
    let t0 = now_ns();
    let p = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_SHARED, fd, off) };
    let t1 = now_ns();
    if p == libc::MAP_FAILED {
        println!("mmap_err={} t0={} t1={}", errname(errno()), t0, t1);
        return;
    }
    let s = unsafe { std::slice::from_raw_parts(p as *const u8, len) };
    let mut h = FNV0;
    fnv_update(&mut h, s);
    let t2 = now_ns();
    let zeros = s.iter().filter(|&&b| b == 0).count();
    let p0 = s[0];
    let p5 = if len > 5 * 4096 { s[5 * 4096] } else { 255 };
    println!("t0={} t1={} t2={} fnv={:x} zeros={} byte0={} byte_page5={}", t0, t1, t2, h, zeros, p0, p5);
}

fn h_otrunc(path: &str) {
    let t0 = now_ns();
    let fd = open_(path, libc::O_WRONLY | libc::O_TRUNC);
    let t1 = now_ns();
    let size_after = fstat_(fd).st_size;
    msleep(300);
    let t2 = now_ns();
    let n = unsafe { libc::pwrite(fd, b"x".as_ptr() as *const _, 1, 0) };
    let t3 = now_ns();
    println!("open0={} open1={} write0={} write1={} size_after_open={} wr={}", t0, t1, t2, t3, size_after, n);
}

fn h_op(op: &str, path: &str) {
    let t0 = now_ns();
    let mut fd = -1;
    let mut ret = 0i32;
    match op {
        "wronly" => fd = open_(path, libc::O_WRONLY),
        "rdwr" => fd = open_(path, libc::O_RDWR),
        "rdonly" => fd = open_(path, libc::O_RDONLY),
        "append" => fd = open_(path, libc::O_WRONLY | libc::O_APPEND),
        "nonblock" => fd = open_(path, libc::O_WRONLY | libc::O_NONBLOCK),
        "creat_trunc" => fd = open_(path, libc::O_WRONLY | libc::O_TRUNC),
        "truncp" => ret = unsafe { libc::truncate(cs(path).as_ptr(), 0) },
        "rename" => ret = unsafe { libc::rename(cs(path).as_ptr(), cs(&format!("{path}.renamed")).as_ptr()) },
        "unlink" => ret = unsafe { libc::unlink(cs(path).as_ptr()) },
        "chmod" => ret = unsafe { libc::chmod(cs(path).as_ptr(), 0o600) },
        "utimes" => ret = unsafe { libc::utimensat(libc::AT_FDCWD, cs(path).as_ptr(), std::ptr::null(), 0) },
        "link" => ret = unsafe { libc::link(cs(path).as_ptr(), cs(&format!("{path}.lnk")).as_ptr()) },
        _ => panic!("op"),
    }
    if fd < 0 {
        ret = -1;
    }
    let e = if ret < 0 { errno() } else { 0 };
    println!("t0={} t1={} err={}", t0, now_ns(), errname(e));
}

/// Hold a file open in some way; print READY; wait for stdin EOF.
fn h_hold(path: &str, kind: &str) {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) as usize;
    let mut keep: Vec<i32> = vec![];
    let mut msg = String::new();
    match kind {
        "none" => {}
        "rdonly" => keep.push(open_(path, libc::O_RDONLY)),
        "wronly" => keep.push(open_(path, libc::O_WRONLY)),
        "rdwr" => keep.push(open_(path, libc::O_RDWR)),
        "opath" => keep.push(open_(path, libc::O_PATH)),
        "trunc" => keep.push(open_(path, libc::O_WRONLY | libc::O_TRUNC)),
        "rdwr-read" => {
            let fd = open_(path, libc::O_RDWR);
            let mut b = [0u8; 4096];
            let n = unsafe { libc::pread(fd, b.as_mut_ptr() as *mut _, 4096, 0) };
            msg = format!("pread={} zeros={}", n, b.iter().filter(|&&x| x == 0).count());
            keep.push(fd);
        }
        "wronly-write" => {
            let fd = open_(path, libc::O_WRONLY);
            let n = unsafe { libc::pwrite(fd, b"Z".as_ptr() as *const _, 1, 0) };
            msg = format!("pwrite={}", n);
            keep.push(fd);
        }
        "trunc-write" => {
            let fd = open_(path, libc::O_WRONLY | libc::O_TRUNC);
            let n = unsafe { libc::pwrite(fd, b"Z".as_ptr() as *const _, 1, 0) };
            msg = format!("pwrite={}", n);
            keep.push(fd);
        }
        "trunc-extend" => {
            let fd = open_(path, libc::O_WRONLY | libc::O_TRUNC);
            let r = unsafe { libc::ftruncate(fd, size as i64) };
            msg = format!("ftruncate_extend={}", r);
            keep.push(fd);
        }
        "ftrunc" => {
            let fd = open_(path, libc::O_RDWR);
            let r = unsafe { libc::ftruncate(fd, size as i64 - 1) };
            msg = format!("ftruncate_shrink1={}", r);
            keep.push(fd);
        }
        "ftrunc-same" => {
            let fd = open_(path, libc::O_RDWR);
            let r = unsafe { libc::ftruncate(fd, size as i64) };
            msg = format!("ftruncate_same={}", r);
            keep.push(fd);
        }
        "mmapw-open" | "mmapw-closed" => {
            let fd = open_(path, libc::O_RDWR);
            let p = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0) };
            msg = format!("mmap_ok={}", p != libc::MAP_FAILED);
            if kind == "mmapw-closed" {
                close_(fd);
            } else {
                keep.push(fd);
            }
        }
        "mmapr-closed" => {
            let fd = open_(path, libc::O_RDONLY);
            let p = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
            msg = format!("mmap_ok={}", p != libc::MAP_FAILED);
            close_(fd);
        }
        "mmapp-closed" => {
            let fd = open_(path, libc::O_RDONLY);
            let p = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE, fd, 0) };
            msg = format!("mmap_ok={}", p != libc::MAP_FAILED);
            close_(fd);
        }
        "stale" => {
            let fd = open_(path, libc::O_WRONLY);
            let n = unsafe { libc::pwrite(fd, b"Z".as_ptr() as *const _, 1, 0) };
            msg = format!("pwrite={}", n);
            close_(fd);
        }
        "closed" => {
            let fd = open_(path, libc::O_RDONLY);
            close_(fd);
        }
        _ => panic!("kind {kind}"),
    }
    println!("READY {msg}");
    flush();
    let mut b = [0u8; 64];
    while std::io::stdin().read(&mut b).unwrap_or(0) > 0 {}
}

// ---------------- E4 readers / writers ----------------

fn h_e4reader(a: &[&str]) {
    // dir nfiles size stopfile seed mode(all|hold)
    let dir = a[0];
    let nf: u32 = a[1].parse().unwrap();
    let size: usize = a[2].parse().unwrap();
    let stop = a[3];
    let mut rng = Rng(a[4].parse::<u64>().unwrap() * 0x9E3779B97F4A7C15 | 1);
    let hold = a[5] == "hold";
    let tables: Vec<Vec<u8>> = (0..nf).map(|k| pat_table(k, size)).collect();
    let (mut reads, mut bad, mut zero_bad, mut errs) = (0u64, 0u64, 0u64, 0u64);
    let mut first = String::new();
    let mut check = |k: u32, off: usize, got: &[u8], bad: &mut u64, zb: &mut u64, first: &mut String| {
        let exp = &tables[k as usize][off..off + got.len()];
        if got != exp {
            *bad += 1;
            let i = got.iter().zip(exp).position(|(x, y)| x != y).unwrap();
            if got[i] == 0 {
                *zb += 1;
            }
            if first.is_empty() {
                *first = format!("f{} off={} got={} exp={}", k, off + i, got[i], exp[i]);
            }
        }
    };
    let cstop = cs(stop);
    while unsafe { libc::access(cstop.as_ptr(), libc::F_OK) } != 0 {
        let k = rng.below(nf as u64) as u32;
        let path = format!("{dir}/f{k}");
        let mode = if hold { [3, 4, 5][rng.below(3) as usize] } else { rng.below(6) };
        let fd = open_(&path, libc::O_RDONLY);
        if fd < 0 {
            errs += 1;
            continue;
        }
        reads += 1;
        let mut fd_closed = false;
        match mode {
            0 | 4 => {
                if mode == 4 {
                    usleep(rng.below(2000));
                }
                let mut buf = vec![0u8; size];
                let mut got = 0;
                while got < size {
                    let n = unsafe { libc::read(fd, buf[got..].as_mut_ptr() as *mut _, (size - got)) };
                    if n <= 0 {
                        errs += 1;
                        break;
                    }
                    got += n as usize;
                }
                if got == size {
                    check(k, 0, &buf, &mut bad, &mut zero_bad, &mut first);
                }
            }
            1 => {
                let off = (rng.below((size / 512) as u64) as usize) * 512;
                let len = (1 + rng.below(32768) as usize).min(size - off);
                let mut buf = vec![0u8; len];
                let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, len, off as i64) };
                if n as usize == len {
                    check(k, off, &buf, &mut bad, &mut zero_bad, &mut first);
                } else {
                    errs += 1;
                }
            }
            2 | 3 => {
                let p = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ, libc::MAP_SHARED, fd, 0) };
                if p == libc::MAP_FAILED {
                    errs += 1;
                } else {
                    if mode == 3 {
                        close_(fd);
                        fd_closed = true;
                        usleep(rng.below(1500));
                    }
                    let s = unsafe { std::slice::from_raw_parts(p as *const u8, size) };
                    check(k, 0, s, &mut bad, &mut zero_bad, &mut first);
                    unsafe { libc::munmap(p, size) };
                }
            }
            _ => {
                for _ in 0..(5 + rng.below(15)) {
                    usleep(1000);
                    let off = (rng.below((size / 4096) as u64) as usize) * 4096;
                    let mut buf = vec![0u8; 4096];
                    let n = unsafe { libc::pread(fd, buf.as_mut_ptr() as *mut _, 4096, off as i64) };
                    if n == 4096 {
                        check(k, off, &buf, &mut bad, &mut zero_bad, &mut first);
                    } else {
                        errs += 1;
                    }
                }
            }
        }
        if !fd_closed {
            close_(fd);
        }
    }
    println!("reader hold={} reads={} bad={} zero_bad={} errs={} first_bad={}", hold, reads, bad, zero_bad, errs, first);
}

fn h_e4writer(a: &[&str]) {
    let dir = a[0];
    let nf: u32 = a[1].parse().unwrap();
    let size: usize = a[2].parse().unwrap();
    let stop = a[3];
    let mut rng = Rng(a[4].parse::<u64>().unwrap() * 0x9E3779B97F4A7C15 | 1);
    let (mut ops, mut errs) = (0u64, 0u64);
    let cstop = cs(stop);
    while unsafe { libc::access(cstop.as_ptr(), libc::F_OK) } != 0 {
        let k = rng.below(nf as u64) as u32;
        let path = format!("{dir}/f{k}");
        let mode = rng.below(4);
        let fl = if mode == 1 { libc::O_WRONLY } else { libc::O_RDWR };
        let fd = open_(&path, fl);
        if fd < 0 {
            errs += 1;
            continue;
        }
        ops += 1;
        let off = (rng.below((size / 4096 - 4) as u64) as usize) * 4096;
        let len = 4096 * (1 + rng.below(4) as usize);
        let mut buf = vec![0u8; len];
        fill_pat(k, off as u64, &mut buf);
        match mode {
            0 | 1 | 3 => {
                if mode == 3 {
                    usleep(rng.below(3000));
                }
                if mode == 1 {
                    if unsafe { libc::ftruncate(fd, size as i64) } < 0 {
                        errs += 1;
                    }
                }
                if unsafe { libc::pwrite(fd, buf.as_ptr() as *const _, len, off as i64) } != len as isize {
                    errs += 1;
                }
                if mode == 3 {
                    usleep(rng.below(3000));
                }
            }
            _ => {
                let p = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0) };
                if p == libc::MAP_FAILED {
                    errs += 1;
                } else {
                    unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), (p as *mut u8).add(off), len) };
                    usleep(rng.below(1000));
                    unsafe { libc::munmap(p, size) };
                }
            }
        }
        close_(fd);
    }
    println!("writer ops={} errs={}", ops, errs);
}
