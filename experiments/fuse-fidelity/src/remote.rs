//! Remote-change simulator.
//!
//! Stands in for the sync engine applying a change that happened on another host. Every metadata
//! change is performed *through the FUSE mount itself* (so it is a real VFS operation that fires
//! inotify for local watchers), after invalidating the kernel's cached view with
//! `notify_inval_inode` / `notify_inval_entry`. It runs on its own thread: the daemon's request
//! threads never block on the kernel while serving these requests, so there is no self-deadlock.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Instant, SystemTime};

use fuser::{INodeNo, Notifier};

use crate::Opts;

fn log(msg: &str) {
    eprintln!("[remote-sim {:?}] {msg}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis());
}

fn ino(p: &Path) -> std::io::Result<INodeNo> {
    Ok(INodeNo(fs::metadata(p)?.ino()))
}

fn set_times(f: &File, secs: i64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let t = libc::timespec { tv_sec: secs, tv_nsec: 0 };
    let r = unsafe { libc::futimens(f.as_raw_fd(), [t, t].as_ptr()) };
    if r < 0 { Err(std::io::Error::last_os_error()) } else { Ok(()) }
}

pub fn run(o: &Opts, rel: &str, n: Notifier, stop: &AtomicBool) {
    let dir = o.mount.join(rel);
    let mut cycle = 0u32;
    sleep(o.sim_delay);
    while !stop.load(Ordering::SeqCst) && o.sim_cycles.map_or(true, |c| cycle < c) {
        cycle += 1;
        let t0 = Instant::now();
        match one_cycle(&dir, &n, cycle) {
            Ok(()) => log(&format!("cycle {cycle} ok in {:?}", t0.elapsed())),
            Err(e) => log(&format!("cycle {cycle} FAILED: {e}")),
        }
        sleep(o.sim_interval);
    }
}

fn one_cycle(dir: &Path, n: &Notifier, cycle: u32) -> std::io::Result<()> {
    // Directory appears (remote mkdir).
    if !dir.exists() {
        fs::create_dir_all(dir)?;
    }
    let (f1, f2) = (dir.join("f1"), dir.join("f2"));

    // Remote create: a new file shows up.
    let mut h = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&f1)?;
    h.write_all(&[b'a'; 4096])?;

    // Remote content change: invalidate kernel's data+attr cache for the inode, then ftruncate through the mount.
    // `h` stays open across the change, to prove an open (possibly passthrough) handle sees it.
    n.inval_inode(ino(&f1)?, 0, 0)?;
    h.set_len(8192)?;
    let sz = fs::metadata(&f1)?.len();
    if sz != 8192 {
        return Err(std::io::Error::other(format!("stat size {sz} after remote ftruncate, want 8192")));
    }
    h.seek(SeekFrom::Start(4096))?;
    let mut tail = [1u8; 4096];
    h.read_exact(&mut tail)?;
    if tail.iter().any(|&b| b != 0) {
        return Err(std::io::Error::other("extended region not zero through open handle"));
    }

    // Remote mtime change (restoring the remote mtime): IN_ATTRIB.
    n.inval_inode(ino(&f1)?, -1, 0)?; // attributes only
    set_times(&h, 1_700_000_000 + cycle as i64)?;
    if fs::metadata(&f1)?.mtime() != 1_700_000_000 + cycle as i64 {
        return Err(std::io::Error::other("mtime not applied"));
    }
    drop(h);

    // Remote rename: invalidate both names, then rename.
    let dino = ino(dir)?;
    n.inval_entry(dino, OsStr::new("f1"))?;
    n.inval_entry(dino, OsStr::new("f2")).ok();
    fs::rename(&f1, &f2)?;

    // Remote delete.
    n.inval_entry(dino, OsStr::new("f2"))?;
    fs::remove_file(&f2)?;
    Ok(())
}
