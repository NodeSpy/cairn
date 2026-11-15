//! The op log: the client's write-ahead log (docs/CLIENT.md "Op log").
//!
//! Every state mutation (`DbOp`) is appended here as a checksummed record before the FUSE reply
//! (a plain `write(2)`, so it survives a daemon crash), and folded into SQLite later by the
//! checkpointer. The log is fsynced by group commit (fsync/syncfs, a short timer, before the
//! sender ships ops), so after power loss the guarantee is the one ext4/btrfs give: fsynced
//! ops survive, otherwise a consistent prefix.
//!
//! File layout: `"CAIRNLG1" | base_lsn u64 LE` then records `len u32 LE | tag u64 LE | payload`,
//! where `tag` is the first 8 bytes of BLAKE3(payload). An LSN is `base + (file offset - 16)`; it
//! identifies a record boundary, and SQLite stores the LSN up to which it has absorbed the log.
//! Recovery stops at the first record that is short or fails its checksum (torn tail) and
//! truncates the file there.

use parking_lot::{Condvar, Mutex};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MAGIC: &[u8; 8] = b"CAIRNLG1";
const HDR: u64 = 16;
/// Records bigger than this are treated as corruption when scanning.
const MAX_REC: usize = 64 << 20;

struct LogState {
    file: Arc<File>,
    base: u64,
    /// bytes in the file, including the header
    file_len: u64,
    /// appended but not yet written to the file
    buf: Vec<u8>,
}

impl LogState {
    fn end_lsn(&self) -> u64 {
        self.base + (self.file_len - HDR) + self.buf.len() as u64
    }
    fn written_lsn(&self) -> u64 {
        self.base + (self.file_len - HDR)
    }
    fn write_out(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        if let Err(e) = self.file.write_all_at(&self.buf, self.file_len) {
            tracing::error!("op log write failed: {e}");
            return;
        }
        self.file_len += self.buf.len() as u64;
        self.buf.clear();
    }
}

struct SyncState {
    synced: u64,
    busy: bool,
}

pub struct OpLog {
    path: PathBuf,
    st: Mutex<LogState>,
    sync: Mutex<SyncState>,
    sync_cv: Condvar,
    pub stop: AtomicBool,
}

fn tag(payload: &[u8]) -> u64 {
    let h = blake3::hash(payload);
    u64::from_le_bytes(h.as_bytes()[..8].try_into().unwrap())
}

fn new_file(path: &Path, base: u64) -> io::Result<File> {
    let tmp = path.with_extension("new");
    let mut f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&tmp)?;
    let mut h = Vec::with_capacity(HDR as usize);
    h.extend_from_slice(MAGIC);
    h.extend_from_slice(&base.to_le_bytes());
    f.write_all(&h)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(d) = path.parent() {
        File::open(d)?.sync_all()?;
    }
    Ok(f)
}

/// Records of `data` (a whole log file) in order: `(start_lsn, end_lsn, payload range)`, up to the
/// first bad record. Returns also the offset where the valid prefix ends.
fn scan(data: &[u8]) -> Option<(u64, Vec<(u64, u64, std::ops::Range<usize>)>, usize)> {
    if data.len() < HDR as usize || &data[..8] != MAGIC {
        return None;
    }
    let base = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let mut recs = vec![];
    let mut off = HDR as usize;
    while off + 12 <= data.len() {
        let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        let t = u64::from_le_bytes(data[off + 4..off + 12].try_into().unwrap());
        if len > MAX_REC || off + 12 + len > data.len() {
            break;
        }
        let p = &data[off + 12..off + 12 + len];
        if tag(p) != t {
            break;
        }
        let start = base + (off as u64 - HDR);
        off += 12 + len;
        recs.push((start, base + (off as u64 - HDR), off - len..off));
    }
    Some((base, recs, off))
}

impl OpLog {
    /// Open (creating) the log. `ckpt` is the LSN SQLite has absorbed; returns the records at or
    /// past it (`(end_lsn, payload)`), which the caller replays into SQLite.
    pub fn open(path: &Path, ckpt: u64) -> io::Result<(Arc<OpLog>, Vec<(u64, Vec<u8>)>)> {
        let data = std::fs::read(path).unwrap_or_default();
        let mut replay = vec![];
        let (file, base, file_len) = match scan(&data) {
            Some((base, recs, valid_end)) if base + (valid_end as u64 - HDR) >= ckpt => {
                for (start, end, r) in recs {
                    if start >= ckpt {
                        replay.push((end, data[r].to_vec()));
                    }
                }
                let f = OpenOptions::new().read(true).write(true).open(path)?;
                if valid_end < data.len() {
                    tracing::warn!("op log: discarding {} torn tail bytes", data.len() - valid_end);
                    f.set_len(valid_end as u64)?;
                    f.sync_all()?;
                }
                (f, base, valid_end as u64)
            }
            // missing/corrupt header, or SQLite already ahead of everything in the log
            _ => (new_file(path, ckpt)?, ckpt, HDR),
        };
        let end = base + (file_len - HDR);
        let log = Arc::new(OpLog {
            path: path.to_path_buf(),
            st: Mutex::new(LogState { file: Arc::new(file), base, file_len, buf: Vec::new() }),
            sync: Mutex::new(SyncState { synced: end, busy: false }),
            sync_cv: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        Ok((log, replay))
    }

    /// Append a record; returns the LSN just past it. Not yet written to the file.
    pub fn append(&self, payload: &[u8]) -> u64 {
        let mut st = self.st.lock();
        st.buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        st.buf.extend_from_slice(&tag(payload).to_le_bytes());
        st.buf.extend_from_slice(payload);
        st.end_lsn()
    }

    /// `write(2)` everything appended so far (crash-safe against daemon death, not power loss).
    pub fn write_out(&self) {
        let mut st = self.st.lock();
        st.write_out();
    }

    pub fn end_lsn(&self) -> u64 {
        self.st.lock().end_lsn()
    }

    /// Size of the file in bytes.
    pub fn file_bytes(&self) -> u64 {
        self.st.lock().file_len
    }

    /// Everything appended so far is on disk when this returns. Concurrent callers share fsyncs.
    pub fn sync(&self) {
        let target = {
            let mut st = self.st.lock();
            st.write_out();
            st.end_lsn()
        };
        self.sync_to(target);
    }

    /// Make the log durable up to `target` (an LSN returned by `append`; written out first).
    pub fn sync_to(&self, target: u64) {
        let mut s = self.sync.lock();
        loop {
            if s.synced >= target {
                return;
            }
            if s.busy {
                self.sync_cv.wait(&mut s);
                continue;
            }
            s.busy = true;
            drop(s);
            let (file, covered) = {
                let mut st = self.st.lock();
                st.write_out();
                (st.file.clone(), st.written_lsn())
            };
            let r = file.sync_data();
            if let Err(e) = r {
                tracing::error!("op log fsync failed: {e}");
            }
            s = self.sync.lock();
            s.synced = s.synced.max(covered);
            s.busy = false;
            self.sync_cv.notify_all();
        }
    }

    pub fn synced_lsn(&self) -> u64 {
        self.sync.lock().synced
    }

    /// Start a fresh empty log at `ckpt` if SQLite has absorbed everything and made that durable.
    pub fn try_rotate(&self, ckpt: u64) -> bool {
        let mut st = self.st.lock();
        if st.end_lsn() != ckpt {
            return false;
        }
        match new_file(&self.path, ckpt) {
            Ok(f) => {
                st.file = Arc::new(f);
                st.base = ckpt;
                st.file_len = HDR;
                let mut s = self.sync.lock();
                s.synced = s.synced.max(ckpt);
                true
            }
            Err(e) => {
                tracing::warn!("op log rotate failed: {e}");
                false
            }
        }
    }

    /// Background thread: write out every ~2 ms, fsync every `sync_every`.
    pub fn spawn_timer(self: &Arc<Self>, sync_every: Duration) {
        let me = self.clone();
        let _ = std::thread::Builder::new().name("cairn-oplog".into()).spawn(move || {
            let mut since_sync = Duration::ZERO;
            let tick = Duration::from_millis(2);
            while !me.stop.load(Ordering::SeqCst) {
                std::thread::sleep(tick);
                since_sync += tick;
                me.write_out();
                if since_sync >= sync_every {
                    since_sync = Duration::ZERO;
                    if me.synced_lsn() < me.end_lsn() {
                        me.sync();
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_replay_torn_tail_and_rotate() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("oplog");
        let (log, rep) = OpLog::open(&p, 0).unwrap();
        assert!(rep.is_empty());
        let mut ends = vec![];
        for i in 0..10u8 {
            ends.push(log.append(&vec![i; 10 + i as usize]));
        }
        log.sync();
        drop(log);
        // replay everything, then only the tail past a checkpoint
        let (log, rep) = OpLog::open(&p, 0).unwrap();
        assert_eq!(rep.len(), 10);
        assert_eq!(rep[3].1, vec![3u8; 13]);
        drop(log);
        let (log, rep) = OpLog::open(&p, ends[6]).unwrap();
        assert_eq!(rep.len(), 3);
        drop(log);
        // torn tail: garbage and a half record are dropped, appends continue after the valid prefix
        let mut f = OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(&[200, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(f);
        let (log, rep) = OpLog::open(&p, 0).unwrap();
        assert_eq!(rep.len(), 10);
        let e = log.append(b"after");
        log.write_out();
        assert_eq!(e, ends[9] + 12 + 5);
        drop(log);
        // flipped payload byte in the last record: that record and nothing before it is lost
        let mut data = std::fs::read(&p).unwrap();
        let n = data.len();
        data[n - 1] ^= 0xff;
        std::fs::write(&p, &data).unwrap();
        let (log, rep) = OpLog::open(&p, 0).unwrap();
        assert_eq!(rep.len(), 10);
        // rotation only once everything is absorbed
        let end = log.append(b"x");
        log.write_out();
        assert!(!log.try_rotate(end - 1));
        assert!(log.try_rotate(end));
        assert_eq!(log.file_bytes(), HDR);
        let e2 = log.append(b"y");
        assert_eq!(e2, end + 12 + 1);
        log.sync();
        drop(log);
        let (_log, rep) = OpLog::open(&p, end).unwrap();
        assert_eq!(rep, vec![(e2, b"y".to_vec())]);
        // SQLite ahead of the whole log: start over at its LSN
        let (log, rep) = OpLog::open(&p, e2 + 1000).unwrap();
        assert!(rep.is_empty());
        assert_eq!(log.end_lsn(), e2 + 1000);
    }
}
