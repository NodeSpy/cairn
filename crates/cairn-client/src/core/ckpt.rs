//! Checkpoints of files that are open for write (docs/CLIENT.md "Checkpoints of open files").
//!
//! A file held open for write (a database, a log) never reaches the last-close save, so every
//! `checkpoint_secs` (or `checkpoint_bytes` of growth) whose stat changed it is reflink-snapshotted,
//! chunked and its missing chunks are uploaded (pre-upload: nothing is published, nothing refers
//! to them yet, the server keeps unreferenced chunks for its grace period). Between consecutive
//! snapshots only the byte ranges whose extents changed (`FIEMAP`: a reflinked, untouched range
//! keeps its physical address, a COW-rewritten one gets a new one) are rehashed, plus the CDC
//! resync margin. A file open for write longer than `checkpoint_publish_secs` also publishes the
//! checkpoint as a version (crash-consistent: the snapshot is a point-in-time view).

use super::*;
use cairn_proto::SMALL_FILE_MAX;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::AtomicU64;

/// Files this big and bigger use snapshot + incremental hashing for saves with writers open.
pub const INCR_MIN: u64 = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub logical: u64,
    pub physical: u64,
    pub len: u64,
    pub flags: u32,
}

const FS_IOC_FIEMAP: libc::c_ulong = 0xC020_660B;
const FIEMAP_EXTENT_LAST: u32 = 0x1;
const FIEMAP_EXTENT_UNKNOWN: u32 = 0x2;
const FIEMAP_EXTENT_DELALLOC: u32 = 0x4;
const FIEMAP_EXTENT_DATA_INLINE: u32 = 0x200;

#[repr(C)]
#[derive(Clone, Copy)]
struct FmExtent {
    logical: u64,
    physical: u64,
    length: u64,
    r0: u64,
    r1: u64,
    flags: u32,
    r2: [u32; 3],
}
const FM_N: usize = 256;
#[repr(C)]
struct FmBuf {
    start: u64,
    length: u64,
    flags: u32,
    mapped: u32,
    count: u32,
    reserved: u32,
    ext: [FmExtent; FM_N],
}

/// All extents of `f`, in logical order.
pub fn fiemap(f: &fs::File) -> std::io::Result<Vec<Extent>> {
    let mut out = vec![];
    let mut buf: Box<FmBuf> = unsafe { Box::new(std::mem::zeroed()) };
    let mut start = 0u64;
    loop {
        buf.start = start;
        buf.length = u64::MAX - start;
        buf.flags = 0;
        buf.mapped = 0;
        buf.count = FM_N as u32;
        let r = unsafe { libc::ioctl(f.as_raw_fd(), FS_IOC_FIEMAP, &mut *buf as *mut FmBuf) };
        if r != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let n = buf.mapped as usize;
        if n == 0 {
            break;
        }
        let mut last = false;
        for e in &buf.ext[..n.min(FM_N)] {
            out.push(Extent { logical: e.logical, physical: e.physical, len: e.length, flags: e.flags });
            if e.flags & FIEMAP_EXTENT_LAST != 0 {
                last = true;
            }
            start = e.logical + e.length;
        }
        if last {
            break;
        }
    }
    Ok(out)
}

fn lookup(ext: &[Extent], at: u64) -> Option<&Extent> {
    let i = ext.partition_point(|e| e.logical + e.len <= at);
    ext.get(i).filter(|e| e.logical <= at)
}

/// Byte ranges `[a, b)` whose backing differs between two snapshots of one file (sorted, merged).
/// A range is unchanged only if both snapshots map it to the same physical bytes (same
/// `physical - logical` delta) or both leave it a hole; inline, delayed-allocation and unknown
/// extents are never trusted.
pub fn changed_ranges(old: &[Extent], new: &[Extent], old_size: u64, new_size: u64) -> Vec<(u64, u64)> {
    let end = old_size.max(new_size);
    let common = old_size.min(new_size);
    let mut bps: Vec<u64> = vec![0, end, common];
    for e in old.iter().chain(new.iter()) {
        bps.push(e.logical.min(end));
        bps.push((e.logical + e.len).min(end));
    }
    bps.sort_unstable();
    bps.dedup();
    let untrusted = FIEMAP_EXTENT_UNKNOWN | FIEMAP_EXTENT_DELALLOC | FIEMAP_EXTENT_DATA_INLINE;
    let mut out: Vec<(u64, u64)> = vec![];
    for w in bps.windows(2) {
        let (a, b) = (w[0], w[1]);
        if a == b {
            continue;
        }
        let changed = if a >= common {
            true
        } else {
            match (lookup(old, a), lookup(new, a)) {
                (None, None) => false,
                (Some(o), Some(n)) => o.flags & untrusted != 0 || n.flags & untrusted != 0 || o.physical.wrapping_sub(o.logical) != n.physical.wrapping_sub(n.logical),
                _ => true,
            }
        };
        if changed {
            match out.last_mut() {
                Some(l) if l.1 == a => l.1 = b,
                _ => out.push((a, b)),
            }
        }
    }
    out
}

struct AtReader<'a> {
    f: &'a fs::File,
    off: u64,
    end: u64,
}
impl std::io::Read for AtReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        if self.off >= self.end {
            return Ok(0);
        }
        let n = buf.len().min((self.end - self.off) as usize);
        let r = self.f.read_at(&mut buf[..n], self.off)?;
        self.off += r as u64;
        Ok(r)
    }
}

pub type Chunks = Vec<(Hash, u64, u32)>;

/// Chunk list of `f` (`size` bytes) given the chunk list `old` of a previous snapshot and the
/// ranges that changed since: re-chunks from the start of the first affected old chunk and stops
/// as soon as a cut lands on an old boundary past the last change (FastCDC cuts depend only on
/// the bytes since the previous cut, so everything after is identical). Returns the new list and
/// the number of bytes that were hashed.
pub fn rechunk(f: &fs::File, size: u64, old: &[(Hash, u64, u32)], changed: &[(u64, u64)]) -> std::io::Result<(Chunks, u64)> {
    let old_total: u64 = old.last().map(|c| c.1 + c.2 as u64).unwrap_or(0);
    if changed.is_empty() && old_total == size {
        return Ok((old.to_vec(), 0));
    }
    let first = changed.first().map(|c| c.0).unwrap_or(old_total.min(size));
    let last_end = changed.last().map(|c| c.1).unwrap_or(0);
    let idx = if old.is_empty() {
        0
    } else if first >= old_total {
        old.len() - 1 // the last chunk ended at EOF, not at a content cut: appended bytes extend it
    } else {
        old.partition_point(|c| c.1 + c.2 as u64 <= first).min(old.len() - 1)
    };
    let s = old.get(idx).map(|c| c.1).unwrap_or(0);
    let mut out: Chunks = old[..idx.min(old.len())].to_vec();
    let bounds: std::collections::HashMap<u64, usize> = old.iter().enumerate().map(|(i, c)| (c.1 + c.2 as u64, i)).collect();
    let mut hashed = 0u64;
    let rd = AtReader { f, off: s, end: size };
    for c in fastcdc::v2020::StreamCDC::with_level(rd, CDC_MIN as usize, CDC_AVG as usize, CDC_MAX as usize, fastcdc::v2020::Normalization::Level1) {
        let c = c.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let off = s + c.offset;
        out.push((Hash::of(&c.data), off, c.length as u32));
        hashed += c.length as u64;
        let e = off + c.length as u64;
        if e >= last_end && e < size && size == old_total {
            if let Some(&k) = bounds.get(&e) {
                out.extend_from_slice(&old[k + 1..]);
                break;
            }
        }
    }
    Ok((out, hashed))
}

/// The diff base of a file: its previous snapshot (a reflink kept in `tmp/`) and what we know of it.
pub struct CkptBase {
    pub snap: PathBuf,
    pub size: u64,
    pub extents: Vec<Extent>,
    pub chunks: Chunks,
}

pub static CKPT: CkptStats = CkptStats {
    snapshots: AtomicU64::new(0),
    rehashed_bytes: AtomicU64::new(0),
    total_bytes: AtomicU64::new(0),
    preuploaded_chunks: AtomicU64::new(0),
    published: AtomicU64::new(0),
};

/// Process-wide checkpoint counters (the FUSE stats dump prints them).
pub fn ckpt_counters() -> Vec<(&'static str, u64)> {
    let s = &CKPT;
    vec![
        ("snapshots", s.snapshots.load(Ordering::Relaxed)),
        ("rehashed_bytes", s.rehashed_bytes.load(Ordering::Relaxed)),
        ("total_bytes", s.total_bytes.load(Ordering::Relaxed)),
        ("preuploaded_chunks", s.preuploaded_chunks.load(Ordering::Relaxed)),
        ("published", s.published.load(Ordering::Relaxed)),
    ]
}

pub struct CkptStats {
    pub snapshots: AtomicU64,
    pub rehashed_bytes: AtomicU64,
    pub total_bytes: AtomicU64,
    pub preuploaded_chunks: AtomicU64,
    pub published: AtomicU64,
}

impl RootCore {
    /// (snapshots taken, bytes rehashed, bytes covered, chunks pre-uploaded, checkpoints published)
    pub fn ckpt_stats(&self) -> (u64, u64, u64, u64, u64) {
        let s = &CKPT;
        (s.snapshots.load(Ordering::Relaxed), s.rehashed_bytes.load(Ordering::Relaxed), s.total_bytes.load(Ordering::Relaxed), s.preuploaded_chunks.load(Ordering::Relaxed), s.published.load(Ordering::Relaxed))
    }

    pub(super) fn drop_ckpt(&self, node: NodeId) {
        if let Some(b) = self.ckpts.lock().remove(&node) {
            let _ = fs::remove_file(&b.snap);
        }
    }

    /// Reflink-snapshot the live file, hash it incrementally against the previous snapshot, make
    /// sure its CAS object exists and (`preupload`) upload the chunks that are new. The snapshot
    /// becomes the base for the next call.
    pub(super) fn snapshot_prepared(&self, node: NodeId, live: &std::path::Path, preupload: bool) -> FsResult<Prepared> {
        let s0 = fs::metadata(live).map(|m| stat_of(&m)).map_err(|_| Errno::EIO)?;
        let snap = self.sh.cache.tmp_path("ckpt");
        self.sh.cache.clone_file(live, &snap).map_err(|e| {
            tracing::warn!("checkpoint snapshot failed: {e}");
            Errno::EIO
        })?;
        let f = fs::File::open(&snap).map_err(|_| Errno::EIO)?;
        let size = f.metadata().map_err(|_| Errno::EIO)?.len();
        let base = self.ckpts.lock().remove(&node);
        let stats = &CKPT;
        stats.snapshots.fetch_add(1, Ordering::Relaxed);
        stats.total_bytes.fetch_add(size, Ordering::Relaxed);
        let extents = if size > SMALL_FILE_MAX { fiemap(&f).unwrap_or_default() } else { vec![] };
        let (hashed, chunks, rehashed): (crate::cas::Hashed, Chunks, u64) = if size <= SMALL_FILE_MAX {
            let h = crate::cas::Cache::hash_file(&snap).map_err(|_| Errno::EIO)?;
            (h, vec![], size)
        } else {
            let incremental = base.as_ref().filter(|b| b.size > SMALL_FILE_MAX && !b.extents.is_empty() && !extents.is_empty());
            let (chunks, rehashed) = match incremental {
                Some(b) => {
                    let changed = changed_ranges(&b.extents, &extents, b.size, size);
                    rechunk(&f, size, &b.chunks, &changed).map_err(|_| Errno::EIO)?
                }
                None => rechunk(&f, size, &[], &[(0, size)]).map_err(|_| Errno::EIO)?,
            };
            let m = Manifest { size, chunks: chunks.iter().map(|c| (c.0, c.2)).collect() };
            (crate::cas::Hashed { content: m.id(), size, manifest: Some(m) }, chunks, rehashed)
        };
        stats.rehashed_bytes.fetch_add(rehashed, Ordering::Relaxed);
        tracing::debug!("{node:?}: snapshot of {size} bytes, rehashed {rehashed}");
        // the CAS object is a second reflink of the snapshot (extents shared, nothing written twice)
        if let Some(oh) = object_hash(&hashed.content) {
            if !self.sh.cache.has(&oh) {
                let tmp = self.sh.cache.tmp_path("cas");
                self.sh.cache.clone_file(&snap, &tmp).map_err(|_| Errno::EIO)?;
                self.sh.cache.adopt(&tmp, &oh).map_err(|_| Errno::EIO)?;
            }
        }
        if preupload && !chunks.is_empty() && !self.sh.cfg.offline && !self.sh.cfg.server.is_empty() && self.sh.link.is_connected() {
            let known: HashSet<Hash> = base.as_ref().map(|b| b.chunks.iter().map(|c| c.0).collect()).unwrap_or_default();
            let file = Arc::new(fs::File::open(&snap).map_err(|_| Errno::EIO)?);
            let pieces: Vec<_> = chunks.iter().filter(|c| !known.contains(&c.0)).map(|c| (c.0, file.clone(), c.1, c.2)).collect();
            let n = pieces.len() as u64;
            let sh = self.sh.clone();
            if self.block(async move { crate::conn::upload_pieces(&sh, pieces).await }).is_ok() {
                stats.preuploaded_chunks.fetch_add(n, Ordering::Relaxed);
            }
        }
        if let Some(old) = base {
            let _ = fs::remove_file(&old.snap);
        }
        self.ckpts.lock().insert(node, CkptBase { snap, size, extents, chunks });
        Ok(Prepared { h: hashed, mtime: s0.mtime_ns, stat: None })
    }

    /// Periodic scan: checkpoint (and, for long-open files, publish) files open for write.
    pub(super) fn checkpoint_scan(&self) {
        if !self.sh.cache.reflinks() {
            return;
        }
        let now = Instant::now();
        let every = Duration::from_secs(self.sh.cfg.checkpoint_secs);
        let pub_after = Duration::from_secs(self.sh.cfg.checkpoint_publish_secs);
        let cands: Vec<(NodeId, u64, Option<(Instant, Stat)>, Option<Instant>, Instant, Option<Instant>)> = {
            let st = self.st.lock();
            st.files.iter().filter(|(_, f)| f.writers > 0 && f.live && !f.queued).map(|(n, f)| (*n, f.ino, f.ckpt, f.write_since, f.write_since.unwrap_or(now), f.last_pub)).map(|(n, i, c, ws, since, lp)| (n, i, c, ws, since, lp)).collect()
        };
        for (node, ino, ckpt, _ws, since, last_pub) in cands {
            let live = self.sh.cache.live_path(&self.name, ino);
            let Ok(m) = fs::metadata(&live) else { continue };
            let s = stat_of(&m);
            let (last_t, last_size) = match ckpt {
                Some((t, st)) => {
                    if st == s {
                        // nothing written since the last checkpoint; still owed a publish if it
                        // has been open long enough and the checkpoint was never published
                        let unpub = last_pub.map_or(true, |lp| lp < t);
                        let owed = now.duration_since(last_pub.unwrap_or(since)) >= pub_after && now.duration_since(since) >= pub_after;
                        if !(unpub && owed) {
                            continue;
                        }
                    }
                    (t, st.size)
                }
                None => (since, 0),
            };
            let republish = ckpt.is_some_and(|(_, st)| st == s);
            let due = republish || now.duration_since(last_t) >= every || s.size.saturating_sub(last_size) >= self.sh.cfg.checkpoint_bytes;
            if !due {
                continue;
            }
            let publish = now.duration_since(last_pub.unwrap_or(since)) >= pub_after && now.duration_since(since) >= pub_after;
            if s.size <= SMALL_FILE_MAX && !publish {
                // nothing worth pre-uploading: remember the stat only
                if let Some(f) = self.st.lock().files.get_mut(&node) {
                    f.ckpt = Some((now, s));
                }
                continue;
            }
            match self.snapshot_prepared(node, &live, true) {
                Ok(prep) => {
                    {
                        let mut st = self.st.lock();
                        if let Some(f) = st.files.get_mut(&node) {
                            f.ckpt = Some((now, s));
                        }
                    }
                    if publish {
                        let cred = self.st.lock().files.get(&node).and_then(|f| f.last_cred).unwrap_or((0, 0));
                        if !self.st.lock().is_local(node) {
                            self.acquire_lease(node);
                        }
                        let lock = self.node_lock(node);
                        let _g = lock.lock();
                        match self.publish_phase(node, Cred { uid: cred.0, gid: cred.1 }, false, prep, 0, false) {
                            Ok(_) => {
                                CKPT.published.fetch_add(1, Ordering::Relaxed);
                                if let Some(f) = self.st.lock().files.get_mut(&node) {
                                    f.last_pub = Some(now);
                                }
                            }
                            Err(e) => tracing::warn!("{node:?}: checkpoint publish failed: {e:?}"),
                        }
                    }
                }
                Err(e) => tracing::debug!("{node:?}: checkpoint failed: {e:?}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, RngCore, SeedableRng};
    use std::os::unix::fs::FileExt;

    fn ext(l: u64, p: u64, n: u64) -> Extent {
        Extent { logical: l, physical: p, len: n, flags: 0 }
    }

    #[test]
    fn changed_ranges_from_extent_maps() {
        // same mapping -> nothing changed
        let a = vec![ext(0, 1000, 4096), ext(8192, 9000, 4096)];
        assert!(changed_ranges(&a, &a, 12288, 12288).is_empty());
        // one extent rewritten (new physical), the other untouched; the hole in between is a hole
        let b = vec![ext(0, 5000, 4096), ext(8192, 9000, 4096)];
        assert_eq!(changed_ranges(&a, &b, 12288, 12288), vec![(0, 4096)]);
        // a partial overwrite splits an extent: only the rewritten middle differs
        let c = vec![ext(0, 1000, 1024), ext(1024, 7000, 1024), ext(2048, 1000 + 2048, 2048), ext(8192, 9000, 4096)];
        assert_eq!(changed_ranges(&a, &c, 12288, 12288), vec![(1024, 2048)]);
        // growth: the new tail is changed
        let d = vec![ext(0, 1000, 4096), ext(8192, 9000, 4096), ext(12288, 20000, 4096)];
        assert_eq!(changed_ranges(&a, &d, 12288, 16384), vec![(12288, 16384)]);
        // shrink
        assert_eq!(changed_ranges(&d, &a, 16384, 12288), vec![(12288, 16384)]);
        // a hole that became data, and data that became a hole
        let e = vec![ext(0, 1000, 4096), ext(4096, 3000, 4096), ext(8192, 9000, 4096)];
        assert_eq!(changed_ranges(&a, &e, 12288, 12288), vec![(4096, 8192)]);
        assert_eq!(changed_ranges(&e, &a, 12288, 12288), vec![(4096, 8192)]);
        // inline / delalloc extents are never trusted
        let mut f = a.clone();
        f[0].flags = FIEMAP_EXTENT_DELALLOC;
        assert_eq!(changed_ranges(&f, &a, 12288, 12288), vec![(0, 4096)]);
    }

    fn full(f: &fs::File, size: u64) -> Chunks {
        rechunk(f, size, &[], &[(0, size)]).unwrap().0
    }

    #[test]
    fn rechunk_equals_full_chunking() {
        let d = tempfile::tempdir().unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for round in 0..12 {
            let p = d.path().join(format!("f{round}"));
            let size: usize = rng.random_range(300_000..3_000_000);
            let mut data = vec![0u8; size];
            rng.fill_bytes(&mut data);
            fs::write(&p, &data).unwrap();
            let f = fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
            let old = full(&f, size as u64);
            // the chunking itself must agree with the manifest hashing used everywhere else
            let h = crate::cas::Cache::hash_file(&p).unwrap();
            assert_eq!(h.manifest.unwrap().chunks, old.iter().map(|c| (c.0, c.2)).collect::<Vec<_>>());
            // random in-place modifications
            let mut changed = vec![];
            for _ in 0..rng.random_range(1..4) {
                let off = rng.random_range(0..size - 1);
                let len = rng.random_range(1..(size - off).min(50_000));
                let mut patch = vec![0u8; len];
                rng.fill_bytes(&mut patch);
                f.write_all_at(&patch, off as u64).unwrap();
                changed.push((off as u64, (off + len) as u64));
            }
            changed.sort();
            let (new, hashed) = rechunk(&f, size as u64, &old, &changed).unwrap();
            assert_eq!(new, full(&f, size as u64), "round {round}");
            assert!(hashed <= size as u64);
            if round > 0 && size > 2_000_000 && changed.len() == 1 {
                assert!(hashed < size as u64 / 2, "incremental rehash should skip most of the file ({hashed} of {size})");
            }
            // append, then truncate
            let tail = vec![9u8; 100_000];
            f.write_all_at(&tail, size as u64).unwrap();
            let n2 = size as u64 + 100_000;
            let old2 = new;
            let (new2, _) = rechunk(&f, n2, &old2, &[(size as u64, n2)]).unwrap();
            assert_eq!(new2, full(&f, n2));
            f.set_len(size as u64 / 2).unwrap();
            let (new3, _) = rechunk(&f, size as u64 / 2, &new2, &[(size as u64 / 2, n2)]).unwrap();
            assert_eq!(new3, full(&f, size as u64 / 2));
        }
    }
}
