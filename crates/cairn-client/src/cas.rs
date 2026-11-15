//! The local content cache: whole-file CAS objects named by content hash,
//! live files (reflinks of CAS objects), tmp area, chunking, eviction.
//! See docs/CLIENT.md "Local layout" and "Content lifecycle".

use cairn_proto::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const FICLONE: libc::c_ulong = 0x4004_9409;
/// Largest live file ingested through memory (see `Cache::ingest_live`).
const INGEST_BUF_MAX: u64 = 8 << 20;

pub struct Cache {
    dir: PathBuf,
    reflink: AtomicBool,
    tmp_ctr: AtomicU64,
    /// Objects touched recently (LRU stamps are by mtime; one utimensat per open is too dear).
    touched: parking_lot::Mutex<std::collections::HashMap<Hash, std::time::Instant>>,
}

/// Result of hashing a file.
#[derive(Clone, Debug)]
pub struct Hashed {
    pub content: ContentId,
    pub size: u64,
    /// Present for chunked content.
    pub manifest: Option<Manifest>,
}

/// The CAS object name for a content id (`None` for the empty file).
pub fn object_hash(c: &ContentId) -> Option<Hash> {
    match c {
        ContentId::Empty => None,
        ContentId::Blob(h) | ContentId::Chunked(h) => Some(*h),
    }
}

impl Cache {
    pub fn open(dir: &Path) -> io::Result<Cache> {
        for d in ["cas", "live", "tmp"] {
            fs::create_dir_all(dir.join(d))?;
        }
        // leftovers from a crash
        if let Ok(rd) = fs::read_dir(dir.join("tmp")) {
            for e in rd.flatten() {
                let _ = fs::remove_file(e.path());
            }
        }
        let c = Cache { dir: dir.to_path_buf(), reflink: AtomicBool::new(true), tmp_ctr: AtomicU64::new(0), touched: Default::default() };
        Ok(c)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
    pub fn cas_path(&self, h: &Hash) -> PathBuf {
        let hex = h.to_hex();
        self.dir.join("cas").join(&hex[..2]).join(hex)
    }
    pub fn has(&self, h: &Hash) -> bool {
        self.cas_path(h).exists()
    }
    pub fn tmp_path(&self, tag: &str) -> PathBuf {
        let n = self.tmp_ctr.fetch_add(1, Ordering::Relaxed);
        self.dir.join("tmp").join(format!("{tag}-{}-{n}", std::process::id()))
    }
    pub fn live_dir(&self, root: &str) -> PathBuf {
        self.dir.join("live").join(root.replace('/', "%2f"))
    }
    pub fn live_path(&self, root: &str, ino: u64) -> PathBuf {
        self.live_dir(root).join(ino.to_string())
    }
    pub fn open_cas(&self, h: &Hash) -> io::Result<File> {
        let f = File::open(self.cas_path(h))?;
        self.touch_lazy(h);
        Ok(f)
    }
    /// `touch`, at most once a minute per object.
    pub fn touch_lazy(&self, h: &Hash) {
        let now = std::time::Instant::now();
        {
            let mut m = self.touched.lock();
            if m.get(h).is_some_and(|t| now.duration_since(*t).as_secs() < 60) {
                return;
            }
            if m.len() > 8192 {
                m.clear();
            }
            m.insert(*h, now);
        }
        self.touch(h);
    }
    /// Mark as recently used (LRU is by mtime; CAS objects are immutable so mtime is free).
    pub fn touch(&self, h: &Hash) {
        let p = self.cas_path(h);
        if let Ok(c) = std::ffi::CString::new(p.as_os_str().as_encoded_bytes()) {
            unsafe {
                libc::utimensat(libc::AT_FDCWD, c.as_ptr(), std::ptr::null(), 0);
            }
        }
    }

    /// Reflink `src` to `dst` (new file, mode 0600); falls back to a copy.
    pub fn clone_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        if let Some(p) = dst.parent() {
            fs::create_dir_all(p)?;
        }
        let mut s = File::open(src)?;
        let mut d = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(dst)?;
        self.clone_into(&mut s, &mut d)
    }

    /// Like `clone_file`, but returns the new file open read-write, and costs no syscalls beyond
    /// open(src), open(dst), the clone and a close (the directory is only created if missing).
    pub fn clone_file_fd(&self, src: &Path, dst: &Path) -> io::Result<File> {
        let mut s = File::open(src)?;
        let mut oo = fs::OpenOptions::new();
        oo.read(true).write(true).create(true).truncate(true).mode(0o600);
        let mut d = match oo.open(dst) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(dst.parent().unwrap())?;
                oo.open(dst)?
            }
            Err(e) => return Err(e),
        };
        self.clone_into(&mut s, &mut d)?;
        Ok(d)
    }

    /// Replace the content of the open file `d` with that of `s`.
    pub fn clone_into(&self, s: &mut File, d: &mut File) -> io::Result<()> {
        if self.reflink.load(Ordering::Relaxed) {
            let r = unsafe { libc::ioctl(d.as_raw_fd(), FICLONE, s.as_raw_fd()) };
            if r == 0 {
                return Ok(());
            }
            let e = io::Error::last_os_error();
            match e.raw_os_error() {
                Some(libc::EOPNOTSUPP) | Some(libc::EXDEV) | Some(libc::EINVAL) | Some(libc::ENOTTY) | Some(libc::ENOSYS) => {
                    self.reflink.store(false, Ordering::Relaxed);
                }
                _ => return Err(e),
            }
        }
        d.set_len(0)?;
        s.seek(SeekFrom::Start(0))?;
        io::copy(s, d)?;
        // callers hand `d` out as a fresh descriptor: FICLONE leaves the offset alone, so must we
        d.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn reflinks(&self) -> bool {
        self.reflink.load(Ordering::Relaxed)
    }

    /// Hash a file: blob for <= 64 KiB, FastCDC manifest above.
    pub fn hash_file(path: &Path) -> io::Result<Hashed> {
        let f = File::open(path)?;
        let size = f.metadata()?.len();
        hash_reader(f, size)
    }

    /// Hash `snapshot` (a file in tmp/) and move it into the CAS (or delete it
    /// if the object already exists). Returns the content id.
    pub fn ingest(&self, snapshot: &Path) -> io::Result<Hashed> {
        let h = Self::hash_file(snapshot)?;
        match object_hash(&h.content) {
            None => {
                let _ = fs::remove_file(snapshot);
            }
            Some(hash) => {
                let dst = self.cas_path(&hash);
                if dst.exists() {
                    let _ = fs::remove_file(snapshot);
                    self.touch(&hash);
                } else {
                    fs::create_dir_all(dst.parent().unwrap())?;
                    fs::set_permissions(snapshot, fs::Permissions::from_mode(0o444))?;
                    fs::rename(snapshot, &dst)?;
                }
            }
        }
        Ok(h)
    }

    /// Snapshot + ingest a live file in one step. Files up to `INGEST_BUF_MAX` are read straight
    /// into memory, hashed from there and (if the object is new) written out as a plain file: no
    /// reflink snapshot, which on btrfs forces the source's dirty pages to disk and waits for the
    /// I/O (a synchronous disk write per freshly written file, and a stall for every other
    /// reflink behind it). Larger files take the reflink-snapshot path.
    pub fn ingest_live(&self, live: &Path) -> io::Result<Hashed> {
        let mut f = File::open(live)?;
        let size = f.metadata()?.len();
        if size > INGEST_BUF_MAX {
            let snap = self.tmp_path("snap");
            self.clone_file(live, &snap)?;
            return self.ingest(&snap);
        }
        let mut buf = Vec::with_capacity(size as usize);
        f.read_to_end(&mut buf)?;
        let h = hash_reader(io::Cursor::new(&buf), buf.len() as u64)?;
        let Some(hash) = object_hash(&h.content) else { return Ok(h) };
        let dst = self.cas_path(&hash);
        if dst.exists() {
            self.touch(&hash);
            return Ok(h);
        }
        let tmp = self.tmp_path("snap");
        let w = (|| -> io::Result<()> {
            if let Some(d) = dst.parent() {
                if !d.exists() {
                    fs::create_dir_all(d)?;
                }
            }
            let mut o = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o444).open(&tmp)?;
            io::Write::write_all(&mut o, &buf)?;
            fs::rename(&tmp, &dst)
        })();
        if let Err(e) = w {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(h)
    }

    /// Move `src` (a tmp file or a live file that is going away) into the CAS as the object `h`
    /// (0444). If the object exists, `src` is removed instead.
    pub fn adopt(&self, src: &Path, h: &Hash) -> io::Result<()> {
        let dst = self.cas_path(h);
        if dst.exists() {
            let _ = fs::remove_file(src);
            return Ok(());
        }
        if let Some(d) = dst.parent() {
            if !d.exists() {
                fs::create_dir_all(d)?;
            }
        }
        fs::set_permissions(src, fs::Permissions::from_mode(0o444))?;
        fs::rename(src, &dst)
    }

    /// Chunks (hash, offset, len) of a file, in order (re-derives the manifest).
    pub fn chunks_of(path: &Path) -> io::Result<Vec<(Hash, u64, u32)>> {
        let f = File::open(path)?;
        let mut out = vec![];
        for c in fastcdc::v2020::StreamCDC::with_level(f, CDC_MIN as usize, CDC_AVG as usize, CDC_MAX as usize, fastcdc::v2020::Normalization::Level1) {
            let c = c.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            out.push((Hash::of(&c.data), c.offset, c.length as u32));
        }
        Ok(out)
    }

    pub fn used_bytes(&self) -> u64 {
        let mut tot = 0;
        self.walk(|_, m| tot += m.len());
        tot
    }

    fn walk(&self, mut f: impl FnMut(Hash, &fs::Metadata)) {
        let Ok(rd) = fs::read_dir(self.dir.join("cas")) else { return };
        for sub in rd.flatten() {
            let Ok(rd2) = fs::read_dir(sub.path()) else { continue };
            for e in rd2.flatten() {
                let name = e.file_name();
                let Some(h) = parse_hex(&name.to_string_lossy()) else { continue };
                if let Ok(m) = e.metadata() {
                    f(h, &m);
                }
            }
        }
    }

    /// Free bytes on the cache filesystem.
    pub fn free_bytes(&self) -> u64 {
        let Ok(c) = std::ffi::CString::new(self.dir.as_os_str().as_encoded_bytes()) else { return u64::MAX };
        let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
            return u64::MAX;
        }
        s.f_bavail as u64 * s.f_frsize as u64
    }

    /// Delete least-recently-used objects for which `protect` is false until
    /// at least `need` bytes were freed. Returns bytes freed.
    pub fn evict(&self, need: u64, protect: &dyn Fn(&Hash) -> bool) -> u64 {
        let mut objs: Vec<(i64, Hash, u64)> = vec![];
        self.walk(|h, m| objs.push((m.mtime() * 1_000_000_000 + m.mtime_nsec(), h, m.len())));
        objs.sort();
        let mut freed = 0;
        for (_, h, len) in objs {
            if freed >= need {
                break;
            }
            if protect(&h) {
                continue;
            }
            if fs::remove_file(self.cas_path(&h)).is_ok() {
                freed += len;
            }
        }
        freed
    }

    /// All object hashes currently resident.
    pub fn resident(&self) -> HashSet<Hash> {
        let mut s = HashSet::new();
        self.walk(|h, _| {
            s.insert(h);
        });
        s
    }
}

fn parse_hex(s: &str) -> Option<Hash> {
    if s.len() != 64 {
        return None;
    }
    let mut h = [0u8; 32];
    for i in 0..32 {
        h[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(Hash(h))
}

fn hash_reader<R: Read>(mut r: R, size: u64) -> io::Result<Hashed> {
    if size == 0 {
        return Ok(Hashed { content: ContentId::Empty, size: 0, manifest: None });
    }
    if size <= SMALL_FILE_MAX {
        let mut buf = Vec::with_capacity(size as usize);
        r.read_to_end(&mut buf)?;
        let n = buf.len() as u64;
        if n == 0 {
            return Ok(Hashed { content: ContentId::Empty, size: 0, manifest: None });
        }
        return Ok(Hashed { content: ContentId::Blob(Hash::of(&buf)), size: n, manifest: None });
    }
    let mut chunks = vec![];
    let mut total = 0u64;
    for c in fastcdc::v2020::StreamCDC::with_level(r, CDC_MIN as usize, CDC_AVG as usize, CDC_MAX as usize, fastcdc::v2020::Normalization::Level1) {
        let c = c.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        total += c.length as u64;
        chunks.push((Hash::of(&c.data), c.length as u32));
    }
    if total <= SMALL_FILE_MAX {
        // file shrank while hashing; callers re-hash from a stable snapshot, so this is a corner case
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file changed while hashing"));
    }
    let m = Manifest { size: total, chunks };
    Ok(Hashed { content: m.id(), size: total, manifest: Some(m) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    fn write_rand(p: &Path, n: usize) -> Vec<u8> {
        let mut b = vec![0u8; n];
        rand::rng().fill_bytes(&mut b);
        fs::write(p, &b).unwrap();
        b
    }

    #[test]
    fn ingest_live_matches_ingest() {
        let d = tempfile::tempdir().unwrap();
        let c = Cache::open(d.path()).unwrap();
        for (i, n) in [0usize, 5, 60 << 10, 200 << 10, 3 << 20].into_iter().enumerate() {
            let live = d.path().join(format!("live{i}"));
            write_rand(&live, n);
            let want = Cache::hash_file(&live).unwrap();
            let got = c.ingest_live(&live).unwrap();
            assert_eq!(got.content, want.content, "size {n}");
            assert_eq!(got.size, want.size);
            assert_eq!(got.manifest.is_some(), want.manifest.is_some());
            if let Some(h) = object_hash(&got.content) {
                let o = c.cas_path(&h);
                assert_eq!(fs::read(&o).unwrap(), fs::read(&live).unwrap());
                assert_eq!(fs::metadata(&o).unwrap().permissions().mode() & 0o777, 0o444);
            }
            // the live file stays; ingesting again is a dedup hit
            assert!(live.exists());
            assert_eq!(c.ingest_live(&live).unwrap().content, want.content);
        }
        assert_eq!(fs::read_dir(d.path().join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn ingest_small_large_and_dedup() {
        let d = tempfile::tempdir().unwrap();
        let c = Cache::open(d.path()).unwrap();
        // empty
        let t = c.tmp_path("s");
        fs::write(&t, b"").unwrap();
        let h = c.ingest(&t).unwrap();
        assert_eq!(h.content, ContentId::Empty);
        assert!(!t.exists());
        // small blob
        let t = c.tmp_path("s");
        fs::write(&t, b"hello").unwrap();
        let h = c.ingest(&t).unwrap();
        assert_eq!(h.content, ContentId::Blob(Hash::of(b"hello")));
        assert!(c.has(&Hash::of(b"hello")));
        assert_eq!(fs::metadata(c.cas_path(&Hash::of(b"hello"))).unwrap().permissions().mode() & 0o777, 0o444);
        // dedup
        let t = c.tmp_path("s");
        fs::write(&t, b"hello").unwrap();
        c.ingest(&t).unwrap();
        assert!(!t.exists());
        // large: manifest covers file, chunk sizes in range, id matches Manifest::id
        let t = c.tmp_path("s");
        let data = write_rand(&t, 3 << 20);
        let h = c.ingest(&t).unwrap();
        let m = h.manifest.clone().unwrap();
        assert_eq!(h.size, data.len() as u64);
        assert_eq!(m.size, h.size);
        assert_eq!(m.chunks.iter().map(|c| c.1 as u64).sum::<u64>(), h.size);
        assert!(m.chunks.iter().rev().skip(1).all(|c| c.1 >= CDC_MIN && c.1 <= CDC_MAX));
        assert_eq!(h.content, m.id());
        let ContentId::Chunked(hh) = h.content else { panic!() };
        assert_eq!(fs::read(c.cas_path(&hh)).unwrap(), data);
        // chunks_of agrees with the manifest
        let ch = Cache::chunks_of(&c.cas_path(&hh)).unwrap();
        assert_eq!(ch.iter().map(|x| (x.0, x.2)).collect::<Vec<_>>(), m.chunks);
        // boundary: exactly 64 KiB is a blob, one more byte is chunked
        let t = c.tmp_path("s");
        write_rand(&t, SMALL_FILE_MAX as usize);
        assert!(matches!(c.ingest(&t).unwrap().content, ContentId::Blob(_)));
        let t = c.tmp_path("s");
        write_rand(&t, SMALL_FILE_MAX as usize + 1);
        assert!(matches!(c.ingest(&t).unwrap().content, ContentId::Chunked(_)));
    }

    #[test]
    fn clone_and_evict_lru() {
        let d = tempfile::tempdir().unwrap();
        let c = Cache::open(d.path()).unwrap();
        let mut hs = vec![];
        for i in 0..4u8 {
            let t = c.tmp_path("s");
            fs::write(&t, vec![i + 1; 1000]).unwrap();
            let h = c.ingest(&t).unwrap();
            hs.push(object_hash(&h.content).unwrap());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // clone is writable independently of the immutable object
        let live = c.live_path("r", 5);
        c.clone_file(&c.cas_path(&hs[0]), &live).unwrap();
        fs::OpenOptions::new().write(true).open(&live).unwrap();
        assert_eq!(fs::read(&live).unwrap(), vec![1u8; 1000]);
        assert_eq!(fs::read(c.cas_path(&hs[0])).unwrap(), vec![1u8; 1000]);
        // touch 0 so 1 is the LRU; protect 2
        c.touch(&hs[0]);
        let prot = hs[2];
        let freed = c.evict(1500, &|h| *h == prot);
        assert_eq!(freed, 2000);
        assert!(c.has(&hs[0]) && c.has(&hs[2]));
        assert!(!c.has(&hs[1]) && !c.has(&hs[3]));
        assert_eq!(c.used_bytes(), 2000);
    }
}
