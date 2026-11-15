//! Content-addressed store: `<data>/cas/ab/<hex>` files, written tmp+fsync+rename.

use cairn_proto::Hash;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Cas {
    root: PathBuf,
}

impl Cas {
    pub fn new(data: &Path) -> io::Result<Cas> {
        let root = data.join("cas");
        std::fs::create_dir_all(&root)?;
        Ok(Cas { root })
    }
    pub fn path(&self, h: &Hash) -> PathBuf {
        let hex = h.to_hex();
        self.root.join(&hex[..2]).join(hex)
    }
    pub fn has(&self, h: &Hash) -> bool {
        self.path(h).is_file()
    }
    pub fn get(&self, h: &Hash) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(self.path(h)) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Durable write of many blobs in one pass (caller has verified the hashes): temp files are
    /// written and renamed into place, then one `syncfs` makes all of them (and the renames)
    /// durable, instead of two fsyncs per blob. Idempotent.
    pub fn put_many(&self, blobs: &[(Hash, Vec<u8>)]) -> io::Result<()> {
        let mut any = false;
        for (h, data) in blobs {
            let dst = self.path(h);
            if dst.is_file() {
                continue;
            }
            let dir = dst.parent().unwrap();
            std::fs::create_dir_all(dir)?;
            let mut tmp = tempfile::Builder::new().prefix(".tmp-").tempfile_in(dir)?;
            tmp.write_all(data)?;
            tmp.persist(&dst).map_err(|e| e.error)?;
            any = true;
        }
        if any {
            let f = std::fs::File::open(&self.root)?;
            use std::os::unix::io::AsRawFd;
            if unsafe { libc::syncfs(f.as_raw_fd()) } != 0 {
                // not fatal on filesystems without syncfs semantics: fall back to per-file fsync
                for (h, _) in blobs {
                    if let Ok(f) = std::fs::File::open(self.path(h)) {
                        f.sync_all()?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Durable write (caller has verified the hash). Idempotent.
    pub fn put(&self, h: &Hash, data: &[u8]) -> io::Result<()> {
        let dst = self.path(h);
        if dst.is_file() {
            return Ok(());
        }
        let dir = dst.parent().unwrap();
        std::fs::create_dir_all(dir)?;
        let mut tmp = tempfile::Builder::new().prefix(".tmp-").tempfile_in(dir)?;
        tmp.write_all(data)?;
        tmp.as_file().sync_all()?;
        tmp.persist(&dst).map_err(|e| e.error)?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }
}
