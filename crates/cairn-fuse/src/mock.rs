//! `MockCore`: a small in-memory `Core` for FUSE-only tests. The namespace lives in memory; the
//! content of every regular file is a real file in a backing directory (`<backing>/<ino>`), which
//! is the node's live file (always present, shared by all openers), so passthrough, mmap and
//! `tail -f` behave exactly as with the real client.
//!
//! `deliver(change)` plays the role of Core receiving a remote change: it records the change as
//! in-flight, calls the registered `KernelApplier`, and then `apply_direct`s it. While in flight,
//! requests with `Origin::Remote` apply the change (idempotently) instead of mutating.

use cairn_client::api::*;
use cairn_proto::*;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;

struct N {
    node: NodeId,
    ino: Ino,
    kind: Kind,
    mode: u32,
    uid: u32,
    gid: u32,
    rdev: u64,
    atime: i64,
    mtime: i64,
    ctime: i64,
    xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
    target: Option<Vec<u8>>,
    children: BTreeMap<Vec<u8>, Ino>,
    links: Vec<(Ino, Vec<u8>)>,
    opens: u32,
    gen: u64,
}

struct Inner {
    nodes: HashMap<Ino, N>,
    by_node: HashMap<NodeId, Ino>,
    next_ino: Ino,
    next_node: u64,
    next_token: u64,
}

pub struct MockCore {
    inner: Mutex<Inner>,
    backing: PathBuf,
    applier: Mutex<Option<Arc<dyn KernelApplier>>>,
    inflight: Mutex<Option<Change>>,
}

fn now() -> i64 {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    d.as_secs() as i64 * 1_000_000_000 + d.subsec_nanos() as i64
}

type R<T> = FsResult<T>;

impl Inner {
    fn get(&self, ino: Ino) -> R<&N> {
        self.nodes.get(&ino).ok_or(Errno::ENOENT)
    }
    fn dir(&self, ino: Ino) -> R<&N> {
        let n = self.get(ino)?;
        if n.kind != Kind::Dir {
            return Err(Errno::ENOTDIR);
        }
        Ok(n)
    }
    fn touch(&mut self, ino: Ino, m: bool) {
        if let Some(n) = self.nodes.get_mut(&ino) {
            let t = now();
            n.ctime = t;
            if m {
                n.mtime = t;
            }
        }
    }
}

impl MockCore {
    pub fn new(backing: PathBuf) -> Arc<MockCore> {
        std::fs::create_dir_all(&backing).unwrap();
        let t = now();
        let root = N {
            node: NodeId::ROOT,
            ino: ROOT_INO,
            kind: Kind::Dir,
            mode: 0o755,
            uid: 0,
            gid: 0,
            rdev: 0,
            atime: t,
            mtime: t,
            ctime: t,
            xattrs: BTreeMap::new(),
            target: None,
            children: BTreeMap::new(),
            links: vec![],
            opens: 0,
            gen: 1,
        };
        let mut nodes = HashMap::new();
        nodes.insert(ROOT_INO, root);
        let mut by_node = HashMap::new();
        by_node.insert(NodeId::ROOT, ROOT_INO);
        Arc::new(MockCore {
            inner: Mutex::new(Inner { nodes, by_node, next_ino: 2, next_node: 1, next_token: 1 }),
            backing,
            applier: Mutex::new(None),
            inflight: Mutex::new(None),
        })
    }

    fn live(&self, ino: Ino) -> PathBuf {
        self.backing.join(ino.to_string())
    }

    fn info(&self, g: &Inner, n: &N) -> NodeInfo {
        let (mut size, mut mtime, mut ctime) = (0u64, n.mtime, n.ctime);
        let mut atime = n.atime;
        match n.kind {
            Kind::File => {
                if let Ok(m) = std::fs::metadata(self.live(n.ino)) {
                    size = m.len();
                    mtime = m.mtime() * 1_000_000_000 + m.mtime_nsec();
                    atime = atime.max(0);
                    ctime = ctime.max(m.ctime() * 1_000_000_000 + m.ctime_nsec());
                }
            }
            Kind::Symlink => size = n.target.as_ref().map_or(0, |t| t.len() as u64),
            _ => {}
        }
        let nlink = match n.kind {
            Kind::Dir => 2 + n.children.values().filter(|c| g.nodes.get(c).is_some_and(|c| c.kind == Kind::Dir)).count() as u32,
            _ => n.links.len() as u32,
        };
        NodeInfo {
            ino: n.ino,
            node: n.node,
            generation: n.gen,
            nlink,
            attrs: Attrs {
                kind: n.kind,
                mode: n.mode & 0o7777,
                uid: n.uid,
                gid: n.gid,
                size,
                mtime_ns: mtime,
                ctime_ns: ctime,
                atime_ns: atime,
                rdev: n.rdev,
                content: None,
                symlink_target: n.target.clone(),
                xattrs: n.xattrs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            },
        }
    }

    fn new_node(&self, g: &mut Inner, node: Option<NodeId>, kind: Kind, mode: u32, uid: u32, gid: u32, rdev: u64) -> Ino {
        let ino = g.next_ino;
        g.next_ino += 1;
        let node = node.unwrap_or_else(|| {
            g.next_node += 1;
            NodeId::new(HostId(1), g.next_node)
        });
        let t = now();
        g.nodes.insert(
            ino,
            N {
                node,
                ino,
                kind,
                mode: mode & 0o7777,
                uid,
                gid,
                rdev,
                atime: t,
                mtime: t,
                ctime: t,
                xattrs: BTreeMap::new(),
                target: None,
                children: BTreeMap::new(),
                links: vec![],
                opens: 0,
                gen: 1,
            },
        );
        g.by_node.insert(node, ino);
        if kind == Kind::File {
            let _ = std::fs::File::create(self.live(ino));
        }
        ino
    }

    fn add_link(&self, g: &mut Inner, parent: Ino, name: &[u8], ino: Ino) {
        g.nodes.get_mut(&parent).unwrap().children.insert(name.to_vec(), ino);
        g.nodes.get_mut(&ino).unwrap().links.push((parent, name.to_vec()));
        g.touch(parent, true);
        g.touch(ino, false);
    }

    fn drop_link(&self, g: &mut Inner, parent: Ino, name: &[u8]) {
        let Some(ino) = g.nodes.get_mut(&parent).and_then(|p| p.children.remove(name)) else { return };
        g.touch(parent, true);
        let mut remove_data = false;
        if let Some(n) = g.nodes.get_mut(&ino) {
            n.links.retain(|(p, nm)| !(*p == parent && nm == name));
            n.ctime = now();
            remove_data = n.links.is_empty() && n.opens == 0 && n.kind == Kind::File;
        }
        if remove_data {
            let _ = std::fs::remove_file(self.live(ino));
        }
    }

    fn check_new(&self, g: &Inner, parent: Ino, name: &[u8]) -> R<()> {
        let p = g.dir(parent)?;
        if name.len() > 255 {
            return Err(Errno::ENAMETOOLONG);
        }
        if p.children.contains_key(name) {
            return Err(Errno::EEXIST);
        }
        Ok(())
    }

    fn apply_inflight(&self) -> R<()> {
        let c = self.inflight.lock().clone();
        match c {
            Some(c) => self.apply_direct(&c),
            None => Ok(()),
        }
    }

    fn set_attrs_locked(&self, g: &mut Inner, ino: Ino, a: &Attrs) {
        let live = self.live(ino);
        let Some(n) = g.nodes.get_mut(&ino) else { return };
        n.mode = a.mode & 0o7777;
        n.uid = a.uid;
        n.gid = a.gid;
        n.atime = a.atime_ns;
        n.mtime = a.mtime_ns;
        n.ctime = a.ctime_ns;
        n.xattrs = a.xattrs.iter().cloned().collect();
        if n.kind == Kind::File {
            if let Ok(f) = OpenOptions::new().write(true).open(&live) {
                let _ = f.set_len(a.size);
                let t = |ns: i64| libc::timespec { tv_sec: ns.div_euclid(1_000_000_000), tv_nsec: ns.rem_euclid(1_000_000_000) };
                let ts = [t(a.atime_ns), t(a.mtime_ns)];
                use std::os::fd::AsRawFd;
                unsafe { libc::futimens(f.as_raw_fd(), ts.as_ptr()) };
            }
        }
    }

    /// Simulate Core receiving a remote change: in-flight, applier, direct (like client-core).
    pub fn deliver(&self, change: Change) -> R<()> {
        *self.inflight.lock() = Some(change.clone());
        let ap = self.applier.lock().clone();
        let r = match ap {
            Some(a) => a.apply(&change),
            None => Ok(()),
        };
        let r2 = self.apply_direct(&change);
        *self.inflight.lock() = None;
        r.and(r2)
    }

    pub fn attrs_of(&self, ino: Ino) -> Option<Attrs> {
        let g = self.inner.lock();
        g.nodes.get(&ino).map(|n| self.info(&g, n).attrs)
    }

    pub fn node_of(&self, ino: Ino) -> Option<NodeId> {
        self.inner.lock().nodes.get(&ino).map(|n| n.node)
    }

    pub fn ino_of(&self, node: NodeId) -> Option<Ino> {
        self.inner.lock().by_node.get(&node).copied()
    }
}

fn kind_attrs(kind: Kind, mode: u32, size: u64) -> Attrs {
    let t = now();
    Attrs {
        kind,
        mode,
        uid: 0,
        gid: 0,
        size,
        mtime_ns: t,
        ctime_ns: t,
        atime_ns: t,
        rdev: 0,
        content: None,
        symlink_target: None,
        xattrs: vec![],
    }
}

/// Fresh attrs for building synthetic changes.
pub fn new_attrs(kind: Kind, mode: u32, size: u64) -> Attrs {
    kind_attrs(kind, mode, size)
}

impl Core for MockCore {
    fn lookup(&self, parent: Ino, name: &[u8]) -> FsResult<NodeInfo> {
        let g = self.inner.lock();
        let p = g.dir(parent)?;
        let ino = *p.children.get(name).ok_or(Errno::ENOENT)?;
        Ok(self.info(&g, g.get(ino)?))
    }

    fn getattr(&self, ino: Ino) -> FsResult<NodeInfo> {
        let g = self.inner.lock();
        Ok(self.info(&g, g.get(ino)?))
    }

    fn readdir(&self, dir: Ino) -> FsResult<Arc<DirSnapshot>> {
        let g = self.inner.lock();
        let d = g.dir(dir)?;
        let entries = d
            .children
            .iter()
            .map(|(name, ino)| DirEntryInfo { name: name.clone(), ino: *ino, kind: g.nodes[ino].kind })
            .collect();
        Ok(Arc::new(DirSnapshot { entries }))
    }

    fn getxattr(&self, ino: Ino, name: &[u8]) -> FsResult<Vec<u8>> {
        let g = self.inner.lock();
        g.get(ino)?.xattrs.get(name).cloned().ok_or(Errno::ENODATA)
    }

    fn listxattr(&self, ino: Ino) -> FsResult<Vec<Vec<u8>>> {
        let g = self.inner.lock();
        Ok(g.get(ino)?.xattrs.keys().cloned().collect())
    }

    fn path_of(&self, ino: Ino) -> Option<Vec<u8>> {
        let g = self.inner.lock();
        let mut parts: Vec<Vec<u8>> = vec![];
        let mut cur = ino;
        while cur != ROOT_INO {
            let n = g.nodes.get(&cur)?;
            let (p, name) = n.links.first()?;
            parts.push(name.clone());
            cur = *p;
        }
        parts.reverse();
        Some(parts.join(&b'/'))
    }

    fn create(&self, origin: Origin, parent: Ino, name: &[u8], new: NewNode) -> FsResult<NodeInfo> {
        if origin == Origin::Remote {
            self.apply_inflight()?;
            return self.lookup(parent, name);
        }
        let mut g = self.inner.lock();
        self.check_new(&g, parent, name)?;
        let ino = self.new_node(&mut g, None, new.kind, new.mode, new.uid, new.gid, new.rdev);
        {
            let n = g.nodes.get_mut(&ino).unwrap();
            n.target = new.symlink_target;
            n.xattrs = new.xattrs.into_iter().collect();
        }
        self.add_link(&mut g, parent, name, ino);
        Ok(self.info(&g, &g.nodes[&ino]))
    }

    fn link(&self, origin: Origin, ino: Ino, new_parent: Ino, new_name: &[u8]) -> FsResult<NodeInfo> {
        if origin == Origin::Remote {
            self.apply_inflight()?;
            return self.lookup(new_parent, new_name);
        }
        let mut g = self.inner.lock();
        if g.get(ino)?.kind == Kind::Dir {
            return Err(Errno::EPERM);
        }
        if g.get(ino)?.links.len() >= 65000 {
            return Err(Errno::EMLINK);
        }
        self.check_new(&g, new_parent, new_name)?;
        self.add_link(&mut g, new_parent, new_name, ino);
        Ok(self.info(&g, &g.nodes[&ino]))
    }

    fn unlink(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()> {
        if origin == Origin::Remote {
            return self.apply_inflight();
        }
        let mut g = self.inner.lock();
        let ino = *g.dir(parent)?.children.get(name).ok_or(Errno::ENOENT)?;
        if g.get(ino)?.kind == Kind::Dir {
            return Err(Errno::EISDIR);
        }
        self.drop_link(&mut g, parent, name);
        Ok(())
    }

    fn rmdir(&self, origin: Origin, parent: Ino, name: &[u8]) -> FsResult<()> {
        if origin == Origin::Remote {
            return self.apply_inflight();
        }
        let mut g = self.inner.lock();
        let ino = *g.dir(parent)?.children.get(name).ok_or(Errno::ENOENT)?;
        let n = g.get(ino)?;
        if n.kind != Kind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if !n.children.is_empty() {
            return Err(Errno::ENOTEMPTY);
        }
        self.drop_link(&mut g, parent, name);
        Ok(())
    }

    fn rename(&self, origin: Origin, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8], mode: RenameMode) -> FsResult<()> {
        if origin == Origin::Remote {
            return self.apply_inflight();
        }
        let mut g = self.inner.lock();
        g.dir(new_parent)?;
        if new_name.len() > 255 {
            return Err(Errno::ENAMETOOLONG);
        }
        let src = *g.dir(parent)?.children.get(name).ok_or(Errno::ENOENT)?;
        let dst = g.nodes[&new_parent].children.get(new_name).copied();
        if dst == Some(src) && mode != RenameMode::Exchange {
            return Ok(());
        }
        let sk = g.nodes[&src].kind;
        // A directory cannot move into its own subtree.
        if sk == Kind::Dir {
            let mut cur = new_parent;
            loop {
                if cur == src {
                    return Err(Errno::EINVAL);
                }
                if cur == ROOT_INO {
                    break;
                }
                match g.nodes[&cur].links.first() {
                    Some((p, _)) => cur = *p,
                    None => break,
                }
            }
        }
        match (mode, dst) {
            (RenameMode::NoReplace, Some(_)) => return Err(Errno::EEXIST),
            (RenameMode::Exchange, None) => return Err(Errno::ENOENT),
            (RenameMode::Exchange, Some(d)) => {
                // swap the two dirents
                g.nodes.get_mut(&parent).unwrap().children.insert(name.to_vec(), d);
                g.nodes.get_mut(&new_parent).unwrap().children.insert(new_name.to_vec(), src);
                for (ino, op, on, np, nn) in [(src, parent, name, new_parent, new_name), (d, new_parent, new_name, parent, name)] {
                    let n = g.nodes.get_mut(&ino).unwrap();
                    for l in n.links.iter_mut() {
                        if l.0 == op && l.1 == on {
                            *l = (np, nn.to_vec());
                        }
                    }
                }
                for i in [src, d, parent, new_parent] {
                    g.touch(i, i == parent || i == new_parent);
                }
                return Ok(());
            }
            (_, Some(d)) => {
                let dn = &g.nodes[&d];
                match (sk == Kind::Dir, dn.kind == Kind::Dir) {
                    (true, false) => return Err(Errno::ENOTDIR),
                    (false, true) => return Err(Errno::EISDIR),
                    (true, true) if !dn.children.is_empty() => return Err(Errno::ENOTEMPTY),
                    _ => {}
                }
                self.drop_link(&mut g, new_parent, new_name);
            }
            _ => {}
        }
        g.nodes.get_mut(&parent).unwrap().children.remove(name);
        g.nodes.get_mut(&src).unwrap().links.retain(|(p, n)| !(*p == parent && n == name));
        self.add_link(&mut g, new_parent, new_name, src);
        g.touch(parent, true);
        Ok(())
    }

    fn setattr(&self, origin: Origin, ino: Ino, ch: AttrChanges, size: Option<u64>) -> FsResult<NodeInfo> {
        if origin == Origin::Remote {
            self.apply_inflight()?;
            return self.getattr(ino);
        }
        let mut g = self.inner.lock();
        let live = self.live(ino);
        let kind = g.get(ino)?.kind;
        if let Some(sz) = size {
            match kind {
                Kind::Dir => return Err(Errno::EISDIR),
                Kind::File => {
                    let f = OpenOptions::new().write(true).open(&live).map_err(|_| Errno::EIO)?;
                    f.set_len(sz).map_err(|_| Errno::EIO)?;
                }
                _ => return Err(Errno::EINVAL),
            }
        }
        let n = g.nodes.get_mut(&ino).unwrap();
        let t = now();
        if let Some(m) = ch.mode {
            n.mode = m & 0o7777;
        }
        if let Some(u) = ch.uid {
            n.uid = u;
        }
        if let Some(gid) = ch.gid {
            n.gid = gid;
        }
        if let Some(a) = ch.atime_ns {
            n.atime = a;
        }
        if let Some(m) = ch.mtime_ns {
            n.mtime = m;
        }
        n.ctime = t;
        if size.is_some() && ch.mtime_ns.is_none() {
            n.mtime = t;
        }
        if kind == Kind::File && (ch.atime_ns.is_some() || ch.mtime_ns.is_some()) {
            let ts = |ns: Option<i64>| match ns {
                Some(ns) => libc::timespec { tv_sec: ns.div_euclid(1_000_000_000), tv_nsec: ns.rem_euclid(1_000_000_000) },
                None => libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
            };
            let times = [ts(ch.atime_ns), ts(ch.mtime_ns)];
            let c = std::ffi::CString::new(live.as_os_str().as_bytes()).unwrap();
            unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) };
        }
        Ok(self.info(&g, &g.nodes[&ino]))
    }

    fn setxattr(&self, origin: Origin, ino: Ino, name: &[u8], value: &[u8], flags: XattrFlags) -> FsResult<()> {
        if origin == Origin::Remote {
            return self.apply_inflight();
        }
        let mut g = self.inner.lock();
        let n = g.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        let has = n.xattrs.contains_key(name);
        match flags {
            XattrFlags::Create if has => return Err(Errno::EEXIST),
            XattrFlags::Replace if !has => return Err(Errno::ENODATA),
            _ => {}
        }
        n.xattrs.insert(name.to_vec(), value.to_vec());
        n.ctime = now();
        Ok(())
    }

    fn removexattr(&self, origin: Origin, ino: Ino, name: &[u8]) -> FsResult<()> {
        if origin == Origin::Remote {
            return self.apply_inflight();
        }
        let mut g = self.inner.lock();
        let n = g.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        n.xattrs.remove(name).ok_or(Errno::ENODATA)?;
        n.ctime = now();
        Ok(())
    }

    fn open(&self, _origin: Origin, ino: Ino, write: bool, truncate: bool) -> FsResult<OpenFile> {
        let mut g = self.inner.lock();
        let n = g.nodes.get_mut(&ino).ok_or(Errno::ENOENT)?;
        match n.kind {
            Kind::File => {}
            Kind::Dir => return Err(Errno::EISDIR),
            _ => return Err(Errno::EINVAL),
        }
        let mut oo = OpenOptions::new();
        oo.read(true);
        if write || truncate {
            oo.write(true);
        }
        if truncate {
            oo.truncate(true);
        }
        let file = oo.open(self.live(ino)).map_err(|_| Errno::EIO)?;
        n.opens += 1;
        let token = g.next_token;
        g.next_token += 1;
        Ok(OpenFile { file, token })
    }

    fn release(&self, ino: Ino, _token: u64) -> FsResult<()> {
        let mut g = self.inner.lock();
        if let Some(n) = g.nodes.get_mut(&ino) {
            n.opens = n.opens.saturating_sub(1);
            if n.opens == 0 && n.links.is_empty() && n.kind == Kind::File {
                let _ = std::fs::remove_file(self.live(ino));
            }
        }
        Ok(())
    }

    fn fsync(&self, _ino: Ino, _datasync: bool) -> FsResult<()> {
        Ok(())
    }

    fn sync_all(&self) -> FsResult<()> {
        Ok(())
    }

    fn set_applier(&self, applier: Arc<dyn KernelApplier>) {
        *self.applier.lock() = Some(applier);
    }

    fn apply_direct(&self, c: &Change) -> FsResult<()> {
        let mut g = self.inner.lock();
        match &c.effect {
            Effect::Upsert { parent, name, node, attrs } => {
                let Some(&p) = g.by_node.get(parent) else { return Ok(()) };
                if g.get(p)?.kind != Kind::Dir {
                    return Ok(());
                }
                let existing = g.nodes[&p].children.get(&name.0).copied();
                let known = g.by_node.get(node).copied();
                match (existing, known) {
                    (Some(e), Some(k)) if e == k => {
                        self.set_attrs_locked(&mut g, e, attrs);
                    }
                    (e, k) => {
                        if e.is_some() {
                            self.drop_link(&mut g, p, &name.0);
                        }
                        match k {
                            Some(k) if attrs.kind != Kind::Dir => self.add_link(&mut g, p, &name.0, k),
                            _ => {
                                let ino = self.new_node(&mut g, Some(*node), attrs.kind, attrs.mode, attrs.uid, attrs.gid, attrs.rdev);
                                g.nodes.get_mut(&ino).unwrap().target = attrs.symlink_target.clone();
                                self.add_link(&mut g, p, &name.0, ino);
                                self.set_attrs_locked(&mut g, ino, attrs);
                            }
                        }
                    }
                }
            }
            Effect::Remove { parent, name, .. } => {
                if let Some(&p) = g.by_node.get(parent) {
                    self.drop_link(&mut g, p, &name.0);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                let (Some(&sp), Some(&dp)) = (g.by_node.get(src_parent), g.by_node.get(dst_parent)) else { return Ok(()) };
                let Some(&ino) = g.by_node.get(node) else { return Ok(()) };
                if g.nodes[&sp].children.get(&src_name.0) == Some(&ino) {
                    if g.nodes[&dp].children.contains_key(&dst_name.0) {
                        self.drop_link(&mut g, dp, &dst_name.0);
                    }
                    g.nodes.get_mut(&sp).unwrap().children.remove(&src_name.0);
                    g.nodes.get_mut(&ino).unwrap().links.retain(|(p, n)| !(*p == sp && n == &src_name.0));
                    self.add_link(&mut g, dp, &dst_name.0, ino);
                }
            }
            Effect::Attrs { node, attrs } => {
                if let Some(&ino) = g.by_node.get(node) {
                    self.set_attrs_locked(&mut g, ino, attrs);
                }
            }
        }
        Ok(())
    }
}
