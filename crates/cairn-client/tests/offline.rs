//! Offline-mode tests: no server; namespace ops, content lifecycle, recovery.

use cairn_client::api::*;
use cairn_client::testing::DirectApplier;
use cairn_client::{Client, Config, RootConfig};
use cairn_proto::*;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

const U: Origin = Origin::Local { uid: 1000, gid: 1000, pid: 1 };

fn nn(kind: Kind) -> NewNode {
    NewNode { kind, mode: 0o644, uid: 1000, gid: 1000, rdev: 0, symlink_target: None, xattrs: vec![] }
}

fn open_client(dir: &Path, cfgf: impl FnOnce(Config) -> Config) -> (Client, Arc<dyn Core>) {
    let cfg = cfgf(Config::offline(dir).with_root("r", RootConfig::default()));
    let c = Client::open(cfg).unwrap();
    let core = c.core("r").unwrap();
    DirectApplier::install(&core);
    (c, core)
}

fn write_file(core: &Arc<dyn Core>, parent: Ino, name: &str, data: &[u8]) -> NodeInfo {
    let n = core.create(U, parent, name.as_bytes(), nn(Kind::File)).unwrap();
    let mut of = core.open(U, n.ino, true, false).unwrap();
    of.file.write_all(data).unwrap();
    core.release(n.ino, of.token).unwrap();
    n
}

fn read_file(core: &Arc<dyn Core>, ino: Ino) -> Vec<u8> {
    let mut of = core.open(U, ino, false, false).unwrap();
    let mut v = vec![];
    of.file.seek(SeekFrom::Start(0)).unwrap();
    of.file.read_to_end(&mut v).unwrap();
    core.release(ino, of.token).unwrap();
    v
}

#[test]
fn namespace_basics_and_errno() {
    let d = tempfile::tempdir().unwrap();
    let (_c, core) = open_client(d.path(), |c| c);
    let dir = core.create(U, ROOT_INO, b"d", nn(Kind::Dir)).unwrap();
    assert_eq!(dir.attrs.kind, Kind::Dir);
    let f = core.create(U, dir.ino, b"f", nn(Kind::File)).unwrap();
    assert_eq!(core.lookup(dir.ino, b"f").unwrap().ino, f.ino);
    assert_eq!(core.lookup(dir.ino, b"nope").unwrap_err(), Errno::ENOENT);
    assert_eq!(core.create(U, dir.ino, b"f", nn(Kind::File)).unwrap_err(), Errno::EEXIST);
    assert_eq!(core.rmdir(U, ROOT_INO, b"d").unwrap_err(), Errno::ENOTEMPTY);
    assert_eq!(core.unlink(U, ROOT_INO, b"d").unwrap_err(), Errno::EISDIR);
    assert_eq!(core.path_of(f.ino).unwrap(), b"d/f".to_vec());
    // hard link + rename + unlink
    let l = core.link(U, f.ino, ROOT_INO, b"g").unwrap();
    assert_eq!(l.nlink, 2);
    core.rename(U, ROOT_INO, b"g", dir.ino, b"h", RenameMode::NoReplace).unwrap();
    assert_eq!(core.rename(U, ROOT_INO, b"nope", dir.ino, b"h", RenameMode::Replace).unwrap_err(), Errno::ENOENT);
    assert_eq!(core.rename(U, dir.ino, b"f", dir.ino, b"h", RenameMode::NoReplace).unwrap_err(), Errno::EEXIST);
    assert_eq!(core.getattr(f.ino).unwrap().nlink, 2);
    core.unlink(U, dir.ino, b"h").unwrap();
    assert_eq!(core.getattr(f.ino).unwrap().nlink, 1);
    let names: Vec<_> = core.readdir(dir.ino).unwrap().entries.iter().map(|e| e.name.clone()).collect();
    assert_eq!(names, vec![b"f".to_vec()]);
    // symlink
    let mut s = nn(Kind::Symlink);
    s.symlink_target = Some(b"d/f".to_vec());
    let sl = core.create(U, ROOT_INO, b"sl", s).unwrap();
    assert_eq!(sl.attrs.symlink_target.as_deref(), Some(&b"d/f"[..]));
    assert_eq!(sl.attrs.size, 3);
    // xattrs and setattr
    core.setxattr(U, f.ino, b"user.k", b"v", XattrFlags::Create).unwrap();
    assert_eq!(core.setxattr(U, f.ino, b"user.k", b"w", XattrFlags::Create).unwrap_err(), Errno::EEXIST);
    assert_eq!(core.getxattr(f.ino, b"user.k").unwrap(), b"v");
    assert_eq!(core.listxattr(f.ino).unwrap(), vec![b"user.k".to_vec()]);
    core.removexattr(U, f.ino, b"user.k").unwrap();
    assert_eq!(core.getxattr(f.ino, b"user.k").unwrap_err(), Errno::ENODATA);
    let a = core.setattr(U, f.ino, AttrChanges { mode: Some(0o600), uid: Some(5), ..Default::default() }, None).unwrap();
    assert_eq!((a.attrs.mode, a.attrs.uid), (0o600, 5));
}

#[test]
fn content_lifecycle_and_dedup() {
    let d = tempfile::tempdir().unwrap();
    let (c, core) = open_client(d.path(), |c| c);
    let a = write_file(&core, ROOT_INO, "a", b"hello world");
    let b = write_file(&core, ROOT_INO, "b", b"hello world");
    assert!(c.wait_pipeline(std::time::Duration::from_secs(10)));
    let attrs = core.getattr(a.ino).unwrap().attrs;
    assert_eq!(attrs.size, 11);
    assert_eq!(attrs.content, Some(ContentId::Blob(Hash::of(b"hello world"))));
    assert_eq!(core.getattr(b.ino).unwrap().attrs.content, attrs.content);
    // one CAS object for both
    assert_eq!(c.cache().resident().len(), 1);
    assert_eq!(read_file(&core, a.ino), b"hello world");
    // truncate without an open fd
    let n = core.setattr(U, a.ino, AttrChanges::default(), Some(5)).unwrap();
    assert_eq!(n.attrs.size, 5);
    assert_eq!(read_file(&core, a.ino), b"hello");
    // O_TRUNC then rewrite; open writer shows live size
    let mut of = core.open(U, a.ino, true, true).unwrap();
    assert_eq!(core.getattr(a.ino).unwrap().attrs.size, 0);
    of.file.write_all(b"xyz").unwrap();
    assert_eq!(core.getattr(a.ino).unwrap().attrs.size, 3);
    core.release(a.ino, of.token).unwrap();
    assert_eq!(read_file(&core, a.ino), b"xyz");
    assert!(c.wait_pipeline(std::time::Duration::from_secs(10)));
    // large file goes chunked
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
    let l = write_file(&core, ROOT_INO, "big", &big);
    assert!(c.wait_pipeline(std::time::Duration::from_secs(10)));
    assert!(matches!(core.getattr(l.ino).unwrap().attrs.content, Some(ContentId::Chunked(_))));
    assert_eq!(read_file(&core, l.ino), big);
    // unlinking an open file keeps it usable
    let mut of = core.open(U, l.ino, false, false).unwrap();
    core.unlink(U, ROOT_INO, b"big").unwrap();
    assert_eq!(core.getattr(l.ino).unwrap().nlink, 0);
    let mut buf = [0u8; 4];
    of.file.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, &big[..4]);
    core.release(l.ino, of.token).unwrap();
    assert_eq!(core.getattr(l.ino).unwrap_err(), Errno::ENOENT);
}

#[test]
fn journal_survives_restart() {
    let d = tempfile::tempdir().unwrap();
    let (ino_a, ino_dir);
    {
        let (c, core) = open_client(d.path(), |c| c);
        let dir = core.create(U, ROOT_INO, b"dir", nn(Kind::Dir)).unwrap();
        let a = write_file(&core, dir.ino, "a", b"persist me");
        core.rename(U, dir.ino, b"a", ROOT_INO, b"moved", RenameMode::Replace).unwrap();
        core.sync_all().unwrap();
        ino_a = a.ino;
        ino_dir = dir.ino;
        let r = c.root("r").unwrap();
        assert!(c.wait_pipeline(std::time::Duration::from_secs(10)));
        assert_eq!(r.pending_len(), 2); // create dir, create a at its final name (content and rename folded in)
        drop(c);
    }
    let (c, core) = open_client(d.path(), |c| c);
    assert_eq!(c.root("r").unwrap().pending_len(), 2);
    let m = core.lookup(ROOT_INO, b"moved").unwrap();
    assert_eq!(m.ino, ino_a, "inos are stable across restarts");
    assert_eq!(core.lookup(ROOT_INO, b"dir").unwrap().ino, ino_dir);
    assert_eq!(m.attrs.size, 10);
    assert_eq!(read_file(&core, m.ino), b"persist me");
    assert!(core.readdir(ino_dir).unwrap().entries.is_empty());
}

#[test]
fn local_only_policy_never_journals() {
    let d = tempfile::tempdir().unwrap();
    let rc: RootConfig = toml_root(
        r#"
[[policy]]
match = ["**/target/**"]
mode = "local_only"
"#,
    );
    let cfg = Config::offline(d.path()).with_root("r", rc);
    let c = Client::open(cfg).unwrap();
    let core = c.core("r").unwrap();
    DirectApplier::install(&core);
    let t = core.create(U, ROOT_INO, b"target", nn(Kind::Dir)).unwrap();
    let o = write_file(&core, t.ino, "x.o", b"obj");
    let _src = write_file(&core, ROOT_INO, "main.rs", b"fn main(){}");
    let r = c.root("r").unwrap();
    assert_eq!(r.pending_len(), 1, "only main.rs (create with content) is journaled");
    assert_eq!(read_file(&core, o.ino), b"obj");
    assert_eq!(core.rename(U, ROOT_INO, b"main.rs", t.ino, b"main.rs", RenameMode::Replace).unwrap_err(), Errno::EXDEV);
    drop(c);
    // local-only nodes persist
    let cfg = Config::offline(d.path()).with_root("r", toml_root("[[policy]]\nmatch = [\"**/target/**\"]\nmode = \"local_only\"\n"));
    let c = Client::open(cfg).unwrap();
    let core = c.core("r").unwrap();
    let t2 = core.lookup(ROOT_INO, b"target").unwrap();
    let o2 = core.lookup(t2.ino, b"x.o").unwrap();
    assert_eq!(o2.attrs.size, 3);
    assert_eq!(read_file(&core, o2.ino), b"obj");
}

fn toml_root(s: &str) -> RootConfig {
    let c = Config::from_toml(&format!("creds_dir=\"/x\"\ncache_dir=\"/y\"\n[root.r]\n{}", s.replace("[[policy]]", "[[root.r.policy]]"))).unwrap();
    c.roots["r"].clone()
}

#[test]
fn recovers_dirty_live_file_after_crash() {
    let d = tempfile::tempdir().unwrap();
    let (ino, token_path);
    {
        let (c, core) = open_client(d.path(), |c| c);
        let n = core.create(U, ROOT_INO, b"w", nn(Kind::File)).unwrap();
        core.sync_all().unwrap();
        let mut of = core.open(U, n.ino, true, false).unwrap();
        of.file.write_all(b"unsaved by release").unwrap();
        of.file.sync_all().unwrap();
        ino = n.ino;
        token_path = d.path().join("live/r").join(ino.to_string());
        assert!(token_path.exists());
        // simulate a crash: the client is dropped without release
        std::mem::forget(of);
        drop(c);
    }
    let (_c, core) = open_client(d.path(), |c| c);
    let n = core.getattr(ino).unwrap();
    assert_eq!(n.attrs.size, 18);
    assert_eq!(read_file(&core, ino), b"unsaved by release");
}

#[test]
fn unchanged_writer_close_skips_commit_and_replaced_dir_stays_reachable() {
    let d = tempfile::tempdir().unwrap();
    let (c, core) = open_client(d.path(), |c| c);
    let n = write_file(&core, ROOT_INO, "f", b"data");
    let before = c.root("r").unwrap().pending_len();
    let of = core.open(U, n.ino, true, false).unwrap();
    core.release(n.ino, of.token).unwrap();
    assert_eq!(c.root("r").unwrap().pending_len(), before, "no-op writer must not commit");
    let mut of = core.open(U, n.ino, true, false).unwrap();
    of.file.write_all(b"X").unwrap();
    core.release(n.ino, of.token).unwrap();
    assert_eq!(c.root("r").unwrap().pending_len(), before, "content folded into the pending create");
    assert_eq!(read_file(&core, n.ino), b"Xata");
    // directory replaced by rename stays reachable by ino
    let d1 = core.create(U, ROOT_INO, b"d1", nn(Kind::Dir)).unwrap();
    let d2 = core.create(U, ROOT_INO, b"d2", nn(Kind::Dir)).unwrap();
    core.rename(U, ROOT_INO, b"d1", ROOT_INO, b"d2", RenameMode::Replace).unwrap();
    let a = core.getattr(d2.ino).unwrap();
    assert_eq!(a.nlink, 0);
    assert_eq!(core.getattr(d1.ino).unwrap().nlink, 2);
}

#[test]
fn huge_sparse_write_is_not_hashed() {
    let d = tempfile::tempdir().unwrap();
    let (c, core) = open_client(d.path(), |c| c);
    let n = core.create(U, ROOT_INO, b"sparse", nn(Kind::File)).unwrap();
    let mut of = core.open(U, n.ino, true, false).unwrap();
    of.file.seek(SeekFrom::Start(17_592_186_040_320)).unwrap();
    of.file.write_all(&[1u8; 4096]).unwrap();
    let t = std::time::Instant::now();
    // the save is processed in the background and refused there (EFBIG is logged, the file stays dirty)
    core.release(n.ino, of.token).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(t.elapsed() < std::time::Duration::from_secs(5));
    assert!(matches!(core.getattr(n.ino).unwrap().attrs.content, None | Some(ContentId::Empty)), "never published");
    drop(c);
}

#[test]
fn reopen_unlinked_open_file() {
    let d = tempfile::tempdir().unwrap();
    let (_c, core) = open_client(d.path(), |c| c);
    let n = write_file(&core, ROOT_INO, "z", b"zombie data");
    let of = core.open(U, n.ino, true, false).unwrap();
    core.unlink(U, ROOT_INO, b"z").unwrap();
    assert_eq!(read_file(&core, n.ino), b"zombie data");
    core.release(n.ino, of.token).unwrap();
    assert_eq!(core.open(U, n.ino, false, false).err(), Some(Errno::ENOENT));
}

fn copy_dir(from: &Path, to: &Path) {
    let st = std::process::Command::new("cp").arg("-a").arg(from).arg(to).status().unwrap();
    assert!(st.success());
}

/// A kill -9 leaves the cache directory exactly as it is at some instant. Copy it while the
/// client keeps running (SQLite has seen nothing: the checkpointer is made very slow) and start a
/// second client on the copy: every acknowledged op must be there, content included.
#[test]
fn crash_recovery_from_the_op_log_alone() {
    let d = tempfile::tempdir().unwrap();
    let a = d.path().join("a");
    let (c, core) = open_client(&a, |mut c| {
        c.commit_interval_ms = 600_000;
        c.commit_max_ops = 1 << 30;
        c.upload_hold_ms = 60_000;
        c
    });
    let dir = core.create(U, ROOT_INO, b"d", nn(Kind::Dir)).unwrap();
    let mut inos = vec![];
    for i in 0..60 {
        let body = format!("content of file {i} ").repeat(i + 1);
        let n = write_file(&core, dir.ino, &format!("f{i}"), body.as_bytes());
        inos.push((i, n.ino, body));
    }
    core.rename(U, dir.ino, b"f3", ROOT_INO, b"moved3", RenameMode::Replace).unwrap();
    core.unlink(U, dir.ino, b"f4").unwrap();
    core.setxattr(U, inos[5].1, b"user.k", b"v", XattrFlags::Any).unwrap();
    // the instant of the "crash": no flush, no wait for the pipeline
    let b = d.path().join("b");
    copy_dir(&a, &b);
    let (c2, core2) = open_client(&b, |c| c);
    let d2 = core2.lookup(ROOT_INO, b"d").unwrap();
    assert_eq!(d2.ino, dir.ino);
    for (i, ino, body) in &inos {
        let name = format!("f{i}");
        let found = match i {
            3 => core2.lookup(ROOT_INO, b"moved3"),
            4 => {
                assert_eq!(core2.lookup(d2.ino, name.as_bytes()).unwrap_err(), Errno::ENOENT);
                continue;
            }
            _ => core2.lookup(d2.ino, name.as_bytes()),
        }
        .unwrap_or_else(|e| panic!("{name} lost: {e:?}"));
        assert_eq!(found.ino, *ino);
        assert_eq!(read_file(&core2, found.ino), body.as_bytes(), "content of {name}");
    }
    assert_eq!(core2.getxattr(inos[5].1, b"user.k").unwrap(), b"v");
    assert!(c2.wait_pipeline(std::time::Duration::from_secs(20)));
    drop(c2);
    drop(c);
}

/// A torn tail (partial last record) loses only that record.
#[test]
fn torn_op_log_tail_is_discarded() {
    let d = tempfile::tempdir().unwrap();
    let a = d.path().join("a");
    {
        let (c, core) = open_client(&a, |mut c| {
            c.commit_interval_ms = 600_000;
            c
        });
        for i in 0..20 {
            core.create(U, ROOT_INO, format!("n{i}").as_bytes(), nn(Kind::File)).unwrap();
        }
        let b = d.path().join("b");
        copy_dir(&a, &b);
        drop(c);
    }
    let b = d.path().join("b");
    let log = std::fs::read(b.join("oplog")).unwrap();
    // cut the log in the middle of its last record
    std::fs::write(b.join("oplog"), &log[..log.len() - 7]).unwrap();
    let (_c, core) = open_client(&b, |c| c);
    let names: Vec<_> = core.readdir(ROOT_INO).unwrap().entries.iter().map(|e| String::from_utf8_lossy(&e.name).into_owned()).collect();
    assert!(names.len() >= 18 && names.len() <= 20, "consistent prefix, got {}", names.len());
    for i in 0..names.len() {
        assert!(names.contains(&format!("n{i}")), "prefix property: n{i} missing from {names:?}");
    }
}
