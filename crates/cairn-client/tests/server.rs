//! Integration tests against the in-process server.

use cairn_client::api::*;
use cairn_client::testing::DirectApplier;
use cairn_client::{Client, Config, RootConfig};
use cairn_proto::*;
use cairn_server::testing::TestServer;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

const U: Origin = Origin::Local { uid: 1000, gid: 1000, pid: 1 };

fn nn(kind: Kind) -> NewNode {
    NewNode { kind, mode: 0o644, uid: 1000, gid: 1000, rdev: 0, symlink_target: None, xattrs: vec![] }
}

struct Env {
    srv: TestServer,
    dir: tempfile::TempDir,
}

fn env() -> Env {
    env_with(|_| {})
}

fn env_with(tweak: impl FnOnce(&mut cairn_server::server::Config)) -> Env {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start_with(dir.path().join("srv"), tweak);
    srv.add_host("a");
    srv.add_host("b");
    srv.add_root("proj", &[("a", true), ("b", true)]);
    Env { srv, dir }
}

fn client(e: &Env, host: &str) -> (Client, Arc<dyn Core>) {
    let mut cfg = Config::new(&e.srv.addr().to_string(), &e.srv.creds(host), &e.dir.path().join(format!("cache-{host}")));
    cfg = cfg.with_root("proj", RootConfig::default());
    let c = Client::open(cfg).unwrap();
    let core = c.core("proj").unwrap();
    DirectApplier::install(&core);
    (c, core)
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(15);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_connected(c: &Client) {
    wait_for("connected", || c.root("proj").unwrap().online.load(std::sync::atomic::Ordering::SeqCst));
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

/// path -> (kind, size, content) for the whole visible tree.
fn view(core: &Arc<dyn Core>) -> BTreeMap<String, (Kind, u64, Option<ContentId>)> {
    fn walk(core: &Arc<dyn Core>, ino: Ino, prefix: &str, out: &mut BTreeMap<String, (Kind, u64, Option<ContentId>)>) {
        for e in core.readdir(ino).unwrap().entries.iter() {
            let p = format!("{prefix}/{}", String::from_utf8_lossy(&e.name));
            let a = core.getattr(e.ino).unwrap().attrs;
            out.insert(p.clone(), (a.kind, if a.kind == Kind::File { a.size } else { 0 }, a.content));
            if e.kind == Kind::Dir {
                walk(core, e.ino, &p, out);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(core, ROOT_INO, "", &mut out);
    out
}

/// Same shape from the server's snapshot.
fn server_view(srv: &TestServer) -> BTreeMap<String, (Kind, u64, Option<ContentId>)> {
    let rows = srv.snapshot("proj");
    let mut paths: BTreeMap<NodeId, String> = BTreeMap::new();
    paths.insert(NodeId::ROOT, String::new());
    let mut out = BTreeMap::new();
    let mut left: Vec<&SnapshotRow> = rows.iter().collect();
    while !left.is_empty() {
        let before = left.len();
        left.retain(|r| match paths.get(&r.parent).cloned() {
            Some(pp) => {
                let p = format!("{pp}/{}", String::from_utf8_lossy(&r.name.0));
                paths.insert(r.node, p.clone());
                out.insert(p, (r.attrs.kind, if r.attrs.kind == Kind::File { r.attrs.size } else { 0 }, r.attrs.content));
                false
            }
            None => true,
        });
        assert!(left.len() < before, "orphan rows in server snapshot");
    }
    out
}

fn settle(clients: &[&Client]) {
    for c in clients {
        assert!(c.wait_synced(Duration::from_secs(15)), "client did not drain its journal");
    }
}

fn converge(e: &Env, clients: &[(&Client, &Arc<dyn Core>)]) {
    let cs: Vec<&Client> = clients.iter().map(|c| c.0).collect();
    settle(&cs);
    let head = e.srv.head("proj");
    for (c, core) in clients {
        wait_for("applied_seq reaches head", || c.root("proj").unwrap().applied_seq() >= head);
        c.settle();
        let _ = core;
    }
    // views must agree with the server (retry briefly: corrections run asynchronously)
    let sv = server_view(&e.srv);
    for (i, (c, core)) in clients.iter().enumerate() {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            c.settle();
            let v = view(core);
            if v == sv {
                break;
            }
            assert!(Instant::now() < end, "client {i} view differs from server:\nclient: {v:#?}\nserver: {sv:#?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn basic_sync_two_clients() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    let d = a.create(U, ROOT_INO, b"dir", nn(Kind::Dir)).unwrap();
    let f = write_file(&a, d.ino, "hello.txt", b"hello from a");
    let big: Vec<u8> = (0..2_500_000u32).map(|i| (i.wrapping_mul(2654435761) >> 11) as u8).collect();
    write_file(&a, ROOT_INO, "big.bin", &big);
    settle(&[&ca]);
    assert_eq!(f.attrs.size, 0);
    // B lists lazily and fetches content on demand
    let bd = b.lookup(ROOT_INO, b"dir").unwrap();
    let bf = b.lookup(bd.ino, b"hello.txt").unwrap();
    assert_eq!(bf.attrs.size, 12);
    assert_eq!(read_file(&b, bf.ino), b"hello from a");
    let bb = b.lookup(ROOT_INO, b"big.bin").unwrap();
    assert_eq!(read_file(&b, bb.ino), big);
    // live change stream: A adds a file, B (dir already listed) sees it without re-listing
    write_file(&a, d.ino, "later.txt", b"later");
    settle(&[&ca]);
    wait_for("B sees later.txt", || b.lookup(bd.ino, b"later.txt").is_ok());
    // rename and delete propagate
    a.rename(U, d.ino, b"later.txt", ROOT_INO, b"moved.txt", RenameMode::Replace).unwrap();
    a.unlink(U, d.ino, b"hello.txt").unwrap();
    settle(&[&ca]);
    wait_for("B sees rename+delete", || b.lookup(ROOT_INO, b"moved.txt").is_ok() && b.lookup(bd.ino, b"hello.txt").is_err());
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    assert!(e.srv.conflicts("proj").is_empty());
}

#[test]
fn offline_queue_replays_on_reconnect() {
    let e = env();
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    a.readdir(ROOT_INO).unwrap(); // list the root while online
    let d = a.create(U, ROOT_INO, b"seed", nn(Kind::Dir)).unwrap();
    settle(&[&ca]);
    ca.pause();
    wait_for("offline", || !ca.is_connected());
    // everything below queues
    let d2 = a.create(U, ROOT_INO, b"offline-dir", nn(Kind::Dir)).unwrap();
    write_file(&a, d2.ino, "x", b"written offline");
    write_file(&a, d.ino, "y", b"also offline");
    a.rename(U, d.ino, b"y", d2.ino, b"y2", RenameMode::Replace).unwrap();
    let z = write_file(&a, ROOT_INO, "z", b"to be deleted");
    a.unlink(U, ROOT_INO, b"z").unwrap();
    let _ = z;
    // folded: z is created and deleted offline (never sent), y is created at its final name
    assert_eq!(ca.root("proj").unwrap().pending_len(), 3);
    assert_eq!(read_file(&a, a.lookup(d2.ino, b"x").unwrap().ino), b"written offline");
    ca.resume();
    wait_connected(&ca);
    converge(&e, &[(&ca, &a)]);
    let sv = server_view(&e.srv);
    assert!(sv.contains_key("/offline-dir/x") && sv.contains_key("/offline-dir/y2") && !sv.contains_key("/z"));
    assert!(e.srv.conflicts("proj").is_empty(), "{:?}", e.srv.conflicts("proj"));
    // a second client reads the offline content
    let (cb, b) = client(&e, "b");
    wait_connected(&cb);
    let od = b.lookup(ROOT_INO, b"offline-dir").unwrap();
    let x = b.lookup(od.ino, b"x").unwrap();
    assert_eq!(read_file(&b, x.ino), b"written offline");
}

#[test]
fn offline_folding_temp_files_and_atomic_saves() {
    let e = env();
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    a.readdir(ROOT_INO).unwrap();
    let d = a.create(U, ROOT_INO, b"fold", nn(Kind::Dir)).unwrap();
    write_file(&a, d.ino, "doc", b"base");
    settle(&[&ca]);
    ca.pause();
    wait_for("offline", || !ca.is_connected());
    let pend = || ca.root("proj").unwrap().pending_len();
    for i in 0..20 {
        write_file(&a, d.ino, &format!(".tmp{i}"), b"x");
        a.unlink(U, d.ino, format!(".tmp{i}").as_bytes()).unwrap();
    }
    let bd = a.create(U, d.ino, b"bd", nn(Kind::Dir)).unwrap();
    write_file(&a, bd.ino, "y", b"x");
    a.unlink(U, bd.ino, b"y").unwrap();
    a.rmdir(U, d.ino, b"bd").unwrap();
    assert_eq!(pend(), 0, "temp files and dirs fold away");
    for i in 0..5 {
        write_file(&a, d.ino, ".doc.tmp", format!("save{i}").as_bytes());
        a.rename(U, d.ino, b".doc.tmp", d.ino, b"doc", RenameMode::Replace).unwrap();
    }
    // the server knows `doc`; the first save replaces it, later saves replace local-only nodes
    assert!(pend() <= 3, "atomic saves fold, pending {}", pend());
    ca.resume();
    wait_connected(&ca);
    settle(&[&ca]);
    let sv = server_view(&e.srv);
    assert!(sv.contains_key("/fold/doc") && !sv.contains_key("/fold/.doc.tmp"));
}

#[test]
fn concurrent_creates_conflict_copy_and_converge() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    a.readdir(ROOT_INO).unwrap();
    b.readdir(ROOT_INO).unwrap();
    ca.pause();
    cb.pause();
    wait_for("both offline", || !ca.is_connected() && !cb.is_connected());
    write_file(&a, ROOT_INO, "same", b"content from a");
    write_file(&b, ROOT_INO, "same", b"content from b, longer");
    write_file(&a, ROOT_INO, "only-a", b"a");
    // identical files from both hosts merge (Adopted)
    write_file(&a, ROOT_INO, "twin", b"identical");
    write_file(&b, ROOT_INO, "twin", b"identical");
    ca.resume();
    wait_connected(&ca);
    settle(&[&ca]);
    cb.resume();
    wait_connected(&cb);
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    let sv = server_view(&e.srv);
    let names: Vec<_> = sv.keys().cloned().collect();
    assert!(names.iter().any(|n| n.starts_with("/same.conflict-")), "{names:?}");
    assert_eq!(names.iter().filter(|n| n.starts_with("/twin")).count(), 1, "{names:?}");
    // both contents survive and read back correctly on both clients
    for core in [&a, &b] {
        let mut got: Vec<Vec<u8>> = vec![];
        for n in core.readdir(ROOT_INO).unwrap().entries.iter().filter(|n| n.name.starts_with(b"same")) {
            got.push(read_file(core, n.ino));
        }
        got.sort();
        assert_eq!(got, vec![b"content from a".to_vec(), b"content from b, longer".to_vec()]);
    }
}

#[test]
fn rename_and_delete_conflicts_converge() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    for n in ["f1", "f2", "f3"] {
        write_file(&a, ROOT_INO, n, n.as_bytes());
    }
    a.create(U, ROOT_INO, b"d1", nn(Kind::Dir)).unwrap();
    a.create(U, ROOT_INO, b"d2", nn(Kind::Dir)).unwrap();
    settle(&[&ca]);
    b.readdir(ROOT_INO).unwrap();
    wait_for("b sees all", || b.lookup(ROOT_INO, b"d2").is_ok() && b.lookup(ROOT_INO, b"f3").is_ok());
    // hydrate f2 on B: a cold file cannot be opened offline
    read_file(&b, b.lookup(ROOT_INO, b"f2").unwrap().ino);
    b.readdir(b.lookup(ROOT_INO, b"d1").unwrap().ino).unwrap();
    b.readdir(b.lookup(ROOT_INO, b"d2").unwrap().ino).unwrap();
    ca.pause();
    cb.pause();
    wait_for("both offline", || !ca.is_connected() && !cb.is_connected());
    // same source renamed to different places
    a.rename(U, ROOT_INO, b"f1", a.lookup(ROOT_INO, b"d1").unwrap().ino, b"f1", RenameMode::Replace).unwrap();
    b.rename(U, ROOT_INO, b"f1", b.lookup(ROOT_INO, b"d2").unwrap().ino, b"f1", RenameMode::Replace).unwrap();
    // delete vs modify
    a.unlink(U, ROOT_INO, b"f2").unwrap();
    {
        let n = b.lookup(ROOT_INO, b"f2").unwrap();
        let mut of = b.open(U, n.ino, true, false).unwrap();
        of.file.write_all(b"modified by b").unwrap();
        b.release(n.ino, of.token).unwrap();
    }
    // attrs on different nodes never conflict
    b.setattr(U, b.lookup(ROOT_INO, b"f3").unwrap().ino, AttrChanges { mode: Some(0o600), ..Default::default() }, None).unwrap();
    ca.resume();
    wait_connected(&ca);
    settle(&[&ca]);
    cb.resume();
    wait_connected(&cb);
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    let sv = server_view(&e.srv);
    // exactly one place holds f1; f2 survives (modify wins)
    let f1s = sv.keys().filter(|k| k.ends_with("/f1")).count();
    assert_eq!(f1s, 1, "{sv:?}");
    assert!(sv.contains_key("/f2"), "modify must win over delete: {:?}", sv.keys().collect::<Vec<_>>());
    let f2 = b.lookup(ROOT_INO, b"f2").unwrap();
    assert_eq!(read_file(&b, f2.ino), b"modified by b");
    assert_eq!(a.getattr(a.lookup(ROOT_INO, b"f3").unwrap().ino).unwrap().attrs.mode, 0o600);
}

#[test]
fn write_lease_recall() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    let f = write_file(&a, ROOT_INO, "shared", b"v0");
    settle(&[&ca]);
    let bf = {
        b.readdir(ROOT_INO).unwrap();
        wait_for("b sees shared", || b.lookup(ROOT_INO, b"shared").is_ok());
        b.lookup(ROOT_INO, b"shared").unwrap()
    };
    // A opens for write and keeps the fd open, holding the lease
    let mut oa = a.open(U, f.ino, true, true).unwrap();
    oa.file.write_all(b"written by a, fd still open").unwrap();
    // B wants to write: the server recalls A, A publishes, B proceeds
    let mut ob = b.open(U, bf.ino, true, false).unwrap();
    // B now sees A's published content
    let mut s = String::new();
    ob.file.seek(SeekFrom::Start(0)).unwrap();
    ob.file.read_to_string(&mut s).unwrap();
    assert_eq!(s, "written by a, fd still open");
    ob.file.set_len(0).unwrap();
    ob.file.write_all(b"b wins later").unwrap();
    b.release(bf.ino, ob.token).unwrap();
    a.release(f.ino, oa.token).unwrap(); // A's last close re-acquires and commits against its base
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    assert!(e.srv.snapshot("proj").len() >= 1);
}

#[test]
fn reconnect_after_server_crash() {
    let mut e = env();
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    a.readdir(ROOT_INO).unwrap();
    write_file(&a, ROOT_INO, "before", b"1");
    settle(&[&ca]);
    e.srv.crash_restart();
    write_file(&a, ROOT_INO, "during-or-after", b"2");
    wait_connected(&ca);
    write_file(&a, ROOT_INO, "after", b"3");
    converge(&e, &[(&ca, &a)]);
    let sv = server_view(&e.srv);
    assert!(sv.contains_key("/before") && sv.contains_key("/during-or-after") && sv.contains_key("/after"));
}

#[test]
fn client_restart_resends_journal() {
    let e = env();
    {
        let (ca, a) = client(&e, "a");
        wait_connected(&ca);
        a.readdir(ROOT_INO).unwrap();
        ca.pause();
        wait_for("offline", || !ca.is_connected());
        a.create(U, ROOT_INO, b"dir", nn(Kind::Dir)).unwrap();
        write_file(&a, ROOT_INO, "queued", b"queued before restart");
        a.sync_all().unwrap();
        assert!(ca.root("proj").unwrap().pending_len() >= 2);
        drop(a);
        drop(ca);
    }
    assert!(e.srv.snapshot("proj").is_empty());
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    converge(&e, &[(&ca, &a)]);
    let sv = server_view(&e.srv);
    assert!(sv.contains_key("/dir") && sv.contains_key("/queued"));
    let q = a.lookup(ROOT_INO, b"queued").unwrap();
    assert_eq!(read_file(&a, q.ino), b"queued before restart");
}

#[test]
fn sync_on_close_waits_for_server() {
    let e = env();
    let mut cfg = Config::new(&e.srv.addr().to_string(), &e.srv.creds("a"), &e.dir.path().join("cache-a"));
    let rc: RootConfig = {
        let c = Config::from_toml("creds_dir=\"/x\"\ncache_dir=\"/y\"\n[root.proj]\n[[root.proj.policy]]\nmatch=[\"**/*.sync\"]\nmode=\"sync_on_close\"\n").unwrap();
        c.roots["proj"].clone()
    };
    cfg = cfg.with_root("proj", rc);
    let ca = Client::open(cfg).unwrap();
    let a = ca.core("proj").unwrap();
    DirectApplier::install(&a);
    wait_connected(&ca);
    let data = vec![7u8; 200_000];
    write_file(&a, ROOT_INO, "x.sync", &data);
    // release returned: the server already holds the bytes and the commit
    let cid = e.srv.live_content("proj");
    assert_eq!(cid.len(), 1, "live contents: {cid:?}");
    assert!(e.srv.has_content(cid[0]));
    assert_eq!(ca.root("proj").unwrap().pending_len(), 0);
}

#[test]
fn evicted_content_is_refetched() {
    let e = env();
    let mut cfg = Config::new(&e.srv.addr().to_string(), &e.srv.creds("a"), &e.dir.path().join("cache-a")).with_root("proj", RootConfig::default());
    cfg.idle_live_files = 0; // no idle live files: every open re-clones from the CAS
    let ca = Client::open(cfg).unwrap();
    let a = ca.core("proj").unwrap();
    DirectApplier::install(&a);
    wait_connected(&ca);
    let n = write_file(&a, ROOT_INO, "f", b"fetch me back");
    let big: Vec<u8> = (0..1_000_000u32).map(|i| (i.wrapping_mul(2654435761) >> 9) as u8).collect();
    let g = write_file(&a, ROOT_INO, "g", &big);
    settle(&[&ca]);
    assert!(ca.wait_synced(Duration::from_secs(10)));
    let before = ca.cache().resident().len();
    assert!(before >= 2);
    let freed = ca.cache().evict(u64::MAX, &|_| false);
    assert!(freed > 0 && ca.cache().resident().is_empty());
    assert_eq!(read_file(&a, n.ino), b"fetch me back");
    assert_eq!(read_file(&a, g.ino), big);
    assert_eq!(ca.cache().resident().len(), before, "resident after refetch");
}

#[test]
fn resync_after_log_pruned() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    a.create(U, ROOT_INO, b"keep", nn(Kind::Dir)).unwrap();
    write_file(&a, ROOT_INO, "gone", b"x");
    write_file(&a, ROOT_INO, "stays", b"y");
    settle(&[&ca]);
    b.readdir(ROOT_INO).unwrap();
    wait_for("b sees all", || b.lookup(ROOT_INO, b"stays").is_ok());
    let keep = b.lookup(ROOT_INO, b"keep").unwrap();
    b.readdir(keep.ino).unwrap();
    cb.pause();
    wait_for("b offline", || !cb.is_connected());
    // A changes things while B is away; then the server forgets its log
    a.unlink(U, ROOT_INO, b"gone").unwrap();
    let ak = a.lookup(ROOT_INO, b"keep").unwrap();
    write_file(&a, ak.ino, "new-in-keep", b"z");
    a.create(U, ROOT_INO, b"newdir", nn(Kind::Dir)).unwrap();
    a.rename(U, ROOT_INO, b"stays", ak.ino, b"stays-moved", RenameMode::Replace).unwrap();
    settle(&[&ca]);
    e.srv.truncate_log("proj", 0);
    cb.resume();
    wait_connected(&cb);
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    assert_eq!(cb.root("proj").unwrap().resync_count(), 1, "B must have gone through resync");
    assert!(b.lookup(ROOT_INO, b"gone").is_err());
    assert!(b.lookup(keep.ino, b"stays-moved").is_ok());
}

#[test]
fn resync_after_epoch_change() {
    let mut e = env();
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    a.create(U, ROOT_INO, b"d", nn(Kind::Dir)).unwrap();
    write_file(&a, ROOT_INO, "f", b"1");
    settle(&[&ca]);
    a.readdir(ROOT_INO).unwrap();
    let d = a.lookup(ROOT_INO, b"d").unwrap();
    ca.pause();
    wait_for("offline", || !ca.is_connected());
    // a local op queued while the server is "restored from backup"
    write_file(&a, d.ino, "queued", b"q");
    let ep = e.srv.bump_epoch();
    e.srv.restart();
    ca.resume();
    wait_connected(&ca);
    converge(&e, &[(&ca, &a)]);
    assert_eq!(ca.root("proj").unwrap().resync_count(), 1);
    assert_eq!(ca.root("proj").unwrap().epoch_and_applied().0, Some(ep));
    assert!(server_view(&e.srv).contains_key("/d/queued"));
}

#[test]
fn delete_lost_restores_dir_with_concurrent_children() {
    let e = env();
    let (c0, a) = client(&e, "a");
    let (c2, b) = client(&e, "b");
    wait_connected(&c0);
    wait_connected(&c2);
    let d0 = a.create(U, ROOT_INO, b"d0", nn(Kind::Dir)).unwrap();
    let s = a.create(U, d0.ino, b"s", nn(Kind::Dir)).unwrap();
    settle(&[&c0]);
    let bd0 = b.lookup(ROOT_INO, b"d0").unwrap();
    b.readdir(bd0.ino).unwrap();
    let bs = b.lookup(bd0.ino, b"s").unwrap();
    b.readdir(bs.ino).unwrap();
    let _ = s;
    c0.pause();
    c2.pause();
    wait_for("offline", || !c0.is_connected() && !c2.is_connected());
    b.rmdir(U, bd0.ino, b"s").unwrap();
    let a_s = a.lookup(d0.ino, b"s").unwrap();
    a.create(U, a_s.ino, b"c", nn(Kind::File)).unwrap();
    c0.resume();
    wait_connected(&c0);
    settle(&[&c0]);
    c2.resume();
    wait_connected(&c2);
    converge(&e, &[(&c0, &a), (&c2, &b)]);
    assert!(server_view(&e.srv).contains_key("/d0/s/c"));
}

#[test]
fn exchange_of_directories_keeps_children() {
    let e = env();
    let (c0, a) = client(&e, "a");
    let (c1, b) = client(&e, "b");
    wait_connected(&c0);
    wait_connected(&c1);
    a.create(U, ROOT_INO, b"d0", nn(Kind::Dir)).unwrap();
    let d1 = a.create(U, ROOT_INO, b"d1", nn(Kind::Dir)).unwrap();
    let d0 = a.lookup(ROOT_INO, b"d0").unwrap();
    a.create(U, d0.ino, b"s", nn(Kind::Dir)).unwrap();
    settle(&[&c0]);
    let bd0 = b.lookup(ROOT_INO, b"d0").unwrap();
    b.readdir(bd0.ino).unwrap();
    let bd1 = b.lookup(ROOT_INO, b"d1").unwrap();
    b.readdir(bd1.ino).unwrap();
    write_file(&b, bd1.ino, "a", b"x");
    settle(&[&c1]);
    wait_for("a sees d1/a", || a.lookup(d1.ino, b"a").is_ok());
    a.rename(U, ROOT_INO, b"d0", ROOT_INO, b"d1", RenameMode::Exchange).unwrap();
    converge(&e, &[(&c0, &a), (&c1, &b)]);
    let sv = server_view(&e.srv);
    assert!(sv.contains_key("/d0/a") && sv.contains_key("/d1/s"), "{:?}", sv.keys().collect::<Vec<_>>());
}

#[test]
fn bulk_create_is_not_round_trip_bound() {
    let e = env();
    let (ca, a) = client(&e, "a");
    wait_connected(&ca);
    let t = Instant::now();
    let d = a.create(U, ROOT_INO, b"repo", nn(Kind::Dir)).unwrap();
    for i in 0..1500 {
        let sub = if i % 50 == 0 { Some(a.create(U, d.ino, format!("dir{i}").as_bytes(), nn(Kind::Dir)).unwrap()) } else { None };
        let p = sub.map(|s| s.ino).unwrap_or(d.ino);
        write_file(&a, p, &format!("f{i}"), format!("file {i}").as_bytes());
    }
    let local = t.elapsed();
    settle(&[&ca]);
    eprintln!("1500 files: local {local:?}, synced {:?}", t.elapsed());
    assert!(t.elapsed() < Duration::from_secs(20), "too slow: {:?}", t.elapsed());
    converge(&e, &[(&ca, &a)]);
}

fn env3() -> Env {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start_with(dir.path().join("srv"), |_| {});
    for h in ["a", "b", "c"] {
        srv.add_host(h);
    }
    srv.add_root("proj", &[("a", true), ("b", true), ("c", true)]);
    Env { srv, dir }
}

fn offline(cs: &[&Client]) {
    for c in cs {
        c.pause();
    }
    wait_for("offline", || cs.iter().all(|c| !c.is_connected()));
}

fn online(c: &Client) {
    c.resume();
    wait_connected(c);
    settle(&[c]);
}

#[test]
fn rename_over_symlink_races_rename_away() {
    let e = env3();
    let (c0, a) = client(&e, "a");
    let (c1, b) = client(&e, "b");
    let (c2, c) = client(&e, "c");
    for x in [&c0, &c1, &c2] {
        wait_connected(x);
    }
    a.create(U, ROOT_INO, b"d0", nn(Kind::Dir)).unwrap();
    a.create(U, ROOT_INO, b"d1", nn(Kind::Dir)).unwrap();
    settle(&[&c0]);
    for core in [&a, &b, &c] {
        core.readdir(ROOT_INO).unwrap();
        let d = core.lookup(ROOT_INO, b"d1").unwrap();
        core.readdir(d.ino).unwrap();
    }
    let b_d1 = b.lookup(ROOT_INO, b"d1").unwrap();
    b.create(U, b_d1.ino, b"c", nn(Kind::File)).unwrap();
    settle(&[&c1]);
    let mut s = nn(Kind::Symlink);
    s.symlink_target = Some(b"t1".to_vec());
    b.create(U, ROOT_INO, b"b", s).unwrap();
    settle(&[&c1]);
    let c_d1 = c.lookup(ROOT_INO, b"d1").unwrap();
    wait_for("c sees d1/c and b", || c.lookup(c_d1.ino, b"c").is_ok() && c.lookup(ROOT_INO, b"b").is_ok());
    offline(&[&c0, &c1, &c2]);
    b.rename(U, ROOT_INO, b"b", b_d1.ino, b"d", RenameMode::Replace).unwrap();
    c.rename(U, c_d1.ino, b"c", ROOT_INO, b"b", RenameMode::Replace).unwrap();
    online(&c0);
    online(&c2);
    online(&c1);
    converge(&e, &[(&c0, &a), (&c1, &b), (&c2, &c)]);
}

#[test]
fn restart_after_adopted_create_keeps_inos_consistent() {
    let e = env();
    let (ca, a) = client(&e, "a");
    let (cb, b) = client(&e, "b");
    wait_connected(&ca);
    wait_connected(&cb);
    a.readdir(ROOT_INO).unwrap();
    b.readdir(ROOT_INO).unwrap();
    offline(&[&ca, &cb]);
    a.create(U, ROOT_INO, b"c", nn(Kind::File)).unwrap();
    b.create(U, ROOT_INO, b"c", nn(Kind::File)).unwrap();
    online(&ca);
    online(&cb);
    converge(&e, &[(&ca, &a), (&cb, &b)]);
    drop(b);
    drop(cb);
    let (cb, b) = client(&e, "b");
    wait_connected(&cb);
    converge(&e, &[(&ca, &a), (&cb, &b)]);
}
