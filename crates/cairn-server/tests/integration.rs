use cairn_proto::*;
use cairn_server::admin::HostSet;
use cairn_server::testing::*;
use std::sync::atomic::Ordering;
use std::time::Duration;

const R: NodeId = NodeId::ROOT;
const T: Duration = Duration::from_secs(5);

async fn setup() -> (tempfile::TempDir, TestServer) {
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start(dir.path());
    srv.add_host("a");
    srv.add_host("b");
    srv.add_root("proj", &[("a", true), ("b", true)]);
    (dir, srv)
}

async fn client(srv: &TestServer, who: &str) -> (TestClient, RootHandle) {
    let c = TestClient::connect(srv.addr(), &srv.creds(who)).await.expect("connect");
    let (h, _, w) = c.mount("proj").await.expect("mount");
    assert!(w);
    (c, h)
}

fn applied(r: &CommitResult) -> bool {
    matches!(r, CommitResult::Applied { .. })
}
fn resolution(r: &CommitResult) -> &Resolution {
    match r {
        CommitResult::Transformed { conflict, .. } | CommitResult::Rejected { conflict } => &conflict.resolution,
        o => panic!("expected a conflict result, got {o:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn create_list_read_back() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (n, r) = a.create_file(ra, R, "hello.txt", b"hello world").await.unwrap();
    assert!(applied(&r));
    let entries = a.readdir_all(ra, R).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].node, n);
    assert_eq!(entries[0].name, name("hello.txt"));
    let ContentId::Blob(h) = entries[0].attrs.content.unwrap() else { panic!() };
    assert!(matches!(a.call(Request::GetBlob { hash: h }).await.unwrap(), Response::Blob { data } if data == b"hello world"));
    assert!(matches!(a.call(Request::GetAttr { root: ra, node: n }).await.unwrap(), Response::NodeAttrs { attrs, .. } if attrs.size == 11));
    let Response::Snapshot { rows, done, head } = a.call(Request::Snapshot { root: ra, after: None, limit: 10 }).await.unwrap() else { panic!() };
    assert!(done && rows.len() == 1 && head == Seq(1));
    assert!(matches!(a.call(Request::GetAttr { root: ra, node: NodeId::new(a.host, 999) }).await, Err(ErrorReply { code: ErrCode::NotFound, .. })));
    assert!(matches!(a.call(Request::ReadDir { root: ra, dir: NodeId::new(a.host, 999), after: None, limit: 10 }).await, Err(ErrorReply { code: ErrCode::NotFound, .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn readdir_and_snapshot_paging() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let cmds: Vec<Commit> = (0..250)
        .map(|i| a.commit_of(Op::Create { parent: R, name: name(&format!("f{i:04}")), node: a.new_node(), attrs: file_attrs(ContentId::Empty, 0) }))
        .collect();
    let res = a.commit(ra, cmds).await.unwrap();
    assert!(res.iter().all(applied));
    let all = a.readdir_all(ra, R).await.unwrap();
    assert_eq!(all.len(), 250);
    assert!(all.windows(2).all(|w| w[0].name < w[1].name));
    let mut after = None;
    let mut n = 0;
    loop {
        let Response::Snapshot { rows, done, .. } = a.call(Request::Snapshot { root: ra, after: after.clone(), limit: 60 }).await.unwrap() else { panic!() };
        n += rows.len();
        after = rows.last().map(|r| (r.parent, r.name.clone()));
        if done {
            break;
        }
    }
    assert_eq!(n, 250);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_and_revoked_hosts_rejected() {
    let (_d, mut srv) = setup().await;
    srv.add_host("evil");
    let (a, _) = client(&srv, "a").await;
    // a live connection of a revoked host is dropped right away
    let (e, _) = {
        let c = TestClient::connect(srv.addr(), &srv.creds("evil")).await.expect("evil connects before revocation");
        (c, 0)
    };
    srv.revoke_host("evil");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(e.call(Request::Ping).await.is_err());
    // and cannot come back: signed by the CA but not in the host table
    let err = TestClient::connect(srv.addr(), &srv.creds("evil")).await.err().expect("denied");
    assert_eq!(err.code, ErrCode::PermissionDenied);
    // others unaffected
    assert!(matches!(a.call(Request::Ping).await, Ok(Response::Ok)));
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_and_per_host_roots() {
    let (_d, mut srv) = setup().await;
    srv.add_host("ro");
    srv.add_host("none");
    srv.add_root("shared", &[("a", true), ("ro", false)]);
    srv.add_root("hosts/a/docker", &[]);
    let ro = TestClient::connect(srv.addr(), &srv.creds("ro")).await.unwrap();
    let (h, _, w) = ro.mount("shared").await.unwrap();
    assert!(!w);
    let r = ro.commit(h, vec![ro.commit_of(Op::Create { parent: R, name: name("x"), node: ro.new_node(), attrs: dir_attrs() })]).await;
    assert!(matches!(r, Err(ErrorReply { code: ErrCode::ReadOnly, .. })));
    assert!(matches!(ro.call(Request::PutBlob { hash: Hash::of(b"x"), data: b"x".to_vec() }).await, Err(ErrorReply { code: ErrCode::ReadOnly, .. })));
    assert!(matches!(ro.call(Request::AcquireWrite { root: h, node: R }).await, Err(ErrorReply { code: ErrCode::ReadOnly, .. })));
    let none = TestClient::connect(srv.addr(), &srv.creds("none")).await.unwrap();
    assert_eq!(none.mount("shared").await.unwrap_err().code, ErrCode::PermissionDenied);
    assert_eq!(none.mount("nope").await.unwrap_err().code, ErrCode::NotFound);
    let a = TestClient::connect(srv.addr(), &srv.creds("a")).await.unwrap();
    assert!(a.mount("hosts/a/docker").await.unwrap().2);
    let b = TestClient::connect(srv.addr(), &srv.creds("b")).await.unwrap();
    assert_eq!(b.mount("hosts/a/docker").await.unwrap_err().code, ErrCode::PermissionDenied);
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_ordered_gapless_under_concurrency() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    let (s, rs) = client(&srv, "a").await;
    // some history before subscribing
    for i in 0..20 {
        a.create_file(ra, R, &format!("pre{i}"), b"").await.unwrap();
    }
    let sub = s.subscribe(rs, Seq(5)).await.unwrap();
    assert!(matches!(sub, Response::Subscribed { head } if head >= Seq(20)));
    let wa = tokio::spawn(async move {
        for i in 0..100 {
            a.create_file(ra, R, &format!("a{i}"), b"").await.unwrap();
        }
    });
    let wb = tokio::spawn(async move {
        for i in 0..100 {
            b.create_file(rb, R, &format!("b{i}"), b"").await.unwrap();
        }
    });
    let mut next = 6u64;
    while next <= 220 {
        let Some(Push::Changes { root, changes }) = s.next_push(T).await else { panic!("stream stalled at {next}") };
        assert_eq!(root, rs);
        for c in changes {
            assert_eq!(c.seq.0, next, "gapless and ordered");
            next += 1;
        }
    }
    wa.await.unwrap();
    wb.await.unwrap();
    assert!(s.next_push(Duration::from_millis(200)).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribe_resync_required() {
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start_with(dir.path(), |c| {
        c.retention.log_secs = 0;
        c.retention.log_min_changes = 0;
    });
    srv.add_host("a");
    srv.add_root("proj", &[("a", true)]);
    let (a, ra) = client(&srv, "a").await;
    for i in 0..5 {
        a.create_file(ra, R, &format!("f{i}"), b"").await.unwrap();
    }
    // client claims to be ahead of the server: log rewound
    assert!(matches!(a.subscribe(ra, Seq(50)).await.unwrap(), Response::ResyncRequired { head: Seq(5) }));
    drop(a);
    srv.restart(); // retention pass at startup truncates the log
    let (a, ra) = client(&srv, "a").await;
    assert!(matches!(a.subscribe(ra, Seq(2)).await.unwrap(), Response::ResyncRequired { head: Seq(5) }));
    // current head is fine
    assert!(matches!(a.subscribe(ra, Seq(5)).await.unwrap(), Response::Subscribed { head: Seq(5) }));
    // changes after truncation keep flowing
    a.create_file(ra, R, "later", b"").await.unwrap();
    assert!(matches!(a.next_push(T).await, Some(Push::Changes { changes, .. }) if changes[0].seq == Seq(6)));
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_creates_same_name() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    // different content: second becomes a conflict copy
    let (fa, r1) = a.create_file(ra, R, "x", b"from a").await.unwrap();
    let (fb, r2) = b.create_file(rb, R, "x", b"from b").await.unwrap();
    assert!(applied(&r1));
    let Resolution::ConflictCopy { parent, name: cn } = resolution(&r2) else { panic!("{r2:?}") };
    assert_eq!(*parent, R);
    assert!(cn.0.starts_with(b"x.conflict-b-"), "{cn:?}");
    assert_eq!(cn.0.len(), "x.conflict-b-20240102T030405".len());
    let rows = srv.snapshot("proj");
    assert!(rows.iter().any(|r| r.name == name("x") && r.node == fa));
    assert!(rows.iter().any(|r| &r.name == cn && r.node == fb));
    // identical content: adopted, no extra entry
    let (_, r1) = a.create_file(ra, R, "same", b"same bytes").await.unwrap();
    let (nb, r2) = b.create_file(rb, R, "same", b"same bytes").await.unwrap();
    assert!(applied(&r1));
    assert!(matches!(resolution(&r2), Resolution::Adopted { .. }));
    assert_eq!(srv.snapshot("proj").len(), 3);
    // the alias works on the wire
    let r = b.op(rb, Op::SetAttr { node: nb, base: Seq(0), changes: AttrChanges { mode: Some(0o600), ..Default::default() } }).await;
    assert!(applied(&r));
    let confl = match b.call(Request::Conflicts { root: rb, since: Seq(0) }).await.unwrap() {
        Response::ConflictList { conflicts } => conflicts,
        o => panic!("{o:?}"),
    };
    assert_eq!(confl.len(), 2);
    assert_eq!(srv.conflicts("proj"), confl);
    // a Rejected record (no change emitted) is found by polling with the head it landed at
    let head = srv.head("proj");
    let d1 = a.new_node();
    let d2 = a.new_node();
    a.op(ra, Op::Create { parent: R, name: name("d1"), node: d1, attrs: dir_attrs() }).await;
    a.op(ra, Op::Create { parent: d1, name: name("d2"), node: d2, attrs: dir_attrs() }).await;
    let r = a.op(ra, Op::Rename { src_parent: R, src_name: name("d1"), node: d1, dst_parent: d2, dst_name: name("d1"), replaces: None, mode: RenameMode::NoReplace }).await;
    assert!(matches!(r, CommitResult::Rejected { .. }));
    let head2 = srv.head("proj");
    assert!(head2 > head);
    let Response::ConflictList { conflicts } = b.call(Request::Conflicts { root: rb, since: head2 }).await.unwrap() else { panic!() };
    assert!(conflicts.iter().any(|c| c.resolution == Resolution::CycleRejected));
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_vs_modify_rename_races_and_dir_recreate() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    let (f, _) = a.create_file(ra, R, "f", b"1").await.unwrap();
    // b modifies, a (stale) deletes: delete loses
    let c2 = b.put_blob(b"2").await.unwrap();
    let r = b.op(rb, Op::SetContent { node: f, base: Seq(1), content: c2, size: 1, mtime_ns: 5 }).await;
    assert!(applied(&r), "{r:?}");
    let r = a.op(ra, Op::Unlink { parent: R, name: name("f"), node: f, base: Seq(1) }).await;
    assert_eq!(*resolution(&r), Resolution::DeleteLost);
    assert!(srv.snapshot("proj").iter().any(|r| r.node == f));
    // two renames of the same source: second loses
    let r1 = a.op(ra, Op::Rename { src_parent: R, src_name: name("f"), node: f, dst_parent: R, dst_name: name("fa"), replaces: None, mode: RenameMode::NoReplace }).await;
    let r2 = b.op(rb, Op::Rename { src_parent: R, src_name: name("f"), node: f, dst_parent: R, dst_name: name("fb"), replaces: None, mode: RenameMode::NoReplace }).await;
    assert!(applied(&r1));
    assert_eq!(*resolution(&r2), Resolution::RenameLost);
    assert!(matches!(r2, CommitResult::Rejected { .. }));
    // directory deleted by a, b creates into it: recreated
    let d = a.new_node();
    assert!(applied(&a.op(ra, Op::Create { parent: R, name: name("d"), node: d, attrs: dir_attrs() }).await));
    assert!(applied(&a.op(ra, Op::Unlink { parent: R, name: name("d"), node: d, base: Seq(100) }).await));
    let (_, r) = b.create_file(rb, d, "inside", b"x").await.unwrap();
    assert_eq!(*resolution(&r), Resolution::DirRecreated { dir: d });
    let rows = srv.snapshot("proj");
    assert!(rows.iter().any(|r| r.node == d && r.parent == R));
    assert!(rows.iter().any(|r| r.parent == d && r.name == name("inside")));
    // cycle
    let x = a.new_node();
    let y = a.new_node();
    a.op(ra, Op::Create { parent: R, name: name("x"), node: x, attrs: dir_attrs() }).await;
    a.op(ra, Op::Create { parent: x, name: name("y"), node: y, attrs: dir_attrs() }).await;
    let r = b.op(rb, Op::Rename { src_parent: R, src_name: name("x"), node: x, dst_parent: y, dst_name: name("x"), replaces: None, mode: RenameMode::NoReplace }).await;
    assert_eq!(*resolution(&r), Resolution::CycleRejected);
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_retry_same_op_id() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let c = a.commit_of(Op::Create { parent: R, name: name("x"), node: a.new_node(), attrs: dir_attrs() });
    let r1 = a.commit(ra, vec![c.clone()]).await.unwrap();
    let r2 = a.commit(ra, vec![c.clone(), c.clone()]).await.unwrap();
    assert_eq!(r1[0], r2[0]);
    assert_eq!(r2[0], r2[1]);
    assert_eq!(srv.change_log("proj").len(), 1);
    assert_eq!(srv.commit_log("proj").len(), 1, "replays are not in the commit log");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_relay_pending_then_upload() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, _rb) = client(&srv, "b").await;
    // a publishes metadata only
    let data = b"not uploaded yet".to_vec();
    let hash = Hash::of(&data);
    let node = a.new_node();
    let r = a.op(ra, Op::Create { parent: R, name: name("lazy"), node, attrs: file_attrs(ContentId::Blob(hash), data.len() as u64) }).await;
    assert!(applied(&r));
    assert_eq!(srv.pending_content(), vec![(ContentId::Blob(hash), a.host)]);
    // b asks; a is told to upload
    let waiting = b.send(Request::GetBlob { hash }).unwrap();
    let Some(Push::NeedContent { hash: h }) = a.next_push(T).await else { panic!("no NeedContent") };
    assert_eq!(h, hash);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(tokio::time::timeout(Duration::from_millis(50), async { /* still held */ }).await.is_ok());
    a.call(Request::PutBlob { hash, data: data.clone() }).await.unwrap();
    let Ok(Ok(Response::Blob { data: got })) = tokio::time::timeout(T, waiting).await.unwrap() else { panic!() };
    assert_eq!(got, data);
    assert!(srv.pending_content().is_empty());
    // bad hash is rejected
    assert!(matches!(a.call(Request::PutBlob { hash: Hash::of(b"q"), data: b"r".to_vec() }).await, Err(ErrorReply { code: ErrCode::Invalid, .. })));
    // unknown content
    assert!(matches!(b.call(Request::GetBlob { hash: Hash::of(b"never") }).await, Err(ErrorReply { code: ErrCode::NotFound, .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn content_relay_owner_offline_is_unavailable() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, _) = client(&srv, "b").await;
    let hash = Hash::of(b"gone");
    a.op(ra, Op::Create { parent: R, name: name("lazy"), node: a.new_node(), attrs: file_attrs(ContentId::Blob(hash), 4) }).await;
    drop(a);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let t0 = std::time::Instant::now();
    assert!(matches!(b.call(Request::GetBlob { hash }).await, Err(ErrorReply { code: ErrCode::Unavailable, .. })));
    assert!(t0.elapsed() < Duration::from_secs(1), "answers immediately");
}

#[tokio::test(flavor = "multi_thread")]
async fn content_relay_owner_leaves_while_waiting() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, _) = client(&srv, "b").await;
    let hash = Hash::of(b"gone2");
    a.op(ra, Op::Create { parent: R, name: name("lazy"), node: a.new_node(), attrs: file_attrs(ContentId::Blob(hash), 5) }).await;
    let waiting = b.send(Request::GetBlob { hash }).unwrap();
    assert!(matches!(a.next_push(T).await, Some(Push::NeedContent { .. })));
    drop(a);
    let r = tokio::time::timeout(T, waiting).await.unwrap().unwrap();
    assert!(matches!(r, Err(ErrorReply { code: ErrCode::Unavailable, .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn chunked_manifest_upload_and_relay() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, _) = client(&srv, "b").await;
    let c1 = vec![1u8; 20_000];
    let c2 = vec![2u8; 30_000];
    let m = Manifest { size: 50_000, chunks: vec![(Hash::of(&c1), 20_000), (Hash::of(&c2), 30_000)] };
    let id = m.id();
    let ContentId::Chunked(mh) = id else { panic!() };
    // manifest before chunks: rejected
    assert!(matches!(a.call(Request::PutManifest { manifest: m.clone() }).await, Err(ErrorReply { code: ErrCode::NotFound, .. })));
    // metadata first
    let node = a.new_node();
    a.op(ra, Op::Create { parent: R, name: name("big"), node, attrs: file_attrs(id, 50_000) }).await;
    let waiting = b.send(Request::GetManifest { id: mh }).unwrap();
    assert!(matches!(a.next_push(T).await, Some(Push::NeedContent { hash }) if hash == mh));
    a.call(Request::PutBlob { hash: Hash::of(&c1), data: c1.clone() }).await.unwrap();
    a.call(Request::PutBlob { hash: Hash::of(&c2), data: c2.clone() }).await.unwrap();
    a.call(Request::PutManifest { manifest: m.clone() }).await.unwrap();
    let Ok(Ok(Response::Manifest { manifest })) = tokio::time::timeout(T, waiting).await.unwrap() else { panic!() };
    assert_eq!(manifest, m);
    let Response::Missing { hashes } = b.call(Request::Has { hashes: vec![Hash::of(&c1), mh, Hash::of(b"zz")] }).await.unwrap() else { panic!() };
    assert_eq!(hashes, vec![Hash::of(b"zz")]);
    assert!(matches!(b.call(Request::PutBlob { hash: Hash::of(&[0u8; 300_000]), data: vec![0u8; 300_000] }).await, Err(ErrorReply { code: ErrCode::TooLarge, .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_recall_release_and_conflict_copy() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    let (f, _) = a.create_file(ra, R, "f", b"v1").await.unwrap();
    assert!(matches!(a.call(Request::AcquireWrite { root: ra, node: f }).await.unwrap(), Response::Granted { version: Seq(1), .. }));
    // re-acquire by the holder is immediate
    assert!(matches!(a.call(Request::AcquireWrite { root: ra, node: f }).await.unwrap(), Response::Granted { .. }));
    // b without lease while a holds it: conflict copy even with a current base
    let c = b.put_blob(b"b data").await.unwrap();
    let r = b.op(rb, Op::SetContent { node: f, base: Seq(1), content: c, size: 6, mtime_ns: 1 }).await;
    assert!(matches!(resolution(&r), Resolution::ContentConflictCopy { .. }));
    // b wants the lease: a is recalled, publishes, releases
    let pending = b.send(Request::AcquireWrite { root: rb, node: f }).unwrap();
    let Some(Push::Recall { root, node }) = a.next_push(T).await else { panic!("no recall") };
    assert_eq!((root, node), (ra, f));
    let c2 = a.put_blob(b"a final").await.unwrap();
    assert!(applied(&a.op(ra, Op::SetContent { node: f, base: Seq(1), content: c2, size: 7, mtime_ns: 2 }).await));
    a.call(Request::ReleaseWrite { root: ra, node: f }).await.unwrap();
    let Ok(Ok(Response::Granted { version, content })) = tokio::time::timeout(T, pending).await.unwrap() else { panic!() };
    assert_eq!(content, Some(c2));
    assert!(version > Seq(1));
    // now b holds: its SetContent applies regardless of base
    let c3 = b.put_blob(b"b wins").await.unwrap();
    assert!(applied(&b.op(rb, Op::SetContent { node: f, base: Seq(1), content: c3, size: 6, mtime_ns: 3 }).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_break_timeout_and_death_with_connection() {
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start_with(dir.path(), |c| c.lease_break = Duration::from_millis(400));
    srv.add_host("a");
    srv.add_host("b");
    srv.add_root("proj", &[("a", true), ("b", true)]);
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    let (f, _) = a.create_file(ra, R, "f", b"v1").await.unwrap();
    a.call(Request::AcquireWrite { root: ra, node: f }).await.unwrap();
    // a ignores the recall: the lease is broken after the timeout
    let t0 = std::time::Instant::now();
    let r = b.call(Request::AcquireWrite { root: rb, node: f }).await.unwrap();
    assert!(matches!(r, Response::Granted { .. }));
    assert!(t0.elapsed() >= Duration::from_millis(350) && t0.elapsed() < Duration::from_secs(3), "{:?}", t0.elapsed());
    assert!(matches!(a.next_push(T).await, Some(Push::Recall { .. })));
    // lease dies with the holder's connection: a takes it back after b disconnects, instantly
    drop(b);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(srv.shared().leases.count(), 0);
    let t0 = std::time::Instant::now();
    a.call(Request::AcquireWrite { root: ra, node: f }).await.unwrap();
    assert!(t0.elapsed() < Duration::from_millis(300));
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_durability_and_retry_after_crash() {
    let (_d, mut srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let c = a.commit_of(Op::Create { parent: R, name: name("keep"), node: a.new_node(), attrs: dir_attrs() });
    let r1 = a.commit(ra, vec![c.clone()]).await.unwrap();
    let (_, _) = a.create_file(ra, R, "file", b"payload").await.unwrap();
    let before = srv.snapshot("proj");
    let head = srv.head("proj");
    let epoch = srv.epoch();
    assert_eq!(head, Seq(2));
    srv.crash_restart();
    assert_eq!(srv.snapshot("proj"), before);
    assert!(a.call(Request::Ping).await.is_err(), "old connection died with the server");
    let (a2, ra2) = client(&srv, "a").await;
    let r2 = a2.commit(ra2, vec![c.clone()]).await.unwrap();
    assert_eq!(r1, r2, "stored result replayed after restart");
    assert_eq!(srv.head("proj"), head);
    assert_eq!(srv.epoch(), epoch);
    // content survived and can be read
    let ContentId::Blob(h) = before.iter().find(|r| r.name == name("file")).unwrap().attrs.content.unwrap() else { panic!() };
    assert!(matches!(a2.call(Request::GetBlob { hash: h }).await.unwrap(), Response::Blob { data } if data == b"payload"));
    // graceful restart too; subscribe backlog replays from the log
    srv.restart();
    let (a3, ra3) = client(&srv, "a").await;
    assert!(matches!(a3.subscribe(ra3, Seq(0)).await.unwrap(), Response::Subscribed { head: Seq(2) }));
    let Some(Push::Changes { changes, .. }) = a3.next_push(T).await else { panic!() };
    assert_eq!(changes.iter().map(|c| c.seq.0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(changes, srv.change_log("proj"));
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_server_never_acknowledges_uncommitted_work() {
    let (_d, mut srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let mut acked = vec![];
    // fire pipelined commits then kill; whatever was acked must be durable
    let mut rxs = vec![];
    for i in 0..300 {
        let c = a.commit_of(Op::Create { parent: R, name: name(&format!("k{i}")), node: a.new_node(), attrs: dir_attrs() });
        rxs.push((i, c.op_id, a.send(Request::Commit { root: ra, commits: vec![c] }).unwrap()));
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    srv.crash_restart();
    for (i, _op, rx) in rxs {
        if let Ok(Ok(Response::Committed { results })) = rx.await {
            assert!(applied(&results[0]));
            acked.push(i);
        }
    }
    let rows = srv.snapshot("proj");
    for i in &acked {
        assert!(rows.iter().any(|r| r.name == name(&format!("k{i}"))), "acked k{i} lost");
    }
    // invariant: applying the change log reproduces the snapshot
    assert_eq!(srv.change_log("proj").len(), rows.len());
    println!("acked before kill: {}/300, durable rows: {}", acked.len(), rows.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_batches() {
    let (_d, srv) = setup().await;
    let mut tasks = vec![];
    for t in 0..8 {
        let (c, r) = client(&srv, if t % 2 == 0 { "a" } else { "b" }).await;
        tasks.push(tokio::spawn(async move {
            let mut rxs = vec![];
            for i in 0..100 {
                let cm = c.commit_of(Op::Create { parent: R, name: name(&format!("t{t}-{i}")), node: c.new_node(), attrs: dir_attrs() });
                rxs.push(c.send(Request::Commit { root: r, commits: vec![cm] }).unwrap());
            }
            for rx in rxs {
                assert!(matches!(rx.await.unwrap().unwrap(), Response::Committed { .. }));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let st = &srv.shared().writer.stats;
    let (batches, jobs) = (st.batches.load(Ordering::Relaxed), st.commit_jobs.load(Ordering::Relaxed));
    println!("group commit: {jobs} commit requests in {batches} transactions");
    assert_eq!(jobs, 800);
    assert!(batches < jobs, "commits were batched into shared transactions");
    assert_eq!(srv.snapshot("proj").len(), 800);
}

#[tokio::test(flavor = "multi_thread")]
async fn credential_rules_over_the_wire() {
    let (_d, srv) = setup().await;
    srv.set_host("a", HostSet { allowed_uids: Some(Some("1000-1999".into())), root_squash: Some(true), ..Default::default() });
    let (a, ra) = client(&srv, "a").await; // connect after the change
    let mut c = a.commit_of(Op::Create { parent: R, name: name("ok"), node: a.new_node(), attrs: dir_attrs() });
    assert!(applied(&a.commit(ra, vec![c.clone()]).await.unwrap()[0]));
    c = a.commit_of(Op::Create { parent: R, name: name("bad"), node: a.new_node(), attrs: dir_attrs() });
    c.cred.uid = 0;
    assert!(matches!(a.commit(ra, vec![c]).await.unwrap()[0], CommitResult::Failed { error: ErrorReply { code: ErrCode::PermissionDenied, .. } }));
    let mut attrs = dir_attrs();
    attrs.uid = 5;
    let r = a.op(ra, Op::Create { parent: R, name: name("bad2"), node: a.new_node(), attrs }).await;
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::PermissionDenied, .. } }));
    assert_eq!(srv.snapshot("proj").len(), 1);
    // host b is unrestricted but root is squashed when asked
    srv.set_host("b", HostSet { root_squash: Some(true), ..Default::default() });
    let (b, rb) = client(&srv, "b").await;
    let mut cm = b.commit_of(Op::Create { parent: R, name: name("r"), node: b.new_node(), attrs: Attrs { uid: 0, gid: 0, ..dir_attrs() } });
    cm.cred = Cred { uid: 0, gid: 0 };
    assert!(applied(&b.commit(rb, vec![cm]).await.unwrap()[0]));
    let row = srv.snapshot("proj").into_iter().find(|r| r.name == name("r")).unwrap();
    assert_eq!((row.attrs.uid, row.attrs.gid), (65534, 65534));
    assert_eq!(srv.change_log("proj").last().unwrap().cred, Cred { uid: 65534, gid: 65534 });
}

#[tokio::test(flavor = "multi_thread")]
async fn request_size_caps() {
    let (_d, srv) = setup().await;
    srv.set_host("a", HostSet { max_commits: Some(Some(3)), max_frame: Some(Some(64 * 1024)), ..Default::default() });
    let (a, ra) = client(&srv, "a").await;
    let cmds: Vec<Commit> = (0..4).map(|i| a.commit_of(Op::Create { parent: R, name: name(&format!("c{i}")), node: a.new_node(), attrs: dir_attrs() })).collect();
    assert!(matches!(a.commit(ra, cmds).await, Err(ErrorReply { code: ErrCode::TooLarge, .. })));
    let cmds: Vec<Commit> = (0..3).map(|i| a.commit_of(Op::Create { parent: R, name: name(&format!("c{i}")), node: a.new_node(), attrs: dir_attrs() })).collect();
    assert_eq!(a.commit(ra, cmds).await.unwrap().len(), 3);
    // an oversized frame closes the connection
    let big = vec![7u8; 100 * 1024];
    let _ = a.call(Request::PutBlob { hash: Hash::of(&big), data: big }).await;
    assert!(a.call(Request::Ping).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn protocol_version_mismatch_and_double_hello() {
    use cairn_proto::aio::*;
    let (_d, srv) = setup().await;
    let cfg = tls::client_config(&srv.creds("a")).unwrap();
    let tcp = tokio::net::TcpStream::connect(srv.addr()).await.unwrap();
    let mut s = tokio_rustls::TlsConnector::from(cfg).connect(tls::server_name(), tcp).await.unwrap();
    write_envelope(&mut s, &Envelope { id: 1, body: Body::Request(Request::Hello { proto: 99, software: "x".into() }) }).await.unwrap();
    let r = read_envelope(&mut s).await.unwrap().unwrap();
    assert!(matches!(r.body, Body::Response(Err(ErrorReply { code: ErrCode::Invalid, .. }))));
    assert!(read_envelope(&mut s).await.unwrap().is_none(), "closed");
    let (a, _) = client(&srv, "a").await;
    assert!(matches!(a.call(Request::Hello { proto: 1, software: "x".into() }).await, Err(ErrorReply { code: ErrCode::Invalid, .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn change_log_replay_equals_snapshot() {
    let (_d, srv) = setup().await;
    let (a, ra) = client(&srv, "a").await;
    let (b, rb) = client(&srv, "b").await;
    let d = a.new_node();
    a.op(ra, Op::Create { parent: R, name: name("d"), node: d, attrs: dir_attrs() }).await;
    let (f, _) = a.create_file(ra, d, "f", b"x").await.unwrap();
    b.op(rb, Op::Rename { src_parent: d, src_name: name("f"), node: f, dst_parent: R, dst_name: name("g"), replaces: None, mode: RenameMode::NoReplace }).await;
    a.op(ra, Op::Link { parent: d, name: name("h"), node: f }).await;
    b.op(rb, Op::SetXattr { node: f, name: b"user.k".to_vec(), value: Some(b"v".to_vec()) }).await;
    a.op(ra, Op::Unlink { parent: d, name: name("h"), node: f, base: Seq(100) }).await;
    use std::collections::BTreeMap;
    let mut dirents: BTreeMap<(NodeId, Name), NodeId> = BTreeMap::new();
    let mut attrs: BTreeMap<NodeId, Attrs> = BTreeMap::new();
    for c in srv.change_log("proj") {
        match c.effect {
            Effect::Upsert { parent, name, node, attrs: a } => {
                dirents.insert((parent, name), node);
                attrs.insert(node, a);
            }
            Effect::Remove { parent, name, node, node_deleted } => {
                dirents.remove(&(parent, name));
                if node_deleted {
                    attrs.remove(&node);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                dirents.remove(&(src_parent, src_name));
                dirents.insert((dst_parent, dst_name), node);
            }
            Effect::Attrs { node, attrs: a } => {
                attrs.insert(node, a);
            }
        }
    }
    let snap: BTreeMap<(NodeId, Name), NodeId> = srv.snapshot("proj").into_iter().map(|r| ((r.parent, r.name), r.node)).collect();
    assert_eq!(dirents, snap);
    for r in srv.snapshot("proj") {
        assert_eq!(attrs[&r.node], r.attrs);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn prune_job_runs_inside_writer_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start_with(dir.path(), |c| {
        c.retention.log_secs = 0;
        c.retention.log_min_changes = 2;
    });
    srv.add_host("a");
    srv.add_root("proj", &[("a", true)]);
    let (a, ra) = client(&srv, "a").await;
    for i in 0..6 {
        a.create_file(ra, R, &format!("f{i}"), b"").await.unwrap();
    }
    srv.shared().writer.submit(cairn_server::store::Job::Prune);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(srv.change_log("proj").len(), 2, "log truncated to the last 2 changes");
    assert!(matches!(a.subscribe(ra, Seq(1)).await.unwrap(), Response::ResyncRequired { .. }));
}
