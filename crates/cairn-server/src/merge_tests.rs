use crate::merge::mem::*;
use crate::merge::*;
use cairn_proto::*;
use std::collections::BTreeMap;

const A: HostId = HostId(1);
const B: HostId = HostId(2);
const NOW: u64 = 1_704_164_645; // 2024-01-02T03:04:05Z

struct W {
    t: MemTree,
    n: u64,
    ctr: u64,
    rules: Rules,
}

fn nm(s: &str) -> Name {
    Name(s.as_bytes().to_vec())
}
fn hname(h: HostId) -> &'static str {
    if h == A {
        "a"
    } else {
        "b"
    }
}
fn fattrs(c: Option<ContentId>) -> Attrs {
    Attrs { kind: Kind::File, mode: 0o644, uid: 1000, gid: 1000, size: 3, content: c, ..dir_attrs() }
}
fn blob(x: u8) -> ContentId {
    ContentId::Blob(Hash::of(&[x]))
}
const R: NodeId = NodeId::ROOT;

impl W {
    fn new() -> W {
        W { t: MemTree::new(), n: 0, ctr: 0, rules: Rules::default() }
    }
    fn id(&mut self, h: HostId) -> NodeId {
        self.ctr += 1;
        NodeId::new(h, self.ctr)
    }
    fn run(&mut self, h: HostId, op: Op) -> CommitResult {
        self.run_o(h, op).result
    }
    fn run_o(&mut self, h: HostId, op: Op) -> Outcome {
        self.n += 1;
        let c = Commit { op_id: OpId { host: h, n: self.n }, cred: Cred { uid: 1000, gid: 1000 }, op };
        let ctx = Ctx { host: h, host_name: hname(h), rules: &self.rules, now_secs: NOW };
        apply(&mut self.t, &ctx, &c)
    }
    fn create_file(&mut self, h: HostId, parent: NodeId, name: &str, c: Option<ContentId>) -> NodeId {
        let id = self.id(h);
        let r = self.run(h, Op::Create { parent, name: nm(name), node: id, attrs: fattrs(c) });
        assert!(matches!(r, CommitResult::Applied { .. }), "{r:?}");
        id
    }
    fn mkdir(&mut self, h: HostId, parent: NodeId, name: &str) -> NodeId {
        let id = self.id(h);
        let r = self.run(h, Op::Create { parent, name: nm(name), node: id, attrs: dir_attrs() });
        assert!(matches!(r, CommitResult::Applied { .. }), "{r:?}");
        id
    }
    fn at(&self, p: NodeId, name: &str) -> Option<NodeId> {
        self.t.dirent(p, &nm(name))
    }
    fn unlink(&mut self, h: HostId, p: NodeId, name: &str, node: NodeId, base: u64) -> CommitResult {
        self.run(h, Op::Unlink { parent: p, name: nm(name), node, base: Seq(base) })
    }
    fn mv(&mut self, h: HostId, sp: NodeId, sn: &str, node: NodeId, dp: NodeId, dn: &str, rep: Option<NodeId>, mode: RenameMode) -> CommitResult {
        self.run(h, Op::Rename { src_parent: sp, src_name: nm(sn), node, dst_parent: dp, dst_name: nm(dn), replaces: rep, mode })
    }
    fn ver(&self, n: NodeId) -> NodeRec {
        self.t.node(n).unwrap()
    }
}

fn res(r: &CommitResult) -> &Resolution {
    match r {
        CommitResult::Transformed { conflict, .. } | CommitResult::Rejected { conflict } => &conflict.resolution,
        o => panic!("expected conflict, got {o:?}"),
    }
}

// ---- Create ----

#[test]
fn create_name_absent_applied() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    assert_eq!(w.at(R, "x"), Some(f));
    assert_eq!(w.ver(f).cver, Seq(1));
    assert_eq!(w.t.log.len(), 1);
    assert_eq!(w.t.pending_list, vec![(blob(1), A)]);
}

#[test]
fn create_equivalent_file_adopted_and_aliased() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let g = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("x"), node: g, attrs: fattrs(Some(blob(1))) });
    assert_eq!(*res(&r), Resolution::Adopted { node: f });
    assert!(matches!(r, CommitResult::Transformed { .. }));
    assert_eq!(w.t.log.len(), 1, "no new change for adoption");
    // queued ops naming the client's node keep working through the alias
    let r = w.run(B, Op::SetAttr { node: g, base: Seq(0), changes: AttrChanges { mode: Some(0o600), ..Default::default() } });
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.ver(f).attrs.mode, 0o600);
    // a new child under the adopted dir id too
    let d = w.mkdir(A, R, "d");
    let d2 = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("d"), node: d2, attrs: dir_attrs() });
    assert_eq!(*res(&r), Resolution::Adopted { node: d });
    let c = w.id(B);
    let r = w.run(B, Op::Create { parent: d2, name: nm("c"), node: c, attrs: fattrs(None) });
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.at(d, "c"), Some(c));
}

#[test]
fn create_empty_equals_none_content() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    let g = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("x"), node: g, attrs: fattrs(Some(ContentId::Empty)) });
    assert_eq!(*res(&r), Resolution::Adopted { node: f });
}

#[test]
fn create_symlinks() {
    let mut w = W::new();
    let mk = |t: &[u8]| Attrs { kind: Kind::Symlink, symlink_target: Some(t.to_vec()), size: t.len() as u64, ..dir_attrs() };
    let s = w.id(A);
    w.run(A, Op::Create { parent: R, name: nm("l"), node: s, attrs: mk(b"t1") });
    let s2 = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("l"), node: s2, attrs: mk(b"t1") });
    assert_eq!(*res(&r), Resolution::Adopted { node: s });
    let s3 = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("l"), node: s3, attrs: mk(b"t2") });
    assert!(matches!(res(&r), Resolution::ConflictCopy { .. }));
}

#[test]
fn create_different_content_conflict_copy() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let g = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("x"), node: g, attrs: fattrs(Some(blob(2))) });
    assert_eq!(*res(&r), Resolution::ConflictCopy { parent: R, name: nm("x.conflict-b-20240102T030405") });
    assert_eq!(w.at(R, "x"), Some(f));
    assert_eq!(w.at(R, "x.conflict-b-20240102T030405"), Some(g));
    // second collision in the same second gets a suffix
    let h = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("x"), node: h, attrs: fattrs(Some(blob(3))) });
    assert_eq!(*res(&r), Resolution::ConflictCopy { parent: R, name: nm("x.conflict-b-20240102T030405-2") });
    // conflict log
    assert_eq!(w.t.conflicts.len(), 2);
    assert_eq!(w.t.conflicts[0].host, B);
}

#[test]
fn create_different_kind_conflict_copy() {
    let mut w = W::new();
    w.mkdir(A, R, "x");
    let g = w.id(B);
    let r = w.run(B, Op::Create { parent: R, name: nm("x"), node: g, attrs: fattrs(None) });
    assert!(matches!(res(&r), Resolution::ConflictCopy { .. }));
}

#[test]
fn utc_stamp_values() {
    assert_eq!(utc_stamp(0), "19700101T000000");
    assert_eq!(utc_stamp(NOW), "20240102T030405");
    assert_eq!(utc_stamp(1_709_164_800), "20240229T000000");
    assert_eq!(utc_stamp(1_709_164_800 + 86399), "20240229T235959");
    assert_eq!(utc_stamp(1_709_251_200), "20240301T000000");
}

#[test]
fn invalid_names_rejected() {
    let mut w = W::new();
    for bad in ["", ".", "..", "a/b", "a\0b"] {
        let id = w.id(A);
        let r = w.run(A, Op::Create { parent: R, name: nm(bad), node: id, attrs: fattrs(None) });
        assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::Invalid, .. } }), "{bad:?}");
    }
    let id = w.id(A);
    let r = w.run(A, Op::Create { parent: R, name: Name(vec![b'a'; 256]), node: id, attrs: fattrs(None) });
    assert!(matches!(r, CommitResult::Failed { .. }));
}

// ---- Link ----

#[test]
fn link_absent_applied_and_nlink() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    let r = w.run(B, Op::Link { parent: R, name: nm("y"), node: f });
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.t.dirents_of(f).len(), 2);
}

#[test]
fn link_name_taken_conflict_copy() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.create_file(A, R, "y", Some(blob(1)));
    let r = w.run(B, Op::Link { parent: R, name: nm("y"), node: f });
    assert_eq!(*res(&r), Resolution::ConflictCopy { parent: R, name: nm("y.conflict-b-20240102T030405") });
    assert_eq!(w.at(R, "y.conflict-b-20240102T030405"), Some(f));
}

#[test]
fn link_to_deleted_node_resurrects() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let v = w.ver(f).version.0;
    assert!(matches!(w.unlink(A, R, "x", f, v), CommitResult::Applied { .. }));
    assert!(w.t.node(f).is_none());
    let r = w.run(B, Op::Link { parent: R, name: nm("z"), node: f });
    assert!(matches!(r, CommitResult::Applied { .. }), "{r:?}");
    assert_eq!(w.at(R, "z"), Some(f));
    assert_eq!(w.ver(f).attrs.content, Some(blob(1)));
    assert!(w.t.tomb(f).is_none());
}

// ---- Unlink ----

#[test]
fn unlink_applied_tombstones() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    let r = w.unlink(A, R, "x", f, 1);
    assert!(matches!(r, CommitResult::Applied { seq: Seq(2) }));
    assert!(w.at(R, "x").is_none() && w.t.node(f).is_none());
    assert_eq!(w.t.tomb(f).unwrap().name, nm("x"));
}

#[test]
fn unlink_absent_is_noop() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.unlink(A, R, "x", f, 1);
    let n = w.t.log.len();
    let r = w.unlink(B, R, "x", f, 1);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.t.log.len(), n);
}

#[test]
fn unlink_modified_by_other_loses_and_reannounces() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let base = w.ver(f).version.0;
    let r = w.run(B, Op::SetAttr { node: f, base: Seq(base), changes: AttrChanges { mode: Some(0o600), ..Default::default() } });
    assert!(matches!(r, CommitResult::Applied { .. }));
    let r = w.unlink(A, R, "x", f, base);
    assert_eq!(*res(&r), Resolution::DeleteLost);
    assert_eq!(w.at(R, "x"), Some(f));
    assert!(matches!(w.t.log.last().unwrap().effect, Effect::Upsert { .. }), "client gets the entry back");
}

#[test]
fn unlink_own_modification_ok() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.run(A, Op::SetAttr { node: f, base: Seq(0), changes: AttrChanges { mode: Some(0o600), ..Default::default() } });
    let r = w.unlink(A, R, "x", f, 1);
    assert!(matches!(r, CommitResult::Applied { .. }));
}

#[test]
fn unlink_dirent_points_elsewhere_loses() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    w.unlink(A, R, "x", f, 1);
    let g = w.create_file(B, R, "x", Some(blob(2)));
    let r = w.unlink(A, R, "x", f, 1);
    assert_eq!(*res(&r), Resolution::DeleteLost);
    assert_eq!(w.at(R, "x"), Some(g));
}

#[test]
fn unlink_nonempty_dir_loses() {
    let mut w = W::new();
    let d = w.mkdir(A, R, "d");
    w.create_file(B, d, "child", None);
    let r = w.unlink(A, R, "d", d, 1);
    assert_eq!(*res(&r), Resolution::DeleteLost);
    assert_eq!(w.at(R, "d"), Some(d));
}

#[test]
fn unlink_hardlink_keeps_node() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.run(A, Op::Link { parent: R, name: nm("y"), node: f });
    // modified by B; but removing one of two links loses nothing
    w.run(B, Op::SetAttr { node: f, base: Seq(1), changes: AttrChanges { mode: Some(0o600), ..Default::default() } });
    let r = w.unlink(A, R, "x", f, 1);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert!(w.t.node(f).is_some());
    assert_eq!(w.t.dirents_of(f), vec![(R, nm("y"))]);
}

// ---- Rename ----

#[test]
fn rename_applied() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    let d = w.mkdir(A, R, "d");
    let r = w.mv(A, R, "x", f, d, "y", None, RenameMode::NoReplace);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.at(d, "y"), Some(f));
    assert!(w.at(R, "x").is_none());
}

#[test]
fn rename_replace_expected() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let g = w.create_file(A, R, "y", Some(blob(2)));
    let r = w.mv(A, R, "x", f, R, "y", Some(g), RenameMode::Replace);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.at(R, "y"), Some(f));
    assert!(w.t.node(g).is_none() && w.t.tomb(g).is_some());
    assert!(matches!(w.t.log[w.t.log.len() - 2].effect, Effect::Remove { node_deleted: true, .. }));
}

#[test]
fn rename_source_moved_by_other_rejected() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.mv(B, R, "x", f, R, "b", None, RenameMode::NoReplace);
    let r = w.mv(A, R, "x", f, R, "a", None, RenameMode::NoReplace);
    assert!(matches!(r, CommitResult::Rejected { .. }));
    assert_eq!(*res(&r), Resolution::RenameLost);
    assert_eq!(w.at(R, "b"), Some(f));
    assert!(w.at(R, "a").is_none());
}

#[test]
fn rename_source_deleted_resurrects_at_destination() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    w.unlink(B, R, "x", f, 1000);
    let r = w.mv(A, R, "x", f, R, "z", None, RenameMode::NoReplace);
    assert!(matches!(r, CommitResult::Applied { .. }), "{r:?}");
    assert_eq!(w.at(R, "z"), Some(f));
    assert_eq!(w.ver(f).attrs.content, Some(blob(1)));
}

#[test]
fn rename_destination_unexpected_conflict_copy() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let g = w.create_file(B, R, "y", Some(blob(2)));
    // client believed y absent
    let r = w.mv(A, R, "x", f, R, "y", None, RenameMode::Replace);
    assert_eq!(*res(&r), Resolution::ConflictCopy { parent: R, name: nm("y.conflict-a-20240102T030405") });
    assert_eq!(w.at(R, "y"), Some(g));
    assert_eq!(w.at(R, "y.conflict-a-20240102T030405"), Some(f));
    // client believed y was a different node
    let h = w.create_file(A, R, "p", None);
    let r = w.mv(A, R, "p", h, R, "y", Some(f), RenameMode::Replace);
    assert!(matches!(res(&r), Resolution::ConflictCopy { .. }));
}

#[test]
fn rename_noreplace_over_existing_conflict_copy() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.create_file(B, R, "y", Some(blob(2)));
    let r = w.mv(A, R, "x", f, R, "y", None, RenameMode::NoReplace);
    assert!(matches!(res(&r), Resolution::ConflictCopy { .. }));
}

#[test]
fn rename_cycle_rejected() {
    let mut w = W::new();
    let a = w.mkdir(A, R, "a");
    let b = w.mkdir(A, a, "b");
    let c = w.mkdir(A, b, "c");
    let r = w.mv(A, R, "a", a, c, "a", None, RenameMode::NoReplace);
    assert_eq!(*res(&r), Resolution::CycleRejected);
    let r = w.mv(A, R, "a", a, a, "a", None, RenameMode::NoReplace);
    assert_eq!(*res(&r), Resolution::CycleRejected);
    // concurrent opposite renames: second one would cycle
    let x = w.mkdir(A, R, "x");
    let y = w.mkdir(A, R, "y");
    assert!(matches!(w.mv(A, R, "x", x, y, "x", None, RenameMode::NoReplace), CommitResult::Applied { .. }));
    let r = w.mv(B, R, "y", y, x, "y", None, RenameMode::NoReplace);
    assert_eq!(*res(&r), Resolution::CycleRejected);
}

#[test]
fn rename_exchange() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let g = w.create_file(A, R, "y", Some(blob(2)));
    let r = w.mv(A, R, "x", f, R, "y", Some(g), RenameMode::Exchange);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.at(R, "x"), Some(g));
    assert_eq!(w.at(R, "y"), Some(f));
    assert!(w.t.node(f).is_some() && w.t.node(g).is_some());
}

#[test]
fn rename_dir_over_empty_dir_and_nonempty() {
    let mut w = W::new();
    let d1 = w.mkdir(A, R, "d1");
    let d2 = w.mkdir(A, R, "d2");
    let r = w.mv(A, R, "d1", d1, R, "d2", Some(d2), RenameMode::Replace);
    assert!(matches!(r, CommitResult::Applied { .. }));
    let d3 = w.mkdir(A, R, "d3");
    w.create_file(B, d1, "k", None); // d1 now lives at "d2"
    let r = w.mv(A, R, "d3", d3, R, "d2", Some(d1), RenameMode::Replace);
    assert!(matches!(res(&r), Resolution::ConflictCopy { .. }));
}

// ---- Deleted directories ----

#[test]
fn op_into_deleted_dir_recreates_recursively() {
    let mut w = W::new();
    let a = w.mkdir(A, R, "a");
    let b = w.mkdir(A, a, "b");
    assert!(matches!(w.unlink(B, a, "b", b, 1000), CommitResult::Applied { .. }));
    assert!(matches!(w.unlink(B, R, "a", a, 1000), CommitResult::Applied { .. }));
    let f = w.id(A);
    let r = w.run(A, Op::Create { parent: b, name: nm("f"), node: f, attrs: fattrs(None) });
    assert!(matches!(r, CommitResult::Transformed { .. }), "{r:?}");
    assert_eq!(*res(&r), Resolution::DirRecreated { dir: b });
    assert_eq!(w.at(R, "a"), Some(a));
    assert_eq!(w.at(a, "b"), Some(b));
    assert_eq!(w.at(b, "f"), Some(f));
}

#[test]
fn dir_recreation_name_taken_gets_conflict_name() {
    let mut w = W::new();
    let a = w.mkdir(A, R, "a");
    w.unlink(B, R, "a", a, 1000);
    w.create_file(B, R, "a", Some(blob(9)));
    let f = w.id(A);
    let r = w.run(A, Op::Create { parent: a, name: nm("f"), node: f, attrs: fattrs(None) });
    assert!(matches!(res(&r), Resolution::DirRecreated { .. }));
    assert_eq!(w.at(R, "a.conflict-a-20240102T030405"), Some(a));
    assert_eq!(w.at(a, "f"), Some(f));
}

#[test]
fn rename_and_link_into_deleted_dir() {
    let mut w = W::new();
    let d = w.mkdir(A, R, "d");
    let f = w.create_file(A, R, "f", None);
    w.unlink(B, R, "d", d, 1000);
    let r = w.mv(A, R, "f", f, d, "g", None, RenameMode::NoReplace);
    assert_eq!(*res(&r), Resolution::DirRecreated { dir: d });
    assert_eq!(w.at(d, "g"), Some(f));
    let h = w.create_file(A, R, "h", None);
    let _ = h;
}

#[test]
fn parent_unknown_is_not_found() {
    let mut w = W::new();
    let f = w.id(A);
    let r = w.run(A, Op::Create { parent: NodeId::new(A, 999), name: nm("x"), node: f, attrs: fattrs(None) });
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::NotFound, .. } }));
}

// ---- SetAttr / SetXattr ----

#[test]
fn setattr_last_writer_wins() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.run(A, Op::SetAttr { node: f, base: Seq(1), changes: AttrChanges { mode: Some(0o600), ..Default::default() } });
    w.run(B, Op::SetAttr { node: f, base: Seq(1), changes: AttrChanges { mode: Some(0o640), uid: Some(5), ..Default::default() } });
    let a = w.ver(f).attrs;
    assert_eq!((a.mode, a.uid), (0o640, 5));
}

#[test]
fn setattr_on_deleted_is_not_found() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.unlink(A, R, "x", f, 1);
    let r = w.run(B, Op::SetAttr { node: f, base: Seq(1), changes: AttrChanges::default() });
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::NotFound, .. } }));
    let r = w.run(B, Op::SetXattr { node: f, name: b"user.a".to_vec(), value: Some(vec![1]) });
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::NotFound, .. } }));
}

#[test]
fn setxattr_set_and_remove_sorted() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", None);
    w.run(A, Op::SetXattr { node: f, name: b"user.b".to_vec(), value: Some(vec![2]) });
    w.run(A, Op::SetXattr { node: f, name: b"user.a".to_vec(), value: Some(vec![1]) });
    assert_eq!(w.ver(f).attrs.xattrs, vec![(b"user.a".to_vec(), vec![1]), (b"user.b".to_vec(), vec![2])]);
    w.run(A, Op::SetXattr { node: f, name: b"user.a".to_vec(), value: None });
    assert_eq!(w.ver(f).attrs.xattrs.len(), 1);
}

// ---- SetContent ----

fn setc(w: &mut W, h: HostId, f: NodeId, base: u64, c: ContentId) -> CommitResult {
    w.run(h, Op::SetContent { node: f, base: Seq(base), content: c, size: 5, mtime_ns: 77 })
}

#[test]
fn setcontent_current_base_applied() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    let r = setc(&mut w, B, f, 1, blob(2));
    assert!(matches!(r, CommitResult::Applied { seq: Seq(2) }));
    let rec = w.ver(f);
    assert_eq!((rec.cver, rec.attrs.content, rec.attrs.size, rec.attrs.mtime_ns), (Seq(2), Some(blob(2)), 5, 77));
    assert!(w.t.pending_list.contains(&(blob(2), B)));
}

#[test]
fn setcontent_base_ge_cver_applies_base_lt_conflicts() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1))); // cver = 1
    // base above the content version (client learned the node from a listing at a later head)
    let r = setc(&mut w, B, f, 50, blob(2));
    assert!(matches!(r, CommitResult::Applied { .. }), "{r:?}");
    assert_eq!(w.ver(f).cver, Seq(2));
    // base below the current content version: conflict copy
    let r = setc(&mut w, A, f, 1, blob(3));
    assert!(matches!(res(&r), Resolution::ContentConflictCopy { .. }));
    assert_eq!(w.ver(f).attrs.content, Some(blob(2)));
    // exactly equal still applies
    let r = setc(&mut w, A, f, 2, blob(4));
    assert!(matches!(r, CommitResult::Applied { .. }));
}

#[test]
fn unlink_base_above_version_deletes() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    w.run(B, Op::SetAttr { node: f, base: Seq(1), changes: AttrChanges { mode: Some(0o600), ..Default::default() } }); // version 2
    let r = w.unlink(A, R, "x", f, 9); // client saw it in a listing at head 9
    assert!(matches!(r, CommitResult::Applied { .. }));
}

#[test]
fn setcontent_stale_base_conflict_copy_with_server_node() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    setc(&mut w, A, f, 1, blob(2));
    let r = setc(&mut w, B, f, 1, blob(3));
    assert_eq!(*res(&r), Resolution::ContentConflictCopy { parent: R, name: nm("x.conflict-b-20240102T030405") });
    assert_eq!(w.ver(f).attrs.content, Some(blob(2)), "original untouched");
    let c = w.at(R, "x.conflict-b-20240102T030405").unwrap();
    assert_eq!(c, NodeId::new(HostId(0), 2));
    assert_eq!(w.ver(c).attrs.content, Some(blob(3)));
    // next mint increments
    let r = setc(&mut w, B, f, 1, blob(4));
    assert!(matches!(res(&r), Resolution::ContentConflictCopy { .. }));
    assert!(w.t.node(NodeId::new(HostId(0), 3)).is_some());
}

#[test]
fn setcontent_lease_holder_wins_regardless_of_base() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    setc(&mut w, B, f, 1, blob(2));
    w.t.leases.insert(f, A);
    let r = setc(&mut w, A, f, 1, blob(3)); // stale base but holds lease
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.ver(f).attrs.content, Some(blob(3)));
}

#[test]
fn setcontent_other_holds_lease_conflict_copy() {
    let mut w = W::new();
    let f = w.create_file(A, R, "x", Some(blob(1)));
    w.t.leases.insert(f, A);
    let r = setc(&mut w, B, f, 1, blob(3)); // base current, but A holds the lease
    assert!(matches!(res(&r), Resolution::ContentConflictCopy { .. }));
    assert_eq!(w.ver(f).attrs.content, Some(blob(1)));
}

#[test]
fn setcontent_on_deleted_resurrects_then_applies() {
    let mut w = W::new();
    let d = w.mkdir(A, R, "d");
    let f = w.create_file(A, d, "x", Some(blob(1)));
    w.unlink(B, d, "x", f, 1000);
    w.unlink(B, R, "d", d, 1000);
    let r = setc(&mut w, A, f, 0, blob(2));
    assert_eq!(*res(&r), Resolution::DirRecreated { dir: d });
    assert_eq!(w.at(d, "x"), Some(f));
    assert_eq!(w.ver(f).attrs.content, Some(blob(2)));
    // plain deleted file
    let g = w.create_file(A, R, "g", Some(blob(1)));
    w.unlink(B, R, "g", g, 1000);
    let r = setc(&mut w, A, g, 0, blob(3));
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.ver(g).attrs.content, Some(blob(3)));
}

#[test]
fn setcontent_known_content_not_pending() {
    let mut w = W::new();
    w.t.content.insert(blob(5));
    let f = w.create_file(A, R, "x", None);
    setc(&mut w, A, f, 1, blob(5));
    assert!(w.t.pending_list.is_empty());
}

// ---- idempotency, creds ----

#[test]
fn replay_returns_stored_result_without_applying() {
    let mut w = W::new();
    let f = w.id(A);
    let c = Commit { op_id: OpId { host: A, n: 500 }, cred: Cred { uid: 1000, gid: 1000 }, op: Op::Create { parent: R, name: nm("x"), node: f, attrs: fattrs(None) } };
    let ctx = Ctx { host: A, host_name: "a", rules: &Rules::default(), now_secs: NOW };
    let o1 = apply(&mut w.t, &ctx, &c);
    assert!(!o1.replay);
    let o2 = apply(&mut w.t, &ctx, &c);
    assert!(o2.replay && o2.changes.is_empty());
    assert_eq!(o1.result, o2.result);
    assert_eq!(w.t.log.len(), 1);
    // replay of a conflicting result is stable too
    let g = w.id(B);
    let c2 = Commit { op_id: OpId { host: B, n: 501 }, cred: c.cred, op: Op::Create { parent: R, name: nm("x"), node: g, attrs: fattrs(Some(blob(7))) } };
    let ctx = Ctx { host: B, host_name: "b", rules: &Rules::default(), now_secs: NOW };
    let r1 = apply(&mut w.t, &ctx, &c2).result;
    let ctx2 = Ctx { host: B, host_name: "b", rules: &Rules::default(), now_secs: NOW + 5000 };
    let r2 = apply(&mut w.t, &ctx2, &c2);
    assert_eq!(r1, r2.result);
    assert_eq!(w.t.log.len(), 2);
}

#[test]
fn credential_rules() {
    let mut w = W::new();
    w.rules = Rules {
        allowed_uids: Some(vec![(0, 0), (1000, 1999)]),
        allowed_gids: Some(vec![(1000, 1999)]),
        root_squash: true,
        uid_map: vec![MapRange { from: 100000, to: 1000, len: 1000 }],
        gid_map: vec![],
    };
    let mk = |w: &mut W, uid: u32, gid: u32, attr_uid: u32| {
        let id = w.id(A);
        w.n += 1;
        let c = Commit { op_id: OpId { host: A, n: w.n }, cred: Cred { uid, gid }, op: Op::Create { parent: R, name: nm(&format!("f{}", w.n)), node: id, attrs: Attrs { uid: attr_uid, ..fattrs(None) } } };
        let ctx = Ctx { host: A, host_name: "a", rules: &w.rules, now_secs: NOW };
        let o = apply(&mut w.t, &ctx, &c);
        (o.result, id)
    };
    let (r, _) = mk(&mut w, 1000, 1000, 1000);
    assert!(matches!(r, CommitResult::Applied { .. }));
    let (r, _) = mk(&mut w, 5, 1000, 1000);
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::PermissionDenied, .. } }));
    let (r, _) = mk(&mut w, 1000, 5, 1000);
    assert!(matches!(r, CommitResult::Failed { error: ErrorReply { code: ErrCode::PermissionDenied, .. } }));
    let (r, _) = mk(&mut w, 1000, 1000, 2500);
    assert!(matches!(r, CommitResult::Failed { .. }), "attrs uid checked");
    // root is allowed but squashed
    let (r, id) = mk(&mut w, 0, 1000, 0);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.ver(id).attrs.uid, NOBODY);
    assert_eq!(w.t.log.last().unwrap().cred.uid, NOBODY);
    // mapped
    let (r, id) = mk(&mut w, 100005, 1000, 100007);
    assert!(matches!(r, CommitResult::Applied { .. }));
    assert_eq!(w.ver(id).attrs.uid, 1007);
    assert_eq!(w.t.log.last().unwrap().cred.uid, 1005);
}

#[test]
fn rules_parsing() {
    assert_eq!(Rules::parse_ranges("0, 1000-1999").unwrap(), vec![(0, 0), (1000, 1999)]);
    assert_eq!(Rules::fmt_ranges(&[(0, 0), (1000, 1999)]), "0,1000-1999");
    assert_eq!(Rules::parse_map("1:2:3").unwrap(), vec![MapRange { from: 1, to: 2, len: 3 }]);
    assert!(Rules::parse_map("1:2").is_err());
}

// ---- invariant: replaying the change log reproduces the namespace ----

#[derive(Default)]
struct Model {
    nodes: BTreeMap<NodeId, Attrs>,
    dirents: BTreeMap<(NodeId, Name), NodeId>,
}
impl Model {
    fn apply(&mut self, e: &Effect) {
        match e {
            Effect::Upsert { parent, name, node, attrs } => {
                self.nodes.insert(*node, attrs.clone());
                self.dirents.insert((*parent, name.clone()), *node);
            }
            Effect::Remove { parent, name, node, node_deleted } => {
                self.dirents.remove(&(*parent, name.clone()));
                if *node_deleted {
                    self.nodes.remove(node);
                }
            }
            Effect::Move { src_parent, src_name, dst_parent, dst_name, node } => {
                self.dirents.remove(&(*src_parent, src_name.clone()));
                self.dirents.insert((*dst_parent, dst_name.clone()), *node);
            }
            Effect::Attrs { node, attrs } => {
                self.nodes.insert(*node, attrs.clone());
            }
        }
    }
}

#[test]
fn model_replay_reproduces_namespace_random() {
    for seed in 1..=40u64 {
        let mut s = seed.wrapping_mul(0x9E3779B97F4A7C15) | 1;
        let mut rnd = move |n: u64| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s % n
        };
        let mut w = W::new();
        let mut ids: Vec<NodeId> = vec![R];
        let names = ["a", "b", "c", "d"];
        for _ in 0..300 {
            let h = if rnd(2) == 0 { A } else { B };
            let pick = |r: &mut dyn FnMut(u64) -> u64, ids: &Vec<NodeId>| ids[r(ids.len() as u64) as usize];
            let parent = pick(&mut rnd, &ids);
            let name = nm(names[rnd(4) as usize]);
            let node = pick(&mut rnd, &ids);
            let op = match rnd(9) {
                0 | 1 => {
                    let id = w.id(h);
                    ids.push(id);
                    let attrs = if rnd(3) == 0 { dir_attrs() } else { fattrs(Some(blob(rnd(3) as u8))) };
                    Op::Create { parent, name, node: id, attrs }
                }
                2 => Op::Link { parent, name, node },
                3 => Op::Unlink { parent, name, node, base: Seq(rnd(w.t.head + 1)) },
                4 | 5 => {
                    let dp = pick(&mut rnd, &ids);
                    let rep = if rnd(2) == 0 { Some(pick(&mut rnd, &ids)) } else { None };
                    let mode = [RenameMode::Replace, RenameMode::NoReplace, RenameMode::Exchange][rnd(3) as usize];
                    Op::Rename { src_parent: parent, src_name: name, node, dst_parent: dp, dst_name: nm(names[rnd(4) as usize]), replaces: rep, mode }
                }
                6 => Op::SetAttr { node, base: Seq(0), changes: AttrChanges { mode: Some(rnd(512) as u32), ..Default::default() } },
                7 => Op::SetXattr { node, name: b"user.x".to_vec(), value: if rnd(2) == 0 { Some(vec![rnd(9) as u8]) } else { None } },
                _ => Op::SetContent { node, base: Seq(rnd(w.t.head + 1)), content: blob(rnd(5) as u8), size: 1, mtime_ns: 1 },
            };
            let o = w.run_o(h, op);
            if let CommitResult::Applied { seq } | CommitResult::Transformed { seq, .. } = &o.result {
                assert!(seq.0 <= w.t.head);
            }
        }
        let mut m = Model::default();
        m.nodes.insert(R, dir_attrs());
        let mut last = 0;
        for c in &w.t.log {
            assert_eq!(c.seq.0, last + 1, "seq gapless");
            last = c.seq.0;
            m.apply(&c.effect);
        }
        let live: BTreeMap<_, _> = w.t.nodes.iter().map(|(k, v)| (*k, v.attrs.clone())).collect();
        assert_eq!(m.dirents, w.t.dirents, "seed {seed}");
        assert_eq!(m.nodes, live, "seed {seed}");
        // structural sanity: every dirent's node exists; every non-root node reachable via dirents; no dir has 2 dirents
        for ((p, _), n) in &w.t.dirents {
            assert!(w.t.nodes.contains_key(n) && w.t.nodes.contains_key(p));
        }
        for (n, r) in &w.t.nodes {
            if *n != R {
                let links = w.t.dirents_of(*n).len();
                assert!(links >= 1, "orphan {n:?} seed {seed}");
                if r.attrs.kind == Kind::Dir {
                    assert_eq!(links, 1);
                }
            }
        }
    }
}
