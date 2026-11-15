//! Seeded generators and shrinking.
//!
//! Two layers:
//! - `Step`/`SimOp`: client-level operations over a tiny name space (to
//!   force conflicts), executed through the `Core` trait by `sim`.
//! - `wire_commits`: raw `Commit`s for exercising the reference model on its
//!   own (model self-test).

use cairn_proto::RenameMode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimOp {
    Create(String),
    Mkdir(String),
    Symlink(String, String),
    Link(String, String),
    Unlink(String),
    Rmdir(String),
    Rename(String, String, RenameMode),
    Chmod(String, u32),
    SetXattr(String, String, Option<Vec<u8>>),
    /// Write `len` bytes derived from `seed` (truncating); len > 64 KiB gives chunked content.
    Write(String, u8, usize),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    Partition(usize),
    Heal(usize),
    /// Drop the client without shutdown and reopen from the same cache dir.
    KillClient(usize),
    /// Abrupt server death followed by restart.
    ServerRestart,
    /// Reset all connections of one client mid-stream (proxy).
    Reset(usize),
    /// Add latency (ms) to one client's link.
    Delay(usize, u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Op(usize, SimOp),
    Fault(Fault),
    /// Heal everything, wait until quiescent, run the invariant checks.
    Quiesce,
    /// Deterministic concurrency: partition every client, run each client's ops
    /// offline (so they all work from stale views), then heal one client at a time
    /// in `heal` order, waiting for each to sync before the next. Ends with the checks.
    Round { ops: Vec<(usize, SimOp)>, heal: Vec<usize> },
}

#[derive(Clone, Debug)]
pub struct GenCfg {
    pub clients: usize,
    pub steps: usize,
    pub faults: bool,
    pub big_files: bool,
    /// Generate `Round` steps (deterministic, shrinkable) instead of free-running concurrency.
    pub rounds: bool,
}

impl Default for GenCfg {
    fn default() -> Self {
        GenCfg { clients: 3, steps: 200, faults: true, big_files: false, rounds: false }
    }
}

const DIRS: &[&str] = &["", "", "d0", "d0", "d1", "d0/s"];
const NAMES: &[&str] = &["a", "b", "c", "d"];
const DNAMES: &[&str] = &["d0", "d1", "x"];

fn path(r: &mut StdRng) -> String {
    let d = DIRS[r.random_range(0..DIRS.len())];
    let n = NAMES[r.random_range(0..NAMES.len())];
    if d.is_empty() { n.to_string() } else { format!("{d}/{n}") }
}

fn dpath(r: &mut StdRng) -> String {
    match r.random_range(0..5) {
        0 => "d0".into(),
        1 => "d1".into(),
        2 => "d0/s".into(),
        3 => format!("d1/{}", DNAMES[r.random_range(0..DNAMES.len())]),
        _ => format!("d0/{}", DNAMES[r.random_range(0..DNAMES.len())]),
    }
}

pub fn gen_op(r: &mut StdRng, big: bool) -> SimOp {
    match r.random_range(0..100) {
        0..=25 => SimOp::Create(path(r)),
        26..=29 => SimOp::Mkdir(dpath(r)),
        30..=31 => SimOp::Symlink(path(r), ["t1", "t2"][r.random_range(0..2)].into()),
        32..=36 => SimOp::Link(path(r), path(r)),
        37..=43 => SimOp::Unlink(path(r)),
        44..=48 => SimOp::Rmdir(dpath(r)),
        49..=66 => {
            let mode = match r.random_range(0..10) {
                0 => RenameMode::NoReplace,
                1 => RenameMode::Exchange,
                _ => RenameMode::Replace,
            };
            if r.random_bool(0.2) { SimOp::Rename(dpath(r), dpath(r), mode) } else { SimOp::Rename(path(r), path(r), mode) }
        }
        67..=72 => SimOp::Chmod(path(r), [0o600, 0o644, 0o755][r.random_range(0..3)]),
        73..=77 => SimOp::SetXattr(path(r), ["user.a", "user.b"][r.random_range(0..2)].into(), if r.random_bool(0.7) { Some(vec![r.random_range(0..4)]) } else { None }),
        _ => {
            let len = if big && r.random_bool(0.1) { 70_000 + r.random_range(0..200_000) } else { r.random_range(0..2000) };
            // few distinct seeds so identical content collides (adopt rule)
            SimOp::Write(path(r), r.random_range(0..3), len)
        }
    }
}

pub fn gen_steps(seed: u64, cfg: &GenCfg) -> Vec<Step> {
    let mut r = StdRng::seed_from_u64(seed);
    let mut v = vec![Step::Op(0, SimOp::Mkdir("d0".into())), Step::Op(0, SimOp::Mkdir("d1".into())), Step::Op(0, SimOp::Mkdir("d0/s".into())), Step::Quiesce];
    if cfg.rounds {
        for _ in 0..cfg.steps / 12 + 1 {
            let n = r.random_range(2..14);
            let ops = (0..n).map(|_| (r.random_range(0..cfg.clients), gen_op(&mut r, cfg.big_files))).collect();
            let mut heal: Vec<usize> = (0..cfg.clients).collect();
            for i in (1..heal.len()).rev() {
                heal.swap(i, r.random_range(0..=i));
            }
            v.push(Step::Round { ops, heal });
        }
        return v;
    }
    for i in 0..cfg.steps {
        let roll = r.random_range(0..100);
        if cfg.faults && roll < 10 {
            let c = r.random_range(0..cfg.clients);
            v.push(Step::Fault(match r.random_range(0..10) {
                0..=2 => Fault::Partition(c),
                3..=4 => Fault::Heal(c),
                5 => Fault::KillClient(c),
                6 => Fault::ServerRestart,
                7 => Fault::Reset(c),
                _ => Fault::Delay(c, r.random_range(0..50)),
            }));
        } else if roll < 12 || (i + 1) % 60 == 0 {
            v.push(Step::Quiesce);
        } else {
            v.push(Step::Op(r.random_range(0..cfg.clients), gen_op(&mut r, cfg.big_files)));
        }
    }
    v.push(Step::Quiesce);
    v
}

/// Delta-debugging style shrinker: repeatedly drop chunks while `fails`
/// stays true. `Quiesce` at the end is always kept.
pub fn shrink(mut steps: Vec<Step>, mut fails: impl FnMut(&[Step]) -> bool) -> Vec<Step> {
    let mut chunk = steps.len() / 2;
    while chunk >= 1 {
        let mut i = 0;
        while i < steps.len() {
            let end = (i + chunk).min(steps.len());
            let mut cand = steps.clone();
            cand.drain(i..end);
            if !cand.is_empty() && cand.last() == Some(&Step::Quiesce) && fails(&cand) {
                steps = cand;
            } else {
                i += chunk;
            }
        }
        chunk /= 2;
    }
    // second pass: shrink inside Round steps (drop single ops)
    for si in 0..steps.len() {
        let mut oi = 0;
        loop {
            let Step::Round { ops, .. } = &steps[si] else { break };
            if oi >= ops.len() {
                break;
            }
            let mut cand = steps.clone();
            if let Step::Round { ops, .. } = &mut cand[si] {
                ops.remove(oi);
            }
            if fails(&cand) {
                steps = cand;
            } else {
                oi += 1;
            }
        }
    }
    steps
}

/// Deterministic content bytes for a write.
pub fn content_bytes(seed: u8, len: usize) -> Vec<u8> {
    let mut r = StdRng::seed_from_u64(seed as u64 * 7919 + len as u64);
    (0..len).map(|_| r.random()).collect()
}

/// One raw commit of the wire-level stream.
#[derive(Clone, Debug)]
pub struct Wire {
    pub host: cairn_proto::HostId,
    pub commit: cairn_proto::Commit,
    /// For SetContent: Some(true) = this host holds the lease, Some(false) = nobody does.
    pub lease: Option<bool>,
    /// Resend the same commit right after (idempotency check).
    pub dup: bool,
}

/// Random raw commits from 3 hosts with stale bases and expectations over a
/// tiny name space, for the model self-test and the model-vs-server
/// differential test.
pub fn wire_stream(seed: u64, n: usize) -> Vec<Wire> {
    wire_stream_hosts(seed, n, [cairn_proto::HostId(1), cairn_proto::HostId(2), cairn_proto::HostId(3)])
}

/// Same, with the real host ids (node ids are minted under them).
pub fn wire_stream_hosts(seed: u64, n: usize, hosts: [cairn_proto::HostId; 3]) -> Vec<Wire> {
    use cairn_proto::*;
    let mut r = StdRng::seed_from_u64(seed);
    let mut counters = [0u64; 3];
    let mut opn = [0u64; 3];
    let mut known: Vec<(NodeId, Kind)> = vec![(NodeId::ROOT, Kind::Dir)];
    let names: Vec<Name> = ["a", "b", "c"].iter().map(|s| Name(s.as_bytes().to_vec())).collect();
    let mut out = vec![];
    for i in 0..n {
        let h = r.random_range(0..3);
        let host = hosts[h];
        let pick = |r: &mut StdRng, known: &Vec<(NodeId, Kind)>, dir: bool| -> NodeId {
            let c: Vec<_> = known.iter().filter(|(_, k)| !dir || *k == Kind::Dir).collect();
            c[r.random_range(0..c.len())].0
        };
        let name = names[r.random_range(0..3)].clone();
        let name2 = names[r.random_range(0..3)].clone();
        let base = Seq(r.random_range(0..=(i as u64 * 2)));
        let mk = |kind: Kind, content: Option<ContentId>| Attrs { kind, mode: 0o644, uid: 0, gid: 0, size: 0, mtime_ns: 0, ctime_ns: 0, atime_ns: 0, rdev: 0, content, symlink_target: None, xattrs: vec![] };
        let mut lease = None;
        let op = match r.random_range(0..9) {
            0 | 1 => {
                counters[h] += 1;
                let node = NodeId::new(host, counters[h]);
                let kind = if r.random_bool(0.3) { Kind::Dir } else { Kind::File };
                let content = if kind == Kind::File { Some(if r.random_bool(0.5) { ContentId::Empty } else { ContentId::Blob(Hash::of(&[r.random_range(0..2u8)])) }) } else { None };
                known.push((node, kind));
                Op::Create { parent: pick(&mut r, &known, true), name, node, attrs: mk(kind, content) }
            }
            2 => Op::Link { parent: pick(&mut r, &known, true), name, node: pick(&mut r, &known, false) },
            3 => Op::Unlink { parent: pick(&mut r, &known, true), name, node: pick(&mut r, &known, false), base },
            4 | 5 => Op::Rename {
                src_parent: pick(&mut r, &known, true),
                src_name: name,
                node: pick(&mut r, &known, false),
                dst_parent: pick(&mut r, &known, true),
                dst_name: name2,
                replaces: if r.random_bool(0.3) { Some(pick(&mut r, &known, false)) } else { None },
                mode: [RenameMode::Replace, RenameMode::NoReplace, RenameMode::Exchange][r.random_range(0..3)],
            },
            6 => Op::SetAttr { node: pick(&mut r, &known, false), base, changes: AttrChanges { mode: Some(r.random_range(0..0o777)), ..Default::default() } },
            7 => Op::SetXattr { node: pick(&mut r, &known, false), name: b"user.x".to_vec(), value: if r.random_bool(0.5) { Some(vec![1]) } else { None } },
            _ => {
                lease = Some(r.random_bool(0.3));
                Op::SetContent { node: pick(&mut r, &known, false), base, content: ContentId::Blob(Hash::of(&[r.random_range(0..3u8)])), size: 1, mtime_ns: 1 }
            }
        };
        opn[h] += 1;
        let commit = Commit { op_id: OpId { host, n: opn[h] }, cred: Cred { uid: 0, gid: 0 }, op };
        out.push(Wire { host, commit, lease, dup: r.random_bool(0.1) });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::view;
    use cairn_proto::*;

    fn run(seed: u64) {
        let mut m = Model::new();
        let ts = "20261113T000000";
        for (i, w) in wire_stream(seed, 400).iter().enumerate() {
            if let Some(g) = w.lease {
                if let Op::SetContent { node, .. } = &w.commit.op {
                    m.set_lease(*node, if g { Some(w.host) } else { None });
                }
            }
            let commit = &w.commit;
            let _ = m.apply(w.host, commit, &Ctx { ts });
            if w.dup {
                let before = m.head();
                let _ = m.apply(w.host, commit, &Ctx { ts });
                assert_eq!(before, m.head(), "seed {seed}: duplicate op applied twice");
            }
            let replayed = view::replay(&m.changes);
            let rows = view::from_rows(&m.rows());
            if let Some(d) = view::diff("replay", &replayed, &[], "model", &rows, &[]) {
                panic!("seed {seed} step {i}: replay != model after {:?}\n{d}", commit);
            }
            for ((p, _), (n, _)) in &rows {
                let pa = rows.iter().find(|(_, (x, _))| x == p);
                assert!(*p == NodeId::ROOT || pa.map_or(false, |(_, (_, a))| a.kind == Kind::Dir), "seed {seed}: dangling parent {p:?}");
                let mut cur = *p;
                for _ in 0..100 {
                    if cur == NodeId::ROOT { break; }
                    assert_ne!(cur, *n, "seed {seed}: cycle");
                    cur = rows.iter().find(|(_, (x, _))| *x == cur).map(|((pp, _), _)| *pp).unwrap_or(NodeId::ROOT);
                }
            }
        }
    }

    #[test]
    fn model_replay_invariant() {
        for seed in 0..300 {
            run(seed);
        }
    }

    #[test]
    fn shrinker_minimizes() {
        let steps: Vec<Step> = (0..50).map(|i| Step::Op(0, SimOp::Create(format!("f{i}")))).chain([Step::Quiesce]).collect();
        let min = shrink(steps, |s| s.iter().any(|x| *x == Step::Op(0, SimOp::Create("f7".into()))));
        assert_eq!(min.len(), 2);
    }
}
