//! E10: randomized multi-client simulation driver and invariant checks.
//!
//! The driver is generic over `Env`, which owns the server, the per-client
//! fault proxies and the clients (`cairn_client::Client` with
//! `DirectApplier`). The real `Env` lives in `real.rs` (feature `real`) and
//! is wired once `TestServer` / `Client::open` land.

use crate::gen::*;
use crate::model::{self, Ctx, Model};
use crate::view::{self, Ns};
use cairn_client::api::*;
use cairn_proto::*;
use std::collections::HashMap;
use std::sync::Arc;

pub struct AppliedCommit {
    pub host: HostId,
    pub commit: Commit,
    pub result: CommitResult,
}

/// What the harness needs from the in-process server.
pub trait SimServer {
    fn snapshot(&self) -> Vec<SnapshotRow>;
    fn change_log(&self) -> Vec<Change>;
    fn conflicts(&self) -> Vec<ConflictRecord>;
    /// In server application order; duplicates (replayed op_ids) excluded.
    fn commit_log(&self) -> Vec<AppliedCommit>;
    fn live_contents(&self) -> Vec<ContentId>;
    fn has_content(&self, id: &ContentId) -> bool;
}

pub trait Env {
    fn clients(&self) -> usize;
    fn core(&self, i: usize) -> Arc<dyn Core>;
    fn partition(&mut self, i: usize);
    fn heal(&mut self, i: usize);
    fn reset(&mut self, i: usize);
    fn delay(&mut self, i: usize, ms: u64);
    /// Drop the client without shutdown; reopen from the same cache dir.
    fn kill_client(&mut self, i: usize);
    fn server_restart(&mut self);
    /// Wait until client `i` has nothing pending and is connected (its commits are acked).
    fn sync_client(&mut self, i: usize) -> Result<(), String>;
    /// Heal all links, wait until every client has an empty outbound queue,
    /// no stuck intents, and has applied the server head. Err = stuck
    /// (message says what).
    fn quiesce(&mut self) -> Result<(), String>;
    fn server(&self) -> &dyn SimServer;
}

// ----------------------------------------------------------------- executing ops

fn split(p: &str) -> (Vec<&str>, &str) {
    let mut v: Vec<&str> = p.split('/').collect();
    let last = v.pop().unwrap();
    (v, last)
}

fn walk(core: &dyn Core, dirs: &[&str]) -> FsResult<Ino> {
    let mut ino = ROOT_INO;
    for d in dirs {
        ino = core.lookup(ino, d.as_bytes())?.ino;
    }
    Ok(ino)
}

fn local() -> Origin {
    Origin::Local { uid: 0, gid: 0, pid: 1 }
}

fn newnode(kind: Kind, mode: u32, target: Option<&str>) -> NewNode {
    NewNode { kind, mode, uid: 0, gid: 0, rdev: 0, symlink_target: target.map(|t| t.as_bytes().to_vec()), xattrs: vec![] }
}

/// Run one op against a Core. Errors are normal (ENOENT etc.).
pub fn exec(core: &dyn Core, op: &SimOp) -> FsResult<()> {
    use std::io::{Seek, Write};
    match op {
        SimOp::Create(p) | SimOp::Mkdir(p) | SimOp::Symlink(p, _) => {
            let (d, n) = split(p);
            let parent = walk(core, &d)?;
            let new = match op {
                SimOp::Create(_) => newnode(Kind::File, 0o644, None),
                SimOp::Mkdir(_) => newnode(Kind::Dir, 0o755, None),
                SimOp::Symlink(_, t) => newnode(Kind::Symlink, 0o777, Some(t)),
                _ => unreachable!(),
            };
            core.create(local(), parent, n.as_bytes(), new).map(|_| ())
        }
        SimOp::Link(from, to) => {
            let (fd, fnm) = split(from);
            let src = core.lookup(walk(core, &fd)?, fnm.as_bytes())?;
            let (td, tn) = split(to);
            core.link(local(), src.ino, walk(core, &td)?, tn.as_bytes()).map(|_| ())
        }
        SimOp::Unlink(p) => {
            let (d, n) = split(p);
            core.unlink(local(), walk(core, &d)?, n.as_bytes())
        }
        SimOp::Rmdir(p) => {
            let (d, n) = split(p);
            core.rmdir(local(), walk(core, &d)?, n.as_bytes())
        }
        SimOp::Rename(a, b, mode) => {
            let (ad, an) = split(a);
            let (bd, bn) = split(b);
            core.rename(local(), walk(core, &ad)?, an.as_bytes(), walk(core, &bd)?, bn.as_bytes(), *mode)
        }
        SimOp::Chmod(p, mode) => {
            let (d, n) = split(p);
            let ino = core.lookup(walk(core, &d)?, n.as_bytes())?.ino;
            core.setattr(local(), ino, AttrChanges { mode: Some(*mode), ..Default::default() }, None).map(|_| ())
        }
        SimOp::SetXattr(p, name, val) => {
            let (d, n) = split(p);
            let ino = core.lookup(walk(core, &d)?, n.as_bytes())?.ino;
            match val {
                Some(v) => core.setxattr(local(), ino, name.as_bytes(), v, XattrFlags::Any),
                None => core.removexattr(local(), ino, name.as_bytes()),
            }
        }
        SimOp::Write(p, seed, len) => {
            let (d, n) = split(p);
            let ino = core.lookup(walk(core, &d)?, n.as_bytes())?.ino;
            let mut f = core.open(local(), ino, true, true)?;
            let r = (|| -> std::io::Result<()> {
                f.file.seek(std::io::SeekFrom::Start(0))?;
                f.file.write_all(&content_bytes(*seed, *len))
            })();
            let rel = core.release(ino, f.token);
            r.map_err(|_| Errno::EIO)?;
            rel
        }
    }
}

// ----------------------------------------------------------------- checking

/// A client's visible tree as `Ns`.
pub fn client_view(core: &dyn Core) -> Result<Ns, String> {
    fn rec(core: &dyn Core, dir: Ino, dnode: NodeId, ns: &mut Ns, depth: usize) -> Result<(), String> {
        let snap = core.readdir(dir).map_err(|e| format!("readdir {dir}: errno {}", e.0))?;
        for e in &snap.entries {
            let info = core.getattr(e.ino).map_err(|x| format!("getattr {}: errno {}", e.ino, x.0))?;
            let mut a = info.attrs.clone();
            a.xattrs.sort();
            ns.insert((dnode, e.name.clone()), (info.node, view::nattrs(&a)));
            if e.kind == Kind::Dir && depth < 16 {
                rec(core, e.ino, info.node, ns, depth + 1)?;
            }
        }
        Ok(())
    }
    let mut ns = Ns::new();
    let root = core.getattr(ROOT_INO).map_err(|e| format!("getattr root: errno {}", e.0))?;
    rec(core, ROOT_INO, root.node, &mut ns, 0)?;
    Ok(ns)
}

/// Server-side checks: snapshot == replay(change log); server results and
/// namespace agree with the reference model replaying the commit log;
/// conflict log completeness; content reachability.
pub fn check_server(s: &dyn SimServer) -> Result<(), String> {
    let log = s.change_log();
    for w in log.windows(2) {
        if w[1].seq <= w[0].seq {
            return Err(format!("change log not strictly increasing at {:?}", w[1].seq));
        }
    }
    let snap = view::from_rows(&s.snapshot());
    let minted = view::minted_order(&log);
    let replayed = view::replay(&log);
    if let Some(d) = view::diff("snapshot", &snap, &minted, "replay(change_log)", &replayed, &minted) {
        return Err(format!("server snapshot != replay of its change log:\n{d}"));
    }

    // reference model
    let mut m = Model::new();
    let ts = "20000101T000000";
    let commits = s.commit_log();
    for (i, ac) in commits.iter().enumerate() {
        let pred = predict(&mut m, ac, &ts);
        if !pred.matches(&ac.result) {
            return Err(format!(
                "commit #{i} {:?} by host {}: server answered {:?} (class {:?}), model predicted {:?} (class {:?})",
                ac.commit.op, ac.host.0, ac.result, model::classify(&ac.result), pred.result, model::classify(&pred.result)
            ));
        }
    }
    let mrows = view::from_rows(&m.rows());
    // model-minted ids are host-0 counters in order; canon both with their own orders
    let mmin: Vec<NodeId> = (2..=65).map(|i| NodeId::new(HostId(0), i)).collect();
    if let Some(d) = view::diff("server", &snap, &minted, "model", &mrows, &mmin) {
        return Err(format!("server namespace != reference model:\n{d}"));
    }

    // conflict log: one record per non-Applied, non-Failed result
    let recs: HashMap<OpId, usize> = s.conflicts().iter().fold(HashMap::new(), |mut a, c| {
        *a.entry(c.op_id).or_default() += 1;
        a
    });
    for ac in &commits {
        if matches!(model::classify(&ac.result), model::Class::Applied | model::Class::Failed(_)) {
            continue;
        }
        if !recs.contains_key(&ac.commit.op_id) {
            return Err(format!("no conflict-log record for {:?} ({:?})", ac.commit.op_id, ac.result));
        }
    }
    // content: every id referenced by a live node is present on the server
    // (conflict copies keep losers reachable, see model).
    for id in s.live_contents() {
        if !s.has_content(&id) {
            return Err(format!("live content {id:?} missing on server (lost data)"));
        }
    }
    let live: std::collections::HashSet<_> = m.live_contents().into_iter().collect();
    for ac in &commits {
        if let (Op::SetContent { content, .. }, model::Class::ContentConflictCopy) = (&ac.commit.op, model::classify(&ac.result)) {
            if !live.contains(content) && !s.conflicts().iter().any(|c| c.op_id == ac.commit.op_id) {
                return Err(format!("conflict-copy content {content:?} neither reachable nor logged"));
            }
        }
    }
    Ok(())
}

fn predict(m: &mut Model, ac: &AppliedCommit, ts: &str) -> model::Predicted {
    let ctx = Ctx { ts };
    if let Op::SetContent { node, .. } = &ac.commit.op {
        // Leases are not observable from outside: try none / this host / another host.
        let mut first = None;
        for lease in [None, Some(ac.host), Some(HostId(u64::MAX))] {
            let mut t = m.clone();
            t.set_lease(*node, lease);
            let p = t.apply(ac.host, &ac.commit, &ctx);
            if p.matches(&ac.result) {
                *m = t;
                return p;
            }
            first.get_or_insert((t, p));
        }
        let (t, p) = first.unwrap();
        *m = t;
        return p;
    }
    m.apply(ac.host, &ac.commit, &ctx)
}

pub fn check_clients(env: &dyn Env) -> Result<(), String> {
    let s = env.server();
    let log = s.change_log();
    let minted = view::minted_order(&log);
    let snap = view::from_rows(&s.snapshot());
    for i in 0..env.clients() {
        let v = client_view(&*env.core(i)).map_err(|e| format!("client {i}: {e}"))?;
        if let Some(d) = view::diff(&format!("client{i}"), &v, &minted, "server", &snap, &minted) {
            return Err(format!("client {i} view != server snapshot:\n{d}"));
        }
    }
    Ok(())
}

pub fn check_all(env: &mut dyn Env) -> Result<(), String> {
    env.quiesce().map_err(|e| format!("quiesce: {e}"))?;
    if std::env::var("VERBOSE").is_ok() {
        let s = env.server();
        eprintln!("quiesced: commits={} changes={} conflicts={} rows={}", s.commit_log().len(), s.change_log().len(), s.conflicts().len(), s.snapshot().len());
    }
    check_server(env.server())?;
    check_clients(env)
}

// ----------------------------------------------------------------- driver

pub struct Failure {
    pub seed: u64,
    pub step: usize,
    pub msg: String,
    pub trace: Vec<Step>,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "E10 FAILURE seed={} at step {}: {}", self.seed, self.step, self.msg)?;
        writeln!(f, "minimized trace ({} steps):", self.trace.len())?;
        for (i, s) in self.trace.iter().enumerate() {
            writeln!(f, "  {i:3}: {s:?}")?;
        }
        Ok(())
    }
}

/// Execute steps against `env`; stops at the first failed check.
pub fn run_steps(env: &mut dyn Env, steps: &[Step]) -> Result<(), (usize, String)> {
    // clients must be connected (and have listed the root) before the first op
    env.quiesce().map_err(|e| (0, format!("initial connect: {e}")))?;
    for (i, s) in steps.iter().enumerate() {
        match s {
            Step::Op(c, op) => {
                let r = exec(&*env.core(*c % env.clients()), op);
                if std::env::var("VERBOSE").is_ok() {
                    eprintln!("  c{c} {op:?} -> {:?}", r.err().map(|e| e.0));
                }
            }
            Step::Fault(f) => match f {
                Fault::Partition(c) => env.partition(*c),
                Fault::Heal(c) => env.heal(*c),
                Fault::KillClient(c) => env.kill_client(*c),
                Fault::ServerRestart => env.server_restart(),
                Fault::Reset(c) => env.reset(*c),
                Fault::Delay(c, ms) => env.delay(*c, *ms),
            },
            Step::Quiesce => check_all(env).map_err(|e| (i, e))?,
            Step::Round { ops, heal } => {
                let n = env.clients();
                for c in 0..n {
                    env.partition(c);
                }
                for (c, op) in ops {
                    let r = exec(&*env.core(*c % n), op);
                    if std::env::var("VERBOSE").is_ok() {
                        eprintln!("  c{c} {op:?} -> {:?}", r.err().map(|e| e.0));
                    }
                }
                for c in heal {
                    env.heal(*c % n);
                    env.sync_client(*c % n).map_err(|e| (i, format!("round sync client {c}: {e}")))?;
                }
                check_all(env).map_err(|e| (i, e))?
            }
        }
    }
    Ok(())
}

/// Run one seed; on failure shrink (re-running with fresh envs) and report.
pub fn run_seed<E: Env>(seed: u64, cfg: &GenCfg, mut mk_env: impl FnMut() -> E) -> Result<(), Failure> {
    let steps = gen_steps(seed, cfg);
    let mut env = mk_env();
    match run_steps(&mut env, &steps) {
        Ok(()) => Ok(()),
        Err((step, msg)) => {
            eprintln!("E10 seed {seed}: first failure at step {step}: {msg}\n(shrinking...)");
            drop(env);
            let trace = shrink(steps[..=step].to_vec(), |cand| {
                let mut e = mk_env();
                run_steps(&mut e, cand).is_err()
            });
            // re-run the minimized trace for its message
            let mut e = mk_env();
            let msg = run_steps(&mut e, &trace).err().map(|x| x.1).unwrap_or(msg);
            Err(Failure { seed, step, msg, trace })
        }
    }
}

/// `SimServer` over the in-process `cairn_server::testing::TestServer`.
pub struct ServerAdapter<'a>(pub &'a cairn_server::testing::TestServer, pub &'a str);

impl SimServer for ServerAdapter<'_> {
    fn snapshot(&self) -> Vec<SnapshotRow> {
        self.0.snapshot(self.1)
    }
    fn change_log(&self) -> Vec<Change> {
        self.0.change_log(self.1)
    }
    fn conflicts(&self) -> Vec<ConflictRecord> {
        self.0.conflicts(self.1)
    }
    fn commit_log(&self) -> Vec<AppliedCommit> {
        self.0.commit_log(self.1).into_iter().map(|(host, commit, result)| AppliedCommit { host, commit, result }).collect()
    }
    fn live_contents(&self) -> Vec<ContentId> {
        self.0.live_content(self.1)
    }
    fn has_content(&self, id: &ContentId) -> bool {
        self.0.has_content(*id)
    }
}
