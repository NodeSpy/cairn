//! Differential test: the reference model (written from PROTOCOL.md) versus
//! the server's merge engine (`cairn_server::merge` on `MemTree`) on the same
//! random commit stream. Any disagreement is a bug in one of them.

use cairn_proto::*;
use cairn_server::merge::{self, mem::MemTree, Ctx as SCtx, Rules};
use cairn_testkit::gen::wire_stream;
use cairn_testkit::model::{classify, Ctx, Model};
use cairn_testkit::view;

fn run(seed: u64, n: usize) -> Result<(), String> {
    let mut m = Model::new();
    m.last_mod_only = true; // server tracks only the last modifier; see report
    let mut t = MemTree::new();
    let rules = Rules::default();
    let names = ["", "h1", "h2", "h3"];
    for (i, w) in wire_stream(seed, n).iter().enumerate() {
        let node = match &w.commit.op {
            Op::SetContent { node, .. } => Some(*node),
            _ => None,
        };
        if let (Some(g), Some(node)) = (w.lease, node) {
            m.set_lease(node, if g { Some(w.host) } else { None });
            if g { t.leases.insert(node, w.host); } else { t.leases.remove(&node); }
        }
        let sctx = SCtx { host: w.host, host_name: names[w.host.0 as usize], rules: &rules, now_secs: 1_700_000_000 };
        let so = merge::apply(&mut t, &sctx, &w.commit);
        let mp = m.apply(w.host, &w.commit, &Ctx { ts: "x" });
        if !mp.matches(&so.result) {
            return Err(format!("seed {seed} step {i}: {:?}\n  server: {:?}\n  model:  {:?} (lax={})", w.commit.op, so.result, mp.result, mp.lax));
        }
        if so.changes.len() != mp.changes.len() && !so.replay {
            return Err(format!("seed {seed} step {i}: {:?}\n  server emitted {} changes {:?}\n  model emitted {} changes {:?}", w.commit.op, so.changes.len(), so.changes.iter().map(|c| &c.effect).collect::<Vec<_>>(), mp.changes.len(), mp.changes.iter().map(|c| &c.effect).collect::<Vec<_>>()));
        }
        let srows: Vec<SnapshotRow> = t.rows().into_iter().map(|(parent, name, node, attrs)| SnapshotRow { parent, name, node, attrs }).collect();
        let (sns, mns) = (view::from_rows(&srows), view::from_rows(&m.rows()));
        let smin = view::minted_order(&t.log);
        let mmin = view::minted_order(&m.changes);
        if let Some(d) = view::diff("server", &sns, &smin, "model", &mns, &mmin) {
            return Err(format!("seed {seed} step {i}: namespace differs after {:?} (server {:?}, model {:?}):\n{d}", w.commit.op, classify(&so.result), classify(&mp.result)));
        }
    }
    Ok(())
}

#[test]
fn model_vs_server_merge() {
    let mut fails = vec![];
    for seed in 0..std::env::var("SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(300u64) {
        if let Err(e) = run(seed, 300) {
            fails.push(e);
            if fails.len() >= 5 { break; }
        }
    }
    assert!(fails.is_empty(), "{} disagreements; first ones:\n{}", fails.len(), fails.join("\n---\n"));
}
