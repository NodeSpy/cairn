//! Real server (in-process `TestServer`, TLS over loopback) driven by three
//! raw protocol clients through fault-injecting proxies, with random wire-level
//! commits. Faults: connection resets, mid-frame cuts, partitions, server
//! crash+restart. Commits are retried with the same op_id (idempotency).
//! Afterwards `check_server`: snapshot == replay(change log), results and
//! namespace == reference model replaying the server's commit order.
//!
//! SEEDS=n (default 8) FAULTS=0 to disable faults.

use cairn_proto::*;
use cairn_server::testing::{TestClient, TestServer};
use cairn_testkit::gen::{wire_stream_hosts, Wire};
use cairn_testkit::proxy::Proxy;
use cairn_testkit::sim::{check_server, ServerAdapter};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn connect(addr: std::net::SocketAddr, creds: &std::path::Path) -> Option<(TestClient, RootHandle)> {
    let c = tokio::time::timeout(Duration::from_secs(5), TestClient::connect(addr, creds)).await.ok()?.ok()?;
    let (h, _, _) = tokio::time::timeout(Duration::from_secs(5), c.mount("proj")).await.ok()?.ok()?;
    Some((c, h))
}

async fn run_seed(seed: u64, faults: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let mut srv = TestServer::start(dir.path());
    let names = ["a", "b", "c"];
    let creds: Vec<_> = names.iter().map(|n| srv.add_host(n)).collect();
    let hosts = [srv.host_id("a"), srv.host_id("b"), srv.host_id("c")];
    srv.add_root("proj", &[("a", true), ("b", true), ("c", true)]);
    let addr = srv.addr();
    // upload the blobs the generator references, directly
    {
        let (c, _) = connect(addr, &creds[0]).await.ok_or("setup connect")?;
        for k in 0..3u8 {
            c.put_blob(&[k]).await.map_err(|e| format!("put_blob: {e:?}"))?;
        }
    }
    let srv = Arc::new(Mutex::new(srv));
    let stream = wire_stream_hosts(seed, 240, hosts);
    let mut proxies = vec![];
    for _ in 0..3 {
        proxies.push(Arc::new(Proxy::start(addr).await.unwrap()));
    }
    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let returned: Arc<Mutex<HashMap<OpId, CommitResult>>> = Default::default();
    let progress: Arc<Vec<std::sync::atomic::AtomicUsize>> = Arc::new((0..3).map(|_| Default::default()).collect());
    let mut tasks = vec![];
    for i in 0..3 {
        let mine: Vec<Wire> = stream.iter().filter(|w| w.host == hosts[i]).cloned().collect();
        let (px, cr, done, returned, progress) = (proxies[i].clone(), creds[i].clone(), done.clone(), returned.clone(), progress.clone());
        tasks.push(tokio::spawn(async move {
            let mut r = StdRng::seed_from_u64(seed * 31 + i as u64);
            let mut conn: Option<(TestClient, RootHandle)> = None;
            let mut idx = 0;
            while idx < mine.len() {
                let n = r.random_range(1..=3).min(mine.len() - idx);
                let batch: Vec<Commit> = mine[idx..idx + n].iter().map(|w| w.commit.clone()).collect();
                if conn.is_none() {
                    conn = connect(px.addr(), &cr).await;
                    if conn.is_none() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                }
                let (c, h) = conn.as_ref().unwrap();
                match tokio::time::timeout(Duration::from_secs(10), c.commit(*h, batch.clone())).await {
                    Ok(Ok(res)) => {
                        let mut g = returned.lock().unwrap();
                        for (cm, rs) in batch.iter().zip(res) {
                            g.insert(cm.op_id, rs);
                        }
                        idx += n;
                        progress[i].store(idx, std::sync::atomic::Ordering::SeqCst);
                    }
                    other => {
                        if std::env::var("DBG").is_ok() { eprintln!("client {i} idx {idx} retry: {:?} first op {:?}", other.as_ref().map(|r| r.as_ref().err().cloned()), batch[0].op); }
                        conn = None; // retry the same op_ids on a fresh connection
                    }
                }
            }
            done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));
    }
    // fault injector
    let inj = {
        let (proxies, srv, done) = (proxies.clone(), srv.clone(), done.clone());
        tokio::spawn(async move {
            let mut r = StdRng::seed_from_u64(seed ^ 0xfa17);
            while done.load(std::sync::atomic::Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(r.random_range(80..300))).await;
                if !faults {
                    continue;
                }
                let p = &proxies[r.random_range(0..3)];
                match r.random_range(0..10) {
                    0..=2 => p.reset_all(),
                    3..=5 => p.cut_after(r.random_range(0..3000)),
                    6..=7 => {
                        p.partition();
                        tokio::time::sleep(Duration::from_millis(r.random_range(20..200))).await;
                        p.heal();
                    }
                    8 => p.set_delay(r.random_range(0..5)),
                    _ => {
                        let srv = srv.clone();
                        tokio::task::spawn_blocking(move || srv.lock().unwrap().crash_restart()).await.unwrap();
                    }
                }
            }
        })
    };
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(40), t).await.map_err(|_| format!("client task stuck; progress {:?}/{:?} server running={}", progress.iter().map(|p| p.load(std::sync::atomic::Ordering::SeqCst)).collect::<Vec<_>>(), stream.len() / 3, srv.lock().map(|s| s.is_running()).unwrap_or(false)))?.map_err(|e| e.to_string())?;
    }
    inj.await.unwrap();
    let srv = srv.lock().unwrap();
    check_server(&ServerAdapter(&srv, "proj"))?;
    // results returned to clients agree with the server's commit log
    let log = srv.commit_log("proj");
    let ret = returned.lock().unwrap();
    for (_, c, res) in &log {
        if let Some(got) = ret.get(&c.op_id) {
            if got != res {
                return Err(format!("client saw {got:?} for {:?} but commit_log has {res:?}", c.op_id));
            }
        }
    }
    let applied: std::collections::HashSet<_> = log.iter().map(|(_, c, _)| c.op_id).collect();
    for w in &stream {
        if !applied.contains(&w.commit.op_id) {
            return Err(format!("commit {:?} acked/retried but missing from server commit log", w.commit.op_id));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_server_vs_model() {
    let n: u64 = std::env::var("SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let faults = std::env::var("FAULTS").map(|v| v != "0").unwrap_or(true);
    let mut fails = vec![];
    for seed in 0..n {
        if let Err(e) = run_seed(seed, faults).await {
            fails.push(format!("seed {seed}: {e}"));
        }
    }
    assert!(fails.is_empty(), "{} failing seeds:\n{}", fails.len(), fails.join("\n---\n"));
}
