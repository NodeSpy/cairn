//! `Env` over the real thing: in-process `TestServer`, one fault proxy per
//! client, and `cairn_client::Client`s with `DirectApplier` (no FUSE).

use crate::proxy::Proxy;
use crate::sim::{AppliedCommit, Env, SimServer};
use cairn_client::api::Core;
use cairn_client::testing::DirectApplier;
use cairn_client::{Client, Config, RootConfig};
use cairn_proto::*;
use cairn_server::testing::TestServer;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const ROOT: &str = "proj";

/// Server + root name, implementing `SimServer`.
pub struct OwnedServer {
    pub ts: TestServer,
    pub root: String,
}

impl SimServer for OwnedServer {
    fn snapshot(&self) -> Vec<SnapshotRow> {
        self.ts.snapshot(&self.root)
    }
    fn change_log(&self) -> Vec<Change> {
        self.ts.change_log(&self.root)
    }
    fn conflicts(&self) -> Vec<ConflictRecord> {
        self.ts.conflicts(&self.root)
    }
    fn commit_log(&self) -> Vec<AppliedCommit> {
        self.ts.commit_log(&self.root).into_iter().map(|(host, commit, result)| AppliedCommit { host, commit, result }).collect()
    }
    fn live_contents(&self) -> Vec<ContentId> {
        self.ts.live_content(&self.root)
    }
    fn has_content(&self, id: &ContentId) -> bool {
        self.ts.has_content(*id)
    }
}

pub struct RealEnv {
    // drop order: clients first, then proxies/runtime, then the server, then the dir
    clients: Vec<Option<Client>>,
    cores: Vec<Arc<dyn Core>>,
    proxies: Vec<Proxy>,
    rt: tokio::runtime::Runtime,
    srv: OwnedServer,
    caches: Vec<PathBuf>,
    creds: Vec<PathBuf>,
    _dir: tempfile::TempDir,
}

impl RealEnv {
    pub fn new(n: usize) -> RealEnv {
        let dir = tempfile::tempdir().unwrap();
        let mut ts = TestServer::start(dir.path());
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let creds: Vec<PathBuf> = names.iter().map(|nm| ts.add_host(nm)).collect();
        let acl: Vec<(&str, bool)> = names.iter().map(|nm| (nm.as_str(), true)).collect();
        ts.add_root(ROOT, &acl);
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let proxies: Vec<Proxy> = (0..n).map(|_| rt.block_on(Proxy::start(ts.addr())).unwrap()).collect();
        let caches: Vec<PathBuf> = (0..n).map(|i| dir.path().join(format!("cache{i}"))).collect();
        let mut env = RealEnv { clients: vec![], cores: vec![], proxies, rt, srv: OwnedServer { ts, root: ROOT.into() }, caches, creds, _dir: dir };
        for i in 0..n {
            let (c, core) = env.open_client(i);
            env.clients.push(Some(c));
            env.cores.push(core);
        }
        env
    }

    fn open_client(&self, i: usize) -> (Client, Arc<dyn Core>) {
        let cfg = Config::new(&self.proxies[i].addr().to_string(), &self.creds[i], &self.caches[i]).with_root(ROOT, RootConfig::default());
        let c = Client::open(cfg).expect("Client::open");
        let core = c.core(ROOT).expect("core");
        DirectApplier::install(&core);
        (c, core)
    }
}

impl Env for RealEnv {
    fn clients(&self) -> usize {
        self.clients.len()
    }
    fn core(&self, i: usize) -> Arc<dyn Core> {
        self.cores[i].clone()
    }
    fn partition(&mut self, i: usize) {
        self.proxies[i].partition();
    }
    fn heal(&mut self, i: usize) {
        self.proxies[i].heal();
    }
    fn reset(&mut self, i: usize) {
        self.proxies[i].reset_all();
    }
    fn delay(&mut self, i: usize, ms: u64) {
        self.proxies[i].set_delay(ms);
    }
    fn kill_client(&mut self, i: usize) {
        self.clients[i] = None; // Drop without graceful shutdown of the server link
        let (c, core) = self.open_client(i);
        self.clients[i] = Some(c);
        self.cores[i] = core;
    }
    fn server_restart(&mut self) {
        self.srv.ts.crash_restart();
        for p in &self.proxies {
            p.set_target(self.srv.ts.addr());
        }
    }
    fn sync_client(&mut self, i: usize) -> Result<(), String> {
        let c = self.clients[i].as_ref().unwrap();
        if c.wait_synced(Duration::from_secs(20)) {
            Ok(())
        } else {
            Err(format!("not synced after 20s (pending {}, connected {})", c.root(ROOT).unwrap().pending_len(), c.is_connected()))
        }
    }
    fn quiesce(&mut self) -> Result<(), String> {
        for p in &self.proxies {
            p.heal();
            p.set_delay(0);
            p.cut_after(-1);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let head = self.srv.ts.head(ROOT);
            let mut bad = vec![];
            for (i, c) in self.clients.iter().enumerate() {
                let c = c.as_ref().unwrap();
                let r = c.root(ROOT).unwrap();
                if !c.wait_synced(Duration::from_millis(50)) {
                    bad.push(format!("client {i}: not synced (pending {}, uploads {}, connected {})", r.pending_len(), c.uploads_pending(), c.is_connected()));
                } else if r.applied_seq() < head {
                    bad.push(format!("client {i}: applied_seq {:?} < server head {:?}", r.applied_seq(), head));
                }
            }
            if bad.is_empty() {
                for c in &self.clients {
                    c.as_ref().unwrap().settle();
                }
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("stuck after 30s: {}", bad.join("; ")));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    fn server(&self) -> &dyn SimServer {
        &self.srv
    }
}
