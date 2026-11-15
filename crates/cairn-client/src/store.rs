//! The state DB (SQLite WAL) behind the op log.
//!
//! All reads happen once at startup (`load`); afterwards the in-memory state is authoritative.
//! Every mutation is a [`DbOp`]: `send` appends it to the op log (`oplog.rs`, the WAL: written
//! with `write(2)` at the next sync point, before the FUSE reply) and queues it for the
//! checkpointer thread, which folds batches into SQLite (~5 ms / 500 ops) and records in the same
//! transaction how far into the log it got (`ckpt_lsn`). At startup SQLite recovers first, then
//! the log tail past `ckpt_lsn` is replayed into it.

use anyhow::{Context, Result};
use cairn_proto::frame::{decode, encode};
use cairn_proto::*;
use crate::oplog::OpLog;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::time::{Duration, Instant};

/// Journaled pipeline state of a file whose content is not yet in the CAS (docs/CLIENT.md
/// "Staged write pipeline"). Absent row = committed (nothing to do).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRow {
    pub ino: u64,
    /// `Stage` as u8: 0 dirty, 1 hashed, 2 published, 3 uploaded
    pub stage: u8,
    pub content: Option<ContentId>,
    pub size: u64,
    pub mtime_ns: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DbOp {
    Meta(String, Vec<u8>),
    Node { root: String, node: NodeId, row: Option<(Seq, Seq, Attrs)> },
    Dirent { root: String, parent: NodeId, name: Vec<u8>, node: Option<NodeId> },
    Listed { root: String, node: NodeId, listed: bool },
    Ino { root: String, node: NodeId, ino: u64, gen: u64 },
    InoDel { root: String, node: NodeId },
    Outbound { root: String, n: u64, row: Option<(Op, Cred, i64)> },
    Intent { root: String, seq: u64, change: Option<Change> },
    Upload { content: ContentId, add: bool },
    Pin { hash: Hash, add: bool },
    LocalNode { root: String, node: NodeId, attrs: Option<Attrs> },
    LocalDirent { root: String, parent: NodeId, name: Vec<u8>, node: Option<NodeId> },
    /// Pipeline state of a node's live file (`None` = committed).
    File { root: String, node: NodeId, row: Option<FileRow> },
    /// Live file `ino` is garbage and waits for the reaper (deferred delete).
    Reap { root: String, ino: u64, add: bool },
}

enum Msg {
    Op(DbOp, u64),
    Flush(SyncSender<()>),
    Stop,
}

#[derive(Default)]
pub struct LoadedRoot {
    pub epoch: Option<Epoch>,
    pub applied_seq: Seq,
    pub nodes: Vec<(NodeId, Seq, Seq, Attrs)>,
    pub dirents: Vec<(NodeId, Vec<u8>, NodeId)>,
    pub listed: Vec<NodeId>,
    pub inos: Vec<(NodeId, u64, u64)>,
    pub outbound: Vec<(u64, Op, Cred, i64)>,
    pub intents: Vec<(u64, Change)>,
    pub local_nodes: Vec<(NodeId, Attrs)>,
    pub local_dirents: Vec<(NodeId, Vec<u8>, NodeId)>,
    pub files: Vec<(NodeId, FileRow)>,
    pub reaps: Vec<u64>,
}

#[derive(Default)]
pub struct LoadedGlobal {
    pub host_id: Option<u64>,
    pub node_ctr: u64,
    pub op_ctr: u64,
    pub uploads: Vec<ContentId>,
    pub pins: Vec<Hash>,
    pub roots: HashMap<String, LoadedRoot>,
}

pub struct Db {
    tx: Sender<Msg>,
    pub log: Arc<OpLog>,
    writer: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// Rotate the op log (start an empty file) once it is this big and fully absorbed by SQLite.
const LOG_ROTATE_BYTES: u64 = 64 << 20;

fn nb(n: NodeId) -> [u8; 16] {
    n.0.to_be_bytes()
}
fn bn(b: Vec<u8>) -> NodeId {
    let mut a = [0u8; 16];
    a.copy_from_slice(&b);
    NodeId(u128::from_be_bytes(a))
}
fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    encode(v).expect("encode")
}
fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T> {
    Ok(decode(b)?)
}
fn u64le(v: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a[..v.len().min(8)].copy_from_slice(&v[..v.len().min(8)]);
    u64::from_le_bytes(a)
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS nodes(root TEXT, node BLOB, version INTEGER, cversion INTEGER, attrs BLOB, PRIMARY KEY(root,node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS dirents(root TEXT, parent BLOB, name BLOB, node BLOB, PRIMARY KEY(root,parent,name)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS dirs_listed(root TEXT, node BLOB, PRIMARY KEY(root,node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS inos(root TEXT, node BLOB, ino INTEGER, gen INTEGER, PRIMARY KEY(root,node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS outbound(root TEXT, n INTEGER, op BLOB, cred BLOB, ts INTEGER, PRIMARY KEY(root,n)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS intents(root TEXT, seq INTEGER, change BLOB, PRIMARY KEY(root,seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS uploads(content BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pins(hash BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS local_nodes(root TEXT, node BLOB, attrs BLOB, PRIMARY KEY(root,node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS local_dirents(root TEXT, parent BLOB, name BLOB, node BLOB, PRIMARY KEY(root,parent,name)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS files(root TEXT, node BLOB, row BLOB, PRIMARY KEY(root,node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS reaps(root TEXT, ino INTEGER, PRIMARY KEY(root,ino)) WITHOUT ROWID;
";

impl Db {
    /// Open (creating) the DB, recover (SQLite, then replay the op log tail), load everything for
    /// `roots`, start the checkpointer.
    pub fn open(path: &Path, roots: &[String], interval: Duration, max_ops: usize, log_sync: Duration) -> Result<(Db, LoadedGlobal)> {
        let mut conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        // A checkpoint fsyncs the WAL and the DB file; on btrfs (where the cache lives) every fsync
        // forces a transaction commit that stalls the handlers' file creates and reflinks for
        // 100s of ms. The default (1000 pages) hit that about once a second under a create-heavy
        // load; 64 MiB makes it rare (the WAL is also checkpointed on a clean close).
        conn.pragma_update(None, "wal_autocheckpoint", 16384)?;
        conn.execute_batch(SCHEMA)?;
        let ckpt = conn.query_row("SELECT v FROM meta WHERE k='ckpt_lsn'", [], |r| r.get::<_, Vec<u8>>(0)).optional()?.map(|v| u64le(&v)).unwrap_or(0);
        let log_path = path.with_file_name("oplog");
        let (log, replay) = OpLog::open(&log_path, ckpt)?;
        if !replay.is_empty() {
            let mut n = 0usize;
            let mut last = ckpt;
            let tx = conn.transaction()?;
            for (end, payload) in &replay {
                let Ok(op) = dec::<DbOp>(payload) else { break };
                apply(&tx, op)?;
                last = *end;
                n += 1;
            }
            tx.prepare_cached("INSERT OR REPLACE INTO meta(k,v) VALUES('ckpt_lsn',?)")?.execute(params![last.to_le_bytes().to_vec()])?;
            tx.commit()?;
            tracing::info!("op log: replayed {n} records past the last checkpoint");
        }
        log.spawn_timer(log_sync);
        let g = load(&conn, roots)?;
        let (tx, rx) = mpsc::channel();
        let l2 = log.clone();
        let start_lsn = ckpt.max(replay.last().map(|r| r.0).unwrap_or(0));
        let h = std::thread::Builder::new().name("cairn-db".into()).spawn(move || writer(conn, rx, interval, max_ops, l2, start_lsn))?;
        Ok((Db { tx, log, writer: parking_lot::Mutex::new(Some(h)) }, g))
    }

    /// Append to the op log (written out at the next `sync_point`) and queue for the checkpointer.
    pub fn send(&self, op: DbOp) {
        let lsn = self.log.append(&enc(&op));
        let _ = self.tx.send(Msg::Op(op, lsn));
    }
    /// Everything sent so far is in the log file (survives a daemon crash). Called before replying.
    pub fn sync_point(&self) {
        self.log.write_out();
    }
    /// Everything sent so far is durable on disk (group-committed fsync of the log).
    pub fn sync_log(&self) {
        self.log.sync();
    }
    /// True if every op sent so far is already durable (no fsync needed).
    pub fn is_durable(&self) -> bool {
        self.log.synced_lsn() >= self.log.end_lsn()
    }
    pub fn send_all(&self, ops: Vec<DbOp>) {
        for o in ops {
            self.send(o);
        }
    }
    /// Make the log durable and fold everything queued so far into SQLite, waiting for both.
    pub fn flush(&self) {
        self.log.sync();
        let (t, r) = mpsc::sync_channel(0);
        if self.tx.send(Msg::Flush(t)).is_ok() {
            let _ = r.recv();
        }
    }
    pub fn meta_u64(&self, k: &str, v: u64) {
        self.send(DbOp::Meta(k.to_string(), v.to_le_bytes().to_vec()));
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.tx.send(Msg::Stop);
        if let Some(h) = self.writer.lock().take() {
            let _ = h.join();
        }
        self.log.stop.store(true, Ordering::SeqCst);
    }
}

fn load(conn: &Connection, roots: &[String]) -> Result<LoadedGlobal> {
    let mut g = LoadedGlobal::default();
    let meta = |k: &str| -> Result<Option<Vec<u8>>> { Ok(conn.query_row("SELECT v FROM meta WHERE k=?", [k], |r| r.get(0)).optional()?) };
    g.host_id = meta("host_id")?.map(|v| u64le(&v));
    g.node_ctr = meta("node_ctr")?.map(|v| u64le(&v)).unwrap_or(1);
    g.op_ctr = meta("op_ctr")?.map(|v| u64le(&v)).unwrap_or(1);
    {
        let mut st = conn.prepare("SELECT content FROM uploads")?;
        for r in st.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            g.uploads.push(dec(&r?)?);
        }
        let mut st = conn.prepare("SELECT hash FROM pins")?;
        for r in st.query_map([], |r| r.get::<_, Vec<u8>>(0))? {
            let b = r?;
            let mut h = [0u8; 32];
            h.copy_from_slice(&b);
            g.pins.push(Hash(h));
        }
    }
    for root in roots {
        let mut lr = LoadedRoot::default();
        let rk = |k: &str| format!("r/{root}/{k}");
        lr.epoch = meta(&rk("epoch"))?.map(|v| Epoch(u64le(&v)));
        lr.applied_seq = Seq(meta(&rk("applied_seq"))?.map(|v| u64le(&v)).unwrap_or(0));
        let mut st = conn.prepare("SELECT node,version,cversion,attrs FROM nodes WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, Vec<u8>>(3)?)))? {
            let (n, v, c, a) = r?;
            lr.nodes.push((bn(n), Seq(v as u64), Seq(c as u64), dec(&a)?));
        }
        let mut st = conn.prepare("SELECT parent,name,node FROM dirents WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?)))? {
            let (p, n, c) = r?;
            lr.dirents.push((bn(p), n, bn(c)));
        }
        let mut st = conn.prepare("SELECT node FROM dirs_listed WHERE root=?")?;
        for r in st.query_map([root], |r| r.get::<_, Vec<u8>>(0))? {
            lr.listed.push(bn(r?));
        }
        let mut st = conn.prepare("SELECT node,ino,gen FROM inos WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)))? {
            let (n, i, g) = r?;
            lr.inos.push((bn(n), i as u64, g as u64));
        }
        let mut st = conn.prepare("SELECT n,op,cred,ts FROM outbound WHERE root=? ORDER BY n")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?, r.get::<_, i64>(3)?)))? {
            let (n, op, cred, ts) = r?;
            lr.outbound.push((n as u64, dec(&op)?, dec(&cred)?, ts));
        }
        let mut st = conn.prepare("SELECT seq,change FROM intents WHERE root=? ORDER BY seq")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))? {
            let (s, c) = r?;
            lr.intents.push((s as u64, dec(&c)?));
        }
        let mut st = conn.prepare("SELECT node,attrs FROM local_nodes WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)))? {
            let (n, a) = r?;
            lr.local_nodes.push((bn(n), dec(&a)?));
        }
        let mut st = conn.prepare("SELECT parent,name,node FROM local_dirents WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, Vec<u8>>(2)?)))? {
            let (p, n, c) = r?;
            lr.local_dirents.push((bn(p), n, bn(c)));
        }
        let mut st = conn.prepare("SELECT node,row FROM files WHERE root=?")?;
        for r in st.query_map([root], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)))? {
            let (n, b) = r?;
            lr.files.push((bn(n), dec(&b)?));
        }
        let mut st = conn.prepare("SELECT ino FROM reaps WHERE root=?")?;
        for r in st.query_map([root], |r| r.get::<_, i64>(0))? {
            lr.reaps.push(r? as u64);
        }
        g.roots.insert(root.clone(), lr);
    }
    Ok(g)
}

fn apply(tx: &rusqlite::Transaction, op: DbOp) -> rusqlite::Result<()> {
    match op {
        DbOp::Meta(k, v) => {
            tx.prepare_cached("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)")?.execute(params![k, v])?;
        }
        DbOp::Node { root, node, row } => match row {
            Some((v, c, a)) => {
                tx.prepare_cached("INSERT OR REPLACE INTO nodes(root,node,version,cversion,attrs) VALUES(?,?,?,?,?)")?.execute(params![root, nb(node), v.0 as i64, c.0 as i64, enc(&a)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM nodes WHERE root=? AND node=?")?.execute(params![root, nb(node)])?;
            }
        },
        DbOp::Dirent { root, parent, name, node } => match node {
            Some(n) => {
                tx.prepare_cached("INSERT OR REPLACE INTO dirents(root,parent,name,node) VALUES(?,?,?,?)")?.execute(params![root, nb(parent), name, nb(n)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM dirents WHERE root=? AND parent=? AND name=?")?.execute(params![root, nb(parent), name])?;
            }
        },
        DbOp::Listed { root, node, listed } => {
            if listed {
                tx.prepare_cached("INSERT OR REPLACE INTO dirs_listed(root,node) VALUES(?,?)")?.execute(params![root, nb(node)])?;
            } else {
                tx.prepare_cached("DELETE FROM dirs_listed WHERE root=? AND node=?")?.execute(params![root, nb(node)])?;
            }
        }
        DbOp::Ino { root, node, ino, gen } => {
            tx.prepare_cached("INSERT OR REPLACE INTO inos(root,node,ino,gen) VALUES(?,?,?,?)")?.execute(params![root, nb(node), ino as i64, gen as i64])?;
        }
        DbOp::InoDel { root, node } => {
            tx.prepare_cached("DELETE FROM inos WHERE root=? AND node=?")?.execute(params![root, nb(node)])?;
        }
        DbOp::Outbound { root, n, row } => match row {
            Some((op, cred, ts)) => {
                tx.prepare_cached("INSERT OR REPLACE INTO outbound(root,n,op,cred,ts) VALUES(?,?,?,?,?)")?.execute(params![root, n as i64, enc(&op), enc(&cred), ts])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM outbound WHERE root=? AND n=?")?.execute(params![root, n as i64])?;
            }
        },
        DbOp::Intent { root, seq, change } => match change {
            Some(c) => {
                tx.prepare_cached("INSERT OR REPLACE INTO intents(root,seq,change) VALUES(?,?,?)")?.execute(params![root, seq as i64, enc(&c)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM intents WHERE root=? AND seq<=?")?.execute(params![root, seq as i64])?;
            }
        },
        DbOp::Upload { content, add } => {
            if add {
                tx.prepare_cached("INSERT OR REPLACE INTO uploads(content) VALUES(?)")?.execute(params![enc(&content)])?;
            } else {
                tx.prepare_cached("DELETE FROM uploads WHERE content=?")?.execute(params![enc(&content)])?;
            }
        }
        DbOp::Pin { hash, add } => {
            if add {
                tx.prepare_cached("INSERT OR REPLACE INTO pins(hash) VALUES(?)")?.execute(params![hash.0.to_vec()])?;
            } else {
                tx.prepare_cached("DELETE FROM pins WHERE hash=?")?.execute(params![hash.0.to_vec()])?;
            }
        }
        DbOp::LocalNode { root, node, attrs } => match attrs {
            Some(a) => {
                tx.prepare_cached("INSERT OR REPLACE INTO local_nodes(root,node,attrs) VALUES(?,?,?)")?.execute(params![root, nb(node), enc(&a)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM local_nodes WHERE root=? AND node=?")?.execute(params![root, nb(node)])?;
            }
        },
        DbOp::File { root, node, row } => match row {
            Some(r) => {
                tx.prepare_cached("INSERT OR REPLACE INTO files(root,node,row) VALUES(?,?,?)")?.execute(params![root, nb(node), enc(&r)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM files WHERE root=? AND node=?")?.execute(params![root, nb(node)])?;
            }
        },
        DbOp::Reap { root, ino, add } => {
            if add {
                tx.prepare_cached("INSERT OR REPLACE INTO reaps(root,ino) VALUES(?,?)")?.execute(params![root, ino as i64])?;
            } else {
                tx.prepare_cached("DELETE FROM reaps WHERE root=? AND ino=?")?.execute(params![root, ino as i64])?;
            }
        }
        DbOp::LocalDirent { root, parent, name, node } => match node {
            Some(n) => {
                tx.prepare_cached("INSERT OR REPLACE INTO local_dirents(root,parent,name,node) VALUES(?,?,?,?)")?.execute(params![root, nb(parent), name, nb(n)])?;
            }
            None => {
                tx.prepare_cached("DELETE FROM local_dirents WHERE root=? AND parent=? AND name=?")?.execute(params![root, nb(parent), name])?;
            }
        },
    }
    Ok(())
}

fn commit(conn: &mut Connection, batch: &mut Vec<DbOp>, lsn: u64) {
    if batch.is_empty() {
        return;
    }
    let res = (|| -> rusqlite::Result<()> {
        let tx = conn.transaction()?;
        for op in batch.drain(..) {
            apply(&tx, op)?;
        }
        tx.prepare_cached("INSERT OR REPLACE INTO meta(k,v) VALUES('ckpt_lsn',?)")?.execute(params![lsn.to_le_bytes().to_vec()])?;
        tx.commit()
    })();
    if let Err(e) = res {
        tracing::error!("state db commit failed: {e}");
        batch.clear();
    }
}

/// The checkpointer: folds the op log into SQLite in batches.
fn writer(mut conn: Connection, rx: Receiver<Msg>, interval: Duration, max_ops: usize, log: Arc<OpLog>, mut lsn: u64) {
    let mut batch: Vec<DbOp> = vec![];
    let mut stopping = false;
    loop {
        let first = match rx.recv() {
            Ok(m) => m,
            Err(_) => break,
        };
        let mut acks: Vec<SyncSender<()>> = vec![];
        let handle = |m: Msg, batch: &mut Vec<DbOp>, acks: &mut Vec<SyncSender<()>>, lsn: &mut u64, stopping: &mut bool| match m {
            Msg::Op(o, l) => {
                batch.push(o);
                *lsn = l;
                false
            }
            Msg::Flush(a) => {
                acks.push(a);
                true
            }
            Msg::Stop => {
                *stopping = true;
                true
            }
        };
        let mut flush = handle(first, &mut batch, &mut acks, &mut lsn, &mut stopping);
        let deadline = Instant::now() + interval;
        let mut closed = false;
        while !flush && batch.len() < max_ops {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(m) => flush = handle(m, &mut batch, &mut acks, &mut lsn, &mut stopping),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        commit(&mut conn, &mut batch, lsn);
        for a in acks {
            let _ = a.send(());
        }
        // fully absorbed and the log is big (or we are shutting down): make SQLite durable, then
        // start an empty log
        let big = log.file_bytes() > LOG_ROTATE_BYTES;
        if (big || stopping || closed) && log.end_lsn() == lsn && lsn > 0 && conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).is_ok() {
            log.try_rotate(lsn);
        }
        if closed || stopping {
            break;
        }
    }
    commit(&mut conn, &mut batch, lsn);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::dir_attrs;

    #[test]
    fn roundtrip_and_flush() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("state.db");
        let roots = vec!["r".to_string()];
        let (db, g) = Db::open(&p, &roots, Duration::from_millis(5), 500, Duration::from_millis(50)).unwrap();
        assert!(g.host_id.is_none());
        let n = NodeId::new(HostId(1), 5);
        db.send(DbOp::Node { root: "r".into(), node: n, row: Some((Seq(3), Seq(2), dir_attrs(1))) });
        db.send(DbOp::Dirent { root: "r".into(), parent: NodeId::ROOT, name: b"x".to_vec(), node: Some(n) });
        db.send(DbOp::Outbound { root: "r".into(), n: 9, row: Some((Op::SetXattr { node: n, name: b"a".to_vec(), value: None }, Cred { uid: 1, gid: 2 }, 77)) });
        db.send(DbOp::Meta("host_id".into(), 4u64.to_le_bytes().to_vec()));
        db.flush();
        drop(db);
        let (_db, g) = Db::open(&p, &roots, Duration::from_millis(5), 500, Duration::from_millis(50)).unwrap();
        assert_eq!(g.host_id, Some(4));
        let r = &g.roots["r"];
        assert_eq!(r.nodes.len(), 1);
        assert_eq!(r.nodes[0].1, Seq(3));
        assert_eq!(r.dirents, vec![(NodeId::ROOT, b"x".to_vec(), n)]);
        assert_eq!(r.outbound.len(), 1);
        assert_eq!(r.outbound[0].0, 9);
    }
}
