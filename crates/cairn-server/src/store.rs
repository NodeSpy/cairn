//! SQLite storage: schema, the `Tree` implementation the merge engine runs
//! on, read queries, and the group-commit writer thread.

use crate::cas::Cas;
use crate::hub::Hub;
use crate::leases::Leases;
use crate::merge::{self, Ctx, NodeRec, Rules, Tomb, Tree};
use anyhow::{Context, Result};
use cairn_proto::frame::{decode, encode};
use cairn_proto::*;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

pub const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS hosts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  fp BLOB NOT NULL UNIQUE,
  allowed_uids TEXT, allowed_gids TEXT,
  root_squash INTEGER NOT NULL DEFAULT 0,
  uid_map TEXT, gid_map TEXT,
  max_frame INTEGER, max_commits INTEGER);
CREATE TABLE IF NOT EXISTS roots(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  head INTEGER NOT NULL DEFAULT 0,
  log_floor INTEGER NOT NULL DEFAULT 0,
  srv_node INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS acl(root INTEGER NOT NULL, host INTEGER NOT NULL, rw INTEGER NOT NULL, PRIMARY KEY(root, host)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS nodes(
  root INTEGER NOT NULL, node BLOB NOT NULL,
  version INTEGER NOT NULL, cver INTEGER NOT NULL, mod_host INTEGER NOT NULL, content_host INTEGER NOT NULL,
  attrs BLOB NOT NULL, PRIMARY KEY(root, node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS dirents(
  root INTEGER NOT NULL, parent BLOB NOT NULL, name BLOB NOT NULL, node BLOB NOT NULL,
  PRIMARY KEY(root, parent, name)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dirents_node ON dirents(root, node);
CREATE TABLE IF NOT EXISTS tombs(
  root INTEGER NOT NULL, node BLOB NOT NULL, parent BLOB NOT NULL, name BLOB NOT NULL,
  version INTEGER NOT NULL, cver INTEGER NOT NULL, mod_host INTEGER NOT NULL, content_host INTEGER NOT NULL,
  attrs BLOB NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY(root, node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS aliases(root INTEGER NOT NULL, from_node BLOB NOT NULL, to_node BLOB NOT NULL, PRIMARY KEY(root, from_node)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS changes(root INTEGER NOT NULL, seq INTEGER NOT NULL, ts INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(root, seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ops(root INTEGER NOT NULL, host INTEGER NOT NULL, n INTEGER NOT NULL, ts INTEGER NOT NULL, result BLOB NOT NULL, PRIMARY KEY(root, host, n)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS conflicts(id INTEGER PRIMARY KEY AUTOINCREMENT, root INTEGER NOT NULL, seq INTEGER NOT NULL, host INTEGER NOT NULL, uid INTEGER NOT NULL, ts INTEGER NOT NULL, data BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS conflicts_seq ON conflicts(root, seq);
CREATE TABLE IF NOT EXISTS pending(content BLOB NOT NULL, host INTEGER NOT NULL, PRIMARY KEY(content, host)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS manifests(id BLOB PRIMARY KEY, data BLOB NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS lost_content(content BLOB NOT NULL, host INTEGER NOT NULL, ts INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS commit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, root INTEGER NOT NULL, host INTEGER NOT NULL, commit_ BLOB NOT NULL, result BLOB NOT NULL);
";

pub fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

pub fn nid(n: NodeId) -> [u8; 16] {
    n.0.to_be_bytes()
}
pub fn unid(b: &[u8]) -> NodeId {
    let mut a = [0u8; 16];
    a.copy_from_slice(b);
    NodeId(u128::from_be_bytes(a))
}
pub(crate) fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    encode(v).expect("encode")
}
pub(crate) fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> rusqlite::Result<T> {
    decode(b).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Blob, Box::new(e)))
}

pub fn open_rw(path: &Path) -> Result<Connection> {
    let c = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
    c.busy_timeout(std::time::Duration::from_secs(10))?;
    c.pragma_update(None, "journal_mode", "WAL")?;
    c.pragma_update(None, "synchronous", "FULL")?;
    Ok(c)
}

pub fn open_ro(path: &Path) -> Result<Connection> {
    let c = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
    c.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(c)
}

pub fn init_schema(c: &Connection) -> Result<()> {
    c.execute_batch(SCHEMA)?;
    let have: Option<Vec<u8>> = c.query_row("SELECT v FROM meta WHERE k='epoch'", [], |r| r.get(0)).optional()?;
    if have.is_none() {
        let epoch: u64 = rand::random::<u64>() >> 1;
        c.execute("INSERT INTO meta(k,v) VALUES('epoch', ?)", [epoch.to_be_bytes().to_vec()])?;
        c.execute("INSERT INTO meta(k,v) VALUES('schema', ?)", [SCHEMA_VERSION.to_be_bytes().to_vec()])?;
    }
    Ok(())
}

pub fn epoch(c: &Connection) -> Result<Epoch> {
    let v: Vec<u8> = c.query_row("SELECT v FROM meta WHERE k='epoch'", [], |r| r.get(0))?;
    Ok(Epoch(u64::from_be_bytes(v.try_into().map_err(|_| anyhow::anyhow!("bad epoch"))?)))
}

/// A small pool of read-only connections (WAL snapshot reads).
pub struct Db {
    path: PathBuf,
    readers: Mutex<Vec<Connection>>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Db> {
        Ok(Db { path: path.to_path_buf(), readers: Mutex::new(vec![]) })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Run `f` inside one read transaction (consistent snapshot).
    pub fn read<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
        let conn = match self.readers.lock().pop() {
            Some(c) => c,
            None => open_ro(&self.path)?,
        };
        conn.execute_batch("BEGIN")?;
        let r = f(&conn);
        let _ = conn.execute_batch("COMMIT");
        let mut g = self.readers.lock();
        if g.len() < 8 {
            g.push(conn);
        }
        Ok(r?)
    }
}

// ---------- host / root rows ----------

#[derive(Clone, Debug)]
pub struct HostRow {
    pub id: HostId,
    pub name: String,
    pub rules: Rules,
    pub max_frame: usize,
    pub max_commits: usize,
}

pub const DEFAULT_MAX_COMMITS: usize = 4096;

fn host_row(r: &rusqlite::Row) -> rusqlite::Result<HostRow> {
    let s = |i: usize| -> rusqlite::Result<Option<String>> { r.get(i) };
    let bad = |e: String| rusqlite::Error::InvalidParameterName(e);
    let rules = Rules {
        allowed_uids: s(3)?.map(|x| Rules::parse_ranges(&x)).transpose().map_err(bad)?,
        allowed_gids: s(4)?.map(|x| Rules::parse_ranges(&x)).transpose().map_err(bad)?,
        root_squash: r.get::<_, i64>(5)? != 0,
        uid_map: s(6)?.map(|x| Rules::parse_map(&x)).transpose().map_err(bad)?.unwrap_or_default(),
        gid_map: s(7)?.map(|x| Rules::parse_map(&x)).transpose().map_err(bad)?.unwrap_or_default(),
    };
    let mf: Option<i64> = r.get(8)?;
    let mc: Option<i64> = r.get(9)?;
    Ok(HostRow {
        id: HostId(r.get::<_, i64>(0)? as u64),
        name: r.get(1)?,
        rules,
        max_frame: mf.map(|x| x as usize).unwrap_or(cairn_proto::frame::MAX_FRAME).min(cairn_proto::frame::MAX_FRAME),
        max_commits: mc.map(|x| x as usize).unwrap_or(DEFAULT_MAX_COMMITS),
    })
}
const HOST_COLS: &str = "id,name,fp,allowed_uids,allowed_gids,root_squash,uid_map,gid_map,max_frame,max_commits";

pub fn host_by_fp(c: &Connection, fp: &Hash) -> rusqlite::Result<Option<HostRow>> {
    c.query_row(&format!("SELECT {HOST_COLS} FROM hosts WHERE fp=?"), [fp.0.to_vec()], host_row).optional()
}
pub fn host_by_id(c: &Connection, id: HostId) -> rusqlite::Result<Option<HostRow>> {
    c.query_row(&format!("SELECT {HOST_COLS} FROM hosts WHERE id=?"), [id.0 as i64], host_row).optional()
}
pub fn host_by_name(c: &Connection, name: &str) -> rusqlite::Result<Option<HostRow>> {
    c.query_row(&format!("SELECT {HOST_COLS} FROM hosts WHERE name=?"), [name], host_row).optional()
}

#[derive(Clone, Debug)]
pub struct RootRow {
    pub id: i64,
    pub name: String,
    pub head: u64,
    pub floor: u64,
}
pub fn root_by_name(c: &Connection, name: &str) -> rusqlite::Result<Option<RootRow>> {
    c.query_row("SELECT id,name,head,log_floor FROM roots WHERE name=?", [name], |r| {
        Ok(RootRow { id: r.get(0)?, name: r.get(1)?, head: r.get::<_, i64>(2)? as u64, floor: r.get::<_, i64>(3)? as u64 })
    })
    .optional()
}
pub fn root_head_floor(c: &Connection, root: i64) -> rusqlite::Result<(u64, u64)> {
    c.query_row("SELECT head,log_floor FROM roots WHERE id=?", [root], |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)))
}
pub fn acl_for(c: &Connection, root: i64, host: HostId) -> rusqlite::Result<Option<bool>> {
    c.query_row("SELECT rw FROM acl WHERE root=? AND host=?", params![root, host.0 as i64], |r| Ok(r.get::<_, i64>(0)? != 0)).optional()
}
pub fn host_can_write_any(c: &Connection, host: &HostRow) -> rusqlite::Result<bool> {
    let n: i64 = c.query_row("SELECT COUNT(*) FROM acl WHERE host=? AND rw=1", [host.id.0 as i64], |r| r.get(0))?;
    if n > 0 {
        return Ok(true);
    }
    let like = format!("hosts/{}/%", host.name);
    let n: i64 = c.query_row("SELECT COUNT(*) FROM roots WHERE name LIKE ? OR name=?", params![like, format!("hosts/{}", host.name)], |r| r.get(0))?;
    Ok(n > 0)
}

// ---------- read queries ----------

pub fn resolve_alias(c: &Connection, root: i64, mut n: NodeId) -> rusqlite::Result<NodeId> {
    for _ in 0..32 {
        let to: Option<Vec<u8>> = c.query_row("SELECT to_node FROM aliases WHERE root=? AND from_node=?", params![root, nid(n).to_vec()], |r| r.get(0)).optional()?;
        match to {
            Some(b) => n = unid(&b),
            None => break,
        }
    }
    Ok(n)
}

pub fn node_rec(c: &Connection, root: i64, n: NodeId) -> rusqlite::Result<Option<NodeRec>> {
    c.prepare_cached("SELECT attrs,version,cver,mod_host,content_host FROM nodes WHERE root=? AND node=?")?
        .query_row(params![root, nid(n).to_vec()], |r| {
            Ok(NodeRec {
                attrs: dec(&r.get::<_, Vec<u8>>(0)?)?,
                version: Seq(r.get::<_, i64>(1)? as u64),
                cver: Seq(r.get::<_, i64>(2)? as u64),
                mod_host: HostId(r.get::<_, i64>(3)? as u64),
                content_host: HostId(r.get::<_, i64>(4)? as u64),
            })
        })
        .optional()
}

pub fn read_dir(c: &Connection, root: i64, dir: NodeId, after: Option<&Name>, limit: usize) -> rusqlite::Result<(Seq, Vec<DirEntry>, bool)> {
    let (head, _) = root_head_floor(c, root)?;
    let dir = resolve_alias(c, root, dir)?;
    let mut st = c.prepare_cached(
        "SELECT d.name, d.node, n.attrs FROM dirents d JOIN nodes n ON n.root=d.root AND n.node=d.node
         WHERE d.root=?1 AND d.parent=?2 AND d.name>?3 ORDER BY d.name LIMIT ?4",
    )?;
    let after_b = after.map(|n| n.0.clone()).unwrap_or_default();
    let mut rows = st.query(params![root, nid(dir).to_vec(), after_b, (limit as i64).saturating_add(1)])?;
    let mut out = vec![];
    let mut bytes = 0usize;
    let mut done = true;
    while let Some(r) = rows.next()? {
        if out.len() >= limit || bytes > (8 << 20) {
            done = false;
            break;
        }
        let ab: Vec<u8> = r.get(2)?;
        bytes += ab.len() + 64;
        out.push(DirEntry { name: Name(r.get(0)?), node: unid(&r.get::<_, Vec<u8>>(1)?), attrs: dec(&ab)? });
    }
    Ok((Seq(head), out, done))
}

pub fn snapshot(c: &Connection, root: i64, after: Option<&(NodeId, Name)>, limit: usize) -> rusqlite::Result<(Seq, Vec<SnapshotRow>, bool)> {
    let (head, _) = root_head_floor(c, root)?;
    let mut st = c.prepare_cached(
        "SELECT d.parent, d.name, d.node, n.attrs FROM dirents d JOIN nodes n ON n.root=d.root AND n.node=d.node
         WHERE d.root=?1 AND (d.parent>?2 OR (d.parent=?2 AND d.name>?3)) ORDER BY d.parent, d.name LIMIT ?4",
    )?;
    let (ap, an) = match after {
        Some((p, n)) => (nid(*p).to_vec(), n.0.clone()),
        None => (vec![0u8; 16], vec![]),
    };
    // parent>?2 with an all-zero lower bound includes every real parent (ids are >= 1)
    let mut rows = st.query(params![root, ap, an, (limit as i64).saturating_add(1)])?;
    let mut out = vec![];
    let mut bytes = 0usize;
    let mut done = true;
    while let Some(r) = rows.next()? {
        if out.len() >= limit || bytes > (8 << 20) {
            done = false;
            break;
        }
        let ab: Vec<u8> = r.get(3)?;
        bytes += ab.len() + 96;
        out.push(SnapshotRow { parent: unid(&r.get::<_, Vec<u8>>(0)?), name: Name(r.get(1)?), node: unid(&r.get::<_, Vec<u8>>(2)?), attrs: dec(&ab)? });
    }
    Ok((Seq(head), out, done))
}

/// Changes with `after < seq <= upto`, at most `limit`.
pub fn changes_range(c: &Connection, root: i64, after: u64, upto: u64, limit: usize) -> rusqlite::Result<Vec<Change>> {
    let mut st = c.prepare_cached("SELECT data FROM changes WHERE root=? AND seq>? AND seq<=? ORDER BY seq LIMIT ?")?;
    let rows = st.query_map(params![root, after as i64, upto as i64, limit as i64], |r| dec::<Change>(&r.get::<_, Vec<u8>>(0)?))?;
    rows.collect()
}

/// Records with `seq >= since` (inclusive: a Rejected record carries the head seq and emits no change, so
/// it can share a seq with an earlier record; clients dedupe by op_id).
pub fn conflicts_since(c: &Connection, root: i64, since: u64, limit: usize) -> rusqlite::Result<Vec<ConflictRecord>> {
    let mut st = c.prepare_cached("SELECT data FROM conflicts WHERE root=? AND seq>=? ORDER BY id LIMIT ?")?;
    let rows = st.query_map(params![root, since as i64, limit as i64], |r| dec::<ConflictRecord>(&r.get::<_, Vec<u8>>(0)?))?;
    rows.collect()
}

pub fn get_manifest(c: &Connection, id: &Hash) -> rusqlite::Result<Option<Manifest>> {
    c.query_row("SELECT data FROM manifests WHERE id=?", [id.0.to_vec()], |r| dec(&r.get::<_, Vec<u8>>(0)?)).optional()
}
pub fn has_manifest(c: &Connection, id: &Hash) -> rusqlite::Result<bool> {
    Ok(c.query_row("SELECT 1 FROM manifests WHERE id=?", [id.0.to_vec()], |_| Ok(())).optional()?.is_some())
}
pub fn pending_owners(c: &Connection, content: &ContentId) -> rusqlite::Result<Vec<HostId>> {
    let mut st = c.prepare_cached("SELECT host FROM pending WHERE content=?")?;
    let rows = st.query_map([enc(content)], |r| Ok(HostId(r.get::<_, i64>(0)? as u64)))?;
    rows.collect()
}

// ---------- the Tree over SQLite ----------

pub struct SqlTree<'a> {
    conn: &'a Connection,
    root: i64,
    head: u64,
    srv: u64,
    ts: i64,
    err: RefCell<Option<rusqlite::Error>>,
    leases: &'a Leases,
    cas: &'a Cas,
    record: Option<HostId>,
}

impl<'a> SqlTree<'a> {
    fn new(conn: &'a Connection, root: i64, leases: &'a Leases, cas: &'a Cas, record: Option<HostId>) -> rusqlite::Result<Self> {
        let (head, srv) = conn.query_row("SELECT head,srv_node FROM roots WHERE id=?", [root], |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)))?;
        Ok(SqlTree { conn, root, head, srv, ts: now_secs() as i64, err: RefCell::new(None), leases, cas, record })
    }
    fn finish(self) -> rusqlite::Result<()> {
        if let Some(e) = self.err.into_inner() {
            return Err(e);
        }
        self.conn.execute("UPDATE roots SET head=?, srv_node=? WHERE id=?", params![self.head as i64, self.srv as i64, self.root])?;
        Ok(())
    }
    fn fail(&self, e: rusqlite::Error) {
        let mut g = self.err.borrow_mut();
        if g.is_none() {
            *g = Some(e);
        }
    }
    fn exec(&self, sql: &str, p: impl rusqlite::Params) {
        if self.err.borrow().is_some() {
            return;
        }
        if let Err(e) = self.conn.prepare_cached(sql).and_then(|mut s| s.execute(p)) {
            self.fail(e);
        }
    }
    fn one<T>(&self, r: rusqlite::Result<Option<T>>) -> Option<T> {
        match r {
            Ok(v) => v,
            Err(e) => {
                self.fail(e);
                None
            }
        }
    }
}

impl Tree for SqlTree<'_> {
    fn head(&self) -> Seq {
        Seq(self.head)
    }
    fn bump_seq(&mut self) -> Seq {
        self.head += 1;
        Seq(self.head)
    }
    fn node(&self, n: NodeId) -> Option<NodeRec> {
        self.one(node_rec(self.conn, self.root, n))
    }
    fn dirent(&self, parent: NodeId, name: &Name) -> Option<NodeId> {
        let r = self
            .conn
            .prepare_cached("SELECT node FROM dirents WHERE root=? AND parent=? AND name=?")
            .and_then(|mut s| s.query_row(params![self.root, nid(parent).to_vec(), name.0], |r| r.get::<_, Vec<u8>>(0)).optional());
        self.one(r).map(|b| unid(&b))
    }
    fn has_children(&self, dir: NodeId) -> bool {
        let r = self
            .conn
            .prepare_cached("SELECT 1 FROM dirents WHERE root=? AND parent=? LIMIT 1")
            .and_then(|mut s| s.query_row(params![self.root, nid(dir).to_vec()], |_| Ok(())).optional());
        self.one(r).is_some()
    }
    fn dirents_of(&self, node: NodeId) -> Vec<(NodeId, Name)> {
        let r = (|| -> rusqlite::Result<Vec<(NodeId, Name)>> {
            let mut s = self.conn.prepare_cached("SELECT parent,name FROM dirents WHERE root=? AND node=? ORDER BY parent,name")?;
            let rows = s.query_map(params![self.root, nid(node).to_vec()], |r| Ok((unid(&r.get::<_, Vec<u8>>(0)?), Name(r.get(1)?))))?;
            rows.collect()
        })();
        r.unwrap_or_else(|e| {
            self.fail(e);
            vec![]
        })
    }
    fn tomb(&self, n: NodeId) -> Option<Tomb> {
        let r = self
            .conn
            .prepare_cached("SELECT parent,name,attrs,version,cver,mod_host,content_host FROM tombs WHERE root=? AND node=?")
            .and_then(|mut s| {
                s.query_row(params![self.root, nid(n).to_vec()], |r| {
                    Ok(Tomb {
                        parent: unid(&r.get::<_, Vec<u8>>(0)?),
                        name: Name(r.get(1)?),
                        rec: NodeRec {
                            attrs: dec(&r.get::<_, Vec<u8>>(2)?)?,
                            version: Seq(r.get::<_, i64>(3)? as u64),
                            cver: Seq(r.get::<_, i64>(4)? as u64),
                            mod_host: HostId(r.get::<_, i64>(5)? as u64),
                            content_host: HostId(r.get::<_, i64>(6)? as u64),
                        },
                    })
                })
                .optional()
            });
        self.one(r)
    }
    fn alias(&self, n: NodeId) -> Option<NodeId> {
        let r = self
            .conn
            .prepare_cached("SELECT to_node FROM aliases WHERE root=? AND from_node=?")
            .and_then(|mut s| s.query_row(params![self.root, nid(n).to_vec()], |r| r.get::<_, Vec<u8>>(0)).optional());
        self.one(r).map(|b| unid(&b))
    }
    fn lease_holder(&self, n: NodeId) -> Option<HostId> {
        self.leases.holder(self.root, n)
    }
    fn has_content(&self, c: &ContentId) -> bool {
        match c {
            ContentId::Empty => true,
            ContentId::Blob(h) => self.cas.has(h),
            ContentId::Chunked(h) => match has_manifest(self.conn, h) {
                Ok(b) => b,
                Err(e) => {
                    self.fail(e);
                    false
                }
            },
        }
    }
    fn op_result(&self, op: OpId) -> Option<CommitResult> {
        let r = self
            .conn
            .prepare_cached("SELECT result FROM ops WHERE root=? AND host=? AND n=?")
            .and_then(|mut s| s.query_row(params![self.root, op.host.0 as i64, op.n as i64], |r| dec::<CommitResult>(&r.get::<_, Vec<u8>>(0)?)).optional());
        self.one(r)
    }
    fn next_server_node(&mut self) -> NodeId {
        // counter starts at 2: HostId(0)/1 is NodeId::ROOT
        self.srv = self.srv.max(1) + 1;
        NodeId::new(HostId(0), self.srv)
    }
    fn put_node(&mut self, n: NodeId, rec: &NodeRec) {
        self.exec(
            "INSERT OR REPLACE INTO nodes(root,node,version,cver,mod_host,content_host,attrs) VALUES(?,?,?,?,?,?,?)",
            params![self.root, nid(n).to_vec(), rec.version.0 as i64, rec.cver.0 as i64, rec.mod_host.0 as i64, rec.content_host.0 as i64, enc(&rec.attrs)],
        );
    }
    fn del_node(&mut self, n: NodeId) {
        self.exec("DELETE FROM nodes WHERE root=? AND node=?", params![self.root, nid(n).to_vec()]);
    }
    fn put_dirent(&mut self, parent: NodeId, name: &Name, node: NodeId) {
        self.exec("INSERT OR REPLACE INTO dirents(root,parent,name,node) VALUES(?,?,?,?)", params![self.root, nid(parent).to_vec(), name.0, nid(node).to_vec()]);
    }
    fn del_dirent(&mut self, parent: NodeId, name: &Name) {
        self.exec("DELETE FROM dirents WHERE root=? AND parent=? AND name=?", params![self.root, nid(parent).to_vec(), name.0]);
    }
    fn put_tomb(&mut self, n: NodeId, t: &Tomb) {
        self.exec(
            "INSERT OR REPLACE INTO tombs(root,node,parent,name,version,cver,mod_host,content_host,attrs,ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
            params![self.root, nid(n).to_vec(), nid(t.parent).to_vec(), t.name.0, t.rec.version.0 as i64, t.rec.cver.0 as i64, t.rec.mod_host.0 as i64, t.rec.content_host.0 as i64, enc(&t.rec.attrs), self.ts],
        );
    }
    fn del_tomb(&mut self, n: NodeId) {
        self.exec("DELETE FROM tombs WHERE root=? AND node=?", params![self.root, nid(n).to_vec()]);
    }
    fn put_alias(&mut self, from: NodeId, to: NodeId) {
        self.exec("INSERT OR REPLACE INTO aliases(root,from_node,to_node) VALUES(?,?,?)", params![self.root, nid(from).to_vec(), nid(to).to_vec()]);
    }
    fn add_pending(&mut self, c: ContentId, host: HostId) {
        self.exec("INSERT OR IGNORE INTO pending(content,host) VALUES(?,?)", params![enc(&c), host.0 as i64]);
    }
    fn log_change(&mut self, c: &Change) {
        self.exec("INSERT INTO changes(root,seq,ts,data) VALUES(?,?,?,?)", params![self.root, c.seq.0 as i64, self.ts, enc(c)]);
    }
    fn put_op_result(&mut self, op: OpId, r: &CommitResult) {
        self.exec("INSERT OR REPLACE INTO ops(root,host,n,ts,result) VALUES(?,?,?,?,?)", params![self.root, op.host.0 as i64, op.n as i64, self.ts, enc(r)]);
    }
    fn log_conflict(&mut self, rec: &ConflictRecord, cred: Cred) {
        self.exec(
            "INSERT INTO conflicts(root,seq,host,uid,ts,data) VALUES(?,?,?,?,?,?)",
            params![self.root, rec.seq.0 as i64, rec.host.0 as i64, cred.uid as i64, self.ts, enc(rec)],
        );
    }
}

// ---------- the writer thread (group commit) ----------

#[derive(Clone, Debug)]
pub struct Retention {
    pub log_secs: u64,
    pub log_min_changes: u64,
    pub op_window_secs: u64,
}

impl Default for Retention {
    fn default() -> Self {
        Retention { log_secs: 7 * 86400, log_min_changes: 1_000_000, op_window_secs: 7 * 86400 }
    }
}

pub struct CommitJob {
    pub root: i64,
    pub host: HostRow,
    pub commits: Vec<Commit>,
    pub reply: oneshot::Sender<Result<Vec<CommitResult>, ErrorReply>>,
}

pub enum Job {
    Commit(CommitJob),
    /// Content landed in the CAS/manifests: clear its pending rows.
    Landed { keys: Vec<ContentId>, manifest: Option<(Hash, Vec<u8>)>, done: oneshot::Sender<()> },
    /// Record that an owner could not supply pending content.
    Lost { key: ContentId, host: HostId, done: oneshot::Sender<()> },
    Prune,
    Barrier(std::sync::mpsc::Sender<()>),
    Shutdown,
}

/// Group-commit counters (evidence that commits batch).
#[derive(Default)]
pub struct WriterStats {
    pub batches: std::sync::atomic::AtomicU64,
    pub commit_jobs: std::sync::atomic::AtomicU64,
}

pub struct Writer {
    pub stats: Arc<WriterStats>,
    tx: crossbeam_channel::Sender<Job>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    dead: Arc<AtomicBool>,
}

pub struct WriterDeps {
    pub db_path: PathBuf,
    pub hub: Arc<Hub>,
    pub leases: Arc<Leases>,
    pub cas: Cas,
    pub retention: Retention,
    pub record_commits: bool,
    pub max_batch: usize,
}

impl Writer {
    pub fn start(deps: WriterDeps) -> Result<Writer> {
        let conn = open_rw(&deps.db_path)?;
        init_schema(&conn)?;
        let (tx, rx) = crossbeam_channel::unbounded::<Job>();
        let dead = Arc::new(AtomicBool::new(false));
        let d2 = dead.clone();
        let stats = Arc::new(WriterStats::default());
        let st2 = stats.clone();
        let join = std::thread::Builder::new().name("cairn-writer".into()).spawn(move || writer_loop(conn, rx, deps, d2, st2))?;
        Ok(Writer { stats, tx, join: Mutex::new(Some(join)), dead })
    }
    /// Enqueue in submission order (call from the connection's read loop).
    pub fn submit(&self, job: Job) -> bool {
        self.tx.send(job).is_ok()
    }
    /// Stop after draining everything queued so far.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Job::Shutdown);
        if let Some(j) = self.join.lock().take() {
            let _ = j.join();
        }
    }
    /// Simulate sudden death: queued/in-flight work is discarded unreplied.
    pub fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
        let _ = self.tx.send(Job::Shutdown);
        if let Some(j) = self.join.lock().take() {
            let _ = j.join();
        }
    }
}

fn writer_loop(conn: Connection, rx: crossbeam_channel::Receiver<Job>, deps: WriterDeps, dead: Arc<AtomicBool>, stats: Arc<WriterStats>) {
    let mut since_prune = 0u64;
    let _ = prune(&conn, &deps.retention);
    loop {
        let first = match rx.recv() {
            Ok(j) => j,
            Err(_) => return,
        };
        let mut batch = vec![first];
        while batch.len() < deps.max_batch {
            match rx.try_recv() {
                Ok(j) => batch.push(j),
                Err(_) => break,
            }
        }
        let mut shutdown = false;
        if let Some(i) = batch.iter().position(|j| matches!(j, Job::Shutdown)) {
            shutdown = true;
            batch.truncate(i);
        }
        if dead.load(Ordering::SeqCst) {
            return; // drop the batch (and its reply channels) unanswered
        }
        if !batch.is_empty() {
            since_prune += 1;
            stats.batches.fetch_add(1, Ordering::Relaxed);
            stats.commit_jobs.fetch_add(batch.iter().filter(|j| matches!(j, Job::Commit(_))).count() as u64, Ordering::Relaxed);
            process(&conn, batch, &deps, &dead, &mut since_prune);
        }
        if shutdown {
            return;
        }
    }
}

struct Done {
    root: i64,
    head: u64,
    changes: Vec<Change>,
    reply: oneshot::Sender<Result<Vec<CommitResult>, ErrorReply>>,
    results: Vec<CommitResult>,
}

fn process(conn: &Connection, batch: Vec<Job>, deps: &WriterDeps, dead: &AtomicBool, since_prune: &mut u64) {
    let internal = |m: String| ErrorReply { code: ErrCode::Internal, msg: m };
    if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE") {
        tracing::error!("begin: {e}");
        for j in batch {
            fail_job(j, internal(format!("db: {e}")));
        }
        return;
    }
    let mut done: Vec<Done> = vec![];
    let mut after: Vec<Box<dyn FnOnce() + Send>> = vec![];
    for job in batch {
        match job {
            Job::Commit(cj) => {
                let _ = conn.execute_batch("SAVEPOINT j");
                match run_commit(conn, &cj, deps) {
                    Ok((results, changes, head)) => {
                        let _ = conn.execute_batch("RELEASE j");
                        done.push(Done { root: cj.root, head, changes, reply: cj.reply, results });
                    }
                    Err(e) => {
                        tracing::error!("commit failed: {e}");
                        let _ = conn.execute_batch("ROLLBACK TO j; RELEASE j");
                        let _ = cj.reply.send(Err(internal(format!("db: {e}"))));
                    }
                }
            }
            Job::Landed { keys, manifest, done: tx } => {
                if let Some((id, data)) = manifest {
                    let _ = conn.execute("INSERT OR IGNORE INTO manifests(id,data) VALUES(?,?)", params![id.0.to_vec(), data]);
                }
                for k in &keys {
                    let _ = conn.execute("DELETE FROM pending WHERE content=?", [enc(k)]);
                }
                after.push(Box::new(move || {
                    let _ = tx.send(());
                }));
            }
            Job::Lost { key, host, done: tx } => {
                let _ = conn.execute("INSERT INTO lost_content(content,host,ts) VALUES(?,?,?)", params![enc(&key), host.0 as i64, now_secs() as i64]);
                after.push(Box::new(move || {
                    let _ = tx.send(());
                }));
            }
            Job::Prune => {
                if let Err(e) = prune_in_txn(conn, &deps.retention) {
                    tracing::warn!("prune: {e}");
                }
            }
            Job::Barrier(tx) => after.push(Box::new(move || {
                let _ = tx.send(());
            })),
            Job::Shutdown => {}
        }
    }
    if *since_prune >= 5000 {
        *since_prune = 0;
        let _ = prune_in_txn(conn, &deps.retention);
    }
    if dead.load(Ordering::SeqCst) {
        let _ = conn.execute_batch("ROLLBACK");
        return;
    }
    match conn.execute_batch("COMMIT") {
        Ok(()) => {
            for d in done {
                deps.hub.publish(d.root, d.head, d.changes);
                let _ = d.reply.send(Ok(d.results));
            }
            for f in after {
                f();
            }
        }
        Err(e) => {
            tracing::error!("commit: {e}");
            let _ = conn.execute_batch("ROLLBACK");
            for d in done {
                let _ = d.reply.send(Err(internal(format!("db: {e}"))));
            }
        }
    }
}

fn fail_job(j: Job, e: ErrorReply) {
    if let Job::Commit(c) = j {
        let _ = c.reply.send(Err(e));
    }
}

fn run_commit(conn: &Connection, cj: &CommitJob, deps: &WriterDeps) -> rusqlite::Result<(Vec<CommitResult>, Vec<Change>, u64)> {
    let record = if deps.record_commits { Some(cj.host.id) } else { None };
    let mut tree = SqlTree::new(conn, cj.root, &deps.leases, &deps.cas, record)?;
    let ctx = Ctx { host: cj.host.id, host_name: &cj.host.name, rules: &cj.host.rules, now_secs: now_secs() };
    let mut results = Vec::with_capacity(cj.commits.len());
    let mut changes = vec![];
    for c in &cj.commits {
        let o = merge::apply(&mut tree, &ctx, c);
        if let Some(h) = tree.record {
            if !o.replay {
                tree.exec("INSERT INTO commit_log(root,host,commit_,result) VALUES(?,?,?,?)", params![tree.root, h.0 as i64, enc(c), enc(&o.result)]);
            }
        }
        results.push(o.result);
        changes.extend(o.changes);
    }
    let head = tree.head;
    tree.finish()?;
    Ok((results, changes, head))
}

/// Log/tombstone/op-result retention. Truncation never changes the epoch.
pub fn prune(conn: &Connection, ret: &Retention) -> rusqlite::Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    match prune_in_txn(conn, ret) {
        Ok(()) => conn.execute_batch("COMMIT"),
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Same, for callers that already hold a write transaction (the writer's batch).
pub fn prune_in_txn(conn: &Connection, ret: &Retention) -> rusqlite::Result<()> {
    let now = now_secs() as i64;
    let cutoff = now - ret.log_secs as i64;
    let mut roots = vec![];
    {
        let mut st = conn.prepare("SELECT id, head FROM roots")?;
        let rows = st.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        for r in rows {
            roots.push(r?);
        }
    }
    (|| {
        for (root, head) in roots {
            let keep_from = head - ret.log_min_changes as i64; // seq <= keep_from may go if old enough
            if keep_from > 0 {
                let max_old: Option<i64> = conn.query_row("SELECT MAX(seq) FROM changes WHERE root=? AND seq<=? AND ts<=?", params![root, keep_from, cutoff], |r| r.get(0))?;
                if let Some(m) = max_old {
                    conn.execute("DELETE FROM changes WHERE root=? AND seq<=?", params![root, m])?;
                    conn.execute("UPDATE roots SET log_floor=MAX(log_floor,?) WHERE id=?", params![m, root])?;
                }
            }
        }
        conn.execute("DELETE FROM tombs WHERE ts<?", [cutoff])?;
        conn.execute("DELETE FROM ops WHERE ts<?", [now - ret.op_window_secs as i64])?;
        Ok(())
    })()
}
