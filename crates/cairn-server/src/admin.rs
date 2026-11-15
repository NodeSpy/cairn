//! Local admin operations on the data directory (the server may be running:
//! SQLite WAL handles the concurrency; hosts and ACLs are read per
//! connection, so changes apply on the next connect or within ~10 s).

use crate::merge::Rules;
use crate::store::{self, enc};
use anyhow::{anyhow, bail, Context, Result};
use cairn_proto::tls::{self, pki};
use cairn_proto::*;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

pub fn pki_dir(data: &Path) -> PathBuf {
    data.join("pki")
}

/// Create the data dir: SQLite schema, CA (ca.pem + ca.key) and the server certificate.
pub fn init(data: &Path) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::create_dir_all(data)?;
    let pki_d = pki_dir(data);
    if pki_d.join("ca.key").exists() {
        bail!("{} is already initialised", data.display());
    }
    std::fs::create_dir_all(&pki_d)?;
    let ca = pki::new_ca("cairn CA")?;
    let srv = pki::issue(&ca, tls::SERVER_NAME)?;
    pki::write_host_dir(&pki_d, &ca.cert, &srv)?;
    let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(pki_d.join("ca.key"))?;
    std::io::Write::write_all(&mut f, ca.key.as_bytes())?;
    std::fs::create_dir_all(data.join("cas"))?;
    let conn = store::open_rw(&data.join("meta.db"))?;
    store::init_schema(&conn)?;
    Ok(())
}

pub struct Admin {
    conn: Connection,
    data: PathBuf,
}

/// Fields to change in `host set`; `None` leaves the field alone, `Some(None)` clears it.
#[derive(Default, Clone, Debug)]
pub struct HostSet {
    pub allowed_uids: Option<Option<String>>,
    pub allowed_gids: Option<Option<String>>,
    pub root_squash: Option<bool>,
    pub uid_map: Option<Option<String>>,
    pub gid_map: Option<Option<String>>,
    pub max_frame: Option<Option<u64>>,
    pub max_commits: Option<Option<u64>>,
}

fn valid_host_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

impl Admin {
    pub fn open(data: &Path) -> Result<Admin> {
        let db = data.join("meta.db");
        if !db.exists() {
            bail!("{} has no meta.db (run `cairn-server init` first)", data.display());
        }
        let conn = store::open_rw(&db)?;
        store::init_schema(&conn)?;
        Ok(Admin { conn, data: data.to_path_buf() })
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    fn ca(&self) -> Result<pki::Pem> {
        let d = pki_dir(&self.data);
        Ok(pki::Pem {
            cert: std::fs::read_to_string(d.join("ca.pem")).context("read ca.pem")?,
            key: std::fs::read_to_string(d.join("ca.key")).context("read ca.key")?,
        })
    }

    /// Issue key+cert, write a client creds dir to `out`, register the fingerprint.
    pub fn add_host(&self, name: &str, out: &Path) -> Result<HostId> {
        if !valid_host_name(name) {
            bail!("invalid host name {name:?} (use letters, digits, . _ -)");
        }
        if store::host_by_name(&self.conn, name)?.is_some() {
            bail!("host {name} already exists");
        }
        let ca = self.ca()?;
        let host = pki::issue(&ca, name)?;
        let fp = tls::fingerprint_of_pem(host.cert.as_bytes())?;
        pki::write_host_dir(out, &ca.cert, &host)?;
        self.conn.execute("INSERT INTO hosts(name, fp) VALUES(?, ?)", params![name, fp.0.to_vec()])?;
        Ok(HostId(self.conn.last_insert_rowid() as u64))
    }

    /// Returns false if there was no such host. Live connections drop within
    /// ~10 s (immediately via `Shared::kick_host` when in-process).
    pub fn revoke_host(&self, name: &str) -> Result<Option<HostId>> {
        let Some(h) = store::host_by_name(&self.conn, name)? else { return Ok(None) };
        self.conn.execute("DELETE FROM acl WHERE host=?", [h.id.0 as i64])?;
        self.conn.execute("DELETE FROM hosts WHERE id=?", [h.id.0 as i64])?;
        Ok(Some(h.id))
    }

    pub fn set_host(&self, name: &str, s: &HostSet) -> Result<()> {
        let h = store::host_by_name(&self.conn, name)?.ok_or_else(|| anyhow!("no such host {name}"))?;
        let id = h.id.0 as i64;
        let norm = |v: &Option<String>, ranges: bool| -> Result<Option<String>> {
            match v {
                None => Ok(None),
                Some(x) if x == "any" || x.is_empty() => Ok(None),
                Some(x) => {
                    if ranges {
                        Rules::parse_ranges(x).map_err(|e| anyhow!(e))?;
                    } else {
                        Rules::parse_map(x).map_err(|e| anyhow!(e))?;
                    }
                    Ok(Some(x.clone()))
                }
            }
        };
        if let Some(v) = &s.allowed_uids {
            self.conn.execute("UPDATE hosts SET allowed_uids=? WHERE id=?", params![norm(v, true)?, id])?;
        }
        if let Some(v) = &s.allowed_gids {
            self.conn.execute("UPDATE hosts SET allowed_gids=? WHERE id=?", params![norm(v, true)?, id])?;
        }
        if let Some(v) = s.root_squash {
            self.conn.execute("UPDATE hosts SET root_squash=? WHERE id=?", params![v as i64, id])?;
        }
        if let Some(v) = &s.uid_map {
            self.conn.execute("UPDATE hosts SET uid_map=? WHERE id=?", params![norm(v, false)?, id])?;
        }
        if let Some(v) = &s.gid_map {
            self.conn.execute("UPDATE hosts SET gid_map=? WHERE id=?", params![norm(v, false)?, id])?;
        }
        if let Some(v) = &s.max_frame {
            self.conn.execute("UPDATE hosts SET max_frame=? WHERE id=?", params![v.map(|x| x as i64), id])?;
        }
        if let Some(v) = &s.max_commits {
            self.conn.execute("UPDATE hosts SET max_commits=? WHERE id=?", params![v.map(|x| x as i64), id])?;
        }
        Ok(())
    }

    /// Create the root if needed and set `acl` (host name, read-write).
    /// Per-host roots (`hosts/<name>/...`) need no ACL entry for their owner.
    pub fn add_root(&self, name: &str, acl: &[(String, bool)]) -> Result<i64> {
        if name.is_empty() || name.starts_with('/') || name.ends_with('/') || name.split('/').any(|c| c.is_empty() || c == "." || c == "..") {
            bail!("invalid root name {name:?}");
        }
        if let Some(rest) = name.strip_prefix("hosts/") {
            let owner = rest.split('/').next().unwrap_or("");
            if store::host_by_name(&self.conn, owner)?.is_none() {
                bail!("per-host root {name}: no such host {owner}");
            }
        }
        let mut ids = vec![];
        for (h, rw) in acl {
            let row = store::host_by_name(&self.conn, h)?.ok_or_else(|| anyhow!("no such host {h}"))?;
            ids.push((row.id, *rw));
        }
        let tx = self.conn.unchecked_transaction()?;
        let rid = match store::root_by_name(&self.conn, name)? {
            Some(r) => r.id,
            None => {
                tx.execute("INSERT INTO roots(name) VALUES(?)", [name])?;
                let rid = tx.last_insert_rowid();
                let attrs = Attrs {
                    kind: Kind::Dir,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                    size: 0,
                    mtime_ns: 0,
                    ctime_ns: 0,
                    atime_ns: 0,
                    rdev: 0,
                    content: None,
                    symlink_target: None,
                    xattrs: vec![],
                };
                tx.execute(
                    "INSERT INTO nodes(root,node,version,cver,mod_host,content_host,attrs) VALUES(?,?,0,0,0,0,?)",
                    params![rid, store::nid(NodeId::ROOT).to_vec(), enc(&attrs)],
                )?;
                rid
            }
        };
        for (h, rw) in ids {
            tx.execute("INSERT OR REPLACE INTO acl(root,host,rw) VALUES(?,?,?)", params![rid, h.0 as i64, rw as i64])?;
        }
        tx.commit()?;
        Ok(rid)
    }

    /// Call after restoring the data dir from a backup: clients see a new epoch and resync.
    pub fn bump_epoch(&self) -> Result<Epoch> {
        let e: u64 = rand::random::<u64>() >> 1;
        self.conn.execute("UPDATE meta SET v=? WHERE k='epoch'", [e.to_be_bytes().to_vec()])?;
        Ok(Epoch(e))
    }

    pub fn list_hosts(&self) -> Result<Vec<String>> {
        let mut st = self.conn.prepare("SELECT id,name,allowed_uids,allowed_gids,root_squash,uid_map,gid_map,max_frame,max_commits FROM hosts ORDER BY id")?;
        let rows = st.query_map([], |r| {
            let o = |i: usize| r.get::<_, Option<String>>(i).map(|x| x.unwrap_or_else(|| "any".into()));
            Ok(format!(
                "{}\t{}\tuids={} gids={} root_squash={} uid_map={} gid_map={} max_frame={:?} max_commits={:?}",
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                o(2)?,
                o(3)?,
                r.get::<_, i64>(4)? != 0,
                o(5)?,
                o(6)?,
                r.get::<_, Option<i64>>(7)?,
                r.get::<_, Option<i64>>(8)?
            ))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn list_roots(&self) -> Result<Vec<String>> {
        let mut st = self.conn.prepare(
            "SELECT r.name, r.head, COALESCE(group_concat(h.name || ':' || CASE a.rw WHEN 1 THEN 'rw' ELSE 'ro' END, ','), '') \
             FROM roots r LEFT JOIN acl a ON a.root=r.id LEFT JOIN hosts h ON h.id=a.host GROUP BY r.id ORDER BY r.name",
        )?;
        let rows = st.query_map([], |r| Ok(format!("{}\thead={}\tacl={}", r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}
