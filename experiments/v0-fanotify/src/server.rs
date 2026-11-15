//! Authoritative store + lease table + change broadcast.
//!
//! The store is a plain directory written at local speed (run this next to
//! the disks, e.g. on the NAS). One thread per connection; Acquire runs on
//! its own thread because it can block on a recall from another client.

use crate::log;
use crate::proto::*;
use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub struct Opts {
    pub store: PathBuf,
    pub listen: String,
    /// Artificial delay before every new write-lease grant (testing).
    pub grant_delay: Duration,
    pub recall_timeout: Duration,
}

#[derive(Default)]
struct State {
    seq: u64,
    next_client: u64,
    versions: HashMap<String, u64>,
    wleases: HashMap<String, u64>,
    recalled: HashSet<String>,
    clients: HashMap<u64, Sender<Frame>>,
}

struct Server {
    opts: Opts,
    st: Mutex<State>,
    cv: Condvar,
}

const TMP_PREFIX: &str = ".leasefs.";

pub fn run(opts: Opts) -> Result<()> {
    fs::create_dir_all(&opts.store)?;
    let listener = TcpListener::bind(&opts.listen).with_context(|| format!("bind {}", opts.listen))?;
    log!("server", "listening on {} store={}", opts.listen, opts.store.display());
    let srv = Arc::new(Server { opts, st: Mutex::new(State::default()), cv: Condvar::new() });
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nodelay(true).ok();
        let srv = srv.clone();
        std::thread::spawn(move || {
            if let Err(e) = handle_conn(srv, stream) {
                log!("server", "connection ended: {e:#}");
            }
        });
    }
    Ok(())
}

fn handle_conn(srv: Arc<Server>, stream: TcpStream) -> Result<()> {
    let mut rd = BufReader::new(stream.try_clone()?);
    let mut wr = BufWriter::new(stream);
    let (tx, rx) = channel::<Frame>();
    std::thread::spawn(move || {
        for f in rx {
            if write_frame(&mut wr, &f).is_err() {
                break;
            }
        }
    });

    let hello = read_frame(&mut rd)?;
    let name = match hello.body {
        Body::Hello { name } => name,
        _ => bail!("expected Hello"),
    };
    let cid = {
        let mut st = srv.st.lock().unwrap();
        st.next_client += 1;
        let cid = st.next_client;
        st.clients.insert(cid, tx.clone());
        cid
    };
    log!("server", "client {cid} ({name}) connected");
    tx.send(Frame::new(hello.id, Body::Welcome { client_id: cid })).ok();

    let res = (|| -> Result<()> {
        loop {
            let f = read_frame(&mut rd)?;
            let id = f.id;
            // Acquire and Put can block on a recall from another client; run
            // them off the reader thread so this client's Released messages
            // keep flowing (otherwise two clients recalling each other deadlock).
            let blocking = matches!(f.body, Body::Acquire { .. } | Body::Put { .. });
            let srv2 = srv.clone();
            let tx2 = tx.clone();
            let work = move || {
                let reply = match srv2.dispatch(cid, f) {
                    Ok(fr) => fr,
                    Err(e) => Frame::new(id, Body::Err { msg: format!("{e:#}") }),
                };
                tx2.send(Frame { id, ..reply }).ok();
            };
            if blocking {
                std::thread::spawn(work);
            } else {
                work();
            }
        }
    })();

    // Disconnect: drop the client's leases so waiters can proceed.
    let mut st = srv.st.lock().unwrap();
    st.clients.remove(&cid);
    st.wleases.retain(|_, h| *h != cid);
    srv.cv.notify_all();
    log!("server", "client {cid} ({name}) disconnected");
    res
}

impl Server {
    fn abs(&self, rel: &str) -> Result<PathBuf> {
        Ok(self.opts.store.join(check_rel(rel)?))
    }

    fn acquire(&self, cid: u64, path: &str) -> Result<u64> {
        check_rel(path)?;
        let deadline = Instant::now() + self.opts.recall_timeout;
        let mut st = self.st.lock().unwrap();
        let mut fresh = false;
        loop {
            match st.wleases.get(path).copied() {
                None => {
                    st.wleases.insert(path.to_string(), cid);
                    st.recalled.remove(path);
                    fresh = true;
                    break;
                }
                Some(h) if h == cid => break,
                Some(h) => {
                    if !st.recalled.contains(path) {
                        log!("server", "recall {path} from client {h} (wanted by {cid})");
                        if let Some(tx) = st.clients.get(&h) {
                            tx.send(Frame::new(0, Body::Recall { path: path.to_string() })).ok();
                        }
                        st.recalled.insert(path.to_string());
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        log!("server", "recall of {path} from {h} timed out; breaking lease");
                        st.wleases.insert(path.to_string(), cid);
                        st.recalled.remove(path);
                        fresh = true;
                        break;
                    }
                    st = self.cv.wait_timeout(st, deadline - now).unwrap().0;
                }
            }
        }
        let v = st.versions.get(path).copied().unwrap_or(0);
        drop(st);
        if fresh {
            log!("server", "grant {path} -> client {cid} (v{v})");
            if !self.opts.grant_delay.is_zero() {
                std::thread::sleep(self.opts.grant_delay);
            }
        }
        Ok(v)
    }

    /// Bump seq and send `op` to every client except `from`. Caller holds the lock.
    fn broadcast(st: &mut State, from: u64, op: Op) -> u64 {
        st.seq += 1;
        let seq = st.seq;
        for (id, tx) in &st.clients {
            if *id != from {
                tx.send(Frame::new(0, Body::Changed { seq, from_client: from, op: op.clone() })).ok();
            }
        }
        seq
    }

    fn meta(&self, st: &State, rel: &str) -> Result<Meta> {
        let p = self.abs(rel)?;
        let md = fs::symlink_metadata(&p).with_context(|| format!("stat {rel}"))?;
        let ft = md.file_type();
        let (kind, target) = if ft.is_dir() {
            (Kind::Dir, None)
        } else if ft.is_symlink() {
            (Kind::Symlink, Some(fs::read_link(&p)?.to_string_lossy().into_owned()))
        } else {
            (Kind::File, None)
        };
        Ok(Meta {
            path: rel.to_string(),
            kind,
            size: if kind == Kind::File { md.len() } else { 0 },
            mtime_ns: md.mtime() * 1_000_000_000 + md.mtime_nsec(),
            mode: md.mode() & 0o7777,
            version: st.versions.get(rel).copied().unwrap_or(0),
            target,
        })
    }

    fn dispatch(&self, cid: u64, f: Frame) -> Result<Frame> {
        let id = f.id;
        Ok(match f.body {
            Body::Acquire { path } => Frame::new(id, Body::Granted { version: self.acquire(cid, &path)? }),
            Body::List => {
                let st = self.st.lock().unwrap();
                let mut entries = Vec::new();
                self.walk(&st, "", &mut entries)?;
                Frame::new(id, Body::Listing { entries, seq: st.seq })
            }
            Body::Get { path } => {
                let st = self.st.lock().unwrap();
                let m = self.meta(&st, &path)?;
                drop(st);
                if m.kind != Kind::File {
                    bail!("{path} is not a file");
                }
                let data = fs::read(self.abs(&path)?)?;
                Frame::with_payload(id, Body::Data(Meta { size: data.len() as u64, ..m }), data)
            }
            Body::Put { path, mtime_ns, mode } => {
                // Writing implies holding the write lease; take it if we don't.
                self.acquire(cid, &path)?;
                let p = self.abs(&path)?;
                let tmp = p.with_file_name(format!("{TMP_PREFIX}{cid}.{id}.tmp"));
                fs::write(&tmp, &f.payload)?;
                fs::set_permissions(&tmp, fs::Permissions::from_mode(mode & 0o7777))?;
                crate::sys::set_mtime_path_nofollow(&tmp, mtime_ns)?;
                fs::rename(&tmp, &p)?;
                let mut st = self.st.lock().unwrap();
                let version = st.seq + 1;
                st.versions.insert(path.clone(), version);
                let m = self.meta(&st, &path)?;
                Self::broadcast(&mut st, cid, Op::Put(m));
                Frame::new(id, Body::PutOk { version })
            }
            Body::Mkdir { path, mode } => {
                let p = self.abs(&path)?;
                match fs::create_dir(&p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e.into()),
                }
                fs::set_permissions(&p, fs::Permissions::from_mode(mode & 0o7777)).ok();
                let mut st = self.st.lock().unwrap();
                let m = self.meta(&st, &path)?;
                let seq = Self::broadcast(&mut st, cid, Op::Mkdir(m));
                Frame::new(id, Body::Ok { seq })
            }
            Body::Symlink { path, target } => {
                let p = self.abs(&path)?;
                let _ = fs::remove_file(&p);
                std::os::unix::fs::symlink(&target, &p)?;
                let mut st = self.st.lock().unwrap();
                let m = self.meta(&st, &path)?;
                let seq = Self::broadcast(&mut st, cid, Op::Symlink(m));
                Frame::new(id, Body::Ok { seq })
            }
            Body::Remove { path } => {
                let p = self.abs(&path)?;
                match fs::symlink_metadata(&p) {
                    Ok(md) if md.is_dir() => fs::remove_dir_all(&p)?,
                    Ok(_) => fs::remove_file(&p)?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                let mut st = self.st.lock().unwrap();
                let pre = format!("{path}/");
                st.versions.retain(|k, _| k != &path && !k.starts_with(&pre));
                st.wleases.retain(|k, _| k != &path && !k.starts_with(&pre));
                let seq = Self::broadcast(&mut st, cid, Op::Remove { path });
                self.cv.notify_all();
                Frame::new(id, Body::Ok { seq })
            }
            Body::Rename { from, to } => {
                fs::rename(self.abs(&from)?, self.abs(&to)?)?;
                let mut st = self.st.lock().unwrap();
                rekey(&mut st.versions, &from, &to);
                rekey(&mut st.wleases, &from, &to);
                let seq = Self::broadcast(&mut st, cid, Op::Rename { from, to });
                self.cv.notify_all();
                Frame::new(id, Body::Ok { seq })
            }
            Body::Released { path } => {
                let mut st = self.st.lock().unwrap();
                if st.wleases.get(&path) == Some(&cid) {
                    st.wleases.remove(&path);
                }
                st.recalled.remove(&path);
                self.cv.notify_all();
                log!("server", "client {cid} released {path}");
                Frame::new(id, Body::Ok { seq: st.seq })
            }
            other => bail!("unexpected request {other:?}"),
        })
    }

    fn walk(&self, st: &State, rel: &str, out: &mut Vec<Meta>) -> Result<()> {
        let dir: &Path = &if rel.is_empty() { self.opts.store.clone() } else { self.abs(rel)? };
        let mut names: Vec<String> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with(TMP_PREFIX))
            .collect();
        names.sort();
        for n in names {
            let child = if rel.is_empty() { n } else { format!("{rel}/{n}") };
            let m = self.meta(st, &child)?;
            let is_dir = m.kind == Kind::Dir;
            out.push(m);
            if is_dir {
                self.walk(st, &child, out)?;
            }
        }
        Ok(())
    }
}

/// Move `from` (and everything under `from/`) to `to` in a path-keyed map.
pub fn rekey<V>(m: &mut HashMap<String, V>, from: &str, to: &str) {
    let pre = format!("{from}/");
    let keys: Vec<String> = m.keys().filter(|k| *k == from || k.starts_with(&pre)).cloned().collect();
    for k in keys {
        let v = m.remove(&k).unwrap();
        let nk = format!("{to}{}", &k[from.len()..]);
        m.insert(nk, v);
    }
}
