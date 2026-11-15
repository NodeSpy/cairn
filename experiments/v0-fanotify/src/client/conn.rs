//! Client side of the wire protocol: multiplexed request/response plus a
//! push channel for server-initiated messages (Recall, Changed).

use crate::log;
use crate::proto::*;
use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

pub struct Conn {
    wr: Mutex<BufWriter<TcpStream>>,
    pending: Mutex<HashMap<u64, Sender<Frame>>>,
    next: AtomicU64,
}

impl Conn {
    pub fn connect(addr: &str, name: &str, push: Sender<Frame>) -> Result<(Arc<Conn>, u64)> {
        let s = TcpStream::connect(addr)?;
        s.set_nodelay(true).ok();
        let rd = s.try_clone()?;
        let conn = Arc::new(Conn {
            wr: Mutex::new(BufWriter::new(s)),
            pending: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        });
        let c2 = conn.clone();
        std::thread::Builder::new().name("conn-rd".into()).spawn(move || {
            let mut rd = BufReader::new(rd);
            loop {
                match read_frame(&mut rd) {
                    Ok(f) if f.id == 0 => {
                        push.send(f).ok();
                    }
                    Ok(f) => {
                        if let Some(tx) = c2.pending.lock().unwrap().remove(&f.id) {
                            tx.send(f).ok();
                        }
                    }
                    Err(e) => {
                        // v0: no reconnect. Exiting closes the fanotify groups,
                        // which lets every blocked access through.
                        log!("conn", "server connection lost: {e}; exiting");
                        std::process::exit(2);
                    }
                }
            }
        })?;
        let r = conn.call(Body::Hello { name: name.to_string() }, Vec::new())?;
        let cid = match r.body {
            Body::Welcome { client_id } => client_id,
            b => bail!("unexpected handshake reply {b:?}"),
        };
        Ok((conn, cid))
    }

    pub fn call(&self, body: Body, payload: Vec<u8>) -> Result<Frame> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        self.pending.lock().unwrap().insert(id, tx);
        {
            let mut w = self.wr.lock().unwrap();
            write_frame(&mut *w, &Frame::with_payload(id, body, payload))?;
        }
        let f = rx.recv().map_err(|_| anyhow!("connection closed"))?;
        if let Body::Err { msg } = &f.body {
            bail!("server: {msg}");
        }
        Ok(f)
    }
}
