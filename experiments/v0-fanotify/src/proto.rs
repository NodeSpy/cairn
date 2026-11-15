//! Wire protocol: one TCP connection per client, frames of
//! `u32 header_len | u32 payload_len | JSON header | raw payload`.
//! Requests carry a non-zero id and get exactly one reply with the same id;
//! server pushes (Recall, Changed) use id 0.

use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    File,
    Dir,
    Symlink,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Meta {
    pub path: String,
    pub kind: Kind,
    pub size: u64,
    pub mtime_ns: i64,
    pub mode: u32,
    pub version: u64,
    /// Symlink target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

/// A namespace/content change, broadcast to every other client.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Op {
    Put(Meta),
    Mkdir(Meta),
    Symlink(Meta),
    Remove { path: String },
    Rename { from: String, to: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Body {
    // handshake
    Hello { name: String },
    Welcome { client_id: u64 },
    // reads
    List,
    Listing { entries: Vec<Meta>, seq: u64 },
    Get { path: String },
    /// Reply to Get; payload = file contents.
    Data(Meta),
    // write leases
    Acquire { path: String },
    Granted { version: u64 },
    Released { path: String },
    // mutations (payload = contents for Put)
    Put { path: String, mtime_ns: i64, mode: u32 },
    PutOk { version: u64 },
    Mkdir { path: String, mode: u32 },
    Symlink { path: String, target: String },
    Remove { path: String },
    Rename { from: String, to: String },
    Ok { seq: u64 },
    Err { msg: String },
    // server -> client pushes
    Recall { path: String },
    Changed { seq: u64, from_client: u64, op: Op },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Header {
    pub id: u64,
    pub body: Body,
}

#[derive(Debug)]
pub struct Frame {
    pub id: u64,
    pub body: Body,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(id: u64, body: Body) -> Self {
        Frame { id, body, payload: Vec::new() }
    }
    pub fn with_payload(id: u64, body: Body, payload: Vec<u8>) -> Self {
        Frame { id, body, payload }
    }
}

pub fn write_frame<W: Write>(w: &mut W, f: &Frame) -> io::Result<()> {
    let h = serde_json::to_vec(&Header { id: f.id, body: f.body.clone() })?;
    let mut buf = Vec::with_capacity(8 + h.len() + f.payload.len());
    buf.extend_from_slice(&(h.len() as u32).to_be_bytes());
    buf.extend_from_slice(&(f.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&h);
    buf.extend_from_slice(&f.payload);
    w.write_all(&buf)?;
    w.flush()
}

pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Frame> {
    let mut lens = [0u8; 8];
    r.read_exact(&mut lens)?;
    let hl = u32::from_be_bytes(lens[0..4].try_into().unwrap()) as usize;
    let pl = u32::from_be_bytes(lens[4..8].try_into().unwrap()) as usize;
    let mut h = vec![0u8; hl];
    r.read_exact(&mut h)?;
    let mut payload = vec![0u8; pl];
    r.read_exact(&mut payload)?;
    let hdr: Header = serde_json::from_slice(&h)?;
    Ok(Frame { id: hdr.id, body: hdr.body, payload })
}

/// Reject absolute paths and `..` components; returns the normalized relative path.
pub fn check_rel(p: &str) -> io::Result<&str> {
    if p.is_empty() || p.starts_with('/') || p.split('/').any(|c| c == ".." || c == "." || c.is_empty()) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("bad path {p:?}")));
    }
    Ok(p)
}
