//! Framing: `u32 big-endian length | bincode(Envelope)`. Runs over TLS.

use crate::msg::Envelope;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Read, Write};

/// Hard cap on one frame (largest payload is a CDC_MAX chunk or a page of rows).
pub const MAX_FRAME: usize = 16 << 20;

fn cfg() -> bincode::config::Configuration {
    bincode::config::standard()
}

pub fn encode<T: Serialize>(v: &T) -> io::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(v, cfg()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn decode<T: DeserializeOwned>(b: &[u8]) -> io::Result<T> {
    let (v, n) = bincode::serde::decode_from_slice(b, cfg()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if n != b.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "trailing bytes in frame"));
    }
    Ok(v)
}

pub fn write_envelope<W: Write>(w: &mut W, env: &Envelope) -> io::Result<()> {
    let body = encode(env)?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
    }
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    w.write_all(&buf)?;
    w.flush()
}

pub fn read_envelope<R: Read>(r: &mut R) -> io::Result<Envelope> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    decode(&body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    #[test]
    fn roundtrip() {
        let env = Envelope {
            id: 7,
            body: Body::Request(Request::Commit {
                root: 1,
                commits: vec![Commit {
                    op_id: OpId { host: HostId(3), n: 9 },
                    cred: Cred { uid: 1000, gid: 1000 },
                    op: Op::Unlink { parent: NodeId::ROOT, name: Name(b"a".to_vec()), node: NodeId::new(HostId(3), 5), base: Seq(4) },
                }],
            }),
        };
        let mut buf = Vec::new();
        write_envelope(&mut buf, &env).unwrap();
        let back = read_envelope(&mut &buf[..]).unwrap();
        assert_eq!(env, back);
    }
}
