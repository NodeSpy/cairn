//! Async framing over any tokio stream (TLS in production). Same wire
//! format as `frame`: `u32 BE length | bincode(Envelope)`.

use crate::frame::{decode, encode, MAX_FRAME};
use crate::msg::Envelope;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn write_envelope<W: AsyncWrite + Unpin>(w: &mut W, env: &Envelope) -> io::Result<()> {
    let body = encode(env)?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
    }
    w.write_all(&(body.len() as u32).to_be_bytes()).await?;
    w.write_all(&body).await?;
    w.flush().await
}

/// Returns Ok(None) on clean EOF at a frame boundary.
pub async fn read_envelope<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<Envelope>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    decode(&body).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::{self, pki};
    use crate::*;
    use std::sync::Arc;

    /// Full mTLS handshake + one request/response with freshly minted certs.
    #[tokio::test]
    async fn mtls_roundtrip() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let ca = pki::new_ca("test-ca").unwrap();
        let sdir = tempfile::tempdir().unwrap();
        let cdir = tempfile::tempdir().unwrap();
        pki::write_host_dir(sdir.path(), &ca.cert, &pki::issue(&ca, tls::SERVER_NAME).unwrap()).unwrap();
        let host = pki::issue(&ca, "lfs-a").unwrap();
        let fp = tls::fingerprint_of_pem(host.cert.as_bytes()).unwrap();
        pki::write_host_dir(cdir.path(), &ca.cert, &host).unwrap();

        let lst = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lst.local_addr().unwrap();
        let acc = tokio_rustls::TlsAcceptor::from(tls::server_config(sdir.path()).unwrap());
        let srv = tokio::spawn(async move {
            let (s, _) = lst.accept().await.unwrap();
            let mut s = acc.accept(s).await.unwrap();
            assert_eq!(tls::peer_fingerprint(s.get_ref().1).unwrap(), fp);
            let req = read_envelope(&mut s).await.unwrap().unwrap();
            write_envelope(&mut s, &Envelope { id: req.id, body: Body::Response(Ok(Response::Ok)) }).await.unwrap();
        });
        let con = tokio_rustls::TlsConnector::from(tls::client_config(cdir.path()).unwrap());
        let s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut s = con.connect(tls::server_name(), s).await.unwrap();
        write_envelope(&mut s, &Envelope { id: 1, body: Body::Request(Request::Ping) }).await.unwrap();
        let resp = read_envelope(&mut s).await.unwrap().unwrap();
        assert_eq!(resp, Envelope { id: 1, body: Body::Response(Ok(Response::Ok)) });
        srv.await.unwrap();
        let _ = Arc::new(());
    }
}
