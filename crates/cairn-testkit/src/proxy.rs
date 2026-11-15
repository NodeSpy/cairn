//! Fault-injecting TCP proxy (byte level, so it works under TLS): one per
//! client. Supports partition (refuse/blackhole + reset live connections),
//! added delay, and resetting connections after N more bytes (mid-frame).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Default)]
struct State {
    partitioned: AtomicBool,
    delay_ms: AtomicU64,
    /// Bytes (either direction) until the next connection is cut; <0 = off.
    cut_after: AtomicI64,
    target: std::sync::Mutex<Option<SocketAddr>>,
    bytes: AtomicU64,
}

pub struct Proxy {
    addr: SocketAddr,
    st: Arc<State>,
    kill: watch::Sender<u64>,
}

impl Proxy {
    /// Listen on an ephemeral loopback port, forwarding to `target`.
    pub async fn start(target: SocketAddr) -> std::io::Result<Proxy> {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        let st = Arc::new(State { cut_after: AtomicI64::new(-1), ..Default::default() });
        *st.target.lock().unwrap() = Some(target);
        let (kill, rx) = watch::channel(0u64);
        let st2 = st.clone();
        tokio::spawn(async move {
            loop {
                let Ok((c, _)) = l.accept().await else { return };
                if st2.partitioned.load(SeqCst) {
                    drop(c); // refuse: connection closes immediately
                    continue;
                }
                let (st, mut rx) = (st2.clone(), rx.clone());
                rx.borrow_and_update(); // only resets issued after this connection count
                tokio::spawn(async move {
                    let _ = pipe(c, st, rx).await;
                });
            }
        });
        Ok(Proxy { addr, st, kill })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Re-point at a restarted server (new port).
    pub fn set_target(&self, t: SocketAddr) {
        *self.st.target.lock().unwrap() = Some(t);
    }

    pub fn partition(&self) {
        self.st.partitioned.store(true, SeqCst);
        self.reset_all();
    }

    pub fn heal(&self) {
        self.st.partitioned.store(false, SeqCst);
    }

    pub fn is_partitioned(&self) -> bool {
        self.st.partitioned.load(SeqCst)
    }

    pub fn set_delay(&self, ms: u64) {
        self.st.delay_ms.store(ms, SeqCst);
    }

    /// Abort every live connection now.
    pub fn reset_all(&self) {
        self.kill.send_modify(|v| *v += 1);
    }

    /// Cut the next connection after `n` more bytes (likely mid-frame).
    pub fn cut_after(&self, n: i64) {
        self.st.cut_after.store(n, SeqCst);
    }

    pub fn bytes_forwarded(&self) -> u64 {
        self.st.bytes.load(SeqCst)
    }
}

async fn pipe(client: TcpStream, st: Arc<State>, mut kill: watch::Receiver<u64>) -> std::io::Result<()> {
    let target = st.target.lock().unwrap().unwrap();
    let server = TcpStream::connect(target).await?;
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let st1 = st.clone();
    let st2 = st.clone();
    async fn copy(r: &mut tokio::net::tcp::OwnedReadHalf, w: &mut tokio::net::tcp::OwnedWriteHalf, st: Arc<State>) -> std::io::Result<()> {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = r.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            let d = st.delay_ms.load(SeqCst);
            if d > 0 {
                tokio::time::sleep(Duration::from_millis(d)).await;
            }
            if st.partitioned.load(SeqCst) {
                return Err(std::io::ErrorKind::ConnectionReset.into());
            }
            let cut = st.cut_after.load(SeqCst);
            if cut >= 0 {
                if (n as i64) >= cut {
                    // forward only part of this read, then die mid-frame
                    let k = cut as usize;
                    let _ = w.write_all(&buf[..k]).await;
                    st.cut_after.store(-1, SeqCst);
                    return Err(std::io::ErrorKind::ConnectionReset.into());
                }
                st.cut_after.fetch_sub(n as i64, SeqCst);
            }
            st.bytes.fetch_add(n as u64, SeqCst);
            w.write_all(&buf[..n]).await?;
        }
    }
    tokio::select! {
        r = copy(&mut cr, &mut sw, st1) => r,
        r = copy(&mut sr, &mut cw, st2) => r,
        _ = kill.changed() => Err(std::io::ErrorKind::ConnectionReset.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn forwards_partitions_and_cuts() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ta = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut b = [0u8; 1024];
                    while let Ok(n) = s.read(&mut b).await {
                        if n == 0 || s.write_all(&b[..n]).await.is_err() { break }
                    }
                });
            }
        });
        let p = Proxy::start(ta).await.unwrap();
        let mut c = TcpStream::connect(p.addr()).await.unwrap();
        c.write_all(b"hello").await.unwrap();
        let mut b = [0u8; 5];
        c.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"hello");
        p.partition();
        let mut one = [0u8; 1];
        assert!(matches!(c.read(&mut one).await, Ok(0) | Err(_)));
        let mut c2 = TcpStream::connect(p.addr()).await.unwrap();
        c2.write_all(b"x").await.ok();
        assert!(matches!(c2.read(&mut one).await, Ok(0) | Err(_)));
        p.heal();
        p.cut_after(3);
        let mut c3 = TcpStream::connect(p.addr()).await.unwrap();
        c3.write_all(b"abcdef").await.unwrap();
        let mut got = vec![];
        let _ = c3.read_to_end(&mut got).await;
        assert!(got.len() <= 3, "{got:?}");
    }
}
