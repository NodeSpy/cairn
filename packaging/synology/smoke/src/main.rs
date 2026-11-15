use cairn_proto::*;
use cairn_server::testing::TestClient;

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (addr, creds, root) = (a[1].parse().unwrap(), std::path::PathBuf::from(&a[2]), a[3].clone());
    let c = TestClient::connect(addr, &creds).await.expect("connect/hello");
    println!("hello ok, host {}", c.host_name);
    let (h, head, w) = c.mount(&root).await.expect("mount");
    println!("mount ok head={head:?} writable={w}");
    let data = b"hello from the devbox smoke test".to_vec();
    let (n, r) = c.create_file(h, NodeId::ROOT, "smoke.txt", &data).await.expect("create");
    println!("commit (PutBlob+Create) -> {r:?}");
    let ents = c.readdir_all(h, NodeId::ROOT).await.expect("readdir");
    assert!(ents.iter().any(|e| e.node == n), "created node missing");
    println!("readdir ok: {} entries", ents.len());
    let ContentId::Blob(hash) = ents.iter().find(|e| e.node == n).unwrap().attrs.content.unwrap() else { panic!("not a blob") };
    match c.call(Request::GetBlob { hash }).await.expect("getblob") {
        Response::Blob { data: d } => assert_eq!(d, data),
        o => panic!("unexpected {o:?}"),
    }
    println!("getblob ok, content matches (hash {hash:?})");
    println!("SMOKE PASS");
}
