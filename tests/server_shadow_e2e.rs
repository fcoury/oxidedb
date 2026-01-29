use bson::doc;
use oxidedb::config::{Config, ShadowConfig};
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn upstream_addr() -> Option<String> {
    std::env::var("OXIDEDB_TEST_MONGODB_ADDR").ok()
}

async fn read_one_op_msg(stream: &mut TcpStream) -> bson::Document {
    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await.unwrap();
    let (hdr, _) = MessageHeader::parse(&header).unwrap();
    assert_eq!(hdr.op_code, OP_MSG);
    let mut body = vec![0u8; (hdr.message_length as usize) - 16];
    stream.read_exact(&mut body).await.unwrap();
    let (_flags, doc) = decode_op_msg_section0(&body).unwrap();
    doc
}

#[tokio::test]
async fn e2e_shadow_hello_ping_buildinfo() {
    let mongo = match upstream_addr() {
        Some(a) => a,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_MONGODB_ADDR");
            return;
        }
    };

    // Build config with ephemeral port and shadow enabled
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: None,
        timeout_ms: 1500,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: None,
        password: None,
        auth_db: "admin".to_string(),
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();

    // Connect to server
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // hello
    let hello = doc! {"hello": 1i32, "$db": "admin"};
    let msg = encode_op_msg(&hello, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // ping
    let ping = doc! {"ping": 1i32, "$db": "admin"};
    let msg = encode_op_msg(&ping, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // buildInfo
    let build = doc! {"buildInfo": 1i32, "$db": "admin"};
    let msg = encode_op_msg(&build, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Allow shadow tasks to complete
    tokio::time::sleep(Duration::from_millis(300)).await;

    // We expect at least 3 attempts (one per cmd) given sample_rate=1.0
    let attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        attempts >= 3,
        "expected >=3 shadow attempts, got {}",
        attempts
    );

    // Shut down
    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
