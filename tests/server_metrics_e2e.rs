use bson::doc;
use oxidedb::config::{Config, ShadowConfig};
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "common/postgres.rs"]
mod pg;

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

fn rand_suffix(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

#[tokio::test]
async fn e2e_oxidedb_shadow_metrics() {
    // Requires both upstream Mongo and Postgres admin URL
    let mongo = match upstream_addr() {
        Some(a) => a,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_MONGODB_ADDR");
            return;
        }
    };
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    // Build config with ephemeral port, Postgres, and shadow
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: None,
        timeout_ms: 2000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: None,
        password: None,
        auth_db: "admin".to_string(),
        tls_enabled: false,
        tls_ca_file: None,
        tls_client_cert: None,
        tls_client_key: None,
        tls_allow_invalid_certs: false,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_metrics_{}", rand_suffix(6));

    // Run some commands to generate shadow metrics
    for i in 1..=3 {
        let ping = doc! {"ping": 1i32, "$db": &dbname};
        let msg = encode_op_msg(&ping, 0, i);
        stream.write_all(&msg).await.unwrap();
        let doc = read_one_op_msg(&mut stream).await;
        assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    }

    // Wait for shadow to finish
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Query shadow metrics via admin command
    let metrics_cmd = doc! {"oxidedbShadowMetrics": 1i32, "$db": "admin"};
    let msg = encode_op_msg(&metrics_cmd, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify shadow metrics structure
    let shadow = doc.get_document("shadow").expect("shadow field");
    let attempts = shadow.get_i64("attempts").expect("attempts");
    let matches = shadow.get_i64("matches").expect("matches");
    let mismatches = shadow.get_i64("mismatches").expect("mismatches");
    let timeouts = shadow.get_i64("timeouts").expect("timeouts");

    // Verify metrics are present and non-negative
    assert!(attempts >= 3, "expected >=3 attempts, got {}", attempts);
    assert!(matches >= 0, "expected >=0 matches, got {}", matches);
    assert!(
        mismatches >= 0,
        "expected >=0 mismatches, got {}",
        mismatches
    );
    assert!(timeouts >= 0, "expected >=0 timeouts, got {}", timeouts);

    // Verify state matches
    let state_attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(attempts, state_attempts as i64);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_oxidedb_shadow_metrics_no_shadow() {
    // Test metrics endpoint when shadow is disabled
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };

    // Build config with shadow disabled
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = None;

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_metrics_noshadow_{}", rand_suffix(6));

    // Query shadow metrics via admin command
    let metrics_cmd = doc! {"oxidedbShadowMetrics": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&metrics_cmd, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify shadow metrics are all zero when shadow is disabled
    let shadow = doc.get_document("shadow").expect("shadow field");
    assert_eq!(shadow.get_i64("attempts").unwrap_or(-1), 0);
    assert_eq!(shadow.get_i64("matches").unwrap_or(-1), 0);
    assert_eq!(shadow.get_i64("mismatches").unwrap_or(-1), 0);
    assert_eq!(shadow.get_i64("timeouts").unwrap_or(-1), 0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
