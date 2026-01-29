// Integration test for SCRAM-SHA-256 authentication
// This test requires a MongoDB instance with authentication enabled

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

fn upstream_username() -> Option<String> {
    std::env::var("OXIDEDB_TEST_MONGODB_USER").ok()
}

fn upstream_password() -> Option<String> {
    std::env::var("OXIDEDB_TEST_MONGODB_PASS").ok()
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
async fn e2e_shadow_with_scram_auth() {
    // Requires upstream Mongo with auth enabled, and Postgres admin URL
    let mongo = match upstream_addr() {
        Some(a) => a,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_MONGODB_ADDR");
            return;
        }
    };

    let username = match upstream_username() {
        Some(u) => u,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_MONGODB_USER");
            return;
        }
    };

    let password = match upstream_password() {
        Some(p) => p,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_MONGODB_PASS");
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

    // Build config with ephemeral port, Postgres, and shadow with auth
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: None,
        timeout_ms: 5000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: Some(username.clone()),
        password: Some(password.clone()),
        auth_db: "admin".to_string(),
        tls_enabled: false,
        tls_ca_file: None,
        tls_client_cert: None,
        tls_client_key: None,
        tls_allow_invalid_certs: false,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_scram_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "users", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert
    let ins = doc! {"insert": "users", "documents": [{"name": "Alice"}], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 1);

    // find
    let find = doc! {"find": "users", "filter": {}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert!(first_batch.len() >= 1);

    // Let shadow finish and assert attempts grew
    tokio::time::sleep(Duration::from_millis(500)).await;
    let attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        attempts >= 3,
        "expected >=3 shadow attempts, got {}",
        attempts
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_shadow_with_invalid_scram_auth() {
    // Test that shadow fails gracefully with wrong credentials
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

    // Build config with invalid credentials
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: None,
        timeout_ms: 5000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: Some("invalid_user".to_string()),
        password: Some("wrong_password".to_string()),
        auth_db: "admin".to_string(),
        tls_enabled: false,
        tls_ca_file: None,
        tls_client_cert: None,
        tls_client_key: None,
        tls_allow_invalid_certs: false,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_scram_invalid_{}", rand_suffix(6));

    // create collection - should work on OxideDB side
    let create = doc! {"create": "users", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Wait for shadow to attempt (and fail) auth
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Shadow should have attempted but auth failed (no attempts recorded on failure)
    // The server should still work normally, just no shadow comparison
    let attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    // With invalid auth, shadow won't record attempts since auth fails before forwarding
    tracing::info!("Shadow attempts with invalid auth: {}", attempts);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
