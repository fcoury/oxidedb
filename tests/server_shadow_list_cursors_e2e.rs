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
async fn e2e_shadow_list_indexes() {
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

    // Build config with ephemeral port, Postgres, and shadow with db_prefix
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: Some("shadow".to_string()),
        timeout_ms: 2000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: None,
        password: None,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_listidx_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "users", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // createIndexes with multiple indexes
    let idx_spec1 = doc! {"name": "email_1", "key": {"email": 1i32}};
    let idx_spec2 = doc! {"name": "name_1_age_-1", "key": {"name": 1i32, "age": -1i32}};
    let ci = doc! {"createIndexes": "users", "indexes": [idx_spec1, idx_spec2], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 2);

    // listIndexes
    let li = doc! {"listIndexes": "users", "$db": &dbname};
    let msg = encode_op_msg(&li, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should have at least 3 indexes: _id_, email_1, name_1_age_-1
    assert!(
        first_batch.len() >= 3,
        "expected at least 3 indexes, got {}",
        first_batch.len()
    );

    // Verify we can find our custom indexes
    let mut found_email = false;
    let mut found_name_age = false;
    for idx in first_batch {
        if let bson::Bson::Document(idx_doc) = idx {
            if let Ok(name) = idx_doc.get_str("name") {
                if name == "email_1" {
                    found_email = true;
                }
                if name == "name_1_age_-1" {
                    found_name_age = true;
                }
            }
        }
    }
    assert!(found_email, "listIndexes should include email_1 index");
    assert!(
        found_name_age,
        "listIndexes should include name_1_age_-1 index"
    );

    // Let shadow finish and assert attempts grew
    tokio::time::sleep(Duration::from_millis(400)).await;
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
async fn e2e_shadow_cursors_getmore_killcursors() {
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

    // Build config with ephemeral port, Postgres, and shadow with db_prefix
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: Some("shadow".to_string()),
        timeout_ms: 2000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: None,
        password: None,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_cursors_{}", rand_suffix(6));

    // create
    let create = doc! {"create": "items", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert 10 docs
    let docs: Vec<bson::Document> = (1..=10)
        .map(|i| doc! {"id": i, "value": format!("item_{}", i)})
        .collect();
    let ins = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 10);

    // find with batchSize=3
    let find = doc! {"find": "items", "filter": {}, "batchSize": 3i32, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 3);

    // getMore batchSize=3
    let gm = doc! {"getMore": id, "collection": "items", "batchSize": 3i32, "$db": &dbname};
    let msg = encode_op_msg(&gm, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);
    let next_batch = cursor.get_array("nextBatch").unwrap();
    assert_eq!(next_batch.len(), 3);

    // killCursors
    let kc = doc! {"killCursors": "items", "cursors": [bson::Bson::Int64(id)], "$db": &dbname};
    let msg = encode_op_msg(&kc, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let killed = doc.get_array("cursorsKilled").unwrap();
    assert_eq!(killed.len(), 1);

    // getMore after kill should be empty, id=0
    let gm2 = doc! {"getMore": id, "collection": "items", "batchSize": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&gm2, 0, 6);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 0);

    // Let shadow finish and assert attempts grew
    tokio::time::sleep(Duration::from_millis(400)).await;
    let attempts = state
        .shadow_attempts
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        attempts >= 6,
        "expected >=6 shadow attempts, got {}",
        attempts
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_shadow_list_databases_collections() {
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

    // Build config with ephemeral port, Postgres, and shadow with db_prefix
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    cfg.shadow = Some(ShadowConfig {
        enabled: true,
        addr: mongo,
        db_prefix: Some("shadow".to_string()),
        timeout_ms: 2000,
        sample_rate: 1.0,
        mode: Default::default(),
        compare: Default::default(),
        deterministic_sampling: false,
        username: None,
        password: None,
    });

    let (state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("e2e_list_{}", rand_suffix(6));

    // create a collection "products" in dbname
    let create = doc! {"create": "products", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // listDatabases (nameOnly)
    let ldb = doc! {"listDatabases": 1i32, "nameOnly": true, "$db": "admin"};
    let msg = encode_op_msg(&ldb, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let dbs = doc.get_array("databases").unwrap();
    let mut found = false;
    for d in dbs {
        if let bson::Bson::Document(dd) = d {
            if dd.get_str("name").ok() == Some(dbname.as_str()) {
                found = true;
                break;
            }
        }
    }
    assert!(found, "listDatabases should include {}", dbname);

    // listCollections on dbname
    let lc = doc! {"listCollections": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&lc, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    let mut found_products = false;
    for c in first_batch {
        if let bson::Bson::Document(dd) = c {
            if dd.get_str("name").ok() == Some("products") {
                found_products = true;
                break;
            }
        }
    }
    assert!(found_products, "listCollections should include 'products'");

    // Let shadow finish and assert attempts grew
    tokio::time::sleep(Duration::from_millis(400)).await;
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
