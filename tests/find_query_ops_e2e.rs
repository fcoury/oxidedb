use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "common/postgres.rs"]
mod pg;

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
async fn e2e_find_with_ne_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_ne_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "age": 30},
        doc! {"name": "Bob", "age": 25},
        doc! {"name": "Charlie", "age": 35},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $ne operator
    let find = doc! {"find": "items", "filter": {"age": {"$ne": 30}}, "$db": &dbname};
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 2, "Expected 2 docs with age != 30");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_nin_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_nin_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "status": "active"},
        doc! {"name": "Bob", "status": "inactive"},
        doc! {"name": "Charlie", "status": "pending"},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $nin operator
    let find = doc! {"find": "items", "filter": {"status": {"$nin": ["active", "pending"]}}, "$db": &dbname};
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        1,
        "Expected 1 doc with status not in [active, pending]"
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_or_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_or_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "age": 30},
        doc! {"name": "Bob", "age": 25},
        doc! {"name": "Charlie", "age": 35},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $or operator
    let find = doc! {
        "find": "items",
        "filter": {"$or": [{"age": 25}, {"age": 35}]},
        "$db": &dbname
    };
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 2, "Expected 2 docs with age 25 or 35");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_and_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_and_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "age": 30, "status": "active"},
        doc! {"name": "Bob", "age": 30, "status": "inactive"},
        doc! {"name": "Charlie", "age": 35, "status": "active"},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $and operator
    let find = doc! {
        "find": "items",
        "filter": {"$and": [{"age": 30}, {"status": "active"}]},
        "$db": &dbname
    };
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        1,
        "Expected 1 doc with age 30 AND status active"
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_regex() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_regex_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "email": "alice@example.com"},
        doc! {"name": "Bob", "email": "bob@test.com"},
        doc! {"name": "Charlie", "email": "charlie@example.com"},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $regex operator
    let find = doc! {
        "find": "items",
        "filter": {"email": {"$regex": "@example\\.com$"}},
        "$db": &dbname
    };
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        2,
        "Expected 2 docs with emails ending in @example.com"
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_all_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_all_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "tags": ["a", "b", "c"]},
        doc! {"name": "Bob", "tags": ["a", "b"]},
        doc! {"name": "Charlie", "tags": ["a", "c"]},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $all operator
    let find = doc! {
        "find": "items",
        "filter": {"tags": {"$all": ["a", "b"]}},
        "$db": &dbname
    };
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(
        first_batch.len(),
        2,
        "Expected 2 docs with both tags a and b"
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_find_with_size_operator() {
    let testdb = match pg::TestDb::provision_from_env().await {
        Some(db) => db,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let dbname = format!("test_size_{}", rand_suffix(6));

    // Create collection and insert docs
    let create = doc! {"create": "items", "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"name": "Alice", "tags": ["a", "b", "c"]},
        doc! {"name": "Bob", "tags": ["a", "b"]},
        doc! {"name": "Charlie", "tags": ["a"]},
    ];
    let insert = doc! {"insert": "items", "documents": docs, "$db": &dbname};
    stream
        .write_all(&encode_op_msg(&insert, 0, 2))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Find with $size operator
    let find = doc! {
        "find": "items",
        "filter": {"tags": {"$size": 2}},
        "$db": &dbname
    };
    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 1, "Expected 1 doc with exactly 2 tags");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
