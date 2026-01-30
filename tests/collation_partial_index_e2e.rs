use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use rand::{Rng, distributions::Alphanumeric};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "common/postgres.rs"]
mod pg;

fn rand_suffix(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
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
async fn e2e_collation_single_field_index() {
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

    let (_state, addr, shutdown, handle) = oxidedb::server::spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("collation_test_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "users", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents with varying cases
    let docs = vec![
        doc! {"name": "Alice", "status": "active"},
        doc! {"name": "alice", "status": "active"},
        doc! {"name": "ALICE", "status": "inactive"},
        doc! {"name": "Bob", "status": "active"},
        doc! {"name": "bob", "status": "inactive"},
    ];
    let insert = doc! {"insert": "users", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Create index with collation (case insensitive)
    let collation = doc! {
        "locale": "en",
        "strength": 2  // Secondary strength = case insensitive
    };
    let idx_spec = doc! {
        "name": "name_collation_idx",
        "key": {"name": 1i32},
        "collation": collation
    };
    let ci = doc! {"createIndexes": "users", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // Query with sort to verify collation affects ordering
    let find = doc! {
        "find": "users",
        "sort": {"name": 1i32},
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify results
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 5);

    // Cleanup
    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_partial_index() {
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

    let (_state, addr, shutdown, handle) = oxidedb::server::spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("partial_idx_test_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "orders", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents with different statuses
    let docs = vec![
        doc! {"order_id": "A001", "status": "pending", "amount": 100},
        doc! {"order_id": "A002", "status": "completed", "amount": 200},
        doc! {"order_id": "A003", "status": "pending", "amount": 150},
        doc! {"order_id": "A004", "status": "cancelled", "amount": 75},
        doc! {"order_id": "A005", "status": "completed", "amount": 300},
    ];
    let insert = doc! {"insert": "orders", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Create partial index for pending orders only
    let partial_filter = doc! {"status": "pending"};
    let idx_spec = doc! {
        "name": "pending_orders_idx",
        "key": {"order_id": 1i32},
        "partialFilterExpression": partial_filter
    };
    let ci = doc! {"createIndexes": "orders", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // Query pending orders (should use partial index)
    let find = doc! {
        "find": "orders",
        "filter": {"status": "pending"},
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify only pending orders are returned
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 2);

    // Cleanup
    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_collation_and_partial_index_combined() {
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

    let (_state, addr, shutdown, handle) = oxidedb::server::spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("combined_test_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "products", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents
    let docs = vec![
        doc! {"name": "Apple", "category": "fruit", "active": true},
        doc! {"name": "apple", "category": "fruit", "active": true},
        doc! {"name": "APPLE", "category": "fruit", "active": false},
        doc! {"name": "Banana", "category": "fruit", "active": true},
        doc! {"name": "banana", "category": "fruit", "active": false},
    ];
    let insert = doc! {"insert": "products", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Create index with both collation and partial filter
    let collation = doc! {
        "locale": "en",
        "strength": 2  // Case insensitive
    };
    let partial_filter = doc! {"active": true};
    let idx_spec = doc! {
        "name": "active_products_name_idx",
        "key": {"name": 1i32},
        "collation": collation,
        "partialFilterExpression": partial_filter
    };
    let ci = doc! {"createIndexes": "products", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // Query active products
    let find = doc! {
        "find": "products",
        "filter": {"active": true},
        "sort": {"name": 1i32},
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify results
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 3); // Only active products

    // Cleanup
    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_partial_index_with_operators() {
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

    let (_state, addr, shutdown, handle) = oxidedb::server::spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("partial_ops_test_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "inventory", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents
    let docs = vec![
        doc! {"item": "A", "qty": 10, "price": 5.0},
        doc! {"item": "B", "qty": 50, "price": 10.0},
        doc! {"item": "C", "qty": 5, "price": 15.0},
        doc! {"item": "D", "qty": 100, "price": 20.0},
        doc! {"item": "E", "qty": 25, "price": 25.0},
    ];
    let insert = doc! {"insert": "inventory", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Create partial index with $gt operator
    let partial_filter = doc! {"qty": {"$gt": 20}};
    let idx_spec = doc! {
        "name": "high_qty_idx",
        "key": {"item": 1i32},
        "partialFilterExpression": partial_filter
    };
    let ci = doc! {"createIndexes": "inventory", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // Query high quantity items
    let find = doc! {
        "find": "inventory",
        "filter": {"qty": {"$gt": 20}},
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Verify results (should return items B, D, E with qty > 20)
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 3);

    // Cleanup
    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
