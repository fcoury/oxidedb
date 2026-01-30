use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
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
async fn e2e_bucket_basic_boundaries() {
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

    let dbname = format!("agg_bucket_{}", rand_suffix(6));
    let create = doc! {"create": "scores", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents with scores
    let ins = doc! {"insert": "scores", "documents": [
        {"name": "Alice", "score": 85i32},
        {"name": "Bob", "score": 55i32},
        {"name": "Charlie", "score": 72i32},
        {"name": "David", "score": 45i32},
        {"name": "Eve", "score": 95i32},
        {"name": "Frank", "score": 60i32},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Basic $bucket with boundaries only
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$score",
            "boundaries": [0i32, 60i32, 80i32, 100i32]
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "scores", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 3, "Expected 3 buckets");

    // Check bucket boundaries
    let bucket0 = fb[0].as_document().unwrap();
    assert_eq!(bucket0.get_i32("_id").unwrap(), 0);
    assert_eq!(bucket0.get_i32("count").unwrap(), 2); // David (45), Bob (55)

    let bucket1 = fb[1].as_document().unwrap();
    assert_eq!(bucket1.get_i32("_id").unwrap(), 60);
    assert_eq!(bucket1.get_i32("count").unwrap(), 2); // Charlie (72), Frank (60)

    let bucket2 = fb[2].as_document().unwrap();
    assert_eq!(bucket2.get_i32("_id").unwrap(), 80);
    assert_eq!(bucket2.get_i32("count").unwrap(), 2); // Alice (85), Eve (95)

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_with_default() {
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

    let dbname = format!("agg_bucket_def_{}", rand_suffix(6));
    let create = doc! {"create": "items", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents with prices
    let ins = doc! {"insert": "items", "documents": [
        {"item": "A", "price": 25i32},
        {"item": "B", "price": 75i32},
        {"item": "C", "price": 150i32},
        {"item": "D", "price": 5i32},
        {"item": "E", "price": 200i32},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket with default bucket
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$price",
            "boundaries": [0i32, 50i32, 100i32],
            "default": "Other"
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "items", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 3, "Expected 3 buckets (2 regular + 1 default)");

    // Check buckets
    let bucket0 = fb[0].as_document().unwrap();
    assert_eq!(bucket0.get_i32("_id").unwrap(), 0);
    assert_eq!(bucket0.get_i32("count").unwrap(), 2); // D (5), A (25)

    let bucket1 = fb[1].as_document().unwrap();
    assert_eq!(bucket1.get_i32("_id").unwrap(), 50);
    assert_eq!(bucket1.get_i32("count").unwrap(), 1); // B (75)

    // Default bucket
    let bucket2 = fb[2].as_document().unwrap();
    assert_eq!(bucket2.get_str("_id").unwrap(), "Other");
    assert_eq!(bucket2.get_i32("count").unwrap(), 2); // C (150), E (200)

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_with_output_accumulators() {
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

    let dbname = format!("agg_bucket_out_{}", rand_suffix(6));
    let create = doc! {"create": "sales", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents with sales data
    let ins = doc! {"insert": "sales", "documents": [
        {"product": "Widget", "quantity": 10i32, "price": 25i32},
        {"product": "Gadget", "quantity": 5i32, "price": 75i32},
        {"product": "Tool", "quantity": 8i32, "price": 45i32},
        {"product": "Device", "quantity": 15i32, "price": 35i32},
        {"product": "Instrument", "quantity": 3i32, "price": 95i32},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket with output accumulators
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$price",
            "boundaries": [0i32, 50i32, 100i32],
            "output": {
                "totalQuantity": {"$sum": "$quantity"},
                "avgQuantity": {"$avg": "$quantity"},
                "minQuantity": {"$min": "$quantity"},
                "maxQuantity": {"$max": "$quantity"},
                "count": {"$sum": 1i32}
            }
        }
    };
    let pipeline = vec![
        bson::Bson::Document(bucket),
        bson::Bson::Document(doc! {"$sort": {"_id": 1i32}}),
    ];
    let agg = doc! {"aggregate": "sales", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 2, "Expected 2 buckets");

    // Check first bucket [0, 50): Widget (25, qty 10), Tool (45, qty 8), Device (35, qty 15)
    let bucket0 = fb[0].as_document().unwrap();
    assert_eq!(bucket0.get_i32("_id").unwrap(), 0);
    assert_eq!(bucket0.get_i32("count").unwrap(), 3);
    assert_eq!(bucket0.get_i32("totalQuantity").unwrap(), 33); // 10 + 8 + 15
    assert_eq!(bucket0.get_f64("avgQuantity").unwrap(), 11.0); // (10 + 8 + 15) / 3
    assert_eq!(bucket0.get_i32("minQuantity").unwrap(), 8);
    assert_eq!(bucket0.get_i32("maxQuantity").unwrap(), 15);

    // Check second bucket [50, 100): Gadget (75, qty 5), Instrument (95, qty 3)
    let bucket1 = fb[1].as_document().unwrap();
    assert_eq!(bucket1.get_i32("_id").unwrap(), 50);
    assert_eq!(bucket1.get_i32("count").unwrap(), 2);
    assert_eq!(bucket1.get_i32("totalQuantity").unwrap(), 8); // 5 + 3
    assert_eq!(bucket1.get_f64("avgQuantity").unwrap(), 4.0); // (5 + 3) / 2
    assert_eq!(bucket1.get_i32("minQuantity").unwrap(), 3);
    assert_eq!(bucket1.get_i32("maxQuantity").unwrap(), 5);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_values_outside_boundaries() {
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

    let dbname = format!("agg_bucket_outside_{}", rand_suffix(6));
    let create = doc! {"create": "temps", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents with temperatures
    let ins = doc! {"insert": "temps", "documents": [
        {"city": "A", "temp": -10i32},
        {"city": "B", "temp": 15i32},
        {"city": "C", "temp": 35i32},
        {"city": "D", "temp": 55i32},
        {"city": "E", "temp": 120i32},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket without default - values outside boundaries should be skipped
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$temp",
            "boundaries": [0i32, 20i32, 40i32, 60i32]
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "temps", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 3, "Expected 3 buckets");

    // B (15) should be in [0, 20)
    let bucket0 = fb[0].as_document().unwrap();
    assert_eq!(bucket0.get_i32("_id").unwrap(), 0);
    assert_eq!(bucket0.get_i32("count").unwrap(), 1);

    // C (35) should be in [20, 40)
    let bucket1 = fb[1].as_document().unwrap();
    assert_eq!(bucket1.get_i32("_id").unwrap(), 20);
    assert_eq!(bucket1.get_i32("count").unwrap(), 1);

    // D (55) should be in [40, 60)
    let bucket2 = fb[2].as_document().unwrap();
    assert_eq!(bucket2.get_i32("_id").unwrap(), 40);
    assert_eq!(bucket2.get_i32("count").unwrap(), 1);

    // A (-10) and E (120) should be skipped (outside boundaries)

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_error_unsorted_boundaries() {
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

    let dbname = format!("agg_bucket_err_{}", rand_suffix(6));
    let create = doc! {"create": "data", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert a test document
    let ins = doc! {"insert": "data", "documents": [{"val": 50i32}], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket with unsorted boundaries should error
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$val",
            "boundaries": [100i32, 50i32, 0i32]
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "data", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    // Should return an error
    assert_eq!(doc.get_f64("ok").unwrap(), 0.0);
    let errmsg = doc.get_str("errmsg").unwrap();
    assert!(
        errmsg.contains("boundaries must be sorted"),
        "Error message should mention boundaries must be sorted: {}",
        errmsg
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_with_single_boundary() {
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

    let dbname = format!("agg_bucket_single_{}", rand_suffix(6));
    let create = doc! {"create": "data", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents
    let ins = doc! {"insert": "data", "documents": [
        {"val": 5i32},
        {"val": 15i32},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket with only 1 boundary should error
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$val",
            "boundaries": [10i32]
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "data", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    // Should return an error
    assert_eq!(doc.get_f64("ok").unwrap(), 0.0);
    let errmsg = doc.get_str("errmsg").unwrap();
    assert!(
        errmsg.contains("at least 2 elements"),
        "Error message should mention at least 2 elements: {}",
        errmsg
    );

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_bucket_with_missing_groupby() {
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

    let dbname = format!("agg_bucket_miss_{}", rand_suffix(6));
    let create = doc! {"create": "data", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test documents - some with missing field
    let ins = doc! {"insert": "data", "documents": [
        {"val": 5i32},
        {"other": "no val field"},
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $bucket with default to catch missing values
    let bucket = doc! {
        "$bucket": {
            "groupBy": "$val",
            "boundaries": [0i32, 10i32],
            "default": "Missing"
        }
    };
    let pipeline = vec![bson::Bson::Document(bucket)];
    let agg = doc! {"aggregate": "data", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 2, "Expected 2 buckets");

    // First bucket [0, 10)
    let bucket0 = fb[0].as_document().unwrap();
    assert_eq!(bucket0.get_i32("_id").unwrap(), 0);
    assert_eq!(bucket0.get_i32("count").unwrap(), 1);

    // Default bucket for missing field
    let bucket1 = fb[1].as_document().unwrap();
    assert_eq!(bucket1.get_str("_id").unwrap(), "Missing");
    assert_eq!(bucket1.get_i32("count").unwrap(), 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
