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
async fn e2e_unionwith_basic_string() {
    // Test $unionWith with just collection name as string
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

    let dbname = format!("agg_union_{}", rand_suffix(6));

    // Create both collections
    for coll in ["coll_a", "coll_b"] {
        let create = doc! {"create": coll, "$db": &dbname};
        let msg = encode_op_msg(&create, 0, 1);
        stream.write_all(&msg).await.unwrap();
        let _ = read_one_op_msg(&mut stream).await;
    }

    // Insert into coll_a
    let ins_a = doc! {
        "insert": "coll_a",
        "documents": [
            {"_id": "a1", "val": 1i32},
            {"_id": "a2", "val": 2i32},
        ],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins_a, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert into coll_b
    let ins_b = doc! {
        "insert": "coll_b",
        "documents": [
            {"_id": "b1", "val": 3i32},
            {"_id": "b2", "val": 4i32},
        ],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins_b, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Aggregate with $unionFrom coll_b
    let pipeline = vec![
        bson::Bson::Document(doc! {"$unionWith": "coll_b"}),
        bson::Bson::Document(doc! {"$sort": {"_id": 1i32}}),
    ];
    let agg = doc! {"aggregate": "coll_a", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 4);

    // Check order: a1, a2, b1, b2
    let ids: Vec<String> = fb
        .iter()
        .map(|d| d.as_document().unwrap().get_str("_id").unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["a1", "a2", "b1", "b2"]);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_unionwith_with_pipeline() {
    // Test $unionWith with document format including pipeline
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

    let dbname = format!("agg_union_{}", rand_suffix(6));

    // Create both collections
    for coll in ["main", "other"] {
        let create = doc! {"create": coll, "$db": &dbname};
        let msg = encode_op_msg(&create, 0, 1);
        stream.write_all(&msg).await.unwrap();
        let _ = read_one_op_msg(&mut stream).await;
    }

    // Insert into main
    let ins_main = doc! {
        "insert": "main",
        "documents": [
            {"_id": "m1", "type": "main", "score": 10i32},
        ],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins_main, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert into other
    let ins_other = doc! {
        "insert": "other",
        "documents": [
            {"_id": "o1", "type": "other", "score": 5i32},
            {"_id": "o2", "type": "other", "score": 15i32},
            {"_id": "o3", "type": "other", "score": 8i32},
        ],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins_other, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Aggregate with $unionWith including a pipeline to filter union collection
    let union_with_doc = doc! {
        "coll": "other",
        "pipeline": [
            {"$match": {"score": {"$gte": 8i32}}},
            {"$project": {"_id": 1i32, "type": 1i32, "score": 1i32}},
        ]
    };
    let pipeline = vec![
        bson::Bson::Document(doc! {"$unionWith": union_with_doc}),
        bson::Bson::Document(doc! {"$sort": {"score": 1i32}}),
    ];
    let agg = doc! {"aggregate": "main", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 3); // m1 (score 10), o2 (score 15), o3 (score 8) - o1 filtered out (score 5)

    // Check order by score: o3 (8), m1 (10), o2 (15)
    let scores: Vec<i32> = fb
        .iter()
        .map(|d| d.as_document().unwrap().get_i32("score").unwrap())
        .collect();
    assert_eq!(scores, vec![8i32, 10i32, 15i32]);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_unionwith_nonexistent_collection() {
    // Test $unionWith with non-existent collection (should handle gracefully)
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

    let dbname = format!("agg_union_{}", rand_suffix(6));

    // Create only main collection
    let create = doc! {"create": "main", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert into main
    let ins = doc! {
        "insert": "main",
        "documents": [
            {"_id": "m1", "val": 1i32},
            {"_id": "m2", "val": 2i32},
        ],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Aggregate with $unionWith on non-existent collection
    let pipeline = vec![bson::Bson::Document(doc! {"$unionWith": "nonexistent"})];
    let agg = doc! {"aggregate": "main", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    // Should only have main collection docs
    assert_eq!(fb.len(), 2);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_unionwith_multiple_stages() {
    // Test multiple $unionWith stages in the same pipeline
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

    let dbname = format!("agg_union_{}", rand_suffix(6));

    // Create three collections
    for coll in ["first", "second", "third"] {
        let create = doc! {"create": coll, "$db": &dbname};
        let msg = encode_op_msg(&create, 0, 1);
        stream.write_all(&msg).await.unwrap();
        let _ = read_one_op_msg(&mut stream).await;
    }

    // Insert into first
    let ins = doc! {
        "insert": "first",
        "documents": [{"_id": "f1", "src": "first"}],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert into second
    let ins = doc! {
        "insert": "second",
        "documents": [{"_id": "s1", "src": "second"}],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert into third
    let ins = doc! {
        "insert": "third",
        "documents": [{"_id": "t1", "src": "third"}],
        "$db": &dbname
    };
    let msg = encode_op_msg(&ins, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Aggregate with multiple $unionWith stages
    let pipeline = vec![
        bson::Bson::Document(doc! {"$unionWith": "second"}),
        bson::Bson::Document(doc! {"$unionWith": "third"}),
        bson::Bson::Document(doc! {"$sort": {"_id": 1i32}}),
    ];
    let agg = doc! {"aggregate": "first", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 3);

    // Check order: f1, s1, t1
    let ids: Vec<String> = fb
        .iter()
        .map(|d| d.as_document().unwrap().get_str("_id").unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["f1", "s1", "t1"]);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
