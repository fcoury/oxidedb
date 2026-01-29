use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};
use std::time::Duration;
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
async fn e2e_cursors_find_getmore_kill() {
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

    let dbname = format!("cursors_{}", rand_suffix(6));

    // create
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // insert 5 docs
    let docs = vec![
        doc! {"i": 1},
        doc! {"i": 2},
        doc! {"i": 3},
        doc! {"i": 4},
        doc! {"i": 5},
    ];
    let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 5);

    // find with batchSize=2
    let find = doc! {"find": "u", "filter": {}, "batchSize": 2i32, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let id1 = cursor.get_i64("id").unwrap();
    assert_ne!(id1, 0);
    let first_batch = cursor.get_array("firstBatch").unwrap();
    assert_eq!(first_batch.len(), 2);

    // getMore batchSize=2
    let gm = doc! {"getMore": id1, "collection": "u", "batchSize": 2i32, "$db": &dbname};
    let msg = encode_op_msg(&gm, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor2 = doc.get_document("cursor").unwrap();
    let id2 = cursor2.get_i64("id").unwrap();
    let next_batch = cursor2.get_array("nextBatch").unwrap();
    assert_eq!(next_batch.len(), 2);
    assert_ne!(id2, 0);

    // getMore to exhaust
    let gm2 = doc! {"getMore": id2, "collection": "u", "batchSize": 10i32, "$db": &dbname};
    let msg = encode_op_msg(&gm2, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor3 = doc.get_document("cursor").unwrap();
    let id3 = cursor3.get_i64("id").unwrap();
    let next_batch2 = cursor3.get_array("nextBatch").unwrap();
    assert_eq!(next_batch2.len(), 1);
    assert_eq!(id3, 0);

    // New cursor to test killCursors
    let find2 = doc! {"find": "u", "filter": {}, "batchSize": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&find2, 0, 6);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);

    // killCursors
    let kc = doc! {"killCursors": "u", "cursors": [bson::Bson::Int64(id)], "$db": &dbname};
    let msg = encode_op_msg(&kc, 0, 7);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let killed = doc.get_array("cursorsKilled").unwrap();
    assert_eq!(killed.len(), 1);

    // getMore after kill should be empty, id=0
    let gm3 = doc! {"getMore": id, "collection": "u", "batchSize": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&gm3, 0, 8);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_cursor_ttl_prune() {
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
    cfg.cursor_timeout_secs = Some(1);
    cfg.cursor_sweep_interval_secs = Some(1);

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("cursors_ttl_{}", rand_suffix(6));

    // create + insert
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![doc! {"i": 1}, doc! {"i": 2}, doc! {"i": 3}];
    let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // find to create a cursor
    let find = doc! {"find": "u", "filter": {}, "batchSize": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    let id = cursor.get_i64("id").unwrap();
    assert_ne!(id, 0);

    // wait for TTL (1s) + sweep interval (1s) + margin
    tokio::time::sleep(Duration::from_millis(2300)).await;

    // getMore should return id 0 and empty
    let gm = doc! {"getMore": id, "collection": "u", "batchSize": 1i32, "$db": &dbname};
    let msg = encode_op_msg(&gm, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    assert_eq!(cursor.get_i64("id").unwrap(), 0);
    assert_eq!(cursor.get_array("nextBatch").unwrap().len(), 0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
