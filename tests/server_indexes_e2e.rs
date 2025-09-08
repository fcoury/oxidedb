use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{decode_op_msg_section0, encode_op_msg, MessageHeader, OP_MSG};
use oxidedb::server::spawn_with_shutdown;
use rand::{distributions::Alphanumeric, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "common/postgres.rs"]
mod pg;

fn rand_suffix(n: usize) -> String {
    rand::thread_rng().sample_iter(&Alphanumeric).take(n).map(char::from).collect()
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
async fn e2e_create_and_drop_indexes() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("idx_{}", rand_suffix(6));

    // create collection
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // createIndexes single-field
    let idx_spec = doc!{"name": "a_1", "key": {"a": 1i32}};
    let ci = doc!{"createIndexes": "u", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // dropIndexes by name
    let di = doc!{"dropIndexes": "u", "index": "a_1", "$db": &dbname};
    let msg = encode_op_msg(&di, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("nIndexesWas").unwrap_or(0), 1);

    // createIndexes compound
    let idx_spec2 = doc!{"name": "a1_bm1", "key": {"a": 1i32, "b": -1i32}};
    let ci2 = doc!{"createIndexes": "u", "indexes": [idx_spec2], "$db": &dbname};
    let msg = encode_op_msg(&ci2, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    // dropIndexes all "*"
    let di2 = doc!{"dropIndexes": "u", "index": "*", "$db": &dbname};
    let msg = encode_op_msg(&di2, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("nIndexesWas").unwrap_or(0) >= 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

