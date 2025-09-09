use bson::doc;
use oxidedb::config::Config;
use oxidedb::protocol::{decode_op_msg_section0, encode_op_msg, MessageHeader, OP_MSG};
use oxidedb::server::spawn_with_shutdown;
use rand::{distributions::Alphanumeric, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[path = "common/postgres.rs"]
mod pg;

fn rand_suffix(n: usize) -> String { rand::thread_rng().sample_iter(&Alphanumeric).take(n).map(char::from).collect() }

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
async fn e2e_unwind_and_sort() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("agg_unwind_{}", rand_suffix(6));
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let ins = doc!{"insert": "u", "documents": [ {"_id":"d1", "tags":["b","a","c"]} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let pipeline = vec![
        bson::Bson::Document(doc!{"$unwind": "$tags"}),
        bson::Bson::Document(doc!{"$sort": {"tags": 1i32}}),
        bson::Bson::Document(doc!{"$project": {"_id": 0i32, "tags": 1i32}}),
    ];
    let agg = doc!{"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let fb = doc.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
    let vals: Vec<String> = fb.iter().map(|b| b.as_document().unwrap().get_str("tags").unwrap().to_string()).collect();
    assert_eq!(vals, vec!["a","b","c"]);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_sort_on_computed_field() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("agg_sort_comp_{}", rand_suffix(6));
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let ins = doc!{"insert": "u", "documents": [ {"_id":"a","i":1i32}, {"_id":"b","i":5i32}, {"_id":"c","i":3i32} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let pipeline = vec![
        bson::Bson::Document(doc!{"$addFields": {"sum": {"$add": ["$i", 5i32]}}}),
        bson::Bson::Document(doc!{"$sort": {"sum": -1i32}}),
        bson::Bson::Document(doc!{"$project": {"_id": 1i32}}),
        bson::Bson::Document(doc!{"$limit": 2i32}),
    ];
    let agg = doc!{"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let fb = doc.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
    let ids: Vec<String> = fb.iter().map(|b| b.as_document().unwrap().get_str("_id").unwrap().to_string()).collect();
    assert_eq!(ids, vec!["b","c"]);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

