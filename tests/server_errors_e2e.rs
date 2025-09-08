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
async fn e2e_error_codes() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("err_{}", rand_suffix(6));

    // create collection
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // update with upsert=true -> code 20
    let u_spec = doc!{"q": {}, "u": {"$set": {"x": 1}}, "upsert": true};
    let upd = doc!{"update": "u", "updates": [u_spec], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 20);

    // update with unsupported operator -> code 9
    let u_spec = doc!{"q": {}, "u": {"$foo": {"a": 1}}};
    let upd = doc!{"update": "u", "updates": [u_spec], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 9);

    // update with negative array index in path -> code 2
    let u_spec = doc!{"q": {}, "u": {"$set": {"a.-1": 1}}};
    let upd = doc!{"update": "u", "updates": [u_spec], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 2);

    // delete with invalid limit -> code 2
    let d_spec = doc!{"q": {}, "limit": 2i32};
    let del = doc!{"delete": "u", "deletes": [d_spec], "$db": &dbname};
    let msg = encode_op_msg(&del, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 2);

    // aggregate with unsupported stage -> code 9
    let pipeline = vec![bson::Bson::Document(doc!{"$group": {"_id": "$x"}})];
    let agg = doc!{"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 6);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 9);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

