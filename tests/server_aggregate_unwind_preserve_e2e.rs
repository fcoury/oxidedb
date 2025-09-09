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
async fn e2e_unwind_preserve_null_and_empty() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("agg_unwind_pres_{}", rand_suffix(6));
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let ins = doc!{"insert": "u", "documents": [
        {"_id":"m", "tags": ["x"]},
        {"_id":"e", "tags": []},
        {"_id":"n"},
        {"_id":"z", "tags": null}
    ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let pipeline = vec![bson::Bson::Document(doc!{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}), bson::Bson::Document(doc!{"$project": {"_id": 1i32, "tags": 1i32}})];
    let agg = doc!{"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let fb = doc.get_document("cursor").unwrap().get_array("firstBatch").unwrap();
    // Expect 4 results: m (x), e (null), n (null), z (null)
    assert_eq!(fb.len(), 4);
    // Confirm one of each id present and tags null for e/n/z
    let mut has_m = false; let mut null_count = 0;
    for d in fb {
        let dd = d.as_document().unwrap();
        let id = dd.get_str("_id").unwrap();
        let tags = dd.get("tags").cloned().unwrap_or(bson::Bson::Null);
        if id == "m" { has_m = true; assert_eq!(tags.as_str().unwrap(), "x"); }
        if id == "e" || id == "n" || id == "z" { assert!(matches!(tags, bson::Bson::Null)); null_count += 1; }
    }
    assert!(has_m);
    assert_eq!(null_count, 3);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
