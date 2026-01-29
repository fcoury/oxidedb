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
async fn e2e_update_and_delete_one() {
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

    let dbname = format!("upd_del_{}", rand_suffix(6));

    // create
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // insert docs with fixed _id
    let ins = doc! {"insert": "u", "documents": [ {"_id": "a", "name": "old"}, {"_id": "b", "name": "keep"} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // update one: set name for _id a
    let u_spec = doc! {"q": {"_id": "a"}, "u": {"$set": {"name": "new"}}, "multi": false};
    let upd = doc! {"update": "u", "updates": [u_spec], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 1);
    assert_eq!(doc.get_i32("nModified").unwrap_or(0), 1);

    // find by _id a and check name==new
    let find = doc! {"find": "u", "filter": {"_id": "a"}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 1);
    let d = fb[0].as_document().unwrap();
    assert_eq!(d.get_str("name").unwrap(), "new");

    // delete one: _id b
    let d_spec = doc! {"q": {"_id": "b"}, "limit": 1i32};
    let del = doc! {"delete": "u", "deletes": [d_spec], "$db": &dbname};
    let msg = encode_op_msg(&del, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 1);

    // find by _id b should return empty
    let find_b = doc! {"find": "u", "filter": {"_id": "b"}, "$db": &dbname};
    let msg = encode_op_msg(&find_b, 0, 6);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_delete_many_limit_zero() {
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

    let dbname = format!("del_many_{}", rand_suffix(6));

    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"_id": "a", "group": "x"},
        doc! {"_id": "b", "group": "x"},
        doc! {"_id": "c", "group": "y"},
    ];
    let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let d_spec = doc! {"q": {"group": "x"}, "limit": 0i32};
    let del = doc! {"delete": "u", "deletes": [d_spec], "$db": &dbname};
    let msg = encode_op_msg(&del, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert_eq!(doc.get_i32("n").unwrap_or(0), 2);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_update_nested_and_multi() {
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

    let dbname = format!("upd_nested_{}", rand_suffix(6));

    // create + insert
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"_id": "a", "group": "x", "a": {"b": 1}},
        doc! {"_id": "b", "group": "x", "a": {"b": 1}},
        doc! {"_id": "c", "group": "y", "a": {"b": 1}},
    ];
    let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // multi update for group x, set nested a.b=42
    let u_spec = doc! {"q": {"group": "x"}, "u": {"$set": {"a.b": 42i32}}, "multi": true};
    let upd = doc! {"update": "u", "updates": [u_spec], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("n").unwrap_or(0) >= 2);

    // find group x and verify nested field changed
    let find = doc! {"find": "u", "filter": {"group": "x"}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 2);
    for d in fb {
        let dd = d.as_document().unwrap();
        let a = dd.get_document("a").unwrap();
        assert_eq!(a.get_i32("b").unwrap(), 42);
    }

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
