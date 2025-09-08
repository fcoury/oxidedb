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
async fn e2e_rename_collision_errors() {
    let testdb = match pg::TestDb::provision_from_env().await { Some(db) => db, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());
    let (_state, addr, shutdown, handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let dbname = format!("ren_err_{}", rand_suffix(6));
    let create = doc!{"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;
    let ins = doc!{"insert": "u", "documents": [ {"_id":"x", "a": {"b": 1}, "c": 2} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Same path rename
    let upd = doc!{"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$rename": {"a": "a"}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 2);

    // Ancestor to descendant
    let upd = doc!{"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$rename": {"a": "a.b"}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 2);

    // Duplicate target
    let upd = doc!{"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$rename": {"a": "z", "c": "z"}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    assert_eq!(doc.get_i32("code").unwrap_or(0), 2);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

