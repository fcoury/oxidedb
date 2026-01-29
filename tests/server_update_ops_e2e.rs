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
async fn e2e_update_inc_rename_push_pull() {
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

    let dbname = format!("upd_ops_{}", rand_suffix(6));

    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let ins = doc! {"insert": "u", "documents": [ {"_id":"x", "n": 1i32, "a": [1i32, 2i32]} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $inc n by 2 => 3 (int)
    let upd = doc! {"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$inc": {"n": 2i32}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $rename n -> m
    let upd = doc! {"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$rename": {"n": "m"}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 4);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // $push a: 3 and $pull a: 2
    let upd = doc! {"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$push": {"a": 3i32}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 5);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let upd = doc! {"update": "u", "updates": [ {"q": {"_id":"x"}, "u": {"$pull": {"a": 2i32}} } ], "$db": &dbname};
    let msg = encode_op_msg(&upd, 0, 6);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // find and assert
    let find = doc! {"find": "u", "filter": {"_id":"x"}, "$db": &dbname};
    let msg = encode_op_msg(&find, 0, 7);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let d = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()[0]
        .as_document()
        .unwrap()
        .clone();
    assert_eq!(d.get_i32("m").unwrap(), 3);
    let arr = d.get_array("a").unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0].as_i32().unwrap(), 1);
    assert_eq!(arr[1].as_i32().unwrap(), 3);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
