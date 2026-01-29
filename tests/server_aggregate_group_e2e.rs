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
async fn e2e_group_sum_count() {
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

    let dbname = format!("agg_group_{}", rand_suffix(6));
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let ins = doc! {"insert": "u", "documents": [ {"g":"x","i":1i32}, {"g":"x","i":2i32}, {"g":"y","i":4i32} ], "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let group = doc! {"_id": "$g", "c": {"$sum": 1i32}, "s": {"$sum": "$i"}};
    let pipeline = vec![
        bson::Bson::Document(doc! {"$group": group}),
        bson::Bson::Document(doc! {"$sort": {"_id": 1i32}}),
    ];
    let agg = doc! {"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 2);
    let d0 = fb[0].as_document().unwrap();
    assert_eq!(d0.get_str("_id").unwrap(), "x");
    assert_eq!(d0.get_i32("c").unwrap(), 2);
    assert_eq!(d0.get_f64("s").unwrap(), 3.0);
    let d1 = fb[1].as_document().unwrap();
    assert_eq!(d1.get_str("_id").unwrap(), "y");
    assert_eq!(d1.get_i32("c").unwrap(), 1);
    assert_eq!(d1.get_f64("s").unwrap(), 4.0);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
