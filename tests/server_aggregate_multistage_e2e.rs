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
async fn e2e_aggregate_match_sort_project_limit() {
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

    let dbname = format!("agg_multi_{}", rand_suffix(6));

    // create + insert
    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![
        doc! {"_id": "a", "group": "x", "i": 1, "extra": "e1"},
        doc! {"_id": "b", "group": "x", "i": 3, "extra": "e2"},
        doc! {"_id": "c", "group": "x", "i": 2, "extra": "e3"},
        doc! {"_id": "d", "group": "y", "i": 100, "extra": "e4"},
    ];
    let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&ins, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // pipeline: match group x, sort i desc, project {_id:0,i:1}, limit 2
    let pipeline = vec![
        bson::Bson::Document(doc! {"$match": {"group": "x"}}),
        bson::Bson::Document(doc! {"$sort": {"i": -1i32}}),
        bson::Bson::Document(doc! {"$project": {"_id": 0i32, "i": 1i32}}),
        bson::Bson::Document(doc! {"$limit": 2i32}),
    ];
    let agg = doc! {"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let fb = cursor.get_array("firstBatch").unwrap();
    assert_eq!(fb.len(), 2);
    // Expect i values [3,2]
    assert_eq!(fb[0].as_document().unwrap().get_i32("i").unwrap(), 3);
    assert_eq!(fb[1].as_document().unwrap().get_i32("i").unwrap(), 2);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
