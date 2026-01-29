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
async fn e2e_list_databases_and_collections() {
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

    let dbname = format!("list_{}", rand_suffix(6));

    // create a collection "u" in dbname
    let create = doc! { "create": "u", "$db": &dbname };
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // listDatabases (nameOnly)
    let ldb = doc! { "listDatabases": 1i32, "nameOnly": true, "$db": "admin" };
    let msg = encode_op_msg(&ldb, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let dbs = doc.get_array("databases").unwrap();
    let mut found = false;
    for d in dbs {
        if let bson::Bson::Document(dd) = d {
            if dd.get_str("name").ok() == Some(dbname.as_str()) {
                found = true;
                break;
            }
        }
    }
    assert!(found, "listDatabases should include {}", dbname);

    // listCollections on dbname
    let lc = doc! { "listCollections": 1i32, "$db": &dbname };
    let msg = encode_op_msg(&lc, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();
    let mut found_u = false;
    for c in first_batch {
        if let bson::Bson::Document(dd) = c {
            if dd.get_str("name").ok() == Some("u") {
                found_u = true;
                break;
            }
        }
    }
    assert!(found_u, "listCollections should include 'u'");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
