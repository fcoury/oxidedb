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
#[ignore]
async fn e2e_aggregate_count_large_int64() {
    if std::env::var("OXIDEDB_TEST_LARGE_COUNT").ok().as_deref() != Some("1") {
        eprintln!("skipping: set OXIDEDB_TEST_LARGE_COUNT=1 to enable");
        return;
    }

    let target = std::env::var("OXIDEDB_TEST_LARGE_COUNT_TARGET")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or((i32::MAX as u64) + 1);

    if target <= i32::MAX as u64 {
        eprintln!("skipping: target must be > i32::MAX for Int64 verification");
        return;
    }

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

    let dbname = format!("agg_count_large_{}", rand_suffix(6));

    let create = doc! {"create": "u", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert documents in batches to reach the target count
    let batch_size: u64 = 10_000;
    let mut inserted: u64 = 0;
    let mut req_id = 2;
    while inserted < target {
        let remaining = target - inserted;
        let current = std::cmp::min(batch_size, remaining) as usize;
        let mut docs = Vec::with_capacity(current);
        for i in 0..current {
            docs.push(doc! {"_id": format!("{}", inserted + i as u64)});
        }
        let ins = doc! {"insert": "u", "documents": docs, "$db": &dbname};
        let msg = encode_op_msg(&ins, 0, req_id);
        req_id += 1;
        stream.write_all(&msg).await.unwrap();
        let _ = read_one_op_msg(&mut stream).await;
        inserted += current as u64;
    }

    let pipeline = vec![bson::Bson::Document(doc! {"$count": "c"})];
    let agg = doc! {"aggregate": "u", "pipeline": pipeline, "cursor": {}, "$db": &dbname};
    let msg = encode_op_msg(&agg, 0, req_id);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    let fb = doc
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(fb.len(), 1);
    let result = fb[0].as_document().unwrap();
    assert!(matches!(result.get("c"), Some(bson::Bson::Int64(_))));

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
