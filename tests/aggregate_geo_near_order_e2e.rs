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
async fn e2e_geo_near_must_be_first_stage() {
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

    let dbname = format!("geo_order_{}", rand_suffix(6));

    let create = doc! {"create": "places", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let docs = vec![doc! {
        "_id": "1",
        "name": "Central Park",
        "location": {"type": "Point", "coordinates": [-73.9654, 40.7829]}
    }];

    let insert = doc! {"insert": "places", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    let pipeline = vec![
        bson::Bson::Document(doc! {"$project": {"name": 1}}),
        bson::Bson::Document(doc! {"$geoNear": {
            "near": {"$geometry": {"type": "Point", "coordinates": [-73.98, 40.75]}},
            "distanceField": "distance",
            "spherical": true,
            "key": "location"
        }}),
    ];

    let aggregate = doc! {
        "aggregate": "places",
        "pipeline": pipeline,
        "cursor": {},
        "$db": &dbname
    };
    let msg = encode_op_msg(&aggregate, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(1.0), 0.0);
    let errmsg = doc.get_str("errmsg").unwrap_or("");
    assert!(errmsg.contains("$geoNear"));

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
