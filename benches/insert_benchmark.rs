// Insert operation benchmarks
use bson::doc;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::{Rng, distributions::Alphanumeric};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;
use common::*;

async fn setup_server() -> (String, TcpStream) {
    let testdb = common::postgres::TestDb::provision_from_env()
        .await
        .expect("Failed to provision test database - ensure OXIDEDB_TEST_POSTGRES_URL is set");

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, _shutdown, _handle) = spawn_with_shutdown(cfg).await.unwrap();
    let stream = TcpStream::connect(addr).await.unwrap();

    (testdb.dbname.clone(), stream)
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

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn generate_document(size: DocumentSize) -> bson::Document {
    match size {
        DocumentSize::Small => {
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "name": random_string(10),
                "value": rand::thread_rng().gen_range(1..1000),
            }
        }
        DocumentSize::Medium => {
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "name": random_string(10),
                "email": format!("{}@example.com", random_string(8)),
                "age": rand::thread_rng().gen_range(18..80),
                "tags": (0..5).map(|_| random_string(5)).collect::<Vec<_>>(),
            }
        }
        DocumentSize::Large => {
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "name": random_string(20),
                "description": random_string(200),
                "data": (0..100).map(|_| random_string(30)).collect::<Vec<_>>(),
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum DocumentSize {
    Small,
    Medium,
    Large,
}

fn bench_insert_single(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("insert_single");
    group.measurement_time(Duration::from_secs(10));

    for size in [
        DocumentSize::Small,
        DocumentSize::Medium,
        DocumentSize::Large,
    ] {
        let size_name = format!("{:?}", size);

        group.bench_with_input(
            BenchmarkId::new("size", &size_name),
            &size,
            |b, &doc_size| {
                b.to_async(&rt).iter(|| async {
                    let (dbname, mut stream) = setup_server().await;

                    // Create collection
                    let create = doc! {"create": "bench", "$db": &dbname};
                    stream
                        .write_all(&encode_op_msg(&create, 0, 1))
                        .await
                        .unwrap();
                    let _ = read_one_op_msg(&mut stream).await;

                    // Benchmark insert
                    let doc = generate_document(doc_size);
                    let insert = doc! {"insert": "bench", "documents": [doc], "$db": &dbname};

                    stream
                        .write_all(&encode_op_msg(&insert, 0, 2))
                        .await
                        .unwrap();
                    let response = read_one_op_msg(&mut stream).await;

                    black_box(response);
                });
            },
        );
    }

    group.finish();
}

fn bench_insert_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("insert_batch");
    group.measurement_time(Duration::from_secs(10));

    for batch_size in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            &batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let (dbname, mut stream) = setup_server().await;

                    // Create collection
                    let create = doc! {"create": "bench", "$db": &dbname};
                    stream
                        .write_all(&encode_op_msg(&create, 0, 1))
                        .await
                        .unwrap();
                    let _ = read_one_op_msg(&mut stream).await;

                    // Generate batch
                    let docs: Vec<bson::Document> = (0..size)
                        .map(|_| generate_document(DocumentSize::Medium))
                        .collect();

                    let insert = doc! {"insert": "bench", "documents": docs, "$db": &dbname};

                    stream
                        .write_all(&encode_op_msg(&insert, 0, 2))
                        .await
                        .unwrap();
                    let response = read_one_op_msg(&mut stream).await;

                    black_box(response);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(insert_benches, bench_insert_single, bench_insert_batch);
criterion_main!(insert_benches);
