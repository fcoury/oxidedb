// Insert operation benchmarks
use bson::doc;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oxidedb::protocol::{decode_op_msg_section0, encode_op_msg};
use rand::Rng;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod common;
use common::postgres::TestDb;
use common::server::BenchServer;

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
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

async fn read_one_op_msg(stream: &mut TcpStream) -> bson::Document {
    use oxidedb::protocol::{MessageHeader, OP_MSG};
    let mut header = [0u8; 16];
    stream.read_exact(&mut header).await.unwrap();
    let (hdr, _) = MessageHeader::parse(&header).unwrap();
    assert_eq!(hdr.op_code, OP_MSG);
    let mut body = vec![0u8; (hdr.message_length as usize) - 16];
    stream.read_exact(&mut body).await.unwrap();
    let (_flags, doc) = decode_op_msg_section0(&body).unwrap();
    doc
}

struct BenchContext {
    _server: BenchServer,
    stream: TcpStream,
    dbname: String,
    request_id: std::cell::Cell<i32>,
}

impl BenchContext {
    async fn new(doc_size: DocumentSize) -> Self {
        let testdb = TestDb::provision_from_env()
            .await
            .expect("Failed to provision test database");
        let server = BenchServer::start(testdb).await;
        let mut stream = TcpStream::connect(server.addr()).await.unwrap();
        let dbname = server.dbname().to_string();

        // Create collection once
        let create = doc! {"create": "bench", "$db": &dbname};
        stream
            .write_all(&encode_op_msg(&create, 0, 1))
            .await
            .unwrap();
        let _ = read_one_op_msg(&mut stream).await;

        Self {
            _server: server,
            stream,
            dbname,
            request_id: std::cell::Cell::new(2),
        }
    }

    async fn insert_single(&mut self, doc_size: DocumentSize) -> bson::Document {
        let doc = generate_document(doc_size);
        let insert = doc! {"insert": "bench", "documents": [doc], "$db": &self.dbname};
        let req_id = self.request_id.get();

        self.stream
            .write_all(&encode_op_msg(&insert, 0, req_id))
            .await
            .unwrap();
        let response = read_one_op_msg(&mut self.stream).await;

        self.request_id.set(req_id + 1);
        response
    }

    async fn insert_batch(&mut self, batch_size: usize) -> bson::Document {
        let docs: Vec<bson::Document> = (0..batch_size)
            .map(|_| generate_document(DocumentSize::Medium))
            .collect();

        let insert = doc! {"insert": "bench", "documents": docs, "$db": &self.dbname};
        let req_id = self.request_id.get();

        self.stream
            .write_all(&encode_op_msg(&insert, 0, req_id))
            .await
            .unwrap();
        let response = read_one_op_msg(&mut self.stream).await;

        self.request_id.set(req_id + 1);
        response
    }
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
                b.to_async(&rt).iter_batched(
                    || rt.block_on(BenchContext::new(doc_size)),
                    |mut ctx| async move {
                        let response = ctx.insert_single(doc_size).await;
                        black_box(response);
                    },
                    criterion::BatchSize::PerIteration,
                );
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
                b.to_async(&rt).iter_batched(
                    || rt.block_on(BenchContext::new(DocumentSize::Medium)),
                    |mut ctx| async move {
                        let response = ctx.insert_batch(size).await;
                        black_box(response);
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(insert_benches, bench_insert_single, bench_insert_batch);
criterion_main!(insert_benches);
