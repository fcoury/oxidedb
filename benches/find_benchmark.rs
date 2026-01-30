// Find/Query operation benchmarks
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
    async fn with_data(doc_count: usize) -> Self {
        let testdb = TestDb::provision_from_env()
            .await
            .expect("Failed to provision test database");
        let server = BenchServer::start(testdb).await;
        let mut stream = TcpStream::connect(server.addr()).await.unwrap();
        let dbname = server.dbname().to_string();

        // Create collection
        let create = doc! {"create": "bench", "$db": &dbname};
        stream
            .write_all(&encode_op_msg(&create, 0, 1))
            .await
            .unwrap();
        let _ = read_one_op_msg(&mut stream).await;

        // Insert test data
        let batch_size = 100;
        let mut inserted = 0;
        let mut req_id = 2i32;
        while inserted < doc_count {
            let to_insert = std::cmp::min(batch_size, doc_count - inserted);
            let docs: Vec<bson::Document> = (0..to_insert)
                .map(|i| {
                    doc! {
                        "_id": bson::oid::ObjectId::new(),
                        "index": (inserted + i) as i32,
                        "name": format!("user_{}", inserted + i),
                        "age": rand::thread_rng().gen_range(18..80),
                        "score": rand::thread_rng().gen_range(0.0..100.0),
                        "tags": vec!["tag1", "tag2", "tag3"],
                        "active": rand::thread_rng().gen_bool(0.8),
                    }
                })
                .collect();

            let insert = doc! {"insert": "bench", "documents": docs, "$db": &dbname};
            stream
                .write_all(&encode_op_msg(&insert, 0, req_id))
                .await
                .unwrap();
            let _ = read_one_op_msg(&mut stream).await;

            inserted += to_insert;
            req_id += 1;
        }

        Self {
            _server: server,
            stream,
            dbname,
            request_id: std::cell::Cell::new(req_id),
        }
    }

    async fn find(&mut self, filter: bson::Document) -> bson::Document {
        let find = doc! {
            "find": "bench",
            "filter": filter,
            "$db": &self.dbname
        };
        let req_id = self.request_id.get();

        self.stream
            .write_all(&encode_op_msg(&find, 0, req_id))
            .await
            .unwrap();
        let response = read_one_op_msg(&mut self.stream).await;

        self.request_id.set(req_id + 1);
        response
    }

    async fn find_with_options(
        &mut self,
        filter: bson::Document,
        projection: Option<bson::Document>,
        sort: Option<bson::Document>,
        limit: Option<i32>,
    ) -> bson::Document {
        let mut find = doc! {
            "find": "bench",
            "filter": filter,
            "$db": &self.dbname
        };
        if let Some(proj) = projection {
            find.insert("projection", proj);
        }
        if let Some(s) = sort {
            find.insert("sort", s);
        }
        if let Some(l) = limit {
            find.insert("limit", l);
        }

        let req_id = self.request_id.get();
        self.stream
            .write_all(&encode_op_msg(&find, 0, req_id))
            .await
            .unwrap();
        let response = read_one_op_msg(&mut self.stream).await;

        self.request_id.set(req_id + 1);
        response
    }
}

fn bench_find_by_id(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_by_id");
    group.measurement_time(Duration::from_secs(10));

    for &collection_size in &[100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("collection_size", collection_size),
            &collection_size,
            |b, &size| {
                b.to_async(&rt).iter_batched(
                    || rt.block_on(BenchContext::with_data(size)),
                    |mut ctx| async move {
                        let response = ctx.find(doc! {"index": 0}).await;
                        black_box(response);
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn bench_find_with_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_filter");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 1000;

    group.bench_function("equality", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx.find(doc! {"active": true}).await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("range_gt", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx.find(doc! {"age": {"$gt": 50}}).await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("in_operator", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx.find(doc! {"age": {"$in": [25, 35, 45, 55, 65]}}).await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("or_operator", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find(doc! {"$or": [{"age": {"$lt": 30}}, {"age": {"$gt": 60}}]})
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("complex", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find(doc! {
                        "$and": [
                            {"active": true},
                            {"age": {"$gte": 25, "$lte": 55}},
                            {"score": {"$gt": 50.0}}
                        ]
                    })
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_find_with_projection(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_projection");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 1000;

    group.bench_function("include_fields", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find_with_options(
                        doc! {},
                        Some(doc! {"name": 1, "age": 1}),
                        None,
                        Some(100i32),
                    )
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("exclude_fields", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find_with_options(
                        doc! {},
                        Some(doc! {"tags": 0, "metadata": 0}),
                        None,
                        Some(100i32),
                    )
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn bench_find_with_sort(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_sort");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 1000;

    group.bench_function("sort_single", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find_with_options(doc! {}, None, Some(doc! {"age": 1}), Some(100i32))
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("sort_multiple", |b| {
        b.to_async(&rt).iter_batched(
            || rt.block_on(BenchContext::with_data(collection_size)),
            |mut ctx| async move {
                let response = ctx
                    .find_with_options(
                        doc! {},
                        None,
                        Some(doc! {"active": -1, "age": 1, "score": -1}),
                        Some(100i32),
                    )
                    .await;
                black_box(response);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    find_benches,
    bench_find_by_id,
    bench_find_with_filter,
    bench_find_with_projection,
    bench_find_with_sort
);
criterion_main!(find_benches);
