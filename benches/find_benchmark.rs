// Find/Query operation benchmarks
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

async fn setup_server_with_data(doc_count: usize) -> (String, TcpStream) {
    let testdb = common::postgres::TestDb::provision_from_env()
        .await
        .expect("Failed to provision test database - ensure OXIDEDB_TEST_POSTGRES_URL is set");

    let mut cfg = Config::default();
    cfg.listen_addr = "127.0.0.1:0".into();
    cfg.postgres_url = Some(testdb.url.clone());

    let (_state, addr, _shutdown, _handle) = spawn_with_shutdown(cfg).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Create collection
    let create = doc! {"create": "bench", "$db": &testdb.dbname};
    stream
        .write_all(&encode_op_msg(&create, 0, 1))
        .await
        .unwrap();
    let _ = read_one_op_msg(&mut stream).await;

    // Insert test data
    let batch_size = 100;
    let mut inserted = 0;
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

        let insert = doc! {"insert": "bench", "documents": docs, "$db": &testdb.dbname};
        stream
            .write_all(&encode_op_msg(&insert, 0, 2))
            .await
            .unwrap();
        let _ = read_one_op_msg(&mut stream).await;

        inserted += to_insert;
    }

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

fn bench_find_by_id(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_by_id");
    group.measurement_time(Duration::from_secs(10));

    for &collection_size in &[100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("collection_size", collection_size),
            &collection_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let (dbname, mut stream) = setup_server_with_data(size).await;

                    // Find by ID (first document)
                    let find = doc! {
                        "find": "bench",
                        "filter": {"index": 0},
                        "$db": &dbname
                    };

                    stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
                    let response = read_one_op_msg(&mut stream).await;

                    black_box(response);
                });
            },
        );
    }

    group.finish();
}

fn bench_find_with_filter(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_filter");
    group.measurement_time(Duration::from_secs(10));

    // Pre-setup with 1000 documents
    let collection_size = 1000;

    // Benchmark simple equality filter
    group.bench_function("equality", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {"active": true},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    // Benchmark range filter
    group.bench_function("range_gt", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {"age": {"$gt": 50}},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    // Benchmark $in filter
    group.bench_function("in_operator", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {"age": {"$in": [25, 35, 45, 55, 65]}},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    // Benchmark $or filter
    group.bench_function("or_operator", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {"$or": [{"age": {"$lt": 30}}, {"age": {"$gt": 60}}]},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    // Benchmark complex filter
    group.bench_function("complex", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {
                    "$and": [
                        {"active": true},
                        {"age": {"$gte": 25, "$lte": 55}},
                        {"score": {"$gt": 50.0}}
                    ]
                },
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.finish();
}

fn bench_find_with_projection(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_projection");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 1000;

    group.bench_function("include_fields", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {},
                "projection": {"name": 1, "age": 1},
                "limit": 100i32,
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.bench_function("exclude_fields", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {},
                "projection": {"tags": 0, "metadata": 0},
                "limit": 100i32,
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.finish();
}

fn bench_find_with_sort(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("find_with_sort");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 1000;

    group.bench_function("sort_single", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {},
                "sort": {"age": 1},
                "limit": 100i32,
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.bench_function("sort_multiple", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let find = doc! {
                "find": "bench",
                "filter": {},
                "sort": {"active": -1, "age": 1, "score": -1},
                "limit": 100i32,
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&find, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
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
