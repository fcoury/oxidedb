// Aggregation pipeline benchmarks
use bson::doc;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use oxidedb::config::Config;
use oxidedb::protocol::{MessageHeader, OP_MSG, decode_op_msg_section0, encode_op_msg};
use oxidedb::server::spawn_with_shutdown;
use rand::Rng;
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
                    "category": format!("cat_{}", (inserted + i) % 10),
                    "value": rand::thread_rng().gen_range(1..1000),
                    "quantity": rand::thread_rng().gen_range(1..100),
                    "tags": vec!["a", "b", "c"],
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

fn bench_aggregate_match(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregate_match");
    group.measurement_time(Duration::from_secs(10));

    for &collection_size in &[100, 1000, 5000] {
        group.bench_with_input(
            BenchmarkId::new("collection_size", collection_size),
            &collection_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let (dbname, mut stream) = setup_server_with_data(size).await;

                    let pipeline = vec![bson::Bson::Document(doc! {
                        "$match": {"value": {"$gt": 500}}
                    })];

                    let agg = doc! {
                        "aggregate": "bench",
                        "pipeline": pipeline,
                        "cursor": {},
                        "$db": &dbname
                    };

                    stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
                    let response = read_one_op_msg(&mut stream).await;

                    black_box(response);
                });
            },
        );
    }

    group.finish();
}

fn bench_aggregate_group(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregate_group");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 5000;

    // Simple group by category
    group.bench_function("group_by_category", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let pipeline = vec![bson::Bson::Document(doc! {
                "$group": {
                    "_id": "$category",
                    "total": {"$sum": "$value"},
                    "count": {"$sum": 1},
                    "avg": {"$avg": "$value"}
                }
            })];

            let agg = doc! {
                "aggregate": "bench",
                "pipeline": pipeline,
                "cursor": {},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    // Group with match stage
    group.bench_function("match_then_group", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let pipeline = vec![
                bson::Bson::Document(doc! {"$match": {"value": {"$gt": 300}}}),
                bson::Bson::Document(doc! {
                    "$group": {
                        "_id": "$category",
                        "total": {"$sum": "$value"},
                        "count": {"$sum": 1}
                    }
                }),
            ];

            let agg = doc! {
                "aggregate": "bench",
                "pipeline": pipeline,
                "cursor": {},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.finish();
}

fn bench_aggregate_sort_limit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregate_sort_limit");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 5000;

    group.bench_function("sort_desc_limit", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let pipeline = vec![
                bson::Bson::Document(doc! {"$sort": {"value": -1}}),
                bson::Bson::Document(doc! {"$limit": 10i32}),
            ];

            let agg = doc! {
                "aggregate": "bench",
                "pipeline": pipeline,
                "cursor": {},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.bench_function("match_sort_limit", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let pipeline = vec![
                bson::Bson::Document(doc! {"$match": {"category": "cat_1"}}),
                bson::Bson::Document(doc! {"$sort": {"value": -1}}),
                bson::Bson::Document(doc! {"$limit": 20i32}),
            ];

            let agg = doc! {
                "aggregate": "bench",
                "pipeline": pipeline,
                "cursor": {},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.finish();
}

fn bench_aggregate_project(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregate_project");
    group.measurement_time(Duration::from_secs(10));

    let collection_size = 5000;

    group.bench_function("project_fields", |b| {
        b.to_async(&rt).iter(|| async {
            let (dbname, mut stream) = setup_server_with_data(collection_size).await;

            let pipeline = vec![bson::Bson::Document(doc! {
                "$project": {
                    "category": 1,
                    "value": 1,
                    "doubled": {"$multiply": ["$value", 2]},
                    "total": {"$add": ["$value", "$quantity"]}
                }
            })];

            let agg = doc! {
                "aggregate": "bench",
                "pipeline": pipeline,
                "cursor": {},
                "$db": &dbname
            };

            stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
            let response = read_one_op_msg(&mut stream).await;

            black_box(response);
        });
    });

    group.finish();
}

fn bench_aggregate_multistage(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregate_multistage");
    group.measurement_time(Duration::from_secs(10));

    for &collection_size in &[100, 1000, 5000] {
        group.bench_with_input(
            BenchmarkId::new("collection_size", collection_size),
            &collection_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let (dbname, mut stream) = setup_server_with_data(size).await;

                    let pipeline = vec![
                        bson::Bson::Document(doc! {"$match": {"value": {"$gt": 100}}}),
                        bson::Bson::Document(doc! {"$sort": {"value": -1}}),
                        bson::Bson::Document(doc! {"$limit": 50i32}),
                        bson::Bson::Document(doc! {
                            "$project": {
                                "category": 1,
                                "value": 1,
                                "computed": {"$add": ["$value", "$quantity"]}
                            }
                        }),
                    ];

                    let agg = doc! {
                        "aggregate": "bench",
                        "pipeline": pipeline,
                        "cursor": {},
                        "$db": &dbname
                    };

                    stream.write_all(&encode_op_msg(&agg, 0, 3)).await.unwrap();
                    let response = read_one_op_msg(&mut stream).await;

                    black_box(response);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    aggregate_benches,
    bench_aggregate_match,
    bench_aggregate_group,
    bench_aggregate_sort_limit,
    bench_aggregate_project,
    bench_aggregate_multistage
);
criterion_main!(aggregate_benches);
