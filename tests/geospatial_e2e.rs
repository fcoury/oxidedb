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
async fn e2e_create_2dsphere_index() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "locations", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // create 2dsphere index
    let idx_spec = doc! {"name": "loc_2dsphere", "key": {"location": "2dsphere"}};
    let ci = doc! {"createIndexes": "locations", "indexes": [idx_spec], "$db": &dbname};
    let msg = encode_op_msg(&ci, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);
    assert!(doc.get_i32("createdIndexes").unwrap_or(0) >= 1);

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_geo_near_aggregation() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "places", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents with GeoJSON Point locations
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "Central Park",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9654, 40.7829]  // NYC
            }
        },
        doc! {
            "_id": "2",
            "name": "Times Square",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]  // NYC
            }
        },
        doc! {
            "_id": "3",
            "name": "Statue of Liberty",
            "location": doc! {
                "type": "Point",
                "coordinates": [-74.0445, 40.6892]  // NYC
            }
        },
        doc! {
            "_id": "4",
            "name": "Golden Gate Bridge",
            "location": doc! {
                "type": "Point",
                "coordinates": [-122.4783, 37.8199]  // San Francisco
            }
        },
    ];

    let insert = doc! {"insert": "places", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test $geoNear aggregation stage
    let pipeline = vec![bson::Bson::Document(doc! {
        "$geoNear": doc! {
            "near": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]  // Times Square
            },
            "distanceField": "distance",
            "maxDistance": 10000.0,  // 10km in meters
            "spherical": true,
            "key": "location"
        }
    })];

    let aggregate = doc! {
        "aggregate": "places",
        "pipeline": pipeline,
        "cursor": doc! {},
        "$db": &dbname
    };
    let msg = encode_op_msg(&aggregate, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Check that we got results with distance field
    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find Times Square (distance ~0), Central Park, and Statue of Liberty
    // Golden Gate Bridge should be too far
    assert!(!first_batch.is_empty(), "Should find nearby locations");

    // Check that distance field is present and correct
    for result in first_batch {
        if let bson::Bson::Document(place_doc) = result {
            assert!(
                place_doc.contains_key("distance"),
                "Result should have distance field"
            );
            let distance = place_doc.get_f64("distance").unwrap_or(-1.0);
            assert!(distance >= 0.0, "Distance should be non-negative");

            // Times Square should have distance close to 0
            if place_doc.get_str("_id").unwrap_or("") == "2" {
                assert!(
                    distance < 1.0,
                    "Times Square should have distance close to 0"
                );
            }
        }
    }

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_geo_within_polygon() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "areas", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "Inside NYC",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9654, 40.7829]  // Central Park, NYC
            }
        },
        doc! {
            "_id": "2",
            "name": "Inside NYC",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]  // Times Square, NYC
            }
        },
        doc! {
            "_id": "3",
            "name": "Outside NYC",
            "location": doc! {
                "type": "Point",
                "coordinates": [-122.4783, 37.8199]  // San Francisco
            }
        },
    ];

    let insert = doc! {"insert": "areas", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test $geoWithin with Polygon
    let filter = doc! {
        "location": doc! {
            "$geoWithin": doc! {
                "$geometry": doc! {
                    "type": "Polygon",
                    "coordinates": vec![
                        bson::Bson::Array(vec![
                            bson::Bson::Array(vec![
                                bson::Bson::Double(-74.5),
                                bson::Bson::Double(40.5),
                            ]),
                            bson::Bson::Array(vec![
                                bson::Bson::Double(-73.5),
                                bson::Bson::Double(40.5),
                            ]),
                            bson::Bson::Array(vec![
                                bson::Bson::Double(-73.5),
                                bson::Bson::Double(41.0),
                            ]),
                            bson::Bson::Array(vec![
                                bson::Bson::Double(-74.5),
                                bson::Bson::Double(41.0),
                            ]),
                            bson::Bson::Array(vec![
                                bson::Bson::Double(-74.5),
                                bson::Bson::Double(40.5),
                            ]),
                        ])
                    ]
                }
            }
        }
    };

    let find = doc! {
        "find": "areas",
        "filter": filter,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find the two NYC locations inside the polygon
    assert_eq!(
        first_batch.len(),
        2,
        "Should find 2 locations inside polygon"
    );

    // Verify we got the right documents
    let ids: Vec<String> = first_batch
        .iter()
        .filter_map(|d| d.as_document())
        .filter_map(|d| d.get_str("_id").ok())
        .map(|s| s.to_string())
        .collect();
    assert!(ids.contains(&"1".to_string()), "Should find document 1");
    assert!(ids.contains(&"2".to_string()), "Should find document 2");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_near_with_max_distance() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "nearby", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "Close",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]  // Times Square
            }
        },
        doc! {
            "_id": "2",
            "name": "Medium",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9654, 40.7829]  // Central Park (~2km away)
            }
        },
        doc! {
            "_id": "3",
            "name": "Far",
            "location": doc! {
                "type": "Point",
                "coordinates": [-74.0445, 40.6892]  // Statue of Liberty (~8km away)
            }
        },
    ];

    let insert = doc! {"insert": "nearby", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test $near with maxDistance
    let filter = doc! {
        "location": doc! {
            "$near": doc! {
                "$geometry": doc! {
                    "type": "Point",
                    "coordinates": [-73.9857, 40.7589]  // Times Square
                },
                "$maxDistance": 5000.0  // 5km
            }
        }
    };

    let find = doc! {
        "find": "nearby",
        "filter": filter,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find Times Square (distance 0) and Central Park (~2km)
    // Statue of Liberty (~8km) should be excluded
    assert_eq!(first_batch.len(), 2, "Should find 2 locations within 5km");

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_near_sphere_spherical() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "sphere_test", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents with various locations
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "NYC",
            "location": doc! {
                "type": "Point",
                "coordinates": [-74.0060, 40.7128]
            }
        },
        doc! {
            "_id": "2",
            "name": "Boston",
            "location": doc! {
                "type": "Point",
                "coordinates": [-71.0589, 42.3601]
            }
        },
        doc! {
            "_id": "3",
            "name": "London",
            "location": doc! {
                "type": "Point",
                "coordinates": [-0.1276, 51.5074]
            }
        },
    ];

    let insert = doc! {"insert": "sphere_test", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test $nearSphere with spherical calculations
    let filter = doc! {
        "location": doc! {
            "$nearSphere": doc! {
                "$geometry": doc! {
                    "type": "Point",
                    "coordinates": [-74.0060, 40.7128]  // NYC
                },
                "$maxDistance": 350000.0  // 350km - should include Boston but not London
            }
        }
    };

    let find = doc! {
        "find": "sphere_test",
        "filter": filter,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find NYC (distance 0) and Boston (~300km)
    // London (~5500km) should be excluded
    assert_eq!(first_batch.len(), 2, "Should find 2 locations within 300km");

    // Verify results are sorted by distance (NYC first, then Boston)
    if let Some(bson::Bson::Document(first)) = first_batch.first() {
        assert_eq!(
            first.get_str("_id").unwrap_or(""),
            "1",
            "First result should be NYC"
        );
    }

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_geo_within_legacy_box() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "legacy_box", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "Inside",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]
            }
        },
        doc! {
            "_id": "2",
            "name": "Outside",
            "location": doc! {
                "type": "Point",
                "coordinates": [-122.4783, 37.8199]
            }
        },
    ];

    let insert = doc! {"insert": "legacy_box", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test legacy $box operator
    let filter = doc! {
        "location": doc! {
            "$geoWithin": doc! {
                "$box": vec![
                    bson::Bson::Array(vec![
                        bson::Bson::Double(-74.5),
                        bson::Bson::Double(40.5),
                    ]),
                    bson::Bson::Array(vec![
                        bson::Bson::Double(-73.5),
                        bson::Bson::Double(41.0),
                    ]),
                ]
            }
        }
    };

    let find = doc! {
        "find": "legacy_box",
        "filter": filter,
        "$db": &dbname
    };
    let msg = encode_op_msg(&find, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find only the document inside the box
    assert_eq!(first_batch.len(), 1);
    if let Some(bson::Bson::Document(result)) = first_batch.first() {
        assert_eq!(result.get_str("_id").unwrap_or(""), "1");
    }

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}

#[tokio::test]
async fn e2e_geo_near_with_query_filter() {
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

    let dbname = format!("geo_{}", rand_suffix(6));

    // create collection
    let create = doc! {"create": "filtered", "$db": &dbname};
    let msg = encode_op_msg(&create, 0, 1);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Insert test documents with categories
    let docs = vec![
        doc! {
            "_id": "1",
            "name": "Restaurant A",
            "category": "restaurant",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]
            }
        },
        doc! {
            "_id": "2",
            "name": "Restaurant B",
            "category": "restaurant",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9654, 40.7829]
            }
        },
        doc! {
            "_id": "3",
            "name": "Museum",
            "category": "museum",
            "location": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]
            }
        },
    ];

    let insert = doc! {"insert": "filtered", "documents": docs, "$db": &dbname};
    let msg = encode_op_msg(&insert, 0, 2);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;
    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    // Test $geoNear with query filter
    let pipeline = vec![bson::Bson::Document(doc! {
        "$geoNear": doc! {
            "near": doc! {
                "type": "Point",
                "coordinates": [-73.9857, 40.7589]
            },
            "distanceField": "distance",
            "maxDistance": 10000.0,
            "spherical": true,
            "key": "location",
            "query": doc! {
                "category": "restaurant"
            }
        }
    })];

    let aggregate = doc! {
        "aggregate": "filtered",
        "pipeline": pipeline,
        "cursor": doc! {},
        "$db": &dbname
    };
    let msg = encode_op_msg(&aggregate, 0, 3);
    stream.write_all(&msg).await.unwrap();
    let doc = read_one_op_msg(&mut stream).await;

    assert_eq!(doc.get_f64("ok").unwrap_or(0.0), 1.0);

    let cursor = doc.get_document("cursor").unwrap();
    let first_batch = cursor.get_array("firstBatch").unwrap();

    // Should find only restaurants (2), not the museum
    assert_eq!(first_batch.len(), 2, "Should find only 2 restaurants");

    for result in first_batch {
        if let bson::Bson::Document(place_doc) = result {
            assert_eq!(
                place_doc.get_str("category").unwrap_or(""),
                "restaurant",
                "All results should be restaurants"
            );
        }
    }

    let _ = shutdown.send(true);
    let _ = handle.await.unwrap();
}
