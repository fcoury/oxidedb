mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;
use bson::{doc, oid::ObjectId};

#[tokio::test]
async fn sort_and_projection_pushdown() {
    let td = match TestDb::provision_from_env().await { Some(v) => v, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");
    store.ensure_collection("test", "items").await.expect("ensure");

    // Insert docs with unsorted names and extra fields
    let docs = vec![
        doc!{"_id": ObjectId::new(), "name": "b", "n": 2i32, "x": 1i32},
        doc!{"_id": ObjectId::new(), "name": "a", "n": 1i32, "x": 2i32},
        doc!{"_id": ObjectId::new(), "name": "c", "n": 3i32, "x": 3i32},
    ];
    for d in &docs {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        let b = bson::to_vec(d).unwrap();
        let j = serde_json::to_value(d).unwrap();
        store.insert_one("test", "items", &id, &b, &j).await.unwrap();
    }

    // Sort by name asc, project only name
    let sort = doc!{"name": 1};
    let projection = doc!{"name": 1};
    let res = store.find_docs("test", "items", None, Some(&sort), Some(&projection), 10).await.unwrap();
    let names: Vec<String> = res.iter().map(|d| d.get_str("name").unwrap().to_string()).collect();
    assert_eq!(names, vec!["a","b","c"]);
    // Ensure only projected fields (plus _id)
    for d in res {
        assert!(d.get_document("_id").is_ok() || d.get_object_id("_id").is_ok() || d.contains_key("_id"));
        assert!(d.get("n").is_none());
        assert!(d.get("x").is_none());
    }

    // Exclude _id explicitly
    let projection = doc!{"name": 1, "_id": 0};
    let res = store.find_docs("test", "items", None, Some(&sort), Some(&projection), 10).await.unwrap();
    for d in res { assert!(!d.contains_key("_id")); }
}

