mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;
use bson::{doc, oid::ObjectId};

#[tokio::test]
async fn filter_gt_in_exists_top_level() {
    let td = match TestDb::provision_from_env().await {
        Some(v) => v,
        None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; }
    };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");
    store.ensure_collection("test", "nums").await.expect("ensure");

    // Insert docs: ages 20, 30, missing age
    let docs = vec![
        doc!{"_id": ObjectId::new(), "age": 20i32, "name": "a"},
        doc!{"_id": ObjectId::new(), "age": 30i32, "name": "b"},
        doc!{"_id": ObjectId::new(), "name": "noage"},
    ];
    for d in docs.iter() {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        let b = bson::to_vec(d).unwrap();
        let j = serde_json::to_value(d).unwrap();
        store.insert_one("test", "nums", &id, &b, &j).await.unwrap();
    }

    // age > 25
    let filter = doc!{"age": {"$gt": 25}};
    let res = store.find_with_top_level_filter("test", "nums", &filter, 100).await.unwrap();
    assert!(res.iter().all(|d| d.get_i32("age").unwrap_or(0) > 25));

    // name in ["a","c"]
    let filter = doc!{"name": {"$in": ["a", "c"]}};
    let res = store.find_with_top_level_filter("test", "nums", &filter, 100).await.unwrap();
    assert!(res.iter().all(|d| d.get_str("name").map(|s| s == "a").unwrap_or(false)));

    // age exists false
    let filter = doc!{"age": {"$exists": false}};
    let res = store.find_with_top_level_filter("test", "nums", &filter, 100).await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get_str("name").unwrap(), "noage");
}

