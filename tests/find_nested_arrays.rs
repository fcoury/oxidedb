mod common;
use bson::{doc, oid::ObjectId};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn nested_fields_and_array_membership() {
    let td = match TestDb::provision_from_env().await {
        Some(v) => v,
        None => {
            eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL");
            return;
        }
    };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");
    store
        .ensure_collection("test", "docs")
        .await
        .expect("ensure");

    let docs = vec![
        doc! {"_id": ObjectId::new(), "a": {"b": 5}},
        doc! {"_id": ObjectId::new(), "a": {"b": 10}},
        doc! {"_id": ObjectId::new(), "a": {"b": [5,7]}},
        doc! {"_id": ObjectId::new(), "tags": ["x","y"]},
    ];
    for d in &docs {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        let b = bson::to_vec(d).unwrap();
        let j = serde_json::to_value(d).unwrap();
        store.insert_one("test", "docs", &id, &b, &j).await.unwrap();
    }

    // a.b == 5 should match first and third docs (array contains 5)
    let res = store
        .find_with_top_level_filter("test", "docs", &doc! {"a.b": 5}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 2);

    // a.b > 6 should match second and third (10 and [5,7] -> 7 matches any element > 6)
    let res = store
        .find_with_top_level_filter("test", "docs", &doc! {"a.b": {"$gt": 6}}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 2);

    // tags equality should match array membership
    let res = store
        .find_with_top_level_filter("test", "docs", &doc! {"tags": "x"}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // nested $in
    let res = store
        .find_with_top_level_filter("test", "docs", &doc! {"a.b": {"$in": [9, 10]}}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // nested $exists false
    let res = store
        .find_with_top_level_filter("test", "docs", &doc! {"a.c": {"$exists": false}}, 100)
        .await
        .unwrap();
    assert!(res.len() >= 1);
}
