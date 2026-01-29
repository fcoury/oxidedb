mod common;
use bson::{doc, oid::ObjectId};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn nested_projection_include_and_exclude() {
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

    let d = doc! {"_id": ObjectId::new(), "a": {"b": 5, "c": 7}, "name": "x"};
    let id = d.get_object_id("_id").unwrap().bytes().to_vec();
    store
        .insert_one(
            "test",
            "docs",
            &id,
            &bson::to_vec(&d).unwrap(),
            &serde_json::to_value(&d).unwrap(),
        )
        .await
        .unwrap();

    // Include nested a.b and name, exclude _id
    let res = store
        .find_docs(
            "test",
            "docs",
            None,
            None,
            Some(&doc! {"a.b": 1, "name": 1, "_id": 0}),
            10,
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    let r = &res[0];
    assert_eq!(r.get_document("a").unwrap().get_i32("b").unwrap(), 5);
    assert_eq!(r.get_str("name").unwrap(), "x");
    assert!(!r.contains_key("_id"));

    // Exclude nested a.c only
    let res = store
        .find_docs("test", "docs", None, None, Some(&doc! {"a.c": 0}), 10)
        .await
        .unwrap();
    let r = &res[0];
    assert!(r.get_document("a").unwrap().get("c").is_none());
    assert!(r.get_document("a").unwrap().get("b").is_some());
}
