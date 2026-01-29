mod common;
use bson::{doc, oid::ObjectId};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn insert_and_find_round_trip_bson() {
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
        .ensure_collection("test", "users")
        .await
        .expect("ensure_collection");

    let oid = ObjectId::new();
    let doc = doc! { "_id": oid, "name": "Alice", "age": 30i32, "tags": ["a", "b"] };
    let id_bytes = oid.bytes().to_vec();
    let bson_bytes = bson::to_vec(&doc).unwrap();
    let json = serde_json::to_value(&doc).unwrap();

    let n = store
        .insert_one("test", "users", &id_bytes, &bson_bytes, &json)
        .await
        .expect("insert_one");
    assert_eq!(n, 1);

    // Find by id
    let found = store
        .find_by_id_docs("test", "users", &id_bytes, 10)
        .await
        .expect("find_by_id");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0], doc);

    // Find by subdoc equality
    let filter = serde_json::json!({"name": "Alice"});
    let found2 = store
        .find_by_subdoc("test", "users", &filter, 10)
        .await
        .expect("find_by_subdoc");
    assert!(found2.iter().any(|d| d == &doc));
}
