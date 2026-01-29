mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn create_single_field_index_on_jsonb_field() {
    let td = match TestDb::provision_from_env().await {
        Some(v) => v,
        None => {
            eprintln!("skipping test: set OXIDEDB_TEST_POSTGRES_URL to run integration tests");
            return;
        }
    };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");

    // Ensure the collection exists
    store
        .ensure_collection("test", "users")
        .await
        .expect("ensure_collection");

    // Drop if exists to start clean
    let _ = store.drop_index("test", "users", "name_1").await;

    // Attempt to create the index on a jsonb field
    let spec = serde_json::json!({"key": {"name": 1}, "name": "name_1"});
    store
        .create_index_single_field("test", "users", "name_1", "name", 1, &spec)
        .await
        .expect("create_index_single_field should succeed");

    // Verify metadata recorded
    let names = store
        .list_index_names("test", "users")
        .await
        .expect("list index names");
    assert!(names.iter().any(|n| n == "name_1"));
}
