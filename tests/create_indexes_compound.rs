mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn create_compound_index() {
    let td = match TestDb::provision_from_env().await {
        Some(v) => v,
        None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; }
    };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");
    store.ensure_collection("test", "users").await.expect("ensure_collection");

    let _ = store.drop_index("test", "users", "name_age_1_-1").await;

    let fields = vec![("name".to_string(), 1), ("age".to_string(), -1)];
    let spec = serde_json::json!({"key": {"name": 1, "age": -1}, "name": "name_age_1_-1"});
    store
        .create_index_compound("test", "users", "name_age_1_-1", &fields, &spec)
        .await
        .expect("create_index_compound");

    let names = store.list_index_names("test", "users").await.expect("list names");
    assert!(names.iter().any(|n| n == "name_age_1_-1"));
}

