mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn create_drop_collection_and_database_updates_metadata() {
    let td = match TestDb::provision_from_env().await {
        Some(v) => v,
        None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; }
    };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");

    // Initially, metadata should be empty
    let dbs = store.list_databases().await.expect("list dbs");
    assert!(dbs.is_empty());

    // Create collection and verify metadata
    store.ensure_collection("testdb", "c1").await.expect("ensure_collection");
    let dbs = store.list_databases().await.expect("list dbs");
    assert!(dbs.iter().any(|d| d == "testdb"));
    let cols = store.list_collections("testdb").await.expect("list colls");
    assert!(cols.iter().any(|c| c == "c1"));

    // Drop collection and verify metadata updated
    store.drop_collection("testdb", "c1").await.expect("drop coll");
    let cols = store.list_collections("testdb").await.expect("list colls");
    assert!(!cols.iter().any(|c| c == "c1"));

    // Drop database and verify metadata updated
    store.drop_database("testdb").await.expect("drop db");
    let dbs = store.list_databases().await.expect("list dbs");
    assert!(!dbs.iter().any(|d| d == "testdb"));
}

