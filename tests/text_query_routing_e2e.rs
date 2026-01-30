mod common;
use bson::{Document, doc};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn text_query_requires_text_index() {
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
        .ensure_collection("test", "articles")
        .await
        .expect("ensure_collection");

    // Insert test documents
    let docs: Vec<Document> = vec![
        doc! {
            "_id": "1",
            "title": "Rust Programming",
            "content": "Introduction to Rust"
        },
        doc! {
            "_id": "2",
            "title": "Python Programming",
            "content": "Introduction to Python"
        },
    ];

    for doc in &docs {
        let id = doc
            .get("_id")
            .unwrap()
            .as_str()
            .unwrap()
            .as_bytes()
            .to_vec();
        let bson_bytes = bson::to_vec(doc).unwrap();
        let json = serde_json::to_value(doc).unwrap();
        store
            .insert_one("test", "articles", &id, &bson_bytes, &json)
            .await
            .expect("insert should succeed");
    }

    // Try to query without a text index - should fail
    let result = store
        .get_text_index_fields("test", "articles")
        .await
        .expect("get_text_index_fields should not error");

    assert!(
        result.is_empty(),
        "Should return empty vec when no text index exists"
    );
}

#[tokio::test]
async fn cannot_create_multiple_text_indexes() {
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
        .ensure_collection("test", "articles")
        .await
        .expect("ensure_collection");

    // Create first text index
    let spec1 = serde_json::json!({"key": {"title": "text"}, "name": "title_text"});
    store
        .create_index_text(
            "test",
            "articles",
            "title_text",
            &["title".to_string()],
            "english",
            &spec1,
        )
        .await
        .expect("first text index should succeed");

    // Try to create second text index - should fail
    let spec2 = serde_json::json!({"key": {"content": "text"}, "name": "content_text"});
    let result = store
        .create_index_text(
            "test",
            "articles",
            "content_text",
            &["content".to_string()],
            "english",
            &spec2,
        )
        .await;

    assert!(result.is_err(), "Should fail to create second text index");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("cannot have more than one text index"),
        "Error should mention text index restriction: {}",
        err_msg
    );
}

#[tokio::test]
async fn text_query_uses_index_fields_not_all_fields() {
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
        .ensure_collection("test", "articles")
        .await
        .expect("ensure_collection");

    // Insert test documents
    let docs: Vec<Document> = vec![
        doc! {
            "_id": "1",
            "title": "Rust Programming",
            "content": "Python is great"  // 'Python' only in content
        },
        doc! {
            "_id": "2",
            "title": "Python Programming",  // 'Python' only in title
            "content": "Rust is great"
        },
    ];

    for doc in &docs {
        let id = doc
            .get("_id")
            .unwrap()
            .as_str()
            .unwrap()
            .as_bytes()
            .to_vec();
        let bson_bytes = bson::to_vec(doc).unwrap();
        let json = serde_json::to_value(doc).unwrap();
        store
            .insert_one("test", "articles", &id, &bson_bytes, &json)
            .await
            .expect("insert should succeed");
    }

    // Create text index ONLY on title field
    let _ = store.drop_index("test", "articles", "title_text").await;
    let spec = serde_json::json!({"key": {"title": "text"}, "name": "title_text"});
    store
        .create_index_text(
            "test",
            "articles",
            "title_text",
            &["title".to_string()],
            "english",
            &spec,
        )
        .await
        .expect("create_index_text should succeed");

    // Verify text index fields are correct
    let fields = store
        .get_text_index_fields("test", "articles")
        .await
        .expect("get_text_index_fields should succeed");

    assert_eq!(
        fields,
        vec!["title"],
        "Text index should only include 'title' field"
    );

    // Search for 'Python' - should only find document 2 (Python in title)
    // NOT document 1 (Python only in content, which is not indexed)
    let results = store
        .find_with_text_search(
            "test", "articles", "Python", "english", false, false, 10, &fields,
        )
        .await
        .expect("text search should succeed");

    assert_eq!(
        results.len(),
        1,
        "Should find exactly 1 document with 'Python' in title"
    );
    let found_id = results[0].get_str("_id").expect("should have _id");
    assert_eq!(
        found_id, "2",
        "Should find document 2 (Python in title), not document 1"
    );
}

#[tokio::test]
async fn text_query_with_additional_filters() {
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
        .ensure_collection("test", "articles")
        .await
        .expect("ensure_collection");

    // Insert test documents
    let docs: Vec<Document> = vec![
        doc! {
            "_id": "1",
            "title": "Rust Programming",
            "category": "programming"
        },
        doc! {
            "_id": "2",
            "title": "Rust for Beginners",
            "category": "tutorial"
        },
        doc! {
            "_id": "3",
            "title": "Python Programming",
            "category": "programming"
        },
    ];

    for doc in &docs {
        let id = doc
            .get("_id")
            .unwrap()
            .as_str()
            .unwrap()
            .as_bytes()
            .to_vec();
        let bson_bytes = bson::to_vec(doc).unwrap();
        let json = serde_json::to_value(doc).unwrap();
        store
            .insert_one("test", "articles", &id, &bson_bytes, &json)
            .await
            .expect("insert should succeed");
    }

    // Create text index on title
    let _ = store.drop_index("test", "articles", "title_text").await;
    let spec = serde_json::json!({"key": {"title": "text"}, "name": "title_text"});
    store
        .create_index_text(
            "test",
            "articles",
            "title_text",
            &["title".to_string()],
            "english",
            &spec,
        )
        .await
        .expect("create_index_text should succeed");

    // Get text index fields
    let fields = store
        .get_text_index_fields("test", "articles")
        .await
        .expect("get_text_index_fields should succeed");

    // Search for 'Rust' - should find both Rust documents
    let results = store
        .find_with_text_search(
            "test", "articles", "Rust", "english", false, false, 10, &fields,
        )
        .await
        .expect("text search should succeed");

    assert_eq!(results.len(), 2, "Should find 2 Rust documents");

    // Now simulate additional filter: category == "tutorial"
    // In real server.rs this would be done via document_matches_filter
    let filtered: Vec<_> = results
        .into_iter()
        .filter(|d| d.get_str("category").unwrap_or("") == "tutorial")
        .collect();

    assert_eq!(filtered.len(), 1, "Should find 1 Rust tutorial");
    assert_eq!(filtered[0].get_str("_id").unwrap(), "2");
}
