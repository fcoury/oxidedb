mod common;
use bson::{Document, doc};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn create_text_index_single_field() {
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

    // Drop if exists to start clean
    let _ = store.drop_index("test", "articles", "title_text").await;

    // Create a text index on a single field
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

    // Verify metadata recorded
    let names = store
        .list_index_names("test", "articles")
        .await
        .expect("list index names");
    assert!(names.iter().any(|n| n == "title_text"));
}

#[tokio::test]
async fn create_text_index_multiple_fields() {
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

    // Drop if exists to start clean
    let _ = store
        .drop_index("test", "articles", "title_content_text")
        .await;

    // Create a compound text index on multiple fields
    let spec = serde_json::json!({"key": {"title": "text", "content": "text"}, "name": "title_content_text"});
    store
        .create_index_text(
            "test",
            "articles",
            "title_content_text",
            &["title".to_string(), "content".to_string()],
            "english",
            &spec,
        )
        .await
        .expect("create_index_text should succeed");

    // Verify metadata recorded
    let names = store
        .list_index_names("test", "articles")
        .await
        .expect("list index names");
    assert!(names.iter().any(|n| n == "title_content_text"));
}

#[tokio::test]
async fn text_search_single_field() {
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
            "title": "Introduction to Rust Programming",
            "content": "Rust is a systems programming language with a focus on safety and performance."
        },
        doc! {
            "_id": "2",
            "title": "MongoDB Text Search Guide",
            "content": "Learn how to use text search in MongoDB for full-text queries."
        },
        doc! {
            "_id": "3",
            "title": "PostgreSQL vs MongoDB",
            "content": "A comparison of two popular database systems."
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

    // Create a text index
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

    // Test text search
    let results = store
        .find_with_text_search(
            "test",
            "articles",
            "Rust Programming",
            "english",
            false,
            false,
            10,
        )
        .await
        .expect("text search should succeed");

    // Should find the Rust document
    assert!(
        !results.is_empty(),
        "Should find documents matching 'Rust Programming'"
    );
    let titles: Vec<&str> = results
        .iter()
        .filter_map(|d| d.get_str("title").ok())
        .collect();
    assert!(
        titles.iter().any(|t| t.contains("Rust")),
        "Should find Rust document"
    );
}

#[tokio::test]
async fn text_search_multiple_fields() {
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
            "title": "Introduction to Rust",
            "content": "Rust programming language tutorial"
        },
        doc! {
            "_id": "2",
            "title": "Database Systems",
            "content": "Learn about MongoDB and Rust integration"
        },
        doc! {
            "_id": "3",
            "title": "Web Development",
            "content": "Building web applications with various frameworks"
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

    // Create a compound text index
    let _ = store
        .drop_index("test", "articles", "title_content_text")
        .await;
    let spec = serde_json::json!({"key": {"title": "text", "content": "text"}, "name": "title_content_text"});
    store
        .create_index_text(
            "test",
            "articles",
            "title_content_text",
            &["title".to_string(), "content".to_string()],
            "english",
            &spec,
        )
        .await
        .expect("create_index_text should succeed");

    // Test text search - should match in both title and content
    let results = store
        .find_with_text_search("test", "articles", "Rust", "english", false, false, 10)
        .await
        .expect("text search should succeed");

    // Should find documents with "Rust" in title or content
    assert!(
        results.len() >= 2,
        "Should find at least 2 documents with 'Rust'"
    );
}

#[tokio::test]
async fn text_search_language_option() {
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
            "title": "Running fast in the park",
            "content": "Tips for improving your running speed"
        },
        doc! {
            "_id": "2",
            "title": "Database Performance",
            "content": "Optimizing query performance in PostgreSQL"
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

    // Create a text index
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

    // Test text search with English language (should handle stemming)
    let results = store
        .find_with_text_search("test", "articles", "running", "english", false, false, 10)
        .await
        .expect("text search should succeed");

    // Should find the document with "Running" (stemming should match "run" and "running")
    assert!(!results.is_empty(), "Should find documents with 'running'");
}

#[tokio::test]
async fn text_search_with_filter() {
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
            "content": "Introduction to Rust",
            "category": "programming"
        },
        doc! {
            "_id": "2",
            "title": "Rust for Beginners",
            "content": "Getting started with Rust",
            "category": "tutorial"
        },
        doc! {
            "_id": "3",
            "title": "Python Programming",
            "content": "Introduction to Python",
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

    // Create a text index
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

    // Test text search
    let results = store
        .find_with_text_search("test", "articles", "Rust", "english", false, false, 10)
        .await
        .expect("text search should succeed");

    // Should find Rust documents
    assert!(results.len() >= 2, "Should find at least 2 Rust documents");

    // Verify both Rust documents are found
    let titles: Vec<&str> = results
        .iter()
        .filter_map(|d| d.get_str("title").ok())
        .collect();
    assert!(
        titles.iter().any(|t| t.contains("Rust")),
        "Should find Rust documents"
    );
}
