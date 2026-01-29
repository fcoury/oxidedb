// Driver compatibility test for Rust mongodb driver
// Tests basic operations against OxideDB

use mongodb::bson::{Document, doc};
use mongodb::{Client, options::ClientOptions};
use std::env;

const TEST_DB: &str = "driver_compat_test_rust";

#[derive(Debug, serde::Serialize)]
struct TestResult {
    name: String,
    status: String,
    error: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct Results {
    passed: i32,
    failed: i32,
    tests: Vec<TestResult>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let oxidedb_url =
        env::var("OXIDEDB_URL").unwrap_or_else(|_| "mongodb://localhost:27017".to_string());

    println!("Rust mongodb Driver Compatibility Test");
    println!("======================================");
    println!("Connecting to: {}", oxidedb_url);
    println!();

    let mut results = Results {
        passed: 0,
        failed: 0,
        tests: vec![],
    };

    let client_options = ClientOptions::parse(&oxidedb_url).await?;
    let client = Client::with_options(client_options)?;

    // Test connection
    client
        .database("admin")
        .run_command(doc! {"ping": 1})
        .await?;
    println!("✓ Connected successfully\n");

    let db = client.database(TEST_DB);
    let collection = db.collection::<Document>("test_collection");

    // Test 1: Ping
    match test_ping(&client).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "ping".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 1: ping");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "ping".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 1: ping - {}", e);
        }
    }

    // Test 2: Create collection
    match test_create_collection(&db, &collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "createCollection".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 2: createCollection");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "createCollection".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 2: createCollection - {}", e);
        }
    }

    // Test 3: Insert
    match test_insert(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "insert_one".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 3: insert_one");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "insert_one".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 3: insert_one - {}", e);
        }
    }

    // Test 4: Find
    match test_find(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "find_one".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 4: find_one");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "find_one".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 4: find_one - {}", e);
        }
    }

    // Test 5: Insert many
    match test_insert_many(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "insert_many".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 5: insert_many");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "insert_many".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 5: insert_many - {}", e);
        }
    }

    // Test 6: Find with cursor
    match test_find_with_cursor(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "find with cursor".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 6: find with cursor");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "find with cursor".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 6: find with cursor - {}", e);
        }
    }

    // Test 7: Update
    match test_update(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "update_one".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 7: update_one");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "update_one".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 7: update_one - {}", e);
        }
    }

    // Test 8: Delete
    match test_delete(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "delete_one".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 8: delete_one");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "delete_one".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 8: delete_one - {}", e);
        }
    }

    // Test 9: List collections
    match test_list_collections(&db).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "listCollections".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 9: listCollections");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "listCollections".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 9: listCollections - {}", e);
        }
    }

    // Test 10: Drop collection
    match test_drop(&collection).await {
        Ok(_) => {
            results.passed += 1;
            results.tests.push(TestResult {
                name: "drop".to_string(),
                status: "PASS".to_string(),
                error: None,
            });
            println!("✓ Test 10: drop");
        }
        Err(e) => {
            results.failed += 1;
            results.tests.push(TestResult {
                name: "drop".to_string(),
                status: "FAIL".to_string(),
                error: Some(e.to_string()),
            });
            println!("✗ Test 10: drop - {}", e);
        }
    }

    // Summary
    println!("\n======================================");
    println!("Summary:");
    println!("  Passed: {}", results.passed);
    println!("  Failed: {}", results.failed);
    println!("  Total:  {}", results.passed + results.failed);
    println!("======================================");

    // Output JSON for CI
    println!("\nJSON_OUTPUT:");
    println!("{}", serde_json::to_string_pretty(&results)?);

    if results.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

async fn test_ping(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client
        .database("admin")
        .run_command(doc! {"ping": 1})
        .await?;
    Ok(())
}

async fn test_create_collection(
    _db: &mongodb::Database,
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Drop if exists
    let _ = collection.drop().await;

    // Create collection by inserting a document
    collection.insert_one(doc! {"_init": true}).await?;
    Ok(())
}

async fn test_insert(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = collection
        .insert_one(doc! {"name": "Alice", "age": 30})
        .await?;
    if matches!(result.inserted_id, mongodb::bson::Bson::Null) {
        return Err("No inserted_id returned".into());
    }
    Ok(())
}

async fn test_find(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let doc = collection.find_one(doc! {"name": "Alice"}).await?;
    match doc {
        Some(d) => {
            if d.get_str("name")? != "Alice" || d.get_i32("age")? != 30 {
                return Err("Document not found or incorrect".into());
            }
        }
        None => return Err("Document not found".into()),
    }
    Ok(())
}

async fn test_insert_many(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let docs = vec![
        doc! {"name": "Bob", "age": 25},
        doc! {"name": "Charlie", "age": 35},
    ];
    let result = collection.insert_many(docs).await?;
    if result.inserted_ids.len() != 2 {
        return Err(format!(
            "Expected 2 inserted documents, got {}",
            result.inserted_ids.len()
        )
        .into());
    }
    Ok(())
}

async fn test_find_with_cursor(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    use mongodb::options::FindOptions;

    let options = FindOptions::builder().limit(3).build();
    let mut cursor = collection.find(doc! {}).with_options(options).await?;

    let mut count = 0;
    while cursor.advance().await? {
        count += 1;
    }

    if count != 3 {
        return Err(format!("Expected 3 documents, got {}", count).into());
    }
    Ok(())
}

async fn test_update(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = collection
        .update_one(doc! {"name": "Alice"}, doc! {"$set": {"age": 31}})
        .await?;

    if result.modified_count != 1 {
        return Err(format!(
            "Expected 1 modified document, got {}",
            result.modified_count
        )
        .into());
    }
    Ok(())
}

async fn test_delete(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = collection.delete_one(doc! {"name": "Bob"}).await?;

    if result.deleted_count != 1 {
        return Err(format!("Expected 1 deleted document, got {}", result.deleted_count).into());
    }
    Ok(())
}

async fn test_list_collections(db: &mongodb::Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut cursor = db.list_collections().await?;

    let mut found = false;
    while cursor.advance().await? {
        let spec = cursor.deserialize_current()?;
        if spec.name == "test_collection" {
            found = true;
            break;
        }
    }

    if !found {
        return Err("test_collection not found".into());
    }
    Ok(())
}

async fn test_drop(
    collection: &mongodb::Collection<Document>,
) -> Result<(), Box<dyn std::error::Error>> {
    collection.drop().await?;
    Ok(())
}
