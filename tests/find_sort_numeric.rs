mod common;
use common::postgres::TestDb;
use oxidedb::store::PgStore;
use bson::{doc, oid::ObjectId};

#[tokio::test]
async fn numeric_sort_heuristic() {
    let td = match TestDb::provision_from_env().await { Some(v) => v, None => { eprintln!("skipping: set OXIDEDB_TEST_POSTGRES_URL"); return; } };
    let store = PgStore::connect(&td.url).await.expect("connect");
    store.bootstrap().await.expect("bootstrap");
    store.ensure_collection("test", "nums").await.expect("ensure");

    let docs = vec![
        doc!{"_id": ObjectId::new(), "n": 20, "s": "20"},
        doc!{"_id": ObjectId::new(), "n": 3, "s": "3"},
        doc!{"_id": ObjectId::new(), "n": 10, "s": "10"},
    ];
    for d in &docs { let id = d.get_object_id("_id").unwrap().bytes().to_vec(); store.insert_one("test","nums", &id, &bson::to_vec(d).unwrap(), &serde_json::to_value(d).unwrap()).await.unwrap(); }

    // Sort by n (numeric) asc
    let res = store.find_docs("test", "nums", None, Some(&doc!{"n": 1}), None, 10).await.unwrap();
    let ns: Vec<i32> = res.iter().map(|d| d.get_i32("n").unwrap()).collect();
    assert_eq!(ns, vec![3,10,20]);

    // Sort by s (string numbers) asc should still sort numerically first
    let res = store.find_docs("test", "nums", None, Some(&doc!{"s": 1}), None, 10).await.unwrap();
    let ss: Vec<String> = res.iter().map(|d| d.get_str("s").unwrap().to_string()).collect();
    assert_eq!(ss, vec!["3","10","20"]);
}

