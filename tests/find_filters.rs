mod common;
use bson::{doc, oid::ObjectId};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn filter_gt_in_exists_top_level() {
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
        .ensure_collection("test", "nums")
        .await
        .expect("ensure");

    // Insert docs: ages 20, 30, missing age
    let docs = vec![
        doc! {"_id": ObjectId::new(), "age": 20i32, "name": "a"},
        doc! {"_id": ObjectId::new(), "age": 30i32, "name": "b"},
        doc! {"_id": ObjectId::new(), "name": "noage"},
    ];
    for d in docs.iter() {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        let b = bson::to_vec(d).unwrap();
        let j = serde_json::to_value(d).unwrap();
        store.insert_one("test", "nums", &id, &b, &j).await.unwrap();
    }

    // age > 25
    let filter = doc! {"age": {"$gt": 25}};
    let res = store
        .find_with_top_level_filter("test", "nums", &filter, 100)
        .await
        .unwrap();
    assert!(res.iter().all(|d| d.get_i32("age").unwrap_or(0) > 25));

    // name in ["a","c"]
    let filter = doc! {"name": {"$in": ["a", "c"]}};
    let res = store
        .find_with_top_level_filter("test", "nums", &filter, 100)
        .await
        .unwrap();
    assert!(
        res.iter()
            .all(|d| d.get_str("name").map(|s| s == "a").unwrap_or(false))
    );

    // age exists false
    let filter = doc! {"age": {"$exists": false}};
    let res = store
        .find_with_top_level_filter("test", "nums", &filter, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get_str("name").unwrap(), "noage");
}

#[tokio::test]
async fn filter_strings_with_special_chars() {
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
        .ensure_collection("test", "names")
        .await
        .expect("ensure");

    let docs = vec![
        doc! {"_id": ObjectId::new(), "name": "O'Brien"},
        doc! {"_id": ObjectId::new(), "name": "say \"hi\""},
        doc! {"_id": ObjectId::new(), "name": "back\\slash"},
        doc! {"_id": ObjectId::new(), "name": "plain", "a'b": 5i32},
        doc! {"_id": ObjectId::new(), "name": "bs", "a\\b": 7i32},
    ];
    for d in docs.iter() {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        let b = bson::to_vec(d).unwrap();
        let j = serde_json::to_value(d).unwrap();
        store
            .insert_one("test", "names", &id, &b, &j)
            .await
            .unwrap();
    }

    // equality on a value containing an apostrophe
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"name": "O'Brien"}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get_str("name").unwrap(), "O'Brien");

    // $in with an apostrophe value
    let res = store
        .find_with_top_level_filter(
            "test",
            "names",
            &doc! {"name": {"$in": ["O'Brien", "nobody"]}},
            100,
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // equality on a value containing double quotes
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"name": "say \"hi\""}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // equality on a value containing a backslash
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"name": "back\\slash"}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // field name containing an apostrophe
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"a'b": 5}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get_str("name").unwrap(), "plain");

    // field name containing a backslash
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"a\\b": 7}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get_str("name").unwrap(), "bs");

    // $regex with an apostrophe in the pattern
    let res = store
        .find_with_top_level_filter("test", "names", &doc! {"name": {"$regex": "O'Bri"}}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
}
