mod common;
use bson::{doc, oid::ObjectId};
use common::postgres::TestDb;
use oxidedb::store::PgStore;

#[tokio::test]
async fn elem_match_scalar_and_subdoc() {
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
        .ensure_collection("test", "arrs")
        .await
        .expect("ensure");

    let docs = vec![
        doc! {"_id": ObjectId::new(), "arr": [1,5,7]},
        doc! {"_id": ObjectId::new(), "arr": [2,3]},
        doc! {"_id": ObjectId::new(), "arr": [{"x": 1}, {"x": 3}]},
        doc! {"_id": ObjectId::new(), "arr": [{"x": 2}]},
    ];
    for d in &docs {
        let id = d.get_object_id("_id").unwrap().bytes().to_vec();
        store
            .insert_one(
                "test",
                "arrs",
                &id,
                &bson::to_vec(d).unwrap(),
                &serde_json::to_value(d).unwrap(),
            )
            .await
            .unwrap();
    }

    // Scalar elemMatch
    let res = store
        .find_with_top_level_filter(
            "test",
            "arrs",
            &doc! {"arr": {"$elemMatch": {"$gt": 6}}},
            100,
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // Subdocument elemMatch: x > 2
    let res = store
        .find_with_top_level_filter(
            "test",
            "arrs",
            &doc! {"arr": {"$elemMatch": {"x": {"$gt": 2}}}},
            100,
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);

    // Subdocument elemMatch: x == 2
    let res = store
        .find_with_top_level_filter("test", "arrs", &doc! {"arr": {"$elemMatch": {"x": 2}}}, 100)
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
}
