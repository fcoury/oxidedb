use crate::aggregation::exec::WriteStats;
use crate::store::PgStore;
use bson::Document;

pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    target_coll: &str,
) -> anyhow::Result<WriteStats> {
    let mut stats = WriteStats::default();

    // Generate a temporary collection name
    let temp_coll = format!(
        "{}_temp_{}",
        target_coll,
        uuid::Uuid::new_v4().to_string().replace("-", "")
    );

    // Insert all documents into the temporary collection
    for doc in docs {
        // Serialize document
        let id = doc
            .get("_id")
            .and_then(|id| bson::to_vec(id).ok())
            .unwrap_or_else(|| bson::to_vec(&bson::oid::ObjectId::new()).unwrap());
        let bson_bytes = bson::to_vec(&doc)?;
        // Convert bson Document to serde_json::Value
        let json = serde_json::to_value(&doc)?;

        pg.insert_one(db, &temp_coll, &id, &bson_bytes, &json)
            .await?;
        stats.inserted_count += 1;
    }

    // Atomically replace the target collection with the temp collection
    // This is done by dropping the target and renaming the temp
    pg.drop_collection(db, target_coll).await?;

    // Rename temp collection to target using raw SQL
    let schema = format!("mdb_{}", db);
    let rename_sql = format!(
        "ALTER TABLE IF EXISTS {}.{} RENAME TO {}",
        schema, temp_coll, target_coll
    );

    let client = pg.pool().get().await.map_err(|e| anyhow::anyhow!(e))?;
    client
        .batch_execute(&rename_sql)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    // Update metadata
    client
        .execute(
            "DELETE FROM mdb_meta.collections WHERE db = $1 AND coll = $2",
            &[&db, &temp_coll],
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    client.execute(
        "INSERT INTO mdb_meta.collections(db, coll) VALUES($1, $2) ON CONFLICT (db, coll) DO NOTHING",
        &[&db, &target_coll],
    ).await.map_err(|e| anyhow::anyhow!(e))?;

    Ok(stats)
}
