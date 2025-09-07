use crate::error::{Error, Result};
use tokio_postgres::{Client, NoTls};

pub struct PgStore {
    client: Client,
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await.map_err(|e| Error::Msg(e.to_string()))?;
        // Spawn the connection driver task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres connection error");
            }
        });
        Ok(Self { client })
    }

    pub async fn bootstrap(&self) -> Result<()> {
        // Create metadata schema and tables
        self.client
            .batch_execute(
                r#"
                CREATE SCHEMA IF NOT EXISTS mdb_meta;
                CREATE TABLE IF NOT EXISTS mdb_meta.databases (
                    db TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS mdb_meta.collections (
                    db TEXT NOT NULL,
                    coll TEXT NOT NULL,
                    PRIMARY KEY (db, coll)
                );
                CREATE TABLE IF NOT EXISTS mdb_meta.indexes (
                    db TEXT NOT NULL,
                    coll TEXT NOT NULL,
                    name TEXT NOT NULL,
                    spec JSONB NOT NULL,
                    sql TEXT,
                    PRIMARY KEY (db, coll, name)
                );
                "#,
            )
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;
        Ok(())
    }

    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let rows = self
            .client
            .query("SELECT db FROM mdb_meta.databases ORDER BY db", &[])
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    pub async fn list_collections(&self, db: &str) -> Result<Vec<String>> {
        let rows = self
            .client
            .query("SELECT coll FROM mdb_meta.collections WHERE db = $1 ORDER BY coll", &[&db])
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    pub async fn ensure_database(&self, db: &str) -> Result<()> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let ddl = format!("CREATE SCHEMA IF NOT EXISTS {}", q_schema);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        self.client
            .execute(
                "INSERT INTO mdb_meta.databases(db) VALUES($1) ON CONFLICT (db) DO NOTHING",
                &[&db],
            )
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn ensure_collection(&self, db: &str, coll: &str) -> Result<()> {
        self.ensure_database(db).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let idx_name = format!("idx_{}_doc_gin", coll);
        let q_idx_name = q_ident(&idx_name);
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (id bytea PRIMARY KEY, doc jsonb NOT NULL, doc_bson bytea NOT NULL);\nCREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIN (doc jsonb_path_ops)",
            q_schema, q_table, q_idx_name, q_schema, q_table
        );
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        self.client
            .execute(
                "INSERT INTO mdb_meta.collections(db, coll) VALUES($1,$2) ON CONFLICT (db, coll) DO NOTHING",
                &[&db, &coll],
            )
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn drop_collection(&self, db: &str, coll: &str) -> Result<()> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let ddl = format!("DROP TABLE IF EXISTS {}.{}", q_schema, q_table);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        self.client
            .execute(
                "DELETE FROM mdb_meta.collections WHERE db = $1 AND coll = $2",
                &[&db, &coll],
            )
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn drop_database(&self, db: &str) -> Result<()> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let ddl = format!("DROP SCHEMA IF EXISTS {} CASCADE", q_schema);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        self.client
            .execute("DELETE FROM mdb_meta.collections WHERE db = $1", &[&db])
            .await
            .map_err(err_msg)?;
        self.client
            .execute("DELETE FROM mdb_meta.databases WHERE db = $1", &[&db])
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn insert_one(&self, db: &str, coll: &str, id: &[u8], bson_bytes: &[u8], json: &serde_json::Value) -> Result<u64> {
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "INSERT INTO {}.{} (id, doc_bson, doc) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            q_schema, q_table
        );
        let n = self
            .client
            .execute(&sql, &[&id, &bson_bytes, &json])
            .await
            .map_err(err_msg)?;
        Ok(n)
    }

    pub async fn find_simple_docs(&self, db: &str, coll: &str, limit: i64) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!("SELECT doc_bson, doc FROM {}.{} ORDER BY id ASC LIMIT $1", q_schema, q_table);
        let rows = self.client.query(&sql, &[&limit]).await.map_err(err_msg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    out.push(doc);
                    continue;
                }
            }
            let json: serde_json::Value = r.get(1);
            let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
            let doc = match b { bson::Bson::Document(d) => d, _ => bson::Document::new() };
            out.push(doc);
        }
        Ok(out)
    }

    pub async fn find_by_id_docs(&self, db: &str, coll: &str, id: &[u8], limit: i64) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!("SELECT doc_bson, doc FROM {}.{} WHERE id = $1 LIMIT $2", q_schema, q_table);
        let rows = self.client.query(&sql, &[&id, &limit]).await.map_err(err_msg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    out.push(doc);
                    continue;
                }
            }
            let json: serde_json::Value = r.get(1);
            let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
            let doc = match b { bson::Bson::Document(d) => d, _ => bson::Document::new() };
            out.push(doc);
        }
        Ok(out)
    }
}

fn schema_name(db: &str) -> String {
    format!("mdb_{}", db)
}

fn q_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn q_string(s: &str) -> String {
    s.to_string()
}

fn err_msg<E: std::fmt::Display>(e: E) -> Error {
    Error::Msg(e.to_string())
}
