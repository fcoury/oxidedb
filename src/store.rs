use crate::error::{Error, Result};
use crate::translate::translate_expression;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio_postgres::{NoTls, Transaction};

pub struct PgStore {
    pool: Pool,
    dsn: String,
    databases_cache: RwLock<HashSet<String>>, // known databases
    collections_cache: RwLock<HashSet<(String, String)>>, // known (db, coll)
}

impl PgStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let pgcfg = tokio_postgres::Config::from_str(url).map_err(err_msg)?;
        let mgr = Manager::from_config(
            pgcfg,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr).max_size(16).build().map_err(err_msg)?;
        Ok(Self {
            pool,
            dsn: url.to_string(),
            databases_cache: RwLock::new(HashSet::new()),
            collections_cache: RwLock::new(HashSet::new()),
        })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub async fn bootstrap(&self) -> Result<()> {
        // Create metadata schema and tables
        let client = self.pool.get().await.map_err(err_msg)?;
        client
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
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client
            .query("SELECT db FROM mdb_meta.databases ORDER BY db", &[])
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    pub async fn list_collections(&self, db: &str) -> Result<Vec<String>> {
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client
            .query(
                "SELECT coll FROM mdb_meta.collections WHERE db = $1 ORDER BY coll",
                &[&db],
            )
            .await
            .map_err(|e| Error::Msg(e.to_string()))?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    pub async fn ensure_database(&self, db: &str) -> Result<()> {
        // Fast path: cache
        if self.is_known_db(db).await {
            return Ok(());
        }
        let t = Instant::now();
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let ddl = format!("CREATE SCHEMA IF NOT EXISTS {}", q_schema);
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        client
            .execute(
                "INSERT INTO mdb_meta.databases(db) VALUES($1) ON CONFLICT (db) DO NOTHING",
                &[&db],
            )
            .await
            .map_err(err_msg)?;
        self.mark_db_known(db).await;
        tracing::debug!(op="ensure_database", db=%db, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    pub async fn ensure_collection(&self, db: &str, coll: &str) -> Result<()> {
        // Fast path: cache
        if self.is_known_collection(db, coll).await {
            return Ok(());
        }
        self.ensure_database(db).await?;
        let t = Instant::now();
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let idx_name = format!("idx_{}_doc_gin", coll);
        let q_idx_name = q_ident(&idx_name);
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (id bytea PRIMARY KEY, doc jsonb NOT NULL, doc_bson bytea NOT NULL);\nCREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIN (doc jsonb_path_ops)",
            q_schema, q_table, q_idx_name, q_schema, q_table
        );
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        client
            .execute(
                "INSERT INTO mdb_meta.collections(db, coll) VALUES($1,$2) ON CONFLICT (db, coll) DO NOTHING",
                &[&db, &coll],
            )
            .await
            .map_err(err_msg)?;
        self.mark_collection_known(db, coll).await;
        tracing::debug!(op="ensure_collection", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    pub async fn drop_collection(&self, db: &str, coll: &str) -> Result<()> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let ddl = format!("DROP TABLE IF EXISTS {}.{}", q_schema, q_table);
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        client
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
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        client
            .execute("DELETE FROM mdb_meta.collections WHERE db = $1", &[&db])
            .await
            .map_err(err_msg)?;
        client
            .execute("DELETE FROM mdb_meta.databases WHERE db = $1", &[&db])
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn insert_one(
        &self,
        db: &str,
        coll: &str,
        id: &[u8],
        bson_bytes: &[u8],
        json: &serde_json::Value,
    ) -> Result<u64> {
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "INSERT INTO {}.{} (id, doc_bson, doc) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            q_schema, q_table
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let n = client
            .execute(&sql, &[&id, &bson_bytes, &json])
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="insert_one", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Insert with a specific client (for transaction support)
    pub async fn insert_one_with_client(
        &self,
        client: &tokio_postgres::Client,
        db: &str,
        coll: &str,
        id: &[u8],
        bson_bytes: &[u8],
        json: &serde_json::Value,
    ) -> Result<u64> {
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "INSERT INTO {}.{} (id, doc_bson, doc) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            q_schema, q_table
        );
        let t = Instant::now();
        let n = client
            .execute(&sql, &[&id, &bson_bytes, &json])
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="insert_one_with_client", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    pub async fn find_simple_docs(
        &self,
        db: &str,
        coll: &str,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} ORDER BY id ASC LIMIT $1",
            q_schema, q_table
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = match client.query(&sql, &[&limit]).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    return Ok(Vec::new());
                }
                return Err(err_msg(e));
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(doc);
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let doc = to_doc_from_json(json);
            out.push(doc);
        }
        tracing::debug!(op="find_simple_docs", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }

    pub async fn find_by_id_docs(
        &self,
        db: &str,
        coll: &str,
        id: &[u8],
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE id = $1 LIMIT $2",
            q_schema, q_table
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = match client.query(&sql, &[&id, &limit]).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    return Ok(Vec::new());
                }
                return Err(err_msg(e));
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(doc);
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let doc = to_doc_from_json(json);
            out.push(doc);
        }
        tracing::debug!(op="find_by_id_docs", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }

    /// Find by ID with a specific client (for transaction support)
    pub async fn find_by_id_docs_with_client(
        &self,
        client: &tokio_postgres::Client,
        db: &str,
        coll: &str,
        id: &[u8],
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE id = $1 LIMIT $2",
            q_schema, q_table
        );
        let t = Instant::now();
        let rows = match client.query(&sql, &[&id, &limit]).await {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    return Ok(Vec::new());
                }
                return Err(err_msg(e));
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(doc);
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let doc = to_doc_from_json(json);
            out.push(doc);
        }
        tracing::debug!(op="find_by_id_docs_with_client", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }

    /// Limited filter support: equality, $gt, $gte, $lt, $lte, $in, $exists (true/false), $elemMatch
    /// Supports dotted field paths and array membership for equality and comparisons.
    pub async fn find_with_top_level_filter(
        &self,
        db: &str,
        coll: &str,
        filter: &bson::Document,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);

        let mut where_clauses: Vec<String> = Vec::new();

        for (k, v) in filter.iter() {
            // skip _id here; server handles id fast path
            if k == "_id" {
                continue;
            }
            let path = jsonpath_path(k);
            match v {
                bson::Bson::Document(d) => {
                    for (op, val) in d.iter() {
                        match op.as_str() {
                            "$elemMatch" => {
                                if let bson::Bson::Document(em) = val
                                    && let Some(pred) = build_elem_match_pred(&path, em)
                                {
                                    let jsonpath = format!("{}[*] ? ({})", path, pred);
                                    where_clauses.push(format!(
                                        "jsonb_path_exists(doc, '{}')",
                                        escape_single(&jsonpath)
                                    ));
                                }
                            }
                            "$exists" => {
                                let exists = matches!(val, bson::Bson::Boolean(true));
                                let clause = if exists {
                                    format!("jsonb_path_exists(doc, '{}')", escape_single(&path))
                                } else {
                                    format!(
                                        "NOT jsonb_path_exists(doc, '{}')",
                                        escape_single(&path)
                                    )
                                };
                                where_clauses.push(clause);
                            }
                            "$in" => {
                                if let bson::Bson::Array(arr) = val {
                                    let mut preds: Vec<String> = Vec::new();
                                    for item in arr {
                                        if let Some(lit) = json_literal_from_bson(item) {
                                            preds.push(format!("@ == {}", lit));
                                        }
                                    }
                                    if preds.is_empty() {
                                        where_clauses.push("FALSE".to_string());
                                    } else {
                                        let predicate = preds.join(" || ");
                                        let p1 = format!(
                                            "jsonb_path_exists(doc, '{} ? ({} )')",
                                            escape_single(&path),
                                            predicate
                                        );
                                        let p2 = format!(
                                            "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                            escape_single(&path),
                                            predicate
                                        );
                                        where_clauses.push(format!("({} OR {})", p1, p2));
                                    }
                                }
                            }
                            "$gt" | "$gte" | "$lt" | "$lte" => {
                                let op_sql = match op.as_str() {
                                    "$gt" => ">",
                                    "$gte" => ">=",
                                    "$lt" => "<",
                                    "$lte" => "<=",
                                    _ => unreachable!(),
                                };
                                if let Some(lit) = json_literal_from_bson(val) {
                                    let p1 = format!(
                                        "jsonb_path_exists(doc, '{} ? (@ {} {} )')",
                                        escape_single(&path),
                                        op_sql,
                                        lit
                                    );
                                    let p2 = format!(
                                        "jsonb_path_exists(doc, '{}[*] ? (@ {} {} )')",
                                        escape_single(&path),
                                        op_sql,
                                        lit
                                    );
                                    where_clauses.push(format!("({} OR {})", p1, p2));
                                }
                            }
                            "$eq" => {
                                if let Some(lit) = json_literal_from_bson(val) {
                                    let p1 = format!(
                                        "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                                        escape_single(&path),
                                        lit
                                    );
                                    let p2 = format!(
                                        "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                                        escape_single(&path),
                                        lit
                                    );
                                    where_clauses.push(format!("({} OR {})", p1, p2));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => {
                    if let Some(lit) = json_literal_from_bson(v) {
                        let p1 = format!(
                            "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                            escape_single(&path),
                            lit
                        );
                        let p2 = format!(
                            "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                            escape_single(&path),
                            lit
                        );
                        where_clauses.push(format!("({} OR {})", p1, p2));
                    }
                }
            }
        }

        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let where_sql = build_where_from_filter(filter);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE {} ORDER BY id ASC LIMIT {}",
            q_schema, q_table, where_sql, limit
        );
        let rows = client.query(&sql, &[]).await.map_err(err_msg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(doc);
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let doc = to_doc_from_json(json);
            out.push(doc);
        }
        tracing::debug!(op="find_with_top_level_filter", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }

    /// General find with optional sort and projection pushdown (simple top-level includes).
    pub async fn find_docs(
        &self,
        db: &str,
        coll: &str,
        filter: Option<&bson::Document>,
        sort: Option<&bson::Document>,
        projection: Option<&bson::Document>,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        // Check for $text operator - should be handled by server layer
        if let Some(f) = filter
            && f.contains_key("$text")
        {
            return Err(Error::Msg(
                "$text must be handled by the server layer, not find_docs".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = filter.map(build_where_from_filter);
        let order_sql = build_order_by(sort);

        if let Some(proj_sql) = projection_pushdown_sql(projection) {
            let t = Instant::now();
            let client = self.pool.get().await.map_err(err_msg)?;
            let res = match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE {} {} LIMIT {}",
                        proj_sql, q_schema, q_table, where_clause, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        proj_sql, q_schema, q_table, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
            };
            let rows = match res {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    // If the schema or table doesn't exist, emulate Mongo and return empty
                    if msg.contains("does not exist") {
                        return Ok(Vec::new());
                    }
                    return Err(err_msg(e));
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let json: serde_json::Value = r.get(0);
                out.push(to_doc_from_json(json));
            }
            tracing::debug!(op="find_docs_pushdown", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
            Ok(out)
        } else {
            let t = Instant::now();
            let client = self.pool.get().await.map_err(err_msg)?;
            let res = match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                        q_schema, q_table, where_clause, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        q_schema, q_table, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
            };
            let rows = match res {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("does not exist") {
                        return Ok(Vec::new());
                    }
                    return Err(err_msg(e));
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
                if let Some(bytes) = bson_bytes
                    && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
                {
                    out.push(if let Some(p) = projection {
                        project_document(&doc, p)
                    } else {
                        doc
                    });
                    continue;
                }
                let json: serde_json::Value = r.get(1);
                let d = to_doc_from_json(json);
                out.push(if let Some(p) = projection {
                    project_document(&d, p)
                } else {
                    d
                });
            }
            tracing::debug!(op="find_docs", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
            Ok(out)
        }
    }

    /// Find with a specific client (for transaction support)
    #[allow(clippy::too_many_arguments)]
    pub async fn find_docs_with_client(
        &self,
        client: &tokio_postgres::Client,
        db: &str,
        coll: &str,
        filter: Option<&bson::Document>,
        sort: Option<&bson::Document>,
        projection: Option<&bson::Document>,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = filter.map(build_where_from_filter);
        let order_sql = build_order_by(sort);

        if let Some(proj_sql) = projection_pushdown_sql(projection) {
            let t = Instant::now();
            let res = match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE {} {} LIMIT {}",
                        proj_sql, q_schema, q_table, where_clause, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        proj_sql, q_schema, q_table, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
            };
            let rows = match res {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("does not exist") {
                        return Ok(Vec::new());
                    }
                    return Err(err_msg(e));
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let json: serde_json::Value = r.get(0);
                out.push(to_doc_from_json(json));
            }
            tracing::debug!(op="find_docs_with_client_pushdown", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
            Ok(out)
        } else {
            let t = Instant::now();
            let res = match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                        q_schema, q_table, where_clause, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        q_schema, q_table, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
            };
            let rows = match res {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("does not exist") {
                        return Ok(Vec::new());
                    }
                    return Err(err_msg(e));
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
                if let Some(bytes) = bson_bytes
                    && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
                {
                    out.push(if let Some(p) = projection {
                        project_document(&doc, p)
                    } else {
                        doc
                    });
                    continue;
                }
                let json: serde_json::Value = r.get(1);
                let d = to_doc_from_json(json);
                out.push(if let Some(p) = projection {
                    project_document(&d, p)
                } else {
                    d
                });
            }
            tracing::debug!(op="find_docs_with_client", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
            Ok(out)
        }
    }

    pub async fn find_by_subdoc(
        &self,
        db: &str,
        coll: &str,
        subdoc: &serde_json::Value,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE doc @> $1::jsonb ORDER BY id ASC LIMIT $2",
            q_schema, q_table
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client
            .query(&sql, &[&subdoc, &limit])
            .await
            .map_err(err_msg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(doc);
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
            let doc = match b {
                bson::Bson::Document(d) => d,
                _ => bson::Document::new(),
            };
            out.push(doc);
        }
        tracing::debug!(op="find_by_subdoc", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }

    pub async fn create_index_single_field(
        &self,
        db: &str,
        coll: &str,
        name: &str,
        field: &str,
        _order: i32,
        spec: &serde_json::Value,
    ) -> Result<()> {
        // Ensure collection (schema/table) exists
        self.ensure_collection(db, coll).await?;
        // Create an expression index on the extracted text value
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let q_idx = q_ident(name);
        let field_escaped = field.replace('"', "\"\"");
        // Expression index requires parentheses around the expression inside the list parentheses
        // e.g., USING btree ((doc->>'field'))
        let expr = format!("((doc->>'{}'))", field_escaped);
        let t = Instant::now();
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING btree {}",
            q_idx, q_schema, q_table, expr
        );
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        // Persist metadata
        client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="create_index_single", db=%db, coll=%coll, name=%name, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    pub async fn drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_idx = q_ident(name);
        let ddl = format!("DROP INDEX IF EXISTS {}.{}", q_schema, q_idx);
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        let n = client
            .execute(
                "DELETE FROM mdb_meta.indexes WHERE db=$1 AND coll=$2 AND name=$3",
                &[&db, &coll, &name],
            )
            .await
            .map_err(err_msg)?;
        Ok(n > 0)
    }

    pub async fn list_index_names(&self, db: &str, coll: &str) -> Result<Vec<String>> {
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client
            .query(
                "SELECT name FROM mdb_meta.indexes WHERE db=$1 AND coll=$2",
                &[&db, &coll],
            )
            .await
            .map_err(err_msg)?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    /// Get the fields from the text index for a collection.
    /// Returns empty Vec if no text index exists.
    /// Returns error if multiple text indexes exist (shouldn't happen with uniqueness enforcement).
    pub async fn get_text_index_fields(&self, db: &str, coll: &str) -> Result<Vec<String>> {
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client
            .query(
                "SELECT spec FROM mdb_meta.indexes WHERE db=$1 AND coll=$2",
                &[&db, &coll],
            )
            .await
            .map_err(err_msg)?;

        let mut text_indexes_found = 0;
        let mut text_fields = Vec::new();

        for row in rows {
            let spec_json: serde_json::Value = row.get(0);
            if let Some(key) = spec_json.get("key").and_then(|k| k.as_object()) {
                // Check if this is a text index
                let has_text_field = key
                    .values()
                    .any(|v| v.as_str().map(|s| s == "text").unwrap_or(false));

                if has_text_field {
                    text_indexes_found += 1;
                    if text_indexes_found > 1 {
                        return Err(Error::Msg(
                            "too many text indexes for collection".to_string(),
                        ));
                    }
                    // Extract field names with "text" value
                    for (field_name, field_type) in key.iter() {
                        if field_type.as_str().map(|s| s == "text").unwrap_or(false) {
                            text_fields.push(field_name.clone());
                        }
                    }
                }
            }
        }

        Ok(text_fields)
    }

    pub async fn create_index_compound(
        &self,
        db: &str,
        coll: &str,
        name: &str,
        fields: &[(String, i32)],
        spec: &serde_json::Value,
    ) -> Result<()> {
        // Ensure collection (schema/table) exists
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let q_idx = q_ident(name);
        let mut elems: Vec<String> = Vec::with_capacity(fields.len());
        for (field, order) in fields.iter() {
            let field_escaped = field.replace('"', "\"\"");
            let ord = if *order < 0 { "DESC" } else { "ASC" };
            // expression index elem
            elems.push(format!("((doc->>'{}')) {}", field_escaped, ord));
        }
        let elems_joined = elems.join(", ");
        let t = Instant::now();
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING btree ({})",
            q_idx, q_schema, q_table, elems_joined
        );
        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;
        client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="create_index_compound", db=%db, coll=%coll, name=%name, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    /// Create a 2dsphere index for geospatial queries using PostgreSQL GIN index
    pub async fn create_index_2dsphere(
        &self,
        db: &str,
        coll: &str,
        name: &str,
        field: &str,
        spec: &serde_json::Value,
    ) -> Result<()> {
        // Ensure collection (schema/table) exists
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let q_idx = q_ident(name);
        let field_escaped = field.replace('"', "\"\"");

        let t = Instant::now();

        // Create GIN index on the GeoJSON field
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIN ((doc->'{}') jsonb_path_ops)",
            q_idx, q_schema, q_table, field_escaped
        );

        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;

        // Also create a functional index for geometry operations
        let geo_idx_name = format!("{}_geo", name);
        let q_geo_idx = q_ident(&geo_idx_name);
        let functional_ddl = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIN ((doc->'{}'))",
            q_geo_idx, q_schema, q_table, field_escaped
        );
        client
            .batch_execute(&functional_ddl)
            .await
            .map_err(err_msg)?;

        // Persist metadata
        client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="create_index_2dsphere", db=%db, coll=%coll, name=%name, field=%field, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    /// Create a text index for full-text search using PostgreSQL GIN index with to_tsvector
    pub async fn create_index_text(
        &self,
        db: &str,
        coll: &str,
        name: &str,
        fields: &[String],
        language: &str,
        spec: &serde_json::Value,
    ) -> Result<()> {
        // Ensure collection (schema/table) exists
        self.ensure_collection(db, coll).await?;

        // Check for existing text index - MongoDB allows only one text index per collection
        let existing_text_indexes = self.get_text_index_fields(db, coll).await?;
        if !existing_text_indexes.is_empty() {
            return Err(Error::Msg(
                "cannot have more than one text index per collection".to_string(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let q_idx = q_ident(name);

        let t = Instant::now();

        // Validate language against known PostgreSQL text search configurations
        let valid_languages = [
            "danish",
            "dutch",
            "english",
            "finnish",
            "french",
            "german",
            "hungarian",
            "italian",
            "norwegian",
            "portuguese",
            "romanian",
            "russian",
            "simple",
            "spanish",
            "swedish",
            "turkish",
        ];
        let safe_language = if valid_languages.contains(&language) {
            language.to_string()
        } else {
            "english".to_string()
        };

        // Build the to_tsvector expression for multiple fields
        // Escape both single and double quotes in field names to prevent SQL injection
        let tsvector_parts: Vec<String> = fields
            .iter()
            .map(|f| {
                let escaped = f.replace('"', "\"\"").replace('\'', "''");
                format!("COALESCE(doc->>'{}', '')", escaped)
            })
            .collect();
        let tsvector_expr = tsvector_parts.join(" || ' ' || ");

        // Create GIN index on the concatenated text fields
        let ddl = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIN (to_tsvector('{}', {}))",
            q_idx, q_schema, q_table, safe_language, tsvector_expr
        );

        let client = self.pool.get().await.map_err(err_msg)?;
        client.batch_execute(&ddl).await.map_err(err_msg)?;

        // Persist metadata
        client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="create_index_text", db=%db, coll=%coll, name=%name, fields=?fields, language=%language, elapsed_ms=?t.elapsed().as_millis());
        Ok(())
    }

    /// Find documents using full-text search
    #[allow(clippy::too_many_arguments)]
    pub async fn find_with_text_search(
        &self,
        db: &str,
        coll: &str,
        search_text: &str,
        language: &str,
        _case_sensitive: bool,
        _diacritic_sensitive: bool,
        limit: i64,
        fields: &[String],
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);

        let t = Instant::now();

        // Build the text search query using plainto_tsquery for safe parsing
        // Escape single quotes for SQL string literal
        let escaped_search = search_text.replace("'", "''");

        // Validate language against known PostgreSQL text search configurations
        let valid_languages = [
            "danish",
            "dutch",
            "english",
            "finnish",
            "french",
            "german",
            "hungarian",
            "italian",
            "norwegian",
            "portuguese",
            "romanian",
            "russian",
            "simple",
            "spanish",
            "swedish",
            "turkish",
        ];
        let safe_language = if valid_languages.contains(&language) {
            language.to_string()
        } else {
            "english".to_string()
        };

        // Build the tsvector expression from the provided fields (must match index definition)
        // Escape both single and double quotes in field names to prevent SQL injection
        let tsvector_parts: Vec<String> = if fields.is_empty() {
            // Default to searching all text fields if no fields specified
            vec!["doc::text".to_string()]
        } else {
            fields
                .iter()
                .map(|f| {
                    let escaped = f.replace('"', "\"\"").replace('\'', "''");
                    format!("COALESCE(doc->>'{}', '')", escaped)
                })
                .collect()
        };
        let tsvector_expr = tsvector_parts.join(" || ' ' || ");

        let sql = format!(
            "SELECT id, doc FROM {}.{} WHERE to_tsvector('{}', {}) @@ plainto_tsquery('{}', '{}') LIMIT {}",
            q_schema, q_table, safe_language, tsvector_expr, safe_language, escaped_search, limit
        );

        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client.query(&sql, &[]).await.map_err(err_msg)?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let _id: Vec<u8> = row.get("id");
            let doc_json: serde_json::Value = row.get("doc");
            let mut doc = bson::to_document(&doc_json)
                .map_err(|e| Error::Msg(format!("Failed to convert JSON to BSON: {}", e)))?;
            // Restore _id as ObjectId or String
            if _id.len() == 12 {
                doc.insert(
                    "_id",
                    bson::oid::ObjectId::from_bytes(_id.try_into().unwrap()),
                );
            } else {
                doc.insert("_id", String::from_utf8_lossy(&_id).to_string());
            }
            results.push(doc);
        }

        tracing::debug!(op="find_with_text_search", db=%db, coll=%coll, search_text=%search_text, results_count=%results.len(), elapsed_ms=?t.elapsed().as_millis());
        Ok(results)
    }

    pub async fn count_docs(
        &self,
        db: &str,
        coll: &str,
        filter: Option<&bson::Document>,
    ) -> Result<i64> {
        // Check for $text operator - not supported in count operations
        if let Some(f) = filter
            && f.contains_key("$text")
        {
            return Err(Error::Msg(
                "$text is not supported in count operations".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = filter
            .map(build_where_from_filter)
            .unwrap_or_else(|| "TRUE".to_string());
        let sql = format!(
            "SELECT COUNT(*) FROM {}.{} WHERE {}",
            q_schema, q_table, where_sql
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let res = client.query_one(&sql, &[]).await;
        match res {
            Ok(row) => {
                let n: i64 = row.get(0);
                tracing::debug!(op="count_docs", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
                Ok(n)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    Ok(0)
                } else {
                    Err(err_msg(e))
                }
            }
        }
    }

    // --- Update/Delete helpers (basic) ---

    /// Find one matching document for update, returning (id bytes, document).
    pub async fn find_one_for_update(
        &self,
        db: &str,
        coll: &str,
        filter: &bson::Document,
    ) -> Result<Option<(Vec<u8>, bson::Document)>> {
        // Check for $text operator - not supported in update operations
        if filter.contains_key("$text") {
            return Err(Error::Msg(
                "$text is not supported in update operations".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        // Fast path: _id equality
        if let Some(idb) = filter.get("_id").and_then(id_bytes_from_bson) {
            let sql = format!(
                "SELECT id, doc_bson, doc FROM {}.{} WHERE id = $1 LIMIT 1",
                q_schema, q_table
            );
            let client = self.pool.get().await.map_err(err_msg)?;
            let rows = client.query(&sql, &[&idb]).await.map_err(err_msg)?;
            if rows.is_empty() {
                return Ok(None);
            }
            let r = &rows[0];
            let id: Vec<u8> = r.get(0);
            if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1)
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                return Ok(Some((id, doc)));
            }
            let json: serde_json::Value = r.get(2);
            let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
            let doc = match b {
                bson::Bson::Document(d) => d,
                _ => bson::Document::new(),
            };
            return Ok(Some((id, doc)));
        }
        let where_sql = build_where_from_filter(filter);
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let sql = format!(
            "SELECT id, doc_bson, doc FROM {}.{} WHERE {} ORDER BY id ASC LIMIT 1",
            q_schema, q_table, where_sql
        );
        let rows = client.query(&sql, &[]).await.map_err(err_msg)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let r = &rows[0];
        let id: Vec<u8> = r.get(0);
        // Prefer bson if present
        if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1)
            && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
        {
            return Ok(Some((id, doc)));
        }
        let json: serde_json::Value = r.get(2);
        let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
        let doc = match b {
            bson::Bson::Document(d) => d,
            _ => bson::Document::new(),
        };
        tracing::debug!(op="find_one_for_update", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(Some((id, doc)))
    }

    /// Overwrite the full document by id (updates both doc_bson and doc JSON).
    pub async fn update_doc_by_id(
        &self,
        db: &str,
        coll: &str,
        id: &[u8],
        new_doc: &bson::Document,
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "UPDATE {}.{} SET doc_bson = $1, doc = $2 WHERE id = $3",
            q_schema, q_table
        );
        let bson_bytes = bson::to_vec(new_doc).map_err(err_msg)?;
        let json = serde_json::to_value(new_doc).map_err(err_msg)?;
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let n = client
            .execute(&sql, &[&bson_bytes, &json, &id])
            .await
            .map_err(err_msg)?;
        tracing::debug!(op="update_doc_by_id", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Delete one matching row based on filter; returns number of rows deleted (0 or 1).
    pub async fn delete_one_by_filter(
        &self,
        db: &str,
        coll: &str,
        filter: &bson::Document,
    ) -> Result<u64> {
        // Check for $text operator - not supported in delete operations
        if filter.contains_key("$text") {
            return Err(Error::Msg(
                "$text is not supported in delete operations".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        // Fast path: _id equality
        if let Some(idb) = filter.get("_id").and_then(id_bytes_from_bson) {
            let del_sql = format!("DELETE FROM {}.{} WHERE id = $1", q_schema, q_table);
            let client = self.pool.get().await.map_err(err_msg)?;
            let n = client.execute(&del_sql, &[&idb]).await.map_err(err_msg)?;
            return Ok(n);
        }
        let where_sql = build_where_from_filter(filter);
        let select_sql = format!(
            "SELECT id FROM {}.{} WHERE {} ORDER BY id ASC LIMIT 1",
            q_schema, q_table, where_sql
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = client.query(&select_sql, &[]).await.map_err(err_msg)?;
        if rows.is_empty() {
            return Ok(0);
        }
        let id: Vec<u8> = rows[0].get(0);
        let del_sql = format!("DELETE FROM {}.{} WHERE id = $1", q_schema, q_table);
        let n = client.execute(&del_sql, &[&id]).await.map_err(err_msg)?;
        tracing::debug!(op="delete_one_by_filter", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Delete many rows matching filter; returns number of rows deleted.
    pub async fn delete_many_by_filter(
        &self,
        db: &str,
        coll: &str,
        filter: &bson::Document,
    ) -> Result<u64> {
        // Check for $text operator - not supported in delete operations
        if filter.contains_key("$text") {
            return Err(Error::Msg(
                "$text is not supported in delete operations".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = build_where_from_filter(filter);
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let sql = format!("DELETE FROM {}.{} WHERE {}", q_schema, q_table, where_sql);
        let n = client.execute(&sql, &[]).await.map_err(err_msg)?;
        tracing::debug!(op="delete_many_by_filter", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }
}

fn schema_name(db: &str) -> String {
    format!("mdb_{}", db)
}

fn q_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn err_msg<E: std::fmt::Display>(e: E) -> Error {
    Error::Msg(e.to_string())
}

fn escape_single(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

// removed unused helpers

fn build_where_from_filter(filter: &bson::Document) -> String {
    build_where_from_filter_internal(filter, false)
}

fn build_where_from_filter_internal(filter: &bson::Document, is_nested: bool) -> String {
    let mut where_clauses: Vec<String> = Vec::new();

    // Handle logical operators at the top level first
    if let Some(or_val) = filter.get("$or")
        && let bson::Bson::Array(arr) = or_val
    {
        let mut or_clauses: Vec<String> = Vec::new();
        for item in arr {
            if let bson::Bson::Document(d) = item {
                let clause = build_where_from_filter_internal(d, true);
                if clause != "TRUE" {
                    or_clauses.push(clause);
                }
            }
        }
        if !or_clauses.is_empty() {
            let joined = or_clauses.join(" OR ");
            where_clauses.push(if or_clauses.len() > 1 {
                format!("({})", joined)
            } else {
                joined
            });
        }
    }

    if let Some(and_val) = filter.get("$and")
        && let bson::Bson::Array(arr) = and_val
    {
        let mut and_clauses: Vec<String> = Vec::new();
        for item in arr {
            if let bson::Bson::Document(d) = item {
                let clause = build_where_from_filter_internal(d, true);
                if clause != "TRUE" {
                    and_clauses.push(clause);
                }
            }
        }
        if !and_clauses.is_empty() {
            let joined = and_clauses.join(" AND ");
            where_clauses.push(if and_clauses.len() > 1 {
                format!("({})", joined)
            } else {
                joined
            });
        }
    }

    if let Some(not_val) = filter.get("$not")
        && let bson::Bson::Document(d) = not_val
    {
        let clause = build_where_from_filter_internal(d, true);
        if clause != "TRUE" {
            where_clauses.push(format!("NOT ({})", clause));
        }
    }

    if let Some(nor_val) = filter.get("$nor")
        && let bson::Bson::Array(arr) = nor_val
    {
        let mut nor_clauses: Vec<String> = Vec::new();
        for item in arr {
            if let bson::Bson::Document(d) = item {
                let clause = build_where_from_filter_internal(d, true);
                if clause != "TRUE" {
                    nor_clauses.push(clause);
                }
            }
        }
        if !nor_clauses.is_empty() {
            let joined = nor_clauses.join(" OR ");
            where_clauses.push(format!(
                "NOT ({})",
                if nor_clauses.len() > 1 {
                    format!("({})", joined)
                } else {
                    joined
                }
            ));
        }
    }

    // Process field-level operators (skip keys starting with $)
    for (k, v) in filter.iter() {
        if k == "_id" || k.starts_with('$') {
            continue;
        }
        let path = jsonpath_path(k);
        match v {
            bson::Bson::Document(d) => {
                for (op, val) in d.iter() {
                    match op.as_str() {
                        "$elemMatch" => {
                            if let bson::Bson::Document(em) = val
                                && let Some(pred) = build_elem_match_pred(&path, em)
                            {
                                where_clauses.push(format!(
                                    "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                    escape_single(&path),
                                    pred
                                ));
                            }
                        }
                        "$exists" => {
                            let clause = if matches!(val, bson::Bson::Boolean(true)) {
                                format!("jsonb_path_exists(doc, '{}')", escape_single(&path))
                            } else {
                                format!("NOT jsonb_path_exists(doc, '{}')", escape_single(&path))
                            };
                            where_clauses.push(clause);
                        }
                        "$in" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut preds: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
                                        preds.push(format!("@ == {}", lit));
                                    }
                                }
                                if preds.is_empty() {
                                    where_clauses.push("FALSE".to_string());
                                } else {
                                    let predicate = preds.join(" || ");
                                    let p1 = format!(
                                        "jsonb_path_exists(doc, '{} ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    let p2 = format!(
                                        "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    where_clauses.push(format!("({} OR {})", p1, p2));
                                }
                            }
                        }
                        "$ne" => {
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ != {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ != {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        "$nin" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut preds: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
                                        preds.push(format!("@ != {}", lit));
                                    }
                                }
                                if preds.is_empty() {
                                    where_clauses.push("TRUE".to_string());
                                } else {
                                    let predicate = preds.join(" && ");
                                    let p1 = format!(
                                        "jsonb_path_exists(doc, '{} ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    let p2 = format!(
                                        "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    where_clauses.push(format!("({} AND {})", p1, p2));
                                }
                            }
                        }
                        "$regex" => {
                            if let bson::Bson::String(pattern) = val {
                                let flags =
                                    d.get("$options").and_then(|o| o.as_str()).unwrap_or("");
                                let regex_clause = build_regex_clause(&path, pattern, flags);
                                where_clauses.push(regex_clause);
                            }
                        }
                        "$all" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut all_clauses: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
                                        let p1 = format!(
                                            "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                                            escape_single(&path),
                                            lit
                                        );
                                        let p2 = format!(
                                            "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                                            escape_single(&path),
                                            lit
                                        );
                                        all_clauses.push(format!("({} OR {})", p1, p2));
                                    }
                                }
                                if !all_clauses.is_empty() {
                                    where_clauses.push(format!("({})", all_clauses.join(" AND ")));
                                }
                            }
                        }
                        "$size" => {
                            let size_val = match val {
                                bson::Bson::Int32(n) => Some(*n as i64),
                                bson::Bson::Int64(n) => Some(*n),
                                _ => None,
                            };
                            if let Some(n) = size_val {
                                let size_clause = format!(
                                    "jsonb_array_length(doc->'{}') = {}",
                                    escape_single(k),
                                    n
                                );
                                where_clauses.push(size_clause);
                            }
                        }
                        "$gt" | "$gte" | "$lt" | "$lte" => {
                            let op_sql = match op.as_str() {
                                "$gt" => ">",
                                "$gte" => ">=",
                                "$lt" => "<",
                                "$lte" => "<=",
                                _ => unreachable!(),
                            };
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ {} {} )')",
                                    escape_single(&path),
                                    op_sql,
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ {} {} )')",
                                    escape_single(&path),
                                    op_sql,
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        "$eq" => {
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        "$geoWithin" => {
                            if let bson::Bson::Document(gw) = val {
                                if let Some(geom) = gw.get("$geometry") {
                                    // GeoJSON format
                                    if let bson::Bson::Document(geom_doc) = geom
                                        && let Ok(geom_type) = geom_doc.get_str("type")
                                        && let Ok(coords) = geom_doc.get_array("coordinates")
                                    {
                                        let clause = build_geo_within_clause(k, geom_type, coords);
                                        where_clauses.push(clause);
                                    }
                                } else if let Some(bson::Bson::Array(box_coords)) = gw.get("$box") {
                                    // Legacy $box format
                                    let clause = build_geo_box_clause(k, box_coords);
                                    where_clauses.push(clause);
                                } else if let Some(bson::Bson::Array(poly_coords)) =
                                    gw.get("$polygon")
                                {
                                    // Legacy $polygon format
                                    let clause = build_geo_polygon_clause(k, poly_coords);
                                    where_clauses.push(clause);
                                }
                            }
                        }
                        "$near" | "$nearSphere" => {
                            if let bson::Bson::Document(near_doc) = val {
                                let spherical = op == "$nearSphere";

                                // Get the near point (can be GeoJSON or legacy [lon, lat])
                                let (near_lon, near_lat) = if let Some(bson::Bson::Document(geom)) =
                                    near_doc.get("$geometry")
                                {
                                    // GeoJSON format
                                    if let Ok(coords) = geom.get_array("coordinates") {
                                        if coords.len() >= 2 {
                                            let lon = coords[0]
                                                .as_f64()
                                                .or_else(|| coords[0].as_i64().map(|v| v as f64))
                                                .unwrap_or(0.0);
                                            let lat = coords[1]
                                                .as_f64()
                                                .or_else(|| coords[1].as_i64().map(|v| v as f64))
                                                .unwrap_or(0.0);
                                            (lon, lat)
                                        } else {
                                            (0.0, 0.0)
                                        }
                                    } else {
                                        (0.0, 0.0)
                                    }
                                } else if let Ok(coords) = near_doc.get_array("$near") {
                                    // Legacy format
                                    if coords.len() >= 2 {
                                        let lon = coords[0]
                                            .as_f64()
                                            .or_else(|| coords[0].as_i64().map(|v| v as f64))
                                            .unwrap_or(0.0);
                                        let lat = coords[1]
                                            .as_f64()
                                            .or_else(|| coords[1].as_i64().map(|v| v as f64))
                                            .unwrap_or(0.0);
                                        (lon, lat)
                                    } else {
                                        (0.0, 0.0)
                                    }
                                } else {
                                    (0.0, 0.0)
                                };

                                // Get maxDistance
                                let max_distance = near_doc
                                    .get_f64("$maxDistance")
                                    .or_else(|_| near_doc.get_i64("$maxDistance").map(|v| v as f64))
                                    .ok();

                                let clause = build_near_clause(
                                    k,
                                    near_lon,
                                    near_lat,
                                    max_distance,
                                    spherical,
                                );
                                where_clauses.push(clause);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                if let Some(lit) = json_literal_from_bson(v) {
                    let p1 = format!(
                        "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                        escape_single(&path),
                        lit
                    );
                    let p2 = format!(
                        "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                        escape_single(&path),
                        lit
                    );
                    where_clauses.push(format!("({} OR {})", p1, p2));
                }
            }
        }
    }

    if where_clauses.is_empty() {
        String::from("TRUE")
    } else if where_clauses.len() == 1 {
        where_clauses[0].clone()
    } else if is_nested {
        format!("({})", where_clauses.join(" AND "))
    } else {
        where_clauses.join(" AND ")
    }
}

fn build_regex_clause(path: &str, pattern: &str, flags: &str) -> String {
    // Convert MongoDB regex pattern to PostgreSQL regex
    // Escape single quotes in pattern
    let escaped_pattern = pattern.replace("'", "''");

    // Extract field name from JSONPath format $.”field” or $."field"
    // Remove leading '$' and extract quoted segments
    let field_name = if let Some(stripped) = path.strip_prefix("$.") {
        // Extract content between quotes: $."field" -> field
        stripped
            .split(".")
            .map(|s| s.trim_matches('"'))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        path.trim_start_matches('$').to_string()
    };
    let escaped_path = escape_single(&field_name);

    // Build PostgreSQL regex flags
    let mut pg_flags = String::new();
    if flags.contains('i') {
        pg_flags.push('i');
    }
    if flags.contains('m') || flags.contains('s') {
        pg_flags.push('n');
    }

    let flag_prefix = if pg_flags.is_empty() {
        String::new()
    } else {
        format!("(?{})", pg_flags)
    };

    // Use ~ operator for regex matching
    format!(
        "(doc->>'{}') ~ '{}{}'",
        escaped_path, flag_prefix, escaped_pattern
    )
}

fn build_order_by(sort: Option<&bson::Document>) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut has_id = false;
    if let Some(spec) = sort {
        for (k, v) in spec.iter() {
            let dir = match v {
                bson::Bson::Int32(n) => *n,
                bson::Bson::Int64(n) => *n as i32,
                bson::Bson::Double(f) => {
                    if *f < 0.0 {
                        -1
                    } else {
                        1
                    }
                }
                _ => 1,
            };
            let ord = if dir < 0 { "DESC" } else { "ASC" };
            if k == "_id" {
                has_id = true;
                parts.push(format!("id {}", ord));
            } else {
                let f = escape_single(k);
                // Heuristic: numbers before strings, then numeric ASC/DESC, then text ASC/DESC
                let numeric_re = format!("(doc->>'{}') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'", f);
                let num_first = format!("(CASE WHEN {} THEN 0 ELSE 1 END) ASC", numeric_re);
                let num_val = format!(
                    "(CASE WHEN {} THEN (doc->>'{}')::double precision END) {}",
                    numeric_re, f, ord
                );
                let text_val = format!("(doc->>'{}') {}", f, ord);
                parts.push(num_first);
                parts.push(num_val);
                parts.push(text_val);
            }
        }
    }
    if !has_id {
        parts.push("id ASC".to_string());
    }
    format!("ORDER BY {}", parts.join(", "))
}

fn projection_pushdown_sql(projection: Option<&bson::Document>) -> Option<String> {
    let proj = projection?;
    if proj.is_empty() {
        return None;
    }
    // Allow inclusive projections with possible _id exclusion and computed fields
    let mut include_fields: Vec<(String, String)> = Vec::new();
    let mut include_id = true;
    for (k, v) in proj.iter() {
        if k == "_id" {
            match v {
                bson::Bson::Int32(n) if *n == 0 => include_id = false,
                bson::Bson::Boolean(b) if !*b => include_id = false,
                _ => {}
            }
            continue;
        }
        // Only top-level fields qualify for pushdown; dotted paths require server-side projection
        if k.contains('.') {
            return None;
        }
        match v {
            bson::Bson::Int32(n) => {
                if *n != 0 {
                    include_fields.push((k.clone(), format!("doc->'{}'", escape_single(k))));
                } else {
                    return None;
                }
            }
            bson::Bson::Boolean(b) => {
                if *b {
                    include_fields.push((k.clone(), format!("doc->'{}'", escape_single(k))));
                } else {
                    return None;
                }
            }
            bson::Bson::Document(_) => {
                // Computed field - try to translate to SQL expression
                if let Some(sql_expr) = translate_expression(v) {
                    include_fields.push((k.clone(), sql_expr));
                } else {
                    return None;
                }
            }
            _ => {
                return None;
            }
        }
    }
    if include_fields.is_empty() && include_id {
        return None;
    }
    let mut elems: Vec<String> = Vec::new();
    if include_id {
        elems.push("'_id', doc->'_id'".to_string());
    }
    for (field_name, sql_expr) in include_fields {
        elems.push(format!("'{}', {}", escape_single(&field_name), sql_expr));
    }
    if elems.is_empty() {
        return None;
    }
    Some(format!("jsonb_build_object({})", elems.join(", ")))
}

fn json_to_bson(v: &serde_json::Value) -> bson::Bson {
    use serde_json::Value;
    match v {
        Value::Null => bson::Bson::Null,
        Value::Bool(b) => bson::Bson::Boolean(*b),
        Value::Number(num) => {
            if let Some(i) = num.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    bson::Bson::Int32(i as i32)
                } else {
                    bson::Bson::Int64(i)
                }
            } else if let Some(u) = num.as_u64() {
                if u <= i32::MAX as u64 {
                    bson::Bson::Int32(u as i32)
                } else if u <= i64::MAX as u64 {
                    bson::Bson::Int64(u as i64)
                } else {
                    bson::Bson::Double(u as f64)
                }
            } else if let Some(f) = num.as_f64() {
                bson::Bson::Double(f)
            } else {
                bson::Bson::Double(0.0)
            }
        }
        Value::String(s) => bson::Bson::String(s.clone()),
        Value::Array(arr) => bson::Bson::Array(arr.iter().map(json_to_bson).collect()),
        Value::Object(map) => {
            let mut d = bson::Document::new();
            for (k, v) in map.iter() {
                d.insert(k.clone(), json_to_bson(v));
            }
            bson::Bson::Document(d)
        }
    }
}

fn to_doc_from_json(json: serde_json::Value) -> bson::Document {
    match json_to_bson(&json) {
        bson::Bson::Document(d) => d,
        _ => bson::Document::new(),
    }
}

fn jsonpath_path(key: &str) -> String {
    // Build $."a"."b" style path for dotted keys
    let mut out = String::from("$");
    for seg in key.split('.') {
        let esc = seg.replace('"', "\\\"");
        out.push_str(".\"");
        out.push_str(&esc);
        out.push('"');
    }
    out
}

fn json_literal_from_bson(v: &bson::Bson) -> Option<String> {
    // Only simple scalar types for now
    match v {
        bson::Bson::Null => Some("null".to_string()),
        bson::Bson::Boolean(b) => Some(if *b { "true".into() } else { "false".into() }),
        bson::Bson::Int32(n) => Some(n.to_string()),
        bson::Bson::Int64(n) => Some(n.to_string()),
        bson::Bson::Double(n) => Some(n.to_string()),
        bson::Bson::String(s) => Some((serde_json::to_string(s).ok()?).to_string()),
        _ => None,
    }
}

fn id_bytes_from_bson(b: &bson::Bson) -> Option<Vec<u8>> {
    match b {
        bson::Bson::ObjectId(oid) => Some(oid.bytes().to_vec()),
        bson::Bson::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

fn build_elem_match_pred(_path: &str, em: &bson::Document) -> Option<String> {
    // Two forms supported:
    // 1) Scalar operators on array of scalars: { $gt: 5 }
    // 2) Equality/ops on subdocument fields: { x: 2, y: { $gt: 3 } }
    if em.iter().all(|(k, _)| k.starts_with('$')) {
        // scalar ops on @
        let mut clauses: Vec<String> = Vec::new();
        for (op, val) in em.iter() {
            let (sql_op, lit) = match op.as_str() {
                "$gt" => (">", json_literal_from_bson(val)),
                "$gte" => (">=", json_literal_from_bson(val)),
                "$lt" => ("<", json_literal_from_bson(val)),
                "$lte" => ("<=", json_literal_from_bson(val)),
                "$eq" => ("==", json_literal_from_bson(val)),
                _ => ("", None),
            };
            if sql_op.is_empty() || lit.is_none() {
                continue;
            }
            clauses.push(format!("@ {} {}", sql_op, lit.unwrap()));
        }
        if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(" && "))
        }
    } else {
        // subdocument fields
        let mut clauses: Vec<String> = Vec::new();
        for (k, v) in em.iter() {
            let attr = format!("@.\"{}\"", k.replace('"', "\\\""));
            match v {
                bson::Bson::Document(d) => {
                    for (op, val) in d.iter() {
                        let (sql_op, lit) = match op.as_str() {
                            "$gt" => (">", json_literal_from_bson(val)),
                            "$gte" => (">=", json_literal_from_bson(val)),
                            "$lt" => ("<", json_literal_from_bson(val)),
                            "$lte" => ("<=", json_literal_from_bson(val)),
                            "$eq" => ("==", json_literal_from_bson(val)),
                            _ => ("", None),
                        };
                        if sql_op.is_empty() || lit.is_none() {
                            continue;
                        }
                        clauses.push(format!("{} {} {}", attr, sql_op, lit.unwrap()));
                    }
                }
                _ => {
                    if let Some(lit) = json_literal_from_bson(v) {
                        clauses.push(format!("{} == {}", attr, lit));
                    }
                }
            }
        }
        if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(" && "))
        }
    }
}

fn project_document(doc: &bson::Document, projection: &bson::Document) -> bson::Document {
    // Determine include/exclude mode
    let mut include_mode = false;
    let mut include_id = true;
    for (k, v) in projection.iter() {
        if k == "_id" {
            match v {
                bson::Bson::Int32(n) if *n == 0 => include_id = false,
                bson::Bson::Boolean(b) if !*b => include_id = false,
                _ => {}
            }
        } else if matches!(v, bson::Bson::Int32(n) if *n != 0)
            || matches!(v, bson::Bson::Boolean(true))
        {
            include_mode = true;
        }
    }

    if include_mode {
        let mut out = bson::Document::new();
        if include_id && let Some(idv) = doc.get("_id").cloned() {
            out.insert("_id", idv);
        }
        for (k, v) in projection.iter() {
            if k == "_id" {
                continue;
            }
            let on = match v {
                bson::Bson::Int32(n) => *n != 0,
                bson::Bson::Boolean(b) => *b,
                _ => false,
            };
            if on && let Some(val) = get_path(doc, k) {
                set_path(&mut out, k, val);
            }
        }
        out
    } else {
        // Exclusion mode: start with full doc and remove fields
        let mut out = doc.clone();
        for (k, v) in projection.iter() {
            if k == "_id" {
                continue;
            }
            let off = match v {
                bson::Bson::Int32(n) => *n == 0,
                bson::Bson::Boolean(b) => !*b,
                _ => false,
            };
            if off {
                remove_path(&mut out, k);
            }
        }
        if !include_id {
            out.remove("_id");
        }
        out
    }
}

fn get_path(doc: &bson::Document, path: &str) -> Option<bson::Bson> {
    let mut cur = bson::Bson::Document(doc.clone());
    let mut segs = path.split('.').peekable();
    while let Some(seg) = segs.next() {
        match cur {
            bson::Bson::Document(ref d) => {
                if let Some(v) = d.get(seg) {
                    if segs.peek().is_some() {
                        cur = v.clone();
                    } else {
                        return Some(v.clone());
                    }
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    None
}

fn set_path(doc: &mut bson::Document, path: &str, value: bson::Bson) {
    let mut segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return;
    }
    let last = segments.pop().unwrap();
    let mut cur = doc;
    for seg in segments {
        if !cur.contains_key(seg) {
            cur.insert(seg, bson::Bson::Document(bson::Document::new()));
        }
        let entry = cur.get_mut(seg).unwrap();
        if entry.as_document().is_none() {
            *entry = bson::Bson::Document(bson::Document::new());
        }
        cur = entry.as_document_mut().unwrap();
    }
    cur.insert(last, value);
}

fn remove_path(doc: &mut bson::Document, path: &str) {
    let mut segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return;
    }
    let last = segments.pop().unwrap();
    let mut cur = doc;
    for seg in segments {
        match cur.get_mut(seg) {
            Some(bson::Bson::Document(d)) => {
                cur = d;
            }
            _ => return,
        }
    }
    cur.remove(last);
}

impl PgStore {
    pub fn dsn(&self) -> &str {
        &self.dsn
    }
    pub async fn get_client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(err_msg)
    }

    /// Transactional: find first matching row with optional sort, locking it FOR UPDATE
    pub async fn find_one_for_update_sorted_tx(
        &self,
        tx: &Transaction<'_>,
        db: &str,
        coll: &str,
        filter: &bson::Document,
        sort: Option<&bson::Document>,
    ) -> Result<Option<(Vec<u8>, bson::Document)>> {
        // Check for $text operator - not supported in update operations
        if filter.contains_key("$text") {
            return Err(Error::Msg(
                "$text is not supported in update operations".into(),
            ));
        }

        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = build_where_from_filter(filter);
        let order_sql = build_order_by(sort);
        let sql = format!(
            "SELECT id, doc_bson, doc FROM {}.{} WHERE {} {} LIMIT 1 FOR UPDATE",
            q_schema, q_table, where_sql, order_sql
        );
        match tx.query(&sql, &[]).await {
            Ok(rows) => {
                if rows.is_empty() {
                    return Ok(None);
                }
                let r = &rows[0];
                let id: Vec<u8> = r.get(0);
                if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1)
                    && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
                {
                    return Ok(Some((id, doc)));
                }
                let json: serde_json::Value = r.get(2);
                let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
                let doc = match b {
                    bson::Bson::Document(d) => d,
                    _ => bson::Document::new(),
                };
                Ok(Some((id, doc)))
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    Ok(None)
                } else {
                    Err(err_msg(e))
                }
            }
        }
    }

    pub async fn update_doc_by_id_tx(
        &self,
        tx: &Transaction<'_>,
        db: &str,
        coll: &str,
        id: &[u8],
        new_doc: &bson::Document,
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "UPDATE {}.{} SET doc_bson = $1, doc = $2 WHERE id = $3",
            q_schema, q_table
        );
        let bson_bytes = bson::to_vec(new_doc).map_err(err_msg)?;
        let json = serde_json::to_value(new_doc).map_err(err_msg)?;
        let n = tx
            .execute(&sql, &[&bson_bytes, &json, &id])
            .await
            .map_err(err_msg)?;
        Ok(n)
    }

    pub async fn insert_one_tx(
        &self,
        tx: &Transaction<'_>,
        db: &str,
        coll: &str,
        id: &[u8],
        bson_bytes: &[u8],
        json: &serde_json::Value,
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "INSERT INTO {}.{} (id, doc_bson, doc) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            q_schema, q_table
        );
        let n = tx
            .execute(&sql, &[&id, &bson_bytes, &json])
            .await
            .map_err(err_msg)?;
        Ok(n)
    }

    pub async fn delete_by_id_tx(
        &self,
        tx: &Transaction<'_>,
        db: &str,
        coll: &str,
        id: &[u8],
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!("DELETE FROM {}.{} WHERE id = $1", q_schema, q_table);
        let n = tx.execute(&sql, &[&id]).await.map_err(err_msg)?;
        Ok(n)
    }

    /// Transaction-aware insert: uses transaction client if provided, otherwise uses pool
    pub async fn insert_one_tx_opt(
        &self,
        tx: Option<&Transaction<'_>>,
        db: &str,
        coll: &str,
        id: &[u8],
        bson_bytes: &[u8],
        json: &serde_json::Value,
    ) -> Result<u64> {
        self.ensure_collection(db, coll).await?;
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "INSERT INTO {}.{} (id, doc_bson, doc) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            q_schema, q_table
        );
        let t = Instant::now();
        let n = if let Some(transaction) = tx {
            transaction
                .execute(&sql, &[&id, &bson_bytes, &json])
                .await
                .map_err(err_msg)?
        } else {
            let client = self.pool.get().await.map_err(err_msg)?;
            client
                .execute(&sql, &[&id, &bson_bytes, &json])
                .await
                .map_err(err_msg)?
        };
        tracing::debug!(op="insert_one_tx_opt", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Transaction-aware update: uses transaction client if provided, otherwise uses pool
    pub async fn update_doc_by_id_tx_opt(
        &self,
        tx: Option<&Transaction<'_>>,
        db: &str,
        coll: &str,
        id: &[u8],
        new_doc: &bson::Document,
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "UPDATE {}.{} SET doc_bson = $1, doc = $2 WHERE id = $3",
            q_schema, q_table
        );
        let bson_bytes = bson::to_vec(new_doc).map_err(err_msg)?;
        let json = serde_json::to_value(new_doc).map_err(err_msg)?;
        let t = Instant::now();
        let n = if let Some(transaction) = tx {
            transaction
                .execute(&sql, &[&bson_bytes, &json, &id])
                .await
                .map_err(err_msg)?
        } else {
            let client = self.pool.get().await.map_err(err_msg)?;
            client
                .execute(&sql, &[&bson_bytes, &json, &id])
                .await
                .map_err(err_msg)?
        };
        tracing::debug!(op="update_doc_by_id_tx_opt", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Transaction-aware delete: uses transaction client if provided, otherwise uses pool
    pub async fn delete_by_id_tx_opt(
        &self,
        tx: Option<&Transaction<'_>>,
        db: &str,
        coll: &str,
        id: &[u8],
    ) -> Result<u64> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!("DELETE FROM {}.{} WHERE id = $1", q_schema, q_table);
        let t = Instant::now();
        let n = if let Some(transaction) = tx {
            transaction.execute(&sql, &[&id]).await.map_err(err_msg)?
        } else {
            let client = self.pool.get().await.map_err(err_msg)?;
            client.execute(&sql, &[&id]).await.map_err(err_msg)?
        };
        tracing::debug!(op="delete_by_id_tx_opt", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(n)
    }

    /// Transaction-aware find: uses transaction client if provided, otherwise uses pool
    #[allow(clippy::too_many_arguments)]
    pub async fn find_docs_tx_opt(
        &self,
        tx: Option<&Transaction<'_>>,
        db: &str,
        coll: &str,
        filter: Option<&bson::Document>,
        sort: Option<&bson::Document>,
        projection: Option<&bson::Document>,
        limit: i64,
    ) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_sql = filter.map(build_where_from_filter);
        let order_sql = build_order_by(sort);

        let t = Instant::now();
        let res = if let Some(transaction) = tx {
            match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                        q_schema, q_table, where_clause, order_sql, limit
                    );
                    transaction.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        q_schema, q_table, order_sql, limit
                    );
                    transaction.query(&sql, &[]).await
                }
            }
        } else {
            let client = self.pool.get().await.map_err(err_msg)?;
            match &where_sql {
                Some(where_clause) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                        q_schema, q_table, where_clause, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
                None => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE TRUE {} LIMIT {}",
                        q_schema, q_table, order_sql, limit
                    );
                    client.query(&sql, &[]).await
                }
            }
        };

        let rows = match res {
            Ok(r) => r,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("does not exist") {
                    return Ok(Vec::new());
                }
                return Err(err_msg(e));
            }
        };

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let bson_bytes: Option<Vec<u8>> = r.try_get(0).ok();
            if let Some(bytes) = bson_bytes
                && let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
            {
                out.push(if let Some(p) = projection {
                    project_document(&doc, p)
                } else {
                    doc
                });
                continue;
            }
            let json: serde_json::Value = r.get(1);
            let d = to_doc_from_json(json);
            out.push(if let Some(p) = projection {
                project_document(&d, p)
            } else {
                d
            });
        }
        tracing::debug!(op="find_docs_tx_opt", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
        Ok(out)
    }
}

// --- Internal cache helpers ---
impl PgStore {
    async fn is_known_db(&self, db: &str) -> bool {
        let g = self.databases_cache.read().await;
        g.contains(db)
    }
    async fn mark_db_known(&self, db: &str) {
        let mut g = self.databases_cache.write().await;
        g.insert(db.to_string());
    }
    async fn is_known_collection(&self, db: &str, coll: &str) -> bool {
        let g = self.collections_cache.read().await;
        g.contains(&(db.to_string(), coll.to_string()))
    }
    async fn mark_collection_known(&self, db: &str, coll: &str) {
        let mut g = self.collections_cache.write().await;
        g.insert((db.to_string(), coll.to_string()));
    }
}

/// Collation configuration for sorting
pub struct Collation {
    pub locale: String,
    pub strength: Option<i32>,
}

impl Collation {
    pub fn from_bson(doc: &bson::Document) -> Option<Self> {
        let locale = doc.get_str("locale").ok()?;
        let strength = doc.get_i32("strength").ok();
        Some(Collation {
            locale: locale.to_string(),
            strength,
        })
    }

    pub fn to_collate_clause(&self) -> String {
        format!("COLLATE \"{}\"", self.locale)
    }
}

// --- Geospatial query helper functions ---

/// Build SQL clause for $geoWithin with GeoJSON geometry (using bson::Array)
fn build_geo_within_clause(field: &str, geom_type: &str, coords: &bson::Array) -> String {
    match geom_type {
        "Polygon" => {
            // Extract bounding box from polygon coordinates
            if let Some(outer_ring) = coords.first().and_then(|r| r.as_array()) {
                let (min_lon, max_lon, min_lat, max_lat) = extract_bounding_box_bson(outer_ring);
                // Use direct JSONB extraction for better compatibility
                format!(
                    "(doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {}",
                    field, min_lon, field, max_lon, field, min_lat, field, max_lat
                )
            } else {
                "FALSE".to_string()
            }
        }
        "MultiPolygon" => {
            // For MultiPolygon, check within any of the polygons
            let mut clauses: Vec<String> = Vec::new();
            for poly in coords.iter().filter_map(|p| p.as_array()) {
                if let Some(outer_ring) = poly.first().and_then(|r| r.as_array()) {
                    let (min_lon, max_lon, min_lat, max_lat) =
                        extract_bounding_box_bson(outer_ring);
                    clauses.push(format!(
                        "((doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {})",
                        field, min_lon, field, max_lon, field, min_lat, field, max_lat
                    ));
                }
            }
            if clauses.is_empty() {
                "FALSE".to_string()
            } else {
                format!("({})", clauses.join(" OR "))
            }
        }
        _ => "FALSE".to_string(),
    }
}

/// Build SQL clause for legacy $box operator (using bson::Array)
fn build_geo_box_clause(field: &str, box_coords: &bson::Array) -> String {
    if box_coords.len() >= 2 {
        let bottom_left = box_coords[0].as_array();
        let top_right = box_coords[1].as_array();

        if let (Some(bl), Some(tr)) = (bottom_left, top_right)
            && bl.len() >= 2
            && tr.len() >= 2
        {
            let min_lon = bl[0]
                .as_f64()
                .or_else(|| bl[0].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let min_lat = bl[1]
                .as_f64()
                .or_else(|| bl[1].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let max_lon = tr[0]
                .as_f64()
                .or_else(|| tr[0].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let max_lat = tr[1]
                .as_f64()
                .or_else(|| tr[1].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);

            return format!(
                "(doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {}",
                field, min_lon, field, max_lon, field, min_lat, field, max_lat
            );
        }
    }
    "FALSE".to_string()
}

/// Build SQL clause for legacy $polygon operator (using bson::Array)
fn build_geo_polygon_clause(field: &str, poly_coords: &bson::Array) -> String {
    // Compute bounding box from all polygon vertices
    let (min_lon, max_lon, min_lat, max_lat) = extract_bounding_box_bson(poly_coords);

    // Check if we got valid bounds (not the initial MAX/MIN values)
    if min_lon <= max_lon && min_lat <= max_lat {
        return format!(
            "(doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {}",
            field, min_lon, field, max_lon, field, min_lat, field, max_lat
        );
    }
    "FALSE".to_string()
}

/// Build SQL clause for $near/$nearSphere operators
fn build_near_clause(
    field: &str,
    near_lon: f64,
    near_lat: f64,
    max_distance: Option<f64>,
    _spherical: bool,
) -> String {
    // Build the clause using bounding box for efficiency
    if let Some(max_dist) = max_distance {
        // For spherical calculations, convert distance from meters to degrees approximately
        // 1 degree of latitude is approximately 111km
        let max_dist_degrees = max_dist / 111000.0;
        format!(
            "(doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {}",
            field,
            near_lon - max_dist_degrees,
            field,
            near_lon + max_dist_degrees,
            field,
            near_lat - max_dist_degrees,
            field,
            near_lat + max_dist_degrees
        )
    } else {
        format!(
            "(doc->'{}'->'coordinates'->>0)::float8 >= {} AND (doc->'{}'->'coordinates'->>0)::float8 <= {} AND (doc->'{}'->'coordinates'->>1)::float8 >= {} AND (doc->'{}'->'coordinates'->>1)::float8 <= {}",
            field,
            near_lon - 1.0,
            field,
            near_lon + 1.0,
            field,
            near_lat - 1.0,
            field,
            near_lat + 1.0
        )
    }
}

/// Extract bounding box from a ring of coordinates (using bson::Array)
fn extract_bounding_box_bson(ring: &bson::Array) -> (f64, f64, f64, f64) {
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;

    for point in ring.iter().filter_map(|p| p.as_array()) {
        if point.len() >= 2 {
            let lon = point[0]
                .as_f64()
                .or_else(|| point[0].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let lat = point[1]
                .as_f64()
                .or_else(|| point[1].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);

            min_lon = min_lon.min(lon);
            max_lon = max_lon.max(lon);
            min_lat = min_lat.min(lat);
            max_lat = max_lat.max(lat);
        }
    }

    (min_lon, max_lon, min_lat, max_lat)
}
