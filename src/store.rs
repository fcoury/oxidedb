use crate::error::{Error, Result};
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
            if let Some(bytes) = bson_bytes {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    out.push(doc);
                    continue;
                }
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
            if let Some(bytes) = bson_bytes {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    out.push(doc);
                    continue;
                }
            }
            let json: serde_json::Value = r.get(1);
            let doc = to_doc_from_json(json);
            out.push(doc);
        }
        tracing::debug!(op="find_by_id_docs", db=%db, coll=%coll, elapsed_ms=?t.elapsed().as_millis());
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
                                if let bson::Bson::Document(em) = val {
                                    if let Some(pred) = build_elem_match_pred(&path, em) {
                                        let jsonpath = format!("{}[*] ? ({})", path, pred);
                                        where_clauses.push(format!(
                                            "jsonb_path_exists(doc, '{}')",
                                            escape_single(&jsonpath)
                                        ));
                                    }
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
        // Prefer JSONB containment when filter is simple equality-only
        let rows = match build_where_spec(filter) {
            WhereSpec::Containment(val) => {
                let sql = format!(
                    "SELECT doc_bson, doc FROM {}.{} WHERE doc @> $1::jsonb ORDER BY id ASC LIMIT $2",
                    q_schema, q_table
                );
                client.query(&sql, &[&val, &limit]).await.map_err(err_msg)?
            }
            WhereSpec::Raw(where_sql) => {
                let sql = format!(
                    "SELECT doc_bson, doc FROM {}.{} WHERE {} ORDER BY id ASC LIMIT {}",
                    q_schema, q_table, where_sql, limit
                );
                client.query(&sql, &[]).await.map_err(err_msg)?
            }
        };
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
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_spec = filter.map(build_where_spec);
        let order_sql = build_order_by(sort);

        if let Some(proj_sql) = projection_pushdown_sql(projection) {
            let t = Instant::now();
            let client = self.pool.get().await.map_err(err_msg)?;
            let res = match &where_spec {
                Some(WhereSpec::Containment(val)) => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE doc @> $1::jsonb {} LIMIT $2",
                        proj_sql, q_schema, q_table, order_sql
                    );
                    client.query(&sql, &[val, &limit]).await
                }
                Some(WhereSpec::Raw(where_sql)) => {
                    let sql = format!(
                        "SELECT {} AS doc FROM {}.{} WHERE {} {} LIMIT {}",
                        proj_sql, q_schema, q_table, where_sql, order_sql, limit
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
            let res = match &where_spec {
                Some(WhereSpec::Containment(val)) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE doc @> $1::jsonb {} LIMIT $2",
                        q_schema, q_table, order_sql
                    );
                    client.query(&sql, &[val, &limit]).await
                }
                Some(WhereSpec::Raw(where_sql)) => {
                    let sql = format!(
                        "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                        q_schema, q_table, where_sql, order_sql, limit
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
                if let Some(bytes) = bson_bytes {
                    if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                        out.push(if let Some(p) = projection {
                            project_document(&doc, p)
                        } else {
                            doc
                        });
                        continue;
                    }
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
            if let Some(bytes) = bson_bytes {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    out.push(doc);
                    continue;
                }
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

    pub async fn count_docs(
        &self,
        db: &str,
        coll: &str,
        filter: Option<&bson::Document>,
    ) -> Result<i64> {
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
            if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1) {
                if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                    return Ok(Some((id, doc)));
                }
            }
            let json: serde_json::Value = r.get(2);
            let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
            let doc = match b {
                bson::Bson::Document(d) => d,
                _ => bson::Document::new(),
            };
            return Ok(Some((id, doc)));
        }
        let where_spec = build_where_spec(filter);
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = match where_spec {
            WhereSpec::Containment(val) => {
                let sql = format!(
                    "SELECT id, doc_bson, doc FROM {}.{} WHERE doc @> $1::jsonb ORDER BY id ASC LIMIT 1",
                    q_schema, q_table
                );
                client.query(&sql, &[&val]).await.map_err(err_msg)?
            }
            WhereSpec::Raw(where_sql) => {
                let sql = format!(
                    "SELECT id, doc_bson, doc FROM {}.{} WHERE {} ORDER BY id ASC LIMIT 1",
                    q_schema, q_table, where_sql
                );
                client.query(&sql, &[]).await.map_err(err_msg)?
            }
        };
        if rows.is_empty() {
            return Ok(None);
        }
        let r = &rows[0];
        let id: Vec<u8> = r.get(0);
        // Prefer bson if present
        if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1) {
            if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                return Ok(Some((id, doc)));
            }
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
        let where_spec = build_where_spec(filter);
        let select_sql = format!(
            "SELECT id FROM {}.{} WHERE {} ORDER BY id ASC LIMIT 1",
            q_schema,
            q_table,
            match &where_spec {
                WhereSpec::Raw(s) => s.as_str(),
                _ => "doc @> $1::jsonb",
            }
        );
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let rows = match &where_spec {
            WhereSpec::Containment(val) => {
                client.query(&select_sql, &[val]).await.map_err(err_msg)?
            }
            WhereSpec::Raw(_) => client.query(&select_sql, &[]).await.map_err(err_msg)?,
        };
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
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let where_spec = build_where_spec(filter);
        let t = Instant::now();
        let client = self.pool.get().await.map_err(err_msg)?;
        let n = match where_spec {
            WhereSpec::Containment(val) => {
                let sql = format!(
                    "DELETE FROM {}.{} WHERE doc @> $1::jsonb",
                    q_schema, q_table
                );
                client.execute(&sql, &[&val]).await.map_err(err_msg)?
            }
            WhereSpec::Raw(where_sql) => {
                let sql = format!("DELETE FROM {}.{} WHERE {}", q_schema, q_table, where_sql);
                client.execute(&sql, &[]).await.map_err(err_msg)?
            }
        };
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
    let mut where_clauses: Vec<String> = Vec::new();
    for (k, v) in filter.iter() {
        if k == "_id" {
            continue;
        }
        let path = jsonpath_path(k);
        match v {
            bson::Bson::Document(d) => {
                for (op, val) in d.iter() {
                    match op.as_str() {
                        "$elemMatch" => {
                            if let bson::Bson::Document(em) = val {
                                if let Some(pred) = build_elem_match_pred(&path, em) {
                                    where_clauses.push(format!(
                                        "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                        escape_single(&path),
                                        pred
                                    ));
                                }
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
    if where_clauses.is_empty() {
        String::from("TRUE")
    } else {
        where_clauses.join(" AND ")
    }
}

enum WhereSpec {
    Raw(String),
    Containment(serde_json::Value),
}

fn json_value_from_bson(v: &bson::Bson) -> Option<serde_json::Value> {
    match v {
        bson::Bson::Null => Some(serde_json::Value::Null),
        bson::Bson::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        bson::Bson::Int32(n) => Some(serde_json::Value::Number((*n).into())),
        bson::Bson::Int64(n) => Some(serde_json::Value::Number((*n).into())),
        bson::Bson::Double(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
        bson::Bson::String(s) => Some(serde_json::Value::String(s.clone())),
        _ => None,
    }
}

fn build_where_spec(filter: &bson::Document) -> WhereSpec {
    // Try simple equality-only pushdown using JSONB containment
    use serde_json::{Map, Value};
    let mut m = Map::new();
    for (k, v) in filter.iter() {
        if k == "_id" {
            continue;
        } // server has fast path; avoid ambiguity
        if k.contains('.') {
            return WhereSpec::Raw(build_where_from_filter(filter));
        }
        match v {
            bson::Bson::Document(d) => {
                if d.len() == 1 {
                    if let Some(eqv) = d.get("$eq") {
                        if let Some(jv) = json_value_from_bson(eqv) {
                            m.insert(k.clone(), jv);
                            continue;
                        } else {
                            return WhereSpec::Raw(build_where_from_filter(filter));
                        }
                    } else {
                        return WhereSpec::Raw(build_where_from_filter(filter));
                    }
                } else {
                    return WhereSpec::Raw(build_where_from_filter(filter));
                }
            }
            other => {
                if let Some(jv) = json_value_from_bson(other) {
                    m.insert(k.clone(), jv);
                } else {
                    return WhereSpec::Raw(build_where_from_filter(filter));
                }
            }
        }
    }
    if m.is_empty() {
        WhereSpec::Raw(build_where_from_filter(filter))
    } else {
        WhereSpec::Containment(Value::Object(m))
    }
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
    // only allow inclusive projections with possible _id exclusion
    let mut include_fields: Vec<String> = Vec::new();
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
        let on = match v {
            bson::Bson::Int32(n) => *n != 0,
            bson::Bson::Boolean(b) => *b,
            _ => false,
        };
        if on {
            // Only top-level fields qualify for pushdown; dotted paths require server-side projection
            if k.contains('.') {
                return None;
            }
            include_fields.push(k.clone());
        } else {
            return None;
        }
    }
    if include_fields.is_empty() && include_id {
        return None;
    }
    let mut elems: Vec<String> = Vec::new();
    if include_id {
        elems.push("'_id', doc->'_id'".to_string());
    }
    for f in include_fields {
        elems.push(format!(
            "'{}', doc->'{}'",
            escape_single(&f),
            escape_single(&f)
        ));
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
        bson::Bson::String(s) => Some(format!("{}", serde_json::to_string(s).ok()?)),
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
        if include_id {
            if let Some(idv) = doc.get("_id").cloned() {
                out.insert("_id", idv);
            }
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
            if on {
                if let Some(val) = get_path(doc, k) {
                    set_path(&mut out, k, val);
                }
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
        if !entry.as_document().is_some() {
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
                if let Ok(bytes) = r.try_get::<usize, Vec<u8>>(1) {
                    if let Ok(doc) = bson::Document::from_reader(&mut std::io::Cursor::new(bytes)) {
                        return Ok(Some((id, doc)));
                    }
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
