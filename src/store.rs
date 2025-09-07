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

    /// Limited top-level filter support: equality, $gt, $gte, $lt, $lte, $in, $exists (true/false)
    /// Comparisons operate on strings or double precision numbers based on value type.
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
        let mut eq_obj = serde_json::Map::new();

        for (k, v) in filter.iter() {
            // skip _id here; server handles id fast path
            if k == "_id" { continue; }
            match v {
                bson::Bson::Document(d) => {
                    // operators
                    for (op, val) in d.iter() {
                        match op.as_str() {
                            "$exists" => {
                                let exists = matches!(val, bson::Bson::Boolean(true));
                                let clause = if exists {
                                    format!("doc ? '{}'", escape_single(k))
                                } else {
                                    format!("NOT (doc ? '{}')", escape_single(k))
                                };
                                where_clauses.push(clause);
                            }
                            "$in" => {
                                if let bson::Bson::Array(arr) = val {
                                    if arr.is_empty() { where_clauses.push("FALSE".to_string()); continue; }
                                    if all_numeric(arr) {
                                        let list = join_numeric_array(arr);
                                        where_clauses.push(format!(
                                            "((doc->>'{}')::double precision) = ANY(ARRAY[{}]::double precision[])",
                                            escape_single(k), list
                                        ));
                                    } else if all_strings(arr) {
                                        let list = join_string_array(arr);
                                        where_clauses.push(format!(
                                            "(doc->>'{}') = ANY(ARRAY[{}]::text[])",
                                            escape_single(k), list
                                        ));
                                    }
                                }
                            }
                            "$gt" | "$gte" | "$lt" | "$lte" => {
                                let (op_sql, _is_inclusive) = match op.as_str() {
                                    "$gt" => (">", false),
                                    "$gte" => (">=", true),
                                    "$lt" => ("<", false),
                                    "$lte" => ("<=", true),
                                    _ => unreachable!(),
                                };
                                if let Some(num) = as_f64(val) {
                                    where_clauses.push(format!(
                                        "((doc->>'{}')::double precision) {} {}",
                                        escape_single(k), op_sql, num
                                    ));
                                } else if let Some(s) = val.as_str() {
                                    let val_sql = format!("'{}'", escape_single(s));
                                    where_clauses.push(format!(
                                        "(doc->>'{}') {} {}",
                                        escape_single(k), op_sql, val_sql
                                    ));
                                }
                            }
                            "$eq" => {
                                if let Some(json_val) = bson_to_json_value(val) {
                                    eq_obj.insert(k.clone(), json_val);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                // equality on scalar
                _ => {
                    if let Some(json_val) = bson_to_json_value(v) {
                        eq_obj.insert(k.clone(), json_val);
                    }
                }
            }
        }

        if !eq_obj.is_empty() {
            let json = serde_json::Value::Object(eq_obj);
            let json_str = json.to_string();
            where_clauses.push(format!("doc @> '{}'::jsonb", escape_single(&json_str)));
        }

        let where_sql = if where_clauses.is_empty() {
            String::from("TRUE")
        } else {
            where_clauses.join(" AND ")
        };

        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE {} ORDER BY id ASC LIMIT {}",
            q_schema, q_table, where_sql, limit
        );
        let rows = self.client.query(&sql, &[]).await.map_err(err_msg)?;
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
        let where_sql = filter.map(build_where_from_filter).unwrap_or_else(|| "TRUE".to_string());
        let order_sql = build_order_by(sort);

        if let Some(proj_sql) = projection_pushdown_sql(projection) {
            let sql = format!(
                "SELECT {} AS doc FROM {}.{} WHERE {} {} LIMIT {}",
                proj_sql, q_schema, q_table, where_sql, order_sql, limit
            );
            let rows = self.client.query(&sql, &[]).await.map_err(err_msg)?;
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let json: serde_json::Value = r.get(0);
                out.push(to_doc_from_json(json));
            }
            Ok(out)
        } else {
            let sql = format!(
                "SELECT doc_bson, doc FROM {}.{} WHERE {} {} LIMIT {}",
                q_schema, q_table, where_sql, order_sql, limit
            );
            let rows = self.client.query(&sql, &[]).await.map_err(err_msg)?;
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
                out.push(to_doc_from_json(json));
            }
            Ok(out)
        }
    }
    pub async fn find_by_subdoc(&self, db: &str, coll: &str, subdoc: &serde_json::Value, limit: i64) -> Result<Vec<bson::Document>> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let sql = format!(
            "SELECT doc_bson, doc FROM {}.{} WHERE doc @> $1::jsonb ORDER BY id ASC LIMIT $2",
            q_schema, q_table
        );
        let rows = self.client.query(&sql, &[&subdoc, &limit]).await.map_err(err_msg)?;
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

    pub async fn create_index_single_field(&self, db: &str, coll: &str, name: &str, field: &str, _order: i32, spec: &serde_json::Value) -> Result<()> {
        // Create an expression index on the extracted text value
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_table = q_ident(coll);
        let q_idx = q_ident(name);
        let field_escaped = field.replace('"', "\"\"");
        // Expression index requires parentheses around the expression inside the list parentheses
        // e.g., USING btree ((doc->>'field'))
        let expr = format!("((doc->>'{}'))", field_escaped);
        let ddl = format!("CREATE INDEX IF NOT EXISTS {} ON {}.{} USING btree {}", q_idx, q_schema, q_table, expr);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        // Persist metadata
        self.client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        Ok(())
    }

    pub async fn drop_index(&self, db: &str, coll: &str, name: &str) -> Result<bool> {
        let schema = schema_name(db);
        let q_schema = q_ident(&schema);
        let q_idx = q_ident(name);
        let ddl = format!("DROP INDEX IF EXISTS {}.{}", q_schema, q_idx);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        let n = self
            .client
            .execute("DELETE FROM mdb_meta.indexes WHERE db=$1 AND coll=$2 AND name=$3", &[&db, &coll, &name])
            .await
            .map_err(err_msg)?;
        Ok(n > 0)
    }

    pub async fn list_index_names(&self, db: &str, coll: &str) -> Result<Vec<String>> {
        let rows = self
            .client
            .query("SELECT name FROM mdb_meta.indexes WHERE db=$1 AND coll=$2", &[&db, &coll])
            .await
            .map_err(err_msg)?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    pub async fn create_index_compound(&self, db: &str, coll: &str, name: &str, fields: &[(String, i32)], spec: &serde_json::Value) -> Result<()> {
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
        let ddl = format!("CREATE INDEX IF NOT EXISTS {} ON {}.{} USING btree ({})", q_idx, q_schema, q_table, elems_joined);
        self.client.batch_execute(&ddl).await.map_err(err_msg)?;
        self.client
            .execute(
                "INSERT INTO mdb_meta.indexes(db, coll, name, spec, sql) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (db, coll, name) DO UPDATE SET spec = EXCLUDED.spec, sql = EXCLUDED.sql",
                &[&db, &coll, &name, &spec, &ddl],
            )
            .await
            .map_err(err_msg)?;
        Ok(())
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

fn escape_single(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

fn all_numeric(arr: &Vec<bson::Bson>) -> bool { arr.iter().all(|v| as_f64(v).is_some()) }
fn all_strings(arr: &Vec<bson::Bson>) -> bool { arr.iter().all(|v| matches!(v, bson::Bson::String(_))) }

fn join_numeric_array(arr: &Vec<bson::Bson>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(arr.len());
    for v in arr {
        parts.push(as_f64(v).unwrap().to_string());
    }
    parts.join(",")
}

fn join_string_array(arr: &Vec<bson::Bson>) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(arr.len());
    for v in arr {
        if let bson::Bson::String(s) = v { parts.push(format!("'{}'", escape_single(s))); }
    }
    parts.join(",")
}

fn as_f64(v: &bson::Bson) -> Option<f64> {
    match v {
        bson::Bson::Int32(n) => Some(*n as f64),
        bson::Bson::Int64(n) => Some(*n as f64),
        bson::Bson::Double(n) => Some(*n),
        _ => None,
    }
}

fn bson_to_json_value(v: &bson::Bson) -> Option<serde_json::Value> {
    match v {
        bson::Bson::Null => Some(serde_json::Value::Null),
        bson::Bson::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        bson::Bson::Int32(n) => Some(serde_json::Value::Number((*n).into())),
        bson::Bson::Int64(n) => serde_json::Number::from_f64(*n as f64).map(serde_json::Value::Number),
        bson::Bson::Double(n) => serde_json::Number::from_f64(*n).map(serde_json::Value::Number),
        bson::Bson::String(s) => Some(serde_json::Value::String(s.clone())),
        _ => None,
    }
}

fn build_where_from_filter(filter: &bson::Document) -> String {
    let mut where_clauses: Vec<String> = Vec::new();
    for (k, v) in filter.iter() {
        if k == "_id" { continue; }
        let path = jsonpath_path(k);
        match v {
            bson::Bson::Document(d) => {
                for (op, val) in d.iter() {
                    match op.as_str() {
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
                                    if let Some(lit) = json_literal_from_bson(item) { preds.push(format!("@ == {}", lit)); }
                                }
                                if preds.is_empty() { where_clauses.push("FALSE".to_string()); }
                                else {
                                    let predicate = preds.join(" || ");
                                    where_clauses.push(format!("jsonb_path_exists(doc, '{} ? ({}))", escape_single(&path), predicate));
                                }
                            }
                        }
                        "$gt" | "$gte" | "$lt" | "$lte" => {
                            let op_sql = match op.as_str() { "$gt" => ">", "$gte" => ">=", "$lt" => "<", "$lte" => "<=", _ => unreachable!() };
                            if let Some(lit) = json_literal_from_bson(val) {
                                where_clauses.push(format!("jsonb_path_exists(doc, '{} ? (@ {} {}))", escape_single(&path), op_sql, lit));
                            }
                        }
                        "$eq" => {
                            if let Some(lit) = json_literal_from_bson(val) {
                                where_clauses.push(format!("jsonb_path_exists(doc, '{} ? (@ == {}))", escape_single(&path), lit));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                if let Some(lit) = json_literal_from_bson(v) {
                    where_clauses.push(format!("jsonb_path_exists(doc, '{} ? (@ == {}))", escape_single(&path), lit));
                }
            }
        }
    }
    if where_clauses.is_empty() { String::from("TRUE") } else { where_clauses.join(" AND ") }
}

fn build_order_by(sort: Option<&bson::Document>) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut has_id = false;
    if let Some(spec) = sort {
        for (k, v) in spec.iter() {
            let dir = match v { bson::Bson::Int32(n) => *n, bson::Bson::Int64(n) => *n as i32, bson::Bson::Double(f) => if *f < 0.0 { -1 } else { 1 }, _ => 1 };
            let ord = if dir < 0 { "DESC" } else { "ASC" };
            if k == "_id" { has_id = true; parts.push(format!("id {}", ord)); }
            else {
                // Use JSONPath value as text for ordering; keep simple cast
                parts.push(format!("(doc->>'{}') {}", escape_single(k), ord));
            }
        }
    }
    if !has_id { parts.push("id ASC".to_string()); }
    format!("ORDER BY {}", parts.join(", "))
}

fn projection_pushdown_sql(projection: Option<&bson::Document>) -> Option<String> {
    let proj = projection?;
    if proj.is_empty() { return None; }
    // only allow inclusive projections with possible _id exclusion
    let mut include_fields: Vec<String> = Vec::new();
    let mut include_id = true;
    for (k, v) in proj.iter() {
        if k == "_id" {
            match v { bson::Bson::Int32(n) if *n == 0 => include_id = false, bson::Bson::Boolean(b) if !*b => include_id = false, _ => {} }
            continue;
        }
        let on = match v { bson::Bson::Int32(n) => *n != 0, bson::Bson::Boolean(b) => *b, _ => false };
        if on { include_fields.push(k.clone()); } else { return None; }
    }
    if include_fields.is_empty() && include_id { return None; }
    let mut elems: Vec<String> = Vec::new();
    if include_id { elems.push("'_id', doc->'_id'".to_string()); }
    for f in include_fields { elems.push(format!("'{}', doc->'{}'", escape_single(&f), escape_single(&f))); }
    if elems.is_empty() { return None; }
    Some(format!("jsonb_build_object({})", elems.join(", ")))
}

fn to_doc_from_json(json: serde_json::Value) -> bson::Document {
    let b = bson::to_bson(&json).unwrap_or(bson::Bson::Document(bson::Document::new()));
    match b { bson::Bson::Document(d) => d, _ => bson::Document::new() }
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
