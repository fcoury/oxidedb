use crate::aggregation::ast::AggregateStage;
use crate::error::{Error, Result};
use crate::translate::{
    build_order_by, build_where_from_filter, escape_single, projection_pushdown_sql,
};

struct SelectState {
    select: String,
    where_clauses: Vec<String>,
    order: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    group_by: Option<String>,
}

impl Default for SelectState {
    fn default() -> Self {
        Self {
            select: "doc_bson, doc".to_string(),
            where_clauses: Vec::new(),
            order: None,
            limit: None,
            offset: None,
            group_by: None,
        }
    }
}

pub struct SqlBuilder {
    stages: Vec<AggregateStage>,
    ctes: Vec<(String, String)>,
    current_cte: String,
    state: SelectState,
    cte_counter: usize,
}

impl SqlBuilder {
    pub fn new(db: &str, coll: &str, stages: Vec<AggregateStage>) -> Self {
        let schema = format!("\"mdb_{}\"", db.replace("\"", "\"\""));
        let table = format!("\"{}\"", coll.replace("\"", "\"\""));
        let initial_table = format!("{}.{}", schema, table);

        Self {
            stages,
            ctes: Vec::new(),
            current_cte: initial_table,
            state: SelectState::default(),
            cte_counter: 0,
        }
    }

    fn flush_cte(&mut self) {
        let cte_name = format!("cte_{}", self.cte_counter);
        self.cte_counter += 1;

        let where_sql = if self.state.where_clauses.is_empty() {
            "TRUE".to_string()
        } else {
            self.state.where_clauses.join(" AND ")
        };

        let order_sql = self
            .state
            .order
            .clone()
            .unwrap_or_else(|| "ORDER BY id ASC".to_string());
        let limit_sql = self
            .state
            .limit
            .map(|n| format!("LIMIT {}", n))
            .unwrap_or_default();
        let offset_sql = self
            .state
            .offset
            .map(|n| format!("OFFSET {}", n))
            .unwrap_or_default();
        let group_sql = self
            .state
            .group_by
            .as_ref()
            .map(|s| format!("GROUP BY {}", s))
            .unwrap_or_default();

        let sql = format!(
            "SELECT {} FROM {} WHERE {} {} {} {} {}",
            self.state.select,
            self.current_cte,
            where_sql,
            group_sql,
            order_sql,
            limit_sql,
            offset_sql
        );

        self.ctes.push((cte_name.clone(), sql));
        self.current_cte = cte_name;
        self.state = SelectState::default();
        // Reset select to just "doc" usually, since intermediate CTEs might have dropped doc_bson
        // But for safety, let's stick to "doc" as the primary carrier.
        self.state.select = "id, doc".to_string();
    }

    pub fn build(&mut self) -> Result<String> {
        let stages = std::mem::take(&mut self.stages);
        for stage in stages {
            match stage {
                AggregateStage::Match(doc) => {
                    // Check for $text operator - not supported in aggregation $match via SQL translation
                    if doc.contains_key("$text") {
                        return Err(Error::Msg(
                            "$text is not supported in aggregation $match stages".into(),
                        ));
                    }
                    // If we have grouping/limit/offset/project, we must flush first
                    if self.state.group_by.is_some()
                        || self.state.limit.is_some()
                        || self.state.offset.is_some()
                        || (self.state.select != "doc_bson, doc" && self.state.select != "id, doc")
                    {
                        self.flush_cte();
                    }
                    let w = build_where_from_filter(&doc);
                    self.state.where_clauses.push(format!("({})", w));
                }
                AggregateStage::Sort(doc) => {
                    if self.state.limit.is_some() || self.state.offset.is_some() {
                        self.flush_cte();
                    }
                    self.state.order = Some(build_order_by(Some(&doc)));
                }
                AggregateStage::Limit(n) => {
                    if self.state.limit.is_some() {
                        // Combine limits? min(old, new). For now flush.
                        self.flush_cte();
                    }
                    self.state.limit = Some(n);
                }
                AggregateStage::Skip(n) => {
                    if self.state.limit.is_some() || self.state.offset.is_some() {
                        self.flush_cte();
                    }
                    self.state.offset = Some(n);
                }
                AggregateStage::Project(doc) => {
                    if self.state.select != "doc_bson, doc" && self.state.select != "id, doc" {
                        self.flush_cte();
                    }
                    if let Some(sql) = projection_pushdown_sql(Some(&doc)) {
                        self.state.select = format!("id, {} AS doc", sql); // preserve id for stability
                    } else {
                        return Err(Error::Msg("Projection too complex for SQL pushdown".into()));
                    }
                }
                AggregateStage::Unwind(path, preserve, include) => {
                    self.flush_cte();

                    // Generate lateral join
                    let join_type = if preserve { "LEFT JOIN" } else { "CROSS JOIN" };

                    let ordinality = if let Some(ref idx_field) = include {
                        format!("WITH ORDINALITY AS elem(value, {})", idx_field)
                    } else {
                        "AS elem(value)".to_string()
                    };

                    // Correct path format for jsonb_array_elements path
                    let path_segments: Vec<String> = path.split('.').map(escape_single).collect();
                    let path_pg_fmt = format!("{{{}}}", path_segments.join(","));

                    let join_clause = format!(
                        "{} LATERAL jsonb_array_elements(doc #> '{}') {}",
                        join_type,
                        escape_single(&path_pg_fmt),
                        ordinality
                    );

                    // Post-unwind select list construction
                    let set_path = path.split('.').collect::<Vec<&str>>();
                    let pg_path = format!("'{{{}}}'", set_path.join(","));

                    let mut new_doc_expr = format!("jsonb_set(doc, {}, elem.value)", pg_path);

                    if let Some(idx_field) = include {
                        new_doc_expr = format!(
                            "jsonb_set({}, '{{{}}}', to_jsonb(elem.{} - 1))",
                            new_doc_expr, idx_field, idx_field
                        );
                    }

                    let cte_name = format!("cte_{}", self.cte_counter);
                    self.cte_counter += 1;

                    let sql = format!(
                        "SELECT id, {} AS doc FROM {} {}",
                        new_doc_expr, self.current_cte, join_clause
                    );

                    self.ctes.push((cte_name.clone(), sql));
                    self.current_cte = cte_name;
                    self.state = SelectState::default();
                    self.state.select = "id, doc".to_string();
                }
                AggregateStage::Group(spec) => {
                    self.flush_cte();
                    // Parse group spec
                    let id_expr = spec
                        .get("_id")
                        .ok_or(Error::Msg("Group missing _id".into()))?;

                    let group_key_sql = translate_expr(id_expr)
                        .ok_or(Error::Msg("Unsupported group _id expr".into()))?;

                    let mut json_pairs = Vec::new();
                    json_pairs.push("_id".to_string());
                    json_pairs.push(group_key_sql.clone());

                    for (k, v) in spec.iter() {
                        if k == "_id" {
                            continue;
                        }
                        if let bson::Bson::Document(d) = v
                            && let Some(op) = d.keys().next()
                        {
                            let val = d.get(op).unwrap();
                            let sql_expr = translate_expr(val).unwrap_or("NULL".to_string());
                            let acc_sql = match op.as_str() {
                                "$sum" => format!("SUM(({})::numeric)", sql_expr),
                                "$avg" => format!("AVG(({})::numeric)", sql_expr),
                                "$min" => format!("MIN({})", sql_expr),
                                "$max" => format!("MAX({})", sql_expr),
                                "$first" => format!("(array_agg({}) ORDER BY id)[1]", sql_expr),
                                "$last" => {
                                    format!("(array_agg({}) ORDER BY id DESC)[1]", sql_expr)
                                }
                                "$push" => format!("jsonb_agg({})", sql_expr),
                                "$addToSet" => format!("jsonb_agg(DISTINCT {})", sql_expr),
                                "$count" => "COUNT(*)".to_string(),
                                _ => {
                                    return Err(Error::Msg(format!(
                                        "Unsupported accumulator {}",
                                        op
                                    )));
                                }
                            };
                            json_pairs.push(k.to_string());
                            json_pairs.push(acc_sql);
                        }
                    }

                    let doc_expr = format!("jsonb_build_object({})", json_pairs.join(", "));

                    let cte_name = format!("cte_{}", self.cte_counter);
                    self.cte_counter += 1;

                    let group_clause = if group_key_sql == "NULL" || group_key_sql == "null" {
                        "".to_string()
                    } else {
                        format!("GROUP BY {}", group_key_sql)
                    };

                    let sql = format!(
                        "SELECT MIN(id) as id, {} AS doc FROM {} {}",
                        doc_expr, self.current_cte, group_clause
                    );

                    self.ctes.push((cte_name.clone(), sql));
                    self.current_cte = cte_name;
                    self.state = SelectState::default();
                    self.state.select = "id, doc".to_string();
                }
                AggregateStage::ReplaceRoot(doc) => {
                    if self.state.group_by.is_some()
                        || self.state.limit.is_some()
                        || self.state.offset.is_some()
                    {
                        self.flush_cte();
                    }

                    let new_root = doc
                        .get("newRoot")
                        .ok_or(Error::Msg("$replaceRoot missing newRoot field".into()))?;

                    let new_root_sql = match new_root {
                        bson::Bson::String(s) if s.starts_with('$') => {
                            let path = &s[1..];
                            let segs: Vec<String> = path.split('.').map(escape_single).collect();
                            let path_str = segs.join("','");
                            format!("doc #> '{{\"{}\"}}'", path_str)
                        }
                        bson::Bson::Document(d) => {
                            let mut json_pairs = Vec::new();
                            for (k, v) in d.iter() {
                                if let Some(sql_expr) = translate_expr(v) {
                                    json_pairs.push(format!(
                                        "'{}', {}",
                                        escape_single(k),
                                        sql_expr
                                    ));
                                } else {
                                    return Err(Error::Msg(format!(
                                        "Unsupported expression in $replaceRoot newRoot: {:?}",
                                        v
                                    )));
                                }
                            }
                            format!("jsonb_build_object({})", json_pairs.join(", "))
                        }
                        _ => {
                            return Err(Error::Msg(
                                "newRoot must be a path string or document".into(),
                            ));
                        }
                    };

                    self.state.select = format!("id, {} AS doc", new_root_sql);
                }
                AggregateStage::Facet(_) => {
                    return Err(Error::Msg(
                        "$facet requires engine fallback (multiple sub-pipelines)".into(),
                    ));
                }
                AggregateStage::Sample(n) => {
                    if self.state.limit.is_some()
                        || self.state.offset.is_some()
                        || self.state.group_by.is_some()
                    {
                        self.flush_cte();
                    }
                    self.state.order = Some("ORDER BY random()".to_string());
                    self.state.limit = Some(n);
                }
                AggregateStage::UnionWith(_) => {
                    return Err(Error::Msg(
                        "$unionWith requires engine fallback (UNION ALL)".into(),
                    ));
                }
                AggregateStage::Out(_) => {
                    return Err(Error::Msg(
                        "$out requires special handling beyond SQL pushdown".into(),
                    ));
                }
                AggregateStage::Merge(_) => {
                    return Err(Error::Msg(
                        "$merge requires special handling beyond SQL pushdown".into(),
                    ));
                }
                AggregateStage::GeoNear(spec) => {
                    // $geoNear must be first stage - this is enforced at pipeline validation time
                    // Build distance calculation SQL
                    let jsonb_path = format!(
                        "doc->'{}'",
                        spec.key.replace('"', "\"\"").replace('\'', "''")
                    );

                    let distance_expr = if spec.spherical {
                        // Haversine distance in meters
                        format!(
                            "6371000 * acos(
                                cos(radians({})) * cos(radians(({}->'coordinates'->>1)::double precision)) *
                                cos(radians(({}->'coordinates'->>0)::double precision) - radians({})) +
                                sin(radians({})) * sin(radians(({}->'coordinates'->>1)::double precision))
                            )",
                            spec.near_lat, jsonb_path, jsonb_path, spec.near_lon, spec.near_lat, jsonb_path
                        )
                    } else {
                        // Simple Euclidean distance
                        format!(
                            "sqrt(
                                power(({}->'coordinates'->>0)::double precision - {}, 2) +
                                power(({}->'coordinates'->>1)::double precision - {}, 2)
                            )",
                            jsonb_path, spec.near_lon, jsonb_path, spec.near_lat
                        )
                    };

                    // Build WHERE clause with query filter and maxDistance
                    let mut where_clauses = vec![
                        format!(
                            "jsonb_path_exists(doc, '$.\"{}\".type ? (@ == \"Point\")')",
                            spec.key
                        ),
                        format!("jsonb_path_exists(doc, '$.\"{}\".coordinates')", spec.key),
                    ];

                    // Add query filter if present
                    if let Some(ref query) = spec.query {
                        let query_sql = build_where_from_filter(query);
                        if query_sql != "TRUE" {
                            where_clauses.push(query_sql);
                        }
                    }

                    // Add maxDistance filter if present
                    if let Some(max_dist) = spec.max_distance {
                        where_clauses.push(format!("{} <= {}", distance_expr, max_dist));
                    }

                    let where_sql = where_clauses.join(" AND ");

                    // Build the CTE for $geoNear
                    let cte_name = format!("cte_{}", self.cte_counter);
                    self.cte_counter += 1;

                    // Select includes the distance field
                    let sql = format!(
                        "SELECT id, doc, {} AS \"{}\" FROM {} WHERE {} ORDER BY {} ASC",
                        distance_expr,
                        spec.distance_field,
                        self.current_cte,
                        where_sql,
                        distance_expr
                    );

                    self.ctes.push((cte_name.clone(), sql));
                    self.current_cte = cte_name;
                    self.state = SelectState::default();
                    self.state.select = "id, doc".to_string();
                }
            }
        }

        // Final flush
        self.flush_cte();

        // Construct WITH ... SELECT
        let mut full_sql = "WITH ".to_string();
        for (i, (name, body)) in self.ctes.iter().enumerate() {
            if i > 0 {
                full_sql.push_str(",\n ");
            }
            full_sql.push_str(&format!("{} AS ({})", name, body));
        }
        full_sql.push_str(&format!("\nSELECT doc FROM {}", self.current_cte));

        Ok(full_sql)
    }
}

fn translate_expr(v: &bson::Bson) -> Option<String> {
    match v {
        bson::Bson::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            // Escape single quotes in path segments
            let segs: Vec<String> = path.split('.').map(escape_single).collect();
            let path_str = segs.join("','"); // e.g. 'a','b'
            Some(format!("doc #> '{{\"{}\"}}'", path_str))
        }
        bson::Bson::Int32(n) => Some(n.to_string()),
        bson::Bson::Int64(n) => Some(n.to_string()),
        bson::Bson::Double(f) => Some(f.to_string()),
        bson::Bson::Boolean(b) => Some(if *b { "true".into() } else { "false".into() }),
        bson::Bson::Null => Some("NULL".into()),
        _ => None,
    }
}
