use crate::aggregation::memory::MemoryManager;
use crate::aggregation::pipeline::{Pipeline, Stage};
use crate::store::PgStore;
use bson::{Bson, Document};
use std::collections::HashMap;

const MAX_MATERIALIZED_DOCUMENTS: i64 = 100_000;
pub(crate) const MATERIALIZED_FETCH_LIMIT: i64 = MAX_MATERIALIZED_DOCUMENTS + 1;

/// Execution context for pipeline
pub struct ExecContext<'a> {
    pub pg: Option<&'a PgStore>,
    pub db: String,
    pub coll: String,
    pub memory: MemoryManager,
    pub vars: HashMap<String, Bson>,
}

impl<'a> ExecContext<'a> {
    pub fn new(pg: Option<&'a PgStore>, db: String, coll: String, allow_disk_use: bool) -> Self {
        Self {
            pg,
            db,
            coll,
            memory: MemoryManager::new(allow_disk_use),
            vars: HashMap::new(),
        }
    }

    pub fn with_vars(
        pg: Option<&'a PgStore>,
        db: String,
        coll: String,
        allow_disk_use: bool,
        vars: HashMap<String, Bson>,
    ) -> Self {
        Self {
            pg,
            db,
            coll,
            memory: MemoryManager::new(allow_disk_use),
            vars,
        }
    }
}

/// Execution result
pub enum ExecResult {
    Cursor(Vec<Document>),
    WriteOut(WriteStats),
}

/// Write statistics for $out/$merge
#[derive(Debug, Default)]
pub struct WriteStats {
    pub matched_count: i64,
    pub modified_count: i64,
    pub inserted_count: i64,
    pub deleted_count: i64,
}

/// Execute a pipeline
pub async fn execute_pipeline(
    ctx: &ExecContext<'_>,
    pipeline: Pipeline,
) -> anyhow::Result<ExecResult> {
    let mut docs: Vec<Document> = Vec::new();
    let mut main_coll_fetched = false;

    for stage in pipeline.stages {
        // Fetch collection if not yet fetched and this is not a $match/$geoNear stage
        if !main_coll_fetched
            && !matches!(stage, Stage::Match(_) | Stage::GeoNear(_))
            && let Some(pg) = ctx.pg
        {
            docs = pg
                .find_docs(
                    &ctx.db,
                    &ctx.coll,
                    None,
                    None,
                    None,
                    MATERIALIZED_FETCH_LIMIT,
                )
                .await?;
            ensure_document_limit(&docs)?;
            main_coll_fetched = true;
        }

        match stage {
            Stage::Match(filter) => {
                if !main_coll_fetched {
                    // First match - fetch from collection with filter
                    if let Some(pg) = ctx.pg {
                        docs = pg
                            .find_docs(
                                &ctx.db,
                                &ctx.coll,
                                Some(&filter),
                                None,
                                None,
                                MATERIALIZED_FETCH_LIMIT,
                            )
                            .await?;
                        ensure_document_limit(&docs)?;
                        main_coll_fetched = true;
                    }
                } else {
                    // Filter existing docs
                    docs.retain(|d| document_matches_filter(d, &filter, &ctx.vars));
                }
            }
            Stage::Project(spec) => {
                docs = crate::aggregation::stages::project::execute(docs, &spec, &ctx.vars)?;
            }
            Stage::AddFields(spec) => {
                docs = crate::aggregation::stages::add_fields::execute(docs, &spec, &ctx.vars)?;
            }
            Stage::Set(spec) => {
                docs = crate::aggregation::stages::set::execute(docs, &spec, &ctx.vars)?;
            }
            Stage::Unset(fields) => {
                docs = crate::aggregation::stages::unset::execute(docs, &fields)?;
            }
            Stage::ReplaceRoot { replacement } => {
                docs = crate::aggregation::stages::replace_root::execute(
                    docs,
                    &replacement,
                    &ctx.vars,
                )?;
            }
            Stage::ReplaceWith(replacement) => {
                docs = crate::aggregation::stages::replace_root::execute(
                    docs,
                    &replacement,
                    &ctx.vars,
                )?;
            }
            Stage::Sort(spec) => {
                docs = crate::aggregation::stages::sort::execute(docs, &spec)?;
            }
            Stage::Limit(n) => {
                docs = crate::aggregation::stages::limit::execute(docs, n)?;
            }
            Stage::Skip(n) => {
                docs = crate::aggregation::stages::skip::execute(docs, n)?;
            }
            Stage::Count(field) => {
                docs = crate::aggregation::stages::count::execute(docs, &field)?;
            }
            Stage::Group { id, accumulators } => {
                docs = crate::aggregation::stages::group::execute(
                    docs,
                    &id,
                    &accumulators,
                    &ctx.vars,
                )?;
            }
            Stage::Bucket {
                group_by,
                boundaries,
                default,
                output,
            } => {
                docs = crate::aggregation::stages::bucket::execute(
                    docs,
                    &group_by,
                    &boundaries,
                    default.as_ref(),
                    output.as_ref(),
                    &ctx.vars,
                )?;
            }
            Stage::BucketAuto {
                group_by,
                buckets,
                granularity,
                output,
            } => {
                docs = crate::aggregation::stages::bucket_auto::execute(
                    docs,
                    &group_by,
                    buckets,
                    granularity.as_deref(),
                    output.as_ref(),
                    &ctx.vars,
                )?;
            }
            Stage::Lookup {
                from,
                local_field,
                foreign_field,
                as_field,
                let_vars,
                pipeline,
            } => {
                if let Some(pg) = ctx.pg {
                    docs = crate::aggregation::stages::lookup::execute(
                        docs,
                        pg,
                        &ctx.db,
                        &from,
                        local_field.as_deref(),
                        foreign_field.as_deref(),
                        &as_field,
                        let_vars.as_ref(),
                        pipeline.as_ref(),
                        &ctx.vars,
                    )
                    .await?;
                }
            }
            Stage::Unwind {
                path,
                include_array_index,
                preserve_null_and_empty_arrays,
            } => {
                docs = crate::aggregation::stages::unwind::execute(
                    docs,
                    &path,
                    include_array_index.as_deref(),
                    preserve_null_and_empty_arrays,
                )?;
            }
            Stage::Sample(size) => {
                docs = crate::aggregation::stages::sample::execute(docs, size)?;
            }
            Stage::Facet(facets) => {
                docs = crate::aggregation::stages::facet::execute(docs, &facets, &ctx.vars)?;
            }
            Stage::UnionWith {
                coll,
                pipeline: union_pipeline,
            } => {
                if let Some(pg) = ctx.pg {
                    docs = crate::aggregation::stages::union_with::execute(
                        docs,
                        pg,
                        &ctx.db,
                        &coll,
                        &union_pipeline,
                        &ctx.vars,
                    )
                    .await?;
                }
            }
            Stage::GeoNear(spec) => {
                if let Some(pg) = ctx.pg {
                    docs = crate::aggregation::stages::geo_near::execute(
                        docs, pg, &ctx.db, &ctx.coll, &spec,
                    )
                    .await?;
                    main_coll_fetched = true;
                }
            }
            Stage::Out(target_coll) => {
                if let Some(pg) = ctx.pg {
                    let stats =
                        crate::aggregation::stages::out::execute(docs, pg, &ctx.db, &target_coll)
                            .await?;
                    return Ok(ExecResult::WriteOut(stats));
                }
            }
            Stage::Merge(spec) => {
                if let Some(pg) = ctx.pg {
                    let stats =
                        crate::aggregation::stages::merge::execute(docs, pg, &ctx.db, &spec)
                            .await?;
                    return Ok(ExecResult::WriteOut(stats));
                }
            }
            Stage::SortByCount(expr) => {
                docs = crate::aggregation::stages::sort_by_count::execute(docs, &expr, &ctx.vars)?;
            }
            Stage::SetWindowFields(spec) => {
                docs =
                    crate::aggregation::stages::set_window_fields::execute(docs, &spec, &ctx.vars)?;
            }
            Stage::Densify(spec) => {
                docs = crate::aggregation::stages::densify::execute(docs, &spec)?;
            }
            Stage::Fill(spec) => {
                docs = crate::aggregation::stages::fill::execute(docs, &spec)?;
            }
            Stage::Redact(expr) => {
                docs = crate::aggregation::stages::redact::execute(docs, &expr, &ctx.vars)?;
            }
        }
    }

    Ok(ExecResult::Cursor(docs))
}

pub(crate) fn ensure_document_limit(docs: &[Document]) -> anyhow::Result<()> {
    if docs.len() as i64 > MAX_MATERIALIZED_DOCUMENTS {
        anyhow::bail!(
            "aggregation input exceeds in-memory limit of {} documents",
            MAX_MATERIALIZED_DOCUMENTS
        );
    }
    Ok(())
}

/// Check if document matches filter using MongoDB matching semantics:
/// dotted paths, array-element matching, cross-type numeric comparison,
/// `$and`/`$or`/`$nor`/`$expr` and field-level operators.
pub(crate) fn document_matches_filter(
    doc: &Document,
    filter: &Document,
    vars: &HashMap<String, Bson>,
) -> bool {
    for (key, value) in filter.iter() {
        if key.starts_with('$') {
            match key.as_str() {
                "$and" => {
                    if let Bson::Array(arr) = value {
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond
                                && !document_matches_filter(doc, cond_doc, vars)
                            {
                                return false;
                            }
                        }
                    }
                }
                "$or" => {
                    if let Bson::Array(arr) = value {
                        let any_match = arr.iter().any(|cond| {
                            matches!(cond, Bson::Document(cond_doc)
                                if document_matches_filter(doc, cond_doc, vars))
                        });
                        if !any_match {
                            return false;
                        }
                    }
                }
                "$nor" => {
                    if let Bson::Array(arr) = value {
                        let any_match = arr.iter().any(|cond| {
                            matches!(cond, Bson::Document(cond_doc)
                                if document_matches_filter(doc, cond_doc, vars))
                        });
                        if any_match {
                            return false;
                        }
                    }
                }
                "$expr" => {
                    if !eval_match_expr(doc, value, vars) {
                        return false;
                    }
                }
                _ => {}
            }
        } else {
            // Field match
            let mut candidates = Vec::new();
            collect_path_values(
                &Bson::Document(doc.clone()),
                &key.split('.').collect::<Vec<_>>(),
                &mut candidates,
            );
            let field_exists = !candidates.is_empty();
            if candidates.is_empty() {
                // Missing field behaves like null for equality/comparison
                candidates.push(Bson::Null);
            }
            if !value_matches(&candidates, field_exists, value) {
                return false;
            }
        }
    }
    true
}

/// Evaluate an `$expr` expression against a document; errors count as no match.
fn eval_match_expr(doc: &Document, expr_bson: &Bson, vars: &HashMap<String, Bson>) -> bool {
    use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};

    let Ok(expr) = parse_expr(expr_bson) else {
        return false;
    };
    let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
    match eval_expr(&expr, &ctx) {
        Ok(val) => is_truthy_bson(&val),
        Err(_) => false,
    }
}

fn is_truthy_bson(val: &Bson) -> bool {
    match val {
        Bson::Boolean(b) => *b,
        Bson::Int32(n) => *n != 0,
        Bson::Int64(n) => *n != 0,
        Bson::Double(n) => *n != 0.0 && !n.is_nan(),
        Bson::Null | Bson::Undefined => false,
        _ => true,
    }
}

/// Collect candidate values for a dotted path, expanding arrays one level at
/// each step. The raw value at each path endpoint is included so that exact
/// array equality still works alongside element matching.
fn collect_path_values(val: &Bson, parts: &[&str], out: &mut Vec<Bson>) {
    if parts.is_empty() {
        out.push(val.clone());
        if let Bson::Array(arr) = val {
            for item in arr {
                out.push(item.clone());
            }
        }
        return;
    }
    match val {
        Bson::Document(d) => {
            if let Some(next) = d.get(parts[0]) {
                collect_path_values(next, &parts[1..], out);
            }
        }
        Bson::Array(arr) => {
            // Numeric segment indexes into the array
            if let Ok(idx) = parts[0].parse::<usize>()
                && let Some(next) = arr.get(idx)
            {
                collect_path_values(next, &parts[1..], out);
            }
            // Each element is a candidate path continuation
            for item in arr {
                collect_path_values(item, parts, out);
            }
        }
        _ => {}
    }
}

/// Range predicates compare array *elements*, never the raw array itself
/// (a whole array would outrank every scalar by BSON type order).
fn range_candidates(candidates: &[Bson]) -> impl Iterator<Item = &Bson> {
    candidates.iter().filter(|c| !matches!(c, Bson::Array(_)))
}

/// Check field candidates against a filter value.
///
/// `candidates` holds the field value plus expanded array elements (a single
/// Null when the field is missing). `field_exists` distinguishes a missing
/// field from an explicit null for `$exists`.
pub(crate) fn value_matches(candidates: &[Bson], field_exists: bool, filter_val: &Bson) -> bool {
    use crate::aggregation::values::{bson_cmp, bson_eq};

    match filter_val {
        Bson::Document(filter_doc) if filter_doc.keys().any(|k| k.starts_with('$')) => {
            for (op, op_val) in filter_doc.iter() {
                match op.as_str() {
                    "$eq" => {
                        if !candidates.iter().any(|c| bson_eq(c, op_val)) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if candidates.iter().any(|c| bson_eq(c, op_val)) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if !range_candidates(candidates)
                            .any(|c| bson_cmp(c, op_val) == std::cmp::Ordering::Greater)
                        {
                            return false;
                        }
                    }
                    "$gte" => {
                        if !range_candidates(candidates).any(|c| {
                            let cmp = bson_cmp(c, op_val);
                            cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
                        }) {
                            return false;
                        }
                    }
                    "$lt" => {
                        if !range_candidates(candidates)
                            .any(|c| bson_cmp(c, op_val) == std::cmp::Ordering::Less)
                        {
                            return false;
                        }
                    }
                    "$lte" => {
                        if !range_candidates(candidates).any(|c| {
                            let cmp = bson_cmp(c, op_val);
                            cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
                        }) {
                            return false;
                        }
                    }
                    "$in" => {
                        if let Bson::Array(arr) = op_val {
                            let any = candidates.iter().any(|c| arr.iter().any(|v| bson_eq(c, v)));
                            if !any {
                                return false;
                            }
                        }
                    }
                    "$nin" => {
                        if let Bson::Array(arr) = op_val {
                            let any = candidates.iter().any(|c| arr.iter().any(|v| bson_eq(c, v)));
                            if any {
                                return false;
                            }
                        }
                    }
                    "$exists" => {
                        let should_exist = op_val.as_bool().unwrap_or(true);
                        if should_exist != field_exists {
                            return false;
                        }
                    }
                    "$regex" => {
                        // Simple substring match for now
                        let pattern = match op_val {
                            Bson::String(p) => p,
                            _ => return false,
                        };
                        let any = candidates.iter().any(|c| match c {
                            Bson::String(s) => s.contains(pattern),
                            _ => false,
                        });
                        if !any {
                            return false;
                        }
                    }
                    "$not" => {
                        if let Bson::Document(inner) = op_val
                            && value_matches(
                                candidates,
                                field_exists,
                                &Bson::Document(inner.clone()),
                            )
                        {
                            return false;
                        }
                    }
                    _ => {}
                }
            }
            true
        }
        _ => candidates.iter().any(|c| bson_eq(c, filter_val)),
    }
}

#[cfg(test)]
mod tests {
    use super::document_matches_filter;
    use bson::{Bson, Document, doc};
    use std::collections::HashMap;

    fn no_vars() -> HashMap<String, Bson> {
        HashMap::new()
    }

    fn matches(doc: &Document, filter: &Document) -> bool {
        document_matches_filter(doc, filter, &no_vars())
    }

    #[test]
    fn numeric_cross_type_equality() {
        let d = doc! { "a": 1i32 };
        assert!(matches(&d, &doc! { "a": 1i64 }));
        assert!(matches(&d, &doc! { "a": 1.0f64 }));
        assert!(!matches(&d, &doc! { "a": 2i64 }));
        assert!(matches(&d, &doc! { "a": { "$ne": 2.0f64 } }));
        assert!(!matches(&d, &doc! { "a": { "$ne": 1i64 } }));
    }

    #[test]
    fn numeric_cross_type_ranges() {
        let d = doc! { "a": 5i32 };
        assert!(matches(&d, &doc! { "a": { "$gt": 4i64 } }));
        assert!(matches(&d, &doc! { "a": { "$gte": 5.0f64 } }));
        assert!(matches(&d, &doc! { "a": { "$lt": 6i64 } }));
        assert!(matches(&d, &doc! { "a": { "$lte": 5.0f64 } }));
        assert!(!matches(&d, &doc! { "a": { "$gt": 5i64 } }));
    }

    #[test]
    fn dotted_paths() {
        let d = doc! { "a": { "b": { "c": 7i32 } } };
        assert!(matches(&d, &doc! { "a.b.c": 7i32 }));
        assert!(!matches(&d, &doc! { "a.b.c": 8i32 }));
        assert!(!matches(&d, &doc! { "a.x.c": 7i32 }));
    }

    #[test]
    fn array_element_matching() {
        let d = doc! { "tags": ["red", "blue"] };
        // Scalar matches any element
        assert!(matches(&d, &doc! { "tags": "red" }));
        assert!(!matches(&d, &doc! { "tags": "green" }));
        // Exact array equality still works
        assert!(matches(&d, &doc! { "tags": ["red", "blue"] }));
        // $in matches any element
        assert!(matches(&d, &doc! { "tags": { "$in": ["green", "blue"] } }));
        assert!(!matches(
            &d,
            &doc! { "tags": { "$in": ["green", "yellow"] } }
        ));
        // Range matches any element
        let scores = doc! { "scores": [3i32, 7i32] };
        assert!(matches(&scores, &doc! { "scores": { "$gt": 5i32 } }));
        assert!(!matches(&scores, &doc! { "scores": { "$gt": 10i32 } }));
    }

    #[test]
    fn dotted_path_through_array_of_docs() {
        let d = doc! { "items": [ { "sku": "a" }, { "sku": "b" } ] };
        assert!(matches(&d, &doc! { "items.sku": "b" }));
        assert!(!matches(&d, &doc! { "items.sku": "c" }));
        // Numeric segment indexes into the array
        assert!(matches(&d, &doc! { "items.0.sku": "a" }));
        assert!(!matches(&d, &doc! { "items.1.sku": "a" }));
    }

    #[test]
    fn null_and_missing_semantics() {
        let d = doc! { "a": Bson::Null, "b": 1i32 };
        // Null literal matches null and missing
        assert!(matches(&d, &doc! { "a": Bson::Null }));
        assert!(matches(&d, &doc! { "missing": Bson::Null }));
        // $exists distinguishes the two
        assert!(matches(&d, &doc! { "a": { "$exists": true } }));
        assert!(!matches(&d, &doc! { "missing": { "$exists": true } }));
        assert!(matches(&d, &doc! { "missing": { "$exists": false } }));
        // $ne: null requires an existing, non-null value
        assert!(matches(&d, &doc! { "b": { "$ne": Bson::Null } }));
        assert!(!matches(&d, &doc! { "a": { "$ne": Bson::Null } }));
    }

    #[test]
    fn logical_operators() {
        let d = doc! { "a": 1i32, "b": 2i32 };
        assert!(matches(
            &d,
            &doc! { "$and": [ { "a": 1i32 }, { "b": 2i32 } ] }
        ));
        assert!(!matches(
            &d,
            &doc! { "$and": [ { "a": 1i32 }, { "b": 3i32 } ] }
        ));
        assert!(matches(
            &d,
            &doc! { "$or": [ { "a": 9i32 }, { "b": 2i32 } ] }
        ));
        assert!(matches(
            &d,
            &doc! { "$nor": [ { "a": 9i32 }, { "b": 9i32 } ] }
        ));
        assert!(!matches(&d, &doc! { "$nor": [ { "a": 1i32 } ] }));
    }

    #[test]
    fn subdocument_equality() {
        let d = doc! { "a": { "x": 1i32 } };
        assert!(matches(&d, &doc! { "a": { "x": 1i32 } }));
        assert!(!matches(&d, &doc! { "a": { "x": 2i32 } }));
    }

    #[test]
    fn expr_operator_with_field_refs() {
        let d = doc! { "a": 3i32, "b": 4i32 };
        assert!(matches(&d, &doc! { "$expr": { "$gt": ["$b", "$a"] } }));
        assert!(!matches(&d, &doc! { "$expr": { "$lt": ["$b", "$a"] } }));
    }

    #[test]
    fn expr_operator_with_user_vars() {
        let d = doc! { "a": 3i32 };
        let mut vars = HashMap::new();
        vars.insert("threshold".to_string(), Bson::Int32(2));
        assert!(document_matches_filter(
            &d,
            &doc! { "$expr": { "$gt": ["$a", "$$threshold"] } },
            &vars
        ));
    }
}
