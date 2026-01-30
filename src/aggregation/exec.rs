use crate::aggregation::memory::MemoryManager;
use crate::aggregation::pipeline::{Pipeline, Stage};
use crate::store::PgStore;
use bson::{Bson, Document, doc};
use std::collections::HashMap;

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

/// Document stream trait for lazy evaluation
#[allow(dead_code)]
trait DocumentStream {
    fn next(&mut self) -> anyhow::Result<Option<Document>>;
}

/// Execute a pipeline
pub async fn execute_pipeline(
    ctx: &ExecContext<'_>,
    pipeline: Pipeline,
) -> anyhow::Result<ExecResult> {
    let mut docs: Vec<Document> = Vec::new();
    let mut main_coll_fetched = false;

    for stage in pipeline.stages {
        // Fetch collection if not yet fetched and this is not a $match stage
        if !main_coll_fetched && !matches!(stage, Stage::Match(_)) {
            if let Some(pg) = ctx.pg {
                docs = pg
                    .find_docs(&ctx.db, &ctx.coll, None, None, None, 100_000)
                    .await?;
                main_coll_fetched = true;
            }
        }

        match stage {
            Stage::Match(filter) => {
                if !main_coll_fetched {
                    // First match - fetch from collection with filter
                    if let Some(pg) = ctx.pg {
                        docs = pg
                            .find_docs(&ctx.db, &ctx.coll, Some(&filter), None, None, 100_000)
                            .await?;
                        main_coll_fetched = true;
                    }
                } else {
                    // Filter existing docs
                    docs.retain(|d| document_matches_filter(d, &filter));
                }
            }
            Stage::Project(spec) => {
                docs = crate::aggregation::stages::project::execute(docs, &spec)?;
            }
            Stage::AddFields(spec) => {
                docs = crate::aggregation::stages::add_fields::execute(docs, &spec)?;
            }
            Stage::Set(spec) => {
                docs = crate::aggregation::stages::set::execute(docs, &spec)?;
            }
            Stage::Unset(fields) => {
                docs = crate::aggregation::stages::unset::execute(docs, &fields)?;
            }
            Stage::ReplaceRoot { replacement } => {
                docs = crate::aggregation::stages::replace_root::execute(docs, &replacement)?;
            }
            Stage::ReplaceWith(replacement) => {
                docs = crate::aggregation::stages::replace_root::execute(docs, &replacement)?;
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
                // Transform docs into a single document with the count
                // This is NOT terminal - subsequent stages can process the count document
                let count = docs.len() as i32;
                docs = vec![doc! { field: count }];
            }
            Stage::Group { id, accumulators } => {
                docs = crate::aggregation::stages::group::execute(docs, &id, &accumulators)?;
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
                docs = crate::aggregation::stages::facet::execute(docs, &facets)?;
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
                docs = crate::aggregation::stages::sort_by_count::execute(docs, &expr)?;
            }
            Stage::SetWindowFields(spec) => {
                docs = crate::aggregation::stages::set_window_fields::execute(docs, &spec)?;
            }
            Stage::Densify(spec) => {
                docs = crate::aggregation::stages::densify::execute(docs, &spec)?;
            }
            Stage::Fill(spec) => {
                docs = crate::aggregation::stages::fill::execute(docs, &spec)?;
            }
            Stage::Redact(expr) => {
                docs = crate::aggregation::stages::redact::execute(docs, &expr)?;
            }
        }
    }

    Ok(ExecResult::Cursor(docs))
}

/// Check if document matches filter (simplified)
#[allow(clippy::collapsible_if)]
fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        if key.starts_with('$') {
            // Logical operator
            match key.as_str() {
                "$and" => {
                    if let Bson::Array(arr) = value {
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond {
                                if !document_matches_filter(doc, cond_doc) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                "$or" => {
                    if let Bson::Array(arr) = value {
                        let mut any_match = false;
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond {
                                if document_matches_filter(doc, cond_doc) {
                                    any_match = true;
                                    break;
                                }
                            }
                        }
                        if !any_match {
                            return false;
                        }
                    }
                }
                "$not" => {
                    if let Bson::Document(cond_doc) = value {
                        if document_matches_filter(doc, cond_doc) {
                            return false;
                        }
                    }
                }
                _ => {}
            }
        } else {
            // Field match
            let doc_val = doc.get(key);
            if !value_matches(doc_val, value) {
                return false;
            }
        }
    }
    true
}

#[allow(clippy::collapsible_if)]
fn value_matches(doc_val: Option<&Bson>, filter_val: &Bson) -> bool {
    match filter_val {
        Bson::Document(filter_doc) => {
            // Check for operators
            for (op, op_val) in filter_doc.iter() {
                match op.as_str() {
                    "$eq" => {
                        if doc_val != Some(op_val) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if doc_val == Some(op_val) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            if crate::aggregation::bson_cmp(dv, fv) != std::cmp::Ordering::Greater {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$gte" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            let cmp = crate::aggregation::bson_cmp(dv, fv);
                            if cmp != std::cmp::Ordering::Greater
                                && cmp != std::cmp::Ordering::Equal
                            {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lt" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            if crate::aggregation::bson_cmp(dv, fv) != std::cmp::Ordering::Less {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lte" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            let cmp = crate::aggregation::bson_cmp(dv, fv);
                            if cmp != std::cmp::Ordering::Less && cmp != std::cmp::Ordering::Equal {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$in" => {
                        if let Bson::Array(arr) = op_val {
                            if let Some(dv) = doc_val {
                                if !arr.contains(dv) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                    "$nin" => {
                        if let Bson::Array(arr) = op_val {
                            if let Some(dv) = doc_val {
                                if arr.contains(dv) {
                                    return false;
                                }
                            }
                        }
                    }
                    "$exists" => {
                        let should_exist = op_val.as_bool().unwrap_or(true);
                        let does_exist = doc_val.is_some();
                        if should_exist != does_exist {
                            return false;
                        }
                    }
                    "$regex" => {
                        if let Some(Bson::String(doc_str)) = doc_val {
                            let pattern = match op_val {
                                Bson::String(p) => p,
                                _ => return false,
                            };
                            // Simple substring match for now
                            if !doc_str.contains(pattern) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    _ => {}
                }
            }
            true
        }
        _ => doc_val == Some(filter_val),
    }
}
