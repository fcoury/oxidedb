use crate::aggregation::exec::{
    MATERIALIZED_FETCH_LIMIT, document_matches_filter, ensure_document_limit,
};
use crate::aggregation::pipeline::Stage;
use crate::store::PgStore;
use bson::{Bson, Document};
use std::collections::HashMap;

pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    coll: &str,
    pipeline: &[Stage],
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = docs;

    // Fetch documents from the other collection
    let union_docs = pg
        .find_docs(db, coll, None, None, None, MATERIALIZED_FETCH_LIMIT)
        .await?;
    ensure_document_limit(&union_docs)?;

    // Apply optional pipeline to the union collection documents
    let mut processed_union = union_docs;
    for stage in pipeline {
        match stage {
            Stage::Match(filter) => {
                processed_union.retain(|d| document_matches_filter(d, filter, vars));
            }
            Stage::Project(spec) => {
                processed_union =
                    crate::aggregation::stages::project::execute(processed_union, spec, vars)?;
            }
            Stage::AddFields(spec) => {
                processed_union =
                    crate::aggregation::stages::add_fields::execute(processed_union, spec, vars)?;
            }
            Stage::Set(spec) => {
                processed_union =
                    crate::aggregation::stages::set::execute(processed_union, spec, vars)?;
            }
            Stage::Unset(fields) => {
                processed_union =
                    crate::aggregation::stages::unset::execute(processed_union, fields)?;
            }
            Stage::Sort(spec) => {
                processed_union = crate::aggregation::stages::sort::execute(processed_union, spec)?;
            }
            Stage::Limit(n) => {
                processed_union = crate::aggregation::stages::limit::execute(processed_union, *n)?;
            }
            Stage::Skip(n) => {
                processed_union = crate::aggregation::stages::skip::execute(processed_union, *n)?;
            }
            _ => {}
        }
    }

    // Union the documents
    result.extend(processed_union);

    Ok(result)
}
