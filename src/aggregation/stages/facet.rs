use crate::aggregation::exec::document_matches_filter;
use crate::aggregation::pipeline::Stage;
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    facets: &HashMap<String, Vec<Stage>>,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Document::new();

    // Run each facet sub-pipeline on the same input documents
    for (facet_name, stages) in facets.iter() {
        let mut facet_docs = docs.clone();

        // Execute each stage in the sub-pipeline
        for stage in stages {
            match stage {
                Stage::Match(filter) => {
                    facet_docs.retain(|d| document_matches_filter(d, filter));
                }
                Stage::Project(spec) => {
                    facet_docs =
                        crate::aggregation::stages::project::execute(facet_docs, spec, vars)?;
                }
                Stage::AddFields(spec) => {
                    facet_docs =
                        crate::aggregation::stages::add_fields::execute(facet_docs, spec, vars)?;
                }
                Stage::Set(spec) => {
                    facet_docs = crate::aggregation::stages::set::execute(facet_docs, spec, vars)?;
                }
                Stage::Unset(fields) => {
                    facet_docs = crate::aggregation::stages::unset::execute(facet_docs, fields)?;
                }
                Stage::Sort(spec) => {
                    facet_docs = crate::aggregation::stages::sort::execute(facet_docs, spec)?;
                }
                Stage::Limit(n) => {
                    facet_docs = crate::aggregation::stages::limit::execute(facet_docs, *n)?;
                }
                Stage::Skip(n) => {
                    facet_docs = crate::aggregation::stages::skip::execute(facet_docs, *n)?;
                }
                Stage::Group { id, accumulators } => {
                    facet_docs = crate::aggregation::stages::group::execute(
                        facet_docs,
                        id,
                        accumulators,
                        vars,
                    )?;
                }
                Stage::Count(field) => {
                    facet_docs = crate::aggregation::stages::count::execute(facet_docs, field)?;
                }
                _ => {
                    // Other stages not supported in facet sub-pipelines for now
                }
            }
        }

        // Store the result as an array
        result.insert(
            facet_name.clone(),
            Bson::Array(facet_docs.into_iter().map(Bson::Document).collect()),
        );
    }

    Ok(vec![result])
}
