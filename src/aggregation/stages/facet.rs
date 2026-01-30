use crate::aggregation::pipeline::Stage;
use bson::{Bson, Document, doc};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    facets: &HashMap<String, Vec<Stage>>,
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
                    facet_docs = crate::aggregation::stages::project::execute(facet_docs, spec)?;
                }
                Stage::AddFields(spec) => {
                    facet_docs = crate::aggregation::stages::add_fields::execute(facet_docs, spec)?;
                }
                Stage::Set(spec) => {
                    facet_docs = crate::aggregation::stages::set::execute(facet_docs, spec)?;
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
                    facet_docs =
                        crate::aggregation::stages::group::execute(facet_docs, id, accumulators)?;
                }
                Stage::Count(field) => {
                    // Return single document with count
                    facet_docs = vec![doc! { field.clone(): facet_docs.len() as i32 }];
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

fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        if key.starts_with('$') {
            match key.as_str() {
                "$and" => {
                    if let Bson::Array(arr) = value {
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond
                                && !document_matches_filter(doc, cond_doc)
                            {
                                return false;
                            }
                        }
                    }
                }
                "$or" => {
                    if let Bson::Array(arr) = value {
                        let mut any_match = false;
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond
                                && document_matches_filter(doc, cond_doc)
                            {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return false;
                        }
                    }
                }
                _ => {}
            }
        } else {
            let doc_val = doc.get(key);
            if doc_val != Some(value) {
                return false;
            }
        }
    }
    true
}
