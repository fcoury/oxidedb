use crate::aggregation::pipeline::Stage;
use crate::store::PgStore;
use bson::Document;

pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    coll: &str,
    pipeline: &[Stage],
) -> anyhow::Result<Vec<Document>> {
    let mut result = docs;

    // Fetch documents from the other collection
    let union_docs = pg.find_docs(db, coll, None, None, None, 100_000).await?;

    // Apply optional pipeline to the union collection documents
    let mut processed_union = union_docs;
    for stage in pipeline {
        match stage {
            Stage::Match(filter) => {
                processed_union.retain(|d| document_matches_filter(d, filter));
            }
            Stage::Project(spec) => {
                processed_union =
                    crate::aggregation::stages::project::execute(processed_union, spec)?;
            }
            Stage::AddFields(spec) => {
                processed_union =
                    crate::aggregation::stages::add_fields::execute(processed_union, spec)?;
            }
            Stage::Set(spec) => {
                processed_union = crate::aggregation::stages::set::execute(processed_union, spec)?;
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

fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    use bson::Bson;

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
