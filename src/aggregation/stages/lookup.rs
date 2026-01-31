use crate::aggregation::exec::ExecContext;
use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use crate::aggregation::pipeline::Stage;
use crate::store::PgStore;
use bson::{Bson, Document};
use std::collections::HashMap;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    from: &str,
    local_field: Option<&str>,
    foreign_field: Option<&str>,
    as_field: &str,
    let_vars: Option<&Document>,
    pipeline: Option<&Vec<Bson>>,
    outer_vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    // Check if we're using simple form or pipeline form
    if let (Some(local), Some(foreign)) = (local_field, foreign_field) {
        // Simple form: left outer join
        for doc in docs {
            let local_val = doc.get(local).cloned();

            let matches = if let Some(ref val) = local_val {
                // Build filter for foreign collection
                let mut filter = Document::new();
                filter.insert(foreign.to_string(), val.clone());

                // Query the foreign collection
                pg.find_docs(db, from, Some(&filter), None, None, 100_000)
                    .await?
            } else {
                Vec::new()
            };

            let mut new_doc = doc.clone();
            new_doc.insert(
                as_field.to_string(),
                Bson::Array(matches.into_iter().map(Bson::Document).collect()),
            );
            result.push(new_doc);
        }
    } else if let Some(pipe) = pipeline {
        // Pipeline form
        for doc in docs {
            // Build variables for the sub-pipeline
            let mut vars = outer_vars.clone();

            if let Some(let_doc) = let_vars {
                for (key, value) in let_doc.iter() {
                    let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
                    let expr = parse_expr(value)?;
                    let evaluated = eval_expr(&expr, &ctx)?;
                    vars.insert(key.clone(), evaluated);
                }
            }

            // Parse the sub-pipeline stages
            let mut stages = Vec::new();
            for stage_bson in pipe.iter() {
                if let Bson::Document(stage_doc) = stage_bson {
                    // Parse stage - this is a simplified version
                    // In reality, we'd need to properly parse each stage type
                    if let Some((stage_name, stage_value)) = stage_doc.iter().next() {
                        match stage_name.as_str() {
                            "$match" => {
                                if let Bson::Document(filter) = stage_value {
                                    stages.push(Stage::Match(filter.clone()));
                                }
                            }
                            "$project" => {
                                if let Bson::Document(spec) = stage_value {
                                    stages.push(Stage::Project(spec.clone()));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Execute the sub-pipeline on the foreign collection
            let _ctx = ExecContext::with_vars(
                Some(pg),
                db.to_string(),
                from.to_string(),
                false,
                vars.clone(),
            );

            // First, get all documents from the foreign collection
            let foreign_docs = pg.find_docs(db, from, None, None, None, 100_000).await?;

            // Execute the pipeline on foreign docs
            let mut pipeline_docs = foreign_docs;
            for stage in stages {
                match stage {
                    Stage::Match(filter) => {
                        pipeline_docs.retain(|d| document_matches_filter(d, &filter));
                    }
                    Stage::Project(spec) => {
                        pipeline_docs = crate::aggregation::stages::project::execute(
                            pipeline_docs,
                            &spec,
                            &vars,
                        )?;
                    }
                    _ => {}
                }
            }

            let mut new_doc = doc.clone();
            new_doc.insert(
                as_field.to_string(),
                Bson::Array(pipeline_docs.into_iter().map(Bson::Document).collect()),
            );
            result.push(new_doc);
        }
    } else {
        return Err(anyhow::anyhow!(
            "$lookup requires either localField/foreignField or pipeline"
        ));
    }

    Ok(result)
}

fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        if key.starts_with('$') {
            // Logical operator - simplified
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
            // Field match
            let doc_val = doc.get(key);
            if doc_val != Some(value) {
                return false;
            }
        }
    }
    true
}
