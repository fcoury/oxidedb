use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

/// Redaction result
#[derive(Debug, Clone, Copy, PartialEq)]
enum RedactResult {
    Descend,
    Prune,
    Keep,
}

pub fn execute(
    docs: Vec<Document>,
    expr: &Bson,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let redacted = redact_document(&doc, expr, &ctx)?;
        if let Some(doc) = redacted {
            result.push(doc);
        }
    }

    Ok(result)
}

fn redact_document(
    doc: &Document,
    expr: &Bson,
    ctx: &ExprEvalContext,
) -> anyhow::Result<Option<Document>> {
    let result = evaluate_redact_expr(doc, expr, ctx)?;

    match result {
        RedactResult::Prune => Ok(None),
        RedactResult::Keep => Ok(Some(doc.clone())),
        RedactResult::Descend => {
            // Recursively process nested documents
            let mut new_doc = Document::new();

            for (key, value) in doc.iter() {
                match value {
                    Bson::Document(nested) => {
                        // Recursively redact nested document
                        if let Some(redacted) = redact_document(nested, expr, ctx)? {
                            new_doc.insert(key.clone(), Bson::Document(redacted));
                        }
                    }
                    Bson::Array(arr) => {
                        // Redact each element in the array
                        let mut new_arr = Vec::new();
                        for item in arr {
                            if let Bson::Document(item_doc) = item {
                                if let Some(redacted) = redact_document(item_doc, expr, ctx)? {
                                    new_arr.push(Bson::Document(redacted));
                                }
                            } else {
                                new_arr.push(item.clone());
                            }
                        }
                        new_doc.insert(key.clone(), Bson::Array(new_arr));
                    }
                    _ => {
                        new_doc.insert(key.clone(), value.clone());
                    }
                }
            }

            Ok(Some(new_doc))
        }
    }
}

fn evaluate_redact_expr(
    _doc: &Document,
    expr: &Bson,
    ctx: &ExprEvalContext,
) -> anyhow::Result<RedactResult> {
    let parsed_expr = parse_expr(expr)?;
    let result = eval_expr(&parsed_expr, ctx)?;

    match result {
        Bson::String(s) => {
            match s.as_str() {
                "$$DESCEND" => Ok(RedactResult::Descend),
                "$$PRUNE" => Ok(RedactResult::Prune),
                "$$KEEP" => Ok(RedactResult::Keep),
                _ => {
                    // Check if it's a variable reference or literal
                    if s.starts_with("$$") {
                        // It's a system variable - evaluate it
                        match s.as_str() {
                            "$$ROOT" | "$$CURRENT" => Ok(RedactResult::Descend),
                            _ => Ok(RedactResult::Keep),
                        }
                    } else {
                        // Literal string - treat as Keep
                        Ok(RedactResult::Keep)
                    }
                }
            }
        }
        Bson::Document(_) => {
            // Expression evaluated to a document - descend into it
            Ok(RedactResult::Descend)
        }
        _ => {
            // Any other value - keep
            Ok(RedactResult::Keep)
        }
    }
}
