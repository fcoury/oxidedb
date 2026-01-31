use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    spec: &Document,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let mut new_doc = doc.clone();

        for (key, value) in spec.iter() {
            let expr = parse_expr(value)?;
            let evaluated = eval_expr(&expr, &ctx)?;
            // Skip fields that evaluate to $$REMOVE (Bson::Undefined)
            if matches!(evaluated, Bson::Undefined) {
                new_doc.remove(key);
            } else {
                new_doc.insert(key, evaluated);
            }
        }

        result.push(new_doc);
    }

    Ok(result)
}
