use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    replacement: &Bson,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let expr = parse_expr(replacement)?;
        let evaluated = eval_expr(&expr, &ctx)?;

        // Check for $$REMOVE which is not allowed in $replaceRoot
        if matches!(evaluated, Bson::Undefined) {
            return Err(anyhow::anyhow!("$replaceRoot with $$REMOVE is not allowed"));
        }

        // Must be a document
        if let Bson::Document(new_doc) = evaluated {
            result.push(new_doc);
        } else {
            return Err(anyhow::anyhow!(
                "$replaceRoot/$replaceWith expression must evaluate to a document, got {:?}",
                evaluated
            ));
        }
    }

    Ok(result)
}
