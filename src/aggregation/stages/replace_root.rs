use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};

pub fn execute(docs: Vec<Document>, replacement: &Bson) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::new(doc.clone(), doc.clone());
        let expr = parse_expr(replacement)?;
        let evaluated = eval_expr(&expr, &ctx)?;

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
