use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::Document;

pub fn execute(docs: Vec<Document>, spec: &Document) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::new(doc.clone(), doc.clone());
        let mut new_doc = doc.clone();

        for (key, value) in spec.iter() {
            let expr = parse_expr(value)?;
            let evaluated = eval_expr(&expr, &ctx)?;
            new_doc.insert(key, evaluated);
        }

        result.push(new_doc);
    }

    Ok(result)
}
