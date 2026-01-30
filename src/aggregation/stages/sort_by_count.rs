use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use crate::aggregation::values::bson_cmp;
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(docs: Vec<Document>, expr: &Bson) -> anyhow::Result<Vec<Document>> {
    // Group by expression value and count
    // Use string representation of Bson as key since Bson doesn't implement Hash
    let mut counts: HashMap<String, (Bson, i64)> = HashMap::new();

    for doc in &docs {
        let ctx = ExprEvalContext::new(doc.clone(), doc.clone());
        let expr_val = if let Bson::String(s) = expr {
            if let Some(field_name) = s.strip_prefix('$') {
                doc.get(field_name).cloned().unwrap_or(Bson::Null)
            } else {
                Bson::String(s.clone())
            }
        } else {
            let parsed_expr = parse_expr(expr)?;
            eval_expr(&parsed_expr, &ctx)?
        };

        let key = format!("{:?}", expr_val);
        counts.entry(key).or_insert_with(|| (expr_val, 0)).1 += 1;
    }

    // Build result documents and sort by count descending
    let mut result: Vec<Document> = counts
        .into_iter()
        .map(|(_key, (id, count))| {
            let mut doc = Document::new();
            doc.insert("_id", id);
            doc.insert("count", Bson::Int64(count));
            doc
        })
        .collect();

    // Sort by count descending
    result.sort_by(|a, b| {
        let a_count = a.get("count").unwrap_or(&Bson::Int64(0));
        let b_count = b.get("count").unwrap_or(&Bson::Int64(0));
        bson_cmp(b_count, a_count) // Descending order
    });

    Ok(result)
}
