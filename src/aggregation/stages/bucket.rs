use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use crate::aggregation::stages::group::{
    AccumulatorState, AccumulatorType, compute_accumulator, parse_accumulator_type,
};
use bson::{Bson, Document};
use std::collections::HashMap;

#[allow(clippy::collapsible_if)]
pub fn execute(
    docs: Vec<Document>,
    group_by: &Bson,
    boundaries: &[Bson],
    default: Option<&Bson>,
    output: Option<&Document>,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    if boundaries.len() < 2 {
        return Err(anyhow::anyhow!("$bucket requires at least 2 boundaries"));
    }

    // Bucket index -> documents and accumulator states
    let mut buckets: HashMap<usize, Vec<Document>> = HashMap::new();
    let mut default_docs: Vec<Document> = Vec::new();

    // Group documents into buckets
    for doc in &docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());

        // Evaluate groupBy expression
        let group_val = if let Bson::String(s) = group_by {
            if let Some(field_name) = s.strip_prefix('$') {
                doc.get(field_name).cloned().unwrap_or(Bson::Null)
            } else {
                Bson::String(s.clone())
            }
        } else {
            let expr = parse_expr(group_by)?;
            eval_expr(&expr, &ctx)?
        };

        // Find which bucket this document belongs to
        let bucket_idx = find_bucket_index(&group_val, boundaries);

        match bucket_idx {
            Some(idx) => {
                buckets.entry(idx).or_default().push(doc.clone());
            }
            None => {
                if default.is_some() {
                    default_docs.push(doc.clone());
                }
            }
        }
    }

    // Build result documents
    let mut result = Vec::new();

    for (i, boundary) in boundaries.iter().enumerate().take(boundaries.len() - 1) {
        let mut bucket_doc = Document::new();

        // _id is the lower boundary
        bucket_doc.insert("_id", boundary.clone());

        // Add count by default
        let bucket_docs = buckets.get(&i).map(|v| v.as_slice()).unwrap_or(&[]);
        bucket_doc.insert("count", Bson::Int64(bucket_docs.len() as i64));

        // Process output accumulators if specified
        if let Some(output_spec) = output {
            for (field_name, acc_spec) in output_spec.iter() {
                if field_name == "count" {
                    continue; // count is built-in
                }

                let final_value = compute_bucket_accumulator(bucket_docs, acc_spec, vars)?;
                bucket_doc.insert(field_name.clone(), final_value);
            }
        }

        result.push(bucket_doc);
    }

    // Add default bucket if specified and there are documents
    if let Some(default_id) = default {
        if !default_docs.is_empty() {
            let mut bucket_doc = Document::new();
            bucket_doc.insert("_id", default_id.clone());
            bucket_doc.insert("count", Bson::Int64(default_docs.len() as i64));

            // Process output accumulators for default bucket
            if let Some(output_spec) = output {
                for (field_name, acc_spec) in output_spec.iter() {
                    if field_name == "count" {
                        continue;
                    }

                    let final_value = compute_bucket_accumulator(&default_docs, acc_spec, vars)?;
                    bucket_doc.insert(field_name.clone(), final_value);
                }
            }

            result.push(bucket_doc);
        }
    }

    Ok(result)
}

fn find_bucket_index(value: &Bson, boundaries: &[Bson]) -> Option<usize> {
    for i in 0..boundaries.len() - 1 {
        let lower = &boundaries[i];
        let upper = &boundaries[i + 1];

        // Check if value >= lower
        let ge_lower = crate::aggregation::bson_cmp(value, lower) != std::cmp::Ordering::Less;

        // Check if value < upper
        let lt_upper = crate::aggregation::bson_cmp(value, upper) == std::cmp::Ordering::Less;

        if ge_lower && lt_upper {
            return Some(i);
        }
    }

    None
}

fn compute_bucket_accumulator(
    docs: &[Document],
    acc_spec: &Bson,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Bson> {
    if let Bson::Document(acc_doc) = acc_spec {
        if let Some((acc_op, acc_val)) = acc_doc.iter().next() {
            let acc_type = parse_accumulator_type(acc_op)?;

            let mut state = AccumulatorState {
                acc_type,
                values: Vec::new(),
                first_value: None,
                last_value: None,
            };

            for doc in docs {
                let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
                let expr = parse_expr(acc_val)?;
                let value = eval_expr(&expr, &ctx)?;

                // Update accumulator state
                match acc_type {
                    AccumulatorType::First => {
                        if state.first_value.is_none() {
                            state.first_value = Some(value);
                        }
                    }
                    AccumulatorType::Last => {
                        state.last_value = Some(value);
                    }
                    AccumulatorType::Sum
                    | AccumulatorType::Avg
                    | AccumulatorType::Min
                    | AccumulatorType::Max
                    | AccumulatorType::Push => {
                        state.values.push(value);
                    }
                }
            }

            compute_accumulator(&state)
        } else {
            Ok(Bson::Null)
        }
    } else {
        Ok(acc_spec.clone())
    }
}
