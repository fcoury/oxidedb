use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use crate::aggregation::stages::group::{
    AccumulatorState, AccumulatorType, compute_accumulator, parse_accumulator_type,
};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    group_by: &Bson,
    buckets: i32,
    _granularity: Option<&str>,
    output: Option<&Document>,
) -> anyhow::Result<Vec<Document>> {
    if buckets < 1 {
        return Err(anyhow::anyhow!("$bucketAuto requires at least 1 bucket"));
    }

    if docs.is_empty() {
        return Ok(Vec::new());
    }

    // Collect all group values
    let mut group_values: Vec<Bson> = Vec::new();
    for doc in &docs {
        let ctx = ExprEvalContext::new(doc.clone(), doc.clone());

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

        group_values.push(group_val);
    }

    // Sort values to calculate boundaries
    group_values.sort_by(crate::aggregation::bson_cmp);

    // Calculate bucket boundaries
    let bucket_count = buckets as usize;
    let doc_count = docs.len();
    let docs_per_bucket = doc_count / bucket_count;
    let remainder = doc_count % bucket_count;

    let mut boundaries: Vec<Bson> = Vec::new();
    boundaries.push(group_values[0].clone());

    let mut idx = 0;
    for i in 0..bucket_count {
        let bucket_size = docs_per_bucket + if i < remainder { 1 } else { 0 };
        idx += bucket_size;
        if idx < doc_count {
            boundaries.push(group_values[idx].clone());
        }
    }

    // Ensure we have at least 2 boundaries
    if boundaries.len() < 2 {
        boundaries.push(group_values[group_values.len() - 1].clone());
    }

    // Group documents into buckets
    let mut bucket_map: HashMap<usize, Vec<Document>> = HashMap::new();

    for (i, doc) in docs.iter().enumerate() {
        let bucket_idx = (i * bucket_count / doc_count).min(bucket_count - 1);
        bucket_map.entry(bucket_idx).or_default().push(doc.clone());
    }

    // Build result documents
    let mut result = Vec::new();

    for i in 0..boundaries.len() - 1 {
        let mut bucket_doc = Document::new();

        // _id contains min and max
        let mut id_doc = Document::new();
        id_doc.insert("min", boundaries[i].clone());
        id_doc.insert("max", boundaries.get(i + 1).cloned().unwrap_or(Bson::Null));
        bucket_doc.insert("_id", Bson::Document(id_doc));

        // Add count
        let bucket_docs = bucket_map.get(&i).map(|v| v.as_slice()).unwrap_or(&[]);
        bucket_doc.insert("count", Bson::Int64(bucket_docs.len() as i64));

        // Process output accumulators if specified
        if let Some(output_spec) = output {
            for (field_name, acc_spec) in output_spec.iter() {
                if field_name == "count" {
                    continue;
                }

                let final_value = compute_bucket_accumulator(bucket_docs, acc_spec)?;
                bucket_doc.insert(field_name.clone(), final_value);
            }
        }

        result.push(bucket_doc);
    }

    Ok(result)
}

fn compute_bucket_accumulator(docs: &[Document], acc_spec: &Bson) -> anyhow::Result<Bson> {
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
                let ctx = ExprEvalContext::new(doc.clone(), doc.clone());
                let expr = parse_expr(acc_val)?;
                let value = eval_expr(&expr, &ctx)?;

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
