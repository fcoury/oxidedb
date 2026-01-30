use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    id: &Bson,
    accumulators: &Document,
) -> anyhow::Result<Vec<Document>> {
    // Group key -> (original_key, accumulated values)
    // Use string representation as key since Bson doesn't implement Hash
    let mut groups: HashMap<String, (Bson, HashMap<String, AccumulatorState>)> = HashMap::new();

    for doc in &docs {
        let ctx = ExprEvalContext::new(doc.clone(), doc.clone());

        // Compute the _id (group key)
        let group_id = if let Bson::String(s) = id {
            // Field reference like "$field"
            if s.starts_with('$') {
                let field_name = &s[1..];
                doc.get(field_name).cloned().unwrap_or(Bson::Null)
            } else {
                // Literal string
                Bson::String(s.clone())
            }
        } else {
            // Evaluate as expression
            let expr = parse_expr(id)?;
            eval_expr(&expr, &ctx)?
        };

        // Get or create group entry using string key
        let group_key = format!("{:?}", group_id);
        let (_, group) = groups
            .entry(group_key)
            .or_insert_with(|| (group_id.clone(), HashMap::new()));

        // Process each accumulator
        for (field_name, acc_spec) in accumulators.iter() {
            let state = group
                .entry(field_name.clone())
                .or_insert_with(|| AccumulatorState {
                    acc_type: AccumulatorType::First,
                    values: Vec::new(),
                    first_value: None,
                    last_value: None,
                });

            // Parse accumulator specification
            if let Bson::Document(acc_doc) = acc_spec {
                if let Some((acc_op, acc_val)) = acc_doc.iter().next() {
                    state.acc_type = parse_accumulator_type(acc_op)?;

                    // Evaluate the accumulator expression
                    let expr = parse_expr(acc_val)?;
                    let value = eval_expr(&expr, &ctx)?;

                    // Update accumulator state
                    match state.acc_type {
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
            }
        }
    }

    // Build result documents
    let mut result = Vec::new();
    for (_key, (group_id, accs)) in groups {
        let mut doc = Document::new();
        doc.insert("_id", group_id);

        for (field_name, state) in accs {
            let final_value = compute_accumulator(&state)?;
            doc.insert(field_name, final_value);
        }

        result.push(doc);
    }

    Ok(result)
}

#[derive(Debug, Clone)]
pub struct AccumulatorState {
    pub acc_type: AccumulatorType,
    pub values: Vec<Bson>,
    pub first_value: Option<Bson>,
    pub last_value: Option<Bson>,
}

#[derive(Debug, Clone, Copy)]
pub enum AccumulatorType {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Push,
}

pub fn parse_accumulator_type(op: &str) -> anyhow::Result<AccumulatorType> {
    match op {
        "$sum" => Ok(AccumulatorType::Sum),
        "$avg" => Ok(AccumulatorType::Avg),
        "$min" => Ok(AccumulatorType::Min),
        "$max" => Ok(AccumulatorType::Max),
        "$first" => Ok(AccumulatorType::First),
        "$last" => Ok(AccumulatorType::Last),
        "$push" => Ok(AccumulatorType::Push),
        _ => Err(anyhow::anyhow!("Unknown accumulator: {}", op)),
    }
}

pub fn compute_accumulator(state: &AccumulatorState) -> anyhow::Result<Bson> {
    match state.acc_type {
        AccumulatorType::First => Ok(state.first_value.clone().unwrap_or(Bson::Null)),
        AccumulatorType::Last => Ok(state.last_value.clone().unwrap_or(Bson::Null)),
        AccumulatorType::Push => Ok(Bson::Array(state.values.clone())),
        AccumulatorType::Sum => {
            let mut sum: f64 = 0.0;
            for val in &state.values {
                match val {
                    Bson::Int32(n) => sum += *n as f64,
                    Bson::Int64(n) => sum += *n as f64,
                    Bson::Double(n) => sum += *n,
                    _ => {}
                }
            }
            Ok(Bson::Double(sum))
        }
        AccumulatorType::Avg => {
            if state.values.is_empty() {
                return Ok(Bson::Null);
            }
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for val in &state.values {
                match val {
                    Bson::Int32(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Bson::Int64(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Bson::Double(n) => {
                        sum += *n;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                Ok(Bson::Null)
            } else {
                Ok(Bson::Double(sum / count as f64))
            }
        }
        AccumulatorType::Min => {
            if state.values.is_empty() {
                return Ok(Bson::Null);
            }
            let mut min = &state.values[0];
            for val in &state.values[1..] {
                if crate::aggregation::bson_cmp(val, min) == std::cmp::Ordering::Less {
                    min = val;
                }
            }
            Ok(min.clone())
        }
        AccumulatorType::Max => {
            if state.values.is_empty() {
                return Ok(Bson::Null);
            }
            let mut max = &state.values[0];
            for val in &state.values[1..] {
                if crate::aggregation::bson_cmp(val, max) == std::cmp::Ordering::Greater {
                    max = val;
                }
            }
            Ok(max.clone())
        }
    }
}
