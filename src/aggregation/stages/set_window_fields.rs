use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

/// $setWindowFields stage specification
#[derive(Debug, Clone)]
pub struct SetWindowFieldsSpec {
    pub partition_by: Option<Bson>,
    pub sort_by: Document,
    pub output: Document,
}

impl SetWindowFieldsSpec {
    pub fn parse(value: &Bson) -> anyhow::Result<Self> {
        let doc = value
            .as_document()
            .ok_or_else(|| anyhow::anyhow!("$setWindowFields value must be a document"))?;

        let partition_by = doc.get("partitionBy").cloned();
        let sort_by = doc
            .get_document("sortBy")
            .map_err(|_| anyhow::anyhow!("$setWindowFields requires sortBy"))?
            .clone();
        let output = doc
            .get_document("output")
            .map_err(|_| anyhow::anyhow!("$setWindowFields requires output"))?
            .clone();

        Ok(Self {
            partition_by,
            sort_by,
            output,
        })
    }
}

pub fn execute(
    docs: Vec<Document>,
    spec: &SetWindowFieldsSpec,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    if docs.is_empty() {
        return Ok(docs);
    }

    let mut result = docs;

    // Sort by sortBy specification
    let mut sort_specs: Vec<(String, i32)> = Vec::new();
    for (key, value) in spec.sort_by.iter() {
        let direction = match value {
            Bson::Int32(n) => *n,
            Bson::Int64(n) => *n as i32,
            _ => 1,
        };
        sort_specs.push((key.clone(), direction));
    }

    result.sort_by(|a, b| {
        for (field, direction) in &sort_specs {
            let a_val = a.get(field).unwrap_or(&Bson::Null);
            let b_val = b.get(field).unwrap_or(&Bson::Null);
            let cmp = crate::aggregation::bson_cmp(a_val, b_val);
            if cmp != std::cmp::Ordering::Equal {
                return if *direction == 1 { cmp } else { cmp.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });

    // Process each output field with window functions
    // Collect field names first to avoid borrow issues
    let output_fields: Vec<(String, Bson)> = spec
        .output
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    for (field_name, window_spec) in output_fields {
        if let Bson::Document(window_doc) = &window_spec {
            // Parse window function
            if let Some((func_name, func_expr)) = window_doc.iter().next() {
                let window_field = if let Some(stripped) = func_name.strip_prefix('$') {
                    stripped
                } else {
                    func_name.as_str()
                };

                // Get window bounds if specified
                let _window_bounds = window_doc.get_document("window").ok();

                // Pre-compute all values first
                let mut computed_values: Vec<(usize, Bson)> = Vec::new();

                for (idx, doc) in result.iter().enumerate() {
                    let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());

                    let value = match window_field {
                        "sum" | "avg" | "min" | "max" | "count" => {
                            let expr = parse_expr(func_expr)?;
                            eval_expr(&expr, &ctx)?
                        }
                        "first" | "last" => {
                            let expr = parse_expr(func_expr)?;
                            eval_expr(&expr, &ctx)?
                        }
                        _ => {
                            let expr = parse_expr(func_expr)?;
                            eval_expr(&expr, &ctx)?
                        }
                    };

                    computed_values.push((idx, value));
                }

                // Now insert the computed values
                for (idx, value) in computed_values {
                    if let Some(doc) = result.get_mut(idx) {
                        doc.insert(field_name.clone(), value);
                    }
                }
            }
        }
    }

    Ok(result)
}
