use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    spec: &Document,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut has_inclusion = false;
    let mut has_exclusion = false;
    let mut id_spec = None;

    // First pass: validate and detect inclusion/exclusion mode
    for (key, value) in spec.iter() {
        if key == "_id" {
            id_spec = Some(value);
            continue;
        }

        match value {
            Bson::Int32(0) | Bson::Int64(0) => has_exclusion = true,
            Bson::Int32(1) | Bson::Int64(1) => has_inclusion = true,
            _ => has_inclusion = true, // Computed fields count as inclusion
        }
    }

    // Check for mixing inclusion and exclusion (except _id)
    if has_inclusion && has_exclusion {
        return Err(anyhow::anyhow!(
            "Cannot mix inclusion and exclusion in $project (except _id)"
        ));
    }

    let is_exclusion_mode = has_exclusion && !has_inclusion;

    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let mut projected = Document::new();

        if is_exclusion_mode {
            // Exclusion mode: start with all fields, remove excluded ones
            projected = doc.clone();

            for (key, value) in spec.iter() {
                if key == "_id" {
                    // Handle _id specially
                    if let Some(Bson::Int32(0) | Bson::Int64(0)) = id_spec {
                        projected.remove("_id");
                    }
                    continue;
                }

                match value {
                    Bson::Int32(0) | Bson::Int64(0) => {
                        projected.remove(key);
                    }
                    _ => {
                        // In exclusion mode, computed fields are errors
                        return Err(anyhow::anyhow!(
                            "Cannot use computed fields in exclusion mode"
                        ));
                    }
                }
            }
        } else {
            // Inclusion mode: only include specified fields
            for (key, value) in spec.iter() {
                if key == "_id" {
                    if let Some(Bson::Int32(0) | Bson::Int64(0)) = id_spec {
                        // Exclude _id
                        continue;
                    } else {
                        // Include _id (default behavior)
                        if let Some(id) = doc.get("_id") {
                            projected.insert("_id", id.clone());
                        }
                    }
                    continue;
                }

                match value {
                    Bson::Int32(1) | Bson::Int64(1) => {
                        // Include field as-is
                        if let Some(field_val) = doc.get(key) {
                            projected.insert(key, field_val.clone());
                        }
                    }
                    _ => {
                        // Computed field - evaluate expression
                        let expr = parse_expr(value)?;
                        let evaluated = eval_expr(&expr, &ctx)?;
                        // Skip fields that evaluate to $$REMOVE (Bson::Undefined)
                        if !matches!(evaluated, Bson::Undefined) {
                            projected.insert(key, evaluated);
                        }
                    }
                }
            }

            // If _id not specified, include it by default
            if id_spec.is_none()
                && let Some(id) = doc.get("_id")
            {
                projected.insert("_id", id.clone());
            }
        }

        result.push(projected);
    }

    Ok(result)
}
