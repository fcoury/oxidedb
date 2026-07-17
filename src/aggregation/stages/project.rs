use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

/// Classification of a `$project` field specification.
enum FieldSpec<'a> {
    Include,
    Exclude,
    Computed(&'a Bson),
}

fn classify(value: &Bson) -> FieldSpec<'_> {
    match value {
        Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false) => FieldSpec::Exclude,
        Bson::Int32(1) | Bson::Int64(1) | Bson::Boolean(true) => FieldSpec::Include,
        other => FieldSpec::Computed(other),
    }
}

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

        match classify(value) {
            FieldSpec::Exclude => has_exclusion = true,
            // Computed fields count as inclusion
            FieldSpec::Include | FieldSpec::Computed(_) => has_inclusion = true,
        }
    }

    // Check for mixing inclusion and exclusion (except _id)
    if has_inclusion && has_exclusion {
        return Err(anyhow::anyhow!(
            "Cannot mix inclusion and exclusion in $project (except _id)"
        ));
    }

    let is_exclusion_mode = if !has_inclusion && !has_exclusion {
        // Spec contains only _id: {_id: 0} excludes _id and keeps the rest,
        // {_id: 1} or a computed _id is an inclusion projection
        matches!(id_spec, Some(v) if matches!(classify(v), FieldSpec::Exclude))
    } else {
        has_exclusion && !has_inclusion
    };

    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let mut projected = Document::new();

        if is_exclusion_mode {
            // Exclusion mode: start with all fields, remove excluded ones
            projected = doc.clone();

            for (key, value) in spec.iter() {
                if key == "_id" {
                    continue;
                }

                match classify(value) {
                    FieldSpec::Exclude => {
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

            apply_id_spec(&mut projected, &doc, id_spec, &ctx)?;
        } else {
            // Inclusion mode: only include specified fields
            for (key, value) in spec.iter() {
                if key == "_id" {
                    continue;
                }

                match classify(value) {
                    FieldSpec::Include => {
                        // Include field as-is
                        if let Some(field_val) = doc.get(key) {
                            projected.insert(key, field_val.clone());
                        }
                    }
                    FieldSpec::Exclude => {
                        // Exclusion of a non-_id field in inclusion mode is a
                        // no-op (field simply stays absent)
                    }
                    FieldSpec::Computed(expr_bson) => {
                        let expr = parse_expr(expr_bson)?;
                        let evaluated = eval_expr(&expr, &ctx)?;
                        // Skip fields that evaluate to $$REMOVE (Bson::Undefined)
                        if !matches!(evaluated, Bson::Undefined) {
                            projected.insert(key, evaluated);
                        }
                    }
                }
            }

            apply_id_spec(&mut projected, &doc, id_spec, &ctx)?;
        }

        result.push(projected);
    }

    Ok(result)
}

/// Apply the `_id` projection: excluded by 0/false, included by 1/true,
/// replaced by any other expression, included by default when unspecified.
fn apply_id_spec(
    projected: &mut Document,
    doc: &Document,
    id_spec: Option<&Bson>,
    ctx: &ExprEvalContext,
) -> anyhow::Result<()> {
    match id_spec {
        Some(Bson::Int32(0)) | Some(Bson::Int64(0)) | Some(Bson::Boolean(false)) => {
            projected.remove("_id");
        }
        Some(Bson::Int32(1)) | Some(Bson::Int64(1)) | Some(Bson::Boolean(true)) => {
            if let Some(id) = doc.get("_id") {
                projected.insert("_id", id.clone());
            }
        }
        Some(expr_bson) => {
            // Computed _id expression (e.g. { _id: "$name" })
            let expr = parse_expr(expr_bson)?;
            let evaluated = eval_expr(&expr, ctx)?;
            if matches!(evaluated, Bson::Undefined) {
                projected.remove("_id");
            } else {
                projected.insert("_id", evaluated);
            }
        }
        None => {
            // _id not specified: include by default
            if let Some(id) = doc.get("_id") {
                projected.insert("_id", id.clone());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::execute;
    use bson::{Bson, doc};
    use std::collections::HashMap;

    fn vars() -> HashMap<String, Bson> {
        HashMap::new()
    }

    #[test]
    fn boolean_flags_include_and_exclude() {
        let docs = vec![doc! { "_id": 1, "name": "a", "secret": "x" }];
        // true includes
        let out = execute(docs.clone(), &doc! { "name": true }, &vars()).unwrap();
        assert_eq!(out[0].get_str("name").unwrap(), "a");
        assert!(out[0].contains_key("_id"));
        assert!(!out[0].contains_key("secret"));
        // false excludes
        let out = execute(docs, &doc! { "secret": false }, &vars()).unwrap();
        assert!(!out[0].contains_key("secret"));
        assert_eq!(out[0].get_str("name").unwrap(), "a");
    }

    #[test]
    fn computed_id_expression() {
        let docs = vec![doc! { "_id": 1, "name": "a" }];
        let out = execute(docs, &doc! { "_id": "$name" }, &vars()).unwrap();
        assert_eq!(out[0].get("_id").unwrap(), &Bson::String("a".into()));
    }

    #[test]
    fn computed_id_in_exclusion_mode() {
        let docs = vec![doc! { "_id": 1, "name": "a", "secret": "x" }];
        let out = execute(docs, &doc! { "_id": "$name", "secret": 0 }, &vars()).unwrap();
        assert_eq!(out[0].get("_id").unwrap(), &Bson::String("a".into()));
        assert!(!out[0].contains_key("secret"));
    }

    #[test]
    fn exclude_id_keeps_other_fields() {
        let docs = vec![doc! { "_id": 1, "name": "a" }];
        let out = execute(docs, &doc! { "_id": false }, &vars()).unwrap();
        assert!(!out[0].contains_key("_id"));
        assert_eq!(out[0].get_str("name").unwrap(), "a");
    }

    #[test]
    fn mixing_inclusion_exclusion_still_errors() {
        let docs = vec![doc! { "_id": 1, "a": 1, "b": 2 }];
        assert!(execute(docs, &doc! { "a": 1, "b": 0 }, &vars()).is_err());
    }

    #[test]
    fn default_id_included() {
        let docs = vec![doc! { "_id": 1, "name": "a" }];
        let out = execute(docs, &doc! { "name": 1 }, &vars()).unwrap();
        assert!(out[0].contains_key("_id"));
    }
}
