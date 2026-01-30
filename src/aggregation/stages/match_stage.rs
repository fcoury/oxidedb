use bson::{Bson, Document};

pub fn execute(docs: Vec<Document>, filter: &Document) -> anyhow::Result<Vec<Document>> {
    let result: Vec<Document> = docs
        .into_iter()
        .filter(|doc| document_matches_filter(doc, filter))
        .collect();

    Ok(result)
}

fn document_matches_filter(doc: &Document, filter: &Document) -> bool {
    for (key, value) in filter.iter() {
        if key.starts_with('$') {
            // Logical operator
            match key.as_str() {
                "$and" => {
                    if let Bson::Array(arr) = value {
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond {
                                if !document_matches_filter(doc, cond_doc) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                "$or" => {
                    if let Bson::Array(arr) = value {
                        let mut any_match = false;
                        for cond in arr {
                            if let Bson::Document(cond_doc) = cond {
                                if document_matches_filter(doc, cond_doc) {
                                    any_match = true;
                                    break;
                                }
                            }
                        }
                        if !any_match {
                            return false;
                        }
                    }
                }
                "$not" => {
                    if let Bson::Document(cond_doc) = value {
                        if document_matches_filter(doc, cond_doc) {
                            return false;
                        }
                    }
                }
                _ => {}
            }
        } else {
            // Field match
            let doc_val = doc.get(key);
            if !value_matches(doc_val, value) {
                return false;
            }
        }
    }
    true
}

fn value_matches(doc_val: Option<&Bson>, filter_val: &Bson) -> bool {
    match filter_val {
        Bson::Document(filter_doc) => {
            // Check for operators
            for (op, op_val) in filter_doc.iter() {
                match op.as_str() {
                    "$eq" => {
                        if doc_val != Some(op_val) {
                            return false;
                        }
                    }
                    "$ne" => {
                        if doc_val == Some(op_val) {
                            return false;
                        }
                    }
                    "$gt" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            if crate::aggregation::bson_cmp(dv, fv) != std::cmp::Ordering::Greater {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$gte" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            let cmp = crate::aggregation::bson_cmp(dv, fv);
                            if cmp != std::cmp::Ordering::Greater
                                && cmp != std::cmp::Ordering::Equal
                            {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lt" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            if crate::aggregation::bson_cmp(dv, fv) != std::cmp::Ordering::Less {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$lte" => {
                        if let (Some(dv), Some(fv)) = (doc_val, Some(op_val)) {
                            let cmp = crate::aggregation::bson_cmp(dv, fv);
                            if cmp != std::cmp::Ordering::Less && cmp != std::cmp::Ordering::Equal {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    "$in" => {
                        if let Bson::Array(arr) = op_val {
                            if let Some(dv) = doc_val {
                                if !arr.contains(dv) {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                    "$nin" => {
                        if let Bson::Array(arr) = op_val {
                            if let Some(dv) = doc_val {
                                if arr.contains(dv) {
                                    return false;
                                }
                            }
                        }
                    }
                    "$exists" => {
                        let should_exist = op_val.as_bool().unwrap_or(true);
                        let does_exist = doc_val.is_some();
                        if should_exist != does_exist {
                            return false;
                        }
                    }
                    "$regex" => {
                        if let Some(Bson::String(doc_str)) = doc_val {
                            let pattern = match op_val {
                                Bson::String(p) => p,
                                _ => return false,
                            };
                            // Simple substring match for now
                            if !doc_str.contains(pattern) {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                    _ => {}
                }
            }
            true
        }
        _ => doc_val == Some(filter_val),
    }
}
