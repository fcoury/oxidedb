use bson::{Bson, Document};

pub fn execute(
    docs: Vec<Document>,
    path: &str,
    include_array_index: Option<&str>,
    preserve_null_and_empty_arrays: bool,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    // Remove leading $ from path if present
    let field_path = if let Some(stripped) = path.strip_prefix('$') {
        stripped
    } else {
        path
    };

    for doc in docs {
        // Get the field value
        let field_val = get_field_by_path(&doc, field_path);

        match field_val {
            Some(Bson::Array(arr)) if !arr.is_empty() => {
                // Unwind the array
                for (idx, item) in arr.iter().enumerate() {
                    let mut new_doc = doc.clone();

                    // Replace the array field with the item
                    set_field_by_path(&mut new_doc, field_path, item.clone());

                    // Add array index if specified
                    if let Some(index_field) = include_array_index {
                        let index_bson = if idx <= i32::MAX as usize {
                            Bson::Int32(idx as i32)
                        } else {
                            Bson::Int64(idx as i64)
                        };
                        new_doc.insert(index_field, index_bson);
                    }

                    result.push(new_doc);
                }
            }
            Some(Bson::Array(arr)) if arr.is_empty() && preserve_null_and_empty_arrays => {
                // Empty array - preserve as null if flag is set
                let mut new_doc = doc.clone();
                set_field_by_path(&mut new_doc, field_path, Bson::Null);

                if let Some(index_field) = include_array_index {
                    new_doc.insert(index_field, Bson::Null);
                }

                result.push(new_doc);
            }
            Some(Bson::Array(_)) | Some(Bson::Null) | None => {
                if preserve_null_and_empty_arrays {
                    // Missing field or null - preserve as null
                    let mut new_doc = doc.clone();
                    set_field_by_path(&mut new_doc, field_path, Bson::Null);

                    if let Some(index_field) = include_array_index {
                        new_doc.insert(index_field, Bson::Null);
                    }

                    result.push(new_doc);
                }
                // Otherwise: skip documents with empty arrays, nulls or
                // missing fields
            }
            Some(_) => {
                // Non-array scalar: treated as a single-element array, so the
                // document passes through unchanged (MongoDB >= 3.2)
                result.push(doc);
            }
        }
    }

    Ok(result)
}

fn get_field_by_path(doc: &Document, path: &str) -> Option<Bson> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = Bson::Document(doc.clone());

    for part in &parts {
        match current {
            Bson::Document(d) => {
                current = d.get(*part)?.clone();
            }
            _ => return None,
        }
    }

    Some(current)
}

fn set_field_by_path(doc: &mut Document, path: &str, value: Bson) {
    let parts: Vec<&str> = path.split('.').collect();

    if parts.len() == 1 {
        doc.insert(parts[0].to_string(), value);
        return;
    }

    // Navigate to the parent document
    let mut current = doc;
    for part in &parts[..parts.len() - 1] {
        if !current.contains_key(*part) {
            current.insert(*part, Bson::Document(Document::new()));
        }
        match current.get_mut(*part) {
            Some(Bson::Document(d)) => {
                current = d;
            }
            _ => return,
        }
    }

    // Set the final field
    current.insert(parts[parts.len() - 1].to_string(), value);
}

#[cfg(test)]
mod tests {
    use super::execute;
    use bson::{Bson, doc};

    #[test]
    fn non_array_scalar_passes_through() {
        let docs = vec![doc! { "_id": 1, "v": 42i32 }];
        let out = execute(docs, "$v", None, false).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_i32("v").unwrap(), 42);
    }

    #[test]
    fn null_field_dropped_without_preserve() {
        let docs = vec![doc! { "_id": 1, "v": Bson::Null }];
        let out = execute(docs, "$v", None, false).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn null_field_kept_with_preserve() {
        let docs = vec![doc! { "_id": 1, "v": Bson::Null }];
        let out = execute(docs, "$v", None, true).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].contains_key("v"));
    }

    #[test]
    fn array_still_unwinds() {
        let docs = vec![doc! { "_id": 1, "v": [1i32, 2i32] }];
        let out = execute(docs, "$v", None, false).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].get_i32("v").unwrap(), 1);
        assert_eq!(out[1].get_i32("v").unwrap(), 2);
    }
}
