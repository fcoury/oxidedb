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
                        new_doc.insert(index_field, Bson::Int64(idx as i64));
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
            Some(_) if preserve_null_and_empty_arrays => {
                // Non-array value - treat as single-element array
                result.push(doc);
            }
            None if preserve_null_and_empty_arrays => {
                // Missing field - preserve as null
                let mut new_doc = doc.clone();
                set_field_by_path(&mut new_doc, field_path, Bson::Null);

                if let Some(index_field) = include_array_index {
                    new_doc.insert(index_field, Bson::Null);
                }

                result.push(new_doc);
            }
            Some(Bson::Array(_)) | None => {
                // Skip documents with empty arrays or missing fields
                // when preserve_null_and_empty_arrays is false
            }
            Some(_) => {
                // Non-array value and preserve is false - error
                return Err(anyhow::anyhow!(
                    "$unwind field '{}' must be an array, got {:?}",
                    field_path,
                    field_val
                ));
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
