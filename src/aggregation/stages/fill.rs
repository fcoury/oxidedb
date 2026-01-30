use bson::{Bson, Document};

pub fn execute(docs: Vec<Document>, spec: &FillSpec) -> anyhow::Result<Vec<Document>> {
    if docs.is_empty() {
        return Ok(docs);
    }

    let mut result = docs;

    // Sort by sortBy if specified
    if let Some(ref sort_doc) = spec.sort_by {
        let mut sort_specs: Vec<(String, i32)> = Vec::new();
        for (key, value) in sort_doc.iter() {
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
    }

    // Process each output field specification
    for (field, _fill_spec) in spec.output.iter() {
        let mut last_value: Option<Bson> = None;

        // First pass: forward fill
        for doc in &mut result {
            if let Some(value) = doc.get(field) {
                if !matches!(value, Bson::Null) {
                    last_value = Some(value.clone());
                }
            } else if let Some(ref last) = last_value {
                doc.insert(field.clone(), last.clone());
            }
        }

        // Second pass: backward fill for remaining nulls (if method allows)
        last_value = None;
        for doc in result.iter_mut().rev() {
            if let Some(value) = doc.get(field) {
                if !matches!(value, Bson::Null) {
                    last_value = Some(value.clone());
                }
            } else if let Some(ref last) = last_value {
                doc.insert(field.clone(), last.clone());
            }
        }
    }

    Ok(result)
}

/// $fill stage specification
#[derive(Debug, Clone)]
pub struct FillSpec {
    pub partition_by: Option<Bson>,
    pub sort_by: Option<Document>,
    pub output: Document,
}

impl FillSpec {
    pub fn parse(value: &Bson) -> anyhow::Result<Self> {
        let doc = value
            .as_document()
            .ok_or_else(|| anyhow::anyhow!("$fill value must be a document"))?;

        let partition_by = doc.get("partitionBy").cloned();
        let sort_by = doc.get_document("sortBy").ok().cloned();
        let output = doc
            .get_document("output")
            .map_err(|_| anyhow::anyhow!("$fill requires output"))?
            .clone();

        Ok(Self {
            partition_by,
            sort_by,
            output,
        })
    }
}
