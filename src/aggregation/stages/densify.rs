use bson::{Bson, Document};

pub fn execute(docs: Vec<Document>, spec: &DensifySpec) -> anyhow::Result<Vec<Document>> {
    if docs.is_empty() {
        return Ok(docs);
    }

    let mut result = docs.clone();

    // Sort documents by the densify field if needed
    // This is a simplified implementation

    // For each field to densify
    for (field, _options) in spec.output.iter() {
        // Sort by this field
        result.sort_by(|a, b| {
            let a_val = a.get(field).unwrap_or(&Bson::Null);
            let b_val = b.get(field).unwrap_or(&Bson::Null);
            crate::aggregation::bson_cmp(a_val, b_val)
        });

        // Fill in missing values (simplified - just forward fill)
        let mut last_value: Option<Bson> = None;
        for doc in &mut result {
            if let Some(value) = doc.get(field) {
                if !matches!(value, Bson::Null) {
                    last_value = Some(value.clone());
                }
            }

            // If field is missing or null, fill it
            if !doc.contains_key(field) || matches!(doc.get(field), Some(Bson::Null)) {
                if let Some(ref last) = last_value {
                    doc.insert(field.clone(), last.clone());
                }
            }
        }
    }

    Ok(result)
}

/// $densify stage specification
#[derive(Debug, Clone)]
pub struct DensifySpec {
    pub partition_by: Option<Bson>,
    pub sort_by: Option<Document>,
    pub output: Document,
}

impl DensifySpec {
    pub fn parse(value: &Bson) -> anyhow::Result<Self> {
        let doc = value
            .as_document()
            .ok_or_else(|| anyhow::anyhow!("$densify value must be a document"))?;

        let partition_by = doc.get("partitionBy").cloned();
        let sort_by = doc.get_document("sortBy").ok().cloned();
        let output = doc
            .get_document("output")
            .map_err(|_| anyhow::anyhow!("$densify requires output"))?
            .clone();

        Ok(Self {
            partition_by,
            sort_by,
            output,
        })
    }
}
