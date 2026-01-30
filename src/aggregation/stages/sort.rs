use crate::aggregation::values::bson_cmp;
use bson::{Bson, Document};
use std::cmp::Ordering;

pub fn execute(docs: Vec<Document>, spec: &Document) -> anyhow::Result<Vec<Document>> {
    let mut sort_specs: Vec<(String, i32)> = Vec::new();

    // Parse sort specification
    for (key, value) in spec.iter() {
        let direction = match value {
            Bson::Int32(n) => *n,
            Bson::Int64(n) => *n as i32,
            _ => return Err(anyhow::anyhow!("Sort direction must be 1 or -1")),
        };

        if direction != 1 && direction != -1 {
            return Err(anyhow::anyhow!("Sort direction must be 1 or -1"));
        }

        sort_specs.push((key.clone(), direction));
    }

    let mut result = docs;

    // Sort using stable sort with multiple keys
    result.sort_by(|a, b| {
        for (field, direction) in &sort_specs {
            let a_val = a.get(field).unwrap_or(&Bson::Null);
            let b_val = b.get(field).unwrap_or(&Bson::Null);

            let cmp = bson_cmp(a_val, b_val);

            if cmp != Ordering::Equal {
                return if *direction == 1 { cmp } else { cmp.reverse() };
            }
        }
        Ordering::Equal
    });

    Ok(result)
}
