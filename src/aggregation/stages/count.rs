use bson::{Document, doc};

pub fn execute(docs: Vec<Document>, field: &str) -> anyhow::Result<Vec<Document>> {
    let count = docs.len() as i32;
    let result = vec![doc! { field: count }];
    Ok(result)
}
