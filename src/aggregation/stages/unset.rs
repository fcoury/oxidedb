use bson::Document;

pub fn execute(docs: Vec<Document>, fields: &[String]) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for mut doc in docs {
        for field in fields {
            doc.remove(field);
        }
        result.push(doc);
    }

    Ok(result)
}
