use bson::Document;

pub fn execute(docs: Vec<Document>, n: i64) -> anyhow::Result<Vec<Document>> {
    if n < 0 {
        return Err(anyhow::anyhow!("$skip value must be non-negative"));
    }

    let n_usize = n as usize;
    let result: Vec<Document> = docs.into_iter().skip(n_usize).collect();

    Ok(result)
}
