use bson::Document;
use rand::seq::SliceRandom;
use rand::thread_rng;

pub fn execute(docs: Vec<Document>, size: i32) -> anyhow::Result<Vec<Document>> {
    if size < 1 {
        return Err(anyhow::anyhow!("$sample size must be >= 1"));
    }

    let size_usize = size as usize;

    if docs.len() <= size_usize {
        // Return all docs if we have fewer than requested
        return Ok(docs);
    }

    // Fisher-Yates shuffle and take first N
    let mut docs = docs;
    let mut rng = thread_rng();
    docs.shuffle(&mut rng);

    // Take the first size elements
    docs.truncate(size_usize);

    Ok(docs)
}
