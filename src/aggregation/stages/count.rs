use bson::{Bson, Document, doc};

pub fn execute(docs: Vec<Document>, field: &str) -> anyhow::Result<Vec<Document>> {
    let count = docs.len() as i64;
    Ok(build_count_docs(field, count))
}

fn build_count_docs(field: &str, count: i64) -> Vec<Document> {
    if count == 0 {
        return Vec::new();
    }
    let value = if count >= i32::MIN as i64 && count <= i32::MAX as i64 {
        Bson::Int32(count as i32)
    } else {
        Bson::Int64(count)
    };
    vec![doc! { field: value }]
}

#[cfg(test)]
mod tests {
    use super::build_count_docs;
    use bson::Bson;

    #[test]
    fn count_empty_returns_no_docs() {
        let docs = build_count_docs("c", 0);
        assert!(docs.is_empty());
    }

    #[test]
    fn count_large_returns_i64() {
        let docs = build_count_docs("c", (i32::MAX as i64) + 1);
        assert_eq!(docs.len(), 1);
        let doc = &docs[0];
        match doc.get("c").unwrap() {
            Bson::Int64(v) => assert_eq!(*v, (i32::MAX as i64) + 1),
            other => panic!("expected Int64, got {:?}", other),
        }
    }
}
