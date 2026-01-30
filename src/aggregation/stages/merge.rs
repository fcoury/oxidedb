use crate::aggregation::exec::WriteStats;
use crate::store::PgStore;
use bson::{Bson, Document, doc};

/// $merge stage specification
#[derive(Debug, Clone)]
pub struct MergeSpec {
    pub into: MergeInto,
    pub on: Option<MergeOn>,
    pub when_matched: WhenMatched,
    pub when_not_matched: WhenNotMatched,
    pub let_vars: Option<Document>,
}

#[derive(Debug, Clone)]
pub enum MergeInto {
    String(String),
    Document { db: String, coll: String },
}

#[derive(Debug, Clone)]
pub enum MergeOn {
    Field(String),
    Fields(Vec<String>),
}

#[derive(Debug, Clone)]
pub enum WhenMatched {
    Merge,
    Replace,
    KeepExisting,
    Fail,
    Pipeline(Vec<Bson>),
}

#[derive(Debug, Clone)]
pub enum WhenNotMatched {
    Insert,
    Discard,
    Fail,
}

impl MergeSpec {
    pub fn parse(value: &Bson) -> anyhow::Result<Self> {
        let doc = value
            .as_document()
            .ok_or_else(|| anyhow::anyhow!("$merge value must be a document"))?;

        // Parse into
        let into = if let Ok(s) = doc.get_str("into") {
            MergeInto::String(s.to_string())
        } else if let Ok(d) = doc.get_document("into") {
            let db = d
                .get_str("db")
                .map_err(|_| anyhow::anyhow!("$merge into.db required"))?
                .to_string();
            let coll = d
                .get_str("coll")
                .map_err(|_| anyhow::anyhow!("$merge into.coll required"))?
                .to_string();
            MergeInto::Document { db, coll }
        } else {
            return Err(anyhow::anyhow!("$merge requires into field"));
        };

        // Parse on
        let on = if let Ok(s) = doc.get_str("on") {
            Some(MergeOn::Field(s.to_string()))
        } else if let Ok(arr) = doc.get_array("on") {
            let fields: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            if fields.is_empty() {
                return Err(anyhow::anyhow!("$merge on array must not be empty"));
            }
            Some(MergeOn::Fields(fields))
        } else {
            None
        };

        // Parse whenMatched
        let when_matched = if let Ok(s) = doc.get_str("whenMatched") {
            match s {
                "merge" => WhenMatched::Merge,
                "replace" => WhenMatched::Replace,
                "keepExisting" => WhenMatched::KeepExisting,
                "fail" => WhenMatched::Fail,
                _ => return Err(anyhow::anyhow!("Invalid whenMatched value: {}", s)),
            }
        } else if let Ok(arr) = doc.get_array("whenMatched") {
            WhenMatched::Pipeline(arr.clone())
        } else {
            WhenMatched::Merge // Default
        };

        // Parse whenNotMatched
        let when_not_matched = if let Ok(s) = doc.get_str("whenNotMatched") {
            match s {
                "insert" => WhenNotMatched::Insert,
                "discard" => WhenNotMatched::Discard,
                "fail" => WhenNotMatched::Fail,
                _ => return Err(anyhow::anyhow!("Invalid whenNotMatched value: {}", s)),
            }
        } else {
            WhenNotMatched::Insert // Default
        };

        let let_vars = doc.get_document("let").ok().cloned();

        Ok(Self {
            into,
            on,
            when_matched,
            when_not_matched,
            let_vars,
        })
    }
}

pub async fn execute(
    docs: Vec<Document>,
    pg: &PgStore,
    db: &str,
    spec: &MergeSpec,
) -> anyhow::Result<WriteStats> {
    let mut stats = WriteStats::default();

    // Determine target collection name
    let target_coll = match &spec.into {
        MergeInto::String(coll) => coll.clone(),
        MergeInto::Document { db: _, coll } => coll.clone(),
    };

    // Get the target database (may be different from source)
    let target_db = match &spec.into {
        MergeInto::String(_) => db.to_string(),
        MergeInto::Document { db: target_db, .. } => target_db.clone(),
    };

    for doc in docs {
        // Determine the match key value(s)
        let match_values = get_match_values(&doc, &spec.on);

        // Build filter to find matching document in target
        let filter = build_match_filter(&match_values);

        // Check if document exists in target
        let existing = if !filter.is_empty() {
            pg.find_docs(&target_db, &target_coll, Some(&filter), None, None, 1)
                .await?
        } else {
            Vec::new()
        };

        if let Some(existing_doc) = existing.first() {
            // Document exists - handle whenMatched
            match &spec.when_matched {
                WhenMatched::Merge => {
                    // Merge fields from input doc into existing doc
                    let mut merged = existing_doc.clone();
                    for (key, value) in doc.iter() {
                        if key != "_id" {
                            merged.insert(key.clone(), value.clone());
                        }
                    }
                    // Update the document
                    if let Some(id) = existing_doc.get("_id") {
                        let id_bytes = bson::to_vec(id)?;
                        let bson_bytes = bson::to_vec(&merged)?;
                        let json = serde_json::to_value(&merged)?;

                        // Delete and re-insert (since we don't have update_by_id exposed)
                        pg.delete_one_by_filter(&target_db, &target_coll, &doc! { "_id": id })
                            .await?;
                        pg.insert_one(&target_db, &target_coll, &id_bytes, &bson_bytes, &json)
                            .await?;

                        stats.matched_count += 1;
                        stats.modified_count += 1;
                    }
                }
                WhenMatched::Replace => {
                    // Replace the entire document
                    if let Some(id) = existing_doc.get("_id") {
                        let id_bytes = bson::to_vec(id)?;
                        let bson_bytes = bson::to_vec(&doc)?;
                        let json = serde_json::to_value(&doc)?;

                        pg.delete_one_by_filter(&target_db, &target_coll, &doc! { "_id": id })
                            .await?;
                        pg.insert_one(&target_db, &target_coll, &id_bytes, &bson_bytes, &json)
                            .await?;

                        stats.matched_count += 1;
                        stats.modified_count += 1;
                    }
                }
                WhenMatched::KeepExisting => {
                    // Do nothing
                    stats.matched_count += 1;
                }
                WhenMatched::Fail => {
                    return Err(anyhow::anyhow!(
                        "Merge failed: document with matching key already exists"
                    ));
                }
                WhenMatched::Pipeline(_) => {
                    // Complex pipeline merge - simplified for now
                    stats.matched_count += 1;
                }
            }
        } else {
            // Document doesn't exist - handle whenNotMatched
            match &spec.when_not_matched {
                WhenNotMatched::Insert => {
                    // Generate ID if not present
                    let id = doc
                        .get("_id")
                        .cloned()
                        .unwrap_or_else(|| bson::Bson::ObjectId(bson::oid::ObjectId::new()));
                    let id_bytes = bson::to_vec(&id)?;
                    let bson_bytes = bson::to_vec(&doc)?;
                    let json = serde_json::to_value(&doc)?;

                    pg.insert_one(&target_db, &target_coll, &id_bytes, &bson_bytes, &json)
                        .await?;
                    stats.inserted_count += 1;
                }
                WhenNotMatched::Discard => {
                    // Do nothing
                }
                WhenNotMatched::Fail => {
                    return Err(anyhow::anyhow!(
                        "Merge failed: no document with matching key found"
                    ));
                }
            }
        }
    }

    Ok(stats)
}

fn get_match_values(doc: &Document, on: &Option<MergeOn>) -> Vec<(String, Bson)> {
    let mut values = Vec::new();

    if let Some(merge_on) = on {
        match merge_on {
            MergeOn::Field(field) => {
                if let Some(val) = doc.get(field) {
                    values.push((field.clone(), val.clone()));
                }
            }
            MergeOn::Fields(fields) => {
                for field in fields {
                    if let Some(val) = doc.get(field) {
                        values.push((field.clone(), val.clone()));
                    }
                }
            }
        }
    } else {
        // Default to _id
        if let Some(id) = doc.get("_id") {
            values.push(("_id".to_string(), id.clone()));
        }
    }

    values
}

fn build_match_filter(match_values: &[(String, Bson)]) -> Document {
    let mut filter = Document::new();
    for (key, value) in match_values {
        filter.insert(key.clone(), value.clone());
    }
    filter
}
