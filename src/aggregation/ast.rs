use crate::error::{Error, Result};
use bson::Document;

#[derive(Debug, Clone)]
pub enum AggregateStage {
    Match(Document),
    Project(Document),
    Sort(Document),
    Limit(i64),
    Skip(i64),
    Unwind(String, bool, Option<String>), // path, preserve, include_index
    Group(Document),
    // Lookup, etc. will be added later
}

impl AggregateStage {
    pub fn parse(doc: Document) -> Result<Self> {
        if doc.len() != 1 {
            return Err(Error::Msg("Stage must have exactly one operator".into()));
        }
        let (key, val) = doc.iter().next().unwrap();
        match key.as_str() {
            "$match" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$match must be a document".into()))?;
                Ok(AggregateStage::Match(d.clone()))
            }
            "$project" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$project must be a document".into()))?;
                Ok(AggregateStage::Project(d.clone()))
            }
            "$sort" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$sort must be a document".into()))?;
                Ok(AggregateStage::Sort(d.clone()))
            }
            "$limit" => {
                let n = val
                    .as_i64()
                    .or_else(|| val.as_i32().map(|i| i as i64))
                    .ok_or_else(|| Error::Msg("$limit must be a number".into()))?;
                Ok(AggregateStage::Limit(n))
            }
            "$skip" => {
                let n = val
                    .as_i64()
                    .or_else(|| val.as_i32().map(|i| i as i64))
                    .ok_or_else(|| Error::Msg("$skip must be a number".into()))?;
                Ok(AggregateStage::Skip(n))
            }
            "$unwind" => {
                if let Some(s) = val.as_str() {
                    if !s.starts_with('$') {
                        return Err(Error::Msg("$unwind path must start with '$'".into()));
                    }
                    Ok(AggregateStage::Unwind(s[1..].to_string(), false, None))
                } else if let Some(d) = val.as_document() {
                    let path = d
                        .get_str("path")
                        .map_err(|_| Error::Msg("$unwind missing path".into()))?;
                    if !path.starts_with('$') {
                        return Err(Error::Msg("$unwind path must start with '$'".into()));
                    }
                    let preserve = d.get_bool("preserveNullAndEmptyArrays").unwrap_or(false);
                    let include = d.get_str("includeArrayIndex").ok().map(|s| s.to_string());
                    Ok(AggregateStage::Unwind(
                        path[1..].to_string(),
                        preserve,
                        include,
                    ))
                } else {
                    Err(Error::Msg("$unwind must be string or object".into()))
                }
            }
            "$group" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$group must be a document".into()))?;
                Ok(AggregateStage::Group(d.clone()))
            }
            _ => Err(Error::Msg(format!("Unknown or unsupported stage: {}", key))),
        }
    }
}
