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
    ReplaceRoot(Document),
    Facet(Document),
    Sample(i64),
    UnionWith(Document),
    Out(String), // collection name for $out
    Merge(Document), // merge specification for $merge
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
            "$replaceRoot" | "$replaceWith" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg(format!("{} must be a document", key)))?;
                if !d.contains_key("newRoot") {
                    return Err(Error::Msg(format!("{} must have a 'newRoot' field", key)));
                }
                Ok(AggregateStage::ReplaceRoot(d.clone()))
            }
            "$facet" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$facet must be a document".into()))?;
                Ok(AggregateStage::Facet(d.clone()))
            }
            "$sample" => {
                let n = val
                    .as_i64()
                    .or_else(|| val.as_i32().map(|i| i as i64))
                    .ok_or_else(|| Error::Msg("$sample must be a number".into()))?;
                if n <= 0 {
                    return Err(Error::Msg("$sample must be a positive integer".into()));
                }
                Ok(AggregateStage::Sample(n))
            }
            "$unionWith" => {
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$unionWith must be a document".into()))?;
                if !d.contains_key("coll") {
                    return Err(Error::Msg("$unionWith must have a 'coll' field".into()));
                }
                Ok(AggregateStage::UnionWith(d.clone()))
            }
            "$out" => {
                // $out can be a string (collection name) or a document with "coll" field
                if let Some(s) = val.as_str() {
                    Ok(AggregateStage::Out(s.to_string()))
                } else if let Some(d) = val.as_document() {
                    let coll = d
                        .get_str("coll")
                        .map_err(|_| Error::Msg("$out document must have a 'coll' field".into()))?;
                    Ok(AggregateStage::Out(coll.to_string()))
                } else {
                    Err(Error::Msg("$out must be a string or document".into()))
                }
            }
            "$merge" => {
                // $merge must be a document with required "into" field
                let d = val
                    .as_document()
                    .ok_or_else(|| Error::Msg("$merge must be a document".into()))?;
                if !d.contains_key("into") {
                    return Err(Error::Msg("$merge must have an 'into' field".into()));
                }
                // Validate optional fields if present
                if let Ok(on) = d.get_str("on")
                    && on.is_empty()
                {
                    return Err(Error::Msg("$merge 'on' field cannot be empty".into()));
                }
                if let Ok(when_matched) = d.get_str("whenMatched") {
                    let valid = ["merge", "replace", "keepExisting", "fail"];
                    if !valid.contains(&when_matched) {
                        return Err(Error::Msg(format!(
                            "$merge 'whenMatched' must be one of: {:?}",
                            valid
                        )));
                    }
                }
                if let Ok(when_not_matched) = d.get_str("whenNotMatched") {
                    let valid = ["insert", "discard"];
                    if !valid.contains(&when_not_matched) {
                        return Err(Error::Msg(format!(
                            "$merge 'whenNotMatched' must be one of: {:?}",
                            valid
                        )));
                    }
                }
                Ok(AggregateStage::Merge(d.clone()))
            }
            _ => Err(Error::Msg(format!("Unknown or unsupported stage: {}", key))),
        }
    }
}
