use bson::{Bson, Document};
use std::collections::HashMap;

/// Aggregate command options
#[derive(Debug, Clone, Default)]
pub struct AggregateOptions {
    pub allow_disk_use: bool,
    pub max_time_ms: Option<u64>,
    pub collation: Option<Document>,
    pub hint: Option<Document>,
    pub comment: Option<Bson>,
    pub let_vars: Document,
    pub bypass_document_validation: bool,
    pub read_concern: Option<Document>,
    pub write_concern: Option<Document>,
    pub explain: bool,
}

impl AggregateOptions {
    /// Parse options from aggregate command document
    pub fn from_command(cmd: &Document) -> Self {
        Self {
            allow_disk_use: cmd.get_bool("allowDiskUse").unwrap_or(false),
            max_time_ms: cmd.get_i64("maxTimeMS").ok().map(|v| v as u64),
            collation: cmd.get_document("collation").ok().cloned(),
            hint: cmd.get_document("hint").ok().cloned(),
            comment: cmd.get("comment").cloned(),
            let_vars: cmd.get_document("let").ok().cloned().unwrap_or_default(),
            bypass_document_validation: cmd.get_bool("bypassDocumentValidation").unwrap_or(false),
            read_concern: cmd.get_document("readConcern").ok().cloned(),
            write_concern: cmd.get_document("writeConcern").ok().cloned(),
            explain: cmd.contains_key("explain"),
        }
    }
}

/// Pipeline stage
#[derive(Debug, Clone)]
pub enum Stage {
    Match(Document),
    Project(Document),
    AddFields(Document),
    Set(Document),
    Unset(Vec<String>),
    ReplaceRoot {
        replacement: Bson,
    },
    ReplaceWith(Bson),
    Sort(Document),
    Limit(i64),
    Skip(i64),
    Count(String),
    Group {
        id: Bson,
        accumulators: Document,
    },
    Bucket {
        group_by: Bson,
        boundaries: Vec<Bson>,
        default: Option<Bson>,
        output: Option<Document>,
    },
    BucketAuto {
        group_by: Bson,
        buckets: i32,
        granularity: Option<String>,
        output: Option<Document>,
    },
    Lookup {
        from: String,
        local_field: Option<String>,
        foreign_field: Option<String>,
        as_field: String,
        // Pipeline form
        let_vars: Option<Document>,
        pipeline: Option<Vec<Bson>>,
    },
    Unwind {
        path: String,
        include_array_index: Option<String>,
        preserve_null_and_empty_arrays: bool,
    },
    Sample(i32),
    Facet(HashMap<String, Vec<Stage>>),
    UnionWith {
        coll: String,
        pipeline: Vec<Stage>,
    },
    GeoNear(crate::aggregation::stages::GeoNearSpec),
    Out(String),
    Merge(crate::aggregation::stages::MergeSpec),
    SortByCount(Bson),
    SetWindowFields(crate::aggregation::stages::SetWindowFieldsSpec),
    Densify(crate::aggregation::stages::DensifySpec),
    Fill(crate::aggregation::stages::FillSpec),
    Redact(Bson),
}

/// Parsed pipeline
#[derive(Debug)]
pub struct Pipeline {
    pub stages: Vec<Stage>,
    pub options: AggregateOptions,
}

impl Pipeline {
    /// Parse pipeline from command document
    pub fn parse(cmd: &Document) -> anyhow::Result<Self> {
        let options = AggregateOptions::from_command(cmd);

        // Extract pipeline array
        let pipeline_array = cmd
            .get_array("pipeline")
            .map_err(|_| anyhow::anyhow!("aggregate command requires pipeline array"))?;

        let mut stages = Vec::new();
        #[allow(unused_variables)]
        let _has_geo_near = false;
        let mut has_out = false;
        let mut has_merge = false;
        let mut has_facet = false;

        for (idx, stage_bson) in pipeline_array.iter().enumerate() {
            let stage_doc = stage_bson
                .as_document()
                .ok_or_else(|| anyhow::anyhow!("pipeline stage must be a document"))?;

            let stage = Self::parse_stage(stage_doc)?;

            // Validate stage ordering and constraints
            match &stage {
                Stage::GeoNear(_) => {
                    if idx != 0 {
                        return Err(anyhow::anyhow!(
                            "$geoNear must be the first stage in the pipeline"
                        ));
                    }
                }
                Stage::Out(_) => {
                    if has_out || has_merge {
                        return Err(anyhow::anyhow!(
                            "only one $out or $merge stage allowed per pipeline"
                        ));
                    }
                    if idx != pipeline_array.len() - 1 {
                        return Err(anyhow::anyhow!("$out must be the last stage"));
                    }
                    has_out = true;
                }
                Stage::Merge(_) => {
                    if has_out || has_merge {
                        return Err(anyhow::anyhow!(
                            "only one $out or $merge stage allowed per pipeline"
                        ));
                    }
                    if idx != pipeline_array.len() - 1 {
                        return Err(anyhow::anyhow!("$merge must be the last stage"));
                    }
                    has_merge = true;
                }
                Stage::Facet(_) => {
                    if has_facet {
                        return Err(anyhow::anyhow!("only one $facet stage allowed"));
                    }
                    if idx != pipeline_array.len() - 1 {
                        return Err(anyhow::anyhow!("$facet must be the last stage"));
                    }
                    has_facet = true;
                }
                Stage::Match(filter) => {
                    // Validate $match restrictions
                    Self::validate_match_filter(filter, idx == 0)?;
                }
                _ => {}
            }

            stages.push(stage);
        }

        Ok(Self { stages, options })
    }

    /// Parse a single stage document
    fn parse_stage(doc: &Document) -> anyhow::Result<Stage> {
        if doc.is_empty() {
            return Err(anyhow::anyhow!("empty pipeline stage"));
        }

        let (stage_name, stage_value) = doc.iter().next().unwrap();

        match stage_name.as_str() {
            "$match" => {
                let filter = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$match value must be a document"))?
                    .clone();
                Ok(Stage::Match(filter))
            }
            "$project" => {
                let spec = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$project value must be a document"))?
                    .clone();
                Ok(Stage::Project(spec))
            }
            "$addFields" => {
                let spec = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$addFields value must be a document"))?
                    .clone();
                Ok(Stage::AddFields(spec))
            }
            "$set" => {
                let spec = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$set value must be a document"))?
                    .clone();
                Ok(Stage::Set(spec))
            }
            "$unset" => {
                let fields: Vec<String> = if let Some(arr) = stage_value.as_array() {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                } else if let Some(s) = stage_value.as_str() {
                    vec![s.to_string()]
                } else {
                    return Err(anyhow::anyhow!("$unset value must be string or array"));
                };
                Ok(Stage::Unset(fields))
            }
            "$replaceRoot" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$replaceRoot value must be a document"))?;
                let replacement = doc
                    .get("newRoot")
                    .ok_or_else(|| anyhow::anyhow!("$replaceRoot requires newRoot"))?
                    .clone();
                Ok(Stage::ReplaceRoot { replacement })
            }
            "$replaceWith" => Ok(Stage::ReplaceWith(stage_value.clone())),
            "$sort" => {
                let spec = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$sort value must be a document"))?
                    .clone();
                Ok(Stage::Sort(spec))
            }
            "$limit" => {
                let n = if let Some(n) = stage_value.as_i64() {
                    n
                } else if let Some(n) = stage_value.as_i32() {
                    n as i64
                } else {
                    return Err(anyhow::anyhow!("$limit value must be an integer"));
                };
                if n < 0 {
                    return Err(anyhow::anyhow!("$limit value must be non-negative"));
                }
                Ok(Stage::Limit(n))
            }
            "$skip" => {
                let n = if let Some(n) = stage_value.as_i64() {
                    n
                } else if let Some(n) = stage_value.as_i32() {
                    n as i64
                } else {
                    return Err(anyhow::anyhow!("$skip value must be an integer"));
                };
                if n < 0 {
                    return Err(anyhow::anyhow!("$skip value must be non-negative"));
                }
                Ok(Stage::Skip(n))
            }
            "$count" => {
                let field = if let Some(s) = stage_value.as_str() {
                    s.to_string()
                } else {
                    return Err(anyhow::anyhow!("$count value must be a string"));
                };
                // Validate field name
                if field.is_empty()
                    || field.starts_with('$')
                    || field.contains('.')
                    || field.contains('\0')
                {
                    return Err(anyhow::anyhow!(
                        "$count field name must be non-empty, not start with $, not contain . or null"
                    ));
                }
                Ok(Stage::Count(field))
            }
            "$group" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$group value must be a document"))?;
                let id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                let mut accumulators = Document::new();
                for (k, v) in doc.iter() {
                    if k != "_id" {
                        accumulators.insert(k.clone(), v.clone());
                    }
                }
                Ok(Stage::Group { id, accumulators })
            }
            "$bucket" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$bucket value must be a document"))?;
                let group_by = doc
                    .get("groupBy")
                    .ok_or_else(|| anyhow::anyhow!("$bucket requires groupBy"))?
                    .clone();
                let boundaries = doc
                    .get_array("boundaries")
                    .map_err(|_| anyhow::anyhow!("$bucket requires boundaries array"))?
                    .clone();
                let default = doc.get("default").cloned();
                let output = doc.get_document("output").ok().cloned();

                // Validate boundaries
                if boundaries.len() < 2 {
                    return Err(anyhow::anyhow!("$bucket requires at least 2 boundaries"));
                }

                Ok(Stage::Bucket {
                    group_by,
                    boundaries,
                    default,
                    output,
                })
            }
            "$bucketAuto" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$bucketAuto value must be a document"))?;
                let group_by = doc
                    .get("groupBy")
                    .ok_or_else(|| anyhow::anyhow!("$bucketAuto requires groupBy"))?
                    .clone();
                let buckets = doc
                    .get_i32("buckets")
                    .map_err(|_| anyhow::anyhow!("$bucketAuto requires buckets"))?;
                if buckets < 1 {
                    return Err(anyhow::anyhow!("$bucketAuto buckets must be >= 1"));
                }
                let granularity = doc.get_str("granularity").ok().map(|s| s.to_string());
                let output = doc.get_document("output").ok().cloned();

                Ok(Stage::BucketAuto {
                    group_by,
                    buckets,
                    granularity,
                    output,
                })
            }
            "$lookup" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$lookup value must be a document"))?;

                let from = doc
                    .get_str("from")
                    .map_err(|_| anyhow::anyhow!("$lookup requires from"))?
                    .to_string();
                let as_field = doc
                    .get_str("as")
                    .map_err(|_| anyhow::anyhow!("$lookup requires as"))?
                    .to_string();

                // Check for pipeline form
                let let_vars = doc.get_document("let").ok().cloned();
                let pipeline = doc.get_array("pipeline").ok().cloned();

                // Check for simple form
                let local_field = doc.get_str("localField").ok().map(|s| s.to_string());
                let foreign_field = doc.get_str("foreignField").ok().map(|s| s.to_string());

                Ok(Stage::Lookup {
                    from,
                    local_field,
                    foreign_field,
                    as_field,
                    let_vars,
                    pipeline,
                })
            }
            "$unwind" => {
                if let Some(path) = stage_value.as_str() {
                    Ok(Stage::Unwind {
                        path: path.to_string(),
                        include_array_index: None,
                        preserve_null_and_empty_arrays: false,
                    })
                } else if let Some(doc) = stage_value.as_document() {
                    let path = doc
                        .get_str("path")
                        .map_err(|_| anyhow::anyhow!("$unwind requires path"))?
                        .to_string();
                    let include_array_index =
                        doc.get_str("includeArrayIndex").ok().map(|s| s.to_string());
                    let preserve_null_and_empty_arrays =
                        doc.get_bool("preserveNullAndEmptyArrays").unwrap_or(false);
                    Ok(Stage::Unwind {
                        path,
                        include_array_index,
                        preserve_null_and_empty_arrays,
                    })
                } else {
                    Err(anyhow::anyhow!("$unwind value must be string or document"))
                }
            }
            "$sample" => {
                let size = if let Some(n) = stage_value.as_i32() {
                    n
                } else if let Some(n) = stage_value.as_i64() {
                    n as i32
                } else {
                    return Err(anyhow::anyhow!("$sample value must be an integer"));
                };
                if size < 1 {
                    return Err(anyhow::anyhow!("$sample size must be >= 1"));
                }
                Ok(Stage::Sample(size))
            }
            "$facet" => {
                let doc = stage_value
                    .as_document()
                    .ok_or_else(|| anyhow::anyhow!("$facet value must be a document"))?;

                let mut facets = HashMap::new();
                for (name, pipeline_bson) in doc.iter() {
                    let pipeline_array = pipeline_bson
                        .as_array()
                        .ok_or_else(|| anyhow::anyhow!("$facet pipeline must be an array"))?;

                    let mut sub_stages = Vec::new();
                    for stage_bson in pipeline_array {
                        let stage_doc = stage_bson
                            .as_document()
                            .ok_or_else(|| anyhow::anyhow!("pipeline stage must be a document"))?;
                        let stage = Self::parse_stage(stage_doc)?;

                        // Check for forbidden stages in facet subpipeline
                        match &stage {
                            Stage::Out(_) | Stage::Merge(_) | Stage::GeoNear(_) => {
                                return Err(anyhow::anyhow!(
                                    "{} stage not allowed in $facet subpipeline",
                                    stage_doc.keys().next().unwrap()
                                ));
                            }
                            _ => {}
                        }

                        sub_stages.push(stage);
                    }
                    facets.insert(name.clone(), sub_stages);
                }
                Ok(Stage::Facet(facets))
            }
            "$unionWith" => {
                if let Some(coll) = stage_value.as_str() {
                    Ok(Stage::UnionWith {
                        coll: coll.to_string(),
                        pipeline: Vec::new(),
                    })
                } else if let Some(doc) = stage_value.as_document() {
                    let coll = doc
                        .get_str("coll")
                        .map_err(|_| anyhow::anyhow!("$unionWith requires coll"))?
                        .to_string();
                    let pipeline = if let Ok(arr) = doc.get_array("pipeline") {
                        arr.iter()
                            .filter_map(|v| v.as_document())
                            .map(Self::parse_stage)
                            .collect::<Result<Vec<_>, _>>()?
                    } else {
                        Vec::new()
                    };
                    Ok(Stage::UnionWith { coll, pipeline })
                } else {
                    Err(anyhow::anyhow!(
                        "$unionWith value must be string or document"
                    ))
                }
            }
            "$geoNear" => {
                let spec = crate::aggregation::stages::GeoNearSpec::parse(stage_value)?;
                Ok(Stage::GeoNear(spec))
            }
            "$out" => {
                let coll = if let Some(s) = stage_value.as_str() {
                    s.to_string()
                } else if let Some(doc) = stage_value.as_document() {
                    doc.get_str("coll")
                        .map_err(|_| anyhow::anyhow!("$out document requires coll"))?
                        .to_string()
                } else {
                    return Err(anyhow::anyhow!("$out value must be string or document"));
                };
                Ok(Stage::Out(coll))
            }
            "$merge" => {
                let spec = crate::aggregation::stages::MergeSpec::parse(stage_value)?;
                Ok(Stage::Merge(spec))
            }
            "$sortByCount" => Ok(Stage::SortByCount(stage_value.clone())),
            "$setWindowFields" => {
                let spec = crate::aggregation::stages::SetWindowFieldsSpec::parse(stage_value)?;
                Ok(Stage::SetWindowFields(spec))
            }
            "$densify" => {
                let spec = crate::aggregation::stages::DensifySpec::parse(stage_value)?;
                Ok(Stage::Densify(spec))
            }
            "$fill" => {
                let spec = crate::aggregation::stages::FillSpec::parse(stage_value)?;
                Ok(Stage::Fill(spec))
            }
            "$redact" => Ok(Stage::Redact(stage_value.clone())),
            _ => Err(anyhow::anyhow!("Unknown pipeline stage: {}", stage_name)),
        }
    }

    /// Validate $match filter restrictions
    fn validate_match_filter(filter: &Document, is_first_stage: bool) -> anyhow::Result<()> {
        // Check for $where (always forbidden)
        if filter.contains_key("$where") {
            return Err(anyhow::anyhow!("$where is not allowed in $match"));
        }

        // Check for $near/$nearSphere (always forbidden in aggregation)
        if filter.contains_key("$near") || filter.contains_key("$nearSphere") {
            return Err(anyhow::anyhow!(
                "$near and $nearSphere are not allowed in $match, use $geoNear instead"
            ));
        }

        // Check for $text (only allowed in first stage)
        if filter.contains_key("$text") && !is_first_stage {
            return Err(anyhow::anyhow!(
                "$match with $text is only allowed as the first pipeline stage"
            ));
        }

        Ok(())
    }
}
