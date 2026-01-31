use crate::aggregation::expr::{ExprEvalContext, eval_expr, parse_expr};
use bson::{Bson, Document};
use std::collections::HashMap;

pub fn execute(
    docs: Vec<Document>,
    spec: &Document,
    vars: &HashMap<String, Bson>,
) -> anyhow::Result<Vec<Document>> {
    let mut result = Vec::new();

    for doc in docs {
        let ctx = ExprEvalContext::with_vars(doc.clone(), doc.clone(), vars.clone());
        let mut new_doc = doc.clone();

        for (key, value) in spec.iter() {
            let expr = parse_expr(value)?;
            let evaluated = eval_expr(&expr, &ctx)?;
            // Skip fields that evaluate to $$REMOVE (Bson::Undefined)
            if matches!(evaluated, Bson::Undefined) {
                unset_path_nested(&mut new_doc, key);
            } else {
                set_path_nested(&mut new_doc, key, evaluated);
            }
        }

        result.push(new_doc);
    }

    Ok(result)
}

fn set_path_nested(doc: &mut Document, path: &str, value: Bson) {
    let mut root = Bson::Document(doc.clone());
    let segments: Vec<&str> = path.split('.').collect();
    set_path_bson(&mut root, &segments, value);
    if let Bson::Document(updated) = root {
        *doc = updated;
    }
}

fn seg_is_index(seg: &str) -> Option<usize> {
    seg.parse::<usize>().ok()
}

fn set_path_bson(cur: &mut Bson, segs: &[&str], value: Bson) {
    if segs.is_empty() {
        *cur = value;
        return;
    }
    let seg = segs[0];
    let next_is_index = segs.get(1).and_then(|s| seg_is_index(s)).is_some();
    match seg_is_index(seg) {
        Some(idx) => {
            if !matches!(cur, Bson::Array(_)) {
                *cur = Bson::Array(Vec::new());
            }
            if let Bson::Array(arr) = cur {
                let desired_len = idx + 1;
                if arr.len() < desired_len {
                    let pad = desired_len - arr.len();
                    arr.extend(std::iter::repeat_n(Bson::Null, pad));
                }
                if segs.len() == 1 {
                    arr[idx] = value;
                } else {
                    if matches!(arr[idx], Bson::Null) {
                        arr[idx] = if next_is_index {
                            Bson::Array(Vec::new())
                        } else {
                            Bson::Document(Document::new())
                        };
                    }
                    set_path_bson(&mut arr[idx], &segs[1..], value);
                }
            }
        }
        None => {
            if !matches!(cur, Bson::Document(_)) {
                *cur = Bson::Document(Document::new());
            }
            if let Bson::Document(d) = cur {
                if segs.len() == 1 {
                    d.insert(seg, value);
                } else {
                    let entry = d.entry(seg.to_string());
                    let child = entry.or_insert_with(|| {
                        if next_is_index {
                            Bson::Array(Vec::new())
                        } else {
                            Bson::Document(Document::new())
                        }
                    });
                    set_path_bson(child, &segs[1..], value);
                }
            }
        }
    }
}

fn unset_path_nested(doc: &mut Document, path: &str) {
    let mut root = Bson::Document(doc.clone());
    let segments: Vec<&str> = path.split('.').collect();
    unset_path_bson(&mut root, &segments);
    if let Bson::Document(updated) = root {
        *doc = updated;
    }
}

fn unset_path_bson(cur: &mut Bson, segs: &[&str]) {
    if segs.is_empty() {
        return;
    }
    let seg = segs[0];
    match seg.parse::<usize>().ok() {
        Some(idx) => {
            if let Bson::Array(arr) = cur
                && idx < arr.len()
            {
                if segs.len() == 1 {
                    arr[idx] = Bson::Null;
                } else {
                    unset_path_bson(&mut arr[idx], &segs[1..]);
                }
            }
        }
        None => {
            if let Bson::Document(d) = cur {
                if segs.len() == 1 {
                    d.remove(seg);
                } else if let Some(child) = d.get_mut(seg) {
                    unset_path_bson(child, &segs[1..]);
                }
            }
        }
    }
}
