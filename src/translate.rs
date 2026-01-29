use serde_json::{Map, Value};

pub enum WhereSpec {
    Raw(String),
    Containment(serde_json::Value),
}

pub fn build_where_from_filter(filter: &bson::Document) -> String {
    let mut where_clauses: Vec<String> = Vec::new();
    for (k, v) in filter.iter() {
        if k == "_id" {
            continue;
        }
        let path = jsonpath_path(k);
        match v {
            bson::Bson::Document(d) => {
                for (op, val) in d.iter() {
                    match op.as_str() {
                        "$elemMatch" => {
                            if let bson::Bson::Document(em) = val {
                                if let Some(pred) = build_elem_match_pred(&path, em) {
                                    where_clauses.push(format!(
                                        "jsonb_path_exists(doc, ‘{}’ ? ({} ))",
                                        escape_single(&path),
                                        pred
                                    ));
                                }
                            }
                        }
                        "$exists" => {
                            let clause = if matches!(val, bson::Bson::Boolean(true)) {
                                format!("jsonb_path_exists(doc, '{}')", escape_single(&path))
                            } else {
                                format!("NOT jsonb_path_exists(doc, '{}')", escape_single(&path))
                            };
                            where_clauses.push(clause);
                        }
                        "$in" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut preds: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
                                        preds.push(format!("@ == {}", lit));
                                    }
                                }
                                if preds.is_empty() {
                                    where_clauses.push("FALSE".to_string());
                                } else {
                                    let predicate = preds.join(" || ");
                                    let p1 = format!(
                                        "jsonb_path_exists(doc, '{} ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    let p2 = format!(
                                        "jsonb_path_exists(doc, '{}[*] ? ({} )')",
                                        escape_single(&path),
                                        predicate
                                    );
                                    where_clauses.push(format!("({} OR {})", p1, p2));
                                }
                            }
                        }
                        "$gt" | "$gte" | "$lt" | "$lte" => {
                            let op_sql = match op.as_str() {
                                "$gt" => ">",
                                "$gte" => ">=",
                                "$lt" => "<",
                                "$lte" => "<=",
                                _ => unreachable!(),
                            };
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ {} {} )')",
                                    escape_single(&path),
                                    op_sql,
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ {} {} )')",
                                    escape_single(&path),
                                    op_sql,
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        "$eq" => {
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                if let Some(lit) = json_literal_from_bson(v) {
                    let p1 = format!(
                        "jsonb_path_exists(doc, '{} ? (@ == {} )')",
                        escape_single(&path),
                        lit
                    );
                    let p2 = format!(
                        "jsonb_path_exists(doc, '{}[*] ? (@ == {} )')",
                        escape_single(&path),
                        lit
                    );
                    where_clauses.push(format!("({} OR {})", p1, p2));
                }
            }
        }
    }
    if where_clauses.is_empty() {
        String::from("TRUE")
    } else {
        where_clauses.join(" AND ")
    }
}

pub fn build_where_spec(filter: &bson::Document) -> WhereSpec {
    let mut m = Map::new();
    for (k, v) in filter.iter() {
        if k == "_id" {
            continue;
        }
        if k.contains('.') {
            return WhereSpec::Raw(build_where_from_filter(filter));
        }
        match v {
            bson::Bson::Document(d) => {
                if d.len() == 1 {
                    if let Some(eqv) = d.get("$eq") {
                        if let Some(jv) = json_value_from_bson(eqv) {
                            m.insert(k.clone(), jv);
                            continue;
                        } else {
                            return WhereSpec::Raw(build_where_from_filter(filter));
                        }
                    } else {
                        return WhereSpec::Raw(build_where_from_filter(filter));
                    }
                } else {
                    return WhereSpec::Raw(build_where_from_filter(filter));
                }
            }
            other => {
                if let Some(jv) = json_value_from_bson(other) {
                    m.insert(k.clone(), jv);
                } else {
                    return WhereSpec::Raw(build_where_from_filter(filter));
                }
            }
        }
    }
    if m.is_empty() {
        WhereSpec::Raw(build_where_from_filter(filter))
    } else {
        WhereSpec::Containment(Value::Object(m))
    }
}

pub fn build_order_by(sort: Option<&bson::Document>) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut has_id = false;
    if let Some(spec) = sort {
        for (k, v) in spec.iter() {
            let dir = match v {
                bson::Bson::Int32(n) => *n,
                bson::Bson::Int64(n) => *n as i32,
                bson::Bson::Double(f) => {
                    if *f < 0.0 {
                        -1
                    } else {
                        1
                    }
                }
                _ => 1,
            };
            let ord = if dir < 0 { "DESC" } else { "ASC" };
            if k == "_id" {
                has_id = true;
                parts.push(format!("id {}", ord));
            } else {
                let f = escape_single(k);
                let numeric_re = format!("(doc->>'{}') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'", f);
                let num_first = format!("(CASE WHEN {} THEN 0 ELSE 1 END) ASC", numeric_re);
                let num_val = format!(
                    "(CASE WHEN {} THEN (doc->>'{}')::double precision END) {}",
                    numeric_re, f, ord
                );
                let text_val = format!("(doc->>'{}') {}", f, ord);
                parts.push(num_first);
                parts.push(num_val);
                parts.push(text_val);
            }
        }
    }
    if !has_id {
        parts.push("id ASC".to_string());
    }
    format!("ORDER BY {}", parts.join(", "))
}

pub fn projection_pushdown_sql(projection: Option<&bson::Document>) -> Option<String> {
    let proj = projection?;
    if proj.is_empty() {
        return None;
    }
    let mut include_fields: Vec<String> = Vec::new();
    let mut include_id = true;
    for (k, v) in proj.iter() {
        if k == "_id" {
            match v {
                bson::Bson::Int32(n) if *n == 0 => include_id = false,
                bson::Bson::Boolean(b) if !*b => include_id = false,
                _ => {}
            }
            continue;
        }
        let on = match v {
            bson::Bson::Int32(n) => *n != 0,
            bson::Bson::Boolean(b) => *b,
            _ => false,
        };
        if on {
            if k.contains('.') {
                return None;
            }
            include_fields.push(k.clone());
        } else {
            return None;
        }
    }
    if include_fields.is_empty() && include_id {
        return None;
    }
    let mut elems: Vec<String> = Vec::new();
    if include_id {
        elems.push("'_id', doc->'_id'".to_string());
    }
    for f in include_fields {
        elems.push(format!(
            "'{}', doc->'{}'",
            escape_single(&f),
            escape_single(&f)
        ));
    }
    if elems.is_empty() {
        return None;
    }
    Some(format!("jsonb_build_object({})", elems.join(", ")))
}

pub fn json_value_from_bson(v: &bson::Bson) -> Option<serde_json::Value> {
    match v {
        bson::Bson::Null => Some(serde_json::Value::Null),
        bson::Bson::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        bson::Bson::Int32(n) => Some(serde_json::Value::Number((*n).into())),
        bson::Bson::Int64(n) => Some(serde_json::Value::Number((*n).into())),
        bson::Bson::Double(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number),
        bson::Bson::String(s) => Some(serde_json::Value::String(s.clone())),
        _ => None,
    }
}

pub fn jsonpath_path(key: &str) -> String {
    let mut out = String::from("$");
    for seg in key.split('.') {
        let esc = seg.replace('"', "\\\"");
        out.push_str(".\"");
        out.push_str(&esc);
        out.push('"');
    }
    out
}

pub fn json_literal_from_bson(v: &bson::Bson) -> Option<String> {
    match v {
        bson::Bson::Null => Some("null".to_string()),
        bson::Bson::Boolean(b) => Some(if *b { "true".into() } else { "false".into() }),
        bson::Bson::Int32(n) => Some(n.to_string()),
        bson::Bson::Int64(n) => Some(n.to_string()),
        bson::Bson::Double(n) => Some(n.to_string()),
        bson::Bson::String(s) => Some(format!("{}", serde_json::to_string(s).ok()?)),
        _ => None,
    }
}

pub fn build_elem_match_pred(_path: &str, em: &bson::Document) -> Option<String> {
    if em.iter().all(|(k, _)| k.starts_with('$')) {
        let mut clauses: Vec<String> = Vec::new();
        for (op, val) in em.iter() {
            let (sql_op, lit) = match op.as_str() {
                "$gt" => (">", json_literal_from_bson(val)),
                "$gte" => (">=", json_literal_from_bson(val)),
                "$lt" => ("<", json_literal_from_bson(val)),
                "$lte" => ("<=", json_literal_from_bson(val)),
                "$eq" => ("==", json_literal_from_bson(val)),
                _ => ("", None),
            };
            if sql_op.is_empty() || lit.is_none() {
                continue;
            }
            clauses.push(format!("@ {} {}", sql_op, lit.unwrap()));
        }
        if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(" && "))
        }
    } else {
        let mut clauses: Vec<String> = Vec::new();
        for (k, v) in em.iter() {
            let attr = format!("@.\"{}\"", k.replace('"', "\\\""));
            match v {
                bson::Bson::Document(d) => {
                    for (op, val) in d.iter() {
                        let (sql_op, lit) = match op.as_str() {
                            "$gt" => (">", json_literal_from_bson(val)),
                            "$gte" => (">=", json_literal_from_bson(val)),
                            "$lt" => ("<", json_literal_from_bson(val)),
                            "$lte" => ("<=", json_literal_from_bson(val)),
                            "$eq" => ("==", json_literal_from_bson(val)),
                            _ => ("", None),
                        };
                        if sql_op.is_empty() || lit.is_none() {
                            continue;
                        }
                        clauses.push(format!("{} {} {}", attr, sql_op, lit.unwrap()));
                    }
                }
                _ => {
                    if let Some(lit) = json_literal_from_bson(v) {
                        clauses.push(format!("{} == {}", attr, lit));
                    }
                }
            }
        }
        if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(" && "))
        }
    }
}

pub fn escape_single(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "''")
}
