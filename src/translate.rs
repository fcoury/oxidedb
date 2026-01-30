use serde_json::{Map, Value};

pub enum WhereSpec {
    Raw(String),
    Containment(serde_json::Value),
}

pub fn build_where_from_filter(filter: &bson::Document) -> String {
    build_where_from_filter_internal(filter, false)
}

pub fn build_where_from_filter_internal(filter: &bson::Document, is_nested: bool) -> String {
    let mut where_clauses: Vec<String> = Vec::new();

    // Handle logical operators at the top level first
    if let Some(or_val) = filter.get("$or") {
        if let bson::Bson::Array(arr) = or_val {
            let mut or_clauses: Vec<String> = Vec::new();
            for item in arr {
                if let bson::Bson::Document(d) = item {
                    let clause = build_where_from_filter_internal(d, true);
                    if clause != "TRUE" {
                        or_clauses.push(clause);
                    }
                }
            }
            if !or_clauses.is_empty() {
                let joined = or_clauses.join(" OR ");
                where_clauses.push(if or_clauses.len() > 1 {
                    format!("({})", joined)
                } else {
                    joined
                });
            }
        }
    }

    if let Some(and_val) = filter.get("$and") {
        if let bson::Bson::Array(arr) = and_val {
            let mut and_clauses: Vec<String> = Vec::new();
            for item in arr {
                if let bson::Bson::Document(d) = item {
                    let clause = build_where_from_filter_internal(d, true);
                    if clause != "TRUE" {
                        and_clauses.push(clause);
                    }
                }
            }
            if !and_clauses.is_empty() {
                let joined = and_clauses.join(" AND ");
                where_clauses.push(if and_clauses.len() > 1 {
                    format!("({})", joined)
                } else {
                    joined
                });
            }
        }
    }

    if let Some(not_val) = filter.get("$not") {
        if let bson::Bson::Document(d) = not_val {
            let clause = build_where_from_filter_internal(d, true);
            if clause != "TRUE" {
                where_clauses.push(format!("NOT ({})", clause));
            }
        }
    }

    if let Some(nor_val) = filter.get("$nor") {
        if let bson::Bson::Array(arr) = nor_val {
            let mut nor_clauses: Vec<String> = Vec::new();
            for item in arr {
                if let bson::Bson::Document(d) = item {
                    let clause = build_where_from_filter_internal(d, true);
                    if clause != "TRUE" {
                        nor_clauses.push(clause);
                    }
                }
            }
            if !nor_clauses.is_empty() {
                let joined = nor_clauses.join(" OR ");
                where_clauses.push(format!(
                    "NOT ({})",
                    if nor_clauses.len() > 1 {
                        format!("({})", joined)
                    } else {
                        joined
                    }
                ));
            }
        }
    }

    // Note: $text operator is handled at the server layer (server.rs) to ensure
    // it uses the correct text index fields. Do not handle $text here.

    // Process field-level operators (skip keys starting with $)
    for (k, v) in filter.iter() {
        if k == "_id" || k.starts_with('$') {
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
                                        "jsonb_path_exists(doc, '{}') ? ({} ))",
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
                        "$ne" => {
                            if let Some(lit) = json_literal_from_bson(val) {
                                let p1 = format!(
                                    "jsonb_path_exists(doc, '{} ? (@ != {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                let p2 = format!(
                                    "jsonb_path_exists(doc, '{}[*] ? (@ != {} )')",
                                    escape_single(&path),
                                    lit
                                );
                                where_clauses.push(format!("({} OR {})", p1, p2));
                            }
                        }
                        "$nin" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut preds: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
                                        preds.push(format!("@ != {}", lit));
                                    }
                                }
                                if preds.is_empty() {
                                    where_clauses.push("TRUE".to_string());
                                } else {
                                    let predicate = preds.join(" && ");
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
                                    where_clauses.push(format!("({} AND {})", p1, p2));
                                }
                            }
                        }
                        "$regex" => {
                            if let bson::Bson::String(pattern) = val {
                                let flags =
                                    d.get("$options").and_then(|o| o.as_str()).unwrap_or("");
                                let regex_clause = build_regex_clause(&path, pattern, flags);
                                where_clauses.push(regex_clause);
                            }
                        }
                        "$all" => {
                            if let bson::Bson::Array(arr) = val {
                                let mut all_clauses: Vec<String> = Vec::new();
                                for item in arr {
                                    if let Some(lit) = json_literal_from_bson(item) {
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
                                        all_clauses.push(format!("({} OR {})", p1, p2));
                                    }
                                }
                                if !all_clauses.is_empty() {
                                    where_clauses.push(format!("({})", all_clauses.join(" AND ")));
                                }
                            }
                        }
                        "$size" => {
                            let size_val = match val {
                                bson::Bson::Int32(n) => Some(*n as i64),
                                bson::Bson::Int64(n) => Some(*n),
                                _ => None,
                            };
                            if let Some(n) = size_val {
                                let size_clause = format!(
                                    "jsonb_array_length(doc->'{}') = {}",
                                    escape_single(k),
                                    n
                                );
                                where_clauses.push(size_clause);
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
                        "$geoWithin" => {
                            if let bson::Bson::Document(gw) = val {
                                if let Some(geom) = gw.get("$geometry") {
                                    // GeoJSON format
                                    if let bson::Bson::Document(geom_doc) = geom {
                                        if let Some(geom_type) = geom_doc.get_str("type").ok() {
                                            if let Ok(coords) = geom_doc.get_array("coordinates") {
                                                let clause = build_geo_within_clause(
                                                    &path, geom_type, coords,
                                                );
                                                where_clauses.push(clause);
                                            }
                                        }
                                    }
                                } else if let Some(bson::Bson::Document(box_doc)) = gw.get("$box") {
                                    // Legacy $box format
                                    if let Ok(box_coords) = box_doc.get_array("$box") {
                                        let clause = build_geo_box_clause(&path, box_coords);
                                        where_clauses.push(clause);
                                    }
                                } else if let Some(bson::Bson::Document(poly_doc)) =
                                    gw.get("$polygon")
                                {
                                    // Legacy $polygon format
                                    if let Ok(poly_coords) = poly_doc.get_array("$polygon") {
                                        let clause = build_geo_polygon_clause(&path, poly_coords);
                                        where_clauses.push(clause);
                                    }
                                }
                            }
                        }
                        "$near" | "$nearSphere" => {
                            if let bson::Bson::Document(near_doc) = val {
                                let spherical = op == "$nearSphere";

                                // Get the near point (can be GeoJSON or legacy [lon, lat])
                                let (near_lon, near_lat) = if let Some(bson::Bson::Document(geom)) =
                                    near_doc.get("$geometry")
                                {
                                    // GeoJSON format
                                    if let Ok(coords) = geom.get_array("coordinates") {
                                        if coords.len() >= 2 {
                                            let lon = coords[0]
                                                .as_f64()
                                                .or_else(|| coords[0].as_i64().map(|v| v as f64))
                                                .unwrap_or(0.0);
                                            let lat = coords[1]
                                                .as_f64()
                                                .or_else(|| coords[1].as_i64().map(|v| v as f64))
                                                .unwrap_or(0.0);
                                            (lon, lat)
                                        } else {
                                            (0.0, 0.0)
                                        }
                                    } else {
                                        (0.0, 0.0)
                                    }
                                } else if let Ok(coords) = near_doc.get_array("$near") {
                                    // Legacy format
                                    if coords.len() >= 2 {
                                        let lon = coords[0]
                                            .as_f64()
                                            .or_else(|| coords[0].as_i64().map(|v| v as f64))
                                            .unwrap_or(0.0);
                                        let lat = coords[1]
                                            .as_f64()
                                            .or_else(|| coords[1].as_i64().map(|v| v as f64))
                                            .unwrap_or(0.0);
                                        (lon, lat)
                                    } else {
                                        (0.0, 0.0)
                                    }
                                } else {
                                    (0.0, 0.0)
                                };

                                // Get maxDistance
                                let max_distance =
                                    near_doc.get_f64("$maxDistance").ok().or_else(|| {
                                        near_doc.get_i64("$maxDistance").ok().map(|v| v as f64)
                                    });

                                let clause = build_near_clause(
                                    &path,
                                    near_lon,
                                    near_lat,
                                    max_distance,
                                    spherical,
                                );
                                where_clauses.push(clause);
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
    } else if where_clauses.len() == 1 {
        where_clauses[0].clone()
    } else if is_nested {
        format!("({})", where_clauses.join(" AND "))
    } else {
        where_clauses.join(" AND ")
    }
}

fn build_regex_clause(path: &str, pattern: &str, flags: &str) -> String {
    // Convert MongoDB regex pattern to PostgreSQL regex
    // Escape single quotes in pattern
    let escaped_pattern = pattern.replace("'", "''");
    let escaped_path = escape_single(path.trim_start_matches("$"));

    // Build PostgreSQL regex flags
    let mut pg_flags = String::new();
    if flags.contains('i') {
        pg_flags.push('i');
    }
    if flags.contains('m') || flags.contains('s') {
        pg_flags.push('n');
    }

    let flag_prefix = if pg_flags.is_empty() {
        String::new()
    } else {
        format!("(?{})", pg_flags)
    };

    // Use ~ operator for regex matching
    format!(
        "(doc->>'{}') ~ '{}{}'",
        escaped_path, flag_prefix, escaped_pattern
    )
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
    build_order_by_with_collation(sort, None)
}

pub fn build_order_by_with_collation(
    sort: Option<&bson::Document>,
    collation: Option<&crate::store::Collation>,
) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut has_id = false;
    let collate_clause = collation.map(|c| c.to_collate_clause()).unwrap_or_default();

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
                // Add COLLATE clause for text sorting if collation is specified
                let text_val = if collate_clause.is_empty() {
                    format!("(doc->>'{}') {}", f, ord)
                } else {
                    format!("(doc->>'{}') {} {}", f, collate_clause, ord)
                };
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
    let mut include_fields: Vec<(String, String)> = Vec::new();
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
        match v {
            bson::Bson::Int32(n) => {
                if *n != 0 {
                    if k.contains('.') {
                        return None;
                    }
                    include_fields.push((k.clone(), format!("doc->'{}'", escape_single(k))));
                } else {
                    return None;
                }
            }
            bson::Bson::Boolean(b) => {
                if *b {
                    if k.contains('.') {
                        return None;
                    }
                    include_fields.push((k.clone(), format!("doc->'{}'", escape_single(k))));
                } else {
                    return None;
                }
            }
            bson::Bson::Document(_) => {
                if k.contains('.') {
                    return None;
                }
                if let Some(sql_expr) = translate_expression(v) {
                    include_fields.push((k.clone(), sql_expr));
                } else {
                    return None;
                }
            }
            _ => {
                return None;
            }
        }
    }
    if include_fields.is_empty() && include_id {
        return None;
    }
    let mut elems: Vec<String> = Vec::new();
    if include_id {
        elems.push("'_id', doc->'_id'".to_string());
    }
    for (field_name, sql_expr) in include_fields {
        elems.push(format!("'{}', {}", escape_single(&field_name), sql_expr));
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

pub fn translate_expression(expr: &bson::Bson) -> Option<String> {
    match expr {
        bson::Bson::Document(doc) => {
            if doc.is_empty() {
                return None;
            }
            let (op, val) = doc.iter().next()?;
            match op.as_str() {
                "$cond" => translate_cond(val),
                "$ifNull" => translate_if_null(val),
                "$toString" => translate_type_cast(val, "text"),
                "$toInt" => translate_type_cast(val, "integer"),
                "$toDouble" => translate_type_cast(val, "double precision"),
                "$toBool" => translate_type_cast(val, "boolean"),
                "$concat" => translate_concat(val),
                "$concatArrays" => translate_concat_arrays(val),
                "$substr" => translate_substr(val),
                "$substrCP" => translate_substr(val),
                _ => None,
            }
        }
        bson::Bson::String(s) if s.starts_with('$') => {
            let path = &s[1..];
            let segs: Vec<String> = path.split('.').map(escape_single).collect();
            if segs.len() == 1 {
                // Simple field access - use text extraction operator
                Some(format!("doc->>'{}'", segs[0]))
            } else {
                // Nested field access - use text extraction with path
                let path_str = segs.join("','");
                Some(format!("doc #>> '{{\"{}\"}}'", path_str))
            }
        }
        bson::Bson::String(s) => {
            // String literal - escape and quote for SQL
            Some(format!("'{}'", escape_single(s)))
        }
        bson::Bson::Int32(n) => Some(n.to_string()),
        bson::Bson::Int64(n) => Some(n.to_string()),
        bson::Bson::Double(f) => Some(f.to_string()),
        bson::Bson::Boolean(b) => Some(if *b { "true".into() } else { "false".into() }),
        bson::Bson::Null => Some("NULL".into()),
        _ => None,
    }
}

fn translate_cond(val: &bson::Bson) -> Option<String> {
    match val {
        bson::Bson::Array(arr) if arr.len() == 3 => {
            let if_expr = translate_expression(&arr[0])?;
            let then_expr = translate_expression(&arr[1])?;
            let else_expr = translate_expression(&arr[2])?;
            Some(format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                if_expr, then_expr, else_expr
            ))
        }
        bson::Bson::Document(doc) => {
            let if_expr = doc.get("if").and_then(translate_expression)?;
            let then_expr = doc.get("then").and_then(translate_expression)?;
            let else_expr = doc.get("else").and_then(translate_expression)?;
            Some(format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                if_expr, then_expr, else_expr
            ))
        }
        _ => None,
    }
}

fn translate_if_null(val: &bson::Bson) -> Option<String> {
    match val {
        bson::Bson::Array(arr) if arr.len() == 2 => {
            let expr = translate_expression(&arr[0])?;
            let replacement = translate_expression(&arr[1])?;
            Some(format!("COALESCE({}, {})", expr, replacement))
        }
        _ => None,
    }
}

fn translate_type_cast(val: &bson::Bson, target_type: &str) -> Option<String> {
    let expr = translate_expression(val)?;
    Some(format!("({})::{}", expr, target_type))
}

fn translate_concat(val: &bson::Bson) -> Option<String> {
    match val {
        bson::Bson::Array(arr) => {
            let exprs: Vec<String> = arr.iter().filter_map(translate_expression).collect();
            if exprs.is_empty() {
                None
            } else {
                Some(format!("CONCAT({})", exprs.join(", ")))
            }
        }
        _ => None,
    }
}

fn translate_concat_arrays(val: &bson::Bson) -> Option<String> {
    match val {
        bson::Bson::Array(arr) => {
            // Build a subquery that concatenates arrays using jsonb_agg and jsonb_array_elements
            let mut array_sources: Vec<String> = Vec::new();

            for item in arr {
                match item {
                    // Field reference like "$a" - extract as JSONB array
                    bson::Bson::String(s) if s.starts_with('$') => {
                        let path = &s[1..];
                        let segs: Vec<String> = path.split('.').map(escape_single).collect();
                        let jsonb_path = if segs.len() == 1 {
                            format!("doc->'{}'", segs[0])
                        } else {
                            let path_str = segs.join("','");
                            format!("doc #> '{{\"{}\"}}'", path_str)
                        };
                        array_sources.push(format!(
                            "SELECT jsonb_array_elements({}) AS elem",
                            jsonb_path
                        ));
                    }
                    // Literal array like [4] - build as JSONB array and unnest it
                    bson::Bson::Array(lit_arr) => {
                        let elems: Vec<String> = lit_arr
                            .iter()
                            .filter_map(|elem| translate_expression(elem))
                            .collect();
                        if !elems.is_empty() {
                            let array_expr = format!("jsonb_build_array({})", elems.join(", "));
                            array_sources.push(format!(
                                "SELECT jsonb_array_elements({}) AS elem",
                                array_expr
                            ));
                        }
                    }
                    // Other literal values - wrap in array and unnest
                    _ => {
                        if let Some(expr) = translate_expression(item) {
                            let array_expr = format!("jsonb_build_array({})", expr);
                            array_sources.push(format!(
                                "SELECT jsonb_array_elements({}) AS elem",
                                array_expr
                            ));
                        }
                    }
                }
            }

            if array_sources.is_empty() {
                Some("'[]'::jsonb".to_string())
            } else {
                // Build a subquery that unions all array elements and aggregates them
                let union_sql = array_sources.join(" UNION ALL ");
                Some(format!(
                    "(SELECT jsonb_agg(elem) FROM ({} UNION ALL SELECT NULL::jsonb WHERE FALSE) sub WHERE elem IS NOT NULL)",
                    union_sql
                ))
            }
        }
        _ => None,
    }
}

fn translate_substr(val: &bson::Bson) -> Option<String> {
    match val {
        bson::Bson::Array(arr) if arr.len() == 3 => {
            let string_expr = translate_expression(&arr[0])?;
            let start = translate_expression(&arr[1])?;
            let length = translate_expression(&arr[2])?;
            Some(format!(
                "SUBSTRING({} FROM ({}::integer + 1) FOR {}::integer)",
                string_expr, start, length
            ))
        }
        _ => None,
    }
}

// --- Geospatial query helper functions ---

/// Build SQL clause for $geoWithin with GeoJSON geometry
fn build_geo_within_clause(path: &str, geom_type: &str, coords: &bson::Array) -> String {
    let escaped_path = escape_single(path.trim_start_matches("$"));

    match geom_type {
        "Polygon" => {
            // Extract bounding box from polygon coordinates
            if let Some(outer_ring) = coords.first().and_then(|r| r.as_array()) {
                let (min_lon, max_lon, min_lat, max_lat) = extract_bounding_box(outer_ring);
                format!(
                    "jsonb_path_exists(doc, '$.\"{}\".coordinates ? (@[0] >= {} && @[0] <= {} && @[1] >= {} && @[1] <= {})')",
                    escaped_path, min_lon, max_lon, min_lat, max_lat
                )
            } else {
                "FALSE".to_string()
            }
        }
        "MultiPolygon" => {
            // For MultiPolygon, check within any of the polygons
            let mut clauses: Vec<String> = Vec::new();
            for poly in coords.iter().filter_map(|p| p.as_array()) {
                if let Some(outer_ring) = poly.first().and_then(|r| r.as_array()) {
                    let (min_lon, max_lon, min_lat, max_lat) = extract_bounding_box(outer_ring);
                    clauses.push(format!(
                        "jsonb_path_exists(doc, '$.\"{}\".coordinates ? (@[0] >= {} && @[0] <= {} && @[1] >= {} && @[1] <= {})')",
                        escaped_path, min_lon, max_lon, min_lat, max_lat
                    ));
                }
            }
            if clauses.is_empty() {
                "FALSE".to_string()
            } else {
                format!("({})", clauses.join(" OR "))
            }
        }
        _ => "FALSE".to_string(),
    }
}

/// Build SQL clause for legacy $box operator
fn build_geo_box_clause(path: &str, box_coords: &bson::Array) -> String {
    let escaped_path = escape_single(path.trim_start_matches("$"));

    if box_coords.len() >= 2 {
        let bottom_left = box_coords[0].as_array();
        let top_right = box_coords[1].as_array();

        if let (Some(bl), Some(tr)) = (bottom_left, top_right) {
            if bl.len() >= 2 && tr.len() >= 2 {
                let min_lon = bl[0]
                    .as_f64()
                    .or_else(|| bl[0].as_i64().map(|v| v as f64))
                    .unwrap_or(0.0);
                let min_lat = bl[1]
                    .as_f64()
                    .or_else(|| bl[1].as_i64().map(|v| v as f64))
                    .unwrap_or(0.0);
                let max_lon = tr[0]
                    .as_f64()
                    .or_else(|| tr[0].as_i64().map(|v| v as f64))
                    .unwrap_or(0.0);
                let max_lat = tr[1]
                    .as_f64()
                    .or_else(|| tr[1].as_i64().map(|v| v as f64))
                    .unwrap_or(0.0);

                return format!(
                    "jsonb_path_exists(doc, '$.\"{}\".coordinates ? (@[0] >= {} && @[0] <= {} && @[1] >= {} && @[1] <= {})')",
                    escaped_path, min_lon, max_lon, min_lat, max_lat
                );
            }
        }
    }
    "FALSE".to_string()
}

/// Build SQL clause for legacy $polygon operator
fn build_geo_polygon_clause(path: &str, poly_coords: &bson::Array) -> String {
    // Compute bounding box from all polygon vertices
    let escaped_path = escape_single(path.trim_start_matches("$"));
    let (min_lon, max_lon, min_lat, max_lat) = extract_bounding_box(poly_coords);

    // Check if we got valid bounds
    if min_lon <= max_lon && min_lat <= max_lat {
        return format!(
            "(doc->'{}'->'coordinates'->>0)::float >= {} AND (doc->'{}'->'coordinates'->>0)::float <= {} AND (doc->'{}'->'coordinates'->>1)::float >= {} AND (doc->'{}'->'coordinates'->>1)::float <= {}",
            escaped_path,
            min_lon,
            escaped_path,
            max_lon,
            escaped_path,
            min_lat,
            escaped_path,
            max_lat
        );
    }
    "FALSE".to_string()
}

/// Build SQL clause for $near/$nearSphere operators
fn build_near_clause(
    path: &str,
    near_lon: f64,
    near_lat: f64,
    max_distance: Option<f64>,
    spherical: bool,
) -> String {
    let escaped_path = escape_single(path.trim_start_matches("$"));

    // Build distance calculation expression
    let distance_expr = if spherical {
        // Haversine distance in meters
        format!(
            "6371000 * acos(
                cos(radians({})) * cos(radians((doc->'{}'->'coordinates'->>1)::double precision)) *
                cos(radians((doc->'{}'->'coordinates'->>0)::double precision) - radians({})) +
                sin(radians({})) * sin(radians((doc->'{}'->'coordinates'->>1)::double precision))
            )",
            near_lat, escaped_path, escaped_path, near_lon, near_lat, escaped_path
        )
    } else {
        // Simple Euclidean distance
        format!(
            "sqrt(
                power((doc->'{}'->'coordinates'->>0)::double precision - {}, 2) +
                power((doc->'{}'->'coordinates'->>1)::double precision - {}, 2)
            )",
            escaped_path, near_lon, escaped_path, near_lat
        )
    };

    // Build the complete clause
    let mut clauses = vec![
        format!(
            "jsonb_path_exists(doc, '$.\"{}\".type ? (@ == \"Point\")')",
            escaped_path
        ),
        format!(
            "jsonb_path_exists(doc, '$.\"{}\".coordinates')",
            escaped_path
        ),
    ];

    if let Some(max_dist) = max_distance {
        clauses.push(format!("{} <= {}", distance_expr, max_dist));
    }

    clauses.join(" AND ")
}

/// Extract bounding box from a ring of coordinates
fn extract_bounding_box(ring: &bson::Array) -> (f64, f64, f64, f64) {
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;

    for point in ring.iter().filter_map(|p| p.as_array()) {
        if point.len() >= 2 {
            let lon = point[0]
                .as_f64()
                .or_else(|| point[0].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);
            let lat = point[1]
                .as_f64()
                .or_else(|| point[1].as_i64().map(|v| v as f64))
                .unwrap_or(0.0);

            min_lon = min_lon.min(lon);
            max_lon = max_lon.max(lon);
            min_lat = min_lat.min(lat);
            max_lat = max_lat.max(lat);
        }
    }

    (min_lon, max_lon, min_lat, max_lat)
}
