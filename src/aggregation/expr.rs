use bson::{Bson, Document};
use std::collections::HashMap;

/// Expression AST node
#[derive(Debug, Clone)]
pub enum Expr {
    // Literals
    Literal(Bson),

    // Field references
    FieldRef(String), // $field

    // System variables
    Root,    // $$ROOT
    Current, // $$CURRENT
    Remove,  // $$REMOVE
    Now,     // $$NOW

    // Arithmetic
    Add(Vec<Expr>),
    Subtract(Box<Expr>, Box<Expr>),
    Multiply(Vec<Expr>),
    Divide(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),

    // Comparison
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Gte(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Lte(Box<Expr>, Box<Expr>),

    // Logical
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),

    // Conditional
    Cond {
        if_true: Box<Expr>,
        then: Box<Expr>,
        else_expr: Box<Expr>,
    },
    IfNull(Vec<Expr>),

    // Type conversion
    ToString(Box<Expr>),
    ToInt(Box<Expr>),
    ToDouble(Box<Expr>),
    ToBool(Box<Expr>),
    ToDate(Box<Expr>),
    ToObjectId(Box<Expr>),

    // Array
    Array(Vec<Expr>),
    ConcatArrays(Vec<Expr>),
    Size(Box<Expr>),
    Slice {
        array: Box<Expr>,
        start: Box<Expr>,
        end: Option<Box<Expr>>,
    },

    // String
    Concat(Vec<Expr>),
    Substr {
        string: Box<Expr>,
        start: Box<Expr>,
        length: Box<Expr>,
    },
    ToLower(Box<Expr>),
    ToUpper(Box<Expr>),

    // Date
    Year(Box<Expr>),
    Month(Box<Expr>),
    DayOfMonth(Box<Expr>),
    Hour(Box<Expr>),
    Minute(Box<Expr>),
    Second(Box<Expr>),

    // Object
    MergeObjects(Vec<Expr>),
    ObjectToArray(Box<Expr>),
    ArrayToObject(Box<Expr>),

    // Meta
    TextScore, // $meta: "textScore"
}

/// Context for expression evaluation
pub struct ExprEvalContext {
    pub vars: HashMap<String, Bson>,
    pub root: Document,
    pub current: Document,
}

impl ExprEvalContext {
    pub fn new(root: Document, current: Document) -> Self {
        Self {
            vars: HashMap::new(),
            root,
            current,
        }
    }

    pub fn with_vars(root: Document, current: Document, vars: HashMap<String, Bson>) -> Self {
        Self {
            vars,
            root,
            current,
        }
    }
}

/// Parse a BSON value into an expression
pub fn parse_expr(bson: &Bson) -> anyhow::Result<Expr> {
    match bson {
        Bson::Document(doc) => {
            // Check for operators
            #[allow(clippy::collapsible_if)]
            if let Some((op, val)) = doc.iter().next() {
                if op.starts_with('$') {
                    return parse_operator(op, val);
                }
            }

            // It's a literal document
            let mut fields = Vec::new();
            for (k, v) in doc.iter() {
                fields.push((k.clone(), parse_expr(v)?));
            }
            Ok(Expr::Literal(Bson::Document(doc.clone())))
        }
        Bson::String(s) if s.starts_with("$$") => {
            // System variable
            match s.as_str() {
                "$$ROOT" => Ok(Expr::Root),
                "$$CURRENT" => Ok(Expr::Current),
                "$$REMOVE" => Ok(Expr::Remove),
                "$$NOW" => Ok(Expr::Now),
                _ => {
                    // User-defined variable
                    Ok(Expr::Literal(Bson::String(s.clone())))
                }
            }
        }
        Bson::String(s) if s.starts_with('$') => {
            // Field reference
            Ok(Expr::FieldRef(s[1..].to_string()))
        }
        _ => Ok(Expr::Literal(bson.clone())),
    }
}

fn parse_operator(op: &str, val: &Bson) -> anyhow::Result<Expr> {
    match op {
        "$add" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$add requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Add(exprs))
        }
        "$subtract" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$subtract requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$subtract requires exactly 2 arguments"));
            }
            Ok(Expr::Subtract(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$multiply" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$multiply requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Multiply(exprs))
        }
        "$divide" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$divide requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$divide requires exactly 2 arguments"));
            }
            Ok(Expr::Divide(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$eq" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$eq requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$eq requires exactly 2 arguments"));
            }
            Ok(Expr::Eq(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$ne" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$ne requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$ne requires exactly 2 arguments"));
            }
            Ok(Expr::Ne(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$gt" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$gt requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$gt requires exactly 2 arguments"));
            }
            Ok(Expr::Gt(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$gte" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$gte requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$gte requires exactly 2 arguments"));
            }
            Ok(Expr::Gte(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$lt" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$lt requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$lt requires exactly 2 arguments"));
            }
            Ok(Expr::Lt(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$lte" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$lte requires array"))?;
            if arr.len() != 2 {
                return Err(anyhow::anyhow!("$lte requires exactly 2 arguments"));
            }
            Ok(Expr::Lte(
                Box::new(parse_expr(&arr[0])?),
                Box::new(parse_expr(&arr[1])?),
            ))
        }
        "$and" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$and requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::And(exprs))
        }
        "$or" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$or requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Or(exprs))
        }
        "$not" => Ok(Expr::Not(Box::new(parse_expr(val)?))),
        "$cond" => {
            if let Some(arr) = val.as_array() {
                if arr.len() != 3 {
                    return Err(anyhow::anyhow!("$cond array form requires 3 elements"));
                }
                Ok(Expr::Cond {
                    if_true: Box::new(parse_expr(&arr[0])?),
                    then: Box::new(parse_expr(&arr[1])?),
                    else_expr: Box::new(parse_expr(&arr[2])?),
                })
            } else if let Some(doc) = val.as_document() {
                let if_true = doc
                    .get("if")
                    .ok_or_else(|| anyhow::anyhow!("$cond missing if"))?;
                let then = doc
                    .get("then")
                    .ok_or_else(|| anyhow::anyhow!("$cond missing then"))?;
                let else_expr = doc
                    .get("else")
                    .ok_or_else(|| anyhow::anyhow!("$cond missing else"))?;
                Ok(Expr::Cond {
                    if_true: Box::new(parse_expr(if_true)?),
                    then: Box::new(parse_expr(then)?),
                    else_expr: Box::new(parse_expr(else_expr)?),
                })
            } else {
                Err(anyhow::anyhow!("$cond must be array or document"))
            }
        }
        "$ifNull" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$ifNull requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::IfNull(exprs))
        }
        "$toString" => Ok(Expr::ToString(Box::new(parse_expr(val)?))),
        "$toInt" => Ok(Expr::ToInt(Box::new(parse_expr(val)?))),
        "$toDouble" => Ok(Expr::ToDouble(Box::new(parse_expr(val)?))),
        "$toBool" => Ok(Expr::ToBool(Box::new(parse_expr(val)?))),
        "$toDate" => Ok(Expr::ToDate(Box::new(parse_expr(val)?))),
        "$toObjectId" => Ok(Expr::ToObjectId(Box::new(parse_expr(val)?))),
        "$concat" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$concat requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::Concat(exprs))
        }
        "$concatArrays" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$concatArrays requires array"))?;
            let exprs: Vec<Expr> = arr.iter().map(parse_expr).collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ConcatArrays(exprs))
        }
        "$size" => Ok(Expr::Size(Box::new(parse_expr(val)?))),
        "$substr" => {
            let arr = val
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("$substr requires array"))?;
            if arr.len() < 2 || arr.len() > 3 {
                return Err(anyhow::anyhow!("$substr requires 2 or 3 arguments"));
            }
            Ok(Expr::Substr {
                string: Box::new(parse_expr(&arr[0])?),
                start: Box::new(parse_expr(&arr[1])?),
                length: if arr.len() == 3 {
                    Box::new(parse_expr(&arr[2])?)
                } else {
                    Box::new(Expr::Literal(Bson::Int64(i64::MAX)))
                },
            })
        }
        "$toLower" => Ok(Expr::ToLower(Box::new(parse_expr(val)?))),
        "$toUpper" => Ok(Expr::ToUpper(Box::new(parse_expr(val)?))),
        "$meta" => {
            if let Bson::String(s) = val {
                if s == "textScore" {
                    Ok(Expr::TextScore)
                } else {
                    Err(anyhow::anyhow!("Unsupported $meta value: {}", s))
                }
            } else {
                Err(anyhow::anyhow!("$meta requires string"))
            }
        }
        _ => Err(anyhow::anyhow!("Unknown operator: {}", op)),
    }
}

/// Evaluate an expression in a context
pub fn eval_expr(expr: &Expr, ctx: &ExprEvalContext) -> anyhow::Result<Bson> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::FieldRef(path) => {
            // Navigate the path in current document
            let parts: Vec<&str> = path.split('.').collect();
            let mut current = Bson::Document(ctx.current.clone());
            for part in &parts {
                match current {
                    Bson::Document(doc) => {
                        current = doc.get(*part).cloned().unwrap_or(Bson::Null);
                    }
                    _ => return Ok(Bson::Null),
                }
            }
            Ok(current)
        }
        Expr::Root => Ok(Bson::Document(ctx.root.clone())),
        Expr::Current => Ok(Bson::Document(ctx.current.clone())),
        Expr::Remove => Ok(Bson::Null), // Special marker
        Expr::Now => {
            // Return current timestamp
            Ok(Bson::DateTime(bson::DateTime::now()))
        }
        Expr::Add(exprs) => {
            let mut sum_i128: i128 = 0;
            let mut has_double = false;
            let mut sum_double: f64 = 0.0;

            for e in exprs {
                let val = eval_expr(e, ctx)?;
                match val {
                    Bson::Int32(n) => sum_i128 += n as i128,
                    Bson::Int64(n) => sum_i128 += n as i128,
                    Bson::Double(n) => {
                        has_double = true;
                        sum_double += n;
                    }
                    _ => {}
                }
            }

            if has_double {
                Ok(Bson::Double(sum_double + sum_i128 as f64))
            } else if sum_i128 >= i32::MIN as i128 && sum_i128 <= i32::MAX as i128 {
                Ok(Bson::Int32(sum_i128 as i32))
            } else if sum_i128 >= i64::MIN as i128 && sum_i128 <= i64::MAX as i128 {
                Ok(Bson::Int64(sum_i128 as i64))
            } else {
                Ok(Bson::Double(sum_i128 as f64))
            }
        }
        Expr::Subtract(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            match (av, bv) {
                (Bson::Int32(a), Bson::Int32(b)) => Ok(Bson::Int32(a - b)),
                (Bson::Int64(a), Bson::Int64(b)) => Ok(Bson::Int64(a - b)),
                (Bson::Double(a), Bson::Double(b)) => Ok(Bson::Double(a - b)),
                (a, b) => {
                    let an = crate::aggregation::coerce_numeric(&a);
                    let bn = crate::aggregation::coerce_numeric(&b);
                    if let (Some(an), Some(bn)) = (an, bn) {
                        Ok(Bson::Double(an.as_f64() - bn.as_f64()))
                    } else {
                        Ok(Bson::Null)
                    }
                }
            }
        }
        Expr::Multiply(exprs) => {
            let mut prod: f64 = 1.0;
            let mut is_int = true;
            let mut prod_i128: i128 = 1;

            for e in exprs {
                let val = eval_expr(e, ctx)?;
                match val {
                    Bson::Int32(n) => prod_i128 *= n as i128,
                    Bson::Int64(n) => prod_i128 *= n as i128,
                    Bson::Double(n) => {
                        is_int = false;
                        prod *= n;
                    }
                    _ => {}
                }
            }

            if is_int {
                if prod_i128 >= i32::MIN as i128 && prod_i128 <= i32::MAX as i128 {
                    Ok(Bson::Int32(prod_i128 as i32))
                } else if prod_i128 >= i64::MIN as i128 && prod_i128 <= i64::MAX as i128 {
                    Ok(Bson::Int64(prod_i128 as i64))
                } else {
                    Ok(Bson::Double(prod_i128 as f64))
                }
            } else {
                Ok(Bson::Double(prod * prod_i128 as f64))
            }
        }
        Expr::Divide(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            let an = crate::aggregation::coerce_numeric(&av);
            let bn = crate::aggregation::coerce_numeric(&bv);
            if let (Some(an), Some(bn)) = (an, bn) {
                if bn.as_f64() == 0.0 {
                    Ok(Bson::Null)
                } else {
                    Ok(Bson::Double(an.as_f64() / bn.as_f64()))
                }
            } else {
                Ok(Bson::Null)
            }
        }
        Expr::Eq(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            Ok(Bson::Boolean(
                crate::aggregation::bson_cmp(&av, &bv) == std::cmp::Ordering::Equal,
            ))
        }
        Expr::Ne(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            Ok(Bson::Boolean(
                crate::aggregation::bson_cmp(&av, &bv) != std::cmp::Ordering::Equal,
            ))
        }
        Expr::Gt(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            Ok(Bson::Boolean(
                crate::aggregation::bson_cmp(&av, &bv) == std::cmp::Ordering::Greater,
            ))
        }
        Expr::Gte(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            let cmp = crate::aggregation::bson_cmp(&av, &bv);
            Ok(Bson::Boolean(
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal,
            ))
        }
        Expr::Lt(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            Ok(Bson::Boolean(
                crate::aggregation::bson_cmp(&av, &bv) == std::cmp::Ordering::Less,
            ))
        }
        Expr::Lte(a, b) => {
            let av = eval_expr(a, ctx)?;
            let bv = eval_expr(b, ctx)?;
            let cmp = crate::aggregation::bson_cmp(&av, &bv);
            Ok(Bson::Boolean(
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal,
            ))
        }
        Expr::And(exprs) => {
            for e in exprs {
                let val = eval_expr(e, ctx)?;
                if !is_truthy(&val) {
                    return Ok(Bson::Boolean(false));
                }
            }
            Ok(Bson::Boolean(true))
        }
        Expr::Or(exprs) => {
            for e in exprs {
                let val = eval_expr(e, ctx)?;
                if is_truthy(&val) {
                    return Ok(Bson::Boolean(true));
                }
            }
            Ok(Bson::Boolean(false))
        }
        Expr::Not(e) => {
            let val = eval_expr(e, ctx)?;
            Ok(Bson::Boolean(!is_truthy(&val)))
        }
        Expr::Cond {
            if_true,
            then,
            else_expr,
        } => {
            let cond_val = eval_expr(if_true, ctx)?;
            if is_truthy(&cond_val) {
                eval_expr(then, ctx)
            } else {
                eval_expr(else_expr, ctx)
            }
        }
        Expr::IfNull(exprs) => {
            for e in exprs {
                let val = eval_expr(e, ctx)?;
                if !matches!(val, Bson::Null) {
                    return Ok(val);
                }
            }
            Ok(Bson::Null)
        }
        Expr::ToString(e) => {
            let val = eval_expr(e, ctx)?;
            Ok(Bson::String(format!("{}", val)))
        }
        Expr::ToInt(e) => {
            let val = eval_expr(e, ctx)?;
            match val {
                Bson::Int32(n) => Ok(Bson::Int32(n)),
                Bson::Int64(n) => Ok(Bson::Int32(n as i32)),
                Bson::Double(n) => Ok(Bson::Int32(n as i32)),
                Bson::String(s) => s
                    .parse::<i32>()
                    .map(Bson::Int32)
                    .map_err(|e| anyhow::anyhow!("Cannot convert to int: {}", e)),
                _ => Ok(Bson::Null),
            }
        }
        Expr::ToDouble(e) => {
            let val = eval_expr(e, ctx)?;
            match val {
                Bson::Int32(n) => Ok(Bson::Double(n as f64)),
                Bson::Int64(n) => Ok(Bson::Double(n as f64)),
                Bson::Double(n) => Ok(Bson::Double(n)),
                Bson::String(s) => s
                    .parse::<f64>()
                    .map(Bson::Double)
                    .map_err(|e| anyhow::anyhow!("Cannot convert to double: {}", e)),
                _ => Ok(Bson::Null),
            }
        }
        Expr::ToBool(e) => {
            let val = eval_expr(e, ctx)?;
            Ok(Bson::Boolean(is_truthy(&val)))
        }
        Expr::ToDate(e) => {
            let val = eval_expr(e, ctx)?;
            match val {
                Bson::DateTime(d) => Ok(Bson::DateTime(d)),
                Bson::Int64(n) => Ok(Bson::DateTime(bson::DateTime::from_millis(n))),
                Bson::String(_s) => {
                    // Try to parse ISO date using bson's built-in parser
                    // bson::DateTime::parse_rfc3339_str is not available, so we use a simple approach
                    // For now, return null for string dates (would need chrono for full support)
                    Ok(Bson::Null)
                }
                _ => Ok(Bson::Null),
            }
        }
        Expr::ToObjectId(e) => {
            let val = eval_expr(e, ctx)?;
            match val {
                Bson::ObjectId(oid) => Ok(Bson::ObjectId(oid)),
                Bson::String(s) => {
                    if let Ok(oid) = bson::oid::ObjectId::parse_str(&s) {
                        Ok(Bson::ObjectId(oid))
                    } else {
                        Ok(Bson::Null)
                    }
                }
                _ => Ok(Bson::Null),
            }
        }
        Expr::Concat(exprs) => {
            let mut result = String::new();
            for e in exprs {
                let val = eval_expr(e, ctx)?;
                if let Bson::String(s) = val {
                    result.push_str(&s);
                } else {
                    result.push_str(&format!("{}", val));
                }
            }
            Ok(Bson::String(result))
        }
        Expr::ConcatArrays(exprs) => {
            let mut result: Vec<Bson> = Vec::new();
            for e in exprs {
                let val = eval_expr(e, ctx)?;
                if let Bson::Array(arr) = val {
                    result.extend(arr);
                }
            }
            Ok(Bson::Array(result))
        }
        Expr::Size(e) => {
            let val = eval_expr(e, ctx)?;
            if let Bson::Array(arr) = val {
                Ok(Bson::Int32(arr.len() as i32))
            } else {
                Ok(Bson::Null)
            }
        }
        Expr::Substr {
            string,
            start,
            length,
        } => {
            let s_val = eval_expr(string, ctx)?;
            let start_val = eval_expr(start, ctx)?;
            let len_val = eval_expr(length, ctx)?;

            if let (Bson::String(s), Bson::Int32(st), Bson::Int32(len)) =
                (s_val, start_val, len_val)
            {
                let start_idx = st.max(0) as usize;
                let len_idx = len.max(0) as usize;
                let result: String = s.chars().skip(start_idx).take(len_idx).collect();
                Ok(Bson::String(result))
            } else {
                Ok(Bson::String(String::new()))
            }
        }
        Expr::ToLower(e) => {
            let val = eval_expr(e, ctx)?;
            if let Bson::String(s) = val {
                Ok(Bson::String(s.to_lowercase()))
            } else {
                Ok(Bson::String(String::new()))
            }
        }
        Expr::ToUpper(e) => {
            let val = eval_expr(e, ctx)?;
            if let Bson::String(s) = val {
                Ok(Bson::String(s.to_uppercase()))
            } else {
                Ok(Bson::String(String::new()))
            }
        }
        Expr::TextScore => {
            // Return 1.0 as default text score (actual score would come from text search)
            Ok(Bson::Double(1.0))
        }
        _ => Err(anyhow::anyhow!(
            "Expression evaluation not implemented for: {:?}",
            expr
        )),
    }
}

fn is_truthy(val: &Bson) -> bool {
    match val {
        Bson::Boolean(b) => *b,
        Bson::Int32(n) => *n != 0,
        Bson::Int64(n) => *n != 0,
        Bson::Double(n) => *n != 0.0 && !n.is_nan(),
        Bson::String(s) => !s.is_empty(),
        Bson::Array(arr) => !arr.is_empty(),
        Bson::Document(doc) => !doc.is_empty(),
        Bson::Null => false,
        Bson::Undefined => false,
        _ => true,
    }
}
