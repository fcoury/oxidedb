use bson::Bson;
use std::cmp::Ordering;

/// Compare two BSON values according to MongoDB ordering rules
pub fn bson_cmp(a: &Bson, b: &Bson) -> Ordering {
    // MongoDB ordering: Null < Numbers < Strings < Documents < Arrays < Binary < ObjectId < Boolean < Date < Timestamp < Regex < DBPointer < JavaScript < Symbol < JavaScriptWithScope < Integer (deprecated) < Decimal128 < MinKey < MaxKey
    let type_order = |v: &Bson| match v {
        Bson::Null => 0,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::ObjectId(_) => 6,
        Bson::Boolean(_) => 7,
        Bson::DateTime(_) => 8,
        Bson::Timestamp(_) => 9,
        Bson::RegularExpression(_) => 10,
        Bson::DbPointer(_) => 11,
        Bson::JavaScriptCode(_) => 12,
        Bson::Symbol(_) => 13,
        Bson::JavaScriptCodeWithScope(_) => 14,
        Bson::Undefined => 15,
        _ => 16,
    };

    let ord_a = type_order(a);
    let ord_b = type_order(b);

    if ord_a != ord_b {
        return ord_a.cmp(&ord_b);
    }

    // Same type, compare values
    match (a, b) {
        (Bson::Null, Bson::Null) => Ordering::Equal,
        (Bson::Int32(a), Bson::Int32(b)) => a.cmp(b),
        (Bson::Int64(a), Bson::Int64(b)) => a.cmp(b),
        (Bson::Double(a), Bson::Double(b)) => {
            if a.is_nan() && b.is_nan() {
                Ordering::Equal
            } else if a.is_nan() {
                Ordering::Less
            } else if b.is_nan() {
                Ordering::Greater
            } else {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
        }
        (Bson::String(a), Bson::String(b)) => a.cmp(b),
        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),
        (Bson::DateTime(a), Bson::DateTime(b)) => a.timestamp_millis().cmp(&b.timestamp_millis()),
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a.to_hex().cmp(&b.to_hex()),
        _ => Ordering::Equal, // Fallback for complex types
    }
}

/// Numeric type for coercion
#[derive(Debug, Clone, Copy)]
pub enum Numeric {
    Int32(i32),
    Int64(i64),
    Double(f64),
}

impl Numeric {
    pub fn as_f64(&self) -> f64 {
        match self {
            Numeric::Int32(n) => *n as f64,
            Numeric::Int64(n) => *n as f64,
            Numeric::Double(n) => *n,
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Numeric::Int32(n) => *n as i64,
            Numeric::Int64(n) => *n,
            Numeric::Double(n) => *n as i64,
        }
    }
}

/// Coerce a BSON value to a numeric type
pub fn coerce_numeric(val: &Bson) -> Option<Numeric> {
    match val {
        Bson::Int32(n) => Some(Numeric::Int32(*n)),
        Bson::Int64(n) => Some(Numeric::Int64(*n)),
        Bson::Double(n) => Some(Numeric::Double(*n)),
        _ => None,
    }
}
