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

    // Same type order; numbers compare by value across Int32/Int64/Double
    if let (Some(na), Some(nb)) = (coerce_numeric(a), coerce_numeric(b)) {
        return cmp_numeric(na, nb);
    }

    // Same type, compare values
    match (a, b) {
        (Bson::Null, Bson::Null) => Ordering::Equal,
        (Bson::String(a), Bson::String(b)) => a.cmp(b),
        (Bson::Boolean(a), Bson::Boolean(b)) => a.cmp(b),
        (Bson::DateTime(a), Bson::DateTime(b)) => a.timestamp_millis().cmp(&b.timestamp_millis()),
        (Bson::ObjectId(a), Bson::ObjectId(b)) => a.to_hex().cmp(&b.to_hex()),
        _ => Ordering::Equal, // Fallback for complex types
    }
}

/// Compare two numeric values, preserving integer precision when possible.
///
/// NaN sorts below all other numbers and equals itself, matching MongoDB.
fn cmp_numeric(a: Numeric, b: Numeric) -> Ordering {
    match (a, b) {
        (Numeric::Int32(x), Numeric::Int32(y)) => x.cmp(&y),
        (Numeric::Int64(x), Numeric::Int64(y)) => x.cmp(&y),
        (Numeric::Int32(x), Numeric::Int64(y)) => (x as i64).cmp(&y),
        (Numeric::Int64(x), Numeric::Int32(y)) => x.cmp(&(y as i64)),
        _ => {
            let x = a.as_f64();
            let y = b.as_f64();
            if x.is_nan() && y.is_nan() {
                Ordering::Equal
            } else if x.is_nan() {
                Ordering::Less
            } else if y.is_nan() {
                Ordering::Greater
            } else {
                x.partial_cmp(&y).unwrap_or(Ordering::Equal)
            }
        }
    }
}

/// Equality comparison following MongoDB semantics: numbers compare across
/// Int32/Int64/Double by value, other types require the same variant.
pub fn bson_eq(a: &Bson, b: &Bson) -> bool {
    if let (Some(na), Some(nb)) = (coerce_numeric(a), coerce_numeric(b)) {
        return cmp_numeric(na, nb) == Ordering::Equal;
    }
    a == b
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_type_int32_vs_int64() {
        assert_eq!(bson_cmp(&Bson::Int32(5), &Bson::Int64(10)), Ordering::Less);
        assert_eq!(bson_cmp(&Bson::Int64(5), &Bson::Int32(5)), Ordering::Equal);
        assert_eq!(
            bson_cmp(&Bson::Int64(10), &Bson::Int32(-1)),
            Ordering::Greater
        );
    }

    #[test]
    fn cross_type_int_vs_double() {
        assert_eq!(
            bson_cmp(&Bson::Int32(1), &Bson::Double(1.5)),
            Ordering::Less
        );
        assert_eq!(
            bson_cmp(&Bson::Double(2.0), &Bson::Int64(2)),
            Ordering::Equal
        );
        assert_eq!(
            bson_cmp(&Bson::Int64(i64::MAX), &Bson::Double(0.0)),
            Ordering::Greater
        );
    }

    #[test]
    fn nan_sorts_below_numbers_and_equals_itself() {
        assert_eq!(
            bson_cmp(&Bson::Double(f64::NAN), &Bson::Double(f64::NAN)),
            Ordering::Equal
        );
        assert_eq!(
            bson_cmp(&Bson::Double(f64::NAN), &Bson::Int32(i32::MIN)),
            Ordering::Less
        );
        assert_eq!(
            bson_cmp(&Bson::Int32(i32::MIN), &Bson::Double(f64::NAN)),
            Ordering::Greater
        );
    }

    #[test]
    fn numbers_sort_below_strings() {
        assert_eq!(
            bson_cmp(&Bson::Int64(42), &Bson::String("a".into())),
            Ordering::Less
        );
        assert_eq!(bson_cmp(&Bson::Null, &Bson::Int32(0)), Ordering::Less);
    }

    #[test]
    fn same_type_comparisons_still_work() {
        assert_eq!(
            bson_cmp(&Bson::String("a".into()), &Bson::String("b".into())),
            Ordering::Less
        );
        assert_eq!(
            bson_cmp(&Bson::Boolean(false), &Bson::Boolean(true)),
            Ordering::Less
        );
    }
}
