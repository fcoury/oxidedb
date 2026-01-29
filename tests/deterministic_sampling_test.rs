// Unit tests for deterministic sampling

use oxidedb::config::should_sample_deterministically;

#[test]
fn deterministic_sampling_consistency() {
    // Same inputs should always produce the same result
    let request_id = 12345i32;
    let db = "testdb";
    let sample_rate = 0.5;

    let result1 = should_sample_deterministically(request_id, db, sample_rate);
    let result2 = should_sample_deterministically(request_id, db, sample_rate);
    let result3 = should_sample_deterministically(request_id, db, sample_rate);

    assert_eq!(result1, result2);
    assert_eq!(result2, result3);
}

#[test]
fn deterministic_sampling_different_inputs() {
    // Different request IDs should produce different results
    let db = "testdb";
    let sample_rate = 0.5;

    let result1 = should_sample_deterministically(1, db, sample_rate);
    let result2 = should_sample_deterministically(2, db, sample_rate);
    let result3 = should_sample_deterministically(3, db, sample_rate);

    // Not all should be the same (statistically unlikely with 50% rate)
    let any_different = result1 != result2 || result2 != result3 || result1 != result3;
    assert!(
        any_different,
        "Different request IDs should produce different sampling results"
    );
}

#[test]
fn deterministic_sampling_different_dbs() {
    // Different databases should produce different results
    let request_id = 12345i32;
    let sample_rate = 0.5;

    let result1 = should_sample_deterministically(request_id, "db1", sample_rate);
    let result2 = should_sample_deterministically(request_id, "db2", sample_rate);
    let result3 = should_sample_deterministically(request_id, "db3", sample_rate);

    // Not all should be the same
    let any_different = result1 != result2 || result2 != result3 || result1 != result3;
    assert!(
        any_different,
        "Different databases should produce different sampling results"
    );
}

#[test]
fn deterministic_sampling_rate_0() {
    // 0% sample rate should never sample
    let request_id = 12345i32;
    let db = "testdb";
    let sample_rate = 0.0;

    for i in 0..100 {
        let result = should_sample_deterministically(request_id + i, db, sample_rate);
        assert!(!result, "0% sample rate should never sample");
    }
}

#[test]
fn deterministic_sampling_rate_1() {
    // 100% sample rate should always sample
    let request_id = 12345i32;
    let db = "testdb";
    let sample_rate = 1.0;

    for i in 0..100 {
        let result = should_sample_deterministically(request_id + i, db, sample_rate);
        assert!(result, "100% sample rate should always sample");
    }
}

#[test]
fn deterministic_sampling_rate_50_distribution() {
    // 50% sample rate should sample roughly half the time
    let db = "testdb";
    let sample_rate = 0.5;
    let total = 1000;

    let mut sampled = 0;
    for i in 0..total {
        if should_sample_deterministically(i, db, sample_rate) {
            sampled += 1;
        }
    }

    // Should be roughly 50% (within 10% tolerance)
    let rate = sampled as f64 / total as f64;
    assert!(
        rate > 0.4 && rate < 0.6,
        "50% sample rate should produce ~50% samples, got {}%",
        rate * 100.0
    );
}
