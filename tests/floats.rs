mod common;

use common::utils::{assert_all_examples, find_any};
use hegel::gen;

#[test]
fn test_f32_finite() {
    // Disable nan/infinity to test finite f32 values
    assert_all_examples(
        gen::floats::<f32>().allow_nan(false).allow_infinity(false),
        |&n| n.is_finite(),
    );
}

#[test]
fn test_f64_finite() {
    // Disable nan/infinity to test finite f64 values
    assert_all_examples(
        gen::floats::<f64>().allow_nan(false).allow_infinity(false),
        |&n| n.is_finite(),
    );
}

#[test]
fn test_f64_with_min() {
    // Bounds require disabling nan/infinity
    assert_all_examples(
        gen::floats::<f64>()
            .with_min(0.0)
            .allow_nan(false)
            .allow_infinity(false),
        |&n| n >= 0.0,
    );
}

#[test]
fn test_f64_with_max() {
    assert_all_examples(
        gen::floats::<f64>()
            .with_max(100.0)
            .allow_nan(false)
            .allow_infinity(false),
        |&n| n <= 100.0,
    );
}

#[test]
fn test_f64_with_min_and_max() {
    assert_all_examples(
        gen::floats::<f64>()
            .with_min(10.0)
            .with_max(20.0)
            .allow_nan(false)
            .allow_infinity(false),
        |&n| (10.0..=20.0).contains(&n),
    );
}

#[test]
fn test_f64_exclude_min() {
    assert_all_examples(
        gen::floats::<f64>()
            .with_min(0.0)
            .exclude_min()
            .allow_nan(false)
            .allow_infinity(false),
        |&n| n > 0.0,
    );
}

#[test]
fn test_f64_exclude_max() {
    assert_all_examples(
        gen::floats::<f64>()
            .with_max(100.0)
            .exclude_max()
            .allow_nan(false)
            .allow_infinity(false),
        |&n| n < 100.0,
    );
}

#[test]
fn test_f64_nan_by_default() {
    // NaN is allowed by default
    find_any(gen::floats::<f64>(), |n| n.is_nan());
}

#[test]
fn test_f64_infinity_by_default() {
    // Infinity is allowed by default
    find_any(gen::floats::<f64>(), |n| n.is_infinite());
}

#[test]
fn test_f64_can_find_positive() {
    find_any(
        gen::floats::<f64>().allow_nan(false).allow_infinity(false),
        |&n| n > 0.0,
    );
}

#[test]
fn test_f64_can_find_negative() {
    find_any(
        gen::floats::<f64>().allow_nan(false).allow_infinity(false),
        |&n| n < 0.0,
    );
}

#[test]
fn test_f32_nan_by_default() {
    find_any(gen::floats::<f32>(), |n| n.is_nan());
}

#[test]
fn test_f32_infinity_by_default() {
    find_any(gen::floats::<f32>(), |n| n.is_infinite());
}
