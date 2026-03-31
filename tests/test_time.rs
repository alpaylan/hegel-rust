mod common;

use common::utils::assert_all_examples;
use hegel::generators;
use hegel::generators::{DefaultGenerator, Generator};
use std::time::Duration;

#[test]
fn test_durations_default() {
    assert_all_examples(generators::durations(), |d| *d >= Duration::ZERO);
}

#[test]
fn test_duration_default_generator() {
    assert_all_examples(Duration::default_generator(), |d| *d >= Duration::ZERO);
}

#[test]
fn test_durations_mapped() {
    assert_all_examples(generators::durations().map(|d| d.as_secs()), |_| true);
}

#[test]
fn test_durations_bounded() {
    let min = Duration::from_secs(5);
    let max = Duration::from_secs(60);
    assert_all_examples(
        generators::durations().min_value(min).max_value(max),
        move |d| *d >= min && *d <= max,
    );
}

#[test]
fn test_durations_in_vec() {
    let max = Duration::from_secs(60);
    assert_all_examples(
        generators::vecs(generators::durations().max_value(max)).max_size(5),
        move |v| v.iter().all(|d| *d <= max),
    );
}

#[test]
fn test_duration_default_generator() {
    assert_all_examples(generators::default::<Duration>(), |d| *d >= Duration::ZERO);
}
