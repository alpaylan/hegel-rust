use hegel::generators as gs;

#[test]
fn test_default_runs_100_test_cases() {
    let mut count = 0;

    hegel::hegel(|tc| {
        let _ = tc.draw(gs::integers::<i32>());
        count += 1;
    });

    assert_eq!(count, 100);
}
