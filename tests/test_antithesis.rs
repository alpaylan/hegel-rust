mod common;

use common::project::TempRustProject;
use tempfile::TempDir;

#[test]
fn test_antithesis_jsonl_written_when_env_set() {
    let output_dir = TempDir::new().expect("Failed to create temp dir");
    let output_path = output_dir.path().to_str().unwrap().to_string();

    let code = r#"
use hegel::generators;

#[hegel::test(test_cases = 1)]
fn my_test(tc: hegel::TestCase) {
    let _ = tc.draw(generators::booleans());
}
"#;

    let output = TempRustProject::new()
        .test_file(code)
        .env("ANTITHESIS_OUTPUT_DIR", &output_path)
        .run();

    assert!(
        output.status.success(),
        "Subprocess failed: {}",
        output.stderr
    );

    let jsonl_path = output_dir.path().join("sdk.jsonl");
    assert!(jsonl_path.exists(), "sdk.jsonl was not created");

    let contents = std::fs::read_to_string(&jsonl_path).expect("Failed to read sdk.jsonl");
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "Expected 2 lines (declaration + evaluation), got {}",
        lines.len()
    );

    let declaration: serde_json::Value =
        serde_json::from_str(lines[0]).expect("Failed to parse declaration line");
    let evaluation: serde_json::Value =
        serde_json::from_str(lines[1]).expect("Failed to parse evaluation line");

    let decl_assert = &declaration["antithesis_assert"];
    assert_eq!(decl_assert["hit"], false);
    assert_eq!(decl_assert["must_hit"], true);
    assert_eq!(decl_assert["assert_type"], "always");
    assert_eq!(decl_assert["condition"], false);
    assert!(
        decl_assert["id"].as_str().unwrap().contains("my_test"),
        "id should contain test function name"
    );

    let eval_assert = &evaluation["antithesis_assert"];
    assert_eq!(eval_assert["hit"], true);
    assert_eq!(
        eval_assert["condition"], true,
        "passing test should have condition=true"
    );

    let loc = &eval_assert["location"];
    assert_eq!(loc["function"], "my_test");
    assert_eq!(loc["file"], "tests/test.rs");
    assert_eq!(loc["class"], "test");
    assert_eq!(loc["begin_line"], 4);
    assert_eq!(loc["begin_column"], 0);
}

#[test]
fn test_antithesis_jsonl_not_written_when_env_unset() {
    let output_dir = TempDir::new().expect("Failed to create temp dir");

    let code = r#"
use hegel::generators;

#[hegel::test(test_cases = 1)]
fn my_test(tc: hegel::TestCase) {
    let _ = tc.draw(generators::booleans());
}
"#;

    let output = TempRustProject::new().test_file(code).run();

    assert!(
        output.status.success(),
        "Subprocess failed: {}",
        output.stderr
    );

    let jsonl_path = output_dir.path().join("sdk.jsonl");
    assert!(
        !jsonl_path.exists(),
        "sdk.jsonl should not be created without ANTITHESIS_OUTPUT_DIR"
    );
}
