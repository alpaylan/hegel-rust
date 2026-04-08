//! Native in-process engine scaffold (no protocol).
//!
//! This module is a shell for replacing the current protocol-based runner with a pure-Rust
//! implementation. All functions here are intentionally `todo!()` and meant to be implemented
//! incrementally behind parity tests.
//!
//! ## Why this exists
//! The current architecture depends on `hegel-core` over the Hegel protocol. The goal here is to
//! move generation, shrinking, replay, and persistence fully into Rust.
//!
//! ## External references
//! - Hegel homepage: <https://hegel.dev>
//! - hegel-rust docs: <https://docs.rs/hegeltest>
//! - hegel-core repo: <https://github.com/hegeldev/hegel-core>
//! - Hypothesis repo (algorithm inspiration): <https://github.com/HypothesisWorks/hypothesis>
//!
//! ## Local references (read these first)
//! - Current run loop: `src/runner.rs`
//! - Current test-case command surface: `src/test_case.rs`
//! - Current stateful pool behavior: `src/stateful.rs`
//! - Quality suites:
//!   - `tests/test_shrink_quality/*`
//!   - `tests/test_find_quality/*`
//!   - `tests/conformance/*`
//!
//! ## Migration TODOs
//! - [ ] Implement `run_test()` orchestration with deterministic seeding and replay queue.
//! - [ ] Implement `generate()` for all schemas currently consumed by generators.
//! - [ ] Implement span tracking (`start_span` / `stop_span`) and discard behavior.
//! - [ ] Implement collection sizing (`new_collection` / `collection_more` / `collection_reject`).
//! - [ ] Implement stateful pools (`new_pool` / `pool_add` / `pool_generate`).
//! - [ ] Implement shrinking and minimal replay.
//! - [ ] Implement health checks and suppression behavior.
//! - [ ] Implement flaky detection semantics.
//! - [ ] Implement example database load/store keyed by test.
//! - [ ] Wire into `runner.rs` behind a feature flag, then switch default.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use ciborium::Value;
#[cfg(feature = "native-engine")]
use std::sync::{Mutex, OnceLock};

#[cfg(feature = "native-engine")]
static GLOBAL_ENGINE_STATE: OnceLock<Mutex<EngineState>> = OnceLock::new();

#[cfg(feature = "native-engine")]
pub(crate) fn with_global_engine<R>(f: impl FnOnce(&mut EngineState) -> R) -> R {
    let mtx = GLOBAL_ENGINE_STATE.get_or_init(|| Mutex::new(EngineState::default()));
    let mut guard = mtx.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut guard)
}

/// Engine-level run settings (decoupled from `runner::Settings` during migration).
#[derive(Debug, Clone)]
pub struct EngineSettings {
    pub test_cases: u64,
    pub seed: Option<u64>,
    pub derandomize: bool,
    pub database: DatabaseMode,
    pub suppress_health_check: Vec<HealthCheckName>,
}

/// Database behavior for a test run.
#[derive(Debug, Clone)]
pub enum DatabaseMode {
    Unset,
    Disabled,
    Path(String),
}

/// Stable test metadata needed for deterministic behavior and persistence.
#[derive(Debug, Clone)]
pub struct TestMetadata {
    pub function: String,
    pub module_path: String,
    pub file: String,
    pub begin_line: u32,
    pub database_key: Option<String>,
}

/// Health checks mirrored from current behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthCheckName {
    FilterTooMuch,
    TooSlow,
    TestCasesTooLarge,
    LargeInitialTestCase,
}

/// Current lifecycle state for a single test case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CaseId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionId(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PoolId(pub i128);

/// Outcome classification for a test case.
#[derive(Debug, Clone)]
pub enum CaseStatus {
    Valid,
    Invalid,
    Interesting {
        panic_message: String,
        origin: String,
    },
}

/// Summary returned when a run completes.
#[derive(Debug, Clone)]
pub struct RunSummary {
    pub passed: bool,
    pub interesting_test_cases: usize,
    pub health_check_failure: Option<String>,
    pub flaky: Option<String>,
    pub error: Option<String>,
}

/// Per-run mutable state for the in-process engine.
#[derive(Debug, Default)]
pub struct EngineState {
    pub active_run: Option<RunState>,
    pub collections: HashMap<CollectionId, CollectionState>,
    pub pools: HashMap<PoolId, PoolState>,
}

/// Run-wide state.
#[derive(Debug)]
pub struct RunState {
    pub settings: EngineSettings,
    pub metadata: TestMetadata,
    pub started_at: Instant,
    pub next_case_id: u64,
    pub completed: Vec<CompletedCase>,
    pub replay_queue: Vec<CaseId>,
    pub db_examples_loaded: usize,
}

/// Persisted and/or shrinkable case result.
#[derive(Debug, Clone)]
pub struct CompletedCase {
    pub case_id: CaseId,
    pub status: CaseStatus,
    pub duration: Duration,
    pub draws: Vec<DrawRecord>,
}

/// Captured draw for shrinking/replay diagnostics.
#[derive(Debug, Clone)]
pub struct DrawRecord {
    pub display_name: String,
    pub raw_value: Value,
    pub schema: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct CollectionState {
    pub min_size: usize,
    pub max_size: Option<usize>,
    pub accepted: usize,
    pub attempted: usize,
    pub finished: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PoolState {
    pub next_variable_id: i128,
    pub live_variable_ids: Vec<i128>,
}

/// Run request entrypoint for `runner.rs`.
///
/// TODO:
/// - initialize deterministic RNG from `settings` + `metadata`
/// - load examples from DB for this `database_key`
/// - create and return a fresh `RunState` in `engine.active_run`
pub fn run_test(engine: &mut EngineState, settings: EngineSettings, metadata: TestMetadata) {
    assert!(
        engine.active_run.is_none(),
        "native engine already has an active run"
    );
    engine.collections.clear();
    engine.pools.clear();
    engine.active_run = Some(RunState {
        settings,
        metadata,
        started_at: Instant::now(),
        next_case_id: 1,
        completed: Vec::new(),
        replay_queue: Vec::new(),
        db_examples_loaded: 0,
    });
}

/// Allocate the next case to execute.
///
/// TODO:
/// - schedule replay cases before fresh random cases
/// - enforce `test_cases` budget and stop conditions
pub fn next_case(engine: &mut EngineState) -> Option<CaseId> {
    let run = engine
        .active_run
        .as_mut()
        .expect("next_case called without an active run");

    if !run.replay_queue.is_empty() {
        return Some(run.replay_queue.remove(0));
    }

    if run.next_case_id > run.settings.test_cases {
        return None;
    }

    let case_id = CaseId(run.next_case_id);
    run.next_case_id += 1;
    Some(case_id)
}

/// Record a case result. Equivalent to protocol `mark_complete`.
///
/// TODO:
/// - update health-check counters
/// - enqueue interesting cases for shrinking/replay
/// - store invalid/valid stats
pub fn mark_complete(
    engine: &mut EngineState,
    case_id: CaseId,
    status: CaseStatus,
    duration: Duration,
    draws: Vec<DrawRecord>,
) {
    let run = engine
        .active_run
        .as_mut()
        .expect("mark_complete called without an active run");

    run.completed.push(CompletedCase {
        case_id,
        status,
        duration,
        draws,
    });
}

/// Finalize run and compute externally visible summary.
///
/// TODO:
/// - execute shrinking pipeline for interesting cases
/// - replay shrunk cases to confirm non-flaky status
/// - persist selected examples to DB
/// - produce `RunSummary` matching existing behavior
pub fn finish_run(engine: &mut EngineState) -> RunSummary {
    let run = engine
        .active_run
        .take()
        .expect("finish_run called without an active run");
    engine.collections.clear();
    engine.pools.clear();

    let interesting_test_cases = run
        .completed
        .iter()
        .filter(|c| matches!(c.status, CaseStatus::Interesting { .. }))
        .count();

    // TODO: evaluate health checks once counters are wired.
    let health_check_failure = None;
    // TODO: run replay-based flaky detection once replay/shrinking is wired.
    let flaky = None;
    // TODO: surface structured engine errors.
    let error = None;

    let passed = interesting_test_cases == 0
        && health_check_failure.is_none()
        && flaky.is_none()
        && error.is_none();

    RunSummary {
        passed,
        interesting_test_cases,
        health_check_failure,
        flaky,
        error,
    }
}

/// Generate a value from a schema for a case. Equivalent to protocol `generate`.
///
/// TODO:
/// - support every schema shape emitted by generators in `src/generators/*`
/// - keep generation decisions in a shrinkable buffer, not one-off RNG calls
/// - produce deterministic output under replay
pub fn generate(engine: &mut EngineState, case_id: CaseId, schema: &Value) -> Value {
    let _ = (engine, case_id, schema);
    todo!("generate")
}

/// Start a shrink span. Equivalent to protocol `start_span`.
///
/// TODO:
/// - push span frame with label and start offset in decision buffer
pub fn start_span(engine: &mut EngineState, case_id: CaseId, label: u64) -> SpanId {
    let _ = (engine, case_id, label);
    todo!("start_span")
}

/// Stop a shrink span. Equivalent to protocol `stop_span`.
///
/// TODO:
/// - close span frame and mark `discard` for shrink guidance
pub fn stop_span(engine: &mut EngineState, case_id: CaseId, span_id: SpanId, discard: bool) {
    let _ = (engine, case_id, span_id, discard);
    todo!("stop_span")
}

/// Create collection sizing state. Equivalent to protocol `new_collection`.
pub fn new_collection(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: Option<usize>,
) -> CollectionId {
    let _ = (engine, case_id, min_size, max_size);
    todo!("new_collection")
}

/// Decide whether collection should generate one more element.
/// Equivalent to protocol `collection_more`.
pub fn collection_more(
    engine: &mut EngineState,
    case_id: CaseId,
    collection_id: CollectionId,
) -> bool {
    let _ = (engine, case_id, collection_id);
    todo!("collection_more")
}

/// Reject last attempted collection element.
/// Equivalent to protocol `collection_reject`.
pub fn collection_reject(
    engine: &mut EngineState,
    case_id: CaseId,
    collection_id: CollectionId,
    why: Option<&str>,
) {
    let _ = (engine, case_id, collection_id, why);
    todo!("collection_reject")
}

/// Create a stateful variable pool. Equivalent to protocol `new_pool`.
pub fn new_pool(engine: &mut EngineState, case_id: CaseId) -> PoolId {
    let _ = (engine, case_id);
    todo!("new_pool")
}

/// Add a value slot to pool and return variable id.
/// Equivalent to protocol `pool_add`.
pub fn pool_add(engine: &mut EngineState, case_id: CaseId, pool_id: PoolId) -> i128 {
    let _ = (engine, case_id, pool_id);
    todo!("pool_add")
}

/// Draw or consume a variable id from pool.
/// Equivalent to protocol `pool_generate`.
pub fn pool_generate(
    engine: &mut EngineState,
    case_id: CaseId,
    pool_id: PoolId,
    consume: bool,
) -> i128 {
    let _ = (engine, case_id, pool_id, consume);
    todo!("pool_generate")
}

/// Load stored examples for a test key.
///
/// TODO:
/// - design Rust-native on-disk format (or preserve compatibility with existing DB format)
/// - bound file IO and handle corruption gracefully
pub fn database_load_examples(database_key: &str, database: &DatabaseMode) -> Vec<Vec<u8>> {
    let _ = (database_key, database);
    todo!("database_load_examples")
}

/// Store a new interesting example for future replay.
pub fn database_store_example(database_key: &str, database: &DatabaseMode, encoded_case: &[u8]) {
    let _ = (database_key, database, encoded_case);
    todo!("database_store_example")
}

/// Evaluate active health checks and return first failure, if any.
pub fn evaluate_health_checks(
    settings: &EngineSettings,
    counters: &BTreeMap<&'static str, u64>,
) -> Option<String> {
    let _ = (settings, counters);
    todo!("evaluate_health_checks")
}

/// Determine whether replay indicates flaky behavior.
pub fn detect_flaky(original: &CompletedCase, replayed: &CompletedCase) -> Option<String> {
    let _ = (original, replayed);
    todo!("detect_flaky")
}
