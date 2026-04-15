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
//! - [ ] Implement `run_test()` orchestration with byte-buffer based generation and replay queue.
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

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ciborium::Value;
use regex::Regex;
#[cfg(feature = "native-engine")]
use std::sync::{Mutex, OnceLock};

use crate::native_engine::choice::{ChoiceConstraints, ChoiceKind, ChoiceNode, ChoiceValue};

#[cfg(feature = "native-engine")]
static GLOBAL_ENGINE_STATE: OnceLock<Mutex<EngineState>> = OnceLock::new();

#[cfg(feature = "native-engine")]
pub mod choice;
#[cfg(feature = "native-engine")]
pub mod datatree;
#[cfg(feature = "native-engine")]
pub mod shrink;

#[cfg(feature = "native-engine")]
pub mod intervalset;

#[cfg(feature = "native-engine")]
pub mod utils;

#[cfg(feature = "native-engine")]
pub mod floats;

#[cfg(feature = "native-engine")]
pub mod provider;

#[cfg(feature = "native-engine")]
pub mod data;

#[cfg(feature = "native-engine")]
pub mod random;

#[cfg(feature = "native-engine")]
pub(crate) fn with_global_engine<R>(f: impl FnOnce(&mut EngineState) -> R) -> R {
    let mtx = GLOBAL_ENGINE_STATE.get_or_init(|| Mutex::new(EngineState::default()));
    let mut guard = mtx.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut guard)
}

/// The maximum number of times the shrinker will reduce the complexity of a failing
/// input before giving up. This avoids falling down a trap of exponential (or worse)
/// complexity, where the shrinker appears to be making progress but will take a
/// substantially long time to finish completely.
pub static MAX_SHRINKS: u64 = 500;

/// If the shrinking phase takes more than five minutes, abort it early and print
/// a warning.   Many CI systems will kill a build after around ten minutes with
/// no output, and appearing to hang isn't great for interactive use either -
/// showing partially-shrunk examples is better than quitting with no examples!
/// (but make it monkeypatchable, for the rare users who need to keep on shrinking)

/// The maximum total time in seconds that the shrinker will try to shrink a failure
/// for before giving up. This is across all shrinks for the same failure, so even
/// if the shrinker successfully reduces the complexity of a single failure several
/// times, it will stop when it hits |MAX_SHRINKING_SECONDS| of total time taken.
pub static MAX_SHRINKING_SECONDS: u64 = 300;

/// The maximum amount of entropy a single test case can use before giving up
/// while making random choices during input generation.
///
/// The "unit" of one |BUFFER_SIZE| does not have any defined semantics, and you
/// should not rely on it, except that a linear increase |BUFFER_SIZE| will linearly
/// increase the amount of entropy a test case can use during generation.
pub static BUFFER_SIZE: usize = 8 * 1024;
pub static CACHE_SIZE: usize = 10000;
pub static MIN_TEST_CALLS: usize = 10;

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
    Overrun,
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
    pub cases: HashMap<CaseId, CaseBufferState>,
    pub collections: HashMap<CollectionId, CollectionState>,
    pub pools: HashMap<PoolId, PoolState>,
}

/// Run-wide state.
#[derive(Debug)]
pub struct RunState {
    pub settings: EngineSettings,
    pub metadata: TestMetadata,
    pub started_at: Instant,
    pub run_seed: u64,
    pub py_random: PythonRandom,
    pub pending_simplest_case: bool,
    pub current_case_simplest_observed: bool,
    pub random_trace_enabled: bool,
    pub current_case_id: Option<CaseId>,
    pub random_trace_by_case: HashMap<CaseId, Vec<RandomTraceEvent>>,
    pub pending_scheduler_random_events: Vec<RandomTraceEvent>,
    pub last_scheduler_rng_index: Option<usize>,
    pub next_case_id: u64,
    pub completed: Vec<CompletedCase>,
    pub replay_queue: Vec<CaseId>,
    pub replay_buffers: Vec<Vec<u8>>,
    pub db_examples_loaded: usize,
    pub constant_pools: HypothesisConstantPools,
    pub radix_tree: datatree::DataTree,
    pub cached_simplest_probes: HashMap<String, CachedSimplestProbeResult>,
    pub pending_random_prefixes: VecDeque<PendingRandomPrefixCase>,
    pub consecutive_zero_extend_is_invalid: usize,
    pub mutation_data_cache: HashMap<String, MutationCandidateData>,
    pub pending_mutation_state: Option<PendingMutationState>,
    pub(crate) novel_prefix_children_cache: HashMap<String, datatree::ChildrenCacheValue>,
    pub last_generate_novel_prefix_trace: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct PendingRandomPrefixCase {
    pub prefix: Vec<ChoiceValue>,
    pub max_choices: usize,
}

#[derive(Debug, Clone)]
pub struct MutationCandidateData {
    pub choices: Vec<ChoiceValue>,
    pub constraints: Vec<ChoiceConstraints>,
    pub spans: Vec<SpanState>,
    pub status: CaseStatus,
    pub has_discard: bool,
}

#[derive(Debug, Clone)]
pub struct PendingMutationState {
    pub initial_calls: usize,
    pub failed_mutations: usize,
    pub current_data: MutationCandidateData,
    pub pending_attempt: Option<Vec<ChoiceValue>>,
    pub inflight_case_id: Option<CaseId>,
}

#[derive(Debug, Clone, Copy)]
pub struct CachedSimplestProbeResult {
    pub terminal: GenerationTerminal,
    pub node_count: usize,
}

const MAX_CHILDREN_EFFECTIVELY_INFINITE: u128 = 10_000_000;

#[derive(Debug, Clone, Default)]
pub struct HypothesisConstantPools {
    pub global_integer_constants: Vec<i128>,
    pub local_integer_constants: Vec<i128>,
    pub global_float_constants: Vec<f64>,
    pub local_float_constants: Vec<f64>,
    pub global_bytes_constants: Vec<Vec<u8>>,
    pub local_bytes_constants: Vec<Vec<u8>>,
    pub global_string_constants: Vec<String>,
    pub local_string_constants: Vec<String>,
    pub strict_local_integer_policy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationTerminal {
    Valid,
    Invalid,
    Interesting,
}

/// Persisted and/or shrinkable case result.
#[derive(Debug, Clone)]
pub struct CompletedCase {
    pub case_id: CaseId,
    pub status: CaseStatus,
    pub duration: Duration,
    /// Canonical byte buffer for replay/shrinking of this example.
    pub buffer: Vec<u8>,
    /// Recorded typed choices for sort-key comparisons while shrinking.
    pub typed_nodes: Vec<ChoiceNode>,
    /// Closed span metadata for span-guided shrinking passes.
    pub spans: Vec<SpanState>,
    pub random_events: Vec<RandomTraceEvent>,
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
pub enum RandomTraceEvent {
    Random { bits: u64 },
    GetRandBits { k: usize, value_hex: String },
}

/// Error returned when attempting to append an invalid typed choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordChoiceError {
    TypeMismatch {
        expected: ChoiceKind,
        got: ChoiceKind,
    },
    MaxLengthExceeded {
        current: usize,
        added: usize,
        max: usize,
    },
    MaxChoicesExceeded {
        current: usize,
        max: usize,
    },
}

/// Mutable typed-choice stream for one case.
#[derive(Debug, Clone)]
pub struct TypedChoiceState {
    pub nodes: Vec<ChoiceNode>,
    pub cursor: usize,
    pub observed_size: usize,
    pub max_size: usize,
    pub max_choices: Option<usize>,
}

const DEFAULT_TYPED_MAX_SIZE: usize = 8 * 1024;

impl Default for TypedChoiceState {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            cursor: 0,
            observed_size: 0,
            max_size: DEFAULT_TYPED_MAX_SIZE,
            max_choices: None,
        }
    }
}

impl TypedChoiceState {
    pub fn with_limits(max_size: usize, max_choices: Option<usize>) -> Self {
        Self {
            max_size,
            max_choices,
            ..Self::default()
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn record_choice(
        &mut self,
        value: ChoiceValue,
        constraints: ChoiceConstraints,
        was_forced: bool,
        encoded_size: usize,
    ) -> Result<&ChoiceNode, RecordChoiceError> {
        let expected = constraints.kind();
        let got = value.kind();
        if expected != got {
            return Err(RecordChoiceError::TypeMismatch { expected, got });
        }

        if let Some(max) = self.max_choices
            && self.nodes.len() >= max
        {
            return Err(RecordChoiceError::MaxChoicesExceeded {
                current: self.nodes.len(),
                max,
            });
        }

        if self.observed_size.saturating_add(encoded_size) > self.max_size {
            return Err(RecordChoiceError::MaxLengthExceeded {
                current: self.observed_size,
                added: encoded_size,
                max: self.max_size,
            });
        }

        let node = ChoiceNode {
            kind: expected,
            value,
            constraints,
            was_forced,
            index: self.nodes.len(),
            encoded_size,
        };
        self.observed_size += encoded_size;
        self.nodes.push(node);
        Ok(self
            .nodes
            .last()
            .expect("nodes is non-empty immediately after push"))
    }
}

fn derive_metadata_seed(metadata: &TestMetadata) -> u64 {
    let mut hasher = DefaultHasher::new();
    metadata.function.hash(&mut hasher);
    metadata.module_path.hash(&mut hasher);
    metadata.file.hash(&mut hasher);
    metadata.begin_line.hash(&mut hasher);
    metadata.database_key.hash(&mut hasher);
    hasher.finish()
}

fn make_case_buffer(run_seed: u64, case_id: CaseId, size: usize) -> Vec<u8> {
    let mut state = mix_seed(run_seed, case_id.0);
    let mut bytes = vec![0u8; size];
    for chunk in bytes.chunks_mut(8) {
        let block = splitmix64_next(&mut state).to_le_bytes();
        let n = chunk.len();
        chunk.copy_from_slice(&block[..n]);
    }
    bytes
}

pub(crate) fn compare_typed_nodes_shortlex(
    a: &[ChoiceNode],
    b: &[ChoiceNode],
) -> std::cmp::Ordering {
    // Mirrors Hypothesis shortlex ordering over typed choices:
    // hypothesis/internal/conjecture/shrinker.py::sort_key
    // hypothesis/internal/conjecture/choice.py::choice_to_index
    a.len().cmp(&b.len()).then_with(|| {
        for (left, right) in a.iter().zip(b.iter()) {
            let ord = ChoiceNode::choice_to_index(&left.value, &left.constraints).cmp(
                &ChoiceNode::choice_to_index(&right.value, &right.constraints),
            );
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    })
}

fn uleb128_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn signed_i128_payload_len(value: i128) -> usize {
    let bytes = value.to_be_bytes();
    let mut start = 0usize;
    while start < bytes.len() - 1 {
        let byte = bytes[start];
        let next = bytes[start + 1];
        let redundant_positive = byte == 0x00 && (next & 0x80) == 0;
        let redundant_negative = byte == 0xFF && (next & 0x80) == 0x80;
        if redundant_positive || redundant_negative {
            start += 1;
        } else {
            break;
        }
    }
    bytes.len() - start
}

fn encoded_size_for_choice_value(value: &ChoiceValue) -> usize {
    let (tagged_size, payload_size) = match value {
        ChoiceValue::Boolean(_) => return 1,
        ChoiceValue::Float(_) => (true, 8),
        ChoiceValue::Integer(i) => (true, signed_i128_payload_len(*i)),
        ChoiceValue::Bytes(bytes) => (true, bytes.len()),
        ChoiceValue::String(s) => (true, s.len()),
    };
    if !tagged_size {
        return payload_size;
    }
    if payload_size < 0b11111 {
        1 + payload_size
    } else {
        1 + uleb128_len(payload_size) + payload_size
    }
}

fn draw_boolean_choice_with_forced(
    engine: &mut EngineState,
    case_id: CaseId,
    p: f64,
    forced_override: Option<bool>,
    observe: bool,
) -> bool {
    fn draw_boolean_from_case_bits(case: &mut CaseBufferState, p: f64) -> bool {
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let bits = 8usize;
        let size = 1usize << bits;
        let falsey = ((size as f64) * (1.0 - p)).floor().max(1.0) as u128;
        let n = draw_bits_from_case(case, bits);
        n >= falsey
    }

    let (forced, consume_prefix) = {
        let case = case_state_mut(engine, case_id);
        let forced_from_replay = if observe {
            take_forced_choice(case, ChoiceKind::Boolean)
        } else {
            None
        };
        if let Some(forced_value) = forced_override {
            if let Some(ChoiceValue::Boolean(replay_value)) = forced_from_replay.as_ref()
                && *replay_value != forced_value
            {
                case.exhausted = true;
                stop_test_now();
            }
            (
                Some(ChoiceValue::Boolean(forced_value)),
                forced_from_replay.is_none(),
            )
        } else {
            let consume_prefix = forced_from_replay.is_none();
            (forced_from_replay, consume_prefix)
        }
    };
    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe && consume_prefix {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    let simplest_value = if p >= 1.0 { true } else { false };
    let value = if let Some(ChoiceValue::Boolean(v)) = forced.as_ref() {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ = draw_boolean_from_case_bits(case, p);
        }
        if (p <= 0.0 && *v) || (p >= 1.0 && !*v) {
            let case = case_state_mut(engine, case_id);
            case.exhausted = true;
            stop_test_now();
        }
        *v
    } else if let Some(choice) = prefix_choice {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ = draw_boolean_from_case_bits(case, p);
        }
        match choice {
            ChoiceValue::Boolean(v) if !((p <= 0.0 && v) || (p >= 1.0 && !v)) => v,
            _ => simplest_value,
        }
    } else if should_use_simplest_observed_draws(engine, observe) {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ = draw_boolean_from_case_bits(case, p);
        }
        simplest_value
    } else {
        if engine.active_run.is_some() {
            // Keep byte-buffer progression stable for replay/shrinking bookkeeping.
            let case = case_state_mut(engine, case_id);
            let _ = draw_boolean_from_case_bits(case, p);
            if p <= 0.0 {
                false
            } else if p >= 1.0 {
                true
            } else {
                run_random_f64(engine) < p
            }
        } else {
            let case = case_state_mut(engine, case_id);
            draw_boolean_from_case_bits(case, p)
        }
    };
    let case = case_state_mut(engine, case_id);
    record_choice_or_stop(
        case,
        ChoiceValue::Boolean(value),
        ChoiceConstraints::Boolean { p },
        forced.is_some(),
        observe,
    );
    value
}

pub(crate) fn draw_boolean_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    p: f64,
    observe: bool,
) -> bool {
    draw_boolean_choice_with_forced(engine, case_id, p, None, observe)
}

fn draw_u128_uniform_from_case(case: &mut CaseBufferState, min: u128, max: u128) -> u128 {
    if min == max {
        return min;
    }
    let delta = max - min;
    let bits = bit_length_u128(delta);
    loop {
        let raw = draw_bits_from_case(case, bits);
        if raw <= delta {
            break min + raw;
        }
    }
}

#[cold]
fn stop_test_now() -> ! {
    panic!("{}", crate::test_case::STOP_TEST_STRING);
}

fn case_state_mut(engine: &mut EngineState, case_id: CaseId) -> &mut CaseBufferState {
    engine.cases.entry(case_id).or_default()
}

pub fn case_stopped_because_overrun(engine: &EngineState, case_id: CaseId) -> bool {
    engine
        .cases
        .get(&case_id)
        .map(|case| case.stopped_because_overrun)
        .unwrap_or(false)
}

fn record_choice_or_stop(
    case: &mut CaseBufferState,
    value: ChoiceValue,
    constraints: ChoiceConstraints,
    was_forced: bool,
    observe: bool,
) {
    if !observe {
        return;
    }
    let encoded_size = encoded_size_for_choice_value(&value);
    if case
        .typed
        .record_choice(value, constraints, was_forced, encoded_size)
        .is_err()
    {
        case.exhausted = true;
        case.stopped_because_overrun = true;
        stop_test_now();
    }
}

fn take_forced_choice(case: &mut CaseBufferState, kind: ChoiceKind) -> Option<ChoiceValue> {
    let forced = case.forced_choices.as_ref()?;
    if case.forced_choice_cursor >= forced.len() {
        return None;
    }
    let value = forced[case.forced_choice_cursor].clone();
    if value.kind() != kind {
        case.exhausted = true;
        stop_test_now();
    }
    case.forced_choice_cursor += 1;
    Some(value)
}

fn pop_prefix_choice(case: &mut CaseBufferState) -> Option<ChoiceValue> {
    let prefix = case.prefix_choices.as_ref()?;
    if case.prefix_choice_cursor >= prefix.len() {
        return None;
    }
    let value = prefix[case.prefix_choice_cursor].clone();
    case.prefix_choice_cursor += 1;
    Some(value)
}

fn should_use_simplest_observed_draws(engine: &EngineState, observe: bool) -> bool {
    observe
        && engine
            .active_run
            .as_ref()
            .is_some_and(|run| run.current_case_simplest_observed)
}

fn draw_u128_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    min: u128,
    max: u128,
    _shrink_towards: u128,
) -> u128 {
    assert!(min <= max, "min_value cannot be greater than max_value");
    let case = case_state_mut(engine, case_id);
    draw_u128_uniform_from_case(case, min, max)
}

fn draw_integer_uniform_from_case(case: &mut CaseBufferState, min: i128, max: i128) -> i128 {
    if min == max {
        return min;
    }
    let min_u = min as u128;
    let delta = (max as u128).wrapping_sub(min_u);
    let bits = bit_length_u128(delta);
    loop {
        let raw = draw_bits_from_case(case, bits);
        if raw <= delta {
            let candidate_u = min_u.wrapping_add(raw);
            break candidate_u as i128;
        }
    }
}

fn hex_encode_u128(value: u128, width_bits: usize) -> String {
    let width_nibbles = ((width_bits.saturating_add(3)) / 4).max(1);
    format!("{:0width$x}", value, width = width_nibbles)
}

fn python_random_state_fingerprint(rng: &PythonRandom) -> String {
    format!(
        "i:{}:{:08x}:{:08x}:{:08x}:{:08x}",
        rng.index, rng.mt[0], rng.mt[1], rng.mt[2], rng.mt[3]
    )
}

fn log_random_trace_event(engine: &mut EngineState, event: RandomTraceEvent) {
    let run = engine
        .active_run
        .as_mut()
        .expect("random trace event requires active run");
    if !run.random_trace_enabled {
        return;
    }
    if let Some(case_id) = run.current_case_id {
        run.random_trace_by_case
            .entry(case_id)
            .or_default()
            .push(event);
    } else {
        run.pending_scheduler_random_events.push(event);
    }
}

fn run_random_f64(engine: &mut EngineState) -> f64 {
    let value = {
        let run = engine
            .active_run
            .as_mut()
            .expect("random draw requires active run");
        run.py_random.random()
    };
    log_random_trace_event(
        engine,
        RandomTraceEvent::Random {
            bits: value.to_bits(),
        },
    );
    value
}

fn run_getrandbits_u128(engine: &mut EngineState, k: usize) -> u128 {
    let value = {
        let run = engine
            .active_run
            .as_mut()
            .expect("getrandbits requires active run");
        run.py_random.getrandbits_u128(k)
    };
    log_random_trace_event(
        engine,
        RandomTraceEvent::GetRandBits {
            k,
            value_hex: hex_encode_u128(value, k),
        },
    );
    value
}

fn run_randbelow_u128(engine: &mut EngineState, n: u128) -> u128 {
    assert!(n > 0, "randbelow requires n > 0");
    // Match CPython Random._randbelow_with_getrandbits(n):
    // use n.bit_length(), not (n - 1).bit_length().
    let bits = bit_length_u128(n);
    loop {
        let candidate = run_getrandbits_u128(engine, bits);
        if candidate < n {
            return candidate;
        }
    }
}

fn run_randint_i128(engine: &mut EngineState, lower: i128, upper: i128) -> i128 {
    assert!(lower <= upper, "lower must be <= upper");
    if lower == upper {
        return lower;
    }
    if lower == i128::MIN && upper == i128::MAX {
        return run_getrandbits_u128(engine, 128) as i128;
    }
    let lower_u = lower as u128;
    let span = (upper as u128).wrapping_sub(lower_u);
    let offset = run_randbelow_u128(engine, span.saturating_add(1));
    lower_u.wrapping_add(offset) as i128
}

fn run_getrandbits_u128_on_run(run: &mut RunState, k: usize) -> u128 {
    let value = run.py_random.getrandbits_u128(k);
    if run.random_trace_enabled {
        run.pending_scheduler_random_events
            .push(RandomTraceEvent::GetRandBits {
                k,
                value_hex: hex_encode_u128(value, k),
            });
    }
    value
}

fn mutation_status_rank(status: &CaseStatus) -> u8 {
    match status {
        CaseStatus::Overrun => 0,
        CaseStatus::Invalid => 1,
        CaseStatus::Valid => 2,
        CaseStatus::Interesting { .. } => 3,
    }
}

fn mutation_data_from_case(
    choices: Vec<ChoiceValue>,
    constraints: Vec<ChoiceConstraints>,
    spans: Vec<SpanState>,
    status: CaseStatus,
) -> MutationCandidateData {
    let has_discard = spans.iter().any(|span| span.discard);
    MutationCandidateData {
        choices,
        constraints,
        spans,
        status,
        has_discard,
    }
}

fn clamp_slice_bounds(len: usize, start: usize, end: usize) -> (usize, usize) {
    let s = start.min(len);
    let e = end.min(len);
    if s <= e { (s, e) } else { (e, s) }
}

fn mutation_mutator_groups(data: &MutationCandidateData) -> Vec<Vec<(usize, usize)>> {
    let spans = &data.spans;
    let mut spans_by_start = spans.to_vec();
    spans_by_start.sort_by_key(|span| span.id.0);

    let mut label_order = Vec::new();
    let mut seen_labels = HashSet::new();
    for span in &spans_by_start {
        if seen_labels.insert(span.label) {
            label_order.push(span.label);
        }
    }

    let mut groups = Vec::new();
    for label in label_order {
        let mut entries: BTreeSet<(usize, usize)> = BTreeSet::new();
        for span in spans.iter().filter(|span| span.label == label) {
            entries.insert((span.start_choice_index, span.end_choice_index));
        }
        if entries.len() >= 2 {
            groups.push(entries.into_iter().collect::<Vec<_>>());
        }
    }

    if !groups.is_empty() {
        return groups;
    }

    // Fallback for generators that do not emit enough span labels:
    // group by repeated draw constraints so duplicate-value mutations are still possible.
    let mut by_constraint: BTreeMap<String, BTreeSet<(usize, usize)>> = BTreeMap::new();
    for (i, constraints) in data.constraints.iter().enumerate() {
        by_constraint
            .entry(choice_constraints_trace_token(constraints))
            .or_default()
            .insert((i, i.saturating_add(1)));
    }
    by_constraint
        .into_values()
        .filter(|entries| entries.len() >= 2)
        .map(|entries| entries.into_iter().collect::<Vec<_>>())
        .collect::<Vec<_>>()
}

fn mutation_sample_two_indices(run: &mut RunState, n: usize) -> (usize, usize) {
    assert!(n >= 2, "need at least two entries for random.sample(k=2)");

    let k = 2usize;
    let mut out = [0usize; 2];
    let setsize = 21usize;

    if n <= setsize {
        let mut pool: Vec<usize> = (0..n).collect();
        for i in 0..k {
            let j = run_randbelow_u128_on_run(run, (n - i) as u128) as usize;
            out[i] = pool[j];
            pool[j] = pool[n - i - 1];
        }
    } else {
        let mut selected = HashSet::new();
        for i in 0..k {
            let mut j = run_randbelow_u128_on_run(run, n as u128) as usize;
            while selected.contains(&j) {
                j = run_randbelow_u128_on_run(run, n as u128) as usize;
            }
            selected.insert(j);
            out[i] = j;
        }
    }
    (out[0], out[1])
}

fn run_should_generate_more_for_mutation(run: &RunState) -> bool {
    if run.next_case_id > run.settings.test_cases {
        return false;
    }
    !run.completed
        .iter()
        .any(|case| matches!(case.status, CaseStatus::Interesting { .. }))
}

enum CachedMutationOutcome {
    Novel,
    ContainsDiscard,
    Overrun,
    Predictable(MutationCandidateData),
}

fn cached_mutation_attempt_outcome(
    run: &RunState,
    attempt: &[ChoiceValue],
) -> CachedMutationOutcome {
    let (rewritten, status) = datatree::rewrite_prefix(&run.radix_tree, attempt);
    let Some(status) = status else {
        return CachedMutationOutcome::Novel;
    };
    if matches!(status, datatree::RewriteStatus::Overrun) {
        return CachedMutationOutcome::Overrun;
    }

    let key = mutation_choices_key(&rewritten);
    if let Some(cached) = run.mutation_data_cache.get(&key) {
        if cached.has_discard {
            return CachedMutationOutcome::ContainsDiscard;
        }
        return CachedMutationOutcome::Predictable(cached.clone());
    }

    let _ = rewritten;
    let _ = status;
    CachedMutationOutcome::Novel
}

fn mutation_attempt_from_state(
    run: &mut RunState,
    data: &MutationCandidateData,
) -> Option<Vec<ChoiceValue>> {
    let groups = mutation_mutator_groups(data);
    if groups.is_empty() {
        return None;
    }
    let group_idx = run_randbelow_u128_on_run(run, groups.len() as u128) as usize;
    let group = &groups[group_idx];

    let (i1, i2) = mutation_sample_two_indices(run, group.len());
    let mut span1 = group[i1];
    let mut span2 = group[i2];
    if span1.0 > span2.0 {
        std::mem::swap(&mut span1, &mut span2);
    }
    let (start1, end1) = span1;
    let (start2, end2) = span2;

    let len = data.choices.len();
    if start1 <= start2 && start2 <= end2 && end2 <= end1 {
        let (a0, a1) = clamp_slice_bounds(len, 0, start2);
        let (b0, b1) = clamp_slice_bounds(len, start1, len);
        let mut attempt = Vec::new();
        attempt.extend_from_slice(&data.choices[a0..a1]);
        attempt.extend_from_slice(&data.choices[b0..b1]);
        Some(attempt)
    } else {
        let pick_first = run_randbelow_u128_on_run(run, 2) == 0;
        let (start, end) = if pick_first {
            (start1, end1)
        } else {
            (start2, end2)
        };
        let (r0, r1) = clamp_slice_bounds(len, start, end);
        let replacement = data.choices[r0..r1].to_vec();

        let (a0, a1) = clamp_slice_bounds(len, 0, start1);
        let (b0, b1) = clamp_slice_bounds(len, end1, start2);
        let (c0, c1) = clamp_slice_bounds(len, end2, len);
        let mut attempt = Vec::new();
        attempt.extend_from_slice(&data.choices[a0..a1]);
        attempt.extend_from_slice(&replacement);
        attempt.extend_from_slice(&data.choices[b0..b1]);
        attempt.extend_from_slice(&replacement);
        attempt.extend_from_slice(&data.choices[c0..c1]);
        Some(attempt)
    }
}

fn mutation_accepts_transition(
    current: &MutationCandidateData,
    next: &MutationCandidateData,
) -> bool {
    mutation_status_rank(&next.status) >= mutation_status_rank(&current.status)
        && mutation_choices_key(&current.choices) != mutation_choices_key(&next.choices)
}

fn mutation_case_is_generate_phase_candidate(
    run: &RunState,
    schedule_tag: &str,
    initial_simplest_observed: bool,
    mutation_attempt: bool,
) -> bool {
    if initial_simplest_observed || mutation_attempt {
        return false;
    }
    // Approximate Hypothesis' `health_check_state is None` gate, including
    // startup calls that occur before visible generation cases.
    if run.completed.len() < 14 {
        return false;
    }
    matches!(
        schedule_tag,
        "novel_prefix" | "novel_prefix_none" | "random_followup"
    )
}

fn advance_pending_mutation_state_after_case(
    run: &mut RunState,
    case_id: CaseId,
    schedule_tag: &str,
    initial_simplest_observed: bool,
    mutation_attempt: bool,
    completed_data: &MutationCandidateData,
) {
    if mutation_attempt {
        let mut clear_state = false;
        if let Some(state) = run.pending_mutation_state.as_mut() {
            if state.inflight_case_id != Some(case_id) {
                clear_state = true;
            } else {
                state.inflight_case_id = None;
                if mutation_status_rank(&completed_data.status) == 0 {
                    state.failed_mutations = state.failed_mutations.saturating_add(1);
                } else if mutation_accepts_transition(&state.current_data, completed_data) {
                    state.current_data = completed_data.clone();
                    state.failed_mutations = 0;
                } else {
                    state.failed_mutations = state.failed_mutations.saturating_add(1);
                }
            }
        }
        if clear_state {
            run.pending_mutation_state = None;
        }
    } else if run.pending_mutation_state.is_none()
        && mutation_case_is_generate_phase_candidate(
            run,
            schedule_tag,
            initial_simplest_observed,
            mutation_attempt,
        )
        && mutation_status_rank(&completed_data.status)
            >= mutation_status_rank(&CaseStatus::Invalid)
    {
        let call_count = run.completed.len();
        run.pending_mutation_state = Some(PendingMutationState {
            initial_calls: call_count,
            failed_mutations: 0,
            current_data: completed_data.clone(),
            pending_attempt: None,
            inflight_case_id: None,
        });
    }

    loop {
        let can_continue = if let Some(state) = run.pending_mutation_state.as_ref() {
            run_should_generate_more_for_mutation(run)
                && state.inflight_case_id.is_none()
                && state.pending_attempt.is_none()
                && run.completed.len() <= state.initial_calls.saturating_add(5)
                && state.failed_mutations <= 5
        } else {
            false
        };
        if !can_continue {
            break;
        }

        let base_data = run
            .pending_mutation_state
            .as_ref()
            .map(|state| state.current_data.clone())
            .expect("pending state should exist");
        let Some(attempt) = mutation_attempt_from_state(run, &base_data) else {
            run.pending_mutation_state = None;
            break;
        };

        match cached_mutation_attempt_outcome(run, &attempt) {
            CachedMutationOutcome::Novel => {
                if let Some(state) = run.pending_mutation_state.as_mut() {
                    state.pending_attempt = Some(attempt);
                }
                break;
            }
            CachedMutationOutcome::ContainsDiscard | CachedMutationOutcome::Overrun => {
                if let Some(state) = run.pending_mutation_state.as_mut() {
                    state.failed_mutations = state.failed_mutations.saturating_add(1);
                }
            }
            CachedMutationOutcome::Predictable(predicted) => {
                if let Some(state) = run.pending_mutation_state.as_mut() {
                    if mutation_accepts_transition(&state.current_data, &predicted) {
                        state.current_data = predicted;
                        state.failed_mutations = 0;
                    } else {
                        state.failed_mutations = state.failed_mutations.saturating_add(1);
                    }
                }
            }
        }
    }

    if let Some(state) = run.pending_mutation_state.as_ref()
        && (state.failed_mutations > 5
            || run.completed.len() > state.initial_calls.saturating_add(5)
            || !run_should_generate_more_for_mutation(run))
    {
        run.pending_mutation_state = None;
    }
}

/// Per-case mutable byte-buffer state.
///
/// This should mirror Conjecture/Hypothesis-style semantics:
/// generation consumes from a mutable byte buffer rather than directly using RNG.
#[derive(Debug, Clone, Default)]
pub struct CaseBufferState {
    /// Full byte stream for this test case.
    pub bytes: Vec<u8>,
    /// Read cursor into `bytes`.
    pub cursor: usize,
    /// If true, no more bytes may be consumed and further draws should StopTest.
    pub exhausted: bool,
    /// True when STOP_TEST was triggered by an overrun condition.
    pub stopped_because_overrun: bool,
    /// Typed choice sequence (Phase 0 scaffolding; not yet used for generation).
    pub typed: TypedChoiceState,
    /// Optional forced observed-choice values used by replay-based shrinking.
    pub forced_choices: Option<Vec<ChoiceValue>>,
    /// Cursor into `forced_choices`.
    pub forced_choice_cursor: usize,
    /// Optional prefix values for this generated case (Hypothesis-style novel prefix).
    pub prefix_choices: Option<Vec<ChoiceValue>>,
    /// Cursor into `prefix_choices`.
    pub prefix_choice_cursor: usize,
    /// True when this case is the "minimal (simplest extension) probe" for a prefix.
    pub zero_extend_probe: bool,
    /// Scheduling path used to create this case (parity diagnostics).
    pub schedule_tag: String,
    /// True when scheduled from generate-phase mutation logic.
    pub mutation_attempt: bool,
    /// Prefix length at case creation time (parity diagnostics).
    pub initial_prefix_len: usize,
    /// Prefix-generation decision trace captured during scheduling (parity diagnostics).
    pub initial_prefix_decisions: Vec<String>,
    /// Random-event count consumed during scheduling before this case allocation.
    pub initial_pre_case_random_event_count: usize,
    /// Python RNG state at case creation time (parity diagnostics).
    pub initial_scheduler_rng_state: String,
    /// Max choices cap at case creation time (parity diagnostics).
    pub initial_max_choices: Option<usize>,
    /// Whether this case used simplest-observed mode (parity diagnostics).
    pub initial_simplest_observed: bool,
    /// Closed spans recorded for future shrink guidance.
    pub spans: Vec<SpanState>,
    /// Currently-open spans keyed by id.
    pub open_spans: HashMap<SpanId, OpenSpanState>,
    /// Monotonic per-case span id source.
    pub next_span_id: u64,
}

#[derive(Debug, Clone)]
pub struct OpenSpanState {
    pub label: u64,
    pub start_cursor: usize,
    pub start_choice_index: usize,
}

#[derive(Debug, Clone)]
pub struct SpanState {
    pub id: SpanId,
    pub label: u64,
    pub start_cursor: usize,
    pub end_cursor: usize,
    pub start_choice_index: usize,
    pub end_choice_index: usize,
    pub discard: bool,
}

#[derive(Debug, Clone)]
pub struct CollectionState {
    pub min_size: usize,
    pub max_size: Option<usize>,
    pub accepted: usize,
    pub attempted: usize,
    pub rejections: usize,
    pub force_stop: bool,
    pub p_continue: f64,
    pub finished: bool,
}

#[derive(Debug, Clone, Default)]
pub struct PoolState {
    pub next_variable_id: i128,
    pub live_variable_ids: Vec<i128>,
}

fn generation_terminal_for_status(status: &CaseStatus) -> GenerationTerminal {
    match status {
        CaseStatus::Valid => GenerationTerminal::Valid,
        CaseStatus::Invalid | CaseStatus::Overrun => GenerationTerminal::Invalid,
        CaseStatus::Interesting { .. } => GenerationTerminal::Interesting,
    }
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
    let run_seed = settings.seed.unwrap_or_else(|| {
        if settings.derandomize {
            derive_metadata_seed(&metadata)
        } else {
            nondeterministic_seed()
        }
    });
    let replay_buffers = metadata
        .database_key
        .as_deref()
        .map(|key| database_load_examples(key, &settings.database))
        .unwrap_or_default();
    let db_examples_loaded = replay_buffers.len();
    let random_trace_enabled = std::env::var_os("HEGEL_SHRINK_PARITY_RANDOM_TRACE").is_some();

    engine.collections.clear();
    engine.pools.clear();
    engine.cases.clear();
    engine.active_run = Some(RunState {
        settings,
        metadata,
        started_at: Instant::now(),
        run_seed,
        py_random: PythonRandom::from_u64_seed(run_seed),
        pending_simplest_case: true,
        current_case_simplest_observed: false,
        random_trace_enabled,
        current_case_id: None,
        random_trace_by_case: HashMap::new(),
        pending_scheduler_random_events: Vec::new(),
        last_scheduler_rng_index: None,
        next_case_id: 1,
        completed: Vec::new(),
        replay_queue: Vec::new(),
        replay_buffers,
        db_examples_loaded,
        constant_pools: todo!(),
        radix_tree: datatree::DataTree::default(),
        cached_simplest_probes: HashMap::new(),
        pending_random_prefixes: VecDeque::new(),
        consecutive_zero_extend_is_invalid: 0,
        mutation_data_cache: HashMap::new(),
        pending_mutation_state: None,
        novel_prefix_children_cache: HashMap::new(),
        last_generate_novel_prefix_trace: None,
    });
}

/// Allocate the next case to execute.
///
/// TODO:
/// - schedule replay cases before fresh random cases
/// - enforce `test_cases` budget and stop conditions
pub fn next_case(engine: &mut EngineState) -> Option<CaseId> {
    enum FreshModeCandidate {
        InitialSimplest,
        MutationAttempt {
            prefix: Vec<ChoiceValue>,
        },
        PendingFollowup {
            prefix: Vec<ChoiceValue>,
            max_choices: usize,
            radix_tree: datatree::DataTree,
        },
        NovelPrefix {
            radix_tree: datatree::DataTree,
            run_zero_extend_probe: bool,
        },
    }

    enum FreshMode {
        InitialSimplest,
        MutationAttempt {
            prefix: Vec<ChoiceValue>,
        },
        RandomFollowup {
            prefix: Vec<ChoiceValue>,
            max_choices: usize,
        },
        NovelPrefix {
            prefix: Option<Vec<ChoiceValue>>,
            run_zero_extend_probe: bool,
            generated_prefix_trace: Vec<String>,
        },
    }

    loop {
        {
            let run = engine
                .active_run
                .as_mut()
                .expect("next_case called without an active run");

            if !run.replay_queue.is_empty() {
                let case_id = run.replay_queue.remove(0);
                run.current_case_id = Some(case_id);
                run.current_case_simplest_observed = false;
                return Some(case_id);
            }

            if let Some(buffer) =
                (!run.replay_buffers.is_empty()).then(|| run.replay_buffers.remove(0))
            {
                let case_id = CaseId(run.next_case_id);
                run.next_case_id += 1;
                engine.cases.insert(
                    case_id,
                    CaseBufferState {
                        bytes: buffer,
                        ..CaseBufferState::default()
                    },
                );
                run.current_case_id = Some(case_id);
                run.current_case_simplest_observed = false;
                return Some(case_id);
            }

            if run.next_case_id > run.settings.test_cases {
                run.current_case_id = None;
                run.current_case_simplest_observed = false;
                return None;
            }
        }

        let candidate = {
            let run = engine
                .active_run
                .as_mut()
                .expect("next_case called without an active run");

            if run.pending_simplest_case {
                run.pending_simplest_case = false;
                FreshModeCandidate::InitialSimplest
            } else if let Some(attempt) = run
                .pending_mutation_state
                .as_mut()
                .and_then(|state| state.pending_attempt.take())
            {
                FreshModeCandidate::MutationAttempt { prefix: attempt }
            } else if let Some(pending) = run.pending_random_prefixes.pop_front() {
                FreshModeCandidate::PendingFollowup {
                    prefix: pending.prefix,
                    max_choices: pending.max_choices,
                    radix_tree: run.radix_tree.clone(),
                }
            } else {
                let has_interesting = run
                    .completed
                    .iter()
                    .any(|case| matches!(case.status, CaseStatus::Interesting { .. }));
                let valid_examples = run
                    .completed
                    .iter()
                    .filter(|case| matches!(case.status, CaseStatus::Valid))
                    .count();
                let call_count = run.completed.len();
                let small_example_cap =
                    std::cmp::min((run.settings.test_cases / 10) as usize, 50usize);
                let run_zero_extend_probe = !has_interesting
                    && valid_examples <= small_example_cap
                    && call_count <= 5 * small_example_cap
                    && run.consecutive_zero_extend_is_invalid < 5;
                FreshModeCandidate::NovelPrefix {
                    radix_tree: run.radix_tree.clone(),
                    run_zero_extend_probe,
                }
            }
        };

        let fresh_mode = match candidate {
            FreshModeCandidate::InitialSimplest => FreshMode::InitialSimplest,
            FreshModeCandidate::MutationAttempt { prefix } => FreshMode::MutationAttempt { prefix },
            FreshModeCandidate::PendingFollowup {
                prefix,
                max_choices,
                radix_tree,
            } => match datatree::simulate_followup_prefix(
                engine,
                &radix_tree,
                &prefix,
                max_choices,
            ) {
                datatree::FollowupSimulation::Predictable => continue,
                datatree::FollowupSimulation::Novel { prefix } => FreshMode::RandomFollowup {
                    prefix,
                    max_choices,
                },
            },
            FreshModeCandidate::NovelPrefix {
                radix_tree,
                run_zero_extend_probe,
            } => {
                let prefix = datatree::generate_novel_prefix(engine, &radix_tree);
                let generated_prefix_trace = engine
                    .active_run
                    .as_mut()
                    .and_then(|run| run.last_generate_novel_prefix_trace.take())
                    .unwrap_or_default();
                FreshMode::NovelPrefix {
                    prefix,
                    run_zero_extend_probe,
                    generated_prefix_trace,
                }
            }
        };

        let (case_id, bytes, pre_case_random_event_count, scheduler_rng_state) = {
            let run = engine
                .active_run
                .as_mut()
                .expect("next_case called without an active run");
            let case_id = CaseId(run.next_case_id);
            let bytes = make_case_buffer(run.run_seed, case_id, DEFAULT_CASE_BUFFER_BYTES);
            let current_rng_index = run.py_random.index;
            let pre_case_random_event_count = run
                .last_scheduler_rng_index
                .map(|prev| {
                    if current_rng_index >= prev {
                        current_rng_index - prev
                    } else {
                        (624usize - prev) + current_rng_index
                    }
                })
                .unwrap_or(0usize);
            let scheduler_rng_state = python_random_state_fingerprint(&run.py_random);
            run.pending_scheduler_random_events.clear();
            run.last_scheduler_rng_index = Some(current_rng_index);
            run.next_case_id += 1;
            run.current_case_id = Some(case_id);
            (
                case_id,
                bytes,
                pre_case_random_event_count,
                scheduler_rng_state,
            )
        };

        let mut case_state = CaseBufferState {
            bytes,
            ..CaseBufferState::default()
        };

        let (use_simplest_observed, schedule_tag, prefix_decisions, is_mutation_attempt) =
            match fresh_mode {
                FreshMode::InitialSimplest => (true, "initial_simplest", Vec::new(), false),
                FreshMode::MutationAttempt { prefix } => {
                    if !prefix.is_empty() {
                        case_state.prefix_choices = Some(prefix);
                    }
                    case_state.typed.max_choices =
                        case_state.prefix_choices.as_ref().map(std::vec::Vec::len);
                    let run = engine
                        .active_run
                        .as_mut()
                        .expect("next_case called without an active run");
                    if let Some(state) = run.pending_mutation_state.as_mut() {
                        state.inflight_case_id = Some(case_id);
                    }
                    (false, "random_followup", Vec::new(), true)
                }
                FreshMode::RandomFollowup {
                    prefix,
                    max_choices,
                } => {
                    if !prefix.is_empty() {
                        case_state.prefix_choices = Some(prefix);
                    }
                    case_state.typed.max_choices = Some(max_choices);
                    (false, "random_followup", Vec::new(), false)
                }
                FreshMode::NovelPrefix {
                    prefix,
                    run_zero_extend_probe,
                    generated_prefix_trace,
                } => {
                    if let Some(prefix) = prefix {
                        if run_zero_extend_probe {
                            let cache_key = prefix_cache_key(&prefix);
                            let cached_probe = engine.active_run.as_ref().and_then(|run| {
                                run.cached_simplest_probes.get(&cache_key).copied()
                            });
                            if let Some(cached_probe) = cached_probe {
                                let run = engine
                                    .active_run
                                    .as_mut()
                                    .expect("next_case called without an active run");
                                match cached_probe.terminal {
                                    GenerationTerminal::Valid => {
                                        run.consecutive_zero_extend_is_invalid = 0;
                                        let minimal_extension =
                                            cached_probe.node_count.saturating_sub(prefix.len());
                                        let max_choices = prefix
                                            .len()
                                            .saturating_add(minimal_extension.saturating_mul(5));
                                        run.pending_random_prefixes.push_back(
                                            PendingRandomPrefixCase {
                                                prefix,
                                                max_choices,
                                            },
                                        );
                                    }
                                    GenerationTerminal::Invalid => {
                                        run.consecutive_zero_extend_is_invalid = run
                                            .consecutive_zero_extend_is_invalid
                                            .saturating_add(1);
                                    }
                                    GenerationTerminal::Interesting => {
                                        run.consecutive_zero_extend_is_invalid = 0;
                                    }
                                }
                                continue;
                            }
                        }

                        case_state.prefix_choices = Some(prefix);
                        case_state.zero_extend_probe = run_zero_extend_probe;
                        if run_zero_extend_probe {
                            (true, "novel_zero_probe", generated_prefix_trace, false)
                        } else {
                            (false, "novel_prefix", generated_prefix_trace, false)
                        }
                    } else {
                        (false, "novel_prefix_none", generated_prefix_trace, false)
                    }
                }
            };
        case_state.schedule_tag = schedule_tag.to_string();
        case_state.mutation_attempt = is_mutation_attempt;
        case_state.initial_prefix_decisions = prefix_decisions;
        case_state.initial_pre_case_random_event_count = pre_case_random_event_count;
        case_state.initial_scheduler_rng_state = scheduler_rng_state;
        case_state.initial_prefix_len = case_state
            .prefix_choices
            .as_ref()
            .map_or(0, std::vec::Vec::len);
        case_state.initial_max_choices = case_state.typed.max_choices;
        case_state.initial_simplest_observed = use_simplest_observed;

        let run = engine
            .active_run
            .as_mut()
            .expect("next_case called without an active run");
        run.current_case_simplest_observed = use_simplest_observed;

        engine.cases.insert(case_id, case_state);
        return Some(case_id);
    }
}

pub fn set_case_forced_choices(
    engine: &mut EngineState,
    case_id: CaseId,
    forced_choices: Vec<ChoiceValue>,
) {
    let case = engine
        .cases
        .get_mut(&case_id)
        .unwrap_or_else(|| panic!("unknown case id {}", case_id.0));
    case.forced_choices = Some(forced_choices);
    case.forced_choice_cursor = 0;
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
    if run.current_case_id == Some(case_id) {
        run.current_case_id = None;
        run.current_case_simplest_observed = false;
    }

    let (
        buffer,
        typed_nodes,
        spans,
        probe_prefix,
        initial_prefix_choices,
        initial_prefix_decisions,
        initial_pre_case_random_event_count,
        initial_scheduler_rng_state,
        schedule_tag,
        initial_prefix_len,
        initial_max_choices,
        initial_simplest_observed,
        initial_zero_extend_probe,
        mutation_attempt,
    ) = engine
        .cases
        .remove(&case_id)
        .map(|c| {
            let probe_prefix = if c.zero_extend_probe {
                c.prefix_choices.clone()
            } else {
                None
            };
            (
                c.bytes[..c.cursor.min(c.bytes.len())].to_vec(),
                c.typed.nodes,
                c.spans,
                probe_prefix,
                c.prefix_choices.unwrap_or_default(),
                c.initial_prefix_decisions,
                c.initial_pre_case_random_event_count,
                c.initial_scheduler_rng_state,
                c.schedule_tag,
                c.initial_prefix_len,
                c.initial_max_choices,
                c.initial_simplest_observed,
                c.zero_extend_probe,
                c.mutation_attempt,
            )
        })
        .unwrap_or_else(|| {
            (
                Vec::new(),
                Vec::new(),
                Vec::new(),
                None,
                Vec::new(),
                Vec::new(),
                0,
                String::new(),
                String::new(),
                0,
                None,
                false,
                false,
                false,
            )
        });
    let random_events = run
        .random_trace_by_case
        .remove(&case_id)
        .unwrap_or_default();
    let status_for_flow = status.clone();
    let typed_choice_values = typed_nodes
        .iter()
        .map(|node| node.value.clone())
        .collect::<Vec<_>>();
    let typed_choice_constraints = typed_nodes
        .iter()
        .map(|node| node.constraints.clone())
        .collect::<Vec<_>>();
    let mutation_completed_data = mutation_data_from_case(
        typed_choice_values.clone(),
        typed_choice_constraints,
        spans.clone(),
        status_for_flow.clone(),
    );
    let typed_node_count = typed_nodes.len();
    datatree::record_case(&mut run.radix_tree, &typed_nodes, &status_for_flow);
    let tree_signature = radix_tree_signature(&run.radix_tree);

    run.completed.push(CompletedCase {
        case_id,
        status,
        duration,
        buffer,
        typed_nodes,
        spans,
        random_events,
        draws,
    });

    if std::env::var_os("HEGEL_SHRINK_PARITY_CASE_TRACE").is_some()
        && let Some(case) = run.completed.last()
    {
        let scenario = std::env::var("HEGEL_SHRINK_PARITY_SCENARIO")
            .unwrap_or_else(|_| "<unknown>".to_string());
        let node_tokens = case
            .typed_nodes
            .iter()
            .map(|node| choice_value_trace_token(&node.value))
            .collect::<Vec<_>>();
        let constraint_tokens = case
            .typed_nodes
            .iter()
            .map(|node| choice_constraints_trace_token(&node.constraints))
            .collect::<Vec<_>>();
        eprintln!(
            "case_trace {} case_id={} status={} node_count={} random_events={} nodes={} constraints={}",
            scenario,
            case.case_id.0,
            case_status_kind(&case.status),
            case.typed_nodes.len(),
            case.random_events.len(),
            node_tokens.join("\t"),
            constraint_tokens.join("\t")
        );
        eprintln!(
            "case_trace_meta {} case_id={} schedule={} prefix_len={} max_choices={} simplest={} zero_probe={} pre_random_events={} rng_state={}",
            scenario,
            case.case_id.0,
            schedule_tag,
            initial_prefix_len,
            initial_max_choices.map_or_else(|| "None".to_string(), |n| n.to_string()),
            if initial_simplest_observed { 1 } else { 0 },
            if initial_zero_extend_probe { 1 } else { 0 },
            initial_pre_case_random_event_count,
            initial_scheduler_rng_state
        );
        eprintln!(
            "case_trace_prefix {} case_id={} prefix={}",
            scenario,
            case.case_id.0,
            initial_prefix_choices
                .iter()
                .map(choice_value_trace_token)
                .collect::<Vec<_>>()
                .join("\t")
        );
        eprintln!(
            "case_trace_prefix_decisions {} case_id={} decisions={}",
            scenario,
            case.case_id.0,
            initial_prefix_decisions.join("\t")
        );
        eprintln!(
            "case_trace_tree {} case_id={} signature={}",
            scenario, case.case_id.0, tree_signature
        );
    }

    if let Some(prefix) = probe_prefix {
        run.cached_simplest_probes.insert(
            prefix_cache_key(&prefix),
            CachedSimplestProbeResult {
                terminal: generation_terminal_for_status(&status_for_flow),
                node_count: typed_node_count,
            },
        );
        match status_for_flow {
            CaseStatus::Valid => {
                run.consecutive_zero_extend_is_invalid = 0;
                let minimal_extension = typed_node_count.saturating_sub(prefix.len());
                let max_choices = prefix
                    .len()
                    .saturating_add(minimal_extension.saturating_mul(5));
                run.pending_random_prefixes
                    .push_back(PendingRandomPrefixCase {
                        prefix,
                        max_choices,
                    });
            }
            CaseStatus::Invalid | CaseStatus::Overrun => {
                run.consecutive_zero_extend_is_invalid =
                    run.consecutive_zero_extend_is_invalid.saturating_add(1);
            }
            CaseStatus::Interesting { .. } => {
                run.consecutive_zero_extend_is_invalid = 0;
            }
        }
    }

    run.mutation_data_cache.insert(
        mutation_choices_key(&typed_choice_values),
        mutation_completed_data.clone(),
    );
    advance_pending_mutation_state_after_case(
        run,
        case_id,
        &schedule_tag,
        initial_simplest_observed,
        mutation_attempt,
        &mutation_completed_data,
    );
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
    engine.cases.clear();

    let interesting_test_cases = run
        .completed
        .iter()
        .filter(|c| matches!(c.status, CaseStatus::Interesting { .. }))
        .count();

    if let Some(database_key) = run.metadata.database_key.as_deref()
        && let Some(case) = run
            .completed
            .iter()
            .find(|case| matches!(case.status, CaseStatus::Interesting { .. }))
    {
        database_store_example(database_key, &run.settings.database, &case.buffer);
    }

    // TODO: evaluate health checks once counters are wired.
    let health_check_failure = None;
    // TODO: run replay-based flaky detection once replay/shrinking is wired.
    let flaky = None;
    // Surface first interesting-case panic until shrinking/replay is wired.
    // Keep the message text stable for existing test helpers that match panic
    // strings exactly (e.g. minimal()/find_any wrappers).
    let error = run.completed.iter().find_map(|case| match &case.status {
        CaseStatus::Interesting { panic_message, .. } => Some(panic_message.clone()),
        _ => None,
    });

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
/// - read/write through case byte-buffer state (`CaseBufferState`)
/// - produce deterministic output under replay
pub fn generate(engine: &mut EngineState, case_id: CaseId, schema: &Value) -> Value {
    let schema_type =
        crate::cbor_utils::map_get(schema, "type").and_then(crate::cbor_utils::as_text);

    match schema_type {
        Some("boolean") => {
            let p = map_f64(schema, "p").unwrap_or(0.5);
            Value::Bool(draw_boolean_choice(engine, case_id, p, true))
        }
        Some("integer") => {
            let shrink_towards = map_i128(schema, "shrink_towards").unwrap_or(0);
            let min_raw = crate::cbor_utils::map_get(schema, "min_value");
            let max_raw = crate::cbor_utils::map_get(schema, "max_value");
            if let (Some(min_u), Some(max_u)) = (
                min_raw.and_then(value_as_u128),
                max_raw.and_then(value_as_u128),
            ) {
                assert!(min_u <= max_u, "min_value cannot be greater than max_value");
                if max_u > i128::MAX as u128 || min_u > i128::MAX as u128 {
                    let shrink_u = if shrink_towards < 0 {
                        0
                    } else {
                        shrink_towards as u128
                    };
                    let value = draw_u128_choice(engine, case_id, min_u, max_u, shrink_u);
                    return Value::from(value);
                }
                let value = draw_integer_choice(
                    engine,
                    case_id,
                    Some(min_u as i128),
                    Some(max_u as i128),
                    shrink_towards,
                    true,
                );
                return Value::from(value);
            }

            let min_value = map_i128(schema, "min_value");
            let max_value = map_i128(schema, "max_value");
            let value =
                draw_integer_choice(engine, case_id, min_value, max_value, shrink_towards, true);
            Value::from(value)
        }
        Some("float") => {
            let width = map_u64(schema, "width").unwrap_or(64);
            let mut min_value = map_f64(schema, "min_value").unwrap_or(f64::NEG_INFINITY);
            let mut max_value = map_f64(schema, "max_value").unwrap_or(f64::INFINITY);
            let allow_nan = map_bool(schema, "allow_nan").unwrap_or(true);
            let allow_infinity = map_bool(schema, "allow_infinity").unwrap_or(true);
            let exclude_min = map_bool(schema, "exclude_min").unwrap_or(false);
            let exclude_max = map_bool(schema, "exclude_max").unwrap_or(false);
            let smallest_nonzero_magnitude = map_f64(schema, "smallest_nonzero_magnitude")
                .unwrap_or_else(|| {
                    if width == 32 {
                        f32::from_bits(1) as f64
                    } else {
                        f64::from_bits(1)
                    }
                });

            if !allow_infinity {
                if width == 32 {
                    min_value = min_value.max(-(f32::MAX as f64));
                    max_value = max_value.min(f32::MAX as f64);
                } else {
                    min_value = min_value.max(-f64::MAX);
                    max_value = max_value.min(f64::MAX);
                }
            }
            if exclude_min && min_value.is_finite() {
                min_value = float_next_up_for_width(min_value, width);
            }
            if exclude_max && max_value.is_finite() {
                max_value = float_next_down_for_width(max_value, width);
            }
            assert!(
                min_value <= max_value,
                "invalid float bounds after exclusions"
            );

            let value = draw_float_choice(
                engine,
                case_id,
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
                true,
            );
            Value::from(value)
        }
        Some("binary") => {
            let (min_size, max_size) = parse_collection_bounds(schema, 64);
            Value::Bytes(draw_bytes_choice(engine, case_id, min_size, max_size, true))
        }
        Some("string") => {
            let (min_size, max_size) = parse_collection_bounds(schema, 64);
            let constraints = parse_string_char_constraints(schema);
            Value::Text(draw_string_choice_with_constraints(
                engine,
                case_id,
                min_size,
                max_size,
                &constraints,
                true,
            ))
        }
        Some("regex") => generate_regex_value(engine, case_id, schema),
        Some("list") => {
            let min_size = map_usize(schema, "min_size").unwrap_or(0);
            let max_size = map_usize(schema, "max_size");
            let unique = map_bool(schema, "unique").unwrap_or(false);
            let elements_schema = crate::cbor_utils::map_get(schema, "elements")
                .unwrap_or_else(|| panic!("list schema missing `elements`"))
                .clone();

            let collection_id = new_collection(engine, case_id, min_size, max_size);
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            while collection_more(engine, case_id, collection_id) {
                let value = generate(engine, case_id, &elements_schema);
                if unique {
                    let key = value_fingerprint(&value);
                    if !seen.insert(key) {
                        collection_reject(
                            engine,
                            case_id,
                            collection_id,
                            Some("duplicate element"),
                        );
                        continue;
                    }
                }
                out.push(value);
            }
            Value::Array(out)
        }
        Some("tuple") => {
            let elements = crate::cbor_utils::map_get(schema, "elements")
                .unwrap_or_else(|| panic!("tuple schema missing `elements`"));
            let Value::Array(element_schemas) = elements else {
                panic!("tuple `elements` must be an array");
            };
            Value::Array(
                element_schemas
                    .iter()
                    .map(|element| generate(engine, case_id, element))
                    .collect(),
            )
        }
        Some("dict") => {
            let min_size = map_usize(schema, "min_size").unwrap_or(0);
            let max_size = map_usize(schema, "max_size");
            let keys_schema = crate::cbor_utils::map_get(schema, "keys")
                .unwrap_or_else(|| panic!("dict schema missing `keys`"))
                .clone();
            let values_schema = crate::cbor_utils::map_get(schema, "values")
                .unwrap_or_else(|| panic!("dict schema missing `values`"))
                .clone();

            let collection_id = new_collection(engine, case_id, min_size, max_size);
            let mut out = Vec::new();
            let mut seen_keys = HashSet::new();
            while collection_more(engine, case_id, collection_id) {
                let key = generate(engine, case_id, &keys_schema);
                let key_fp = value_fingerprint(&key);
                if !seen_keys.insert(key_fp) {
                    collection_reject(engine, case_id, collection_id, Some("duplicate key"));
                    continue;
                }
                let value = generate(engine, case_id, &values_schema);
                out.push(Value::Array(vec![key, value]));
            }
            Value::Array(out)
        }
        Some("one_of") => {
            let generators = crate::cbor_utils::map_get(schema, "generators")
                .unwrap_or_else(|| panic!("one_of schema missing `generators`"));
            let Value::Array(options) = generators else {
                panic!("one_of `generators` must be an array");
            };
            assert!(
                !options.is_empty(),
                "one_of schema must contain at least one option"
            );
            let idx = if options.len() == 1 {
                0usize
            } else {
                draw_integer_choice(
                    engine,
                    case_id,
                    Some(0),
                    Some((options.len() - 1) as i128),
                    0,
                    true,
                ) as usize
            };
            generate(engine, case_id, &options[idx])
        }
        Some("sampled_from") => {
            let values = crate::cbor_utils::map_get(schema, "values")
                .unwrap_or_else(|| panic!("sampled_from schema missing `values`"));
            let Value::Array(options) = values else {
                panic!("sampled_from `values` must be an array");
            };
            assert!(
                !options.is_empty(),
                "sampled_from schema must contain at least one value"
            );
            let idx = if options.len() == 1 {
                0usize
            } else {
                draw_integer_choice(
                    engine,
                    case_id,
                    Some(0),
                    Some((options.len() - 1) as i128),
                    0,
                    true,
                ) as usize
            };
            options[idx].clone()
        }
        Some("constant") => crate::cbor_utils::map_get(schema, "value")
            .cloned()
            .unwrap_or(Value::Null),
        Some("null") => Value::Null,
        Some(other) => panic!("Unsupported native schema type: {}", other),
        None => {
            if let Some(Value::Array(options)) = crate::cbor_utils::map_get(schema, "one_of") {
                assert!(
                    !options.is_empty(),
                    "one_of schema must contain at least one option"
                );
                let idx = if options.len() == 1 {
                    0usize
                } else {
                    draw_integer_choice(
                        engine,
                        case_id,
                        Some(0),
                        Some((options.len() - 1) as i128),
                        0,
                        false,
                    ) as usize
                };
                return generate(engine, case_id, &options[idx]);
            }
            panic!("Unsupported native schema (missing `type`): {:?}", schema);
        }
    }
}

/// Start a shrink span. Equivalent to protocol `start_span`.
///
/// TODO:
/// - push span frame with label and start offset in decision buffer
pub fn start_span(engine: &mut EngineState, case_id: CaseId, label: u64) -> SpanId {
    let case = case_state_mut(engine, case_id);
    case.next_span_id = case.next_span_id.saturating_add(1);
    let span_id = SpanId(case.next_span_id);
    case.open_spans.insert(
        span_id,
        OpenSpanState {
            label,
            start_cursor: case.cursor,
            start_choice_index: case.typed.len(),
        },
    );
    span_id
}

/// Stop a shrink span. Equivalent to protocol `stop_span`.
///
/// TODO:
/// - close span frame and mark `discard` for shrink guidance
pub fn stop_span(engine: &mut EngineState, case_id: CaseId, span_id: SpanId, discard: bool) {
    let case = case_state_mut(engine, case_id);
    let open = case
        .open_spans
        .remove(&span_id)
        .unwrap_or_else(|| panic!("unknown open span id {}", span_id.0));
    case.spans.push(SpanState {
        id: span_id,
        label: open.label,
        start_cursor: open.start_cursor,
        end_cursor: case.cursor,
        start_choice_index: open.start_choice_index,
        end_choice_index: case.typed.len(),
        discard,
    });
}

/// Create collection sizing state. Equivalent to protocol `new_collection`.
pub fn new_collection(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: Option<usize>,
) -> CollectionId {
    let _ = case_id;
    if let Some(max) = max_size {
        assert!(min_size <= max, "Cannot have max_size < min_size");
    }

    let next_id = engine
        .collections
        .keys()
        .map(|id| id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let average_size = default_average_size(min_size, max_size);
    let desired_avg = (average_size - min_size as f64).max(0.0);
    let max_delta = max_size.map(|max| max.saturating_sub(min_size));

    let id = CollectionId(next_id);
    engine.collections.insert(
        id,
        CollectionState {
            min_size,
            max_size,
            accepted: 0,
            attempted: 0,
            rejections: 0,
            force_stop: false,
            p_continue: calc_p_continue(desired_avg, max_delta),
            finished: false,
        },
    );
    id
}

/// Decide whether collection should generate one more element.
/// Equivalent to protocol `collection_more`.
pub fn collection_more(
    engine: &mut EngineState,
    case_id: CaseId,
    collection_id: CollectionId,
) -> bool {
    let state = engine
        .collections
        .get(&collection_id)
        .unwrap_or_else(|| panic!("unknown collection id {}", collection_id.0))
        .clone();

    if state.finished {
        return false;
    }

    let should_continue = if matches!(state.max_size, Some(max) if state.min_size == max) {
        // Match Hypothesis many.more(): exact-size collections do not emit a
        // continuation choice; they stop/start deterministically.
        state.accepted < state.min_size
    } else {
        let forced_result = if state.force_stop {
            Some(false)
        } else if state.accepted < state.min_size {
            Some(true)
        } else if matches!(state.max_size, Some(max) if state.accepted >= max) {
            Some(false)
        } else {
            None
        };

        draw_boolean_choice_with_forced(engine, case_id, state.p_continue, forced_result, true)
    };

    let current = engine
        .collections
        .get_mut(&collection_id)
        .unwrap_or_else(|| panic!("unknown collection id {}", collection_id.0));
    if should_continue {
        current.accepted += 1;
        current.attempted += 1;
        true
    } else {
        current.finished = true;
        false
    }
}

/// Reject last attempted collection element.
/// Equivalent to protocol `collection_reject`.
pub fn collection_reject(
    engine: &mut EngineState,
    case_id: CaseId,
    collection_id: CollectionId,
    why: Option<&str>,
) {
    let _ = (case_id, why);
    let state = engine
        .collections
        .get_mut(&collection_id)
        .unwrap_or_else(|| panic!("unknown collection id {}", collection_id.0));

    if state.finished || state.accepted == 0 {
        return;
    }

    state.accepted -= 1;
    state.rejections += 1;
    if state.rejections > std::cmp::max(3, 2 * state.accepted) {
        if state.accepted < state.min_size {
            stop_test_now();
        } else {
            state.force_stop = true;
        }
    }
}

/// Create a stateful variable pool. Equivalent to protocol `new_pool`.
pub fn new_pool(engine: &mut EngineState, case_id: CaseId) -> PoolId {
    let _ = case_id;
    let next_id = engine
        .pools
        .keys()
        .map(|id| id.0)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let id = PoolId(next_id);
    engine.pools.insert(id, PoolState::default());
    id
}

/// Add a value slot to pool and return variable id.
/// Equivalent to protocol `pool_add`.
pub fn pool_add(engine: &mut EngineState, case_id: CaseId, pool_id: PoolId) -> i128 {
    let _ = case_id;
    let pool = engine
        .pools
        .get_mut(&pool_id)
        .unwrap_or_else(|| panic!("unknown pool id {}", pool_id.0));
    let variable_id = pool.next_variable_id;
    pool.next_variable_id = pool.next_variable_id.saturating_add(1);
    pool.live_variable_ids.push(variable_id);
    variable_id
}

/// Draw or consume a variable id from pool.
/// Equivalent to protocol `pool_generate`.
pub fn pool_generate(
    engine: &mut EngineState,
    case_id: CaseId,
    pool_id: PoolId,
    consume: bool,
) -> i128 {
    let live_ids = engine
        .pools
        .get(&pool_id)
        .unwrap_or_else(|| panic!("unknown pool id {}", pool_id.0))
        .live_variable_ids
        .clone();

    if live_ids.is_empty() {
        stop_test_now();
    }

    let idx = if live_ids.len() == 1 {
        0usize
    } else {
        draw_integer_choice(
            engine,
            case_id,
            Some(0),
            Some((live_ids.len() - 1) as i128),
            0,
            true,
        ) as usize
    };
    let variable_id = live_ids[idx];

    if consume {
        let pool = engine
            .pools
            .get_mut(&pool_id)
            .unwrap_or_else(|| panic!("unknown pool id {}", pool_id.0));
        let remove_idx = pool
            .live_variable_ids
            .iter()
            .position(|id| *id == variable_id)
            .unwrap_or_else(|| panic!("variable id {} not present in pool", variable_id));
        pool.live_variable_ids.remove(remove_idx);
    }

    variable_id
}

/// Load stored examples for a test key.
///
/// TODO:
/// - design Rust-native on-disk format (or preserve compatibility with existing DB format)
/// - bound file IO and handle corruption gracefully
fn database_file_path(database_key: &str, database: &DatabaseMode) -> Option<PathBuf> {
    todo!()
}

pub fn database_load_examples(database_key: &str, database: &DatabaseMode) -> Vec<Vec<u8>> {
    todo!()
}

/// Store a new interesting example for future replay.
pub fn database_store_example(database_key: &str, database: &DatabaseMode, encoded_case: &[u8]) {
    todo!()
}

/// Evaluate active health checks and return first failure, if any.
pub fn evaluate_health_checks(
    settings: &EngineSettings,
    counters: &BTreeMap<&'static str, u64>,
) -> Option<String> {
    let _ = (settings, counters);
    todo!("evaluate_health_checks")
}

fn case_status_kind(status: &CaseStatus) -> &'static str {
    match status {
        CaseStatus::Valid => "valid",
        CaseStatus::Invalid => "invalid",
        CaseStatus::Overrun => "overrun",
        CaseStatus::Interesting { .. } => "interesting",
    }
}

fn fnv1a64_update(state: &mut u64, bytes: &[u8]) {
    const FNV_PRIME: u64 = 1_099_511_628_211;
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(FNV_PRIME);
    }
}

/// Determine whether replay indicates flaky behavior.
pub fn detect_flaky(original: &CompletedCase, replayed: &CompletedCase) -> Option<String> {
    match (&original.status, &replayed.status) {
        (
            CaseStatus::Interesting {
                panic_message: left,
                ..
            },
            CaseStatus::Interesting {
                panic_message: right,
                ..
            },
        ) if left != right => {
            return Some(
                "Your data generation is non-deterministic: replay produced a different panic \
message."
                    .to_string(),
            );
        }
        (left, right) if std::mem::discriminant(left) != std::mem::discriminant(right) => {
            return Some(format!(
                "Your data generation is non-deterministic: replay changed case status from {} \
to {}.",
                case_status_kind(left),
                case_status_kind(right),
            ));
        }
        _ => {}
    }

    if original.typed_nodes != replayed.typed_nodes {
        return Some(
            "Your data generation is non-deterministic: replay produced different draw choices."
                .to_string(),
        );
    }
    if original.buffer != replayed.buffer {
        return Some(
            "Your data generation is non-deterministic: replay consumed a different byte buffer."
                .to_string(),
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use ciborium::Value;
    use serde_json::json;

    use super::{
        CaseBufferState, CaseId, ChoiceConstraints, ChoiceKind, ChoiceValue, EngineSettings,
        EngineState, RecordChoiceError, RunState, TestMetadata, TypedChoiceState, collection_more,
        collection_reject, draw_boolean_choice, draw_float_choice, draw_integer_choice, generate,
        generation_tree_choice_from_index, generation_tree_max_children, new_collection, new_pool,
        next_case, pool_add, pool_generate, run_draw_choice_from_constraints, run_test, start_span,
        stop_span,
    };

    fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = payload.downcast_ref::<String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_string()
        }
    }

    #[test]
    fn phase0_records_choice_with_index_and_size() {
        let mut state = TypedChoiceState::with_limits(16, Some(4));
        let node = state
            .record_choice(
                ChoiceValue::Integer(7),
                ChoiceConstraints::Integer {
                    min_value: Some(0),
                    max_value: Some(10),
                    shrink_towards: 0,
                },
                false,
                3,
            )
            .expect("recording should succeed");

        assert_eq!(node.index, 0);
        assert_eq!(state.len(), 1);
        assert_eq!(state.observed_size, 3);
    }

    #[test]
    fn phase0_rejects_choice_type_mismatch() {
        let mut state = TypedChoiceState::default();
        let err = state
            .record_choice(
                ChoiceValue::Bytes(vec![1, 2, 3]),
                ChoiceConstraints::String {
                    min_size: 0,
                    max_size: 8,
                    alphabet: None,
                },
                false,
                4,
            )
            .expect_err("mismatched type should fail");

        assert_eq!(
            err,
            RecordChoiceError::TypeMismatch {
                expected: super::ChoiceKind::String,
                got: super::ChoiceKind::Bytes,
            }
        );
        assert!(state.is_empty());
    }

    #[test]
    fn phase0_rejects_when_max_choices_is_reached() {
        let mut state = TypedChoiceState::with_limits(32, Some(1));
        state
            .record_choice(
                ChoiceValue::Boolean(true),
                ChoiceConstraints::Boolean { p: 0.5 },
                false,
                1,
            )
            .expect("first record should succeed");

        let err = state
            .record_choice(
                ChoiceValue::Boolean(false),
                ChoiceConstraints::Boolean { p: 0.5 },
                false,
                1,
            )
            .expect_err("second record should exceed max choices");

        assert_eq!(
            err,
            RecordChoiceError::MaxChoicesExceeded { current: 1, max: 1 }
        );
    }

    #[test]
    fn phase0_rejects_when_size_budget_is_exceeded() {
        let mut state = TypedChoiceState::with_limits(4, None);
        state
            .record_choice(
                ChoiceValue::Bytes(vec![0, 1]),
                ChoiceConstraints::Bytes {
                    min_size: 0,
                    max_size: 8,
                },
                false,
                3,
            )
            .expect("first record should fit");

        let err = state
            .record_choice(
                ChoiceValue::Bytes(vec![2, 3]),
                ChoiceConstraints::Bytes {
                    min_size: 0,
                    max_size: 8,
                },
                false,
                2,
            )
            .expect_err("second record should exceed size");

        assert_eq!(
            err,
            RecordChoiceError::MaxLengthExceeded {
                current: 3,
                added: 2,
                max: 4,
            }
        );
        assert_eq!(state.len(), 1);
        assert_eq!(state.observed_size, 3);
    }

    #[test]
    fn phase1_boolean_draw_consumes_one_byte_and_records_choice() {
        let case_id = CaseId(1);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF],
                ..CaseBufferState::default()
            },
        );

        let value = draw_boolean_choice(&mut engine, case_id, 0.5, true);
        assert!(value);

        let case = engine.cases.get(&case_id).unwrap();
        assert_eq!(case.cursor, 1);
        assert_eq!(case.typed.len(), 1);
        assert_eq!(case.typed.nodes[0].kind, ChoiceKind::Boolean);
        assert_eq!(case.typed.nodes[0].encoded_size, 1);
    }

    #[test]
    fn phase1_integer_draw_retries_until_in_range() {
        let case_id = CaseId(2);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x03, 0x00],
                ..CaseBufferState::default()
            },
        );

        let value = draw_integer_choice(&mut engine, case_id, Some(1), Some(3), 0, true);
        assert_eq!(value, 1);

        let case = engine.cases.get(&case_id).unwrap();
        assert_eq!(case.cursor, 2);
        assert_eq!(case.typed.len(), 1);
        assert_eq!(case.typed.nodes[0].kind, ChoiceKind::Integer);
    }

    #[test]
    fn phase1_float_draw_applies_constraints_and_records() {
        let case_id = CaseId(3);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF; 8],
                ..CaseBufferState::default()
            },
        );

        let value = draw_float_choice(&mut engine, case_id, -1.0, 1.0, false, 0.1, true);
        assert!(value >= -1.0);
        assert!(value <= 1.0);
        assert!(!value.is_nan());

        let case = engine.cases.get(&case_id).unwrap();
        assert_eq!(case.cursor, 8);
        assert_eq!(case.typed.len(), 1);
        assert_eq!(case.typed.nodes[0].kind, ChoiceKind::Float);
    }

    #[test]
    fn phase1_overrun_panics_stop_test_and_marks_case_exhausted() {
        let case_id = CaseId(4);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x01],
                ..CaseBufferState::default()
            },
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = draw_integer_choice(&mut engine, case_id, Some(0), Some(255), 0, false);
            let _ = draw_integer_choice(&mut engine, case_id, Some(0), Some(255), 0, false);
        }))
        .expect_err("second draw should overrun buffer");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
        let case = engine.cases.get(&case_id).unwrap();
        assert!(case.exhausted);
    }

    #[test]
    fn phase1_choice_budget_panics_stop_test() {
        let case_id = CaseId(5);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF, 0xFF],
                typed: TypedChoiceState::with_limits(8 * 1024, Some(1)),
                ..CaseBufferState::default()
            },
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = draw_boolean_choice(&mut engine, case_id, 0.5, true);
            let _ = draw_boolean_choice(&mut engine, case_id, 0.5, true);
        }))
        .expect_err("second observed draw should exceed choice budget");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
        let case = engine.cases.get(&case_id).unwrap();
        assert!(case.exhausted);
    }

    #[test]
    fn phase1_size_budget_panics_stop_test() {
        let case_id = CaseId(6);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF, 0x00],
                typed: TypedChoiceState::with_limits(1, None),
                ..CaseBufferState::default()
            },
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = draw_boolean_choice(&mut engine, case_id, 0.5, true);
            let _ = draw_boolean_choice(&mut engine, case_id, 0.5, true);
        }))
        .expect_err("second observed draw should exceed size budget");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
        let case = engine.cases.get(&case_id).unwrap();
        assert!(case.exhausted);
    }

    #[test]
    fn phase2_generate_boolean_schema() {
        let case_id = CaseId(7);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF],
                ..CaseBufferState::default()
            },
        );

        let out = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {"type" => "boolean"},
        );
        assert_eq!(out, Value::Bool(true));
    }

    #[test]
    fn phase2_generate_constant_and_null_schemas() {
        let case_id = CaseId(8);
        let mut engine = EngineState::default();
        let constant = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "constant",
                "value" => 42i64
            },
        );
        let null = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {"type" => "null"},
        );

        assert_eq!(constant, Value::from(42i64));
        assert_eq!(null, Value::Null);
    }

    #[test]
    fn phase2_generate_binary_schema() {
        let case_id = CaseId(9);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xAA, 0xBB],
                ..CaseBufferState::default()
            },
        );

        let out = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "binary",
                "min_size" => 2u64,
                "max_size" => 2u64
            },
        );

        assert_eq!(out, Value::Bytes(vec![0xAA, 0xBB]));
    }

    #[test]
    fn phase2_generate_string_schema_with_include_characters() {
        let case_id = CaseId(10);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x00, 0x01, 0x01],
                ..CaseBufferState::default()
            },
        );

        let out = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "string",
                "min_size" => 3u64,
                "max_size" => 3u64,
                "include_characters" => "ab"
            },
        );

        assert_eq!(out, Value::Text("abb".to_string()));
    }

    #[test]
    fn phase2_generate_integer_schema_records_integer_choice() {
        let case_id = CaseId(11);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x03],
                ..CaseBufferState::default()
            },
        );

        let out = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "integer",
                "min_value" => 0u64,
                "max_value" => 3u64
            },
        );

        assert_eq!(out, Value::from(3i64));
        let case = engine.cases.get(&case_id).unwrap();
        assert_eq!(case.typed.len(), 1);
        assert_eq!(case.typed.nodes[0].kind, ChoiceKind::Integer);
    }

    #[test]
    fn phase2_generate_overrun_panics_stop_test() {
        let case_id = CaseId(12);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x01],
                ..CaseBufferState::default()
            },
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = generate(
                &mut engine,
                case_id,
                &crate::cbor_utils::cbor_map! {
                    "type" => "binary",
                    "min_size" => 2u64,
                    "max_size" => 2u64
                },
            );
        }))
        .expect_err("expected buffer overrun to stop test");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
    }

    #[test]
    fn phase3_collection_exact_size_stops_at_bound() {
        let case_id = CaseId(13);
        let mut engine = EngineState::default();
        let collection_id = new_collection(&mut engine, case_id, 2, Some(2));

        assert!(collection_more(&mut engine, case_id, collection_id));
        assert!(collection_more(&mut engine, case_id, collection_id));
        assert!(!collection_more(&mut engine, case_id, collection_id));
    }

    #[test]
    fn phase3_collection_rejections_can_force_stop() {
        let case_id = CaseId(14);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF; 16],
                ..CaseBufferState::default()
            },
        );
        let collection_id = new_collection(&mut engine, case_id, 0, Some(10));

        for _ in 0..4 {
            assert!(collection_more(&mut engine, case_id, collection_id));
            collection_reject(&mut engine, case_id, collection_id, Some("reject for test"));
        }

        assert!(!collection_more(&mut engine, case_id, collection_id));
    }

    #[test]
    fn phase3_collection_rejections_under_min_panics_stop_test() {
        let case_id = CaseId(15);
        let mut engine = EngineState::default();
        let collection_id = new_collection(&mut engine, case_id, 2, Some(10));

        let panic = catch_unwind(AssertUnwindSafe(|| {
            for _ in 0..4 {
                assert!(collection_more(&mut engine, case_id, collection_id));
                collection_reject(
                    &mut engine,
                    case_id,
                    collection_id,
                    Some("reject under min"),
                );
            }
        }))
        .expect_err("expected too many rejections under min to stop test");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
    }

    #[test]
    fn phase3_generate_list_unique_rejects_duplicates() {
        let case_id = CaseId(16);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x00, 0x00, 0x01],
                ..CaseBufferState::default()
            },
        );

        let out = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "list",
                "min_size" => 2u64,
                "max_size" => 2u64,
                "unique" => true,
                "elements" => crate::cbor_utils::cbor_map! {
                    "type" => "integer",
                    "min_value" => 0u64,
                    "max_value" => 1u64
                }
            },
        );

        assert_eq!(
            out,
            Value::Array(vec![Value::from(0i64), Value::from(1i64)])
        );
    }

    #[test]
    fn phase3_generate_tuple_dict_and_typed_one_of() {
        let case_id = CaseId(17);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x00, 0x00, 0x00, 0x01, 0xFF],
                ..CaseBufferState::default()
            },
        );

        let tuple = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "tuple",
                "elements" => crate::cbor_utils::cbor_array![
                    crate::cbor_utils::cbor_map! {"type" => "integer", "min_value" => 0u64, "max_value" => 1u64},
                    crate::cbor_utils::cbor_map! {"type" => "boolean"}
                ]
            },
        );
        assert_eq!(
            tuple,
            Value::Array(vec![Value::from(0i64), Value::Bool(false)])
        );

        let dict = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "dict",
                "min_size" => 1u64,
                "max_size" => 1u64,
                "keys" => crate::cbor_utils::cbor_map! {"type" => "integer", "min_value" => 0u64, "max_value" => 1u64},
                "values" => crate::cbor_utils::cbor_map! {"type" => "boolean"}
            },
        );
        assert_eq!(
            dict,
            Value::Array(vec![Value::Array(vec![
                Value::from(0i64),
                Value::Bool(false)
            ])])
        );

        let one_of = generate(
            &mut engine,
            case_id,
            &crate::cbor_utils::cbor_map! {
                "type" => "one_of",
                "generators" => crate::cbor_utils::cbor_array![
                    crate::cbor_utils::cbor_map! {"type" => "constant", "value" => 1i64},
                    crate::cbor_utils::cbor_map! {"type" => "constant", "value" => 2i64}
                ]
            },
        );
        assert_eq!(one_of, Value::from(2i64));
    }

    #[test]
    fn phase4_span_records_nested_offsets_and_discard() {
        let case_id = CaseId(18);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0xFF, 0xAB],
                ..CaseBufferState::default()
            },
        );

        let outer = start_span(&mut engine, case_id, 7);
        let _ = draw_boolean_choice(&mut engine, case_id, 0.5, true);
        let inner = start_span(&mut engine, case_id, 8);
        let _ = draw_integer_choice(&mut engine, case_id, Some(0), Some(255), 0, true);
        stop_span(&mut engine, case_id, inner, true);
        stop_span(&mut engine, case_id, outer, false);

        let case = engine.cases.get(&case_id).expect("case should exist");
        assert!(case.open_spans.is_empty());
        assert_eq!(case.spans.len(), 2);

        let inner_span = &case.spans[0];
        assert_eq!(inner_span.label, 8);
        assert_eq!(inner_span.start_cursor, 1);
        assert_eq!(inner_span.end_cursor, 2);
        assert_eq!(inner_span.start_choice_index, 1);
        assert_eq!(inner_span.end_choice_index, 2);
        assert!(inner_span.discard);

        let outer_span = &case.spans[1];
        assert_eq!(outer_span.label, 7);
        assert_eq!(outer_span.start_cursor, 0);
        assert_eq!(outer_span.end_cursor, 2);
        assert_eq!(outer_span.start_choice_index, 0);
        assert_eq!(outer_span.end_choice_index, 2);
        assert!(!outer_span.discard);
    }

    #[test]
    fn phase4_span_ids_monotonic_per_case() {
        let case_id = CaseId(19);
        let mut engine = EngineState::default();

        let s1 = start_span(&mut engine, case_id, 1);
        let s2 = start_span(&mut engine, case_id, 2);
        assert_eq!(s1.0, 1);
        assert_eq!(s2.0, 2);
    }

    #[test]
    fn phase4_span_ids_restart_per_case() {
        let case_a = CaseId(20);
        let case_b = CaseId(21);
        let mut engine = EngineState::default();

        let a = start_span(&mut engine, case_a, 1);
        let b = start_span(&mut engine, case_b, 1);
        assert_eq!(a.0, 1);
        assert_eq!(b.0, 1);
    }

    #[test]
    fn phase5_pool_add_and_generate_without_consume() {
        let case_id = CaseId(22);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x02],
                ..CaseBufferState::default()
            },
        );
        let pool_id = new_pool(&mut engine, case_id);

        let a = pool_add(&mut engine, case_id, pool_id);
        let b = pool_add(&mut engine, case_id, pool_id);
        let c = pool_add(&mut engine, case_id, pool_id);
        assert_eq!((a, b, c), (0, 1, 2));

        let picked = pool_generate(&mut engine, case_id, pool_id, false);
        assert_eq!(picked, 2);

        let pool = engine.pools.get(&pool_id).expect("pool should exist");
        assert_eq!(pool.live_variable_ids, vec![0, 1, 2]);

        let case = engine.cases.get(&case_id).expect("case should exist");
        assert_eq!(case.cursor, 1);
        assert_eq!(case.typed.len(), 1);
        assert_eq!(case.typed.nodes[0].kind, ChoiceKind::Integer);
    }

    #[test]
    fn phase5_pool_generate_consume_removes_selected_id() {
        let case_id = CaseId(23);
        let mut engine = EngineState::default();
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0x01],
                ..CaseBufferState::default()
            },
        );
        let pool_id = new_pool(&mut engine, case_id);
        let _ = pool_add(&mut engine, case_id, pool_id);
        let _ = pool_add(&mut engine, case_id, pool_id);
        let _ = pool_add(&mut engine, case_id, pool_id);

        let picked = pool_generate(&mut engine, case_id, pool_id, true);
        assert_eq!(picked, 1);
        let pool = engine.pools.get(&pool_id).expect("pool should exist");
        assert_eq!(pool.live_variable_ids, vec![0, 2]);
    }

    #[test]
    fn phase5_pool_singleton_does_not_consume_bytes() {
        let case_id = CaseId(24);
        let mut engine = EngineState::default();
        engine.cases.insert(case_id, CaseBufferState::default());
        let pool_id = new_pool(&mut engine, case_id);
        let id = pool_add(&mut engine, case_id, pool_id);
        assert_eq!(id, 0);

        let picked = pool_generate(&mut engine, case_id, pool_id, false);
        assert_eq!(picked, 0);

        let case = engine.cases.get(&case_id).expect("case should exist");
        assert_eq!(case.cursor, 0);
        assert_eq!(case.typed.len(), 0);
    }

    #[test]
    fn phase5_pool_empty_panics_stop_test() {
        let case_id = CaseId(25);
        let mut engine = EngineState::default();
        let pool_id = new_pool(&mut engine, case_id);

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = pool_generate(&mut engine, case_id, pool_id, false);
        }))
        .expect_err("empty pool should stop test");

        assert_eq!(panic_message(panic), crate::test_case::STOP_TEST_STRING);
    }

    fn test_settings(seed: Option<u64>, derandomize: bool) -> EngineSettings {
        EngineSettings {
            test_cases: 2,
            seed,
            derandomize,
            database: super::DatabaseMode::Disabled,
            suppress_health_check: Vec::new(),
        }
    }

    fn test_metadata() -> TestMetadata {
        TestMetadata {
            function: "f".to_string(),
            module_path: "m".to_string(),
            file: "file.rs".to_string(),
            begin_line: 1,
            database_key: Some("db-key".to_string()),
        }
    }

    fn take_run(engine: &mut EngineState) -> RunState {
        engine.active_run.take().expect("active run should exist")
    }

    #[test]
    fn phase6_next_case_initializes_random_buffer() {
        let mut engine = EngineState::default();
        run_test(
            &mut engine,
            test_settings(Some(123), false),
            test_metadata(),
        );

        let case_id = next_case(&mut engine).expect("expected first case");
        assert_eq!(case_id.0, 1);
        let case = engine.cases.get(&case_id).expect("case should exist");
        assert_eq!(case.bytes.len(), 16 * 1024);
        assert_eq!(case.cursor, 0);
    }

    #[test]
    fn phase6_seeded_runs_are_deterministic() {
        let mut engine_a = EngineState::default();
        run_test(
            &mut engine_a,
            test_settings(Some(42), false),
            test_metadata(),
        );
        let case_a = next_case(&mut engine_a).expect("expected first case");
        let bytes_a = engine_a
            .cases
            .get(&case_a)
            .expect("case should exist")
            .bytes
            .clone();
        let _ = take_run(&mut engine_a);

        let mut engine_b = EngineState::default();
        run_test(
            &mut engine_b,
            test_settings(Some(42), false),
            test_metadata(),
        );
        let case_b = next_case(&mut engine_b).expect("expected first case");
        let bytes_b = engine_b
            .cases
            .get(&case_b)
            .expect("case should exist")
            .bytes
            .clone();

        assert_eq!(bytes_a, bytes_b);
    }

    #[test]
    fn phase6_derandomized_runs_are_deterministic_without_explicit_seed() {
        let mut engine_a = EngineState::default();
        run_test(&mut engine_a, test_settings(None, true), test_metadata());
        let case_a = next_case(&mut engine_a).expect("expected first case");
        let bytes_a = engine_a
            .cases
            .get(&case_a)
            .expect("case should exist")
            .bytes
            .clone();
        let _ = take_run(&mut engine_a);

        let mut engine_b = EngineState::default();
        run_test(&mut engine_b, test_settings(None, true), test_metadata());
        let case_b = next_case(&mut engine_b).expect("expected first case");
        let bytes_b = engine_b
            .cases
            .get(&case_b)
            .expect("case should exist")
            .bytes
            .clone();

        assert_eq!(bytes_a, bytes_b);
    }

    #[test]
    fn phase6_python_random_seed_one_matches_python_reference() {
        let mut rng = super::PythonRandom::from_u64_seed(1);
        let got = [
            rng.random().to_bits(),
            rng.random().to_bits(),
            rng.random().to_bits(),
            rng.random().to_bits(),
            rng.random().to_bits(),
        ];
        let expected = [
            0x3fc1_32d8_f91b_7584,
            0x3feb_1e2d_5b35_84f8,
            0x3fe8_70d7_7840_9f13,
            0x3fd0_530d_08f1_7f5c,
            0x3fdf_b535_5e16_737a,
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn phase6_python_random_getrandbits_32_matches_python_reference() {
        let mut rng = super::PythonRandom::from_u64_seed(1);
        let got = [
            rng.getrandbits(32),
            rng.getrandbits(32),
            rng.getrandbits(32),
            rng.getrandbits(32),
            rng.getrandbits(32),
        ];
        let expected = [
            577_090_037u64,
            2_444_712_010u64,
            3_639_700_191u64,
            3_445_702_192u64,
            3_280_387_012u64,
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn phase6_python_random_getrandbits_64_matches_python_reference() {
        let mut rng = super::PythonRandom::from_u64_seed(1);
        let got = [
            rng.getrandbits(64),
            rng.getrandbits(64),
            rng.getrandbits(64),
            rng.getrandbits(64),
            rng.getrandbits(64),
        ];
        let expected = [
            10_499_958_131_665_514_997u64,
            14_799_178_230_035_213_023u64,
            1_164_115_433_906_158_532u64,
            2_175_216_119_781_798_972u64,
            14_037_279_428_536_751_483u64,
        ];
        assert_eq!(got, expected);
    }

    fn probe_value_token(value: &ChoiceValue) -> String {
        match value {
            ChoiceValue::Boolean(v) => format!("b:{}", if *v { 1 } else { 0 }),
            ChoiceValue::Integer(v) => format!("i:{v}"),
            ChoiceValue::Float(v) => format!("f:{:016x}", v.to_bits()),
            ChoiceValue::Bytes(bytes) => format!(
                "y:{}",
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            ),
            ChoiceValue::String(s) => format!(
                "s:{}",
                serde_json::to_string(s).expect("string token JSON encoding should succeed")
            ),
        }
    }

    fn enable_probe_random_trace(engine: &mut EngineState) {
        if let Some(run) = engine.active_run.as_mut() {
            run.random_trace_enabled = true;
            run.random_trace_by_case.clear();
            run.pending_scheduler_random_events.clear();
        }
    }

    fn probe_scheduler_event_count(engine: &EngineState) -> usize {
        engine
            .active_run
            .as_ref()
            .map_or(0, |run| run.pending_scheduler_random_events.len())
    }

    fn draw_probe_tokens_with_events(
        constraints: &ChoiceConstraints,
        seed: u64,
        count: usize,
    ) -> (Vec<String>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(probe_value_token(&run_draw_choice_from_constraints(
                &mut engine,
                constraints,
            )));
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn draw_probe_tokens(constraints: &ChoiceConstraints, seed: u64, count: usize) -> Vec<String> {
        draw_probe_tokens_with_events(constraints, seed, count).0
    }

    fn draw_collection_sizes_probe(
        seed: u64,
        min_size: usize,
        max_size: usize,
        count: usize,
    ) -> Vec<usize> {
        draw_collection_sizes_probe_with_events(seed, min_size, max_size, count).0
    }

    fn draw_collection_sizes_probe_with_events(
        seed: u64,
        min_size: usize,
        max_size: usize,
        count: usize,
    ) -> (Vec<usize>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(super::run_draw_collection_size_hypothesis(
                &mut engine,
                min_size,
                max_size,
            ));
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn maybe_integer_constants_probe(seed: u64, count: usize) -> Vec<Option<String>> {
        maybe_integer_constants_probe_with_events(seed, count).0
    }

    fn maybe_integer_constants_probe_with_events(
        seed: u64,
        count: usize,
    ) -> (Vec<Option<String>>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(
                super::run_maybe_draw_integer_constant(&mut engine, None, None, 0.05)
                    .map(|v| format!("i:{v}")),
            );
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn maybe_string_constants_probe(seed: u64, count: usize) -> Vec<Option<String>> {
        maybe_string_constants_probe_with_events(seed, count).0
    }

    fn maybe_string_constants_probe_with_events(
        seed: u64,
        count: usize,
    ) -> (Vec<Option<String>>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let alphabet = ['a', 'b'];
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(
                super::run_maybe_draw_string_constant(&mut engine, 0, 8, Some(&alphabet), 0.05)
                    .map(|s| {
                        format!(
                            "s:{}",
                            serde_json::to_string(&s)
                                .expect("string constant token JSON encoding should succeed")
                        )
                    }),
            );
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn maybe_float_constants_probe(seed: u64, count: usize) -> Vec<Option<String>> {
        maybe_float_constants_probe_with_events(seed, count).0
    }

    fn maybe_float_constants_probe_with_events(
        seed: u64,
        count: usize,
    ) -> (Vec<Option<String>>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let smallest = f64::from_bits(1);
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(
                super::run_maybe_draw_float_constant(&mut engine, -1.0, 1.0, false, smallest, 0.15)
                    .map(|v| probe_value_token(&ChoiceValue::Float(v))),
            );
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn maybe_bytes_constants_probe(seed: u64, count: usize) -> Vec<Option<String>> {
        maybe_bytes_constants_probe_with_events(seed, count).0
    }

    fn maybe_bytes_constants_probe_with_events(
        seed: u64,
        count: usize,
    ) -> (Vec<Option<String>>, Vec<usize>) {
        let mut engine = probe_engine_with_seed(seed);
        enable_probe_random_trace(&mut engine);
        let mut values = Vec::with_capacity(count);
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let before = probe_scheduler_event_count(&engine);
            values.push(
                super::run_maybe_draw_bytes_constant(&mut engine, 0, 8, 0.05)
                    .map(|v| probe_value_token(&ChoiceValue::Bytes(v))),
            );
            let after = probe_scheduler_event_count(&engine);
            events.push(after.saturating_sub(before));
        }
        (values, events)
    }

    fn observed_string_prefix_probe(seed: u64) -> serde_json::Value {
        let scenario_seed = {
            let mut seed_rng = super::PythonRandom::from_u64_seed(seed);
            seed_rng.getrandbits(64)
        };
        let mut engine = probe_engine_with_seed(scenario_seed);
        for _ in 0..628usize {
            let _ = super::run_getrandbits_u128(&mut engine, 32);
        }
        let case_id = CaseId(9_999_001);
        engine.cases.insert(
            case_id,
            CaseBufferState {
                bytes: vec![0u8; super::DEFAULT_CASE_BUFFER_BYTES],
                prefix_choices: Some(vec![ChoiceValue::String("bbbbabab".to_string())]),
                ..CaseBufferState::default()
            },
        );
        {
            let run = engine
                .active_run
                .as_mut()
                .expect("expected active run for observed string prefix probe");
            run.current_case_id = Some(case_id);
            run.current_case_simplest_observed = false;
        }

        let alphabet = ['a', 'b'];
        let start_index = engine
            .active_run
            .as_ref()
            .expect("expected active run for observed string prefix probe")
            .py_random
            .index;
        let first = super::draw_string_choice(&mut engine, case_id, 0, 8, &alphabet, true);
        let second = super::draw_string_choice(&mut engine, case_id, 0, 8, &alphabet, true);
        let end_index = engine
            .active_run
            .as_ref()
            .expect("expected active run for observed string prefix probe")
            .py_random
            .index;
        let consumed_words_32 = if end_index >= start_index {
            end_index - start_index
        } else {
            (624usize - start_index) + end_index
        };
        json!({
            "scenario_seed": scenario_seed,
            "start_index": start_index,
            "end_index": end_index,
            "consumed_words_32": consumed_words_32,
            "draws": [
                format!("s:{}", serde_json::to_string(&first).expect("first draw should serialize")),
                format!("s:{}", serde_json::to_string(&second).expect("second draw should serialize")),
            ],
        })
    }

    fn make_probe_typed_nodes(
        first: ChoiceValue,
        second: ChoiceValue,
        second_constraints: ChoiceConstraints,
    ) -> Vec<super::ChoiceNode> {
        vec![
            super::ChoiceNode {
                kind: ChoiceKind::Boolean,
                value: first,
                constraints: ChoiceConstraints::Boolean { p: 0.5 },
                was_forced: false,
                index: 0,
                encoded_size: 1,
            },
            super::ChoiceNode {
                kind: ChoiceKind::Integer,
                value: second,
                constraints: second_constraints,
                was_forced: false,
                index: 1,
                encoded_size: 1,
            },
        ]
    }

    fn make_probe_string_pair_nodes(
        first: &str,
        second: &str,
        constraints: ChoiceConstraints,
    ) -> Vec<super::ChoiceNode> {
        vec![
            super::ChoiceNode {
                kind: ChoiceKind::String,
                value: ChoiceValue::String(first.to_string()),
                constraints: constraints.clone(),
                was_forced: false,
                index: 0,
                encoded_size: 1,
            },
            super::ChoiceNode {
                kind: ChoiceKind::String,
                value: ChoiceValue::String(second.to_string()),
                constraints,
                was_forced: false,
                index: 1,
                encoded_size: 1,
            },
        ]
    }

    fn followup_simulation_json(sim: super::datatree::FollowupSimulation) -> serde_json::Value {
        match sim {
            super::datatree::FollowupSimulation::Predictable => json!({"kind": "predictable"}),
            super::datatree::FollowupSimulation::Novel { prefix } => json!({
                "kind": "novel",
                "prefix": prefix.iter().map(probe_value_token).collect::<Vec<_>>(),
            }),
        }
    }

    fn probe_prefix_tokens(prefix: &[ChoiceValue]) -> Vec<String> {
        prefix.iter().map(probe_value_token).collect::<Vec<_>>()
    }

    fn rewrite_status_token(
        status: Option<super::datatree::RewriteStatus>,
    ) -> Option<&'static str> {
        match status {
            Some(super::datatree::RewriteStatus::Overrun) => Some("overrun"),
            Some(super::datatree::RewriteStatus::Valid) => Some("valid"),
            Some(super::datatree::RewriteStatus::Invalid) => Some("invalid"),
            Some(super::datatree::RewriteStatus::Interesting) => Some("interesting"),
            None => None,
        }
    }

    fn probe_engine_with_seed(seed: u64) -> EngineState {
        let mut engine = EngineState::default();
        run_test(
            &mut engine,
            EngineSettings {
                test_cases: 1,
                seed: Some(seed),
                derandomize: false,
                database: super::DatabaseMode::Disabled,
                suppress_health_check: Vec::new(),
            },
            test_metadata(),
        );
        engine
    }

    #[test]
    fn phase6_api_function_probe_dump() {
        if env::var_os("HEGEL_API_FUNCTION_PROBE").is_none() {
            return;
        }

        let seed = env::var("HEGEL_API_FUNCTION_PROBE_SEED")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(10_499_958_131_665_514_997u64);
        let string_constraints = ChoiceConstraints::String {
            min_size: 0,
            max_size: 8,
            alphabet: Some(vec!['a', 'b']),
        };
        let bool_constraints = ChoiceConstraints::Boolean { p: 0.5 };
        let float_constraints = ChoiceConstraints::Float {
            min_value: -1.0,
            max_value: 1.0,
            allow_nan: false,
            smallest_nonzero_magnitude: f64::from_bits(1),
        };
        let bytes_constraints = ChoiceConstraints::Bytes {
            min_size: 0,
            max_size: 8,
        };

        let choice_from_index_string = (0..16u128)
            .map(|index| {
                let value = generation_tree_choice_from_index(&string_constraints, index)
                    .expect("string choice_from_index should exist");
                json!({
                    "index": index,
                    "value_token": probe_value_token(&value),
                })
            })
            .collect::<Vec<_>>();
        let choice_from_index_bool = (0..2u128)
            .map(|index| {
                let value = generation_tree_choice_from_index(&bool_constraints, index)
                    .expect("bool choice_from_index should exist");
                json!({
                    "index": index,
                    "value_token": probe_value_token(&value),
                })
            })
            .collect::<Vec<_>>();

        let simple_bool_int_tree = {
            let mut tree = super::datatree::DataTree::default();
            let int_constraints = ChoiceConstraints::Integer {
                min_value: Some(0),
                max_value: Some(2),
                shrink_towards: 0,
            };
            super::datatree::record_case(
                &mut tree,
                &make_probe_typed_nodes(
                    ChoiceValue::Boolean(false),
                    ChoiceValue::Integer(0),
                    int_constraints.clone(),
                ),
                &super::CaseStatus::Valid,
            );
            super::datatree::record_case(
                &mut tree,
                &make_probe_typed_nodes(
                    ChoiceValue::Boolean(false),
                    ChoiceValue::Integer(1),
                    int_constraints.clone(),
                ),
                &super::CaseStatus::Invalid,
            );
            super::datatree::record_case(
                &mut tree,
                &make_probe_typed_nodes(
                    ChoiceValue::Boolean(true),
                    ChoiceValue::Integer(0),
                    int_constraints.clone(),
                ),
                &super::CaseStatus::Valid,
            );
            let mut engine = probe_engine_with_seed(seed);
            let generated = super::datatree::generate_novel_prefix(&mut engine, &tree)
                .unwrap_or_default()
                .iter()
                .map(probe_value_token)
                .collect::<Vec<_>>();
            let predictable = super::datatree::simulate_followup_prefix(
                &mut engine,
                &tree,
                &[ChoiceValue::Boolean(false), ChoiceValue::Integer(0)],
                2,
            );
            let novel = super::datatree::simulate_followup_prefix(
                &mut engine,
                &tree,
                &[ChoiceValue::Boolean(false), ChoiceValue::Integer(2)],
                2,
            );
            let followup_cases = vec![
                vec![],
                vec![ChoiceValue::Boolean(false)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(0)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(1)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(2)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(99)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(0)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(1)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(2)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(99)],
                vec![
                    ChoiceValue::Boolean(true),
                    ChoiceValue::Integer(0),
                    ChoiceValue::Integer(7),
                ],
                vec![
                    ChoiceValue::Boolean(false),
                    ChoiceValue::Integer(0),
                    ChoiceValue::Integer(7),
                ],
            ];
            let followup_case_results = followup_cases
                .iter()
                .map(|prefix| {
                    let sim = super::datatree::simulate_followup_prefix(
                        &mut engine,
                        &tree,
                        prefix,
                        prefix.len(),
                    );
                    json!({
                        "input": probe_prefix_tokens(prefix),
                        "simulation": followup_simulation_json(sim),
                    })
                })
                .collect::<Vec<_>>();
            let rewrite_case_results = followup_cases
                .iter()
                .map(|prefix| {
                    let (rewritten, status) = super::datatree::rewrite_prefix(&tree, prefix);
                    json!({
                        "input": probe_prefix_tokens(prefix),
                        "rewritten": probe_prefix_tokens(&rewritten),
                        "status": rewrite_status_token(status),
                    })
                })
                .collect::<Vec<_>>();
            let random_followup_cases = vec![
                (vec![], 1usize),
                (vec![], 2usize),
                (vec![ChoiceValue::Boolean(false)], 2usize),
                (vec![ChoiceValue::Boolean(true)], 2usize),
                (vec![ChoiceValue::Boolean(false)], 3usize),
                (vec![ChoiceValue::Boolean(true)], 3usize),
            ];
            let random_followup_results = random_followup_cases
                .iter()
                .map(|(prefix, max_choices)| {
                    let mut case_engine = probe_engine_with_seed(seed);
                    let sim = super::datatree::simulate_followup_prefix(
                        &mut case_engine,
                        &tree,
                        prefix,
                        *max_choices,
                    );
                    json!({
                        "input": probe_prefix_tokens(prefix),
                        "max_choices": max_choices,
                        "simulation": followup_simulation_json(sim),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "prefix": generated,
                "simulate_predictable": followup_simulation_json(predictable),
                "simulate_novel": followup_simulation_json(novel),
                "followup_cases": followup_case_results,
                "rewrite_cases": rewrite_case_results,
                "random_followup_cases": random_followup_results,
            })
        };
        let forced_bool_int_tree = {
            let mut tree = super::datatree::DataTree::default();
            let int_constraints = ChoiceConstraints::Integer {
                min_value: Some(0),
                max_value: Some(2),
                shrink_towards: 0,
            };
            let mut nodes_valid = make_probe_typed_nodes(
                ChoiceValue::Boolean(false),
                ChoiceValue::Integer(0),
                int_constraints.clone(),
            );
            nodes_valid[0].was_forced = true;
            super::datatree::record_case(&mut tree, &nodes_valid, &super::CaseStatus::Valid);
            let mut nodes_invalid = make_probe_typed_nodes(
                ChoiceValue::Boolean(false),
                ChoiceValue::Integer(1),
                int_constraints,
            );
            nodes_invalid[0].was_forced = true;
            super::datatree::record_case(&mut tree, &nodes_invalid, &super::CaseStatus::Invalid);

            let mut engine = probe_engine_with_seed(seed);
            let generated = super::datatree::generate_novel_prefix(&mut engine, &tree)
                .unwrap_or_default()
                .iter()
                .map(probe_value_token)
                .collect::<Vec<_>>();
            let rewrite_cases = vec![
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(0)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(0)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(1)],
                vec![ChoiceValue::Boolean(true), ChoiceValue::Integer(2)],
                vec![ChoiceValue::Boolean(false), ChoiceValue::Integer(2)],
                vec![ChoiceValue::Boolean(true)],
                vec![],
            ];
            let rewrite_case_results = rewrite_cases
                .iter()
                .map(|prefix| {
                    let (rewritten, status) = super::datatree::rewrite_prefix(&tree, prefix);
                    json!({
                        "input": probe_prefix_tokens(prefix),
                        "rewritten": probe_prefix_tokens(&rewritten),
                        "status": rewrite_status_token(status),
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "prefix": generated,
                "rewrite_cases": rewrite_case_results,
            })
        };
        let replay_paired_strings_case11 = {
            let scenario_seed = {
                let mut seed_rng = super::PythonRandom::from_u64_seed(seed);
                seed_rng.getrandbits(64)
            };
            let string_constraints = ChoiceConstraints::String {
                min_size: 0,
                max_size: 8,
                alphabet: Some(vec!['a', 'b']),
            };
            let replay_cases: [(&str, &str); 11] = [
                ("", ""),
                ("b", ""),
                ("b", "aaababab"),
                ("babaa", ""),
                ("babaa", "bbaa"),
                ("b", "bbaaa"),
                ("a", ""),
                ("a", "aa"),
                ("bbaaa", ""),
                ("bbaaa", "abbaabaa"),
                ("", "baaaaabb"),
            ];

            let mut tree_case11 = super::datatree::DataTree::default();
            for (left, right) in replay_cases {
                super::datatree::record_case(
                    &mut tree_case11,
                    &make_probe_string_pair_nodes(left, right, string_constraints.clone()),
                    &super::CaseStatus::Valid,
                );
            }
            let mut engine_case11 = probe_engine_with_seed(scenario_seed);
            for _ in 0..238usize {
                let _ = super::run_getrandbits_u128(&mut engine_case11, 32);
            }
            let generated_case11 =
                super::datatree::generate_novel_prefix(&mut engine_case11, &tree_case11)
                    .unwrap_or_default()
                    .iter()
                    .map(probe_value_token)
                    .collect::<Vec<_>>();

            let replay_cases10: [(&str, &str); 10] = [
                ("", ""),
                ("b", ""),
                ("b", "aaababab"),
                ("babaa", ""),
                ("babaa", "bbaa"),
                ("b", "bbaaa"),
                ("a", ""),
                ("a", "aa"),
                ("bbaaa", ""),
                ("bbaaa", "abbaabaa"),
            ];
            let mut tree_case10 = super::datatree::DataTree::default();
            for (left, right) in replay_cases10 {
                super::datatree::record_case(
                    &mut tree_case10,
                    &make_probe_string_pair_nodes(left, right, string_constraints.clone()),
                    &super::CaseStatus::Valid,
                );
            }
            let mut engine_case10 = probe_engine_with_seed(scenario_seed);
            for _ in 0..238usize {
                let _ = super::run_getrandbits_u128(&mut engine_case10, 32);
            }
            let before_idx = engine_case10
                .active_run
                .as_ref()
                .expect("expected active run")
                .py_random
                .index;
            let generated_case10 =
                super::datatree::generate_novel_prefix(&mut engine_case10, &tree_case10)
                    .unwrap_or_default()
                    .iter()
                    .map(probe_value_token)
                    .collect::<Vec<_>>();
            let after_idx = engine_case10
                .active_run
                .as_ref()
                .expect("expected active run")
                .py_random
                .index;
            let consumed_words = if after_idx >= before_idx {
                after_idx - before_idx
            } else {
                (624usize - before_idx) + after_idx
            };

            json!({
                "scenario_seed": scenario_seed,
                "case11_tree": {
                    "prefix": generated_case11,
                },
                "case10_tree_from_index_238": {
                    "prefix": generated_case10,
                    "start_index": before_idx,
                    "end_index": after_idx,
                    "consumed_words_32": consumed_words,
                },
            })
        };
        let mutation_api = {
            let string_constraints = ChoiceConstraints::String {
                min_size: 0,
                max_size: 8,
                alphabet: Some(vec!['a', 'b']),
            };
            let data = super::MutationCandidateData {
                choices: vec![
                    ChoiceValue::String("abaa".to_string()),
                    ChoiceValue::String("aaabbbba".to_string()),
                ],
                constraints: vec![string_constraints.clone(), string_constraints],
                spans: vec![
                    super::SpanState {
                        id: super::SpanId(1),
                        label: 7,
                        start_cursor: 0,
                        end_cursor: 1,
                        start_choice_index: 0,
                        end_choice_index: 1,
                        discard: false,
                    },
                    super::SpanState {
                        id: super::SpanId(2),
                        label: 7,
                        start_cursor: 1,
                        end_cursor: 2,
                        start_choice_index: 1,
                        end_choice_index: 2,
                        discard: false,
                    },
                ],
                status: super::CaseStatus::Valid,
                has_discard: false,
            };
            let groups = super::mutation_mutator_groups(&data)
                .into_iter()
                .map(|group| {
                    group
                        .into_iter()
                        .map(|(start, end)| json!([start, end]))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let mut engine = probe_engine_with_seed(seed);
            let run = engine
                .active_run
                .as_mut()
                .expect("expected active run for mutation probe");
            let before_idx = run.py_random.index;
            let attempt = super::mutation_attempt_from_state(run, &data)
                .unwrap_or_default()
                .iter()
                .map(probe_value_token)
                .collect::<Vec<_>>();
            let after_idx = run.py_random.index;
            let consumed_words = if after_idx >= before_idx {
                after_idx - before_idx
            } else {
                (624usize - before_idx) + after_idx
            };

            json!({
                "mutator_groups_simple": groups,
                "attempt_simple": {
                    "start_index": before_idx,
                    "end_index": after_idx,
                    "consumed_words_32": consumed_words,
                    "attempt": attempt,
                },
            })
        };

        let payload = json!({
            "seed": seed,
            "max_children": {
                "string_ab_0_8": generation_tree_max_children(ChoiceKind::String, &string_constraints).to_string(),
                "bool_p_0_5": generation_tree_max_children(ChoiceKind::Boolean, &bool_constraints).to_string(),
            },
            "choice_from_index": {
                "string_ab_0_8": choice_from_index_string,
                "bool_p_0_5": choice_from_index_bool,
            },
            "draw_sequence": {
                "string_ab_0_8": draw_probe_tokens(&string_constraints, seed, 20),
                "bool_p_0_5": draw_probe_tokens(&bool_constraints, seed, 20),
                "float_m1_1_no_nan": draw_probe_tokens(&float_constraints, seed, 20),
                "bytes_0_8": draw_probe_tokens(&bytes_constraints, seed, 20),
            },
            "observed_draws": {
                "string_prefix_case21_like": observed_string_prefix_probe(seed),
            },
            "random_consumption": {
                "draw_sequence_events": {
                    "string_ab_0_8": draw_probe_tokens_with_events(&string_constraints, seed, 20).1,
                    "bool_p_0_5": draw_probe_tokens_with_events(&bool_constraints, seed, 20).1,
                    "float_m1_1_no_nan": draw_probe_tokens_with_events(&float_constraints, seed, 20).1,
                    "bytes_0_8": draw_probe_tokens_with_events(&bytes_constraints, seed, 20).1,
                },
                "collection_sizes_events": {
                    "size_0_8": draw_collection_sizes_probe_with_events(seed, 0, 8, 20).1,
                    "size_4_8": draw_collection_sizes_probe_with_events(seed, 4, 8, 20).1,
                },
                "maybe_constants_events": {
                    "integer_unbounded": maybe_integer_constants_probe_with_events(seed, 40).1,
                    "string_ab_0_8": maybe_string_constants_probe_with_events(seed, 40).1,
                    "float_m1_1_no_nan": maybe_float_constants_probe_with_events(seed, 40).1,
                    "bytes_0_8": maybe_bytes_constants_probe_with_events(seed, 40).1,
                },
            },
            "collection_sizes": {
                "size_0_8": draw_collection_sizes_probe(seed, 0, 8, 20),
                "size_4_8": draw_collection_sizes_probe(seed, 4, 8, 20),
            },
            "maybe_constants": {
                "integer_unbounded": maybe_integer_constants_probe(seed, 40),
                "string_ab_0_8": maybe_string_constants_probe(seed, 40),
                "float_m1_1_no_nan": maybe_float_constants_probe(seed, 40),
                "bytes_0_8": maybe_bytes_constants_probe(seed, 40),
            },
            "radix_tree": {
                "generate_novel_prefix": {
                    "simple_bool_int_tree": simple_bool_int_tree,
                    "forced_bool_int_tree": forced_bool_int_tree,
                    "replay_paired_strings_case11": replay_paired_strings_case11,
                }
            },
            "mutations": mutation_api,
        });
        eprintln!(
            "api_function_probe => {}",
            serde_json::to_string(&payload).expect("probe payload serialization should succeed")
        );
    }
}
