use std::cmp::Ordering;
use std::collections::HashMap;

use super::{CaseStatus, ChoiceConstraints, ChoiceValue, SpanState, TypedChoiceNode};

pub mod passes;

#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub status: CaseStatus,
    pub buffer: Vec<u8>,
    pub typed_nodes: Vec<TypedChoiceNode>,
    pub spans: Vec<SpanState>,
}

#[derive(Debug, Clone)]
pub(crate) struct NativeShrinkState {
    pub(crate) best: Vec<u8>,
    pub(crate) best_typed_nodes: Vec<TypedChoiceNode>,
    pub(crate) best_spans: Vec<SpanState>,
    pub(crate) best_forced_choices: Option<Vec<ChoiceValue>>,
    pub(crate) remaining_attempts: usize,
    pub(crate) random_state: u64,
    pub(crate) calls: usize,
    pub(crate) calls_at_last_shrink: usize,
    pub(crate) max_stall: usize,
    pub(crate) enforce_cached_test_checks: bool,
}

impl NativeShrinkState {
    fn new(
        best: Vec<u8>,
        best_typed_nodes: Vec<TypedChoiceNode>,
        best_spans: Vec<SpanState>,
    ) -> Self {
        Self {
            best,
            best_typed_nodes,
            best_spans,
            best_forced_choices: None,
            remaining_attempts: 12_000usize,
            random_state: 0x9e37_79b9_7f4a_7c15,
            calls: 0,
            calls_at_last_shrink: 0,
            max_stall: 200,
            enforce_cached_test_checks: true,
        }
    }

    fn should_stop_for_stall(&self) -> bool {
        self.calls.saturating_sub(self.calls_at_last_shrink) >= self.max_stall
    }

    fn should_stop(&self) -> bool {
        self.remaining_attempts == 0 || self.should_stop_for_stall()
    }

    fn note_call(&mut self) {
        self.remaining_attempts = self.remaining_attempts.saturating_sub(1);
        self.calls = self.calls.saturating_add(1);
    }

    fn note_successful_shrink(&mut self) {
        let since_last = self.calls.saturating_sub(self.calls_at_last_shrink);
        self.max_stall = self.max_stall.max(since_last.saturating_mul(2));
        self.calls_at_last_shrink = self.calls;
    }

    fn into_output(self) -> (Vec<u8>, Vec<TypedChoiceNode>, Option<Vec<ChoiceValue>>) {
        (self.best, self.best_typed_nodes, self.best_forced_choices)
    }
}

pub fn compare_replay_complexity(
    left_nodes: &[TypedChoiceNode],
    right_nodes: &[TypedChoiceNode],
) -> Ordering {
    super::compare_typed_nodes_shortlex(left_nodes, right_nodes)
}

fn value_permitted_by_constraints(value: &ChoiceValue, constraints: &ChoiceConstraints) -> bool {
    match (value, constraints) {
        (ChoiceValue::Boolean(v), ChoiceConstraints::Boolean { p }) => {
            (*v && *p > 0.0) || (!*v && *p < 1.0)
        }
        (
            ChoiceValue::Integer(v),
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                ..
            },
        ) => min_value.is_none_or(|min| *v >= min) && max_value.is_none_or(|max| *v <= max),
        (
            ChoiceValue::Float(v),
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            },
        ) => {
            if v.is_nan() {
                *allow_nan
            } else {
                let zeroish = v.to_bits() == 0.0f64.to_bits() || v.to_bits() == (-0.0f64).to_bits();
                (*v >= *min_value && *v <= *max_value)
                    && (zeroish || v.abs() >= *smallest_nonzero_magnitude)
            }
        }
        (ChoiceValue::Bytes(v), ChoiceConstraints::Bytes { min_size, max_size }) => {
            *min_size <= v.len() && v.len() <= *max_size
        }
        (
            ChoiceValue::String(v),
            ChoiceConstraints::String {
                min_size, max_size, ..
            },
        ) => {
            let len = v.chars().count();
            *min_size <= len && len <= *max_size
        }
        _ => false,
    }
}

fn prefilter_forced_choices(
    state: &NativeShrinkState,
    forced_choices: &mut Vec<ChoiceValue>,
) -> bool {
    let current_len = state.best_typed_nodes.len();
    if forced_choices.len() > current_len {
        forced_choices.truncate(current_len);
    }

    if forced_choices
        .iter()
        .zip(state.best_typed_nodes.iter())
        .any(|(value, node)| !value_permitted_by_constraints(value, &node.constraints))
    {
        return false;
    }

    let candidate_nodes: Vec<TypedChoiceNode> = state
        .best_typed_nodes
        .iter()
        .zip(forced_choices.iter())
        .map(|(node, value)| TypedChoiceNode {
            kind: node.kind,
            value: value.clone(),
            constraints: node.constraints.clone(),
            was_forced: true,
            index: node.index,
            encoded_size: node.encoded_size,
        })
        .collect();

    compare_replay_complexity(&candidate_nodes, &state.best_typed_nodes) != Ordering::Greater
}

pub(crate) fn typed_values(nodes: &[TypedChoiceNode]) -> Vec<ChoiceValue> {
    nodes.iter().map(|node| node.value.clone()).collect()
}

pub(crate) fn forced_replacements_for_node(node: &TypedChoiceNode) -> Vec<ChoiceValue> {
    fn push_unique(out: &mut Vec<ChoiceValue>, candidate: ChoiceValue) {
        let exists = out.iter().any(|existing| match (existing, &candidate) {
            (ChoiceValue::Float(left), ChoiceValue::Float(right)) => {
                left.to_bits() == right.to_bits()
            }
            _ => existing == &candidate,
        });
        if !exists {
            out.push(candidate);
        }
    }

    fn clamped_shrink_towards(
        min_value: Option<i128>,
        max_value: Option<i128>,
        shrink_towards: i128,
    ) -> i128 {
        max_value
            .map_or(shrink_towards, |max| shrink_towards.min(max))
            .max(min_value.unwrap_or(i128::MIN))
    }

    match (&node.value, &node.constraints) {
        (ChoiceValue::Boolean(true), ChoiceConstraints::Boolean { .. }) => {
            vec![ChoiceValue::Boolean(false)]
        }
        (ChoiceValue::Boolean(false), ChoiceConstraints::Boolean { .. }) => Vec::new(),
        (
            ChoiceValue::Integer(current),
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                weights,
                shrink_towards,
            },
        ) => {
            let target = clamped_shrink_towards(*min_value, *max_value, *shrink_towards);
            let in_bounds = |v: i128| {
                min_value.map_or(true, |min| v >= min) && max_value.map_or(true, |max| v <= max)
            };
            let mut out = Vec::new();

            let push_integer = |out: &mut Vec<ChoiceValue>, candidate: i128| {
                if candidate != *current && in_bounds(candidate) {
                    push_unique(out, ChoiceValue::Integer(candidate));
                }
            };

            // Match Hypothesis-style integer shrinking order around shrink_towards:
            // 0 distance, then +1, -1, +2, -2, ...
            let max_distance = if *current >= target {
                (*current as u128).wrapping_sub(target as u128)
            } else {
                (target as u128).wrapping_sub(*current as u128)
            };
            push_integer(&mut out, target);
            let mut distance = 1u128;
            while distance <= max_distance {
                let plus = target.saturating_add(distance as i128);
                push_integer(&mut out, plus);
                let minus = target.saturating_sub(distance as i128);
                push_integer(&mut out, minus);
                if out.len() >= 1024 {
                    break;
                }
                distance += 1;
            }
            out
        }
        (
            ChoiceValue::Float(current),
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            },
        ) => {
            let mut out = Vec::new();
            let push_float = |out: &mut Vec<ChoiceValue>, candidate: f64| {
                if candidate.is_nan() && !allow_nan {
                    return;
                }
                if !candidate.is_nan() && !(candidate >= *min_value && candidate <= *max_value) {
                    return;
                }
                if !candidate.is_nan()
                    && candidate.to_bits() != 0.0f64.to_bits()
                    && candidate.to_bits() != (-0.0f64).to_bits()
                    && candidate.abs() < *smallest_nonzero_magnitude
                {
                    return;
                }
                if candidate.to_bits() != current.to_bits() {
                    push_unique(out, ChoiceValue::Float(candidate));
                }
            };

            push_float(&mut out, 0.0);
            if *smallest_nonzero_magnitude > 0.0 && smallest_nonzero_magnitude.is_finite() {
                if *current < 0.0 {
                    push_float(&mut out, -*smallest_nonzero_magnitude);
                } else {
                    push_float(&mut out, *smallest_nonzero_magnitude);
                }
            }
            if current.is_finite() {
                push_float(&mut out, *current / 2.0);
            }
            out
        }
        (ChoiceValue::Bytes(current), ChoiceConstraints::Bytes { min_size, .. }) => {
            let mut out = Vec::new();
            let push_bytes = |out: &mut Vec<ChoiceValue>, candidate: Vec<u8>| {
                if candidate != *current {
                    push_unique(out, ChoiceValue::Bytes(candidate));
                }
            };

            push_bytes(&mut out, vec![0; *min_size]);
            if current.len() > *min_size {
                push_bytes(&mut out, current[..*min_size].to_vec());
            }
            if !current.is_empty() {
                let mut zeroed = current.clone();
                zeroed.fill(0);
                push_bytes(&mut out, zeroed);

                if let Some(max_byte) = current.iter().copied().max()
                    && max_byte > 0
                {
                    let lowered = max_byte - 1;
                    let mut replaced = current.clone();
                    for byte in &mut replaced {
                        if *byte == max_byte {
                            *byte = lowered;
                        }
                    }
                    push_bytes(&mut out, replaced);
                }
            }
            out
        }
        (ChoiceValue::String(current), ChoiceConstraints::String { min_size, .. }) => {
            let mut out = Vec::new();
            let push_string = |out: &mut Vec<ChoiceValue>, candidate: String| {
                if candidate != *current {
                    push_unique(out, ChoiceValue::String(candidate));
                }
            };

            if *min_size == 0 {
                push_string(&mut out, String::new());
            }

            let chars: Vec<char> = current.chars().collect();
            if chars.len() > *min_size {
                let truncated: String = chars.iter().take(*min_size).collect();
                push_string(&mut out, truncated);
            }

            if !chars.is_empty() {
                if let Some(min_char) = chars.iter().copied().min() {
                    let target_len = (*min_size).max(1);
                    let lowered: String = std::iter::repeat(min_char).take(target_len).collect();
                    push_string(&mut out, lowered);
                }

                let mut seen = std::collections::BTreeSet::new();
                for ch in chars.iter().copied() {
                    if !seen.insert(ch) {
                        continue;
                    }
                    let cp = ch as u32;
                    if cp == 0 {
                        continue;
                    }
                    if let Some(lowered_ch) = char::from_u32(cp - 1) {
                        let lowered: String = chars
                            .iter()
                            .map(|c| if *c == ch { lowered_ch } else { *c })
                            .collect();
                        push_string(&mut out, lowered);
                    }
                    if out.len() >= 16 {
                        break;
                    }
                }
            }
            out
        }
        _ => Vec::new(),
    }
}

pub(crate) fn replay_if_better_with_forced_choices<F>(
    state: &mut NativeShrinkState,
    replay: &mut F,
    mut forced_choices: Vec<ChoiceValue>,
) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    if state.should_stop() {
        return false;
    }
    if state.enforce_cached_test_checks && !prefilter_forced_choices(state, &mut forced_choices) {
        return false;
    }

    state.note_call();
    let replay_result = replay(state.best.as_slice(), Some(forced_choices.clone()));
    if matches!(replay_result.status, CaseStatus::Interesting { .. })
        && compare_replay_complexity(&replay_result.typed_nodes, &state.best_typed_nodes)
            == Ordering::Less
    {
        state.best = replay_result.buffer;
        state.best_typed_nodes = replay_result.typed_nodes;
        state.best_spans = replay_result.spans;
        state.best_forced_choices = Some(forced_choices);
        state.note_successful_shrink();
        return true;
    }
    false
}

fn remove_discarded<F>(state: &mut NativeShrinkState, replay: &mut F) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    loop {
        let mut discarded = Vec::<(usize, usize)>::new();
        for span in &state.best_spans {
            let start = span.start_choice_index.min(state.best_typed_nodes.len());
            let end = span.end_choice_index.min(state.best_typed_nodes.len());
            if !span.discard || start >= end {
                continue;
            }
            if discarded
                .last()
                .is_none_or(|(_, last_end)| start >= *last_end)
            {
                discarded.push((start, end));
            }
        }

        if discarded.is_empty() {
            return true;
        }

        let mut forced = typed_values(&state.best_typed_nodes);
        for (u, v) in discarded.into_iter().rev() {
            forced.drain(u..v);
        }
        if !replay_if_better_with_forced_choices(state, replay, forced) {
            return false;
        }
    }
}

pub fn shrink_interesting_buffer_with_passes<F>(
    best: Vec<u8>,
    best_typed_nodes: Vec<TypedChoiceNode>,
    best_spans: Vec<SpanState>,
    replay: &mut F,
) -> (Vec<u8>, Vec<TypedChoiceNode>, Option<Vec<ChoiceValue>>)
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    if best.is_empty() {
        return (best, best_typed_nodes, None);
    }

    #[derive(Debug, Clone)]
    enum ScheduledPassKind {
        TryTrivialSpans(passes::TryTrivialSpansPass),
        NodeProgram5(passes::NodeProgramPass),
        NodeProgram4(passes::NodeProgramPass),
        NodeProgram3(passes::NodeProgramPass),
        NodeProgram2(passes::NodeProgramPass),
        NodeProgram1(passes::NodeProgramPass),
        RedistributeNumericPairs(passes::RedistributeNumericPairsPass),
        MinimizeIndividualChoices(passes::MinimizeIndividualChoicesPass),
        LowerIntegersTogether(passes::LowerIntegersTogetherPass),
    }

    #[derive(Debug, Clone)]
    struct ScheduledPass {
        name: &'static str,
        kind: ScheduledPassKind,
    }

    impl ScheduledPass {
        fn reset(&mut self) {
            match &mut self.kind {
                ScheduledPassKind::TryTrivialSpans(pass) => pass.reset(),
                ScheduledPassKind::NodeProgram5(pass)
                | ScheduledPassKind::NodeProgram4(pass)
                | ScheduledPassKind::NodeProgram3(pass)
                | ScheduledPassKind::NodeProgram2(pass)
                | ScheduledPassKind::NodeProgram1(pass) => pass.reset(),
                ScheduledPassKind::RedistributeNumericPairs(pass) => pass.reset(),
                ScheduledPassKind::MinimizeIndividualChoices(pass) => pass.reset(),
                ScheduledPassKind::LowerIntegersTogether(pass) => pass.reset(),
            }
        }

        fn step<F>(
            &mut self,
            state: &mut NativeShrinkState,
            replay: &mut F,
            random_order: bool,
        ) -> passes::PassStep
        where
            F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
        {
            match &mut self.kind {
                ScheduledPassKind::TryTrivialSpans(pass) => pass.step(state, replay, random_order),
                ScheduledPassKind::NodeProgram5(pass)
                | ScheduledPassKind::NodeProgram4(pass)
                | ScheduledPassKind::NodeProgram3(pass)
                | ScheduledPassKind::NodeProgram2(pass)
                | ScheduledPassKind::NodeProgram1(pass) => pass.step(state, replay, random_order),
                ScheduledPassKind::RedistributeNumericPairs(pass) => {
                    pass.step(state, replay, random_order)
                }
                ScheduledPassKind::MinimizeIndividualChoices(pass) => {
                    pass.step(state, replay, random_order)
                }
                ScheduledPassKind::LowerIntegersTogether(pass) => {
                    pass.step(state, replay, random_order)
                }
            }
        }
    }

    let mut state = NativeShrinkState::new(best, best_typed_nodes, best_spans);
    let all_passes: [ScheduledPass; 9] = [
        ScheduledPass {
            name: "try_trivial_spans",
            kind: ScheduledPassKind::TryTrivialSpans(passes::TryTrivialSpansPass::default()),
        },
        ScheduledPass {
            name: "node_program_XXXXX",
            kind: ScheduledPassKind::NodeProgram5(passes::NodeProgramPass::new(5)),
        },
        ScheduledPass {
            name: "node_program_XXXX",
            kind: ScheduledPassKind::NodeProgram4(passes::NodeProgramPass::new(4)),
        },
        ScheduledPass {
            name: "node_program_XXX",
            kind: ScheduledPassKind::NodeProgram3(passes::NodeProgramPass::new(3)),
        },
        ScheduledPass {
            name: "node_program_XX",
            kind: ScheduledPassKind::NodeProgram2(passes::NodeProgramPass::new(2)),
        },
        ScheduledPass {
            name: "node_program_X",
            kind: ScheduledPassKind::NodeProgram1(passes::NodeProgramPass::new(1)),
        },
        ScheduledPass {
            name: "redistribute_numeric_pairs",
            kind: ScheduledPassKind::RedistributeNumericPairs(
                passes::RedistributeNumericPairsPass::default(),
            ),
        },
        ScheduledPass {
            name: "minimize_individual_choices",
            kind: ScheduledPassKind::MinimizeIndividualChoices(
                passes::MinimizeIndividualChoicesPass::default(),
            ),
        },
        ScheduledPass {
            name: "lower_integers_together",
            kind: ScheduledPassKind::LowerIntegersTogether(
                passes::LowerIntegersTogetherPass::default(),
            ),
        },
    ];
    let pass_filter = selected_pass_filter_from_env();
    let mut shrink_passes: Vec<ScheduledPass> = all_passes
        .into_iter()
        .filter(|pass| match pass_filter.as_ref() {
            None => true,
            Some(wanted) => wanted
                .iter()
                .any(|token| pass_name_matches(token, pass.name)),
        })
        .collect();

    if shrink_passes.is_empty() {
        return state.into_output();
    }

    let max_failures = 20usize;
    let mut any_ran = true;
    while any_ran && !state.should_stop() {
        any_ran = false;
        let mut pass_reordering: HashMap<&'static str, i8> = HashMap::new();
        let mut can_discard = remove_discarded(&mut state, replay);
        let calls_at_loop_start = state.calls;
        let mut max_calls_per_failing_step = 1usize;

        for pass_ix in 0..shrink_passes.len() {
            if state.should_stop() {
                break;
            }
            if can_discard {
                can_discard = remove_discarded(&mut state, replay);
            }

            let pass_name = shrink_passes[pass_ix].name;
            let before_nodes = state.best_typed_nodes.clone();
            let before_len = before_nodes.len();
            let mut failures = 0usize;
            let mut pass_progress = false;

            while failures < max_failures && !state.should_stop() {
                state.max_stall = state.max_stall.max(
                    2usize
                        .saturating_mul(max_calls_per_failing_step)
                        .saturating_add(state.calls.saturating_sub(calls_at_loop_start)),
                );
                if state.should_stop() {
                    break;
                }
                let random_order = failures >= max_failures / 2;
                let calls_before_step = state.calls;
                let step = shrink_passes[pass_ix].step(&mut state, replay, random_order);
                if !step.attempted {
                    break;
                }
                any_ran = true;
                let calls_this_step = state.calls.saturating_sub(calls_before_step);
                let made_call = calls_this_step > 0;
                if step.changed {
                    pass_progress = true;
                    failures = 0;
                    for pass in &mut shrink_passes {
                        pass.reset();
                    }
                } else if made_call {
                    max_calls_per_failing_step = max_calls_per_failing_step.max(calls_this_step);
                    failures += 1;
                }
            }

            let score = if !pass_progress {
                1
            } else if state.best_typed_nodes.len() < before_len {
                -1
            } else {
                0
            };
            pass_reordering.insert(pass_name, score);
        }

        shrink_passes.sort_by_key(|pass| pass_reordering.get(pass.name).copied().unwrap_or(0));
    }

    state.into_output()
}

fn selected_pass_filter_from_env() -> Option<Vec<String>> {
    let raw = std::env::var("HEGEL_NATIVE_SHRINK_PASSES").ok()?;
    let tokens: Vec<String> = raw
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if tokens.is_empty() {
        None
    } else {
        Some(tokens)
    }
}

fn pass_name_matches(token: &str, pass_name: &str) -> bool {
    token == pass_name || token == format!("pass_{pass_name}")
}
