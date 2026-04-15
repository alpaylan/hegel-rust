use super::{
    NativeShrinkState, ReplayResult, forced_replacements_for_node,
    replay_if_better_with_forced_choices, typed_values,
};
use crate::native_engine::ChoiceNode;
use crate::native_engine::{ChoiceConstraints, ChoiceValue, SpanState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PassStep {
    pub(crate) attempted: bool,
    pub(crate) changed: bool,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn pick_attempt_index(
    state: &mut NativeShrinkState,
    len: usize,
    cursor: usize,
    random_order: bool,
) -> usize {
    debug_assert!(cursor < len);
    if !random_order {
        return cursor;
    }

    // Match Hypothesis behavior more closely than simple reversal: during stalls
    // choose a randomized traversal order across available branches.
    let salt = splitmix64(state.random_state);
    state.random_state = salt;
    let mut order: Vec<usize> = (0..len).collect();
    order.sort_by_key(|i| splitmix64(salt ^ (*i as u64)));
    order[cursor]
}

#[derive(Debug, Clone)]
enum MinimizeAttempt {
    Replace {
        idx: usize,
        replacement: ChoiceValue,
    },
    DropOne {
        idx: usize,
        delete_idx: usize,
    },
    Truncate {
        idx: usize,
        delete_idx: usize,
    },
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MinimizeIndividualChoicesPass {
    cursor: usize,
}

impl MinimizeIndividualChoicesPass {
    pub(crate) fn reset(&mut self) {
        self.cursor = 0;
    }

    fn attempts_for_state(state: &NativeShrinkState) -> Vec<MinimizeAttempt> {
        let typed_limit = state.best_typed_nodes.len().min(256);
        let mut out = Vec::new();

        for idx in 0..typed_limit {
            let replacements = forced_replacements_for_node(&state.best_typed_nodes[idx]);
            for replacement in replacements {
                out.push(MinimizeAttempt::Replace { idx, replacement });
            }

            let ChoiceValue::Integer(current) = state.best_typed_nodes[idx].value else {
                continue;
            };
            let ChoiceConstraints::Integer {
                min_value,
                max_value,
                ..
            } = &state.best_typed_nodes[idx].constraints
            else {
                continue;
            };
            let lowered = current.saturating_sub(1);
            let in_bounds = min_value.map_or(true, |min| lowered >= min)
                && max_value.map_or(true, |max| lowered <= max);
            if !in_bounds {
                continue;
            }
            let len = state.best_typed_nodes.len();
            for delete_idx in ((idx + 1)..len).rev() {
                out.push(MinimizeAttempt::DropOne { idx, delete_idx });
                out.push(MinimizeAttempt::Truncate { idx, delete_idx });
            }
        }
        out
    }

    pub(crate) fn step<F>(
        &mut self,
        state: &mut NativeShrinkState,
        replay: &mut F,
        random_order: bool,
    ) -> PassStep
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if state.remaining_attempts == 0 {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }

        let attempts = Self::attempts_for_state(state);
        if attempts.is_empty() || self.cursor >= attempts.len() {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }

        let pick = pick_attempt_index(state, attempts.len(), self.cursor, random_order);
        self.cursor += 1;
        let changed = match attempts[pick].clone() {
            MinimizeAttempt::Replace { idx, replacement } => {
                let mut forced_choices = typed_values(&state.best_typed_nodes);
                if idx >= forced_choices.len() {
                    false
                } else {
                    forced_choices[idx] = replacement;
                    replay_if_better_with_forced_choices(state, replay, forced_choices)
                }
            }
            MinimizeAttempt::DropOne { idx, delete_idx } => {
                if idx >= state.best_typed_nodes.len() || delete_idx >= state.best_typed_nodes.len()
                {
                    false
                } else {
                    let ChoiceValue::Integer(current) = state.best_typed_nodes[idx].value else {
                        return PassStep {
                            attempted: true,
                            changed: false,
                        };
                    };
                    let mut lowered_forced = typed_values(&state.best_typed_nodes);
                    lowered_forced[idx] = ChoiceValue::Integer(current.saturating_sub(1));
                    let mut drop_one = lowered_forced;
                    if delete_idx >= drop_one.len() {
                        false
                    } else {
                        drop_one.remove(delete_idx);
                        replay_if_better_with_forced_choices(state, replay, drop_one)
                    }
                }
            }
            MinimizeAttempt::Truncate { idx, delete_idx } => {
                if idx >= state.best_typed_nodes.len() || delete_idx > state.best_typed_nodes.len()
                {
                    false
                } else {
                    let ChoiceValue::Integer(current) = state.best_typed_nodes[idx].value else {
                        return PassStep {
                            attempted: true,
                            changed: false,
                        };
                    };
                    let mut lowered_forced = typed_values(&state.best_typed_nodes);
                    lowered_forced[idx] = ChoiceValue::Integer(current.saturating_sub(1));
                    if delete_idx > lowered_forced.len() {
                        false
                    } else {
                        let truncate_suffix = lowered_forced[..delete_idx].to_vec();
                        replay_if_better_with_forced_choices(state, replay, truncate_suffix)
                    }
                }
            }
        };

        PassStep {
            attempted: true,
            changed,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RedistributePairAttempt {
    left_idx: usize,
    right_idx: usize,
}

#[derive(Debug, Clone, Copy)]
struct LowerIntegersPairAttempt {
    left_idx: usize,
    right_idx: usize,
}

#[derive(Debug, Clone, Copy)]
struct TryTrivialSpanAttempt {
    span_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct NodeProgramAttempt {
    start: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct RedistributeNumericPairsPass {
    cursor: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct LowerIntegersTogetherPass {
    cursor: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TryTrivialSpansPass {
    cursor: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeProgramPass {
    program_len: usize,
    cursor: usize,
}

impl Default for NodeProgramPass {
    fn default() -> Self {
        Self {
            program_len: 1,
            cursor: 0,
        }
    }
}

fn find_integer_max_true(mut f: impl FnMut(i128) -> bool) -> i128 {
    for i in 1..=4 {
        if !f(i) {
            return i - 1;
        }
    }

    let mut lo: i128 = 4;
    let mut hi: i128 = 5;
    while f(hi) {
        lo = hi;
        let next = hi.saturating_mul(2);
        if next == hi {
            break;
        }
        hi = next;
    }

    while lo + 1 < hi {
        let mid = lo + ((hi - lo) / 2);
        if f(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

fn is_numeric_nontrivial(node: &crate::native_engine::ChoiceNode) -> bool {
    match (&node.value, &node.constraints) {
        (
            ChoiceValue::Integer(value),
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                shrink_towards,
            },
        ) => {
            let target = max_value
                .map_or(*shrink_towards, |max| (*shrink_towards).min(max))
                .max(min_value.unwrap_or(i128::MIN));
            *value != target
        }
        (ChoiceValue::Float(value), ChoiceConstraints::Float { .. }) => {
            value.to_bits() != 0.0f64.to_bits() && value.to_bits() != (-0.0f64).to_bits()
        }
        _ => false,
    }
}

const MAX_PRECISE_INTEGER: f64 = 9_007_199_254_740_992.0;

fn is_redistribute_numeric_candidate(node: &ChoiceNode) -> bool {
    match (&node.value, &node.constraints) {
        (ChoiceValue::Integer(_), ChoiceConstraints::Integer { .. }) => true,
        (ChoiceValue::Float(value), ChoiceConstraints::Float { .. }) => {
            !value.is_nan() && value.abs() < MAX_PRECISE_INTEGER
        }
        _ => false,
    }
}

impl RedistributeNumericPairsPass {
    pub(crate) fn reset(&mut self) {
        self.cursor = 0;
    }

    fn attempts_for_state(state: &NativeShrinkState) -> Vec<RedistributePairAttempt> {
        let numeric_indexes: Vec<usize> = state
            .best_typed_nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if is_redistribute_numeric_candidate(node) {
                    Some(idx)
                } else {
                    None
                }
            })
            .take(256)
            .collect();

        let mut out = Vec::new();
        for &left_idx in numeric_indexes.iter().rev() {
            let left_node = &state.best_typed_nodes[left_idx];
            if !is_numeric_nontrivial(left_node) {
                continue;
            }

            for &right_idx in numeric_indexes.iter().rev() {
                if right_idx <= left_idx || right_idx > left_idx + 4 {
                    continue;
                }
                if !is_redistribute_numeric_candidate(&state.best_typed_nodes[right_idx]) {
                    continue;
                }
                if state.best_typed_nodes[right_idx].was_forced {
                    continue;
                }
                out.push(RedistributePairAttempt {
                    left_idx,
                    right_idx,
                });
            }
        }
        out
    }

    fn try_pair<F>(
        state: &mut NativeShrinkState,
        replay: &mut F,
        pair: RedistributePairAttempt,
    ) -> bool
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        let left_idx = pair.left_idx;
        let right_idx = pair.right_idx;
        if left_idx >= state.best_typed_nodes.len() || right_idx >= state.best_typed_nodes.len() {
            return false;
        }
        let left_snapshot = state.best_typed_nodes[left_idx].clone();
        let right_snapshot = state.best_typed_nodes[right_idx].clone();

        let mut boost = |k: i128| -> bool {
            if state.remaining_attempts == 0 {
                return false;
            }

            let (left_value, right_value) = match (&left_snapshot.value, &right_snapshot.value) {
                (ChoiceValue::Integer(m), ChoiceValue::Integer(n)) => {
                    let ChoiceConstraints::Integer { shrink_towards, .. } =
                        &left_snapshot.constraints
                    else {
                        return false;
                    };
                    let ChoiceConstraints::Integer { .. } = &right_snapshot.constraints else {
                        return false;
                    };

                    let max_k = if *m >= *shrink_towards {
                        (*m as u128).wrapping_sub(*shrink_towards as u128)
                    } else {
                        (*shrink_towards as u128).wrapping_sub(*m as u128)
                    } as i128;
                    if k > max_k {
                        return false;
                    }
                    let signed_k = if *m < *shrink_towards { -k } else { k };
                    let Some(v1) = m.checked_sub(signed_k) else {
                        return false;
                    };
                    let Some(v2) = n.checked_add(signed_k) else {
                        return false;
                    };
                    (ChoiceValue::Integer(v1), ChoiceValue::Integer(v2))
                }
                (ChoiceValue::Float(m), ChoiceValue::Float(n)) => {
                    let ChoiceConstraints::Float { .. } = &left_snapshot.constraints else {
                        return false;
                    };
                    let ChoiceConstraints::Float { .. } = &right_snapshot.constraints else {
                        return false;
                    };
                    if !m.is_finite() || !n.is_finite() {
                        return false;
                    }
                    let max_k = m.abs().floor();
                    if (k as f64) > max_k {
                        return false;
                    }
                    let signed_k = if *m < 0.0 { -(k as f64) } else { k as f64 };
                    let v1 = m - signed_k;
                    let v2 = n + signed_k;
                    if !v1.is_finite() || !v2.is_finite() {
                        return false;
                    }
                    if v2.abs() >= MAX_PRECISE_INTEGER {
                        return false;
                    }
                    (ChoiceValue::Float(v1), ChoiceValue::Float(v2))
                }
                _ => return false,
            };

            let mut forced = typed_values(&state.best_typed_nodes);
            if left_idx >= forced.len() || right_idx >= forced.len() {
                return false;
            }
            forced[left_idx] = left_value;
            forced[right_idx] = right_value;
            replay_if_better_with_forced_choices(state, replay, forced)
        };

        let found = find_integer_max_true(&mut boost);
        found > 0
    }

    pub(crate) fn step<F>(
        &mut self,
        state: &mut NativeShrinkState,
        replay: &mut F,
        random_order: bool,
    ) -> PassStep
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if state.remaining_attempts == 0 {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let attempts = Self::attempts_for_state(state);
        if attempts.is_empty() || self.cursor >= attempts.len() {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let pick = pick_attempt_index(state, attempts.len(), self.cursor, random_order);
        self.cursor += 1;
        let changed = Self::try_pair(state, replay, attempts[pick]);
        PassStep {
            attempted: true,
            changed,
        }
    }
}

fn is_integer_nontrivial(node: &ChoiceNode) -> bool {
    match (&node.value, &node.constraints) {
        (
            ChoiceValue::Integer(value),
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                shrink_towards,
            },
        ) => {
            let target = max_value
                .map_or(*shrink_towards, |max| (*shrink_towards).min(max))
                .max(min_value.unwrap_or(i128::MIN));
            *value != target
        }
        _ => false,
    }
}

impl LowerIntegersTogetherPass {
    pub(crate) fn reset(&mut self) {
        self.cursor = 0;
    }

    fn selection_order(prefix: &[usize], depth: usize, n: usize) -> Vec<usize> {
        if n == 0 {
            return Vec::new();
        }
        if depth < prefix.len() {
            let i = prefix[depth].min(n - 1);
            let mut out = Vec::with_capacity(n);
            for v in (0..=i).rev() {
                out.push(v);
            }
            for v in ((i + 1)..n).rev() {
                out.push(v);
            }
            out
        } else {
            (0..n).rev().collect()
        }
    }

    fn attempts_for_state(state: &NativeShrinkState) -> Vec<LowerIntegersPairAttempt> {
        // Port of Hypothesis choicetree traversal for the two choose() calls in
        // lower_integers_together, so pair ordering matches chooser behavior.
        let nodes = &state.best_typed_nodes;
        let n1 = nodes.len().min(256);
        let mut root_dead = vec![false; n1];
        let mut child_dead: std::collections::HashMap<usize, Vec<bool>> =
            std::collections::HashMap::new();
        let mut last_prefix: Vec<usize> = Vec::new();
        let mut out = Vec::new();

        while root_dead.iter().any(|dead| !dead) {
            let mut choices: Vec<usize> = Vec::new();
            let mut chosen_left: Option<usize> = None;

            for child_i in Self::selection_order(&last_prefix, 0, n1) {
                if root_dead[child_i] {
                    continue;
                }
                let left_idx = child_i;
                let Some(node1) = nodes.get(left_idx) else {
                    root_dead[child_i] = true;
                    continue;
                };
                if !matches!(node1.value, ChoiceValue::Integer(_)) || !is_integer_nontrivial(node1)
                {
                    root_dead[child_i] = true;
                    continue;
                }
                choices.push(child_i);
                chosen_left = Some(left_idx);
                break;
            }

            let Some(left_idx) = chosen_left else {
                break;
            };

            let right_candidates: Vec<usize> =
                ((left_idx + 1)..nodes.len().min(left_idx + 3 + 1)).collect();
            let n2 = right_candidates.len();
            let right_dead = child_dead
                .entry(left_idx)
                .or_insert_with(|| vec![false; n2]);

            let mut chosen_right: Option<(usize, usize)> = None;
            for child_i in Self::selection_order(&last_prefix, 1, n2) {
                if right_dead.get(child_i).copied().unwrap_or(true) {
                    continue;
                }
                let Some(&right_idx) = right_candidates.get(child_i) else {
                    if let Some(slot) = right_dead.get_mut(child_i) {
                        *slot = true;
                    }
                    continue;
                };
                let right_ok = matches!(
                    nodes.get(right_idx).map(|n| &n.value),
                    Some(ChoiceValue::Integer(_))
                ) && nodes.get(right_idx).map(|n| n.was_forced).unwrap_or(true)
                    == false;
                if !right_ok {
                    if let Some(slot) = right_dead.get_mut(child_i) {
                        *slot = true;
                    }
                    continue;
                }
                choices.push(child_i);
                chosen_right = Some((right_idx, child_i));
                break;
            }

            let result_prefix = choices.clone();
            match chosen_right {
                Some((right_idx, child_i)) => {
                    if let Some(slot) = right_dead.get_mut(child_i) {
                        *slot = true;
                    }
                    if right_dead.iter().all(|dead| *dead) {
                        root_dead[left_idx] = true;
                    }
                    out.push(LowerIntegersPairAttempt {
                        left_idx,
                        right_idx,
                    });
                }
                None => {
                    root_dead[left_idx] = true;
                }
            }

            last_prefix = result_prefix;
        }

        out
    }

    fn try_pair<F>(
        state: &mut NativeShrinkState,
        replay: &mut F,
        pair: LowerIntegersPairAttempt,
    ) -> bool
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        let left_idx = pair.left_idx;
        let right_idx = pair.right_idx;
        if left_idx >= state.best_typed_nodes.len() || right_idx >= state.best_typed_nodes.len() {
            return false;
        }

        let left_snapshot = state.best_typed_nodes[left_idx].clone();
        let right_snapshot = state.best_typed_nodes[right_idx].clone();
        let (left_value, right_value, shrink_towards) = match (
            &left_snapshot.value,
            &right_snapshot.value,
            &left_snapshot.constraints,
        ) {
            (
                ChoiceValue::Integer(left),
                ChoiceValue::Integer(right),
                ChoiceConstraints::Integer { shrink_towards, .. },
            ) => (*left, *right, *shrink_towards),
            _ => return false,
        };

        let try_n = |state: &mut NativeShrinkState, replay: &mut F, n: i128| -> bool {
            let Some(new_left) = left_value.checked_sub(n) else {
                return false;
            };
            let Some(new_right) = right_value.checked_sub(n) else {
                return false;
            };
            let mut forced = typed_values(&state.best_typed_nodes);
            if left_idx >= forced.len() || right_idx >= forced.len() {
                return false;
            }
            forced[left_idx] = ChoiceValue::Integer(new_left);
            forced[right_idx] = ChoiceValue::Integer(new_right);
            replay_if_better_with_forced_choices(state, replay, forced)
        };

        let first = find_integer_max_true(|k| {
            let Some(n) = shrink_towards.checked_sub(k) else {
                return false;
            };
            try_n(state, replay, n)
        });
        let second = find_integer_max_true(|k| {
            let Some(n) = k.checked_sub(shrink_towards) else {
                return false;
            };
            try_n(state, replay, n)
        });

        first > 0 || second > 0
    }

    pub(crate) fn step<F>(
        &mut self,
        state: &mut NativeShrinkState,
        replay: &mut F,
        random_order: bool,
    ) -> PassStep
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if state.remaining_attempts == 0 {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let attempts = Self::attempts_for_state(state);
        if attempts.is_empty() || self.cursor >= attempts.len() {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let pick = pick_attempt_index(state, attempts.len(), self.cursor, random_order);
        self.cursor += 1;
        let changed = Self::try_pair(state, replay, attempts[pick]);
        PassStep {
            attempted: true,
            changed,
        }
    }
}

fn trivial_replacement_for_node(node: &ChoiceNode) -> ChoiceValue {
    if node.was_forced {
        return node.value.clone();
    }
    forced_replacements_for_node(node)
        .into_iter()
        .next()
        .unwrap_or_else(|| node.value.clone())
}

fn replay_and_maybe_update_state<F>(
    state: &mut NativeShrinkState,
    replay: &mut F,
    forced_choices: Vec<ChoiceValue>,
) -> Option<(bool, ReplayResult)>
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    if state.remaining_attempts == 0 {
        return None;
    }
    state.remaining_attempts -= 1;
    let replay_result = replay(state.best.as_slice(), Some(forced_choices.clone()));
    let changed = matches!(
        replay_result.status,
        crate::native_engine::CaseStatus::Interesting { .. }
    ) && crate::native_engine::shrink::compare_replay_complexity(
        &replay_result.typed_nodes,
        &state.best_typed_nodes,
    ) == std::cmp::Ordering::Less;
    if changed {
        state.best = replay_result.buffer.clone();
        state.best_typed_nodes = replay_result.typed_nodes.clone();
        state.best_spans = replay_result.spans.clone();
        state.best_forced_choices = Some(forced_choices);
    }
    Some((changed, replay_result))
}

impl TryTrivialSpansPass {
    pub(crate) fn reset(&mut self) {
        self.cursor = 0;
    }

    fn attempts_for_state(state: &NativeShrinkState) -> Vec<TryTrivialSpanAttempt> {
        state
            .best_spans
            .iter()
            .enumerate()
            .rev()
            .map(|(span_index, _)| TryTrivialSpanAttempt { span_index })
            .collect()
    }

    fn try_span<F>(
        state: &mut NativeShrinkState,
        replay: &mut F,
        attempt: TryTrivialSpanAttempt,
    ) -> bool
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        let Some(span) = state.best_spans.get(attempt.span_index).cloned() else {
            return false;
        };
        let nodes_len = state.best_typed_nodes.len();
        let start = span.start_choice_index.min(nodes_len);
        let end = span.end_choice_index.min(nodes_len);
        if start >= end {
            return false;
        }

        let before_nodes = state.best_typed_nodes.clone();
        let mut forced = typed_values(&before_nodes);
        for idx in start..end {
            forced[idx] = trivial_replacement_for_node(&before_nodes[idx]);
        }

        let Some((changed, replay_result)) = replay_and_maybe_update_state(state, replay, forced)
        else {
            return false;
        };
        if changed {
            return true;
        }
        let Some(new_span) = replay_result.spans.get(attempt.span_index).cloned() else {
            return false;
        };
        let prefix: Vec<ChoiceValue> = before_nodes[..start]
            .iter()
            .map(|n| n.value.clone())
            .collect();
        let suffix: Vec<ChoiceValue> = before_nodes[end..]
            .iter()
            .map(|n| n.value.clone())
            .collect();

        let replacement_start = new_span
            .start_choice_index
            .min(replay_result.typed_nodes.len());
        let replacement_end = new_span
            .end_choice_index
            .min(replay_result.typed_nodes.len());
        if replacement_start > replacement_end {
            return false;
        }
        let replacement: Vec<ChoiceValue> = replay_result.typed_nodes
            [replacement_start..replacement_end]
            .iter()
            .map(|n| n.value.clone())
            .collect();

        let mut forced_followup =
            Vec::with_capacity(prefix.len() + replacement.len() + suffix.len());
        forced_followup.extend(prefix);
        forced_followup.extend(replacement);
        forced_followup.extend(suffix);
        replay_if_better_with_forced_choices(state, replay, forced_followup)
    }

    pub(crate) fn step<F>(
        &mut self,
        state: &mut NativeShrinkState,
        replay: &mut F,
        random_order: bool,
    ) -> PassStep
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if state.remaining_attempts == 0 {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let attempts = Self::attempts_for_state(state);
        if attempts.is_empty() || self.cursor >= attempts.len() {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let pick = pick_attempt_index(state, attempts.len(), self.cursor, random_order);
        self.cursor += 1;
        let changed = Self::try_span(state, replay, attempts[pick]);
        PassStep {
            attempted: true,
            changed,
        }
    }
}

impl NodeProgramPass {
    pub(crate) fn new(program_len: usize) -> Self {
        Self {
            program_len: program_len.max(1),
            cursor: 0,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.cursor = 0;
    }

    fn attempts_for_state(
        state: &NativeShrinkState,
        program_len: usize,
    ) -> Vec<NodeProgramAttempt> {
        if program_len == 0 || state.best_typed_nodes.len() < program_len {
            return Vec::new();
        }
        (0..=state.best_typed_nodes.len() - program_len)
            .rev()
            .map(|start| NodeProgramAttempt { start })
            .collect()
    }

    fn run_node_program_once<F>(
        state: &mut NativeShrinkState,
        replay: &mut F,
        start: usize,
        program_len: usize,
        repeats: usize,
        original_nodes: &[ChoiceNode],
    ) -> bool
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if repeats == 0 || program_len == 0 {
            return false;
        }
        let delete_len = match program_len.checked_mul(repeats) {
            Some(v) => v,
            None => return false,
        };
        if start > original_nodes.len() || start + delete_len > original_nodes.len() {
            return false;
        }
        let mut forced = typed_values(original_nodes);
        forced.drain(start..start + delete_len);
        replay_if_better_with_forced_choices(state, replay, forced)
    }

    fn try_start<F>(
        state: &mut NativeShrinkState,
        replay: &mut F,
        attempt: NodeProgramAttempt,
        program_len: usize,
    ) -> bool
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        let original = state.best_typed_nodes.clone();
        if !Self::run_node_program_once(state, replay, attempt.start, program_len, 1, &original) {
            return false;
        }

        let moved_left = find_integer_max_true(|k| {
            let shift = match (k as usize).checked_mul(program_len) {
                Some(v) => v,
                None => return false,
            };
            let Some(start) = attempt.start.checked_sub(shift) else {
                return false;
            };
            let now = state.best_typed_nodes.clone();
            Self::run_node_program_once(state, replay, start, program_len, 1, &now)
        });
        let shift = (moved_left as usize).saturating_mul(program_len);
        let start = attempt.start.saturating_sub(shift);

        let original_after_left = state.best_typed_nodes.clone();
        let _ = find_integer_max_true(|k| {
            Self::run_node_program_once(
                state,
                replay,
                start,
                program_len,
                k as usize,
                &original_after_left,
            )
        });
        true
    }

    pub(crate) fn step<F>(
        &mut self,
        state: &mut NativeShrinkState,
        replay: &mut F,
        random_order: bool,
    ) -> PassStep
    where
        F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
    {
        if state.remaining_attempts == 0 {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let attempts = Self::attempts_for_state(state, self.program_len);
        if attempts.is_empty() || self.cursor >= attempts.len() {
            return PassStep {
                attempted: false,
                changed: false,
            };
        }
        let pick = pick_attempt_index(state, attempts.len(), self.cursor, random_order);
        self.cursor += 1;
        let changed = Self::try_start(state, replay, attempts[pick], self.program_len);
        PassStep {
            attempted: true,
            changed,
        }
    }
}

pub(crate) fn pass_redistribute_numeric_pairs<F>(
    state: &mut NativeShrinkState,
    replay: &mut F,
) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    let mut pass = RedistributeNumericPairsPass::default();
    loop {
        let step = pass.step(state, replay, false);
        if !step.attempted {
            return false;
        }
        if step.changed {
            return true;
        }
    }
}

pub(crate) fn pass_try_trivial_spans<F>(state: &mut NativeShrinkState, replay: &mut F) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    let mut pass = TryTrivialSpansPass::default();
    loop {
        let step = pass.step(state, replay, false);
        if !step.attempted {
            return false;
        }
        if step.changed {
            return true;
        }
    }
}

pub(crate) fn pass_node_program<F>(
    state: &mut NativeShrinkState,
    replay: &mut F,
    program_len: usize,
) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    let mut pass = NodeProgramPass::new(program_len);
    loop {
        let step = pass.step(state, replay, false);
        if !step.attempted {
            return false;
        }
        if step.changed {
            return true;
        }
    }
}

pub(crate) fn pass_minimize_individual_choices<F>(
    state: &mut NativeShrinkState,
    replay: &mut F,
) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    let mut pass = MinimizeIndividualChoicesPass::default();
    loop {
        let step = pass.step(state, replay, false);
        if !step.attempted {
            return false;
        }
        if step.changed {
            return true;
        }
    }
}

pub(crate) fn pass_lower_integers_together<F>(state: &mut NativeShrinkState, replay: &mut F) -> bool
where
    F: FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult,
{
    let mut pass = LowerIntegersTogetherPass::default();
    loop {
        let step = pass.step(state, replay, false);
        if !step.attempted {
            return false;
        }
        if step.changed {
            return true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_engine::{CaseStatus, ChoiceKind, ChoiceNode, SpanId, SpanState};
    use serde::Deserialize;
    use std::sync::Arc;

    fn int_node(
        index: usize,
        value: i128,
        min: i128,
        max: i128,
        shrink_towards: i128,
    ) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::Integer,
            value: ChoiceValue::Integer(value),
            constraints: ChoiceConstraints::Integer {
                min_value: Some(min),
                max_value: Some(max),
                shrink_towards,
            },
            was_forced: false,
            index,
            encoded_size: 1,
        }
    }

    fn int_node_forced(
        index: usize,
        value: i128,
        min: i128,
        max: i128,
        shrink_towards: i128,
        was_forced: bool,
    ) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::Integer,
            value: ChoiceValue::Integer(value),
            constraints: ChoiceConstraints::Integer {
                min_value: Some(min),
                max_value: Some(max),
                shrink_towards,
            },
            was_forced,
            index,
            encoded_size: 1,
        }
    }

    fn float_node_forced(
        index: usize,
        value: f64,
        min: f64,
        max: f64,
        allow_nan: bool,
        smallest_nonzero_magnitude: f64,
        was_forced: bool,
    ) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::Float,
            value: ChoiceValue::Float(value),
            constraints: ChoiceConstraints::Float {
                min_value: min,
                max_value: max,
                allow_nan,
                smallest_nonzero_magnitude,
            },
            was_forced,
            index,
            encoded_size: 8,
        }
    }

    fn bool_node(index: usize, value: bool) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::Boolean,
            value: ChoiceValue::Boolean(value),
            constraints: ChoiceConstraints::Boolean { p: 0.5 },
            was_forced: false,
            index,
            encoded_size: 1,
        }
    }

    fn bool_node_forced(index: usize, value: bool, p: f64, was_forced: bool) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::Boolean,
            value: ChoiceValue::Boolean(value),
            constraints: ChoiceConstraints::Boolean { p },
            was_forced,
            index,
            encoded_size: 1,
        }
    }

    fn string_node(index: usize, value: &str, min_size: usize, max_size: usize) -> ChoiceNode {
        ChoiceNode {
            kind: ChoiceKind::String,
            value: ChoiceValue::String(value.to_string()),
            constraints: ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: None,
            },
            was_forced: false,
            index,
            encoded_size: value.len(),
        }
    }

    fn mk_state(best_typed_nodes: Vec<ChoiceNode>) -> NativeShrinkState {
        NativeShrinkState {
            best: vec![42, 99],
            best_typed_nodes,
            best_spans: Vec::new(),
            best_forced_choices: None,
            remaining_attempts: 128,
            random_state: 0x9e37_79b9_7f4a_7c15,
            calls: 0,
            calls_at_last_shrink: 0,
            max_stall: 200,
            enforce_cached_test_checks: false,
        }
    }

    fn mk_span(index: usize, start_choice_index: usize, end_choice_index: usize) -> SpanState {
        SpanState {
            id: SpanId(index as u64 + 1),
            label: index as u64,
            start_cursor: start_choice_index,
            end_cursor: end_choice_index,
            start_choice_index,
            end_choice_index,
            discard: false,
        }
    }

    fn redistribute_node_sequence_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("shrink_parity")
            .join("fixtures")
            .join("redistribute_numeric_pairs_node_sequences_v1.json")
    }

    fn lower_integers_together_node_sequence_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("shrink_parity")
            .join("fixtures")
            .join("lower_integers_together_node_sequences_v1.json")
    }

    fn try_trivial_spans_node_sequence_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("shrink_parity")
            .join("fixtures")
            .join("try_trivial_spans_node_sequences_v1.json")
    }

    fn node_program_node_sequence_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("shrink_parity")
            .join("fixtures")
            .join("node_program_node_sequences_v1.json")
    }

    fn multi_pass_node_sequence_fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("shrink_parity")
            .join("fixtures")
            .join("multi_pass_node_sequences_v1.json")
    }

    #[derive(Debug, Deserialize)]
    struct NodeSequenceFixtureFile {
        version: u32,
        scenarios: Vec<NodeSequenceScenarioFixture>,
    }

    #[derive(Debug, Deserialize)]
    struct NodeSequenceScenarioFixture {
        id: String,
        #[allow(dead_code)]
        description: String,
        nodes: Vec<NodeSequenceNodeFixture>,
        #[serde(default)]
        spans: Vec<NodeSequenceSpanFixture>,
        #[serde(default)]
        attempt_result_spans: Option<Vec<NodeSequenceSpanFixture>>,
        #[serde(default)]
        program: Option<String>,
        #[serde(default)]
        passes: Vec<String>,
        #[serde(default)]
        accept_attempts: Vec<Vec<String>>,
        expectations: NodeSequenceExpectations,
    }

    #[derive(Debug, Deserialize)]
    struct NodeSequenceSpanFixture {
        start: usize,
        end: usize,
        #[serde(default)]
        label: u64,
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "lowercase")]
    enum NodeSequenceNodeFixture {
        Integer {
            value: i64,
            min_value: i64,
            max_value: i64,
            shrink_towards: i64,
            #[serde(default)]
            was_forced: bool,
        },
        Float {
            value: f64,
            min_value: f64,
            max_value: f64,
            #[serde(default)]
            allow_nan: bool,
            smallest_nonzero_magnitude: f64,
            #[serde(default)]
            was_forced: bool,
        },
        Boolean {
            value: bool,
            #[serde(default = "default_boolean_probability")]
            p: f64,
            #[serde(default)]
            was_forced: bool,
        },
    }

    fn default_boolean_probability() -> f64 {
        0.5
    }

    #[derive(Debug, Deserialize)]
    struct NodeSequenceExpectations {
        hypothesis_attempts: Vec<Vec<String>>,
    }

    fn load_node_sequence_fixture(
        path: std::path::PathBuf,
        context: &str,
    ) -> NodeSequenceFixtureFile {
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|_| panic!("failed to read {context} fixture"));
        serde_json::from_str(&raw).unwrap_or_else(|_| panic!("failed to parse {context} fixture"))
    }

    fn load_redistribute_node_sequence_fixture() -> NodeSequenceFixtureFile {
        load_node_sequence_fixture(
            redistribute_node_sequence_fixture_path(),
            "redistribute node-sequence",
        )
    }

    fn load_lower_integers_together_node_sequence_fixture() -> NodeSequenceFixtureFile {
        load_node_sequence_fixture(
            lower_integers_together_node_sequence_fixture_path(),
            "lower-integers-together node-sequence",
        )
    }

    fn load_try_trivial_spans_node_sequence_fixture() -> NodeSequenceFixtureFile {
        load_node_sequence_fixture(
            try_trivial_spans_node_sequence_fixture_path(),
            "try-trivial-spans node-sequence",
        )
    }

    fn load_node_program_node_sequence_fixture() -> NodeSequenceFixtureFile {
        load_node_sequence_fixture(
            node_program_node_sequence_fixture_path(),
            "node-program node-sequence",
        )
    }

    fn load_multi_pass_node_sequence_fixture() -> NodeSequenceFixtureFile {
        load_node_sequence_fixture(
            multi_pass_node_sequence_fixture_path(),
            "multi-pass node-sequence",
        )
    }

    fn nodes_from_fixture(nodes: &[NodeSequenceNodeFixture]) -> Vec<ChoiceNode> {
        nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| match node {
                NodeSequenceNodeFixture::Integer {
                    value,
                    min_value,
                    max_value,
                    shrink_towards,
                    was_forced,
                } => int_node_forced(
                    idx,
                    i128::from(*value),
                    i128::from(*min_value),
                    i128::from(*max_value),
                    i128::from(*shrink_towards),
                    *was_forced,
                ),
                NodeSequenceNodeFixture::Float {
                    value,
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                    was_forced,
                } => float_node_forced(
                    idx,
                    *value,
                    *min_value,
                    *max_value,
                    *allow_nan,
                    *smallest_nonzero_magnitude,
                    *was_forced,
                ),
                NodeSequenceNodeFixture::Boolean {
                    value,
                    p,
                    was_forced,
                } => bool_node_forced(idx, *value, *p, *was_forced),
            })
            .collect()
    }

    fn spans_from_fixture(spans: &[NodeSequenceSpanFixture]) -> Vec<SpanState> {
        spans
            .iter()
            .enumerate()
            .map(|(idx, span)| SpanState {
                label: span.label,
                ..mk_span(idx, span.start, span.end)
            })
            .collect()
    }

    fn choice_rows_as_tokens(rows: &[Vec<ChoiceValue>]) -> Vec<Vec<String>> {
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|value| match value {
                        ChoiceValue::Integer(v) => format!("i:{v}"),
                        ChoiceValue::Float(v) => format!("f:{:016x}", v.to_bits()),
                        ChoiceValue::Boolean(v) => format!("b:{}", if *v { 1 } else { 0 }),
                        other => panic!("unsupported forced choice token in fixture: {other:?}"),
                    })
                    .collect()
            })
            .collect()
    }

    fn parse_choice_token(token: &str) -> ChoiceValue {
        if let Some(rest) = token.strip_prefix("i:") {
            return ChoiceValue::Integer(
                rest.parse::<i128>()
                    .unwrap_or_else(|_| panic!("invalid integer token {token}")),
            );
        }
        if let Some(rest) = token.strip_prefix("f:") {
            let bits = u64::from_str_radix(rest, 16)
                .unwrap_or_else(|_| panic!("invalid float token {token}"));
            return ChoiceValue::Float(f64::from_bits(bits));
        }
        if let Some(rest) = token.strip_prefix("b:") {
            return match rest {
                "0" => ChoiceValue::Boolean(false),
                "1" => ChoiceValue::Boolean(true),
                _ => panic!("invalid boolean token {token}"),
            };
        }
        panic!("unsupported token {token}");
    }

    fn parse_choice_rows(tokens: &[Vec<String>]) -> Vec<Vec<ChoiceValue>> {
        tokens
            .iter()
            .map(|row| row.iter().map(|tok| parse_choice_token(tok)).collect())
            .collect()
    }

    fn typed_nodes_with_forced_values(
        template: &[ChoiceNode],
        forced: &[ChoiceValue],
    ) -> Vec<ChoiceNode> {
        template
            .iter()
            .zip(forced.iter())
            .map(|(node, value)| ChoiceNode {
                kind: node.kind,
                value: value.clone(),
                constraints: node.constraints.clone(),
                was_forced: true,
                index: node.index,
                encoded_size: node.encoded_size,
            })
            .collect()
    }

    fn replay_with_oracle(
        template: Vec<ChoiceNode>,
        oracle: impl Fn(&[ChoiceValue]) -> bool + Send + Sync + 'static,
        seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>>,
    ) -> impl FnMut(&[u8], Option<Vec<ChoiceValue>>) -> ReplayResult {
        let oracle: Arc<dyn Fn(&[ChoiceValue]) -> bool + Send + Sync> = Arc::new(oracle);
        move |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            let forced = forced.expect("forced choices should be present");
            seen.lock()
                .expect("seen mutex poisoned")
                .push(forced.clone());
            let status = if oracle(forced.as_slice()) {
                CaseStatus::Interesting {
                    panic_message: "x".to_string(),
                    origin: "x".to_string(),
                }
            } else {
                CaseStatus::Invalid
            };
            ReplayResult {
                status,
                buffer: buffer.to_vec(),
                typed_nodes: typed_nodes_with_forced_values(&template, forced.as_slice()),
                spans: Vec::new(),
            }
        }
    }

    #[test]
    fn redistribute_numeric_pairs_applies_improvement() {
        let mut state = mk_state(vec![int_node(0, 7, 0, 20, 0), int_node(1, 7, 0, 20, 0)]);
        let fallback_nodes = state.best_typed_nodes.clone();

        let mut replay = |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            if forced.as_ref() == Some(&vec![ChoiceValue::Integer(6), ChoiceValue::Integer(8)]) {
                ReplayResult {
                    status: CaseStatus::Interesting {
                        panic_message: "x".to_string(),
                        origin: "x".to_string(),
                    },
                    buffer: vec![1],
                    typed_nodes: vec![int_node(0, 6, 0, 20, 0), int_node(1, 8, 0, 20, 0)],
                    spans: Vec::new(),
                }
            } else {
                ReplayResult {
                    status: CaseStatus::Invalid,
                    buffer: buffer.to_vec(),
                    typed_nodes: fallback_nodes.clone(),
                    spans: Vec::new(),
                }
            }
        };

        let changed = pass_redistribute_numeric_pairs(&mut state, &mut replay);
        assert!(changed);
        assert_eq!(state.best, vec![1]);
        assert_eq!(
            state.best_forced_choices,
            Some(vec![ChoiceValue::Integer(6), ChoiceValue::Integer(8)])
        );
    }

    #[test]
    fn redistribute_numeric_pairs_rejects_non_improving_replays() {
        let original_nodes = vec![int_node(0, 7, 0, 20, 0), int_node(1, 7, 0, 20, 0)];
        let mut state = mk_state(original_nodes.clone());

        let mut replay = |buffer: &[u8], _forced: Option<Vec<ChoiceValue>>| ReplayResult {
            status: CaseStatus::Interesting {
                panic_message: "x".to_string(),
                origin: "x".to_string(),
            },
            buffer: buffer.to_vec(),
            typed_nodes: original_nodes.clone(),
            spans: Vec::new(),
        };

        let changed = pass_redistribute_numeric_pairs(&mut state, &mut replay);
        assert!(!changed);
        assert_eq!(state.best_typed_nodes, original_nodes);
        assert!(state.best_forced_choices.is_none());
    }

    #[test]
    fn try_trivial_spans_applies_trivial_replacements_inside_span() {
        let mut state = mk_state(vec![
            int_node(0, 7, 0, 20, 0),
            bool_node(1, true),
            int_node(2, 9, 0, 20, 0),
        ]);
        state.best_spans = vec![mk_span(0, 0, 2)];

        let mut seen_forced: Vec<Vec<ChoiceValue>> = Vec::new();
        let mut replay = |_buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            let forced = forced.expect("forced choices should be present");
            seen_forced.push(forced.clone());
            if forced
                == vec![
                    ChoiceValue::Integer(0),
                    ChoiceValue::Boolean(false),
                    ChoiceValue::Integer(9),
                ]
            {
                ReplayResult {
                    status: CaseStatus::Interesting {
                        panic_message: "x".to_string(),
                        origin: "x".to_string(),
                    },
                    buffer: vec![13],
                    typed_nodes: vec![
                        int_node(0, 0, 0, 20, 0),
                        bool_node(1, false),
                        int_node(2, 9, 0, 20, 0),
                    ],
                    spans: vec![mk_span(0, 0, 2)],
                }
            } else {
                ReplayResult {
                    status: CaseStatus::Invalid,
                    buffer: vec![42, 99],
                    typed_nodes: vec![
                        int_node(0, 7, 0, 20, 0),
                        bool_node(1, true),
                        int_node(2, 9, 0, 20, 0),
                    ],
                    spans: vec![mk_span(0, 0, 2)],
                }
            }
        };

        let changed = pass_try_trivial_spans(&mut state, &mut replay);
        assert!(changed);
        assert_eq!(
            seen_forced.first(),
            Some(&vec![
                ChoiceValue::Integer(0),
                ChoiceValue::Boolean(false),
                ChoiceValue::Integer(9),
            ])
        );
        assert_eq!(state.best, vec![13]);
    }

    #[test]
    fn node_program_deletes_contiguous_nodes() {
        let mut state = mk_state(vec![
            int_node(0, 5, 0, 20, 0),
            int_node(1, 6, 0, 20, 0),
            int_node(2, 7, 0, 20, 0),
            int_node(3, 8, 0, 20, 0),
        ]);
        let mut seen_forced: Vec<Vec<ChoiceValue>> = Vec::new();

        let mut replay = |_buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            let forced = forced.expect("forced choices should be present");
            seen_forced.push(forced.clone());
            if forced == vec![ChoiceValue::Integer(5), ChoiceValue::Integer(6)] {
                ReplayResult {
                    status: CaseStatus::Interesting {
                        panic_message: "x".to_string(),
                        origin: "x".to_string(),
                    },
                    buffer: vec![17],
                    typed_nodes: vec![int_node(0, 5, 0, 20, 0), int_node(1, 6, 0, 20, 0)],
                    spans: Vec::new(),
                }
            } else {
                ReplayResult {
                    status: CaseStatus::Invalid,
                    buffer: vec![42, 99],
                    typed_nodes: vec![
                        int_node(0, 5, 0, 20, 0),
                        int_node(1, 6, 0, 20, 0),
                        int_node(2, 7, 0, 20, 0),
                        int_node(3, 8, 0, 20, 0),
                    ],
                    spans: Vec::new(),
                }
            }
        };

        let changed = pass_node_program(&mut state, &mut replay, 2);
        assert!(changed);
        assert!(
            seen_forced
                .iter()
                .any(|attempt| attempt == &vec![ChoiceValue::Integer(5), ChoiceValue::Integer(6)])
        );
        assert_eq!(state.best, vec![17]);
    }

    #[test]
    fn minimize_individual_choices_boolean_true_to_false() {
        let mut state = mk_state(vec![bool_node(0, true)]);
        let mut replay = |_buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            assert_eq!(forced, Some(vec![ChoiceValue::Boolean(false)]));
            ReplayResult {
                status: CaseStatus::Interesting {
                    panic_message: "x".to_string(),
                    origin: "x".to_string(),
                },
                buffer: vec![3],
                typed_nodes: vec![bool_node(0, false)],
                spans: Vec::new(),
            }
        };

        let changed = pass_minimize_individual_choices(&mut state, &mut replay);
        assert!(changed);
        assert_eq!(state.best, vec![3]);
        assert_eq!(
            state.best_forced_choices,
            Some(vec![ChoiceValue::Boolean(false)])
        );
    }

    #[test]
    fn minimize_individual_choices_integer_tries_replacements_in_order() {
        let mut state = mk_state(vec![int_node(0, -8, -10, 10, 5)]);
        let mut seen_forced: Vec<Vec<ChoiceValue>> = Vec::new();

        let mut replay = |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            let forced = forced.expect("forced choices should be present");
            seen_forced.push(forced.clone());
            let first = forced.first().cloned();
            if first == Some(ChoiceValue::Integer(0)) {
                ReplayResult {
                    status: CaseStatus::Interesting {
                        panic_message: "x".to_string(),
                        origin: "x".to_string(),
                    },
                    buffer: vec![7],
                    typed_nodes: vec![int_node(0, 0, -10, 10, 5)],
                    spans: Vec::new(),
                }
            } else {
                ReplayResult {
                    status: CaseStatus::Invalid,
                    buffer: buffer.to_vec(),
                    typed_nodes: vec![int_node(0, -8, -10, 10, 5)],
                    spans: Vec::new(),
                }
            }
        };

        let changed = pass_minimize_individual_choices(&mut state, &mut replay);
        assert!(changed);
        assert_eq!(seen_forced.len(), 11);
        assert_eq!(seen_forced[0], vec![ChoiceValue::Integer(5)]);
        assert_eq!(seen_forced.last(), Some(&vec![ChoiceValue::Integer(0)]),);
        assert_eq!(state.best_typed_nodes, vec![int_node(0, 0, -10, 10, 5)]);
    }

    #[test]
    fn minimize_individual_choices_string_tries_lowered_characters() {
        let mut state = mk_state(vec![string_node(0, "bb", 0, 8)]);
        let mut attempts: Vec<Vec<ChoiceValue>> = Vec::new();

        let mut replay = |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
            let forced = forced.expect("forced choices should be present");
            attempts.push(forced.clone());
            if forced.first() == Some(&ChoiceValue::String("aa".to_string())) {
                ReplayResult {
                    status: CaseStatus::Interesting {
                        panic_message: "x".to_string(),
                        origin: "x".to_string(),
                    },
                    buffer: vec![11],
                    typed_nodes: vec![string_node(0, "aa", 0, 8)],
                    spans: Vec::new(),
                }
            } else {
                ReplayResult {
                    status: CaseStatus::Invalid,
                    buffer: buffer.to_vec(),
                    typed_nodes: vec![string_node(0, "bb", 0, 8)],
                    spans: Vec::new(),
                }
            }
        };

        let changed = pass_minimize_individual_choices(&mut state, &mut replay);
        assert!(changed);
        assert!(
            attempts
                .iter()
                .any(|forced| { forced.first() == Some(&ChoiceValue::String("aa".to_string())) })
        );
        assert_eq!(state.best_typed_nodes, vec![string_node(0, "aa", 0, 8)],);
    }

    #[test]
    fn redistribute_numeric_pairs_node_sequence_attempt_order_no_shrinks() {
        let template = vec![
            int_node(0, 18, 0, 20, 0),
            int_node(1, 16, 0, 20, 0),
            int_node(2, 18, 0, 20, 0),
        ];
        let mut state = mk_state(template.clone());
        let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut replay = replay_with_oracle(template, |_forced| false, seen.clone());

        let changed = pass_redistribute_numeric_pairs(&mut state, &mut replay);
        assert!(!changed);
        let seen = seen.lock().expect("seen mutex poisoned");
        assert_eq!(
            *seen,
            vec![
                vec![
                    ChoiceValue::Integer(18),
                    ChoiceValue::Integer(15),
                    ChoiceValue::Integer(19),
                ],
                vec![
                    ChoiceValue::Integer(17),
                    ChoiceValue::Integer(16),
                    ChoiceValue::Integer(19),
                ],
                vec![
                    ChoiceValue::Integer(17),
                    ChoiceValue::Integer(17),
                    ChoiceValue::Integer(18),
                ],
            ]
        );
    }

    #[test]
    fn redistribute_numeric_pairs_node_sequence_can_shrink_later_pair() {
        let template = vec![
            int_node(0, 18, 0, 20, 0),
            int_node(1, 16, 0, 20, 0),
            int_node(2, 18, 0, 20, 0),
        ];
        let mut state = mk_state(template.clone());
        let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        // Only accept shrinking attempts on the (idx1, idx2) pair.
        let mut replay = replay_with_oracle(
            template,
            |forced| {
                forced
                    == [
                        ChoiceValue::Integer(18),
                        ChoiceValue::Integer(15),
                        ChoiceValue::Integer(19),
                    ]
            },
            seen.clone(),
        );

        let changed = pass_redistribute_numeric_pairs(&mut state, &mut replay);
        assert!(changed);
        assert_eq!(
            state.best_typed_nodes.first().map(|n| &n.value),
            Some(&ChoiceValue::Integer(18))
        );
        assert!(matches!(
            state.best_typed_nodes.get(1).map(|n| &n.value),
            Some(ChoiceValue::Integer(v)) if *v < 16
        ));
        assert!(matches!(
            state.best_typed_nodes.get(2).map(|n| &n.value),
            Some(ChoiceValue::Integer(v)) if *v > 18
        ));
        let seen = seen.lock().expect("seen mutex poisoned");
        assert!(seen.contains(&vec![
            ChoiceValue::Integer(18),
            ChoiceValue::Integer(15),
            ChoiceValue::Integer(19),
        ]));
    }

    #[test]
    fn redistribute_numeric_pairs_node_sequence_fixture_parity() {
        let fixture = load_redistribute_node_sequence_fixture();
        assert_eq!(fixture.version, 1, "unsupported fixture version");
        let scenario_filter = std::env::var("HEGEL_REDIS_NODE_SCENARIO").ok();

        for scenario in &fixture.scenarios {
            if let Some(filter) = scenario_filter.as_ref()
                && scenario.id != *filter
            {
                continue;
            }
            let template = nodes_from_fixture(&scenario.nodes);
            let mut state = mk_state(template.clone());
            let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut replay = replay_with_oracle(template, |_forced| false, seen.clone());
            let changed = pass_redistribute_numeric_pairs(&mut state, &mut replay);
            assert!(
                !changed,
                "fixture scenario {} unexpectedly shrank in no-shrink oracle",
                scenario.id
            );
            let seen = seen.lock().expect("seen mutex poisoned");
            let actual_attempts = choice_rows_as_tokens(&seen);
            assert_eq!(
                actual_attempts, scenario.expectations.hypothesis_attempts,
                "attempt-order mismatch for fixture scenario {}",
                scenario.id
            );
        }
    }

    #[test]
    fn lower_integers_together_node_sequence_fixture_parity() {
        let fixture = load_lower_integers_together_node_sequence_fixture();
        assert_eq!(fixture.version, 1, "unsupported fixture version");
        let scenario_filter = std::env::var("HEGEL_LOWER_INT_NODE_SCENARIO").ok();

        for scenario in &fixture.scenarios {
            if let Some(filter) = scenario_filter.as_ref()
                && scenario.id != *filter
            {
                continue;
            }
            let template = nodes_from_fixture(&scenario.nodes);
            let mut state = mk_state(template.clone());
            let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut replay = replay_with_oracle(template, |_forced| false, seen.clone());
            let changed = pass_lower_integers_together(&mut state, &mut replay);
            assert!(
                !changed,
                "fixture scenario {} unexpectedly shrank in no-shrink oracle",
                scenario.id
            );
            let seen = seen.lock().expect("seen mutex poisoned");
            let actual_attempts = choice_rows_as_tokens(&seen);
            assert_eq!(
                actual_attempts, scenario.expectations.hypothesis_attempts,
                "attempt-order mismatch for fixture scenario {}",
                scenario.id
            );
        }
    }

    #[test]
    fn try_trivial_spans_node_sequence_fixture_parity() {
        let fixture = load_try_trivial_spans_node_sequence_fixture();
        assert_eq!(fixture.version, 1, "unsupported fixture version");
        let scenario_filter = std::env::var("HEGEL_TRY_TRIVIAL_NODE_SCENARIO").ok();

        for scenario in &fixture.scenarios {
            if let Some(filter) = scenario_filter.as_ref()
                && scenario.id != *filter
            {
                continue;
            }

            let template = nodes_from_fixture(&scenario.nodes);
            let mut state = mk_state(template.clone());
            state.best_spans = spans_from_fixture(&scenario.spans);
            let attempt_result_spans = scenario
                .attempt_result_spans
                .as_ref()
                .map(|spans| spans_from_fixture(spans));
            let accepted_rows = parse_choice_rows(&scenario.accept_attempts);

            let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen_clone = Arc::clone(&seen);
            let mut replay = move |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
                let forced = forced.expect("forced choices should be present");
                seen_clone
                    .lock()
                    .expect("seen mutex poisoned")
                    .push(forced.clone());
                let typed_nodes = typed_nodes_with_forced_values(&template, &forced);
                let accepted = accepted_rows.iter().any(|row| row == &forced);
                if accepted {
                    ReplayResult {
                        status: CaseStatus::Interesting {
                            panic_message: "x".to_string(),
                            origin: "x".to_string(),
                        },
                        buffer: buffer.to_vec(),
                        typed_nodes,
                        spans: attempt_result_spans.clone().unwrap_or_default(),
                    }
                } else if let Some(spans) = attempt_result_spans.as_ref() {
                    ReplayResult {
                        status: CaseStatus::Valid,
                        buffer: buffer.to_vec(),
                        typed_nodes,
                        spans: spans.clone(),
                    }
                } else {
                    ReplayResult {
                        status: CaseStatus::Invalid,
                        buffer: buffer.to_vec(),
                        typed_nodes,
                        spans: Vec::new(),
                    }
                }
            };

            let changed = pass_try_trivial_spans(&mut state, &mut replay);
            assert!(
                !changed,
                "fixture scenario {} unexpectedly shrank",
                scenario.id
            );

            let seen = seen.lock().expect("seen mutex poisoned");
            let actual_attempts = choice_rows_as_tokens(&seen);
            assert_eq!(
                actual_attempts, scenario.expectations.hypothesis_attempts,
                "attempt-order mismatch for fixture scenario {}",
                scenario.id
            );
        }
    }

    #[test]
    fn node_program_node_sequence_fixture_parity() {
        let fixture = load_node_program_node_sequence_fixture();
        assert_eq!(fixture.version, 1, "unsupported fixture version");
        let scenario_filter = std::env::var("HEGEL_NODE_PROGRAM_NODE_SCENARIO").ok();

        for scenario in &fixture.scenarios {
            if let Some(filter) = scenario_filter.as_ref()
                && scenario.id != *filter
            {
                continue;
            }

            let Some(program) = scenario.program.as_ref() else {
                panic!("node_program scenario {} missing program", scenario.id);
            };
            assert!(
                !program.is_empty() && program.chars().all(|c| c == 'X'),
                "node_program scenario {} has unsupported program {}",
                scenario.id,
                program
            );

            let program_len = program.len();
            let template = nodes_from_fixture(&scenario.nodes);
            let mut state = mk_state(template.clone());
            let accepted_rows = parse_choice_rows(&scenario.accept_attempts);

            let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen_clone = Arc::clone(&seen);
            let mut replay = move |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
                let forced = forced.expect("forced choices should be present");
                seen_clone
                    .lock()
                    .expect("seen mutex poisoned")
                    .push(forced.clone());
                let accepted = accepted_rows.iter().any(|row| row == &forced);
                if accepted {
                    ReplayResult {
                        status: CaseStatus::Interesting {
                            panic_message: "x".to_string(),
                            origin: "x".to_string(),
                        },
                        buffer: buffer.to_vec(),
                        typed_nodes: typed_nodes_with_forced_values(&template, &forced),
                        spans: Vec::new(),
                    }
                } else {
                    ReplayResult {
                        status: CaseStatus::Invalid,
                        buffer: buffer.to_vec(),
                        typed_nodes: typed_nodes_with_forced_values(&template, &forced),
                        spans: Vec::new(),
                    }
                }
            };

            let _changed = pass_node_program(&mut state, &mut replay, program_len);

            let seen = seen.lock().expect("seen mutex poisoned");
            let actual_attempts = choice_rows_as_tokens(&seen);
            assert_eq!(
                actual_attempts, scenario.expectations.hypothesis_attempts,
                "attempt-order mismatch for fixture scenario {}",
                scenario.id
            );
        }
    }

    #[test]
    fn multi_pass_node_sequence_fixture_parity() {
        let fixture = load_multi_pass_node_sequence_fixture();
        assert_eq!(fixture.version, 1, "unsupported fixture version");
        let scenario_filter = std::env::var("HEGEL_MULTI_PASS_NODE_SCENARIO").ok();

        for scenario in &fixture.scenarios {
            if let Some(filter) = scenario_filter.as_ref()
                && scenario.id != *filter
            {
                continue;
            }

            let template = nodes_from_fixture(&scenario.nodes);
            let mut state = mk_state(template.clone());
            state.best_spans = spans_from_fixture(&scenario.spans);
            let attempt_result_spans = scenario
                .attempt_result_spans
                .as_ref()
                .map(|spans| spans_from_fixture(spans));

            let seen: Arc<std::sync::Mutex<Vec<Vec<ChoiceValue>>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen_clone = Arc::clone(&seen);
            let mut replay = move |buffer: &[u8], forced: Option<Vec<ChoiceValue>>| {
                let forced = forced.expect("forced choices should be present");
                seen_clone
                    .lock()
                    .expect("seen mutex poisoned")
                    .push(forced.clone());
                let typed_nodes = typed_nodes_with_forced_values(&template, &forced);
                if let Some(spans) = attempt_result_spans.as_ref() {
                    ReplayResult {
                        status: CaseStatus::Valid,
                        buffer: buffer.to_vec(),
                        typed_nodes,
                        spans: spans.clone(),
                    }
                } else {
                    ReplayResult {
                        status: CaseStatus::Invalid,
                        buffer: buffer.to_vec(),
                        typed_nodes,
                        spans: Vec::new(),
                    }
                }
            };

            for pass in &scenario.passes {
                match pass.as_str() {
                    "try_trivial_spans" => {
                        let _ = pass_try_trivial_spans(&mut state, &mut replay);
                    }
                    name if name.starts_with("node_program_") => {
                        let program = name.trim_start_matches("node_program_");
                        assert!(
                            !program.is_empty() && program.chars().all(|c| c == 'X'),
                            "unsupported node_program pass {} in scenario {}",
                            name,
                            scenario.id
                        );
                        let _ = pass_node_program(&mut state, &mut replay, program.len());
                    }
                    "redistribute_numeric_pairs" => {
                        let _ = pass_redistribute_numeric_pairs(&mut state, &mut replay);
                    }
                    "lower_integers_together" => {
                        let _ = pass_lower_integers_together(&mut state, &mut replay);
                    }
                    other => panic!(
                        "unsupported pass {} in multi-pass fixture scenario {}",
                        other, scenario.id
                    ),
                }
            }

            let seen = seen.lock().expect("seen mutex poisoned");
            let actual_attempts = choice_rows_as_tokens(&seen);
            assert_eq!(
                actual_attempts, scenario.expectations.hypothesis_attempts,
                "attempt-order mismatch for fixture scenario {}",
                scenario.id
            );
        }
    }
}
