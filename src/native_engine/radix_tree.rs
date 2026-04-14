use std::collections::{HashMap, HashSet};

use super::{
    CaseStatus, ChoiceConstraints, ChoiceKind, ChoiceValue, EngineState, GenerationTerminal,
    TypedChoiceNode, choice_value_equals, contains_choice_value, generation_terminal_for_status,
    generation_tree_choice_from_index, generation_tree_max_children,
    run_draw_choice_from_constraints,
};

#[derive(Debug, Clone, Default)]
pub struct DataTree {
    pub root: TreeNode,
}

#[derive(Debug, Clone, Default)]
pub struct TreeNode {
    pub choice_types: Vec<ChoiceKind>,
    pub constraints: Vec<ChoiceConstraints>,
    pub values: Vec<ChoiceValue>,
    pub forced: HashSet<usize>,
    pub transition: Option<Transition>,
    pub is_exhausted: bool,
}

#[derive(Debug, Clone)]
pub enum Transition {
    Branch(Branch),
    Conclusion(GenerationTerminal),
}

#[derive(Debug, Clone)]
pub struct Branch {
    pub kind: ChoiceKind,
    pub constraints: ChoiceConstraints,
    pub children: Vec<BranchChild>,
}

#[derive(Debug, Clone)]
pub struct BranchChild {
    pub value: ChoiceValue,
    pub child: Box<TreeNode>,
}

#[derive(Debug, Clone)]
pub enum FollowupSimulation {
    Predictable,
    Novel { prefix: Vec<ChoiceValue> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteStatus {
    Overrun,
    Valid,
    Invalid,
    Interesting,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChildrenCacheValue {
    next_index: u128,
    children: Vec<ChoiceValue>,
    rejected: Vec<ChoiceValue>,
    exhausted: bool,
}

fn child_index(children: &[BranchChild], value: &ChoiceValue) -> Option<usize> {
    children
        .iter()
        .position(|child| choice_value_equals(&child.value, value))
}

fn trace_choice_value_token(value: &ChoiceValue) -> String {
    match value {
        ChoiceValue::Boolean(v) => format!("b:{}", if *v { 1 } else { 0 }),
        ChoiceValue::Integer(v) => format!("i:{v}"),
        ChoiceValue::Float(v) => format!("f:{:016x}", v.to_bits()),
        ChoiceValue::Bytes(bytes) => format!(
            "y:{}",
            bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ),
        ChoiceValue::String(s) => format!("s:{s:?}"),
    }
}

fn store_generate_prefix_trace(
    engine: &mut EngineState,
    trace_enabled: bool,
    trace: Vec<String>,
    cache: &HashMap<String, ChildrenCacheValue>,
) {
    if let Some(run) = engine.active_run.as_mut() {
        run.novel_prefix_children_cache = cache.clone();
        if trace_enabled {
            run.last_generate_novel_prefix_trace = Some(trace);
        } else {
            run.last_generate_novel_prefix_trace = None;
        }
    }
}

fn split_at(node: &mut TreeNode, i: usize) {
    if i >= node.values.len() || node.forced.contains(&i) {
        return;
    }

    let key = node.values[i].clone();
    let kind = node.choice_types[i];
    let constraints = node.constraints[i].clone();

    let mut child = TreeNode {
        choice_types: node.choice_types[i + 1..].to_vec(),
        constraints: node.constraints[i + 1..].to_vec(),
        values: node.values[i + 1..].to_vec(),
        forced: node
            .forced
            .iter()
            .filter_map(|idx| if *idx > i { Some(*idx - i - 1) } else { None })
            .collect(),
        transition: node.transition.take(),
        is_exhausted: false,
    };
    recompute_exhausted(&mut child);

    node.forced.retain(|idx| *idx < i);
    node.choice_types.truncate(i);
    node.constraints.truncate(i);
    node.values.truncate(i);
    node.transition = Some(Transition::Branch(Branch {
        kind,
        constraints,
        children: vec![BranchChild {
            value: key,
            child: Box::new(child),
        }],
    }));
}

fn check_exhausted(node: &mut TreeNode) -> bool {
    if node.transition.is_none() {
        node.is_exhausted = false;
        return false;
    }

    if node.forced.len() != node.values.len() {
        node.is_exhausted = false;
        return false;
    }

    match node.transition.as_ref().expect("checked is_some above") {
        Transition::Conclusion(_) => {
            node.is_exhausted = true;
        }
        Transition::Branch(branch) => {
            let all_children_exhausted = branch.children.iter().all(|c| c.child.is_exhausted);
            let max_children = generation_tree_max_children(branch.kind, &branch.constraints);
            node.is_exhausted =
                (branch.children.len() as u128) >= max_children && all_children_exhausted;
        }
    }
    node.is_exhausted
}

fn recompute_exhausted(node: &mut TreeNode) -> bool {
    if let Some(Transition::Branch(branch)) = node.transition.as_mut() {
        for child in &mut branch.children {
            recompute_exhausted(&mut child.child);
        }
    }
    check_exhausted(node)
}

fn conclude_at(node: &mut TreeNode, i: usize, terminal: GenerationTerminal) {
    if i < node.values.len() {
        if node.forced.contains(&i) {
            return;
        }
        split_at(node, i);
    }

    if matches!(node.transition, Some(Transition::Branch(_))) {
        return;
    }

    match node.transition.as_ref() {
        Some(Transition::Conclusion(existing)) if *existing == terminal => {}
        _ => {
            node.transition = Some(Transition::Conclusion(terminal));
        }
    }
}

fn record_from(
    node: &mut TreeNode,
    typed_nodes: &[TypedChoiceNode],
    index: usize,
    terminal: GenerationTerminal,
) {
    let mut i = 0usize;
    let mut seq_idx = index;

    loop {
        if seq_idx >= typed_nodes.len() {
            conclude_at(node, i, terminal);
            return;
        }

        let current = &typed_nodes[seq_idx];
        if i < node.values.len() {
            if node.choice_types[i] != current.kind || node.constraints[i] != current.constraints {
                return;
            }

            if current.was_forced {
                node.forced.insert(i);
            }

            if choice_value_equals(&node.values[i], &current.value) {
                i = i.saturating_add(1);
                seq_idx = seq_idx.saturating_add(1);
                continue;
            }

            if node.forced.contains(&i) {
                return;
            }
            split_at(node, i);
            if let Some(Transition::Branch(branch)) = node.transition.as_mut() {
                let child_idx = if let Some(idx) = child_index(&branch.children, &current.value) {
                    idx
                } else {
                    branch.children.push(BranchChild {
                        value: current.value.clone(),
                        child: Box::default(),
                    });
                    branch.children.len() - 1
                };
                record_from(
                    &mut branch.children[child_idx].child,
                    typed_nodes,
                    seq_idx.saturating_add(1),
                    terminal,
                );
            }
            return;
        }

        match node.transition.as_mut() {
            None => {
                node.choice_types.push(current.kind);
                node.constraints.push(current.constraints.clone());
                node.values.push(current.value.clone());
                if current.was_forced {
                    node.forced.insert(i);
                }

                if generation_tree_max_children(current.kind, &current.constraints) == 1
                    && !current.was_forced
                {
                    split_at(node, i);
                    if let Some(Transition::Branch(branch)) = node.transition.as_mut() {
                        let next_idx =
                            child_index(&branch.children, &current.value).expect("just split");
                        record_from(
                            &mut branch.children[next_idx].child,
                            typed_nodes,
                            seq_idx.saturating_add(1),
                            terminal,
                        );
                    }
                    return;
                }

                i = i.saturating_add(1);
                seq_idx = seq_idx.saturating_add(1);
            }
            Some(Transition::Conclusion(_)) => {
                return;
            }
            Some(Transition::Branch(branch)) => {
                if branch.kind != current.kind || branch.constraints != current.constraints {
                    return;
                }

                let next_idx = if let Some(idx) = child_index(&branch.children, &current.value) {
                    idx
                } else {
                    branch.children.push(BranchChild {
                        value: current.value.clone(),
                        child: Box::default(),
                    });
                    branch.children.len() - 1
                };
                record_from(
                    &mut branch.children[next_idx].child,
                    typed_nodes,
                    seq_idx.saturating_add(1),
                    terminal,
                );
                return;
            }
        }
    }
}

pub fn record_case(tree: &mut DataTree, typed_nodes: &[TypedChoiceNode], status: &CaseStatus) {
    record_from(
        &mut tree.root,
        typed_nodes,
        0,
        generation_terminal_for_status(status),
    );
    recompute_exhausted(&mut tree.root);
}

fn draw_from_cache(
    engine: &mut EngineState,
    kind: ChoiceKind,
    constraints: &ChoiceConstraints,
    key: &str,
    cache: &mut HashMap<String, ChildrenCacheValue>,
) -> Option<ChoiceValue> {
    let entry = cache.entry(key.to_string()).or_default();
    let max_children = generation_tree_max_children(kind, constraints);

    while entry.children.len() < 100 && !entry.exhausted {
        if entry.next_index >= max_children {
            entry.exhausted = true;
            break;
        }
        let next = generation_tree_choice_from_index(constraints, entry.next_index)?;
        entry.next_index = entry.next_index.saturating_add(1);
        if contains_choice_value(&entry.rejected, &next) {
            continue;
        }
        entry.children.push(next);
    }

    if entry.children.is_empty() {
        return None;
    }

    let choice_idx =
        super::run_randint_i128(engine, 0, (entry.children.len() - 1) as i128) as usize;
    Some(entry.children[choice_idx].clone())
}

fn reject_cached_child(
    key: &str,
    child: &ChoiceValue,
    cache: &mut HashMap<String, ChildrenCacheValue>,
) {
    let entry = cache.entry(key.to_string()).or_default();
    if !contains_choice_value(&entry.rejected, child) {
        entry.rejected.push(child.clone());
    }
    entry
        .children
        .retain(|existing| !choice_value_equals(existing, child));
}

pub fn generate_novel_prefix(
    engine: &mut EngineState,
    tree: &DataTree,
) -> Option<Vec<ChoiceValue>> {
    let trace_enabled = std::env::var_os("HEGEL_SHRINK_PARITY_PREFIX_TRACE").is_some();
    let mut trace: Vec<String> = Vec::new();
    if trace_enabled {
        trace.push("start".to_string());
    }

    if tree.root.is_exhausted {
        if trace_enabled {
            trace.push("root_exhausted".to_string());
        }
        let cache = engine
            .active_run
            .as_ref()
            .map(|run| run.novel_prefix_children_cache.clone())
            .unwrap_or_default();
        store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
        return None;
    }

    let mut cache: HashMap<String, ChildrenCacheValue> = engine
        .active_run
        .as_ref()
        .map(|run| run.novel_prefix_children_cache.clone())
        .unwrap_or_default();
    let mut prefix = Vec::new();
    let mut current = &tree.root;
    let mut current_path = "R".to_string();
    let max_depth = 1024usize;

    for depth in 0..max_depth {
        if current.is_exhausted {
            if trace_enabled {
                trace.push(format!("depth={depth} node_exhausted"));
            }
            store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
            return None;
        }

        for (i, ((kind, constraints), value)) in current
            .choice_types
            .iter()
            .zip(current.constraints.iter())
            .zip(current.values.iter())
            .enumerate()
        {
            if current.forced.contains(&i) {
                if trace_enabled {
                    trace.push(format!(
                        "depth={depth} fixed i={i} value={}",
                        trace_choice_value_token(value)
                    ));
                }
                prefix.push(value.clone());
                continue;
            }

            let key = format!("N:{current_path}");
            let mut attempts = 0usize;
            loop {
                let (draw, source) = if attempts <= 10 {
                    (
                        run_draw_choice_from_constraints(engine, constraints),
                        "direct",
                    )
                } else if let Some(cached) =
                    draw_from_cache(engine, *kind, constraints, &key, &mut cache)
                {
                    (cached, "cache")
                } else {
                    (
                        run_draw_choice_from_constraints(engine, constraints),
                        "direct",
                    )
                };
                if trace_enabled {
                    trace.push(format!(
                        "depth={depth} vary i={i} attempt={attempts} source={source} draw={} target={}",
                        trace_choice_value_token(&draw),
                        trace_choice_value_token(value)
                    ));
                }

                if !choice_value_equals(&draw, value) {
                    prefix.push(draw);
                    if trace_enabled {
                        trace.push(format!(
                            "depth={depth} novel_at_node i={i} prefix_len={}",
                            prefix.len()
                        ));
                    }
                    store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                    return Some(prefix);
                }
                reject_cached_child(&key, &draw, &mut cache);
                attempts = attempts.saturating_add(1);
                if attempts > 1000 {
                    if trace_enabled {
                        trace.push(format!("depth={depth} fail_attempts_node i={i}"));
                    }
                    store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                    return None;
                }
            }
        }

        match current.transition.as_ref() {
            None => {
                if trace_enabled {
                    trace.push(format!(
                        "depth={depth} transition_none prefix_len={}",
                        prefix.len()
                    ));
                }
                store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                return Some(prefix);
            }
            Some(Transition::Conclusion(_)) => {
                if trace_enabled {
                    trace.push(format!("depth={depth} transition_conclusion"));
                }
                store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                return None;
            }
            Some(Transition::Branch(branch)) => {
                let key = format!("B:{current_path}");
                let mut attempts = 0usize;
                loop {
                    let (draw, source) = if attempts <= 10 {
                        (
                            run_draw_choice_from_constraints(engine, &branch.constraints),
                            "direct",
                        )
                    } else if let Some(cached) =
                        draw_from_cache(
                            engine,
                            branch.kind,
                            &branch.constraints,
                            &key,
                            &mut cache,
                        )
                    {
                        (cached, "cache")
                    } else {
                        (
                            run_draw_choice_from_constraints(engine, &branch.constraints),
                            "direct",
                        )
                    };
                    if trace_enabled {
                        trace.push(format!(
                            "depth={depth} branch attempt={attempts} source={source} draw={}",
                            trace_choice_value_token(&draw)
                        ));
                    }

                    let Some(next_idx) = child_index(&branch.children, &draw) else {
                        prefix.push(draw);
                        if trace_enabled {
                            trace.push(format!(
                                "depth={depth} novel_at_branch prefix_len={}",
                                prefix.len()
                            ));
                        }
                        store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                        return Some(prefix);
                    };
                    let child = &branch.children[next_idx];
                    if child.child.is_exhausted {
                        reject_cached_child(&key, &draw, &mut cache);
                        attempts = attempts.saturating_add(1);
                        if attempts > 1000 {
                            if trace_enabled {
                                trace.push(format!("depth={depth} fail_attempts_branch"));
                            }
                            store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
                            return None;
                        }
                        continue;
                    }

                    let draw_token = trace_choice_value_token(&draw);
                    prefix.push(draw);
                    if trace_enabled {
                        trace.push(format!(
                            "depth={depth} descend branch_child={} prefix_len={}",
                            next_idx,
                            prefix.len()
                        ));
                    }
                    current_path = format!("{current_path}/{draw_token}");
                    current = &child.child;
                    break;
                }
            }
        }
    }

    if trace_enabled {
        trace.push(format!("max_depth_return prefix_len={}", prefix.len()));
    }
    store_generate_prefix_trace(engine, trace_enabled, trace, &cache);
    Some(prefix)
}

pub fn simulate_followup_prefix(
    engine: &mut EngineState,
    tree: &DataTree,
    prefix: &[ChoiceValue],
    max_choices: usize,
) -> FollowupSimulation {
    if tree.root.is_exhausted {
        return FollowupSimulation::Predictable;
    }

    let mut choices = prefix.to_vec();
    let mut index = 0usize;
    let mut current = &tree.root;
    let max_depth = 1024usize;

    for _ in 0..max_depth {
        for (i, constraints) in current.constraints.iter().enumerate() {
            let next_value = if current.forced.contains(&i) {
                current.values[i].clone()
            } else if index < prefix.len() {
                let candidate = prefix[index].clone();
                if super::choice_permitted_by_constraints(&candidate, constraints) {
                    candidate
                } else {
                    super::simplest_choice_for_constraints(constraints)
                }
            } else {
                if choices.len() >= max_choices {
                    return FollowupSimulation::Predictable;
                }
                run_draw_choice_from_constraints(engine, constraints)
            };

            if index < choices.len() {
                choices[index] = next_value.clone();
            } else {
                choices.push(next_value.clone());
            }

            if !choice_value_equals(&next_value, &current.values[i]) {
                return FollowupSimulation::Novel { prefix: choices };
            }
            index = index.saturating_add(1);
        }

        match current.transition.as_ref() {
            None => return FollowupSimulation::Novel { prefix: choices },
            Some(Transition::Conclusion(_)) => return FollowupSimulation::Predictable,
            Some(Transition::Branch(branch)) => {
                let next_value = if index < prefix.len() {
                    let candidate = prefix[index].clone();
                    if super::choice_permitted_by_constraints(&candidate, &branch.constraints) {
                        candidate
                    } else {
                        super::simplest_choice_for_constraints(&branch.constraints)
                    }
                } else {
                    if choices.len() >= max_choices {
                        return FollowupSimulation::Predictable;
                    }
                    run_draw_choice_from_constraints(engine, &branch.constraints)
                };

                if index < choices.len() {
                    choices[index] = next_value.clone();
                } else {
                    choices.push(next_value.clone());
                }

                let Some(child_idx) = child_index(&branch.children, &next_value) else {
                    return FollowupSimulation::Novel { prefix: choices };
                };
                current = &branch.children[child_idx].child;
                index = index.saturating_add(1);
            }
        }
    }

    FollowupSimulation::Predictable
}

fn rewrite_status_from_terminal(terminal: GenerationTerminal) -> RewriteStatus {
    match terminal {
        GenerationTerminal::Valid => RewriteStatus::Valid,
        GenerationTerminal::Invalid => RewriteStatus::Invalid,
        GenerationTerminal::Interesting => RewriteStatus::Interesting,
    }
}

/// Hypothesis `DataTree.rewrite` equivalent for the currently implemented tree semantics.
///
/// Returns `(rewritten_choices, status)` where:
/// - `status == None` indicates previously-unseen behaviour.
/// - `status == Some(Overrun)` indicates that replay ran out of provided choices.
/// - `status == Some(..terminal..) ` indicates a predictable terminal result.
pub fn rewrite_prefix(
    tree: &DataTree,
    choices: &[ChoiceValue],
) -> (Vec<ChoiceValue>, Option<RewriteStatus>) {
    let mut rewritten = Vec::new();
    let mut current = &tree.root;
    let mut index = 0usize;
    let max_depth = 1024usize;

    for _ in 0..max_depth {
        for (i, constraints) in current.constraints.iter().enumerate() {
            let next = if current.forced.contains(&i) {
                if index >= choices.len() {
                    return (rewritten, Some(RewriteStatus::Overrun));
                }
                current.values[i].clone()
            } else if index < choices.len() {
                let candidate = choices[index].clone();
                if super::choice_permitted_by_constraints(&candidate, constraints) {
                    candidate
                } else {
                    super::simplest_choice_for_constraints(constraints)
                }
            } else {
                return (rewritten, Some(RewriteStatus::Overrun));
            };

            if !choice_value_equals(&next, &current.values[i]) {
                return (choices.to_vec(), None);
            }
            rewritten.push(next);
            index = index.saturating_add(1);
        }

        match current.transition.as_ref() {
            None => return (choices.to_vec(), None),
            Some(Transition::Conclusion(terminal)) => {
                return (rewritten, Some(rewrite_status_from_terminal(*terminal)));
            }
            Some(Transition::Branch(branch)) => {
                let next = if index < choices.len() {
                    let candidate = choices[index].clone();
                    if super::choice_permitted_by_constraints(&candidate, &branch.constraints) {
                        candidate
                    } else {
                        super::simplest_choice_for_constraints(&branch.constraints)
                    }
                } else {
                    return (rewritten, Some(RewriteStatus::Overrun));
                };

                rewritten.push(next.clone());
                index = index.saturating_add(1);

                let Some(child_idx) = child_index(&branch.children, &next) else {
                    return (choices.to_vec(), None);
                };
                current = &branch.children[child_idx].child;
            }
        }
    }

    (choices.to_vec(), None)
}
