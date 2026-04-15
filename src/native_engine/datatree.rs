use std::collections::{HashMap, HashSet};

use sha2::digest::consts::False;

use super::{
    CaseStatus, ChoiceConstraints, ChoiceKind, ChoiceValue, EngineState, GenerationTerminal,
};

#[derive(Debug, Clone, Default)]
pub struct DataTree {
    pub root: TreeNode,
}

#[derive(Debug, Clone)]
pub enum Transition {
    Killed,
    Branch(Branch),
    Conclusion(Conclusion),
}

#[derive(Debug, Clone)]
pub struct Branch {
    pub constraints: ChoiceConstraints,
    pub children: HashMap<ChoiceValue, TreeNode>,
}

#[derive(Debug, Clone)]
pub struct Conclusion {
    pub status: CaseStatus,
    // TODO: I think this normally uses Python specific information, so I'll leave it for now.
    // pub interesting_origin: Option<InterestingOrigin>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteStatus {
    Overrun,
    Valid,
    Invalid,
    Interesting,
}

#[derive(Debug, Clone, Default)]
pub struct TreeNode {
    pub constraints: Vec<ChoiceConstraints>,
    pub values: Vec<ChoiceValue>,
    pub forced: HashSet<usize>,
    pub transition: Option<Transition>,
    pub is_exhausted: bool,
}

impl TreeNode {
    pub fn split_at(&mut self, i: usize) {
        if self.forced.contains(i) {
            // TODO: Let's turn this into a proper error propagation
            panic!("raise FlakyStrategyDefinition(_FLAKY_STRAT_MSG)")
        }

        let key = self.values[i];

        let child = TreeNode {
            constraints: self.constraints.split_off(i + 1),
            values: self.values.split_off(i + 1),
            forced: HashSet::new(),
            transition: self.transition.take(),
            is_exhausted: false,
        };

        self.transition = Some(Transition::Branch(Branch {
            constraints: self.constraints.pop().unwrap(),
            children: HashMap::from([(key, child)]),
        }));

        for j in self.forced.iter().filter(|&&j| j >= i) {
            child.forced.insert(j - i - 1);
        }
        self.forced.retain(|&j| j < i);

        child.check_exhausted();

        self.constraints = self.constraints[..i].to_vec();
        self.values = self.values[..i].to_vec();
    }
}

pub struct TreeRecordingObserver<'a> {
    root: &'a TreeNode,
    current_node: &'a TreeNode,
    index_in_current_node: usize,
    trail: Vec<&'a TreeNode>,
    killed: bool,
}

impl<'a> TreeRecordingObserver<'a> {
    pub fn new(tree: &'a DataTree) -> Self {
        Self {
            root: &tree.root,
            current_node: &tree.root,
            index_in_current_node: 0,
            trail: vec![&tree.root],
            killed: false,
        }
    }
    pub fn draw_value(
        &mut self,
        value: ChoiceValue,
        constraints: ChoiceConstraints,
        was_forced: bool,
    ) {
        let i = self.index_in_current_node;
        self.index_in_current_node += 1;
        let node = self.current_node;
        // NOTE: We do this at the `hash` implementation of `ChoiceValue`.
        // if isinstance(value, float):
        //     value = float_to_int(value)

        if i < node.values.len() {
            if constraints != node.constraints[i] {
                // TODO: Let's turn this into a proper error propagation
                panic!("raise FlakyStrategyDefinition(_FLAKY_STRAT_MSG)")
            }

            if was_forced && !node.forced.contains(&i) {
                // TODO: Let's turn this into a proper error propagation
                panic!("raise FlakyStrategyDefinition(_FLAKY_STRAT_MSG)")
            }

            if value != node.values[i] {
                node.split_at(i);
                let new_node = TreeNode::default();
                node.transition.children.set(value, new_node);
                self.current_node = &new_node;
                self.index_in_current_node = 0;
            }
        }
    }
}
