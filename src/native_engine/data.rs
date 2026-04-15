use core::f64;
use std::{collections::HashMap, rc::Rc};

use rand::distr::weighted;
use sha2::Digest as _;

use crate::native_engine::{
    choice::{ChoiceConstraints, ChoiceValue},
    datatree::TreeRecordingObserver,
    floats::{float_to_int, int_to_float, next_up},
    provider::Provider,
    random::Random,
};

pub struct Span {}
pub enum ConjectureDataStatus {
    Overrun,
    Invalid,
    Valid,
    Interesting,
}

pub struct ConjectureData {
    pub(crate) prefix: Vec<ChoiceValue>,
    pub(crate) max_choices: Option<usize>,
    observer: TreeRecordingObserver,
    nodes: Vec<ChoiceValue>,
    random: Random,
    max_length: usize,
    overdraw: usize,
    length: usize,
    index: usize,
    status: ConjectureDataStatus,
    spans: Vec<Span>,
}

impl ConjectureData {
    pub fn new() -> Self {
        todo!()
    }
    pub fn start_span(&mut self, _label: usize) -> Span {
        todo!()
    }

    pub fn stop_span(&mut self) {
        todo!()
    }

    pub fn draw_boolean(&mut self, p: f64, forced: Option<bool>, observe: bool) -> bool {
        todo!()
    }

    pub fn draw_integer(
        &mut self,
        min_value: Option<i128>,
        max_value: Option<i128>,
        shrink_towards: Option<i128>,
        weights: Option<HashMap<i128, f64>>,
        forced: Option<i128>,
        observe: bool,
    ) -> i128 {
        let ChoiceValue::Integer(forced) = self.draw(
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                weights,
                shrink_towards: shrink_towards.unwrap_or(0),
            },
            observe,
            forced.map(ChoiceValue::Integer),
        ) else {
            panic!("Expected an integer choice value")
        };
        forced
    }

    pub fn draw_float(
        &mut self,
        min_value: Option<f64>,
        max_value: Option<f64>,
        allow_nan: bool,
        smallest_nonzero_magnitude: Option<f64>,
        forced: Option<f64>,
        observe: bool,
    ) -> f64 {
        let ChoiceValue::Float(forced) = self.draw(
            ChoiceConstraints::Float {
                min_value: min_value.unwrap_or(f64::NEG_INFINITY),
                max_value: max_value.unwrap_or(f64::INFINITY),
                allow_nan,
                smallest_nonzero_magnitude: smallest_nonzero_magnitude.unwrap_or(next_up(0.0, 64)),
            },
            observe,
            forced.map(ChoiceValue::Float),
        ) else {
            panic!("Expected a float choice value")
        };
        forced
    }

    pub fn draw<P: Provider>(
        &mut self,
        provider: &mut P,
        constraint: ChoiceConstraints,
        observe: bool,
        forced: Option<ChoiceValue>,
    ) -> ChoiceValue {
        if self.length == self.max_length {
            self.mark_overrun();
        }
        if Some(self.nodes.len()) == self.max_choices {
            self.mark_overrun();
        }

        let mut value = if observe && self.index < self.prefix.len() {
            self.pop_choice(constraint, forced)
        } else if let Some(forced) = forced {
            forced
        } else {
            provider.draw(constraint)
        };

        if let ChoiceValue::Float(f) = value {
            if f.is_nan() {
                value = ChoiceValue::Float(int_to_float(float_to_int(f, 64), 64));
            }
        }

        if observe {
            self.observer.draw(&value, constraint, forced.is_some());
        }

        value
    }

    fn pop_choice(
        &mut self,
        constraint: ChoiceConstraints,
        forced: Option<ChoiceValue>,
    ) -> ChoiceValue {
        todo!()
    }

    fn mark_overrun(&mut self) {
        todo!()
    }
}
