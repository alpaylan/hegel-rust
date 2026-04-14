use std::collections::HashMap;

use crate::native_engine::{
    BUFFER_SIZE,
    floats::{choice_permitted_float, float_to_lex, lex_to_float, make_float_clamper},
    intervalset::IntervalSet,
    utils::{zigzag_index, zigzag_value},
};

#[derive(Debug, Clone, PartialEq)]
pub enum ChoiceConstraints {
    Boolean {
        p: f64,
    },
    Integer {
        min_value: Option<i128>,
        max_value: Option<i128>,
        weights: Option<HashMap<i128, f64>>,
        shrink_towards: i128,
    },
    Float {
        min_value: f64,
        max_value: f64,
        allow_nan: bool,
        smallest_nonzero_magnitude: f64,
    },
    Bytes {
        min_size: usize,
        max_size: usize,
    },
    String {
        min_size: usize,
        max_size: usize,
        intervals: IntervalSet,
    },
}

/// Choice kinds tracked by the typed-choice recording layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChoiceKind {
    Boolean,
    Integer,
    Float,
    Bytes,
    String,
}

/// Typed constraints attached to each recorded choice.
impl ChoiceConstraints {
    pub fn kind(&self) -> ChoiceKind {
        match self {
            Self::Boolean { .. } => ChoiceKind::Boolean,
            Self::Integer { .. } => ChoiceKind::Integer,
            Self::Float { .. } => ChoiceKind::Float,
            Self::Bytes { .. } => ChoiceKind::Bytes,
            Self::String { .. } => ChoiceKind::String,
        }
    }
}

/// Typed primitive value chosen by generation.
#[derive(Debug, Clone, PartialEq)]
pub enum ChoiceValue {
    Boolean(bool),
    Integer(i128),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
}

/// Single recorded typed choice.
#[derive(Debug, Clone, PartialEq)]
pub struct ChoiceNode {
    pub value: ChoiceValue,
    pub constraints: ChoiceConstraints,
    pub was_forced: bool,
    pub index: Option<usize>,
}

impl ChoiceValue {
    pub fn kind(&self) -> ChoiceKind {
        match self {
            Self::Boolean(_) => ChoiceKind::Boolean,
            Self::Integer(_) => ChoiceKind::Integer,
            Self::Float(_) => ChoiceKind::Float,
            Self::Bytes(_) => ChoiceKind::Bytes,
            Self::String(_) => ChoiceKind::String,
        }
    }
}

impl ChoiceNode {
    pub fn new(
        value: ChoiceValue,
        constraints: ChoiceConstraints,
        was_forced: bool,
        index: Option<usize>,
    ) -> Self {
        assert_eq!(value.kind(), constraints.kind());
        Self {
            value,
            constraints,
            was_forced,
            index,
        }
    }

    pub fn copy(
        &self,
        with_value: Option<ChoiceValue>,
        with_constraints: Option<ChoiceConstraints>,
    ) -> Self {
        let value = with_value.unwrap_or_else(|| self.value.clone());
        let constraints = with_constraints.unwrap_or_else(|| self.constraints.clone());
        Self::new(value, constraints, self.was_forced, self.index)
    }

    pub fn trivial(&self) -> bool {
        //! A node is trivial if it cannot be simplified any further. This does not
        //! mean that modifying a trivial node can't produce simpler test cases when
        //! viewing the tree as a whole. Just that when viewing this node in
        //! isolation, this is the simplest the node can get.

        if self.was_forced {
            return true;
        }

        match self.constraints {
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan: _,
                smallest_nonzero_magnitude: _,
            } => {
                if min_value == f64::NEG_INFINITY && max_value == f64::INFINITY {
                    return self.value == ChoiceValue::Float(0.0);
                }
                if !min_value.is_infinite()
                    && !max_value.is_infinite()
                    && min_value.ceil() <= max_value.floor()
                {
                    return self.value
                        == ChoiceValue::Float(
                            0.0_f64.max(min_value.ceil()).min(max_value.floor()),
                        );
                }

                false
            }
            ChoiceConstraints::Boolean { .. }
            | ChoiceConstraints::Integer { .. }
            | ChoiceConstraints::Bytes { .. }
            | ChoiceConstraints::String { .. } => {
                self.value == Self::choice_from_index(0, &self.constraints)
            }
        }
    }

    pub fn choice_from_index(index: u128, c: &ChoiceConstraints) -> ChoiceValue {
        match c {
            ChoiceConstraints::Boolean { p } => {
                if *p <= 2.0_f64.powi(-64) {
                    assert!(index == 0);
                    ChoiceValue::Boolean(false)
                } else if *p >= 1.0 - 2.0_f64.powi(-64) {
                    assert!(index == 0);
                    ChoiceValue::Boolean(true)
                } else {
                    if index == 0 {
                        ChoiceValue::Boolean(false)
                    } else if index == 1 {
                        ChoiceValue::Boolean(true)
                    } else {
                        panic!("Invalid index for boolean choice: {}", index);
                    }
                }
            }
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                weights: _,
                shrink_towards,
            } => {
                let shrink_towards = shrink_towards
                    .max(min_value.as_ref().unwrap_or(&i128::MIN))
                    .min(max_value.as_ref().unwrap_or(&i128::MAX));

                ChoiceValue::Integer(match (min_value, max_value) {
                    (None, None) => zigzag_value(index, *shrink_towards),
                    (Some(min), None) => {
                        if index <= zigzag_index(*min, *shrink_towards) {
                            zigzag_value(index, *shrink_towards)
                        } else {
                            index as i128 + *min
                        }
                    }
                    (None, Some(max)) => {
                        if index <= zigzag_index(*max, *shrink_towards) {
                            zigzag_value(index, *shrink_towards)
                        } else {
                            max - index as i128
                        }
                    }
                    (Some(min), Some(max)) => {
                        // TODO: Weights check
                        if (shrink_towards - min) < (max - shrink_towards) {
                            if index <= zigzag_index(*min, *shrink_towards) {
                                zigzag_value(index, *shrink_towards)
                            } else {
                                min + index as i128
                            }
                        } else {
                            if index <= zigzag_index(*max, *shrink_towards) {
                                zigzag_value(index, *shrink_towards)
                            } else {
                                max - index as i128
                            }
                        }
                    }
                } as i128)
            }
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            } => {
                let sign = if index >> 64 == 0 { 1.0 } else { -1.0 };
                let result = sign * lex_to_float(index as u64 & ((1 << 64) - 1));
                let clamper = make_float_clamper(
                    *min_value,
                    *max_value,
                    *allow_nan,
                    *smallest_nonzero_magnitude,
                );
                ChoiceValue::Float(clamper(result))
            }
            ChoiceConstraints::Bytes {
                min_size,
                max_size: _,
            } => {
                let alphabet_size = 256;
                let value = collection_value(index, *min_size, alphabet_size, |i| i as u8);
                ChoiceValue::Bytes(value)
            }
            ChoiceConstraints::String {
                min_size,
                max_size: _,
                intervals,
            } => {
                let value = collection_value(index, *min_size, intervals.len(), |i| {
                    intervals.char_in_shrink_order(i as usize)
                });
                ChoiceValue::String(value.into_iter().collect())
            }
        }
    }

    pub fn choice_to_index(value: &ChoiceValue, c: &ChoiceConstraints) -> u128 {
        match (value, c) {
            (ChoiceValue::Boolean(v), ChoiceConstraints::Boolean { p }) => {
                if *p <= 2.0_f64.powi(-64) {
                    assert!(*v == false);
                    0
                } else if *p >= 1.0 - 2.0_f64.powi(-64) {
                    assert!(*v == true);
                    0
                } else {
                    if *v == false { 0 } else { 1 }
                }
            }
            (
                ChoiceValue::Integer(v),
                ChoiceConstraints::Integer {
                    min_value,
                    max_value,
                    weights: _,
                    shrink_towards,
                },
            ) => {
                let shrink_towards = shrink_towards
                    .max(min_value.as_ref().unwrap_or(&i128::MIN))
                    .min(max_value.as_ref().unwrap_or(&i128::MAX));

                match (min_value, max_value) {
                    (None, None) => zigzag_index(*v, *shrink_towards),
                    (Some(min), None) => {
                        if (*v - shrink_towards).abs() <= (shrink_towards - min).abs() {
                            zigzag_index(*v, *shrink_towards)
                        } else {
                            (*v - min) as u128
                        }
                    }
                    (None, Some(max)) => {
                        if (*v - shrink_towards).abs() <= (max - shrink_towards).abs() {
                            zigzag_index(*v, *shrink_towards)
                        } else {
                            (max - *v) as u128
                        }
                    }
                    (Some(min), Some(max)) => {
                        // TODO: Weights check
                        if (shrink_towards - min) < (max - shrink_towards) {
                            if (*v - shrink_towards).abs() <= (shrink_towards - min).abs() {
                                zigzag_index(*v, *shrink_towards)
                            } else {
                                (*v - min) as u128
                            }
                        } else {
                            if (*v - shrink_towards).abs() <= (shrink_towards - min).abs() {
                                zigzag_index(*v, *shrink_towards)
                            } else {
                                (max - *v) as u128
                            }
                        }
                    }
                }
            }
            (ChoiceValue::Float(v), ChoiceConstraints::Float { .. }) => {
                let sign = if *v < 0.0 { 1 } else { 0 };
                (sign << 64) | float_to_lex(v.abs()) as u128
            }
            (ChoiceValue::Bytes(v), ChoiceConstraints::Bytes { min_size, .. }) => {
                collection_index(v, *min_size, 256, |b| *b as u128)
            }
            (ChoiceValue::String(v), ChoiceConstraints::String { intervals, .. }) => {
                collection_index(
                    &v.chars().collect::<Vec<_>>(),
                    v.len(),
                    intervals.len(),
                    |c| intervals.index_from_char_in_shrink_order(*c) as u128,
                )
            }
            (_, _) => panic!("Mismatched choice value and constraints"),
        }
    }

    pub fn choice_permitted(value: &ChoiceValue, c: &ChoiceConstraints) -> bool {
        match (value, c) {
            (ChoiceValue::Boolean(v), ChoiceConstraints::Boolean { p }) => {
                if *p <= 0.0 {
                    *v == false
                } else if *p >= 1.0 {
                    *v == true
                } else {
                    true
                }
            }
            (
                ChoiceValue::Integer(v),
                ChoiceConstraints::Integer {
                    min_value,
                    max_value,
                    ..
                },
            ) => {
                if let Some(min) = min_value {
                    if v < min {
                        return false;
                    }
                }
                if let Some(max) = max_value {
                    if v > max {
                        return false;
                    }
                }
                true
            }
            (
                ChoiceValue::Float(v),
                ChoiceConstraints::Float {
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                },
            ) => choice_permitted_float(
                *v,
                *min_value,
                *max_value,
                *allow_nan,
                *smallest_nonzero_magnitude,
            ),
            (ChoiceValue::Bytes(v), ChoiceConstraints::Bytes { min_size, max_size }) => {
                v.len() >= *min_size && v.len() <= *max_size
            }
            (
                ChoiceValue::String(v),
                ChoiceConstraints::String {
                    min_size,
                    max_size,
                    intervals,
                },
            ) => {
                if v.len() < *min_size || v.len() > *max_size {
                    return false;
                }
                for c in v.chars() {
                    if !intervals.contains_char(c) {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }
}

fn size_to_index(size: usize, alphabet_size: usize) -> u128 {
    ((alphabet_size.pow(size as u32) - 1) / (alphabet_size - 1)) as u128
}

fn index_to_size(index: u128, alphabet_size: usize) -> usize {
    if alphabet_size == 1 {
        return index as usize;
    }

    let mut total = index * (alphabet_size as u128 - 1) + 1;
    let size = (total as f64).log(alphabet_size as f64);

    if 0.0 < size.ceil() - (size as f64) && size.ceil() - size < 1e-7 {
        let mut s = 0;
        while total >= alphabet_size as u128 {
            total /= alphabet_size as u128;
            s += 1;
        }
        s
    } else {
        size.floor() as usize
    }
}

fn collection_value<T>(
    mut index: u128,
    min_size: usize,
    alphabet_size: usize,
    from_ordering: impl Fn(u128) -> T,
) -> Vec<T> {
    index += size_to_index(min_size, alphabet_size);
    let size = index_to_size(index, alphabet_size);

    if size >= BUFFER_SIZE {
        panic!(
            "Collection size {} exceeds maximum supported size {}",
            size, BUFFER_SIZE
        );
    }

    index -= size_to_index(size, alphabet_size);
    let mut vals = Vec::with_capacity(size);

    for i in (0..size).rev() {
        if index == 0 {
            vals.push(from_ordering(0));
        } else {
            let n = index / (alphabet_size.pow(i as u32) as u128);
            vals.push(from_ordering(n));
            index -= n * alphabet_size.pow(i as u32) as u128;
        }
    }

    vals
}

fn collection_index<T>(
    value: &[T],
    min_size: usize,
    alphabet_size: usize,
    to_ordering: impl Fn(&T) -> u128,
) -> u128 {
    let mut index =
        size_to_index(value.len(), alphabet_size) - size_to_index(min_size, alphabet_size);
    let mut running_exp = 1;
    for c in value.iter().rev() {
        index += to_ordering(c) * running_exp;
        running_exp *= alphabet_size as u128;
    }
    index
}
