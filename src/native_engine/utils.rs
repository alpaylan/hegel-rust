use sha2::Digest as _;

use crate::{
    native_engine::{data::ConjectureData, floats::next_up},
    test_case::labels,
};

pub(crate) fn zigzag_value(index: u128, shrink_towards: i128) -> i128 {
    let n = index.saturating_add(1) as i128 / 2;
    let delta = if (index & 1) == 0 { -n } else { n };
    shrink_towards + delta as i128
}

pub(crate) fn zigzag_index(value: i128, shrink_towards: i128) -> u128 {
    let mut index = (shrink_towards - value).unsigned_abs().saturating_mul(2);
    if value > shrink_towards {
        index = index.saturating_sub(1);
    }
    index
}

pub struct Many {
    pub(crate) min_size: usize,
    pub(crate) max_size: usize,
    pub(crate) average_size: usize,
    pub(crate) forced_size: Option<usize>,
    pub(crate) observe: bool,
    p_continue: f64,
    count: usize,
    rejections: usize,
    drawn: bool,
    force_stop: bool,
    rejected: bool,
}

impl Many {
    pub fn new(
        min_size: usize,
        max_size: usize,
        // TODO: `average_size` in Hypothesis has the type `int | float`, but I think keeping it to usize is probably fine for now.
        average_size: usize,
        forced_size: Option<usize>,
        observe: bool,
    ) -> Self {
        Self {
            min_size,
            max_size,
            average_size,
            forced_size,
            observe,
            p_continue: Self::calc_p_continue(
                (average_size - min_size) as f64,
                (max_size - min_size) as f64,
            ),
            count: 0,
            rejections: 0,
            drawn: false,
            force_stop: false,
            rejected: false,
        }
    }

    pub fn stop_span(&mut self, cd: &mut ConjectureData) {
        if self.observe {
            cd.stop_span();
        }
    }

    pub fn start_span(&mut self, cd: &mut ConjectureData, label: usize) {
        if self.observe {
            cd.start_span(label);
        }
    }

    pub fn more(&mut self, cd: &mut ConjectureData) -> bool {
        if self.drawn {
            self.stop_span(cd);
        }

        self.drawn = true;
        self.rejected = false;

        self.start_span(cd, Self::calc_label_from_name("one more from many()"));

        let should_continue = if self.min_size == self.max_size {
            self.count < self.min_size
        } else {
            let forced_result = if self.force_stop {
                Some(false)
            } else if self.count < self.min_size {
                Some(true)
            } else if let Some(forced_size) = self.forced_size {
                Some(self.count < forced_size)
            } else {
                None
            };
            cd.draw_boolean(self.p_continue, forced_result, self.observe)
        };

        if should_continue {
            self.count += 1;
            true
        } else {
            self.stop_span(cd);
            false
        }
    }

    fn calc_p_continue(desired_avg: f64, max_size: f64) -> f64 {
        if desired_avg == max_size {
            return 1.0;
        }

        let mut p_continue = 1.0 - 1.0 / (1.0 + desired_avg);

        if p_continue == 0.0 || max_size == f64::INFINITY {
            return p_continue;
        }

        while Self::p_continue_to_avg(p_continue, max_size) > desired_avg {
            p_continue -= 0.0001;

            if p_continue < next_up(0.0, 64) {
                p_continue = next_up(0.0, 64);
                break;
            }
        }

        let mut hi = 1.0;

        while desired_avg - Self::p_continue_to_avg(p_continue, max_size) > 0.01 {
            let mut mid = p_continue.midpoint(hi);
            if Self::p_continue_to_avg(mid, max_size) <= desired_avg {
                p_continue = mid;
            } else {
                hi = mid;
            }
        }

        p_continue
    }

    fn p_continue_to_avg(p_continue: f64, max_size: f64) -> f64 {
        if p_continue >= 1.0 {
            return max_size;
        }

        (1.0 / (1.0 - p_continue) - 1.0) * (1.0 - p_continue.powf(max_size))
    }

    fn calc_label_from_name(name: &str) -> usize {
        let hash = sha2::Sha384::digest(name.as_bytes());
        usize::from_ne_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
        ])
    }
}

pub struct Sampler {
    observe: bool,
    table: Vec<(usize, usize, f64)>,
}

impl Sampler {
    pub fn new(weights: Vec<f64>, observe: bool) -> Self {
        let table = todo!();
        Self { observe, table }
    }

    pub fn sample(&mut self, cd: &mut ConjectureData, forced: Option<usize>) -> usize {
        todo!()
    }

    pub fn int_sizes_sampler() -> Self {
        Self::new(vec![4.0, 8.0, 1.0, 1.0, 0.5], false)
    }
}
