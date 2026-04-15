use sha2::Digest as _;

use crate::native_engine::{floats::next_up, random::Random};

pub struct Span {}

pub struct ConjectureData {
    random: Random,
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
}
