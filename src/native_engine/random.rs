pub struct Random;

impl Random {
    pub fn random(&mut self) -> f64 {
        todo!()
    }

    pub fn random_int(&mut self, min: i128, max: i128) -> i128 {
        todo!()
    }

    pub fn get_rand_bits(&mut self, bits: usize) -> i128 {
        todo!()
    }

    pub fn random_bytes(&mut self, n: usize) -> Vec<u8> {
        todo!()
    }

    pub fn random_byte(&mut self) -> u8 {
        todo!()
    }

    pub fn choice<T>(&mut self, _choices: &[T]) -> T
    where
        T: Clone,
    {
        todo!()
    }
}
