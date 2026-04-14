pub struct Random;

impl Random {
    pub fn random(&mut self) -> f64 {
        todo!()
    }

    pub fn choice<T>(&mut self, _choices: &[T]) -> T
    where
        T: Clone,
    {
        todo!()
    }
}
