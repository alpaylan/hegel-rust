#![cfg(feature = "rand")]

use hegel::gen::{integers, randoms, vecs, Generate};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::Rng;

#[test]
fn test_randoms_generate() {
    hegel::hegel(|| {
        let _: bool = randoms().generate().random();

        let x: i32 = randoms().generate().random_range(1..=100);
        assert!((1..=100).contains(&x));
    });
}

#[test]
fn test_randoms_shuffle_preserves_elements() {
    hegel::hegel(|| {
        let mut rng = randoms().generate();

        let original: Vec<i32> = vecs(integers()).generate();
        let mut shuffled = original.clone();
        shuffled.shuffle(&mut rng);

        let mut sorted_original = original.clone();
        sorted_original.sort();
        shuffled.sort();
        assert_eq!(sorted_original, shuffled);
    });
}

#[test]
fn test_randoms_choose() {
    hegel::hegel(|| {
        let mut rng = randoms().generate();
        let items: Vec<i32> = vecs(integers()).with_min_size(1).generate();
        let picked = items.choose(&mut rng).unwrap();
        assert!(items.contains(picked));
    });
}

#[test]
fn test_randoms_fill() {
    hegel::hegel(|| {
        let mut rng = randoms().generate();
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);
    });
}

#[test]
fn test_true_random() {
    hegel::hegel(|| {
        let mut rng = randoms().use_true_random().generate();
        let x: i32 = rng.random_range(1..=100);
        assert!((1..=100).contains(&x));
    });
}

#[test]
fn test_randoms_composes() {
    hegel::hegel(|| {
        let _ = vecs(randoms()).generate();
    });
}

#[test]
fn test_randoms_u64() {
    hegel::hegel(|| {
        let _: u64 = randoms().generate().random();
    });
}

#[test]
fn test_true_randoms_u64() {
    hegel::hegel(|| {
        let _: u64 = randoms().use_true_random().generate().random();
    });
}

#[test]
fn test_true_randoms_fill() {
    hegel::hegel(|| {
        let mut rng = randoms().use_true_random().generate();
        let mut bytes = [0u8; 16];
        rng.fill(&mut bytes);
    });
}
