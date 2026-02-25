mod common;

use common::utils::assert_all_examples;
use hegel::gen::{self, Generate};

#[test]
fn test_compose_basic() {
    hegel::hegel(|| {
        let value = hegel::draw(&hegel::compose!(|draw| {
            draw(&gen::integers::<i32>().with_min(0).with_max(100))
        }));
        assert!((0..=100).contains(&value));
    });
}

#[test]
fn test_compose_dependent_generation() {
    hegel::hegel(|| {
        let (x, y) = hegel::draw(&hegel::compose!(|draw| {
            let x = draw(&gen::integers::<i32>().with_min(0).with_max(50));
            let y = draw(&gen::integers::<i32>().with_min(x).with_max(100));
            (x, y)
        }));
        assert!(y >= x);
        assert!((0..=50).contains(&x));
        assert!((0..=100).contains(&y));
    });
}

#[test]
fn test_compose_with_map() {
    hegel::hegel(|| {
        let value = hegel::draw(
            &hegel::compose!(|draw| { draw(&gen::integers::<i32>().with_min(0).with_max(10)) })
                .map(|n| n * 2),
        );
        assert!(value % 2 == 0);
        assert!((0..=20).contains(&value));
    });
}

#[test]
fn test_compose_with_filter() {
    hegel::hegel(|| {
        let value = hegel::draw(
            &hegel::compose!(|draw| { draw(&gen::integers::<i32>().with_min(0).with_max(100)) })
                .filter(|n| n % 2 == 0),
        );
        assert!(value % 2 == 0);
    });
}

#[test]
fn test_compose_with_boxed() {
    hegel::hegel(|| {
        let gen =
            hegel::compose!(|draw| { draw(&gen::integers::<i32>().with_min(0).with_max(50)) })
                .boxed();
        let value = hegel::draw(&gen);
        assert!((0..=50).contains(&value));
    });
}

#[test]
fn test_compose_assert_all_examples() {
    assert_all_examples(
        hegel::compose!(|draw| {
            let x = draw(&gen::integers::<i32>().with_min(0).with_max(100));
            let y = draw(&gen::integers::<i32>().with_min(0).with_max(100));
            (x, y)
        }),
        |&(x, y)| (0..=100).contains(&x) && (0..=100).contains(&y),
    );
}

#[test]
fn test_compose_inside_one_of() {
    hegel::hegel(|| {
        let value: i32 = hegel::draw(&hegel::one_of!(
            hegel::compose!(|draw| { draw(&gen::integers::<i32>().with_min(0).with_max(10)) }),
            gen::integers::<i32>().with_min(100).with_max(110),
        ));
        assert!((0..=10).contains(&value) || (100..=110).contains(&value));
    });
}

#[test]
fn test_compose_list_with_index() {
    hegel::hegel(|| {
        let (list, index) = hegel::draw(&hegel::compose!(|draw| {
            let list = draw(
                &gen::vecs(gen::integers::<i32>())
                    .with_min_size(1)
                    .with_max_size(20),
            );
            let index = draw(
                &gen::integers::<usize>()
                    .with_min(0)
                    .with_max(list.len() - 1),
            );
            (list, index)
        }));
        assert!(!list.is_empty());
        assert!(index < list.len());
    });
}

#[test]
fn test_compose_string_building() {
    hegel::hegel(|| {
        let s = hegel::draw(&hegel::compose!(|draw| {
            let prefix = draw(&gen::sampled_from(vec!["hello", "world"]));
            let n = draw(&gen::integers::<i32>().with_min(0).with_max(99));
            format!("{}-{}", prefix, n)
        }));
        assert!(s.starts_with("hello-") || s.starts_with("world-"));
    });
}
