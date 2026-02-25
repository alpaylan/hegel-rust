//! Hegel is a property-based testing framework for Rust.
//!
//! # Quick Start
//!
//! ```no_run
//! use hegel::gen;
//!
//! #[test]
//! fn test_addition_commutative() {
//!     hegel::hegel(|| {
//!         let x = hegel::draw(&gen::integers::<i32>());
//!         let y = hegel::draw(&gen::integers::<i32>());
//!         assert_eq!(x + y, y + x);
//!     });
//! }
//! ```
//!
//! # Configuration
//!
//! Use the builder pattern for more control:
//!
//! ```no_run
//! use hegel::{Hegel, Verbosity};
//! use hegel::gen;
//!
//! #[test]
//! fn test_with_options() {
//!     Hegel::new(|| {
//!         let n = hegel::draw(&gen::integers::<i32>());
//!         assert!(n + 0 == n);
//!     })
//!     .test_cases(500)
//!     .verbosity(Verbosity::Verbose)
//!     .run();
//! }
//! ```
//!
//! # Generators
//!
//! All generators implement [`gen::Generate<T>`] and are created via factory functions
//! in the [`gen`] module.
//!
//! ## Primitives
//!
//! ```no_run
//! use hegel::gen;
//!
//! # hegel::hegel(|| {
//! let _: () = hegel::draw(&gen::unit());
//! let b: bool = hegel::draw(&gen::booleans());
//! let n: i32 = hegel::draw(&gen::just(42));  // constant with schema
//! # });
//! ```
//!
//! ## Numbers
//!
//! ```no_run
//! use hegel::gen;
//!
//! # hegel::hegel(|| {
//! // Integers - bounds default to type limits
//! let i: i32 = hegel::draw(&gen::integers::<i32>());
//! let bounded: i32 = hegel::draw(&gen::integers().with_min(0).with_max(100));
//!
//! // Floating point
//! let f: f64 = hegel::draw(&gen::floats::<f64>());
//! let bounded: f64 = hegel::draw(&gen::floats()
//!     .with_min(0.0)
//!     .with_max(1.0)
//!     .exclude_min()
//!     .exclude_max());
//! # });
//! ```
//!
//! ## Strings
//!
//! ```no_run
//! use hegel::gen;
//!
//! # hegel::hegel(|| {
//! let s: String = hegel::draw(&gen::text());
//! let bounded: String = hegel::draw(&gen::text().with_min_size(1).with_max_size(100));
//!
//! // Regex patterns (auto-anchored)
//! let pattern: String = hegel::draw(&gen::from_regex(r"[a-z]{3}-[0-9]{3}"));
//!
//! // Format strings
//! let email: String = hegel::draw(&gen::emails());
//! let url: String = hegel::draw(&gen::urls());
//! let ip: String = hegel::draw(&gen::ip_addresses().v4());
//! let date: String = hegel::draw(&gen::dates());  // YYYY-MM-DD
//! # });
//! ```
//!
//! ## Collections
//!
//! ```no_run
//! use hegel::gen;
//! use std::collections::{HashSet, HashMap};
//!
//! # hegel::hegel(|| {
//! let vec: Vec<i32> = hegel::draw(&gen::vecs(gen::integers()).with_min_size(1));
//! let set: HashSet<i32> = hegel::draw(&gen::hashsets(gen::integers()));
//! let map: HashMap<String, i32> = hegel::draw(&gen::hashmaps(gen::text(), gen::integers()));
//! # });
//! ```
//!
//! ## Combinators
//!
//! ```no_run
//! use hegel::gen;
//!
//! # hegel::hegel(|| {
//! // Sample from a fixed set
//! let color: &str = hegel::draw(&gen::sampled_from(vec!["red", "green", "blue"]));
//!
//! // Choose from multiple generators
//! let n: i32 = hegel::draw(&hegel::one_of!(
//!     gen::integers::<i32>().with_min(0).with_max(10),
//!     gen::integers::<i32>().with_min(100).with_max(110),
//! ));
//!
//! // Optional values
//! let opt: Option<i32> = hegel::draw(&gen::optional(gen::integers()));
//! # });
//! ```
//!
//! ## Transformations
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! // Transform values
//! let squared: i32 = hegel::draw(&gen::integers::<i32>()
//!     .with_min(1)
//!     .with_max(10)
//!     .map(|x| x * x));
//!
//! // Filter values
//! let even: i32 = hegel::draw(&gen::integers::<i32>()
//!     .filter(|x| x % 2 == 0));
//!
//! // Dependent generation
//! let sized: String = hegel::draw(&gen::integers::<usize>()
//!     .with_min(1)
//!     .with_max(10)
//!     .flat_map(|len| gen::text().with_min_size(len).with_max_size(len)));
//! # });
//! ```
//!
//! # Deriving Generators
//!
//! Use `#[derive(Generate)]` to automatically create generators for structs and enums,
//! then use [`gen::from_type`] to get a generator:
//!
//! ```no_run
//! use hegel::Generate;
//! use hegel::gen;
//!
//! #[derive(Generate, Debug)]
//! struct Person {
//!     name: String,
//!     age: u32,
//! }
//!
//! # hegel::hegel(|| {
//! // Generate with defaults
//! let person: Person = hegel::draw(&gen::from_type::<Person>());
//!
//! // Customize field generators
//! let person: Person = hegel::draw(&gen::from_type::<Person>()
//!     .with_age(gen::integers().with_min(0).with_max(120)));
//! # });
//! ```
//!
//! For external types, use [`derive_generator!`]:
//!
//! ```ignore
//! use hegel::derive_generator;
//! use hegel::gen;
//!
//! derive_generator!(Point { x: f64, y: f64 });
//!
//! let point: Point = hegel::draw(&gen::from_type::<Point>());
//! ```
//!
//! # Assumptions
//!
//! Use [`assume`] to reject invalid test inputs:
//!
//! ```no_run
//! use hegel::gen;
//!
//! # hegel::hegel(|| {
//! let age: u32 = hegel::draw(&gen::integers());
//! hegel::assume(age >= 18);
//! // Test logic for adults only...
//! # });
//! ```
//!
//! # Feature Flags
//!
//! - **`rand`**: Enables [`gen::randoms()`] for generating random number generators
//!   that implement [`rand::RngCore`].
//!
//! # Debugging
//!
//! Set verbosity to [`Verbosity::Debug`] to enable debug logging of requests/responses.

pub(crate) mod cbor_helpers;
pub mod gen;
pub(crate) mod protocol;
pub(crate) mod runner;

pub use gen::draw;
pub use gen::Generate;

// Re-export for macro use
#[doc(hidden)]
pub use ciborium;
#[doc(hidden)]
pub use paste;

// re-export public api
pub use hegel_derive::Generate;
pub use runner::{hegel, Hegel, Verbosity};

/// Note a message which will be displayed with the reported failing test case.
pub fn note(message: &str) {
    gen::note(message)
}

/// Assume a condition is true. If false, reject the current test input.
pub fn assume(condition: bool) {
    if !condition {
        panic!("{}", runner::ASSUME_FAIL_STRING);
    }
}
