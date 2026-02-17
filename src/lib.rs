//! Hegel is a property-based testing framework for Rust.
//!
//! # Quick Start
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! #[test]
//! fn test_addition_commutative() {
//!     hegel::hegel(|| {
//!         let x = gen::integers::<i32>().generate();
//!         let y = gen::integers::<i32>().generate();
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
//! use hegel::gen::{self, Generate};
//!
//! #[test]
//! fn test_with_options() {
//!     Hegel::new(|| {
//!         let n = gen::integers::<i32>().generate();
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
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! let _: () = gen::unit().generate();
//! let b: bool = gen::booleans().generate();
//! let n: i32 = gen::just(42).generate();  // constant with schema
//! # });
//! ```
//!
//! ## Numbers
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! // Integers - bounds default to type limits
//! let i: i32 = gen::integers::<i32>().generate();
//! let bounded: i32 = gen::integers().with_min(0).with_max(100).generate();
//!
//! // Floating point
//! let f: f64 = gen::floats::<f64>().generate();
//! let bounded: f64 = gen::floats()
//!     .with_min(0.0)
//!     .with_max(1.0)
//!     .exclude_min()
//!     .exclude_max()
//!     .generate();
//! # });
//! ```
//!
//! ## Strings
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! let s: String = gen::text().generate();
//! let bounded: String = gen::text().with_min_size(1).with_max_size(100).generate();
//!
//! // Regex patterns (auto-anchored)
//! let pattern: String = gen::from_regex(r"[a-z]{3}-[0-9]{3}").generate();
//!
//! // Format strings
//! let email: String = gen::emails().generate();
//! let url: String = gen::urls().generate();
//! let ip: String = gen::ip_addresses().v4().generate();
//! let date: String = gen::dates().generate();  // YYYY-MM-DD
//! # });
//! ```
//!
//! ## Collections
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//! use std::collections::{HashSet, HashMap};
//!
//! # hegel::hegel(|| {
//! let vec: Vec<i32> = gen::vecs(gen::integers()).with_min_size(1).generate();
//! let set: HashSet<i32> = gen::hashsets(gen::integers()).generate();
//! let map: HashMap<String, i32> = gen::hashmaps(gen::text(), gen::integers()).generate();
//! # });
//! ```
//!
//! ## Combinators
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! // Sample from a fixed set
//! let color: &str = gen::sampled_from(vec!["red", "green", "blue"]).generate();
//!
//! // Choose from multiple generators
//! let n: i32 = hegel::one_of!(
//!     gen::integers::<i32>().with_min(0).with_max(10),
//!     gen::integers::<i32>().with_min(100).with_max(110),
//! ).generate();
//!
//! // Optional values
//! let opt: Option<i32> = gen::optional(gen::integers()).generate();
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
//! let squared: i32 = gen::integers::<i32>()
//!     .with_min(1)
//!     .with_max(10)
//!     .map(|x| x * x)
//!     .generate();
//!
//! // Filter values
//! let even: i32 = gen::integers::<i32>()
//!     .filter(|x| x % 2 == 0)
//!     .generate();
//!
//! // Dependent generation
//! let sized: String = gen::integers::<usize>()
//!     .with_min(1)
//!     .with_max(10)
//!     .flat_map(|len| gen::text().with_min_size(len).with_max_size(len))
//!     .generate();
//! # });
//! ```
//!
//! # Deriving Generators
//!
//! Use `#[derive(Generate)]` to automatically create generators for structs and enums:
//!
//! ```no_run
//! use hegel::Generate;
//! use hegel::gen::{self, Generate as _};
//!
//! #[derive(Generate, Debug)]
//! struct Person {
//!     name: String,
//!     age: u32,
//! }
//!
//! # hegel::hegel(|| {
//! let person: Person = PersonGenerator::new()
//!     .with_age(gen::integers().with_min(0).with_max(120))
//!     .generate();
//! # });
//! ```
//!
//! For external types, use [`derive_generator!`]:
//!
//! ```ignore
//! use hegel::derive_generator;
//!
//! derive_generator!(Point { x: f64, y: f64 });
//! ```
//!
//! # Assumptions
//!
//! Use [`assume`] to reject invalid test inputs:
//!
//! ```no_run
//! use hegel::gen::{self, Generate};
//!
//! # hegel::hegel(|| {
//! let age: u32 = gen::integers().generate();
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

pub use gen::Generate;

// Re-export for macro use
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
