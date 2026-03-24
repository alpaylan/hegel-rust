mod composite;
mod enum_gen;
mod hegel_test;
mod stateful;
mod struct_gen;
mod utils;

use proc_macro::TokenStream;
use syn::{Data, DeriveInput, ItemFn, ItemImpl, parse_macro_input};

// docs are in hegel's lib.rs so that we get intra-doc links
#[proc_macro_derive(DefaultGenerator)]
pub fn derive_generator(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match &input.data {
        Data::Struct(data) => struct_gen::derive_struct_generator(&input, data),
        Data::Enum(data) => enum_gen::derive_enum_generator(&input, data),
        Data::Union(_) => syn::Error::new_spanned(&input, "Generator cannot be derived for unions")
            .to_compile_error()
            .into(),
    }
}

// docs are in hegel's lib.rs so that we get intra-doc links
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    hegel_test::expand_test(attr.into(), item.into()).into()
}

/// Define a composite generator from a function.
///
/// The first parameter must be `tc: TestCase` and is passed automatically
/// when the generator is drawn. Any additional parameters become parameters
/// of the returned factory function. The function must have an explicit
/// return type.
///
/// ```ignore
/// use hegel::generators;
///
/// #[hegel::composite]
/// fn sorted_vec(tc: hegel::TestCase, min_len: usize) -> Vec<i32> {
///     let mut v: Vec<i32> = tc.draw(generators::vecs(generators::integers()).min_size(min_len));
///     v.sort();
///     v
/// }
///
/// #[hegel::test]
/// fn test_sorted(tc: hegel::TestCase) {
///     let v = tc.draw(sorted_vec(3));
///     assert!(v.len() >= 3);
///     assert!(v.windows(2).all(|w| w[0] <= w[1]));
/// }
/// ```
#[proc_macro_attribute]
pub fn composite(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    composite::expand_composite(input).into()
}

#[proc_macro_attribute]
pub fn state_machine(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let block = parse_macro_input!(item as ItemImpl);
    stateful::expand_state_machine(block).into()
}
