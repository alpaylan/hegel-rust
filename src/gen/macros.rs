//! Declarative macro for deriving generators for external types.

/// Derive a generator for a struct type defined externally.
///
/// This macro creates a hidden generator struct with builder methods for each field,
/// and implements [`DefaultGenerator`](crate::gen::DefaultGenerator) for the type
/// so it can be used with [`from_type`](crate::gen::from_type).
///
/// # Example
///
/// ```ignore
/// // In your production crate (no hegel dependency needed):
/// pub struct Person {
///     pub name: String,
///     pub age: u32,
/// }
///
/// // In your test crate:
/// use hegel::derive_generator;
/// use hegel::gen::{self, Generate};
/// use production_crate::Person;
///
/// derive_generator!(Person {
///     name: String,
///     age: u32,
/// });
///
/// // Use from_type to get a generator:
/// let gen = gen::from_type::<Person>()
///     .with_name(gen::from_regex("[A-Z][a-z]+"))
///     .with_age(gen::integers::<u32>().with_min(0).with_max(120));
///
/// let person: Person = hegel::draw(&gen);
/// ```
#[macro_export]
macro_rules! derive_generator {
    ($struct_name:ident { $($field_name:ident : $field_type:ty),* $(,)? }) => {
        const _: () = {
            $crate::paste::paste! {
                pub struct [<$struct_name Generator>]<'a> {
                    $(
                        $field_name: $crate::gen::BoxedGenerator<'a, $field_type>,
                    )*
                }

                impl<'a> [<$struct_name Generator>]<'a> {
                    pub fn new() -> Self
                    where
                        $($field_type: $crate::gen::DefaultGenerator,)*
                        $(<$field_type as $crate::gen::DefaultGenerator>::Generator: Send + Sync + 'a,)*
                    {
                        use $crate::gen::{DefaultGenerator, Generate};
                        Self {
                            $($field_name: <$field_type as DefaultGenerator>::default_generator().boxed(),)*
                        }
                    }

                    $(
                        pub fn [<with_ $field_name>]<G>(mut self, gen: G) -> Self
                        where
                            G: $crate::gen::Generate<$field_type> + Send + Sync + 'a,
                        {
                            use $crate::gen::Generate;
                            self.$field_name = gen.boxed();
                            self
                        }
                    )*
                }

                impl<'a> Default for [<$struct_name Generator>]<'a>
                where
                    $($field_type: $crate::gen::DefaultGenerator,)*
                    $(<$field_type as $crate::gen::DefaultGenerator>::Generator: Send + Sync + 'a,)*
                {
                    fn default() -> Self {
                        Self::new()
                    }
                }

                impl<'a> $crate::gen::Generate<$struct_name> for [<$struct_name Generator>]<'a> {
                    fn do_generate(&self) -> $struct_name {
                        use $crate::gen::Generate;
                        $struct_name {
                            $($field_name: self.$field_name.do_generate(),)*
                        }
                    }
                }

                impl $crate::gen::DefaultGenerator for $struct_name
                where
                    $($field_type: $crate::gen::DefaultGenerator,)*
                    $(<$field_type as $crate::gen::DefaultGenerator>::Generator: Send + Sync + 'static,)*
                {
                    type Generator = [<$struct_name Generator>]<'static>;
                    fn default_generator() -> Self::Generator {
                        [<$struct_name Generator>]::new()
                    }
                }
            }
        };
    };
}
