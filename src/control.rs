use std::cell::RefCell;

use crate::test_case::TestCase;

thread_local! {
    static CURRENT_TEST_CONTEXT: RefCell<Option<TestCase>> = const { RefCell::new(None) };
}

pub(crate) fn with_test_context<R>(tc: &TestCase, f: impl FnOnce() -> R) -> R {
    CURRENT_TEST_CONTEXT.with(|c| c.borrow_mut().replace(tc.clone()));
    let result = f();
    CURRENT_TEST_CONTEXT.with(|c| *c.borrow_mut() = None);
    result
}

/// Returns `true` if we are currently inside a Hegel test context.
///
/// This can be used to conditionally execute code that depends on a
/// live test case (e.g., generating values, recording notes).
///
/// # Example
///
/// ```no_run
/// if hegel::currently_in_test_context() {
///     // inside a test
/// }
/// ```
pub fn currently_in_test_context() -> bool {
    CURRENT_TEST_CONTEXT.with(|c| c.borrow().is_some())
}
