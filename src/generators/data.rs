use ciborium::Value;

use crate::cbor_utils::{cbor_map, map_insert};

use std::cell::{Cell, RefCell};
use std::sync::{Arc, LazyLock};

use crate::protocol::{Channel, Connection};
use crate::runner::Verbosity;

use super::value;

static PROTOCOL_DEBUG: LazyLock<bool> = LazyLock::new(|| {
    matches!(
        std::env::var("HEGEL_PROTOCOL_DEBUG")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true"
    )
});

/// Per-test-case state, consolidating all thread-local state into one struct.
///
/// This is an internal implementation detail. Do not use directly.
#[doc(hidden)]
pub struct TestCaseData {
    #[allow(dead_code)]
    connection: Arc<Connection>,
    channel: Channel,
    span_depth: Cell<usize>,
    verbosity: Verbosity,
    is_last_run: bool,
    pub(crate) output: RefCell<Vec<String>>,
    draw_count: Cell<usize>,
    test_aborted: Cell<bool>,
    in_composite: Cell<bool>,
}

impl TestCaseData {
    pub(crate) fn new(
        connection: Arc<Connection>,
        channel: Channel,
        verbosity: Verbosity,
        is_last_run: bool,
    ) -> Self {
        TestCaseData {
            connection,
            channel,
            span_depth: Cell::new(0),
            verbosity,
            is_last_run,
            output: RefCell::new(Vec::new()),
            draw_count: Cell::new(0),
            test_aborted: Cell::new(false),
            in_composite: Cell::new(false),
        }
    }

    pub(crate) fn is_last_run(&self) -> bool {
        self.is_last_run
    }

    pub(crate) fn test_aborted(&self) -> bool {
        self.test_aborted.get()
    }

    pub(crate) fn set_test_aborted(&self, val: bool) {
        self.test_aborted.set(val);
    }

    pub(crate) fn record_draw<T: std::fmt::Debug>(&self, value: &T) {
        if !self.is_last_run {
            return;
        }
        let n = self.draw_count.get() + 1;
        self.draw_count.set(n);
        self.output
            .borrow_mut()
            .push(format!("Draw {}: {:?}", n, value));
    }

    #[doc(hidden)]
    pub fn in_composite(&self) -> bool {
        self.in_composite.get()
    }

    #[doc(hidden)]
    pub fn set_in_composite(&self, val: bool) {
        self.in_composite.set(val);
    }

    fn increment_span_depth(&self) {
        self.span_depth.set(self.span_depth.get() + 1);
    }

    fn decrement_span_depth(&self) {
        let depth = self.span_depth.get();
        assert!(depth > 0, "stop_span called with no open spans");
        self.span_depth.set(depth - 1);
    }

    pub(crate) fn channel(&self) -> &Channel {
        &self.channel
    }

    fn verbosity(&self) -> Verbosity {
        self.verbosity
    }

    fn start_span(&self, label: u64) {
        self.increment_span_depth();
        if let Err(StopTestError) = self.send_request("start_span", &cbor_map! {"label" => label}) {
            self.decrement_span_depth();
            crate::assume(false);
        }
    }

    fn stop_span(&self, discard: bool) {
        self.decrement_span_depth();
        // Ignore StopTest errors from stop_span - we're already closing
        let _ = self.send_request("stop_span", &cbor_map! {"discard" => discard});
    }

    /// Send a request and receive a response via the channel.
    /// Returns Err(StopTestError) if the server sends an overflow error.
    pub(super) fn send_request(
        &self,
        command: &str,
        payload: &Value,
    ) -> Result<Value, StopTestError> {
        let debug = *PROTOCOL_DEBUG || self.verbosity() == Verbosity::Debug;

        // Build the request message by merging command into the payload map
        let mut entries = vec![(
            Value::Text("command".to_string()),
            Value::Text(command.to_string()),
        )];

        // Merge payload fields into the request
        if let Value::Map(map) = payload {
            for (k, v) in map {
                entries.push((k.clone(), v.clone()));
            }
        }

        let request = Value::Map(entries);

        if debug {
            eprintln!("REQUEST: {:?}", request);
        }

        let result = self.channel().request_cbor(&request);

        match result {
            Ok(response) => {
                if debug {
                    eprintln!("RESPONSE: {:?}", response);
                }
                Ok(response)
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("overflow") || error_msg.contains("StopTest") {
                    if debug {
                        eprintln!("RESPONSE: StopTest/overflow");
                    }
                    // Mark test as aborted so the runner skips sending mark_complete
                    // (the server has already moved on from this test case)
                    self.set_test_aborted(true);
                    Err(StopTestError)
                } else {
                    panic!("Failed to communicate with Hegel: {}", e);
                }
            }
        }
    }

    /// Send a schema to the server and return the raw CBOR response.
    ///
    /// This is the core generation primitive. It handles StopTest errors
    /// by calling `assume(false)` to mark the test case as invalid.
    pub fn generate_raw(&self, schema: &Value) -> Value {
        match self.send_request("generate", &cbor_map! {"schema" => schema.clone()}) {
            Ok(v) => v,
            Err(StopTestError) => {
                crate::assume(false);
                unreachable!()
            }
        }
    }

    /// Generate a value from a schema, deserializing the result.
    pub fn generate_from_schema<T: serde::de::DeserializeOwned>(&self, schema: &Value) -> T {
        deserialize_value(self.generate_raw(schema))
    }

    /// Run a function within a labeled span group.
    ///
    /// Groups related generation calls together, which helps the testing engine
    /// understand the structure of generated data and improve shrinking.
    pub fn span_group<T, F: FnOnce() -> T>(&self, label: u64, f: F) -> T {
        self.start_span(label);
        let result = f();
        self.stop_span(false);
        result
    }

    /// Run a function within a labeled span group, discarding if the function returns None.
    ///
    /// Useful for filter-like operations where rejected values should be discarded.
    pub fn discardable_span_group<T, F: FnOnce() -> Option<T>>(
        &self,
        label: u64,
        f: F,
    ) -> Option<T> {
        self.start_span(label);
        let result = f();
        self.stop_span(result.is_none());
        result
    }
}

/// Custom error for StopTest (overflow) condition.
#[derive(Debug)]
pub struct StopTestError;

impl std::fmt::Display for StopTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server ran out of data (StopTest)")
    }
}

impl std::error::Error for StopTestError {}

/// Deserialize a raw CBOR value into a Rust type.
///
/// This is a public helper for use by derived generators (proc macros)
/// that need to deserialize individual field values from CBOR.
pub fn deserialize_value<T: serde::de::DeserializeOwned>(raw: Value) -> T {
    let hv = value::HegelValue::from(raw.clone());
    value::from_hegel_value(hv).unwrap_or_else(|e| {
        panic!("Failed to deserialize value: {}\nValue: {:?}", e, raw);
    })
}

/// Label constants for spans.
/// These help Hypothesis understand the structure of generated data.
#[doc(hidden)]
pub mod labels {
    pub const LIST: u64 = 1;
    pub const LIST_ELEMENT: u64 = 2;
    pub const SET: u64 = 3;
    pub const SET_ELEMENT: u64 = 4;
    pub const MAP: u64 = 5;
    pub const MAP_ENTRY: u64 = 6;
    pub const TUPLE: u64 = 7;
    pub const ONE_OF: u64 = 8;
    pub const OPTIONAL: u64 = 9;
    pub const FIXED_DICT: u64 = 10;
    pub const FLAT_MAP: u64 = 11;
    pub const FILTER: u64 = 12;
    /// For .map() transformations (distinct from MAP which is for collections)
    pub const MAPPED: u64 = 13;
    pub const SAMPLED_FROM: u64 = 14;
    pub const ENUM_VARIANT: u64 = 15;
}

/// Uses the hegel server to determine collection sizing.
///
///  The server-side `many` object is created lazily on the first call to
/// [`more()`](Collection::more).
///
/// # Example
///
/// ```ignore
/// use hegel::generators::Collection;
///
/// let data = hegel::generators::test_case_data();
/// let mut coll = Collection::new(data, "my_list", 0, None);
/// let mut result = Vec::new();
/// while coll.more() {
///     result.push(generators::integers::<i32>().do_draw(data));
/// }
/// ```
pub struct Collection<'a> {
    data: &'a TestCaseData,
    base_name: String,
    min_size: usize,
    max_size: Option<usize>,
    server_name: Option<String>,
    finished: bool,
}

impl<'a> Collection<'a> {
    /// Create a new collection handle.
    ///
    /// The server-side `many` object is not created until the first call
    /// to [`more()`](Collection::more), matching the Python SDK's lazy
    /// initialization behavior.
    pub fn new(
        data: &'a TestCaseData,
        name: &str,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Self {
        Collection {
            data,
            base_name: name.to_string(),
            min_size,
            max_size,
            server_name: None,
            finished: false,
        }
    }

    /// Ensure the server-side collection is initialized, returning the server name.
    fn ensure_initialized(&mut self) -> &str {
        if self.server_name.is_none() {
            let mut payload = cbor_map! {
                "name" => self.base_name.as_str(),
                "min_size" => self.min_size as u64
            };
            if let Some(max) = self.max_size {
                map_insert(&mut payload, "max_size", Value::from(max as u64));
            }
            let response = match self.data.send_request("new_collection", &payload) {
                Ok(v) => v,
                Err(StopTestError) => {
                    crate::assume(false);
                    unreachable!()
                }
            };
            let name = match response {
                Value::Text(s) => s,
                _ => panic!(
                    "Expected text response from new_collection, got {:?}",
                    response
                ),
            };
            self.server_name = Some(name);
        }
        self.server_name.as_ref().unwrap()
    }

    /// Check if more elements should be generated.
    ///
    /// On the first call, this lazily creates the server-side collection.
    /// Returns `false` when the collection has reached its target size.
    pub fn more(&mut self) -> bool {
        if self.finished {
            return false;
        }
        let server_name = self.ensure_initialized().to_string();
        let response = match self.data.send_request(
            "collection_more",
            &cbor_map! { "collection" => server_name.as_str() },
        ) {
            Ok(v) => v,
            Err(StopTestError) => {
                self.finished = true;
                crate::assume(false);
                unreachable!()
            }
        };
        let result = match response {
            Value::Bool(b) => b,
            _ => panic!("Expected bool from collection_more, got {:?}", response),
        };
        if !result {
            self.finished = true;
        }
        result
    }

    /// Reject the last element (don't count it towards the size budget).
    ///
    /// This is useful for unique collections where a generated element
    /// turned out to be a duplicate.
    pub fn reject(&mut self, why: Option<&str>) {
        if self.finished {
            return;
        }
        let server_name = self.ensure_initialized().to_string();
        let mut payload = cbor_map! {
            "collection" => server_name.as_str()
        };
        if let Some(reason) = why {
            map_insert(&mut payload, "why", Value::Text(reason.to_string()));
        }
        let _ = self.data.send_request("collection_reject", &payload);
    }
}
