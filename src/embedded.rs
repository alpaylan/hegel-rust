use crate::gen::{
    clear_connection, set_connection, set_is_last_run, take_generated_values,
};
use crate::protocol::{
    Channel, Connection, VERSION_NEGOTIATION_MESSAGE, VERSION_NEGOTIATION_OK,
};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::os::unix::net::UnixStream;
use std::panic::{self, catch_unwind, AssertUnwindSafe};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::time::Duration;
use tempfile::TempDir;

static PANIC_HOOK_INIT: Once = Once::new();

thread_local! {
    /// Stores panic info captured by our panic hook: (thread_name, location)
    static LAST_PANIC_INFO: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
}

/// Get and clear the last panic info (thread_name, location).
fn take_panic_info() -> Option<(String, String)> {
    LAST_PANIC_INFO.with(|info| info.borrow_mut().take())
}

// Panic unconditionally prints to stderr, even if it's caught later. This results in
// messy output during shrinking. To avoid this, we replace the panic hook with our
// own that suppresses the printing except for the final replay.
//
// This is called once per process, the first time any hegel test runs.
fn init_panic_hook() {
    PANIC_HOOK_INIT.call_once(|| {
        panic::set_hook(Box::new(|info| {
            // Capture thread name and location for later use
            let thread_name = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_string();
            let location = info
                .location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "<unknown>".to_string());
            LAST_PANIC_INFO.with(|l| *l.borrow_mut() = Some((thread_name, location)));
            // Don't print anything - we'll format the output ourselves
        }));
    });
}

/// Path to the hegel binary, determined at compile time by build.rs.
/// This will be either a system hegel found on PATH, or one installed
/// into the build directory's cache.
const HEGEL_BINARY_PATH: &str = env!("HEGEL_BINARY_PATH");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Verbosity {
    Quiet,
    #[default]
    Normal,
    Verbose,
    Debug,
}

impl Verbosity {
    fn as_str(&self) -> &'static str {
        match self {
            Verbosity::Quiet => "quiet",
            Verbosity::Normal => "normal",
            Verbosity::Verbose => "verbose",
            Verbosity::Debug => "debug",
        }
    }
}

/// Special marker used to identify assume(false) panics.
const REJECT_MARKER: &str = "HEGEL_REJECT";

/// Run property-based tests using Hegel with default options.
///
/// This is a convenience function for simple cases. For configuration options,
/// use [`Hegel::new`] with the builder pattern.
///
/// # Example
///
/// ```no_run
/// use hegel::gen::{self, Generate};
///
/// hegel::hegel(|| {
///     let n = gen::integers::<i32>().generate();
///     assert!(n + 0 == n); // Identity property
/// });
/// ```
pub fn hegel<F>(test_fn: F)
where
    F: FnMut(),
{
    Hegel::new(test_fn).run();
}

/// Builder for running property-based tests with Hegel.
///
/// Use [`Hegel::new`] to create a builder, configure it with method chains,
/// then call [`run`](Hegel::run) to execute the tests.
///
/// # Example
///
/// ```no_run
/// use hegel::{Hegel, Verbosity, gen::{self, Generate}};
///
/// Hegel::new(|| {
///     let n = gen::integers::<i32>().generate();
///     assert!(n + 0 == n);
/// })
/// .test_cases(500)
/// .verbosity(Verbosity::Verbose)
/// .run();
/// ```
pub struct Hegel<F> {
    test_fn: F,
    test_cases: Option<u64>,
    verbosity: Verbosity,
    hegel_path: Option<String>,
}

impl<F> Hegel<F>
where
    F: FnMut(),
{
    /// Create a new Hegel test runner with the given test function.
    pub fn new(test_fn: F) -> Self {
        Self {
            test_fn,
            test_cases: None,
            verbosity: Verbosity::Normal,
            hegel_path: None,
        }
    }

    /// Set the number of test cases to run. Default: 100.
    pub fn test_cases(mut self, n: u64) -> Self {
        self.test_cases = Some(n);
        self
    }

    /// Set the verbosity level. Default: Normal.
    pub fn verbosity(mut self, verbosity: Verbosity) -> Self {
        self.verbosity = verbosity;
        self
    }

    /// Set the path to the hegel binary. Default: auto-detected at compile time.
    pub fn hegel_path(mut self, path: impl Into<String>) -> Self {
        self.hegel_path = Some(path.into());
        self
    }

    /// Run the property-based tests.
    ///
    /// This function:
    /// 1. Creates a Unix socket server
    /// 2. Spawns the hegeld as a subprocess
    /// 3. Accepts the connection from hegeld
    /// 4. Handles test case events from hegeld
    /// 5. Runs the test function for each test case
    /// 6. Reports results back to hegeld
    /// 7. Panics if any test case fails
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Failed to create socket or spawn hegel
    /// - Any test case fails (after shrinking)
    /// - Socket communication errors
    pub fn run(self) {
        init_panic_hook();

        // Create temp directory with socket path
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let socket_path = temp_dir.path().join("hegel.sock");

        // Build hegel command - hegeld will bind to the socket and listen
        let hegel_path = self.hegel_path.as_deref().unwrap_or(HEGEL_BINARY_PATH);
        let mut cmd = Command::new(hegel_path);
        cmd.arg(&socket_path)
            .arg("--verbosity")
            .arg(self.verbosity.as_str());

        let test_cases = self.test_cases.unwrap_or(100);
        cmd.arg("--test-cases").arg(test_cases.to_string());

        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

        if self.verbosity == Verbosity::Debug {
            eprintln!("Starting hegeld: {:?}", cmd);
        }

        let mut child = cmd.spawn().expect("Failed to spawn hegel");

        // Wait for hegeld to create the socket and start listening
        let mut attempts = 0;
        let stream = loop {
            if socket_path.exists() {
                match UnixStream::connect(&socket_path) {
                    Ok(stream) => break stream,
                    Err(e) if attempts < 50 => {
                        // Socket exists but not yet listening
                        std::thread::sleep(Duration::from_millis(100));
                        attempts += 1;
                        continue;
                    }
                    Err(e) => {
                        let _ = child.kill();
                        panic!("Failed to connect to hegeld socket: {}", e);
                    }
                }
            }
            if attempts >= 50 {
                let _ = child.kill();
                panic!("Timeout waiting for hegeld to create socket");
            }
            std::thread::sleep(Duration::from_millis(100));
            attempts += 1;
        };

        // Create connection and perform version negotiation
        let connection = Connection::new(stream);

        // Initiate version negotiation (SDK is the client)
        let control = connection.control_channel();
        let req_id = control.send_request(VERSION_NEGOTIATION_MESSAGE.to_vec())
            .expect("Failed to send version negotiation");
        let response = control.receive_response(req_id)
            .expect("Failed to receive version response");

        if response != VERSION_NEGOTIATION_OK {
            let _ = child.kill();
            panic!("Version negotiation failed: {:?}", String::from_utf8_lossy(&response));
        }

        if self.verbosity == Verbosity::Debug {
            eprintln!("Version negotiation complete");
        }

        // Run the test
        let mut test_fn = self.test_fn;
        let verbosity = self.verbosity;
        let got_interesting = Arc::new(AtomicBool::new(false));

        // Send run_test request
        let run_test_msg = json!({
            "command": "run_test",
            "name": "test",
            "test_cases": test_cases,
        });

        let pending_id = control.send_request(cbor_encode(&run_test_msg))
            .expect("Failed to send run_test");

        // Handle test_case events until test_done
        loop {
            let (event_id, event_payload) = control.receive_request()
                .expect("Failed to receive event");

            let event: Value = cbor_decode(&event_payload);
            let event_type = event.get("event").and_then(|e| e.as_str());

            if verbosity == Verbosity::Debug {
                eprintln!("Received event: {:?}", event);
            }

            match event_type {
                Some("test_case") => {
                    let channel_id = event.get("channel")
                        .and_then(|c| c.as_u64())
                        .expect("Missing channel id") as u32;
                    let is_final = event.get("is_final")
                        .and_then(|f| f.as_bool())
                        .unwrap_or(false);

                    let test_channel = connection.connect_channel(channel_id);

                    let (status, origin) = run_test_case(
                        &connection,
                        &test_channel,
                        &mut test_fn,
                        is_final,
                        verbosity,
                        &got_interesting,
                    );

                    // Send mark_complete (unless we hit overflow/StopTest)
                    if status != "OVERFLOW" {
                        let mark_complete = json!({
                            "command": "mark_complete",
                            "status": status,
                            "origin": origin,
                        });
                        let _ = test_channel.request_json(&mark_complete);
                    }

                    // Ack the test_case event
                    control.send_response(event_id, cbor_encode(&json!({"result": null})))
                        .expect("Failed to ack test_case");
                }
                Some("test_done") => {
                    // Ack the test_done event
                    control.send_response(event_id, cbor_encode(&json!({"result": null})))
                        .expect("Failed to ack test_done");
                    break;
                }
                _ => {
                    // Unknown event, just ack it
                    control.send_response(event_id, cbor_encode(&json!({"result": null})))
                        .expect("Failed to ack event");
                }
            }
        }

        // Get the run_test result
        let result_payload = control.receive_response(pending_id)
            .expect("Failed to receive run_test result");
        let result: Value = cbor_decode(&result_payload);

        if verbosity == Verbosity::Debug {
            eprintln!("Test result: {:?}", result);
        }

        let passed = result.get("passed").and_then(|p| p.as_bool()).unwrap_or(true);

        // Wait for hegeld to exit
        let _ = child.wait().expect("Failed to wait for hegel");

        if !passed || got_interesting.load(Ordering::SeqCst) {
            let failure = result.get("failure").cloned().unwrap_or(json!(null));
            let exc_type = failure.get("exc_type")
                .and_then(|e| e.as_str())
                .unwrap_or("AssertionError");
            let filename = failure.get("filename")
                .and_then(|f| f.as_str())
                .unwrap_or("");
            let lineno = failure.get("lineno")
                .and_then(|l| l.as_u64())
                .unwrap_or(0);

            panic!("Property test failed: {} at {}:{}", exc_type, filename, lineno);
        }
    }
}

/// Run a single test case.
fn run_test_case<F: FnMut()>(
    connection: &Arc<Connection>,
    test_channel: &Channel,
    test_fn: &mut F,
    is_final: bool,
    _verbosity: Verbosity,
    got_interesting: &Arc<AtomicBool>,
) -> (String, Option<Value>) {
    // Set thread-local state for this test case
    set_is_last_run(is_final);
    set_connection(Arc::clone(connection), test_channel.clone_for_embedded());

    // Run test in catch_unwind
    let result = catch_unwind(AssertUnwindSafe(test_fn));

    // Clear connection before returning (test is done generating)
    clear_connection();

    match result {
        Ok(()) => ("VALID".to_string(), None),
        Err(e) => {
            // Check if this is an assume(false) panic
            let msg = panic_message(&e);
            if msg == REJECT_MARKER {
                ("INVALID".to_string(), None)
            } else {
                got_interesting.store(true, Ordering::SeqCst);

                // Take panic info once and use for both display and origin
                let panic_info = take_panic_info();
                let (thread_name, location) = panic_info
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| ("<unknown>".to_string(), "<unknown>".to_string()));

                if is_final {
                    eprintln!("thread '{}' panicked at {}:", thread_name, location);
                    eprintln!("{}", msg);

                    for value in take_generated_values() {
                        eprintln!("{}", value);
                    }
                }

                // Extract origin info from the same panic info
                let origin = {
                    let parts: Vec<&str> = location.split(':').collect();
                    json!({
                        "exc_type": "Panic",
                        "filename": parts.first().unwrap_or(&""),
                        "lineno": parts.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
                    })
                };

                ("INTERESTING".to_string(), Some(origin))
            }
        }
    }
}

/// Extract a message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    }
}

/// Encode a JSON value to CBOR bytes using serde.
fn cbor_encode(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding failed");
    bytes
}

/// Decode CBOR bytes to a JSON value using serde.
fn cbor_decode(bytes: &[u8]) -> Value {
    ciborium::from_reader(bytes).expect("CBOR decoding failed")
}
