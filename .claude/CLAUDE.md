# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the Rust SDK for Hegel, a universal property-based testing framework. The SDK communicates with a Python server (powered by Hypothesis) via Unix sockets to generate test data.

## Build & Test Commands

```bash
just check                          # Full CI: fmt --check + clippy + docs + test + test --all-features
just test                           # cargo test
just lint                           # cargo fmt --check + cargo clippy
just format                         # cargo fmt
just docs                           # cargo doc --open --all-features --no-deps
just coverage                       # cargo llvm-cov (requires cargo-llvm-cov + llvm-tools-preview)
cargo test test_name                # Run single test
cargo test --all-features           # Run tests including optional features
```

MSRV is 1.81 (enforced in CI and Cargo.toml).

## Crate Structure

```
hegel-rust/
├── src/
│   ├── lib.rs          # Public API: hegel(), Hegel builder, assume(), note()
│   ├── protocol.rs     # Binary protocol: packet encoding/decoding, channel multiplexing
│   ├── cbor_helpers.rs # Macros and helpers for ciborium::Value (cbor_map!, cbor_array!, map_get, etc.)
│   ├── runner.rs       # Spawns hegel CLI, manages socket server
│   └── gen/            # Generator implementations
│       ├── mod.rs      # Generate trait, socket communication, thread-local state
│       ├── primitives.rs   # unit(), booleans(), just()
│       ├── numeric.rs      # integers(), floats() with bounds
│       ├── strings.rs      # text(), from_regex()
│       ├── formats.rs      # emails(), urls(), dates(), ip_addresses(), etc.
│       ├── collections.rs  # vecs(), hashsets(), hashmaps()
│       ├── tuples.rs       # tuples(), tuples3()
│       ├── combinators.rs  # one_of!(), optional(), sampled_from(), BoxedGenerator
│       ├── fixed_dict.rs   # fixed_dicts() for JSON objects
│       ├── default.rs      # DefaultGenerator trait implementations
│       ├── macros.rs       # one_of!(), derive_generator!() macros
│       ├── binary.rs       # binary() for Vec<u8> generation
│       ├── random.rs       # randoms() for RNG generation (requires "rand" feature)
│       ├── value.rs        # HegelValue wrapper for NaN/Infinity handling
│       └── compose.rs      # ComposedGenerator, compose!() macro
├── hegel-derive/       # Proc macro crate for #[derive(Generate)]
│   └── src/lib.rs      # Derives generators for structs and enums
└── build.rs            # Auto-installs hegel CLI via uv if not on PATH
```

## Feature Flags

- **`rand`**: Enables `gen::randoms()` for generating `rand::RngCore` implementations

## Architecture

### How It Works

The SDK creates a socket path and spawns the `hegel` CLI as a subprocess. Hegeld binds to the socket and listens for connections. The SDK then connects as a client, and a single persistent connection is maintained for the program run. Multiple tests can be executed over this connection. The build script (`build.rs`) automatically installs Python and hegel into cargo's `OUT_DIR/hegel` via uv if not found on PATH.

### Protocol

The protocol uses CBOR encoding over multiplexed channels. For each test:
1. SDK sends `run_test` request on control channel
2. Hegeld sends `test_case` events with channel IDs for each test case
3. SDK runs test function, sending `generate`/`start_span`/`stop_span` requests on the test channel
4. SDK sends `mark_complete` with status (VALID, INVALID, or INTERESTING)
5. After all test cases, hegeld sends `test_done` with results`

### Thread-Local State

The SDK uses thread-local storage for:
- `IS_LAST_RUN`: Whether this is the final replay for counterexample output
- `CONNECTION`: The active socket connection with span depth tracking

### Generation Protocol

Generators implement the `Generate<T>` trait:
- `schema()`: Returns a CBOR schema (as `ciborium::Value`) describing generated values (enables single-request composition)
- `generate()`: Produces a value, either via schema or compositional fallback

When `schema()` returns `Some`, the SDK sends one request. When `None` (after `map`/`filter`), it falls back to multiple requests with span grouping for shrinking.

### Span System

Spans (`start_span`/`stop_span`) group related generation calls, helping Hypothesis understand data structure for effective shrinking. Labels in `gen::labels` identify span types (LIST, TUPLE, ONE_OF, etc.).

## Key Patterns

### Adding a New Generator

1. Create a builder struct with configuration fields
2. Implement `Generate<T>` with `schema()` and `generate()`
3. Export a factory function from `gen/mod.rs`
4. If the generated type should work with `#[derive(Generate)]`, implement `DefaultGenerator`

### Derive Macro

`#[derive(Generate)]` creates a `<Type>Generator` struct with:
- `new()`: Uses `DefaultGenerator` for all fields
- `with_<field>(gen)`: Builder methods to customize field generators

For enums, it also creates `<Enum><Variant>Generator` for each data variant.
