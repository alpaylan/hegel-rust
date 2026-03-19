# Installing Hegel for Rust

## Prerequisites

Add Hegel to your `Cargo.toml`:

```toml
[dev-dependencies]
hegeltest = "0.1.8"
```

Hegel for Rust requires a Python-based server component (`hegel-core`) to
generate test data. There are two ways to provide it:

## Option 1: Automatic installation via uv (recommended)

Install [`uv`](https://docs.astral.sh/uv/), a fast Python package manager:

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# or with Homebrew
brew install uv

# or with pip
pip install uv
```

See the [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/)
for more options.

Once `uv` is on your PATH, Hegel will automatically install the correct
version of `hegel-core` into a local `.hegel/venv/` directory the first time
you run a test. No further setup is needed.

## Option 2: Manual server binary via HEGEL_SERVER_COMMAND

If you prefer not to use `uv`, or you have a custom build of the hegel server,
set the `HEGEL_SERVER_COMMAND` environment variable to the path of a
`hegel` binary:

```bash
export HEGEL_SERVER_COMMAND=/path/to/hegel
cargo test
```

This skips automatic installation entirely. You are responsible for ensuring
the binary version is compatible with the SDK.

## Troubleshooting

### "uv not found" error

If you see an error about `uv` not being found, either:

1. Install `uv` (see Option 1 above), or
2. Set `HEGEL_SERVER_COMMAND` to a hegel binary (see Option 2 above).

### Installation log

When automatic installation fails, Hegel writes detailed output to
`.hegel/install.log` in your project directory. Check this file for
pip/uv errors.

### Version mismatches and other problems

If run into problems (e.g. protocol mismatches) try removing `.hegel/venv/` to force a fresh install:

```bash
rm -rf .hegel/venv
cargo test
```

This shouldn't ever be necessary, so if it is please let us know that you're running into problems by [filing an issue](https://github.com/hegeldev/hegel-rust/issues).
