docs:
    cargo clean --doc && cargo doc --open --all-features --no-deps

test:
    cargo test

format:
    cargo fmt
