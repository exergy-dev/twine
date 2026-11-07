#!/bin/sh
# Replays the steps of rust/ci.yml on this machine (test job, cross job, and
# the nightly job with FUZZTIME configurable in seconds) so the workflow's
# commands are exercised even without a GitHub remote. Run from anywhere.
set -e
cd "$(dirname "$0")/.."
FUZZTIME=${FUZZTIME:-600}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$PWD/target/ci}
step() { echo; echo "== $*"; }
step "test job: cargo fmt --check";            cargo fmt --all --check
step "test job: cargo clippy -D warnings";     cargo clippy --workspace --all-targets -- -D warnings
step "test job: cargo test --workspace";       cargo test --workspace
step "test job: cargo doc -D warnings";        RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
step "test job (Linux): uring feature check";  cargo check -p twine --features uring
step "cross job: darwin check (crates without ring)"; cargo check -p twine -p twine-proto -p twine-http --target x86_64-apple-darwin
if rustup target list --installed | grep -q x86_64-unknown-freebsd; then
  step "cross job: freebsd check";             cargo check -p twine -p twine-proto -p twine-http --target x86_64-unknown-freebsd
else
  echo "(freebsd target not installed; skipped — rustup target add x86_64-unknown-freebsd)"
fi
step "nightly: ignored tests in release (timing gates, long stress, soak)"
cargo test --workspace --release -- --ignored
step "nightly: fuzz ${FUZZTIME}s per target"
scripts/fuzz.sh "$FUZZTIME"
echo; echo "CI replay complete"
