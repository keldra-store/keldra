#!/usr/bin/env bash
set -euo pipefail

out_dir="${KELDRA_TEST_TIMING_DIR:-target/keldra/test-timings}"
mkdir -p "$out_dir"

printf 'Writing timing outputs to %s\n' "$out_dir"

cargo test --workspace --no-run --timings 2>&1 | tee "$out_dir/cargo-no-run.log"

if command -v cargo-nextest >/dev/null 2>&1; then
  KELDRA_TEST_TIMINGS=1 cargo nextest run \
    --workspace \
    --no-fail-fast \
    --status-level slow \
    --final-status-level slow \
    --success-output final \
    --failure-output immediate-final \
    2>&1 | tee "$out_dir/nextest.log"
else
  printf 'cargo-nextest not found; falling back to cargo test with --nocapture so timing stderr is visible.\n' \
    | tee "$out_dir/nextest.log"
  KELDRA_TEST_TIMINGS=1 cargo test --workspace -- --nocapture 2>&1 | tee "$out_dir/cargo-test-nocapture.log"
fi
