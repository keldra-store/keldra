# Anvil performance stack

This directory contains the local performance stack used to investigate slow release gates and request-level latency. GreptimeDB and Grafana run in Docker; Anvil itself runs on the host through the normal Rust test harness.

## Performance inventory

- `anvil/tests/performance_tests.rs` is the broad, environment-gated CoreStore
  and gRPC timing suite. It emits baseline-compatible summaries but is not an
  asymptotic complexity gate.
- `anvil-core/src/perf_baseline.rs` defines the full-system baseline manifest,
  deterministic generators, and summary schema.
- `scripts/bench-authz-mutations.sh` is a focused diagnostic command, not a
  release gate.
- `scripts/release-gates.sh` owns the executable release groups. `perf-quick`
  and `perf-release` capture the MVCC-under-Raft benchmark evidence defined by
  the current architecture.
- `anvil-core/benches/mvcc_rfc.rs` exercises transaction shapes, durability
  levels, streaming erasure coding, bundle persistence, Raft certification,
  local MVCC application, and snapshot reads.

## Start GreptimeDB and Grafana

```sh
docker compose -f ops/perf/docker-compose.yml up -d
```

Grafana listens on <http://127.0.0.1:3000>. The local credentials are `admin` / `admin`; anonymous admin access is enabled for this local-only stack.

GreptimeDB listens on:

- HTTP line protocol: <http://127.0.0.1:4000>
- MySQL protocol for Grafana: `127.0.0.1:4002`

## Run the focused performance suite

Use absolute output paths. Cargo integration tests execute with the package manifest as the current directory, so relative `target/...` paths can otherwise end up below `anvil/target/...`.

```sh
ANVIL_RUN_PERF_TESTS=1 \
ANVIL_PERF_TRACE=1 \
ANVIL_TEST_TIMINGS=1 \
ANVIL_PERF_GREPTIME_URL='http://127.0.0.1:4000/v1/influxdb/write?db=public' \
ANVIL_PERF_TRACE_FILE="$(pwd)/target/anvil/perf/anvil.line" \
ANVIL_PERF_REPORT_PATH="$(pwd)/target/anvil/perf/performance-summary.json" \
cargo test -p anvil-server --test performance_tests -- --nocapture --test-threads=1
```

The suite records two layers:

- method-level timings for CoreStore primitives such as blob put/get, append/read stream, CAS ref, fences, and mutation batches;
- end-to-end gRPC timings for bucket creation, object writes, object reads, listing, index creation, and caught-up index queries.

## Summarise local output

```sh
scripts/analyze-perf.py \
  --summary target/anvil/perf/performance-summary.json \
  --line target/anvil/perf/anvil.line \
  --release-log target/anvil/logs/release-gates.log
```

This prints the slowest measured cases, request paths, internal spans, and release-gate slow-test warnings.

## Capture MVCC-under-Raft performance evidence

The `mvcc_rfc` harness records the phase boundaries required by
`docs/rfcs/mvcc_under_raft.md`: stripe encoding, shard streaming, remote
persistence, Raft certification, local MVCC application, reads, and total
transaction time. It covers metadata, inline-object, streaming-erasure,
single-key, ten-key, cross-table, and durability-level transaction shapes.

Run the default pull-request profile with:

```sh
./scripts/release-gates.sh perf
```

Run the larger scheduled/release profile with:

```sh
./scripts/release-gates.sh perf-release
```

The quick profile runs each shape once. The release profile runs five samples by
default; override that with `ANVIL_MVCC_PERF_ITERATIONS`. Evidence is retained
under `target/anvil/perf/mvcc/<quick|release>/`: `run.log` contains the phase
samples and `metadata.txt` records the exact commit and toolchain.

This harness captures evidence; it does not yet impose hardware-independent
latency thresholds. Compare like-for-like machines and retain the results with
release evidence. The remaining RFC-required contention, group-commit,
reconnect, retained-history, and garbage-collection workloads must be added
before treating the performance section of the RFC as complete.

## Capture a macOS Time Profiler trace

For CPU-level analysis, run the same performance suite under Instruments through `xctrace`:

```sh
ANVIL_RUN_PERF_TESTS=1 \
ANVIL_PERF_TRACE=1 \
ANVIL_TEST_TIMINGS=1 \
ANVIL_PERF_TRACE_FILE="$(pwd)/target/anvil/perf/anvil.line" \
ANVIL_PERF_REPORT_PATH="$(pwd)/target/anvil/perf/performance-summary.json" \
ops/perf/run-xctrace.sh target/anvil/perf/anvil-time-profile.trace
```

Open the resulting `.trace` file in Instruments and inspect the hot call stacks for the slow cases reported by `scripts/analyze-perf.py`.
