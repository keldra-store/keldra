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
  and `perf-release` are the first counter-backed performance groups that fail
  directly on measured work and calibrated latency.
- `anvil-core/benches/coremeta_release_gate/` is the standalone CoreMeta gate;
  it does not depend on an ignored test or an environment variable to enforce
  pass/fail.

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
  --coremeta-report target/anvil/perf/coremeta/quick/report.json \
  --line target/anvil/perf/anvil.line \
  --release-log target/anvil/logs/release-gates.log
```

This prints the slowest measured cases, request paths, internal spans, and release-gate slow-test warnings.

## Run the CoreMeta ordered-access gate

The older `performance_tests` suite records broad integration timings but does
not enforce asymptotic work. The CoreMeta correction therefore has a separate
benchmark target backed by RocksDB `PerfContext` counters and the deterministic
`coremeta-release-gate.json` manifest.

Run the default pull-request profile with:

```sh
./scripts/release-gates.sh perf
```

Run the larger scheduled/release profile with:

```sh
./scripts/release-gates.sh perf-release
```

The quick profile compares 4,096 and 65,536 row collections. The release profile
compares 65,536 and 1,048,576 row collections. Both enforce point reads, early
and deep cursor pages, page-size scaling, a durable single-row calibration, and
a point-head read followed by one atomic two-row batch. A deep page fails when
RocksDB work grows with rows before the cursor rather than the requested page.

Evidence is retained under
`target/anvil/perf/coremeta/<quick|release>/`: `report.json` contains raw samples,
work counters, thresholds, and every gate result; `gate-manifest.json` is the
exact manifest used; and `run.log` is the benchmark output. The checker rejects
missing scenarios, missing raw samples, absent complexity gates, non-finite
metrics, bounded-result mismatches, or any failed gate.

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
