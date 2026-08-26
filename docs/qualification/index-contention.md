# Index contention qualification

`scripts/qualify-index-contention.sh` is the black-box acceptance workload for
KELDRA-0016. It drives mutations and ordinary queries concurrently through the
public Rust client against the exact packaged Keldra image. Each matrix cell
starts with fresh durable state and creates the requested number of independent,
simultaneously lagging index definitions; the matrix value is definition count,
not projection-lane configuration. On one node, `1,4,16,64` is the normative
node-wide admitted-builder pressure matrix. Three-node placement distributes
definitions using HRW, so its definition count is cluster-wide and is
supplementary evidence, not a claim that every node ran that many builders.

The smoke mode is deliberately small. It proves setup, authentication, workload
correctness, report generation, and cleanup, but it is not performance evidence:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=smoke \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
  ./scripts/qualify-index-contention.sh
```

Run the sustained matrix on the host with Docker allocated enough CPU, memory,
and disk for the intended qualification environment:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=three \
KELDRA_INDEX_CONTENTION_BASELINE_SECONDS=120 \
KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS=600 \
KELDRA_INDEX_CONTENTION_POST_SECONDS=120 \
KELDRA_INDEX_CONTENTION_MATRIX=1,4,16,64 \
  ./scripts/qualify-index-contention.sh
```

For the required before/after comparison, provide the released baseline image.
The same newly built harness runs baseline and candidate cells with fresh state:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_BASELINE_IMAGE=keldra:0.12.0 \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
  ./scripts/qualify-index-contention.sh
```

The candidate image must be built from the clean checked-out commit used to run
the qualification. A baseline may have another revision, but must carry a full
revision label. The wrapper resolves both to immutable IDs and records the
harness commit separately from both server revisions.

A baseline responsiveness failure is evidence, not a reason to skip the
candidate. The wrapper continues when the baseline's correctness and workload
validity checks pass, records its nonzero driver exit and timeout/drop/p99
counts, and still produces `comparison.json`. If either histogram has no
completed samples, its delta is `null` rather than a fabricated comparison.
The candidate must pass
correctness, workload validity, and the configured responsiveness gate.

## Evidence and monitoring

By default, evidence is written below the shared checkout root at
`releases/keldra/index-contention/<run-id>`. Override this with
`KELDRA_INDEX_CONTENTION_EVIDENCE_ROOT`. The stable `latest` symlink identifies
the active or most recent run.

Monitor an active run without reading credentials or attaching to the workload:

```bash
tail -F ../../releases/keldra/index-contention/latest/progress.jsonl
watch -n 2 cat ../../releases/keldra/index-contention/latest/status.json
```

`run.json` records the source commit, immutable image ID and revision, image
platform, topology, matrix, phase durations, host resources, and Docker resource
allocation. Every cell retains the client report, client stdout/stderr, and
server logs. `container-resources.jsonl` samples Docker CPU, memory, network,
block-I/O, and process counters throughout the workload. `progress.jsonl` is
the append-only orchestration lifecycle;
`active-driver-progress.jsonl` points to the current cell's flushed, one-second
cumulative snapshots; latency histograms are reset at phase transitions, while
operation counters remain cumulative. `status.json` is replaced atomically
after every lifecycle event. None of these files contains client credentials.

The public API exposes definition IDs and freshness placement terms/indexes, but
not the node currently executing each builder. The three-node report therefore
records those public observations and the assignment-observability limitation;
it must not infer per-node builder counts from total definitions.

When a baseline is supplied, `comparison.json` gives baseline, candidate, and
candidate-minus-baseline p50/p95/p99/max values for concurrent ordinary-query
schedule-to-response latency, dispatched service latency, and
end-to-end mutation-acceptance-to-query-visibility lag. That last measurement
includes polling and query observation, so it is not pure server publication
latency. Raw reports remain the authority for every phase and correctness
counter.

The server log filter defaults to a production-shaped `info` level rather than
hot-path debug logging, which would contaminate latency results. Record and
review the value in `run.json`; use `KELDRA_INDEX_CONTENTION_RUST_LOG` only for a
separate diagnostic run when more verbose telemetry is necessary.

An interrupted or failing run retains its evidence and records a terminal
failure event. Containers and temporary durable state are removed unless
`KELDRA_INDEX_CONTENTION_KEEP=1`; that option is intended only for diagnosis and
can consume substantial disk.

Run single-node and three-node matrices separately. A valid performance claim
requires the sustained matrix, equivalent hardware and configuration for every
candidate, and comparison of the raw per-cell query and publication-lag
distributions. Smoke results must never be reported as sustained evidence.

For this harness, “queries remained responsive” means the open-loop driver
recorded zero dropped schedules, request errors, and timeouts, every completed
query satisfied its stable-result oracle, and every request stayed within the
configured request timeout. The p50/p95/p99 values are measured evidence, not an
SLO claim. The wrapper does enforce a configurable concurrent-query p99 gate,
defaulting to 2,000 ms. Setting
`KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS=disabled` removes
that absolute latency assertion and weakens the responsiveness result, although
the matrix distributions are still recorded. A single matrix pass exposes
scaling behavior;
statistically credible release comparison requires repeated sustained runs on
equivalent hardware, preferably with baseline/candidate order varied between
runs. Set `KELDRA_INDEX_CONTENTION_COMPARISON_ORDER=candidate-first` on
alternating runs; the default is `baseline-first`.
