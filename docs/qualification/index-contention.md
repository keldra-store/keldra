# Index contention qualification

`scripts/qualify-index-contention.sh` is the black-box acceptance workload for
KELDRA-0016. It runs the packaged Linux Keldra server in fresh Docker state on
the Debian controller and drives mutations and ordinary queries through a
native macOS build of the public Rust client on the physical Mac. The default
split is explicit: Docker publishes the single-node API through
`192.168.64.3`, while the driver is built and run over SSH on
`zcourts@192.168.64.1` from `/Users/zcourts/projects/keldra/keldra`. Keldra is
never run as a native macOS server. Each matrix cell
starts with fresh durable state and creates the requested number of independent,
simultaneously lagging index definitions; the matrix value is definition count,
not projection-lane configuration. On one node, `1,4,16,64` is the normative
legacy node-wide admitted-builder pressure matrix. The harness accepts values
through 1,024 so replacement architectures can run the D640 active-fan-out
gate defined in `index-scale-investigation.md`; raising the harness ceiling does
not raise the production runtime's 64-builder lease bound. Three-node placement distributes
definitions using HRW, so its definition count is cluster-wide and is
supplementary evidence, not a claim that every node ran that many builders.

The smoke mode is deliberately small. It proves setup, cross-host connectivity,
authentication, workload
correctness, report generation, and cleanup, but it is not performance evidence:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=smoke \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
  ./scripts/qualify-index-contention.sh
```

Run the sustained matrix through the same split topology:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
KELDRA_INDEX_CONTENTION_BASELINE_SECONDS=120 \
KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS=600 \
KELDRA_INDEX_CONTENTION_POST_SECONDS=120 \
KELDRA_INDEX_CONTENTION_MATRIX=1,4,16,64 \
  ./scripts/qualify-index-contention.sh
```

Mutation intensity is independent of the definition-count matrix. The defaults
(`4` workers, `32` objects per batch, queue depth `32`) intentionally drive the
server toward its ingress limit. For a resource-constrained controller, start
with a bounded load and increase it only after the prior run passes:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
KELDRA_INDEX_CONTENTION_MATRIX=1 \
KELDRA_INDEX_CONTENTION_MUTATION_WORKERS=1 \
KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE=8 \
KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH=8 \
  ./scripts/qualify-index-contention.sh
```

The evidence records all three settings. A result at one intensity must not be
compared with another intensity as a before/after performance claim.

The default mutation workload, `material-change`, changes an indexed generation
on every update. The format-v5 projection-preserving path is qualified
separately with:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_MATRIX=1,4,16,64,256,640 \
KELDRA_INDEX_CONTENTION_MUTATION_WORKLOAD=projection-preserving \
KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND=100 \
  ./scripts/qualify-index-contention.sh
```

That mode continuously changes source versions and the unindexed payload while
keeping every indexed value constant. It pre-creates a bounded set of marker
objects and rotates visibility samples across them, so marker observation also
uses the projection-preserving path. A sample passes only when an ordinary
query returns the newest exact source version; merely suppressing physical
index work cannot satisfy the oracle. Reported results from the two workload
modes are separate populations and must not be combined.

`KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES` optionally pads each mutable
JSON record to an exact minimum byte size. Its default `0` preserves the normal
small-record corpus. Set it together with the batch size and worker count to
reproduce large valid `BulkWrite` requests; the report records the value so a
large-payload run cannot be compared silently with the default workload.

To measure sustainable publication capacity instead of saturating ingress, set
an explicit offered rate. The rate counts every public mutation operation,
including the per-batch marker used for visibility measurement. Requests are
scheduled open-loop; a full client queue is recorded as a dropped mutation
batch and fails workload validity rather than silently lowering the offered
rate:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_MATRIX=1,4,16,64 \
KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND=1000 \
  ./scripts/qualify-index-contention.sh
```

Omit the variable or set it to `disabled` for the saturated-queue workload.
Matrix entries may be any unique integer from 1 through 1,024. `1,4,16,64`
remains the legacy comparison matrix, while D640 is the required tenfold
active-fan-out gate for the replacement design.

For supplementary three-node Docker evidence, use:

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

When Docker is available, `KELDRA_IMAGE` selects the candidate. If that tag is
absent locally, the wrapper runs `scripts/build-image.sh`; an existing image with
the wrong revision is rejected rather than silently rebuilt. A missing released
baseline is also a hard error and is never manufactured from the current tree.

The Linux server backend is always Docker and `KELDRA_IMAGE` is always required.
The wrapper fails if the local Docker daemon is unavailable; it never substitutes
a native server. The driver backend defaults to `ssh-macos`. Override
`KELDRA_INDEX_CONTENTION_DRIVER_HOST`,
`KELDRA_INDEX_CONTENTION_DRIVER_IDENTITY_FILE`,
`KELDRA_INDEX_CONTENTION_DRIVER_REPO_ROOT`, or
`KELDRA_INDEX_CONTENTION_SERVER_ADVERTISE_HOST` only when the physical-host
layout differs. `KELDRA_INDEX_CONTENTION_DRIVER_EVIDENCE_ROOT` must name the
macOS view of the same shared evidence directory. The wrapper verifies that the
remote checkout is clean, matches the harness commit, and can read the bound
`run.json` before starting a workload. Secrets travel in the SSH input stream,
not command arguments or logs. SSH defaults to the controller-owned
`$HOME/.ssh/debian1_id_ed25519` with `IdentitiesOnly=yes`.

`KELDRA_INDEX_CONTENTION_DRIVER_BACKEND=local` retains the controller-local
driver for diagnostics. The default `ssh-macos` driver currently supports the
normative single-node smoke and sustained matrices. It rejects three-node mode
explicitly because the existing Compose topology publishes its APIs on Debian
loopback only; use the local driver backend for supplementary three-node
evidence until that topology has an intentionally external publication surface.

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
allocation. It also records mutation workers, batch size, queue depth, and the
optional fixed offered operation rate, mutation workload, plus
the request, drain, visibility polling and total
visibility-observation timeouts, sampling interval, and both absolute p99
acceptance bounds. Every cell retains the client report, client stdout/stderr, and
server logs. Docker cells use `container-resources.jsonl` for CPU, memory,
network, block-I/O, and process counters; native cells use
`process-resources.jsonl` for host-reported server CPU and RSS.
`progress.jsonl` is the append-only orchestration lifecycle;
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

The final drain authority is exact public `HeadObject` path/version state
compared with every index definition, plus a complete initial build, no rebuild,
and the full source set. `observed_tail` is intentionally optional because an
ordinary query does not synchronously inspect the journal. When every query
replica supplies it, the report records advisory zero-lag verification;
otherwise that field is `null` and the exact public state comparison remains
authoritative.

The server log filter defaults to a production-shaped `info` level rather than
hot-path debug logging, which would contaminate latency results. Record and
review the value in `run.json`; use `KELDRA_INDEX_CONTENTION_RUST_LOG` only for a
separate diagnostic run when more verbose telemetry is necessary.

An interrupted or failing run retains its evidence and records a terminal
failure event. Once configuration has been accepted, terminal setup, drain, and
final-verification errors also produce a structured failure `report.json`.
Containers and temporary durable state are removed unless
`KELDRA_INDEX_CONTENTION_KEEP=1`; that option is intended only for diagnosis and
can consume substantial disk.

Run single-node and three-node matrices separately. A valid performance claim
requires the sustained matrix, equivalent hardware and configuration for every
candidate, and comparison of the raw per-cell query and publication-lag
distributions. Smoke results must never be reported as sustained evidence.

For this harness, “queries remained responsive” means the open-loop driver
recorded zero dropped schedules, request errors, and timeouts, every completed
query satisfied its stable-result oracle, and every request stayed within the
configured per-request timeout. Publication probes use that timeout for each
ordinary query RPC, but may continue polling for the separate total observation
timeout. The latter defaults to the 600-second drain timeout and is configured
with `KELDRA_INDEX_CONTENTION_VISIBILITY_OBSERVATION_TIMEOUT_SECONDS`; a slow
publication is therefore measured rather than incorrectly classified as one
slow RPC after 30 seconds. Samples rotate across definitions by sample ordinal,
independently of canary IDs and the sampling interval. Reports retain up to 16
failed sample identities and bounded error messages, plus an omitted count.

The p50/p95/p99 values are measured evidence, not an SLO claim. The wrapper
enforces a configurable concurrent-query p99 gate, defaulting to 2,000 ms, and
a publication-visibility p99 gate, defaulting to 30,000 ms. Setting
`KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS=disabled` removes
that absolute latency assertion and weakens the responsiveness result, although
the matrix distributions are still recorded. Likewise,
`KELDRA_INDEX_CONTENTION_MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS=disabled`
removes only the publication p99 assertion; visibility sample completeness and
final exact convergence remain mandatory. A single matrix pass exposes scaling
behavior;
statistically credible release comparison requires repeated sustained runs on
equivalent hardware, preferably with baseline/candidate order varied between
runs. Set `KELDRA_INDEX_CONTENTION_COMPARISON_ORDER=candidate-first` on
alternating runs; the default is `baseline-first`.
