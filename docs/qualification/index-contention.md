# Index contention qualification

> Historical evidence note: the D1--D64 builder, format-v5, manifest, and
> per-definition publication descriptions below explain the superseded runtime
> measured during the KELDRA-0020 investigation. They are not production
> guidance for v6. The current harness separates logical definitions with
> `KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX` from physical recipes with
> `KELDRA_INDEX_CONTENTION_PHYSICAL_RECIPE_COUNT`, and controls the pipeline
> through `KELDRA_INDEXING_CORES` and `KELDRA_INDEX_PIPELINE_MEMORY_BYTES`. It
> admits only Typed JSON and
> has no external-builder fallback.

`scripts/qualify-index-contention.sh` is the retained black-box Docker
comparison workload from the KELDRA-0016 investigation. It runs the packaged
Linux Keldra server in fresh Docker state on the Debian controller and drives
mutations and ordinary queries through a native macOS build of the public Rust
client on the physical Mac. Its default split is explicit: Docker publishes
the single-node API through
`192.168.64.3`, while the driver is built and run over SSH on
`zcourts@192.168.64.1` from `/Users/zcourts/projects/keldra/keldra`. Keldra is
never run as a native macOS server. The measured D/P details that follow are
historical Docker evidence; current v6 release qualification is the direct SSD
kit runner described below.

## Historical Docker comparison evidence

The Docker and split-topology commands in this section preserve evidence from
the superseded external-builder architecture. They are not v6 release
qualification and must not be used to set current throughput or correctness
claims. The retained single-node and three-node wrappers now execute only their
non-index release phases, so the command blocks below are archival evidence,
not runnable index qualification.

The smoke mode is deliberately small. It proves setup, cross-host connectivity,
authentication, workload
correctness, report generation, and cleanup, but it is not performance evidence:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=smoke \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
  ./scripts/qualify-index-contention.sh
```

## Current v6 SSD qualification

For the v6 SSD matrix, use `scripts/qualify-index-v6-ssd-scale.sh` on the SSD
host. It is a direct binary-kit runbook, not a Docker wrapper: install the
attested `keldra-server`, `keldra`, and `index-contention-qualification`
binaries, this runner, `SOURCE_COMMIT`, `HARNESS_COMMIT`, and
`SHA256SUMS` in `~/keldra_experiments/kit/` first. The checksum manifest must
cover the runner as well as `bin/*`. It verifies the kit before work begins and
keeps every input, durable database, log, raw report, and result archive below
`~/keldra_experiments`. Smoke defaults cover D64/P1,P4 with W1,W4 at 256 MiB
per worker. Sustained defaults run three non-duplicating axes: the P1 logical
ladder D1,D64,D1K,D10K,D250K at the largest resource cell; the D64 physical
ladder P1,P4,P16,P64 at that same resource cell; and D64/P1 with W1,W2,W4,W8
at 128/256 MiB per worker. The D250K catalog cell uses one configurable offered
rate (`KELDRA_V6_SCALE_CATALOG_RATE`) rather than repeating the expensive
admission step at every rate; `qualify-index-catalog.sh` provides the separate
create/restart catalog qualification. This avoids an uninformative full
Cartesian product while varying each independent cause:

```text
~/keldra_experiments/kit/
  SOURCE_COMMIT                         # exact 40-hex server revision
  HARNESS_COMMIT                        # exact 40-hex harness revision
  SHA256SUMS                            # sha256sum --check manifest for this kit
  qualify-index-v6-ssd-scale.sh         # this exact runner
  bin/keldra-server
  bin/keldra
  bin/index-contention-qualification
```

The controller creates that kit from one validated revision, writes its hash
manifest, and transfers it as a single artifact. The host does not build or
fetch source during qualification.

```bash
cd ~/keldra_experiments/kit
KELDRA_V6_SCALE_MODE=sustained ./qualify-index-v6-ssd-scale.sh
```

Each offered-rate cell starts with fresh durable state and walks the ascending
open-loop rate ladder (smoke: 100, 1,000; sustained: 1,000, 5,000, 10,000,
20,000, 40,000 operations/s by default). It stops an axis at the first
capacity-limit result, but treats correctness/workload failure as a failed run,
not a capacity figure. A sustainable cell requires the public correctness and
responsiveness gates plus a concurrent-phase source-lag slope no greater than
`KELDRA_V6_SCALE_MAX_LAG_SLOPE_RECORDS_PER_SECOND` (default 1). The small
object floor uses at least 1 KiB payloads. Sustained mode additionally runs a
96 KiB pathological source-object stream at D1/P1 and the largest resource
cell; it deliberately does not multiply that payload into D/P scale. Configure
both through `KELDRA_V6_SCALE_OBJECT_SIZE_MATRIX`.

Before any performance cell, a separate fresh-state public-API preflight proves
exact and range predicates, explicit ordering, facets, aggregates, and
full-text search. Its state is destroyed before the D/P/W/memory matrix, so
those extra capability recipes cannot contaminate the measured physical-work
axis. Failure aborts the run and its report is embedded in the final evidence.

Each summary reports offered, accepted, checkpointed-source, and end-to-end
indexed operations/s; accepted source bytes/s; both accepted and checkpointed
source rates per indexing core and per 256 MiB; lag
slope; drain; concurrent query latency; publication visibility; sampled
CPU/RSS; process write bytes (which cover WAL and RocksDB store writes); and
final durable-store bytes. It separately records definition creation seconds,
definitions created/s, and recipe-spanning qualified-activation seconds, so
D250K catalog admission is never folded into steady ingestion. The runner never estimates projected-byte
throughput from input payload size. A development record may explicitly mark
that value `null`, with its missing telemetry provenance, but it is not
qualification evidence. A final v6 qualification requires two
concurrent-phase `keldra_index_v6_summary` samples and derives source,
selected, prepared, projected, sealed, and checkpointed rows/bytes per second
from their cumulative counters. Every required rate must be positive; missing,
malformed, unchanged, or regressing summary evidence fails the cell. Raw driver
progress, process samples, VM samples, server logs, and the complete public
report remain beside the summary and are packaged as a SHA-256 sidecar archive.

Catalog cardinality D250K is qualified separately with
`scripts/qualify-index-catalog.sh`; the contention matrix does not conflate
catalog admission/restart cost with sustained physical indexing throughput.

## Historical Docker comparison evidence (continued)

Run the sustained matrix through the same split topology:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=single \
KELDRA_INDEX_CONTENTION_BASELINE_SECONDS=120 \
KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS=600 \
KELDRA_INDEX_CONTENTION_POST_SECONDS=120 \
KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX=1,64,1000 \
KELDRA_INDEX_CONTENTION_PHYSICAL_RECIPE_COUNT=1 \
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
KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX=1 \
KELDRA_INDEX_CONTENTION_MUTATION_WORKERS=1 \
KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE=8 \
KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH=8 \
  ./scripts/qualify-index-contention.sh
```

The evidence records all three settings. A result at one intensity must not be
compared with another intensity as a before/after performance claim.

The default mutation workload, `material-change`, changes an indexed generation
on every update. The v6 projection-preserving head-delta path is qualified
separately with:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX=1,64,256,640 \
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
KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX=1,64,1000 \
KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND=1000 \
  ./scripts/qualify-index-contention.sh
```

Omit the variable or set it to `disabled` for the saturated-queue workload.
Matrix entries may be any unique integer from 1 through 250,000. Physical
recipes are independently bounded to 64. `1,4,16,64` remains the historical
comparison matrix; v6 qualification separates D cardinality from P physical
work rather than treating either as a proxy for the other.

For supplementary three-node Docker evidence, use:

```bash
KELDRA_IMAGE=keldra:qa-<commit> \
KELDRA_INDEX_CONTENTION_MODE=sustained \
KELDRA_INDEX_CONTENTION_TOPOLOGY=three \
KELDRA_INDEX_CONTENTION_BASELINE_SECONDS=120 \
KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS=600 \
KELDRA_INDEX_CONTENTION_POST_SECONDS=120 \
KELDRA_INDEX_CONTENTION_DEFINITION_MATRIX=1,64,1000 \
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

The Docker contention wrapper defaults to the shared checkout release evidence
directory and may be redirected with `KELDRA_INDEX_CONTENTION_EVIDENCE_ROOT`.
For every remote v6 run, use the direct SSD runner above: it has no such
override and writes only to `~/keldra_experiments/results/index-v6-scale/`.
The stable `latest` symlink identifies the active or most recent run.

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
not each partition producer's current node assignment. The three-node report
therefore records those public observations and the assignment-observability
limitation; it must not infer per-node producer counts from cluster-wide D or P.

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
