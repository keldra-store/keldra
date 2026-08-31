# Keldra observability

Keldra always writes its structured `tracing` logs to stdout. OTLP export is an
optional startup setting and carries metrics and traces only; logs remain on
stdout.

Set the standard OpenTelemetry endpoint variable or its equivalent command-line
flag:

```console
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 keldra-server ...
keldra-server --otlp-endpoint http://collector:4318 ...
```

The endpoint is treated as the OTLP HTTP base URL. Keldra uses protobuf over HTTP
and the exporter appends `/v1/metrics` and `/v1/traces`. Keldra's successful
startup log does not print the endpoint. If the option and environment variable
are both absent (or empty), Keldra does not construct an exporter, start an
OpenTelemetry worker, or make a collector connection.

Observability configuration is read once at process startup. Enabling,
disabling, or changing the endpoint requires a restart. There is no metrics
listener, diagnostics listener, public observability RPC, or administration
API.

## Resource and bounds

Metrics and traces share these resource attributes:

- `service.name=keldra`
- `service.version=<running Keldra version>`
- `node.id=<KELDRA_NODE_ID>`

Trace export uses a dedicated worker with a 2,048-span queue and batches of at
most 512 spans. A full queue drops new telemetry instead of blocking request
processing. Metrics use a periodic dedicated worker and cap each instrument at
128 attribute sets, with excess sets folded into the SDK overflow series. Each
HTTP export has a five-second timeout. Shutdown flushes and stops both providers
from a blocking worker so it does not occupy a Tokio request worker.

OTLP setup errors, such as an invalid endpoint, fail startup. Collector
unavailability after startup does not add a dependency to the Keldra data path.

## Signal contract

Metric attributes must stay low-cardinality. The allowed dimensions are closed
outcomes and modes such as `outcome=success|error|replayed`,
`durability=local|replicated`, and a bounded worker trigger. Never attach a
tenant, bucket, path, invocation ID, program hash, bundle hash, command ID,
payload, or user-selected string to a metric.

Program traces may carry the derived `invocation.hash`, `program.hash`,
`nomination.log_index`, and `commit.log_index`. These fields are identities
needed to join a single invocation's work; caller-selected invocation IDs,
opaque program input, paths, and object payload bytes are never span fields or
events.

The Keldra 0.9 process metric vocabulary covers:

- executor nomination count;
- atomic-program invocation counts, total and combined preparation latency,
  prepared bytes, and Raft commit latency;
- unfinalized tail entries and bytes plus finalization retry count;
- committed-invocation replay-window entries and bytes;
- watch-journal retained-entry and retained-byte gauges plus consumer-lag
  observations whenever a watch starts or advances;
- bulk attempted operations and encoded request bytes, mutually exclusive
  successful, failed, and replayed operation counts, and request latency;
- interrupted bulk dispatch and Store duration with the bounded `bulk.phase`
  values `coordinator_dispatch`, `payload_preparation`, `ordinary_path_lock`,
  `commit_lock`, `operation_evaluation`, `rocksdb_persistence`,
  `source_journal_settlement`, or `capacity_wait`; and
- blob garbage-collection run, removal, and failure counts.

Index runtime metrics use only bounded attributes such as
`pipeline.stage`, `pipeline.reason`, `update.kind`, and `recovery.action`.
Stable logical-definition, physical-recipe, partition, root, and segment IDs
belong in traces, never metric labels. Range-aware compaction measurements use
the bounded
`compaction.lane_limit_reason=configured|workers|budget|ranges` value. Index,
tenant, and bucket IDs are deliberately absent from metrics.

The v6 Typed JSON pipeline emits one structured summary every ten seconds on
the `keldra::index_runtime::v6_summary` target. Every field is prefixed
`keldra_index_v6_`. The live counters cover source rows/bytes; hot raw hits,
prepared hits, misses, and evictions; payload parsed, selected, extracted,
prepared, projected, sealed, published, and checkpointed rows/bytes; catalog
directory publications and activations; and stage CPU and queue-wait
nanoseconds. The live gauges are `stage_resident_bytes`, `stage_limit_bytes`,
`local_next_offset`, `local_tail`, `lag_entries`, and
`lag_oldest_age_milliseconds`. These are admission/accounting values, not
sampled process RSS. The default planning frame is 256 MiB per indexing core.

Compaction admission reports
`keldra_index_compaction_configured_lanes`,
`keldra_index_compaction_worker_limit`,
`keldra_index_compaction_budget_limit`, shared and incremental workspace bytes,
admitted workspace bytes, and leased bytes. Once the engine has planned its
deterministic key ranges, progress and terminal events report
`keldra_index_compaction_effective_lanes` as the minimum of configured lanes,
workers, budget-admitted lanes, and `keldra_index_compaction_range_limit`. They
also report active and waiting lanes, ranges total and completed, selected
input segments/documents/bytes, input component rows/read bytes/blocks, output
component rows/bytes/blocks, elapsed time, last-progress age, attempts,
failures, and duration. An input document is the document count declared by a
selected immutable segment. An input component row is one row decoded at a
document, typed-key, posting, projection, or spill component
boundary. Re-decoding a block counts its rows again, so this is a CPU and
decoding-work measure and may legitimately be orders of magnitude larger than
the selected document count. Output component rows similarly count rows emitted
by canonical component writers, not unique source objects. The corresponding
cumulative `*_total` instruments make component-row, byte, block, and
completed-range rates directly queryable with the metrics backend's rate
function. Input/output ratios are not published as a progress percentage
because compaction can discard superseded rows and change the encoded byte
count.

Projection `rayon_queue_seconds` is aggregate worker queue time summed across
the finite projection units in one wave. Parallel waits can therefore make it
larger than wall-clock duration; it is a saturation signal, not elapsed time.

Partition-local baseline and journal catch-up expose overlap-safe active
counters, records and bytes, elapsed time, last-progress age, source reads,
failures, and selected recovery actions. Publication exposes the v6 partition
root/current revision, checkpoint, presence, age, freshness, source lag,
partition-local CAS result, fence result, segment and head-delta bytes, and
duration. Query materialization separately reports family-directory generation,
root-vector size, retired-predecessor coverage, readers opened, and cache reuse.

Query execution exposes the complete request and local-work path. Public RPCs
report `keldra_index_query_requests_total`, failures, deadline expirations, and
request duration. Local execution reports admission waiting/active counts,
wait duration, runs, cancellation and failure counts, returned hits, artifact
read operations and bytes, cooperative yields, and query duration. CPU chunks
report waiting/active counts, queue time, execution time, chunk count, and
failures. These metrics are split only by closed outcome or status values. The
`keldra.index.query` and `keldra.index.query.cpu` spans may
carry stable numeric identifiers and detailed terminal snapshots; identifiers
never become metric attributes. Artifact reads remain asynchronous, while
decoded-page construction, filtering, ranking, bounded top-k maintenance, and
response-page serialization execute on the process-owned index CPU pool.
Planner and posting telemetry reports conjunctions, reordered conjunctions,
costed children, the selected lead cost range, posting blocks sought, decoded,
and skipped, posting bytes, posting advances, conjunction advances, bounded
union-heap pushes and pops, exact second-phase checks, live-mask decodes, and
candidate-gate batches. Point and doc-value block decodes are reported
separately. Typed JSON computations add
`keldra_index_query_facet_computations_requested_total`,
`keldra_index_query_facet_computation_results_total`,
`keldra_index_query_facet_documents_processed_total`,
`keldra_index_query_facet_values_processed_total`, and the corresponding four
`aggregate` counters. Document, index, tenant, and bucket identifiers remain
trace fields.
Each terminal query also reports desired and granted memory, admitted resident
segment slots, current and peak resident segments, conservatively charged
decoded bytes, evictions, and reloads. The fixed `index.phase` dimension reports
planning, continuation seek, head initialization, physical merge/advance,
candidate visibility, and response materialization duration. These are charged
or phase-boundary measurements, not sampled process RSS. A phase which runs
several segment lanes concurrently reports aggregate lane time; coordinator
phases report wall time, so the phase value is a work/saturation diagnostic and
must not be summed to reconstruct request latency.
`keldra_index_query_returned_hits` is emitted only for a completed response
page. A failed, timed-out, or otherwise cancelled query has no returned page,
so its hit count remains unknown rather than being recorded as zero; its read
work and terminal outcome are still emitted.

The serving fence reports renewal attempts, successful renewals, and failed
attempts separately, plus renewal duration and Tokio scheduling lateness,
remaining lease margin, current validity, missed deadlines, placement progress,
and the overlap-safe
`keldra_control_plane_tasks_active` count. `keldra.serving_fence.renewal` spans
carry the placement term/index and leader node for diagnosis; the metric series
uses only closed operation and outcome labels. A failed attempt while the
previous grant remains valid is an informational
`renewal_failed_lease_valid` outcome; only the absence of a valid fence is
warned as `fence_unavailable`. The missed-deadline counter increments once when
an unavailable transition is observed between renewal attempts. The legacy
`keldra_serving_fence_renewals_total` remains an alias for attempts; use
`keldra_serving_fence_renewal_successes_total` for successful grants.

A ten-second runtime sampler exports process RSS/virtual memory/thread count,
cgroup current/limit/peak memory and pressure/OOM events, and RocksDB block
cache, table-reader, memtable, flush, compaction, pending-byte, delayed-write,
and stall state. It also reports source-journal occupancy and per-consumer lag,
plus mutation-receipt occupancy and projected capacity. Collection runs on a
blocking worker and optional kernel or RocksDB properties fail independently,
so telemetry sampling cannot stall or fail the storage path.

`keldra.index.partition_pipeline` and `keldra.index.compaction` are
phase-lifetime spans. They contain stable numeric recipe, partition, tenant,
and bucket IDs for investigation, plus progress snapshots and terminal outcome.
A phase emits at start, at most one heartbeat every 30 seconds, and at
completion or failure; it does not emit per-record, per-frame, or per-block
logs.

The OSV qualification tool separately writes a measured JSON
report. It is benchmark output, not an OTLP process metric.

Two requested signals are deliberately not approximated in 0.5.2.
The store does not expose an exact instantaneous orphan-byte/count inventory,
and the atomic engine does not expose lock acquisition separately from its
combined prepare operation. Keldra does not add full-store scans or new durable
state solely to manufacture those numbers. Separate durability-wait and
finalized-through-lag instruments are also absent. Garbage-collection results
and combined preparation timing are the bounded alternatives.

The qualification tool's report is the end-to-end OSV ingest
measurement. Its `result` section records `end_to_end_seconds`, source records
and logical mutations per second, `total_payload_bytes`, and bulk-request
latency quantiles for the exact pinned corpus run. Byte throughput is derived
from the reported total and duration rather than emitted as a second
measurement.
