# Anvil 0.7 observability

Anvil always writes its structured `tracing` logs to stdout. OTLP export is an
optional startup setting and carries metrics and traces only; logs remain on
stdout.

Set the standard OpenTelemetry endpoint variable or its equivalent command-line
flag:

```console
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 anvil-server ...
anvil-server --otlp-endpoint http://collector:4318 ...
```

The endpoint is treated as the OTLP HTTP base URL. Anvil uses protobuf over HTTP
and the exporter appends `/v1/metrics` and `/v1/traces`. Anvil's successful
startup log does not print the endpoint. If the option and environment variable
are both absent (or empty), Anvil does not construct an exporter, start an
OpenTelemetry worker, or make a collector connection.

Observability configuration is read once at process startup. Enabling,
disabling, or changing the endpoint requires a restart. There is no metrics
listener, diagnostics listener, public observability RPC, or administration
API.

## Resource and bounds

Metrics and traces share these resource attributes:

- `service.name=anvil`
- `service.version=0.7.0`
- `node.id=<ANVIL_NODE_ID>`

Trace export uses a dedicated worker with a 2,048-span queue and batches of at
most 512 spans. A full queue drops new telemetry instead of blocking request
processing. Metrics use a periodic dedicated worker and cap each instrument at
128 attribute sets, with excess sets folded into the SDK overflow series. Each
HTTP export has a five-second timeout. Shutdown flushes and stops both providers
from a blocking worker so it does not occupy a Tokio request worker.

OTLP setup errors, such as an invalid endpoint, fail startup. Collector
unavailability after startup does not add a dependency to the Anvil data path.

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

The Anvil 0.7 process metric vocabulary covers:

- executor nomination count;
- atomic-program invocation counts, total and combined preparation latency,
  prepared bytes, and Raft commit latency;
- unfinalized tail entries and bytes plus finalization retry count;
- committed-invocation replay-window entries and bytes;
- watch-journal retained-entry and retained-byte gauges plus consumer-lag
  observations whenever a watch starts or advances;
- bulk attempted operations and encoded request bytes, mutually exclusive
  successful, failed, and replayed operation counts, and request latency; and
- blob garbage-collection run, removal, and failure counts.

The OSV qualification tool separately writes a measured JSON
report. It is benchmark output, not an OTLP process metric.

Two requested signals are deliberately not approximated in 0.5.2.
The store does not expose an exact instantaneous orphan-byte/count inventory,
and the atomic engine does not expose lock acquisition separately from its
combined prepare operation. Anvil does not add full-store scans or new durable
state solely to manufacture those numbers. Separate durability-wait and
finalized-through-lag instruments are also absent. Garbage-collection results
and combined preparation timing are the bounded alternatives.

The qualification tool's report is the end-to-end OSV ingest
measurement. Its `result` section records `end_to_end_seconds`, source records
and logical mutations per second, `total_payload_bytes`, and bulk-request
latency quantiles for the exact pinned corpus run. Byte throughput is derived
from the reported total and duration rather than emitted as a second
measurement.
