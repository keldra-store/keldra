# Anvil 0.5 OSV import qualification

The executable `anvil-osv-qualification` measures the ordinary `BulkWrite`
path against Developer Defence's real raw OSV object shape. It never invokes
an atomic program.

This repository contains no OSV corpus and no measured result. A run is valid
only when the operator explicitly supplies and hashes the corpus, identifies
the Anvil revision, and points the tool at a clean single-node target.

## Qualified shape

For each accepted JSON document, the tool reproduces the schema observed at
Developer Defence revision
`ac838a79e5b9fd4aed08d1ac7786e5374b01b733`:

1. JCS-canonicalise the source JSON and write
   `raw/osv/{sha256(trimmed id)}/record.json`.
2. Retain the returned Anvil version, JCS-canonicalise a
   `developer-defence.source-raw-record-head.v1` document referring to it, and
   write `raw/osv/{sha256(trimmed id)}/current.json` with an absent
   precondition.

The dependency makes two bulk phases necessary: head payloads cannot be built
until leaf receipts provide their versions. If `N` is the number of accepted
source JSON documents, the logical mutation count is exactly `M = 2N`.
Normalised shard construction and snapshot publication are intentionally not
part of this storage qualification.

## Run the gate

Build or start one Anvil node with an empty data directory. Then run:

```sh
cargo run -p anvil-osv-qualification --release -- \
  --endpoint http://127.0.0.1:50051 \
  --bearer-token-file /absolute/path/to/token \
  --tenant dd-osv-qualification \
  --corpus /absolute/path/to/osv-corpus.zip \
  --corpus-sha256 <64-lowercase-hex-digits> \
  --anvil-commit <40-hex-digit-revision> \
  --durability-class local \
  --batch-size 256 \
  --concurrency 4 \
  --confirm-clean-target \
  --output /absolute/path/to/result.json
```

Omit `--bearer-token-file` only when the node was deliberately started without
authentication. The token is neither printed nor stored in the report.

The tool verifies the ZIP hash before connecting. The 150-second timer includes
ZIP reading, JSON parsing, JCS encoding, both write phases, every `BulkWrite`
response. Hash verification, connection establishment, and post-ingest
read-back are outside that timer. After the timed interval, bounded `HeadObject`
calls verify that every leaf and head is current at the exact version returned
by its receipt.

Anvil 0.5 currently has no list, count, or batch-head operation. Read-back can
therefore prove that all expected `2N` exact versions exist, but cannot prove
the absence of unrelated extra paths. `--confirm-clean-target` is the
operator's explicit assertion covering that missing API capability.

The process exits non-zero if the import exceeds 150 seconds, any item fails,
any operation is replayed on the asserted-clean target, or read-back does not
verify all `2N` objects. JSON is always printed for a completed measured run;
`--output` writes the same report to a file.

`durability_class` is a closed choice. Exact `local` performs the write with
single-node durability and is the value used by this qualification. Exact
`replicated` returns `DURABILITY_UNAVAILABLE` without a mutation in Anvil 0.5.0;
every other value is invalid. The tool sends and records the selected value, and
the timed response boundary includes the requested durability work.

## Comparing batching

Each batch size or concurrency point needs a fresh target because the head
objects are create-only on first import. Retain one report per run and compare
only identical corpus hashes, schema revisions, durability classes, Anvil
revisions, and hardware.

For any pinned corpus:

```text
required source records/second = N / 150
required logical mutations/second = 2N / 150
```

Run with `--batch-size 1 --concurrency 1` to exercise the worst-case
one-operation-per-request shape. Its reported mean and p50/p95/p99 request
latencies show whether the 10 ms one-operation target is achievable; the hard
qualification gate remains the full-corpus 150-second limit.

`baseline-manifest.json` is an unmeasured field template, not a performance
claim. Never fill it with estimated or synthetic values.
