# Anvil 0.5 OSV import qualification

`anvil-osv-qualification` measures the complete authoritative Developer Defence
OSV storage shape over Anvil 0.5 `BulkWrite`. It never invokes an atomic
program and it does not create a raw object plus mutable head for every input
document.

This repository contains no OSV corpus or measured result. A valid run pins the
exact ZIP bytes, supplies the acquisition's explicit snapshot day, identifies
the Anvil revision, and targets a clean single-node bucket.

## Qualified shape

The transform is pinned to Developer Defence revision
`ac838a79e5b9fd4aed08d1ac7786e5374b01b733`:

1. Write the immutable `developer-defence.source-definition.v2` document at
   `entities/source-definition/{sha256("source-definition\0osv")}/current.json`.
2. Apply Developer Defence's exact OSV package grouping, normalization,
   derived fields, identities, and deterministic JSON serialization.
3. Build partitioned
   `developer-defence.osv-source-record.ndjson.v1` shards, including the final
   newline; compress each with zstd level 6; and write it immutably at
   `shards/v1/{records_sha256[0..2]}/{records_sha256}.ndjson.zst`.
4. After exact shard versions are known, write the immutable
   `developer-defence.osv-snapshot-manifest.v1` at
   `snapshots/{snapshot_id}/manifest.json`.

The manifest is authoritative for partition, record count, compressed and
uncompressed lengths, hashes, ecosystems, date bounds, object path, and exact
Anvil version. Shard puts intentionally contain no user metadata, and the tool
creates no metadata sidecar.

The snapshot identity is
`osv-{snapshot_day}-{first 24 hex characters of archive sha256}`. The required
`--snapshot-day` is never inferred from the machine clock.

## Run the gate

Create the exact `dd-source-osv-raw` bucket for the qualification application.
It needs bucket list/get/put/manage-policy authority. Before the timer starts,
the tool enables one-way versioning, installs immutable policy for the three
path families above, and uses `ListObjects(limit=1)` to prove the bucket is
empty.

```sh
cargo run -p anvil-osv-qualification --release -- \
  --endpoint http://127.0.0.1:50051 \
  --client-id dd-osv-qualification \
  --client-secret-file /absolute/path/to/mode-0600-client-secret \
  --tenant dd-osv-qualification \
  --bucket dd-source-osv-raw \
  --corpus /absolute/path/to/osv-corpus.zip \
  --corpus-sha256 <64-lowercase-hex-digits> \
  --snapshot-day YYYY-MM-DD \
  --anvil-commit <40-hex-digit-revision> \
  --durability local \
  --shard-uncompressed-bytes 67108864 \
  --batch-size 256 \
  --maximum-batch-payload-bytes 62914560 \
  --concurrency 4 \
  --confirm-clean-target \
  --output /absolute/path/to/result.json
```

The durable secret file must be a regular mode-`0600` file. It is used only to
obtain a short-lived bearer token; neither secret nor token is printed or
stored in the report.

Hash verification, authentication, bucket setup, the empty-target check, and
post-ingest verification are outside the 150-second gate. The timer includes
the source-definition write, ZIP reading, JSON transforms, NDJSON construction,
zstd compression, every shard `BulkWrite`, and the authoritative manifest
write.

Batch construction is deterministic and bounded by operation count, aggregate
payload bytes, and the actual encoded protobuf size. If one compressed shard
exceeds the selected payload cap, the tool fails and asks for a smaller
`--shard-uncompressed-bytes`; it does not silently switch storage shape.

After the timer the tool checks every current source-definition, shard, and
manifest head against the exact receipt version, content length, content type,
and BLAKE3 payload digest. A completed run fails if it exceeds 150 seconds, an
item fails, any mutation replays on the verified-empty target, or verification
is incomplete.

`durability` is a closed choice. `local` is the only valid single-node 0.5.0
qualification mode. `replicated` remains part of the API but is unavailable on
one node and is rejected before the run.

## Comparing batching

Use a fresh bucket per point and compare only identical corpus hashes,
snapshot days, Developer Defence schema revisions, shard thresholds,
durability, Anvil revisions, and hardware. If a run produces `S` shard objects,
the exact mutation count is `S + 2` (source definition plus shards plus
manifest). The report separately records accepted input documents, normalized
source records, compressed payload throughput, batch fill, and p50/p95/p99
request latency.

`baseline-manifest.json` is an unmeasured field template, not a performance
claim. Never fill it with estimated or synthetic values.
