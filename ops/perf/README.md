# Anvil 0.5 OSV import qualification

`anvil-osv-qualification` measures a deterministic OSV import over Anvil 0.5
`BulkWrite`. It never invokes an atomic program and it does not create a raw
object plus mutable head for every input document.

This repository contains no OSV corpus or measured result. A valid run pins the
exact ZIP bytes, supplies the acquisition's explicit snapshot day, identifies
the Anvil revision, and targets a clean bucket.

## Qualified shape

The qualification uses one checked-in deterministic transform:

1. Write the immutable `anvil.osv.source-definition.v1` document at
   `entities/source-definition/{sha256("source-definition\0osv")}/current.json`.
2. Group OSV package records, normalize their fields, derive stable identities,
   and serialize deterministic JSON.
3. Build partitioned
   `anvil.osv.source-record.ndjson.v1` shards, including the final
   newline; compress each with zstd level 6; and write it immutably at
   `shards/v1/{records_sha256[0..2]}/{records_sha256}.ndjson.zst`.
4. After exact shard versions are known, write the immutable
   `anvil.osv.snapshot-manifest.v1` at
   `snapshots/{snapshot_id}/manifest.json`.

The manifest is authoritative for partition, record count, compressed and
uncompressed lengths, hashes, ecosystems, date bounds, object path, and exact
Anvil version. Shard puts intentionally contain no user metadata, and the tool
creates no metadata sidecar.

The snapshot identity is
`osv-{snapshot_day}-{first 24 hex characters of archive sha256}`. The required
`--snapshot-day` is never inferred from the machine clock.

## Run the gate

Create the `anvil-osv-qualification` bucket for the qualification application.
It needs bucket list/get/put/manage-policy authority. Before the timer starts,
the tool enables one-way versioning, installs immutable policy for the three
path families above, and uses `ListObjects(limit=1)` to prove the bucket is
empty.

```sh
cargo run -p anvil-osv-qualification --release -- \
  --endpoint http://127.0.0.1:50051 \
  --client-id anvil-osv-qualification \
  --client-secret-file /absolute/path/to/mode-0600-client-secret \
  --tenant anvil-osv-qualification \
  --bucket anvil-osv-qualification \
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

`--concurrency` controls archive parsing and compression. To stripe data writes
across a cluster, repeat `--write-endpoint` once per node. The qualifier keeps
at most one `BulkWrite` request active on each configured endpoint; setup,
manifest publication, and verification continue to use the primary `--endpoint`.
Explicit endpoint values must be distinct. When no write endpoint is supplied,
the original `--concurrency` number of write slots all use the primary endpoint.

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

`durability` is a closed choice. `local` is the only valid mode for this 0.5.1
qualification. `replicated` remains part of the API and requires enough active
nodes for the configured erasure profile, so the tool rejects it before a run.

## Comparing batching

Use a fresh bucket per point and compare only identical corpus hashes,
snapshot days, qualification schema revisions, shard thresholds,
durability, Anvil revisions, and hardware. If a run produces `S` shard objects,
the exact mutation count is `S + 2` (source definition plus shards plus
manifest). The report separately records accepted input documents, normalized
source records, compressed payload throughput, batch fill, and p50/p95/p99
request latency.

`baseline-manifest.json` is an unmeasured field template, not a performance
claim. Never fill it with estimated or synthetic values.
