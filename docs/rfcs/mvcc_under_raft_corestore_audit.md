# MVCC-under-Raft CoreStore production-use audit

Audit date: 2026-07-26

Scope: production Rust call sites outside `anvil-core/src/core_store`. Test-only
code and the concurrently edited PersonalDB, admin-audit, and tenant-audit
groups are excluded.

## Allowed immutable byte, segment, and shard I/O

- `authz_segment.rs`, `authz_segment/query.rs`, `authz_userset_index.rs`
- `full_text_segment.rs`, `git_source_index.rs`, `registry_segment.rs`
- `typed_field_segment.rs`, `vector_segment.rs`, `writer_segment_range.rs`
- immutable-byte portions of `index_builder.rs`, `object_manager.rs`, and
  `gateway_store.rs`

These call sites store or retrieve immutable artifacts, segments, logical-file
bytes, blocks, or erasure shards. Their locators and visible product state must
remain in MVCC, but the bytes themselves correctly remain in CoreStore.

## Allowed external mesh and security-control ingestion

- `mesh_control_stream/store.rs`, `mesh_control_segment.rs`
- `mesh_lifecycle.rs`, `mesh_lifecycle/topology_mutation.rs`
- `mesh_directory/helpers.rs`, `mesh_directory/listing.rs`
- `node_identity.rs`

These are cluster/region/mesh topology ingestion or durable node-identity
operations. They are not ordinary product rows. Mesh directory projections
should nevertheless be revisited after the transaction cutover because several
helpers still publish mutable CoreMeta rows directly.

## Allowed internal durability machinery

- `mvcc_bootstrap.rs`, `mvcc_outbox.rs`
- `partition_fence/coremeta.rs`
- CoreStore construction in `lib.rs`
- shard/task recovery portions of `worker.rs`
- internal object/stream RPC adapters

These calls implement the durability substrate, assignment fencing, prepared
bundle materialisation, or physical transport used by MVCC.

## Forbidden mutable product state still requiring cutover

- `metadata_journal/object_mutation.rs` and `metadata_journal/helpers.rs`:
  legacy physical metadata stream/head and mutation paths remain.
- `gateway_store.rs`: mutable gateway manifests, routes, and watch-stream state
  remain mixed with allowed immutable logical-file bytes.
- `system_realm.rs`: mutable system-realm bootstrap/current rows remain
  physically authoritative.
- `bucket_journal.rs`: legacy physical test/helper paths remain. Production
  bucket current state already uses MVCC; the production watch event/head path
  was moved to MVCC in the commit accompanying this audit.
- `append_journal.rs` and `append_journal/read.rs`: physical append journal
  state requires a dedicated cutover assessment.

## Bucket watch cutover evidence

The bucket mutation plan now writes an immutable event row keyed by
`(tenant_id, collection_revision)` in the same MVCC transaction as both current
bucket projections, the event head, allocator, and collection revision.
Production latest-event and paged-watch reads use a single applied MVCC
snapshot. The service no longer subscribes to the removed physical stream; it
polls the MVCC event index while preserving cursor semantics.

Static searches after the change show no production caller of the old bucket
watch stream API. Legacy physical bucket helpers remain isolated to test-only
coverage and should be deleted when that test group is rewritten for MVCC.
