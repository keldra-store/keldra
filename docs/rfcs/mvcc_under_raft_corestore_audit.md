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
- Gateway mutable metadata is now MVCC-only. Remaining `CoreStore` calls are
  immutable blob/upload-part I/O and external security-audit ingestion.
- System-realm bootstrap completion is now canonical MVCC state. Mesh lifecycle
  projections used to authorize internal nodes remain external control input.
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

## Gateway follow-up evidence

Gateway repositories, blob locators, tags and manifests, credentials, upload
sessions, idempotency rows, mounts, and mount routes use MVCC product rows.
Transaction-scoped gateway reads and writes now stage exact `Absent` or
`ValueHash` predicates. Non-transactional package-version and registry-ref APIs
create one internal quorum, linearized MVCC transaction, stage a compact-Raft
assignment guard, and certify all mutable rows together. Gateway metadata
autocommits and upload finalisation request quorum durability.

The remaining production `CoreStore` calls in the gateway group are limited to
immutable registry blob/upload-part bytes and the explicitly external gateway
security-audit stream. No physical mutable-row fallback or gateway test module
remains.

## System-realm follow-up evidence

The cluster-local system-realm bootstrap marker is now an MVCC-v2 product row.
Bootstrap admission obtains a compact-Raft work assignment, performs a
linearized second existence check, and certifies the marker at quorum with an
exact `Absent` predicate and assignment guard. Reads use one applied MVCC
snapshot. The physical bootstrap fence, CoreMeta marker publication, legacy row
common metadata, and disabled physical test suite were removed.

The following dependencies deliberately remain outside this marker row:

- mesh/region/cell/node lifecycle projections are external mesh-control input;
- the node capability check reads those external projections;
- the operator bootstrap credential export is an immutable operator-owned file
  outside Anvil storage;
- authz schema and tuple state are canonical MVCC data owned by their existing
  authz transactions.
