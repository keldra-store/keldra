# MVCC-under-Raft CoreStore production-use audit

Audit date: 2026-07-26

Scope: production Rust call sites outside `anvil-core/src/core_store`, checked
against `mvcc_under_raft.md`. Test-only code is excluded. This audit classifies
every remaining direct physical CoreStore mutation site and the product
features that retain CoreStore reads or immutable-byte writes.

## Result

No mutable cluster-product row, stream head, product root publication, or
product precondition remains physically authoritative in CoreStore.

Cluster product state is certified through MVCC. Non-transactional product
autocommits use quorum durability; caller-owned transactions retain their
explicit durability level. Product audit and strict outbox rows are composed
into the originating transaction.

The remaining physical CoreStore mutation sites are limited to:

- immutable data, segment, logical-file, block, and erasure-shard storage;
- MVCC durability and assignment-fencing internals;
- external mesh, region, topology, routing, and security-control ingestion.

There are no `#[cfg(any())]` compatibility paths in production Rust source.

## Immutable bytes, segments, and shards

The following product groups use CoreStore only for immutable artifacts or for
reading those artifacts:

- `authz_segment.rs`, `authz_segment/query.rs`, `authz_userset_index.rs`;
- `full_text_segment.rs`, `git_source_index.rs`, `registry_segment.rs`;
- `typed_field_segment.rs`, `vector_segment.rs`, `writer_segment_range.rs`;
- immutable-byte portions of `index_builder.rs`, `object_manager.rs`, and
  `gateway_store`;
- PersonalDB segment, changeset, snapshot, schema, row-index, and projection
  payloads.

Their visible locators, heads, manifests, indexes, and other mutable product
metadata are MVCC rows. Writing a PersonalDB logical file before publishing its
locator is an immutable-byte operation; the locator is subsequently published
through the PersonalDB MVCC transaction.

## MVCC and fencing internals

These physical operations are implementation machinery rather than ordinary
product authority:

- `mvcc_bootstrap.rs`, `mvcc_outbox.rs`, and bundle materialisation;
- `partition_fence/coremeta.rs`;
- shard transfer, repair, placement, and prepared-bundle recovery;
- CoreStore construction and recovery-readiness wiring;
- internal object, stream, and consensus transport adapters.

`partition_fence/coremeta.rs` is the only production physical
`commit_mutation_batch` site outside the external-control and direct-audit
groups below. It stores assignment-fencing evidence used by the compact
consensus boundary.

## External mesh and topology control

The following physical mutation sites are outside the cluster transaction
domain by design:

- `mesh_control_stream/store.rs`;
- `mesh_lifecycle.rs` and `mesh_lifecycle/topology_mutation.rs`;
- `mesh_directory/helpers.rs`;
- `mesh_control_segment.rs`;
- `node_identity.rs`.

They ingest or project mesh, region, cell, node, and routing control state.
Moving these rows into a cluster product transaction would incorrectly make
one cluster authoritative for a wider mesh-control consequence.

## Direct security and topology audit ingestion

`admin_audit.rs` permits physical direct ingestion only for its explicit
external control-plane allowlist. Cluster product actions must use
`admin_audit_mvcc_plan` in the transaction that changes their product state.
Single and batch application-policy changes now stage their audit rows with the
authz tuple mutation.

`tenant_audit.rs` permits physical direct ingestion only for:

- `host_alias.create`;
- `host_alias.verify`;
- `host_alias.delete`.

Those operations alter external mesh-routing control. Every other tenant
action is rejected by `require_direct_tenant_audit_action` and must compose
`tenant_audit_mvcc_plan` into its originating cluster MVCC transaction.

## Product cutover evidence

The following formerly physical product authorities are now MVCC-only:

- bucket current rows, allocators, watch events, revisions, and lifecycle
  mutations;
- object metadata events, current projections, watch rows, manifests, audit,
  and outbox consequences;
- append stream heads, record indexes, cursors, state, and sealing metadata;
- gateway repositories, tags, manifests, routes, credentials, upload sessions,
  idempotency records, mounts, and mount routes;
- authorization schemas, tuples, usersets, journals, projections, and watches;
- index definitions, locators, diagnostics, proof checkpoints, and watches;
- full-text journals and projections;
- tasks, leases, repair findings, and repair overlays;
- Git source manifests and watches;
- system-realm bootstrap state;
- PersonalDB admission, committed heads, snapshot heads, group rows, and data
  locators.

Index-definition compatibility functions that retained the old physical API
were deleted. The live collection-revision and paginated-list APIs read MVCC
state and are no longer hidden behind disabled configuration.

Control, index-definition, index-diagnostic, manifest, and multipart product
writers no longer construct physical partition preconditions and discard them.
They validate the permit scope, carry its fence token in the product record,
and rely on the transaction's compact-Raft assignment guard and exact MVCC
predicates for authority.

Mutable bucket, index, gateway-mount, native-idempotency, boundary-migration,
and PersonalDB autocommits request quorum durability.

## Static verification boundary

The final source search for physical `commit_mutation_batch`,
`stream_head_precondition`, and `prepare_mutation_batch` calls outside
`core_store` resolves only to:

- `partition_fence/coremeta.rs`;
- `mesh_control_stream/store.rs`;
- `mesh_lifecycle.rs` and `mesh_lifecycle/topology_mutation.rs`;
- `mesh_directory/helpers.rs`;
- `admin_audit.rs`;
- `tenant_audit.rs`.

These are exactly the internal-fencing and external-control categories
documented above. No known production mutable product cutover remains in this
audit.

This is a source-level classification. Compilation, focused tests, multi-node
fault tests, and end-to-end durability verification remain separate acceptance
gates and are not claimed by this document.
