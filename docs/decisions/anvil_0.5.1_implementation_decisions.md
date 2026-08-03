# Anvil 0.5.1 implementation decisions

This log records implementation decisions made while completing Anvil 0.5.1.
The architecture remains defined by
[`anvil_0010_cluster_distribution.md`](../rfcs/anvil_0010_cluster_distribution.md).
Each entry explains the concrete issue found during implementation, the viable
options considered, and the selected KISS solution. New entries are appended
before the release is tagged.

## Exact membership-transition scope for handoff

### Issue

Mandatory mTLS proves that a private request came from an ACTIVE cluster node,
but that alone does not prove that a state-transfer request belongs to the
currently active membership transition. A delayed request from an earlier ADD
could otherwise arrive while the same peer identity remains valid and attempt
to reinstall stale mutable metadata or lifecycle state.

### Options considered

1. Permit any ACTIVE peer to invoke state transfer and rely on idempotence.
   This does not reject a request from an obsolete transition and is unsafe for
   mutable lifecycle records.
2. Put a generic transaction or placement object into every peer request. This
   conflates ordinary data-plane operations with membership handoff.
3. Add a small handoff-only scope containing the joining node ID and the Raft
   log index that began the transition, then validate it against locally
   applied Raft state.

### Decision

Use option 3. Handoff-only requests carry
`{ joining_node_id, started_log_index }`. The recipient checks that the caller
is the current Raft leader, the exact ADD transition remains current, the
descriptor still names a JOINING node, and an install directed at the joiner is
running on that node. Streaming installs recheck the scope before applying
their final mutable state. The transport binds the scope once for one
`TypedAddHandoff` run.

Immutable content-addressed byte reads and seals remain ordinary typed
data-plane operations: replaying identical verified bytes cannot change
logical state. Lifecycle installation and reference-cursor advancement are
handoff-scoped because they are mutable.

## Placement validation for ordinary peer operations

### Issue

Typed object and Zanzibar mutations already contain their coordinator's active
placement and serving-leader term, but the receiving peer previously validated
only mTLS membership. A request delayed across a membership cutover could
therefore reach a node that was no longer a replica for that placement.

### Options considered

1. Trust the authenticated coordinator and let storage lineage checks reject
   every stale request. This is insufficient for operations without object
   lineage and needlessly writes data to obsolete owners.
2. Ask Raft to decide every private request. That puts ordinary traffic through
   consensus and defeats serving leases.
3. Carry the already captured placement fence on typed peer operations and
   compare it with the recipient's locally applied Raft state. For mutable
   mutations, also compare the embedded serving-leader term.

### Decision

Use option 3. This is a local validation and performs no Raft write or quorum
round trip. A delayed request from the same stable placement remains safe; a
request from another placement or leader term fails closed. Immutable
content-addressed reads and seals do not acquire a synthetic mutable fence, but
their placement-sensitive callers still recheck the captured placement before
accepting the overall operation.

## Tenant-wide Zanzibar placement

### Issue

Placing each schema, realm, tuple, or revision independently would scatter one
tenant's authorization graph across many coordinator groups and turn a normal
authorization decision into a distributed join. Recording millions of such
placements in Raft is not viable.

### Options considered

1. One replica group per schema or realm.
2. One replica group for all Zanzibar state in the cluster.
3. One replica group per stable tenant ID, containing that tenant's schemas and
   realms.

### Decision

Use option 3. Weighted HRW selects one tenant-wide Zanzibar replica group from
the stable `tenant_id`. No per-tenant or per-realm placement record enters
Raft. This keeps authorization local to one small replica group while
distributing different tenants across the cluster.

## Retaining schema-publication lineage for retry

### Issue

A coordinator can durably write a new immutable schema revision locally and
then fail to reach a remote quorum. Retrying the same publication finds the
schema by digest, but the schema bytes alone do not contain the exact original
command ID, predecessor, placement fence, source-journal position, and
mutation fingerprint required by replicas. Merely proving that a remote lacks
the revision cannot repair it.

### Options considered

1. Return `UNAVAILABLE` forever after this failure pattern.
2. Reconstruct a new mutation with new lineage. That would turn one logical
   publication into a sibling mutation.
3. Add a separate retry receipt, repair log, or schema registry.
4. Retain the exact typed publication mutation inside the existing immutable
   schema-revision value.

### Decision

Use option 4. The mutation is immutable lineage for the revision, not a second
registry or an expiring receipt. A retry resends the exact original mutation.
Legacy 0.5.0 schema values remain readable as baseline revisions and are
copied through normal tenant-catalogue handoff.

## Upload ingress and path coordination

### Issue

An upload stream may run for hours. The path's weighted-HRW coordinator must
serialize CAS/publication, but the opaque bytes themselves do not need to pass
through that coordinator while streaming.

### Options considered

1. Proxy `StartPut` and the complete client stream to the path coordinator.
   This makes the coordinator a bandwidth hop and couples a long stream to
   membership routing.
2. Seal the content-addressed upload on the ingress node, identify that source
   in the ready capability, and proxy only `PutEnd` to the path coordinator.

### Decision

Use option 2. `StartPut` and `Put` remain on the authenticated ingress. `PutEnd`
is the small routed operation that evaluates CAS and publishes the head. Before
publication the coordinator obtains the byte-plane evidence required by the
requested acknowledgement policy; durable source-journal work drives any
remaining placement. This avoids proxying a multi-hour stream without
weakening path serialization.

## Payload lifecycle ordering during handoff

### Issue

The existing lifecycle column family deliberately uses different durable keys
for complete blobs and erasure-coded shards. Raw RocksDB order therefore does
not place all artifacts for one blob next to each other. Retaining every
artifact to regroup it would make handoff memory proportional to stored data.

### Options considered

1. Add a new persisted index or change lifecycle keys.
2. Scan the complete and shard classes separately and ignore cross-class
   inconsistencies. This cannot fail closed when a large complete source and
   its shards disagree.
3. Expose an ephemeral blob-first handoff order by merging two filtered
   iterators over the existing column family.

### Decision

Use option 3. The non-authoritative handoff cursor is
`[blob_hash][length][kind][ordinal]`. Two RocksDB iterators retain one lookahead
per key class and emit one bounded sorted page; persisted keys do not change
and no index or column family is added. Cluster merge retains evidence for only
one blob at a time.

Lifecycle replicas must agree on logical `ref_count` and flags. Physical
timestamps may legitimately differ, so handoff preserves the earliest observed
`created_at` and latest `updated_at`; installation never moves a target's
`updated_at` backwards. This prevents handoff from shortening the GC grace.

## Reference-proof retirement

### Issue

Reference proofs are needed while a source-journal event may still need
classification for a lagging destination. Deleting from a metadata replica as
soon as that replica's own reference cursor advances can turn a valid proof
quorum into a mixture of presence and absence while another ACTIVE destination
still needs the event.

### Options considered

1. Add proof-deletion acknowledgements and a replicated cleanup watermark.
2. Delete according to each replica's local destination cursor.
3. Use the source journal's existing durable `retention_floor` as the deletion
   watermark and prune locally in bounded pages.

### Decision

Use option 3. A source can advance its durable retention floor only through the
minimum cursor of every ACTIVE destination under one stable placement. Every
node periodically reads each ACTIVE source's status, rechecks the placement,
and synchronously deletes bounded local proof pages at or below that source's
floor. Cleanup progress is derived and need not be persisted or acknowledged.

Reference proofs are not transferred during normal ADD handoff: before
activation, every old ACTIVE destination and the joiner are advanced to the
frozen source tails. A source that is unavailable prevents proof cleanup and
keeps payload GC disabled rather than guessing.

