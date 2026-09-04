# Keldra architecture

Status: current high-level architecture for Keldra 0.16

This document describes the system Keldra is intended to operate as. It is the
starting point for reviewing implementation choices and more detailed RFCs. If
a detailed RFC conflicts with this document, the conflict must be resolved
explicitly; it must not be hidden behind an extra readiness check, authority,
or compatibility layer.

## 1. Design rules

Keldra follows a small set of architectural rules:

1. Each fact has one clear authority. Caches, indexes, reconstructed payloads,
   and gateway materializations do not become competing authorities.
2. Raft contains bounded cluster decisions, not application objects or data
   migration inventories.
3. Any publicly reachable node may act as a gateway. It routes work to the
   active replicas that own the data.
4. A joining node becomes publicly usable after its membership has been
   acknowledged through Raft and applied locally. Data movement and replica
   activation continue in the background.
5. A stale placement or executor response causes the caller to refresh the
   existing Raft-derived routing view and retry through the existing peer API.
6. Correctness-critical metadata is committed synchronously. Rebuildable and
   placement work proceeds asynchronously where the requested durability allows
   it.
7. Keldra 0.16 is a clean storage and protocol generation. It does not provide
   backward compatibility with earlier on-disk or protocol versions.

There is no separate proxy authority, readiness authority, per-object ownership
registry, or durable migration-progress database.

## 2. System overview

Keldra has a small consensus control plane and a distributed data plane:

```text
                         public gRPC / S3 / Git / HTTP
                                      |
                              any admitted node
                                      |
                         authenticate and authorize
                                      |
                    derive current owner from Raft state
                                      |
                     existing authenticated peer APIs
                                      |
             +---------------- active replica group ----------------+
             |                         |                             |
          node A                    node B                        node C
       RocksDB + payloads        RocksDB + payloads            RocksDB + payloads
             |                         |                             |
             +------ journals and rebuildable index projections ----+

        Raft: identity, membership, placement fence, capabilities,
              peer pins, cluster constants, bounded atomic decisions
```

The Raft group decides who the nodes are and which cluster-wide epoch is
current. Stable object identity plus that committed state deterministically
selects replicas. The selected replicas, not Raft, store object metadata and
payloads.

## 3. Authorities

### 3.1 Raft control plane

OpenRaft is authoritative for the bounded state required for all nodes to make
the same cluster decisions:

- cluster identity;
- Raft voter and learner membership;
- admitted node descriptors and whether a node is `JOINING` or `ACTIVE`;
- peer addresses, certificate pins, and storage weights;
- the active placement term and log index;
- selected protocol and storage capabilities;
- the cluster JWT-key fingerprint and erasure-code profile;
- bootstrap completion; and
- bounded atomic-program decisions and replay evidence.

Raft does not store object paths, object versions, payload bytes, authorization
tuples, index segments, or a record of which node owns each object.

OpenRaft membership and the Keldra node state answer different questions. A
voter or learner participates in consensus replication. An `ACTIVE` descriptor
participates in data placement. A `JOINING` node is a cluster member and may
serve as a gateway, but it is not yet selected as an authoritative data owner.

### 3.2 Object authority

A public object address is `(tenant name, bucket name, path)`. Tenant and bucket
names resolve to stable numeric identities before persistence or placement.
The authoritative current state of an object is its replicated head plus its
immutable version descriptor.

Mutation receipts provide bounded idempotent replay evidence. Reference proofs
bind journal positions to exact committed mutations for recovery and reference
delivery. Neither replaces the object head as object-state authority.

### 3.3 Payload authority

An immutable payload is identified by `BlobRef { BLAKE3 hash, length }`.
Physical files, RocksDB keys, erasure fragments, reconstructed scratch files,
and caches are representations of that identity; they are not independent
logical objects.

### 3.4 Authorization authority

Authentication establishes the caller. Zanzibar is the authorization
authority. Authentication and authorization are applied independently, and a
failed or malformed credential does not fall back to anonymous access.

### 3.5 Projection authority

Indexes, local Git repositories, query caches, and reconstructed payloads are
disposable projections. They can improve performance but cannot prove that an
object exists, is current, or is accessible. Queries must re-establish current
object state and authorization before returning results.

## 4. Node lifecycle

### 4.1 Fresh cluster

A fresh cluster starts with one Raft voter and records the current Keldra 0.16
protocol and storage capability, currently `2/2`. This is independent of node
count: a one-node cluster does not start in a legacy capability mode and does
not require a later activation ceremony.

The initial node binds its durable storage roots, establishes cluster identity
and immutable cluster settings, creates the protected system records when
requested, starts the normal runtimes, and opens the public listener.

### 4.2 Joining a cluster

An operator supplies a single-use join bundle. The existing cluster
authenticates it and commits the node's membership and `JOINING` descriptor
through Raft. The new process starts its private mTLS peer listener and applies
the admitted cluster state locally.

Once that membership is applied, startup constructs the same object, index,
authorization, and gateway services used by every other node and opens the
public listener. It does not wait for the joining node to receive a full copy of
cluster data.

While the descriptor is `JOINING`:

- the node accepts public requests as a gateway;
- it derives owners from the current `ACTIVE` set;
- it forwards reads and writes through the existing typed peer APIs;
- the destination independently authenticates the peer, authorizes the caller,
  and validates the placement fence; and
- an explicit stale-placement or moved-executor response refreshes the
  Raft-derived routing view and retries within the original request deadline.

There is no additional readiness record or proxying authority. The Raft-applied
membership is the admission fact; ordinary routing and peer connections do the
proxying.

### 4.3 Background activation

Replica preparation and data movement run after gateway service has begun.
Placement continues to exclude the joining node until the existing membership
transition completes and Raft commits its descriptor as `ACTIVE` with a new
placement fence. Only that final committed transition makes the node eligible
to own data.

This keeps the public join time bounded by admission, local state application,
and ordinary service startup rather than dataset size. Migration is idempotent
background work derived from the old and new placement functions; it does not
create a persistent per-object ownership registry.

## 5. Placement and routing

Placement is deterministic capacity-weighted rendezvous hashing over:

- the cluster identity;
- placement kind;
- stable tenant/bucket/object identity;
- the committed active-node set and node weights; and
- the active placement fence.

Every node can therefore calculate the same ordered replica group without a
directory lookup. The highest-ranked replica coordinates mutable records, and
the first members of the ranking hold the logical replicas required by the
current topology.

A gateway routes directly to the selected coordinator through the existing
authenticated peer API. Peer-routed requests do not recursively proxy. If the
coordinator or atomic executor changed, the gateway refreshes committed state
and retries the same idempotent request. Generic network failures are not
blindly replayed as mutations.

Raft leadership and object placement are separate. Electing a new Raft leader
does not by itself move objects. A serving fence prevents an old placement
epoch from continuing to coordinate writes after a placement transition.

## 6. Storage layout

Each node has one local RocksDB database for authoritative metadata and
integrated payload artifacts. Durable roots are pinned to the node installation:

- Raft and node state;
- RocksDB metadata/SST storage;
- RocksDB WAL storage; and
- payload storage.

Index scratch space, reconstructed payloads, and gateway caches are explicitly
disposable. A node's RocksDB database is one replica; it is never treated as a
complete cluster-wide database.

Keldra 0.16 uses its current compact durable codecs and storage markers. Earlier
volumes are not opened or migrated in place. Operators must start 0.16 with new
storage and move data through supported public or cluster workflows.

## 7. Writes

An ordinary object mutation follows one path:

1. Authenticate the caller and authorize the requested operation.
2. Resolve stable tenant and bucket identities and bucket governance.
3. Derive the exact replica group and route to its coordinator.
4. Prepare and verify immutable payload bytes, when present.
5. Reconcile the complete current object record from the replica group.
6. Evaluate preconditions once under the current placement and serving fence.
7. Atomically and synchronously persist the local head/version, receipt,
   reference proof, journal event, and related metadata.
8. Replicate the typed mutation and require the topology's metadata quorum.
9. Settle the proven contiguous journal positions.

The acknowledged metadata quorum is `1/1`, `2/2`, or `2/3` according to the
active topology. Replays return the committed receipt rather than performing a
second mutation.

Payload durability is selected separately:

- `LOCAL` requires a verified complete local source and may finish physical
  placement in the background.
- `REPLICATED` additionally waits for the required complete-copy or erasure
  evidence and the corresponding reference effects.

`REPLICATED` never silently degrades to `LOCAL`.

## 8. Reads and repair

Current metadata reads query the exact derived replica group, require quorum
agreement, validate stable tenant/bucket/path identity, and recheck the placement
fence. Responding minority replicas can be repaired from the selected complete
record.

Payload reads use the selected complete-copy or erasure owners. Reconstructed
bytes are written only to disposable scratch space. Length and BLAKE3 identity
are verified before bytes are exposed, followed by a placement-fence recheck.

A joining gateway with no local data uses this same distributed read path. It
does not need a special proxy database or readiness subsystem.

## 9. Journals, references, and recovery

Each metadata coordinator maintains a bounded ordered source journal. The
journal is recovery and downstream-consumption evidence; current object state
remains the authority for object liveness.

Reference delivery processes contiguous journal prefixes, proves mutations
against their metadata replica groups, applies positive and negative payload
reference changes to the current physical owners, and advances idempotent
destination cursors. Source journal and proof retention advances only through a
prefix known safe for every active destination.

Alias-expanded journal entries are proven by the canonical mutation proof and
its exact bounded alias snapshot. Recovery validates the source, stable
tenant/bucket, canonical path, version/deleted state, alias order, and derived
offset before treating an alias event as quorum-proven.

Visibility settlement and physical reference delivery are separate monotonic
cuts. Garbage collection fails closed while current journal prefixes have not
been proven reference-safe.

## 10. Atomic programs

Atomic programs reserve and validate their object paths using capability 2/2.
Raft owns only the compact decision, executor nomination, limits, and bounded
replay evidence. The object changes themselves use the ordinary authoritative
object and journal storage paths.

Any gateway can submit an invocation. It routes to the executor named by the
current Raft state. An explicit moved-executor or stale-placement response
causes a state refresh and retry of the same invocation ID within the original
deadline. A peer-routed request never routes again.

## 11. Indexing

An index definition is an authorized ordinary Keldra object. Compatible logical
TypedJson definitions within one tenant, bucket, and source scope are compiled
into shared physical recipes and one projection family.

The v6 index pipeline consumes authoritative journal order and exact retained
object versions. Its bounded in-memory preparation cache and hot-ingress path
are accelerators; the journal is the replay path. Immutable segments, head
deltas, roots, and checkpoints are durable performance artifacts but remain
rebuildable projections.

Index publication commits immutable artifacts before advancing the small
current pointer. A query pins a projection generation, but candidates still
require fresh Zanzibar authorization and exact-current object/version checks.

## 12. Public surfaces

Native gRPC object, index, PersonalDB, accounting, authorization, credential,
and administration services share one public listener with the S3, Git, and
configured HTTP gateways.

S3 and Git map their operations onto the same object, placement, authorization,
CAS, durability, and read paths. PersonalDB stores manifests, signed payloads,
heads, definitions, and snapshots as protected ordinary objects. None of these
surfaces introduces a second storage authority.

Peer traffic uses a separate TLS 1.3 mutual-TLS listener. Every peer RPC is
checked against the current cluster identity, admitted node descriptor,
certificate pin, and RPC class. A routed public request carries its original
credential and is authorized again at the destination.

## 13. Failure boundaries

Keldra is designed around explicit failure boundaries:

- A mutation is acknowledged only after its required metadata quorum and
  requested payload durability are proven.
- Placement changes are fenced; stale coordinators cannot continue writing.
- Immutable content is checked by identity at every reconstruction boundary.
- Recovery advances only contiguous, proven journal and reference prefixes.
- Index or cache loss affects performance and freshness while rebuilding, not
  object authority.
- A joining node that has not become `ACTIVE` can remain a gateway without being
  selected as a data owner.
- If authority, authorization, integrity, quorum, or durability cannot be
  proven, the operation fails closed.

The architecture does not promise that every public surface is available during
every partial failure. It does promise that optional caches, projections, and
background movement cannot silently weaken object authority or durability.

## 14. Current operational limits

- Data replica groups contain at most three complete logical metadata replicas.
- The current public cluster workflow supports adding and preparing nodes; a
  complete operator-facing remove/reweight workflow is not yet shipped.
- One membership transition is processed at a time.
- TypedJson is the implemented v6 index kind for this release.
- Keldra 0.16 requires fresh storage; there is no in-place compatibility path
  from earlier versions.

These are limits of the present release, not reasons to add new authorities or
make node join wait for full data migration.

## 15. Source map and detailed decisions

The main production entrypoint is `crates/keldra/src/lib.rs::serve`. The primary
implementation authorities are:

- cluster state: `crates/keldra-consensus/src/state_machine.rs` and `types.rs`;
- startup and join: `crates/keldra/src/peer_runtime.rs` and
  `cluster_startup.rs`;
- placement: `crates/keldra/src/placement.rs` and `cluster_placement.rs`;
- object distribution: `crates/keldra/src/object_distribution.rs` and
  `cluster_object_read.rs`;
- integrated storage: `crates/keldra-store/src/store.rs` and
  `crates/keldra-store/src/blob.rs`;
- reference recovery: `crates/keldra/src/reference_delivery.rs`;
- indexing: `crates/keldra/src/index_runtime/` and
  `crates/keldra/src/index_service/`; and
- public/peer boundaries: `crates/keldra/src/lib.rs`, `authentication.rs`,
  `data_peer.rs`, and `peer_runtime.rs`.

Detailed accepted decisions remain in:

- `keldra_0009_atomic_programs.md`;
- `keldra_0010_cluster_distribution.md`;
- `keldra_0018_integrated_payload_storage.md`; and
- `keldra_0020_logical_index_catalog_and_shared_physical_projections.md`.
