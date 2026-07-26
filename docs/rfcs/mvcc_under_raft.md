# MVCC Under Raft: Minimal Consensus for Anvil Transactions

Status: Proposed for review

Audience: Anvil implementors, storage engineers, operators, and reviewers

Scope: transaction ordering, MVCC visibility, atomic transactions, replication,
durability, OpenRaft integration, local storage, recovery, materialisation, and
garbage collection

## 1. Decision

Anvil will use a global MVCC commit sequencer backed by OpenRaft. Raft is used
only to agree on compact cluster-control decisions and compact transaction
certification decisions. Object bytes, CoreMeta rows, transaction bundles,
indexes, streams, authorization data, jobs, manifests, and erasure-coded shards
are never replicated through the Raft log.

Transaction data is transferred between nodes over persistent, authenticated,
bidirectional gRPC streams. A receiver acknowledges data at the application
layer only after the acknowledged bytes are durably stored. Transport-level
success is not a durability acknowledgement.

Every transaction is certified with one Raft command, regardless of how many
logical keys, tables, buckets, tenants, or product features it changes.
Certification either commits the complete transaction at one global commit
version or commits none of it. There is no distributed participant protocol and
no separate per-feature commit.

The initial implementation uses one cluster-wide OpenRaft group for transaction
certification and cluster control. Transaction bundles are replicated outside
Raft before certification. This deliberately chooses a simple global ordering
point over sharded consensus. Consensus sharding may be considered later only
if measurements demonstrate that the global sequencer is a material bottleneck.

OpenRaft `0.9` will be used behind an Anvil-owned adapter. OpenRaft durable state
will be stored in dedicated column families in the same local RocksDB database
as CoreMeta. Anvil will not introduce another authoritative local database for
Raft.

Anvil will not sign ordinary node-to-node messages, transaction receipts, Raft
entries, CoreMeta rows, manifests, or replication acknowledgements. Nodes
authenticate and authorize one another when a connection is established or
re-established. The authenticated connection session authorizes subsequent
messages. TLS provides transport confidentiality and integrity; content hashes
and checksums detect incomplete transfer and storage corruption.

This is a clean replacement architecture for an alpha-stage product. It carries
no storage-format transition machinery and no dual-write compatibility mode.
Public APIs should remain stable where their behavior fits this model, but
breaking API changes are permitted when required for coherent transactional or
durability semantics.

## 2. Motivation

Anvil needs:

- large bytes outside RocksDB;
- fast durable writes;
- atomic metadata updates;
- consistent snapshots;
- automatic recovery and failover;
- configurable physical durability;
- asynchronous production of efficient final data representations.

Those requirements do not require several independently durable foreground
protocols for one logical operation.

MVCC provides a direct visibility model: immutable transaction data is invisible
until one certification decision assigns it a commit version. Raft provides only
the property MVCC cannot provide by itself: one irreversible answer about
transaction order and outcome when nodes disagree, fail, or lose connectivity.

The data path and decision path are therefore separated:

```text
data path       persistent gRPC streams + local RocksDB/raw files
decision path   compact OpenRaft certification commands
```

## 3. Goals

- Provide serializable atomic transactions across arbitrary logical keys and
  product features.
- Give every committed transaction one global, monotonically increasing commit
  version.
- Use MVCC versions for visibility, snapshots, conflict detection, recovery,
  retained history, and garbage collection.
- Keep object bytes and ordinary CoreMeta rows outside the Raft log.
- Replicate transaction data over persistent gRPC streams with durable
  application acknowledgements.
- Preserve acknowledged-write safety according to an explicit durability level.
- Store OpenRaft state in the existing local RocksDB instance.
- Keep foreground writes free of erasure coding unless the caller explicitly
  requests erasure durability.
- Make erasure coding, placement improvement, derived indexing, and other
  maintenance durable background jobs.
- Remove per-request and per-record cryptographic signatures from internal node
  communication.
- Provide deterministic recovery after crashes, leader changes, connection
  loss, retries, and partially completed background work.
- Define the minimum state permitted in Raft.
- Avoid artificial transaction boundaries based on storage organization.

## 4. Non-Goals

- Implement the Raft algorithm in Anvil.
- Replicate all CoreMeta state through Raft.
- Replicate object payloads through Raft.
- Make libp2p, gRPC, RocksDB, or MVCC act as an implicit consensus protocol.
- Provide write availability on both sides of a network partition.
- Acknowledge `local` durability as though it survived node loss.
- Require final erasure-coded placement before acknowledging `local` or
  `quorum` writes.
- Preserve existing internal storage formats or internal transaction machinery.
- Add storage-format upgrade logic or a dual-protocol operation mode.
- Preserve a public API when doing so would retain misleading or incorrect
  semantics.

## 5. Normative Language

The words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, and MAY are normative.

## 6. Terminology

This section is normative. Implementations and subsequent design documents must
use these terms consistently.

### 6.1 Node

A **node** is one running Anvil storage-server identity with its own local
RocksDB database and local byte storage.

A restarted process using the same durable identity remains the same node. A
replacement process that cannot prove continuity with that durable identity is
a new node incarnation.

### 6.2 Node ID

A **node ID** is the stable identifier assigned to a node. It is not an IP
address, socket address, process ID, or container ID.

### 6.3 Node Incarnation

A **node incarnation** is:

```text
NodeIncarnation = (node_id, incarnation_number)
```

The incarnation number increases when a node is re-created without continuity
of its prior durable state. Durable acknowledgements name an incarnation so an
acknowledgement from an obsolete process cannot be mistaken for one from its
replacement.

### 6.4 Cluster

A **cluster** is the set of nodes governed by one Anvil cluster identity and one
initial consensus group.

### 6.5 Consensus Group

A **consensus group** is an OpenRaft group containing voters and optionally
learners. The initial architecture has one consensus group for the cluster.

### 6.6 Voter

A **voter** is a node that participates in OpenRaft elections and quorum
decisions.

### 6.7 Learner

A **learner** receives consensus state without voting. It may be promoted
through OpenRaft membership change after catching up.

### 6.8 Leader

The **leader** is the current OpenRaft leader. Only the leader may successfully
sequence new certification decisions. Leadership is a consensus role, not a
claim that all transaction data must pass through that node.

### 6.9 Realm

A **realm** is a durable administrative and authorization namespace. Realms
separate tenants, system state, or other independently governed scopes.

### 6.10 Logical Key

A **logical key** identifies one logical value before MVCC versioning:

```text
LogicalKey = (table_id, encoded_application_key)
```

Examples include an object head, object version, bucket record, authorization
tuple, task, stream position, index definition, or idempotency result.

Logical-key encoding must preserve the ordering required for point, prefix, and
range access.

### 6.11 Key Range

A **key range** is a half-open ordered interval:

```text
[start_logical_key, end_logical_key)
```

Prefix reads are represented as their corresponding ordered interval.

### 6.12 Table

A **table** is a typed collection of logical keys and values within CoreMeta. A
table is a schema and key-space concept, not an independent database,
replication protocol, or transaction boundary.

### 6.13 Partition

A **partition** is a routing and physical-placement unit used to distribute
transaction bundles and data. It does not constrain transaction atomicity. One
transaction may affect any number of partitions.

### 6.14 Transaction

A **transaction** is an atomic set of reads, predicates, metadata mutations,
byte references, events, and durable jobs.

A transaction may read or modify any number of logical keys, tables,
partitions, realms, and product features, subject only to authorization and
resource limits. It has exactly one final outcome:

```text
Committed(commit_version)
Aborted(reason)
```

### 6.15 Transaction ID

A **transaction ID** is a globally unique, stable identifier for a transaction
attempt. Retries of the same logical attempt reuse the same transaction ID.

The certification state retains transaction outcomes for an idempotency window
so a retry returns the original result.

### 6.16 Transaction Coordinator

The **transaction coordinator** is the node handling a transaction attempt. It
builds and replicates the transaction bundle and submits the certification
command.

The coordinator is not a durable authority. Its failure cannot change a
certification result.

### 6.17 Transaction Bundle

A **transaction bundle** is the immutable, canonically encoded data body of a
transaction. It is transferred and persisted outside Raft.

It contains:

- transaction identity and snapshot version;
- MVCC puts and deletes;
- read observations and predicates;
- object and raw-byte references;
- idempotency results;
- watch and outbox events;
- materialisation jobs;
- authorization changes;
- index-maintenance jobs;
- user-visible metadata.

The bundle is identified by a content hash. A committed certification decision
refers to its hash and length, not its body.

### 6.18 Prepared Bundle

A **prepared bundle** is a transaction bundle durably stored on a node but not
yet assigned a committed certification result.

Prepared bundles are invisible to ordinary reads.

### 6.19 Raw Object

A **raw object** is the first durable representation of an object's bytes. It is
immutable and content addressed.

A raw object is stored directly in a local append-only data segment. It is not a
disposable upload copy and is not a separate admission-protocol object. It may
serve reads until background materialisation creates a better representation.

### 6.20 Raw Replica

A **raw replica** is one complete, hash-verified, fsynced raw-object copy on one
node incarnation.

### 6.21 Materialised Object

A **materialised object** is a final or improved physical representation,
including compressed, encrypted, chunked, deduplicated, or erasure-coded bytes
and their placement metadata.

Materialisation does not change the logical object's commit version.

### 6.22 MVCC

**Multi-Version Concurrency Control (MVCC)** stores multiple committed versions
of a logical key and makes visibility a function of a read snapshot.

A versioned row is conceptually identified by:

```text
(logical_key, commit_version)
```

### 6.23 Snapshot Version

A **snapshot version** is the greatest global commit version visible to a
transaction's ordinary reads.

Every read in a transaction uses the same snapshot version unless a public API
explicitly requests weaker behavior.

### 6.24 Commit Version

A **commit version** is the globally ordered version assigned to a committed
transaction. The initial implementation derives it from the committed Raft log
position.

Commit versions are unique and monotonically increasing. They need not be
contiguous because aborted certifications, membership entries, and control
decisions may consume log positions.

### 6.25 Row Version

A **row version** is one committed value or tombstone for one logical key at one
commit version.

### 6.26 Head

A **head** is a local performance index from a logical key to its newest locally
applied committed row version. It is updated atomically with the row version.

A head is not a separate publication or consensus object.

### 6.27 Tombstone

A **tombstone** is a row version indicating that its logical key is deleted as
of the tombstone's commit version.

### 6.28 Read Set

A transaction's **read set** contains the point observations, range
observations, and explicit predicates whose state influenced the transaction.

### 6.29 Write Set

A transaction's **write set** contains its logical-key puts and deletes.

### 6.30 Point Observation

A **point observation** records the version observed for one logical key:

```text
PointObservation = (logical_key_hash, observed_version_or_absent)
```

### 6.31 Range Observation

A **range observation** records the version of an ordered interval used by a
transaction:

```text
RangeObservation =
    (table_id, start_key_hash, end_key_hash, observed_range_stamp)
```

Range observations prevent phantoms without listing every row returned.

### 6.32 Range Stamp

A **range stamp** is a monotonically increasing conflict marker for a bounded
portion of ordered key space. A write advances every range stamp required by
the configured range-indexing scheme.

The range-stamp scheme must guarantee that any insertion, deletion, or update
that could change a prior range result invalidates that range observation.

### 6.33 Conflict Key

A **conflict key** is the compact identifier used by certification to serialize
conflicting operations. Point conflict keys represent logical keys; range
conflict keys represent range stamps or explicit predicates.

Conflict keys are transaction-certification metadata, not product data.

### 6.34 Certification

**Certification** is deterministic validation that decides whether a
transaction may commit after the transactions ordered before it.

Certification is performed by the OpenRaft state machine using compact conflict
state. It does not read arbitrary local product data.

### 6.35 Certification Command

A **certification command** is the compact application entry proposed to
OpenRaft. It identifies the prepared bundle, requested durability, read
observations, and write conflict keys.

### 6.36 Certification Result

A **certification result** is the committed or aborted outcome produced when a
certification command is applied.

### 6.37 Durable Holder

A **durable holder** is a node incarnation that has acknowledged complete,
hash-verified, fsynced storage of a transaction bundle and any raw bytes required
by the requested durability level.

### 6.38 Durability Level

A **durability level** determines which physical writes must finish before
certification may be proposed:

```text
local
quorum
erasure
```

Durability level is distinct from read consistency and transaction isolation.

### 6.39 Read Consistency

**Read consistency** controls how a reader selects or verifies its visible
snapshot. Examples include a local snapshot read and a leader-confirmed
linearized read.

Read consistency does not determine how many physical copies a write has.

### 6.40 Transaction Isolation

**Transaction isolation** defines which concurrent histories are permitted.
This RFC requires serializable outcomes through optimistic MVCC certification.

### 6.41 Application Acknowledgement

An **application acknowledgement** is a replication response emitted after the
receiver has performed the operation represented by the acknowledgement.

A `Persisted` or `Complete` acknowledgement is not inferred from TCP delivery,
HTTP/2 flow control, or successful gRPC message transmission.

### 6.42 Connection Session

A **connection session** is one authenticated node-to-node connection lifetime.
It has a unique session ID and is bound to the authenticated peer's node
incarnation.

### 6.43 Materialisation Job

A **materialisation job** is an ordinary durable MVCC row committed atomically
with the logical state requiring background transformation.

Materialisation jobs are not Raft entries.

### 6.44 Outbox Event

An **outbox event** is a durable event row committed atomically with the state
change it describes. Watch delivery, index maintenance, replication catch-up,
and other derived work consume outbox events.

### 6.45 Applied Watermark

A node's **applied watermark** is the greatest consecutive commit version for
which the node has applied every required transaction bundle locally.

### 6.46 Garbage Collection Watermark

A **garbage collection watermark** is a commit version below which obsolete
MVCC versions may be removed, subject to active snapshots, retention policy,
replica catch-up, backup, and audit requirements.

## 7. Architecture Overview

```text
                              client request
                                    |
                                    v
                     authn, authz, validation, routing
                                    |
                                    v
                         read at MVCC snapshot S
                                    |
                                    v
                   build immutable transaction bundle
                                    |
                   +----------------+----------------+
                   |                                 |
                   v                                 v
          local raw data segments          local prepared bundle
                   |                                 |
                   +---------- persistent gRPC ------+
                                    |
                       remote durable acknowledgements
                                    |
                                    v
                      OpenRaft certification command
                      compact decision metadata only
                                    |
                       +------------+-------------+
                       |                          |
                       v                          v
                    aborted             committed at version C
                                                  |
                                      apply bundle as one
                                      RocksDB WriteBatch
                                                  |
                                      visible to snapshot C+
                                                  |
                                      background workers
                                      materialise/index/repair
```

## 8. Why MVCC Does Not Replace Consensus

MVCC defines version storage, snapshot visibility, and conflict detection. It
does not determine which node may assign the next authoritative version during
a partition, which of two conflicting transactions committed first, or whether
an ambiguous transaction committed before its coordinator failed.

gRPC and libp2p provide transport. Acknowledgements provide evidence that a peer
performed an operation. Neither prevents two disconnected nodes from each
declaring a conflicting transaction committed.

Without consensus, Anvil must choose at least one of:

- one permanent writer with no automatic failover;
- eventual consistency with application-level conflict resolution;
- revocation of previously acknowledged writes;
- CRDT-only operations;
- an external consensus service.

Anvil requires automatic failover and irreversible serializable transactions.
It therefore requires consensus for transaction outcome and order. This RFC
minimizes that consensus surface instead of hiding consensus inside a custom
MVCC replication protocol.

## 9. Consensus Boundary

### 9.1 State Permitted in Raft

Only the following application state may be proposed through OpenRaft:

1. transaction certification commands;
2. transaction outcomes produced by deterministic certification;
3. compact point-conflict versions and range stamps;
4. cluster identity and node-incarnation installation/removal;
5. OpenRaft voter and learner membership;
6. authoritative partition assignments and assignment epochs;
7. durability-policy generations;
8. garbage-collection safety watermarks requiring cluster-wide agreement.

OpenRaft also stores its own vote, term, log, committed index, applied index,
membership, and snapshot metadata.

### 9.2 State Forbidden in Raft

The following must not appear in Raft application entries or snapshots:

- object payload bytes;
- raw data segments;
- erasure-coded shards;
- transaction bundle bodies;
- ordinary CoreMeta row bodies;
- index segment bodies;
- stream payloads;
- PersonalDB payloads;
- registry blobs;
- authorization tuple bodies or projections;
- materialisation job bodies;
- watch event bodies;
- per-object manifests;
- per-transfer acknowledgements;
- health checks, capacity reports, or metrics;
- per-request authorization evidence;
- cryptographic receipt signatures.

### 9.3 Certification State

The deterministic certification state is:

```text
last_applied_log_id
stored_membership
point_latest_write_version: logical_key_hash -> commit_version
range_latest_write_stamp: range_conflict_key -> commit_version
recent_transaction_results: transaction_id -> certification_result
cluster_configuration
gc_safety_watermark
```

Conflict state may use a bounded hierarchical range structure to prevent the
consensus snapshot from growing once per historical transaction.

Recent transaction results may be pruned after their idempotency retention
window and all relevant GC safety conditions expire.

## 10. OpenRaft Choice and Encapsulation

### 10.1 Version

The initial implementation will pin the latest reviewed OpenRaft `0.9.x`
release. It must not follow an unconstrained version range and must not use a
`0.10` alpha in production without a separate review.

### 10.2 Adapter Boundary

Product code must not import OpenRaft types. An internal adapter exposes:

```rust
trait Consensus {
    async fn certify(
        &self,
        command: CertifyTransaction,
    ) -> Result<CertificationResult>;

    async fn linearized_read_barrier(
        &self,
    ) -> Result<CommitVersion>;

    async fn apply_cluster_change(
        &self,
        change: ClusterChange,
    ) -> Result<ClusterConfiguration>;

    fn observed_commit_version(&self) -> CommitVersion;
}
```

The adapter owns OpenRaft configuration, networking, storage traits, snapshots,
membership changes, metrics, and conversion between OpenRaft log IDs and Anvil
commit versions.

### 10.3 No Initial Fork

Anvil will not initially fork OpenRaft or `raft-rs`. A fork requires a written
decision identifying a correctness, performance, maintenance, or API limitation
that cannot be contained by the adapter.

## 11. RocksDB Layout

OpenRaft and CoreMeta share one RocksDB database but use separate column
families.

Required consensus column families:

```text
cf_raft_vote
cf_raft_log
cf_raft_meta
cf_consensus_state
```

Required MVCC concepts:

```text
cf_mvcc_versions
cf_mvcc_heads
cf_transaction_bundles
cf_prepared_bundle_index
cf_materialisation
cf_outbox
```

Feature-specific column families may remain separate. Their values become MVCC
row versions rather than independently published records.

### 11.1 Raft Keys

```text
cf_raft_vote:
  group_id -> encoded vote

cf_raft_log:
  group_id | big_endian(log_index) -> encoded raft entry

cf_raft_meta:
  group_id | last_purged_log_id
  group_id | committed_log_id
  group_id | last_applied_log_id
  group_id | stored_membership
  group_id | current_snapshot

cf_consensus_state:
  point | logical_key_hash -> latest_write_version
  range | range_conflict_key -> latest_write_stamp
  tx | transaction_id -> certification_result
  cluster | key -> cluster configuration value
```

### 11.2 Raft I/O Ordering

OpenRaft requires consecutive logs and serialized vote/log writes. Anvil must
route all OpenRaft storage writes through one ordered storage executor per
consensus group.

The executor may group several ordered requests into one RocksDB `WriteBatch`
and WAL sync. It must not:

- apply a later write before an earlier write;
- expose a log entry before the append operation permits it;
- invoke OpenRaft's flush callback before the entry is durable;
- leave a hole in the log;
- report a vote persisted before its WAL durability requirement is satisfied.

### 11.3 State-Machine Application

Applying a certification entry uses one RocksDB `WriteBatch` containing:

- the transaction certification result;
- changed point-conflict versions;
- changed range stamps;
- the last applied Raft log ID;
- membership state when applicable;
- compact cluster-control changes when applicable.

The transaction bundle is applied separately to the MVCC column families. A
node must not expose commit version `C` until both:

1. the certification decision for `C` is applied; and
2. the referenced transaction bundle is verified and applied locally.

The applied watermark is the highest consecutive version for which local
application is complete.

## 12. MVCC Physical Model

### 12.1 Versioned Keys

A row key is encoded so versions of one logical key are adjacent and
newest-visible lookup is bounded:

```text
VersionedKey =
    table_id
    | length_prefixed(application_key)
    | invert_u64(commit_version)
```

`invert_u64` sorts newer versions before older versions.

### 12.2 Head Index

`cf_mvcc_heads` maps a logical key to its newest locally applied committed
version:

```text
logical_key -> commit_version
```

This is a performance index. The versioned row is authoritative. Head and row
version are updated in the same RocksDB batch.

### 12.3 Tombstones

A delete writes a tombstone at the transaction's commit version. It does not
immediately remove older versions.

### 12.4 Visibility

A row version is visible to snapshot `S` when:

```text
row.commit_version <= S
and no newer row version <= S supersedes it
and the selected row version is not a tombstone
```

Prepared bundles and raw objects have no MVCC visibility until their
certification decision commits and local application reaches that version.

### 12.5 Global Snapshot

The global commit version provides one snapshot coordinate across all logical
keys, tables, partitions, and product features. A transaction opened at
snapshot `S` reads all state as of `S`.

## 13. Transaction Bundle

### 13.1 Header

```text
schema
transaction_id
snapshot_version
authenticated_principal
realms
created_at
body_length
body_hash
```

### 13.2 Body

```text
point_observations
range_observations
explicit_predicates
write_operations
raw_object_references
idempotency_results
outbox_events
materialisation_jobs
```

### 13.3 Canonical Encoding

Bundle encoding must be deterministic and versioned. The same logical bundle
must produce the same content hash on every node. Unknown schema versions must
be rejected before durable acknowledgement.

Canonical encoding protects against corruption and implementation disagreement;
it is not a trust signature.

### 13.4 Immutability

Once any node acknowledges a bundle as persisted, its bytes and hash must not
change. A retry with the same transaction ID and a different bundle hash is a
conflict.

## 14. Certification Protocol

### 14.1 Certification Command

```rust
struct CertifyTransaction {
    transaction_id: TransactionId,
    snapshot_version: CommitVersion,
    point_observations: Vec<PointObservation>,
    range_observations: Vec<RangeObservation>,
    written_point_keys: Vec<LogicalKeyHash>,
    advanced_range_stamps: Vec<RangeConflictKey>,
    bundle_hash: Hash,
    bundle_length: u64,
    durability: DurabilityLevel,
    durable_holders: Vec<NodeIncarnation>,
}
```

Every repeated field must be canonically sorted and unique.

The command contains conflict metadata, not row values.

### 14.2 Deterministic Certification

When the command is applied at Raft log position `C`:

1. If the transaction ID already has a result, return that result.
2. Validate canonical form and durability-evidence shape.
3. Validate every point observation against the latest certified write version.
4. Validate every range observation against the latest certified range stamp.
5. Validate explicit uniqueness, compare-and-swap, and other predicates
   represented by certification state.
6. If any validation fails, record and return `Aborted`.
7. Otherwise assign `commit_version = C`.
8. Set every written point conflict key to `C`.
9. Advance every affected range stamp to `C`.
10. Record and return `Committed(C)`.

All observations and writes are checked and advanced in one deterministic
state-machine application.

### 14.3 Point Validation

For a key observed at version `V`:

```text
latest_certified_write_version(key) == V
```

For a key observed absent:

```text
latest visible version at snapshot was absent
and no later certification wrote the key
```

### 14.4 Range Validation

For each range observation:

```text
latest_range_stamp(range_conflict_key) == observed_range_stamp
```

Every write must advance every range stamp whose query result it could affect.
The range-stamp hierarchy and write-to-stamp mapping are part of the canonical
key-space definition and must be deterministic.

### 14.5 Read-Only Transactions

A read-only transaction needs no certification when it accepts its original
snapshot. A read-only transaction requesting a linearized current snapshot uses
a consensus read barrier but creates no application log entry.

### 14.6 Blind Writes

A blind write that does not depend on the prior value may omit a point
observation for that key. It still advances the key's latest write version and
all affected range stamps.

APIs requiring create-only, update-only, uniqueness, or compare-and-swap
semantics must include the corresponding observation or predicate.

### 14.7 Small Transaction Fast Path

A transaction touching few keys has:

- a small bundle;
- a small conflict set;
- few version comparisons;
- one consensus proposal;
- one local RocksDB application batch.

There is no separate protocol based on which product areas or partitions those
keys occupy.

### 14.8 Large Transactions

Large conflict sets increase certification-entry size and state-machine work.
The implementation must define limits for:

- point observations;
- range observations;
- written keys;
- encoded certification-command bytes;
- bundle bytes;
- raw payload bytes.

Limits are resource controls, not artificial atomicity boundaries. They may be
raised without changing transaction semantics.

## 15. Foreground Write Protocol

### 15.1 Begin

1. Route the transaction to a coordinator.
2. Obtain a snapshot version.
3. Create or recover the transaction ID.
4. Read ordinary state at that snapshot.
5. Record point/range observations and explicit predicates.

### 15.2 Build

1. Stream object payloads directly into immutable local raw segments.
2. Compute hashes while streaming.
3. Construct one immutable transaction bundle.
4. Store the prepared bundle locally.

An implementation must not first write a disposable temporary file and then copy
it into another admission file.

### 15.3 Replicate

1. Select target nodes according to requested durability.
2. Stream the bundle and required raw data over existing authenticated gRPC
   streams.
3. Wait for the required `Complete` acknowledgements.
4. Record the node incarnations that supplied those acknowledgements.

### 15.4 Certify

1. Submit one certification command to the OpenRaft leader.
2. If it commits, use its Raft log position as commit version.
3. If it aborts, retain prepared data only until safe cleanup.
4. Return the deterministic result for retries.

### 15.5 Apply and Respond

The coordinating node applies the committed bundle as one RocksDB batch and
advances its applied watermark.

The normal response is sent after:

- the requested physical durability threshold was met;
- the certification decision committed;
- the coordinator applied the bundle locally.

It does not wait for:

- erasure coding under `local` or `quorum`;
- index construction unless explicitly requested;
- compaction;
- remote application beyond the durability contract;
- garbage collection.

## 16. Durability Levels

### 16.1 `local`

Before certification:

- the bundle is fsynced on the coordinator;
- required raw objects are fsynced on the coordinator.

The certification decision still goes through Raft for global transaction
order. If the sole durable holder is lost before replication or
materialisation, committed data may be unrecoverable.

The API and metrics must label this durability accurately.

### 16.2 `quorum`

Before certification:

- the complete bundle is fsynced on a durability set that intersects every
  valid election quorum;
- required raw objects are fsynced on the required data durability set;
- hashes and lengths are verified by every acknowledged holder.

For an ordinary three-voter majority, two appropriate durable holders satisfy
the intersection rule.

`quorum` is the default durability level.

### 16.3 `erasure`

Before certification:

- final erasure shards are durably stored across required failure domains;
- enough verified shards exist to satisfy configured erasure durability;
- the transaction bundle references the final materialised representation.

This has greater foreground latency and is opt-in unless policy requires it.

### 16.4 Durability Versus Consensus

Raft commitment proves that the certification decision reached a voter quorum.
It does not prove that object bytes or a transaction bundle were stored on that
same quorum.

Anvil enforces data durability before proposing certification. The command
records durable-holder incarnations so recovery can locate referenced data.

## 17. Persistent Replication Streams

### 17.1 Connection Establishment

For every node pair:

1. establish TLS;
2. present an Anvil node token;
3. validate token identity, audience, expiry, cluster, and incarnation;
4. apply the Zanzibar node-to-node authorization check;
5. bind the identity to a new connection session;
6. open one bidirectional multiplexed replication stream.

Authorization is repeated on connection and reconnection, not on every frame.

### 17.2 Frame

```rust
struct ReplicationFrame {
    session_id: SessionId,
    sequence: u64,
    partition: ReplicationPartition,
    transfer_id: TransferId,
    kind: TransferKind,
    offset: u64,
    payload: Bytes,
    payload_checksum: Hash,
}
```

`TransferKind` includes:

```text
TransactionBundle
RawObject
MaterialisedShard
MvccCatchUp
ConsensusSnapshot
Repair
```

### 17.3 Acknowledgement

```rust
struct ReplicationAck {
    session_id: SessionId,
    acknowledged_sequence: u64,
    transfer_id: TransferId,
    persisted_through: u64,
    completed_hash: Option<Hash>,
    status: AckStatus,
}

enum AckStatus {
    Received,
    Persisted,
    Complete,
    Applied,
    Rejected,
}
```

Semantics:

- `Received`: parsed or buffered; counts for no durability level.
- `Persisted`: bytes through `persisted_through` are fsynced.
- `Complete`: the complete item is fsynced and its final hash verified.
- `Applied`: a committed bundle was applied to local MVCC state.
- `Rejected`: the receiver will not accept the transfer.

### 17.4 Silent Connection Failure

Each sender tracks:

- last sent sequence;
- last acknowledged sequence;
- last persistence progress;
- heartbeat progress;
- outstanding byte and time windows.

If application acknowledgement progress stops beyond the configured deadline,
the sender treats the connection as failed even if the socket remains open.

### 17.5 Reconnection

On reconnect:

1. authenticate and authorize the new session;
2. exchange transfer watermarks;
3. resume incomplete transfers from persisted offsets;
4. retransmit frames whose durable status is unknown;
5. deduplicate by transfer ID, offset, checksum, and final hash.

## 18. Raw Data Storage

### 18.1 Append-Only Segments

Raw objects are written to append-only segment files:

```text
raw/<segment-id>
```

Each record contains:

```text
magic
format_version
transaction_id
content_hash
payload_length
payload_checksum
payload
record_checksum
```

### 18.2 Provisional and Live Records

A raw record is provisional until a committed bundle references it. A
provisional record is invisible to ordinary reads.

After commit it becomes a live raw representation. It may serve reads until a
verified materialised representation replaces it.

### 18.3 Segment Recovery

After a crash, a node scans only the tail of the last open raw segment,
validates framing and checksums, and truncates an incomplete tail record. Closed
segments have durable indexes and do not require full rescans.

## 19. Background Materialisation

### 19.1 Job Creation

For `local` and `quorum` writes requiring final transformation, the transaction
bundle contains a materialisation job row. The job becomes visible atomically
with the object.

### 19.2 Worker Loop

Workers continuously:

1. list eligible committed jobs;
2. claim a job with MVCC compare-and-swap;
3. read a valid raw representation;
4. chunk, compress, encrypt, deduplicate, or erasure-code as policy requires;
5. stream shards to target nodes;
6. wait for complete durable acknowledgements;
7. verify the resulting representation;
8. commit an MVCC placement update;
9. mark the job complete;
10. schedule obsolete raw copies for delayed deletion.

### 19.3 Idempotency

Materialised blocks and shards are content addressed. Repeating a job produces
the same identities or safely replaces an incomplete attempt.

Job claims are an efficiency mechanism, not a correctness boundary. Duplicate
workers must not corrupt data or produce divergent logical versions.

### 19.4 Placement Update

Changing physical representation is an ordinary MVCC transaction. It does not
require a special consensus command beyond normal certification.

## 20. Reads

### 20.1 Snapshot Reads

A transaction reads at its fixed snapshot version. A non-transactional read may
use the node's applied watermark or request a leader-confirmed linearization
barrier.

### 20.2 Local Snapshot

A local snapshot read may return any state up to the node's applied watermark.
It must not return a partially applied transaction.

### 20.3 Linearized Read

A linearized read:

1. obtains an OpenRaft read barrier or confirmed commit position;
2. waits until the local applied watermark reaches that position;
3. reads the requested MVCC snapshot.

### 20.4 Physical Representation Selection

For a visible logical object version, a reader chooses a verified available
representation:

1. preferred final materialised representation;
2. alternate materialised representation;
3. raw replica.

Materialisation must never create a period with no readable representation.

## 21. Recovery

### 21.1 Coordinator Failure Before Certification

No committed decision exists. Prepared bundles and raw records remain invisible
and are eventually garbage-collected.

### 21.2 Failure After Proposal but Before Response

The client retries with the same transaction ID. Certification returns the
recorded result if applied, or safely determines the result through OpenRaft.

### 21.3 Missing Bundle on an Applying Node

A node observing a committed decision but lacking the referenced bundle:

1. does not advance its applied watermark past the missing commit;
2. fetches the bundle from a recorded durable holder;
3. verifies hash and length;
4. fetches required raw data if necessary;
5. applies the bundle;
6. resumes watermark advancement.

### 21.4 Leader Failure

OpenRaft elects a new leader. The new leader recovers certification state from
the Raft log/state machine. It does not reconstruct transaction order by
comparing product rows.

### 21.5 Durable Holder Loss

For `quorum`, the holder set must intersect a surviving election quorum under
the advertised failure model. The new leader locates a surviving holder and
repairs missing copies.

For `local`, loss of the sole holder may make committed data unrecoverable. The
cluster reports a durability violation rather than inventing or silently
discarding data.

### 21.6 Node Catch-Up

A lagging node catches up in two coordinated streams:

1. OpenRaft catches up compact decisions.
2. Anvil replication fetches referenced transaction bundles and physical data.

The node serves snapshots only through its applied watermark.

## 22. Garbage Collection

### 22.1 Prepared Bundle GC

An uncommitted prepared bundle may be removed when:

- no committed certification result references it;
- its preparation retention period expired;
- no active certification attempt may still refer to it.

### 22.2 Raw Object GC

A raw object may be removed only when:

- no visible MVCC version requires it;
- a replacement representation satisfies policy;
- required replicas applied the placement update;
- the rollback/grace window expired;
- no reader or materialisation job pins it.

### 22.3 MVCC Version GC

A version below the GC watermark may be deleted only when it is not needed by:

- an active snapshot;
- configured history retention;
- a supported lagging replica;
- backup or export;
- audit retention;
- an unfinished materialisation or repair job.

### 22.4 Conflict-State GC

Point-conflict entries may be removed only when the corresponding logical key
and every relevant historical transaction are below the GC watermark.

Range stamps represent current conflict state and are compacted structurally;
they are not deleted merely because one contributing write is old.

### 22.5 Raft Log GC

OpenRaft entries may be purged after a consensus snapshot contains their
certification state and OpenRaft safety rules permit purging.

Transaction-bundle retention is independent from Raft-log retention. A bundle
needed for catch-up or MVCC history must not be removed because its
certification entry was compacted.

## 23. Authorization and Trust

### 23.1 Client Authorization

External requests authenticate with Anvil tokens and are authorized through the
existing Zanzibar-style authorization model.

### 23.2 Node Authorization

Nodes use Anvil node tokens during connection setup. The receiver performs the
node-to-node Zanzibar authorization check and binds the result to the connection
session.

### 23.3 Per-Frame Authorization

Frames are checked against authenticated connection identity, cluster,
permitted operation classes, and node incarnation. The system does not repeat
full token parsing and Zanzibar graph evaluation per frame.

### 23.4 Cryptography

Required:

- TLS for node transport;
- content hashes for immutable bundles and objects;
- checksums for frames and on-disk records;
- secure token validation at connection establishment.

Not required:

- signatures on internal acknowledgements;
- signed prepare receipts;
- signed commit certificates;
- signatures on each CoreMeta row;
- per-request node authentication after connection establishment.

## 24. Backpressure

Nodes advertise stream credit for:

- in-flight bytes;
- unfsynced bytes;
- prepared bundle bytes;
- raw-segment capacity;
- materialisation backlog;
- MVCC apply lag.

The sender must stop or reroute before exceeding receiver credit. A transaction
must fail with a retryable resource error rather than commit without satisfying
requested durability.

## 25. Public API

### 25.1 Compatibility Preference

Existing public method names, request shapes, and response shapes should remain
unchanged when they accurately express this RFC's semantics and impose no
internal compatibility burden.

### 25.2 Permitted Breaking Changes

A public API may change when necessary to:

- expose a transaction snapshot or commit version;
- remove an artificial transaction-scope restriction;
- select or report durability;
- distinguish durability from read consistency;
- report deterministic conflict details;
- remove response fields tied exclusively to discarded internal protocols;
- prevent a caller from assuming stronger guarantees than Anvil provides.

### 25.3 No Compatibility Internals

Public API stability must not require:

- dual writes;
- dual reads;
- old storage formats;
- shadow transaction protocols;
- translation between old and new internal receipts;
- preserving obsolete internal identifiers.

## 26. Observability

Required transaction metrics:

```text
anvil_transaction_build_duration_ms
anvil_transaction_replication_duration_ms
anvil_transaction_certification_duration_ms
anvil_transaction_apply_duration_ms
anvil_transaction_total_duration_ms
anvil_transaction_conflicts_total
anvil_transaction_point_observations
anvil_transaction_range_observations
anvil_transaction_written_keys
```

Required replication metrics:

```text
anvil_replication_stream_connected
anvil_replication_ack_latency_ms
anvil_replication_persist_latency_ms
anvil_replication_unacked_bytes
anvil_replication_reconnect_total
anvil_replication_resume_bytes_total
```

Required MVCC metrics:

```text
anvil_mvcc_applied_watermark
anvil_mvcc_apply_lag_versions
anvil_mvcc_versions_total
anvil_mvcc_gc_watermark
anvil_mvcc_gc_bytes_total
```

Required consensus metrics:

```text
anvil_consensus_proposal_duration_ms
anvil_consensus_apply_duration_ms
anvil_consensus_commit_index
anvil_consensus_leader_changes_total
anvil_consensus_log_entries
anvil_consensus_snapshot_duration_ms
```

Required materialisation metrics:

```text
anvil_materialisation_queue_depth
anvil_materialisation_age_ms
anvil_materialisation_duration_ms
anvil_raw_replica_bytes
anvil_erasure_shard_bytes
```

Stable trace operations:

```text
request.receive
transaction.snapshot
transaction.bundle_build
raw.append
raw.fsync
replication.stream
replication.persist_ack
consensus.certify
transaction.apply
response.send
materialisation.claim
materialisation.encode
materialisation.place
materialisation.commit
gc.mvcc
gc.raw
```

## 27. Correctness Invariants

The implementation must preserve:

1. **Atomic visibility:** every row in a committed bundle becomes visible at the
   same commit version.
2. **Unrestricted atomicity:** transaction atomicity does not depend on table,
   partition, tenant, or product feature.
3. **Single outcome:** one transaction ID never has two different outcomes.
4. **Global order:** committed transactions have one unambiguous commit order.
5. **No dirty reads:** prepared bundles are invisible.
6. **No partial apply:** a node never exposes only part of a committed bundle.
7. **Snapshot consistency:** every read in a transaction observes one snapshot.
8. **Point conflict safety:** an invalidated point observation cannot certify.
9. **Phantom safety:** an invalidated range observation cannot certify.
10. **Durability honesty:** acknowledgement satisfies the requested durability
    level and no stronger level is claimed.
11. **Quorum recoverability:** a quorum commit's bundle and required raw data
    remain obtainable under the advertised minority-failure model.
12. **Hash identity:** one transaction ID cannot refer to two bundle hashes.
13. **Monotonic application:** a node's applied watermark never decreases.
14. **No visibility gaps:** a node does not expose commit `C+1` while required
    application for `C` is incomplete.
15. **Safe materialisation:** a physical representation is not retired until a
    policy-satisfying replacement is verified.
16. **Fenced membership:** obsolete node incarnations cannot supply current
    durability acknowledgements.

## 28. Required Tests

### 28.1 Model Tests

An executable model must cover:

- conflicting point transactions;
- non-conflicting transactions;
- one transaction modifying several tables and partitions;
- one conflicting observation aborting an entire large transaction;
- range insertion phantom;
- range deletion phantom;
- duplicate certification proposal;
- coordinator crash before proposal;
- coordinator crash after proposal;
- leader crash before commit;
- leader crash after commit;
- bundle missing from one follower;
- durable-holder minority failure;
- network partition and leader change;
- node-incarnation replacement;
- local-durability data loss reporting;
- materialisation worker duplication;
- garbage collection with active snapshots;
- garbage collection with lagging replicas.

### 28.2 Storage Adapter Tests

- RocksDB Raft log keys preserve numeric order.
- Log append never creates holes.
- Vote and log writes are serialized.
- Flush callbacks occur only after durable persistence.
- Truncate and purge preserve OpenRaft invariants.
- State-machine application and last-applied state are atomic.
- Restart reconstructs identical certification state.
- Consensus snapshots install and recover correctly.

### 28.3 Transaction Tests

- A transaction may modify arbitrary tables and partitions.
- Every mutation in one transaction receives one commit version.
- Readers never observe partial transaction state.
- Retries return stable outcomes.
- Point conflicts abort correctly.
- Range conflicts and phantoms abort correctly.
- Read-only snapshots remain stable.
- Tombstones hide earlier versions correctly.
- Blind writes obey their declared semantics.

### 28.4 Replication Tests

- TCP delivery without a durable ACK does not count.
- A silent half-open connection is detected through ACK progress.
- Reconnect resumes at the persisted watermark.
- Duplicate frames do not duplicate stored bytes.
- A stale node incarnation cannot acknowledge current durability.
- Corrupt frames and final hashes are rejected.

### 28.5 Fault Tests

At minimum:

- process kill before and after every durability boundary;
- disk-full during raw append;
- disk-full during bundle persistence;
- disk-full during Raft append;
- delayed and reordered replication frames;
- dropped acknowledgements;
- leader isolation;
- follower isolation;
- simultaneous worker execution;
- loss of one node after quorum acknowledgement.

## 29. Performance Requirements

The implementation must report separately:

- raw streaming time;
- remote persistence wait;
- Raft certification time;
- local MVCC application time;
- deferred materialisation time.

Required benchmarks:

```text
metadata-only transaction
small inline-object transaction
large raw-object transaction
transaction touching one logical key
transaction touching ten logical keys
transaction spanning several tables and partitions
transaction under unrelated concurrency
same-key conflicting transaction
overlapping-range conflicting transaction
local/quorum/erasure durability comparison
group-committed certification throughput
replication reconnect and resume
MVCC read with retained history
MVCC garbage collection
```

The global sequencer must be measured before sharded consensus is designed. The
benchmark must include proposal batching and RocksDB WAL group commit.

## 30. Implementation Approach

This is a clean implementation, not an in-place protocol transition.

### Phase 1: Executable Model

- Define canonical command, bundle, key, range, and version types.
- Implement deterministic point and range certification.
- Model transaction atomicity and failure cases.

### Phase 2: OpenRaft Adapter

- Pin OpenRaft `0.9.x`.
- Implement RocksDB log storage, state machine, and snapshots.
- Implement the minimal `Consensus` interface.
- Validate storage ordering and restart behavior.

### Phase 3: Replication Streams

- Implement authenticated persistent gRPC sessions.
- Implement framing, persisted acknowledgements, resume, and deduplication.
- Implement transaction-bundle and raw-object transfer.

### Phase 4: MVCC CoreMeta

- Add versioned keys, heads, snapshot reads, tombstones, and applied watermark.
- Apply committed bundles atomically.
- Add point and range certification metadata.

### Phase 5: Public Transactions

- Route mutations through immutable bundles and certification.
- Permit transactions across arbitrary logical keys and product features.
- Preserve public API shapes where useful and change them where semantics
  require it.

### Phase 6: Background Materialisation

- Use raw records as the initial durable representation.
- Run erasure coding and final placement in durable workers.
- Add representation swap and safe raw-data retirement.

### Phase 7: Remove Discarded Internals

- Delete superseded transaction and publication machinery.
- Delete redundant admission representations.
- Delete internal receipt-signature protocols.
- Delete compensation workflows that exist only because atomic transactions
  were unavailable.
- Remove obsolete tests rather than maintaining two architectures.

## 31. Open Questions for Review

1. What range-stamp hierarchy best balances phantom safety, false conflicts, and
   compact consensus state?
2. What retention period is required for transaction-result deduplication?
3. Should `local` durability be available to external clients or only internal
   reconstructable workloads?
4. Which node set must hold data for `quorum` when Raft voters and storage nodes
   differ?
5. Should cluster-control decisions share the transaction certifier group or
   eventually use a separate small OpenRaft group?
6. What transaction command and bundle size limits are appropriate?
7. Which read APIs require a linearized barrier by default?
8. What policy governs a node that remains below the GC watermark?
9. Which existing public fields describe discarded implementation details and
   should be removed?

## 32. Acceptance Criteria

This RFC is implemented only when:

- transactions atomically modify arbitrary logical keys, tables, partitions,
  and product features;
- one global commit version orders committed transactions;
- OpenRaft contains only state permitted by Section 9;
- OpenRaft state is stored in the existing RocksDB database;
- object bytes and bundle bodies never enter the Raft log;
- persistent gRPC replication uses durable application acknowledgements;
- connection authorization is performed on connect/reconnect rather than every
  operation;
- `local`, `quorum`, and `erasure` durability have tested distinct semantics;
- ordinary `local` and `quorum` writes do not synchronously erasure-code data;
- committed bundles apply atomically to MVCC state;
- prepared bundles remain invisible;
- point and range certification provide serializable conflict detection;
- recovery fetches a missing committed bundle from a durable holder;
- no discarded internal protocol remains on the active write path;
- model, storage-adapter, replication, transaction, fault, and performance tests
  demonstrate every invariant in this RFC.
