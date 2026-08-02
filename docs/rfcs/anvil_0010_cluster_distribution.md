# ANVIL-0010: Single-Cluster Distribution in Anvil 0.5.1

Status: Accepted architecture. Section 24 records the final implementation
decisions approved before release work continued.

Audience: Anvil implementors, operators, client authors, and reviewers

Compatibility: An existing Anvil 0.5.0 data directory can become the first
node of a 0.5.1 cluster in place. Anvil 0.5.1 does not restore an older product
surface or introduce PersonalDB, index, gateway, region, or mesh APIs.

## 1. Decision

Anvil 0.5.1 adds one flat cluster containing `N` heterogeneous Anvil nodes.
Raft decides the cluster's small control-plane state. Weighted rendezvous
hashing derives ownership of the much larger data-plane state from the active
membership. Neither object paths nor per-object placement decisions enter
Raft.

Any active node may receive a public request. It authenticates and bounds the
request, calculates the current owner, and either executes locally or proxies
the typed request once to that owner. The owner repeats authentication and
Zanzibar authorization before using its local authority.

The cluster has these deliberately different data treatments:

- mutable RocksDB records needed for current heads, compare-and-swap,
  authorization, naming, and recovery are stored as complete logical replicas;
- payloads of at most 64 KiB remain complete content-addressed values in the
  `small_blobs` RocksDB column family and are copied to their selected content
  owners;
- larger payloads converge to erasure-coded shards on distinct
  weighted-HRW-selected nodes regardless of acknowledgement policy;
- Raft records only membership, node identity and weight, bootstrap state,
  atomic-executor nomination, compact atomic decisions, and other bounded
  cluster coordination state; and
- caches and materialized views never become an additional authoritative
  persistence plane.

There remains exactly one logical atomic-program executor. Raft may nominate
any active voter or learner. This singleton concerns only explicitly invoked
atomic programs over `PROGRAM_ONLY` paths. It does not route ordinary object
traffic through one node.

There is no one cluster-wide data primary and there are no persisted per-path
or per-database primary assignments. An exact key's weighted-HRW rank-zero
node coordinates that key while the current membership is fenced. A future
PersonalDB primary is independently derived for each database. A future index
has exactly one logical owner and query executor, not one index instance per
node. PersonalDB and index APIs are not part of 0.5.1.

## 2. Required invariants

1. Raft state is small, bounded, snapshotable, and independent of object,
   tenant, database, or index cardinality.
2. Payload bytes, path inventories, object heads, Zanzibar tuples, fragment
   reference counts, and event journals never enter Raft.
3. The active cluster membership and stable node weights are the only inputs
   to data placement.
4. Every node computes identical weighted-HRW rankings for the same key and
   committed membership.
5. Only a node holding a fresh serving fence for the exact committed
   membership may coordinate mutable state or grant a positive authorization
   result.
6. Joining nodes do not own data or serve public requests until catch-up has
   completed and Raft marks them active.
7. One exact-path coordinator serializes CAS and version assignment. A stale
   former coordinator cannot make a mutation visible.
8. Acknowledged authoritative RocksDB state has enough complete logical
   replicas to elect a current value after one node is lost in a cluster of at
   least three active nodes.
9. An erasure-coded fragment is not itself replicated. The set of different
   data and parity fragments provides payload redundancy.
10. A successful `REPLICATED` payload write has the configured recoverable
    fragment or copy set. It never silently degrades to `LOCAL`.
11. Public watch delivery is unordered and at least once. Its internal source
    journals are locally ordered and gap-detecting.
12. An atomic-program commit can be recovered after executor loss and cannot
    be observed as a partly finalized multi-path result.
13. Peer RPCs require a current cluster-managed node identity. Public
    administration RPCs require ordinary JWT authentication and Zanzibar
    authorization.
14. Joining, removing, or reweighting a node moves only keys whose weighted
    HRW ranking changes.
15. Loss, lag, or corruption is reported. Anvil does not return a successful
    partial listing, silently use an older object version, or manufacture an
    authorization answer from stale state.

## 3. Goals

1. Let an existing single-node installation expand online into one cluster.
2. Survive an active-node loss for data written with the promised durability.
3. Preserve exact-path CAS, immutable writes, version and tombstone semantics,
   Zanzibar enforcement, watches, bulk operations, and explicit atomic
   programs.
4. Use heterogeneous storage capacity without topology-aware placement.
5. Keep public API behavior independent of the ingress node.
6. Keep the existing prefix-sortable object keys and inline-small-blob design.
7. Make membership change and recovery observable and testable.
8. Establish a source-journal contract from which later single-owner indexes
   can be built correctly without turning query execution into a distributed
   problem.
9. Retain the single-node ingest performance envelope and quantify the cost of
   proxying, metadata replication, and replicated payload durability.

## 4. Non-goals

Anvil 0.5.1 does not provide:

- regions, zones, topology-aware placement, cross-cluster mesh, federation,
  or disaster recovery between clusters;
- PersonalDB APIs;
- path, metadata, full-text, bitmap, vector, or other index APIs;
- Git, S3, PersonalDB, or other gateways;
- distributed index query execution;
- an external public-key infrastructure or operator-managed peer CA;
- a distributed rate-limit counter;
- a globally ordered public watch or change-data-capture log;
- automatic eviction of an unreachable member;
- continuously changing placement based on live free disk space;
- online changes to the large-object erasure-code geometry;
- physical RocksDB replication, RocksDB checkpoint recovery as an authority,
  or raw column-family access over the peer API;
- cross-path transactions for ordinary object operations; or
- placing paths, payloads, user metadata, Zanzibar tuples, fragment counts,
  watches, PersonalDB identities, or index identities into Raft.

## 5. Cluster and node identity

A cluster has one random stable `cluster_id`. A node has one stable `node_id`
in `1..=1023`, preserving the existing ten-bit Snowflake identity field. It is
persisted with the cluster identity and data directory. A restart must present
exactly the identity already recorded in that directory. A data directory
belonging to another cluster or node is rejected.

Each data directory stores this identity and its peer credentials in one
bounded, versioned `node-identity.json` file. The file is mode `0600` and
contains the stable cluster and node IDs, the peer identity currently presented
on new connections, and at most one overlap identity. Each peer identity holds
one self-signed certificate and its private-key PEM. Initial creation never
replaces an existing file. Rotation writes a bounded same-directory temporary
file, synchronizes it, atomically renames it over the current file, and
synchronizes the parent directory. Startup rejects symlinks, non-regular files,
wrong permissions, oversized or malformed input, and any stable-ID mismatch.
Private-key material is never logged.

The committed descriptor of an admitted node contains only bounded data:

```text
NodeDescriptor {
  node_id
  peer_address
  storage_weight
  state = JOINING | ACTIVE
  current_peer_spki_sha256
  overlap_peer_spki_sha256?
  join_capability_hash?
  supported_protocol_range
  supported_storage_format_range
}
```

Voter or learner role comes from OpenRaft's committed membership and is not
duplicated inside the descriptor.

An implementation may keep operational observations such as free disk,
latency, last heartbeat, and repair backlog outside Raft. They do not influence
placement automatically.

`JOINING` nodes receive the Raft log and state-transfer traffic but do not
appear in the active placement set, coordinate mutable records, answer public
requests, or become the atomic executor. `ACTIVE` voters and learners all
receive the committed Raft log, all participate in weighted-HRW storage, and
all are eligible for atomic-executor nomination. Only voters determine Raft
quorum.

The cluster has at most three voters and admits additional storage nodes as
learners. One ACTIVE node is the sole voter, two ACTIVE nodes are both voters,
and a cluster with three or more ACTIVE nodes has exactly three voters. A ready
ACTIVE learner is promoted before a planned voter removal when that is required
to preserve this target. A JOINING learner is never eligible for promotion,
serving authority, placement, or atomic-executor nomination. An unreachable
node is never automatically removed or replaced as a voter.

Raft retains a fixed 1,024-bit record of every node ID ever admitted. Removing
a descriptor never permits that ID to be reused. This bounded 128-byte record
protects Snowflake versions, journal source identities, certificate pins, and
placement from acquiring a second meaning.

Reusing a node identity with a newly empty data directory is prohibited. Node
identity participates in version allocation, source-journal identity,
certificate pins, and placement. Recovery either restores that node's durable
state or admits a new identity.

## 6. Raft's exact scope

There is one OpenRaft group for the cluster. Its application state contains:

- cluster identity and correctness-critical cluster configuration;
- admitted node descriptors and their `JOINING` or `ACTIVE` state;
- voter and learner membership;
- the Raft log ID of the active weighted-placement membership;
- at most one in-progress membership operation;
- system-bootstrap state;
- the current atomic-executor nomination and nomination log index;
- compact `CommitBatch` references for committed atomic programs; and
- a monotonically advancing `FinalizedThrough` cursor.

OpenRaft membership continues to encode its released `BasicNode { address }`
value so 0.5.0 logs and snapshots remain readable. The richer bounded node
descriptor lives in the Raft application state and is cross-validated against
membership; it is not smuggled into the address string and does not require a
physical object-store record.

Raft does not store:

- object or reserved-system paths;
- heads or immutable version descriptors;
- payload, metadata JSON, prepared bundles, or index segments;
- fragment locations or placement epochs;
- fragment reference counts or reference identities;
- tenant-name claims, bucket mappings, credentials, schemas, or Zanzibar
  tuples;
- public watch events or source journals;
- per-path locks;
- future PersonalDB identities or assignments; or
- future index definitions, segments, checkpoints, or assignments.

New Raft commands, snapshots, and storage records use explicit versioned
envelopes. The decoder retains a narrow 0.5.0 raw-layout fallback with frozen
fixtures, and new command variants preserve the released enum discriminants.
A node advertises its supported protocol and storage-format ranges during join.
A feature becomes active only when the required voting set supports it.
Unknown required command or snapshot versions fail closed. Raw Rust `bincode`
layout is not the protocol for new durable formats.

Snapshots contain the latest bounded application state and OpenRaft membership.
Compaction therefore bounds the Raft log. No feature in this release is allowed
to require preserving every historical membership, lease, certificate, or
atomic-finalization acknowledgement in Raft.

## 7. Weighted rendezvous placement

For a placement key `K` and active node `n`, define a uniformly distributed
value strictly between zero and one:

```text
H(K, n) in (0, 1)
```

The weighted rendezvous score is:

```text
score(K, n) = -storage_weight(n) / ln(H(K, n))
```

Nodes are ranked by descending score. A stable node-ID comparison breaks a
mathematical tie. A node with weight `1.0` receives approximately twice as many
independent keys as a node with weight `0.5`. Adding, removing, or reweighting a
node changes only the rankings affected by that change.

The following rules are fixed:

- `storage_weight` is a positive administrator-selected capacity ratio;
- it describes intended usable storage capacity, not momentary free space;
- free-space telemetry may reject new placement or alert an operator but never
  silently changes the weight;
- a weight change uses the same copy, tail, fence, and cutover process as a
  membership change;
- all owners for one replicated or erasure-coded item must be distinct nodes;
- joining nodes are excluded until activation commits; and
- a membership operation uses one exact old and one exact proposed active set.

The placement calculation is an integer protocol. A weight is a positive
`u32` number of millionths, where `1_000_000` means `1.0`. For node `n`,
BLAKE3 derive-key mode uses context `anvil.storage/weighted-hrw/v1` and hashes
this exact tuple:

```text
[placement_kind:u8]
[cluster_id:16 bytes]
[key_length:u64 BE]
[key:key_length bytes]
[node_id:u64 BE]
```

Interpreting the first eight digest bytes as an unsigned big-endian integer
`r` defines the exact open-interval value `H = (2r + 1) / 2^65`. A fixed
64-round integer binary-log calculation produces Q64.64 `-log2(H)`, clamped to
one least-significant bit so the denominator is never zero. Multiplying every
denominator by the constant `ln(2)` would leave the ranking unchanged, so this
is exactly equivalent to the natural-log formula above.

The binary-log calculation is fully integer and rounds the final denominator
down:

```text
n = 2r + 1
b = bit_length(n)
integer = 66 - b
z = n << (127 - b)                 # Q2.126 value in [1, 2)
fraction = 0

for output_bit = 63 down to 0:
    square = z * z                 # exact 256-bit product
    if square >= 2^253:
        z = floor(square / 2^127)
        fraction |= 1 << output_bit
    else:
        z = floor(square / 2^126)

rounded_up = (integer << 64) - fraction
denominator = rounded_up           if n == 1
              max(1, rounded_up-1) otherwise
```

The final one-bit subtraction converts the truncated fractional logarithm's
ceiling into a floor. An odd `n` other than one cannot be a power of two, so it
always has a non-zero discarded remainder.

Two scores `weight_a / denominator_a` and `weight_b / denominator_b` are
compared by `u128` cross multiplication. No division, platform floating point,
or math dependency participates. Lower node ID wins an exactly equal
quantized score. Frozen golden vectors are part of the placement format and
must produce the same result on AMD64 and ARM64.

Weights cannot defeat the distinct-fragment requirement. If a cluster has
exactly `K+M` nodes, every large blob places one equal-sized fragment on every
node regardless of weight. Capacity weighting becomes effective when more
eligible nodes exist than fragments per blob. Placing two fragments of the
same blob on one high-capacity node would weaken node-failure tolerance and is
not done implicitly.

## 8. Placement keys and ownership

Placement is derived independently for each logical item:

| Item | Placement key | Rank zero means |
|---|---|---|
| Tenant-name claim | canonical tenant name | mutation coordinator |
| Tenant or bucket record | stable record ID | mutation coordinator |
| Exact object state | tenant ID, bucket ID, exact path | path coordinator |
| Zanzibar realm aggregate | tenant ID, realm ID | realm coordinator |
| Credential | stable application or credential ID | mutation coordinator |
| Small content | content hash and length | first content owner |
| Large fragment | blob identity and fragment ordinal | fragment owner |
| Future PersonalDB | tenant ID and PersonalDB ID | one database primary |
| Future index | tenant ID and index ID | one index owner |

There is no metadata-primary service. “Coordinator” means only the current
rank-zero node for one exact logical key. The next ranked nodes hold that
key's complete logical replicas where replication applies.

Rank zero is the one authoritative primary during normal operation; replicas
do not independently accept mutations. Raft fences which committed membership
may coordinate, but it does not elect or record billions of per-key primaries.
After rank-zero loss, the next coordinator is derived from the fenced
membership and reconciles the complete replicas. The bounded mutation lineage
in section 13 is necessary only to distinguish a coordinator that failed
before replication, during quorum application, or after quorum application but
before replying. Moving those per-key outcomes into Raft would violate its
bounded control-plane role.

Requests use stable tenant and bucket IDs after name resolution. Mutable names
never enter object head or version keys. Existing head keys remain:

```text
[format_version:u8][tenant_id:u64 BE][bucket_id:u64 BE][raw UTF-8 path]
```

This layout remains directly seekable for literal-prefix RocksDB iteration.

## 9. Serving fences and stale owners

Weighted HRW says who should own a key but cannot stop an isolated node from
continuing to use an older membership. Anvil therefore requires one short
node-wide serving fence, not a per-path lease or lock service.

The current Raft leader may grant a serving lease only while it has recently
confirmed its own leadership with a voter quorum. A lease names:

```text
ServingLease {
  cluster_id
  raft_term
  active_placement_log_id
  maximum_local_lifetime
}
```

Lease renewal is transient peer traffic. It does not append a periodic Raft
entry. A recipient measures its lifetime with its local monotonic clock and
never extends it without a fresh leader grant.

A successful linearizable quorum confirmation may be reused for at most 500
milliseconds. A grant issued after that confirmation expires is rejected until
the leader confirms again; Anvil does not perform a quorum round trip for every
recipient or ordinary request.

A node may coordinate current heads, accept mutable operations, grant positive
Zanzibar results, or run as atomic executor only while it has a valid lease for
the exact active-placement log ID it has applied. The bounded
`active_placement_log_id` is the Raft log ID of the command that activates an
add, remove, or reweight. It is not a managed counter. Unlike OpenRaft's voter
membership log ID, it therefore changes when a weight change alters HRW
ownership. Expiry fails those operations
closed. Exact immutable content reads by verified content identity may continue
when doing so does not disclose a path or require a positive authorization
decision.

Membership cutover stops renewal for the old membership and waits three
seconds before any new owner may coordinate the changed keys. Thus an isolated
former owner's two-second lease expires, with one second of scheduler margin,
before a replacement begins accepting writes.

Leases are renewed every 500 milliseconds. On Linux their two-second local
lifetime is measured with `CLOCK_BOOTTIME`, which continues across host
suspend; wall-clock synchronization is never used. A renewal expires relative
to the recipient's local timestamp taken immediately before sending the grant
request, not when the response arrives, so network or scheduling delay cannot
extend authority. A clock failure, term regression, membership mismatch, or
overlong grant fails closed. A per-operation Raft `ReadIndex` is not a fallback:
ordinary requests rely on the valid node-wide lease and do not add a quorum
round trip.

This mechanism is cluster-wide and constant in size. It does not create
billions of path leases, placement epochs, or Raft log entries.

## 10. Public ingress and peer routing

The public object, authorization, credential, and administration APIs remain
cluster-transparent. Any active node can be an ingress:

1. Parse and bound the request.
2. Apply that process's public rate limit.
3. Validate the JWT and install its immutable caller identity.
4. Resolve mutable names to stable IDs.
5. Calculate the responsible node from the applied active membership.
6. Execute locally or proxy once to that node.
7. The destination independently validates the original JWT, resolves the
   authoritative Zanzibar state, and authorizes the operation.
8. The destination rejects a second routing hop, a stale membership, or a
   request beyond its deadline.

No peer trusts a serialized `Caller`, an ingress authorization decision, or a
client-supplied routing target. The original signed bearer token and canonical
request are carried to the destination.

`BulkWrite` and `BatchGet` group items by coordinator and send bounded groups in
parallel. Results retain input order. `BulkWrite` remains a set of independent
single-path results: grouping does not turn it into an atomic transaction.
`BatchGet` likewise remains a batch of independent read-committed reads. Items
handled by one owner may share a local RocksDB snapshot as an optimization, but
0.5.1 does not claim one cluster-wide snapshot across owner groups.

Client rate limits remain process-local in 0.5.1. Only the ingress consumes the
public request quota. Destination work consumes separate peer request,
concurrency, and byte budgets. Raft traffic has reserved capacity so bulk or
repair traffic cannot starve consensus.

## 11. Cluster-managed peer TLS

The peer listener requires mutual TLS for Raft, routing, state transfer,
replication, repair, source-journal, and atomic-program RPCs. This is a separate
operational listener because it has a node-membership trust model, not because
the RPCs are safe when accidentally exposed.

Anvil does not operate a certificate authority. Each node uses a self-signed
certificate. After the TLS handshake proves possession of its private key, the
receiver verifies:

- the presented SPKI fingerprint matches the committed current or overlap
  fingerprint for that node;
- the claimed node belongs to the same cluster;
- its membership state permits the requested RPC; and
- the receiver has applied at least the membership entry authorizing it.

The cluster's authorized node-preparation operation creates a one-time mode
`0600` join bundle containing:

- the joining node's certificate and private key;
- cluster ID and configured node ID;
- current seed addresses and certificate pins;
- a single-use, narrowly scoped join capability; and
- the requested stable capacity weight.

The administrator copies that file to the configured joining node and deletes
the generated copy. The private key is not retained in Raft or in an
authoritative cluster-side record; only its SPKI fingerprint is committed.
A `JOINING` identity can invoke only join, Raft catch-up, and state-transfer
operations until activation.

The joining node consumes that bundle into its mode-`0600`
`node-identity.json`. The copied bundle is input, not a second durable identity
or certificate store.

### 11.1 Online certificate rotation

Rotation never requires cluster downtime or an external PKI:

1. The node generates and durably stores a new key and self-signed certificate.
2. Over its currently authenticated connection it proposes the new SPKI
   fingerprint.
3. Raft commits that fingerprint as `overlap`; peers now accept current or
   overlap.
4. The cluster waits until every required active peer has applied the overlap.
5. The node switches new connections to the overlap certificate and drains old
   connections.
6. One Raft command swaps the fields so the new fingerprint is `current` and
   the old fingerprint is `overlap`. Peers on either side of this ordered
   command still accept both pins.
7. After required peers have applied promotion, Raft removes the old pin.

Repeating any step is idempotent. A lost or compromised current private key
cannot authorize its own replacement; an authorized cluster administrator
must prepare replacement join material or remove and readmit the node. Raft
snapshots retain only the current bounded overlap, not rotation history.

Public TLS remains deployable through an external terminator in 0.5.1. Peer
TLS is mandatory and cannot be disabled.

The implementation uses a small custom Rustls accept/connect layer around the
existing Tonic peer protocol. It verifies the presented self-signed leaf
against the descriptor's exact SPKI SHA-256 pins rather than introducing a
cluster CA. Each peer RPC rechecks the authenticated node ID, current pin set,
membership state, and permitted RPC class so a removed node cannot retain
authority through an existing connection. TLS session resumption is disabled
in this release. The approved dependency set is `tokio-rustls`,
`rustls-webpki`, `sha2`, and `rcgen`, using the Rustls AWS-LC provider already
present in the server dependency graph.

## 12. Formation, join, and system bootstrap

Raft cannot choose the first member before a Raft group exists. One explicit
genesis action remains necessary. Seed addresses discover an existing cluster;
they never independently elect a new cluster.

Startup behavior is:

| Local state | Seed nodes | `--run-system-bootstrap` | Result |
|---|---|---|---|
| Existing data directory | Any | Any | Restart the persisted cluster identity only |
| Empty | None | Present | Create cluster identity, peer identity, and a one-voter Raft group |
| Empty | Present | Either | Join the discovered cluster; never create another cluster |
| Empty | None | Absent | Refuse to start |

When seeds are supplied, `--run-system-bootstrap` cannot create a cluster and
Anvil logs that the flag is ignored. A joining node queries seeds until it
finds the authoritative leader or another node that can redirect it. Its join
capability and pinned seed identity authenticate this pre-activation traffic.

0.5.1 does not introduce a separate cluster-formation secret or initial-node
manifest. Consequently the genesis node completes system bootstrap before an
administrator can use its Zanzibar-authorized node-preparation API to create
join bundles. Several wholly uninitialized nodes cannot securely form a new
cluster simultaneously merely by sharing seed addresses. A seed-started node
must already possess a join bundle issued by the bootstrapped cluster.

Creating the Raft group and bootstrapping Anvil's protected identity are
different operations. Raft permanently snapshots one bounded state:

```text
SystemBootstrap = MISSING | COMPLETE
```

If it is `MISSING`, the currently nominated atomic executor performs the
idempotent bootstrap while other nodes wait. It writes the protected system
realm, initial tenant and application identities, credential verifier, and
bootstrap-admin tuple through the normal authoritative metadata replication
path available at the current cluster size. It creates the bootstrap credential
file without replacement at mode `0600`, logs only its exact location, and
instructs the administrator to copy and delete it. Only after those records are
durable does Raft commit `COMPLETE`.

`COMPLETE` remains in every snapshot even after the creating log entry is
compacted. No joining or restarting node runs bootstrap again. Joining nodes
receive the system state before becoming ready. A 0.5.0 directory with a valid
bootstrap marker converts that marker exactly once into the cluster's
`COMPLETE` state and never creates a new administrator credential.

Anvil's JWT signing key remains operator-supplied and never enters Raft, a join
bundle, or cluster state transfer. Genesis commits this domain-separated
BLAKE3 fingerprint as immutable bounded cluster configuration:

```text
BLAKE3-DERIVE("anvil.auth/jwt-signing-key/v1", signing_secret)
```

A 0.5.0 in-place upgrade commits the first node's existing fingerprint once.
Every joining or restarting node computes the fingerprint of its local
mode-`0600` secret and fails before readiness when it differs. Online JWT
signing-key rotation is not a 0.5.1 capability.

## 13. Complete logical replication of mutable records

RocksDB is not physically replicated. Anvil replicates typed logical records to
the weighted-HRW-selected replica group:

```text
replicas(key) = WeightedHRW(key, active_members).take(min(3, active_count))
coordinator   = replicas(key)[0]
```

The metadata acknowledgement threshold is:

```text
active_count = 1  -> 1 of 1
active_count = 2  -> 2 of 2
active_count >= 3 -> 2 of 3
```

This threshold protects the current logical truth independently of the
client's payload durability choice. A `LOCAL` payload write must not leave its
head or tombstone on only one node: losing such a head could resurrect an older
version and retroactively violate CAS.

The `1/1`, `2/2`, then `2/3` rule is normative. A two-node cluster therefore
stops accepting mutations when either node is unavailable instead of claiming
node-loss safety it cannot provide.

The coordinator holds the local exact-key lock, checks the precondition,
observes the current version high watermark, allocates the next version, and
constructs one deterministic mutation containing:

- command ID and bounded input fingerprint;
- predecessor version or absence;
- new immutable descriptor, head, or tombstone;
- name, policy, authorization, or credential change when applicable;
- precise old/new content-reference deltas; and
- the source-journal position assigned by the coordinating node.

Every complete-record replica validates the predecessor and applies that
mutation idempotently in one synchronous RocksDB `WriteBatch`. Only the
coordinator appends the corresponding source event; replica application does
not manufacture duplicate source events. The coordinator acknowledges only
after the required complete replicas have durably applied the mutation and its
required content-reference effects are safe. Bounded receipts allow an unknown
response to be retried with the same command ID. Reuse with a different
fingerprint fails.

After coordinator loss, the new coordinator reconciles the old replica group
before acquiring a serving lease. It reads a quorum, validates predecessor
links, selects the highest valid state, and writes that state back to the
replica group before serving. A mutation whose response was lost may therefore
be completed rather than discarded. Missing predecessor evidence,
contradictory state, or the absence of the required read quorum is corruption
or unavailability, never permission to choose an arbitrary record.

Each candidate carries one versioned internal stamp:

```text
MutationStamp {
  predecessor_version?
  mutation_fingerprint
  active_placement_log_id
  serving_fence_term
  source_id
  source_journal_position
}
```

The mutation fingerprint binds the command ID, bounded input fingerprint, and
complete typed result; the raw command ID need not be repeated in every head.
There is no separate commit marker or certification phase. A candidate present
on only one replica and never acknowledged may be discarded when a quorum
proves a different valid successor, while every acknowledged `2/3` candidate
intersects every later read quorum. Retrying an unknown outcome uses the same
command ID and either observes or completes the same mutation. Contradictory
siblings without enough evidence fail unavailable rather than being ordered by
version alone.

Remote replication uses typed, versioned mutations. Peers never expose raw
column-family reads or writes.

An authoritative 0.5.0 head without a `MutationStamp` is a committed baseline,
not an uncommitted candidate. Upgrade does not rewrite it. The first 0.5.1
mutation names that head's version as its predecessor and stores the first
stamp. Joining replicas receive the baseline during state transfer before they
can become ACTIVE.

### 13.1 Column-family treatment

| Column family | Contents | 0.5.1 treatment |
|---|---|---|
| `heads` | Current exact-path version or tombstone | Complete logical replicas by exact path |
| `versions` | Immutable version descriptors | Same replica group as the exact path |
| `small_blobs` | Raw content of at most 64 KiB | Complete content copies on selected content owners |
| `blob_references` | Count, flags, and timestamps | Stored with every small copy, complete upload source, or large shard it governs |
| `bucket_options` | Version-retention configuration | Complete logical replicas by bucket |
| `names` | Tenant and bucket name claims | Complete logical replicas by canonical claim |
| `receipts` | Bounded mutation idempotency | Replicated with the governed mutation |
| `policies` | Mutable, immutable, or program-only admission | Complete logical replicas by bucket and policy key |
| `local_invalidations` | Ordered local source journal | Source-local; loss or retention gap is explicit |
| `authz_tenants` | Zanzibar tenant state and revisions | Complete logical replicas with the realm authority |
| `authz_schemas` | Immutable schema revisions | Complete logical replicas with the realm authority |
| `authz_bindings` | Current realm/schema binding | Complete logical replicas with the realm authority |
| `authz_tuples` | Current Zanzibar tuples | Complete logical replicas with the realm authority |
| `authz_receipts` | Zanzibar operation replay | Replicated with the governed mutation |
| `credentials` | Applications and Argon2id verifiers | Complete logical replicas by stable identity |
| `metadata` | Mixed implementation records | Split by the rules below; never copied wholesale |

Authoritative tenant and bucket records currently held in `metadata` use
complete logical replication. The system-bootstrap completion flag moves to
Raft. Version-allocation high watermarks, local journal epochs/tails/floors,
local receipt-size counters, applied atomic cursors, and cache information are
local recovery or derived state. They are reconstructed from authoritative
records, source journals, or Raft and are not independently replicated as
authority.

Consensus column families are owned by Raft itself. Its log is replicated by
Raft, local protocol metadata is recovered through Raft, and snapshots use the
Raft installation protocol. They do not pass through object storage.

### 13.2 Why not erasure-code RocksDB

Erasure coding a physical RocksDB database or checkpoint while excluding its
source would create a backup, not a live authoritative replica:

- current CAS and authorization could not fail over without reconstructing a
  node-sized database;
- a checkpoint could lag an acknowledged logical mutation;
- SST and WAL files contain compaction- and version-dependent derived layout;
- restoring a small record would require unrelated bytes; and
- byte-plane recovery would depend circularly on metadata stored inside the
  database being recovered.

Physical checkpoints may become a later backup capability. They cannot replace
record-level replication in 0.5.1. The coordinator's local RocksDB is one of
the complete replicas; it is not excluded from its own replica group.

## 14. Payload plane

Payloads remain content addressed by BLAKE3 hash and byte length. Identical
content has one logical content identity regardless of how many paths or
versions reference it.

### 14.1 Small payloads

Payloads of at most 64 KiB remain raw values in `small_blobs`. This is a
deliberate primary storage path, not a cache. Selected content owners hold
complete copies. The healthy final copy count is `M + 1`, derived from the
cluster's committed erasure-code profile; there is no separate copy-count
configuration. This gives complete small values the same configured
node-failure tolerance as large values without multiplying them by `K`. The
request's rank-zero coordinator first stores the raw value with a synchronous
RocksDB write. `LOCAL` accepts that coordinator acknowledgement; copying to the
remaining selected owners continues durably after the response. `REPLICATED`
waits for all `M + 1` selected complete-copy owners. If there are too few
ACTIVE nodes, `LOCAL` remains available and reports under-redundancy while
`REPLICATED` is unavailable.

No large-object implementation, index segment cache, or distributed feature
may remove or bypass `small_blobs`.

### 14.2 Large payloads

Larger payloads use one cluster-wide fixed erasure-code profile regardless of
the request acknowledgement policy. The default profile is:

```text
algorithm     = systematic Reed-Solomon Vandermonde
K             = 2 data chunks
M             = 1 coding chunk
stripe_unit   = 16 KiB
stripe_width  = K * stripe_unit
```

This is Ceph's conventional systematic, fixed-stripe layout, not a custom
byte-interleaving scheme. Each stripe takes `K` contiguous `stripe_unit`
ranges from the object and computes `M` equal-sized coding chunks over them.
Chunks of one ordinal, in stripe order, form that ordinal's stored shard.
The final stripe is logically zero-padded for encoding; the known object
length trims reconstruction and the zero tail need not be stored physically.

The administrator may override `K`, `M`, and `stripe_unit` when creating the
cluster. The genesis node commits the resulting bounded profile to Raft once.
Joining nodes learn it from Raft and reject a conflicting local configuration.
The profile is immutable for that fragment format. It is not repeated per
object and there is no profile registry.

The persisted shard identity is only:

```text
[fragment_format_version][blob_hash][blob_length][shard_ordinal]
```

The cluster profile defines stripe boundaries; they do not enter placement
keys, object descriptors, or manifests. A shard owner stores its one shard; it
does not store a second replica of that shard. The full set of different shards
provides redundancy. Every ordinal is assigned to a distinct active node; an
undersized cluster never silently co-locates shards and weakens the profile.

The rank-zero coordinator accepting an upload first seals and verifies one
complete content-addressed source blob in the ordinary byte plane. This is
upload and repair source material, not another storage class or side plane.
`LOCAL` may publish and return success after the coordinator has durably stored
the source and the mandatory logical-metadata quorum has committed. Encoding
and placement of the standard shards then continue durably in the background.
`REPLICATED` withholds publication and success until the evidence required by
section 24.1 is durable. Failure of the sole source after a `LOCAL` response
but before placement completes may lose the payload; it never rolls metadata
back or exposes an older version.

Placement ranks distinct active nodes once for the blob identity and assigns
shard ordinal `i` to rank `i`. A healthy read de-stripes the `K` systematic
shards directly. A degraded read obtains any `K` independently verified
shards, reconstructs the missing data, and verifies the final object BLAKE3 and
length. Erasure parity is not a checksum: shard-local integrity evidence is
checked before a shard is offered to the decoder. A corrupt or missing shard
is repaired from any valid `K` onto its current owner.

The codec is `reed-solomon-erasure` 6.0.0 in its normal pure-Rust
configuration, without `simd-accel`. The fragment format freezes golden vectors
so AMD64 and ARM64 produce identical bytes and a later implementation may be
substituted only when it reproduces those vectors. Each versioned shard file
stores its identity and inline CRC32C for every stored shard-stripe chunk; no
sidecar or checksum column family is created. Invalid chunks are treated as
missing before decode, and the reconstructed object's existing BLAKE3 hash and
length remain the end-to-end verification.

The default `2+1` profile uses 1.5 times the payload capacity and tolerates one
missing shard. All three shards are required for a non-degraded write, so one
unavailable owner pauses new `REPLICATED` writes under that profile. The exact
general rule is `K+1` durable final shards before a `REPLICATED` success, as
fixed in section 24.1; background placement still converges to `K+M`.

`LOCAL` and `REPLICATED` are response-evidence choices, not persistent object
durability classes. Content identity and final placement are identical under
both. The complete upload source remains through the ordinary age-gated
collection window after shard placement completes; it does not create a second
logical content identity or a per-reference inventory. Changing a committed
profile is a future explicit re-encode operation under a new fragment format
version, not a configuration edit.

### 14.3 Reference counts and garbage collection

Every complete small copy, complete upload-source blob, or large shard stores
its bytes and the same bounded lifecycle record:

```text
BlobLifecycle {
  ref_count: u64
  flags: u8 = { AWAITING_PUBLISH }
  created_at_unix_millis: u64
  updated_at_unix_millis: u64
}
```

There is no reference-ID inventory, upload lease table, preparation side
plane, or placement record. Sealing first content stores count one, marks it
`AWAITING_PUBLISH`, and updates `updated_at`. Publication clears the flag
without incrementing again. Adding or retiring a retained logical payload
version increments or decrements once. Same-content replacement in an
unversioned bucket changes the path version but has zero net content delta.

Reference mutations use the same bounded source journal described in section
19; there is no second reference log. Every content owner durably stores one
`last_applied_reference_position` per stable source ID. A source sends a
bounded batch naming an exclusive starting position, an inclusive through
position, and the deltas relevant to that destination. The destination applies
those deltas and advances its cursor in one RocksDB `WriteBatch`:

- a batch at or below the durable cursor is an idempotent retry;
- a batch starting exactly at the cursor applies once;
- a batch starting beyond the cursor is a gap and fails closed; and
- a decrement that would underflow is a gap or corruption, never a saturating
  arithmetic operation.

Cursor-advance batches may omit events irrelevant to that destination. This
avoids broadcasting every object event to every node while still proving a
contiguous source prefix. The ordinary internal RPC response only tells the
source that the destination durably advanced; it is not a public receipt or
watch acknowledgement. Source-side acknowledgement caches are derived and do
not become authority.

Before publishing a new reference, its available physical artifacts are
sealed or touched with `AWAITING_PUBLISH` and a fresh `updated_at`. The head,
version descriptor, mutation stamp, and source-journal event commit atomically
on the coordinator and the typed metadata mutation reaches its mandatory
logical-record quorum. `REPLICATED` also waits for every required new payload
owner to hold its bytes and apply the positive reference effect. `LOCAL` does
not wait for remote effects. Negative deltas continue asynchronously because a
delay can retain excess bytes but cannot delete live content.

The source journal compacts only through a prefix every required current
destination has durably advanced beyond. If its configured bound is reached,
every mutation that must append a source record stops rather than dropping an
event, creating an offset gap, or allowing the journal to grow without bound. A
temporary source outage pauses GC until its tail is again proven current. An
unrecoverable source-journal loss keeps GC disabled while authoritative version
descriptors rebuild counts. This keeps replay state proportional to cluster
nodes rather than reference IDs proportional to objects.

Garbage collection runs only on the current content owner while it has a fresh
serving lease. It performs the configured hourly scan and never removes content
whose `updated_at` is younger than the configured grace, defaulting to 24
hours. After that grace, count zero or a still-set `AWAITING_PUBLISH` makes the
content eligible.

GC is disabled for affected ownership ranges during handoff, repair, or count
reconstruction. After unrecoverable source-journal loss, reference counts are
rebuilt by scanning authoritative version descriptors. No reference inventory
is added merely to make that rebuild incremental.

## 15. Membership changes and rebalancing

Only one membership or weight transition is active at a time. Raft stores one
bounded transition containing its `ADD`, `REMOVE`, or `REWEIGHT` kind, the node
ID, and the target weight when applicable. The transition's Raft log ID is its
identity; there is no separately managed counter. A new leader derives which
step remains from the descriptor and OpenRaft membership. Individual keys,
copy progress, and progress inventories remain outside Raft and are recomputed
when recovery needs them.

### 15.1 Adding a node

1. Authorize and admit the identity as `JOINING` and a Raft learner.
2. Install the current Raft snapshot and catch up its log.
3. Calculate changed owners under hypothetical `active + joining node`.
4. Current owners stream the complete logical records, small content, large
   fragments, and lifecycle state the candidate would own.
5. Capture and replay the bounded source-journal tail while ordinary traffic
   continues.
6. Stop old-membership lease renewal and enter a bounded mutable-write pause.
7. Replay and verify the final tail and required content hashes.
8. Commit `ACTIVE` membership and the stable weight in Raft.
9. Grant leases for the new membership and resume traffic.
10. Keep former copies through the normal grace before GC removes them.

### 15.2 Removing a node

Planned removal performs the same calculation for hypothetical
`active - removed node`, pre-copies changed ownership, replays the tail, pauses
mutable traffic, expires old fences, commits removal, and resumes. If the node
is a voter, a ready ACTIVE learner is promoted first when needed to preserve
the fixed target of at most three voters.

Forced removal is an explicit authorized administrator action. It first checks
that surviving complete-record replicas and payload fragments meet the promised
recovery requirements. It reconstructs changed content owners, disables GC for
affected ranges, waits for old serving leases to expire, and only then commits
removal. If evidence is insufficient, Anvil reports the affected data-loss
risk and does not pretend removal is safe.

There is no automatic unreachable-node eviction in 0.5.1.

### 15.3 Reweighting

A capacity-weight change follows the add/remove handoff using the same node in
both sets at different weights. It is not activated until changed ownership is
copied and fenced. Live disk measurements never trigger this automatically.

## 16. Zanzibar in one cluster

Zanzibar remains Anvil's sole authorization mechanism. Its schemas, bindings,
tuples, revisions, receipts, application identities, and verifier records are
typed authoritative data placed and replicated like other mutable RocksDB
records. They do not enter Raft.

The proposed KISS placement unit is the complete `(storage_tenant, realm)`
aggregate, not each tuple or column-family key independently. Its rank-zero
node serializes schema binding, tuple batches, revision changes, and checks;
the next ranked nodes hold complete logical replicas of that same realm. A
check therefore evaluates one coherent local revision and never scatters a
Zanzibar graph across nodes. The protected system realm is one such aggregate.
This aggregate boundary is normative. Bounded administration operations that
must change it together with another placement aggregate execute through the
existing Raft-nominated atomic executor. They do not introduce a second
transaction coordinator or a raw cross-node RocksDB transaction.

The realm coordinator, not only the public ingress, evaluates the current
authoritative revision. A stale positive authorization cache can never grant.
An `AT_LEAST` or `EXACT` request either reaches an authoritative replica capable
of serving that revision or fails. Negative caches may be conservative but do
not become authority.

The protected system realm uses the same storage and evaluator as application
realms. It receives the same complete logical replication, fresh serving-fence
requirement, and fail-closed behavior. Raft infrastructure authenticates node
membership rather than consulting Zanzibar, avoiding a bootstrap cycle. Public
cluster administration APIs are ordinary JWT-authenticated,
Zanzibar-authorized system-realm operations.

Credential exchange is routed by stable client/application identity to the
current credential coordinator. The destination performs Argon2 verification
and token issuance. The cluster-shared signing-key fingerprint is committed;
the secret key is provisioned to nodes out of band.

## 17. Globally unique tenant names

Within one 0.5.1 cluster, a tenant name is a replicated create-if-absent claim
owned by weighted HRW over its canonical name. The claim is cluster-wide, not
node-local. Tenant deletion retains a reserved tombstone; releasing or
transferring a name to another tenant is forbidden and never happens as an
incidental object deletion. A rename adds the new claim while retaining the old
claim or tombstone bound to its original stable tenant ID.

External tenant names are canonical lowercase ASCII DNS labels: one through 63
characters, containing only `a` through `z`, `0` through `9`, and interior
hyphens, with an alphanumeric first and last character. Input is rejected
rather than lowercased or Unicode-normalized. Unicode, punycode aliases, and
confusable display spellings are not tenant identifiers. `_anvil` is the one
reserved system-tenant exception and cannot be registered by a client.

Tenant name is the one identity intended eventually to be unique across a
multi-region mesh. That prevents another region from registering a visually or
textually identical tenant to impersonate an existing organization. Bucket
names, paths, payload placement, PersonalDBs, and indexes remain scoped below
the tenant and need no mesh-global registry.

An isolated cluster cannot prove mesh-wide uniqueness before a mesh exists.
The later mesh capability must introduce one mesh-wide tenant-name authority
and route regional tenant creation through it. Human-facing display names, if
added later, are separate non-authoritative metadata and never participate in
identity or authorization.

## 18. Distributed `ListObjects`

Without a path index, a complete prefix listing must ask every active node
because exact paths are independently weighted across the cluster. This is an
intentional correct fallback, not a hidden distributed index.

For a request scoped to one tenant, bucket, literal prefix, and exclusive
`start_after`, every active node:

1. seeks its local `heads` iterator directly to
   `[format][tenant_id][bucket_id][prefix]`;
2. walks in byte-lexical order while the literal prefix matches;
3. emits only live heads for which it is current weighted-HRW rank zero;
4. omits tombstones and paths containing the reserved `_anvil` segment; and
5. returns a bounded sorted page plus continuation evidence.

The ingress k-way merges those sorted pages into the public page. The maximum
page size remains 1,000; there is no maximum total number of results. A later
request resumes from the last returned path. Pages are read committed and do
not retain a cluster snapshot between calls. Each source uses one local
RocksDB snapshot for its page; the merge is not a single point-in-time snapshot
of the cluster.

Failure or stale membership at any required active source makes the page
unavailable. Anvil never returns a successful partial listing. A later path
index removes this all-node query fanout, but 0.5.1 does not implement that
index.

## 19. Source journals and `WatchPrefix`

Every node has one source-local, monotonically sequenced change journal. It is
the existing `local_invalidations` column family generalized with a versioned
tagged value; no second replication, reference-count, or index log is added:

```text
SourceId = (node_id, source_epoch)

LocalChange {
  offset
  change =
      ObjectHead { tenant_id, bucket_id, exact_path, path_version,
                   kind = PUT | DELETE,
                   reference_deltas = [{ blob_identity, change }] }
    | RetainedVersionDeleted { tenant_id, bucket_id, exact_path,
                               deleted_version, resulting_head_version?,
                               reference_deltas = [{ blob_identity, change }] }
    | AggregateChanged { aggregate_kind, aggregate_key, revision }
    | ContentLifecycleChanged { blob_identity, revision,
                                reference_deltas = [{ blob_identity, change }] }
}
```

`reference_deltas` contains only exact non-zero signed count effects caused by
that committed change and is empty when content identity and retained-reference
cardinality do not change. It is bounded by the containing operation. A source
derives the destination-filtered batches in section 14.3 from these fields;
it never attempts to reconstruct a deleted reference from current heads after
the fact.

The authoritative local mutation and its `LocalChange` are committed in the
same RocksDB `WriteBatch`. Internal catch-up and handoff consumers can read all
variants and fetch current typed state or a required delta. Public
`WatchPrefix` filters to `ObjectHead`. Events are invalidations and compact
identities, not payloads or an authoritative historical row stream.

The public watch aggregator opens a filtered stream to every required source
and emits their events without imposing a cluster-wide order. Delivery is at
least once. A checkpoint is an opaque, integrity-protected, scope-bound vector
of:

```text
(membership_revision, source_id, next_offset)
```

A consumer reconnects through any active node, which verifies the checkpoint
and resumes each source. Loss of a source epoch, passage below a retained floor,
or a membership transition for which the cursor lacks required source evidence
returns `RESUME_EXPIRED`. The client performs a current-state rescan and starts
from a new checkpoint. Anvil never skips a gap and calls the watch resumed.

Source journals are bounded and compactable. They are not replicated as
authority. Join handoff attempts to transfer the needed tail, but permanent
loss has the explicit watch-expiry and future-index-rebuild consequence.

## 20. Future single-owner indexes

Although 0.5.1 exposes no index API, its journal and storage design must support
the following invariant without a storage-format replacement:

```text
one index definition
  -> one logical cluster index
  -> WeightedHRW(index_id) selects one active owner
  -> one owner builds, caches, and executes queries
  -> immutable index artifacts live as ordinary Anvil objects
```

One definition must never create one independently queried index per node.
Queries do not scatter across storage nodes and merge distributed results.
Modern nodes can keep many complete indexes in memory, and cold or larger
indexes may fetch only required immutable segments into an evictable local
RAM/disk cache.

### 20.1 Getting every cluster change

The one future index owner maintains a durable checkpoint vector containing one
source epoch and offset for every required cluster journal. It streams all
sources into one local builder. For every invalidation it rereads the current
head, payload, and associated user metadata through ordinary Anvil APIs and
compares the current path version with the version already represented.

Out-of-order cross-source arrival is harmless:

```text
indexed DELETE version 11
late PUT version 10
10 <= 11, therefore ignore it
```

The event is a prompt to obtain current truth. Intermediate versions need not
be indexed. A journal gap forces a complete rebuild rather than an incorrect
continuation.

A gap-free initial build or rebuild, bound to one membership revision, captures
each source's tail, scans current rank-zero heads under RocksDB snapshots, then
replays events after the captured tails. Version comparison makes overlap
idempotent. A membership change restarts or explicitly rebases the build; it
does not guess which events moved.

The resulting immutable segments, definition, manifest, and checkpoint vector
are normal `REPLICATED` Anvil objects. Only the owner CAS-publishes a manifest.
No index-specific authoritative column family or segment-placement plane is
created. A replacement owner downloads the manifest and required segments on
demand.

### 20.2 Boundary before filtering

A future index definition chooses a tenant, bucket, and path-subtree boundary.
For example, a shared multi-tenant bucket may define independent indexes rooted
at `/tenant/123` and `/tenant/456`. The boundary is segment-aware:
`/tenant/123/x` matches `/tenant/123`, while `/tenant/1234/x` does not.

This reduces index size and query work before any result filtering and gives
Zanzibar the coarsest cheap authorization boundary. Path structure does not
itself grant permission; the system realm still authorizes definition and
query access. Per-result checks are required only when permissions vary inside
the authorized index boundary.

Vector segment construction and merging may later distribute temporary build
work within one chosen boundary. Workers may upload immutable uncommitted
segments, but they never answer queries or publish the manifest. The single
index owner verifies coverage and versions and performs the one CAS publication.

These are downstream design constraints, not 0.5.1 APIs or background workers.

## 21. Reserved user metadata objects

Application-supplied metadata uses ordinary object storage. For subject path:

```text
/a/b/c.txt
```

the associated metadata path is:

```text
/a/b/c.txt/_anvil/meta.json
```

Any path segment exactly `_anvil` is reserved for Anvil-defined behavior. The
metadata bytes therefore use the normal object path, inline storage, erasure
coding, durability, deduplication, reference counts, GC, and authorization.
No metadata-specific storage plane or fields are added to the object version
descriptor.

The metadata document identifies its subject version. A read or future index
builder returns it only when that version matches the current subject head.
Delete and recreation cannot resurrect stale metadata. Ordinary public listing
and watch hide reserved descendants unless a defined Anvil capability exposes
them.

0.5.1 reserves this representation and transports the object normally. It does
not add a new user-metadata API or make an ordinary subject write and metadata
write cross-path atomic. A downstream API requiring that guarantee uses the
explicit atomic-program layer.

## 22. Distributed atomic programs

The cluster has one Raft-nominated atomic-program executor. Any active voter or
learner may be nominated. Its nomination log index is its fence. Program
requests arriving elsewhere are authenticated and proxied to it.

Only explicit atomic APIs and `PROGRAM_ONLY` paths participate. Ordinary object
puts, deletes, CAS, bulk transport, media upload, and index artifact writes do
not pass through the executor.

For an invocation, the executor:

1. verifies its serving lease and nomination fence;
2. expands, authorizes, sorts, and locally locks the complete exact-path set;
3. reads current state from each path's weighted-HRW authority;
4. executes the bounded deterministic program;
5. prepares output bytes through the normal distributed byte plane;
6. stores the complete prepared bundle with internal failure-tolerant
   durability, regardless of the requested output payload durability;
7. durably stages idempotent path mutations on their current complete-record
   replica groups;
8. proposes one compact Raft `CommitBatch` containing only durable references,
   hashes, the executor nomination fence, and active-membership revision;
9. drives idempotent finalization on every affected authority; and
10. advances `FinalizedThrough` only through the minimum contiguous commit
    durably finalized by all required authorities.

Program-only reads go through the current executor. Until an invocation is
globally finalized, it serves the prior finalized heads rather than exposing
the subset already materialized on individual authorities. The committed
prepared bundle is the recovery input; no separate program-receipt column
family, delivery-acknowledgement plane, or payload in Raft is introduced.

After executor loss, Raft nominates another active node. It fetches the
replicated prepared bundles, replays the bounded committed tail, and resumes
idempotent finalization. A membership change waits until that tail is finalized
and no program locks remain. This deliberately avoids solving atomic
finalization concurrently with ownership movement in 0.5.1.

## 23. Peer operations, recovery, and operations

The peer protocol is internal and versioned. It contains typed operations for:

- Raft append, vote, and snapshot installation;
- one-hop public-request proxying;
- complete-record replicate, reconcile, scan, and state transfer;
- small-content copy and large-fragment put/get/repair;
- ordered reference and lifecycle mutation streams;
- source-journal status, bounded read, and wait;
- distributed list source pages;
- atomic executor proxy, stage, finalize, and recovery;
- node preparation, join, activation, rotation, reweight, and removal; and
- health, drain, and capability negotiation.

All unary and bounded control operations have bounded messages, one
startup-configured maximum server execution time, and client deadlines clamped
to that maximum. The current default remains 30 seconds for such bounded work.
That is not a total lifetime cap on a progressing object upload: long uploads,
state transfer, and repair use bounded streaming frames with progress/idle
limits rather than one unbounded unary handler.

An active node is ready only when:

- local store and identity checks pass;
- it has applied current membership and holds a valid serving lease;
- system bootstrap is complete and authoritative authorization is safe;
- assigned complete records and payload ownership have caught up;
- no corruption or GC-safety latch affects its ownership;
- its atomic applied/finalized cursor is safe; and
- it is not draining.

Graceful shutdown stops new ingress, stops accepting newly coordinated writes,
finishes or safely aborts bounded work, relinquishes executor activity, flushes
local state, and then exits. It does not implicitly remove the node from
membership.

OTLP-compatible logs, metrics, and traces include cluster and node identity,
Raft term/role/leader/commit/applied state, membership and transition status,
serving-fence freshness, proxy latency, peer rejection and throttling, metadata
replica acknowledgements, fragment under-redundancy and repair, authorization
revision/latency, journal floors and lag, watch expiry, executor nomination,
and atomic finalization lag.

## 24. Resolved implementation decisions

The following choices are normative. They close the boundaries at which
implementation previously had to stop.

### 24.1 Payload acknowledgement thresholds

The cluster profile now defaults to systematic Reed-Solomon `K=2, M=1` with a
16-KiB stripe unit and is configurable only at genesis. `LOCAL` is resolved:
the rank-zero coordinator accepting the request must durably seal the complete
input, and the mandatory logical-metadata quorum must commit, before success.
It never weakens authoritative metadata replication or changes the required
final placement. Standard placement after a `LOCAL` response is mandatory
background work, not optional best effort.

Small payloads converge to `M + 1` complete copies and `REPLICATED` waits for
all of them as defined in section 14.1. The pure-Rust codec and inline integrity
format are also resolved.

For a large payload, `REPLICATED` waits for `K + 1` distinct final shards. This
is the general form of the default profile's `2+1`: enough final shards to
reconstruct after one selected owner is lost. It creates no temporary encoding
or second payload representation. Placement still converges to all `K + M`
final shards. Crash-safe continuation after a response and reference deltas
follow section 14.3.

### 24.2 Tenant-name canonicalization

Tenant identifiers use the lowercase ASCII DNS-label rule in section 17.
Noncanonical input is rejected, `_anvil` is reserved, and a claim is never
reassigned to another stable tenant.

### 24.3 Zanzibar aggregate and cross-aggregate administration

The proposed KISS Zanzibar unit is one complete realm on three logical
replicas, with checks and writes routed to its rank-zero coordinator. This
keeps a revision and graph local but makes the protected system realm one
cluster-wide authorization coordinator.

Provisioning can atomically change a name claim, tenant or bucket record,
credential, and protected-realm tuples in the current one-node store. Those
records have different distributed placement keys. Bounded cross-aggregate
administration uses the existing singleton atomic executor internally. It must
not become an accidental raw cross-node RocksDB transaction or a partially
visible sequence.

### 24.4 Serving and upgrade fences

Raft state stores one `active_placement_log_id`, assigned from the activating
Raft log entry rather than a new counter. A leader may reuse a successful
linearizable quorum proof for no more than 500 milliseconds when issuing
two-second serving leases. Existing unstamped 0.5.0 heads are committed
baselines and acquire a predecessor-linked stamp on their first mutation.

### 24.5 Bounded journal behavior

Reference effects use the single ordered source journal and destination cursor
protocol in section 14.3. When cursor-safe compaction cannot free enough space,
all mutations that require a source-journal append apply backpressure. Anvil
does not drop only the reference effect, omit an index/watch event, create a
gap, or add an unbounded secondary log.

## 25. Upgrade behavior

An existing 0.5.0 node upgrades in place:

1. preserve all `0x01` object, name, version, receipt, small-blob, lifecycle,
   Zanzibar, credential, and bucket data;
2. create and persist one cluster identity and initial node peer identity;
3. replace the local-only Raft address with its configured peer address;
4. translate the valid local bootstrap marker to Raft `COMPLETE` exactly once;
5. initially remain a one-active-node, one-voter cluster; and
6. admit additional 0.5.1 nodes through the ordinary join and online handoff.

There is no offline object-key rewrite. A 0.5.0 binary cannot participate as a
peer because it has no peer protocol. Future 0.5.x binaries use the versioned
command, snapshot, storage, and peer capability negotiation established here
for rolling upgrades.

Unknown or incompatible on-disk, peer, or Raft formats fail before the node
becomes ready. Anvil never partially opens a newer cluster with older serving
semantics.

## 26. Test and release evidence

0.5.1 is releasable only with automated and manual evidence for:

1. Three- and five-node elections, leader loss, quorum loss, learner catch-up,
   snapshot installation, and bounded compaction.
2. Fresh genesis, seed join, ignored bootstrap flag with seeds, bootstrap
   election, crash at every bootstrap boundary, and exactly one administrator
   identity.
3. Wrong cluster, signing-key mismatch, protocol mismatch, duplicate or reused
   node identity, invalid join capability, and unauthorized membership changes.
4. Peer certificate pinning, joining-node RPC restrictions, online overlap
   rotation, restart in every rotation phase, and old-pin rejection.
5. Weighted-HRW golden vectors across supported architectures, statistical
   capacity distribution, minimal movement, and distinct-fragment placement.
6. Planned join, planned removal, forced removal, weight change, process crash
   during every handoff phase, tail replay, and old-owner serving-fence expiry.
7. Public request ingress on every node, one-hop routing, destination JWT and
   Zanzibar recheck, deadline propagation, peer bounds, and rate limiting.
8. Concurrent `PutIfAbsent` and `PutIfVersion` with exactly one winner across
   coordinator failure and membership change; monotonic tombstones never reveal
   an older version.
9. Complete-record quorum recovery, minority-only write rejection, corruption
   detection, and no successful partial state.
10. Inline small-content deduplication and durability, large-object encode,
    decode, hash verification, missing/corrupt fragment repair, and configured
    node-failure tolerance.
11. Reference increment/decrement retries, source gaps, crash recovery, count
    rebuild, GC grace, unpublished cleanup, and GC disabled during unsafe state.
12. Cluster-wide Zanzibar revision consistency, revocation with stale caches,
    credential routing, protected system administration, and fail-closed quorum
    or serving-lease loss.
13. `ListObjects` global lexical ordering, page continuation beyond any number
    of pages, concurrent updates, reserved-path hiding, all-node fanout, source
    failure, and no partial success.
14. `WatchPrefix` fan-in, duplicate and cross-source reordering, cursor resume
    on another ingress, source-floor expiry, membership change, and forced full
    rescan.
15. Atomic programs spanning several coordinators, executor loss during prepare,
    Raft commit, and finalize, membership-change exclusion, bundle recovery, and
    no partial program-only visibility.
16. A simulated downstream single-owner index consuming every source, ignoring
    an older late event, rebuilding after a journal gap, and restoring a cold
    cache from ordinary immutable objects. This validates the 0.5.1 journal
    contract without shipping index APIs.
17. In-place upgrade of a populated 0.5.0 data directory without an object-key
    rewrite or duplicate bootstrap credential.
18. Readiness, graceful drain, repair and under-redundancy telemetry, and OTLP
    correlation across proxy and peer calls.
19. The established one-node OSV qualification completes within 150 seconds
    without material regression. Separate measurements report local and
    replicated cluster write throughput, proxy overhead, CAS latency,
    encode/decode cost, and p50/p95/p99 durable acknowledgement time.

## 27. Consequences

The design accepts several explicit trade-offs:

- common exact-path traffic remains distributed and does not pass through one
  transactional node;
- mutable logical records pay a complete-record quorum cost because current
  truth, CAS, tombstones, and authorization require immediate failover;
- large payloads use storage-efficient erasure coding rather than shard
  replication; `LOCAL` changes when success is returned, not final placement;
- `ListObjects` and public watches fan out until later single-owner indexes
  exist;
- membership cutover includes a short mutable-write pause instead of a
  permanent dual-write placement protocol;
- unreachable members require administrator action;
- source-journal loss expires watches and rebuilds future indexes instead of
  creating an unbounded global log; and
- indexes, PersonalDB, regions, mesh, and gateways remain later capabilities
  built on this cluster rather than being stitched into 0.5.1.

In exchange, Anvil gains one coherent distributed foundation: compact Raft
coordination, deterministic capacity-aware placement, exact mutable-record
replication, economical payload redundancy, cluster-managed peer trust,
recoverable explicit atomic programs, and a sound future indexing feed without
inventing a second distributed database inside the first.
