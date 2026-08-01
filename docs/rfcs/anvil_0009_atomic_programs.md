# ANVIL-0009: Bounded Atomic Programs in the Anvil 0.5 Core

Status: Accepted architecture for the Anvil 0.5.0 core. The explicitly listed
architecture gaps must be closed before implementation is release-qualified.

Audience: Anvil implementors, client authors, operators, and reviewers

Compatibility: None. Anvil 0.5 is a new capability architecture, not a
restoration or emulation of an earlier API.

## 1. Decision

Anvil 0.5.0 is an exact-path, opaque, versioned object store with a deliberately
small core:

- exact-path reads;
- ordinary `Put`, `Delete`, and compare-and-swap;
- non-atomic bulk transport for independent writes;
- explicit bounded atomic-program APIs for paths marked `PROGRAM_ONLY`; and
- an unordered, resumable `WatchPrefix` invalidation feed; and
- realm-scoped Zanzibar schemas, relationship tuples, and permission checks
  used both by Anvil itself and by applications.

Ordinary `Put`, `Delete`, CAS, and bulk operations are never transactions. They
do not accept a transaction ID, do not stage work for a later client commit,
and do not pass through the atomic-program executor. A bulk request shares
transport and bounded server work, but each item succeeds or fails
independently.

An atomic program is different because it deliberately changes several exact
paths under one visibility decision. Anvil 0.5.0 is a single-node release. Its
sole node is the nominated atomic-program executor and executes explicit atomic
API calls locally. There is no 0.5.0 peer transport, request proxy, or claim of
node-loss availability.

A later 0.5.x distributed capability may admit multiple nodes. When it does,
calls may arrive at any node and be proxied to the cluster's one currently
nominated executor. Any current Raft voter or learner must then be eligible for
nomination. That extension does not change the ordinary or atomic API boundary
defined here.

The executor nomination is a Raft decision. The Raft log index at which the
nomination commits is the executor's fence and epoch. Executor placement and
coordination ownership are not derived from object paths. There are no
per-path owners in Raft and no lock service.

For each atomic batch, the nominated executor:

1. expands and authorises its bounded exact-path set;
2. takes those paths' locks in its local in-memory lock table;
3. reads their current committed state;
4. executes the deterministic bounded program;
5. prepares every payload, immutable version descriptor, and the complete
   batch bundle on storage satisfying the requested durability;
6. waits for the configured durability requirement;
7. proposes one compact `CommitBatch` command containing identities, hashes,
   durable references, and its nomination log index, but no application
   payload; and
8. treats the committed `CommitBatch` entry as the visibility decision.

In 0.5.0 the sole node receives the committed decision through its one-node
Raft group. Materialising that decision into serving structures is
deterministic and idempotent. It may be retried by recovery workers; it is not
a second commit. A later distributed capability applies the same decision on
every current voter and learner.

Raft is used only for bounded decisions: executor nomination, compact
atomic-batch commits, and a monotonically advancing finalized-through
checkpoint. Object bodies, program definitions, complete version descriptors,
path inventories, locks, and prepared bundles remain in the byte plane outside
Raft. The 0.5.0 byte plane is local to its one node; a later distributed byte
plane must preserve this separation.

## 2. Why this boundary exists

Anvil stores opaque bytes at exact paths. Most writes need only a version and an
exact-path condition. Turning every write into a transaction would add latency,
state, recovery work, and API ceremony without adding useful correctness.

Some application commands nevertheless have a real bounded multi-path
invariant. A ledger command may need to create an immutable ledger entry and
advance a balance together. CAS on either path alone cannot make readers see
both changes together. The atomic-program capability exists only for that
case.

The design therefore has two honest write classes:

```text
ordinary exact-path write
  -> one independently visible path result

explicit bounded atomic program
  -> one Raft visibility decision for a prepared set of exact-path versions
```

There is no general transaction class between them.

## 3. Goals

1. Keep the common object API as exact path to opaque versioned bytes.
2. Make immutable ingest, CAS, and batched independent writes inexpensive.
3. Support genuine bounded multi-path invariants without exposing arbitrary
   transactions.
4. Keep application payloads and path cardinality out of Raft.
5. Keep the executor fence valid in the 0.5.0 one-node topology and reusable by
   a later distributed capability without adding distributed locks.
6. Keep Raft state and replay receipts explicitly bounded.
7. Provide reliable invalidation watches without inventing a global event
   sequence.
8. Ingest the pinned Developer Defence OSV corpus into the Developer Defence
   schema on one node in at most 150 seconds.
9. Use one Zanzibar authorization engine for Anvil's own request decisions and
   for application-defined relationship realms.

## 4. Non-goals

Anvil 0.5.0 does not provide:

- `BeginTransaction`, staged mutation, client `Commit`, rollback, transaction
  status, or certification;
- serializable or repeatable-read snapshots;
- arbitrary client-supplied read/write plans;
- range or prefix locks;
- a distributed lock service;
- multi-node membership, peer transport, executor proxying, replicated
  durability, or node-loss availability;
- path-derived routing or coordination ownership;
- SQL predicates, joins, foreign keys, or uniqueness constraints;
- automatic payload merging;
- hard-coded roles, path-prefix ACLs, or a second administrator authorization
  system;
- a global watch order or delivery of every intermediate version;
- atomic visibility of external effects;
- indexes or protocol gateways in the 0.5.0 core release; or
- compatibility with pre-0.5 APIs, data directories, Raft logs, cursors,
  transaction IDs, or response shapes.

## 5. Core object model

An object address is:

```text
(tenant, bucket, exact_path)
```

Slashes are naming bytes. They do not create directories, aggregates, lock
domains, or consensus records.

A version is an immutable value or tombstone for one address. The head selects
the currently visible version. Its version number increases monotonically for
that exact path, including across deletion and recreation. Clients use equality
for CAS and may compare versions only when they belong to the same exact path;
versions from different paths establish no order and are not transaction IDs.

An exact-path read returns:

```text
Present { version, bytes, content_hash }
Deleted { version }
NeverExisted
```

`Deleted` is distinct from `NeverExisted`. A delete advances the path's version
and leaves enough tombstone identity to prevent ABA within the advertised CAS
window.

An ordinary mutation uses one exact-path condition:

```text
Any
Absent
Version(expected_version)
```

- `Any` publishes a new version without a head precondition.
- `Absent` succeeds only when there is no live value.
- `Version(v)` succeeds only when the current live or tombstone head is exactly
  `v`.

`PutImmutable` is an `Absent` convenience with idempotent same-content replay.
An existing different value is a conflict.

### 5.1 Ordinary writes are not atomic programs

Ordinary `Put`, `Delete`, and CAS publish one path. They do not:

- contact the singleton atomic executor;
- acquire its multi-path locks;
- prepare an atomic bundle;
- append a `CommitBatch`; or
- gain rollback or grouping semantics when carried in a bulk RPC.

Their implementation must still make one path's version/head change atomic and
must durably couple the corresponding source invalidation with that change.
That single-path storage property is not a transaction API.

### 5.2 Durability choice

Every write request retains one per-request durability choice. It is a closed
choice, not an opaque policy name:

```text
LOCAL
REPLICATED
```

`LOCAL` is the only satisfiable choice in the single-node 0.5.0 release. A
successful response means the payload and authoritative metadata required by
that operation have completed the node's configured synchronous durable write.
For an atomic program, all prepared artifacts are locally durable before the
one-node Raft group accepts `CommitBatch`.

`REPLICATED` is a valid stable request value but 0.5.0 returns
`DURABILITY_UNAVAILABLE` without publishing the mutation because one node
cannot honestly satisfy it. It must never silently degrade to `LOCAL`.

A later distributed 0.5.x capability makes `REPLICATED` operational. Before
acknowledging an ordinary mutation or proposing an atomic `CommitBatch`, it
must wait for durable acknowledgements from at least the cluster-configured
number `N` of distinct storage nodes. The later capability must define the
replica or erasure-fragment evidence precisely; accepting the enum today does
not invent that evidence. Unknown durability values are invalid requests.

### 5.3 Bulk transport

`BulkWrite` carries many independent ordinary commands in one bounded request:

```text
BulkWriteResult {
  results[] = Applied(receipt) | Replayed(receipt) | Failed(error)
}
```

Some items may succeed while others fail. Retrying an unknown item uses its
original command ID and input fingerprint. The server bounds item count,
encoded bytes, response bytes, concurrency, and total work.

Bulk transport is the normal Developer Defence import path. Independent OSV
records must not pay one RPC, one consensus round, or one fsync each merely
because the importer was written as a loop.

### 5.4 Path policy

Path policies are capability admission rules:

- `MUTABLE`: ordinary versioned `Put`, `Delete`, and CAS are allowed;
- `IMMUTABLE`: only create-once publication is allowed; and
- `PROGRAM_ONLY`: only an explicitly invoked atomic program may mutate the
  path.

Every expanded domain path whose state may influence an atomic program, and
every path it may write or delete, is `PROGRAM_ONLY`. This is what makes a
local lock table at one singleton executor sufficient: no ordinary writer can
change a dependency while a program is preparing. An ordinary `Get` remains
allowed subject to Zanzibar authorization.

An ordinary `Put`, `Delete`, or CAS against such a path fails with
`PROGRAM_CONCURRENCY_VIOLATION` whether or not a program is executing at that
instant. A competing atomic program instead waits for the same exact-path lock,
then reads the newly committed state and recalculates. The lock itself is not
persistent: it is held only for one invocation and is released after commit,
definitive failure, cancellation, or executor loss.

Path policy is the lasting admission boundary, not the lock. In 0.5.0 policy
may only become stricter, so a path admitted to `PROGRAM_ONLY` does not later
return to ordinary mutation. A one-path atomic program remains a valid way to
change such a path. Prefixes may select policy, but prefixes are never locks
and are never expanded into Raft state.

### 5.5 Core Zanzibar authorization

Zanzibar authorization is part of the 0.5 storage core. It is not an index,
gateway, compatibility feature, or optional policy plug-in. Anvil uses the same
schema, tuple, revision, and evaluator primitives that it exposes to
applications.

Authentication establishes an immutable caller identity:

```text
Caller {
  storage_tenant
  subject = namespace:id
}
```

The tenant in an ordinary object address must equal the authenticated caller's
storage tenant. Clients cannot select another tenant by changing a request
field. Anonymous access, where enabled by a later capability, is represented by
the explicit reserved public subject and is evaluated normally; it is not an
authorization bypass.

System bootstrap is an explicit operator action. A fresh 0.5.0 node does not
guess that it should bootstrap and does not create authority merely because its
data directory is empty. The operator starts the intended first node with:

```text
--run-system-bootstrap
```

That one operation publishes and binds the system schema through the ordinary
authorization repository, establishes one durable bootstrap application
identity and credential, and adds the ordinary tuple:

```text
system:_anvil#bootstrap_admin@app:<bootstrap-app-id>
```

Bootstrap generates a high-entropy credential document at the operator's
explicit output path or, by default:

```text
<data-dir>/system-bootstrap-credential.json
```

The file is created without replacement at mode `0600`, flushed durably, and
contains the system tenant, stable bootstrap application ID, client ID, and
client secret. Anvil logs the exact path and tells the administrator to copy it
to their secret store and delete this generated copy; it never logs the secret.
Only a salted verifier is stored in Anvil.

The credential file is durable before one atomic metadata write installs the
schema, binding, application/verifier, `bootstrap_admin` tuple, authorization
revision, and permanent versioned bootstrap-complete marker. A crash before
that metadata write may resume only from the exact existing secure output file;
it must not generate a different administrator or overwrite the file. A crash
after the metadata write observes the marker and never bootstraps again.

Deleting the copied credential file does not delete the completion marker.
Repeating bootstrap must fail and must never mint another bootstrap
administrator. A later distributed capability replicates the bootstrap
identity metadata, system schema, binding, tuples, and completion state to
joining nodes; joining nodes therefore do not run cluster bootstrap.

Long-lived client credentials are exchanged at a separate unauthenticated
boundary for a short-lived signed token. The token fixes the storage tenant and
stable application subject installed as `Caller`. Protected typed provisioning
operations create storage tenants, applications, buckets, and their validated
system-realm ownership or grant tuples. They do not expose public mutation of
the reserved system scope or add an administrator bypass.

An authorization realm is identified by:

```text
AuthzScope {
  storage_tenant
  realm
}
```

The default application realm name is `default`. A realm is a scope, not a
registry entry or an object that must be created first. It begins to exist when
an immutable schema revision is bound to that scope. Namespace, object,
relation, and userset identities remain structurally inside their realm;
implementations must not emulate isolation by exposing realm prefixes in
namespace strings. A userset cannot cross a realm.

There is one protected system scope whose identity is reserved to Anvil:

```text
storage tenant = Anvil's internal system tenant
realm          = _anvil/system
schema         = anvil-system
```

The system realm is otherwise an ordinary realm. Its schema revisions,
binding, tuples, revision, snapshots, and permission evaluation use the same
types, storage operations, limits, and evaluator as every application realm.
There is no separate ACL engine and no hard-coded administrator role that
short-circuits checks.

Only Anvil's internal bootstrap and system operations may target the reserved
system scope for mutation. Public realm APIs reject that scope even if a caller
spells it explicitly. This protects mutation authority, not representation:
internal system writes still execute the same validated schema/binding/tuple
operations as third-party writes.

The system schema models the resources Anvil currently exposes. The 0.5.0
schema includes the system root, storage tenants, buckets, exact objects, and
authorization realms. Later capability releases add their own resource
namespaces to a new immutable system-schema revision; they do not add a second
authorization mechanism.

Every public request is mapped to a system-realm permission check for the
authenticated subject. In particular:

- object `Get` and `Head` check exact-object `get`, with an explicit bucket
  `get_object` fallback;
- object `Put`, `Publish`, and each `BulkWrite` put check exact-object `put`,
  with an explicit bucket `put_object` fallback;
- object `Delete` and each bulk delete check exact-object `delete`, with an
  explicit bucket `delete_object` fallback;
- bucket-policy mutation checks the bucket's policy-management permission;
- loading an atomic program checks the program definition as an exact object,
  and every expanded read, put, or delete path is checked as the invoking
  caller before locks are acquired; and
- custom realm schema, tuple, read, and check requests are authorized against
  the corresponding system `authz_realm` resource.

The bucket fallback is evaluated structurally from the exact object address.
Anvil must not write a `parent_bucket` tuple for every object merely to recover
information already present in that address.

The protected `authz_realm` resource exposes relations and derived permissions
for its owner, schema administrators, tuple writers, checkers, and auditors.
Binding the first schema is the custom-realm creation boundary. It also grants
the creating caller ownership and records the realm's parent storage tenant in
the protected system realm. Subsequent authority comes from Zanzibar tuples,
not from an `admin` flag. Schema publication is tenant-wide; realm binding and
realm operations are authorized independently.

#### 5.5.1 Schemas and bindings

A storage tenant owns immutable, canonical schema revisions:

```text
SchemaRef {
  schema_id
  schema_revision
  schema_digest
}
```

The exact same canonical schema content is an idempotent replay. Different
content advances that schema ID's revision. A realm binding selects one exact
schema reference and has a monotonically increasing binding generation. First
binding accepts an absent or zero expected generation; rebinding requires an
exact generation CAS. Tuple mutation is validated against, and conditional on,
the observed binding so a concurrent rebind cannot admit a batch under the
wrong schema.

The bounded schema language has:

- direct relations, which accept tuples and declare allowed subject selectors;
- permissions, which never accept tuples and are bounded unions of
  same-object inheritance and tuple-to-userset rewrites;
- selectors for any canonical ID in a namespace, one exact subject,
  same-resource ID, one userset relation, and the explicit public subject; and
- complete validation of referenced namespaces, relations, and userset
  targets before publication.

There are no caveats in 0.5.0. A hash field without stored expressions and
request-context evaluation is not caveat support and is omitted.

#### 5.5.2 Tuple batches, revisions, and checks

A tuple mutation batch belongs to one realm, contains at most the configured
hard maximum, and applies all-or-nothing in one durable metadata write. Every
mutation targets a direct relation and is validated before any mutation is
written. Add and remove are idempotent set operations.

Each storage tenant has one monotonically increasing authorization revision.
One schema publication, binding change, or tuple batch advances it once; every
tuple in one batch shares that revision. An optional expected revision is an
exact CAS. A bounded operation ID and canonical input fingerprint make a lost
tuple-batch response safely replayable and reject reuse with different input.

Checks and batch checks pin one authoritative storage snapshot and one exact
authorization revision for the complete graph walk. A batch check evaluates
all its questions at the same revision. Consistency choices are:

```text
LATEST       evaluate the current authoritative revision
AT_LEAST(r)  evaluate current state only if it is at least r
EXACT(r)     evaluate the retained snapshot at exactly r
```

The response returns the one revision it evaluated; there is no duplicate
"zookie" field containing the same number. Historical snapshots and operation
receipts have explicit bounded retention. A request for an expired exact
revision fails rather than silently evaluating a different state.

Evaluation is deterministic, fail-closed, cycle-safe, and bounded by schema
size, tuple count, recursion depth, graph nodes, and total work. Missing realm
bindings and undeclared relations deny or fail validation; there is no
schema-less compatibility fallback.

Authorization tuples and schema bodies are data-plane metadata, not Raft
commands. They are stored and atomically updated beside other authoritative
metadata. Raft may decide placement or leadership needed by the distributed
metadata plane, but tuple cardinality, schema bodies, and permission walks do
not enter the Raft log.

## 6. Atomic programs

An atomic program is an immutable, bounded server capability. An invocation
supplies a stable invocation ID, a pinned program identity, and bounded input.
It does not supply arbitrary code or a transaction plan.

A program definition is an ordinary immutable Anvil object whose exact path is
under the reserved `_anvil/programs/` prefix. For example:

```text
_anvil/programs/import_osv@1
```

The definition is created through the ordinary object write API using the
`Absent` condition. The same Zanzibar path authorisation used for every other
object decides whether the caller may write it. There is no program registry,
registry root, registration RPC, or Anvil-defined administrator role. The
nominated executor loads the exact program object and verifies its pinned
content hash before execution.

A program definition records at least:

```text
definition_schema_version
input_schema_hash
possible_read_path_templates[]
possible_write_path_templates[]
maximum_path_count
maximum_input_bytes
maximum_value_bytes
maximum_emitted_bytes
maximum_execution_work
```

All possible paths must be derivable from validated input before locks are
taken. Execution may use a subset, but it cannot discover a new path later.
Every expanded path is authorised as the invoking caller. Permission to write
a program definition grants no privilege over paths that the program later
reads or writes.

Programs are deterministic. Clocks, randomness, generated IDs, and timestamps
needed in output are explicit inputs or deterministic derivations from the
invocation ID. Execution has hard path, byte, time, and work limits. It cannot
make network calls or trigger external effects inside the commit.

Whether the first implementation represents a program as a small interpreter
definition or as a compiled built-in handler is an explicit architecture gap
in section 16. Either representation must obey this same data-plane contract.

## 7. The singleton executor

At any time Raft retains one current executor nomination:

```text
AtomicExecutor {
  node_id
  nomination_log_index
}
```

In 0.5.0 the sole node nominates itself. A later distributed capability permits
any current cluster voter or learner to be nominated; eligibility is not
derived from object paths or payload placement. A request received elsewhere
is then authenticated normally and proxied to the current executor without
changing the caller identity or capability checks.

The nomination's committed Raft log index is the epoch. There is no separately
allocated epoch counter.

Before executing at nomination index `E`, the node must have learned all Raft
entries through `E`, including every earlier committed batch. Every
`CommitBatch` proposed by that executor names `E`. Raft rejects it if `E` is no
longer the current nomination.

When a later distributed capability commits a new nomination:

- the old executor may finish local computation or leave prepared orphans, but
  it cannot commit them;
- the new executor may begin only after it has caught up through its nomination
  entry; and
- retry with the same invocation ID goes to the new executor.

This one fence is retained in 0.5.0 even though only one executor exists. It is
therefore already the rule a later distributed capability uses to make local
in-memory locks safe across failover: two processes may temporarily believe
they are executor, but only the current nomination can make a batch visible.

### 7.1 Exact-path lock table

The executor expands the full possible path set, canonicalises each
`(tenant, bucket, path)`, sorts it, and acquires the exact-path locks in that
order. Locks exist only in executor memory while an invocation runs.

Every expanded path is locked, including a path the program only reads. Since
all such paths are `PROGRAM_ONLY`, every possible mutator uses this same table.
Ordinary mutations fail admission rather than waiting on these locks.

Sorted acquisition prevents local deadlock. Deadlines, cancellation, fair
waiting, and hard execution limits bound contention. A crash needs no lock
recovery.

The program reads current committed state only after it owns every lock.
Therefore an invocation that waited behind another sees the earlier invocation's
committed values and recalculates from them. There is no draft, snapshot from
invocation start, `xmin`/`xmax`, validation pass, or `EvalPlanQual` protocol.

## 8. Prepare and commit protocol

One atomic invocation produces one bounded mutation batch.

### 8.1 Prepare outside Raft

While holding its exact-path locks, the current executor:

1. validates program identity, input, limits, and replay state;
2. reads every required committed head/value;
3. executes the deterministic program;
4. allocates the result version identities;
5. creates every immutable payload and version descriptor;
6. creates one canonical bundle describing all old-head expectations and new
   heads;
7. writes all payloads, descriptors, and the bundle to the byte plane in
   an invisible prepared namespace; and
8. waits until the configured durability class is satisfied.

Nothing prepared is visible through ordinary reads. Preparation may be
repeated by content hash. An unreferenced preparation is an orphan eligible for
bounded garbage collection after its executor fence is stale or its preparation
deadline passes.

For `LOCAL` in 0.5.0, the entire recoverable result is synchronously durable on
the sole node before consensus. This protects process restart and ordinary
machine restart, but does not claim survival of permanent loss of that node or
disk. `REPLICATED` fails before consensus in 0.5.0.

When a later distributed capability implements `REPLICATED`, the entire
recoverable result must satisfy its configured remote acknowledgement count
before consensus. Raft is never used to carry or reconstruct payload bytes.

### 8.2 Compact Raft commit

After durable preparation, the executor proposes one bounded command shaped
semantically like:

```text
CommitBatch {
  invocation_id
  input_fingerprint
  program_path_hash
  program_hash
  executor_node_id
  executor_nomination_log_index
  bundle_ref
  bundle_hash
  durability_class
  durability_evidence_hash
}
```

The command contains no object body, program definition, complete version
descriptor, path list, or lock record. Before proposing it, the executor has
already loaded the named program object, verified its content hash, and
authorised its expanded paths. Raft validates the current nomination fence,
invocation replay state still retained by the core, bounded command shape, and
bundle/durability identities. It records the pinned object-path identity and
content hash; it does not maintain or consult a program registry.

The committed Raft log index `C` of `CommitBatch` is the batch's commit cursor.
It is assigned by consensus, not by the client. Committing `CommitBatch` is the
visibility decision. It is not certification: there is no read set, range set,
predicate stamp, draft synchronization, or second decision.

The executor releases locks only after `CommitBatch` commits or the invocation
definitively fails. If its response is lost after commit, retry uses the bounded
receipt described in section 10.

## 9. Visibility and idempotent finalization

In 0.5.0 the sole node learns each committed `CommitBatch`. Applying that
decision advances its logical committed view from all-old to all-new for the
batch; it must never expose only a subset of the new heads. A later distributed
capability applies the same rule independently at every current voter and
learner.

The prepared bundle is the complete description of the change. In 0.5.0 the
same node prepares, commits, fetches, and verifies it. If that node learns
commit `C` but cannot verify its local bundle, it returns an availability or
data-loss error rather than serving a falsely current partial view. A later
distributed capability must separately choose its follower freshness and proxy
contract before adding followers.

Finalization materialises committed versions and heads from the durable bundle
into serving structures. In 0.5.0 the entire bundle, its receipt, and required
invalidations are installed in one local atomic metadata write. Finalization is
keyed by `(C, bundle_hash)` and is idempotent:

- repeating finalization produces the same versions and heads;
- a conflicting bundle for `C` is corruption;
- any eligible recovery worker may retry incomplete work; and
- finalization never decides whether the batch committed.

Atomic visibility comes from the one committed Raft decision plus the complete
durable bundle. The 0.5.0 implementation may use its one local storage-engine
batch directly. A later distributed capability may use an atomic overlay while
finalization catches up, but that overlay is an implementation detail and not a
second source of truth.

Ordinary sequential reads remain read committed rather than snapshot reads. A
caller can read path A, an atomic batch can commit, and a later independent read
of path B can see the new value. The atomic guarantee is that a serving view at
one learned commit boundary does not expose a partial batch.

## 10. Recovery, bounded Raft state, and receipts

### 10.1 Failure outcomes

| Failure point | Required result |
| --- | --- |
| Before durable preparation | No new state is visible. |
| After partial preparation | No new state is visible; partial data is an orphan. |
| After durable preparation but before `CommitBatch` | No new state is visible; retry may reuse the prepared content. |
| After `CommitBatch` but before finalization | The batch is committed and visible logically; recovery fetches and finalizes the bundle. |
| After finalization but before response | Retry returns the retained receipt. |
| Old executor proposes after renomination in a later distributed cluster | Raft rejects its stale nomination log index. |
| A node cannot fetch a committed bundle | It must not serve a partial or falsely current view. |

### 10.2 Bounded Raft tail

Committed but not safely finalized batches form a bounded Raft recovery tail.
Raft periodically commits:

```text
FinalizedThrough { commit_log_index }
```

In 0.5.0 the checkpoint advances monotonically only after the sole node has
atomically installed every earlier `CommitBatch`, including versions, heads,
receipt, and invalidations, and has durably advanced its applied commit cursor.
Once a checkpoint is included in a Raft snapshot, older batch commands may be
compacted. The snapshot retains the current executor nomination,
finalized-through index, and only the bounded replay state still inside its
advertised window. A later distributed capability must define a new
cluster-wide finalized-through criterion before it adds followers.

The tail has hard entry and byte limits. If finalization cannot advance and the
tail reaches its bound, Anvil applies backpressure to new atomic-program
commits. It must not grow Raft without bound, discard recovery information, or
pretend an unfinalized batch is safe. Independent ordinary object operations do
not become transactions merely because atomic-program admission is stalled.

### 10.3 Bounded invocation receipts

Within the advertised replay window:

- the same invocation ID and input fingerprint returns the same committed
  result;
- the same invocation ID with different input fails; and
- a lost response can be recovered without reapplying the program.

Receipts are bounded by configured time and space limits. Every successful
response reports the replay-guarantee expiry. After that point Anvil does not
promise that an old invocation can be distinguished from a new submission or
that its original response can be reconstructed. A caller must not blindly
retry an expired financial or otherwise non-repeatable operation; it must
reconcile using domain state such as the immutable ledger-entry path.

The exact safe treatment of an expired invocation ID is an open architecture
gap in section 16. The RFC does not hide an unbounded receipt table behind the
word idempotency.

## 11. `WatchPrefix`: unordered invalidation, not CDC

`WatchPrefix` is a core capability for learning that current path state may
have changed. It is not an audit log and does not expose Raft order.

Each storage source maintains a bounded durable invalidation journal with its
own source epoch and local offset. An ordinary path mutation durably couples its
head change and source invalidation. An atomic batch derives its source
invalidations idempotently from the committed bundle; finalization cannot be
declared complete while required invalidations are missing.

There is no global watch counter. In 0.5.0 there is one storage source and one
bounded journal. A later distributed capability fans a prefix watch out to the
relevant storage sources and merges their journals.

### 11.1 Event semantics

An invalidation is shaped semantically like:

```text
Invalidate {
  path
  minimum_path_version
  state_hint = PRESENT | DELETED
}
```

Its meaning is: "the current state of this exact path may have changed; reread
until the observed path version is at least `minimum_path_version`."

The guarantees are:

- delivery is at least once;
- duplicates are legal;
- there is no order across paths or storage sources;
- consumers must tolerate out-of-order work completion;
- several rapid mutations of one path may be coalesced into one invalidation;
- every intermediate version or delete need not be reported; and
- the current reread, not `state_hint`, decides the derived result.

For example, put version 10, delete version 11, and recreate version 12 may
produce one invalidation requiring a read of at least version 12. If deletion at
version 13 is the final state, the path-state read returns `Deleted { version:
13 }` and the consumer removes its derived row. Delete/recreate therefore uses
one monotonically advancing path version; deletion must not reset identity.

A consumer that requires every mutation uses an explicit append/audit
capability, not `WatchPrefix`.

### 11.2 Opaque checkpoints

In 0.5.0 a resume point contains the sole source's epoch and local offset. It is
exposed only as a versioned opaque token so a later distributed capability can
extend it to a source vector without changing the API. The token is
integrity-protected and bound to the authenticated tenant, bucket, exact
prefix, source epoch, retention window, and offset. Clients must not parse or
compare it.

The stream has invalidations and checkpoint barriers:

```text
WatchMessage = Invalidate(...) | Checkpoint { resume_token }
```

`Checkpoint(T)` means that every retained source-journal record through the
token's offset has been represented by at least one preceding invalidation. In
a later distributed capability it covers every component of the source vector.
It never means that invalidations form a global order or timestamp.

The consumer stores `T` only after all preceding invalidations have been
durably applied. A crash before storing it causes duplicates on reconnect. A
server must never advance a checkpoint over an invalidation it silently
dropped.

New subscriptions explicitly choose `NOW` or the retained beginning; an empty
token has no ambiguous resume meaning. If the token offset is older than the
source's retention floor, or its source epoch is not current, the server
returns `RESUME_EXPIRED` and requires a full prefix rebuild. It must not
silently skip to the current tail.

This contract is sufficient for future current-state index builders: reread
the latest path state, update or remove the derived row conditional on source
version, and persist a checkpoint only after all prior work is durable. An
index that requires every historical version needs a different source log.

## 12. Ledger example

Consider two `PROGRAM_ONLY` paths:

```text
billing/ledger/tenant-7/ledger-entry-op-42
billing/balances/tenant-7
```

Before `record_usage(op-42, charge=25 GBP)`:

```text
ledger entry: NeverExisted

balance @ version 9:
  {"currency":"GBP","balance_minor":70,"last_entry_id":"op-41"}
```

In 0.5.0 the request arrives at the sole node, which is the singleton executor
nominated at Raft index 811. The executor:

1. verifies the program and caller;
2. sorts and locks the two exact paths locally;
3. reads the absent ledger entry and balance version 9;
4. asserts absence and currency, then calculates balance 95;
5. prepares two complete immutable versions:

```text
ledger entry @ version 1:
  {"entry_id":"op-42","delta_minor":25,
   "resulting_balance_minor":95,"currency":"GBP"}

balance @ version 10:
  {"currency":"GBP","balance_minor":95,"last_entry_id":"op-42"}
```

6. writes both payloads, both version descriptors, and one bundle to local
   durable storage and waits for `LOCAL` durability;
7. proposes one compact `CommitBatch` naming executor fence 811 and the bundle
   hash; and
8. makes both versions visible when that command commits at Raft index 8,842.

The sole node learns commit 8,842. Its finalizer may materialise the heads more
than once, but the result remains ledger-entry version 1 and balance version
10.

No path prefix or tenant aggregate is assigned a writer or coordinator. Only
these two exact paths are locked. A concurrent command touching the same
balance waits, then reads balance 95 and recalculates. A command for another
balance can execute concurrently at the same singleton because its locks do not
overlap.

A retry inside the receipt window returns the original result. After that
window, the immutable ledger path remains the domain-level evidence that
`op-42` was already recorded.

## 13. Semantic API shapes

These shapes define capability boundaries, not protobuf field numbers.

```text
Get {
  tenant, bucket, path, version = CURRENT | specific
}

Put {
  command_id, tenant, bucket, path, bytes,
  condition = Any | Absent | Version(v),
  durability = LOCAL | REPLICATED
}

Delete {
  command_id, tenant, bucket, path,
  condition = Any | Version(v),
  durability = LOCAL | REPLICATED
}

BulkWrite {
  items[] = Put | Delete
}
```

None has `transaction_id`, `begin`, `stage`, or `commit` fields.

Atomic program invocation is a separate explicit capability:

```text
InvokeAtomicProgram {
  invocation_id
  program_address
  program_hash
  input
  durability = LOCAL | REPLICATED
}

InvokeAtomicProgramResult {
  invocation_id
  program_address
  program_hash
  executor_nomination_log_index
  commit_log_index
  path_receipts[]
  output
  replay_guarantee_expires_at
}
```

Authorization uses a separate bounded service surface over the same core
repository Anvil consults internally:

```text
PutAuthzSchema {
  schema_id
  namespaces[]
}

BindAuthzSchema {
  scope
  schema_ref
  expected_binding_generation?
}

MutateAuthzTuples {
  scope
  operation_id
  expected_revision?
  mutations[] = Add(tuple) | Remove(tuple)
}

ReadAuthzTuples {
  scope
  filters
  consistency
  page_token?
}

CheckPermission {
  scope
  subject
  object
  relation
  consistency
}

CheckPermissions {
  scope
  checks[]
  consistency
}
```

There is deliberately no `CreateRealm`, `DeleteRealm`, `ApplySchema`, writable
permission tuple, caller-supplied publication metadata, realm-encoded
namespace, or duplicate revision token. The API exposes immutable schema
publication, binding CAS, atomic tuple-set mutation, tuple inspection, and
revision-pinned evaluation directly.

Required stable outcome classes include:

```text
APPLIED
IDEMPOTENT_REPLAY
CONDITION_FAILED
ASSERTION_FAILED
IDEMPOTENCY_INPUT_MISMATCH
EXECUTOR_MOVED
PROGRAM_VERSION_MISMATCH
PROGRAM_CONCURRENCY_VIOLATION
RESOURCE_LIMIT
DURABILITY_UNAVAILABLE
FINALIZATION_LAG
REPLAY_GUARANTEE_EXPIRED
RESUME_EXPIRED
```

`EXECUTOR_MOVED` is retryable with the same invocation ID while its replay
window remains valid. Assertion, concurrency, version, and input-mismatch
failures are not automatically retryable.

## 14. Capability release sequence

Anvil 0.5 is delivered as capabilities, not as layers of compatibility code.

1. **0.5.0 Core:** opaque versioned objects, ordinary CAS/immutable/bulk
   writes, bounded atomic programs, executor nomination and recovery,
   `WatchPrefix`, authentication, realm-scoped Zanzibar authorization,
   single-node `LOCAL` durability, a stable unavailable `REPLICATED` request
   value, and required operator evidence.
2. **Index capability release:** current-state indexes and their builders use
   the core read/version/watch contracts. They are rebuilt from source and are
   never part of an atomic core commit.
3. **Gateway capability release:** protocol adapters map their honest subset
   onto the stable core. They do not restore removed transaction or index APIs.

Exact post-0.5.0 version numbers belong to the release plan; this RFC fixes the
sequence rather than inventing dates.

A later, discrete 0.5.x distribution capability adds membership, peer
transport, executor proxying/failover, replicated byte placement, and genuine
`REPLICATED` acknowledgements from at least configured `N` nodes. It is not
silently folded into an index or gateway implementation, and it does not widen
the 0.5.0 image's claims.

No 0.4 RPC is retained merely because generated clients or old gateway code
used it. There is no dual read, dual write, in-place upgrade, mixed-version
rolling upgrade, or automatic data-directory conversion. Migration is explicit
export from the old deployment and import through 0.5 capabilities.

## 15. Developer Defence performance contract

Performance is a 0.5.0 release gate.

The benchmark manifest records:

- OSV corpus identity and hash;
- source-record count `N`;
- logical path-mutation count `M`;
- encoded input and stored byte counts;
- Developer Defence schema/program versions;
- durability configuration;
- hardware and storage layout;
- client concurrency and batch size; and
- p50, p95, p99, and end-to-end time.

The pinned corpus must ingest into the real Developer Defence schema on one
node in at most 150 seconds. Required sustained rates are:

```text
source records/second = N / 150
logical mutations/second = M / 150
```

The observed failure rate of one record every ten seconds is 0.1 record/second
and would ingest only 15 records in 150 seconds.

At a fixed 10 ms command/request overhead and `B` independent records per bulk
request, the overhead-only ceilings are:

| Records per request `B` | Requests/second | Records/second | Records in 150 seconds |
| ---: | ---: | ---: | ---: |
| 1 | 100 | 100 | 15,000 |
| 100 | 100 | 10,000 | 1,500,000 |
| 500 | 100 | 50,000 | 7,500,000 |
| 1,000 | 100 | 100,000 | 15,000,000 |

For `N` records the simplified overhead is
`ceil(N / B) * 0.010 seconds`. Validation, hashing, bytes, durability, flow
control, and storage bandwidth make real throughput lower; the table is not a
promise.

Release qualification therefore requires:

- no one-RPC-per-record importer when safe bounded bulk is possible;
- no atomic-program or Raft lifecycle for independent OSV records;
- no one-fsync-per-logical-subrecord design when configured durability permits
  a shared bounded batch;
- average small-command/request overhead at or below the documented 10 ms
  target on qualification hardware, with percentiles reported separately;
- the full pinned Developer Defence import in at most 150 seconds; and
- retry, crash, durability, finalization, and watch-journal work included in
  the benchmark rather than measured later as hidden debt.

If the measured corpus cannot satisfy 150 seconds at the selected durability
and hardware, the release fails. The target is not weakened by calling the
remaining time background finalization.

## 16. Architecture gaps that must be closed

The confirmed shape above intentionally does not invent answers to these
remaining questions:

1. **Expanded authorization intent.** The executor must authorize reads, puts,
   and deletes using their distinct system-realm permissions. The bounded
   program representation must expose exact operation intent for every
   expanded path rather than collapsing put and delete into one `ReadWrite`
   label.
2. **Expired invocation IDs.** Bounded receipts mean old replay evidence is
   eventually gone. The API must decide whether expired IDs are recognisable
   and rejected, carry a time/window component, or may be treated as new. It
   must not claim indefinite idempotency without indefinite state.
3. **Single-source watch retention.** The implementation must fix the 0.5.0
   journal's entry/byte or time bounds, pruning trigger, durable source epoch,
   and exact `RESUME_EXPIRED` boundary.
4. **Program representation.** The first release must choose a small bounded
   interpreter format or compiled built-in handlers. Canonical identity is
   already the immutable program object's full address plus pinned content
   hash; the implementation must not grow both representation mechanisms in
   0.5.0.
5. **Authorization retention.** Exact-revision checks and idempotent tuple
   mutation receipts require finite advertised retention. The implementation
   must fix the bounds, expired-revision result, pruning trigger, and recovery
   rule without retaining every historical authorization state forever.

These are release blockers for the affected capability. They do not justify
reintroducing transactions, path-derived routing, payloads in Raft, or
unbounded logs.

The later distributed capability separately must define replicated durability
evidence for configured `N`, cluster-wide finalized-through evidence,
follower-read freshness/proxying, snapshot installation, and multi-source watch
epoch handoff. None is silently claimed by the single-node 0.5.0 release.

## 17. Correctness and operational evidence

The 0.5.0 core is not release-qualified until tests demonstrate:

- ordinary `Put`, `Delete`, CAS, and bulk have no transaction lifecycle;
- bulk partial success and idempotent retry of unknown items;
- ordinary APIs reject `PROGRAM_ONLY` mutation specifically as
  `PROGRAM_CONCURRENCY_VIOLATION`;
- all expanded mutable dependencies, including read-only dependencies, require
  `PROGRAM_ONLY`, while locks are released at invocation end;
- `LOCAL` completes the configured synchronous local write, `REPLICATED`
  returns `DURABILITY_UNAVAILABLE` without mutation, and every other value is
  invalid;
- authenticated tenant identity cannot be replaced by an object or authz scope
  supplied in the request;
- the system realm and a third-party realm use the same schema, tuple,
  revision, snapshot, and evaluator implementation;
- public APIs cannot read, bind, or mutate the reserved system realm;
- system bootstrap runs only under `--run-system-bootstrap`, uses validated
  system-realm tuples rather than a bypass role, and cannot mint a second
  bootstrap administrator;
- object, bucket-policy, program-definition, expanded program-path, and custom
  realm operations are denied unless the corresponding system-realm check
  allows them;
- custom realms are isolated by structural scope and usersets cannot cross
  realms;
- immutable schema replay, changed-content revision, binding-generation CAS,
  and failure on a concurrent rebind;
- whole-batch tuple validation, one durable mutation, one shared revision,
  expected-revision conflict, idempotent replay, and input mismatch;
- direct, exact, same-resource, userset, and explicit-public selectors;
- inherited and tuple-to-userset evaluation, cycle termination, hard work
  bounds, fail-closed missing bindings, and same-revision batch checks;
- exact-object authorization with structural bucket fallback and no mandatory
  parent tuple per stored object;
- deterministic bounded path expansion and per-path authorisation;
- canonical exact-path lock ordering, cancellation, and contention bounds;
- the sole node nominates itself and every `CommitBatch` names that nomination
  log index;
- complete synchronous local preparation before every 0.5.0 `CommitBatch`;
- application payload bytes never enter Raft;
- crashes at every row of the failure table;
- all-old/all-new visibility at the sole node's learned commit boundary;
- idempotent finalization and corruption detection for conflicting bundles;
- finalized-through compaction and hard backpressure at the Raft-tail bound;
- bounded orphan cleanup and bounded receipt retention;
- duplicate, coalesced, unordered, resumed, and expired watch behavior;
- delete/recreate invalidation using monotonic path versions;
- explicit rebuild after `RESUME_EXPIRED`; and
- the pinned Developer Defence 150-second benchmark.

Metrics expose executor nomination, lock wait, program execution,
prepare bytes and latency, durability wait, Raft commit latency, unfinalized
tail entries/bytes, finalized-through lag, finalization retries, orphan bytes,
receipt-window usage, watch-journal retention/lag, bulk throughput, and
end-to-end Developer Defence ingest time. Traces carry invocation ID, program
hash, nomination log index, commit log index, and bundle hash without logging
opaque payloads.

## 18. Consequences

The common path stays simple: one exact-path operation or a non-atomic transport
batch. Applications pay singleton execution, multi-path locking, synchronous
local preparation, and one Raft decision only when they invoke an explicit
atomic capability.

Authorization has one model rather than an operational ACL layer beside a
product tuple layer. The protected system realm governs Anvil requests; custom
realms govern application relationships. Protection changes who may mutate the
system scope, not how that scope is stored or evaluated. Schema and tuple
cardinality stay outside Raft, and structural object addresses avoid one
authorization tuple per stored path.

The singleton is intentionally a KISS trade-off. It removes distributed locks,
path-derived ownership and routing, cross-owner commit, and certification. It
may eventually become a throughput limit, but 0.5.0 must measure that before
adding coordination machinery.

Atomic commit remains recoverable because the complete result is durable before
Raft makes it visible. Raft remains bounded because finalized commits are
checkpointed and compacted, and the system stops admitting atomic work rather
than growing an unbounded recovery tail. Watches remain reliable without
pretending independently ordered storage sources have one global timeline.

The result is a clean 0.5 core capability, not a relational transaction system
and not a compatibility shell around the 0.4 design.
