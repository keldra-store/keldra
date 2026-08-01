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
- an unordered, resumable `WatchPrefix` invalidation feed.

Ordinary `Put`, `Delete`, CAS, and bulk operations are never transactions. They
do not accept a transaction ID, do not stage work for a later client commit,
and do not pass through the atomic-program executor. A bulk request shares
transport and bounded server work, but each item succeeds or fails
independently.

An atomic program is different because it deliberately changes several exact
paths under one visibility decision. Calls to an explicit atomic API may arrive
at any node. That node proxies the call to the cluster's one currently nominated
atomic-program executor. Any current Raft voter or learner is eligible to be
that singleton executor.

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
   batch bundle on distributed storage;
6. waits for the configured durability requirement;
7. proposes one compact `CommitBatch` command containing identities, hashes,
   durable references, and its nomination log index, but no application
   payload; and
8. treats the committed `CommitBatch` entry as the visibility decision.

All current voters and learners receive the committed decision through Raft.
Materialising that decision into serving structures is deterministic and
idempotent. It may be retried by recovery workers; it is not a second commit.

Raft is used only for bounded distributed decisions: executor nomination,
compact atomic-batch commits, and a monotonically advancing finalized-through
checkpoint. Object bodies, program definitions, complete version descriptors,
path inventories, locks, and prepared bundles remain in distributed storage
outside Raft.

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
5. Make executor failover safe with one simple Raft fence.
6. Keep Raft state and replay receipts explicitly bounded.
7. Provide reliable invalidation watches without inventing a global event
   sequence.
8. Ingest the pinned Developer Defence OSV corpus into the Developer Defence
   schema on one node in at most 150 seconds.

## 4. Non-goals

Anvil 0.5.0 does not provide:

- `BeginTransaction`, staged mutation, client `Commit`, rollback, transaction
  status, or certification;
- serializable or repeatable-read snapshots;
- arbitrary client-supplied read/write plans;
- range or prefix locks;
- a distributed lock service;
- path-derived routing or coordination ownership;
- SQL predicates, joins, foreign keys, or uniqueness constraints;
- automatic payload merging;
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

### 5.2 Bulk transport

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

### 5.3 Path policy

Path policies are capability admission rules:

- `MUTABLE`: ordinary versioned `Put`, `Delete`, and CAS are allowed;
- `IMMUTABLE`: only create-once publication is allowed; and
- `PROGRAM_ONLY`: only an explicitly invoked atomic program may publish the
  path.

Every path written by an atomic program is `PROGRAM_ONLY`. This is what makes a
local lock table at one singleton executor sufficient: no ordinary writer can
bypass it. Prefixes may select policy, but prefixes are never locks and are
never expanded into Raft state.

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

Any current cluster voter or learner may be nominated. Eligibility is not
derived from object paths or payload placement. A request received elsewhere
is authenticated normally, then proxied to the current executor without
changing the caller identity or capability checks.

The nomination's committed Raft log index is the epoch. There is no separately
allocated epoch counter.

Before executing at nomination index `E`, the node must have learned all Raft
entries through `E`, including every earlier committed batch. Every
`CommitBatch` proposed by that executor names `E`. Raft rejects it if `E` is no
longer the current nomination.

After a new nomination commits:

- the old executor may finish local computation or leave prepared orphans, but
  it cannot commit them;
- the new executor may begin only after it has caught up through its nomination
  entry; and
- retry with the same invocation ID goes to the new executor.

This one fence makes local in-memory locks safe across failover. Two processes
may temporarily believe they are executor, but only the current nomination can
make a batch visible.

### 7.1 Exact-path lock table

The executor expands the full possible path set, canonicalises each
`(tenant, bucket, path)`, sorts it, and acquires the exact-path locks in that
order. Locks exist only in executor memory while an invocation runs.

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
7. writes all payloads, descriptors, and the bundle to distributed storage in
   an invisible prepared namespace; and
8. waits until the configured durability class is satisfied.

Nothing prepared is visible through ordinary reads. Preparation may be
repeated by content hash. An unreferenced preparation is an orphan eligible for
bounded garbage collection after its executor fence is stale or its preparation
deadline passes.

The entire recoverable result is remote and durable before consensus. Raft is
not used to carry or reconstruct payload bytes.

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

All current Raft voters and learners learn each committed `CommitBatch`.
Applying that decision advances a node's logical committed view from all-old to
all-new for the batch. A node must never expose only a subset of the new heads.

The prepared bundle is the complete description of the change. A node that has
learned commit `C` but cannot yet fetch and verify its bundle cannot claim that
its serving view includes `C`: it must continue under an explicitly stale-read
contract, proxy, or return an availability error. Until it has the bundle it
does not know the affected path inventory, so it cannot safely claim a current
view while selectively serving pre-`C` values. Section 16 leaves the public
freshness choice explicit rather than disguising it as finalization.

Finalization materialises committed versions and heads from the durable bundle
into normal distributed and node-local serving structures. It is keyed by
`(C, bundle_hash)` and is idempotent:

- repeating finalization produces the same versions and heads;
- a conflicting bundle for `C` is corruption;
- any eligible recovery worker may retry incomplete work; and
- finalization never decides whether the batch committed.

Atomic visibility comes from the one committed Raft decision plus the complete
durable bundle. It does not depend on one former owner installing every path in
one local storage-engine batch. Node-local indexes or caches may use an atomic
overlay while finalization catches up, but that is an implementation detail and
not a second source of truth.

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
| Old executor proposes after renomination | Raft rejects its stale nomination log index. |
| A node cannot fetch a committed bundle | It must not serve a partial or falsely current view. |

### 10.2 Bounded Raft tail

Committed but not safely finalized batches form a bounded Raft recovery tail.
Raft periodically commits:

```text
FinalizedThrough { commit_log_index }
```

The checkpoint advances monotonically only when every earlier `CommitBatch` is
recoverably finalized under the cluster's agreed criterion. Once a checkpoint
is included in a Raft snapshot, older batch commands may be compacted. The
snapshot retains the current executor nomination, finalized-through index, and
only the bounded replay state still inside its advertised window.

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

There is no global watch counter. A prefix watch fans out to the relevant
storage sources and merges their journals.

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

### 11.2 Opaque vector checkpoints

Because sources are independently ordered, a resume point is a vector, exposed
only as a versioned opaque token. The token is integrity-protected and bound to
the authenticated tenant, bucket, exact prefix, source topology/epochs,
retention window, and per-source offsets. Clients must not parse or compare it.

The stream has invalidations and checkpoint barriers:

```text
WatchMessage = Invalidate(...) | Checkpoint { resume_token }
```

`Checkpoint(T)` means that every retained source-journal record covered by
vector `T` has been represented by at least one preceding invalidation. It does
not mean that those invalidations form a global order or a single timestamp.

The consumer stores `T` only after all preceding invalidations have been
durably applied. A crash before storing it causes duplicates on reconnect. A
server must never advance a checkpoint over an invalidation it silently
dropped.

New subscriptions explicitly choose `NOW` or the retained beginning; an empty
token has no ambiguous resume meaning. If any token component is older than its
source's retention floor, or its source epoch can no longer be translated, the
server returns `RESUME_EXPIRED` and requires a full prefix rebuild. It must not
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

The request may arrive at any node. That node proxies it to the current
singleton executor nominated at Raft index 811. The executor:

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

6. writes both payloads, both version descriptors, and one bundle to
   distributed storage and waits for configured durability;
7. proposes one compact `CommitBatch` naming executor fence 811 and the bundle
   hash; and
8. makes both versions visible when that command commits at Raft index 8,842.

Every current voter and learner learns commit 8,842. Finalizers may materialise
its heads more than once, but the result remains ledger-entry version 1 and
balance version 10.

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
  condition = Any | Absent | Version(v), durability
}

Delete {
  command_id, tenant, bucket, path,
  condition = Any | Version(v), durability
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
  durability
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

Required stable outcome classes include:

```text
APPLIED
IDEMPOTENT_REPLAY
CONDITION_FAILED
ASSERTION_FAILED
IDEMPOTENCY_INPUT_MISMATCH
EXECUTOR_MOVED
PROGRAM_VERSION_MISMATCH
PROGRAM_POLICY_VIOLATION
RESOURCE_LIMIT
DURABILITY_UNAVAILABLE
FINALIZATION_LAG
REPLAY_GUARANTEE_EXPIRED
RESUME_EXPIRED
```

`EXECUTOR_MOVED` is retryable with the same invocation ID while its replay
window remains valid. Assertion, policy, version, and input-mismatch failures
are not automatically retryable.

## 14. Capability release sequence

Anvil 0.5 is delivered as capabilities, not as layers of compatibility code.

1. **0.5.0 Core:** opaque versioned objects, ordinary CAS/immutable/bulk
   writes, bounded atomic programs, executor nomination and recovery,
   `WatchPrefix`, authentication, durability, and required operator evidence.
2. **Index capability release:** current-state indexes and their builders use
   the core read/version/watch contracts. They are rebuilt from source and are
   never part of an atomic core commit.
3. **Gateway capability release:** protocol adapters map their honest subset
   onto the stable core. They do not restore removed transaction or index APIs.

Exact post-0.5.0 version numbers belong to the release plan; this RFC fixes the
sequence rather than inventing dates.

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

1. **Durability evidence.** Each durability class needs an exact, independently
   verifiable rule for when payloads, descriptors, and bundle are safe enough
   for `CommitBatch`. In particular, a node-local preparation cannot support
   executor-loss recovery after commit unless the API explicitly accepts that
   availability/loss boundary.
2. **Finalized-through evidence.** The cluster needs a precise criterion for
   proposing `FinalizedThrough`: which distributed facts and which current
   voters/learners must acknowledge, how membership removal affects the
   minimum, and how a lagging node installs a compacted snapshot. This cannot be
   replaced by a timeout.
3. **Follower read freshness.** Each node can move its own serving view from
   all-old to all-new atomically, but the public contract must choose whether a
   read may be stale while that node is behind Raft, must proxy, or must perform
   a freshness barrier. `CommitBatch` alone does not answer cross-node read
   latency.
4. **Expired invocation IDs.** Bounded receipts mean old replay evidence is
   eventually gone. The API must decide whether expired IDs are recognisable
   and rejected, carry a time/window component, or may be treated as new. It
   must not claim indefinite idempotency without indefinite state.
5. **Watch source topology.** Opaque vector tokens need a specified rule for
   storage-source replacement, journal handoff, source-epoch translation, and
   the point at which a token becomes `RESUME_EXPIRED`.
6. **Program representation.** The first release must choose a small bounded
   interpreter format or compiled built-in handlers. Canonical identity is
   already the immutable program object's full address plus pinned content
   hash; the implementation must not grow both representation mechanisms in
   0.5.0.

These are release blockers for the affected capability. They do not justify
reintroducing transactions, path-derived routing, payloads in Raft, or
unbounded logs.

## 17. Correctness and operational evidence

The 0.5.0 core is not release-qualified until tests demonstrate:

- ordinary `Put`, `Delete`, CAS, and bulk have no transaction lifecycle;
- bulk partial success and idempotent retry of unknown items;
- ordinary APIs cannot mutate `PROGRAM_ONLY` paths;
- deterministic bounded path expansion and per-path authorisation;
- canonical exact-path lock ordering, cancellation, and contention bounds;
- any current voter or learner can be nominated executor;
- the nomination Raft log index fences the former executor;
- complete remote preparation and durability before every `CommitBatch`;
- application payload bytes never enter Raft;
- crashes at every row of the failure table;
- all-old/all-new visibility at each node's learned commit boundary;
- idempotent finalization and corruption detection for conflicting bundles;
- finalized-through compaction and hard backpressure at the Raft-tail bound;
- bounded orphan cleanup and bounded receipt retention;
- duplicate, coalesced, unordered, resumed, and expired watch behavior;
- delete/recreate invalidation using monotonic path versions;
- explicit rebuild after `RESUME_EXPIRED`; and
- the pinned Developer Defence 150-second benchmark.

Metrics expose executor nomination and proxying, lock wait, program execution,
prepare bytes and latency, durability wait, Raft commit latency, unfinalized
tail entries/bytes, finalized-through lag, finalization retries, orphan bytes,
receipt-window usage, watch-journal retention/lag, bulk throughput, and
end-to-end Developer Defence ingest time. Traces carry invocation ID, program
hash, nomination log index, commit log index, and bundle hash without logging
opaque payloads.

## 18. Consequences

The common path stays simple: one exact-path operation or a non-atomic transport
batch. Applications pay singleton execution, multi-path locking, remote durable
preparation, and one Raft decision only when they invoke an explicit atomic
capability.

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
