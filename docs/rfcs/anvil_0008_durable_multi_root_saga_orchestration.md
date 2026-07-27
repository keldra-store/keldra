# ANVIL-0008: Durable Multi-Root Saga Orchestration

Status: Superseded by `docs/rfcs/mvcc_under_raft.md`
Normative dependency incorporated by reference: ANVIL-0007  
Supersedes: ANVIL-0007 for combined ANVIL-0008 conformance; ANVIL-0008 prevails on conflict  
Audience: Anvil implementors, operators, SDK authors, storage engineers, and API designers  
Scope: Durable saga construction, execution, compensation, recovery, reference retention, scheduling, observability, and integration with CoreStore

Cluster-local MVCC transactions remove the root boundary that motivated this
compensating-SAGA design. This document is retained only as historical context.

## 1. Summary

Anvil must provide a general durable saga engine for business operations that
span any number of CoreStore roots and therefore cannot share one Anvil
transaction lifecycle. The engine is logically centralised per saga: one
authoritative durable state machine determines the saga's next valid transition.
It is not physically centralised on one process or node. Any eligible Anvil node
may run a task that claims and applies one runnable saga transaction block using
a lease fence and state-version compare-and-swap.

A saga is constructed incrementally. A caller starts a saga, opens an ordered
saga transaction block, records ordinary native Anvil mutation requests into that
block, seals the block, and repeats as many times as required. Recording a saga
transaction block does not open a target-root Anvil transaction and does not
change target-root visibility. `ApplySaga` freezes the complete plan and starts
asynchronous execution. Each sealed forward or compensation block is then
executed as exactly one ordinary single-root Anvil transaction.

A saga may:

- contain an arbitrary number of transaction blocks;
- address an arbitrary number of roots;
- revisit the same root after intervening roots;
- address a root derived from an earlier result, such as a bucket created by a
  preceding transaction block;
- retain exact object versions, manifests, stream ranges, descriptors, and other
  immutable values required for future execution or compensation;
- compensate committed work in reverse forward-block order when a later
  pre-pivot block fails.

Every compensatable forward operation MUST declare its compensation semantics in
the same request that records the forward operation. Anvil MUST NOT infer a
compensation policy or default to no compensation. A caller may explicitly choose
an Anvil-provided inverse, a sealed explicit compensation program, compensation
coverage by another operation, or an irreversible/pivot policy where permitted.
Anvil-provided inverse operations are implementations of a caller-selected
business policy; they are not policies selected by Anvil.

For a versioned object write, a caller may explicitly select
`OBJECT_PUT_RESTORE_PREVIOUS_HEAD_V1`. The forward transaction captures the exact
pre-saga head and the exact head produced by the saga. Compensation uses a new
single-root transaction to make the captured previous version current again,
conditional on the current head still being the exact saga-produced head.

A saga never claims cross-root ACID atomicity. Intermediate committed effects may
be visible. The saga guarantee is durable ordered execution, durable uncertain-
outcome reconciliation, and eventual completion, compensation, or an explicit
blocked state that preserves all evidence and reference holds required for
operator resolution.

## 2. Relationship to ANVIL-0007 and precedence

`ANVIL-0007: CoreStore Unified Storage Manifest` is incorporated into this RFC by
reference in full. An implementation claiming ANVIL-0008 conformance MUST also
conform to ANVIL-0007 except where this RFC expressly adds, replaces, or narrows
a requirement.

This RFC subsumes ANVIL-0007 for the combined CoreStore and saga architecture.
Where the two RFCs conflict, this RFC takes precedence. Where this RFC is silent,
ANVIL-0007 remains normative.

In particular, this RFC:

- preserves ANVIL-0007's rule that an explicit Anvil transaction is scoped to
  exactly one `RootAnchorKey`;
- preserves the prohibition on cross-root ACID claims and on rolling back a
  committed transaction;
- replaces the statement that multi-root sagas are only application-level with a
  native durable Anvil saga engine;
- extends the CoreMeta column-family and table registries;
- extends the root-kind and writer-family registries;
- extends `WriteOptions`, write response states, native services, error codes,
  metrics, traces, recovery rules, and conformance tests;
- adds owner-addressable reference edges required to prevent saga-dependent data
  from being reclaimed before a saga reaches a safe terminal state.

ANVIL-0008 does not copy ANVIL-0007's storage, root publication, transaction,
manifest, byte-pipeline, authz, or failure-recovery contracts. Those contracts are
normatively reused.

## 3. Goals

- Provide one durable saga state machine for each multi-root business operation.
- Allow an arbitrary ordered number of single-root transaction blocks without a
  protocol-level limit on the number of distinct roots.
- Support repeated visits to the same root without coalescing or reordering
  business-significant transaction boundaries.
- Construct saga plans incrementally using native Anvil mutation APIs and
  streaming payload paths rather than one monolithic step payload.
- Make it impossible in typed SDKs, and invalid in the raw protocol, to record a
  compensatable operation without an explicit compensation choice.
- Reuse ordinary ANVIL-0007 transactions for every forward and compensation
  block.
- Ensure no target-root transaction remains open while another root is addressed.
- Reconcile ambiguous commit outcomes before deciding whether compensation is
  required.
- Allow any eligible node to claim one runnable block or cleanup transition,
  without assigning the entire saga to one node.
- Keep exact object versions and every other compensation dependency reachable
  until execution, compensation, and cleanup no longer need them.
- Release all saga-owned references for every terminal outcome.
- Preserve deterministic encoding, typed operation dispatch, idempotency,
  authz, observability, and CoreStore's no-sidecar-storage architecture.

## 4. Non-goals

- Provide distributed ACID, two-phase commit, or atomic cross-root visibility.
- Keep target-root transactions open during saga construction or between saga
  transaction blocks.
- Guarantee exactly-once network delivery.
- Select a compensation policy on behalf of the caller.
- Allow arbitrary executable code, scripts, dynamic handler names, arbitrary HTTP
  callbacks, or user-supplied function bodies as saga operations.
- Silently split a saga transaction block when an operation resolves to a
  different root.
- Silently merge non-adjacent transaction blocks that happen to resolve to the
  same root.
- Permit compensation to overwrite unrelated concurrent state without an
  explicit, separately authorised force policy.
- Assign permanent saga ownership to a node or require one task to execute the
  entire saga.
- Define external-system connector semantics. A future RFC may add registered,
  enum-addressed connectors with deterministic request, idempotency, and
  reconciliation contracts.
- Define concurrent DAG execution in version 1. ANVIL-0008 sagas are ordered
  programs; later RFCs may add explicitly declared parallel groups.

## 5. Normative language and inherited canonical profile

The words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, and `MAY` are
normative.

All canonical encoding, deterministic protobuf, hashing, signing, CoreMeta,
manifest, locator, root publication, transaction, idempotency, and error-envelope
rules are inherited from ANVIL-0007.

ANVIL-0008 adds these hash domains:

```text
anvil.saga.partition.v1
anvil.saga.plan_content.v1
anvil.saga.plan.v1
anvil.saga.block.v1
anvil.saga.operation.v1
anvil.saga.compensation_program.v1
anvil.saga.compensation_spec.v1
anvil.saga.invocation.v1
anvil.saga.receipt.v1
anvil.saga.reference_hold.v1
anvil.saga.event.v1
anvil.saga.authorization.v1
anvil.saga.runnable.v1
```

Exact hash inputs are defined in section 27.

## 6. Terminology

```text
Saga
  One durable ordered business workflow whose state is authoritative in one saga
  root. A saga may address any number of participant roots.

Saga draft
  The mutable, durable construction state before ApplySaga freezes the plan.
  Target-root transactions do not exist during draft construction.

Saga transaction block
  An ordered group of native Anvil mutations that must execute together as one
  ordinary single-root transaction. A block is the unit of execution claiming.

Forward block
  A saga transaction block executed in the caller-declared forward order.

Compensation program
  The caller-selected or Anvil-generated ordered set of compensation transaction
  blocks for one forward operation or forward block.

Recorded operation
  A canonical native Anvil mutation request durably attached to a draft block.
  It has not yet changed its target root.

Automatic compensation
  An Anvil implementation of an inverse explicitly selected by the caller. The
  word automatic describes execution and operand capture, not policy selection.

Explicit compensation
  A sealed caller-constructed compensation program referenced by the forward
  operation when that operation is recorded.

Covered operation
  A forward operation whose effects are explicitly declared to be reversed by
  another operation's compensation, such as deleting a saga-created bucket that
  contains saga-created stream records.

Pivot block
  The explicit point of no return. After it commits, earlier blocks are not
  compensated; remaining blocks are forward-recovery-only.

Typed saga reference
  A schema-defined reference to a result that will be produced by an earlier
  operation, such as a created bucket, committed object version, stream range, or
  fence claim. It is not an expression language.

Reference hold
  An owner-addressable strong reference that prevents an immutable value or
  descriptor required by an unfinished saga from becoming garbage-collectable.

Execution task
  A transient task on any eligible Anvil node that claims and processes one
  runnable block, reconciliation transition, or reference-cleanup unit.

Claim fence
  A monotonically increasing token for one saga execution unit. A stale task may
  not advance saga state even if it continues running after losing its lease.

Outcome unknown
  A state in which Anvil sent an operation or commit request but lacks
  authoritative evidence of whether the participant transaction committed.
```

## 7. Required invariants

Implementations MUST preserve all of the following invariants.

### 7.1 Single-root execution boundary

Every forward and compensation transaction block resolves to exactly one
`RootAnchorKey`. All native mutations recorded in that block MUST resolve to the
same root selector. A block whose operation resolves to another root is invalid.

### 7.2 No target transaction during construction

`StartSaga`, `BeginSagaTransaction`, native mutation recording, compensation
program construction, and `SealSagaTransaction` MUST NOT open, stage, commit, or
hold open a target-root transaction. They only mutate the saga draft and acquire
required reference holds through separate bounded transactions.

### 7.3 Explicit block boundaries

Anvil MUST NOT automatically seal a block, begin another block, or split a block
because a later operation addresses another root. The caller must explicitly
seal the current block and begin a new one.

### 7.4 Arbitrary root sequence

The protocol MUST NOT impose a semantic maximum on the number of roots or
require roots to form a fixed set. Capacity quotas may limit total plan bytes,
operations, blocks, or active holds, but a saga may revisit a root and may contain
more distinct roots than any particular example or built-in operation family.

### 7.5 Mandatory compensation declaration

Every operation in a compensatable block MUST carry a present and valid
`SagaCompensationSpec` in the same request that records the operation.
`UNSPECIFIED`, absent, inferred, and implicit-none values are invalid.

### 7.6 Commit versus rollback

If a target transaction has not committed, its local failure is handled by
ordinary `RollbackTransaction`. If it has committed, it can be reversed only by
one or more new compensation transactions. A saga MUST NOT call rollback on a
committed transaction and describe that as compensation.

### 7.7 Durable intent before invocation

The saga root MUST contain a durable invocation-intent transition before a task
opens or resumes the target-root transaction for a block.

### 7.8 Outcome reconciliation

A timeout, lost response, process failure, or claim expiry does not prove target
failure. An outcome-unknown block MUST be reconciled through the target
transaction/idempotency evidence before it may be treated as committed or failed.
Compensation MUST NOT begin while a relevant forward commit remains unknown.

### 7.9 Stable logical invocation

Forward and compensation retries for the same block and phase MUST reuse the same
logical invocation id and target idempotency context. Attempt numbers are
observability and backoff data; they do not define new business invocations.

### 7.10 Immutable applied plan

`ApplySaga` freezes a canonical plan hash. No block, operation, target selector,
compensation choice, payload, typed reference, retry policy, or authorisation
envelope may change after that transition.

### 7.11 Safe compensation preconditions

An automatic inverse MUST target the exact state produced by the corresponding
forward operation. It MUST use conditional preconditions that prevent overwriting
an unrelated later update unless the plan explicitly selected a separately
authorised force policy.

### 7.12 Reference retention

Every immutable value or descriptor required for pending forward execution,
uncertain-outcome reconciliation, or any possible compensation path MUST remain
reachable through an active owner-addressable reference hold.

### 7.13 Terminal cleanup

A saga is not fully terminal while saga-owned reference holds remain active. All
terminal outcomes MUST pass through reference release or an explicit transfer of
ownership to another durable retention owner.

### 7.14 Per-unit execution claims

Claims apply to one runnable block, reconciliation transition, or cleanup unit.
There is no whole-saga node lease. Completion of one unit releases its claim; a
task on any eligible node may claim the next unit.

## 8. Architecture overview

```text
                    Native client / typed SDK
                              |
                              v
                     StartSaga -> DRAFT
                              |
              +---------------+----------------+
              |                                |
              v                                v
   BeginSagaTransaction             Build explicit compensation
   fixed root selector              programs when required
              |
              v
   Native mutation APIs with
   SagaOperationContext
   + mandatory compensation choice
              |
              v
   durable operation rows, payload locators,
   reference-hold intents/receipts
              |
              v
   SealSagaTransaction -> repeat arbitrary times
              |
                              v
                         ApplySaga
                validate + freeze plan hash
                              |
                              v
                saga root runnable transition
                              |
          any eligible node claims one block
                              |
                              v
       begin one real single-root Anvil transaction
       replay recorded native operations -> commit/rollback
                              |
                    +---------+---------+
                    |                   |
                    v                   v
             record receipt       reconcile unknown
                    |
         next forward block or compensation
                    |
                    v
             release saga references
                    |
                    v
                  TERMINAL
```

The saga state machine is authoritative in CoreStore. Execution tasks are
stateless beyond their current durable claim and may disappear after any RPC.

## 9. Saga root and partitioning

ANVIL-0008 adds the canonical root kind `sagas`:

```text
RootAnchorKey = realm_id "/sagas/" partition_id
```

The saga partition is selected by:

```text
saga_partition_hash =
  Hash(anvil.saga.partition.v1, canonical(realm_id), canonical(saga_id))

partition_id = low_u64(saga_partition_hash) modulo configured_saga_partition_count
```

All authoritative current-state rows for one saga MUST reside under one saga
root so that state version, block state, cursor advancement, event append, and
runnable-index updates can be committed atomically through the existing
single-root publication protocol.

Saga partition ownership is ordinary mesh/root ownership under ANVIL-0007. A
node executing a target block does not become owner of the saga. It routes saga
state transitions to the current saga-root owner and target transactions to the
current participant-root owner.

ANVIL-0008 adds writer family `saga_control`. Large immutable recorded request
bodies, compensation bodies, result bodies, event batches, and audit bundles use
this writer family and the unified byte pipeline. They MUST NOT be placed as
large RocksDB values or durable sidecar files.

## 10. Ordered saga program model

A saga plan is an ordered list of sealed forward transaction blocks:

```text
SagaPlan = [F0, F1, F2, ... Fn]
```

Each `Fi` contains:

```text
block id
block ordinal
block semantics
one fixed target-root selector
ordered recorded native operations
per-operation compensation specification
retry policy
canonical block hash
```

The same concrete root may occur at any number of ordinals:

```text
F0 -> Root A
F1 -> Core/control root
F2 -> Root X streams
F3 -> Root X objects
F4 -> Root A
F5 -> Root X objects
```

Anvil MUST execute this order exactly. It MUST NOT combine `F0` and `F4`, even
though both target Root A, because the intervening effects and failure points are
business-significant.

Version 1 permits only one forward block to be runnable for a saga at a time.
This does not imply one-node affinity: after each block is resolved, its claim
ends and the next block may be claimed by a task on another node.

## 11. Lifecycle and outcome state machines

Lifecycle and business outcome are separate fields. Lifecycle says what work may
still occur. Outcome says the business result selected once forward or reverse
processing has resolved.

```protobuf
enum SagaLifecycleState {
  SAGA_LIFECYCLE_STATE_UNSPECIFIED = 0;
  DRAFT = 1;
  DRAFT_INVALID = 2;
  SEALED = 3;
  EXECUTING_FORWARD = 4;
  COMPENSATING = 5;
  RELEASING_REFERENCES = 6;
  BLOCKED = 7;
  TERMINAL = 8;
}

enum SagaOutcome {
  SAGA_OUTCOME_UNSPECIFIED = 0;
  NONE = 1;
  SUCCEEDED = 2;
  COMPENSATED = 3;
  REJECTED = 4;
  ABORTED = 5;
  ABANDONED = 6;
}
```

Required high-level transitions:

```text
DRAFT
  -> DRAFT_INVALID                 deterministic structural construction violation
  -> SEALED                        ApplySaga validates and freezes plan
  -> RELEASING_REFERENCES          AbortSagaDraft or draft expiry

SEALED
  -> EXECUTING_FORWARD             first runnable block published
  -> BLOCKED                       scheduling or invariant failure requiring repair

EXECUTING_FORWARD
  -> EXECUTING_FORWARD             forward block committed; next block ready
  -> COMPENSATING                  pre-pivot failure or accepted cancellation
  -> RELEASING_REFERENCES          all forward blocks committed; outcome=SUCCEEDED
  -> BLOCKED                       unresolved invariant/operator condition

COMPENSATING
  -> COMPENSATING                  one compensation unit committed; next ready
  -> RELEASING_REFERENCES          all required compensation complete; outcome=COMPENSATED
  -> BLOCKED                       compensation conflict or exhausted retry policy

RELEASING_REFERENCES
  -> RELEASING_REFERENCES          one hold or hold batch released
  -> TERMINAL                      outstanding hold count reaches zero
  -> BLOCKED                       reference cleanup cannot safely complete

DRAFT_INVALID
  -> RELEASING_REFERENCES          AbortSagaDraft

BLOCKED
  -> EXECUTING_FORWARD             authorised resolution resumes forward processing
  -> COMPENSATING                  authorised resolution resumes reverse processing
  -> RELEASING_REFERENCES          authorised abandonment chooses cleanup
```

An applied saga MUST NOT expire into silent abandonment. Drafts may expire and be
aborted because they have no target effects. An applied saga with an exceeded
business deadline remains executable, compensatable, or blocked until an
authorised resolution is recorded.

### 11.1 Block states

```protobuf
enum SagaTransactionBlockState {
  SAGA_TRANSACTION_BLOCK_STATE_UNSPECIFIED = 0;
  DRAFT_OPEN = 1;
  DRAFT_SEALED = 2;
  READY = 3;
  CLAIMED = 4;
  INVOCATION_INTENT_RECORDED = 5;
  TARGET_TRANSACTION_OPEN = 6;
  COMMIT_OUTCOME_UNKNOWN = 7;
  COMMITTED = 8;
  FORWARD_FAILED = 9;
  COMPENSATION_READY = 10;
  COMPENSATION_CLAIMED = 11;
  COMPENSATION_OUTCOME_UNKNOWN = 12;
  COMPENSATED = 13;
  COMPENSATION_BLOCKED = 14;
  SKIPPED_COVERED = 15;
}
```

A block may be `COMMIT_OUTCOME_UNKNOWN` or
`COMPENSATION_OUTCOME_UNKNOWN` for an unbounded recovery interval. Reference
holds required to resolve it MUST remain active.

## 12. Incremental construction service

The native saga service is asynchronous and plan-oriented:

```protobuf
service SagaService {
  rpc StartSaga(StartSagaRequest) returns (StartSagaResponse);

  rpc BeginSagaTransaction(BeginSagaTransactionRequest)
      returns (BeginSagaTransactionResponse);

  rpc SealSagaTransaction(SealSagaTransactionRequest)
      returns (SealSagaTransactionResponse);

  rpc StartSagaCompensationProgram(StartSagaCompensationProgramRequest)
      returns (StartSagaCompensationProgramResponse);

  rpc BeginSagaCompensationTransaction(BeginSagaCompensationTransactionRequest)
      returns (BeginSagaCompensationTransactionResponse);

  rpc SealSagaCompensationTransaction(SealSagaCompensationTransactionRequest)
      returns (SealSagaCompensationTransactionResponse);

  rpc SealSagaCompensationProgram(SealSagaCompensationProgramRequest)
      returns (SealSagaCompensationProgramResponse);

  rpc ApplySaga(ApplySagaRequest) returns (ApplySagaResponse);
  rpc GetSaga(GetSagaRequest) returns (SagaStatus);
  rpc WatchSaga(WatchSagaRequest) returns (stream SagaEvent);
  rpc CancelSaga(CancelSagaRequest) returns (SagaStatus);
  rpc AbortSagaDraft(AbortSagaDraftRequest) returns (SagaStatus);
  rpc ResolveBlockedSaga(ResolveBlockedSagaRequest) returns (SagaStatus);
}
```

Ordinary forward and compensation operations are recorded through their existing
native Anvil services using the execution contexts in section 13. The saga
service does not introduce a monolithic `AddSteps` request containing all object,
stream, registry, authz, or PersonalDB payloads.

### 12.1 StartSaga

```protobuf
message StartSagaRequest {
  string idempotency_key = 1;
  string realm_id = 2;
  uint64 draft_ttl_ms = 3;
  string purpose = 4;
  SagaExecutionPolicy execution_policy = 5;
}

message StartSagaResponse {
  string request_id = 1;
  string saga_id = 2;
  uint64 draft_revision = 3;
  SagaLifecycleState lifecycle_state = 4; // DRAFT
  uint64 draft_expires_at_unix_nanos = 5;
}
```

`StartSaga` creates only saga-root state. It is idempotent by caller, realm, and
idempotency key. A repeated key with a different canonical request hash is
`IdempotencyConflict`.

### 12.2 BeginSagaTransaction

```protobuf
enum SagaBlockSemantics {
  SAGA_BLOCK_SEMANTICS_UNSPECIFIED = 0;
  COMPENSATABLE = 1;
  PIVOT = 2;
  RETRY_FORWARD_ONLY = 3;
}

message BeginSagaTransactionRequest {
  string saga_id = 1;
  uint64 expected_draft_revision = 2;
  string block_idempotency_key = 3;
  SagaTargetRoot target_root = 4;
  SagaBlockSemantics semantics = 5;
  SagaRetryPolicy retry_policy = 6;
}

message BeginSagaTransactionResponse {
  string request_id = 1;
  string saga_id = 2;
  string saga_transaction_id = 3;
  uint64 block_ordinal = 4;
  uint64 draft_revision = 5;
  SagaTransactionBlockState block_state = 6; // DRAFT_OPEN
}
```

A saga may have at most one open forward block at a time. The request fixes the
block's target-root selector and semantics. It does not begin a target-root
transaction.

### 12.3 SealSagaTransaction

```protobuf
message SealSagaTransactionRequest {
  string saga_id = 1;
  string saga_transaction_id = 2;
  uint64 expected_draft_revision = 3;
}

message SealSagaTransactionResponse {
  string request_id = 1;
  string saga_id = 2;
  string saga_transaction_id = 3;
  uint64 block_ordinal = 4;
  string block_hash = 5;
  uint64 draft_revision = 6;
  SagaTransactionBlockState block_state = 7; // DRAFT_SEALED
}
```

Sealing validates that:

- the block contains at least one operation;
- every operation resolves structurally to the block's root selector;
- every compensatable operation has a valid compensation specification;
- every referenced explicit compensation program is sealed;
- every `CoveredBy` relation is type-correct and potentially dominating;
- every draft-time reference hold is active;
- operation ids and idempotency keys are unique within their required scopes;
- pivot and retry-forward-only rules are satisfied.

The SDK may call this method `commit()` because it returns the caller from
`SagaTransaction` to `OngoingSaga`. The raw protocol uses `Seal` because no
target-root state has committed.

### 12.4 ApplySaga

```protobuf
message ApplySagaRequest {
  string saga_id = 1;
  uint64 expected_draft_revision = 2;
  string idempotency_key = 3;
}

message ApplySagaResponse {
  string request_id = 1;
  string saga_id = 2;
  SagaLifecycleState lifecycle_state = 3;
  string sealed_plan_hash = 4;
  uint64 saga_revision = 5;
  string idempotency_outcome = 6;
}
```

`ApplySaga` MUST:

1. reject a draft with an open forward or compensation transaction;
2. reject `DRAFT_INVALID`;
3. revalidate all block roots, operation schemas, typed references,
   compensation coverage, pivot ordering, payload locators, holds, quotas, and
   authorisation;
4. compute the immutable plan-content hash over the validated blocks,
   operations, compensation programs, typed references, payload hashes, and
   execution policy;
5. create the restricted execution authorisation envelope bound to that
   plan-content hash;
6. compute and persist the final sealed plan hash over the plan-content hash and
   authorisation-envelope hash;
7. transition the saga to `SEALED` and publish the first runnable unit in the
   same saga-root transaction;
8. return without waiting for the saga to reach a terminal state.

A repeated `ApplySaga` with the same idempotency key and plan hash returns the
original accepted result. A different plan hash is impossible because the first
successful apply freezes the draft.

### 12.5 Structural construction failures

A raw protocol caller can attempt combinations that a typed SDK prevents. The
server MUST reject the invalid call before persisting its operation. The
following deterministic invariant violations also transition the saga draft to
`DRAFT_INVALID` in the same saga-root transaction so the caller cannot continue
building an ambiguously interpreted plan:

```text
missing or unspecified compensation
operation kind/payload variant mismatch
operation target different from open block target
invalid or cyclic typed saga reference
reference to another saga
reference to an unsealed explicit compensation program
irreversible operation in a compensatable block
compensatable operation in a retry-forward-only block without explicit policy
second pivot block
forward block added after a pivot with COMPENSATABLE semantics
```

Transport failures, unavailable nodes, authz denial, stale draft revision, quota
backpressure, and an idempotent replay do not poison the draft because they do
not prove that the caller constructed an invalid program.

A `DRAFT_INVALID` saga may be inspected and aborted but not repaired in place.
This keeps the sealed plan hash unambiguous and makes typed SDK behavior match raw
protocol validation.

## 13. Native mutation API integration

ANVIL-0008 replaces the single optional transaction field in `WriteOptions` with
a wire-compatible oneof that retains tag 6 for `transaction_id` and adds saga
contexts:

```protobuf
message WriteOptions {
  string idempotency_key = 1;
  ConsistencyMode consistency = 2;
  bool wait_for_finalization = 3;
  repeated WritePrecondition preconditions = 4;
  repeated BoundaryValue boundary_values = 5;

  oneof execution {
    string transaction_id = 6;
    SagaOperationContext saga_operation = 7;
    SagaCompensationOperationContext saga_compensation_operation = 8;
  }
}
```

Exactly one execution context may be present. Absence retains ANVIL-0007 implicit
transaction semantics.

### 13.1 Forward recording context

```protobuf
message SagaOperationContext {
  string saga_id = 1;
  string saga_transaction_id = 2;
  string operation_id = 3;
  uint64 expected_draft_revision = 4;
  SagaCompensationSpec compensation = 5; // REQUIRED
}
```

When `saga_operation` is present, the native mutation API MUST:

1. authenticate and authorise the caller for the native operation and saga draft;
2. perform static schema, boundary, content-type, size, and request validation;
3. resolve the operation's structural target selector and verify that it matches
   the open block;
4. validate the operation-specific compensation choice;
5. land and durably store any request body required for later execution;
6. acquire any reference holds that are knowable during construction;
7. persist the canonical recorded operation and compensation specification in
   the saga root;
8. return a recorded-operation receipt without changing the target root.

Dynamic target preconditions, including object absence, version equality, fence
ownership, current authz revision, and uniqueness claims, MUST be evaluated at
block execution time in their recorded order. Static validation MUST NOT hoist a
later runtime failure ahead of earlier saga blocks.

### 13.2 Explicit compensation recording context

```protobuf
message SagaCompensationOperationContext {
  string saga_id = 1;
  string compensation_program_id = 2;
  string saga_compensation_transaction_id = 3;
  string compensation_operation_id = 4;
  uint64 expected_draft_revision = 5;
}
```

A compensation operation does not carry a nested compensation policy. If a
multi-block compensation program partially commits and a later compensation
block fails, Anvil retries or resolves the remaining compensation program; it
does not recursively compensate compensation.

### 13.3 Write response extensions

```protobuf
enum WriteState {
  WRITE_STATE_UNSPECIFIED = 0;
  WRITE_STATE_COMMITTED = 1;
  WRITE_STATE_FINALISED = 2;
  WRITE_STATE_FINALISATION_FAILED = 3;
  WRITE_STATE_STAGED = 4;
  WRITE_STATE_SAGA_OPERATION_RECORDED = 5;
  WRITE_STATE_SAGA_COMPENSATION_RECORDED = 6;
}

message SagaRecordedOperationReceipt {
  string saga_id = 1;
  string saga_transaction_id = 2;
  string operation_id = 3;
  uint32 operation_ordinal = 4;
  uint64 draft_revision = 5;
  string operation_hash = 6;
}

message SagaRecordedCompensationOperationReceipt {
  string saga_id = 1;
  string compensation_program_id = 2;
  string saga_compensation_transaction_id = 3;
  string compensation_operation_id = 4;
  uint32 compensation_operation_ordinal = 5;
  uint64 draft_revision = 6;
  string operation_hash = 7;
}

message WriteResponseSagaExtension {
  oneof receipt {
    SagaRecordedOperationReceipt forward_operation = 1;
    SagaRecordedCompensationOperationReceipt compensation_operation = 2;
  }
}
```

The existing `WriteResponse` is extended with
`optional WriteResponseSagaExtension saga = 9;`. A saga
recording response MUST NOT include a target root generation or target
transaction manifest because no target mutation has occurred.

## 14. Typed SDK construction contract

Typed SDKs MUST model construction as a stateful builder whose types prevent
cross-root transaction misuse:

```rust
let mut saga: OngoingSaga = client.start_saga(options).await?;

let mut tx: SagaTransaction = saga
    .begin_transaction(bucket_a.objects_root())
    .await?;

tx.put_object(
    put_a_request,
    ObjectPutCompensation::RestorePreviousHeadV1,
).await?;

saga = tx.commit().await?; // seals the saga block; no target commit

let applied: AppliedSaga = saga.apply().await?;
```

The following API shape is forbidden because it admits a persisted operation
before compensation semantics are known:

```rust
// Forbidden API shape.
let op = tx.put_object(request).await?;
op.compensate_automatically(...).await?;
```

The compensation argument MUST be part of the forward operation call:

```rust
pub async fn put_object(
    &mut self,
    request: PutObjectRequest,
    compensation: ObjectPutCompensation,
) -> Result<SagaObjectWriteRef>;
```

Operation-specific compensation types SHOULD be used in public SDKs so invalid
cross-family combinations do not compile. The wire format uses the typed enums
and oneofs in sections 15 and 16.

A `SagaTransaction` value MUST NOT expose `begin_transaction` for another root.
Only sealing or aborting it returns an `OngoingSaga` value that can open the next
block.

## 15. Typed operation registry

Saga operation dispatch MUST use closed protobuf enums and typed request variants.
Dynamic handler strings are forbidden.

```protobuf
enum SagaOperationKind {
  SAGA_OPERATION_KIND_UNSPECIFIED = 0;

  OBJECT_PUT_V1 = 1;
  OBJECT_DELETE_V1 = 2;
  OBJECT_PUT_LINK_V1 = 3;
  OBJECT_RESTORE_HEAD_V1 = 4;          // compensation-capable native operation
  OBJECT_PUT_IF_ABSENT_V1 = 5;

  BUCKET_CREATE_V1 = 20;
  BUCKET_DELETE_V1 = 21;

  STREAM_APPEND_V1 = 40;
  STREAM_SEAL_SEGMENT_V1 = 41;
  STREAM_RETRACT_RANGE_V1 = 42;        // logical retraction/tombstone operation

  LEASE_FENCE_CLAIM_V1 = 60;
  LEASE_FENCE_RELEASE_V1 = 61;

  AUTHZ_PUT_SCHEMA_V1 = 80;
  AUTHZ_BIND_SCHEMA_V1 = 81;
  AUTHZ_WRITE_TUPLES_V1 = 82;

  REGISTRY_PUT_BLOB_V1 = 100;
  REGISTRY_PUT_VERSION_V1 = 101;
  REGISTRY_PUT_REF_V1 = 102;

  PERSONALDB_APPLY_CHANGESET_V1 = 120;

  INDEX_PUT_DEFINITION_V1 = 140;

  BOUNDARY_PUT_SCHEMA_V1 = 160;
  BOUNDARY_START_MIGRATION_V1 = 161;

  MESH_PUT_REGION_V1 = 180;
  MESH_PUT_CELL_V1 = 181;
  MESH_PUT_NODE_V1 = 182;
  MESH_DRAIN_NODE_V1 = 183;
  MESH_DRAIN_CELL_V1 = 184;
  MESH_MOVE_BUCKET_V1 = 185;
}
```

The registry MUST be extended whenever a native mutating API is added. A release
conformance test MUST compare the native service descriptor against the saga
registry and fail if a mutating method has no declared saga behavior. An
operation may be declared `not_saga_recordable` only by updating this RFC or a
later superseding RFC with a safety rationale.

The canonical recorded row uses a typed oneof. Large bodies are replaced by
CoreStore locators before hashing and storage. The type names shown below are
canonical recorded forms: the recorder removes `WriteOptions.execution`, removes
transport-only fields, and replaces large inline/stream bodies with immutable
body locators, hashes, and lengths before deterministic re-encoding. An
implementation MUST NOT hash the live transport message containing its own saga
context.

```protobuf
message RecordedSagaOperation {
  string saga_id = 1;
  string block_id = 2;
  string operation_id = 3;
  uint32 operation_ordinal = 4;
  SagaOperationKind kind = 5;

  oneof request {
    PutObjectRequest object_put_v1 = 20;
    DeleteObjectRequest object_delete_v1 = 21;
    PutLinkRequest object_put_link_v1 = 22;
    RecordedRestoreObjectHeadRequest object_restore_head_v1 = 23;
    RecordedCreateBucketRequest bucket_create_v1 = 40;
    RecordedDeleteBucketRequest bucket_delete_v1 = 41;
    AppendRecordRequest stream_append_v1 = 60;
    SealSegmentRequest stream_seal_segment_v1 = 61;
    RecordedRetractStreamRangeRequest stream_retract_range_v1 = 62;
    RecordedLeaseFenceClaimRequest lease_fence_claim_v1 = 80;
    RecordedLeaseFenceReleaseRequest lease_fence_release_v1 = 81;
    WriteTuplesRequest authz_write_tuples_v1 = 100;
    PutPackageVersionRequest registry_put_version_v1 = 120;
    PutRegistryRefRequest registry_put_ref_v1 = 121;
    ApplyChangesetRequest personaldb_apply_changeset_v1 = 140;
    PutIndexDefinitionRequest index_put_definition_v1 = 160;
    PutBoundarySchemaRequest boundary_put_schema_v1 = 180;
    StartBoundaryMigrationRequest boundary_start_migration_v1 = 181;
  }

  SagaCompensationSpec compensation = 200;
}
```

A native request type name shown without a `Recorded` prefix in this oneof is
shorthand for its fixed canonical recorded form: transport context is removed and
large bodies are replaced by immutable locators. The illustrative oneof above is
not permission to omit other enum-declared native request variants. The
implementation's schema registry MUST contain one exact typed recorded-request
variant for every supported enum value.

A receiver MUST reject:

```text
unknown operation enum value
kind/request-oneof mismatch
unknown protobuf fields
non-deterministic re-encoding
request body locator/hash mismatch
operation method inconsistent with the native RPC used to record it
unsupported operation schema version
```

## 16. Mandatory compensation model

```protobuf
message SagaCompensationSpec {
  oneof policy {
    AutomaticSagaCompensation automatic = 1;
    ExplicitSagaCompensation explicit = 2;
    CoveredBySagaCompensation covered_by = 3;
    IrreversibleSagaOperation irreversible = 4;
  }
}
```

An absent oneof is `SagaCompensationRequired`. There is no default branch.

### 16.1 Automatic compensation

```protobuf
enum SagaAutomaticCompensationKind {
  SAGA_AUTOMATIC_COMPENSATION_KIND_UNSPECIFIED = 0;

  OBJECT_PUT_RESTORE_PREVIOUS_HEAD_V1 = 1;
  OBJECT_DELETE_RESTORE_PREVIOUS_HEAD_V1 = 2;
  OBJECT_LINK_RESTORE_PREVIOUS_TARGET_V1 = 3;
  OBJECT_PUT_IF_ABSENT_RESTORE_ABSENCE_V1 = 4;

  BUCKET_CREATE_DELETE_CREATED_BUCKET_V1 = 20;

  STREAM_APPEND_RETRACT_COMMITTED_RANGE_V1 = 40;

  LEASE_FENCE_CLAIM_RELEASE_EXACT_FENCE_V1 = 60;

  AUTHZ_WRITE_TUPLES_APPLY_INVERSE_DELTA_V1 = 80;

  REGISTRY_PUT_REF_RESTORE_PREVIOUS_REF_V1 = 100;
  INDEX_PUT_DEFINITION_RESTORE_PREVIOUS_DEFINITION_V1 = 120;
  BOUNDARY_PUT_SCHEMA_RESTORE_PREVIOUS_GENERATION_V1 = 140;
}

message AutomaticSagaCompensation {
  SagaAutomaticCompensationKind kind = 1;
}
```

The operation registry declares which automatic compensation kinds are legal for
each forward kind. `UNSPECIFIED`, a family mismatch, or an inverse whose
preconditions cannot be supported is invalid.

The caller chooses the enum. Anvil captures the exact operands needed to execute
that selected inverse during the forward transaction.

### 16.2 Explicit compensation

```protobuf
message ExplicitSagaCompensation {
  string compensation_program_id = 1;
  string expected_program_hash = 2;
}
```

The referenced program MUST be sealed before the forward operation is recorded.
The program may contain any number of ordered single-root compensation blocks.
It may use typed references to the eventual forward receipt of the operation it
compensates.

SDKs MAY hide the preallocation and program construction through a closure:

```rust
let operation_id = tx.allocate_operation_id();
let compensation = saga.build_compensation_for(operation_id, |program, result| {
    let mut c = program.begin_transaction(result.created_bucket().objects_root())?;
    c.delete_object(result.created_object())?;
    c.commit()
})?;

tx.custom_mutation(request, compensation).await?;
```

The wire protocol still records a sealed program reference in the same forward
operation request.

### 16.3 CoveredBy compensation

```protobuf
message CoveredBySagaCompensation {
  string covering_operation_id = 1;
  SagaCoverageKind coverage_kind = 2;
}

enum SagaCoverageKind {
  SAGA_COVERAGE_KIND_UNSPECIFIED = 0;
  CREATED_CONTAINER_DELETION_V1 = 1;
  RESOURCE_GROUP_RESTORE_V1 = 2;
  EXPLICIT_DOMINATING_PROGRAM_V1 = 3;
}
```

Coverage is valid only when the operation registry's coverage validator proves
all of the following:

- the covering operation belongs to the same saga;
- it occurs earlier in forward order, so its compensation occurs later in
  reverse order;
- its compensation is mandatory and cannot be skipped independently;
- its resource scope contains every effect of the covered operation;
- reference holds needed by the covered effect remain active until the covering
  compensation completes;
- compensation after the covering operation would not leave the covered effect
  visible.

For example, stream appends into a bucket created solely by the saga may be
covered by the conditional deletion of that exact saga-created bucket. Coverage
is not valid if external writes may make bucket deletion unsafe unless the plan
uses a different explicit compensation for those appends.

### 16.4 Irreversible operation

```protobuf
message IrreversibleSagaOperation {
  SagaIrreversiblePolicy policy = 1;
  string reason = 2;
}

enum SagaIrreversiblePolicy {
  SAGA_IRREVERSIBLE_POLICY_UNSPECIFIED = 0;
  PIVOT_REQUIRED_V1 = 1;
  RETRY_FORWARD_AFTER_PIVOT_V1 = 2;
}
```

An irreversible operation is an explicit policy selection, not an omitted
compensation. It is legal only in a `PIVOT` or `RETRY_FORWARD_ONLY` block as
specified in section 20.

### 16.5 Reverse order

Within a committed forward block, automatically generated inverse operations run
in reverse operation order unless an operation-specific contract defines a
stricter sequence. Forward blocks are compensated in descending block ordinal.
Explicit compensation programs run their own blocks in declared order when their
owning forward operation is reached during reverse traversal.

A block that fails before local commit is rolled back and is not compensated.
Only previously committed forward blocks participate in reverse traversal.

## 17. Typed result references and derived roots

ANVIL-0008 does not define a string binding or expression language. Dependencies
on future results use closed typed references.

```protobuf
message SagaBucketRef {
  string saga_id = 1;
  string producing_operation_id = 2;
}

message SagaObjectVersionRef {
  string saga_id = 1;
  string producing_operation_id = 2;
  SagaObjectVersionSelection selection = 3;
}

enum SagaObjectVersionSelection {
  SAGA_OBJECT_VERSION_SELECTION_UNSPECIFIED = 0;
  BEFORE_OPERATION = 1;
  AFTER_OPERATION = 2;
}

message SagaStreamRangeRef {
  string saga_id = 1;
  string producing_operation_id = 2;
}

message SagaFenceClaimRef {
  string saga_id = 1;
  string producing_operation_id = 2;
}

enum SagaRootKind {
  SAGA_ROOT_KIND_UNSPECIFIED = 0;
  OBJECTS = 1;
  STREAMS = 2;
  INDEXES = 3;
  AUTHZ = 4;
  PERSONALDB = 5;
  REGISTRY = 6;
  MESH = 7;
  CORE_CONTROL = 8;
  SAGAS = 9;
}

message SagaTargetRoot {
  oneof selector {
    TransactionScope concrete_root = 1;
    SagaCreatedBucketRoot created_bucket_root = 2;
  }
}

message SagaCreatedBucketRoot {
  SagaBucketRef bucket = 1;
  SagaRootKind root_kind = 2;
}
```

A typed reference is resolved only from a verified committed invocation receipt.
The plan validator MUST verify:

```text
producer and consumer belong to the same saga
producer operation precedes consumer block
producer kind can emit the referenced result type
reference selection is legal for that operation kind
no dependency cycle exists
root-kind use is supported for the produced resource
```

A derived root is resolved immediately before its block becomes runnable. If the
producer never commits, the dependent block never executes. If the producer is
later compensated, dependent blocks have already been compensated first because
reverse traversal is ordered.

The root selector identifies a canonical root, not a node. Normal mesh routing
resolves the current owner and fence at execution time.

## 18. Versioned object compensation

Versioned object operations are a first-class automatic compensation case.

### 18.1 Object put

For:

```rust
put_object(
    request,
    ObjectPutCompensation::RestorePreviousHeadV1,
)
```

the target-root forward transaction MUST atomically capture and commit:

```text
object identity
previous head kind/version/manifest, or explicit absence
new version id and manifest
new head value produced by the saga
forward mutation id
saga id, block id, operation id, invocation id
reference holds for every captured immutable version/manifest needed later
```

The automatic inverse is equivalent to:

```text
OBJECT_RESTORE_HEAD_V1
  object = exact object identity
  expected_current_head = exact saga-produced head
  restored_head = exact captured previous head or absence
```

Compensation creates a new root generation whose current head points to the
captured previous immutable version. It does not mutate historical versions and
does not pretend that the original commit never happened.

If the previous head was absent, restoring the previous head means restoring
absence according to the bucket's delete-marker and versioning semantics.

### 18.2 Concurrent update protection

Suppose the saga changed `V17 -> V18`, but an unrelated writer later changed the
head to `V19`. Automatic compensation sees:

```text
expected_current_head = V18
actual_current_head = V19
```

It MUST fail with `SagaCompensationConflict` and move the saga to `BLOCKED` while
retaining all required holds. It MUST NOT overwrite `V19` with `V17`.

A force-restore operation is not part of the initial automatic compensation
registry. A future RFC may add one with a distinct enum value, explicit elevated
authorisation, and audit requirements.

### 18.3 Repeated writes to one object

Reverse order naturally unwinds repeated saga writes:

```text
before saga: V17
block 0:     V17 -> V18
block 4:     V18 -> V19

compensation:
  block 4:   V19 -> V18
  block 0:   V18 -> V17
```

The saga MUST retain `V17`, `V18`, and `V19` as required until their relevant
compensation and reconciliation paths are closed.

### 18.4 Delete and link operations

An object delete with `OBJECT_DELETE_RESTORE_PREVIOUS_HEAD_V1` captures the head
that existed before the delete marker or head removal. A link mutation with
`OBJECT_LINK_RESTORE_PREVIOUS_TARGET_V1` captures the previous link target or
absence. Their compensation uses the same exact-head conditional safety rule.

## 19. Saga-owned reference holds and garbage collection

A saga may outlive ordinary object-head reachability. A version replaced or
deleted by a forward block may otherwise become unreachable before a later
failure requires it for compensation. Aggregate reference counts alone are
insufficient because cleanup must identify which saga owns each increment.

ANVIL-0008 therefore adds owner-addressable reference edges.

### 19.1 Held values

A saga MUST hold every exact value required by any still-possible path,
including where applicable:

- streamed request payloads durably recorded during construction;
- existing object versions and manifests referenced by future operations;
- object heads and versions captured before a forward mutation;
- versions and manifests created by a forward mutation when a later saga block
  may replace their head;
- stream segment/range identities required for retraction or reconciliation;
- bucket, registry, index, authz, lease, fence, and PersonalDB descriptors needed
  by compensation;
- explicit compensation payloads;
- typed-result receipts used to resolve later roots or requests;
- transaction and manifest evidence needed to reconcile an unknown outcome.

A reference to an object key or current head is not sufficient when compensation
requires a particular immutable version.

### 19.2 Authoritative target-root edge

The authoritative GC edge is stored in the root that owns the referenced
resource or its CoreStore reachability metadata:

```protobuf
enum ReferenceOwnerKind {
  REFERENCE_OWNER_KIND_UNSPECIFIED = 0;
  SAGA = 1;
}

enum SagaReferenceReason {
  SAGA_REFERENCE_REASON_UNSPECIFIED = 0;
  RECORDED_INPUT = 1;
  FORWARD_DEPENDENCY = 2;
  BEFORE_STATE = 3;
  AFTER_STATE = 4;
  COMPENSATION_DEPENDENCY = 5;
  OUTCOME_RECONCILIATION = 6;
  EXPLICIT_COMPENSATION_PAYLOAD = 7;
}

message OwnedReferenceEdgeRow {
  CoreMetaRowCommon common = 1;
  ReferenceOwnerKind owner_kind = 2;
  string owner_id = 3;               // saga_id for owner_kind=SAGA
  string hold_id = 4;
  string ref_kind = 5;
  string ref_id = 6;
  CoreMetaLocator locator = 7;
  SagaReferenceReason reason = 8;
  bool active = 9;
  uint64 acquired_root_generation = 10;
  uint64 released_root_generation = 11;
  string hold_hash = 12;
}
```

`OwnedReferenceEdgeRow` is added to `cf_refcounts` as specified in section 26.
Its creation/deactivation and the corresponding aggregate `RefCountRow` change
MUST be committed atomically in the referenced resource's root transaction.

GC MUST treat every active owned edge as reachability. It MUST NOT reclaim the
referenced value merely because no object head currently points to it.

### 19.3 Saga-root hold obligation

The saga root stores a mirrored obligation state so recovery can complete or
release an edge without scanning every participant root:

```protobuf
enum SagaReferenceHoldState {
  SAGA_REFERENCE_HOLD_STATE_UNSPECIFIED = 0;
  ACQUIRE_INTENT = 1;
  HELD = 2;
  RELEASE_INTENT = 3;
  RELEASED = 4;
  HOLD_BLOCKED = 5;
  RELEASE_BLOCKED = 6;
}
```

The saga-side row stores the target root selector, resolved root when known,
`hold_id`, ref identity/hash, edge key, state, and evidence receipt. Large
locators are present only while the hold remains active.

### 19.4 Draft-time hold protocol

When a recorded request references an already existing immutable value:

```text
1. Commit ACQUIRE_INTENT in the saga root.
2. In one transaction on the referenced resource root:
     create OwnedReferenceEdgeRow(saga_id, hold_id), idempotently;
     increment/update RefCountRow;
     commit.
3. Commit HELD and the target transaction receipt in the saga root.
4. Commit the recorded operation, or atomically record it with step 3.
5. Return SAGA_OPERATION_RECORDED only after the operation and HELD state exist.
```

If the process fails after step 2, recovery finds `ACQUIRE_INTENT`, checks the
owner-addressable edge, and either completes the operation record or releases the
orphan hold. Repeating the same `hold_id` with the same hash is idempotent; a
mismatched ref is `SagaReferenceHoldConflict`.

### 19.5 Execution-time capture protocol

When a forward operation changes reachability, pre-state capture and its holds
MUST be part of the same target-root transaction as the mutation:

```text
read exact pre-state under the transaction's preconditions
create owner-addressable holds for required pre-state/post-state values
update aggregate refcounts
apply the forward mutation
commit all rows as one target-root transaction
```

This ordering prevents a GC window between removing the old head and retaining
its version. The target transaction receipt carries the generated hold ids. If
the saga-root receipt update is lost, reconciliation recovers the committed
transaction and imports those hold receipts.

### 19.6 Release protocol

For each active hold:

```text
1. Commit RELEASE_INTENT in the saga root.
2. In one transaction on the referenced resource root:
     mark/delete the exact OwnedReferenceEdgeRow idempotently;
     decrement/update RefCountRow without underflow;
     commit.
3. Commit RELEASED in the saga root and remove strong locator fields from the
   retained audit row.
```

Reference release units MAY be grouped by target root, but a group must remain a
single-root transaction. Any eligible node may claim a release unit.

A saga remains `RELEASING_REFERENCES` until every hold is `RELEASED` or ownership
has been durably transferred to another explicitly named retention owner. A
failed release moves the saga to `BLOCKED` with reason `SagaCleanupBlocked`; it
does not falsely report terminal success.

### 19.7 Terminal audit retention

A terminal saga may retain:

```text
saga id and plan hash
operation and block hashes
transaction ids
root generations
manifest/receipt hashes
error codes
reference hold ids and released hashes
state transition event hashes
```

It MUST NOT retain strong locators or active owned edges solely for historical
convenience after cleanup. Otherwise completed sagas become permanent hidden GC
roots.

## 20. Compensatable, pivot, and forward-recovery-only blocks

A saga with no pivot treats every forward block as compensatable. If a later
block fails after its retry policy, committed blocks are compensated in reverse
order.

A saga MAY declare one pivot block:

```text
F0 COMPENSATABLE
F1 COMPENSATABLE
F2 PIVOT
F3 RETRY_FORWARD_ONLY
F4 RETRY_FORWARD_ONLY
```

Rules:

- At most one pivot is allowed.
- Every block before the pivot MUST be `COMPENSATABLE`.
- Every block after the pivot MUST be `RETRY_FORWARD_ONLY`.
- A pivot operation MUST explicitly use an irreversible policy or another policy
  valid for the pivot operation kind.
- Before the pivot transaction commits, its local failure rolls back and earlier
  blocks may be compensated.
- Once the pivot transaction commits, earlier blocks MUST NOT be compensated.
- A post-pivot failure is retried forward according to policy and then moves to
  `BLOCKED` if it cannot progress. It does not start reverse execution.

`CancelSaga` behaves as follows:

```text
DRAFT
  -> equivalent to AbortSagaDraft after validating caller permission.

EXECUTING_FORWARD before pivot commit
  -> set cancel_requested;
  -> resolve any current outcome-unknown block;
  -> if the current block did not commit, roll it back;
  -> begin compensation of committed preceding blocks.

At or after pivot commit
  -> return SagaPastPivot;
  -> do not stop required forward recovery.

COMPENSATING
  -> idempotently preserve compensation; cancellation cannot cancel cleanup.
```

## 21. Forward execution protocol

Each sealed forward block is applied by one transient execution task. The task
MUST NOT retain responsibility for the next block after it completes the current
unit.

### 21.1 Stable invocation identity

```text
forward_invocation_id =
  Hash(anvil.saga.invocation.v1,
       saga_id, sealed_plan_hash, block_id, "forward")

compensation_invocation_id =
  Hash(anvil.saga.invocation.v1,
       saga_id, sealed_plan_hash, compensation_block_id, "compensation")
```

Every retry uses the same invocation id. Target transaction begin and every
replayed operation use idempotency keys derived from that invocation and the
recorded operation id.

### 21.2 Forward sequence

```mermaid
sequenceDiagram
    participant T as Execution task on any node
    participant S as Saga root
    participant P as Participant root transaction service

    T->>S: claim READY block with expected saga revision
    S-->>T: claim fence + invocation id
    T->>S: persist INVOCATION_INTENT_RECORDED
    T->>P: BeginTransaction(target root, stable idempotency key)
    P-->>T: transaction id
    T->>P: replay recorded operations in order
    T->>P: CommitTransaction
    alt committed response
      P-->>T: committed root generation + transaction evidence
      T->>S: persist verified receipt; mark block COMMITTED; publish next runnable unit
    else response lost or timeout
      T->>S: mark COMMIT_OUTCOME_UNKNOWN
    else deterministic pre-commit failure
      T->>P: RollbackTransaction
      T->>S: mark FORWARD_FAILED; schedule retry or compensation
    end
```

Normative sequence:

1. Claim the exact `READY` block and phase using expected saga revision.
2. Commit `INVOCATION_INTENT_RECORDED` before opening the target transaction.
3. Resolve the block's typed target root from committed prior receipts.
4. Call idempotent `BeginTransaction` for that root.
5. Replay every recorded native operation in ordinal order using the target
   transaction id.
6. Capture required automatic-compensation pre-state and holds in the target
   transaction.
7. Commit the target transaction using ANVIL-0007.
8. Verify the returned transaction id, root key, root generation, transaction
   manifest, mutation ids, response hashes, and hold receipts.
9. Commit the invocation receipt and cursor advancement in the saga root.
10. End the claim. The next unit may be claimed by a task on any node.

One task executes the whole block because its operations share one real local
transaction. No task carries a target transaction across claims, roots, or block
boundaries.

### 21.3 Runtime preconditions and business order

Dynamic preconditions are evaluated only when their block executes. For example:

```text
F0 put object in A
F1 create bucket X
F2 append records in X
F3 claim unique file in X using OBJECT_ABSENT/fence
F4 put another object in A
```

The uniqueness check in `F3` MUST NOT be evaluated before `F0`, `F1`, or `F2`.
If it fails, `F3` rolls back locally and the saga compensates `F2`, `F1`, and
`F0` in that order unless retry policy says the error is transient.

### 21.4 Failure classification

```text
Transient before target commit
  retry the same invocation according to policy.

Deterministic before target commit
  rollback the target transaction;
  pre-pivot: begin compensation after recording failure;
  post-pivot: move to forward-recovery BLOCKED or policy-defined retry.

Commit response proves committed
  record committed receipt and advance.

Timeout/lost response/process failure after invocation
  mark or retain outcome unknown; reconcile; do not compensate yet.

Target transaction proves rolled_back/failed/not_committed
  record failure and apply retry/compensation policy.
```

## 22. Uncertain-outcome reconciliation

The durable invocation id and target idempotency key are the primary recovery
keys. A reconciliation task may run on any node after claiming the corresponding
outcome-unknown unit.

Reconciliation MUST inspect, as available:

```text
GetTransaction(transaction_id)
idempotency lookup by target begin/commit key
committed transaction manifest and mutation evidence
target root generation history
operation-specific result receipts
owner-addressable holds created by the invocation
```

Required outcomes:

```text
Committed with valid evidence
  import/verify receipt, mark block COMMITTED, advance saga.

Open and safely resumable
  resume staging/commit using the same transaction and invocation id when the
  transaction contract permits it.

Open but no longer safely resumable
  roll back, record failure, and follow retry/compensation rules.

Rolled back or deterministically failed
  record failure and follow retry/compensation rules.

Evidence inconsistent or root generation in doubt
  move saga to BLOCKED; retain all holds; emit protocol finding.
```

An implementation MUST NOT infer failure solely from elapsed time or an expired
execution claim.

## 23. Compensation execution protocol

When a pre-pivot forward block reaches terminal failure, the saga root sets
`next_compensation_ordinal` to the greatest committed compensatable block ordinal.

For each committed block in descending ordinal:

1. Evaluate each forward operation in reverse operation order.
2. Mark `CoveredBy` operations `SKIPPED_COVERED`, retaining their holds until the
   covering compensation commits.
3. Materialise Anvil-generated inverse requests from the selected automatic
   compensation kind and verified forward receipt.
4. Schedule each explicit compensation program's transaction blocks in its
   declared order.
5. Execute every compensation block as one ordinary single-root transaction,
   using the same intent, claim, idempotency, outcome-unknown, and reconciliation
   protocol as forward execution.
6. Mark the owning forward block `COMPENSATED` only after all non-covered
   compensation obligations for that block are complete.
7. Advance to the next lower committed forward ordinal.

A compensation transaction failure is not grounds to forget the obligation.
Transient errors are retried. A deterministic precondition conflict, exhausted
retry policy, corrupt receipt, unavailable referenced version, or inconsistent
root evidence moves the saga to `BLOCKED` and preserves all required holds.

Compensation programs are not recursively compensated. If one compensation block
commits and a later block cannot complete, the saga remains in compensation with
its program cursor at the first incomplete block.

## 24. Per-unit claims, leases, and fences

No node owns an entire saga. Any eligible Anvil node may discover and claim the
next runnable unit through the saga runnable index.

A claim is scoped to:

```text
(saga_id, execution_phase, block_or_hold_unit_id)
```

and carries:

```text
claiming node id
monotonic claim fence
lease expiry
stable invocation id when applicable
expected saga revision
attempt number
```

Claim acquisition atomically changes the unit from `READY` to `CLAIMED` and
increments its fence in the saga root. A task may renew only the same current
claim. After expiry, another task may claim the unit with a greater fence.

Every saga-root transition submitted by a task MUST include its expected claim
fence. A stale task is rejected with `SagaExecutionClaimStale`.

A stale task may already have sent a participant RPC immediately before losing
its lease. Therefore claim fencing is not a substitute for target idempotency.
The stable invocation id ensures that an old and new task can only attempt the
same logical participant transaction, and reconciliation selects the one durable
outcome.

The implementation SHOULD avoid affinity that repeatedly assigns every block of
one long saga to the same node. Scheduling MAY consider locality, but correctness
must not depend on it.

## 25. Authorisation model

Saga execution must not persist bearer tokens or grant a general internal
principal unrestricted authority.

### 25.1 Construction checks

Every recorded forward and explicit compensation operation is authorised when it
is recorded. The caller must have permission to mutate the saga draft and the
operation's target resource or typed future resource scope.

### 25.2 Apply-time envelope

`ApplySaga` reauthorises the sealed plan and creates a deterministic restricted
`SagaAuthorizationEnvelope` containing at least:

```text
saga id
sealed plan hash
initiating principal
apply-time authz revision
allowed operation hashes
allowed compensation program hashes
allowed concrete and typed-derived root selectors
forward revalidation policy
compensation recovery grant
expiry/revocation metadata
```

The envelope is signed or committed through the system authz/control path and is
part of the plan hash. It authorises only the exact immutable plan.

### 25.3 Forward execution

Before a forward block begins, the task MUST verify the envelope and recheck the
initiating principal's current permission for that block unless an operator-
controlled policy explicitly pins forward authority at apply time. The default is
current-permission revalidation.

A denied forward block is a deterministic forward failure. Before pivot it
causes compensation; after pivot it causes forward-recovery blocking.

### 25.4 Compensation authority

Compensation MUST remain executable through the sealed recovery grant even if the
initiating user's ordinary permission is later removed. Otherwise permission
revocation can make consistency recovery impossible.

The recovery grant permits only the exact compensation operation hashes and
resource selectors frozen in the plan or derived from verified receipts. An
administrator may quarantine a saga, but ordinary authz revocation does not
silently discard compensation obligations.

### 25.5 Saga namespace extension

The built-in authz schema is extended conceptually with:

```text
namespace saga
  relation parent_tenant
  relation owner
  relation editor
  relation viewer
  relation operator
  permission build = owner or editor or parent_tenant->manage_tenant
  permission apply = owner or editor or parent_tenant->manage_tenant
  permission view = owner or editor or viewer or operator or parent_tenant->read_tenant
  permission cancel = owner or editor or parent_tenant->manage_tenant
  permission resolve = operator or parent_tenant->manage_tenant
```

Underlying resource permissions remain required. Saga permissions do not grant
access to buckets, objects, streams, authz realms, registries, or control roots.

## 26. CoreMeta and CoreStore extensions

### 26.1 Column family and writer registry

ANVIL-0008 adds:

```text
CoreMeta column family: cf_sagas
WriterFamily value:     saga_control
Root kind:              sagas
```

`cf_sagas` is the only authoritative compact metadata store for saga plans,
current state, claims, receipts, hold obligations, runnable entries, and compact
events. Large request bodies, compensation bodies, receipts, and event payloads
MUST use `saga_control` logical files through the unified byte pipeline.

No saga subsystem may create a durable JSON plan file, local workflow database,
secondary embedded KV store, or independent event journal.

### 26.2 Table registry additions

The CoreMeta table registry is extended as follows:

| Table id | Column family | Tuple key | Payload | Visibility |
|---:|---|---|---|---|
| `0x8e01` | `cf_sagas` | `realm / saga_id` | `SagaInstanceRow` | committed saga root generation |
| `0x8e02` | `cf_sagas` | `realm / saga_id / block_kind / block_owner_id / block_ordinal` | `SagaTransactionBlockRow` | committed saga root generation |
| `0x8e03` | `cf_sagas` | `realm / saga_id / block_id / operation_ordinal` | `SagaOperationRow` | committed saga root generation |
| `0x8e04` | `cf_sagas` | `realm / saga_id / compensation_program_id` | `SagaCompensationProgramRow` | committed saga root generation |
| `0x8e05` | `cf_sagas` | `realm / saga_id / execution_phase / unit_id` | `SagaExecutionClaimRow` | committed saga root generation |
| `0x8e06` | `cf_sagas` | `realm / saga_id / invocation_id` | `SagaInvocationReceiptRow` | committed saga root generation |
| `0x8e07` | `cf_sagas` | `realm / saga_id / hold_id` | `SagaReferenceHoldRow` | committed saga root generation |
| `0x8e08` | `cf_sagas` | `realm / saga_id / event_sequence` | `SagaEventRow` | committed saga root generation |
| `0x8e09` | `cf_sagas` | `next_wakeup / realm / saga_id / execution_phase / unit_id` | `SagaRunnableIndexRow` | committed saga root generation |
| `0x8b03` | `cf_refcounts` | `ref_kind / ref_id / owner_kind / owner_id / hold_id` | `OwnedReferenceEdgeRow` | committed referenced-resource root generation |

All rows use deterministic protobuf, `CoreMetaRowCommon common = 1`, and the
CoreMeta envelope and size limits inherited from ANVIL-0007.

### 26.3 Policy messages

```protobuf
enum SagaRetryExhaustedAction {
  SAGA_RETRY_EXHAUSTED_ACTION_UNSPECIFIED = 0;
  START_COMPENSATION = 1;
  BLOCK = 2;
}

message SagaRetryPolicy {
  uint32 max_attempts = 1;              // 0 means operator-defined unbounded
  uint64 initial_backoff_ms = 2;
  uint64 maximum_backoff_ms = 3;
  uint32 multiplier_milli = 4;          // 2000 means 2.0
  uint32 jitter_basis_points = 5;       // deterministic bounded jitter
  SagaRetryExhaustedAction exhausted_action = 6;
}

enum SagaForwardAuthorizationMode {
  SAGA_FORWARD_AUTHORIZATION_MODE_UNSPECIFIED = 0;
  REVALIDATE_CURRENT_PERMISSION = 1;
  PIN_AT_APPLY_BY_OPERATOR_POLICY = 2;
}

message SagaExecutionPolicy {
  SagaRetryPolicy default_forward_retry = 1;
  SagaRetryPolicy default_compensation_retry = 2;
  SagaForwardAuthorizationMode forward_authorization_mode = 3;
  uint64 maximum_execution_age_ms = 4;  // deadline signal; never silent abandonment
}
```

A retry policy cannot make an unsafe block compensatable. For pre-pivot forward
blocks, exhausted action normally is `START_COMPENSATION`. For pivot or post-
pivot blocks it MUST be `BLOCK`.

### 26.4 Saga instance row

```protobuf
message SagaInstanceRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  SagaLifecycleState lifecycle_state = 3;
  SagaOutcome outcome = 4;

  uint64 draft_revision = 5;
  uint64 saga_revision = 6;
  string sealed_plan_hash = 7;

  string open_forward_block_id = 8;
  uint64 next_forward_ordinal = 9;
  int64 next_compensation_ordinal = 10;  // -1 when none
  optional uint64 pivot_ordinal = 11;

  bool cancel_requested = 12;
  string initiating_principal_id = 13;
  string initiating_authz_subject = 14;
  CoreMetaInlineOrLocator authorization_envelope = 15;
  SagaExecutionPolicy execution_policy = 16;

  uint64 created_at_unix_nanos = 17;
  uint64 draft_expires_at_unix_nanos = 18;
  uint64 applied_at_unix_nanos = 19;
  uint64 terminal_at_unix_nanos = 20;

  uint64 forward_block_count = 21;
  uint64 operation_count = 22;
  uint64 compensation_program_count = 23;
  uint64 outstanding_reference_hold_count = 24;
  uint64 next_event_sequence = 25;

  string terminal_error_code = 26;
  string blocked_reason_code = 27;
  string last_event_hash = 28;
}
```

`draft_revision` changes only during construction. `saga_revision` changes for
every applied-state transition, claim, receipt, compensation cursor, hold cleanup,
and terminal transition. An applied transition MUST compare the expected
`saga_revision`.

### 26.5 Transaction block row

```protobuf
enum SagaBlockKind {
  SAGA_BLOCK_KIND_UNSPECIFIED = 0;
  FORWARD = 1;
  COMPENSATION = 2;
}

enum SagaExecutionPhase {
  SAGA_EXECUTION_PHASE_UNSPECIFIED = 0;
  FORWARD = 1;
  FORWARD_RECONCILIATION = 2;
  COMPENSATION = 3;
  COMPENSATION_RECONCILIATION = 4;
  REFERENCE_ACQUIRE = 5;
  REFERENCE_RELEASE = 6;
}

message SagaTransactionBlockRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  string block_id = 3;
  uint64 block_ordinal = 4;
  SagaBlockKind block_kind = 5;
  SagaBlockSemantics semantics = 6;
  SagaTargetRoot target_root = 7;
  string target_root_selector_hash = 8;

  SagaTransactionBlockState state = 9;
  repeated string operation_ids = 10;
  repeated string explicit_compensation_program_ids = 11;
  string block_hash = 12;

  string invocation_id = 13;
  string target_transaction_id = 14;
  uint32 attempt_count = 15;
  uint64 next_attempt_unix_nanos = 16;

  uint64 committed_root_generation = 17;
  CoreMetaLocator transaction_manifest_locator = 18;
  string invocation_receipt_hash = 19;
  string last_error_code = 20;

  SagaRetryPolicy retry_policy = 21;
  uint64 current_claim_fence = 22;
  string block_owner_id = 23;          // "forward" or compensation_program_id
}
```

For forward blocks, `block_owner_id` is the canonical sentinel `forward`. For
compensation blocks it is the owning `compensation_program_id`, and
`block_ordinal` is the ordinal inside that program. This key shape prevents
ordinal collisions between independent compensation programs.

### 26.6 Operation row

```protobuf
enum SagaOperationState {
  SAGA_OPERATION_STATE_UNSPECIFIED = 0;
  RECORDED = 1;
  FORWARD_COMMITTED = 2;
  FORWARD_FAILED = 3;
  COMPENSATION_PENDING = 4;
  COMPENSATED = 5;
  COVERED = 6;
  COMPENSATION_BLOCKED = 7;
}

message SagaOperationRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  string block_id = 3;
  string operation_id = 4;
  uint32 operation_ordinal = 5;
  SagaOperationKind operation_kind = 6;

  CoreMetaInlineOrLocator canonical_recorded_request = 7;
  string request_hash = 8;
  CoreMetaInlineOrLocator canonical_compensation_contract = 9;
  string compensation_contract_hash = 10;
  string operation_hash = 11;

  SagaOperationState state = 12;
  CoreMetaInlineOrLocator execution_receipt = 13;
  string execution_receipt_hash = 14;
  repeated string reference_hold_ids = 15;
  repeated string produced_typed_reference_ids = 16;
  string last_error_code = 17;
}
```

The canonical request stored for a streaming operation contains immutable body
locators, hashes, lengths, and media metadata rather than embedding large bytes.

For a forward block, `canonical_compensation_contract` contains the required
`SagaCompensationSpec`. For a compensation block it contains the deterministic
marker below, which makes the absence of nested compensation explicit rather
than relying on an omitted/default field:

```protobuf
message SagaCompensationExecutionMarker {
  bool no_nested_compensation = 1; // MUST be true
}
```

### 26.7 Compensation program row

```protobuf
enum SagaCompensationProgramState {
  SAGA_COMPENSATION_PROGRAM_STATE_UNSPECIFIED = 0;
  DRAFT = 1;
  SEALED = 2;
  EXECUTING = 3;
  COMPLETE = 4;
  BLOCKED = 5;
}

message SagaCompensationProgramRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  string compensation_program_id = 3;
  string owning_forward_operation_id = 4;
  SagaCompensationProgramState state = 5;
  repeated string compensation_block_ids = 6;
  uint64 next_compensation_block_ordinal = 7;
  string program_hash = 8;
  string last_error_code = 9;
}
```

### 26.8 Execution claim row

```protobuf
message SagaExecutionClaimRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  SagaExecutionPhase phase = 3;
  string unit_id = 4;
  string claiming_node_id = 5;
  uint64 claim_fence = 6;
  uint64 lease_expires_at_unix_nanos = 7;
  string invocation_id = 8;
  uint32 attempt = 9;
  uint64 claimed_saga_revision = 10;
}
```

A missing or expired claim does not change the target transaction outcome. It
only permits another task to obtain a greater claim fence.

### 26.9 Invocation receipt row

```protobuf
enum SagaInvocationOutcome {
  SAGA_INVOCATION_OUTCOME_UNSPECIFIED = 0;
  COMMITTED = 1;
  NOT_COMMITTED = 2;
  OUTCOME_UNKNOWN = 3;
  BLOCKED_INCONSISTENT_EVIDENCE = 4;
}

message SagaInvocationReceiptRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  string invocation_id = 3;
  SagaExecutionPhase phase = 4;
  string block_id = 5;
  string target_root_key_hash = 6;
  string target_transaction_id = 7;
  SagaInvocationOutcome outcome = 8;

  uint64 committed_root_generation = 9;
  CoreMetaLocator transaction_manifest_locator = 10;
  repeated string mutation_ids = 11;
  CoreMetaInlineOrLocator canonical_result = 12;
  string result_hash = 13;
  repeated string generated_reference_hold_ids = 14;

  uint64 first_invoked_at_unix_nanos = 15;
  uint64 resolved_at_unix_nanos = 16;
  string receipt_hash = 17;
  string error_code = 18;
}
```

### 26.10 Saga reference hold row

```protobuf
message SagaReferenceHoldRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  string hold_id = 3;
  SagaReferenceReason reason = 4;
  SagaReferenceHoldState state = 5;

  SagaTargetRoot target_root = 6;
  string resolved_target_root_key_hash = 7;
  string ref_kind = 8;
  string ref_id = 9;
  CoreMetaLocator locator = 10;
  string ref_hash = 11;

  bytes owned_reference_edge_key = 12;
  string acquire_transaction_id = 13;
  string release_transaction_id = 14;
  uint64 acquired_root_generation = 15;
  uint64 released_root_generation = 16;
  string hold_hash = 17;
  string last_error_code = 18;
}
```

After `RELEASED`, `locator` and other strong-resolution fields MUST be cleared or
replaced by non-resolving hashes in the current row version.

### 26.11 Event and runnable rows

```protobuf
enum SagaEventKind {
  SAGA_EVENT_KIND_UNSPECIFIED = 0;
  SAGA_STARTED = 1;
  BLOCK_OPENED = 2;
  OPERATION_RECORDED = 3;
  BLOCK_SEALED = 4;
  PLAN_APPLIED = 5;
  UNIT_CLAIMED = 6;
  INVOCATION_STARTED = 7;
  INVOCATION_OUTCOME_UNKNOWN = 8;
  INVOCATION_COMMITTED = 9;
  INVOCATION_FAILED = 10;
  COMPENSATION_STARTED = 11;
  COMPENSATION_COMMITTED = 12;
  SAGA_BLOCKED = 13;
  REFERENCE_ACQUIRE_STARTED = 14;
  REFERENCE_ACQUIRED = 15;
  REFERENCE_RELEASE_STARTED = 16;
  REFERENCE_RELEASED = 17;
  SAGA_TERMINAL = 18;
}

message SagaEventRow {
  CoreMetaRowCommon common = 1;
  string saga_id = 2;
  uint64 event_sequence = 3;
  SagaEventKind event_kind = 4;
  uint64 saga_revision = 5;
  string block_id = 6;
  string operation_id = 7;
  string invocation_id = 8;
  CoreMetaInlineOrLocator event_payload = 9;
  string previous_event_hash = 10;
  string event_hash = 11;
  uint64 created_at_unix_nanos = 12;
}

message SagaRunnableIndexRow {
  CoreMetaRowCommon common = 1;
  uint64 next_wakeup_unix_nanos = 2;
  string saga_id = 3;
  SagaExecutionPhase phase = 4;
  string unit_id = 5;
  uint64 expected_saga_revision = 6;
  string runnable_hash = 7;
}
```

The runnable row is a committed scheduling index, not a second queue system. It
may be rebuilt from saga current-state rows and receipts. A stale runnable row
cannot authorise execution because claim acquisition also compares current saga
revision and unit state.

### 26.12 Value-size and storage rules

- Every `cf_sagas` value remains subject to the CoreMeta 64 KiB encoded limit.
- High-cardinality event or operation rows SHOULD remain below 16 KiB.
- Large request, result, event, and compensation bytes MUST use
  `CoreMetaInlineOrLocator.locator` and `saga_control` logical files.
- A saga row may point at a large body only after that body's byte-plane
  durability receipts exist.
- Saga metadata publication MUST follow ANVIL-0007 ordering: durable bytes first,
  then compact CoreMeta locators, then saga-root publication.
- Event compaction may produce immutable saga audit segments, but current state
  and unresolved obligations remain in CoreMeta.

## 27. Canonical saga hashes

All fields below are canonical deterministic protobuf or canonical scalar bytes
under ANVIL-0007.

```text
SagaCompensationContractHash =
  if forward operation:
    Hash(anvil.saga.compensation_spec.v1,
         canonical(SagaCompensationSpec))
  if explicit compensation-program operation:
    Hash(anvil.saga.compensation_spec.v1,
         canonical(SagaCompensationExecutionMarker{no_nested_compensation=true}))

SagaOperationHash =
  Hash(anvil.saga.operation.v1,
       saga_id,
       block_id,
       operation_id,
       operation_ordinal,
       operation_kind,
       request_hash,
       SagaCompensationContractHash,
       ordered(reference_hold_ids),
       ordered(produced_typed_reference_ids))

SagaBlockHash =
  Hash(anvil.saga.block.v1,
       saga_id,
       block_id,
       block_ordinal,
       block_kind,
       semantics,
       canonical(SagaTargetRoot),
       canonical(SagaRetryPolicy),
       ordered(SagaOperationHash))

SagaCompensationProgramHash =
  Hash(anvil.saga.compensation_program.v1,
       saga_id,
       compensation_program_id,
       owning_forward_operation_id,
       ordered(SagaBlockHash for compensation blocks))

SagaPlanContentHash =
  Hash(anvil.saga.plan_content.v1,
       saga_id,
       realm_id,
       canonical(SagaExecutionPolicy),
       ordered(SagaBlockHash for forward blocks),
       sorted(SagaCompensationProgramHash by program id))

SagaAuthorizationEnvelopeHash =
  Hash(anvil.saga.authorization.v1,
       canonical(SagaAuthorizationEnvelope without envelope_hash and signature))

SagaPlanHash =
  Hash(anvil.saga.plan.v1,
       SagaPlanContentHash,
       SagaAuthorizationEnvelopeHash)

SagaInvocationId =
  Hash(anvil.saga.invocation.v1,
       saga_id,
       SagaPlanHash,
       unit_id,
       execution_phase)

SagaInvocationReceiptHash =
  Hash(anvil.saga.receipt.v1,
       saga_id,
       SagaInvocationId,
       target_root_key_hash,
       target_transaction_id,
       outcome,
       committed_root_generation,
       transaction_manifest_locator_hash,
       sorted(mutation_ids),
       result_hash,
       sorted(generated_reference_hold_ids))

SagaReferenceHoldHash =
  Hash(anvil.saga.reference_hold.v1,
       saga_id,
       hold_id,
       reason,
       canonical(target_root),
       ref_kind,
       ref_id,
       locator_hash)

SagaEventHash =
  Hash(anvil.saga.event.v1,
       saga_id,
       event_sequence,
       event_kind,
       saga_revision,
       block_id,
       operation_id,
       invocation_id,
       event_payload_hash,
       previous_event_hash)

SagaRunnableHash =
  Hash(anvil.saga.runnable.v1,
       next_wakeup_unix_nanos,
       saga_id,
       execution_phase,
       unit_id,
       expected_saga_revision)
```

The plan hash MUST include body content hashes, not merely mutable local landing
paths. Two implementations receiving the same logical plan MUST produce the same
plan hash.

## 28. Explicit compensation construction API

Explicit compensation programs are built before the forward operation that
references them. SDKs SHOULD preallocate the forward `operation_id` locally so
typed result references can name it.

```protobuf
message StartSagaCompensationProgramRequest {
  string saga_id = 1;
  uint64 expected_draft_revision = 2;
  string compensation_program_id = 3;
  string owning_forward_operation_id = 4;
  string idempotency_key = 5;
}

message StartSagaCompensationProgramResponse {
  string request_id = 1;
  string saga_id = 2;
  string compensation_program_id = 3;
  uint64 draft_revision = 4;
  SagaCompensationProgramState state = 5;
}

message BeginSagaCompensationTransactionRequest {
  string saga_id = 1;
  string compensation_program_id = 2;
  uint64 expected_draft_revision = 3;
  string block_idempotency_key = 4;
  SagaTargetRoot target_root = 5;
  SagaRetryPolicy retry_policy = 6;
}

message BeginSagaCompensationTransactionResponse {
  string request_id = 1;
  string saga_id = 2;
  string compensation_program_id = 3;
  string saga_compensation_transaction_id = 4;
  uint64 compensation_block_ordinal = 5;
  uint64 draft_revision = 6;
}

message SealSagaCompensationTransactionRequest {
  string saga_id = 1;
  string compensation_program_id = 2;
  string saga_compensation_transaction_id = 3;
  uint64 expected_draft_revision = 4;
}

message SealSagaCompensationTransactionResponse {
  string request_id = 1;
  string compensation_program_id = 2;
  string compensation_block_hash = 3;
  uint64 draft_revision = 4;
}

message SealSagaCompensationProgramRequest {
  string saga_id = 1;
  string compensation_program_id = 2;
  uint64 expected_draft_revision = 3;
}

message SealSagaCompensationProgramResponse {
  string request_id = 1;
  string compensation_program_id = 2;
  string compensation_program_hash = 3;
  uint64 draft_revision = 4;
  SagaCompensationProgramState state = 5; // SEALED
}
```

An explicit compensation program cannot be modified after sealing. A forward
operation referencing it MUST carry both id and expected program hash.

## 29. Status, cancellation, and operator resolution API

### 29.1 Status and watch

```protobuf
message GetSagaRequest {
  string saga_id = 1;
}

message WatchSagaRequest {
  string saga_id = 1;
  uint64 after_event_sequence = 2;
  uint32 batch_limit = 3;
}

message SagaStatus {
  string saga_id = 1;
  SagaLifecycleState lifecycle_state = 2;
  SagaOutcome outcome = 3;
  uint64 draft_revision = 4;
  uint64 saga_revision = 5;
  string sealed_plan_hash = 6;

  uint64 forward_block_count = 7;
  uint64 next_forward_ordinal = 8;
  int64 next_compensation_ordinal = 9;
  optional uint64 pivot_ordinal = 10;

  string active_block_id = 11;
  SagaExecutionPhase active_phase = 12;
  SagaTransactionBlockState active_block_state = 13;
  uint64 outstanding_reference_hold_count = 14;

  string blocked_reason_code = 15;
  optional AnvilError last_error = 16;
  uint64 latest_event_sequence = 17;
}

message SagaEvent {
  string saga_id = 1;
  uint64 event_sequence = 2;
  SagaEventKind event_kind = 3;
  uint64 saga_revision = 4;
  string block_id = 5;
  string operation_id = 6;
  string invocation_id = 7;
  bytes canonical_event_summary = 8;
  string event_hash = 9;
  uint64 created_at_unix_nanos = 10;
}
```

`WatchSaga` is cursor-based over committed saga events. It does not read a local
task queue. An expired retained cursor returns `WatchCursorExpired` under the
same retention principles as other Anvil watch APIs.

### 29.2 Cancellation and draft abort

```protobuf
message CancelSagaRequest {
  string saga_id = 1;
  string idempotency_key = 2;
  string reason = 3;
}

message AbortSagaDraftRequest {
  string saga_id = 1;
  uint64 expected_draft_revision = 2;
  string idempotency_key = 3;
  string reason = 4;
}
```

`AbortSagaDraft` is legal only for `DRAFT` or `DRAFT_INVALID`. It transitions to
reference cleanup and never executes a target-root forward operation.

### 29.3 Blocked-saga resolution

```protobuf
enum SagaResolutionAction {
  SAGA_RESOLUTION_ACTION_UNSPECIFIED = 0;
  RETRY_CURRENT_UNIT = 1;
  RECONCILE_CURRENT_INVOCATION = 2;
  RESUME_FORWARD = 3;
  RESUME_COMPENSATION = 4;
  ACCEPT_VERIFIED_EXTERNAL_RECEIPT = 5;
  ABANDON_AND_RELEASE_REFERENCES = 6;
}

message ResolveBlockedSagaRequest {
  string saga_id = 1;
  uint64 expected_saga_revision = 2;
  SagaResolutionAction action = 3;
  bytes canonical_evidence = 4;
  string evidence_hash = 5;
  string idempotency_key = 6;
}
```

Resolution requires `saga.resolve` and, where applicable, system-level
operational authority. It MUST append evidence and a state-transition event. It
MUST NOT rewrite or delete prior receipts.

`ACCEPT_VERIFIED_EXTERNAL_RECEIPT` is permitted only when the supplied evidence
verifies under the same transaction/root signature and hash rules as an ordinary
receipt. It is not an operator assertion that an unverified effect happened.

`ABANDON_AND_RELEASE_REFERENCES` selects outcome `ABANDONED`. It is an explicit
loss-of-business-guarantee decision and requires elevated authority. It does not
claim that compensation completed.

### 29.4 Authorisation envelope schema

```protobuf
message SagaAuthorisedOperation {
  string operation_hash = 1;
  SagaOperationKind operation_kind = 2;
  string target_root_selector_hash = 3;
  bool compensation = 4;
}

message SagaAuthorizationEnvelope {
  string saga_id = 1;
  string plan_content_hash = 2;
  string initiating_principal_id = 3;
  string initiating_authz_subject = 4;
  string apply_authz_revision = 5;
  repeated SagaAuthorisedOperation allowed_operations = 6;
  repeated string allowed_compensation_program_hashes = 7;
  SagaForwardAuthorizationMode forward_authorization_mode = 8;
  uint64 valid_from_unix_nanos = 9;
  uint64 forward_authority_expires_at_unix_nanos = 10;
  bool compensation_recovery_grant = 11;
  string envelope_hash = 12;
  bytes signature = 13;
}
```

The allowed operation set is sorted by `operation_hash` before hashing.
`envelope_hash` and `signature` are excluded from the envelope hash input. The
final `sealed_plan_hash` is computed only after this envelope hash exists, so the
profile has no circular hash dependency.

## 30. Native bucket and fence operations required by the initial saga profile

ANVIL-0007's authz matrix refers to bucket creation but does not define the
complete native bucket mutation messages. ANVIL-0008 adds the minimum typed
operations needed by the saga profile; a later bucket RFC may extend but must not
weaken their idempotency and conditional-compensation semantics.

```protobuf
service BucketService {
  rpc CreateBucket(CreateBucketRequest) returns (WriteResponse);
  rpc DeleteBucket(DeleteBucketRequest) returns (WriteResponse);
}

message CreateBucketRequest {
  string bucket = 1;
  string storage_class = 2;
  map<string,string> metadata = 3;
  WriteOptions options = 4;
}

message DeleteBucketRequest {
  string bucket = 1;
  optional string expected_creation_id = 2;
  bool require_no_external_mutations = 3;
  WriteOptions options = 4;
}

message BucketCreationResult {
  string bucket_id = 1;
  string bucket_name = 2;
  string creation_id = 3;
  uint64 creation_root_generation = 4;
  repeated TransactionScope root_scopes = 5;
}
```

`BUCKET_CREATE_DELETE_CREATED_BUCKET_V1` records the exact `creation_id` and
creation generation. Compensation MUST require that the bucket is still that
saga-created bucket. Unless all contained effects are proven saga-owned and
covered, it MUST also require no external mutations. Failure of these conditions
is `SagaCompensationConflict`, not blind deletion.

The initial fence profile adds:

```protobuf
service LeaseFenceService {
  rpc ClaimFence(ClaimFenceRequest) returns (WriteResponse);
  rpc ReleaseFence(ReleaseFenceRequest) returns (WriteResponse);
}

message ClaimFenceRequest {
  string lease_id = 1;
  string resource_key = 2;
  uint64 ttl_ms = 3;
  bool require_absent = 4;
  WriteOptions options = 5;
}

message ReleaseFenceRequest {
  string lease_id = 1;
  string resource_key = 2;
  uint64 expected_fence_token = 3;
  WriteOptions options = 4;
}

message FenceClaimResult {
  string lease_id = 1;
  string resource_key = 2;
  uint64 fence_token = 3;
  uint64 expires_at_unix_nanos = 4;
}
```

`LEASE_FENCE_CLAIM_RELEASE_EXACT_FENCE_V1` releases only the exact token produced
by the forward invocation. A later claimant's token cannot be released by stale
saga compensation.


### 30.1 Canonical recorded-only request types

The compensation-only and control operations referenced by the operation
registry use these deterministic recorded forms. They are not accepted with
caller-supplied execution context; the saga engine supplies the target
transaction at replay time.

```protobuf
message RecordedMutationOptions {
  string idempotency_key = 1;
  repeated WritePrecondition preconditions = 2;
  repeated BoundaryValue boundary_values = 3;
}

message RecordedObjectHeadValue {
  bool absent = 1;
  string head_kind = 2;
  string version_id = 3;
  CoreMetaLocator manifest_locator = 4;
  bool delete_marker = 5;
  string head_hash = 6;
}

message RecordedRestoreObjectHeadRequest {
  string bucket = 1;
  string key = 2;
  RecordedObjectHeadValue expected_current_head = 3;
  RecordedObjectHeadValue restored_head = 4;
  RecordedMutationOptions options = 5;
}

message RecordedCreateBucketRequest {
  string bucket = 1;
  string storage_class = 2;
  map<string,string> metadata = 3;
  RecordedMutationOptions options = 4;
}

message RecordedDeleteBucketRequest {
  string bucket = 1;
  string expected_creation_id = 2;
  bool require_no_external_mutations = 3;
  RecordedMutationOptions options = 4;
}

message RecordedRetractStreamRangeRequest {
  string stream = 1;
  string partition = 2;
  uint64 first_sequence = 3;
  uint64 last_sequence_inclusive = 4;
  string source_forward_invocation_id = 5;
  RecordedMutationOptions options = 6;
}

message RecordedLeaseFenceClaimRequest {
  string lease_id = 1;
  string resource_key = 2;
  uint64 ttl_ms = 3;
  bool require_absent = 4;
  RecordedMutationOptions options = 5;
}

message RecordedLeaseFenceReleaseRequest {
  string lease_id = 1;
  string resource_key = 2;
  uint64 expected_fence_token = 3;
  RecordedMutationOptions options = 4;
}
```

`RecordedRestoreObjectHeadRequest` is constructed by Anvil from the verified
forward receipt; callers select the corresponding automatic compensation enum
but do not supply arbitrary before/after head values.

`RecordedRetractStreamRangeRequest` creates a revision-aware logical retraction
or tombstone for the exact committed range. It does not physically edit an
immutable stream segment in place. A stream family that cannot support this
semantic MUST reject the automatic kind and require explicit or covering
compensation instead.

## 31. Error-code additions

The ANVIL-0007 stable error registry is extended with:

```text
SagaNotFound
SagaInvalidState
SagaNotDraft
SagaDraftInvalid
SagaDraftExpired
SagaHasOpenTransaction
SagaTransactionNotFound
SagaTransactionScopeMismatch
SagaPlanRevisionConflict
SagaAlreadyApplied
SagaPlanInvalid
SagaTooLarge
SagaOperationCompensationRequired
SagaCompensationKindMismatch
SagaCompensationUnsupported
SagaCompensationProgramNotSealed
SagaCompensationCoverageInvalid
SagaCompensationConflict
SagaDerivedReferenceInvalid
SagaDerivedReferenceCycle
SagaOperationKindUnsupported
SagaOperationPayloadMismatch
SagaOperationIdempotencyConflict
SagaReferenceHoldConflict
SagaReferenceHoldFailed
SagaReferenceReleaseFailed
SagaExecutionClaimStale
SagaInvocationEvidenceConflict
SagaCommitOutcomeUnknown
SagaCompensationBlocked
SagaCleanupBlocked
SagaPastPivot
SagaAuthorizationExpired
SagaAuthorizationRevoked
SagaResolutionEvidenceInvalid
```

Required details and retryability:

```text
SagaTransactionScopeMismatch
  retryable: false for the same draft call
  details: saga_id,block_id,declared_root_selector_hash,operation_root_selector_hash

SagaOperationCompensationRequired
  retryable: false for the same request
  details: saga_id,block_id,operation_kind

SagaCompensationKindMismatch
  retryable: false for the same request
  details: operation_kind,compensation_kind

SagaCompensationConflict
  retryable: false until state changes or operator resolution
  details: saga_id,block_id,operation_id,expected_state_hash,actual_state_hash

SagaCommitOutcomeUnknown
  retryable: true through reconciliation, not by creating a new invocation
  details: saga_id,block_id,invocation_id,target_transaction_id

SagaReferenceHoldFailed
  retryable: according to referenced-root availability
  details: saga_id,hold_id,ref_kind,ref_hash,target_root_key_hash

SagaExecutionClaimStale
  retryable: false for the stale claim; task must stop
  details: saga_id,unit_id,expected_fence,current_fence

SagaPastPivot
  retryable: false for cancellation
  details: saga_id,pivot_ordinal,pivot_root_generation

SagaCleanupBlocked
  retryable: true after target-root recovery or operator repair
  details: saga_id,hold_id,target_root_key_hash
```

A missing compensation and other structural draft violations return their error
and transition the saga to `DRAFT_INVALID` as specified in section 12.5.

## 32. Recovery

Saga recovery reuses RocksDB recovery, CoreMeta catch-up, root discovery, and
transaction evidence from ANVIL-0007. It does not introduce a saga WAL.

### 32.1 Node startup

After normal CoreStore recovery, a node eligible to run saga tasks MUST:

1. catch up saga-root partitions it owns or serves before claiming current work;
2. validate `SagaInstanceRow`, block, operation, receipt, hold, and event hashes;
3. rebuild or validate runnable index rows from current state;
4. treat expired claims as claimable only after their saga revision and target
   evidence are checked;
5. schedule outcome-unknown reconciliation before any dependent forward or
   compensation block;
6. schedule hold-acquire/release intents that lack matching target receipts;
7. keep blocked sagas and their active holds visible to diagnostics.

### 32.2 Required recovery cases

```text
Recorded body exists, operation row absent
  no target effect exists;
  complete an existing hold intent only if the operation commit can be proven;
  otherwise release orphan plan-body holds after safety window.

Operation row exists, plan not applied
  retain draft; expire/abort according to draft TTL.

Apply committed, runnable row absent
  reconstruct runnable row atomically from saga cursor and block state.

Claim expired before invocation intent
  obtain greater fence and retry unit.

Invocation intent exists, target transaction id absent
  call idempotent BeginTransaction with the same invocation key or prove no
  target transaction was created.

Target transaction open
  resume or roll back according to target transaction state and current saga
  policy; never open another logical invocation.

Target commit occurred, saga receipt absent
  reconcile transaction/idempotency/root evidence; import receipt and generated
  hold ids; advance saga.

Saga says outcome unknown, target proves not committed
  record NOT_COMMITTED and apply retry/compensation policy.

Compensation commit occurred, receipt absent
  reconcile and mark the exact compensation unit complete.

Saga outcome selected, active holds remain
  resume RELEASING_REFERENCES; do not mark TERMINAL.

Target owned-reference edge exists, saga hold row says ACQUIRE_INTENT
  complete HELD receipt or release the edge according to operation evidence.

Saga hold says RELEASE_INTENT, target edge already inactive
  idempotently mark RELEASED.

Event chain hash mismatch or conflicting transaction evidence
  fail closed; mark BLOCKED; publish repair finding.
```

### 32.3 Total restart

After total restart, the saga state is discoverable through the ordinary saga
root-register chain, committed CoreMeta rows, transaction manifests, and
CoreStore blocks. No local task queue, process memory, or sidecar plan directory
may be required to reconstruct runnable work.

### 32.4 Anti-entropy

Anti-entropy MUST verify at least:

```text
saga current row versus latest event sequence/hash
block state versus invocation receipt
runnable row versus current saga cursor
active SagaReferenceHoldRow versus active OwnedReferenceEdgeRow
OwnedReferenceEdgeRow versus aggregate RefCountRow contribution
terminal saga versus zero active saga-owned edges
plan hash versus all sealed blocks/programs
```

Disagreement becomes a repair finding. GC fails closed while active-edge evidence
is inconsistent.

## 33. Observability

### 33.1 Trace operations

ANVIL-0008 adds stable trace operation names:

```text
saga.start
saga.block_begin
saga.operation_record
saga.compensation_program_begin
saga.compensation_operation_record
saga.block_seal
saga.plan_validate
saga.apply
saga.runnable_publish
saga.unit_claim
saga.claim_renew
saga.invocation_intent
saga.target_transaction_begin
saga.target_operation_replay
saga.target_commit
saga.outcome_unknown
saga.reconcile
saga.forward_committed
saga.forward_failed
saga.compensation_begin
saga.compensation_commit
saga.compensation_blocked
saga.reference_acquire_intent
saga.reference_acquire
saga.reference_release_intent
saga.reference_release
saga.cleanup_complete
saga.blocked
saga.terminal
```

The trace must link saga-root state transitions to target transaction request,
mutation, commit-certificate, root-generation, and reference-hold spans through
`trace_id`, `saga_id_hash`, `block_id_hash`, and `invocation_id_hash`.

Raw saga, tenant, bucket, object, and principal ids MUST NOT be unconditional
metric labels.

### 33.2 Metrics

```text
anvil_saga_state_transition_total
  labels: from_state,to_state,outcome,reason

anvil_saga_active
  labels: lifecycle_state,realm_class

anvil_saga_draft_duration_ms
  labels: outcome

anvil_saga_apply_duration_ms
  labels: outcome,block_count_bucket,operation_count_bucket

anvil_saga_block_execution_duration_ms
  labels: phase,operation_family,outcome,attempt_bucket

anvil_saga_block_claim_total
  labels: phase,outcome

anvil_saga_claim_expired_total
  labels: phase

anvil_saga_outcome_unknown_total
  labels: phase,operation_family

anvil_saga_reconciliation_duration_ms
  labels: phase,outcome

anvil_saga_compensation_total
  labels: compensation_kind,outcome

anvil_saga_blocked_total
  labels: phase,reason

anvil_saga_reference_holds
  labels: state,reason,ref_kind

anvil_saga_reference_hold_duration_ms
  labels: reason,ref_kind,outcome

anvil_saga_runnable_lag_ms
  labels: phase

anvil_saga_plan_bytes
  labels: storage_kind

anvil_saga_operation_record_duration_ms
  labels: operation_kind,outcome
```

Duration metrics use the ANVIL-0007 histogram buckets. Counts are counters or
gauges as appropriate.

### 33.3 Required dashboard panels

The CoreMeta and root protocol dashboards are extended with:

```text
active sagas by lifecycle state
runnable lag and claim expiry
forward versus compensation throughput
outcome-unknown count and reconciliation latency
blocked sagas by reason
active reference holds and oldest hold age
cleanup backlog
per-operation recording and execution latency
saga plan block/operation-size distributions
```

### 33.4 Audit diagnostics

Operator diagnostics for one saga must display, subject to authz:

```text
sealed plan hash and block order
current cursor and phase
all target transaction ids/root generations
outcome-unknown evidence
compensation selection for every forward operation
active and released hold ids
claim fences and expiries
blocked reason and repair findings
event hash chain
```

Diagnostics must not expose raw payload bytes or secrets by default.

## 34. Capacity, quotas, and backpressure

ANVIL-0008 defines no protocol-level maximum number of distinct roots. Operators
MUST instead enforce resource quotas over dimensions that correspond to actual
cost:

```text
active saga count per tenant/realm
open draft count
sealed forward block count
recorded operation count
CoreMeta saga metadata bytes
CoreStore recorded payload bytes
active reference hold count and held bytes
runnable/reconciliation backlog
oldest active hold age
blocked saga count
```

Suggested initial configuration keys are:

```text
saga_draft_soft_limit_per_tenant
saga_draft_hard_limit_per_tenant
saga_active_soft_limit_per_tenant
saga_active_hard_limit_per_tenant
saga_plan_coremeta_soft_bytes
saga_plan_coremeta_hard_bytes
saga_recorded_payload_soft_bytes
saga_recorded_payload_hard_bytes
saga_active_hold_soft_count
saga_active_hold_hard_count
saga_runnable_lag_soft_ms
saga_runnable_lag_hard_ms
```

Defaults are operator profile data, not wire semantics. A deployment may support
very large sagas by raising quotas while retaining the same protocol.

At soft limits, recording and `ApplySaga` apply backpressure. At hard limits,
new draft operations fail with `SagaTooLarge` or
`ResourceExhaustedPendingBacklog`. Already applied sagas and compensation MUST
retain priority over new drafts so quota pressure cannot prevent consistency
recovery.

Reference cleanup and compensation traffic MUST NOT be rejected merely because
the tenant has reached its ordinary write quota. They consume a reserved recovery
budget controlled by the operator.

## 35. Security considerations

### 35.1 Closed dispatch

Operation and automatic-compensation dispatch use enums and typed protobuf
oneofs. No user-controlled handler string is executed. Unknown values and
kind/payload mismatches are rejected before plan mutation.

### 35.2 No executable plan content

Recorded requests are data for registered native Anvil methods. The saga plan
MUST NOT contain source code, bytecode, scripts, shell commands, dynamic library
names, arbitrary URLs, or expression-language evaluation.

### 35.3 Typed references only

Future-result dependencies use typed references with producer-kind validation.
There is no general property path, reflection expression, or string interpolation
engine that could access unapproved receipt fields.

### 35.4 Request-body integrity

Large recorded bodies are content-addressed and held by locator/hash. Execution
MUST verify the hash before replay. A local landing path or mutable cache entry is
never plan identity.

### 35.5 Replay and stale task defense

Plan immutability, stable invocation ids, target idempotency, saga revision CAS,
and per-unit claim fences jointly prevent a stale or duplicated task from
creating a distinct logical operation.

### 35.6 Compensation privilege

The compensation recovery grant is restricted to exact sealed inverse hashes. It
is not a general service-account capability. Operator resolution is separately
authorised and audited.

### 35.7 Denial of service through retention

Long-lived drafts and blocked sagas can retain large values. Draft TTLs, quotas,
oldest-hold metrics, operator diagnostics, and explicit abandonment exist to
control this risk. Applied obligations may not be silently expired to recover
space.

### 35.8 Secret material

Recorded requests containing credentials or secrets follow the same encryption,
key authorisation, redaction, and manifest rules as the corresponding native API.
Saga events and metrics store hashes and classifications, not secret bodies.

## 36. End-to-end example

A tenant needs to:

1. write an object in bucket A;
2. create bucket X;
3. append logs in bucket X;
4. claim a unique file in X with an absence/fence precondition;
5. write another object in A;
6. write a final object in X.

Construction may look like:

```rust
let mut saga = client.start_saga(start).await?;

// F0: A objects root.
let mut tx = saga.begin_transaction(bucket_a.objects_root()).await?;
tx.put_object(
    put_a_one,
    ObjectPutCompensation::RestorePreviousHeadV1,
).await?;
saga = tx.commit().await?;

// F1: core/control root. The result is a typed future bucket reference.
let mut tx = saga.begin_transaction(core_control_root).await?;
let bucket_x: SagaBucketRef = tx.create_bucket(
    create_x,
    BucketCreateCompensation::DeleteCreatedBucketV1,
).await?;
saga = tx.commit().await?;

// F2: X streams root, resolved after F1 commits.
let mut tx = saga.begin_transaction(bucket_x.root(SagaRootKind::Streams)).await?;
tx.append_records(
    logs,
    StreamAppendCompensation::CoveredBy {
        covering_operation: bucket_x.creation_operation(),
        coverage: SagaCoverageKind::CreatedContainerDeletionV1,
    },
).await?;
saga = tx.commit().await?;

// F3: X objects root. The uniqueness condition is evaluated here at execution.
let mut tx = saga.begin_transaction(bucket_x.root(SagaRootKind::Objects)).await?;
let claim = tx.claim_fence(
    unique_file_claim,
    FenceClaimCompensation::ReleaseExactFenceV1,
).await?;
tx.put_object(
    unique_file,
    ObjectPutCompensation::RestorePreviousHeadV1,
).await?;
saga = tx.commit().await?;

// F4: return to A.
let mut tx = saga.begin_transaction(bucket_a.objects_root()).await?;
tx.put_object(
    put_a_two,
    ObjectPutCompensation::RestorePreviousHeadV1,
).await?;
saga = tx.commit().await?;

// F5: return to X.
let mut tx = saga.begin_transaction(bucket_x.root(SagaRootKind::Objects)).await?;
tx.put_object(
    put_x_final,
    ObjectPutCompensation::RestorePreviousHeadV1,
).await?;
saga = tx.commit().await?;

let applied = saga.apply().await?;
```

None of the construction calls opens a target transaction. `ApplySaga` freezes:

```text
F0 -> A objects
F1 -> core/control
F2 -> X streams, derived from F1 result
F3 -> X objects, derived from F1 result
F4 -> A objects
F5 -> X objects, derived from F1 result
```

If the unique-file claim in `F3` finds an existing claim:

```text
F3 target transaction rolls back
F2 compensation is evaluated (covered by F1 bucket deletion in this example)
F1 conditionally deletes the exact saga-created bucket X
F0 restores the exact previous head in A
all saga holds are released
outcome = COMPENSATED
```

If `F5` fails after `F3` and `F4` committed, reverse traversal is:

```text
compensate F4
compensate F3, including release of the exact fence token
process F2 coverage
compensate F1
compensate F0
release all holds
```

If an unrelated writer updates the object changed by `F4` before compensation,
its safe restore precondition fails and the saga becomes `BLOCKED`; Anvil does
not overwrite that unrelated version.

If `F3` commit times out, Anvil does not start compensation until it reconciles
whether the claim and object write committed.

## 37. Amendments to ANVIL-0007 registries and contracts

For conformance, the following ANVIL-0007 registries are considered extended:

```text
RootAnchorKey.root_kind
  add: sagas

WriterFamily
  add: saga_control

CoreMetaColumnFamily
  add: cf_sagas

CoreMeta table registry
  add: 0x8e01..0x8e09 and 0x8b03 from section 26

WriteOptions
  replace optional transaction-only execution at field 6 with the oneof in
  section 13 while preserving transaction_id wire tag 6

WriteState
  add: WRITE_STATE_SAGA_OPERATION_RECORDED
       WRITE_STATE_SAGA_COMPENSATION_RECORDED

Native services
  add: SagaService, BucketService minimum profile, LeaseFenceService minimum
  profile

Authz built-in schema
  add: saga namespace from section 25.5

Error registry
  add: section 31 values

Metrics/traces/tests
  add: sections 33 and 39
```

The following ANVIL-0007 rules remain unchanged:

```text
one explicit transaction -> one root
committed transactions cannot be rolled back
RocksDB is the only local metadata database
large bytes use the unified byte pipeline
CoreMeta quorum/certificate precedes root publication
root CAS is linearizable per root key
idempotency and deterministic canonical encoding are mandatory
normal readers ignore pending rows
```

## 38. Implementation phases without semantic shortcuts

Implementation may be phased, but every phase must preserve final semantics.

### Phase 1: draft and storage substrate

- Add saga root, `cf_sagas`, table registry, hash profile, operation recording,
  large-body locators, and draft reference holds.
- Implement `StartSaga`, forward block construction, mandatory compensation
  validation, seal, inspect, and abort.
- No phase may execute target operations before `ApplySaga`.

### Phase 2: forward execution and reconciliation

- Add runnable index, per-unit claims/fences, stable invocation ids, target
  transaction replay, commit receipt import, outcome-unknown recovery, and
  multi-node block handoff.

### Phase 3: automatic compensation and GC safety

- Add versioned object restore, exact fence release, bucket-create deletion,
  owned reference edges, reverse traversal, cleanup, and blocked compensation.

### Phase 4: explicit programs and complete native registry

- Add explicit compensation construction and typed references for all supported
  mutating native APIs.
- Add conformance descriptor coverage test.

### Phase 5: operational hardening

- Add anti-entropy, dashboards, operator resolution, chaos tests, performance
  gates, and large-saga quota/backpressure tests.

Forbidden shortcuts in every phase:

```text
in-memory-only plan or cursor
local JSON saga files
one node owning a saga for its lifetime
open target transactions between blocks
missing-compensation defaults
string-dispatched handlers
blind compensation without exact preconditions
cleanup that drops aggregate counts without owner-addressable edges
```

## 39. Required conformance and recovery tests

The implementation MUST include at least these named tests or release-gate steps:

```text
saga_anvil_0007_incorporation_and_precedence_declared
saga_no_final_sidecar_storage
saga_large_recorded_payload_uses_byte_pipeline
saga_draft_opens_no_target_transaction
saga_apply_freezes_plan_hash
saga_plan_mutation_after_apply_rejected

saga_allows_arbitrary_root_sequence
saga_revisits_same_root_after_intervening_roots
saga_does_not_merge_non_adjacent_same_root_blocks
saga_cross_root_operation_in_open_block_invalidates_draft
saga_does_not_implicitly_split_block

saga_forward_operation_requires_compensation_same_call
saga_unspecified_compensation_invalidates_draft
saga_operation_compensation_enum_mismatch_rejected
saga_unknown_operation_enum_rejected
saga_kind_payload_oneof_mismatch_rejected
saga_typed_sdk_has_no_uncompensated_put_shape

saga_object_put_restores_exact_previous_head
saga_object_put_restores_absence
saga_object_delete_restores_exact_previous_head
saga_object_restore_does_not_overwrite_unrelated_head
saga_repeated_object_writes_unwind_in_reverse_order

saga_explicit_compensation_program_must_be_sealed
saga_explicit_compensation_program_uses_single_root_blocks
saga_covered_by_requires_dominating_compensation
saga_bucket_delete_coverage_rejects_external_mutation
saga_irreversible_pre_pivot_rejected
saga_second_pivot_rejected
saga_post_pivot_failure_does_not_compensate

saga_created_bucket_typed_ref_resolves_later_roots
saga_typed_reference_to_later_operation_rejected
saga_typed_reference_cycle_rejected
saga_typed_reference_cross_saga_rejected
saga_no_string_binding_language

saga_runtime_object_absent_precondition_not_hoisted
saga_unique_fence_claim_failure_compensates_prior_blocks
saga_exact_fence_release_cannot_release_later_claim

saga_each_block_may_be_claimed_by_different_node
saga_no_whole_saga_node_ownership
saga_stale_claim_fence_cannot_advance_state
saga_claim_expiry_does_not_imply_target_failure
saga_duplicate_tasks_collapse_by_invocation_id

saga_crash_after_intent_before_begin_transaction
saga_crash_after_begin_before_operation_replay
saga_crash_after_target_commit_before_saga_receipt
saga_timeout_reconciles_committed_target_transaction
saga_timeout_reconciles_rolled_back_target_transaction
saga_compensation_timeout_reconciles_before_advancing
saga_conflicting_target_evidence_blocks_saga

saga_draft_reference_hold_prevents_gc
saga_before_state_hold_acquired_atomically_with_head_change
saga_after_state_hold_survives_later_saga_overwrite
saga_hold_intent_recovers_orphan_target_edge
saga_release_intent_recovers_already_released_edge
saga_terminal_success_releases_all_holds
saga_terminal_compensated_releases_all_holds
saga_aborted_draft_releases_all_holds
saga_blocked_compensation_retains_required_holds
saga_terminal_audit_has_no_strong_locator
saga_owned_reference_edge_and_refcount_anti_entropy

saga_forward_permission_revocation_triggers_pre_pivot_compensation
saga_compensation_remains_authorised_after_user_revocation
saga_authorization_envelope_restricts_exact_operation_hashes
saga_operator_resolution_requires_evidence_and_authz

saga_runnable_index_rebuilds_from_current_state
saga_total_restart_recovers_without_task_queue
saga_event_hash_chain_detects_corruption
saga_metrics_required_series_present
saga_trace_links_saga_root_to_target_commit
saga_grafana_panels_load_from_provisioning
saga_backpressure_prioritises_compensation_and_cleanup
saga_no_protocol_distinct_root_limit
```

Every test must emit machine-readable artifacts under
`target/anvil/conformance` or `target/anvil/perf` with request id, trace id, saga
id hash, git commit, and pass/fail reason.

## 40. Performance and reliability gates

The saga subsystem is release-ready only when it passes fixed crash, contention,
and throughput gates.

Required scenarios:

```text
small saga
  3 blocks, 3 roots, one object mutation per block

revisited-root saga
  100 blocks alternating across 10 roots

large-plan saga
  operator-profile maximum block/operation metadata and streamed bodies

compensation storm
  deterministic failure near the end of many concurrently applied sagas

unknown-outcome storm
  drop target commit responses after durable commit and require reconciliation

claim churn
  expire claims between every block and move execution tasks across nodes

GC pressure
  replace/delete held object versions while compaction and GC run continuously

blocked compensation
  introduce unrelated head updates and verify holds remain until resolution
```

Release reports MUST include:

```text
operation-record p50/p95/p99
ApplySaga validation p50/p95/p99 by plan size
forward block execution p50/p95/p99 excluding native operation baseline
compensation block execution p50/p95/p99
claim acquisition and handoff latency
outcome reconciliation latency
runnable lag
reference hold acquisition/release latency
active/oldest hold counts
CoreMeta bytes per block/operation/event
number of extra root publications per saga block
```

Suggested initial acceptance targets on the ANVIL-0007 release cluster profile:

```text
saga operation recording overhead p95       <= 2x equivalent staged native write metadata path
claim plus intent overhead p95              <= 25 ms excluding target transaction
receipt plus cursor advancement p95         <= 25 ms excluding target transaction
lost-ack committed reconciliation p95       <= 2 s under healthy quorum
reference hold acquire/release p95          <= 50 ms excluding unavailable target root
runnable lag p95                            <= 1 s at release concurrency
missing required trace/metric series        fail release gate
unreleased holds after terminal tests       zero
incorrect compensation under fault tests    zero
```

These targets may be tightened by later baseline data. A release exception may
change performance thresholds but cannot waive correctness, reference retention,
mandatory compensation, or uncertain-outcome reconciliation.

## 41. Acceptance criteria

ANVIL-0008 is implemented only when all of the following are true:

- ANVIL-0007 remains satisfied except for explicit extensions in this RFC.
- A caller can construct a saga incrementally through native streaming mutation
  APIs without opening target transactions.
- A saga can contain arbitrary ordered roots and revisit roots.
- Every compensatable forward operation requires a same-call explicit
  compensation choice.
- Typed SDKs cannot construct an operation then attach compensation later.
- Raw protocol validation rejects and invalidates structurally ambiguous drafts.
- Operation dispatch and automatic compensation use enums and typed oneofs.
- `ApplySaga` freezes a deterministic immutable plan hash.
- Each forward and compensation block uses one ordinary single-root transaction.
- Any eligible node may claim the next unit, and no node owns the whole saga.
- Commit ambiguity is reconciled before compensation or advancement.
- Versioned object compensation restores the exact prior head conditionally.
- Typed references resolve newly created resources without a general binding
  language.
- Active saga dependencies are protected by owner-addressable reference edges.
- Every terminal outcome releases every saga-owned reference.
- Blocked sagas retain the evidence and holds necessary for safe resolution.
- Recovery works from root anchors, CoreMeta, manifests, and blocks without a
  sidecar task queue or saga WAL.
- Required metrics, traces, dashboards, conformance tests, and performance gates
  pass.

## 42. Implementor checklist

Before adding or enabling a native mutation in sagas, answer in the implementation
PR:

1. Which `SagaOperationKind` enum value identifies it?
2. Which exact deterministic request oneof variant stores it?
3. How is its single target root resolved during draft validation and execution?
4. Which dynamic preconditions remain deferred until execution?
5. Which automatic compensation kinds, if any, are valid?
6. Which pre-state and post-state values must be captured?
7. Which owner-addressable holds prevent those values from being reclaimed?
8. What exact conditional preconditions make compensation safe under concurrent
   writes?
9. What result type and typed saga references may the operation produce?
10. How is an ambiguous invocation reconciled?
11. What authz checks apply at record, forward execution, and compensation?
12. Which trace spans, metrics, conformance tests, and performance scenario cover
    it?
13. Does the native service descriptor coverage test recognise the new method?
14. Does terminal cleanup leave zero saga-owned strong references?
