# Anvil 0.5.x known limitations

## Request deadline coverage in 0.5.2

Index and PersonalDB unary requests use one absolute deadline: the shorter of
the client `grpc-timeout` and the startup-configured 30-second maximum. That
same remaining budget is propagated across object and peer calls. The maximum
is deliberately not a transport-wide timeout because `Put` and `WatchPrefix`
are long-lived streams. Local authorization, administration, and credential
unary requests still rely on their client or external TLS terminator to supply
a deadline in 0.5.2; extending the shared deadline wrapper to those existing
services is deferred.

## Minimum PersonalDB transport in 0.5.2

PersonalDB 0.5.2 runs the canonical PersonalDB v0 server state machine on the
weighted-HRW primary for each database group. Its manifests, heads, log
entries, certificates, payloads, and snapshots are ordinary Anvil objects
below `_anvil/personaldb/v0/`; they use the normal inline or erasure-coded byte
path and `REPLICATED` acknowledgement. There is no PersonalDB side store.

The public session transport is a unary gRPC exchange carrying PersonalDB's
canonical JSON `WireFrame`. The separate `GrantLeaderLease`,
`RenewLeaderLease`, and `WitnessCommit` RPCs expose the canonical v0
coordinator boundary with PersonalDB's own JSON types; Anvil does not maintain
a second protocol schema. The exchange returns every response and notification
frame produced for the initiating request in order. It does not yet retain a
connection-level session directory across calls or push a broadcast to other
connected clients. Clients can use normal catch-up requests to observe later
commits; live cross-session subscription delivery is deferred.

Every database-scoped exchange, catch-up, lease operation, and commit witness
hydrates the predecessor-linked committed log from ordinary Anvil objects
before it acts. Membership and the latest lease authority are retained together
as one ordinary replicated object. A restart on the same primary restores that
authority before granting, renewing, or witnessing. When HRW moves the group to
another primary, the old authority is restored only as a monotonic floor: the
former primary's lease cannot be renewed or used there, and a fresh grant
advances the client-log epoch.

The minimum coordinator boundary relies on the active server's exact lease ID,
placement epoch, client-log epoch, lease generation, expiry, membership, and
predecessor-linked committed head. Witness certificates are signed with the
production Ed25519 witness key and verified during hydration. PersonalDB v0
leader leases and voter acknowledgements themselves are not signed artifacts
in this release, so 0.5.2 claims the configured single-client
`StrictWitnessed` profile, not a cryptographic multi-client quorum protocol.

Authorization is fresh Zanzibar evaluation in the caller's tenant realm named
`personaldb`, using the PersonalDB v0 group permissions (`open`, `sync`,
`witness_sensitive_submit`, `snapshot`, `attach`, and `administer`). Each check
accepts either an exact
`database_group:<stable bucket id>:<canonical database id>` grant or the
tenant-wide `personaldb_tenant:<stable tenant id>` authority installed for the
tenant owner during provisioning. The latter lets the owner create and
administer new groups without pre-registering every database ID; applications
can later receive narrower exact-group grants. A remotely routed request is
checked at ingress and checked again at its mTLS destination. Because the
canonical artifacts use the ordinary object pipeline, the caller also needs
the corresponding object permissions on the selected bucket in this release.
Row/resource changeset authorization beyond the v0 group-level hook is not
added by Anvil 0.5.2.

Every request names the bucket holding that database group's artifacts. Anvil
does not add a separate authoritative database-to-bucket registry in 0.5.2.
The same canonical database ID in another bucket is an independent group with
independent placement, artifact history, and exact Zanzibar grants because its
stable bucket ID is part of each identity.

The production witness key is deterministically, domain-separately derived
from the cluster-wide JWT signing secret whose fingerprint is already fenced
in Raft. JWT signing-key rotation is not exposed in 0.5.2; a future rotation
capability must retain the former PersonalDB public trust record while stored
certificates can still reference it.

The canonical PersonalDB v0 committed log contains an opaque SQLite changeset,
its content metadata, hashes, log position, and certificate. It does not expose
canonical row metadata or bind a row to an ordinary Anvil source object path
and version. Anvil does not parse those opaque changesets or invent a second
row feed in 0.5.2. Creating or updating a PersonalDB row-metadata index is
therefore rejected as `UNIMPLEMENTED`; its retained engine format requires a
non-zero ordinary source object path and version before any result can cross
the public Zanzibar authorization boundary.

## Minimum index engines in 0.5.2

The first 0.5.2 index engines intentionally provide a small, usable query
surface rather than every optimization commonly found in a dedicated search
server:

- full-text search has one bounded Unicode-aware lowercase tokenizer, phrase
  positions, and BM25-style ranking; it does not yet provide language-specific
  stemming, fuzzy matching, synonyms, or configurable token filters;
- vector search is an exact page-streamed scan rather than HNSW, so it is
  correct for small indexes but query cost grows linearly with indexed vectors;
- hybrid search evaluates the complete full-text and vector candidate sets
  before deterministic weighted fusion; it has no threshold-pruning algorithm;
- Git-source and tensor/model indexes provide minimum manifest projections;
  Git supports exact and ordered tree lookup, while Tensor supports exact
  `(model_id, tensor_name)` lookup. Tensor input objects must contain one
  `TensorRecord` JSON value or an array of them, including the ordinary source
  object path and version used for result authorization. The retained Hugging
  Face manifest format has no public index definition or gateway in 0.5.2;
- typed JSON and metadata predicates use paged ordered postings without a
  compressed-bitmap accelerator in this release.

These are performance and feature limits, not weaker visibility semantics. A
query returns the latest published generation available to the serving node
together with freshness metadata describing its source checkpoints and known
lag. Before the first generation is published, it returns empty results with
generation `0`, `initial_build_complete=false`, and `rebuilding=true`. Index lag
never changes an otherwise valid query into an error; the client decides
whether the returned generation is fresh enough for its use.

Generation construction currently holds the selected source-object projection
and newly encoded engine files in builder memory. Source invalidations are
collected once per node and shared across local builders, but an affected index
is rebuilt as a complete immutable generation rather than incrementally
compacted. This favors the intended small and medium indexes; very large
indexes can create substantial temporary builder memory and write load.
Logical index files use a fixed 4 MiB segment target in 0.5.2. The cache has an
internal bounded prefetch operation, but format-specific read hints and writer
seal-boundary hints are not yet part of the engine interface.

The memory-cache budget is configured as a percentage in 0.5.2; there is no
absolute-byte memory option or separate materialisation-concurrency setting.

The disposable source-router history is fixed at 1,024 complete barriers or
1,000,000 changes per node in 0.5.2. A builder that falls behind those bounds,
or observes a source epoch or membership change, performs a fresh current-head
scan. This does not lose acknowledged changes, but catch-up can be expensive.
The minimum clustered runtime starts this node-level router on every ACTIVE
node, even when that node currently owns no index builder, so empty or lightly
indexed clusters perform more source-tail polling than necessary. The router
still reads each source only once per node and shares batches among that node's
builders; assignment-aware suspension is deferred as an optimization.

Initial index-router readiness requires a clear checkpoint from every ACTIVE
source. A restarting node therefore keeps its public listener closed while an
ACTIVE peer is unreachable, even if the remaining Raft voters otherwise have
quorum. Nodes that were already serving continue to serve their last published
index generations with freshness evidence. Relaxing cold-start discovery
without weakening checkpoint evidence is deferred.

Concurrent cold misses for the same immutable index segment are not yet
coalesced, so simultaneous queries can fetch the same bytes more than once.
The content hash and atomic cache-file installation keep the result correct.
Cache files left by a prior process are verified and admitted lazily when
reused; the 0.5.2 cache does not eagerly inventory those files at startup, so
the configured disk budget is restored as entries become known rather than by
one cold-start sweep. Cache eviction runs on cache activity rather than an idle
timer; if the last pinned handle is dropped while the cache is above budget,
the next cache access restores the configured bound.

Obsolete generation objects are deleted oldest-first under the configured
count, age, and authoritative-byte caps. An in-flight query remains safe through
the ordinary blob inactivity window, which is at least 24 hours while the
server request deadline is at most 30 seconds; 0.5.2 does not add a separate
distributed generation-lease protocol.

Index definition create, update, and delete use ordinary `LOCAL`
acknowledgement and then the normal metadata replication path. Immutable
generation segments, manifests, and current-pointer publication use
`REPLICATED` acknowledgement. Definition requests do not expose a durability
selector in 0.5.2.

Definition validation does not yet reject every syntactically unusable or
reserved path prefix. Such a definition is stored but its builder fails closed
without publishing a generation; it cannot expose reserved objects.

Deleting an index definition prevents further queries and builders, but its
already-published generation artifacts are not eagerly removed in this
release; normal blob reference accounting remains correct, and a later
maintenance capability will collect those unreachable index generations.

## Cold index-definition discovery in 0.5.2

Index definitions are ordinary authoritative Anvil objects and there is no
separate registry or index-specific persistence plane. A node without its
disposable assignment cache scans the reserved definition paths at startup,
then applies weighted HRW to determine which indexes it builds or serves. The
cold-start work therefore grows with the number of index definitions. Anvil
0.5.2 accepts that startup cost rather than adding an unmeasured catalogue or
side plane; later releases can optimize discovery without changing the stored
definition format.

## First custom-realm binding in a multi-node 0.5.1 cluster

The first schema binding for a custom Zanzibar realm must atomically create
the realm binding and its protected-system ownership grant. Anvil 0.5.1 keeps
that guarantee on a one-node cluster, but rejects the first binding with
`UNAVAILABLE` when more than one node is active. Existing realms can be
rebound and used normally across the cluster. A later capability must add one
bounded cross-Zanzibar operation before enabling first binding on multi-node
clusters; 0.5.1 does not weaken the atomic ownership guarantee.

The built-in `personaldb` tenant realm added in 0.5.2 is a narrow exception:
tenant provisioning installs its fixed schema, first binding, tenant-owner
tuple, and protected-system ownership grant through a deterministic journaled
bootstrap. This does not provide a generic multi-node first-binding operation
for caller-defined realms.

## Cluster lifecycle operations in 0.5.1

Anvil 0.5.1 supports genesis, authorized node preparation, learner catch-up,
typed ownership handoff, and activation for ordinary cluster formation. The
bounded Raft state machine also validates removal, capacity-reweight, and peer
certificate-overlap transitions, but 0.5.1 does not expose public operations
that start those transitions because their corresponding online ownership
handoff and live TLS-reload orchestration are not yet complete. Operators must
not submit those internal Raft commands directly.

There is no public drain or detailed cluster-health RPC in 0.5.1. Public
listener availability is the readiness boundary: Anvil binds it only after
membership, serving-fence, bootstrap, authorization, atomic recovery, and
ordered reference startup checks complete. Normal process termination stops
the public listener before the peer runtime and flushes the local store; it
does not remove the node from committed membership.

## Transient reference-delivery cursor skew in 0.5.1

Under concurrent cluster activity, ordered reference delivery can transiently
observe a destination cursor one position beyond the source-tail snapshot and
pause that source until a later retry. Anvil fails closed while this condition
exists: blob garbage collection remains disabled and source-journal entries
remain retained, so acknowledged object data is not collected prematurely. A
persistent condition can delay reference-count convergence and eventually
apply bounded-journal write backpressure; it does not weaken `LOCAL` or
`REPLICATED` acknowledgement guarantees.

## Tenant schema catalogue handoff in 0.5.1

Node admission transfers each tenant's complete Zanzibar schema catalogue as
one typed private handoff unit; individual `TenantSchema` records are discovery
keys and are never independently repaired. The 0.5.1 private typed-message
limit bounds one encoded catalogue to 16 MiB. Admission fails closed if a
catalogue exceeds that size. A later release can stream unusually large
catalogues without changing their storage format or quorum semantics.

## Coordinator error detail in 0.5.1

Bulk writes correctly reject failed preconditions, but a per-item failure that
crosses the object-coordinator boundary is reported as `INVALID` rather than
the more specific `CONDITION_FAILED`. Other independent items in the same bulk
request retain their normal success or failure outcomes.

Deleting the current tombstone of a versioned object correctly returns
`FAILED_PRECONDITION` and leaves that tombstone unchanged, but the coordinator
path does not preserve the `CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED` text
prefix. Clients must use the gRPC status code rather than matching that text in
0.5.1.

## Per-object user metadata

Anvil 0.5.0 accepts the bounded `content_type` header but does not accept
arbitrary caller-defined metadata on an object version. Applications that need
descriptive or index input fields must currently carry them in their payload or
in an application-owned manifest.

## Atomic preparation and the blob inactivity clock

Staging an atomic program's prepared output blobs and bundle sets their
ordinary blob `updated_at` timestamps before `CommitBatch` is proposed. An
unusually long delay in staging, waiting for the commit gate, or recovering an
earlier commit therefore reduces the effective post-commit replay and recovery
retention by the length of that delay.

The normal path is expected to take milliseconds. Anvil 0.5.0 does not refresh
the prepared blobs immediately before `CommitBatch`, and it does not add a
special lease, side store, or second lifecycle clock for atomic programs.

## Permanent deletion from versioned program-only paths

A version-enabled `PROGRAM_ONLY` path retains its historical payload versions.
Path policy correctly prohibits the ordinary `DeleteVersion` API from mutating
that path, and the 0.5.0 atomic-program DSL has no operation for deleting one
exact retained version. Operators therefore cannot permanently prune that
history in Anvil 0.5.0.
