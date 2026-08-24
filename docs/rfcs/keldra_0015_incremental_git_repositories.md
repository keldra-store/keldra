# KELDRA-0015: Incremental Git Repositories

Status: Accepted architecture

Audience: Keldra implementors, operators, client authors, and reviewers

This RFC defines Keldra's authoritative representation, publication,
materialization, compaction, and serving model for Git repositories. It
replaces the complete-bundle persistence model used by the first Git smart
HTTP gateway. It does not change Git's public smart HTTP protocol or weaken
Keldra's existing authentication, Zanzibar authorization, durability, or
serving-fence requirements.

## 1. Decision

A Git repository is an immutable, ordered sequence of accepted pack and
reference changes published through one small mutable Keldra object. Native
bare Git repositories on serving nodes are disposable materializations of that
authoritative sequence.

There is exactly one mutable authority per repository: its `current` object.
Every pack, push batch, checkpoint, multi-pack index, commit graph, and
compaction output is an ordinary immutable Keldra object. A compare-and-swap of
`current` is the only publication boundary.

Any active node may accept a Git request. Weighted rendezvous hashing selects
preferred nodes for write coordination, compaction, and read-cache affinity,
but no repository assignment or route is persisted. Exact-path Keldra CAS,
logical-record replication, and serving fences remain authoritative.

Raft receives no Git refs, packs, repository paths, serving assignments, push
receipts, or materialization state. It continues to contain only the bounded
cluster decisions defined by the cluster-distribution architecture.

## 2. Goals

1. Make normal push work proportional to the new pack and ref transaction, not
   to the repository's accumulated size.
2. Serve clones and fetches from fast native Git repositories on local storage.
3. Reuse a warm materialization by applying only changes since its generation.
4. Preserve atomic visibility of every Git reference transaction.
5. Serialize publication for one repository without serializing unrelated
   repositories or the cluster.
6. Allow a hot repository to have as many disposable serving materializations
   as the active cluster can use.
7. Perform expensive Git compaction once and distribute its immutable result.
8. Recover from loss of any disposable state using ordinary Keldra reads.
9. Retain Keldra's ordinary placement, erasure coding, reference accounting,
   authorization, accounting, and garbage-collection mechanisms.
10. Support real Git clients without inventing a Keldra-specific Git protocol.

## 3. Non-goals

This design does not:

- store one Keldra object for every Git object;
- perform distributed traversal of Git's object graph;
- make a local Git repository authoritative;
- create a separate Git database, routing catalogue, or assignment registry;
- create a cluster-global ordered Git journal;
- put payloads or repository-cardinality state in Raft;
- rely on public watches for freshness or correctness;
- run compaction independently on every serving node;
- route ordinary pushes through the atomic-program executor;
- preserve or dual-write the complete-bundle storage representation; or
- replace Git's pack validation, connectivity checks, ref rules, or wire
  protocol with hand-written equivalents.

Migration from the complete-bundle representation is an explicit export and
import operation. The implementation does not carry a compatibility reader,
dual writer, or background format converter.

## 4. Vocabulary

**Repository ID** is Keldra's stable internal identifier for one repository.
Mutable tenant, bucket, and repository names never form persistent internal
identity.

**Pack object** is one immutable Git pack stored through the ordinary Keldra
object path.

**Push batch** is an immutable, ordered group of one or more accepted Git push
transactions. It points to its predecessor and the pack objects required by
its transactions.

**Current pointer** is the one small mutable object whose exact-version CAS
publishes a new repository generation.

**Checkpoint** is an immutable complete description of refs and the compacted
pack set at one published generation.

**Tail** is the bounded sequence of push batches after the current checkpoint.

**Materialization** is a disposable native bare Git repository representing
one exact published generation on one node.

**Preferred writer** is the current weighted-HRW rank-zero active node for the
repository execution key. It is an optimization, not persistent authority.

**Serving width** is the maximum number of HRW-ranked active nodes among which
ordinary reads for a repository are distributed. It controls disposable cache
placement and never changes authoritative payload placement.

## 5. Required invariants

1. A push is not visible until every referenced immutable object has received
   the required durability acknowledgement and `current` has been CAS-published.
2. One `current` value identifies one complete ref state. No reader observes a
   partial multi-ref transaction or a partial group publication.
3. Only an exact expected-version CAS may advance `current`.
4. Immutable artifacts never become authority merely because they exist.
5. A local materialization is served only after it is proven to represent the
   `current` version observed for that request.
6. A dropped watch or cache notification cannot cause a stale repository to be
   represented as current.
7. Losing all local Git caches cannot lose acknowledged repository state.
8. A compaction result is invisible until one CAS publishes it.
9. Concurrent writers may waste immutable preparation work, but cannot lose or
   overwrite a committed push.
10. Unrelated repositories never wait on one process-wide Git lock.
11. Repository execution placement never enters Raft and does not become a
    second source of truth.
12. Git identities and Keldra storage identities remain distinct and are both
    verified at their relevant boundaries.

## 6. Authoritative namespace

The protected internal namespace is:

```text
_keldra/git/v2/repos/<repository_id>/
  current
  packs/<pack_id>
  batches/<batch_id>
  checkpoints/<checkpoint_id>
  compacted-packs/<pack_id>
```

`repository_id` is a fixed-width stable identifier. `pack_id`, `batch_id`, and
`checkpoint_id` are content-derived identities encoded canonically for path
use. The external route remains name based:

```text
/git/<tenant>/<bucket>/<repository>.git
```

Normal Keldra name resolution maps that route to stable tenant, bucket, and
repository IDs before accessing the protected namespace.

No object is special to the byte plane. Small values use `small_blobs`; larger
values use the configured complete-copy or erasure-coded path. Artifacts and
their current pointer use normal object APIs and normal distributed placement.

## 7. Records

The following are logical schemas. The implementation uses one canonical,
versioned binary encoding and rejects unknown required fields, invalid lengths,
non-canonical ordering, cycles, and identity mismatches.

```text
GitCurrent {
  format_version
  repository_id
  generation
  checkpoint_id
  tail_batch_id?
  tail_depth
  ref_state_hash
}

GitPushBatch {
  format_version
  repository_id
  parent_batch_id?
  base_checkpoint_id
  first_generation
  pushes[] {
    operation_id
    pack_ids[]
    reference_commands[] {
      ref_name
      expected_old_object_id?
      new_object_id?
    }
    authenticated_principal
    accepted_at
  }
  resulting_ref_state_hash
}

GitCheckpoint {
  format_version
  repository_id
  source_generation
  refs[] {
    ref_name
    object_id
  }
  pack_ids[]
  multi_pack_index_id?
  commit_graph_id?
  ref_state_hash
}
```

References and commands are canonically ordered by ref name. One push entry is
indivisible even when a group contains several independent pushes.

The current pointer remains deliberately small and inlineable. Push batches and
checkpoints are normal objects and do not inherit a metadata-component size
limit. Large records stream through the same ordinary payload path as any other
large object.

## 8. Identity and integrity

Git object IDs retain their repository-selected Git hash algorithm. Keldra does
not reinterpret a BLAKE3 digest as a Git object ID.

Every immutable artifact also has Keldra's ordinary BLAKE3 content identity and
length. The immutable path identity is checked against those bytes before use.
Git independently validates pack structure, its own trailer checksum, object
connectivity, and referenced object IDs.

The two checks protect different contracts:

- Git identity proves Git graph and pack semantics; and
- Keldra identity proves the bytes stored and distributed by Keldra.

No additional SHA or BLAKE pass is added when an existing verified result from
the relevant layer is available.

## 9. Authentication and authorization

Git continues to use Keldra's existing public listener and credential paths.
Basic credentials are exchanged through the normal application credential
flow; bearer credentials use normal Keldra tokens.

Every request is authenticated and Zanzibar-authorized before repository work:

- clone, fetch, and ref advertisement require bucket read permission;
- anonymous reads are allowed only when the bucket owner explicitly grants the
  built-in anonymous principal read access;
- push and receive-pack advertisement require write permission;
- repository compaction, serving-width changes, and destructive maintenance
  require their declared authorized relations; and
- an invalid credential never falls back to anonymous access.

Protected `_keldra` paths remain inaccessible through ordinary user object
operations. Gateway access to those paths is an internal implementation of the
already-authorized Git operation, not an authorization bypass or a separate
credential authority.

## 10. Preferred writer and request routing

Any public node may accept a Git request. It computes:

```text
writer = WeightedHRW(git_repository_execution, repository_id, active_members)[0]
```

If the ingress node is not the preferred writer, it proxies a push once over
the authenticated peer transport. The destination authenticates the forwarded
principal and repeats Zanzibar authorization.

The preferred writer keeps one lazily created per-repository actor. The actor
owns only transient execution state:

- the local publication queue;
- the local materialization update lock;
- group-commit timing; and
- in-flight pack preparation references.

There is no process-wide Git request lock. Actors are bounded and removed after
an idle period when they have no requests or cache handles.

Membership can transiently produce two nodes which believe they should
coordinate. The exact-path owner and serving fence protect the `current` CAS;
only one expected version can win. No extra lease or persisted repository
primary is added.

## 11. Push preparation

The preferred writer performs these steps for each push:

1. Read authoritative `current` and bring its native materialization to that
   exact generation.
2. Advertise refs and capabilities through native Git smart HTTP behavior.
3. Receive the client's commands and pack into Git quarantine.
4. Validate packet framing, pack integrity, object connectivity, ref names,
   expected old object IDs, fast-forward policy, and requested atomic-ref
   behavior using Git's established implementation.
5. Stream each accepted pack to its immutable Keldra path while retaining only
   bounded validation and transport buffers.
6. Wait for the normal artifact durability acknowledgement.
7. Submit the prepared ref transaction to the repository publication queue.

The implementation must not buffer the whole HTTP request, CGI response, or
pack in process memory. Backpressure propagates from local quarantine and the
Keldra upload stream to the client.

A push which only changes or deletes refs may contain no new pack. An invalid
push publishes nothing. An immutable pack which was stored before a later
validation or CAS failure is unreachable and follows the orphan-retention rule.

## 12. Ordered group publication

Publication for one repository is serialized, while pack receipt and validation
remain parallel. The writer opens a short bounded group window and evaluates
prepared pushes in deterministic queue order against a rolling ref map.

For each prepared push:

- every expected old object ID is compared with the rolling current value;
- all commands in that push either pass or fail together when atomic push was
  negotiated;
- a rejected push does not reject independent pushes in the group;
- an accepted push updates the rolling ref map before the next push is checked;
  and
- the writer assigns the next repository generation in accepted order.

The writer then:

1. encodes one immutable `GitPushBatch` containing the accepted transactions;
2. publishes that batch with the artifact durability rule;
3. constructs `GitCurrent` with the new batch, generation, tail depth, and ref
   state hash;
4. calls `PutIfVersion` with the exact `current` version read for the group; and
5. acknowledges each accepted client only after that CAS succeeds.

The successful `current` CAS is the linearization point for every accepted push
in the group. The declared order determines the conceptual point within that
one atomic publication.

If the CAS loses, the writer discards its candidate ref state, reads current,
and re-evaluates each still-valid prepared push. It never blindly retries a
stale pointer body. Conflicting pushes receive an ordinary Git rejection;
unaffected pushes may enter a later group.

The batch window, maximum pushes, and maximum encoded bytes are runtime
configuration. With no queued peer, a push publishes immediately or after only
the configured low-traffic latency bound.

## 13. Artifact acknowledgement

Git artifacts follow the established internal-artifact rule:

- with one ACTIVE node, request `LOCAL` and acknowledge only after that node has
  durably stored the object;
- with at least two ACTIVE nodes, request `REPLICATED`; and
- never publish `current` when the requested evidence is unavailable.

This affects acknowledgement timing only. Inline versus erasure-coded storage,
eventual placement, online cluster growth, and reference accounting follow the
ordinary object rules.

The current pointer itself is mutable logical metadata. It receives the normal
`1/1`, `2/2`, then `2/3` logical-record acknowledgement independently of the
payload acknowledgement choice.

## 14. Read freshness and catch-up

A clone, fetch, or ref-advertisement request binds to one repository generation:

1. Read authoritative `current` through the normal exact-path route.
2. Acquire a shared handle to a local materialization at that exact version.
3. If the cached version matches, serve immediately through native Git.
4. If it is behind, fetch missing push batches backwards from the current tail
   until reaching the locally applied batch, then apply them forwards.
5. If that predecessor is outside retained tail history, materialize the current
   checkpoint and then apply its bounded tail.
6. Only expose the handle after refs, packs, multi-pack index, commit graph, and
   local generation marker agree.

The exact `current` read is the request's freshness proof. A push published
after that read need not alter the request's bound snapshot. A push completed
before the read must be represented.

Public watches and internal notifications may tell a materializer to prefetch a
new generation. They are latency optimizations only. A missed, duplicated, or
out-of-order notification cannot make a stale materialization pass the current
version check.

## 15. Disposable native repositories

A materialization is an ordinary bare Git repository rooted under
`KELDRA_CACHE_DIR`:

```text
git/<repository_id>/
  repo.git/
  applied-generation
  checkpoint-id
  tail-batch-id
```

The marker is written atomically only after the corresponding native repository
state is complete. On startup, a missing, malformed, or mismatched marker makes
the directory disposable; it is removed or rebuilt rather than repaired by
guessing.

Local coordination is per repository:

- read handles share one immutable applied generation;
- one updater obtains an exclusive transition between generations;
- an active handle pins the files needed by its Git subprocess;
- eviction begins only when the handle count is zero; and
- unrelated repositories progress concurrently.

The process-wide Git cache has an operator-configured disk budget. Least
recently used, unpinned materializations are evicted first. Linux page cache and
native filesystem behavior provide memory caching; Keldra does not maintain a
second authoritative in-memory object graph.

## 16. Read serving width

Serving materializations are not payload replicas and are not limited by the
mutable-record replication factor. Every active node is eligible to cache and
serve a repository.

For configured serving width `R`:

```text
candidates = WeightedHRW(git_repository_serving, repository_id, active_members)
             .take(min(R, active_count))
selected   = candidates[hash(request_identity) mod candidates.len()]
```

The ingress node may serve locally when it owns a current warm materialization
inside the selected set; otherwise it proxies to the selected node. A width of
zero means no proactively warm replica: an ingress request may materialize on
demand or proxy to the writer according to cache admission. A width equal to
the active count permits every node to serve the repository.

Serving width is a repository or bucket policy, not cluster membership state.
Changing it affects only future routing and cache warming. Existing caches
remain disposable and expire under normal eviction.

Automatic demand-based width selection is a later optimization. The first
implementation exposes the explicit authorized policy and the metrics needed
to tune it; it does not add a global popularity service.

## 17. Compaction and checkpoints

An immutable push log requires bounded compaction. The preferred writer is also
the preferred compactor. The following independently configurable conditions
may request work:

- tail batch count;
- active pack count;
- uncompacted pack bytes;
- native loose-object pressure;
- elapsed time since checkpoint; or
- an explicit authorized compaction request.

Only one compaction for a repository runs at once on one node. Different
repositories compact concurrently within shared Git CPU, memory, scratch, and
I/O budgets.

Compaction performs:

1. bind a native materialization to source generation `G`;
2. run native Git multi-pack-index and geometric repack operations;
3. write compacted packs as immutable ordinary Keldra objects;
4. write optional immutable multi-pack-index and commit-graph artifacts;
5. write an immutable checkpoint with complete refs and the compacted pack set;
6. reread `current`; and
7. CAS-publish the checkpoint only when `current` still represents `G`.

The new current pointer has an empty tail and retains the same visible ref state
and state hash. No serving node observes an independently published stripe,
pack subset, or checkpoint fragment.

If `current` advanced, the compactor does not block pushes. It abandons the
candidate publication and retries later, optionally applying the short new tail
before producing another candidate. Immutable losing outputs are unreachable
and age into normal cleanup.

Compaction policy should use Git's established multi-pack-index, commit-graph,
bitmap, and geometric repacking facilities where they fit this immutable
publication boundary. Keldra does not invent new pack or graph formats.

## 18. Retention and garbage collection

The Git layer retains obsolete published generations according to all three
operator-configured bounds:

- maximum generations;
- maximum age; and
- maximum retained bytes.

Crossing any bound makes the oldest unpinned generation eligible for cleanup.
An active local read handle does not require remote generation pinning after all
of its files are materialized, but cleanup retains a minimum age window so a
request which has read `current` can fetch that generation's artifacts.

An unpublished immutable artifact is retained for at least the ordinary
unfinished-publication threshold. Repository-scoped cleanup compares immutable
paths with current and retained checkpoint/tail reachability, then deletes only
objects which are both unreachable and old enough.

Cleanup is bounded and resumable per repository. It pages only the protected
repository prefix and persists no global pack inventory. Core object deletion
decrements the existing payload references; zero-count byte-plane storage is
reclaimed by ordinary Keldra garbage collection.

## 19. Unknown outcomes and retry

A client connection may fail after the current-pointer CAS. This is a normal
unknown outcome, not permission to roll back a generation.

Each accepted push receives a server operation identity recorded in its batch.
The server never uses a random retry to overwrite refs. On reconnect, standard
Git negotiation observes the published refs:

- if the desired ref state is already current, no duplicate mutation is
  required;
- if another push advanced a relevant ref, the normal expected-old comparison
  rejects the stale command.

The current pointer and immutable batch chain decide whether a push is
committed. The standard Git protocol does not gain a Keldra-specific receipt or
client-managed idempotency token.

## 20. Failure behavior

| Failure | Required result |
| --- | --- |
| Writer dies while receiving a pack | No publication; unfinished bytes follow ordinary cleanup. |
| Pack is durable but its batch is not | The pack is invisible and later collected. |
| Batch is durable but current CAS does not commit | The batch is invisible and later collected. |
| CAS commits but the response is lost | The generation remains committed and retry observes its refs. |
| Two writers race | Only one exact expected-version CAS succeeds. |
| Notification is lost | The next authoritative current check detects staleness. |
| Local cache is corrupt or missing | Discard it and materialize checkpoint plus tail. |
| Serving node fails during a clone | Client reconnects through normal Git behavior; authority is unchanged. |
| Compactor fails before CAS | The preceding generation remains current. |
| Compactor loses CAS to a push | Push remains current; candidate outputs are invisible. |
| Membership changes | HRW changes preferred execution and cache affinity; no repository record is rewritten solely for routing. |
| Isolated node attempts publication | Serving-fence and exact-path coordination reject it. |

## 21. Accounting

Git gateway accounting records client-visible traffic once:

- received smart HTTP bytes as gateway ingress;
- returned smart HTTP bytes as gateway egress;
- repository logical bytes according to the existing bucket/path accounting
  contract; and
- optional repository-scoped aggregates when explicitly configured.

Internal replication, materializer downloads, checkpoint construction,
compaction rewrites, and cache eviction are operational I/O, not customer
ingress or egress. Their bytes remain observable through internal metrics.

## 22. Observability

Metrics use low-cardinality labels such as operation and phase. Repository,
tenant, bucket, operation, generation, and artifact identities are trace fields,
never metric labels.

Required metrics include:

- Git requests, successes, failures, active requests, and duration by
  advertise/fetch/push operation;
- received and returned client bytes;
- pack validation bytes, duration, and failures;
- immutable pack and batch publication bytes, duration, and failures;
- per-repository actor queue depth aggregated as distributions;
- group size, wait duration, accepted pushes, rejected pushes, and CAS losses;
- current-pointer CAS latency;
- materialization cache hits, misses, active handles, bytes, and evictions;
- catch-up batches, packs, bytes, duration, and fallback-to-checkpoint count;
- serving proxy requests and latency;
- compaction input packs/bytes, output packs/bytes, duration, CAS loss, and
  failure;
- current tail-depth and checkpoint-age distributions; and
- unreachable artifact bytes awaiting the retention threshold.

Traces cover authentication, authorization, routing, current read, cache
acquisition, pack receipt, native Git validation, artifact durability, group
admission, CAS publication, catch-up, native Git serving, compaction, and
cleanup. Logs identify actionable failure boundaries without printing
credentials, pack contents, private refs, or tenant data.

## 23. Resource governance

Git uses separate process-wide budgets for:

- concurrent pack validation;
- group-publication queues;
- compaction CPU and memory;
- scratch bytes;
- materialized repository cache bytes; and
- concurrent native Git subprocesses.

Budgets are shared across all repositories so one repository cannot allocate a
per-repository maximum repeatedly. Active work may borrow unused capacity only
through the existing process-wide resource accounting; configured total
ceilings remain hard.

When a budget is exhausted, ingress applies backpressure or returns the declared
resource-exhausted result before accepting unbounded work. It does not silently
drop a prepared push, truncate authoritative history, or expose an incomplete
generation.

## 24. Public protocol behavior

The existing smart HTTP routes and ordinary Git CLI behavior remain the public
contract. Clone, fetch, pull, push, ref deletion, tags, branches, force-push
policy, and negotiated atomic push are qualified through the standard Git CLI.

The incremental representation is internal. Clients do not receive Keldra
artifact paths, generation manifests, pack placement, materialization tokens,
or cache-routing responsibilities.

The gateway streams request and response bodies end to end. It does not impose
the old object-maximum request buffering behavior on Git protocol traffic.
Protocol and operator resource bounds remain explicit and are enforced through
backpressure rather than whole-body buffering.

## 25. Scale model

One repository necessarily orders conflicting reference mutations, but pack
receipt and validation are parallel and one current-pointer CAS may publish
several accepted pushes.

For average group size `B` and current-pointer CAS latency `L` seconds, the
metadata-publication ceiling is approximately:

```text
pushes_per_second = B / L
```

At a 10 ms CAS, groups of one, four, and eight correspond to approximately 100,
400, and 800 push publications per second before pack-validation and payload
bandwidth costs. This is a planning model, not a performance claim.

Across repositories, current pointers are independent exact paths and their
coordinators distribute through weighted HRW. There is no cluster-global Git
publication lock or WAL pointer.

For reads, an unchanged warm repository performs one small authoritative
current check followed by native local Git work. Increasing serving width adds
independent disposable execution replicas without multiplying authoritative
logical records or changing pack placement.

## 26. Qualification gates

The implementation is not described as horizontally scalable until all of the
following pass against the public listener and standard Git CLI:

1. One hot repository sustains at least 350 acknowledged small pushes per second
   with durable pack artifacts and replicated current publication.
2. Group sizes of one, four, and eight expose the CAS and latency curve.
3. Read tests use 1, 3, 10, 30, and 100 serving materializations and identify
   the first external bottleneck rather than claiming unlimited linearity.
4. Cached current checks meet a recorded latency budget and never serve a
   generation older than the request's authoritative read.
5. At least 10,000 repositories make concurrent progress without a global lock
   or persisted routing catalogue.
6. Same-ref, different-ref, ref-deletion, tag, force-policy, and negotiated
   atomic pushes preserve native Git results.
7. Process termination is injected before and after pack durability, batch
   durability, current CAS, local materialization, and compaction publication.
8. A node losing its complete cache reconstructs from checkpoint plus tail and
   produces the exact refs and reachable Git objects of the source generation.
9. Membership changes during push, read catch-up, and compaction preserve the
   exact-path CAS and serving-fence invariants.
10. Cache eviction under active readers never removes their files and remains
    inside its configured steady-state budget after handles close.
11. Unreachable pack and batch cleanup cannot remove current or retained
    generation data.
12. Authentication and Zanzibar red-team cases prove private-by-default reads,
    explicit anonymous access, tenant isolation, and write denial.

Every result records source revision, Keldra topology, durability request,
repository shape, pack sizes, object counts, CPU, memory, disk class, network,
cache state, group configuration, and observed bottleneck.

## 27. Implementation sequence

The minimum coherent implementation lands in this order:

1. canonical v2 current, pack, batch, and checkpoint records;
2. immutable artifact publication and exact current-pointer CAS;
3. per-repository actors and removal of the process-wide Git lock;
4. streaming native Git quarantine and pack validation;
5. durable local materialization with exact generation checks;
6. incremental batch catch-up and checkpoint fallback;
7. ordered group publication;
8. HRW write proxying and configurable serving width;
9. single-owner native Git compaction and CAS checkpoint publication;
10. bounded retention, orphan cleanup, accounting, and complete observability;
11. correctness, failure, and scale qualification from section 26.

Steps are not independently advertised as completing this RFC. Until current
publication, materialization freshness, and recovery are proven together, the
existing gateway remains a small-repository capability rather than claiming
the guarantees defined here.

## 28. Result

Keldra stores Git's durable truth as immutable ordinary objects and one small
CAS-published pointer. Native Git repositories become fast, disposable views;
weighted HRW supplies deterministic execution affinity; group publication
amortizes the only serialized metadata operation; and one compactor's immutable
outputs are reused by every serving node.

The design introduces no new authority, storage plane, consensus path, routing
database, or client protocol. It composes Keldra's existing object, CAS,
durability, placement, authorization, cache, and garbage-collection primitives
around Git's established local algorithms.
