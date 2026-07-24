---
title: Watch and Derived Maintenance
description: Operate watch consumers, cursor checkpoints, derived indexes, projections, routing records, authz views, and rebuild/repair workflows without losing source truth.
---

# Watch and Derived Maintenance

Anvil stores source records first. Object versions, bucket metadata, append-stream records, authz tuples, index definitions, PersonalDB commits, Git source records, routing records, and host-alias descriptors are the evidence that something happened. A derived view is anything built from that evidence: a path index, full-text segment, vector segment, typed JSON field segment, authz userset index, PersonalDB projection, routing projection, package catalogue, static-site inventory, export ledger, or cache.

Watches are how those derived views avoid rescanning everything on every restart. A watch asks for committed changes after a cursor. A consumer reads the events, updates its own durable output, and stores a checkpoint only after that output is safe. If the consumer crashes, the checkpoint tells it where to resume. If the checkpoint lies, the derived view can skip source records forever.

Read this chapter with [Watches and Derived Data](/learn/watches-and-derived-data/), [CoreStore](/learn/corestore/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Indexes and Query](/learn/indexes-and-query/), [Authorisation](/learn/authorisation/), [PersonalDB](/learn/personaldb/), and [Observability](/operators/observability/). The hands-on companion is [Watches](/tutorials/watches/). Related operator chapters are [Index Operations](/operators/index-operations/), [PersonalDB Operations](/operators/personaldb-operations/), [Gateway Operations](/operators/gateway-operations/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), and [CoreStore Operations](/operators/corestore-operations/). Command syntax lives in [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## The source-to-derived contract

A watch consumer has a simple contract, but the contract is strict. It starts from a durable checkpoint, opens the relevant watch after that position, processes each event idempotently, writes derived output durably, and then advances the checkpoint. Receiving an event is not enough. The checkpoint must mean "all derived work through this cursor is durable".

That order handles restarts safely. If the process crashes after writing the derived output but before checkpointing, it may process the same event again. That is acceptable when the derived write is idempotent: use a stable key derived from the source cursor, source version id, tuple revision, log index, or mutation id; use compare-and-swap where a manifest must move once; use idempotency keys for side-effect records. If the process checkpoints first and then crashes before output is durable, Anvil cannot infer the missing work from the checkpoint. The source record still exists, but the consumer has told itself to skip it.

The cursor is scoped to one watch family. An object-prefix cursor is not an authz revision. An index-partition cursor has low and high parts. A PersonalDB projection cursor is not the same as the source database log index. Store the full position exactly as returned, with tenant id, stream family, bucket or namespace, prefix or partition, index name or database id, consumer id, software version, and the time the checkpoint was written. That extra context prevents an operator from resuming the wrong stream during an incident.

The API responses often carry a `WatchEventEnvelope`. That envelope is common evidence: watch stream id, partition family, partition id, cursor range, mutation id, record kind, object reference, authz revision, index generation, PersonalDB log index, payload hash, and emitted time where the source path supplies those values. The current CLI prints compact human-readable lines. Production consumers that need complete evidence should use the API directly.

## Replay, tail, and retained windows

Most watches behave as replay followed by tail. The server first reads retained durable events after the supplied cursor. Then it streams or polls newer events as they arrive. `ObjectService.WatchPrefix`, for example, reads an object-watch snapshot and then listens for live object events. `IndexService.WatchIndexPartition` reads durable partition watch events and polls for more. `PersonalDbService.WatchPersonalDbGroup` reads retained group events and then tails live group updates.

Operators should treat both styles the same at the checkpoint boundary. If a connection drops, reopen from the last durable checkpoint, not from the last event printed to a terminal. If a live broadcast reports that the receiver fell behind the retained live event window, stop consuming that stream and replay from the durable checkpoint. That error means the connection cannot prove it delivered every live event; it does not mean the committed source records vanished.

The current watch implementations commonly read an initial batch of up to 1000 events. That is enough for ordinary tailing and many restart windows, but it is not a promise that a consumer can stay offline indefinitely and recover in one streaming call. If a checkpoint is too old, if replay falls behind retention, or if a single poisoned event blocks progress for too long, use a rebuild or repair path from source records.

## Public watch surfaces today

The API exposes more watch families than the public CLI. Do not invent commands to fill the gaps.

| Source or derived family | API surface | Current CLI helper |
| --- | --- | --- |
| Object changes under a prefix | `ObjectService.WatchPrefix` | `anvil watch prefix` |
| Bucket metadata and policy changes | `BucketService.WatchBucketMetadata` | API only today |
| Append stream history | `ReadAppendStream` and `TailAppendStream` | `anvil stream read` and `anvil stream tail`, not `anvil watch` |
| Authz tuple log | `AuthService.WatchAuthzTupleLog` | `anvil watch authz` and `anvil authz watch` |
| Authz namespace/schema invalidation | `AuthService.WatchAuthzNamespace` | API only today |
| Authz derived-userset lag | `AuthService.WatchAuthzDerivedLag` | API only today |
| Index definitions | `IndexService.WatchIndexDefinition` | `anvil watch index-definition` |
| Index partitions | `IndexService.WatchIndexPartition` | `anvil watch index-partition` |
| PersonalDB group commits | `PersonalDbService.WatchPersonalDbGroup` | `anvil watch personaldb` and `anvil personaldb watch` |
| PersonalDB projection progress | `PersonalDbService.WatchPersonalDbProjection` | API only today |
| Git source ingestion/index events | `GitSourceService.WatchGitSource` | API only today |

There is no generic public CLI command that stores watch checkpoints for every stream. The task-lease CLI can checkpoint leased task progress, and mutation batches can include lease checkpoint operations, but that is not a universal watch checkpoint service. Application workers must persist their own checkpoints in a durable place they control, or use a feature-specific Anvil API that exposes the checkpoint they need.

## Runbook: prove an object-derived worker can resume

For an object-derived worker, the source stream is normally an object prefix. The CLI is useful for a manual smoke test:

```bash
anvil --profile acme watch prefix documents incoming/ --after-cursor 0
```

This opens `ObjectService.WatchPrefix` for the `documents` bucket and `incoming/` prefix. A successful stream proves the public endpoint is reachable, the profile can authenticate, the caller has `object:list` on the bucket, the bucket exists, and Anvil can deliver retained object-watch events followed by live events. It does not prove the worker has stored a durable checkpoint, that downstream output is idempotent, or that an index has consumed the same object events.

In production, the worker should read its checkpoint before opening the watch. A tutorial might keep the value in a local file; an application should use durable storage. The safe loop is:

```text
read checkpoint for tenant/bucket/prefix/consumer
open WatchPrefix after that cursor
for each event:
  derive output using source version, mutation id, or cursor as an idempotency key
  commit output durably
  checkpoint the event cursor after the output commit succeeds
```

If the process restarts, it may replay the last event. That is expected. The operator question is not "did it repeat work?" but "did the repeat write produce the same durable state, and did the checkpoint advance only after that state existed?".

## Runbook: distinguish stale search from no data

Search indexes are derived from object source records. A user may upload a document and then query a typed, full-text, vector, or hybrid index before the builder has published a segment containing that document. For metadata-backed and typed JSON indexes, the query surface can prove catch-up to a source cursor.

```bash
anvil --profile acme index query documents invoices_by_due \
  --typed-predicates-json '[{"field":"state","op":"eq","value":"open"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"}]' \
  --require-caught-up-to-watch-cursor 12345 \
  --limit 20
```

This asks the index service to answer only if the materialised segment has applied at least object watch cursor `12345`. A successful result proves the query was authorised and that the segment used for this typed query had caught up to that cursor. It does not prove later writes are included. If the service returns `IndexLagging`, that is useful evidence: show an indexing state, retry within a bounded workflow, or investigate worker lag instead of returning an empty result as if no invoices existed.

Current direct full-text, vector, and hybrid query paths do not expose meaningful source-cursor catch-up. For those, use operational evidence: index partition watches where available, index diagnostics, expected build delay, and user-facing "indexing" states for fresh uploads. [Index Operations](/operators/index-operations/) documents those limits in more detail.

## Index maintenance

Index maintenance uses several watch-like signals. Definition watches tell you that an index was created, updated, disabled, or dropped. Partition watches tell you that a derived partition published a generation with source cursor, proof hash, segment hashes, and authz revision. Query responses can tell callers whether a particular metadata-backed or typed JSON answer is caught up.

The definition watch is a safe operator probe:

```bash
anvil --profile acme watch index-definition documents --after-cursor 0
```

This proves the caller has `index:watch` on the `documents` bucket and can see definition lifecycle events. It does not prove any index segment is current. Partition watches are more specific and require a partition id from an index worker, manifest, or API response. Do not guess a partition id in a runbook; record it where your builder publishes generation evidence.

If a source record repeatedly fails extraction, the index builder should record diagnostics rather than advancing silently. The public diagnostic helper reads those records:

```bash
anvil --profile acme index diagnostics documents invoices_by_due --severity warning --page-size 50
```

This proves diagnostics can be read for that bucket/index. It does not prove the index is healthy. A page with no returned diagnostics can still miss records because the selector is wrong, the caller lacks visibility, the builder is behind, or diagnostics are outside the requested severity/page. Use diagnostics together with source proof and lag evidence.

When derived state is suspect, rebuild from source instead of hand-editing index artefacts:

```bash
anvil --profile acme repair run index documents invoices_by_due --rebuild
```

This asks the public repair service to rebuild that derived index from committed source records. It does not fix malformed source objects, missing embedding providers, public policy grants, or object visibility. After repair, rerun the original query and diagnostics, then check lag again.

## Routing, gateway, package, and static derived state

Not every derived view has a tenant-facing watch command. Mesh routing projections, bucket locators, host aliases, and gateway routing decisions are operator surfaces. They derive from topology, bucket, host-alias, and lifecycle records; they should be inspected through admin diagnostics, routing commands, and gateway logs rather than through a made-up public watch.

For a wrong-region or host-routing incident, start with read-only diagnostics:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --limit 50
```

This proves the private admin listener is reachable and the caller can view mesh diagnostics. It does not repair routing records, prove a gateway reverse proxy is configured correctly, or prove tenant object bytes are present. If diagnostics point at a repairable mesh routing projection, run the targeted admin repair flow documented in [Repair and Diagnostics](/operators/repair-and-diagnostics/) and keep the audit reason specific.

Package and static-hosting workflows are usually built from ordinary objects, links, metadata, public-read policy, indexes, and gateway records. A package catalogue can watch object prefixes for new artefacts and use typed indexes for version metadata. A static-site publisher can watch content prefixes, but link changes and host-alias lifecycle do not currently have a dedicated public CLI watch. Where a workflow needs those events, use the supported API surface or a repair/listing diagnostic loop rather than direct storage scans.

## Authz-derived maintenance

Relationship authorisation has its own source stream: the tuple log. Consumers that cache allowed objects, precompute usersets, export relationship changes, or invalidate search visibility must treat authz revisions as first-class source positions.

```bash
anvil --profile acme watch authz document --after-revision 0
```

This calls `AuthService.WatchAuthzTupleLog` for the `document` namespace. It proves the caller has `authz:watch` for that namespace and can read tuple-log changes from the tenant scope used by the CLI. It does not watch schema binding changes, namespace invalidation, or derived-userset lag. Those API surfaces exist today, but the current public CLI exposes only tuple-log watching.

The checkpoint for an authz consumer is a revision, not an object cursor. A cache that removes access after a tuple revoke should not report itself caught up merely because an object watch advanced. If a derived userset is stale or damaged, use authz derived diagnostics and repair surfaces; do not patch the userset artefact by hand.

## PersonalDB projections

PersonalDB group commits are source records for local-first replicas and projection builders. The group watch tells you that the committed head moved:

```bash
anvil --profile acme watch personaldb customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

This calls `PersonalDbService.WatchPersonalDbGroup` for `customer-notes` in the authenticated tenant. It proves the caller can authenticate, belongs to the requested tenant, and is authorised to watch that PersonalDB group. It does not prove a projection has caught up, that every client replica has synced, or that row-level effects matched product intent.

Projection watches exist in the API as `WatchPersonalDbProjection`, but the current public CLI does not expose a projection-watch command. Operators should therefore combine group-watch evidence, projection reads, repair findings, and application-side checkpoints when diagnosing a stuck projection. If the log chain itself is suspect, use the PersonalDB log-chain repair surface rather than trying to reconstruct a projection from local client state.

## Append streams, audit exports, and external side effects

Append streams are already ordered histories. They use sequence numbers and the `ReadAppendStream`/`TailAppendStream` API rather than the generic `anvil watch` command family. The CLI helper is still useful for manual tailing:

```bash
anvil --profile acme stream tail documents audit/events STREAM_ID --from-sequence 0
```

This tails records from an append stream starting at sequence `0`. It proves the caller can authenticate, read that stream, and receive ordered stream records. It does not prove an external audit sink, SIEM export, webhook sender, or package registry mirror has durably processed those records.

External side effects make checkpointing harder. If a consumer sends an event to a third-party system, decide what makes the output durable: an accepted message id, an idempotent upsert in the sink, an Anvil append-stream receipt, or a manifest update. Store the source checkpoint only after that durable evidence exists. If the external sink is down, stop or apply backpressure. Do not keep advancing checkpoints just because Anvil continues to deliver events.

## Poisoned events and backpressure

A poisoned event is a committed source record that a consumer cannot process with its current code or configuration. Examples include invalid JSON for a typed index, a missing required metadata field, a vector with the wrong dimension, an unavailable embedding provider, an authz schema change that invalidates a cached userset, or a PersonalDB changeset that a projection definition cannot apply.

The wrong response is an infinite retry loop that hides the error while lag grows. The better response is to record a diagnostic or repair finding with the source cursor, stop or quarantine the affected partition, and leave the checkpoint at the last safely processed cursor unless the quarantine itself is durable and intentionally part of the derived output. That makes the incident visible and replayable.

Backpressure is the normal state when downstream work is slower than source writes. Bound queues, limit concurrency, and prefer batching that preserves idempotency. If a consumer falls behind the retained live watch window, reopen from the last durable checkpoint. If the backlog is too large for incremental replay, run a rebuild or repair from source records and publish a new checkpoint only after the rebuilt derived state is verified.

## Rebuilds and repairs are part of the model

A watch-only mental model is too narrow. Initial builds often read source records in bulk. Repairs rebuild derived views from source when a proof, segment, projection, userset, directory index, or routing projection is missing or inconsistent. Some current services poll durable watch records rather than streaming every update through an in-memory live channel. Some specialised surfaces, such as Git source queries, can rebuild a latest index from a manifest when needed.

That is not a failure of the model. The boundary is source truth. Incremental watches are the efficient path while checkpoints are trustworthy. Rebuilds and repairs are the recovery path when checkpoints are stale, derived evidence is corrupt, or a consumer fell too far behind. Neither path should write source records that were never accepted by the source service.

A focused repair should name the derived target and keep audit evidence. For example, rebuilding one tenant index through the public plane is different from repairing a system routing projection through the private admin plane. In both cases, diagnosis comes first, repair is scoped, and verification uses the original read/query/watch symptom.

## Operating signals

For each consumer or derived system, keep a small set of signals visible:

| Question | Evidence |
| --- | --- |
| What source is being consumed? | Tenant, stream family, bucket/namespace/database/index, prefix or partition id. |
| Where is the consumer? | Last durable checkpoint, latest source cursor, lag count or age, last successful apply time. |
| What did it publish? | Derived generation, manifest hash, segment hashes, projection log index, output id, or external receipt. |
| Why did it stop? | Last error, diagnostic cursor, poisoned event id, repair finding, lease/fence state. |
| Who may see it? | Public policy scopes, relationship authorisation revision, authz-derived lag, object visibility mode. |

Current observability defines names such as `watch_stream_lag`, `full_text_indexing_lag`, `vector_indexing_lag`, `authz_derived_index_lag`, `personaldb_projection_lag`, and `repair_findings`, but export and dashboards are deployment work. Do not assume a turnkey metrics endpoint or dashboard exists in the current repository.

## What to take forward

Operate watches as correctness machinery, not convenience streams. Checkpoints are durable claims about completed work. Idempotency makes replay safe. Lag tells you how far derived state is behind source truth. Poisoned events need diagnostics, not silent skips. Rebuilds and repairs are valid recovery tools when they rebuild derived state from committed source records. The API currently exposes more watch detail than the CLI, so production workers should use API surfaces when they need full envelopes, split cursors, projection watches, authz namespace or derived-lag watches, bucket metadata watches, or Git source watches.

## Cursor evidence

Every derived maintenance incident should name the source cursor, applied cursor, checkpoint owner, and fence token. Without those values, it is hard to distinguish a slow builder from a stuck builder or a stale owner from a healthy takeover.

If a consumer cannot resume because its cursor is invalid or too old, use the feature-specific rebuild path. Do not advance the cursor manually to make lag disappear; that only hides skipped work.
