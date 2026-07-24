---
title: Observability
description: Build evidence for serving health, data correctness, authorisation, derived lag, gateway behaviour, diagnostics, and audit.
---

# Observability

Observability in Anvil is not just a graph of request counts. It is the evidence that a tenant can write, read, search, sync, publish, and recover data without crossing the wrong trust boundary. A process can be listening on its public port while a search index is stale, a watch consumer is behind, an authz tuple has not reached its derived view, a bucket locator points at the wrong region, or a PersonalDB projection has stopped advancing. Those are correctness incidents even when the HTTP health check is green.

Read this chapter with [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), [CoreStore Operations](/operators/corestore-operations/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), and [Admin Plane](/operators/admin-plane/). The concepts behind the signals are covered in [CoreStore](/learn/corestore/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Indexes and Query](/learn/indexes-and-query/), [Authorisation](/learn/authorisation/), [Gateways](/learn/gateways/), and [PersonalDB](/learn/personaldb/).

## What evidence should answer

A useful Anvil dashboard or incident note should answer four questions.

First, can requests reach the right trust surface? Tenant API traffic, S3/static gateway traffic, admin API traffic, and authenticated internal node gRPC have different security meanings. A public endpoint being healthy does not prove the admin listener is private or that node RPC authorisation works. An admin token working does not prove tenant credentials can use the public API. A gateway responding does not prove native object reads or distributed CoreStore operations are authorised correctly.

Second, did the source record commit? For object storage this means an object version, delete marker, link, bucket record, or metadata update exists as durable CoreStore-backed state. For append streams it means a sequence record is present. For PersonalDB it means a changeset was witnessed and committed into the group log. For task leases it means ownership and fence state changed under the expected principal.

Third, have derived views caught up to that source? Listings, full-text search, vector search, typed indexes, authz usersets, routing projections, repair findings, and PersonalDB projections are derived. They are allowed to lag, but they must expose enough evidence for operators and callers to tell lag from absence. A search page returning no results because the index is an hour behind is an outage, not a valid empty state.

Fourth, can you explain the decision later? Request ids, tenant ids, safe resource identifiers, authz decision context, admin audit entries, diagnostic findings, repair reports, and gateway request ids should let an operator reconstruct what happened without logging object bodies, bearer tokens, client secrets, S3 signing material, or PersonalDB payload contents.

## Current public surfaces and limits

Current source defines an in-process observability catalogue with names such as `object_write_latency`, `object_read_latency`, `prefix_list_latency`, `full_text_indexing_lag`, `vector_indexing_lag`, `authz_derived_index_lag`, `watch_stream_lag`, `personaldb_projection_lag`, `reserved_namespace_rejection_count`, `compaction_backlog`, and `repair_findings`. Some object and reserved-namespace signals are wired in current code. Other names define the signal shape operators should wire as the feature path matures.

Do not assume the repository currently provides a turnkey Prometheus endpoint, a complete dashboard pack, or an alert rule set. Treat the metric catalogue, structured logs, public diagnostics, admin diagnostics, repair findings, audit events, and gateway request ids as the current evidence surfaces. In production you still need to connect process logs and in-process metrics to your telemetry system, choose labels that do not leak tenant data, and build dashboards that match your topology.

The public CLI exposes tenant-facing index diagnostics and repairs. The admin CLI exposes private administrative diagnostics, repair, and audit listing over the admin API. These CLIs are helpers over API calls; they are useful for smoke tests and incident triage, but production applications and automation should call the APIs directly where appropriate. See [Public CLI](/reference/public-cli/) and [Admin CLI](/reference/admin-cli/).

## Request evidence

Every request should be observable as a path through one plane. For native public and admin API traffic, track method, service path, status, latency, request id, API family, caller class, and safe resource fields such as tenant id, bucket name, index name, database id, or host alias when those fields are already part of the operation. Current middleware attaches `x-anvil-request-id` to responses and logs request method, path, and safe header names rather than secret header values. The S3 gateway uses S3-style request ids, including `x-amz-request-id`, because S3 clients expect that shape.

Those ids are most valuable when they join the whole story. A failed index query should be traceable from client request id, to server log, to authz decision, to index diagnostic, and then to a repair or catch-up result. An admin command should carry a request id and an audit reason so that a later audit event can be tied to the change request or incident ticket.

Avoid high-cardinality or sensitive labels. Object keys may contain user content, email addresses, or document titles. Bearer tokens, app secrets, S3 signatures, `x-amz-security-token`, request bodies, and PersonalDB changesets must not become metric labels or log fields. If you need to correlate a specific object during an incident, use a short-lived, access-controlled log query or a hashed identifier rather than a permanent metric label.

## Storage and CoreStore evidence

Storage observability starts with the host volume: free bytes, free inodes, write latency, read latency, filesystem errors, permissions, backup freshness, and restore-drill results. Anvil's local CoreStore backend stores durable state under `STORAGE_PATH`; losing that path can lose object bodies, refs, streams, transaction evidence, tenant credentials, authz state, PersonalDB state, diagnostics, and audit evidence. See [CoreStore Operations](/operators/corestore-operations/) and [Backup and Recovery](/operators/backup-and-recovery/).

Inside the server, the source-level signals are different from ordinary disk metrics. Object write latency tells you whether object versions are committing slowly. Object read latency tells you whether current or pinned reads are slow. Prefix listing latency points at directory or metadata pressure. Ref CAS failures and fenced mutation rejections can be healthy correctness signals: they often mean Anvil rejected a stale writer instead of losing an update. Reserved namespace rejections are security signals because they show a caller tried to use an Anvil-owned key prefix through a tenant data surface.

A storage dashboard should therefore show both capacity and semantic health. Disk space alone cannot tell you that a stream consumer skipped a cursor. A repair finding alone cannot tell you that the durable volume is almost full. You need both layers.

## Authorisation evidence

Authentication answers who the caller is. Authorisation answers whether that caller may do the requested action on the requested resource. In Anvil there are two authorisation shapes to observe: public policy scopes for tenant API principals, and relationship authorisation for realm/schema/tuple checks. They are related in the product, but they are not the same decision.

A useful denial investigation separates at least five cases: no bearer token, invalid bearer token, public policy scope missing, relationship authz check denied, and admin system-realm relation missing. The admin listener also rejects tenant data-plane credentials before method-level system-realm checks, so an admin failure may be a plane misuse rather than a missing tenant permission.

Log denials with request id, plane, action, resource shape, principal id or app id where safe, and whether the decision came from public scopes or relationship authz. Do not log secrets or the full token. Alert on unusual spikes in denials, because they can mean a bad deploy, a rotated credential not updated in clients, an attempted bypass, or an authz derived index that is lagging behind tuple writes.

For the conceptual model and resource names, see [Authorisation](/learn/authorisation/) and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Index and search lag

Indexes turn source object records into queryable views. Path, metadata, typed JSON, full-text, vector, and hybrid indexes each have different extraction logic, but the operational question is the same: what source cursor has the index seen, what cursor has it applied, and did any record produce a diagnostic?

Consider a stale search incident. A user uploads `contracts/acme-2026.pdf` and immediately searches for a phrase from the document. The object read succeeds by key, but full-text search returns no hits. That does not prove the object is missing or that the user is unauthorised. It may prove only that the full-text index has not applied the watch cursor that contains the new version, or that extraction failed and wrote a diagnostic.

The tenant-facing diagnostic command is a safe first check for one index:

```bash
anvil diagnostics list documents body_text --severity warning --page-size 50
```

This reads diagnostics for the `body_text` index in the `documents` bucket using the current public CLI profile. It proves that the caller can authenticate to the public API and read diagnostics for that tenant-scoped index. It does not prove the whole bucket is indexed, that search is caught up, or that another index has no findings.

For searches that must not return stale results, use query surfaces that support `require_caught_up_to_watch_cursor` and a lag timeout. That is a correctness fence between the source write and the derived query. If the query times out waiting for catch-up, treat it as lag evidence rather than silently showing an empty result set. The query JSON and CLI flags are covered in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/) and [Indexes and Query](/learn/indexes-and-query/).

## Watches and derived consumers

Watches are how consumers avoid rescanning source state. A consumer tails object, bucket, authz, index, or PersonalDB watch events, does its own durable work, and then stores the last processed cursor as a checkpoint. The checkpoint is only safe after the consumer's own output is durable. If a consumer checkpoints first and crashes before committing its output, restart can skip source records.

Monitor watch lag as the distance between the latest source cursor and each consumer's durable checkpoint. Also monitor last successful apply time, last error, restart count, and whether a cursor is too old to resume from the retained history. A consumer that restarts from its stored cursor and replays a small range is normal. A consumer that repeatedly falls back to broad rebuilds is either underprovisioned, blocked on errors, or using checkpoints incorrectly.

Derived indexes, routing projections, repair finding builders, audit exporters, and PersonalDB projections are all watch-like from an operational point of view. Their lag is data correctness evidence, not background noise. See [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/) and [Watches and Derived Data](/learn/watches-and-derived-data/).

## PersonalDB evidence

PersonalDB is not a SQL server hosted by Anvil. Anvil stores and authorises committed SQLite changesets, group heads, witness evidence, watches, snapshots, and projections. A PersonalDB deployment can therefore look healthy at the object API while one database group is stuck behind a rejected commit or a projection lag.

For a stuck sync incident, separate client-side retry from server-side evidence. Check whether commits are being rejected, whether the current head advances, whether group watch events advance, whether projection watch events advance, and whether a repair finding exists for the log chain. Current observability names include `personaldb_witness_latency`, `personaldb_commit_rejection_reasons`, and `personaldb_projection_lag`; current repair surfaces include tenant and admin PersonalDB log-chain repair commands.

The tenant-facing repair helper for one database is:

```bash
anvil repair run personal-db customer_notes
```

This asks the public repair service to inspect and repair the PersonalDB log chain for `customer_notes` using the caller's tenant credentials. It proves the caller can reach the public repair API and has enough permission for that database repair. It does not prove every replica client is healthy, that row-level authorisation matched the application intent, or that projections are already caught up after the repair.

For the model and operational boundaries, read [PersonalDB](/learn/personaldb/) and [PersonalDB Operations](/operators/personaldb-operations/).

## Routing and gateway evidence

Gateways are adapters over Anvil's native model. Their observability has to include both protocol-level signals and Anvil-level signals. For S3-compatible traffic, monitor signature failures, access key id where safe, timestamp-window failures, malformed chunked uploads, public-read decisions, reserved namespace rejections, bucket region mismatches, and whether the request reached the expected host route. For static hosting, monitor host alias state, public-read misses, link resolution failures, and cache behaviour at your reverse proxy.

A wrong-region object incident is a routing problem before it is a storage problem. If a bucket's home region is `eu-west-1` and a client sends high-volume S3 traffic to a `us-east-1` endpoint, Anvil may redirect, proxy, reject, or report proxy unavailability depending on the configured cross-region routing policy and current implementation path. Observe bucket locator records, route source, returned region information, gateway response status, and whether generic proxy fallback was used. See [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Topology Planning](/operators/topology-planning/), and [Gateway Operations](/operators/gateway-operations/).

A gateway signature failure has a different shape. A SigV4 mismatch usually means the client signed a different method, path, host, body hash, timestamp, or credential than the gateway verified. Check clock skew, forwarded host and proto handling, whether the reverse proxy is trusted by configuration, and whether the access key was rotated. This proves neither that the bucket is missing nor that public-read is broken; it proves the gateway could not authenticate the signed request as presented.

## Diagnostics, repair, and audit evidence

Diagnostics are read-only findings. Repair is a mutating or rebuilding action. Audit is evidence that a sensitive operation was attempted and how it was authorised. Keep those roles separate during incidents.

The admin diagnostic command is useful when the incident crosses tenant-visible surfaces or derived mesh state:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source index \
  --tenant-id acme \
  --bucket-name documents \
  --severity error \
  --limit 50
```

This proves the private admin listener is reachable, the admin credential can get a token, the caller is authorised to view diagnostics, and the selected diagnostic backend can return findings. It does not scan every CoreStore blob, prove backups are recoverable, or prove unrelated tenants have no findings.

After a repair or sensitive administrative change, audit should answer who, what, when, where, why, and by which relation. The admin audit list command can narrow the evidence:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list \
  --action admin.repair.run \
  --limit 50
```

This reads recent admin audit events for repair runs. It proves that the audit service is reachable and that matching events can be listed by the caller. It does not prove the repair fixed the incident; you still need before-and-after diagnostics, the failing user operation, and lag checks.

Public tenant audit has its own public CLI surface for tenant-owned activity. Use the public plane for tenant-owned evidence and the admin plane for operator evidence; do not use the admin API as a shortcut for normal tenant data operations.

## Incident examples

| Symptom | Evidence to gather | Common conclusion to avoid |
| --- | --- | --- |
| A newly uploaded document does not appear in search. | Object read by key, source watch cursor, index applied cursor, index diagnostics, query catch-up timeout, authz filter decision. | Assuming no search hit means the object is absent. |
| A tenant app gets permission denied on an object it used yesterday. | Request id, token validity, public policy scope, relationship authz tuple/schema revision, object visibility, audit of recent grants or revocations. | Treating every denial as an outage; some denials are correct enforcement. |
| An S3 client receives a wrong-region or redirect response. | Bucket home region, endpoint host, cross-region routing policy, gateway request id, route source, proxy/redirect/reject branch. | Looking for missing object bytes before checking bucket placement. |
| A PersonalDB client keeps retrying sync. | Commit rejection reason, group head, witness latency, group watch cursor, projection cursor, log-chain repair finding. | Blaming SQLite locally before checking server witness and projection state. |
| A gateway reports signature verification failure. | Access key id, clock skew, signed host/path, forwarded host/proto trust, body hash mode, recent credential rotation. | Interpreting signature failure as public access failure or bucket absence. |

These examples are deliberately narrow. They are not complete runbooks, but they show the kind of evidence chain that separates serving failure from correctness failure.

## Dashboards and alerts

Build dashboards around questions, not around whatever counters are easiest to export.

| Question | Signals to show together |
| --- | --- |
| Can tenants read and write? | Public API success rate, latency, object write/read latency, token failures, representative tenant smoke tests. |
| Are source records durable? | `STORAGE_PATH` capacity, filesystem errors, CoreStore read/write errors, ref CAS/fence rejection rates, backup and restore-drill status. |
| Are derived views current? | Per-index lag, watch lag, PersonalDB projection lag, authz derived lag, routing projection diagnostics, last successful apply time. |
| Is authorisation enforcing the model? | Authn failures, public policy denials, relationship authz denials, admin system-realm denials, reserved namespace rejections. |
| Are gateways behaving as adapters? | S3 signature failures, host alias state, wrong-region responses, public-read misses, forwarded-host validation failures. |
| Are repairs safe and auditable? | Diagnostic finding counts by severity, repair runs by kind/scope, audit events with request id and reason, before/after lag. |

Alert on loss of evidence as well as bad evidence. If metrics export stops, logs stop arriving, audit listing fails, or diagnostics cannot be read from the admin plane, the system has lost part of its ability to explain itself. That is an operator incident even if public reads still succeed.

## Current gaps to plan around

Current Anvil source provides building blocks rather than a complete observability product. There is no documented general `/metrics` endpoint, no bundled Grafana dashboard, and no single command that certifies every CoreStore blob, ref, stream, transaction, shard, derived index, routing projection, and PersonalDB log. Some metric names exist before all feature paths emit them consistently.

Administrative diagnostics currently cover specific backends such as index diagnostics and mesh lifecycle/routing projection diagnostics. Tenant-facing diagnostics focus on indexes. Repair commands cover focused targets, and some repairs intentionally rebuild derived state rather than synthesising lost source records. If your incident requires evidence outside those surfaces, document the gap, preserve storage and logs, and avoid direct storage edits.

The practical posture is to wire the current signals you have, keep dashboards honest about what they prove, include request ids in every incident trail, protect logs and audit evidence, and rehearse the difference between stale derived state, failed authorisation, wrong routing, gateway authentication failure, and true source-data loss.

## Useful alert labels

Alerts should include the plane, tenant id when applicable, bucket or index name when applicable, region, node id, command or API family, and last known cursor or generation. Those labels let a responder decide whether to look at policy, routing, derived lag, storage capacity, or gateway translation.

Avoid alerts that only say "Anvil is unhealthy". An object write failure, an index lag warning, a watch checkpoint failure, and an admin routing diagnostic have different owners and different safe actions.
