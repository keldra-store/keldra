---
title: Repair and Diagnostics
description: Use read-only diagnostics and focused repairs to rebuild derived Anvil state from source records without bypassing authorisation, audit, or CoreStore boundaries.
---

# Repair and Diagnostics

Diagnostics and repair are related, but they are not the same operation. A diagnostic is read-only evidence: it says Anvil observed a finding about an index, routing projection, lifecycle record, PersonalDB log chain, or another supported surface. A repair is a state-changing or rebuilding action: it asks Anvil to rebuild a derived view, validate a source chain, or repair a projection from committed source records.

That distinction matters during incidents. If a search index is stale, a directory listing is wrong, a derived authz userset lags, or a routing projection disagrees with control-stream state, repair may be the right tool. If object source records are missing, app secrets cannot decrypt, disk is full, or a PersonalDB commit never existed, repair should not invent truth. Anvil's repair posture is source-first: preserve and validate committed source records, rebuild derived state from them, and verify the original symptom afterwards.

Read this chapter with [Incident Response](/operators/incident-response/), [Observability](/operators/observability/), [CoreStore Operations](/operators/corestore-operations/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [Index Operations](/operators/index-operations/), [PersonalDB Operations](/operators/personaldb-operations/), [Gateway Operations](/operators/gateway-operations/), [Topology Planning](/operators/topology-planning/), [Admin Plane](/operators/admin-plane/), [CoreStore](/learn/corestore/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Indexes and Query](/learn/indexes-and-query/), [Authorisation](/learn/authorisation/), [Admin CLI](/reference/admin-cli/), and [Public CLI](/reference/public-cli/).

## Source records before derived views

Anvil stores source records first. Object versions, bucket records, append-stream records, authz tuples, PersonalDB commits, gateway descriptors, topology lifecycle records, and admin audit events are source evidence. Listings, path indexes, typed indexes, full-text segments, vector segments, hybrid scores, derived userset indexes, PersonalDB projections, routing projections, and caches are derived views.

A diagnostic should tell you which side of that boundary you are investigating. If an object can be read directly but search cannot find it, the source object may be healthy while the search index is behind. If a PersonalDB projection is behind but the group head advances, the source chain may be healthy while derived projection work is lagging. If CoreStore cannot read the object bytes or a PersonalDB log-chain repair reports missing payloads, the response is backup and recovery, not a blind derived rebuild.

Before running repair, capture the failing operation, request id, tenant, bucket or database id, index or derived id, current diagnostic page, relevant watch cursor or revision, and recent audit if an operator changed state. This before-state evidence is what lets you prove the repair did something useful rather than merely consuming capacity.

## Public versus admin surfaces

Anvil exposes tenant-facing repair and diagnostics through the public API, and system/operator repair and diagnostics through the private admin API. Use the surface that owns the problem.

Public tenant diagnostics and repairs are for tenant-owned derived state: a tenant index has diagnostics, a tenant directory/path view needs rebuild, a tenant authz derived userset needs repair, or a tenant PersonalDB log chain needs validation. The caller authenticates as a tenant app and is checked against public policy and relationship authorisation as appropriate. This is the right path when the tenant owns the data and the operation can be expressed safely within that tenant boundary.

Admin diagnostics and repairs are for operator-owned evidence or system surfaces: mesh routing projections, topology lifecycle diagnostics, cross-tenant incident evidence, system-side host alias/routing repair, and admin audit. Admin repair requires a system-realm relation and an audit reason. It should not replace ordinary tenant work. If a tenant can rebuild its own index through the public API, using an admin credential instead hides the tenant authorisation model and weakens the audit trail.

A quick public index diagnostic reads tenant-owned findings:

```bash
anvil --profile acme diagnostics list documents body_text \
  --severity warning \
  --page-size 50
```

This proves the tenant profile can authenticate, has authority to read diagnostics for the `documents/body_text` index, and can receive diagnostic records from the public index diagnostic surface. It does not prove the index is complete, that other indexes are healthy, or that the source object exists.

An admin diagnostic for the same tenant is operator evidence:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source index \
  --tenant-id acme \
  --bucket-name documents \
  --index-name body_text \
  --severity error \
  --limit 50
```

This proves the admin listener is reachable, the caller is authorised by the system realm to view diagnostics, and the selected admin diagnostic backend can return index findings for that tenant and bucket. It does not prove the tenant's own app has permission to read those diagnostics, and it does not repair anything.

## Scope repair narrowly

Repair should be scoped to the smallest source and derived target that explains the symptom. Repairing one index is safer and more explainable than rebuilding every index in a tenant. Repairing one bucket directory view is safer than running unrelated authz and routing repairs. A narrow scope also reduces capacity risk during an incident because rebuild work competes with foreground reads and writes.

A scope needs enough identity to be unambiguous: tenant id or tenant name where the admin API resolves one, bucket name, index name, derived index id, database id, routing record family and key, or repair finding scope id. If you cannot name the scope, keep diagnosing. Do not run broad repair because the symptom is vague.

Every admin repair should carry an audit reason that names the symptom or ticket:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind directory-index \
  --tenant-id acme \
  --bucket-name documents \
  --rebuild \
  --audit-reason 'rebuild documents directory index after diagnostic DIAG-1842'
```

This asks the private admin API to rebuild the directory/path-derived state for the `documents` bucket in tenant `acme`. It proves the operator can run admin repair, the bucket can be resolved, and Anvil attempted the directory repair backend with an audit trail. It does not repair object payloads, restore deleted source records, fix unrelated indexes, or prove the public tenant caller can list the bucket after repair. Verify with the original listing or object navigation path afterwards.

## Directory and path repair

Directory and path repair rebuilds derived navigation state from object source records. It is useful when object reads by exact key work but prefix listings, folder-like navigation, or directory diagnostics show inconsistent counts or hashes. It is not a payload repair and it is not a permission repair.

A tenant can run the public directory repair helper where delegated:

```bash
anvil --profile acme repair run directory documents --rebuild
```

This calls the public repair service for the authenticated tenant and the `documents` bucket. It proves the caller can request a directory repair and that the repair backend can inspect the bucket's source records. It does not prove every object body is readable, every object version is present, or every gateway list path is fixed. Rerun the failing list, directory view, or diagnostic page after the repair.

If the problem appears in mesh-wide diagnostics or during an operator incident, the admin command shown earlier gives system-side audit evidence. Use the admin path when the operator owns the incident; use the public path when the tenant owns the derived view.

## Index repair

Index repair covers tenant query indexes: path, metadata-filter, typed JSON, full-text, vector, and hybrid definitions where the public index service supports diagnostics and repair. Index repair rebuilds or validates derived index state from source records selected by the index definition. It does not change the definition unless a separate update command does that, and it does not fix source objects that fail extraction.

A public index rebuild is narrow and tenant-owned:

```bash
anvil --profile acme repair run index documents body_text --rebuild
```

This asks the public repair service to rebuild `body_text` for the `documents` bucket. It proves the caller can reach repair, is authorised for that index repair, and Anvil accepted the rebuild request. It does not prove the full-text extractor is correct, a vector embedding provider is configured, or `inherit_object` authorisation lets the querying user see every hit.

After repair, rerun diagnostics and the original query. For metadata-backed and typed JSON queries, use catch-up fields where the query surface supports them. Current direct full-text, vector, and hybrid query paths have weaker source-cursor catch-up evidence, so verify them with diagnostics, partition/watch evidence where available, and the actual user query.

Admin index repair is useful for cross-tenant incident response or operator-owned rebuilds:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind index \
  --tenant-id acme \
  --bucket-name documents \
  --index-name body_text \
  --rebuild \
  --audit-reason 'rebuild body_text after stale search incident INC-9201'
```

This proves the repair was requested through the private admin API and recorded with an audit reason. It does not replace the tenant's need to hold `index:read` or object visibility for queries.

## Authorisation derived-state repair

Relationship authorisation source truth is the tuple log and schema/binding records in the tenant's authz realm. Derived userset indexes are maintained views used to make checks and visibility filtering efficient. If tuple facts are correct but derived userset evidence is stale or inconsistent, repair can rebuild the derived userset index.

The public CLI exposes a tenant-facing authz-derived repair target when you know the derived index id:

```bash
anvil --profile acme repair run authz-derived derived-userset-acme-docs --rebuild
```

This proves the tenant app can request repair for that derived userset id. It does not write missing tuples, change schema bindings, evaluate unimplemented caveat expressions, or grant object access. If a user lacks access because the tuple was never written, write the correct tuple through the Authz API instead of repairing the derived view.

The admin equivalent is for operator evidence or system-led incidents:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind authz-derived-index \
  --tenant-id acme \
  --derived-index-id derived-userset-acme-docs \
  --rebuild \
  --audit-reason 'rebuild authz derived userset after tuple lag incident INC-9202'
```

This proves the system principal can run authz-derived repair for that tenant. It does not make tenants able to alter Anvil's built-in system realm. Tenant relationship schemas and tuples remain tenant-owned; system-realm admin relations remain Anvil operator state.

## Routing projections and gateway records

Routing projections turn source lifecycle and descriptor records into materialised routing records: tenant locators, bucket locators, host aliases, and related mesh routing entries. Gateway symptoms often begin here. A wrong-region response, stale host alias, or custom-domain route may be a routing projection problem rather than missing object data.

Start with diagnostics:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --severity warning \
  --limit 50
```

This proves the admin diagnostic backend can return mesh findings. It does not prove S3 credentials are valid, DNS is correct, or public-read is intended. Check gateway logs, host alias state, and bucket locator state before choosing repair.

For one known routing record, the admin routing repair command repairs the materialised record from durable source state:

```bash
anvil-admin --host http://10.10.0.12:50052 routing repair \
  --family host-alias \
  --record-key docs.example.com \
  --audit-reason 'repair host-alias routing projection after diagnostic MESH-311'
```

This proves the operator can request repair for one host-alias routing record and that Anvil can attempt to rebuild it from source control records. It does not create DNS records, issue TLS certificates, make a bucket public, or change tenant object data.

There is also an admin repair backend for mesh routing projections:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind mesh-routing-projection \
  --tenant-id acme \
  --audit-reason 'repair mesh routing projection findings after region incident INC-9203'
```

The current backend diagnoses mesh routing projection records and repairs entries that are marked safe for automatic repair, skipping findings that require operator review. The `--tenant-id` argument is accepted by the generic admin repair CLI shape, but this repair kind is mesh-scoped internally rather than a tenant data repair. It does not complete region activation, complete a drain, or prove every gateway route is healthy. Verify the specific host, bucket locator, or wrong-region request that failed.

Gateway records beyond routing, such as package-foundation records or upload-session concepts, are not exposed as a general gateway repair family today. Treat gateway incidents as native Anvil incidents: inspect source records, routing records, object links, public-read policy, diagnostics, and logs; repair only the supported derived target that explains the symptom.

## PersonalDB repair and projections

PersonalDB repair validates the source log chain for one PersonalDB group. It checks that committed heads, log entries, changeset payload references, certificates, and hashes agree. It does not repair a user's local SQLite file, choose conflict winners, invent missing commits, or guarantee projections are current.

The public helper is tenant-owned:

```bash
anvil --profile acme repair run personal-db customer-notes
```

This proves the tenant app can ask the repair service to check the `customer-notes` group. A response such as `up_to_date` proves the checked source chain is internally consistent through the committed head at the time of the repair. It does not prove every replica has caught up or every projection has processed the latest commit.

An admin PersonalDB repair is useful when the incident is operator-owned or crosses normal tenant support boundaries:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind personaldb-log-chain \
  --tenant-id acme \
  --database-id customer-notes \
  --audit-reason 'inspect PersonalDB log chain after sync incident INC-9204'
```

This records an admin repair action and returns source-chain evidence. It does not give the operator permission to inspect application row contents through a local SQLite shortcut. Use API-level PersonalDB group, catch-up, watch, and projection evidence to verify client recovery.

Current projection repair is more limited. PersonalDB projection watches and projection definitions exist in the API, but there is no broad public CLI command that repairs every projection or proves every projection is caught up. Diagnose projection lag by comparing source group head, target projection group head, projection watch events, and repair findings. If the projection source chain is healthy but output is stale, the fix is usually projection builder catch-up or rebuild logic, not source-chain repair.

## Append streams and audit histories

Append streams are source histories. They are used for durable event, audit, and history use cases where sequence ordering matters. Segment sealing is storage maintenance, not logical stream closure. There is no general public CLI repair family for arbitrary append-stream source records today.

If an append-stream consumer is behind, treat that as a watch/checkpoint problem: read or tail the stream, inspect the consumer checkpoint, and replay from the last durable checkpoint after downstream output is safe. If append-stream source records are missing or corrupt, preserve evidence and use backup/recovery procedures rather than trying to rebuild the stream from a derived export.

A manual tail can prove the source surface emits records:

```bash
anvil --profile acme stream tail documents audit/events stream-2026 --from-sequence 0
```

This proves the caller can authenticate, read the named append stream, and receive ordered records from the requested sequence. It does not prove an external SIEM, webhook sender, or derived export consumed them, and it does not repair the stream.

## CoreStore integrity boundaries

CoreStore stores the durable substrate, but the current operator surface does not expose a general `corestore fsck` command. There is no single supported command that walks every blob shard, manifest, ref, stream, transaction, object version, append stream, PersonalDB log, gateway record, and derived segment and certifies the whole volume.

Use feature diagnostics and repairs for feature-scoped evidence. Use storage monitoring, logs, backup validation, and restore drills for the recovery boundary. If a read fails with a manifest mismatch, hash mismatch, missing source record, or unrecoverable volume problem, move into backup and recovery. A derived repair cannot reconstruct source data that was never committed or that has been lost.

Do not edit CoreStore files to fix a diagnostic. Direct edits bypass authentication, public policy, system-realm authorisation, CAS, fences, stream hashes, audit, and repair records. If a supported API does not exist for the required recovery action, document the gap and preserve evidence.

## Findings and verification

Repair findings are durable evidence that a repair or diagnostic path found something worth tracking. The public CLI can list findings when you know the scope kind and scope id returned by repair or diagnostics:

```bash
anvil --profile acme repair findings index "$REPAIR_SCOPE_ID" --page-size 20
```

This proves the caller can read repair findings for the exact scope id returned by the repair or diagnostic flow. It does not apply repair, and it does not prove the current symptom still exists. Findings should be paired with the failing request, the repair response, and a post-repair verification command.

Verification must repeat the user-visible invariant. If listing was wrong, list the same prefix. If search was stale, rerun the same query with the same caller and freshness expectation. If an authz derived index was stale, rerun the denied or allowed check at the relevant zookie. If a PersonalDB replica was stuck, run catch-up from the replica checkpoint. If a host alias was wrong, request the same host/path through the gateway.

A repair is not complete because the command returned success. It is complete when the original invariant is true again, diagnostics are clean or understood, lag is within the expected range, and audit records explain what changed.

## Current surfaces and gaps

The current repair and diagnostics surface is intentionally focused:

| Area | Current surface |
| --- | --- |
| Public index diagnostics | `anvil diagnostics list` and `anvil index diagnostics` read tenant index diagnostics. |
| Public repairs | `anvil repair run index`, `directory`, `authz-derived`, and `personal-db`; `anvil repair findings` lists findings by scope. |
| Admin diagnostics | `anvil-admin diagnostics list` reads system diagnostic backends such as index and mesh diagnostics. |
| Admin repairs | `anvil-admin repair run` covers `index`, `directory-index`, `authz-derived-index`, `personaldb-log-chain`, and `mesh-routing-projection`. |
| Routing record repair | `anvil-admin routing repair` repairs one materialised routing record by family/key. |
| Audit evidence | Admin repair records audit events; tenant/public operations should use tenant audit where relevant. |

Current gaps matter during incidents. There is no general `corestore fsck`, no broad automatic proof that every derived system is correct, no universal append-stream repair, no complete CLI projection-repair workflow for every PersonalDB projection case, and some watch/projection evidence is API-only. Some diagnostics are surface-specific and may not cover the path you are investigating. Some repair commands rebuild derived state but intentionally refuse to synthesise committed source records.

The safe operating model is therefore conservative: diagnose read-only first, name the source and derived target, run the narrowest repair available, preserve audit evidence, verify the original symptom, and treat missing repair surfaces as implementation gaps rather than reasons for direct storage edits.

## Repair safety rules

Run the narrowest repair that matches the evidence. Use check/list diagnostics before rebuild when you are still diagnosing. Use tenant repair for tenant-owned derived state. Use admin repair for platform routing or administrative backends. Always preserve command output and request ids so the next operator can see what changed.

Repair cannot create missing source truth. If object metadata streams, authz tuple logs, PersonalDB commits, or lifecycle records are absent after a restore, repeated derived rebuilds will not fix the source gap.
