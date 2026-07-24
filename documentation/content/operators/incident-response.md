---
title: Incident Response
description: Triage Anvil incidents with evidence-first classification, safe mitigations, source-versus-derived checks, and auditable recovery.
---

# Incident Response

Incident response in Anvil should reduce uncertainty before it changes state. A failed request may be a network-plane mistake, an authentication failure, a correct authorisation denial, stale derived data, gateway signature drift, PersonalDB projection lag, disk pressure, a topology error, or a bad release. Those failures can look similar from a user's point of view, but they need different mitigations.

The safest pattern is evidence first, then the smallest safe action. Preserve request ids, logs, audit entries, diagnostics, topology state, and affected resource names. Classify whether the source record is wrong or only a derived view is stale. Mitigate the immediate exposure or outage without creating a second incident. Verify the failed user operation, not only a health check. Only then write the postmortem and improve the runbook.

Read this page with [Observability](/operators/observability/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Security Hardening](/operators/security-hardening/), [Admin Plane](/operators/admin-plane/), [Network and Ports](/operators/network-and-ports/), [Gateway Operations](/operators/gateway-operations/), [PersonalDB Operations](/operators/personaldb-operations/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [CoreStore Operations](/operators/corestore-operations/), [Backup and Recovery](/operators/backup-and-recovery/), [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/), [Authorisation](/learn/authorisation/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Admin CLI](/reference/admin-cli/), and [Public CLI](/reference/public-cli/).

## The first minutes

Start by naming the incident in terms of impact and uncertainty: which tenants, buckets, regions, hosts, clients, and data families are affected; when it started; whether it is still happening; whether the failure is confidentiality, integrity, availability, or freshness. Avoid naming the root cause too early. "Search returns stale results for tenant acme" is better than "the indexer is broken" until you have cursor and diagnostic evidence.

Preserve evidence before mutating the system. Capture request ids from clients, gateway response ids, safe tenant and bucket identifiers, error codes, status codes, timestamps, deployment version, image digest, recent admin audit entries, relevant logs, diagnostics, and current topology descriptors. If you need a storage snapshot for later forensics, take it before destructive cleanup or broad repair. Do not paste bearer tokens, app secrets, S3 signatures, server keys, PersonalDB changesets, or object bodies into incident notes.

A read-only admin diagnostic is usually a safe early probe from the private operator network:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --limit 50
```

This proves the admin listener is reachable, the caller has the system-realm relation for diagnostics, and the mesh diagnostic backend can return findings. It does not prove tenant public API health, object durability, index freshness, or that the admin listener is hidden from public networks. Use it as one piece of the evidence chain.

Also capture audit evidence early:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list --limit 50
```

This proves admin audit can be read by the caller. It does not prove tenant-owned activity or gateway requests are fully explained. Pair it with public tenant audit, gateway logs, and request ids where the incident involves tenant actions or public exposure.

## Classify the failure

Classification prevents overreaction. An availability failure on the public listener is not the same as a correct permission denial. A stale full-text index is not the same as lost object data. A gateway signature error is not the same as public access being disabled. A PersonalDB projection lag is not the same as a corrupt SQLite database.

Use this classification as the first triage pass:

| Class | First evidence | Safer early mitigation |
| --- | --- | --- |
| Network boundary | Which listener is reachable from which network; tenant, admin, and internal node gRPC routes; proxy and firewall changes. | Close unintended exposure, reroute traffic, or remove the node from the load balancer. |
| Authentication | Token minting, token expiry, credential rotation, S3 signature inputs, JWT secret consistency. | Rotate the affected credential or roll back the credential deployment. |
| Public policy | Action/resource checked, app grant set, token scopes, recent grant/revoke audit. | Grant or revoke the narrow missing scope if policy is wrong; do not use wildcard grants. |
| Relationship authorisation | Tuple revision, zookie, userset facts, schema binding, derived userset lag, object visibility. | Restore or write the precise tuple if missing; repair derived authz only after source facts are verified. |
| Source storage | Object version, append sequence, PersonalDB commit, CoreStore read/write error, disk capacity. | Quiesce writes, preserve storage evidence, restore from backup only if source records are missing or corrupt. |
| Derived data | Index cursor, watch cursor, projection head, routing projection generation, diagnostics. | Rebuild or repair the affected derived target, not the whole system. |
| Gateway | Host route, public-read state, S3 signature, forwarded host/proto, bucket locator, wrong-region policy. | Fix routing/proxy/public-read state; do not bypass authorisation. |
| Topology | Region/cell/node lifecycle, bucket home region, locator status, drain state, proxy eligibility. | Drain or reroute narrowly; avoid unsupported direct lifecycle edits. |
| Capacity | Disk bytes/inodes, CPU, memory, queue depth, compaction backlog, watch/index lag. | Reduce load, add capacity, pause non-critical rebuilds, or move traffic deliberately. |
| Release | Image digest, server/CLI/client versions, recent migrations, changed defaults, failed smoke test. | Roll forward or storage-aware rollback; do not downgrade after one-way storage writes. |

The classification can change as evidence arrives. That is normal. Write down the old assumption and the new evidence so responders do not repeat the same branch of triage.

## Choose the smallest safe mitigation

The first mitigation should stop the harm or reduce blast radius without hiding the evidence. If the admin API is exposed, remove the public route or firewall rule before rotating every secret. If a public bucket is accidental, turn off public-read before rebuilding indexes. If a search result is stale, check index lag before running repair. If a gateway signature fails, check the proxy host and scheme before rotating every app secret.

Avoid three unsafe habits.

Do not edit files under `STORAGE_PATH` to make an incident go away. Direct edits bypass authentication, authorisation, CAS, fences, stream hashes, audit, diagnostics, and repair evidence. Use public/admin APIs, focused repairs, backups, and restore drills.

Do not use admin credentials to perform ordinary tenant data work because a tenant application is failing. That hides the actual public policy or relationship-authorisation problem and makes audit ambiguous. Admin credentials are for system operations.

Do not start with broad repair. Repair can be correct when derived state is stale or damaged, but source proof comes first. Repairing every index or projection during an incident consumes capacity, destroys before-state evidence, and can make a small stale view look like a cluster-wide outage.

## Incident: admin API exposed

Treat public reachability of the admin listener as a security incident even though admin calls still require authentication and system-realm authorisation. The first mitigation is network-level: remove the public load-balancer route, Service, Ingress, firewall rule, or port publication that exposed `ADMIN_LISTEN_ADDR`. Do not wait for a full root-cause analysis before closing the path.

Then preserve evidence: when the route was exposed, which source addresses reached it, what request ids and status codes appeared in logs, whether any admin bearer token was accepted, and which admin audit events occurred during the window. A focused audit query can help:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list \
  --limit 100
```

This proves audit listing works from the private admin path. It does not prove nobody attempted unauthorised calls; use reverse-proxy, firewall, and server logs for rejected traffic as well.

Rotate credentials only according to evidence. If logs show successful admin token use from an untrusted network, rotate or revoke the affected admin app secret and review system-realm relations. If only unauthenticated probes reached the port, you may still rotate high-risk credentials, but document the reason. Do not create a special emergency admin bypass; use the existing system-realm path and audit every change.

## Incident: public data exposed

Public exposure can come from an accidental public-read bucket, a host alias pointing at the wrong bucket or prefix, a moved object link, a gateway route, or an overly broad public policy grant. First stop further exposure in the narrowest place that matches the evidence.

For an accidental public-read bucket, the tenant-owned public API can disable the read policy:

```bash
anvil --profile acme bucket set-public public-assets --allow false
```

This proves the authenticated tenant app can change the bucket public-read policy. It does not delete object data, revoke signed or authenticated access, remove cached copies from browsers or CDNs, or prove that a host alias no longer routes to the bucket. After the policy change, test anonymous S3/static reads for representative objects and preserve gateway logs for the exposure window.

If the exposure came through a custom host alias under operator control, suspend or repair that alias through the admin path only when the operation is genuinely operator-owned:

```bash
anvil-admin --host http://10.10.0.12:50052 host-alias suspend \
  --hostname docs.example.com \
  --expected-generation 7 \
  --audit-reason 'suspend public alias during incident INC-9142'
```

This proves an authorised operator moved the alias lifecycle state with a generation check and audit reason. It does not change bucket policy, purge caches, or rotate tenant credentials. Use public tenant APIs for tenant-owned alias and object-link fixes where supported.

For overly broad grants, revoke the specific grant rather than replacing it with another broad emergency rule. Review [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) before deciding the replacement scope.

## Incident: stale search

Stale search is usually a derived-data incident. Do not assume the object is missing until a direct source read fails. Start by reading the object or metadata through an authorised public caller:

```bash
anvil --profile acme object head s3://documents/contracts/acme-2026.txt
```

This proves the caller can authenticate and read metadata for that object key. It does not prove the body is correct, the full-text extractor succeeded, the vector index embedded it, or the query caller can see it through `inherit_object` filtering.

Next inspect diagnostics for the affected index:

```bash
anvil --profile acme diagnostics list documents body_text \
  --severity warning \
  --page-size 50
```

This proves tenant-facing diagnostics for `documents/body_text` can be read. It does not prove the index is caught up. Use query catch-up fields where the index family supports them, and inspect index or watch lag where it does not.

Repair only the affected derived target when source proof and diagnostics point there:

```bash
anvil --profile acme repair run index documents body_text --rebuild
```

This asks the public repair service to rebuild the named tenant index from committed source records. It does not fix malformed source objects, grant object visibility, configure a missing embedding provider, or make unrelated indexes current. Verify by rerunning the exact failed query and checking diagnostics again.

## Incident: wrong-region or gateway error

Wrong-region and gateway errors are often routing incidents, not storage incidents. A client may be using a generic endpoint, the wrong regional hostname, a stale DNS record, a changed host alias, or a reverse proxy that rewrites host/proto in a way that breaks S3 Signature Version 4.

Start with the gateway evidence: request id, host, method, path, response status, `x-amz-request-id` where present, effective region, bucket home region, and whether the response was redirect, proxy, reject, or signature failure. Then inspect mesh/routing diagnostics:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --severity warning \
  --limit 50
```

This proves the admin diagnostic backend can return mesh warnings. It does not prove S3 credentials are valid, the client clock is correct, or the bucket object exists. Pair it with a signed S3 `HEAD` or a native object read through the bucket's home region.

If the error is signature-related, check access key id, secret rotation, clock skew, signed host, forwarded host, forwarded proto, trusted proxy ranges, and body hash mode before changing bucket policy. If the error is wrong-region, check bucket locator and endpoint selection before looking for missing bytes.

## Incident: PersonalDB sync stuck

PersonalDB incidents need source-chain evidence. A local SQLite client can be behind, ahead with unsubmitted edits, submitting from a stale base head, blocked by row-level authorisation, or waiting for a projection. Anvil is the witness and history service, not the remote SQL server to query directly.

Start with the group state:

```bash
anvil --profile acme personaldb group read customer-notes
```

This proves the public API can read the group manifest for the authenticated tenant and caller. The current CLI prints compact group information; it does not prove every replica is current or show every field needed for automated catch-up comparison. Use the API for full committed-head and certificate evidence.

Probe catch-up from the replica's reported checkpoint:

```bash
anvil --profile acme personaldb catch-up customer-notes \
  --replica-id laptop-a \
  --have-log-index 42 \
  --have-log-hash "$REPLICA_LOG_HASH" \
  --max-entries 100
```

This proves whether Anvil can compare the supplied replica position with the witnessed log and report replay/catch-up status. It does not apply changes to the client SQLite database, does not prove row-level authorisation for future commits, and does not prove projections are current.

If source-chain integrity is suspect, run the focused repair check:

```bash
anvil --profile acme repair run personal-db customer-notes
```

This checks the PersonalDB log chain for that tenant/database. It does not invent missing commits, choose conflict winners, repair a local replica database, or make projections caught up. Use projection lag and API-level projection watch evidence for projection incidents.

## Incident: key or credential leak

First identify which secret leaked. The mitigation is different for an app client secret, bearer token, `JWT_SECRET`, `ANVIL_SECRET_ENCRYPTION_KEY`, previous encryption key, per-node internal bearer credential, local node signing key, or first-admin credential.

For a tenant app secret, rotate the affected app and update only the service that owns it:

```bash
anvil-admin --host http://10.10.0.12:50052 app rotate-secret \
  --tenant-id acme \
  --app-name docs-writer \
  --expected-generation 3 \
  --audit-reason 'rotate docs-writer secret after incident INC-9142'
```

This proves an authorised admin rotated the app credential with generation protection and audit evidence. It does not revoke bearer tokens that were already minted; those normally expire according to token lifetime unless another control invalidates them.

For a server-side encryption-key leak, configure a new active key and keep the leaked key as a previous key only long enough to rotate existing envelopes. Start with a dry run:

```bash
anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason 'dry-run envelope rotation after key leak INC-9142'
```

This proves the restored or running server can inspect known encrypted envelopes with the configured keyring. It does not rotate records. Run the real rotation only after configuration is correct, then verify app token minting and affected integrations before removing previous keys. For `JWT_SECRET`, plan a coordinated node rollout; there is no documented multi-key overlap workflow equivalent to envelope rotation.

If an internal node credential leaks, replace that node's credential, update its protected runtime configuration, and verify that the old credential is rejected while distributed writes and recovery still succeed. If a node's Ed25519 signing private key leaks, remove or drain the compromised lifecycle identity, preserve audit evidence, and register a fresh node identity and public key. Do not overwrite the committed public key for an existing live identity as though ordinary key rotation had occurred.

## Incident: disk pressure or CoreStore errors

Disk pressure is a source-integrity risk. It can turn a normal write burst into object write failures, stream append failures, compaction backlog, repair failures, or corrupted partial writes if the host behaves badly. First reduce write pressure and preserve evidence: storage free bytes/inodes, filesystem errors, node logs, recent CoreStore error messages, compaction backlog, and request ids.

Do not delete files under `STORAGE_PATH` by hand. Unknown files may be staging, manifests, stream records, or evidence of a bug. Free space by moving traffic away, expanding the volume, pausing non-critical rebuilds, or restoring in a controlled environment. If source reads fail with manifest or hash errors, treat it as integrity and recovery work, not an authorisation problem.

A tenant object source read can distinguish storage failure from stale search:

```bash
anvil --profile acme object head s3://documents/contracts/acme-2026.txt
```

A successful response proves one metadata path is readable. It does not prove the object body can be streamed, every version is intact, or CoreStore is globally healthy. Pair it with logs, diagnostics, backups, and, if necessary, an isolated restore drill.

## Incident: watch consumer lag

Watch lag means a derived consumer is behind source records. The consumer may be under-provisioned, blocked on a poisoned event, unable to write its own output, or checkpointing incorrectly. The mitigation is not to advance the checkpoint manually. A false checkpoint can permanently skip source records.

Use a manual watch tail only to prove the source watch surface can emit events:

```bash
anvil --profile acme watch prefix documents incoming/ --after-cursor 12345
```

This proves the public watch endpoint can deliver object events after cursor `12345` to the authenticated caller. It does not prove the production consumer processed those events, wrote durable output, or stored its checkpoint safely. Inspect the consumer's own checkpoint, last successful apply time, last error, and output store.

If a poisoned event is blocking progress, record a diagnostic or repair finding with the source cursor and keep the checkpoint at the last safe output unless the quarantine itself is durable and intentional. If lag is too large for incremental replay, rebuild the derived view from source and publish a new checkpoint only after the rebuild is verified.

## Incident: bad release

A bad release is both a serving incident and a storage-compatibility question. First identify the exact image digest, server version, public CLI version, admin CLI version, client crate version, rollout scope, and whether the new binary wrote durable source records or only failed before serving.

If the release fails before writing to the durable volume, restarting the previous image may be safe after normal smoke tests. If the release wrote one-way storage or internal records, arbitrary downgrade is unsafe. Follow the release notes and [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/): restore from a pre-upgrade backup in isolation or roll forward with a fix.

Do not run broad repair simply because a new release behaves badly. First classify whether the failure is API compatibility, gateway routing, derived lag, schema/storage format, secret configuration, or capacity. Repair can rebuild derived state from source records; it cannot make an old binary safely read a new source-record format.

## Verification and closure

An incident is not recovered when the first command succeeds. It is recovered when the failed invariant is true again. If the issue was public exposure, anonymous reads must fail and logs/audit must show the exposure window. If the issue was stale search, the exact query must return the expected result or a deliberate catch-up response. If the issue was PersonalDB sync, replicas must catch up or have a documented recovery path. If the issue was admin exposure, the admin listener must be unreachable from public networks and audit must be reviewed.

Closure should include:

| Record | Why it matters |
| --- | --- |
| Timeline | Preserves when impact started, when mitigation happened, and when verification passed. |
| Scope | Names affected tenants, buckets, regions, hosts, indexes, groups, or credentials without leaking sensitive payloads. |
| Evidence | Keeps request ids, logs, diagnostics, audit ids, repair findings, and before/after checks. |
| Source versus derived conclusion | Prevents future responders from confusing lost source records with stale views. |
| Mitigation and verification | Shows exactly what changed and which user-visible operation proved recovery. |
| Follow-up | Adds tests, alerts, runbook changes, least-privilege fixes, capacity changes, or missing product work. |

The postmortem should also record what not to repeat: direct storage edits that were avoided, broad repairs that were rejected, admin shortcuts that were not used, and any documentation gap that forced responders to inspect source. Incident response improves when the next responder has fewer mysteries than the first one.

## Current gaps to plan around

The current repository provides useful diagnostics, repairs, audit, public/admin CLIs, gateway request ids, and feature-specific smoke paths, but it is not a complete incident platform. There is no documented general `corestore fsck`, no bundled dashboard pack, no single command that certifies all derived systems, and no complete graceful drain workflow for every lifecycle case. Some watch and PersonalDB projection evidence is API-only. Some public policy scopes are coarser than ideal, especially object listing and index read surfaces.

Plan incidents with those limits in mind. Prefer evidence-preserving mitigations, use API surfaces where the CLI is compact, keep admin and tenant work on their proper planes, and file missing operational surfaces as gaps instead of filling them with direct storage edits or undocumented bypasses.

## Source-versus-derived triage

The fastest way to reduce incident scope is to compare a source read with the failing derived path. If object reads work but search fails, focus on index builders, selectors, diagnostics, and result filtering. If object reads fail through both native and gateway paths, focus on object existence, policy, routing, or storage. If admin topology reads fail but public reads continue, focus on the admin listener or system-realm authorisation rather than tenant credentials.

Capture audit before changing state. A recent policy revoke, region drain, host-alias suspension, or secret rotation can explain symptoms that otherwise look like storage faults.
