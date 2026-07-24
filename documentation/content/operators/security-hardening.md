---
title: Security Hardening
description: Harden Anvil by keeping network planes separate, using least-privilege public scopes, protecting the system realm, preserving CoreStore fences, and validating gateways, logs, audit, and release gates.
---

# Security Hardening

Security hardening in Anvil is layered. No single mechanism is supposed to carry the whole system. Network boundaries reduce who can reach each plane. Authentication proves which app or system principal made the request. Public policy scopes authorise tenant-facing API calls. Relationship authorisation models product access inside a storage tenant. The system realm authorises private admin operations. CoreStore preconditions, fences, idempotency keys, and reserved namespaces stop stale writers and bypass attempts from turning into durable state. Gateways translate external protocols back into the same model, rather than creating a weaker parallel one.

This chapter is for operators preparing or reviewing a production deployment. It is not a penetration-test checklist and it is not a replacement for source review, but it gives you the operating model: which boundary each layer protects, what evidence proves the layer is working, and which current implementation gaps require conservative configuration.

Read this with [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), [Admin Plane](/operators/admin-plane/), [Secrets and Key Management](/operators/secrets-and-key-management/), [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/), [CoreStore Operations](/operators/corestore-operations/), [Gateway Operations](/operators/gateway-operations/), [Observability](/operators/observability/), [Release Readiness Checklist](/operators/release-readiness-checklist/), [Authorisation](/learn/authorisation/), [CoreStore](/learn/corestore/), [Gateways](/learn/gateways/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## Start with the trust surfaces

Anvil runs as one server process per node with two listeners and three authorisation contexts. The public listener handles tenant native API traffic, the S3/static gateway, and internal node gRPC. The admin listener handles private operator API traffic. Internal RPC shares the public transport listener but is not tenant authority: it requires a node bearer credential and built-in system-realm permission.

A hardened deployment keeps the boundaries visible in configuration and network policy:

| Surface | Should reach it | What protects it |
| --- | --- | --- |
| Public plane | Tenant applications, approved public clients, S3/static callers, and public-read readers where deliberate. | Bearer-token authentication for authenticated calls, public policy scopes, relationship authorisation, reserved namespace rejection, gateway routing checks, TLS/proxy controls. |
| Admin plane | Operators and trusted automation on private networks. | Private reachability, bearer-token authentication, built-in system-realm authorisation, generation checks, audit reasons, diagnostics and audit evidence. |
| Internal node gRPC | Equal Anvil nodes named by committed CoreMeta lifecycle topology. | Per-node bearer credentials, system-realm authorisation, committed `public_api_addr` routing, Ed25519 evidence verification, TLS/private routing, and topology diagnostics. |
| Durable storage | The Anvil server process and backup/restore tooling under operator control. | Filesystem permissions, secret key history, CoreStore invariants, backups, restore drills, and a policy of no direct application writers. |

There is no independent membership-discovery path. A reachable process is not eligible for distributed work unless its lifecycle descriptor is committed with the expected node id, signing public key, endpoint, capabilities, and active state. Network policy should restrict internal gRPC paths, but correctness and authority come from authenticated RPC plus committed topology rather than address discovery.

The admin API must not be internet-facing. It still checks authentication and system-realm authorisation, but network privacy is another layer. Publishing `50052` merely because `50051` is public is a deployment bug. If `ADMIN_LISTEN_ADDR` is bound off loopback, `ALLOW_PUBLIC_ADMIN_LISTENER=true` is only an explicit operator acknowledgement that private networking, firewall rules, service mesh policy, or a bastion protects that listener.

A useful first admin-plane check is read-only:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list --limit 20
```

This proves the caller can reach the private admin listener, present an admin bearer token, and satisfy the system-realm relation for diagnostic reads. It does not prove the admin listener is hidden from tenant networks. Test that separately from the public side of your deployment by checking firewall, load-balancer, Service, and Ingress rules.

## Authentication identifies the caller

Anvil applications authenticate with a client id and client secret. The public authentication service mints a short-lived bearer token containing the tenant id, subject, expiry, token id, and approved public policy scopes. Admin callers also use bearer tokens, but admin methods then check the built-in system realm rather than ordinary public policy scopes.

Hardening starts by reducing the blast radius of credentials. Use one application credential per service or automation role rather than one tenant-wide secret shared by every job. Rotate app secrets when a service owner changes or a secret may have leaked. Treat bearer tokens as sensitive even though they expire; do not log `Authorization` headers, S3 signatures, app secrets, first-admin credentials, or server key material.

The CLI can prove token minting, but it cannot prove the token is least-privilege by itself:

```bash
anvil --profile docs-writer auth get-token
```

A successful response proves the profile's client id and secret can mint a bearer token through the public endpoint. It does not prove the token contains only the scopes that service should have. Follow it with grant inspection and a negative test for an operation the service must not perform.

## Public policy scopes are API authority, not product sharing

Public policy scopes decide whether an app principal may call a public API method for a resource string. They are good for service authority: this app may create this bucket, write this object, query indexes in this bucket, watch this prefix, or commit to this PersonalDB group. They are not the same thing as end-user sharing rules.

Use exact resources or narrow prefix resources. Avoid broad grants for long-lived credentials. A narrow operator grant for one object write looks like this:

```bash
anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource documents/inbox/welcome.txt \
  --audit-reason 'allow docs-writer to upload the onboarding welcome document'
```

This asks the private admin API to add one public policy grant to the `docs-writer` app in tenant `acme`. It proves the admin principal can manage policies and that the grant was recorded with an audit reason. It does not grant reads, deletes, listings, index queries, app management, authz tuple writes, or access to other object keys.

Current public scope checks are not always as fine-grained as an ideal product model. Object listing checks `object:list` on the bucket name, not on a prefix. Index list, query, and diagnostics currently check `index:read` on the bucket name, not on one index definition. Tenant app-management checks use `tenant:<tenant_id>`, not one app name. Hardening means designing buckets, prefixes, indexes, and apps around those current boundaries instead of papering over them with broad grants.

Tenant-side delegation has its own guardrails. A tenant app can grant only permissions it already holds, and public delegation rejects global wildcard authority, cross-tenant resources, system resources, internal mesh resources, and reserved `_anvil/` resources. Use that as a non-escalation layer, not as the only defence.

## Relationship authorisation is tenant-owned product access

Relationship authorisation is where a tenant models product access: users, groups, documents, projects, folders, object readers, PersonalDB rows, and similar application concepts. It stores tuples such as `document/doc-42#viewer <- user:amy` and userset relationships such as `document/doc-42#viewer <- userset:document/doc-42#owner`.

A tenant can define and bind schemas in its own relationship-authorisation realm when delegated the relevant public scopes. It can write tuples and run checks for its own storage tenant. That does not give the tenant authority over Anvil's internal system realm, mesh topology, server secrets, admin audit, or built-in admin relations. Tenant realms and the system realm are deliberately separate.

Current relationship authorisation has limits to account for. Schema documents and bindings are stored and useful as reviewed contracts, but the current evaluator resolves tuple and userset facts rather than a full Zanzibar schema rule language. `caveat_hash` is stored and matched, but caveat expression evaluation is not implemented. Some richer consistency and non-default realm flows are API-first rather than fully exposed in the public CLI. Do not build a security design that assumes unimplemented schema rules or caveats are already enforced.

For protected object data, prefer `inherit_object` index authorisation so index hits still pass object visibility checks. Use `index_only` or `public` only for deliberately shareable derived rows. A search result can leak object keys, titles, snippets, scores, and existence, even when the body is never fetched.

## The system realm protects Anvil itself

The private admin API is authorised by Anvil's built-in system realm. System-realm relations such as `manage_tenants`, `manage_apps`, `manage_policies`, `manage_regions`, `manage_nodes`, `manage_routing`, `run_repair`, `view_diagnostics`, and `view_audit_log` decide which system principals can perform operator actions. These are not public policy scopes.

First boot is the only special moment. When the system realm is absent, server startup can install the built-in system schema and create the first system administrator credential according to bootstrap configuration. That happens below the API before public or admin requests are accepted. Once the system realm exists, startup does not create another administrator just because the environment still contains first-boot settings.

That means there should be no permanent API bypass for administration. No special header should make a caller an admin. No tenant app should be able to grant itself system-realm relations through public policy. `anvil-admin` should not write storage files. Except for its local key-generation helper, it is a network client for the private admin API. Admin mutations should authenticate, check the system realm, validate request fields, use generation or idempotency guards where applicable, require an audit reason, and be written by the server.

## Secrets and key material are separate layers

Several secrets protect different parts of Anvil. `JWT_SECRET` signs bearer tokens. `ANVIL_SECRET_ENCRYPTION_KEY`, `ANVIL_SECRET_ENCRYPTION_KEY_ID`, and `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` protect server-side encrypted secret envelopes and related encrypted data. Each node has its own internal bearer credential and a locally persisted Ed25519 signing key. First-admin and tenant app credentials mint bearer tokens. Bearer tokens authorise requests for a bounded period.

Do not mix their runbooks. Rotating an app secret does not rotate server-side encrypted envelopes. Changing `JWT_SECRET` can invalidate active tokens but does not decrypt stored secrets. Losing `ANVIL_SECRET_ENCRYPTION_KEY` can make encrypted records in backups unreadable. Losing every system admin credential after the system realm exists is not fixed by restarting with first-boot variables.

The local helper for generating server-side encryption key material is safe to run because it does not contact Anvil:

```bash
anvil-admin key generate-secret-encryption-key
```

This proves only that the CLI can generate a 32-byte hex value suitable for `ANVIL_SECRET_ENCRYPTION_KEY`. It does not install the key, rotate envelopes, or test decryption. Store the value in a secret manager and inject it only into server processes.

Before removing old encryption keys, dry-run envelope rotation through the admin API:

```bash
anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason 'verify secret envelope rotation before removing previous key 2026-07'
```

This proves the server can inspect known encrypted envelopes with its configured active and previous keys and that the caller has the system-realm relation for secret-envelope rotation. It does not rewrite records. A real rotation and post-rotation service test are required before removing previous keys from configuration and backup retention.

## Reserved namespaces fail closed

Anvil reserves internal object-key prefixes such as `_anvil/meta/`, `_anvil/index/`, `_anvil/authz/`, `_anvil/watch/`, `_anvil/personaldb/`, `_anvil/git/`, and `_anvil/tmp/`. Public object APIs and the S3/static gateway must reject reads, writes, lists, version lists, range reads, copy sources, copy destinations, multipart operations, append streams, watches, and other object-shaped operations under those prefixes.

This is not a tenant policy choice. A public-read bucket does not make reserved internal paths readable. A broad public policy scope should not target reserved resources. Returning ordinary not-found for reserved internals can leak that a hidden namespace exists or differs from normal data; the expected security outcome is an explicit `UnauthorizedReservedNamespace` style failure.

A manual negative check through the public CLI is useful after gateway or object-service changes:

```bash
anvil --profile acme object head s3://documents/_anvil/authz/tuples
```

For a correctly hardened deployment, this command should fail with reserved-namespace rejection. That failure proves the public object path is not treating Anvil-owned internal keys as ordinary tenant data. It does not prove every gateway route is protected; test S3, static, copy, list, and range paths that your deployment exposes.

## CoreStore preconditions and fences are security controls

CoreStore is not just persistence. It is where Anvil enforces durable ordering and stale-writer rejection. Object versions move a current pointer. Refs use compare-and-swap generations. Streams carry sequence, cursors, hashes, and optional idempotency keys. Task leases carry ownership and fence tokens. Mutation batches combine preconditions so one stale worker cannot publish state after another worker has taken over.

A CAS failure, stale fence rejection, or idempotency conflict is often a security success. It means the server rejected an update that did not match the durable state the caller claimed to have observed. Treat repeated failures as signals to investigate ownership, retries, replay, and stale automation; do not disable the guard by adding a direct storage writer.

Direct file writes under `STORAGE_PATH` bypass the model. They skip authentication, public policy, relationship authorisation, system-realm checks, CAS, fences, idempotency, stream hashes, audit, diagnostics, and repair evidence. Application containers should not mount the Anvil durable volume. Operator tooling should use public or admin APIs. Backups and restore drills may read the volume, but they should not become a shadow mutation path.

A source-review check helps catch accidental filesystem side stores:

```bash
rg -n "tokio::fs|std::fs|OpenOptions|File::create|write_all" anvil-core/src anvil/src
```

This proves only that you looked for direct filesystem write paths. It does not prove the remaining writes are safe. Review each hit: temporary upload staging, operator identity paths outside `STORAGE_PATH`, and first-admin credential output have different handling from feature source records, which should be CoreStore-backed.

## Gateways must translate, not weaken

The S3 and static gateways are adapters over Anvil's native model. An S3 access key is an Anvil app client id. An S3 secret key is the app client secret. Signed S3 operations still map to public policy scopes and relationship checks. Static hosting still maps host, path, bucket, object, link, public-read policy, and object visibility. A gateway should not add its own ACL system that disagrees with Anvil, and it should not become a place where reserved namespaces or admin functions are reachable through a friendlier protocol.

Reverse proxies are part of the security boundary. S3 signatures and host routing depend on the effective host and scheme. Configure `TRUSTED_PROXY_SOURCE_RANGES` to the exact proxy source addresses Anvil sees. Do not trust forwarded host headers from arbitrary clients. If S3 signatures fail only through the proxy, inspect host, scheme, and trusted proxy configuration before widening permissions or rotating credentials.

Public-read is deliberate exposure. If a bucket is public, anyone who can reach the public surface may read matching object data through supported read routes. That can include object names, versions, content types, sizes, simple metadata, and bodies. Public-read does not expose the admin API, does not grant writes, and does not bypass reserved namespace rejection, but it may still be a serious data-exposure decision.

A public-read change should therefore be explicit and auditable:

```bash
anvil --profile acme bucket set-public public-assets --allow true
```

This proves the authenticated tenant app can change the public-read policy for `public-assets`. It does not upload assets, grant writes, expose neighbouring buckets, or purge caches if you later turn public-read off. Use dedicated public buckets where practical so one policy decision does not mix private and public data.

## Logs, audit, and diagnostics are evidence

Security operations need evidence that is useful without leaking secrets. Logs should include request ids, operation names, safe tenant or bucket identifiers where appropriate, response status, latency, route source, proxy trust result, and authorisation outcome. They should not include bearer tokens, app client secrets, first-admin credentials, server encryption keys, S3 signatures, `Authorization` headers, object bodies, PersonalDB changesets, or package artefact bytes.

Admin mutations should have specific audit reasons. `rotate acme reader secret after incident INC-9142` is useful. `fix` is not. Audit is how a later operator proves whether a tenant published data, an operator changed topology, a secret was rotated, or a host alias was suspended.

A read-only audit check is safe during hardening reviews:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list --limit 50
```

This proves the admin listener is reachable and the caller has `view_audit_log` in the system realm. It does not prove audit reasons are high quality or that every tenant-facing event is exported to your SIEM. Pair it with review of representative mutation records and log redaction tests.

Diagnostics are similar: they are a place to start, not a proof that no bug exists. Use admin diagnostics for system-level evidence and tenant/public diagnostics where a tenant owns the concern. Do not inspect tenant object bodies through the admin plane just because a diagnostic points at a tenant bucket.

## Release gates preserve hardening

Hardening must be checked before release, not rediscovered after exposure. The repository has static and documentation gates that protect several invariants:

```bash
./scripts/check-no-external-db.sh
./scripts/check-no-public-unfenced-journal-writes.sh
./scripts/check-docs-hardening.sh
```

The first command checks that external relational metadata-store references have not reappeared in code paths outside documentation. The second checks that public or crate-public journal mutation entrypoints do not bypass fenced permit APIs. The third checks documentation for known bypass language and invalid CLI shapes. Passing these scripts does not prove the whole system is secure; it proves these specific regressions were not detected.

The broader release gate is:

```bash
./scripts/release-gates.sh
```

That runs the hardening scripts, release-note checks, documentation build checks, crate dry run, and workspace tests. It is useful release-candidate evidence. It does not replace threat modelling, deployment firewall checks, secret rotation drills, restore drills, gateway tests, or tenant-specific authorisation tests.

## Current gaps and conservative defaults

Some current surfaces are coarser or less complete than the ideal model. Design around them honestly:

| Area | Current hardening implication |
| --- | --- |
| Object listing | Current object listing checks `object:list` at bucket scope. Split buckets or avoid exposing listings where prefix-only list isolation is required. |
| Index reads | Index list/query/diagnostics currently use `index:read` at bucket scope. Keep private corpora on `inherit_object` and use bucket design to reduce over-broad query access. |
| Relationship schemas | Schemas and bindings are stored, but the evaluator currently resolves tuple/userset facts rather than a full schema rule language. Write the tuples the current evaluator needs. |
| Caveats | `caveat_hash` is stored and matched, but caveat expression evaluation is not implemented. Do not rely on it for time, device, or purpose restrictions. |
| Public CLI coverage | Some consistency, non-default realm, batched tuple, projection-watch, and rich upload/CAS flows are API-first today. Use the API for production workflows that need those fields. |
| Gateway feature surface | S3 compatibility is partial, static redirect semantics are limited, and package gateways are foundational. Do not import security assumptions from protocols Anvil does not implement. |
| CoreStore verification | There is no general `corestore fsck` command. Use feature diagnostics, repair findings, logs, backups, restore drills, and source-specific smoke tests. |
| Lifecycle | Region activation checkpoint generation and drain-completion workflows are still coarse. Do not work around them with storage edits. |

The conservative posture is straightforward: keep admin private, restrict internal node gRPC to authorised nodes, grant public scopes narrowly, model product sharing with tenant relationship tuples, keep tenants out of the system realm, reject reserved namespaces on every public and gateway path, protect server secrets and key history, use CoreStore preconditions and fences instead of direct writes, log evidence without secrets, and run release gates before deploying.

## Review questions

For every credential, ask what plane it can reach, which actions it can perform, which resource strings it can name, and how it is rotated. For every gateway, ask which hostnames are trusted, how public access is intentionally enabled, and whether the gateway can reveal data that native object reads would hide. For every admin principal, ask which system-realm relation justifies the access.

A secure deployment should fail closed. Missing auth should not become anonymous access, denied relationship checks should not become partial search snippets, and reserved namespace requests should not reveal whether internal records exist.
