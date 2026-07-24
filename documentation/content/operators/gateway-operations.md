---
title: Gateway Operations
description: Operate S3, static, host-alias, link, and package-gateway surfaces as adapters over Anvil's native model.
---

# Gateway Operations

A gateway is the part of Anvil that lets an outside protocol reach native Anvil state. S3 clients speak buckets, keys, ETags, versions, and multipart uploads. Browsers speak hosts, paths, redirects, headers, and caches. A future package registry would speak repositories, packages, blobs, manifests, tags, upload sessions, and bearer tokens. Those protocols are useful because existing tools already understand them, but they are not Anvil's durable truth and they are not separate security models.

Inside Anvil, a gateway request still resolves to native concepts: storage tenant, bucket, object key, object version, current pointer, metadata, link, host alias, region, principal, public policy scope, relationship-authorisation check, CoreStore record, watch cursor, diagnostic, and audit event. If an operator lets the gateway protocol become the design centre, production incidents become harder to explain. A failed S3 signature can be mistaken for missing data. A public static route can be mistaken for a permission grant. A package-style `latest` name can be mistaken for a copied artefact. This page explains how to operate gateways without losing the native model underneath.

Read this chapter with [Gateways](/learn/gateways/), [Object Model](/learn/object-model/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Authorisation](/learn/authorisation/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Network and Ports](/operators/network-and-ports/), [Observability](/operators/observability/), [S3-Compatible Gateway](/tutorials/s3-gateway/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), [Package Gateway Foundations](/tutorials/package-gateway-foundations/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## The gateway operating model

Operate every gateway as an adapter over the native public API and CoreStore-backed records. The gateway may translate headers, parse hostnames, verify a protocol signature, return protocol-shaped errors, or keep short-lived request state. It should not become a second database, a second identity system, or a private shortcut around tenant authorisation.

A useful mental model is:

| Gateway concern | Native Anvil concern |
| --- | --- |
| Endpoint, host, path, method, and protocol headers | Public listener, routing record, tenant locator, bucket locator, host alias, and object route. |
| Protocol credential such as an S3 access key | Anvil app credential, bearer token, public policy scopes, and relationship checks. |
| Bucket, key, version id, ETag, range, or metadata header | Bucket, object key, object version, current pointer, content identity, and object metadata. |
| Static path, `latest` URL, or package channel | Object link, link generation, target key, target version where exposed, and public-read policy. |
| Protocol response, request id, or error code | Native outcome translated back to the caller, with logs, diagnostics, and audit evidence. |

The native API remains the richer contract. Use it when a workflow needs Anvil-specific correctness: object idempotency keys, compare-and-swap preconditions, fenced mutations, link generation checks, structured metadata, watches, index catch-up, relationship schemas and tuples, PersonalDB commits, repair, or diagnostics. Use a gateway when the client is already protocol-shaped and the operation maps cleanly to Anvil.

## Keep the planes separate

The public plane is for tenant applications, native public API calls, S3-compatible traffic, static object delivery, public-read reads, and tenant-owned host aliases. The admin plane is for operators managing system bootstrap, topology, routing repair, secret rotation, diagnostics, repair, and system-realm authorisation. Internal node services also use the public gRPC listener, but require node credentials and are never gateway routes. A gateway belongs on the public side unless it is an operator-only administrative lifecycle command.

The current server exposes the native public gRPC API and the S3/static HTTP gateway from the same public listener. Requests with a gRPC content type go to public services; ordinary HTTP requests are handled by the S3/static gateway path. This is why exposing the public listener can make native clients, S3 tools, and browser static reads work through one application-facing endpoint.

The admin listener is separate. It is configured with `ADMIN_LISTEN_ADDR`, defaults to loopback, and still requires normal admin authentication and system-realm authorisation. Do not expose the admin API merely because a gateway, static site, package download, or S3 client needs network access. If the admin listener is deliberately bound off loopback with `ALLOW_PUBLIC_ADMIN_LISTENER=true`, treat that as private-network operator access, not as an internet-facing gateway.

## S3 gateway operations

The implemented S3-compatible gateway maps common S3 object operations into ordinary Anvil object operations. The S3 access key id is an Anvil app `client_id`, and the S3 secret key is that app's `client_secret`. The gateway verifies AWS Signature Version 4 for signed requests, checks request freshness, resolves the app to an Anvil principal, and then runs normal public policy and relationship-authorisation checks.

Signed S3 writes are not privileged writes. A `PUT Object` creates a new Anvil object version and moves the current pointer only if the principal is authorised. A `GET` or `HEAD` reads the current version, or a pinned version when the S3 request supplies a version id. Prefix listing is Anvil listing with gateway-shaped output. Multipart upload uses Anvil multipart state and commits a normal object version on completion. Copy, delete markers, range reads, simple `x-amz-meta-*` metadata, read preconditions, some write preconditions, and version-listing paths are currently implemented.

Unsigned S3 `GET` and `HEAD` may reach the object handlers so deliberate public-read buckets can be served. Other unsigned S3 operations are rejected before they mutate state. Public-read is therefore a read policy, not a bypass of the tenant model. If a bucket is public, anyone who can reach the public surface may be able to read matching object bodies and read-side metadata through supported S3/static routes.

S3 compatibility is partial. AWS IAM policy documents, S3 ACLs, S3 bucket-policy JSON, lifecycle rules, event notifications, object tags, CORS management, and S3 website configuration are not Anvil's core control plane today. `GetBucketVersioning` reports Anvil's always-versioned object model, while `PutBucketVersioning` only accepts the implemented enabled/no-op shape rather than suspending versioning. S3 user metadata is simple string metadata; use the native API when rich typed metadata, idempotency, or fenced mutations matter.

When smoke-testing S3, make the evidence explicit. A successful signed upload proves that the client and gateway agreed on SigV4, the app credential was valid, the requested bucket existed for that tenant, the public policy and relationship checks allowed the write, and Anvil committed an object version. It does not prove that an index has caught up, a watch consumer has processed the event, the object is public, or the public CLI can express all metadata used by the upload.

Common S3 failures have different meanings:

| Symptom | What to check first | What it does not prove by itself |
| --- | --- | --- |
| Signature mismatch | Access key, secret key, method, path, query string, signed headers, body hash, clock skew, and proxy host/proto handling. | That the bucket or object is missing. |
| Permission denied | Public policy scope, relationship tuple visibility, tenant mismatch, bucket name, and key resource shape. | That SigV4 failed. |
| Wrong-region or redirect response | Bucket home region, endpoint hostname, route source, and `CROSS_REGION_ROUTING_POLICY`. | That the object body is unavailable in its home region. |
| Reserved namespace rejection | Object key or prefix uses an Anvil-owned internal namespace such as `_anvil/`. | That ordinary application prefixes are blocked. |
| Empty listing | Prefix, delimiter, delete marker, object visibility, list scope, and public-read state. | That every object body is absent. |

The current public object-list scope is coarser than ideal in some paths: ordinary object listing checks `object:list` at bucket level. Do not promise per-prefix S3 listing isolation unless the current authorisation shape for that workflow has been checked. If an application needs precise visibility, design the native object layout, relationship authorisation, and index authorisation together.

## Reverse proxies, hosts, and SigV4

Gateways depend on the effective host. S3 virtual-host style requests, static delivery, custom host aliases, and SigV4 all care which host the caller used. A reverse proxy that terminates TLS or rewrites headers can therefore break both routing and signature verification if Anvil does not trust the forwarded metadata.

`TRUSTED_PROXY_SOURCE_RANGES` controls which proxy source addresses may supply forwarded host and scheme information. When a request comes from an untrusted source, Anvil ignores forwarded host metadata and uses the direct request authority. Ambiguous forwarded host chains are rejected. For S3, this matters because the gateway recomputes the signature against the effective method, host, scheme, path, query, headers, and body hash. A proxy that changes `Host`, `X-Forwarded-Host`, or `X-Forwarded-Proto` without matching the server configuration produces authentication failures that look like bad S3 credentials from the client side.

Operators should test this before exposing gateway traffic through a load balancer:

- A signed S3 `HEAD` for a private object succeeds through the public proxy with the same endpoint host the client signed.
- The same request fails when the secret is deliberately wrong, proving the proxy has not accidentally created unsigned write access.
- A public-read `GET` succeeds only for a deliberately public bucket and only through the intended host route.
- Requests for `_anvil/` or other reserved internal prefixes are rejected through the gateway as well as through native paths.
- Logs contain request ids and safe routing evidence, but not bearer tokens, S3 secrets, signatures, or object bodies.

`PUBLIC_REGION_BASE_DOMAIN` and `CROSS_REGION_ROUTING_POLICY` are also gateway-relevant. Regional hostnames let Anvil parse region, tenant, bucket, and key from host/path shapes. Cross-region policy decides whether a request that arrives in the wrong region is redirected, proxied where supported, or rejected locally. These settings do not grant data access; they decide how a correctly authorised request reaches the bucket's home region.

## Static hosting, public-read, and object links

Static hosting is object delivery over HTTP routing. A host and path are resolved to a tenant, bucket, region, and object key. The read then follows Anvil object rules. If the route is anonymous, the bucket or delivery design must deliberately allow public reading. Static hosting should therefore be operated like public object delivery, not like a filesystem copied to a web server.

A host alias maps a custom hostname to a tenant, bucket, region, and optional key prefix. In the public plane, host aliases are tenant-owned. The current public CLI exposes `anvil host-alias create`, `read`, `verify`, `list`, and `delete`. Creation and verification require bucket write authority; reading and listing require bucket read authority. That is the right model for ordinary tenant publishing because the tenant controls its own bucket and domain proof.

A tenant-owned alias flow looks like this:

```bash
anvil --profile acme host-alias create docs.example.com public-assets \
  --region eu-west-1 \
  --prefix sites/docs/
```

This creates a pending host alias for the authenticated tenant's `public-assets` bucket and prints a verification challenge plus a generation. It proves the caller can reach the public Object API, can write host-alias state for that bucket, and did not collide with a protected native Anvil hostname. It does not prove DNS is configured, TLS is issued, the alias is active, or the bucket is public.

After the operator or tenant DNS process has made the challenge observable, the tenant verifies the alias with the generation printed by the create command:

```bash
anvil --profile acme host-alias verify docs.example.com "$OBSERVED_CHALLENGE" \
  --expected-generation "$ALIAS_GENERATION"
```

This activates the alias only if the challenge matches and the generation has not changed. It proves domain control for that alias lifecycle step and protects against racing updates. It does not change bucket public-read, upload site files, or configure your reverse proxy to forward the host.

A stable public URL often points at an object link rather than a direct object. For example, `releases/latest.tar.gz` can be a link to `releases/app-1.4.2.tar.gz`. Links are not copies. Moving the link changes the alias descriptor and generation; it does not duplicate or rewrite the target object bytes.

```bash
anvil --profile acme object link read s3://public-assets/releases/latest.tar.gz
```

This command reads link metadata through the native public Object API. It proves the caller can inspect the descriptor and see the current target and generation. It does not prove the target body is public, that a static gateway will issue an HTTP redirect, or that the link target still exists if the link was allowed to be dangling.

The public link CLI supports `--resolution follow` and `--resolution redirect` on create/update. Current native object reads follow ordinary follow links. Redirect-style links are metadata that a gateway may use, but do not assume a general HTTP `3xx` redirect surface unless the current static-hosting implementation and tutorial explicitly support the exact route. Dangling links are useful for planned promotions, but followed reads fail until the target object exists.

To make anonymous static delivery possible, use a dedicated public bucket where practical:

```bash
anvil --profile acme bucket set-public public-assets --allow true
```

This changes the bucket policy through the public Bucket API and requires `bucket:write` on `public-assets`. It proves a deliberate public-read policy change. It does not grant writes, expose the admin API, make neighbouring private buckets public, or remove data from downstream caches when you later turn public-read off. Because public-read can expose object names, versions, sizes, content types, simple metadata, and bodies through supported read-side routes, treat the bucket name and metadata as public too.

## Tenant-owned aliases versus admin host-alias lifecycle

Operators have admin host-alias commands because routing state sometimes needs private lifecycle work: inspecting a suspicious alias, suspending a compromised domain, repairing a routing projection, migrating system-owned routes, or recovering from a failed verification flow. That does not make the admin API the normal publishing path for tenants.

Use the public API or `anvil host-alias` when a tenant is creating, verifying, listing, or deleting an alias for its own bucket. Use the admin API only when the operation is genuinely operator-owned and can be justified with an audit reason. The admin command shape makes that explicit:

```bash
anvil-admin --host http://10.10.0.12:50052 host-alias read \
  --hostname docs.example.com
```

This reads system-side host-alias metadata over the private admin API. It proves the admin listener is reachable, the operator credential is authenticated and authorised, and the descriptor can be read. It does not prove the tenant's bucket is public, that DNS reaches this Anvil deployment, or that ordinary tenant credentials can manage the alias.

If an incident requires suspension, use a generation-checked mutation with an audit reason:

```bash
anvil-admin --host http://10.10.0.12:50052 host-alias suspend \
  --hostname docs.example.com \
  --expected-generation 7 \
  --audit-reason 'suspend compromised custom domain during incident'
```

This asks the admin API to move the host alias from its current generation to a suspended state. It proves an authorised operator made an auditable lifecycle change. It does not delete object data, revoke tenant app credentials, or repair the tenant's DNS. After the incident, use diagnostics, tenant audit, and a tenant-visible test path to show what changed.

## Package gateway foundations

Package registries are gateway-shaped, but the current package story is foundational rather than a complete tenant-facing protocol surface. The codebase contains internal gateway-store concepts for repositories, blobs, mutable tags, upload sessions, credentials, mounts, short-lived access tokens, and audit records. Those records are the right kind of durable substrate for future package gateways because they can be tied back to Anvil storage, identity, authorisation, routing, and audit.

Do not describe Docker Registry v2, npm, PyPI, Maven, Cargo, or other package-manager protocols as implemented unless the current repo exposes the protocol handler and public surface you need. Today there is no public `anvil package` or `anvil registry` CLI command, no public package-gateway mount workflow, and no tenant-facing package-manager endpoint documented as ready. S3 is implemented. Static/object delivery is implemented. Package gateway foundations are not the same as a full registry protocol.

You can still model package-like delivery honestly today:

| Package need | Current Anvil primitive |
| --- | --- |
| Immutable artefact bytes | Object under a digest-shaped key. |
| Version manifest | JSON object with package name, version, digest, media type, and size. |
| Integrity | Application-level checksum stored in the key or manifest, not merely the Anvil ETag. |
| Mutable channel such as `latest` | Object link moved with a generation check. |
| Catalogue and search | Typed JSON index over manifest objects, with index lag and diagnostics monitored. |
| Public downloads | Dedicated public-read bucket or authenticated reads through native/S3/static routes. |
| Existing bulk tooling | Native Object API or S3-compatible gateway, depending on the client. |

Tenant package publishing should use tenant-owned public APIs, app credentials, object writes, links, indexes, and public-read policy. The admin API should not upload package artefacts, move `latest`, or fill in missing tenant grants just because it can see system state. If a future package gateway is added, it should translate package protocol calls into the same tenant-owned records and checks.

## Gateway records, audit, and logging

Gateway-facing records are durable Anvil records. Objects, object versions, links, bucket policies, host aliases, routing descriptors, index definitions, package-foundation records, diagnostics, and audit events belong in CoreStore through Anvil services. Derived gateway projections, caches, or route tables should be repairable from those records. If a gateway requires a hidden side database that cannot be audited or rebuilt, it is no longer operating as an Anvil adapter.

Audit should show who changed durable gateway state and why. Tenant-owned changes such as host-alias create/verify/delete, object-link movement, bucket public-read changes, app credential rotation, and package-like object publication belong in tenant audit surfaces. Operator changes such as host-alias suspension, routing repair, topology updates, and admin diagnostics belong in admin audit surfaces and should carry a specific audit reason.

Gateway logs should be useful without becoming data leaks. Record request id, gateway family, method, response status, latency, route source, tenant id or safe tenant locator, bucket or repository where safe, operation name, authorisation outcome, remote address/proxy trust result, and error code. For S3, include `x-amz-request-id` or the corresponding request id so S3 clients can correlate failures. Do not log bearer tokens, app secrets, S3 secret keys, signatures, `Authorization` headers, signed payload fragments, object bodies, PersonalDB changesets, or package artefact bytes.

For operator triage, separate read-only evidence from repair. An admin diagnostic query is a safe starting point:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --severity warning \
  --limit 50
```

This asks the private admin diagnostics service for mesh-related warning findings. It proves the admin plane is reachable and authorised and that the selected diagnostics backend can return findings. It does not repair routing, prove every host alias is healthy, or inspect tenant object bodies. If a derived route projection needs repair, use the documented admin repair or routing repair surface with a narrow scope and audit reason; do not edit storage directly.

Admin audit can then show operator actions:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list \
  --action admin.host_alias.suspend \
  --limit 20
```

This lists matching admin audit events if the caller is authorised. It proves that audit records can be queried for the action, not that the original incident is resolved. Pair audit with before-and-after gateway requests, routing state, tenant-visible reads, and diagnostics.

## Operational incident patterns

A stale static site usually has more than one possible cause. Check that the object version was written, the public-read policy is still deliberate, the host alias is active, the reverse proxy forwards the expected host, the link target and generation are what the publisher intended, and any CDN or browser cache has not retained the old body. Do not start by repairing CoreStore when the evidence points at a moved alias or cache.

A signature failure from an S3 client should start with the protocol edge: clock skew, access key, secret, signed region, endpoint URL, proxy host/proto forwarding, trusted proxy ranges, and whether the client used path-style or virtual-host style addressing. Only after SigV4 evidence is clean should you spend time on object versions or bucket locators.

A wrong-region gateway response is a routing and placement question. Check the bucket home region, the region encoded in the host, `PUBLIC_REGION_BASE_DOMAIN`, `CROSS_REGION_ROUTING_POLICY`, and whether proxying is available in the current deployment. A redirect or local-only rejection does not prove data loss.

A package `latest` mistake is usually a mutable-name mistake. Inspect the immutable version manifest, artefact digest, object link target, link generation, publisher audit trail, and catalogue index lag. The link may have moved while the index is stale, or the index may show the new version while a client cached the old static URL.

A public-read surprise should be treated as a security incident until proved otherwise. Public means anyone who can reach the public surface may read matching data through supported routes. Turn off public-read if appropriate, preserve logs and audit, identify whether object names or metadata were exposed, and remember that caches and clients may retain data that was legitimately public while the policy was enabled.

## Current public surfaces and gaps

The reliable current gateway story is object-focused. The native public API is the source contract. The S3-compatible gateway supports common object operations, SigV4, public-read `GET`/`HEAD`, host-routed object paths, version ids, delete markers, range reads, copy, multipart upload, and simple S3 user metadata. Static object delivery and custom host aliases build on the same public routing model. Tenant host-alias commands exist in the public CLI, and system-side host-alias commands exist in the admin CLI for operator lifecycle and repair.

Current gaps matter for production design:

| Area | Current limitation to plan around |
| --- | --- |
| S3 feature surface | Partial S3 compatibility; AWS IAM policies, ACLs, lifecycle rules, notifications, tags, CORS management, and website configuration are not Anvil control-plane features today. |
| Metadata and uploads | The public CLI `object put` helper does not expose content type, user metadata JSON, or rich CAS/idempotency flags; use the native API or S3 where those fields are required. |
| Listing and visibility | Some list/query scopes are coarser than ideal, including bucket-level object listing checks in current paths. |
| Static redirects | Object links support follow and redirect metadata, but do not rely on general HTTP redirect behaviour unless the exact static/gateway path currently documents it. |
| Host alias lifecycle | Tenants have public host-alias helpers for owned buckets; admin host-alias commands are for operator/system lifecycle, not routine tenant publishing. |
| Package gateways | Package gateway records are foundational; no full Docker, npm, PyPI, Maven, Cargo, or similar protocol gateway is currently exposed as a tenant-facing surface. |
| Cross-region proxying | Redirect/proxy/reject behaviour depends on bucket locators, `CROSS_REGION_ROUTING_POLICY`, and current proxy availability; do not treat generic proxying as the hot path without testing it. |
| Observability | There are logs, request ids, diagnostics, repair, and audit surfaces, but no guarantee of a turnkey gateway dashboard or a single command that certifies every adapter path. |

Operate within those limits. Use gateways to make existing clients useful. Use native APIs where the product needs Anvil-specific correctness and evidence. Keep tenant publishing on the public plane, keep admin lifecycle on the private plane, and make every gateway incident explainable in native Anvil terms.

## Native comparison test

For every gateway incident, reproduce the same logical resource through the native public API. If native `object head` and `object get` fail, investigate storage, policy, relationship visibility, or routing. If native operations succeed while S3 or static HTTP fails, investigate signature calculation, host alias state, virtual-host suffix, proxy headers, cache, or gateway-specific metadata translation.

This comparison keeps gateway debugging from turning into broad storage repair when the source object is healthy.
