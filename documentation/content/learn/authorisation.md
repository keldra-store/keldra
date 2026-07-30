---
title: Authorisation
description: Understand Anvil authentication, public policy scopes, relationship authorisation, realms, schemas, tuples, zookies, system-realm boundaries, and current implementation limits.
---

# Authorisation

Authorisation answers a narrower question than identity: given this authenticated caller, this tenant, this resource, and this requested operation, is the operation allowed? Anvil has to answer that question for object reads, bucket creation, app management, relationship tuple writes, index queries, watches, gateways, public reads, repair jobs, and private admin operations.

The model has two layers that are easy to blur if you come from a single-role system. **Public policy scopes** decide whether an application principal may call a public Anvil API method over a resource string. **Relationship authorisation** decides whether a product subject is related to an application object, using tuples and usersets. The private admin API uses a separate **system realm**. A tenant can be powerful inside its own storage tenant and still have no authority to create tenants, change mesh topology, or rewrite Anvil's built-in admin relations.

Read this page with [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Authorisation Grants and Revokes](/tutorials/authorisation/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), and [Admin Plane](/operators/admin-plane/). Object and query implications are covered in [Object Model](/learn/object-model/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Indexes and Query](/learn/indexes-and-query/), [Public Access](/tutorials/public-access/), and [S3 Gateway](/tutorials/s3-gateway/).

## Authentication is not authorisation

Authentication proves who made the request. In Anvil public APIs, an app exchanges its client id and client secret for a bearer token. The token contains a tenant id, a subject such as the app id, and minted public policy scopes. That token says, "this is app 17 in tenant 3, with these scopes, until expiry".

Authorisation then asks whether those scopes and any relevant relationship facts permit the operation. A token may authenticate the `docs-reader` app correctly and still fail an object read because it lacks `object:read` for that object and has no object-reader relationship. A token may let an owner app create another tenant-owned app and still fail a private admin call because app-management scopes are not system-realm admin relations.

This separation matters for product users. Your application may have end users such as `user:amy` and `user:ben`. Those are usually relationship-authorisation subjects, not Anvil login accounts. An Anvil app principal can write or check tuples about them when it has the right public policy scopes. That does not mean those users can authenticate to Anvil directly unless your deployment adds such an authentication flow.

## Storage tenants and product users

An **Anvil storage tenant** is the storage and policy isolation boundary. Buckets, objects, app credentials, public policy grants, indexes, watches, repair findings, PersonalDB groups, and tenant-owned relationship tuples belong to one storage tenant. The public API derives the storage tenant from the authenticated token and rejects relationship-authorisation scopes that name a different storage tenant.

A storage tenant is not the same thing as a product user. One storage tenant may hold a whole customer workspace containing thousands of product users. Those users can be represented as relationship subjects:

```text
user:amy
user:ben
group:legal
service:importer
app:17
```

Anvil's built-in object read path checks the authenticated app principal. It first accepts a matching public policy scope such as `object:read|documents/contracts/nda.pdf`. If the scope is absent, it can also check the default relationship-authorisation realm for the built-in object relation:

```text
object/documents/contracts/nda.pdf#reader <- app:17
```

That gives applications two tools. Public policy scopes are good for service-principal API authority. Relationship tuples are good for product sharing models, inherited membership, and object-level visibility that changes without rotating app secrets.

## Public policy scopes

Public policy scopes are minted into bearer tokens and checked by public/data-plane services. The scope format is:

```text
action|resource
```

For example, `object:write|documents/inbox/report.json` lets the token ask the Object API to write that exact object resource. `authz:tuple_write|document/doc-42#viewer` lets the token ask the Auth service to write that relation. `index:read|documents` lets the token query or list indexes in the `documents` bucket under the current implementation.

Scopes are attached to tenant apps. The private admin API can grant scopes during initial tenant handover. A tenant app can also delegate scopes through the public API when it has both `policy:grant` for the resource and the action it is trying to delegate. That non-escalation rule is important: a principal cannot grant `object:delete` on a resource unless it already has `object:delete` there.

Public delegation also rejects the global wildcard resource, wildcard actions, cross-tenant tenant resources, `system:` resources, `anvil_mesh:` resources, and reserved `_anvil/` resources. Those checks are there to stop tenant-side delegation from becoming private admin authority or internal-state access. Use exact resources or narrow suffix-prefix patterns, and treat broad grants as operational exceptions rather than examples.

Current resource checks are not uniformly fine-grained. Object listing currently checks `object:list` on the bucket name, not on an individual prefix. Index list, query, and diagnostics currently check `index:read` on the bucket name, not on one index definition. Tenant app lifecycle checks use `tenant:<tenant_id>`, not one app name. These are implementation boundaries, not design ideals; document them in runbooks and avoid pretending a narrower scope will be enforced where the service does not currently check it.

## Relationship authorisation

Relationship authorisation stores facts about subjects and objects. The vocabulary is small, but each word matters.

A **realm** is a relationship-authorisation scope inside a storage tenant. The API field is `AuthzScope { anvil_storage_tenant_id, authz_realm_id }`. If callers omit it, Anvil uses the tenant from the token and the default realm id `default`. The public CLI mostly works in that default realm; schema binding commands expose a realm id, while tuple, check, read, and watch helpers currently do not expose a general realm flag.

A **namespace** names a kind of object in the relationship model, such as `document`, `project`, `folder`, or `object`. A namespace is a safe component, so it cannot contain slashes. An **object id** names a specific object inside that namespace. Object ids may contain slashes because they are identifiers, not filesystem paths. A **relation** names the relationship being granted or checked, such as `owner`, `viewer`, `member`, or `reader`.

A **subject** is the thing that receives the relation. It has a `subject_kind` and a `subject_id`. A direct tuple might say:

```text
document/doc-42#viewer <- user:amy
```

That means subject kind `user`, subject id `amy`, relation `viewer`, object id `doc-42`, namespace `document`. A tuple can also use subject kind `userset`. A userset subject points at another relation:

```text
document/doc-42#viewer <- userset:document/doc-42#owner
```

That does not copy the current owners into viewer tuples. It says that whoever is currently in the `owner` userset also satisfies `viewer`. Usersets are the foundation for inherited access, group membership, project membership, and similar product rules.

## Schemas and current evaluator limits

A schema is the reviewed contract for a relationship namespace. It records the namespace, intended relations, and relation rules. A schema revision has a digest. A schema binding attaches one schema revision to one realm with a binding generation, so rollout tools can use compare-and-swap instead of overwriting each other silently.

Schemas are important for audit and operations, but you must understand the current evaluator. Today the public tuple/check path stores schemas and bindings, emits namespace watch events, and records revisions. The permission evaluator itself resolves current tuple facts and userset tuples. It does not yet execute a full Zanzibar schema language with computed relation rules and caveat expressions. If your product needs `viewer includes owner` today, write the userset tuple or the direct tuples needed by the current evaluator; do not rely on schema JSON alone to imply access.

That makes schemas a forward-compatible contract rather than decorative documentation. They let teams review the intended model, bind a revision to a realm, watch namespace changes, and prepare for richer validation. They are not currently a substitute for writing the relationship facts that checks can actually resolve.

Anvil owns published namespace hashes. A client may submit `schema_hash`, but
publication canonicalises the namespace semantics and replaces that value with
the deterministic protobuf-domain hash used by this Anvil API generation.
Clients must validate the returned non-empty hash and the canonical namespace,
relations, rules, and allowed subjects. They must not compare it with a digest
calculated over their input JSON or assume their submitted hash round-trips.
For a whole schema revision, use the authoritative schema-reference digest.
Changing the canonical encoding or hash domain requires an API compatibility
change; it cannot silently reinterpret hashes returned by an earlier version.

## Caveats today

The API and tuple records include a `caveat_hash` field. The current implementation validates that it is empty or a 32-byte hex hash and includes it in tuple matching. It does not currently store caveat expression bodies or evaluate request context such as time, device posture, purpose, or region.

Until caveat evaluation exists, treat `caveat_hash` as provenance or an advanced integration hook, not as a complete conditional-access system. If a tuple is meant to expire or depend on external context, enforce that in your application workflow or avoid granting the tuple until Anvil has the caveat semantics you need.

## Revisions, zookies, and consistency

Every committed relationship-authorisation write advances a tenant authz revision. A response zookie is the portable string form of that revision, currently `authz:<revision>`. Reads, checks, object listings, and query paths use revisions so callers can avoid reading from an older permission view than the one they just wrote.

`WriteAuthzTuples` accepts an optional `operation_id` of at most 128 UTF-8 bytes and an optional `expected_revision`. Operation ids are scoped to the authenticated principal and storage tenant. Retrying the same canonical scoped batch with the same operation id returns its original revision, per-tuple results, and zookie even after later writes; reusing that id with different mutations fails with `AuthzOperationConflict`. An `expected_revision` that is not current fails with `AuthzRevisionConflict`. The receipt and tuple batch are committed atomically, so callers do not need to infer success after a lost response.

The consistency options are:

| Option | Meaning |
| --- | --- |
| `latest` or empty | Evaluate at the latest authz revision visible to the service. |
| `at_least` with a zookie | Require the service to be at least as fresh as that revision, otherwise fail with an unavailable revision error. |
| `exact` with a zookie | Evaluate at that exact revision. |

Use `at_least` when a workflow writes a tuple and then needs another process to observe it before returning data. Use `exact` for audit and reproducible checks. The current public CLI check/read helpers send `latest` and print zookies; use the API directly when you need explicit zookie consistency.

Page tokens for authz reads and query results also bind revision and filter context. Do not edit them, reuse them with a different query, or assume they remain valid after a permission change.

## Who creates tenants and apps

Tenant creation is a private admin-plane operation. `CreateTenant` requires the system-realm relation `manage_tenants`. A public tenant app cannot create another storage tenant by receiving a public policy scope, because storage tenants are outside the tenant's own boundary.

The first app for a tenant is normally created by an operator through the private admin API. That is the handover point: the operator creates the storage tenant, creates an initial tenant app, and grants a small set of public policy scopes to that app. The tenant can then use the public API for routine work.

Tenant-owned app creation is public API work once delegated. A tenant app with `app:create` on `tenant:<tenant_id>` can create another app inside the same storage tenant. It can rotate or delete tenant apps only with the corresponding public app scopes. It still cannot change the system realm, create regions, register nodes, rotate server-side secret envelopes, or read admin audit logs.

Public policy delegation is similarly bounded. A tenant app can grant another tenant app only permissions it already holds and only on resources public delegation allows. It cannot grant wildcard authority, cross-tenant authority, reserved `_anvil/` authority, or system-realm admin relations.

## The system realm and private admin API

Anvil's private admin API is authorised by a built-in system realm. The system storage tenant id is `0`, the system realm id is `system`, and the built-in mesh namespace is `anvil_mesh` encoded internally under that realm. Admin relations include capabilities such as `manage_tenants`, `manage_apps`, `manage_policies`, `manage_regions`, `manage_nodes`, `manage_routing`, `run_repair`, `view_diagnostics`, and `view_audit_log`.

The system realm is installed during bootstrap. If it does not exist, startup can create or bind the first system administrator according to bootstrap configuration. Once it exists, stale bootstrap configuration is ignored and admin requests must authenticate and pass system-realm checks normally.

Tenant/public API principals do not define or modify this built-in system realm. A tenant may store a namespace named for its own product model and bind tenant-owned schemas in its own storage tenant. That does not create admin authority, because the private admin checks resolve against storage tenant `0`, realm `system`, and the built-in namespace. Do not expose the admin API as if it were another public tenant API, and do not model admin permissions as public policy grants.

## Reserved internal namespaces and paths

Anvil stores internal control records through CoreStore and sometimes under reserved object-key prefixes. Public object operations reject reserved object keys such as:

```text
_anvil/meta/
_anvil/index/
_anvil/authz/
_anvil/watch/
_anvil/personaldb/
_anvil/git/
_anvil/tmp/
```

That is a security boundary, not a naming preference. A public-read bucket does not make those internal paths readable. A public policy grant should not target `_anvil/` resources. Relationship tuples should model product objects, not Anvil's internal storage layout.

There is a similar naming concern around the system namespace. The built-in `anvil_mesh` namespace in the system realm belongs to Anvil. Tenant-owned authorisation should use product namespaces such as `document`, `project`, or `workspace` and should not try to mirror Anvil's system-realm names.

## Object reads, indexes, and public access

Object reads can be authorised in two ways. A caller with `object:read` for `bucket/key` may read the object. Without that scope, Anvil can check the default relationship-authorisation realm for `object/<bucket/key>#reader <- app:<caller-app-id>`. Object listings and object-version listings also filter results through object visibility unless the bucket is public-read.

Indexes add another layer. To query an index, the caller needs `index:read` on the bucket under the current implementation. If the index definition uses the default `authorization_mode` value `inherit_object`, each returned hit must also pass object visibility. If the definition uses `index_only` or `public`, the query path skips the per-hit object read check; that is safe only when the indexed keys, metadata, text, vectors, and scores are intended for every principal that can query the index. The exact API field is named `authorization_mode`; the prose model is still authorisation.

Public access is not an admin bypass. Current public-read bucket semantics make matching object data readable from public surfaces that can reach the public API, S3 gateway, or static hosting path. They do not make the admin API public, do not let anonymous callers write authz tuples, and do not relax reserved namespace checks. Public-read should be deliberate, auditable, and scoped to data you intend anyone to read.

Gateway behaviour follows the same security model. S3 compatibility and static hosting are adapters over Anvil's buckets, objects, links, metadata, and public-read state. They are not separate places to hide a weaker permission model.

## Watches, derived state, and revocation

Relationship tuples are source records. Tuple writes emit authz watch events and advance derived userset state. Index queries, object visibility filters, page tokens, and repair flows need revisions so revokes stop leaking through stale derived views.

A revoke is not just removing a row from a side table. It writes a new tuple-log record, advances the revision, and must be consumed by any derived view that caches permissions. If a search page, object listing, or projection uses an older authz revision, it may be intentionally stale or must fail based on the consistency the caller requested.

Operationally, this means authorisation belongs in your derived-data design. Watch tuple logs when you maintain permission-sensitive projections. Store the zookie or revision your output was built against. Use repair when a derived authz index or permission-aware index drifts from source tuples.

## Current gaps to design around

The current implementation gives you a usable relationship-authorisation base, but it is not a complete Zanzibar product yet. The important limits are:

```text
schema documents and bindings are stored, but checks resolve tuple/userset facts rather than a full schema rule language
caveat_hash is stored and matched, but caveat expression evaluation is not implemented
public CLI tuple/check/read commands use the default realm and do not expose zookie consistency flags
the public CLI has no helper for batched WriteAuthzTuples
namespace and derived-lag authz watches are API-only; CLI watch helpers cover tuple logs
some public policy resources are coarser than ideal, especially object listing and index read surfaces
```

Design applications with those facts in mind. Use the API directly when a production workflow needs exact consistency, non-default realms, batched writes, or richer evidence than the CLI prints. Keep schemas as reviewed contracts, keep tuple writes explicit, and do not use broad public policy grants to paper over missing surfaces.

## What to take forward

Anvil authorisation is a layered model. Authentication identifies the caller. Public policy scopes authorise public API calls. Relationship tuples and usersets model product access inside a storage tenant. Revisions and zookies make permission freshness explicit. The system realm authorises private admin operations and is not tenant-owned. Public access changes object read visibility, not Anvil's control plane. The safest designs keep those boundaries visible in code, credentials, runbooks, and audit logs.

## Practical modelling pattern

Start with the public policy scope that lets an application call an Anvil service, then add relationship tuples only for product-level visibility. For a document viewer, the app might need `index:read` on the bucket so it can call the query API, while the query itself uses `inherit_object` and relationship tuples such as `document:doc-42#viewer@user:amy` to decide which hits Amy can see. Removing either layer should deny the result: no public scope means the app cannot call the service, and no relationship means the product subject cannot see the object.

Keep admin authority out of that model. A system-realm relation such as `manage_regions` is for operators changing Anvil topology. It is not a stronger tenant role, and it should not be used to let product code bypass public policy or relationship checks.
