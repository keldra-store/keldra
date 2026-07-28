---
title: Authorisation Actions and Resources
description: Reference for Anvil public policy scopes, relationship authorisation resources, admin system-realm relations, zookies, caveat hashes, and current coarse scope limits.
---

# Authorisation Actions and Resources

Anvil has three related authorisation concepts. They are deliberately separate, and a production runbook should name which one it is using.

**Public policy scopes** are action/resource pairs attached to tenant application credentials and minted into public API bearer tokens. They answer questions such as "may this app write this object key?", "may this app create an index?", or "may this app run tenant-scoped repair?" The token scope format is `action|resource`.

**Relationship authorisation** is Zanzibar-style tenant product authorisation. It stores schemas, schema bindings, tuples, usersets, checks, list calls, watches, zookies, and revisions inside an Anvil storage tenant. It answers product questions such as "may user 17 view document 42?" It can be used in addition to public policy scopes; it does not replace the need for a bearer token that may call the service.

**System-realm admin relations** authorise the private admin API. They are relationship tuples in Anvil's built-in system realm, not public policy scopes. They answer operator questions such as "may this admin principal create tenants?" or "may this principal drain a region?" Tenants cannot create, bind, or modify Anvil's internal system schema through public APIs.

Read this page with [Authorisation](/learn/authorisation/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), [CLI Workflows](/reference/cli-workflows/), [Admin Plane](/operators/admin-plane/), [Security Hardening](/operators/security-hardening/), and [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/).

## Scope syntax

A public policy scope inside a bearer token has this exact shape:

```text
action|resource
```

The CLI surfaces usually split the same value into an action argument and a resource argument. For example, granting `object:read` on `documents/tutorial/welcome.txt` stores a policy that can later be minted as:

```text
object:read|documents/tutorial/welcome.txt
```

The action must be one of the public action strings listed later on this page. The resource string is the exact string checked by the service method. It is not a local filesystem path, even when it contains `/`.

The matcher supports three resource pattern forms:

| Pattern form | Behaviour |
| --- | --- |
| Exact string | `documents/tutorial/welcome.txt` matches only that resource. |
| Trailing `*` | `documents/tutorial/*` matches any required resource beginning with `documents/tutorial/`. It is simple prefix matching. |
| Global `*` | Matches every resource string. It exists in the implementation, but ordinary application credentials should not use it. |

A trailing `*` is not a segment wildcard. A pattern such as `documents/*/welcome.txt` is treated as a literal prefix ending before `*`, not as "one path component".

The action matcher also accepts the global action `*` and family wildcard actions such as `bucket:*`, `object:*`, `index:*`, and `coordination:*`. Public tenant delegation rejects wildcard actions and the global wildcard resource. Admin policy mutation currently validates only that the action and resource are non-empty, so operators must keep least-privilege discipline in their provisioning workflow.

## Tokens and delegation

When an application exchanges its client id and secret for a token, Anvil mints scopes from that app's stored public policies. If the caller asks for no explicit scope list, the current public authentication service mints the approved policies for that app. If a caller asks for explicit scopes, only stored policies that cover the requested action/resource pair should be approved.

Public policy delegation is non-escalating. A tenant app can grant another tenant app only when the caller already holds:

| Needed by grant path | Why |
| --- | --- |
| `policy:grant` on the target resource | Authorises writing the policy record. |
| The delegated action on the same target resource | Prevents a caller from granting authority it does not already have. |

Public delegation rejects empty resources, the global resource `*`, `system:` resources, `anvil_mesh:` resources, reserved `_anvil/` resources, and cross-tenant `tenant:` or `tenant-` resources. That keeps tenant-owned delegation from becoming a path to system administration or reserved internal state.

Revocation is authorised with `policy:revoke` on the target resource. Reading another app's grants requires tenant app-management read authority and policy read/grant/revoke coverage for the returned resources.

## Relationship authorisation model

Relationship authorisation has its own data model:

| Term | Meaning |
| --- | --- |
| Storage tenant | The Anvil tenant that owns the relationship records. Public callers may only use their authenticated tenant id. |
| Authz realm | A namespace container within the storage tenant. The default realm id is `default`. The system realm id is `system` and belongs to Anvil's built-in admin model. |
| Namespace | A product object type such as `document`, `folder`, or `project`. Internally, Anvil stores it with the realm prefix. |
| Object id | The product object identifier within a namespace. |
| Relation | The named relationship or permission, such as `viewer`, `editor`, or `owner`. |
| Subject kind | Usually a product subject kind such as `user`, `group`, or `userset`. |
| Subject id | The product subject id. For `userset`, the current format is `namespace/object_id#relation`. |
| Tuple | One fact: namespace, object id, relation, subject kind, subject id, optional caveat hash, operation, and revision. |
| Userset | A subject that points at another object relation, allowing nested relationship checks. |
| Schema | A tenant-defined set of namespaces, relations, and relation rules. Current rules include inheritance, computed usersets, and tuple-to-userset forms. |
| Schema binding | The active schema reference bound to an authz realm. Binding has a generation for CAS-style updates. |
| Zookie | An opaque revision token for authz reads and checks. Current format is `authz:<revision>`. |

Tenant applications can write their own tenant authz schemas and tuples when they hold the relevant public policy scopes. They cannot write Anvil's system realm or change the built-in `anvil-system` schema that authorises the private admin API.

Authz read and check APIs support these consistency strings today:

| Consistency | Zookie requirement | Behaviour |
| --- | --- | --- |
| Empty or `latest` | None | Reads at the latest available authz revision. |
| `at_least` | Requires `authz:<revision>` | Fails with `AuthzRevisionUnavailable` if the current revision is below the requested revision; otherwise reads latest. |
| `exact` | Requires `authz:<revision>` | Reads at the exact requested revision if the retained journal can answer it. |

Current caveat support is limited. `caveat_hash` may be empty or a 64-character hex string. Anvil stores and matches the hash; it does not evaluate caveat expressions from the hash today. Direct tuple checks must match the requested hash. Userset expansion follows uncaveated userset edges; a userset edge with a non-empty caveat hash is not used by the current resolver.

## Public actions by API family

The tables below list current public policy actions and the resource strings checked by the source. When a row says "API-only", the action exists in the API or service implementation but may not have a public CLI command.

### Buckets and bucket metadata

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create bucket | `bucket:create` | Bucket name, for example `documents` | Region validity is a separate topology concern. |
| Delete bucket | `bucket:delete` | Bucket name | Deletion also checks bucket emptiness/retention state. |
| List buckets | `bucket:list` | `*` | Current coarse scope: there is no narrow "list only one bucket name" resource. |
| Read bucket policy | `bucket:read` | Bucket name | Reads current bucket policy JSON. |
| Set bucket public-read policy through Bucket service/public CLI | `bucket:write` | Bucket name | Public-read is a data exposure decision, not an admin bypass. |
| Set public access through the Auth service method | `policy:grant` | `bucket:<bucket>` | Current secondary API shape; the public CLI uses the Bucket service path. |
| Watch bucket metadata | `bucket:watch` | Bucket name, or `*` for all buckets in the tenant | API surface exists; current public CLI does not expose bucket metadata watch. |

### Objects and object versions

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Put object | `object:write` | `bucket/key`, for example `documents/tutorial/welcome.txt` | Reserved `_anvil/` object keys are rejected. |
| Copy, compose, JSON patch, manifest CAS, multipart operations | `object:write` | Destination `bucket/key` | Source object reads may also need object read authority depending on the operation path. |
| Delete current object or version | `object:delete` | `bucket/key` | Deletion is distinct from write. |
| Get object body | `object:read` | `bucket/key`, or matching relationship `object` reader | Public-read buckets can permit anonymous reads through public surfaces. |
| Head object | `object:read` | `bucket/key`, or matching relationship `object` reader | Returns current or requested version metadata depending on API field. |
| List objects by prefix | `object:list` | Bucket name | Current coarse scope: prefix-specific `object:list` is not enforced by ordinary object listing. Results are then filtered by object read visibility on private buckets. |
| List object versions | `object:list` | Bucket name | Public-read buckets bypass bearer-token list checks where the public route is used. |
| Watch object prefix | `object:list` | Bucket name | Current coarse scope: watch authorisation is bucket-level, not prefix-level. |

Object visibility can combine public policy and relationship authorisation. For private object reads and index results using `inherit_object`, Anvil can allow access if the caller has the relevant `object:read` scope or a relationship authorisation match using namespace `object`, object id `bucket/key`, and relation `reader`.

### Object links and static aliases

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create or update object link | `object:write` | `bucket/link_key` | The link is an alias descriptor, not a copy of the target object. |
| Read object link metadata | `object:read` | `bucket/link_key` | Current public CLI exposes same-bucket links. |
| Delete object link | `object:delete` | `bucket/link_key` | Update/delete also use link generation checks. |
| List object links | `object:list` | `bucket/prefix` | Returned links are filtered by `object:read` on each `bucket/link_key`. This differs from ordinary object listing, which checks the bucket name. |
| Create, verify, or delete tenant host alias | `bucket:write` | Bucket name | Host aliases map a hostname to a bucket/prefix; DNS, TLS, and reverse proxy setup are outside this scope. |
| Read or list tenant host aliases | `bucket:read` | Bucket name | Listing filters aliases to buckets the caller can read. |

Admin host-alias lifecycle uses system-realm `manage_host_aliases`, not these public bucket scopes.

### Tenant apps and public policies

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create tenant app credential | `app:create` | `tenant:<tenant_id>` | Current coarse scope: tenant-level, not per app name. |
| List tenant apps | `app:read` | `tenant:<tenant_id>` | Needed before public CLI `app list`. |
| Rotate tenant app secret | `app:rotate_secret` | `tenant:<tenant_id>` | Current coarse scope: tenant-level. |
| Delete tenant app | `app:delete` | `tenant:<tenant_id>` | Current coarse scope: tenant-level. |
| Read stored grants | `policy:read` | Grant resources being returned, plus tenant app-management read | Results are filtered to grants covered by policy read/grant/revoke. |
| Grant policy | `policy:grant` plus delegated action | The resource being granted | Public delegation is non-escalating. |
| Revoke policy | `policy:revoke` | The resource being revoked | Does not require holding the delegated action. |

### Relationship authorisation (`authz`)

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Write tuple | `authz:tuple_write` | `namespace/object_id#relation` | Tuple operation strings are `add` or `remove`. |
| Batch write tuples | `authz:tuple_write` | Each tuple's `namespace/object_id#relation` | Batch must target one authz scope. |
| Read tuples with full filter | `authz:tuple_read` | `namespace/object_id#relation` | Empty filter components broaden the resource as below. |
| Read all tuples in namespace | `authz:tuple_read` | `namespace` | Current filter resource when object id and relation are empty. |
| Read tuples for a relation across objects | `authz:tuple_read` | `namespace/*#relation` | Produced by the current filter-resource helper. |
| Read tuples for all relations on one object | `authz:tuple_read` | `namespace/object_id#*` | Produced by the current filter-resource helper. |
| Read all tuples | `authz:tuple_read` | `*` | Broad and should be avoided for application credentials. |
| Check permission | `authz:check` | `namespace/object_id#relation` | Supports `latest`, `at_least`, and `exact` consistency in the API. |
| List objects for subject/relation | `authz:tuple_read` | `namespace/*#relation` | Returns object ids reachable for the subject. |
| List subjects for object/relation | `authz:tuple_read` | `namespace/object_id#relation` | Returns matching subjects. |
| Put schema revision | `authz:schema_write` | Each namespace in the schema | Caller must cover every namespace supplied. |
| Bind schema to realm | `authz:schema_write` | Authz realm id, for example `default` | Binding has an expected-generation field. |
| Get schema by id | `authz:schema_read` | Schema id | Used by the schema-ref path. |
| Get current namespace schema | `authz:schema_read` | Namespace, or `*` for all namespaces | Listing all schemas is broad. |
| Get schema binding | `authz:schema_read` | Authz realm id | Reads the active binding for a realm. |
| Watch tuple log | `authz:watch` | Namespace, or `*` if namespace is empty | Watch is scoped to the requested authz realm. |
| Watch namespace schema events | `authz:watch` | Namespace | API-only watch surface. |
| Watch derived authz lag | `authz:watch` | Derived index id | API-only watch surface. |

### Indexes, search, diagnostics, and query visibility

In Anvil 0.4.0, index list, query, and diagnostics remain available, but their
completeness and freshness are not release-qualified. Index watch RPCs return
`Unimplemented`, and `index:watch` cannot be delegated through tenant access
grants.

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create index | `index:create` | `bucket/index_name` | Applies to path, metadata, typed JSON, full-text, vector, hybrid, and accepted source kinds. |
| Update index | `index:update` | `bucket/index_name` | Also used by disable. |
| Drop index | `index:delete` | `bucket/index_name` | Removes the definition. |
| List indexes | `index:read` | Bucket name | Current coarse scope: not one index definition. |
| Query index | `index:read` | Bucket name | Available as a non-authoritative preview in 0.4.0; completeness and freshness are not release-qualified. |
| Index diagnostics | `index:read` | Bucket name | Available in 0.4.0. |
| Watch index definitions | `index:watch` | Bucket name | Unavailable in 0.4.0. |
| Watch index partitions | `index:watch` | Bucket name | Unavailable in 0.4.0. |

For `inherit_object` indexes, returned hits are additionally filtered by object visibility. `index_only` and `public` modes expose index rows to callers with `index:read` on the bucket, so do not use them for sensitive object-derived data unless that is intended.

### Watches

| Watch surface | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Bucket metadata watch | `bucket:watch` | Bucket name or `*` | API-only in current public CLI. |
| Object prefix watch | `object:list` | Bucket name | Current scope is bucket-level, not prefix-level. |
| Index definition watch | `index:watch` | Bucket name | Returns `Unimplemented` in 0.4.0. |
| Index partition watch | `index:watch` | Bucket name | Returns `Unimplemented` in 0.4.0. |
| Authz tuple watch | `authz:watch` | Namespace or `*` | Public CLI exposes tuple-log watch by namespace. |
| Authz namespace and derived-lag watches | `authz:watch` | Namespace or derived index id | API-only surfaces. |
| PersonalDB group/projection watch | `personaldb:watch` | PersonalDB resource or projection resource, or matching relationship | Public CLI exposes compact PersonalDB watch helpers. |
| Git source watch | `git_source:watch` | `repository:<repository_id>` | Returns `Unimplemented` in 0.4.0. |

### Append streams

Append streams are modelled under the object family today.

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create append stream | `object:write` | `bucket/stream_key` | Stream key is object-like. |
| Append record | `object:write` | `bucket/stream_key` | Uses object mutation context and optional write preconditions. |
| Seal append stream segment | `object:write` | `bucket/stream_key` | Segment sealing is not a general close permission. |
| Read append records | `object:read` or `object:list` | Bucket name | Current read path accepts either bucket-level object read or bucket-level object list. |
| Tail append stream | `object:read` or `object:list` | Bucket name | Tail repeatedly uses the same read path. |

### Leases and fenced ownership

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Acquire task lease | `coordination:lease_write` | `task_lease/<task_id>` | Owner is derived from the authenticated caller and optional label; callers cannot spoof another principal. |
| Checkpoint task lease | `coordination:lease_write` | `task_lease/<task_id>` | Fence token is checked by the service. |
| Commit task lease | `coordination:lease_write` | `task_lease/<task_id>` | Completes the lease for the owning principal/fence. |
| Read task lease | `coordination:lease_read` | `task_lease/<task_id>` | Read is separate from write. |
| Force release task lease | `coordination:lease_admin` | `task_lease/<task_id>` | Tenant-scoped administrative lease release. |
| Acquire, renew, transfer, or non-forced release ownership fence | `coordination:lease_write` | `ownership/tenant-<tenant_id>/<resource_kind>/<resource_id>` | API-only public ownership-fence surface. |
| Force expire ownership fence or forced release | `coordination:lease_admin` | `ownership/tenant-<tenant_id>/<resource_kind>/<resource_id>` | Administrative tenant coordination action. |

### PersonalDB

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create group | `personaldb:create` | `tenant-<tenant_id>/<database_id>` | Group identity is tenant-scoped. |
| Read group | `personaldb:read` or relationship `personaldb` reader | `tenant-<tenant_id>/<database_id>` | Relationship fallback uses namespace `personaldb`, object id equal to the resource string, relation `reader`. |
| Create projection | `personaldb:create` | `tenant-<tenant_id>/<database_id>/projections/<projection_id>` | Projection definition is tenant-owned. |
| Read projection | `personaldb:read` or relationship `personaldb_projection` reader | Projection resource | Relationship fallback uses namespace `personaldb_projection`. |
| Watch group/projection | `personaldb:watch` or relationship watcher | Group or projection resource | Watch relation is `watcher`. |
| Submit changeset | `personaldb:commit` or relationship committer | Group resource | Session principal must match the authenticated bearer when a bearer is bound. |
| Row effects | `personaldb:insert`, `personaldb:update`, `personaldb:delete`, or another parsed action from the verified effect | `tenant-<tenant_id>/<database_id>/<resource_type>/<resource_id>` | Effects can require additional scopes or relationship permissions per resource binding. |

### Hugging Face integration

The current source includes Hugging Face key and ingestion actions. Treat these as tenant integration permissions, not admin permissions.

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Create key | `hf_key:create` | Key name | Stores encrypted server-side credential material. |
| Delete key | `hf_key:delete` | Key name | Removes a named key. |
| List keys | `hf_key:list` | `*` | Current coarse scope: no narrow list resource. |
| Start ingestion | `hf_ingestion:create` | `*` | Current coarse scope: start is global within the tenant. |
| Read ingestion status | `hf_ingestion:read` | Ingestion id | Status lookup by id. |
| Cancel ingestion | `hf_ingestion:delete` | Ingestion id | Cancels an ingestion job. |

`hf_key:read` exists in the action parser, but the current service paths above use create, delete, and list.

### Git source

Anvil 0.4.0 enables only tenant-administrator pack ingest. Git actions cannot
be delegated through tenant access grants, and Git query/watch RPCs return
`Unimplemented`.

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Write source pack | `git_source:write` | Tenant administrator | Service stores pack objects under object storage internally; repository-scoped delegation is unavailable in 0.4.0. |
| Read source query/blob data | `git_source:read` | `repository:<repository_id>` | Returns `Unimplemented` in 0.4.0. Read the stored pack through the authorised object API or S3 gateway. |
| Watch source records | `git_source:watch` | `repository:<repository_id>` | Returns `Unimplemented` in 0.4.0. |

### Repair, diagnostics, and tenant audit

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Repair index | `repair:run` | `bucket/index_name` | Tenant-scoped repair. |
| Repair directory index | `repair:run` | Bucket name | Tenant-scoped directory/path repair. |
| Repair authz derived userset index | `repair:run` | `tenant-<tenant_id>/authz/<derived_index_id>` | Tenant-scoped authz derived repair. |
| Repair PersonalDB log chain | `repair:run` | `tenant-<tenant_id>/<database_id>` | Tenant-scoped PersonalDB repair. |
| List repair findings | `repair:read` | Scope id supplied in the request | Scope kind is separate metadata; the scope id is what is authorised. |
| Public index diagnostics | `index:read` | Bucket name | There is no separate public `diagnostics:*` action today. |
| Public tenant audit list | Authenticated tenant token only | Tenant id from claims | Current coarse surface: no separate public `audit:*` action is checked for tenant audit listing. Admin audit uses a system relation. |

### Internal proxy

| Operation | Action | Resource checked | Notes |
| --- | --- | --- | --- |
| Node-to-node object proxy | `internal:proxy_object` | Internal proxy resource | This is not user-facing tenant authority. Do not grant internal proxy actions to tenant applications. |

## System realm and admin relations

The admin API uses bearer-token authentication plus relationship authorisation in the built-in system realm. The system storage tenant id is `0`, the system realm id is `system`, the system schema id is `anvil-system`, and the system namespace is `anvil_mesh`. The checked object is the mesh object for the configured mesh id.

These relations authorise current admin RPC families:

| System relation | Admin operation families |
| --- | --- |
| `manage_tenants` | Create storage tenants. |
| `manage_apps` | Create and rotate tenant application credentials through the admin plane. |
| `manage_policies` | Grant and revoke tenant public policy scopes through the admin plane. |
| `manage_secret_encryption_keys` | Rotate server-side secret encryption envelopes. |
| `manage_buckets` | Admin bucket creation and public-access changes. |
| `manage_regions` | Region lifecycle plus cell register/activate/drain/remove/list. |
| `manage_nodes` | Node register/activate/drain/force-offline/remove/list. |
| `manage_routing` | List and repair materialised routing records. |
| `manage_host_aliases` | Admin host-alias create/activate/suspend/delete/read/list. |
| `manage_links` | Relation exists in the system schema; the current `anvil-admin` CLI does not expose an admin object-link family. |
| `run_repair` | Run administrative repair jobs. |
| `view_diagnostics` | List administrative diagnostics. |
| `view_audit_log` | List administrative audit events. |

These are not public policy actions. Do not attempt to grant `manage_tenants` or `view_audit_log` with `anvil auth grant` or public policy records. Tenants may define product namespaces and schemas in their own authz realms, but they do not define or mutate Anvil's built-in system realm/admin namespaces.

## Common resource strings

Use this table as a quick lookup when designing least-privilege app credentials. The service method's current check is the source of truth.

| Goal | Action | Resource |
| --- | --- | --- |
| Create bucket `documents` | `bucket:create` | `documents` |
| List tenant buckets | `bucket:list` | `*` |
| Read or change public-read policy on `documents` | `bucket:read` / `bucket:write` | `documents` |
| Write one object | `object:write` | `documents/tutorial/welcome.txt` |
| Read one object | `object:read` | `documents/tutorial/welcome.txt` |
| Delete one object | `object:delete` | `documents/tutorial/welcome.txt` |
| List objects under any prefix in a bucket | `object:list` | `documents` |
| Create or update link `latest.txt` | `object:write` | `documents/releases/latest.txt` |
| Create app credentials in tenant 42 | `app:create` | `tenant:42` |
| Read app grants in tenant 42 | `app:read` and `policy:read` | `tenant:42` plus covered grant resources |
| Grant document read | `policy:grant` plus `object:read` | `documents/tutorial/welcome.txt` |
| Create index `by_status` | `index:create` | `documents/by_status` |
| Query any index in bucket `documents` | `index:read` | `documents` |
| Watch index definitions in bucket `documents` | `index:watch` | `documents` |
| Write tuple for document viewer | `authz:tuple_write` | `document/doc-42#viewer` |
| Check document viewer | `authz:check` | `document/doc-42#viewer` |
| Put schema for namespace `document` | `authz:schema_write` | `document` |
| Bind default authz realm | `authz:schema_write` | `default` |
| Acquire task lease `import-docs` | `coordination:lease_write` | `task_lease/import-docs` |
| Read task lease `import-docs` | `coordination:lease_read` | `task_lease/import-docs` |
| Create PersonalDB group `notes` in tenant 42 | `personaldb:create` | `tenant-42/notes` |
| Commit PersonalDB changeset | `personaldb:commit` | `tenant-42/notes` |
| Run index repair | `repair:run` | `documents/by_status` |
| Read repair findings for an index scope | `repair:read` | Service-specific scope id |

## Current coarse scopes and gaps

Some current checks are broader than an ideal product policy model. Treat these as implementation facts to design around, not as reasons to use broad grants silently.

| Area | Current behaviour |
| --- | --- |
| Bucket listing | `bucket:list` checks `*`, so there is no narrow resource for listing only one bucket name. |
| Object listing and object prefix watch | `object:list` checks the bucket name. Prefix-specific list/watch grants are not enough for ordinary object listing. |
| Public CLI upload helper | The CLI discovers the bucket id with `ListBuckets`, which can require broader bucket-list authority than the object write itself. The API/Rust client is better for strict least-privilege upload tests. |
| Object links | Link list checks `object:list` on `bucket/prefix`, but ordinary object list checks only the bucket name. Keep the distinction clear in runbooks. |
| Index read/query/diagnostics | `index:read` checks the bucket name, not `bucket/index_name`. Create/update/delete remain index-definition scoped. |
| Index watches | `index:watch` checks the bucket name. |
| App lifecycle | App create/read/rotate/delete checks `tenant:<tenant_id>`, not one app name. |
| Policy listing | Requires app-management read at tenant level and filters returned grant records by policy coverage. |
| Tenant audit listing | Current public tenant audit listing uses authenticated tenant identity and does not check a separate public audit action. |
| HF list/start | Key listing and ingestion start currently check `*` within their action families. |
| Authz caveats | Caveat hashes are stored and matched; expressions are not evaluated today. |
| System realm management | The admin system realm is bootstrapped and checked by server code. There is no public tenant path to alter its schema. |

If an application needs a narrower boundary than the current service checks provide, use an API path that checks the narrower resource, add the missing enforcement before relying on it, or keep the broader grant in a temporary controlled smoke-test profile rather than in a production app credential.

## Grant review examples

A grant such as `object:read|documents/public/*` is understandable: it names one action and one prefix. A grant such as `object:*|*` is an ownership-level grant and should have an explicit reason, owner, and review date. A grant to manage relationship tuples should be held by the service that owns the product authorisation model, not by every reader app.

When a request is denied, compare the action/resource checked by the service with the action/resource stored in policy. Many mistakes are simple shape mismatches: granting a bucket name when the service checks a bucket/key resource, or granting index definition management when the caller only needed query permission.
