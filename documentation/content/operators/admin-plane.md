---
title: Admin Plane
description: Operate Anvil's private administrative API without bypassing authentication, authorisation, or the server-owned storage path.
---

# Admin Plane

The admin plane is the part of Anvil used by operators and trusted automation to change the system itself. It is where you create storage tenants, issue the first application credentials, manage public policy grants, describe regions, cells, and nodes, repair routing projections, rotate server-side secret envelopes, and inspect system diagnostics and audit events. Those operations are more powerful than ordinary tenant object writes, so the admin listener belongs on a private management network.

Private does not mean unauthorised. An admin request is still a normal request to an Anvil server. The caller authenticates with a bearer token, the server verifies that token, and the method checks a Zanzibar-style relation in Anvil's built-in system realm before it performs the operation. The network boundary reduces exposure; it does not replace authentication, authorisation, audit evidence, or consistency checks.

The admin plane also is not a storage maintenance shortcut. Production operations should not mount `STORAGE_PATH` into an operator laptop, run a script against CoreStore files, or give an external tool the server's encryption keys. The Anvil server owns durable state, enforces preconditions, writes audit records, and publishes derived updates. `anvil-admin` is a client for that server path, not a direct storage writer.

For the deployment and network context, read [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), and [Deployment](/operators/deployment/). For the authorisation model behind this page, read [Authorisation](/learn/authorisation/) and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## What belongs on the admin plane

Anvil separates tenant-facing data operations from system operations. A tenant application should create buckets, write objects, manage tenant relationship tuples, build tenant indexes, and publish tenant-owned links through the public API after it has been delegated the relevant scopes. Operators use the admin API for tasks that affect the mesh, the system realm, initial tenant handover, or shared operational state.

A useful way to draw the line is to ask who owns the result. If the result is normal tenant data, use the public API. If the result decides whether a tenant exists, where buckets are placed, which nodes serve a region, how a custom host resolves, whether an encrypted secret can be read after key rotation, or which operator can repair system records, use the admin API.

| Area | Admin responsibility | Tenant/public responsibility |
| --- | --- | --- |
| Tenant handover | Create a storage tenant and first application credential. | Use that credential to call tenant APIs and delegate narrower application authority. |
| Policy | Grant or revoke coarse public policy scopes for tenant applications. | Use delegated scopes, and relationship authorisation, to protect tenant data. |
| Buckets | Bootstrap or repair bucket-level state when operator-owned provisioning is required. | Create and operate ordinary buckets where the tenant has the relevant scope. |
| Topology | Manage regions, cells, nodes, lifecycle state, and routing records. | Choose permitted bucket regions and follow redirects or proxy behaviour exposed through public endpoints. |
| Gateways and hosts | Manage system-side host alias lifecycle and routing repair. | Manage tenant-owned host aliases and object links through public APIs where supported. |
| Secrets and diagnostics | Rotate server-side secret envelopes, run admin diagnostics, list admin audit events, and run system repairs. | Rotate tenant application secrets through allowed tenant/admin flows and use tenant diagnostics for tenant-scoped checks. |

The current `anvil-admin` command families reflect this split: `tenant`, `app`, `policy`, `bucket`, `region`, `cell`, `node`, `host-alias`, `routing`, `repair`, `diagnostics`, `audit`, and `secret-encryption-key`. The `key` family is different: it is a local helper for generating server configuration material and does not contact the admin API.

## The admin listener is a private trust surface

The public listener, configured by `API_LISTEN_ADDR`, carries the native public API and the S3/static gateway multiplexer. It is the listener you normally place behind tenant-facing load balancers and gateway routes. The admin listener, configured by `ADMIN_LISTEN_ADDR`, carries only the private admin gRPC service. Its default is `127.0.0.1:50052` so a single-node deployment can run local operator commands without publishing that port.

If `ADMIN_LISTEN_ADDR` is set to a non-loopback address, current server startup rejects the configuration unless `ALLOW_PUBLIC_ADMIN_LISTENER=true` is set. That flag is not a permission to expose admin on the internet. It is an explicit statement that the operator has placed the admin listener behind private networking, firewall rules, service mesh policy, a bastion, or equivalent controls.

A safe production shape is:

- public API and gateway traffic reach the public listener through the public edge;
- admin traffic reaches the admin listener only from operator networks and trusted automation;
- internal node gRPC reaches the committed `public_api_addr` on the public listener with a node credential and system-realm authorisation;
- tenants never need admin-plane reachability to read or write their own objects.

A common misconfiguration is publishing both `50051` and `50052` from a container because both are gRPC ports. That makes incident response harder even though admin authentication still applies. Keep the port separation visible in Compose files, Kubernetes Services, firewall rules, load balancer listeners, and runbooks.

## Private still means normal authentication and authorisation

Admin authentication starts with the same basic identity mechanism as other Anvil API calls: a caller presents a bearer token signed with the server's `JWT_SECRET`. On the admin listener, the server then rejects ordinary tenant data-plane credentials and expects a system-tenant principal. Each admin method performs a second check against the system realm for the specific operation.

The system realm is Anvil's built-in Zanzibar-style authorisation realm for mesh administration. It lives under the internal system storage tenant and uses the built-in `anvil_mesh` namespace. Relations include `manage_tenants`, `manage_apps`, `manage_policies`, `manage_secret_encryption_keys`, `manage_buckets`, `manage_regions`, `manage_nodes`, `manage_routing`, `manage_host_aliases`, `run_repair`, `view_diagnostics`, and `view_audit_log`. The server maps each admin RPC to one of those relations before doing the work.

This is different from public policy scopes. Public policy scopes let a tenant application call public APIs such as bucket, object, authz, index, or watch operations for that tenant. System-realm relations authorise operators to change Anvil's control plane. A tenant can define and bind schemas in its own relationship authorisation realm when delegated, but it cannot redefine Anvil's built-in system realm or grant itself admin relations there.

This design avoids fragile bypasses. There should be no special header that makes a caller an admin, no permanent first-run API that creates admin credentials on demand, and no CLI path that writes system tuples directly into storage. If an operation changes the system, it should authenticate, check the system realm, require an audit reason, and be performed by the server.

## First boot bootstrap happens below the API

A new Anvil storage cluster has a chicken-and-egg problem: the admin API requires a system-realm administrator, but the system realm does not exist yet. Current Anvil resolves this at server startup, before public or admin requests are accepted. If the bootstrap marker for the mesh is absent, startup installs the built-in system schema, grants the first system owner relation, and writes a marker so later starts do not grant again.

The normal operator-friendly path is to configure a first admin application name and an output path:

- `BOOTSTRAP_SYSTEM_ADMIN_APP_NAME` names the initial system admin app.
- `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH` tells the server where to write the first credential JSON.

On first boot, the server creates or reuses the internal `system` storage tenant, creates the named app if needed, encrypts the stored secret with the configured secret-encryption keyring, writes the credential file with restrictive permissions on Unix, and grants that app the system owner relation. Treat the output file as a highly privileged secret. Move it into your secret manager, restrict access, and remove it from transient bootstrap locations once a managed operator credential path is in place.

There is also lower-level first-boot configuration for an explicit subject: `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_KIND` and `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_ID`. Current admin bearer-token checks evaluate authenticated admin callers as app subjects, so the routine CLI path is the named app credential. Use an explicit subject only when your deployment has a matching authentication path and you have tested that it authorises the intended admin methods.

After the system realm exists, bootstrap configuration is ignored with a warning. Restarting with a different first-admin name does not create another system owner, and losing the first credential is therefore an operational recovery problem rather than a reason to expect startup to grant a new one automatically. Plan secondary admin credentials and break-glass handling after the first successful handover.

## How `anvil-admin` authenticates

`anvil-admin` is a network client. It needs the private admin endpoint and a way to obtain or supply a bearer token. The endpoint can come from `--host` or `ANVIL_ADMIN_ENDPOINT`. The token can come directly from `ANVIL_AUTH_TOKEN`, from `ANVIL_BOOTSTRAP_CREDENTIAL_FILE`, or from a CLI profile containing a client id and client secret. When it needs to mint a token from client credentials, the CLI calls the public authentication service, using `ANVIL_PUBLIC_ENDPOINT` if set or the profile host otherwise.

That split is deliberate. Token issuance is a public API authentication operation; admin mutations are private admin API operations. In a production deployment, an operator workstation might reach the public authentication endpoint through one route and the admin endpoint through a bastion or internal service route. Both paths still end at Anvil APIs rather than a direct storage directory.

Do not provide `STORAGE_PATH`, `ANVIL_SECRET_ENCRYPTION_KEY`, `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`, or direct filesystem access to `anvil-admin` for normal operations. The exception is the local key helper:

```bash
anvil-admin key generate-secret-encryption-key
```

That command only prints random key material suitable for `ANVIL_SECRET_ENCRYPTION_KEY`; it does not inspect or mutate Anvil state. Store the generated key in a secret manager and follow [Secrets and Key Management](/operators/secrets-and-key-management/) before using it in a real deployment.

## Safe command shape

A read-only diagnostics command is the safest first test of admin reachability:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list --limit 20
```

This proves that the caller can reach the private admin listener, present a valid system-tenant token, and satisfy the `view_diagnostics` relation in the system realm. It does not prove that the public API is healthy, that tenants can read objects, that S3/static routing is configured, or that every admin relation is granted. Diagnostics are a control-plane view, not a complete data-plane health check.

A tenant handover starts with an explicit mutation and an audit reason:

```bash
anvil-admin --host http://10.10.0.12:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason 'create acme storage tenant for onboarding ticket ACME-42'
```

This asks the server to create a storage tenant named `acme` with `eu-west-1` as its home region. The CLI supplies a generated request id and idempotency key unless you provide them. For create and register commands, the expected generation defaults to `0`, which means the operation should create new state rather than overwrite an existing descriptor. A successful response proves the tenant was created through the admin path and audit evidence was recorded. It does not grant the tenant's application broad rights to every API; that delegation is a separate, deliberate policy step.

The next handover command creates a first application credential for that tenant:

```bash
anvil-admin --host http://10.10.0.12:50052 app create \
  --tenant-id acme \
  --app-name acme-owner \
  --audit-reason 'create first acme application credential for secure handover'
```

The returned secret is shown once. Store it immediately in the tenant's secret manager or handover mechanism. This command proves that an application identity exists for the tenant; it does not prove that the app can create buckets, define authz schemas, or write objects until the appropriate public policy scopes are granted. Use narrow grants rather than wildcard grants, and record why each scope is needed. See [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/) and [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) for the tenant handover flow.

For secret envelope rotation, start with a dry run:

```bash
anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason 'verify secret envelope rotation after configuring key 2026-07'
```

This asks the server to check what it would rotate using its active and previous secret-encryption keys. It proves that the caller has the `manage_secret_encryption_keys` relation and that the server can begin the rotation workflow with the configured keyring. It does not prove that you can remove previous keys yet; do that only after a real rotation succeeds and dependent credentials have been verified.

Audit listing is another read-only control-plane check:

```bash
anvil-admin --host http://10.10.0.12:50052 audit list --limit 50
```

A successful audit list proves that the caller has `view_audit_log` and that the admin audit backend is readable. It does not prove that every mutating command had a useful reason; operators still need review practices that reject vague reasons such as `test` or `fix`.

## Audit context, generations, and retries

Admin mutations carry an `AdminRequestContext`. In the CLI this is exposed as `--request-id`, `--idempotency-key`, `--audit-reason`, and, for update or delete lifecycle commands, `--expected-generation`. The server rejects an empty request id, empty idempotency key, or empty audit reason. It also rejects create requests that supply a non-zero expected generation and update requests that omit a non-zero expected generation.

The request id is for tracing one operator action across logs, diagnostics, and audit evidence. The idempotency key gives retry-aware records a stable identity, but you should still read the specific command response before assuming an arbitrary mutation is safe to replay forever. The audit reason should name the human or automation reason for the change: ticket number, incident id, planned maintenance, or a precise provisioning event. The expected generation is a compare-and-swap guard for existing descriptors; it prevents a stale operator or controller from overwriting a record that changed after it was read.

For lifecycle work such as activating, draining, suspending, or deleting existing records, read the descriptor first, note its generation, then submit the update with that generation. If the server rejects the update, re-read and decide whether your intended change is still valid. Do not bypass that rejection by editing storage.

## Tenant work should move to the public plane

Operators often use the admin plane to create a tenant and a first app, but they should not keep using admin credentials for ordinary tenant-owned work. Once the tenant has a credential and the required public policy scopes, use the public API or `anvil` CLI for bucket creation, object writes, metadata updates, relationship authorisation tuples, indexes, watches, object links, public-read settings, and host aliases where tenant-owned support exists.

This keeps ownership and audit evidence clear. A tenant object write performed with a tenant app token is evaluated against tenant public policy scopes and tenant relationship authorisation. A topology change performed with an admin token is evaluated against the system realm. Mixing those paths makes it harder to answer a simple incident question: did the tenant publish this data, or did an operator change the platform?

Admin bucket and host-alias commands exist for operator-controlled provisioning, repair, migration, and lifecycle cases. They are not a reason to build application publishing jobs around the admin API. If a tenant workflow cannot be completed through current public APIs, document that as a product or implementation gap and decide whether an operator-run migration is justified; do not create a hidden storage writer.

## Current public surfaces and gaps

The current admin CLI covers the main implemented admin service families: tenant and app provisioning, policy grants and revokes, secret-envelope rotation, bucket bootstrap and public-access control, region/cell/node lifecycle, host-alias lifecycle, routing inspection and repair, repair jobs, diagnostics, and audit listing. It also exposes the local key-generation helper. For exact command names, use [Admin CLI](/reference/admin-cli/); for tenant-facing command names, use [Public CLI](/reference/public-cli/).

There are still surfaces to treat carefully. First-boot can create a named system admin app or write tuples for an explicit subject, but the routine token-authenticated admin path currently checks app subjects. Do not assume arbitrary subject kinds are useful without a compatible authentication path. Region activation uses an activation checkpoint file, while checkpoint generation and drain-completion automation are still coarse operational surfaces; plan those workflows from [Topology Planning](/operators/topology-planning/) and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/) rather than editing records by hand. Some admin reference examples may lag the current CLI; prefer the compiled CLI help and source when an example and the implementation disagree.

The absence of a public or admin command for a desired maintenance action is not permission to mutate CoreStore directly. The safe escalation path is: diagnose through read-only admin/public APIs, run the narrowest implemented repair if one exists, preserve audit evidence, and file the missing workflow as an implementation gap. [Repair and Diagnostics](/operators/repair-and-diagnostics/) and [Incident Response](/operators/incident-response/) describe that posture in more detail.

## Safe admin automation

Admin automation should be narrow, audited, and idempotent. Give each controller a distinct admin principal where possible. Set explicit `--request-id`, `--idempotency-key`, and `--audit-reason` for repeatable jobs. Read current generation before lifecycle updates and pass `--expected-generation` so a stale controller cannot overwrite an operator's newer decision.

Do not run admin automation in the same trust boundary as tenant application traffic. Even read-only admin diagnostics expose platform state that tenant applications should not depend on.
