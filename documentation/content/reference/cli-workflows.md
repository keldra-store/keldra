---
title: CLI Workflows
description: Verified command-line workflows that connect the tenant-facing anvil CLI with the private anvil-admin CLI, including proof points and current gaps.
---

# CLI Workflows

The CLI references tell you which commands exist. This page shows how to join those commands into repeatable smoke tests and handover workflows without changing the ownership model. `anvil-admin` is a private operator client for the admin API. `anvil` is a public, tenant-facing client for the public API. Neither CLI writes CoreStore files directly, and neither bypasses authentication or authorisation.

Use these workflows as operational evidence, not as your application architecture. Production applications should call the public API or Rust client when they need stable idempotency keys, explicit compare-and-swap preconditions, object metadata, pinned versions, long-running watch consumers, or typed error handling. The CLI is strongest for first checks, reproducing a support issue, verifying a release, and proving that credentials, policies, topology, and derived views are connected.

Keep the command references nearby: [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/). For the concepts behind these workflows, read [Authorisation](/learn/authorisation/), [Object Model](/learn/object-model/), [Indexes and Query](/learn/indexes-and-query/), [Gateways](/learn/gateways/), and the operator chapters on [Admin Plane](/operators/admin-plane/), [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/), [Gateway Operations](/operators/gateway-operations/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), and [Release Readiness Checklist](/operators/release-readiness-checklist/).

## Reading a workflow

Each workflow has three questions: which plane owns the step, what the command proves, and what it does not prove. The public plane proves tenant-facing behaviour: token exchange, public policy scopes, relationship authorisation, object operations, index queries, watches, tenant diagnostics, and tenant repair. The admin plane proves private operator behaviour: first tenant creation, first app handover, topology, system routing records, admin diagnostics, admin repair, and admin audit.

Do not promote a smoke-test credential into a long-lived owner credential. A successful command proves only the checked path and the permissions carried by that caller. For example, a successful `anvil object head` proves the caller can read the current metadata for that object through the public API. It does not prove version-pinned reads, content type propagation, metadata upload, or CAS semantics, because the current public CLI does not expose those fields.

The examples use `eu-west-1` as an active region and `documents` as a bucket. Replace them with values from your topology. If the target region has not been created and activated, bucket creation will fail before the object workflow starts.

## First boot and the first admin credential

First boot is special because the system realm and first administrator need to exist before ordinary API-driven administration can begin. That creation happens in the server start-up path, not through an `anvil-admin` command. Once the server has written the first-admin credential file, `anvil-admin` becomes a normal network client: it mints a bearer token through the public authentication endpoint and sends that token to the private admin endpoint.

For a local or private-network smoke test, set the two endpoints explicitly. `ANVIL_ADMIN_ENDPOINT` points at the private admin listener. `ANVIL_PUBLIC_ENDPOINT` points at the public listener used only for token exchange. `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` points at the first-admin credential JSON created by the server during first boot.

```bash
export ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052
export ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051
export ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json

anvil-admin diagnostics list --limit 10
anvil-admin audit list --limit 10
```

`diagnostics list` proves that the CLI can find the admin listener, exchange the first-admin credential for a token, and call a read-only admin method authorised by the system realm. `audit list` proves the same path can read admin audit events if the principal has the system-realm relation for audit viewing.

This does not prove the admin endpoint is private. Test network exposure separately with your firewall, Kubernetes Service, Ingress, reverse proxy, or container publishing configuration. A working admin CLI also does not mean the CLI has direct storage access; it should not have `STORAGE_PATH`, server encryption keys, or CoreStore write access.

## Tenant handover

Tenant handover starts on the admin plane because only operators create storage tenants and first tenant credentials. It should move to the public plane as soon as the tenant has a usable app credential and the narrow public policy grants needed for self-service.

```bash
anvil-admin --host http://10.10.0.12:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason 'create acme storage tenant for handover TICKET-123'

anvil-admin --host http://10.10.0.12:50052 app create \
  --tenant-id acme \
  --app-name acme-owner \
  --audit-reason 'create first acme tenant app for handover TICKET-123'
```

The tenant command creates the storage isolation boundary and records an admin audit event. The app command creates an application credential in that tenant and prints the client id and secret once. Store that secret in the tenant's secret manager, not in shell history or a shared runbook.

The first app is useful because it gives the tenant a public API principal. It is not meant to be a permanent superuser. Grant only the public scopes needed for the next stage. For example, a tenant owner that is allowed to create tenant apps and delegate access to one tutorial object needs app-management authority on the tenant resource and policy authority on that object resource. Public delegation is non-escalating, so the owner must also hold the action it grants.

```bash
anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action app:create \
  --resource "tenant:${ACME_TENANT_ID}" \
  --audit-reason 'allow acme owner to create tenant apps during handover TICKET-123'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action policy:read \
  --resource "tenant:${ACME_TENANT_ID}" \
  --audit-reason 'allow acme owner to inspect tenant app grants during handover TICKET-123'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action policy:grant \
  --resource documents/tutorial/welcome.json \
  --audit-reason 'allow acme owner to delegate tutorial document access TICKET-123'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action object:write \
  --resource documents/tutorial/welcome.json \
  --audit-reason 'allow acme owner to delegate tutorial uploads TICKET-123'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action object:read \
  --resource documents/tutorial/welcome.json \
  --audit-reason 'allow acme owner to delegate tutorial reads TICKET-123'
```

The first grant lets the owner create another app through `anvil app create`. The second lets it inspect grant state for tenant apps. The third lets it delegate policy for the tutorial object. The final two give it the object authority needed to delegate `object:write` and `object:read` for that same object. These grants are deliberately exact. Do not use wildcard action or resource grants as the normal handover path; if a temporary local smoke test needs broader authority because of a current CLI gap, record it as a controlled exception and revoke it after the test.

The tenant now configures the public CLI. This writes a local CLI profile and verifies the public token service can issue a bearer token for the app.

```bash
anvil static-config \
  --name acme \
  --host https://storage.example.com \
  --client-id "$ACME_CLIENT_ID" \
  --client-secret "$ACME_CLIENT_SECRET" \
  --default

anvil --profile acme auth get-token
```

This proves the tenant credential works on the public plane. It does not prove the tenant should use the owner credential for day-to-day jobs. Create narrower job apps next.

## Least-privilege tenant app and grant smoke

After handover, tenant-owned application credentials should be created through the public API where the tenant has been delegated the required scopes. The operator is not in this loop unless handover or recovery fails.

```bash
anvil --profile acme app create docs-writer

anvil --profile acme auth grant docs-writer object:write documents/tutorial/welcome.json
anvil --profile acme auth grant docs-writer object:read documents/tutorial/welcome.json
anvil --profile acme auth list-grants docs-writer
```

`app create` proves the tenant owner can create another app in its own tenant. The two `auth grant` commands prove non-escalating delegation for one object key: the caller can grant only authority it already has. `auth list-grants` proves the stored public policy grants are visible to the caller.

These commands do not create relationship authorisation tuples. Public policy scopes decide whether an app principal may call a service operation. Relationship authorisation decides product-level access such as whether `user:17` may view `document:welcome`. Use `anvil authz ...` only after your product schema and tuple model are defined.

Current gap to account for: `anvil object put` discovers the bucket id by calling `ListBuckets` before writing. A job app that has exact `object:write` but no bucket-list authority may be able to write through a richer API path with a known bucket id while failing through the CLI helper. Prefer the API or Rust client for strict least-privilege upload tests. If you temporarily broaden a CLI smoke-test profile to work around this, keep it separate from production credentials and remove the grant after the test.

## Bucket and object smoke

A bucket and object smoke test checks the public data plane end to end: token exchange, bucket placement, object write, current metadata read, body read, and prefix listing. It is a smoke test for the current object path, not a full object-model conformance test.

Run it with a profile that has the relevant public scopes for this bucket: `bucket:create` on `documents`, `object:write` and `object:read` on `documents/tutorial/welcome.json`, and `object:list` on `documents`. If you are using the current CLI upload helper, account for the bucket-list discovery gap described later on this page.

```bash
anvil --profile acme bucket create documents eu-west-1

printf '{"title":"Welcome","status":"open","body":"Hello from Anvil"}\n' > /tmp/anvil-welcome.json
anvil --profile acme object put /tmp/anvil-welcome.json s3://documents/tutorial/welcome.json

anvil --profile acme object head s3://documents/tutorial/welcome.json
anvil --profile acme object get s3://documents/tutorial/welcome.json /tmp/anvil-downloaded-welcome.json
anvil --profile acme object ls s3://documents/tutorial/
```

`bucket create` proves the caller can create a bucket in the named region and that the server accepts that region for placement. `object put` proves the public Object API accepts a current-version write from the CLI helper. `object head` proves current metadata is readable and returns an ETag and size. `object get` proves the object body can be read back. `object ls` proves the caller can list the bucket prefix visible through the current listing path.

This does not prove custom content type, user metadata, explicit idempotency keys, CAS preconditions, pinned version reads, or version-specific deletes. The current CLI does not expose those fields. Use the API for production write paths that need those guarantees.

## Object link smoke

Links are public-plane, tenant-owned aliases inside a bucket. A link is not a copy: it stores a descriptor pointing at a target key. Updating the link changes the alias, not the target object bytes. Use generation checks on updates so two release jobs cannot move the alias silently.

```bash
printf 'release 1\n' > /tmp/app-1.0.0.txt
printf 'release 2\n' > /tmp/app-1.0.1.txt

anvil --profile acme object put /tmp/app-1.0.0.txt s3://documents/releases/app-1.0.0.txt
anvil --profile acme object put /tmp/app-1.0.1.txt s3://documents/releases/app-1.0.1.txt

anvil --profile acme object link create \
  s3://documents/releases/latest.txt \
  s3://documents/releases/app-1.0.0.txt \
  --resolution follow

anvil --profile acme object link read s3://documents/releases/latest.txt

anvil --profile acme object link update \
  s3://documents/releases/latest.txt \
  s3://documents/releases/app-1.0.1.txt \
  --expected-generation 1 \
  --resolution follow
```

The create command proves the link descriptor can be written. The read command prints the target and generation. The update command proves the caller can move the alias only if it still sees the expected link generation. The CLI supports `--resolution follow` and `--resolution redirect`, plus `--allow-dangling` when you deliberately want a descriptor whose target does not yet exist.

This does not prove the target object is immutable. If the target key is overwritten later, a follow-style link to that key follows the current target semantics. The current CLI also restricts link targets to the same bucket and does not expose target-version pinning.

## Typed index smoke

An index smoke test should be small and explain the JSON fields it sends. The index definition below selects objects under `tutorial/`, extracts two typed values from each JSON object body, and leaves authorisation in the safe default mode `inherit_object`. Query JSON is separate from definition JSON: it asks for rows already built by the index.

```bash
anvil --profile acme index create documents tutorial_by_status typed_json \
  --selector-json '{"prefix":"tutorial/"}' \
  --extractor-json '{}' \
  --authorization-mode inherit_object \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"status","extractor":"/status","required":true},{"name":"title","extractor":"/title","required":false}],"default_order":[{"field":"title","direction":"asc"}]}'

anvil --profile acme index query documents tutorial_by_status \
  --typed-predicates-json '[{"field":"status","op":"eq","value":"open"}]' \
  --typed-order-json '[{"field":"title","direction":"asc"}]' \
  --limit 10

anvil --profile acme index diagnostics documents tutorial_by_status --page-size 20
```

`index create` proves the definition validates and is accepted. In this example, `selector_json` narrows the source set by object-key prefix, `build_policy_json` defines the typed fields, and `extractor_json` is empty because typed JSON uses the build policy for field extraction. `index query` proves the query path can read materialised rows and apply an equality predicate. `index diagnostics` proves the caller can inspect build warnings and errors for the definition.

This does not prove the index has caught up with a just-finished write. If your application has a source watch cursor from the write path, query with `--require-caught-up-to-watch-cursor` and an appropriate `--lag-timeout-ms`. Current catch-up support is not equally complete across full-text, vector, and hybrid paths, so use the index tutorials and reference for the specific kind you operate.

## Gateway and static-hosting smoke

Gateway smoke has two layers. First prove Anvil's tenant records: bucket public-read policy, object content, and host-alias descriptor. Then prove your external gateway path: DNS, TLS, reverse proxy host forwarding, and the static/S3 gateway process. The public CLI can prove the first layer. It cannot prove the second layer by itself.

```bash
printf '<!doctype html><title>Anvil smoke</title><h1>ok</h1>\n' > /tmp/anvil-index.html

anvil --profile acme bucket set-public documents --allow true
anvil --profile acme object put /tmp/anvil-index.html s3://documents/site/index.html

anvil --profile acme host-alias create docs.example.test documents \
  --region eu-west-1 \
  --prefix site/

anvil --profile acme host-alias read docs.example.test
```

`bucket set-public` proves the caller can change bucket public-read policy. Treat that as a data-exposure decision: anyone who can reach the public surface may read matching public data. `object put` proves a static asset exists at the prefix you intend to serve. `host-alias create` proves the tenant can request an alias mapping a host to a bucket and prefix. `host-alias read` prints the descriptor and, if verification is required, the challenge value.

When the challenge has been placed in DNS or the configured domain proof location, verify it with the generation from the read output:

```bash
anvil --profile acme host-alias verify docs.example.test "$OBSERVED_CHALLENGE" --expected-generation 1
```

That command proves Anvil accepts the observed challenge for the pending alias. It does not create DNS records, issue certificates, configure a reverse proxy, expose the S3 gateway, or make the admin API public. Check the external path with ordinary network tools against the gateway host your deployment exposes, for example an HTTP `HEAD` request to the static object URL. If that request fails while `host-alias read` succeeds, investigate gateway routing, trusted forwarded host handling, DNS, TLS, or public-read policy rather than editing tenant data through the admin API.

Tenant-owned aliases should normally use `anvil host-alias`. Operator `anvil-admin host-alias` commands exist for system lifecycle and corrective operations, but they should not become the routine publishing path for tenant sites.

## Diagnostics and repair smoke

Diagnostics are read-only evidence. Repair mutates or rebuilds derived state. Start with the tenant/public surface when the issue is tenant-owned, such as a missing index hit or directory inconsistency in one bucket. Use the admin surface when the issue is topology, routing projection, system visibility, or an operator-owned incident.

For a tenant index issue:

```bash
anvil --profile acme diagnostics list documents tutorial_by_status --page-size 20
anvil --profile acme repair findings index documents/tutorial_by_status --page-size 20
anvil --profile acme repair run index documents tutorial_by_status
```

The diagnostics command reads index diagnostics through the public Index service. `repair findings` reads stored repair evidence for the given scope. `repair run index` asks the Repair service to check or repair the index. Add `--rebuild` only when you have decided that rebuilding derived state is the intended mutation, because a rebuild can cost more and may temporarily increase lag.

For an operator routing issue:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --limit 50

anvil-admin --host http://10.10.0.12:50052 routing list \
  --family bucket-locator \
  --limit 50

anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind mesh-routing-projection \
  --tenant-id acme \
  --audit-reason 'repair bucket routing projection after incident INC-842'
```

The admin diagnostics command proves the operator can inspect system diagnostic backends. `routing list` proves the materialised routing records are visible to the operator. The admin repair command proves the server accepted an audited repair request for a system-derived projection. None of these commands proves source data is correct by itself. Verify by repeating the failing public request, checking audit evidence, and reading the relevant diagnostics after repair.

There is no general public CLI command that proves all CoreStore records are healthy. Current repairs are surface-specific: directory/path, indexes, authz derived state, PersonalDB log chains, and mesh routing projections. Do not replace evidence gathering with a broad repair-first habit.

## Release smoke

A release smoke test checks that the binary or image you are about to operate matches the docs and that the main public and admin paths still work. It should be fast enough to run after deploy and narrow enough that a failure points to a plane or feature family.

Start by checking the installed CLI versions. This proves the operator is using the intended client binaries; it does not prove server compatibility by itself.

```bash
anvil --version
anvil-admin --version
```

Then run a short admin read path against the private endpoint:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list --limit 10
anvil-admin --host http://10.10.0.12:50052 audit list --limit 10
```

That proves admin authentication, system-realm authorisation, and read-only admin services are reachable. Follow with a public smoke using a non-production test tenant or a dedicated release-check bucket:

```bash
anvil --profile release-smoke bucket create release-smoke eu-west-1
printf '{"status":"open","title":"release smoke"}\n' > /tmp/anvil-release-smoke.json
anvil --profile release-smoke object put /tmp/anvil-release-smoke.json s3://release-smoke/tutorial/release-smoke.json
anvil --profile release-smoke object head s3://release-smoke/tutorial/release-smoke.json
anvil --profile release-smoke object get s3://release-smoke/tutorial/release-smoke.json /tmp/anvil-release-smoke.out
```

If the release includes index changes, create or query a small index and inspect diagnostics as shown in the index smoke workflow. If the release includes gateway changes, verify the tenant host-alias descriptor and then test the external URL through your gateway. If the release includes repair or topology changes, run read-only diagnostics first and keep mutating repairs behind an explicit incident or release ticket with an audit reason.

This release smoke does not prove every protocol adapter, every index kind, every region, or every existing tenant workload. It proves the core command paths you selected for the release gate. Keep longer Docker end-to-end tests, restore drills, and workload-specific checks in your release process as separate gates.

## Current gaps that affect CLI workflows

These gaps do not make the workflows useless; they define what the CLI can and cannot prove today.

| Area | Current impact | Safer workflow choice |
| --- | --- | --- |
| Object upload helper | `anvil object put` calls `ListBuckets` to discover the bucket id and does not expose content type, user metadata, idempotency key, preconditions, or version targeting. | Use API/Rust client for production uploads and strict least-privilege checks. Treat any broader bucket-list grant used for a CLI smoke as temporary and controlled. |
| Object reads | `anvil object get` and `anvil object head` read the current version only. | Use the API for pinned version reads, version audit, and CAS-sensitive recovery. |
| Link management | Public CLI links are same-bucket and do not expose target-version pinning. | Use API fields directly when you need immutable target-version aliases. |
| Index correctness | CLI queries can request catch-up by watch cursor, but catch-up and lag behaviour varies by index kind. | Capture source cursors in production code and validate the relevant index kind in the reference and tutorial. |
| Region lifecycle | Region activation requires a checkpoint file; the current CLI does not generate a production checkpoint. Drain and proxy surfaces are still coarse. | Treat lifecycle commands as operator-controlled changes with separate readiness and routing verification. |
| Admin authority management | `anvil-admin` consumes system-realm authorisation, but tenant public policy grants are not system-realm grants. | Keep private admin relations in the operator model and public tenant scopes in the tenant model. Do not model admin access with public grants. |
| Repair coverage | Repairs are surface-specific; there is no single CLI `fsck` proving every CoreStore invariant. | Run read-only diagnostics first, choose the smallest repair family, and verify the original symptom afterwards. |
| Gateways | CLI host-alias and public-read commands manage Anvil records only. | Verify DNS, TLS, reverse proxy, S3/static gateway behaviour, and trusted host forwarding outside the CLI as part of gateway operations. |

When a workflow needs a capability the CLI does not expose, do not invent a flag in a runbook. Either call the API directly, use the Rust client, or document the limitation and the narrow manual temporary path you used.

## Workflow evidence standard

A workflow should state what it proves and what it does not prove. `anvil object get` proves the current version is readable by that app through the public API; it does not prove historical version reads or S3 signature handling. `anvil-admin region activate` proves an authorised operator moved a lifecycle record; it does not prove every gateway DNS name is reachable. Good runbooks make those limits explicit so the next operator does not overinterpret a green smoke test.
