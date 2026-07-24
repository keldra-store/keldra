---
title: Admin CLI
description: Reference for the private anvil-admin CLI, including authentication, admin relations, tenants, apps, policies, buckets, topology, routing, host aliases, repair, diagnostics, audit, and secret encryption rotation.
---

# Admin CLI

`anvil-admin` is the command-line client for Anvil's private admin API. It is a network client, not a storage repair tool. Except for the local key-generation helper, it sends authenticated requests to the admin listener, the server checks the built-in system realm, and the server performs any durable mutation with validation, generation checks, idempotency context, and audit evidence.

Keep the admin listener on an internal network. Admin requests still require bearer-token authentication and system-realm authorisation, but network privacy is a separate boundary. Do not expose `ADMIN_LISTEN_ADDR` just because the public API or S3/static gateway is exposed. Do not give `anvil-admin` `STORAGE_PATH`, CoreStore files, `ANVIL_SECRET_ENCRYPTION_KEY`, or direct filesystem write access.

This page is a reference for the current source in `anvil-cli/src/cli/admin.rs`. It is not a tutorial. The admin CLI is a network client for the private admin API, not a direct CoreStore writer; [Architecture Overview](/architecture/overview/) explains the storage and plane separation behind that rule. Read it with [Admin Plane](/operators/admin-plane/), [Network and Ports](/operators/network-and-ports/), [Deployment](/operators/deployment/), [Secrets and Key Management](/operators/secrets-and-key-management/), [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/), [Topology Planning](/operators/topology-planning/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Release Readiness Checklist](/operators/release-readiness-checklist/), [Authorisation](/learn/authorisation/), [Public CLI](/reference/public-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Global command shape

The top-level shape is:

```bash
anvil-admin --host http://127.0.0.1:50052 diagnostics list --limit 10
```

`--host` is the private admin API endpoint. It can also come from `ANVIL_ADMIN_ENDPOINT`. The CLI normalises a host without a scheme by adding `http://`. `--profile` and `--config` work like the public `anvil` CLI and select stored client credentials, but the admin service call itself always goes to the admin endpoint selected by `--host` or `ANVIL_ADMIN_ENDPOINT`.

Authentication uses the same bearer-token path as other Anvil services:

| Input | Behaviour |
| --- | --- |
| `ANVIL_AUTH_TOKEN` | Sent directly to the admin API as the bearer token. |
| `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` | Reads JSON with `client_id` and `client_secret`, then exchanges it for a token. |
| `--profile` / stored profile | Uses the profile's client id and client secret when no token or credential file is supplied. |
| `ANVIL_PUBLIC_ENDPOINT` | Endpoint used for token exchange. Set this when `anvil-admin` must mint a token, because `--host` points at the admin listener, not the public authentication listener. |

A common local-container smoke-test shape is:

```bash
export ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052
export ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051
export ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json

anvil-admin diagnostics list --limit 10
```

This proves the CLI can find the admin endpoint, mint a bearer token through the public endpoint using the first-admin credential file, and call a read-only admin method. It does not prove the admin endpoint is private from tenant networks; test that with your firewall, Service, Ingress, or proxy configuration.

## Mutation context and output

Every mutating admin command requires `--audit-reason`. Most also accept:

| Option | Meaning |
| --- | --- |
| `--request-id` | Request id written into the admin context. If omitted, the CLI generates `cli-<uuid>`. |
| `--idempotency-key` | Idempotency key written into the admin context. If omitted, the CLI generates a UUID. |
| `--expected-generation` | Generation check. Create/register commands default to `0` and reject non-zero values. Update/delete lifecycle commands require a non-zero generation. Action-style commands such as policy changes, repair, and secret rotation accept it but do not always require it. |

Admin command output is structured JSON. Successful responses use schema `anvil.admin_cli.output.v1` and include `ok`, `request_id`, `resource_type`, `resource`, `generation` where available, `audit_event_id`, and the idempotency key for mutating calls. Errors are also printed as JSON with the gRPC status code and message before the command exits non-zero.

The generation contract matters. If you list a node and see generation `4`, a later drain should carry `--expected-generation 4`. That prevents two operators or controllers from silently racing.

## System-realm relations

The admin API is authorised by Anvil's built-in system realm. These are not public policy scopes and tenants cannot grant them to themselves. The current command families check these relations on the system mesh object:

| Command family | System-realm relation |
| --- | --- |
| `tenant create` | `manage_tenants` |
| `app create`, `app rotate-secret` | `manage_apps` |
| `policy grant`, `policy revoke` | `manage_policies` |
| `secret-encryption-key rotate` | `manage_secret_encryption_keys` |
| `bucket create`, `bucket public-access set` | `manage_buckets` |
| `region ...`, `cell ...` | `manage_regions` |
| `node ...` | `manage_nodes` |
| `host-alias ...` | `manage_host_aliases` |
| `routing ...` | `manage_routing` |
| `repair run` | `run_repair` |
| `diagnostics list` | `view_diagnostics` |
| `audit list` | `view_audit_log` |
| `key generate-secret-encryption-key` | None; local helper only. |

The implementation also has a `manage_links` system relation, but the current `anvil-admin` CLI does not expose an admin object-link command family. Tenants manage their own object links through the public API and `anvil object link` where authorised.

## Local key helper

The `key` family does not contact Anvil. It generates server configuration material:

```bash
anvil-admin key generate-secret-encryption-key
```

Purpose: print a random 32-byte hex value suitable for `ANVIL_SECRET_ENCRYPTION_KEY`.

Required relation: none, because the command is local. It does not authenticate, read storage, rotate envelopes, or install the key.

Limitations: store the output in a secret manager. Losing the key can make encrypted server-side secrets unrecoverable. If it leaks, configure a new active key and use the network rotation command after servers have the new and previous keys configured.

## Tenants

Create a storage tenant:

```bash
anvil-admin --host http://10.10.0.12:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason 'create acme storage tenant for contract TEN-1842'
```

Purpose: create a storage tenant isolation boundary. `--home-region` defaults to the server's configured region if omitted.

Required relation: `manage_tenants`.

Limitations: the current admin CLI exposes tenant creation only. It does not expose tenant list, rename, suspend, or delete commands. Creating a storage tenant does not create product users or publish tenant data. Tenant applications should use the public API after handover.

## Applications and first tenant credentials

Create or rotate a tenant application credential:

```bash
anvil-admin --host http://10.10.0.12:50052 app create \
  --tenant-id acme \
  --app-name docs-admin \
  --audit-reason 'create first tenant application for acme handover'

anvil-admin --host http://10.10.0.12:50052 app rotate-secret \
  --tenant-id acme \
  --app-name docs-admin \
  --expected-generation 1 \
  --audit-reason 'rotate docs-admin after handover verification'
```

Purpose: create initial tenant application credentials through the admin plane, then rotate a tenant app secret when an operator-owned recovery or handover process requires it.

Required relation: `manage_apps`.

Limitations: app create and rotate responses include secret material once; store it immediately and avoid logs. The current admin CLI does not expose app list, app delete, or app read. After handover, normal tenant-owned app lifecycle should use `anvil app ...` through the public API where delegated.

## Public policy grants

Grant or revoke one tenant app's public API scope:

```bash
anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource documents/inbox/welcome.txt \
  --audit-reason 'allow docs-writer to upload the onboarding document'

anvil-admin --host http://10.10.0.12:50052 policy revoke \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource documents/inbox/welcome.txt \
  --audit-reason 'remove onboarding upload grant after job completion'
```

Purpose: bootstrap or repair tenant app public policy grants from the private admin plane.

Required relation: `manage_policies`.

Limitations: these are public/data-plane scopes, not system-realm admin relations. Use exact resources or narrow prefix resources. Do not use wildcard grants as the normal path. Prefer tenant self-service `anvil auth grant` after the tenant has been delegated the relevant authority.

## Buckets and public access

Create a bucket for a tenant, or set bucket public-read policy from the admin plane:

```bash
anvil-admin --host http://10.10.0.12:50052 bucket create \
  --tenant-id acme \
  --bucket-name documents \
  --region eu-west-1 \
  --audit-reason 'create documents bucket during tenant provisioning'

anvil-admin --host http://10.10.0.12:50052 bucket public-access set \
  --tenant-id acme \
  --bucket-name documents \
  --allow false \
  --expected-generation 2 \
  --audit-reason 'disable public read after access review SEC-441'
```

Purpose: operator provisioning or corrective control for tenant buckets and public-read state.

Required relation: `manage_buckets`.

Limitations: the admin CLI does not upload objects, delete objects, create indexes, list buckets, or publish tenant data. Public-access updates require the bucket generation; keep the create response or use supported diagnostics/state views to avoid guessing. Do not build tenant publishing jobs around the admin API. Public-read means anyone who can reach the public surface may read matching data; use it deliberately and audit it.

## Regions

Regions are placement and routing boundaries. The admin CLI exposes descriptor creation, activation, read-only transition, drain, removal, and listing.

```bash
anvil-admin --host http://10.10.0.12:50052 region create \
  --region eu-west-1 \
  --public-base-url https://eu-west-1.storage.example.com \
  --virtual-host-suffix eu-west-1.storage.example.com \
  --placement-weight 100 \
  --default-cell eu-west-1-a \
  --audit-reason 'register eu-west-1 region descriptor'

anvil-admin --host http://10.10.0.12:50052 region list --limit 100
```

Lifecycle commands:

```bash
anvil-admin --host http://10.10.0.12:50052 region activate \
  --region eu-west-1 \
  --activation-checkpoint ./eu-west-1-activation-checkpoint.json \
  --expected-generation 1 \
  --audit-reason 'activate eu-west-1 after checkpoint review'

anvil-admin --host http://10.10.0.12:50052 region set-read-only \
  --region eu-west-1 \
  --expected-generation 3 \
  --audit-reason 'set eu-west-1 read-only for maintenance window'

anvil-admin --host http://10.10.0.12:50052 region drain \
  --region eu-west-1 \
  --default-disposition remain-proxy-only \
  --bucket-override 'acme:documents:read-only-until-removed:legal hold' \
  --expected-generation 4 \
  --audit-reason 'drain eu-west-1 during network migration'

anvil-admin --host http://10.10.0.12:50052 region remove \
  --region eu-west-1 \
  --expected-generation 5 \
  --audit-reason 'remove drained eu-west-1 descriptor'
```

Purpose: manage region lifecycle records used for placement and routing.

Required relation: `manage_regions`.

Limitations: activation requires an activation checkpoint JSON file. The server validates it, but the current CLI does not generate a production checkpoint for you. Drain completion and cross-region proxy behaviour are still coarse surfaces; do not hand-write fake checkpoint JSON or treat drain commands as complete traffic migration by themselves.

## Cells

Cells are capacity/failure-domain descriptors inside a region. A cell is typically a rack, storage pool, or operational slice; it is not a separate worker process.

```bash
anvil-admin --host http://10.10.0.12:50052 cell register \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --placement-weight 100 \
  --audit-reason 'register eu-west-1 cell a'

anvil-admin --host http://10.10.0.12:50052 cell list --region eu-west-1 --limit 100
```

Lifecycle commands:

```bash
anvil-admin --host http://10.10.0.12:50052 cell activate \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --expected-generation 1 \
  --audit-reason 'activate eu-west-1-a after node registration'

anvil-admin --host http://10.10.0.12:50052 cell drain \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --expected-generation 3 \
  --audit-reason 'drain eu-west-1-a for rack maintenance'

anvil-admin --host http://10.10.0.12:50052 cell remove \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --expected-generation 4 \
  --audit-reason 'remove drained eu-west-1-a descriptor'
```

Purpose: manage cell descriptors and lifecycle transitions.

Required relation: `manage_regions`.

Limitations: the current CLI has no dedicated cell failure-domain option. Record rack/failure-domain mapping in your topology plan or in cell ids until a richer descriptor is exposed.

## Nodes

A node is one Anvil server process with capabilities. Node registration commits the process identity, placement, dialable `public_api_addr`, Ed25519 receipt-signing public key, and capability set. This lifecycle descriptor is the membership and routing authority.

Read the local descriptor from the node being registered and encode its public-key byte array for the registration command:

```bash
NODE_17_SIGNING_KEY_B64="$(
  anvil-admin --host http://10.10.0.17:50052 node describe-local \
  | jq -r '.resource.receipt_signing_public_key | implode | @base64'
)"
```

`node describe-local` reports the node id, configured region and cell, local signing public key, and configured endpoint without inventing a second identity. Compare those fields with the intended topology before registration.

```bash
anvil-admin --host http://10.10.0.12:50052 node register \
  --node-id node-17 \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --receipt-signing-public-key-b64 "$NODE_17_SIGNING_KEY_B64" \
  --public-api-addr http://10.10.0.17:50051 \
  --capability object,index,personaldb,metadata,gateway,admin \
  --audit-reason 'register node-17 in eu-west-1-a'

anvil-admin --host http://10.10.0.12:50052 node list \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --page-size 100
```

Lifecycle commands:

```bash
anvil-admin --host http://10.10.0.12:50052 node activate \
  --node-id node-17 \
  --expected-generation 1 \
  --audit-reason 'activate node-17 after readiness checks'

anvil-admin --host http://10.10.0.12:50052 node drain \
  --node-id node-17 \
  --graceful-timeout-ms 30000 \
  --force-after-timeout \
  --expected-generation 3 \
  --audit-reason 'drain node-17 before image replacement'

anvil-admin --host http://10.10.0.12:50052 node force-offline \
  --node-id node-17 \
  --expected-generation 4 \
  --audit-reason 'mark node-17 offline after confirmed host loss'

anvil-admin --host http://10.10.0.12:50052 node remove \
  --node-id node-17 \
  --expected-generation 5 \
  --audit-reason 'remove drained node-17 descriptor'
```

Purpose: manage node descriptors and lifecycle state.

Required relation: `manage_nodes`.

Nodes contact one another through authenticated gRPC at the exact committed `public_api_addr`. Registration does not probe or discover a process, and DNS reachability alone does not grant membership. The configured internal bearer credential must identify an authorised node principal.

Limitations: `node drain` records lifecycle intent; it does not stop the operating-system process, remove the node from an external load balancer, or prove background work has moved. `force-offline` is an explicit operator action for failure or emergency cases, not graceful drain completion.

## Host aliases

Admin host-alias commands manage system-side descriptors. Tenants should manage their own host aliases with `anvil host-alias` when the public tenant surface is sufficient.

```bash
anvil-admin --host http://10.10.0.12:50052 host-alias create \
  --hostname docs.example.com \
  --tenant-id acme \
  --bucket-name documents \
  --region eu-west-1 \
  --prefix site/ \
  --audit-reason 'create operator-managed docs host alias'

anvil-admin --host http://10.10.0.12:50052 host-alias read \
  --hostname docs.example.com

anvil-admin --host http://10.10.0.12:50052 host-alias list \
  --region eu-west-1 \
  --limit 100
```

Lifecycle commands:

```bash
anvil-admin --host http://10.10.0.12:50052 host-alias activate \
  --hostname docs.example.com \
  --expected-generation 1 \
  --audit-reason 'activate docs.example.com after DNS verification'

anvil-admin --host http://10.10.0.12:50052 host-alias suspend \
  --hostname docs.example.com \
  --expected-generation 2 \
  --audit-reason 'suspend docs.example.com during abuse investigation'

anvil-admin --host http://10.10.0.12:50052 host-alias delete \
  --hostname docs.example.com \
  --expected-generation 3 \
  --audit-reason 'delete retired docs.example.com alias'
```

Purpose: create, inspect, activate, suspend, and delete host-alias descriptors from the admin plane.

Required relation: `manage_host_aliases`.

Limitations: these commands do not create DNS records, issue TLS certificates, configure a reverse proxy, or make a bucket public. Admin activation is an operator lifecycle action; the public tenant `verify` flow is separate.

## Routing records

Routing records are materialised projections for tenant names, tenant locators, bucket locators, and host aliases.

```bash
anvil-admin --host http://10.10.0.12:50052 routing list \
  --family bucket-locator \
  --limit 100

anvil-admin --host http://10.10.0.12:50052 routing repair \
  --family host-alias \
  --record-key docs.example.com \
  --expected-generation 1 \
  --audit-reason 'repair host-alias routing record after diagnostic MESH-311'
```

Purpose: inspect routing projections and repair one materialised routing record from durable source state.

Required relation: `manage_routing`.

Limitations: accepted families are `tenant-name`, `tenant-locator`, `bucket-locator`, and `host-alias`. Routing repair does not create DNS, change bucket policy, complete region activation, or repair object data.

## Diagnostics

Admin diagnostics are read-only operational evidence.

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --severity warning \
  --limit 100

anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source index \
  --tenant-id acme \
  --bucket-name documents \
  --index-name by_status \
  --severity error \
  --limit 100
```

Purpose: list diagnostics from available admin diagnostic backends. Current source filters include `index`, `index_diagnostic_journal`, `mesh`, `mesh_lifecycle`, and `mesh_routing_projection`; an empty source requests all available backends. Index diagnostics require `--tenant-id` and `--bucket-name`.

Required relation: `view_diagnostics`.

Limitations: diagnostics are not repairs and do not prove all of CoreStore is healthy. They are surface-specific findings. Pair them with public smoke tests, logs, audit, and targeted repair when needed.

## Repair

Admin repair runs one repair backend synchronously and returns structured evidence.

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind index \
  --tenant-id acme \
  --bucket-name documents \
  --index-name by_status \
  --rebuild \
  --audit-reason 'rebuild by_status after stale index diagnostic IDX-221'

anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind directory-index \
  --tenant-id acme \
  --bucket-name documents \
  --rebuild \
  --audit-reason 'rebuild documents directory index after listing mismatch'

anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind authz-derived-index \
  --tenant-id acme \
  --derived-index-id derived-userset-acme-docs \
  --rebuild \
  --audit-reason 'rebuild authz derived userset after tuple lag'

anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind personaldb-log-chain \
  --tenant-id acme \
  --database-id customer-notes \
  --audit-reason 'inspect PersonalDB log chain after sync incident'

anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind mesh-routing-projection \
  --tenant-id acme \
  --audit-reason 'repair safe mesh routing projection findings'
```

Purpose: rebuild or validate supported derived state: index, directory index, authz derived index, PersonalDB log chain, or mesh routing projection.

Required relation: `run_repair`.

Limitations: the repair selector option is `--repair-kind`. The generic CLI requires `--tenant-id` even for `mesh-routing-projection`, although that backend is mesh-scoped internally. Repair does not synthesise missing source records and is not a general CoreStore fsck. Diagnose first, repair narrowly, then verify the original symptom.

## Audit

Admin audit lists private admin-plane audit events.

```bash
anvil-admin --host http://10.10.0.12:50052 audit list \
  --principal-id app:ops-admin \
  --action admin.node.drain \
  --limit 50

anvil-admin --host http://10.10.0.12:50052 audit list \
  --resource-id bucket/documents \
  --cursor "$NEXT_CURSOR" \
  --limit 50
```

Purpose: list admin audit events with optional request id, principal id, resource id, action, cursor, and limit filters.

Required relation: `view_audit_log`.

Limitations: admin audit is separate from tenant audit. It records admin-plane operations; it is not a tenant data export and does not prove a source record still exists.

## Secret encryption rotation

Server-side secret encryption keys are configured on Anvil servers. The CLI asks the server to inspect or re-encrypt stored envelopes; it does not hold the key material.

```bash
anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason 'dry-run secret envelope rotation before key 2026-07 cutover'

anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --audit-reason 'rotate secret envelopes to key 2026-07 after dry-run success'
```

Purpose: dry-run or execute re-encryption of server-side secret envelopes with the active configured key.

Required relation: `manage_secret_encryption_keys`.

Limitations: servers must already be configured with the new active `ANVIL_SECRET_ENCRYPTION_KEY`, `ANVIL_SECRET_ENCRYPTION_KEY_ID`, and any required `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`. The command does not change environment variables, restart nodes, or prove applications can still authenticate. Verify with secret-dependent smoke tests before removing previous keys.

## Current gaps and boundaries

The current admin CLI is intentionally narrower than the full platform model:

| Area | Current boundary or gap |
| --- | --- |
| System-realm management | First boot creates the initial system admin relation. The current admin CLI does not expose a general command to edit system-realm admin tuples or bind arbitrary named admin apps. Protect the initial credential and plan recovery carefully. |
| Tenant lifecycle | `tenant create` exists; tenant list/suspend/delete are not exposed in this CLI. |
| Admin app lifecycle | Admin `app` supports create and rotate-secret only. Tenant-owned app list/delete use the public API where delegated. |
| Bucket lifecycle | Admin bucket create and public-access set exist; ordinary object publishing, index creation, links, and tenant data operations belong to the public API. |
| Region activation | `region activate` requires a checkpoint file, but the CLI does not generate a production activation checkpoint. |
| Drain completion | Region, cell, and node drain commands record lifecycle state; external traffic drain and completion evidence are still operator responsibilities. |
| CoreStore integrity | Diagnostics and repair are surface-specific. There is no broad admin command that proves every CoreStore object, ref, stream, and derived view is correct. |
| Package gateways | The admin CLI does not expose full Docker/npm/PyPI/Maven registry gateway lifecycle commands. Treat package gateway work as foundational unless current protocol handlers are added. |

Use `anvil-admin` when the operation belongs to the private operator plane: tenants, first credentials, emergency policy changes, topology, routing, diagnostics, repair, audit, and secret envelope rotation. Use the public API and `anvil` when the operation is tenant-owned data work. If a workflow cannot be completed through either current surface, document it as an implementation gap rather than creating a direct storage writer.

## Reading admin command failures

An admin CLI failure has a different shape from a tenant failure. First confirm `ANVIL_ADMIN_ENDPOINT` points at the private admin listener and `ANVIL_PUBLIC_ENDPOINT` points at the public token service. Then distinguish authentication failure from system-realm denial. A token can be valid and still lack the relation required for `manage_regions`, `manage_nodes`, `run_repair`, or another admin operation.

For lifecycle mutations, also check generation. A stale `--expected-generation` failure is a safety feature: it means another operator or controller changed the resource after the value you read. Re-read the resource, review the newer generation, and decide whether the planned mutation is still valid.
