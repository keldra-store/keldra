---
title: Deployment
description: Deploy Anvil with Docker, durable node identity, authenticated gRPC topology, explicit secrets, and smoke-test evidence.
---

# Deployment

Deploying Anvil makes its durability and trust model concrete. Every node needs a private durable storage path, stable node identity, reachable gRPC endpoint, server key material, region and cell placement, and an administration path. In a multi-node deployment, the committed CoreMeta lifecycle topology is the only membership and routing authority.

This page is Docker-first because the image contains the server plus the public and admin CLIs. The same settings are available as command-line flags and environment variables for raw-binary deployments. Read this page with [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), [Secrets and Key Management](/operators/secrets-and-key-management/), [Admin Plane](/operators/admin-plane/), [Topology Planning](/operators/topology-planning/), [Run Anvil Locally](/tutorials/setup-local-anvil/), and [Admin CLI](/reference/admin-cli/).

## Phase 1: Prepare Storage, Secrets, And Networks

`STORAGE_PATH` holds durable server state: CoreStore rows and blocks, object data, indexes, manifests, system-realm records, lifecycle topology, local node identity, the local Ed25519 receipt-signing private key, and audit evidence. In the Docker image it defaults to `/var/lib/anvil`. Mount one durable volume per live node. Never share one writable storage directory between running nodes.

The node id and signing key are node-local CoreMeta records in that volume, not sidecar identity files. On a fresh volume, `NODE_ID` may set the identity that will be persisted; later starts must supply the same value or omit it. The signing key is generated locally. Its public key is committed in the node's lifecycle descriptor, while the private key never leaves the node's storage boundary.

Prepare server secret material before startup. `JWT_SECRET` signs bearer tokens. `ANVIL_SECRET_ENCRYPTION_KEY` is a 32-byte hex key used to encrypt stored server-side secrets and encrypted payloads. `ANVIL_SECRET_ENCRYPTION_KEY_ID` labels new envelopes, and `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` keeps older key ids readable during rotation. Store these values in a secret manager or orchestrator secret, not source control, shell history, image layers, or public Compose files.

Plan two listeners. The public listener serves tenant traffic, gateways, and authenticated internal node RPCs. The admin listener is private operator traffic. Equal nodes contact one another by gRPC at the `public_api_addr` recorded in committed lifecycle topology; there is no separate node discovery listener.

Build or pull a pinned image for the release you intend to operate. A local build from this repository looks like:

```bash
ANVIL_BUILD_PROFILE=release ANVIL_IMAGE=local/anvil:operator ./scripts/build-image.sh
```

That image contains `anvil-server`, `anvil`, and `anvil-admin`. It does not create durable volumes, generate operator-managed secrets, provision node credentials, or choose topology.

## Phase 2: Start A Single Node

A single-node deployment needs no membership discovery settings. Give it a stable node id, volume, and reachable `PUBLIC_API_ADDR`:

```bash
docker network create anvil-mesh
docker volume create node-eu-west-1-a

docker run -d \
  --name node-eu-west-1-a \
  --restart unless-stopped \
  --network anvil-mesh \
  -p 50051:50051 \
  -v node-eu-west-1-a:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e MESH_ID=production \
  -e NODE_ID=node-eu-west-1-a \
  -e REGION=eu-west-1 \
  -e CELL_ID=eu-west-1-a \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=http://node-eu-west-1-a:50051 \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET="$ANVIL_JWT_SECRET" \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID=2026-07-primary \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=first-admin \
  -e BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/var/lib/anvil/bootstrap/first-admin.json \
  local/anvil:operator
```

`API_LISTEN_ADDR` is a bind address. `PUBLIC_API_ADDR` is a dialable endpoint and becomes part of routing identity, so it must not be `0.0.0.0` or an endpoint that can land on a different node. `ADMIN_LISTEN_ADDR` stays private; the server refuses a non-loopback admin bind unless `ALLOW_PUBLIC_ADMIN_LISTENER=true` explicitly acknowledges private-network protection.

## Phase 3: Complete First-Boot Administration

When the system realm is absent, startup initialises it before ordinary service. If `BOOTSTRAP_SYSTEM_ADMIN_APP_NAME` and `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH` are set, startup creates the first system administration application and writes its credential JSON. An advanced deployment can instead provide `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_KIND` and `BOOTSTRAP_SYSTEM_ADMIN_SUBJECT_ID` to grant initial authority to an existing subject.

After the system realm exists, those settings do not mint new authority. Admin work must authenticate normally, authorise through the built-in system realm, validate requests, and produce audit evidence.

Copy the generated credential to a secret manager and restrict it to provisioning automation or authorised operators:

```bash
docker cp \
  node-eu-west-1-a:/var/lib/anvil/bootstrap/first-admin.json \
  ./first-admin.json
chmod 600 ./first-admin.json
```

The file contains long-lived client credential material used to obtain bearer tokens. It is not an API bypass and should not become an application publishing credential.

## Phase 4: Build A Multi-Node Topology

Choose every initial node id, region, cell, stable `public_api_addr`, and capability set before genesis. Provision each node with its own bearer credential for internal RPCs and configure the intended genesis ids with `BOOTSTRAP_NODE_IDS`. That setting admits node principals to the system realm during first boot; it does not discover addresses or create membership records.

Each node runs with:

```bash
NODE_ID=node-eu-west-1-b
REGION=eu-west-1
CELL_ID=eu-west-1-b
PUBLIC_API_ADDR=http://node-eu-west-1-b.internal:50051
CORESTORE_INTERNAL_BEARER_TOKEN=<node-b-bearer-token>
BOOTSTRAP_NODE_IDS=node-eu-west-1-a,node-eu-west-1-b,node-eu-west-1-c
```

Use the authenticated admin bootstrap workflow to collect each node's local descriptor and install one canonical topology containing regions, cells, and nodes. The seed bootstrap returns canonical CoreMeta rows that joining nodes install before serving distributed traffic. Every node descriptor must include the exact local Ed25519 receipt-signing public key and dialable `public_api_addr` reported by that node.

The bootstrap is idempotent only when the supplied topology matches what is already committed. After genesis, use lifecycle mutations to register, activate, drain, update, or remove nodes. Do not try to change membership by editing environment variables, DNS records, storage files, or process inventories. A process is eligible for routing only when its committed descriptor and lifecycle state say so.

The current compiled admin CLI exposes `node describe-local`, node lifecycle commands, and mesh region/cell/node mutations. Automated multi-node bootstrap may call the authenticated `BootstrapMeshTopology` RPC directly until a dedicated operator command wraps the entire exchange. Preserve the canonical bootstrap response as deployment evidence; do not invent a second topology registry.

## Phase 5: Compose And Kubernetes Shapes

Compose deployments use one service and durable volume per node, stable internal DNS names matching committed endpoints, and a private network that permits authenticated gRPC between nodes. Publish public ingress only where needed and keep the admin port unpublished unless the host network is the operator boundary. Pass secrets through an external secret mechanism.

Kubernetes uses the same model: one persistent volume per node, server key material in Kubernetes Secrets or an external secret operator, public ingress for tenant and gateway traffic, and a private admin Service. Each committed node endpoint must route to exactly that node identity rather than a service that load-balances across identities. NetworkPolicy should permit node gRPC only among expected nodes even though internal RPCs also authenticate and authorise.

Run `docker compose config` or equivalent manifest validation before deployment. Syntax validation does not prove CoreMeta topology bootstrap, internal authentication, quorum, recovery, or storage correctness.

If a reverse proxy terminates TLS for S3/static traffic, configure `PUBLIC_REGION_BASE_DOMAIN` and exact `TRUSTED_PROXY_SOURCE_RANGES`. S3 signatures and host routing depend on the effective host and scheme; trusting every forwarded source is a security bug.

## Phase 6: Health, Readiness, And Logs

The public listener exposes readiness:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

A successful response proves the listener is running and CoreMeta startup recovery is ready. It does not prove admin authorisation, tenant token exchange, object durability, index freshness, watch progress, host routing, or correct network isolation. A distributed node may remain unavailable while it obtains and settles the committed CoreMeta roots needed for recovery.

When readiness fails, inspect logs before changing state:

```bash
docker logs --tail 100 node-eu-west-1-a
```

Typical startup failures include missing JWT or encryption configuration, invalid key length, an undialable or missing `PUBLIC_API_ADDR`, a configured `NODE_ID` that does not match the persisted identity, an admin listener exposed without explicit opt-in, an unwritable volume, failed internal node authentication, or incomplete CoreMeta recovery. Fix the cause; do not delete the volume unless you intend to discard that node.

## Phase 7: Smoke Test The Real Boundaries

First prove the admin CLI can exchange the bootstrap credential for a token, reach the private admin listener, and make an authorised request:

```bash
docker exec \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  node-eu-west-1-a \
  anvil-admin diagnostics list --limit 10
```

Then create a disposable storage tenant and first application through the admin plane, hand over to public API commands, and prove bucket creation plus object write/read. In a distributed deployment, perform those operations through more than one node and exercise a path that requires remote quorum or recovery. A port check is not distributed-system evidence.

Also prove negative cases: the admin listener is unreachable from tenant networks; internal RPCs reject absent, tenant, or invalid credentials; `_anvil/` paths are rejected; and a lifecycle descriptor with the wrong endpoint or signing key is not silently accepted.

## Current Cautions

Each live node needs its own durable state directory and retained signing identity. JWT and encryption key rotations must be deliberate. The admin API stays private even when public gRPC, S3, or static hosting are exposed. `/ready` is a startup signal, not a correctness certificate. Region activation checkpoint generation and some drain-completion workflows still require explicit operator planning, and the repository does not ship a complete Kubernetes chart.

A deployment is ready only when token exchange, source writes and reads, derived views, committed topology, authenticated inter-node routing, restart recovery, and the intended gateway paths all work under the correct principals and network boundaries.
