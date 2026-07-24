---
title: Run Anvil Locally
description: Start a Docker-based local Anvil node and learn the listener, credential, and CLI boundaries.
---

# Run Anvil Locally

This tutorial starts one disposable Anvil node with Docker. It is a learning environment, not a production topology, but it uses the same boundaries as a real deployment: one public listener for tenant/application traffic, one private admin listener for operator traffic, one Anvil-owned storage directory, and explicit server secret material.

The command-line tools in this page are helpers over APIs. The public CLI is `anvil`; it talks to the public plane. The admin CLI is `anvil-admin`; it talks to the private admin plane except for its local key-generation helper. Applications should normally use the public API or a client library, then reserve the CLIs for provisioning, smoke tests, and operator checks. The command references are [Public CLI](/reference/public-cli/) and [Admin CLI](/reference/admin-cli/).

By the end of this page you will have a running `anvil-local` container, a host-side public endpoint at `http://127.0.0.1:50051`, a private admin endpoint reachable only from inside the container, and a CLI profile that can exchange the first-start credential for short-lived bearer tokens.

## Prerequisites

You need Docker, a shell, `curl`, and the `anvil` and `anvil-admin` binaries that match the server image you run. The examples use `jq` to read JSON credential files. If you do not have `jq`, the same steps still work; you will copy the `client_id` and `client_secret` fields manually.

Use a scratch directory for generated files. Do not write copied credentials into the repository.

```bash
mkdir -p /tmp/anvil-tutorial
cd /tmp/anvil-tutorial
```

The examples also assume the Anvil image is available to Docker. Set `ANVIL_IMAGE` to the image you intend to run. Production deployments should pin a release tag or digest; `latest` is acceptable only for a disposable local tutorial.

```bash
export ANVIL_IMAGE="${ANVIL_IMAGE:-ghcr.io/<your-org>/anvil:latest}"
```

If the image name is wrong, the first Docker command that uses it fails before Anvil starts. Fix the image reference first; do not change listener or credential settings to work around an image-pull error.

## Understand the two request surfaces

Before you open a port, know which surface you are opening.

The **public API** is the tenant-facing and application-facing surface. It listens on port `50051` in the default container configuration. Tenant applications, automation jobs, S3-compatible gateway clients, static/object gateway traffic, and the public `anvil` CLI use this surface for authentication, bucket and object operations, indexes, watches, PersonalDB, tenant-owned authorisation, app credentials, and public policy grants. In a real deployment this is the surface you may put behind a public or tenant-facing load balancer. In this tutorial it is published only on host loopback.

The **admin API** is the operator-facing surface. It listens on port `50052` by default. It changes system-level state such as the system realm, tenants, first application credentials, mesh topology, routing, repair operations, diagnostics, admin audit reads, and server-side secret envelopes. It must stay on loopback, a private management network, or an equivalent operator-only path. The admin API is not an application API, and tenant applications should not need to reach it.

This split is both a network boundary and an ownership boundary. Public-plane operations belong to tenants where appropriate. Admin-plane operations belong to operators. Later tutorials keep that distinction: buckets, objects, indexes, watches, app delegation, and tenant-owned authz use `anvil`; topology, first tenant bootstrap, admin diagnostics, and mesh lifecycle use `anvil-admin` from inside the private boundary. For production network planning, see [Network and Ports](/operators/network-and-ports/) and [Admin Plane](/operators/admin-plane/).

## Generate server secret material with Docker

Anvil needs server-side secret encryption material before it can persist encrypted server secrets, including stored application credential envelopes. Generate the value with the same image family you plan to run so the tutorial stays Docker-first.

```bash
export ANVIL_SECRET_ENCRYPTION_KEY="$(
  docker run --rm "$ANVIL_IMAGE" \
    anvil-admin key generate-secret-encryption-key 2>/dev/null
)"
export ANVIL_SECRET_ENCRYPTION_KEY_ID="local-tutorial"
```

The `anvil-admin key generate-secret-encryption-key` command is a local helper. It does not call the admin API, does not authenticate, and does not install the key anywhere. It prints random key material suitable for the server environment variable `ANVIL_SECRET_ENCRYPTION_KEY`.

A successful command leaves two shell variables set. You can confirm the key is present without printing the secret itself:

```bash
printf 'secret key id=%s, key bytes=%s hex characters\n' \
  "$ANVIL_SECRET_ENCRYPTION_KEY_ID" \
  "${#ANVIL_SECRET_ENCRYPTION_KEY}"
```

If key generation fails, check the Docker image and binary path. Do not invent a shorter key or reuse `JWT_SECRET`; those variables protect different things.

## Start a single-node container

The next command creates a named Docker volume for Anvil-owned state and starts the server. It publishes only the public API on host loopback. The admin API binds to loopback inside the container and is not published to the host.

```bash
docker volume create anvil-local-data

docker run -d \
  --name anvil-local \
  -p 127.0.0.1:50051:50051 \
  -v anvil-local-data:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e MESH_ID=local \
  -e NODE_ID=local-node-1 \
  -e REGION=local \
  -e CELL_ID=local-cell-1 \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=http://127.0.0.1:50051 \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET=local-jwt-secret-change-me \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID="$ANVIL_SECRET_ENCRYPTION_KEY_ID" \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$ANVIL_SECRET_ENCRYPTION_KEY" \
  -e BOOTSTRAP_SYSTEM_ADMIN_APP_NAME=system-admin \
  -e BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH=/var/lib/anvil/bootstrap/system-admin.json \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/system-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  "$ANVIL_IMAGE"
```

The command has a few details worth understanding before you continue:

- `STORAGE_PATH=/var/lib/anvil` tells the server where its durable state lives inside the container. That directory belongs to Anvil; do not edit it by hand.
- `NODE_ID=local-node-1` becomes the durable identity stored in node-local CoreMeta on first use of the volume. A later start must use the same value or omit it.
- `API_LISTEN_ADDR=0.0.0.0:50051` lets Docker publish the public listener to host loopback. The host-side `-p 127.0.0.1:50051:50051` keeps it local.
- `PUBLIC_API_ADDR=http://127.0.0.1:50051` is the reachable address stored in this node's lifecycle descriptor. In a multi-node deployment it must be a stable endpoint the other nodes can dial.
- `ADMIN_LISTEN_ADDR=127.0.0.1:50052` keeps the admin listener inside the container. There is no `-p` line for `50052`.
- `BOOTSTRAP_SYSTEM_ADMIN_*` values are first-start settings. They are used only when the system realm does not already exist.
- `ANVIL_BOOTSTRAP_CREDENTIAL_FILE`, `ANVIL_PUBLIC_ENDPOINT`, and `ANVIL_ADMIN_ENDPOINT` help CLI commands run inside the container during this tutorial. They do not make the admin API public.

Docker prints the volume name and container id. From that point, `/var/lib/anvil` in the container is the durable state directory for this tutorial. It contains CoreStore state, indexes, system-realm records, audit data, encrypted secret envelopes, local node identity, and the node's locally generated Ed25519 receipt-signing key.

If the command fails because the container name already exists, inspect the existing container before deleting it:

```bash
docker ps -a --filter name=anvil-local
```

Remove it only if it is the disposable tutorial container you intend to replace. Do not delete a shared or production container to make a local tutorial command pass.

## Wait for the public API to become ready

Readiness is the first proof that the server accepted its configuration and is serving the public listener.

```bash
curl -fsS http://127.0.0.1:50051/ready
```

A successful response proves the public endpoint is reachable from the host. It does not prove the first administrator exists, that credentials can mint tokens, or that the admin plane is reachable. Those are separate checks below.

If readiness fails, inspect logs before changing configuration:

```bash
docker logs --tail 80 anvil-local
```

Common causes are an invalid image, missing secret environment, an incompatible reused volume, or a port already in use. Avoid deleting `anvil-local-data` unless you are intentionally starting over; deleting the volume deletes the tutorial server state.

## Confirm the first administration credential exists

On first startup, if the system realm does not exist, Anvil initialises that realm and creates the first system administration service principal. In this tutorial the principal is named `system-admin`, and the generated long-lived client credential is written inside the Docker volume.

Check that the file exists without printing its secret contents:

```bash
docker exec anvil-local sh -c \
  'test -s /var/lib/anvil/bootstrap/system-admin.json && echo "system-admin credential file exists"'
```

This proves the bootstrap transaction produced a credential file. It does not prove the credential should be shared. The file contains long-lived credential material: a client id and client secret that can be exchanged for short-lived bearer tokens. Treat it like a password file.

Copy the credential out only so your host-side CLI can create a tutorial profile:

```bash
docker cp anvil-local:/var/lib/anvil/bootstrap/system-admin.json /tmp/anvil-system-admin.json
chmod 600 /tmp/anvil-system-admin.json
```

In production, store first-start credentials in a secret manager or protected break-glass store. Do not put them in source control, container images, CI logs, or chat transcripts.

## Configure a public CLI profile for token exchange

`anvil static-config` writes a CLI profile non-interactively. A profile has a name, a public API endpoint, and client credential material. The profile is convenience for the CLI; it is not a grant and not a second security boundary.

Create a `local-system` profile that points at the public endpoint:

```bash
anvil static-config \
  --name local-system \
  --host http://127.0.0.1:50051 \
  --client-id "$(jq -r .client_id /tmp/anvil-system-admin.json)" \
  --client-secret "$(jq -r .client_secret /tmp/anvil-system-admin.json)" \
  --default
```

This command does not call the private admin API. It writes local CLI configuration so later `anvil` commands know how to ask the public authentication service for tokens. If you do not have `jq`, read `/tmp/anvil-system-admin.json`, export the two values manually, and pass them with `--client-id` and `--client-secret`.

## Mint a short-lived bearer token

A bearer token is the request credential that API calls send. It is shorter-lived than the client secret. The CLI can mint tokens automatically when it has a profile, but making the exchange explicit helps you see what applications do through the public API.

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile local-system auth get-token)"
printf 'received bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

The printed length is the expected output. Do not print the token itself into logs. If token exchange fails, debug the public endpoint and the profile values first. A token failure is not an admin-port problem because token exchange happens on the public plane.

When a token expires, run the exchange again. Do not copy old tokens out of terminal scrollback.

## Check the private admin API without exposing it

The host should still not be able to dial `50052` directly. To prove the admin API is alive without publishing it, run the admin CLI inside the container and pass the token explicitly:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 5
```

A successful response proves four things: the bearer token is valid, the admin CLI can reach `http://127.0.0.1:50052` from inside the container, the admin API accepted the request, and the system realm authorises this principal to read admin audit events. It does not prove the admin listener is private from every network path; that is a deployment firewall, Docker, Kubernetes, proxy, or load-balancer question.

If this command fails with permission denied, treat it as an admin authorisation issue. If it cannot connect, check `ANVIL_ADMIN_ENDPOINT` inside the container and the server logs. Do not publish the admin port just to make the smoke test easier.

## Know what an app means in Anvil

The generated `system-admin` identity is an app in current CLI terminology. In Anvil, an app is an application or service principal: a credentialed identity for automation, services, importers, operators, or tenant-owned tools. It is not an S3-specific artefact and it is not limited to object storage.

Separate apps give you different audit identities, rotation schedules, scopes, revocation paths, and least-privilege boundaries. The first system administration app is useful for local bootstrap, but it is too broad for ordinary production work. Later tutorials create tenant apps through the public plane and keep them separate from system administration.

## Success and failure cues

A healthy local setup has three visible signs: `curl /ready` succeeds on `127.0.0.1:50051`, `/var/lib/anvil/bootstrap/system-admin.json` exists inside the container, and `docker exec anvil-local anvil-admin audit list --limit 5` succeeds without publishing host port `50052`.

Use the failing checkpoint to choose the next diagnostic step. Public readiness failures point to server startup or container configuration. Token failures point to the copied credential, profile, or public authentication endpoint. Admin smoke-test failures point to the private endpoint, bearer token, or system-realm relation. These are different failure classes; do not fix one by widening another boundary.

## Clean up the local node

Do not run cleanup if you want to continue the tutorial chain. The next pages expect the `anvil-local` container, volume, copied credential, and CLI profile to keep existing.

When you are finished, remove the disposable container, volume, copied credential, and shell variables:

```bash
docker rm -f anvil-local
docker volume rm anvil-local-data
rm -f /tmp/anvil-system-admin.json
unset ANVIL_AUTH_TOKEN ANVIL_SECRET_ENCRYPTION_KEY ANVIL_SECRET_ENCRYPTION_KEY_ID
```

After cleanup, the server state for this tutorial is gone. Your CLI profile may still exist in your user configuration; overwrite it with another profile or edit your CLI config if you no longer want `local-system` as the default.

## Where to go next

Continue to [Bootstrap Administration](/tutorials/admin-bootstrap/) to inspect first-start administration and the private admin API boundary. Then read [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/) before tenant bucket placement, because later data-plane examples depend on topology being understood.
