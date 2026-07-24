---
title: Mesh Regions, Cells, and Nodes
description: Register the local topology descriptors Anvil uses for placement, routing, and lifecycle operations.
---

# Mesh Regions, Cells, and Nodes

This tutorial continues from [Run Anvil Locally](/tutorials/setup-local-anvil/) and [Bootstrap Administration](/tutorials/admin-bootstrap/). It assumes the `anvil-local` container is running and your shell has `ANVIL_AUTH_TOKEN` set to a short-lived bearer token for the bootstrap-created system administrator.

Mesh topology is system administration. Regions, cells, and nodes affect placement, routing, lifecycle, repair, and operational blast radius for the whole Anvil deployment. Tenant apps cannot create or activate these records through the public API. The control path is the private admin API on port `50052`, and in this Docker setup that port is reachable only from inside the container.

The commands use `anvil-admin` as a helper over the private admin API. The conceptual model is in [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/) and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/). Production planning is covered in [Topology Planning](/operators/topology-planning/) and [Network and Ports](/operators/network-and-ports/). Exact command flags are in [Admin CLI](/reference/admin-cli/).

By the end of this page, the local mesh has a region descriptor, an active cell, and an active node descriptor. The region itself remains in the joining lifecycle state because the current `region activate` command requires a real activation checkpoint file; this tutorial does not fake one.

## Prerequisites and private-plane check

First confirm that the private admin path still works from inside the container:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 1
```

A successful read proves your token is valid for at least one admin-plane operation and that the private endpoint is reachable. It does not prove the principal can mutate topology; the topology commands below still check system-realm relations such as region, cell, and node management.

If this check fails, return to [Bootstrap Administration](/tutorials/admin-bootstrap/) before registering topology. Do not publish the admin port to the host.

## Understand the topology model

A **mesh** is the whole cooperating Anvil deployment and routing universe. It is the boundary inside which regions, cells, nodes, route records, lifecycle state, and placement decisions are expected to agree. The local tutorial has one container, but it still uses mesh records so the same language applies later to multi-node and multi-region deployments.

A **region** is a placement, failure, and latency boundary. In production it is usually a cloud region, data centre, jurisdiction, or other location where you can reason about user latency, regulatory placement, outage blast radius, and routing. When a bucket or route chooses a region, Anvil needs a region descriptor that says how that region should participate in placement and public routing.

A **cell** is a smaller failure domain inside a region. In many deployments it maps to a rack, availability-zone slice, storage pool, or other unit you can drain independently. Cells let operators express, "these nodes fail or drain together", rather than treating every server in a region as interchangeable.

A **node** is one Anvil server process. Nodes advertise capabilities such as object serving, indexing, PersonalDB work, gateway serving, and admin capability. Capabilities are operator intent: they tell routing, maintenance, and repair workflows what this process is meant to do.

Lifecycle state matters as much as existence. A joining record is known but not fully serving. An active record can receive the relevant work. Draining and drained states are used for maintenance and migration. Generation values are optimistic concurrency guards: if a descriptor has generation `3`, a lifecycle update should carry `--expected-generation 3` so two operators do not silently race.

## Register the local region

The local region is a control-plane descriptor for the tutorial's placement boundary. The public base URL is the public API address clients use for this deployment. The virtual-host suffix is the suffix Anvil can use when building or validating virtual-host-style routes. The default cell records the cell we intend to register next.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region create \
    --region local \
    --public-base-url http://127.0.0.1:50051 \
    --virtual-host-suffix local.anvil.test \
    --placement-weight 100 \
    --default-cell local-cell-1 \
    --audit-reason 'register local tutorial region'
```

A successful response proves the caller authenticated to the admin API, had the system-realm relation required to manage regions, the region id was accepted, and Anvil stored the descriptor with audit evidence. The response should include a generation value and an audit event id. New descriptors start in a joining lifecycle state; activation is separate because production region activation must prove routing/control streams have reached a safe checkpoint.

The region response is operator evidence, not tenant placement proof. A tenant may later ask to create a bucket in `local`, but bucket creation should still reject placement while the region lifecycle says joining. That rejection is a safety property: it prevents data from being silently placed into a region that has not completed the control-plane activation workflow.

Common failures are straightforward. `AlreadyExists` means you previously registered `local`; list regions and reuse the existing generation if you are rerunning the tutorial. `PermissionDenied` means the admin principal lacks region-management authority. A validation error usually points to an invalid URL, suffix, or missing required flag.

## Register and activate the local cell

A region needs at least one cell before it is useful for placement. This tutorial's cell represents the local machine's tiny failure domain. In production, choose a cell id that maps to a real operational unit such as a rack, availability-zone slice, or storage pool.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell register \
    --region local \
    --cell-id local-cell-1 \
    --placement-weight 100 \
    --audit-reason 'register local tutorial cell'
```

Registration stores the cell descriptor in the joining state. A newly registered cell has generation `1`, so the activation command uses that expected generation:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell activate \
    --region local \
    --cell-id local-cell-1 \
    --expected-generation 1 \
    --audit-reason 'activate local tutorial cell'
```

Activation proves the descriptor still had the generation you expected and records that the cell may receive work. If activation reports a generation mismatch, list the cell and retry with the current generation. Do not guess a new value; the mismatch exists to prevent accidental races.

If activation fails because the region is missing, register or inspect the region first. If it fails because the cell already exists or is already active during a rerun, read the current record rather than deleting it. Lifecycle commands are safer when they are idempotent in your runbook: inspect, compare desired state, then mutate only when a transition is actually required.

## Register and activate the local node

The node descriptor represents the running `anvil-local` server process. Its `public_api_addr` is both the client endpoint in this tutorial and the authenticated gRPC endpoint other nodes would dial in a distributed deployment. The descriptor must also contain the Ed25519 receipt-signing public key generated and stored locally by this exact node.

Capabilities should reflect the process you are actually running. The local all-in-one node advertises object storage, indexing, PersonalDB work, metadata, gateway serving, and admin capability. Production deployments may vary capabilities, but Anvil nodes remain equal participants rather than a separate worker hierarchy.

Read the local descriptor first and convert its byte-array public key to base64 for the registration command:

```bash
export LOCAL_NODE_SIGNING_KEY_B64="$(
  docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
    anvil-admin node describe-local \
  | jq -r '.resource.receipt_signing_public_key | implode | @base64'
)"
```

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node register \
    --node-id local-node-1 \
    --region local \
    --cell-id local-cell-1 \
    --receipt-signing-public-key-b64 "$LOCAL_NODE_SIGNING_KEY_B64" \
    --public-api-addr http://127.0.0.1:50051 \
    --capability object,index,personaldb,metadata,gateway,admin \
    --audit-reason 'register local tutorial node'
```

The node now exists in the joining state. Activate it with the generation created by registration:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node activate \
    --node-id local-node-1 \
    --expected-generation 1 \
    --audit-reason 'activate local tutorial node'
```

Success proves Anvil recorded the node descriptor with the local signing public key and endpoint, accepted the capability list, and moved the lifecycle to active using a generation check. It does not prove region activation, bucket placement, S3 routing, host aliases, or distributed recovery.

If a capability is wrong, fix the descriptor through the supported lifecycle/update path rather than hoping other services infer reality from traffic. A node that cannot serve gateway traffic should not advertise `gateway`; a node that should not receive admin responsibilities should not advertise `admin`. Accurate capabilities are what make later drain and repair operations explainable.

## Inspect the registered topology

Listing descriptors is the safest verification step. These reads are admin-plane reads because topology is control-plane state, not tenant data.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin region list

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin cell list --region local

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin node list --region local --cell-id local-cell-1
```

The output should show `local`, `local-cell-1`, and `local-node-1`. Pay attention to lifecycle state and generation values. The region may still be joining. The cell and node should be active if the activation steps succeeded.

If a later command needs an expected generation, copy it from the current list output. Do not assume it is still `1` after another operator, controller, or rerun has touched the descriptor.

## About region activation and bucket placement

The current admin CLI has a `region activate` command, but it requires an activation checkpoint file. That checkpoint is not decorative. It proves the region has reached the required mesh control-stream positions before placement and routing treat it as active.

This tutorial does not hand-write a fake checkpoint and does not tell you to bypass activation. The safe local path is to register the region, activate the cell and node, inspect the topology, and understand why later bucket placement may fail while the region remains joining. A production-ready activation walkthrough needs the checkpoint creation and verification path documented alongside the admin API command.

When region activation is available in your deployment workflow, keep it on the admin plane. Do not perform mesh lifecycle changes through the public API, and do not work around activation checks with tenant data-plane grants.

## Success and failure cues

A useful local topology has three inspectable records: a `local` region, an active `local-cell-1`, and an active `local-node-1` with the capabilities you intended to advertise. Generation mismatches mean another operation changed the descriptor, so list the record and retry with the current generation. Placement failures later in the tutorial usually mean the region is still joining or not writable; do not reinterpret them as tenant credential failures until topology is confirmed.

## Where to go next

Read [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) to hand a storage tenant to the public plane. If you operate real deployments, also read [Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/) before activating, draining, or routing regions; it explains the checkpoint and audit evidence that this local page deliberately does not fake.
