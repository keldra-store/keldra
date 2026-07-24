---
title: Mesh Routing and Lifecycle
description: Understand Anvil's mesh, regions, cells, nodes, placement, routing records, host routing, cross-region behaviour, lifecycle transitions, drains, and current implementation limits.
---

# Mesh Routing and Lifecycle

Anvil stores tenant data in a mesh, not in an anonymous pile of processes. Even a local single-node deployment has a region, a node identity, routing choices, and lifecycle state underneath it. In production those concepts become operationally visible: a bucket has a home region, a request can arrive at the wrong region, a node can drain before maintenance, and a host alias can route a browser request to one tenant bucket without changing authorisation.

This chapter explains the model rather than the command syntax. Use it with [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), [CoreStore](/learn/corestore/), [Gateways](/learn/gateways/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/), [Mesh Routing and Lifecycle Tutorial](/tutorials/mesh-routing-and-lifecycle/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), [Topology Planning](/operators/topology-planning/), [Network and Ports](/operators/network-and-ports/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Gateway Operations](/operators/gateway-operations/), and [Admin CLI](/reference/admin-cli/).

## Mesh: the routing and lifecycle universe

A **mesh** is one cooperating Anvil deployment. It is the boundary inside which topology descriptors, tenant locators, bucket locators, host aliases, lifecycle records, internal proxy choices, repair findings, and admin audit evidence are expected to agree.

Application code normally does not name the mesh. It calls the public API, signs an S3 request, or follows a static URL. The mesh still decides whether the receiving process is in the bucket's home region, whether another active node can serve a proxy request, whether a host alias is active, and whether a region is eligible for new bucket placement.

Mesh state is operator state. It is managed through the private admin API and system-realm authorisation. Tenant data is managed through the public API. Keeping those planes separate matters because topology changes can affect every tenant, while tenant credentials should only affect tenant-owned buckets, objects, indexes, links, relationship tuples, apps, and host aliases.

## Regions, cells, and nodes

A **region** is a placement and routing boundary. In production it often maps to a geography, data centre, sovereign boundary, or cloud region. In local development it may simply be `local`. A region descriptor records a stable region id, public base URL, virtual-host suffix, placement weight, optional default cell, lifecycle state, timestamps, and generation.

A **cell** is a smaller boundary inside one region. It is typically a rack, rack-like failure domain, zone slice, storage pool, Kubernetes node pool, or capacity unit. The cell gives operators a way to avoid treating a whole region as one flat bag of nodes. Placement and draining can then reason about correlated failure or maintenance work.

A **node** is one Anvil server process. Anvil does not model separate worker-node binaries for object work, index work, PersonalDB work, gateway work, and admin work. One process may advertise capabilities such as `object`, `index`, `personaldb`, `metadata`, `gateway`, and `admin`, and background work is virtual work inside those processes. A node descriptor records its node id, region, cell, Ed25519 receipt-signing public key, dialable `public_api_addr`, capabilities, lifecycle state, optional drain descriptor, heartbeat timestamp when recorded, and generation.

Committed CoreMeta lifecycle topology is the sole membership and routing authority. There is no independent discovery or announcement path. Equal nodes contact the exact endpoint committed for each identity using authenticated gRPC, and verify signed evidence against the public key in that descriptor.

Capabilities are operational intent. If a process should not be selected for remote object proxying, it should not advertise object capability. If a process lacks capacity for index or PersonalDB maintenance, it should not claim that capability just because the binary includes the code.

## Placement, partitions, and locators

Placement is the step that connects topology to tenant data. A tenant can have routing information, and each bucket has a home region and usually a home cell. When a bucket is created, Anvil records where that bucket should live. Later object reads, writes, listings, S3 requests, and static delivery paths use that placement evidence instead of guessing from the URL alone.

A **tenant locator** is routing state for a storage tenant. A **bucket locator** is routing state for one tenant bucket. A bucket locator records the tenant id, bucket name, bucket id, home region, home cell, status, placement policy, object prefix, timestamps, and generation. Locator statuses include creating, active, read-only, moving, draining, and deleted.

A **partition** is a bounded slice of durable work or control history. Mesh routing records are written through control streams and materialised into routing projections. Background owners use partition fences so stale workers cannot publish after handoff. Partitions are not a promise that every data feature has production-complete distributed placement; they are the vocabulary Anvil uses to make ownership, checkpoints, and repair evidence precise.

Placement checks are lifecycle-aware. New writable placement requires the committed region, cell, and node descriptors to be eligible for writes. A region left in `joining`, `read_only`, `draining`, `offline`, or `removed` must not receive new writable placement. Even a single-node deployment has lifecycle topology; process presence and local configuration do not supersede the committed records.

## Routing records are derived state

Anvil keeps fast routing descriptors for common questions:

| Routing family | What it answers |
| --- | --- |
| Tenant name | Which tenant id a human-readable tenant name identifies. |
| Tenant locator | Which region owns or serves a tenant locator. |
| Bucket locator | Which region and cell own a tenant bucket, and what status the bucket is in. |
| Host alias | Which tenant, bucket, region, and key prefix a hostname maps to. |

These descriptors make request handling practical, but they should not be treated as hand-edited truth. They are materialised records derived from control-stream history and durable source records. If a projection is stale or missing, the safe response is diagnosis and repair from the source stream, not editing storage files or assuming the route is correct because one cached view says so.

This source-versus-derived split explains several operational behaviours. A bucket can exist while a routing projection is stale. A host alias can be tenant-owned while an operator still has admin repair tools for the system routing view. A request that fails with a routing error may be telling you about locator state, lifecycle state, or projection lag rather than object absence.

## Host routing and gateways

Host routing is how the HTTP gateway turns a host and path into an object route. When `PUBLIC_REGION_BASE_DOMAIN` is configured, the current parser supports regional path-style hosts, regional virtual-host style hosts, and active custom host aliases.

A path-style regional request has the region in the host and tenant and bucket in the path. A virtual-host regional request has bucket, tenant, and region in the host. A custom host alias stores a hostname, tenant id, bucket, region, and optional key prefix; the request path is joined to that prefix to produce an object key.

Host routing only chooses the route. It does not make private data public, create DNS records, issue TLS certificates, grant write access, or bypass reserved namespaces. Static hosting and the S3-compatible gateway still use object read rules, public-read state, public policy scopes, and relationship authorisation. Tenant-owned aliases should normally be created and verified through the public API. Admin host-alias operations are for operator lifecycle, repair, migration, suspension, and investigation.

Reverse proxies must be configured carefully because both S3 signing and host routing depend on the effective host and scheme. Anvil only trusts forwarded host metadata from configured trusted proxy ranges. Ambiguous forwarded host chains are rejected rather than guessed.

## Cross-region requests and internal proxying

A request can reach a node outside the bucket's home region. That can happen because a client used a generic endpoint, a DNS record moved, a CDN reused a route, or a static hostname was served near the caller. Anvil compares the request with bucket and routing records and then applies `CROSS_REGION_ROUTING_POLICY`.

| Policy | Behaviour at a high level |
| --- | --- |
| `redirect_preferred` | Prefer returning a redirect to the bucket's home region. This is the default. |
| `proxy_preferred` | Proxy object operations when an eligible remote object node is known; otherwise redirect. |
| `proxy_required` | Proxy object operations when possible; otherwise fail with proxy unavailable. |
| `local_only` | Reject remote-bucket serving instead of redirecting or proxying. |

Proxying is not a blind tunnel and not an authorisation bypass. The proxy path carries the original principal and tenant context, uses an internal node-issued token, and checks the internal `internal:proxy_object` action on the internal proxy service. The destination still enforces object validation, reserved namespace rejection, link behaviour, ETag preconditions where supported, and ordinary object authorisation. Tenant apps should not be granted internal proxy authority.

Current proxy support is partial. The S3/static gateway has object-shaped proxy paths for methods such as `GET`, `HEAD`, `PUT`, and `DELETE` when an active remote node with object capability and a public API address is known. Bucket-management operations are not a universal cross-region proxy surface. Native public gRPC object calls currently report remote-bucket region information rather than transparently proxying every object request.

## Lifecycle states and generations

Topology records are mutable, so Anvil protects changes with lifecycle states and generations. A generation is the compare-and-swap value an operator supplies when updating or deleting a descriptor. If someone else changed the descriptor first, the generation mismatch prevents a silent overwrite.

The shared lifecycle vocabulary includes `joining`, `active`, `read_only`, `draining`, `drained`, `drained_with_exceptions`, `offline`, and `removed`. Not every state means the same thing for every resource. Regions use read-only and drained-with-exceptions because bucket placement and region drains need them. Nodes use a smaller practical subset because a node is one process with capabilities and runtime ownership.

Typical transitions are constrained. A new region, cell, or node starts in `joining`. It becomes useful only after activation. Active resources can drain. Drained or offline resources can be removed where the state machine allows it. A node may be forced offline as an incident or failover action, but that is not the same evidence as a graceful drain completing.

## Activation checkpoints

Region activation is stricter than cell or node activation. A region should not become active until it has seen the control-stream history it must honour. An activation checkpoint is that safety proof. It names the mesh, region, creation time, and required control-stream partitions with the sequence and digest positions that must have been reached.

The server validates the checkpoint schema, mesh id, region id, required streams, regional control checkpoints, digests, and activation dependencies. It also requires at least one active cell in the region and at least one active node in an active cell. A hand-written checkpoint that merely satisfies a shape is not safe; it defeats the point of proving that the region has caught up to the required control history.

There is a current surface gap here. The admin CLI has a `region activate` operation that accepts a checkpoint file, and tests construct valid checkpoints by reading control streams and regional checkpoint records. The documentation and CLI do not yet provide a production-friendly checkpoint generation workflow. Local tutorials therefore explain the limitation instead of pretending that operators should invent checkpoint JSON by hand.

## Drains and removal

Draining records operator intent to stop assigning new work while existing work is handled. A node drain stores a drain descriptor with start time, graceful timeout, and whether force-after-timeout is allowed. It does not stop the operating-system process, terminate load-balancer traffic, or prove every virtual background owner has handed off.

A region drain is broader because bucket locators may still name the region as their primary home. The current drain model can apply dispositions to bucket locators: block until empty, remain proxy-only, read-only until removed, or delete after retention. Some dispositions can become drain exceptions; others keep the region blocked until locators are moved, deleted, or otherwise resolved.

Drain completion is where current implementation limits matter. The lifecycle state machine has drained states, and the server contains validation for completing region drain with or without valid exceptions. The current admin command surface exposes starting region drains, but it does not provide a clear production operator flow for completing a region into `drained` or `drained_with_exceptions`. Node lifecycle similarly supports `draining -> drained` internally, while the CLI lacks a distinct graceful complete-drain command. The available force-offline path is useful for incidents, not a substitute for ordinary maintenance proof.

## Failure, recovery, and repair

Mesh routing is designed to fail with evidence. If a request reaches the wrong region under `redirect_preferred`, a redirect should tell the client which region owns the bucket. If `local_only` is configured, a remote-bucket request should fail as a routing policy decision, not as a misleading object-not-found. If proxying is required but no eligible node is known, the error should say proxying is unavailable.

If a node dies, runtime ownership and partition fences are what keep stale work from publishing after another owner takes over. If a routing projection differs from the control-stream source, diagnostics should report the mismatch and repair should rebuild the projection from durable source state. If a region is draining and bucket locators still point at it, drain diagnostics and repair findings should make the blockers visible.

Repair does not make derived state authoritative. It brings a materialised view back into agreement with source evidence where the repair backend supports that operation. After repair, rerun the route, bucket operation, gateway request, or diagnostic that exposed the problem.

## Observability for operators

Operators need to see both process health and data correctness. A node can be running while a region is still joining, a bucket locator projection is stale, a host alias is suspended, a proxy target is unavailable, or a drain has outstanding bucket blockers.

Useful mesh observability includes region, cell, and node state; descriptor generations; routing-record family and record key; bucket home region and locator status; host-alias state; proxy decision and failure reason; activation checkpoint status; drain disposition and blockers; partition ownership/fence failures; control-stream and projection lag; diagnostics by source; repair findings; and admin audit events. Logs should carry request ids and routing decisions without leaking bearer tokens or object bodies.

For day-to-day operation, start from topology and routing before treating an object failure as lost data. Ask whether the bucket's home region is active, whether the receiving node is in that region, whether the locator is active or read-only, whether a host alias is active, whether the request was redirected or proxied, and whether diagnostics report a stale projection.

## Current public surfaces and gaps

The public tenant API does not manage mesh topology. Tenants use public APIs for buckets, objects, indexes, tuples, links, apps, public access, and tenant-owned host aliases. Operators use the private admin API and `anvil-admin` for region, cell, node, routing, system host-alias, diagnostics, repair, and audit work.

Current implemented surfaces include admin commands for creating/listing regions, registering/listing cells and nodes, generation-checked activation/drain/read-only/remove operations where exposed, tenant and bucket provisioning, routing-record listing and single-record repair, system host-alias lifecycle, admin diagnostics, admin repair, and admin audit. The S3/static gateway can parse configured host routes and can redirect or proxy some remote object operations according to cross-region policy.

The main gaps to design around are direct:

| Area | Current limitation |
| --- | --- |
| Region activation | A real checkpoint is required, but there is no production-friendly CLI command that generates one from control-stream evidence. |
| Drain completion | Region and node state machines include drained states, but the exposed operator workflow for graceful drain completion is incomplete. |
| Cross-region proxying | S3/static object proxying exists for object-shaped operations; native public gRPC object calls and bucket-management operations are not universally proxied. |
| Remote tenant placement | Tenant creation accepts a home region, but parts of tenant-locator projection are still tied to the serving node's configured region; inspect locators before relying on remote tenant home placement. |
| Runtime workers | A node is one Anvil process with virtual background work, not a fleet of separately addressable worker processes. Capability descriptors guide selection but do not prove capacity by themselves. |
| Derived routing views | Routing records are materialised projections and can lag or drift; use diagnostics and repair rather than manual storage edits. |

Design runbooks and application expectations with those gaps visible. Prefer regional endpoints when clients know the bucket region. Keep admin traffic private. Treat lifecycle records as operational evidence, not decorative labels. Treat routing descriptors as repairable derived state over durable control history.

## What to take forward

Mesh routing is the layer that connects Anvil's object model to physical and operational reality. Regions decide placement and wrong-region behaviour. Cells describe failure and capacity boundaries. Nodes are single Anvil processes with advertised capabilities. Locators and host aliases route requests to tenant data. Lifecycle states and generations protect topology changes. Activation checkpoints and drains are safety evidence. Cross-region proxying helps where implemented, but it does not bypass authorisation or replace regional routing. The healthiest deployments keep these concepts visible in APIs, operator runbooks, diagnostics, and audit trails.

## Lifecycle decision table

Use `create` or `register` when a resource should exist in source topology records but is not ready for traffic. Use `activate` only after reachability, identity, and dependency checks pass. Use `set-read-only` when writes must stop but reads may continue. Use `drain` when placement or routing should move away from the resource under an explicit disposition. Use `remove` only after the resource is drained and no routing projection should point at it.

If routing output looks wrong, repair the materialised routing record from lifecycle source state before changing source topology again. Changing topology to compensate for a stale projection can make the source model harder to reason about and can hide the original fault.
