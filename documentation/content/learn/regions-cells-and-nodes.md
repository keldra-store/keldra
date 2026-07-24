---
title: Regions, Cells, and Nodes
description: Understand Anvil mesh topology: regions, cells, nodes, lifecycle state, placement, bucket home regions, routing descriptors, and current implementation limits.
---

# Regions, Cells, and Nodes

Anvil stores data somewhere. That sounds obvious, but it is the first operational fact behind every bucket, object, gateway request, watch, index build, and repair. A write is not only "put these bytes under this key". It is also "place this bucket in a region that is allowed to accept writes, route future requests to that region, and record which nodes and failure domains are responsible for serving the work".

The topology vocabulary for that job is **mesh**, **region**, **cell**, and **node**. Developers need these terms because bucket creation, public URLs, gateway routing, and cross-region errors refer to them. Operators need them because lifecycle, draining, capacity, repair, and incident response depend on them.

For hands-on setup, use [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/) and [Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/). For production planning, read [Production Model](/operators/production-model/), [Topology Planning](/operators/topology-planning/), [Network and Ports](/operators/network-and-ports/), and [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/). Exact operator command syntax belongs in [Admin CLI](/reference/admin-cli/).

## The topology in one picture

A small Anvil deployment still has a topology, even if it is one container:

```text
mesh default
  region local
    cell local-cell-1
      node local-node-1
        capabilities: object, index, personaldb, gateway, admin
```

A larger deployment repeats the same shape:

```text
mesh production
  region eu-west-1
    cell eu-west-1-a
      node eu-west-1-a-01
      node eu-west-1-a-02
    cell eu-west-1-b
      node eu-west-1-b-01

  region us-east-1
    cell us-east-1-a
      node us-east-1-a-01
```

The tree is not just inventory. It is the map Anvil uses to decide whether a region can accept a new bucket, which cell a bucket locator names, which node capabilities are available, which public or virtual-host routes make sense, and which parts of the deployment are joining, active, draining, offline, or removed.

## Mesh: one routing and lifecycle universe

A mesh is one cooperating Anvil deployment. It is the boundary inside which regions, cells, nodes, tenant locators, bucket locators, host aliases, lifecycle records, and routing projections are meant to agree.

Most application developers do not choose a mesh on every request. They see a public API endpoint, a tenant, and a bucket. The mesh is still present underneath. It decides whether `documents` for tenant `acme` is local to the node that received the request, whether the request should be redirected to another region, and whether a host alias can route to a bucket.

Operators should treat the mesh as control-plane state, not tenant data. Mesh records are created and changed through the private admin API and system-realm authorisation. The public API is for tenant operations such as buckets, objects, indexes, tuples, links, and tenant-owned aliases. Keeping that split clear prevents a tenant credential from becoming a topology editor.

## Region: placement and routing boundary

A region is the placement and routing boundary that users and operators usually recognise. In production it often maps to a cloud region, data centre, sovereign boundary, or other location with meaningful latency, outage, and compliance implications. In development it may simply be `local`.

A region descriptor records more than a name:

| Region field | What it is for |
| --- | --- |
| `region` | Stable region id such as `local`, `eu-west-1`, or `us-east-1`. |
| `public_base_url` | Public API base URL clients should use for that region. |
| `virtual_host_suffix` | Host suffix used by virtual-host-style gateway routes. |
| `placement_weight` | Operator intent for placement weighting. Current simple paths record it; do not assume a full automatic scheduler uses it everywhere today. |
| `default_cell` | Preferred cell id for placements that need a default. |
| `state` and `generation` | Lifecycle state and optimistic-concurrency guard for updates. |

A region is not a decorative tag. Bucket creation records a region, and object reads and writes later compare the serving node's configured region with the bucket's home region. Gateways also use regional hostnames to parse requests. If the wrong region receives a request, Anvil can redirect, proxy where supported, or reject according to routing policy.

## Cell: failure and capacity boundary inside a region

A cell is a smaller placement boundary inside one region. The best mental model is a rack-sized or zone-slice-sized failure domain: shared power, shared top-of-rack switch, a Kubernetes node pool, a storage pool, or another unit an operator can drain and reason about independently.

Cells prevent a region from being treated as one flat bag of nodes. If every node in a region appears interchangeable, placement may accidentally put too much risk in one rack or make maintenance planning unclear. A cell gives the operator a durable way to say, "these nodes are close enough that they may fail, fill, or drain together".

A cell descriptor records its region, cell id, placement weight, lifecycle state, timestamps, and generation. Current admin commands expose registration, activation, draining, removal, and listing. The conceptual rule is simple: create the region first, register the cell in that region, activate the cell when it is ready, and drain it before relying on it to stop receiving work.

## Node: one Anvil process with advertised capabilities

A node is one running Anvil server process. It belongs to one region and one cell. Its committed descriptor binds the node id, dialable `public_api_addr`, Ed25519 receipt-signing public key, capabilities, lifecycle state, and generation used by routing and verification.

Committed CoreMeta lifecycle topology is the membership authority. Processes do not discover one another or become eligible merely because they are reachable. Equal nodes use authenticated gRPC at the endpoint committed for each identity.

Current capability names are:

| Capability | Meaning |
| --- | --- |
| `object` | The process can participate in object-serving work. Current remote object proxy selection depends on active nodes with this capability and a public API address. |
| `index` | The process is intended to participate in index build or query responsibilities. |
| `personaldb` | The process is intended to participate in PersonalDB witnessing, snapshot, or projection work. |
| `metadata` | The process can participate in distributed metadata and CoreMeta responsibilities. |
| `gateway` | The process is intended to serve gateway traffic such as S3/static surfaces. |
| `admin` | The process is intended to participate in admin-plane responsibilities. |

Capabilities are not marketing labels. They are operational intent. If a node cannot handle index work at the required scale, do not advertise `index` just because the binary contains the code. If a node should never receive gateway traffic, leave `gateway` out and route accordingly.

Node descriptors also carry lifecycle state, optional drain details, last heartbeat time when recorded, and generation. Node drain descriptors include a start time, graceful timeout, and whether force-after-timeout is allowed. Draining a node records intent; it does not by itself stop the operating-system process or prove every background owner has handed off.

## Lifecycle state and generations

Topology records are mutable, so Anvil protects them with lifecycle states and generations. A generation is the compare-and-swap value an operator supplies when changing an existing descriptor. If the generation has changed since you listed the descriptor, the update should fail rather than silently overwriting a newer operator decision.

The shared lifecycle vocabulary is:

| State | Tutorial meaning |
| --- | --- |
| `joining` | The descriptor exists but is not fully eligible for normal work. New region, cell, and node records start here. |
| `active` | The descriptor is eligible for the responsibilities its type represents. |
| `read_only` | Used for regions to stop new writable placement while preserving read-oriented operation where supported. |
| `draining` | The descriptor is leaving service and should not receive new ownership or placement. Existing work may need to finish, move, or be excepted. |
| `drained` | Drain has completed without outstanding blockers. |
| `drained_with_exceptions` | Region drain has completed only with allowed bucket drain exceptions. |
| `offline` | The descriptor is not currently available but has not been removed. |
| `removed` | The descriptor has been removed from active topology. |

Not every state is equally meaningful or exposed for every descriptor. Nodes use a smaller practical subset than regions. Regions have read-only and drain-exception concepts because they own bucket placement and routing. Cells sit between the two. The operational constraint is that transitions are constrained: for example, active resources drain before they are removed, and stale generations are rejected.

## Activation is readiness, not creation

Creating or registering a descriptor says, "this thing exists in the topology model". Activation says, "this thing may now receive real work".

Cell and node activation are ordinary generation-checked lifecycle transitions. A node can become active only when its placement is valid: the region is joining or active, and the cell is active. Region activation is deliberately stricter. Anvil requires an activation checkpoint so a region cannot become active while ignoring existing routing and lifecycle control-stream history. The checkpoint names required control streams and the sequence/digest positions that must have been reached.

In other words, region activation is a safety proof. It should answer: "has this region seen the control-plane history it must honour before it starts accepting placement and routing responsibility?" A fake checkpoint defeats the point of the mechanism.

The current admin CLI has `region activate`, and the server validates checkpoint schema, mesh id, region id, required streams, digests, and activation dependencies. The current documentation and CLI do not yet provide a production-friendly checkpoint-generation command. The local topology tutorial therefore registers the region and activates the cell and node, but it does not pretend that a hand-written checkpoint is safe. Treat this as a current implementation/documentation gap.

## Placement and bucket home region

Placement connects the topology model to tenant data. A tenant can have routing information, and each bucket has a home region. The bucket's home region is the region where ordinary object operations should be served for that bucket.

A bucket locator is the routing descriptor that makes this concrete. It records the storage tenant id, bucket name, bucket id, home region, home cell, locator status, placement policy, object prefix, timestamps, and generation. Locator statuses include `creating`, `active`, `read_only`, `moving`, `draining`, and `deleted`.

When a tenant creates a bucket through the public API, the request names a region. The bucket creation path checks writable placement when topology records exist: the target region, this node's cell, and this node's stable node id must be eligible for new writes. In a bare local development path with no topology records, compatibility paths may allow work to proceed. Once you start registering topology descriptors, however, the lifecycle state matters. A region left in `joining` can block bucket creation until activation is complete.

This is why topology matters even locally. The local server may be one process, but the moment you ask Anvil to reason about a region, cell, and node, placement checks start using that state. A local deployment that cannot create a bucket is often not an object API problem; it is a topology activation or lifecycle problem.

There is one current caveat to keep visible. Tenant creation accepts a `home_region`, but the current mesh tenant-locator projection is still tied to the serving node's configured region in parts of the implementation. Before relying on remote tenant home-region placement, inspect the tenant locator and bucket locator records and treat mismatches as a known implementation gap rather than product intent.

## Routing descriptors

Routing descriptors are materialised control-plane records that let Anvil find data quickly. They are not tenant payloads, and they are not arbitrary DNS records. They are derived or projected state used by the data plane and gateways.

Current routing families include:

| Routing family | What it answers |
| --- | --- |
| Tenant name | Which storage tenant id a tenant name resolves to. |
| Tenant locator | Which region owns a tenant locator. |
| Bucket locator | Which region and cell own a tenant bucket, and what status the bucket locator is in. |
| Host alias | Which tenant, bucket, region, and key prefix a hostname maps to. |

These records explain common runtime behaviour. A request for a bucket can be served locally if the bucket's home region matches the receiving node. If not, routing policy decides whether to redirect, proxy, or reject. A static-hosting request can resolve through a host alias only if the alias is active and host routing is configured. A drain can mark bucket locators read-only or draining so later requests do not treat them as fresh placement targets.

Routing descriptors are also repairable derived state. Operators should diagnose and repair projections from control-stream history instead of manually editing storage files. The operational flow is covered in [Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/) and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/).

## Cross-region requests

A request can arrive in the wrong region. That can happen because a client used a generic endpoint, cached an old route, followed a host alias during migration, or talked to a nearby load balancer. Anvil then compares the request with bucket and routing records.

The current routing policy values are `redirect_preferred`, `proxy_preferred`, `proxy_required`, and `local_only`. Redirect tells the caller to use the bucket's region. Proxy means Anvil should forward internally where that path is implemented and an eligible remote node is known. Local-only rejects remote-bucket serving.

This is not an authorisation bypass. A proxied request still carries principal and tenant context, and the destination must enforce object validation, reserved namespace rejection, and authorisation. Current proxy support is partial: the S3/static gateway has object-shaped proxy paths, while native gRPC object calls report the remote bucket region with structured status rather than transparently proxying every public API call. Treat cross-region proxying as a routing feature to verify per surface, not as a universal promise.

## Local topology is still real topology

It is tempting to think a local Anvil server should ignore all this. A single process has no cross-region traffic, no rack failure domain, and no remote proxy target. But the local topology is where developers learn the same invariants production uses:

```text
create region local
register and activate cell local-cell-1
register and activate node local-node-1
activate region only after a real checkpoint
create buckets in region local
```

The names are small, but the semantics are real. A bucket has a home region. A node advertises capabilities. A generation protects lifecycle updates. A region in `joining` is not the same as an active region. A private admin API owns topology changes. A public tenant API owns bucket and object work after the topology can accept it.

This is also why local docs should not work around placement failures by editing storage files or using private admin mutations for tenant data. If a local command fails because the region is not active, the honest fix is to complete or document the activation workflow, not to blur the control and data planes.

## Current limitations to remember

The topology model is present, but several surfaces are still incomplete or coarse:

| Area | Current limitation |
| --- | --- |
| Region activation | The admin API and CLI require an activation checkpoint, but there is no documented production-friendly checkpoint generator command yet. |
| Drain completion | Admin commands can start drains and remove drained resources, but the current CLI does not expose a clear "complete drain to drained" operation for normal operator workflows. Region removal only works after allowed drained states. |
| Placement scheduling | Region and cell placement weights are recorded, but current simple creation paths often use the configured region, cell, and node rather than a full automatic placement scheduler. |
| Tenant home region projection | Tenant creation accepts `home_region`, but current locator projection can still reflect the serving node's configured region. Verify routing records before relying on cross-region tenant placement. |
| Cross-region proxy | Proxy behaviour is not uniform across all public API surfaces. Verify S3/static/native behaviour separately. |
| Capability usage | Capabilities are recorded and used by some routing paths, but not every background scheduler or repair workflow is capability-aware yet. Keep descriptors honest anyway. |

These limitations do not make topology optional. They tell you where to be careful when moving from local tutorials to production operation.

## What to take forward

A mesh is Anvil's routing and lifecycle universe. A region is the placement and routing boundary. A cell is a failure and capacity boundary inside a region. A node is one Anvil process with declared capabilities. Lifecycle state decides whether those records can receive work. Generations protect operator updates. Bucket locators connect tenant buckets to home regions and cells. Routing descriptors let requests find the right place.

If you keep that model clear, later topics become easier: bucket creation is placement, redirects are region decisions, host aliases are routing descriptors, drains are lifecycle transitions, repair rebuilds projections, and local setup is not a special toy model but the smallest useful mesh.

## Topology as an operator contract

A region name is part of the durability and routing contract for buckets. A cell name is part of the operator's failure-domain contract. A node id is part of the lifecycle contract for one server process identity. Treat those names as durable operational identifiers, not labels to casually rename after traffic starts.

A node can advertise capabilities such as object, index, PersonalDB, gateway, or admin, but those capabilities do not create a separate worker class. The same Anvil process participates in mesh state and uses leases for background tasks. When writing runbooks, prefer "node with index capability" over "index worker node" so the lifecycle model remains accurate.
