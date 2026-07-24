---
title: Topology Planning
description: Choose regions, rack-sized cells, node capabilities, placement policy, routing, and lifecycle strategy before creating buckets.
---

# Topology Planning

Topology is the operating map Anvil uses to place buckets, route requests, choose eligible nodes, expose gateways, and drain parts of the mesh safely. It is not an inventory spreadsheet to fill in after deployment. If the topology is vague, later failures are vague too: a bucket is "somewhere", a host alias points "at the cluster", a node is "probably safe to remove", and an index builder lands on whichever process happens to be running.

Plan topology before creating production buckets. The choices you make here shape public URLs, region selection, cross-region behaviour, tenant handover, gateway exposure, capacity headroom, and incident response. Read this page with [Production Model](/operators/production-model/), [Network and Ports](/operators/network-and-ports/), [Deployment](/operators/deployment/), [Capacity Planning](/operators/capacity-planning/), [Observability](/operators/observability/), [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/), [Mesh Routing and Lifecycle Tutorial](/tutorials/mesh-routing-and-lifecycle/), and [Admin CLI](/reference/admin-cli/).

## The Vocabulary Operators Design With

A mesh is one cooperating Anvil deployment: the universe where region descriptors, cell descriptors, node descriptors, tenant locators, bucket locators, host aliases, lifecycle records, routing projections, and admin audit should agree.

A region is the placement and routing boundary visible to tenants. In production it usually maps to a data centre, cloud region, sovereign boundary, or other location with real latency and operational meaning. A bucket has a home region. High-volume clients should prefer the regional endpoint for the bucket they are using, while generic routing, redirect, and supported proxy paths are fallbacks rather than a reason to ignore placement.

A cell is a smaller boundary inside one region. It is typically a rack, rack-like failure domain, zone slice, storage pool, Kubernetes node pool, or other capacity unit an operator can drain and reason about independently. A cell should describe correlated risk: shared power, shared top-of-rack switch, shared maintenance window, shared storage pressure, or another condition that makes its nodes fail or fill together.

A node is one Anvil server process. It may advertise capabilities such as `object`, `index`, `personaldb`, `metadata`, `gateway`, and `admin`, and it may run background responsibilities inside that same process. Do not design a topology that assumes separate worker-node binaries for those responsibilities. If you want to isolate gateway traffic from index-heavy work, do it through committed capabilities and deliberate routing.

The committed CoreMeta lifecycle descriptor is the authoritative membership record. Plan a stable node id, a `public_api_addr` that routes to exactly that identity, and durable Ed25519 signing identity for every node. There is no separate discovery registry to reconcile later.

## Regions And Bucket Distribution

Choose region ids as durable promises. `eu-west-1` is useful if it represents an operating commitment to a European region. `prod-a` or a cloud provider's temporary internal label is less useful because it leaks today's hosting decision into bucket placement, DNS, audit records, and customer support explanations.

Bucket distribution should start with the product's data and traffic boundaries. A media application may put public assets close to viewers and private originals in a compliance region. A document system may keep each customer's buckets in the customer's chosen region. A package delivery system may store immutable artefacts in one source region and use links, indexes, gateways, or mirrors for distribution. The point is to choose a bucket home region deliberately, not to create every bucket in the first region because it was easiest during bootstrap.

Tenant and bucket locators make these choices concrete. Tenant locator records identify where a storage tenant is routed. Bucket locator records identify a bucket's home region and home cell, locator status, placement policy, object prefix, timestamps, and generation. When a request reaches the wrong region, Anvil can redirect, proxy where that surface supports proxying, or reject according to routing policy. Those behaviours depend on locators being accurate and current.

Do not treat generic routing as a replacement for regional design. Generic endpoints are useful for discovery, migration, and low-volume callers. High-volume clients should use regional endpoints after they know the bucket's home region because that avoids avoidable redirects, reduces cross-region latency, and makes capacity easier to explain.

## Cells As Failure And Capacity Boundaries

Cells keep a region from becoming one flat bag of machines. In a small deployment, one cell may be enough because there is only one rack or node pool. In a larger region, use cells to reflect boundaries that matter operationally: rack 7 versus rack 8, zone slice A versus zone slice B, hot storage pool versus cold storage pool, or a Kubernetes node group with a separate maintenance window.

The cell boundary should be large enough to manage, but small enough to make failure analysis useful. A cell that spans many racks does not help you reason about a top-of-rack switch failure. A cell per process can be too fine-grained to plan capacity. A good cell lets you answer: if this cell drains, which buckets, indexes, gateway routes, and background owners are affected, and how much spare capacity remains elsewhere?

Capacity headroom belongs in topology planning, not only in alerts. Keep enough room for one cell to be unavailable while the region continues to serve expected traffic where your durability and availability target requires that. Also budget for rebuilds: full-text and vector indexes, PersonalDB projections, routing projections, and repair work can be more demanding than steady-state reads and writes.

## Nodes And Capabilities

A node descriptor should say what the process is intended and resourced to do. `object` means the process is eligible for object-serving responsibilities, including current remote object proxy selection where supported. `index` means it is intended for index work. `personaldb` means it is intended for PersonalDB witnessing, snapshots, or projection work. `metadata` means it can participate in CoreMeta responsibilities. `gateway` means it is intended to serve gateway traffic. `admin` means it participates in admin-plane responsibilities.

Keep these capabilities honest. A small all-in-one deployment can advertise every capability because the same process does everything. A gateway-heavy deployment may run some public-edge Anvil processes with `gateway` and `object` capability, while keeping index-heavy work on processes with more CPU and memory. An index-heavy deployment should not advertise `index` on underpowered gateway nodes merely because the binary supports it.

Capabilities are not a capacity guarantee by themselves. They are routing and ownership intent. The operator still needs metrics for CPU, memory, disk, request latency, index lag, watch lag, PersonalDB projection lag, and repair backlog. If a node advertises a capability but cannot keep up, the topology looks healthy while the service is not.

## Placement Policy And Routing Strategy

Placement policy in the current topology model is still mostly operator-driven. Region and cell descriptors record placement weights, and bucket locators carry placement information, but current simple creation paths often use the requested region, configured cell, and node rather than a full automatic scheduler that balances every future bucket for you. Treat weights as useful intent and future-proofing, not as proof that placement has been optimised everywhere.

For bucket creation, decide who chooses the region. Some products expose a user choice such as "Europe" or "United States". Others choose based on tenant contract, source data location, or compliance rules. The public bucket API names a region, but the operator must make sure that region exists, is active, has active cells and nodes, and has enough capacity for the expected bucket shape.

Routing strategy should be explicit. `redirect_preferred` is a good default for many deployments because it teaches clients to use the bucket's home region. `proxy_preferred` and `proxy_required` can be useful when clients cannot easily move to a regional endpoint, but proxying is not universal across all surfaces today. `local_only` is useful when wrong-region serving should fail rather than hide placement mistakes. Whatever policy you choose, include the expected behaviour in client documentation and incident runbooks.

Host routing and gateway exposure add another layer. A gateway-heavy region needs enough `gateway` capacity near the public edge and enough `object` capacity behind it. A custom host alias still resolves to a tenant, bucket, region, and prefix; it does not move the bucket or make private data public. If you plan static hosting or S3 virtual-host style traffic, include `PUBLIC_REGION_BASE_DOMAIN`, DNS, TLS, trusted proxy ranges, and host-alias lifecycle in the topology design.

## Lifecycle Strategy

Topology records are mutable, so lifecycle state and generations protect operator intent. A new region, cell, or node starts as joining. Active records are eligible for work. Draining records are leaving service. Drained records may be removable. Read-only and drained-with-exceptions states matter most for regions because bucket locators can still name a region as their home.

Design the lifecycle before you need it. Write down what it means to activate a region, activate a cell, activate a node, drain a node for maintenance, drain a cell for a rack event, and drain a region for migration or retirement. Include who is allowed to perform the change, what generation value must be checked, what audit reason is required, and which diagnostics prove the change was safe.

Region activation has a current surface gap. The admin API and CLI require an activation checkpoint file, and the server validates that checkpoint against control-stream evidence. That is a safety feature, not paperwork. However, the current public documentation and CLI do not yet provide a production-friendly checkpoint generation command. Do not hand-write fake checkpoints or edit storage records. Keep the region in the appropriate state until the checkpoint workflow is available for the release you operate.

Drain completion also needs caution. Admin commands can start region, cell, and node drains, and the lifecycle state machine knows about drained states and region drain exceptions. The current operator flow for gracefully completing every drain is still incomplete or coarse in places. Treat a drain command as recorded intent, then use diagnostics, routing records, bucket locator state, load balancer state, and process health to prove what actually happened.

## Planning Examples

A small single-region deployment might use one region called `local` or `eu-west-1`, one cell such as `eu-west-1-a`, and one or two all-in-one nodes advertising `object,index,personaldb,metadata,gateway,admin`. This is easy to understand and useful for early production or internal systems. Its main risk is concentration: one cell failure or one saturated process affects everything. The runbook should say whether the deployment accepts that risk or whether a second cell is required before external tenants arrive.

A multi-region deployment might use `eu-west-1` and `us-east-1`, each with its own public regional endpoint. Tenants or buckets are placed in the region that matches data residency and latency needs. Clients discover or store the bucket home region and use that endpoint for normal traffic. Generic routing can redirect or proxy some wrong-region requests, but it should not be the hot path for every read. Operators should compare bucket locator counts, object bytes, index lag, and gateway traffic per region before deciding a region is balanced.

Rack or cell planning starts with physical or orchestration reality. If a region has three racks, model them as three cells only if each rack can be drained and monitored separately. Keep spare capacity outside the draining cell. Avoid placing all gateway-heavy buckets, vector indexes, or PersonalDB groups in one cell just because it was the first active cell. During a drain, bucket locators and derived workloads should make the blockers visible.

A gateway-heavy deployment should plan public edge capacity and host routing first. Nodes that terminate S3/static traffic need enough network bandwidth, connection capacity, and trusted proxy configuration. They may also need `object` capability for local serving or proxy target eligibility. Gateway nodes should not accidentally become the only index builders unless they are sized for that work too. Validate `_anvil/` rejection, signed request host handling, public-read boundaries, and custom host aliases before serving public traffic.

An index-heavy deployment should plan CPU, memory, rebuild windows, and watch lag. Full-text, vector, hybrid, typed, and path indexes are derived from source records, so they need builders that can catch up after bursts and rebuild after extractor changes or repair. It may be sensible to dedicate nodes with `index` capability and avoid routing public gateway traffic to them. The capacity plan should include temporary headroom for rebuilds, not only steady-state query traffic.

## Current Public Surfaces And Gaps

The admin API and `anvil-admin` expose region creation and listing, cell registration and listing, node registration and listing, generation-checked lifecycle transitions where implemented, region drain requests with bucket dispositions, routing-record listing, routing repair, diagnostics, and audit. The public tenant API does not manage mesh topology; tenants use it for buckets, objects, indexes, authz, links, apps, public access, and tenant-owned host aliases where authorised.

Current limitations to design around are direct:

| Area | Practical effect |
| --- | --- |
| Region activation checkpoint generation | Activation requires a real checkpoint, but a production-friendly generator command is not documented/exposed yet. |
| Drain completion | Drained states exist, but the end-to-end graceful completion workflow is not fully exposed for every normal operator case. |
| Cross-region proxying | S3/static object-shaped proxy paths exist where eligible remote object nodes are known; native public gRPC object calls and bucket-management operations are not universally proxied. |
| Placement scheduling | Placement weights are recorded, but current bucket creation is not a complete automatic scheduler across every region and cell. |
| Tenant locator projection | Some tenant-locator behaviour still reflects the serving node's configured region; verify locators before relying on remote tenant home placement. |
| Capability enforcement | Capabilities guide routing and ownership decisions, but they do not replace capacity monitoring or prove every background scheduler is perfectly isolated. |

Document these gaps in your deployment runbook. The safe response to a missing surface is a narrow supported API, diagnostics, or an explicit operational limitation, not direct storage edits.

## Signals That The Topology Works

A healthy topology is visible. Region, cell, and node descriptors have expected lifecycle states and generations. The public endpoints for each region match `PUBLIC_API_ADDR` and gateway DNS. Bucket locators name the expected home region and cell. Host aliases route only when active and configured. Wrong-region requests redirect, proxy, or fail according to the chosen policy. Drains produce blockers or completion evidence instead of silent disappearance. Admin audit explains who changed topology and why.

Those signals should appear in dashboards and release checks before a production incident. If a user says an object disappeared, the first questions should include "which region owns the bucket?", "which locator status is current?", "did the request arrive at the wrong region?", "is the host alias active?", and "are routing diagnostics clean?" Topology planning is successful when those answers are easy to find.

## Pre-activation checklist

Before activating a region, cell, or node, verify that the node-local identity and Ed25519 public key match the committed descriptor, authenticated gRPC can reach the committed `public_api_addr`, the capability list and placement weight are correct, region/cell membership is expected, and the node volume is covered by backup. For regions, also verify public base URL and virtual-host suffix because gateways and redirects expose those values to clients.

Activation should be a confirmation that the resource is ready, not the first time the operator learns whether it can be reached.
