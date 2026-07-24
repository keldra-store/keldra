---
title: Production Model
description: How Anvil should be deployed and reasoned about in production.
---

# Production Model

The production model is the mental map operators use before they choose ports, regions, cells, secrets, credentials, gateway exposure, and recovery procedures. If that map is wrong, the deployment can still appear to work: uploads succeed, objects read back, and a gateway returns files. The failure shows up later, when an admin listener is reachable from the public internet, a tenant credential is used for topology changes, a derived index is treated as durable truth, or a node is drained without understanding which bucket routes point at it.

Anvil should be operated as a mesh of equal server processes. A node may advertise different capabilities and may be selected for background responsibilities, but those responsibilities run inside Anvil processes; they are not a separate worker-node tier with a different storage model. Every durable feature should be layered on CoreStore, and every request should pass through the same authentication, authorisation, validation, and audit expectations for its plane.

This page sets the model used by the rest of the [Operators](/operators/overview/) book. For the conceptual background, see [Learn Anvil](/learn/overview/), [CoreStore](/learn/corestore/), [Authorisation](/learn/authorisation/), [Gateways](/learn/gateways/), and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/). For hands-on local setup, start with [Run Anvil Locally](/tutorials/setup-local-anvil/) and [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/).

## One Mesh, Distinct Trust Contexts

An Anvil deployment has two listeners and three authorisation contexts. They are served by the same binary, but they must not be treated as interchangeable authority.

| Surface | Who uses it | What it is for |
| --- | --- | --- |
| Public plane | Tenant applications, tenant automation, public clients, and enabled gateways | Object, bucket, authz, index, watch, stream, lease, PersonalDB, S3, and static-hosting work. |
| Admin plane | Operators and trusted automation | Storage-tenant bootstrap, first credentials, topology, routing repair, system-realm authority, secret-envelope operations, and admin diagnostics. |
| Internal node RPC | Equal Anvil nodes recorded in committed CoreMeta topology | Quorum, replication, recovery, proxying, and storage evidence over authenticated gRPC at each node's committed `public_api_addr`. |

The public plane is not unsafe; it is the tenant-facing contract. It should be reachable by the clients the deployment intends to serve, and it should still authenticate callers, enforce public policy scopes, apply relationship authorisation where the feature requires it, reject reserved namespaces, and produce audit evidence. The `anvil` CLI is a manual helper over this plane; production applications should call the public API directly or through a deliberate client wrapper. The [public CLI reference](/reference/public-cli/) shows the current manual surface.

The admin plane is more powerful because it changes the system boundary. It creates storage tenants before tenant credentials exist, changes mesh topology, and repairs system projections. Keep it on an internal network and require normal admin authentication and system-realm authorisation. The `anvil-admin` CLI is a network client over this plane, not a direct storage writer; see [Admin Plane](/operators/admin-plane/) and the [admin CLI reference](/reference/admin-cli/).

Internal node RPC is not a user API even though it is mounted on the public gRPC listener. Each caller uses a node bearer credential and must satisfy built-in system-realm authorisation. The committed lifecycle descriptor supplies the node id, endpoint, Ed25519 signing public key, capabilities, and state used for routing. Network policy and TLS should restrict these calls; application credentials and address obscurity are not substitutes for node authority.

When the planes are blurred, the symptoms are subtle. A CI job may use an admin token to upload tenant content because it was convenient during bootstrap. A gateway may be exposed correctly while the admin listener is accidentally published beside it. A local script may write storage files directly because it can see `STORAGE_PATH`. Each shortcut bypasses the evidence path operators need during an incident.

## Storage Tenants Are Not Product Users

An Anvil storage tenant is the isolation unit the platform enforces. It can contain one customer, one product environment, one autonomous system, or another durable boundary chosen by the operator and product team. It is not automatically the same as an end user, organisation, workspace, account, or project inside the application.

That distinction matters during handover. Operators create the storage tenant and the first application credential through the admin plane because no tenant credential exists yet. After handover, tenant-owned work should move to the public plane: creating buckets, rotating tenant applications, writing objects, defining tenant relationship schemas, writing tuples, managing indexes, configuring public access, and managing tenant-owned links or aliases where supported. The [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/) operator chapter and [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) tutorial explain that boundary in practice.

Application-level access then belongs in the tenant's model. Public policy scopes decide which API families a tenant principal may call. Relationship authorisation tuples and schemas decide which product subjects can see or act on product objects. Do not put every product user in a separate Anvil storage tenant just because the product calls them tenants, and do not let a storage-tenant admin credential become the application authorisation system. Use the [authorisation reference](/reference/authorisation-actions-and-resources/) when deciding which public policy actions a tenant principal needs.

## CoreStore Is The Durable Substrate

CoreStore is the durable substrate beneath Anvil features. Object bodies and manifests obviously belong there, but so do less visible records: refs, streams, append records, watch checkpoints, relationship authz records, routing records, gateway records, index segments, repair findings, audit events, and PersonalDB evidence. Feature code may optimise its record format, but durable feature state should still be recoverable through the CoreStore-backed model.

This model gives operators one recovery question instead of many private ones: can the CoreStore-backed source records be read, checked, watched, repaired, and backed up? If a feature creates a second durable store outside that boundary, backup, repair, audit, and security all become feature-specific exceptions. That is how a deployment ends up with object data restored but missing gateway records, or an index rebuilt from another stale index instead of from source records.

Source records and derived views are deliberately different. Object versions, current pointers, bucket records, authz tuples, mesh control records, append stream records, and PersonalDB commits are source records for their features. Directory listings, query indexes, full-text and vector segments, routing projections, derived usersets, diagnostics, and projections are derived views. Derived views can lag, fail, or be rebuilt; they should not become the only copy of a business fact. [CoreStore Operations](/operators/corestore-operations/), [Indexes and Query](/learn/indexes-and-query/), and [Watches and Derived Data](/learn/watches-and-derived-data/) expand this model.

## Topology Is An Operating Contract

Topology is the map that turns a request into placement and routing decisions. A region is the placement and routing boundary visible to tenants. A cell is typically a rack, failure, or capacity boundary inside a region. A node is one Anvil server process with declared capabilities. A bucket has a home region, and routing records tell the mesh how tenant names, bucket locators, and host aliases should resolve.

Committed CoreMeta lifecycle topology is the sole membership and routing authority. Anvil does not discover membership from running processes, DNS, network announcements, or a second registry. A node becomes eligible only through lifecycle mutation, and other nodes dial the exact `public_api_addr` committed for that identity.

Design topology before deployment, not after traffic arrives. Region ids should represent durable operating commitments, not temporary hostnames. Cell boundaries should match failures or maintenance work you can actually reason about. Node capabilities should describe what a process is allowed and resourced to do; a node should not advertise index, PersonalDB, gateway, or object responsibility merely because the binary contains that code.

The wrong topology model creates hard-to-debug failures. If cells are arbitrary labels, draining one cell tells you little about risk. If every node advertises every capability, background work can land on underpowered processes. If a bucket's home region is ignored, cross-region reads may depend on accidental proxy paths or stale redirects. If host aliases are treated as DNS-only state, static delivery can drift from the bucket and prefix records Anvil authorises.

Use [Topology Planning](/operators/topology-planning/) before [Deployment](/operators/deployment/). The current operator surfaces are improving, but some fine-grained lifecycle steps such as activation checkpoints, drain completion, and cross-region proxy behaviour may still be coarser or more partial than the ideal model in a given release. Treat those gaps as reasons to collect diagnostics and read the current [admin CLI reference](/reference/admin-cli/), not as reasons to edit storage files directly.

## Gateways Are Adapters, Not The Core Model

Gateways let existing clients talk to Anvil through familiar protocols and host shapes. The S3-compatible gateway maps S3 operations to Anvil bucket, object, metadata, version, listing, and authorisation behaviour. Static hosting maps hostnames and paths to bucket prefixes, object links, public-read rules, and object reads. Package gateway foundations should follow the same adapter pattern: registry concepts map to Anvil objects, metadata, links, streams, checksums, and authorisation rather than becoming a separate registry database.

The gateway should not own durable truth. It may hold short-lived request state, but persistent records should be Anvil records. It also should not weaken the plane split. Exposing S3 or static hosting does not make the admin API public, does not grant writes to public readers, and does not allow reserved namespaces such as `_anvil/` to leak through a different protocol.

This matters when operators debug incidents. A failed S3 read might be a signature problem, an object permission problem, a public-read policy problem, a bucket routing problem, or a CoreStore read problem. A static-hosting issue might be DNS, trusted proxy handling, host-alias state, link generation, dangling link behaviour, or object visibility. The gateway is where the outside protocol meets Anvil's model; it is not a separate security or recovery system. See [Gateway Operations](/operators/gateway-operations/) and [Gateways](/learn/gateways/) for the deeper treatment.

## Derived State Is Still Production-Critical

Anvil uses derived state to make reads and operations practical. Path indexes make listing fast. Metadata and typed indexes support structured queries. Full-text, vector, and hybrid indexes support search. Watch consumers maintain projections. Relationship authorisation may use derived userset state. Mesh routing may use projections from control records. PersonalDB projections make accepted commits useful to application readers.

Derived does not mean disposable during an incident. A stale search index can hide documents. A broken routing projection can make an existing bucket look absent. An authz derived-state bug can deny valid users or, worse, expose data. Operators therefore need two kinds of evidence: that the source records are correct, and that each derived view has caught up far enough for the caller's purpose.

The operational signals should reflect that distinction. Watch and index diagnostics should show source cursor, applied cursor, lag, last error, and rebuild or repair state where the current surface exposes them. Request metrics should be labelled by API family and gateway without logging bodies or secrets. Audit streams should record who changed system state and why. [Observability](/operators/observability/), [Index Operations](/operators/index-operations/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), and [Repair and Diagnostics](/operators/repair-and-diagnostics/) explain how to use those signals.

## What To Design Before Deployment

Before deploying Anvil for real tenants, decide the model explicitly. Choose which networks can reach the public and admin listeners and which nodes may call internal gRPC. Choose region ids, cell boundaries, node identities, capabilities, and stable node endpoints. Decide where `STORAGE_PATH` lives and how it is backed up. Store the server encryption key, previous key history, first-admin credential, per-node internal credentials, tenant app secrets, and bearer-token handling in an actual secret-management plan. Decide which gateways are exposed and which hostnames they serve. Decide the handover process from operator-created storage tenant to tenant-owned credentials and public API work.

Those choices also define release and incident evidence. A ready deployment can prove that tenant principals can write and read through the public plane, that admin operations require private reachability plus system authorisation, that internal RPC rejects non-node principals, that committed topology drives node routing, that CoreStore-backed state survives restore drills, that derived state reports lag rather than pretending to be fresh, that gateways respect the same object and authz model, and that repair starts from diagnostics instead of broad mutation.

If a current public CLI or admin CLI command does not expose a workflow at that level of detail, document the gap in your runbook and use the narrowest supported API or operator tool for your release. Do not replace a missing command with an unaudited storage edit. The production model is only useful if the deployment can explain how each state change happened.

## Production invariants

Keep these invariants true during normal operation: one server process owns each writable storage path; admin traffic stays private; tenant traffic uses public APIs; internal node RPC uses node credentials and committed endpoints; server secrets are not mounted into tenant jobs; node identity and signing keys remain in node-local CoreMeta under `STORAGE_PATH`; and backups restore that full storage path together with the secret-manager state required to decrypt it.

When an incident response action would break one of those invariants, treat it as a break-glass action. Capture evidence first and document how the deployment returns to the normal model.
