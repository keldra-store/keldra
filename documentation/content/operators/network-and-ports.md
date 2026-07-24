---
title: Network and Ports
description: Bind and expose Anvil's public and admin listeners while protecting authenticated node-to-node gRPC.
---

# Network and Ports

A port is a trust boundary, not just a TCP socket. The address a process binds, the address committed for other nodes to dial, the proxy that rewrites host headers, and the network allowed to reach each listener all become part of Anvil's security and routing model.

Anvil has two listeners. The public listener serves tenant-facing gRPC, the S3/static HTTP gateway, and authenticated internal node RPCs. The private admin listener serves operator RPCs. Internal node RPCs have a distinct authorisation boundary, but they do not use a separate discovery or transport listener: equal Anvil nodes communicate by authenticated gRPC at the `public_api_addr` committed in CoreMeta lifecycle topology.

Read this page with [Production Model](/operators/production-model/), [Deployment](/operators/deployment/), [Security Hardening](/operators/security-hardening/), [Gateways](/learn/gateways/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [S3-Compatible Gateway](/tutorials/s3-gateway/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## The Public Listener

`API_LISTEN_ADDR` is the local bind address for the public server. By default it is `0.0.0.0:50051`, which means "listen on all interfaces inside this network namespace". In a container that is often correct because Docker, Compose, Kubernetes Services, or a load balancer controls reachability. On a bare host, every routable interface may be reachable unless a firewall says otherwise.

The public listener may be exposed when the deployment is meant to serve tenants or public objects. Tenant API calls still authenticate, public policy scopes still decide which API family and resource the caller may use, relationship authorisation still filters protected data, and reserved namespaces such as `_anvil/` remain inaccessible through gateways.

Internal CoreStore and recovery services are also mounted on this listener. They are not tenant APIs: callers must present the configured node bearer credential and satisfy the built-in system-realm relation for node RPCs. Network policy and TLS should restrict node-to-node paths even though protocol authentication and authorisation still fail closed.

`PUBLIC_API_ADDR` is different from `API_LISTEN_ADDR`. It is the stable gRPC endpoint committed in this node's lifecycle descriptor and used by other nodes for routing, proxying, replication, quorum, and recovery. It must be an address every authorised node that may contact this node can actually dial, not a wildcard bind address. In a container, use an internal service DNS name or routable load-balancer address such as `http://node-a.internal:50051`, not `http://0.0.0.0:50051`.

The committed lifecycle descriptor is authoritative. DNS, a running container, an open port, or an operator inventory does not make a process a member. Register, activate, drain, update, or remove nodes through the admin lifecycle API so CoreMeta topology changes atomically and auditably.

## The Admin Listener

`ADMIN_LISTEN_ADDR` is the bind address for the private admin gRPC API. It defaults to `127.0.0.1:50052`. That default lets a local single-node deployment run admin commands from the same network namespace without publishing operator capabilities.

In production the admin API must stay private. It still requires authentication and system-realm authorisation, but network isolation is another layer of defence. The admin API creates storage tenants, manages committed topology, runs system diagnostics and repair, rotates server-side secret envelopes, and reads administrative audit evidence. It must not share public ingress routes used for S3, static hosting, or tenant gRPC.

If `ADMIN_LISTEN_ADDR` is not loopback while `ALLOW_PUBLIC_ADMIN_LISTENER` is false, startup fails. Setting `ALLOW_PUBLIC_ADMIN_LISTENER=true` only acknowledges that the operator intentionally uses a non-loopback bind protected by private networking, firewall rules, service-mesh policy, or a bastion. It does not make the admin API safe for the internet.

For Docker or Compose, do not publish `50052` unless the host network is itself the private operator boundary. For Kubernetes, use an internal Service restricted by NetworkPolicy and keep external load balancers and ingress away from it.

## Authenticated Node-To-Node gRPC

Anvil has no separate node-discovery network. Nodes derive the eligible routing set from committed CoreMeta lifecycle descriptors. Each descriptor binds a stable node id to its region, cell, Ed25519 receipt-signing public key, `public_api_addr`, capabilities, lifecycle state, and generation.

`CORESTORE_INTERNAL_BEARER_TOKEN` is the credential a node presents when it calls internal services on another node's public listener. The corresponding node principal must have the system-realm relation required for internal RPCs. An empty value disables remote internal writes; a distributed placement fails instead of silently falling back to local-only storage.

`BOOTSTRAP_NODE_IDS` is a first-boot authorisation input, not a discovery list. It admits the named node principals to the system realm when the realm is created. Configure the intended genesis node ids consistently, provision each node's bearer credential securely, then commit node descriptors through the lifecycle workflow. Later membership changes are lifecycle mutations, not environment-variable discovery.

The Ed25519 node signing private key is generated and stored in node-local CoreMeta under that node's `STORAGE_PATH`. Its public key is committed in the lifecycle descriptor and is used to verify signed storage evidence. Nodes do not share this private key, and there is no shared mesh secret.

When node reachability is wrong, simple local requests may still work while quorum, replication, proxying, or recovery fails. Diagnose the committed descriptor first: confirm the node is in the expected active lifecycle state, its generation is current, its signing public key matches its local descriptor, and its `public_api_addr` is reachable by authenticated gRPC from the other nodes.

## Gateways And Host Routing

The public listener multiplexes native gRPC and HTTP gateway traffic. Requests with gRPC content type are routed to gRPC services; other HTTP requests enter the S3/static gateway router. Hostnames such as `s3.example.com`, `static.example.com`, or tenant custom domains remain public data-plane routes and must never route to the admin listener.

`PUBLIC_REGION_BASE_DOMAIN` enables region-aware host routing in the gateway. It does not grant read access, make a bucket public, create DNS records, issue TLS certificates, or bypass object authorisation.

`TRUSTED_PROXY_SOURCE_RANGES` lists exact IPs or CIDR ranges whose forwarded metadata Anvil may trust. Requests from other sources ignore forwarded host metadata. This prevents clients from spoofing custom domains and means legitimate proxies must preserve host and scheme consistently. If S3 signed requests fail only through a proxy, check the effective host, TLS termination, `X-Forwarded-Proto`, and trusted proxy ranges before rotating credentials or widening permissions.

## A Typical Configuration Shape

These values show the listener, identity, and internal-call boundaries; they are not a complete deployment file:

```bash
NODE_ID=node-eu-west-1-a
MESH_ID=production
REGION=eu-west-1
CELL_ID=eu-west-1-a

API_LISTEN_ADDR=0.0.0.0:50051
PUBLIC_API_ADDR=http://node-eu-west-1-a.internal:50051
CORESTORE_INTERNAL_BEARER_TOKEN=<node-a-bearer-token>
BOOTSTRAP_NODE_IDS=node-eu-west-1-a,node-eu-west-1-b,node-eu-west-1-c

ADMIN_LISTEN_ADDR=127.0.0.1:50052
ALLOW_PUBLIC_ADMIN_LISTENER=false

PUBLIC_REGION_BASE_DOMAIN=eu-west-1.storage.example.com
TRUSTED_PROXY_SOURCE_RANGES=10.10.0.0/24
```

For Compose, give each node its own durable volume and stable internal DNS name. Publish public ingress only where needed and keep the admin port private. For Kubernetes, use one reachable gRPC Service endpoint per node or another stable address that can be committed as that node's `public_api_addr`; do not commit an address that load-balances to a different node identity.

## Readiness And Health Checks

The public HTTP gateway exposes `/ready` on the public listener:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

A successful response proves the listener is running and CoreMeta startup recovery is ready. During distributed recovery, an unavailable response includes recovery state such as reachable replicas, known and lagging roots, and settlement progress. Readiness does not prove tenant credentials work, admin authorisation works, object writes are durable, indexes are caught up, watches are healthy, a host alias is active, or the admin listener is private.

There is no reason to publish an unauthenticated admin health endpoint. Operator automation should make a narrow authenticated admin request from the private network when it needs admin-plane validation.

## Validation Checks Before Exposure

From a tenant network, prove the public API or gateway is reachable and rejects unauthorised requests cleanly, while the admin listener remains unreachable. From the operator network, prove the admin listener is reachable but still rejects callers without a valid admin token and system-realm authority.

Between nodes, prove authenticated gRPC reaches every committed `public_api_addr`; an absent or invalid node credential must fail. Compare the local descriptor from each node with the committed lifecycle record. Exercise a distributed write or recovery path rather than treating TCP reachability as sufficient evidence.

For gateways, verify path-style and host-routed requests deliberately, reject `_anvil/` paths, and test the effective host and scheme through the real proxy. Common configuration bugs include committing a wildcard or load-balanced address as a node endpoint, exposing the admin port publicly, trusting forwarded headers from every source, or giving multiple live nodes the same durable identity volume.

When a command fails, confirm its endpoint and trust surface before changing credentials. Pointing `anvil-admin` at the public service can resemble an authentication failure, while an internal node call made with a tenant credential must be rejected even though both use gRPC.
