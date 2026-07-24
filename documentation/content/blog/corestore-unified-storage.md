---
title: Anvil 0.3.0: CoreStore, hardened administration, and book-quality operations docs
slug: /blog/corestore-unified-storage/
description: Anvil 0.3.0 turns CoreStore into the durable centre of objects, metadata, search, authorisation, watches, PersonalDB, mesh lifecycle, and gateway records.
release: v0.3.0
release_date: 2026-07-23
artifacts:
  rust_crate: anvil-storage 0.3.0
---

# Anvil 0.3.0: CoreStore, hardened administration, and book-quality operations docs

Anvil 0.3.0 is the release where the system becomes easier to explain because the architecture has become stricter. Objects, metadata, search indexes, relationship authorisation, PersonalDB witness records, mesh control records, gateway records, leases, watches, audit evidence, and repair findings now share the same durable centre: CoreStore.

That matters because object storage becomes product infrastructure as soon as the stored object is used by more than one subsystem. A document is not only bytes; it may need a current pointer, version history, metadata filters, full text search, vector retrieval, relationship checks, public links, append-only audit records, repair diagnostics, and a live watch stream. If every feature stores its own truth, operators eventually have to debug several small databases and guess which one is authoritative.

Anvil now takes the opposite approach. Feature-specific formats still exist where they are useful, but feature-specific durable storage does not define the system. Every authoritative record is written through CoreStore primitives, every derived view is tied back to source records, and every public result is designed to be explainable in terms of source data, authorisation, generation, cursor, and repair evidence.

## The release in one sentence

CoreStore gives Anvil one durability model for objects, metadata, indexes, permissions, watches, PersonalDB, mesh lifecycle, gateways, leases, audit, and repair.

The implementation is built around three primitives:

| Primitive | Purpose | Examples |
| --- | --- | --- |
| `CoreObject` | Immutable bytes addressed by content and manifest evidence. | Object payloads, multipart output, index segments, source packs, PersonalDB snapshots, gateway blobs. |
| `CoreStream` | Ordered facts that must be replayable, watched, sealed, compacted, or repaired. | Object mutation history, authz tuple events, audit events, append stream records, PersonalDB commits, lifecycle events. |
| `CoreRef` | Compare-and-swap heads over immutable objects and streams. | Current object pointers, index generations, PersonalDB heads, routing records, task leases, ownership fences. |

Those primitives are small enough to reason about and strong enough to cover the product surface. They are also the boundary the documentation now teaches from first principles.

## What changed for object storage

Anvil still behaves like an object store at the public edge: a tenant creates a bucket, writes an object under a key, reads the object later, lists by prefix, and can use the S3-compatible gateway for existing tools. The internal implementation is now more disciplined.

A write stores payload bytes as immutable CoreObjects, records metadata and version facts through CoreStore-backed mutation records, and advances the current pointer through a guarded ref update. A delete writes a delete marker and moves the current pointer into a not-found state for ordinary reads. Versioned reads, object heads, listings, and watches all derive from the same source records rather than parallel metadata stores.

This gives application developers a clean model:

- the object body is the durable source payload;
- object metadata is protected storage metadata, not a casual label bag;
- the current pointer explains ordinary reads;
- older version ids explain reproducible reads;
- delete markers explain current not-found responses;
- watch cursors explain what changed and how far consumers have caught up.

It also gives operators better failure analysis. If an exact object read works but listing is wrong, the issue is in derived listing state. If an index returns stale results, the source object and the index generation can be inspected separately. If an overwrite fails because a precondition does not hold, the system rejected a race instead of silently losing a write.

## What changed for metadata and indexes

Indexes are now documented and operated as derived views over source records. Path indexes, metadata-filter indexes, typed JSON indexes, full-text indexes, vector indexes, and hybrid indexes are described in terms of source selection, extraction, materialisation, query planning, authorisation, and diagnostics.

That gives every index definition a clear set of questions:

| Question | Definition field |
| --- | --- |
| Which source records are eligible? | `selector_json` |
| What values are extracted? | `extractor_json` and `build_policy_json` |
| Which index format is built? | `kind` |
| How are results protected? | `authorization_mode` |
| How does a caller prove freshness? | watch cursors, index generation, catch-up controls, diagnostics |

Typed JSON indexes can materialise fields from canonical JSON object bodies or append records. Full-text indexes tokenise selected text. Vector indexes store model provenance, dimensions, metrics, and ANN configuration. Hybrid indexes combine full-text and vector signals into one queryable index. The reference docs now spell out the JSON shapes instead of dropping unexplained snippets into tutorials.

The query side is permission-aware. Protected indexes can inherit object visibility, so a search result is not considered safe merely because it came from a segment. Anvil plans queries around source identity and authorisation context, and page tokens are bound to the caller, tenant, bucket, index generation, query shape, predicate hash, order hash, and authorisation revision. This avoids the classic mistake of retrieving a broad search result and hoping application code filters it correctly afterwards.

## What changed for full text, vectors, and hybrid search

Full-text search is now described as a token-based index over selected text, not as a magic query box. The docs explain tokenisation, lowercasing, NFKC normalisation, phrase support, snippets, metadata filters, and the current plain-text query semantics.

Vector search is documented around real production concerns: vector dimension, metric, embedding provenance, modality, chunking, provider configuration, and the difference between caller-supplied vectors and provider-generated embeddings. The deterministic development provider is treated as development plumbing, not as a production embedding model.

Hybrid search is now explained as one index that stores text and vector segments under one definition. It is not a pointer to two unrelated indexes. That difference matters for operations because the hybrid index has one definition, one diagnostics surface, one result list, and one authorisation story.

## What changed for authorisation

The authorisation documentation now separates three concepts that were too easy to conflate:

1. **Public policy scopes** decide which tenant application credentials may call public API operations such as `object:write`, `index:read`, `bucket:create`, or `personaldb:commit`.
2. **Relationship authorisation** models tenant product permissions with schemas, tuples, usersets, zookies, checks, and list calls.
3. **System-realm admin relations** protect the private admin API and are not tenant policy scopes.

That split is central to safe operation. A tenant app can use public policy scopes to write a bucket, grant a narrower app access, or query an index. The same tenant can define product-level relationship schemas for its own resources. It cannot use public APIs to rewrite Anvil's system realm or grant itself mesh administration.

The private admin API is now treated as a real network-administered control plane. It is not a local storage writer, it does not need direct access to CoreStore files, and it must be kept on a private network. The `anvil-admin` CLI talks to that private listener. Bootstrap happens before request serving, installs the built-in system realm, creates the first admin credential, and then gets out of the way. After that, admin operations authenticate and authorise like ordinary operations in the correct realm.

## What changed for task leases and fenced mutations

Anvil now exposes task leases as a correctness primitive for background work. A task lease names work inside a tenant, binds ownership to the authenticated caller, returns a fence token, and rejects stale checkpoint or commit attempts.

The important property is not that a lease exists. The important property is that a protected mutation can say: apply this only if I still hold the lease fence I think I hold. That lets queue workers, index repair jobs, ingestion jobs, projection workers, and maintenance loops fail closed when ownership changes.

The tutorials now explain the difference between acquiring a lease, checkpointing work, committing a lease, force-releasing a stuck lease, and using fenced write preconditions for actual state changes.

## What changed for saga-shaped workflows

This release reserves the saga protocol surface without enabling saga execution. The protobuf service, mutation execution contexts, response extension fields, and Rust client types now describe the shape of the durable saga API planned for multi-root workflows. That lets downstream client code compile against stable names while Anvil keeps the execution engine fail-closed until the state machine, recovery loop, and compensation records are implemented.

The important release behaviour is explicit rejection. Every `SagaService` method returns `UNIMPLEMENTED`. Any public or native mutation context that carries a saga operation or compensation operation is rejected before it can mutate a bucket, object, index, registry record, mesh record, or other write target. The high-level Rust saga client API is also intentionally reserved: constructing the types is possible, but calling the high-level behavioural methods panics with a reserved API message.

This is an API reservation, not hidden saga support. It does not create saga roots, saga writer segments, saga reference holds, or saga execution tasks in this release. Ordinary explicit transactions remain the implemented coordination primitive for writes that are scoped to one root.

## What changed for watches and derived data

A watch stream is Anvil's way of saying that a committed change has a durable cursor. Consumers can process changes, write derived state, and store their checkpoint only after the derived work is durable. If a consumer crashes, it can resume from the last checkpoint. If it falls too far behind, repair or rebuild can use source records instead of pretending the cursor is still enough.

That model appears across object changes, index definitions, index partitions, authz tuple logs, PersonalDB groups, and other derived systems. The docs now teach the same loop everywhere:

1. read from a durable source cursor;
2. perform idempotent derived work;
3. publish derived output with appropriate preconditions;
4. checkpoint only after the output is durable;
5. use diagnostics and repair when the retained cursor window is no longer enough.

## What changed for PersonalDB

PersonalDB is documented as a witness service for local-first SQLite, not as hosted SQL. Application replicas own their local SQLite files. Anvil owns the witnessed history: changesets, base heads, commit certificates, snapshots, projections, row metadata, projection watch records, catch-up responses, and repair findings.

That means an operator can debug shared evidence without pretending the server is the user's local database. A missing row might be an unsubmitted local change, a base-head mismatch, an authorisation denial, a projection lag issue, a snapshot/catch-up issue, or a repair finding. The new docs make those distinctions explicit.

## What changed for mesh routing and lifecycle

Regions, cells, and nodes are now treated as first-class operational concepts in the documentation and control plane. A region is a placement and routing boundary. A cell is typically a rack or equivalent failure domain inside a region. A node is one Anvil process with advertised capabilities and lifecycle state.

The mesh lifecycle records describe where buckets live, how host aliases route, which nodes are active, which nodes are draining, which regions are writable, and how a request should be served when it arrives in the wrong place. That is separate from object permissions: routing gets a request to the right place; authorisation decides whether the caller may see or mutate the resource.

## What changed for gateways

The supported public gateway surface is no longer allowed to define Anvil's storage model. The native public API, the S3-compatible gateway, static host aliases, and link resolution all map onto Anvil tenants, buckets, object keys, versions, public policies, relationship checks, and CoreStore records.

This release also standardises gateway foundation records for package and registry-shaped use cases: mounts, credentials, repositories, blobs, manifests, tags, upload sessions, signatures, challenges, and audit records. The point is architectural consistency. Gateway protocols should adapt to Anvil's model; they should not smuggle S3-era assumptions into the core.

## What changed for the CLIs

The public CLI is `anvil`. It is a tenant-facing helper over the public API for smoke tests, manual operations, and examples. It is not the only or primary application integration path; production applications should use the API or Rust client when they need typed responses, richer request fields, stable idempotency, explicit preconditions, or long-running consumers.

The admin CLI is `anvil-admin`. It is an operator helper over the private admin API. It is used for tenant creation, initial application provisioning, policy grants, topology lifecycle, host-alias administration, repair, diagnostics, audit, and secret-envelope rotation. It does not write directly into the server storage directory.

The docs now include a CLIs section with command-family references and workflow pages. The tutorials use the CLIs only where they help a human perform a manual task; they also explain which API concept each command maps to and what the command proves.

## What changed for release operations

The release workflow now treats release evidence as part of the product. The shared gate checks documentation hardening, release-note rendering, the Fission documentation site, the Rust client dry run, and the workspace test suite. Pull requests run the Docker E2E tests that release will run, so a release tag should not discover a failure that a PR could have caught.

The Docker image is built from the Anvil server Dockerfile, release-tested, then pushed with the release tag and `latest`. The Rust client crate is published only when the requested version does not already exist. GitHub release notes are rendered from this blog post and enriched with artifact metadata from the release workflow.

Documentation publishes through a separate Fission static-site workflow. That keeps docs publishing independent from server releases while still letting release notes link to the exact public documentation.


## Architecture status for this release

This release is structured around a clear storage contract. CoreMeta stores metadata in RocksDB column families. Bounded tiny payloads may be stored in the inline payload column family, with a 32 KiB raw default inline cap and a 64 KiB encoded CoreMeta value ceiling. Larger durable bytes, including object bodies and large writer segments, go through the CoreStore byte pipeline and are stored as erasure-coded shard data.

That storage boundary is the important release point. It means the system can continue improving query grammar, gateway coverage, watch ergonomics, and transport performance without asking operators to move data from a feature-specific side store into CoreStore later. Indexes, streams, authz segments, PersonalDB records, registry records, and mesh records now fit the same mental model: metadata and locators in CoreMeta, payload-like bytes through the byte pipeline.

The Architecture book now expands this in detail: [Architecture Overview](/architecture/overview/), [CoreMeta and Blob Storage Layout](/architecture/storage-layout/), [Indexing and Query Architecture](/architecture/indexing-and-query/), [Streams, Watches, and Mesh Transport](/architecture/streams-watches-and-mesh/), and [Release Architecture Status](/architecture/release-status/).

## Performance progression

The optimisation work for this release moved common metadata-heavy paths from tens of seconds into low-single-digit seconds, while preserving the CoreMeta quorum and commit-certificate model. The following numbers are the observed progression from the release investigation. They are useful as release context rather than as universal benchmarks for every deployment.

### Write path

| Run | Main change | Tenant | App | 7 grants | Token | Bucket | PUT 27B | Authz write |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | before optimisation | 66.42s | 16.02s | 19.48s | - | 48.70s | 32.01s | 19.04s |
| v1 | first three optimisations | 7.871s | 1.781s | 5.043s | 54.1ms | 9.291s | 6.976s | 5.679s |
| v2 | inline path | 7.904s | 1.826s | 5.107s | 1.86ms | 9.067s | 6.941s | 2.426s |
| v3 | authz delta | 7.944s | 1.841s | 2.119s | 1.95ms | 5.989s | 3.691s | 2.440s |
| v4 | CoreMeta batching | 5.366s | 1.333s | 1.544s | 5.16ms | 4.350s | 2.814s | 1.998s |
| v5 | stream batching | 3.914s | 816ms | 1.296s | 2.21ms | 3.226s | 2.439s | 1.656s |
| v6 | RPC instrumentation | 3.878s | 833ms | 1.248s | 2.10ms | 3.289s | 2.691s | 1.758s |
| v7 | CoreMeta streaming | 1.261s | 272ms | 373ms | 3.88ms | 1.462s | 1.500s | 531ms |

### Read and query path

| Run | GET 27B | Permission check | List authz objects | List objects cold | List objects warm |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 440ms | 280-480ms | - | 26.26s | 1.33s |
| v1 | 8.86ms | 5.11ms | 4.71ms | 403ms | 401ms |
| v2 | 7.07ms | 3.70ms | 4.28ms | 365ms | 363ms |
| v3 | 6.06ms | 6.39ms | 7.59ms | 24.5ms | 24.0ms |
| v4 | 4.45ms | 5.60ms | 5.08ms | 25.2ms | 21.0ms |
| v5 | 4.99ms | 4.70ms | 4.94ms | 21.2ms | 18.5ms |
| v6 | 10.6ms | 5.86ms | 5.24ms | 27.3ms | 20.9ms |
| v7 | 9.86ms | 5.51ms | 7.49ms | 21.4ms | 19.6ms |

## How to read the new documentation

The documentation has been rebuilt as five books:

- **Learn** teaches the model from first principles: objects, keys, CoreStore, regions, reads, writes, watches, indexes, authorisation, gateways, PersonalDB, and primitive selection.
- **Architecture** documents CoreMeta, RocksDB column families, inline payload policy, the byte pipeline, index segment formats, watches, mesh transport, and release status for contributors and reviewers.
- **Tutorials** turns those concepts into concrete operations: local Docker setup, bootstrap, tenants, buckets, metadata, versions, links, authorisation, public access, watches, search, streams, leases, PersonalDB, S3, static hosting, package gateway foundations, mesh lifecycle, repair, and an end-to-end document system.
- **Operators** covers production decisions: deployment, networking, topology, secrets, admin-plane control, provisioning, CoreStore operations, observability, indexes, watches, gateways, PersonalDB, backup, repair, capacity, upgrades, security, incidents, and release readiness.
- **Reference** documents the public CLI, the admin CLI, CLI workflows, authorisation actions/resources, and the JSON shapes for index definitions and queries.

The goal is not to make every page short. The goal is that a developer or operator can start with no Anvil-specific knowledge and build a correct mental model before copying commands into a terminal.

## Upgrade posture

This release is intended to be consumed as a coordinated server/client/docs release. Use the same release tag for the server image, the CLIs shipped with that image, the Rust client crate, and the documentation. Pin Docker deployments by release tag or digest. Keep the admin listener private. Treat `STORAGE_PATH` as durable state. Back it up before changing storage-affecting releases. Run the release readiness checklist before promoting a new deployment.

## Getting started

Start with the Docker-first setup tutorial if you want to run Anvil locally. Read the Learn section if the vocabulary is new. Use the Tutorials book when you want to perform a specific operation. Use the Operators book before a real deployment. Use the Reference book when you need exact command families, action/resource strings, or JSON field shapes.

Anvil 0.3.0 is a storage release, an operations release, and a documentation release. It makes the system stricter internally and clearer externally, which is the right foundation for teams that want object storage to carry real product data rather than only static blobs.

## What to validate after upgrading

After upgrading to this release line, validate both the new storage discipline and the hardened administration path. Run an object write/read, an index query with diagnostics, a relationship-authorisation check, a PersonalDB catch-up if your deployment uses it, `anvil-admin diagnostics list`, and one read-only routing list. Those checks prove more than process health: they prove source records, derived views, public policy, system-realm authorisation, and admin-plane connectivity are all still aligned.
