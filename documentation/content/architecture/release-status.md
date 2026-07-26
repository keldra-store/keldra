---
title: Release Architecture Status
description: Public release status for Anvil 0.3.0, including storage layout, indexing capabilities, mesh transport, S3 compatibility, tests, and performance progression.
---

# Release Architecture Status

This page records the practical architecture status for the 0.3.0 release. It is written for readers who need to know what is structurally in place, what behaviour is available today, and where the implementation is intentionally staged for future releases.

The release criterion here is not whether every future capability is complete. It is whether the storage architecture is coherent enough that applications can depend on it without expecting a disruptive storage migration in the next patch. On that question, the answer is yes: CoreMeta is the metadata plane, RocksDB is its local engine, bounded inline payloads are explicit, larger durable bytes go through the byte pipeline, index segment bodies follow normal writer-output rules, and gateways map into the same tenant/bucket/object/security model.

## Storage status

| Area | Current status |
| --- | --- |
| Metadata | Stored as CoreMeta rows in RocksDB column families. |
| Tiny payloads | Stored in `cf_inline_payloads` when eligible under the inline policy. Raw inline cap defaults to 32 KiB; encoded CoreMeta value cap is 64 KiB. |
| Large payloads | Stored through the CoreStore byte pipeline using erasure-coded shard placement. |
| Index segment bodies | Writer segment output; inline if tiny, otherwise byte-pipeline stored. Segment locators are CoreMeta rows. |
| Streams | Ordered append records with CoreMeta stream heads/indexes and CoreStore payload storage. |
| Mesh metadata | Logical CoreMeta replication with quorum evidence and commit certificates. |
| Gateway records | Registry/gateway/git-source metadata is CoreMeta-backed; large blobs use the byte pipeline. |
| Operator exports | Bootstrap credential JSON and reports can be exported outside storage; they are not Anvil source-of-truth state. |

## Index status

| Index family | Available behaviour |
| --- | --- |
| Path | Prefix/listing-shaped acceleration over object metadata. |
| Metadata filter | Equality filters over object user metadata. |
| Typed JSON | Object body, object metadata, and append-record field extraction with equality, membership, range, prefix, existence, null/missing, ordering, and boundary participation where supported. |
| Full text | Tokenised postings, BM25 scoring, phrase mode, selected text extractors, and final visibility checks. |
| Vector | HNSW graph, configured dimensions/metric/modality, caller-supplied or provider-generated vectors, and final visibility checks. |
| Hybrid | Full-text plus vector candidate blending with a fixed current scoring recipe. |
| PersonalDB row metadata | Row/projection-oriented metadata indexes for PersonalDB workflows. |
| Git source | Source-pack/repository-oriented index records. |

The staged query-language work is expressiveness, not storage redesign. Full-text boolean grammar can extend the current postings model. Hybrid fusion can become more configurable over the current segment model. Prefix watches can remain simple while indexed queries handle richer predicates.

## Mesh and watch status

CoreMeta metadata quorum traffic uses persistent bidirectional gRPC streams. The stream tracks request ids, pending responses, timeout, closure, eviction, and reconnect. TCP_NODELAY is enabled on server listeners. Blob shard writes are still separate internal calls, while shard reads stream response chunks. This leaves room for transport optimisation without changing the durable layout.

Prefix watches are implemented as bucket/prefix/cursor streams. They are suitable for object-change consumers that know their prefix. They are not intended to become a general query language. Index, authz, PersonalDB, and other derived systems expose watch or watch-like surfaces for maintenance and catch-up.

## S3 compatibility status

The S3-compatible gateway supports normal object-shaped operations through the official AWS SDK in the test suite: put, get, list, range get, multipart flows, public/private access, routing/alias cases, streaming upload, and index/compaction interactions. S3 remains a gateway. Use the native API or Rust client for Anvil-specific capabilities such as relationship-aware queries, typed indexes, watches, leases, PersonalDB, and repair workflows.

## Performance progression

The table below shows the end-user performance progression from the optimisation investigation. Times are wall-clock measurements from the observed runs. A dash means the value was not recorded in that run.

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

These figures show the shape of the release: metadata-heavy writes moved from tens of seconds into low-single-digit seconds, token/read/check paths are millisecond-scale, and listing moved from multi-second cold paths into tens of milliseconds in the measured runs. The remaining performance work is normal optimisation over the current architecture rather than a reason to change the storage model.

## Verification posture

The repository contains broad source, integration, Docker, S3 gateway, CoreStore conformance, query-planning, authz, object, client, and model tests. The release process should still run the release gate before tagging. This page is an architecture status report; it does not replace a green CI run.

## Staged extensions

The following items are intentionally described as staged extensions:

- richer full-text boolean query grammar over the existing postings architecture;
- more configurable hybrid fusion over the existing full-text/vector segment architecture;
- application-level heartbeat signals for long-idle internal streams;
- broader streaming optimisation for blob shard writes;
- richer filtered watch APIs where an index query is not the better interface.

Those extensions improve behaviour and ergonomics, but they do not require replacing RocksDB CoreMeta, changing the inline payload rule, or replacing the erasure-coded byte pipeline.
