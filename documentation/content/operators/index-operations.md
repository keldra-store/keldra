---
title: Index Operations
description: Operate Anvil path, metadata, typed JSON, full-text, vector, hybrid, and specialised indexes as authorised derived data.
---

# Index Operations

Indexes make Anvil data searchable without turning search results into a second source of truth. The source record is still the object version, append record, PersonalDB commit, Git source record, or other CoreStore-backed event that Anvil accepted durably. The index is a maintained view over those records. It can be rebuilt, it can lag, and it can be wrong if extraction, authorisation, or repair is mishandled.

That is the operator mindset for this page. A healthy index is not merely an enabled definition. It has an understandable definition, a selected source set, a build worker that can keep up, diagnostics that explain rejected source records, repair paths for derived drift, capacity headroom, and authorisation behaviour that prevents search results from leaking protected objects.

Read this chapter with [Indexes and Query](/learn/indexes-and-query/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Authorisation](/learn/authorisation/), [CoreStore](/learn/corestore/), [Observability](/operators/observability/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), and [Repair and Diagnostics](/operators/repair-and-diagnostics/). The full request syntax is in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), public policy resources are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and command syntax is in [Public CLI](/reference/public-cli/) and [Admin CLI](/reference/admin-cli/).

## The index operating path

The operating path starts before the first command. Decide what question the product must answer, which source records are authoritative for that answer, and which principals are allowed to see the answer. Only then choose an index kind.

A document system might keep the canonical document body as an object, use object metadata for publication state, use a typed JSON index for dashboard predicates, use full-text search for keywords, and use vector or hybrid search for semantic retrieval. Those indexes are related because they come from the same objects, but they answer different questions and fail in different ways. A stale full-text index should not prevent a direct object read from proving the object exists. A typed JSON index with a missing required field diagnostic should not be repaired by changing the object authorisation model.

After the design comes the lifecycle: create or update a definition, let builders materialise rows, watch lag and diagnostics, query with an appropriate freshness requirement, repair from source records when derived state drifts, and retire definitions deliberately. Operators should make each phase observable. A dashboard that shows only request success rate cannot tell the difference between "there are no results" and "the index has not caught up".

## Index families and what they are for

Anvil's public Index service has several index kinds. They share the same lifecycle but not the same query semantics.

| Kind | Operational use | What it is not |
| --- | --- | --- |
| `path` | Fast prefix navigation over current object metadata and keys. Use it for folder-like views, import scopes, inventories, and simple object-key browsing. | It is not text search and it does not read object bodies. |
| `metadata_filter` | Exact equality filters over object user metadata, optionally narrowed by object key prefix. | It is not range search, containment search, or type coercion. |
| `typed_json` | Predicate and ordering queries over named JSON values extracted from object bodies or append records. | It is not full-text relevance and it does not currently scan every historical object version even though `object_version` is accepted by validation. |
| `full_text` | Token and phrase search over extracted text, ranked by the current scoring recipe. | It is not a boolean query language; `AND`, `OR`, wildcards, and parentheses are not implemented by the direct query path. |
| `vector` | Nearest-neighbour search over numeric embeddings, using the configured dimension, metric, normalisation, and HNSW parameters. | It is not an embedding generator for query text in the CLI, and it is not meaningful if source and query vectors come from different model spaces. |
| `hybrid` | One index definition containing both full-text and vector material, used when ranking needs lexical and semantic signals together. | It is not a reference to two existing indexes, and current direct hybrid weights are not configurable by CLI flag or build-policy field. |
| Specialised source kinds | `personaldb_row_metadata` and `git_source` exist as index enum values for source-specific work. Git source and PersonalDB also have service-specific surfaces. | They are not generic materialised `QueryIndex` paths today. Do not present them as ordinary tenant search indexes unless the implementation has grown that support. |

There is also an internal directory-index repair target used by the repair service for object listing and directory-derived state. That is not the same thing as a tenant-created `path` index, although both sit in the broader category of derived navigation over object metadata.

## Definitions are build contracts

An index definition answers build-time questions. Query fields ask read-time questions. Confusing those two phases is the source of many production surprises.

`selector_json` chooses which source records may enter the index. For object-backed indexes, the common selector is an object key `prefix`, optionally with a `content_type`. This is a build boundary. If a source object is outside the selector, no query-time `path_prefix` can bring it back.

`extractor_json` describes what to extract from each selected record. It is most visible for full-text and hybrid indexes, where it names text sources such as the whole UTF-8 body, object key, a JSON Pointer, or a metadata field. For `path` and `metadata_filter` it is usually `{}`. For `typed_json`, current field definitions live in `build_policy_json`. For `vector`, current validation requires the vector extractor inside `build_policy_json`; a separate vector extractor in `extractor_json` is rejected for kind `vector`.

`build_policy_json` describes how the materialised structure should be built. For full text, it controls tokenisation and positions. For typed JSON, it names fields, extractors, required values, source kind, and default order. For vector, it records the vector schema, embedding provenance, dimension, modality, normalisation, chunking, and HNSW settings. For hybrid, it contains both a `full_text` policy and a complete `vector` policy.

`authorization_mode` controls query visibility. The safest default is `inherit_object`, where the caller needs `index:read` to ask the question and each returned object must also be visible under object read authorisation or the built-in object-reader relationship. `index_only` and `public` skip the per-hit object read check, but they still require an authenticated caller with `index:read` on the bucket. They are for deliberately shareable derived catalogues, not private object content.

A compact typed JSON definition shows the shape without making the JSON mysterious:

```json
{
  "source_kind": "object_current",
  "fields": [
    {"name": "customer_id", "extractor": "/customer/id", "required": true},
    {"name": "state", "extractor": "/state", "required": true},
    {"name": "due_at", "extractor": "/due_at", "required": true}
  ],
  "default_order": [
    {"field": "due_at", "direction": "asc"}
  ]
}
```

`source_kind` says the builder should read the current object view. Each `fields` entry gives the query-time field name and the extractor used to read a JSON value from the object body. `required: true` records a diagnostic if the value is missing or null instead of silently indexing incomplete data. `default_order` is used when a typed query does not provide its own `typed_order_json`. The full grammar, including append-record extractors and vector definitions, lives in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).

## Create, update, disable, and drop deliberately

Applications should use the public Index API directly. The CLI is a manual helper over the same request fields and is useful for smoke tests or operator triage.

To inspect current definitions, include disabled indexes when you are planning a cleanup or investigating a migration:

```bash
anvil --profile acme index list documents --include-disabled
```

This calls `IndexService.ListIndexes`. A successful response proves the profile can authenticate, has `index:read` on the `documents` bucket, and can read index definition records. It does not prove that any definition has built rows, that source cursors are current, or that the caller can see every object returned by an `inherit_object` query.

A create command stores the definition and enqueues build work. For example, a typed JSON index for invoice due dates might be created like this:

```bash
anvil --profile acme index create documents invoices_by_due typed_json \
  --selector-json '{"prefix":"invoices/"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"customer_id","extractor":"/customer/id","required":true},{"name":"state","extractor":"/state","required":true},{"name":"due_at","extractor":"/due_at","required":true}],"default_order":[{"field":"due_at","direction":"asc"}]}' \
  --authorization-mode inherit_object
```

This proves the caller has `index:create` on `documents/invoices_by_due`, the bucket exists, the index kind is recognised, the JSON strings parse, the typed JSON build policy validates, and Anvil stored an enabled definition. It does not prove that objects under `invoices/` exist, that their bodies are valid JSON, that every required field is present, or that a materialised segment is ready. Those facts come from build progress, query responses, diagnostics, and watches.

Updating a definition is an operational change. It can change selected source records, extraction logic, tokenisation, vector provenance, or query visibility. Treat it like a rollout: know whether old rows remain usable during rebuild, watch diagnostics after the update, and avoid changing `authorization_mode` casually. Disabling an index should stop it being served while preserving definition history for inspection. Dropping an index is destructive to the definition and should be done only after callers have moved away or a replacement is verified.

## Build, rebuild, and repair

Index build work is asynchronous. A successful create or update enqueues work; it does not mean the index is ready. Builders read source records, extract material, write derived segments or metadata-backed rows, publish proof and generation evidence, and advance partition watches. That path can fail because source data is malformed, an embedding provider is missing, a vector has the wrong dimension, a required typed field is absent, or a segment cannot be published.

Diagnostics are the first read-only surface to inspect:

```bash
anvil --profile acme index diagnostics documents invoices_by_due \
  --severity warning \
  --page-size 50
```

This calls `IndexService.ListIndexDiagnostics`. A successful response proves the caller has `index:read` on `documents` and that diagnostics for the requested bucket/index can be read. It does not prove the index is complete. No output means no matching diagnostic records were returned in that page; it does not prove every selected source record was indexed or visible.

Repair is for derived drift or an intentional rebuild from source records. The public repair helper can target one tenant index:

```bash
anvil --profile acme repair run index documents invoices_by_due --rebuild
```

This asks `RepairService.RepairIndex` to inspect and rebuild the derived index state for that bucket/index. It proves the caller can reach the public repair service and has the relevant repair authority. It does not create missing source objects, fix bad JSON bodies, grant object visibility, or make unrelated indexes current. After repair, rerun the failing query and diagnostics, and check lag again.

Operators can also inspect administrative diagnostics from the private admin plane when the incident crosses tenant-facing surfaces or needs system evidence:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source index \
  --tenant-id acme \
  --bucket-name documents \
  --index-name invoices_by_due \
  --severity error \
  --limit 50
```

This proves the admin listener is reachable, the admin credential is authorised to view diagnostics, and the selected admin diagnostic backend can return findings. It does not scan all CoreStore records or prove backup recoverability. Keep admin diagnostics for operator evidence; tenant-owned index management should remain on the public API.

## Lag, cursors, and catch-up

An index can be correct for the cursor it has applied and still stale relative to the latest source write. That is why Anvil exposes cursor evidence in query responses.

For metadata-backed and typed JSON direct queries, response fields such as `source_watch_cursor_high`, `index_watch_cursor_applied`, `is_caught_up`, and `lag_record_count_hint` are meaningful. A product that must read its own write can carry a source watch cursor from the write or a prior watch response and require the query to have caught up:

```bash
anvil --profile acme index query documents invoices_by_due \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"},{"field":"state","op":"eq","value":"open"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"}]' \
  --require-caught-up-to-watch-cursor 12345 \
  --limit 20
```

The predicate and order fields are arrays, because that is the current typed JSON query shape. A successful result proves that the query was authorised, the typed segment could answer the predicate/order request, and the segment used by the query had caught up to at least cursor `12345`. It does not prove later writes are indexed, and it does not prove the index definition is the one the product intended.

If the index has not applied the required cursor, the service can fail with `IndexLagging` rather than returning stale rows. That is a useful correctness failure. Surface it as "indexing is catching up" or retry with a bounded user experience rather than translating it into an empty result.

Current direct full-text, vector, and hybrid query responses do not provide meaningful object-source catch-up evidence. They report caught-up status as true with zero cursor fields, and `lag_timeout_ms` does not generally make direct queries wait for freshness. Treat freshness for those indexes as an operational concern: watch source and index partition progress where available, inspect diagnostics, retry after expected build delay, and design user interfaces to show indexing state for fresh uploads.

## Query visibility and authorisation

Index permissions are public policy scopes. Create uses `index:create` on `bucket/index_name`; update and disable use `index:update` on `bucket/index_name`; drop uses `index:delete` on `bucket/index_name`. Listing, querying, and diagnostics currently use `index:read` on the bucket name, not on a specific index name. That bucket-level read scope is coarse, so design buckets and roles with that boundary in mind.

With `inherit_object`, `index:read` is only permission to ask the index service. The query path must also filter each hit by object visibility. A caller can see a hit if it has `object:read` for `bucket/key` or a matching relationship-authorisation tuple for the built-in object reader relation. This matters because object keys, search scores, hit counts, matched term metadata, and vector proximity can all leak information before an object body is read.

With `index_only` or `public`, the query path does not re-check object read for each hit. These modes do not make the public API anonymous. They mean every principal with `index:read` on the bucket can see the derived row contents returned by that index. Use them for catalogues, package metadata, or public search corpora only when those rows are deliberately safe at bucket-index-reader scope.

Prefix object-read grants can be expanded into permission labels for `inherit_object` filtering, but very broad prefixes can exceed the current permission-set cap and fail with `AuthzPermissionSetTooLargeForPrefixScope`. If that appears in production, narrow the bucket/prefix design or use relationship tuples rather than treating one broad prefix grant as a scalable long-term plan.

## Capacity and high-cardinality fields

Every index is an operational cost. A focused selector can keep cost predictable. A whole-bucket selector may be valid, but it says every current and future matching object is a candidate for extraction, diagnostics, rebuild, and query-time authorisation. Use whole-bucket indexes only when the product really needs them.

High-cardinality fields are not forbidden; they are simply expensive in different ways. A typed JSON field such as `document_id` or `email_message_id` may have one unique value per object. That can be useful for exact lookups, but it produces a large term space and can make range/order queries less cache-friendly than a small enum such as `state`. A full-text field with very large bodies and positions enabled consumes more segment space than short titles. A vector index with higher dimension, many chunks per object, or HNSW settings such as larger `m` and `ef_construction` consumes more build time, memory, and storage.

Metadata filters are exact JSON equality. If product code stores the same concept sometimes as a string and sometimes as a number, the index will preserve that inconsistency. Operators will see missing results that are really data-shape problems. Typed JSON comparisons are also based on extracted JSON values, so use consistent timestamp formats, numeric types, and null handling. Required typed fields are a good operational guard: they turn malformed records into diagnostics rather than silently creating partial rows.

Embedding providers deserve their own capacity plan. Caller-supplied vectors move embedding cost outside Anvil and make index build mostly extraction and ANN build work. Provider-generated vectors send object text to a configured command-based provider during build. The built-in `test_only` provider is disabled unless explicitly allowed and is not a production-quality semantic model. If no production provider is configured, Anvil cannot turn arbitrary text objects into useful production embeddings for vector or hybrid indexes.

## Full-text, vector, and hybrid specifics

Full-text operations fail most often because the extractor did not read the text operators expected. A JSON Pointer can be wrong, an object body can be non-UTF-8, or a field can be empty. Phrase queries require positions in the build policy. Stop-word and stemming-related fields may be accepted as provenance, but current direct full-text behaviour should not be described as a language-specific boolean search engine.

Vector operations fail most often because the vector contract changed. Dimension, modality, model, model version, normalisation, and metric must match between indexed vectors and query vectors. The CLI accepts numeric `--vector` values; it does not embed natural-language queries. If an embedding provider returns a different model version from the definition, or a configured provider is missing, the builder records diagnostics instead of silently mixing vector spaces.

Hybrid operations combine both failure modes. The current direct hybrid path stores a text segment and vector segment under one definition, with one selector shared by both sides. It is not a way to combine a broad text scope with a different vector scope. When both `query_text` and `query_vector` are supplied, it combines normalised text, vector, and freshness signals using the current fixed recipe, documented in the reference as 0.55 text, 0.35 vector, and 0.10 freshness. There is no public CLI flag or supported build-policy `weights` object for custom weights today. Direct hybrid queries do not evaluate `typed_predicates_json` or `typed_order_json`; use metadata filters for exact hard narrowing, or design an API path around query-spec planning where that fits current support.

## Source and specialised index surfaces

Not every derived view is an ordinary `anvil index query` target. PersonalDB row metadata and Git source appear in the Index API enum, but current generic `QueryIndex` does not materialise or query them as ordinary tenant indexes. PersonalDB has group, projection, watch, and repair surfaces. Git source has its own service for pack ingestion, object lookup, tree listing, and source watches.

Operate those specialised surfaces by their source contract first. For PersonalDB, the source of truth is the committed changeset log, heads, witnesses, snapshots, and projections. For Git source, the source is the ingested pack/source records and their object/tree mappings. Index-like structures built around them should still obey the same derived-data discipline: source first, cursor evidence, diagnostics, repair, and authorisation. Do not pretend a generic index definition is enough if the service-specific surface is the one actually serving the product.

## A practical triage flow

When a user reports missing or surprising index results, start with the narrowest source proof. Can the object or append record be read directly by an authorised caller? Does the source record fall under `selector_json`? Does the extractor match the actual payload and metadata shape? Does the current caller have `index:read` and, for `inherit_object`, object visibility? Is the index enabled and on the expected definition version? Are diagnostics present for the source record? Is the applied cursor behind the source cursor? Did a page token come from a different query shape or expired context?

Only after those questions should repair enter the picture. Repair can rebuild derived state from source records. It cannot make malformed JSON valid, generate a missing production embedding provider, change public policy scopes, or recover source records that were never committed or have been lost. Keep before-and-after evidence: the failing query, diagnostic records, repair response, lag fields, and a final successful query or a documented remaining gap.

## Current gaps to design around

Several limits are current behaviour, not operator error:

| Area | Current behaviour to account for |
| --- | --- |
| Read scopes | List, query, and diagnostics use `index:read` on the bucket name. They are not currently scoped to one index definition. |
| Catch-up | Metadata-backed and typed JSON direct queries expose meaningful cursor catch-up. Direct full-text, vector, and hybrid queries do not yet expose meaningful source-cursor freshness, and `lag_timeout_ms` does not generally wait. |
| Hybrid scoring | Direct hybrid scoring uses fixed implementation weights and freshness behaviour. There is no CLI or build-policy weight control today. |
| Typed JSON | `typed_predicates_json` and `typed_order_json` must be arrays. Direct full-text, vector, and hybrid queries do not evaluate typed predicates; `object_version` is accepted by validation, but the current builder reads the current object metadata snapshot rather than every historical version. |
| Embeddings | Production vector search needs caller-supplied vectors or a configured production embedding provider. The `test_only` provider is disabled unless configured and is not suitable for semantic production search. |
| Specialised kinds | `personaldb_row_metadata` and `git_source` are enum values, not generic materialised `QueryIndex` paths today. Use their service-specific APIs and docs. |

These limits should shape product and operator runbooks. Keep protected corpora on `inherit_object`, split buckets when bucket-level `index:read` is too broad, prefer focused selectors, make malformed records visible through diagnostics, and treat full-text/vector/hybrid freshness as derived lag until the implementation exposes stronger catch-up evidence.

## Query complaint triage

When a user reports a missing search result, check in this order: source object exists, caller can read the object, selector includes the object, extractor succeeds for the object, builder cursor has reached the write, query predicates match, and result authorisation does not filter the hit. This order prevents rebuilding an index when the actual issue is a policy grant or a selector mismatch.

For vector and hybrid indexes, also verify provider configuration, dimension, and whether the query vector length matches the index definition.
