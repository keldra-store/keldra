---
title: Path, Metadata, and Typed Query Indexes
description: Build object-key, metadata, and typed JSON indexes without turning derived data into a second source of truth.
---

# Path, Metadata, and Typed Query Indexes

This tutorial continues from [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) and [Watches](/tutorials/watches/). The metadata page explained where canonical JSON, user metadata, and typed fields belong. The watches page explained why indexes are derived data and why a query can need proof that an index has caught up to a source cursor.

An index is a maintained shortcut over committed Anvil data. It lets an application answer questions such as "which objects are under this prefix?", "which invoices have this metadata label?", and "which open invoices are due before this date?" without scanning every object in a bucket. The object remains the source of truth. The index stores materialised rows that can be rebuilt from committed object metadata, object bodies, and watch cursors.

Applications should call the public Index API directly in production. The `anvil index` commands in this page are manual helpers over the same API fields. Use [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/) for the full JSON grammar, [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) for scope strings, and [Public CLI](/reference/public-cli/) for the CLI command reference. The conceptual background lives in [Indexes and Query](/learn/indexes-and-query/) and [Watches and Derived Data](/learn/watches-and-derived-data/).

This page is the foundation for the later search tutorials. It teaches the four JSON fields every index definition carries, then walks from prefix navigation to metadata equality and typed JSON range queries. Pay attention to selector, extractor, build-policy, and query JSON; those same shapes appear in full-text, vector, hybrid, and append-stream indexes.

## Know the four JSON fields before creating an index

The Index API separates build-time decisions from query-time decisions. Keeping those roles separate prevents two common mistakes: building a huge index because the selector was vague, and expecting a query filter to find rows that were never materialised.

`selector_json` chooses the source records before extraction. For the index kinds in this page it usually contains an object-key `prefix`, and sometimes a `content_type` if your upload path sets content types reliably. A selector is a build-time filter: an object outside the selector is not in the index at all.

`extractor_json` describes extraction for index kinds such as full text and hybrid search. For `path` and `metadata_filter` indexes it is currently `{}`. For `typed_json`, the current implementation puts typed field definitions in `build_policy_json`, so `extractor_json` is also `{}` in the examples below.

`build_policy_json` describes how Anvil materialises the index. For `path` and `metadata_filter`, `{}` is enough today because both are backed by object metadata rows. For `typed_json`, this is where you define field names, JSON Pointer extractors, required fields, source kind, and default ordering.

Query JSON is different. `metadata_filters_json`, `typed_predicates_json`, and `typed_order_json` are sent with `QueryIndex`, not stored in the index definition. They ask questions of rows that have already been built.

Read these names literally when reviewing an index definition. The selector answers "which source records are eligible?" The extractor answers "what part of each eligible record should be read?" The build policy answers "how should that extracted material be stored?" Query JSON answers "what question is this caller asking now?" A query cannot recover data that the selector excluded, and changing query JSON does not change the stored definition.

For example, an invoice object under `accounting/invoices/2026-07/acme-1001.json` may be eligible because the selector prefix is `accounting/invoices/`. A typed field named `due_at` may be extracted from `/due_at` in the JSON body. A later query may ask for `due_at <= 2026-07-31T23:59:59Z`. Those are three different decisions, and production code should keep them visible in code review.

## Prerequisites and current tutorial limits

The local tutorial chain may still be blocked by earlier implementation gaps: region activation may prevent bucket placement, the current public CLI upload helper cannot set `content_type` or `user_metadata_json`, and the previous pages do not automatically grant index scopes. Treat the commands as valid current CLI/API shapes, but do not assume they will run until your local tenant, bucket, region, uploads, index builder, and grants are ready.

The examples use the public `anvil` CLI because index definitions are tenant-owned public-plane resources. Do not use `anvil-admin` to create these indexes. An operator may bootstrap the tenant and initial scopes, but a tenant app with `index:create` owns the index definition and later queries it through the public Index API.

Use narrow grants rather than wildcards. The useful scopes for this page are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Create `documents_path` | `index:create` | `documents/documents_path` |
| Create `documents_by_workflow` | `index:create` | `documents/documents_by_workflow` |
| Create `invoices_by_due` | `index:create` | `documents/invoices_by_due` |
| List, query, or read diagnostics for indexes in `documents` | `index:read` | `documents` |
| See hits from an `inherit_object` index | `object:read` or object-reader relationship | `documents/<object-key>` |

That last line is easy to miss. `index:read` lets the caller ask the index service a question. With the default `inherit_object` authorisation mode, each returned hit must also be visible under object read authorisation. Current list, query, and diagnostics operations use the coarse `index:read` resource `documents`, not `documents/<index-name>`.

## Choose the authorisation mode deliberately

Every index definition has an `authorization_mode`. The default CLI value is `inherit_object`, and it is the safest choice for indexes derived from object content or protected metadata.

With `inherit_object`, Anvil filters query results through object visibility. A caller can see a hit when it has `object:read` for `bucket/key` or when relationship authorisation allows the built-in object reader relation for that object. This protects search and listing results from leaking object names or metadata just because a caller can query an index.

With `index_only` or `public`, the query path does not re-check object read permission for each hit. The public API still requires an authenticated caller with `index:read` on the bucket; this is not the same thing as anonymous public bucket reads. Use these modes only for derived rows whose keys, metadata, and field values are safe for every principal that can query the index.

The commands below set `--authorization-mode inherit_object` explicitly so the tutorial is clear even though it is also the default.

## Prepare source objects intentionally

Indexes are only useful when the source object layout is deliberate. A typed invoice object for this tutorial would have a body like this:

```json
{
  "invoice_id": "acme-1001",
  "customer": {"id": "acme"},
  "state": "open",
  "due_at": "2026-07-31T23:59:59Z",
  "amount": {"cents": 129900}
}
```

The object key might be `accounting/invoices/2026-07/acme-1001.json`. That key makes prefix navigation possible. The JSON body is the canonical business record. Optional user metadata such as `{"workflow_state":"open"}` can make equality filters cheap, but it should not become a second copy of the invoice. If metadata and the body disagree, your application must know which one is authoritative; this tutorial treats the body as authoritative and indexes derived rows from it.

The public CLI upload helper cannot set all metadata-rich fields today, so metadata-filter examples may need objects created by the public API or a client library. That limitation is a CLI limitation, not a reason to move tenant-owned writes to the admin plane.

## Create a path index for prefix navigation

A path index is for object-key navigation. It answers questions such as "what objects are under `tutorial/`?" without downloading object bodies. It is a good fit for folder-like screens, import repair scopes, static-site inventories, and worker queues that use stable key prefixes.

Create a small path index definition over the tutorial prefix:

```bash
anvil --profile acme index create documents documents_path path \
  --selector-json '{"prefix":"tutorial/"}' \
  --extractor-json '{}' \
  --build-policy-json '{}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex` for bucket `documents`, index name `documents_path`, and kind `path`. A successful response proves the caller authenticated, had `index:create` on `documents/documents_path`, the bucket exists, the JSON fields parsed, and Anvil stored an enabled index definition. It also enqueues build work for that index. The response format is compact in the CLI, but application code should keep the index name, version, request id, and any returned revision or generation fields that the API exposes for the release you are using.

It does not prove the index has already materialised rows. An index builder still has to process source object metadata and publish a segment. Until then, a query can fail with `IndexUnavailable` or return no hits if there are no matching, visible objects.

List current index definitions:

```bash
anvil --profile acme index list documents
```

This calls `IndexService.ListIndexes`. A successful response proves the caller has `index:read` on `documents`. The CLI prints compact rows containing bucket, name, kind, enabled state, and version. Listing an index proves the definition exists; it does not prove the latest source objects have been indexed.

Query the path index:

```bash
anvil --profile acme index query documents documents_path \
  --path-prefix tutorial/ \
  --limit 20
```

This calls `IndexService.QueryIndex`. The `path_prefix` narrows the rows that were already selected by `selector_json`; it cannot reach outside the index's build selector. A successful query proves the caller has `index:read` on the bucket, the index is enabled, a materialised metadata-backed segment is available, and every printed hit passed the authorisation mode check.

If the command prints no rows, there are several possible causes: the prefix has no current objects, the index builder has not caught up, the caller lacks object visibility under `inherit_object`, or the selector excluded the objects. Do not treat an empty result as proof that the bucket is empty.

## Add metadata equality filters when labels are enough

A metadata-filter index also uses object metadata rows, but the query must include `metadata_filters_json`. Use this when the question is simple equality over user metadata: for example, "invoice objects whose `customer_id` is `acme` and `workflow_state` is `open`".

The important limitation from the previous tutorial still applies: the current `anvil object put` command cannot attach user metadata. To make the example produce hits, upload objects through the public API or Rust client with `user_metadata_json` set, or use existing objects that already have matching metadata.

Create the index definition:

```bash
anvil --profile acme index create documents documents_by_workflow metadata_filter \
  --selector-json '{"prefix":"accounting/invoices/"}' \
  --extractor-json '{}' \
  --build-policy-json '{}' \
  --authorization-mode inherit_object
```

This stores a `metadata_filter` index definition. The selector limits source objects to the invoice prefix. The extractor and build policy are empty because the current metadata-backed builder materialises top-level user metadata values for matching current objects. If your upload path sets `content_type` to `application/json`, you may add that selector field through the API or CLI JSON, but do not add it unless the source objects really carry that exact content type.

Query with exact metadata equality:

```bash
anvil --profile acme index query documents documents_by_workflow \
  --path-prefix accounting/invoices/ \
  --metadata-filters-json '{"customer_id":"acme","workflow_state":"open"}' \
  --limit 20
```

This query proves the metadata filter JSON is valid and that the query path found materialised rows whose metadata exactly matches both fields. Metadata filters are JSON equality checks. They are not text search, not range comparisons, and not type coercion. The JSON number `20` is different from the string `"20"`.

A `metadata_filter` query without `metadata_filters_json` fails by design. That guard prevents accidental broad metadata scans through an index intended for labelled lookups. If you only need prefix navigation, use a `path` index instead.

## Define typed JSON fields for range and ordered queries

A typed JSON index is for structured values extracted from canonical JSON bodies or append records. It is the right choice when the application needs predicates and ordering, such as "open Acme invoices due by the end of July, ordered by due date and amount".

The selector below chooses invoice keys. The build policy says Anvil should parse current object bodies as JSON and extract named values from JSON Pointers:

```json
{
  "source_kind": "object_current",
  "fields": [
    {"name": "invoice_id", "extractor": "/invoice_id", "required": true},
    {"name": "customer_id", "extractor": "/customer/id", "required": true},
    {"name": "state", "extractor": "/state", "required": true},
    {"name": "due_at", "extractor": "/due_at", "required": true},
    {"name": "amount_cents", "extractor": "/amount/cents", "required": true}
  ],
  "default_order": [
    {"field": "due_at", "direction": "asc"},
    {"field": "amount_cents", "direction": "desc"},
    {"field": "invoice_id", "direction": "asc"}
  ]
}
```

`source_kind` tells the builder which source family to read. `object_current` means the current object body and metadata snapshot. Each field `name` becomes a query field. Each `extractor` is a JSON Pointer into the object body. `required: true` turns a missing or null value into an index diagnostic for that object instead of silently indexing a null. `default_order` is used when the query sends an empty `typed_order_json`.

Typed JSON fields do not declare separate Anvil types. The extracted JSON values are compared as JSON values. Use RFC 3339 UTC timestamp strings, as shown for `due_at`, when lexical string order must match time order.

Create the typed index:

```bash
anvil --profile acme index create documents invoices_by_due typed_json \
  --selector-json '{"prefix":"accounting/invoices/"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"invoice_id","extractor":"/invoice_id","required":true},{"name":"customer_id","extractor":"/customer/id","required":true},{"name":"state","extractor":"/state","required":true},{"name":"due_at","extractor":"/due_at","required":true},{"name":"amount_cents","extractor":"/amount/cents","required":true}],"default_order":[{"field":"due_at","direction":"asc"},{"field":"amount_cents","direction":"desc"},{"field":"invoice_id","direction":"asc"}]}' \
  --authorization-mode inherit_object
```

A successful response proves definition validation passed: the kind is recognised, the build policy contains a `fields` array, every field has a valid extractor, the authorisation mode is valid, and the caller has `index:create` on `documents/invoices_by_due`. It still does not prove every invoice body parses as JSON. Body parse failures and missing required fields appear as diagnostics during build.

## Query typed JSON with valid predicate and order arrays

`typed_predicates_json` must be an array. Every object in the array names a field from the build policy, an operator, and one comparison value or a `values` array. All predicates must match.

This predicate array means: customer is `acme`, state is `open`, and due date is on or before 31 July 2026:

```json
[
  {"field": "customer_id", "op": "eq", "value": "acme"},
  {"field": "state", "op": "eq", "value": "open"},
  {"field": "due_at", "op": "lte", "value": "2026-07-31T23:59:59Z"}
]
```

`typed_order_json` must also be an array. This order sorts by due date, then by amount descending, then by invoice id for a stable business tie-breaker:

```json
[
  {"field": "due_at", "direction": "asc"},
  {"field": "amount_cents", "direction": "desc"},
  {"field": "invoice_id", "direction": "asc"}
]
```

Send both arrays with the current CLI:

```bash
anvil --profile acme index query documents invoices_by_due \
  --path-prefix accounting/invoices/ \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"},{"field":"state","op":"eq","value":"open"},{"field":"due_at","op":"lte","value":"2026-07-31T23:59:59Z"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"amount_cents","direction":"desc"},{"field":"invoice_id","direction":"asc"}]' \
  --limit 20
```

This query proves the typed predicate array and typed order array parse, the typed index segment is materialised, and matching visible rows can be ordered. If it fails with `TypedJsonIndexNotMaterialised`, the definition exists but the builder has not published a typed-field segment. If it fails with `TypedJsonIndexFieldSetMismatch` or `TypedJsonIndexSourceKindMismatch`, the stored segment does not match the current definition and should be rebuilt or repaired.

Do not send the old object-shaped predicate form such as `{"eq":{"state":"open"}}`. Current `typed_predicates_json` expects the array form shown above.

When a predicate uses `value`, the comparison value is a JSON value. Strings, numbers, booleans, arrays, and null keep their JSON type. Do not rely on the server to coerce `"129900"` into `129900` or parse local date formats into timestamps. When a predicate uses `values`, it is for operators that accept a set of alternatives. Keep predicate arrays short and reviewable; if your product is building arbitrary user filters, validate them before forwarding them to Anvil.

## Require catch-up when a query depends on recent writes

If a worker just observed an object watch cursor in the watches tutorial, it can ask the query to prove the typed or metadata-backed index has processed at least that cursor. Suppose `LAST_CURSOR` contains the object watch cursor you saved after processing a relevant invoice write:

```bash
anvil --profile acme index query documents invoices_by_due \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"},{"field":"state","op":"eq","value":"open"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"invoice_id","direction":"asc"}]' \
  --require-caught-up-to-watch-cursor "$LAST_CURSOR" \
  --lag-timeout-ms 5000
```

A successful response proves the index segment used for the answer has caught up to the required source object cursor. If the command fails with `IndexLagging`, the safe response is to retry later, use a deliberately stale-tolerant product path, or inspect builder health. Do not treat `IndexLagging` as an empty result set.

The `lag_timeout_ms` field exists on the API and CLI, but current direct index query implementations return immediately rather than waiting for catch-up. Treat it as a future-facing field and rely on the success or `IndexLagging` result today.

## Inspect diagnostics when rows are missing

Diagnostics are the first place to look when an index definition exists but expected rows are absent.

```bash
anvil --profile acme index diagnostics documents invoices_by_due \
  --severity error \
  --page-size 20
```

This calls `IndexService.ListIndexDiagnostics`. A successful response proves the caller has `index:read` on `documents` and that Anvil could read diagnostic records for the bucket/index filter. The CLI prints `cursor`, `severity`, `code`, and `message`. No output means no matching diagnostics were returned in that page; it does not prove every object was selected or visible to your query.

For typed JSON indexes, common diagnostic causes are invalid JSON bodies, missing required fields, and extractor pointers that do not match the body shape. For path and metadata-filter indexes, focus first on selector mismatch, metadata not being present because the CLI upload path did not set it, and authorisation filtering through `inherit_object`.

## Page results without changing the query shape

The CLI prints `next_page_token=...` when more results are available. Use that token only with the same logical query: same caller, bucket, index, predicates, order, prefix, and authorisation context.

```bash
anvil --profile acme index query documents invoices_by_due \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"invoice_id","direction":"asc"}]' \
  --limit 10
```

If the next-page command changes the predicate or order, Anvil should reject the signed page token rather than silently skipping or duplicating rows. Treat page tokens as opaque short-lived cursors, not as user-editable state.

## What to take forward

Use path indexes for prefix navigation over object keys. Use metadata-filter indexes for exact equality over small user metadata labels. Use typed JSON indexes for structured predicates, ranges, and stable ordering over canonical JSON bodies or append records. Keep `selector_json` focused, keep `extractor_json` empty for these three current examples, put typed field definitions in `build_policy_json`, and send query JSON only when asking a question. Prefer `inherit_object` unless the indexed data is intentionally visible to every caller with bucket-level `index:read`. When recent writes matter, carry the object watch cursor into `require_caught_up_to_watch_cursor` rather than hoping the derived index has caught up.

## Success and failure cues

Index creation proves the definition is syntactically valid and the caller can create that index name; it does not prove every selected object has already materialised into a row. A query that returns fewer rows than expected can come from selector exclusion, missing or mistyped extracted fields, derived lag, diagnostics, or per-hit authorisation. Use diagnostics and watch-cursor catch-up before changing the selector or granting broader read access.

## Where to go next

Use this page as the base before adding richer retrieval. Read [Full-Text Search](/tutorials/full-text-search/) for tokenised text, [Vector Search](/tutorials/vector-search/) for embedding similarity, and [Hybrid Search](/tutorials/hybrid-search/) when a product search endpoint intentionally combines both. For freshness-sensitive derived data, continue to [Watches](/tutorials/watches/).
