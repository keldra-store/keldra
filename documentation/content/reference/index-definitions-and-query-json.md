---
title: Index Definitions and Query JSON
description: Reference for Anvil index definition fields, selector JSON, extractor JSON, build policy JSON, query payloads, catch-up controls, and diagnostics.
---

# Index Definitions and Query JSON

An Anvil index is derived state. The object body, object metadata, append record, or other source record remains the source of truth; an index is a materialised view built from those records so reads can be fast, ordered, filtered, or scored. That distinction matters operationally. If a write has reached the source record but the index builder has not caught up, a direct object read can be fresh while an index query is still behind. If an index is corrupt or stale, repair and rebuild should recreate the derived view from source records rather than editing the index by hand.

This page documents the JSON shapes used by the current public Index API and the tenant-facing `anvil index` CLI. It is a reference, not a tutorial. For narrative walkthroughs, read [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/), [Full-Text Search](/tutorials/full-text-search/), [Vector Search](/tutorials/vector-search/), [Hybrid Search](/tutorials/hybrid-search/), and [Watches](/tutorials/watches/). The conceptual model is in [Indexes and Query](/learn/indexes-and-query/) and [Watches and Derived Data](/learn/watches-and-derived-data/). Public policy actions are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and command names are in [Public CLI](/reference/public-cli/).

The API is primary. The CLI is a manual helper over the same request fields. Production code should prefer the public API or Rust client when it needs typed errors, stable retry handling, richer write metadata, or query-spec composition that is not exposed as a CLI command today.

## The index request model

An index definition answers four questions:

| Question | Definition field | What it controls |
| --- | --- | --- |
| What kind of index is this? | `kind` | The storage format and query path: path, metadata filter, typed JSON, full text, vector, or hybrid. |
| Which source records are eligible? | `selector_json` | Build-time source selection, usually by object key or append stream prefix and content type. |
| What data is extracted? | `extractor_json` and `build_policy_json` | Text fields, typed fields, vector provenance, tokenisation, ANN settings, and related build options. |
| Who may see query hits? | `authorization_mode` | Whether the query path also checks object-level read visibility for each hit. |

The public proto request for index creation has these fields:

| Field | Type | Notes |
| --- | --- | --- |
| `bucket_name` | string | Tenant bucket that owns the index and the source records. |
| `name` | string | Index name within the bucket. It is used in later list, query, update, disable, diagnostics, and drop calls. |
| `kind` | enum | `INDEX_KIND_PATH`, `INDEX_KIND_METADATA_FILTER`, `INDEX_KIND_FULL_TEXT`, `INDEX_KIND_VECTOR`, `INDEX_KIND_HYBRID`, `INDEX_KIND_PERSONALDB_ROW_METADATA`, `INDEX_KIND_GIT_SOURCE`, or `INDEX_KIND_TYPED_JSON`. |
| `selector_json` | JSON string | Parsed as JSON. `{}` and `null` usually mean all records of the source kind. |
| `extractor_json` | JSON string | Parsed as JSON. Used mainly by full text and the text side of hybrid. For vector, the real extractor is in `build_policy_json`. |
| `authorization_mode` | string | The field name is American-spelled in the API. Supported values are `inherit_object`, `index_only`, and `public`. |
| `build_policy_json` | JSON string | Parsed as JSON. Strictly validated for full-text, vector, hybrid, and typed JSON indexes. |

`UpdateIndexRequest` has the same JSON fields and `authorization_mode`, but no `kind`; the kind is fixed after creation. `DisableIndexRequest`, `DropIndexRequest`, `ListIndexesRequest`, `QueryIndexRequest`, and `ListIndexDiagnosticsRequest` then operate on the named definition.

With the CLI, the same fields are passed as JSON strings:

```bash
anvil --profile acme index create documents by_state typed-json \
  --selector-json '{"prefix":"cases/"}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"state","extractor":"/state"}]}'
```

That command proves only that the service accepted the definition and that the caller has the needed public policy scope to create it. It does not prove the index has finished building, that the selected objects contain valid JSON, or that later queries will be caught up. Use index watches, query catch-up fields, and diagnostics for those questions.

With the Rust/generated API, the same shape is a proto request. The exact client module path depends on how your application imports the generated Anvil API, but the request fields are the same:

```rust
let request = anvil_api::CreateIndexRequest {
    bucket_name: "documents".to_string(),
    name: "by_state".to_string(),
    kind: anvil_api::IndexKind::TypedJson as i32,
    selector_json: r#"{"prefix":"cases/"}"#.to_string(),
    extractor_json: "{}".to_string(),
    authorization_mode: "inherit_object".to_string(),
    build_policy_json: r#"{
      "source_kind":"object_current",
      "fields":[{"name":"state","extractor":"/state"}]
    }"#.to_string(),
};
```

## Index kinds

The kind selects both the builder and the query path. It is not just a label.

| CLI kind | Stored kind | Use it for | Current behaviour and limitations |
| --- | --- | --- | --- |
| `path` | `path` | Fast prefix-style object discovery over current object metadata. | Query with `path_prefix`; optional metadata equality filters can narrow results. Scores are constant. |
| `metadata`, `metadata-filter`, `metadata_filter` | `metadata_filter` | Object metadata equality filters. | Uses the same current-object metadata segment as `path`. The service rejects an empty metadata-filter string, but `{}` is currently accepted, so supply at least one filter for the intended semantics. |
| `typed-json`, `typed_json` | `typed_json` | Equality, range, existence, and ordered queries over named JSON values from object bodies or append records. | No relevance scoring. Direct typed queries do not use `metadata_filters_json`. Current field types are inferred from JSON values, not declared in the definition. |
| `full-text`, `full_text`, `fulltext` | `full_text` | Token-based text search over extracted object text. | `query_text` is a plain text string, not a boolean query language. Phrase queries require positions in the build policy. |
| `vector` | `vector` | Nearest-neighbour search over extracted or provider-generated vectors. | `query_vector` must match the configured dimension. Text embedding through a provider requires server configuration, except the deterministic `test_only` provider when explicitly enabled. |
| `hybrid` | `hybrid` | Combining full-text and vector signals in one index. | Accepts text, vector, or both. Weights are currently fixed by the service, not caller-configurable. |
| `personaldb-row-metadata`, `personaldb_row_metadata` | `personaldb_row_metadata` | Reserved PersonalDB row-metadata index kind. | Accepted by the generic definition API, but not materialised or queried by the generic index builder path today. Use PersonalDB-specific surfaces where available. |
| `git-source`, `git_source` | `git_source` | Reserved source repository indexing concept. | Accepted by the generic definition API, but not materialised or queried by the generic index builder path today. |

Create, update, disable, and drop operations check public policy scopes on the `bucket/index_name` resource. List, query, and diagnostics currently use `index:read` on the bucket name. That is a coarse read scope: it is not yet per index definition. Watch commands use `index:watch` on the bucket. See [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) for the exact action/resource strings.

## Authorisation modes

`authorization_mode` controls visibility after an index has found a candidate hit.

| Mode | Result visibility | Use when |
| --- | --- | --- |
| `inherit_object` | The query path also checks whether the caller can read the underlying object, either by public policy scope or relationship authorisation. | This is the safest default for indexes derived from private object data. |
| `index_only` | The caller needs authority to query the index, but each hit is not rechecked against object read authority. | The indexed fields are a deliberately separate derived view with its own access boundary. |
| `public` | Same current query-path shape as `index_only`: the hit is not rechecked against object read authority. | The indexed data is intended to be visible to principals with index read authority. This does not make the index anonymously queryable by itself. |

Do not use `index_only` or `public` to hide sensitive fields from object readers by convention. The index segment contains derived data from source records, and every principal with `index:read` on the bucket can query it under those modes.

For `inherit_object`, broad prefix read grants can make query-time permission filtering expensive. The current implementation caps the number of prefix-expanded object permissions it will use for query filtering; very broad grants can fail with `AuthzPermissionSetTooLargeForPrefixScope` rather than returning a misleading partial answer.

## `selector_json`

`selector_json` is build-time selection. It decides which source records enter the index segment. A later query can only narrow the built segment; it cannot find records that the selector excluded.

For object-backed indexes, the current selector supports:

| Key | Type | Behaviour |
| --- | --- | --- |
| `prefix` | string | Object key must start with this prefix. |
| `content_type` | string | Object content type must exactly match this value. |

For append-backed typed JSON indexes, the same keys apply to append stream records:

| Key | Type | Behaviour |
| --- | --- | --- |
| `prefix` | string | Append stream key must start with this prefix. |
| `content_type` | string | Append record content type must exactly match this value. |

Example:

```json
{
  "prefix": "cases/2026/",
  "content_type": "application/json"
}
```

`{}` and `null` select every current record for the relevant source kind. Unknown keys are ignored by the current implementation. Non-object JSON is accepted and behaves like no selector, so use an object shape in new definitions.

## JSON Pointer in index definitions

Several extractors use JSON Pointer strings. A pointer begins with `/` and walks a JSON object or array. For example, `/customer/id` reads `body.customer.id`, and `/items/0/title` reads the `title` field of the first array entry. Literal `~` and `/` in JSON member names must be escaped as `~0` and `~1` according to JSON Pointer rules.

Pointers are evaluated against the selected payload or metadata object for that extractor. A missing pointer usually yields JSON `null` for typed JSON fields, or a build diagnostic for text/vector extraction when the extractor requires a value.

## Path and metadata-filter definitions

`path` and `metadata_filter` indexes are backed by a typed-field segment created from current, non-deleted object metadata. Each row includes the object key, current object version id, source identity, authz label, authz revision, creation information, and top-level user metadata values.

A typical path index uses selector JSON to bound the build and empty objects for extractor and build policy:

```json
{}
```

Use that value for both `extractor_json` and `build_policy_json`. The meaningful part is usually:

```json
{
  "prefix": "documents/"
}
```

A path query can then pass `path_prefix` to narrow results within the already built prefix. A metadata-filter query passes `metadata_filters_json` to require exact metadata equality. Because both kinds share the same metadata-backed segment, changing from a path query to a metadata-filter query changes query validation and intent more than it changes the underlying storage format.

## Full-text definitions

A full-text index has two inputs:

- `build_policy_json` describes tokenisation and stored scoring data.
- `extractor_json` describes which text fields Anvil extracts from each selected object.

### Full-text build policy

`build_policy_json` for `full_text` must be a JSON object. Supported keys are:

| Key | Type | Default | Behaviour |
| --- | --- | --- | --- |
| `positions` | boolean | `true` | Stores term positions. Phrase queries require this to be true. |
| `language` | string | `simple` | Stored language label. It must not be empty. |
| `max_token_chars` | integer | `128` | Tokens longer than this many characters are skipped. Valid range is 1 to 128. |
| `lowercase` | boolean | `true` | Applies case folding before tokenisation. |
| `normalize_nfkc` | boolean | `true` | Applies NFKC normalisation before tokenisation. |
| `record_original_ranges` | boolean | `true` | Records original byte ranges in token metadata. |
| `stop_words_enabled` | boolean | `false` | Stored in the definition. The current tokenizer path does not expose a full configurable stop-word list here. |
| `stemming` | string | omitted | Optional stored stemming label. The value must not be empty if present. |
| `require_index_success` | boolean | `false` | Stored build requirement flag. Current extraction failures are reported as diagnostics. |

The tokenizer splits text into Unicode word-like segments and indexes segments containing at least one alphanumeric character. The `query_text` string is tokenised with the same tokenizer settings. It is not a documented boolean expression language; do not rely on operators such as `AND`, `OR`, `NOT`, field prefixes, or parentheses unless a future API explicitly documents them.

Example build policy:

```json
{
  "positions": true,
  "language": "simple",
  "max_token_chars": 128,
  "lowercase": true,
  "normalize_nfkc": true
}
```

### Full-text extractor JSON

If `extractor_json` is `{}` or does not name a source, the builder decodes the whole object body as UTF-8 and indexes it as one field. You can also set one extractor:

```json
{
  "source": "json_pointer",
  "pointer": "/summary"
}
```

Or multiple text fields:

```json
{
  "fields": [
    {"source": "object_key"},
    {"source": "metadata_field", "field": "owner"},
    {"source": "json_pointer", "json_pointer": "/body"}
  ]
}
```

Supported text sources are:

| Source | Extra keys | Behaviour |
| --- | --- | --- |
| `object_body_utf8`, `utf8`, `body`, `git_blob_text` | none | Decode the object payload as UTF-8. |
| `object_key`, `key` | none | Index the object key. |
| `content_type` | none | Index the object's content type if present. |
| `json_pointer` | `json_pointer`, `pointer`, or `path` | Decode the payload as JSON and read the pointer. Strings, numbers, and booleans become text; arrays and objects are stringified JSON. |
| `metadata_field` | `field`, `metadata_field`, `key`, or `path` | Read object user metadata. A value beginning with `/` is treated as a JSON Pointer into the metadata object; otherwise it is a top-level metadata key. |
| `media_transcript` | none | Runs the current media extraction path and indexes the text transcript output. The object must have a content type. |
| `personaldb_table_column` | `column`, `column_name`, or `field`; optional `table` or `table_name` | Decode the payload as JSON and read a PersonalDB row-style column from common shapes such as `columns`, `row`, `new_values`, `values`, or top-level fields. |

Extraction problems are recorded as index diagnostics against the object version. They do not prove the whole index is unusable; inspect diagnostics to see whether the issue is one bad object, an unsupported extractor, or a broader build problem.

## Typed JSON definitions

A `typed_json` index stores named JSON values in a typed-field segment. It is for predicates and ordering, not for text relevance. A field has a name and an extractor; it does not have a declared type in the definition today. The field type at query time is the JSON type of each extracted value: string, number, boolean, null, array, or object. Stable query behaviour is easiest when every object uses the same JSON type for a field.

Fields may be supplied in `build_policy_json.fields`; the builder also accepts `extractor_json.fields`, but new definitions should keep typed JSON fields in `build_policy_json` so one policy object describes the source and fields together.

Example object-current definition:

```json
{
  "source_kind": "object_current",
  "fields": [
    {"name": "state", "extractor": "/state", "required": true},
    {"name": "priority", "extractor": "/priority"},
    {"name": "owner", "extractor": "object_user_metadata_json_pointer:/owner"}
  ],
  "default_order": [
    {"field": "priority", "direction": "desc"},
    {"field": "state", "direction": "asc"}
  ]
}
```

Supported top-level keys are:

| Key | Type | Default | Behaviour |
| --- | --- | --- | --- |
| `source_kind` or `source` | string | `object_current` | Source family: `object_current`, `object_version`, or `append_record`. |
| `fields` | array | required | Field definitions used by predicates and ordering. |
| `default_order` | array | `[]` | Used when the API sends an empty `typed_order_json` string. The current CLI default sends `[]`, so it bypasses this default unless you deliberately pass an empty value through a surface that permits it. |

Each field entry supports:

| Key | Type | Default | Behaviour |
| --- | --- | --- | --- |
| `name` | string | required | Field name used by `typed_predicates_json`, `typed_order_json`, and query specs. |
| `extractor` or `json_pointer` | string | required | Extractor string, described below. |
| `required` | boolean | `false` | If extraction yields JSON `null`, the row is rejected and a diagnostic is recorded. |

For `object_current` and `object_version`, supported extractors are:

| Extractor | Behaviour |
| --- | --- |
| `created_at` | Object creation timestamp as an RFC 3339 string. |
| `object_key` | Object key. |
| `object_content_type` | Object content type as a string, or JSON `null`. |
| `/path` | JSON Pointer into the object body. |
| `object_body_json_pointer:/path` | Explicit JSON Pointer into the object body. |
| `object_user_metadata_json_pointer:/path` | JSON Pointer into object user metadata. |

For `append_record`, supported extractors are:

| Extractor | Behaviour |
| --- | --- |
| `created_at` | Append record creation timestamp as an RFC 3339 string. |
| `append_stream_key` | Append stream key. |
| `append_record_sequence` | Append record sequence as a JSON number. |
| `append_content_type` | Append record content type as a string, or JSON `null`. |
| `/path` | JSON Pointer into the append payload. |
| `append_payload_json_pointer:/path` | Explicit JSON Pointer into the append payload. |
| `append_user_metadata_json_pointer:/path` | JSON Pointer into append record user metadata. |

`object_version` is accepted by validation, but the current object-backed builder reads the current object metadata stream and payload snapshot rather than materialising every historical object version. Treat `object_version` as a reserved or implementation-dependent source kind until you have verified the exact behaviour you need.

## Vector definitions

A vector index stores one or more floating-point vectors per selected object. A vector is an ordered list of `f32` values with a fixed dimension. Both stored vectors and query vectors must have exactly that dimension. The metric controls how scores are calculated: cosine similarity, dot product, or negative L2 distance.

For `vector` indexes, `extractor_json` must be `{}` or `null`. The actual vector extractor lives inside `build_policy_json.extractor`. Current validation rejects a separate external vector extractor for a pure vector index.

A caller-supplied-vector definition looks like this:

```json
{
  "schema": "anvil.index.vector_definition.v1",
  "source": {"kind": "object_current"},
  "extractor": {
    "kind": "object_body_json_vector",
    "json_pointer": "/embedding"
  },
  "embedding": {
    "provider": "caller_supplied",
    "model": "acme-embedding-v1",
    "dimension": 3,
    "modality": "text",
    "normalisation": "unit_l2",
    "chunking": {"strategy": "whole_object"}
  },
  "ann": {
    "algorithm": "hnsw",
    "metric": "cosine"
  }
}
```

Supported top-level keys are:

| Key | Type | Behaviour |
| --- | --- | --- |
| `schema` | string | Must be `anvil.index.vector_definition.v1`. |
| `source` | object | Required provenance object. Current source filtering still uses `selector_json`. |
| `extractor` | object | Required and must include a string `kind`. It decides how vectors are extracted or produced. |
| `embedding` | object | Required provider/model/dimension/modality provenance and chunking metadata. |
| `ann` | object | Required approximate-nearest-neighbour settings. Current algorithm is HNSW. |

`embedding` supports:

| Key | Type | Behaviour |
| --- | --- | --- |
| `provider` | string | Required non-empty provider or provenance name. For text embedding, the provider must be configured unless it is the server-enabled `test_only` provider. For payloads that already contain vectors, names such as `caller_supplied` are provenance labels. |
| `model` | string | Required non-empty model name. |
| `model_version` | string | Optional. If a configured embedding provider returns a different version, extraction records a diagnostic. |
| `dimension` | integer | Required non-zero `u16`. Stored and query vectors must match this length. |
| `modality` | string | `text`, `image`, `audio`, or `video`. |
| `normalisation` | string | Required non-empty provenance string. The string records how vectors are expected to be prepared; the current scorer still calculates cosine normalisation at scoring time for cosine metric. |
| `chunking` | object | Required provenance object, for example `{"strategy":"whole_object"}`. |

`ann` supports:

| Key | Type | Default | Behaviour |
| --- | --- | --- | --- |
| `algorithm` | string | required | Must be `hnsw`. |
| `metric` | string | required | `cosine`, `dot`, or `l2`. |
| `m` | integer | `32` | HNSW graph degree. Must be non-zero if supplied. |
| `ef_construction` | integer | `200` | HNSW construction breadth. Must be non-zero if supplied. |
| `ef_search_default` | integer | `80` | Default search breadth. Must be non-zero if supplied. |

Supported vector extractor kinds are:

| Extractor kind | Payload shape |
| --- | --- |
| `object_body_json_vector`, `object_body_json`, `json_vector` | Object body is JSON. The selected value can be an array of numbers, an object with `vector`, `values`, or `embedding`, an array of vector arrays, an object with a `vectors` array, or an array of vector objects. Optional per-vector fields are `chunk_id`, `source_start`, and `source_len`. The extractor may include `json_pointer`, `vector_pointer`, or `pointer` to select a subvalue first. |
| `object_body_f32_le`, `f32_le` | Object body is raw little-endian `f32` values. Payload byte length must be divisible by four, and the resulting vector length must equal `embedding.dimension`. |
| `object_body_utf8`, `utf8`, `body` | Object body is sent to a configured embedding provider as text. `test_only` produces deterministic vectors for development/test only and is disabled unless the server explicitly allows it. |

Production text embeddings require a configured provider. The current provider registry supports `command_json` providers configured on the server. Provider-generated vectors are an operator concern as well as an application concern: if the provider is missing, fails, times out, or returns the wrong dimension/model version, the build records diagnostics and no vector is indexed for that object.

## Hybrid definitions

A hybrid index builds a full-text segment and a vector segment under one index definition. Use it when users may express intent both as words and as an embedding vector, and you want one query result list rather than two separate result sets.

`build_policy_json` must contain both `full_text` and `vector`:

```json
{
  "full_text": {
    "positions": true,
    "language": "simple"
  },
  "vector": {
    "schema": "anvil.index.vector_definition.v1",
    "source": {"kind": "object_current"},
    "extractor": {"kind": "object_body_json_vector", "json_pointer": "/embedding"},
    "embedding": {
      "provider": "caller_supplied",
      "model": "acme-embedding-v1",
      "dimension": 3,
      "modality": "text",
      "normalisation": "unit_l2",
      "chunking": {"strategy": "whole_object"}
    },
    "ann": {"algorithm": "hnsw", "metric": "cosine"}
  }
}
```

`extractor_json` describes the full-text side. If it contains a `text` object, that object is used as the text extractor; otherwise the whole `extractor_json` value is treated as the text extractor:

```json
{
  "text": {
    "source": "json_pointer",
    "pointer": "/body"
  }
}
```

The vector side uses `build_policy_json.vector.extractor`. The current hybrid builder calculates a `vector` extractor value from `extractor_json`, but the vector build path uses the extractor embedded in the vector policy. Put required vector configuration in `build_policy_json.vector`, not in `extractor_json.vector`.

A hybrid query accepts `query_text`, `query_vector`, or both. When both are supplied, current scoring normalises the text and vector scores and applies fixed weights: 0.55 text, 0.35 vector, and 0.10 freshness. With only text, text receives all scoring weight. With only a vector, vector receives all scoring weight. These weights are reported in `scoring_recipe_json`; they are not caller-configurable today.

## Direct query fields

`QueryIndexRequest` is the direct query API behind `anvil index query`. It combines ordinary typed fields with JSON-string fields:

| Field or CLI flag | Applies to | Behaviour |
| --- | --- | --- |
| `bucket_name`, `index_name` | all | Selects the index. |
| `query_text` / `--text` | full-text, hybrid | Plain text query string. Required for full text; optional for hybrid if a vector is supplied. Invalid for path, metadata-filter, typed JSON, and vector. |
| `query_vector` / `--vector` | vector, hybrid | Inline float vector. CLI format is comma-delimited, for example `--vector 0.2,0.1,0.9`. Required for vector; optional for hybrid if text is supplied. |
| `limit` / `--limit` | all | Service default for zero is 10; the service caps at 1000. The CLI default is 20. |
| `phrase` / `--phrase` | full-text, hybrid text side | Treats tokenised `query_text` as a phrase. Requires positions to be enabled in the full-text policy. |
| `path_prefix` / `--path-prefix` | path, metadata-filter, typed JSON, full-text, vector, hybrid | Narrows hits by object key or typed row object key after build selection. |
| `metadata_filters_json` / `--metadata-filters-json` | path, metadata-filter, full-text, vector, hybrid | Exact equality filters against object user metadata. Ignored by direct typed JSON queries. |
| `typed_predicates_json` / `--typed-predicates-json` | typed JSON | Predicate array for direct typed JSON queries. Direct full-text, vector, and hybrid queries do not evaluate it. |
| `typed_order_json` / `--typed-order-json` | typed JSON | Ordering array for direct typed JSON queries. Score-based indexes ignore it. |
| `page_token` / `--page-token` | all | Opaque token from the previous response. |
| `require_caught_up_to_watch_cursor` / `--require-caught-up-to-watch-cursor` | metadata-backed, typed JSON | Decimal cursor string. Returns `IndexLagging` if the source or segment is behind. |
| `lag_timeout_ms` / `--lag-timeout-ms` | API field, CLI flag | Present today, but current direct query paths return immediately rather than waiting for catch-up. |

### `metadata_filters_json`

`metadata_filters_json` is a JSON object. Each entry is an exact-equality condition against object user metadata, and all entries must match.

```json
{
  "tenant": "acme",
  "/workflow/state": "open",
  "priority": 20
}
```

Keys beginning with `/` are JSON Pointers into the metadata object. Other keys are top-level metadata keys. Expected values may be any JSON value; matching uses exact JSON equality.

This field applies to metadata-backed queries and to score-based full-text, vector, and hybrid queries after candidates have been found. It does not apply to direct typed JSON queries. For typed JSON, index the metadata value as a typed field and query it with `typed_predicates_json`.

Current limitation: `metadata_filter` indexes are intended to be queried with metadata filters, but the service currently rejects only an empty string. `{}` is valid JSON and matches every row. Treat an empty object on a metadata-filter index as an implementation gap, not a recommended pattern.

### `typed_predicates_json`

`typed_predicates_json` must be an array for typed JSON queries:

```json
[
  {"field": "state", "op": "eq", "value": "open"},
  {"field": "priority", "op": "gte", "value": 10},
  {"field": "owner", "op": "in", "values": ["alice", "bob"]},
  {"field": "closed_at", "op": "is_null"}
]
```

Supported keys are:

| Key | Type | Behaviour |
| --- | --- | --- |
| `field` or `field_name` | string | Name from the typed JSON index definition. |
| `op` or `operator` | string | Predicate operator. |
| `value` | any JSON value | Single comparison value. |
| `values` | array | Multiple comparison values, mainly for `in`. |

Supported operators are:

| Operator spellings | Behaviour |
| --- | --- |
| `eq`, `=`, `==` | Actual value equals the first supplied value. |
| `in` | Actual value equals any supplied value. |
| `lt`, `<` | Actual value is less than the first supplied value. |
| `lte`, `<=` | Actual value is less than or equal to the first supplied value. |
| `gt`, `>` | Actual value is greater than the first supplied value. |
| `gte`, `>=` | Actual value is greater than or equal to the first supplied value. |
| `exists` | Field is present and not JSON `null`. |
| `is_null` | Field is missing or JSON `null`. |

Unknown operators are parsed but match no rows. Comparison uses JSON types: numbers compare as numbers, strings lexicographically, booleans as booleans, and `null` lower than non-null. Mixed JSON types fall back to comparing their JSON string representation, so avoid changing a field between number and string across objects.

Current CLI caveat: `anvil index query` defaults `--typed-predicates-json` to `{}`. That default is harmless for non-typed direct queries because the field is ignored there, but it is invalid for a `typed_json` direct query. Pass `--typed-predicates-json '[]'` explicitly if you want no typed predicates.

### `typed_order_json`

`typed_order_json` is an array of order terms:

```json
[
  {"field": "priority", "direction": "desc"},
  {"field": "state", "direction": "asc"},
  {"field": "created_at"}
]
```

Each term supports `field` or `field_name`, plus optional `direction`. Direction must be `asc` or `desc`; it defaults to `asc`. Ties are broken by row source identity so pagination remains stable.

If the API sends an empty string for `typed_order_json`, the service uses the index definition's `default_order`. If it sends `[]`, the query has no explicit order terms and rows are ordered by source identity. The current CLI default is `[]`, so API callers have slightly richer access to `default_order` behaviour than the CLI helper.

## QuerySpec JSON

`QuerySpecRequest` is an API-only composition surface today; there is no current `anvil index query-spec` CLI command. It lets a caller describe intent once and lets the service choose a suitable index or compose a score-based index with a typed JSON filter index.

The top-level request fields are:

| Field | Type | Behaviour |
| --- | --- | --- |
| `query_spec_json` | JSON string | Must parse to the schema described below. |
| `page_token` | string | Opaque token from a previous QuerySpec response. |
| `lag_timeout_ms` | integer | Passed through to selected index queries; current direct paths do not wait. |
| `accept_degraded` | boolean | If true, the planner may use a path index when there is no bounded primitive predicate. |

A QuerySpec JSON document has this shape:

```json
{
  "schema": "anvil.query.spec.v1",
  "scope": {
    "bucket_name": "documents",
    "anvil_storage_tenant_id": "123",
    "mesh_id": "local-mesh",
    "authz_scope": {"realm_id": "default"}
  },
  "source_kind": "object_current",
  "where": {
    "all": [
      {"path_prefix": "cases/"},
      {"field": "state", "op": "eq", "value": "open"},
      {"full_text": {"query": "quarterly report", "phrase": false}},
      {"can": {"relation": "read"}}
    ]
  },
  "order_by": [
    {"field": "priority", "direction": "desc"}
  ],
  "limit": 20,
  "consistency": {
    "min_source_cursor": "42",
    "min_authz_revision": "12",
    "allow_stale_index": false
  }
}
```

Supported fields are:

| Field | Type | Behaviour |
| --- | --- | --- |
| `schema` | string | Must be `anvil.query.spec.v1`. |
| `scope.bucket_name` | string | Required. The caller also needs `index:read` on this bucket. |
| `scope.anvil_storage_tenant_id` | string | Optional. If present and non-empty, it must match the authenticated tenant id. |
| `scope.mesh_id` | string | Optional. If present, it must not be empty. |
| `scope.authz_scope` | object | Optional shape carried in the spec; it must be an object if present. |
| `source_kind` | string | Defaults to `object_current`. Used when selecting a typed JSON index that covers field predicates/order. |
| `where.all` | array | Conjunction of supported predicates. Every item must be understood by the current parser. |
| `order_by` | array | Typed field order terms with `field` and optional `direction`. Direction must be `asc` or `desc`. |
| `limit` | integer | Defaults to 100 if omitted. Direct query limits are still capped by the selected query path. |
| `consistency.min_source_cursor` | string or integer | Becomes `require_caught_up_to_watch_cursor` for the selected query. |
| `consistency.min_authz_revision` | string or integer | Fails with `AuthzRevisionLagging` if the latest authz revision is lower. |
| `consistency.allow_stale_index` | boolean | If exactly `false`, a response whose selected index reports `is_caught_up=false` fails with `IndexLagging`. |

Supported `where.all` predicate objects are:

| Predicate shape | Behaviour |
| --- | --- |
| `{"path_prefix":"cases/"}` | Narrows by source object key or typed row object key. |
| `{"field":"state","op":"eq","value":"open"}` | Adds a typed JSON predicate. `values` may be used instead of `value`; `in` with an array expands to multiple values. |
| `{"full_text":{"query":"quarterly report","phrase":true}}` | Adds a full-text query. |
| `{"vector":{"near":[0.1,0.2,0.3]}}` | Adds an inline numeric query vector. |
| `{"can":{"relation":"read"}}` | States the expected object authorisation relation for protected results. Current QuerySpec planning supports only `read`. |

The planner currently chooses indexes as follows:

| Intent | Selected index shape |
| --- | --- |
| Text and vector | Requires a `hybrid` index. |
| Typed predicates or order only | Uses a covering `typed_json` index. |
| Text only | Uses a `full_text` index; may compose with a covering `typed_json` filter. |
| Vector only | Uses a `vector` index; may compose with a covering `typed_json` filter. |
| Path prefix only | Uses a `path` or `typed_json` path-capable index. |
| No bounded primitive predicate | Fails unless `accept_degraded` is true, in which case it can use a `path` index. |

If the selected primary index uses `inherit_object`, QuerySpec requires an explicit `can` predicate. This prevents a protected-resource query from accidentally omitting the authorisation intent. QuerySpec responses include `result`, `canonical_query_hash`, `plan_json`, and planner `diagnostics` strings.

## Pagination, ordering, and freshness

Every direct query returns `QueryIndexResponse`:

| Field | Behaviour |
| --- | --- |
| `hits` | Ordered result hits. Score-based indexes order by score descending with stable tie-breakers. Metadata-backed indexes order by object key and source identity. Typed JSON indexes order by `typed_order_json` or source identity. |
| `index_kind` | Kind used to answer the query. |
| `index_generation` | Generation of the materialised segment used. Hybrid reports the maximum of text/vector generations. |
| `authz_revision` | Authorisation revision considered by the query path. |
| `scoring_recipe_json` | JSON string describing the current scoring recipe, such as BM25, vector metric, constant score, or hybrid weights. |
| `next_page_token` | Opaque token for the next page, if more rows are available. |
| `source_watch_cursor_high` | Latest known source cursor for freshness comparison where supported. |
| `index_watch_cursor_applied` | Source cursor materialised by the selected index segment where supported. |
| `is_caught_up` | Whether the selected segment has caught up to the latest known source cursor where supported. |
| `lag_record_count_hint` | Best-effort source-cursor gap where supported. |

Each hit has:

| Field | Behaviour |
| --- | --- |
| `kind` | Index kind that produced the hit. |
| `score` | Relevance, vector, hybrid, or constant score depending on index kind. |
| `object_key` | Source object key or append stream key for append-backed typed rows. |
| `object_version_id` | Object version id for object-backed hits. Append-backed typed rows currently use the append record sequence string in this field. |
| `document_id`, `field_id` | Full-text document/field identifiers where relevant. |
| `vector_id`, `chunk_id`, `source_start`, `source_len` | Vector/chunk source information where relevant. |
| `metadata_json` | JSON string with kind-specific metadata, such as matched terms, typed values, user metadata, vector metric, or hybrid component scores. |

`page_token` is signed and bound to the caller, tenant, bucket, index name, index generation, definition version, authorisation revision, query hash, predicate hash, and order hash. It expires after 15 minutes. Reusing a token with a different caller, query text, vector, filter, order, index definition, or generation fails rather than returning a mismatched page.

`require_caught_up_to_watch_cursor` is a decimal cursor string. Metadata-backed and typed JSON query paths compare it to both the latest known source cursor and the materialised segment cursor. If either side is behind, the query fails with `IndexLagging`. This is the practical API for read-after-derived-read correctness when a caller has stored a source cursor from a write, watch, or previous response.

Current freshness limitations:

- Metadata-backed and typed JSON direct queries report meaningful source and applied cursors.
- Full-text, vector, and hybrid direct queries currently return `is_caught_up=true` with zero cursor fields, so `require_caught_up_to_watch_cursor` is not a full freshness fence for those query paths yet.
- `lag_timeout_ms` is accepted by the API and CLI, and QuerySpec passes it through, but current direct query implementations return immediately rather than waiting for an index to catch up.

## Diagnostics JSON and fields

Index diagnostics are build-time evidence. They tell you which source record could not be extracted, indexed, or validated. They do not prove that every query result is correct, and an empty diagnostics page is not a formal proof that the index is complete; use catch-up cursors and watch state for freshness.

`ListIndexDiagnosticsRequest` has:

| Field or CLI flag | Behaviour |
| --- | --- |
| `bucket_name`, `index_name` | Selects diagnostics for one index. |
| `page.page_token` / `--page-token` | Continues from the opaque token returned by the preceding page. The token is bound to the same caller and diagnostic filter. |
| `page.page_size` / `--page-size` | Maximum diagnostics to return. CLI default is 100. |
| `severity` / `--severity` | Optional filter. Supported values are `info`, `warning`, and `error`. |

Each diagnostic record has:

| Field | Behaviour |
| --- | --- |
| `cursor` | Monotonic diagnostic cursor for pagination. |
| `bucket_name`, `index_name` | Index identity. |
| `object_key` | Source object key or append stream key. |
| `version_id` | Source object version id where available. |
| `severity` | `info`, `warning`, or `error`. |
| `code` | Machine-readable diagnostic code, such as `TextPayloadNotUtf8`, `VectorDimensionMismatch`, or `TypedJsonRowExtractionFailed`. |
| `message` | Human-readable explanation. |
| `details_json` | JSON string with extractor-specific details. |
| `created_at` | Timestamp. |

CLI example:

```bash
anvil --profile acme index diagnostics documents by_state --severity error --page-size 50
```

That command proves the caller can read index diagnostics and shows recorded failures up to the returned page. It does not rebuild the index, wait for a builder, or prove absence of future diagnostics. Use it alongside index watches and repair/diagnostic workflows described in [Index Operations](/operators/index-operations/) and [Repair and Diagnostics](/operators/repair-and-diagnostics/).

## Practical CLI examples

Create a full-text index over JSON document bodies:

```bash
anvil --profile acme index create documents docs_text full-text \
  --selector-json '{"prefix":"docs/","content_type":"application/json"}' \
  --extractor-json '{"source":"json_pointer","pointer":"/body"}' \
  --build-policy-json '{"positions":true,"language":"simple","max_token_chars":128}'
```

This proves the definition shape is accepted. It does not prove that every `docs/` object has a `/body` field or that the full-text segment is already built.

Query it:

```bash
anvil --profile acme index query documents docs_text \
  --text "quarterly report" \
  --path-prefix docs/ \
  --metadata-filters-json '{"visibility":"internal"}' \
  --limit 20
```

This proves the query path can read the current materialised segment and apply object visibility and metadata filters. It does not prove there are no newer source writes waiting behind the index unless the response freshness fields are meaningful for that index kind.

Create a typed JSON index for a queue-like object corpus:

```bash
anvil --profile acme index create documents queue_by_due typed-json \
  --selector-json '{"prefix":"queue/","content_type":"application/json"}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"state","extractor":"/state","required":true},{"name":"due_at","extractor":"/due_at"},{"name":"priority","extractor":"/priority"}],"default_order":[{"field":"due_at","direction":"asc"}]}'
```

Query it with explicit predicate and order JSON:

```bash
anvil --profile acme index query documents queue_by_due \
  --typed-predicates-json '[{"field":"state","op":"eq","value":"ready"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"priority","direction":"desc"}]' \
  --limit 20
```

This proves predicate and order JSON are valid and that the typed segment can answer the request. It does not claim exclusive task ownership; use task leases and fenced mutations for that kind of correctness.

Create a caller-supplied vector index:

```bash
anvil --profile acme index create documents embedding_v1 vector \
  --selector-json '{"prefix":"docs/","content_type":"application/json"}' \
  --build-policy-json '{"schema":"anvil.index.vector_definition.v1","source":{"kind":"object_current"},"extractor":{"kind":"object_body_json_vector","json_pointer":"/embedding"},"embedding":{"provider":"caller_supplied","model":"acme-embedding-v1","dimension":3,"modality":"text","normalisation":"unit_l2","chunking":{"strategy":"whole_object"}},"ann":{"algorithm":"hnsw","metric":"cosine"}}'
```

Query it:

```bash
anvil --profile acme index query documents embedding_v1 --vector 0.1,0.2,0.3 --limit 10
```

This proves the query vector has the right dimension and that a vector segment is available. It does not prove the embedding model is semantically useful; for caller-supplied vectors, application code is responsible for generating production-quality embeddings.

## Current public surfaces and gaps

These current edges matter when you decide whether to use the CLI helper, the direct API, or an API-only composition path:

- `index:read` is currently bucket-level for list, query, and diagnostics. It is not per index definition yet.
- Direct full-text, vector, and hybrid queries do not evaluate `typed_predicates_json`; use QuerySpec through the API to compose score-based search with typed JSON filters.
- QuerySpec is API-only in the current public surface; the CLI exposes `index query`, not a query-spec command.
- Full-text `query_text` is plain text tokenised by Anvil. Boolean query syntax is not implemented as a documented language today.
- Full-text, vector, and hybrid freshness reporting is coarser than metadata-backed and typed JSON reporting. Treat catch-up fences on score-based indexes as limited until the service reports source/applied cursors for those paths.
- Vector text embedding is not production quality through `test_only`; that provider is deterministic development/test plumbing and is disabled unless configured. Production embeddings require configured providers or caller-supplied vectors.
- `metadata_filter` queries can currently be run with `{}` because the service checks for an empty string rather than a non-empty object. Use real filters and treat all-row metadata-filter queries as a gap.
- The current CLI defaults `--typed-predicates-json` to `{}`, which is invalid for direct typed JSON queries. Pass a valid array such as `[]` or a predicate list.
- The current CLI default `--typed-order-json '[]'` does not exercise `default_order`; API callers can send an empty string to use the default order.
- `personaldb_row_metadata` and `git_source` are accepted index kinds, but the generic Index service does not currently materialise/query them through the ordinary builder path.

## Debugging query JSON

When a query returns no hits, validate each layer separately. First query with only `path_prefix` or a very broad text query to prove the index has data. Then add metadata filters, typed predicates, ordering, phrase mode, vector input, and catch-up requirements one at a time. If a typed predicate fails, confirm the field name exists in the build policy and that extracted values have the expected JSON type. If vector query fails, confirm the vector dimension matches the index configuration.

For production clients, log the index name, definition generation, query JSON fields, page token, catch-up cursor, lag timeout, and caller principal. That log gives operators enough information to distinguish malformed queries from stale or broken derived state.
