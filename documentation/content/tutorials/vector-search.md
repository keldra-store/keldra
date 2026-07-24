---
title: Vector Search
description: Build nearest-neighbour indexes over embedding vectors while preserving model, metric, and authorisation boundaries.
---

# Vector Search

This tutorial continues from [Full-Text Search](/tutorials/full-text-search/) and the reference page for [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/). Full-text search answers "which documents contain these words?" Vector search answers a different question: "which objects are near this example in an embedding space?"

Applications should use the public Index API directly. The `anvil index create`, `anvil index query`, and `anvil index diagnostics` commands below are supporting manual helpers over the same API fields; the complete command surface is in [Public CLI](/reference/public-cli/). Keep [Indexes and Query](/learn/indexes-and-query/) and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) nearby. If you need to combine words and semantic similarity, read [Hybrid Search](/tutorials/hybrid-search/) after this page.

The examples use tiny vectors so the shape is readable, but the contract is production-shaped. You will keep the embedding model, dimension, modality, metric, normalisation, extractor, and ANN settings explicit; then query with a vector from the same space and preserve per-object visibility in the result set.

## Understand the vector contract first

An embedding is a list of numbers produced by a model. A text embedding model might turn "renewal payment notice" into a vector such as `[0.10, 0.20, 0.30, 0.40]`; an image model might produce a much longer vector for an image. The numbers do not mean much one by one. Their value comes from the model arranging related inputs near each other in the same vector space.

That model identity is part of the data contract. Do not compare vectors from different models just because they have the same dimension. A 768-dimensional image vector and a 768-dimensional text vector are not automatically compatible. Even two text models with the same dimension can use different spaces, tokenisation, training data, and normalisation.

Dimension is the vector length. Anvil validates extracted vectors and query vectors against the dimension declared in the index definition. If the index says `dimension: 4`, every indexed vector and every `query_vector` must contain exactly four numbers. The small dimensions in this tutorial keep examples readable; production text embeddings are usually hundreds or thousands of dimensions.

Metric decides how Anvil ranks neighbours. `cosine` compares direction and is common for unit-normalised text embeddings. `dot` uses dot product, so vector magnitude can affect the score. `l2` uses Euclidean distance, but Anvil returns it as a score where higher is still better by negating the distance. Compare scores only inside the same index, model, metric, and query shape.

Normalisation records how vectors were prepared, for example `unit_l2`. For caller-supplied vectors, Anvil validates the shape and stores the provenance; it does not prove that your pipeline really normalised the values. Keep the same normalisation rule for source vectors and query vectors. With `dot`, inconsistent magnitudes can dominate ranking. With `cosine`, direction matters most, but you should still keep provenance honest.

Anvil's vector definition uses ANN, approximate nearest neighbour, configuration. The only accepted algorithm today is `hnsw`. At a high level, HNSW builds a graph over vectors so the query can walk towards likely neighbours instead of scanning every item in a large corpus. Parameters such as `m`, `ef_construction`, and `ef_search_default` trade build cost, memory, and recall. Treat them as production tuning values, not tutorial decoration.

## Prerequisites and current tutorial limits

The commands below are valid current public CLI shapes, but they are illustrative until your local tutorial environment has bucket placement, source objects, index grants, and a running index builder. Earlier tutorial gaps still matter here: the public CLI upload helper cannot attach all metadata-rich object fields, and it does not generate embeddings for you.

Vector indexes are tenant-owned public-plane resources. Create, query, and inspect them with the public Index API or `anvil`, not `anvil-admin`. Operators may configure embedding providers and diagnose system health, but the index definition in this tutorial belongs to the tenant and is authorised by public policy scopes.

Use narrow grants rather than wildcards. The relevant public policy scopes are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Create the tutorial vector index | `index:create` | `documents/tutorial_vectors` |
| Query or inspect diagnostics for indexes in `documents` | `index:read` | `documents` |
| See hits from an `inherit_object` index | `object:read` or object-reader relationship | `documents/<object-key>` |

Current list, query, and diagnostics operations use the coarse `index:read` resource `documents`, not `documents/<index-name>`. With the default `inherit_object` authorisation mode, `index:read` lets the caller ask the index service a question, but it does not by itself make every matching object visible.

## Choose where embeddings come from

There are two practical patterns.

The simplest tutorial pattern is caller-supplied vectors. Your application or data pipeline embeds content outside Anvil, stores the vector in the object body, and defines a vector index that extracts that vector. In this pattern, the `embedding.provider` value such as `caller_supplied` is provenance. Anvil does not call an embedding service during build; it reads the vector that is already in the object payload.

Provider-generated vectors move source-text embedding into the index builder. The vector extractor uses `object_body_utf8`, and the server sends the object body to a configured embedding provider. Current provider integration is command-based JSON configured on the server, and validation fails if the named provider is missing. The built-in deterministic `test_only` provider is for tests only: it is disabled unless the server explicitly allows it, and its vectors are not production-quality semantic embeddings.

Provider-generated vectors do not remove the need for a query vector. The current `anvil index query` command accepts `--vector`; it does not accept a natural-language query and call the embedding provider for you. A production search service should embed the user's query with the same model, dimension, modality, and normalisation as the index, then pass that numeric vector to `QueryIndex`.

Modality is also part of provenance. The vector schema accepts `text`, `image`, `audio`, and `video`. The current provider-generated extractor sends UTF-8 text to a configured provider. For image, audio, or video embeddings today, generate vectors in your media pipeline and store them as caller-supplied values.

## Shape the source objects deliberately

For caller-supplied vectors, a JSON object body can carry both application fields and an embedding. For example, an object under `tutorial/vectors/renewal.json` might have this body:

```json
{
  "title": "Renewal reminder",
  "body": "Notify the customer before renewal.",
  "embedding": [0.10, 0.20, 0.30, 0.40]
}
```

The vector is not a copy of the object and it is not the source of truth for the title or body. It is derived material that helps retrieval. If the body changes, the embedding should change too, and the index must rebuild from the new current object version before search reflects it.

The JSON vector extractor can read a direct numeric array, an object containing `vector`, `values`, or `embedding`, or an array of vector records. Multi-vector records can include `chunk_id`, `source_start`, and `source_len`, which let a future result point to a passage or chunk inside the source. Start with one whole-object vector unless you already have a chunking pipeline.

## Create a caller-supplied vector index

A vector index uses the same four-index-field idea as the structured tutorial, but one detail is different. `selector_json` still chooses which source objects are eligible. For this tutorial, `{"prefix":"tutorial/vectors/"}` means only object keys under that prefix can contribute vectors. `extractor_json` must be `{}` or `null` for kind `vector`; current validation rejects a separate vector extractor there. The vector extractor lives inside `build_policy_json` because it is part of the vector definition alongside embedding provenance and ANN settings. Query-time JSON and flags, such as `--vector`, `--path-prefix`, and `--metadata-filters-json`, narrow or rank rows that were already materialised.

Create a tutorial index over objects under `tutorial/vectors/`:

```bash
anvil --profile acme index create documents tutorial_vectors vector \
  --selector-json '{"prefix":"tutorial/vectors/"}' \
  --extractor-json '{}' \
  --build-policy-json '{"schema":"anvil.index.vector_definition.v1","source":{"kind":"object_current"},"extractor":{"kind":"object_body_json_vector","json_pointer":"/embedding"},"embedding":{"provider":"caller_supplied","model":"tutorial-embedding-v1","dimension":4,"modality":"text","normalisation":"unit_l2","chunking":{"strategy":"whole_object"}},"ann":{"algorithm":"hnsw","metric":"cosine","m":32,"ef_construction":200,"ef_search_default":80}}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex`. A successful response proves that the caller authenticated, had `index:create` on `documents/tutorial_vectors`, the bucket exists, the index kind is recognised, the three JSON fields parsed, the vector schema passed validation, and Anvil stored an enabled index definition. It also enqueues build work.

It does not prove that any selected object exists, that every object body is valid JSON, that `/embedding` exists, that the extracted vector has four numbers, or that a vector segment has already been materialised. Those facts are discovered by the index builder and visible through diagnostics.

A typical successful create response from the CLI is terse. Application code should keep the index name, definition version or generation if exposed by the API, and the exact vector contract it sent. Those values are how you later explain why a query vector of length `1536` does not fit a tutorial index declared with `dimension: 4`.

The `selector_json` prefix is a build-time boundary. Objects outside `tutorial/vectors/` are not materialised into this index. Query-time `--path-prefix` can narrow within the selected set, but it cannot make the query search objects that the selector excluded.

## Know what each build-policy field means

The `schema` string selects the vector definition format. Use `anvil.index.vector_definition.v1` for current vector indexes.

`source.kind` is required by the schema and currently records provenance for object-current indexing. Source selection still comes from `selector_json`.

`extractor.kind` says how the builder gets vectors from each selected object. `object_body_json_vector` parses the object body as JSON and reads the value at `json_pointer`. Other current kinds are `object_body_f32_le` for raw little-endian `f32` bodies, and `object_body_utf8` for provider-generated text embeddings.

`embedding` records the provider, model, optional model version, dimension, modality, normalisation, and chunking strategy. Keep these fields specific enough that future operators can tell which pipeline produced the vectors. If a configured provider returns a different `model_version` from the one declared in the index, the builder records a diagnostic instead of silently mixing versions.

`ann` selects HNSW and the metric. The defaults are currently `m: 32`, `ef_construction: 200`, and `ef_search_default: 80` if those fields are omitted. The tutorial writes them explicitly so you can see the tuning contract.

## Query with a vector from the same space

Query the index with a vector that was produced by the same embedding model and normalisation process:

```bash
anvil --profile acme index query documents tutorial_vectors \
  --vector 0.11,0.19,0.31,0.39 \
  --limit 5
```

This calls `IndexService.QueryIndex` with `query_vector` set to the four numbers from the CLI flag. A successful response proves that the caller has `index:read` on `documents`, the index exists and is enabled, a vector segment is available, the query vector dimension matches the segment, and every printed hit passed the index's authorisation mode checks.

The CLI prints `score`, `object_key`, and `metadata_json`. For direct vector queries, current metadata includes details such as bucket name, metric, and modality. Results are ordered by score descending. For `cosine` and `dot`, higher means more similar under that metric. For `l2`, higher still means closer because Anvil returns negative distance as the score. A typical row is shaped like this:

```text
score=0.992 object_key=tutorial/vectors/renewal.json metadata_json={...}
```

Read this as retrieval evidence, not as the source document. If the application needs the title, snippet, or current business fields, perform a normal authorised object read after choosing which hits to show.

If the command fails with `query_vector dimension mismatch`, the query vector length does not match the index dimension. If it fails with `IndexUnavailable`, the definition exists but no vector segment is available yet. If it returns no rows, possible causes include no matching vectors, indexing lag, metadata/path filters, or object visibility under `inherit_object`.

## Narrow by path prefix and metadata

Vector similarity is usually only one signal. A product search might first narrow to one customer, product area, or publication state, then rank by semantic distance. The direct vector query path supports `path_prefix` and exact user-metadata filters:

```bash
anvil --profile acme index query documents tutorial_vectors \
  --vector 0.11,0.19,0.31,0.39 \
  --path-prefix tutorial/vectors/billing/ \
  --metadata-filters-json '{"audience":"customer","workflow_state":"published"}' \
  --limit 5
```

The path prefix is checked against object keys represented by vector hits. The metadata filter is exact JSON equality against object user metadata; it is not text search, range filtering, or type coercion. The JSON number `20` is different from the string `"20"`.

This command is illustrative unless your objects already carry user metadata, because the current public CLI object upload helper cannot set `user_metadata_json`. Use the public API or a client library for metadata-rich uploads, or run the query against existing objects that already have the expected metadata.

Do not retrieve broad private neighbours and filter them in application code. The index service must participate in filtering and authorisation so object keys, result existence, and ranking information are not leaked to callers that should not see them.

## Keep authorisation in the search design

The create command used `--authorization-mode inherit_object`, which is the safest default for object-derived vectors. In that mode, a caller needs `index:read` to query the index and must also be allowed to read each returned object, either through `object:read` on `documents/<object-key>` or through a matching relationship-authorisation object reader relation.

This matters because vector search can reveal sensitive information without returning object bodies. A result key, score, modality, or mere presence in the neighbour set can expose what private data resembles the query. Keep `inherit_object` unless the indexed corpus and object keys are deliberately visible to every principal that can query the index.

`index_only` and `public` authorisation modes skip the per-hit object read check, but they do not make the public API anonymous. The caller still needs authenticated `index:read` on the bucket. Use those modes only for derived rows that are safe for all bucket-level index readers.

## Understand lag and catch-up limitations

A vector index is derived data. An object write can commit before the vector builder has read the new current object, extracted its embedding, and published a new segment. A fresh upload may not appear in semantic search immediately.

`QueryIndexRequest` includes `require_caught_up_to_watch_cursor`, and the CLI exposes `--require-caught-up-to-watch-cursor`, but current direct vector query responses do not enforce or report meaningful source-cursor catch-up. Vector responses currently report caught-up status as true with zero cursor fields. The `lag_timeout_ms` field is also present, but direct vector queries do not wait for catch-up today.

Use `require_caught_up_to_watch_cursor` for metadata-backed and typed JSON correctness where it is currently implemented. For vector search, treat freshness as operational derived-data lag: watch source changes, watch index partition progress if your worker exposes the partition id, retry user searches after a short delay, and inspect diagnostics when expected vectors are missing.

## Inspect diagnostics

Diagnostics explain why expected vectors did not enter the index:

```bash
anvil --profile acme index diagnostics documents tutorial_vectors \
  --page-size 20
```

This calls `IndexService.ListIndexDiagnostics`. A successful response proves the caller has `index:read` on `documents` and that Anvil could read diagnostic records for the bucket/index filter. The CLI prints `cursor`, `severity`, `code`, and `message`.

Common vector diagnostics include invalid JSON bodies, unsupported JSON vector shapes, missing vector values, dimension mismatches, raw `f32` payload lengths that are not divisible by four, disabled `test_only` embeddings, missing configured providers, provider failures, and provider model-version mismatches. No output means no matching diagnostics were returned in that page; it does not prove every selected object was indexed or visible to the querying principal.

## What to take forward

Use vector search for semantic similarity, not for exact words, boolean syntax, or structured workflow predicates. Keep model, dimension, modality, metric, normalisation, and chunking as explicit contracts. Prefer caller-supplied vectors when your application already owns the embedding pipeline, and use provider-generated vectors only after configuring a production embedding provider on the server. Query with a vector from the same model space, narrow with path and metadata filters, keep `inherit_object` for protected corpora, and treat vector freshness as derived-data lag until the vector catch-up path reports meaningful cursors.

## Success and failure cues

A vector index is healthy only when the source vectors and query vector share the same model contract and dimension. Creation failures usually point to invalid definition JSON or unsupported ANN settings. Missing hits usually come from selector mismatch, absent vectors, dimension errors, provider failures, derived lag, or object-authorisation filtering. Scores are comparable only within the same index, query, metric, and embedding space.

## Where to go next

Continue to [Hybrid Search](/tutorials/hybrid-search/) when you need to blend semantic neighbours with text terms, or return to [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) if the product question is really a structured filter. For exact JSON shapes and accepted ANN fields, use [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).
