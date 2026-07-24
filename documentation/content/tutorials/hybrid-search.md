---
title: Hybrid Search
description: Combine full-text relevance and vector similarity in one authorised index query.
---

# Hybrid Search

This tutorial builds on [Full-Text Search](/tutorials/full-text-search/), [Vector Search](/tutorials/vector-search/), and [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/). Full-text search is good when the user's words matter. Vector search is good when semantic similarity matters. Hybrid search is for product search experiences that need both signals in one ranked, authorised result set.

Applications should call the public Index API directly. The `anvil index create`, `anvil index query`, and `anvil index diagnostics` commands below are manual helpers over the same request fields; the complete command surface is described in [Public CLI](/reference/public-cli/). Keep [Indexes and Query](/learn/indexes-and-query/) nearby for the conceptual model and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) for scope strings.

Hybrid search is useful only when two retrieval signals are intentional. This tutorial explains how Anvil combines text and vector evidence, how selectors and metadata filters remain hard constraints, why the query vector must come from the same model space as the index, and how authorisation and derived-data lag shape a production search service.

## When hybrid search is the right tool

Use hybrid search when neither exact words nor embeddings alone explain the user's intent. A user searching for `renewal notice` may expect documents containing that phrase to rank well, but they may also expect semantically related content such as "subscription reminder" or "upcoming payment message". The full-text side rewards matching terms and phrases. The vector side rewards nearby embeddings. The hybrid query path combines those scores and applies authorisation before returning hits.

Do not use hybrid search for every list view. A dashboard such as "open invoices due before Friday" should use typed JSON predicates and ordering. A simple document search box may only need full text. A recommendation or duplicate-detection workflow may only need vector search. Hybrid search is useful when you are prepared to explain that ranking depends on multiple relevance signals.

A current hybrid index is not a pointer to two existing index definitions. It is one index definition that stores a full-text segment and a vector segment under the same bucket/index name. Do not create `document_text` and `document_vectors` and then try to reference them from `extractor_json`; that older shape is not supported by the current Index service.

## Prerequisites and current tutorial limits

The commands in this page are valid current public CLI shapes, but they are illustrative until your local tutorial environment has bucket placement, source objects, index grants, a running index builder, and source objects that already contain vectors. The public CLI does not generate embeddings, and its object upload helper still cannot attach all metadata-rich object fields. Use the public API or a client library for production ingestion.

Hybrid indexes are tenant-owned public-plane resources. Create and query them with the public Index API or `anvil`; do not use `anvil-admin` for tenant search definitions. The admin plane can bootstrap tenant credentials and operate system lifecycle, but routine index modelling belongs to the tenant.

Use narrow grants rather than wildcards. The relevant public policy scopes are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Create the tutorial hybrid index | `index:create` | `documents/tutorial_hybrid` |
| Query or inspect diagnostics for indexes in `documents` | `index:read` | `documents` |
| See hits from an `inherit_object` index | `object:read` or object-reader relationship | `documents/<object-key>` |

Current query and diagnostics operations use the coarse `index:read` resource `documents`, not `documents/<index-name>`. With the default `inherit_object` authorisation mode, `index:read` lets the caller ask the index a question, but each returned object must still be visible to that caller.

## Shape one source object for both signals

A hybrid index works best when the selected object family has both human-readable text and an embedding produced from the same content or chunk. For a tutorial object under `tutorial/search/renewal.json`, the body might look like this:

```json
{
  "title": "Renewal payment notice",
  "summary": "How Acme explains upcoming renewal invoices.",
  "body": "Customers receive a notice before payment collection. The renewal notice includes invoice terms and support contacts.",
  "embedding": [0.10, 0.20, 0.30, 0.40]
}
```

The text fields are the material for the full-text segment. The `embedding` array is the material for the vector segment. The embedding is not a copy of the object and it is not a substitute for the body. It is derived material used for retrieval. If the text changes, the embedding pipeline should update the vector and the index builder must publish a new segment before hybrid ranking reflects the change.

The example uses a four-dimensional vector only so the commands fit on the page. Production embeddings normally have hundreds or thousands of dimensions and must come from a real model pipeline. The current deterministic `test_only` embedding provider is not production quality and is disabled unless the server explicitly allows it; this tutorial uses caller-supplied vectors instead.

## Understand the four definition parts

`selector_json` chooses the source objects before indexing. In this tutorial, it selects objects under `tutorial/search/`. That selector is the build-time boundary: an object outside the prefix is not searchable by this hybrid index even if its text or vector would be a perfect match. The selector applies to both the full-text and vector build paths. If the text and vector sources need different object families, use separate indexes or an application-level plan instead of forcing them into one hybrid definition.

`extractor_json` describes the text extraction side. For hybrid indexes, keep the full-text extractor under the `text` key so it is clear which fields feed tokenisation. The vector extractor does not belong in `extractor_json`; current vector build behaviour takes it from `build_policy_json.vector.extractor`.

`build_policy_json.full_text` controls tokenisation options such as positions, lowercasing, and maximum token length. Phrase queries on the text side require positions to have been stored.

`build_policy_json.vector` is a complete vector index definition. It declares the vector schema, source provenance, vector extractor, embedding provider/model/dimension/modality/normalisation, and HNSW metric. See [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/) for the full grammar.

The authorisation mode controls whether hits are filtered through object visibility. This tutorial sets `inherit_object` explicitly because hybrid results can reveal sensitive information through object keys, scores, and result existence.

## Create a hybrid index

Create one hybrid index over the tutorial prefix:

```bash
anvil --profile acme index create documents tutorial_hybrid hybrid \
  --selector-json '{"prefix":"tutorial/search/"}' \
  --extractor-json '{"text":{"fields":[{"source":"object_key"},{"source":"json_pointer","pointer":"/title"},{"source":"json_pointer","pointer":"/summary"},{"source":"json_pointer","pointer":"/body"}]}}' \
  --build-policy-json '{"full_text":{"positions":true,"language":"simple","max_token_chars":128,"lowercase":true,"normalize_nfkc":true,"record_original_ranges":true},"vector":{"schema":"anvil.index.vector_definition.v1","source":{"kind":"object_current"},"extractor":{"kind":"object_body_json_vector","json_pointer":"/embedding"},"embedding":{"provider":"caller_supplied","model":"tutorial-embedding-v1","dimension":4,"modality":"text","normalisation":"unit_l2","chunking":{"strategy":"whole_object"}},"ann":{"algorithm":"hnsw","metric":"cosine","m":32,"ef_construction":200,"ef_search_default":80}}}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex` with kind `hybrid`. A successful response proves that the caller authenticated, had `index:create` on `documents/tutorial_hybrid`, the bucket exists, the index name and kind are valid, all three JSON strings parsed, the full-text policy validated, the vector policy validated, and Anvil stored an enabled index definition. It also enqueues build work.

It does not prove that any selected object exists, that every JSON Pointer is present, that every object body contains a valid four-dimensional vector, or that either segment has been materialised. The builder discovers those facts later. Missing text fields, invalid JSON bodies, vector dimension mismatches, and provider failures appear as diagnostics.

Do not add a `weights` object to this definition. Current direct hybrid query weights are not configurable in the index definition or CLI.

## Query with both text and vector

A normal hybrid query sends both the user's text and a query vector from the same embedding space as the indexed vectors:

```bash
anvil --profile acme index query documents tutorial_hybrid \
  --text 'renewal notice' \
  --vector 0.11,0.19,0.31,0.39 \
  --path-prefix tutorial/search/billing/ \
  --metadata-filters-json '{"audience":"customer","workflow_state":"published"}' \
  --limit 10
```

This calls `IndexService.QueryIndex`. A successful response proves the caller has `index:read` on `documents`, the hybrid index exists and is enabled, the full-text segment is available, the vector segment is available, the query vector dimension matches the vector segment, the metadata filter JSON is valid, and every printed hit passed the index's authorisation mode checks.

A typical CLI row is shaped like this:

```text
score=1.743 object_key=tutorial/search/billing/renewal-payment.json metadata_json={...}
```

Read the score only inside this index and query. Hybrid scores are not portable across indexes, embedding models, fusion weights, or query mixes. If the application needs to explain a ranking decision, keep the source object key, query text, query vector provenance, index definition version, and scoring metadata in your own request logs.

The CLI prints `score`, `object_key`, and `metadata_json`. For hybrid hits, current `metadata_json` includes the raw text score, raw vector score, freshness score, and normalised text/vector scores. The API response also carries `scoring_recipe_json`, including the weights and segment generations, although the current CLI does not print that response field.

The query text is tokenised like a full-text query. It is not a boolean language: `AND`, `OR`, parentheses, wildcards, and field prefixes are not implemented by the direct CLI query. If you pass `--phrase`, the text side looks for adjacent tokens and requires the index to have been built with positions.

The query vector must come from the same model, dimension, modality, and normalisation as the index. The CLI does not embed query text for you. A production search service should call the same embedding model used by the indexing pipeline, then pass the numeric vector to `QueryIndex`.

## How current scoring works

When both `query_text` and `query_vector` are present, Anvil currently uses fixed weights: `0.55` text, `0.35` vector, and `0.10` freshness. These weights are implementation-defined today. There is no supported CLI flag or build-policy field for changing them.

The text side uses full-text relevance. The vector side uses nearest-neighbour scoring from the vector segment. Anvil normalises positive text and vector scores across the candidate set for this query, then adds a freshness score derived from object creation time within the same candidate set. Newer objects among the candidates get more of the freshness contribution; freshness is not a global recency guarantee across the bucket.

The candidate set is a union of candidates from the active signals, not a strict intersection. A document that matches the text side but has no vector hit can still rank with a zero vector contribution. A document that is a strong vector neighbour but does not contain the query words can still rank with a zero text contribution. Path prefixes, metadata filters, and authorisation checks can remove candidates before final results are returned.

For hybrid search today, prefer `cosine` or `dot` metrics for the vector side. Although vector definitions accept `l2`, direct hybrid normalisation clips negative vector scores to zero, and Anvil represents L2 distance as a negative score. That makes L2 a poor fit for current hybrid ranking.

## Query with one signal when you need to compare behaviour

A hybrid index can be queried with only text or only a vector. This is useful while tuning a product search experience because it lets you see which signal is contributing to a result.

Text-only query:

```bash
anvil --profile acme index query documents tutorial_hybrid \
  --text 'renewal notice' \
  --limit 10
```

Vector-only query:

```bash
anvil --profile acme index query documents tutorial_hybrid \
  --vector 0.11,0.19,0.31,0.39 \
  --limit 10
```

With text only, the current direct hybrid path gives text all scoring weight and does not add freshness. With a vector only, it gives vector similarity all scoring weight and does not add freshness. If your production path only ever uses one signal, a dedicated full-text or vector index is usually simpler to operate and explain.

## Use path and metadata filters as hard narrowing

Hybrid search should not rely on relevance scores for hard business constraints. Use `path_prefix` to stay within a customer, product area, import batch, or content family. Use `metadata_filters_json` for exact equality over object user metadata such as `audience`, `workflow_state`, or `locale`.

`metadata_filters_json` is exact JSON equality. It does not coerce strings to numbers, and it is not a range language. The JSON string `"20"` is different from the JSON number `20`.

Direct hybrid queries do not currently evaluate `typed_predicates_json` or `typed_order_json`. If you need structured range predicates, stable ordering, or queue-style dashboards, use a `typed_json` index for that path, or design a public API flow that plans typed filtering explicitly rather than expecting the direct `anvil index query` hybrid command to do it.

## Keep authorisation inside the query

Hybrid search can leak information if it is implemented by fetching broad text hits and vector neighbours, merging them in an application, and filtering afterwards. Object keys, scores, matched-term counts, vector proximity, and the existence of a result are all data.

With `--authorization-mode inherit_object`, the query path filters results through object visibility. A caller needs `index:read` on the bucket to query, and each hit must also be visible through `object:read` on `documents/<object-key>` or through a matching relationship-authorisation object reader relation. The service also uses authorisation-derived labels while collecting candidates, so permission checks are part of the search plan rather than an afterthought.

`index_only` and `public` authorisation modes skip the per-hit object read check. They still require authenticated `index:read` on the bucket; they are not anonymous public access. Use them only for derived indexes whose object keys and ranking metadata are deliberately safe for every bucket-level index reader.

## Understand lag and catch-up limitations

A hybrid index is derived data twice: the builder publishes a full-text segment and a vector segment. A committed object write can be visible through object reads before either segment has caught up. A query that sends both text and vector inputs requires both relevant segments to be available; otherwise it can fail with `IndexUnavailable`.

`QueryIndexRequest` includes `require_caught_up_to_watch_cursor`, and the CLI exposes `--require-caught-up-to-watch-cursor`, but current direct hybrid query responses do not enforce or report meaningful source-cursor catch-up. They currently report caught-up status as true with zero cursor fields. `lag_timeout_ms` is also present on the API and CLI, but direct hybrid queries do not wait for catch-up today.

Use catch-up requirements for metadata-backed and typed JSON indexes where the current implementation supports them. For hybrid search, treat freshness as operational derived-data lag: observe source and index watches, inspect diagnostics, design newly uploaded documents to show an "indexing" state, and retry later rather than treating a missing search hit as proof that the object does not exist.

## Inspect diagnostics

Diagnostics are the first place to look when one side of the hybrid index is missing expected material:

```bash
anvil --profile acme index diagnostics documents tutorial_hybrid \
  --page-size 20
```

This calls `IndexService.ListIndexDiagnostics`. A successful response proves the caller has `index:read` on `documents` and that Anvil could read diagnostic records for the bucket/index filter. The CLI prints `cursor`, `severity`, `code`, and `message`.

Full-text diagnostics can include non-UTF-8 bodies, invalid JSON for text JSON Pointers, missing fields, and empty extracted text. Vector diagnostics can include invalid JSON vector shapes, vector dimension mismatches, disabled `test_only` embeddings, missing configured providers, provider failures, and provider model-version mismatches. No output means no matching diagnostics were returned in that page; it does not prove every selected object has been indexed or is visible to the querying principal.

## What to take forward

Use hybrid search when a user-facing search result needs both lexical evidence and semantic similarity. Keep the hybrid definition as one index with shared selection, a text extractor in `extractor_json.text`, and a complete vector definition in `build_policy_json.vector`. Query with text and a same-model vector when you want the current fixed text/vector/freshness scoring recipe. Use path and metadata filters for hard narrowing, keep `inherit_object` for protected corpora, and treat catch-up for hybrid search as an operational lag concern until direct hybrid queries expose meaningful watch-cursor freshness.

## Success and failure cues

Hybrid results are easiest to debug by temporarily querying each signal alone. If lexical-only works but hybrid does not, inspect vector dimension, model provenance, and fusion weights. If vector-only works but hybrid does not, inspect text extractors and tokenisation. If both work separately but expected rows disappear together, check hard filters, selector scope, derived lag, and per-object visibility before changing ranking parameters.

## Where to go next

Use [End-to-End Document System](/tutorials/end-to-end-document-system/) to see hybrid retrieval in a fuller application design. If the problem you are solving is freshness or maintenance rather than ranking quality, read [Watches](/tutorials/watches/) and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/) before changing index definitions.
