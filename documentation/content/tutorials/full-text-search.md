---
title: Full-Text Search
description: Build token-based search indexes over object text while preserving object visibility and derived-data safety.
---

# Full-Text Search

This tutorial continues from [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) and the structured-index tutorial, [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/). Those pages separated canonical object bodies, user metadata, and derived index rows. Full-text search uses the same model: the object body is still the source of truth, and the full-text index is a derived structure built from selected text.

Use the public Index API directly in applications. The `anvil index create`, `anvil index query`, and `anvil index diagnostics` commands below are manual helpers over the same API fields; the broader command reference is [Public CLI](/reference/public-cli/). Keep [Indexes and Query](/learn/indexes-and-query/), [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) nearby. If you need semantic similarity rather than token matching, read [Vector Search](/tutorials/vector-search/) after this page.

Read this page as a search-contract walkthrough rather than a tokenizer demo. You will choose which text becomes searchable, create a full-text index with explicit selector and extractor JSON, run term and phrase queries, keep object authorisation in the result path, and understand which freshness guarantees are not yet meaningful for direct full-text queries.

## What full-text search does today

Full-text search is for human-language text: titles, paragraphs, notes, transcripts, descriptions, or source text. Anvil tokenises selected text into terms, stores postings for each term, and ranks matching object fields with BM25-style scoring. A higher score means the indexed field looked more relevant to the query terms under the current scoring recipe.

The current direct `QueryIndex` full-text path is intentionally simple. `query_text` is tokenised with the same tokenizer configuration as the index. A normal query matches fields that contain any of the query terms and ranks them; it is not a boolean expression language. Operators such as `AND`, `OR`, `NOT`, parentheses, wildcards, field prefixes, and quoted subexpressions are not implemented by the direct CLI query. If you send `payment OR renewal`, `OR` is just another token.

Phrase mode is different. When you pass `--phrase`, Anvil looks for adjacent query tokens in the same indexed field. Phrase queries require positions to have been stored when the index was built. If the index was created with positions disabled, a phrase query fails instead of approximating the result.

Full-text search is not a substitute for structured filters. Use path prefixes and metadata filters to narrow the candidate set. Use `typed_json` indexes when you need range predicates, stable ordering over structured fields, or exact workflow queues.

## Prerequisites and current tutorial limits

The commands in this page are valid current public CLI shapes, but they are illustrative until your local tutorial environment has working bucket placement, index grants, source objects, and index builder execution. The current public CLI object upload helper still cannot attach `content_type` or `user_metadata_json`, so examples that rely on content-type selectors or metadata filters need objects uploaded through the public API/Rust client, or existing objects that already carry those fields.

Full-text indexes are tenant-owned public-plane resources. Create and query them with the public API or `anvil`; do not use `anvil-admin` for this work. The admin plane may bootstrap tenant credentials or diagnose system-wide operations, but the index definition, query, and diagnostics in this tutorial belong to the tenant.

Use narrow grants. The relevant public policy scopes are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Create the tutorial text index | `index:create` | `documents/tutorial_text` |
| Query or inspect diagnostics for indexes in `documents` | `index:read` | `documents` |
| See hits from an `inherit_object` index | `object:read` or object-reader relationship | `documents/<object-key>` |

The `index:read` check is currently bucket-wide for list/query/diagnostic paths. It is not scoped to one index name. With the default `inherit_object` authorisation mode, `index:read` is not enough to see a hit: each returned object must also be visible through object read authorisation or the object reader relationship.

## Understand tokenisation before choosing fields

Tokenisation turns text into searchable terms. In the current implementation, the tokenizer uses Unicode word boundaries, keeps alphanumeric word segments, can lowercase through Unicode case folding, can apply NFKC normalisation, and ignores tokens longer than `max_token_chars`. These defaults are good for ordinary text because `Payment`, `payment`, and some compatibility forms can become the same search term.

A few build-policy fields are validated and stored as provenance even though they do not yet change tokenisation. `language` is a label today. `stop_words_enabled` and `stemming` are accepted definition fields, but the current tokenizer does not implement stop-word removal or stemming. Do not promise users language-specific stemming until the implementation actually applies it.

The default build policy is already usable:

```json
{
  "positions": true,
  "language": "simple",
  "max_token_chars": 128,
  "lowercase": true,
  "normalize_nfkc": true,
  "record_original_ranges": true
}
```

`positions: true` is important if you will offer phrase search. `record_original_ranges` records original byte ranges in token metadata for future highlighting and extraction work, but the current direct query response does not return snippets.

## Choose extractor fields

`selector_json` chooses which objects enter the index. `extractor_json` chooses which text Anvil reads from each selected object. `build_policy_json` controls tokenisation and scoring metadata. These are build-time settings; changing them requires updating/rebuilding the index.

Suppose Acme stores JSON articles under `tutorial/articles/` with a shape like this:

```json
{
  "title": "Renewal payment notice",
  "summary": "How Acme explains upcoming renewal invoices.",
  "body": "Customers receive a notice before payment collection. The renewal notice includes invoice terms and support contacts."
}
```

The selector for this tutorial will be `{"prefix":"tutorial/articles/"}`. That means only object keys beginning with that prefix are eligible for the index. The selector does not inspect JSON fields and it is not a query-time filter; it decides what source records the builder will ever consider.

A full-text extractor can index several fields from that one JSON body:

```json
{
  "fields": [
    {"source": "object_key"},
    {"source": "json_pointer", "pointer": "/title"},
    {"source": "json_pointer", "pointer": "/summary"},
    {"source": "json_pointer", "pointer": "/body"}
  ]
}
```

Each field is tokenised separately. Phrase matching only checks adjacent tokens inside one field; it does not join the title and body together into one long phrase. `object_key` is useful when users search for stable names or identifiers. `json_pointer` decodes the object body as JSON and extracts the value at a pointer. Strings are indexed as text; arrays and objects are stringified JSON.

If the whole object body is plain UTF-8 text, use an empty extractor or `{"source":"object_body_utf8"}` instead. If you use a JSON Pointer extractor against a non-JSON body, the builder records a diagnostic and that field is not indexed for that object version.

Treat `extractor_json` as an application contract. Adding `/body` to the extractor means users may later find the object because of body text; removing it means body-only matches disappear after rebuild. Changing extractor fields is therefore a product change, not just an optimisation.

## Create the full-text index

Create a full-text index over the tutorial article prefix:

```bash
anvil --profile acme index create documents tutorial_text full_text \
  --selector-json '{"prefix":"tutorial/articles/"}' \
  --extractor-json '{"fields":[{"source":"object_key"},{"source":"json_pointer","pointer":"/title"},{"source":"json_pointer","pointer":"/summary"},{"source":"json_pointer","pointer":"/body"}]}' \
  --build-policy-json '{"positions":true,"language":"simple","max_token_chars":128,"lowercase":true,"normalize_nfkc":true,"record_original_ranges":true}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex`. A successful response proves that the caller authenticated, had `index:create` on `documents/tutorial_text`, the bucket exists, the index name and kind are valid, the three JSON strings parsed, and the full-text build policy passed validation. It also stores the index definition and enqueues build work.

It does not prove that every selected object has valid JSON, that every JSON Pointer exists, or that a segment has already been materialised. Those facts are discovered by the index builder. If the builder cannot extract a field, it records an index diagnostic and continues with the other fields/objects it can process.

## Search terms

Use `--text` for a direct full-text query:

```bash
anvil --profile acme index query documents tutorial_text \
  --text 'payment renewal' \
  --limit 10
```

This calls `IndexService.QueryIndex` with `query_text = "payment renewal"`. A successful query proves the caller has `index:read` on `documents`, the index exists and is enabled, a full-text segment is available, the query produced at least one token, and every printed hit passed the index's authorisation mode checks.

The query is not an implicit `AND`. It tokenises to terms such as `payment` and `renewal`, finds fields containing any of those terms, and scores them. A field containing both terms should usually rank better than a field containing one, but a one-term match can still appear. Use structured filters for hard constraints rather than trying to encode them into `query_text`.

The CLI prints `score`, `object_key`, and `metadata_json`. Current full-text `metadata_json` includes details such as matched term count and an authz label hash. It does not return text snippets or highlighted passages today.

A typical successful row is shaped like this:

```text
score=4.281 object_key=tutorial/articles/billing/renewal-payment.json metadata_json={...}
```

Read the row as evidence that an indexed field matched and the caller was allowed to see that object. It is not a body read. If your UI needs the document title, snippet, or current metadata, follow up with an authorised object read or application API call.

If the command fails with `IndexUnavailable`, the definition exists but no full-text segment is available. If it fails with `query_text is required`, the text was empty or tokenised to no searchable terms.

## Search phrases when positions are stored

Phrase search checks adjacent tokens in the same indexed field:

```bash
anvil --profile acme index query documents tutorial_text \
  --text 'renewal notice' \
  --phrase \
  --limit 10
```

This proves a stricter condition than a term search. The field must contain the token sequence `renewal`, then `notice`, in order, with no other token between them. It is useful for exact product names, legal clauses, error messages, and titles where word order matters.

Phrase queries depend on `positions: true` in `build_policy_json`. If positions are disabled, the query fails with a precondition error rather than silently degrading to a term query. Keep positions enabled unless you have a strong storage reason not to support phrase search.

## Narrow search with path prefixes and metadata filters

A full-text query can also use `path_prefix` and `metadata_filters_json`. These filters are query-time constraints over the objects represented by matching text hits.

```bash
anvil --profile acme index query documents tutorial_text \
  --text 'payment terms' \
  --path-prefix tutorial/articles/billing/ \
  --metadata-filters-json '{"workflow_state":"published","audience":"customer"}' \
  --limit 10
```

The path prefix checks object keys. It can narrow to a product area, customer, content family, or import batch, but it cannot reach outside the objects selected into the index at build time.

The metadata filter is exact JSON equality against object user metadata. Both fields above must match exactly. This is useful for labels such as `workflow_state`, `audience`, `document_type`, or `retention_class`. It is not a range language and it does not coerce types. If your metadata value is the JSON string `"20"`, it does not equal the JSON number `20`.

This command is illustrative unless your objects already carry user metadata, because the current public CLI upload helper cannot set `user_metadata_json`. Use the public API/Rust client for metadata-rich uploads.

## Keep authorisation visible in the design

The create command used `--authorization-mode inherit_object`. In that mode, full-text search does not expose every indexed hit to every index reader. The query path filters by object visibility: a caller needs object read authority for `documents/<object-key>` or a matching relationship-authorisation object reader relation.

That matters because search results can leak sensitive information even without object bodies. Object keys, scores, matched term counts, and the existence of a result are all data. Do not switch to `index_only` or `public` unless the indexed text and keys are intentionally visible to every principal that has `index:read` on the bucket.

Authorisation also affects debugging. If a query returns fewer hits than expected, check the source objects, extractor diagnostics, index build status, and the caller's object visibility. A missing hit is not automatically an indexing bug.

## Understand lag and catch-up limitations

Full-text indexes are derived from object metadata and object bodies. An object write can commit before the full-text builder has tokenised that object and published a new segment. User interfaces should handle this as normal indexing lag rather than promising instant search results for freshly uploaded content.

The `QueryIndexRequest` type has `require_caught_up_to_watch_cursor`, and the CLI exposes `--require-caught-up-to-watch-cursor`, but current direct full-text query responses do not enforce or report meaningful object-cursor catch-up. The response currently reports caught-up status as true with zero cursor fields for full-text search. Use this flag for metadata-backed and typed JSON correctness today; do not rely on it as proof of full-text freshness until the implementation catches up.

For full-text search today, freshness checks are operational: watch object changes, watch index partition progress if your worker exposes the partition id, inspect diagnostics, and design the product to display "indexing" or retry later when a document has just been written.

## Inspect diagnostics

Diagnostics tell you why expected text might not be present in the index:

```bash
anvil --profile acme index diagnostics documents tutorial_text \
  --severity warning \
  --page-size 20
```

This calls `IndexService.ListIndexDiagnostics`. A successful response proves the caller has `index:read` on `documents` and that Anvil could read diagnostic records for the bucket/index filter. The CLI prints `cursor`, `severity`, `code`, and `message`.

Common full-text diagnostics include non-UTF-8 bodies for UTF-8 extraction, invalid JSON for `json_pointer`, missing JSON Pointer values, missing metadata fields, unsupported extractor sources, and empty extracted text. No output means no matching diagnostics were returned in that page; it does not prove every object was selected, indexed, or visible to the querying principal.

## What to take forward

Use full-text search for words and phrases, not for boolean query languages or structured ranges. Keep `selector_json` focused so the index covers the source records you intended. Use `extractor_json` to name the text fields clearly. Keep `positions` enabled if users need phrase search. Use path prefixes and metadata filters for hard narrowing, and rely on `inherit_object` unless the search corpus is deliberately visible to every bucket-level index reader. Treat full-text freshness as derived-data lag today, and use diagnostics plus watch-driven operations to understand when text extraction has caught up.

## Success and failure cues

A full-text index definition proves the tokenizer and extractor contract was accepted. Search results prove the query terms matched materialised text and survived authorisation filtering; they do not prove the corpus is fully fresh. Empty results usually come from selector mismatch, missing extracted text, non-UTF-8 or invalid JSON input, phrase queries without stored positions, derived lag, or object visibility. Diagnostics tell you which source objects failed extraction.

## Where to go next

Read [Vector Search](/tutorials/vector-search/) if exact word matching is not enough, then [Hybrid Search](/tutorials/hybrid-search/) when the product needs both lexical and semantic ranking. Keep [Watches](/tutorials/watches/) nearby for the derived-data side of search freshness, and use diagnostics before assuming a missing hit is a query bug.
