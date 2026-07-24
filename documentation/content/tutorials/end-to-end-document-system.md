---
title: End-to-End Document System
description: Combine Anvil tenants, objects, metadata, links, indexes, watches, streams, leases, search, repair, and optional public delivery into one document workflow.
---

# End-to-End Document System

This capstone ties the tutorial series into one realistic application shape: a tenant-owned document system. It stores canonical JSON documents, records sharing relationships, keeps stable aliases, builds query and search indexes, writes audit history, watches for changes, coordinates background workers, and exposes a deliberately public copy only when the product calls for it.

The goal is decision-making, not a copy-and-paste deployment script. Production code should call the public Anvil APIs or a matching client library directly. The `anvil` CLI commands below are smoke-test helpers over those APIs, useful for proving a shape manually. The private admin CLI is not part of tenant document publishing; it remains an operator control-plane helper.

This page builds on [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Buckets and Objects](/tutorials/buckets-and-objects/), [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), [Authorisation](/tutorials/authorisation/), [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/), [Full-Text Search](/tutorials/full-text-search/), [Vector Search](/tutorials/vector-search/), [Hybrid Search](/tutorials/hybrid-search/), [Watches](/tutorials/watches/), [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/), [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/), [Public Access](/tutorials/public-access/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/). Keep [Public CLI](/reference/public-cli/), [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) nearby for exact reference syntax.

This chapter is the integration map for the tutorial series. It uses a single document system to show where each Anvil primitive belongs: objects for the canonical record, tuples for end-user access, links for stable names, indexes for derived views, watches for maintenance, append streams for history, leases for workers, and diagnostics for incident review.

## Start with the current tutorial limits

The earlier limits still apply. A local `documents` bucket may not exist until the `local` region is active and writable. The current public CLI upload helpers build mutation contexts by listing buckets, so they can hit the least-privilege `ListBuckets` gap described in [Buckets and Objects](/tutorials/buckets-and-objects/). The CLI `object put` helper also does not expose `content_type`, `user_metadata_json`, version preconditions, structured write preconditions, or mutation batches.

That means the commands in this page are valid current command shapes, but some are illustrative until your local region, grants, upload path, and index builder are ready. The production pattern is still clear: use the API for correctness-sensitive writes, metadata-rich uploads, idempotent retries, and fenced mutation batches; use the CLI to smoke-test a flow by hand.

## Choose the application boundaries

A document system has several identities. The tenant owner or platform automation creates service principals. An ingest service writes canonical documents. A search service creates and queries indexes. A sharing service writes relationship tuples. A worker consumes watches and appends audit records. These services should not share one client secret just because they are in the same tenant.

Create a couple of tenant-owned app credentials from the `acme` profile if the earlier credentials page has granted `app:create`:

```bash
anvil --profile acme app create docs-ingest > /tmp/docs-ingest-app.txt
anvil --profile acme app create docs-search > /tmp/docs-search-app.txt
chmod 600 /tmp/docs-ingest-app.txt /tmp/docs-search-app.txt
```

Each command calls the public Auth API to create an app credential inside the current tenant. A successful response proves the caller can manage tenant apps and gives you a separate client id and client secret for that service. It does not grant data access to the new app. Data access comes from public policy scopes and, for end-user sharing, relationship authorisation.

For a one-document smoke test, delegate exact resources rather than broad bucket authority:

```bash
anvil --profile acme auth grant docs-ingest object:write documents/library/acme/doc-001.json
anvil --profile acme auth grant docs-ingest object:read documents/library/acme/doc-001.json
anvil --profile acme auth grant docs-ingest object:write documents/audits/doc-001

anvil --profile acme auth grant docs-search index:read documents
anvil --profile acme auth grant docs-search object:read documents/library/acme/doc-001.json
```

The first group lets `docs-ingest` write one canonical document and append to that document's audit stream. The second group lets `docs-search` query indexes in the `documents` bucket and see this one object when an `inherit_object` index returns it. These grants do not make either app a system administrator, do not let them change mesh topology, and do not grant relationship tuples for product users.

For a real service, choose narrow prefixes or exact document keys that match your product boundary. Do not grant a write service the whole bucket unless it genuinely owns the whole bucket.

The rest of this page assumes the acting profile has the corresponding exact scopes for the operation being demonstrated: `index:create` for each index name such as `documents/docs_workflow`, `index:read` for `documents`, `authz:tuple_write` and `authz:check` for the `document/...#...` relations being used, `object:read` or `object:list` for stream reads, and `coordination:lease_write` for `task_lease/doc-preview-doc-001`. Those are public policy scopes. They let a tenant principal call public APIs; they do not replace relationship tuples and they do not grant private admin authority.

## Store the canonical document as an object

Use one object as the source of truth for a document's current business state. Keep stable identifiers and workflow fields in the body so indexes can be rebuilt from the object later.

```bash
cat > doc-001.json <<'JSON'
{
  "schema": "acme.document.v1",
  "document_id": "doc-001",
  "customer": {"id": "acme", "name": "Acme Ltd"},
  "title": "Acme Renewal Contract",
  "summary": "Contract renewal notice and payment terms for Acme.",
  "body": "The renewal notice must be sent thirty days before the renewal date. Payment terms remain net thirty.",
  "state": "review",
  "renewal_at": "2026-08-01T00:00:00Z",
  "updated_at": "2026-07-07T12:00:00Z",
  "embedding": [0.10, 0.20, 0.30, 0.40]
}
JSON
```

This local file is not Anvil data yet. It is the body that an application will upload to `documents/library/acme/doc-001.json`.

The public CLI upload shape is:

```bash
anvil --profile acme object put doc-001.json s3://documents/library/acme/doc-001.json
```

A successful upload proves the bucket exists, placement is writable, the caller can write that object key, and the public Object API committed a current version. It does not prove the object has `content_type = application/json`, user metadata, a stable idempotency key, or a compare-and-swap precondition, because the current CLI does not expose those fields.

Production ingest should call `ObjectService.PutObject` directly. The first stream frame is `ObjectMetadata`; use it to set `content_type`, compact user metadata, a mutation context, and an idempotency key. For this document, useful user metadata might be:

```json
{
  "domain": "contracts",
  "customer_id": "acme",
  "document_type": "contract",
  "workflow_state": "review",
  "retention_class": "legal-7y"
}
```

Those labels are not the canonical document. They are protected operational metadata for listing, filtering, routing, and auditing. The body remains the source of truth.

## Keep end-user access in relationship tuples

Public policy scopes authorise service principals to call Anvil APIs. They are not your product's user-sharing model. For product users, use relationship authorisation tuples in the tenant authz realm.

Assume the [Authorisation](/tutorials/authorisation/) tutorial has stored and bound a `document` namespace schema in the default realm, with relations such as `owner` and `viewer`. Grant one owner tuple for this document:

```bash
anvil --profile acme authz tuple write \
  document \
  documents/library/acme/doc-001.json \
  owner \
  user \
  user-17 \
  write \
  --reason 'make user-17 owner of doc-001'
```

This calls `AuthService.WriteAuthzTuple`. A successful response proves the caller had the public policy scope to write that tuple, and it advances the tenant tuple log. It does not give `user-17` an Anvil bearer token and it does not grant the writing service any new object API scopes.

Check effective access through the same relationship model:

```bash
anvil --profile acme authz check \
  document \
  documents/library/acme/doc-001.json \
  viewer \
  user \
  user-17
```

A result of `allowed=true` proves the current tuple/schema state allows that relationship. It does not prove the caller can read the object through the Object API; the object read path also evaluates public policy, built-in object reader relations, and public-read policy depending on the request.

## Carry versions through application writes

When an editor opens a document, the application should capture the `version_id` returned by `HeadObject` or `GetObject`. A later save should say, "write this update only if the current pointer is still the version I edited".

The CLI can show basic metadata:

```bash
anvil --profile acme object head s3://documents/library/acme/doc-001.json
```

That proves the caller can read object metadata. The current CLI prints ETag, size, and last-modified time, but not `version_id`, even though the API response contains it. Use the API or client library for any workflow that needs safe saves.

At the API level, set `NativeMutationContext.precondition` on `PutObject` to one of the supported current-pointer forms. For an edit based on a known version, use:

```text
NativeMutationContext.precondition = "version:<version_id returned by HeadObject>"
```

If another writer has already moved the current pointer, Anvil rejects the write with a precondition failure. That is the correct behaviour. The application should reload, merge, ask the user, or abandon the stale edit; it should not blindly retry as last-writer-wins.

For multi-step worker updates, use `ObjectService.MutationBatch` with a structured `WritePrecondition`. Current batch operations include `put_object`, `patch_json_object`, `delete_object`, `append_stream_record`, task-lease checkpoint/commit, and manifest compare-and-swap. The current CLI does not expose `MutationBatch`, so this is an API-only correctness surface.

## Add stable names with object links

A document often has more than one name. The canonical key might be an immutable product identity, while users bookmark a slug or a "latest approved" path. Use links for those aliases instead of copying the body.

```bash
anvil --profile acme object link create \
  s3://documents/slugs/acme-renewal-contract.json \
  s3://documents/library/acme/doc-001.json \
  --resolution follow
```

This creates a same-bucket link. A successful command proves the caller can write the link key, the target exists unless `--allow-dangling` is used, and Anvil created a link descriptor with a generation. It does not copy `doc-001.json`; the link is metadata that points at the target key.

When moving a link, use the generation as a compare-and-swap token. The next command is the shape you would use after `documents/library/acme/doc-002.json` exists; if the target does not exist, Anvil rejects the update unless you deliberately use `--allow-dangling`.

```bash
anvil --profile acme object link update \
  s3://documents/slugs/acme-renewal-contract.json \
  s3://documents/library/acme/doc-002.json \
  --expected-generation 1 \
  --resolution follow
```

This update succeeds only if the link is still at generation `1`. If another publisher moved the slug first, the command fails and you should read the link again before deciding whether to move it. The current CLI creates live links to target keys; it does not expose a `--target-version` flag for pinned links. Use the API for pinned version aliases.

## Build structured and text indexes

The object is the source of truth; indexes are maintained shortcuts. Before creating them, separate the JSON roles. `selector_json` chooses source document objects under `library/acme/`. `extractor_json` is empty for the typed index because typed fields are declared in `build_policy_json`; for full-text and hybrid indexes it names the text fields. `build_policy_json` defines typed fields, tokenisation, vector extraction, or ANN settings. Query-time JSON such as `typed_predicates_json` and `typed_order_json` asks questions of rows already built from those definitions.

Create a typed JSON index for dashboards and queues:

```bash
anvil --profile acme index create documents docs_workflow typed_json \
  --selector-json '{"prefix":"library/acme/"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"document_id","extractor":"/document_id","required":true},{"name":"customer_id","extractor":"/customer/id","required":true},{"name":"state","extractor":"/state","required":true},{"name":"renewal_at","extractor":"/renewal_at","required":false},{"name":"updated_at","extractor":"/updated_at","required":true}],"default_order":[{"field":"updated_at","direction":"desc"},{"field":"document_id","direction":"asc"}]}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex` with kind `typed_json`. A successful response proves the caller had `index:create` on `documents/docs_workflow`, the JSON definition parsed, and Anvil stored an enabled index definition. It does not prove any rows are materialised yet. The builder still has to read source objects, extract fields, publish a segment, and record diagnostics for any bad body.

Create a full-text index for word and phrase search:

```bash
anvil --profile acme index create documents docs_text full_text \
  --selector-json '{"prefix":"library/acme/"}' \
  --extractor-json '{"fields":[{"source":"object_key"},{"source":"json_pointer","pointer":"/title"},{"source":"json_pointer","pointer":"/summary"},{"source":"json_pointer","pointer":"/body"}]}' \
  --build-policy-json '{"positions":true,"language":"simple","max_token_chars":128,"lowercase":true,"normalize_nfkc":true,"record_original_ranges":true}' \
  --authorization-mode inherit_object
```

This proves the full-text definition is accepted and build work is queued. It does not prove boolean query syntax exists. Current `query_text` is tokenised text; operators such as `AND`, `OR`, and parentheses are treated as ordinary tokens by the direct CLI path.

If your ingest pipeline writes production embeddings into the document body, you can also create one hybrid index. The example uses a four-dimensional caller-supplied vector only to keep the command readable; production vectors must come from a real embedding model and must keep model, dimension, modality, metric, and normalisation consistent.

```bash
anvil --profile acme index create documents docs_hybrid hybrid \
  --selector-json '{"prefix":"library/acme/"}' \
  --extractor-json '{"text":{"fields":[{"source":"object_key"},{"source":"json_pointer","pointer":"/title"},{"source":"json_pointer","pointer":"/summary"},{"source":"json_pointer","pointer":"/body"}]}}' \
  --build-policy-json '{"full_text":{"positions":true,"language":"simple","max_token_chars":128,"lowercase":true,"normalize_nfkc":true,"record_original_ranges":true},"vector":{"schema":"anvil.index.vector_definition.v1","source":{"kind":"object_current"},"extractor":{"kind":"object_body_json_vector","json_pointer":"/embedding"},"embedding":{"provider":"caller_supplied","model":"tutorial-embedding-v1","dimension":4,"modality":"text","normalisation":"unit_l2","chunking":{"strategy":"whole_object"}},"ann":{"algorithm":"hnsw","metric":"cosine","m":32,"ef_construction":200,"ef_search_default":80}}}' \
  --authorization-mode inherit_object
```

This stores one index definition that has both a text side and a vector side. It does not call an embedding provider, does not generate a query vector for you, and does not make the deterministic `test_only` provider production-safe. If the source object lacks a valid vector, the builder records diagnostics.

## Query through derived views, not source scans

A workflow dashboard should ask the typed index:

```bash
anvil --profile acme index query documents docs_workflow \
  --path-prefix library/acme/ \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"},{"field":"state","op":"eq","value":"review"}]' \
  --typed-order-json '[{"field":"updated_at","direction":"desc"},{"field":"document_id","direction":"asc"}]' \
  --limit 20
```

This proves the query JSON arrays parse, a typed segment is available, and matching visible rows can be ordered. It does not prove there are no other matching documents if the index is lagging or the caller lacks object visibility under `inherit_object`.

A search box can start with full text:

```bash
anvil --profile acme index query documents docs_text \
  --text 'renewal notice' \
  --phrase \
  --limit 10
```

This proves the text segment is available and phrase positions were stored. It does not support boolean search syntax or snippets today.

If you built the hybrid index and have a query vector from the same embedding model, query both signals together:

```bash
anvil --profile acme index query documents docs_hybrid \
  --text 'renewal notice' \
  --vector 0.11,0.19,0.31,0.39 \
  --limit 10
```

This proves both the text and vector segments are usable for this index and that the query vector dimension matches. Current hybrid weights are fixed by the implementation; there is no CLI flag or supported build-policy field for custom weights.

## Use watches for maintenance, not polling scans

Watches let background services react to source changes without repeatedly listing the whole bucket. A worker that maintains a preview, cache, or external notification queue can watch the document prefix:

```bash
anvil --profile acme watch prefix documents library/acme/ --after-cursor 0
```

This calls `ObjectService.WatchPrefix`. A successful stream proves the caller can watch that bucket/prefix and that Anvil is emitting object change events with cursors. It does not make your worker exactly-once. Your worker must store the last processed cursor only after its side effects are durable.

When an index query must prove it has processed a recent object watch cursor, pass that cursor to typed or metadata-backed queries:

```bash
anvil --profile acme index query documents docs_workflow \
  --typed-predicates-json '[{"field":"document_id","op":"eq","value":"doc-001"}]' \
  --typed-order-json '[{"field":"updated_at","direction":"desc"}]' \
  --require-caught-up-to-watch-cursor "$LAST_OBJECT_CURSOR" \
  --limit 1
```

For typed JSON and metadata-backed paths, a successful response proves the segment used by the query has caught up to the required source cursor. Current direct full-text, vector, and hybrid query paths do not report meaningful watch-cursor freshness, so treat search freshness there as operational lag and use retries, diagnostics, and user-facing "indexing" states.

## Record document history in an append stream

The mutable object tells you current state. An append stream tells you what happened. Create one stream per document or per document family, depending on your replay needs:

```bash
anvil --profile acme stream create documents audits/doc-001
```

The command calls `ObjectService.CreateAppendStream` and prints a `stream_id`. Save it; current CLI has no command to recover a lost stream id from a stream key.

```bash
export DOC_AUDIT_STREAM_ID='paste-stream-id-here'

anvil --profile acme stream append documents audits/doc-001 "$DOC_AUDIT_STREAM_ID" \
  '{"event":"document_uploaded","document_id":"doc-001","actor":"user-17","request_id":"req-doc-001-create"}' \
  --content-type application/json \
  --user-metadata-json '{"document_id":"doc-001","actor":"user-17","kind":"audit"}'
```

A successful append prints `sequence` and `hash`. It proves the stream exists, the caller can write the stream key, the metadata JSON is an object, and Anvil appended the next ordered record. It does not prove the payload matches your event schema, and the current CLI generates a fresh idempotency key each time. Use the API directly for retry-safe append idempotency.

Read the stream when replaying or diagnosing:

```bash
anvil --profile acme stream read documents audits/doc-001 "$DOC_AUDIT_STREAM_ID" \
  --after-sequence 0 \
  --limit 100 \
  --include-payload
```

This proves the caller can read the stream and, with `--include-payload`, fetch record payloads. It does not prove a consumer has processed those records. Consumers should persist their own checkpoints, often with task leases or a CAS-protected object.

## Coordinate workers with leases and fenced API writes

A watch consumer or index-maintenance worker should not rely on "I read the queue first" as ownership. Use a task lease to claim work and a fence token to make stale workers fail.

```bash
PARTITION_ID=0000000000000000000000000000000000000000000000000000000000000001

anvil --profile acme lease acquire doc-preview-doc-001 document-preview document "$PARTITION_ID" \
  --owner-label docs-preview-worker-1 \
  --source-cursor-low 0 \
  --source-cursor-high 0 \
  --ttl-nanos 30000000000
```

A successful acquire proves the caller has `coordination:lease_write` for `task_lease/doc-preview-doc-001`, no other active owner holds the task, and Anvil returned a fence token. It does not prove the worker can write preview objects or append audit records. Those permissions are separate.

After durable work, checkpoint the cursor you processed:

```bash
FENCE_TOKEN=1
anvil --profile acme lease checkpoint doc-preview-doc-001 "$FENCE_TOKEN" 125 0
```

This proves the same security owner still holds the lease, the fence token matches, the lease has not expired, and the checkpoint moved forward. It does not bind an object write to that checkpoint. For correctness-sensitive updates, production workers should call `ObjectService.MutationBatch` with `WritePrecondition.lease_fence` and object-version preconditions so stale workers are rejected at write time. The current CLI exposes lease lifecycle commands, but not fenced mutation batches.

## Keep public delivery separate from private documents

Most document systems should keep the working `documents` bucket private. If you need public static pages or downloads, publish a deliberately public copy to a separate bucket or prefix plan. In the current public CLI, public-read is a bucket-level setting, so a dedicated bucket is the safer production shape.

When region placement and narrow bucket grants are ready, the public bucket flow is:

```bash
anvil --profile acme bucket create public-docs local
anvil --profile acme bucket set-public public-docs --allow true
```

The first command creates a tenant bucket in a writable region. The second commits a bucket policy with public-read enabled. Together, they do not create DNS, TLS, a CDN, host aliases, or private admin exposure. They only prepare a bucket whose matching object reads can be served publicly through supported public surfaces.

Upload a static export or download artefact as ordinary objects:

```bash
cat > public-doc-001.html <<'HTML'
<!doctype html>
<title>Acme Renewal Contract</title>
<h1>Acme Renewal Contract</h1>
<p>This is the public summary copy, not the private working document.</p>
HTML

anvil --profile acme object put public-doc-001.html s3://public-docs/site/doc-001.html
```

This is still an authenticated write. Public-read does not permit anonymous uploads. The current CLI still does not set browser-friendly content types, so production publishing should use the API, S3 gateway, or a client path that can set `text/html` and cache metadata.

If host routing is configured with `PUBLIC_REGION_BASE_DOMAIN` and DNS is ready, create a tenant-owned host alias:

```bash
anvil --profile acme host-alias create docs.example.test public-docs \
  --region local \
  --prefix site/
```

A successful create proves Anvil stored a pending alias and returned a verification challenge. It does not prove DNS or TLS is configured, and it does not activate the alias. Use `anvil host-alias read` to get the generation and challenge, then `anvil host-alias verify` with the observed challenge when DNS is in place.

Do not expose the admin API to make public delivery work. Public API, S3, and static-hosting surfaces are data-plane surfaces; the admin API remains private.

## Diagnose and repair derived state

When a dashboard, search result, or listing looks wrong, start read-only. For the typed index:

```bash
anvil --profile acme diagnostics list documents docs_workflow --page-size 20
```

This calls the public index diagnostics path and prints cursor, severity, code, and message. A successful response proves the caller has `index:read` on `documents` and that Anvil can read diagnostics for that index. No output does not prove the index is perfect; it only means no matching diagnostics were returned in that page.

Run a narrow repair check before rebuilding:

```bash
anvil --profile acme repair run index documents docs_workflow
```

This checks derived index proof and segment consistency for the current tenant. It may write a repair finding if it sees a problem, but without `--rebuild` it does not rebuild the index. If the evidence says a rebuild is appropriate, run the same target with `--rebuild`:

```bash
anvil --profile acme repair run index documents docs_workflow --rebuild
```

A rebuild proves Anvil attempted to reconstruct the derived index from source data. It does not prove the user's original query is now correct. Rerun the failed query, carry a watch cursor where catch-up is supported, and inspect authorisation if expected hits are still missing.

For application audit history, use stream reads. For Anvil service-recorded tenant audit events, use:

```bash
anvil --profile acme audit list --page-size 20
```

This lists tenant audit events recorded by Anvil services. It does not list your application append stream events and does not list private admin-plane audit events.

## What the complete system looks like

The final shape has one source and several derived or supporting views. `documents/library/acme/doc-001.json` is the canonical current document. Relationship tuples say who can see or administer it. Object versions and API preconditions protect concurrent edits. Links give stable aliases without copying payloads. Typed, text, and optional hybrid indexes make the document discoverable. Watches drive workers without rescanning. Append streams record product history. Task leases and API-only fenced mutation batches stop stale workers from committing output. Public/static delivery uses separate public buckets or explicit host aliases when the document is meant for anonymous readers. Diagnostics and repair keep derived state honest without confusing it with source data.

The CLI can prove many pieces manually, but production code should hold the structured API responses: version ids, watch cursors, stream ids, repair finding ids, zookies, fence tokens, and idempotency keys. Those values are the difference between a demo that happens to work and a document system that remains explainable under retries, races, lag, and incident review.

## Success and failure cues

The complete design is healthy when every user-visible answer can be traced back to one source record and one authorisation reason. Search results trace to object versions and index diagnostics, document access traces to tuples or object-read scopes, aliases trace to link generations, history traces to append sequence numbers, and worker output traces to lease fences and idempotency keys. If a component cannot produce that evidence, treat it as a design gap before scaling it.

## Where to go next

Use this page as a design checklist. When you implement a real service, revisit the individual tutorials for exact API shapes and carry the durable values they name: version ids, zookies, watch cursors, stream ids, lease fence tokens, diagnostic cursors, and idempotency keys. For operations, turn the same model into deployment, security, backup, and incident runbooks under [Operators](/operators/overview/).
