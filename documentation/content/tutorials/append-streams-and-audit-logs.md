---
title: Append Streams and Audit Logs
description: Write ordered event records for audit trails, histories, and replayable workers without turning every event into a mutable object.
---

# Append Streams and Audit Logs

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/) and [Watches](/tutorials/watches/). Objects are the right shape for current state: a profile, a document, a manifest, or a JSON record that can be replaced by a new version. Append streams are the right shape for ordered history: "this happened, then this happened, then this happened".

Applications should use the public Object API directly. The `anvil stream` commands below are manual helpers over the same `ObjectService` methods: `CreateAppendStream`, `AppendStreamRecord`, `ReadAppendStream`, `TailAppendStream`, and `SealAppendStreamSegment`. The CLI reference is [Public CLI](/reference/public-cli/), and the scope model is described in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

Use append streams for durable event trails, delivery attempts, background job histories, integration callbacks, product timelines, and application-owned audit logs. Do not use them as a replacement for Anvil's object watch streams. Watches tell consumers that Anvil state changed. Append streams are application data that your tenant writes, reads, replays, and indexes.

By the end of the page, you should be able to tell when an ordered append stream is a better fit than a mutable object, what identity a stream record carries, how stream reads differ from Anvil's service-recorded tenant audit log, and which public scopes a writer or replay worker needs. The examples stay small, but the same pattern is the foundation for product audit trails, delivery histories, job logs, and replayable integration workers.

## What an append stream contains

An append stream belongs to a bucket and has two identifiers. The `stream_key` is the application name, such as `audits/customer-acme` or `jobs/email-delivery/message-42`. The `stream_id` is the UUID returned when the stream is created. Current APIs require both. Treat `bucket_name`, `stream_key`, and `stream_id` together as the stream identity.

Each append creates one record. Records are ordered by `record_sequence`, starting at `1` and increasing inside that one stream. Sequence order is not global across the bucket. If two different streams both have record `7`, those are unrelated records in different histories.

A record's durable identity is therefore `bucket_name`, `stream_key`, `stream_id`, and `record_sequence`. A record also has a payload, optional `content_type`, optional `user_metadata_json`, a `payload_hash`, a `payload_size`, and a creation time. The payload is the event body. Metadata is for labels you want to inspect or index without parsing every payload. The response also includes a mutation id, record hash, authorisation revision, and watch cursor through the API, although the current CLI prints only a small subset.

For audit-style records, design the payload as an event contract rather than a dump of application state. Include stable names such as `event`, `actor`, `subject`, `object`, `request_id`, and the business time you care about. Put sensitive payloads in the stream only if every reader authorised for that stream should see them.

## Application audit logs and tenant audit logs are different

An append stream can be your application's audit log. For example, a document service can append `document_uploaded`, `document_shared`, and `retention_changed` records under `audits/customer-acme`. Those events are tenant data that your application owns.

Anvil also has a tenant audit service for service-recorded audit events. The public command is `anvil audit list`. That service is separate from your application append streams: it lists audit events that Anvil services explicitly recorded, such as some tenant control-plane mutations. Current append stream create, append, and seal operations do not automatically appear as tenant audit events. If your product needs an audit trail for domain events, write those events to an append stream yourself.

## Prerequisites and current tutorial limits

The commands in this page are valid current public CLI shapes, but they depend on the same local tutorial prerequisites as earlier pages: a configured profile, a `documents` bucket, a token for the tenant, object-write permission for the stream key, and object-read or object-list permission for reading.

Use narrow grants rather than broad ones. The service checks and CLI limitations today are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Create, append to, or seal this stream through the API | `object:write` | `documents/audits/customer-acme` |
| Read or tail append stream records | `object:read` or `object:list` | `documents` |
| Create an append-record typed index | `index:create` | `documents/audit_events` |
| Query or inspect index diagnostics | `index:read` | `documents` |

There is an important CLI helper gap. The current `anvil stream create`, `append`, and `seal-segment` helpers build a `NativeMutationContext` by calling `ListBuckets`, and the current bucket list implementation checks `bucket:list` on `*`. A direct API client that already knows the bucket id can construct the mutation context itself, but the public CLI helper is not yet least-privilege for write-only stream operators.

There is also no current public CLI command to list append streams or recover a lost `stream_id` from a `stream_key`. Store the returned `stream_id` in your application state, deployment secret store, or a manifest object.

## Create a stream

Create an append stream for Acme's document audit trail:

```bash
anvil --profile acme stream create documents audits/customer-acme
```

The CLI calls `ObjectService.CreateAppendStream` and prints a line like:

```text
stream_id=8d0a1cf2-5f25-4fd8-8fd8-7acb83e66ad6 version_id=8d0a1cf2-5f25-4fd8-8fd8-7acb83e66ad6
```

A successful response proves that the caller authenticated, the CLI could resolve the bucket id, the bucket exists in the serving region, the `stream_key` is a valid object key and not in a reserved namespace, and the caller had `object:write` for `documents/audits/customer-acme`. It also proves Anvil wrote stream metadata and returned a UUID for this logical stream.

It does not prove that any records exist, that a consumer is reading the stream, that the stream key is unique forever, or that future appends will be authorised. The current implementation can create more than one stream with the same `stream_key`; the `stream_id` distinguishes them.

Save the stream id before continuing:

```bash
export STREAM_ID='paste-stream-id-here'
```

## Append records

Append a first audit event:

```bash
anvil --profile acme stream append documents audits/customer-acme "$STREAM_ID" \
  '{"event":"document_uploaded","document_id":"doc-001","actor":"user-17"}' \
  --content-type application/json \
  --user-metadata-json '{"actor":"user-17","kind":"audit"}'
```

Append a second event to the same stream:

```bash
anvil --profile acme stream append documents audits/customer-acme "$STREAM_ID" \
  '{"event":"document_shared","document_id":"doc-001","actor":"user-17","subject":"user-22"}' \
  --content-type application/json \
  --user-metadata-json '{"actor":"user-17","kind":"audit"}'
```

Each command calls `ObjectService.AppendStreamRecord`. A successful response prints `sequence=<n> hash=<payload_hash>`. That proves the stream id was a valid UUID, the stream exists under the supplied key, the caller still has `object:write` for `documents/audits/customer-acme`, `user_metadata_json` parsed as a JSON object, the payload blob was stored, and Anvil appended a metadata record with the next sequence number.

It does not prove that the JSON payload matches your event schema. Anvil stores bytes and metadata; your application owns semantic validation. It also does not prove that a derived index has processed the record. Indexing happens later.

The API supports native idempotency through `NativeMutationContext.idempotency_key`. For append records, the idempotency target includes the operation, bucket, stream key, stream id, and payload hash; reusing the same idempotency key for a different target fails. The current public CLI generates a fresh idempotency key for each invocation, so rerunning the same `anvil stream append` command appends a new record rather than replaying the previous response. Use the API directly when retry-safe append idempotency matters.

## Read and replay records

Read from the beginning of the stream:

```bash
anvil --profile acme stream read documents audits/customer-acme "$STREAM_ID" \
  --after-sequence 0 \
  --limit 100 \
  --include-payload
```

`after_sequence` is exclusive. `--after-sequence 0` returns records with sequence greater than `0`, so it starts at sequence `1`. A successful read proves the caller authenticated, the stream id and key identify a stream in the bucket, and the caller has the current read permission that the service checks for append streams. With `--include-payload`, it also proves the payload blobs for returned records can be read from storage.

The CLI prints `record_sequence`, `content_type`, and payload text when `--include-payload` is set. Without payloads, it prints `record_sequence`, `payload_size`, and `payload_hash`. The API response also includes `next_after_sequence` and `is_end`, but the current CLI does not print those fields.

A replaying consumer should persist the last sequence only after its side effect is durable. For example, if a worker updates a search projection from record `12`, store checkpoint `12` only after the projection commit succeeds. If the worker crashes before storing the checkpoint, it can restart with `--after-sequence 11` or the last committed value and process idempotently.

There is no dedicated append-stream checkpoint API today. Store checkpoints in your application database, in an Anvil object protected with compare-and-swap, or in task-lease checkpoint fields if your worker is already coordinated by Anvil leases. Do not use segment sealing as a consumer checkpoint; sealing is storage/proof metadata, not proof that your consumer has processed records.

## Tail for local observation

Tail polls the same stream and emits records as they appear:

```bash
anvil --profile acme stream tail documents audits/customer-acme "$STREAM_ID" \
  --from-sequence 1 \
  --poll-interval-ms 1000
```

`from_sequence` is inclusive in the tail helper. Passing `1` starts at record `1`; passing the last processed sequence plus one resumes from the next record. The command is useful for local debugging, demonstrations, and simple operational monitors.

A long-running production consumer should still persist checkpoints and be able to fall back to `ReadAppendStream`. Tail is implemented by polling reads in a loop, not by a retained server-side subscription with delivery guarantees. The current CLI tail output prints sequence and payload hash; it does not print payload or metadata even though the API can include payloads in `TailAppendStreamResponse` when requested.

## Seal a segment without closing the stream

Seal the current stream contents:

```bash
anvil --profile acme stream seal-segment documents audits/customer-acme "$STREAM_ID"
```

The command calls `ObjectService.SealAppendStreamSegment` and prints `records=<count> segment_hash=<hash>`. A successful response proves the stream exists, it has at least one record, the caller has `object:write` for the stream key, and Anvil computed a hash over the records it considered for the seal.

Sealing is not logical stream closure. Current append code does not reject later appends to a sealed stream. A later append can add sequence `n + 1`, and a later seal can compute another hash for the then-current record set. Treat the current seal operation as checkpoint/proof metadata over the stream's records, not as a way to say "this audit log is complete".

The current implementation also does not expose a public list of sealed segments. The response gives the hash and count for this seal operation. Store those values yourself if your compliance process needs to retain evidence that a particular checkpoint was sealed.

## Build queryable summaries from append records

Append streams are efficient for replay, but replaying every payload is not how you should build dashboards. For queryable audit summaries, create a derived index over append records. Current typed JSON indexes support `source_kind: "append_record"` and extract fields from the stream key, record sequence, content type, payload JSON, and user metadata.

The index definition has the same four JSON roles as the typed-query tutorial. `selector_json` chooses append stream records before extraction; here it selects stream keys starting with `audits/` and records whose content type is exactly `application/json`. `extractor_json` is `{}` for current `typed_json` indexes because typed field definitions live in `build_policy_json`. The build policy uses `source_kind: "append_record"` and defines typed fields from append-record attributes and payload JSON. Query-time `typed_predicates_json` and `typed_order_json` ask questions of rows after the builder has materialised them.

Create an index over audit stream records:

```bash
anvil --profile acme index create documents audit_events typed_json \
  --selector-json '{"prefix":"audits/","content_type":"application/json"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"append_record","fields":[{"name":"stream","extractor":"append_stream_key","required":true},{"name":"sequence","extractor":"append_record_sequence","required":true},{"name":"created_at","extractor":"created_at","required":true},{"name":"event","extractor":"/event","required":true},{"name":"actor","extractor":"append_user_metadata_json_pointer:/actor","required":false}],"default_order":[{"field":"created_at","direction":"asc"},{"field":"sequence","direction":"asc"}]}' \
  --authorization-mode inherit_object
```

A successful response proves the caller had `index:create` on `documents/audit_events`, the JSON definition parsed, the source kind is valid, and the field extractors are accepted. It does not prove the builder has materialised rows or that every append payload is valid JSON. Invalid payloads, missing required fields, and unavailable blobs are reported as index diagnostics.

Query that index for one actor:

```bash
anvil --profile acme index query documents audit_events \
  --typed-predicates-json '[{"field":"actor","op":"eq","value":"user-17"}]' \
  --typed-order-json '[{"field":"created_at","direction":"asc"},{"field":"sequence","direction":"asc"}]' \
  --limit 20
```

A successful query proves an append-record typed segment is available, the predicate and order arrays are valid, and every returned row passed the index authorisation mode. With `inherit_object`, the stream key is treated like the object key for visibility checks, so a caller should not see rows for streams it cannot read.

Typed JSON queries can use `require_caught_up_to_watch_cursor` for append-record indexes, but the current `anvil stream append` command does not print the append API's `watch_cursor`. If your application calls the API directly, capture `AppendStreamRecordResponse.watch_cursor` and pass it to `QueryIndexRequest.require_caught_up_to_watch_cursor` when a query must prove that the derived index has processed that append source cursor.

Inspect index diagnostics when expected rows are missing:

```bash
anvil --profile acme index diagnostics documents audit_events \
  --page-size 20
```

This command proves the caller has `index:read` on `documents` and that Anvil can read diagnostics for the index. It does not prove there are no stream records. No output only means no diagnostics were returned in that page.

## Inspect service-recorded tenant audit events

List tenant audit events recorded by Anvil services:

```bash
anvil --profile acme audit list --page-size 20
```

You can narrow by principal, resource, or action:

```bash
anvil --profile acme audit list \
  --principal docs-writer \
  --resource documents/audits/customer-acme \
  --page-size 20
```

A successful audit list proves the token is authenticated for the tenant and the audit service can read the tenant audit stream. It does not prove your application append stream has records, and it does not list private admin-plane audit events. It also does not currently prove a separate audit-read public policy scope, because the tenant audit list implementation authenticates the caller but does not enforce a distinct public policy action.

The CLI prints `created_at`, `principal_id`, `action`, `resource_id`, and
`audit_event_id`. If there are more results, it prints
`next_page_token=<token>`. Pass that opaque token back with `--page-token` to
continue the same audit query. The token is bound to the tenant, caller,
filters, page size, and collection revision, so changing the request invalidates
the token.

## Operational checks and common failures

Use `stream read` first when a consumer appears stuck. If the record is visible there, the stream append succeeded and the problem is in the consumer, derived index, or authorisation. If read fails with `Append stream not found`, check both the `stream_key` and the `stream_id`; one without the other is not enough.

Use `payload_hash` to verify payload integrity across retries, logs, and external systems. Use `record_sequence` for replay order. Use the API's mutation id and record hash when you need lower-level mutation evidence.

Common write failures are invalid stream keys, reserved `_anvil/` keys, missing `object:write` on the stream resource, invalid `stream_id`, non-object `user_metadata_json`, and stale or missing native mutation context fields in direct API calls. Sealing an empty stream fails with `Append stream has no records to seal`.

Common read failures are missing coarse read/list scope for the bucket, a mismatched stream id, or payload blobs that cannot be loaded when `include_payload` is true. For derived indexes, also check `TypedJsonIndexNotMaterialised`, `IndexLagging`, and typed JSON diagnostics.

## What to take forward

Use append streams when order and replay matter. Store the stream id, make every payload a stable event contract, checkpoint consumers by last processed sequence, and use typed JSON indexes for queryable summaries. Treat sealing as checkpoint evidence rather than stream closure. Keep application audit streams separate from Anvil's service-recorded tenant audit log, and design authorisation deliberately because stream reads, derived rows, and audit listings expose history even when they do not expose mutable object state.

## Success and failure cues

A stream create returns the `stream_id` you must keep. An append returns a monotonically increasing record sequence inside that stream. A replay reader should be able to start from a saved sequence and process records idempotently. Common failures are losing the stream id, confusing application append streams with tenant audit events, sealing an empty stream, reading with insufficient bucket read/list scope, or indexing append payloads with a typed-field definition that does not match the event JSON.

## Where to go next

Read [Watches](/tutorials/watches/) if the stream consumer also maintains derived state from Anvil changes, and [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/) if several workers may compete for the same stream or shard. For service-recorded events and repairs, use [Repair and Diagnostics](/tutorials/repair-and-diagnostics/) rather than treating application append streams as the operator audit log.
