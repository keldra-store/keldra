---
title: PersonalDB
description: Use Anvil as the witness for local-first SQLite changesets, heads, projections, and replay.
---

# PersonalDB

PersonalDB is for applications that already want SQLite on the client, but need a shared, authorised history when several devices or replicas synchronise. The application still reads and writes a local SQLite database for its working set. Anvil does not become a SQL server. Anvil witnesses SQLite changesets, records the accepted log, signs commit certificates, maintains heads, emits watches, and stores snapshot and projection state through CoreStore.

This page assumes you know ordinary SQLite tables and transactions. It teaches the Anvil side of the model. If you need the conceptual overview first, read [PersonalDB](/learn/personaldb/). The command reference is [Public CLI](/reference/public-cli/), and the permission strings are described in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/). Watches and checkpoints are introduced in [Watches](/tutorials/watches/). PersonalDB row metadata can also feed index definitions; the current index syntax is in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).

Applications should use the public `PersonalDbService` API or a generated/client library for production synchronisation. The `anvil personaldb` commands are manual helpers for creating groups, reading definitions, checking catch-up state, and tailing watch streams. They do not currently expose every request field needed for a complete production commit loop.

PersonalDB is not another object bucket. This page teaches the group, commit, witness, projection, catch-up, and watch vocabulary you need before building offline-capable or replica-driven features on Anvil's PersonalDB service.

## Prerequisites and ownership boundary

PersonalDB groups are tenant-owned public-plane resources. Use `anvil personaldb ...` or the public PersonalDB API for the group, changeset, catch-up, watch, and projection examples. Do not use `anvil-admin` to submit application changesets. Admin diagnostics and repair may inspect PersonalDB log-chain health later, but the committed data model belongs to the tenant.

A safe reader should know three values before building a replica loop: the database or group id, the last committed head or witness it has durably applied, and the projection cursor if it maintains a derived view. Without those values, a client cannot distinguish "I am caught up" from "I lost my place" after a restart.

## How PersonalDB differs from object storage

Object storage stores bytes at keys. You can version an object, attach metadata, and use preconditions to avoid overwriting another writer. That is the right model for documents, images, exports, snapshots, and other named blobs.

PersonalDB stores an ordered database history. A replica submits a SQLite changeset that says which rows changed. Anvil checks that the request extends the current head, that the schema and epochs match, that the authenticated principal is allowed to make the row mutations, and that the changeset bytes match the supplied hash. If accepted, Anvil appends one log entry, writes the changeset payload and certificate, advances the committed head, updates row metadata, and emits a watch event.

That means a PersonalDB write is not just an object write with a different content type. It has a base head, a proposed next log index, a witness certificate, row-level authorisation effects, and replay semantics. A client that falls behind catches up by replaying committed changesets, not by listing object keys.

## The working vocabulary

A **group** is one replicated SQLite data set inside a tenant. The group has a `database_id`, a schema hash, a genesis hash, membership and policy epochs, a committed head, and optional projection definitions. The current service validates that the database id is non-empty and does not contain `/` or `..`.

A **replica** is a device or process that keeps a local SQLite copy. Replicas should store their local head: the last accepted `log_index` and `log_hash` they have replayed. The CLI exposes a `--replica-id` on catch-up and `--leader-replica-id` on submit, but production clients also need to keep certificates and local retry state.

A **changeset** is a SQLite session changeset: row-level inserts, updates, and deletes encoded by SQLite. Anvil validates that the changeset is non-empty, within the configured size limit, decodable, and only touches tables present in the registered schema SQL.

A **commit** is the witness decision that accepts one changeset at the next log index. The response includes a commit certificate, the new committed head, and a PersonalDB watch cursor. The certificate records the previous hash, entry hash, changeset hash, policy and membership epochs, authorisation revision, witness node, and witness signature.

A **head** identifies the current accepted state of the group. At genesis the head has `log_index = 0` and `log_hash = genesis_hash`. Every accepted commit advances the head to the new log entry hash. Catch-up compares the replica's stored head with Anvil's chain.

A **projection** is another PersonalDB group maintained from one or more source groups. It is not a SQL view queried directly inside Anvil. A source commit can generate a projection changeset into a target group, and the target group has its own head, catch-up path, and watch events.

## Prerequisites and current limits

Use narrow PersonalDB grants. For read, commit, watch, projection, and row-effect checks, Anvil first accepts a matching public policy scope; if no scope matches, it evaluates the relationship kind and relation shown below.

| Operation | Public policy action | Resource checked today | Relationship kind and relation |
| --- | --- | --- | --- |
| Create a source group | `personaldb:create` | `tenant-<tenant_id>/<database_id>` | Public scope only for create |
| Read a group or catch up | `personaldb:read` | `tenant-<tenant_id>/<database_id>` | `personaldb#reader` |
| Submit a source changeset | `personaldb:commit` | `tenant-<tenant_id>/<database_id>` | `personaldb#committer` |
| Watch a group | `personaldb:watch` | `tenant-<tenant_id>/<database_id>` | `personaldb#watcher` |
| Create a projection definition | `personaldb:create` | `tenant-<tenant_id>/<projection_db>/projections/<projection_id>` | Public scope only for create |
| Read or watch a projection definition/watch | `personaldb:read` or `personaldb:watch` | `tenant-<tenant_id>/<projection_db>/projections/<projection_id>` | `personaldb_projection#reader` or `personaldb_projection#watcher` |
| Authorise row effects | `personaldb:insert`, `personaldb:update`, `personaldb:delete` | `tenant-<tenant_id>/<database_id>/<resource_type>/<resource_id>` | `personaldb_row#personaldb:insert`, `personaldb_row#personaldb:update`, or `personaldb_row#personaldb:delete` |

The current CLI is intentionally thinner than the API. `group create`, `group read`, `projection create`, `projection read`, `catch-up`, and `watch` are useful manual helpers. `changeset submit` exists, but it is not currently a complete production commit helper because the CLI generates request/session/idempotency details itself, sends an empty session token, and cannot supply voter acknowledgements. The service requires a non-empty session token matching the authenticated bearer token and at least one voter acknowledgement, so use the API for real commits until that CLI gap is closed.

## Create a group

A group starts with schema SQL, a schema hash, and a genesis hash. The schema hash is the BLAKE3 hex32 hash of the exact schema SQL bytes. The genesis hash is the initial head hash chosen by the client protocol for the empty history. In tests, Anvil commonly derives it from a string such as `genesis:<database_id>`, but the service only requires a 64-character hex value and then uses it as the log hash at index `0`.

Prepare a simple notes schema. The example hashes below are BLAKE3 hex32 values for the exact schema string and `genesis:customer-notes`; change them if you change the schema or genesis convention. The current CLI does not include a hash helper.

```bash
SCHEMA_SQL='CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL, owner TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0);'
SCHEMA_HASH=b510eebd0d545eba1de23c6155bc36f8da9aa10e0dcbc15924d52b1e191dcc4f
GENESIS_HASH=b8e54f73c7235dfc63d1e40988252049a7ae356554c6b0e40b9a13323874487a

anvil --profile acme personaldb group create customer-notes "$SCHEMA_HASH" "$GENESIS_HASH" \
  --schema-sql "$SCHEMA_SQL"
```

This calls `PersonalDbService.CreatePersonalDbGroup`. A successful response proves that the caller authenticated, the `database_id` was safe, the schema hash matched the schema SQL, the schema created at least one SQLite table, the genesis hash was valid hex32, the caller had `personaldb:create` on `tenant-<tenant_id>/customer-notes`, and no group with that id already existed.

It does not prove that any replica has data, that future changesets will be authorised, or that the local SQLite database on a device matches the registered schema. It creates the Anvil witness state: a sealed group manifest and a committed head at log index `0`.

The API response contains both the manifest and the committed head. The current CLI prints only a compact line with tenant id, database id, and schema hash. Use the API response when a client needs the head hash, policy epoch, membership epoch, or signatures.

## Read group state

Read the group before a manual catch-up or when debugging an application replica:

```bash
anvil --profile acme personaldb group read customer-notes
```

This calls `PersonalDbService.GetPersonalDbGroup`. A successful response proves that the caller has `personaldb:read` on `tenant-<tenant_id>/customer-notes` or an allowed `personaldb#reader` relationship, and that the group manifest can be read and verified.

It does not prove that a replica is current. A current replica is one whose locally stored `log_index` and `log_hash` match the committed head returned by the API. Because the CLI does not print the committed head today, use the API for automated replica start-up.

## Submit changesets through the API

A production client submits changesets with `PersonalDbService.SubmitPersonalDbChangeset`. The request is deliberately more detailed than the CLI command because the witness must reject spoofed or stale submissions.

The important fields are:

```text
SubmitPersonalDbChangesetRequest.tenant_id
SubmitPersonalDbChangesetRequest.database_id
SubmitPersonalDbChangesetRequest.principal
SubmitPersonalDbChangesetRequest.session_token
SubmitPersonalDbChangesetRequest.request_id
SubmitPersonalDbChangesetRequest.idempotency_key
SubmitPersonalDbChangesetRequest.base_log_index
SubmitPersonalDbChangesetRequest.base_log_hash
SubmitPersonalDbChangesetRequest.client_log_epoch
SubmitPersonalDbChangesetRequest.membership_epoch
SubmitPersonalDbChangesetRequest.policy_epoch
SubmitPersonalDbChangesetRequest.leader_replica_id
SubmitPersonalDbChangesetRequest.voter_acks[]
SubmitPersonalDbChangesetRequest.changeset_payload_hash
SubmitPersonalDbChangesetRequest.changeset_bytes
```

The authenticated token is not just a transport detail. The request tenant must match the token tenant. The `principal` must match the authenticated subject, and `session_token` must match the authenticated bearer token. This prevents a client from putting another principal into the request body and having Anvil witness the change as that user.

The base head is also a correctness guard. If `base_log_index` and `base_log_hash` do not match Anvil's current committed head, the submit fails instead of branching history silently. The membership epoch, policy epoch, and schema hash must match the active group. The changeset hash must be the BLAKE3 hash of the exact changeset bytes. The service validates at least one voter acknowledgement and stores a hash of the acknowledgements in the certificate.

After structural validation, Anvil derives a mutation envelope from the SQLite changeset. Inserts require `personaldb:insert`, updates require `personaldb:update`, and deletes require `personaldb:delete` on derived row resources. A broad group-level commit permission is not enough to mutate every row unless the row effects are also authorised by public scope or by relationship tuples.

The current CLI submit command is real, but do not treat it as a successful workflow example today:

```bash
anvil --profile acme personaldb changeset submit customer-notes ./changeset.bin \
  --base-log-index 0 \
  --base-log-hash "$GENESIS_HASH" \
  --client-log-epoch 1 \
  --membership-epoch 1 \
  --policy-epoch 1 \
  --leader-replica-id replica-a
```

This proves only that the CLI can read `./changeset.bin` and call the submit RPC shape. In the current implementation it does not let you provide the authenticated session token, a stable idempotency key, or voter acknowledgements, so a normal public submit is expected to fail validation. Use it for command-surface inspection or tests that are updated with the missing fields, not as the production path.

## Catch up a replica

Catch-up asks Anvil: given the head I have locally, what accepted commits do I need next?

For a newly created group, catch up from the genesis head:

```bash
anvil --profile acme personaldb catch-up customer-notes \
  --replica-id laptop-a \
  --have-log-index 0 \
  --have-log-hash "$GENESIS_HASH" \
  --max-entries 100
```

This calls `PersonalDbService.CatchUpPersonalDb`. A successful empty response proves that the caller can read the group and that the supplied replica position is on the chain. After commits exist, the API response includes committed log records, changeset bytes, certificate records, certificate JSON, a `has_more` flag, and the current committed head.

The CLI prints only counts and booleans: `entries=<n> has_more=<bool> snapshot_required=<bool>`. That is useful for an operator checking whether history is available, but it is not enough to replay into SQLite. A real replica must call the API, apply each returned changeset to local SQLite in log order, verify/store the certificate if the client protocol requires it, and then persist the new local head.

If the replica's `have_log_hash` does not match the chain at `have_log_index`, the API returns `snapshot_required=true` with reason `divergent_replica`. If the committed head is missing, it returns `snapshot_required=true` with reason `missing_committed_head`. The CLI currently does not download or restore snapshots; snapshot restore is an API/client concern.

## Watch for new commits

Catch-up is finite. Watches keep a consumer current after it has caught up. A PersonalDB group watch emits ordered commit events with a `cursor_low:cursor_high` pair, event type, log index, log hash, changeset hash, certificate hash, committed head hash, authorisation revision, timestamp, and watch envelope.

Tail a group watch from the beginning:

```bash
anvil --profile acme personaldb watch customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

The generic watch command exposes the same group watch surface:

```bash
anvil --profile acme watch personaldb customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

A watch response proves that the caller has `personaldb:watch` on `tenant-<tenant_id>/customer-notes` or an allowed `personaldb#watcher` relationship, and that at least one commit event exists after the requested cursor. It does not deliver the changeset payload; use catch-up or the API response to fetch replay data. Store the last processed cursor after your consumer has durably recorded its work, then restart with that cursor.

The current implementation sends a stored snapshot of events after the cursor and then live events. If a live consumer falls behind the retained broadcast window, the stream fails with data loss. The recovery loop is to reopen catch-up from your durable log head, replay what you missed, and then start a watch again from the latest processed watch cursor.

## Add a projection

A projection turns source commits into derived commits in a target PersonalDB group. Use this when the server needs a smaller or filtered database to sync, such as open notes, unread counts, or rows visible to a specific application actor. Do not think of it as a live SQL view; it is an ordinary PersonalDB target group whose commits are generated by the projection builder.

Create the projection target group first. Its schema is the schema of the derived database, not the source database. The example hashes are for the exact schema string and `genesis:customer-notes-open`.

```bash
PROJECTION_SCHEMA_SQL='CREATE TABLE open_notes(id TEXT PRIMARY KEY NOT NULL, body TEXT NOT NULL);'
PROJECTION_SCHEMA_HASH=89a79e7b81df7f7b221eefee80e0e3e30725ba00aec75f33a4df4676343837de
PROJECTION_GENESIS_HASH=432a1a0f110d36382f17c62fc31df20d7071fadf9cce4e8b4bbffcca947564b3

anvil --profile acme personaldb group create customer-notes-open "$PROJECTION_SCHEMA_HASH" "$PROJECTION_GENESIS_HASH" \
  --schema-sql "$PROJECTION_SCHEMA_SQL"
```

Then create a projection definition. The CLI takes the target database id as the positional argument. The `projection_id` lives inside the JSON definition.

```json
{
  "format_version": 1,
  "tenant_id": "1",
  "database_id": "customer-notes-open",
  "projection_id": "open-notes",
  "source_database_ids": ["customer-notes"],
  "target_database_id": "customer-notes-open",
  "target_actor_or_scope": "notes-service",
  "table_mappings": [
    {
      "source_database_id": "customer-notes",
      "source_table": "notes",
      "target_table": "open_notes"
    }
  ],
  "column_mappings": [
    {
      "source_table": "notes",
      "source_column": "id",
      "target_table": "open_notes",
      "target_column": "id"
    },
    {
      "source_table": "notes",
      "source_column": "body",
      "target_table": "open_notes",
      "target_column": "body"
    }
  ],
  "row_filters": [
    {
      "kind": "field_equals_literal",
      "table": "notes",
      "field": "deleted",
      "literal": "0"
    }
  ],
  "resource_bindings": [
    {
      "source_table": "notes",
      "primary_key_column": "id",
      "resource_type": "notes",
      "resource_id_column": "id",
      "parent_resource_id_column": null
    }
  ],
  "writeback_policy": {"kind": "deny"},
  "definition_hash": null
}
```

Save that JSON as `open-notes-projection.json`, then create it:

```bash
anvil --profile acme personaldb projection create customer-notes-open "$(cat open-notes-projection.json)"
```

This proves that the caller can create the projection resource, both source and target groups exist, the definition scope matches the authenticated tenant and target database, and the definition can be canonicalised and sealed. Anvil returns the sealed definition JSON with `definition_hash` filled in.

Read it back by target group and projection id:

```bash
anvil --profile acme personaldb projection read customer-notes-open open-notes
```

Projection builds happen when source commits are accepted. If a source changeset produces mapped rows that match the filters, Anvil commits a derived changeset into the target group and emits a projection watch event. The public API exposes `WatchPersonalDbProjection`; the current public CLI does not have a projection-watch command. Use API clients for projection watch consumers, and use `personaldb catch-up customer-notes-open` when you need to replay the target projection group manually.

Writeback is implemented for projections whose definition uses `allow_mapped_columns`; a submit to the projection group is translated back into a source changeset. Definitions with `{"kind":"deny"}` reject direct writeback. Treat writeback as an API/client feature today, not a CLI workflow.

## Diagnose and repair the log chain

PersonalDB state is append-only enough to audit, but operators still need diagnostics for missing payloads, broken certificates, or a head that does not match readable log records. The tenant repair API includes a PersonalDB log-chain repair check, exposed through the public repair CLI:

```bash
anvil --profile acme repair run personal-db customer-notes
```

This proves that the caller has `repair:run` on `tenant-<tenant_id>/customer-notes` and that Anvil can assess the group manifest, committed head, log segments, changeset payloads, and certificates. The command prints status, tenant id, and database id. `up_to_date` means the checked chain matches the current head. `needs_review` means Anvil found evidence that should be handled by an operator; repair does not silently rewrite PersonalDB history.

If a repair finding is written, list findings with the scope id used by the current PersonalDB repair code:

```bash
anvil --profile acme repair findings personaldb tenant-1-database-customer-notes --page-size 20
```

This proves only that findings can be read for that scope. It does not prove a client replica is safe to continue; clients should still catch up from a verified local head or restore from a snapshot when instructed.

## Build a safe replica loop

A robust PersonalDB client loop is small but strict. Store the local SQLite file, the last applied log index and hash, and the last processed watch cursor durably. On start-up, call catch-up from that stored head. Apply returned changesets in order. After each durable apply, advance the local head. When catch-up returns no more entries, start a group watch from the stored cursor. When a watch event arrives, call catch-up again from the local head rather than assuming the watch event contains enough replay data.

When the user edits locally, capture a SQLite changeset, compute its payload hash, submit through the API with the exact current base head and epochs, and only move the local accepted head after the witness returns a commit certificate. If the submit fails because the base is stale, catch up first, apply conflict handling in the local database, and submit a new changeset. If it fails authorisation, do not retry blindly; the row effect was not allowed at the authorisation revision Anvil used.

Use objects for large attachments or full SQLite snapshot payloads, and use PersonalDB for the witnessed row history. Use indexes and projections for derived query surfaces, but remember they are derived from accepted commits. A projection or index can lag; the committed head remains the source of truth for the group.

## What to take forward

PersonalDB lets SQLite remain the application database while Anvil witnesses the shared history. The group manifest tells clients which schema and epochs are active. The committed head tells replicas where the accepted chain ends. Changeset commits are authorised, witnessed, certified, and replayable. Watches let consumers avoid rescanning, and projections turn source commits into derived PersonalDB groups when the server needs filtered or transformed data.

The current API is the reliable production surface. The current CLI is useful for group/projection setup, read checks, catch-up counts, watches, and repair inspection, but it is not yet a full PersonalDB sync client.

## Success and failure cues

A PersonalDB group is healthy when commits form a verifiable chain, catch-up returns the missing range a replica requested, and projections can state which source cursor they reflect. A watch event means the group advanced; it does not prove a projection is already current. Repair evidence should be read as log/projection evidence, not as object-store evidence. If your application cannot name its last durable commit or projection cursor, it cannot recover safely.

## Where to go next

Read [Watches](/tutorials/watches/) for the group-watch shape used by PersonalDB projection workers, and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/) before building operator runbooks around log-chain or projection repair. If your product only needs immutable blobs and current pointers, return to [Buckets and Objects](/tutorials/buckets-and-objects/) instead of forcing it into PersonalDB.
