---
title: PersonalDB Operations
description: Operate PersonalDB witnessing, heads, changeset replay, projections, snapshots, watches, row authorisation, and repair without treating Anvil as a SQL server.
---

# PersonalDB Operations

PersonalDB is the part of Anvil for local-first SQLite applications. The application still owns a SQLite database on each device, worker, or edge process. Users and application code read and write that local SQLite file. Anvil does not become a remote SQL server, and operators should not diagnose product state by opening application SQLite files and running ad-hoc queries.

Anvil's role is the witness and durable history service. A client submits a SQLite session changeset, Anvil checks that the changeset extends the current group head, validates schema and epochs, binds the request to the authenticated principal, checks group and row-level authorisation, writes the changeset and certificate into CoreStore-backed records, advances the committed head, emits watch events, and may build snapshots or projections. Operators therefore monitor the evidence around that witnessed history: heads, commit failures, log-chain repair findings, projection lag, snapshot thresholds, watch lag, and row-authorisation denials.

Read this page with [PersonalDB](/learn/personaldb/), [CoreStore](/learn/corestore/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Authorisation](/learn/authorisation/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [Observability](/operators/observability/), [CoreStore Operations](/operators/corestore-operations/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [PersonalDB Tutorial](/tutorials/personaldb/), [Watches](/tutorials/watches/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## What operators are responsible for

The most common operational mistake is to treat PersonalDB like hosted SQLite. If a customer says a note is missing, the operator's first job is not to query the customer's local `notes.db`. A local database may be offline, ahead with unsubmitted edits, behind the witnessed log, or holding application-specific conflict state. The shared evidence is the PersonalDB group history inside Anvil.

A PersonalDB operator owns these questions:

| Operational question | Evidence to use |
| --- | --- |
| Is the group valid? | Group manifest, schema hash, genesis hash, committed head, policy epoch, membership epoch, and repair status. |
| Are commits being accepted or rejected correctly? | Submit status, stale-head failures, session/principal binding failures, changeset validation errors, voter-ack errors, and row-authorisation denials. |
| Are replicas able to recover? | Catch-up responses, `snapshot_required`, snapshot head metadata, available snapshot thresholds, and client checkpoint reports. |
| Are derived views current? | Projection group heads, projection watch events, source-vs-projection log positions, projection lag, and repair findings. |
| Are watches safe to resume? | Last durable cursor, data-loss watch errors, catch-up from durable head, and consumer checkpoint durability. |
| Is repair finding source damage or derived lag? | PersonalDB log-chain repair report, repair findings, CoreStore errors, and before/after group reads. |

Application teams still own their SQLite schema design, conflict handling, local transaction discipline, and how they apply changesets to local files. Operators keep Anvil's witness path healthy and make sure incidents are explained from source records rather than from one replica's local state.

## The PersonalDB evidence chain

A **group** is one replicated SQLite history inside a storage tenant. It has a `database_id`, schema hash, genesis hash, active membership and policy epochs, row-index generation, projection generation, and a committed head. The public service validates that a database id is a safe component rather than a path.

A **replica** is a device or process with a local SQLite copy of that group. A healthy replica stores its local log index, log hash, and watch cursor next to its SQLite file. Those values are checkpoints, not decoration. If a replica loses them, it cannot safely guess where it is in the group history.

A **changeset** is a SQLite session changeset: inserts, updates, and deletes at row level. Anvil validates that the changeset is non-empty, within the configured size limit, decodable as a SQLite changeset, and limited to tables registered in the group's schema SQL. The current default maximum changeset size is 16 MiB, with a hard implementation cap of 128 MiB.

A **commit** is Anvil's witness decision for one changeset at the next log index. It succeeds only if the submitted base log index and hash match the current committed head, the membership and policy epochs match the active group, the changeset hash matches the bytes, and the caller is authorised for both the group action and the row effects. A successful commit advances the head by exactly one log index.

A **certificate** is the durable proof of that witness decision. It records the tenant, database id, log index, previous log hash, new entry hash, changeset payload hash, verified mutation-envelope hash, epochs, voter-ack hash, authorisation revision, witness node, time, certificate hash, and witness signature. Replicas and support tooling should treat certificates as part of recovery evidence, not as a display field.

A **head** is the current accepted position. At genesis the committed head has log index `0` and the genesis hash. Every accepted commit writes a new head. Catch-up, repair, projections, and watches all refer back to this head and the log chain behind it.

## Public API first, CLI for manual evidence

Production synchronisation should use `PersonalDbService` or a real client library. The API exposes group create/read, projection create/read, changeset submit, catch-up, group watch, and projection watch. It returns richer evidence than the CLI, including committed heads, certificates, replayable changeset bytes, and split watch cursors.

The public `anvil personaldb` CLI is a manual helper. It is useful for smoke tests and operator inspection: create or read a group, create or read a projection definition, ask catch-up for counts, tail group watches, and run repair checks through the public repair CLI. It is not a full PersonalDB sync client today.

Two current gaps matter during incidents. First, `anvil personaldb changeset submit` is incomplete for real public commits: it generates request and idempotency values itself, sends an empty `session_token`, and cannot send voter acknowledgements. The service requires the session token to match the authenticated bearer token and requires at least one voter acknowledgement, so a normal CLI submit is expected to fail validation. Second, `anvil personaldb catch-up` prints compact counts and booleans; it does not print replayable changeset bytes or certificates. Use the API when a client must actually replay history.

## Reading group state

A quick group read is the safest first probe for many incidents:

```bash
anvil --profile acme personaldb group read customer-notes
```

This calls the public `PersonalDbService.GetPersonalDbGroup` RPC for the authenticated tenant. A successful CLI response proves the public endpoint is reachable, the profile can authenticate, the group manifest exists, and the caller has `personaldb:read` on `tenant-<tenant_id>/customer-notes` or an allowed `personaldb#reader` relationship.

It does not prove a replica is current. The current CLI prints a compact manifest line, not the full committed head. The API response includes the committed head, and automated health checks should compare that head with the replica's locally stored log index and log hash. If a user-facing device is stale but Anvil's committed head is healthy, the incident is a replica catch-up problem rather than a witnessed-history problem.

A missing or denied group read should be classified carefully. `not found` may mean the database id is wrong or the group was never created. Permission denied may be a missing public policy scope or a relationship-authorisation denial. A malformed id failure points to client input, not storage corruption. Do not start repair until you know which class you have.

## Commit failures and stale workers

A PersonalDB commit is stricter than an ordinary object write. Object storage can create a new version at a key. A PersonalDB commit must extend exactly one ordered log chain. If two replicas submit from the same base head, one may succeed and the other should receive a stale-base failure. That is correct behaviour: Anvil rejected a branch rather than silently forking history.

Operators should group commit failures by cause:

| Failure class | Likely meaning | Operator response |
| --- | --- | --- |
| Base head mismatch | Replica submitted from an old log index/hash. | Tell the client team to catch up, merge/rebase locally, and resubmit from the current head. |
| Session token mismatch | Request body does not match the authenticated bearer token. | Treat as client bug, stale credential, or attempted spoofing; do not bypass it. |
| Principal mismatch | Request principal differs from authenticated subject. | Investigate client identity handling; Anvil should not witness as another user. |
| Epoch or schema mismatch | Client is using stale group policy, membership, or schema state. | Have clients refresh group metadata and retry only after reconciling. |
| Changeset validation failure | Empty, malformed, too large, wrong hash, or unregistered table. | Fix the client changeset generation or schema registration. |
| Missing voter acknowledgement | Commit request lacks required acknowledgement evidence. | Use the API/client path that supplies `voter_acks`; current CLI submit is not enough. |
| Row-authorisation denial | The caller may commit to the group but not mutate one or more derived row resources. | Inspect public scopes and relationship tuples at the relevant authorisation revision. |

The current CLI submit command is useful only to show the command surface and payload shape:

```bash
anvil --profile acme personaldb changeset submit customer-notes ./changeset.bin \
  --base-log-index 0 \
  --base-log-hash "$GENESIS_HASH" \
  --client-log-epoch 1 \
  --membership-epoch 1 \
  --policy-epoch 1 \
  --leader-replica-id replica-a
```

This command proves the CLI can read a local file and call the submit RPC shape. It does not prove a production commit path works. In current source it omits the authenticated session token and voter acknowledgements, so use the API or a real client library for commit smoke tests that must succeed.

## Row-level authorisation

PersonalDB authorisation starts at the group but does not stop there. Public policy actions such as `personaldb:read`, `personaldb:commit`, and `personaldb:watch` use resources shaped like `tenant-<tenant_id>/<database_id>`. If public policy does not grant the action, the service can evaluate relationship authorisation on the `personaldb` kind with relations such as `reader`, `committer`, and `watcher`.

A commit also creates row effects. Inserts require `personaldb:insert`, updates require `personaldb:update`, and deletes require `personaldb:delete` on derived row resources shaped like `tenant-<tenant_id>/<database_id>/<resource_type>/<resource_id>`. The current row resource is derived from the table and primary-key hash or from projection resource bindings. Relationship checks use the `personaldb_row` namespace and the permission relation being checked.

This matters operationally because a user can have group commit authority while still being denied for one row. That denial is not a storage outage. It may be the correct product policy. When debugging, record the group action, row action, resource string, principal, and authorisation revision where safe. Do not log changeset bodies or row contents merely to explain a denial.

## Catch-up, snapshots, and replica recovery

Catch-up is the normal way a replica proves what it missed. The replica sends the log index and hash it has already applied. Anvil checks whether that position is on the group chain and returns later log entries up to the requested limit. The API response can include changeset bytes, commit certificates, certificate JSON, the current head, `has_more`, and snapshot metadata.

The CLI is useful for a compact operator probe:

```bash
anvil --profile acme personaldb catch-up customer-notes \
  --replica-id laptop-a \
  --have-log-index 0 \
  --have-log-hash "$GENESIS_HASH" \
  --max-entries 100
```

A successful `entries=0 has_more=false snapshot_required=false` response for a new group proves the caller can read the group and that the supplied genesis position is on the chain. A non-zero entry count proves there is replayable history after the supplied position in the API response. It does not prove the CLI has replayed anything into SQLite; the CLI does not output the changeset bytes or certificates needed for a real replica apply loop.

`snapshot_required=true` is not an empty catch-up. It means the supplied position cannot safely replay from the log path Anvil can currently provide. Current reasons include divergent replica state and missing committed head. The implementation can build snapshot manifests and compressed SQLite snapshot objects internally, and snapshot creation is controlled by thresholds: by default Anvil considers a new snapshot after 1024 committed entries or 64 MiB of committed changeset payload since the latest snapshot. Operators should monitor whether groups are crossing those thresholds and whether snapshot building succeeds.

The current public gap is restore exposure. Catch-up can return snapshot metadata and the code can store and verify snapshot objects, but there is no complete public CLI snapshot download or restore command, and no dedicated public snapshot-fetch RPC documented for clients. Until that is exposed, a production client needs a supported API/client workflow before it can rely on snapshots as its only recovery path.

## Watches and checkpoint discipline

A PersonalDB watch tells a consumer that a group head moved. It is not the replay payload. Group watch events carry a split cursor, event type, log index, log hash, changeset payload hash, certificate hash, committed-head hash, authorisation revision, emitted time, and a common watch envelope. Consumers that need row data should call catch-up from their durable log head.

The public CLI can tail group watches through either the PersonalDB command family or the generic watch family:

```bash
anvil --profile acme personaldb watch customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

```bash
anvil --profile acme watch personaldb customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

A successful stream proves the caller has `personaldb:watch` or an allowed `personaldb#watcher` relationship and that Anvil can deliver retained group events followed by live events. It does not prove a replica applied the changeset, a projection caught up, or an external consumer checkpointed safely.

Checkpoint after output, not before. A replica stores a watch cursor only after catch-up has applied the corresponding log entries and durably updated the local head. A projection monitor stores its cursor only after its own status or derived output is durable. If a watch reports that it fell behind the retained live event window, reopen catch-up from the durable log head and then resume watching from a safe cursor. Do not advance a checkpoint just because a terminal printed an event.

## Projections and projection lag

A PersonalDB projection is a derived PersonalDB group. It is not a live SQL view inside Anvil. A source group commit can produce a transformed changeset into a target group according to a sealed projection definition. The target projection group then has its own manifest, committed head, catch-up path, certificates, watches, and repair story.

Operators should monitor projection lag as source log position versus projection log position, not as a SQL query result. If source `customer-notes` is at log index `500` and projection `customer-notes-open` is at `470`, the projection is behind even if local SQLite queries on one replica look plausible. The API exposes `WatchPersonalDbProjection`, whose events include source database id, source log index/hash, projection log index/hash, definition hash, and authorisation revision.

The current public CLI does not expose a projection-watch command. You can inspect the definition and target group manually:

```bash
anvil --profile acme personaldb projection read customer-notes-open open-notes
```

This proves the projection definition exists, can be read by the caller, and is visible for that target group and projection id. It does not prove the builder has processed every source commit. Pair it with API-level projection watch evidence, target-group catch-up, repair findings, and application-side builder checkpoints.

If projection output is stale, avoid querying application SQLite as the first diagnostic. Check whether source commits are still being accepted, whether projection build errors are recorded, whether the target projection group head is moving, whether the projection watch is advancing, and whether row-authorisation filters in the projection definition are excluding rows deliberately.

## Repair and findings

PersonalDB repair checks the source log chain. It assesses the group manifest, committed head, log segments, changeset payload references, commit certificates, hash continuity, and whether the committed head matches the verified chain. It does not invent missing commits, choose conflict winners, rewrite a user's SQLite database, or make a projection current by itself.

The tenant-facing repair helper is:

```bash
anvil --profile acme repair run personal-db customer-notes
```

This calls the public repair service for the authenticated tenant. A result such as `up_to_date` proves the checked chain is internally consistent through the committed head at the time of the repair. `empty_source` can be healthy for a newly created group at genesis. `needs_review` means Anvil wrote or returned evidence that an operator should inspect. The command does not prove every replica is caught up, every projection has caught up, or every row-level authorisation decision matched product intent.

When a repair writes a finding, list the PersonalDB repair findings for the generated scope id:

```bash
anvil --profile acme repair findings personaldb "tenant-${TENANT_ID}-database-customer-notes" \
  --page-size 20
```

This proves the caller can read repair findings for that scope. It does not fix the condition. Use the finding's subject, severity, status, message, expected/actual hashes, and cursor information to decide whether the next step is client catch-up, projection rebuild, backup restore, or escalation to the product team.

Operators can also run the admin repair surface when the incident is operator-owned or crosses tenant-visible boundaries:

```bash
anvil-admin --host http://10.10.0.12:50052 repair run \
  --repair-kind personaldb-log-chain \
  --tenant-id acme \
  --database-id customer-notes \
  --audit-reason 'investigate PersonalDB log-chain finding'
```

This uses the private admin API and records an admin audit reason. It proves the admin plane is reachable and the operator is authorised for that repair action. It should not be used as a substitute for normal tenant synchronisation or as a way to inspect row contents outside the public authorisation model.

## Runbook: a replica cannot catch up

Start with the replica's durable checkpoint: database id, replica id, last applied log index, last applied log hash, and last stored watch cursor. If those are missing or inconsistent, the replica is already in recovery territory.

Run a catch-up probe from the reported head. If catch-up returns entries, the witness history is available and the client should apply entries in order through the API response. If catch-up says `snapshot_required`, check the reason and whether a supported snapshot restore path exists for the client. If catch-up fails permission denied, separate missing group read authority from row-level commit authority; catch-up is a read path.

Then read the group through the API, not just the compact CLI line, and compare the committed head with the client's reported head. If Anvil's head is ahead and repair is `up_to_date`, the source chain is healthy and the replica is stale. If repair reports missing payloads, invalid certificates, or head mismatch, preserve logs and storage evidence before attempting any rebuild or restore.

## Runbook: commits are failing after a deploy

First classify the failures. A spike in session-token mismatches usually points at client authentication wiring. A spike in stale-base failures may mean clients stopped catching up before submit. A spike in row-authorisation denials may mean a relationship tuple, schema, projection binding, or public policy change changed the effective permission model. A spike in malformed changesets may mean the application schema or SQLite session capture changed.

Use request ids and safe principal/resource identifiers. Do not log or paste changeset bytes into tickets. For one representative failure, compare the request tenant to the token tenant, the request principal to the authenticated subject, the base head to the current committed head, the epochs to the group manifest, and the row-effect resources to the current authorisation tuples. Retry only when the failure class is retryable. Permission denials and malformed changesets are not fixed by blind retries.

## Runbook: a projection is stale

Treat the projection as derived state. First prove the source group is still moving by reading its head or watching group commits. Then prove the target projection group exists and read its head through the API. If the source head advances while the target head does not, inspect projection watch/API evidence and repair findings. If the target head advances but users still see stale data, the problem may be a client catch-up issue, cached local SQLite state, or an application query problem.

Because the current public CLI has no projection-watch command, production projection workers should expose their own checkpoint and lag metrics. At minimum they should report source database id, source log index/hash processed, target database id, projection id, projection log index/hash produced, last successful build time, last error, and whether the error is a poisoned changeset, authorisation filter, schema mismatch, or output commit failure.

## Operational signals to wire

A useful PersonalDB dashboard should show source and derived evidence together:

| Signal | Why it matters |
| --- | --- |
| Group committed head by tenant/database | Shows whether the witnessed history is advancing. |
| Submit success and failure counts by reason | Separates stale replicas, bad clients, authz denials, and storage problems. |
| Watch lag and data-loss errors | Shows whether consumers can resume from retained events or need catch-up/rebuild. |
| Projection lag by source and target group | Shows derived state behind accepted source commits. |
| Snapshot threshold distance and build failures | Shows whether recovery points are being produced before replay becomes expensive. |
| Repair finding count by status/severity | Shows log-chain damage or review-needed evidence. |
| Row-authorisation denial counts | Shows policy changes or attempted unauthorised row effects. |
| CoreStore read/write errors for PersonalDB records | Shows source durability problems rather than client sync mistakes. |

Current code and documentation define several observability ideas, including `personaldb_projection_lag`, witness latency, commit rejection reasons, watch lag, and repair findings. Do not assume a turnkey metrics endpoint or bundled dashboard exists in the current repository. Wire the logs, request ids, repair outputs, API responses, and deployment telemetry you actually have.

## Current public surfaces and gaps

The current public API exposes the main PersonalDB service surface: group create/read, projection create/read, changeset submit, catch-up, group watch, and projection watch. The public CLI exposes `anvil personaldb group create`, `group read`, `projection create`, `projection read`, `changeset submit`, `catch-up`, and `watch`; the generic `anvil watch personaldb` tails the same group watch path. Public repair exposes `anvil repair run personal-db` and `anvil repair findings`.

The gaps are operationally significant:

| Area | Current limitation |
| --- | --- |
| CLI submit | Incomplete for real commits because it sends an empty session token and no voter acknowledgements and generates request/idempotency fields itself. |
| CLI catch-up | Prints counts and booleans, not replayable changeset bytes, certificates, certificate JSON, or full head details. |
| CLI group read | Prints compact manifest data, not the full committed head needed for automated replica comparison. |
| Projection watch | Available in the API as `WatchPersonalDbProjection`, but not exposed by the public CLI. |
| Snapshot restore | Snapshot metadata and internal object storage exist, but public snapshot download/restore is not exposed as a complete workflow. |
| Projection definition reference | There is no standalone full projection-definition reference comparable to the index JSON reference; use the tutorial, proto, and source for exact shapes. |
| Dashboards | Metrics names and evidence concepts exist, but production export and dashboards are deployment work. |

Design production operations around those limits. Keep SQLite as the application's local database. Keep Anvil as the witness, log, certificate, projection, snapshot, watch, and repair layer. Use the API where correctness depends on full PersonalDB fields. Use the CLI for manual checks, not as a synchronisation engine.

## Data-correctness escalation

A PersonalDB failure is often a correctness event rather than a capacity event. If commits fail validation, identify the client version, base log index, base hash, database id, and replica id before asking clients to retry. Blind retries with the same invalid changeset will only create noise.

If projections fail but the commit log is healthy, repair or rebuild the projection from accepted commits. If the commit log chain is unhealthy, preserve evidence and restore from a known-good backup if repair cannot prove the chain.
