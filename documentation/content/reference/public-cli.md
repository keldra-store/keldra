---
title: Public CLI
description: Reference for the tenant-facing anvil CLI, including profiles, authentication, buckets, objects, links, authz, indexes, watches, streams, leases, PersonalDB, diagnostics, repair, and audit.
---

# Public CLI

`anvil` is the tenant-facing command-line client. It talks to the public Anvil API using application credentials or a bearer token. It does not write CoreStore files directly, it does not use the private admin API, and it does not bypass public policy scopes or relationship authorisation. Use it for manual inspection, smoke tests, tenant automation, and examples. Production applications should call the public API or the Rust `anvil-storage` client directly when they need richer request fields, stable idempotency keys, explicit preconditions, typed error handling, or long-running streaming consumers.

This page is a reference for the current command names and flags exposed by `anvil-cli/src`. It documents the public CLI surface; implementation architecture is covered in [Architecture Overview](/architecture/overview/) and storage/index internals in [CoreMeta and Blob Storage Layout](/architecture/storage-layout/) and [Indexing and Query Architecture](/architecture/indexing-and-query/). It is not a tutorial. For task-oriented walkthroughs, read [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Buckets and Objects](/tutorials/buckets-and-objects/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), [Authorisation](/tutorials/authorisation/), [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/), [Watches](/tutorials/watches/), [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/), [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/), [PersonalDB](/tutorials/personaldb/), and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/). Scope strings are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), index JSON is in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), and the private operator CLI is documented separately in [Admin CLI](/reference/admin-cli/).

## Global command shape

The top-level command is:

```bash
anvil --profile NAME --config PATH <command> ...
```

Both options are optional. `--profile` selects a stored profile. If it is omitted, `anvil` uses the default profile in the config file. `--config` points at an explicit config file; without it, the CLI uses the platform-specific `confy` location for the application name `anvil`. Hosts are normalised on use: a profile host without `http://` or `https://` is treated as `http://...`.

Token resolution is shared by all commands that call an authenticated API:

| Input | Behaviour |
| --- | --- |
| `ANVIL_AUTH_TOKEN` | Used directly as the bearer token. No client-secret token exchange is performed. |
| `ANVIL_BOOTSTRAP_CREDENTIAL_FILE` | Reads JSON containing `client_id` and `client_secret` and exchanges it for a token. This is mainly useful for first-admin/operator smoke tests that still use the public token service. |
| Stored profile `client_id` and `client_secret` | Used when neither environment override is present. |
| `ANVIL_PUBLIC_ENDPOINT` | Overrides the endpoint used for token exchange. Service calls still use the profile host. |

The CLI requests a token with an empty requested-scope list. The token service then mints the app's approved public policy scopes. `anvil auth get-token --client-id ... --client-secret ...` can override the profile only for that token request.

## Command families

The current public CLI families are:

| Family | Subcommands |
| --- | --- |
| `configure` | Interactive profile creation or update. |
| `static-config` | Non-interactive profile creation or update. |
| `auth` | `get-token`, `grant`, `revoke`, `list-grants`. |
| `app` | `create`, `rotate-secret`, `delete`, `list`. |
| `bucket` | `create`, `rm`, `ls`, `set-public`. |
| `object` | `put`, `get`, `rm`, `ls`, `head`, `link ...`. |
| `host-alias` | `create`, `read`, `verify`, `list`, `delete`. |
| `authz` | `schema ...`, `tuple ...`, `check`, `list-objects`, `list-subjects`, `watch`. |
| `index` | `create`, `update`, `disable`, `drop`, `list`, `query`, `diagnostics`. |
| `watch` | `prefix`, `index-definition`, `index-partition`, `authz`, `personaldb`. |
| `stream` | `create`, `append`, `read`, `tail`, `seal-segment`. |
| `lease` | `acquire`, `checkpoint`, `commit`, `read`, `force-release`. |
| `personaldb` | `group ...`, `projection ...`, `changeset ...`, `catch-up`, `watch`. |
| `diagnostics` | `list`. |
| `repair` | `run ...`, `findings`. |
| `audit` | `list`. |
| `hf` | `key ...`, `ingest ...` for Hugging Face integration. |

All command families after configuration require authentication unless they are printing help. Public policy scopes control whether the request is authorised; relationship authorisation may also filter object, index, and product-level visibility.

## Profiles and tokens

Create a profile interactively:

```bash
anvil configure \
  --name acme \
  --host https://storage.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

Create or update one non-interactively:

```bash
anvil static-config \
  --name acme \
  --host https://storage.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

Print a bearer token:

```bash
anvil --profile acme auth get-token
anvil --profile acme auth get-token --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET"
```

Purpose: store public endpoint and application credential material, then mint a bearer token through the public `AuthService`.

Auth/scope shape: token exchange uses the client id and secret. Grant management below requires `policy:grant`, `policy:revoke`, or `policy:read` on the delegated resource as described in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

Limitations: `auth get-token` prints a token; it does not persist the token. The CLI does not request a custom scope subset today. Use the API directly if an application needs explicit requested scopes or token lifecycle control.

## Tenant applications and public policy grants

Tenant apps are service principals inside one storage tenant. These commands manage tenant-owned app credentials and public policy grants after a tenant has been delegated app and policy authority.

```bash
anvil --profile acme app create docs-writer
anvil --profile acme app rotate-secret docs-writer
anvil --profile acme app delete docs-writer
anvil --profile acme app list
```

```bash
anvil --profile acme auth grant docs-writer object:write documents/inbox/welcome.txt
anvil --profile acme auth revoke docs-writer object:write documents/inbox/welcome.txt
anvil --profile acme auth list-grants docs-writer
```

Purpose: create, rotate, delete, and list app credentials; grant and revoke public policy scopes to another app in the same tenant.

Auth/scope shape: app lifecycle commands use `app:create`, `app:read`, `app:rotate_secret`, or `app:delete` on `tenant:<tenant_id>`. Policy commands use `policy:grant`, `policy:revoke`, or `policy:read`, and delegation is non-escalating: the caller must already hold the authority being delegated. Fully qualified actions such as `object:read` are the safest form.

Limitations: current app-management scope is tenant-wide rather than per app name. The public delegation path rejects system/internal resources and reserved `_anvil/` resources. Do not use the public CLI to model private admin authority; use the admin plane for system operations.

## Buckets and public-read policy

Buckets are tenant-owned placement and policy boundaries.

```bash
anvil --profile acme bucket create documents local
anvil --profile acme bucket ls
anvil --profile acme bucket rm documents
anvil --profile acme bucket set-public documents --allow true
anvil --profile acme bucket set-public documents --allow false
```

Purpose: create a bucket in a region, list buckets visible to the caller, delete a bucket, and update the bucket's public-read policy JSON.

Auth/scope shape: typical actions are `bucket:create`, `bucket:delete`, `bucket:list`, `bucket:read`, and `bucket:write` on the bucket name or list resource. Public-read is still an Anvil policy decision; it is not admin access.

Limitations: local examples often use `local` as the region, but production region names depend on the mesh configuration. Current bucket listing has coarse scope behaviour; some object CLI mutation helpers also call `ListBuckets` to discover the bucket id before writing. If a least-privilege upload fails because the app cannot list buckets, use the API/Rust client with a known bucket id or adjust the grant deliberately.

## Objects

Object command paths use `s3://bucket/key` syntax for convenience. The CLI still calls the native public Object API.

```bash
anvil --profile acme object put ./welcome.txt s3://documents/tutorial/welcome.txt
anvil --profile acme object get s3://documents/tutorial/welcome.txt ./downloaded-welcome.txt
anvil --profile acme object get s3://documents/tutorial/welcome.txt
anvil --profile acme object head s3://documents/tutorial/welcome.txt
anvil --profile acme object ls s3://documents/tutorial/
anvil --profile acme object rm s3://documents/tutorial/welcome.txt
```

Purpose: upload, download, inspect metadata, list a prefix, and delete the current object.

Auth/scope shape: object reads and `head` use `object:read` on `bucket/key`; writes use `object:write`; deletes use `object:delete`; list uses `object:list`. Current object prefix listing checks the bucket name, not a fine-grained prefix resource.

Limitations: `object put` uploads a file as bytes but does not expose flags for `content_type`, `user_metadata_json`, idempotency keys, explicit object preconditions, or version targeting. `object get` and `object head` read the current version only; there is no CLI flag for a pinned version id. Use the public API or Rust client for production writes that need metadata, CAS, idempotency, pinned reads, or careful retry handling.

## Object links

Object links are tenant-owned aliases inside a bucket. They are not copies. The CLI supports same-bucket links only.

```bash
anvil --profile acme object link create \
  s3://documents/releases/latest.bin \
  s3://documents/releases/app-1.0.0.bin \
  --resolution follow

anvil --profile acme object link update \
  s3://documents/releases/latest.bin \
  s3://documents/releases/app-1.0.1.bin \
  --expected-generation 1 \
  --resolution follow

anvil --profile acme object link read s3://documents/releases/latest.bin
anvil --profile acme object link list s3://documents/releases/ --limit 100
anvil --profile acme object link delete s3://documents/releases/latest.bin --expected-generation 2
```

Purpose: create, update, read, list, and delete link descriptors. `--resolution` accepts `follow` or `redirect`; `--allow-dangling` permits a link whose target does not currently exist. Updates and deletes require `--expected-generation`.

Auth/scope shape: create and update use `object:write` on `bucket/link_key`; read uses `object:read`; delete uses `object:delete`; list uses `object:list` on the prefix and filters returned links by `object:read` on each link.

Limitations: the public CLI does not expose target version pinning even though the API has a target-version field. It rejects cross-bucket links. Link generation checks protect link metadata updates; they do not protect the target object from being overwritten unless the object write itself uses its own API preconditions.

## Tenant host aliases

Tenant-owned host aliases attach a hostname to a bucket and optional prefix for static/object delivery. Operator-owned routing and system host-alias lifecycle belong to `anvil-admin`; this section covers the public tenant surface.

```bash
anvil --profile acme host-alias create docs.example.com documents \
  --region local \
  --prefix site/

anvil --profile acme host-alias read docs.example.com
anvil --profile acme host-alias verify docs.example.com "$OBSERVED_CHALLENGE" --expected-generation 1
anvil --profile acme host-alias list --region local --limit 100
anvil --profile acme host-alias delete docs.example.com --expected-generation 2
```

Purpose: create a pending alias, read metadata, verify a DNS/domain challenge, list tenant aliases, and delete an alias with generation checking.

Auth/scope shape: create, verify, and delete use `bucket:write` on the bucket; read and list require `bucket:read` on the bucket attached to the alias.

Limitations: these commands manage Anvil host-alias records only. They do not create DNS records, issue TLS certificates, configure your reverse proxy, or make a bucket public. Custom domain operations are usually coupled to operator routing and gateway configuration, described in [Gateway Operations](/operators/gateway-operations/) and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/).

## Relationship authorisation (`authz`)

Relationship authorisation is tenant-owned product authorisation: schemas, schema bindings, tuples, checks, list APIs, and tuple-log watches. It is separate from public policy scopes.

Schema commands:

```bash
anvil --profile acme authz schema put document_schema document "$(cat schema.document.json)" \
  --reason 'define document relationships'

anvil --profile acme authz schema bind document_schema 1 sha256:abc123 default \
  --expected-generation 0 \
  --reason 'bind document schema'

anvil --profile acme authz schema get document_schema --schema-revision 1
anvil --profile acme authz schema binding default
```

Tuple and check commands:

```bash
anvil --profile acme authz tuple write document doc-42 viewer user user-17 add \
  --reason 'grant user-17 viewer on doc-42'

anvil --profile acme authz tuple read document \
  --object-id doc-42 \
  --relation viewer \
  --page-size 100

anvil --profile acme authz check document doc-42 viewer user user-17
anvil --profile acme authz list-objects document viewer user user-17 --page-size 100
anvil --profile acme authz list-subjects document doc-42 viewer user --page-size 100
anvil --profile acme authz watch document --after-revision 0
```

Purpose: maintain tenant schemas and tuple facts, bind a schema revision to a realm, check whether a subject has a relation, list reachable objects or subjects, and tail tuple revisions.

Auth/scope shape: schema commands use `authz:schema_write` or `authz:schema_read`; tuple commands use `authz:tuple_write` or `authz:tuple_read`; checks use `authz:check`; watches use `authz:watch`. Resource strings are generally shaped like `namespace/object#relation`, with realm/schema scope details described in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

Limitations: the CLI uses latest consistency and does not expose custom zookie/consistency inputs for checks or reads. Tuple `operation` is passed as text and must be `add` or `remove`. Schema JSON is supplied as a string argument; if you want file contents, expand it in the shell, for example `"$(cat schema.json)"`, or call the API directly. The current evaluator stores schemas and tuples, but richer schema-rule and caveat evaluation limits are described in [Authorisation](/learn/authorisation/).

## Indexes and search

Index commands manage definitions, query derived indexes, and read index diagnostics. Supported kind names in the CLI are `path`, `metadata`, `metadata-filter`, `metadata_filter`, `full-text`, `full_text`, `fulltext`, `vector`, `hybrid`, `personaldb-row-metadata`, `personaldb_row_metadata`, `git-source`, `git_source`, `typed-json`, and `typed_json`.

```bash
anvil --profile acme index create documents by_status typed_json \
  --selector-json '{"prefix":"library/"}' \
  --extractor-json '{"fields":{"status":"/status","due_at":"/due_at"}}' \
  --authorization-mode inherit_object \
  --build-policy-json '{}'

anvil --profile acme index update documents by_status \
  --selector-json '{"prefix":"library/"}' \
  --extractor-json '{"fields":{"status":"/status"}}'

anvil --profile acme index disable documents by_status
anvil --profile acme index drop documents by_status
anvil --profile acme index list documents --include-disabled
```

```bash
anvil --profile acme index query documents by_status \
  --text 'renewal notice' \
  --phrase \
  --path-prefix library/acme/ \
  --metadata-filters-json '{"workflow":"review"}' \
  --typed-predicates-json '[{"field":"status","op":"eq","value":"open"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"}]' \
  --limit 20 \
  --page-token "$PAGE_TOKEN" \
  --require-caught-up-to-watch-cursor "$WATCH_CURSOR" \
  --lag-timeout-ms 2000

anvil --profile acme index diagnostics documents by_status --severity warning --page-size 100
```

Purpose: create/update/drop definitions, list definitions, query path/metadata/typed/full-text/vector/hybrid indexes, and inspect diagnostics.

Auth/scope shape: create uses `index:create` on `bucket/index`; update and disable/drop use `index:update` or `index:delete`; list/query/diagnostics currently use `index:read` on the bucket name. Query results may also be filtered by object visibility or index authorisation mode.

Limitations: this CLI passes JSON strings through to the API; it does not validate the full JSON grammar client-side. Full syntax is in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/). `--vector` accepts comma-delimited floats from Clap, for example `--vector 0.1,0.2,0.3`. Current direct full-text, vector, and hybrid catch-up evidence is more limited than typed/path query flows; use diagnostics and watch evidence where needed. Current `index:read` scope is bucket-level rather than per-index.

## Watches

Watch commands stream change records from public watch surfaces. They are intended for manual tailing and debugging; production consumers should use the streaming API and persist checkpoints only after their derived output is durable.

```bash
anvil --profile acme watch prefix documents library/ --after-cursor 0
anvil --profile acme watch index-definition documents --after-cursor 0
anvil --profile acme watch index-partition documents by_status partition-0 \
  --after-cursor-low 0 \
  --after-cursor-high 0
anvil --profile acme watch authz document --after-revision 0
anvil --profile acme watch personaldb customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

Purpose: tail object-prefix changes, index-definition changes, index-partition lag/progress events, authz tuple revisions, and PersonalDB group events.

Auth/scope shape: prefix watches follow object/bucket watch authorisation; index watches use `index:watch` on the bucket; authz watches use `authz:watch`; PersonalDB watches use `personaldb:watch` on the PersonalDB resource.

Limitations: the CLI streams to stdout and does not store checkpoints. There is no generic public CLI checkpoint store. Use the API for resilient consumers that need backpressure handling, checkpoint durability, and restart behaviour. A watch can fail if the retained live window is exceeded; consumers must be able to restart from a durable checkpoint or rescan where the API supports it.

## Append streams

Append streams are ordered records under the object service. They are useful for audit-like histories, delivery attempts, timelines, and event exports.

```bash
anvil --profile acme stream create documents audits/doc-42
anvil --profile acme stream append documents audits/doc-42 "$STREAM_ID" '{"event":"created"}' \
  --content-type application/json \
  --user-metadata-json '{"source":"cli"}'
anvil --profile acme stream read documents audits/doc-42 "$STREAM_ID" \
  --after-sequence 0 \
  --limit 100 \
  --include-payload
anvil --profile acme stream tail documents audits/doc-42 "$STREAM_ID" \
  --from-sequence 0 \
  --poll-interval-ms 1000
anvil --profile acme stream seal-segment documents audits/doc-42 "$STREAM_ID"
```

Purpose: create a stream, append payload records, read historical records, tail new records, and request segment sealing.

Auth/scope shape: stream creation, append, and sealing are object mutations and use object write authority for the stream key; reads and tails use object read authority.

Limitations: append preconditions and idempotency are not exposed by the CLI even though the API has richer mutation context. `seal-segment` is storage maintenance; it is not logical stream closure. The tail command is a display helper, not a durable consumer framework.

## Task leases

Task leases coordinate tenant-owned background work. The caller identity comes from the bearer token; the request does not let a caller spoof another owner principal. The current CLI exposes lease fields directly as positional arguments and flags.

```bash
anvil --profile acme lease acquire import-acme-docs object-import object-prefix library/acme/ \
  --owner-label importer-7 \
  --source-cursor-low 0 \
  --source-cursor-high 0 \
  --ttl-nanos 30000000000

anvil --profile acme lease read import-acme-docs
anvil --profile acme lease checkpoint import-acme-docs "$FENCE_TOKEN" 125 0
anvil --profile acme lease commit import-acme-docs "$FENCE_TOKEN" 150 0
anvil --profile acme lease force-release import-acme-docs
```

Purpose: acquire a named lease, read its current state, checkpoint progress, commit completed progress, or force release it.

Auth/scope shape: acquire/checkpoint/commit use `coordination:lease_write` on `task_lease/<task_id>`; read uses `coordination:lease_read`; force release uses `coordination:lease_admin`. The server derives owner principal from the token and combines it with `--owner-label` for human-readable ownership.

Limitations: acquire uses `--owner-label` and `--ttl-nanos`; checkpoint and commit take the fence token and cursor values as positional arguments. The CLI does not combine lease fences with object writes in one command; production workers should use the API mutation-batch/fenced-write surfaces where correctness depends on stale-worker rejection.

## PersonalDB

PersonalDB commands operate on Anvil's PersonalDB witness and log surfaces. They do not query a local SQLite database.

```bash
anvil --profile acme personaldb group create customer-notes "$SCHEMA_HASH" "$GENESIS_HASH" \
  --schema-sql "$SCHEMA_SQL"
anvil --profile acme personaldb group read customer-notes
```

```bash
anvil --profile acme personaldb changeset submit customer-notes ./changeset.bin \
  --base-log-index 0 \
  --base-log-hash '' \
  --client-log-epoch 0 \
  --membership-epoch 0 \
  --policy-epoch 0 \
  --leader-replica-id cli

anvil --profile acme personaldb catch-up customer-notes \
  --replica-id cli \
  --have-log-index 0 \
  --have-log-hash '' \
  --max-entries 100

anvil --profile acme personaldb watch customer-notes --after-cursor-low 0 --after-cursor-high 0
```

Projection commands:

```bash
anvil --profile acme personaldb projection create customer-notes "$(cat projection.json)"
anvil --profile acme personaldb projection read customer-notes open-notes
```

Purpose: create/read a group, submit a changeset payload, request catch-up entries, watch group events, and create/read projection definitions.

Auth/scope shape: group creation uses `personaldb:create`; group reads and catch-up use `personaldb:read`; changeset submit uses `personaldb:commit` and row-level actions as implemented by PersonalDB policy; watches use `personaldb:watch`. Resources are shaped with the tenant and database id, for example `tenant-<tenant_id>/<database_id>`.

Limitations: the CLI submit path uses generated request and idempotency ids, sends empty session-token/debug metadata, and does not expose voter acknowledgements. Catch-up output is compact: it reports counts and flags, not a full client sync workflow. Snapshot restore/download and richer projection maintenance are API/client responsibilities where implemented.

## Tenant diagnostics and repair

Public diagnostics and repair are tenant-scoped. System-wide repair belongs to the private admin API.

```bash
anvil --profile acme diagnostics list documents by_status \
  --severity warning \
  --page-size 100

anvil --profile acme index diagnostics documents by_status \
  --severity warning \
  --page-size 100
```

`diagnostics list` and `index diagnostics` currently call the same index diagnostic API shape.
Both commands print `next_page_token=...` when another page exists. Pass that
opaque value back with `--page-token` while keeping the caller, bucket, index,
severity, and page size unchanged.

```bash
anvil --profile acme repair run index documents by_status --rebuild
anvil --profile acme repair run directory documents --rebuild
anvil --profile acme repair run authz-derived derived-userset-acme-docs --rebuild
anvil --profile acme repair run personal-db customer-notes
anvil --profile acme repair findings index "$REPAIR_SCOPE_ID" --page-size 100
```

Purpose: read index diagnostics, rebuild or validate tenant-derived state, and list repair findings for a known scope id.

Auth/scope shape: diagnostics use index-read authority for the bucket/index surface. Repair execution uses `repair:run` for the target resource; findings use `repair:read` for the scope id.

Limitations: there is no general public CoreStore fsck, no universal append-stream repair, and no broad proof that all derived systems are correct. Repairs rebuild or validate supported derived state; they do not synthesize missing source records. Read [Repair and Diagnostics](/operators/repair-and-diagnostics/) before using repair during an incident.

## Tenant audit

Tenant audit events record tenant-facing actions that Anvil chooses to audit.

```bash
anvil --profile acme audit list --page-size 100
anvil --profile acme audit list \
  --principal app:docs-writer \
  --resource documents/tutorial/welcome.txt \
  --action object.put \
  --page-token "$NEXT_PAGE_TOKEN" \
  --page-size 100
```

Purpose: list tenant audit events with optional principal, resource, action,
opaque page-token, and page-size filters.

Auth/scope shape: tenant audit listing requires the public authority granted for audit reading in the current service policy. Audit records are tenant-scoped; admin audit is separate and uses `anvil-admin`.

Limitations: audit is evidence, not a repair. A successful audit query does not prove the underlying source record still exists, and not every low-level internal event is necessarily a tenant audit event.

## Hugging Face integration

The `hf` family is part of the current public CLI. It manages tenant Hugging Face keys and ingestion jobs through the public API.

| Command | Purpose |
| --- | --- |
| `anvil hf key add --name NAME --token TOKEN [--note NOTE]` | Store a named Hugging Face token for the tenant. |
| `anvil hf key ls` | List stored key names and update times. |
| `anvil hf key rm --name NAME` | Delete a stored key. |
| `anvil hf ingest start --key NAME --repo REPO --bucket BUCKET --target-region REGION [--revision REV] [--prefix PREFIX] [--include GLOB] [--exclude GLOB]` | Start an ingestion into a bucket/prefix. |
| `anvil hf ingest status --id INGESTION_ID` | Print ingestion counters and state. |
| `anvil hf ingest cancel --id INGESTION_ID` | Cancel an ingestion. |

Auth/scope shape: key commands use `hf_key:create`, `hf_key:read`, `hf_key:list`, or `hf_key:delete`; ingestion commands use `hf_ingestion:create`, `hf_ingestion:read`, or `hf_ingestion:delete`, plus object/bucket authority required by the ingestion destination.

Limitations: ingestion depends on deployment configuration and external Hugging Face availability. This is an integration helper, not a general package-registry gateway.

## API-first limitations

Several CLI helpers intentionally expose only the common manual path:

| Area | CLI limitation | Use API/Rust client when you need... |
| --- | --- | --- |
| Object writes | No content type, user metadata, explicit preconditions, target bucket id, or caller-supplied idempotency key. | Production uploads, CAS writes, rich metadata, deterministic retries. |
| Object reads | No version-id flag. | Pinned version reads and version-aware recovery. |
| Links | No target-version flag; same-bucket only. | Version-pinned links or richer link automation. |
| Index JSON | JSON strings are passed through. | Client-side validation and typed query builders. |
| Watches | Streams to stdout, no checkpoint store. | Durable consumers, replay discipline, backpressure handling. |
| Task leases | Lease operations are separate commands. | Fenced mutations combined with source writes. |
| PersonalDB | Compact admin/test style output. | Full client sync, snapshots, projection workflows, row-level integration. |
| Repair | Surface-specific commands only. | Automated incident tooling with request ids, audit correlation, and post-repair verification. |

Keep long-lived application code on the API/client path. Keep the CLI for evidence, smoke tests, manual administration, and examples that make the public API shape visible.

## Reading command failures

A public CLI failure usually falls into one of five buckets: it could not find credentials, token exchange failed, the service rejected the public policy scope, a relationship check filtered or denied the resource, or the request reached the wrong region or gateway route. Capture the command, profile, endpoint, action/resource grant expected, bucket/key or index name, and whether `ANVIL_AUTH_TOKEN` or stored credentials were used.

Do not solve a public CLI failure by switching to `anvil-admin` unless the task is actually operator-owned. For example, a tenant app that cannot query an index needs a public policy or relationship-authorisation fix, not an admin routing command.
