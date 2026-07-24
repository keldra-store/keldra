---
title: Repair and Diagnostics
description: Diagnose tenant and operator state, then run narrow repairs without treating derived data as the source of truth.
---

# Repair and Diagnostics

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/), [Watches](/tutorials/watches/), [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/), [PersonalDB](/tutorials/personaldb/), and [Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/). Those pages introduced the source records that applications write and the derived records Anvil builds from them. This page explains what to do when the derived view looks wrong.

Use [Public CLI](/reference/public-cli/) for tenant-facing command shapes, [Admin CLI](/reference/admin-cli/) for private operator command shapes, [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) for the scope and system-realm relation names, [Indexes and Query](/learn/indexes-and-query/) for index behaviour, and [Watches and Derived Data](/learn/watches-and-derived-data/) for why derived consumers need cursors and catch-up checks.

Start by diagnosing before repairing. Diagnostics are read-only evidence about a scoped inconsistency. Repair is an explicit action that may rebuild or rewrite derived state. A repair can make a query, directory index, authorisation projection, PersonalDB log-chain check, or mesh routing projection healthier, but it does not turn derived state into the source of truth.

This page is written for incident moments, when the safest first action is usually to read evidence. It walks from tenant-scoped diagnostics to narrow repair jobs and then to private admin diagnostics, keeping source data, derived data, audit evidence, and rebuild actions distinct.

The plane boundary matters more during repair than during happy-path tutorials. Tenant-owned indexes, directories, PersonalDB groups, and relationship-derived views are inspected through the public API when the tenant has the relevant scopes. System topology, routing, and mesh-wide diagnostics are inspected through the private admin API. Do not use `anvil-admin` to repair a tenant index just because the page later shows admin diagnostics; choose the surface that owns the broken state.

## Understand what diagnostics and repairs mean

A diagnostic is a record of something Anvil noticed while building, checking, or comparing state. Current diagnostic records carry a source, severity, code, message, scope kind, scope id, optional object key and version id, details JSON, a creation timestamp, and a cursor-like position. For index diagnostics, the public CLI prints the compact part: cursor, severity, code, and message. The API response contains the rest.

A finding is repair evidence. Repair findings are sealed records with a finding id, scope kind, scope id, repair task id, severity, status, code, message, subjects, proposed action, evidence JSON, and hash/signature material. A finding can say that Anvil only verified state, that it rebuilt a derived index, or that operator review is required. Treat findings as incident evidence, not as a replacement for rerunning the user-visible operation that failed.

Scope is deliberately narrow. A public repair command runs as one tenant principal and uses the tenant id from the bearer token. It cannot choose another tenant by request field. An admin repair command runs through the private admin API and is checked against Anvil's system realm, not against public policy scopes. Do not use the admin API as a tenant publishing or application data path.

Severity is not the same as urgency. Current validation accepts `info`, `warning`, and `error` for diagnostics. A warning can be expected during maintenance, and an error can be harmless if it refers to a disabled or obsolete index. Start with the source, scope, and exact failing operation before deciding whether to mutate anything.

## Know the current surfaces

The public API is the primary surface for application-owned diagnostics and repairs. The `anvil` CLI is a helper over that API. The current public diagnostic command is tenant-scoped but index-specific: `anvil diagnostics list <bucket> <index> ...`. It calls the same `IndexService.ListIndexDiagnostics` method as `anvil index diagnostics`. There is no current public bucket-wide diagnostic list that covers directory, authorisation, PersonalDB, or mesh sources.

The current public repair commands call `RepairService` methods:

| Command target | API method | What it checks or can repair |
| --- | --- | --- |
| `anvil repair run index <bucket> <index> [--rebuild]` | `RepairIndex` | Derived index proofs and referenced segments for `path`, `metadata_filter`, `full_text`, `vector`, `hybrid`, and `typed_json` indexes. With `--rebuild`, it rebuilds the derived index when repair is needed. |
| `anvil repair run directory <bucket> [--rebuild]` | `RepairDirectoryIndex` | The bucket directory index against object metadata. With `--rebuild`, it rebuilds the directory index from metadata. |
| `anvil repair run authz-derived <derived_index_id> [--rebuild]` | `RepairAuthzDerivedIndex` | A tenant authorisation derived userset index against tuple-journal state. With `--rebuild`, it rebuilds that derived index. |
| `anvil repair run personal-db <database_id>` | `RepairPersonalDbLogChain` | A PersonalDB log chain, manifests, committed head, segments, payloads, and commit certificates. It currently verifies and records review findings; it does not synthesise missing PersonalDB commits. |
| `anvil repair findings <scope_kind> <scope_id> --limit <n>` | `ListRepairFindings` | Previously written repair findings for an exact service scope id. The current public CLI has no cursor argument for repair findings. |

Those public repair methods are still permission-checked. Index diagnostics require `index:read` on the bucket. Running repair requires `repair:run` on the resource each method checks: `bucket/index` for an index repair, the bucket name for a directory repair, `tenant-<tenant-id>/authz/<derived-index-id>` for authorisation derived repair, and `tenant-<tenant-id>/<database-id>` for PersonalDB repair. Reading findings requires `repair:read` on the exact `scope_id` passed to `repair findings`. Grant those scopes narrowly; do not use wildcard examples for long-lived application credentials.

The admin API is the operator surface. It is private and system-realm authorised. The current admin repair kinds are `index`, `directory-index`, `authz-derived-index`, `personaldb-log-chain`, and `mesh-routing-projection`. Admin diagnostics can combine sources or filter by `index`, `index_diagnostic_journal`, `mesh`, `mesh_lifecycle`, or `mesh_routing_projection`. Admin repair mutations require an audit reason and return an `audit_event_id`.

## Start with read-only tenant diagnostics

Suppose the typed index from the earlier tutorial is missing rows. Do not rebuild it first. Read diagnostics for that index:

```bash
anvil --profile acme diagnostics list documents invoices_by_due \
  --severity error \
  --page-size 20
```

This calls `IndexService.ListIndexDiagnostics` for bucket `documents` and index `invoices_by_due`. A successful command proves the active profile authenticated, the caller had `index:read` on `documents`, the bucket existed, the severity filter was valid, and Anvil could read matching index diagnostic rows. The CLI output is compact:

```text
<cursor>    <severity>    <code>    <message>
```

This command does not prove the index is healthy when it prints no rows. It only proves that no matching diagnostics were returned in that page. It also does not prove the caller can see every object that a query might return, because query results can still be filtered through `inherit_object` authorisation.

For long lists, the CLI prints an opaque `next_page_token`. Continue with that
exact token while preserving the same caller and filters:

```bash
anvil --profile acme diagnostics list documents invoices_by_due \
  --page-token "$NEXT_PAGE_TOKEN" \
  --page-size 20
```

The page token is signed and bound to the diagnostic request. Do not treat it as
a cursor that can be edited or reused with another bucket, index, severity, or
caller.

You can use the index command spelling for the same public API call:

```bash
anvil --profile acme index diagnostics documents invoices_by_due \
  --severity warning \
  --page-size 20
```

This proves the same things as `anvil diagnostics list`. It does not inspect directory indexes, PersonalDB logs, admin mesh state, or tenant audit events.

## Run a narrow public repair

A repair run without `--rebuild` is a target verification pass, but it is not fully read-only: when the backend finds a problem, it writes a repair finding. What it does not do is rebuild the target derived state. For an index, start without `--rebuild` unless you are in a planned maintenance or incident window:

```bash
anvil --profile acme repair run index documents invoices_by_due
```

This calls `RepairService.RepairIndex` for the current tenant. A successful response proves the caller had `repair:run` on `documents/invoices_by_due`, the bucket and enabled index definition existed, and the repair backend could compare the index proof against the object metadata source checkpoint. The CLI prints `status`, `bucket_name`, and `index_name`.

If the status is `up_to_date`, Anvil found the current proof and referenced segments consistent with the source checkpoint it inspected. That does not prove the query will return rows: there may be no matching objects, object visibility may filter results, or the index definition may not select the keys you expect. If the status is `needs_repair`, the command has recorded a finding but has not rebuilt the index.

When you have read the diagnostic evidence and want Anvil to rebuild a repairable index, add `--rebuild`:

```bash
anvil --profile acme repair run index documents invoices_by_due --rebuild
```

This still runs through the public API and tenant policy. When repair is needed, it rebuilds the derived index from source metadata and object bodies according to the index definition. A `rebuilt_derived_index` status proves Anvil attempted the rebuild and wrote repair evidence. It does not prove the application-facing query is now correct until you rerun the original query, preferably with `--require-caught-up-to-watch-cursor` when you have a watch cursor from the write that should be visible.

Directory repair has similar verify-first and rebuild modes:

```bash
anvil --profile acme repair run directory documents

anvil --profile acme repair run directory documents --rebuild
```

The first command compares the bucket directory index with object metadata. The second rebuilds the directory index when a mismatch is found. A rebuilt directory index can fix listing and prefix navigation behaviour, but it does not create missing object versions, change object authorisation, or repair index definitions.

## Read findings carefully

Repair findings are listed by exact scope kind and scope id, not by friendly bucket and index names. The API repair response includes a finding when one is created, but the current public CLI prints only a compact status row for repair runs. If your automation needs the finding id, evidence JSON, subject hashes, or scope id, call the API directly rather than scraping the CLI.

When you do know the scope id, list findings like this:

```bash
anvil --profile acme repair findings bucket tenant-42-bucket-7 --page-size 20
```

This calls `RepairService.ListRepairFindings`. A successful response proves the caller had `repair:read` on `tenant-42-bucket-7` and that Anvil could read sealed findings for scope kind `bucket` and that scope id. The CLI prints finding id, severity, status, and message.

This command does not prove the latest repair run produced a finding.
`up_to_date` and `empty_source` repairs usually do not produce one. When another
page exists, the CLI emits `next_page_token=...`; continue with `--page-token`
and the same scope and page size.

Current repair findings use these scope patterns in the inspected implementation:

| Repair area | Finding scope kind | Scope id pattern |
| --- | --- | --- |
| Index repair | `bucket` | `tenant-<tenant-id>-bucket-<bucket-id>` |
| Directory repair | `bucket` | `tenant-<tenant-id>-bucket-<bucket-id>` |
| Authorisation derived repair | `authz` | `tenant-<tenant-id>` |
| PersonalDB log-chain repair | `personaldb` | `tenant-<tenant-id>-database-<database-id>` |
| Admin mesh routing projection repair | `mesh_routing_projection` | routing projection subject ids returned in admin findings |

The current public CLI does not provide a name-to-scope lookup for repair findings. That is why application automation should keep the structured API response from the repair call if it wants to show or archive the exact finding.

## Use authorisation and PersonalDB repair as verification tools

Authorisation derived indexes are built from tenant relationship tuples. If a relationship check looks wrong after tuple writes, first verify the tuple and schema path from [Authorisation](/tutorials/authorisation/). Repairing the derived userset index is for cases where the materialised derived index is missing, stale, invalid, or does not match tuple-journal source records.

```bash
anvil --profile acme repair run authz-derived derived-userset-primary --rebuild
```

This command calls `RepairAuthzDerivedIndex` for derived index id `derived-userset-primary`. A successful response proves the caller had `repair:run` on `tenant-<current-tenant-id>/authz/derived-userset-primary` and that Anvil could compare or rebuild that tenant's derived userset index. It does not modify tenant schemas or tuples. It also does not let a tenant principal modify Anvil's built-in system realm; system-realm administration remains private admin API work.

PersonalDB repair is intentionally more conservative:

```bash
anvil --profile acme repair run personal-db customer-notes
```

This calls `RepairPersonalDbLogChain`. A successful response proves the caller had `repair:run` on `tenant-<current-tenant-id>/customer-notes` and that Anvil could verify the PersonalDB manifest, committed head, log segments, payload references, and commit certificates far enough to return a status. If it returns `needs_review`, the finding proposes `VerifyOnly` and requires operator or application review. The current repair implementation does not synthesise committed object versions or PersonalDB commits; those action kinds are explicitly rejected by repair-finding validation.

## Use the private admin surface for system diagnostics

Tenant diagnostics cannot see mesh lifecycle or routing projection state. Use the admin API only from a private management path. In the local tutorials, the admin listener is private inside the `anvil-local` container, so run the admin CLI through `docker exec` with a bearer token:

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile local-system auth get-token)"

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list --source mesh --limit 50
```

This calls `AdminService.ListDiagnostics` and asks for the combined mesh source. A successful response proves the token represents a principal authorised in the system realm for `view_diagnostics`, and that the private admin API can read mesh lifecycle and routing projection diagnostics. It does not prove the public API, S3 gateway, or static hosting path is healthy. It also does not prove all diagnostics have been reviewed when `page.has_more` is true.

For index diagnostics through the admin plane, provide the tenant and bucket filters. The current service requires both `tenant_id` and `bucket_name` when `--source index` or `--source index_diagnostic_journal` is selected:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list \
    --source index \
    --tenant-id acme \
    --bucket-name documents \
    --index-name invoices_by_due \
    --severity error \
    --limit 50
```

This proves the admin principal can inspect index diagnostic records for the selected tenant and bucket. It does not prove a tenant caller has query visibility, because admin diagnostics are an operator view and tenant query results still go through public policy and relationship authorisation.

Admin diagnostics use signed opaque cursors. If the response includes a non-empty next cursor, reuse it with the same filters:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin diagnostics list \
    --source mesh_routing_projection \
    --cursor '<next-cursor-from-previous-response>' \
    --limit 50
```

A cursor is bound to the principal, filters, sort order, limit, and collection revision. Do not edit it by hand or reuse it with different filters.

## Run admin repairs only with audit evidence

Admin repair can cross tenant-facing boundaries, so Anvil requires an audit reason. The command response includes `request_id`, `repair_task_id`, `status`, `scope_kind`, `scope_id`, findings, an `audit_event_id`, and details JSON.

For a tenant index repair through the admin API, include the tenant, bucket, and index explicitly:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin repair run \
    --repair-kind index \
    --tenant-id acme \
    --bucket-name documents \
    --index-name invoices_by_due \
    --rebuild \
    --audit-reason 'rebuild invoices_by_due derived index after incident INC-1234'
```

This calls `AdminService.RunRepair` with repair kind `index`. A successful response proves the principal is authorised in the system realm for `run_repair`, the audit context was accepted, the tenant and bucket resolved, and the index repair backend returned a structured report. It does not prove the tenant application can now read the repaired rows, and it should not be used instead of granting the tenant application the correct public API permissions.

For system mesh routing projection repair, the current generic admin repair command is coarse:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin repair run \
    --repair-kind mesh-routing-projection \
    --tenant-id 0 \
    --audit-reason 'repair mesh routing projection from safe diagnostics during INC-1234'
```

The implementation ignores `tenant_id` for this repair kind, but the generic CLI currently requires a value, so the example uses `0` as a placeholder. The repair scans routing projection diagnostics and only applies records marked repair-safe with the expected proposed action. Unsafe or unknown diagnostics become findings that require review. A successful `completed` or `completed_with_warnings` status does not prove every routing problem is gone; rerun the mesh diagnostics and the original route or host-alias request.

When you need to repair one known routing record, the more explicit command belongs to the mesh tutorial:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin routing repair \
    --family bucket-locator \
    --record-key '<tenant-id>/documents' \
    --expected-generation '<generation-from-routing-list>' \
    --audit-reason 'repair bucket locator projection from control stream'
```

This is a routing projection mutation, not a tenant data write. It rebuilds one materialised routing record from control-stream history. It does not move the bucket, repair object contents, or create missing source records.

## Verify after repair

A repair response is not the end of the incident. It tells you what the repair backend did at one point in time. Verification should use the operation that originally failed.

For an index incident, rerun the query. If the query depends on a recent object write and you saved an object-watch cursor, require catch-up:

The verification query uses the typed-query JSON shape from the index tutorial. `typed_predicates_json` is an array of field/operator/value predicates over fields materialised by the index build policy. `typed_order_json` is an array of stable sort fields. These are query-time inputs; they do not repair or redefine the index.

```bash
anvil --profile acme index query documents invoices_by_due \
  --typed-predicates-json '[{"field":"customer_id","op":"eq","value":"acme"}]' \
  --typed-order-json '[{"field":"due_at","direction":"asc"},{"field":"invoice_id","direction":"asc"}]' \
  --require-caught-up-to-watch-cursor "$LAST_CURSOR" \
  --limit 20
```

A successful catch-up query proves the chosen index segment processed at least the requested watch cursor. It still does not prove future writes are indexed instantly. If it fails with index lag, retry later or investigate builder health rather than treating the result as empty.

For directory repair, rerun the object listing or gateway request that was wrong. For authorisation repair, rerun the relationship check or protected operation. For PersonalDB repair, replay or verify the group through the application/client path that understands your SQLite state. For mesh repair, rerun admin diagnostics and the public route that was misrouting.

Also inspect audit evidence where the current surface records it. Admin repairs always write an admin audit event with the action `admin.repair.run` and the supplied audit reason:

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list \
    --action admin.repair.run \
    --limit 20
```

This proves the admin audit log can be queried by the current principal and that matching admin repair events are present in the returned page. It does not prove public tenant repair events were audited. In the current source, public repair service methods write repair findings but do not appear to emit tenant audit events; use the structured repair response and findings as tenant-facing evidence until that gap is closed.

## What repair does not prove

Repair does not prove source data is correct. Index and directory repair rebuild from object metadata and object bodies. If the object body is wrong, the rebuilt index faithfully reflects the wrong body. Authorisation derived repair rebuilds from tuple-journal state; if the tuple is wrong, the repaired derived userset index faithfully reflects the wrong tuple. Mesh routing repair rebuilds projections from control streams; if the source control record is wrong, the projection can still be wrong.

Repair does not bypass authorisation. Public repair commands use the caller's tenant token and public policy scopes. Admin repair commands use system-realm relations and should stay on the private admin plane. Do not expose `ADMIN_LISTEN_ADDR` or use admin credentials inside tenant publishing jobs.

Repair does not guarantee downstream consumers are caught up. Watches, index builders, projections, and application workers can still lag after source state is healthy. Store consumer checkpoints, page diagnostics completely, and use API catch-up fields where supported.

Repair is also not a fake runbook for every possible incident. The current CLI/API surfaces do not expose a public generic diagnostics list, cursor-paged public repair findings, a public name-to-finding-scope lookup, a tenant audit event for public repair runs, or a specialised admin command shape for mesh routing projection repair. When a workflow needs one of those pieces, document the gap in your incident notes instead of pretending a command exists.

## What to take forward

Use diagnostics as safe, read-only triage. Use repair only after you know the source, scope, and failing operation. Prefer tenant/public repair for tenant-owned indexes, directory indexes, tenant authz derived indexes, and PersonalDB verification. Use the private admin repair surface for operator-only system concerns such as mesh routing projections, and keep the audit reason concrete enough for later review. After every repair, rerun the original operation and any relevant diagnostics; a repair report tells you what Anvil did, not what the user can now observe.

## Success and failure cues

A diagnostic listing is evidence, not repair. A dry repair run should name the narrow target and either report no findings or create findings you can review. A rebuild is successful only after the user-visible symptom is rechecked against source data, authorisation, and freshness requirements. If a repair touches derived state, never describe it as proving the original object, tuple, stream, or PersonalDB commit was correct.

## Where to go next

Use [Watches](/tutorials/watches/) to understand the source cursors behind derived state, [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/) for index-specific lag, and [PersonalDB](/tutorials/personaldb/) for log/projection repair vocabulary. Operators should turn the checks in this page into runbooks with [Incident Response](/operators/incident-response/) and [Backup and Recovery](/operators/backup-and-recovery/).
