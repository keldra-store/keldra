---
title: Upgrades and Rollbacks
description: Upgrade Anvil as a storage, API, client, and operator event, with honest rollback limits and release-aligned evidence.
---

# Upgrades and Rollbacks

An Anvil upgrade is never just a container restart. It changes a running storage service, the public API used by tenant applications, the private admin API used by operators, the CLIs used for manual evidence, and sometimes the durable records under `STORAGE_PATH`. A safe upgrade therefore has two goals: keep serving the current workload, and preserve enough evidence that you can explain whether the new release is healthy or must be abandoned.

Treat each release as a storage and operations event. Read the release notes with the matching documentation, pin the image you intend to run, use matching public and admin CLIs, confirm client compatibility, take backups, rehearse restore where storage records may change, upgrade one scope at a time, and smoke-test source records before derived views. Do not rely on rollback as a universal escape hatch; once a release writes a format the old binary cannot read, rollback becomes restore-from-backup or roll-forward with a fix.

Read this chapter with [Release Readiness Checklist](/operators/release-readiness-checklist/), [Deployment](/operators/deployment/), [Backup and Recovery](/operators/backup-and-recovery/), [Topology Planning](/operators/topology-planning/), [Admin Plane](/operators/admin-plane/), [CoreStore Operations](/operators/corestore-operations/), [Observability](/operators/observability/), [Incident Response](/operators/incident-response/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Admin CLI](/reference/admin-cli/), and [Public CLI](/reference/public-cli/).

## What changes during an upgrade

The Docker image contains the server and both CLIs: `anvil-server`, `anvil`, and `anvil-admin`. That is useful because a release can be treated as one tested artefact. It does not mean every application client is automatically compatible, and it does not mean a new server can be downgraded after it has written new storage records.

Think about five compatibility layers:

| Layer | Upgrade question |
| --- | --- |
| Storage and CoreStore records | Can the new binary read existing blobs, refs, streams, manifests, object metadata, authz records, indexes, PersonalDB records, gateway records, diagnostics, repair findings, and topology records? Will it write records the old binary cannot read? |
| Public API | Do tenant applications, S3/static clients, and the Rust client crate understand the fields, errors, scopes, and behaviours they rely on? |
| Admin API | Do operator workflows for tenants, apps, policy, topology, repair, diagnostics, host aliases, and secret rotation still match the deployed server? |
| CLIs | Are `anvil` and `anvil-admin` from the same release as the server during the upgrade window? Old CLIs may not expose new required flags; new CLIs may call RPCs old servers do not have. |
| Derived systems | Can indexes, watches, append streams, PersonalDB projections, routing projections, and gateway views catch up after the new binary starts? |

A release that changes only a command-line help string is low risk. A release that changes source-record encoding, object metadata compaction, authz schema storage, PersonalDB certificates, index segment format, or mesh lifecycle records is not. Release notes should call that out directly. If they do not, inspect the diff before treating the release as a routine rolling update.

## Release artefacts must agree

The release image, Rust client crate, generated protobuf bindings, public CLI, admin CLI, documentation, and release notes should all describe the same commit. Mixing them creates confusing failures. For example, an operator may run a newer `anvil-admin` that sends a field an older server ignores or rejects. A tenant application may use a newer client crate that expects an error detail older servers do not return. A documentation page may describe a flag that is not in the binary currently on the node.

The release workflow in this repository is image-first. The release gates run code and documentation checks, build the docs, dry-run the Rust client crate, and run the workspace tests. The GitHub release workflow then builds a Docker image, runs Docker end-to-end tests against that image, pushes the tested image, publishes the `anvil-storage` crate if needed, and renders release notes that include the image digest, crate version, commit SHA, and docs URL. Operators should deploy the tested image digest or immutable tag, not an unpinned `latest` reference.

If you are cutting a release from source, the shared local gate is:

```bash
./scripts/release-gates.sh
```

That command proves the repository's hardening checks, documentation build, crate dry run, and workspace tests pass in the local environment. It does not publish an image, prove a production mesh can roll, prove your backup can restore, or prove a particular tenant workload is healthy. Treat it as release-candidate evidence, not deployment evidence.

Before a deployment, inspect the image you plan to run:

```bash
ANVIL_IMAGE="registry.example.com/anvil:v2026.07.07"
docker pull "$ANVIL_IMAGE"
docker image inspect "$ANVIL_IMAGE" --format '{{ index .RepoDigests 0 }}'
docker run --rm "$ANVIL_IMAGE" anvil --version
docker run --rm "$ANVIL_IMAGE" anvil-admin --version
docker run --rm "$ANVIL_IMAGE" anvil-server --version
```

These commands prove the image is pullable, has a resolved digest, and contains runnable public CLI, admin CLI, and server binaries. They do not prove the image can read your storage volume, authenticate your admin credential, or serve your region topology. Those facts require a restore drill or a controlled node upgrade.

## Preflight before touching production

Start by reading the release notes and the matching documentation as a change log for operators, not as marketing text. Look for storage format changes, new required environment variables, changed defaults, authz scope changes, public or admin API shape changes, gateway behaviour changes, index rebuild requirements, PersonalDB changes, and topology lifecycle notes. If the release touches any of those, write the validation evidence you expect before the rollout starts.

Confirm backups before upgrading. A useful backup has each node volume under `STORAGE_PATH` containing its local identity and Ed25519 signing key, server secret versions, previous secret-encryption keys, `JWT_SECRET`, internal node credential references, first-admin or named admin credentials, and the redacted configuration snapshot. If a release may write one-way storage records, run an isolated restore drill from a backup taken before the upgrade. A backup that has not been restored is only a hope.

Check capacity and lag. An upgrade can cause a burst of derived work: indexes rebuild, object metadata compaction resumes, PersonalDB projections catch up, routing projections repair, or watch consumers replay. If the cluster is already close to disk, CPU, memory, or I/O limits, the upgrade may fail because there is no room to recover. Check index lag, watch lag, PersonalDB lag, diagnostics, repair findings, and gateway error rates before the first node changes.

Check the public and admin planes. The admin API must remain private even during maintenance, and admin operations still require normal authentication and system-realm authorisation. The public API and S3/static gateway may be exposed, but that does not make the admin plane public. A load-balancer change that exposes `ADMIN_LISTEN_ADDR` is an incident, not an upgrade convenience.

## Rolling node upgrades

A rolling upgrade changes one node or cell at a time while the rest of the mesh continues serving. The safe shape is: remove the node from user traffic, record lifecycle intent where the current surface supports it, stop the old container, start the new image with the same durable storage and secret configuration, verify the node, then return traffic. Repeat only after the previous scope is healthy.

For a Docker or Compose deployment, the exact command depends on your orchestrator. The storage rule is the same: mount the same node's durable volume at the same `STORAGE_PATH`, keep the same server secret versions and node credential, preserve its committed endpoint, and do not start the volume under a different node identity. A Compose-style single-service replacement looks like this:

```bash
ANVIL_IMAGE="registry.example.com/anvil:v2026.07.07"
docker compose pull anvil
docker compose up -d --no-deps anvil
```

This proves Compose can pull and recreate the service named `anvil` from the configured image. It does not prove the service used the intended image digest unless the Compose file pins it, does not prove the volume mapping is correct, and does not prove the upgraded node is authorised or healthy. Follow it with readiness, admin, public API, and data-path checks.

For a lifecycle-aware node upgrade, first inspect the descriptor generation:

```bash
anvil-admin --host http://10.10.0.12:50052 node list \
  --region eu-west-1 \
  --cell-id eu-west-1-a \
  --limit 100
```

This proves the admin API is reachable and the operator can list node descriptors for the region/cell. It does not prove the node is safe to stop. Use the returned generation for generation-checked mutations.

A node drain records operator intent before maintenance:

```bash
anvil-admin --host http://10.10.0.12:50052 node drain \
  --node-id node-17 \
  --graceful-timeout-ms 30000 \
  --force-after-timeout \
  --expected-generation 4 \
  --audit-reason 'drain node-17 before upgrade to v2026.07.07'
```

This command proves the node descriptor was active at generation `4`, the operator was authorised to manage nodes, and Anvil stored a drain descriptor with a graceful timeout. It does not stop the operating-system process, remove the node from an external load balancer, terminate client connections, prove background ownership has moved, or complete the drain. Check diagnostics and your orchestrator before stopping the container.

Current drain completion is a limitation to plan around. The lifecycle state machine includes drained states, but the exposed CLI does not provide a clear graceful `complete drain` command for ordinary node maintenance. The available `node force-offline` path is an emergency or explicit operator action, not the same thing as graceful completion. For some deployments the practical rolling pattern is therefore external traffic drain plus process replacement, with lifecycle records used as auditable intent and diagnostics rather than as a fully automated scheduler. Document which pattern your release uses before starting.

If the replacement node is registered as a new node, register and activate it through the admin lifecycle. If you keep the same node identity and descriptor, do not invent extra activation commands unless the descriptor state requires them and the transition is supported by the current release. Activation from `joining`, `drained`, or `offline` is different from trying to activate a still-draining node.

## Region and cell considerations

A cell is typically a rack, failure domain, storage pool, or capacity slice. Upgrading one cell at a time can limit blast radius, but only if other cells have enough headroom to carry the work. A region is a placement and routing boundary. Upgrading a region affects public regional endpoints, bucket home-region traffic, gateway routes, and cross-region behaviour.

Do not introduce a new active region during an upgrade unless the activation workflow is part of the tested release plan. Region activation currently requires an activation checkpoint JSON file. The server validates that checkpoint against mesh control-stream evidence, but the documentation and CLI still lack a production-friendly checkpoint generation workflow. Do not hand-write fake checkpoint JSON to make a release move faster; keep the region in the appropriate lifecycle state until the supported workflow is available.

Region and cell drains have similar limits. Admin commands can start drains and record dispositions, but graceful completion into every drained state is still coarse or incomplete in the current exposed surface. During an upgrade, prefer small node or cell scopes, external load-balancer drains you can verify, and read-only diagnostics before making wide regional lifecycle changes.

## Smoke tests after each scope

A node that starts is not yet upgraded. `/ready` is a useful first signal, but it is intentionally narrow:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

A successful response proves the listener accepted a readiness request and CoreMeta startup recovery is ready. It does not prove the admin plane works, the storage volume was mounted correctly, object reads are durable, indexes are caught up, authenticated distributed calls work, or gateways route correctly.

Follow readiness with an admin diagnostic from the private plane:

```bash
docker exec \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  "node-eu-west-1-a" \
  anvil-admin diagnostics list --source mesh_lifecycle --limit 50
```

This proves the matching admin CLI can authenticate through the public auth path, reach the private admin listener from inside the node, and read mesh lifecycle diagnostics. It does not prove tenant applications can authenticate, and it does not scan every CoreStore record.

Then use a tenant smoke profile through the public plane:

```bash
anvil --profile upgrade-smoke bucket ls
anvil --profile upgrade-smoke object head s3://documents/tutorial/welcome.txt
anvil --profile upgrade-smoke index list documents --include-disabled
```

The bucket list proves public authentication and bucket listing for that tenant app. The object head proves a source object metadata path is readable. The index list proves index definitions for that bucket are visible to the caller. These commands do not prove every object body is intact, every version is present, every index is current, or every watcher has checkpointed. Choose representative buckets and objects from your own service-level objectives.

A complete post-scope smoke test should also include the paths your product actually exposes: S3 signed `HEAD` or `GET`, public-read static reads where deliberate, reserved namespace rejection, append stream read/tail, task lease acquire/checkpoint/commit if used, PersonalDB group read or catch-up, authz tuple check for protected data, and index queries for the key index families. Keep the evidence separate. A successful full-text query is not proof that object storage is healthy; a successful object read is not proof that vector search has caught up.

## Client and CLI compatibility during rollout

Use the CLI from the server release when running upgrade evidence. The easiest method in Docker deployments is to run `anvil` and `anvil-admin` from the same image you are deploying, either with `docker run --rm "$ANVIL_IMAGE" ...` for offline help/version checks or `docker exec` inside a node for private admin checks. That avoids old local binaries hiding new flags or new local binaries calling RPCs the old server does not expose.

Application clients need a staged plan. If the new Rust `anvil-storage` crate only uses fields and behaviours that the old server understands, applications can often roll before or during the server upgrade. If the client depends on a new RPC, new error shape, new authz behaviour, or new gateway feature, roll the server side first and keep the client feature disabled until smoke tests prove the server release is present everywhere it needs to be. If the server removes or changes behaviour an old client depends on, the release is a coordinated client/server event rather than a routine server rollout.

During mixed-version windows, be conservative. Public API requests may route to upgraded and not-yet-upgraded nodes depending on topology and load balancing. Admin CLI calls should target a known admin endpoint. Gateway clients may be sensitive to host routing and SigV4 behaviour. If a behaviour must be uniform before enabling a feature, do not enable it until all relevant nodes and clients are on compatible versions.

## Rollback is not always downgrade

Rollback means returning the service to a known-good state. Sometimes that is as simple as restarting a node with the previous image. Sometimes it is impossible without restoring storage from backup. The difference is whether the new release changed durable state in a way the old release can still read and safely ignore.

A safe image rollback requires all of these to be true: the release notes or source review say no one-way storage/internal format change occurred; no migration has removed or rewritten records required by the old binary; public and admin API clients can tolerate the old behaviour again; and you have smoke-tested the old image against a restored copy or a non-critical node. If those are not true, the rollback plan is restore the pre-upgrade backup into a controlled environment, cut traffic back to that state if acceptable, or roll forward with a fix.

Do not treat derived state as the only rollback target. If a new index builder wrote a bad derived segment but source objects are intact, repair or rebuild may be enough. If a new binary wrote source records the old binary misinterprets, rebuilding indexes will not make the old server safe. Source-record compatibility is the line that matters.

A practical rollback decision table looks like this:

| Situation | Response |
| --- | --- |
| New image fails before writing to the durable volume | Stop it and restart the previous image with the same volume and secrets, then run smoke tests. |
| New image starts and writes only compatible records | Roll back the image if release notes/source review and smoke tests support that path. |
| New image writes one-way storage or internal records | Do not downgrade in place. Restore from a pre-upgrade backup or roll forward. |
| Derived index or projection is wrong but source reads are healthy | Keep the server version if otherwise safe, run targeted diagnostics and repair, then verify the original query. |
| Admin or public API compatibility breaks clients | Decide whether clients can be rolled forward, feature-flagged, or routed to compatible nodes; if not, follow the storage-aware rollback plan. |

## Full deployment validation

After all nodes in the intended scope are upgraded, repeat the smoke tests through the real external paths, not only from inside one container. Test the public regional endpoint, S3/static gateway endpoint, admin private endpoint from the operator network, and any reverse proxy or trusted-forwarded-host path. Verify logs include request ids and safe routing evidence without leaking bearer tokens, S3 secrets, app secrets, or object bodies.

Use diagnostics and audit as release evidence:

```bash
anvil-admin --host http://10.10.0.12:50052 diagnostics list \
  --source mesh \
  --limit 100

anvil-admin --host http://10.10.0.12:50052 audit list \
  --action admin.node.drain \
  --limit 20
```

The diagnostics command proves the admin service can return mesh findings after the upgrade. The audit command proves upgrade-related admin mutations are visible. Neither command proves the upgrade is complete by itself. Pair them with public API smoke tests, gateway checks, source object reads, index queries, watches, PersonalDB checks, repair findings, and load/error dashboards.

Finally, record the release evidence: image digest, server version, public CLI version, admin CLI version, Rust client crate version where applicable, documentation URL, backup id, restore-drill id or reason it was not required, nodes upgraded, smoke tests run, failures found, repairs performed, and rollback decision. This record is what lets the next operator distinguish a known release condition from a new incident.

## Current surfaces and gaps

The current repository supports an image-first operational model and exposes public/admin CLIs, diagnostics, repair, topology descriptors, node drains, region/cell/node lifecycle mutations, and release gates. It does not expose every ideal upgrade primitive as a finished production workflow.

Current limitations to plan around:

| Area | Current limitation |
| --- | --- |
| Region activation | `region activate` exists, but it requires a real activation checkpoint file and there is no documented production-friendly checkpoint generation command yet. |
| Drain completion | Node, cell, and region drain initiation exists, but graceful completion into drained states is incomplete or coarse in the exposed CLI/API surface. |
| Rollback certification | There is no single command that proves an older binary can read a volume after a newer binary has run. Release notes, source review, and isolated restore tests carry that evidence. |
| CoreStore verification | Feature diagnostics and repairs exist, but there is no general `corestore fsck` command that certifies every blob, ref, stream, transaction, manifest, and derived record. |
| CLI/reference drift | Some reference examples can lag the compiled CLI. For upgrade-critical commands, prefer the CLI help and source for the release you are deploying. |
| Distributed recovery | The current local backend uses local shard/control-replica machinery inside one storage path; do not treat image rollback or node restart as multi-region disaster recovery. |

Design upgrades around those facts. Pin artefacts, keep CLIs and clients compatible, drain narrowly, verify source records before derived state, document one-way changes, and choose rollback only when the storage state makes rollback safe.

## Format and CLI compatibility

Before rollback, identify whether the newer version wrote durable records that the older version cannot parse. If it did, restoring the pre-upgrade backup is safer than starting old binaries on new data. Also keep CLI versions aligned with the server release; an older CLI can omit fields such as generation, idempotency, catch-up, or lifecycle options that the runbook relies on.

After upgrade, run one public CLI smoke test and one admin CLI smoke test from the same network locations operators and applications use. That catches endpoint, DNS, and proxy issues that unit tests cannot see.
