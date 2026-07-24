---
title: Backup and Recovery
description: Back up Anvil's CoreStore state, server secrets, identity material, configuration, and recovery evidence, then prove restore in isolation.
---

# Backup and Recovery

A backup is not a copy of some files that happened to exist at midnight. A backup is a recovery promise: if a node, volume, secret, release, region, or operator account is lost, you can reconstruct enough of Anvil to read source records, authenticate operators, explain what happened, and resume service without inventing state.

Anvil's recovery boundary starts with CoreStore under `STORAGE_PATH`, but it does not end there. Durable storage without server key history may be unreadable. Server keys without the matching storage are not useful. A restored volume contains its node-local identity and Ed25519 signing key, which must match committed topology. A restored system without an admin credential may be running but unmanageable. A restored index may answer queries, but if the source records behind it are missing, that index is not a recovery source of truth.

Read this page with [CoreStore Operations](/operators/corestore-operations/), [Secrets and Key Management](/operators/secrets-and-key-management/), [Deployment](/operators/deployment/), [Admin Plane](/operators/admin-plane/), [Network and Ports](/operators/network-and-ports/), [Observability](/operators/observability/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/), [CoreStore](/learn/corestore/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Admin CLI](/reference/admin-cli/), and [Public CLI](/reference/public-cli/).

## The backup boundary

The primary backup unit is each Anvil node's durable storage directory. In the Docker image this is normally `/var/lib/anvil`, because the image sets `STORAGE_PATH=/var/lib/anvil`. In raw binary or custom deployments it is whatever `STORAGE_PATH` points to. Node identity and the local Ed25519 receipt-signing private key are node-local CoreMeta rows inside this directory, so restoring or cloning the volume also restores or clones that identity.

CoreStore-backed state includes object bodies, object metadata journals, bucket records, refs, streams, transactions, tenant records, app credential envelopes, public policy state, relationship authorisation records, indexes and index definitions, watch evidence, append streams, task and lease state, gateway records, PersonalDB groups and commits, repair findings, diagnostics, routing and lifecycle records, and audit records. Some of those are source records. Others are derived state. Both live in the backup, but they have different recovery meaning.

A complete recovery set also includes material outside the volume:

| Item | Why it belongs in the recovery set |
| --- | --- |
| `JWT_SECRET` | Required for minted bearer-token compatibility. Changing it invalidates outstanding tokens signed with the old value. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Required to decrypt stored server-side secret envelopes and encrypted shard payloads that use the active key id. |
| `ANVIL_SECRET_ENCRYPTION_KEY_ID` | Labels the active key used for new envelopes. It tells you which key protected records at backup time. |
| `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` | Required when the backup contains envelopes still labelled with older key ids. |
| Internal node credential reference | Required for a restored distributed node to authenticate its gRPC calls as the expected node principal. Store the credential in a secret manager, not in the volume archive. |
| First-admin and named admin credentials | Needed to manage the restored system through the normal admin API. Store them in a secret manager, not only inside the Anvil volume. |
| Tenant/app client secrets where the service owns them | Needed for applications to mint new public API tokens after restore. Rotate deliberately if exposure is suspected. |
| Redacted configuration snapshot | Captures image digest, env names, secret key ids, region/cell/node settings, ports, proxy settings, and volume mappings. |
| Topology and runbook evidence | Tells operators which volumes, addresses, nodes, and restore tests belong together. |

Do not include live bearer tokens as backup material. They expire and are incident evidence, not long-lived recovery credentials. Do protect logs and audit records; they often explain why a backup or restore is being used.

## Source records and derived state

The most useful recovery distinction in Anvil is source versus derived. Source records are the durable facts: an object version was committed, an append stream record was accepted, a PersonalDB changeset was witnessed, a tuple was written, a bucket was created, a host alias changed state, or an admin repair was requested. Derived state is built from those facts: path listings, full-text segments, vector indexes, typed query materialisations, authz derived usersets, PersonalDB projections, routing projections, and caches.

A volume backup normally captures both. That is convenient because a restored system may serve faster when derived state is already present. It is not a reason to treat derived state as the backup of source truth. If an index segment is missing but object source records remain, repair can often rebuild. If object source records or PersonalDB commit records are missing, an index or projection cannot safely recreate them.

This is why restore validation must read source records first, then derived views. A successful search query proves a search path, not object durability. A successful object read proves a source object path. A successful PersonalDB catch-up proves the witnessed log chain can be replayed. A successful admin audit read proves operator evidence exists. Use those as separate smoke tests.

## Docker-first storage model

In Docker, make the storage boundary visible. Use one persistent volume per live Anvil node unless the deployment has explicitly designed a different storage model. Do not run production with `STORAGE_PATH` on the container's writable layer, and do not mount one writable Anvil storage directory into multiple live server containers.

A quick mount check is useful before writing a backup runbook:

```bash
docker inspect node-eu-west-1-a \
  --format '{{ range .Mounts }}{{ .Destination }} <- {{ .Name }}{{ println }}{{ end }}'
```

This asks Docker how the container's filesystems are mounted. It proves which named volume or bind mount is attached to `/var/lib/anvil` for that container. It does not prove the volume is durable, encrypted, backed up, or attached to the correct logical node. Follow it with storage-class, snapshot-policy, and restore-drill evidence from your platform.

For Compose, put `STORAGE_PATH=/var/lib/anvil` in the service environment, mount a named volume at `/var/lib/anvil`, and keep server secrets outside the Compose file. `docker compose config` proves the Compose file parses and variables interpolate; it does not prove the backup boundary is correct. For Kubernetes, use persistent volumes for `STORAGE_PATH`, Secrets or an external secret operator for server key material, and a deployment-specific restore job or documented manual restore process. The repository does not currently ship a complete Helm chart, so Kubernetes backup and restore are operator-owned manifests.

## Taking a consistent backup

A backup must be consistent enough for CoreStore refs, streams, blobs, and transactions to agree. The current repository does not document a general online backup checkpoint or freeze API. That means a simple file copy while the process is writing is not a complete strategy. Use your storage platform's crash-consistent or application-consistent snapshot mechanism, or quiesce the node before copying the volume.

For a small single-node Docker deployment, a conservative manual backup is to stop the container, archive the named volume, then start the container again. This is not a zero-downtime production workflow; it is a clear baseline for drills and internal systems.

```bash
docker stop node-eu-west-1-a
mkdir -p ./anvil-backups

docker run --rm \
  -v node-eu-west-1-a:/var/lib/anvil:ro \
  -v "$PWD/anvil-backups":/backup \
  debian:bookworm-slim \
  sh -c 'tar -C /var/lib/anvil -czf /backup/node-eu-west-1-a-$(date -u +%Y%m%dT%H%M%SZ).tar.gz .'

docker start node-eu-west-1-a
```

The `docker stop` command quiesces that container. The `docker run` command mounts the Anvil volume read-only and writes a tar archive to the host directory. The `docker start` command resumes service. This proves the volume can be read and archived while the server is stopped. It does not prove the archive can restore, does not back up secrets from your secret manager, does not cover other Anvil node volumes, and does not provide continuous availability.

For a multi-node mesh, back up every node storage volume that can hold durable state. Do not back up one gateway node and call that a mesh backup unless your topology and implementation prove that all source records live elsewhere and are backed up there. The current local backend has useful local quorum and `4+2` blob-shard machinery, but the replicas are local to the storage path. There is no current documented automatic multi-region disaster-recovery system or general remote shard rebalancer that lets you ignore node volumes.

## Secret and key history backups

Server secrets are part of restore, but they should not be stored in the same public archive as ordinary runbook text. Use a secret manager with version history and access audit. The recovery set for one backup should record which secret versions were active at backup time, without exposing their values in tickets or dashboards.

`ANVIL_SECRET_ENCRYPTION_KEY` and `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` require special care. If a backup was taken while old envelopes still existed, restoring that backup after you have deleted the old key history can make encrypted app secrets or integration secrets unreadable. Keep previous keys at least as long as backups may contain records encrypted with those key ids, or run and verify envelope rotation before reducing key history. The admin dry-run is useful evidence after a restore:

```bash
anvil-admin --host http://10.10.0.12:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason 'verify restored secret envelopes can decrypt'
```

This asks the restored server to inspect known encrypted envelopes using the configured active and previous keys. It proves the admin API is reachable, the caller has the system-realm relation for secret-envelope rotation, and the server can begin the rotation workflow with the configured keyring. It does not prove every possible tenant workflow works, and with `--dry-run` it does not rewrite records.

`JWT_SECRET` has a different recovery shape. Restoring with a different value usually invalidates outstanding bearer tokens, but stored data may still be readable after applications mint new tokens. Internal node credentials are also authentication material rather than data-decryption keys. A missing, expired, or wrongly authorised node credential prevents distributed calls without making the stored records unreadable. Restore those credentials from the secret manager and test authenticated gRPC separately from decryption.

## Identity, bootstrap, and admin access

The stable node id and Ed25519 signing private key are stored in node-local CoreMeta under `STORAGE_PATH`. A volume backup therefore includes identity automatically. Portable CoreMeta bootstrap data does not replace this node-local state. Losing the volume loses that identity; restoring one volume into two simultaneously running nodes duplicates it.

Restoring a node identity into an isolated drill is safe only if the drill cannot contact production node endpoints. Restoring the same identity while the old production node is still running is a split-brain risk. For production replacement, decide whether you are restoring the same logical node from backup or registering a new node and signing key through lifecycle control. The local public key must match the key committed for that node id. Do not solve a mismatch by editing CoreMeta files.

The first-admin credential is also part of recovery, but it is not a magic bypass. It is an app credential that can mint bearer tokens for a system principal. On first boot, Anvil can write that credential to `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH` if the system realm is absent. Once the system realm exists, startup does not create a new system owner just because you set bootstrap variables again. Store the first credential and later named admin credentials in a secret manager. If all admin credentials are lost after the system realm exists, that is an operational break-glass problem, not a normal restart path.

## Configuration snapshots

Back up configuration as evidence, not as a pile of secrets. A useful snapshot names the release image digest, command-line or environment names, redacted secret references, key ids, region, cell, node id, storage volume, public and admin listener addresses, committed `PUBLIC_API_ADDR`, internal node credential reference, genesis `BOOTSTRAP_NODE_IDS`, proxy and host-routing settings, and feature thresholds such as PersonalDB snapshot thresholds.

A redacted configuration snapshot lets operators answer: which volume belongs to this node, which key versions are needed to read it, which admin credential can manage it, which public endpoint should be used for smoke tests, and whether the restored identity is allowed to contact production nodes. Without that map, recovery becomes guesswork.

Do not restore old configuration blindly. A backup from before a key rotation may need older key history. A backup from before an address change should not advertise the old public address in an isolated drill. A backup from before a region lifecycle change may contain routing records that need diagnostics after restore. Preserve the original config and then choose the restore config deliberately.

## Isolated restore drill

A restore drill should start away from production. Use a separate Docker network, different host ports, no routes to committed production node endpoints, no production node credential, and no public load-balancer routes. The aim is to prove that the backup and key history are sufficient without allowing an old copy of a node identity to participate in production.

For a single-node drill from the tar archive above:

```bash
docker network create anvil-restore
docker volume create anvil-restore-node-a

docker run --rm \
  -v anvil-restore-node-a:/restore \
  -v "$PWD/anvil-backups":/backup:ro \
  debian:bookworm-slim \
  sh -c 'tar -C /restore -xzf /backup/node-eu-west-1-a-20260707T010000Z.tar.gz'
```

The first command creates an isolated network. The second creates a fresh restore volume. The third extracts one archived storage volume into that restore volume. This proves the archive can be unpacked into a Docker volume. It does not prove Anvil can start, that secrets are correct, or that the restored data is healthy.

Start the restored server with the restored volume and the secret versions that match the backup. Use non-production ports and addresses:

```bash
docker run -d \
  --name anvil-restore-node-a \
  --network anvil-restore \
  -p 15051:50051 \
  -v anvil-restore-node-a:/var/lib/anvil \
  -e STORAGE_PATH=/var/lib/anvil \
  -e REGION=eu-west-1 \
  -e CELL_ID=restore-a \
  -e API_LISTEN_ADDR=0.0.0.0:50051 \
  -e PUBLIC_API_ADDR=http://127.0.0.1:15051 \
  -e ADMIN_LISTEN_ADDR=127.0.0.1:50052 \
  -e JWT_SECRET="$RESTORE_JWT_SECRET" \
  -e ANVIL_SECRET_ENCRYPTION_KEY_ID="$RESTORE_KEY_ID" \
  -e ANVIL_SECRET_ENCRYPTION_KEY="$RESTORE_SECRET_ENCRYPTION_KEY" \
  -e ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS="$RESTORE_PREVIOUS_KEYS" \
  local/anvil:operator
```

This starts a restored single-node drill container. It proves only that the process accepted the configuration and is attempting to serve from restored storage. It does not prove the restored identity should rejoin production, that a multi-node mesh can be recovered from one volume, or that every derived view is current. For distributed drills, restore every relevant volume into an isolated network, provision drill-only node credentials, and prevent access to production endpoints.

## Restore smoke tests

After the restored process starts, run smoke tests that prove different parts of the recovery boundary. Start with readiness:

```bash
curl -fsS http://127.0.0.1:15051/ready
```

A successful response proves the restored listener is running and CoreMeta startup recovery is ready. It does not prove admin access, data readability, object durability, indexes, watches, credentials, or distributed quorum.

Next prove admin access with a credential restored from the secret manager. If the first-admin credential file is present in the restored volume, the command shape is:

```bash
docker exec \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  "anvil-restore-node-a" \
  anvil-admin diagnostics list --limit 20
```

This proves the admin CLI can mint or use an admin token, reach the private admin listener from inside the container, and read diagnostics through the system realm. It does not prove tenant applications can authenticate or that every admin relation is granted. If the credential file was deliberately removed from storage after bootstrap, mount or configure the equivalent named admin credential from your secret manager instead.

Then prove source data through the public plane. Use a restored tenant/application profile created from a secret-manager copy of the app credential:

```bash
anvil --profile restore-smoke bucket ls
anvil --profile restore-smoke object head s3://documents/tutorial/welcome.txt
```

The bucket list proves the restored public API can authenticate that tenant app and list buckets visible to it. The object head proves a specific source object metadata path is readable. These commands do not prove every object body is intact, every version is present, or every derived index is current. Choose representative buckets, pinned object versions, PersonalDB groups, append streams, and gateway paths from your own service-level objectives.

Derived-state smoke tests should say what they prove. An index diagnostic check is useful:

```bash
docker exec \
  -e ANVIL_BOOTSTRAP_CREDENTIAL_FILE=/var/lib/anvil/bootstrap/first-admin.json \
  -e ANVIL_PUBLIC_ENDPOINT=http://127.0.0.1:50051 \
  -e ANVIL_ADMIN_ENDPOINT=http://127.0.0.1:50052 \
  "anvil-restore-node-a" \
  anvil-admin diagnostics list \
    --source index \
    --tenant-id acme \
    --bucket-name documents \
    --limit 50
```

This proves the restored admin diagnostics backend can read index diagnostics for that tenant/bucket. It does not prove the index is caught up or that queries will match every source object. If diagnostics or user queries show stale derived state, run the narrow repair documented for that feature and verify the original symptom afterwards.

## Recovery runbooks

A volume-loss runbook should start by deciding whether this is a node restore, node replacement, or mesh restore. For a node restore, restore that node's volume while ensuring the old node is not running; the volume carries the node id and signing key. For replacement, create a fresh node identity and commit its descriptor through lifecycle control before moving placement or routing. For a mesh restore, restore every relevant node volume, server secret version, internal credential, and configuration snapshot into an isolated environment before any production cutover.

A secret-loss runbook depends on the secret. If an application client secret is lost but an admin or tenant owner still exists, rotate that app secret and update the service. If `JWT_SECRET` is lost, existing tokens cannot be verified after replacement; plan a token refresh event. If `ANVIL_SECRET_ENCRYPTION_KEY` or a required previous key is lost, encrypted records that need it may not decrypt from backup. If an internal node credential is lost, replace it for that principal and verify authenticated distributed operations. Do not expect a storage backup to recover a secret held only in the secret manager.

A suspected-corruption runbook should preserve evidence before repair. Copy logs, record request ids, note the volume snapshot id, list relevant diagnostics, and avoid deleting files under `STORAGE_PATH`. Run read-only diagnostics first. Use feature-specific repair only when it rebuilds derived state from source records or writes a repair finding. If source records are missing or invalid, repair may not be able to reconstruct them; use backup restore or product-specific recovery.

A key-rotation recovery runbook should match the backup to key history. If you restore a backup taken before secret-envelope rotation completed, configure both the active key from that time and the previous keys needed by old envelopes. Run a dry-run rotation in the restored environment to prove decryption, then decide whether to rotate envelopes forward in that environment. Do not remove previous keys from production just because current production can decrypt; old backups may still need them until they expire by policy.

## What to record for every backup

A backup without metadata is hard to trust. Record enough to replay the recovery decision later:

| Field | Example content |
| --- | --- |
| Backup id and time | UTC start/end, snapshot id, archive path, storage backend id. |
| Node and volume | Node id, region, cell, volume name, `STORAGE_PATH`, signing public-key fingerprint, and committed endpoint. |
| Release | Image digest or binary version, schema/storage-format notes, migration state. |
| Secret versions | Key ids and secret-manager versions for JWT, encryption keys, previous keys, internal node credentials, and admin credentials. |
| Scope | Single node, all nodes in a cell, all nodes in a region, or full mesh. |
| Consistency method | Stopped container, storage snapshot, filesystem snapshot, or orchestrator-managed snapshot. |
| Restore test | Last restore drill id, environment, tests run, failures, repair commands, and duration. |
| Retention and deletion | How long storage and key history are retained together. |

Do not store raw secret values in the backup metadata. Store secret-manager references and key ids. Keep the metadata protected because tenant names, bucket names, object keys, and topology details can still be sensitive.

## Current surfaces and gaps

The current operational surface is intentionally conservative. The repository exposes feature diagnostics and repairs through public and admin APIs, but there is no documented `corestore fsck` command that certifies every blob, shard, stream, ref, transaction, object manifest, PersonalDB log, gateway record, and index segment. There is also no documented online backup checkpoint API, no bundled Helm chart, and no automatic multi-region disaster-recovery workflow that operators can rely on without their own storage snapshots and restore procedures.

The current local CoreStore backend uses local shard/control-replica machinery inside one storage path. That improves local integrity checks, but it is not proof that a node can lose its whole volume and reconstruct from other regions. Cross-region routing, proxying, activation checkpoints, and drains have their own current gaps, so do not describe a mesh as disaster-recovery-capable until you have a tested recovery design that matches the deployed code.

The practical posture is simple: protect every node volume, keep server key history with backup retention, preserve node identity intentionally, store admin credentials in a secret manager, take configuration snapshots, test restores in isolation, verify source records before derived views, and document any gap as an implementation or operations gap rather than papering over it with direct storage edits.

## Restore acceptance checklist

A restore is acceptable only after both source and derived behaviours are verified. Check token exchange, bucket listing, object write/read, prefix listing, link read, at least one index query, relationship authorisation check, PersonalDB catch-up if used, admin diagnostics, routing list, and repair finding listing. Record the image digest and secret key ids used for the restored environment.

If the restore passes object reads but fails app token exchange, suspect missing or wrong secret encryption keys. If token exchange works but routing is wrong, suspect topology records or routing projection state. If routing works but search is stale, use index diagnostics and rebuild rather than rejecting the whole backup immediately.
