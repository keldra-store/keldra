---
title: Secrets and Key Management
description: Understand Anvil server secrets, bootstrap credentials, app secrets, bearer tokens, blast radius, rotation, and recovery.
---

# Secrets and Key Management

Anvil uses several kinds of secret material, and they do different jobs. A server signing secret is not the same as an application client secret. A server-side encryption key is not an administrator password. A bootstrap credential is not a permanent bypass. A bearer token is not a long-lived identity record. Treating all of them as "tokens" leads to dangerous runbooks because rotation, blast radius, and recovery are different for each one.

This page explains the secret types operators must manage before a production deployment. Read it with [Deployment](/operators/deployment/), [Network and Ports](/operators/network-and-ports/), [Admin Plane](/operators/admin-plane/), [Security Hardening](/operators/security-hardening/), [Backup and Recovery](/operators/backup-and-recovery/), [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/), [Run Anvil Locally](/tutorials/setup-local-anvil/), [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## The Secret Inventory

Operators should be able to name every secret in a running deployment and answer four questions about it: who holds it, what it can do, what breaks when it changes, and how it is recovered.

| Secret | Who should hold it | Main blast radius |
| --- | --- | --- |
| `JWT_SECRET` | Anvil server processes that mint or verify bearer tokens | Changing it invalidates outstanding bearer tokens signed with the old value unless every verifying node changes in a coordinated window. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Anvil server processes only | Losing it can make encrypted server-side secrets unrecoverable; leaking it exposes encrypted secret envelopes if storage is also available. |
| `ANVIL_SECRET_ENCRYPTION_KEY_ID` | Anvil server configuration and operator records | Labels new encrypted envelopes; changing it without changing the key mostly changes metadata, but it must remain consistent and meaningful. |
| `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` | Anvil server processes during rotation | Allows old envelopes to decrypt while rotation rewrites them to the active key id. |
| Purpose-scoped PersonalDB Ed25519 key | The in-process PersonalDB signing provider | Signs one allowed class of PersonalDB control evidence. Private material is held in encrypted System Realm records and is never returned by the admin plane. Loss prevents new signatures for the affected scope; leakage can forge evidence within that key's purpose, generation, scope, and log boundaries. |
| `CORESTORE_INTERNAL_BEARER_TOKEN` | One Anvil node process | Authenticates that node's calls to internal gRPC services. A missing or invalid credential makes remote writes, quorum, proxying, and recovery fail closed. |
| Local node Ed25519 signing key | One Anvil node's local CoreMeta store | Signs storage receipts and evidence. The private key remains in that node's `STORAGE_PATH`; its public key is committed in the lifecycle descriptor. |
| Bootstrap first-admin credential | Initial system administrators or provisioning automation | Can mint a token for a powerful system principal until rotated, deleted, or access is otherwise removed. |
| Tenant/app client secrets | The service or automation that owns the app credential | Can mint short-lived bearer tokens with that app's delegated public policy scopes. |
| Bearer tokens | Processes making API calls | Authorise one caller for a short period; current tokens are minted with a one-hour expiry. |

Store server secrets in an operator secret manager. Store application client secrets in the secret manager for the service that uses them. Do not put any of these values in source control, image layers, public Compose files, shell history, monitoring labels, support tickets, or screenshots.

## `JWT_SECRET`: Signing Short-Lived Tokens

`JWT_SECRET` is the secret Anvil uses to sign and verify bearer tokens. When an app credential calls the authentication API, Anvil returns a bearer token with a subject, tenant id, scopes, expiry, and token id. Current tokens expire after about one hour. Every Anvil process that must accept those tokens needs the same signing secret, or an otherwise compatible signing configuration for the release you operate.

The blast radius is serving authentication rather than stored data. If `JWT_SECRET` changes abruptly on one node, tokens minted or accepted by other nodes may fail verification. If it leaks, an attacker with enough knowledge of claims could forge bearer tokens until you rotate the secret and old forged tokens expire or are rejected. Rotate it as a coordinated deployment event: update the server secret, restart or roll nodes according to your deployment strategy, and expect active clients to refresh tokens.

There is no current multi-key JWT rotation surface documented in the public operator CLI. That means a rotation is more disruptive than the server-side envelope rotation described later. Plan it for a maintenance window or a controlled rolling process, and verify public authentication and admin authentication after the change.

## `ANVIL_SECRET_ENCRYPTION_KEY`: Protecting Stored Secret Envelopes

`ANVIL_SECRET_ENCRYPTION_KEY` is a 32-byte hex key used by Anvil servers to encrypt persisted server-side secrets. Examples include stored application client-secret envelopes and configured integration secrets. It is server-only material. The network `anvil-admin` CLI does not need this key because the CLI never decrypts storage itself; it asks the server to perform authorised operations through the admin API, and the server already has its configured keyring.

Generate the key with the local helper in `anvil-admin`:

```bash
anvil-admin key generate-secret-encryption-key
```

This command does not contact a server. It prints one random hex value suitable for `ANVIL_SECRET_ENCRYPTION_KEY`. The explanatory warning is printed separately so operators understand that losing the key can make encrypted secrets unrecoverable. Generate one active key for a storage cluster, store it in a secret manager, and inject it only into Anvil server processes. Do not hand it to tenant applications, CI jobs that only call APIs, public CLIs, or operators running network-only admin commands.

The recovery implication is severe: a backup of `STORAGE_PATH` without the key history needed to decrypt its envelopes may be incomplete. A key without the matching storage is also not useful. Backup plans must protect both durable storage and the relevant secret key history.

## PersonalDB Protocol Signing Keys

PersonalDB asymmetric signing is optional and runs inside the Anvil server
process. It does not require a signer binary, Unix socket, signing manifest, or
external key-management service. The absence of PersonalDB signing keys must
not prevent server startup or affect object, stream, or index operations.
PersonalDB makes a clean break to Ed25519 evidence: an operation that needs a
PersonalDB signature fails closed when no active key matches its purpose and
scope, while unrelated Anvil operations remain available.

Provision and rotate keys through authenticated, authorised, and audited admin
operations. Anvil stores each private key as an encrypted System Realm/CoreStore
record using the normal server-side secret-encryption keyring. The in-process
provider decrypts private material only while producing a signature and does not
return it through admin responses or audit records. Public trust metadata stays
available for verification without exposing the private key. The admin principal
must hold the System Realm `manage_personaldb_signing_keys` Zanzibar relation;
ordinary secret rotation and PersonalDB signing-key custody are separate
capabilities.

Each key record binds its canonical key ID and generation to one signature
purpose, tenant/database scopes, validity boundaries in the PersonalDB log,
status, public key, encrypted PKCS#8 private key, and audit metadata. Keep these
scopes narrow. A key selected for one purpose must never sign an object for
another purpose, even when both objects belong to the same database or group.

Anvil signs four classes of PersonalDB server evidence, each with an independent
key: `group-control`, `proposal-admission`, `snapshot`, and `witness`. Generate
PKCS#8 DER keys outside the repository with owner-only permissions, then import
them over the private admin listener. For example:

```bash
umask 077
for purpose in group-control proposal-admission snapshot witness; do
  openssl genpkey -algorithm ED25519 -outform DER -out "$purpose.pk8"
  anvil-admin personal-db-signing-key import \
    --private-key-pkcs8 "$purpose.pk8" \
    --key-generation 1 \
    --purpose "$purpose" \
    --status active \
    --audit-reason "initial PersonalDB signing key provision"
done
```

The CLI validates the input file and derives the public key locally; the admin
response contains only public trust metadata. Delete or move the import files
into the deployment's approved secret backup after provisioning. Restart Anvil
when an import or trust-status response reports `runtime_reload_required: true`.
The server continues to reject new PersonalDB signing work until the required
purpose is active after that reload.

Inspect public key state with:

```bash
anvil-admin personal-db-signing-key list
```

Status transitions are one-way. Use the record revision returned by `list` as
the expected generation and set an exclusive log boundary when retiring or
revoking a key:

```bash
anvil-admin personal-db-signing-key set-status \
  --key-id 'sha256:<canonical-public-key-id>' \
  --status retiring \
  --valid-until-log-index 250000 \
  --expected-generation 1 \
  --audit-reason "rotate PersonalDB witness generation"
```

The storage-backed provider checks current status on every signing operation,
so a non-active transition stops new signatures immediately. Restart promptly
when requested so the in-memory verification trust view also reflects the new
historical boundary.

Key IDs are SHA-256 over `personaldb-protocol`'s canonical deterministic
protobuf public-key envelope, not over a PEM file or textual public key. The
provider derives generation, purpose, scopes, status, and log boundaries from
the durable key record and verifies those constraints before signing. Anvil
verifies every produced canonical envelope before accepting it. In the current
Anvil PersonalDB model, the database ID is also the protocol group ID, so a
non-empty `group_scopes` entry must use that same value. Leave either scope list
empty only when the key is intentionally unrestricted on that dimension.

For rotation, retain old public keys as `retiring` with an exclusive
`valid_until_log_index` so historical evidence below that boundary remains
verifiable, and install a new `active` generation for new signatures.
`revoked_future` and `compromised` records also require an exclusive
`valid_until_log_index`; evidence at or after that boundary fails closed.
Removing an old public key also removes the ability to verify retained history
that it signed. Include encrypted PersonalDB key records and the server-side
secret-encryption key history needed to decrypt them in the same backup and
recovery boundary.

## Key IDs And Previous Keys

`ANVIL_SECRET_ENCRYPTION_KEY_ID` is written into new encrypted envelopes. It lets Anvil know which configured key should decrypt a record later. Keep ids human-readable and stable, such as `2026-07-primary`. Do not reuse the same id for different key material.

`ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` is a comma-delimited list of `key_id:hex_key` entries. It is used during rotation so records encrypted with old key ids remain readable while the active key rewrites them. A typical rotation configuration looks like this:

```bash
ANVIL_SECRET_ENCRYPTION_KEY_ID=2026-08-primary
ANVIL_SECRET_ENCRYPTION_KEY=new_64_hex_key_from_secret_manager
ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS=2026-07-primary:old_64_hex_key_from_secret_manager
```

This configuration says: encrypt new envelopes with `2026-08-primary`, but still decrypt envelopes labelled `2026-07-primary`. Previous keys should remain configured only for the rotation window and verification period. Keeping old keys forever increases the blast radius of an old leak.

## Rotating Server-Side Secret Envelopes

Server-side envelope rotation has two phases. First, deploy the new keyring configuration so servers know both the new active key and the previous key. Secondly, call the admin API to re-encrypt existing envelopes with the active key id.

Run a dry run first:

```bash
anvil-admin secret-encryption-key rotate --dry-run \
  --audit-reason 'verify secret envelope rotation to 2026-08-primary'
```

The dry run proves the authenticated admin principal has the system-realm relation for managing secret encryption keys, the admin API can inspect the relevant encrypted records, and the server can decrypt them with the configured keyring. It does not rewrite records.

Run the real rotation only after the dry run is clean:

```bash
anvil-admin secret-encryption-key rotate \
  --audit-reason 'rotate secret envelopes to 2026-08-primary'
```

The current server rotates application secret envelopes and configured integration secret envelopes that the rotation backend knows how to inspect. The response includes counts such as examined records, rotated records, already-active records, active key id, dry-run state, and audit event id. After the real run, verify that application credential rotation, token minting, and affected integrations still work. Only then remove the previous key from server configuration and restart or roll the nodes.

If the old key leaked, shorten the overlap window. If the old key was lost before records were rotated, encrypted records that still require it may be unrecoverable from backup. That is why key history is part of the backup boundary.

## Internal Node Credentials And Signing Identity

Nodes communicate through authenticated gRPC at the `public_api_addr` committed in CoreMeta lifecycle topology. `CORESTORE_INTERNAL_BEARER_TOKEN` is the credential one node presents when it calls another node's internal CoreStore, proxy, quorum, or recovery services. It must identify a node principal with the built-in system-realm relation for internal RPCs. Tenant and ordinary operator credentials must not be reused for this purpose.

This credential is per node, not a mesh-wide shared secret. Provision it through the same protected credential workflow used for other service principals, keep it out of logs and images, and rotate one node deliberately. An empty value disables remote internal writes; distributed work fails instead of silently degrading to local-only storage.

Each node also has a durable Ed25519 receipt-signing identity. Anvil generates the private key and stores it as node-local CoreMeta inside that node's `STORAGE_PATH`. The corresponding 32-byte public key is committed in the lifecycle descriptor beside the node id and `public_api_addr`. Remote nodes verify signed evidence against that committed key. The private key is neither shared nor supplied as an environment variable.

Restoring the same node means restoring its volume and therefore its node id and signing key. Replacing a node means registering a new identity and public key through lifecycle control. Do not copy one live node's volume to another live process or edit descriptors to paper over an identity mismatch.

## Bootstrap First-Admin Credential

On a fresh storage directory where the system realm is absent, startup can create the first system administration application and write its credential JSON to `BOOTSTRAP_SYSTEM_ADMIN_CREDENTIAL_OUTPUT_PATH`. That credential contains a client id and client secret. It is used to mint short-lived bearer tokens through the public authentication API; it is not itself a bearer token.

Bootstrap is below the API because no system administrator exists yet. After the system realm exists, bootstrap settings are ignored rather than minting more authority. From then on, admin operations authenticate normally and authorise through the built-in system realm.

The first-admin credential has a large blast radius because it belongs to a system principal. Copy it into a secret manager, restrict access, use it to create normal operator workflows, and rotate or retire it as soon as those workflows exist. If it leaks, rotate the application secret or remove the credential through the admin API from a still-authorised system principal. If every system administration credential is lost and the system realm already exists, the bootstrap environment variables will not recreate authority; treat that as a serious recovery scenario and plan operational break-glass procedures before production.

## Tenant And Application Client Secrets

Tenant and system applications authenticate with client id and client secret pairs. Admin provisioning can create the first tenant application, and tenant principals can create their own application credentials through the public API when delegated. A successful create or rotate prints the client secret once. Store it immediately.

An admin-side rotation for an application credential uses the private admin API and requires an expected generation because it updates an existing record:

```bash
anvil-admin app rotate-secret \
  --tenant-id acme \
  --app-name acme-admin \
  --expected-generation 1 \
  --audit-reason 'rotate acme-admin application secret'
```

Tenant-owned applications can also rotate through the public API when authorised:

```bash
anvil app rotate-secret docs-writer
```

Rotating an app secret stops future token minting with the old secret. Existing bearer tokens remain valid until they expire unless another control invalidates them. The blast radius is the scopes delegated to that application. A narrow object writer secret is a smaller incident than an owner credential that can delegate policy, create apps, and write broad object prefixes.

Do not use the first system-admin credential inside tenant publishing jobs. Tenant applications should hold tenant-scoped app secrets and call the public API. Operators should use system credentials only for system work such as tenant bootstrap, topology, diagnostics, repair, and secret-envelope rotation.

## Bearer Tokens

Bearer tokens are short-lived request credentials minted from app credentials. Current Anvil tokens include a subject, tenant id, scopes, token id, and expiry, and they expire after about one hour. They are presented as `Authorization: Bearer ...` metadata on public and admin API calls.

The blast radius of a leaked bearer token is usually smaller than the blast radius of the client secret that minted it, because the token expires. It can still be serious: until expiry, the holder can exercise the scopes encoded in the token. Do not log bearer tokens, S3 signatures, app secrets, first-admin credentials, or server keys. Redact them at reverse proxies, application logs, CLI transcripts, and support tooling.

Changing `JWT_SECRET` invalidates outstanding tokens signed with the old secret. Rotating an application client secret prevents new tokens from being minted with the old app secret but does not necessarily invalidate tokens already issued. Incident response should account for both behaviours.

## Why `anvil-admin` Is Network-Only

`anvil-admin` is intentionally a network client. Except for the local key-generation helper, it talks to the private admin API with a bearer token. It should not receive `STORAGE_PATH`, `ANVIL_SECRET_ENCRYPTION_KEY`, raw database files, or mounted CoreStore directories. That separation is a safety property: the server evaluates authentication, system-realm authorisation, validation, generation checks, idempotency, and audit before mutating durable state.

If an operator script needs direct storage access to rotate a secret, it is bypassing the model. Use the admin API for server-side envelope rotation, app secret rotation, and audit evidence. Use backups for recovery, not as a shadow control plane.

## Operator Runbook Guidance

Generate server-side encryption keys with the helper, store all server secrets in a secret manager, and keep key ids tied to calendar or release events. Inject secrets into containers or pods at runtime; do not bake them into images. Keep the admin API private, and keep the network admin CLI away from server key material.

Rotate when a secret leaks, when staff or automation with access leaves the trust boundary, before old key history becomes too large, or as part of scheduled security maintenance. For `ANVIL_SECRET_ENCRYPTION_KEY`, rotate with previous keys configured and verify the admin rotation response before removing old keys. For `JWT_SECRET`, plan a coordinated node rollout because there is no current documented multi-key overlap workflow equivalent to envelope rotation. Rotate each internal node credential independently and verify authenticated distributed operations before retiring its predecessor. For app credentials, rotate the specific app and update only the service that owns it.

Finally, test recovery. A restore drill should prove that Anvil can start with restored `STORAGE_PATH`, the active and previous encryption keys needed for that backup, the expected JWT configuration, the restored local node identity and signing key, and at least one authorised admin credential. A distributed drill must also prove that each restored node can authenticate to internal gRPC and that its local descriptor matches committed topology. A backup that cannot decrypt app secrets or authenticate an operator is not a complete recovery plan.

## Rotation evidence

A secret rotation is complete only after new credentials are stored, old credentials are no longer accepted where intended, dependent services have reloaded, and audit records show who performed the change. For server-side secret-envelope rotation, keep previous keys configured until the rotation command and application credential smoke tests both pass.

Do not confuse key generation with key installation. `anvil-admin key generate-secret-encryption-key` prints local material. The server uses it only after operators store it in configuration and restart or roll nodes according to the deployment model.
