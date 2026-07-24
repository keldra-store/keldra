---
title: Release Readiness Checklist
description: Gate an Anvil release with evidence for code, documentation, images, client crates, CLIs, security, storage safety, smoke tests, publication, and post-release verification.
---

# Release Readiness Checklist

A release gate is a decision record, not a ceremony. It answers one question: is this exact commit safe enough to publish as the server image, Rust client crate, documentation, and operator-facing release notes? For Anvil, that question spans more than Rust tests. A release can change the public API used by tenant applications, the private admin API used by operators, the S3/static gateway, CoreStore source records, derived indexes, PersonalDB witnessing, task leases, and the CLIs used during incidents.

This chapter is written for maintainers cutting a release and operators deciding whether to accept it. Treat each gate as evidence with a boundary. A passing gate proves something specific; it never proves the whole system is safe by itself. The final release decision should name the commit, tag, Docker digest, Rust crate version, documentation URL, smoke-test evidence, known gaps, and rollback or roll-forward plan.

Read this with [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/), [Deployment](/operators/deployment/), [Backup and Recovery](/operators/backup-and-recovery/), [Security Hardening](/operators/security-hardening/), [Network and Ports](/operators/network-and-ports/), [Admin Plane](/operators/admin-plane/), [CoreStore Operations](/operators/corestore-operations/), [Observability](/operators/observability/), [Incident Response](/operators/incident-response/), [Gateway Operations](/operators/gateway-operations/), [Index Operations](/operators/index-operations/), [PersonalDB Operations](/operators/personaldb-operations/), [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Learn: CoreStore](/learn/corestore/), [Learn: Gateways](/learn/gateways/), [Public CLI](/reference/public-cli/), and [Admin CLI](/reference/admin-cli/).

## Define the release unit

Start by deciding what a release contains. In the current repository the supported release surfaces are:

| Surface | Current release shape | What to verify |
| --- | --- | --- |
| Server | Docker image built by `scripts/build-image.sh` from host-built Linux binaries and packaged with `anvil/Dockerfile.prebuilt`; the server binary is `anvil-server`. | Image tag, image digest, healthcheck, runtime configuration, storage compatibility, public/admin listeners. |
| Public CLI | `anvil`, built into the Docker image from `anvil-storage-cli`. | Version matches the image and public API examples in the docs. |
| Admin CLI | `anvil-admin`, built into the Docker image from `anvil-storage-cli`. | Version matches the image and admin API examples in the docs. |
| Rust client crate | `anvil-storage`, published to crates.io by the release workflow when the version is not already present. | Crate version, generated protobuf bindings, public API compatibility. |
| Documentation | Fission static site under `documentation/`, published by the documentation workflow. | Built content, published site, examples matching the shipped CLIs and APIs. |
| GitHub release | Release notes rendered from a matching blog post. | Tag, commit SHA, Docker image, digest, crate version, docs URL, known risks. |

Do not add imaginary artefacts to the gate. The current workflow does not publish npm, PyPI, Maven, Docker-registry package-gateway clients, or separate downloadable CLI archives. Package-gateway foundations may be documented as architecture or future-facing modelling, but they are not a release artefact unless a current implementation and workflow publish them.

The tag is the anchor. The release workflow runs on tags matching `v*` or by manual dispatch with a tag. The Rust workspace version should match the tag without the leading `v`, and the release blog front matter should name the same Docker image tag and Rust crate version.

## Code and test gates

Run fast, source-level checks before building a release image. Formatting is a maintainer-quality gate even though the shared release script currently focuses on hardening, docs, crate packaging, and tests.

```bash
cargo fmt --all -- --check
ANVIL_BUILD_PROFILE=release ANVIL_IMAGE=anvil:test ./scripts/build-image.sh
./scripts/release-gates.sh
```

The formatting command proves the Rust formatter would not rewrite the workspace. It does not prove the code is correct. The image build proves the release-shaped container can be assembled from host-built Linux binaries. The release gate proves the configured Rust, server-core, and Docker integration groups passed. None of those commands prove a production mesh can roll safely without the operator evidence described later in this checklist.

The repository's shared release gate is:

```bash
./scripts/release-gates.sh
```

That script currently runs checks for accidental external-database dependence, public unfenced journal writes, documentation hardening, release-note rendering tests, Fission site check/build, a dry-run publish of `anvil-storage`, Rust unit tests, server core integration tests, and Docker-backed integration groups. Each step emits start/finish timing lines. On runners with GNU `timeout`, each step is bounded by `ANVIL_GATE_STEP_TIMEOUT_SECONDS`, which defaults to 1800 seconds so a stuck test reports the stuck gate instead of consuming an entire release run. CI runs those groups in parallel against one built image artifact; local maintainers can still run the full script sequentially. Passing it proves the release candidate survives the repository's common local gates. It does not publish anything, does not build the Docker image, does not prove the production mesh can roll, and does not prove a particular tenant workload is healthy.

Security-sensitive changes should also carry focused tests for the path they touch. A change to reserved namespace handling should include read and write negative tests. A change to task leases should prove stale fence rejection. A change to authz should prove both allowed and denied checks. A change to S3 signing should include canonical-request and host-forwarding cases. The full workspace test suite catches regressions only where tests exist.

## Documentation and site gates

Documentation is part of the release because operators use it during incidents. A doc page that describes a flag no longer present in `anvil-admin` is an operational defect, not a harmless typo.

The shared script already runs:

```bash
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

The site check proves the Fission documentation project can be analysed in release mode. The site build proves the static site can be generated. Neither command proves every command example is runnable, every link is semantically correct, or the published GitHub Pages site has deployed. For CLI examples, compare against the current `anvil` and `anvil-admin` source or help output. For operator pages, confirm environment-variable names match the current server configuration.

A release candidate should not ship with contradictory documentation: one page saying the admin API is private and another telling users to publish it, one page saying package gateways are implemented and another saying only S3 is exposed, or one page using wildcard grants as a normal path while the security chapter warns against them. When the implementation has a gap, write the gap plainly instead of hiding it behind an aspirational command.

Docs publication is a separate surface. The documentation workflow publishes the Fission site to GitHub Pages after a successful documentation build. The release workflow can link to the docs URL, but that link is useful only if the published site matches the release readers are about to deploy. Verify the docs workflow as part of the release record.

## Release notes and blog gate

Anvil release notes are rendered from a blog post in `documentation/content/blog/`. The renderer looks for a post whose front matter has `release: v...`, checks that the Docker image artefact ends in the tag, checks that the Rust crate artefact ends in the release version, and appends machine-readable artefact metadata.

A minimal local render looks like this:

```bash
TAG=v0.3.0
IMAGE_DIGEST=sha256:replace-with-published-digest
DOCS_URL=https://example.invalid/anvil/

scripts/render-release-notes.py \
  --tag "$TAG" \
  --image-digest "$IMAGE_DIGEST" \
  --crate-version "anvil-storage ${TAG#v}" \
  --commit-sha "$(git rev-parse HEAD)" \
  --docs-url "$DOCS_URL" \
  --output /tmp/anvil-release-notes.md
```

This proves there is exactly one matching release blog post and that its declared Docker and crate artefacts are shaped for the tag. It does not prove the image digest exists, the crate is published, the docs URL is live, or the release notes are operationally complete. A human still needs to read them for breaking changes, storage compatibility, security implications, migration notes, index rebuild expectations, PersonalDB changes, gateway changes, and current gaps.

Good release notes tell operators what changed and how to verify it. They should say whether a release writes new durable CoreStore records, changes authz or policy behaviour, changes public or admin API fields, requires new environment variables, changes S3/static routing behaviour, affects backups, or requires index or projection repair. If the release is safe to roll back by image replacement, say why. If downgrade requires restore-from-backup, say that before the tag is published.

## Docker image gate

The release workflow builds Docker images with `scripts/build-image.sh` for `linux/amd64` and `linux/arm64`. The script uses Zig/cargo-zigbuild by default so the Linux binaries are compatible with the runtime image instead of accidentally depending on a newer runner glibc than the container provides. The `linux/amd64` image is loaded as `anvil:test-amd64` and driven through the Docker end-to-end groups on GitHub's standard runner. The `linux/arm64` image is built and smoke-checked so architecture-specific build failures are caught before publication. Publication pushes architecture-specific tags, then creates the public release tag and `latest` as a multi-architecture manifest list. The script builds `anvil-server`, `anvil`, and `anvil-admin` on the host for the requested Linux target, then `anvil/Dockerfile.prebuilt` ships them in a runtime image without the Rust toolchain.

A local image check should use the same shape as CI where possible:

```bash
ANVIL_BUILD_PROFILE=release ANVIL_IMAGE=anvil:test ./scripts/build-image.sh

docker run --rm anvil:test anvil-server --version
docker run --rm anvil:test anvil --version
docker run --rm anvil:test anvil-admin --version
```

The build command proves the production image packaging path can produce a local image from the current checkout for the current host-selected target. To build the second architecture locally, set `ANVIL_DOCKER_PLATFORM=linux/amd64` or `ANVIL_DOCKER_PLATFORM=linux/arm64` and use a distinct `ANVIL_IMAGE` tag. Image builds require Zig and `cargo-zigbuild`; non-native image smoke checks also require Docker/QEMU support. `ANVIL_USE_NATIVE_CARGO=1` exists only as a local developer escape hatch for same-host Linux builds and should not be used for release evidence. The three version commands prove the server, public CLI, and admin CLI binaries exist in the image and can start far enough for Clap to report their versions. They do not prove the server can boot with real secrets, read an existing `STORAGE_PATH`, authenticate a tenant, or serve traffic behind your proxy.

The release workflow's Docker end-to-end gate is:

```bash
ANVIL_IMAGE=anvil:test-amd64 ./scripts/release-gates.sh docker-auth
ANVIL_IMAGE=anvil:test-amd64 ./scripts/release-gates.sh docker-storage
ANVIL_IMAGE=anvil:test-amd64 ./scripts/release-gates.sh docker-index
ANVIL_IMAGE=anvil:test-amd64 ./scripts/release-gates.sh docker-mesh
```

This proves the test suite can drive the image through the Docker cluster, authz, object storage, S3 gateway, indexing, PersonalDB, Hugging Face ingestion, internal proxy, and mesh/failure integration groups expected by CI. It does not prove every deployment environment has a configured production embedding provider, that vector results are production quality without provider configuration, or that a multi-region production topology is safe to roll. Treat it as image evidence, not production-readiness evidence.

After CI pushes the image, verify the immutable digest rather than relying only on a mutable tag:

```bash
TAG=v0.3.0
IMAGE="ghcr.io/OWNER/REPOSITORY:${TAG}"

docker pull "$IMAGE"
docker buildx imagetools inspect "$IMAGE"
```

This proves the registry can serve the image and report metadata for the tag. It does not prove your deployment is pinned to that digest. Record the digest in the release evidence and in any deployment change you hand to operators.

## Rust client crate gate

The Rust client crate is `anvil-storage` under `clients/rust`. The release workflow reads its version with `cargo metadata`, checks whether that version already exists on crates.io, and publishes it only if it is missing.

Run the dry run before the tag:

```bash
cargo publish --dry-run -p anvil-storage
```

This proves Cargo can package the client crate, include generated bindings as configured, and satisfy packaging checks without uploading. It does not prove crates.io accepted the real publish, that downstream applications compile against the new version, or that the new client is compatible with old servers during a rolling upgrade.

After the release workflow runs, verify publication explicitly:

```bash
VERSION=0.3.0
python3 scripts/crate-version-exists.py anvil-storage "$VERSION"
```

This proves crates.io reports the version as present. It does not prove every application should upgrade immediately. If the client uses new RPCs, new fields, new error details, or new authz behaviour, operators need the compatibility plan described in [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/).

## Public and admin CLI gate

The public and admin CLIs are release-coupled to the server image. They are not just convenience wrappers; they are how operators gather evidence, tenants run manual checks, and release notes demonstrate behaviour.

Use the binaries from the image you will publish:

```bash
ANVIL_IMAGE="ghcr.io/OWNER/REPOSITORY:v0.3.0"

docker run --rm "$ANVIL_IMAGE" anvil --version
docker run --rm "$ANVIL_IMAGE" anvil-admin --version
docker run --rm "$ANVIL_IMAGE" anvil bucket --help
docker run --rm "$ANVIL_IMAGE" anvil-admin diagnostics --help
```

The version commands prove the image contains runnable CLI binaries from the release. The help commands prove the selected command families exist and parse. They do not prove the server accepts those RPCs, that a profile can authenticate, or that examples in the documentation have enough policy grants to run. Pair CLI checks with live smoke tests against a disposable deployment.

During a release window, avoid mixing old local CLIs with a new server unless compatibility was tested deliberately. A newer `anvil-admin` may send fields an older server does not understand. An older `anvil` may not expose a flag that the docs now require. The safest operator habit is to run evidence commands from the same image digest being deployed.

## Security and hardening gate

A release should fail closed before it is convenient. The hardening scripts are not a substitute for review, but they catch classes of mistakes that should never reach an image:

```bash
./scripts/check-no-external-db.sh
./scripts/check-no-public-unfenced-journal-writes.sh
./scripts/check-docs-hardening.sh
```

The first check guards the design boundary that Anvil should not quietly depend on a separate relational database for core durable state. The second looks for public or crate-public journal mutation entry points that bypass fence-permit APIs. The third rejects stale or unsafe documentation patterns such as invented admin commands, misleading bootstrap-token language, and other known dangerous phrasing. Passing these checks does not prove the release is secure; it proves these particular source and documentation patterns were not found.

Security review for a release should also cover network planes, reserved namespaces, token handling, tenant public policy scopes, relationship authorisation, system-realm admin authorisation, CoreStore preconditions, task-lease fences, S3 signature handling, trusted forwarded host settings, public-read behaviour, logs, and audit. A release that touches one of those areas needs both positive and negative tests. For example, a gateway improvement is not complete until an unauthorised request, a reserved `_anvil/` path, and an incorrectly signed request all fail in the expected way.

Do not treat admin reachability as a release convenience. The admin API remains private/internal and still requires authentication and system-realm authorisation. Publishing a new image must not require exposing `ADMIN_LISTEN_ADDR` to a public network.

## Storage, backup, and rollback gate

Anvil is a storage system. A release is not ready for production operators until its storage story is clear. The release notes and operator handoff should answer:

- Does this release write new source-record formats, manifest formats, CoreStore refs, stream records, PersonalDB certificates, gateway records, authz records, index segment formats, or topology lifecycle records?
- Can the previous binary read and safely ignore those records, or is rollback really restore-from-backup or roll-forward?
- Do derived views need rebuild or repair after upgrade?
- Do backups need additional secret key history or configuration snapshots to remain restorable?
- Has a restore drill been run when the release changes durable state?

These questions are not bureaucracy. A Docker image can be rolled back only if durable state remains compatible. If a new release writes a one-way record and an older binary misreads it, restarting the old container is not rollback; it is another incident.

For operator acceptance, collect backup evidence before upgrade: every `STORAGE_PATH` volume snapshot containing its node-local identity and Ed25519 signing key, `JWT_SECRET`, active and previous secret-encryption keys, internal node credential references, bootstrap or named admin credentials, tenant/app credential rotation plan, and redacted configuration. A restore drill proves much more than a backup job succeeding. It proves the backup can become a running node or test deployment that serves source records.

## Smoke-test gate

A smoke test proves representative paths through a live release candidate. It should start with source records and then move to derived views. A successful search query is not proof that object storage is healthy; a successful object read is not proof that full-text or vector indexes are caught up.

A fresh deployment readiness check is intentionally small:

```bash
curl -fsS http://127.0.0.1:50051/ready
```

This proves the public listener answered the readiness endpoint. It does not prove tenant authentication, admin authorisation, storage compatibility, object writes, indexes, watches, PersonalDB, or gateways.

Follow it with a private admin diagnostic from the same release image or node:

```bash
anvil-admin --host http://127.0.0.1:50052 diagnostics list --limit 20
```

This proves the admin CLI can reach the private admin listener and make an authorised read, assuming the caller has a valid admin profile or environment. It does not prove the admin listener is hidden from public networks; test that at the firewall, service, ingress, or load-balancer layer.

Then run tenant-facing public checks with a disposable tenant or release-smoke profile:

```bash
anvil --profile release-smoke bucket ls
anvil --profile release-smoke object head s3://documents/tutorial/welcome.txt
anvil --profile release-smoke index list documents --include-disabled
```

The bucket list proves public authentication and bucket listing for that profile. The object head proves a source object metadata path is readable. The index list proves index definitions are visible. These commands do not prove every object version is intact, public-read is safe, relationship authorisation is correct, search is caught up, or every gateway route works.

A release smoke plan should include the primitives that changed or that your deployment depends on: object put/get/head/list, versioned reads and CAS where relevant, reserved namespace rejection, relationship authz check, public policy negative test, S3 signed `HEAD` or `GET`, static/public-read read where deliberate, append stream append/read/tail, task lease acquire/checkpoint/commit if used, PersonalDB group commit or catch-up, index query for path/metadata/typed/full-text/vector/hybrid families in use, watch tail/restart from cursor, repair/diagnostics read-only listing, and one admin audit read. Keep each result separate so a later incident can tell which layer was proven.

## GitHub release and publication gate

The release workflow publishes three main outputs: the tested Docker image, the `anvil-storage` crate when needed, and a GitHub release whose body is rendered from the release blog post. The documentation site is published by its own workflow.

After the tag workflow completes, verify the public record:

```bash
TAG=v0.3.0

gh release view "$TAG" --json tagName,targetCommitish,url
python3 scripts/crate-version-exists.py anvil-storage "${TAG#v}"
docker buildx imagetools inspect "ghcr.io/OWNER/REPOSITORY:${TAG}"
```

The GitHub command proves a release object exists for the tag and shows the commit it targets. The crate command proves crates.io can see the Rust client version. The image command proves the registry can report the Docker manifest. None of these prove the docs site deployed, the release body contains the right risks, or production has upgraded.

Verify the docs separately:

```bash
curl -fsS https://OWNER.github.io/REPOSITORY/ >/tmp/anvil-docs-home.html
```

This proves the published documentation URL responds. It does not prove the site content matches the exact tag. If release notes link to a moving documentation site, note that in the release evidence and keep version drift in mind during incident response.

## Post-release verification

Publication is not the end of the release. For the first production or staging deployment of the artefacts, record evidence that the release works outside CI:

| Evidence | Why it matters |
| --- | --- |
| Image digest used by deployment | Shows the running image is the tested artefact, not a mutable tag surprise. |
| `anvil-server`, `anvil`, and `anvil-admin` versions | Shows server and CLIs came from the same release. |
| Rust client crate version | Shows application client compatibility was considered. |
| Documentation URL | Shows operators know which docs were used during rollout. |
| Backup and restore-drill ids | Shows rollback is based on recoverable state, not hope. |
| Admin diagnostics and audit reads | Shows the private plane and system-realm authorisation still work. |
| Public API smoke tests | Shows tenant-facing auth, bucket, object, and index paths work. |
| Gateway smoke tests | Shows host, scheme, SigV4, static, and public-read routing still behave. |
| Derived-state lag evidence | Shows indexes, watches, PersonalDB projections, and routing projections are not silently behind. |
| Known gaps and follow-up owners | Shows the release decision did not hide limitations. |

Post-release verification should be time-boxed but real. If a smoke test fails, do not mask it by running broad repair first. Preserve request ids and logs, classify whether the failure is source, derived, security, capacity, gateway, PersonalDB, or topology, and choose the smallest safe mitigation as described in [Incident Response](/operators/incident-response/).

## Current public surfaces and gaps

The current release machinery is strong enough to publish a coherent server image, Rust client crate, documentation site, and GitHub release notes. It is not a complete supply-chain or deployment platform.

Current gaps to account for:

| Gap | Release consequence |
| --- | --- |
| No separate CLI archive publication in the current workflow. | Operators should treat the Docker image as the source for matching `anvil` and `anvil-admin` binaries unless another distribution path is added. |
| Documentation deployment is a separate workflow. | A successful release workflow does not by itself prove the docs site is published and current. |
| Release gates do not replace production smoke tests. | CI can pass while a deployment-specific proxy, secret, region, or storage volume is misconfigured. |
| Docker E2E is representative, not exhaustive. | It does not prove multi-region activation, drain completion, all gateway edge cases, or every embedding/search production configuration. |
| Rollback depends on storage compatibility. | A previous image is not safe if the new release wrote one-way source records. |
| Package registry protocols beyond current surfaces are not release artefacts. | Do not announce npm, PyPI, Maven, Docker registry, or similar gateway support unless the implementation and workflow actually ship it. |

The release is ready when the evidence is coherent: source tests passed, hardening checks passed, docs built and published, the image was tested and pushed with a digest, the Rust client crate was dry-run and published or intentionally skipped because it already exists, the GitHub release links the right artefacts, storage and backup risks are understood, smoke tests passed through the public and admin planes, and known limitations are written down for operators.

## Documentation gate

Before tagging, scan the docs for plane confusion. Public tutorials and reference examples should use `anvil` for tenant-owned operations. Admin examples should use `anvil-admin` only for private operator tasks. Release notes should state whether a change affects the public API, admin API, Docker image, Rust client, gateway behaviour, storage format, or documentation only.

The release blog should be specific enough that `scripts/render-release-notes.py` produces useful notes without hand-editing: include the release tag, artifact metadata, operator impact, validation guidance, and rollback cautions.
