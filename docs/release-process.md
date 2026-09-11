# Release process

Keldra releases are authorized by one `keldra.release-record.v1` JSON record.
The release workflow creates it only after the exact unpublished amd64 image
passes the exact three-node release suite. The record binds that result to the
same candidate image and commit; a single-node result cannot satisfy the release
contract.

Start `Keldra Release` manually with the candidate tag and the required
digest-pinned builder and runtime image inputs. The workflow validates, builds,
qualifies, records, and publishes the exact tagged commit.

For a local release, the build itself is one zrunner `rust` job whose direct
argv is the repository helper below. The accepted job evidence must contain
`RUSTUP_TOOLCHAIN=1.96.0` and `CARGO_PROFILE_DEV_CODEGEN_BACKEND=llvm`, lock
`cargo-target:debian1:keldra`, set `CARGO_TARGET_DIR` to
`/home/zcourts/projects/projects/build/debian1/keldra`, and set
`KELDRA_ZRUNNER_JOB_ID` to the job envelope's ULID. The helper runs fixed
release commands: native Cargo for ARM64 and `cargo zigbuild --target
x86_64-unknown-linux-gnu.2.17` for amd64. It accepts at most two
zrunner-assigned jobs and writes its manifest only after both binaries exist
and pass ELF, interpreter, glibc-version, and NEEDED-library validation.

```bash
./scripts/prepare-release-image-input.sh build 0.17.1 linux/amd64 \
  <SOURCE_COMMIT> \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/build-amd64
```

After zrunner reports the successful `completed` state, export the exact
`zrunner.job.v1` submission meta and terminal `zrunner.event.v1` meta from
Zboard. Seal them with the build-produced manifest; arbitrary binaries and a
clean current checkout are not accepted as provenance:

```bash
./scripts/prepare-release-image-input.sh seal \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/build-amd64 \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/zrunner-job-amd64.json \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/zrunner-completed-amd64.json \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/image-input-amd64
```

Submit each Docker invocation itself to zrunner's `docker-build` profile; do
not submit a wrapper script. Substitute the digest-pinned runtime image and the
three values printed by the preparation command:

```bash
docker buildx build --platform linux/amd64 \
  --build-context "keldra-binaries=/home/zcourts/projects/projects/releases/keldra/0.17.1/image-input-amd64" \
  --build-arg "KELDRA_SOURCE_REVISION=<SOURCE_COMMIT>" \
  --build-arg "KELDRA_SERVER_SHA256=<SERVER_SHA256>" \
  --build-arg "KELDRA_CLI_SHA256=<CLI_SHA256>" \
  --build-arg "KELDRA_INPUT_MANIFEST_SHA256=<INPUT_JSON_SHA256>" \
  --build-arg "KELDRA_PACKAGE_IMAGE=debian:trixie-slim@sha256:<digest>" \
  --build-arg "KELDRA_RUNTIME_IMAGE=debian:trixie-slim@sha256:<digest>" \
  --provenance=mode=max --sbom=true \
  --output "type=oci,dest=/home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-image-amd64.oci.tar" \
  --file crates/keldra/Dockerfile.prebuilt .
```

Use the same build, seal, and image commands with `linux/arm64` and the ARM64
paths. The Dockerfile verifies both binary hashes and the sealed input manifest
on the ARM64 build platform, extracts
the target runtime packages there, and has no `RUN` in its target-platform
stage. Record each archive without loading or executing it:

```bash
./scripts/record-release-image-archive.sh \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/image-input-amd64 \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-image-amd64.oci.tar \
  debian:trixie-slim@sha256:<digest> \
  debian:trixie-slim@sha256:<digest> \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-release-image-amd64.json
```

Record both archives before qualification. The recorder parses the in-toto
layers and requires an SPDX SBOM plus SLSA provenance whose subject is the
runnable manifest and whose values contain the exact commit, binary hashes,
sealed-input hash, package image, and runtime image.

Run three-node qualification on the Debian ARM64 zrunner against the exact
ARM64 release image with explicit LLVM policy. The same contract can select
the amd64 image only on a native x86_64 runner. Its argv is
`./scripts/prepare-release-image-input.sh qualify 0.17.1
<SOURCE_COMMIT> <IMAGE_RECORD> <OCI_ARCHIVE>
<QUALIFICATION_MANIFEST>`, with `KELDRA_ZRUNNER_JOB_ID` bound to the job ULID.
The wrapper requires `RUSTUP_TOOLCHAIN=1.96.0`, the runner's
`build/<runner>/keldra` Cargo target and
matching `cargo-target:<runner>:keldra` lock,
rejects emulation, validates and loads the exact archive, runs the
official three-node entrypoint, and only then writes its result manifest.
Convert that manifest, submitted job, and successful terminal event into
truthful local release evidence:

```bash
./scripts/release-record.py three-node --version 0.17.1 \
  --commit <SOURCE_COMMIT> --image <IMAGE_RECORD> \
  --oci-archive <OCI_ARCHIVE> \
  --qualification-manifest <QUALIFICATION_MANIFEST> \
  --zrunner-job <THREE_NODE_JOB_JSON> \
  --zrunner-completion <THREE_NODE_COMPLETION_JSON> \
  --output <THREE_NODE_RESULT_JSON>
```

The normal archive verifier, release-record assembler, `skopeo copy --all
--preserve-digests`, and single `docker buildx imagetools create` publication
step consume these records unchanged. They assemble the sole release tag from
the two exact platform archives and preserve each archive's provenance and SBOM
attestations; no image compilation or emulation occurs during assembly.
After qualification and release-record assembly, publish that one tag with:

```bash
./scripts/publish-release-image-archives.sh \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-release-record-0.17.1.json \
  0.17.1 <SOURCE_COMMIT> ghcr.io/keldra-store/keldra \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-image-amd64.oci.tar \
  /home/zcourts/projects/projects/releases/keldra/0.17.1/keldra-image-arm64.oci.tar
```

The publisher completes verification and metadata collection for both outer
archives before the first registry mutation. It verifies embedded binary
hashes, source labels, runnable manifests, provenance and SBOM digests, uploads
each exact OCI index by digest, creates only `${repository}:${version}`, and
then verifies that tag contains exactly the two recorded runnable manifests
and the exact recorded attestation reference set.

The dispatch also requires the selected `rust:1.96-trixie` and
`debian:trixie-slim` references with `@sha256:` digests. The Dockerfile retains
tagged defaults for local development only; the release workflow rejects them.
Actions and the Rust installer are pinned to reviewed commits, and the Rust
version is fixed at 1.96.0. Debian package resolution is captured by BuildKit
provenance and the SBOM; it is not represented by invented offline digests.

For each platform the record binds the source commit and version to the target,
platform and architecture, the SHA-256 of the outer OCI archive, the OCI index
digest, runnable manifest digest, and SHA-256 plus architecture of the embedded
`keldra-server` and `keldra` executables. It also binds the exact `.crate` bytes
for both public packages and embeds either the exact GitHub run and attempt or
the exact successful local zrunner job evidence that passed three-node
qualification. Publication rechecks the downloaded outer archives,
embedded binaries and package archives before any public mutation; a resumed
publication performs the same checks and accepts an existing crate only when
crates.io reports the qualified checksum.

Publication consumes that record in a fixed order: `keldra-api`, then the Rust
`keldra` client that requires the same exact API version, then the OCI image and
GitHub release. Stable candidates become latest; versions with a prerelease
suffix do not. Notes, package versions, and channel flags all derive from the
same candidate version. The release record is attached to the GitHub release.

Qualification owns only its namespaced Compose project, temporary directories,
and evidence paths. Each runner records these in its cleanup ledger and checks
its configured disk budget. Cleanup may remove only ledger entries owned by
that run; it must never prune Docker or Cargo state belonging to another job.

This release is format v1 only and always starts on fresh volumes. There are no
legacy migration, mixed-version cluster, or predecessor format guarantees.

`Keldra v1 Index Acceptance` remains a standalone performance-qualification
workflow for the later Performance tranche. It builds a candidate-bound kit and
runs the sustained and Catalog-250K matrices on attested SSD and rotational
hosts, but its evidence is not a prerequisite for publishing 0.17.1.
