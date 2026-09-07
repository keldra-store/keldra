# Release process

Keldra releases are authorized by one `keldra.release-record.v1` JSON record.
The release workflow creates it only after the exact unpublished amd64 image
passes the exact three-node release suite. The record binds that result to the
same candidate image and commit; a single-node result cannot satisfy the release
contract.

Start `Keldra Release` manually with the candidate tag and the required
digest-pinned builder and runtime image inputs. The workflow validates, builds,
qualifies, records, and publishes the exact tagged commit.

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
for both public packages and embeds the exact GitHub run and attempt that passed
three-node qualification. Publication rechecks the downloaded outer archives,
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
hosts, but its evidence is not a prerequisite for publishing 0.17.0.
