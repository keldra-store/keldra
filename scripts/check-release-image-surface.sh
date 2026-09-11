#!/usr/bin/env bash
set -euo pipefail

release_workflow=".github/workflows/release.yml"
resume_workflow=".github/workflows/release-publication-resume.yml"

if grep -REn 'docker[[:space:]]+push' .github/workflows; then
  echo "release workflows must push platform images without public per-architecture tags" >&2
  exit 1
fi

for required in \
  'final_image="${repository}:${RELEASE_TAG}"' \
  'needs: [release-context, validate]' \
  'runner: ubuntu-24.04' \
  'runner: ubuntu-24.04-arm' \
  '--platform "$KELDRA_DOCKER_PLATFORM"' \
  '--provenance=mode=max' \
  '--sbom=true' \
  '--output "type=oci,dest=${OCI_ARCHIVE}"' \
  'name: keldra-release-image-${{ matrix.arch }}' \
  'name: keldra-release-image-amd64' \
  'name: keldra-release-record' \
  'name: Assemble immutable release record' \
  'release-record.py assemble' \
  'Publish API crate then Rust client from release record' \
  'skopeo copy --all --preserve-digests' \
  'vnd.docker.reference.type"] == "attestation-manifest"' \
  'vnd.docker.reference.digest"]' \
  'published attestation references' \
  '"oci-archive:${archive}"' \
  'needs: [release-context, validate, build-image, qualify-three-node, release-record]' \
  'docker buildx imagetools create' \
  '--tag "$final_image"' \
  '"${repository}@${amd64_digest}"' \
  '"${repository}@${arm64_digest}"'
do
  if ! grep -Fq -- "${required}" "${release_workflow}"; then
    echo "release workflow is missing the single-image invariant: ${required}" >&2
    exit 1
  fi
done

if grep -Eq -- 'index_qualification_run_id|accepted-v1-index' "${release_workflow}"; then
  echo "v1 index performance acceptance must not gate the current release" >&2
  exit 1
fi

if grep -Eq -- 'push=true|push-by-digest=true|name-canonical=true' "${release_workflow}"; then
  echo "release image builds must remain local OCI archives until qualification passes" >&2
  exit 1
fi
if grep -Eq -- '--provenance=false|--sbom=false' "${release_workflow}"; then
  echo "release OCI archives must include provenance and SBOM attestations" >&2
  exit 1
fi

for required in \
  "'Qualify exact amd64 candidate across three nodes'" \
  'pattern: keldra-release-image-*' \
  'skopeo copy --all --preserve-digests' \
  'published attestation references' \
  'Assemble immutable release record' \
  'Publish API crate then Rust client from release record' \
  'Verify release tag immediately before resumed publication'
do
  if ! grep -Fq -- "${required}" "${resume_workflow}"; then
    echo "resume workflow is missing archive qualification evidence: ${required}" >&2
    exit 1
  fi
done

unpinned_actions="$(
  grep -REn '^[[:space:]]+uses:[[:space:]]+[^[:space:]#]+@' .github/workflows |
    grep -Ev '@[0-9a-f]{40}[[:space:]]+#[[:space:]]+(v[0-9]+|stable)$' || true
)"
if [[ -n "${unpinned_actions}" ]]; then
  echo "workflow actions must use immutable commits with their selected version as a comment:" >&2
  echo "${unpinned_actions}" >&2
  exit 1
fi

if grep -REn 'dtolnay/rust-toolchain@.*# stable' .github/workflows |
  while IFS=: read -r workflow line_number _; do
    if ! sed -n "$((line_number + 1)),$((line_number + 3))p" "${workflow}" |
      grep -Fq 'toolchain: 1.96.0'
    then
      echo "${workflow}:${line_number}: Rust action must install workspace rust-version 1.96.0" >&2
      exit 1
    fi
  done
then
  :
else
  exit 1
fi

verify_line="$(grep -nF 'Verify release tag immediately before publication' "${release_workflow}" | cut -d: -f1)"
publish_line="$(grep -nF 'Publish API crate then Rust client from release record' "${release_workflow}" | cut -d: -f1)"
next_step_line="$(awk -v verify_line="${verify_line:-0}" \
  'NR > verify_line && /^      - name:/ { print NR; exit }' "${release_workflow}")"
if [[ -z "${verify_line}" || -z "${publish_line}" || "${next_step_line}" != "${publish_line}" ]]; then
  echo "the final release-tag check must immediately precede the first GHCR mutation" >&2
  exit 1
fi

resume_verify_line="$(grep -nF 'Verify release tag immediately before resumed publication' "${resume_workflow}" | cut -d: -f1)"
resume_publish_line="$(grep -nF 'Publish API crate then Rust client from release record' "${resume_workflow}" | cut -d: -f1)"
resume_next_step_line="$(awk -v verify_line="${resume_verify_line:-0}" \
  'NR > verify_line && /^      - name:/ { print NR; exit }' "${resume_workflow}")"
if [[ -z "${resume_verify_line}" || -z "${resume_publish_line}" \
  || "${resume_next_step_line}" != "${resume_publish_line}" ]]
then
  echo "the resumed release-tag check must immediately precede the first GHCR mutation" >&2
  exit 1
fi

if [[ "$(grep -Fc 'docker buildx imagetools create' "${release_workflow}")" != "1" ]]; then
  echo "release workflow must assemble exactly one multi-architecture image" >&2
  exit 1
fi

if grep -Fq 'source_tag_image' "${release_workflow}"; then
  echo "release workflow must not publish a second image tag" >&2
  exit 1
fi

if [[ "$(grep -Fc -- '--tag ' "${release_workflow}")" != "1" ]]; then
  echo "release workflow must publish exactly one image tag" >&2
  exit 1
fi

if [[ -e .github/workflows/candidate-image.yml ]]; then
  echo "the obsolete single-architecture candidate publisher must remain removed" >&2
  exit 1
fi

runner_target_setup='run: printf '\''CARGO_TARGET_DIR=%s\n'\'' "$RUNNER_TEMP/keldra-cargo-target" >> "$GITHUB_ENV"'
host_target_overrides="$(
  grep -REn --exclude='check-release-image-surface.sh' \
    --exclude='prepare-release-image-input.sh' \
    --exclude='release-record.py' \
    --exclude='Dockerfile' \
    'CARGO_TARGET_DIR|--target-dir' \
    .cargo .github/workflows crates/keldra scripts |
    grep -Fv -- "${runner_target_setup}" || true
)"
if [[ -n "${host_target_overrides}" ]]; then
  printf '%s\n' "${host_target_overrides}"
  echo "host release tooling must use the machine's stable Cargo target directory" >&2
  exit 1
fi
workflow_target_errors="$(
  python3 - "${runner_target_setup}" <<'PY'
import pathlib
import re
import sys

target_setup = sys.argv[1]
job_header = re.compile(r"^  ([A-Za-z0-9_-]+):(?:\s*#.*)?$")
cargo_command = re.compile(
    r"(?<![A-Za-z0-9_-])cargo\s+"
    r"(?:bench|build|check|clippy|doc|fmt|install|metadata|package|publish|run|test)\b"
)
cargo_wrapper = re.compile(
    r"(?:\./)?scripts/(?:"
    r"publish-release-crates\.sh|"
    r"qualify-(?:single|three)-node\.sh|"
    r"release-gates\.sh\s+(?:all|rust|server|static)"
    r")\b"
)
errors = []
workflow_root = pathlib.Path(".github/workflows")
paths = sorted([*workflow_root.glob("*.yml"), *workflow_root.glob("*.yaml")])

for path in paths:
    lines = path.read_text(encoding="utf-8").splitlines()
    in_jobs = False
    jobs = []
    current = None
    for line_number, line in enumerate(lines, 1):
        if line == "jobs:":
            in_jobs = True
            continue
        if not in_jobs:
            continue
        match = job_header.match(line)
        if match:
            if current is not None:
                jobs.append(current)
            current = [match.group(1), line_number, []]
        elif current is not None:
            current[2].append((line_number, line))
    if current is not None:
        jobs.append(current)

    for job_name, job_line, body in jobs:
        text = "\n".join(line for _, line in body)
        invokes_host_cargo = bool(cargo_command.search(text) or cargo_wrapper.search(text))
        setup_lines = [
            line_number
            for line_number, line in body
            if target_setup in line
        ]
        location = f"{path}:{job_line}: job {job_name}"
        if invokes_host_cargo and len(setup_lines) != 1:
            errors.append(
                f"{location} invokes host Cargo but has {len(setup_lines)} "
                "runner-local CARGO_TARGET_DIR setups"
            )
        elif not invokes_host_cargo and setup_lines:
            errors.append(
                f"{location} configures runner-local CARGO_TARGET_DIR but no host Cargo "
                "invocation was found"
            )

print("\n".join(errors))
PY
)"
if [[ -n "${workflow_target_errors}" ]]; then
  printf '%s\n' "${workflow_target_errors}" >&2
  echo "every GitHub job that invokes host Cargo must select exactly one runner-local target" >&2
  exit 1
fi

grep -Fq 'CARGO_TARGET_DIR=/usr/src/keldra/target' crates/keldra/Dockerfile
grep -Fq '/usr/src/keldra/target/release/keldra-server' crates/keldra/Dockerfile
grep -Fq '/usr/src/keldra/target/release/keldra' crates/keldra/Dockerfile
if grep -Fq 'docs/dependency-licenses.md' crates/keldra/Dockerfile; then
  echo "runtime image must not ship the historical 0.9 dependency audit" >&2
  exit 1
fi

for excluded_context in \
  '.git/' \
  '.idea/' \
  '.DS_Store' \
  '**/.DS_Store' \
  '**/keldra-data/' \
  'docs/decisions/*.local.md' \
  'tmp/'
do
  if ! grep -Fxq -- "${excluded_context}" .dockerignore; then
    echo "Docker build context may include private local state: ${excluded_context}" >&2
    exit 1
  fi
done

if grep -REn \
  'tmp/docker-bin|KELDRA_ZIG_TARGET|KELDRA_USE_NATIVE_CARGO|KELDRA_RUNTIME_BASE' \
  .github/workflows scripts/build-image.sh README.md .dockerignore crates/keldra/build-and-run.sh
then
  echo "obsolete image build escape hatch is present" >&2
  exit 1
fi

grep -Fq 'ARG KELDRA_RUST_BUILDER_IMAGE=rust:1.96-trixie' crates/keldra/Dockerfile
grep -Fq 'ARG KELDRA_RUNTIME_IMAGE=debian:trixie-slim' crates/keldra/Dockerfile
grep -Fq 'FROM --platform=$TARGETPLATFORM ${KELDRA_RUST_BUILDER_IMAGE} AS builder' crates/keldra/Dockerfile
grep -Fq 'FROM --platform=$TARGETPLATFORM ${KELDRA_RUNTIME_IMAGE}' crates/keldra/Dockerfile
grep -Fq 'org.opencontainers.image.revision=' crates/keldra/Dockerfile
grep -Fxq 'EXPOSE 50051 50052' crates/keldra/Dockerfile
if grep -REn 'KELDRA_GATEWAY_LISTEN|50053' \
  crates/keldra/src \
  crates/keldra/Dockerfile \
  crates/keldra/docker-compose.yml \
  tests/cluster/docker-compose.yml \
  scripts/qualify-single-node.sh \
  scripts/qualify-three-node.sh \
  README.md
then
  echo "release deployment and qualification surfaces must use one public port" >&2
  exit 1
fi
grep -Fq -- '--file crates/keldra/Dockerfile' scripts/build-image.sh
grep -Fq -- '--file crates/keldra/Dockerfile' "${release_workflow}"
grep -Fq -- '--build-arg "KELDRA_SOURCE_REVISION=${source_revision}"' scripts/build-image.sh
grep -Fq -- '--build-arg "KELDRA_SOURCE_REVISION=${SOURCE_COMMIT}"' "${release_workflow}"
grep -Fq -- '--build-arg "KELDRA_RUST_BUILDER_IMAGE=${KELDRA_RUST_BUILDER_IMAGE}"' "${release_workflow}"
grep -Fq -- '--build-arg "KELDRA_RUNTIME_IMAGE=${KELDRA_RUNTIME_IMAGE}"' "${release_workflow}"

prebuilt_dockerfile=crates/keldra/Dockerfile.prebuilt
grep -Fq 'FROM --platform=$BUILDPLATFORM ${KELDRA_PACKAGE_IMAGE} AS runtime-packages' "$prebuilt_dockerfile"
grep -Fq 'FROM --platform=$TARGETPLATFORM ${KELDRA_RUNTIME_IMAGE} AS runtime-base' "$prebuilt_dockerfile"
grep -Fq 'FROM runtime-base' "$prebuilt_dockerfile"
grep -Fq 'COPY --from=keldra-binaries /keldra-server' "$prebuilt_dockerfile"
grep -Fq 'COPY --from=keldra-binaries /keldra ' "$prebuilt_dockerfile"
grep -Fq 'io.keldra.binary.keldra-server.sha256=' "$prebuilt_dockerfile"
grep -Fq 'io.keldra.binary.keldra.sha256=' "$prebuilt_dockerfile"
grep -Fq 'io.keldra.build-input.sha256=' "$prebuilt_dockerfile"
grep -Fq 'sha256sum --check --strict' "$prebuilt_dockerfile"
grep -Fxq 'EXPOSE 50051 50052' "$prebuilt_dockerfile"
final_stage_line="$(grep -nF 'FROM runtime-base' "$prebuilt_dockerfile" | cut -d: -f1)"
if tail -n "+${final_stage_line}" "$prebuilt_dockerfile" | grep -Eq '^RUN[[:space:]]'; then
  echo "prebuilt target image must not execute target-architecture commands" >&2
  exit 1
fi

for helper in \
  scripts/prepare-release-image-input.sh \
  scripts/record-release-image-archive.sh \
  scripts/publish-release-image-archives.sh
do
  if [[ ! -x "$helper" ]]; then
    echo "local release image helper is not executable: $helper" >&2
    exit 1
  fi
done
grep -Fq 'build_mode == "prebuilt-zigbuild"' scripts/release-record.py
grep -Fq 'platform == "linux/amd64"' scripts/release-record.py
grep -Fq 'build_mode == "prebuilt-native"' scripts/release-record.py
grep -Fq 'platform == "linux/arm64"' scripts/release-record.py
grep -Fq 'inputs.get("package_image"' scripts/release-record.py
grep -Fq -- '--provenance=mode=max' docs/release-process.md
grep -Fq -- '--sbom=true' docs/release-process.md
grep -Fq -- '--build-context "keldra-binaries=' docs/release-process.md
grep -Fq 'KELDRA_INPUT_MANIFEST_SHA256' docs/release-process.md
grep -Fq 'readelf --version-info' scripts/prepare-release-image-input.sh
grep -Fq 'keldra.zrunner-release-build.v1' scripts/prepare-release-image-input.sh
grep -Fq 'zrunner.job.v1' scripts/prepare-release-image-input.sh
grep -Fq 'https://spdx.dev/Document' scripts/record-release-image-archive.sh
grep -Fq 'https://slsa.dev/provenance/' scripts/record-release-image-archive.sh
grep -Fq 'commands.add_parser("three-node")' scripts/release-record.py
grep -Fq 'executor == "local-zrunner"' scripts/release-record.py
if [[ "$(grep -Fc 'docker buildx imagetools create' scripts/publish-release-image-archives.sh)" != 1 ]]; then
  echo "local publication must assemble exactly one multi-architecture image" >&2
  exit 1
fi
if grep -Eq -- '--tag.*(amd64|arm64)' scripts/publish-release-image-archives.sh; then
  echo "local publication must not create public per-architecture tags" >&2
  exit 1
fi

for required in \
  'keldra.release-record.v1' \
  'keldra.v1-index.acceptance.v1' \
  '"ssd", "rotational"' \
  '"index-v1-sustained", "catalog-250k"' \
  'oci_archive_sha256' \
  'runnable_manifest_digest' \
  'keldra-server' \
  '"qualification": {"three_node": three_node}' \
  'set(qualification) == {"three_node"}' \
  'publication_order' \
  'crate_sha256' \
  'kit_manifest_sha256' \
  'keldra.v1-index.part-result.v1'
do
  if ! grep -Fq -- "$required" scripts/release-record.py; then
    echo "release-record verifier is missing required authority: ${required}" >&2
    exit 1
  fi
done

for required in \
  'name: Keldra v1 Index Acceptance' \
  'KELDRA_V1_ACCEPTANCE_STORAGE_CLASS' \
  'qualify-index-v1-accepted-host.sh' \
  'name: keldra-accepted-v1-index'
do
  if ! grep -Fq -- "$required" .github/workflows/index-v1-acceptance.yml; then
    echo "v1 index acceptance workflow is missing: ${required}" >&2
    exit 1
  fi
done

grep -Fq 'publish_if_absent keldra-api' scripts/publish-release-crates.sh
grep -Fq 'publish_if_absent keldra' scripts/publish-release-crates.sh
grep -Fq -- '--user-agent "keldra-release/${version}' scripts/publish-release-crates.sh
grep -Fq 'verify-release-image-artifact.sh' "${release_workflow}"
grep -Fq 'verify-release-image-artifact.sh' "${resume_workflow}"
grep -Fq 'protobuf-compiler' "${release_workflow}"
grep -Fq 'protobuf-compiler' "${resume_workflow}"
api_publish_line="$(grep -nF 'publish_if_absent keldra-api' scripts/publish-release-crates.sh | cut -d: -f1)"
client_publish_line="$(grep -nF 'publish_if_absent keldra' scripts/publish-release-crates.sh | tail -n1 | cut -d: -f1)"
if ((api_publish_line >= client_publish_line)); then
  echo "keldra-api must publish before the Rust keldra client" >&2
  exit 1
fi

for qualification in scripts/qualify-three-node.sh scripts/qualify-index-v1-ssd-scale.sh; do
  grep -Fq 'qualification_disk_ledger_init' "$qualification"
  grep -Fq 'qualification_disk_ledger_check' "$qualification"
done
