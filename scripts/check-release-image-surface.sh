#!/usr/bin/env bash
set -euo pipefail

release_workflow=".github/workflows/release.yml"

if grep -REn 'docker[[:space:]]+push' .github/workflows; then
  echo "release workflows must push platform images without public per-architecture tags" >&2
  exit 1
fi

for required in \
  'final_image="${repository}:${RELEASE_TAG}"' \
  'runner: ubuntu-24.04' \
  'runner: ubuntu-24.04-arm' \
  '--platform "$ANVIL_DOCKER_PLATFORM"' \
  '--provenance=false' \
  '--sbom=false' \
  'push-by-digest=true' \
  'name-canonical=true' \
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

if grep -REn --exclude='check-release-image-surface.sh' \
  'CARGO_TARGET_DIR|--target-dir' \
  .cargo .github/workflows crates/keldra scripts
then
  echo "release tooling must use Cargo's configured target directory" >&2
  exit 1
fi

if [[ -e crates/keldra/Dockerfile.prebuilt ]]; then
  echo "the prebuilt-binary image path must remain removed" >&2
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
  'Dockerfile\.prebuilt|cargo-zigbuild|zigbuild|tmp/docker-bin|ANVIL_ZIG_TARGET|ANVIL_USE_NATIVE_CARGO|ANVIL_RUNTIME_BASE' \
  .github/workflows scripts/build-image.sh README.md .dockerignore crates/keldra/build-and-run.sh
then
  echo "image tooling must build from source in the target-platform Dockerfile" >&2
  exit 1
fi

grep -Fq 'FROM --platform=$TARGETPLATFORM rust:1.96-trixie AS builder' crates/keldra/Dockerfile
grep -Fq 'FROM --platform=$TARGETPLATFORM debian:trixie-slim' crates/keldra/Dockerfile
grep -Fq 'org.opencontainers.image.revision=' crates/keldra/Dockerfile
grep -Fxq 'EXPOSE 50051 50052' crates/keldra/Dockerfile
if grep -REn 'ANVIL_GATEWAY_LISTEN|50053' \
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
grep -Fq -- '--build-arg "ANVIL_SOURCE_REVISION=${source_revision}"' scripts/build-image.sh
grep -Fq -- '--build-arg "ANVIL_SOURCE_REVISION=${SOURCE_COMMIT}"' "${release_workflow}"
