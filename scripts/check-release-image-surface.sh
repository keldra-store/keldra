#!/usr/bin/env bash
set -euo pipefail

release_workflow=".github/workflows/release.yml"

if grep -REn 'docker[[:space:]]+push|imagetools[[:space:]]+create' .github/workflows; then
  echo "release workflows must not publish or assemble public per-architecture tags" >&2
  exit 1
fi

for required in \
  'final_image="${repository}:${RELEASE_TAG}"' \
  '--platform linux/amd64,linux/arm64' \
  '--tag "$final_image"' \
  '--push'
do
  if ! grep -Fq -- "${required}" "${release_workflow}"; then
    echo "release workflow is missing the single-image invariant: ${required}" >&2
    exit 1
  fi
done

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
  .cargo .github/workflows anvil scripts
then
  echo "release tooling must use Cargo's configured target directory" >&2
  exit 1
fi

if [[ -e anvil/Dockerfile.prebuilt ]]; then
  echo "the prebuilt-binary image path must remain removed" >&2
  exit 1
fi

if grep -REn \
  'Dockerfile\.prebuilt|cargo-zigbuild|zigbuild|tmp/docker-bin|ANVIL_ZIG_TARGET|ANVIL_USE_NATIVE_CARGO|ANVIL_RUNTIME_BASE' \
  .github/workflows scripts/build-image.sh README.md .dockerignore anvil/build-and-run.sh
then
  echo "image tooling must build from source in the target-platform Dockerfile" >&2
  exit 1
fi

grep -Fq 'FROM --platform=$TARGETPLATFORM rust:1.96-trixie AS builder' anvil/Dockerfile
grep -Fq 'FROM --platform=$TARGETPLATFORM debian:trixie-slim' anvil/Dockerfile
grep -Fq -- '--file anvil/Dockerfile' scripts/build-image.sh
grep -Fq -- '--file anvil/Dockerfile' "${release_workflow}"
