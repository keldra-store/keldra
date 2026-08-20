#!/usr/bin/env bash
set -euo pipefail

image="${ANVIL_IMAGE:-keldra:test}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_commit="$(git -C "${repo_root}" rev-parse --verify 'HEAD^{commit}')"
source_revision="${source_commit}"
if [[ -n "$(git -C "${repo_root}" status --porcelain=v1 --untracked-files=normal)" ]]; then
  source_revision="${source_commit}-dirty"
fi

case "${ANVIL_DOCKER_PLATFORM:-}" in
  "")
    case "$(uname -m)" in
      arm64|aarch64)
        platform="linux/arm64"
        ;;
      x86_64|amd64)
        platform="linux/amd64"
        ;;
      *)
        echo "unsupported host architecture: $(uname -m)" >&2
        exit 2
        ;;
    esac
    ;;
  linux/arm64)
    platform="linux/arm64"
    ;;
  linux/amd64)
    platform="linux/amd64"
    ;;
  *)
    echo "unsupported ANVIL_DOCKER_PLATFORM=${ANVIL_DOCKER_PLATFORM}" >&2
    exit 2
    ;;
esac

echo "[keldra] building ${image} in the ${platform} trixie builder"
iid_file="$(mktemp -t keldra-image.XXXXXX)"
trap 'rm -f "${iid_file}"' EXIT
docker buildx build \
  --platform "${platform}" \
  --build-arg "ANVIL_SOURCE_REVISION=${source_revision}" \
  --load \
  --iidfile "${iid_file}" \
  --file crates/keldra/Dockerfile \
  -t "${image}" \
  .

# Read the ID emitted by this exact build rather than resolving the mutable tag.
# Docker Desktop can briefly list a tag while rejecting an inspect by that tag.
image_id="$(tr -d '[:space:]' < "${iid_file}")"
if [[ "${image_id}" != sha256:* ]]; then
  echo "Docker did not write a valid image ID to ${iid_file}" >&2
  exit 1
fi
docker image inspect "${image_id}" >/dev/null
docker tag "$image_id" "$image"

echo "[keldra] built ${image} (${image_id})"
