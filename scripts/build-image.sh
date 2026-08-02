#!/usr/bin/env bash
set -euo pipefail

image="${ANVIL_IMAGE:-anvil:test}"

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

echo "[anvil] building ${image} in the ${platform} trixie builder"
iid_file="$(mktemp -t anvil-image.XXXXXX)"
trap 'rm -f "${iid_file}"' EXIT
docker buildx build \
  --platform "${platform}" \
  --load \
  --iidfile "${iid_file}" \
  --file anvil/Dockerfile \
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

echo "[anvil] built ${image} (${image_id})"
