#!/usr/bin/env bash
set -euo pipefail

image="${ANVIL_IMAGE:-anvil:test}"
build_profile="${ANVIL_BUILD_PROFILE:-ci}"

case "${build_profile}" in
  release)
    cargo_profile_args=(--release)
    bin_profile_dir="release"
    default_test_tools=0
    ;;
  ci)
    cargo_profile_args=(--profile ci)
    bin_profile_dir="ci"
    default_test_tools=1
    ;;
  dev|debug)
    cargo_profile_args=()
    bin_profile_dir="debug"
    default_test_tools=1
    ;;
  *)
    echo "unsupported ANVIL_BUILD_PROFILE=${build_profile}; expected release, ci, or dev" >&2
    exit 2
    ;;
esac

include_test_tools="${ANVIL_INCLUDE_TEST_TOOLS:-${default_test_tools}}"
if [[ "${include_test_tools}" != "0" && "${include_test_tools}" != "1" ]]; then
  echo "ANVIL_INCLUDE_TEST_TOOLS must be 0 or 1" >&2
  exit 2
fi

case "${ANVIL_DOCKER_PLATFORM:-}" in
  "")
    case "$(uname -m)" in
      arm64|aarch64)
        platform="linux/arm64"
        target="aarch64-unknown-linux-gnu"
        ;;
      x86_64|amd64)
        platform="linux/amd64"
        target="x86_64-unknown-linux-gnu"
        ;;
      *)
        echo "unsupported host architecture: $(uname -m)" >&2
        exit 2
        ;;
    esac
    ;;
  linux/arm64)
    platform="linux/arm64"
    target="aarch64-unknown-linux-gnu"
    ;;
  linux/amd64)
    platform="linux/amd64"
    target="x86_64-unknown-linux-gnu"
    ;;
  *)
    echo "unsupported ANVIL_DOCKER_PLATFORM=${ANVIL_DOCKER_PLATFORM}" >&2
    exit 2
    ;;
esac

target="${ANVIL_ZIG_TARGET:-$target}"
use_zig=1
if [[ "${ANVIL_USE_NATIVE_CARGO:-0}" == "1" ]]; then
  host_triple="$(rustc -vV | awk '/^host: / { print $2 }')"
  if [[ "$(uname -s)" != "Linux" || "$target" != "$host_triple" ]]; then
    echo "ANVIL_USE_NATIVE_CARGO=1 is only valid on Linux when the requested target matches the host" >&2
    exit 2
  fi
  use_zig=0
fi

if command -v rustup >/dev/null 2>&1; then
  if ! rustup target list --installed | grep -Fxq "${target}"; then
    echo "[anvil] installing Rust std target ${target}"
    rustup target add "${target}"
  fi
fi

build_args=(
  -p anvil-server --bin anvil-server
  -p anvil-storage-cli --bin anvil --bin anvil-admin
)
if [[ "${include_test_tools}" == "1" ]]; then
  build_args+=(--features anvil-server/root-publication-test-control)
fi

if [[ "$use_zig" == "1" ]]; then
  if ! command -v cargo-zigbuild >/dev/null 2>&1; then
    echo "cargo-zigbuild is required when building a Linux image from this host/target combination" >&2
    echo "install with: cargo install cargo-zigbuild" >&2
    exit 2
  fi

  if ! command -v zig >/dev/null 2>&1; then
    echo "zig is required when building a Linux image from this host/target combination" >&2
    exit 2
  fi

  echo "[anvil] building Linux binaries with cargo-zigbuild target=${target} profile=${build_profile}"
  cargo zigbuild "${cargo_profile_args[@]}" --locked --target "${target}" "${build_args[@]}"
else
  echo "[anvil] building Linux binaries with cargo target=${target} profile=${build_profile}"
  cargo build "${cargo_profile_args[@]}" --locked --target "${target}" "${build_args[@]}"
fi

target_dir="$(
  cargo metadata --format-version 1 --no-deps \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])'
)"
bin_dir="${target_dir}/${target}/${bin_profile_dir}"
stage_dir="tmp/docker-bin"

rm -rf "${stage_dir}"
mkdir -p "${stage_dir}"
stage_binary() {
  local source="$1"
  local destination="$2"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    cp -c "${source}" "${destination}"
  elif cp --help 2>&1 | grep -q -- '--reflink'; then
    cp --reflink=auto "${source}" "${destination}"
  else
    cp "${source}" "${destination}"
  fi
  if [[ "${build_profile}" == "dev" ]] && command -v zig >/dev/null 2>&1; then
    zig objcopy --strip-debug "${destination}" "${destination}.stripped"
    mv "${destination}.stripped" "${destination}"
  fi
}
stage_binary "${bin_dir}/anvil-server" "${stage_dir}/anvil-server"
stage_binary "${bin_dir}/anvil" "${stage_dir}/anvil"
stage_binary "${bin_dir}/anvil-admin" "${stage_dir}/anvil-admin"

echo "[anvil] packaging runtime image ${image} platform=${platform}"
iid_file="$(mktemp -t anvil-image.XXXXXX)"
trap 'rm -f "${iid_file}"' EXIT
docker build \
  --platform "${platform}" \
  --build-arg "ANVIL_RUNTIME_BASE=${ANVIL_RUNTIME_BASE:-debian:bookworm-slim}" \
  --build-arg "ANVIL_INCLUDE_TEST_TOOLS=${include_test_tools}" \
  --iidfile "${iid_file}" \
  -f anvil/Dockerfile.prebuilt \
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
