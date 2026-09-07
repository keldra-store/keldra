#!/usr/bin/env bash
set -Eeuo pipefail

record="$1"
archive="$2"
version="$3"
commit="$4"
platform="$5"
script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${script_root}/release-record.py" verify \
  --record "$record" --version "$version" --commit "$commit"
case "$platform" in
  linux/amd64) architecture=x86-64; oci_arch=amd64 ;;
  linux/arm64) architecture=aarch64; oci_arch=arm64 ;;
  *) echo "unsupported release platform ${platform}" >&2; exit 2 ;;
esac
image_record="$(jq --exit-status --compact-output --arg platform "$platform" \
  '[.images[] | select(.platform == $platform)] | if length == 1 then .[0] else error("missing release image") end' \
  "$record")"

expected_archive_sha="$(jq -r '.oci_archive_sha256' <<<"$image_record")"
actual_archive_sha="sha256:$(sha256sum "$archive" | awk '{print $1}')"
[[ "$actual_archive_sha" == "$expected_archive_sha" ]] || {
  echo "${platform} archive bytes ${actual_archive_sha} do not match ${expected_archive_sha}" >&2
  exit 1
}
expected_index="$(jq -r '.oci_index_digest' <<<"$image_record")"
actual_index="sha256:$(skopeo inspect --raw "oci-archive:${archive}" | sha256sum | awk '{print $1}')"
[[ "$actual_index" == "$expected_index" ]] || {
  echo "${platform} OCI index ${actual_index} does not match ${expected_index}" >&2
  exit 1
}
expected_runnable="$(jq -r '.runnable_manifest_digest' <<<"$image_record")"
actual_runnable="$(skopeo inspect --raw "oci-archive:${archive}" | jq --exit-status --raw-output --arg platform "$platform" '
  [.manifests[] | select((.platform.os + "/" + .platform.architecture) == $platform) | .digest]
  | if length == 1 then .[0] else error("expected one runnable platform manifest") end
')"
[[ "$actual_runnable" == "$expected_runnable" ]] || {
  echo "${platform} runnable manifest ${actual_runnable} does not match ${expected_runnable}" >&2
  exit 1
}

extract="$(mktemp -d)"
local_ref="keldra:release-verification-${$}"
container=""
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$container" ]]; then docker rm "$container" >/dev/null 2>&1 || true; fi
  docker image rm "$local_ref" >/dev/null 2>&1 || true
  rm -rf -- "$extract"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
skopeo copy --override-os linux --override-arch "$oci_arch" \
  "oci-archive:${archive}" "docker-daemon:${local_ref}"
actual_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$local_ref")"
actual_revision="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$local_ref")"
[[ "$actual_platform" == "$platform" && "$actual_revision" == "$commit" ]] || {
  echo "loaded image identity ${actual_revision} ${actual_platform} does not match ${commit} ${platform}" >&2
  exit 1
}
container="$(docker create --platform "$platform" "$local_ref")"
for binary in keldra-server keldra; do
  docker cp "$container:/usr/local/bin/${binary}" "$extract/${binary}"
  expected_binary_sha="$(jq -r --arg binary "$binary" '.binaries[$binary].sha256' <<<"$image_record")"
  actual_binary_sha="sha256:$(sha256sum "$extract/${binary}" | awk '{print $1}')"
  [[ "$actual_binary_sha" == "$expected_binary_sha" ]] || {
    echo "${platform} ${binary} ${actual_binary_sha} does not match ${expected_binary_sha}" >&2
    exit 1
  }
  case "$architecture" in
    x86-64) file "$extract/${binary}" | grep -Fq 'x86-64' ;;
    aarch64) file "$extract/${binary}" | grep -Fq 'ARM aarch64' ;;
  esac
done
