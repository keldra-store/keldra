#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 6 ]]; then
  echo "usage: $0 <release-record> <version> <commit> <repository> <amd64-oci-archive> <arm64-oci-archive>" >&2
  exit 2
fi

record="$1"
version="$2"
commit="$3"
repository="$4"
amd64_archive="$5"
arm64_archive="$6"
script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "$repository" == *:* || "$repository" == *@* ]]; then
  echo "repository must not include a tag or digest: $repository" >&2
  exit 2
fi
"$script_root/release-record.py" verify \
  --record "$record" --version "$version" --commit "$commit"

for platform in linux/amd64 linux/arm64; do
  arch="${platform#linux/}"
  case "$platform" in
    linux/amd64) archive="$amd64_archive" ;;
    linux/arm64) archive="$arm64_archive" ;;
  esac
  "$script_root/verify-release-image-artifact.sh" \
    "$record" "$archive" "$version" "$commit" "$platform"
  index_digest="$(jq -er --arg platform "$platform" '
    [.images[] | select(.platform == $platform) | .oci_index_digest]
    | if length == 1 then .[0] else error("missing platform image") end
  ' "$record")"
  runnable_digest="$(jq -er --arg platform "$platform" '
    [.images[] | select(.platform == $platform) | .runnable_manifest_digest]
    | if length == 1 then .[0] else error("missing platform image") end
  ' "$record")"
  provenance_digest="$(jq -er --arg platform "$platform" '[.images[] | select(.platform == $platform) | .attestations.provenance] | if length == 1 then .[0] else error("missing provenance") end' "$record")"
  sbom_digest="$(jq -er --arg platform "$platform" '[.images[] | select(.platform == $platform) | .attestations.sbom] | if length == 1 then .[0] else error("missing SBOM") end' "$record")"
  printf -v "${arch}_index_digest" '%s' "$index_digest"
  printf -v "${arch}_runnable_digest" '%s' "$runnable_digest"
  printf -v "${arch}_provenance_digest" '%s' "$provenance_digest"
  printf -v "${arch}_sbom_digest" '%s' "$sbom_digest"
done

# Both archives and their metadata are completely verified before this first
# registry mutation.
for platform in linux/amd64 linux/arm64; do
  arch="${platform#linux/}"
  case "$platform" in linux/amd64) archive="$amd64_archive" ;; linux/arm64) archive="$arm64_archive" ;; esac
  eval "index_digest=\$${arch}_index_digest"
  skopeo copy --all --preserve-digests "oci-archive:${archive}" "docker://${repository}@${index_digest}"
  remote_digest="sha256:$(skopeo inspect --raw "docker://${repository}@${index_digest}" | sha256sum | awk '{print $1}')"
  [[ "$remote_digest" == "$index_digest" ]] || { echo "published $platform index $remote_digest does not match $index_digest" >&2; exit 1; }
done

final_image="${repository}:${version}"
docker buildx imagetools create \
  --tag "$final_image" \
  "${repository}@${amd64_index_digest}" \
  "${repository}@${arm64_index_digest}"

raw_manifest="$(docker buildx imagetools inspect --raw "$final_image")"
AMD64_DIGEST="$amd64_runnable_digest" ARM64_DIGEST="$arm64_runnable_digest" \
  python3 -c '
import json
import os
import sys

manifest = json.load(sys.stdin)
actual = {
    "{}/{}".format(
        item.get("platform", {}).get("os"),
        item.get("platform", {}).get("architecture"),
    ): item.get("digest")
    for item in manifest.get("manifests", [])
    if item.get("platform", {}).get("os") != "unknown"
}
expected = {
    "linux/amd64": os.environ["AMD64_DIGEST"],
    "linux/arm64": os.environ["ARM64_DIGEST"],
}
if actual != expected:
    raise SystemExit(f"published runnable platform set {actual!r} does not match {expected!r}")
' <<<"$raw_manifest"

expected_attestations="$(printf '%s %s\n%s %s\n%s %s\n%s %s\n' \
  "$amd64_provenance_digest" "$amd64_runnable_digest" "$amd64_sbom_digest" "$amd64_runnable_digest" \
  "$arm64_provenance_digest" "$arm64_runnable_digest" "$arm64_sbom_digest" "$arm64_runnable_digest" | sort -u)"
actual_attestations="$(jq -r '
  [.manifests[] | select(.platform.os == "unknown" and .platform.architecture == "unknown" and .annotations["vnd.docker.reference.type"] == "attestation-manifest")
   | [.digest, .annotations["vnd.docker.reference.digest"]] | @tsv][]
' <<<"$raw_manifest" | tr '\t' ' ' | sort -u)"
[[ "$actual_attestations" == "$expected_attestations" ]] || { echo "final tag did not preserve the exact provenance/SBOM reference set" >&2; exit 1; }

final_digest="sha256:$(printf '%s' "$raw_manifest" | sha256sum | awk '{print $1}')"
printf 'image=%s\ndigest=%s\n' "$final_image" "$final_digest"
