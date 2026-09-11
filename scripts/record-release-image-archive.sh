#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 5 ]]; then
  echo "usage: $0 <input-dir> <oci-archive> <digest-pinned-package-image> <digest-pinned-runtime-image> <output-record>" >&2
  exit 2
fi

input_dir="$1"
archive="$2"
package_image="$3"
runtime_image="$4"
output_record="$5"
script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
input="$input_dir/input.json"

jq --exit-status '.schema == "keldra.release-image-input.v1"' "$input" >/dev/null
version="$(jq -er '.version' "$input")"
commit="$(jq -er '.source_commit' "$input")"
platform="$(jq -er '.platform' "$input")"
build_mode="$(jq -er '.build_mode' "$input")"
compiler="$(jq -er '.compiler' "$input")"
expected_server_sha="$(jq -er '.binaries["keldra-server"].sha256' "$input")"
expected_cli_sha="$(jq -er '.binaries.keldra.sha256' "$input")"
expected_input_sha="sha256:$(sha256sum "$input" | awk '{print $1}')"

[[ "sha256:$(sha256sum "$input_dir/keldra-server" | awk '{print $1}')" == "$expected_server_sha" ]]
[[ "sha256:$(sha256sum "$input_dir/keldra" | awk '{print $1}')" == "$expected_cli_sha" ]]

raw_index="$(skopeo inspect --raw "oci-archive:${archive}")"
oci_index_digest="sha256:$(skopeo inspect --raw "oci-archive:${archive}" | sha256sum | awk '{print $1}')"
runnable_manifest_digest="$(jq --exit-status --raw-output --arg platform "$platform" '
  [.manifests[] | select((.platform.os + "/" + .platform.architecture) == $platform) | .digest]
  | if length == 1 then .[0] else error("expected one runnable platform manifest") end
' <<<"$raw_index")"
mapfile -t attestation_descriptors < <(jq -c --arg runnable "$runnable_manifest_digest" '
  [.manifests[] | select(
    (.platform.os == "unknown") and (.platform.architecture == "unknown") and
    (.annotations["vnd.docker.reference.type"] == "attestation-manifest") and
    (.annotations["vnd.docker.reference.digest"] == $runnable)
  )][]
' <<<"$raw_index")
[[ "${#attestation_descriptors[@]}" -ge 1 ]] || { echo "OCI provenance and SBOM attestations are missing" >&2; exit 1; }
total_attestations="$(jq '[.manifests[] | select(.annotations["vnd.docker.reference.type"] == "attestation-manifest")] | length' <<<"$raw_index")"
[[ "$total_attestations" == "${#attestation_descriptors[@]}" ]] || { echo "OCI archive contains an attestation for a different subject" >&2; exit 1; }

attestation_tmp="$(mktemp -d)"
trap 'rm -rf -- "$attestation_tmp"' EXIT
archive_blob() { tar -xOf "$archive" "blobs/sha256/${1#sha256:}"; }
provenance_digest= sbom_digest=
for descriptor in "${attestation_descriptors[@]}"; do
  attestation_digest="$(jq -er '.digest' <<<"$descriptor")"
  manifest_file="$attestation_tmp/${attestation_digest#sha256:}.json"
  archive_blob "$attestation_digest" >"$manifest_file"
  [[ "sha256:$(sha256sum "$manifest_file" | awk '{print $1}')" == "$attestation_digest" ]] || { echo "attestation manifest digest mismatch" >&2; exit 1; }
  while IFS= read -r statement_digest; do
    statement_file="$attestation_tmp/${statement_digest#sha256:}.json"
    archive_blob "$statement_digest" >"$statement_file"
    [[ "sha256:$(sha256sum "$statement_file" | awk '{print $1}')" == "$statement_digest" ]] || { echo "attestation statement digest mismatch" >&2; exit 1; }
    jq -e --arg subject "${runnable_manifest_digest#sha256:}" 'any(.subject[]?.digest.sha256; . == $subject)' "$statement_file" >/dev/null || { echo "attestation subject does not bind the runnable manifest" >&2; exit 1; }
    predicate_type="$(jq -er '.predicateType' "$statement_file")"
    case "$predicate_type" in
      https://slsa.dev/provenance/*)
        for required_value in "$commit" "${expected_server_sha#sha256:}" "${expected_cli_sha#sha256:}" "${expected_input_sha#sha256:}" "$package_image" "$runtime_image"; do
          jq -e --arg value "$required_value" 'any(.. | strings; . == $value)' "$statement_file" >/dev/null || { echo "provenance omits exact build input $required_value" >&2; exit 1; }
        done
        provenance_digest="$attestation_digest"
        ;;
      https://spdx.dev/Document) sbom_digest="$attestation_digest" ;;
    esac
  done < <(jq -er '.layers[].digest' "$manifest_file")
done
[[ -n "$provenance_digest" && -n "$sbom_digest" ]] || { echo "OCI archive requires both SLSA provenance and SPDX SBOM predicates" >&2; exit 1; }

inspection="$(skopeo inspect --override-os linux --override-arch "${platform#linux/}" "oci-archive:${archive}")"
actual_revision="$(jq -r '.Labels["org.opencontainers.image.revision"] // ""' <<<"$inspection")"
actual_platform="$(jq -r '.Os + "/" + .Architecture' <<<"$inspection")"
actual_server_sha="$(jq -r '.Labels["io.keldra.binary.keldra-server.sha256"] // ""' <<<"$inspection")"
actual_cli_sha="$(jq -r '.Labels["io.keldra.binary.keldra.sha256"] // ""' <<<"$inspection")"
actual_input_sha="$(jq -r '.Labels["io.keldra.build-input.sha256"] // ""' <<<"$inspection")"
if [[ "$actual_revision" != "$commit" || "$actual_platform" != "$platform" ]]; then
  echo "OCI identity $actual_revision $actual_platform does not match $commit $platform" >&2
  exit 1
fi
if [[ "$actual_server_sha" != "$expected_server_sha" || "$actual_cli_sha" != "$expected_cli_sha" ]]; then
  echo "OCI binary labels do not match the attested input bytes" >&2
  exit 1
fi
[[ "$actual_input_sha" == "$expected_input_sha" ]] || { echo "OCI build-input label does not match the sealed manifest" >&2; exit 1; }

"$script_root/release-record.py" image \
  --version "$version" \
  --commit "$commit" \
  --platform "$platform" \
  --oci-archive "$archive" \
  --oci-index-digest "$oci_index_digest" \
  --runnable-manifest-digest "$runnable_manifest_digest" \
  --build-mode "$build_mode" \
  --compiler "$compiler" \
  --package-image "$package_image" \
  --input-manifest "$input" \
  --attestation "provenance=$provenance_digest" \
  --attestation "sbom=$sbom_digest" \
  --runtime-image "$runtime_image" \
  --server-binary "$input_dir/keldra-server" \
  --cli-binary "$input_dir/keldra" \
  --output "$output_record"
