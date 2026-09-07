#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
experiment_root="${HOME}/keldra_experiments"
kit_root="${KELDRA_V1_ACCEPTANCE_KIT_ROOT:-${experiment_root}/kit}"
storage_class="${KELDRA_V1_ACCEPTANCE_STORAGE_CLASS:?set KELDRA_V1_ACCEPTANCE_STORAGE_CLASS}"
gate="${KELDRA_V1_ACCEPTANCE_GATE:?set KELDRA_V1_ACCEPTANCE_GATE}"
version="${KELDRA_V1_ACCEPTANCE_VERSION:?set KELDRA_V1_ACCEPTANCE_VERSION}"
commit="${KELDRA_V1_ACCEPTANCE_COMMIT:?set KELDRA_V1_ACCEPTANCE_COMMIT}"
output="${KELDRA_V1_ACCEPTANCE_OUTPUT:?set KELDRA_V1_ACCEPTANCE_OUTPUT}"
shard_index="${KELDRA_V1_ACCEPTANCE_SHARD_INDEX:-}"
shard_count="${KELDRA_V1_ACCEPTANCE_SHARD_COUNT:-}"

case "$storage_class" in ssd|rotational) ;; *) echo "storage class must be ssd or rotational" >&2; exit 2 ;; esac
case "$gate" in
  index-v1-sustained)
    [[ "$shard_index" =~ ^[0-9]+$ && "$shard_count" =~ ^[1-9][0-9]*$ ]] \
      && ((shard_index < shard_count)) || { echo "invalid sustained-gate shard" >&2; exit 2; }
    ;;
  catalog-250k)
    [[ -z "$shard_index" && -z "$shard_count" ]] || { echo "catalog gate is not sharded" >&2; exit 2; }
    ;;
  *) echo "gate must be index-v1-sustained or catalog-250k" >&2; exit 2 ;;
esac
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z._-]+)?$ ]] || { echo "invalid candidate version" >&2; exit 2; }
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid candidate commit" >&2; exit 2; }
for command in cmp findmnt jq lsblk sha256sum tar; do command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }; done
for required in SOURCE_COMMIT HARNESS_COMMIT CATALOG_HARNESS_COMMIT SHA256SUMS; do
  [[ -s "${kit_root}/${required}" ]] || { echo "candidate kit is missing ${required}" >&2; exit 2; }
done
for runner in qualify-index-v1-ssd-scale.sh qualify-index-catalog.sh qualification-disk-ledger.sh; do
  cmp "${repo_root}/scripts/${runner}" "${kit_root}/${runner}" || {
    echo "candidate kit runner ${runner} differs from candidate source" >&2
    exit 2
  }
done
(cd "$kit_root" && sha256sum --check SHA256SUMS)
[[ "$(tr -d '\r\n' <"${kit_root}/SOURCE_COMMIT")" == "$commit" ]] || { echo "kit source commit does not match candidate" >&2; exit 2; }
[[ "$(tr -d '\r\n' <"${kit_root}/HARNESS_COMMIT")" == "$commit" ]] || { echo "kit harness commit does not match candidate" >&2; exit 2; }
[[ "$(tr -d '\r\n' <"${kit_root}/CATALOG_HARNESS_COMMIT")" == "$commit" ]] || { echo "kit catalog harness commit does not match candidate" >&2; exit 2; }

mkdir -p "$experiment_root"
device="$(findmnt -no SOURCE -T "$experiment_root")"
rota_values="$(lsblk -ndo ROTA "$device" | sort -u | tr -d '[:space:]')"
case "$storage_class:$rota_values" in
  ssd:0|rotational:1) ;;
  *) echo "${experiment_root} is on ${device} with ROTA=${rota_values}, not requested ${storage_class}" >&2; exit 2 ;;
esac

mkdir -p "$output"
runner_log="${output}/runner.stdout.log"
case "$gate" in
  index-v1-sustained)
    (
      cd "$kit_root"
      KELDRA_V1_SCALE_KIT_ROOT="$kit_root" \
      KELDRA_V1_SCALE_MODE=sustained \
      KELDRA_V1_SCALE_DEFINITION_MATRIX=1,64,1000,10000,250000 \
      KELDRA_V1_SCALE_RECIPE_MATRIX=1,4,16,64 \
      KELDRA_V1_SCALE_WORKER_MATRIX=1,2,4,8 \
      KELDRA_V1_SCALE_MEMORY_PER_WORKER_MATRIX=134217728,268435456 \
      KELDRA_V1_SCALE_RATE_LADDER=1000,5000,10000,20000,40000 \
      KELDRA_V1_SCALE_OBJECT_SIZE_MATRIX=1024,98304 \
      KELDRA_V1_SCALE_BASELINE_SECONDS=30 \
      KELDRA_V1_SCALE_CONCURRENT_SECONDS=1800 \
      KELDRA_V1_SCALE_POST_SECONDS=30 \
      KELDRA_V1_SCALE_SHARD_INDEX="$shard_index" \
      KELDRA_V1_SCALE_SHARD_COUNT="$shard_count" \
        ./qualify-index-v1-ssd-scale.sh
    ) | tee "$runner_log"
    results="$(sed -n 's/^results=//p' "$runner_log" | tail -n1)"
    archive="$(sed -n 's/^archive=//p' "$runner_log" | tail -n1)"
    [[ -s "$archive" ]] || { echo "sustained qualification archive is missing" >&2; exit 1; }
    jq -e '.schema == "keldra.index-v1-ssd-scale.v1" and .mode == "sustained" and .server_source_commit == $commit and .shard_index == $shard_index and .shard_count == $shard_count and .selected_configurations == 1 and .total_configurations == $shard_count and .fatal_cells == 0' \
      --arg commit "$commit" --argjson shard_index "$shard_index" --argjson shard_count "$shard_count" \
      "${results}/report.json" >/dev/null
    archive_name="index-v1-sustained-shard-${shard_index}-of-${shard_count}.results.tar.gz"
    shard_json="$shard_index"
    count_json="$shard_count"
    ;;
  catalog-250k)
    (
      cd "$kit_root"
      KELDRA_CATALOG_KIT_ROOT="$kit_root" KELDRA_CATALOG_DEFINITIONS=250000 \
        ./qualify-index-catalog.sh
    ) | tee "$runner_log"
    results="$(sed -n 's/^results=//p' "$runner_log" | tail -n1)"
    archive="$(sed -n 's/^archive=//p' "$runner_log" | tail -n1)"
    [[ -s "$archive" ]] || { echo "catalog qualification archive is missing" >&2; exit 1; }
    jq -e '.schema == "keldra.catalog-scale-run.v1" and .server_source_commit == $commit and .catalog_harness_commit == $commit and .create.schema == "keldra.index-catalog-qualification.v1" and .create.phase == "create" and .create.definitions == 250000 and .verify.schema == "keldra.index-catalog-qualification.v1" and .verify.phase == "verify" and .verify.definitions == 250000 and .verify.listed == 250000 and .verify.sampled_gets > 0' \
      --arg commit "$commit" "${results}/report.json" >/dev/null
    archive_name="catalog-250k.results.tar.gz"
    shard_json=null
    count_json=null
    ;;
esac

cp "$archive" "${output}/${archive_name}"
tar -C "$output" -cf "${output}/evidence.tar" "$archive_name"
rm -f -- "${output}/${archive_name}"
host_fingerprint="sha256:$(sha256sum "${results}/host-info.txt" | awk '{print $1}')"
evidence_sha="sha256:$(sha256sum "${output}/evidence.tar" | awk '{print $1}')"
kit_manifest_sha="sha256:$(sha256sum "${kit_root}/SHA256SUMS" | awk '{print $1}')"
scale_runner_sha="sha256:$(sha256sum "${repo_root}/scripts/qualify-index-v1-ssd-scale.sh" | awk '{print $1}')"
catalog_runner_sha="sha256:$(sha256sum "${repo_root}/scripts/qualify-index-catalog.sh" | awk '{print $1}')"
jq -n --arg storage_class "$storage_class" --arg gate_id "$gate" \
  --arg version "$version" --arg source_commit "$commit" \
  --arg run_id "${GITHUB_RUN_ID:-manual}-${GITHUB_RUN_ATTEMPT:-1}-${storage_class}-${gate}-${shard_index:-whole}" \
  --arg evidence_bundle_sha256 "$evidence_sha" --arg hardware_fingerprint_sha256 "$host_fingerprint" \
  --arg kit_manifest_sha256 "$kit_manifest_sha" --arg scale_runner_sha256 "$scale_runner_sha" \
  --arg catalog_runner_sha256 "$catalog_runner_sha" --argjson shard_index "$shard_json" \
  --argjson shard_count "$count_json" \
  '{schema:"keldra.v1-index.part-result.v1",storage_class:$storage_class,gate_id:$gate_id,result:"pass",version:$version,source_commit:$source_commit,run_id:$run_id,shard_index:$shard_index,shard_count:$shard_count,evidence_bundle_sha256:$evidence_bundle_sha256,hardware_fingerprint_sha256:$hardware_fingerprint_sha256,kit_manifest_sha256:$kit_manifest_sha256,runner_sha256:{index_v1_sustained:$scale_runner_sha256,catalog_250k:$catalog_runner_sha256}}' \
  >"${output}/part-result.json"
