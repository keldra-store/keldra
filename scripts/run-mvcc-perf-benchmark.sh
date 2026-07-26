#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

profile="${1:-quick}"
case "${profile}" in
  quick)
    default_iterations=1
    ;;
  release)
    default_iterations=5
    ;;
  *)
    echo "usage: $0 [quick|release]" >&2
    exit 2
    ;;
esac

iterations="${ANVIL_MVCC_PERF_ITERATIONS:-${default_iterations}}"
if [[ ! "${iterations}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ANVIL_MVCC_PERF_ITERATIONS must be a positive integer" >&2
  exit 2
fi

output_dir="${ANVIL_MVCC_PERF_OUTPUT_DIR:-target/anvil/perf/mvcc/${profile}}"
if [[ "${output_dir}" != /* ]]; then
  output_dir="${repo_root}/${output_dir}"
fi
mkdir -p "${output_dir}"
rm -f "${output_dir}/run.log" "${output_dir}/metadata.txt" "${output_dir}/report.csv"

report_schema="iteration,shape,keys,tables,payload_bytes,concurrency,phase,nanos"
echo "${report_schema}" >"${output_dir}/report.csv"

{
  echo "profile=${profile}"
  echo "iterations=${iterations}"
  echo "git_commit=${GITHUB_SHA:-$(git rev-parse HEAD)}"
  echo "rustc=$(rustc --version)"
  echo "cargo=$(cargo --version)"
} >"${output_dir}/metadata.txt"

echo "[mvcc-perf] profile=${profile} iterations=${iterations}"
echo "[mvcc-perf] output=${output_dir}"

for iteration in $(seq 1 "${iterations}"); do
  echo "[mvcc-perf] iteration=${iteration}/${iterations}" | tee -a "${output_dir}/run.log"
  cargo bench --locked \
    -p anvil-storage-core \
    --bench mvcc_rfc \
    2>&1 | tee -a "${output_dir}/run.log" >(
      awk -F, -v iteration="${iteration}" '
        NF == 7 &&
        $2 ~ /^[0-9]+$/ &&
        $3 ~ /^[0-9]+$/ &&
        $4 ~ /^[0-9]+$/ &&
        $5 ~ /^[0-9]+$/ &&
        $7 ~ /^[0-9]+$/ {
          print iteration "," $0
        }
      ' >>"${output_dir}/report.csv"
    )
done

required_workloads=(
  metadata_only
  small_inline_object
  large_streaming_erasure
  one_logical_key
  ten_logical_keys
  cross_table_partition
  unrelated_concurrency
  same_key_conflict
  overlapping_range_conflict
  local_durability
  quorum_durability
  erasure_durability
  group_commit
  replication_reconnect_resume
  retained_history_read
  mvcc_garbage_collection
  deferred_repair
)

for workload in "${required_workloads[@]}"; do
  if ! awk -F, -v workload="${workload}" 'NR > 1 && $2 == workload { found = 1 } END { exit !found }' \
    "${output_dir}/report.csv"; then
    echo "missing required MVCC benchmark workload: ${workload}" >&2
    exit 1
  fi
done

echo "report_schema=${report_schema}" >>"${output_dir}/metadata.txt"
echo "[mvcc-perf] report=${output_dir}/report.csv"
