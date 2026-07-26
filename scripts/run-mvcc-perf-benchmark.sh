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
rm -f "${output_dir}/run.log" "${output_dir}/metadata.txt"

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
    2>&1 | tee -a "${output_dir}/run.log"
done
