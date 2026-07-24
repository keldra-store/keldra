#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

profile="${1:-quick}"
case "${profile}" in
  quick|release) ;;
  *)
    echo "usage: $0 [quick|release]" >&2
    exit 2
    ;;
esac

manifest="${ANVIL_COREMETA_PERF_MANIFEST:-ops/perf/coremeta-release-gate.json}"
output="${ANVIL_COREMETA_PERF_OUTPUT:-target/anvil/perf/coremeta/${profile}/report.json}"
if [[ "${manifest}" != /* ]]; then
  manifest="${repo_root}/${manifest}"
fi
if [[ "${output}" != /* ]]; then
  output="${repo_root}/${output}"
fi
output_dir="$(dirname "${output}")"
mkdir -p "${output_dir}"
rm -f "${output}" "${output_dir}/gate-manifest.json"

echo "[coremeta-perf-gate] running ${profile} profile"
echo "[coremeta-perf-gate] manifest=${manifest}"
echo "[coremeta-perf-gate] output=${output}"

set -o pipefail
cargo bench --locked \
  -p anvil-storage-core \
  --features coremeta-perf-gate \
  --bench coremeta_release_gate \
  -- \
  --profile "${profile}" \
  --manifest "${manifest}" \
  --output "${output}" \
  2>&1 | tee "${output_dir}/run.log"

python3 scripts/check-coremeta-perf-report.py \
  --report "${output}" \
  --profile "${profile}"
