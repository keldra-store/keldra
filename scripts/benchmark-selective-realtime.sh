#!/usr/bin/env bash
set -uo pipefail

# Eight directly comparable SSD runs: the unchanged STANDARD baseline, then
# selectively real-time BulkWrite requests from 30% through 100% in 10% steps.
# Qualification failures are evidence: they are recorded and never stop the
# remaining cases from running.

experiment_root="$(readlink -m -- "${KELDRA_V1_SCALE_EXPERIMENT_ROOT:-${HOME}/keldra_experiments}")"
kit_root="${KELDRA_V1_SCALE_KIT_ROOT:-${experiment_root}/kit}"
runner="${kit_root}/qualify-index-v1-ssd-scale.sh"
suite_id="$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)-selective-realtime"
suite_root="${experiment_root}/results/selective-realtime/${suite_id}"
rows="${suite_root}/runs.jsonl"
percentages="${KELDRA_V1_REALTIME_SUITE_PERCENTAGES:-30,40,50,60,70,80,90,100}"

[[ -x "${runner}" ]] || { echo "missing executable ${runner}" >&2; exit 2; }
mkdir -p "${suite_root}"
chmod 0700 "${suite_root}"
: >"${rows}"

run_case() {
  local label="$1" percent="$2" log="${suite_root}/${label}.runner.log"
  local status results report driver_report
  echo "starting ${label}: realtime_request_percent=${percent}"
  set +e
  KELDRA_V1_SCALE_MODE=sustained \
  KELDRA_V1_SCALE_DEFINITION_MATRIX="${KELDRA_V1_REALTIME_SUITE_DEFINITIONS:-1}" \
  KELDRA_V1_SCALE_RECIPE_MATRIX="${KELDRA_V1_REALTIME_SUITE_RECIPES:-1}" \
  KELDRA_V1_SCALE_WORKER_MATRIX="${KELDRA_V1_REALTIME_SUITE_INDEXING_CORES:-8}" \
  KELDRA_V1_SCALE_MEMORY_PER_WORKER_MATRIX="${KELDRA_V1_REALTIME_SUITE_MEMORY_PER_CORE_BYTES:-805306368}" \
  KELDRA_V1_SCALE_QUERY_MEMORY_BYTES="${KELDRA_V1_REALTIME_SUITE_QUERY_MEMORY_BYTES:-4294967296}" \
  KELDRA_V1_SCALE_MUTATION_WORKERS="${KELDRA_V1_REALTIME_SUITE_MUTATION_WORKERS:-256}" \
  KELDRA_V1_SCALE_MUTABLE_RECORDS="${KELDRA_V1_REALTIME_SUITE_MUTABLE_RECORDS:-262144}" \
  KELDRA_V1_SCALE_PRESEEDED_MUTABLE_RECORDS="${KELDRA_V1_REALTIME_SUITE_PRESEEDED_MUTABLE_RECORDS:-256}" \
  KELDRA_V1_SCALE_RATE_LADDER="${KELDRA_V1_REALTIME_SUITE_OFFERED_OPERATIONS_PER_SECOND:-4000}" \
  KELDRA_V1_SCALE_OBJECT_SIZE_MATRIX="${KELDRA_V1_REALTIME_SUITE_OBJECT_BYTES:-1024}" \
  KELDRA_V1_SCALE_BASELINE_SECONDS="${KELDRA_V1_REALTIME_SUITE_BASELINE_SECONDS:-30}" \
  KELDRA_V1_SCALE_CONCURRENT_SECONDS="${KELDRA_V1_REALTIME_SUITE_CONCURRENT_SECONDS:-300}" \
  KELDRA_V1_SCALE_POST_SECONDS="${KELDRA_V1_REALTIME_SUITE_POST_SECONDS:-30}" \
  KELDRA_V1_SCALE_REALTIME_REQUEST_PERCENT="${percent}" \
    "${runner}" >"${log}" 2>&1
  status=$?

  results="$(awk -F= '$1 == "results" { value=$2 } END { print value }' "${log}")"
  report=""
  if [[ -n "${results}" ]]; then
    report="${results}/report.json"
  fi
  driver_report=""
  if [[ -s "${report}" ]]; then
    driver_report="$(jq -r '.cells[0].report_path // empty' "${report}")"
  fi
  if [[ -s "${report}" && -s "${driver_report}" ]]; then
    jq -cn \
      --arg label "${label}" --argjson percent "${percent}" --argjson runner_exit "${status}" \
      --arg results "${results}" --arg runner_log "${log}" \
      --slurpfile scale "${report}" --slurpfile driver "${driver_report}" '
        ($scale[0].cells[0] // {}) as $cell |
        ($driver[0].mutations // $driver[0].partial_evidence.mutations // {}) as $mutations |
        {label:$label,realtime_request_percent:$percent,runner_exit:$runner_exit,
         results:$results,runner_log:$runner_log,
         driver_result:($driver[0].result // "missing"),
         correctness:($driver[0].correctness.passed // false),
         workload_validity:($driver[0].workload_validity.passed // false),
         responsiveness:($driver[0].responsiveness.passed // false),
         successful_ingest_operations_per_second:($mutations.successful_data_ingest_throughput_operations_per_second // null),
         successful_operations_in_window:($mutations.successful_data_operations_in_window // null),
         successful_realtime_data_operations:($mutations.successful_realtime_data_operations // null),
         successful_standard_data_operations:($mutations.successful_standard_data_operations // null),
         structurally_valid_realtime_batches:($mutations.structurally_valid_realtime_batches // null),
         structurally_valid_standard_batches:($mutations.structurally_valid_standard_batches // null),
         indexed_physical_rows_per_second:($cell.indexed_physical_rows_per_second // null),
         concurrent_query_p99_milliseconds:($driver[0].concurrent.successful_dispatch_to_response_latency.p99_milliseconds // null),
         realtime_visibility_p99_milliseconds:($mutations.successful_receipt_to_query_visibility_latency.p99_milliseconds // null),
         process_cpu_percent:($cell.resources.measurement_window_prorated_time_weighted_process_cpu_percent // null),
         peak_rss_bytes:($cell.resources.measurement_window_sampled_peak_process_rss_bytes // null),
         disk:$cell.block_device_resources,
         quality:$cell.quality}
      ' >>"${rows}"
  else
    jq -cn --arg label "${label}" --argjson percent "${percent}" \
      --argjson runner_exit "${status}" --arg results "${results}" --arg runner_log "${log}" \
      '{label:$label,realtime_request_percent:$percent,runner_exit:$runner_exit,
        results:$results,runner_log:$runner_log,evidence:"runner did not produce a complete report"}' \
      >>"${rows}"
  fi
  echo "completed ${label}: exit=${status} results=${results:-missing}"
}

# Percent zero is the existing suite behavior; the driver defaults to STANDARD.
run_case baseline-standard 0
IFS=, read -r -a requested_percentages <<<"${percentages}"
for percent in "${requested_percentages[@]}"; do
  [[ "${percent}" =~ ^([3-9]0|100)$ ]] || {
    echo "invalid selective real-time percentage ${percent}; expected 30..100 by tens" >&2
    exit 2
  }
  run_case "realtime-${percent}" "${percent}"
done

jq -s --arg suite_id "${suite_id}" '
  {schema:"keldra.selective-realtime-benchmark.v1",suite_id:$suite_id,
   expected_runs:8,completed_runs:length,all_runs_recorded:(length == 8),runs:.}
' "${rows}" >"${suite_root}/report.json"
echo "suite_results=${suite_root}"
