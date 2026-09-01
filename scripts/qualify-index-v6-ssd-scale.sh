#!/usr/bin/env bash
set -Eeuo pipefail

# Run this script on the SSD qualification host. It needs no checkout or Docker
# daemon there: the exact server, CLI and public-driver binaries live in the
# attestable kit beneath ~/keldra_experiments/kit. Durable state and evidence
# are confined to ~/keldra_experiments.

experiment_root="${HOME}/keldra_experiments"
kit_root="${experiment_root}/kit"
results_root="${experiment_root}/results/index-v6-scale"
work_root="${experiment_root}/work/index-v6-scale"
mode="${KELDRA_V6_SCALE_MODE:-smoke}"
keep_work="${KELDRA_V6_SCALE_KEEP_WORK:-0}"
base_port="${KELDRA_V6_SCALE_PORT:-51051}"
server_rust_log="${KELDRA_V6_SCALE_RUST_LOG:-warn,keldra::index_runtime::v6_summary=info,keldra::single_node_group_commit_config=info}"
query_rate="${KELDRA_V6_SCALE_QUERY_RATE:-20}"
query_max_in_flight="${KELDRA_V6_SCALE_QUERY_MAX_IN_FLIGHT:-32}"
mutation_workers_override="${KELDRA_V6_SCALE_MUTATION_WORKERS:-}"
lag_slope_limit="${KELDRA_V6_SCALE_MAX_LAG_SLOPE_RECORDS_PER_SECOND:-1}"
source_journal_entries="${KELDRA_V6_SCALE_SOURCE_JOURNAL_MAX_ENTRIES:-10000000}"
catalog_only_at_or_above="${KELDRA_V6_SCALE_CATALOG_ONLY_AT_OR_ABOVE_DEFINITIONS:-250000}"
group_max_requests="${KELDRA_V6_SCALE_GROUP_MAX_REQUESTS:-}"
group_max_operations="${KELDRA_V6_SCALE_GROUP_MAX_OPERATIONS:-}"
group_max_inline_bytes="${KELDRA_V6_SCALE_GROUP_MAX_INLINE_BYTES:-}"
group_preparation_lanes="${KELDRA_V6_SCALE_GROUP_PREPARATION_LANES:-}"
group_max_queued_requests="${KELDRA_V6_SCALE_GROUP_MAX_QUEUED_REQUESTS:-}"
group_max_queued_operations="${KELDRA_V6_SCALE_GROUP_MAX_QUEUED_OPERATIONS:-}"
group_max_queued_inline_bytes="${KELDRA_V6_SCALE_GROUP_MAX_QUEUED_INLINE_BYTES:-}"
group_dwell_microseconds="${KELDRA_V6_SCALE_GROUP_DWELL_MICROSECONDS:-}"

case "${mode}" in
  smoke)
    definition_matrix="${KELDRA_V6_SCALE_DEFINITION_MATRIX:-64}"
    recipe_matrix="${KELDRA_V6_SCALE_RECIPE_MATRIX:-1,4}"
    worker_matrix="${KELDRA_V6_SCALE_WORKER_MATRIX:-1,4}"
    memory_per_worker_matrix="${KELDRA_V6_SCALE_MEMORY_PER_WORKER_MATRIX:-268435456}"
    rate_ladder="${KELDRA_V6_SCALE_RATE_LADDER:-100,1000}"
    object_size_matrix="${KELDRA_V6_SCALE_OBJECT_SIZE_MATRIX:-1024}"
    baseline_seconds="${KELDRA_V6_SCALE_BASELINE_SECONDS:-5}"
    concurrent_seconds="${KELDRA_V6_SCALE_CONCURRENT_SECONDS:-20}"
    post_seconds="${KELDRA_V6_SCALE_POST_SECONDS:-5}"
    ;;
  sustained)
    definition_matrix="${KELDRA_V6_SCALE_DEFINITION_MATRIX:-1,64,1000,10000,250000}"
    recipe_matrix="${KELDRA_V6_SCALE_RECIPE_MATRIX:-1,4,16,64}"
    worker_matrix="${KELDRA_V6_SCALE_WORKER_MATRIX:-1,2,4,8}"
    memory_per_worker_matrix="${KELDRA_V6_SCALE_MEMORY_PER_WORKER_MATRIX:-134217728,268435456}"
    rate_ladder="${KELDRA_V6_SCALE_RATE_LADDER:-1000,5000,10000,20000,40000}"
    object_size_matrix="${KELDRA_V6_SCALE_OBJECT_SIZE_MATRIX:-1024,98304}"
    baseline_seconds="${KELDRA_V6_SCALE_BASELINE_SECONDS:-30}"
    concurrent_seconds="${KELDRA_V6_SCALE_CONCURRENT_SECONDS:-300}"
    post_seconds="${KELDRA_V6_SCALE_POST_SECONDS:-30}"
    ;;
  *) echo "KELDRA_V6_SCALE_MODE must be smoke or sustained" >&2; exit 2 ;;
esac

case "${experiment_root}" in
  "${HOME}/keldra_experiments") ;;
  *) echo "experiment root escaped HOME/keldra_experiments" >&2; exit 2 ;;
esac
case "${keep_work}" in 0|1) ;; *) echo "KELDRA_V6_SCALE_KEEP_WORK must be 0 or 1" >&2; exit 2 ;; esac

for command in awk base64 dd flock jq lscpu lsblk ps readlink sha256sum ss tar vmstat; do
  command -v "${command}" >/dev/null 2>&1 || { echo "${command} is required" >&2; exit 2; }
done
for binary in keldra-server keldra index-contention-qualification; do
  [[ -x "${kit_root}/bin/${binary}" ]] || { echo "missing ${kit_root}/bin/${binary}" >&2; exit 2; }
done
[[ -s "${kit_root}/SOURCE_COMMIT" ]] || { echo "missing ${kit_root}/SOURCE_COMMIT" >&2; exit 2; }
[[ -s "${kit_root}/HARNESS_COMMIT" ]] || { echo "missing ${kit_root}/HARNESS_COMMIT" >&2; exit 2; }
[[ -s "${kit_root}/SHA256SUMS" ]] || { echo "missing ${kit_root}/SHA256SUMS" >&2; exit 2; }
(cd "${kit_root}" && awk '$2 == "qualify-index-v6-ssd-scale.sh" { found = 1 } END { exit !found }' SHA256SUMS) || {
  echo "SHA256SUMS does not attest qualify-index-v6-ssd-scale.sh" >&2
  exit 2
}
(cd "${kit_root}" && sha256sum --check SHA256SUMS)
source_commit="$(tr -d '\r\n' <"${kit_root}/SOURCE_COMMIT")"
[[ "${source_commit}" =~ ^[0-9a-fA-F]{40}$ ]] || { echo "SOURCE_COMMIT must be a full Git commit" >&2; exit 2; }
harness_commit="$(tr -d '\r\n' <"${kit_root}/HARNESS_COMMIT")"
[[ "${harness_commit}" =~ ^[0-9a-fA-F]{40}$ ]] || { echo "HARNESS_COMMIT must be a full Git commit" >&2; exit 2; }

positive_integer() { [[ "$1" =~ ^[1-9][0-9]*$ ]]; }
positive_number() { [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]] && awk -v value="$1" 'BEGIN { exit !(value > 0) }'; }
parse_integer_matrix() {
  local label="$1" maximum="$2" raw="$3" value seen=,
  local -a values
  IFS=, read -r -a values <<<"${raw}"
  ((${#values[@]} > 0)) || { echo "${label} must not be empty" >&2; return 2; }
  for value in "${values[@]}"; do
    positive_integer "${value}" && ((value <= maximum)) || {
      echo "${label} must contain unique positive integers no greater than ${maximum}" >&2; return 2;
    }
    case "${seen}" in *",${value},"*) echo "${label} contains duplicate ${value}" >&2; return 2 ;; esac
    seen="${seen}${value},"
  done
}
parse_rate_ladder() {
  local value previous=0
  local -a values
  IFS=, read -r -a values <<<"${rate_ladder}"
  ((${#values[@]} > 0)) || { echo "rate ladder must not be empty" >&2; return 2; }
  for value in "${values[@]}"; do
    positive_number "${value}" || { echo "rate ladder entries must be positive numbers" >&2; return 2; }
    awk -v current="${value}" -v previous="${previous}" 'BEGIN { exit !(current > previous) }' || {
      echo "rate ladder must be strictly ascending" >&2; return 2;
    }
    previous="${value}"
  done
}

parse_integer_matrix definition-matrix 250000 "${definition_matrix}"
parse_integer_matrix recipe-matrix 64 "${recipe_matrix}"
parse_integer_matrix worker-matrix 256 "${worker_matrix}"
parse_integer_matrix memory-per-worker-matrix 68719476736 "${memory_per_worker_matrix}"
parse_integer_matrix object-size-matrix 67108864 "${object_size_matrix}"
parse_rate_ladder
if [[ -n "${mutation_workers_override}" ]]; then
  positive_integer "${mutation_workers_override}" && ((mutation_workers_override <= 256)) || {
    echo "KELDRA_V6_SCALE_MUTATION_WORKERS must be a positive integer no greater than 256" >&2
    exit 2
  }
fi
for value in "${base_port}" "${query_rate}" "${query_max_in_flight}" "${source_journal_entries}" \
  "${baseline_seconds}" "${concurrent_seconds}" "${post_seconds}" "${catalog_only_at_or_above}"
do
  positive_integer "${value}" || { echo "server, query, journal, and duration settings must be positive integers" >&2; exit 2; }
done
positive_number "${lag_slope_limit}" || { echo "lag-slope limit must be positive" >&2; exit 2; }
for value in "${group_max_requests}" "${group_max_operations}" "${group_max_inline_bytes}" \
  "${group_preparation_lanes}" \
  "${group_max_queued_requests}" "${group_max_queued_operations}" \
  "${group_max_queued_inline_bytes}" "${group_dwell_microseconds}"
do
  [[ -z "${value}" ]] || positive_integer "${value}" || {
    echo "KELDRA_V6_SCALE_GROUP_* overrides must be positive integers" >&2
    exit 2
  }
done

group_server_env=()
[[ -z "${group_max_requests}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_REQUESTS=${group_max_requests}")
[[ -z "${group_max_operations}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_OPERATIONS=${group_max_operations}")
[[ -z "${group_max_inline_bytes}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_INLINE_BYTES=${group_max_inline_bytes}")
[[ -z "${group_preparation_lanes}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_PREPARATION_LANES=${group_preparation_lanes}")
[[ -z "${group_max_queued_requests}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_QUEUED_REQUESTS=${group_max_queued_requests}")
[[ -z "${group_max_queued_operations}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_QUEUED_OPERATIONS=${group_max_queued_operations}")
[[ -z "${group_max_queued_inline_bytes}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_MAX_QUEUED_INLINE_BYTES=${group_max_queued_inline_bytes}")
[[ -z "${group_dwell_microseconds}" ]] || group_server_env+=("KELDRA_SINGLE_NODE_GROUP_COMMIT_GROUP_DWELL_MICROSECONDS=${group_dwell_microseconds}")

mkdir -p "${results_root}" "${work_root}"
chmod 0700 "${experiment_root}" "${results_root}" "${work_root}"
exec 9>"${experiment_root}/run.lock"
flock -n 9 || { echo "another Keldra experiment is running" >&2; exit 2; }
ulimit -n 65536

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)-v6-ssd-${source_commit:0:12}"
run_dir="${results_root}/${run_id}"
mkdir "${run_dir}"
ln -sfn "${run_id}" "${results_root}/latest"
summary_rows="${run_dir}/cells.jsonl"
: >"${summary_rows}"
{
  echo "schema=keldra.index-v6-ssd-scale-run.v2"
  echo "server_source_commit=${source_commit}"
  echo "harness_commit=${harness_commit}"
  echo "mode=${mode}"
  echo "definition_matrix=${definition_matrix}"
  echo "physical_recipe_matrix=${recipe_matrix}"
  echo "indexing_worker_matrix=${worker_matrix}"
  echo "mutation_workers_override=${mutation_workers_override:-cell-indexing-workers}"
  echo "memory_per_worker_matrix=${memory_per_worker_matrix}"
  echo "offered_rate_ladder=${rate_ladder}"
  echo "mutation_object_size_matrix=${object_size_matrix}"
  echo "query_rate=${query_rate}"
  echo "query_max_in_flight=${query_max_in_flight}"
  echo "max_lag_slope_records_per_second=${lag_slope_limit}"
  echo "group_max_requests=${group_max_requests:-server-default}"
  echo "group_max_operations=${group_max_operations:-server-default}"
  echo "group_max_inline_bytes=${group_max_inline_bytes:-server-default}"
  echo "group_preparation_lanes=${group_preparation_lanes:-server-default}"
  echo "group_max_queued_requests=${group_max_queued_requests:-server-default}"
  echo "group_max_queued_operations=${group_max_queued_operations:-server-default}"
  echo "group_max_queued_inline_bytes=${group_max_queued_inline_bytes:-server-default}"
  echo "group_dwell_microseconds=${group_dwell_microseconds:-server-default}"
  sha256sum "${kit_root}/bin/keldra-server" "${kit_root}/bin/keldra" \
    "${kit_root}/bin/index-contention-qualification"
  uname -srvmo; lscpu; free -h; df -hT "${experiment_root}"
  lsblk -o NAME,KNAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,ROTA,MODEL
} >"${run_dir}/host-info.txt"

server_pid=""; sampler_pid=""; vmstat_pid=""; active_work=""; active_cell=""
stop_server() {
  if [[ -n "${sampler_pid}" ]]; then kill -TERM "${sampler_pid}" 2>/dev/null || true; wait "${sampler_pid}" 2>/dev/null || true; fi
  if [[ -n "${vmstat_pid}" ]]; then kill -TERM "${vmstat_pid}" 2>/dev/null || true; wait "${vmstat_pid}" 2>/dev/null || true; fi
  if [[ -n "${server_pid}" ]]; then
    kill -TERM "${server_pid}" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "${server_pid}" 2>/dev/null || break; sleep 1; done
    kill -KILL "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  server_pid=""; sampler_pid=""; vmstat_pid=""
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  stop_server
  if [[ -n "${active_cell}" ]]; then printf '%s\n' "${status}" >"${active_cell}/runner-exit-status"; fi
  if [[ "${keep_work}" == 0 && -n "${active_work}" && -d "${active_work}" ]]; then
    if ! remove_cell_work "${active_work}"; then
      echo "failed to remove guarded active work directory ${active_work}" >&2
      status=1
    fi
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

sample_process() {
  local pid="$1" output="$2"
  printf 'timestamp_utc\tepoch_seconds\tcpu_percent\trss_kib\tthreads\tread_bytes\twrite_bytes\tcancelled_write_bytes\tmem_available_kib\n' >"${output}"
  while kill -0 "${pid}" 2>/dev/null; do
    local now epoch cpu rss threads read_bytes write_bytes cancelled available
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; epoch="$(date +%s)"
    read -r cpu rss threads < <(ps -p "${pid}" -o %cpu=,rss=,nlwp= 2>/dev/null || printf '0 0 0')
    read_bytes="$(awk '/^read_bytes:/ {print $2}' "/proc/${pid}/io" 2>/dev/null || printf 0)"
    write_bytes="$(awk '/^write_bytes:/ {print $2}' "/proc/${pid}/io" 2>/dev/null || printf 0)"
    cancelled="$(awk '/^cancelled_write_bytes:/ {print $2}' "/proc/${pid}/io" 2>/dev/null || printf 0)"
    available="$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${now}" "${epoch}" "${cpu}" "${rss}" "${threads}" "${read_bytes}" \
      "${write_bytes}" "${cancelled}" "${available}" >>"${output}"
    sleep 1
  done
}

start_server() {
  local port="$1" cell_work="$2" cell_root="$3" credential_file="$4" pipeline_memory_bytes="$5" workers="$6"
  env "${group_server_env[@]}" TMPDIR="${cell_work}/tmp" RUST_LOG="${server_rust_log}" \
  KELDRA_LISTEN="127.0.0.1:${port}" KELDRA_PEER_LISTEN="127.0.0.1:$((port + 1))" \
  KELDRA_DATA_DIR="${cell_work}" KELDRA_STATE_DIR="${cell_work}/state" \
  KELDRA_METADATA_DIR="${cell_work}/metadata" KELDRA_METADATA_WAL_DIR="${cell_work}/wal" \
  KELDRA_PAYLOAD_DIR="${cell_work}/payload" KELDRA_SCRATCH_DIR="${cell_work}/scratch" \
  KELDRA_CACHE_DIR="${cell_work}/cache" KELDRA_NODE_ID=1 \
  KELDRA_TOKEN_SIGNING_KEY_FILE="${cell_work}/token-signing-key" KELDRA_RUN_SYSTEM_BOOTSTRAP=true \
  KELDRA_SYSTEM_BOOTSTRAP_CREDENTIAL_OUTPUT="${credential_file}" \
  KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=10000000 KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=1000000 \
  KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=10000000 KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=1000000 \
  KELDRA_INDEX_DISK_CACHE_BYTES=1073741824 \
  KELDRA_INDEX_PIPELINE_MEMORY_BYTES="${pipeline_memory_bytes}" \
  KELDRA_INDEXING_CORES="${workers}" KELDRA_SOURCE_JOURNAL_MAX_ENTRIES="${source_journal_entries}" \
    "${kit_root}/bin/keldra-server" >"${cell_root}/server.log" 2>&1 &
  server_pid=$!
  sample_process "${server_pid}" "${cell_root}/server-resources.tsv" & sampler_pid=$!
  vmstat -w 1 >"${cell_root}/host-vmstat.log" & vmstat_pid=$!
  for _ in $(seq 1 180); do
    kill -0 "${server_pid}" 2>/dev/null || break
    ss -ltn "sport = :${port}" | awk 'NR > 1 { found = 1 } END { exit !found }' && return 0
    sleep 1
  done
  echo "server did not listen on ${port}" >&2
  return 1
}

extract_v6_telemetry() {
  local server_log="$1" output="$2"
  awk '
    /keldra_index_v6_summary/ {
      elapsed = metric($0, "keldra_index_v6_summary_elapsed_milliseconds")
      if (elapsed == "null") next
      printf "{\"timestamp_utc\":\"%s\",\"summary_elapsed_milliseconds\":%s", $1, elapsed
      printf ",\"source_rows_total\":%s,\"source_bytes_total\":%s", metric($0, "keldra_index_v6_source_rows_total"), metric($0, "keldra_index_v6_source_bytes_total")
      printf ",\"hot_raw_hits_total\":%s,\"hot_prepared_hits_total\":%s,\"hot_misses_total\":%s,\"hot_evictions_total\":%s", metric($0, "keldra_index_v6_hot_raw_hits_total"), metric($0, "keldra_index_v6_hot_prepared_hits_total"), metric($0, "keldra_index_v6_hot_misses_total"), metric($0, "keldra_index_v6_hot_evictions_total")
      printf ",\"payload_parsed_bytes_total\":%s,\"stage_cpu_nanoseconds_total\":%s,\"stage_queue_wait_nanoseconds_total\":%s,\"stage_submit_wall_nanoseconds_total\":%s", metric($0, "keldra_index_v6_payload_parsed_bytes_total"), metric($0, "keldra_index_v6_stage_cpu_nanoseconds_total"), metric($0, "keldra_index_v6_stage_queue_wait_nanoseconds_total"), metric($0, "keldra_index_v6_stage_submit_wall_nanoseconds_total")
      printf ",\"artifact_shadow_requests_total\":%s,\"artifact_shadow_requested_bytes_total\":%s,\"artifact_shadow_unique_identities_total\":%s,\"artifact_shadow_unique_bytes_total\":%s", metric($0, "keldra_index_v6_artifact_shadow_requests_total"), metric($0, "keldra_index_v6_artifact_shadow_requested_bytes_total"), metric($0, "keldra_index_v6_artifact_shadow_unique_identities_total"), metric($0, "keldra_index_v6_artifact_shadow_unique_bytes_total")
      printf ",\"artifact_shadow_pack_requests_total\":%s,\"artifact_shadow_pack_requested_bytes_total\":%s,\"artifact_shadow_unique_pack_identities_total\":%s,\"artifact_shadow_unique_pack_bytes_total\":%s", metric($0, "keldra_index_v6_artifact_shadow_pack_requests_total"), metric($0, "keldra_index_v6_artifact_shadow_pack_requested_bytes_total"), metric($0, "keldra_index_v6_artifact_shadow_unique_pack_identities_total"), metric($0, "keldra_index_v6_artifact_shadow_unique_pack_bytes_total")
      printf ",\"artifact_shadow_oversize_bypasses_total\":%s,\"artifact_shadow_oversize_bypass_bytes_total\":%s,\"artifact_shadow_metadata_limit_bypasses_total\":%s,\"artifact_shadow_metadata_limit_bypass_bytes_total\":%s,\"artifact_shadow_peak_simulated_resident_bytes\":%s", metric($0, "keldra_index_v6_artifact_shadow_oversize_bypasses_total"), metric($0, "keldra_index_v6_artifact_shadow_oversize_bypass_bytes_total"), metric($0, "keldra_index_v6_artifact_shadow_metadata_limit_bypasses_total"), metric($0, "keldra_index_v6_artifact_shadow_metadata_limit_bypass_bytes_total"), metric($0, "keldra_index_v6_artifact_shadow_peak_simulated_resident_bytes")
      printf ",\"selected_bytes_total\":%s,\"prepared_bytes_total\":%s,\"projected_bytes_total\":%s,\"sealed_bytes_total\":%s", metric($0, "keldra_index_v6_selected_bytes_total"), metric($0, "keldra_index_v6_prepared_bytes_total"), metric($0, "keldra_index_v6_projected_bytes_total"), metric($0, "keldra_index_v6_sealed_bytes_total")
      printf ",\"published_source_rows_total\":%s,\"published_source_bytes_total\":%s,\"checkpointed_source_rows_total\":%s,\"checkpointed_source_bytes_total\":%s", metric($0, "keldra_index_v6_published_source_rows_total"), metric($0, "keldra_index_v6_published_source_bytes_total"), metric($0, "keldra_index_v6_checkpointed_source_rows_total"), metric($0, "keldra_index_v6_checkpointed_source_bytes_total")
      printf ",\"catalog_source_rows_total\":%s,\"catalog_source_bytes_total\":%s,\"catalog_checkpointed_source_rows_total\":%s,\"catalog_checkpointed_source_bytes_total\":%s}\n", metric($0, "keldra_index_v6_catalog_source_rows_total"), metric($0, "keldra_index_v6_catalog_source_bytes_total"), metric($0, "keldra_index_v6_catalog_checkpointed_source_rows_total"), metric($0, "keldra_index_v6_catalog_checkpointed_source_bytes_total")
    }
    function metric(line, key, fragment, position) {
      position = index(line, key "=")
      if (position == 0) return "null"
      fragment = substr(line, position + length(key) + 1)
      sub(/[^0-9.].*$/, "", fragment)
      return fragment == "" ? "null" : fragment
    }
  ' "${server_log}" | jq -c '
    (.timestamp_utc
      | capture("^(?<whole>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\\.(?<fraction>[0-9]+))?Z$")) as $timestamp
    | .timestamp_unix_milliseconds = (
        ($timestamp.whole + "Z" | fromdateiso8601) * 1000
        + (((($timestamp.fraction // "0") + "000")[0:3]) | tonumber)
      )
    | del(.timestamp_utc)
  ' >"${output}"
}

remove_cell_work() {
  local candidate="$1" expected_root canonical_root canonical_candidate
  [[ -n "${candidate}" && -d "${candidate}" ]] || {
    echo "refusing to remove a missing cell work directory" >&2
    return 1
  }
  expected_root="${work_root}/${run_id}"
  canonical_root="$(readlink -f -- "${expected_root}")"
  canonical_candidate="$(readlink -f -- "${candidate}")"
  case "${canonical_candidate}" in
    "${canonical_root}"/*) ;;
    *)
      echo "refusing to remove work outside this experiment" >&2
      return 1
      ;;
  esac
  rm -rf -- "${canonical_candidate}"
}

summarize_cell() {
  local cell="$1" report="$2" progress="$3" resource_samples="$4" telemetry_samples="$5" definitions="$6" recipes="$7" workers="$8" memory_per_worker="$9" offered_rate="${10}" object_bytes="${11}" driver_status="${12}" store_bytes="${13}"
  local resources lag throughput quality
  resources="$(awk -F '\t' '
    NR == 1 { next }
    { cpu += $3; if ($4 > rss) rss = $4; if (NR == 2) first_write = $7; last_write = $7; samples++ }
    END { printf "{\"samples\":%d,\"average_cpu_percent\":%s,\"peak_rss_bytes\":%s,\"server_write_bytes\":%s}", samples, (samples ? cpu / samples : 0), rss * 1024, (samples ? last_write - first_write : 0) }
  ' "${resource_samples}")"
  lag="$(jq -s '
    map(select(.phase == "concurrent"))
    | if length < 2 or (last.phase_elapsed_seconds <= first.phase_elapsed_seconds) then {samples:length,lag_slope_records_per_second:null}
      else {samples:length,lag_slope_records_per_second:((last.latest_source_lag_hint - first.latest_source_lag_hint) / (last.phase_elapsed_seconds - first.phase_elapsed_seconds))} end
  ' "${progress}" 2>/dev/null || printf '{"samples":0,"lag_slope_records_per_second":null}')"
  throughput="$(jq -n --slurpfile progress "${progress}" --slurpfile telemetry "${telemetry_samples}" '
    ($progress | map(select(.phase == "concurrent" and (.timestamp_unix_milliseconds? != null))) | if length == 0 then null else {start:(map(.timestamp_unix_milliseconds) | min),end:(map(.timestamp_unix_milliseconds) | max)} end) as $window
    | if $window == null then {measurement:"missing-driver-phase-wall-clock",samples:0}
      else ($telemetry | map(select(.timestamp_unix_milliseconds >= $window.start and .timestamp_unix_milliseconds <= $window.end)) | sort_by(.timestamp_unix_milliseconds)) as $samples
      | if ($samples | length) < 2 then {measurement:"insufficient-v6-summary-samples",samples:($samples | length),window:$window}
        else ($samples[0]) as $first | ($samples[-1]) as $last
        | [
            "source_rows_total", "source_bytes_total", "hot_misses_total",
            "payload_parsed_bytes_total", "stage_cpu_nanoseconds_total",
            "stage_queue_wait_nanoseconds_total", "stage_submit_wall_nanoseconds_total",
            "artifact_shadow_requests_total", "artifact_shadow_requested_bytes_total",
            "artifact_shadow_unique_identities_total", "artifact_shadow_unique_bytes_total",
            "artifact_shadow_pack_requests_total", "artifact_shadow_pack_requested_bytes_total",
            "artifact_shadow_unique_pack_identities_total", "artifact_shadow_unique_pack_bytes_total",
            "artifact_shadow_oversize_bypasses_total", "artifact_shadow_oversize_bypass_bytes_total",
            "artifact_shadow_metadata_limit_bypasses_total", "artifact_shadow_metadata_limit_bypass_bytes_total",
            "selected_bytes_total", "prepared_bytes_total", "projected_bytes_total",
            "sealed_bytes_total", "checkpointed_source_rows_total", "checkpointed_source_bytes_total"
          ] as $required
        | [$required[] | select($first[.] == null or $last[.] == null)] as $missing
        | (($last.timestamp_unix_milliseconds - $first.timestamp_unix_milliseconds) / 1000) as $seconds
        | if ($missing | length) > 0 then {measurement:"incomplete-v6-summary-counters",samples:($samples | length),window:$window,missing:$missing}
          elif $seconds <= 0 then {measurement:"nonpositive-v6-summary-interval",samples:($samples | length),window:$window}
          else def rate($key): (($last[$key] - $first[$key]) / $seconds);
            {
              measurement:"v6-summary-counter-delta", samples:($samples | length),
              window:$window, elapsed_seconds:$seconds,
              source_rows_per_second:rate("source_rows_total"),
              source_bytes_per_second:rate("source_bytes_total"),
              hot_misses_per_second:rate("hot_misses_total"),
              payload_parsed_bytes_per_second:rate("payload_parsed_bytes_total"),
              stage_cpu_nanoseconds_per_second:rate("stage_cpu_nanoseconds_total"),
              stage_queue_wait_nanoseconds_per_second:rate("stage_queue_wait_nanoseconds_total"),
              stage_submit_wall_nanoseconds_per_second:rate("stage_submit_wall_nanoseconds_total"),
              stage_submit_residual_nanoseconds_per_second:([0, (rate("stage_submit_wall_nanoseconds_total") - rate("stage_queue_wait_nanoseconds_total") - rate("stage_cpu_nanoseconds_total"))] | max),
              artifact_shadow_requests_per_second:rate("artifact_shadow_requests_total"),
              artifact_shadow_requested_bytes_per_second:rate("artifact_shadow_requested_bytes_total"),
              artifact_shadow_unique_identities_per_second:rate("artifact_shadow_unique_identities_total"),
              artifact_shadow_unique_bytes_per_second:rate("artifact_shadow_unique_bytes_total"),
              artifact_shadow_pack_requests_per_second:rate("artifact_shadow_pack_requests_total"),
              artifact_shadow_pack_requested_bytes_per_second:rate("artifact_shadow_pack_requested_bytes_total"),
              artifact_shadow_unique_pack_identities_per_second:rate("artifact_shadow_unique_pack_identities_total"),
              artifact_shadow_unique_pack_bytes_per_second:rate("artifact_shadow_unique_pack_bytes_total"),
              artifact_shadow_oversize_bypasses_per_second:rate("artifact_shadow_oversize_bypasses_total"),
              artifact_shadow_oversize_bypass_bytes_per_second:rate("artifact_shadow_oversize_bypass_bytes_total"),
              artifact_shadow_metadata_limit_bypasses_per_second:rate("artifact_shadow_metadata_limit_bypasses_total"),
              artifact_shadow_metadata_limit_bypass_bytes_per_second:rate("artifact_shadow_metadata_limit_bypass_bytes_total"),
              artifact_shadow_peak_simulated_resident_bytes:($last.artifact_shadow_peak_simulated_resident_bytes // 0),
              selected_bytes_per_second:rate("selected_bytes_total"),
              prepared_bytes_per_second:rate("prepared_bytes_total"),
              projected_bytes_per_second:rate("projected_bytes_total"),
              sealed_bytes_per_second:rate("sealed_bytes_total"),
              checkpointed_source_rows_per_second:rate("checkpointed_source_rows_total"),
              checkpointed_source_bytes_per_second:rate("checkpointed_source_bytes_total")
            }
          end
        end
      end
  ' 2>/dev/null || printf '{"measurement":"unparseable-v6-summary","samples":0}')"
  quality="$(jq -cn --argjson status "${driver_status}" --argjson lag "${lag}" --argjson throughput "${throughput}" --argjson limit "${lag_slope_limit}" --slurpfile report "${report}" '
    ($report[0] // {}) as $r | ($lag.lag_slope_records_per_second) as $slope
    | (($throughput.measurement == "v6-summary-counter-delta")
        and (($throughput.source_rows_per_second // 0) > 0)
        and (($throughput.source_bytes_per_second // 0) > 0)
        and (($throughput.selected_bytes_per_second // 0) > 0)
        and (($throughput.prepared_bytes_per_second // 0) > 0)
        and (($throughput.projected_bytes_per_second // 0) > 0)
        and (($throughput.sealed_bytes_per_second // 0) > 0)
        and (($throughput.checkpointed_source_rows_per_second // 0) > 0)
        and (($throughput.checkpointed_source_bytes_per_second // 0) > 0)) as $telemetry_complete
    | {driver_exit:$status,result:($r.result // "missing-report"),correctness:($r.correctness.passed // false),workload:($r.workload_validity.passed // false),responsiveness:($r.responsiveness.passed // false),telemetry_complete:$telemetry_complete,lag_stationary:($slope != null and $slope <= $limit),classification:(if $status == 0 and ($r.result // "") == "pass" and $telemetry_complete and $slope != null and $slope <= $limit then "sustained" elif $telemetry_complete and (($r.correctness.passed // false) and ($r.workload_validity.passed // false)) then "capacity-limit" else "failure" end)}
  ' 2>/dev/null || printf '{"classification":"failure","result":"unparseable-report"}')"
  jq -cn --arg cell "${cell}" --arg report_path "${report}" --arg progress_path "${progress}" --arg telemetry_path "${telemetry_samples}" \
    --argjson definitions "${definitions}" --argjson recipes "${recipes}" --argjson workers "${workers}" \
    --argjson memory_per_worker "${memory_per_worker}" --argjson offered_rate "${offered_rate}" --argjson object_bytes "${object_bytes}" \
    --argjson store_bytes "${store_bytes}" --argjson resources "${resources}" --argjson lag "${lag}" --argjson throughput "${throughput}" \
    --argjson quality "${quality}" --slurpfile report "${report}" '
      ($report[0] // {}) as $r | ($workers * $memory_per_worker) as $pipeline_memory_bytes | ($pipeline_memory_bytes / 268435456) as $memory_256_mib_units | ($r.mutations // {}) as $m
      | {cell:$cell,logical_definitions:$definitions,qualified_definitions:($r.qualified_definition_count // 0),physical_recipes:$recipes,definition_creation_seconds:($r.definition_creation_seconds // null),definition_creation_per_second:(if ($r.definition_creation_seconds // 0) > 0 then ($definitions / $r.definition_creation_seconds) else null end),qualified_definition_activation_seconds:($r.qualified_definition_activation_seconds // null),indexing_cores:$workers,memory_per_core_bytes:$memory_per_worker,pipeline_memory_bytes:$pipeline_memory_bytes,mutation_record_minimum_bytes:$object_bytes,offered_operations_per_second:$offered_rate,offered_operations:($m.offered_operations // 0),accepted_operations:($m.accepted_operations // 0),accepted_operations_per_second:($m.accepted_operations_per_second // 0),accepted_source_bytes:($m.accepted_bytes // 0),accepted_source_bytes_per_second:($m.accepted_bytes_per_second // 0),runtime_throughput:$throughput,indexed_source_rows_per_second:($throughput.checkpointed_source_rows_per_second // null),indexed_source_rows_per_second_per_core:(($throughput.checkpointed_source_rows_per_second // 0) / $workers),indexed_source_rows_per_second_per_256_mib:(($throughput.checkpointed_source_rows_per_second // 0) / $memory_256_mib_units),accepted_operations_per_second_per_core:(($m.accepted_operations_per_second // 0) / $workers),accepted_operations_per_second_per_256_mib:(($m.accepted_operations_per_second // 0) / $memory_256_mib_units),indexed_end_to_end_operations_per_second:(if (($m.elapsed_seconds // 0) + ($r.drain_seconds // 0)) > 0 then (($m.accepted_operations // 0) / (($m.elapsed_seconds // 0) + ($r.drain_seconds // 0))) else 0 end),drain_seconds:($r.drain_seconds // null),concurrent_query_schedule_to_response:($r.concurrent.schedule_to_response // null),concurrent_query_dispatch_to_response:($r.concurrent.dispatch_to_response // null),publication_visibility_lag:($m.publication_visibility_lag // null),lag:$lag,resources:$resources,durable_store_bytes:$store_bytes,report_path:$report_path,progress_path:$progress_path,telemetry_path:$telemetry_path,quality:$quality,raw_report:$r}
    ' >>"${summary_rows}"
  jq -r '.classification' <<<"${quality}"
}

run_capability_preflight() {
  local port="$1" workers="$2" memory_per_worker="$3"
  local cell="public-query-capabilities" credential_file token_key tenant bucket client_id client_secret report driver_status
  active_cell="${run_dir}/${cell}"
  active_work="${work_root}/${run_id}/${cell}"
  mkdir -p "${active_cell}" "${active_work}"/{state,metadata,wal,payload,scratch,cache,tmp}
  chmod -R 0700 "${active_cell}" "${active_work}"
  credential_file="${active_work}/system-bootstrap-credential.json"
  token_key="${active_work}/token-signing-key"
  dd if=/dev/urandom of="${token_key}" bs=64 count=1 status=none
  chmod 0600 "${token_key}"
  tenant="v6-cap-${source_commit:0:8}-${run_id:0:8}-${port}"
  bucket="capabilities"
  client_id="v6-capability-client"
  client_secret="$(dd if=/dev/urandom bs=32 count=1 status=none | base64 | tr -d '\n')"
  printf 'running\n' >"${active_cell}/status.txt"
  start_server "${port}" "${active_work}" "${active_cell}" "${credential_file}" \
    "$((workers * memory_per_worker))" "${workers}"
  for _ in $(seq 1 180); do [[ -s "${credential_file}" ]] && break; sleep 1; done
  [[ -s "${credential_file}" ]] || { echo "capability bootstrap credentials were not produced" >&2; return 1; }
  KELDRA_NEW_CLIENT_SECRET="${client_secret}" "${kit_root}/bin/keldra" \
    --endpoint "http://127.0.0.1:${port}" --credentials-file "${credential_file}" \
    provision-tenant "${tenant}" v6-capability-owner "${client_id}" \
    >"${active_cell}/provision.stdout.log" 2>"${active_cell}/provision.stderr.log"
  rm -f -- "${credential_file}"
  report="${active_cell}/report.json"
  set +e
  KELDRA_INDEX_CONTENTION_CAPABILITY_ONLY=1 \
  KELDRA_INDEX_CONTENTION_ENDPOINTS="http://127.0.0.1:${port}" \
  KELDRA_INDEX_CONTENTION_TENANT="${tenant}" KELDRA_INDEX_CONTENTION_BUCKET="${bucket}" \
  KELDRA_INDEX_CONTENTION_CLIENT_ID="${client_id}" KELDRA_INDEX_CONTENTION_CLIENT_SECRET="${client_secret}" \
  KELDRA_INDEX_CONTENTION_SERVER_SOURCE_COMMIT="${source_commit}" \
  KELDRA_INDEX_CONTENTION_IMAGE="qualification-kit:${source_commit}" \
  KELDRA_INDEX_CONTENTION_TOPOLOGY=single-node KELDRA_INDEX_CONTENTION_DURABILITY=LOCAL \
  KELDRA_INDEX_CONTENTION_REQUEST_TIMEOUT_MILLISECONDS=30000 \
  KELDRA_INDEX_CONTENTION_DRAIN_TIMEOUT_SECONDS=600 \
  KELDRA_INDEX_CONTENTION_OUTPUT="${report}" \
    "${kit_root}/bin/index-contention-qualification" \
    >"${active_cell}/driver.stdout.log" 2>"${active_cell}/driver.stderr.log"
  driver_status=$?
  set -e
  stop_server
  if ((driver_status != 0)) || [[ ! -s "${report}" ]] \
    || ! jq -e '.result == "pass" and (.checks == ["exact","range","order","facet","aggregate","full-text"])' "${report}" >/dev/null
  then
    printf 'failure\n' >"${active_cell}/status.txt"
    return 1
  fi
  printf 'pass\n' >"${active_cell}/status.txt"
  if [[ "${keep_work}" == 0 ]]; then remove_cell_work "${active_work}"; fi
  active_work=""
  active_cell=""
}

IFS=, read -r -a definitions_values <<<"${definition_matrix}"
IFS=, read -r -a recipe_values <<<"${recipe_matrix}"
IFS=, read -r -a worker_values <<<"${worker_matrix}"
IFS=, read -r -a memory_values <<<"${memory_per_worker_matrix}"
IFS=, read -r -a rates <<<"${rate_ladder}"
IFS=, read -r -a object_sizes <<<"${object_size_matrix}"
catalog_rate="${KELDRA_V6_SCALE_CATALOG_RATE:-${rates[0]}}"
positive_number "${catalog_rate}" || { echo "catalog rate must be positive" >&2; exit 2; }
physical_axis_definitions="${KELDRA_V6_SCALE_PHYSICAL_AXIS_DEFINITIONS:-64}"
resource_axis_definitions="${KELDRA_V6_SCALE_RESOURCE_AXIS_DEFINITIONS:-64}"
positive_integer "${physical_axis_definitions}" && ((physical_axis_definitions <= 250000)) || { echo "invalid physical axis D" >&2; exit 2; }
positive_integer "${resource_axis_definitions}" && ((resource_axis_definitions <= 250000)) || { echo "invalid resource axis D" >&2; exit 2; }
maximum_workers=0; maximum_memory_per_worker=0
for workers in "${worker_values[@]}"; do ((workers > maximum_workers)) && maximum_workers="${workers}"; done
for bytes in "${memory_values[@]}"; do ((bytes > maximum_memory_per_worker)) && maximum_memory_per_worker="${bytes}"; done

port="${base_port}"; fatal_cells=0
run_capability_preflight "${port}" "${maximum_workers}" "${maximum_memory_per_worker}"
port=$((port + 2))
for definitions in "${definitions_values[@]}"; do
  for recipes in "${recipe_values[@]}"; do
    ((recipes <= definitions)) || continue
    for workers in "${worker_values[@]}"; do
      for memory_per_worker in "${memory_values[@]}"; do
        if ! { ((recipes == 1 && workers == maximum_workers && memory_per_worker == maximum_memory_per_worker)) || ((definitions == physical_axis_definitions && workers == maximum_workers && memory_per_worker == maximum_memory_per_worker)) || ((definitions == resource_axis_definitions && recipes == 1)); }; then continue; fi
        pipeline_memory_bytes=$((workers * memory_per_worker))
        mutation_workers="${mutation_workers_override:-${workers}}"
        for object_bytes in "${object_sizes[@]}"; do
          # The pathological object is a D1/P1 cell. It measures the known
          # large-object ingestion shape without multiplying catalog or recipe
          # cardinality into a source-payload experiment.
          if ((object_bytes > 1024)) && ! ((definitions == 1 && recipes == 1 && workers == maximum_workers && memory_per_worker == maximum_memory_per_worker)); then
            continue
          fi
          reached_limit=0
          for offered_rate in "${rates[@]}"; do
          if ((definitions >= catalog_only_at_or_above)) \
            && ! awk -v current="${offered_rate}" -v catalog="${catalog_rate}" 'BEGIN { exit !(current == catalog) }'
          then
            continue
          fi
          ((reached_limit == 0)) || break
          cell="d${definitions}-p${recipes}-w${workers}-m${memory_per_worker}-b${object_bytes}-r${offered_rate//./_}"
          if [[ -n "${mutation_workers_override}" ]]; then
            cell="${cell}-cw${mutation_workers}"
          fi
          active_cell="${run_dir}/${cell}"; active_work="${work_root}/${run_id}/${cell}"
          mkdir -p "${active_cell}" "${active_work}"/{state,metadata,wal,payload,scratch,cache,tmp}; chmod -R 0700 "${active_cell}" "${active_work}"
          credential_file="${active_work}/system-bootstrap-credential.json"; token_key="${active_work}/token-signing-key"
          dd if=/dev/urandom of="${token_key}" bs=64 count=1 status=none; chmod 0600 "${token_key}"
          tenant="v6-${source_commit:0:8}-${run_id:0:8}-${port}"; bucket="objects"; client_id="v6-index-client"
          client_secret="$(dd if=/dev/urandom bs=32 count=1 status=none | base64 | tr -d '\n')"
          printf 'running %s\n' "${cell}" >"${active_cell}/status.txt"
          start_server "${port}" "${active_work}" "${active_cell}" "${credential_file}" "${pipeline_memory_bytes}" "${workers}"
          for _ in $(seq 1 180); do [[ -s "${credential_file}" ]] && break; sleep 1; done
          if [[ ! -s "${credential_file}" ]]; then echo "bootstrap credentials were not produced" >&2; stop_server; fatal_cells=$((fatal_cells + 1)); printf 'failure\n' >"${active_cell}/status.txt"; continue; fi
          KELDRA_NEW_CLIENT_SECRET="${client_secret}" "${kit_root}/bin/keldra" --endpoint "http://127.0.0.1:${port}" --credentials-file "${credential_file}" provision-tenant "${tenant}" v6-owner "${client_id}" >"${active_cell}/provision.stdout.log" 2>"${active_cell}/provision.stderr.log"
          rm -f -- "${credential_file}"
          KELDRA_CLIENT_ID="${client_id}" KELDRA_CLIENT_SECRET="${client_secret}" "${kit_root}/bin/keldra" --endpoint "http://127.0.0.1:${port}" create-bucket "${bucket}" >"${active_cell}/bucket.stdout.log" 2>"${active_cell}/bucket.stderr.log"
          report="${active_cell}/report.json"; progress="${active_cell}/driver-progress.jsonl"
          set +e
          KELDRA_INDEX_CONTENTION_ENDPOINTS="http://127.0.0.1:${port}" KELDRA_INDEX_CONTENTION_TENANT="${tenant}" KELDRA_INDEX_CONTENTION_BUCKET="${bucket}" KELDRA_INDEX_CONTENTION_CLIENT_ID="${client_id}" KELDRA_INDEX_CONTENTION_CLIENT_SECRET="${client_secret}" KELDRA_INDEX_CONTENTION_SERVER_SOURCE_COMMIT="${source_commit}" KELDRA_INDEX_CONTENTION_IMAGE="qualification-kit:${source_commit}" KELDRA_INDEX_CONTENTION_TOPOLOGY=single-node KELDRA_INDEX_CONTENTION_DURABILITY=LOCAL KELDRA_INDEX_CONTENTION_DEFINITION_COUNT="${definitions}" KELDRA_INDEX_CONTENTION_PHYSICAL_RECIPE_COUNT="${recipes}" KELDRA_INDEX_CONTENTION_BASELINE_SECONDS="${baseline_seconds}" KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS="${concurrent_seconds}" KELDRA_INDEX_CONTENTION_POST_SECONDS="${post_seconds}" KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND="${offered_rate}" KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES="${object_bytes}" KELDRA_INDEX_CONTENTION_MUTATION_WORKERS="${mutation_workers}" KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE=32 KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH="$((mutation_workers * 8))" KELDRA_INDEX_CONTENTION_QUERY_RATE="${query_rate}" KELDRA_INDEX_CONTENTION_QUERY_MAX_IN_FLIGHT="${query_max_in_flight}" KELDRA_INDEX_CONTENTION_REQUEST_TIMEOUT_MILLISECONDS=30000 KELDRA_INDEX_CONTENTION_DRAIN_TIMEOUT_SECONDS=600 KELDRA_INDEX_CONTENTION_VISIBILITY_OBSERVATION_TIMEOUT_SECONDS=600 KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS=2000 KELDRA_INDEX_CONTENTION_MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS=30000 KELDRA_INDEX_CONTENTION_OUTPUT="${report}" KELDRA_INDEX_CONTENTION_PROGRESS_JSONL="${progress}" "${kit_root}/bin/index-contention-qualification" >"${active_cell}/driver.stdout.log" 2>"${active_cell}/driver.stderr.log"
          driver_status=$?
          set -e
          store_bytes="$(du -sb "${active_work}/metadata" "${active_work}/payload" "${active_work}/wal" 2>/dev/null | awk '{total += $1} END {print total + 0}')"
          stop_server
          telemetry_samples="${active_cell}/v6-summary.jsonl"
          extract_v6_telemetry "${active_cell}/server.log" "${telemetry_samples}"
          [[ -s "${report}" ]] || printf '{"result":"missing-report"}\n' >"${report}"
          classification="$(summarize_cell "${cell}" "${report}" "${progress}" "${active_cell}/server-resources.tsv" "${telemetry_samples}" "${definitions}" "${recipes}" "${workers}" "${memory_per_worker}" "${offered_rate}" "${object_bytes}" "${driver_status}" "${store_bytes}")"
          printf '%s\n' "${classification}" >"${active_cell}/status.txt"
          case "${classification}" in sustained) ;; capacity-limit) reached_limit=1 ;; *) fatal_cells=$((fatal_cells + 1)); reached_limit=1 ;; esac
          if [[ "${keep_work}" == 0 ]]; then remove_cell_work "${active_work}"; fi
          active_work=""; active_cell=""; port=$((port + 2))
        done
        done
      done
    done
  done
done

jq -s --arg run_id "${run_id}" --arg mode "${mode}" --arg source_commit "${source_commit}" --arg harness_commit "${harness_commit}" --argjson fatal_cells "${fatal_cells}" --slurpfile capability "${run_dir}/public-query-capabilities/report.json" '{schema:"keldra.index-v6-ssd-scale.v2",run_id:$run_id,mode:$mode,server_source_commit:$source_commit,harness_commit:$harness_commit,public_query_capabilities:$capability[0],fatal_cells:$fatal_cells,cells:.}' "${summary_rows}" >"${run_dir}/report.json"
tar -C "${results_root}" -czf "${results_root}/${run_id}.results.tar.gz" "${run_id}"
sha256sum "${results_root}/${run_id}.results.tar.gz" >"${results_root}/${run_id}.results.tar.gz.sha256"
echo "results=${run_dir}"
echo "archive=${results_root}/${run_id}.results.tar.gz"
((fatal_cells == 0))
