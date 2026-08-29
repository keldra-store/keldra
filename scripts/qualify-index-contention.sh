#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/tests/cluster/docker-compose.yml"
start_node="${repo_root}/tests/cluster/start-node.sh"
requested_image="${KELDRA_IMAGE:-}"
baseline_image="${KELDRA_INDEX_CONTENTION_BASELINE_IMAGE:-}"
comparison_order="${KELDRA_INDEX_CONTENTION_COMPARISON_ORDER:-baseline-first}"
mode="${KELDRA_INDEX_CONTENTION_MODE:-smoke}"
topology="${KELDRA_INDEX_CONTENTION_TOPOLOGY:-single}"
keep="${KELDRA_INDEX_CONTENTION_KEEP:-0}"
server_rust_log="${KELDRA_INDEX_CONTENTION_RUST_LOG:-info,keldra::index_runtime::cpu=warn}"
index_disk_cache_bytes="${KELDRA_INDEX_CONTENTION_INDEX_DISK_CACHE_BYTES:-1073741824}"
index_memory_percent="${KELDRA_INDEX_CONTENTION_INDEX_MEMORY_PERCENT:-20}"
index_kind_budget_bytes="${KELDRA_INDEX_CONTENTION_INDEX_KIND_BUDGET_BYTES:-268435456}"
index_compaction_lanes="${KELDRA_INDEX_CONTENTION_INDEX_COMPACTION_MAX_LANES:-4}"
index_projection_lanes="${KELDRA_INDEX_CONTENTION_INDEX_PROJECTION_MAX_LANES:-4}"
index_rayon_workers="${KELDRA_INDEX_CONTENTION_INDEX_RAYON_WORKERS:-4}"
source_journal_entries="${KELDRA_INDEX_CONTENTION_SOURCE_JOURNAL_MAX_ENTRIES:-1000000}"
max_concurrent_query_p99_ms="${KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS:-2000}"
max_publication_visibility_p99_ms="${KELDRA_INDEX_CONTENTION_MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS:-30000}"
request_timeout_ms="${KELDRA_INDEX_CONTENTION_REQUEST_TIMEOUT_MILLISECONDS:-30000}"
drain_timeout_seconds="${KELDRA_INDEX_CONTENTION_DRAIN_TIMEOUT_SECONDS:-600}"
visibility_poll_ms="${KELDRA_INDEX_CONTENTION_VISIBILITY_POLL_MILLISECONDS:-100}"
visibility_observation_timeout_seconds="${KELDRA_INDEX_CONTENTION_VISIBILITY_OBSERVATION_TIMEOUT_SECONDS:-${drain_timeout_seconds}}"
visibility_sample_every_batches="${KELDRA_INDEX_CONTENTION_VISIBILITY_SAMPLE_EVERY_BATCHES:-16}"
mutation_workers="${KELDRA_INDEX_CONTENTION_MUTATION_WORKERS:-4}"
mutation_batch_size="${KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE:-32}"
mutation_record_bytes="${KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES:-0}"
mutation_queue_depth="${KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH:-32}"
mutation_rate_operations_per_second="${KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND:-disabled}"
qualification_backend="${KELDRA_INDEX_CONTENTION_BACKEND:-docker}"
driver_backend="${KELDRA_INDEX_CONTENTION_DRIVER_BACKEND:-ssh-macos}"
driver_host="${KELDRA_INDEX_CONTENTION_DRIVER_HOST:-zcourts@192.168.64.1}"
driver_identity_file="${KELDRA_INDEX_CONTENTION_DRIVER_IDENTITY_FILE:-${HOME}/.ssh/debian1_id_ed25519}"
driver_repo_root="${KELDRA_INDEX_CONTENTION_DRIVER_REPO_ROOT:-/Users/zcourts/projects/keldra/keldra}"
server_advertise_host="${KELDRA_INDEX_CONTENTION_SERVER_ADVERTISE_HOST:-192.168.64.3}"

case "${mode}" in
  smoke)
    matrix="${KELDRA_INDEX_CONTENTION_MATRIX:-1}"
    baseline_seconds="${KELDRA_INDEX_CONTENTION_BASELINE_SECONDS:-2}"
    concurrent_seconds="${KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS:-5}"
    post_seconds="${KELDRA_INDEX_CONTENTION_POST_SECONDS:-2}"
    ;;
  sustained)
    matrix="${KELDRA_INDEX_CONTENTION_MATRIX:-1,4,16,64}"
    baseline_seconds="${KELDRA_INDEX_CONTENTION_BASELINE_SECONDS:-120}"
    concurrent_seconds="${KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS:-600}"
    post_seconds="${KELDRA_INDEX_CONTENTION_POST_SECONDS:-120}"
    ;;
  *) echo "KELDRA_INDEX_CONTENTION_MODE must be smoke or sustained" >&2; exit 2 ;;
esac
case "${topology}" in
  single) driver_topology=single-node; durability=LOCAL ;;
  three) driver_topology=three-node; durability=REPLICATED ;;
  *) echo "KELDRA_INDEX_CONTENTION_TOPOLOGY must be single or three" >&2; exit 2 ;;
esac
case "${keep}" in 0|1) ;; *) echo "KELDRA_INDEX_CONTENTION_KEEP must be 0 or 1" >&2; exit 2 ;; esac
case "${qualification_backend}" in
  docker) ;;
  *) echo "KELDRA_INDEX_CONTENTION_BACKEND must be docker" >&2; exit 2 ;;
esac
case "${driver_backend}" in
  local|ssh-macos) ;;
  *) echo "KELDRA_INDEX_CONTENTION_DRIVER_BACKEND must be local or ssh-macos" >&2; exit 2 ;;
esac
if [[ "${driver_backend}" == ssh-macos && "${topology}" != single ]]; then
  echo "ssh-macos driver currently requires single topology; three-node Compose ports remain loopback-only" >&2
  exit 2
fi
case "${driver_host}${driver_identity_file}${driver_repo_root}${server_advertise_host}" in
  *$'\n'*|*$'\r'*) echo "driver/server location values must be single-line" >&2; exit 2 ;;
esac
for phase_seconds in "${baseline_seconds}" "${concurrent_seconds}" "${post_seconds}"; do
  if [[ ! "${phase_seconds}" =~ ^[1-9][0-9]*$ ]]; then
    echo "contention phase durations must be positive decimal integers" >&2
    exit 2
  fi
done
for server_limit in "${index_disk_cache_bytes}" "${index_memory_percent}" \
  "${index_kind_budget_bytes}" "${index_compaction_lanes}" \
  "${index_projection_lanes}" "${index_rayon_workers}" "${source_journal_entries}"
do
  if [[ ! "${server_limit}" =~ ^[1-9][0-9]*$ ]]; then
    echo "contention server limits must be positive decimal integers" >&2
    exit 2
  fi
done
if ((index_memory_percent > 100)); then
  echo "KELDRA_INDEX_CONTENTION_INDEX_MEMORY_PERCENT must not exceed 100" >&2
  exit 2
fi
if [[ "${max_concurrent_query_p99_ms}" != disabled ]] \
  && { [[ ! "${max_concurrent_query_p99_ms}" =~ ^[0-9]+([.][0-9]+)?$ ]] \
    || ! awk -v value="${max_concurrent_query_p99_ms}" 'BEGIN {exit !(value > 0)}'; }
then
  echo "concurrent-query p99 gate must be positive milliseconds or disabled" >&2
  exit 2
fi
if [[ "${max_publication_visibility_p99_ms}" != disabled ]] \
  && { [[ ! "${max_publication_visibility_p99_ms}" =~ ^[0-9]+([.][0-9]+)?$ ]] \
    || ! awk -v value="${max_publication_visibility_p99_ms}" 'BEGIN {exit !(value > 0)}'; }
then
  echo "publication-visibility p99 gate must be positive milliseconds or disabled" >&2
  exit 2
fi
for timeout_value in "${request_timeout_ms}" "${drain_timeout_seconds}" \
  "${visibility_poll_ms}" "${visibility_observation_timeout_seconds}" \
  "${visibility_sample_every_batches}" "${mutation_workers}" \
  "${mutation_batch_size}" "${mutation_queue_depth}"
do
  if [[ ! "${timeout_value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "request, drain, visibility, and sampling settings must be positive decimal integers" >&2
    exit 2
  fi
done
if [[ ! "${mutation_record_bytes}" =~ ^[0-9]+$ ]] || ((mutation_record_bytes > 64 * 1024 * 1024)); then
  echo "KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES must be 0..67108864" >&2
  exit 2
fi
if ((mutation_queue_depth < mutation_workers)); then
  echo "KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH must cover every mutation worker" >&2
  exit 2
fi
if [[ "${mutation_rate_operations_per_second}" != disabled ]] \
  && { [[ ! "${mutation_rate_operations_per_second}" =~ ^[0-9]+([.][0-9]+)?$ ]] \
    || ! awk -v value="${mutation_rate_operations_per_second}" \
      'BEGIN {exit !(value > 0 && value <= 1000000)}'; }
then
  echo "mutation rate must be in 0..=1000000 operations/s or disabled" >&2
  exit 2
fi
IFS=, read -r -a builder_matrix <<<"${matrix}"
if ((${#builder_matrix[@]} == 0)); then
  echo "KELDRA_INDEX_CONTENTION_MATRIX must not be empty" >&2
  exit 2
fi
seen_builder_values=,
for builders in "${builder_matrix[@]}"; do
  if [[ ! "${builders}" =~ ^[1-9][0-9]*$ ]] || ((builders > 64)); then
    echo "KELDRA_INDEX_CONTENTION_MATRIX must contain unique integers from 1 through 64" >&2
    exit 2
  fi
  case "${seen_builder_values}" in
    *,"${builders}",*)
      echo "KELDRA_INDEX_CONTENTION_MATRIX must contain unique integers from 1 through 64" >&2
      exit 2
      ;;
  esac
  seen_builder_values="${seen_builder_values}${builders},"
done

for command in docker git jq; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required" >&2
    exit 2
  }
done
docker info >/dev/null 2>&1 || {
  echo "Docker daemon is required for the Linux Keldra server backend" >&2
  exit 2
}
if [[ "${topology}" == three ]]; then
  docker compose version >/dev/null
fi

source_commit="$(git -C "${repo_root}" rev-parse --verify 'HEAD^{commit}')"
if [[ ! "${source_commit}" =~ ^[0-9a-f]{40}$ ]] \
  || [[ -n "$(git -C "${repo_root}" status --porcelain=v1 --untracked-files=normal)" ]]
then
  echo "qualification requires an unchanged clean tree so its source revision is exact" >&2
  exit 2
fi


if [[ -z "${requested_image}" ]]; then
  echo "KELDRA_IMAGE must name the clean QA image for Docker qualification" >&2
  exit 2
fi
if ! candidate_image_id="$(
  "${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}" 2>/dev/null
)"; then
  case "${KELDRA_DOCKER_PLATFORM:-}" in
    linux/arm64|linux/amd64) auto_build_platform="${KELDRA_DOCKER_PLATFORM}" ;;
    "")
      case "$(uname -m)" in
        arm64|aarch64) auto_build_platform=linux/arm64 ;;
        x86_64|amd64) auto_build_platform=linux/amd64 ;;
        *) echo "cannot auto-build an image for host architecture $(uname -m)" >&2; exit 2 ;;
      esac
      ;;
    *) echo "unsupported KELDRA_DOCKER_PLATFORM=${KELDRA_DOCKER_PLATFORM}" >&2; exit 2 ;;
  esac
  echo "candidate image ${requested_image} is absent; building ${auto_build_platform} through scripts/build-image.sh"
  (
    cd "${repo_root}"
    KELDRA_IMAGE="${requested_image}" KELDRA_DOCKER_PLATFORM="${auto_build_platform}" \
      "${repo_root}/scripts/build-image.sh"
  )
  candidate_image_id="$(
    "${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}"
  )"
fi
if [[ ! "${candidate_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "qualification image did not resolve to an immutable image ID" >&2
  exit 2
fi
candidate_revision="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "${candidate_image_id}")"
if [[ "${candidate_revision}" != "${source_commit}" ]]; then
  echo "candidate image revision ${candidate_revision} does not match harness source ${source_commit}" >&2
  exit 2
fi
platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${candidate_image_id}")"
declare -a comparison_roles=(candidate)
if [[ -n "${baseline_image}" ]]; then
  if ! baseline_image_id="$(
    "${repo_root}/scripts/resolve-docker-image-id.sh" "${baseline_image}" 2>/dev/null
  )"; then
    echo "baseline image ${baseline_image} is absent; released baselines are never auto-built" >&2
    exit 2
  fi
  baseline_revision="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "${baseline_image_id}")"
  baseline_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${baseline_image_id}")"
  if [[ ! "${baseline_image_id}" =~ ^sha256:[0-9a-f]{64}$ \
    || ! "${baseline_revision}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "baseline image requires an immutable ID and full source-revision label" >&2
    exit 2
  fi
  if [[ "${baseline_platform}" != "${platform}" ]]; then
    echo "baseline platform ${baseline_platform} differs from candidate ${platform}" >&2
    exit 2
  fi
  case "${comparison_order}" in
    baseline-first) comparison_roles=(baseline candidate) ;;
    candidate-first) comparison_roles=(candidate baseline) ;;
    *) echo "comparison order must be baseline-first or candidate-first" >&2; exit 2 ;;
  esac
fi

image_id_for_role() {
  case "$1" in
    candidate) printf '%s\n' "${candidate_image_id}" ;;
    baseline) printf '%s\n' "${baseline_image_id:?baseline role requires an image}" ;;
    *) echo "unknown comparison role $1" >&2; return 2 ;;
  esac
}

image_revision_for_role() {
  case "$1" in
    candidate) printf '%s\n' "${candidate_revision}" ;;
    baseline) printf '%s\n' "${baseline_revision:?baseline role requires a revision}" ;;
    *) echo "unknown comparison role $1" >&2; return 2 ;;
  esac
}

driver=""
remote_driver=""
remote_driver_uname=""
remote_driver_logical_cpus=0
remote_driver_memory_bytes=0
if [[ "${driver_backend}" == local ]]; then
  command -v cargo >/dev/null 2>&1 || { echo "cargo is required for a local driver" >&2; exit 2; }
  cargo_target_dir="$(
    cargo metadata --quiet --locked --no-deps --format-version 1 \
      --manifest-path "${repo_root}/Cargo.toml" \
      | jq -er '.target_directory | select(type == "string" and length > 0)'
  )"
  cargo build --quiet --release --locked --package keldra-server \
    --jobs "${CARGO_BUILD_JOBS:-1}" \
    --manifest-path "${repo_root}/Cargo.toml" \
    --example index_contention_qualification
  driver="${cargo_target_dir}/release/examples/index_contention_qualification"
  [[ -x "${driver}" ]] || { echo "Cargo did not produce ${driver}" >&2; exit 1; }
else
  command -v ssh >/dev/null 2>&1 || { echo "ssh is required for the macOS driver" >&2; exit 2; }
  [[ -r "${driver_identity_file}" ]] || { echo "macOS driver identity is not readable: ${driver_identity_file}" >&2; exit 2; }
  driver_ssh=(ssh -i "${driver_identity_file}" -o IdentitiesOnly=yes "${driver_host}")
  remote_driver_identity="$({
    printf 'set -Eeuo pipefail\n'
    printf 'repo=%q\nexpected=%q\njobs=%q\n' "${driver_repo_root}" "${source_commit}" "${CARGO_BUILD_JOBS:-1}"
    cat <<'REMOTE_BUILD'
cd "${repo}"
actual="$(git rev-parse --verify 'HEAD^{commit}')"
[[ "${actual}" == "${expected}" ]] || { echo "remote driver checkout revision ${actual} differs from ${expected}" >&2; exit 2; }
[[ -z "$(git status --porcelain=v1 --untracked-files=normal)" ]] || { echo "remote driver checkout must be clean" >&2; exit 2; }
ulimit -n 65536
cargo_target_dir="$(cargo metadata --quiet --locked --no-deps --format-version 1 --manifest-path "${repo}/Cargo.toml" | jq -er '.target_directory | select(type == "string" and length > 0)')"
cargo build --quiet --release --locked --package keldra-server --jobs "${jobs}" --manifest-path "${repo}/Cargo.toml" --example index_contention_qualification
driver="${cargo_target_dir}/release/examples/index_contention_qualification"
[[ -x "${driver}" ]] || { echo "remote Cargo did not produce ${driver}" >&2; exit 1; }
jq -cn --arg driver "${driver}" --arg uname "$(uname -a)" \
  --argjson logical_cpus "$(sysctl -n hw.logicalcpu)" \
  --argjson memory_bytes "$(sysctl -n hw.memsize)" \
  '{driver:$driver,uname:$uname,logical_cpus:$logical_cpus,memory_bytes:$memory_bytes}'
REMOTE_BUILD
  } | "${driver_ssh[@]}" /bin/bash -s)"
  remote_driver="$(jq -er .driver <<<"${remote_driver_identity}")"
  remote_driver_uname="$(jq -er .uname <<<"${remote_driver_identity}")"
  remote_driver_logical_cpus="$(jq -er .logical_cpus <<<"${remote_driver_identity}")"
  remote_driver_memory_bytes="$(jq -er .memory_bytes <<<"${remote_driver_identity}")"
  [[ "${remote_driver}" == /* ]] || { echo "remote driver path is invalid" >&2; exit 1; }
fi

utc_run_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_id="${utc_run_stamp}-${topology}-${source_commit:0:12}-$$"
evidence_root="${KELDRA_INDEX_CONTENTION_EVIDENCE_ROOT:-${repo_root}/../../releases/keldra/index-contention}"
run_dir="${evidence_root}/${run_id}"
remote_evidence_root="${KELDRA_INDEX_CONTENTION_DRIVER_EVIDENCE_ROOT:-${driver_repo_root}/../../releases/keldra/index-contention}"
remote_run_dir="${remote_evidence_root}/${run_id}"
mkdir -p "${evidence_root}"
mkdir "${run_dir}"
chmod 0755 "${run_dir}"
progress="${run_dir}/progress.jsonl"
status_file="${run_dir}/status.json"
: >"${progress}"
ln -sfn "${run_id}" "${evidence_root}/latest"

emit_event() {
  local event="$1"
  local cell="${2:-}"
  local definition_count="${3:-0}"
  local detail="${4:-}"
  local event_json status_tmp
  event_json="$(jq -cn \
    --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg run_id "${run_id}" --arg event "${event}" --arg cell "${cell}" \
    --arg comparison_role "${current_role:-}" \
    --argjson definition_count "${definition_count}" --arg detail "${detail}" \
    '{timestamp:$timestamp,run_id:$run_id,event:$event,cell:$cell,comparison_role:$comparison_role,index_definition_count:$definition_count,detail:$detail}')"
  printf '%s\n' "${event_json}" >>"${progress}"
  status_tmp="${status_file}.tmp.$$"
  printf '%s\n' "${event_json}" >"${status_tmp}"
  mv -f "${status_tmp}" "${status_file}"
}

host_memory_bytes=0
if [[ -r /proc/meminfo ]]; then
  host_memory_bytes="$(awk '$1 == "MemTotal:" {printf "%.0f", $2 * 1024}' /proc/meminfo)"
elif command -v sysctl >/dev/null 2>&1; then
  host_memory_bytes="$(sysctl -n hw.memsize 2>/dev/null || printf 0)"
fi
docker_cpus="$(docker info --format '{{.NCPU}}')"
docker_memory_bytes="$(docker info --format '{{.MemTotal}}')"
read -r filesystem_kib filesystem_available_kib < <(
  df -Pk "${run_dir}" | awk 'NR == 2 {print $2, $4}'
)
images_json="$(jq -cn \
  --arg candidate_name "${requested_image}" --arg candidate_id "${candidate_image_id}" \
  --arg candidate_revision "${candidate_revision}" --arg platform "${platform}" \
  --arg baseline_name "${baseline_image}" --arg baseline_id "${baseline_image_id:-}" \
  --arg baseline_revision "${baseline_revision:-}" \
  '{candidate:{requested:$candidate_name,id:$candidate_id,revision:$candidate_revision,platform:$platform}}
   + if $baseline_name == "" then {} else {baseline:{requested:$baseline_name,id:$baseline_id,revision:$baseline_revision,platform:$platform}} end')"
jq -n \
  --arg run_id "${run_id}" --arg source_commit "${source_commit}" \
  --argjson images "${images_json}" \
  --arg mode "${mode}" --arg topology "${driver_topology}" --arg durability "${durability}" --arg matrix "${matrix}" \
  --arg comparison_order "${comparison_order}" \
  --arg server_backend "${qualification_backend}" --arg driver_backend "${driver_backend}" \
  --arg driver_host "${driver_host}" --arg driver_repo_root "${driver_repo_root}" \
  --arg server_advertise_host "${server_advertise_host}" \
  --arg driver_uname "${remote_driver_uname}" \
  --argjson driver_logical_cpus "${remote_driver_logical_cpus}" \
  --argjson driver_memory_bytes "${remote_driver_memory_bytes}" \
  --arg server_rust_log "${server_rust_log}" \
  --arg max_concurrent_query_p99_ms "${max_concurrent_query_p99_ms}" \
  --arg max_publication_visibility_p99_ms "${max_publication_visibility_p99_ms}" \
  --argjson request_timeout_ms "${request_timeout_ms}" \
  --argjson drain_timeout_seconds "${drain_timeout_seconds}" \
  --argjson visibility_poll_ms "${visibility_poll_ms}" \
  --argjson visibility_observation_timeout_seconds "${visibility_observation_timeout_seconds}" \
  --argjson visibility_sample_every_batches "${visibility_sample_every_batches}" \
  --argjson mutation_workers "${mutation_workers}" \
  --argjson mutation_batch_size "${mutation_batch_size}" \
  --argjson mutation_record_bytes "${mutation_record_bytes}" \
  --argjson mutation_queue_depth "${mutation_queue_depth}" \
  --arg mutation_rate_operations_per_second "${mutation_rate_operations_per_second}" \
  --argjson index_disk_cache_bytes "${index_disk_cache_bytes}" \
  --argjson index_memory_percent "${index_memory_percent}" \
  --argjson index_kind_budget_bytes "${index_kind_budget_bytes}" \
  --argjson index_compaction_lanes "${index_compaction_lanes}" \
  --argjson index_projection_lanes "${index_projection_lanes}" \
  --argjson index_rayon_workers "${index_rayon_workers}" \
  --argjson source_journal_entries "${source_journal_entries}" \
  --arg uname "$(uname -a)" --argjson baseline_seconds "${baseline_seconds}" \
  --argjson concurrent_seconds "${concurrent_seconds}" --argjson post_seconds "${post_seconds}" \
  --argjson host_logical_cpus "$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf 0)" \
  --argjson host_memory_bytes "${host_memory_bytes:-0}" \
  --argjson docker_cpus "${docker_cpus}" --argjson docker_memory_bytes "${docker_memory_bytes}" \
  --argjson filesystem_kib "${filesystem_kib}" \
  --argjson filesystem_available_kib "${filesystem_available_kib}" \
  '{schema_version:1,run_id:$run_id,harness_source_commit:$source_commit,images:$images,execution:{server_backend:$server_backend,driver_backend:$driver_backend,driver_host:$driver_host,driver_repo_root:$driver_repo_root,server_advertise_host:$server_advertise_host},workload:{mode:$mode,topology:$topology,durability:$durability,comparison_order:$comparison_order,index_definition_count_matrix:($matrix|split(",")|map(tonumber)),baseline_seconds:$baseline_seconds,concurrent_seconds:$concurrent_seconds,post_seconds:$post_seconds,mutation_workers:$mutation_workers,mutation_batch_size:$mutation_batch_size,mutation_record_bytes:$mutation_record_bytes,mutation_queue_depth:$mutation_queue_depth,mutation_rate_operations_per_second:(if $mutation_rate_operations_per_second == "disabled" then null else ($mutation_rate_operations_per_second|tonumber) end),request_timeout_milliseconds:$request_timeout_ms,drain_timeout_seconds:$drain_timeout_seconds,visibility_poll_milliseconds:$visibility_poll_ms,visibility_observation_timeout_seconds:$visibility_observation_timeout_seconds,visibility_sample_every_batches:$visibility_sample_every_batches,max_concurrent_query_p99_milliseconds:(if $max_concurrent_query_p99_ms == "disabled" then null else ($max_concurrent_query_p99_ms|tonumber) end),max_publication_visibility_p99_milliseconds:(if $max_publication_visibility_p99_ms == "disabled" then null else ($max_publication_visibility_p99_ms|tonumber) end)},server:{rust_log:$server_rust_log,index_disk_cache_bytes:$index_disk_cache_bytes,index_memory_percent:$index_memory_percent,index_kind_budget_bytes:$index_kind_budget_bytes,index_compaction_max_lanes:$index_compaction_lanes,index_projection_max_lanes:$index_projection_lanes,index_rayon_workers:$index_rayon_workers,source_journal_max_entries:$source_journal_entries},hardware:{uname:$uname,host_logical_cpus:$host_logical_cpus,host_memory_bytes:$host_memory_bytes,docker_logical_cpus:$docker_cpus,docker_memory_bytes:$docker_memory_bytes,driver_uname:$driver_uname,driver_logical_cpus:$driver_logical_cpus,driver_memory_bytes:$driver_memory_bytes,evidence_filesystem_kib:$filesystem_kib,evidence_filesystem_available_kib:$filesystem_available_kib}}' \
  >"${run_dir}/run.json"

if [[ "${driver_backend}" == ssh-macos ]]; then
  {
    printf 'set -Eeuo pipefail\nremote_run=%q\nexpected=%q\n' "${remote_run_dir}" "${source_commit}"
    cat <<'REMOTE_EVIDENCE'
[[ -f "${remote_run}/run.json" ]] || { echo "shared evidence path is not visible on the driver host: ${remote_run}" >&2; exit 2; }
[[ "$(jq -er .harness_source_commit "${remote_run}/run.json")" == "${expected}" ]] || { echo "remote evidence source binding mismatch" >&2; exit 2; }
REMOTE_EVIDENCE
  } | "${driver_ssh[@]}" /bin/bash -s
fi

current_container=""
current_project=""
current_state=""
current_cell=""
current_builders=0
current_image_id="${candidate_image_id}"
current_role=""
sampler_pid=""
run_complete=0
stop_sampler() {
  if [[ -n "${sampler_pid}" ]] && kill -0 "${sampler_pid}" 2>/dev/null; then
    kill "${sampler_pid}" >/dev/null 2>&1 || true
    wait "${sampler_pid}" >/dev/null 2>&1 || true
  fi
  sampler_pid=""
}
cleanup_cell() {
  stop_sampler
  if [[ "${keep}" == 1 ]]; then
    echo "retained ${current_project:-${current_container}} state=${current_state}" >&2
    current_container=""
    current_project=""
    current_state=""
    return
  fi
  if [[ -n "${current_project}" ]]; then
    KELDRA_QUALIFICATION_PROJECT="${current_project}" \
    KELDRA_QUALIFICATION_DIR="${current_state}" \
    KELDRA_QUALIFICATION_START_NODE="${start_node}" \
    KELDRA_IMAGE="${current_image_id}" \
      docker compose --project-name "${current_project}" --file "${compose_file}" \
        down --volumes --remove-orphans >/dev/null 2>&1 || true
  elif [[ -n "${current_container}" ]]; then
    docker rm --force "${current_container}" >/dev/null 2>&1 || true
  fi
  current_container=""
  current_project=""
  if [[ "${keep}" != 1 && -n "${current_state}" && "${current_state}" == /var/tmp/keldra-index-contention.* ]]; then
    docker run --rm --user 0 --volume "${current_state}:/state" "${current_image_id}" \
      rm -rf /state/node-1 /state/node-2 /state/node-3 /state/artifacts \
        /state/data /state/token-signing-key >/dev/null 2>&1 || true
    rm -rf -- "${current_state}"
  fi
  current_state=""
}
cleanup() {
  local exit_status=$?
  trap - EXIT INT TERM
  if ((run_complete == 0)); then
    emit_event failed "${current_cell}" "${current_builders}" "qualification interrupted or failed (exit ${exit_status})" || true
    if [[ -n "${current_container}" ]]; then
      docker logs "${current_container}" >"${run_dir}/failure-server.log" 2>&1 || true
    elif [[ -n "${current_project}" ]]; then
      KELDRA_QUALIFICATION_PROJECT="${current_project}" KELDRA_QUALIFICATION_DIR="${current_state}" \
      KELDRA_QUALIFICATION_START_NODE="${start_node}" KELDRA_IMAGE="${current_image_id}" \
        docker compose --project-name "${current_project}" --file "${compose_file}" \
          logs --no-color >"${run_dir}/failure-server.log" 2>&1 || true
    fi
  fi
  cleanup_cell
  exit "${exit_status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

wait_for_file() {
  local container="$1" path="$2" attempt
  attempt=1
  while ((attempt <= 90)); do
    docker exec "${container}" test -f "${path}" >/dev/null 2>&1 && return 0
    docker inspect --format '{{.State.Running}}' "${container}" 2>/dev/null | grep -Fxq true || return 1
    sleep 1
    attempt=$((attempt + 1))
  done
  return 1
}

start_resource_sampler() {
  local output="$1"
  shift
  local -a containers=("$@")
  (
    while true; do
      timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      docker stats --no-stream --format '{{json .}}' "${containers[@]}" 2>/dev/null \
        | jq -c --arg timestamp "${timestamp}" '. + {sampled_at:$timestamp}' \
        >>"${output}" || true
      sleep 1
    done
  ) &
  sampler_pid=$!
}

run_qualification_driver() {
  local output_path="$1" progress_path="$2"
  if [[ "${driver_backend}" == local ]]; then
    KELDRA_INDEX_CONTENTION_ENDPOINTS="${endpoint_csv}" \
    KELDRA_INDEX_CONTENTION_TENANT="${tenant}" \
    KELDRA_INDEX_CONTENTION_BUCKET="${bucket}" \
    KELDRA_INDEX_CONTENTION_CLIENT_ID="${client_id}" \
    KELDRA_INDEX_CONTENTION_CLIENT_SECRET="${client_secret}" \
    KELDRA_INDEX_CONTENTION_SERVER_SOURCE_COMMIT="${current_image_revision}" \
    KELDRA_INDEX_CONTENTION_IMAGE="${current_image_id}" \
    KELDRA_INDEX_CONTENTION_TOPOLOGY="${driver_topology}" \
    KELDRA_INDEX_CONTENTION_DURABILITY="${durability}" \
    KELDRA_INDEX_CONTENTION_DEFINITION_COUNT="${builders}" \
    KELDRA_INDEX_CONTENTION_BASELINE_SECONDS="${baseline_seconds}" \
    KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS="${concurrent_seconds}" \
    KELDRA_INDEX_CONTENTION_POST_SECONDS="${post_seconds}" \
    KELDRA_INDEX_CONTENTION_REQUEST_TIMEOUT_MILLISECONDS="${request_timeout_ms}" \
    KELDRA_INDEX_CONTENTION_DRAIN_TIMEOUT_SECONDS="${drain_timeout_seconds}" \
    KELDRA_INDEX_CONTENTION_VISIBILITY_POLL_MILLISECONDS="${visibility_poll_ms}" \
    KELDRA_INDEX_CONTENTION_VISIBILITY_OBSERVATION_TIMEOUT_SECONDS="${visibility_observation_timeout_seconds}" \
    KELDRA_INDEX_CONTENTION_VISIBILITY_SAMPLE_EVERY_BATCHES="${visibility_sample_every_batches}" \
    KELDRA_INDEX_CONTENTION_MUTATION_WORKERS="${mutation_workers}" \
    KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE="${mutation_batch_size}" \
    KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES="${mutation_record_bytes}" \
    KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH="${mutation_queue_depth}" \
    KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND="${mutation_rate_operations_per_second}" \
    KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS="${max_concurrent_query_p99_ms}" \
    KELDRA_INDEX_CONTENTION_MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS="${max_publication_visibility_p99_ms}" \
    KELDRA_INDEX_CONTENTION_OUTPUT="${output_path}" \
    KELDRA_INDEX_CONTENTION_PROGRESS_JSONL="${progress_path}" \
      "${driver}"
    return
  fi

  {
    printf 'set -Eeuo pipefail\n'
    printf 'cd %q\n' "${driver_repo_root}"
    printf 'export KELDRA_INDEX_CONTENTION_ENDPOINTS=%q\n' "${endpoint_csv}"
    printf 'export KELDRA_INDEX_CONTENTION_TENANT=%q\n' "${tenant}"
    printf 'export KELDRA_INDEX_CONTENTION_BUCKET=%q\n' "${bucket}"
    printf 'export KELDRA_INDEX_CONTENTION_CLIENT_ID=%q\n' "${client_id}"
    printf 'export KELDRA_INDEX_CONTENTION_CLIENT_SECRET=%q\n' "${client_secret}"
    printf 'export KELDRA_INDEX_CONTENTION_SERVER_SOURCE_COMMIT=%q\n' "${current_image_revision}"
    printf 'export KELDRA_INDEX_CONTENTION_IMAGE=%q\n' "${current_image_id}"
    printf 'export KELDRA_INDEX_CONTENTION_TOPOLOGY=%q\n' "${driver_topology}"
    printf 'export KELDRA_INDEX_CONTENTION_DURABILITY=%q\n' "${durability}"
    printf 'export KELDRA_INDEX_CONTENTION_DEFINITION_COUNT=%q\n' "${builders}"
    printf 'export KELDRA_INDEX_CONTENTION_BASELINE_SECONDS=%q\n' "${baseline_seconds}"
    printf 'export KELDRA_INDEX_CONTENTION_CONCURRENT_SECONDS=%q\n' "${concurrent_seconds}"
    printf 'export KELDRA_INDEX_CONTENTION_POST_SECONDS=%q\n' "${post_seconds}"
    printf 'export KELDRA_INDEX_CONTENTION_REQUEST_TIMEOUT_MILLISECONDS=%q\n' "${request_timeout_ms}"
    printf 'export KELDRA_INDEX_CONTENTION_DRAIN_TIMEOUT_SECONDS=%q\n' "${drain_timeout_seconds}"
    printf 'export KELDRA_INDEX_CONTENTION_VISIBILITY_POLL_MILLISECONDS=%q\n' "${visibility_poll_ms}"
    printf 'export KELDRA_INDEX_CONTENTION_VISIBILITY_OBSERVATION_TIMEOUT_SECONDS=%q\n' "${visibility_observation_timeout_seconds}"
    printf 'export KELDRA_INDEX_CONTENTION_VISIBILITY_SAMPLE_EVERY_BATCHES=%q\n' "${visibility_sample_every_batches}"
    printf 'export KELDRA_INDEX_CONTENTION_MUTATION_WORKERS=%q\n' "${mutation_workers}"
    printf 'export KELDRA_INDEX_CONTENTION_MUTATION_BATCH_SIZE=%q\n' "${mutation_batch_size}"
    printf 'export KELDRA_INDEX_CONTENTION_MUTATION_RECORD_BYTES=%q\n' "${mutation_record_bytes}"
    printf 'export KELDRA_INDEX_CONTENTION_MUTATION_QUEUE_DEPTH=%q\n' "${mutation_queue_depth}"
    printf 'export KELDRA_INDEX_CONTENTION_MUTATION_RATE_OPERATIONS_PER_SECOND=%q\n' "${mutation_rate_operations_per_second}"
    printf 'export KELDRA_INDEX_CONTENTION_MAX_CONCURRENT_QUERY_P99_MILLISECONDS=%q\n' "${max_concurrent_query_p99_ms}"
    printf 'export KELDRA_INDEX_CONTENTION_MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS=%q\n' "${max_publication_visibility_p99_ms}"
    printf 'export KELDRA_INDEX_CONTENTION_OUTPUT=%q\n' "${remote_run_dir}/${cell}/report.json"
    printf 'export KELDRA_INDEX_CONTENTION_PROGRESS_JSONL=%q\n' "${remote_run_dir}/${cell}/driver-progress.jsonl"
    printf 'driver=%q\n' "${remote_driver}"
    cat <<'REMOTE_DRIVER'
"${driver}" &
driver_pid=$!
stop_driver() {
  kill -TERM "${driver_pid}" >/dev/null 2>&1 || true
  wait "${driver_pid}" >/dev/null 2>&1 || true
  exit 130
}
trap stop_driver HUP INT TERM
set +e
wait "${driver_pid}"
driver_status=$?
set -e
trap - HUP INT TERM
exit "${driver_status}"
REMOTE_DRIVER
  } | "${driver_ssh[@]}" /bin/bash -s
}

emit_event run_started "" 0 "evidence=${run_dir}"
for current_role in "${comparison_roles[@]}"; do
current_image_id="$(image_id_for_role "${current_role}")"
current_image_revision="$(image_revision_for_role "${current_role}")"
for builders in "${builder_matrix[@]}"; do
  cell="${current_role}-definitions-${builders}"
  cell_dir="${run_dir}/${cell}"
  mkdir "${cell_dir}"
  current_cell="${cell}"
  current_builders="${builders}"
  current_state="$(mktemp -d /var/tmp/keldra-index-contention.XXXXXX)"
  mkdir "${current_state}/artifacts"
  chmod 0777 "${current_state}/artifacts"
  dd if=/dev/urandom of="${current_state}/token-signing-key" \
    bs=64 count=1 2>/dev/null
  chmod 0600 "${current_state}/token-signing-key"
  tenant="qcontention-${current_role:0:1}-${source_commit:0:8}-${builders}-${$}"
  bucket="objects-${builders}"
  client_id="contention-client-${builders}"
  client_secret="contention-secret-${run_id}-${builders}-0000000000000000"
  emit_event cell_start "${cell}" "${builders}" "creating fresh ${topology}-node state"

  endpoints=()
  resource_containers=()
  if [[ "${topology}" == single ]]; then
    mkdir "${current_state}/data"
    docker run --rm --user 0 --volume "${current_state}:/state" "${current_image_id}" \
      chown -R 10001:10001 /state/data /state/token-signing-key
    current_container="keldra-contention-${run_id//[^a-zA-Z0-9_.-]/-}-${current_role}-${builders}"
    docker run --detach --name "${current_container}" --platform "${platform}" \
      --publish "${server_advertise_host}:0:50051" \
      --env "RUST_LOG=${server_rust_log}" \
      --env KELDRA_LISTEN=0.0.0.0:50051 --env KELDRA_PEER_LISTEN=127.0.0.1:50052 \
      --env KELDRA_DATA_DIR=/var/lib/keldra --env KELDRA_NODE_ID=1 \
      --env KELDRA_TOKEN_SIGNING_KEY_FILE=/run/secrets/keldra-token-signing-key \
      --env KELDRA_RUN_SYSTEM_BOOTSTRAP=true \
      --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=1000000 \
      --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=100000 \
      --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=1000000 \
      --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100000 \
      --env "KELDRA_INDEX_DISK_CACHE_BYTES=${index_disk_cache_bytes}" \
      --env "KELDRA_INDEX_MEMORY_PERCENT=${index_memory_percent}" \
      --env "KELDRA_INDEX_BUILDER_MEMORY_BYTES_PER_KIND=${index_kind_budget_bytes}" \
      --env "KELDRA_INDEX_TYPED_JSON_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
      --env "KELDRA_INDEX_TYPED_JSON_COMPACTION_MAX_LANES=${index_compaction_lanes}" \
      --env "KELDRA_INDEX_TYPED_JSON_PROJECTION_MAX_LANES=${index_projection_lanes}" \
      --env "KELDRA_INDEX_RAYON_WORKERS=${index_rayon_workers}" \
      --env "KELDRA_SOURCE_JOURNAL_MAX_ENTRIES=${source_journal_entries}" \
      --volume "${current_state}/data:/var/lib/keldra" \
      --volume "${current_state}/token-signing-key:/run/secrets/keldra-token-signing-key:ro" \
      "${current_image_id}" >/dev/null
    wait_for_file "${current_container}" /var/lib/keldra/system-bootstrap-credential.json || {
      echo "single node did not bootstrap" >&2; exit 1;
    }
    provisioned=0
    attempt=1
    while ((attempt <= 90)); do
      if KELDRA_NEW_CLIENT_SECRET="${client_secret}" \
        docker exec --env KELDRA_NEW_CLIENT_SECRET "${current_container}" \
          keldra --endpoint http://127.0.0.1:50051 \
          --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
          provision-tenant "${tenant}" contention-owner "${client_id}" \
          >/dev/null 2>&1
      then
        provisioned=1
        break
      fi
      sleep 1
      attempt=$((attempt + 1))
    done
    if ((provisioned == 0)); then
      echo "single node did not accept tenant provisioning" >&2
      exit 1
    fi
    published="$(docker port "${current_container}" 50051/tcp)"
    case "${published}" in
      "${server_advertise_host}":[1-9][0-9]*) ;;
      *) echo "invalid public endpoint ${published}" >&2; exit 1 ;;
    esac
    published_port="${published##*:}"
    [[ "${published_port}" =~ ^[1-9][0-9]*$ ]] || { echo "invalid public port ${published_port}" >&2; exit 1; }
    endpoints+=("http://${server_advertise_host}:${published_port}")
    resource_containers+=("${current_container}")
  else
    current_project="keldra-contention-${run_id//[^a-zA-Z0-9_.-]/-}-${current_role}-${builders}"
    for directory in node-1 node-2 node-3; do mkdir "${current_state}/${directory}"; chmod 0777 "${current_state}/${directory}"; done
    chmod 0755 "${current_state}"
    docker run --rm --user 0 --volume "${current_state}/token-signing-key:/key" "${current_image_id}" chown 10001:10001 /key
    export KELDRA_QUALIFICATION_PROJECT="${current_project}" KELDRA_QUALIFICATION_DIR="${current_state}"
    export KELDRA_QUALIFICATION_START_NODE="${start_node}" KELDRA_IMAGE="${current_image_id}" KELDRA_DOCKER_PLATFORM="${platform}"
    export KELDRA_QUALIFICATION_RUST_LOG="${server_rust_log}"
    export KELDRA_QUALIFICATION_INDEX_DISK_CACHE_BYTES="${index_disk_cache_bytes}"
    export KELDRA_QUALIFICATION_INDEX_MEMORY_PERCENT="${index_memory_percent}"
    export KELDRA_QUALIFICATION_INDEX_KIND_BUDGET_BYTES="${index_kind_budget_bytes}"
    export KELDRA_QUALIFICATION_INDEX_COMPACTION_MAX_LANES="${index_compaction_lanes}"
    export KELDRA_QUALIFICATION_INDEX_PROJECTION_MAX_LANES="${index_projection_lanes}"
    export KELDRA_QUALIFICATION_INDEX_RAYON_WORKERS="${index_rayon_workers}"
    export KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES="${source_journal_entries}"
    compose=(docker compose --project-name "${current_project}" --file "${compose_file}")
    "${compose[@]}" config --quiet
    "${compose[@]}" up --detach keldra-1
    node_one="$("${compose[@]}" ps --quiet keldra-1)"
    wait_for_file "${node_one}" /var/lib/keldra/system-bootstrap-credential.json || { echo "node 1 did not bootstrap" >&2; exit 1; }
    network="${current_project}_default"
    run_bootstrap() {
      docker run --rm --network "${network}" --volume "${current_state}:/qualification" \
        --env KELDRA_NEW_CLIENT_SECRET "${current_image_id}" \
        keldra --endpoint http://keldra-1:50051 \
        --credentials-file /qualification/node-1/system-bootstrap-credential.json "$@"
    }
    provisioned=0
    attempt=1
    while ((attempt <= 90)); do
      if KELDRA_NEW_CLIENT_SECRET="${client_secret}" run_bootstrap \
        provision-tenant "${tenant}" contention-owner "${client_id}" \
        >/dev/null 2>&1
      then
        provisioned=1
        break
      fi
      sleep 1
      attempt=$((attempt + 1))
    done
    if ((provisioned == 0)); then
      echo "node 1 did not accept tenant provisioning" >&2
      exit 1
    fi
    run_client() {
      local node="$1"
      shift
      docker run --rm --network "${network}" \
        --env "KELDRA_CLIENT_ID=${client_id}" \
        --env "KELDRA_CLIENT_SECRET=${client_secret}" \
        "${current_image_id}" keldra --endpoint "http://${node}:50051" "$@"
    }
    run_client keldra-1 create-bucket readiness >/dev/null
    for node_id in 2 3; do
      output="$(run_bootstrap prepare-node "${node_id}" "keldra-${node_id}:50052")"
      bundle="$(sed -n 's/^bundle=\([^ ]*\) .*/\1/p' <<<"${output}")"
      [[ "${bundle}" == "/var/lib/keldra/keldra-node-${node_id}.join.json" ]] || { echo "node ${node_id} returned invalid join bundle" >&2; exit 1; }
      "${compose[@]}" cp "keldra-1:${bundle}" "${current_state}/artifacts/keldra-node-${node_id}.join.json"
      chmod 0600 "${current_state}/artifacts/keldra-node-${node_id}.join.json"
      docker run --rm --user 0 --volume "${current_state}/artifacts/keldra-node-${node_id}.join.json:/bundle" "${current_image_id}" chown 10001:10001 /bundle
      "${compose[@]}" up --detach "keldra-${node_id}"
      ready=0
      attempt=1
      while ((attempt <= 90)); do
        if run_client "keldra-${node_id}" list "${tenant}" readiness --limit 1 \
          >/dev/null 2>&1
        then
          ready=1
          break
        fi
        sleep 1
        attempt=$((attempt + 1))
      done
      if ((ready == 0)); then
        echo "keldra-${node_id} did not become authenticated and ACTIVE" >&2
        exit 1
      fi
    done
    for node in keldra-1 keldra-2 keldra-3; do
      published="$("${compose[@]}" port "${node}" 50051)"
      [[ "${published}" =~ ^127\.0\.0\.1:[1-9][0-9]*$ ]] || { echo "${node} returned invalid endpoint ${published}" >&2; exit 1; }
      endpoints+=("http://${published}")
      resource_containers+=("$("${compose[@]}" ps --quiet "${node}")")
    done
  fi
  endpoint_csv="$(IFS=,; echo "${endpoints[*]}")"
  emit_event topology_ready "${cell}" "${builders}" "endpoints=${#endpoints[@]}"
  start_resource_sampler "${cell_dir}/container-resources.jsonl" "${resource_containers[@]}"
  ln -sfn "${cell}/driver-progress.jsonl" "${run_dir}/active-driver-progress.jsonl"
  emit_event workload_started "${cell}" "${builders}" "progress=${cell_dir}/driver-progress.jsonl"
  set +e
  run_qualification_driver "${cell_dir}/report.json" "${cell_dir}/driver-progress.jsonl" \
    >"${cell_dir}/driver.stdout.log" 2>"${cell_dir}/driver.stderr.log"
  driver_status=$?
  set -e
  stop_sampler
  if [[ "${topology}" == single ]]; then
    docker logs "${current_container}" >"${cell_dir}/server.log" 2>&1 || true
  else
    "${compose[@]}" logs --no-color >"${cell_dir}/server.log" 2>&1 || true
  fi
  report_gate='.result == "pass"'
  if [[ "${current_role}" == baseline ]]; then
    report_gate='.correctness.passed == true and .workload_validity.passed == true'
  fi
  if [[ ! -s "${cell_dir}/report.json" ]] \
    || ! jq -e "${report_gate}" "${cell_dir}/report.json" >/dev/null \
    || [[ ! -s "${cell_dir}/container-resources.jsonl" ]] \
    || ! jq -e . "${cell_dir}/container-resources.jsonl" >/dev/null; then
    emit_event cell_failed "${cell}" "${builders}" "driver_exit=${driver_status}"
    echo "contention cell ${cell} failed (driver exit ${driver_status}); evidence retained in ${cell_dir}" >&2
    exit 1
  fi
  if [[ "${current_role}" == baseline && "${driver_status}" != 0 ]]; then
    emit_event baseline_performance_failed "${cell}" "${builders}" \
      "correctness and workload valid; responsiveness failure retained for comparison"
  else
    emit_event cell_passed "${cell}" "${builders}" "report=${cell_dir}/report.json"
  fi
  cleanup_cell
done
done
if [[ -n "${baseline_image}" ]]; then
  comparison_rows="${run_dir}/comparison-rows.jsonl"
  : >"${comparison_rows}"
  for builders in "${builder_matrix[@]}"; do
    jq -cn \
      --argjson definition_count "${builders}" \
      --slurpfile before "${run_dir}/baseline-definitions-${builders}/report.json" \
      --slurpfile after "${run_dir}/candidate-definitions-${builders}/report.json" '
      def latency($report; $path):
        ($report | getpath($path)) | {samples,p50_ms,p95_ms,p99_ms,max_ms};
      def delta($after; $before):
        if $after.samples > 0 and $before.samples > 0 then
          {p50_ms:($after.p50_ms-$before.p50_ms),
           p95_ms:($after.p95_ms-$before.p95_ms),
           p99_ms:($after.p99_ms-$before.p99_ms),
           max_ms:($after.max_ms-$before.max_ms)}
        else null end;
      (latency($before[0];["concurrent","schedule_to_response"])) as $before_query |
      (latency($after[0];["concurrent","schedule_to_response"])) as $after_query |
      (latency($before[0];["concurrent","dispatch_to_response"])) as $before_service |
      (latency($after[0];["concurrent","dispatch_to_response"])) as $after_service |
      (latency($before[0];["mutations","publication_visibility_lag"])) as $before_visible |
      (latency($after[0];["mutations","publication_visibility_lag"])) as $after_visible |
      {index_definition_count:$definition_count,
       outcomes:{baseline:{result:$before[0].result,responsiveness:$before[0].responsiveness,concurrent:{offered:$before[0].concurrent.offered_schedules,completed:$before[0].concurrent.completed,dropped:$before[0].concurrent.dropped_schedules,request_errors:$before[0].concurrent.request_errors,timeouts:$before[0].concurrent.timeouts}},candidate:{result:$after[0].result,responsiveness:$after[0].responsiveness,concurrent:{offered:$after[0].concurrent.offered_schedules,completed:$after[0].concurrent.completed,dropped:$after[0].concurrent.dropped_schedules,request_errors:$after[0].concurrent.request_errors,timeouts:$after[0].concurrent.timeouts}}},
       query_concurrent:{baseline:$before_query,candidate:$after_query,delta_candidate_minus_baseline:(delta($after_query;$before_query))},
       query_service_concurrent:{baseline:$before_service,candidate:$after_service,delta_candidate_minus_baseline:(delta($after_service;$before_service))},
       publication_visibility_lag:{definition:"mutation acceptance to first ordinary-query observation of exact version",baseline:$before_visible,candidate:$after_visible,delta_candidate_minus_baseline:(delta($after_visible;$before_visible))}}
    ' >>"${comparison_rows}"
  done
  jq -s '{schema:"keldra.index-contention-comparison.v1",cells:.}' \
    "${comparison_rows}" >"${run_dir}/comparison.json"
  rm -f -- "${comparison_rows}"
fi
run_complete=1
if [[ -n "${baseline_image}" ]]; then
  emit_event run_qualified "" 0 \
    "candidate matrix passed; baseline correctness/workload valid and responsiveness retained as comparison evidence"
else
  emit_event run_passed "" 0 "candidate matrix passed"
fi
echo "index contention qualification passed; evidence=${run_dir}"
