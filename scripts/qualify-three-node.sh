#!/usr/bin/env bash
set -Eeuo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/qualification-log-evidence.sh"
source "${repo_root}/scripts/qualification-scale-evidence.sh"
source "${repo_root}/scripts/qualification-three-node-phases.sh"
compose_file="${repo_root}/tests/cluster/docker-compose.yml"
start_node="${repo_root}/tests/cluster/start-node.sh"
requested_image="${KELDRA_IMAGE:-keldra:0.11.0}"
qualification_mode="${KELDRA_QUALIFICATION_MODE:-smoke}"
index_disk_cache_bytes="${KELDRA_QUALIFICATION_INDEX_DISK_CACHE_BYTES:-1073741824}"
index_memory_percent="${KELDRA_QUALIFICATION_INDEX_MEMORY_PERCENT:-20}"
index_kind_budget_bytes="${KELDRA_QUALIFICATION_INDEX_KIND_BUDGET_BYTES:-268435456}"
index_compaction_max_lanes="${KELDRA_QUALIFICATION_INDEX_COMPACTION_MAX_LANES:-4}"
index_rayon_workers="${KELDRA_QUALIFICATION_INDEX_RAYON_WORKERS:-4}"
index_projection_max_lanes="${KELDRA_QUALIFICATION_INDEX_PROJECTION_MAX_LANES:-${index_rayon_workers}}"
# The default is a fast smoke. Set this to 839980 for the full
# production-shaped, twelve-field corpus used by the resource qualification.
case "${qualification_mode}" in
  release)
    index_resource_records="${KELDRA_QUALIFICATION_INDEX_RECORDS:-839980}"
    require_performance_targets=1
    ;;
  smoke)
    index_resource_records="${KELDRA_QUALIFICATION_INDEX_RECORDS:-16384}"
    require_performance_targets=0
    ;;
  *)
    echo "KELDRA_QUALIFICATION_MODE must be release or smoke" >&2
    exit 2
    ;;
esac
index_resource_mutations="${KELDRA_QUALIFICATION_INDEX_MUTATIONS:-512}"
index_resource_max_anonymous_growth_bytes="${KELDRA_QUALIFICATION_INDEX_MAX_ANONYMOUS_GROWTH_BYTES:-2147483648}"
release_source_journal_max_entries="${KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES:-1000000}"
pressure_source_journal_max_entries="${KELDRA_QUALIFICATION_PRESSURE_SOURCE_JOURNAL_MAX_ENTRIES:-64}"
index_kinds=(Path MetadataFilter TypedJson FullText Vector Hybrid GitSource Tensor)
for configured_limit in \
  "${index_disk_cache_bytes}" \
  "${index_memory_percent}" \
  "${index_kind_budget_bytes}" \
  "${index_compaction_max_lanes}" \
  "${index_rayon_workers}" \
  "${index_projection_max_lanes}" \
  "${index_resource_records}" \
  "${index_resource_mutations}" \
  "${index_resource_max_anonymous_growth_bytes}" \
  "${release_source_journal_max_entries}" \
  "${pressure_source_journal_max_entries}"
do
  if [[ ! "${configured_limit}" =~ ^[1-9][0-9]*$ ]]; then
    echo "index qualification limits must be positive decimal integers" >&2
    exit 2
  fi
done
if ((index_memory_percent > 100)); then
  echo "KELDRA_QUALIFICATION_INDEX_MEMORY_PERCENT must not exceed 100" >&2
  exit 2
fi
case "${index_resource_records}" in
  839980) index_resource_scope=release-corpus ;;
  16384) index_resource_scope=smoke ;;
  *) index_resource_scope=custom ;;
esac
if [[ "${qualification_mode}" == "release" \
  && "${index_resource_scope}" != "release-corpus" ]]; then
  echo "release qualification requires exactly 839980 resource records" >&2
  exit 2
fi
if [[ "${qualification_mode}" == "release" ]] \
  && ((release_source_journal_max_entries < 1000000)); then
  echo "release qualification requires the production source-journal entry bound of at least 1000000" >&2
  exit 2
fi
if ((pressure_source_journal_max_entries >= release_source_journal_max_entries)); then
  echo "journal-pressure entry bound must be smaller than the release entry bound" >&2
  exit 2
fi
export KELDRA_QUALIFICATION_INDEX_DISK_CACHE_BYTES="${index_disk_cache_bytes}"
export KELDRA_QUALIFICATION_INDEX_MEMORY_PERCENT="${index_memory_percent}"
export KELDRA_QUALIFICATION_INDEX_KIND_BUDGET_BYTES="${index_kind_budget_bytes}"
export KELDRA_QUALIFICATION_INDEX_COMPACTION_MAX_LANES="${index_compaction_max_lanes}"
export KELDRA_QUALIFICATION_INDEX_RAYON_WORKERS="${index_rayon_workers}"
export KELDRA_QUALIFICATION_INDEX_PROJECTION_MAX_LANES="${index_projection_max_lanes}"
export KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES="${release_source_journal_max_entries}"
export KELDRA_QUALIFICATION_RUST_LOG=info,keldra::index_runtime::cpu=warn,keldra::index_runtime::retention=debug,keldra::observability::runtime=debug
case "${KELDRA_DOCKER_PLATFORM:-}" in
  "")
    case "$(uname -m)" in
      x86_64|amd64) export KELDRA_DOCKER_PLATFORM=linux/amd64 ;;
      aarch64|arm64) export KELDRA_DOCKER_PLATFORM=linux/arm64 ;;
      *)
        echo "unsupported host architecture: $(uname -m)" >&2
        exit 2
        ;;
    esac
    ;;
  linux/amd64|linux/arm64) ;;
  *)
    echo "unsupported KELDRA_DOCKER_PLATFORM=${KELDRA_DOCKER_PLATFORM}" >&2
    exit 2
    ;;
esac
command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 2
}
command -v cargo >/dev/null 2>&1 || {
  echo "cargo is required for the test-only index qualification client" >&2
  exit 2
}
command -v jq >/dev/null 2>&1 || {
  echo "jq is required to resolve Cargo's configured target directory" >&2
  exit 2
}
command -v git >/dev/null 2>&1 || {
  echo "git is required for the smart HTTP gateway qualification" >&2
  exit 2
}
docker compose version >/dev/null
source_commit="$(git -C "${repo_root}" rev-parse --verify 'HEAD^{commit}')"
if [[ ! "${source_commit}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "qualification could not derive the exact source commit" >&2
  exit 2
fi
assert_source_tree_exact() {
  if [[ "$(git -C "${repo_root}" rev-parse --verify 'HEAD^{commit}')" != "${source_commit}" \
    || -n "$(git -C "${repo_root}" status --porcelain=v1 --untracked-files=normal)" ]]
  then
    echo "qualification requires an unchanged clean source tree so the source commit is exact" >&2
    return 1
  fi
}
assert_source_tree_exact
native_architecture="$(uname -m)"
hardware_logical_cpus="$(getconf _NPROCESSORS_ONLN)"
hardware_memory_bytes="$({
  awk '$1 == "MemTotal:" { printf "%.0f\n", $2 * 1024; found = 1 }
       END { if (!found) exit 1 }' /proc/meminfo
})"
read -r qualification_filesystem_total_bytes qualification_filesystem_available_bytes \
  < <(df -B1 --output=size,avail /var/tmp | awk 'NR == 2 { print $1, $2 }')
if [[ ! "${hardware_logical_cpus}" =~ ^[1-9][0-9]*$ \
  || ! "${hardware_memory_bytes}" =~ ^[1-9][0-9]*$ \
  || ! "${qualification_filesystem_total_bytes}" =~ ^[1-9][0-9]*$ \
  || ! "${qualification_filesystem_available_bytes}" =~ ^[1-9][0-9]*$ ]]; then
  echo "qualification could not derive the bounded host hardware summary" >&2
  exit 2
fi
qualification_examples=(
  accounting_qualification
  atomic_index_qualification
  cluster_index_qualification
  index_recovery_qualification
  personaldb_qualification
  s3_qualification
  v06_index_resource_qualification
)
qualification_example_flags=()
for qualification_example in "${qualification_examples[@]}"; do
  qualification_example_flags+=(--example "${qualification_example}")
done
cargo_target_dir="$(
  cargo metadata --quiet --locked --no-deps --format-version 1 \
    --manifest-path "${repo_root}/Cargo.toml" \
    | jq --exit-status --raw-output \
      '.target_directory | select(type == "string" and length > 0)'
)"
cargo build --quiet --release --locked --package keldra-server \
  --jobs "${CARGO_BUILD_JOBS:-1}" \
  --manifest-path "${repo_root}/Cargo.toml" \
  "${qualification_example_flags[@]}"
declare -A qualification_binaries=()
for qualification_example in "${qualification_examples[@]}"; do
  qualification_binaries["${qualification_example}"]="${cargo_target_dir}/release/examples/${qualification_example}"
  if [[ ! -x "${qualification_binaries[${qualification_example}]}" ]]; then
    echo "Cargo did not produce executable ${qualification_binaries[${qualification_example}]}" >&2
    exit 1
  fi
done
echo "[keldra-qualification] optimized qualification clients are ready; Cargo is no longer needed"
image_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}")"
if [[ ! "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "qualification image did not resolve to an immutable sha256 digest" >&2
  exit 2
fi
container_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${image_id}")"
if [[ "${container_platform}" != "${KELDRA_DOCKER_PLATFORM}" ]]; then
  echo "qualification image platform ${container_platform} does not match ${KELDRA_DOCKER_PLATFORM}" >&2
  exit 2
fi
image_revision="$(
  docker image inspect \
    --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' \
    "${image_id}"
)"
if [[ "${image_revision}" != "${source_commit}" ]]; then
  echo "qualification image revision ${image_revision} does not match source commit ${source_commit}" >&2
  exit 2
fi
server_version="$(
  docker run --rm --platform "${KELDRA_DOCKER_PLATFORM}" \
    "${image_id}" keldra-server --version
)"
client_version="$(
  docker run --rm --platform "${KELDRA_DOCKER_PLATFORM}" \
    "${image_id}" keldra --version
)"
if [[ "${server_version}" != "keldra-server 0.11.0" \
  || "${client_version}" != "keldra 0.11.0" ]]; then
  echo "qualification requires the exact Keldra 0.11.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
export KELDRA_IMAGE="${image_id}"
export KELDRA_QUALIFICATION_PROJECT="${KELDRA_QUALIFICATION_PROJECT:-keldra-v090-${$}}"
export KELDRA_QUALIFICATION_DIR="$(mktemp -d /var/tmp/keldra-v090-qualification.XXXXXX)"
export KELDRA_QUALIFICATION_START_NODE="${start_node}"
qualification_suffix="${KELDRA_QUALIFICATION_DIR##*.}"
index_verification_state="${KELDRA_QUALIFICATION_DIR}/artifacts/index-verification-state.json"
index_membership_state="${KELDRA_QUALIFICATION_DIR}/artifacts/index-membership-state.json"
index_pressure_state="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-state.json"
index_pressure_release="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-release"
index_pressure_writer_output="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-writer.log"
index_resource_state="${KELDRA_QUALIFICATION_DIR}/artifacts/index-resource-state.json"
index_resource_bucket="index-resource-${$}"
index_resource_report="/var/tmp/keldra-v090-three-index-resource-${qualification_suffix}.json"
index_resource_telemetry_prefix="/var/tmp/keldra-v090-three-index-telemetry-${qualification_suffix}"
journal_pressure_evidence_prefix="/var/tmp/keldra-v090-three-journal-pressure-${qualification_suffix}"
KELDRA_QUALIFICATION_STATE_DIR="${KELDRA_QUALIFICATION_DIR}/artifacts"
keep="${KELDRA_QUALIFICATION_KEEP:-0}"
paused_container=""
pressure_writer_pid=""
compose() {
  docker compose \
    --project-name "${KELDRA_QUALIFICATION_PROJECT}" \
    --file "${compose_file}" \
    "$@"
}
require_service_image() {
  local service="$1"
  local expected_image="$2"
  local label="$3"
  local container
  local actual_image
  container="$(compose ps --quiet "${service}")"
  actual_image="$(docker inspect --format '{{.Image}}' "${container}")"
  if [[ "${actual_image}" != "${expected_image}" ]]; then
    echo "${service} did not start from the exact ${label} image" >&2
    echo "expected: ${expected_image}" >&2
    echo "actual:   ${actual_image}" >&2
    return 1
  fi
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "${pressure_writer_pid}" ]] && kill -0 "${pressure_writer_pid}" 2>/dev/null; then
    kill "${pressure_writer_pid}" >/dev/null 2>&1 || true
    wait "${pressure_writer_pid}" >/dev/null 2>&1 || true
  fi
  pressure_writer_pid=""
  if [[ -n "${paused_container}" ]] \
    && docker inspect --format '{{.State.Paused}}' "${paused_container}" 2>/dev/null \
      | grep -Fxq true
  then
    docker unpause "${paused_container}" >/dev/null 2>&1 || true
  fi
  if ((status != 0)); then
    echo "[keldra-qualification] FAILED; container status and logs follow" >&2
    compose ps --all >&2 || true
    compose logs --no-color >&2 || true
  fi
  if [[ "${keep}" == "1" ]]; then
    echo "[keldra-qualification] retained project ${KELDRA_QUALIFICATION_PROJECT}" >&2
    echo "[keldra-qualification] retained files ${KELDRA_QUALIFICATION_DIR}" >&2
  else
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [[ "${KELDRA_QUALIFICATION_DIR}" == /var/tmp/keldra-v090-qualification.* ]]; then
      docker run --rm --user 0 \
        --volume "${KELDRA_QUALIFICATION_DIR}:/qualification" \
        "${image_id}" rm -rf \
          /qualification/node-1 \
          /qualification/node-2 \
          /qualification/node-3 \
          /qualification/artifacts \
          /qualification/token-signing-key >/dev/null 2>&1 || true
      rm -rf -- "${KELDRA_QUALIFICATION_DIR}"
    else
      echo "refusing to remove unexpected qualification path ${KELDRA_QUALIFICATION_DIR}" >&2
      status=1
    fi
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
server_help="$(docker run --rm "${image_id}" keldra-server --help)"
for required in --peer-listen --peer-advertise --join-bundle; do
  if ! grep -Fq -- "${required}" <<<"${server_help}"; then
    echo "qualification image is missing required server option ${required}" >&2
    exit 1
  fi
done
cli_help="$(docker run --rm "${image_id}" keldra --help)"
for required in prepare-node provision-tenant create-bucket; do
  if ! grep -Fq -- "${required}" <<<"${cli_help}"; then
    echo "qualification image is missing required CLI command ${required}" >&2
    exit 1
  fi
done
for directory in node-1 node-2 node-3 artifacts; do
  mkdir "${KELDRA_QUALIFICATION_DIR}/${directory}"
  chmod 0777 "${KELDRA_QUALIFICATION_DIR}/${directory}"
done
chmod 0755 "${KELDRA_QUALIFICATION_DIR}"
head -c 64 /dev/urandom >"${KELDRA_QUALIFICATION_DIR}/token-signing-key"
chmod 0600 "${KELDRA_QUALIFICATION_DIR}/token-signing-key"
docker run --rm --user 0 \
  --volume "${KELDRA_QUALIFICATION_DIR}/token-signing-key:/qualification-key" \
  "${image_id}" chown 10001:10001 /qualification-key
compose config --quiet
compose up --detach keldra-1
require_service_image keldra-1 "${image_id}" candidate
network="${KELDRA_QUALIFICATION_PROJECT}_default"
run_cli() {
  local node="$1"
  local client_id="$2"
  local client_secret="$3"
  shift 3
  docker run --rm \
    --network "${network}" \
    --volume "${KELDRA_QUALIFICATION_DIR}:/qualification" \
    --env "KELDRA_CLIENT_ID=${client_id}" \
    --env "KELDRA_CLIENT_SECRET=${client_secret}" \
    "${image_id}" \
    keldra --endpoint "http://${node}:50051" "$@"
}
run_bootstrap_cli() {
  local node="$1"
  shift
  local -a secret_environment=()
  if [[ -n "${KELDRA_NEW_CLIENT_SECRET:-}" ]]; then
    secret_environment=(--env KELDRA_NEW_CLIENT_SECRET)
  fi
  docker run --rm \
    --network "${network}" \
    --volume "${KELDRA_QUALIFICATION_DIR}:/qualification" \
    "${secret_environment[@]}" \
    "${image_id}" \
    keldra --endpoint "http://${node}:50051" \
      --credentials-file /qualification/node-1/system-bootstrap-credential.json "$@"
}
wait_for_bootstrap() {
  local attempt
  for attempt in $(seq 1 60); do
    if compose exec -T keldra-1 \
      test -f /var/lib/keldra/system-bootstrap-credential.json \
      >/dev/null 2>&1
    then
      return 0
    fi
    sleep 1
  done
  echo "node 1 did not generate its bootstrap credential within 60 seconds" >&2
  return 1
}
wait_for_node() {
  local node="$1"
  local attempt
  local output=""
  for attempt in $(seq 1 90); do
    if output="$(run_cli "${node}" qprobe-client \
      qualification-probe-secret-000000000000000000000000 \
      list qprobe objects --prefix readiness/ --limit 1 2>&1)"
    then
      return 0
    fi
    sleep 1
  done
  echo "${node} did not become an authenticated ACTIVE server within 90 seconds" >&2
  echo "last client error: ${output}" >&2
  return 1
}
service_container() {
  compose ps --quiet "$1"
}
service_logs() {
  docker logs "$(service_container "$1")" 2>&1 | strip_ansi
}

service_logs_since() {
  local node="$1"
  local cursor="$2"
  local until="${3:-}"
  if [[ -n "${until}" ]]; then
    docker logs --since "${cursor}" --until "${until}" \
      "$(service_container "${node}")" 2>&1 | strip_ansi
  else
    docker logs --since "${cursor}" "$(service_container "${node}")" 2>&1 \
      | strip_ansi
  fi
}

public_endpoint_for() {
  local node="$1"
  local published
  published="$(compose port "${node}" 50051)"
  if [[ ! "${published}" =~ ^127\.0\.0\.1:([1-9][0-9]*)$ ]]; then
    echo "${node} returned an invalid loopback public endpoint: ${published}" >&2
    return 1
  fi
  printf 'http://%s\n' "${published}"
}

run_index_recovery_qualification() {
  local mode="$1"
  local endpoints="$2"
  local tenant="$3"
  local client_id="$4"
  local client_secret="$5"
  local state_path="$6"
  local bucket="${7:-}"
  local release_path="${8:-}"
  KELDRA_INDEX_RECOVERY_QUALIFICATION_MODE="${mode}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_ENDPOINTS="${endpoints}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_CLIENT_ID="${client_id}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_CLIENT_SECRET="${client_secret}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_STATE="${state_path}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_BUCKET="${bucket}" \
  KELDRA_INDEX_RECOVERY_QUALIFICATION_RELEASE="${release_path}" \
    "${qualification_binaries[index_recovery_qualification]}"
}

log_cursor() {
  qualification_log_cursor
}

save_log_suffix() {
  local node="$1"
  local cursor="$2"
  local destination="$3"
  service_logs_since "${node}" "${cursor}" >"${destination}"
}

state_index_ids() {
  sed -n 's/^[[:space:]]*"index_id":[[:space:]]*\([1-9][0-9]*\),\{0,1\}[[:space:]]*$/\1/p' "$1"
}

log_has_index_event() {
  local log="$1"
  local index_id="$2"
  local message="$3"
  awk -v marker="index.id=${index_id} " -v message="${message}" '
      index($0, marker) && index($0, "index.kind=Path") &&
      index($0, message) { found = 1 }
      END { exit !found }
    ' "${log}"
}

index_sparse_start_count() {
  service_logs "$1" \
    | grep -Fc 'index runtime starts from sparse assigned-definition state' \
    || true
}

startup_scan_evidence_count() {
  service_logs "$1" \
    | grep -Fc 'keldra_startup_scan_evidence' \
    || true
}

wait_for_sparse_index_startup() {
  local node="$1"
  local minimum_count="$2"
  local deadline=$((SECONDS + 90))
  while (( $(index_sparse_start_count "${node}") < minimum_count \
    || $(startup_scan_evidence_count "${node}") < minimum_count )); do
    if ! compose ps --status running --services | grep -Fxq "${node}"; then
      echo "${node} exited during index startup" >&2
      return 1
    fi
    if ((SECONDS >= deadline)); then
      echo "${node} index runtime did not finish startup within 90 seconds" >&2
      return 1
    fi
    sleep 1
  done
}

assert_zero_global_startup_scan_evidence() {
  local node="$1"
  local minimum_count="$2"
  local count=0
  local expected_node_id="${node#keldra-}"
  local field
  local line
  local node_id
  local value
  while IFS= read -r line; do
    node_id="$(log_unsigned_field node_id "${line}")" || {
      echo "${node} startup scan evidence omitted node_id" >&2
      return 1
    }
    if [[ "${node_id}" != "${expected_node_id}" ]]; then
      echo "${node} startup scan evidence reported node=${node_id}" >&2
      return 1
    fi
    for field in \
      global_object_head_scans_total \
      global_index_artifact_scans_total \
      global_blob_scans_total \
      global_cache_scans_total
    do
      value="$(log_unsigned_field "${field}" "${line}")" || {
        echo "${node} startup scan evidence omitted ${field}" >&2
        return 1
      }
      if [[ "${value}" != "0" ]]; then
        echo "${node} startup reported ${field}=${value}" >&2
        return 1
      fi
    done
    count=$((count + 1))
  done < <(
    service_logs "${node}" \
      | grep -F 'keldra_startup_scan_evidence' || true
  )
  if ((count < minimum_count)); then
    echo "${node} startup emitted ${count} measured scan samples; expected at least ${minimum_count}" >&2
    return 1
  fi
}

assert_sparse_index_startup() {
  local node="$1"
  local minimum_count="$2"
  local observed
  wait_for_sparse_index_startup "${node}" "${minimum_count}"
  observed="$(index_sparse_start_count "${node}")"
  if ((observed < minimum_count)); then
    echo "${node} startup omitted the sparse index-runtime marker" >&2
    return 1
  fi
  if service_logs "${node}" \
    | grep -F 'index journals did not reach a clear initial definition barrier' \
      >/dev/null
  then
    echo "${node} startup entered the removed global definition barrier" >&2
    return 1
  fi
  assert_zero_global_startup_scan_evidence "${node}" "${minimum_count}"
}

provision_tenant() {
  local tenant="$1"
  local client_id="$2"
  local client_secret="$3"
  local node
  local output=""
  for node in keldra-1 keldra-2 keldra-3; do
    if ! compose ps --status running --services | grep -Fxq "${node}"; then
      continue
    fi
    if output="$(KELDRA_NEW_CLIENT_SECRET="${client_secret}" \
      run_bootstrap_cli "${node}" provision-tenant \
        "${tenant}" "${tenant}-owner" "${client_id}" 2>&1)"
    then
      grep -Fq "tenant=${tenant}" <<<"${output}" || {
        echo "tenant provisioning returned unexpected output: ${output}" >&2
        return 1
      }
      return 0
    fi
  done
  echo "no ACTIVE node accepted tenant provisioning for ${tenant}" >&2
  echo "last administration error: ${output}" >&2
  return 1
}

create_bucket() {
  local node="$1"
  local client_id="$2"
  local client_secret="$3"
  local bucket="$4"
  run_cli "${node}" "${client_id}" "${client_secret}" create-bucket "${bucket}" \
    | grep -Fq "bucket=${bucket}"
}

run_index_resource_qualification() {
  local -A resource_log_starts=()
  local containers=()
  local resource_node
  assert_source_tree_exact
  for resource_node in keldra-1 keldra-2 keldra-3; do
    containers+=("$(service_container "${resource_node}")")
    resource_log_starts["${resource_node}"]="$(log_cursor)"
  done
  KELDRA_V06_RESOURCE_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  KELDRA_V06_RESOURCE_TENANT="${index_resource_tenant}" \
  KELDRA_V06_RESOURCE_BUCKET="${index_resource_bucket}" \
  KELDRA_V06_RESOURCE_CLIENT_ID="${index_resource_client}" \
  KELDRA_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
  KELDRA_V06_RESOURCE_RECORDS="${index_resource_records}" \
  KELDRA_V06_RESOURCE_MUTATIONS="${index_resource_mutations}" \
  KELDRA_V06_RESOURCE_BATCH_SIZE=1000 \
  KELDRA_V06_RESOURCE_WORKERS=6 \
  KELDRA_V06_RESOURCE_VERIFICATION_WORKERS=8 \
  KELDRA_V06_RESOURCE_CONTAINERS="$(IFS=,; echo "${containers[*]}")" \
  KELDRA_V06_REQUIRE_RESOURCE_TARGETS=1 \
  KELDRA_V06_KIND_BUDGET_BYTES="${index_kind_budget_bytes}" \
  KELDRA_V06_INDEX_COMPACTION_MAX_LANES="${index_compaction_max_lanes}" \
  KELDRA_V06_INDEX_PROJECTION_MAX_LANES="${index_projection_max_lanes}" \
  KELDRA_V06_INDEX_RAYON_WORKERS="${index_rayon_workers}" \
  KELDRA_V06_MAX_ANONYMOUS_GROWTH_BYTES="${index_resource_max_anonymous_growth_bytes}" \
  KELDRA_V09_REQUIRE_PERFORMANCE_TARGETS="${require_performance_targets}" \
  KELDRA_V09_EVIDENCE_SOURCE_COMMIT="${source_commit}" \
  KELDRA_V09_EVIDENCE_CONTAINER_DIGEST="${image_id}" \
  KELDRA_V09_EVIDENCE_NATIVE_ARCHITECTURE="${native_architecture}" \
  KELDRA_V09_EVIDENCE_CONTAINER_PLATFORM="${container_platform}" \
  KELDRA_V09_EVIDENCE_TOPOLOGY=three-node \
  KELDRA_V09_EVIDENCE_NODE_COUNT=3 \
  KELDRA_V09_EVIDENCE_HARDWARE_LOGICAL_CPUS="${hardware_logical_cpus}" \
  KELDRA_V09_EVIDENCE_HARDWARE_MEMORY_BYTES="${hardware_memory_bytes}" \
  KELDRA_V09_EVIDENCE_FILESYSTEM_TOTAL_BYTES="${qualification_filesystem_total_bytes}" \
  KELDRA_V09_EVIDENCE_FILESYSTEM_AVAILABLE_BYTES="${qualification_filesystem_available_bytes}" \
  KELDRA_V09_EVIDENCE_INDEX_DISK_CACHE_BYTES_PER_NODE="${index_disk_cache_bytes}" \
  KELDRA_V09_EVIDENCE_INDEX_MEMORY_PERCENT_PER_NODE="${index_memory_percent}" \
  KELDRA_V06_RESOURCE_OUTPUT="${index_resource_report}" \
  KELDRA_V06_RESOURCE_STATE_OUTPUT="${index_resource_state}" \
    "${qualification_binaries[v06_index_resource_qualification]}" >/dev/null
  for resource_node in keldra-1 keldra-2 keldra-3; do
    capture_three_node_resource_evidence \
      "${resource_node}" "${resource_log_starts[${resource_node}]}"
  done
  test -s "${index_resource_report}"
  test -s "${index_resource_state}"
  grep -Eq "^[[:space:]]*\"records\":[[:space:]]*${index_resource_records},?[[:space:]]*$" \
    "${index_resource_report}"
  grep -Eq '^[[:space:]]*"indexed_fields":[[:space:]]*12,?[[:space:]]*$' \
    "${index_resource_report}"
  jq -e \
    --arg source_commit "${source_commit}" \
    --arg container_digest "${image_id}" \
    --arg native_architecture "${native_architecture}" \
    --arg container_platform "${container_platform}" \
    --argjson hardware_logical_cpus "${hardware_logical_cpus}" \
    --argjson hardware_memory_bytes "${hardware_memory_bytes}" \
    --argjson filesystem_total_bytes "${qualification_filesystem_total_bytes}" \
    --argjson filesystem_available_bytes "${qualification_filesystem_available_bytes}" \
    --argjson disk_cache_bytes "${index_disk_cache_bytes}" \
    --argjson memory_percent "${index_memory_percent}" \
    --argjson kind_budget_bytes "${index_kind_budget_bytes}" \
    --argjson compaction_lanes "${index_compaction_max_lanes}" \
    --argjson projection_lanes "${index_projection_max_lanes}" \
    --argjson rayon_workers "${index_rayon_workers}" \
    --argjson maximum_growth "${index_resource_max_anonymous_growth_bytes}" \
    --argjson performance_targets_required "${require_performance_targets}" \
    '
      .evidence.source_commit == $source_commit and
      .evidence.resolved_container_digest == $container_digest and
      .evidence.native_architecture == $native_architecture and
      .evidence.container_platform == $container_platform and
      .evidence.hardware.logical_cpus == $hardware_logical_cpus and
      .evidence.hardware.memory_bytes == $hardware_memory_bytes and
      .evidence.hardware.qualification_filesystem_total_bytes == $filesystem_total_bytes and
      .evidence.hardware.qualification_filesystem_available_bytes_at_start == $filesystem_available_bytes and
      .evidence.corpus.identity == "keldra.synthetic-index-resource.initial.v1" and
      (.evidence.corpus.initial_corpus_sha256 | test("^sha256:[0-9a-f]{64}$")) and
      .evidence.corpus.records == .records and
      .evidence.corpus.indexed_fields == .indexed_fields and
      .evidence.topology.kind == "three-node" and
      .evidence.topology.node_count == 3 and
      .evidence.topology.ingress_endpoint_count == 3 and
      .evidence.durability.initial_writes == "LOCAL" and
      .evidence.durability.updates == "LOCAL" and
      .evidence.durability.deletes == "LOCAL" and
      .evidence.execution.bulk_write_max_operations == .batch_size and
      .evidence.execution.ingest_workers == .ingest_workers and
      .evidence.execution.verification_workers == .verification_workers and
      .evidence.resource_configuration.index_disk_cache_bytes_per_node == $disk_cache_bytes and
      .evidence.resource_configuration.index_memory_percent_per_node == $memory_percent and
      .evidence.resource_configuration.builder_memory_bytes_per_kind_per_node == $kind_budget_bytes and
      .evidence.resource_configuration.compaction_max_lanes_per_kind == $compaction_lanes and
      .evidence.resource_configuration.projection_max_lanes_per_kind == $projection_lanes and
      .evidence.resource_configuration.projection_max_lanes_per_kind ==
        .evidence.resource_configuration.rayon_workers_per_node and
      .evidence.resource_configuration.rayon_workers_per_node == $rayon_workers and
      .evidence.resource_configuration.maximum_anonymous_growth_bytes == $maximum_growth and
      .evidence.resource_configuration.monitored_target_count == 3 and
      .evidence.resource_configuration.resource_targets_required == true and
      (.evidence.timer_boundaries | to_entries | all(.value | if type == "object" then (.starts | length > 0) and (.stops | length > 0) else length > 0 end)) and
      .evidence.correctness.result == "pass" and
      .evidence.correctness.source_complete_generation_observed == true and
      .evidence.correctness.source_complete_sources_observed == 3 and
      .evidence.correctness.initial_exact_partition_verification == true and
      .evidence.correctness.final_exact_partition_verification == true and
      .evidence.correctness.update_and_delete_verification == true and
      .evidence.correctness.resource_limits_passed == true and
      .production_query_regression.schema == "keldra.index-production-query-regression.v1" and
      .production_query_regression.corpus_records == .records and
      .production_query_regression.index_id > 0 and
      .production_query_regression.definition_version > 0 and
      .production_query_regression.generation > 0 and
      .production_query_regression.physical_order == ["modified_day DESC", "record_id ASC"] and
      .production_query_regression.incident_predicates == [
        "withdrawn = false",
        "active = true",
        "ecosystem IN (cargo, npm, pypi)"
      ] and
      .production_query_regression.limit_four.returned_hits == 4 and
      .production_query_regression.limit_four.exact_order == true and
      .production_query_regression.consecutive_pages.requested_page_size == 999 and
      .production_query_regression.consecutive_pages.page_one_hits == 999 and
      .production_query_regression.consecutive_pages.page_two_hits == 999 and
      .production_query_regression.consecutive_pages.continuation_token_bytes > 0 and
      .production_query_regression.consecutive_pages.page_two_used_page_one_token == true and
      .production_query_regression.consecutive_pages.exact_order == true and
      .production_query_regression.consecutive_pages.overlap == 0 and
      .production_query_regression.zero_hit_sparse_conjunction.returned_hits == 0 and
      .production_query_regression.zero_hit_sparse_conjunction.exact_order == true and
      .production_query_regression.unselective_arbitrary_sort.returned_hits == 4 and
      .production_query_regression.unselective_arbitrary_sort.exact_order == true and
      .evidence.correctness.performance_targets_required == ($performance_targets_required == 1) and
      (if $performance_targets_required == 1
       then .evidence.correctness.performance_targets_passed == true
       else .evidence.correctness.performance_targets_passed == null
       end)
    ' "${index_resource_report}" >/dev/null
  if ((require_performance_targets == 1)); then
    jq -e '
      .accepted_objects_per_second >= 3000 and
      .source_complete_objects_per_second >= 1000
    ' "${index_resource_report}" >/dev/null
  fi
  echo "[keldra-qualification] bounded distributed index resource qualification passed scope=${index_resource_scope} records=${index_resource_records} kind_budget=${index_kind_budget_bytes}"
  echo "[keldra-qualification] preserved resource report ${index_resource_report}"
  echo "[keldra-qualification] preserved full production telemetry ${index_resource_telemetry_prefix}-keldra-{1,2,3}.log"
}

verify_index_resource_state() {
  local endpoint="$1"
  KELDRA_V06_RESOURCE_ENDPOINTS="${endpoint}" \
  KELDRA_V06_RESOURCE_TENANT="${index_resource_tenant}" \
  KELDRA_V06_RESOURCE_BUCKET="${index_resource_bucket}" \
  KELDRA_V06_RESOURCE_CLIENT_ID="${index_resource_client}" \
  KELDRA_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
  KELDRA_V06_RESOURCE_VERIFICATION_WORKERS=8 \
  KELDRA_V06_RESOURCE_STATE_INPUT="${index_resource_state}" \
    "${qualification_binaries[v06_index_resource_qualification]}"
}
assert_index_resource_bounds() {
  local -A observed_kinds=()
  local configured
  local kind
  local line
  local observed=0
  local leased
  local peak_leased
  local resource_node
  while IFS= read -r line; do
    if [[ "${line}" =~ index\.kind=(Path|MetadataFilter|TypedJson|FullText|Vector|Hybrid|GitSource|Tensor) ]]; then
      kind="${BASH_REMATCH[1]}"
    else
      continue
    fi
    configured="$(log_unsigned_field gauge.keldra_index_construction_configured_bytes "${line}")" \
      || continue
    leased="$(log_unsigned_field gauge.keldra_index_construction_leased_bytes "${line}")" \
      || return 1
    peak_leased="$(log_unsigned_field gauge.keldra_index_construction_peak_leased_bytes "${line}")" \
      || return 1
    if ((configured != index_kind_budget_bytes \
      || leased > configured \
      || peak_leased > configured)); then
      echo "distributed index construction exceeded or misstated its configured kind budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    observed_kinds["${kind}"]=1
    observed=$((observed + 1))
  done < <(
    for resource_node in keldra-1 keldra-2 keldra-3; do
      grep -F 'index construction budget state' \
        "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${resource_node}.log" || true
    done
  )
  if ((observed == 0)); then
    echo "distributed index qualification emitted no construction budget evidence" >&2
    return 1
  fi
  for kind in "${index_kinds[@]}"; do
    if [[ -z "${observed_kinds[${kind}]:-}" ]]; then
      echo "distributed qualification emitted no ${kind} construction budget evidence" >&2
      return 1
    fi
  done

  local -A resident_kinds=()
  local resident
  local workspace
  while IFS= read -r line; do
    [[ "${line}" =~ index\.kind=(Path|MetadataFilter|TypedJson|FullText|Vector|Hybrid|GitSource|Tensor) ]] \
      || continue
    kind="${BASH_REMATCH[1]}"
    resident="$(log_unsigned_field gauge.keldra_index_construction_resident_bytes "${line}")" \
      || return 1
    workspace="$(log_unsigned_field gauge.keldra_index_construction_workspace_bytes "${line}")" \
      || return 1
    if ((resident == 0 || workspace == 0 || resident > workspace \
      || workspace > index_kind_budget_bytes)); then
      echo "${kind} emitted out-of-budget distributed construction residency/workspace evidence" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resident_kinds["${kind}"]=1
  done < <(
    for resource_node in keldra-1 keldra-2 keldra-3; do
      grep -F 'format-v4 index segment flushed' \
        "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${resource_node}.log" || true
    done
  )
  for kind in "${index_kinds[@]}"; do
    if [[ -z "${resident_kinds[${kind}]:-}" ]]; then
      echo "distributed qualification emitted no ${kind} construction residency/workspace evidence" >&2
      return 1
    fi
  done

  local resource_budget_evidence=0 resource_positive_peak_evidence=0
  while IFS= read -r line; do
    if [[ "${line}" != *"index.kind=TypedJson"* \
      || "${line}" != *"index construction budget state"* ]]; then
      continue
    fi
    configured="$(log_unsigned_field gauge.keldra_index_construction_configured_bytes "${line}")" \
      || {
      echo "distributed production-shaped TypedJson build emitted malformed budget evidence" >&2
      printf '%s\n' "${line}" >&2
      return 1
    }
    leased="$(log_unsigned_field gauge.keldra_index_construction_leased_bytes "${line}")" \
      || return 1
    peak_leased="$(log_unsigned_field gauge.keldra_index_construction_peak_leased_bytes "${line}")" \
      || return 1
    if ((configured != index_kind_budget_bytes \
      || leased > configured \
      || peak_leased > configured)); then
      echo "distributed production-shaped TypedJson build exceeded or misstated its configured kind budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    if ((peak_leased > 0)); then resource_positive_peak_evidence=1; fi
    resource_budget_evidence=$((resource_budget_evidence + 1))
  done < <(
    for resource_node in keldra-1 keldra-2 keldra-3; do
      cat "${KELDRA_QUALIFICATION_DIR}/artifacts/index-resource-${resource_node}.log"
    done
  )
  if ((resource_budget_evidence == 0 || resource_positive_peak_evidence == 0)); then
    echo "distributed production-shaped TypedJson build emitted no positive construction-budget evidence" >&2
    return 1
  fi

  local resource_residency_evidence=0
  while IFS= read -r line; do
    [[ "${line}" == *"index.kind=TypedJson"* ]] || continue
    resident="$(log_unsigned_field gauge.keldra_index_construction_resident_bytes "${line}")" \
      || return 1
    workspace="$(log_unsigned_field gauge.keldra_index_construction_workspace_bytes "${line}")" \
      || return 1
    if ((resident == 0 || workspace == 0 || resident > workspace \
      || workspace > index_kind_budget_bytes)); then
      echo "distributed production-shaped TypedJson build exceeded its residency/workspace budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resource_residency_evidence=$((resource_residency_evidence + 1))
  done < <(
    for resource_node in keldra-1 keldra-2 keldra-3; do
      grep -F 'format-v4 index segment flushed' \
        "${KELDRA_QUALIFICATION_DIR}/artifacts/index-resource-${resource_node}.log" || true
    done
  )
  if ((resource_residency_evidence == 0)); then
    echo "distributed production-shaped TypedJson build emitted no fresh residency/workspace evidence" >&2
    return 1
  fi

  if [[ "${index_resource_scope}" == "release-corpus" ]]; then
    assert_production_resource_compaction
  fi
  local cache_bytes
  for resource_node in 1 2 3; do
    cache_bytes="$(find \
      "${KELDRA_QUALIFICATION_DIR}/node-${resource_node}/index-cache" \
      -type f -printf '%s\n' \
      | awk '{ total += $1 } END { print total + 0 }')"
    if ((cache_bytes > index_disk_cache_bytes)); then
      echo "keldra-${resource_node} disposable index cache exceeded its ${index_disk_cache_bytes}-byte budget: ${cache_bytes}" >&2
      return 1
    fi
  done
  echo "[keldra-qualification] distributed index construction and disk caches remained within configured bounds"
}

declare -A index_qualification_log_start=()

capture_index_qualification_log_start() {
  local node
  for node in keldra-1 keldra-2 keldra-3; do
    index_qualification_log_start["${node}"]="$(log_cursor)"
  done
}

save_index_qualification_logs() {
  local node
  local cursor
  for node in keldra-1 keldra-2 keldra-3; do
    cursor="${index_qualification_log_start[${node}]}"
    service_logs_since "${node}" "${cursor}" \
      >"${KELDRA_QUALIFICATION_DIR}/artifacts/index-${node}.log"
    preserve_all_kind_telemetry "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${node}.log" three "${qualification_suffix}" "${node}"
  done
}

assert_production_resource_compaction() {
  local builder=
  local builders=0
  local node
  local log
  for node in keldra-1 keldra-2 keldra-3; do
    log="${KELDRA_QUALIFICATION_DIR}/artifacts/index-resource-${node}.log"
    if awk '
          index($0, "index.kind=TypedJson") &&
          index($0, "index compaction terminal metrics") { found = 1 }
          END { exit !found }
        ' "${log}"
    then
      builder="${node}"
      builders=$((builders + 1))
    fi
  done
  if ((builders != 1)); then
    echo "production-shaped TypedJson compaction completed on ${builders} nodes; expected exactly one builder" >&2
    return 1
  fi
  assert_compaction_telemetry_for_kind \
    TypedJson "${KELDRA_QUALIFICATION_DIR}/artifacts/index-resource-${builder}.log"
  echo "[keldra-qualification] production-shaped TypedJson compaction completed on its sole builder"
}

assert_one_builder_published_and_compacted_each_index_kind() {
  local compactors
  local compactor_node
  local kind
  local node
  local publisher_node
  local publishers
  for kind in "${index_kinds[@]}"; do
    compactors=0
    compactor_node=
    publisher_node=
    publishers=0
    for node in keldra-1 keldra-2 keldra-3; do
      if awk -v kind="index.kind=${kind}" '
            index($0, kind) && index($0, "index generation published") { found = 1 }
            END { exit !found }
          ' "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${node}.log"
      then
        publishers=$((publishers + 1))
        publisher_node="${node}"
      fi
      if awk -v kind="index.kind=${kind}" '
            index($0, kind) && index($0, "format-v4 index segments compacted") { found = 1 }
            END { exit !found }
          ' "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${node}.log"
      then
        compactors=$((compactors + 1))
        compactor_node="${node}"
      fi
    done
    if ((publishers != 1)); then
      echo "${kind} index generations were published by ${publishers} nodes; expected exactly one builder" >&2
      return 1
    fi
    if ((compactors != 1)); then
      echo "${kind} index was compacted by ${compactors} nodes; expected exactly one builder" >&2
      return 1
    fi
    if [[ "${publisher_node}" != "${compactor_node}" ]]; then
      echo "${kind} publication and compaction ran on different builders" >&2
      return 1
    fi
    assert_compaction_telemetry_for_kind \
      "${kind}" "${KELDRA_QUALIFICATION_DIR}/artifacts/index-${compactor_node}.log"
  done
  echo "[keldra-qualification] all eight kinds consumed all three ingress journals and emitted bounded range-compaction metrics with trace-backed completion logs on their sole builder"
}

verify_existing_indexes() {
  KELDRA_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  KELDRA_INDEX_QUALIFICATION_TENANT=qindex \
  KELDRA_INDEX_QUALIFICATION_CLIENT_ID=qindex-client \
  KELDRA_INDEX_QUALIFICATION_CLIENT_SECRET="${index_secret}" \
  KELDRA_INDEX_QUALIFICATION_STATE_INPUT="${index_verification_state}" \
    "${qualification_binaries[cluster_index_qualification]}"
}

assert_index_retention_converged() {
  local deadline=$((SECONDS + 65))
  local failed
  local index_id
  local node
  local pending
  local converged
  local -a index_ids=()
  mapfile -t index_ids < <(
    sed -n 's/^[[:space:]]*"index_id":[[:space:]]*\([1-9][0-9]*\),[[:space:]]*$/\1/p' \
      "${index_verification_state}"
  )
  if ((${#index_ids[@]} != ${#index_kinds[@]})); then
    echo "distributed verification state did not contain all eight index IDs" >&2
    return 1
  fi
  while true; do
    failed="$({
      for node in keldra-1 keldra-2 keldra-3; do
        service_logs "${node}"
      done
    } | grep -F 'bounded index retention work failed' | tail -n 1 || true)"
    if [[ -n "${failed}" ]]; then
      echo "distributed index retention reported failed work" >&2
      printf '%s\n' "${failed}" >&2
      return 1
    fi
    pending=0
    for index_id in "${index_ids[@]}"; do
      converged=0
      for node in keldra-1 keldra-2 keldra-3; do
        if service_logs "${node}" | awk -v marker="index.id=${index_id} " '
            index($0, marker) && index($0, "bounded node-wide index retention tick completed") &&
            $0 ~ /monotonic_counter.keldra_index_retention_artifacts_deleted_total=[1-9][0-9]*/ {
              deleted = 1
            }
            deleted && index($0, marker) &&
            index($0, "bounded node-wide index retention tick completed") &&
            $0 ~ /gauge.keldra_index_retention_backlog=0/ {
              converged = 1
            }
            END { exit !converged }
          '
        then
          converged=1
          break
        fi
      done
      if ((converged == 0)); then
        pending=$((pending + 1))
      fi
    done
    if ((pending == 0)); then
      echo "[keldra-qualification] all eight indexes deleted obsolete artifacts and drained their retention backlog"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "${pending} distributed indexes did not converge retention within 65 seconds" >&2
      return 1
    fi
    sleep 1
  done
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "${label} unexpectedly succeeded" >&2
    return 1
  fi
}

run_git_qualification() {
  local tenant=qgit
  local client_id=qgit-client
  local client_secret=qualification-git-secret-0000000000000000000000000
  local bucket="git-three-${$}"
  local git_root="${KELDRA_QUALIFICATION_DIR}/git"
  local source_repository="${git_root}/source"
  local authenticated_clone="${git_root}/authenticated-clone"
  local denied_clone="${git_root}/denied-clone"
  local public_clone="${git_root}/public-clone"
  local push_url="${public_endpoints[0]}/git/${tenant}/${bucket}/qualification.git"
  local authenticated_clone_url="${public_endpoints[1]}/git/${tenant}/${bucket}/qualification.git"
  local public_clone_url="${public_endpoints[2]}/git/${tenant}/${bucket}/qualification.git"
  local authorization

  provision_tenant "${tenant}" "${client_id}" "${client_secret}"
  create_bucket keldra-1 "${client_id}" "${client_secret}" "${bucket}"

  mkdir -p "${git_root}"
  git init --quiet --initial-branch=main "${source_repository}"
  git -C "${source_repository}" config user.name "Keldra Qualification"
  git -C "${source_repository}" config user.email "qualification@example.invalid"
  printf 'three-node smart HTTP gateway\n' >"${source_repository}/README.md"
  git -C "${source_repository}" add README.md
  git -C "${source_repository}" commit --quiet -m initial

  authorization="$(
    printf '%s:%s' "${client_id}" "${client_secret}" | base64 | tr -d '\n'
  )"
  git -C "${source_repository}" \
    -c "http.extraHeader=Authorization: Basic ${authorization}" \
    push --quiet "${push_url}" main
  git -c "http.extraHeader=Authorization: Basic ${authorization}" \
    clone --quiet --branch main "${authenticated_clone_url}" \
      "${authenticated_clone}"
  cmp "${source_repository}/README.md" "${authenticated_clone}/README.md"

  if GIT_TERMINAL_PROMPT=0 git clone --quiet --branch main \
    "${public_clone_url}" "${denied_clone}" >/dev/null 2>&1; then
    echo "private Git repository allowed an anonymous clone" >&2
    return 1
  fi

  run_cli keldra-3 "${client_id}" "${client_secret}" \
    set-bucket-public-read "${bucket}" enabled >/dev/null
  git clone --quiet --branch main "${public_clone_url}" "${public_clone}"
  cmp "${source_repository}/README.md" "${public_clone}/README.md"

  echo "[keldra-qualification] cross-node Git push, authenticated clone, and public clone passed"
}

run_atomic_index_qualification() {
  KELDRA_ATOMIC_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  KELDRA_ATOMIC_INDEX_QUALIFICATION_TENANT=qatomic \
  KELDRA_ATOMIC_INDEX_QUALIFICATION_BUCKET="atomic-index-three-${$}" \
  KELDRA_ATOMIC_INDEX_QUALIFICATION_CLIENT_ID=qatomic-client \
  KELDRA_ATOMIC_INDEX_QUALIFICATION_CLIENT_SECRET="${atomic_secret}" \
    "${qualification_binaries[atomic_index_qualification]}"
  echo "[keldra-qualification] distributed atomic-program index visibility passed"
}

run_live_builder_reassignment_qualification() {
  local active_nodes="$1"
  local endpoints="$2"
  local new_builder_node="$3"
  local membership_mode
  local -A log_starts=()
  local index_id
  local -a index_ids=()
  local node
  local reassigned=0
  case "${active_nodes}" in
    2) membership_mode=membership-verify-two ;;
    3) membership_mode=membership-verify-three ;;
    *)
      echo "membership reassignment qualification requires 2 or 3 ACTIVE nodes" >&2
      return 1
      ;;
  esac
  mapfile -t index_ids < <(state_index_ids "${index_membership_state}")
  if ((${#index_ids[@]} != 16)); then
    echo "membership state did not contain all 16 fixture index IDs" >&2
    return 1
  fi
  for node_number in $(seq 1 "${active_nodes}"); do
    node="keldra-${node_number}"
    log_starts["${node}"]="$(log_cursor)"
  done
  run_index_recovery_qualification \
    "${membership_mode}" "${endpoints}" \
    qindex-membership qindex-membership-client "${index_membership_secret}" \
    "${index_membership_state}"
  for node_number in $(seq 1 "${active_nodes}"); do
    node="keldra-${node_number}"
    save_log_suffix \
      "${node}" "${log_starts[${node}]}" \
      "${KELDRA_QUALIFICATION_DIR}/artifacts/index-reassignment-${active_nodes}-${node}.log"
  done
  for index_id in "${index_ids[@]}"; do
    if log_has_index_event \
      "${KELDRA_QUALIFICATION_DIR}/artifacts/index-reassignment-${active_nodes}-${new_builder_node}.log" \
      "${index_id}" "index generation published"
    then
      reassigned=1
      break
    fi
  done
  if ((reassigned == 0)); then
    echo "no pre-growth Path index published from ${new_builder_node} after the ${active_nodes}-node cutover" >&2
    return 1
  fi
  echo "[keldra-qualification] pre-growth indexes remained exact and published from ${new_builder_node} after online $((active_nodes - 1))->${active_nodes} reassignment"
}

run_journal_pressure_qualification() {
  local builder="" capacity_node="" ingress="" node pressure_log seed_log
  local builder_count=0 deadline pending_command pressure_cursor pressure_index_id
  local seed_cursor successful_mutations
  seed_cursor="$(log_cursor)"
  run_index_recovery_qualification \
    pressure-seed "$(IFS=,; echo "${public_endpoints[*]}")" \
    qindex-pressure qindex-pressure-client "${index_pressure_secret}" \
    "${index_pressure_state}" "index-pressure-${$}"
  pressure_index_id="$(state_index_ids "${index_pressure_state}")"
  if [[ ! "${pressure_index_id}" =~ ^[1-9][0-9]*$ ]]; then
    echo "journal-pressure state did not contain exactly one fixture index ID" >&2
    return 1
  fi
  for node in keldra-1 keldra-2 keldra-3; do
    seed_log="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-seed-${node}.log"
    save_log_suffix "${node}" "${seed_cursor}" "${seed_log}"
    if log_has_index_event "${seed_log}" "${pressure_index_id}" \
      "index generation published"
    then
      builder="${node}"
      builder_count=$((builder_count + 1))
    fi
  done
  if ((builder_count != 1)); then
    echo "journal-pressure seed was published by ${builder_count} nodes; expected one builder" >&2
    return 1
  fi
  for node in keldra-1 keldra-2 keldra-3; do
    if [[ "${node}" != "${builder}" ]]; then
      ingress="${node}"
      break
    fi
  done
  if [[ -z "${ingress}" ]]; then
    echo "journal-pressure qualification found no non-builder ingress" >&2
    return 1
  fi

  pressure_cursor="$(log_cursor)"
  paused_container="$(service_container "${builder}")"
  docker pause "${paused_container}" >/dev/null
  : >"${index_pressure_writer_output}"
  run_index_recovery_qualification \
    pressure-write "$(public_endpoint_for "${ingress}")" \
    qindex-pressure qindex-pressure-client "${index_pressure_secret}" \
    "${index_pressure_state}" "" "${index_pressure_release}" \
    >"${index_pressure_writer_output}" 2>&1 &
  pressure_writer_pid=$!

  deadline=$((SECONDS + 90))
  while ((SECONDS < deadline)); do
    if ! kill -0 "${pressure_writer_pid}" 2>/dev/null; then
      wait "${pressure_writer_pid}" >/dev/null 2>&1 || true
      pressure_writer_pid=""
      echo "journal-pressure writer exited before reaching backpressure" >&2
      cat "${index_pressure_writer_output}" >&2
      return 1
    fi
    read -r pending_command successful_mutations < <(
      jq -r '[.pending_command_id // "", .successful_mutations // 0] | @tsv' \
        "${index_pressure_state}" 2>/dev/null || true
    )
    capacity_node=""
    for node in keldra-1 keldra-2 keldra-3; do
      pressure_log="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-live-${node}.log"
      save_log_suffix "${node}" "${pressure_cursor}" "${pressure_log}"
      if grep -F 'distributed object mutation is waiting for bounded durable state' \
        "${pressure_log}" | grep -Fq 'capacity="source_journal"'
      then
        capacity_node="${node}"
        break
      fi
    done
    if [[ -n "${pending_command}" \
      && "${successful_mutations}" =~ ^[1-9][0-9]*$ \
      && -n "${capacity_node}" ]]
    then
      break
    fi
    sleep 1
  done
  if ((SECONDS >= deadline)); then
    echo "journal-pressure writer did not reach source-journal backpressure within 90 seconds" >&2
    cat "${index_pressure_writer_output}" >&2
    return 1
  fi

  sleep 3
  if ! kill -0 "${pressure_writer_pid}" 2>/dev/null; then
    wait "${pressure_writer_pid}" >/dev/null 2>&1 || true
    pressure_writer_pid=""
    echo "journal-pressure writer did not remain pending while its index builder was paused" >&2
    cat "${index_pressure_writer_output}" >&2
    return 1
  fi
  run_index_recovery_qualification \
    pressure-assert-blocked "$(public_endpoint_for "${ingress}")" \
    qindex-pressure qindex-pressure-client "${index_pressure_secret}" \
    "${index_pressure_state}"
  if ! kill -0 "${pressure_writer_pid}" 2>/dev/null; then
    wait "${pressure_writer_pid}" >/dev/null 2>&1 || true
    pressure_writer_pid=""
    echo "journal-pressure writer completed before capacity was released" >&2
    cat "${index_pressure_writer_output}" >&2
    return 1
  fi

  for node in keldra-1 keldra-2 keldra-3; do
    pressure_log="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-${node}.log"
    save_log_suffix "${node}" "${pressure_cursor}" "${pressure_log}"
    if grep -Fq 'index snapshot rebuild started' "${pressure_log}"; then
      echo "${node} started an unauthorized automatic snapshot rebuild under journal pressure" >&2
      return 1
    fi
  done

  touch "${index_pressure_release}"
  docker unpause "${paused_container}" >/dev/null
  paused_container=""
  wait_for_node "${builder}"
  deadline=$((SECONDS + 90))
  while kill -0 "${pressure_writer_pid}" 2>/dev/null && ((SECONDS < deadline)); do
    sleep 1
  done
  if kill -0 "${pressure_writer_pid}" 2>/dev/null; then
    kill "${pressure_writer_pid}" >/dev/null 2>&1 || true
    wait "${pressure_writer_pid}" >/dev/null 2>&1 || true
    pressure_writer_pid=""
    echo "journal-pressure writer did not wake within 90 seconds" >&2
    cat "${index_pressure_writer_output}" >&2
    return 1
  fi
  local completed_writer_pid="${pressure_writer_pid}"
  pressure_writer_pid=""
  if ! wait "${completed_writer_pid}"; then
    echo "journal-pressure writer failed after capacity was released" >&2
    cat "${index_pressure_writer_output}" >&2
    return 1
  fi
  run_index_recovery_qualification \
    pressure-verify "$(IFS=,; echo "${public_endpoints[*]}")" \
    qindex-pressure qindex-pressure-client "${index_pressure_secret}" \
    "${index_pressure_state}"

  for node in keldra-1 keldra-2 keldra-3; do
    pressure_log="${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-${node}.log"
    save_log_suffix "${node}" "${pressure_cursor}" "${pressure_log}"
    if grep -Fq 'index snapshot rebuild started' "${pressure_log}"; then
      echo "${node} started an unauthorized automatic snapshot rebuild while catching up" >&2
      return 1
    fi
  done
  if ! awk '
    index($0, "distributed object mutation capacity wait finished") &&
    index($0, "capacity=\"source_journal\"") &&
    index($0, "backpressure.outcome=\"capacity_available\"") { found = 1 }
    END { exit !found }
  ' "${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-${capacity_node}.log"
  then
    echo "${capacity_node} did not report that source-journal capacity released its writer" >&2
    return 1
  fi
  echo "[keldra-qualification] hard source-journal capacity kept a public write pending and uncommitted, then woke it and reached an exact incremental index generation"
}

assert_zero_accounting_traffic_drops() {
  local batches
  local bytes
  local count
  local expected_node_id
  local line
  local node
  local node_id
  for node in keldra-1 keldra-2 keldra-3; do
    count=0
    expected_node_id="${node#keldra-}"
    while IFS= read -r line; do
      node_id="$(log_unsigned_field node_id "${line}")" || {
        echo "${node} accounting drop evidence omitted node_id" >&2
        return 1
      }
      batches="$(log_unsigned_field dropped_batches_total "${line}")" || {
        echo "${node} accounting drop evidence omitted dropped_batches_total" >&2
        return 1
      }
      bytes="$(log_unsigned_field dropped_bytes_total "${line}")" || {
        echo "${node} accounting drop evidence omitted dropped_bytes_total" >&2
        return 1
      }
      if [[ "${node_id}" != "${expected_node_id}" ]] \
        || ((batches != 0 || bytes != 0)); then
        echo "${node} reported node=${node_id} dropped_batches_total=${batches} dropped_bytes_total=${bytes}" >&2
        return 1
      fi
      count=$((count + 1))
    done < <(
      service_logs "${node}" \
        | grep -F 'keldra_accounting_traffic_drop_state' || true
    )
    if ((count == 0)); then
      echo "${node} emitted no accounting drop-state evidence" >&2
      return 1
    fi
  done
  echo "[keldra-qualification] accounting traffic reported zero dropped batches and bytes on every node"
}

wait_for_bootstrap
assert_sparse_index_startup keldra-1 1
qprobe_secret=qualification-probe-secret-000000000000000000000000
provision_tenant qprobe qprobe-client "${qprobe_secret}"
create_bucket keldra-1 qprobe-client \
  "${qprobe_secret}" objects

# Every index below is created while node 1 is the only ACTIVE member. The
# three-node phase later mutates the same definitions and requires publication
# from a newly selected builder.
index_membership_secret=qualification-index-membership-secret-0000000000000000
provision_tenant \
  qindex-membership qindex-membership-client "${index_membership_secret}"
run_index_recovery_qualification \
  membership-seed "$(public_endpoint_for keldra-1)" \
  qindex-membership qindex-membership-client "${index_membership_secret}" \
  "${index_membership_state}"
test -s "${index_membership_state}"
echo "[keldra-qualification] one-node index assignment fixtures seeded before online growth"

require_qprobe_head() {
  local node="$1"
  local path="$2"
  local expected="$3"
  local actual
  actual="$(run_cli "${node}" qprobe-client "${qprobe_secret}" \
    head qprobe objects "${path}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${node} changed the object head for ${path} during cluster growth" >&2
    echo "expected: ${expected}" >&2
    echo "actual:   ${actual}" >&2
    return 1
  fi
}

head_blake3() {
  local head="$1"
  local hash
  hash="$(sed -n \
    's/^present version=[0-9][0-9]* bytes=[0-9][0-9]* blake3=\([0-9a-f]\{64\}\)$/\1/p' \
    <<<"${head}")"
  if [[ -z "${hash}" ]]; then
    echo "Head returned an invalid present-object identity: ${head}" >&2
    return 1
  fi
  printf '%s\n' "${hash}"
}

complete_blob_path() {
  local hash="$1"
  printf '/var/lib/keldra/blobs/%s/%s\n' "${hash:0:2}" "${hash}"
}

move_complete_blob() {
  local node="$1"
  local hash="$2"
  local path
  path="$(complete_blob_path "${hash}")"
  compose exec -T --user 0 "${node}" test -f "${path}"
  compose exec -T --user 0 "${node}" test ! -e "${path}.qualification-away"
  compose exec -T --user 0 "${node}" \
    mv -- "${path}" "${path}.qualification-away"
}

restore_complete_blob() {
  local node="$1"
  local hash="$2"
  local path
  path="$(complete_blob_path "${hash}")"
  compose exec -T --user 0 "${node}" test -f "${path}.qualification-away"
  compose exec -T --user 0 "${node}" \
    mv -- "${path}.qualification-away" "${path}"
}

shard_path_on_node() {
  local node="$1"
  local hash="$2"
  local directory="/var/lib/keldra/blobs/${hash:0:2}"
  local -a paths=()
  mapfile -t paths < <(
    compose exec -T --user 0 "${node}" \
      find "${directory}" -maxdepth 1 -type f \
        -name "0001${hash}*" ! -name '*.qualification-away' -print
  )
  if ((${#paths[@]} != 1)); then
    echo "expected exactly one shard for ${hash} on ${node}, found ${#paths[@]}" >&2
    return 1
  fi
  printf '%s\n' "${paths[0]}"
}

move_shard() {
  local node="$1"
  local hash="$2"
  local path
  path="$(shard_path_on_node "${node}" "${hash}")"
  compose exec -T --user 0 "${node}" test ! -e "${path}.qualification-away"
  compose exec -T --user 0 "${node}" \
    mv -- "${path}" "${path}.qualification-away"
  printf '%s\n' "${path}"
}

restore_shard() {
  local node="$1"
  local path="$2"
  compose exec -T --user 0 "${node}" test -f "${path}.qualification-away"
  compose exec -T --user 0 "${node}" \
    mv -- "${path}.qualification-away" "${path}"
}

# Exercise the exact online growth path with a payload that cannot use the
# inline RocksDB representation. The object is created before either joining
# node exists and must remain readable after both membership cutovers.
dd if=/dev/zero \
  of="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  bs=1M count=2 status=none
cp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin"
printf '\177' | dd \
  of="${KELDRA_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin" \
  bs=1 seek=0 count=1 conv=notrunc status=none
chmod 0444 \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin"
expect_failure "one-node REPLICATED large Put" \
  run_cli keldra-1 qprobe-client "${qprobe_secret}" \
    put qprobe objects growth/replicated-must-fail.bin \
      /qualification/artifacts/one-node-replicated-rejected.bin \
      --command-id qprobe-one-node-replicated-rejected \
      --durability replicated --if-absent
rejected_head="$(run_cli keldra-1 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/replicated-must-fail.bin)"
if [[ "${rejected_head}" != "never-existed" ]]; then
  echo "failed one-node REPLICATED Put published an object head: ${rejected_head}" >&2
  exit 1
fi
echo "[keldra-qualification] one-node REPLICATED large Put failed closed without a head"
run_cli keldra-1 qprobe-client \
  "${qprobe_secret}" \
  put qprobe objects growth/from-one.bin \
    /qualification/artifacts/growth-large.bin \
    --command-id qprobe-growth-one --durability local >/dev/null
growth_one_head="$(run_cli keldra-1 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-one.bin)"
run_cli keldra-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
echo "[keldra-qualification] one-node large-object read passed"

# Restart the exact installation that will grow. This proves the durable
# one-node representation and reference-journal recovery before ADD begins.
compose restart keldra-1
wait_for_node keldra-1
rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
run_cli keldra-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
require_qprobe_head keldra-1 growth/from-one.bin "${growth_one_head}"
echo "[keldra-qualification] one-node large object survived restart before growth"

prepare_and_start_node 2
membership_two_endpoints="$(public_endpoint_for keldra-1),$(public_endpoint_for keldra-2)"
run_live_builder_reassignment_qualification \
  2 "${membership_two_endpoints}" keldra-2

growth_one_two_node_head="$(run_cli keldra-2 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-one.bin)"
if [[ "${growth_one_two_node_head}" != "${growth_one_head}" ]]; then
  echo "node 2 observed another head for the one-node object after ADD" >&2
  echo "expected: ${growth_one_head}" >&2
  echo "actual:   ${growth_one_two_node_head}" >&2
  exit 1
fi
growth_one_hash="$(head_blake3 "${growth_one_two_node_head}")"
move_complete_blob keldra-1 "${growth_one_hash}"
rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
run_cli keldra-2 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
restore_complete_blob keldra-1 "${growth_one_hash}"
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
require_qprobe_head keldra-2 growth/from-one.bin "${growth_one_head}"
echo "[keldra-qualification] two-node read succeeded without node 1's complete blob"

# Use a different content identity so this is a real two-node payload write,
# not a second logical reference to the preexisting deduplicated blob.
cp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
chmod 0644 "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
printf '\001' | dd \
  of="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" \
  bs=1 seek=0 count=1 conv=notrunc status=none
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
run_cli keldra-2 qprobe-client \
  "${qprobe_secret}" \
  put qprobe objects growth/from-two.bin \
    /qualification/artifacts/growth-two-large.bin \
    --command-id qprobe-growth-two --durability replicated >/dev/null
growth_two_head="$(run_cli keldra-2 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-two.bin)"
growth_two_hash="$(head_blake3 "${growth_two_head}")"
move_complete_blob keldra-2 "${growth_two_hash}"
run_cli keldra-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-two.bin \
    --output /qualification/artifacts/growth-two-read.bin
restore_complete_blob keldra-2 "${growth_two_hash}"
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-read.bin"
require_qprobe_head keldra-1 growth/from-two.bin "${growth_two_head}"
echo "[keldra-qualification] two-node REPLICATED read succeeded without its ingress copy"

start_source_journal_phase "${pressure_source_journal_max_entries}" keldra-1 keldra-2
echo "[keldra-qualification] topology handoff passed; cutover and pressure phases use source-journal max entries ${pressure_source_journal_max_entries}"
prepare_no_event_membership_cutover_qualification keldra-2 2 qprobe-client "${qprobe_secret}" qprobe objects "${pressure_source_journal_max_entries}"
prepare_joining_node 3
prepare_indexed_membership_cutover_qualification \
  keldra-1 1 keldra-2 qindex-membership \
  qindex-membership-client "${index_membership_secret}" \
  "${index_membership_state}" "${pressure_source_journal_max_entries}"
start_prepared_node_during_indexed_cutover 3
qualify_no_event_membership_cutover keldra-2 2 qprobe-client "${qprobe_secret}" qprobe objects "${pressure_source_journal_max_entries}"
membership_three_endpoints="$(public_endpoint_for keldra-1),$(public_endpoint_for keldra-2),$(public_endpoint_for keldra-3)"
run_live_builder_reassignment_qualification \
  3 "${membership_three_endpoints}" keldra-3

declare -a moved_complete_blobs=()
for growth_node in keldra-1 keldra-2 keldra-3; do
  for growth_hash in "${growth_one_hash}" "${growth_two_hash}"; do
    growth_complete_path="$(complete_blob_path "${growth_hash}")"
    if compose exec -T --user 0 "${growth_node}" test -f "${growth_complete_path}"; then
      move_complete_blob "${growth_node}" "${growth_hash}"
      moved_complete_blobs+=("${growth_node} ${growth_hash}")
    else
      compose exec -T --user 0 "${growth_node}" \
        test ! -e "${growth_complete_path}.qualification-away"
    fi
  done
done

for unavailable_node in keldra-1 keldra-2 keldra-3; do
  declare -a moved_shards=()
  for growth_hash in "${growth_one_hash}" "${growth_two_hash}"; do
    moved_shards+=("$(move_shard "${unavailable_node}" "${growth_hash}")")
  done
  for growth_object in from-one from-two; do
    case "${growth_object}" in
      from-one) growth_expected="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" ;;
      from-two) growth_expected="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" ;;
    esac
    growth_output="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    rm -f "${growth_output}"
    run_cli keldra-1 qprobe-client \
      "${qprobe_secret}" \
      get qprobe objects "growth/${growth_object}.bin" \
        --output "/qualification/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    cmp "${growth_expected}" "${growth_output}"
    case "${growth_object}" in
      from-one) growth_expected_head="${growth_one_head}" ;;
      from-two) growth_expected_head="${growth_two_head}" ;;
    esac
    require_qprobe_head \
      keldra-1 "growth/${growth_object}.bin" "${growth_expected_head}"
  done
  for moved_shard in "${moved_shards[@]}"; do
    restore_shard "${unavailable_node}" "${moved_shard}"
  done
done
for moved_complete_blob in "${moved_complete_blobs[@]}"; do
  read -r growth_node growth_hash <<<"${moved_complete_blob}"
  restore_complete_blob "${growth_node}" "${growth_hash}"
done
echo "[keldra-qualification] three-node 2+1 reads preserved both large object heads and bytes without complete copies after every one-shard loss"

echo "[keldra-qualification] three-node cluster is ACTIVE"
case "${index_resource_scope}" in
  release-corpus)
    echo "[keldra-qualification] index resource scope=release-corpus records=839980 indexed_fields=12"
    ;;
  smoke)
    echo "[keldra-qualification] index resource scope=smoke records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
  custom)
    echo "[keldra-qualification] index resource scope=custom records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
esac

public_endpoints=()
for index_node in keldra-1 keldra-2 keldra-3; do
  public_endpoints+=("$(public_endpoint_for "${index_node}")")
done

index_pressure_secret=qualification-index-pressure-secret-000000000000000000
provision_tenant qindex-pressure qindex-pressure-client "${index_pressure_secret}"
run_journal_pressure_qualification
for pressure_node in keldra-1 keldra-2 keldra-3; do
  preserve_qualification_log \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/index-pressure-${pressure_node}.log" \
    "${journal_pressure_evidence_prefix}-${pressure_node}.log"
done
echo "[keldra-qualification] preserved journal-pressure evidence ${journal_pressure_evidence_prefix}-keldra-{1,2,3}.log"
start_release_source_journal_phase "${release_source_journal_max_entries}"

index_secret=qualification-index-secret-00000000000000000000000
provision_tenant qindex qindex-client "${index_secret}"
capture_index_qualification_log_start
KELDRA_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
KELDRA_INDEX_QUALIFICATION_TENANT=qindex \
KELDRA_INDEX_QUALIFICATION_CLIENT_ID=qindex-client \
KELDRA_INDEX_QUALIFICATION_CLIENT_SECRET="${index_secret}" \
KELDRA_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE=1 \
KELDRA_INDEX_QUALIFICATION_STATE_OUTPUT="${index_verification_state}" \
  "${qualification_binaries[cluster_index_qualification]}"
test -s "${index_verification_state}"
save_index_qualification_logs
echo "[keldra-qualification] distributed index qualification passed"
assert_one_builder_published_and_compacted_each_index_kind

configure_three_node_resource_qualification
if [[ "${qualification_mode}" == "release" ]]; then
  run_scale_baseline_resource_qualification three
fi
run_exact_resource_scale_qualification three
verify_index_resource_state "${public_endpoints[0]}"

accounting_secret=qualification-accounting-secret-000000000000000000000
provision_tenant qaccounting qaccounting-client "${accounting_secret}"
KELDRA_ACCOUNTING_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
KELDRA_ACCOUNTING_QUALIFICATION_TENANT=qaccounting \
KELDRA_ACCOUNTING_QUALIFICATION_BUCKET="accounting-three-${$}" \
KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_ID=qaccounting-client \
KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_SECRET="${accounting_secret}" \
  "${qualification_binaries[accounting_qualification]}"
echo "[keldra-qualification] distributed accounting qualification passed"

personaldb_secret=qualification-personaldb-secret-0000000000000000000
provision_tenant qpersonaldb qpersonaldb-client "${personaldb_secret}"
KELDRA_PERSONALDB_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
KELDRA_PERSONALDB_QUALIFICATION_TENANT=qpersonaldb \
KELDRA_PERSONALDB_QUALIFICATION_CLIENT_ID=qpersonaldb-client \
KELDRA_PERSONALDB_QUALIFICATION_CLIENT_SECRET="${personaldb_secret}" \
  "${qualification_binaries[personaldb_qualification]}"
echo "[keldra-qualification] distributed PersonalDB qualification passed"

s3_secret=qualification-s3-secret-00000000000000000000000000
provision_tenant qs3 qs3-client "${s3_secret}"
KELDRA_S3_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
KELDRA_S3_QUALIFICATION_CLIENT_ID=qs3-client \
KELDRA_S3_QUALIFICATION_CLIENT_SECRET="${s3_secret}" \
KELDRA_S3_QUALIFICATION_BUCKET="s3-three-${$}" \
  "${qualification_binaries[s3_qualification]}"
echo "[keldra-qualification] distributed official AWS SDK S3 qualification passed"
run_git_qualification

cas_secret=qualification-cas-secret-000000000000000000000000
provision_tenant qcas qcas-client "${cas_secret}"
create_bucket keldra-2 qcas-client "${cas_secret}" objects
printf 'three-node-cas\n' >"${KELDRA_QUALIFICATION_DIR}/artifacts/cas.txt"
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/cas.txt"
run_cli keldra-1 qcas-client "${cas_secret}" \
  put qcas objects cas/value.txt /qualification/artifacts/cas.txt \
  --command-id qcas-first --if-absent >/dev/null
expect_failure "second PutIfAbsent" \
  run_cli keldra-3 qcas-client "${cas_secret}" \
  put qcas objects cas/value.txt /qualification/artifacts/cas.txt \
  --command-id qcas-second --if-absent
run_cli keldra-2 qcas-client "${cas_secret}" \
  get qcas objects cas/value.txt \
  --output /qualification/artifacts/cas-read.txt
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/cas.txt" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/cas-read.txt"
echo "[keldra-qualification] cross-node CAS test passed"

version_secret=qualification-version-secret-00000000000000000000
provision_tenant qversion qversion-client "${version_secret}"
run_cli keldra-2 qversion-client "${version_secret}" \
  create-bucket objects --versioning enabled \
  | grep -Fq "bucket=objects versioning=enabled"
printf 'retained-version-one\n' \
  >"${KELDRA_QUALIFICATION_DIR}/artifacts/version-one.txt"
printf 'retained-version-two\n' \
  >"${KELDRA_QUALIFICATION_DIR}/artifacts/version-two.txt"
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/version-"*.txt
version_one="$(run_cli keldra-1 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-one.txt \
  --command-id qversion-one --durability replicated)"
version_two="$(run_cli keldra-3 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-two.txt \
  --command-id qversion-two --durability replicated)"
if [[ ! "${version_one}" =~ ^[1-9][0-9]*$ || ! "${version_two}" =~ ^[1-9][0-9]*$ ]]; then
  echo "distributed puts returned invalid versions: ${version_one}, ${version_two}" >&2
  exit 1
fi
old_delete="$(run_cli keldra-2 qversion-client "${version_secret}" \
  delete-version qversion objects retained/value.txt "${version_one}" --durability replicated)"
if [[ "${old_delete}" != 'deleted=true replacement_tombstone_version=none' ]]; then
  echo "distributed historical DeleteVersion returned: ${old_delete}" >&2
  exit 1
fi
run_cli keldra-1 qversion-client "${version_secret}" \
  get qversion objects retained/value.txt \
  --output /qualification/artifacts/version-current.txt
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/version-two.txt" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/version-current.txt"
current_delete="$(run_cli keldra-3 qversion-client "${version_secret}" \
  delete-version qversion objects retained/value.txt "${version_two}" --durability replicated)"
if [[ ! "${current_delete}" =~ ^deleted=true\ replacement_tombstone_version=([1-9][0-9]*)$ ]]; then
  echo "distributed current DeleteVersion returned: ${current_delete}" >&2
  exit 1
fi
replacement_tombstone_version="${BASH_REMATCH[1]}"
for version_node in keldra-1 keldra-2 keldra-3; do
  version_head="$(run_cli "${version_node}" qversion-client "${version_secret}" \
    head qversion objects retained/value.txt)"
  if [[ "${version_head}" != "deleted version=${replacement_tombstone_version}" ]]; then
    echo "${version_node} did not observe the fresh tombstone" >&2
    exit 1
  fi
done
echo "[keldra-qualification] distributed retained-version deletion test passed"

list_secret=qualification-list-secret-00000000000000000000000
provision_tenant qlist qlist-client "${list_secret}"
create_bucket keldra-3 qlist-client "${list_secret}" objects
printf 'cluster-list\n' >"${KELDRA_QUALIFICATION_DIR}/artifacts/list.txt"
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/list.txt"
for item in alpha bravo charlie delta; do
  case "${item}" in
    alpha) list_node=keldra-1 ;;
    bravo) list_node=keldra-2 ;;
    charlie|delta) list_node=keldra-3 ;;
  esac
  run_cli "${list_node}" qlist-client "${list_secret}" \
    put qlist objects "prefix/${item}.txt" /qualification/artifacts/list.txt \
    --command-id "qlist-${item}" --durability replicated >/dev/null
done
expected_list=$'prefix/alpha.txt\nprefix/bravo.txt\nprefix/charlie.txt\nprefix/delta.txt'
for list_node in keldra-1 keldra-2 keldra-3; do
  actual_list="$(run_cli "${list_node}" qlist-client "${list_secret}" \
    list qlist objects --prefix prefix/ --limit 100)"
  if [[ "${actual_list}" != "${expected_list}" ]]; then
    echo "${list_node} returned an incorrect distributed lexical list" >&2
    printf 'expected:\n%s\nactual:\n%s\n' "${expected_list}" "${actual_list}" >&2
    exit 1
  fi
done
page_one="$(run_cli keldra-2 qlist-client "${list_secret}" \
  list qlist objects --prefix prefix/ --limit 2 2>/dev/null)"
page_two="$(run_cli keldra-1 qlist-client "${list_secret}" \
  list qlist objects --prefix prefix/ --start-after prefix/bravo.txt --limit 2)"
if [[ "${page_one}" != $'prefix/alpha.txt\nprefix/bravo.txt' \
  || "${page_two}" != $'prefix/charlie.txt\nprefix/delta.txt' ]]; then
  echo "distributed ListObjects pagination is incorrect" >&2
  exit 1
fi
echo "[keldra-qualification] distributed listing and pagination test passed"

watch_paths="$(run_cli keldra-3 qlist-client "${list_secret}" \
  watch qlist objects --prefix prefix --retained --events 4 \
  --idle-timeout-seconds 30 \
  | cut -f2 | sort)"
if [[ "${watch_paths}" != "${expected_list}" ]]; then
  echo "distributed WatchPrefix did not replay the four retained paths" >&2
  printf 'expected:\n%s\nactual:\n%s\n' "${expected_list}" "${watch_paths}" >&2
  exit 1
fi
echo "[keldra-qualification] distributed retained watch test passed"

atomic_secret=qualification-atomic-secret-000000000000000000000
provision_tenant qatomic qatomic-client "${atomic_secret}"
run_atomic_index_qualification

ec_secret=qualification-ec-secret-0000000000000000000000000
provision_tenant qec qec-client "${ec_secret}"
create_bucket keldra-3 qec-client "${ec_secret}" objects
dd if=/dev/urandom of="${KELDRA_QUALIFICATION_DIR}/artifacts/large.bin" \
  bs=1M count=2 status=none
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/large.bin"
run_cli keldra-2 qec-client "${ec_secret}" \
  put qec objects ec/large.bin /qualification/artifacts/large.bin \
  --command-id qec-replicated --durability replicated >/dev/null
run_cli keldra-1 qec-client "${ec_secret}" \
  get qec objects ec/large.bin \
  --output /qualification/artifacts/large-read.bin
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/large-read.bin"
echo "[keldra-qualification] 2+1 replicated payload test passed"

restart_secret=qualification-restart-secret-000000000000000000000
provision_tenant qrestart qrestart-client "${restart_secret}"
create_bucket keldra-1 qrestart-client "${restart_secret}" objects
printf 'survives-rolling-restart\n' \
  >"${KELDRA_QUALIFICATION_DIR}/artifacts/restart.txt"
chmod 0444 "${KELDRA_QUALIFICATION_DIR}/artifacts/restart.txt"
run_cli keldra-3 qrestart-client "${restart_secret}" \
  put qrestart objects restart/value.txt /qualification/artifacts/restart.txt \
  --command-id qrestart-seed --durability replicated >/dev/null
for node in keldra-1 keldra-2 keldra-3; do
  sparse_starts_before="$(index_sparse_start_count "${node}")"
  populated_restart_started="${SECONDS}"
  compose restart "${node}"
  wait_for_node "${node}"
  if ((SECONDS - populated_restart_started > 30)); then
    echo "${node} populated restart exceeded 30 seconds" >&2
    exit 1
  fi
  service_logs "${node}" | preserve_startup_scan_evidence \
    "/var/tmp/keldra-v090-three-startup-scans-${qualification_suffix}-${node}.log"
  assert_sparse_index_startup "${node}" "$((sparse_starts_before + 1))"
  rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  run_cli "${node}" qrestart-client "${restart_secret}" \
    get qrestart objects restart/value.txt \
    --output /qualification/artifacts/restart-read.txt
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/restart.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  verify_existing_indexes
  verify_index_resource_state "$(public_endpoint_for "${node}")"
  echo "[keldra-qualification] ${node} restart preserved every final complete index generation through all public endpoints"
  for growth_object in from-one from-two; do
    case "${growth_object}" in
      from-one)
        growth_expected="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin"
        growth_expected_head="${growth_one_head}"
        ;;
      from-two)
        growth_expected="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
        growth_expected_head="${growth_two_head}"
        ;;
    esac
    growth_output="${KELDRA_QUALIFICATION_DIR}/artifacts/restart-${node}-${growth_object}.bin"
    rm -f "${growth_output}"
    run_cli "${node}" qprobe-client "${qprobe_secret}" \
      get qprobe objects "growth/${growth_object}.bin" \
        --output "/qualification/artifacts/restart-${node}-${growth_object}.bin"
    cmp "${growth_expected}" "${growth_output}"
    require_qprobe_head \
      "${node}" "growth/${growth_object}.bin" "${growth_expected_head}"
  done
done
echo "[keldra-qualification] rolling populated restart preserved objects and indexes; sparse startup markers were present and no legacy definition barrier was reported"
assert_index_retention_converged
assert_zero_accounting_traffic_drops

if [[ "${qualification_mode}" == "release" ]]; then
  echo "[keldra-qualification] PASS scope=release-corpus records=${index_resource_records} image=${image_id} platform=${KELDRA_DOCKER_PLATFORM}"
else
  echo "[keldra-qualification] SMOKE PASS records=${index_resource_records} image=${image_id} platform=${KELDRA_DOCKER_PLATFORM}"
fi
