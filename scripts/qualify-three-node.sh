#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/tests/cluster/docker-compose.yml"
start_node="${repo_root}/tests/cluster/start-node.sh"
requested_image="${ANVIL_IMAGE:-anvil:0.7.0}"
qualification_mode="${ANVIL_QUALIFICATION_MODE:-smoke}"
index_disk_cache_bytes="${ANVIL_QUALIFICATION_INDEX_DISK_CACHE_BYTES:-268435456}"
index_memory_percent="${ANVIL_QUALIFICATION_INDEX_MEMORY_PERCENT:-5}"
index_kind_budget_bytes="${ANVIL_QUALIFICATION_INDEX_KIND_BUDGET_BYTES:-67108864}"
index_rayon_workers="${ANVIL_QUALIFICATION_INDEX_RAYON_WORKERS:-2}"
# The default is a fast smoke. Set this to 839980 for the full
# production-shaped, twelve-field corpus used by the resource qualification.
case "${qualification_mode}" in
  release) index_resource_records="${ANVIL_QUALIFICATION_INDEX_RECORDS:-839980}" ;;
  smoke) index_resource_records="${ANVIL_QUALIFICATION_INDEX_RECORDS:-16384}" ;;
  *)
    echo "ANVIL_QUALIFICATION_MODE must be release or smoke" >&2
    exit 2
    ;;
esac
index_resource_mutations="${ANVIL_QUALIFICATION_INDEX_MUTATIONS:-512}"
index_resource_max_anonymous_growth_bytes="${ANVIL_QUALIFICATION_INDEX_MAX_ANONYMOUS_GROWTH_BYTES:-1073741824}"
index_kinds=(Path MetadataFilter TypedJson FullText Vector Hybrid GitSource Tensor)

for configured_limit in \
  "${index_disk_cache_bytes}" \
  "${index_memory_percent}" \
  "${index_kind_budget_bytes}" \
  "${index_rayon_workers}" \
  "${index_resource_records}" \
  "${index_resource_mutations}" \
  "${index_resource_max_anonymous_growth_bytes}"
do
  if [[ ! "${configured_limit}" =~ ^[1-9][0-9]*$ ]]; then
    echo "index qualification limits must be positive decimal integers" >&2
    exit 2
  fi
done
if ((index_memory_percent > 100)); then
  echo "ANVIL_QUALIFICATION_INDEX_MEMORY_PERCENT must not exceed 100" >&2
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
export ANVIL_QUALIFICATION_INDEX_DISK_CACHE_BYTES="${index_disk_cache_bytes}"
export ANVIL_QUALIFICATION_INDEX_MEMORY_PERCENT="${index_memory_percent}"
export ANVIL_QUALIFICATION_INDEX_KIND_BUDGET_BYTES="${index_kind_budget_bytes}"
export ANVIL_QUALIFICATION_INDEX_RAYON_WORKERS="${index_rayon_workers}"
export ANVIL_QUALIFICATION_RUST_LOG=info,anvil::index_runtime::retention=debug

case "${ANVIL_DOCKER_PLATFORM:-}" in
  "")
    case "$(uname -m)" in
      x86_64|amd64) export ANVIL_DOCKER_PLATFORM=linux/amd64 ;;
      aarch64|arm64) export ANVIL_DOCKER_PLATFORM=linux/arm64 ;;
      *)
        echo "unsupported host architecture: $(uname -m)" >&2
        exit 2
        ;;
    esac
    ;;
  linux/amd64|linux/arm64) ;;
  *)
    echo "unsupported ANVIL_DOCKER_PLATFORM=${ANVIL_DOCKER_PLATFORM}" >&2
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
command -v git >/dev/null 2>&1 || {
  echo "git is required for the smart HTTP gateway qualification" >&2
  exit 2
}
docker compose version >/dev/null

image_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}")"
server_version="$(
  docker run --rm --platform "${ANVIL_DOCKER_PLATFORM}" \
    "${image_id}" anvil-server --version
)"
client_version="$(
  docker run --rm --platform "${ANVIL_DOCKER_PLATFORM}" \
    "${image_id}" anvil --version
)"
if [[ "${server_version}" != "anvil-server 0.7.0" \
  || "${client_version}" != "anvil 0.7.0" ]]; then
  echo "qualification requires the exact Anvil 0.7.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
export ANVIL_IMAGE="${image_id}"
export ANVIL_QUALIFICATION_PROJECT="${ANVIL_QUALIFICATION_PROJECT:-anvil-v070-${$}}"
export ANVIL_QUALIFICATION_DIR="$(mktemp -d /var/tmp/anvil-v070-qualification.XXXXXX)"
export ANVIL_QUALIFICATION_START_NODE="${start_node}"
qualification_suffix="${ANVIL_QUALIFICATION_DIR##*.}"
index_verification_state="${ANVIL_QUALIFICATION_DIR}/artifacts/index-verification-state.json"
index_membership_state="${ANVIL_QUALIFICATION_DIR}/artifacts/index-membership-state.json"
index_gap_state="${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-state.json"
index_resource_report="/var/tmp/anvil-v070-three-index-resource-${qualification_suffix}.json"
keep="${ANVIL_QUALIFICATION_KEEP:-0}"
paused_container=""

compose() {
  docker compose \
    --project-name "${ANVIL_QUALIFICATION_PROJECT}" \
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
  if [[ -n "${paused_container}" ]] \
    && docker inspect --format '{{.State.Paused}}' "${paused_container}" 2>/dev/null \
      | grep -Fxq true
  then
    docker unpause "${paused_container}" >/dev/null 2>&1 || true
  fi
  if ((status != 0)); then
    echo "[anvil-qualification] FAILED; container status and logs follow" >&2
    compose ps --all >&2 || true
    compose logs --no-color >&2 || true
  fi
  if [[ "${keep}" == "1" ]]; then
    echo "[anvil-qualification] retained project ${ANVIL_QUALIFICATION_PROJECT}" >&2
    echo "[anvil-qualification] retained files ${ANVIL_QUALIFICATION_DIR}" >&2
  else
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [[ "${ANVIL_QUALIFICATION_DIR}" == /var/tmp/anvil-v070-qualification.* ]]; then
      docker run --rm --user 0 \
        --volume "${ANVIL_QUALIFICATION_DIR}:/qualification" \
        "${image_id}" rm -rf \
          /qualification/node-1 \
          /qualification/node-2 \
          /qualification/node-3 \
          /qualification/artifacts \
          /qualification/token-signing-key >/dev/null 2>&1 || true
      rm -rf -- "${ANVIL_QUALIFICATION_DIR}"
    else
      echo "refusing to remove unexpected qualification path ${ANVIL_QUALIFICATION_DIR}" >&2
      status=1
    fi
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

server_help="$(docker run --rm "${image_id}" anvil-server --help)"
for required in --peer-listen --peer-advertise --join-bundle; do
  if ! grep -Fq -- "${required}" <<<"${server_help}"; then
    echo "qualification image is missing required server option ${required}" >&2
    exit 1
  fi
done
cli_help="$(docker run --rm "${image_id}" anvil --help)"
for required in prepare-node provision-tenant create-bucket; do
  if ! grep -Fq -- "${required}" <<<"${cli_help}"; then
    echo "qualification image is missing required CLI command ${required}" >&2
    exit 1
  fi
done

for directory in node-1 node-2 node-3 artifacts; do
  mkdir "${ANVIL_QUALIFICATION_DIR}/${directory}"
  chmod 0777 "${ANVIL_QUALIFICATION_DIR}/${directory}"
done
chmod 0755 "${ANVIL_QUALIFICATION_DIR}"
head -c 64 /dev/urandom >"${ANVIL_QUALIFICATION_DIR}/token-signing-key"
chmod 0600 "${ANVIL_QUALIFICATION_DIR}/token-signing-key"
docker run --rm --user 0 \
  --volume "${ANVIL_QUALIFICATION_DIR}/token-signing-key:/qualification-key" \
  "${image_id}" chown 10001:10001 /qualification-key

compose config --quiet
compose up --detach anvil-1
require_service_image anvil-1 "${image_id}" candidate

network="${ANVIL_QUALIFICATION_PROJECT}_default"

run_cli() {
  local node="$1"
  local client_id="$2"
  local client_secret="$3"
  shift 3
  docker run --rm \
    --network "${network}" \
    --volume "${ANVIL_QUALIFICATION_DIR}:/qualification" \
    --env "ANVIL_CLIENT_ID=${client_id}" \
    --env "ANVIL_CLIENT_SECRET=${client_secret}" \
    "${image_id}" \
    anvil --endpoint "http://${node}:50051" "$@"
}

run_bootstrap_cli() {
  local node="$1"
  shift
  local -a secret_environment=()
  if [[ -n "${ANVIL_NEW_CLIENT_SECRET:-}" ]]; then
    secret_environment=(--env ANVIL_NEW_CLIENT_SECRET)
  fi
  docker run --rm \
    --network "${network}" \
    --volume "${ANVIL_QUALIFICATION_DIR}:/qualification" \
    "${secret_environment[@]}" \
    "${image_id}" \
    anvil --endpoint "http://${node}:50051" \
      --credentials-file /qualification/node-1/system-bootstrap-credential.json "$@"
}

wait_for_bootstrap() {
  local attempt
  for attempt in $(seq 1 60); do
    if compose exec -T anvil-1 \
      test -f /var/lib/anvil/system-bootstrap-credential.json \
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

strip_ansi() {
  LC_ALL=C sed $'s/\033\\[[0-9;?]*[ -\\/]*[@-~]//g'
}

service_logs() {
  docker logs "$(service_container "$1")" 2>&1 | strip_ansi
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
  ANVIL_INDEX_RECOVERY_QUALIFICATION_MODE="${mode}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_ENDPOINTS="${endpoints}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_CLIENT_ID="${client_id}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_CLIENT_SECRET="${client_secret}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_STATE="${state_path}" \
  ANVIL_INDEX_RECOVERY_QUALIFICATION_BUCKET="${bucket}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example index_recovery_qualification
}

log_line_count() {
  { service_logs "$1" || true; } | wc -l
}

save_log_suffix() {
  local node="$1"
  local start_line="$2"
  local destination="$3"
  service_logs "${node}" | tail -n "+$((start_line + 1))" >"${destination}"
}

state_index_ids() {
  sed -n 's/^[[:space:]]*"index_id":[[:space:]]*\([1-9][0-9]*\),\{0,1\}[[:space:]]*$/\1/p' "$1"
}

state_unsigned_field() {
  local field="$1"
  local state="$2"
  sed -n "s/^[[:space:]]*\"${field}\":[[:space:]]*\([0-9][0-9]*\),\{0,1\}[[:space:]]*$/\1/p" \
    "${state}"
}

log_unsigned_field() {
  local field="$1"
  local line="$2"
  if [[ "${line}" =~ (^|[[:space:]])${field}=([0-9]+)($|[[:space:]]) ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
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
  service_logs "$1" | grep -Fc 'anvil_index_startup_scan_evidence' || true
}

assert_zero_global_startup_scan_evidence() {
  local node="$1"
  local minimum_count="$2"
  local count=0
  local expected_node_id="${node#anvil-}"
  local global_scans
  local line
  local node_id
  local scoped_scans
  while IFS= read -r line; do
    node_id="$(log_unsigned_field node_id "${line}")" || {
      echo "${node} startup scan evidence omitted node_id" >&2
      return 1
    }
    global_scans="$(log_unsigned_field global_head_scans_total "${line}")" || {
      echo "${node} startup scan evidence omitted its measured global scan count" >&2
      return 1
    }
    scoped_scans="$(log_unsigned_field scoped_head_scans_total "${line}")" || {
      echo "${node} startup scan evidence omitted its measured scoped scan count" >&2
      return 1
    }
    if [[ "${node_id}" != "${expected_node_id}" ]] || ((global_scans != 0)); then
      echo "${node} startup reported node=${node_id} global_head_scans_total=${global_scans} scoped_head_scans_total=${scoped_scans}" >&2
      return 1
    fi
    count=$((count + 1))
  done < <(
    service_logs "${node}" \
      | grep -F 'anvil_index_startup_scan_evidence' || true
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
  for node in anvil-1 anvil-2 anvil-3; do
    if ! compose ps --status running --services | grep -Fxq "${node}"; then
      continue
    fi
    if output="$(ANVIL_NEW_CLIENT_SECRET="${client_secret}" \
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

prepare_and_start_node() {
  local node_id="$1"
  local service="anvil-${node_id}"
  local peer_address="${service}:50052"
  local leader
  local output=""
  local bundle_path=""
  local source_service=""
  for leader in anvil-1 anvil-2 anvil-3; do
    if ! compose ps --status running --services | grep -Fxq "${leader}"; then
      continue
    fi
    if output="$(run_bootstrap_cli "${leader}" prepare-node \
      "${node_id}" "${peer_address}" 2>&1)"
    then
      bundle_path="$(sed -n 's/^bundle=\([^ ]*\) .*/\1/p' <<<"${output}")"
      source_service="${leader}"
      break
    fi
  done
  if [[ "${bundle_path}" != "/var/lib/anvil/anvil-node-${node_id}.join.json" ]]; then
    echo "node ${node_id} preparation did not return its expected private bundle" >&2
    echo "last administration output: ${output}" >&2
    return 1
  fi

  local copied="${ANVIL_QUALIFICATION_DIR}/artifacts/anvil-node-${node_id}.join.json"
  compose cp "${source_service}:${bundle_path}" "${copied}"
  chmod 0600 "${copied}"
  docker run --rm --user 0 \
    --volume "${copied}:/join-bundle" \
    "${image_id}" chown 10001:10001 /join-bundle

  compose up --detach "${service}"
  wait_for_node "${service}"
  assert_sparse_index_startup "${service}" 1
  if [[ -e "${copied}" ]]; then
    echo "${service} became ready without consuming and deleting its join bundle" >&2
    return 1
  fi
}

run_index_resource_qualification() {
  local -A resource_log_starts=()
  local containers=()
  local resource_node
  for resource_node in anvil-1 anvil-2 anvil-3; do
    containers+=("$(service_container "${resource_node}")")
    resource_log_starts["${resource_node}"]="$(log_line_count "${resource_node}")"
  done
  ANVIL_V06_RESOURCE_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  ANVIL_V06_RESOURCE_TENANT=qindex-resource \
  ANVIL_V06_RESOURCE_BUCKET="index-resource-${$}" \
  ANVIL_V06_RESOURCE_CLIENT_ID=qindex-resource-client \
  ANVIL_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
  ANVIL_V06_RESOURCE_RECORDS="${index_resource_records}" \
  ANVIL_V06_RESOURCE_MUTATIONS="${index_resource_mutations}" \
  ANVIL_V06_RESOURCE_BATCH_SIZE=256 \
  ANVIL_V06_RESOURCE_WORKERS=6 \
  ANVIL_V06_RESOURCE_CONTAINERS="$(IFS=,; echo "${containers[*]}")" \
  ANVIL_V06_REQUIRE_RESOURCE_TARGETS=1 \
  ANVIL_V06_KIND_BUDGET_BYTES="${index_kind_budget_bytes}" \
  ANVIL_V06_INDEX_RAYON_WORKERS="${index_rayon_workers}" \
  ANVIL_V06_MAX_ANONYMOUS_GROWTH_BYTES="${index_resource_max_anonymous_growth_bytes}" \
  ANVIL_V06_RESOURCE_OUTPUT="${index_resource_report}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example v06_index_resource_qualification >/dev/null
  for resource_node in anvil-1 anvil-2 anvil-3; do
    save_log_suffix \
      "${resource_node}" "${resource_log_starts[${resource_node}]}" \
      "${ANVIL_QUALIFICATION_DIR}/artifacts/index-resource-${resource_node}.log"
  done
  test -s "${index_resource_report}"
  grep -Eq "^[[:space:]]*\"records\":[[:space:]]*${index_resource_records},?[[:space:]]*$" \
    "${index_resource_report}"
  grep -Eq '^[[:space:]]*"indexed_fields":[[:space:]]*12,?[[:space:]]*$' \
    "${index_resource_report}"
  echo "[anvil-qualification] bounded distributed index resource qualification passed scope=${index_resource_scope} records=${index_resource_records} kind_budget=${index_kind_budget_bytes}"
  echo "[anvil-qualification] preserved resource report ${index_resource_report}"
}

assert_index_resource_bounds() {
  local -A observed_kinds=()
  local configured
  local kind
  local line
  local observed=0
  local peak
  local resource_node
  local used
  while IFS= read -r line; do
    if [[ "${line}" =~ index\.kind=(Path|MetadataFilter|TypedJson|FullText|Vector|Hybrid|GitSource|Tensor) ]]; then
      kind="${BASH_REMATCH[1]}"
    else
      continue
    fi
    if [[ "${line}" =~ gauge\.anvil_index_construction_configured_bytes=([0-9]+).*gauge\.anvil_index_construction_used_bytes=([0-9]+).*gauge\.anvil_index_construction_peak_bytes=([0-9]+) ]]; then
      configured="${BASH_REMATCH[1]}"
      used="${BASH_REMATCH[2]}"
      peak="${BASH_REMATCH[3]}"
      if ((configured != index_kind_budget_bytes \
        || used > configured \
        || peak > configured)); then
        echo "distributed index construction exceeded or misstated its configured kind budget" >&2
        printf '%s\n' "${line}" >&2
        return 1
      fi
      observed_kinds["${kind}"]=1
      observed=$((observed + 1))
    fi
  done < <(
    for resource_node in anvil-1 anvil-2 anvil-3; do
      grep -F 'index construction budget state' \
        "${ANVIL_QUALIFICATION_DIR}/artifacts/index-${resource_node}.log" || true
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

  local resource_budget_evidence=0
  while IFS= read -r line; do
    if [[ "${line}" != *"index.kind=TypedJson"* \
      || "${line}" != *"index construction budget state"* ]]; then
      continue
    fi
    if [[ ! "${line}" =~ gauge\.anvil_index_construction_configured_bytes=([0-9]+).*gauge\.anvil_index_construction_used_bytes=([0-9]+).*gauge\.anvil_index_construction_peak_bytes=([0-9]+) ]]; then
      echo "distributed production-shaped TypedJson build emitted malformed budget evidence" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    configured="${BASH_REMATCH[1]}"
    used="${BASH_REMATCH[2]}"
    peak="${BASH_REMATCH[3]}"
    if ((configured != index_kind_budget_bytes \
      || used > configured \
      || peak == 0 \
      || peak > configured)); then
      echo "distributed production-shaped TypedJson build exceeded or misstated its configured kind budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resource_budget_evidence=$((resource_budget_evidence + 1))
  done < <(
    for resource_node in anvil-1 anvil-2 anvil-3; do
      cat "${ANVIL_QUALIFICATION_DIR}/artifacts/index-resource-${resource_node}.log"
    done
  )
  if ((resource_budget_evidence == 0)); then
    echo "distributed production-shaped TypedJson build emitted no fresh construction-budget evidence" >&2
    return 1
  fi

  local cache_bytes
  for resource_node in 1 2 3; do
    cache_bytes="$(find \
      "${ANVIL_QUALIFICATION_DIR}/node-${resource_node}/index-cache" \
      -type f -printf '%s\n' \
      | awk '{ total += $1 } END { print total + 0 }')"
    if ((cache_bytes > index_disk_cache_bytes)); then
      echo "anvil-${resource_node} disposable index cache exceeded its ${index_disk_cache_bytes}-byte budget: ${cache_bytes}" >&2
      return 1
    fi
  done
  echo "[anvil-qualification] distributed index construction and disk caches remained within configured bounds"
}

declare -A index_qualification_log_start=()

capture_index_qualification_log_start() {
  local node
  for node in anvil-1 anvil-2 anvil-3; do
    index_qualification_log_start["${node}"]="$({ service_logs "${node}" || true; } | wc -l)"
  done
}

save_index_qualification_logs() {
  local node
  local start_line
  for node in anvil-1 anvil-2 anvil-3; do
    start_line=$((index_qualification_log_start["${node}"] + 1))
    service_logs "${node}" | tail -n "+${start_line}" \
      >"${ANVIL_QUALIFICATION_DIR}/artifacts/index-${node}.log"
  done
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
    for node in anvil-1 anvil-2 anvil-3; do
      if awk -v kind="index.kind=${kind}" '
            index($0, kind) && index($0, "index generation published") { found = 1 }
            END { exit !found }
          ' "${ANVIL_QUALIFICATION_DIR}/artifacts/index-${node}.log"
      then
        publishers=$((publishers + 1))
        publisher_node="${node}"
      fi
      if awk -v kind="index.kind=${kind}" '
            index($0, kind) && index($0, "index runs compacted") { found = 1 }
            END { exit !found }
          ' "${ANVIL_QUALIFICATION_DIR}/artifacts/index-${node}.log"
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
  done
  echo "[anvil-qualification] all eight kinds consumed all three ingress journals and compacted on their sole builder"
}

verify_existing_indexes() {
  ANVIL_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  ANVIL_INDEX_QUALIFICATION_TENANT=qindex \
  ANVIL_INDEX_QUALIFICATION_CLIENT_ID=qindex-client \
  ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${index_secret}" \
  ANVIL_INDEX_QUALIFICATION_STATE_INPUT="${index_verification_state}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example cluster_index_qualification
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
      for node in anvil-1 anvil-2 anvil-3; do
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
      for node in anvil-1 anvil-2 anvil-3; do
        if service_logs "${node}" | awk -v marker="index.id=${index_id} " '
            index($0, marker) && index($0, "bounded node-wide index retention tick completed") &&
            $0 ~ /monotonic_counter.anvil_index_retention_artifacts_deleted_total=[1-9][0-9]*/ {
              deleted = 1
            }
            deleted && index($0, marker) &&
            index($0, "bounded node-wide index retention tick completed") &&
            $0 ~ /gauge.anvil_index_retention_backlog=0/ {
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
      echo "[anvil-qualification] all eight indexes deleted obsolete artifacts and drained their retention backlog"
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
  local git_root="${ANVIL_QUALIFICATION_DIR}/git"
  local source_repository="${git_root}/source"
  local authenticated_clone="${git_root}/authenticated-clone"
  local denied_clone="${git_root}/denied-clone"
  local public_clone="${git_root}/public-clone"
  local push_url="${public_endpoints[0]}/git/${tenant}/${bucket}/qualification.git"
  local authenticated_clone_url="${public_endpoints[1]}/git/${tenant}/${bucket}/qualification.git"
  local public_clone_url="${public_endpoints[2]}/git/${tenant}/${bucket}/qualification.git"
  local authorization

  provision_tenant "${tenant}" "${client_id}" "${client_secret}"
  create_bucket anvil-1 "${client_id}" "${client_secret}" "${bucket}"

  mkdir -p "${git_root}"
  git init --quiet --initial-branch=main "${source_repository}"
  git -C "${source_repository}" config user.name "Anvil Qualification"
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

  run_cli anvil-3 "${client_id}" "${client_secret}" \
    set-bucket-public-read "${bucket}" enabled >/dev/null
  git clone --quiet --branch main "${public_clone_url}" "${public_clone}"
  cmp "${source_repository}/README.md" "${public_clone}/README.md"

  echo "[anvil-qualification] cross-node Git push, authenticated clone, and public clone passed"
}

run_atomic_index_qualification() {
  ANVIL_ATOMIC_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_TENANT=qatomic \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_BUCKET="atomic-index-three-${$}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_ID=qatomic-client \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_SECRET="${atomic_secret}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example atomic_index_qualification
  echo "[anvil-qualification] distributed atomic-program index visibility passed"
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
    node="anvil-${node_number}"
    log_starts["${node}"]="$(log_line_count "${node}")"
  done
  run_index_recovery_qualification \
    "${membership_mode}" "${endpoints}" \
    qindex-membership qindex-membership-client "${index_membership_secret}" \
    "${index_membership_state}"
  for node_number in $(seq 1 "${active_nodes}"); do
    node="anvil-${node_number}"
    save_log_suffix \
      "${node}" "${log_starts[${node}]}" \
      "${ANVIL_QUALIFICATION_DIR}/artifacts/index-reassignment-${active_nodes}-${node}.log"
  done
  for index_id in "${index_ids[@]}"; do
    if log_has_index_event \
      "${ANVIL_QUALIFICATION_DIR}/artifacts/index-reassignment-${active_nodes}-${new_builder_node}.log" \
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
  echo "[anvil-qualification] pre-growth indexes remained exact and published from ${new_builder_node} after online $((active_nodes - 1))->${active_nodes} reassignment"
}

run_real_journal_gap_qualification() {
  local -A recovery_log_starts=()
  local -A seed_log_starts=()
  local builder=""
  local builder_count=0
  local bucket_id
  local emitted_total=0
  local evidence
  local expected_scoped_heads
  local gap_index_id
  local head_reads
  local heads_emitted
  local ingress=""
  local max_scoped_head_reads
  local node
  local node_id
  local rebuild_line
  local seed_log
  local tenant_id
  local unrelated_objects
  local value
  for node in anvil-1 anvil-2 anvil-3; do
    seed_log_starts["${node}"]="$(log_line_count "${node}")"
  done
  run_index_recovery_qualification \
    gap-seed "$(IFS=,; echo "${public_endpoints[*]}")" \
    qindex-gap qindex-gap-client "${index_gap_secret}" \
    "${index_gap_state}" "index-gap-${$}"
  gap_index_id="$(state_index_ids "${index_gap_state}")"
  if [[ ! "${gap_index_id}" =~ ^[1-9][0-9]*$ ]]; then
    echo "journal-gap state did not contain exactly one fixture index ID" >&2
    return 1
  fi
  for node in anvil-1 anvil-2 anvil-3; do
    seed_log="${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-seed-${node}.log"
    save_log_suffix "${node}" "${seed_log_starts[${node}]}" "${seed_log}"
    if log_has_index_event \
      "${seed_log}" "${gap_index_id}" "index generation published"
    then
      builder="${node}"
      builder_count=$((builder_count + 1))
    fi
  done
  if ((builder_count != 1)); then
    echo "journal-gap seed was published by ${builder_count} nodes; expected one builder" >&2
    return 1
  fi
  for node in anvil-1 anvil-2 anvil-3; do
    if [[ "${node}" != "${builder}" ]]; then
      ingress="${node}"
      break
    fi
  done
  if [[ -z "${ingress}" ]]; then
    echo "journal-gap qualification found no non-builder ingress" >&2
    return 1
  fi

  for node in anvil-1 anvil-2 anvil-3; do
    recovery_log_starts["${node}"]="$(log_line_count "${node}")"
  done
  paused_container="$(service_container "${builder}")"
  docker pause "${paused_container}" >/dev/null
  sleep 4
  run_index_recovery_qualification \
    gap-write "$(public_endpoint_for "${ingress}")" \
    qindex-gap qindex-gap-client "${index_gap_secret}" \
    "${index_gap_state}"
  docker unpause "${paused_container}" >/dev/null
  paused_container=""
  wait_for_node "${builder}"
  run_index_recovery_qualification \
    gap-verify "$(IFS=,; echo "${public_endpoints[*]}")" \
    qindex-gap qindex-gap-client "${index_gap_secret}" \
    "${index_gap_state}"
  for node in anvil-1 anvil-2 anvil-3; do
    save_log_suffix \
      "${node}" "${recovery_log_starts[${node}]}" \
      "${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-recovery-${node}.log"
  done
  if ! log_has_index_event \
    "${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-recovery-${builder}.log" \
    "${gap_index_id}" "index snapshot rebuild started"
  then
    echo "${builder} did not report a scoped snapshot rebuild after its source cursor expired" >&2
    return 1
  fi
  rebuild_line="$(awk -v marker="index.id=${gap_index_id} " '
      index($0, marker) && index($0, "index snapshot rebuild started") { line = $0 }
      END { print line }
    ' "${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-recovery-${builder}.log")"
  tenant_id="$(log_unsigned_field tenant_id "${rebuild_line}")" || {
    echo "journal-gap rebuild omitted stable tenant_id evidence" >&2
    return 1
  }
  bucket_id="$(log_unsigned_field bucket_id "${rebuild_line}")" || {
    echo "journal-gap rebuild omitted stable bucket_id evidence" >&2
    return 1
  }
  expected_scoped_heads="$(state_unsigned_field expected_scoped_heads "${index_gap_state}")"
  max_scoped_head_reads="$(state_unsigned_field max_scoped_head_reads_per_source "${index_gap_state}")"
  unrelated_objects="$(state_unsigned_field unrelated_objects "${index_gap_state}")"
  for value in "${expected_scoped_heads}" "${max_scoped_head_reads}" "${unrelated_objects}"; do
    if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
      echo "journal-gap state omitted its scoped head-read bounds" >&2
      return 1
    fi
  done
  if ((unrelated_objects <= max_scoped_head_reads * 3)); then
    echo "journal-gap unrelated fixture is too small to expose a broad scan on every source" >&2
    return 1
  fi

  for node_id in 1 2 3; do
    node="anvil-${node_id}"
    evidence="$(awk -v tenant="tenant_id=${tenant_id}" -v bucket="bucket_id=${bucket_id}" '
        index($0, "anvil_index_scoped_snapshot_evidence") &&
        index($0, tenant) && index($0, bucket) { line = $0 }
        END { print line }
      ' "${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-recovery-${node}.log")"
    if [[ -z "${evidence}" ]]; then
      echo "${node} emitted no terminal scoped snapshot evidence for the journal-gap bucket" >&2
      return 1
    fi
    if [[ "$(log_unsigned_field node_id "${evidence}")" != "${node_id}" ]]; then
      echo "${node} scoped snapshot evidence carried another node identity" >&2
      return 1
    fi
    head_reads="$(log_unsigned_field head_reads_total "${evidence}")" || {
      echo "${node} scoped snapshot evidence omitted physical head reads" >&2
      return 1
    }
    heads_emitted="$(log_unsigned_field heads_emitted_total "${evidence}")" || {
      echo "${node} scoped snapshot evidence omitted emitted heads" >&2
      return 1
    }
    if ((head_reads > max_scoped_head_reads)); then
      echo "${node} physically read ${head_reads} heads for a scope capped at ${max_scoped_head_reads}" >&2
      return 1
    fi
    emitted_total=$((emitted_total + heads_emitted))
  done
  if ((emitted_total != expected_scoped_heads)); then
    echo "scoped snapshot emitted ${emitted_total} heads, expected ${expected_scoped_heads}" >&2
    return 1
  fi
  echo "[anvil-qualification] a genuine retained-journal gap read only its scoped bucket and recovered ${expected_scoped_heads} exact live heads"
}

assert_zero_accounting_traffic_drops() {
  local batches
  local bytes
  local count
  local expected_node_id
  local line
  local node
  local node_id
  for node in anvil-1 anvil-2 anvil-3; do
    count=0
    expected_node_id="${node#anvil-}"
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
        | grep -F 'anvil_accounting_traffic_drop_state' || true
    )
    if ((count == 0)); then
      echo "${node} emitted no accounting drop-state evidence" >&2
      return 1
    fi
  done
  echo "[anvil-qualification] accounting traffic reported zero dropped batches and bytes on every node"
}

wait_for_bootstrap
assert_sparse_index_startup anvil-1 1
qprobe_secret=qualification-probe-secret-000000000000000000000000
provision_tenant qprobe qprobe-client "${qprobe_secret}"
create_bucket anvil-1 qprobe-client \
  "${qprobe_secret}" objects

# Every index below is created while node 1 is the only ACTIVE member. The
# three-node phase later mutates the same definitions and requires publication
# from a newly selected builder.
index_membership_secret=qualification-index-membership-secret-0000000000000000
provision_tenant \
  qindex-membership qindex-membership-client "${index_membership_secret}"
run_index_recovery_qualification \
  membership-seed "$(public_endpoint_for anvil-1)" \
  qindex-membership qindex-membership-client "${index_membership_secret}" \
  "${index_membership_state}"
test -s "${index_membership_state}"
echo "[anvil-qualification] one-node index assignment fixtures seeded before online growth"

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
  printf '/var/lib/anvil/blobs/%s/%s\n' "${hash:0:2}" "${hash}"
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
  local directory="/var/lib/anvil/blobs/${hash:0:2}"
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
  of="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  bs=1M count=2 status=none
cp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin"
printf '\177' | dd \
  of="${ANVIL_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin" \
  bs=1 seek=0 count=1 conv=notrunc status=none
chmod 0444 \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/one-node-replicated-rejected.bin"
expect_failure "one-node REPLICATED large Put" \
  run_cli anvil-1 qprobe-client "${qprobe_secret}" \
    put qprobe objects growth/replicated-must-fail.bin \
      /qualification/artifacts/one-node-replicated-rejected.bin \
      --command-id qprobe-one-node-replicated-rejected \
      --durability replicated --if-absent
rejected_head="$(run_cli anvil-1 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/replicated-must-fail.bin)"
if [[ "${rejected_head}" != "never-existed" ]]; then
  echo "failed one-node REPLICATED Put published an object head: ${rejected_head}" >&2
  exit 1
fi
echo "[anvil-qualification] one-node REPLICATED large Put failed closed without a head"
run_cli anvil-1 qprobe-client \
  "${qprobe_secret}" \
  put qprobe objects growth/from-one.bin \
    /qualification/artifacts/growth-large.bin \
    --command-id qprobe-growth-one --durability local >/dev/null
growth_one_head="$(run_cli anvil-1 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-one.bin)"
run_cli anvil-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
echo "[anvil-qualification] one-node large-object read passed"

# Restart the exact installation that will grow. This proves the durable
# one-node representation and reference-journal recovery before ADD begins.
compose restart anvil-1
wait_for_node anvil-1
rm -f "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
run_cli anvil-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
require_qprobe_head anvil-1 growth/from-one.bin "${growth_one_head}"
echo "[anvil-qualification] one-node large object survived restart before growth"

prepare_and_start_node 2
membership_two_endpoints="$(public_endpoint_for anvil-1),$(public_endpoint_for anvil-2)"
run_live_builder_reassignment_qualification \
  2 "${membership_two_endpoints}" anvil-2

growth_one_two_node_head="$(run_cli anvil-2 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-one.bin)"
if [[ "${growth_one_two_node_head}" != "${growth_one_head}" ]]; then
  echo "node 2 observed another head for the one-node object after ADD" >&2
  echo "expected: ${growth_one_head}" >&2
  echo "actual:   ${growth_one_two_node_head}" >&2
  exit 1
fi
growth_one_hash="$(head_blake3 "${growth_one_two_node_head}")"
move_complete_blob anvil-1 "${growth_one_hash}"
rm -f "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
run_cli anvil-2 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-one.bin \
    --output /qualification/artifacts/growth-one-read.bin
restore_complete_blob anvil-1 "${growth_one_hash}"
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
require_qprobe_head anvil-2 growth/from-one.bin "${growth_one_head}"
echo "[anvil-qualification] two-node read succeeded without node 1's complete blob"

# Use a different content identity so this is a real two-node payload write,
# not a second logical reference to the preexisting deduplicated blob.
cp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
chmod 0644 "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
printf '\001' | dd \
  of="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" \
  bs=1 seek=0 count=1 conv=notrunc status=none
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
run_cli anvil-2 qprobe-client \
  "${qprobe_secret}" \
  put qprobe objects growth/from-two.bin \
    /qualification/artifacts/growth-two-large.bin \
    --command-id qprobe-growth-two --durability replicated >/dev/null
growth_two_head="$(run_cli anvil-2 qprobe-client "${qprobe_secret}" \
  head qprobe objects growth/from-two.bin)"
growth_two_hash="$(head_blake3 "${growth_two_head}")"
move_complete_blob anvil-2 "${growth_two_hash}"
run_cli anvil-1 qprobe-client \
  "${qprobe_secret}" \
  get qprobe objects growth/from-two.bin \
    --output /qualification/artifacts/growth-two-read.bin
restore_complete_blob anvil-2 "${growth_two_hash}"
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-read.bin"
require_qprobe_head anvil-1 growth/from-two.bin "${growth_two_head}"
echo "[anvil-qualification] two-node REPLICATED read succeeded without its ingress copy"

prepare_and_start_node 3
membership_three_endpoints="$(public_endpoint_for anvil-1),$(public_endpoint_for anvil-2),$(public_endpoint_for anvil-3)"
run_live_builder_reassignment_qualification \
  3 "${membership_three_endpoints}" anvil-3

declare -a moved_complete_blobs=()
for growth_node in anvil-1 anvil-2 anvil-3; do
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

for unavailable_node in anvil-1 anvil-2 anvil-3; do
  declare -a moved_shards=()
  for growth_hash in "${growth_one_hash}" "${growth_two_hash}"; do
    moved_shards+=("$(move_shard "${unavailable_node}" "${growth_hash}")")
  done
  for growth_object in from-one from-two; do
    case "${growth_object}" in
      from-one) growth_expected="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin" ;;
      from-two) growth_expected="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" ;;
    esac
    growth_output="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    rm -f "${growth_output}"
    run_cli anvil-1 qprobe-client \
      "${qprobe_secret}" \
      get qprobe objects "growth/${growth_object}.bin" \
        --output "/qualification/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    cmp "${growth_expected}" "${growth_output}"
    case "${growth_object}" in
      from-one) growth_expected_head="${growth_one_head}" ;;
      from-two) growth_expected_head="${growth_two_head}" ;;
    esac
    require_qprobe_head \
      anvil-1 "growth/${growth_object}.bin" "${growth_expected_head}"
  done
  for moved_shard in "${moved_shards[@]}"; do
    restore_shard "${unavailable_node}" "${moved_shard}"
  done
done
for moved_complete_blob in "${moved_complete_blobs[@]}"; do
  read -r growth_node growth_hash <<<"${moved_complete_blob}"
  restore_complete_blob "${growth_node}" "${growth_hash}"
done
echo "[anvil-qualification] three-node 2+1 reads preserved both large object heads and bytes without complete copies after every one-shard loss"

echo "[anvil-qualification] three-node cluster is ACTIVE"
case "${index_resource_scope}" in
  release-corpus)
    echo "[anvil-qualification] index resource scope=release-corpus records=839980 indexed_fields=12"
    ;;
  smoke)
    echo "[anvil-qualification] index resource scope=smoke records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
  custom)
    echo "[anvil-qualification] index resource scope=custom records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
esac

public_endpoints=()
for index_node in anvil-1 anvil-2 anvil-3; do
  public_endpoints+=("$(public_endpoint_for "${index_node}")")
done

index_gap_secret=qualification-index-gap-secret-0000000000000000000000
provision_tenant qindex-gap qindex-gap-client "${index_gap_secret}"
run_real_journal_gap_qualification

index_secret=qualification-index-secret-00000000000000000000000
provision_tenant qindex qindex-client "${index_secret}"
capture_index_qualification_log_start
ANVIL_INDEX_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
ANVIL_INDEX_QUALIFICATION_TENANT=qindex \
ANVIL_INDEX_QUALIFICATION_CLIENT_ID=qindex-client \
ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${index_secret}" \
ANVIL_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE=1 \
ANVIL_INDEX_QUALIFICATION_STATE_OUTPUT="${index_verification_state}" \
  cargo run --quiet --locked --package anvil-server \
    --manifest-path "${repo_root}/Cargo.toml" \
    --example cluster_index_qualification
test -s "${index_verification_state}"
save_index_qualification_logs
echo "[anvil-qualification] distributed index qualification passed"
assert_one_builder_published_and_compacted_each_index_kind

index_resource_secret=qualification-index-resource-secret-000000000000000000
provision_tenant qindex-resource qindex-resource-client "${index_resource_secret}"
run_index_resource_qualification
assert_index_resource_bounds

accounting_secret=qualification-accounting-secret-000000000000000000000
provision_tenant qaccounting qaccounting-client "${accounting_secret}"
ANVIL_ACCOUNTING_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
ANVIL_ACCOUNTING_QUALIFICATION_TENANT=qaccounting \
ANVIL_ACCOUNTING_QUALIFICATION_BUCKET="accounting-three-${$}" \
ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_ID=qaccounting-client \
ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_SECRET="${accounting_secret}" \
  cargo run --quiet --locked --package anvil-server \
    --manifest-path "${repo_root}/Cargo.toml" \
    --example accounting_qualification
echo "[anvil-qualification] distributed accounting qualification passed"

personaldb_secret=qualification-personaldb-secret-0000000000000000000
provision_tenant qpersonaldb qpersonaldb-client "${personaldb_secret}"
ANVIL_PERSONALDB_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
ANVIL_PERSONALDB_QUALIFICATION_TENANT=qpersonaldb \
ANVIL_PERSONALDB_QUALIFICATION_CLIENT_ID=qpersonaldb-client \
ANVIL_PERSONALDB_QUALIFICATION_CLIENT_SECRET="${personaldb_secret}" \
  cargo run --quiet --locked --package anvil-server \
    --manifest-path "${repo_root}/Cargo.toml" \
    --example personaldb_qualification
echo "[anvil-qualification] distributed PersonalDB qualification passed"

s3_secret=qualification-s3-secret-00000000000000000000000000
provision_tenant qs3 qs3-client "${s3_secret}"
ANVIL_S3_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
ANVIL_S3_QUALIFICATION_CLIENT_ID=qs3-client \
ANVIL_S3_QUALIFICATION_CLIENT_SECRET="${s3_secret}" \
ANVIL_S3_QUALIFICATION_BUCKET="s3-three-${$}" \
  cargo run --quiet --locked --package anvil-server \
    --manifest-path "${repo_root}/Cargo.toml" \
    --example s3_qualification
echo "[anvil-qualification] distributed official AWS SDK S3 qualification passed"
run_git_qualification

cas_secret=qualification-cas-secret-000000000000000000000000
provision_tenant qcas qcas-client "${cas_secret}"
create_bucket anvil-2 qcas-client "${cas_secret}" objects
printf 'three-node-cas\n' >"${ANVIL_QUALIFICATION_DIR}/artifacts/cas.txt"
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/cas.txt"
run_cli anvil-1 qcas-client "${cas_secret}" \
  put qcas objects cas/value.txt /qualification/artifacts/cas.txt \
  --command-id qcas-first --if-absent >/dev/null
expect_failure "second PutIfAbsent" \
  run_cli anvil-3 qcas-client "${cas_secret}" \
  put qcas objects cas/value.txt /qualification/artifacts/cas.txt \
  --command-id qcas-second --if-absent
run_cli anvil-2 qcas-client "${cas_secret}" \
  get qcas objects cas/value.txt \
  --output /qualification/artifacts/cas-read.txt
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/cas.txt" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/cas-read.txt"
echo "[anvil-qualification] cross-node CAS test passed"

version_secret=qualification-version-secret-00000000000000000000
provision_tenant qversion qversion-client "${version_secret}"
run_cli anvil-2 qversion-client "${version_secret}" \
  create-bucket objects --versioning enabled \
  | grep -Fq "bucket=objects versioning=enabled"
printf 'retained-version-one\n' \
  >"${ANVIL_QUALIFICATION_DIR}/artifacts/version-one.txt"
printf 'retained-version-two\n' \
  >"${ANVIL_QUALIFICATION_DIR}/artifacts/version-two.txt"
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/version-"*.txt
version_one="$(run_cli anvil-1 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-one.txt \
  --command-id qversion-one --durability replicated)"
version_two="$(run_cli anvil-3 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-two.txt \
  --command-id qversion-two --durability replicated)"
if [[ ! "${version_one}" =~ ^[1-9][0-9]*$ || ! "${version_two}" =~ ^[1-9][0-9]*$ ]]; then
  echo "distributed puts returned invalid versions: ${version_one}, ${version_two}" >&2
  exit 1
fi
old_delete="$(run_cli anvil-2 qversion-client "${version_secret}" \
  delete-version qversion objects retained/value.txt "${version_one}" --durability replicated)"
if [[ "${old_delete}" != 'deleted=true replacement_tombstone_version=none' ]]; then
  echo "distributed historical DeleteVersion returned: ${old_delete}" >&2
  exit 1
fi
run_cli anvil-1 qversion-client "${version_secret}" \
  get qversion objects retained/value.txt \
  --output /qualification/artifacts/version-current.txt
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/version-two.txt" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/version-current.txt"
current_delete="$(run_cli anvil-3 qversion-client "${version_secret}" \
  delete-version qversion objects retained/value.txt "${version_two}" --durability replicated)"
if [[ ! "${current_delete}" =~ ^deleted=true\ replacement_tombstone_version=([1-9][0-9]*)$ ]]; then
  echo "distributed current DeleteVersion returned: ${current_delete}" >&2
  exit 1
fi
replacement_tombstone_version="${BASH_REMATCH[1]}"
for version_node in anvil-1 anvil-2 anvil-3; do
  version_head="$(run_cli "${version_node}" qversion-client "${version_secret}" \
    head qversion objects retained/value.txt)"
  if [[ "${version_head}" != "deleted version=${replacement_tombstone_version}" ]]; then
    echo "${version_node} did not observe the fresh tombstone" >&2
    exit 1
  fi
done
echo "[anvil-qualification] distributed retained-version deletion test passed"

list_secret=qualification-list-secret-00000000000000000000000
provision_tenant qlist qlist-client "${list_secret}"
create_bucket anvil-3 qlist-client "${list_secret}" objects
printf 'cluster-list\n' >"${ANVIL_QUALIFICATION_DIR}/artifacts/list.txt"
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/list.txt"
for item in alpha bravo charlie delta; do
  case "${item}" in
    alpha) list_node=anvil-1 ;;
    bravo) list_node=anvil-2 ;;
    charlie|delta) list_node=anvil-3 ;;
  esac
  run_cli "${list_node}" qlist-client "${list_secret}" \
    put qlist objects "prefix/${item}.txt" /qualification/artifacts/list.txt \
    --command-id "qlist-${item}" --durability replicated >/dev/null
done
expected_list=$'prefix/alpha.txt\nprefix/bravo.txt\nprefix/charlie.txt\nprefix/delta.txt'
for list_node in anvil-1 anvil-2 anvil-3; do
  actual_list="$(run_cli "${list_node}" qlist-client "${list_secret}" \
    list qlist objects --prefix prefix/ --limit 100)"
  if [[ "${actual_list}" != "${expected_list}" ]]; then
    echo "${list_node} returned an incorrect distributed lexical list" >&2
    printf 'expected:\n%s\nactual:\n%s\n' "${expected_list}" "${actual_list}" >&2
    exit 1
  fi
done
page_one="$(run_cli anvil-2 qlist-client "${list_secret}" \
  list qlist objects --prefix prefix/ --limit 2 2>/dev/null)"
page_two="$(run_cli anvil-1 qlist-client "${list_secret}" \
  list qlist objects --prefix prefix/ --start-after prefix/bravo.txt --limit 2)"
if [[ "${page_one}" != $'prefix/alpha.txt\nprefix/bravo.txt' \
  || "${page_two}" != $'prefix/charlie.txt\nprefix/delta.txt' ]]; then
  echo "distributed ListObjects pagination is incorrect" >&2
  exit 1
fi
echo "[anvil-qualification] distributed listing and pagination test passed"

watch_paths="$(run_cli anvil-3 qlist-client "${list_secret}" \
  watch qlist objects --prefix prefix --retained --events 4 \
  --idle-timeout-seconds 30 \
  | cut -f2 | sort)"
if [[ "${watch_paths}" != "${expected_list}" ]]; then
  echo "distributed WatchPrefix did not replay the four retained paths" >&2
  printf 'expected:\n%s\nactual:\n%s\n' "${expected_list}" "${watch_paths}" >&2
  exit 1
fi
echo "[anvil-qualification] distributed retained watch test passed"

atomic_secret=qualification-atomic-secret-000000000000000000000
provision_tenant qatomic qatomic-client "${atomic_secret}"
run_atomic_index_qualification

ec_secret=qualification-ec-secret-0000000000000000000000000
provision_tenant qec qec-client "${ec_secret}"
create_bucket anvil-3 qec-client "${ec_secret}" objects
dd if=/dev/urandom of="${ANVIL_QUALIFICATION_DIR}/artifacts/large.bin" \
  bs=1M count=2 status=none
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/large.bin"
run_cli anvil-2 qec-client "${ec_secret}" \
  put qec objects ec/large.bin /qualification/artifacts/large.bin \
  --command-id qec-replicated --durability replicated >/dev/null
run_cli anvil-1 qec-client "${ec_secret}" \
  get qec objects ec/large.bin \
  --output /qualification/artifacts/large-read.bin
cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/large.bin" \
  "${ANVIL_QUALIFICATION_DIR}/artifacts/large-read.bin"
echo "[anvil-qualification] 2+1 replicated payload test passed"

restart_secret=qualification-restart-secret-000000000000000000000
provision_tenant qrestart qrestart-client "${restart_secret}"
create_bucket anvil-1 qrestart-client "${restart_secret}" objects
printf 'survives-rolling-restart\n' \
  >"${ANVIL_QUALIFICATION_DIR}/artifacts/restart.txt"
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/restart.txt"
run_cli anvil-3 qrestart-client "${restart_secret}" \
  put qrestart objects restart/value.txt /qualification/artifacts/restart.txt \
  --command-id qrestart-seed --durability replicated >/dev/null
for node in anvil-1 anvil-2 anvil-3; do
  sparse_starts_before="$(index_sparse_start_count "${node}")"
  populated_restart_started="${SECONDS}"
  compose restart "${node}"
  wait_for_node "${node}"
  if ((SECONDS - populated_restart_started > 30)); then
    echo "${node} populated restart exceeded 30 seconds" >&2
    exit 1
  fi
  assert_sparse_index_startup "${node}" "$((sparse_starts_before + 1))"
  rm -f "${ANVIL_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  run_cli "${node}" qrestart-client "${restart_secret}" \
    get qrestart objects restart/value.txt \
    --output /qualification/artifacts/restart-read.txt
  cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/restart.txt" \
    "${ANVIL_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  verify_existing_indexes
  echo "[anvil-qualification] ${node} restart preserved every final complete index generation through all public endpoints"
  for growth_object in from-one from-two; do
    case "${growth_object}" in
      from-one)
        growth_expected="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-large.bin"
        growth_expected_head="${growth_one_head}"
        ;;
      from-two)
        growth_expected="${ANVIL_QUALIFICATION_DIR}/artifacts/growth-two-large.bin"
        growth_expected_head="${growth_two_head}"
        ;;
    esac
    growth_output="${ANVIL_QUALIFICATION_DIR}/artifacts/restart-${node}-${growth_object}.bin"
    rm -f "${growth_output}"
    run_cli "${node}" qprobe-client "${qprobe_secret}" \
      get qprobe objects "growth/${growth_object}.bin" \
        --output "/qualification/artifacts/restart-${node}-${growth_object}.bin"
    cmp "${growth_expected}" "${growth_output}"
    require_qprobe_head \
      "${node}" "growth/${growth_object}.bin" "${growth_expected_head}"
  done
done
echo "[anvil-qualification] rolling populated restart preserved objects and indexes; sparse startup markers were present and no legacy definition barrier was reported"
assert_index_retention_converged
assert_zero_accounting_traffic_drops

if [[ "${qualification_mode}" == "release" ]]; then
  echo "[anvil-qualification] PASS scope=release-corpus records=${index_resource_records} image=${image_id} platform=${ANVIL_DOCKER_PLATFORM}"
else
  echo "[anvil-qualification] SMOKE PASS records=${index_resource_records} image=${image_id} platform=${ANVIL_DOCKER_PLATFORM}"
fi
