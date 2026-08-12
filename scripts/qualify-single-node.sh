#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/qualification-log-evidence.sh"
source "${repo_root}/scripts/qualification-scale-evidence.sh"
requested_image="${ANVIL_IMAGE:-anvil:0.8.0}"
keep="${ANVIL_QUALIFICATION_KEEP:-0}"
qualification_mode="${ANVIL_QUALIFICATION_MODE:-smoke}"
index_disk_cache_bytes="${ANVIL_QUALIFICATION_INDEX_DISK_CACHE_BYTES:-1073741824}"
index_memory_percent="${ANVIL_QUALIFICATION_INDEX_MEMORY_PERCENT:-20}"
index_kind_budget_bytes="${ANVIL_QUALIFICATION_INDEX_KIND_BUDGET_BYTES:-268435456}"
index_compaction_max_lanes="${ANVIL_QUALIFICATION_INDEX_COMPACTION_MAX_LANES:-4}"
index_rayon_workers="${ANVIL_QUALIFICATION_INDEX_RAYON_WORKERS:-4}"
# The default is a fast smoke. Set this to 839980 for the full
# production-shaped, twelve-field corpus used by the resource qualification.
case "${qualification_mode}" in
  release)
    index_resource_records="${ANVIL_QUALIFICATION_INDEX_RECORDS:-839980}"
    require_performance_targets=1
    ;;
  smoke)
    index_resource_records="${ANVIL_QUALIFICATION_INDEX_RECORDS:-16384}"
    require_performance_targets=0
    ;;
  *)
    echo "ANVIL_QUALIFICATION_MODE must be release or smoke" >&2
    exit 2
    ;;
esac
index_resource_mutations="${ANVIL_QUALIFICATION_INDEX_MUTATIONS:-512}"
index_resource_max_anonymous_growth_bytes="${ANVIL_QUALIFICATION_INDEX_MAX_ANONYMOUS_GROWTH_BYTES:-2147483648}"
index_kinds=(Path MetadataFilter TypedJson FullText Vector Hybrid GitSource Tensor)
qualification_examples=(
  accounting_qualification
  atomic_index_qualification
  cluster_index_qualification
  personaldb_qualification
  public_read_qualification
  s3_qualification
  v06_index_resource_qualification
)
declare -A qualification_example_binaries=()

for configured_limit in \
  "${index_disk_cache_bytes}" \
  "${index_memory_percent}" \
  "${index_kind_budget_bytes}" \
  "${index_compaction_max_lanes}" \
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

case "${ANVIL_DOCKER_PLATFORM:-}" in
  "")
    case "$(uname -m)" in
      x86_64|amd64) platform=linux/amd64 ;;
      aarch64|arm64) platform=linux/arm64 ;;
      *)
        echo "unsupported host architecture: $(uname -m)" >&2
        exit 2
        ;;
    esac
    ;;
  linux/amd64|linux/arm64) platform="${ANVIL_DOCKER_PLATFORM}" ;;
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
  echo "cargo is required for the public qualification clients" >&2
  exit 2
}
command -v jq >/dev/null 2>&1 || {
  echo "jq is required to resolve the public qualification client binaries" >&2
  exit 2
}
command -v git >/dev/null 2>&1 || {
  echo "git is required for the smart HTTP gateway qualification" >&2
  exit 2
}

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

image_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}")"
if [[ ! "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "qualification image did not resolve to an immutable sha256 digest" >&2
  exit 2
fi
container_platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "${image_id}")"
if [[ "${container_platform}" != "${platform}" ]]; then
  echo "qualification image platform ${container_platform} does not match ${platform}" >&2
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
server_version="$(docker run --rm --platform "${platform}" "${image_id}" anvil-server --version)"
client_version="$(docker run --rm --platform "${platform}" "${image_id}" anvil --version)"
if [[ "${server_version}" != "anvil-server 0.8.0" \
  || "${client_version}" != "anvil 0.8.0" ]]; then
  echo "qualification requires the exact Anvil 0.8.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
qualification_dir="$(mktemp -d /var/tmp/anvil-v080-single-qualification.XXXXXX)"
qualification_suffix="${qualification_dir##*.}"
container_name="anvil-v080-single-${qualification_suffix}"
data_dir="${qualification_dir}/data"
signing_key="${qualification_dir}/token-signing-key"
index_verification_state="${qualification_dir}/index-verification-state.json"
index_qualification_log="${qualification_dir}/index-qualification.log"
index_resource_qualification_log="${qualification_dir}/index-resource-qualification.log"
index_resource_state="${qualification_dir}/index-resource-state.json"
index_resource_bucket="index-resource-${qualification_suffix}"
index_resource_report="/var/tmp/anvil-v080-single-index-resource-${qualification_suffix}.json"
index_resource_observability_report="/var/tmp/anvil-v080-single-index-observability-${qualification_suffix}.json"
index_resource_telemetry_prefix="/var/tmp/anvil-v080-single-index-telemetry-${qualification_suffix}"
ANVIL_QUALIFICATION_STATE_DIR="${qualification_dir}"
qualification_build_messages="${qualification_dir}/qualification-client-build.jsonl"
container_started=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if ((container_started == 1)) && ((status != 0)); then
    echo "[anvil-single-qualification] FAILED; container logs follow" >&2
    docker logs "${container_name}" >&2 || true
  fi

  if [[ "${keep}" == "1" ]]; then
    echo "[anvil-single-qualification] retained container ${container_name}" >&2
    echo "[anvil-single-qualification] retained files ${qualification_dir}" >&2
    exit "${status}"
  fi

  if ((container_started == 1)); then
    docker rm --force "${container_name}" >/dev/null 2>&1 || true
  fi
  if [[ "${qualification_dir}" == /var/tmp/anvil-v080-single-qualification.* ]]; then
    docker run --rm --user 0 \
      --volume "${qualification_dir}:/qualification" \
      "${image_id}" rm -rf \
        /qualification/data \
        /qualification/token-signing-key >/dev/null 2>&1 || true
    rm -rf -- "${qualification_dir}"
  else
    echo "refusing to remove unexpected qualification path ${qualification_dir}" >&2
    status=1
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

build_qualification_clients() {
  local cargo_metadata
  local cargo_target_directory
  local example
  local executable
  local -a build_command=(
    cargo build
    --quiet
    --release
    --locked
    --package anvil-server
    --manifest-path "${repo_root}/Cargo.toml"
    --message-format json-render-diagnostics
  )

  cargo_metadata="$(
    cargo metadata --quiet --locked --no-deps --format-version 1 \
      --manifest-path "${repo_root}/Cargo.toml"
  )"
  cargo_target_directory="$(
    jq -er '.target_directory | select(type == "string" and length > 0)' \
      <<<"${cargo_metadata}"
  )"
  if [[ "${cargo_target_directory}" != /* ]]; then
    echo "Cargo returned a non-absolute target directory: ${cargo_target_directory}" >&2
    return 1
  fi

  for example in "${qualification_examples[@]}"; do
    build_command+=(--example "${example}")
  done

  echo "[anvil-single-qualification] building public qualification clients in ${cargo_target_directory}"
  if ! "${build_command[@]}" >"${qualification_build_messages}"; then
    jq -r '
      select(.reason == "compiler-message")
      | .message.rendered // empty
    ' "${qualification_build_messages}" >&2 || true
    return 1
  fi

  for example in "${qualification_examples[@]}"; do
    executable="$(
      jq -rs --arg example "${example}" '
        [
          .[]
          | select(
              .reason == "compiler-artifact"
              and .target.name == $example
              and (.target.kind | index("example"))
              and (.executable | type == "string")
            )
          | .executable
        ]
        | last // empty
      ' "${qualification_build_messages}"
    )"
    if [[ -z "${executable}" || ! -x "${executable}" ]]; then
      echo "Cargo did not produce an executable ${example} qualification client" >&2
      return 1
    fi
    case "${executable}" in
      "${cargo_target_directory}"/*) ;;
      *)
        echo "Cargo produced ${example} outside its configured target directory" >&2
        return 1
        ;;
    esac
    qualification_example_binaries["${example}"]="${executable}"
  done
  rm -f -- "${qualification_build_messages}"
  echo "[anvil-single-qualification] public qualification clients are ready; Cargo is no longer needed"
}

build_qualification_clients

mkdir "${data_dir}"
chmod 0755 "${qualification_dir}"
head -c 64 /dev/urandom >"${signing_key}"
chmod 0600 "${signing_key}"
docker run --rm --user 0 \
  --volume "${qualification_dir}:/qualification" \
  "${image_id}" chown -R 10001:10001 \
    /qualification/data \
    /qualification/token-signing-key

# The public all-kind workload publishes independently observed generations;
# use the engine's four-run maintenance bound so it exercises real compaction
# without manufacturing more than 64 serial generations per kind.
docker run --detach \
  --name "${container_name}" \
  --platform "${platform}" \
  --publish 127.0.0.1::50051 \
  --env RUST_LOG=info,anvil::index_runtime::retention=debug,anvil::observability::runtime=debug \
  --env ANVIL_LISTEN=0.0.0.0:50051 \
  --env ANVIL_PEER_LISTEN=127.0.0.1:50052 \
  --env ANVIL_DATA_DIR=/var/lib/anvil \
  --env ANVIL_NODE_ID=1 \
  --env ANVIL_TOKEN_SIGNING_KEY_FILE=/run/secrets/anvil-token-signing-key \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=6000 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=1000 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=600 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100 \
  --env "ANVIL_INDEX_DISK_CACHE_BYTES=${index_disk_cache_bytes}" \
  --env "ANVIL_INDEX_MEMORY_PERCENT=${index_memory_percent}" \
  --env "ANVIL_INDEX_BUILDER_MEMORY_BYTES_PER_KIND=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_PATH_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_PATH_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_METADATA_FILTER_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_METADATA_FILTER_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_TYPED_JSON_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_TYPED_JSON_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_FULL_TEXT_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_FULL_TEXT_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_VECTOR_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_VECTOR_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_HYBRID_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_HYBRID_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_GIT_SOURCE_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_GIT_SOURCE_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_TENSOR_BUILDER_MEMORY_BYTES=${index_kind_budget_bytes}" \
  --env "ANVIL_INDEX_TENSOR_COMPACTION_MAX_LANES=${index_compaction_max_lanes}" \
  --env "ANVIL_INDEX_RAYON_WORKERS=${index_rayon_workers}" \
  --env ANVIL_INDEX_MAX_RUNS_PER_LEVEL=4 \
  --env ANVIL_INDEX_MAX_RETAINED_GENERATIONS=1 \
  --env ANVIL_RUN_SYSTEM_BOOTSTRAP=true \
  --volume "${data_dir}:/var/lib/anvil" \
  --volume "${signing_key}:/run/secrets/anvil-token-signing-key:ro" \
  "${image_id}" >/dev/null
container_started=1

wait_for_bootstrap() {
  local attempt
  for attempt in $(seq 1 90); do
    if docker exec "${container_name}" \
      test -f /var/lib/anvil/system-bootstrap-credential.json \
      >/dev/null 2>&1
    then
      return 0
    fi
    if ! docker inspect --format '{{.State.Running}}' "${container_name}" \
      2>/dev/null | grep -Fxq true
    then
      echo "single-node qualification server exited during bootstrap" >&2
      return 1
    fi
    sleep 1
  done
  echo "single-node qualification bootstrap did not finish within 90 seconds" >&2
  return 1
}

provision_owner() {
  local provisioned_tenant="$1"
  local provisioned_app="$2"
  local provisioned_client="$3"
  local provisioned_secret="$4"
  local output=""
  local attempt
  for attempt in $(seq 1 90); do
    if output="$(docker exec \
      --env "ANVIL_NEW_CLIENT_SECRET=${provisioned_secret}" \
      "${container_name}" \
      anvil --endpoint http://127.0.0.1:50051 \
        --credentials-file /var/lib/anvil/system-bootstrap-credential.json \
        provision-tenant "${provisioned_tenant}" "${provisioned_app}" \
          "${provisioned_client}" 2>&1)"
    then
      if grep -Fq "tenant=${provisioned_tenant}" <<<"${output}"; then
        return 0
      fi
      echo "tenant provisioning returned unexpected output: ${output}" >&2
      return 1
    fi
    sleep 1
  done
  echo "single-node server did not accept tenant provisioning" >&2
  echo "last administration error: ${output}" >&2
  return 1
}

published_endpoint() {
  local container_port="$1"
  local label="$2"
  local published
  published="$(docker port "${container_name}" "${container_port}/tcp")"
  if [[ ! "${published}" =~ ^127\.0\.0\.1:([1-9][0-9]*)$ ]]; then
    echo "invalid ${label} loopback endpoint: ${published}" >&2
    return 1
  fi
  printf 'http://%s\n' "${published}"
}

container_logs() {
  docker logs "${container_name}" 2>&1 | strip_ansi
}

index_qualification_log_start=0

capture_index_qualification_log_start() {
  index_qualification_log_start="$({ container_logs || true; } | wc -l)"
}

save_index_qualification_log() {
  local start_line=$((index_qualification_log_start + 1))
  container_logs | tail -n "+${start_line}" >"${index_qualification_log}"
  preserve_all_kind_telemetry "${index_qualification_log}" single "${qualification_suffix}"
}

assert_each_index_kind_published_and_compacted() {
  local kind
  local message
  for kind in "${index_kinds[@]}"; do
    for message in 'index generation published' 'index runs compacted'; do
      if ! awk -v kind="index.kind=${kind}" -v message="${message}" '
          index($0, kind) && index($0, message) &&
          (message != "index runs compacted" || $0 ~ /histogram\.anvil_index_compaction_input_runs=([2-9]|[1-9][0-9]+)([[:space:]]|$)/) { found = 1 }
          END { exit !found }
        ' "${index_qualification_log}"
      then
        echo "${kind} emitted no public-API ${message} evidence" >&2
        return 1
      fi
    done
  done
  echo "[anvil-single-qualification] all eight index kinds published and compacted from public mutations"
}

assert_index_compaction_observability() {
  local -A observed_kinds=()
  local budget_limit
  local completed
  local configured
  local effective
  local expected
  local kind
  local line
  local peak_active
  local range_limit
  local ranges
  local worker_limit
  while IFS= read -r line; do
    [[ "${line}" =~ index\.kind=(Path|MetadataFilter|TypedJson|FullText|Vector|Hybrid|GitSource|Tensor) ]] \
      || continue
    kind="${BASH_REMATCH[1]}"
    configured="$(log_unsigned_field gauge.anvil_index_compaction_configured_lanes "${line}")" \
      || continue
    worker_limit="$(log_unsigned_field gauge.anvil_index_compaction_worker_limit "${line}")" \
      || return 1
    budget_limit="$(log_unsigned_field gauge.anvil_index_compaction_budget_limit "${line}")" \
      || return 1
    effective="$(log_unsigned_field compaction.effective_lanes "${line}")" \
      || return 1
    range_limit="$(log_unsigned_field gauge.anvil_index_compaction_range_limit "${line}")" \
      || return 1
    ranges="$(log_unsigned_field gauge.anvil_index_compaction_ranges_total "${line}")" \
      || return 1
    completed="$(log_unsigned_field gauge.anvil_index_compaction_ranges_completed "${line}")" \
      || return 1
    peak_active="$(log_unsigned_field gauge.anvil_index_compaction_peak_active_lanes "${line}")" \
      || return 1
    expected="${index_compaction_max_lanes}"
    ((worker_limit < expected)) && expected="${worker_limit}"
    ((budget_limit < expected)) && expected="${budget_limit}"
    if unsigned_decimal_less_than "${range_limit}" "${expected}"; then
      expected="${range_limit}"
    fi
    if ((configured != index_compaction_max_lanes \
      || worker_limit != index_rayon_workers \
      || budget_limit < 1 \
      || ranges < 1 \
      || effective != expected \
      || peak_active < 1 \
      || peak_active > effective \
      || completed != ranges)) \
      || ! unsigned_decimal_is_positive "${range_limit}" \
      || [[ "${line}" != *"anvil.index.compaction"* ]]
    then
      echo "${kind} emitted inconsistent bounded compaction telemetry" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    if ((effective >= 2 && peak_active >= 2)); then observed_kinds["${kind}"]=1; fi
  done < <(grep -F 'index compaction terminal metrics' "${index_qualification_log}" || true)
  for kind in "${index_kinds[@]}"; do
    if [[ -z "${observed_kinds[${kind}]:-}" ]]; then
      echo "${kind} emitted no terminal compaction with at least two effective and concurrently active lanes" >&2
      return 1
    fi
    if ! awk -v kind="index.kind=${kind}" '
        index($0, kind) && index($0, "anvil.index.builder") &&
        index($0, "index builder phase finished") { found = 1 }
        END { exit !found }
      ' "${index_qualification_log}"
    then
      echo "${kind} emitted no builder trace and completion log" >&2
      return 1
    fi
  done
  echo "[anvil-single-qualification] all eight index kinds emitted bounded range-compaction metrics and trace-backed completion logs"
}

run_public_read_qualification() {
  ANVIL_PUBLIC_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_PUBLIC_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_PUBLIC_QUALIFICATION_BUCKET=single-public-read \
  ANVIL_PUBLIC_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_PUBLIC_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[public_read_qualification]}"
  echo "[anvil-single-qualification] public-read qualification passed"
}

run_index_qualification() {
  capture_index_qualification_log_start
  ANVIL_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
  ANVIL_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE=1 \
  ANVIL_INDEX_QUALIFICATION_STATE_OUTPUT="${index_verification_state}" \
    "${qualification_example_binaries[cluster_index_qualification]}"
  test -s "${index_verification_state}"
  save_index_qualification_log
  assert_each_index_kind_published_and_compacted
  assert_index_compaction_observability
  echo "[anvil-single-qualification] all-eight-index qualification passed"
}

verify_existing_indexes() {
  ANVIL_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
  ANVIL_INDEX_QUALIFICATION_STATE_INPUT="${index_verification_state}" \
    "${qualification_example_binaries[cluster_index_qualification]}"
  echo "[anvil-single-qualification] final complete generations remained queryable after restart"
}

index_sparse_start_count() {
  container_logs \
    | grep -Fc 'index runtime starts from sparse assigned-definition state' \
    || true
}

startup_scan_evidence_count() {
  container_logs \
    | grep -Fc 'anvil_startup_scan_evidence' \
    || true
}

wait_for_sparse_index_startup() {
  local minimum_count="$1"
  local deadline=$((SECONDS + 90))
  while (( $(index_sparse_start_count) < minimum_count \
    || $(startup_scan_evidence_count) < minimum_count )); do
    if ! docker inspect --format '{{.State.Running}}' "${container_name}" \
      2>/dev/null | grep -Fxq true
    then
      echo "single-node qualification server exited during index startup" >&2
      return 1
    fi
    if ((SECONDS >= deadline)); then
      echo "single-node index runtime did not finish startup within 90 seconds" >&2
      return 1
    fi
    sleep 1
  done
}

assert_zero_global_startup_scan_evidence() {
  local minimum_count="$1"
  local count=0
  local field
  local line
  local node_id
  local value
  while IFS= read -r line; do
    node_id="$(log_unsigned_field node_id "${line}")" || {
      echo "single-node startup scan evidence omitted node_id" >&2
      return 1
    }
    if [[ "${node_id}" != "1" ]]; then
      echo "single-node startup scan evidence reported node=${node_id}" >&2
      return 1
    fi
    for field in \
      global_object_head_scans_total \
      global_index_artifact_scans_total \
      global_blob_scans_total \
      global_cache_scans_total
    do
      value="$(log_unsigned_field "${field}" "${line}")" || {
        echo "single-node startup scan evidence omitted ${field}" >&2
        return 1
      }
      if [[ "${value}" != "0" ]]; then
        echo "single-node startup reported ${field}=${value}" >&2
        return 1
      fi
    done
    count=$((count + 1))
  done < <(container_logs | grep -F 'anvil_startup_scan_evidence' || true)
  if ((count < minimum_count)); then
    echo "single-node startup emitted ${count} measured scan samples; expected at least ${minimum_count}" >&2
    return 1
  fi
}

assert_sparse_index_startup() {
  local minimum_count="$1"
  local observed
  wait_for_sparse_index_startup "${minimum_count}"
  observed="$(index_sparse_start_count)"
  if ((observed < minimum_count)); then
    echo "single-node startup omitted the sparse index-runtime marker" >&2
    return 1
  fi
  if container_logs \
    | grep -F 'index journals did not reach a clear initial definition barrier' \
      >/dev/null
  then
    echo "single-node startup entered the removed global definition barrier" >&2
    return 1
  fi
  assert_zero_global_startup_scan_evidence "${minimum_count}"
}

run_index_resource_qualification() {
  local resource_log_start
  assert_source_tree_exact
  resource_log_start="$({ container_logs || true; } | wc -l)"
  ANVIL_V06_RESOURCE_ENDPOINTS="${public_endpoint}" \
  ANVIL_V06_RESOURCE_TENANT="${index_resource_tenant}" \
  ANVIL_V06_RESOURCE_BUCKET="${index_resource_bucket}" \
  ANVIL_V06_RESOURCE_CLIENT_ID="${index_resource_client}" \
  ANVIL_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
  ANVIL_V06_RESOURCE_RECORDS="${index_resource_records}" \
  ANVIL_V06_RESOURCE_MUTATIONS="${index_resource_mutations}" \
  ANVIL_V06_RESOURCE_BATCH_SIZE=1000 \
  ANVIL_V06_RESOURCE_WORKERS=4 \
  ANVIL_V06_RESOURCE_VERIFICATION_WORKERS=8 \
  ANVIL_V06_RESOURCE_CONTAINERS="${container_name}" \
  ANVIL_V06_REQUIRE_RESOURCE_TARGETS=1 \
  ANVIL_V06_KIND_BUDGET_BYTES="${index_kind_budget_bytes}" \
  ANVIL_V06_INDEX_COMPACTION_MAX_LANES="${index_compaction_max_lanes}" \
  ANVIL_V06_INDEX_RAYON_WORKERS="${index_rayon_workers}" \
  ANVIL_V06_MAX_ANONYMOUS_GROWTH_BYTES="${index_resource_max_anonymous_growth_bytes}" \
  ANVIL_V08_REQUIRE_PERFORMANCE_TARGETS="${require_performance_targets}" \
  ANVIL_V08_EVIDENCE_SOURCE_COMMIT="${source_commit}" \
  ANVIL_V08_EVIDENCE_CONTAINER_DIGEST="${image_id}" \
  ANVIL_V08_EVIDENCE_NATIVE_ARCHITECTURE="${native_architecture}" \
  ANVIL_V08_EVIDENCE_CONTAINER_PLATFORM="${container_platform}" \
  ANVIL_V08_EVIDENCE_TOPOLOGY=single-node \
  ANVIL_V08_EVIDENCE_NODE_COUNT=1 \
  ANVIL_V08_EVIDENCE_HARDWARE_LOGICAL_CPUS="${hardware_logical_cpus}" \
  ANVIL_V08_EVIDENCE_HARDWARE_MEMORY_BYTES="${hardware_memory_bytes}" \
  ANVIL_V08_EVIDENCE_FILESYSTEM_TOTAL_BYTES="${qualification_filesystem_total_bytes}" \
  ANVIL_V08_EVIDENCE_FILESYSTEM_AVAILABLE_BYTES="${qualification_filesystem_available_bytes}" \
  ANVIL_V08_EVIDENCE_INDEX_DISK_CACHE_BYTES_PER_NODE="${index_disk_cache_bytes}" \
  ANVIL_V08_EVIDENCE_INDEX_MEMORY_PERCENT_PER_NODE="${index_memory_percent}" \
  ANVIL_V06_RESOURCE_OUTPUT="${index_resource_report}" \
  ANVIL_V06_RESOURCE_STATE_OUTPUT="${index_resource_state}" \
    "${qualification_example_binaries[v06_index_resource_qualification]}" \
      >/dev/null
  local attempt
  for attempt in $(seq 1 12); do
    container_logs | tail -n "+$((resource_log_start + 1))" \
      >"${index_resource_qualification_log}"
    if grep -Fq 'sampled process resources' "${index_resource_qualification_log}" \
      && grep -Fq 'sampled cgroup memory resources' "${index_resource_qualification_log}" \
      && grep -Fq 'sampled RocksDB resources' "${index_resource_qualification_log}"
    then
      break
    fi
    sleep 1
  done
  preserve_qualification_log \
    "${index_resource_qualification_log}" "${index_resource_telemetry_prefix}.log"
  test -s "${index_resource_report}"
  test -s "${index_resource_state}"
  grep -Eq "^[[:space:]]*\"records\":[[:space:]]*${index_resource_records},?[[:space:]]*$" \
    "${index_resource_report}"
  grep -Eq '^[[:space:]]*"indexed_fields":[[:space:]]*12,?[[:space:]]*$' \
    "${index_resource_report}"
  grep -Eq "^[[:space:]]*\"configured_compaction_max_lanes\":[[:space:]]*${index_compaction_max_lanes},?[[:space:]]*$" \
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
      .evidence.corpus.identity == "anvil.synthetic-index-resource.initial.v1" and
      (.evidence.corpus.initial_corpus_sha256 | test("^sha256:[0-9a-f]{64}$")) and
      .evidence.corpus.records == .records and
      .evidence.corpus.indexed_fields == .indexed_fields and
      .evidence.topology.kind == "single-node" and
      .evidence.topology.node_count == 1 and
      .evidence.topology.ingress_endpoint_count == 1 and
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
      .evidence.resource_configuration.rayon_workers_per_node == $rayon_workers and
      .evidence.resource_configuration.maximum_anonymous_growth_bytes == $maximum_growth and
      .evidence.resource_configuration.monitored_target_count == 1 and
      .evidence.resource_configuration.resource_targets_required == true and
      (.evidence.timer_boundaries | to_entries | all(.value | if type == "object" then (.starts | length > 0) and (.stops | length > 0) else length > 0 end)) and
      .evidence.correctness.result == "pass" and
      .evidence.correctness.source_complete_generation_observed == true and
      .evidence.correctness.source_complete_sources_observed == 1 and
      .evidence.correctness.initial_exact_partition_verification == true and
      .evidence.correctness.final_exact_partition_verification == true and
      .evidence.correctness.update_and_delete_verification == true and
      .evidence.correctness.resource_limits_passed == true and
      .evidence.correctness.performance_targets_required == ($performance_targets_required == 1) and
      (if $performance_targets_required == 1
       then .evidence.correctness.performance_targets_passed == true
       else .evidence.correctness.performance_targets_passed == null
       end)
    ' "${index_resource_report}" >/dev/null
  if ((require_performance_targets == 1)); then
    jq -e '
      .accepted_objects_per_second >= 10000 and
      .source_complete_objects_per_second >= 10000 and
      .timings.first_complete_generation_seconds <= 150
    ' "${index_resource_report}" >/dev/null
  fi
  echo "[anvil-single-qualification] bounded index resource qualification passed scope=${index_resource_scope} records=${index_resource_records} kind_budget=${index_kind_budget_bytes} disk_cache=${index_disk_cache_bytes}"
  echo "[anvil-single-qualification] preserved resource report ${index_resource_report}"
}

verify_index_resource_state() {
  ANVIL_V06_RESOURCE_ENDPOINTS="${public_endpoint}" \
  ANVIL_V06_RESOURCE_TENANT="${index_resource_tenant}" \
  ANVIL_V06_RESOURCE_BUCKET="${index_resource_bucket}" \
  ANVIL_V06_RESOURCE_CLIENT_ID="${index_resource_client}" \
  ANVIL_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
  ANVIL_V06_RESOURCE_VERIFICATION_WORKERS=8 \
  ANVIL_V06_RESOURCE_STATE_INPUT="${index_resource_state}" \
    "${qualification_example_binaries[v06_index_resource_qualification]}"
}

assert_production_typed_json_compaction_observability() {
  local active
  local budget_limit
  local completed
  local configured
  local effective
  local failures
  local input_rate
  local line
  local output_rate
  local peak_active
  local range_limit
  local ranges
  local worker_limit
  local terminal_found=0

  production_compaction_peak_active_lanes=0
  production_compaction_input_rate=0
  production_compaction_output_rate=0

  while IFS= read -r line; do
    [[ "${line}" == *"index.kind=TypedJson"* ]] || continue
    configured="$(log_unsigned_field gauge.anvil_index_compaction_configured_lanes "${line}")" \
      || return 1
    worker_limit="$(log_unsigned_field gauge.anvil_index_compaction_worker_limit "${line}")" \
      || return 1
    budget_limit="$(log_unsigned_field gauge.anvil_index_compaction_budget_limit "${line}")" \
      || return 1
    effective="$(log_unsigned_field compaction.effective_lanes "${line}")" \
      || return 1
    range_limit="$(log_unsigned_field gauge.anvil_index_compaction_range_limit "${line}")" \
      || return 1
    ranges="$(log_unsigned_field gauge.anvil_index_compaction_ranges_total "${line}")" \
      || return 1
    completed="$(log_unsigned_field gauge.anvil_index_compaction_ranges_completed "${line}")" \
      || return 1
    peak_active="$(log_unsigned_field gauge.anvil_index_compaction_peak_active_lanes "${line}")" \
      || return 1
    failures="$(log_unsigned_field monotonic_counter.anvil_index_compaction_failures_total "${line}")" \
      || return 1
    input_rate="$(log_number_field gauge.anvil_index_compaction_input_bytes_per_second "${line}")" \
      || return 1
    output_rate="$(log_number_field gauge.anvil_index_compaction_output_bytes_per_second "${line}")" \
      || return 1
    if ((configured != index_compaction_max_lanes \
      || worker_limit != index_rayon_workers \
      || budget_limit < index_compaction_max_lanes \
      || effective > index_compaction_max_lanes \
      || peak_active < 1 \
      || peak_active > effective \
      || ranges < 1 \
      || completed != ranges \
      || failures != 0)) \
      || unsigned_decimal_less_than "${range_limit}" "${effective}"
    then
      echo "production-shaped TypedJson emitted inconsistent terminal compaction telemetry" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    if ((effective == index_compaction_max_lanes \
      && ranges >= index_compaction_max_lanes)) \
      && ! unsigned_decimal_less_than \
        "${range_limit}" "${index_compaction_max_lanes}"
    then
      production_compaction_configured_lanes="${configured}"
      production_compaction_worker_limit="${worker_limit}"
      production_compaction_budget_limit="${budget_limit}"
      production_compaction_effective_lanes="${effective}"
      production_compaction_range_limit="${range_limit}"
      production_compaction_ranges_total="${ranges}"
      production_compaction_ranges_completed="${completed}"
      if ((peak_active > production_compaction_peak_active_lanes)); then
        production_compaction_peak_active_lanes="${peak_active}"
        production_compaction_input_rate="${input_rate}"
        production_compaction_output_rate="${output_rate}"
      fi
      terminal_found=1
    fi
  done < <(
    grep -F 'index compaction terminal metrics' \
      "${index_resource_qualification_log}" || true
  )
  if ((terminal_found == 0)); then
    echo "production-shaped TypedJson emitted no completed ${index_compaction_max_lanes}-lane compaction" >&2
    return 1
  fi

  while IFS= read -r line; do
    [[ "${line}" == *"index.kind=TypedJson"* ]] || continue
    active="$(log_unsigned_field gauge.anvil_index_compaction_active_lanes "${line}")" \
      || continue
    effective="$(log_unsigned_field compaction.effective_lanes "${line}")" \
      || continue
    input_rate="$(log_number_field gauge.anvil_index_compaction_input_bytes_per_second "${line}")" \
      || continue
    output_rate="$(log_number_field gauge.anvil_index_compaction_output_bytes_per_second "${line}")" \
      || continue
    if ((active >= 2 && effective == index_compaction_max_lanes)) \
      && { number_is_positive "${input_rate}" || number_is_positive "${output_rate}"; }
    then
      if ((active > production_compaction_peak_active_lanes)); then
        production_compaction_peak_active_lanes="${active}"
        production_compaction_input_rate="${input_rate}"
        production_compaction_output_rate="${output_rate}"
      fi
    fi
  done < <(
    grep -F 'index compaction progress' "${index_resource_qualification_log}" || true
  )
  if ((production_compaction_peak_active_lanes < 2)); then
    echo "production-shaped TypedJson terminal telemetry proved no concurrent compaction" >&2
    return 1
  fi
  echo "[anvil-single-qualification] production TypedJson used ${production_compaction_effective_lanes} effective lanes with ${production_compaction_peak_active_lanes} concurrently active"
}

assert_production_runtime_observability() {
  local line

  line="$(grep -F 'sampled process resources' "${index_resource_qualification_log}" | tail -n 1 || true)"
  if [[ -z "${line}" ]] \
    || [[ "$(log_unsigned_field gauge.anvil_process_memory_metrics_available "${line}" || true)" != "1" ]] \
    || ! log_unsigned_field gauge.anvil_process_resident_memory_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_process_virtual_memory_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_process_threads "${line}" >/dev/null
  then
    echo "production qualification emitted no complete process resource sample" >&2
    return 1
  fi
  production_process_samples="$(grep -Fc 'sampled process resources' "${index_resource_qualification_log}")"

  line="$(grep -F 'sampled cgroup memory resources' "${index_resource_qualification_log}" | tail -n 1 || true)"
  if [[ -z "${line}" ]] \
    || [[ "$(log_unsigned_field gauge.anvil_cgroup_memory_metrics_available "${line}" || true)" != "1" ]] \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_current_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_limit_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_limited "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_peak_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_low_events "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_high_events "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_max_events "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_oom_events "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_oom_kill_events "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_cgroup_memory_oom_group_kill_events "${line}" >/dev/null
  then
    echo "production qualification emitted no complete cgroup resource sample" >&2
    return 1
  fi
  production_cgroup_samples="$(grep -Fc 'sampled cgroup memory resources' "${index_resource_qualification_log}")"
  assert_zero_cgroup_oom_samples \
    "${index_resource_qualification_log}" "single-node production qualification"
  assert_capacity_samples "${index_resource_qualification_log}" \
    "single-node production qualification" 0

  line="$(grep -F 'sampled RocksDB resources' "${index_resource_qualification_log}" | tail -n 1 || true)"
  if [[ -z "${line}" ]] \
    || ! log_unsigned_field gauge.anvil_rocksdb_block_cache_capacity_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_rocksdb_block_cache_usage_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_rocksdb_block_cache_pinned_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_rocksdb_write_buffer_capacity_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_rocksdb_write_buffer_usage_bytes "${line}" >/dev/null \
    || ! log_unsigned_field gauge.anvil_rocksdb_unavailable_properties "${line}" >/dev/null
  then
    echo "production qualification emitted no complete RocksDB resource sample" >&2
    return 1
  fi
  production_rocksdb_samples="$(grep -Fc 'sampled RocksDB resources' "${index_resource_qualification_log}")"
  echo "[anvil-single-qualification] process, cgroup, and RocksDB operational signals were present during the production run"
}

write_index_resource_observability_report() {
  printf '%s\n' \
    '{' \
    '  "schema": "anvil.index-resource-observability.v1",' \
    '  "index_kind": "TypedJson",' \
    "  \"configured_lanes\": ${production_compaction_configured_lanes}," \
    "  \"worker_limit\": ${production_compaction_worker_limit}," \
    "  \"budget_limit\": ${production_compaction_budget_limit}," \
    "  \"effective_lanes\": ${production_compaction_effective_lanes}," \
    "  \"range_limit\": ${production_compaction_range_limit}," \
    "  \"ranges_total\": ${production_compaction_ranges_total}," \
    "  \"ranges_completed\": ${production_compaction_ranges_completed}," \
    "  \"peak_active_lanes\": ${production_compaction_peak_active_lanes}," \
    "  \"sample_input_bytes_per_second\": ${production_compaction_input_rate}," \
    "  \"sample_output_bytes_per_second\": ${production_compaction_output_rate}," \
    "  \"process_samples\": ${production_process_samples}," \
    "  \"cgroup_samples\": ${production_cgroup_samples}," \
    "  \"rocksdb_samples\": ${production_rocksdb_samples}" \
    '}' >"${index_resource_observability_report}"
  echo "[anvil-single-qualification] preserved observability report ${index_resource_observability_report}"
}

assert_index_resource_bounds() {
  local -A observed_kinds=()
  local configured
  local kind
  local line
  local observed=0
  local leased
  local peak_leased
  while IFS= read -r line; do
    if [[ "${line}" =~ index\.kind=(Path|MetadataFilter|TypedJson|FullText|Vector|Hybrid|GitSource|Tensor) ]]; then
      kind="${BASH_REMATCH[1]}"
    else
      continue
    fi
    configured="$(log_unsigned_field gauge.anvil_index_construction_configured_bytes "${line}")" \
      || continue
    leased="$(log_unsigned_field gauge.anvil_index_construction_leased_bytes "${line}")" \
      || return 1
    peak_leased="$(log_unsigned_field gauge.anvil_index_construction_peak_leased_bytes "${line}")" \
      || return 1
    if ((configured != index_kind_budget_bytes \
      || leased > configured \
      || peak_leased > configured)); then
      echo "single-node index construction exceeded or misstated its configured kind budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    observed_kinds["${kind}"]=1
    observed=$((observed + 1))
  done < <(
    grep -F 'index construction budget state' "${index_qualification_log}" || true
  )
  if ((observed == 0)); then
    echo "single-node index qualification emitted no construction budget evidence" >&2
    return 1
  fi
  for kind in "${index_kinds[@]}"; do
    if [[ -z "${observed_kinds[${kind}]:-}" ]]; then
      echo "single-node qualification emitted no ${kind} construction budget evidence" >&2
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
    resident="$(log_unsigned_field gauge.anvil_index_construction_resident_bytes "${line}")" \
      || return 1
    workspace="$(log_unsigned_field gauge.anvil_index_construction_workspace_bytes "${line}")" \
      || return 1
    if ((resident == 0 || resident + workspace > index_kind_budget_bytes)); then
      echo "${kind} emitted out-of-budget construction residency/workspace evidence" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resident_kinds["${kind}"]=1
  done < <(grep -F 'index L0 run flushed' "${index_qualification_log}" || true)
  for kind in "${index_kinds[@]}"; do
    if [[ -z "${resident_kinds[${kind}]:-}" ]]; then
      echo "single-node qualification emitted no ${kind} construction residency/workspace evidence" >&2
      return 1
    fi
  done

  local resource_budget_evidence=0
  while IFS= read -r line; do
    if [[ "${line}" != *"index.kind=TypedJson"* \
      || "${line}" != *"index construction budget state"* ]]; then
      continue
    fi
    configured="$(log_unsigned_field gauge.anvil_index_construction_configured_bytes "${line}")" \
      || {
      echo "production-shaped TypedJson build emitted malformed budget evidence" >&2
      printf '%s\n' "${line}" >&2
      return 1
    }
    leased="$(log_unsigned_field gauge.anvil_index_construction_leased_bytes "${line}")" \
      || return 1
    peak_leased="$(log_unsigned_field gauge.anvil_index_construction_peak_leased_bytes "${line}")" \
      || return 1
    if ((configured != index_kind_budget_bytes \
      || leased > configured \
      || peak_leased == 0 \
      || peak_leased > configured)); then
      echo "production-shaped TypedJson build exceeded or misstated its configured kind budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resource_budget_evidence=$((resource_budget_evidence + 1))
  done <"${index_resource_qualification_log}"
  if ((resource_budget_evidence == 0)); then
    echo "production-shaped TypedJson build emitted no fresh construction-budget evidence" >&2
    return 1
  fi

  local resource_residency_evidence=0
  while IFS= read -r line; do
    [[ "${line}" == *"index.kind=TypedJson"* ]] || continue
    resident="$(log_unsigned_field gauge.anvil_index_construction_resident_bytes "${line}")" \
      || return 1
    workspace="$(log_unsigned_field gauge.anvil_index_construction_workspace_bytes "${line}")" \
      || return 1
    if ((resident == 0 || resident + workspace > index_kind_budget_bytes)); then
      echo "production-shaped TypedJson build exceeded its residency/workspace budget" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    resource_residency_evidence=$((resource_residency_evidence + 1))
  done < <(grep -F 'index L0 run flushed' "${index_resource_qualification_log}" || true)
  if ((resource_residency_evidence == 0)); then
    echo "production-shaped TypedJson build emitted no fresh residency/workspace evidence" >&2
    return 1
  fi

  local cache_bytes
  cache_bytes="$(find "${data_dir}/index-cache" -type f -printf '%s\n' \
    | awk '{ total += $1 } END { print total + 0 }')"
  if ((cache_bytes > index_disk_cache_bytes)); then
    echo "single-node disposable index cache exceeded its ${index_disk_cache_bytes}-byte budget: ${cache_bytes}" >&2
    return 1
  fi
  assert_production_runtime_observability
  if [[ "${index_resource_scope}" == "release-corpus" ]]; then
    assert_production_typed_json_compaction_observability
    write_index_resource_observability_report
  fi
  echo "[anvil-single-qualification] preserved full production telemetry ${index_resource_telemetry_prefix}.log"
  echo "[anvil-single-qualification] index construction and disk cache remained within configured bounds"
}

restart_populated_node() {
  local before
  local deadline
  local elapsed
  local started
  before="$(index_sparse_start_count)"
  started="${SECONDS}"
  docker restart "${container_name}" >/dev/null
  deadline=$((SECONDS + 30))
  while ! docker exec \
    --env "ANVIL_CLIENT_ID=${owner_client}" \
    --env "ANVIL_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    anvil --endpoint http://127.0.0.1:50051 \
      head "${tenant}" index-journal-events docs/a.json >/dev/null 2>&1
  do
    if ((SECONDS >= deadline)); then
      echo "populated single-node server did not resume public reads within 30 seconds" >&2
      return 1
    fi
    sleep 1
  done
  public_endpoint="$(published_endpoint 50051 public)"
  elapsed=$((SECONDS - started))
  container_logs | preserve_startup_scan_evidence \
    "/var/tmp/anvil-v080-single-startup-scans-${qualification_suffix}.log"
  assert_sparse_index_startup "$((before + 1))"
  verify_existing_indexes
  verify_index_resource_state
  echo "[anvil-single-qualification] populated restart served in ${elapsed}s; sparse startup marker was present and no legacy definition barrier was reported"
}

assert_index_retention_converged() {
  local deadline=$((SECONDS + 65))
  local failed
  local index_id
  local pending
  local -a index_ids=()
  mapfile -t index_ids < <(
    sed -n 's/^[[:space:]]*"index_id":[[:space:]]*\([1-9][0-9]*\),[[:space:]]*$/\1/p' \
      "${index_verification_state}"
  )
  if ((${#index_ids[@]} != ${#index_kinds[@]})); then
    echo "single-node verification state did not contain all eight index IDs" >&2
    return 1
  fi
  while true; do
    failed="$(
      container_logs \
        | grep -F 'bounded index retention work failed' \
        | tail -n 1 || true
    )"
    if [[ -n "${failed}" ]]; then
      echo "single-node index retention reported failed work" >&2
      printf '%s\n' "${failed}" >&2
      return 1
    fi
    pending=0
    for index_id in "${index_ids[@]}"; do
      if ! container_logs | awk -v marker="index.id=${index_id} " '
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
        pending=$((pending + 1))
      fi
    done
    if ((pending == 0)); then
      echo "[anvil-single-qualification] bounded index retention deleted obsolete artifacts and drained its backlog"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "single-node index retention did not delete obsolete artifacts and drain its backlog within 65 seconds" >&2
      return 1
    fi
    sleep 1
  done
}

run_accounting_qualification() {
  ANVIL_ACCOUNTING_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_ACCOUNTING_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_ACCOUNTING_QUALIFICATION_BUCKET="single-accounting-${$}" \
  ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[accounting_qualification]}"
  echo "[anvil-single-qualification] accounting qualification passed"
}

run_atomic_index_qualification() {
  ANVIL_ATOMIC_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_BUCKET="atomic-index-single-${$}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[atomic_index_qualification]}"
  echo "[anvil-single-qualification] atomic-program index visibility passed"
}

assert_zero_accounting_traffic_drops() {
  local batches
  local bytes
  local count=0
  local line
  local node_id
  while IFS= read -r line; do
    node_id="$(log_unsigned_field node_id "${line}")" || {
      echo "single-node accounting drop evidence omitted node_id" >&2
      return 1
    }
    batches="$(log_unsigned_field dropped_batches_total "${line}")" || {
      echo "single-node accounting drop evidence omitted dropped_batches_total" >&2
      return 1
    }
    bytes="$(log_unsigned_field dropped_bytes_total "${line}")" || {
      echo "single-node accounting drop evidence omitted dropped_bytes_total" >&2
      return 1
    }
    if [[ "${node_id}" != "1" ]] || ((batches != 0 || bytes != 0)); then
      echo "single-node qualification reported node=${node_id} dropped_batches_total=${batches} dropped_bytes_total=${bytes}" >&2
      return 1
    fi
    count=$((count + 1))
  done < <(
    container_logs | grep -F 'anvil_accounting_traffic_drop_state' || true
  )
  if ((count == 0)); then
    echo "single-node qualification emitted no accounting drop-state evidence" >&2
    return 1
  fi
  echo "[anvil-single-qualification] accounting traffic reported zero dropped batches and bytes"
}

run_personaldb_qualification() {
  ANVIL_PERSONALDB_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_PERSONALDB_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_PERSONALDB_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_PERSONALDB_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[personaldb_qualification]}"
  echo "[anvil-single-qualification] PersonalDB qualification passed"
}

run_large_object_qualification() {
  local bucket="large-single-${$}"
  local input="${qualification_dir}/large-input.bin"
  local before_restart="${qualification_dir}/large-before-restart.bin"
  local after_restart="${qualification_dir}/large-after-restart.bin"
  local command=(
    docker exec
    --env "ANVIL_CLIENT_ID=${owner_client}"
    --env "ANVIL_CLIENT_SECRET=${owner_secret}"
    "${container_name}"
    anvil --endpoint http://127.0.0.1:50051
  )

  dd if=/dev/zero of="${input}" bs=1M count=2 status=none
  chmod 0444 "${input}"
  docker cp "${input}" "${container_name}:/tmp/anvil-large-input.bin"
  "${command[@]}" create-bucket "${bucket}" | grep -Fq "bucket=${bucket}"

  # One node cannot prove survival of one owner loss. REPLICATED must fail
  # closed without publishing a head; callers can explicitly choose LOCAL.
  if "${command[@]}" put "${tenant}" "${bucket}" fixtures/replicated.bin \
    /tmp/anvil-large-input.bin --command-id single-large-replicated \
    --durability replicated --if-absent >/dev/null 2>&1
  then
    echo "single-node large REPLICATED put unexpectedly succeeded" >&2
    return 1
  fi
  local failed_head
  failed_head="$("${command[@]}" head \
    "${tenant}" "${bucket}" fixtures/replicated.bin)"
  if [[ "${failed_head}" != "never-existed" ]]; then
    echo "failed REPLICATED put published an object head: ${failed_head}" >&2
    return 1
  fi

  "${command[@]}" put "${tenant}" "${bucket}" fixtures/large.bin \
    /tmp/anvil-large-input.bin --command-id single-large --durability local \
    --if-absent >/dev/null
  "${command[@]}" get "${tenant}" "${bucket}" fixtures/large.bin \
    --output /tmp/anvil-large-before-restart.bin
  docker cp "${container_name}:/tmp/anvil-large-before-restart.bin" "${before_restart}"
  cmp "${input}" "${before_restart}"

  docker restart "${container_name}" >/dev/null
  local attempt
  for attempt in $(seq 1 90); do
    if "${command[@]}" head "${tenant}" "${bucket}" fixtures/large.bin \
      >/dev/null 2>&1
    then
      break
    fi
    if ((attempt == 90)); then
      echo "single-node server did not recover the large object after restart" >&2
      return 1
    fi
    sleep 1
  done
  public_endpoint="$(published_endpoint 50051 public)"
  "${command[@]}" get "${tenant}" "${bucket}" fixtures/large.bin \
    --output /tmp/anvil-large-after-restart.bin
  docker cp "${container_name}:/tmp/anvil-large-after-restart.bin" "${after_restart}"
  cmp "${input}" "${after_restart}"
  docker exec --user 0 "${container_name}" rm -f \
    /tmp/anvil-large-input.bin \
    /tmp/anvil-large-before-restart.bin \
    /tmp/anvil-large-after-restart.bin
  echo "[anvil-single-qualification] large LOCAL object survived restart; REPLICATED failed closed"
}

run_s3_qualification() {
  ANVIL_S3_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_S3_QUALIFICATION_CLIENT_ID="${s3_client}" \
  ANVIL_S3_QUALIFICATION_CLIENT_SECRET="${s3_secret}" \
  ANVIL_S3_QUALIFICATION_BUCKET="s3-single-${$}" \
    "${qualification_example_binaries[s3_qualification]}"
  echo "[anvil-single-qualification] official AWS SDK S3 qualification passed"
}

run_git_qualification() {
  local bucket="git-single-${$}"
  local git_root="${qualification_dir}/git"
  local source_repository="${git_root}/source"
  local authenticated_clone="${git_root}/authenticated-clone"
  local denied_clone="${git_root}/denied-clone"
  local public_clone="${git_root}/public-clone"
  local git_url="${public_endpoint}/git/${tenant}/${bucket}/qualification.git"
  local authorization

  docker exec \
    --env "ANVIL_CLIENT_ID=${owner_client}" \
    --env "ANVIL_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    anvil --endpoint http://127.0.0.1:50051 create-bucket "${bucket}" \
    | grep -Fq "bucket=${bucket}"

  mkdir -p "${git_root}"
  git init --quiet --initial-branch=main "${source_repository}"
  git -C "${source_repository}" config user.name "Anvil Qualification"
  git -C "${source_repository}" config user.email "qualification@example.invalid"
  printf 'single-node smart HTTP gateway\n' >"${source_repository}/README.md"
  git -C "${source_repository}" add README.md
  git -C "${source_repository}" commit --quiet -m initial

  authorization="$(
    printf '%s:%s' "${owner_client}" "${owner_secret}" | base64 | tr -d '\n'
  )"
  git -C "${source_repository}" \
    -c "http.extraHeader=Authorization: Basic ${authorization}" \
    push --quiet "${git_url}" main
  git -c "http.extraHeader=Authorization: Basic ${authorization}" \
    clone --quiet --branch main "${git_url}" "${authenticated_clone}"
  cmp "${source_repository}/README.md" "${authenticated_clone}/README.md"

  if GIT_TERMINAL_PROMPT=0 git clone --quiet --branch main \
    "${git_url}" "${denied_clone}" >/dev/null 2>&1; then
    echo "private Git repository allowed an anonymous clone" >&2
    return 1
  fi

  docker exec \
    --env "ANVIL_CLIENT_ID=${owner_client}" \
    --env "ANVIL_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    anvil --endpoint http://127.0.0.1:50051 \
      set-bucket-public-read "${bucket}" enabled >/dev/null
  git clone --quiet --branch main "${git_url}" "${public_clone}"
  cmp "${source_repository}/README.md" "${public_clone}/README.md"

  echo "[anvil-single-qualification] Git push, authenticated clone, and public clone passed"
}

wait_for_bootstrap
assert_sparse_index_startup 1

tenant=qsingle
owner_app=qsingle-owner
owner_client=qsingle-client
owner_secret=qualification-single-owner-secret-00000000000000000000
provision_owner "${tenant}" "${owner_app}" "${owner_client}" "${owner_secret}"
index_resource_tenant="${tenant}"
index_resource_client="${owner_client}"
index_resource_secret="${owner_secret}"
if [[ "${qualification_mode}" == "release" ]]; then
  scale_baseline_resource_tenant=qsingle-scale
  scale_baseline_resource_client=qsingle-scale-client
  scale_baseline_resource_secret=qualification-single-scale-secret-000000000000000000000
  provision_owner "${scale_baseline_resource_tenant}" qsingle-scale-owner \
    "${scale_baseline_resource_client}" "${scale_baseline_resource_secret}"
fi

s3_tenant=qsingle-s3
s3_app=qsingle-s3-owner
s3_client=qsingle-s3-client
s3_secret=qualification-single-s3-secret-000000000000000000000
provision_owner "${s3_tenant}" "${s3_app}" "${s3_client}" "${s3_secret}"

public_endpoint="$(published_endpoint 50051 public)"

echo "[anvil-single-qualification] node ready public=${public_endpoint}"
case "${index_resource_scope}" in
  release-corpus)
    echo "[anvil-single-qualification] index resource scope=release-corpus records=839980 indexed_fields=12"
    ;;
  smoke)
    echo "[anvil-single-qualification] index resource scope=smoke records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
  custom)
    echo "[anvil-single-qualification] index resource scope=custom records=${index_resource_records}; this does not satisfy the required 839980-record release-resource gate"
    ;;
esac
run_large_object_qualification
run_public_read_qualification
run_index_qualification
run_atomic_index_qualification
if [[ "${qualification_mode}" == "release" ]]; then
  run_scale_baseline_resource_qualification single
fi
run_exact_resource_scale_qualification single
verify_index_resource_state
restart_populated_node
run_accounting_qualification
run_personaldb_qualification
run_s3_qualification
run_git_qualification
assert_index_retention_converged
assert_zero_accounting_traffic_drops

if [[ "${qualification_mode}" == "release" ]]; then
  echo "[anvil-single-qualification] PASS scope=release-corpus records=${index_resource_records} image=${image_id} platform=${platform}"
else
  echo "[anvil-single-qualification] SMOKE PASS records=${index_resource_records} image=${image_id} platform=${platform}"
fi
