#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
requested_image="${ANVIL_IMAGE:-anvil:0.7.0}"
keep="${ANVIL_QUALIFICATION_KEEP:-0}"
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
index_resource_max_anonymous_growth_bytes="${ANVIL_QUALIFICATION_INDEX_MAX_ANONYMOUS_GROWTH_BYTES:-536870912}"
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
command -v git >/dev/null 2>&1 || {
  echo "git is required for the smart HTTP gateway qualification" >&2
  exit 2
}

image_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}")"
server_version="$(docker run --rm --platform "${platform}" "${image_id}" anvil-server --version)"
client_version="$(docker run --rm --platform "${platform}" "${image_id}" anvil --version)"
if [[ "${server_version}" != "anvil-server 0.7.0" \
  || "${client_version}" != "anvil 0.7.0" ]]; then
  echo "qualification requires the exact Anvil 0.7.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
qualification_dir="$(mktemp -d /var/tmp/anvil-v070-single-qualification.XXXXXX)"
qualification_suffix="${qualification_dir##*.}"
container_name="anvil-v070-single-${qualification_suffix}"
data_dir="${qualification_dir}/data"
signing_key="${qualification_dir}/token-signing-key"
index_verification_state="${qualification_dir}/index-verification-state.json"
index_qualification_log="${qualification_dir}/index-qualification.log"
index_resource_qualification_log="${qualification_dir}/index-resource-qualification.log"
index_resource_report="/var/tmp/anvil-v070-single-index-resource-${qualification_suffix}.json"
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
  if [[ "${qualification_dir}" == /var/tmp/anvil-v070-single-qualification.* ]]; then
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

mkdir "${data_dir}"
chmod 0755 "${qualification_dir}"
head -c 64 /dev/urandom >"${signing_key}"
chmod 0600 "${signing_key}"
docker run --rm --user 0 \
  --volume "${qualification_dir}:/qualification" \
  "${image_id}" chown -R 10001:10001 \
    /qualification/data \
    /qualification/token-signing-key

docker run --detach \
  --name "${container_name}" \
  --platform "${platform}" \
  --publish 127.0.0.1::50051 \
  --env RUST_LOG=info,anvil::index_runtime::retention=debug \
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
  --env "ANVIL_INDEX_RAYON_WORKERS=${index_rayon_workers}" \
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

strip_ansi() {
  LC_ALL=C sed $'s/\033\\[[0-9;?]*[ -\\/]*[@-~]//g'
}

container_logs() {
  docker logs "${container_name}" 2>&1 | strip_ansi
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

index_qualification_log_start=0

capture_index_qualification_log_start() {
  index_qualification_log_start="$({ container_logs || true; } | wc -l)"
}

save_index_qualification_log() {
  local start_line=$((index_qualification_log_start + 1))
  container_logs | tail -n "+${start_line}" >"${index_qualification_log}"
}

assert_each_index_kind_published_and_compacted() {
  local kind
  local message
  for kind in "${index_kinds[@]}"; do
    for message in 'index generation published' 'index runs compacted'; do
      if ! awk -v kind="index.kind=${kind}" -v message="${message}" '
          index($0, kind) && index($0, message) { found = 1 }
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

run_public_read_qualification() {
  ANVIL_PUBLIC_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_PUBLIC_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_PUBLIC_QUALIFICATION_BUCKET=single-public-read \
  ANVIL_PUBLIC_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_PUBLIC_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example public_read_qualification
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
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example cluster_index_qualification
  test -s "${index_verification_state}"
  save_index_qualification_log
  assert_each_index_kind_published_and_compacted
  echo "[anvil-single-qualification] all-eight-index qualification passed"
}

verify_existing_indexes() {
  ANVIL_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
  ANVIL_INDEX_QUALIFICATION_STATE_INPUT="${index_verification_state}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example cluster_index_qualification
  echo "[anvil-single-qualification] final complete generations remained queryable after restart"
}

index_sparse_start_count() {
  container_logs \
    | grep -Fc 'index runtime starts from sparse assigned-definition state' \
    || true
}

startup_scan_evidence_count() {
  container_logs | grep -Fc 'anvil_index_startup_scan_evidence' || true
}

assert_zero_global_startup_scan_evidence() {
  local minimum_count="$1"
  local count=0
  local global_scans
  local line
  local node_id
  local scoped_scans
  while IFS= read -r line; do
    node_id="$(log_unsigned_field node_id "${line}")" || {
      echo "single-node startup scan evidence omitted node_id" >&2
      return 1
    }
    global_scans="$(log_unsigned_field global_head_scans_total "${line}")" || {
      echo "single-node startup scan evidence omitted its measured global scan count" >&2
      return 1
    }
    scoped_scans="$(log_unsigned_field scoped_head_scans_total "${line}")" || {
      echo "single-node startup scan evidence omitted its measured scoped scan count" >&2
      return 1
    }
    if [[ "${node_id}" != "1" ]] || ((global_scans != 0)); then
      echo "single-node startup reported node=${node_id} global_head_scans_total=${global_scans} scoped_head_scans_total=${scoped_scans}" >&2
      return 1
    fi
    count=$((count + 1))
  done < <(container_logs | grep -F 'anvil_index_startup_scan_evidence' || true)
  if ((count < minimum_count)); then
    echo "single-node startup emitted ${count} measured scan samples; expected at least ${minimum_count}" >&2
    return 1
  fi
}

assert_sparse_index_startup() {
  local minimum_count="$1"
  local observed
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
  resource_log_start="$({ container_logs || true; } | wc -l)"
  ANVIL_V06_RESOURCE_ENDPOINTS="${public_endpoint}" \
  ANVIL_V06_RESOURCE_TENANT="${tenant}" \
  ANVIL_V06_RESOURCE_BUCKET="index-resource-${qualification_suffix}" \
  ANVIL_V06_RESOURCE_CLIENT_ID="${owner_client}" \
  ANVIL_V06_RESOURCE_CLIENT_SECRET="${owner_secret}" \
  ANVIL_V06_RESOURCE_RECORDS="${index_resource_records}" \
  ANVIL_V06_RESOURCE_MUTATIONS="${index_resource_mutations}" \
  ANVIL_V06_RESOURCE_BATCH_SIZE=256 \
  ANVIL_V06_RESOURCE_WORKERS=4 \
  ANVIL_V06_RESOURCE_CONTAINERS="${container_name}" \
  ANVIL_V06_REQUIRE_RESOURCE_TARGETS=1 \
  ANVIL_V06_KIND_BUDGET_BYTES="${index_kind_budget_bytes}" \
  ANVIL_V06_INDEX_RAYON_WORKERS="${index_rayon_workers}" \
  ANVIL_V06_MAX_ANONYMOUS_GROWTH_BYTES="${index_resource_max_anonymous_growth_bytes}" \
  ANVIL_V06_RESOURCE_OUTPUT="${index_resource_report}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example v06_index_resource_qualification >/dev/null
  container_logs | tail -n "+$((resource_log_start + 1))" \
    >"${index_resource_qualification_log}"
  test -s "${index_resource_report}"
  grep -Eq "^[[:space:]]*\"records\":[[:space:]]*${index_resource_records},?[[:space:]]*$" \
    "${index_resource_report}"
  grep -Eq '^[[:space:]]*"indexed_fields":[[:space:]]*12,?[[:space:]]*$' \
    "${index_resource_report}"
  echo "[anvil-single-qualification] bounded index resource qualification passed scope=${index_resource_scope} records=${index_resource_records} kind_budget=${index_kind_budget_bytes} disk_cache=${index_disk_cache_bytes}"
  echo "[anvil-single-qualification] preserved resource report ${index_resource_report}"
}

assert_index_resource_bounds() {
  local -A observed_kinds=()
  local configured
  local kind
  local line
  local observed=0
  local peak
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
        echo "single-node index construction exceeded or misstated its configured kind budget" >&2
        printf '%s\n' "${line}" >&2
        return 1
      fi
      observed_kinds["${kind}"]=1
      observed=$((observed + 1))
    fi
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

  local resource_budget_evidence=0
  while IFS= read -r line; do
    if [[ "${line}" != *"index.kind=TypedJson"* \
      || "${line}" != *"index construction budget state"* ]]; then
      continue
    fi
    if [[ ! "${line}" =~ gauge\.anvil_index_construction_configured_bytes=([0-9]+).*gauge\.anvil_index_construction_used_bytes=([0-9]+).*gauge\.anvil_index_construction_peak_bytes=([0-9]+) ]]; then
      echo "production-shaped TypedJson build emitted malformed budget evidence" >&2
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

  local cache_bytes
  cache_bytes="$(find "${data_dir}/index-cache" -type f -printf '%s\n' \
    | awk '{ total += $1 } END { print total + 0 }')"
  if ((cache_bytes > index_disk_cache_bytes)); then
    echo "single-node disposable index cache exceeded its ${index_disk_cache_bytes}-byte budget: ${cache_bytes}" >&2
    return 1
  fi
  echo "[anvil-single-qualification] index construction and disk cache remained within configured bounds"
}

restart_populated_node() {
  local before
  local before_scan_evidence
  local deadline
  local elapsed
  local started
  before="$(index_sparse_start_count)"
  before_scan_evidence="$(startup_scan_evidence_count)"
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
  assert_sparse_index_startup "$((before + 1))"
  assert_zero_global_startup_scan_evidence "$((before_scan_evidence + 1))"
  verify_existing_indexes
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
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example accounting_qualification
  echo "[anvil-single-qualification] accounting qualification passed"
}

run_atomic_index_qualification() {
  ANVIL_ATOMIC_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_BUCKET="atomic-index-single-${$}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example atomic_index_qualification
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
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example personaldb_qualification
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
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example s3_qualification
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
run_index_resource_qualification
assert_index_resource_bounds
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
