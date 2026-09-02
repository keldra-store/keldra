#!/usr/bin/env bash
set -Eeuo pipefail

# Three-node release qualification for cluster formation, peer authentication,
# replicated/erasure payload durability, object semantics, accounting,
# PersonalDB, S3, and Git. Indexing is qualified separately by
# scripts/qualify-index-v6-ssd-scale.sh on the attested SSD kit.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/qualification-log-evidence.sh"
source "${repo_root}/scripts/qualification-three-node-phases.sh"
compose_file="${repo_root}/tests/cluster/docker-compose.yml"
start_node="${repo_root}/tests/cluster/start-node.sh"
requested_image="${KELDRA_IMAGE:-keldra:0.16.0}"
qualification_mode="${KELDRA_QUALIFICATION_MODE:-smoke}"
case "${qualification_mode}" in
  release|smoke) ;;
  *)
    echo "KELDRA_QUALIFICATION_MODE must be release or smoke" >&2
    exit 2
    ;;
esac
release_source_journal_max_entries="${KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES:-1000000}"
pressure_source_journal_max_entries="${KELDRA_QUALIFICATION_PRESSURE_SOURCE_JOURNAL_MAX_ENTRIES:-64}"
release_max_atomic_commit_entries="${KELDRA_QUALIFICATION_MAX_ATOMIC_COMMIT_ENTRIES:-4096}"
if [[ ! "${release_source_journal_max_entries}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES must be a positive decimal integer" >&2
  exit 2
fi
if [[ ! "${pressure_source_journal_max_entries}" =~ ^[1-9][0-9]*$ ]] \
  || ((pressure_source_journal_max_entries < 2))
then
  echo "KELDRA_QUALIFICATION_PRESSURE_SOURCE_JOURNAL_MAX_ENTRIES must be an integer of at least 2" >&2
  exit 2
fi
if [[ ! "${release_max_atomic_commit_entries}" =~ ^[1-9][0-9]*$ ]]; then
  echo "KELDRA_QUALIFICATION_MAX_ATOMIC_COMMIT_ENTRIES must be a positive decimal integer" >&2
  exit 2
fi
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
  echo "cargo is required for the public qualification clients" >&2
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
qualification_examples=(
  accounting_qualification
  atomic_program_qualification
  personaldb_qualification
  s3_qualification
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
if [[ "${server_version}" != "keldra-server 0.16.0" \
  || "${client_version}" != "keldra 0.16.0" ]]; then
  echo "qualification requires the exact Keldra 0.16.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
export KELDRA_IMAGE="${image_id}"
export KELDRA_QUALIFICATION_PROJECT="${KELDRA_QUALIFICATION_PROJECT:-keldra-v090-${$}}"
export KELDRA_QUALIFICATION_DIR="$(mktemp -d /var/tmp/keldra-v090-qualification.XXXXXX)"
export KELDRA_QUALIFICATION_START_NODE="${start_node}"
KELDRA_QUALIFICATION_STATE_DIR="${KELDRA_QUALIFICATION_DIR}/artifacts"
keep="${KELDRA_QUALIFICATION_KEEP:-0}"
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
for required in \
  prepare-node \
  provision-tenant \
  create-bucket \
  get-cluster-capabilities \
  activate-cluster-capabilities \
  clone-object \
  link-object \
  unlink-object
do
  if ! grep -Fq -- "${required}" <<<"${cli_help}"; then
    echo "qualification image is missing required CLI command ${required}" >&2
    exit 1
  fi
done

qualify_generalized_object_paths() {
  local capabilities=""
  local attempt
  for attempt in $(seq 1 60); do
    capabilities="$(run_bootstrap_cli keldra-1 get-cluster-capabilities 2>/dev/null || true)"
    if grep -Eq 'active_protocol=1 active_storage=1 target_protocol=2 target_storage=2 .*ready=true quiescent=true blocking_active_nodes=none' <<<"${capabilities}"; then
      break
    fi
    sleep 1
  done
  if ! grep -Eq 'active_protocol=1 active_storage=1 target_protocol=2 target_storage=2 .*ready=true quiescent=true blocking_active_nodes=none' <<<"${capabilities}"; then
    echo "three-node cluster did not become ready for capability 2/2: ${capabilities}" >&2
    return 1
  fi
  local placement_term placement_index
  placement_term="$(sed -n 's/.*placement_term=\([0-9][0-9]*\).*/\1/p' <<<"${capabilities}")"
  placement_index="$(sed -n 's/.*placement_index=\([0-9][0-9]*\).*/\1/p' <<<"${capabilities}")"
  if [[ ! "${placement_term}" =~ ^[1-9][0-9]*$ || ! "${placement_index}" =~ ^[1-9][0-9]*$ ]]; then
    echo "three-node capability status omitted an exact placement fence: ${capabilities}" >&2
    return 1
  fi
  run_bootstrap_cli keldra-1 activate-cluster-capabilities \
    --protocol-version 2 \
    --storage-format 2 \
    --expected-placement-term "${placement_term}" \
    --expected-placement-index "${placement_index}" >/dev/null

  printf 'three-node-clone-source\n' >"${KELDRA_QUALIFICATION_DIR}/artifacts/link-source.txt"
  printf 'three-node-linked-update\n' >"${KELDRA_QUALIFICATION_DIR}/artifacts/link-update.txt"
  chmod 0444 \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-source.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-update.txt"
  local source_version
  source_version="$(run_cli keldra-1 qprobe-client "${qprobe_secret}" \
    put qprobe objects links/target \
      /qualification/artifacts/link-source.txt \
      --command-id qprobe-link-source --durability replicated --if-absent)"
  if [[ ! "${source_version}" =~ ^[1-9][0-9]*$ ]]; then
    echo "three-node qualification received an invalid source version: ${source_version}" >&2
    return 1
  fi
  run_cli keldra-2 qprobe-client "${qprobe_secret}" \
    clone-object qprobe objects links/target "${source_version}" links/clone \
      --command-id qprobe-clone --durability replicated --if-absent >/dev/null
  run_cli keldra-3 qprobe-client "${qprobe_secret}" \
    link-object qprobe objects links/alias links/target \
      --command-id qprobe-link --durability replicated >/dev/null
  expect_failure "target delete with an inbound link" \
    run_cli keldra-2 qprobe-client "${qprobe_secret}" \
      delete qprobe objects links/target \
        --command-id qprobe-linked-target-delete --durability replicated
  run_cli keldra-2 qprobe-client "${qprobe_secret}" \
    put qprobe objects links/alias /qualification/artifacts/link-update.txt \
      --command-id qprobe-link-write --durability replicated >/dev/null
  run_cli keldra-1 qprobe-client "${qprobe_secret}" \
    get qprobe objects links/target \
      --output /qualification/artifacts/link-target-read.txt
  run_cli keldra-3 qprobe-client "${qprobe_secret}" \
    get qprobe objects links/alias \
      --output /qualification/artifacts/link-alias-read.txt
  run_cli keldra-2 qprobe-client "${qprobe_secret}" \
    get qprobe objects links/clone \
      --output /qualification/artifacts/link-clone-read.txt
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/link-update.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-target-read.txt"
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/link-update.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-alias-read.txt"
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/link-source.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-clone-read.txt"
  run_cli keldra-3 qprobe-client "${qprobe_secret}" \
    unlink-object qprobe objects links/alias \
      --command-id qprobe-unlink --durability replicated >/dev/null
  expect_failure "unlinked alias read" \
    run_cli keldra-1 qprobe-client "${qprobe_secret}" \
      get qprobe objects links/alias
  run_cli keldra-1 qprobe-client "${qprobe_secret}" \
    delete qprobe objects links/target \
      --command-id qprobe-target-delete-after-unlink --durability replicated >/dev/null
  rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/link-clone-read.txt"
  run_cli keldra-3 qprobe-client "${qprobe_secret}" \
    get qprobe objects links/clone \
      --output /qualification/artifacts/link-clone-read.txt
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/link-source.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/link-clone-read.txt"
  echo "[keldra-qualification] capability 2/2 clone and protected-link paths passed across three nodes"
}
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

log_cursor() {
  qualification_log_cursor
}

save_log_suffix() {
  local node="$1"
  local cursor="$2"
  local destination="$3"
  service_logs_since "${node}" "${cursor}" >"${destination}"
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

run_atomic_program_qualification() {
  KELDRA_ATOMIC_QUALIFICATION_ENDPOINTS="$(IFS=,; echo "${public_endpoints[*]}")" \
  KELDRA_ATOMIC_QUALIFICATION_TENANT=qatomic \
  KELDRA_ATOMIC_QUALIFICATION_BUCKET="atomic-program-three-${$}" \
  KELDRA_ATOMIC_QUALIFICATION_CLIENT_ID=qatomic-client \
  KELDRA_ATOMIC_QUALIFICATION_CLIENT_SECRET="${atomic_secret}" \
    "${qualification_binaries[atomic_program_qualification]}"
  echo "[keldra-qualification] distributed atomic multi-object program and replay passed"
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
qprobe_secret=qualification-probe-secret-000000000000000000000000
provision_tenant qprobe qprobe-client "${qprobe_secret}"
create_bucket keldra-1 qprobe-client \
  "${qprobe_secret}" objects

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

converged_qprobe_head=
wait_for_qprobe_head_after_growth() {
  local node="$1"
  local path="$2"
  local expected="$3"
  local attempt
  local actual=""
  local evidence="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-head-convergence.txt"
  for attempt in $(seq 1 90); do
    if run_cli "${node}" qprobe-client "${qprobe_secret}" \
      head qprobe objects "${path}" >"${evidence}" 2>&1
    then
      actual="$(<"${evidence}")"
      if [[ "${actual}" == "${expected}" ]]; then
        converged_qprobe_head="${actual}"
        return 0
      fi
    else
      actual="$(<"${evidence}")"
    fi
    sleep 1
  done
  echo "${node} did not converge on the existing head for ${path} after cluster growth" >&2
  echo "expected: ${expected}" >&2
  echo "last result: ${actual}" >&2
  return 1
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

wait_for_qprobe_head_after_growth \
  keldra-2 growth/from-one.bin "${growth_one_head}"
rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
run_cli keldra-2 qprobe-client \
  "${qprobe_secret}" \
    get qprobe objects growth/from-one.bin \
      --output /qualification/artifacts/growth-one-read.bin
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-one-read.bin"
require_qprobe_head keldra-2 growth/from-one.bin "${growth_one_head}"
echo "[keldra-qualification] two-node read preserved the pre-growth head and bytes"

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
run_cli keldra-1 qprobe-client \
  "${qprobe_secret}" \
    get qprobe objects growth/from-two.bin \
      --output /qualification/artifacts/growth-two-read.bin
cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-large.bin" \
  "${KELDRA_QUALIFICATION_DIR}/artifacts/growth-two-read.bin"
require_qprobe_head keldra-1 growth/from-two.bin "${growth_two_head}"
echo "[keldra-qualification] two-node REPLICATED read preserved its head and bytes"

export KELDRA_QUALIFICATION_MAX_ATOMIC_COMMIT_ENTRIES="$((pressure_source_journal_max_entries - 1))"
start_source_journal_phase "${pressure_source_journal_max_entries}" keldra-1 keldra-2
echo "[keldra-qualification] cutover pressure phase uses source-journal max entries ${pressure_source_journal_max_entries} and max atomic commit entries ${KELDRA_QUALIFICATION_MAX_ATOMIC_COMMIT_ENTRIES}"
prepare_no_event_membership_cutover_qualification \
  keldra-2 2 qprobe-client "${qprobe_secret}" qprobe objects \
  "${pressure_source_journal_max_entries}"
prepare_and_start_node 3
qualify_no_event_membership_cutover \
  keldra-2 2 qprobe-client "${qprobe_secret}" qprobe objects \
  "${pressure_source_journal_max_entries}"

for unavailable_node in keldra-1 keldra-2 keldra-3; do
  case "${unavailable_node}" in
    keldra-1) growth_reader=keldra-2 ;;
    keldra-2|keldra-3) growth_reader=keldra-1 ;;
  esac
  compose stop -t 30 "${unavailable_node}"
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
    wait_for_qprobe_head_after_growth \
      "${growth_reader}" "growth/${growth_object}.bin" "${growth_expected_head}"
    growth_output="${KELDRA_QUALIFICATION_DIR}/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    rm -f "${growth_output}"
    run_cli "${growth_reader}" qprobe-client \
      "${qprobe_secret}" \
      get qprobe objects "growth/${growth_object}.bin" \
        --output "/qualification/artifacts/growth-without-${unavailable_node}-${growth_object}.bin"
    cmp "${growth_expected}" "${growth_output}"
    require_qprobe_head \
      "${growth_reader}" "growth/${growth_object}.bin" "${growth_expected_head}"
  done
  compose start "${unavailable_node}"
  wait_for_node "${unavailable_node}"
  wait_for_qprobe_head_after_growth \
    "${unavailable_node}" growth/from-one.bin "${growth_one_head}"
  wait_for_qprobe_head_after_growth \
    "${unavailable_node}" growth/from-two.bin "${growth_two_head}"
done
echo "[keldra-qualification] three-node 2+1 reads preserved both large object heads and bytes through every single-node outage"
export KELDRA_QUALIFICATION_MAX_ATOMIC_COMMIT_ENTRIES="${release_max_atomic_commit_entries}"
start_release_source_journal_phase "${release_source_journal_max_entries}"

echo "[keldra-qualification] three-node cluster is ACTIVE"
qualify_generalized_object_paths

public_endpoints=()
for node in keldra-1 keldra-2 keldra-3; do
  public_endpoints+=("$(public_endpoint_for "${node}")")
done

echo "[keldra-qualification] indexing is qualified separately by scripts/qualify-index-v6-ssd-scale.sh"

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
run_atomic_program_qualification

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
  populated_restart_started="${SECONDS}"
  compose restart "${node}"
  wait_for_node "${node}"
  if ((SECONDS - populated_restart_started > 30)); then
    echo "${node} populated restart exceeded 30 seconds" >&2
    exit 1
  fi
  rm -f "${KELDRA_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  run_cli "${node}" qrestart-client "${restart_secret}" \
    get qrestart objects restart/value.txt \
    --output /qualification/artifacts/restart-read.txt
  cmp "${KELDRA_QUALIFICATION_DIR}/artifacts/restart.txt" \
    "${KELDRA_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  echo "[keldra-qualification] ${node} restart preserved the replicated object through its public endpoint"
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
echo "[keldra-qualification] rolling populated restart preserved replicated objects"
assert_zero_accounting_traffic_drops

if [[ "${qualification_mode}" == "release" ]]; then
  echo "[keldra-qualification] PASS non-index release phases image=${image_id} platform=${KELDRA_DOCKER_PLATFORM}"
else
  echo "[keldra-qualification] SMOKE PASS non-index phases image=${image_id} platform=${KELDRA_DOCKER_PLATFORM}"
fi
