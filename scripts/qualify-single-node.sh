#!/usr/bin/env bash
set -Eeuo pipefail

# Single-node release qualification for non-index storage, authentication,
# accounting, PersonalDB, S3, Git, and public-read behavior. Indexing is
# qualified separately by scripts/qualify-index-v6-ssd-scale.sh.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/qualification-log-evidence.sh"
requested_image="${KELDRA_IMAGE:-keldra:0.16.1}"
keep="${KELDRA_QUALIFICATION_KEEP:-0}"
qualification_mode="${KELDRA_QUALIFICATION_MODE:-smoke}"
case "${qualification_mode}" in
  release|smoke) ;;
  *)
    echo "KELDRA_QUALIFICATION_MODE must be release or smoke" >&2
    exit 2
    ;;
esac
qualification_examples=(
  accounting_qualification
  atomic_program_qualification
  personaldb_qualification
  public_read_qualification
  s3_qualification
)
declare -A qualification_example_binaries=()

case "${KELDRA_DOCKER_PLATFORM:-}" in
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
  linux/amd64|linux/arm64) platform="${KELDRA_DOCKER_PLATFORM}" ;;
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
server_version="$(docker run --rm --platform "${platform}" "${image_id}" keldra-server --version)"
client_version="$(docker run --rm --platform "${platform}" "${image_id}" keldra --version)"
if [[ "${server_version}" != "keldra-server 0.16.1" \
  || "${client_version}" != "keldra 0.16.1" ]]; then
  echo "qualification requires the exact Keldra 0.16.1 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
qualification_dir="$(mktemp -d /var/tmp/keldra-v090-single-qualification.XXXXXX)"
qualification_suffix="${qualification_dir##*.}"
container_name="keldra-v090-single-${qualification_suffix}"
data_dir="${qualification_dir}/data"
signing_key="${qualification_dir}/token-signing-key"
KELDRA_QUALIFICATION_STATE_DIR="${qualification_dir}"
qualification_build_messages="${qualification_dir}/qualification-client-build.jsonl"
container_started=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if ((container_started == 1)) && ((status != 0)); then
    echo "[keldra-single-qualification] FAILED; container logs follow" >&2
    docker logs "${container_name}" >&2 || true
  fi

  if [[ "${keep}" == "1" ]]; then
    echo "[keldra-single-qualification] retained container ${container_name}" >&2
    echo "[keldra-single-qualification] retained files ${qualification_dir}" >&2
    exit "${status}"
  fi

  if ((container_started == 1)); then
    docker rm --force "${container_name}" >/dev/null 2>&1 || true
  fi
  if [[ "${qualification_dir}" == /var/tmp/keldra-v090-single-qualification.* ]]; then
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
    --jobs "${CARGO_BUILD_JOBS:-1}"
    --quiet
    --release
    --locked
    --package keldra-server
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

  echo "[keldra-single-qualification] building public qualification clients in ${cargo_target_directory}"
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
  echo "[keldra-single-qualification] public qualification clients are ready; Cargo is no longer needed"
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

start_single_node() {
  docker run --detach \
    --name "${container_name}" \
    --platform "${platform}" \
    --publish 127.0.0.1::50051 \
    --env RUST_LOG=info \
    --env KELDRA_LISTEN=0.0.0.0:50051 \
    --env KELDRA_PEER_LISTEN=127.0.0.1:50052 \
    --env KELDRA_DATA_DIR=/var/lib/keldra \
    --env KELDRA_NODE_ID=1 \
    --env KELDRA_TOKEN_SIGNING_KEY_FILE=/run/secrets/keldra-token-signing-key \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=6000 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=1000 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=600 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100 \
    --env KELDRA_RUN_SYSTEM_BOOTSTRAP=true \
    --volume "${data_dir}:/var/lib/keldra" \
    --volume "${signing_key}:/run/secrets/keldra-token-signing-key:ro" \
    "${image_id}" >/dev/null
  container_started=1
}

start_single_node

wait_for_bootstrap() {
  local attempt
  for attempt in $(seq 1 90); do
    if docker exec "${container_name}" \
      test -f /var/lib/keldra/system-bootstrap-credential.json \
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
      --env "KELDRA_NEW_CLIENT_SECRET=${provisioned_secret}" \
      "${container_name}" \
      keldra --endpoint http://127.0.0.1:50051 \
        --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
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

container_logs_since() {
  local cursor="$1"
  local until="${2:-}"
  if [[ -n "${until}" ]]; then
    docker logs --since "${cursor}" --until "${until}" \
      "${container_name}" 2>&1 | strip_ansi
  else
    docker logs --since "${cursor}" "${container_name}" 2>&1 | strip_ansi
  fi
}

run_public_read_qualification() {
  KELDRA_PUBLIC_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  KELDRA_PUBLIC_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_PUBLIC_QUALIFICATION_BUCKET=single-public-read \
  KELDRA_PUBLIC_QUALIFICATION_CLIENT_ID="${owner_client}" \
  KELDRA_PUBLIC_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[public_read_qualification]}"
  echo "[keldra-single-qualification] public-read qualification passed"
}

restart_populated_node() {
  local deadline
  local elapsed
  local started
  started="${SECONDS}"
  docker restart "${container_name}" >/dev/null
  deadline=$((SECONDS + 30))
  while ! docker exec \
    --env "KELDRA_CLIENT_ID=${owner_client}" \
    --env "KELDRA_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    keldra --endpoint http://127.0.0.1:50051 \
      head "${tenant}" "${restart_probe_bucket}" fixtures/large.bin >/dev/null 2>&1
  do
    if ((SECONDS >= deadline)); then
      echo "populated single-node server did not resume public reads within 30 seconds" >&2
      return 1
    fi
    sleep 1
  done
  public_endpoint="$(published_endpoint 50051 public)"
  elapsed=$((SECONDS - started))
  echo "[keldra-single-qualification] populated restart served ordinary reads in ${elapsed}s"
}

run_accounting_qualification() {
  KELDRA_ACCOUNTING_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  KELDRA_ACCOUNTING_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_ACCOUNTING_QUALIFICATION_BUCKET="single-accounting-${$}" \
  KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_ID="${owner_client}" \
  KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[accounting_qualification]}"
  echo "[keldra-single-qualification] accounting qualification passed"
}

run_atomic_program_qualification() {
  KELDRA_ATOMIC_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  KELDRA_ATOMIC_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_ATOMIC_QUALIFICATION_BUCKET="atomic-program-single-${$}" \
  KELDRA_ATOMIC_QUALIFICATION_CLIENT_ID="${owner_client}" \
  KELDRA_ATOMIC_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[atomic_program_qualification]}"
  echo "[keldra-single-qualification] atomic multi-object program and replay passed"
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
    container_logs | grep -F 'keldra_accounting_traffic_drop_state' || true
  )
  if ((count == 0)); then
    echo "single-node qualification emitted no accounting drop-state evidence" >&2
    return 1
  fi
  echo "[keldra-single-qualification] accounting traffic reported zero dropped batches and bytes"
}

run_personaldb_qualification() {
  KELDRA_PERSONALDB_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  KELDRA_PERSONALDB_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_PERSONALDB_QUALIFICATION_CLIENT_ID="${owner_client}" \
  KELDRA_PERSONALDB_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
    "${qualification_example_binaries[personaldb_qualification]}"
  echo "[keldra-single-qualification] PersonalDB qualification passed"
}

run_large_object_qualification() {
  local bucket="${restart_probe_bucket}"
  local input="${qualification_dir}/large-input.bin"
  local before_restart="${qualification_dir}/large-before-restart.bin"
  local after_restart="${qualification_dir}/large-after-restart.bin"
  local command=(
    docker exec
    --env "KELDRA_CLIENT_ID=${owner_client}"
    --env "KELDRA_CLIENT_SECRET=${owner_secret}"
    "${container_name}"
    keldra --endpoint http://127.0.0.1:50051
  )

  dd if=/dev/zero of="${input}" bs=1M count=2 status=none
  chmod 0444 "${input}"
  docker cp "${input}" "${container_name}:/tmp/keldra-large-input.bin"
  "${command[@]}" create-bucket "${bucket}" | grep -Fq "bucket=${bucket}"

  # One node cannot prove survival of one owner loss. REPLICATED must fail
  # closed without publishing a head; callers can explicitly choose LOCAL.
  if "${command[@]}" put "${tenant}" "${bucket}" fixtures/replicated.bin \
    /tmp/keldra-large-input.bin --command-id single-large-replicated \
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
    /tmp/keldra-large-input.bin --command-id single-large --durability local \
    --if-absent >/dev/null
  "${command[@]}" get "${tenant}" "${bucket}" fixtures/large.bin \
    --output /tmp/keldra-large-before-restart.bin
  docker cp "${container_name}:/tmp/keldra-large-before-restart.bin" "${before_restart}"
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
    --output /tmp/keldra-large-after-restart.bin
  docker cp "${container_name}:/tmp/keldra-large-after-restart.bin" "${after_restart}"
  cmp "${input}" "${after_restart}"
  docker exec --user 0 "${container_name}" rm -f \
    /tmp/keldra-large-input.bin \
    /tmp/keldra-large-before-restart.bin \
    /tmp/keldra-large-after-restart.bin
  echo "[keldra-single-qualification] large LOCAL object survived restart; REPLICATED failed closed"
}

run_s3_qualification() {
  KELDRA_S3_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  KELDRA_S3_QUALIFICATION_CLIENT_ID="${s3_client}" \
  KELDRA_S3_QUALIFICATION_CLIENT_SECRET="${s3_secret}" \
  KELDRA_S3_QUALIFICATION_BUCKET="s3-single-${$}" \
    "${qualification_example_binaries[s3_qualification]}"
  echo "[keldra-single-qualification] official AWS SDK S3 qualification passed"
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
    --env "KELDRA_CLIENT_ID=${owner_client}" \
    --env "KELDRA_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    keldra --endpoint http://127.0.0.1:50051 create-bucket "${bucket}" \
    | grep -Fq "bucket=${bucket}"

  mkdir -p "${git_root}"
  git init --quiet --initial-branch=main "${source_repository}"
  git -C "${source_repository}" config user.name "Keldra Qualification"
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
    --env "KELDRA_CLIENT_ID=${owner_client}" \
    --env "KELDRA_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    keldra --endpoint http://127.0.0.1:50051 \
      set-bucket-public-read "${bucket}" enabled >/dev/null
  git clone --quiet --branch main "${git_url}" "${public_clone}"
  cmp "${source_repository}/README.md" "${public_clone}/README.md"

  echo "[keldra-single-qualification] Git push, authenticated clone, and public clone passed"
}

wait_for_bootstrap

tenant=qsingle
owner_app=qsingle-owner
owner_client=qsingle-client
owner_secret=qualification-single-owner-secret-00000000000000000000
provision_owner "${tenant}" "${owner_app}" "${owner_client}" "${owner_secret}"
s3_tenant=qsingle-s3
s3_app=qsingle-s3-owner
s3_client=qsingle-s3-client
s3_secret=qualification-single-s3-secret-000000000000000000000

public_endpoint="$(published_endpoint 50051 public)"
restart_probe_bucket="large-single-${$}"

echo "[keldra-single-qualification] node ready public=${public_endpoint}"
echo "[keldra-single-qualification] indexing is qualified separately by scripts/qualify-index-v6-ssd-scale.sh"

run_large_object_qualification
run_public_read_qualification
run_atomic_program_qualification
restart_populated_node
run_accounting_qualification
run_personaldb_qualification
provision_owner "${s3_tenant}" "${s3_app}" "${s3_client}" "${s3_secret}"
run_s3_qualification
run_git_qualification
assert_zero_accounting_traffic_drops

if [[ "${qualification_mode}" == "release" ]]; then
  echo "[keldra-single-qualification] PASS non-index release phases image=${image_id} platform=${platform}"
else
  echo "[keldra-single-qualification] SMOKE PASS non-index phases image=${image_id} platform=${platform}"
fi
