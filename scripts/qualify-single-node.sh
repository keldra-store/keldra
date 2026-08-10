#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
requested_image="${ANVIL_IMAGE:-anvil:0.6.0}"
keep="${ANVIL_QUALIFICATION_KEEP:-0}"

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
if [[ "${server_version}" != "anvil-server 0.6.0" \
  || "${client_version}" != "anvil 0.6.0" ]]; then
  echo "qualification requires the exact Anvil 0.6.0 image" >&2
  echo "server: ${server_version}" >&2
  echo "client: ${client_version}" >&2
  exit 2
fi
qualification_dir="$(mktemp -d /var/tmp/anvil-v060-single-qualification.XXXXXX)"
qualification_suffix="${qualification_dir##*.}"
container_name="anvil-v060-single-${qualification_suffix}"
data_dir="${qualification_dir}/data"
signing_key="${qualification_dir}/token-signing-key"
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
  if [[ "${qualification_dir}" == /var/tmp/anvil-v060-single-qualification.* ]]; then
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
  --env RUST_LOG=info \
  --env ANVIL_LISTEN=0.0.0.0:50051 \
  --env ANVIL_PEER_LISTEN=127.0.0.1:50052 \
  --env ANVIL_DATA_DIR=/var/lib/anvil \
  --env ANVIL_NODE_ID=1 \
  --env ANVIL_TOKEN_SIGNING_KEY_FILE=/run/secrets/anvil-token-signing-key \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=6000 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=1000 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=600 \
  --env ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100 \
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
  ANVIL_INDEX_QUALIFICATION_ENDPOINTS="${public_endpoint}" \
  ANVIL_INDEX_QUALIFICATION_TENANT="${tenant}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_ID="${owner_client}" \
  ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET="${owner_secret}" \
  ANVIL_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE=1 \
    cargo run --quiet --locked --package anvil-server \
      --manifest-path "${repo_root}/Cargo.toml" \
      --example cluster_index_qualification
  echo "[anvil-single-qualification] all-eight-index qualification passed"
}

assert_index_retention_converged() {
  local deadline=$((SECONDS + 65))
  local completed
  local deferred
  while true; do
    deferred="$(
      docker logs "${container_name}" 2>&1 \
        | grep -E 'obsolete index cleanup( reload)? deferred' \
        | tail -n 1 || true
    )"
    if [[ -n "${deferred}" ]]; then
      echo "single-node index retention reported deferred cleanup" >&2
      printf '%s\n' "${deferred}" >&2
      return 1
    fi
    completed="$(
      docker logs "${container_name}" 2>&1 \
        | grep -F 'idle obsolete index cleanup completed' \
        | tail -n 1 || true
    )"
    if [[ -n "${completed}" ]]; then
      echo "[anvil-single-qualification] idle obsolete index cleanup converged"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "single-node index retention did not complete within 65 seconds" >&2
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
run_large_object_qualification
run_public_read_qualification
run_index_qualification
run_accounting_qualification
run_personaldb_qualification
run_s3_qualification
run_git_qualification
assert_index_retention_converged

echo "[anvil-single-qualification] PASS image=${image_id} platform=${platform}"
