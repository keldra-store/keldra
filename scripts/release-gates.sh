#!/usr/bin/env bash
set -euo pipefail

group="${1:-all}"

run_step() {
  local name="$1"
  shift
  local timeout_seconds="${ANVIL_GATE_STEP_TIMEOUT_SECONDS:-1800}"
  local started
  started="$(date +%s)"
  echo "::group::${name}"
  echo "[keldra-gate] start ${name}"
  set +e
  if [[ "${timeout_seconds}" != "0" ]] && command -v timeout >/dev/null 2>&1; then
    timeout --kill-after=30s "${timeout_seconds}s" "$@"
  else
    "$@"
  fi
  local status=$?
  set -e
  echo "[keldra-gate] finish ${name} status=${status} elapsed=$(($(date +%s) - started))s"
  echo "::endgroup::"
  return "${status}"
}

static_gates() {
  run_step "Rust source file size" ./scripts/check-rust-file-size.sh
  run_step "Rust formatting" cargo fmt --all -- --check
  run_step "locked workspace metadata" cargo metadata --locked --no-deps --format-version 1
  run_step "no external database gate" ./scripts/check-no-external-db.sh
  run_step "single public multiarchitecture image" ./scripts/check-release-image-surface.sh
}

rust_gates() {
  local build_jobs="${CARGO_BUILD_JOBS:-1}"
  local test_threads="${ANVIL_RUST_TEST_THREADS:-4}"
  run_step "Keldra 0.10 workspace Clippy" cargo clippy --jobs "${build_jobs}" --locked --workspace \
    --all-targets \
    --no-deps
  run_step "Keldra 0.10 workspace tests" cargo test --jobs "${build_jobs}" --locked --workspace --all-targets -- \
    --nocapture \
    --test-threads="${test_threads}"
}

server_gates() {
  local build_jobs="${CARGO_BUILD_JOBS:-1}"
  local test_threads="${ANVIL_RUST_TEST_THREADS:-4}"
  run_step "Keldra 0.10 server, client, and CLI tests" cargo test --jobs "${build_jobs}" --locked \
    -p keldra-server \
    -p keldra \
    -p keldra-cli \
    --all-targets \
    -- \
    --nocapture \
    --test-threads="${test_threads}"
}

image_gates() (
  local configured_image="${ANVIL_IMAGE:-keldra:test}"
  local image
  image="$(./scripts/resolve-docker-image-id.sh "${configured_image}")"
  run_step "image server version" docker run --rm "${image}" keldra-server --version
  run_step "image CLI version" docker run --rm "${image}" keldra --version

  local scratch
  scratch="$(mktemp -d)"
  chmod 0755 "${scratch}"
  local container="keldra-v09-smoke-${$}"
  cleanup_image_gate() {
    docker rm --force "${container}" >/dev/null 2>&1 || true
    docker run --rm --user 0 --volume "${scratch}:/smoke" "${image}" \
      rm -rf /smoke/data /smoke/signing-key /smoke/payload >/dev/null 2>&1 || true
    rm -rf "${scratch}"
  }
  trap cleanup_image_gate EXIT INT TERM

  mkdir "${scratch}/data"
  chmod 0777 "${scratch}/data"
  head -c 64 /dev/urandom >"${scratch}/signing-key"
  chmod 0600 "${scratch}/signing-key"
  docker run --rm --user 0 --volume "${scratch}:/smoke" "${image}" \
    chown 10001:10001 /smoke/signing-key
  printf 'keldra-0.1-smoke\n' >"${scratch}/payload"
  chmod 0444 "${scratch}/payload"
  docker run --detach --name "${container}" \
    --env ANVIL_LISTEN=0.0.0.0:50051 \
    --env ANVIL_DATA_DIR=/var/lib/keldra \
    --env ANVIL_NODE_ID=1 \
    --env ANVIL_TOKEN_SIGNING_KEY_FILE=/run/secrets/keldra-token-signing-key \
    --env ANVIL_RUN_SYSTEM_BOOTSTRAP=true \
    --volume "${scratch}/data:/var/lib/keldra" \
    --volume "${scratch}/signing-key:/run/secrets/keldra-token-signing-key:ro" \
    "${image}" >/dev/null

  local owner_client_id="smoke-owner-client"
  local owner_client_secret="smoke-owner-secret-with-at-least-32-bytes"
  local ready=0
  local attempt
  for attempt in $(seq 1 30); do
    local probe
    probe="$(
      docker run --rm --network "container:${container}" \
        --volume "${scratch}/data:/var/lib/keldra:ro" \
        --env ANVIL_NEW_CLIENT_SECRET="${owner_client_secret}" \
        "${image}" \
        keldra --endpoint http://127.0.0.1:50051 \
        --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
        provision-tenant smoke smoke-owner "${owner_client_id}" 2>&1 || true
    )"
    if grep -Fq 'tenant=smoke' <<<"${probe}"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != "1" ]]; then
    docker logs "${container}" >&2 || true
    echo "Anvil did not bootstrap and provision the smoke tenant within 30 seconds" >&2
    return 1
  fi

  run_step "image authenticated bucket provisioning" docker run --rm \
    --network "container:${container}" \
    --env ANVIL_CLIENT_ID="${owner_client_id}" \
    --env ANVIL_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 create-bucket objects

  docker run --rm --volume "${scratch}/data:/var/lib/keldra" "${image}" \
    rm /var/lib/keldra/system-bootstrap-credential.json
  if [[ -e "${scratch}/data/system-bootstrap-credential.json" ]]; then
    echo "generated bootstrap credential was not deleted after provisioning" >&2
    return 1
  fi

  run_step "image authenticated put" docker run --rm \
    --network "container:${container}" \
    --volume "${scratch}:/smoke:ro" \
    --env ANVIL_CLIENT_ID="${owner_client_id}" \
    --env ANVIL_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    put smoke objects hello /smoke/payload \
      --command-id image-smoke --durability local --if-absent

  local value
  value="$(
    docker run --rm --network "container:${container}" \
      --env ANVIL_CLIENT_ID="${owner_client_id}" \
      --env ANVIL_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" \
      keldra --endpoint http://127.0.0.1:50051 \
      get smoke objects hello
  )"
  if [[ "${value}" != 'keldra-0.1-smoke' ]]; then
    echo "image smoke read returned unexpected bytes" >&2
    return 1
  fi
)

case "${group}" in
  all)
    static_gates
    rust_gates
    ;;
  static)
    static_gates
    ;;
  rust)
    rust_gates
    ;;
  server)
    server_gates
    ;;
  image)
    image_gates
    ;;
  *)
    echo "usage: $0 [all|static|rust|server|image]" >&2
    exit 2
    ;;
esac
