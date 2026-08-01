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
  echo "[anvil-gate] start ${name}"
  set +e
  if [[ "${timeout_seconds}" != "0" ]] && command -v timeout >/dev/null 2>&1; then
    timeout --kill-after=30s "${timeout_seconds}s" "$@"
  else
    "$@"
  fi
  local status=$?
  set -e
  echo "[anvil-gate] finish ${name} status=${status} elapsed=$(($(date +%s) - started))s"
  echo "::endgroup::"
  return "${status}"
}

static_gates() {
  run_step "Rust formatting" cargo fmt --all -- --check
  run_step "locked workspace metadata" cargo metadata --locked --no-deps --format-version 1
  run_step "no external database gate" ./scripts/check-no-external-db.sh
}

rust_gates() {
  local test_threads="${ANVIL_RUST_TEST_THREADS:-4}"
  run_step "Anvil 0.5 workspace tests" cargo test --locked --workspace --all-targets -- \
    --nocapture \
    --test-threads="${test_threads}"
}

server_gates() {
  local test_threads="${ANVIL_RUST_TEST_THREADS:-4}"
  run_step "Anvil 0.5 server, client, and CLI tests" cargo test --locked \
    -p anvil-server \
    -p anvil-storage \
    -p anvil-storage-cli \
    --all-targets \
    -- \
    --nocapture \
    --test-threads="${test_threads}"
}

image_gates() (
  local configured_image="${ANVIL_IMAGE:-anvil:test}"
  local image
  image="$(./scripts/resolve-docker-image-id.sh "${configured_image}")"
  run_step "image server version" docker run --rm "${image}" anvil-server --version
  run_step "image CLI version" docker run --rm "${image}" anvil --version

  local scratch
  scratch="$(mktemp -d)"
  local container="anvil-v05-smoke-${$}"
  local token="anvil-v05-smoke-token"
  cleanup_image_gate() {
    docker rm --force "${container}" >/dev/null 2>&1 || true
    rm -rf "${scratch}"
  }
  trap cleanup_image_gate EXIT INT TERM

  printf 'anvil-0.5-smoke\n' >"${scratch}/payload"
  chmod 0444 "${scratch}/payload"
  docker run --detach --name "${container}" \
    --env ANVIL_LISTEN=0.0.0.0:50051 \
    --env ANVIL_DATA_DIR=/var/lib/anvil \
    --env ANVIL_NODE_ID=1 \
    --env ANVIL_API_TOKEN="${token}" \
    "${image}" >/dev/null

  local ready=0
  local attempt
  for attempt in $(seq 1 30); do
    local probe
    probe="$(
      docker run --rm --network "container:${container}" "${image}" \
        anvil --endpoint http://127.0.0.1:50051 --token "${token}" \
        head smoke missing probe 2>&1 || true
    )"
    if grep -Fq 'never-existed' <<<"${probe}"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != "1" ]]; then
    docker logs "${container}" >&2 || true
    echo "Anvil did not accept a gRPC request within 30 seconds" >&2
    return 1
  fi

  run_step "image authenticated put" docker run --rm \
    --network "container:${container}" \
    --volume "${scratch}:/smoke:ro" \
    "${image}" \
    anvil --endpoint http://127.0.0.1:50051 --token "${token}" \
    put smoke objects hello /smoke/payload \
      --command-id image-smoke --durability-class local --if-absent

  local value
  value="$(
    docker run --rm --network "container:${container}" "${image}" \
      anvil --endpoint http://127.0.0.1:50051 --token "${token}" \
      get smoke objects hello
  )"
  if [[ "${value}" != 'anvil-0.5-smoke' ]]; then
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
