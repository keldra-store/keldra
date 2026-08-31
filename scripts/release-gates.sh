#!/usr/bin/env bash
set -euo pipefail

group="${1:-all}"

run_step() {
  local name="$1"
  shift
  local timeout_seconds="${KELDRA_GATE_STEP_TIMEOUT_SECONDS:-1800}"
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
  run_step "Zanzibar submodule and version alignment" ./scripts/check-zanzibar-integration.sh
  run_step "Rust source file size" ./scripts/check-rust-file-size.sh
  run_step "Rust formatting" cargo fmt --all -- --check
  run_step "locked workspace metadata" cargo metadata --locked --no-deps --format-version 1
  run_step "no external database gate" ./scripts/check-no-external-db.sh
  run_step "single public multiarchitecture image" ./scripts/check-release-image-surface.sh
}

rust_gates() {
  local build_jobs="${CARGO_BUILD_JOBS:-1}"
  local test_threads="${KELDRA_RUST_TEST_THREADS:-4}"
  run_step "Keldra workspace Clippy" cargo clippy --jobs "${build_jobs}" --locked --workspace \
    --all-targets \
    --no-deps
  run_step "Keldra workspace tests" cargo test --jobs "${build_jobs}" --locked --workspace --all-targets -- \
    --nocapture \
    --test-threads="${test_threads}"
}

server_gates() {
  local build_jobs="${CARGO_BUILD_JOBS:-1}"
  local test_threads="${KELDRA_RUST_TEST_THREADS:-4}"
  run_step "Keldra server, client, and CLI tests" cargo test --jobs "${build_jobs}" --locked \
    -p keldra-server \
    -p keldra \
    -p keldra-cli \
    --all-targets \
    -- \
    --nocapture \
    --test-threads="${test_threads}"
}

image_gates() (
  local configured_image="${KELDRA_IMAGE:-keldra:test}"
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
      rm -rf /smoke/data /smoke/signing-key /smoke/payload /smoke/replacement \
      >/dev/null 2>&1 || true
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
  printf 'keldra-0.15-linked-update\n' >"${scratch}/replacement"
  chmod 0444 "${scratch}/payload"
  chmod 0444 "${scratch}/replacement"
  docker run --detach --name "${container}" \
    --env KELDRA_LISTEN=0.0.0.0:50051 \
    --env KELDRA_DATA_DIR=/var/lib/keldra \
    --env KELDRA_NODE_ID=1 \
    --env KELDRA_TOKEN_SIGNING_KEY_FILE=/run/secrets/keldra-token-signing-key \
    --env KELDRA_RUN_SYSTEM_BOOTSTRAP=true \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=1000000 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=100000 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=1000000 \
    --env KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100000 \
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
        --env KELDRA_NEW_CLIENT_SECRET="${owner_client_secret}" \
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
    echo "Keldra did not bootstrap and provision the smoke tenant within 30 seconds" >&2
    return 1
  fi

  local capabilities=""
  for attempt in $(seq 1 30); do
    capabilities="$(
      docker run --rm --network "container:${container}" \
        --volume "${scratch}/data:/var/lib/keldra:ro" \
        "${image}" \
        keldra --endpoint http://127.0.0.1:50051 \
        --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
        get-cluster-capabilities 2>/dev/null || true
    )"
    if grep -Eq 'active_protocol=1 active_storage=1 target_protocol=2 target_storage=2 .*ready=true quiescent=true blocking_active_nodes=none' <<<"${capabilities}"; then
      break
    fi
    sleep 1
  done
  if ! grep -Eq 'active_protocol=1 active_storage=1 target_protocol=2 target_storage=2 .*ready=true quiescent=true blocking_active_nodes=none' <<<"${capabilities}"; then
    echo "Keldra did not become ready for capability 2/2 activation: ${capabilities}" >&2
    return 1
  fi
  local placement_term placement_index
  placement_term="$(sed -n 's/.*placement_term=\([0-9][0-9]*\).*/\1/p' <<<"${capabilities}")"
  placement_index="$(sed -n 's/.*placement_index=\([0-9][0-9]*\).*/\1/p' <<<"${capabilities}")"
  if [[ ! "${placement_term}" =~ ^[1-9][0-9]*$ || ! "${placement_index}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Keldra returned an invalid capability placement fence: ${capabilities}" >&2
    return 1
  fi
  run_step "image capability 2/2 activation" docker run --rm \
    --network "container:${container}" \
    --volume "${scratch}/data:/var/lib/keldra:ro" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    --credentials-file /var/lib/keldra/system-bootstrap-credential.json \
    activate-cluster-capabilities \
      --protocol-version 2 \
      --storage-format 2 \
      --expected-placement-term "${placement_term}" \
      --expected-placement-index "${placement_index}"

  run_step "image authenticated bucket provisioning" docker run --rm \
    --network "container:${container}" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
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
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    put smoke objects hello /smoke/payload \
      --command-id image-smoke --durability local --if-absent

  local value
  value="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" \
      keldra --endpoint http://127.0.0.1:50051 \
      get smoke objects hello
  )"
  if [[ "${value}" != 'keldra-0.1-smoke' ]]; then
    echo "image smoke read returned unexpected bytes" >&2
    return 1
  fi

  local source_head source_version
  source_head="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" \
      keldra --endpoint http://127.0.0.1:50051 \
      head smoke objects hello
  )"
  source_version="$(sed -n 's/^present version=\([0-9][0-9]*\) .*/\1/p' <<<"${source_head}")"
  if [[ ! "${source_version}" =~ ^[1-9][0-9]*$ ]]; then
    echo "image smoke could not resolve the source version: ${source_head}" >&2
    return 1
  fi

  run_step "image zero-copy clone" docker run --rm \
    --network "container:${container}" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    clone-object smoke objects hello "${source_version}" cloned \
      --command-id image-smoke-clone --durability local --if-absent

  run_step "image protected link" docker run --rm \
    --network "container:${container}" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    link-object smoke objects linked hello \
      --command-id image-smoke-link --durability local

  local delete_output delete_status
  set +e
  delete_output="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" \
      keldra --endpoint http://127.0.0.1:50051 \
      delete smoke objects hello --command-id image-smoke-blocked-delete 2>&1
  )"
  delete_status=$?
  set -e
  if [[ "${delete_status}" == "0" ]] || ! grep -Fq 'object target cannot be deleted while inbound links exist' <<<"${delete_output}"; then
    echo "target delete was not fenced by its inbound link: ${delete_output}" >&2
    return 1
  fi

  run_step "image link write-through" docker run --rm \
    --network "container:${container}" \
    --volume "${scratch}:/smoke:ro" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    put smoke objects linked /smoke/replacement \
      --command-id image-smoke-linked-put --durability local

  local canonical_value clone_value linked_value
  canonical_value="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" keldra --endpoint http://127.0.0.1:50051 get smoke objects hello
  )"
  linked_value="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" keldra --endpoint http://127.0.0.1:50051 get smoke objects linked
  )"
  clone_value="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" keldra --endpoint http://127.0.0.1:50051 get smoke objects cloned
  )"
  if [[ "${canonical_value}" != 'keldra-0.15-linked-update' \
    || "${linked_value}" != "${canonical_value}" \
    || "${clone_value}" != 'keldra-0.1-smoke' ]]; then
    echo "clone independence or link write-through changed" >&2
    return 1
  fi

  run_step "image protected unlink" docker run --rm \
    --network "container:${container}" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    unlink-object smoke objects linked \
      --command-id image-smoke-unlink --durability local

  local unlinked_output unlinked_status
  set +e
  unlinked_output="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" \
      keldra --endpoint http://127.0.0.1:50051 get smoke objects linked 2>&1
  )"
  unlinked_status=$?
  set -e
  if [[ "${unlinked_status}" == "0" ]]; then
    echo "unlinked path remained readable: ${unlinked_output}" >&2
    return 1
  fi

  run_step "image target delete after unlink" docker run --rm \
    --network "container:${container}" \
    --env KELDRA_CLIENT_ID="${owner_client_id}" \
    --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
    "${image}" \
    keldra --endpoint http://127.0.0.1:50051 \
    delete smoke objects hello --command-id image-smoke-delete-after-unlink

  clone_value="$(
    docker run --rm --network "container:${container}" \
      --env KELDRA_CLIENT_ID="${owner_client_id}" \
      --env KELDRA_CLIENT_SECRET="${owner_client_secret}" \
      "${image}" keldra --endpoint http://127.0.0.1:50051 get smoke objects cloned
  )"
  if [[ "${clone_value}" != 'keldra-0.1-smoke' ]]; then
    echo "clone did not survive canonical target deletion" >&2
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
