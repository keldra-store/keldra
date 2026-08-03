#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${repo_root}/tests/cluster/docker-compose.yml"
start_node="${repo_root}/tests/cluster/start-node.sh"
requested_image="${ANVIL_IMAGE:-anvil:0.5.1}"

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
docker compose version >/dev/null

image_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${requested_image}")"
export ANVIL_IMAGE="${image_id}"
export ANVIL_QUALIFICATION_PROJECT="${ANVIL_QUALIFICATION_PROJECT:-anvil-v051-${$}}"
export ANVIL_QUALIFICATION_DIR="$(mktemp -d /tmp/anvil-v051-qualification.XXXXXX)"
export ANVIL_QUALIFICATION_START_NODE="${start_node}"
keep="${ANVIL_QUALIFICATION_KEEP:-0}"

compose() {
  docker compose \
    --project-name "${ANVIL_QUALIFICATION_PROJECT}" \
    --file "${compose_file}" \
    "$@"
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
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
    if [[ "${ANVIL_QUALIFICATION_DIR}" == /tmp/anvil-v051-qualification.* ]]; then
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
        "${tenant}" owner "${client_id}" 2>&1)"
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

  local copied="${ANVIL_QUALIFICATION_DIR}/node-${node_id}/anvil-node-${node_id}.join.json"
  compose cp "${source_service}:${bundle_path}" "${copied}"
  chmod 0600 "${copied}"
  docker run --rm --user 0 \
    --volume "${copied}:/join-bundle" \
    "${image_id}" chown 10001:10001 /join-bundle

  compose up --detach "${service}"
  wait_for_node "${service}"
  if [[ -e "${copied}" ]]; then
    echo "${service} became ready without consuming and deleting its join bundle" >&2
    return 1
  fi
}

expect_failure() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "${label} unexpectedly succeeded" >&2
    return 1
  fi
}

wait_for_bootstrap
provision_tenant qprobe qprobe-client \
  qualification-probe-secret-000000000000000000000000
create_bucket anvil-1 qprobe-client \
  qualification-probe-secret-000000000000000000000000 objects
prepare_and_start_node 2
prepare_and_start_node 3

echo "[anvil-qualification] three-node cluster is ACTIVE"

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
run_cli anvil-1 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-one.txt \
  --command-id qversion-one --durability replicated >/dev/null
run_cli anvil-3 qversion-client "${version_secret}" \
  put qversion objects retained/value.txt /qualification/artifacts/version-two.txt \
  --command-id qversion-two --durability replicated >/dev/null
old_delete="$(run_cli anvil-2 qversion-client "${version_secret}" \
  delete-version qversion objects retained/value.txt 1 --durability replicated)"
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
  delete-version qversion objects retained/value.txt 2 --durability replicated)"
if [[ "${current_delete}" != 'deleted=true replacement_tombstone_version=3' ]]; then
  echo "distributed current DeleteVersion returned: ${current_delete}" >&2
  exit 1
fi
for version_node in anvil-1 anvil-2 anvil-3; do
  version_head="$(run_cli "${version_node}" qversion-client "${version_secret}" \
    head qversion objects retained/value.txt)"
  if [[ "${version_head}" != 'deleted version=3' ]]; then
    echo "${version_node} did not observe the fresh version-3 tombstone" >&2
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
  watch qlist objects --prefix prefix/ --retained --events 4 \
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
create_bucket anvil-1 qatomic-client "${atomic_secret}" objects
cat >"${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-program.json" <<'JSON'
{"schema_version":1,"documents":[{"name":"primary","path":{"tenant":"{tenant}","bucket":"objects","path":"atomic/primary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true},{"name":"secondary","path":{"tenant":"{tenant}","bucket":"objects","path":"atomic/secondary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true}],"assertions":[],"operations":[{"kind":"set_value","target":{"document":{"slot":"primary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"primary-committed"}},{"kind":"set_value","target":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"secondary-committed"}}],"returns":[{"name":"primary_status","value":{"value":{"document":{"slot":"primary","index":0},"pointer":"/status"},"view":"current"}},{"name":"secondary_status","value":{"value":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"view":"current"}}],"caps":{"max_paths":2,"max_writes":2,"max_operations":4,"max_input_bytes":4096,"max_document_bytes":4096}}
JSON
cat >"${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-input.json" <<'JSON'
{"bindings":{"primary":[{"path":{"tenant":"qatomic","bucket":"objects","path":"atomic/primary.json"},"template_values":{},"expected_head":{"kind":"absent"},"initial_json":{"status":"uncommitted"}}],"secondary":[{"path":{"tenant":"qatomic","bucket":"objects","path":"atomic/secondary.json"},"template_values":{},"expected_head":{"kind":"absent"},"initial_json":{"status":"uncommitted"}}]}}
JSON
printf '{"status":"primary-committed"}' \
  >"${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-primary.expected.json"
printf '{"status":"secondary-committed"}' \
  >"${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-secondary.expected.json"
chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-"*.json
run_cli anvil-1 qatomic-client "${atomic_secret}" \
  put qatomic objects _anvil/programs/qualification@1 \
  /qualification/artifacts/atomic-program.json \
  --content-type application/json --command-id qatomic-install --immutable \
  --durability replicated >/dev/null
program_head="$(run_cli anvil-2 qatomic-client "${atomic_secret}" \
  head qatomic objects _anvil/programs/qualification@1)"
program_hash="$(sed -n 's/^present version=[0-9][0-9]* bytes=[0-9][0-9]* blake3=\([0-9a-f]\{64\}\)$/\1/p' \
  <<<"${program_head}")"
if [[ -z "${program_hash}" ]]; then
  echo "program Head did not return its BLAKE3 identity: ${program_head}" >&2
  exit 1
fi
run_cli anvil-2 qatomic-client "${atomic_secret}" \
  set-policy qatomic objects --program-only atomic/ >/dev/null
program_output="$(run_cli anvil-3 qatomic-client "${atomic_secret}" \
  invoke-program qatomic objects _anvil/programs/qualification@1 \
  qatomic-invocation --program-hash "${program_hash}" \
  --durability replicated /qualification/artifacts/atomic-input.json)"
if [[ "${program_output}" != \
  '{"primary_status":"primary-committed","secondary_status":"secondary-committed"}' ]]; then
  echo "atomic program returned unexpected output: ${program_output}" >&2
  exit 1
fi
for atomic_node in anvil-1 anvil-2 anvil-3; do
  for atomic_output in primary secondary; do
    actual="${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-${atomic_node}-${atomic_output}.json"
    rm -f "${actual}"
    run_cli "${atomic_node}" qatomic-client "${atomic_secret}" \
      get qatomic objects "atomic/${atomic_output}.json" \
      --output "/qualification/artifacts/atomic-${atomic_node}-${atomic_output}.json"
    cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/atomic-${atomic_output}.expected.json" \
      "${actual}"
  done
done
echo "[anvil-qualification] distributed atomic program test passed"

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
  compose restart "${node}"
  wait_for_node "${node}"
  rm -f "${ANVIL_QUALIFICATION_DIR}/artifacts/restart-read.txt"
  run_cli "${node}" qrestart-client "${restart_secret}" \
    get qrestart objects restart/value.txt \
    --output /qualification/artifacts/restart-read.txt
  cmp "${ANVIL_QUALIFICATION_DIR}/artifacts/restart.txt" \
    "${ANVIL_QUALIFICATION_DIR}/artifacts/restart-read.txt"
done
echo "[anvil-qualification] rolling restart test passed"

echo "[anvil-qualification] PASS image=${image_id} platform=${ANVIL_DOCKER_PLATFORM}"
