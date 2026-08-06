#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
legacy_image="ghcr.io/worka-ai/anvil:0.5.3"
candidate_image="${ANVIL_IMAGE:?ANVIL_IMAGE must name the already-built candidate image}"
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

# The baseline is deliberately obtained from the public released tag. The
# candidate remains local and immutable for the duration of this qualification.
docker pull --platform "${platform}" "${legacy_image}" >/dev/null
legacy_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${legacy_image}")"
candidate_id="$("${repo_root}/scripts/resolve-docker-image-id.sh" "${candidate_image}")"
if [[ "${legacy_id}" == "${candidate_id}" ]]; then
  echo "candidate image resolves to the released 0.5.3 image" >&2
  exit 2
fi

qualification_dir="$(mktemp -d /tmp/anvil-v053-upgrade-qualification.XXXXXX)"
qualification_suffix="${qualification_dir##*.}"
container_name="anvil-v053-upgrade-${qualification_suffix}"
data_dir="${qualification_dir}/data"
artifacts_dir="${qualification_dir}/artifacts"
signing_key="${qualification_dir}/token-signing-key"
container_started=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM

  if ((container_started == 1)) && ((status != 0)); then
    echo "[anvil-v053-upgrade] FAILED; container logs follow" >&2
    docker logs "${container_name}" >&2 || true
  fi
  if [[ "${keep}" == "1" ]]; then
    echo "[anvil-v053-upgrade] retained container ${container_name}" >&2
    echo "[anvil-v053-upgrade] retained files ${qualification_dir}" >&2
    exit "${status}"
  fi

  if ((container_started == 1)); then
    docker rm --force "${container_name}" >/dev/null 2>&1 || true
  fi
  if [[ "${qualification_dir}" == /tmp/anvil-v053-upgrade-qualification.* ]]; then
    docker run --rm --user 0 \
      --volume "${qualification_dir}:/qualification" \
      "${legacy_id}" rm -rf \
        /qualification/data \
        /qualification/artifacts \
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

mkdir "${data_dir}" "${artifacts_dir}"
chmod 0755 "${qualification_dir}"
chmod 0777 "${artifacts_dir}"
head -c 64 /dev/urandom >"${signing_key}"
chmod 0600 "${signing_key}"
printf 'legacy ordinary object\n' >"${artifacts_dir}/legacy-small.txt"
dd if=/dev/urandom of="${artifacts_dir}/candidate-large.bin" \
  bs=1M count=2 status=none
chmod 0444 \
  "${artifacts_dir}/legacy-small.txt" \
  "${artifacts_dir}/candidate-large.bin"
docker run --rm --user 0 \
  --volume "${qualification_dir}:/qualification" \
  "${legacy_id}" chown -R 10001:10001 \
    /qualification/data \
    /qualification/token-signing-key

start_server() {
  local image_id="$1"
  docker run --detach \
    --name "${container_name}" \
    --platform "${platform}" \
    --env RUST_LOG="${RUST_LOG:-info}" \
    --env ANVIL_LISTEN=0.0.0.0:50051 \
    --env ANVIL_PEER_LISTEN=127.0.0.1:50052 \
    --env ANVIL_DATA_DIR=/var/lib/anvil \
    --env ANVIL_NODE_ID=1 \
    --env ANVIL_TOKEN_SIGNING_KEY_FILE=/run/secrets/anvil-token-signing-key \
    --env ANVIL_RUN_SYSTEM_BOOTSTRAP=true \
    --volume "${data_dir}:/var/lib/anvil" \
    --volume "${artifacts_dir}:/qualification/artifacts" \
    --volume "${signing_key}:/run/secrets/anvil-token-signing-key:ro" \
    "${image_id}" >/dev/null
  container_started=1
}

stop_server() {
  docker stop --time 30 "${container_name}" >/dev/null
  docker rm "${container_name}" >/dev/null
  container_started=0
}

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
      echo "released 0.5.3 server exited during bootstrap" >&2
      return 1
    fi
    sleep 1
  done
  echo "released 0.5.3 bootstrap did not finish within 90 seconds" >&2
  return 1
}

provision_owner() {
  local attempt
  local output=""
  for attempt in $(seq 1 90); do
    if output="$(ANVIL_NEW_CLIENT_SECRET="${owner_secret}" docker exec \
      --env ANVIL_NEW_CLIENT_SECRET \
      "${container_name}" \
      anvil --endpoint http://127.0.0.1:50051 \
        --credentials-file /var/lib/anvil/system-bootstrap-credential.json \
        provision-tenant "${tenant}" "${owner_app}" "${owner_client}" 2>&1)"
    then
      if grep -Fq "tenant=${tenant}" <<<"${output}"; then
        return 0
      fi
      echo "released 0.5.3 provisioning returned unexpected output: ${output}" >&2
      return 1
    fi
    sleep 1
  done
  echo "released 0.5.3 did not accept tenant provisioning" >&2
  echo "last administration error: ${output}" >&2
  return 1
}

tenant=qupgrade
owner_app=qupgrade-owner
owner_client=qupgrade-client
owner_secret=qualification-upgrade-owner-secret-000000000000000000
bucket=objects

run_owner_cli() {
  docker exec \
    --env "ANVIL_CLIENT_ID=${owner_client}" \
    --env "ANVIL_CLIENT_SECRET=${owner_secret}" \
    "${container_name}" \
    anvil --endpoint http://127.0.0.1:50051 "$@"
}

wait_for_owner_access() {
  local attempt
  local output=""
  for attempt in $(seq 1 90); do
    if output="$(run_owner_cli head "${tenant}" "${bucket}" legacy/small.txt 2>&1)"
    then
      return 0
    fi
    if ! docker inspect --format '{{.State.Running}}' "${container_name}" \
      2>/dev/null | grep -Fxq true
    then
      echo "candidate server exited during legacy journal recovery" >&2
      return 1
    fi
    sleep 1
  done
  echo "candidate did not recover legacy owner access within 90 seconds" >&2
  echo "last object error: ${output}" >&2
  return 1
}

start_server "${legacy_id}"
wait_for_bootstrap

provision_owner
run_owner_cli create-bucket "${bucket}" | grep -Fq "bucket=${bucket}"
run_owner_cli put "${tenant}" "${bucket}" legacy/small.txt \
  /qualification/artifacts/legacy-small.txt \
  --command-id legacy-small --durability local --if-absent >/dev/null
run_owner_cli get "${tenant}" "${bucket}" legacy/small.txt \
  --output /tmp/legacy-small-read.txt
docker cp "${container_name}:/tmp/legacy-small-read.txt" \
  "${artifacts_dir}/legacy-small-before-upgrade.txt"
cmp "${artifacts_dir}/legacy-small.txt" \
  "${artifacts_dir}/legacy-small-before-upgrade.txt"
echo "[anvil-v053-upgrade] released 0.5.3 legacy object created"

stop_server
start_server "${candidate_id}"
wait_for_owner_access
run_owner_cli get "${tenant}" "${bucket}" legacy/small.txt \
  --output /tmp/legacy-small-after-upgrade.txt
docker cp "${container_name}:/tmp/legacy-small-after-upgrade.txt" \
  "${artifacts_dir}/legacy-small-after-upgrade.txt"
cmp "${artifacts_dir}/legacy-small.txt" \
  "${artifacts_dir}/legacy-small-after-upgrade.txt"
echo "[anvil-v053-upgrade] candidate recovered the legacy proofless journal"

run_owner_cli put "${tenant}" "${bucket}" candidate/large.bin \
  /qualification/artifacts/candidate-large.bin \
  --command-id candidate-large --durability local --if-absent >/dev/null
run_owner_cli get "${tenant}" "${bucket}" candidate/large.bin \
  --output /tmp/candidate-large-before-restart.bin
docker cp "${container_name}:/tmp/candidate-large-before-restart.bin" \
  "${artifacts_dir}/candidate-large-before-restart.bin"
cmp "${artifacts_dir}/candidate-large.bin" \
  "${artifacts_dir}/candidate-large-before-restart.bin"

docker restart "${container_name}" >/dev/null
wait_for_owner_access
run_owner_cli get "${tenant}" "${bucket}" candidate/large.bin \
  --output /tmp/candidate-large-after-restart.bin
docker cp "${container_name}:/tmp/candidate-large-after-restart.bin" \
  "${artifacts_dir}/candidate-large-after-restart.bin"
cmp "${artifacts_dir}/candidate-large.bin" \
  "${artifacts_dir}/candidate-large-after-restart.bin"

echo "[anvil-v053-upgrade] PASS legacy=${legacy_id} candidate=${candidate_id} platform=${platform}"
