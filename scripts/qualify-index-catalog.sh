#!/usr/bin/env bash
set -Eeuo pipefail

# Destructive experiment state is deliberately confined to this one removable
# directory on the qualification host.
experiment_root="${HOME}/keldra_experiments"
kit_root="${experiment_root}/kit"
results_root="${experiment_root}/results"
work_root="${experiment_root}/work"
definitions="${KELDRA_CATALOG_DEFINITIONS:-250000}"
concurrency="${KELDRA_CATALOG_CONCURRENCY:-64}"
port="${KELDRA_CATALOG_PORT:-50051}"
indexing_cores="${KELDRA_CATALOG_INDEXING_CORES:-4}"
pipeline_memory_bytes="${KELDRA_CATALOG_PIPELINE_MEMORY_BYTES:-1073741824}"
# Per-request INFO emits several lines per definition and would both distort a
# rotational-disk catalogue run and create multi-gigabyte evidence logs.
server_rust_log="${KELDRA_CATALOG_RUST_LOG:-warn}"
server_source_commit="$(tr -d '\r\n' <"${kit_root}/SOURCE_COMMIT")"
catalog_harness_commit="$(tr -d '\r\n' <"${kit_root}/CATALOG_HARNESS_COMMIT")"

case "${experiment_root}" in
  "${HOME}/keldra_experiments") ;;
  *) echo "experiment root escaped HOME/keldra_experiments" >&2; exit 2 ;;
esac
for value in "${definitions}" "${concurrency}" "${port}" \
  "${indexing_cores}" "${pipeline_memory_bytes}"
do
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || {
    echo "catalog counts and port must be positive decimal integers" >&2
    exit 2
  }
done
((definitions <= 1000000)) || { echo "definition bound is 1000000" >&2; exit 2; }
((concurrency <= 1024)) || { echo "concurrency bound is 1024" >&2; exit 2; }
for binary in keldra-server keldra index-catalog-qualification; do
  [[ -x "${kit_root}/bin/${binary}" ]] || { echo "missing ${binary}" >&2; exit 2; }
done
for command in flock jq lscpu lsblk ps sha256sum tar vmstat; do
  command -v "${command}" >/dev/null || { echo "missing ${command}" >&2; exit 2; }
done
(cd "${kit_root}" && sha256sum --check SHA256SUMS)

mkdir -p "${results_root}" "${work_root}"
chmod 0700 "${experiment_root}" "${results_root}" "${work_root}"
exec 9>"${experiment_root}/run.lock"
flock -n 9 || { echo "another Keldra experiment is running" >&2; exit 2; }
ulimit -n 65536

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(hostname -s)-catalog-${server_source_commit:0:12}"
run_dir="${results_root}/${run_id}"
cell_work="${work_root}/${run_id}"
mkdir -p "${run_dir}" "${cell_work}/state" "${cell_work}/metadata" \
  "${cell_work}/wal" "${cell_work}/payload" "${cell_work}/scratch" \
  "${cell_work}/cache" "${cell_work}/tmp"
chmod -R 0700 "${run_dir}" "${cell_work}"
ln -sfn "${run_id}" "${results_root}/latest"
dd if=/dev/urandom of="${cell_work}/token-signing-key" bs=64 count=1 status=none
chmod 0600 "${cell_work}/token-signing-key"
tenant="catalog-${server_source_commit:0:8}-${run_id:0:8}${run_id:9:6}"
bucket="catalog"
client_id="catalog-client"
client_secret="$(dd if=/dev/urandom bs=32 count=1 status=none | base64 | tr -d '\n')"
credential_file="${cell_work}/state/system-bootstrap-credential.json"
server_pid=""
sampler_pid=""
vmstat_pid=""
catalog_pid=""

sample_process() {
  local pid="$1" output="$2"
  printf 'timestamp_utc\tepoch_seconds\tcpu_percent\trss_kib\tthreads\tread_bytes\twrite_bytes\tmem_available_kib\n' >"${output}"
  while kill -0 "${pid}" 2>/dev/null; do
    local now epoch cpu rss threads read_bytes write_bytes available
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; epoch="$(date +%s)"
    read -r cpu rss threads < <(ps -p "${pid}" -o %cpu=,rss=,nlwp=)
    read_bytes="$(awk '/^read_bytes:/ {print $2}' "/proc/${pid}/io" 2>/dev/null || echo 0)"
    write_bytes="$(awk '/^write_bytes:/ {print $2}' "/proc/${pid}/io" 2>/dev/null || echo 0)"
    available="$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${now}" "${epoch}" "${cpu}" "${rss}" "${threads}" \
      "${read_bytes}" "${write_bytes}" "${available}" >>"${output}"
    sleep 1
  done
}

stop_server() {
  if [[ -n "${catalog_pid}" ]]; then
    kill -TERM "${catalog_pid}" 2>/dev/null || true
    wait "${catalog_pid}" 2>/dev/null || true
  fi
  if [[ -n "${sampler_pid}" ]]; then kill "${sampler_pid}" 2>/dev/null || true; wait "${sampler_pid}" 2>/dev/null || true; fi
  if [[ -n "${vmstat_pid}" ]]; then kill "${vmstat_pid}" 2>/dev/null || true; wait "${vmstat_pid}" 2>/dev/null || true; fi
  if [[ -n "${server_pid}" ]]; then
    kill -TERM "${server_pid}" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "${server_pid}" 2>/dev/null || break; sleep 1; done
    kill -KILL "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  server_pid=""; sampler_pid=""; vmstat_pid=""; catalog_pid=""
}
on_exit() {
  local status=$?
  trap - EXIT INT TERM
  stop_server
  printf '%s\n' "${status}" >"${run_dir}/runner-exit-status"
  exit "${status}"
}
trap on_exit EXIT INT TERM

start_server() {
  local suffix="$1" bootstrap="$2"
  TMPDIR="${cell_work}/tmp" \
  RUST_LOG="${server_rust_log}" \
  KELDRA_LISTEN="127.0.0.1:${port}" \
  KELDRA_PEER_LISTEN="127.0.0.1:$((port + 1))" \
  KELDRA_DATA_DIR="${cell_work}" \
  KELDRA_STATE_DIR="${cell_work}/state" \
  KELDRA_METADATA_DIR="${cell_work}/metadata" \
  KELDRA_METADATA_WAL_DIR="${cell_work}/wal" \
  KELDRA_PAYLOAD_DIR="${cell_work}/payload" \
  KELDRA_SCRATCH_DIR="${cell_work}/scratch" \
  KELDRA_CACHE_DIR="${cell_work}/cache" \
  KELDRA_NODE_ID=1 \
  KELDRA_TOKEN_SIGNING_KEY_FILE="${cell_work}/token-signing-key" \
  KELDRA_RUN_SYSTEM_BOOTSTRAP="${bootstrap}" \
  KELDRA_SYSTEM_BOOTSTRAP_CREDENTIAL_OUTPUT="${credential_file}" \
  KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE=1000000 \
  KELDRA_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST=100000 \
  KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE=1000000 \
  KELDRA_RATE_LIMIT_CREDENTIAL_CLIENT_BURST=100000 \
  KELDRA_INDEX_DISK_CACHE_BYTES=1073741824 \
  KELDRA_INDEX_PIPELINE_MEMORY_BYTES="${pipeline_memory_bytes}" \
  KELDRA_INDEXING_CORES="${indexing_cores}" \
  KELDRA_SOURCE_JOURNAL_MAX_ENTRIES=1000000 \
    "${kit_root}/bin/keldra-server" >"${run_dir}/server-${suffix}.log" 2>&1 &
  server_pid=$!
  sample_process "${server_pid}" "${run_dir}/server-${suffix}-resources.tsv" & sampler_pid=$!
  vmstat -w 1 >"${run_dir}/host-${suffix}-vmstat.log" & vmstat_pid=$!
  for _ in $(seq 1 180); do
    kill -0 "${server_pid}" 2>/dev/null || break
    if (exec 8<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then exec 8>&-; return 0; fi
    sleep 1
  done
  echo "server ${suffix} did not become ready" >&2
  return 1
}

run_catalog_phase() {
  local phase="$1" output="$2" stdout="$3" stderr="$4"
  KELDRA_CLIENT_ID="${client_id}" KELDRA_CLIENT_SECRET="${client_secret}" \
    "${kit_root}/bin/index-catalog-qualification" \
    --endpoint "http://127.0.0.1:${port}" --bucket "${bucket}" \
    --definitions "${definitions}" --concurrency "${concurrency}" --phase "${phase}" \
    --output "${output}" >"${stdout}" 2>"${stderr}" &
  catalog_pid=$!
  local status=0
  wait "${catalog_pid}" || status=$?
  catalog_pid=""
  return "${status}"
}

{
  echo "server_source_commit=${server_source_commit}"
  echo "catalog_harness_commit=${catalog_harness_commit}"
  echo "definitions=${definitions}"
  echo "concurrency=${concurrency}"
  echo "indexing_cores=${indexing_cores}"
  echo "pipeline_memory_bytes=${pipeline_memory_bytes}"
  uname -srvmo; lscpu; free -h; df -hT "${experiment_root}"
  lsblk -o NAME,KNAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS,ROTA,MODEL
} >"${run_dir}/host-info.txt"

start_server initial true
for _ in $(seq 1 180); do [[ -s "${credential_file}" ]] && break; sleep 1; done
[[ -s "${credential_file}" ]] || { echo "bootstrap credentials were not produced" >&2; exit 1; }
KELDRA_NEW_CLIENT_SECRET="${client_secret}" "${kit_root}/bin/keldra" \
  --endpoint "http://127.0.0.1:${port}" --credentials-file "${credential_file}" \
  provision-tenant "${tenant}" catalog-owner "${client_id}" \
  >"${run_dir}/provision.stdout.log" 2>"${run_dir}/provision.stderr.log"
rm -f "${credential_file}"
KELDRA_CLIENT_ID="${client_id}" KELDRA_CLIENT_SECRET="${client_secret}" \
  "${kit_root}/bin/keldra" --endpoint "http://127.0.0.1:${port}" \
  create-bucket "${bucket}" >"${run_dir}/bucket.stdout.log" 2>"${run_dir}/bucket.stderr.log"

run_catalog_phase create "${run_dir}/create-report.json" \
  "${run_dir}/create.stdout.log" "${run_dir}/create.stderr.log"
ps -o pid,%cpu,rss,nlwp,etime -p "${server_pid}" >"${run_dir}/before-restart-process.txt"
stop_server

start_server restarted false
run_catalog_phase verify "${run_dir}/verify-report.json" \
  "${run_dir}/verify.stdout.log" "${run_dir}/verify.stderr.log"
ps -o pid,%cpu,rss,nlwp,etime -p "${server_pid}" >"${run_dir}/after-restart-process.txt"
stop_server

jq -n --slurpfile create "${run_dir}/create-report.json" \
  --slurpfile verify "${run_dir}/verify-report.json" \
  --arg server "${server_source_commit}" --arg harness "${catalog_harness_commit}" \
  '{schema:"keldra.catalog-scale-run.v1",server_source_commit:$server,catalog_harness_commit:$harness,create:$create[0],verify:$verify[0]}' \
  >"${run_dir}/report.json"
printf '0\n' >"${run_dir}/overall-status"
tar -C "${results_root}" -czf "${results_root}/${run_id}.results.tar.gz" "${run_id}"
sha256sum "${results_root}/${run_id}.results.tar.gz" >"${results_root}/${run_id}.results.tar.gz.sha256"
echo "results=${run_dir}"
echo "archive=${results_root}/${run_id}.results.tar.gz"
