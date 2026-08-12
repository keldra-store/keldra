#!/usr/bin/env bash

# Three-node qualification phase transitions. The caller provides compose,
# readiness, image, startup-evidence, and log helpers in its shell.

preserve_journal_pressure_evidence() {
  local destination_prefix="$1"
  local node
  for node in anvil-1 anvil-2 anvil-3; do
    preserve_qualification_log \
      "${ANVIL_QUALIFICATION_DIR}/artifacts/index-gap-recovery-${node}.log" \
      "${destination_prefix}-${node}.log"
  done
  echo "[anvil-qualification] preserved journal-pressure evidence ${destination_prefix}-anvil-{1,2,3}.log"
}

capture_three_node_resource_evidence() {
  local node="$1"
  local start_cursor="$2"
  local log="${ANVIL_QUALIFICATION_DIR}/artifacts/index-resource-${node}.log"
  local capture_cursor="${start_cursor}"
  local next_cursor
  local attempt
  : >"${log}"
  for attempt in $(seq 1 12); do
    next_cursor="$(qualification_log_cursor)"
    service_logs_since "${node}" "${capture_cursor}" "${next_cursor}" \
      >>"${log}"
    capture_cursor="$(qualification_log_cursor_after "${next_cursor}")"
    if grep -Fq 'sampled process resources' "${log}" \
      && grep -Fq 'sampled cgroup memory resources' "${log}" \
      && grep -Fq 'sampled RocksDB resources' "${log}" \
      && grep -Fq 'sampled source-journal safety and capacity' "${log}" \
      && grep -Fq 'sampled mutation receipt capacity' "${log}"
    then
      break
    fi
    sleep 1
  done
  preserve_qualification_log "${log}" "${index_resource_telemetry_prefix}-${node}.log"
  assert_zero_cgroup_oom_samples "${log}" "${node} production qualification"
  assert_capacity_samples "${log}" "${node} production qualification" \
    "${ANVIL_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES}"
}

wait_for_source_journal_entry_bound() {
  local node="$1"
  local expected="$2"
  local attempt
  local line
  for attempt in $(seq 1 30); do
    line="$(
      service_logs "${node}" \
        | grep -F 'sampled source-journal safety and capacity' \
        | tail -n 1 || true
    )"
    if [[ -n "${line}" ]] \
      && [[ "$(log_unsigned_field gauge.anvil_source_journal_max_entries "${line}" || true)" == "${expected}" ]]
    then
      return 0
    fi
    sleep 1
  done
  echo "${node} did not report source-journal max entries ${expected}" >&2
  return 1
}

start_release_source_journal_phase() {
  local bound="$1"
  local node
  export ANVIL_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES="${bound}"
  for node in anvil-1 anvil-2 anvil-3; do
    compose up --detach --no-deps --force-recreate "${node}"
    wait_for_node "${node}"
    require_service_image "${node}" "${image_id}" qualification
    assert_sparse_index_startup "${node}" 1
    wait_for_source_journal_entry_bound "${node}" "${bound}"
  done
  public_endpoints=()
  for node in anvil-1 anvil-2 anvil-3; do
    public_endpoints+=("$(public_endpoint_for "${node}")")
  done
  echo "[anvil-qualification] journal-pressure phase ended; release phases use source-journal max entries ${bound}"
}

prepare_joining_node() {
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

  local copied="${ANVIL_QUALIFICATION_DIR}/artifacts/anvil-node-${node_id}.join.json"
  compose cp "${source_service}:${bundle_path}" "${copied}"
  chmod 0600 "${copied}"
  docker run --rm --user 0 \
    --volume "${copied}:/join-bundle" \
    "${image_id}" chown 10001:10001 /join-bundle
}

start_prepared_node() {
  local node_id="$1"
  local service="anvil-${node_id}"
  compose up --detach "${service}"
  wait_for_node "${service}"
  assert_sparse_index_startup "${service}" 1
  if [[ -e "${ANVIL_QUALIFICATION_DIR}/artifacts/anvil-node-${node_id}.join.json" ]]; then
    echo "${service} became ready without consuming and deleting its join bundle" >&2
    return 1
  fi
}

prepare_and_start_node() {
  prepare_joining_node "$1"
  start_prepared_node "$1"
}

start_prepared_node_during_indexed_cutover() {
  local node_id="$1"
  local service="anvil-${node_id}"
  if [[ -z "${paused_container}" ]]; then
    echo "indexed membership cutover has no paused pre-cutover builder" >&2
    return 1
  fi
  compose up --detach "${service}"
  # Let the joining node reach its existing peers while the old-fence index
  # builder remains unable to publish. Resuming the builder and quorum voter
  # then races real pending index work against the ACTIVE membership cutover.
  sleep 1
  docker unpause "${paused_container}" >/dev/null
  paused_container=""
  wait_for_node "${service}"
  assert_sparse_index_startup "${service}" 1
  if [[ -e "${ANVIL_QUALIFICATION_DIR}/artifacts/anvil-node-${node_id}.join.json" ]]; then
    echo "${service} became ready without consuming and deleting its join bundle" >&2
    return 1
  fi
}

# State captured immediately before the two-to-three membership cutover. Node 2
# is deliberately used because node 1 is the stable lowest-ID membership
# reconciliation coordinator.
membership_cutover_source_tail=
membership_cutover_source_fence_term=
membership_cutover_source_fence_index=
membership_cutover_source_log_start=
membership_cutover_index_id=
membership_cutover_index_bucket=
membership_cutover_index_path=
membership_cutover_index_version=
membership_cutover_index_generation_before=
membership_cutover_index_generation_after=
membership_cutover_index_attempts=
membership_cutover_index_burst=
membership_cutover_index_token=
membership_cutover_index_source_node_id=
membership_cutover_index_source_tail=

latest_source_journal_sample() {
  service_logs "$1" \
    | grep -F 'sampled source-journal safety and capacity' \
    | tail -n 1 || true
}

latest_completed_membership_fence() {
  local node="$1"
  local node_id="$2"
  service_logs "${node}" \
    | awk -v source="source.node_id=${node_id}" '
        index($0, "derived-consumer membership fence finished") &&
        index($0, source) &&
        index($0, "fence.outcome=\"completed\"") { line = $0 }
        END { if (line != "") print line }
      '
}

source_journal_sample_is_clear_at_bound() {
  local line="$1"
  local bound="$2"
  local accounting
  local index
  local maximum
  local retained
  local settled
  local tail
  [[ -n "${line}" ]] || return 1
  tail="$(log_unsigned_field gauge.anvil_source_journal_tail "${line}" || true)"
  settled="$(log_unsigned_field gauge.anvil_source_journal_settled_through "${line}" || true)"
  index="$(log_unsigned_field gauge.anvil_source_journal_index_safe_through "${line}" || true)"
  accounting="$(log_unsigned_field gauge.anvil_source_journal_accounting_safe_through "${line}" || true)"
  retained="$(log_unsigned_field gauge.anvil_source_journal_retained_entries "${line}" || true)"
  maximum="$(log_unsigned_field gauge.anvil_source_journal_max_entries "${line}" || true)"
  [[ -n "${tail}" \
    && "${settled}" == "${tail}" \
    && "${index}" == "${tail}" \
    && "${accounting}" == "${tail}" \
    && "${retained}" == "${bound}" \
    && "${maximum}" == "${bound}" ]]
}

run_cutover_writes() {
  local node="$1"
  local client_id="$2"
  local client_secret="$3"
  local tenant="$4"
  local bucket="$5"
  local phase="$6"
  local first="$7"
  local count="$8"
  local concurrency=8
  local last=$((first + count))
  local pid
  local position
  local -a pids=()
  local failed=0
  for ((position = first; position < last; position++)); do
    run_cli "${node}" "${client_id}" "${client_secret}" \
      put "${tenant}" "${bucket}" \
        "membership-cutover/${phase}-${position}.bin" \
        /qualification/artifacts/membership-cutover-byte.bin \
        --command-id "membership-cutover-${phase}-${position}" \
        --durability local --if-absent >/dev/null &
    pids+=("$!")
    if ((${#pids[@]} == concurrency)); then
      for pid in "${pids[@]}"; do
        wait "${pid}" || failed=1
      done
      pids=()
    fi
  done
  for pid in "${pids[@]}"; do
    wait "${pid}" || failed=1
  done
  if ((failed != 0)); then
    echo "ordinary ${phase} writes failed during membership-cutover qualification" >&2
    return 1
  fi
}

prepare_no_event_membership_cutover_qualification() {
  local node="$1"
  local node_id="$2"
  local client_id="$3"
  local client_secret="$4"
  local tenant="$5"
  local bucket="$6"
  local bound="$7"
  local batch=$((bound * 2))
  local attempt
  local fence
  local line
  local round
  if [[ "${node_id}" == "1" ]]; then
    echo "no-event membership cutover source must not be the reconciliation coordinator" >&2
    return 1
  fi
  printf 'x' >"${ANVIL_QUALIFICATION_DIR}/artifacts/membership-cutover-byte.bin"
  chmod 0444 "${ANVIL_QUALIFICATION_DIR}/artifacts/membership-cutover-byte.bin"

  for round in 0 1; do
    run_cutover_writes \
      "${node}" "${client_id}" "${client_secret}" "${tenant}" "${bucket}" \
      pre "$((round * batch))" "${batch}"
    for attempt in $(seq 1 30); do
      line="$(latest_source_journal_sample "${node}")"
      source_journal_sample_is_clear_at_bound "${line}" "${bound}" && break
      sleep 1
    done
    source_journal_sample_is_clear_at_bound "${line}" "${bound}" && break
  done
  if ! source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
    echo "${node} did not reach a clear source-journal entry bound of ${bound}" >&2
    printf '%s\n' "${line}" >&2
    return 1
  fi
  membership_cutover_source_tail="$(
    log_unsigned_field gauge.anvil_source_journal_tail "${line}"
  )"
  fence="$(latest_completed_membership_fence "${node}" "${node_id}")"
  membership_cutover_source_fence_term="$(
    log_unsigned_field membership.term "${fence}" || true
  )"
  membership_cutover_source_fence_index="$(
    log_unsigned_field membership.index "${fence}" || true
  )"
  if [[ -z "${membership_cutover_source_fence_term}" \
    || -z "${membership_cutover_source_fence_index}" ]]
  then
    echo "${node} emitted no parseable completed pre-cutover membership fence" >&2
    return 1
  fi
  membership_cutover_source_log_start="$(log_cursor)"
  echo "[anvil-qualification] non-coordinator source ${node_id} reached journal bound ${bound} at tail ${membership_cutover_source_tail} before the no-event 2->3 cutover"
}

new_cutover_fence_line() {
  local log="$1"
  local node_id="$2"
  local previous_term="$3"
  local previous_index="$4"
  local expected_active_nodes="$5"
  local active_nodes
  local line
  local term
  local index
  while IFS= read -r line; do
    term="$(log_unsigned_field membership.term "${line}" || true)"
    index="$(log_unsigned_field membership.index "${line}" || true)"
    active_nodes="$(log_unsigned_field membership.active_nodes "${line}" || true)"
    if [[ -n "${term}" && -n "${index}" ]] \
      && [[ "${active_nodes}" == "${expected_active_nodes}" ]] \
      && ((term > previous_term || (term == previous_term && index > previous_index)))
    then
      printf '%s\n' "${line}"
      return 0
    fi
  done < <(
    awk -v source="source.node_id=${node_id}" '
      index($0, "derived-consumer membership fence finished") &&
      index($0, source) &&
      index($0, "fence.outcome=\"completed\"")
    ' "${log}"
  )
  return 1
}

grpcurl_public() {
  local endpoint="${1#http://}"
  shift
  command -v grpcurl >/dev/null 2>&1 || {
    echo "grpcurl is required for the public indexed-cutover proof" >&2
    return 2
  }
  grpcurl -plaintext -max-time 35 \
    -import-path "${repo_root}/crates/anvil-api/proto" \
    -import-path /usr/include -proto anvil.proto "$@" "${endpoint}"
}

cutover_access_token() {
  local endpoint="$1"
  local client_id="$2"
  local client_secret="$3"
  local response
  response="$(
    jq -nc --arg client_id "${client_id}" --arg client_secret "${client_secret}" \
      '{clientId:$client_id,clientSecret:$client_secret}' \
      | grpcurl_public "${endpoint}" -d @ \
          anvil.v1.CredentialService/ExchangeClientCredentials
  )"
  jq -er '.accessToken | select(type == "string" and length > 0)' <<<"${response}"
}

latest_index_generation_from_log() {
  local log="$1"
  local index_id="$2"
  local line
  line="$(awk -v marker="index.id=${index_id} " '
      index($0, marker) && index($0, "index.kind=Path") &&
      index($0, "index generation published") { line = $0 }
      END { if (line != "") print line }
    ' "${log}")"
  log_unsigned_field generation "${line}"
}

select_indexed_cutover_fixture() {
  local state="$1"
  local builder="$2"
  local reassignment_log="${ANVIL_QUALIFICATION_DIR}/artifacts/index-reassignment-2-${builder}.log"
  local line
  while IFS= read -r line; do
    membership_cutover_index_id="$(log_unsigned_field index.id "${line}" || true)"
    [[ -n "${membership_cutover_index_id}" ]] && break
  done < <(awk '
      index($0, "index.kind=Path") && index($0, "index generation published")
    ' "${reassignment_log}")
  if [[ ! "${membership_cutover_index_id}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${builder} published no selectable pre-cutover Path index" >&2
    return 1
  fi
  membership_cutover_index_bucket="$(
    jq -er --argjson index_id "${membership_cutover_index_id}" \
      '.fixtures[] | select(.index_id == $index_id) | .bucket' "${state}" \
      | head -n 1
  )"
  membership_cutover_index_generation_before="$(
    latest_index_generation_from_log "${reassignment_log}" \
      "${membership_cutover_index_id}"
  )"
  if [[ -z "${membership_cutover_index_bucket}" \
    || ! "${membership_cutover_index_generation_before}" =~ ^[1-9][0-9]*$ ]]
  then
    echo "selected cutover index omitted its bucket or published generation" >&2
    return 1
  fi
}

indexed_cutover_bulk_request() {
  local tenant="$1"
  local bucket="$2"
  local first="$3"
  local count="$4"
  jq -nc \
    --arg tenant "${tenant}" --arg bucket "${bucket}" \
    --argjson first "${first}" --argjson count "${count}" '
      {operations: [range($first; $first + $count) as $position |
        {put: {
          address: {
            tenant: $tenant,
            bucket: $bucket,
            path: ("membership-cutover/pending-" + ($position | tostring) + ".json")
          },
          bytes: "eA==",
          contentType: "application/json",
          commandId: ("membership-cutover-indexed-" + ($position | tostring)),
          durability: "DURABILITY_LOCAL"
        }}]}'
}

prepare_indexed_membership_cutover_qualification() {
  local source_node="$1"
  local source_node_id="$2"
  local builder="$3"
  local tenant="$4"
  local client_id="$5"
  local client_secret="$6"
  local state="$7"
  local bound="$8"
  local endpoint
  local response="${ANVIL_QUALIFICATION_DIR}/artifacts/membership-indexed-bulk.json"
  local builder_log="${ANVIL_QUALIFICATION_DIR}/artifacts/membership-indexed-pending-${builder}.log"
  local builder_log_start
  local before_line
  local before_tail
  local index_safe
  local line
  local tail
  local first
  local last_index
  local attempt
  local sample_attempt

  select_indexed_cutover_fixture "${state}" "${builder}"
  membership_cutover_index_source_node_id="${source_node_id}"
  endpoint="$(public_endpoint_for "${source_node}")"
  membership_cutover_index_token="$(
    cutover_access_token "${endpoint}" "${client_id}" "${client_secret}"
  )"
  membership_cutover_index_burst=$((bound / 2))
  if ((membership_cutover_index_burst > 32)); then
    membership_cutover_index_burst=32
  elif ((membership_cutover_index_burst < 1)); then
    membership_cutover_index_burst=1
  fi
  chmod 0600 "${response}" 2>/dev/null || true

  for attempt in $(seq 1 8); do
    membership_cutover_index_attempts="${attempt}"
    first=$(((attempt - 1) * membership_cutover_index_burst))
    last_index=$((membership_cutover_index_burst - 1))
    before_line="$(latest_source_journal_sample "${source_node}")"
    before_tail="$(
      log_unsigned_field gauge.anvil_source_journal_tail "${before_line}" || true
    )"
    if [[ -z "${before_tail}" ]]; then
      sleep 1
      continue
    fi
    builder_log_start="$(log_cursor)"
    : >"${response}"
    chmod 0600 "${response}"
    if ! indexed_cutover_bulk_request \
        "${tenant}" "${membership_cutover_index_bucket}" \
        "${first}" "${membership_cutover_index_burst}" \
      | grpcurl_public "${endpoint}" \
          -H "authorization: Bearer ${membership_cutover_index_token}" -d @ \
          anvil.v1.ObjectService/BulkWrite >"${response}"
    then
      sleep 1
      continue
    fi
    paused_container="$(service_container "${builder}")"
    docker pause "${paused_container}" >/dev/null
    if ! jq -e --argjson count "${membership_cutover_index_burst}" '
        (.outcomes | length) == $count and
        all(.outcomes[]; (.receipt.version | tonumber) > 0)
      ' "${response}" >/dev/null
    then
      docker unpause "${paused_container}" >/dev/null
      paused_container=""
      sleep 1
      continue
    fi
    save_log_suffix "${builder}" "${builder_log_start}" "${builder_log}"
    if log_has_index_event \
      "${builder_log}" "${membership_cutover_index_id}" \
      "index generation published"
    then
      docker unpause "${paused_container}" >/dev/null
      paused_container=""
      sleep 1
      continue
    fi
    membership_cutover_index_path="membership-cutover/pending-$((first + last_index)).json"
    membership_cutover_index_version="$(
      jq -er --argjson index "${last_index}" '
        .outcomes[] |
        select(((.index // 0) | tonumber) == $index) |
        .receipt.version
      ' "${response}"
    )"
    for sample_attempt in $(seq 1 15); do
      line="$(latest_source_journal_sample "${source_node}")"
      tail="$(log_unsigned_field gauge.anvil_source_journal_tail "${line}" || true)"
      index_safe="$(
        log_unsigned_field gauge.anvil_source_journal_index_safe_through "${line}" || true
      )"
      if [[ -n "${tail}" && -n "${index_safe}" ]] \
        && ((tail > before_tail && index_safe < tail))
      then
        membership_cutover_index_source_tail="${tail}"
        preserve_qualification_log \
          "${builder_log}" \
          "/var/tmp/anvil-v080-three-membership-indexed-pending-${qualification_suffix}-${builder}.log"
        echo "[anvil-qualification] Path index ${membership_cutover_index_id} has accepted effect ${membership_cutover_index_path}@${membership_cutover_index_version} pending at source ${source_node_id} tail ${tail}; old-fence builder ${builder} is paused with no later publication"
        return 0
      fi
      sleep 1
    done
    docker unpause "${paused_container}" >/dev/null
    paused_container=""
  done
  echo "could not establish a measured pending indexed effect before membership cutover" >&2
  return 1
}

indexed_cutover_query_request() {
  local tenant="$1"
  jq -nc \
    --arg tenant "${tenant}" \
    --arg bucket "${membership_cutover_index_bucket}" \
    --arg path "${membership_cutover_index_path}" '
      {
        tenant: $tenant,
        bucket: $bucket,
        indexName: "paths",
        query: {path: {prefix: $path}},
        limit: 2
      }'
}

indexed_cutover_response_matches() {
  local response="$1"
  local tenant="$2"
  local fence_term="$3"
  local fence_index="$4"
  jq -e \
    --arg tenant "${tenant}" \
    --arg bucket "${membership_cutover_index_bucket}" \
    --arg path "${membership_cutover_index_path}" \
    --arg version "${membership_cutover_index_version}" \
    --argjson index_id "${membership_cutover_index_id}" \
    --argjson generation_before "${membership_cutover_index_generation_before}" \
    --argjson pending_source_node "${membership_cutover_index_source_node_id}" \
    --argjson pending_tail "${membership_cutover_index_source_tail}" \
    --argjson fence_term "${fence_term}" \
    --argjson fence_index "${fence_index}" '
      (.hits | length) == 1 and
      .hits[0].address.tenant == $tenant and
      .hits[0].address.bucket == $bucket and
      .hits[0].address.path == $path and
      (.hits[0].objectVersion | tonumber) == ($version | tonumber) and
      .freshness.initialBuildComplete == true and
      ((.freshness.rebuilding // false) == false) and
      (.freshness.indexId | tonumber) == $index_id and
      (.freshness.generation | tonumber) > $generation_before and
      (.freshness.placementTerm | tonumber) == $fence_term and
      (.freshness.placementIndex | tonumber) == $fence_index and
      (.freshness.sources | length) == 3 and
      ([.freshness.sources[].nodeId | tonumber] | sort) == [1, 2, 3] and
      all(.freshness.sources[];
        (.sourceEpoch | length) > 0 and
        ((.lagHint // "0") | tonumber) == 0 and
        ((.observedTail | tonumber) + 1) == (.indexedNextOffset | tonumber)) and
      any(.freshness.sources[];
        (.nodeId | tonumber) == $pending_source_node and
        (.indexedNextOffset | tonumber) > $pending_tail)
    ' "${response}" >/dev/null
}

wait_for_indexed_cutover_effect() {
  local tenant="$1"
  local fence_term="$2"
  local fence_index="$3"
  local attempt
  local endpoint
  local node
  local response
  local generation=""
  local all_ready
  for attempt in $(seq 1 90); do
    all_ready=1
    for node in anvil-1 anvil-2 anvil-3; do
      endpoint="$(public_endpoint_for "${node}")"
      response="${ANVIL_QUALIFICATION_DIR}/artifacts/membership-indexed-query-${node}.json"
      : >"${response}"
      chmod 0600 "${response}"
      if ! indexed_cutover_query_request "${tenant}" \
        | grpcurl_public "${endpoint}" \
            -H "authorization: Bearer ${membership_cutover_index_token}" -d @ \
            anvil.v1.IndexService/QueryIndex >"${response}" 2>/dev/null \
        || ! indexed_cutover_response_matches \
          "${response}" "${tenant}" "${fence_term}" "${fence_index}"
      then
        all_ready=0
        break
      fi
      generation="$(jq -er '.freshness.generation' "${response}")"
    done
    if ((all_ready == 1)); then
      membership_cutover_index_generation_after="${generation}"
      for node in anvil-1 anvil-2 anvil-3; do
        preserve_qualification_log \
          "${ANVIL_QUALIFICATION_DIR}/artifacts/membership-indexed-query-${node}.json" \
          "/var/tmp/anvil-v080-three-membership-indexed-query-${qualification_suffix}-${node}.json"
      done
      return 0
    fi
    sleep 1
  done
  echo "indexed cutover effect did not become exactly query-visible under the three-ACTIVE-node fence" >&2
  return 1
}

sample_after_log_line() {
  local log="$1"
  local preceding="$2"
  awk -v preceding="${preceding}" '
    $0 == preceding { after = 1; next }
    after && index($0, "sampled source-journal safety and capacity") { line = $0 }
    END { if (line != "") print line }
  ' "${log}"
}

qualify_no_event_membership_cutover() {
  local node="$1"
  local node_id="$2"
  local client_id="$3"
  local client_secret="$4"
  local tenant="$5"
  local bucket="$6"
  local bound="$7"
  local evidence="${ANVIL_QUALIFICATION_DIR}/artifacts/membership-no-event-${node}.log"
  local fence=
  local fence_index=
  local fence_term=
  local indexed_line
  local indexed_proof_complete=0
  local line=
  local tail
  local attempt
  for attempt in $(seq 1 90); do
    save_log_suffix "${node}" "${membership_cutover_source_log_start}" "${evidence}"
    fence="$(new_cutover_fence_line \
      "${evidence}" "${node_id}" \
      "${membership_cutover_source_fence_term}" \
      "${membership_cutover_source_fence_index}" 3 || true)"
    [[ -n "${fence}" ]] && line="$(sample_after_log_line "${evidence}" "${fence}")"
    if [[ -n "${membership_cutover_index_id}" ]]; then
      fence_term="$(log_unsigned_field membership.term "${fence}" || true)"
      fence_index="$(log_unsigned_field membership.index "${fence}" || true)"
      indexed_line="$(latest_source_journal_sample anvil-1)"
      if [[ -n "${fence_term}" && -n "${fence_index}" ]] \
        && [[ "$(log_unsigned_field gauge.anvil_source_journal_index_safe_through "${indexed_line}" || true)" \
          == "${membership_cutover_index_source_tail}" ]]
      then
        wait_for_indexed_cutover_effect \
          "qindex-membership" \
          "${fence_term}" "${fence_index}"
        indexed_proof_complete=1
      else
        indexed_line=""
      fi
    fi
    if source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
      tail="$(log_unsigned_field gauge.anvil_source_journal_tail "${line}")"
      if [[ "${tail}" != "${membership_cutover_source_tail}" ]]; then
        echo "${node} appended source events during the intended no-event cutover" >&2
        echo "pre-cutover tail: ${membership_cutover_source_tail}; post-cutover tail: ${tail}" >&2
        return 1
      fi
      [[ -z "${membership_cutover_index_id}" || "${indexed_proof_complete}" == "1" ]] && break
    fi
    sleep 1
  done
  if ! source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
    echo "${node} did not prove index/accounting convergence at its unchanged tail under the new membership fence" >&2
    return 1
  fi
  if [[ -n "${membership_cutover_index_id}" \
    && "${indexed_proof_complete}" != "1" ]]
  then
    echo "indexed cutover proof did not complete under the selected three-node fence" >&2
    return 1
  fi

  run_cutover_writes \
    "${node}" "${client_id}" "${client_secret}" "${tenant}" "${bucket}" \
    post 0 "${bound}"
  for attempt in $(seq 1 30); do
    line="$(latest_source_journal_sample "${node}")"
    tail="$(log_unsigned_field gauge.anvil_source_journal_tail "${line}" || true)"
    [[ -n "${tail}" ]] \
      && ((tail > membership_cutover_source_tail)) \
      && break
    sleep 1
  done
  if [[ -z "${tail}" ]] || ((tail <= membership_cutover_source_tail)); then
    echo "${node} did not admit a subsequent ordinary write on its bounded source journal" >&2
    return 1
  fi
  save_log_suffix "${node}" "${membership_cutover_source_log_start}" "${evidence}"
  preserve_qualification_log \
    "${evidence}" \
    "/var/tmp/anvil-v080-three-membership-no-event-${qualification_suffix}-${node}.log"
  if [[ -n "${membership_cutover_index_id}" ]]; then
    echo "[anvil-qualification] indexed cutover preserved Path index ${membership_cutover_index_id}, made ${membership_cutover_index_path}@${membership_cutover_index_version} visible in generation ${membership_cutover_index_generation_after}, and proved three-source zero-lag freshness under the exact three-ACTIVE-node fence"
  fi
  echo "[anvil-qualification] cutover advanced index/accounting through source ${node_id} tail ${membership_cutover_source_tail} under the new fence; the next ordinary write advanced it to ${tail}"
}
