#!/usr/bin/env bash

# Shared three-node membership and source-journal qualification helpers.

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
      && [[ "$(log_unsigned_field gauge.keldra_source_journal_max_entries "${line}" || true)" == "${expected}" ]]
    then
      return 0
    fi
    sleep 1
  done
  echo "${node} did not report source-journal max entries ${expected}" >&2
  return 1
}

start_source_journal_phase() {
  local bound="$1"
  shift
  local node
  export KELDRA_QUALIFICATION_SOURCE_JOURNAL_MAX_ENTRIES="${bound}"
  for node in "$@"; do
    compose up --detach --no-deps --force-recreate "${node}"
    wait_for_node "${node}"
    require_service_image "${node}" "${image_id}" qualification
    wait_for_source_journal_entry_bound "${node}" "${bound}"
  done
}

start_release_source_journal_phase() {
  local bound="$1"
  local node
  start_source_journal_phase "${bound}" keldra-1 keldra-2 keldra-3
  public_endpoints=()
  for node in keldra-1 keldra-2 keldra-3; do
    public_endpoints+=("$(public_endpoint_for "${node}")")
  done
  echo "[keldra-qualification] journal-pressure phase ended; release phases use source-journal max entries ${bound}"
}

prepare_joining_node() {
  local node_id="$1"
  local service="keldra-${node_id}"
  local peer_address="${service}:50052"
  local leader
  local output=""
  local bundle_path=""
  local source_service=""
  for leader in keldra-1 keldra-2 keldra-3; do
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
  if [[ "${bundle_path}" != "/var/lib/keldra/keldra-node-${node_id}.join.json" ]]; then
    echo "node ${node_id} preparation did not return its expected private bundle" >&2
    echo "last administration output: ${output}" >&2
    return 1
  fi

  local copied="${KELDRA_QUALIFICATION_DIR}/artifacts/keldra-node-${node_id}.join.json"
  compose cp "${source_service}:${bundle_path}" "${copied}"
  chmod 0600 "${copied}"
  docker run --rm --user 0 \
    --volume "${copied}:/join-bundle" \
    "${image_id}" chown 10001:10001 /join-bundle
}

start_prepared_node() {
  local node_id="$1"
  local service="keldra-${node_id}"
  compose up --detach "${service}"
  wait_for_node "${service}" "${joining_node_ready_timeout_seconds}"
  echo "[keldra-qualification] ${service} public coordinator endpoint became ready within ${joining_node_ready_timeout_seconds}s"
  if [[ -e "${KELDRA_QUALIFICATION_DIR}/artifacts/keldra-node-${node_id}.join.json" ]]; then
    echo "${service} became ready without consuming and deleting its join bundle" >&2
    return 1
  fi
}

wait_for_background_join() {
  local service="$1"
  local timeout_seconds="$2"
  local attempt
  for attempt in $(seq 1 "${timeout_seconds}"); do
    # Consume the complete log stream. With pipefail, grep -q can close the
    # pipe after the match and turn docker logs' SIGPIPE into a false failure.
    if service_logs "${service}" \
      | grep -F 'background cluster join completed' >/dev/null
    then
      echo "[keldra-qualification] ${service} background ownership handoff completed"
      return 0
    fi
    sleep 1
  done
  echo "${service} did not complete background ownership handoff within ${timeout_seconds}s" >&2
  return 1
}

prepare_and_start_node() {
  prepare_joining_node "$1"
  start_prepared_node "$1"
}

# State captured immediately before the two-to-three membership cutover. Node 2
# is deliberately used because node 1 is the stable lowest-ID membership
# reconciliation coordinator.
membership_cutover_source_tail=
membership_cutover_source_fence_term=
membership_cutover_source_fence_index=
membership_cutover_source_log_start=

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
  tail="$(log_unsigned_field gauge.keldra_source_journal_tail "${line}" || true)"
  settled="$(log_unsigned_field gauge.keldra_source_journal_settled_through "${line}" || true)"
  index="$(log_unsigned_field gauge.keldra_source_journal_index_safe_through "${line}" || true)"
  accounting="$(log_unsigned_field gauge.keldra_source_journal_accounting_safe_through "${line}" || true)"
  retained="$(log_unsigned_field gauge.keldra_source_journal_retained_entries "${line}" || true)"
  maximum="$(log_unsigned_field gauge.keldra_source_journal_max_entries "${line}" || true)"
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
  KELDRA_CUTOVER_QUALIFICATION_ENDPOINT="$(public_endpoint_for "${node}")" \
  KELDRA_CUTOVER_QUALIFICATION_TENANT="${tenant}" \
  KELDRA_CUTOVER_QUALIFICATION_BUCKET="${bucket}" \
  KELDRA_CUTOVER_QUALIFICATION_CLIENT_ID="${client_id}" \
  KELDRA_CUTOVER_QUALIFICATION_CLIENT_SECRET="${client_secret}" \
  KELDRA_CUTOVER_QUALIFICATION_PHASE="${phase}" \
  KELDRA_CUTOVER_QUALIFICATION_FIRST="${first}" \
  KELDRA_CUTOVER_QUALIFICATION_COUNT="${count}" \
    "${qualification_binaries[cluster_cutover_qualification]}" >/dev/null
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
  local clear_tail
  local fence
  local line
  local previous_clear_line=
  local previous_clear_tail=
  local round
  local stable_clear=0
  if [[ "${node_id}" == "1" ]]; then
    echo "no-event membership cutover source must not be the reconciliation coordinator" >&2
    return 1
  fi

  for round in 0 1; do
    run_cutover_writes \
      "${node}" "${client_id}" "${client_secret}" "${tenant}" "${bucket}" \
      pre "$((round * batch))" "${batch}"
    # LOCAL durability may finish its deliberately asynchronous payload
    # placement after the client has received every mutation receipt. A
    # single sampled clear cut therefore does not prove that the source is
    # quiescent: the 10-second sampler can still be showing the state from
    # immediately before a final background placement. Require the same clear
    # tail in two distinct samples before declaring the membership cutover to
    # be a no-event interval.
    previous_clear_line=
    previous_clear_tail=
    stable_clear=0
    for attempt in $(seq 1 45); do
      line="$(latest_source_journal_sample "${node}")"
      if source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
        clear_tail="$(log_unsigned_field gauge.keldra_source_journal_tail "${line}")"
        if [[ -n "${previous_clear_line}" \
          && "${line}" != "${previous_clear_line}" \
          && "${clear_tail}" == "${previous_clear_tail}" ]]
        then
          stable_clear=1
          break
        fi
        if [[ "${line}" != "${previous_clear_line}" ]]; then
          previous_clear_line="${line}"
          previous_clear_tail="${clear_tail}"
        fi
      else
        previous_clear_line=
        previous_clear_tail=
      fi
      sleep 1
    done
    ((stable_clear == 1)) && break
  done
  if ((stable_clear != 1)); then
    echo "${node} did not reach a stable clear source-journal entry bound of ${bound}" >&2
    printf '%s\n' "${line}" >&2
    return 1
  fi
  membership_cutover_source_tail="$(
    log_unsigned_field gauge.keldra_source_journal_tail "${line}"
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
  echo "[keldra-qualification] non-coordinator source ${node_id} reached journal bound ${bound} at tail ${membership_cutover_source_tail} before the no-event 2->3 cutover"
}

refresh_no_event_membership_cutover_tail() {
  local node="$1"
  local bound="$2"
  local attempt
  local clear_tail
  local line=
  local previous_clear_line=
  local previous_clear_tail=
  for attempt in $(seq 1 45); do
    line="$(latest_source_journal_sample "${node}")"
    if source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
      clear_tail="$(log_unsigned_field gauge.keldra_source_journal_tail "${line}")"
      if [[ -n "${previous_clear_line}" \
        && "${line}" != "${previous_clear_line}" \
        && "${clear_tail}" == "${previous_clear_tail}" ]]
      then
        membership_cutover_source_tail="${clear_tail}"
        echo "[keldra-qualification] refreshed no-event cutover baseline at tail ${membership_cutover_source_tail} after the JOINING-node probe"
        return 0
      fi
      if [[ "${line}" != "${previous_clear_line}" ]]; then
        previous_clear_line="${line}"
        previous_clear_tail="${clear_tail}"
      fi
    else
      previous_clear_line=
      previous_clear_tail=
    fi
    sleep 1
  done
  echo "${node} did not return to a stable clear source-journal entry bound of ${bound} after the JOINING-node probe" >&2
  printf '%s\n' "${line}" >&2
  return 1
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
  local evidence="${KELDRA_QUALIFICATION_DIR}/artifacts/membership-no-event-${node}.log"
  local fence=
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
    if source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
      tail="$(log_unsigned_field gauge.keldra_source_journal_tail "${line}")"
      if [[ "${tail}" != "${membership_cutover_source_tail}" ]]; then
        echo "${node} appended source events during the intended no-event cutover" >&2
        echo "pre-cutover tail: ${membership_cutover_source_tail}; post-cutover tail: ${tail}" >&2
        return 1
      fi
      break
    fi
    sleep 1
  done
  if ! source_journal_sample_is_clear_at_bound "${line}" "${bound}"; then
    echo "${node} did not prove derived-consumer convergence at its unchanged tail under the new membership fence" >&2
    return 1
  fi

  run_cutover_writes \
    "${node}" "${client_id}" "${client_secret}" "${tenant}" "${bucket}" \
    post 0 "${bound}"
  for attempt in $(seq 1 30); do
    line="$(latest_source_journal_sample "${node}")"
    tail="$(log_unsigned_field gauge.keldra_source_journal_tail "${line}" || true)"
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
    "/var/tmp/keldra-v090-three-membership-no-event-${qualification_suffix}-${node}.log"
  echo "[keldra-qualification] cutover advanced derived consumers through source ${node_id} tail ${membership_cutover_source_tail} under the new fence; the next ordinary write advanced it to ${tail}"
}
