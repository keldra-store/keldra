#!/usr/bin/env bash

# Bounded scale comparison built from the existing public resource workload and
# the low-cardinality telemetry emitted by Anvil itself.

telemetry_files() {
  local prefix="$1"
  local file
  for file in "${prefix}.log" "${prefix}"-*.log; do
    [[ -f "${file}" ]] && printf '%s\n' "${file}"
  done
}

log_span_unsigned_field() {
  local field="$1"
  local line="$2"
  local escaped_field="${field//./\\.}"
  local pattern="(^|[[:space:]{])${escaped_field}=([0-9]+)($|[[:space:]},])"
  if [[ "${line}" =~ ${pattern} ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

log_field_equals() {
  local field="$1"
  local expected="$2"
  local line="$3"
  local escaped_field="${field//./\\.}"
  local pattern="(^|[[:space:]{])${escaped_field}=${expected}($|[[:space:]},])"
  local quoted_pattern="(^|[[:space:]{])${escaped_field}=\"${expected}\"($|[[:space:]},])"
  [[ "${line}" =~ ${pattern} || "${line}" =~ ${quoted_pattern} ]]
}

log_span_number_field() {
  local field="$1"
  local line="$2"
  local escaped_field="${field//./\\.}"
  local pattern="(^|[[:space:]{])${escaped_field}=([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)($|[[:space:]},])"
  if [[ "${line}" =~ ${pattern} ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

log_word_field() {
  local field="$1"
  local line="$2"
  local escaped_field="${field//./\\.}"
  local pattern="(^|[[:space:]{])${escaped_field}=([A-Za-z0-9_-]+)($|[[:space:]},])"
  local quoted_pattern="(^|[[:space:]{])${escaped_field}=\"([A-Za-z0-9_-]+)\"($|[[:space:]},])"
  if [[ "${line}" =~ ${pattern} ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  if [[ "${line}" =~ ${quoted_pattern} ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

matching_publication_authoritative_bytes() {
  local prefix="$1"
  local expected_index_id="$2"
  local expected_generation="$3"
  local authoritative_bytes=0
  local count=0
  local file
  local generation
  local index_id
  local line
  while IFS= read -r file; do
    while IFS= read -r line; do
      index_id="$(log_span_unsigned_field index.id "${line}")" || continue
      generation="$(log_span_unsigned_field generation "${line}")" || continue
      [[ "${index_id}" == "${expected_index_id}" \
        && "${generation}" == "${expected_generation}" ]] || continue
      authoritative_bytes="$(
        log_span_unsigned_field publication.authoritative_bytes "${line}"
      )" || return 1
      count=$((count + 1))
    done < <(
      awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index generation publication metrics")
      ' "${file}"
    )
  done < <(telemetry_files "${prefix}")
  if ((count != 1 || authoritative_bytes == 0)); then
    echo "TypedJson incident generation matched ${count} publication records; expected exactly one" >&2
    return 1
  fi
  printf '%s\n' "${authoritative_bytes}"
}

incident_query_terminal_json() {
  local name="$1"
  local expected_index_id="$2"
  local expected_definition_version="$3"
  local expected_generation="$4"
  local expected_hits="$5"
  local physical_fetches="$6"
  local physical_fetch_bytes="$7"
  local line="$8"
  local duration
  local field
  local read_quantum
  local tier
  local value
  local -a fields=(
    monotonic_counter.anvil_index_query_read_ops_total
    monotonic_counter.anvil_index_query_read_bytes_total
    monotonic_counter.anvil_index_query_cooperative_yields_total
    monotonic_counter.anvil_index_query_failures_total
    monotonic_counter.anvil_index_query_cancellations_total
    monotonic_counter.anvil_index_query_planner_conjunctions_total
    monotonic_counter.anvil_index_query_planner_reordered_conjunctions_total
    monotonic_counter.anvil_index_query_planner_costed_children_total
    monotonic_counter.anvil_index_query_planner_child_cost_total
    monotonic_counter.anvil_index_query_term_seeks_total
    monotonic_counter.anvil_index_query_enumerated_terms_total
    monotonic_counter.anvil_index_query_posting_blocks_decoded_total
    monotonic_counter.anvil_index_query_posting_blocks_sought_total
    monotonic_counter.anvil_index_query_posting_blocks_skipped_total
    monotonic_counter.anvil_index_query_posting_bytes_read_total
    monotonic_counter.anvil_index_query_posting_advance_calls_total
    monotonic_counter.anvil_index_query_conjunction_advances_total
    monotonic_counter.anvil_index_query_union_heap_pushes_total
    monotonic_counter.anvil_index_query_union_heap_pops_total
    monotonic_counter.anvil_index_query_two_phase_verifications_total
    monotonic_counter.anvil_index_query_candidate_doc_ids_total
    monotonic_counter.anvil_index_query_live_mask_blocks_decoded_total
    monotonic_counter.anvil_index_query_live_mask_rejects_total
    monotonic_counter.anvil_index_query_fast_column_blocks_decoded_total
    monotonic_counter.anvil_index_query_stored_field_blocks_decoded_total
    monotonic_counter.anvil_index_query_cursor_seeks_total
    monotonic_counter.anvil_index_query_cursor_skipped_doc_ids_total
    monotonic_counter.anvil_index_query_physical_early_terminations_total
    monotonic_counter.anvil_index_query_top_k_inspected_total
    monotonic_counter.anvil_index_query_candidate_gate_checked_total
    monotonic_counter.anvil_index_query_candidate_gate_batches_total
    monotonic_counter.anvil_index_query_candidate_gate_denied_total
    monotonic_counter.anvil_index_query_candidate_gate_stale_total
    monotonic_counter.anvil_index_query_candidate_gate_refills_total
    histogram.anvil_index_query_returned_hits
    histogram.anvil_index_query_planner_lead_cost_min
    histogram.anvil_index_query_planner_lead_cost_max
  )
  [[ "$(log_span_unsigned_field index.id "${line}")" == "${expected_index_id}" ]] \
    || return 1
  [[ "$(log_span_unsigned_field definition.version "${line}")" \
    == "${expected_definition_version}" ]] || return 1
  [[ "$(log_span_unsigned_field generation "${line}")" == "${expected_generation}" ]] \
    || return 1
  log_field_equals query.outcome completed "${line}" || return 1
  tier="$(log_word_field index.tier "${line}")" || return 1
  duration="$(
    log_span_number_field histogram.anvil_index_query_duration_seconds "${line}"
  )" || return 1
  read_quantum="$(
    log_span_number_field histogram.anvil_index_query_read_quantum_bytes "${line}"
  )" || return 1
  local rows
  rows="$(mktemp)"
  for field in "${fields[@]}"; do
    value="$(log_span_unsigned_field "${field}" "${line}")" || {
      echo "incident query terminal event omitted ${field}" >&2
      rm -f -- "${rows}"
      return 1
    }
    printf '%s\t%s\n' "${field}" "${value}" >>"${rows}"
  done
  jq -Rn \
    --arg name "${name}" \
    --arg tier "${tier}" \
    --argjson index_id "${expected_index_id}" \
    --argjson definition_version "${expected_definition_version}" \
    --argjson generation "${expected_generation}" \
    --argjson expected_hits "${expected_hits}" \
    --argjson physical_fetches "${physical_fetches}" \
    --argjson physical_fetch_bytes "${physical_fetch_bytes}" \
    --argjson duration_seconds "${duration}" \
    --argjson read_quantum_bytes "${read_quantum}" '
      [inputs | split("\t") | {(.[0]): (.[1] | tonumber)}] | add as $c |
      {
        name: $name,
        index_id: $index_id,
        definition_version: $definition_version,
        generation: $generation,
        tier: $tier,
        expected_hits: $expected_hits,
        logical_read_ops: $c["monotonic_counter.anvil_index_query_read_ops_total"],
        logical_read_bytes: $c["monotonic_counter.anvil_index_query_read_bytes_total"],
        physical_cache_fetches: $physical_fetches,
        physical_cache_fetch_bytes: $physical_fetch_bytes,
        cache_warm_for_required_blocks: ($physical_fetch_bytes == 0),
        duration_seconds: $duration_seconds,
        read_quantum_bytes: $read_quantum_bytes,
        terminal_counters: $c
      }
    ' <"${rows}"
  local status=$?
  rm -f -- "${rows}"
  return "${status}"
}

write_incident_query_evidence() {
  local topology="$1"
  local client_report="$2"
  local telemetry_prefix="$3"
  local output="$4"
  local authoritative_bytes
  local candidate_count=0
  local corpus_records
  local expected_definition_version
  local expected_generation
  local expected_index_id
  local file
  local line
  local publication_bytes_ceiling
  local record_ceiling
  local value
  local -a selected_fetch_bytes=()
  local -a selected_fetches=()
  local -a selected_lines=()
  local -a expected_hits=(4 999 999 0 4)
  local -a names=(limit_four page_one page_two zero_hit_sparse_conjunction unselective_arbitrary_sort)

  if [[ -e "${output}" ]]; then
    echo "incident-query evidence output already exists: ${output}" >&2
    return 1
  fi
  expected_index_id="$(jq -er '.production_query_regression.index_id' "${client_report}")"
  expected_definition_version="$(
    jq -er '.production_query_regression.definition_version' "${client_report}"
  )"
  expected_generation="$(jq -er '.production_query_regression.generation' "${client_report}")"
  corpus_records="$(jq -er '.records' "${client_report}")"
  if [[ "${expected_index_id}" == "0" ]] \
    || ((expected_definition_version == 0 || expected_generation == 0 || corpus_records == 0))
  then
    echo "incident-query client report omitted its immutable index identity" >&2
    return 1
  fi
  authoritative_bytes="$(matching_publication_authoritative_bytes \
    "${telemetry_prefix}" "${expected_index_id}" "${expected_generation}")"
  publication_bytes_ceiling=$((authoritative_bytes / 4))
  record_ceiling=$((corpus_records / 4))
  ((publication_bytes_ceiling > 0)) || publication_bytes_ceiling=1
  ((record_ceiling > 0)) || record_ceiling=1

  while IFS= read -r file; do
    local active=0
    local fetch_bytes=0
    local fetches=0
    local returned_hits
    local session_index_id
    local -a file_fetch_bytes=()
    local -a file_fetches=()
    local -a file_lines=()
    while IFS= read -r line; do
      session_index_id="$(log_span_unsigned_field index.id "${line}" || true)"
      [[ "${session_index_id}" == "${expected_index_id}" ]] || continue
      if [[ "${line}" == *"local index query admitted"* ]] \
        && [[ "$(log_span_unsigned_field \
          monotonic_counter.anvil_index_query_runs_total "${line}" || true)" == "1" ]]
      then
        active=1
        fetch_bytes=0
        fetches=0
        continue
      fi
      ((active == 1)) || continue
      if [[ "${line}" == *"index cache block fetch"* ]]; then
        value="$(log_span_unsigned_field \
          monotonic_counter.anvil_index_cache_fetches_total "${line}" || true)"
        [[ -z "${value}" ]] || fetches=$((fetches + value))
      fi
      if [[ "${line}" == *"index cache block fetched"* ]]; then
        value="$(log_span_unsigned_field \
          monotonic_counter.anvil_index_cache_fetch_bytes_total "${line}" || true)"
        [[ -z "${value}" ]] || fetch_bytes=$((fetch_bytes + value))
      fi
      if [[ "${line}" == *"local index query reached a terminal outcome"* ]]; then
        file_lines+=("${line}")
        file_fetches+=("${fetches}")
        file_fetch_bytes+=("${fetch_bytes}")
        active=0
      fi
    done <"${file}"

    local start
    local position
    for ((start = 0; start + 4 < ${#file_lines[@]}; start++)); do
      for ((position = 0; position < 5; position++)); do
        returned_hits="$(log_span_unsigned_field \
          histogram.anvil_index_query_returned_hits \
          "${file_lines[start + position]}" || true)"
        [[ "${returned_hits}" == "${expected_hits[position]}" ]] || break
      done
      if ((position == 5)); then
        for ((position = 0; position < 5; position++)); do
          [[ "$(log_span_unsigned_field definition.version \
            "${file_lines[start + position]}" || true)" \
            == "${expected_definition_version}" ]] || break
          [[ "$(log_span_unsigned_field generation \
            "${file_lines[start + position]}" || true)" \
            == "${expected_generation}" ]] || break
        done
      fi
      if ((position == 5)); then
        candidate_count=$((candidate_count + 1))
        if ((candidate_count == 1)); then
          for ((position = 0; position < 5; position++)); do
            selected_lines[position]="${file_lines[start + position]}"
            selected_fetches[position]="${file_fetches[start + position]}"
            selected_fetch_bytes[position]="${file_fetch_bytes[start + position]}"
          done
        fi
      fi
    done
  done < <(telemetry_files "${telemetry_prefix}")

  if ((candidate_count != 1)); then
    echo "incident query sequence matched ${candidate_count} telemetry sessions; expected exactly one" >&2
    return 1
  fi

  local queries
  queries="$(mktemp)"
  for ((position = 0; position < 5; position++)); do
    incident_query_terminal_json \
      "${names[position]}" \
      "${expected_index_id}" \
      "${expected_definition_version}" \
      "${expected_generation}" \
      "${expected_hits[position]}" \
      "${selected_fetches[position]}" \
      "${selected_fetch_bytes[position]}" \
      "${selected_lines[position]}" >>"${queries}" || {
        rm -f -- "${queries}"
        return 1
      }
  done

  jq -s \
    --arg schema anvil.index-production-query-server-evidence.v1 \
    --arg topology "${topology}" \
    --arg source_commit "${source_commit}" \
    --argjson corpus_records "${corpus_records}" \
    --argjson index_id "${expected_index_id}" \
    --argjson definition_version "${expected_definition_version}" \
    --argjson generation "${expected_generation}" \
    --argjson authoritative_bytes "${authoritative_bytes}" \
    --argjson publication_bytes_ceiling "${publication_bytes_ceiling}" \
    --argjson record_ceiling "${record_ceiling}" '
      . as $queries |
      {
        schema: $schema,
        topology: $topology,
        source_commit: $source_commit,
        corpus_records: $corpus_records,
        index_id: $index_id,
        definition_version: $definition_version,
        generation: $generation,
        authoritative_generation_bytes: $authoritative_bytes,
        ordered_query_logical_read_bytes_ceiling: $publication_bytes_ceiling,
        ordered_query_candidate_doc_ids_ceiling: $record_ceiling,
        queries: $queries,
        result: "pass"
      }
      | select(
          ($queries | length) == 5 and
          ($queries | map(.name)) == [
            "limit_four", "page_one", "page_two",
            "zero_hit_sparse_conjunction", "unselective_arbitrary_sort"
          ] and
          ($queries | map(.terminal_counters["histogram.anvil_index_query_returned_hits"])) ==
            [4, 999, 999, 0, 4] and
          ($queries | all(
            .index_id == $index_id and
            .definition_version == $definition_version and
            .generation == $generation and
            .terminal_counters["monotonic_counter.anvil_index_query_failures_total"] == 0 and
            .terminal_counters["monotonic_counter.anvil_index_query_cancellations_total"] == 0 and
            .logical_read_ops > 0 and .logical_read_bytes > 0 and .duration_seconds > 0
          )) and
          ($queries[0:4] | all(
            .tier == "physical" and
            .logical_read_bytes < $publication_bytes_ceiling and
            .terminal_counters["monotonic_counter.anvil_index_query_candidate_doc_ids_total"] < $record_ceiling and
            .terminal_counters["monotonic_counter.anvil_index_query_stored_field_blocks_decoded_total"] < $record_ceiling
          )) and
          ($queries[0:3] | all(
            .terminal_counters["monotonic_counter.anvil_index_query_physical_early_terminations_total"] > 0
          )) and
          $queries[2].terminal_counters["monotonic_counter.anvil_index_query_cursor_seeks_total"] > 0 and
          $queries[2].terminal_counters["monotonic_counter.anvil_index_query_cursor_skipped_doc_ids_total"] < $record_ceiling and
          $queries[4].tier == "top_k" and
          $queries[4].terminal_counters["monotonic_counter.anvil_index_query_top_k_inspected_total"] > 0
        )
    ' "${queries}" >"${output}"
  local status=$?
  rm -f -- "${queries}"
  if ((status != 0)) || [[ ! -s "${output}" ]]; then
    rm -f -- "${output}"
    echo "incident query telemetry violated the RFC 0014 bounded-read contract" >&2
    return 1
  fi
  chmod 0600 "${output}"
  echo "[anvil-qualification] preserved incident-query server evidence ${output}"
}

isolated_typed_json_query_work() {
  local prefix="$1"
  local expected_index_id="$2"
  local bytes
  local file
  local index_id
  local line
  local ops
  local terminal_count=0
  while IFS= read -r file; do
    while IFS= read -r line; do
      terminal_count=$((terminal_count + 1))
      index_id="$(log_span_unsigned_field index.id "${line}")" || return 1
      ops="$(log_unsigned_field monotonic_counter.anvil_index_query_read_ops_total "${line}")" \
        || return 1
      bytes="$(log_unsigned_field monotonic_counter.anvil_index_query_read_bytes_total "${line}")" \
        || return 1
      if [[ "${index_id}" != "${expected_index_id}" ]] \
        || ! log_field_equals query.outcome completed "${line}" \
        || [[ "$(log_unsigned_field monotonic_counter.anvil_index_query_failures_total "${line}")" != "0" ]] \
        || [[ "$(log_unsigned_field monotonic_counter.anvil_index_query_cancellations_total "${line}")" != "0" ]] \
        || [[ "$(log_unsigned_field histogram.anvil_index_query_returned_hits "${line}")" != "1" ]] \
        || ((ops == 0 || bytes == 0))
      then
        echo "isolated TypedJson query emitted inconsistent terminal telemetry" >&2
        printf '%s\n' "${line}" >&2
        return 1
      fi
    done < <(
      awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "local index query reached a terminal outcome")
      ' "${file}"
    )
  done < <(telemetry_files "${prefix}")
  if ((terminal_count != 1)); then
    echo "isolated TypedJson probe emitted ${terminal_count} terminal query events; expected exactly one" >&2
    return 1
  fi
  printf '%s %s\n' "${ops}" "${bytes}"
}

matching_publication_segment_count() {
  local prefix="$1"
  local expected_index_id="$2"
  local expected_generation="$3"
  local count=0
  local file
  local generation
  local index_id
  local line
  local segments=0
  while IFS= read -r file; do
    while IFS= read -r line; do
      index_id="$(log_span_unsigned_field index.id "${line}")" || continue
      generation="$(log_span_unsigned_field generation "${line}")" || continue
      [[ "${index_id}" == "${expected_index_id}" \
        && "${generation}" == "${expected_generation}" ]] || continue
      segments="$(log_span_unsigned_field publication.segments "${line}")" || return 1
      count=$((count + 1))
    done < <(
      awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index generation publication metrics")
      ' "${file}"
    )
  done < <(telemetry_files "${prefix}")
  if ((count != 1 || segments == 0)); then
    echo "TypedJson probe generation matched ${count} publication records; expected exactly one" >&2
    return 1
  fi
  printf '%s\n' "${segments}"
}

terminal_typed_json_debt() {
  local prefix="$1"
  local byte_limit=0
  local debt_bytes
  local debt_tiers
  local debt_segments
  local evidence=0
  local file
  local line
  local observed_byte_limit
  local observed_segment_limit
  local segment_limit=0
  while IFS= read -r file; do
    line="$(awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index compaction debt observed") { line = $0 }
        END { print line }
      ' "${file}")"
    [[ -n "${line}" ]] || continue
    debt_tiers="$(log_unsigned_field gauge.anvil_index_compaction_debt_tiers "${line}")" \
      || return 1
    debt_segments="$(log_unsigned_field gauge.anvil_index_compaction_debt_segments "${line}")" \
      || return 1
    debt_bytes="$(log_unsigned_field gauge.anvil_index_compaction_debt_bytes "${line}")" \
      || return 1
    observed_segment_limit="$(log_unsigned_field gauge.anvil_index_compaction_debt_segment_limit "${line}")" \
      || return 1
    observed_byte_limit="$(log_unsigned_field gauge.anvil_index_compaction_debt_byte_limit "${line}")" \
      || return 1
    if ((debt_tiers != 0 || debt_segments != 0 || debt_bytes != 0)); then
      echo "TypedJson scale run ended with compaction debt" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    if ((evidence != 0)) \
      && ((segment_limit != observed_segment_limit || byte_limit != observed_byte_limit)); then
      echo "TypedJson builders reported inconsistent debt limits" >&2
      return 1
    fi
    segment_limit="${observed_segment_limit}"
    byte_limit="${observed_byte_limit}"
    evidence=$((evidence + 1))
  done < <(telemetry_files "${prefix}")
  if ((evidence == 0 || segment_limit == 0 || byte_limit == 0)); then
    echo "TypedJson scale run emitted no terminal debt evidence" >&2
    return 1
  fi
  printf '%s %s %s\n' "${segment_limit}" "${byte_limit}" "${evidence}"
}

typed_json_terminal_event_count() {
  local prefix="$1"
  local count=0
  local file
  local observed
  while IFS= read -r file; do
    observed="$(awk '
      index($0, "index.kind=TypedJson") &&
      index($0, "local index query reached a terminal outcome") { count++ }
      END { print count + 0 }
    ' "${file}")"
    count=$((count + observed))
  done < <(telemetry_files "${prefix}")
  printf '%s\n' "${count}"
}

assert_typed_json_full_pool_projection() {
  local prefix="$1"
  local expected_workers="$2"
  local configured
  local effective
  local file
  local line
  while IFS= read -r file; do
    while IFS= read -r line; do
      configured="$(log_unsigned_field gauge.anvil_index_projection_configured_lanes "${line}")" \
        || continue
      effective="$(log_unsigned_field gauge.anvil_index_projection_effective_lanes "${line}")" \
        || continue
      if [[ "${configured}" == "${expected_workers}" \
        && "${effective}" == "${expected_workers}" ]]
      then
        echo "[anvil-qualification] TypedJson projection exercised all ${expected_workers} Rayon workers"
        return 0
      fi
    done < <(
      awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index projection wave started")
      ' "${file}"
    )
  done < <(telemetry_files "${prefix}")
  echo "TypedJson qualification emitted no projection wave using all ${expected_workers} Rayon workers" >&2
  return 1
}

capture_scale_publication_evidence() {
  local _topology="$1"
  local prefix="$2"
  local -a sources=()
  mapfile -t sources < <(telemetry_files "${index_resource_telemetry_prefix}")
  if ((${#sources[@]} == 0)); then
    echo "resource telemetry is absent while capturing publication evidence" >&2
    return 1
  fi
  awk '
    index($0, "index.kind=TypedJson") &&
    index($0, "index generation publication metrics")
  ' "${sources[@]}" >"${prefix}.log"
  while IFS= read -r file; do
    chmod 0600 "${file}"
  done < <(telemetry_files "${prefix}")
}

run_scale_singleton_probe() {
  local topology="$1"
  local probe_report="$2"
  local probe_telemetry_prefix="$3"
  local proof_report="$4"
  local publication_telemetry_prefix="${proof_report%.json}-publication-telemetry"
  local binary
  local endpoint
  local expected_sources
  local probe_status
  local single_start=0
  local -A starts=()
  local attempt
  local node

  case "${topology}" in
    single)
      binary="${qualification_example_binaries[v06_index_resource_qualification]}"
      endpoint="${public_endpoint}"
      expected_sources=1
      single_start="$(qualification_log_cursor)"
      ;;
    three)
      binary="${qualification_binaries[v06_index_resource_qualification]}"
      endpoint="${public_endpoints[0]}"
      expected_sources=3
      for node in anvil-1 anvil-2 anvil-3; do
        starts["${node}"]="$(log_cursor)"
      done
      ;;
    *)
      echo "unsupported scale-probe topology ${topology}" >&2
      return 1
      ;;
  esac
  if [[ -e "${probe_report}" || -e "${proof_report}" ]]; then
    echo "scale-probe output already exists" >&2
    return 1
  fi
  assert_source_tree_exact
  if ANVIL_V06_RESOURCE_ENDPOINTS="${endpoint}" \
    ANVIL_V06_RESOURCE_TENANT="${index_resource_tenant}" \
    ANVIL_V06_RESOURCE_BUCKET="${index_resource_bucket}" \
    ANVIL_V06_RESOURCE_CLIENT_ID="${index_resource_client}" \
    ANVIL_V06_RESOURCE_CLIENT_SECRET="${index_resource_secret}" \
    ANVIL_V09_RESOURCE_SINGLETON_PROBE_STATE_INPUT="${index_resource_state}" \
    ANVIL_V09_RESOURCE_SINGLETON_PROBE_OUTPUT="${probe_report}" \
      "${binary}" >/dev/null
  then
    probe_status=0
  else
    probe_status=$?
  fi

  for attempt in $(seq 1 10); do
    if [[ "${topology}" == "single" ]]; then
      container_logs_since "${single_start}" \
        >"${probe_telemetry_prefix}.log"
    else
      for node in anvil-1 anvil-2 anvil-3; do
        save_log_suffix "${node}" "${starts[${node}]}" \
          "${probe_telemetry_prefix}-${node}.log"
      done
    fi
    if ((probe_status != 0)) \
      || [[ "$(typed_json_terminal_event_count "${probe_telemetry_prefix}")" != "0" ]]
    then
      break
    fi
    sleep 1
  done
  while IFS= read -r file; do
    chmod 0600 "${file}"
  done < <(telemetry_files "${probe_telemetry_prefix}")
  if ((probe_status != 0)); then
    echo "public singleton scale probe failed with status ${probe_status}" >&2
    return "${probe_status}"
  fi
  test -s "${probe_report}"
  chmod 0600 "${probe_report}"
  capture_scale_publication_evidence \
    "${topology}" "${publication_telemetry_prefix}"

  local generation
  local index_id
  local records
  jq -e \
    --arg endpoint "${endpoint}" \
    --arg tenant "${index_resource_tenant}" \
    --arg bucket "${index_resource_bucket}" \
    --argjson expected_sources "${expected_sources}" '
      .schema == "anvil.index-resource-singleton-probe.v1" and
      .endpoint == $endpoint and .tenant == $tenant and .bucket == $bucket and
      .index_name == "records-by-field" and .field == "record_id" and
      .operator == "EQUAL" and .value_json == "0" and
      .expected_path == "records/000000000000.json" and
      .source_count == $expected_sources and .returned_hits == 1 and
      .index_id > 0 and .definition_version > 0 and .generation > 0 and
      .placement_term > 0 and .placement_index > 0 and .object_version > 0 and
      .started_at_unix_millis <= .completed_at_unix_millis and
      .elapsed_milliseconds >= 0
    ' "${probe_report}" >/dev/null
  index_id="$(jq -er '.index_id' "${probe_report}")"
  generation="$(jq -er '.generation' "${probe_report}")"
  records="$(jq -er '.records' "${index_resource_report}")"

  local query_ops
  local query_bytes
  local publication_segments
  local read_ops_ceiling
  read -r query_ops query_bytes \
    < <(isolated_typed_json_query_work "${probe_telemetry_prefix}" "${index_id}")
  publication_segments="$(matching_publication_segment_count \
    "${publication_telemetry_prefix}" "${index_id}" "${generation}")"
  # This exact, single-valued equality probe uses at most six component
  # streams per segment: term, posting, live-mask, exact-value verification,
  # identity, and stored fields. Each stream traverses no more than the eight
  # routing levels plus one leaf allowed by the v4 format. The qualification
  # deliberately rounds the derived 54 reads per segment up to 64 and reserves
  # another 64 reads of fixed headroom rather than asserting a brittle minimum.
  read_ops_ceiling=$((64 * (publication_segments + 1)))
  if ((query_ops > read_ops_ceiling)); then
    echo "singleton TypedJson query used ${query_ops} reads; format ceiling is ${read_ops_ceiling}" >&2
    return 1
  fi

  jq -n \
    --arg schema anvil.index-scale-singleton-query-proof.v2 \
    --arg source_commit "${source_commit}" \
    --argjson records "${records}" \
    --argjson expected_sources "${expected_sources}" \
    --argjson publication_segments "${publication_segments}" \
    --argjson query_ops "${query_ops}" \
    --argjson query_bytes "${query_bytes}" \
    --argjson read_ops_ceiling "${read_ops_ceiling}" \
    --slurpfile client "${probe_report}" '
      {
        schema: $schema,
        source_commit: $source_commit,
        corpus_records: $records,
        client_report: $client[0],
        freshness: {expected_sources: $expected_sources, client_validated_complete_zero_lag: true},
        server_telemetry: {
          isolated_terminal_events: 1,
          generation_publication_events: 1,
          publication_segments: $publication_segments,
          query_read_ops: $query_ops,
          query_read_bytes: $query_bytes,
          read_ops_ceiling: $read_ops_ceiling
        },
        format_bound: {
          maximum_routing_height: 8,
          exact_query_component_streams_per_segment: 6,
          maximum_artifact_reads_per_component_stream: 9,
          statically_derived_reads_per_segment: 54,
          qualification_ceiling_reads_per_segment: 64,
          fixed_headroom_reads: 64,
          formula: "64 * (publication_segments + 1)",
          scope: "exact single-valued TypedJson equality probe with one materialized hit",
          source_locations: [
            "crates/anvil-index/src/v4/model.rs::INDEX_ROUTING_HEIGHT",
            "crates/anvil-index/src/v4/reader.rs::ComponentStream::next_leaf",
            "crates/anvil-index/src/v4/executor/plan.rs::resolve_exact",
            "crates/anvil-index/src/v4/executor/posting.rs::PostingStream",
            "crates/anvil-index/src/v4/executor/execute.rs::SegmentExecution::next_unranked",
            "crates/anvil-index/src/v4/executor/execute.rs::SegmentExecution::materialize",
            "crates/anvil-index/src/v4/executor/values.rs::SegmentValues",
            "crates/anvil-index/src/v4/io.rs::read_exact_at",
            "crates/anvil/src/index_runtime/cache.rs::IndexFile::read_at",
            "crates/anvil/src/index_runtime/local_query.rs::QueryObservedFile::read_at"
          ]
        },
        result: "pass"
      }
    ' >"${proof_report}"
  chmod 0600 "${proof_report}"
  echo "[anvil-qualification] singleton query stayed within ${query_ops}/${read_ops_ceiling} format-bounded reads"
  echo "[anvil-qualification] preserved singleton query proof ${proof_report}"
}

run_scale_baseline_resource_qualification() {
  local topology="$1"
  local exact_bucket="${index_resource_bucket}"
  local exact_client="${index_resource_client}"
  local exact_records="${index_resource_records}"
  local exact_report="${index_resource_report}"
  local exact_scope="${index_resource_scope}"
  local exact_secret="${index_resource_secret}"
  local exact_state="${index_resource_state}"
  local exact_telemetry="${index_resource_telemetry_prefix}"
  local exact_tenant="${index_resource_tenant}"
  local exact_targets="${require_performance_targets}"

  scale_baseline_resource_report="/var/tmp/anvil-v090-${topology}-index-scale-baseline-${qualification_suffix}.json"
  scale_baseline_telemetry_prefix="/var/tmp/anvil-v090-${topology}-index-scale-baseline-telemetry-${qualification_suffix}"
  scale_baseline_probe_report="/var/tmp/anvil-v090-${topology}-index-scale-baseline-singleton-${qualification_suffix}.json"
  scale_baseline_probe_telemetry_prefix="/var/tmp/anvil-v090-${topology}-index-scale-baseline-singleton-telemetry-${qualification_suffix}"
  scale_baseline_probe_proof="/var/tmp/anvil-v090-${topology}-index-scale-baseline-singleton-proof-${qualification_suffix}.json"
  scale_comparison_report="/var/tmp/anvil-v090-${topology}-index-scale-comparison-${qualification_suffix}.json"
  index_resource_bucket="index-scale-baseline-${qualification_suffix}"
  index_resource_client="${scale_baseline_resource_client}"
  index_resource_records=16384
  index_resource_report="${scale_baseline_resource_report}"
  index_resource_scope=scale-baseline
  index_resource_secret="${scale_baseline_resource_secret}"
  index_resource_state="${ANVIL_QUALIFICATION_STATE_DIR}/index-scale-baseline-state.json"
  index_resource_telemetry_prefix="${scale_baseline_telemetry_prefix}"
  index_resource_tenant="${scale_baseline_resource_tenant}"
  require_performance_targets=0
  run_index_resource_qualification
  write_incident_query_evidence \
    "${topology}" \
    "${index_resource_report}" \
    "${index_resource_telemetry_prefix}" \
    "${index_resource_report%.json}-incident-evidence.json"
  assert_index_resource_bounds
  assert_typed_json_full_pool_projection \
    "${index_resource_telemetry_prefix}" "${index_rayon_workers}"
  run_scale_singleton_probe "${topology}" \
    "${scale_baseline_probe_report}" \
    "${scale_baseline_probe_telemetry_prefix}" \
    "${scale_baseline_probe_proof}"

  index_resource_bucket="${exact_bucket}"
  index_resource_client="${exact_client}"
  index_resource_records="${exact_records}"
  index_resource_report="${exact_report}"
  index_resource_scope="${exact_scope}"
  index_resource_secret="${exact_secret}"
  index_resource_state="${exact_state}"
  index_resource_telemetry_prefix="${exact_telemetry}"
  index_resource_tenant="${exact_tenant}"
  require_performance_targets="${exact_targets}"
}

configure_three_node_resource_qualification() {
  index_resource_secret=qualification-index-resource-secret-000000000000000000
  index_resource_tenant=qindex-resource
  index_resource_client=qindex-resource-client
  provision_tenant "${index_resource_tenant}" \
    "${index_resource_client}" "${index_resource_secret}"
  if [[ "${qualification_mode}" == "release" ]]; then
    scale_baseline_resource_tenant=qindex-scale
    scale_baseline_resource_client=qindex-scale-client
    scale_baseline_resource_secret=qualification-index-scale-secret-00000000000000000000
    provision_tenant "${scale_baseline_resource_tenant}" \
      "${scale_baseline_resource_client}" "${scale_baseline_resource_secret}"
  fi
}

run_exact_resource_scale_qualification() {
  local topology="$1"
  run_index_resource_qualification
  write_incident_query_evidence \
    "${topology}" \
    "${index_resource_report}" \
    "${index_resource_telemetry_prefix}" \
    "${index_resource_report%.json}-incident-evidence.json"
  assert_index_resource_bounds
  assert_typed_json_full_pool_projection \
    "${index_resource_telemetry_prefix}" "${index_rayon_workers}"
  if [[ "${qualification_mode}" != "release" ]]; then
    return 0
  fi
  index_resource_probe_report="/var/tmp/anvil-v090-${topology}-index-scale-exact-singleton-${qualification_suffix}.json"
  index_resource_probe_telemetry_prefix="/var/tmp/anvil-v090-${topology}-index-scale-exact-singleton-telemetry-${qualification_suffix}"
  index_resource_probe_proof="/var/tmp/anvil-v090-${topology}-index-scale-exact-singleton-proof-${qualification_suffix}.json"
  run_scale_singleton_probe "${topology}" \
    "${index_resource_probe_report}" \
    "${index_resource_probe_telemetry_prefix}" \
    "${index_resource_probe_proof}"
  if [[ -n "${scale_baseline_resource_report:-}" ]]; then
    write_scale_comparison_report
  fi
}

write_scale_comparison_report() {
  local small_report="${scale_baseline_resource_report}"
  local large_report="${index_resource_report}"
  local small_records
  local large_records
  local small_debt_segments small_debt_bytes small_debt_samples
  local large_debt_segments large_debt_bytes large_debt_samples

  jq -e --slurp '
    .[0].records == 16384 and
    .[1].records == 839980 and
    .[0].evidence.resource_configuration == .[1].evidence.resource_configuration and
    (.[0].observed_peak_anonymous_growth_bytes <= .[0].max_anonymous_growth_bytes) and
    (.[1].observed_peak_anonymous_growth_bytes <= .[1].max_anonymous_growth_bytes) and
    .[0].evidence.correctness.resource_limits_passed == true and
    .[1].evidence.correctness.resource_limits_passed == true
  ' "${small_report}" "${large_report}" >/dev/null
  small_records="$(jq -r '.records' "${small_report}")"
  large_records="$(jq -r '.records' "${large_report}")"
  read -r small_debt_segments small_debt_bytes small_debt_samples \
    < <(terminal_typed_json_debt "${scale_baseline_telemetry_prefix}")
  read -r large_debt_segments large_debt_bytes large_debt_samples \
    < <(terminal_typed_json_debt "${index_resource_telemetry_prefix}")
  if ((small_debt_segments != large_debt_segments || small_debt_bytes != large_debt_bytes)); then
    echo "small and exact scale runs used different TypedJson debt limits" >&2
    return 1
  fi
  jq -n \
    --arg schema anvil.index-scale-comparison.v2 \
    --argjson small_records "${small_records}" \
    --argjson large_records "${large_records}" \
    --argjson debt_segment_limit "${large_debt_segments}" \
    --argjson debt_byte_limit "${large_debt_bytes}" \
    --argjson small_debt_samples "${small_debt_samples}" \
    --argjson large_debt_samples "${large_debt_samples}" \
    --slurpfile small "${small_report}" \
    --slurpfile large "${large_report}" \
    --slurpfile small_probe "${scale_baseline_probe_proof}" \
    --slurpfile large_probe "${index_resource_probe_proof}" '
      {
        schema: $schema,
        configured_builder_memory_bytes_per_kind:
          $large[0].evidence.resource_configuration.builder_memory_bytes_per_kind_per_node,
        configured_maximum_anonymous_growth_bytes:
          $large[0].evidence.resource_configuration.maximum_anonymous_growth_bytes,
        terminal_compaction_debt: {
          segment_limit: $debt_segment_limit,
          byte_limit: $debt_byte_limit,
          small_samples: $small_debt_samples,
          large_samples: $large_debt_samples,
          small_debt: 0,
          large_debt: 0
        },
        equality_query_work: {
          proof_model: $small_probe[0].format_bound,
          small: {
            records: $small_records,
            publication_segments: $small_probe[0].server_telemetry.publication_segments,
            read_ops: $small_probe[0].server_telemetry.query_read_ops,
            read_bytes: $small_probe[0].server_telemetry.query_read_bytes,
            read_ops_ceiling: $small_probe[0].server_telemetry.read_ops_ceiling
          },
          large: {
            records: $large_records,
            publication_segments: $large_probe[0].server_telemetry.publication_segments,
            read_ops: $large_probe[0].server_telemetry.query_read_ops,
            read_bytes: $large_probe[0].server_telemetry.query_read_bytes,
            read_ops_ceiling: $large_probe[0].server_telemetry.read_ops_ceiling
          },
          bounded_by_current_segment_count_not_corpus_objects: true
        },
        proven_clauses: [
          "same configured memory ceilings",
          "zero terminal debt under identical debt limits",
          "one isolated exact-match public query per corpus",
          "query reads bounded by immutable current segments rather than corpus leaf count"
        ],
        result: "pass"
      }
    | select(
        $small_probe[0].result == "pass" and $large_probe[0].result == "pass" and
        $small_probe[0].corpus_records == $small_records and
        $large_probe[0].corpus_records == $large_records and
        $small_probe[0].format_bound == $large_probe[0].format_bound and
        $small_probe[0].server_telemetry.query_read_ops <=
          $small_probe[0].server_telemetry.read_ops_ceiling and
        $large_probe[0].server_telemetry.query_read_ops <=
          $large_probe[0].server_telemetry.read_ops_ceiling
      )
    ' >"${scale_comparison_report}"
  test -s "${scale_comparison_report}"
  chmod 0600 "${scale_comparison_report}"
  echo "[anvil-qualification] preserved bounded scale comparison ${scale_comparison_report}"
}
