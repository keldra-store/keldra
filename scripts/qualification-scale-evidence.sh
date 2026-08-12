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

matching_publication_run_count() {
  local prefix="$1"
  local expected_index_id="$2"
  local expected_generation="$3"
  local count=0
  local file
  local generation
  local index_id
  local line
  local runs=0
  while IFS= read -r file; do
    while IFS= read -r line; do
      index_id="$(log_span_unsigned_field index.id "${line}")" || continue
      generation="$(log_span_unsigned_field generation "${line}")" || continue
      [[ "${index_id}" == "${expected_index_id}" \
        && "${generation}" == "${expected_generation}" ]] || continue
      runs="$(log_span_unsigned_field publication.runs "${line}")" || return 1
      count=$((count + 1))
    done < <(
      awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index generation publication metrics")
      ' "${file}"
    )
  done < <(telemetry_files "${prefix}")
  if ((count != 1 || runs == 0)); then
    echo "TypedJson probe generation matched ${count} publication records; expected exactly one" >&2
    return 1
  fi
  printf '%s\n' "${runs}"
}

terminal_typed_json_debt() {
  local prefix="$1"
  local byte_limit=0
  local debt_bytes
  local debt_levels
  local debt_runs
  local evidence=0
  local file
  local line
  local observed_byte_limit
  local observed_run_limit
  local run_limit=0
  while IFS= read -r file; do
    line="$(awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index compaction debt observed") { line = $0 }
        END { print line }
      ' "${file}")"
    [[ -n "${line}" ]] || continue
    debt_levels="$(log_unsigned_field gauge.anvil_index_compaction_debt_levels "${line}")" \
      || return 1
    debt_runs="$(log_unsigned_field gauge.anvil_index_compaction_debt_runs "${line}")" \
      || return 1
    debt_bytes="$(log_unsigned_field gauge.anvil_index_compaction_debt_bytes "${line}")" \
      || return 1
    observed_run_limit="$(log_unsigned_field gauge.anvil_index_compaction_debt_run_limit "${line}")" \
      || return 1
    observed_byte_limit="$(log_unsigned_field gauge.anvil_index_compaction_debt_byte_limit "${line}")" \
      || return 1
    if ((debt_levels != 0 || debt_runs != 0 || debt_bytes != 0)); then
      echo "TypedJson scale run ended with compaction debt" >&2
      printf '%s\n' "${line}" >&2
      return 1
    fi
    if ((evidence != 0)) \
      && ((run_limit != observed_run_limit || byte_limit != observed_byte_limit)); then
      echo "TypedJson builders reported inconsistent debt limits" >&2
      return 1
    fi
    run_limit="${observed_run_limit}"
    byte_limit="${observed_byte_limit}"
    evidence=$((evidence + 1))
  done < <(telemetry_files "${prefix}")
  if ((evidence == 0 || run_limit == 0 || byte_limit == 0)); then
    echo "TypedJson scale run emitted no terminal debt evidence" >&2
    return 1
  fi
  printf '%s %s %s\n' "${run_limit}" "${byte_limit}" "${evidence}"
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

capture_scale_publication_evidence() {
  local topology="$1"
  local prefix="$2"
  local node
  if [[ "${topology}" == "single" ]]; then
    container_logs | awk '
      index($0, "index.kind=TypedJson") &&
      index($0, "index generation publication metrics")
    ' >"${prefix}.log"
  else
    for node in anvil-1 anvil-2 anvil-3; do
      service_logs "${node}" | awk '
        index($0, "index.kind=TypedJson") &&
        index($0, "index generation publication metrics")
      ' >"${prefix}-${node}.log"
    done
  fi
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
      single_start="$({ container_logs || true; } | wc -l)"
      ;;
    three)
      binary="${qualification_binaries[v06_index_resource_qualification]}"
      endpoint="${public_endpoints[0]}"
      expected_sources=3
      for node in anvil-1 anvil-2 anvil-3; do
        starts["${node}"]="$(log_line_count "${node}")"
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
    ANVIL_V08_RESOURCE_SINGLETON_PROBE_STATE_INPUT="${index_resource_state}" \
    ANVIL_V08_RESOURCE_SINGLETON_PROBE_OUTPUT="${probe_report}" \
      "${binary}" >/dev/null
  then
    probe_status=0
  else
    probe_status=$?
  fi

  for attempt in $(seq 1 10); do
    if [[ "${topology}" == "single" ]]; then
      container_logs | tail -n "+$((single_start + 1))" \
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
  local publication_runs
  local read_ops_ceiling
  read -r query_ops query_bytes \
    < <(isolated_typed_json_query_work "${probe_telemetry_prefix}" "${index_id}")
  publication_runs="$(matching_publication_run_count \
    "${publication_telemetry_prefix}" "${index_id}" "${generation}")"
  read_ops_ceiling=$((39 * publication_runs + 36))
  if ((query_ops > read_ops_ceiling)); then
    echo "singleton TypedJson query used ${query_ops} reads; format ceiling is ${read_ops_ceiling}" >&2
    return 1
  fi

  jq -n \
    --arg schema anvil.index-scale-singleton-query-proof.v1 \
    --arg source_commit "${source_commit}" \
    --argjson records "${records}" \
    --argjson expected_sources "${expected_sources}" \
    --argjson publication_runs "${publication_runs}" \
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
          publication_runs: $publication_runs,
          query_read_ops: $query_ops,
          query_read_bytes: $query_bytes,
          read_ops_ceiling: $read_ops_ceiling
        },
        format_bound: {
          maximum_routing_height: 8,
          root_reads_per_run: 3,
          posting_tree_reads_per_run: 18,
          latest_path_tree_reads_per_run: 18,
          selected_typed_row_reads: 18,
          selected_document_reads: 18,
          formula: "39 * publication_runs + 36",
          source_locations: [
            "crates/anvil-index/src/model.rs::MAX_INDEX_ROUTING_HEIGHT",
            "crates/anvil-index/src/codec.rs::read_component_file",
            "crates/anvil-index/src/run.rs::read_descriptor_bytes/find_leaf/LeafCursor::in_range",
            "crates/anvil-index/src/typed_json/query.rs",
            "crates/anvil-index/src/segment.rs::LatestLiveProbe/latest_path_change/document_by_ordinal"
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

  scale_baseline_resource_report="/var/tmp/anvil-v080-${topology}-index-scale-baseline-${qualification_suffix}.json"
  scale_baseline_telemetry_prefix="/var/tmp/anvil-v080-${topology}-index-scale-baseline-telemetry-${qualification_suffix}"
  scale_baseline_probe_report="/var/tmp/anvil-v080-${topology}-index-scale-baseline-singleton-${qualification_suffix}.json"
  scale_baseline_probe_telemetry_prefix="/var/tmp/anvil-v080-${topology}-index-scale-baseline-singleton-telemetry-${qualification_suffix}"
  scale_baseline_probe_proof="/var/tmp/anvil-v080-${topology}-index-scale-baseline-singleton-proof-${qualification_suffix}.json"
  scale_comparison_report="/var/tmp/anvil-v080-${topology}-index-scale-comparison-${qualification_suffix}.json"
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
  assert_index_resource_bounds
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
  assert_index_resource_bounds
  if [[ "${qualification_mode}" != "release" ]]; then
    return 0
  fi
  index_resource_probe_report="/var/tmp/anvil-v080-${topology}-index-scale-exact-singleton-${qualification_suffix}.json"
  index_resource_probe_telemetry_prefix="/var/tmp/anvil-v080-${topology}-index-scale-exact-singleton-telemetry-${qualification_suffix}"
  index_resource_probe_proof="/var/tmp/anvil-v080-${topology}-index-scale-exact-singleton-proof-${qualification_suffix}.json"
  run_scale_singleton_probe "${topology}" \
    "${index_resource_probe_report}" \
    "${index_resource_probe_telemetry_prefix}" \
    "${index_resource_probe_proof}"
  write_scale_comparison_report
}

write_scale_comparison_report() {
  local small_report="${scale_baseline_resource_report}"
  local large_report="${index_resource_report}"
  local small_records
  local large_records
  local small_debt_runs small_debt_bytes small_debt_samples
  local large_debt_runs large_debt_bytes large_debt_samples

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
  read -r small_debt_runs small_debt_bytes small_debt_samples \
    < <(terminal_typed_json_debt "${scale_baseline_telemetry_prefix}")
  read -r large_debt_runs large_debt_bytes large_debt_samples \
    < <(terminal_typed_json_debt "${index_resource_telemetry_prefix}")
  if ((small_debt_runs != large_debt_runs || small_debt_bytes != large_debt_bytes)); then
    echo "small and exact scale runs used different TypedJson debt limits" >&2
    return 1
  fi
  jq -n \
    --arg schema anvil.index-scale-comparison.v1 \
    --argjson small_records "${small_records}" \
    --argjson large_records "${large_records}" \
    --argjson debt_run_limit "${large_debt_runs}" \
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
          run_limit: $debt_run_limit,
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
            publication_runs: $small_probe[0].server_telemetry.publication_runs,
            read_ops: $small_probe[0].server_telemetry.query_read_ops,
            read_bytes: $small_probe[0].server_telemetry.query_read_bytes,
            read_ops_ceiling: $small_probe[0].server_telemetry.read_ops_ceiling
          },
          large: {
            records: $large_records,
            publication_runs: $large_probe[0].server_telemetry.publication_runs,
            read_ops: $large_probe[0].server_telemetry.query_read_ops,
            read_bytes: $large_probe[0].server_telemetry.query_read_bytes,
            read_ops_ceiling: $large_probe[0].server_telemetry.read_ops_ceiling
          },
          bounded_by_current_run_count_not_corpus_objects: true
        },
        proven_clauses: [
          "same configured memory ceilings",
          "zero terminal debt under identical debt limits",
          "one isolated exact-match public query per corpus",
          "query reads bounded by immutable current runs rather than corpus leaf count"
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
