#!/usr/bin/env bash

# Shared parsing and preservation for the single-node and three-node release
# qualification logs. Callers enable their own strict shell mode.

strip_ansi() {
  LC_ALL=C sed $'s/\033\\[[0-9;?]*[ -\\/]*[@-~]//g'
}

log_unsigned_field() {
  local field="$1"
  local line="$2"
  local escaped_field="${field//./\\.}"
  if [[ "${line}" =~ (^|[[:space:]])${escaped_field}=([0-9]+)($|[[:space:]]) ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

normalize_unsigned_decimal() {
  local value="$1"
  while ((${#value} > 1)) && [[ "${value}" == 0* ]]; do
    value="${value#0}"
  done
  printf '%s\n' "${value}"
}

unsigned_decimal_less_than() {
  local left
  local right
  left="$(normalize_unsigned_decimal "$1")"
  right="$(normalize_unsigned_decimal "$2")"
  if ((${#left} != ${#right})); then
    ((${#left} < ${#right}))
    return
  fi
  [[ "${left}" < "${right}" ]]
}

unsigned_decimal_is_positive() {
  [[ "$1" =~ [1-9] ]]
}

log_number_field() {
  local field="$1"
  local line="$2"
  local escaped_field="${field//./\\.}"
  if [[ "${line}" =~ (^|[[:space:]])${escaped_field}=([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)($|[[:space:]]) ]]; then
    printf '%s\n' "${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

number_is_positive() {
  awk -v value="$1" 'BEGIN { exit !(value + 0 > 0) }'
}

assert_zero_cgroup_oom_samples() {
  local log="$1"
  local label="$2"
  local available
  local field
  local line
  local samples=0
  local value
  while IFS= read -r line; do
    available="$(log_unsigned_field gauge.anvil_cgroup_memory_metrics_available "${line}")" || {
      echo "${label} emitted a malformed cgroup resource sample" >&2
      return 1
    }
    if [[ "${available}" != "1" ]]; then
      echo "${label} reported unavailable cgroup memory metrics" >&2
      return 1
    fi
    for field in \
      gauge.anvil_cgroup_memory_oom_events \
      gauge.anvil_cgroup_memory_oom_kill_events \
      gauge.anvil_cgroup_memory_oom_group_kill_events
    do
      value="$(log_unsigned_field "${field}" "${line}")" || {
        echo "${label} cgroup sample omitted ${field}" >&2
        return 1
      }
      if [[ "${value}" != "0" ]]; then
        echo "${label} reported ${field}=${value}; release qualification requires zero OOM events" >&2
        return 1
      fi
    done
    samples=$((samples + 1))
  done < <(grep -F 'sampled cgroup memory resources' "${log}" || true)
  if ((samples == 0)); then
    echo "${label} emitted no cgroup resource samples" >&2
    return 1
  fi
}

assert_capacity_samples() {
  local log="$1"
  local label="$2"
  local expected_journal_entries="$3"
  local field
  local line
  local value
  line="$(grep -F 'sampled source-journal safety and capacity' "${log}" | tail -n 1 || true)"
  if [[ -z "${line}" ]] \
    || [[ "$(log_unsigned_field gauge.anvil_source_journal_metrics_available "${line}" || true)" != "1" ]]
  then
    echo "${label} emitted no available source-journal capacity sample" >&2
    return 1
  fi
  for field in \
    gauge.anvil_source_journal_retained_entries \
    gauge.anvil_source_journal_retained_bytes \
    gauge.anvil_source_journal_max_entries \
    gauge.anvil_source_journal_max_bytes \
    gauge.anvil_source_journal_index_lag_entries
  do
    log_unsigned_field "${field}" "${line}" >/dev/null || {
      echo "${label} source-journal sample omitted ${field}" >&2
      return 1
    }
  done
  value="$(log_unsigned_field gauge.anvil_source_journal_max_entries "${line}")"
  if [[ "${expected_journal_entries}" != "0" && "${value}" != "${expected_journal_entries}" ]]; then
    echo "${label} used source-journal max entries ${value}, expected ${expected_journal_entries}" >&2
    return 1
  fi

  line="$(grep -F 'sampled mutation receipt capacity' "${log}" | tail -n 1 || true)"
  if [[ -z "${line}" ]] \
    || [[ "$(log_unsigned_field gauge.anvil_mutation_receipt_metrics_available "${line}" || true)" != "1" ]]
  then
    echo "${label} emitted no available mutation-receipt capacity sample" >&2
    return 1
  fi
  for field in \
    gauge.anvil_mutation_receipt_entries \
    gauge.anvil_mutation_receipt_bytes \
    gauge.anvil_mutation_receipt_max_entries \
    gauge.anvil_mutation_receipt_max_bytes
  do
    log_unsigned_field "${field}" "${line}" >/dev/null || {
      echo "${label} mutation-receipt sample omitted ${field}" >&2
      return 1
    }
  done
}

preserve_all_kind_telemetry() {
  local source="$1"
  local topology="$2"
  local suffix="$3"
  local node="${4:-}"
  local destination="/var/tmp/anvil-v080-${topology}-all-kind-telemetry-${suffix}"
  [[ -n "${node}" ]] && destination="${destination}-${node}"
  preserve_qualification_log "${source}" "${destination}.log"
}

preserve_startup_scan_evidence() {
  local destination="$1"
  grep -F 'anvil_startup_scan_evidence' >"${destination}"
  test -s "${destination}"
  chmod 0600 "${destination}"
}

preserve_qualification_log() {
  local source="$1"
  local destination="$2"
  if [[ ! -s "${source}" ]]; then
    echo "qualification log is absent or empty: ${source}" >&2
    return 1
  fi
  install -m 0600 "${source}" "${destination}"
}
