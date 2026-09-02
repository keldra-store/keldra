#!/usr/bin/env bash

# Shared parsing for active non-index qualification logs. Callers enable their own strict mode.

strip_ansi() {
  LC_ALL=C sed $'s/\033\\[[0-9;?]*[ -\\/]*[@-~]//g'
}

qualification_log_cursor() {
  date --utc +'%s.%N'
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

preserve_qualification_log() {
  local source="$1"
  local destination="$2"
  if [[ ! -s "${source}" ]]; then
    echo "qualification log is absent or empty: ${source}" >&2
    return 1
  fi
  install -m 0600 "${source}" "${destination}"
}
