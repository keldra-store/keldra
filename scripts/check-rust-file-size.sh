#!/usr/bin/env bash
set -euo pipefail

readonly max_lines=2000
status=0

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

while IFS= read -r -d '' file; do
  [[ -f "${file}" ]] || continue

  lines="$(awk 'END { print NR }' "${file}")"
  if ((lines > max_lines)); then
    printf '%s: %s lines (maximum %s)\n' "${file}" "${lines}" "${max_lines}" >&2
    status=1
  fi
done < <(git ls-files --cached --others --exclude-standard -z -- '*.rs')

if ((status != 0)); then
  echo "Rust source files must be split before they exceed ${max_lines} lines." >&2
  exit "${status}"
fi

echo "All Rust source files are at or below ${max_lines} lines."
