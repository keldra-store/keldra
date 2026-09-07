#!/usr/bin/env bash
set -euo pipefail

command -v rg >/dev/null 2>&1 || { echo "ripgrep (rg) is required for this gate" >&2; exit 2; }

# Keldra must not depend on an external relational metadata store.
# Build the matcher from fragments so this checker can scan the whole repo
# without matching its own source.
db_a='post''gres'
db_b='post''gresql'
db_c='pg''vector'
db_d='s''qlx'
db_e='tokio-''post''gres'
db_f='deadpool-''post''gres'
db_g='DATABASE''_URL'
db_h='POST''GRES'
pattern="${db_a}|${db_b}|${db_c}|${db_d}|${db_e}|${db_f}|${db_g}|${db_h}"

matches="$(
  while IFS= read -r -d '' tracked_file; do
    # Gitlinks are tracked entries but are not files in this worktree.
    [[ -f "$tracked_file" ]] || continue
    if rg --quiet --ignore-case -- "$pattern" "$tracked_file"; then
      printf '%s\n' "$tracked_file"
    fi
  done < <(git ls-files -z -- . ':(exclude)docs/**')
)"
if [[ -n "$matches" ]]; then
  echo "External relational metadata-store reference found in tracked source; Keldra must be self-contained." >&2
  printf '%s\n' "$matches" >&2
  exit 1
fi
