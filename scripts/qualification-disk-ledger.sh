#!/usr/bin/env bash
# Shared, non-destructive disk ownership and budget accounting.

qualification_disk_ledger_init() {
  local owner="$1" root="$2" budget="$3" ledger="$4"
  [[ "$owner" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "invalid disk-ledger owner" >&2; return 2; }
  [[ "$root" == /* && -d "$root" ]] || { echo "disk-ledger root must be an existing absolute directory" >&2; return 2; }
  [[ "$budget" =~ ^[1-9][0-9]*$ ]] || { echo "disk budget must be a positive byte count" >&2; return 2; }
  mkdir -p "$(dirname "$ledger")"
  : >"$ledger"
  export KELDRA_DISK_LEDGER_OWNER="$owner"
  export KELDRA_DISK_LEDGER_ROOT="$(readlink -f "$root")"
  export KELDRA_DISK_LEDGER_ROOTS="$KELDRA_DISK_LEDGER_ROOT"
  export KELDRA_DISK_LEDGER_BUDGET_BYTES="$budget"
  export KELDRA_DISK_LEDGER_FILE="$ledger"
  qualification_disk_ledger_event begin run-root "$KELDRA_DISK_LEDGER_ROOT"
  qualification_disk_ledger_check
}

qualification_disk_ledger_add_root() {
  local root="$1" canonical
  [[ "$root" == /* && -e "$root" ]] || { echo "additional disk-ledger root must be an existing absolute path" >&2; return 2; }
  canonical="$(readlink -f "$root")"
  export KELDRA_DISK_LEDGER_ROOTS="${KELDRA_DISK_LEDGER_ROOTS}:${canonical}"
  qualification_disk_ledger_event own run-root "$canonical"
}

qualification_disk_ledger_event() {
  local event="$1" kind="$2" path="$3" canonical
  canonical="$(readlink -m "$path")"
  jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg owner "$KELDRA_DISK_LEDGER_OWNER" --arg event "$event" \
    --arg kind "$kind" --arg path "$canonical" \
    '{schema:"keldra.qualification-disk-ledger.v1",at:$at,owner:$owner,event:$event,kind:$kind,path:$path}' \
    >>"$KELDRA_DISK_LEDGER_FILE"
}

qualification_disk_ledger_check() {
  local used=0 available root root_used
  local -a roots
  IFS=: read -r -a roots <<<"$KELDRA_DISK_LEDGER_ROOTS"
  for root in "${roots[@]}"; do
    root_used="$(du -sb "$root" | awk '{print $1}')"
    used=$((used + root_used))
  done
  available="$(df --output=avail -B1 "$KELDRA_DISK_LEDGER_ROOT" | awk 'NR == 2 {print $1}')"
  jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg owner "$KELDRA_DISK_LEDGER_OWNER" --argjson used "$used" \
    --argjson available "$available" --argjson budget "$KELDRA_DISK_LEDGER_BUDGET_BYTES" \
    '{schema:"keldra.qualification-disk-ledger.v1",at:$at,owner:$owner,event:"sample",used_bytes:$used,filesystem_available_bytes:$available,budget_bytes:$budget}' \
    >>"$KELDRA_DISK_LEDGER_FILE"
  if ((used > KELDRA_DISK_LEDGER_BUDGET_BYTES)); then
    echo "qualification-owned bytes ${used} exceed budget ${KELDRA_DISK_LEDGER_BUDGET_BYTES}" >&2
    return 1
  fi
}

qualification_disk_ledger_finish() {
  local result="$1"
  if [[ -e "$KELDRA_DISK_LEDGER_ROOT" ]]; then
    qualification_disk_ledger_check || true
  fi
  qualification_disk_ledger_event finish "$result" "$KELDRA_DISK_LEDGER_ROOTS"
}
