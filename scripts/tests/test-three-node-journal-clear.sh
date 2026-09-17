#!/usr/bin/env bash
set -euo pipefail

# Exercise the production predicate without containers, peer keys or servers.
test_scripts_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${test_scripts_dir}/qualification-log-evidence.sh"
source "${test_scripts_dir}/qualification-three-node-phases.sh"

docker() { echo 'unexpected Docker invocation' >&2; return 99; }
proof_status=0
proof_calls=0
proof_arguments=
prove_cutover_journal_artifact_suffix() {
  proof_calls=$((proof_calls + 1))
  proof_arguments="$*"
  return "${proof_status}"
}

sample() {
  printf '%s\n' "gauge.keldra_source_journal_tail=5000 gauge.keldra_source_journal_settled_through=${2:-5000} gauge.keldra_source_journal_reference_safe_through=${3:-5000} gauge.keldra_source_journal_index_safe_through=$1 gauge.keldra_source_journal_accounting_safe_through=${4:-5000} gauge.keldra_source_journal_retained_entries=${5:-4097} gauge.keldra_source_journal_max_entries=${6:-4097}"
}

expect_pass() {
  if ! source_journal_sample_is_clear_at_bound "$1" 4097 keldra-2; then
    echo "expected predicate success: $2" >&2
    exit 1
  fi
}

expect_fail() {
  if source_journal_sample_is_clear_at_bound "$1" 4097 keldra-2; then
    echo "expected predicate rejection: $2" >&2
    exit 1
  fi
}

expect_pass "$(sample 5000)" 'exact clear cut'
expect_pass "$(sample 4900)" 'complete artifact-only suffix proof'
[[ "${proof_arguments}" == 'keldra-2 4900 5000' ]]
proof_status=1
expect_fail "$(sample 4900)" 'user suffix or failed proof'
expect_fail "$(sample 4999)" 'even a one-entry gap requires proof'
proof_status=0

# Missing or inconsistent metrics must fail BEFORE invoking the suffix proof.
previous_calls="${proof_calls}"
expect_fail '' 'empty metrics'
expect_fail "$(sample 5001)" 'index safe-through beyond tail'
expect_fail "$(sample 4900 4999)" 'unsettled tail'
expect_fail "$(sample 4900 5000 4999)" 'reference delivery incomplete'
expect_fail "$(sample 4900 5000 5000 4999)" 'accounting incomplete'
expect_fail "$(sample 4900 5000 5000 5000 4098)" 'retained entry overrun'
expect_fail "$(sample 4900 5000 5000 5000 4096)" 'retained entries below exact bound'
expect_fail "$(sample 4900 5000 5000 5000 4097 4098)" 'different configured bound'
complete_sample="$(sample 4900)"
for field in tail settled_through reference_safe_through index_safe_through accounting_safe_through retained_entries max_entries; do
  value="$(log_unsigned_field "gauge.keldra_source_journal_${field}" "${complete_sample}")"
  expect_fail "${complete_sample//gauge.keldra_source_journal_${field}=${value}/}" "missing ${field}"
done
[[ "${proof_calls}" == "${previous_calls}" ]]

# Exercise the actual production wait with a deterministic clock. Unsetting
# Bash's special SECONDS variable in this isolated subshell makes it an ordinary
# test counter; the production script continues using its real elapsed clock.
(
  unset SECONDS
  SECONDS=0
  joining_node_handoff_timeout_seconds=80
  proof_status=0
  sleep() { SECONDS=$((SECONDS + 1)); }
  latest_source_journal_sample() {
    if ((SECONDS < 46)); then
      sample 5000 4999
    else
      printf '%s sample_time=%s\n' "$(sample 4900)" "$SECONDS"
    fi
  }
  refresh_no_event_membership_cutover_tail keldra-2 4097 >/dev/null
  [[ "$SECONDS" -eq 47 && "$membership_cutover_source_tail" -eq 5000 ]]

  # Two polls of the identical sampler line are NOT two distinct samples.
  SECONDS=0
  joining_node_handoff_timeout_seconds=5
  latest_source_journal_sample() { sample 5000; }
  if refresh_no_event_membership_cutover_tail keldra-2 4097 >/dev/null 2>&1; then
    echo 'identical samples incorrectly passed stable-clear wait' >&2
    exit 1
  fi
  [[ "$SECONDS" -eq 5 ]]

  # A genuinely uncleared journal still reaches the bounded failure deadline.
  SECONDS=0
  joining_node_handoff_timeout_seconds=4
  latest_source_journal_sample() { sample 5000 4999; }
  if refresh_no_event_membership_cutover_tail keldra-2 4097 >/dev/null 2>&1; then
    echo 'uncleared journal incorrectly passed bounded wait' >&2
    exit 1
  fi
  [[ "$SECONDS" -eq 4 ]]

  # Distinct clear-looking samples cannot replace the full suffix proof.
  SECONDS=0
  proof_status=1
  latest_source_journal_sample() {
    printf '%s sample_time=%s\n' "$(sample 4900)" "$SECONDS"
  }
  if refresh_no_event_membership_cutover_tail keldra-2 4097 >/dev/null 2>&1; then
    echo 'failed suffix proof incorrectly passed stable-clear wait' >&2
    exit 1
  fi
  [[ "$SECONDS" -eq 4 ]]

  # The initial pressure wait uses the same extended elapsed budget, before
  # recording its existing membership fence and no-event cutover baseline.
  SECONDS=0
  joining_node_handoff_timeout_seconds=80
  proof_status=0
  run_cutover_writes() { :; }
  latest_source_journal_sample() {
    if ((SECONDS < 46)); then
      sample 5000 4999
    else
      printf '%s sample_time=%s\n' "$(sample 4900)" "$SECONDS"
    fi
  }
  latest_completed_membership_fence() { echo 'membership.term=1 membership.index=12'; }
  log_cursor() { echo 'test-clock'; }
  prepare_no_event_membership_cutover_qualification \
    keldra-2 2 test-client unused-secret test-tenant test-bucket 4097 >/dev/null
  [[ "$SECONDS" -eq 47 && "$membership_cutover_source_tail" -eq 5000 ]]
  [[ "$membership_cutover_source_fence_term" -eq 1 && "$membership_cutover_source_fence_index" -eq 12 ]]
)
echo 'three-node source-journal clear predicate: passed'
