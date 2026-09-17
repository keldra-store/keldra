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
proof_epoch='[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]'
proof_term=1
proof_fence_index=12
proof_mode=valid
prove_cutover_journal_artifact_suffix() {
  proof_calls=$((proof_calls + 1))
  proof_arguments="$*"
  ((proof_status == 0)) || return "${proof_status}"
  local start="$2" tail="$3" encoded_start="$2" proof
  [[ "${proof_mode}" == missing ]] && return 0
  [[ "${proof_mode}" == missed_baseline && "${start}" == 5000 ]] && encoded_start=$((start + 1))
  proof="$(jq --null-input --compact-output --argjson epoch "${proof_epoch}" \
    --argjson start "${encoded_start}" --argjson tail "${tail}" \
    --argjson term "${proof_term}" --argjson fence "${proof_fence_index}" '{
      proof:"complete_reserved_derived_artifact_suffix", all_records_are_reserved_derived_artifacts:true,
      source_node_id:2, source_epoch:$epoch, index_safe_through:$start, stable_tail:$tail,
      settled_through:$tail, captured_fence:{term:$term,index:$fence},
      retention_floor_before:0, retention_floor_after:0, verified_records:($tail-$start)}')"
  validate_cutover_journal_artifact_proof "${proof}" 2 "${start}" "${tail}" "${proof_term}" "${proof_fence_index}"
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

# Parsed proof metadata, not success text or a zero subprocess exit, binds the
# authenticated observation to the requested source, fence and full interval.
valid_proof="${cutover_verified_proof}"
for mutation in '.source_node_id = 3' '.captured_fence.index = 13' \
  '.verified_records = 99' '.retention_floor_after = 4901' \
  '.all_records_are_reserved_derived_artifacts = false' '.source_epoch = []'; do
  invalid_proof="$(jq --compact-output "${mutation}" <<<"${valid_proof}")"
  if validate_cutover_journal_artifact_proof "${invalid_proof}" 2 4900 5000 1 12; then
    echo "invalid proof metadata accepted: ${mutation}" >&2
    exit 1
  fi
done
for invalid_proof in '' 'successful complete_reserved_derived_artifact_suffix' '{}'; do
  if validate_cutover_journal_artifact_proof "${invalid_proof}" 2 4900 5000 1 12 2>/dev/null; then
    echo 'missing or malformed JSON proof accepted' >&2
    exit 1
  fi
done

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

# Exercise the REAL no-event cutover function. Only I/O is replaced: the same
# JSON proof validator, interval selection and post-write guard run unchanged.
no_event_case() (
  local mode="$1" expected="$2" candidate_tail=5002 candidate_index=4990 post_sent=0
  KELDRA_QUALIFICATION_DIR=/unused-private-test
  qualification_suffix=test
  membership_cutover_source_tail=5000
  membership_cutover_source_epoch="${proof_epoch}"
  membership_cutover_source_fence_term=1
  membership_cutover_source_fence_index=12
  membership_cutover_source_log_start=test
  proof_fence_index=18
  proof_mode=valid
  proof_status=0
  case "${mode}" in
    ordinary|gapped|pruned|failed) proof_status=1 ;;
    epoch) proof_epoch='[2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]' ;;
    regression) candidate_tail=4999; candidate_index=4999 ;;
    missed_baseline) candidate_index=5002; proof_mode=missed_baseline ;;
    missing) proof_mode=missing; cutover_verified_proof= ;;
    baseline_start) candidate_index=5002 ;;
  esac
  sleep() { :; }
  save_log_suffix() { :; }
  preserve_qualification_log() { :; }
  new_cutover_fence_line() { echo 'membership.term=1 membership.index=18'; }
  sample_after_log_line() {
    local line
    line="$(sample "${candidate_index}")"
    printf '%s\n' "${line//5000/${candidate_tail}}"
  }
  run_cutover_writes() { post_sent=1; }
  latest_source_journal_sample() {
    local next=5003 line
    # Artifact growth BEFORE the explicit write is not proof that the write
    # advanced the journal: its already-verified 5002 tail must not pass again.
    [[ "${mode}" == no_post_advance ]] && next=5002
    line="$(sample 5000)"
    printf '%s\n' "${line//5000/${next}}"
  }
  if qualify_no_event_membership_cutover keldra-2 2 client unused tenant bucket 4097 >/dev/null 2>&1; then
    [[ "${expected}" == pass ]] || { echo "unexpected cutover success: ${mode}" >&2; exit 1; }
    [[ "${post_sent}" == 1 ]]
    if [[ "${mode}" == baseline_start ]]; then
      [[ "${proof_arguments}" == 'keldra-2 5000 5002' ]]
    else
      [[ "${proof_arguments}" == 'keldra-2 4990 5002' ]]
    fi
  else
    [[ "${expected}" == fail ]] || { echo "unexpected cutover failure: ${mode}" >&2; exit 1; }
  fi
)
no_event_case valid pass
no_event_case baseline_start pass
for mode in ordinary gapped pruned failed epoch regression missed_baseline missing no_post_advance; do
  no_event_case "${mode}" fail
done
echo 'three-node source-journal clear predicate: passed'
