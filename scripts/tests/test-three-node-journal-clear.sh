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
echo 'three-node source-journal clear predicate: passed'
