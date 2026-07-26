#!/usr/bin/env bash
set -euo pipefail

export ANVIL_TEST_LOG="${ANVIL_TEST_LOG:-warn}"
# Timing instrumentation is intentionally opt-in. Enabling it on release gates
# can produce hundreds of thousands of log lines and materially slow Docker E2E.
if [[ -n "${ANVIL_TEST_TIMINGS:-}" ]]; then
  export ANVIL_TEST_TIMINGS
else
  unset ANVIL_TEST_TIMINGS
fi

group="${1:-all}"

run_step() {
  local name="$1"
  shift
  local start
  start="$(date +%s)"
  echo "::group::${name}"
  echo "[anvil-gate] start ${name}"
  local timeout_seconds="${ANVIL_GATE_STEP_TIMEOUT_SECONDS:-1800}"
  local timeout_bin=""
  if [[ -n "${timeout_seconds}" && "${timeout_seconds}" != "0" ]]; then
    timeout_bin="$(command -v timeout || true)"
  fi
  set +e
  if [[ -n "${timeout_bin}" ]]; then
    "${timeout_bin}" --kill-after=30s "${timeout_seconds}s" "$@"
  else
    "$@"
  fi
  local status=$?
  set -e
  local end
  end="$(date +%s)"
  echo "[anvil-gate] finish ${name} status=${status} elapsed=$((end - start))s"
  if [[ "${status}" == "124" ]]; then
    echo "[anvil-gate] timed out ${name} after ${timeout_seconds}s" >&2
  fi
  echo "::endgroup::"
  return "$status"
}

run_cargo_test() {
  local name="$1"
  shift
  local test_threads="${ANVIL_RUST_TEST_THREADS:-4}"
  run_step "$name" cargo test "$@" -- --nocapture --test-threads="${test_threads}"
}

run_docker_cargo_test() {
  local name="$1"
  shift
  local test_threads="${ANVIL_DOCKER_TEST_THREADS:-1}"
  run_step "$name" cargo test "$@" -- --nocapture --test-threads="${test_threads}"
}

run_docker_cargo_test_skipping() {
  local name="$1"
  local skip_filter="$2"
  shift 2
  local test_threads="${ANVIL_DOCKER_TEST_THREADS:-1}"
  run_step "$name" cargo test "$@" -- \
    --nocapture \
    --test-threads="${test_threads}" \
    --skip "${skip_filter}"
}

require_image() {
  local configured_anvil_image="${ANVIL_IMAGE:-anvil:test}"
  export ANVIL_IMAGE="$(./scripts/resolve-docker-image-id.sh "$configured_anvil_image")"
  echo "[anvil-gate] using ANVIL_IMAGE=${ANVIL_IMAGE}"
}

reset_shared_docker_cluster() {
  local project="${ANVIL_DOCKER_TEST_PROJECT:-anvil-shared-test}"
  local compose_file="anvil/tests/docker-compose.test.yml"
  local node_count="${ANVIL_DOCKER_TEST_NODE_COUNT:-6}"
  local node
  for node in $(seq 1 "${node_count}"); do
    export "ANVIL_TEST_NODE${node}_TOKEN=release-gate-reset-token-${node}"
  done
  local state_dir="${TMPDIR:-/tmp}/anvil-test-cluster-locks"
  local project_state="${project//[^a-zA-Z0-9_-]/-}"
  docker compose -p "${project}" -f "${compose_file}" down -v --remove-orphans
  rm -f "${state_dir}/${project_state}.ports"
}

cleanup_docker_gates() {
  reset_shared_docker_cluster
  local image="${ANVIL_IMAGE:-}"
  if [[ -n "${image}" ]] && docker image inspect "${image}" >/dev/null 2>&1; then
    docker image rm "${image}"
  fi
}

static_gates() {
  run_step "no external database gate" ./scripts/check-no-external-db.sh
  run_step "no public unfenced journal writes gate" ./scripts/check-no-public-unfenced-journal-writes.sh
  run_step "documentation hardening gate" ./scripts/check-docs-hardening.sh
  run_step "release notes gate" ./scripts/test-release-notes.sh
  run_step "fission docs check" fission site check --project-dir documentation --release
  run_step "fission docs build" fission site build --project-dir documentation --release
  run_step "Rust client publish dry-run" cargo publish --dry-run -p anvil-storage
}

rust_unit_gates() {
  run_cargo_test "MVCC-under-Raft executable model" -p anvil-mvcc-raft-model --lib --tests
  # Exercise the public command contract before the long-running core suite so
  # CLI/docs drift fails quickly instead of consuming most of the CI timeout.
  run_cargo_test "public CLI non-Docker integration tests" -p anvil-storage-cli \
    --test binary_names \
    --test confy_test \
    --test public_command_surface
  run_cargo_test "public CLI binary/unit tests" -p anvil-storage-cli --bins
  run_cargo_test "Rust client package tests" -p anvil-storage --lib --tests
  run_cargo_test "test utils package tests" -p anvil-storage-test-utils --lib
  run_cargo_test "documentation package tests" -p anvil-documentation --lib --bins
  run_cargo_test "core library tests" -p anvil-storage-core --lib --bins
  run_cargo_test "server library and binary tests" -p anvil-server --lib --bins
}

server_core_integration_gates() {
  local tests=(
    admin_lifecycle
    corestore_conformance
    corestore_conformance_durable_families
    corestore_conformance_rfc0007_byte_pipeline
    corestore_conformance_rfc0007_internal_protocols
    corestore_conformance_rfc0007_journals
    corestore_conformance_rfc0007_payloads
    corestore_conformance_rfc0007_perf
    corestore_conformance_rfc0007_query_e2e_scaffolding
    corestore_conformance_rfc0007_query_planning
    corestore_conformance_rfc0007_roots
    corestore_conformance_rfc0007_transactions
    corestore_conformance_rfc0007_writer_segments
    corestore_source_size
    hardening_static
    performance_tests
  )
  for test_name in "${tests[@]}"; do
    run_cargo_test "server integration ${test_name}" -p anvil-server --test "${test_name}"
  done
}

docker_auth_gates() {
  require_image
  reset_shared_docker_cluster
  run_docker_cargo_test "Docker auth integration auth" -p anvil-server --test auth
  # auth_tests covers several independent authz and tenant-scope suites. Run
  # each module as its own step so CI keeps useful timeout boundaries while
  # still exercising the complete public/admin auth surface.
  run_docker_cargo_test "Docker auth integration auth_tests grpc errors" -p anvil-server --test auth_tests "grpc_error_responses_include_server_request_id"
  # These two asynchronous materialization checks remain available for focused
  # execution but are excluded from the release gate until issue #62 is closed.
  run_docker_cargo_test_skipping \
    "Docker auth integration auth_tests access and tuples" \
    "access_and_tuple::test_authz_tuple_write_check_and_watch" \
    -p anvil-server --test auth_tests "access_and_tuple::"
  run_docker_cargo_test_skipping \
    "Docker auth integration auth_tests leases and object authz" \
    "leases_and_object_authz::test_authz_permission_resolves_nested_usersets" \
    -p anvil-server --test auth_tests "leases_and_object_authz::"
  run_docker_cargo_test "Docker auth integration auth_tests links apps tenant scope" -p anvil-server --test auth_tests "links_apps_and_tenant_scope::"
  run_docker_cargo_test "Docker auth integration auth_tests object lists schemas" -p anvil-server --test auth_tests "object_lists_and_schemas::"
  run_docker_cargo_test "Docker auth integration auth_tests public access secret reset" -p anvil-server --test auth_tests "public_access_and_secret_reset::"
  run_docker_cargo_test "Docker auth integration auth_tests stream authorisation" -p anvil-server --test auth_tests "stream_authorisation::"
  run_docker_cargo_test "Docker CLI auth integration" -p anvil-storage-cli --test cli_auth
}

run_docker_object_test_filter() {
  local label="$1"
  local filter="$2"
  run_docker_cargo_test "Docker storage integration object_tests ${label}" \
    -p anvil-server --test object_tests "${filter}"
}

run_docker_object_test_filter_serial() {
  local label="$1"
  local filter="$2"
  reset_shared_docker_cluster
  ANVIL_DOCKER_TEST_THREADS=1 \
    run_docker_cargo_test "Docker storage integration object_tests ${label}" \
      -p anvil-server --test object_tests "${filter}"
}

run_docker_s3_test_filter() {
  local label="$1"
  local filter="$2"
  run_docker_cargo_test "Docker storage integration s3_gateway_tests ${label}" \
    -p anvil-server --test s3_gateway_tests "${filter}"
}

run_docker_s3_test_filter_serial() {
  local label="$1"
  local filter="$2"
  reset_shared_docker_cluster
  ANVIL_DOCKER_TEST_THREADS=1 \
    run_docker_cargo_test "Docker storage integration s3_gateway_tests ${label}" \
      -p anvil-server --test s3_gateway_tests "${filter}"
}

docker_storage_gates() {
  require_image
  reset_shared_docker_cluster
  local tests=(
    bucket_tests
    rust_client_tests
  )
  for test_name in "${tests[@]}"; do
    run_docker_cargo_test "Docker storage integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  # object_tests is intentionally split by module. The suite exercises the
  # shared Docker cluster, CoreMeta quorum commits and background maintenance;
  # one aggregate process can exceed a per-step timeout while providing no extra
  # coverage beyond the same test set run as fresh filtered processes.
  run_docker_object_test_filter "batch CAS multipart" "batch_cas_multipart::"
  run_docker_object_test_filter "copy private watch stream" "copy_private_watch_stream::"
  # These tests exercise version/delete/read/listing mutations against the
  # shared Docker cluster. Run each as a single-test process so the release gate
  # verifies the same behaviour without depending on incidental intra-binary
  # scheduling between version-state tests.
  run_docker_object_test_filter_serial "native route redirect" "native_delete_listing::native_object_routes_apply_cross_region_policy_before_local_metadata"
  run_docker_object_test_filter_serial "native route proxy" "native_delete_listing::native_object_routes_report_proxy_required_as_unavailable_when_proxy_is_absent"
  run_docker_object_test_filter_serial "native context validation" "native_delete_listing::test_native_mutations_require_valid_context"
  run_docker_object_test_filter_serial "native preconditions" "native_delete_listing::test_native_object_mutation_preconditions_are_enforced"
  run_docker_object_test_filter_serial "native idempotency" "native_delete_listing::test_native_object_mutation_idempotency_replays_without_duplicate_mutation"
  run_docker_object_test_filter_serial "native repair" "native_delete_listing::test_repair_rebuilds_missing_directory_segment_from_metadata_journal"
  run_docker_object_test_filter_serial "native delete marker" "native_delete_listing::test_delete_object_creates_delete_marker"
  run_docker_object_test_filter_serial "native delete version" "native_delete_listing::test_delete_object_specific_version_removes_only_that_version"
  run_docker_object_test_filter_serial "native latest get" "native_delete_listing::test_get_object_without_version_id_returns_latest_version"
  run_docker_object_test_filter_serial "native utf8 keys" "native_delete_listing::test_utf8_object_keys_with_spaces_round_trip"
  run_docker_object_test_filter_serial "native reserved listing" "native_delete_listing::test_listing_omits_reserved_internal_object_keys"
  run_docker_object_test_filter "mesh locator routing" "native_object_routes_use_mesh_locator_before_local_bucket_metadata"
  run_docker_object_test_filter "patch and list" "patch_and_list::"
  run_docker_object_test_filter "planner listing" "planner_listing::"
  run_docker_object_test_filter "reserved head core" "reserved_head_core::"

  # Split S3 gateway tests for the same reason: each module uses the shared
  # Docker cluster and some tests intentionally wait for asynchronous index or
  # compaction workers to catch up.
  reset_shared_docker_cluster
  run_docker_s3_test_filter "public/private large objects" "public_private_large_object::"
  run_docker_s3_test_filter "routing public aliases" "routing_public_alias::"
  run_docker_s3_test_filter "streaming upload" "streaming_upload::"
  # The write/index/compaction tests intentionally drive background workers.
  # Running the whole module in one process creates incidental contention between
  # independent buckets and obscures the actual release signal, so keep the same
  # coverage while exercising one scenario per process.
  run_docker_s3_test_filter_serial "S3 write ETag preconditions" "writes_indexes_compaction::test_s3_put_write_etag_preconditions"
  run_docker_s3_test_filter_serial "S3 version listing authz" "writes_indexes_compaction::test_s3_list_versions_and_get_filter_by_relationship_authorization"
  run_docker_s3_test_filter_serial "S3 compaction reads/lists" "writes_indexes_compaction::test_s3_reads_and_lists_survive_object_metadata_compaction"
  run_docker_s3_test_filter_serial "S3 active get compaction" "writes_indexes_compaction::test_s3_active_get_survives_object_metadata_compaction"
  run_docker_s3_test_filter_serial "S3 worker compaction" "writes_indexes_compaction::test_s3_writes_trigger_worker_metadata_compaction"
  run_docker_s3_test_filter_serial "S3 body full-text index" "writes_indexes_compaction::test_s3_put_triggers_full_text_index_build"
  run_docker_s3_test_filter_serial "S3 metadata full-text index" "writes_indexes_compaction::test_s3_put_metadata_field_triggers_full_text_index_build"
  run_docker_s3_test_filter_serial "S3 PersonalDB full-text index" "writes_indexes_compaction::test_s3_put_personaldb_table_column_triggers_full_text_index_build"
  run_docker_s3_test_filter_serial "S3 media full-text index" "writes_indexes_compaction::test_s3_put_media_transcript_triggers_full_text_index_build"
  run_docker_s3_test_filter_serial "S3 vector index" "writes_indexes_compaction::test_s3_put_triggers_vector_index_build"
  run_docker_cargo_test "Docker CLI storage integration" -p anvil-storage-cli --test cli
}

run_docker_index_test_filter() {
  local label="$1"
  local filter="$2"
  ANVIL_DOCKER_TEST_THREADS="${ANVIL_INDEX_TEST_THREADS:-1}" \
    run_docker_cargo_test "Docker index/data integration index_tests ${label}" \
      -p anvil-server --test index_tests "${filter}"
}

docker_index_gates() {
  require_image
  reset_shared_docker_cluster
  local tests=(
    git_source_tests
    hf_ingestion_e2e
    internal_proxy_tests
  )
  for test_name in "${tests[@]}"; do
    run_docker_cargo_test "Docker index/data integration ${test_name}" -p anvil-server --test "${test_name}"
  done
  echo "[anvil-gate] skipping Docker index/data integration personaldb_tests; tracked by https://github.com/worka-ai/anvil/issues/19"
  ANVIL_DOCKER_TEST_THREADS=1 \
    run_docker_cargo_test "Docker index/data integration hf_ingestion_integration" \
      -p anvil-server --test hf_ingestion_integration

  # index_tests intentionally runs as smaller filters. The tests exercise shared
  # clusters, background index workers and CoreMeta quorum writes; one aggregate
  # process accumulates enough state to make CI timing unreliable while adding no
  # extra coverage beyond the same test set run as fresh filtered processes.
  run_docker_index_test_filter "build_repair" "build_repair::"
  run_docker_index_test_filter "query authz" "query_spec::test_query_index_results_are_filtered_by_zanzibar_object_relationships"
  run_docker_index_test_filter "query inherited usersets" "query_spec::test_query_spec_inherit_object_filter_uses_derived_userset_grants"
  run_docker_index_test_filter "query read-scope intersection" "query_spec::test_query_spec_intersection_filters_inherit_object_hits_by_read_scope"
  run_docker_index_test_filter "query full-text intersection" "query_spec::test_query_spec_intersects_full_text_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query hybrid intersection" "query_spec::test_query_spec_intersects_hybrid_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query vector intersection" "query_spec::test_query_spec_intersects_vector_with_typed_filter_without_bucket_scan"
  run_docker_index_test_filter "query path authz" "query_spec::test_query_spec_path_filter_intersects_authz_before_results"
  run_docker_index_test_filter "query typed json" "query_spec::test_typed_json_index_queries_canonical_object_body_with_range_order_and_page_token"
  # Keep typed lifecycle scenarios as separate filtered processes. The append
  # record scenario is intentionally first because it validates stream-derived
  # typed rows independently of later lifecycle churn in the shared cluster.
  run_docker_index_test_filter "typed append records" "typed_lifecycle::test_typed_json_index_queries_append_record_payloads"
  run_docker_index_test_filter "typed reserved candidates" "typed_lifecycle::test_typed_json_index_omits_reserved_internal_candidates"
  run_docker_index_test_filter "typed definition lifecycle" "typed_lifecycle::test_index_definition_lifecycle"
  run_docker_index_test_filter "typed metadata indexes" "typed_lifecycle::test_query_path_and_metadata_filter_indexes_from_object_metadata"
  run_docker_index_test_filter "typed planner authz" "typed_lifecycle::test_live_metadata_query_uses_planner_authz_candidates_and_scoped_page_tokens"
  run_docker_index_test_filter "validation invalid policy" "validation_diagnostics::test_index_definition_rejects_invalid_policy_shape"
  run_docker_index_test_filter "validation diagnostics list" "validation_diagnostics::test_list_index_diagnostics_filters_by_index_and_severity"
  # The vector/hybrid module exercises multiple independent slow background
  # index-build paths. Keep the same coverage, but avoid one aggregate process
  # accumulating enough state to trip the per-step timeout.
  run_docker_index_test_filter "vector hybrid build hybrid" "vector_hybrid::test_hybrid_index_builds_text_and_vector_segments_from_object_write_task"
  run_docker_index_test_filter "vector hybrid full text latest" "vector_hybrid::test_query_full_text_index_reads_latest_segment"
  run_docker_index_test_filter "vector hybrid phrase positions" "vector_hybrid::test_query_full_text_phrase_requires_position_enabled_index"
  run_docker_index_test_filter "vector hybrid query hybrid" "vector_hybrid::test_query_hybrid_index_combines_full_text_and_vector_segments"
  run_docker_index_test_filter "vector hybrid inherit full text" "vector_hybrid::test_query_inherit_object_full_text_filters_results_by_object_read_scope"
  run_docker_index_test_filter "vector hybrid inherit vector" "vector_hybrid::test_query_inherit_object_vector_filters_results_by_object_read_scope"
  run_docker_index_test_filter "vector hybrid vector latest" "vector_hybrid::test_query_vector_index_reads_latest_segment"
  run_docker_index_test_filter "vector hybrid dimension diagnostic" "vector_hybrid::test_vector_index_build_records_dimension_mismatch_diagnostic"
  run_docker_index_test_filter "vector hybrid build vector" "vector_hybrid::test_vector_index_builds_from_object_write_task"
  run_docker_index_test_filter "vector hybrid media modalities" "vector_hybrid::test_vector_index_builds_required_media_modalities_from_object_write_tasks"

  # CLI extended tests validate independent public-CLI workflows and do not
  # depend on state created by the index tests above. Reset first so a slow or
  # aborted index run cannot leave retained CoreMeta generations that obscure
  # CLI regressions. Run serially because the HF CLI tests share one control
  # stream and authz policy surface.
  reset_shared_docker_cluster
  ANVIL_DOCKER_TEST_THREADS=1 \
    run_docker_cargo_test "Docker CLI extended integration" -p anvil-storage-cli --test cli_extended
}

docker_mesh_gates() {
  require_image
  reset_shared_docker_cluster
  # Each distributed scenario owns an isolated six-node cluster. Keep the
  # modules in separate test processes so the step timeout applies to useful
  # units of work rather than the cumulative startup and fault-injection time
  # of every scenario in the binary.
  local distributed_modules=(
    faults
    metadata
    objects
    publication_recovery
  )
  local module
  for module in "${distributed_modules[@]}"; do
    run_docker_cargo_test "Docker mesh integration distributed_tests ${module}" \
      -p anvil-server --test distributed_tests "${module}::"
  done
  # The remaining binaries share the named Docker cluster. Recreate it between
  # binaries so fault/restart state from one contract cannot leak into another.
  reset_shared_docker_cluster
  run_docker_cargo_test "Docker mesh integration docker_cluster_test" \
    -p anvil-server --test docker_cluster_test
  reset_shared_docker_cluster
  run_docker_cargo_test "Docker mesh integration grpc" \
    -p anvil-server --test grpc
}

performance_quick_gates() {
  run_step "MVCC-under-Raft performance evidence (quick)" \
    ./scripts/run-mvcc-perf-benchmark.sh quick
}

performance_release_gates() {
  local expected_commit="${ANVIL_EXPECTED_GIT_COMMIT:-}"
  if [[ -n "${expected_commit}" ]]; then
    if [[ ! "${expected_commit}" =~ ^([0-9a-fA-F]{40}|[0-9a-fA-F]{64})$ ]]; then
      echo "ANVIL_EXPECTED_GIT_COMMIT must be a full Git object ID" >&2
      return 2
    fi
    run_step "MVCC-under-Raft performance evidence (release)" \
      env GITHUB_SHA="${expected_commit}" ./scripts/run-mvcc-perf-benchmark.sh release
  else
    run_step "MVCC-under-Raft performance evidence (release)" \
      ./scripts/run-mvcc-perf-benchmark.sh release
  fi
}

case "$group" in
  all)
    static_gates
    rust_unit_gates
    server_core_integration_gates
    performance_quick_gates
    docker_auth_gates
    docker_storage_gates
    docker_index_gates
    docker_mesh_gates
    ;;
  static)
    static_gates
    ;;
  rust)
    rust_unit_gates
    ;;
  server-core)
    server_core_integration_gates
    ;;
  perf|perf-quick)
    performance_quick_gates
    ;;
  perf-release)
    performance_release_gates
    ;;
  docker-auth)
    docker_auth_gates
    ;;
  docker-storage)
    docker_storage_gates
    ;;
  docker-index)
    docker_index_gates
    ;;
  docker-mesh)
    docker_mesh_gates
    ;;
  docker-cleanup)
    cleanup_docker_gates
    ;;
  *)
    cat >&2 <<USAGE
usage: $0 [all|static|rust|server-core|perf|perf-quick|perf-release|docker-auth|docker-storage|docker-index|docker-mesh|docker-cleanup]
USAGE
    exit 2
    ;;
esac
