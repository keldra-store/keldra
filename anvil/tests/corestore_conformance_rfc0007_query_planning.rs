#![recursion_limit = "256"]

use std::fs;
use std::path::Path;

fn repo_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

fn repo_source_files_under(path: &str) -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path);
    let mut pending = vec![root];
    let mut files = Vec::new();

    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        {
            let entry = entry.unwrap_or_else(|error| {
                panic!(
                    "failed to read directory entry in {}: {error}",
                    path.display()
                )
            });
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
                files.push((path.display().to_string(), source));
            }
        }
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn function_body(source: &str, name: &str) -> String {
    let marker = format!("fn {name}");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("missing function {name}"));
    let body_start = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing body for function {name}"));
    let mut depth = 0_i32;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return source[body_start..=body_start + offset].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unterminated function body for {name}");
}

fn assert_contains_in_order(source: &str, labels: &[&str]) {
    let mut cursor = 0;
    for label in labels {
        let relative = source[cursor..]
            .find(label)
            .unwrap_or_else(|| panic!("missing {label} after byte {cursor}"));
        cursor += relative + label.len();
    }
}

#[test]
fn query_planning_defines_shared_candidate_set_contract() {
    let source = repo_file("anvil-core/src/query_planner.rs");
    for expected in [
        "pub struct CoreDocId",
        "pub struct CandidateSetScope",
        "boundary_schema_generation_hash",
        "pub enum CandidateSetKind",
        "AllWithinPartition",
        "Bitmap",
        "SortedDocIdRanges",
        "OrderedTuples",
        "authz_scope_hash",
        "authz_object_namespace",
        "authz_relation",
        "authz_principal_hash",
        "pub trait AuthzCandidateReader",
        "pub trait IndexCandidateReader",
        "pub struct QueryPlanMetrics",
        "full_scan_forbidden_count",
    ] {
        assert!(source.contains(expected), "missing {expected}");
    }
}

#[test]
fn page_tokens_bind_boundary_schema_generation() {
    let source = format!(
        "{}\n{}\n{}",
        repo_file("anvil-core/src/services/index/query.rs"),
        repo_file("anvil-core/src/services/index/query_page_token.rs"),
        repo_file("anvil-core/src/services/index/operations.rs")
    );
    assert!(source.contains("boundary_schema_generation_hash"));
    assert!(source.contains("root_generation"));
    assert!(source.contains("root_key_hash"));
    assert!(source.contains("index_generation"));
    assert!(source.contains("authz_realm_id"));
    assert!(source.contains("authz_revision"));
    assert!(source.contains("predicate_hash"));
    assert!(source.contains("order_hash"));
    assert!(source.contains("anvil.query.root_key.v1"));
    assert!(source.contains("anvil.query.page_token_scope.v1"));
    assert!(source.contains("boundary_predicates_json"));
    assert!(source.contains("boundary_predicates_hash"));
    assert!(source.contains("PageTokenScopeMismatch"));
    assert!(source.contains("token.validate(&binding)"));
    assert!(source.contains("format!(\"blake3:{}\", blake3::hash(value.as_bytes()).to_hex())"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.root_key_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.predicate_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.order_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.authz_scope_hash"));
    assert!(
        source
            .contains("update_mac_part(&mut mac, self.boundary_schema_generation_hash.as_bytes())")
    );

    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );
    assert!(operations.contains("index_page_token_boundary_hash"));
    assert!(operations.contains("read_boundary_schema_generation_hash"));
    assert!(operations.contains("anvil.query.boundary_schema_generation_hash.v1"));
}

#[test]
fn query_planner_forbids_unbounded_index_candidates_before_range_plans() {
    let planner = repo_file("anvil-core/src/query_planner.rs");
    assert!(planner.contains("CandidateSetKind::AllWithinPartition"));
    assert!(planner.contains("bail!(\"IndexCapabilityMissing\")"));
    assert!(planner.contains("AuthzCandidateSetStale"));
    assert!(planner.contains("payload_ranges_planned"));
    assert!(planner.contains("payload_bytes_planned"));
    assert!(planner.contains("pub fn stable_doc_ordinal"));
    assert!(planner.contains("pub authz_keys: Vec<ObjectAuthzKey>"));
    assert!(planner.contains("flat_map(|range| range.authz_keys.iter().cloned())"));
}

#[test]
fn authz_writer_segments_are_live_query_candidate_sources() {
    let journal = format!(
        "{}\n{}",
        repo_file("anvil-core/src/authz_journal.rs"),
        repo_file("anvil-core/src/authz_journal/materialization.rs")
    );
    let segment = repo_file("anvil-core/src/authz_segment.rs");
    let persistence_tasks = repo_file("anvil-core/src/persistence/tasks.rs");
    let worker = repo_file("anvil-core/src/worker.rs");

    for expected in [
        "record_authz_materialization_deferred(",
        "materialize_authz_derived_state_at_revision",
        "collect_source_records_for_rebuild(",
        "write_authz_tuple_checkpoint_segment(",
    ] {
        assert!(journal.contains(expected), "missing {expected}");
    }
    for forbidden in [
        "advance_authz_materialization(\n        storage,\n        record.tenant_id",
        "advance_authz_materialization(storage, tenant_id, records",
    ] {
        assert!(
            !journal.contains(forbidden),
            "tuple writes must not synchronously materialize authz segments: {forbidden}"
        );
    }
    assert!(
        persistence_tasks.contains("enqueue_authz_materialization("),
        "authz tuple writes must schedule persistent materialization work"
    );
    assert!(
        persistence_tasks.contains("run_authz_materialization_task("),
        "background worker must have a durable authz materialization task body"
    );
    assert!(
        worker.contains("TaskType::AuthzMaterialization"),
        "worker must execute authz materialization tasks"
    );

    for expected in [
        "TABLE_AUTHZ_SCHEMA_DESCRIPTOR",
        "TABLE_AUTHZ_TUPLE",
        "TABLE_AUTHZ_RELATION_RULE",
        "TABLE_AUTHZ_USERSET_EDGE",
        "TABLE_AUTHZ_REVISION_LOG",
        "TABLE_AUTHZ_LIST_OBJECTS",
        "TABLE_AUTHZ_LIST_SUBJECTS",
        "AuthzRevisionCheckpointRow",
        "AuthzSegmentCandidateReader",
        "impl AuthzCandidateReader for AuthzSegmentCandidateReader",
        "request.candidate_scope.clone()",
        "request.partition_id",
        "CandidateSet::bitmap_from_ordinals",
        "stable_doc_ordinal(&[namespace, object_id])",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }
}

#[test]
fn live_index_query_paths_route_through_corestore_query_planner() {
    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    let adapter = repo_file("anvil-core/src/services/index/query_planner_adapter.rs");

    assert!(adapter.contains("execute_corestore_query_plan"));
    assert!(adapter.contains("CoreStoreQueryPlanner"));
    assert!(adapter.contains("PlannerBoundaryCandidateAdapter"));
    assert!(adapter.contains("PlannerAuthzCandidateAdapter"));
    assert!(adapter.contains("PlannerIndexCandidateAdapter"));
    assert!(adapter.contains("selected_object_ids"));
    assert!(adapter.contains("from_index_query_hits"));
    assert!(adapter.contains("from_hybrid_candidates"));
    assert!(adapter.contains("from_typed_value_entries"));
    assert!(!adapter.contains("from_full_text_hits"));
    assert!(!adapter.contains("from_hybrid_accum"));
    assert!(!adapter.contains("from_vector_hits"));

    let planner_calls = operations.matches("execute_corestore_query_plan(").count();
    assert!(
        planner_calls >= 5,
        "expected full_text, metadata/path, typed_json, hybrid, and vector query paths to call planner; saw {planner_calls}"
    );
    assert!(!operations.contains("_planner_snapshot"));
    assert!(operations.contains("selected_object_ids.contains"));
    assert!(operations.contains("query_hit_visible("));
    assert!(operations.contains("planner_result.metrics.index_candidate_count"));
    assert!(
        repo_file("anvil-core/src/services/index/query.rs")
            .contains("anvil_query_index_candidate_count")
    );
}

#[test]
fn every_live_index_query_method_enters_the_unified_planner() {
    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    for method in [
        "query_full_text_index",
        "query_metadata_backed_index",
        "query_typed_json_index",
        "query_hybrid_index",
        "query_vector_index",
    ] {
        let body = function_body(&operations, method);
        assert!(
            body.contains("execute_corestore_query_plan("),
            "{method} does not call the unified planner"
        );
        assert!(
            body.contains("PlannerCandidateSnapshot::"),
            "{method} does not build a planner snapshot"
        );
        assert!(
            body.contains("planner_result.metrics."),
            "{method} does not emit real planner metrics"
        );
        assert!(
            body.contains("query_hit_visible("),
            "{method} does not apply the final visibility gate"
        );
        assert_contains_in_order(
            &body,
            &[
                "execute_corestore_query_plan(",
                "query_hit_visible(",
                "Ok(Response::new(QueryIndexResponse",
            ],
        );

        let planner_pos = body.find("execute_corestore_query_plan(").unwrap();
        let before_planner = &body[..planner_pos];
        assert!(
            !before_planner.contains("query_hit_visible("),
            "{method} performs final visibility before candidate intersection"
        );
    }
}

#[test]
fn old_per_index_and_composite_bypass_symbols_are_absent_from_production_sources() {
    let banned = [
        "execute_composite_query_spec",
        "from_full_text_hits",
        "from_vector_hits",
        "from_hybrid_accum",
        "plan_metadata_backed_candidates(",
        "plan_typed_json_candidates(",
        "fn plan_row_candidates",
        "typed_json_candidate_ordinals_from_value_index",
        "metadata_candidate_ordinals_from_value_index",
    ];

    for (path, source) in repo_source_files_under("anvil-core/src/services/index") {
        for symbol in banned {
            assert!(
                !source.contains(symbol),
                "{path} still contains legacy query bypass symbol {symbol}"
            );
        }
    }

    let rpc = repo_file("anvil-core/src/services/index/rpc.rs");
    assert!(
        rpc.contains("plan.typed_filter_index.is_some()")
            && rpc.contains("self.query_composite_query_spec("),
        "QuerySpec composite execution must route through the planner-backed composite path"
    );
}

#[test]
fn candidate_helpers_are_either_called_or_removed() {
    let corpus = repo_source_files_under("anvil-core/src/services/index")
        .into_iter()
        .map(|(_, source)| source)
        .collect::<Vec<_>>()
        .join("\n");

    for helper in [
        "from_loaded_typed_rows",
        "collect_segment_allowed_objects",
        "plan_loaded_typed_json_candidates",
        "plan_loaded_metadata_backed_candidates",
    ] {
        let count = corpus.matches(helper).count();
        assert!(
            count == 0 || count > 1,
            "candidate helper {helper} is defined but not used by the live planner path"
        );
    }
}

#[test]
fn query_candidate_adapters_fail_closed_for_unsupported_shapes_and_scope_mismatch() {
    let adapter = repo_file("anvil-core/src/services/index/query_planner_adapter.rs");
    let planner = repo_file("anvil-core/src/query_planner.rs");

    for expected in [
        "ensure_planner_supported_query_shape",
        "Status::failed_precondition(\"IndexCapabilityMissing\")",
        "validate_boundary_request_scope",
        "validate_index_request_scope",
        "request_or_json_hash",
        "IndexGenerationMismatch",
    ] {
        assert!(adapter.contains(expected), "missing {expected}");
    }

    for expected in [
        "CandidateSetScopeMismatch",
        "ensure_compatible_with",
        "pub fn intersect_all",
        "pub fn validate(&self) -> Result<()>",
        "CandidateRangePartitionMismatch",
        "CandidateOrderedTuplePartitionMismatch",
    ] {
        assert!(planner.contains(expected), "missing {expected}");
    }
}

#[test]
fn public_api_tests_cover_planner_authz_and_page_scope_behaviour() {
    let typed_lifecycle = repo_file("anvil/tests/index_tests/typed_lifecycle.rs");
    for expected in [
        "test_live_metadata_query_uses_planner_authz_candidates_and_scoped_page_tokens",
        "planner-no-object-reader",
        "tenant-a/denied.json",
        "first_page.next_page_token",
        "require_caught_up_to_watch_cursor: u64::MAX.to_string()",
    ] {
        assert!(typed_lifecycle.contains(expected), "missing {expected}");
    }
}

#[test]
fn unsupported_query_shapes_fail_closed_in_live_guards() {
    let adapter = repo_file("anvil-core/src/services/index/query_planner_adapter.rs");
    let candidates = repo_file("anvil-core/src/services/index/query_candidates.rs");
    let value_index = repo_file("anvil-core/src/services/index/query_value_index.rs");
    let operations = repo_file("anvil-core/src/services/index/operations.rs");

    let guard = function_body(&adapter, "ensure_planner_supported_query_shape");
    for expected in [
        "\"full_text\" | \"vector\" | \"hybrid\"",
        "\"path\"",
        "\"metadata_filter\"",
        "\"typed_json\"",
        "Status::failed_precondition(\"IndexCapabilityMissing\")",
    ] {
        assert!(guard.contains(expected), "missing planner guard {expected}");
    }
    assert!(guard.contains("req.path_prefix.trim().is_empty()"));
    assert!(guard.contains("query_json_field_has_terms(&req.metadata_filters_json)"));
    assert!(guard.contains("query_json_field_has_terms(&req.boundary_predicates_json)"));
    assert!(guard.contains("query_json_field_has_terms(&req.typed_predicates_json)"));
    assert!(guard.contains("query_json_field_has_terms(&req.typed_order_json)"));

    let lookup = function_body(&candidates, "typed_json_value_index_lookups_for_predicate");
    assert!(lookup.contains("\"lt\" | \"<\" | \"lte\" | \"<=\" | \"gt\" | \">\" | \"gte\" | \">=\" | \"exists\" | \"prefix\""));
    assert!(
        lookup.contains("return Err(Status::failed_precondition(\"IndexCapabilityMissing\"));")
    );

    assert!(value_index.contains("if predicates.is_empty()"));
    assert!(value_index.contains("if !path_prefix.trim().is_empty()"));
    assert!(value_index.contains("if filters.metadata.is_empty()"));
    assert!(
        value_index
            .matches("Status::failed_precondition(\"IndexCapabilityMissing\")")
            .count()
            >= 3
    );

    assert!(operations.contains("if selected.typed_filter.is_some()"));
    assert!(operations.contains("if !boundary_predicates.is_empty()"));
    assert!(
        operations
            .matches("ensure_planner_supported_query_shape(")
            .count()
            >= 4
    );
}

#[test]
fn query_services_reject_late_or_legacy_authz_filters() {
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );

    assert!(query.contains("QuerySpec degraded full-scan fallback is forbidden"));
    assert!(!query.contains("typed_json_index_has_field"));
    assert!(query.contains("requires_object_authorization"));
    assert!(query.contains("effective_authorization_mode"));
    assert!(query.contains("authz_label_filter_for_index_candidate_set"));
    assert!(!query.contains("object_read_scope_patterns_for_bucket"));
    assert!(!query.contains("object_key_prefixes"));
    assert!(
        operations.contains("AuthzRevisionLagging")
            || operations.contains("AuthzCandidateSetStale")
    );
    assert!(!query.contains("collect_object_scope"));

    assert!(operations.contains("Status::failed_precondition(\"IndexCapabilityMissing\")"));
    assert!(operations.contains("query_hit_visible("));
    assert!(operations.contains("system_realm_relationship_allows("));
    assert!(!operations.contains("list_objects(bucket.id, \"\", \"\", i32::MAX"));
    assert!(operations.contains("selected.requires_object_authorization()"));
    assert!(operations.contains("selected.effective_authorization_mode()"));
    assert!(operations.contains("authz_label_filter_for_index_candidate_set("));
    assert!(!operations.contains("QueryPermissionFilter"));
    assert!(!operations.contains("map(|filter| &filter.authorized_labels)"));
    assert!(!operations.contains("AuthzPermissionSetTooLargeForPrefixScope"));
}

#[test]
fn boundary_predicates_use_typed_candidates_not_score_index_scans() {
    let query = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/query.rs"),
        repo_file("anvil-core/src/services/index/query_boundary.rs")
    );
    for expected in [
        "pub(super) struct BoundaryPredicate",
        "fn parse_boundary_predicate_item",
        "fn boundary_predicates_from_range_node",
        "matches_row",
        "matches_metadata",
        "ensure_no_direct_boundary_predicates",
        "Status::failed_precondition(\"IndexCapabilityMissing\")",
    ] {
        assert!(query.contains(expected), "missing {expected}");
    }

    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );
    assert!(operations.contains("BoundaryPredicate::parse_list(&req.boundary_predicates_json)"));
    assert!(operations.contains("typed_json_predicate_hash(&req, &authz_scope)"));
    assert!(!operations.contains("predicate.matches_row(&typed)"));
    assert!(!operations.contains("predicate.matches_metadata(metadata)"));
    assert!(operations.contains("ensure_no_direct_boundary_predicates(&req)?;"));
}

#[test]
fn query_scope_hashes_are_authz_aware_and_fail_closed() {
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let token = repo_file("anvil-core/src/services/index/query_page_token.rs");
    let candidates = repo_file("anvil-core/src/services/index/query_planner_adapter.rs");
    let planner = repo_file("anvil-core/src/query_planner.rs");

    for expected in [
        "authz_aware_query_scope_hash",
        "\"schema\": \"anvil.query.scope_hash.v1\"",
        "\"principal_hash\": authz_scope.principal_hash",
        "\"scope_hash\": authz_scope.scope_hash",
        "stable_json_hash_checked(",
        "Status::invalid_argument(format!(\"Invalid {field_name}: {e}\"))",
    ] {
        assert!(query.contains(expected), "missing {expected}");
    }

    for expected in [
        "validate_scope_hashes",
        "ensure_algorithm_prefixed_hash(&self.query_hash",
        "&self.boundary_schema_generation_hash",
        "\"schema\": \"anvil.query.page_token_scope.v1\"",
        "\"authz_scope_hash\": authz_scope.scope_hash",
        "\"caller_principal_hash\": caller_principal_hash",
    ] {
        assert!(token.contains(expected), "missing {expected}");
    }

    for expected in [
        "root_key_hash: authz_aware_query_scope_hash(",
        "authz_scope_hash: authz_scope.scope_hash.clone()",
        "authz_object_namespace: authz_scope.object_namespace.clone()",
        "authz_relation: authz_scope.relation.clone()",
        "authz_principal_hash: authz_scope.principal_hash.clone()",
    ] {
        assert!(candidates.contains(expected), "missing {expected}");
    }

    for expected in [
        "pub fn validate(&self) -> Result<()>",
        "ensure_algorithm_prefixed_hash(&self.root_key_hash",
        "ensure_algorithm_prefixed_hash(&self.authz_scope_hash",
        "ensure_algorithm_prefixed_hash(&self.authz_principal_hash",
        "self.scope.validate()?",
        "other.validate()?",
    ] {
        assert!(planner.contains(expected), "missing {expected}");
    }
}

#[test]
fn typed_json_queries_use_materialised_value_index_candidates() {
    let segment = repo_file("anvil-core/src/typed_field_segment.rs");
    for expected in [
        "TABLE_TYPED_FIELD_VALUE_INDEX",
        "TABLE_TYPED_ROW_BY_ORDINAL",
        "pub struct TypedFieldValueIndexEntry",
        "pub value_index: Vec<TypedFieldValueIndexEntry>",
        "fn typed_value_index_key",
        "fn decode_typed_field_value_index",
        "pub fn encode_json_value_for_typed_index",
        "pub struct TypedFieldValueIndexLookup",
        "read_typed_field_value_index_entries",
        "typed_value_index_key_prefix",
        "read_typed_field_rows_by_ordinals",
        "read_typed_field_segment_rows_by_ordinals",
        "typed_row_ordinal_key",
        "typed_rows_by_ordinal_rows",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let candidates = repo_file("anvil-core/src/services/index/query_candidates.rs");
    let value_index = repo_file("anvil-core/src/services/index/query_value_index.rs");
    for expected in [
        "typed_json_value_index_lookups_for_predicate",
        "typed_json_predicate_entries_from_entries",
        "typed_json_predicate_ordinals_from_entries",
        "Status::failed_precondition(\"IndexCapabilityMissing\")",
        "encoded_typed_predicate_value",
        "entry.row_ordinal",
    ] {
        assert!(candidates.contains(expected), "missing {expected}");
    }
    for expected in [
        "typed_json_candidate_entries_from_value_index",
        "metadata_candidate_entries_from_value_index",
        "TypedValueCandidateEntry",
        "read_typed_field_value_index_entries",
        "selected = Some(match selected",
    ] {
        assert!(value_index.contains(expected), "missing {expected}");
    }
    for removed in [
        "struct QueryCandidatePlan",
        "plan_metadata_backed_candidates(",
        "plan_typed_json_candidates(",
        "fn plan_row_candidates",
        "fn typed_json_value_index_ordinals",
    ] {
        assert!(
            !candidates.contains(removed),
            "stale helper still present: {removed}"
        );
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("typed_json_candidate_entries_from_value_index"));
    assert!(operations.contains("metadata_candidate_entries_from_value_index"));
    assert!(operations.contains("PlannerCandidateSnapshot::from_typed_value_entries"));
    assert!(operations.contains("read_typed_field_rows_by_ordinals"));
    assert!(
        operations.contains("return Err(Status::failed_precondition(\"IndexCapabilityMissing\"));")
    );
    assert!(!operations.contains("read_latest_typed_field_segment"));
}

#[test]
fn page_token_scope_uses_authz_revision_fence_and_system_revision() {
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    let adapter = repo_file("anvil-core/src/services/index/query_planner_adapter.rs");

    for expected in [
        "pub(super) system_revision: u64",
        "\"system_revision\": system_revision",
        "pub(super) fn revision_fence(&self) -> u64",
        "self.revision.max(self.system_revision)",
    ] {
        assert!(
            query.contains(expected),
            "missing query scope field {expected}"
        );
    }

    assert!(operations.contains("latest_system_authz_revision_for_query"));
    assert!(operations.matches("authz_scope.revision_fence()").count() >= 5);
    assert!(operations.contains("token.validate(&binding)?"));
    assert!(adapter.contains("system_revision: authz_scope.system_revision"));
    assert!(adapter.contains("authz_revision: authz_scope.revision_fence()"));
    assert!(adapter.contains("principal_has_bucket_wide_object_access("));
    assert!(adapter.contains("principal_has_system_object_access("));
    assert!(adapter.contains("system_realm_relationship_allows("));
    assert!(adapter.matches("Some(system_revision)").count() >= 2);
    assert!(adapter.contains("Invalid system authz revision"));
}

#[test]
fn final_visibility_gate_uses_revision_bound_zanzibar_checks() {
    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let body = function_body(&operations, "query_hit_visible");

    assert!(body.contains("resolve_permission_at_revision("));
    assert!(body.contains("Some(system_revision)"));
    assert!(body.contains("system_realm_relationship_allows("));
    assert!(body.contains("validation::is_reserved_internal_key(object_key)"));
    assert!(query.contains("must not replace Zanzibar"));
    assert!(query.contains("must run query_hit_visible() before returning a hit"));
}

#[test]
fn full_text_queries_have_term_addressed_postings_range_path() {
    let segment = repo_file("anvil-core/src/full_text_segment.rs");
    for expected in [
        "TABLE_FULL_TEXT_POSTINGS_BY_TERM",
        "fn postings_by_term_rows",
        "read_latest_full_text_segment_terms",
        "read_full_text_segment_terms",
        "RangeAddressedWriterSegment::open",
        "read_table_pages_matching_key_prefix(dictionary_entry",
        "read_table_pages_matching_key_prefix(postings_entry",
        "decode_postings(&posting_row.value)",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("read_latest_full_text_segment_terms"));
    assert!(operations.contains("tokenize_text(&req.query_text"));
}

#[test]
fn vector_queries_have_hnsw_range_reader_path() {
    let segment = repo_file("anvil-core/src/vector_segment.rs");
    for expected in [
        "TABLE_VECTOR_ENTRY_BY_ID",
        "TABLE_VECTOR_HNSW_BY_NODE",
        "fn vector_entry_rows",
        "fn hnsw_adjacency_rows",
        "fn hnsw_adjacency_key",
        "fn decode_hnsw_neighbors",
        "fn decode_hnsw_entrypoints",
        "fn encode_vector_entry_row",
        "fn decode_vector_entry_row",
        "read_vector_segment_header",
        "query_latest_vector_segment_ranges",
        "query_vector_segment_ranges",
        "RangeVectorEntryReader",
        "RangeVectorGraphReader",
        "query_hnsw_graph_with_range_reader",
        "read_table_pages_matching_key_prefix(self.entry_table",
        "graph_adjacency_by_layer",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("read_vector_segment_header"));
    assert!(operations.contains("query_vector_segment_ranges"));
    assert!(!operations.contains("search_query::query_vector_segment("));
}

#[test]
fn writer_segment_range_reader_exposes_table_page_ranges() {
    let range_reader = repo_file("anvil-core/src/writer_segment_range.rs");
    for expected in [
        "RangeAddressedWriterSegment",
        "read_body_table_directory",
        "read_table_pages_matching_key_prefix",
        "read_exact_body_directory_len",
        "read_exact_table_page_directory_len",
        "get_blob_range(GetBlobRange",
        "CoreByteRange",
        "WriterSegmentFixedHeader::decode",
    ] {
        assert!(range_reader.contains(expected), "missing {expected}");
    }

    let table = repo_file("anvil-core/src/formats/table.rs");
    for expected in [
        "pub struct WriterBodyTableDirectory",
        "pub struct WriterBodyTableDirectoryEntry",
        "pub struct TablePageRange",
        "decode_writer_body_table_directory",
        "decode_writer_body_table_page_ranges",
        "decode_table_page_rows",
    ] {
        assert!(table.contains(expected), "missing {expected}");
    }
}
