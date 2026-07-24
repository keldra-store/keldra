use super::*;
mod fencing;
mod helpers;
mod index_definition_lifecycle;
use crate::task_execution_guard::TaskExecutionGuard;
use helpers::*;
use serde_json::json;
use tempfile::tempdir;

fn test_config(storage_path: &std::path::Path) -> Config {
    Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: "test-node".to_string(),
        api_listen_addr: "127.0.0.1:0".to_string(),
        region: "test-region".to_string(),
        storage_path: storage_path.to_string_lossy().to_string(),
        ..Config::default()
    }
}

fn model_manifest() -> crate::anvil_api::ModelManifest {
    crate::anvil_api::ModelManifest {
        schema_version: "1".to_string(),
        artifact_id: "artifact-a".to_string(),
        name: "artifact-a".to_string(),
        format: "test".to_string(),
        components: Vec::new(),
        base_artifact_id: String::new(),
        delta_artifact_ids: Vec::new(),
        signatures: Vec::new(),
        merkle_root: "abc".to_string(),
        meta: std::collections::HashMap::new(),
    }
}

fn test_authz_relation(name: &str) -> crate::anvil_api::AuthzRelationSchema {
    crate::anvil_api::AuthzRelationSchema {
        relation: name.to_string(),
        rules: Vec::new(),
        member_kind: crate::anvil_api::AuthzSchemaMemberKind::DirectRelation as i32,
        allowed_subjects: vec![crate::anvil_api::AuthzAllowedSubject {
            selector_kind: crate::anvil_api::AuthzSubjectSelectorKind::AnyCanonicalId as i32,
            subject_kind: "user".to_string(),
            subject_id: String::new(),
        }],
    }
}

async fn bind_persistence_test_authz_schema(persistence: &Persistence, tenant_id: i64) {
    let schema = crate::authz_realm_schema::put_schema_revision(
        &persistence.storage,
        tenant_id,
        "persistence-test-authz",
        vec![
            crate::anvil_api::AuthzNamespaceSchema {
                namespace: "document".to_string(),
                relations: vec![test_authz_relation("reader"), test_authz_relation("viewer")],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            crate::anvil_api::AuthzNamespaceSchema {
                namespace: "object".to_string(),
                relations: vec![test_authz_relation("reader")],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
        ],
        "test",
        "bind persistence test schema",
    )
    .await
    .unwrap();
    crate::authz_realm_schema::bind_schema(
        &persistence.storage,
        tenant_id,
        crate::authz_scope::DEFAULT_AUTHZ_REALM_ID,
        schema.schema_ref,
        None,
        "test",
        "bind persistence test schema",
    )
    .await
    .unwrap();
}

async fn claim_authz_materialization_guard(
    persistence: &Persistence,
    tenant_id: i64,
    target_revision: u64,
) -> TaskExecutionGuard {
    let task = persistence
        .claim_pending_tasks(1)
        .await
        .unwrap()
        .pop()
        .expect("pending authz materialization task");
    assert_eq!(task.task_type, crate::tasks::TaskType::AuthzMaterialization);
    assert_eq!(task.payload["tenant_id"], json!(tenant_id));
    assert_eq!(task.payload["target_revision"], json!(target_revision));

    let lease = persistence
        .acquire_task_execution_lease(&task)
        .await
        .unwrap();
    TaskExecutionGuard::new(
        persistence.storage().clone(),
        persistence.partition_owner_signing_key().to_vec(),
        lease,
    )
    .unwrap()
}

#[tokio::test]
async fn authz_tuple_write_enqueues_and_materializes_bounded_authorization_state() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    bind_persistence_test_authz_schema(&persistence, 1).await;

    let record = persistence
        .write_authz_tuple(
            1,
            "document",
            "doc-a",
            "reader",
            "user",
            "alice",
            "",
            "add",
            "test",
            "grant reader",
        )
        .await
        .unwrap();

    let tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks;
    let materialization = tasks
        .iter()
        .find(|task| task.task_type == crate::tasks::TaskType::AuthzMaterialization)
        .expect("authz write should enqueue persistent materialization task");
    assert_eq!(materialization.payload["tenant_id"], json!(1));
    assert_eq!(
        materialization.payload["target_revision"],
        json!(record.revision)
    );

    let unavailable = crate::authz_segment::resolve_materialized_permission_at_revision(
        &persistence.storage,
        1,
        "document",
        "doc-a",
        "reader",
        "user",
        "alice",
        "",
        record.revision as u64,
    )
    .await
    .unwrap_err();
    assert!(unavailable.to_string().contains("AuthzRevisionUnavailable"));

    let guard = claim_authz_materialization_guard(&persistence, 1, record.revision as u64).await;
    let outcome = persistence
        .run_authz_materialization_task(1, record.revision as u64, &guard)
        .await
        .unwrap();
    assert_eq!(outcome.processed_revision, record.revision as u64);
    assert_eq!(outcome.source_rows_visited, 1);
    let materialized = crate::authz_segment::resolve_materialized_permission_at_revision(
        &persistence.storage,
        1,
        "document",
        "doc-a",
        "reader",
        "user",
        "alice",
        "",
        record.revision as u64,
    )
    .await
    .unwrap();
    assert!(materialized.allowed);
    assert!(materialized.stats.segments_opened > 0);
    assert!(
        materialized.stats.segments_opened
            <= crate::authz_segment::AUTHZ_DELTA_CHECKPOINT_INTERVAL as usize
    );
    assert_eq!(materialized.stats.table_rows_visited, 1);
    let lag = crate::authz_derived_lag_watch::latest_authz_derived_lag_watch_event(
        &persistence.storage,
        1,
        crate::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID,
    )
    .await
    .unwrap()
    .expect("authz materialization should publish lag catch-up event");
    assert_eq!(lag.payload.processed_revision, record.revision as u64);
    assert_eq!(lag.payload.latest_revision, record.revision as u64);
}

#[tokio::test]
async fn authz_materialization_task_catches_up_a_grouped_revision_backlog() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    bind_persistence_test_authz_schema(&persistence, 42).await;

    let first = persistence
        .write_authz_tuple(
            42,
            "document",
            "backlog-1",
            "reader",
            "user",
            "alice",
            "",
            "add",
            "test",
            "seed authz materialization",
        )
        .await
        .unwrap();

    let mut latest = first;
    for revision in 2..=8 {
        latest = persistence
            .write_authz_tuple(
                42,
                "document",
                &format!("backlog-{revision}"),
                "reader",
                "user",
                "alice",
                "",
                "add",
                "test",
                "extend authz materialization backlog",
            )
            .await
            .unwrap();
    }

    let target_revision = latest.revision as u64;
    let guard = claim_authz_materialization_guard(&persistence, 42, target_revision).await;
    let outcome = persistence
        .run_authz_materialization_task(42, target_revision, &guard)
        .await
        .unwrap();

    assert_eq!(outcome.processed_revision, target_revision);
    assert_eq!(outcome.source_rows_visited, 8);
    for revision in 1..=target_revision {
        assert!(
            crate::authz_segment::existing_authz_tuple_segment_ref(
                &persistence.storage,
                42,
                revision,
            )
            .await
            .unwrap()
            .is_some(),
            "materialization task must publish every revision through {target_revision}"
        );
    }
}

#[tokio::test]
async fn authz_materialization_job_latency_with_retained_history_perf() {
    if std::env::var_os("ANVIL_RUN_AUTHZ_JOB_PERF").is_none() {
        return;
    }

    let retained = std::env::var("ANVIL_AUTHZ_PERF_SEED")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100);
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    bind_persistence_test_authz_schema(&persistence, 42).await;

    let seed_started = std::time::Instant::now();
    for idx in 0..retained {
        persistence
            .write_authz_tuple(
                42,
                "document",
                &format!("seed-{idx}"),
                "reader",
                "user",
                "alice",
                "",
                "add",
                "bench",
                "seed retained tuple history",
            )
            .await
            .unwrap();
    }
    let seed_elapsed = seed_started.elapsed();

    let write_started = std::time::Instant::now();
    let record = persistence
        .write_authz_tuple(
            42,
            "document",
            "measured",
            "reader",
            "user",
            "alice",
            "",
            "add",
            "bench",
            "measured retained tuple write",
        )
        .await
        .unwrap();
    let write_elapsed = write_started.elapsed();

    let immediate_check_ms =
        measure_authz_permission_checks(&persistence.storage, 42, record.revision).await;
    let guard = claim_authz_materialization_guard(&persistence, 42, record.revision as u64).await;
    let materialize_started = std::time::Instant::now();
    let outcome = persistence
        .run_authz_materialization_task(42, record.revision as u64, &guard)
        .await
        .unwrap();
    let materialize_elapsed = materialize_started.elapsed();
    assert_eq!(outcome.processed_revision, record.revision as u64);
    let post_materialization_check_ms =
        measure_authz_permission_checks(&persistence.storage, 42, record.revision).await;

    eprintln!(
        "[authz-job-perf] retained={retained} seed_ms={} measured_write_ms={} immediate_check_ms={:?} materialize_job_ms={} post_materialization_check_ms={:?}",
        seed_elapsed.as_millis(),
        write_elapsed.as_millis(),
        immediate_check_ms,
        materialize_elapsed.as_millis(),
        post_materialization_check_ms,
    );
}

async fn measure_authz_permission_checks(
    storage: &crate::storage::Storage,
    tenant_id: i64,
    revision: i64,
) -> Vec<u128> {
    let mut check_elapsed_ms = Vec::new();
    for _ in 0..10 {
        let check_started = std::time::Instant::now();
        let allowed = crate::authz_journal::resolve_permission_at_revision(
            storage, tenant_id, "document", "measured", "reader", "user", "alice", "", revision,
        )
        .await
        .unwrap();
        check_elapsed_ms.push(check_started.elapsed().as_millis());
        assert!(allowed);
    }
    check_elapsed_ms
}

#[tokio::test]
async fn empty_bucket_index_build_materialises_an_empty_typed_json_segment() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let tenant = persistence
        .create_tenant("empty-index-tenant", "empty-index-tenant")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "empty-index-bucket", "test-region")
        .await
        .unwrap();
    let mutation = IndexDefinitionMutation::Create {
        name: "pending-items".to_string(),
        kind: "typed_json".to_string(),
        selector: json!({"prefix": "items/"}),
        extractor: json!({}),
        authorization_mode: "inherit_object".to_string(),
        build_policy: json!({
            "source_kind": "object_current",
            "fields": [
                {"name": "state", "extractor": "/state", "required": true}
            ]
        }),
    };
    let IndexDefinitionMutationOutcome::Published { index, .. } = persistence
        .apply_index_definition_mutation(&bucket, &mutation, None, None)
        .await
        .unwrap()
    else {
        panic!("index definition create should publish");
    };

    assert!(
        persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .unwrap(),
        "an empty source still needs an initial materialised generation"
    );
    let outcome = persistence
        .rebuild_index_direct(tenant.id, bucket.id, index.id, index.version, 0)
        .await
        .unwrap()
        .expect("typed JSON index build outcome");

    assert_eq!(outcome.item_count, 0);
    assert_eq!(outcome.source_cursor, 0);
    assert!(
        crate::typed_field_segment::latest_typed_field_segment_ref(
            &persistence.storage,
            &outcome.index_storage_id,
        )
        .await
        .unwrap()
        .is_some(),
        "empty typed JSON indexes must be queryable as empty results"
    );

    persistence
        .create_object(
            tenant.id,
            bucket.id,
            "items/one.json",
            "hash-one",
            2,
            "etag-one",
            Some("application/json"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let stats = metadata_journal::active_object_journal_stats(
        &persistence.storage,
        &bucket,
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let current_cursor = index_repair::source_cursor_from_stats(stats);
    assert!(current_cursor > 0);
    persistence
        .rebuild_index_direct(
            tenant.id,
            bucket.id,
            index.id,
            index.version,
            current_cursor,
        )
        .await
        .unwrap()
        .expect("advanced typed JSON index build outcome");

    let stale = persistence
        .rebuild_index_direct(tenant.id, bucket.id, index.id, index.version, 0)
        .await
        .unwrap();
    assert!(stale.is_none(), "stale index tasks must be skipped");
}

#[tokio::test]
async fn tenant_and_bucket_creation_materialise_mesh_directory_locators() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();

    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "eu-west-1")
        .await
        .unwrap();

    let tenant_name = persistence
        .get_mesh_tenant_name_locator("tenant-a")
        .await
        .unwrap()
        .expect("tenant-name locator");
    assert_eq!(tenant_name.tenant_id.as_str(), tenant.id.to_string());
    assert_eq!(tenant_name.status, mesh_directory::TenantNameStatus::Active);
    assert_eq!(tenant_name.idempotency_key.as_deref(), Some("unused"));
    assert_eq!(tenant_name.reservation_expires_at, None);
    assert_eq!(tenant_name.generation, 2);

    let bucket_locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(bucket_locator.tenant_id.as_str(), tenant.id.to_string());
    assert_eq!(bucket_locator.bucket_name.as_str(), "docs");
    assert_eq!(bucket_locator.bucket_id.as_str(), bucket.id.to_string());
    assert_eq!(bucket_locator.home_region.as_str(), "eu-west-1");
    assert_eq!(
        bucket_locator.descriptor_key(),
        format!(
            "_anvil/control/v1/mesh/buckets/{}/{}/docs.pb",
            bucket_locator.partition(),
            tenant.id
        )
    );

    let tenant_name_owner = read_partition_owner(
        &persistence.storage,
        mesh_directory::CONTROL_PARTITION_FAMILY,
        &mesh_directory::control_partition_id(
            mesh_directory::RoutingRecordFamily::TenantName.stream_family(),
            &tenant_name.partition(),
        ),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("tenant-name control partition owner");
    assert_eq!(tenant_name_owner.owner_node_id, persistence.owner_node_id);

    let bucket_locator_owner = read_partition_owner(
        &persistence.storage,
        mesh_directory::CONTROL_PARTITION_FAMILY,
        &mesh_directory::control_partition_id(
            mesh_directory::RoutingRecordFamily::BucketLocator.stream_family(),
            &bucket_locator.partition(),
        ),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("bucket-locator control partition owner");
    assert_eq!(
        bucket_locator_owner.owner_node_id,
        persistence.owner_node_id
    );
}

#[tokio::test]
async fn region_drain_blocks_bucket_creation_and_completion_with_active_locator() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let placement_err = persistence
        .create_bucket(tenant.id, "more-docs", "test-region")
        .await
        .unwrap_err();
    assert_eq!(placement_err.code(), tonic::Code::FailedPrecondition);
    assert!(
        placement_err
            .message()
            .contains("cannot accept new writable placement")
    );

    let completion_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
        )
        .await
        .unwrap_err();
    assert!(
        completion_err
            .to_string()
            .contains("still name the region as primary")
    );
}

#[tokio::test]
async fn region_drain_applies_read_only_exceptions_to_bucket_locators() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let report = persistence
        .apply_region_drain_plan(
            "test-region",
            crate::mesh_lifecycle::BucketDrainDisposition::BlockUntilEmpty,
            vec![RegionDrainBucketOverride {
                tenant_id: tenant.id.to_string(),
                bucket_name: "docs".to_string(),
                disposition: crate::mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly,
                reason: "customer-approved delayed migration".to_string(),
                expires_at: Some("2026-08-02T00:00:00Z".to_string()),
            }],
        )
        .await
        .unwrap();

    assert_eq!(report.decisions.len(), 1);
    let decision = &report.decisions[0];
    assert_eq!(
        decision.status_before,
        mesh_directory::BucketLocatorStatus::Active
    );
    assert_eq!(
        decision.status_after,
        mesh_directory::BucketLocatorStatus::ReadOnly
    );
    assert!(decision.exception_written);
    assert!(decision.locator_updated);

    let locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(
        locator.status,
        mesh_directory::BucketLocatorStatus::ReadOnly
    );
    assert_eq!(locator.generation, 2);

    let exceptions = crate::mesh_lifecycle::list_bucket_drain_exceptions(
        &persistence.storage,
        Some("test-region"),
    )
    .await
    .unwrap();
    assert_eq!(exceptions.len(), 1);
    assert_eq!(
        exceptions[0].disposition,
        crate::mesh_lifecycle::BucketDrainDisposition::RemainProxyOnly
    );

    let full_drain_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
        )
        .await
        .unwrap_err();
    assert!(
        full_drain_err
            .to_string()
            .contains("still name the region as primary")
    );

    let drained_with_exceptions = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions,
        )
        .await
        .unwrap();
    assert_eq!(
        drained_with_exceptions.state,
        crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions
    );
}

#[tokio::test]
async fn region_drain_delete_after_retention_keeps_region_from_exception_completion() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let (region, _, _) = register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let draining = persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
        )
        .await
        .unwrap();
    let report = persistence
        .apply_region_drain_plan(
            "test-region",
            crate::mesh_lifecycle::BucketDrainDisposition::DeleteAfterRetention,
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        report.decisions[0].status_after,
        mesh_directory::BucketLocatorStatus::Draining
    );

    let completion_err = persistence
        .transition_region_descriptor(
            "test-region",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::DrainedWithExceptions,
        )
        .await
        .unwrap_err();
    assert!(
        completion_err
            .to_string()
            .contains("do not have a valid read-only drain exception")
    );
}

#[tokio::test]
async fn node_drain_completion_requires_no_runtime_ownership_and_force_offline_expires_it() {
    let temp = tempdir().unwrap();
    let mut config = test_config(temp.path());
    config.public_api_addr = "admin-node".to_string();
    let persistence = Persistence::new(&config).unwrap();
    let now_nanos = current_time_nanos()
        .unwrap()
        .saturating_add(3_600_000_000_000);
    let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)
        .unwrap()
        .saturating_mul(1_000_000);

    let region = persistence
        .create_region_descriptor(crate::mesh_lifecycle::CreateRegionDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            public_base_url: "https://test-region.anvil-storage.test".to_string(),
            virtual_host_suffix: "test-region.anvil-storage.test".to_string(),
            placement_weight: 100,
            default_cell: Some("default".to_string()),
        })
        .await
        .unwrap();
    let cell = persistence
        .register_cell_descriptor(crate::mesh_lifecycle::RegisterCellDescriptor {
            mesh_id: "default".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        })
        .await
        .unwrap();
    persistence
        .transition_cell_descriptor(
            "test-region",
            "default",
            cell.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    persistence
        .transition_region_descriptor(
            "test-region",
            region.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
        )
        .await
        .unwrap();
    let worker = persistence
        .register_node_descriptor(crate::mesh_lifecycle::RegisterNodeDescriptor {
            mesh_id: "default".to_string(),
            node_id: "worker-node".to_string(),
            region: "test-region".to_string(),
            cell_id: "default".to_string(),
            receipt_signing_public_key: crate::node_signing::NodeSigningKeypair::generate()
                .unwrap()
                .public_key_bytes()
                .to_vec(),
            public_api_addr: "worker-node".to_string(),
            capabilities: vec![crate::mesh_lifecycle::NodeCapability::Object],
            capacity_json: "{}".to_string(),
        })
        .await
        .unwrap();
    let worker = persistence
        .transition_node_descriptor(
            "worker-node",
            worker.generation,
            crate::mesh_lifecycle::LifecycleState::Active,
            None,
        )
        .await
        .unwrap();

    let partition_owner = crate::partition_fence::acquire_partition_recovery(
        &persistence.storage,
        crate::partition_fence::PartitionRecoveryAcquire {
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([8; 32]),
            owner_node_id: "worker-node".to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let partition_owner = crate::partition_fence::publish_partition_ready(
        &persistence.storage,
        &partition_owner.partition_family,
        &partition_owner.partition_id,
        "worker-node",
        partition_owner.fence_token,
        1,
        &hex::encode([1; 32]),
        now_nanos.saturating_add(1),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let stale_partition_permit = partition_owner.write_permit().unwrap();

    crate::partition_fence::acquire_ownership(
        &persistence.storage,
        crate::partition_fence::AcquireOwnership {
            request_id: "worker-control-acquire".to_string(),
            idempotency_key: "worker-control-acquire".to_string(),
            resource: crate::partition_fence::OwnershipResource {
                resource_kind: crate::partition_fence::OwnershipResourceKind::WatchPartition,
                resource_id: "watch/alpha".to_string(),
            },
            owner: crate::partition_fence::OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "worker-node".to_string(),
                actor_instance_id: "worker-node".to_string(),
                display_name: "worker-node".to_string(),
                region: "test-region".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let task_lease = crate::task_lease::acquire_task_lease(
        &persistence.storage,
        crate::task_lease::TaskLeaseAcquire {
            task_id: "worker-task".to_string(),
            task_kind: "index-build".to_string(),
            partition_family: "index_partition".to_string(),
            partition_id: hex::encode([9; 32]),
            owner: crate::task_lease::TaskLeaseOwner::node("worker-node"),
            source_cursor: 1,
            now_nanos,
            ttl_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let draining = persistence
        .transition_node_descriptor(
            "worker-node",
            worker.generation,
            crate::mesh_lifecycle::LifecycleState::Draining,
            Some(crate::mesh_lifecycle::NodeDrainDescriptor {
                started_at: "2026-07-02T00:00:00Z".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
        )
        .await
        .unwrap();
    let blockers = persistence
        .node_runtime_ownership_blockers("worker-node")
        .await
        .unwrap();
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker.starts_with("partition_owner:object_metadata/"))
    );
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker.starts_with("ownership_fence:watch_partition/watch/alpha"))
    );
    assert!(
        blockers
            .iter()
            .any(|blocker| blocker == "task_lease:index-build:worker-task:fence=1")
    );

    let drained = persistence
        .transition_node_descriptor(
            "worker-node",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Drained,
            None,
        )
        .await
        .unwrap_err();
    assert!(drained.to_string().contains("drain cannot complete"));

    let offline = persistence
        .transition_node_descriptor(
            "worker-node",
            draining.generation,
            crate::mesh_lifecycle::LifecycleState::Offline,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        offline.state,
        crate::mesh_lifecycle::LifecycleState::Offline
    );
    assert!(
        persistence
            .node_runtime_ownership_blockers("worker-node")
            .await
            .unwrap()
            .is_empty()
    );
    let stale_rejection = crate::partition_fence::validate_partition_write(
        &persistence.storage,
        &stale_partition_permit,
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap_err();
    assert_eq!(
        stale_rejection.code,
        crate::error_codes::AnvilErrorCode::PartitionNotOwned
    );
    assert!(
        crate::task_lease::checkpoint_task_lease(
            &persistence.storage,
            &task_lease,
            task_lease.source_cursor,
            now_nanos.saturating_add(2),
            &persistence.partition_owner_signing_key,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn mesh_routing_projection_diagnostics_detect_bucket_locator_mismatch() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();

    let clean = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(clean.is_empty());

    let bucket_locator = persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(bucket_locator.bucket_id.as_str(), bucket.id.to_string());
    let mut projected: serde_json::Value = serde_json::to_value(&bucket_locator).unwrap();
    projected["home_region"] = json!("us-east-1");
    mesh_directory::rebuild_routing_record_projection_from_payload(
        &persistence.storage,
        mesh_directory::RoutingRecordFamily::BucketLocator,
        &format!("{}/docs", tenant.id),
        &serde_json::to_vec_pretty(&projected).unwrap(),
    )
    .await
    .unwrap();

    let diagnostics = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == "mesh_control_projection_payload_mismatch"
            && diagnostic.record_key == format!("{}/docs", tenant.id)
            && diagnostic.repair_safe
            && diagnostic.proposed_action == "repair_routing_record_from_control_stream"
    }));

    let repaired = persistence
        .repair_mesh_routing_record(
            mesh_directory::RoutingRecordFamily::BucketLocator,
            &format!("{}/docs", tenant.id),
        )
        .await
        .unwrap();
    assert_eq!(repaired.record_key, format!("{}/docs", tenant.id));
    let repaired_payload: serde_json::Value = serde_json::from_str(&repaired.payload_json).unwrap();
    assert_eq!(repaired_payload["home_region"], "test-region");
    let clean = persistence
        .diagnose_mesh_routing_projection(Some(mesh_directory::RoutingRecordFamily::BucketLocator))
        .await
        .unwrap();
    assert!(clean.is_empty(), "{clean:#?}");
}

#[test]
fn persistence_replays_anvil_owned_state_after_fresh_instance() {
    std::thread::Builder::new()
        .name("persistence-replay-test".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .unwrap()
                .block_on(persistence_replays_anvil_owned_state_after_fresh_instance_body())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn persistence_replays_anvil_owned_state_after_fresh_instance_body() {
    let temp = tempdir().unwrap();
    let first_config = test_config(temp.path());
    let persistence = Persistence::new(&first_config).unwrap();

    persistence.create_region("local").await.unwrap();
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    bind_persistence_test_authz_schema(&persistence, tenant.id).await;
    let app = persistence
        .create_app(tenant.id, "app-a", "client-a", b"encrypted-secret")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "local")
        .await
        .unwrap();
    let object = persistence
        .create_object(
            tenant.id,
            bucket.id,
            "project/a.txt",
            "payload-hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            Some(json!({"label": "alpha"})),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    persistence
        .create_object(
            tenant.id,
            bucket.id,
            "project/nested/b.txt",
            "payload-hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let upload = persistence
        .create_multipart_upload(tenant.id, bucket.id, "uploads/large.bin")
        .await
        .unwrap()
        .upload;
    persistence
        .upsert_multipart_part(
            upload.id,
            1,
            payload_ref("part-hash-a", 4),
            4,
            "part-etag-a",
        )
        .await
        .unwrap();

    let append_stream = persistence
        .create_append_stream(tenant.id, bucket.id, &bucket.name, "events")
        .await
        .unwrap()
        .stream;
    persistence
        .append_stream_record(
            tenant.id,
            bucket.id,
            &append_stream,
            payload_ref("event-payload-hash", 42),
            42,
            None,
            None,
            "tenant/1/principal/test",
        )
        .await
        .unwrap();

    let manifest = persistence
        .compare_and_swap_manifest(
            tenant.id,
            bucket.id,
            &bucket.name,
            "manifests/current.json",
            0,
            json!({"generation": 1}),
            "manifest-hash-a",
        )
        .await
        .unwrap()
        .unwrap();

    let mutation = IndexDefinitionMutation::Create {
        name: "body".to_string(),
        kind: "full_text".to_string(),
        selector: json!({"prefix": "project/"}),
        extractor: json!({"field": "body"}),
        authorization_mode: "inherit".to_string(),
        build_policy: json!({"mode": "watch"}),
    };
    let IndexDefinitionMutationOutcome::Published { index, .. } = persistence
        .apply_index_definition_mutation(&bucket, &mutation, None, None)
        .await
        .unwrap()
    else {
        panic!("index definition create should publish");
    };
    persistence
        .create_index_diagnostic(
            tenant.id,
            bucket.id,
            &bucket.name,
            Some(index.id),
            &index.name,
            &object.key,
            Some(object.version_id),
            "warning",
            "diagnostic-alpha",
            "synthetic diagnostic for replay coverage",
            json!({"source": "test"}),
        )
        .await
        .unwrap();

    let authz = persistence
        .write_authz_tuple(
            tenant.id,
            "document",
            &object.key,
            "reader",
            "user",
            "user-a",
            "",
            "add",
            "test",
            "grant reader",
        )
        .await
        .unwrap();
    persistence
        .enqueue_task(
            crate::tasks::TaskType::DeleteBucket,
            json!({"bucket_id": bucket.id}),
            5,
        )
        .await
        .unwrap();
    persistence
        .create_model_artifact("artifact-a", tenant.id, "models/a", &model_manifest())
        .await
        .unwrap();
    persistence
        .hf_create_key(tenant.id, "primary", b"secret", Some("note"))
        .await
        .unwrap();

    drop(persistence);

    let replayed = Persistence::new(&first_config).unwrap();

    assert!(
        replayed
            .list_regions()
            .await
            .unwrap()
            .contains(&"local".to_string())
    );
    assert_eq!(
        replayed
            .get_tenant_by_name("tenant-a")
            .await
            .unwrap()
            .unwrap()
            .id,
        tenant.id
    );
    assert_eq!(
        replayed
            .get_app_by_client_id("client-a")
            .await
            .unwrap()
            .unwrap()
            .id,
        app.id
    );
    assert_eq!(
        replayed
            .get_bucket_by_name(tenant.id, "docs")
            .await
            .unwrap()
            .unwrap()
            .id,
        bucket.id
    );

    let replayed_object = replayed
        .get_object(bucket.id, "project/a.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed_object.version_id, object.version_id);
    assert_eq!(replayed_object.content_hash, object.content_hash);
    assert_eq!(replayed_object.user_meta.unwrap()["label"], "alpha");

    let (objects, common_prefixes) = replayed
        .list_objects(bucket.id, "project/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["project/a.txt"]
    );
    assert_eq!(common_prefixes, vec!["project/nested/".to_string()]);
    assert_eq!(
        replayed
            .list_object_versions(bucket.id, "project/", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );

    assert_eq!(
        replayed
            .get_active_multipart_upload(
                tenant.id,
                bucket.id,
                "uploads/large.bin",
                upload.upload_id
            )
            .await
            .unwrap()
            .unwrap()
            .id,
        upload.id
    );
    assert_eq!(
        replayed
            .list_multipart_parts(upload.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_append_stream_records(&append_stream, 0, 100)
            .await
            .unwrap()
            .records
            .len(),
        1
    );

    let second_manifest = replayed
        .compare_and_swap_manifest(
            tenant.id,
            bucket.id,
            &bucket.name,
            "manifests/current.json",
            manifest.revision,
            json!({"generation": 2}),
            "manifest-hash-b",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_manifest.revision, manifest.revision + 1);

    assert_eq!(
        replayed
            .list_index_definitions(tenant.id, bucket.id, false)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_index_definition_events(tenant.id, bucket.id, 0, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .list_index_diagnostics(tenant.id, bucket.id, &index.name, "", 0, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replayed
            .check_authz_tuple(
                tenant.id,
                "document",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
            )
            .await
            .unwrap()
            .unwrap()
            .revision,
        authz.revision
    );
    let replayed_tasks = replayed.list_tasks_page(None, 1_000).await.unwrap().tasks;
    assert_eq!(replayed_tasks.len(), 2);
    assert!(
        replayed_tasks
            .iter()
            .any(|task| task.task_type == crate::tasks::TaskType::DeleteBucket)
    );
    assert!(replayed_tasks.iter().any(|task| {
        task.task_type == crate::tasks::TaskType::AuthzMaterialization
            && task.payload["target_revision"] == json!(authz.revision)
    }));
    assert!(
        replayed
            .get_model_artifact("artifact-a")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        replayed
            .hf_list_key_page(tenant.id, None, 10)
            .await
            .unwrap()
            .keys
            .len(),
        1
    );
}

#[tokio::test]
async fn persistence_compacts_object_metadata_and_restarts_from_manifest() {
    let temp = tempdir().unwrap();
    let first_config = test_config(temp.path());
    let persistence = Persistence::new(&first_config).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "compact-bucket", "local")
        .await
        .unwrap();
    let first = persistence
        .create_object(
            1,
            bucket.id,
            "docs/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            Some(json!({"label": "a"})),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "docs/nested/b.txt",
            "hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let sealed = persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata journal should compact");
    assert_eq!(sealed.metadata_record_count, 2);
    assert_eq!(sealed.directory_record_count, 2);

    drop(persistence);
    let restarted = Persistence::new(&first_config).unwrap();

    let replayed = restarted
        .get_object(bucket.id, "docs/a.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replayed.version_id, first.version_id);
    assert_eq!(replayed.content_hash, first.content_hash);
    assert_eq!(replayed.user_meta.unwrap()["label"], "a");

    let (objects, common_prefixes) = restarted
        .list_objects(bucket.id, "docs/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(
        objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt"]
    );
    assert_eq!(common_prefixes, vec!["docs/nested/".to_string()]);
    assert_eq!(
        restarted
            .list_object_versions(bucket.id, "docs/", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );

    let replacement = restarted
        .create_object(
            1,
            bucket.id,
            "docs/a.txt",
            "hash-c",
            13,
            "etag-c",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let (objects_after_append, _) = restarted
        .list_objects(bucket.id, "docs/", "", 100, "/")
        .await
        .unwrap();
    assert_eq!(objects_after_append[0].version_id, replacement.version_id);
    assert_eq!(objects_after_append[0].content_hash, "hash-c");
    assert_eq!(
        restarted
            .list_object_versions(bucket.id, "docs/a.txt", "", None, 100)
            .await
            .unwrap()
            .versions
            .len(),
        2
    );
}

#[tokio::test]
async fn object_metadata_writes_use_one_authoritative_partition_fence() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    register_active_mesh_placement(&persistence).await;
    let tenant = persistence
        .create_tenant("tenant-a", "unused")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "docs", "test-region")
        .await
        .unwrap();
    let partition_id = hex::encode(metadata_journal::object_metadata_partition_id(
        tenant.id, bucket.id,
    ));
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::ObjectPartition,
        resource_id: format!(
            "tenant/{}/bucket/{}/object_metadata/{partition_id}",
            tenant.id, bucket.id
        ),
    };
    persistence
        .create_object(
            tenant.id,
            bucket.id,
            "single-fence.txt",
            "payload-hash",
            1,
            "etag",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let owner = read_partition_owner(
        &persistence.storage,
        "object_metadata",
        &partition_id,
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("object metadata partition owner");
    assert_eq!(owner.owner_node_id, persistence.owner_node_id);
    assert!(
        read_ownership_fence(
            &persistence.storage,
            0,
            &resource,
            &persistence.partition_owner_signing_key,
        )
        .await
        .unwrap()
        .is_none(),
        "ordinary data writes must not stack a generic resource lease on the partition fence"
    );
}

#[tokio::test]
async fn persistence_schedules_deduplicated_object_metadata_compaction_tasks() {
    let temp = tempdir().unwrap();
    let config = Config {
        object_metadata_compaction_frame_threshold: 2,
        object_metadata_compaction_bytes_threshold: 0,
        ..test_config(temp.path())
    };
    let persistence = Persistence::new(&config).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "scheduled-compact-bucket", "local")
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "objects/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks;
    assert!(tasks.is_empty());

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/b.txt",
            "hash-b",
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        tasks[0].task_type,
        crate::tasks::TaskType::ObjectMetadataCompaction
    );
    assert_eq!(tasks[0].payload, json!({ "bucket_id": bucket.id }));

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/c.txt",
            "hash-c",
            13,
            "etag-c",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks
            .len(),
        1,
        "live compaction task should be deduplicated per bucket"
    );

    let claimed = persistence.claim_pending_tasks(1).await.unwrap();
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap();
    persistence
        .update_task_status(claimed[0].id, crate::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/d.txt",
            "hash-d",
            14,
            "etag-d",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks
            .len(),
        1,
        "one post-compaction frame should remain below the threshold"
    );

    persistence
        .create_object(
            1,
            bucket.id,
            "objects/e.txt",
            "hash-e",
            15,
            "etag-e",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks
            .len(),
        2,
        "two post-compaction journal frames should schedule a new task"
    );
}

#[tokio::test]
async fn persistence_serializes_concurrent_task_queue_writes() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();

    let writes = (0..12).map(|bucket_id| {
        let persistence = persistence.clone();
        async move {
            persistence
                .enqueue_task(
                    crate::tasks::TaskType::DeleteBucket,
                    json!({ "bucket_id": bucket_id }),
                    100,
                )
                .await
        }
    });
    let results = futures_util::future::join_all(writes).await;

    for result in results {
        result.unwrap();
    }
    let tasks = persistence
        .list_tasks_page(None, 1_000)
        .await
        .unwrap()
        .tasks;
    assert_eq!(tasks.len(), 12);
    let ids = tasks.iter().map(|task| task.id).collect::<HashSet<_>>();
    assert_eq!(ids.len(), 12);
}

#[test]
fn task_queue_retries_coremeta_target_conflicts() {
    assert!(is_retryable_partition_fence_error(&anyhow!(
        "CoreMeta row cf_leases_fences/0x8904 target mismatch"
    )));
}

#[tokio::test]
async fn persistence_task_execution_lease_targets_object_metadata_partition() {
    let temp = tempdir().unwrap();
    let config = test_config(temp.path());
    let persistence = Persistence::new(&config).unwrap();

    persistence.create_region("local").await.unwrap();
    let bucket = persistence
        .create_bucket(1, "lease-target-bucket", "local")
        .await
        .unwrap();
    persistence
        .create_object(
            1,
            bucket.id,
            "objects/a.txt",
            "hash-a",
            11,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let now = Utc::now();
    let task = TaskRecord {
        id: 77,
        task_type: crate::tasks::TaskType::ObjectMetadataCompaction,
        payload: json!({ "bucket_id": bucket.id }),
        priority: 0,
        status: crate::tasks::TaskStatus::Running,
        attempts: 1,
        last_error: None,
        scheduled_at: now,
        created_at: now,
        updated_at: now,
    };
    let lease = persistence
        .acquire_task_execution_lease(&task)
        .await
        .unwrap();
    assert_eq!(lease.task_id, "task-77");
    assert_eq!(lease.task_kind, "OBJECT_METADATA_COMPACTION");
    assert_eq!(lease.partition_family, "object_metadata");
    assert_eq!(
        lease.partition_id,
        hex::encode(metadata_journal::object_metadata_partition_id(1, bucket.id))
    );
    assert_eq!(
        lease.source_cursor, 1,
        "one object PUT should advance the authoritative metadata stream once"
    );

    let read_back = persistence
        .read_task_execution_lease(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_back, lease);

    let competing_config = Config {
        public_api_addr: "other-worker-node".to_string(),
        ..config
    };
    let competing = Persistence::new(&competing_config).unwrap();
    let err = competing
        .acquire_task_execution_lease(&task)
        .await
        .unwrap_err();
    assert!(err.to_string().contains(task_lease::LEASE_HELD));

    let checkpointed = persistence
        .checkpoint_task_execution_lease(&lease, lease.source_cursor)
        .await
        .unwrap();
    assert_eq!(checkpointed.checkpoint_cursor, lease.source_cursor);
}
