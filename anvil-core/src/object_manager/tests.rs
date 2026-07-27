use super::*;
use crate::{
    access_control, config::Config, core_store::CoreStore, storage::Storage, system_realm,
};
use tempfile::{TempDir, tempdir};

#[test]
fn core_store_status_distinguishes_availability_from_internal_failure() {
    let unavailable = core_store_status(
        crate::core_store::CoreStoreAvailabilityError::QuorumUnavailable {
            operation: "prepare",
            required: 3,
            received: 2,
            details: "joining peer".to_string(),
        }
        .into(),
    );
    assert_eq!(unavailable.code(), tonic::Code::Unavailable);
    assert!(
        unavailable
            .message()
            .contains(AnvilErrorCode::CoreMetaQuorumUnavailable.as_str())
    );

    let shard_unavailable = core_store_status(
        crate::core_store::CoreStoreAvailabilityError::ShardQuorumUnavailable {
            operation: "object_write",
            required: 6,
            received: 4,
            details: "two peers are unavailable".to_string(),
        }
        .into(),
    );
    assert_eq!(shard_unavailable.code(), tonic::Code::Unavailable);
    assert!(
        shard_unavailable
            .message()
            .contains(AnvilErrorCode::ObjectShardQuorumUnavailable.as_str())
    );

    let internal = core_store_status(anyhow::anyhow!("invalid commit certificate"));
    assert_eq!(internal.code(), tonic::Code::Internal);
}

fn test_config(storage_path: &std::path::Path) -> Config {
    Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: "test-node".to_string(),
        api_listen_addr: "127.0.0.1:0".to_string(),
        region: "test-region".to_string(),
        bootstrap_system_admin_subject_kind: "app".to_string(),
        bootstrap_system_admin_subject_id: "admin-principal".to_string(),
        storage_path: storage_path.to_string_lossy().to_string(),
        ..Config::default()
    }
}

async fn seeded_object_manager(
    bucket_name: &str,
) -> (TempDir, ObjectManager, Bucket, auth::Claims) {
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let config = test_config(&storage_path);
    let storage = Storage::new_at(&config.storage_path).await.unwrap();
    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    let persistence = crate::test_support::persistence_with_active_topology(&config)
        .await
        .unwrap();
    system_realm::ensure_bootstrapped(
        &config,
        &persistence,
        &storage,
        &config.secret_keyring().unwrap(),
    )
    .await
    .unwrap();
    persistence.create_region("test-region").await.unwrap();
    let tenant = persistence
        .create_tenant("tenant-a", "tenant-a")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, bucket_name, "test-region")
        .await
        .unwrap();
    let claims = auth::Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        tenant_id: tenant.id,
        jti: None,
    };
    access_control::grant_storage_tenant_owner(
        &persistence,
        tenant.id,
        &claims.sub,
        "test",
        "object manager dedupe seed",
    )
    .await
    .unwrap();
    access_control::grant_bucket_defaults(
        &persistence,
        &bucket,
        &claims.sub,
        "test",
        "object manager dedupe seed",
    )
    .await
    .unwrap();
    let mvcc = persistence.mvcc_arc().unwrap();
    let manager = ObjectManager::new(
        persistence,
        storage,
        core_store,
        "test-region".to_string(),
        CrossRegionRoutingPolicy::RedirectPreferred,
        hex::decode(&config.anvil_secret_encryption_key).unwrap(),
        Observability::default(),
    );
    manager.install_mvcc(mvcc).unwrap();
    (temp, manager, bucket, claims)
}

fn boundary_schema() -> CoreBoundarySchema {
    CoreBoundarySchema {
        schema: crate::core_store::CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
        bucket: "docs".to_string(),
        generation: 3,
        dimensions: vec![
            crate::core_store::CoreBoundaryDimension {
                name: "customer_tenant".to_string(),
                source: CoreBoundarySource::UserMetadataJsonPointer {
                    pointer: "/customer_tenant_id".to_string(),
                },
                value_type: "uuid".to_string(),
                categories: vec![
                    "security_realm".to_string(),
                    "storage_partition".to_string(),
                ],
                required: true,
                cardinality: "extreme".to_string(),
                max_values_per_block: 1,
                placement_affinity: "prefer_colocate".to_string(),
                compaction_scope: "require_same_value".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
            crate::core_store::CoreBoundaryDimension {
                name: "project".to_string(),
                source: CoreBoundarySource::PathTemplate {
                    template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
                },
                value_type: "string".to_string(),
                categories: vec!["query_prune".to_string()],
                required: true,
                cardinality: "high".to_string(),
                max_values_per_block: 8,
                placement_affinity: "prefer_colocate".to_string(),
                compaction_scope: "prefer_same_value".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
            crate::core_store::CoreBoundaryDimension {
                name: "document_day".to_string(),
                source: CoreBoundarySource::BodyJsonPointer {
                    pointer: "/document/day".to_string(),
                    max_body_bytes: 1024,
                },
                value_type: "date".to_string(),
                categories: vec!["retention_group".to_string()],
                required: false,
                cardinality: "medium".to_string(),
                max_values_per_block: 32,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            },
        ],
        created_at: "2026-01-01T00:00:00Z".to_string(),
    }
}

#[test]
fn object_boundary_extraction_reads_metadata_path_and_body() {
    let values = extract_object_boundary_values(
        &boundary_schema(),
        1,
        "docs",
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("application/json"),
        Some(&serde_json::json!({
            "customer_tenant_id": "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
        })),
        br#"{"document":{"day":"2026-07-07"}}"#.len() as u64,
        br#"{"document":{"day":"2026-07-07"}}"#,
    )
    .unwrap();

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].schema_generation, 3);
    assert_eq!(values[0].name, "customer_tenant");
    assert_eq!(values[0].value, "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a");
    assert_eq!(values[0].source_kind, "user_metadata_json_pointer");
    assert_eq!(values[1].name, "project");
    assert_eq!(values[1].value, "alpha");
    assert_eq!(values[1].source_kind, "path_template");
    assert_eq!(values[2].name, "document_day");
    assert_eq!(values[2].value, "2026-07-07");
    assert_eq!(values[2].source_kind, "body_json_pointer");
}

#[test]
fn object_boundary_extraction_rejects_missing_required_metadata() {
    let error = extract_object_boundary_values(
        &boundary_schema(),
        1,
        "docs",
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("application/json"),
        Some(&serde_json::json!({})),
        br#"{"document":{"day":"2026-07-07"}}"#.len() as u64,
        br#"{"document":{"day":"2026-07-07"}}"#,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(AnvilErrorCode::BoundaryRequiredMissing.as_str())
    );
}

#[test]
fn object_boundary_extraction_rejects_non_json_body_source() {
    let error = extract_object_boundary_values(
        &boundary_schema(),
        1,
        "docs",
        "customers/8e4b4477-99d8-4f4b-89db-876d2c7f0c6a/projects/alpha/docs/a.json",
        Some("text/plain"),
        Some(&serde_json::json!({
            "customer_tenant_id": "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
        })),
        b"plain".len() as u64,
        b"plain",
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str())
    );
}

#[test]
fn default_write_visibility_defers_expensive_follow_up_work() {
    let visibility = ObjectWriteVisibility::default();
    let options = visibility.persistence_options();

    assert_eq!(visibility.indexes, IndexMaintenanceVisibility::Deferred);
    assert_eq!(visibility.watches, WatchVisibility::Deferred);
    assert_eq!(
        visibility.authz_materialization,
        AuthzMaterializationVisibility::InheritedOk
    );
    assert_eq!(
        visibility.boundary_extraction,
        BoundaryExtractionVisibility::HintsOnly
    );
    assert_eq!(
        visibility.index_policy_snapshot,
        IndexPolicySnapshotVisibility::Cached
    );
    assert_eq!(
        visibility.authz_revision,
        AuthzRevisionVisibility::CurrentKnown
    );
    assert!(!options.exact_index_policy_snapshot);
    assert!(!options.exact_authz_revision);
    assert!(!options.enqueue_index_maintenance);
    assert!(!options.enqueue_metadata_compaction);
}

#[test]
fn strict_write_visibility_preserves_previous_synchronous_behaviour() {
    let visibility = ObjectWriteVisibility::strict();
    let options = visibility.persistence_options();

    assert_eq!(visibility.indexes, IndexMaintenanceVisibility::Enqueued);
    assert!(visibility.requires_watch_visible());
    assert!(visibility.requires_payload_boundary_extraction());
    assert!(visibility.requires_authz_materialization());
    assert!(options.exact_index_policy_snapshot);
    assert!(options.exact_authz_revision);
    assert!(options.enqueue_index_maintenance);
    assert!(options.enqueue_metadata_compaction);
}
