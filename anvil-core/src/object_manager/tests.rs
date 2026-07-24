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

async fn seeded_core_store_link() -> (TempDir, ObjectManager, Bucket, Object, Object, auth::Claims)
{
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let config = test_config(&storage_path);
    let storage = Storage::new_at(&config.storage_path).await.unwrap();
    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    let persistence = Persistence::new(&config).unwrap();
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
        .create_bucket(tenant.id, "links", "test-region")
        .await
        .unwrap();
    let bucket = persistence
        .set_bucket_public_access(tenant.id, &bucket.name, true)
        .await
        .unwrap();
    access_control::write_bucket_public_read_tuple(
        &persistence,
        &bucket,
        true,
        "test",
        "object manager public link seed",
    )
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
        "object manager link seed",
    )
    .await
    .unwrap();
    access_control::grant_bucket_defaults(
        &persistence,
        &bucket,
        &claims.sub,
        "test",
        "object manager link seed",
    )
    .await
    .unwrap();

    let manager = ObjectManager::new(
        persistence.clone(),
        storage,
        core_store,
        "test-region".to_string(),
        CrossRegionRoutingPolicy::RedirectPreferred,
        hex::decode(&config.anvil_secret_encryption_key).unwrap(),
        Observability::default(),
    );
    let target = manager
        .put_object(
            &claims,
            &bucket.name,
            "versions/app-v1.bin",
            tokio_stream::iter(vec![Ok(b"linked payload".to_vec())]),
            ObjectWriteOptions {
                content_type: Some("application/octet-stream".to_string()),
                user_metadata: None,
                transaction_id: None,
                transaction_principal: None,
                storage_class_id: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let now = chrono::Utc::now();
    let link_target = object_links::ObjectLinkTarget {
        target_key: target.key.clone(),
        target_version: None,
        resolution: object_links::ObjectLinkResolution::Follow,
        generation: 1,
        created_at: now,
        created_by: "principal:test".to_string(),
    };
    let descriptor = object_links::ObjectLinkDescriptor {
        schema: "anvil.object_link.v1".to_string(),
        tenant_id: tenant.id.to_string(),
        bucket_name: bucket.name.clone(),
        link_key: "latest.bin".to_string(),
        target_key: target.key.clone(),
        target_version: None,
        resolution: object_links::ObjectLinkResolution::Follow,
        created_at: now,
        updated_at: now,
        created_by: "principal:test".to_string(),
        generation: 1,
    };
    let link = Object {
        id: target.id + 1,
        tenant_id: tenant.id,
        bucket_id: bucket.id,
        key: descriptor.link_key.clone(),
        kind: object_links::ObjectEntryKind::Link,
        content_hash: object_links::link_metadata_hash(&descriptor),
        size: 0,
        etag: object_links::link_metadata_etag(&descriptor),
        content_type: Some(object_links::LINK_METADATA_CONTENT_TYPE.to_string()),
        version_id: uuid::Uuid::new_v4(),
        mutation_id: uuid::Uuid::new_v4(),
        index_policy_snapshot: "test-index-policy".to_string(),
        user_metadata_hash: blake3::hash(b"core-store-link").to_hex().to_string(),
        authz_revision: 0,
        record_hash: "core-store-link-record".to_string(),
        created_at: now,
        deleted_at: None,
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: Some(link_target),
    };
    manager
        .core_store
        .put_object_metadata(&bucket, &link)
        .await
        .unwrap();

    (temp, manager, bucket, target, link, claims)
}

async fn seeded_object_manager(
    bucket_name: &str,
) -> (TempDir, ObjectManager, Bucket, auth::Claims) {
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let config = test_config(&storage_path);
    let storage = Storage::new_at(&config.storage_path).await.unwrap();
    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    let persistence = Persistence::new(&config).unwrap();
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
    let manager = ObjectManager::new(
        persistence,
        storage,
        core_store,
        "test-region".to_string(),
        CrossRegionRoutingPolicy::RedirectPreferred,
        hex::decode(&config.anvil_secret_encryption_key).unwrap(),
        Observability::default(),
    );
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

#[tokio::test]
async fn small_inline_object_versions_dedupe_and_reference_count_payload() {
    let (_temp, manager, bucket, claims) = seeded_object_manager("inline-dedupe").await;
    let payload = br#"{"schema_version":"1.6.0","id":"GHSA-inline","modified":"2026-07-18T00:00:00Z","aliases":["CVE-2026-0001"]}"#.to_vec();
    let key = "osv/GHSA-inline.json";

    let first = manager
        .put_object(
            &claims,
            &bucket.name,
            key,
            tokio_stream::iter(vec![Ok(payload.clone())]),
            ObjectWriteOptions {
                content_type: Some("application/json".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = manager
        .put_object(
            &claims,
            &bucket.name,
            key,
            tokio_stream::iter(vec![Ok(payload.clone())]),
            ObjectWriteOptions {
                content_type: Some("application/json".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_ne!(first.version_id, second.version_id);
    assert_eq!(first.content_hash, second.content_hash);
    let summaries = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].storage_kind, "inline_payload");
    assert_eq!(summaries[0].reference_count, 2);

    manager
        .delete_object_version(
            &claims,
            &bucket.name,
            key,
            first.version_id,
            None,
            None,
            ObjectWriteVisibility::default(),
        )
        .await
        .unwrap();
    let after_first_delete = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert_eq!(after_first_delete[0].reference_count, 1);

    let result = manager
        .get_object(
            Some(claims.clone()),
            bucket.name.clone(),
            key.to_string(),
            Some(second.version_id),
            None,
        )
        .await
        .unwrap();
    assert_eq!(collect_stream_bytes(result.1).await.unwrap(), payload);

    manager
        .delete_object_version(
            &claims,
            &bucket.name,
            key,
            second.version_id,
            None,
            None,
            ObjectWriteVisibility::default(),
        )
        .await
        .unwrap();
    let after_all_deletes = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert_eq!(after_all_deletes[0].reference_count, 0);
}

#[tokio::test]
async fn erasure_coded_object_versions_dedupe_and_reference_count_blocks() {
    let (_temp, manager, bucket, claims) = seeded_object_manager("erasure-dedupe").await;
    let payload = vec![0xAB; 80 * 1024];
    let key = "payloads/repeated.bin";

    let first = manager
        .put_object(
            &claims,
            &bucket.name,
            key,
            tokio_stream::iter(vec![Ok(payload.clone())]),
            ObjectWriteOptions {
                content_type: Some("application/octet-stream".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = manager
        .put_object(
            &claims,
            &bucket.name,
            key,
            tokio_stream::iter(vec![Ok(payload.clone())]),
            ObjectWriteOptions {
                content_type: Some("application/octet-stream".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_ne!(first.version_id, second.version_id);
    assert_eq!(first.content_hash, second.content_hash);
    let summaries = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert!(!summaries.is_empty());
    assert!(
        summaries
            .iter()
            .all(|summary| summary.storage_kind == "erasure_block")
    );
    assert!(
        summaries.iter().all(|summary| summary.reference_count == 2),
        "large ref summaries: {summaries:?}"
    );

    manager
        .delete_object_version(
            &claims,
            &bucket.name,
            key,
            first.version_id,
            None,
            None,
            ObjectWriteVisibility::default(),
        )
        .await
        .unwrap();
    let after_first_delete = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert!(
        after_first_delete
            .iter()
            .all(|summary| summary.reference_count == 1)
    );

    let result = manager
        .get_object(
            Some(claims.clone()),
            bucket.name.clone(),
            key.to_string(),
            Some(second.version_id),
            None,
        )
        .await
        .unwrap();
    assert_eq!(collect_stream_bytes(result.1).await.unwrap(), payload);

    manager
        .delete_object_version(
            &claims,
            &bucket.name,
            key,
            second.version_id,
            None,
            None,
            ObjectWriteVisibility::default(),
        )
        .await
        .unwrap();
    let after_all_deletes = manager
        .core_store
        .payload_reference_summaries_for_object(&second)
        .await
        .unwrap();
    assert!(
        after_all_deletes
            .iter()
            .all(|summary| summary.reference_count == 0)
    );
}

#[tokio::test]
async fn object_link_metadata_head_and_read_use_core_store_metadata() {
    let (_temp, manager, bucket, target, link, claims) = seeded_core_store_link().await;

    let current = manager
        .read_object_link(Some(claims.clone()), &bucket.name, &link.key, None)
        .await
        .unwrap();
    assert_eq!(current.link_key, link.key);
    assert_eq!(current.target_key, target.key);
    assert_eq!(current.generation, 1);

    let versioned = manager
        .read_object_link(
            Some(claims.clone()),
            &bucket.name,
            &link.key,
            Some(link.version_id),
        )
        .await
        .unwrap();
    assert_eq!(versioned.link_key, link.key);
    assert_eq!(versioned.target_key, target.key);

    let head = manager
        .head_object(Some(claims.clone()), &bucket.name, &link.key, None)
        .await
        .unwrap();
    assert_eq!(head.key, target.key);
    assert_eq!(head.version_id, target.version_id);
    assert_eq!(head.size, target.size);
    assert!(head.etag.starts_with("link-follow-"));

    let result = manager
        .get_object(Some(claims), bucket.name, link.key, None, None)
        .await
        .unwrap();
    let body = collect_stream_bytes(result.1).await.unwrap();
    assert_eq!(body, b"linked payload");
    assert_eq!(result.0.key, target.key);
    assert_eq!(result.2, 0);
}
