use super::*;
use crate::partition_fence::{
    PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
};
use crate::storage::Storage;
use tempfile::tempdir;

const NOW: &str = "2026-07-02T00:00:00Z";
const TEST_SIGNING_KEY: &[u8] = b"mesh-directory-control-stream-test-key";

async fn collect_routing_records_for_test(
    storage: &Storage,
    family: RoutingRecordFamily,
) -> Vec<RoutingRecordDescriptor> {
    let mut cursor = None;
    let mut records = Vec::new();
    loop {
        let page = page_projected_routing_records(storage, family, cursor.as_deref(), 128)
            .await
            .unwrap();
        records.extend(page.records);
        let Some(next_cursor) = page.next_tuple_key else {
            break;
        };
        cursor = Some(next_cursor);
    }
    records
}

async fn mesh_permit(
    storage: &Storage,
    family: RoutingRecordFamily,
    partition: &str,
) -> PartitionWritePermit {
    let partition_id = control_partition_id(family.stream_family(), partition);
    let recovering = acquire_partition_recovery(
        storage,
        PartitionRecoveryAcquire {
            partition_family: CONTROL_PARTITION_FAMILY.to_string(),
            partition_id: partition_id.clone(),
            owner_node_id: "node-test".to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: Utc::now().timestamp_nanos_opt().unwrap(),
        },
        TEST_SIGNING_KEY,
    )
    .await
    .unwrap();
    let ready = publish_partition_ready(
        storage,
        CONTROL_PARTITION_FAMILY,
        &partition_id,
        "node-test",
        recovering.fence_token,
        0,
        &hex::encode([0; 32]),
        Utc::now().timestamp_nanos_opt().unwrap(),
        TEST_SIGNING_KEY,
    )
    .await
    .unwrap();
    ready.write_permit().unwrap()
}

fn authority(permit: &PartitionWritePermit) -> MeshControlWriteAuthority<'_> {
    MeshControlWriteAuthority {
        permit,
        signing_key: TEST_SIGNING_KEY,
    }
}

#[test]
fn tenant_name_partition_path_is_stable() {
    let tenant_name = TenantName::canonicalize("Acme").unwrap();

    assert_eq!(tenant_name.as_str(), "acme");
    assert_eq!(tenant_name.partition_key().as_slice(), b"tenant-name\0acme");
    assert_eq!(tenant_name.partition(), "c1ae");
    assert_eq!(
        tenant_name.descriptor_key(),
        "_anvil/control/v1/mesh/tenant-names/c1ae/acme.pb"
    );
}

#[test]
fn bucket_locator_partition_path_is_stable() {
    let key = BucketLocatorKey::new(
        TenantId::new("tenant_acme").unwrap(),
        BucketName::canonicalize("releases").unwrap(),
    );

    assert_eq!(
        key.partition_key().as_slice(),
        b"bucket-locator\0tenant_acme\0releases"
    );
    assert_eq!(key.partition(), "b41d");
    assert_eq!(
        key.descriptor_key(),
        "_anvil/control/v1/mesh/buckets/b41d/tenant_acme/releases.pb"
    );
}

#[test]
fn duplicate_bucket_names_are_allowed_for_different_tenant_ids() {
    let mut directory = BucketLocatorDirectory::default();

    directory
        .insert(locator("tenant_acme", "bucket_01HYA"))
        .unwrap();
    directory
        .insert(locator("tenant_beta", "bucket_01HYB"))
        .unwrap();

    assert_eq!(directory.len(), 2);
    assert_ne!(
        BucketLocatorKey::new(
            TenantId::new("tenant_acme").unwrap(),
            BucketName::canonicalize("releases").unwrap(),
        )
        .descriptor_key(),
        BucketLocatorKey::new(
            TenantId::new("tenant_beta").unwrap(),
            BucketName::canonicalize("releases").unwrap(),
        )
        .descriptor_key()
    );
}

#[test]
fn duplicate_bucket_names_in_same_tenant_are_rejected_at_locator_layer() {
    let mut directory = BucketLocatorDirectory::default();

    directory
        .insert(locator("tenant_acme", "bucket_01HYA"))
        .unwrap();
    let err = directory
        .insert(locator("tenant_acme", "bucket_01HYZ"))
        .unwrap_err();

    assert_eq!(
        err,
        MeshDirectoryError::DuplicateBucketLocator {
            tenant_id: "tenant_acme".to_string(),
            bucket_name: "releases".to_string(),
        }
    );
    assert_eq!(directory.len(), 1);
}

#[test]
fn tenant_name_canonicalization_rejects_dotted_names() {
    assert!(matches!(
        TenantName::canonicalize("acme.prod"),
        Err(MeshDirectoryError::InvalidTenantName(_))
    ));
    assert!(matches!(
        TenantName::canonicalize("prod.acme."),
        Err(MeshDirectoryError::InvalidTenantName(_))
    ));
}

#[tokio::test]
async fn tenant_name_reservation_is_create_once_and_promoted_by_generation() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let tenant_name = TenantName::canonicalize("Acme").unwrap();
    let tenant_id = TenantId::new("tenant_01").unwrap();
    let reserved = TenantNameDescriptor::reserved(
        MeshId::new("mesh_01").unwrap(),
        tenant_name.clone(),
        tenant_id.clone(),
        "req-1",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();

    let name_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantName,
        &reserved.partition(),
    )
    .await;
    let name_authority = authority(&name_permit);

    let written = reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();
    assert_eq!(written.status, TenantNameStatus::Reserved);
    assert_eq!(written.generation, 1);

    let retry = reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();
    assert_eq!(retry, written);

    let active = activate_tenant_name(&storage, &tenant_name, &tenant_id, 1, NOW, name_authority)
        .await
        .unwrap();
    assert_eq!(active.status, TenantNameStatus::Active);
    assert_eq!(active.generation, 2);
    assert_eq!(active.idempotency_key.as_deref(), Some("req-1"));
    assert_eq!(active.reservation_expires_at, None);

    let active_retry = reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();
    assert_eq!(active_retry.status, TenantNameStatus::Active);
    assert_eq!(active_retry.generation, 2);

    let stream = mesh_control_stream::read_control_stream_page(
        &storage,
        RoutingRecordFamily::TenantName.stream_family(),
        &reserved.partition(),
        0,
        8,
    )
    .await
    .unwrap();
    assert_eq!(stream.records.len(), 2);
    let first_header =
        mesh_control_stream::decode_control_mutation_header(&stream.records[0].frame.header_proto)
            .unwrap();
    let second_header =
        mesh_control_stream::decode_control_mutation_header(&stream.records[1].frame.header_proto)
            .unwrap();
    assert_eq!(first_header.operation, "create");
    assert_eq!(first_header.sequence, 1);
    assert_eq!(first_header.writer_node_id, "node-test");
    assert_eq!(first_header.writer_fence, name_permit.fence_token);
    assert_eq!(second_header.operation, "upsert");
    assert_eq!(second_header.sequence, 2);
}

#[tokio::test]
async fn routing_reads_and_lists_use_control_stream_when_projection_is_stale_or_missing() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let tenant_name = TenantName::canonicalize("Acme").unwrap();
    let tenant_id = TenantId::new("tenant_01").unwrap();
    let reserved = TenantNameDescriptor::reserved(
        MeshId::new("mesh_01").unwrap(),
        tenant_name.clone(),
        tenant_id.clone(),
        "req-1",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();
    let name_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantName,
        &reserved.partition(),
    )
    .await;
    let name_authority = authority(&name_permit);
    reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();
    let active = activate_tenant_name(&storage, &tenant_name, &tenant_id, 1, NOW, name_authority)
        .await
        .unwrap();
    let mut stale_projection = active.clone();
    stale_projection.tenant_id = TenantId::new("tenant_wrong").unwrap();
    stale_projection.generation = 99;
    write_descriptor(&storage, &active.descriptor_key(), &stale_projection)
        .await
        .unwrap();

    let read = read_tenant_name_descriptor(&storage, &tenant_name)
        .await
        .unwrap()
        .expect("tenant-name from stream");
    assert_eq!(read.tenant_id.as_str(), "tenant_01");
    assert_eq!(read.generation, 2);
    let repaired_projection: serde_json::Value = serde_json::from_str(
        &read_descriptor_projection_payload(&storage, &active.descriptor_key())
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(repaired_projection["tenant_id"], "tenant_01");
    assert_eq!(repaired_projection["generation"], 2);

    delete_descriptor_projection(&storage, &active.descriptor_key())
        .await
        .unwrap();
    let recovered = read_tenant_name_descriptor(&storage, &tenant_name)
        .await
        .unwrap()
        .expect("tenant-name rebuilt from stream");
    assert_eq!(recovered.tenant_id.as_str(), "tenant_01");
    assert!(
        read_descriptor_projection_payload(&storage, &active.descriptor_key())
            .await
            .unwrap()
            .is_some()
    );

    let listed = collect_routing_records_for_test(&storage, RoutingRecordFamily::TenantName).await;
    let listed_acme = listed
        .iter()
        .find(|record| record.record_key == "acme")
        .expect("acme listed from stream");
    let listed_payload: serde_json::Value =
        serde_json::from_str(&listed_acme.payload_json).unwrap();
    assert_eq!(listed_payload["tenant_id"], "tenant_01");
    assert_eq!(listed_acme.generation, 2);
}

#[tokio::test]
async fn tenant_name_reservation_rejects_competing_tenant_ids_and_stale_generations() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let tenant_name = TenantName::canonicalize("Acme").unwrap();
    let tenant_id = TenantId::new("tenant_01").unwrap();
    let reserved = TenantNameDescriptor::reserved(
        MeshId::new("mesh_01").unwrap(),
        tenant_name.clone(),
        tenant_id.clone(),
        "req-1",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();
    let name_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantName,
        &reserved.partition(),
    )
    .await;
    let name_authority = authority(&name_permit);
    reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();

    let competing = TenantNameDescriptor::reserved(
        MeshId::new("mesh_01").unwrap(),
        tenant_name.clone(),
        TenantId::new("tenant_02").unwrap(),
        "req-2",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();
    assert!(matches!(
        reserve_tenant_name(&storage, &competing, name_authority).await,
        Err(MeshDirectoryError::TenantNameAlreadyExists { tenant_name })
            if tenant_name == "acme"
    ));

    assert!(matches!(
        activate_tenant_name(&storage, &tenant_name, &tenant_id, 99, NOW, name_authority).await,
        Err(MeshDirectoryError::GenerationConflict {
            expected: 99,
            actual: 1,
            ..
        })
    ));
}

#[tokio::test]
async fn tenant_name_recovery_completes_reserved_name_when_locator_exists() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mesh_id = MeshId::new("mesh_01").unwrap();
    let tenant_name = TenantName::canonicalize("Acme").unwrap();
    let tenant_id = TenantId::new("tenant_01").unwrap();
    let reserved = TenantNameDescriptor::reserved(
        mesh_id.clone(),
        tenant_name.clone(),
        tenant_id.clone(),
        "req-1",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();
    let name_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantName,
        &reserved.partition(),
    )
    .await;
    let name_authority = authority(&name_permit);
    reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();
    let locator_descriptor = TenantLocatorDescriptor::active(
        mesh_id,
        tenant_id,
        tenant_name.clone(),
        RegionName::new("eu-west-1").unwrap(),
        NOW,
    )
    .unwrap();
    let locator_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantLocator,
        &locator_descriptor.partition(),
    )
    .await;
    create_tenant_locator(&storage, &locator_descriptor, authority(&locator_permit))
        .await
        .unwrap();

    let recovered = recover_tenant_name_reservation(
        &storage,
        &tenant_name,
        "2026-07-02T00:01:00Z",
        name_authority,
    )
    .await
    .unwrap()
    .expect("recovered tenant-name");

    assert_eq!(recovered.status, TenantNameStatus::Active);
    assert_eq!(recovered.generation, 2);
}

#[tokio::test]
async fn tenant_name_recovery_tombstones_expired_reserved_name_without_locator() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let tenant_name = TenantName::canonicalize("Acme").unwrap();
    let reserved = TenantNameDescriptor::reserved(
        MeshId::new("mesh_01").unwrap(),
        tenant_name.clone(),
        TenantId::new("tenant_01").unwrap(),
        "req-1",
        "2026-07-02T00:05:00Z",
        NOW,
    )
    .unwrap();
    let name_permit = mesh_permit(
        &storage,
        RoutingRecordFamily::TenantName,
        &reserved.partition(),
    )
    .await;
    let name_authority = authority(&name_permit);
    reserve_tenant_name(&storage, &reserved, name_authority)
        .await
        .unwrap();

    let recovered = recover_tenant_name_reservation(
        &storage,
        &tenant_name,
        "2026-07-02T00:06:00Z",
        name_authority,
    )
    .await
    .unwrap()
    .expect("recovered tenant-name");

    assert_eq!(recovered.status, TenantNameStatus::Tombstoned);
    assert_eq!(recovered.generation, 2);

    let listed = collect_routing_records_for_test(&storage, RoutingRecordFamily::TenantName).await;
    assert!(listed.iter().any(|record| {
        record.record_key == tenant_name.as_str()
            && record.payload_json.contains("\"status\":\"tombstoned\"")
    }));
}

fn locator(tenant_id: &str, bucket_id: &str) -> BucketLocatorDescriptor {
    let tenant_id = TenantId::new(tenant_id).unwrap();
    BucketLocatorDescriptor::active(
        MeshId::new("mesh_01").unwrap(),
        tenant_id.clone(),
        BucketName::canonicalize("releases").unwrap(),
        BucketId::new(bucket_id).unwrap(),
        RegionName::new("eu-west-1").unwrap(),
        CellId::new("cell_a").unwrap(),
        "regional-primary",
        format!("objects/{tenant_id}/releases/"),
        NOW,
    )
    .unwrap()
}
