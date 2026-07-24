use super::*;
use chrono::Utc;
use tempfile::tempdir;

const PARTITION_OWNER_KEY: &[u8] = b"bucket metadata partition owner signing key";

async fn list_current_buckets_for_test(storage: &Storage, tenant_id: i64) -> Vec<Bucket> {
    let revision = current_bucket_collection_revision(storage, tenant_id)
        .await
        .unwrap();
    let mut cursor = None;
    let mut buckets = Vec::new();
    loop {
        let page = page_current_buckets(storage, tenant_id, &revision, cursor.as_deref(), 128)
            .await
            .unwrap();
        buckets.extend(page.buckets);
        let Some(next_cursor) = page.next_tuple_key else {
            break;
        };
        cursor = Some(next_cursor);
    }
    buckets
}

fn bucket(id: i64, name: &str, is_public_read: bool) -> Bucket {
    Bucket {
        id,
        tenant_id: 42,
        name: name.to_string(),
        region: "test-region".to_string(),
        created_at: Utc::now(),
        is_public_read,
    }
}

async fn ready_bucket_permit(
    storage: &Storage,
    scope: BucketJournalScope,
    owner_node_id: &str,
) -> PartitionWritePermit {
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        "bucket_metadata".to_string(),
        hex::encode(scope.partition_id()),
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([2; 32]),
        PARTITION_OWNER_KEY,
    )
    .await
    .write_permit()
    .unwrap()
}

async fn write_bucket_current_rows_without_journal(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) {
    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    for scope in [
        BucketJournalScope::Tenant(bucket.tenant_id),
        BucketJournalScope::Global,
    ] {
        let partition_id = hex::encode(scope.partition_id());
        let mutation_id = uuid::Uuid::new_v4().to_string();
        let row_generation = current_unix_nanos();
        let mut operations = bucket_current_coremeta_operations(
            scope,
            bucket,
            mutation,
            &partition_id,
            &mutation_id,
            row_generation,
        )
        .unwrap();
        if scope == BucketJournalScope::Global && mutation == BucketJournalMutation::Create {
            operations.push(
                bucket_id_allocator_put(bucket.id, &partition_id, &mutation_id, row_generation)
                    .unwrap(),
            );
        }
        core_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!(
                    "test-bucket-current:{}:{}",
                    scope.stream_id(),
                    uuid::Uuid::new_v4()
                ),
                scope_partition: partition_id.clone(),
                committed_by_principal: "test-bucket-current".to_string(),
                root_publications: bucket_root_publications(&partition_id, scope.root_anchor_key()),
                preconditions: Vec::new(),
                operations,
            })
            .await
            .unwrap();
    }
}

async fn read_bucket_journal_payloads_for_test(
    storage: &Storage,
    scope: BucketJournalScope,
) -> Result<Vec<BucketJournalBodyProto>> {
    let mut payloads = Vec::new();
    for entry in read_bucket_journal_entries(storage, scope).await? {
        let encoded = encode_bucket_journal_body(&entry.body)?;
        let proto = BucketJournalBodyProto::decode(encoded.as_slice())?;
        ensure_deterministic_proto(&proto, &encoded, "bucket metadata body")?;
        payloads.push(proto);
    }
    Ok(payloads)
}

#[tokio::test]
async fn bucket_ids_are_reserved_by_point_cas_without_catalogue_scans() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let permit = ready_bucket_permit(&storage, BucketJournalScope::Global, "node-a").await;

    let first = reserve_next_bucket_id_with_permit(&storage, &permit, PARTITION_OWNER_KEY);
    let second = reserve_next_bucket_id_with_permit(&storage, &permit, PARTITION_OWNER_KEY);
    let (first, second) = tokio::join!(first, second);
    let mut ids = vec![first.unwrap(), second.unwrap()];
    ids.sort_unstable();

    assert_eq!(ids, vec![1, 2]);
    assert_eq!(next_bucket_id(&storage).await.unwrap(), 3);
    assert!(
        crate::core_store::CoreMetaStore::open(storage.core_store_meta_path())
            .unwrap()
            .scan_prefix_page(
                CF_MESH,
                TABLE_BUCKET_CURRENT_BY_ID_ROW,
                &global_bucket_id_current_tuple_prefix().unwrap(),
                None,
                1,
            )
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn bucket_journal_recovers_create_update_delete_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let private = bucket(1, "private-bucket", false);
    let public = bucket(1, "private-bucket", true);
    let other = bucket(2, "other-bucket", false);

    append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
        .await
        .unwrap();
    append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
        .await
        .unwrap();
    append_bucket_mutation(&storage, &other, BucketJournalMutation::Create)
        .await
        .unwrap();
    append_bucket_mutation(&storage, &other, BucketJournalMutation::Delete)
        .await
        .unwrap();

    let buckets = list_current_buckets_for_test(&storage, 42).await;
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].name, "private-bucket");
    assert!(buckets[0].is_public_read);
    assert!(
        read_current_bucket(&storage, 42, "other-bucket")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        read_current_bucket(&storage, 42, "private-bucket")
            .await
            .unwrap()
            .unwrap()
            .is_public_read
    );
}

#[tokio::test]
async fn bucket_current_rows_page_in_name_order_at_a_stable_revision() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for (id, name) in [(3, "zeta"), (1, "alpha"), (2, "middle")] {
        append_bucket_mutation(
            &storage,
            &bucket(id, name, false),
            BucketJournalMutation::Create,
        )
        .await
        .unwrap();
    }

    let revision = current_bucket_collection_revision(&storage, 42)
        .await
        .unwrap();
    let first = page_current_buckets(&storage, 42, &revision, None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "middle"]
    );
    let second = page_current_buckets(&storage, 42, &revision, first.next_tuple_key.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .buckets
            .iter()
            .map(|bucket| bucket.name.as_str())
            .collect::<Vec<_>>(),
        vec!["zeta"]
    );
    assert!(second.next_tuple_key.is_none());

    append_bucket_mutation(
        &storage,
        &bucket(4, "new", false),
        BucketJournalMutation::Create,
    )
    .await
    .unwrap();
    assert!(
        page_current_buckets(&storage, 42, &revision, None, 2)
            .await
            .unwrap_err()
            .to_string()
            .contains("revision changed")
    );
}

#[tokio::test]
async fn bucket_current_rows_are_sufficient_without_bucket_journal_records() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let public = bucket(7, "core-meta-bucket", true);

    write_bucket_current_rows_without_journal(&storage, &public, BucketJournalMutation::Create)
        .await;

    assert!(
        read_bucket_journal_entries(&storage, BucketJournalScope::Tenant(public.tenant_id))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        read_bucket_journal_entries(&storage, BucketJournalScope::Global)
            .await
            .unwrap()
            .is_empty()
    );

    let buckets = list_current_buckets_for_test(&storage, public.tenant_id).await;
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].id, public.id);
    assert_eq!(
        read_current_bucket(&storage, public.tenant_id, &public.name)
            .await
            .unwrap()
            .unwrap()
            .id,
        public.id
    );
    assert_eq!(
        read_current_bucket_by_id(&storage, public.id)
            .await
            .unwrap()
            .unwrap()
            .name,
        public.name
    );
    assert_eq!(next_bucket_id(&storage).await.unwrap(), public.id + 1);

    write_bucket_current_rows_without_journal(&storage, &public, BucketJournalMutation::Delete)
        .await;

    assert!(
        read_current_bucket(&storage, public.tenant_id, &public.name)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        list_current_buckets_for_test(&storage, public.tenant_id)
            .await
            .is_empty()
    );
    assert!(
        read_current_bucket_by_id(&storage, public.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(next_bucket_id(&storage).await.unwrap(), public.id + 1);
}

#[tokio::test]
async fn bucket_journal_lists_watch_events_from_native_log() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let private = bucket(1, "watched-bucket", false);
    let public = bucket(1, "watched-bucket", true);
    append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
        .await
        .unwrap();
    append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
        .await
        .unwrap();

    let all = list_bucket_metadata_event_page(&storage, 42, "", 0, 10)
        .await
        .unwrap();
    assert_eq!(all.events.len(), 2);
    assert_eq!(all.events[0].event_type, "create");
    assert_eq!(all.events[1].event_type, "policy_update");
    assert!(
        all.events[1].bucket_metadata["is_public_read"]
            .as_bool()
            .unwrap()
    );

    let after_first = list_bucket_metadata_event_page(&storage, 42, "", 1, 10)
        .await
        .unwrap();
    assert_eq!(after_first.events.len(), 1);
    assert_eq!(after_first.events[0].id, 2);

    let latest = latest_bucket_metadata_event(&storage, 42, "watched-bucket")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.id, 2);
    assert_eq!(latest.bucket_name, "watched-bucket");
}

#[tokio::test]
async fn bucket_journal_permits_set_tenant_and_global_payload_fences() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = bucket(1, "fenced-bucket", false);
    let tenant_permit = ready_bucket_permit(
        &storage,
        BucketJournalScope::Tenant(bucket.tenant_id),
        "node-a",
    )
    .await;
    let global_permit = ready_bucket_permit(&storage, BucketJournalScope::Global, "node-a").await;

    append_bucket_mutation_with_permits(
        &storage,
        &bucket,
        BucketJournalMutation::Create,
        &tenant_permit,
        &global_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();

    let tenant_payloads = read_bucket_journal_payloads_for_test(
        &storage,
        BucketJournalScope::Tenant(bucket.tenant_id),
    )
    .await
    .unwrap();
    assert_eq!(tenant_payloads.len(), 1);
    assert_eq!(tenant_payloads[0].fence_token, tenant_permit.fence_token);
    assert!(uuid::Uuid::parse_str(&tenant_payloads[0].mutation_id).is_ok());

    let global_payloads =
        read_bucket_journal_payloads_for_test(&storage, BucketJournalScope::Global)
            .await
            .unwrap();
    assert_eq!(global_payloads.len(), 1);
    assert_eq!(global_payloads[0].fence_token, global_permit.fence_token);
    assert!(uuid::Uuid::parse_str(&global_payloads[0].mutation_id).is_ok());

    let global_records = CoreStore::new(storage.clone())
        .await
        .unwrap()
        .read_stream(ReadStream {
            stream_id: (BucketJournalScope::Global).stream_id(),
            after_sequence: 0,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        global_records[0].payload,
        global_payloads[0].encode_to_vec()
    );
}

#[tokio::test]
async fn bucket_journal_rejects_stale_scope_permit() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = bucket(1, "stale-bucket", false);
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
    let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
    let global_permit = ready_bucket_permit(&storage, BucketJournalScope::Global, "node-b").await;
    assert!(fresh_tenant.fence_token > stale_tenant.fence_token);

    let rejected = append_bucket_mutation_with_permits(
        &storage,
        &bucket,
        BucketJournalMutation::Create,
        &stale_tenant,
        &global_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("PartitionNotOwned"));

    append_bucket_mutation_with_permits(
        &storage,
        &bucket,
        BucketJournalMutation::Create,
        &fresh_tenant,
        &global_permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn bucket_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let bucket = bucket(1, "stale-precondition-bucket", false);
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
    let stale_precondition =
        partition_write_precondition(&storage, &stale_tenant, PARTITION_OWNER_KEY)
            .await
            .unwrap();
    let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
    assert!(fresh_tenant.fence_token > stale_tenant.fence_token);

    let rejected = append_bucket_mutation_to_stream(
        &storage,
        &bucket,
        BucketJournalMutation::Create,
        tenant_scope,
        stale_tenant.fence_token,
        Some(stale_precondition),
    )
    .await
    .unwrap_err();
    assert!(
        rejected.to_string().contains("target mismatch")
            || rejected.to_string().contains("generation mismatch")
            || rejected.to_string().contains("precondition failed"),
        "unexpected error: {rejected:?}"
    );
}
