use super::*;
use crate::core_store::{
    CF_LEASES_FENCES, CoreMetaStore, TABLE_OWNERSHIP_FENCE_ROW, TABLE_PARTITION_OWNER_ROW,
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tempfile::tempdir;

const KEY: &[u8] = b"partition owner signing key";

#[tokio::test]
async fn recovery_acquire_blocks_writes_until_owner_ready() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let recovering = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    assert_eq!(recovering.fence_token, 1);
    assert_eq!(recovering.generation, 1);
    assert_eq!(recovering.status, PartitionOwnerStatus::Recovering);

    let permit = PartitionWritePermit {
        partition_family: recovering.partition_family.clone(),
        partition_id: recovering.partition_id.clone(),
        owner_node_id: "node-a".to_string(),
        fence_token: recovering.fence_token,
    };
    let rejected = validate_partition_write(&storage, &permit, KEY)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, AnvilErrorCode::PartitionNotOwned);

    let ready = publish_partition_ready(
        &storage,
        &recovering.partition_family,
        &recovering.partition_id,
        "node-a",
        recovering.fence_token,
        77,
        &hex::encode([9; 32]),
        200,
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(ready.status, PartitionOwnerStatus::Ready);
    assert_eq!(ready.generation, 2);
    assert_eq!(ready.recovered_through_sequence, 77);
    validate_partition_write(&storage, &ready.write_permit().unwrap(), KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn owner_handoff_rejects_stale_fence_token() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    let first = publish_partition_ready(
        &storage,
        &first.partition_family,
        &first.partition_id,
        "node-a",
        first.fence_token,
        10,
        &hex::encode([3; 32]),
        150,
        KEY,
    )
    .await
    .unwrap();
    let stale_permit = first.write_permit().unwrap();

    let blocked = acquire_partition_recovery(&storage, acquire("node-b", 250), KEY)
        .await
        .unwrap_err();
    assert!(blocked.to_string().contains(OWNERSHIP_HELD));
    validate_partition_write(&storage, &stale_permit, KEY)
        .await
        .unwrap();

    let expired = force_expire_partition_owner_for_node(
        &storage,
        &first.partition_family,
        &first.partition_id,
        "node-a",
        275,
        KEY,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(expired.fence_token > first.fence_token);

    let second = acquire_partition_recovery(&storage, acquire("node-b", 300), KEY)
        .await
        .unwrap();
    assert!(second.fence_token > first.fence_token);
    let stale_rejection = validate_partition_write(&storage, &stale_permit, KEY)
        .await
        .unwrap_err();
    assert_eq!(stale_rejection.code, AnvilErrorCode::PartitionNotOwned);

    let second = publish_partition_ready(
        &storage,
        &second.partition_family,
        &second.partition_id,
        "node-b",
        second.fence_token,
        20,
        &hex::encode([4; 32]),
        350,
        KEY,
    )
    .await
    .unwrap();
    validate_partition_write(&storage, &second.write_permit().unwrap(), KEY)
        .await
        .unwrap();

    let mut stale_same_owner = second.write_permit().unwrap();
    stale_same_owner.fence_token -= 1;
    let stale_rejection = validate_partition_write(&storage, &stale_same_owner, KEY)
        .await
        .unwrap_err();
    assert_eq!(stale_rejection.code, AnvilErrorCode::StaleFenceToken);
}

#[tokio::test]
async fn same_node_concurrent_partition_recovery_acquire_is_idempotent() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let request = acquire("node-a", 100);
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let storage = storage.clone();
        let request = request.clone();
        tasks.push(tokio::spawn(async move {
            acquire_partition_recovery(&storage, request, KEY).await
        }));
    }

    let mut owners = Vec::new();
    for task in tasks {
        owners.push(task.await.unwrap().unwrap());
    }
    let first = owners.first().expect("at least one owner");
    assert_eq!(first.owner_node_id, "node-a");
    assert_eq!(first.status, PartitionOwnerStatus::Recovering);
    for owner in &owners {
        assert_eq!(owner.owner_node_id, first.owner_node_id);
        assert_eq!(owner.fence_token, first.fence_token);
        assert_eq!(owner.recovery_epoch, first.recovery_epoch);
    }
    let page = list_partition_owners_page(&storage, None, 2, KEY)
        .await
        .unwrap();
    assert_eq!(page.owners.len(), 1);
    assert!(page.next_cursor.is_none());
    assert_eq!(page.owners[0].owner_node_id, "node-a");
    assert_eq!(page.owners[0].fence_token, first.fence_token);
}

#[tokio::test]
async fn unrelated_partition_owner_keys_enter_cas_concurrently() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_storage = storage.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            first_barrier,
            acquire_partition_recovery(
                &first_storage,
                acquire_for_partition("node-a", 11, 100),
                KEY,
            ),
        )
        .await
    });
    let second_storage = storage.clone();
    let second = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            barrier,
            acquire_partition_recovery(
                &second_storage,
                acquire_for_partition("node-b", 12, 100),
                KEY,
            ),
        )
        .await
    });

    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .expect("unrelated point-key CAS operations must not wait on a module-global lock");
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first.partition_id, second.partition_id);
    assert_eq!(first.fence_token, 1);
    assert_eq!(second.fence_token, 1);
}

#[tokio::test]
async fn ownership_fence_and_partition_owner_cas_are_not_globally_serialized() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let fence_storage = storage.clone();
    let fence_barrier = barrier.clone();
    let fence = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            fence_barrier,
            acquire_ownership(
                &fence_storage,
                ownership_acquire(principal("app-a", "token-a", "node-a"), 100, 500, "fence-a"),
                KEY,
            ),
        )
        .await
    });
    let owner_storage = storage.clone();
    let owner = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            barrier,
            acquire_partition_recovery(
                &owner_storage,
                acquire_for_partition("node-b", 13, 100),
                KEY,
            ),
        )
        .await
    });

    let (fence, owner) = tokio::time::timeout(Duration::from_secs(10), async {
        (fence.await.unwrap(), owner.await.unwrap())
    })
    .await
    .expect("unrelated fence tables must not share a module-global CAS lock");
    assert_eq!(fence.unwrap().record.fence, 1);
    assert_eq!(owner.unwrap().fence_token, 1);
}

#[tokio::test]
async fn same_partition_owner_key_has_one_cas_winner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_storage = storage.clone();
    let first_barrier = barrier.clone();
    let first = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            first_barrier,
            acquire_partition_recovery(
                &first_storage,
                acquire_for_partition("node-a", 21, 100),
                KEY,
            ),
        )
        .await
    });
    let second_storage = storage.clone();
    let second = tokio::spawn(async move {
        super::coremeta::with_point_cas_barrier(
            barrier,
            acquire_partition_recovery(
                &second_storage,
                acquire_for_partition("node-b", 21, 100),
                KEY,
            ),
        )
        .await
    });

    let outcomes = tokio::time::timeout(Duration::from_secs(10), async {
        [first.await.unwrap(), second.await.unwrap()]
    })
    .await
    .expect("same-key CAS contenders must complete after one typed conflict retry");
    let successes = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    let held = outcomes
        .iter()
        .filter(|outcome| {
            outcome
                .as_ref()
                .is_err_and(|error| error.to_string().contains(OWNERSHIP_HELD))
        })
        .count();
    assert_eq!(successes, 1);
    assert_eq!(held, 1);

    let stored = read_partition_owner(&storage, "object_metadata", &hex::encode([21; 32]), KEY)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(stored.owner_node_id.as_str(), "node-a" | "node-b"));
    assert_eq!(stored.fence_token, 1);
    assert_eq!(stored.generation, 1);
}

#[tokio::test]
async fn partition_owner_pages_are_bounded_and_continue_without_duplicates() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let expected = (31_u8..36)
        .map(|seed| hex::encode([seed; 32]))
        .collect::<BTreeSet<_>>();
    for seed in 31_u8..36 {
        acquire_partition_recovery(
            &storage,
            acquire_for_partition("node-a", seed, i64::from(seed)),
            KEY,
        )
        .await
        .unwrap();
    }

    let mut cursor = None;
    let mut page_sizes = Vec::new();
    let mut listed = BTreeSet::new();
    loop {
        let page = list_partition_owners_page(&storage, cursor.as_ref(), 2, KEY)
            .await
            .unwrap();
        assert!(page.owners.len() <= 2);
        page_sizes.push(page.owners.len());
        for owner in page.owners {
            assert!(listed.insert(owner.partition_id));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(page_sizes, vec![2, 2, 1]);
    assert_eq!(listed, expected);
    assert!(
        list_partition_owners_page(&storage, None, 0, KEY)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn partition_owner_node_pages_use_the_node_projection() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for seed in 1..=32 {
        acquire_partition_recovery(
            &storage,
            acquire_for_partition(&format!("noise-{seed}"), seed, 100),
            KEY,
        )
        .await
        .unwrap();
    }
    for seed in 40..=42 {
        acquire_partition_recovery(
            &storage,
            acquire_for_partition("node-target", seed, 100),
            KEY,
        )
        .await
        .unwrap();
    }

    let first = list_partition_owners_for_node_page(&storage, "node-target", None, 2, KEY)
        .await
        .unwrap();
    assert_eq!(first.owners.len(), 2);
    assert!(
        first
            .owners
            .iter()
            .all(|owner| owner.owner_node_id == "node-target")
    );
    let second = list_partition_owners_for_node_page(
        &storage,
        "node-target",
        first.next_cursor.as_ref(),
        2,
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(second.owners.len(), 1);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn point_cas_reads_are_independent_of_unrelated_owner_cardinality() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    for seed in 40_u8..104 {
        let owner = PartitionOwnerState {
            format_version: 1,
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([seed; 32]),
            owner_node_id: format!("unrelated-{seed}"),
            fence_token: 1,
            recovery_epoch: 1,
            generation: 1,
            status: PartitionOwnerStatus::Recovering,
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            updated_at_nanos: i64::from(seed),
            owner_hash: None,
            owner_signature: None,
        }
        .seal(KEY)
        .unwrap();
        meta.put(
            CF_LEASES_FENCES,
            TABLE_PARTITION_OWNER_ROW,
            &partition_owner_row_key(&owner.partition_family, &owner.partition_id).unwrap(),
            &encode_partition_owner_record(&owner).unwrap(),
        )
        .unwrap();
    }

    let (outcome, point_reads) = super::coremeta::count_point_reads(acquire_partition_recovery(
        &storage,
        acquire_for_partition("target-node", 200, 200),
        KEY,
    ))
    .await;
    outcome.unwrap();
    assert_eq!(point_reads, 1);
}

#[tokio::test]
async fn same_node_recovery_acquire_rejects_mismatched_basis() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();

    let mut mismatched = acquire("node-a", 150);
    mismatched.recovered_through_sequence = first.recovered_through_sequence.saturating_add(1);
    mismatched.recovered_manifest_hash = hex::encode([8; 32]);
    let err = acquire_partition_recovery(&storage, mismatched, KEY)
        .await
        .unwrap_err();
    assert!(err.to_string().contains(OWNERSHIP_HELD));

    let still_owner =
        read_partition_owner(&storage, &first.partition_family, &first.partition_id, KEY)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(still_owner.owner_node_id, "node-a");
    assert_eq!(still_owner.fence_token, first.fence_token);
    assert_eq!(
        still_owner.recovered_manifest_hash,
        first.recovered_manifest_hash
    );
}

#[tokio::test]
async fn different_node_recovery_acquire_does_not_steal_ready_owner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let recovering = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    let ready = publish_partition_ready(
        &storage,
        &recovering.partition_family,
        &recovering.partition_id,
        "node-a",
        recovering.fence_token,
        10,
        &hex::encode([3; 32]),
        150,
        KEY,
    )
    .await
    .unwrap();
    let permit = ready.write_permit().unwrap();

    let err = acquire_partition_recovery(&storage, acquire("node-b", 200), KEY)
        .await
        .unwrap_err();
    assert!(err.to_string().contains(OWNERSHIP_HELD));

    let still_owner =
        read_partition_owner(&storage, &ready.partition_family, &ready.partition_id, KEY)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(still_owner.owner_node_id, "node-a");
    assert_eq!(still_owner.fence_token, ready.fence_token);
    validate_partition_write(&storage, &permit, KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn partition_owner_state_is_signed_and_path_scoped() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    let (row, _) =
        read_partition_owner_state(&storage, &owner.partition_family, &owner.partition_id, KEY)
            .await
            .unwrap()
            .unwrap();
    assert_ne!(row.first().copied(), Some(b'{'));
    let mut tampered = decode_partition_owner_record(&row).unwrap();
    tampered.fence_token = 99;
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    meta.put(
        CF_LEASES_FENCES,
        TABLE_PARTITION_OWNER_ROW,
        &partition_owner_row_key(&owner.partition_family, &owner.partition_id).unwrap(),
        &encode_partition_owner_record(&tampered).unwrap(),
    )
    .unwrap();
    assert!(
        read_partition_owner(&storage, &owner.partition_family, &owner.partition_id, KEY)
            .await
            .is_err()
    );
    assert!(partition_owner_row_key("../escape", &owner.partition_id).is_err());
}

#[tokio::test]
async fn partition_owner_state_is_coremeta_row() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();

    let row_key = partition_owner_row_key(&owner.partition_family, &owner.partition_id).unwrap();
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    let row = meta
        .get(CF_LEASES_FENCES, TABLE_PARTITION_OWNER_ROW, &row_key)
        .unwrap()
        .expect("partition owner must be stored in CoreMeta leases/fences");
    assert_ne!(row.first().copied(), Some(b'{'));
    let stored = decode_partition_owner_record(&row).unwrap();
    assert_eq!(stored.partition_family, owner.partition_family);
    assert_eq!(stored.partition_id, owner.partition_id);
    stored.verify(KEY).unwrap();
}

#[tokio::test]
async fn ownership_label_is_not_security_identity() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner_a = principal("app-a", "token-a", "node-shared");
    let owner_b = principal("app-b", "token-b", "node-shared");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner_a.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert_eq!(first.owner.display_name, "node-shared");
    assert_eq!(first.owner.principal_id, "app-a");
    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "renew-b".to_string(),
                resource: ownership_resource(),
                owner: owner_b.clone(),
                current_fence: first.fence,
                now_nanos: 200,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-b".to_string(),
                idempotency_key: "release-b".to_string(),
                resource: ownership_resource(),
                owner: owner_b,
                current_fence: first.fence,
                administrative_force: false,
                now_nanos: 250,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
}

#[tokio::test]
async fn ownership_fences_are_coremeta_rows() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let acquired = acquire_ownership(
        &storage,
        ownership_acquire(owner, 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let row_key = ownership_fence_row_key(acquired.owner.tenant_id, &acquired.resource).unwrap();
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    let row = meta
        .get(CF_LEASES_FENCES, TABLE_OWNERSHIP_FENCE_ROW, &row_key)
        .unwrap()
        .expect("ownership fence must be stored in CoreMeta leases/fences");
    assert_ne!(row.first().copied(), Some(b'{'));
    let stored = decode_ownership_fence_record(&row).unwrap();
    assert_eq!(stored.fence, acquired.fence);
    assert_eq!(stored.resource, acquired.resource);
    stored.verify(KEY).unwrap();
}

#[tokio::test]
async fn ownership_fence_pages_continue_in_bounded_coremeta_order() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut expected = BTreeSet::new();
    for index in 0..5 {
        let resource_id = format!("tenant-acme/resource-{index}");
        expected.insert(resource_id.clone());
        let mut request = ownership_acquire(
            principal(
                format!("app-{index}"),
                format!("token-{index}"),
                format!("node-{index}"),
            ),
            100 + index,
            500,
            format!("acquire-{index}"),
        );
        request.request_id = format!("ownership-page-{index}");
        request.resource.resource_id = resource_id;
        acquire_ownership(&storage, request, KEY).await.unwrap();
    }

    let mut cursor = None;
    let mut listed = BTreeSet::new();
    let mut page_sizes = Vec::new();
    loop {
        let page = list_ownership_fences_page(&storage, cursor.as_ref(), 2, KEY)
            .await
            .unwrap();
        assert!(page.fences.len() <= 2);
        page_sizes.push(page.fences.len());
        for record in page.fences {
            assert!(listed.insert(record.resource.resource_id));
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(page_sizes, vec![2, 2, 1]);
    assert_eq!(listed, expected);
}

#[tokio::test]
async fn ownership_fence_node_pages_use_the_node_projection() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for index in 0..24 {
        let mut request = ownership_acquire(
            OwnershipPrincipal::node(format!("noise-{index}")),
            100,
            500,
            format!("noise-{index}"),
        );
        request.resource.resource_id = format!("noise-resource-{index}");
        acquire_ownership(&storage, request, KEY).await.unwrap();
    }
    for index in 0..3 {
        let mut request = ownership_acquire(
            OwnershipPrincipal::node("node-target"),
            100,
            500,
            format!("target-{index}"),
        );
        request.resource.resource_id = format!("target-resource-{index}");
        acquire_ownership(&storage, request, KEY).await.unwrap();
    }

    let first =
        list_active_ownership_fences_for_node_page(&storage, "node-target", 200, None, 2, KEY)
            .await
            .unwrap();
    assert_eq!(first.fences.len(), 2);
    let second = list_active_ownership_fences_for_node_page(
        &storage,
        "node-target",
        200,
        first.next_cursor.as_ref(),
        2,
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(second.fences.len(), 1);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn same_resource_in_different_tenants_has_distinct_coremeta_roots() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-a", "token-a", "node-a"),
            100,
            500,
            "tenant-one",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;
    let mut second_owner = principal("app-b", "token-b", "node-b");
    second_owner.tenant_id = 2;
    let second = acquire_ownership(
        &storage,
        ownership_acquire(second_owner, 100, 500, "tenant-two"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let first_root = ownership_fence_record_to_proto(&first)
        .unwrap()
        .common
        .unwrap()
        .root_key_hash;
    let second_root = ownership_fence_record_to_proto(&second)
        .unwrap()
        .common
        .unwrap()
        .root_key_hash;
    assert_ne!(first_root, second_root);
}

#[tokio::test]
async fn expired_ownership_can_be_acquired_and_increments_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-a", "token-a", "node-a"),
            100,
            50,
            "acquire-a",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let second = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-b", "token-b", "node-b"),
            200,
            50,
            "acquire-b",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert_eq!(second.fence, first.fence + 1);
    assert_eq!(second.owner.principal_id, "app-b");
    assert_eq!(second.state, OwnershipFenceState::Active);
}

#[tokio::test]
async fn renew_advances_coremeta_generation_without_changing_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let acquired = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let renewed = renew_ownership(
        &storage,
        RenewOwnership {
            request_id: "renew-a".to_string(),
            resource: ownership_resource(),
            owner,
            current_fence: acquired.fence,
            now_nanos: 200,
            ttl_nanos: 500,
        },
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(renewed.fence, acquired.fence);
    assert_eq!(renewed.generation, acquired.generation + 1);
}

#[tokio::test]
async fn ownership_operations_reject_stale_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;
    let stale_fence = first.fence + 1;

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "renew-stale".to_string(),
                resource: ownership_resource(),
                owner: owner.clone(),
                current_fence: stale_fence,
                now_nanos: 200,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
    assert!(
        transfer_ownership(
            &storage,
            TransferOwnership {
                request_id: "transfer-stale".to_string(),
                idempotency_key: "transfer-stale".to_string(),
                resource: ownership_resource(),
                current_owner: owner.clone(),
                new_owner: principal("app-b", "token-b", "node-b"),
                current_fence: stale_fence,
                now_nanos: 220,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-stale".to_string(),
                idempotency_key: "release-stale".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: stale_fence,
                administrative_force: false,
                now_nanos: 240,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
}

#[tokio::test]
async fn concurrent_ownership_acquires_have_one_winner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut tasks = Vec::new();
    for idx in 0..16 {
        let storage = storage.clone();
        tasks.push(tokio::spawn(async move {
            acquire_ownership(
                &storage,
                ownership_acquire(
                    principal(
                        format!("app-{idx}"),
                        format!("token-{idx}"),
                        format!("node-{idx}"),
                    ),
                    100,
                    500,
                    format!("acquire-{idx}"),
                ),
                KEY,
            )
            .await
        }));
    }

    let mut successes = 0;
    let mut held = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => successes += 1,
            Err(err) if err.to_string().contains(OWNERSHIP_HELD) => held += 1,
            Err(err) => panic!("unexpected ownership error: {err}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(held, 15);
}

#[tokio::test]
async fn force_expire_increments_fence_and_blocks_stale_owner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let expired = force_expire_ownership(
        &storage,
        ForceExpireOwnership {
            request_id: "force-expire".to_string(),
            idempotency_key: "force-expire".to_string(),
            resource: ownership_resource(),
            admin: principal("admin", "admin-token", "admin"),
            reason: "test failover".to_string(),
            now_nanos: 200,
        },
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(expired.state, OwnershipFenceState::Expired);
    assert_eq!(expired.fence, first.fence + 1);

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "stale-renew".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: first.fence,
                now_nanos: 220,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );

    let replacement = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-b", "token-b", "node-b"),
            250,
            500,
            "acquire-b",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(replacement.fence, expired.fence + 1);
}

#[tokio::test]
async fn transfer_moves_to_explicit_target_identity_and_is_idempotent() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let new_owner = principal("app-b", "token-b", "node-b");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let transferred = transfer_ownership(
        &storage,
        TransferOwnership {
            request_id: "transfer".to_string(),
            idempotency_key: "transfer-key".to_string(),
            resource: ownership_resource(),
            current_owner: owner.clone(),
            new_owner: new_owner.clone(),
            current_fence: first.fence,
            now_nanos: 200,
            ttl_nanos: 500,
        },
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(transferred.record.fence, first.fence + 1);
    assert!(transferred.record.owner.same_security_owner(&new_owner));
    assert!(!transferred.record.owner.same_security_owner(&owner));
    assert!(!transferred.idempotent_replay);

    let replay = transfer_ownership(
        &storage,
        TransferOwnership {
            request_id: "transfer-replay".to_string(),
            idempotency_key: "transfer-key".to_string(),
            resource: ownership_resource(),
            current_owner: owner.clone(),
            new_owner,
            current_fence: first.fence,
            now_nanos: 220,
            ttl_nanos: 500,
        },
        KEY,
    )
    .await
    .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(replay.record.fence, transferred.record.fence);

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "old-owner-renew".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: transferred.record.fence,
                now_nanos: 230,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
}

#[tokio::test]
async fn release_requires_owner_and_fence_unless_force() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let other = principal("app-b", "token-b", "node-b");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner, 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-other".to_string(),
                idempotency_key: "release-other".to_string(),
                resource: ownership_resource(),
                owner: other.clone(),
                current_fence: first.fence,
                administrative_force: false,
                now_nanos: 200,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );

    let released = release_ownership(
        &storage,
        ReleaseOwnership {
            request_id: "release-force".to_string(),
            idempotency_key: "release-force".to_string(),
            resource: ownership_resource(),
            owner: other,
            current_fence: 0,
            administrative_force: true,
            now_nanos: 220,
        },
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(released.state, OwnershipFenceState::Released);
    assert_eq!(released.fence, first.fence + 1);
}

fn acquire(owner_node_id: &str, now_nanos: i64) -> PartitionRecoveryAcquire {
    acquire_for_partition(owner_node_id, 7, now_nanos)
}

fn acquire_for_partition(
    owner_node_id: &str,
    partition_seed: u8,
    now_nanos: i64,
) -> PartitionRecoveryAcquire {
    PartitionRecoveryAcquire {
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode([partition_seed; 32]),
        owner_node_id: owner_node_id.to_string(),
        recovered_through_sequence: 0,
        recovered_manifest_hash: hex::encode([0; 32]),
        now_nanos,
    }
}

fn ownership_acquire(
    owner: OwnershipPrincipal,
    now_nanos: i64,
    ttl_nanos: i64,
    idempotency_key: impl Into<String>,
) -> AcquireOwnership {
    AcquireOwnership {
        request_id: format!("req-{}", now_nanos),
        idempotency_key: idempotency_key.into(),
        resource: ownership_resource(),
        owner,
        now_nanos,
        ttl_nanos,
    }
}

fn ownership_resource() -> OwnershipResource {
    OwnershipResource {
        resource_kind: OwnershipResourceKind::BucketPrimary,
        resource_id: "tenant-acme/releases".to_string(),
    }
}

fn principal(
    principal_id: impl Into<String>,
    actor_instance_id: impl Into<String>,
    display_name: impl Into<String>,
) -> OwnershipPrincipal {
    OwnershipPrincipal {
        tenant_id: 1,
        principal_kind: "app".to_string(),
        principal_id: principal_id.into(),
        actor_instance_id: actor_instance_id.into(),
        display_name: display_name.into(),
        region: "eu-west-1".to_string(),
        cell: "cell-a".to_string(),
    }
}
