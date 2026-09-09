use keldra_store::{
    DeleteRequest, Durability, ObjectKey, Precondition, PutMode, PutRequest, VersionId,
};

use super::*;

fn put(path: &str, bytes: usize) -> BatchOperation {
    let bytes = format!("{{\"value\":\"{}\"}}", "x".repeat(bytes.saturating_sub(12))).into_bytes();
    BatchOperation::Put(PutRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        bytes,
        content_type: Some("application/json".into()),
        mode: PutMode::Put,
        command_id: Some(format!("put-{path}")),
        durability: Durability::Local,
    })
}

fn receipt(version: u64) -> MutationReceipt {
    MutationReceipt {
        command_id: Some(format!("v-{version}")),
        fingerprint: [version as u8; 32],
        version: VersionId(version),
        deleted: false,
        replayed: false,
        replay_guarantee_expires_at_unix_millis: 1,
    }
}

fn delete(path: &str) -> BatchOperation {
    BatchOperation::Delete(DeleteRequest {
        key: ObjectKey::new("tenant", "bucket", path).unwrap(),
        precondition: Precondition::Any,
        command_id: Some(format!("delete-{path}")),
        durability: Durability::Local,
    })
}

fn selected(value: &str) -> ProjectedScalarPointers {
    let bytes = format!("{{\"value\":\"{value}\"}}").into_bytes();
    project_scalar_pointers(&mut Cursor::new(bytes), &[String::from("/value")], 4_096)
        .unwrap()
        .unwrap()
}

#[test]
fn committed_exact_version_is_consumed_once() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("a", 100));
    ingress.admit_committed(pending, &receipt(3));
    assert!(ingress.take_exact_selected(1, 2, "a", 2).is_none());
    assert!(ingress.take_exact_selected(1, 2, "a", 3).is_some());
    assert!(ingress.take_exact_selected(1, 2, "a", 3).is_none());
    assert_eq!(ingress.used_bytes(), 0);
    assert_eq!(ingress.fifo_len(), 0);
}

#[test]
fn repeated_mutations_retain_only_the_latest_version_per_path() {
    let ingress = HotProjectionIngress::new(16 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("hot/path", 100));
    ingress.admit_committed(pending, &receipt(1));
    let retained_bytes = ingress.used_bytes();
    for version in 2..=10_000 {
        let pending = ingress.pending(1, 2, &put("hot/path", 100));
        ingress.admit_committed(pending, &receipt(version));
    }

    assert_eq!(ingress.slot_len(), 1);
    assert_eq!(ingress.fifo_len(), 1);
    assert_eq!(ingress.used_bytes(), retained_bytes);
    assert!(
        ingress
            .take_exact_selected(1, 2, "hot/path", 9_999)
            .is_none()
    );
    assert_eq!(
        ingress.slot_len(),
        1,
        "an older read preserves the newer slot"
    );
    assert!(
        ingress
            .take_exact_selected(1, 2, "hot/path", 10_000)
            .is_some()
    );
    assert_eq!(ingress.slot_len(), 0);
}

#[test]
fn superseded_preparation_cannot_publish_after_the_newer_version() {
    let ingress = HotProjectionIngress::new(16 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let first = ingress
        .register_preparing(
            ingress.pending(1, 2, &put("same", 100)).unwrap(),
            VersionId(10),
        )
        .unwrap();
    let second = ingress
        .register_preparing(
            ingress.pending(1, 2, &put("same", 100)).unwrap(),
            VersionId(11),
        )
        .unwrap();

    assert!(!ingress.preparation_is_current(&first));
    ingress.finish_selected(first, selected("old"));
    ingress.finish_selected(second, selected("new"));

    assert!(ingress.take_exact_selected(1, 2, "same", 10).is_none());
    assert_eq!(ingress.slot_len(), 1);
    assert!(ingress.take_exact_selected(1, 2, "same", 11).is_some());
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn late_older_commit_callback_cannot_replace_newer_ready_state() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    ingress.admit_committed(ingress.pending(1, 2, &put("same", 100)), &receipt(11));
    ingress.admit_committed(ingress.pending(1, 2, &put("same", 100)), &receipt(10));

    assert!(ingress.take_exact_selected(1, 2, "same", 10).is_none());
    assert_eq!(ingress.slot_len(), 1);
    assert!(ingress.take_exact_selected(1, 2, "same", 11).is_some());
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn consumer_take_before_commit_callback_cancels_exact_pending_token() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("overtaken", 100)).unwrap();
    let reserved = ingress.reserved_bytes();
    assert!(reserved > 0);
    assert_eq!(ingress.pending_token_len(), 1);

    assert!(ingress.take_exact_selected(1, 2, "overtaken", 7).is_none());
    assert_eq!(ingress.pending_token_len(), 0);
    assert_eq!(ingress.reserved_bytes(), reserved);
    ingress.admit_committed(Some(pending), &receipt(7));

    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn consumer_discard_before_commit_callback_rejects_delayed_older_version() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("delayed", 100)).unwrap();
    assert_eq!(ingress.pending_token_len(), 1);

    ingress.discard_through(1, 2, "delayed", 11);
    assert_eq!(ingress.pending_token_len(), 0);
    assert!(ingress.reserved_bytes() > 0);
    ingress.admit_committed(Some(pending), &receipt(10));

    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.fifo_len(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn consumed_newer_slot_cannot_be_resurrected_by_older_pending_callback() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let older = ingress.pending(1, 2, &put("same", 100)).unwrap();
    let newer = ingress.pending(1, 2, &put("same", 100)).unwrap();
    assert_eq!(ingress.pending_token_len(), 2);

    ingress.admit_committed(Some(newer), &receipt(11));
    assert_eq!(ingress.pending_token_len(), 0);
    assert!(ingress.take_exact_selected(1, 2, "same", 11).is_some());
    ingress.admit_committed(Some(older), &receipt(10));

    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.fifo_len(), 0);
    assert_eq!(ingress.used_bytes(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn newer_commit_supersedes_older_ready_state_after_its_token_was_cancelled() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let older = ingress.pending(1, 2, &put("same", 100)).unwrap();
    let newer = ingress.pending(1, 2, &put("same", 100)).unwrap();

    ingress.admit_committed(Some(older), &receipt(10));
    assert_eq!(ingress.slot_len(), 1);
    ingress.admit_committed(Some(newer), &receipt(11));

    assert!(ingress.take_exact_selected(1, 2, "same", 10).is_none());
    assert!(ingress.take_exact_selected(1, 2, "same", 11).is_none());
    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.used_bytes(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn consumer_fallback_cancels_preparing_token_without_resurrection() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let registered = ingress
        .register_preparing(
            ingress.pending(1, 2, &put("overtaken", 100)).unwrap(),
            VersionId(7),
        )
        .unwrap();

    assert!(ingress.take_exact_selected(1, 2, "overtaken", 7).is_none());
    ingress.finish_selected(registered, selected("late"));

    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.fifo_len(), 0);
    assert_eq!(ingress.used_bytes(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
}

#[test]
fn newer_consumer_version_discards_an_older_ready_slot() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("a", 100));
    ingress.admit_committed(pending, &receipt(3));

    assert!(ingress.take_exact_selected(1, 2, "a", 4).is_none());
    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.used_bytes(), 0);
}

#[test]
fn in_flight_payloads_are_reserved_before_their_bytes_are_cloned() {
    // One 100-byte projection plus its router fits, while two concurrent
    // projections do not. Derive the bound from the conservative charges so
    // the test follows the represented allocation shapes instead of a stale
    // allocator-specific constant.
    let probe = HotProjectionIngress::new(64 * 1_024).unwrap();
    probe.activate_test_route(1, 2);
    let projected_charge = probe.pending(1, 2, &put("a", 100)).unwrap().charge;
    let maximum_bytes = probe
        .router_bytes()
        .saturating_add(projected_charge)
        .saturating_add(pending_state_resident_bytes(1));
    let ingress = HotProjectionIngress::new(u64::try_from(maximum_bytes).unwrap()).unwrap();
    ingress.activate_test_route(1, 2);
    let first = ingress.pending(1, 2, &put("a", 100)).unwrap();
    assert!(first.project);
    assert!(ingress.reserved_bytes() > 0);
    assert!(
        ingress
            .router_bytes()
            .saturating_add(ingress.reserved_bytes())
            <= maximum_bytes
    );
    let replay_only = ingress.pending(1, 2, &put("b", 100)).unwrap();
    assert!(!replay_only.project);
    drop(first);
    assert_eq!(ingress.reserved_bytes(), replay_only.charge);
    drop(replay_only);
    assert_eq!(ingress.reserved_bytes(), 0);
    assert!(ingress.pending(1, 2, &put("b", 100)).unwrap().project);
}

#[test]
fn full_budget_falls_back_without_blocking_ingestion() {
    let maximum_bytes = 4_096;
    let ingress = HotProjectionIngress::new(maximum_bytes).unwrap();
    ingress.activate_test_route(1, 2);
    for version in 1..=20 {
        let path = format!("item-{version}");
        let pending = ingress.pending(1, 2, &put(&path, 100));
        ingress.admit_committed(pending, &receipt(version));
    }
    assert!(ingress.take_exact_selected(1, 2, "item-1", 1).is_none());
    assert!(ingress.take_exact_selected(1, 2, "item-20", 20).is_some());
    let state = ingress
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(u64::try_from(total_bytes(&state)).unwrap() <= maximum_bytes);
}

#[test]
fn replayed_receipt_is_not_admitted() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("a", 100));
    let mut replayed = receipt(3);
    replayed.replayed = true;
    ingress.admit_committed(pending, &replayed);
    assert!(ingress.take_exact_selected(1, 2, "a", 3).is_none());
}

#[test]
fn production_take_requires_the_global_physical_catalog_identity() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    ingress.admit_committed(ingress.pending(1, 2, &put("a", 100)), &receipt(3));

    assert!(
        ingress
            .take_exact_selected_for_generation(1, 2, "a", 3, [8; 32])
            .is_none()
    );
    assert!(
        ingress
            .take_exact_selected_for_generation(1, 2, "a", 3, [9; 32])
            .is_some()
    );
}

#[tokio::test]
async fn relevant_committed_delete_wakes_journal_reconciliation() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    let mut changes = ingress.subscribe();
    let pending = ingress.pending(1, 2, &delete("a"));
    assert!(ingress.reserved_bytes() > 0);
    assert_eq!(ingress.pending_token_len(), 1);
    let mut deleted = receipt(3);
    deleted.deleted = true;
    ingress.admit_committed(pending, &deleted);
    changes.recv().await.unwrap();
    assert_eq!(ingress.reserved_bytes(), 0);
    assert_eq!(ingress.pending_token_len(), 0);
}

#[test]
fn delete_and_no_payload_commits_invalidate_older_ready_data() {
    let ingress = HotProjectionIngress::new(4_096).unwrap();
    ingress.activate_test_route(1, 2);
    ingress.admit_committed(ingress.pending(1, 2, &put("a", 100)), &receipt(1));

    let mut deleted = receipt(2);
    deleted.deleted = true;
    ingress.admit_committed(ingress.pending(1, 2, &delete("a")), &deleted);
    assert!(ingress.take_exact_selected(1, 2, "a", 1).is_none());
    assert_eq!(ingress.slot_len(), 1, "the newer replay barrier remains");
    assert!(ingress.take_exact_selected(1, 2, "a", 2).is_none());
    assert_eq!(ingress.slot_len(), 0);

    ingress.admit_committed(ingress.pending(1, 2, &put("a", 100)), &receipt(3));
    let no_payload = ingress
        .replay_only_pending(1, 2, "a", [9; 32], Arc::from([]))
        .unwrap();
    ingress.admit_committed(Some(no_payload), &receipt(4));
    assert!(ingress.take_exact_selected(1, 2, "a", 3).is_none());
    assert_eq!(ingress.slot_len(), 1);
    assert!(ingress.take_exact_selected(1, 2, "a", 4).is_none());
    assert_eq!(ingress.slot_len(), 0);
}

#[test]
fn replacing_catalog_generation_clears_ready_and_preparing_state() {
    let ingress = HotProjectionIngress::new(8 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    ingress.admit_committed(ingress.pending(1, 2, &put("ready", 100)), &receipt(1));
    let preparing = ingress
        .register_preparing(
            ingress.pending(1, 2, &put("preparing", 100)).unwrap(),
            VersionId(2),
        )
        .unwrap();
    assert_eq!(ingress.slot_len(), 2);

    assert!(
        ingress.replace_compiled_catalog(Arc::new(PhysicalCatalogSnapshot {
            generation: 2,
            identity: [7; 32],
            recipes: Arc::from(Vec::new()),
        }))
    );
    assert_eq!(ingress.slot_len(), 0);
    assert_eq!(ingress.fifo_len(), 0);
    assert_eq!(ingress.used_bytes(), 0);
    ingress.finish_selected(preparing, selected("stale generation"));
    assert_eq!(ingress.reserved_bytes(), 0);
    assert!(ingress.take_exact_selected(1, 2, "ready", 1).is_none());
}

#[test]
fn reclaimed_selector_generations_release_unused_vector_capacity() {
    let ingress = HotProjectionIngress::new(16 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = ingress.pending(1, 2, &put("held", 100)).unwrap();

    assert!(
        ingress.replace_compiled_catalog(Arc::new(PhysicalCatalogSnapshot {
            generation: 2,
            identity: [7; 32],
            recipes: Arc::from(Vec::new()),
        }))
    );
    drop(pending);

    let mut state = ingress
        .inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    reclaim_retired_selectors(&mut state);
    assert!(state.retired_selectors.is_empty());
    assert_eq!(state.retired_selectors.capacity(), 0);
}

#[test]
fn thousand_item_bulk_admission_stays_aligned_to_exact_committed_receipts() {
    let ingress = HotProjectionIngress::new(4 * 1024 * 1024).unwrap();
    ingress.activate_test_route(1, 2);
    let pending = (0..1_000)
        .map(|index| ingress.pending(1, 2, &put(&format!("objects/{index}"), 100)))
        .collect::<Vec<_>>();
    assert!(pending.iter().all(Option::is_some));

    for (index, pending) in pending.into_iter().enumerate() {
        let mut committed = receipt(10_000 + index as u64);
        // Replayed operations have no new journal mutation and must not be
        // duplicated by the hot path. A failed operation is represented by
        // dropping its reservation, exactly as the batch caller does when
        // no successful receipt is returned.
        if index == 111 {
            committed.replayed = true;
            ingress.admit_committed(pending, &committed);
        } else if index == 777 {
            drop(pending);
        } else {
            ingress.admit_committed(pending, &committed);
        }
    }

    for index in 0..1_000 {
        let payload =
            ingress.take_exact_selected(1, 2, &format!("objects/{index}"), 10_000 + index as u64);
        assert_eq!(payload.is_some(), index != 111 && index != 777);
    }
    assert_eq!(ingress.used_bytes(), 0);
    assert_eq!(ingress.reserved_bytes(), 0);
    assert_eq!(ingress.fifo_len(), 0);
}

#[test]
fn hot_ingress_swaps_only_the_compiled_physical_router() {
    let ingress = HotProjectionIngress::new(16 * 1_024).unwrap();
    ingress.activate_test_route(1, 2);
    let first = ingress.pending(1, 2, &put("objects/a", 100)).unwrap();
    let second = ingress.pending(1, 2, &put("objects/b", 100)).unwrap();
    assert!(Arc::ptr_eq(first.pointers(), second.pointers()));
    assert!(ingress.router_bytes() > 0);
}

#[test]
fn compiled_router_obeys_segment_aware_public_prefix_semantics() {
    let mut router = BuildingBucketRouter {
        root: PrefixNode::default(),
        pointers: BTreeSet::new(),
    };
    router.insert("model", None);
    let router = CompiledBucketRouter {
        generation: [1; 32],
        root: router.root,
        pointers: Arc::from([]),
        charge: 0,
        selector_charge: 0,
    };
    assert!(router.matches("model", None));
    assert!(router.matches("model/weights", None));
    assert!(!router.matches("models/weights", None));

    let mut children = BuildingBucketRouter {
        root: PrefixNode::default(),
        pointers: BTreeSet::new(),
    };
    children.insert("model/", Some("application/json"));
    let children = CompiledBucketRouter {
        generation: [1; 32],
        root: children.root,
        pointers: Arc::from([]),
        charge: 0,
        selector_charge: 0,
    };
    assert!(!children.matches("model", Some("application/json")));
    assert!(children.matches("model/weights", Some("application/json")));
    assert!(!children.matches("model/weights", Some("text/plain")));
}
