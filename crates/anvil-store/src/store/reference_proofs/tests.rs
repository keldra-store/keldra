use rocksdb::WriteBatchIteratorCf;

use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::watch::{REFERENCE_PROOF_KEY_BYTES, offset_from_key};
use crate::{
    BatchOperation, Durability, ObjectMutationContext, PlacementLogId, PutMode, PutRequest,
    ReplicaObjectMutationApplied, StoreOptions, VersionId,
};

use super::*;

fn key(path: &str) -> ObjectKey {
    ObjectKey::new("tenant", "bucket", path).unwrap()
}

fn put(path: &str, command: &str) -> PutRequest {
    PutRequest {
        key: key(path),
        bytes: b"proof payload".to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutIfAbsent,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

fn context() -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 7, index: 9 },
        serving_fence_term: 7,
    }
}

async fn stores() -> (tempfile::TempDir, Store, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let source = Store::open(StoreOptions::new(temporary.path().join("source"), 1))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 2))
        .await
        .unwrap();
    (temporary, source, replica)
}

#[tokio::test]
async fn proof_export_enforces_record_and_byte_bounds_without_skipping() {
    let (_temporary, source, _replica) = stores().await;
    for (path, command) in [("page-a", "page-a"), ("page-b", "page-b")] {
        source
            .coordinate_object_mutation(BatchOperation::Put(put(path, command)), context())
            .await
            .unwrap();
    }

    let first = source
        .export_reference_proofs(None, 1, MAX_REFERENCE_PROOF_EXPORT_BYTES)
        .unwrap();
    assert_eq!(first.proofs.len(), 1);
    let cursor = first.next_cursor.as_ref().expect("one record truncated the page");
    let second = source
        .export_reference_proofs(Some(cursor), 1, MAX_REFERENCE_PROOF_EXPORT_BYTES)
        .unwrap();
    assert_eq!(second.proofs.len(), 1);
    assert!(second.next_cursor.is_none());
    assert_ne!(first.proofs[0], second.proofs[0]);

    let required = serde_json::to_vec(&first.proofs[0]).unwrap().len() as u64;
    assert_eq!(
        source.export_reference_proofs(None, 1, required - 1),
        Err(ReferenceProofExportError::RecordTooLarge {
            required_bytes: required,
        })
    );
    assert_eq!(
        source.export_reference_proofs(None, 1, 0),
        Err(ReferenceProofExportError::InvalidLimits)
    );
}

fn wal_batches_since(store: &Store, sequence: u64) -> usize {
    store
        .db
        .get_updates_since(sequence)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

#[derive(Default)]
struct WalPuts(Vec<Vec<u8>>);

impl WriteBatchIteratorCf for WalPuts {
    fn put_cf(&mut self, _cf_id: u32, key: &[u8], _value: &[u8]) {
        self.0.push(key.to_vec());
    }

    fn delete_cf(&mut self, _cf_id: u32, _key: &[u8]) {}

    fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {}
}

fn wal_put_batches_since(store: &Store, sequence: u64) -> Vec<Vec<Vec<u8>>> {
    store
        .db
        .get_updates_since(sequence)
        .unwrap()
        .map(|entry| {
            let (_, batch) = entry.unwrap();
            let mut puts = WalPuts::default();
            batch.iterate_cf(&mut puts);
            puts.0
        })
        .collect()
}

#[tokio::test]
async fn source_and_replica_store_exact_evidence_in_the_mutation_batch() {
    let (_temporary, source, replica) = stores().await;
    source.resolve_bucket_identity("tenant", "bucket").unwrap();
    let source_sequence = source.db.latest_sequence_number();
    let coordinated = source
        .coordinate_object_mutation(
            BatchOperation::Put(put("atomic", "atomic-source")),
            context(),
        )
        .await
        .unwrap();
    let mutation = coordinated.mutation.unwrap();
    let source_proof = source
        .read_reference_proof(
            mutation.stamp.source_id,
            mutation.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap();
    assert_eq!(source_proof.source_id, mutation.stamp.source_id);
    assert_eq!(
        source_proof.mutation_fingerprint,
        mutation.stamp.mutation_fingerprint
    );
    assert_eq!(
        source_proof.change,
        source
            .read_local_change(mutation.stamp.source_journal_position)
            .unwrap()
            .unwrap()
    );
    let identity = BucketIdentity {
        tenant_id: TenantId(mutation.tenant_id),
        bucket_id: BucketId(mutation.bucket_id),
    };
    let head_key = identity.head_key(&mutation.exact_path);
    let proof_key = reference_proof_key(
        mutation.stamp.source_id,
        mutation.stamp.source_journal_position,
    );
    let journal_key = invalidation_key(mutation.stamp.source_journal_position);
    assert!(
        wal_put_batches_since(&source, source_sequence)
            .iter()
            .any(|puts| puts.contains(&head_key)
                && puts.contains(&proof_key.to_vec())
                && puts.contains(&journal_key.to_vec()))
    );

    let replica_sequence = replica.db.latest_sequence_number();
    assert_eq!(
        replica
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap(),
        ReplicaObjectMutationApplied {
            version: mutation.version.id,
            replayed: false,
        }
    );
    let replica_batches = wal_put_batches_since(&replica, replica_sequence);
    assert_eq!(replica_batches.len(), 1);
    assert!(replica_batches[0].contains(&head_key));
    assert!(replica_batches[0].contains(&proof_key.to_vec()));
    assert_eq!(
        replica
            .read_reference_proof(
                mutation.stamp.source_id,
                mutation.stamp.source_journal_position,
            )
            .unwrap(),
        Some(source_proof)
    );
    assert_eq!(replica.local_watch_status().unwrap().tail, 0);
    assert!(
        replica
            .read_local_change(mutation.stamp.source_journal_position)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exact_replay_restores_a_missing_proof_and_is_otherwise_a_no_op() {
    let (_temporary, source, replica) = stores().await;
    let request = put("replay", "replay-source");
    let first = source
        .coordinate_object_mutation(BatchOperation::Put(request.clone()), context())
        .await
        .unwrap();
    let mutation = first.mutation.unwrap();
    replica
        .apply_object_mutation_replica(&mutation)
        .await
        .unwrap();
    let proof = replica
        .read_reference_proof(
            mutation.stamp.source_id,
            mutation.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap();

    assert!(
        replica
            .delete_reference_proof_if_matches(&proof)
            .await
            .unwrap()
    );
    let before_restore = replica.db.latest_sequence_number();
    assert!(
        replica
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(wal_batches_since(&replica, before_restore), 1);
    assert_eq!(
        replica
            .read_reference_proof(proof.source_id, proof.offset())
            .unwrap(),
        Some(proof.clone())
    );
    let after_restore = replica.db.latest_sequence_number();
    assert!(
        replica
            .apply_object_mutation_replica(&mutation)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(replica.db.latest_sequence_number(), after_restore);

    let source_proof = source
        .read_reference_proof(proof.source_id, proof.offset())
        .unwrap()
        .unwrap();
    assert!(
        source
            .delete_reference_proof_if_matches(&source_proof)
            .await
            .unwrap()
    );
    let replayed = source
        .coordinate_object_mutation(BatchOperation::Put(request), context())
        .await
        .unwrap();
    assert!(replayed.receipt.replayed);
    assert_eq!(replayed.mutation, Some(mutation));
    assert_eq!(
        source
            .read_reference_proof(proof.source_id, proof.offset())
            .unwrap(),
        Some(source_proof)
    );
}

#[tokio::test]
async fn sibling_minority_proofs_remain_exactly_distinguishable() {
    let (_temporary, source, replica) = stores().await;
    let first = source
        .coordinate_object_mutation(
            BatchOperation::Put(put("minority", "first-minority")),
            context(),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    let mut sibling = first.clone();
    sibling.command_id = "second-minority".into();
    sibling.input_fingerprint = [23; 32];
    sibling.version.id = VersionId(first.version.id.0 + 1);
    sibling.version.committed_at_unix_millis += 1;
    sibling.stamp.source_id = SourceId {
        node_id: 2,
        source_epoch: [29; 32],
    };
    sibling.stamp.source_journal_position = 1;
    sibling.set_computed_fingerprint();
    sibling.validate().unwrap();
    replica
        .apply_object_mutation_replica(&sibling)
        .await
        .unwrap();

    let first_proof = source
        .read_reference_proof(first.stamp.source_id, first.stamp.source_journal_position)
        .unwrap()
        .unwrap();
    let sibling_proof = replica
        .read_reference_proof(
            sibling.stamp.source_id,
            sibling.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap();
    assert_ne!(first_proof, sibling_proof);
    assert!(
        source
            .read_reference_proof(
                sibling.stamp.source_id,
                sibling.stamp.source_journal_position,
            )
            .unwrap()
            .is_none()
    );
    assert!(
        replica
            .read_reference_proof(first.stamp.source_id, first.stamp.source_journal_position)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn malformed_mismatched_and_changed_proofs_fail_closed() {
    let (_temporary, source, replica) = stores().await;
    let mutation = source
        .coordinate_object_mutation(
            BatchOperation::Put(put("reject", "reject-proof")),
            context(),
        )
        .await
        .unwrap()
        .mutation
        .unwrap();
    let proof = source
        .read_reference_proof(
            mutation.stamp.source_id,
            mutation.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap();

    let malformed_source = SourceId {
        node_id: 11,
        source_epoch: [12; 32],
    };
    source
        .db
        .put_cf(
            source.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            reference_proof_key(malformed_source, 7),
            b"not-json",
        )
        .unwrap();
    assert!(source.read_reference_proof(malformed_source, 7).is_err());

    let mismatched_source = SourceId {
        node_id: 13,
        source_epoch: [14; 32],
    };
    source
        .db
        .put_cf(
            source.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            reference_proof_key(mismatched_source, proof.offset()),
            encode_reference_proof(&proof).unwrap(),
        )
        .unwrap();
    assert!(
        source
            .read_reference_proof(mismatched_source, proof.offset())
            .is_err()
    );
    source
        .db
        .put_cf(
            source.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            reference_proof_key(proof.source_id, proof.offset() + 1),
            encode_reference_proof(&proof).unwrap(),
        )
        .unwrap();
    assert!(
        source
            .read_reference_proof(proof.source_id, proof.offset() + 1)
            .is_err()
    );

    let mut changed = proof.clone();
    changed.mutation_fingerprint[0] ^= 1;
    assert!(
        !source
            .delete_reference_proof_if_matches(&changed)
            .await
            .unwrap()
    );
    assert_eq!(
        source
            .read_reference_proof(proof.source_id, proof.offset())
            .unwrap(),
        Some(proof.clone())
    );

    let mut wrong_path = proof.clone();
    let LocalChange::ObjectHead(path_change) = &mut wrong_path.change else {
        unreachable!()
    };
    path_change.exact_path = "different-path".into();
    let mut wrong_version = proof.clone();
    let LocalChange::ObjectHead(version_change) = &mut wrong_version.change else {
        unreachable!()
    };
    version_change.path_version = VersionId(version_change.path_version.0 + 1);
    let mut wrong_deltas = proof.clone();
    let LocalChange::ObjectHead(delta_change) = &mut wrong_deltas.change else {
        unreachable!()
    };
    delta_change.reference_deltas[0].change = -1;
    for mismatch in [changed, wrong_path, wrong_version, wrong_deltas] {
        replica
            .db
            .put_cf(
                replica.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                reference_proof_key(proof.source_id, proof.offset()),
                encode_reference_proof(&mismatch).unwrap(),
            )
            .unwrap();
        assert_eq!(
            replica
                .apply_object_mutation_replica(&mutation)
                .await
                .unwrap_err(),
            MutationError::ObjectMutationConflict
        );
    }
    let identity = BucketIdentity {
        tenant_id: TenantId(mutation.tenant_id),
        bucket_id: BucketId(mutation.bucket_id),
    };
    assert!(
        replica
            .head_by_storage_key(&identity.head_key(&mutation.exact_path))
            .unwrap()
            .is_none()
    );
}

#[test]
fn proof_keys_are_versioned_namespaced_and_fixed_width() {
    let source = SourceId {
        node_id: u16::MAX,
        source_epoch: [0xab; 32],
    };
    let first = reference_proof_key(source, 1);
    let last = reference_proof_key(source, u64::MAX);
    assert_eq!(first.len(), REFERENCE_PROOF_KEY_BYTES);
    assert_eq!(last.len(), REFERENCE_PROOF_KEY_BYTES);
    assert_eq!(&first[..2], &[STORAGE_KEY_FORMAT_VERSION, 0xff]);
    assert_eq!(&first[2..4], &u16::MAX.to_be_bytes());
    assert_eq!(&first[4..36], &[0xab; 32]);
    assert_eq!(&first[36..], &1_u64.to_be_bytes());
    assert_eq!(&last[36..], &u64::MAX.to_be_bytes());
    assert!(offset_from_key(&first).is_none());
}
