use rocksdb::WriteBatchIteratorCf;

use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::watch::{REFERENCE_PROOF_KEY_BYTES, offset_from_key};
use crate::{
    BatchOperation, DestinationReferenceArtifact, DestinationReferenceDelta, Durability,
    ObjectMutationContext, PlacementLogId, PutMode, PutRequest, ReferenceDeltaBatch,
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
async fn unversioned_source_version_retires_only_with_its_checkpointed_journal_record() {
    let (_temporary, source, _replica) = stores().await;
    let first = source
        .coordinate_object_mutation(BatchOperation::Put(put("retained", "first")), context())
        .await
        .unwrap()
        .mutation
        .unwrap();
    let first_after = source
        .reference_delta_cursor(first.stamp.source_id)
        .unwrap();
    assert_eq!(first_after + 1, first.stamp.source_journal_position);
    source
        .apply_reference_deltas(ReferenceDeltaBatch {
            source: first.stamp.source_id,
            // Sealing the distributed payload is itself a locally consumed
            // lifecycle record. Consume only the subsequent object-head
            // effect described by this mutation proof.
            after: first_after,
            through: first.stamp.source_journal_position,
            deltas: first
                .reference_deltas
                .iter()
                .map(|delta| DestinationReferenceDelta {
                    artifact: DestinationReferenceArtifact::CompleteBlob(delta.blob.clone()),
                    change: delta.change,
                })
                .collect(),
        })
        .await
        .unwrap();
    let mut replacement = put("retained", "second");
    replacement.mode = PutMode::Put;
    let second = source
        .coordinate_object_mutation(BatchOperation::Put(replacement), context())
        .await
        .unwrap()
        .mutation
        .unwrap();
    let second_after = source
        .reference_delta_cursor(second.stamp.source_id)
        .unwrap();
    assert!(second_after < second.stamp.source_journal_position);
    source
        .apply_reference_deltas(ReferenceDeltaBatch {
            source: second.stamp.source_id,
            // The range also covers intervening destination-lifecycle and
            // payload-seal records. They carry no source reference effect, so
            // this one contiguous delivery contains only the head delta.
            after: second_after,
            through: second.stamp.source_journal_position,
            deltas: second
                .reference_deltas
                .iter()
                .map(|delta| DestinationReferenceDelta {
                    artifact: DestinationReferenceArtifact::CompleteBlob(delta.blob.clone()),
                    change: delta.change,
                })
                .collect(),
        })
        .await
        .unwrap();

    let identity = BucketIdentity {
        tenant_id: TenantId(first.tenant_id),
        bucket_id: BucketId(first.bucket_id),
    };
    let retained_key = key("retained");
    assert!(
        source
            .version_metadata_by_identity(identity, &retained_key, first.version.id)
            .unwrap()
            .is_some()
    );
    source
        .advance_source_journal_reference_safe_through(second.stamp.source_journal_position)
        .await
        .unwrap();
    source
        .advance_source_journal_settled_through(second.stamp.source_journal_position)
        .await
        .unwrap();
    assert!(source.prune_source_journal_for_capacity().await.unwrap());
    assert!(source.prune_source_journal_for_capacity().await.unwrap());
    assert!(
        source
            .version_metadata_by_identity(identity, &retained_key, first.version.id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        source
            .head_by_storage_key(&identity.head_key("retained"))
            .unwrap()
            .unwrap()
            .version,
        second.version.id
    );
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
    let cursor = first
        .next_cursor
        .as_ref()
        .expect("one record truncated the page");
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
    assert_eq!(
        source_proof.mutation,
        ReferenceProofMutation::Object(mutation.clone())
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
    let source_batches = wal_put_batches_since(&source, source_sequence);
    let mutation_batch = source_batches
        .iter()
        .find(|puts| {
            puts.contains(&head_key)
                && puts.contains(&proof_key.to_vec())
                && puts.contains(&journal_key.to_vec())
        })
        .expect("source metadata and proof share one batch");
    assert!(!mutation_batch.contains(&LOCAL_INVALIDATION_SETTLED_KEY.to_vec()));

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
        source
            .delete_reference_proof_if_matches(&changed)
            .await
            .is_err()
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
        assert!(
            encode_reference_proof(&mismatch).is_err(),
            "the durable encoder must reject contradictory typed evidence"
        );
    }

    let valid = encode_reference_proof(&proof).unwrap();
    let mut bad_magic = valid.clone();
    bad_magic[0] ^= 1;
    let mut bad_kind = valid.clone();
    bad_kind[6] = 99;
    let mut bad_reserved = valid.clone();
    bad_reserved[7] = 1;
    let mut truncated = valid;
    truncated.pop();
    for malformed in [bad_magic, bad_kind, bad_reserved, truncated] {
        replica
            .db
            .put_cf(
                replica.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
                reference_proof_key(proof.source_id, proof.offset()),
                malformed,
            )
            .unwrap();
        assert!(
            replica
                .apply_object_mutation_replica(&mutation)
                .await
                .is_err()
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

async fn coordinated_proof(store: &Store, path: &str, command: &str) -> ReferenceProof {
    let mutation = store
        .coordinate_object_mutation(BatchOperation::Put(put(path, command)), context())
        .await
        .unwrap()
        .mutation
        .unwrap();
    store
        .read_reference_proof(
            mutation.stamp.source_id,
            mutation.stamp.source_journal_position,
        )
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn object_proof_value_has_a_versioned_binary_envelope_and_round_trips() {
    let (_temporary, source, _replica) = stores().await;
    let proof = coordinated_proof(&source, "codec/object", "codec-object").await;
    let encoded = encode_reference_proof(&proof).unwrap();

    assert_eq!(&encoded[..4], REFERENCE_PROOF_MAGIC);
    assert_eq!(
        u16::from_be_bytes(encoded[4..6].try_into().unwrap()),
        REFERENCE_PROOF_VALUE_FORMAT
    );
    assert_eq!(encoded[6], OBJECT_MUTATION_PROOF);
    assert_eq!(encoded[7], 0);
    assert_eq!(
        usize::try_from(u32::from_be_bytes(encoded[8..12].try_into().unwrap())).unwrap(),
        encoded.len() - REFERENCE_PROOF_HEADER_BYTES
    );
    assert_eq!(&encoded[REFERENCE_PROOF_HEADER_BYTES..][..4], b"KOMU");
    assert_eq!(decode_reference_proof(&encoded).unwrap(), proof);

    let legacy_json = serde_json::to_vec(&proof).unwrap();
    assert!(encoded.len() < legacy_json.len());
    assert!(decode_reference_proof(&legacy_json).is_err());
}

#[tokio::test]
async fn malformed_proof_envelopes_fail_closed() {
    let (_temporary, source, _replica) = stores().await;
    let proof = coordinated_proof(&source, "codec/reject", "codec-reject").await;
    let encoded = encode_reference_proof(&proof).unwrap();

    let mutations: [fn(&mut Vec<u8>); 5] = [
        |bytes| bytes[0] ^= 1,
        |bytes| bytes[5] = bytes[5].wrapping_add(1),
        |bytes| bytes[6] = 99,
        |bytes| bytes[7] = 1,
        |bytes| bytes[11] = bytes[11].wrapping_add(1),
    ];
    for mutation in mutations {
        let mut corrupt = encoded.clone();
        mutation(&mut corrupt);
        assert!(decode_reference_proof(&corrupt).is_err());
    }

    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(decode_reference_proof(&truncated).is_err());
    assert!(decode_reference_proof(&[]).is_err());
}

#[tokio::test]
async fn batched_proof_staging_preserves_idempotency_and_conflict_atomicity() {
    let (_temporary, source, replica) = stores().await;
    let proofs = [
        coordinated_proof(&source, "batch/first", "batch-first").await,
        coordinated_proof(&source, "batch/second", "batch-second").await,
        coordinated_proof(&source, "batch/third", "batch-third").await,
        coordinated_proof(&source, "batch/fourth", "batch-fourth").await,
    ];
    let mutations = proofs
        .iter()
        .map(|proof| match &proof.mutation {
            ReferenceProofMutation::Object(mutation) => mutation,
            _ => unreachable!("coordinated puts produce object-mutation proofs"),
        })
        .collect::<Vec<_>>();

    let mut initial = WriteBatch::default();
    replica
        .stage_object_mutation_reference_proofs(&mut initial, &mutations[..2])
        .unwrap();
    replica.db.write(initial).unwrap();
    for proof in &proofs[..2] {
        assert_eq!(
            replica
                .read_reference_proof(proof.source_id, proof.offset())
                .unwrap(),
            Some(proof.clone())
        );
    }

    let mut replay = WriteBatch::default();
    replica
        .stage_object_mutation_reference_proofs(&mut replay, &mutations[..2])
        .unwrap();
    assert!(replay.is_empty());

    let mut conflicting = (*mutations[3]).clone();
    conflicting.command_id = "conflicting-fourth".into();
    conflicting.input_fingerprint = [37; 32];
    conflicting.set_computed_fingerprint();
    conflicting.validate().unwrap();
    let mut seed = WriteBatch::default();
    replica
        .stage_object_mutation_reference_proof(&mut seed, &conflicting)
        .unwrap();
    replica.db.write(seed).unwrap();

    let mut rejected = WriteBatch::default();
    assert_eq!(
        replica.stage_object_mutation_reference_proofs(&mut rejected, &mutations[2..]),
        Err(MutationError::ObjectMutationConflict)
    );
    assert!(!rejected.is_empty());
    drop(rejected);
    assert!(
        replica
            .read_reference_proof(proofs[2].source_id, proofs[2].offset())
            .unwrap()
            .is_none(),
        "a staged prefix remains invisible when the rejected batch is dropped"
    );
    assert_eq!(
        replica
            .read_reference_proof(proofs[3].source_id, proofs[3].offset())
            .unwrap(),
        Some(proof_for_mutation(&conflicting).unwrap())
    );
}

#[tokio::test]
async fn prune_is_source_scoped_and_through_inclusive() {
    let temporary = tempfile::tempdir().unwrap();
    let first_source = Store::open(StoreOptions::new(temporary.path().join("first"), 1))
        .await
        .unwrap();
    let second_source = Store::open(StoreOptions::new(temporary.path().join("second"), 2))
        .await
        .unwrap();
    let replica = Store::open(StoreOptions::new(temporary.path().join("replica"), 3))
        .await
        .unwrap();
    let first = coordinated_proof(&first_source, "first/1", "first-1").await;
    let boundary = coordinated_proof(&first_source, "first/2", "first-2").await;
    let after = coordinated_proof(&first_source, "first/3", "first-3").await;
    let other = coordinated_proof(&second_source, "second/1", "second-1").await;
    for proof in [&first, &boundary, &after, &other] {
        replica
            .install_quorum_reconciled_reference_proof(proof)
            .await
            .unwrap();
    }

    let result = replica
        .prune_reference_proofs(
            first.source_id,
            boundary.offset(),
            MAX_REFERENCE_PROOF_PRUNE_RECORDS,
            MAX_REFERENCE_PROOF_PRUNE_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(result.deleted_records, 2);
    assert!(result.complete);
    assert!(
        replica
            .read_reference_proof(first.source_id, first.offset())
            .unwrap()
            .is_none()
    );
    assert!(
        replica
            .read_reference_proof(boundary.source_id, boundary.offset())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        replica
            .read_reference_proof(after.source_id, after.offset())
            .unwrap(),
        Some(after)
    );
    assert_eq!(
        replica
            .read_reference_proof(other.source_id, other.offset())
            .unwrap(),
        Some(other)
    );
}

#[tokio::test]
async fn prune_pages_resume_after_reopen_without_a_persisted_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("paged");
    let store = Store::open(StoreOptions::new(&path, 1)).await.unwrap();
    let proofs = vec![
        coordinated_proof(&store, "paged/1", "paged-1").await,
        coordinated_proof(&store, "paged/2", "paged-2").await,
        coordinated_proof(&store, "paged/3", "paged-3").await,
    ];
    let source = proofs[0].source_id;
    let through = proofs[2].offset();
    let required_bytes = REFERENCE_PROOF_KEY_BYTES as u64
        + u64::try_from(encode_reference_proof(&proofs[0]).unwrap().len()).unwrap();
    assert_eq!(
        store
            .prune_reference_proofs(source, through, 1, required_bytes - 1)
            .await,
        Err(ReferenceProofPruneError::RecordTooLarge { required_bytes })
    );
    assert_eq!(
        store
            .prune_reference_proofs(source, through, 0, required_bytes)
            .await,
        Err(ReferenceProofPruneError::InvalidLimits)
    );

    let first_page = store
        .prune_reference_proofs(source, through, 1, MAX_REFERENCE_PROOF_PRUNE_BYTES)
        .await
        .unwrap();
    assert_eq!(first_page.deleted_records, 1);
    assert!(!first_page.complete);
    drop(store);

    let reopened = Store::open(StoreOptions::new(&path, 1)).await.unwrap();
    let second_page = reopened
        .prune_reference_proofs(source, through, 1, MAX_REFERENCE_PROOF_PRUNE_BYTES)
        .await
        .unwrap();
    assert_eq!(second_page.deleted_records, 1);
    assert!(!second_page.complete);
    let final_page = reopened
        .prune_reference_proofs(source, through, 1, MAX_REFERENCE_PROOF_PRUNE_BYTES)
        .await
        .unwrap();
    assert_eq!(final_page.deleted_records, 1);
    assert!(final_page.complete);
    assert_eq!(
        reopened
            .prune_reference_proofs(source, through, 1, MAX_REFERENCE_PROOF_PRUNE_BYTES)
            .await
            .unwrap(),
        ReferenceProofPruneResult {
            complete: true,
            ..ReferenceProofPruneResult::default()
        }
    );
}

#[tokio::test]
async fn malformed_eligible_proof_aborts_the_whole_prune_page() {
    let (_temporary, source, _replica) = stores().await;
    let first = coordinated_proof(&source, "malformed/1", "malformed-1").await;
    let second = coordinated_proof(&source, "malformed/2", "malformed-2").await;
    source
        .db
        .put_cf(
            source.cf(CF_LOCAL_INVALIDATIONS).unwrap(),
            reference_proof_key(second.source_id, second.offset()),
            b"not-a-reference-proof",
        )
        .unwrap();

    assert!(matches!(
        source
            .prune_reference_proofs(
                first.source_id,
                second.offset(),
                MAX_REFERENCE_PROOF_PRUNE_RECORDS,
                MAX_REFERENCE_PROOF_PRUNE_BYTES,
            )
            .await,
        Err(ReferenceProofPruneError::Storage(_))
    ));
    assert_eq!(
        source
            .read_reference_proof(first.source_id, first.offset())
            .unwrap(),
        Some(first)
    );
    assert!(
        source
            .read_reference_proof(second.source_id, second.offset())
            .is_err()
    );
}

#[derive(Default)]
struct WalMutations {
    puts: Vec<Vec<u8>>,
    deletes: Vec<Vec<u8>>,
}

impl WriteBatchIteratorCf for WalMutations {
    fn put_cf(&mut self, _cf_id: u32, key: &[u8], _value: &[u8]) {
        self.puts.push(key.to_vec());
    }

    fn delete_cf(&mut self, _cf_id: u32, key: &[u8]) {
        self.deletes.push(key.to_vec());
    }

    fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {}
}

fn column_family_snapshot(store: &Store, name: &'static str) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .db
        .iterator_cf(store.cf(name).unwrap(), IteratorMode::Start)
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

#[tokio::test]
async fn prune_writes_only_proof_deletes_and_no_timestamp_or_side_state() {
    let (_temporary, source, _replica) = stores().await;
    let proof = coordinated_proof(&source, "side-state", "side-state").await;
    let metadata_before = column_family_snapshot(&source, CF_METADATA);
    let journal_status_before = source.local_watch_status().unwrap();
    let sequence = source.db.latest_sequence_number();

    let result = source
        .prune_reference_proofs(
            proof.source_id,
            proof.offset(),
            MAX_REFERENCE_PROOF_PRUNE_RECORDS,
            MAX_REFERENCE_PROOF_PRUNE_BYTES,
        )
        .await
        .unwrap();
    assert_eq!(result.deleted_records, 1);
    assert!(result.complete);
    assert_eq!(
        column_family_snapshot(&source, CF_METADATA),
        metadata_before
    );
    assert_eq!(source.local_watch_status().unwrap(), journal_status_before);

    let batches = source
        .db
        .get_updates_since(sequence)
        .unwrap()
        .map(|entry| {
            let (_, batch) = entry.unwrap();
            let mut mutations = WalMutations::default();
            batch.iterate_cf(&mut mutations);
            mutations
        })
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    assert!(batches[0].puts.is_empty());
    assert_eq!(
        batches[0].deletes,
        vec![reference_proof_key(proof.source_id, proof.offset()).to_vec()]
    );
}
