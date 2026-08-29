use keldra_atomic_program::{
    Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
    DocumentValueRef, DocumentView, ExpectedHead, InputValue, IntegerType, InvocationContext,
    JsonPointerRef, Operation, PathBinding, PathTemplate, ProgramCaps, ProgramInvocation,
    ReturnDefinition, ValueSource,
};
use serde_json::json;
use tempfile::TempDir;

use super::distributed::PROGRAM_PATH_STAGE_FORMAT;
use super::*;
use crate::{
    BucketPolicy, DeleteRetainedVersionOutcome, DestinationReferenceArtifact,
    DestinationReferenceDelta, Durability, LocalChange, LogicalRecordCandidate,
    LogicalRecordMutationContext, LogicalRecordValue, ObjectMutationContext,
    ObjectMutationGovernance, ObjectVersioning, PlacementLogId, PutMode, PutRequest,
    ReferenceDeltaBatch, StoreOptions, WatchRetention,
};

fn counter_path() -> ObjectPath {
    ObjectPath::new("tenant", "bucket", "managed/counter").unwrap()
}

fn mutation_context() -> ObjectMutationContext {
    ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 3, index: 7 },
        serving_fence_term: 3,
    }
}

fn definition() -> ProgramDefinition {
    let counter = DocumentRef::one("counter");
    ProgramDefinition {
        schema_version: DEFINITION_SCHEMA_VERSION,
        documents: vec![DocumentSpec {
            name: "counter".into(),
            path: PathTemplate::new("{tenant}", "bucket", "managed/counter"),
            cardinality: Cardinality::One,
            access: DocumentAccess::ReadWrite,
            allow_initial_json: true,
        }],
        assertions: Vec::new(),
        operations: vec![Operation::CheckedIntegerAdd {
            target: JsonPointerRef::new(counter.clone(), "/value"),
            delta: InputValue::Input {
                name: "delta".into(),
            },
            numeric_type: IntegerType::I64 {
                min: Some(0),
                max: None,
            },
        }],
        returns: vec![ReturnDefinition {
            name: "value".into(),
            value: DocumentValueRef {
                value: JsonPointerRef::new(counter.clone(), "/value"),
                view: DocumentView::Current,
            },
        }],
        caps: ProgramCaps {
            max_paths: 1,
            max_writes: 1,
            max_operations: 2,
            max_input_bytes: 64 * 1024,
            max_document_bytes: 64 * 1024,
        },
    }
}

fn invocation(command: &str, expected_head: ExpectedHead) -> ProgramInvocation {
    ProgramInvocation {
        program_path_hash: [0x11; 32],
        command_id: command.into(),
        input_fingerprint: hex::encode(blake3::hash(command.as_bytes()).as_bytes()),
        arguments: Default::default(),
        inputs: [("delta".into(), json!(1))].into_iter().collect(),
        blobs: Default::default(),
        bindings: [(
            "counter".into(),
            vec![PathBinding {
                path: counter_path(),
                template_values: Default::default(),
                expected_head,
                initial_json: Some(json!({"value": 0})),
            }],
        )]
        .into_iter()
        .collect(),
    }
}

fn verified_definition() -> VerifiedProgramDefinition {
    let bytes = serde_json::to_vec(&definition()).unwrap();
    VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash::for_definition_bytes(&bytes))
        .unwrap()
}

async fn configured_store() -> (TempDir, Store, VerifiedProgramDefinition) {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    configure_policy(&store).await;
    (temporary, store, verified_definition())
}

#[tokio::test]
async fn descriptor_mime_without_target_sidecar_provenance_remains_an_ordinary_object() {
    let (_temporary, store, _) = configured_store().await;
    let requested = ObjectPath::new("tenant", "bucket", "historical/descriptor-shaped").unwrap();
    let descriptor = crate::ObjectLinkDescriptor::new("historical/target").unwrap();
    let receipt = store
        .put(PutRequest {
            key: object_key(&requested).unwrap(),
            bytes: descriptor.encode(),
            content_type: Some(crate::OBJECT_LINK_CONTENT_TYPE.into()),
            mode: PutMode::Put,
            command_id: Some("historical-descriptor-mime".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let bindings = store
        .resolve_program_alias_bindings(&[ExpandedProgramPath {
            path: requested.clone(),
            intent: keldra_atomic_program::ProgramPathIntent {
                get: true,
                put: false,
                delete: false,
            },
        }])
        .await
        .unwrap();

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].requested_path, requested);
    assert_eq!(bindings[0].canonical_path, requested);
    assert!(bindings[0].descriptor_version.is_none());
    assert_eq!(
        bindings[0].canonical_version.as_ref().map(|v| v.id),
        Some(receipt.version)
    );
}

async fn configure_policy(store: &Store) {
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                program_only_prefixes: vec!["managed".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

fn install_versioned_governance(store: &Store) {
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    for value in [
        LogicalRecordValue::BucketPolicy {
            tenant_id,
            bucket_id,
            policy: BucketPolicy {
                program_only_prefixes: vec!["managed".into()],
                ..Default::default()
            },
        },
        LogicalRecordValue::BucketOptions {
            tenant_id,
            bucket_id,
            versioning: ObjectVersioning::Unversioned,
        },
    ] {
        let id = value.id();
        let mutation = store
            .construct_logical_record_mutation(
                value,
                LogicalRecordMutationContext {
                    record_version: store.allocate_logical_record_version().unwrap(),
                    active_placement_log_id: PlacementLogId { term: 3, index: 7 },
                    serving_fence_term: 3,
                },
            )
            .unwrap();
        store.commit_logical_record_mutation(&mutation).unwrap();
        assert!(matches!(
            store.logical_record_candidate(&id).unwrap(),
            Some(LogicalRecordCandidate::Versioned(_))
        ));
    }
}

fn reserved_program_path_attempt(
    command_id: &str,
    expected_head: ExpectedHead,
    operation: Operation,
) -> (VerifiedProgramDefinition, ProgramInvocation) {
    let target = ObjectPath::new("tenant", "bucket", "_keldra/programs/victim@1").unwrap();
    let mut definition = definition();
    definition.documents[0].path =
        PathTemplate::new("{tenant}", "bucket", "_keldra/programs/victim@1");
    definition.documents[0].allow_initial_json = false;
    definition.operations = vec![operation];
    definition.returns.clear();
    let bytes = serde_json::to_vec(&definition).unwrap();
    let verified =
        VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash::for_definition_bytes(&bytes))
            .unwrap();

    let mut invocation = invocation(command_id, expected_head);
    let binding = &mut invocation.bindings.get_mut("counter").unwrap()[0];
    binding.path = target;
    binding.initial_json = None;
    (verified, invocation)
}

async fn snapshot(store: &Store) -> ProgramSnapshot {
    StateReader::read_snapshot(store, &[counter_path()])
        .await
        .unwrap()
}

fn commit(
    prepared: &PreparedProgramBundle,
    previous_commit_cursor: Option<u64>,
    commit_cursor: u64,
) -> ProgramCommit {
    ProgramCommit {
        previous_commit_cursor,
        commit_cursor,
        begin_cursor: commit_cursor,
        bundle_ref: prepared.bundle,
        bundle_hash: prepared.hash,
        program_hash: prepared.program_hash,
        authority: prepared.authority,
        participant_manifest_hash: prepared.participant_manifest_hash,
        durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
        durability_evidence_hash: prepared.durability_evidence_hash,
    }
}

async fn commit_prepared_reservations(
    store: &Store,
    prepared: &PreparedProgramBundle,
    commit: &ProgramCommit,
) -> Vec<ProgramReservation> {
    let record = store.prepared_program_record(prepared).await.unwrap();
    let context = mutation_context();
    let reservations = record
        .reservations(
            commit.begin_cursor,
            [0x51; 32],
            prepared.hash,
            1,
            context.serving_fence_term,
            context.active_placement_log_id,
        )
        .unwrap();
    for reservation in &reservations {
        store
            .reserve_program_participant(reservation)
            .await
            .unwrap();
        store
            .commit_program_participant(reservation, commit.commit_cursor)
            .await
            .unwrap();
    }
    reservations
}

#[tokio::test]
async fn ordinary_blob_plane_attests_executor_local_durability() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    configure_policy(&store).await;
    let engine = store.program_engine(&verified_definition()).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("local", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();

    assert!(matches!(
        &prepared.durability.scope,
        ProgramDurabilityScope::ExecutorLocal {
            node_id: 1,
            synced: true
        }
    ));
    assert_eq!(
        prepared.remote_durability_evidence_hash().unwrap_err(),
        ProgramStoreError::ExecutorLocalDurability
    );

    let wrong_class = ProgramCommit {
        previous_commit_cursor: None,
        commit_cursor: 1,
        begin_cursor: 1,
        bundle_ref: prepared.bundle,
        bundle_hash: prepared.hash,
        program_hash: prepared.program_hash,
        authority: prepared.authority,
        participant_manifest_hash: prepared.participant_manifest_hash,
        durability_class: ProgramDurabilityClassHash::for_class("replicated"),
        durability_evidence_hash: prepared.durability_evidence_hash,
    };
    assert_eq!(
        verify_prepared_commit(&prepared, &wrong_class).unwrap_err(),
        ProgramStoreError::DurabilityClassMismatch
    );

    let local_commit = ProgramCommit {
        durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
        ..wrong_class
    };
    let _reservations = commit_prepared_reservations(&store, &prepared, &local_commit).await;
    let before_apply = store.db.latest_sequence_number();
    let applied = store
        .apply_program_bundle(lease, &prepared, local_commit.clone(), mutation_context())
        .await
        .unwrap();
    let apply_batches = store
        .db
        .get_updates_since(before_apply)
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(apply_batches.len(), 1);
    assert_eq!(
        store.applied_program_commit().unwrap(),
        Some(AppliedProgramCommit {
            commit_cursor: 1,
            bundle_ref: prepared.bundle,
            bundle_hash: prepared.hash,
            program_hash: prepared.program_hash,
            authority: prepared.authority,
            participant_manifest_hash: prepared.participant_manifest_hash,
            durability_class: local_commit.durability_class,
            durability_evidence_hash: prepared.durability_evidence_hash,
        })
    );
    let marker = store
        .raw_get(CF_METADATA, APPLIED_PROGRAM_COMMIT_KEY)
        .unwrap()
        .unwrap();
    let marker = serde_json::from_slice::<serde_json::Value>(&marker).unwrap();
    let marker_fields = marker
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        marker_fields,
        BTreeSet::from([
            "authority",
            "bundle_hash",
            "bundle_ref",
            "commit_cursor",
            "durability_class",
            "durability_evidence_hash",
            "participant_manifest_hash",
            "program_hash",
        ])
    );
    let applied_key = object_key(&counter_path()).unwrap();
    let applied_head = store.head(&applied_key).unwrap().unwrap();
    let applied_stamp = applied_head
        .mutation_stamp
        .expect("one-node atomic head carries visibility lineage");
    assert_eq!(applied_stamp.program_commit_cursor, Some(1));
    assert_eq!(
        applied_stamp.active_placement_log_id,
        mutation_context().active_placement_log_id
    );
    assert_eq!(
        applied_stamp.serving_fence_term,
        mutation_context().serving_fence_term
    );
    let applied_version = store
        .version_metadata(&applied_key, applied_head.version)
        .unwrap()
        .unwrap();
    let applied_blob = applied_version.blob.unwrap();
    let applied_blob_state = store.blob_reference_state(&applied_blob).unwrap().unwrap();
    assert_eq!(applied_blob_state.ref_count, 1);
    assert_eq!(applied_blob_state.flags, 0);
    let bundle_blob = BlobRef::from(prepared.bundle);
    let released_bundle = store.blob_reference_state(&bundle_blob).unwrap().unwrap();
    assert_eq!(released_bundle.ref_count, 0);
    assert_eq!(released_bundle.flags, 0);
    assert!(
        !store
            .read_retained_blob_bytes(&bundle_blob)
            .await
            .unwrap()
            .is_empty()
    );
    let invalidations = store
        .scan_local_changes(0, 10)
        .unwrap()
        .into_iter()
        .filter_map(|change| change.into_object_head())
        .collect::<Vec<_>>();
    let journal = store.local_watch_status().unwrap();
    assert_eq!(invalidations.len(), 1);
    assert_eq!(applied_stamp.source_id, journal.source_id);
    assert_eq!(
        applied_stamp.source_journal_position,
        invalidations[0].offset
    );
    let counter_key = object_key(&counter_path()).unwrap();
    assert_eq!(invalidations[0].exact_path, counter_key.path());
    assert_eq!(
        invalidations[0].reference_deltas,
        [ReferenceDelta {
            blob: applied_blob.clone(),
            change: 1,
        }]
    );
    assert_eq!(
        invalidations[0].path_version,
        store.head(&counter_key).unwrap().unwrap().version
    );
    let journal_tail_before_replay = store.local_invalidation_offset().unwrap();
    assert_eq!(
        store.reference_delta_cursor(journal.source_id).unwrap(),
        journal.tail
    );

    // Recovery of an already-finalized commit must not append a duplicate
    // invalidation. The compact commit marker, head and journal move together in
    // the one local RocksDB WriteBatch above.
    let replayed = store
        .recover_program_bundle(
            ProgramCommit {
                previous_commit_cursor: None,
                commit_cursor: 1,
                begin_cursor: 1,
                bundle_ref: prepared.bundle,
                bundle_hash: prepared.hash,
                program_hash: prepared.program_hash,
                authority: prepared.authority,
                participant_manifest_hash: prepared.participant_manifest_hash,
                durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
                durability_evidence_hash: prepared.durability_evidence_hash,
            },
            mutation_context(),
        )
        .await
        .unwrap();
    assert_eq!(replayed, applied);
    assert_eq!(
        store.local_invalidation_offset().unwrap(),
        journal_tail_before_replay
    );
    assert_eq!(
        store.blob_reference_state(&applied_blob).unwrap().unwrap(),
        applied_blob_state
    );

    let replay_grace_millis = crate::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS * 1_000;
    assert_eq!(
        store
            .collect_blob_garbage_at(released_bundle.updated_at + replay_grace_millis - 1)
            .await
            .unwrap(),
        0
    );
    assert!(store.read_retained_blob_bytes(&bundle_blob).await.is_ok());
    assert_eq!(
        store
            .collect_blob_garbage_at(released_bundle.updated_at + replay_grace_millis)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .read_retained_blob_bytes(&bundle_blob)
            .await
            .unwrap_err(),
        MutationError::BlobNotFound
    );
}

#[tokio::test]
async fn preparation_rejects_an_atomic_transition_that_cannot_fit_the_source_journal() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(1, 64 * 1024 * 1024).unwrap()),
    )
    .await
    .unwrap();
    configure_policy(&store).await;
    let engine = store.program_engine(&verified_definition()).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("journal-bound", ExpectedHead::Absent),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.prepare_program_bundle(&lease).await.unwrap_err(),
        ProgramStoreError::SourceJournalTransitionTooLarge {
            entries: 2,
            maximum_entries: 1,
            ..
        }
    ));
}

#[tokio::test]
async fn preparation_rejects_an_atomic_transition_over_the_aggregate_journal_byte_bound() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreOptions::new(temporary.path(), 1)
            .with_watch_retention(WatchRetention::new(100, 1).unwrap()),
    )
    .await
    .unwrap();
    configure_policy(&store).await;
    let engine = store.program_engine(&verified_definition()).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("journal-byte-bound", ExpectedHead::Absent),
        )
        .await
        .unwrap();

    assert!(matches!(
        store.prepare_program_bundle(&lease).await.unwrap_err(),
        ProgramStoreError::SourceJournalTransitionTooLarge {
            entries: 2,
            maximum_bytes: 1,
            ..
        }
    ));
}

#[test]
fn replay_uses_no_local_receipt_column_family() {
    assert!(
        !crate::store::COLUMN_FAMILIES
            .iter()
            .any(|name| name.contains("program") || name.contains("replay"))
    );
}

#[test]
fn unsynced_executor_local_evidence_cannot_satisfy_local_commit() {
    assert_eq!(
        verify_commit_durability(
            &ProgramDurabilityScope::ExecutorLocal {
                node_id: 1,
                synced: false,
            },
            ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
        )
        .unwrap_err(),
        ProgramStoreError::ExecutorLocalDurability
    );
}

#[tokio::test]
async fn apply_is_all_old_or_all_new_and_records_only_the_compact_cursor() {
    let (_temporary, store, verified) = configured_store().await;
    let engine = store.program_engine(&verified).unwrap();
    let context = InvocationContext::new("tenant").unwrap();
    let first_invocation = invocation("command-1", ExpectedHead::Absent);
    let lease = engine.prepare(&context, &first_invocation).await.unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();

    let before = snapshot(&store).await;
    assert!(before.documents.is_empty());
    let first_commit = commit(&prepared, None, 1);
    let first_reservations = commit_prepared_reservations(&store, &prepared, &first_commit).await;
    let first = store
        .apply_program_bundle(lease, &prepared, first_commit, mutation_context())
        .await
        .unwrap();
    for reservation in &first_reservations {
        store
            .release_program_participant(reservation, Some(1))
            .await
            .unwrap();
    }
    let after = snapshot(&store).await;
    assert_eq!(
        after.documents[&counter_path()].value,
        Some(StoredValue::Json(json!({"value": 1})))
    );
    assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(1));
    let current_version = first.published_versions[&counter_path()];
    let second_invocation = invocation(
        "command-2",
        ExpectedHead::Version {
            version: current_version.version.0.to_string(),
        },
    );
    let second_lease = engine.prepare(&context, &second_invocation).await.unwrap();
    let second_prepared = store.prepare_program_bundle(&second_lease).await.unwrap();
    let second_commit = commit(&second_prepared, Some(1), 2);
    let _reservations =
        commit_prepared_reservations(&store, &second_prepared, &second_commit).await;
    let second = store
        .apply_program_bundle(
            second_lease,
            &second_prepared,
            second_commit,
            mutation_context(),
        )
        .await
        .unwrap();
    assert!(second.published_versions[&counter_path()].version > current_version.version);
    assert_eq!(
        snapshot(&store).await.documents[&counter_path()].value,
        Some(StoredValue::Json(json!({"value": 2})))
    );
    assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(2));
}

#[tokio::test]
async fn atomic_program_cannot_replace_or_delete_a_program_definition() {
    let (_temporary, store, _) = configured_store().await;
    let target = ObjectPath::new("tenant", "bucket", "_keldra/programs/victim@1").unwrap();
    let existing = store
        .put(PutRequest {
            key: object_key(&target).unwrap(),
            bytes: serde_json::to_vec(&json!({"value": 1})).unwrap(),
            content_type: Some("application/json".into()),
            mode: PutMode::PutImmutable,
            command_id: Some("install-victim".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                program_only_prefixes: vec!["_keldra/programs".into(), "managed".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let attempts = [
        reserved_program_path_attempt(
            "replace-definition",
            ExpectedHead::Version {
                version: existing.version.0.to_string(),
            },
            Operation::SetValue {
                target: JsonPointerRef::new(DocumentRef::one("counter"), ""),
                value: ValueSource::Literal {
                    value: json!({"value": 2}),
                },
            },
        ),
        reserved_program_path_attempt(
            "delete-definition",
            ExpectedHead::Version {
                version: existing.version.0.to_string(),
            },
            Operation::RemoveValue {
                target: JsonPointerRef::new(DocumentRef::one("counter"), ""),
            },
        ),
    ];

    for (program, invocation) in attempts {
        let engine = store.program_engine(&program).unwrap();
        let lease = engine
            .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
            .await
            .unwrap();
        assert_eq!(
            store.prepare_program_bundle(&lease).await.unwrap_err(),
            ProgramStoreError::Immutable {
                path: target.clone(),
            }
        );
    }
    assert_eq!(
        store
            .head(&object_key(&target).unwrap())
            .unwrap()
            .unwrap()
            .version,
        existing.version
    );
}

#[tokio::test]
async fn exact_head_is_rechecked_before_atomic_apply() {
    let (_temporary, store, verified) = configured_store().await;
    let engine = store.program_engine(&verified).unwrap();
    let context = InvocationContext::new("tenant").unwrap();
    let lease = engine
        .prepare(&context, &invocation("stale", ExpectedHead::Absent))
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    let stale_commit = commit(&prepared, None, 1);
    let _reservations = commit_prepared_reservations(&store, &prepared, &stale_commit).await;

    let rogue_id = store.clock.next().unwrap();
    let rogue = Version {
        id: rogue_id,
        blob: Some(BlobRef {
            hash: [0x99; 32],
            length: 1,
        }),
        content_type: Some("application/json".into()),
        deleted: false,
        committed_at_unix_millis: now_unix_millis().unwrap(),
        protected_link_descriptor: false,
    };
    let key = object_key(&counter_path()).unwrap();
    let identity = store
        .resolve_bucket_identity(key.tenant(), key.bucket())
        .unwrap();
    let mut batch = WriteBatch::default();
    batch.put_cf(
        store.program_cf(CF_VERSIONS).unwrap(),
        version_key(identity, &key, rogue_id),
        serde_json::to_vec(&StoredVersion::new(
            rogue,
            StoredVersionRetention::JournalPending,
        ))
        .unwrap(),
    );
    batch.put_cf(
        store.program_cf(CF_HEADS).unwrap(),
        identity.head_key(key.path()),
        serde_json::to_vec(&Head {
            version: rogue_id,
            deleted: false,
            mutation_stamp: None,
        })
        .unwrap(),
    );
    store.write_program_batch(batch).unwrap();

    assert_eq!(
        store
            .apply_program_bundle(lease, &prepared, stale_commit, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::PreconditionFailed {
            path: counter_path(),
            current: Some(rogue_id),
        }
    );
    assert!(store.applied_program_commit().unwrap().is_none());
}

#[tokio::test]
async fn ordinary_prepared_blobs_survive_reopen_for_recovery() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    configure_policy(&store).await;
    let verified = verified_definition();
    let engine = store.program_engine(&verified).unwrap();
    let context = InvocationContext::new("tenant").unwrap();
    let lease = engine
        .prepare(&context, &invocation("recover", ExpectedHead::Absent))
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    drop(lease.release());
    drop(engine);
    drop(store);

    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .prepared_program_bundle(
                prepared.bundle,
                prepared.hash,
                prepared.durability_evidence_hash,
            )
            .await
            .unwrap(),
        Some(prepared.clone())
    );
    let recovery_commit = commit(&prepared, None, 1);
    let _reservations = commit_prepared_reservations(&reopened, &prepared, &recovery_commit).await;
    let applied = reopened
        .recover_program_bundle(recovery_commit.clone(), mutation_context())
        .await
        .unwrap();
    assert_eq!(applied.receipt.command_id, "recover");
    assert_eq!(reopened.applied_program_commit_cursor().unwrap(), Some(1));
    let replayed = reopened
        .committed_program_result(recovery_commit)
        .await
        .unwrap();
    assert_eq!(replayed, applied);
}

#[tokio::test]
async fn recovery_rejects_committed_bundle_reference_and_durability_class_mismatches() {
    let (_temporary, store, program) = configured_store().await;
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("identity-mismatch", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    drop(lease.release());

    let mut wrong_reference = commit(&prepared, None, 1);
    wrong_reference.bundle_ref = PreparedBundleRef {
        hash: [0x41; 32],
        length: prepared.bundle.length,
    };
    assert_eq!(
        store
            .recover_program_bundle(wrong_reference, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::PreparedBundleMismatch
    );

    let mut wrong_class = commit(&prepared, None, 1);
    wrong_class.durability_class = ProgramDurabilityClassHash::for_class("other-remote");
    assert_eq!(
        store
            .recover_program_bundle(wrong_class, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::DurabilityClassMismatch
    );
    assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
    assert!(snapshot(&store).await.documents.is_empty());
}

#[tokio::test]
async fn finalization_rejects_a_committed_durability_class_mismatch_before_publication() {
    let (_temporary, store, program) = configured_store().await;
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("class-mismatch", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    let mut wrong_class = commit(&prepared, None, 1);
    wrong_class.durability_class = ProgramDurabilityClassHash::for_class("other-remote");

    assert_eq!(
        store
            .apply_program_bundle(lease, &prepared, wrong_class, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::DurabilityClassMismatch
    );
    assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
    assert!(snapshot(&store).await.documents.is_empty());
}

#[tokio::test]
async fn finalization_is_idempotent_and_rejects_cursor_corruption() {
    let (_temporary, store, program) = configured_store().await;
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("idempotent", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    let first_commit = commit(&prepared, None, 10);
    let _reservations = commit_prepared_reservations(&store, &prepared, &first_commit).await;
    let first = store
        .apply_program_bundle(lease, &prepared, first_commit, mutation_context())
        .await
        .unwrap();
    let replay = store
        .recover_program_bundle(commit(&prepared, None, 10), mutation_context())
        .await
        .unwrap();
    assert_eq!(replay, first);

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.program_hash = ProgramHash([9; 32]);
    assert_eq!(
        store
            .recover_program_bundle(corrupt, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::CommitCorruption { cursor: 10 }
    );

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.bundle_ref = PreparedBundleRef {
        hash: [8; 32],
        length: prepared.bundle.length,
    };
    assert_eq!(
        store
            .recover_program_bundle(corrupt, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::CommitCorruption { cursor: 10 }
    );

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.durability_class = ProgramDurabilityClassHash([7; 32]);
    assert_eq!(
        store
            .recover_program_bundle(corrupt, mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::CommitCorruption { cursor: 10 }
    );
    assert_eq!(store.applied_program_commit_cursor().unwrap(), Some(10));
}

#[tokio::test]
async fn predecessor_cursor_prevents_out_of_order_publication() {
    let (_temporary, store, program) = configured_store().await;
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("future", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let prepared = store.prepare_program_bundle(&lease).await.unwrap();
    drop(lease.release());

    assert_eq!(
        store
            .recover_program_bundle(commit(&prepared, Some(20), 30), mutation_context())
            .await
            .unwrap_err(),
        ProgramStoreError::OutOfOrderCommit {
            applied: None,
            expected: Some(20),
            requested: 30,
        }
    );
    assert!(snapshot(&store).await.documents.is_empty());
    assert_eq!(store.applied_program_commit_cursor().unwrap(), None);
}

#[test]
fn program_definition_must_match_loaded_immutable_bytes() {
    let bytes = serde_json::to_vec(&definition()).unwrap();
    assert_eq!(
        VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash([7; 32])).unwrap_err(),
        ProgramStoreError::ProgramHashMismatch
    );
}

#[tokio::test]
async fn mutable_read_only_program_dependency_is_evaluated_at_its_exact_version() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
    let config = store
        .put(PutRequest {
            key: object_key(&config_path).unwrap(),
            bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
            content_type: Some("application/json".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("install-mutable-configuration".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    configure_policy(&store).await;

    let mut definition = definition();
    definition.documents.push(DocumentSpec {
        name: "configuration".into(),
        path: PathTemplate::new("{tenant}", "bucket", "configuration/current"),
        cardinality: Cardinality::One,
        access: DocumentAccess::ReadOnly,
        allow_initial_json: false,
    });
    definition.caps.max_paths = 3;
    let bytes = serde_json::to_vec(&definition).unwrap();
    let verified =
        VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash::for_definition_bytes(&bytes))
            .unwrap();

    let mut invocation = invocation("read-mutable-configuration", ExpectedHead::Absent);
    invocation.bindings.insert(
        "configuration".into(),
        vec![PathBinding {
            path: config_path.clone(),
            template_values: Default::default(),
            expected_head: ExpectedHead::Version {
                version: config.version.0.to_string(),
            },
            initial_json: None,
        }],
    );

    let lease = store
        .program_engine(&verified)
        .unwrap()
        .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
        .await
        .unwrap();
    assert_eq!(
        lease.bundle().head_preconditions,
        vec![
            HeadPrecondition {
                path: counter_path(),
                expected: ObservedHead::NeverExisted,
            },
            HeadPrecondition {
                path: config_path.clone(),
                expected: ObservedHead::Version {
                    version: config.version.0.to_string(),
                },
            },
        ]
    );
    assert_eq!(lease.bundle().writes.len(), 1);
    assert_eq!(lease.bundle().writes[0].path, counter_path());
    assert_eq!(
        lease.bundle().writes[0].expected,
        ObservedHead::NeverExisted
    );
    assert_eq!(
        lease.bundle().writes[0].value,
        Some(StoredValue::Json(json!({"value": 1})))
    );
    assert!(
        lease
            .bundle()
            .writes
            .iter()
            .all(|write| write.path != config_path)
    );
    assert_eq!(lease.bundle().outputs.get("value"), Some(&json!(1)));
    assert_eq!(
        store.head(&object_key(&counter_path()).unwrap()).unwrap(),
        None
    );
    assert_eq!(
        store.head(&object_key(&config_path).unwrap()).unwrap(),
        Some(Head {
            version: config.version,
            deleted: false,
            mutation_stamp: None,
        })
    );
    drop(lease.release());
}

#[tokio::test]
async fn immutable_read_only_program_dependency_is_evaluated_at_its_exact_version() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let config_path = ObjectPath::new("tenant", "bucket", "configuration/current").unwrap();
    let config = store
        .put(PutRequest {
            key: object_key(&config_path).unwrap(),
            bytes: serde_json::to_vec(&json!({"enabled": true})).unwrap(),
            content_type: Some("application/json".into()),
            mode: PutMode::PutIfAbsent,
            command_id: Some("install-configuration".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                immutable_prefixes: vec!["configuration".into()],
                program_only_prefixes: vec!["managed".into()],
            },
        )
        .await
        .unwrap();

    let mut definition = definition();
    definition.documents.push(DocumentSpec {
        name: "configuration".into(),
        path: PathTemplate::new("{tenant}", "bucket", "configuration/current"),
        cardinality: Cardinality::One,
        access: DocumentAccess::ReadOnly,
        allow_initial_json: false,
    });
    definition.caps.max_paths = 3;
    let bytes = serde_json::to_vec(&definition).unwrap();
    let verified =
        VerifiedProgramDefinition::from_bytes(&bytes, ProgramHash::for_definition_bytes(&bytes))
            .unwrap();

    let mut invocation = invocation("read-configuration", ExpectedHead::Absent);
    invocation.bindings.insert(
        "configuration".into(),
        vec![PathBinding {
            path: config_path.clone(),
            template_values: Default::default(),
            expected_head: ExpectedHead::Version {
                version: config.version.0.to_string(),
            },
            initial_json: None,
        }],
    );

    let lease = store
        .program_engine(&verified)
        .unwrap()
        .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
        .await
        .unwrap();
    assert_eq!(
        lease.bundle().head_preconditions,
        vec![
            HeadPrecondition {
                path: counter_path(),
                expected: ObservedHead::NeverExisted,
            },
            HeadPrecondition {
                path: config_path.clone(),
                expected: ObservedHead::Version {
                    version: config.version.0.to_string(),
                },
            },
        ]
    );
    assert_eq!(lease.bundle().writes.len(), 1);
    assert_eq!(lease.bundle().writes[0].path, counter_path());
    assert_eq!(
        lease.bundle().writes[0].expected,
        ObservedHead::NeverExisted
    );
    assert_eq!(
        lease.bundle().writes[0].value,
        Some(StoredValue::Json(json!({"value": 1})))
    );
    assert!(
        lease
            .bundle()
            .writes
            .iter()
            .all(|write| write.path != config_path)
    );
    assert_eq!(lease.bundle().outputs.get("value"), Some(&json!(1)));
    assert_eq!(
        store.head(&object_key(&counter_path()).unwrap()).unwrap(),
        None
    );
    assert_eq!(
        store.head(&object_key(&config_path).unwrap()).unwrap(),
        Some(Head {
            version: config.version,
            deleted: false,
            mutation_stamp: None,
        })
    );
    drop(lease.release());
}

#[tokio::test]
async fn distributed_path_stage_is_invisible_until_commit_bound_finalization() {
    let (_temporary, store, program) = configured_store().await;
    install_versioned_governance(&store);
    assert_eq!(
        store.bucket_policy("tenant", "bucket").unwrap(),
        BucketPolicy {
            program_only_prefixes: vec!["managed".into()],
            ..Default::default()
        }
    );
    assert_eq!(
        store.bucket_versioning("tenant", "bucket").unwrap(),
        ObjectVersioning::Unversioned
    );
    let engine = store.program_engine(&program).unwrap();
    let lease = engine
        .prepare(
            &InvocationContext::new("tenant").unwrap(),
            &invocation("distributed-stage", ExpectedHead::Absent),
        )
        .await
        .unwrap();
    let mut prepared = store
        .prepare_distributed_program_bundle(program.hash, lease.bundle(), &BTreeMap::new())
        .await
        .unwrap();
    prepared.attest_remote_durability("replicated").unwrap();
    let record = store.prepared_program_record(&prepared).await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let stage = path_stage_from_prepared(
        &prepared,
        record.writes().first().unwrap(),
        40,
        tenant_id,
        bucket_id,
    )
    .unwrap();
    let reservations = record
        .reservations(
            40,
            [1; 32],
            prepared.hash,
            1,
            3,
            PlacementLogId { term: 3, index: 7 },
        )
        .unwrap();
    for reservation in &reservations {
        store
            .reserve_program_participant(reservation)
            .await
            .unwrap();
        store
            .commit_program_participant(reservation, 42)
            .await
            .unwrap();
    }

    let persisted = store.persist_program_path_stage(&stage).await.unwrap();
    assert_eq!(persisted, stage.blob_ref().unwrap());
    assert_eq!(
        store.head(&object_key(&counter_path()).unwrap()).unwrap(),
        None
    );

    let stale_fence = store
        .coordinate_program_path_finalization(
            stage.clone(),
            42,
            crate::ObjectMutationContext {
                active_placement_log_id: crate::PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 2,
            },
        )
        .await;
    assert!(stale_fence.is_err());
    assert_eq!(
        store.head(&object_key(&counter_path()).unwrap()).unwrap(),
        None
    );

    let finalized = store
        .coordinate_program_path_finalization(
            stage.clone(),
            42,
            crate::ObjectMutationContext {
                active_placement_log_id: crate::PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(finalized.mutation.commit_cursor, 42);
    assert_eq!(finalized.mutation.stamp.program_commit_cursor, Some(42));
    let head = store
        .head(&object_key(&counter_path()).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(head.version, stage.version.id);
    assert_eq!(head.mutation_stamp.unwrap().program_commit_cursor, Some(42));
    let proof = store
        .read_reference_proof(
            finalized.mutation.stamp.source_id,
            finalized.mutation.stamp.source_journal_position,
        )
        .unwrap()
        .expect("program path proof");
    assert_eq!(
        proof.mutation,
        crate::ReferenceProofMutation::ProgramPath(finalized.mutation.clone())
    );

    let replay = store
        .apply_program_path_finalization_replica(&finalized.mutation)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, stage.version.id);

    let publication = SealedAtomicBatchPublication::from_prepared(
        42,
        prepared.bundle,
        prepared.hash,
        &record,
        &[stage.clone()],
        &[finalized.mutation.clone()],
        &[],
    )
    .unwrap();
    assert!(
        store
            .publish_atomic_batch(publication.clone())
            .await
            .unwrap()
    );
    let tail = store.local_watch_status().unwrap().tail;
    assert!(!store.publish_atomic_batch(publication).await.unwrap());
    assert_eq!(store.local_watch_status().unwrap().tail, tail);
    let published = store.read_local_change(tail).unwrap().unwrap();
    let LocalChange::AtomicBatchPublished(published) = published else {
        panic!("last source event is not the complete atomic batch");
    };
    assert_eq!(published.cursor, 42);
    assert_eq!(published.bundle_hash, prepared.hash);
    assert_eq!(published.mutations.len(), 1);
    let truncated = SealedAtomicBatchPublication::from_prepared(
        43,
        prepared.bundle,
        prepared.hash,
        &record,
        &[],
        &[],
        &[],
    );
    assert_eq!(truncated, Err(ProgramStoreError::PreparedBundleMismatch));
}

#[tokio::test]
async fn distributed_versioned_program_counts_each_same_blob_retained_version() {
    let (_temporary, store, _program) = configured_store().await;
    assert!(
        store
            .enable_bucket_versioning("tenant", "bucket")
            .await
            .unwrap()
    );
    let payload = store.stage_blob(br#"{"value":1}"#).await.unwrap();
    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let first_version = Version {
        id: store.clock.next().unwrap(),
        blob: Some(payload.clone()),
        content_type: Some("application/json".into()),
        deleted: false,
        committed_at_unix_millis: now_unix_millis().unwrap(),
        protected_link_descriptor: false,
    };
    let first = store
        .coordinate_program_path_finalization(
            ProgramPathStage {
                format: PROGRAM_PATH_STAGE_FORMAT,
                begin_cursor: 40,
                bundle_hash: PreparedBundleHash([0x11; 32]),
                program_hash: ProgramHash([0x22; 32]),
                authority: ProgramBundleAuthority::LegacyProgramOnly {
                    program_path_hash: [0x33; 32],
                    program_hash: [0x22; 32],
                },
                participant_manifest_hash: [0x44; 32],
                tenant_id,
                bucket_id,
                path: counter_path(),
                expected: ObservedHead::NeverExisted,
                previous_version: None,
                version: first_version.clone(),
            },
            41,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.mutation.reference_deltas,
        [ReferenceDelta {
            blob: payload.clone(),
            change: 1,
        }]
    );
    let source = first.mutation.stamp.source_id;
    let before_first = store.reference_delta_cursor(source).unwrap();
    store
        .apply_reference_deltas(ReferenceDeltaBatch {
            source,
            after: before_first,
            through: first.mutation.stamp.source_journal_position,
            deltas: vec![DestinationReferenceDelta {
                artifact: DestinationReferenceArtifact::CompleteBlob(payload.clone()),
                change: 1,
            }],
        })
        .await
        .unwrap();

    let second_version = Version {
        id: store.clock.next().unwrap(),
        blob: Some(payload.clone()),
        content_type: Some("application/json".into()),
        deleted: false,
        committed_at_unix_millis: now_unix_millis().unwrap(),
        protected_link_descriptor: false,
    };
    let second = store
        .coordinate_program_path_finalization(
            ProgramPathStage {
                format: PROGRAM_PATH_STAGE_FORMAT,
                begin_cursor: 41,
                bundle_hash: PreparedBundleHash([0x33; 32]),
                program_hash: ProgramHash([0x22; 32]),
                authority: ProgramBundleAuthority::LegacyProgramOnly {
                    program_path_hash: [0x33; 32],
                    program_hash: [0x22; 32],
                },
                participant_manifest_hash: [0x44; 32],
                tenant_id,
                bucket_id,
                path: counter_path(),
                expected: ObservedHead::Version {
                    version: first_version.id.0.to_string(),
                },
                previous_version: Some(first_version.clone()),
                version: second_version.clone(),
            },
            42,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        second.mutation.reference_deltas,
        [ReferenceDelta {
            blob: payload.clone(),
            change: 1,
        }]
    );
    store
        .apply_reference_deltas(ReferenceDeltaBatch {
            source,
            after: first.mutation.stamp.source_journal_position,
            through: second.mutation.stamp.source_journal_position,
            deltas: vec![DestinationReferenceDelta {
                artifact: DestinationReferenceArtifact::CompleteBlob(payload.clone()),
                change: 1,
            }],
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&payload)
            .unwrap()
            .unwrap()
            .ref_count,
        2
    );

    let (_replica_temporary, replica, _program) = configured_store().await;
    let policy = LogicalRecordValue::BucketPolicy {
        tenant_id,
        bucket_id,
        policy: BucketPolicy {
            program_only_prefixes: vec!["managed".into()],
            ..Default::default()
        },
    };
    let policy_mutation = replica
        .construct_logical_record_mutation(
            policy,
            LogicalRecordMutationContext {
                record_version: replica.allocate_logical_record_version().unwrap(),
                active_placement_log_id: PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 3,
            },
        )
        .unwrap();
    replica
        .commit_logical_record_mutation(&policy_mutation)
        .unwrap();
    replica
        .apply_program_path_finalization_replica(&first.mutation)
        .await
        .unwrap();
    replica
        .apply_program_path_finalization_replica(&second.mutation)
        .await
        .unwrap();

    let deletion = store
        .coordinate_retained_version_delete(
            &object_key(&counter_path()).unwrap(),
            first_version.id,
            ObjectMutationGovernance {
                tenant_id,
                bucket_id,
                versioning: ObjectVersioning::Enabled,
                policy: BucketPolicy::default(),
            },
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 3, index: 7 },
                serving_fence_term: 3,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        deletion.outcome,
        DeleteRetainedVersionOutcome::DeletedNonCurrent
    );
    let deletion = deletion.mutation.unwrap();
    assert_eq!(
        deletion.reference_deltas,
        [ReferenceDelta {
            blob: payload.clone(),
            change: -1,
        }]
    );
    store
        .apply_reference_deltas(ReferenceDeltaBatch {
            source,
            after: second.mutation.stamp.source_journal_position,
            through: deletion.stamp.source_journal_position,
            deltas: vec![DestinationReferenceDelta {
                artifact: DestinationReferenceArtifact::CompleteBlob(payload.clone()),
                change: -1,
            }],
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .blob_reference_state(&payload)
            .unwrap()
            .unwrap()
            .ref_count,
        1
    );
    assert_eq!(
        store
            .version_metadata(&object_key(&counter_path()).unwrap(), second_version.id)
            .unwrap(),
        Some(second_version)
    );
    assert_eq!(
        store.read_blob_bytes(&payload).await.unwrap(),
        br#"{"value":1}"#
    );
}
