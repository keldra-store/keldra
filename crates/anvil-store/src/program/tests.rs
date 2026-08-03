use anvil_atomic_program::{
    Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
    DocumentValueRef, DocumentView, ExpectedHead, InputValue, IntegerType, InvocationContext,
    JsonPointerRef, Operation, PathBinding, PathTemplate, ProgramCaps, ProgramInvocation,
    ReturnDefinition, ValueSource,
};
use serde_json::json;
use tempfile::TempDir;

use super::*;
use crate::{BucketPolicy, Durability, PutMode, PutRequest, StoreOptions};

fn counter_path() -> ObjectPath {
    ObjectPath::new("tenant", "bucket", "managed/counter").unwrap()
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

fn reserved_program_path_attempt(
    command_id: &str,
    expected_head: ExpectedHead,
    operation: Operation,
) -> (VerifiedProgramDefinition, ProgramInvocation) {
    let target = ObjectPath::new("tenant", "bucket", "_anvil/programs/victim@1").unwrap();
    let mut definition = definition();
    definition.documents[0].path =
        PathTemplate::new("{tenant}", "bucket", "_anvil/programs/victim@1");
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
        bundle_ref: prepared.bundle,
        bundle_hash: prepared.hash,
        program_hash: prepared.program_hash,
        durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
        durability_evidence_hash: prepared.durability_evidence_hash,
    }
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
        bundle_ref: prepared.bundle,
        bundle_hash: prepared.hash,
        program_hash: prepared.program_hash,
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
    let before_apply = store.db.latest_sequence_number();
    let applied = store
        .apply_program_bundle(lease, &prepared, local_commit.clone())
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
            "bundle_hash",
            "bundle_ref",
            "commit_cursor",
            "durability_class",
            "durability_evidence_hash",
            "program_hash",
        ])
    );
    let applied_key = object_key(&counter_path()).unwrap();
    let applied_head = store.head(&applied_key).unwrap().unwrap();
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
            .read_blob_bytes(&bundle_blob)
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
    assert_eq!(invalidations.len(), 1);
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

    // Recovery of an already-finalized commit must not append a duplicate
    // invalidation. The compact commit marker, head and journal move together in
    // the one local RocksDB WriteBatch above.
    let replayed = store
        .recover_program_bundle(ProgramCommit {
            previous_commit_cursor: None,
            commit_cursor: 1,
            bundle_ref: prepared.bundle,
            bundle_hash: prepared.hash,
            program_hash: prepared.program_hash,
            durability_class: ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS),
            durability_evidence_hash: prepared.durability_evidence_hash,
        })
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
            .unwrap(),
        0
    );
    assert!(store.read_blob_bytes(&bundle_blob).await.is_ok());
    assert_eq!(
        store
            .collect_blob_garbage_at(released_bundle.updated_at + replay_grace_millis)
            .unwrap(),
        1
    );
    assert_eq!(
        store.read_blob_bytes(&bundle_blob).await.unwrap_err(),
        MutationError::BlobNotFound
    );
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
    let first = store
        .apply_program_bundle(lease, &prepared, first_commit)
        .await
        .unwrap();
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
    let second = store
        .apply_program_bundle(second_lease, &second_prepared, second_commit)
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
    let target = ObjectPath::new("tenant", "bucket", "_anvil/programs/victim@1").unwrap();
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
                program_only_prefixes: vec!["_anvil/programs".into(), "managed".into()],
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
    };
    let key = object_key(&counter_path()).unwrap();
    let identity = store
        .resolve_bucket_identity(key.tenant(), key.bucket())
        .unwrap();
    let mut batch = WriteBatch::default();
    batch.put_cf(
        store.program_cf(CF_VERSIONS).unwrap(),
        version_key(identity, &key, rogue_id),
        serde_json::to_vec(&rogue).unwrap(),
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

    let stale_commit = commit(&prepared, None, 1);
    assert_eq!(
        store
            .apply_program_bundle(lease, &prepared, stale_commit)
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
    let applied = reopened
        .recover_program_bundle(commit(&prepared, None, 1))
        .await
        .unwrap();
    assert_eq!(applied.receipt.command_id, "recover");
    assert_eq!(reopened.applied_program_commit_cursor().unwrap(), Some(1));
    let replayed = reopened
        .committed_program_result(commit(&prepared, None, 1))
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
            .recover_program_bundle(wrong_reference)
            .await
            .unwrap_err(),
        ProgramStoreError::PreparedBundleMismatch
    );

    let mut wrong_class = commit(&prepared, None, 1);
    wrong_class.durability_class = ProgramDurabilityClassHash::for_class("other-remote");
    assert_eq!(
        store.recover_program_bundle(wrong_class).await.unwrap_err(),
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
            .apply_program_bundle(lease, &prepared, wrong_class)
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
    let first = store
        .apply_program_bundle(lease, &prepared, first_commit)
        .await
        .unwrap();
    let replay = store
        .recover_program_bundle(commit(&prepared, None, 10))
        .await
        .unwrap();
    assert_eq!(replay, first);

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.program_hash = ProgramHash([9; 32]);
    assert_eq!(
        store.recover_program_bundle(corrupt).await.unwrap_err(),
        ProgramStoreError::CommitCorruption { cursor: 10 }
    );

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.bundle_ref = PreparedBundleRef {
        hash: [8; 32],
        length: prepared.bundle.length,
    };
    assert_eq!(
        store.recover_program_bundle(corrupt).await.unwrap_err(),
        ProgramStoreError::CommitCorruption { cursor: 10 }
    );

    let mut corrupt = commit(&prepared, None, 10);
    corrupt.durability_class = ProgramDurabilityClassHash([7; 32]);
    assert_eq!(
        store.recover_program_bundle(corrupt).await.unwrap_err(),
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
            .recover_program_bundle(commit(&prepared, Some(20), 30))
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
async fn mutable_read_only_program_dependency_is_rejected_before_execution() {
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

    let error = store
        .program_engine(&verified)
        .unwrap()
        .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::ProgramConcurrency { path, reason }
            if path.path == "configuration/current" && reason.contains("PROGRAM_ONLY")
    ));
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
}

#[tokio::test]
async fn immutable_read_only_program_dependency_still_requires_program_only_policy() {
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

    let error = store
        .program_engine(&verified)
        .unwrap()
        .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::ProgramConcurrency { path, reason }
            if path.path == "configuration/current" && reason.contains("PROGRAM_ONLY")
    ));
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
}

#[tokio::test]
async fn distributed_path_stage_is_invisible_until_commit_bound_finalization() {
    let (_temporary, store, program) = configured_store().await;
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
        tenant_id,
        bucket_id,
    )
    .unwrap();

    let persisted = store.persist_program_path_stage(&stage).await.unwrap();
    assert_eq!(persisted, stage.blob_ref().unwrap());
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

    let replay = store
        .apply_program_path_finalization_replica(&finalized.mutation)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.version, stage.version.id);
}
