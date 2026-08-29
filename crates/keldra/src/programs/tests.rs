use std::sync::Mutex;

use keldra_atomic_program::{
    Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
    DocumentValueRef, DocumentView, ExpectedHead, InputValue, IntegerType, JsonPointerRef,
    Operation, PathBinding, PathTemplate, ProgramCaps, ProgramDefinition, ReturnDefinition,
};
use keldra_authz::ObjectRef;
use keldra_consensus::{
    CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterId, JoinCapabilityHash,
    NodeDescriptor, NodeState, PeerAddress, PeerSpkiSha256,
};
use keldra_store::{
    AuthzRevision, BucketPolicy, CreateBucketRequest, Durability, ObjectVersioning,
    ProvisionTenantRequest, PutMode, PutRequest, StorageTenantId, StoreOptions,
    SystemBootstrapRequest,
};
use serde_json::json;

use super::*;

fn counter_definition() -> ProgramDefinition {
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
            numeric_type: IntegerType::U64 {
                min: Some(0),
                max: None,
            },
        }],
        returns: vec![ReturnDefinition {
            name: "value".into(),
            value: DocumentValueRef {
                value: JsonPointerRef::new(counter, "/value"),
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

fn counter_input() -> ProgramInput {
    ProgramInput {
        inputs: [("delta".into(), json!(1))].into_iter().collect(),
        bindings: [(
            "counter".into(),
            vec![PathBinding {
                path: ObjectPath::new("tenant", "bucket", "managed/counter").unwrap(),
                template_values: BTreeMap::new(),
                expected_head: ExpectedHead::Absent,
                initial_json: Some(json!({"value": 0})),
            }],
        )]
        .into_iter()
        .collect(),
        ..ProgramInput::default()
    }
}

async fn configured_program_store(root: &Path) -> (Store, ObjectKey, [u8; 32], Vec<u8>) {
    let store = Store::open(StoreOptions::new(root, 1)).await.unwrap();
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: "bootstrap-secret-0123456789abcdef0123456789abcdef".into(),
        })
        .unwrap();
    let tenant = StorageTenantId::parse("tenant").unwrap();
    let owner = ObjectRef::opaque("app", "owner-app").unwrap();
    store
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: tenant.clone(),
            owner_app_id: "owner-app".into(),
            owner_client_id: "owner-client".into(),
            owner_client_secret: "owner-secret-0123456789abcdef0123456789abcdef".into(),
            principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
            expected_authorization_revision: AuthzRevision(3),
            expected_binding_generation: 1,
        })
        .unwrap();
    store
        .create_bucket(CreateBucketRequest {
            storage_tenant: tenant,
            bucket: "bucket".into(),
            owner: owner.clone(),
            principal: owner,
            expected_authorization_revision: AuthzRevision(4),
            expected_binding_generation: 1,
            versioning: ObjectVersioning::Unversioned,
        })
        .unwrap();
    store
        .set_bucket_policy(
            "tenant",
            "bucket",
            BucketPolicy {
                immutable_prefixes: Vec::new(),
                program_only_prefixes: vec!["managed".into()],
            },
        )
        .await
        .unwrap();
    let program_key = ObjectKey::new("tenant", "bucket", "_keldra/programs/counter@1").unwrap();
    let definition = serde_json::to_vec(&counter_definition()).unwrap();
    let program_hash = ProgramHash::for_definition_bytes(&definition).0;
    store
        .put(PutRequest {
            key: program_key.clone(),
            bytes: definition,
            content_type: Some("application/json".into()),
            mode: PutMode::PutImmutable,
            command_id: Some("install-counter-program".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();
    (
        store,
        program_key,
        program_hash,
        serde_json::to_vec(&counter_input()).unwrap(),
    )
}

async fn open_test_coordinator(store: Store, root: &Path) -> ProgramCoordinator {
    open_test_coordinator_with_limits(store, root, 8, 64 * 1024).await
}

async fn prepare_with_canonical_bindings(
    store: &Store,
    engine: &keldra_store::StoreProgramEngine,
    context: &InvocationContext,
    invocation: &ProgramInvocation,
) -> (
    keldra_store::ProgramExecutionLease,
    keldra_store::PreparedProgramBundle,
) {
    let expanded = engine.expanded_paths(context, invocation).unwrap();
    let alias_bindings = store
        .resolve_program_alias_bindings(&expanded)
        .await
        .unwrap();
    let canonical_paths = alias_bindings
        .iter()
        .map(|binding| {
            (
                binding.requested_path.clone(),
                binding.canonical_path.clone(),
            )
        })
        .collect();
    let lease = engine
        .prepare_canonicalized(context, invocation, &canonical_paths)
        .await
        .unwrap();
    let prepared = store
        .prepare_program_bundle_with_aliases(&lease, &alias_bindings)
        .await
        .unwrap();
    (lease, prepared)
}

async fn commit_prepared_for_recovery(
    coordinator: &ProgramCoordinator,
    store: &Store,
    prepared: &keldra_store::PreparedProgramBundle,
    invocation_id: InvocationId,
    fingerprint: [u8; 32],
    proposal_at_unix_millis: u64,
) -> keldra_consensus::CommitResult {
    let nomination = coordinator.current_nomination().unwrap();
    let begun = coordinator
        .decisions
        .submit(Command::BeginBatch(BeginBatch {
            executor: NodeId(1),
            nomination_log_index: nomination.nomination_log_index,
            authority: decision_bundle_authority(prepared.authority),
            invocation_id,
            input_fingerprint: InvocationFingerprint(fingerprint),
            bundle_ref: BundleRef {
                hash: prepared.bundle.hash,
                length: prepared.bundle.length,
            },
            bundle_hash: BundleHash(prepared.hash.0),
            durability_class: DurabilityClass(
                ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS).0,
            ),
            durability_evidence_hash: DurabilityEvidenceHash(prepared.durability_evidence_hash.0),
            participant_manifest_hash: ParticipantManifestHash(prepared.participant_manifest_hash),
            proposal_at_unix_millis,
            replay_expires_at_unix_millis: proposal_at_unix_millis + ATOMIC_REPLAY_RETENTION_MILLIS,
        }))
        .await
        .unwrap();
    let keldra_consensus::BeginResult::Prepared { batch, .. } =
        expect_batch_begun(begun.result).unwrap()
    else {
        panic!("fresh recovery fixture unexpectedly replayed")
    };
    let record = store.prepared_program_record(prepared).await.unwrap();
    let placement = coordinator
        .one_node_mutation_context(nomination)
        .unwrap()
        .active_placement_log_id;
    let reservations = record
        .reservations(
            batch.begin_cursor,
            invocation_id.0,
            prepared.hash,
            1,
            nomination.nomination_log_index,
            placement,
        )
        .unwrap();
    coordinator
        .reserve_local_participants(&reservations)
        .await
        .unwrap();
    let committed = coordinator
        .decisions
        .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
            executor: NodeId(1),
            nomination_log_index: nomination.nomination_log_index,
            begin_cursor: batch.begin_cursor,
            invocation_id,
            participant_manifest_hash: ParticipantManifestHash(prepared.participant_manifest_hash),
        }))
        .await
        .unwrap();
    let committed = expect_batch_committed(committed.result).unwrap();
    coordinator
        .commit_local_participants(
            &reservations,
            committed.invocation.committed_batch.commit_cursor,
        )
        .await
        .unwrap();
    committed
}

async fn open_test_coordinator_with_limits(
    store: Store,
    root: &Path,
    max_commit_entries: u32,
    max_commit_bytes: u64,
) -> ProgramCoordinator {
    let decisions = DecisionRaft::open(
        root.join("decisions"),
        1,
        max_commit_entries,
        max_commit_bytes,
    )
    .await
    .unwrap();
    decisions.ensure_one_node().await.unwrap();
    decisions
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    commit_test_active_placement(&decisions).await;
    ProgramCoordinator::start(store, decisions, NodeId(1))
        .await
        .unwrap()
}

async fn commit_test_active_placement(decisions: &DecisionRaft) {
    let state = decisions.state().unwrap();
    if state.cluster_control().active_placement_log_id().is_some() {
        return;
    }
    if state.cluster_id().is_none() {
        decisions
            .submit(Command::InitializeCluster {
                cluster_id: ClusterId([1; 16]),
            })
            .await
            .unwrap();
    }
    let begun = decisions
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: NodeDescriptor {
                node_id: NodeId(1),
                peer_address: PeerAddress("keldra-local://1".into()),
                storage_weight_millionths: 1_000_000,
                state: NodeState::Joining,
                current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
                overlap_peer_spki_sha256: None,
                join_capability_hash: Some(JoinCapabilityHash([2; 32])),
                supported_protocol: CapabilityRange { min: 1, max: 2 },
                supported_storage_format: CapabilityRange { min: 1, max: 2 },
            },
        })
        .await
        .unwrap();
    for _ in 0..2 {
        decisions
            .submit(Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: begun.log_index,
            })
            .await
            .unwrap();
    }
    let placement = decisions
        .state()
        .unwrap()
        .cluster_control()
        .active_placement_log_id()
        .unwrap();
    decisions
        .submit(Command::ActivateClusterCapabilities {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            protocol_version: 2,
            storage_format: 2,
            expected_active_placement_log_id: placement,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn public_coordinator_retains_replay_then_compacts_the_recovery_tail() {
    let temporary = tempfile::tempdir().unwrap();
    let (store, program_key, program_hash, input) =
        configured_program_store(temporary.path()).await;
    let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
    let intents = Mutex::new(Vec::new());

    let first = coordinator
        .invoke(
            program_key.clone(),
            program_hash,
            "increment-1".into(),
            &input,
            LOCAL_DURABILITY_CLASS,
            |dependency| {
                intents.lock().unwrap().push(dependency.intent);
                Ok(())
            },
            |_| Ok(()),
        )
        .await
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.receipt.outputs["value"], json!(1));
    assert_eq!(
        intents.into_inner().unwrap(),
        vec![keldra_atomic_program::ProgramPathIntent {
            get: true,
            put: true,
            delete: false,
        }]
    );
    let decision_state = coordinator.decisions.state().unwrap();
    assert_eq!(decision_state.unfinalized_commit_len(), 0);
    assert_eq!(
        decision_state.finalized_through(),
        Some(first.commit_log_index)
    );
    assert!(first.replay_guarantee_expires_at_unix_millis > current_unix_millis().unwrap());

    let replay = coordinator
        .invoke(
            program_key,
            program_hash,
            "increment-1".into(),
            &input,
            LOCAL_DURABILITY_CLASS,
            |_| Ok(()),
            |_| Ok(()),
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.commit_log_index, first.commit_log_index);
    assert_eq!(
        replay.replay_guarantee_expires_at_unix_millis,
        first.replay_guarantee_expires_at_unix_millis
    );
    assert_eq!(replay.published_versions, first.published_versions);
    coordinator.decisions.shutdown().await.unwrap();
}

#[tokio::test]
async fn each_success_advances_finalized_through_before_the_next_commit() {
    let temporary = tempfile::tempdir().unwrap();
    let (store, program_key, program_hash, first_input) =
        configured_program_store(temporary.path()).await;
    let coordinator =
        open_test_coordinator_with_limits(store, temporary.path(), 1, 64 * 1024).await;

    let mut input = serde_json::from_slice::<ProgramInput>(&first_input).unwrap();
    let mut previous_version: Option<u64> = None;
    for index in 0..4 {
        if let Some(version) = previous_version {
            let binding = &mut input.bindings.get_mut("counter").unwrap()[0];
            binding.expected_head = ExpectedHead::Version {
                version: version.to_string(),
            };
            binding.initial_json = None;
        }
        let result = coordinator
            .invoke(
                program_key.clone(),
                program_hash,
                format!("bounded-tail-{index}"),
                &serde_json::to_vec(&input).unwrap(),
                LOCAL_DURABILITY_CLASS,
                |_| Ok(()),
                |_| Ok(()),
            )
            .await
            .unwrap();
        previous_version = result
            .published_versions
            .values()
            .next()
            .map(|published| published.version.0);
        let state = coordinator.decisions.state().unwrap();
        assert_eq!(state.unfinalized_commit_len(), 0);
        assert_eq!(state.finalized_through(), Some(result.commit_log_index));
    }
    coordinator.decisions.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_recovers_committed_bundle_before_advancing_finalized_through() {
    let temporary = tempfile::tempdir().unwrap();
    let (store, program_key, program_hash, input_json) =
        configured_program_store(temporary.path()).await;
    let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
    coordinator.shutdown_recovery_worker().await;
    let input = serde_json::from_slice::<ProgramInput>(&input_json).unwrap();
    let path_hash = program_path_hash(&program_key);
    let invocation =
        ProgramInvocation::from_input(path_hash, "crash-before-finalize", input).unwrap();
    let fingerprint = decode_fingerprint(&invocation.input_fingerprint).unwrap();
    let invocation_id = invocation_identity(&program_key, &invocation.command_id);
    let program_object = store.get(&program_key).await.unwrap().unwrap();
    let verified =
        VerifiedProgramDefinition::from_bytes(&program_object.bytes, ProgramHash(program_hash))
            .unwrap();
    let engine = store.program_engine(&verified).unwrap();
    let context = InvocationContext::new("tenant").unwrap();
    let (lease, prepared) =
        prepare_with_canonical_bindings(&store, &engine, &context, &invocation).await;
    let proposal_at_unix_millis = current_unix_millis().unwrap();
    let committed = commit_prepared_for_recovery(
        &coordinator,
        &store,
        &prepared,
        invocation_id,
        fingerprint,
        proposal_at_unix_millis,
    )
    .await;
    let commit_cursor = committed.invocation.committed_batch.commit_cursor;
    assert!(!coordinator.cursor_is_visible(commit_cursor).unwrap());
    drop(lease);
    drop(engine);
    coordinator.shutdown_recovery_worker().await;
    coordinator.decisions.shutdown().await.unwrap();
    drop(coordinator);
    drop(store);

    let reopened_store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let reopened = open_test_coordinator(reopened_store.clone(), temporary.path()).await;
    let state = reopened.decisions.state().unwrap();
    assert_eq!(state.finalized_through(), Some(commit_cursor));
    assert_eq!(state.unfinalized_commit_len(), 0);
    assert!(reopened.cursor_is_visible(commit_cursor).unwrap());
    assert_eq!(
        reopened_store.applied_program_commit_cursor().unwrap(),
        Some(commit_cursor)
    );
    let replay = reopened
        .invoke(
            program_key,
            program_hash,
            "crash-before-finalize".into(),
            &input_json,
            LOCAL_DURABILITY_CLASS,
            |_| Ok(()),
            |_| Ok(()),
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.commit_log_index, commit_cursor);
    assert_eq!(replay.receipt.outputs["value"], json!(1));
    reopened.shutdown_recovery_worker().await;
    reopened.decisions.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_finalizes_a_partially_recovered_multi_commit_tail() {
    let temporary = tempfile::tempdir().unwrap();
    let (store, program_key, program_hash, _) = configured_program_store(temporary.path()).await;
    let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
    coordinator.shutdown_recovery_worker().await;
    let path_hash = program_path_hash(&program_key);
    let program_object = store.get(&program_key).await.unwrap().unwrap();
    let verified =
        VerifiedProgramDefinition::from_bytes(&program_object.bytes, ProgramHash(program_hash))
            .unwrap();
    let engine = store.program_engine(&verified).unwrap();
    let context = InvocationContext::new("tenant").unwrap();
    let nomination = coordinator.current_nomination().unwrap();

    let first_invocation =
        ProgramInvocation::from_input(path_hash, "partial-recovery-1", counter_input()).unwrap();
    let first_fingerprint = decode_fingerprint(&first_invocation.input_fingerprint).unwrap();
    let first_id = invocation_identity(&program_key, &first_invocation.command_id);
    let (first_lease, first_prepared) =
        prepare_with_canonical_bindings(&store, &engine, &context, &first_invocation).await;
    let first_consensus = commit_prepared_for_recovery(
        &coordinator,
        &store,
        &first_prepared,
        first_id,
        first_fingerprint,
        1_000,
    )
    .await;
    let first_applied = store
        .apply_program_bundle(
            first_lease,
            &first_prepared,
            program_commit(None, first_consensus.invocation.committed_batch),
            coordinator.one_node_mutation_context(nomination).unwrap(),
        )
        .await
        .unwrap();
    require_result_matches_consensus(&first_applied, first_consensus.invocation).unwrap();
    let first_cursor = first_consensus.invocation.committed_batch.commit_cursor;
    let counter_path = ObjectPath::new("tenant", "bucket", "managed/counter").unwrap();
    let first_version = first_applied.published_versions[&counter_path];

    let mut second_input = counter_input();
    let second_binding = &mut second_input.bindings.get_mut("counter").unwrap()[0];
    second_binding.expected_head = ExpectedHead::Version {
        version: first_version.version.0.to_string(),
    };
    second_binding.initial_json = None;
    let second_invocation =
        ProgramInvocation::from_input(path_hash, "partial-recovery-2", second_input).unwrap();
    let second_fingerprint = decode_fingerprint(&second_invocation.input_fingerprint).unwrap();
    let second_id = invocation_identity(&program_key, &second_invocation.command_id);
    let (second_lease, second_prepared) =
        prepare_with_canonical_bindings(&store, &engine, &context, &second_invocation).await;
    let second_consensus = commit_prepared_for_recovery(
        &coordinator,
        &store,
        &second_prepared,
        second_id,
        second_fingerprint,
        2_000,
    )
    .await;
    let second_applied = store
        .apply_program_bundle(
            second_lease,
            &second_prepared,
            program_commit(
                Some(first_cursor),
                second_consensus.invocation.committed_batch,
            ),
            coordinator.one_node_mutation_context(nomination).unwrap(),
        )
        .await
        .unwrap();
    require_result_matches_consensus(&second_applied, second_consensus.invocation).unwrap();
    let second_cursor = second_consensus.invocation.committed_batch.commit_cursor;
    assert_eq!(
        store.applied_program_commit_cursor().unwrap(),
        Some(second_cursor)
    );
    assert_eq!(
        coordinator
            .decisions
            .state()
            .unwrap()
            .unfinalized_commit_len(),
        2
    );

    drop(engine);
    coordinator.shutdown_recovery_worker().await;
    coordinator.decisions.shutdown().await.unwrap();
    drop(coordinator);
    drop(store);

    let reopened_store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    let reopened = open_test_coordinator(reopened_store, temporary.path()).await;
    let state = reopened.decisions.state().unwrap();
    assert_eq!(state.finalized_through(), Some(second_cursor));
    assert_eq!(state.unfinalized_commit_len(), 0);
    drop(state);
    reopened.shutdown_recovery_worker().await;
    reopened.decisions.shutdown().await.unwrap();
}

#[test]
fn program_and_invocation_identities_include_the_full_object_address() {
    let left = ObjectKey::new("tenant-a", "bucket", "_keldra/programs/import_osv@1").unwrap();
    let other_tenant =
        ObjectKey::new("tenant-b", "bucket", "_keldra/programs/import_osv@1").unwrap();
    let other_bucket =
        ObjectKey::new("tenant-a", "other", "_keldra/programs/import_osv@1").unwrap();

    assert_ne!(program_path_hash(&left), program_path_hash(&other_tenant));
    assert_ne!(program_path_hash(&left), program_path_hash(&other_bucket));
    assert_ne!(
        invocation_identity(&left, "same"),
        invocation_identity(&other_tenant, "same")
    );
}

#[test]
fn only_nonempty_reserved_program_paths_are_accepted() {
    let valid = ObjectKey::new("tenant", "bucket", "_keldra/programs/import_osv@1").unwrap();
    assert!(
        validate_program_request(
            &valid,
            "invoke-1",
            b"{}",
            LOCAL_DURABILITY_CLASS,
            ProgramRuntimeTopology::OneNode,
        )
        .is_ok()
    );

    let outside = ObjectKey::new("tenant", "bucket", "programs/import_osv@1").unwrap();
    assert!(
        validate_program_request(
            &outside,
            "invoke-1",
            b"{}",
            LOCAL_DURABILITY_CLASS,
            ProgramRuntimeTopology::OneNode,
        )
        .is_err()
    );
}

#[test]
fn program_durability_class_is_a_closed_exact_choice() {
    let key = ObjectKey::new("tenant", "bucket", "_keldra/programs/import_osv@1").unwrap();
    assert!(
        validate_program_request(
            &key,
            "invoke-1",
            b"{}",
            LOCAL_DURABILITY_CLASS,
            ProgramRuntimeTopology::OneNode,
        )
        .is_ok()
    );
    assert_eq!(
        validate_program_request(
            &key,
            "invoke-1",
            b"{}",
            REPLICATED_DURABILITY_CLASS,
            ProgramRuntimeTopology::OneNode,
        )
        .unwrap_err()
        .code(),
        tonic::Code::Unavailable
    );
    for supported in [LOCAL_DURABILITY_CLASS, REPLICATED_DURABILITY_CLASS] {
        assert!(
            validate_program_request(
                &key,
                "invoke-1",
                b"{}",
                supported,
                ProgramRuntimeTopology::Clustered,
            )
            .is_ok()
        );
    }
    for invalid in ["", " local", "local ", "LOCAL", "remote"] {
        assert_eq!(
            validate_program_request(
                &key,
                "invoke-1",
                b"{}",
                invalid,
                ProgramRuntimeTopology::Clustered,
            )
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn atomic_failure_statuses_expose_stable_outcome_names() {
    let assertion = engine_status(EngineError::Assertion {
        index: 0,
        reason: "no".into(),
    });
    assert_eq!(assertion.code(), tonic::Code::FailedPrecondition);
    assert!(assertion.message().starts_with("ASSERTION_FAILED:"));

    let concurrency = engine_status(EngineError::ProgramConcurrency {
        path: ObjectPath::new("tenant", "bucket", "managed/value").unwrap(),
        reason: "dependency must use PROGRAM_ONLY policy".into(),
    });
    assert!(
        concurrency
            .message()
            .starts_with("PROGRAM_CONCURRENCY_VIOLATION:")
    );

    let version = program_store_status(ProgramStoreError::ProgramHashMismatch);
    assert!(version.message().starts_with("PROGRAM_VERSION_MISMATCH:"));

    let capacity = decision_status(DecisionRaftError::Rejected(
        ApplyError::CommittedInvocationWindowFull {
            entries: keldra_consensus::MAX_COMMITTED_INVOCATIONS,
            bytes: keldra_consensus::MAX_COMMITTED_INVOCATION_BYTES,
            required_bytes: 1,
        },
    ));
    assert_eq!(capacity.code(), tonic::Code::ResourceExhausted);
    assert!(capacity.message().starts_with("RESOURCE_LIMIT:"));

    let lag = decision_status(DecisionRaftError::Rejected(ApplyError::CommitTailFull {
        entries: 1,
        bytes: 1,
        required_bytes: 1,
        max_entries: 1,
        max_bytes: 1,
    }));
    assert_eq!(lag.code(), tonic::Code::ResourceExhausted);
    assert!(lag.message().starts_with("FINALIZATION_LAG:"));
}

#[test]
fn local_durability_requires_synced_evidence_from_the_executor() {
    let evidence_hash = ProgramDurabilityEvidenceHash([7; 32]);
    assert_eq!(
        accepted_program_evidence_hash(
            &ProgramDurabilityScope::ExecutorLocal {
                node_id: 3,
                synced: true,
            },
            evidence_hash,
            LOCAL_DURABILITY_CLASS,
            NodeId(3),
        )
        .unwrap(),
        evidence_hash
    );

    let unsynced = accepted_program_evidence_hash(
        &ProgramDurabilityScope::ExecutorLocal {
            node_id: 3,
            synced: false,
        },
        evidence_hash,
        LOCAL_DURABILITY_CLASS,
        NodeId(3),
    )
    .unwrap_err();
    assert_eq!(unsynced.code(), tonic::Code::Unavailable);

    let wrong_node = accepted_program_evidence_hash(
        &ProgramDurabilityScope::ExecutorLocal {
            node_id: 4,
            synced: true,
        },
        evidence_hash,
        LOCAL_DURABILITY_CLASS,
        NodeId(3),
    )
    .unwrap_err();
    assert_eq!(wrong_node.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn commit_cursor_comparison_detects_pending_recovery() {
    assert_eq!(compare_commit_cursors(None, None), Ordering::Equal);
    assert_eq!(compare_commit_cursors(None, Some(10)), Ordering::Less);
    assert_eq!(compare_commit_cursors(Some(9), Some(10)), Ordering::Less);
    assert_eq!(compare_commit_cursors(Some(10), Some(10)), Ordering::Equal);
    assert_eq!(
        compare_commit_cursors(Some(11), Some(10)),
        Ordering::Greater
    );
    assert_eq!(compare_commit_cursors(Some(10), None), Ordering::Greater);
}

#[test]
fn committed_batch_mapping_retains_every_storage_identity() {
    let committed = CommittedBatch {
        commit_cursor: 12,
        executor: NodeId(3),
        nomination_log_index: 7,
        begin_cursor: 6,
        authority: AtomicBundleAuthority::StoredProgram {
            program_path_hash: ProgramPathHash([1; 32]),
            program_hash: DecisionProgramHash([2; 32]),
        },
        bundle_ref: BundleRef {
            hash: [3; 32],
            length: 33,
        },
        bundle_hash: BundleHash([4; 32]),
        durability_class: DurabilityClass([5; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([6; 32]),
        participant_manifest_hash: ParticipantManifestHash([7; 32]),
    };

    assert_eq!(
        program_commit(Some(11), committed),
        ProgramCommit {
            previous_commit_cursor: Some(11),
            commit_cursor: 12,
            begin_cursor: committed.begin_cursor,
            bundle_ref: PreparedBundleRef {
                hash: [3; 32],
                length: 33,
            },
            bundle_hash: PreparedBundleHash([4; 32]),
            program_hash: ProgramHash([2; 32]),
            authority: store_bundle_authority(committed.authority),
            participant_manifest_hash: committed.participant_manifest_hash.0,
            durability_class: ProgramDurabilityClassHash([5; 32]),
            durability_evidence_hash: ProgramDurabilityEvidenceHash([6; 32]),
        }
    );
}

#[test]
fn prepared_replay_result_must_match_the_committed_invocation() {
    let fingerprint = [9; 32];
    let committed = CommittedBatch {
        commit_cursor: 12,
        executor: NodeId(3),
        nomination_log_index: 7,
        begin_cursor: 6,
        authority: AtomicBundleAuthority::StoredProgram {
            program_path_hash: ProgramPathHash([1; 32]),
            program_hash: DecisionProgramHash([2; 32]),
        },
        bundle_ref: BundleRef {
            hash: [3; 32],
            length: 33,
        },
        bundle_hash: BundleHash([4; 32]),
        durability_class: DurabilityClass([5; 32]),
        durability_evidence_hash: DurabilityEvidenceHash([6; 32]),
        participant_manifest_hash: ParticipantManifestHash([7; 32]),
    };
    let invocation = CommittedInvocation {
        invocation_id: InvocationId([8; 32]),
        input_fingerprint: InvocationFingerprint(fingerprint),
        proposal_at_unix_millis: 1_000,
        replay_expires_at_unix_millis: 1_000 + ATOMIC_REPLAY_RETENTION_MILLIS,
        committed_batch: committed,
    };
    let result = CommittedProgramResult {
        receipt: CommandReceipt {
            program_path_hash: [1; 32],
            command_id: "recover-identity".into(),
            input_fingerprint: hex::encode(fingerprint),
            outputs: BTreeMap::new(),
        },
        published_versions: BTreeMap::new(),
        asserted_versions: BTreeMap::new(),
        alias_targets: BTreeMap::new(),
    };

    require_result_matches_consensus(&result, invocation).unwrap();
    let mut wrong = result;
    wrong.receipt.input_fingerprint = hex::encode([99; 32]);
    assert_eq!(
        require_result_matches_consensus(&wrong, invocation)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}
