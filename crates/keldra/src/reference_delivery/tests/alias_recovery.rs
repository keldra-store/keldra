use keldra_atomic_program::{
    AtomicParticipant, AtomicWriteBundle, CommandReceipt, StoredValue, VersionedWrite,
};
use keldra_store::{
    OBJECT_ALIAS_REGISTRY_FORMAT, OBJECT_MUTATION_FORMAT, ObjectAliasRegistry, ObjectAliasSnapshot,
    PROGRAM_PATH_RESERVATION_FORMAT, ProgramAliasBinding, ProgramAliasRegistryCondition,
    ProgramAliasRegistryStage, ProgramObjectParticipant, ProgramParticipantIntent,
    ProgramPathCondition, ProgramPathReservation, ProgramReservation, ProgramReservationState,
    SealedAtomicBatchPublication, path_stage_from_prepared,
};

use super::*;

async fn alias_proof_fixture() -> (ProofFixture, Vec<LocalChange>, String) {
    let stores = TestStores::open(&[1]).await;
    let source = stores.stores[&NodeId(1)].clone();
    let canonical_path = node_one_coordinator_path("alias-proof");
    let key = ObjectKey::new("tenant", "bucket", &canonical_path).expect("canonical test key");
    publish(
        &source,
        &canonical_path,
        b"alias predecessor",
        "alias-predecessor",
    )
    .await;
    let predecessor = source
        .current_version_metadata(&key)
        .await
        .unwrap()
        .expect("canonical predecessor");
    publish(&source, &canonical_path, b"alias current", "alias-current").await;

    let source_id = source.local_watch_status().unwrap().source_id;
    let change = source
        .scan_local_changes(0, 16)
        .unwrap()
        .into_iter()
        .filter(|change| {
            matches!(change, LocalChange::ObjectHead(head) if head.exact_path == canonical_path)
        })
        .last()
        .expect("canonical replacement change");
    let original = source
        .read_reference_proof(source_id, change.offset())
        .unwrap()
        .expect("canonical replacement proof");
    let mut proof = original.clone();
    let aliases = node_two_coordinator_paths("aliases/recovery", 2);
    let ReferenceProofMutation::Object(mutation) = &mut proof.mutation else {
        panic!("canonical replacement must have an object proof");
    };
    mutation.format = OBJECT_MUTATION_FORMAT;
    mutation.alias_snapshot = Some(ObjectAliasSnapshot {
        registry: ObjectAliasRegistry {
            format: OBJECT_ALIAS_REGISTRY_FORMAT,
            revision: 1,
            aliases: aliases.clone(),
            program_commit_cursor: Some(1),
        },
        canonical_version: predecessor,
    });
    mutation.stamp.mutation_fingerprint = mutation.computed_fingerprint();
    proof.mutation_fingerprint = mutation.stamp.mutation_fingerprint;
    let LocalChange::ObjectHead(primary) = &mut proof.change else {
        panic!("canonical replacement proof changed event kind");
    };
    primary.accounting_transition = None;
    source
        .delete_reference_proof_if_matches(&original)
        .await
        .unwrap();
    source
        .install_quorum_reconciled_reference_proof(&proof)
        .await
        .unwrap();

    let LocalChange::ObjectHead(primary) = &change else {
        panic!("canonical fixture must be an object-head change");
    };
    let alias_changes = aliases
        .iter()
        .enumerate()
        .map(|(index, alias)| {
            let mut alias_change = primary.clone();
            alias_change.offset = primary.offset + 1 + index as u64;
            alias_change.exact_path = alias.clone();
            alias_change.canonical_path = Some(canonical_path.clone());
            alias_change.program_commit_cursor = None;
            alias_change.reference_deltas.clear();
            alias_change.accounting_transition = None;
            alias_change.definition_transition = None;
            LocalChange::ObjectHead(alias_change)
        })
        .collect();
    (
        ProofFixture {
            _stores: stores,
            source,
            source_id,
            change,
            proof,
        },
        alias_changes,
        canonical_path,
    )
}

async fn retained_alias_proof_fixture() -> (ProofFixture, Vec<LocalChange>, String) {
    let stores = TestStores::open(&[1]).await;
    let source = stores.stores[&NodeId(1)].clone();
    source
        .enable_bucket_versioning("tenant", "bucket")
        .await
        .expect("enable versioning for retained deletion");
    let canonical_path = node_one_coordinator_path("retained-alias-proof");
    let key = ObjectKey::new("tenant", "bucket", &canonical_path).expect("canonical test key");
    publish(
        &source,
        &canonical_path,
        b"retained predecessor",
        "retained-alias-predecessor",
    )
    .await;
    let predecessor = source
        .current_version_metadata(&key)
        .await
        .unwrap()
        .expect("retained predecessor");
    publish(
        &source,
        &canonical_path,
        b"retained current",
        "retained-alias-current",
    )
    .await;

    let coordinated = source
        .coordinate_retained_version_delete(
            &key,
            predecessor.id,
            keldra_store::ObjectMutationGovernance {
                tenant_id: 1,
                bucket_id: 1,
                versioning: ObjectVersioning::Enabled,
                policy: BucketPolicy::default(),
            },
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .expect("coordinate retained deletion");
    let mutation = coordinated
        .mutation
        .expect("new retained deletion mutation");
    let source_id = mutation.stamp.source_id;
    let original = source
        .read_reference_proof(source_id, mutation.stamp.source_journal_position)
        .unwrap()
        .expect("canonical retained deletion proof");
    let mut proof = original.clone();
    let aliases = node_two_coordinator_paths("aliases/retained-recovery", 2);
    let ReferenceProofMutation::RetainedVersionDelete(mutation) = &mut proof.mutation else {
        panic!("retained deletion must have a typed retained proof");
    };
    mutation.alias_paths = aliases.clone();
    mutation.stamp.mutation_fingerprint = mutation.computed_fingerprint();
    proof.mutation_fingerprint = mutation.stamp.mutation_fingerprint;
    let primary = {
        let LocalChange::RetainedVersionDeleted(primary) = &mut proof.change else {
            panic!("retained deletion proof changed event kind");
        };
        primary.accounting_transition = None;
        primary.clone()
    };
    source
        .delete_reference_proof_if_matches(&original)
        .await
        .unwrap();
    source
        .install_quorum_reconciled_reference_proof(&proof)
        .await
        .unwrap();

    let alias_changes = aliases
        .iter()
        .enumerate()
        .map(|(index, alias)| {
            let mut alias_change = primary.clone();
            alias_change.offset = primary.offset + 1 + index as u64;
            alias_change.exact_path = alias.clone();
            alias_change.canonical_path = Some(canonical_path.clone());
            alias_change.reference_deltas.clear();
            alias_change.accounting_transition = None;
            LocalChange::RetainedVersionDeleted(alias_change)
        })
        .collect();
    (
        ProofFixture {
            _stores: stores,
            source,
            source_id,
            change: proof.change.clone(),
            proof,
        },
        alias_changes,
        canonical_path,
    )
}

async fn install_alias_registry(store: &Store, canonical_path: &str, aliases: &[String]) {
    let key = ObjectKey::new("tenant", "bucket", canonical_path).expect("canonical test key");
    let current = store
        .current_version_metadata(&key)
        .await
        .unwrap()
        .expect("live canonical target");
    let path = ObjectPath::new("tenant", "bucket", canonical_path).unwrap();
    let authority = ProgramBundleAuthority::BuiltInObjectTransaction {
        kind: 1,
        contract_version: 1,
    };
    let bundle_hash = [0x41; 32];
    let manifest_hash = [0x42; 32];
    let reservation = ProgramReservation::Object(ProgramPathReservation {
        format: PROGRAM_PATH_RESERVATION_FORMAT,
        begin_cursor: 1,
        invocation_id: [0x43; 32],
        bundle_hash,
        participant_manifest_hash: manifest_hash,
        authority,
        executor_node_id: 1,
        nomination_log_index: 1,
        placement: PlacementLogId { term: 1, index: 1 },
        participant: ProgramObjectParticipant {
            tenant_id: 1,
            bucket_id: 1,
            path: path.clone(),
            condition: ProgramPathCondition::HeadVersion { expected: current },
            alias_registry: Some(ProgramAliasRegistryCondition::Absent),
            intent: ProgramParticipantIntent {
                read: true,
                put: true,
                delete: false,
            },
        },
        governance: ProgramGovernanceParticipant {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 1,
            policy: BucketPolicy::default(),
            versioning: ObjectVersioning::Unversioned,
        },
        state: ProgramReservationState::Prepared,
    });
    store
        .reserve_program_participant(&reservation)
        .await
        .expect("reserve canonical alias participant");
    let committed = store
        .commit_program_participant(&reservation, 2)
        .await
        .expect("commit canonical alias participant");
    store
        .coordinate_program_alias_registry_finalization(
            ProgramAliasRegistryStage {
                // The stage format is intentionally sealed but its validated wire value is 1.
                format: 1,
                begin_cursor: 1,
                bundle_hash: PreparedBundleHash(bundle_hash),
                program_hash: ProgramHash([0x44; 32]),
                authority,
                participant_manifest_hash: manifest_hash,
                tenant_id: 1,
                bucket_id: 1,
                target: path,
                expected: None,
                replacement_aliases: aliases.to_vec(),
            },
            2,
            ObjectMutationContext {
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
            },
        )
        .await
        .expect("install canonical alias registry");
    store
        .release_program_participant(&committed, Some(2))
        .await
        .expect("release canonical alias participant");
}

fn object_mutation(proof: &ReferenceProof) -> &ObjectMutation {
    let ReferenceProofMutation::Object(mutation) = &proof.mutation else {
        panic!("test proof must contain an object mutation");
    };
    mutation
}

#[tokio::test]
async fn alias_events_use_the_exact_canonical_primary_quorum_proof() {
    let (fixture, aliases, canonical_path) = alias_proof_fixture().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    let authority = fixture.authority(TestPlacement::new(placement(&[1, 2], 9)), peers.clone());

    for alias in &aliases {
        assert_eq!(
            authority.classify(fixture.source_id, alias).await.unwrap(),
            ReferenceCommitDisposition::CommittedOrAncestor
        );
    }

    let reads = peers.reads.lock().unwrap();
    assert_eq!(reads.len(), aliases.len());
    for (_, _, request) in reads.iter() {
        assert_eq!(request.offset, fixture.proof.offset());
        assert_eq!(request.exact_path, canonical_path);
        assert_eq!(request.placement_fence.index, 9);
    }
}

#[tokio::test]
async fn retained_alias_wakes_use_the_exact_canonical_delete_quorum_proof() {
    let (fixture, aliases, canonical_path) = retained_alias_proof_fixture().await;
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    let authority = fixture.authority(TestPlacement::new(placement(&[1, 2], 9)), peers.clone());

    for alias in &aliases {
        assert_eq!(
            authority.classify(fixture.source_id, alias).await.unwrap(),
            ReferenceCommitDisposition::CommittedOrAncestor
        );
    }

    let reads = peers.reads.lock().unwrap();
    assert_eq!(reads.len(), aliases.len());
    for (_, _, request) in reads.iter() {
        assert_eq!(request.offset, fixture.proof.offset());
        assert_eq!(request.exact_path, canonical_path);
        assert_eq!(request.placement_fence.index, 9);
    }
}

#[tokio::test]
async fn alias_event_must_match_the_primary_snapshot_path_offset_and_version() {
    let (fixture, aliases, _) = alias_proof_fixture().await;
    let authority = fixture.authority(
        TestPlacement::new(placement(&[1, 2], 1)),
        Arc::new(TestProofPeers::default()),
    );
    let LocalChange::ObjectHead(first) = &aliases[0] else {
        panic!("alias fixture must contain object-head changes");
    };

    let mut wrong_path = first.clone();
    wrong_path.exact_path.push_str("-wrong");
    assert!(
        authority
            .source_proof_for_change(fixture.source_id, &LocalChange::ObjectHead(wrong_path))
            .unwrap()
            .is_none()
    );

    let mut wrong_offset = first.clone();
    wrong_offset.offset += 1;
    assert!(
        authority
            .source_proof_for_change(fixture.source_id, &LocalChange::ObjectHead(wrong_offset))
            .unwrap()
            .is_none()
    );

    let mut wrong_version = first.clone();
    wrong_version.path_version = VersionId(wrong_version.path_version.0 + 1);
    assert!(
        authority
            .source_proof_for_change(fixture.source_id, &LocalChange::ObjectHead(wrong_version))
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn alias_primary_proof_still_obeys_the_placement_fence_recheck() {
    let (fixture, aliases, _) = alias_proof_fixture().await;
    let current = TestPlacement::new(placement(&[1, 2], 1));
    let peers = Arc::new(TestProofPeers::default());
    peers.respond(NodeId(2), Ok(Some(fixture.proof.clone())));
    peers.change_placement_on_read(current.clone(), placement(&[1, 2], 2));
    let error = fixture
        .authority(current, peers)
        .classify(fixture.source_id, &aliases[0])
        .await
        .unwrap_err();

    assert!(error.contains("placement changed during reference-proof read"));
}

#[tokio::test]
async fn restart_settles_real_alias_expansion_across_page_boundaries() {
    let directories = (0..3)
        .map(|_| tempfile::tempdir().expect("test directory"))
        .collect::<Vec<_>>();
    let mut stores = open_paths(&[1, 2, 3], &directories, None).await;
    let source = stores[&NodeId(1)].clone();
    let canonical_path = node_one_coordinator_path("canonical/restart-alias");
    let aliases = node_two_coordinator_paths("aliases/restart-alias", 2);

    publish(
        &source,
        &canonical_path,
        b"canonical predecessor",
        "restart-alias-predecessor",
    )
    .await;
    let source_id = source.local_watch_status().unwrap().source_id;
    let first_primary = source
        .scan_local_changes(0, 16)
        .unwrap()
        .into_iter()
        .find(|change| {
            matches!(change, LocalChange::ObjectHead(head) if head.exact_path == canonical_path)
        })
        .expect("initial canonical journal event");
    let first_proof = source
        .read_reference_proof(source_id, first_primary.offset())
        .unwrap()
        .expect("initial canonical proof");
    for node in [NodeId(2), NodeId(3)] {
        stores[&node]
            .apply_object_mutation_replica(object_mutation(&first_proof))
            .await
            .expect("replicate canonical predecessor");
    }
    for store in stores.values() {
        install_alias_registry(store, &canonical_path, &aliases).await;
    }

    publish(
        &source,
        &canonical_path,
        b"canonical replacement",
        "restart-alias-replacement",
    )
    .await;
    let changes = source.scan_local_changes(0, 32).unwrap();
    let primary = changes
        .iter()
        .filter(|change| {
            matches!(change, LocalChange::ObjectHead(head) if head.exact_path == canonical_path)
        })
        .last()
        .expect("replacement canonical journal event");
    let proof = source
        .read_reference_proof(source_id, primary.offset())
        .unwrap()
        .expect("replacement canonical proof");
    for node in [NodeId(2), NodeId(3)] {
        stores[&node]
            .apply_object_mutation_replica(object_mutation(&proof))
            .await
            .expect("replicate canonical replacement");
    }
    let alias_events = changes
        .iter()
        .filter_map(|change| match change {
            LocalChange::ObjectHead(head)
                if head.canonical_path.as_deref() == Some(canonical_path.as_str()) =>
            {
                Some(head)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        alias_events
            .iter()
            .map(|change| change.exact_path.as_str())
            .collect::<Vec<_>>(),
        aliases.iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(alias_events[0].offset, primary.offset() + 1);
    assert_eq!(alias_events[1].offset, primary.offset() + 2);
    let before_restart = source.local_watch_status().unwrap();
    assert!(before_restart.settled_through < alias_events[1].offset);

    drop(source);
    drop(stores.remove(&NodeId(1)).expect("source store"));
    let reopened = Store::open(StoreOptions::new(directories[0].path(), 1))
        .await
        .expect("reopen source store");
    stores.insert(NodeId(1), reopened.clone());
    let stores = Arc::new(stores);
    let peers = Arc::new(StoreMetadataPeers::new(stores.clone()));
    let current = TestPlacement::new(placement(&[1, 2, 3], 1));
    let authority = Arc::new(QuorumReferenceCommitAuthority::new(
        reopened.clone(),
        Arc::new(current.clone()),
        peers,
    ));
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(reopened.clone(), stores, order));
    let runner = ReferenceDelivery::new(
        reopened.clone(),
        Arc::new(current),
        authority,
        destinations,
        payloads,
        ErasureProfile::default(),
    )
    .with_page_size(1);

    for _ in 0..32 {
        let progress = runner
            .deliver_once()
            .await
            .expect("recover one bounded journal page");
        let status = reopened.local_watch_status().unwrap();
        if status.settled_through == status.tail && progress.reference_safe_through == status.tail {
            break;
        }
    }
    let recovered = reopened.local_watch_status().unwrap();
    assert_eq!(recovered.settled_through, recovered.tail);
    assert!(recovered.settled_through >= alias_events[1].offset);
}

#[tokio::test]
async fn restart_settles_atomic_publication_aliases_across_page_boundaries() {
    let directories = (0..3)
        .map(|_| tempfile::tempdir().expect("test directory"))
        .collect::<Vec<_>>();
    let mut stores = open_paths(&[1, 2, 3], &directories, None).await;
    let source = stores[&NodeId(1)].clone();
    let canonical_path = node_one_coordinator_path("canonical/atomic-alias");
    let aliases = node_two_coordinator_paths("aliases/atomic-alias", 2);
    let canonical_key =
        ObjectKey::new("tenant", "bucket", &canonical_path).expect("canonical test key");

    publish(
        &source,
        &canonical_path,
        b"atomic alias predecessor",
        "atomic-alias-predecessor",
    )
    .await;
    let source_id = source.local_watch_status().unwrap().source_id;
    let initial = source
        .scan_local_changes(0, 32)
        .unwrap()
        .into_iter()
        .find(|change| {
            matches!(change, LocalChange::ObjectHead(head) if head.exact_path == canonical_path)
        })
        .expect("initial canonical journal event");
    let initial_proof = source
        .read_reference_proof(source_id, initial.offset())
        .unwrap()
        .expect("initial canonical proof");
    for node in [NodeId(2), NodeId(3)] {
        stores[&node]
            .apply_object_mutation_replica(object_mutation(&initial_proof))
            .await
            .expect("replicate canonical predecessor");
    }
    for store in stores.values() {
        install_alias_registry(store, &canonical_path, &aliases).await;
    }

    let predecessor = source
        .current_version_metadata(&canonical_key)
        .await
        .unwrap()
        .expect("canonical predecessor metadata");
    let registry = source
        .object_alias_registry(1, 1, &canonical_path)
        .unwrap()
        .expect("canonical alias registry");
    let canonical = ObjectPath::new("tenant", "bucket", &canonical_path).unwrap();
    let expected = ObservedHead::Version {
        version: predecessor.id.0.to_string(),
    };
    let bundle = AtomicWriteBundle {
        participants: vec![AtomicParticipant {
            path: canonical.clone(),
            expected,
            write: Some(VersionedWrite {
                value: Some(StoredValue::Opaque(b"atomic alias replacement".to_vec())),
                content_type: Some("application/octet-stream".into()),
            }),
        }],
        receipt: CommandReceipt {
            program_path_hash: [0x51; 32],
            command_id: "atomic-alias-publication".into(),
            input_fingerprint: hex::encode([0x52; 32]),
            outputs: BTreeMap::new(),
        },
    };
    let governance: BTreeMap<(String, String), ProgramGovernanceParticipant> = BTreeMap::from([(
        ("tenant".into(), "bucket".into()),
        ProgramGovernanceParticipant {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 1,
            policy: BucketPolicy::default(),
            versioning: ObjectVersioning::Unversioned,
        },
    )]);
    let previous_versions = BTreeMap::from([(canonical.clone(), predecessor.clone())]);
    let alias_bindings = vec![ProgramAliasBinding {
        requested_path: canonical.clone(),
        canonical_path: canonical,
        descriptor_version: None,
        descriptor_bytes: None,
        canonical_version: Some(predecessor),
        alias_registry: Some(registry),
    }];
    let prepared = source
        .prepare_distributed_program_bundle_with_aliases(
            ProgramHash([0x53; 32]),
            &bundle,
            &previous_versions,
            &alias_bindings,
            &governance,
        )
        .await
        .expect("prepare alias-aware atomic bundle");
    let record = source
        .prepared_program_record(&prepared)
        .await
        .expect("load prepared atomic record");
    let begin_cursor = 71;
    let commit_cursor = 72;
    let stage = path_stage_from_prepared(
        &prepared,
        &record,
        record.writes().first().expect("one prepared write"),
        begin_cursor,
    )
    .expect("seal atomic path stage");
    let context = ObjectMutationContext {
        active_placement_log_id: PlacementLogId { term: 1, index: 1 },
        serving_fence_term: 1,
    };
    let reservations = record
        .reservations(
            begin_cursor,
            [0x54; 32],
            prepared.bundle.hash,
            1,
            1,
            context.active_placement_log_id,
        )
        .expect("seal atomic participant reservations");
    for store in stores.values() {
        for reservation in &reservations {
            store
                .reserve_program_participant(reservation)
                .await
                .expect("reserve atomic participant");
            store
                .commit_program_participant(reservation, commit_cursor)
                .await
                .expect("commit atomic participant");
        }
    }
    let finalized = source
        .coordinate_program_path_finalization(stage.clone(), commit_cursor, context)
        .await
        .expect("finalize atomic canonical path")
        .mutation;
    for node in [NodeId(2), NodeId(3)] {
        stores[&node]
            .apply_program_path_finalization_replica(&finalized, context)
            .await
            .expect("replicate atomic canonical finalization");
    }
    let publication = SealedAtomicBatchPublication::from_prepared(
        commit_cursor,
        prepared.bundle,
        &record,
        &[stage],
        &[finalized],
        &[],
    )
    .expect("seal complete atomic publication");
    assert!(
        source
            .publish_atomic_batch(publication)
            .await
            .expect("publish complete atomic batch")
    );

    let changes = source.scan_local_changes(0, 64).unwrap();
    let alias_events = changes
        .iter()
        .filter_map(|change| match change {
            LocalChange::ObjectHead(head)
                if head.program_commit_cursor == Some(commit_cursor)
                    && head.canonical_path.as_deref() == Some(canonical_path.as_str()) =>
            {
                Some(head)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        alias_events
            .iter()
            .map(|change| change.exact_path.as_str())
            .collect::<Vec<_>>(),
        aliases.iter().map(String::as_str).collect::<Vec<_>>()
    );
    let publication = changes
        .iter()
        .find_map(|change| match change {
            LocalChange::AtomicBatchPublished(batch) if batch.cursor == commit_cursor => {
                Some(batch)
            }
            _ => None,
        })
        .expect("complete atomic publication journal event");
    assert_eq!(publication.offset, alias_events[1].offset + 1);
    assert!(matches!(
        source.read_local_change(alias_events[0].offset).unwrap(),
        Some(LocalChange::ObjectHead(_))
    ));
    assert!(matches!(
        source.read_local_change(publication.offset).unwrap(),
        Some(LocalChange::AtomicBatchPublished(_))
    ));
    let before_restart = source.local_watch_status().unwrap();
    assert!(before_restart.settled_through < alias_events[0].offset);
    let first_alias_offset = alias_events[0].offset;
    let publication_offset = publication.offset;

    drop(source);
    drop(stores.remove(&NodeId(1)).expect("source store"));
    let reopened = Store::open(StoreOptions::new(directories[0].path(), 1))
        .await
        .expect("reopen source store");
    stores.insert(NodeId(1), reopened.clone());
    let stores = Arc::new(stores);
    let peers = Arc::new(StoreMetadataPeers::new(stores.clone()));
    let current = TestPlacement::new(placement(&[1, 2, 3], 1));
    let authority = Arc::new(QuorumReferenceCommitAuthority::new(
        reopened.clone(),
        Arc::new(current.clone()),
        peers,
    ));
    let reopened_alias = reopened
        .read_local_change(first_alias_offset)
        .unwrap()
        .expect("reopened atomic alias event");
    assert_eq!(
        authority
            .classify(source_id, &reopened_alias)
            .await
            .unwrap(),
        ReferenceCommitDisposition::CommittedOrAncestor
    );
    let order = Arc::new(Mutex::new(Vec::new()));
    let destinations = Arc::new(TestDestinations::new(stores.clone(), order.clone()));
    let payloads = Arc::new(TestPayloads::new(reopened.clone(), stores, order));
    let runner = ReferenceDelivery::new(
        reopened.clone(),
        Arc::new(current),
        authority,
        destinations,
        payloads,
        ErasureProfile::default(),
    )
    .with_page_size(1);

    for _ in 0..64 {
        let progress = runner
            .deliver_once()
            .await
            .expect("recover one bounded atomic journal page");
        let status = reopened.local_watch_status().unwrap();
        if status.settled_through == status.tail && progress.reference_safe_through == status.tail {
            break;
        }
    }
    let recovered = reopened.local_watch_status().unwrap();
    assert_eq!(recovered.settled_through, recovered.tail);
    assert!(recovered.settled_through >= publication_offset);
}
