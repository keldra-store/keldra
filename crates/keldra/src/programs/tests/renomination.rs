use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use keldra_consensus::{
    CapabilityRange, ClusterId, CommittedPeerPinProvider, CommittedPeerPins, InMemoryPeerTransport,
    JoinCapabilityHash, NodeDescriptor, NodeState, PeerAddress, PeerNode, PeerRpcKind,
    PeerSpkiSha256,
};
use keldra_store::{ProgramGovernanceParticipant, path_stage_from_prepared};
use tonic::Request;

use super::*;
use crate::cluster_peer::wire::cluster_peer_server::ClusterPeer;
use crate::cluster_peer::{
    CLUSTER_PEER_SCHEMA_VERSION, ClusterPeerService, LateBoundDistributedControl,
    LateBoundFreshAuthorization, RoutedAccountingHandlers, RoutedAuthzHandlers,
    RoutedIndexQueryHandlers, RoutedPublicHandlers, wire,
};
use crate::distributed_list::LateBoundListAuthorizer;
use crate::index_runtime::publication::LateBoundIndexArtifactPublication;
use crate::logical_name_resolution::LateBoundLogicalNameResolution;
use crate::personaldb::RoutedPersonalDbHandlers;
use crate::placement::PlacementKind;

const TEST_CLUSTER_ID: ClusterId = ClusterId([0x51; 16]);

fn descriptor(node_id: u64) -> NodeDescriptor {
    NodeDescriptor {
        node_id: NodeId(node_id),
        peer_address: PeerAddress(format!("memory://{node_id}")),
        storage_weight_millionths: 1_000_000,
        state: NodeState::Joining,
        current_peer_spki_sha256: PeerSpkiSha256([node_id as u8; 32]),
        overlap_peer_spki_sha256: None,
        join_capability_hash: Some(JoinCapabilityHash([(node_id + 32) as u8; 32])),
        supported_protocol: CapabilityRange { min: 1, max: 1 },
        supported_storage_format: CapabilityRange { min: 1, max: 1 },
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "two-node decision state did not converge before timeout"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn open_two_node_cluster(
    first_root: &Path,
    second_root: &Path,
) -> (DecisionRaft, DecisionRaft) {
    let transport = InMemoryPeerTransport::new();
    let first = DecisionRaft::open_with_transport(
        first_root,
        1,
        8,
        128 * 1024,
        Arc::new(transport.clone()),
    )
    .await
    .unwrap();
    let second = DecisionRaft::open_with_transport(
        second_root,
        2,
        8,
        128 * 1024,
        Arc::new(transport.clone()),
    )
    .await
    .unwrap();
    transport.register(1, first.clone()).unwrap();
    transport.register(2, second.clone()).unwrap();

    first
        .initialize_genesis(BTreeMap::from([(1, PeerNode::new("memory://1"))]))
        .await
        .unwrap();
    assert_eq!(
        first.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    first
        .submit(Command::InitializeCluster {
            cluster_id: TEST_CLUSTER_ID,
        })
        .await
        .unwrap();
    let first_add = first
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: descriptor(1),
        })
        .await
        .unwrap();
    for _ in 0..2 {
        first
            .submit(Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: first_add.log_index,
            })
            .await
            .unwrap();
    }

    let second_add = first
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: descriptor(2),
        })
        .await
        .unwrap();
    first
        .catch_up_joining_learner(second_add.log_index)
        .await
        .unwrap();
    first
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: second_add.log_index,
        })
        .await
        .unwrap();
    assert_eq!(
        first
            .apply_fixed_voters_for_transition(second_add.log_index)
            .await
            .unwrap(),
        BTreeSet::from([NodeId(1), NodeId(2)])
    );
    first
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: second_add.log_index,
        })
        .await
        .unwrap();
    wait_until(|| {
        let state = second.state().unwrap();
        state.cluster_control().nodes().len() == 2
            && state.cluster_control().active_protocol_version() == 1
            && state.cluster_control().active_storage_format() == 1
    })
    .await;
    (first, second)
}

#[derive(Clone)]
struct TestPins;

impl CommittedPeerPinProvider for TestPins {
    fn connection_pins(&self, node_id: NodeId) -> Option<CommittedPeerPins> {
        (matches!(node_id.0, 1 | 2)).then_some(CommittedPeerPins {
            current: PeerSpkiSha256([node_id.0 as u8; 32]),
            overlap: None,
        })
    }

    fn authorized_rpc_pins(
        &self,
        cluster_id: ClusterId,
        node_id: NodeId,
        kind: PeerRpcKind,
    ) -> Option<CommittedPeerPins> {
        if cluster_id == TEST_CLUSTER_ID && kind == PeerRpcKind::DataPlane {
            self.connection_pins(node_id)
        } else {
            None
        }
    }
}

fn peer_service(store: Store, decisions: DecisionRaft) -> ClusterPeerService {
    ClusterPeerService::new(
        NodeId(2),
        store,
        decisions,
        Arc::new(TestPins),
        Arc::new(LateBoundListAuthorizer::default()),
        LateBoundFreshAuthorization::default(),
        LateBoundDistributedControl::default(),
        LateBoundLogicalNameResolution::default(),
        LateBoundIndexArtifactPublication::default(),
        RoutedIndexQueryHandlers::default(),
        RoutedAccountingHandlers::default(),
        RoutedPersonalDbHandlers::default(),
        RoutedPublicHandlers::default(),
        RoutedAuthzHandlers::default(),
        Duration::from_secs(30),
    )
}

fn peer_context(source: NodeId, placement: PlacementLogId) -> wire::PeerContext {
    wire::PeerContext {
        schema_version: CLUSTER_PEER_SCHEMA_VERSION,
        cluster_id: TEST_CLUSTER_ID.0.to_vec(),
        source_node_id: source.0,
        placement_term: placement.term,
        placement_index: placement.index,
        hop_count: 0,
        remaining_deadline_millis: 30_000,
    }
}

fn authenticated<T>(value: T, source: NodeId) -> Request<T> {
    let mut request = Request::new(value);
    request
        .extensions_mut()
        .insert(PeerSpkiSha256([source.0 as u8; 32]));
    request
}

fn counter_input_with_node_one_authority(
    placement: &crate::cluster_placement::ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
) -> ProgramInput {
    for suffix in 0..256 {
        let name = format!("renomination-{suffix}");
        let path = format!("managed/{name}");
        let mut placement_key = Vec::with_capacity(16 + path.len());
        placement_key.extend_from_slice(&tenant_id.to_be_bytes());
        placement_key.extend_from_slice(&bucket_id.to_be_bytes());
        placement_key.extend_from_slice(path.as_bytes());
        if placement.rank(PlacementKind::Object, &placement_key)[0] != NodeId(1) {
            continue;
        }
        let mut input = counter_input();
        let binding = &mut input.bindings.get_mut("counter").unwrap()[0];
        binding.path = ObjectPath::new("tenant", "bucket", &path).unwrap();
        binding.template_values.insert("counter_id".into(), name);
        return input;
    }
    panic!("bounded path search found no node-one object authority")
}

async fn peer_reserve(
    service: &ClusterPeerService,
    source: NodeId,
    nomination_log_index: u64,
    placement: PlacementLogId,
    reservation: &ProgramReservation,
) {
    ClusterPeer::reserve_program_participant(
        service,
        authenticated(
            wire::ProgramReserveParticipantRequest {
                peer: Some(peer_context(source, placement)),
                executor_nomination_log_index: nomination_log_index,
                reservation_json: serde_json::to_vec(reservation).unwrap(),
            },
            source,
        ),
    )
    .await
    .unwrap();
}

async fn peer_commit(
    service: &ClusterPeerService,
    source: NodeId,
    nomination_log_index: u64,
    placement: PlacementLogId,
    commit_cursor: u64,
    reservation: &ProgramReservation,
) {
    ClusterPeer::commit_program_participant(
        service,
        authenticated(
            wire::ProgramCommitParticipantRequest {
                peer: Some(peer_context(source, placement)),
                executor_nomination_log_index: nomination_log_index,
                commit_cursor,
                reservation_json: serde_json::to_vec(reservation).unwrap(),
            },
            source,
        ),
    )
    .await
    .unwrap();
}

async fn peer_release(
    service: &ClusterPeerService,
    source: NodeId,
    nomination_log_index: u64,
    placement: PlacementLogId,
    commit_cursor: u64,
    reservation: &ProgramReservation,
) {
    ClusterPeer::release_program_participant(
        service,
        authenticated(
            wire::ProgramReleaseParticipantRequest {
                peer: Some(peer_context(source, placement)),
                executor_nomination_log_index: nomination_log_index,
                reservation_json: serde_json::to_vec(reservation).unwrap(),
                finalized_commit_cursor: commit_cursor,
            },
            source,
        ),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_path_recovery_rebinds_to_the_new_executor_without_resealing() {
    let first_store_root = tempfile::tempdir().unwrap();
    let (store, program_key, program_hash, _) =
        configured_program_store(first_store_root.path()).await;
    let second_root = tempfile::tempdir().unwrap();
    let replica = Store::open(StoreOptions::new(second_root.path(), 2))
        .await
        .unwrap();
    let (first, second) = open_two_node_cluster(
        &first_store_root.path().join("decisions"),
        &second_root.path().join("decisions"),
    )
    .await;
    let placement =
        crate::cluster_placement::ClusterPlacement::from_applied(&first.state().unwrap()).unwrap();
    let placement_fence = placement.fence();
    let old_nomination = expect_nomination(
        first
            .submit(Command::NominateExecutor {
                executor: NodeId(1),
            })
            .await
            .unwrap()
            .result,
        NodeId(1),
    )
    .unwrap();
    wait_until(|| second.state().unwrap().executor() == Some(old_nomination)).await;

    let (tenant_id, bucket_id) = store.resolve_bucket_ids("tenant", "bucket").unwrap();
    let input = counter_input_with_node_one_authority(&placement, tenant_id, bucket_id);
    let invocation = ProgramInvocation::from_input(
        program_path_hash(&program_key),
        "distributed-renomination",
        input,
    )
    .unwrap();
    let fingerprint = decode_fingerprint(&invocation.input_fingerprint).unwrap();
    let invocation_id = invocation_identity(&program_key, &invocation.command_id);
    let program = store.get(&program_key).await.unwrap().unwrap();
    let definition =
        VerifiedProgramDefinition::from_bytes(&program.bytes, ProgramHash(program_hash)).unwrap();
    let engine = store.program_engine(&definition).unwrap();
    let lease = engine
        .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
        .await
        .unwrap();
    let governance = BTreeMap::from([(
        ("tenant".into(), "bucket".into()),
        ProgramGovernanceParticipant {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id,
            bucket_id,
            policy: BucketPolicy {
                immutable_prefixes: Vec::new(),
                program_only_prefixes: vec!["managed".into()],
            },
            versioning: ObjectVersioning::Unversioned,
        },
    )]);
    let mut prepared = store
        .prepare_distributed_program_bundle(
            definition.hash,
            lease.bundle(),
            &BTreeMap::new(),
            &governance,
        )
        .await
        .unwrap();
    prepared
        .attest_remote_durability(REPLICATED_DURABILITY_CLASS)
        .unwrap();
    let record = store.prepared_program_record(&prepared).await.unwrap();
    let proposed_at = current_unix_millis().unwrap();
    let begun = first
        .submit(Command::BeginBatch(BeginBatch {
            executor: old_nomination.executor,
            nomination_log_index: old_nomination.nomination_log_index,
            authority: decision_bundle_authority(prepared.authority),
            invocation_id,
            input_fingerprint: InvocationFingerprint(fingerprint),
            bundle_ref: BundleRef {
                hash: prepared.bundle.hash.0,
                length: prepared.bundle.length,
            },
            durability_class: DurabilityClass(
                ProgramDurabilityClassHash::for_class(REPLICATED_DURABILITY_CLASS).0,
            ),
            durability_evidence_hash: DurabilityEvidenceHash(prepared.durability_evidence_hash.0),
            participant_manifest_hash: ParticipantManifestHash(prepared.participant_manifest_hash),
            proposal_at_unix_millis: proposed_at,
            replay_expires_at_unix_millis: proposed_at + ATOMIC_REPLAY_RETENTION_MILLIS,
        }))
        .await
        .unwrap();
    let keldra_consensus::BeginResult::Prepared { batch, .. } =
        expect_batch_begun(begun.result).unwrap()
    else {
        panic!("fresh distributed invocation unexpectedly replayed")
    };
    wait_until(|| {
        second
            .state()
            .unwrap()
            .preparing_batch()
            .is_some_and(|current| current.begin_cursor == batch.begin_cursor)
    })
    .await;

    let stage = path_stage_from_prepared(
        &prepared,
        &record,
        record.writes().first().unwrap(),
        batch.begin_cursor,
    )
    .unwrap();
    let service = peer_service(replica.clone(), second.clone());
    let local_stage = store.persist_program_path_stage(&stage).await.unwrap();
    let peer_stage = ClusterPeer::stage_program_path(
        &service,
        authenticated(
            wire::ProgramStagePathRequest {
                peer: Some(peer_context(NodeId(1), placement_fence)),
                executor_nomination_log_index: old_nomination.nomination_log_index,
                stage_json: serde_json::to_vec(&stage).unwrap(),
            },
            NodeId(1),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(peer_stage.stage_blob_hash, local_stage.hash);
    assert_eq!(peer_stage.stage_blob_length, local_stage.length);
    let replica_stage_tail = replica.local_invalidation_offset().unwrap();

    let old_reservation = record
        .reservations(
            batch.begin_cursor,
            invocation_id.0,
            prepared.bundle.hash,
            old_nomination.executor.0,
            old_nomination.nomination_log_index,
            placement_fence,
        )
        .unwrap()
        .into_iter()
        .find(|reservation| matches!(reservation, ProgramReservation::Object(_)))
        .unwrap();
    store
        .reserve_program_participant(&old_reservation)
        .await
        .unwrap();
    peer_reserve(
        &service,
        NodeId(1),
        old_nomination.nomination_log_index,
        placement_fence,
        &old_reservation,
    )
    .await;

    let expected_commit_cursor = batch.begin_cursor.checked_add(1).unwrap();
    let early_peer_commit = peer_commit(
        &service,
        NodeId(1),
        old_nomination.nomination_log_index,
        placement_fence,
        expected_commit_cursor,
        &old_reservation,
    );
    tokio::pin!(early_peer_commit);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut early_peer_commit)
            .await
            .is_err(),
        "reservation commit must wait for its local Raft decision"
    );
    let committed = first
        .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
            executor: old_nomination.executor,
            nomination_log_index: old_nomination.nomination_log_index,
            begin_cursor: batch.begin_cursor,
            invocation_id,
            participant_manifest_hash: ParticipantManifestHash(prepared.participant_manifest_hash),
        }))
        .await
        .unwrap();
    let committed = expect_batch_committed(committed.result).unwrap();
    let commit_cursor = committed.invocation.committed_batch.commit_cursor;
    assert_eq!(commit_cursor, expected_commit_cursor);
    store
        .commit_program_participant(&old_reservation, commit_cursor)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut early_peer_commit)
        .await
        .expect("reservation commit did not resume after local Raft apply");

    let old_context = ObjectMutationContext {
        active_placement_log_id: placement_fence,
        serving_fence_term: old_nomination.nomination_log_index,
    };
    let sealed = store
        .coordinate_program_path_finalization(stage.clone(), commit_cursor, old_context)
        .await
        .unwrap();
    assert!(!sealed.replayed);
    // Crash boundary: Raft committed and the path authority materialized the
    // mutation, while the replica retains only its stage and reservation.
    assert_eq!(
        replica.local_invalidation_offset().unwrap(),
        replica_stage_tail,
        "the committed path is not visible on the replica before recovery"
    );

    let replacement = expect_nomination(
        first
            .submit(Command::NominateExecutor {
                executor: NodeId(2),
            })
            .await
            .unwrap()
            .result,
        NodeId(2),
    )
    .unwrap();
    assert!(replacement.nomination_log_index > old_nomination.nomination_log_index);
    wait_until(|| second.state().unwrap().executor() == Some(replacement)).await;
    let retained = first
        .state()
        .unwrap()
        .committed_invocation(commit_cursor)
        .unwrap();
    assert_eq!(retained.committed_batch.executor, old_nomination.executor);
    assert_eq!(
        retained.committed_batch.nomination_log_index,
        old_nomination.nomination_log_index
    );

    let rebound = record
        .reservations(
            batch.begin_cursor,
            invocation_id.0,
            prepared.bundle.hash,
            replacement.executor.0,
            replacement.nomination_log_index,
            placement_fence,
        )
        .unwrap()
        .into_iter()
        .find(|reservation| matches!(reservation, ProgramReservation::Object(_)))
        .unwrap();
    // Recovery ownership moves to the new nomination, but the already-sealed
    // coordinator mutation below remains immutable old-executor provenance.
    store.reserve_program_participant(&rebound).await.unwrap();
    peer_reserve(
        &service,
        NodeId(2),
        replacement.nomination_log_index,
        placement_fence,
        &rebound,
    )
    .await;
    store
        .commit_program_participant(&rebound, commit_cursor)
        .await
        .unwrap();
    peer_commit(
        &service,
        NodeId(2),
        replacement.nomination_log_index,
        placement_fence,
        commit_cursor,
        &rebound,
    )
    .await;

    let replacement_context = ObjectMutationContext {
        active_placement_log_id: placement_fence,
        serving_fence_term: replacement.nomination_log_index,
    };
    let replay = store
        .coordinate_program_path_finalization(stage, commit_cursor, replacement_context)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.mutation, sealed.mutation);
    assert_eq!(
        replay.mutation.stamp.serving_fence_term,
        old_nomination.nomination_log_index
    );
    assert_eq!(
        replay.mutation.stamp.mutation_fingerprint,
        sealed.mutation.stamp.mutation_fingerprint
    );
    assert_eq!(
        replay.mutation.computed_fingerprint().unwrap(),
        sealed.mutation.stamp.mutation_fingerprint
    );
    let peer_finalization = ClusterPeer::apply_program_path_finalization(
        &service,
        authenticated(
            wire::ProgramApplyPathFinalizationRequest {
                peer: Some(peer_context(NodeId(2), placement_fence)),
                executor_nomination_log_index: replacement.nomination_log_index,
                mutation_json: serde_json::to_vec(&replay.mutation).unwrap(),
            },
            NodeId(2),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!peer_finalization.replayed);
    assert_eq!(
        peer_finalization.version,
        replay.mutation.stage.version.id.0
    );
    assert_eq!(
        replica
            .export_object_path_record(
                replay.mutation.stage.tenant_id,
                replay.mutation.stage.bucket_id,
                &replay.mutation.stage.path.path,
            )
            .unwrap()
            .unwrap()
            .head
            .mutation_stamp
            .unwrap(),
        sealed.mutation.stamp
    );
    let peer_replay = ClusterPeer::apply_program_path_finalization(
        &service,
        authenticated(
            wire::ProgramApplyPathFinalizationRequest {
                peer: Some(peer_context(NodeId(2), placement_fence)),
                executor_nomination_log_index: replacement.nomination_log_index,
                mutation_json: serde_json::to_vec(&replay.mutation).unwrap(),
            },
            NodeId(2),
        ),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(peer_replay.replayed);
    assert_eq!(peer_replay.version, replay.mutation.stage.version.id.0);

    let early_peer_release = peer_release(
        &service,
        NodeId(2),
        replacement.nomination_log_index,
        placement_fence,
        commit_cursor,
        &rebound,
    );
    tokio::pin!(early_peer_release);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut early_peer_release)
            .await
            .is_err(),
        "reservation release must wait for local FinalizedThrough apply"
    );
    first
        .submit(Command::FinalizedThrough {
            executor: replacement.executor,
            nomination_log_index: replacement.nomination_log_index,
            through_commit_cursor: commit_cursor,
        })
        .await
        .unwrap();
    store
        .release_program_participant(&rebound, Some(commit_cursor))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut early_peer_release)
        .await
        .expect("reservation release did not resume after local FinalizedThrough apply");
    assert!(store.program_reservations().unwrap().is_empty());
    assert!(replica.program_reservations().unwrap().is_empty());

    drop(lease);
    drop(engine);
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}
