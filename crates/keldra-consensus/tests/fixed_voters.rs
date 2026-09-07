use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use keldra_consensus::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterId, Command,
    DecisionRaft, DecisionRaftError, InMemoryPeerTransport, JoinCapabilityHash, NodeDescriptor,
    NodeId, NodeState, PeerAddress, PeerNode, PeerSpkiSha256,
};

fn joining_descriptor(node_id: u64) -> NodeDescriptor {
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

async fn open_node(
    path: &std::path::Path,
    node_id: u64,
    transport: &InMemoryPeerTransport,
) -> DecisionRaft {
    DecisionRaft::open_with_transport(path, node_id, 8, 128 * 1024, Arc::new(transport.clone()))
        .await
        .unwrap()
}

async fn establish_first_active(leader: &DecisionRaft) {
    leader
        .initialize_genesis(BTreeMap::from([(1, PeerNode::new("memory://1"))]))
        .await
        .unwrap();
    leader
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();
    leader
        .submit(Command::InitializeCluster {
            cluster_id: ClusterId([41; 16]),
        })
        .await
        .unwrap();
    let add = leader
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: joining_descriptor(1),
        })
        .await
        .unwrap();
    leader
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: add.log_index,
        })
        .await
        .unwrap();
    leader
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: add.log_index,
        })
        .await
        .unwrap();
}

async fn begin_add(leader: &DecisionRaft, node_id: u64) -> u64 {
    leader
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: joining_descriptor(node_id),
        })
        .await
        .unwrap()
        .log_index
}

async fn activate_and_finish(leader: &DecisionRaft, started_log_index: u64) {
    let activated = leader
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index,
        })
        .await
        .unwrap();
    assert!(matches!(
        activated.result,
        ApplyResult::MembershipTransitionAdvanced(_)
    ));
    leader
        .apply_fixed_voters_for_transition(started_log_index)
        .await
        .unwrap();
    // A lost response after the OpenRaft membership commit is a no-op retry.
    leader
        .apply_fixed_voters_for_transition(started_log_index)
        .await
        .unwrap();
    leader
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_voters_cover_one_two_three_and_four_active_nodes() {
    let directories = (0..4)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let transport = InMemoryPeerTransport::new();
    let mut nodes = Vec::new();
    for (offset, directory) in directories.iter().enumerate() {
        let node_id = offset as u64 + 1;
        let node = open_node(directory.path(), node_id, &transport).await;
        transport.register(node_id, node.clone()).unwrap();
        nodes.push(node);
    }
    let leader = &nodes[0];
    establish_first_active(leader).await;
    assert_eq!(
        leader.committed_voter_ids().unwrap(),
        BTreeSet::from([NodeId(1)])
    );

    for node_id in 2..=4 {
        let started_log_index = begin_add(leader, node_id).await;
        leader
            .catch_up_joining_learner(started_log_index)
            .await
            .unwrap();

        if node_id == 2 {
            let descriptor = leader.state().unwrap().cluster_control().nodes()[&NodeId(2)].clone();
            let rejected = leader
                .submit(Command::RefreshJoiningNodePreparation {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id: NodeId(2),
                    started_log_index,
                    expected_peer_spki_sha256: descriptor.current_peer_spki_sha256,
                    expected_join_capability_hash: descriptor.join_capability_hash.unwrap(),
                    replacement_peer_spki_sha256: PeerSpkiSha256([102; 32]),
                    replacement_join_capability_hash: JoinCapabilityHash([103; 32]),
                })
                .await
                .unwrap_err();
            assert!(matches!(
                rejected,
                DecisionRaftError::Rejected(ApplyError::JoiningNodeAlreadyRaftMember {
                    node_id: NodeId(2)
                })
            ));
        }

        // Catch-up is learner-only. Voter reconciliation is fenced until the
        // separate activation entry commits.
        assert!(
            !leader
                .committed_voter_ids()
                .unwrap()
                .contains(&NodeId(node_id))
        );
        assert!(
            leader
                .committed_learner_ids()
                .unwrap()
                .contains(&NodeId(node_id))
        );
        assert!(matches!(
            leader
                .apply_fixed_voters_for_transition(started_log_index)
                .await,
            Err(DecisionRaftError::Configuration(_))
        ));

        activate_and_finish(leader, started_log_index).await;
        if node_id == 2 {
            let rejected = leader
                .submit(Command::RefreshJoiningNodePreparation {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id: NodeId(2),
                    started_log_index,
                    expected_peer_spki_sha256: PeerSpkiSha256([2; 32]),
                    expected_join_capability_hash: JoinCapabilityHash([34; 32]),
                    replacement_peer_spki_sha256: PeerSpkiSha256([102; 32]),
                    replacement_join_capability_hash: JoinCapabilityHash([103; 32]),
                })
                .await
                .unwrap_err();
            assert!(matches!(
                rejected,
                DecisionRaftError::Rejected(ApplyError::JoiningNodeAlreadyRaftMember {
                    node_id: NodeId(2)
                })
            ));
        }
        let expected = match node_id {
            2 => BTreeSet::from([NodeId(1), NodeId(2)]),
            3 | 4 => BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]),
            _ => unreachable!(),
        };
        assert_eq!(leader.committed_voter_ids().unwrap(), expected);
    }
    assert_eq!(
        leader.committed_learner_ids().unwrap(),
        BTreeSet::from([NodeId(4)])
    );

    // Removing a voter retains the exact target and fills the vacancy from
    // the stable lowest-ID ACTIVE learner without a separate selection record.
    let remove = leader
        .submit(Command::BeginRemoveNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            node_id: NodeId(2),
        })
        .await
        .unwrap();
    assert_eq!(
        leader
            .apply_fixed_voters_for_transition(remove.log_index)
            .await
            .unwrap(),
        BTreeSet::from([NodeId(1), NodeId(3), NodeId(4)])
    );
    leader
        .apply_fixed_voters_for_transition(remove.log_index)
        .await
        .unwrap();
    leader
        .submit(Command::CompleteMembershipTransition {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            started_log_index: remove.log_index,
        })
        .await
        .unwrap();
    assert_eq!(
        leader.committed_voter_ids().unwrap(),
        BTreeSet::from([NodeId(1), NodeId(3), NodeId(4)])
    );
    assert!(!leader.committed_learner_ids().unwrap().contains(&NodeId(2)));

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn learner_catch_up_and_voter_application_resume_after_restart() {
    let first_directory = tempfile::tempdir().unwrap();
    let second_directory = tempfile::tempdir().unwrap();
    let transport = InMemoryPeerTransport::new();
    let first = open_node(first_directory.path(), 1, &transport).await;
    let second = open_node(second_directory.path(), 2, &transport).await;
    transport.register(1, first.clone()).unwrap();
    transport.register(2, second.clone()).unwrap();
    establish_first_active(&first).await;

    let started_log_index = begin_add(&first, 2).await;
    first
        .catch_up_joining_learner(started_log_index)
        .await
        .unwrap();
    first
        .catch_up_joining_learner(started_log_index)
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    transport.unregister(1).unwrap();
    drop(first);

    let first = open_node(first_directory.path(), 1, &transport).await;
    transport.register(1, first.clone()).unwrap();
    assert_eq!(
        first.wait_for_leader(Duration::from_secs(5)).await.unwrap(),
        1
    );
    assert_eq!(
        first
            .state()
            .unwrap()
            .cluster_control()
            .transition()
            .unwrap()
            .started_log_index,
        started_log_index
    );
    first
        .catch_up_joining_learner(started_log_index)
        .await
        .unwrap();
    activate_and_finish(&first, started_log_index).await;
    assert_eq!(
        first.committed_voter_ids().unwrap(),
        BTreeSet::from([NodeId(1), NodeId(2)])
    );
    assert!(
        first
            .state()
            .unwrap()
            .cluster_control()
            .transition()
            .is_none()
    );

    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}
