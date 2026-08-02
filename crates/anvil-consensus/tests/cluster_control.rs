use anvil_consensus::{
    ApplyError, ApplyResult, CLUSTER_CONTROL_COMMAND_VERSION, CapabilityRange, ClusterId, Command,
    ErasureCodeProfile, JoinCapabilityHash, JwtSigningKeyFingerprint, MembershipTransitionKind,
    NodeDescriptor, NodeId, NodeState, PeerAddress, PeerSpkiSha256, StateMachine,
};
use openraft::{CommittedLeaderId, LogId};

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(7, 3), index)
}

fn initialize(state: &mut StateMachine) {
    state
        .apply(
            log_id(1),
            &Command::InitializeCluster {
                cluster_id: ClusterId([1; 16]),
            },
        )
        .unwrap();
}

fn joining(node_id: u64) -> NodeDescriptor {
    NodeDescriptor {
        node_id: NodeId(node_id),
        peer_address: PeerAddress(format!("node-{node_id}:7443")),
        storage_weight_millionths: 1_000_000,
        state: NodeState::Joining,
        current_peer_spki_sha256: PeerSpkiSha256([node_id as u8; 32]),
        overlap_peer_spki_sha256: None,
        join_capability_hash: Some(JoinCapabilityHash([(node_id + 32) as u8; 32])),
        supported_protocol: CapabilityRange { min: 1, max: 1 },
        supported_storage_format: CapabilityRange { min: 1, max: 1 },
    }
}

fn begin_add(state: &mut StateMachine, log_index: u64, descriptor: NodeDescriptor) {
    let result = state
        .apply(
            log_id(log_index),
            &Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: descriptor.clone(),
            },
        )
        .unwrap();
    let ApplyResult::MembershipTransitionBegun(transition) = result else {
        panic!("ADD returned the wrong result")
    };
    assert_eq!(transition.kind, MembershipTransitionKind::Add);
    assert_eq!(transition.node_id, descriptor.node_id);
    assert_eq!(transition.started_log_index, log_index);
    assert_eq!(
        transition.target_weight_millionths,
        Some(descriptor.storage_weight_millionths)
    );
}

fn complete(state: &mut StateMachine, log_index: u64, started_log_index: u64) {
    assert_eq!(
        state
            .apply(
                log_id(log_index),
                &Command::CompleteMembershipTransition {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    started_log_index,
                },
            )
            .unwrap(),
        ApplyResult::MembershipTransitionFinished { started_log_index }
    );
}

fn admit(state: &mut StateMachine, begin_log: u64, node_id: u64) {
    begin_add(state, begin_log, joining(node_id));
    let transition = state
        .apply(
            log_id(begin_log + 1),
            &Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: begin_log,
            },
        )
        .unwrap();
    assert!(matches!(
        transition,
        ApplyResult::MembershipTransitionAdvanced(_)
    ));
    complete(state, begin_log + 2, begin_log);
}

#[test]
fn add_is_bounded_fenced_and_sets_the_fixed_voter_target() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);

    begin_add(&mut state, 2, joining(1));
    let cluster = state.cluster_control();
    assert!(cluster.used_node_ids().contains(NodeId(1)));
    assert_eq!(cluster.active_node_count(), 0);
    assert_eq!(cluster.voter_target(), 0);
    assert_eq!(cluster.nodes()[&NodeId(1)].state, NodeState::Joining);
    assert_eq!(
        state.apply(
            log_id(3),
            &Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: 99,
            }
        ),
        Err(ApplyError::MembershipTransitionFenceMismatch {
            expected: 2,
            requested: 99,
        })
    );
    assert!(matches!(
        state
            .apply(
                log_id(4),
                &Command::CompleteMembershipTransition {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    started_log_index: 2,
                },
            )
            .unwrap(),
        ApplyResult::MembershipTransitionAdvanced(_)
    ));
    complete(&mut state, 5, 2);
    assert_eq!(state.cluster_control().active_node_count(), 1);
    assert_eq!(state.cluster_control().voter_target(), 1);
    assert_eq!(
        state.cluster_control().nodes()[&NodeId(1)].join_capability_hash,
        None
    );

    admit(&mut state, 10, 2);
    assert_eq!(state.cluster_control().voter_target(), 2);
    admit(&mut state, 20, 3);
    assert_eq!(state.cluster_control().voter_target(), 3);
    admit(&mut state, 30, 4);
    assert_eq!(state.cluster_control().voter_target(), 3);
}

#[test]
fn joining_preparation_refresh_replaces_only_the_unused_pair_and_retries_exactly() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    let original = joining(1);
    begin_add(&mut state, 2, original.clone());
    let transition = state.cluster_control().transition().cloned().unwrap();
    let replacement_pin = PeerSpkiSha256([91; 32]);
    let replacement_capability = JoinCapabilityHash([92; 32]);
    let command = Command::RefreshJoiningNodePreparation {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        node_id: NodeId(1),
        started_log_index: transition.started_log_index,
        expected_peer_spki_sha256: original.current_peer_spki_sha256,
        expected_join_capability_hash: original.join_capability_hash.unwrap(),
        replacement_peer_spki_sha256: replacement_pin,
        replacement_join_capability_hash: replacement_capability,
    };

    let mut refreshed = None;
    for log_index in [3, 4] {
        let ApplyResult::JoiningNodePreparationRefreshed(descriptor) =
            state.apply(log_id(log_index), &command).unwrap()
        else {
            panic!("refresh returned the wrong result")
        };
        assert_eq!(descriptor.current_peer_spki_sha256, replacement_pin);
        assert_eq!(
            descriptor.join_capability_hash,
            Some(replacement_capability)
        );
        refreshed = Some(descriptor);
    }
    let refreshed = refreshed.unwrap();

    let mut expected = original.clone();
    expected.current_peer_spki_sha256 = replacement_pin;
    expected.join_capability_hash = Some(replacement_capability);
    assert_eq!(refreshed, expected);
    assert_eq!(state.cluster_control().nodes()[&NodeId(1)], expected);
    assert_eq!(
        state.cluster_control().transition(),
        Some(&transition),
        "refresh must retain the ADD fence and target"
    );

    let before_rejection = state.clone();
    assert_eq!(
        state.apply(
            log_id(5),
            &Command::RefreshJoiningNodePreparation {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                started_log_index: transition.started_log_index,
                expected_peer_spki_sha256: original.current_peer_spki_sha256,
                expected_join_capability_hash: original.join_capability_hash.unwrap(),
                replacement_peer_spki_sha256: PeerSpkiSha256([93; 32]),
                replacement_join_capability_hash: JoinCapabilityHash([94; 32]),
            },
        ),
        Err(ApplyError::JoiningPreparationPairMismatch { node_id: NodeId(1) })
    );
    assert_eq!(state.cluster_control(), before_rejection.cluster_control());
}

#[test]
fn active_placement_log_id_changes_only_with_active_placement() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    assert_eq!(state.cluster_control().active_placement_log_id(), None);

    let first = joining(1);
    begin_add(&mut state, 2, first.clone());
    assert_eq!(state.cluster_control().active_placement_log_id(), None);
    let replay = state
        .apply(
            log_id(3),
            &Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: first,
            },
        )
        .unwrap();
    assert!(matches!(
        replay,
        ApplyResult::MembershipTransitionBegun(ref transition)
            if transition.started_log_index == 2
    ));
    assert_eq!(state.cluster_control().active_placement_log_id(), None);

    assert!(matches!(
        state
            .apply(
                log_id(4),
                &Command::CompleteMembershipTransition {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    started_log_index: 2,
                },
            )
            .unwrap(),
        ApplyResult::MembershipTransitionAdvanced(_)
    ));
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(4))
    );
    complete(&mut state, 5, 2);
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(4))
    );

    begin_add(&mut state, 10, joining(2));
    state
        .apply(
            log_id(11),
            &Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: 10,
            },
        )
        .unwrap();
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(11))
    );
    complete(&mut state, 12, 10);
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(11))
    );

    let reweight = Command::BeginReweightNode {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        node_id: NodeId(1),
        storage_weight_millionths: 2_000_000,
    };
    state.apply(log_id(20), &reweight).unwrap();
    state.apply(log_id(21), &reweight).unwrap();
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(11))
    );
    complete(&mut state, 22, 20);
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(22))
    );

    let remove = Command::BeginRemoveNode {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        node_id: NodeId(1),
    };
    state.apply(log_id(30), &remove).unwrap();
    state.apply(log_id(31), &remove).unwrap();
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(22))
    );
    complete(&mut state, 32, 30);
    assert_eq!(
        state.cluster_control().active_placement_log_id(),
        Some(log_id(32))
    );
}

#[test]
fn one_compact_transition_serializes_add_remove_and_reweight() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    admit(&mut state, 10, 1);
    admit(&mut state, 20, 2);

    let begin = state
        .apply(
            log_id(30),
            &Command::BeginReweightNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                storage_weight_millionths: 2_000_000,
            },
        )
        .unwrap();
    let ApplyResult::MembershipTransitionBegun(reweight) = begin else {
        panic!("REWEIGHT returned the wrong result")
    };
    assert_eq!(reweight.kind, MembershipTransitionKind::Reweight);
    assert_eq!(reweight.target_weight_millionths, Some(2_000_000));
    assert!(matches!(
        state.apply(
            log_id(31),
            &Command::BeginRemoveNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(2),
            }
        ),
        Err(ApplyError::MembershipTransitionInProgress {
            started_log_index: 30
        })
    ));
    complete(&mut state, 32, 30);
    assert_eq!(
        state.cluster_control().nodes()[&NodeId(1)].storage_weight_millionths,
        2_000_000
    );

    state
        .apply(
            log_id(40),
            &Command::BeginRemoveNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
            },
        )
        .unwrap();
    complete(&mut state, 41, 40);
    assert!(!state.cluster_control().nodes().contains_key(&NodeId(1)));
    assert!(state.cluster_control().used_node_ids().contains(NodeId(1)));
    assert_eq!(
        state.apply(
            log_id(42),
            &Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: joining(1),
            }
        ),
        Err(ApplyError::NodeIdAlreadyUsed { node_id: NodeId(1) })
    );
}

#[test]
fn peer_pin_rotation_uses_exactly_current_and_overlap_slots() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    admit(&mut state, 10, 1);
    let old = PeerSpkiSha256([1; 32]);
    let new = PeerSpkiSha256([99; 32]);

    for log_index in [4, 5] {
        state
            .apply(
                log_id(log_index),
                &Command::StagePeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id: NodeId(1),
                    expected_current: old,
                    overlap: new,
                },
            )
            .unwrap();
    }
    let descriptor = &state.cluster_control().nodes()[&NodeId(1)];
    assert_eq!(descriptor.current_peer_spki_sha256, old);
    assert_eq!(descriptor.overlap_peer_spki_sha256, Some(new));

    for log_index in [6, 7] {
        state
            .apply(
                log_id(log_index),
                &Command::PromotePeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id: NodeId(1),
                    expected_current: old,
                    expected_overlap: new,
                },
            )
            .unwrap();
    }
    let descriptor = &state.cluster_control().nodes()[&NodeId(1)];
    assert_eq!(descriptor.current_peer_spki_sha256, new);
    assert_eq!(descriptor.overlap_peer_spki_sha256, Some(old));

    for log_index in [8, 9] {
        state
            .apply(
                log_id(log_index),
                &Command::ClearPeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id: NodeId(1),
                    expected_current: new,
                    expected_overlap: old,
                },
            )
            .unwrap();
    }
    assert_eq!(
        state.cluster_control().nodes()[&NodeId(1)].overlap_peer_spki_sha256,
        None
    );
}

#[test]
fn jwt_fingerprint_binds_once_and_exact_retry_is_idempotent() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    let fingerprint = JwtSigningKeyFingerprint([8; 32]);
    let command = Command::BindJwtSigningKeyFingerprint {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        fingerprint,
    };
    for log_index in [2, 3] {
        assert_eq!(
            state.apply(log_id(log_index), &command).unwrap(),
            ApplyResult::JwtSigningKeyFingerprintBound(fingerprint)
        );
    }
    assert_eq!(
        state.cluster_control().jwt_signing_key_fingerprint(),
        Some(fingerprint)
    );
    assert!(matches!(
        state.apply(
            log_id(4),
            &Command::BindJwtSigningKeyFingerprint {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                fingerprint: JwtSigningKeyFingerprint([9; 32]),
            }
        ),
        Err(ApplyError::JwtSigningKeyFingerprintConflict { .. })
    ));
}

#[test]
fn erasure_profile_binds_once_without_a_registry() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    let profile = ErasureCodeProfile {
        data_shards: 2,
        parity_shards: 1,
        stripe_unit: 16 * 1024,
    };
    let command = Command::BindErasureCodeProfile {
        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
        profile,
    };
    for log_index in [2, 3] {
        assert_eq!(
            state.apply(log_id(log_index), &command).unwrap(),
            ApplyResult::ErasureCodeProfileBound(profile)
        );
    }
    assert_eq!(
        state.cluster_control().erasure_code_profile(),
        Some(profile)
    );
    assert_eq!(
        state.apply(
            log_id(4),
            &Command::BindErasureCodeProfile {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                profile: ErasureCodeProfile {
                    data_shards: 0,
                    ..profile
                },
            }
        ),
        Err(ApplyError::InvalidErasureCodeProfile)
    );
    assert!(matches!(
        state.apply(
            log_id(5),
            &Command::BindErasureCodeProfile {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                profile: ErasureCodeProfile {
                    data_shards: 4,
                    parity_shards: 2,
                    stripe_unit: 16 * 1024,
                },
            }
        ),
        Err(ApplyError::ErasureCodeProfileConflict { .. })
    ));
}

#[test]
fn malformed_or_unversioned_admission_fails_closed() {
    let mut state = StateMachine::new(16, 64 * 1024).unwrap();
    initialize(&mut state);
    let descriptor = joining(1);
    assert_eq!(
        state.apply(
            log_id(2),
            &Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION + 1,
                descriptor: descriptor.clone(),
            }
        ),
        Err(ApplyError::UnsupportedClusterControlVersion {
            requested: CLUSTER_CONTROL_COMMAND_VERSION + 1
        })
    );

    let mut invalid = descriptor;
    invalid.peer_address = PeerAddress("x".repeat(256));
    assert_eq!(
        state.apply(
            log_id(3),
            &Command::BeginAddNode {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                descriptor: invalid,
            }
        ),
        Err(ApplyError::InvalidPeerAddress)
    );
}
