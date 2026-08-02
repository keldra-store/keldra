//! Crash-safe online rotation of one node's self-signed peer certificate.
//!
//! The local identity's presented/overlap pair and the committed descriptor's
//! current/overlap pair are the entire recovery record. Every call performs at
//! most one durable step, so a caller may retry after any unknown outcome.

use std::path::Path;

use anvil_consensus::{
    CLUSTER_CONTROL_COMMAND_VERSION, Command, DecisionRaft, DecisionRaftError, NodeId, NodeState,
    PeerSpkiSha256,
};
use thiserror::Error;

use super::{
    LocalNodeIdentity, NodeIdentityError, generate, load, replace, validate_peer_identity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotationAdvance {
    Prepared {
        replacement: PeerSpkiSha256,
    },
    OverlapCommitted {
        replacement: PeerSpkiSha256,
    },
    PresentingReplacement {
        replacement: PeerSpkiSha256,
    },
    Promoted {
        replacement: PeerSpkiSha256,
    },
    ReadyToFinalize {
        current: PeerSpkiSha256,
        retiring: PeerSpkiSha256,
    },
    Complete {
        current: PeerSpkiSha256,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotationFinalization {
    Blocked(RotationBlock),
    CommittedOldPinCleared { current: PeerSpkiSha256 },
    Complete { current: PeerSpkiSha256 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RotationBlock {
    RequiredActivePeersNotApplied,
    StaleJoiningPreparationMaterialNotReplaced,
}

#[derive(Debug, Error)]
pub(crate) enum RotationError {
    #[error(transparent)]
    Identity(#[from] NodeIdentityError),
    #[error(transparent)]
    Consensus(#[from] DecisionRaftError),
    #[error("committed cluster identity is unavailable")]
    ClusterIdentityUnavailable,
    #[error("node {0:?} has no committed descriptor")]
    NodeNotAdmitted(NodeId),
    #[error("node {0:?} must be ACTIVE before rotating its peer certificate")]
    NodeNotActive(NodeId),
    #[error("peer-certificate rotation has not reached promoted state")]
    NotReadyToFinalize,
    #[error(
        "local and committed peer-certificate pin pairs do not describe a valid rotation phase"
    )]
    ImpossiblePinPairing,
}

#[derive(Clone)]
struct RotationState {
    local: LocalNodeIdentity,
    local_presented: PeerSpkiSha256,
    local_overlap: Option<PeerSpkiSha256>,
    committed_current: PeerSpkiSha256,
    committed_overlap: Option<PeerSpkiSha256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RotationPhase {
    Clean,
    Prepared {
        replacement: PeerSpkiSha256,
    },
    OverlapCommitted {
        replacement: PeerSpkiSha256,
    },
    PresentingReplacement {
        replacement: PeerSpkiSha256,
    },
    Promoted {
        current: PeerSpkiSha256,
        retiring: PeerSpkiSha256,
    },
    CommittedOldPinCleared {
        current: PeerSpkiSha256,
    },
    Complete {
        current: PeerSpkiSha256,
    },
}

/// Advance one pre-finalization rotation step.
///
/// `expected_current` is the request fence. A new request supplies the pin it
/// observed before rotation; retries continue that rotation after restart. If
/// the pair is already clean at a different pin, the earlier request is
/// complete and cannot accidentally begin another rotation.
pub(crate) async fn advance(
    data_dir: &Path,
    decisions: &DecisionRaft,
    node_id: NodeId,
    expected_current: PeerSpkiSha256,
) -> Result<RotationAdvance, RotationError> {
    let state = load_state(data_dir, decisions, node_id)?;
    match classify(&state, expected_current)? {
        RotationPhase::Clean => {
            let generated = generate(state.local.cluster_id(), node_id)?;
            let replacement = generated.presented_peer_identity().clone();
            let replacement_pin = validate_peer_identity(&replacement)?;
            let prepared = LocalNodeIdentity::new(
                state.local.cluster_id(),
                node_id,
                state.local.presented_peer_identity().clone(),
                Some(replacement),
            )?;
            persist(data_dir, &state.local, &prepared)?;
            Ok(RotationAdvance::Prepared {
                replacement: replacement_pin,
            })
        }
        RotationPhase::Prepared { replacement } => {
            decisions
                .submit(Command::StagePeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id,
                    expected_current,
                    overlap: replacement,
                })
                .await?;
            require_phase(
                data_dir,
                decisions,
                node_id,
                expected_current,
                RotationPhase::OverlapCommitted { replacement },
            )?;
            Ok(RotationAdvance::OverlapCommitted { replacement })
        }
        RotationPhase::OverlapCommitted { replacement } => {
            let replacement_identity = state
                .local
                .overlap_peer_identity()
                .cloned()
                .ok_or(RotationError::ImpossiblePinPairing)?;
            let switched = LocalNodeIdentity::new(
                state.local.cluster_id(),
                node_id,
                replacement_identity,
                Some(state.local.presented_peer_identity().clone()),
            )?;
            persist(data_dir, &state.local, &switched)?;
            Ok(RotationAdvance::PresentingReplacement { replacement })
        }
        RotationPhase::PresentingReplacement { replacement } => {
            decisions
                .submit(Command::PromotePeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id,
                    expected_current,
                    expected_overlap: replacement,
                })
                .await?;
            require_phase(
                data_dir,
                decisions,
                node_id,
                expected_current,
                RotationPhase::Promoted {
                    current: replacement,
                    retiring: expected_current,
                },
            )?;
            Ok(RotationAdvance::Promoted { replacement })
        }
        RotationPhase::Promoted { current, retiring } => {
            Ok(RotationAdvance::ReadyToFinalize { current, retiring })
        }
        RotationPhase::CommittedOldPinCleared { .. } => Err(RotationError::NotReadyToFinalize),
        RotationPhase::Complete { current } => Ok(RotationAdvance::Complete { current }),
    }
}

/// Retire the old pin and key after external safety checks.
///
/// The caller may replace unused preparation material only while its target is
/// still JOINING. It must not mutate identity fields for an ACTIVE target. The
/// core takes the resulting bounded precondition as a boolean rather than
/// introducing a bundle registry. A committed clear proves both checks already
/// ran, so restart cleanup removes the local old key without asking transient
/// predicates again.
pub(crate) async fn finalize(
    data_dir: &Path,
    decisions: &DecisionRaft,
    node_id: NodeId,
    expected_previous: PeerSpkiSha256,
    all_required_active_peers_applied: bool,
    stale_joining_material_replaced: bool,
) -> Result<RotationFinalization, RotationError> {
    let state = load_state(data_dir, decisions, node_id)?;
    match classify(&state, expected_previous)? {
        RotationPhase::Promoted { current, retiring } => {
            if !all_required_active_peers_applied {
                return Ok(RotationFinalization::Blocked(
                    RotationBlock::RequiredActivePeersNotApplied,
                ));
            }
            if !stale_joining_material_replaced {
                return Ok(RotationFinalization::Blocked(
                    RotationBlock::StaleJoiningPreparationMaterialNotReplaced,
                ));
            }
            decisions
                .submit(Command::ClearPeerSpkiOverlap {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    node_id,
                    expected_current: current,
                    expected_overlap: retiring,
                })
                .await?;
            require_phase(
                data_dir,
                decisions,
                node_id,
                expected_previous,
                RotationPhase::CommittedOldPinCleared { current },
            )?;
            Ok(RotationFinalization::CommittedOldPinCleared { current })
        }
        RotationPhase::CommittedOldPinCleared { current } => {
            let cleaned = LocalNodeIdentity::new(
                state.local.cluster_id(),
                node_id,
                state.local.presented_peer_identity().clone(),
                None,
            )?;
            persist(data_dir, &state.local, &cleaned)?;
            Ok(RotationFinalization::Complete { current })
        }
        RotationPhase::Complete { current } => Ok(RotationFinalization::Complete { current }),
        RotationPhase::Clean
        | RotationPhase::Prepared { .. }
        | RotationPhase::OverlapCommitted { .. }
        | RotationPhase::PresentingReplacement { .. } => Err(RotationError::NotReadyToFinalize),
    }
}

fn load_state(
    data_dir: &Path,
    decisions: &DecisionRaft,
    node_id: NodeId,
) -> Result<RotationState, RotationError> {
    let consensus = decisions.state()?;
    let cluster_id = consensus
        .cluster_id()
        .ok_or(RotationError::ClusterIdentityUnavailable)?;
    let local = load(data_dir, cluster_id, node_id)?;
    let descriptor = consensus
        .cluster_control()
        .nodes()
        .get(&node_id)
        .ok_or(RotationError::NodeNotAdmitted(node_id))?;
    if descriptor.state != NodeState::Active {
        return Err(RotationError::NodeNotActive(node_id));
    }
    let local_presented = validate_peer_identity(local.presented_peer_identity())?;
    let local_overlap = local
        .overlap_peer_identity()
        .map(validate_peer_identity)
        .transpose()?;
    Ok(RotationState {
        local,
        local_presented,
        local_overlap,
        committed_current: descriptor.current_peer_spki_sha256,
        committed_overlap: descriptor.overlap_peer_spki_sha256,
    })
}

fn classify(
    state: &RotationState,
    expected: PeerSpkiSha256,
) -> Result<RotationPhase, RotationError> {
    let local = (state.local_presented, state.local_overlap);
    let committed = (state.committed_current, state.committed_overlap);
    if local == (expected, None) && committed == (expected, None) {
        return Ok(RotationPhase::Clean);
    }
    if let Some(replacement) = state.local_overlap
        && state.local_presented == expected
        && replacement != expected
    {
        return match committed {
            (current, None) if current == expected => Ok(RotationPhase::Prepared { replacement }),
            (current, Some(overlap)) if current == expected && overlap == replacement => {
                Ok(RotationPhase::OverlapCommitted { replacement })
            }
            _ => Err(RotationError::ImpossiblePinPairing),
        };
    }
    if state.local_overlap == Some(expected) && state.local_presented != expected {
        let replacement = state.local_presented;
        return match committed {
            (current, Some(overlap)) if current == expected && overlap == replacement => {
                Ok(RotationPhase::PresentingReplacement { replacement })
            }
            (current, Some(overlap)) if current == replacement && overlap == expected => {
                Ok(RotationPhase::Promoted {
                    current: replacement,
                    retiring: expected,
                })
            }
            (current, None) if current == replacement => {
                Ok(RotationPhase::CommittedOldPinCleared {
                    current: replacement,
                })
            }
            _ => Err(RotationError::ImpossiblePinPairing),
        };
    }
    if state.local_overlap.is_none()
        && state.committed_overlap.is_none()
        && state.local_presented == state.committed_current
        && state.local_presented != expected
    {
        return Ok(RotationPhase::Complete {
            current: state.local_presented,
        });
    }
    Err(RotationError::ImpossiblePinPairing)
}

fn persist(
    data_dir: &Path,
    previous: &LocalNodeIdentity,
    replacement: &LocalNodeIdentity,
) -> Result<(), RotationError> {
    replace(
        data_dir,
        previous.cluster_id(),
        previous.node_id(),
        replacement,
    )?;
    Ok(())
}

fn require_phase(
    data_dir: &Path,
    decisions: &DecisionRaft,
    node_id: NodeId,
    expected: PeerSpkiSha256,
    required: RotationPhase,
) -> Result<(), RotationError> {
    let state = load_state(data_dir, decisions, node_id)?;
    if classify(&state, expected)? != required {
        return Err(RotationError::ImpossiblePinPairing);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    use anvil_consensus::{
        CapabilityRange, ClusterId, JoinCapabilityHash, NodeDescriptor, PeerAddress, PeerNode,
        PeerSpkiSha256,
    };

    use super::*;
    use crate::node_identity::{create, identity_path};

    struct Fixture {
        directory: tempfile::TempDir,
        decisions: DecisionRaft,
        original: PeerSpkiSha256,
    }

    impl Fixture {
        async fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let identity = generate(ClusterId([71; 16]), NodeId(1)).unwrap();
            let original = validate_peer_identity(identity.presented_peer_identity()).unwrap();
            create(directory.path(), &identity).unwrap();
            let decisions = open_decisions(directory.path()).await;
            decisions
                .initialize_genesis(BTreeMap::from([(1, PeerNode::new("anvil-local://1"))]))
                .await
                .unwrap();
            decisions
                .wait_for_leader(Duration::from_secs(5))
                .await
                .unwrap();
            decisions
                .submit(Command::InitializeCluster {
                    cluster_id: ClusterId([71; 16]),
                })
                .await
                .unwrap();
            let admitted = decisions
                .submit(Command::BeginAddNode {
                    format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                    descriptor: joining_descriptor(original),
                })
                .await
                .unwrap();
            for _ in 0..2 {
                decisions
                    .submit(Command::CompleteMembershipTransition {
                        format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                        started_log_index: admitted.log_index,
                    })
                    .await
                    .unwrap();
            }
            Self {
                directory,
                decisions,
                original,
            }
        }

        async fn restart(self) -> Self {
            self.decisions.shutdown().await.unwrap();
            let Self {
                directory,
                decisions,
                original,
            } = self;
            drop(decisions);
            let decisions = open_decisions(directory.path()).await;
            decisions
                .wait_for_leader(Duration::from_secs(5))
                .await
                .unwrap();
            Self {
                directory,
                decisions,
                original,
            }
        }
    }

    async fn open_decisions(data_dir: &Path) -> DecisionRaft {
        DecisionRaft::open(data_dir.join("decisions"), 1, 16, 256 * 1024)
            .await
            .unwrap()
    }

    fn joining_descriptor(pin: PeerSpkiSha256) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(1),
            peer_address: PeerAddress("anvil-local://1".into()),
            storage_weight_millionths: 1_000_000,
            state: NodeState::Joining,
            current_peer_spki_sha256: pin,
            overlap_peer_spki_sha256: None,
            join_capability_hash: Some(JoinCapabilityHash([72; 32])),
            supported_protocol: CapabilityRange { min: 1, max: 1 },
            supported_storage_format: CapabilityRange { min: 1, max: 1 },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_rotation_phase_resumes_after_restart() {
        let mut fixture = Fixture::new().await;
        let prepared = advance(
            fixture.directory.path(),
            &fixture.decisions,
            NodeId(1),
            fixture.original,
        )
        .await
        .unwrap();
        let RotationAdvance::Prepared { replacement } = prepared else {
            panic!("first step did not prepare a replacement")
        };
        assert_ne!(replacement, fixture.original);
        fixture = fixture.restart().await;

        assert_eq!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await
            .unwrap(),
            RotationAdvance::OverlapCommitted { replacement }
        );
        fixture = fixture.restart().await;

        assert_eq!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await
            .unwrap(),
            RotationAdvance::PresentingReplacement { replacement }
        );
        fixture = fixture.restart().await;

        assert_eq!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await
            .unwrap(),
            RotationAdvance::Promoted { replacement }
        );
        fixture = fixture.restart().await;
        assert_eq!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await
            .unwrap(),
            RotationAdvance::ReadyToFinalize {
                current: replacement,
                retiring: fixture.original,
            }
        );

        assert_eq!(
            finalize(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
                false,
                false,
            )
            .await
            .unwrap(),
            RotationFinalization::Blocked(RotationBlock::RequiredActivePeersNotApplied)
        );
        assert_eq!(
            finalize(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
                true,
                false,
            )
            .await
            .unwrap(),
            RotationFinalization::Blocked(
                RotationBlock::StaleJoiningPreparationMaterialNotReplaced
            )
        );
        assert_eq!(
            finalize(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
                true,
                true,
            )
            .await
            .unwrap(),
            RotationFinalization::CommittedOldPinCleared {
                current: replacement
            }
        );
        fixture = fixture.restart().await;
        assert_eq!(
            finalize(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
                false,
                false,
            )
            .await
            .unwrap(),
            RotationFinalization::Complete {
                current: replacement
            }
        );
        fixture = fixture.restart().await;
        assert_eq!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await
            .unwrap(),
            RotationAdvance::Complete {
                current: replacement
            }
        );
        fixture.decisions.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_and_tampered_pin_pairs_fail_without_advancing_raft() {
        let fixture = Fixture::new().await;
        let unrelated = generate(ClusterId([71; 16]), NodeId(1)).unwrap();
        replace(
            fixture.directory.path(),
            ClusterId([71; 16]),
            NodeId(1),
            &unrelated,
        )
        .unwrap();
        assert!(matches!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await,
            Err(RotationError::ImpossiblePinPairing)
        ));
        let consensus = fixture.decisions.state().unwrap();
        let descriptor = &consensus.cluster_control().nodes()[&NodeId(1)];
        assert_eq!(descriptor.current_peer_spki_sha256, fixture.original);
        assert_eq!(descriptor.overlap_peer_spki_sha256, None);

        let mut encoded = std::fs::read(identity_path(fixture.directory.path())).unwrap();
        let key_marker = b"PRIVATE KEY";
        let marker = encoded
            .windows(key_marker.len())
            .position(|window| window == key_marker)
            .unwrap();
        encoded[marker] = b'X';
        std::fs::write(identity_path(fixture.directory.path()), encoded).unwrap();
        assert!(matches!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await,
            Err(RotationError::Identity(_))
        ));
        fixture.decisions.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_wrong_overlap_cannot_replace_the_prepared_target() {
        let fixture = Fixture::new().await;
        let RotationAdvance::Prepared { replacement } = advance(
            fixture.directory.path(),
            &fixture.decisions,
            NodeId(1),
            fixture.original,
        )
        .await
        .unwrap() else {
            panic!("rotation was not prepared")
        };
        let wrong = generate(ClusterId([71; 16]), NodeId(1)).unwrap();
        let wrong_pin = validate_peer_identity(wrong.presented_peer_identity()).unwrap();
        assert_ne!(wrong_pin, replacement);
        fixture
            .decisions
            .submit(Command::StagePeerSpkiOverlap {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                node_id: NodeId(1),
                expected_current: fixture.original,
                overlap: wrong_pin,
            })
            .await
            .unwrap();
        assert!(matches!(
            advance(
                fixture.directory.path(),
                &fixture.decisions,
                NodeId(1),
                fixture.original,
            )
            .await,
            Err(RotationError::ImpossiblePinPairing)
        ));
        fixture.decisions.shutdown().await.unwrap();
    }

    #[test]
    fn classifier_rejects_switching_before_overlap_commit() {
        let old = PeerSpkiSha256([1; 32]);
        let new = PeerSpkiSha256([2; 32]);
        let local = generate(ClusterId([71; 16]), NodeId(1)).unwrap();
        let state = RotationState {
            local,
            local_presented: new,
            local_overlap: Some(old),
            committed_current: old,
            committed_overlap: None,
        };
        assert!(matches!(
            classify(&state, old),
            Err(RotationError::ImpossiblePinPairing)
        ));
    }
}
