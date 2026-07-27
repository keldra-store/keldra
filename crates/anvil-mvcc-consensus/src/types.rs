use serde::{Deserialize, Serialize};

/// Globally ordered MVCC commit coordinate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct CommitVersion(pub u64);

/// Stable identity for one transaction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub [u8; 16]);

/// Content identity of an immutable transaction bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BundleHash(pub [u8; 32]);

/// Compact identity of one logical key. The transaction bundle retains the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalKeyHash(pub [u8; 32]);

/// Compact identity of one deterministic range-stamp bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RangeConflictKey(pub [u8; 32]);

/// Stable node identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// A node identity fenced by the generation of its durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeIncarnation {
    pub node_id: NodeId,
    pub incarnation: u64,
}

/// Physical durability that must be established before certification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityLevel {
    Local,
    Quorum,
    Erasure,
}

/// Newest committed row version observed for one logical key.
///
/// A tombstone contributes its commit version even though the logical value is
/// absent. `None` means no committed value or tombstone existed at the
/// transaction snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PointObservation {
    pub key: LogicalKeyHash,
    pub observed_version: Option<CommitVersion>,
}

/// Stamp observed for a deterministic range-conflict bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RangeObservation {
    pub range: RangeConflictKey,
    pub observed_stamp: Option<CommitVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PredicateKind {
    Unique,
    Exists,
    Absent,
    ValueHash([u8; 32]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExplicitPredicate {
    pub key: LogicalKeyHash,
    pub kind: PredicateKind,
    /// Observed row version, including the commit version of a tombstone.
    pub observed_version: Option<CommitVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WrittenPoint {
    pub key: LogicalKeyHash,
    pub value_hash: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AssignmentPredicate {
    pub partition_id: u64,
    pub assignment_epoch: u64,
    pub topology_epoch: u64,
    pub owner: NodeIncarnation,
}

/// Compact application entry placed in Raft.
///
/// It intentionally contains no transaction bundle body or product row value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertifyTransaction {
    pub cluster_id_hash: [u8; 32],
    pub transaction_id: TransactionId,
    pub snapshot_version: CommitVersion,
    pub point_observations: Vec<PointObservation>,
    pub range_observations: Vec<RangeObservation>,
    #[serde(default)]
    pub predicates: Vec<ExplicitPredicate>,
    #[serde(default)]
    pub assignment_predicates: Vec<AssignmentPredicate>,
    pub written_point_keys: Vec<LogicalKeyHash>,
    #[serde(default)]
    pub written_points: Vec<WrittenPoint>,
    pub advanced_range_stamps: Vec<RangeConflictKey>,
    pub bundle_hash: BundleHash,
    pub bundle_length: u64,
    pub durability: DurabilityLevel,
    pub durable_holders: Vec<NodeIncarnation>,
}

/// Compact state-machine commands. No variant may contain product data,
/// transaction bundle bytes, mesh topology, regions, or network endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsensusCommand {
    Certify(CertifyTransaction),
    InstallNode {
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
        raft_node_id: NodeId,
        failure_domain: String,
    },
    RemoveNode {
        cluster_id_hash: [u8; 32],
        node: NodeIncarnation,
    },
    AssignPartition {
        cluster_id_hash: [u8; 32],
        partition_id: u64,
        owner: NodeIncarnation,
        epoch: u64,
    },
    SetDurabilityPolicy {
        cluster_id_hash: [u8; 32],
        generation: u64,
        bundle_quorum_holders: u16,
        tolerated_failure_domains: u16,
    },
    AdvanceGcWatermark {
        cluster_id_hash: [u8; 32],
        watermark: CommitVersion,
    },
    /// Compact post-commit durability evidence for a completed upgrade.
    ///
    /// The physical bundle/shards remain outside Raft. This updates only the
    /// retained transaction outcome after the new holders durably acknowledged
    /// the immutable bundle.
    UpgradeDurability {
        cluster_id_hash: [u8; 32],
        commit_version: CommitVersion,
        bundle_hash: BundleHash,
        durability: DurabilityLevel,
        durable_holders: Vec<NodeIncarnation>,
    },
}

impl ConsensusCommand {
    pub fn cluster_id_hash(&self) -> [u8; 32] {
        match self {
            Self::Certify(command) => command.cluster_id_hash,
            Self::InstallNode {
                cluster_id_hash, ..
            }
            | Self::RemoveNode {
                cluster_id_hash, ..
            }
            | Self::AssignPartition {
                cluster_id_hash, ..
            }
            | Self::SetDurabilityPolicy {
                cluster_id_hash, ..
            }
            | Self::AdvanceGcWatermark {
                cluster_id_hash, ..
            }
            | Self::UpgradeDurability {
                cluster_id_hash, ..
            } => *cluster_id_hash,
        }
    }

    /// Compile-time-enforced Section 9 application-entry boundary.
    ///
    /// Every collection admitted here contains only sealed, fixed-size compact
    /// evidence. The exhaustive destructuring intentionally avoids `..`: a new
    /// command variant or field cannot compile until this boundary is reviewed.
    pub(crate) fn validate_section9_boundary(&self) -> Result<(), &'static str> {
        fn compact<T: section9::CompactEvidence>(_values: &[T]) {}
        fn compact_one<T: section9::CompactEvidence>(_value: &T) {}

        match self {
            Self::Certify(CertifyTransaction {
                cluster_id_hash: _,
                transaction_id: _,
                snapshot_version: _,
                point_observations,
                range_observations,
                predicates,
                assignment_predicates,
                written_point_keys,
                written_points,
                advanced_range_stamps,
                bundle_hash: _,
                bundle_length: _,
                durability: _,
                durable_holders,
            }) => {
                compact(point_observations);
                compact(range_observations);
                compact(predicates);
                compact(assignment_predicates);
                compact(written_point_keys);
                compact(written_points);
                compact(advanced_range_stamps);
                compact(durable_holders);
            }
            Self::InstallNode {
                cluster_id_hash: _,
                node: _,
                raft_node_id: _,
                failure_domain,
            } => {
                if failure_domain.len() > 255
                    || failure_domain.contains("://")
                    || failure_domain.bytes().any(|byte| byte.is_ascii_control())
                {
                    return Err(
                        "failure domain must be a compact label, not an endpoint or payload",
                    );
                }
            }
            Self::RemoveNode {
                cluster_id_hash: _,
                node,
            } => compact_one(node),
            Self::AssignPartition {
                cluster_id_hash: _,
                partition_id: _,
                owner: node,
                epoch: _,
            } => compact_one(node),
            Self::SetDurabilityPolicy {
                cluster_id_hash: _,
                generation: _,
                bundle_quorum_holders: _,
                tolerated_failure_domains: _,
            }
            | Self::AdvanceGcWatermark {
                cluster_id_hash: _,
                watermark: _,
            } => {}
            Self::UpgradeDurability {
                cluster_id_hash: _,
                commit_version: _,
                bundle_hash: _,
                durability: _,
                durable_holders,
            } => compact(durable_holders),
        }
        Ok(())
    }
}

mod section9 {
    pub trait CompactEvidence: Copy {}

    impl CompactEvidence for super::AssignmentPredicate {}
    impl CompactEvidence for super::ExplicitPredicate {}
    impl CompactEvidence for super::LogicalKeyHash {}
    impl CompactEvidence for super::NodeIncarnation {}
    impl CompactEvidence for super::PointObservation {}
    impl CompactEvidence for super::RangeConflictKey {}
    impl CompactEvidence for super::RangeObservation {}
    impl CompactEvidence for super::WrittenPoint {}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub owner: NodeIncarnation,
    pub epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConsensusDurabilityPolicy {
    pub generation: u64,
    pub bundle_quorum_holders: u16,
    pub tolerated_failure_domains: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlApplyResult {
    NodeInstalled(NodeIncarnation),
    NodeRemoved(NodeIncarnation),
    PartitionAssigned {
        partition_id: u64,
        assignment: PartitionAssignment,
    },
    DurabilityPolicySet(ConsensusDurabilityPolicy),
    GcWatermarkAdvanced(CommitVersion),
    DurabilityUpgraded {
        commit_version: CommitVersion,
        durability: DurabilityLevel,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedBundleDecision {
    pub cluster_id_hash: [u8; 32],
    pub bundle_hash: BundleHash,
    pub bundle_length: u64,
    pub durability: DurabilityLevel,
    pub durable_holders: Vec<NodeIncarnation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalDurabilityViolation {
    pub commit_version: CommitVersion,
    pub bundle_hash: BundleHash,
    pub lost_holder: NodeIncarnation,
    pub detected_at_log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedDecision {
    pub position: CommitVersion,
    pub committed_bundle: Option<CommittedBundleDecision>,
}

/// Stable reason for a transaction certification abort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationAbort {
    InvalidCommand(String),
    PointConflict {
        key: LogicalKeyHash,
        expected: Option<CommitVersion>,
        actual: Option<CommitVersion>,
    },
    PredicateConflict {
        key: LogicalKeyHash,
        predicate: PredicateKind,
        expected_version: Option<CommitVersion>,
        actual_version: Option<CommitVersion>,
    },
    RangeConflict {
        range: RangeConflictKey,
        expected: Option<CommitVersion>,
        actual: Option<CommitVersion>,
    },
    AssignmentConflict {
        partition_id: u64,
        expected_epoch: CommitVersion,
        actual_epoch: Option<CommitVersion>,
        expected_topology_epoch: CommitVersion,
        actual_topology_epoch: CommitVersion,
    },
}

/// Deterministic result stored for transaction retry deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationResult {
    Committed {
        commit_version: CommitVersion,
        bundle_hash: BundleHash,
    },
    Aborted {
        at_version: CommitVersion,
        bundle_hash: BundleHash,
        reason: CertificationAbort,
    },
}

impl CertificationResult {
    pub fn bundle_hash(&self) -> BundleHash {
        match self {
            Self::Committed { bundle_hash, .. } | Self::Aborted { bundle_hash, .. } => *bundle_hash,
        }
    }
}

#[cfg(test)]
mod section9_boundary_tests {
    use super::*;

    fn certification() -> CertifyTransaction {
        CertifyTransaction {
            cluster_id_hash: [1; 32],
            transaction_id: TransactionId([2; 16]),
            snapshot_version: CommitVersion(3),
            point_observations: vec![PointObservation {
                key: LogicalKeyHash([4; 32]),
                observed_version: Some(CommitVersion(2)),
            }],
            range_observations: vec![RangeObservation {
                range: RangeConflictKey([5; 32]),
                observed_stamp: None,
            }],
            predicates: vec![ExplicitPredicate {
                key: LogicalKeyHash([6; 32]),
                kind: PredicateKind::ValueHash([7; 32]),
                observed_version: Some(CommitVersion(2)),
            }],
            assignment_predicates: vec![AssignmentPredicate {
                partition_id: 8,
                assignment_epoch: 9,
                topology_epoch: 10,
                owner: NodeIncarnation {
                    node_id: NodeId(11),
                    incarnation: 2,
                },
            }],
            written_point_keys: vec![LogicalKeyHash([12; 32])],
            written_points: vec![WrittenPoint {
                key: LogicalKeyHash([12; 32]),
                value_hash: Some([13; 32]),
            }],
            advanced_range_stamps: vec![RangeConflictKey([14; 32])],
            bundle_hash: BundleHash([15; 32]),
            bundle_length: 4096,
            durability: DurabilityLevel::Quorum,
            durable_holders: vec![NodeIncarnation {
                node_id: NodeId(16),
                incarnation: 3,
            }],
        }
    }

    #[test]
    fn every_serializable_application_variant_passes_the_closed_boundary() {
        let node = NodeIncarnation {
            node_id: NodeId(1),
            incarnation: 2,
        };
        let commands = [
            ConsensusCommand::Certify(certification()),
            ConsensusCommand::InstallNode {
                cluster_id_hash: [1; 32],
                node,
                raft_node_id: NodeId(1),
                failure_domain: "zone-a".into(),
            },
            ConsensusCommand::RemoveNode {
                cluster_id_hash: [1; 32],
                node,
            },
            ConsensusCommand::AssignPartition {
                cluster_id_hash: [1; 32],
                partition_id: 2,
                owner: node,
                epoch: 3,
            },
            ConsensusCommand::SetDurabilityPolicy {
                cluster_id_hash: [1; 32],
                generation: 2,
                bundle_quorum_holders: 2,
                tolerated_failure_domains: 1,
            },
            ConsensusCommand::AdvanceGcWatermark {
                cluster_id_hash: [1; 32],
                watermark: CommitVersion(4),
            },
            ConsensusCommand::UpgradeDurability {
                cluster_id_hash: [1; 32],
                commit_version: CommitVersion(4),
                bundle_hash: BundleHash([5; 32]),
                durability: DurabilityLevel::Erasure,
                durable_holders: vec![node],
            },
        ];
        let forbidden = [
            b"object-payload-body".as_slice(),
            b"transaction-bundle-body".as_slice(),
            b"authorization-bearer-token".as_slice(),
            b"cryptographic-receipt-signature".as_slice(),
            b"background-job-payload".as_slice(),
            b"per-frame-ack-state".as_slice(),
        ];
        for command in commands {
            command.validate_section9_boundary().unwrap();
            let encoded =
                bincode::serde::encode_to_vec(&command, bincode::config::standard()).unwrap();
            for marker in forbidden {
                assert!(
                    !encoded.windows(marker.len()).any(|window| window == marker),
                    "forbidden Section 9 body reached serialized Raft command"
                );
            }
        }
    }

    #[test]
    fn install_node_rejects_an_endpoint_disguised_as_failure_domain() {
        let command = ConsensusCommand::InstallNode {
            cluster_id_hash: [1; 32],
            node: NodeIncarnation {
                node_id: NodeId(1),
                incarnation: 1,
            },
            raft_node_id: NodeId(1),
            failure_domain: "https://node.internal".into(),
        };
        assert!(command.validate_section9_boundary().is_err());
    }
}
