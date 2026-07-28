//! Conversion boundary between product transaction types and compact consensus
//! commands. Full durability evidence is validated by the coordinator and only
//! its canonically ordered holder incarnations cross the Raft boundary.

use std::collections::BTreeSet;

use anvil_mvcc_consensus as consensus;
use anyhow::{Context, Result};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::mvcc_transaction as product;

pub struct ConsensusTransactionCertifier<C> {
    consensus: C,
}

impl<C> ConsensusTransactionCertifier<C> {
    pub fn new(consensus: C) -> Self {
        Self { consensus }
    }
}

#[async_trait]
impl<C> product::TransactionCertifier for ConsensusTransactionCertifier<C>
where
    C: consensus::Consensus,
{
    fn durability_policy(&self) -> Option<product::DurabilityPolicy> {
        let policy = self.consensus.durability_policy()?;
        (policy.generation > 0).then_some(product::DurabilityPolicy {
            bundle_quorum_holders: usize::from(policy.bundle_quorum_holders),
            tolerated_failure_domains: usize::from(policy.tolerated_failure_domains),
        })
    }

    async fn observed_commit_version(
        &self,
        consistency: product::ReadConsistency,
    ) -> Result<product::CommitVersion> {
        let version = match consistency {
            product::ReadConsistency::LocalSnapshot => self.consensus.observed_commit_version(),
            product::ReadConsistency::Linearized => self
                .consensus
                .linearized_read_barrier()
                .await
                .context("consensus linearized read barrier failed")?,
        };
        Ok(version.0)
    }

    async fn certify(
        &self,
        request: product::CertificationRequest,
    ) -> Result<product::CertificationResult> {
        let started_at = std::time::Instant::now();
        tracing::debug!(
            operation = "consensus.certify",
            transaction_id = %request.transaction_id,
            "proposing compact transaction certification command"
        );
        let result = self
            .consensus
            .certify(to_consensus_command(&request)?)
            .await
            .context("consensus certification failed");
        let elapsed = started_at.elapsed();
        match result {
            Ok(result) => {
                crate::perf::record_consensus_phase("proposal", "ok", elapsed);
                crate::perf::record_consensus_phase("apply", "ok", elapsed);
                let result = from_consensus_result(result);
                let commit_index = match &result {
                    product::CertificationResult::Committed { commit_version } => *commit_version,
                    product::CertificationResult::Aborted { .. } => {
                        self.consensus.observed_commit_version().0
                    }
                };
                crate::perf::record_consensus_commit(commit_index);
                Ok(result)
            }
            Err(error) => {
                crate::perf::record_consensus_phase("proposal", "error", elapsed);
                if error.to_string().contains("leader") {
                    crate::perf::record_consensus_leader_change("proposal_redirect");
                }
                Err(error)
            }
        }
    }
}

pub(crate) fn to_consensus_command(
    request: &product::CertificationRequest,
) -> Result<consensus::CertifyTransaction> {
    let mut point_observations = request
        .point_observations
        .iter()
        .map(|observation| consensus::PointObservation {
            key: logical_key_hash(&request.cluster_id, &observation.key),
            observed_version: observation.observed_version.map(consensus::CommitVersion),
        })
        .collect::<Vec<_>>();
    point_observations.sort();
    point_observations.dedup();

    let mut range_observations = request
        .range_observations
        .iter()
        .map(|observation| consensus::RangeObservation {
            range: range_conflict_hash(&request.cluster_id, &observation.conflict_key),
            observed_stamp: observation
                .observed_range_stamp
                .map(consensus::CommitVersion),
        })
        .collect::<Vec<_>>();
    range_observations.sort();
    range_observations.dedup();

    let mut predicates = request
        .predicates
        .iter()
        .map(|predicate| consensus::ExplicitPredicate {
            key: logical_key_hash(&request.cluster_id, &predicate.key),
            kind: match predicate.kind {
                product::PredicateKind::Unique => consensus::PredicateKind::Unique,
                product::PredicateKind::Exists => consensus::PredicateKind::Exists,
                product::PredicateKind::Absent => consensus::PredicateKind::Absent,
                product::PredicateKind::ValueHash(hash) => {
                    consensus::PredicateKind::ValueHash(hash)
                }
            },
            observed_version: predicate.observed_version.map(consensus::CommitVersion),
        })
        .collect::<Vec<_>>();
    predicates.sort();
    predicates.dedup();

    let mut written_point_keys = request
        .written_keys
        .iter()
        .map(|key| logical_key_hash(&request.cluster_id, key))
        .collect::<Vec<_>>();
    written_point_keys.sort();
    written_point_keys.dedup();
    let mut written_points = request
        .written_points
        .iter()
        .map(|(key, value_hash)| consensus::WrittenPoint {
            key: logical_key_hash(&request.cluster_id, key),
            value_hash: *value_hash,
        })
        .collect::<Vec<_>>();
    written_points.sort();
    written_points.dedup();

    let mut advanced_range_stamps = request
        .advanced_range_stamps
        .iter()
        .map(|key| range_conflict_hash(&request.cluster_id, key))
        .collect::<Vec<_>>();
    advanced_range_stamps.sort();
    advanced_range_stamps.dedup();

    let mut assignment_predicates = request
        .assignment_predicates
        .iter()
        .map(|predicate| consensus::AssignmentPredicate {
            partition_id: predicate.partition_id,
            assignment_epoch: predicate.assignment_epoch,
            topology_epoch: predicate.topology_epoch,
            owner: node_incarnation(&predicate.owner),
        })
        .collect::<Vec<_>>();
    assignment_predicates.sort_by_key(|predicate| predicate.partition_id);
    if assignment_predicates
        .windows(2)
        .any(|pair| pair[0].partition_id == pair[1].partition_id)
    {
        anyhow::bail!("assignment predicates must be unique by partition");
    }
    if assignment_predicates.iter().any(|predicate| {
        predicate.partition_id == 0
            || predicate.assignment_epoch == 0
            || predicate.topology_epoch == 0
            || predicate.owner.node_id.0 == 0
            || predicate.owner.incarnation == 0
    }) {
        anyhow::bail!("assignment predicates require non-zero exact authority");
    }

    let command = consensus::CertifyTransaction {
        cluster_id_hash: cluster_id_hash(&request.cluster_id),
        transaction_id: consensus_transaction_id(&request.cluster_id, &request.transaction_id),
        principal_hash: consensus_principal_hash(&request.cluster_id, &request.principal),
        snapshot_version: consensus::CommitVersion(request.snapshot_version),
        point_observations,
        range_observations,
        predicates,
        assignment_predicates,
        written_point_keys,
        written_points,
        advanced_range_stamps,
        bundle_hash: parse_bundle_hash(&request.bundle.hash)?,
        bundle_length: request.bundle.length,
        durability: match request.durability {
            product::DurabilityLevel::Local => consensus::DurabilityLevel::Local,
            product::DurabilityLevel::Quorum => consensus::DurabilityLevel::Quorum,
            product::DurabilityLevel::Erasure => consensus::DurabilityLevel::Erasure,
        },
        durable_holders: valid_bundle_holders(&request.cluster_id, &request.bundle_holders),
    };
    if serde_json::to_vec(&command)?.len() > request.max_command_bytes {
        anyhow::bail!("transaction exceeds certification command byte limit");
    }
    Ok(command)
}

fn valid_bundle_holders(
    cluster_id: &str,
    evidence: &[product::BundleDurabilityEvidence],
) -> Vec<consensus::NodeIncarnation> {
    let mut holders = BTreeSet::new();
    for holder in evidence {
        if holder.cluster_id == cluster_id
            && holder.complete
            && holder.hash_verified
            && holder.fsynced
        {
            holders.insert(node_incarnation(&holder.node));
        }
    }
    holders.into_iter().collect()
}

pub(crate) fn from_consensus_result(
    result: consensus::CertificationResult,
) -> product::CertificationResult {
    match result {
        consensus::CertificationResult::Committed { commit_version, .. } => {
            product::CertificationResult::Committed {
                commit_version: commit_version.0,
            }
        }
        consensus::CertificationResult::Aborted { reason, .. } => {
            let reason = match reason {
                consensus::CertificationAbort::InvalidCommand(message) => {
                    product::CertificationAbort::InvalidCommand(message)
                }
                consensus::CertificationAbort::PointConflict { key, .. } => {
                    product::CertificationAbort::PointConflict { key_hash: key.0 }
                }
                consensus::CertificationAbort::RangeConflict { range, .. } => {
                    product::CertificationAbort::RangeConflict {
                        range_hash: range.0,
                    }
                }
                consensus::CertificationAbort::PredicateConflict { key, .. } => {
                    product::CertificationAbort::PredicateConflict { key_hash: key.0 }
                }
                consensus::CertificationAbort::AssignmentConflict { partition_id, .. } => {
                    product::CertificationAbort::AssignmentConflict { partition_id }
                }
            };
            product::CertificationResult::Aborted { reason }
        }
    }
}

fn cluster_id_hash(value: &str) -> [u8; 32] {
    domain_hash(b"anvil.mvcc.cluster-id.v1", &[value.as_bytes()])
}

pub(crate) fn consensus_transaction_id(cluster_id: &str, value: &str) -> consensus::TransactionId {
    let digest = domain_hash(
        b"anvil.mvcc.transaction-id.v1",
        &[cluster_id.as_bytes(), value.as_bytes()],
    );
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    consensus::TransactionId(id)
}

pub(crate) fn consensus_principal_hash(cluster_id: &str, principal: &str) -> [u8; 32] {
    domain_hash(
        b"anvil.mvcc.transaction-principal.v1",
        &[cluster_id.as_bytes(), principal.as_bytes()],
    )
}

pub(crate) fn from_consensus_durability(
    durability: consensus::DurabilityLevel,
) -> product::DurabilityLevel {
    match durability {
        consensus::DurabilityLevel::Local => product::DurabilityLevel::Local,
        consensus::DurabilityLevel::Quorum => product::DurabilityLevel::Quorum,
        consensus::DurabilityLevel::Erasure => product::DurabilityLevel::Erasure,
    }
}

fn logical_key_hash(cluster_id: &str, key: &product::LogicalKey) -> consensus::LogicalKeyHash {
    consensus::LogicalKeyHash(domain_hash(
        b"anvil.mvcc.logical-key.v1",
        &[
            cluster_id.as_bytes(),
            &key.table_id.to_be_bytes(),
            &key.application_key,
        ],
    ))
}

fn range_conflict_hash(
    cluster_id: &str,
    key: &product::RangeStampKey,
) -> consensus::RangeConflictKey {
    consensus::RangeConflictKey(domain_hash(
        b"anvil.mvcc.range-conflict.v1",
        &[
            cluster_id.as_bytes(),
            &key.scheme_version.to_be_bytes(),
            &key.table_id.to_be_bytes(),
            &key.key_prefix,
        ],
    ))
}

fn node_incarnation(node: &product::NodeIncarnation) -> consensus::NodeIncarnation {
    let digest = domain_hash(b"anvil.node-id.v1", &[node.node_id.as_bytes()]);
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    consensus::NodeIncarnation {
        node_id: consensus::NodeId(u64::from_be_bytes(bytes)),
        incarnation: node.incarnation,
    }
}

fn parse_bundle_hash(value: &str) -> Result<consensus::BundleHash> {
    let digest = value
        .strip_prefix("sha256:")
        .context("bundle hash must use sha256")?;
    let bytes = hex::decode(digest).context("bundle hash is not hexadecimal")?;
    let hash: [u8; 32] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("bundle hash must contain 32 bytes"))?;
    Ok(consensus::BundleHash(hash))
}

fn domain_hash(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for field in fields {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> product::NodeIncarnation {
        product::NodeIncarnation {
            node_id: id.to_string(),
            incarnation: 2,
        }
    }

    fn bundle_evidence(
        id: &str,
        complete: bool,
        hash_verified: bool,
        fsynced: bool,
    ) -> product::BundleDurabilityEvidence {
        product::BundleDurabilityEvidence {
            cluster_id: "cluster".into(),
            node: node(id),
            failure_domain: format!("zone-{id}"),
            complete,
            hash_verified,
            fsynced,
        }
    }

    fn request() -> product::CertificationRequest {
        product::CertificationRequest {
            cluster_id: "cluster".to_string(),
            transaction_id: "tx-a".to_string(),
            principal: "tenant/1/principal/alice".to_string(),
            snapshot_version: 7,
            bundle: product::BundleIdentity {
                hash: format!("sha256:{}", "a".repeat(64)),
                length: 123,
            },
            durability: product::DurabilityLevel::Quorum,
            bundle_holders: vec![
                bundle_evidence("c", true, true, true),
                bundle_evidence("a", true, true, true),
            ],
            object_durability: vec![
                product::ObjectDurabilityEvidence::ShardPlacement {
                    cluster_id: "cluster".into(),
                    object_hash: format!("sha256:{}", "b".repeat(64)),
                    encoding_generation: 1,
                    stripe_ordinal: 0,
                    shard_ordinal: 0,
                    data_shards: 2,
                    parity_shards: 1,
                    node: node("b"),
                    failure_domain: "zone-b".to_string(),
                    complete: true,
                    hash_verified: true,
                    fsynced: true,
                },
                product::ObjectDurabilityEvidence::LocalRepresentation {
                    cluster_id: "cluster".into(),
                    object_hash: format!("sha256:{}", "b".repeat(64)),
                    node: node("invalid"),
                    failure_domain: "zone-invalid".to_string(),
                    complete: true,
                    hash_verified: false,
                    fsynced: true,
                },
            ],
            point_observations: vec![product::PointObservation {
                key: product::LogicalKey {
                    table_id: 8,
                    application_key: b"key".to_vec(),
                },
                observed_version: Some(6),
            }],
            range_observations: vec![product::RangeObservation {
                table_id: 8,
                start_application_key: Some(b"a".to_vec()),
                end_application_key: Some(b"z".to_vec()),
                conflict_key: product::RangeStampKey {
                    scheme_version: product::HierarchicalRangeStampScheme::SCHEME_VERSION,
                    table_id: 8,
                    key_prefix: Vec::new(),
                },
                observed_range_stamp: Some(5),
            }],
            predicates: Vec::new(),
            assignment_predicates: Vec::new(),
            advanced_range_stamps: vec![product::RangeStampKey {
                scheme_version: product::HierarchicalRangeStampScheme::SCHEME_VERSION,
                table_id: 8,
                key_prefix: Vec::new(),
            }],
            written_keys: vec![product::LogicalKey {
                table_id: 8,
                application_key: b"key".to_vec(),
            }],
            written_points: vec![(
                product::LogicalKey {
                    table_id: 8,
                    application_key: b"key".to_vec(),
                },
                Some(*blake3::hash(b"value").as_bytes()),
            )],
            max_command_bytes: product::TransactionResourceLimits::default()
                .max_certification_command_bytes,
        }
    }

    #[test]
    fn bundle_holders_never_promote_evidence_from_another_cluster() {
        let mut request = request();
        request
            .bundle_holders
            .push(product::BundleDurabilityEvidence {
                cluster_id: "foreign".into(),
                node: node("foreign-bundle"),
                failure_domain: "zone-foreign".into(),
                complete: true,
                hash_verified: true,
                fsynced: true,
            });

        let holders = valid_bundle_holders(&request.cluster_id, &request.bundle_holders);
        assert!(!holders.contains(&node_incarnation(&node("foreign-bundle"))));
    }

    fn request_for_bundle(bundle: product::TransactionBundle) -> product::CertificationRequest {
        let identity = bundle.identity().unwrap();
        let written_keys = bundle
            .writes
            .iter()
            .map(|write| write.key().clone())
            .collect();
        let written_points = bundle
            .writes
            .iter()
            .map(|write| match write {
                product::WriteOperation::Put { key, value } => {
                    (key.clone(), Some(*blake3::hash(value).as_bytes()))
                }
                product::WriteOperation::Delete { key } => (key.clone(), None),
            })
            .collect();
        product::CertificationRequest {
            cluster_id: bundle.cluster_id,
            transaction_id: bundle.transaction_id,
            principal: bundle.authenticated_principal,
            snapshot_version: bundle.snapshot_version,
            bundle: identity,
            durability: product::DurabilityLevel::Local,
            bundle_holders: vec![bundle_evidence("a", true, true, true)],
            object_durability: Vec::new(),
            point_observations: bundle.point_observations,
            range_observations: bundle.range_observations,
            predicates: bundle.predicates,
            assignment_predicates: bundle.assignment_predicates,
            advanced_range_stamps: bundle.advanced_range_stamps,
            written_keys,
            written_points,
            max_command_bytes: product::TransactionResourceLimits::default()
                .max_certification_command_bytes,
        }
    }

    #[test]
    fn only_valid_bundle_evidence_becomes_a_raft_holder() {
        let mut request = request();
        request
            .bundle_holders
            .push(bundle_evidence("invalid-bundle", true, false, true));

        let command = to_consensus_command(&request).unwrap();
        assert_eq!(command.durable_holders.len(), 2);
        assert!(
            command
                .durable_holders
                .contains(&node_incarnation(&node("a")))
        );
        assert!(
            command
                .durable_holders
                .contains(&node_incarnation(&node("c")))
        );
        assert!(
            !command
                .durable_holders
                .contains(&node_incarnation(&node("invalid-bundle")))
        );
    }

    #[test]
    fn object_durability_nodes_never_become_raft_bundle_holders() {
        let command = to_consensus_command(&request()).unwrap();
        assert_eq!(command.durable_holders.len(), 2);
        assert!(
            !command
                .durable_holders
                .contains(&node_incarnation(&node("b")))
        );
        assert!(
            !command
                .durable_holders
                .contains(&node_incarnation(&node("invalid")))
        );
    }

    #[test]
    fn certification_command_byte_limit_is_enforced_before_raft() {
        let mut request = request();
        request.max_command_bytes = 1;
        let error = to_consensus_command(&request).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("certification command byte limit")
        );
    }

    #[test]
    fn holder_order_is_canonical_and_independent_of_evidence_order() {
        let first = to_consensus_command(&request()).unwrap();
        let mut reordered = request();
        reordered.bundle_holders.reverse();
        reordered.object_durability.reverse();
        let second = to_consensus_command(&reordered).unwrap();
        assert_eq!(first.durable_holders, second.durable_holders);
        assert!(
            first
                .durable_holders
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
    }

    #[test]
    fn point_range_and_write_identifiers_are_domain_separated_and_stable() {
        let command = to_consensus_command(&request()).unwrap();
        assert_eq!(
            command.point_observations[0].key,
            command.written_point_keys[0]
        );
        assert_eq!(
            command.range_observations[0].range,
            command.advanced_range_stamps[0]
        );
        assert_ne!(
            command.point_observations[0].key.0,
            command.range_observations[0].range.0
        );
    }

    #[test]
    fn consensus_results_map_back_without_exposing_consensus_types() {
        let committed = from_consensus_result(consensus::CertificationResult::Committed {
            commit_version: consensus::CommitVersion(19),
            bundle_hash: consensus::BundleHash([1; 32]),
        });
        assert_eq!(
            committed,
            product::CertificationResult::Committed { commit_version: 19 }
        );

        let range = consensus::RangeConflictKey([7; 32]);
        let aborted = from_consensus_result(consensus::CertificationResult::Aborted {
            at_version: consensus::CommitVersion(20),
            bundle_hash: consensus::BundleHash([1; 32]),
            reason: consensus::CertificationAbort::RangeConflict {
                range,
                expected: Some(consensus::CommitVersion(18)),
                actual: Some(consensus::CommitVersion(19)),
            },
        });
        assert_eq!(
            aborted,
            product::CertificationResult::Aborted {
                reason: product::CertificationAbort::RangeConflict {
                    range_hash: range.0,
                },
            }
        );
    }

    #[test]
    fn assignment_conflict_maps_to_product_partition_identity() {
        let aborted = from_consensus_result(consensus::CertificationResult::Aborted {
            at_version: consensus::CommitVersion(20),
            bundle_hash: consensus::BundleHash([1; 32]),
            reason: consensus::CertificationAbort::AssignmentConflict {
                partition_id: 41,
                expected_epoch: consensus::CommitVersion(17),
                actual_epoch: Some(consensus::CommitVersion(18)),
                expected_topology_epoch: consensus::CommitVersion(7),
                actual_topology_epoch: consensus::CommitVersion(8),
            },
        });

        assert_eq!(
            aborted,
            product::CertificationResult::Aborted {
                reason: product::CertificationAbort::AssignmentConflict { partition_id: 41 },
            }
        );
    }

    #[test]
    fn insert_delete_and_rename_invalidate_an_earlier_range_scan() {
        let scheme = product::HierarchicalRangeStampScheme::new();
        let writers = [
            {
                let mut builder = product::TransactionBundleBuilder::new(
                    "cluster",
                    "insert",
                    0,
                    "principal",
                    scheme,
                );
                builder.put(
                    product::LogicalKey {
                        table_id: 7,
                        application_key: b"orders/m".to_vec(),
                    },
                    b"value".to_vec(),
                );
                builder.build().unwrap()
            },
            {
                let mut builder = product::TransactionBundleBuilder::new(
                    "cluster",
                    "delete",
                    0,
                    "principal",
                    scheme,
                );
                builder.delete(product::LogicalKey {
                    table_id: 7,
                    application_key: b"orders/n".to_vec(),
                });
                builder.build().unwrap()
            },
            {
                let mut builder = product::TransactionBundleBuilder::new(
                    "cluster",
                    "rename",
                    0,
                    "principal",
                    scheme,
                );
                builder.rename(
                    product::LogicalKey {
                        table_id: 3,
                        application_key: b"partition-a/source".to_vec(),
                    },
                    product::LogicalKey {
                        table_id: 7,
                        application_key: b"orders/p".to_vec(),
                    },
                    b"value".to_vec(),
                );
                builder.build().unwrap()
            },
        ];

        for (index, writer) in writers.into_iter().enumerate() {
            let mut scan = product::TransactionBundleBuilder::new(
                "cluster",
                format!("scan-{index}"),
                0,
                "principal",
                scheme,
            );
            scan.observe_range(7, b"orders/a".to_vec(), b"orders/z".to_vec(), None)
                .unwrap();

            let writer = to_consensus_command(&request_for_bundle(writer)).unwrap();
            let scanner = to_consensus_command(&request_for_bundle(scan.build().unwrap())).unwrap();
            let mut state = consensus::CertificationState::new(writer.cluster_id_hash).unwrap();
            assert!(matches!(
                state.apply(consensus::CommitVersion(1), &writer).unwrap(),
                consensus::CertificationResult::Committed { .. }
            ));
            assert!(matches!(
                state.apply(consensus::CommitVersion(2), &scanner).unwrap(),
                consensus::CertificationResult::Aborted {
                    reason: consensus::CertificationAbort::RangeConflict { .. },
                    ..
                }
            ));
        }
    }

    #[test]
    fn one_cross_table_transaction_advances_each_table_hierarchy() {
        let scheme = product::HierarchicalRangeStampScheme::new();
        let mut builder = product::TransactionBundleBuilder::new(
            "cluster",
            "cross-table",
            0,
            "principal",
            scheme,
        );
        builder
            .put(
                product::LogicalKey {
                    table_id: 2,
                    application_key: b"partition-a/key".to_vec(),
                },
                b"a".to_vec(),
            )
            .put(
                product::LogicalKey {
                    table_id: 11,
                    application_key: b"partition-z/key".to_vec(),
                },
                b"z".to_vec(),
            );
        let command = to_consensus_command(&request_for_bundle(builder.build().unwrap())).unwrap();
        assert_eq!(command.written_point_keys.len(), 2);
        assert!(command.advanced_range_stamps.len() >= 4);
    }
}
