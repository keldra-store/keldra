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
        let result = self
            .consensus
            .certify(to_consensus_command(&request)?)
            .await
            .context("consensus certification failed")?;
        Ok(from_consensus_result(result))
    }
}

pub(crate) fn to_consensus_command(
    request: &product::CertificationRequest,
) -> Result<consensus::CertifyTransaction> {
    let mut point_observations = request
        .point_observations
        .iter()
        .map(|observation| consensus::PointObservation {
            key: logical_key_hash(&observation.key),
            observed_version: observation.observed_version.map(consensus::CommitVersion),
        })
        .collect::<Vec<_>>();
    point_observations.sort();
    point_observations.dedup();

    let mut range_observations = request
        .range_observations
        .iter()
        .map(|observation| consensus::RangeObservation {
            range: range_conflict_hash(
                observation.table_id,
                &observation.start_application_key,
                &observation.end_application_key,
            ),
            observed_stamp: Some(consensus::CommitVersion(observation.observed_range_stamp)),
        })
        .collect::<Vec<_>>();
    range_observations.sort();
    range_observations.dedup();

    let mut written_point_keys = request
        .written_keys
        .iter()
        .map(logical_key_hash)
        .collect::<Vec<_>>();
    written_point_keys.sort();
    written_point_keys.dedup();

    let mut advanced_range_stamps = request
        .advanced_range_stamps
        .iter()
        .map(|range| {
            range_conflict_hash(
                range.table_id,
                &range.start_application_key,
                &range.end_application_key,
            )
        })
        .collect::<Vec<_>>();
    advanced_range_stamps.sort();
    advanced_range_stamps.dedup();

    Ok(consensus::CertifyTransaction {
        transaction_id: transaction_id(&request.transaction_id),
        snapshot_version: consensus::CommitVersion(request.snapshot_version),
        point_observations,
        range_observations,
        written_point_keys,
        advanced_range_stamps,
        bundle_hash: parse_bundle_hash(&request.bundle.hash)?,
        bundle_length: request.bundle.length,
        durability: match request.durability {
            product::DurabilityLevel::Local => consensus::DurabilityLevel::Local,
            product::DurabilityLevel::Quorum => consensus::DurabilityLevel::Quorum,
            product::DurabilityLevel::Erasure => consensus::DurabilityLevel::Erasure,
        },
        durable_holders: valid_durable_holders(request),
    })
}

fn valid_durable_holders(
    request: &product::CertificationRequest,
) -> Vec<consensus::NodeIncarnation> {
    let mut holders = BTreeSet::new();
    for evidence in &request.bundle_holders {
        if evidence.complete && evidence.hash_verified && evidence.fsynced {
            holders.insert(node_incarnation(&evidence.node));
        }
    }
    for evidence in &request.object_durability {
        match evidence {
            product::ObjectDurabilityEvidence::LocalRepresentation {
                node,
                complete: true,
                hash_verified: true,
                fsynced: true,
                ..
            }
            | product::ObjectDurabilityEvidence::ShardPlacement {
                node,
                complete: true,
                hash_verified: true,
                fsynced: true,
                ..
            } => {
                holders.insert(node_incarnation(node));
            }
            _ => {}
        }
    }
    holders.into_iter().collect()
}

fn from_consensus_result(result: consensus::CertificationResult) -> product::CertificationResult {
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
            };
            product::CertificationResult::Aborted { reason }
        }
    }
}

fn transaction_id(value: &str) -> consensus::TransactionId {
    let digest = domain_hash(b"anvil.mvcc.transaction-id.v1", &[value.as_bytes()]);
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest[..16]);
    consensus::TransactionId(id)
}

fn logical_key_hash(key: &product::LogicalKey) -> consensus::LogicalKeyHash {
    consensus::LogicalKeyHash(domain_hash(
        b"anvil.mvcc.logical-key.v1",
        &[&key.table_id.to_be_bytes(), &key.application_key],
    ))
}

fn range_conflict_hash(table_id: u16, start: &[u8], end: &[u8]) -> consensus::RangeConflictKey {
    consensus::RangeConflictKey(domain_hash(
        b"anvil.mvcc.range-conflict.v1",
        &[&table_id.to_be_bytes(), start, end],
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
            node: node(id),
            failure_domain: format!("zone-{id}"),
            complete,
            hash_verified,
            fsynced,
        }
    }

    fn request() -> product::CertificationRequest {
        product::CertificationRequest {
            transaction_id: "tx-a".to_string(),
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
                start_application_key: b"a".to_vec(),
                end_application_key: b"z".to_vec(),
                observed_range_stamp: 5,
            }],
            advanced_range_stamps: vec![product::RangeConflict {
                table_id: 8,
                start_application_key: b"a".to_vec(),
                end_application_key: b"z".to_vec(),
            }],
            written_keys: vec![product::LogicalKey {
                table_id: 8,
                application_key: b"key".to_vec(),
            }],
        }
    }

    #[test]
    fn invalid_durability_evidence_never_becomes_a_raft_holder() {
        let command = to_consensus_command(&request()).unwrap();
        assert_eq!(command.durable_holders.len(), 3);
        assert!(
            !command
                .durable_holders
                .contains(&node_incarnation(&node("invalid")))
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
}
