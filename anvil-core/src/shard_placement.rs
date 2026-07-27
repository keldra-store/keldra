//! Deterministic final shard placement and ingest-to-replication adaptation.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;
use uuid::Uuid;

use crate::{
    mvcc_transaction::{DurabilityLevel, NodeIncarnation, ObjectDurabilityEvidence},
    replication::{AckStatus, ReplicationAck},
    streaming_erasure::{
        EncodedObject, EncodedShard, ErasureProfile, ShardSink, StreamingErasureEncoder,
    },
};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ShardTarget {
    pub cluster_id: String,
    pub node: NodeIncarnation,
    pub failure_domain: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacementPlan {
    pub targets_by_ordinal: Vec<ShardTarget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardPlacementPolicy {
    pub tolerated_failure_domains: usize,
}

impl ShardPlacementPolicy {
    pub fn plan(
        self,
        object_identity: Uuid,
        encoding_generation: u64,
        profile: ErasureProfile,
        candidates: &[ShardTarget],
    ) -> Result<ShardPlacementPlan> {
        let total = profile
            .data_shards
            .checked_add(profile.parity_shards)
            .context("erasure shard count overflow")?;
        if candidates.len() < total {
            bail!("not enough distinct nodes for k+m shard placement");
        }
        let mut seen_nodes = BTreeSet::new();
        let mut domains = BTreeMap::<String, Vec<ShardTarget>>::new();
        for candidate in candidates {
            if candidate.cluster_id.trim().is_empty()
                || candidate.node.node_id.trim().is_empty()
                || candidate.node.incarnation == 0
                || candidate.failure_domain.trim().is_empty()
            {
                bail!("shard target identity and failure domain must be valid");
            }
            if !seen_nodes.insert(candidate.node.clone()) {
                bail!("shard placement candidates contain a duplicate node incarnation");
            }
            domains
                .entry(candidate.failure_domain.clone())
                .or_default()
                .push(candidate.clone());
        }
        if domains.len() <= self.tolerated_failure_domains {
            bail!("not enough failure domains for requested tolerance");
        }
        for nodes in domains.values_mut() {
            nodes.sort_by(|left, right| {
                target_score(object_identity, encoding_generation, &left.node)
                    .cmp(&target_score(
                        object_identity,
                        encoding_generation,
                        &right.node,
                    ))
                    .then_with(|| left.node.cmp(&right.node))
            });
        }
        let mut domain_order = domains.keys().cloned().collect::<Vec<_>>();
        domain_order.sort_by(|left, right| {
            domain_score(object_identity, encoding_generation, left)
                .cmp(&domain_score(object_identity, encoding_generation, right))
                .then_with(|| left.cmp(right))
        });

        let mut used = BTreeSet::new();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut targets = Vec::with_capacity(total);
        for ordinal in 0..total {
            let start = ordinal % domain_order.len();
            let selected = (0..domain_order.len())
                .find_map(|step| {
                    let domain = &domain_order[(start + step) % domain_order.len()];
                    let count = counts.get(domain).copied().unwrap_or(0);
                    let minimum = counts.values().copied().min().unwrap_or(0);
                    if count > minimum {
                        return None;
                    }
                    domains[domain]
                        .iter()
                        .find(|target| !used.contains(&target.node))
                        .cloned()
                })
                .or_else(|| {
                    domain_order.iter().find_map(|domain| {
                        domains[domain]
                            .iter()
                            .find(|target| !used.contains(&target.node))
                            .cloned()
                    })
                })
                .context("not enough unused target nodes")?;
            used.insert(selected.node.clone());
            *counts.entry(selected.failure_domain.clone()).or_default() += 1;
            targets.push(selected);
        }
        ensure_failure_survival(
            &targets,
            profile.data_shards,
            self.tolerated_failure_domains,
        )?;
        Ok(ShardPlacementPlan {
            targets_by_ordinal: targets,
        })
    }
}

#[async_trait]
pub trait ShardTargetStream: Send + Sync {
    /// Sends one complete stripe shard and waits for its application ACK.
    async fn send(&self, target: &ShardTarget, shard: &EncodedShard<'_>) -> Result<ReplicationAck>;
}

pub struct DistributedIngest<'a, T> {
    transport: &'a T,
    plan: &'a ShardPlacementPlan,
    durability: DurabilityLevel,
    profile: ErasureProfile,
    tolerated_failure_domains: usize,
    completed: Vec<CompletedShard>,
    failures: Vec<String>,
}

#[derive(Clone, Debug)]
struct CompletedShard {
    stripe_ordinal: u64,
    shard_ordinal: u16,
    payload_length: u64,
    payload_hash: [u8; 32],
    target: ShardTarget,
}

#[derive(Debug)]
pub struct DistributedIngestResult {
    pub encoded: EncodedObject,
    pub evidence: Vec<ObjectDurabilityEvidence>,
    pub placements: Vec<CompletedShardPlacement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedShardPlacement {
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub payload_length: u64,
    pub payload_hash: [u8; 32],
    pub target: ShardTarget,
}

impl<'a, T: ShardTargetStream> DistributedIngest<'a, T> {
    pub async fn encode<R: AsyncRead + Unpin + Send>(
        transport: &'a T,
        plan: &'a ShardPlacementPlan,
        policy: ShardPlacementPolicy,
        profile: ErasureProfile,
        durability: DurabilityLevel,
        reader: &mut R,
        transaction_id: &str,
        prepared_snapshot_version: u64,
        prepared_at_unix_ms: u64,
        provisional: bool,
        object_identity: Uuid,
        expected_object_hash: Option<&str>,
        encoding_generation: u64,
    ) -> Result<DistributedIngestResult> {
        if durability == DurabilityLevel::Local {
            bail!("local durability uses a local representation, not distributed shard ingest");
        }
        if plan.targets_by_ordinal.len() != profile.data_shards + profile.parity_shards {
            bail!("shard placement plan does not match erasure profile");
        }
        let encoder = StreamingErasureEncoder::new(profile)?;
        let mut sink = Self {
            transport,
            plan,
            durability,
            profile,
            tolerated_failure_domains: policy.tolerated_failure_domains,
            completed: Vec::new(),
            failures: Vec::new(),
        };
        let encoded = encoder
            .encode(
                reader,
                transaction_id,
                prepared_snapshot_version,
                prepared_at_unix_ms,
                provisional,
                object_identity,
                encoding_generation,
                &mut sink,
            )
            .await?;
        if let Some(expected_object_hash) = expected_object_hash {
            let expected_hash = parse_sha256(expected_object_hash)?;
            if encoded.content_hash != expected_hash {
                bail!("streamed object content hash does not match expected object identity");
            }
        }
        let object_hash = format!("sha256:{}", hex::encode(encoded.content_hash));
        sink.validate(encoded.stripe_count)?;
        let placements = sink
            .completed
            .iter()
            .map(|completed| CompletedShardPlacement {
                stripe_ordinal: completed.stripe_ordinal,
                shard_ordinal: completed.shard_ordinal,
                payload_length: completed.payload_length,
                payload_hash: completed.payload_hash,
                target: completed.target.clone(),
            })
            .collect();
        let evidence = sink
            .completed
            .into_iter()
            .map(|completed| ObjectDurabilityEvidence::ShardPlacement {
                cluster_id: completed.target.cluster_id.clone(),
                object_hash: object_hash.clone(),
                encoding_generation,
                stripe_ordinal: completed.stripe_ordinal,
                shard_ordinal: completed.shard_ordinal,
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                node: completed.target.node,
                failure_domain: completed.target.failure_domain,
                complete: true,
                hash_verified: true,
                fsynced: true,
            })
            .collect();
        Ok(DistributedIngestResult {
            encoded,
            evidence,
            placements,
        })
    }

    fn validate(&self, stripe_count: u64) -> Result<()> {
        for stripe in 0..stripe_count {
            let completed = self
                .completed
                .iter()
                .filter(|entry| entry.stripe_ordinal == stripe)
                .collect::<Vec<_>>();
            if self.durability == DurabilityLevel::Erasure
                && completed.len() != self.profile.data_shards + self.profile.parity_shards
            {
                bail!("erasure durability requires Complete ACKs for every planned shard");
            }
            let targets = completed
                .iter()
                .map(|entry| entry.target.clone())
                .collect::<Vec<_>>();
            ensure_failure_survival(
                &targets,
                self.profile.data_shards,
                self.tolerated_failure_domains,
            )
            .with_context(|| {
                format!(
                    "stripe {stripe} lacks policy-safe Complete ACKs: {}",
                    self.failures.join("; ")
                )
            })?;
        }
        Ok(())
    }
}

#[async_trait]
impl<T: ShardTargetStream> ShardSink for DistributedIngest<'_, T> {
    async fn send(&mut self, shard: EncodedShard<'_>) -> Result<()> {
        let started_at = std::time::Instant::now();
        let durability = match self.durability {
            DurabilityLevel::Local => "local",
            DurabilityLevel::Quorum => "quorum",
            DurabilityLevel::Erasure => "erasure",
        };
        let target = self
            .plan
            .targets_by_ordinal
            .get(usize::from(shard.shard_ordinal))
            .context("encoder produced an unplanned shard ordinal")?;
        match self.transport.send(target, &shard).await {
            Ok(ack)
                if ack.status == AckStatus::Complete
                    && ack.completed_hash == Some(shard.payload_hash) =>
            {
                crate::perf::record_ingest_shard_stream(
                    durability,
                    "complete",
                    started_at.elapsed(),
                    shard.payload.len() as u64,
                );
                tracing::debug!(
                    operation = "shard.stream",
                    node_id = %target.node.node_id,
                    incarnation = target.node.incarnation,
                    failure_domain = %target.failure_domain,
                    stripe_ordinal = shard.stripe_ordinal,
                    shard_ordinal = shard.shard_ordinal,
                    "shard stream received durable completion ACK"
                );
                self.completed.push(CompletedShard {
                    stripe_ordinal: shard.stripe_ordinal,
                    shard_ordinal: shard.shard_ordinal,
                    payload_length: shard.payload.len() as u64,
                    payload_hash: shard.payload_hash,
                    target: target.clone(),
                });
                Ok(())
            }
            Ok(ack) => {
                crate::perf::record_ingest_shard_stream(
                    durability,
                    "invalid_ack",
                    started_at.elapsed(),
                    shard.payload.len() as u64,
                );
                tracing::warn!(
                    operation = "shard.stream",
                    node_id = %target.node.node_id,
                    incarnation = target.node.incarnation,
                    failure_domain = %target.failure_domain,
                    stripe_ordinal = shard.stripe_ordinal,
                    shard_ordinal = shard.shard_ordinal,
                    ack_status = ?ack.status,
                    "shard stream received invalid completion ACK"
                );
                let failure = format!(
                    "shard {} received {:?} or a mismatched completion hash",
                    shard.shard_ordinal, ack.status
                );
                if self.durability == DurabilityLevel::Erasure {
                    bail!(failure);
                }
                self.failures.push(failure);
                Ok(())
            }
            Err(error) => {
                crate::perf::record_ingest_shard_stream(
                    durability,
                    "error",
                    started_at.elapsed(),
                    shard.payload.len() as u64,
                );
                tracing::warn!(
                    operation = "shard.stream",
                    node_id = %target.node.node_id,
                    incarnation = target.node.incarnation,
                    failure_domain = %target.failure_domain,
                    stripe_ordinal = shard.stripe_ordinal,
                    shard_ordinal = shard.shard_ordinal,
                    %error,
                    "shard stream failed before durable completion ACK"
                );
                if self.durability == DurabilityLevel::Erasure {
                    return Err(error);
                }
                self.failures.push(error.to_string());
                Ok(())
            }
        }
    }
}

pub struct LocalRepresentationAck {
    pub cluster_id: String,
    pub node: NodeIncarnation,
    pub failure_domain: String,
    pub status: AckStatus,
    pub completed_hash: Option<[u8; 32]>,
}

pub fn local_durability_evidence(
    object_hash: &str,
    ack: LocalRepresentationAck,
) -> Result<ObjectDurabilityEvidence> {
    let expected = parse_sha256(object_hash)?;
    if ack.status != AckStatus::Complete || ack.completed_hash != Some(expected) {
        bail!("local representation requires a matching Complete ACK");
    }
    Ok(ObjectDurabilityEvidence::LocalRepresentation {
        cluster_id: ack.cluster_id,
        object_hash: object_hash.to_string(),
        node: ack.node,
        failure_domain: ack.failure_domain,
        complete: true,
        hash_verified: true,
        fsynced: true,
    })
}

fn ensure_failure_survival(
    targets: &[ShardTarget],
    required_shards: usize,
    tolerated_failures: usize,
) -> Result<()> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for target in targets {
        *counts.entry(&target.failure_domain).or_default() += 1;
    }
    if counts.len() <= tolerated_failures {
        bail!("shards do not span enough failure domains");
    }
    let mut counts = counts.into_values().collect::<Vec<_>>();
    counts.sort_unstable_by(|left, right| right.cmp(left));
    let lost = counts.into_iter().take(tolerated_failures).sum::<usize>();
    if targets.len().saturating_sub(lost) < required_shards {
        bail!("shards are not reconstructable after tolerated failure-domain loss");
    }
    Ok(())
}

fn parse_sha256(value: &str) -> Result<[u8; 32]> {
    let digest = value
        .strip_prefix("sha256:")
        .context("object hash must use sha256")?;
    let bytes = hex::decode(digest)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("object hash must contain 32 bytes"))
}

fn domain_score(object: Uuid, generation: u64, domain: &str) -> [u8; 32] {
    score(object, generation, domain.as_bytes())
}

fn target_score(object: Uuid, generation: u64, node: &NodeIncarnation) -> [u8; 32] {
    score(
        object,
        generation,
        format!("{}\0{}", node.node_id, node.incarnation).as_bytes(),
    )
}

fn score(object: Uuid, generation: u64, value: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"anvil.shard-placement.v1");
    hash.update(object.as_bytes());
    hash.update(generation.to_be_bytes());
    hash.update(value);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::Cursor,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;

    fn target(index: usize, domain: &str) -> ShardTarget {
        ShardTarget {
            cluster_id: "cluster-a".into(),
            node: NodeIncarnation {
                node_id: format!("node-{index}"),
                incarnation: 1,
            },
            failure_domain: domain.to_string(),
        }
    }

    fn candidates() -> Vec<ShardTarget> {
        vec![
            target(0, "zone-a"),
            target(1, "zone-b"),
            target(2, "zone-c"),
            target(3, "zone-d"),
        ]
    }

    fn profile() -> ErasureProfile {
        ErasureProfile {
            data_shards: 2,
            parity_shards: 2,
            shard_bytes: 4,
        }
    }

    struct Transport {
        statuses: BTreeMap<u16, AckStatus>,
        seen: Arc<Mutex<Vec<(u64, u16, String)>>>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ShardTargetStream for Transport {
        async fn send(
            &self,
            target: &ShardTarget,
            shard: &EncodedShard<'_>,
        ) -> Result<ReplicationAck> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.seen.lock().unwrap().push((
                shard.stripe_ordinal,
                shard.shard_ordinal,
                target.node.node_id.clone(),
            ));
            self.active.fetch_sub(1, Ordering::SeqCst);
            let status = self
                .statuses
                .get(&shard.shard_ordinal)
                .copied()
                .unwrap_or(AckStatus::Complete);
            Ok(ReplicationAck {
                session_id: Uuid::new_v4(),
                acknowledged_sequence: u64::from(shard.shard_ordinal) + 1,
                transfer_id: Uuid::new_v4(),
                persisted_through: shard.payload.len() as u64,
                completed_hash: (status == AckStatus::Complete).then_some(shard.payload_hash),
                status,
            })
        }
    }

    fn transport(statuses: BTreeMap<u16, AckStatus>) -> Transport {
        Transport {
            statuses,
            seen: Arc::new(Mutex::new(Vec::new())),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn object_hash(bytes: &[u8]) -> String {
        format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
    }

    #[test]
    fn placement_is_deterministic_unique_and_failure_domain_safe() {
        let policy = ShardPlacementPolicy {
            tolerated_failure_domains: 1,
        };
        let identity = Uuid::from_u128(7);
        let first = policy.plan(identity, 2, profile(), &candidates()).unwrap();
        let mut reversed = candidates();
        reversed.reverse();
        let second = policy.plan(identity, 2, profile(), &reversed).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first
                .targets_by_ordinal
                .iter()
                .map(|target| &target.node)
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
        assert_eq!(
            first
                .targets_by_ordinal
                .iter()
                .map(|target| &target.failure_domain)
                .collect::<BTreeSet<_>>()
                .len(),
            4
        );
    }

    #[tokio::test]
    async fn quorum_accepts_only_complete_acks_when_remaining_set_is_safe() {
        let policy = ShardPlacementPolicy {
            tolerated_failure_domains: 1,
        };
        let object_identity = Uuid::from_u128(9);
        let plan = policy
            .plan(object_identity, 1, profile(), &candidates())
            .unwrap();
        let transport = transport(BTreeMap::from([(3, AckStatus::Persisted)]));
        let bytes = b"one stripe";
        let result = DistributedIngest::encode(
            &transport,
            &plan,
            policy,
            profile(),
            DurabilityLevel::Quorum,
            &mut Cursor::new(bytes),
            "tx",
            1,
            1,
            true,
            object_identity,
            Some(&object_hash(bytes)),
            1,
        )
        .await
        .unwrap();
        assert_eq!(result.evidence.len(), 6);
        assert!(result.evidence.iter().all(|entry| matches!(
            entry,
            ObjectDurabilityEvidence::ShardPlacement {
                shard_ordinal,
                complete: true,
                hash_verified: true,
                fsynced: true,
                ..
            } if *shard_ordinal != 3
        )));
        assert_eq!(transport.max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn erasure_rejects_incomplete_ack_immediately() {
        let policy = ShardPlacementPolicy {
            tolerated_failure_domains: 1,
        };
        let object_identity = Uuid::from_u128(11);
        let plan = policy
            .plan(object_identity, 1, profile(), &candidates())
            .unwrap();
        let transport = transport(BTreeMap::from([(2, AckStatus::Persisted)]));
        let bytes = b"abcdefgh";
        let error = DistributedIngest::encode(
            &transport,
            &plan,
            policy,
            profile(),
            DurabilityLevel::Erasure,
            &mut Cursor::new(bytes),
            "tx",
            1,
            1,
            true,
            object_identity,
            Some(&object_hash(bytes)),
            1,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("Persisted"));
    }

    #[tokio::test]
    async fn quorum_rejects_ack_set_that_cannot_survive_domain_loss() {
        let policy = ShardPlacementPolicy {
            tolerated_failure_domains: 1,
        };
        let object_identity = Uuid::from_u128(13);
        let plan = policy
            .plan(object_identity, 1, profile(), &candidates())
            .unwrap();
        let transport = transport(BTreeMap::from([
            (2, AckStatus::Received),
            (3, AckStatus::Persisted),
        ]));
        let bytes = b"abcdefgh";
        let error = DistributedIngest::encode(
            &transport,
            &plan,
            policy,
            profile(),
            DurabilityLevel::Quorum,
            &mut Cursor::new(bytes),
            "tx",
            1,
            1,
            true,
            object_identity,
            Some(&object_hash(bytes)),
            1,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("policy-safe Complete ACKs"));
    }

    #[test]
    fn local_evidence_requires_matching_complete_ack() {
        let bytes = b"local object";
        let hash = object_hash(bytes);
        let node = NodeIncarnation {
            node_id: "local".into(),
            incarnation: 1,
        };
        let incomplete = LocalRepresentationAck {
            cluster_id: "cluster-a".into(),
            node: node.clone(),
            failure_domain: "zone-a".into(),
            status: AckStatus::Persisted,
            completed_hash: Some(Sha256::digest(bytes).into()),
        };
        assert!(local_durability_evidence(&hash, incomplete).is_err());
        let complete = LocalRepresentationAck {
            cluster_id: "cluster-a".into(),
            node,
            failure_domain: "zone-a".into(),
            status: AckStatus::Complete,
            completed_hash: Some(Sha256::digest(bytes).into()),
        };
        assert!(matches!(
            local_durability_evidence(&hash, complete).unwrap(),
            ObjectDurabilityEvidence::LocalRepresentation {
                complete: true,
                hash_verified: true,
                fsynced: true,
                ..
            }
        ));
    }
}
