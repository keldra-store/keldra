use super::*;
use crate::anvil_api::{
    FailoverVoteReceipt, ReadRootRequest, VoteFailoverRequest,
    root_register_internal_client::RootRegisterInternalClient,
};
use crate::formats::hash32;
use crate::mesh_lifecycle;
use futures_util::{StreamExt, stream::FuturesUnordered};
use prost::Message;
use tonic::metadata::MetadataValue;

const ROOT_FAILOVER_VOTE_SCHEMA: &str = "anvil.core.root_failover_vote.v1";
const ROOT_FAILOVER_CERTIFICATE_SCHEMA: &str = "anvil.core.root_failover_certificate.v1";
const ROOT_FAILOVER_PROBE_COUNT: u32 = 3;
const ROOT_FAILOVER_PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const ROOT_FAILOVER_CONFIRMED_EVIDENCE_TTL: Duration = Duration::from_secs(30);
const ROOT_FAILOVER_VOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const ROOT_FAILOVER_TIMEOUT: Duration = Duration::from_secs(5);
const ROOT_FAILOVER_VOTE_ROUNDS: usize = 4;
const ROOT_FAILOVER_REASON_OWNER_HEALTHY: &str = "owner_healthy";
const ROOT_FAILOVER_REASON_OWNER_UNREACHABLE: &str = "owner_unreachable";
const ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED: &str =
    "synthetic_control_owner_retired_by_canonical_activation";

#[derive(Clone, PartialEq, Message)]
struct RootFailoverVoteRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    current_generation: u64,
    #[prost(string, tag = "5")]
    current_root_hash: String,
    #[prost(uint64, tag = "6")]
    target_generation: u64,
    #[prost(string, tag = "7")]
    register_cohort_hash: String,
    #[prost(string, tag = "8")]
    observed_owner_node_id: String,
    #[prost(string, tag = "9")]
    proposed_owner_node_id: String,
    #[prost(uint64, tag = "10")]
    previous_owner_fence: u64,
    #[prost(uint64, tag = "11")]
    proposed_owner_fence: u64,
    #[prost(string, tag = "12")]
    evidence_hash: String,
    #[prost(string, tag = "13")]
    voter_node_id: String,
    #[prost(string, tag = "14")]
    decision: String,
    #[prost(string, tag = "15")]
    reason_code: String,
    #[prost(uint32, tag = "16")]
    failed_probe_count: u32,
    #[prost(uint64, tag = "17")]
    first_failed_probe_unix_nanos: u64,
    #[prost(uint64, tag = "18")]
    last_probe_unix_nanos: u64,
    #[prost(string, tag = "19")]
    signed_payload_hash: String,
    #[prost(bytes, tag = "20")]
    voter_signature: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RootFailoverCertificateRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    current_generation: u64,
    #[prost(string, tag = "5")]
    current_root_hash: String,
    #[prost(uint64, tag = "6")]
    target_generation: u64,
    #[prost(string, tag = "7")]
    register_cohort_hash: String,
    #[prost(string, tag = "8")]
    previous_owner_node_id: String,
    #[prost(string, tag = "9")]
    new_owner_node_id: String,
    #[prost(uint64, tag = "10")]
    previous_owner_fence: u64,
    #[prost(uint64, tag = "11")]
    new_owner_fence: u64,
    #[prost(bytes, repeated, tag = "12")]
    vote_receipts: Vec<Vec<u8>>,
    #[prost(string, tag = "13")]
    certificate_hash: String,
    #[prost(uint64, tag = "14")]
    created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RootOwnerTerms {
    pub(super) owner_node_id: String,
    pub(super) owner_epoch: u64,
    pub(super) owner_fence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerProbeState {
    Healthy,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RootOwnerFailureScope {
    register_ownership_set_hash: String,
    failed_owner_node_id: String,
    candidate_owner_node_id: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RootOwnerFailureEvidence {
    failed_probe_count: u32,
    first_failed_probe_unix_nanos: u64,
    last_probe_unix_nanos: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootFailoverVoteEvidence {
    OwnerProbe {
        probe_state: OwnerProbeState,
        failure_evidence: RootOwnerFailureEvidence,
    },
    SyntheticOwnerRetiredByCanonicalActivation,
}

#[derive(Debug, Default)]
pub(super) struct RootOwnerFailureTracker {
    observations: BTreeMap<RootOwnerFailureScope, RootOwnerFailureEvidence>,
    refreshes_in_flight: BTreeSet<RootOwnerFailureScope>,
}

impl RootOwnerFailureTracker {
    fn confirmed_evidence(
        &mut self,
        scope: &RootOwnerFailureScope,
        now: u64,
    ) -> Option<(RootOwnerFailureEvidence, bool)> {
        let evidence = *self.observations.get(scope)?;
        let observed_failure_window = evidence
            .last_probe_unix_nanos
            .saturating_sub(evidence.first_failed_probe_unix_nanos);
        let evidence_age = now.saturating_sub(evidence.last_probe_unix_nanos);
        if evidence.failed_probe_count < ROOT_FAILOVER_PROBE_COUNT
            || observed_failure_window < ROOT_FAILOVER_TIMEOUT.as_nanos() as u64
            || evidence_age > ROOT_FAILOVER_CONFIRMED_EVIDENCE_TTL.as_nanos() as u64
        {
            return None;
        }
        let refresh_started = self.refreshes_in_flight.insert(scope.clone());
        Some((evidence, refresh_started))
    }

    fn observe(
        &mut self,
        scope: RootOwnerFailureScope,
        probe_state: OwnerProbeState,
        now: u64,
    ) -> RootOwnerFailureEvidence {
        match probe_state {
            OwnerProbeState::Healthy => {
                self.observations.remove(&scope);
                RootOwnerFailureEvidence {
                    last_probe_unix_nanos: now,
                    ..Default::default()
                }
            }
            OwnerProbeState::Failed => {
                let evidence = self.observations.entry(scope).or_default();
                if evidence.failed_probe_count == 0 {
                    evidence.first_failed_probe_unix_nanos = now;
                }
                evidence.failed_probe_count = evidence.failed_probe_count.saturating_add(1);
                evidence.last_probe_unix_nanos = now;
                *evidence
            }
        }
    }

    fn finish_refresh(
        &mut self,
        scope: RootOwnerFailureScope,
        probe_state: OwnerProbeState,
        now: u64,
    ) {
        self.refreshes_in_flight.remove(&scope);
        self.observe(scope, probe_state, now);
    }
}

impl CoreStore {
    /// Selects the canonical-settlement owner without probing network liveness.
    pub(crate) async fn canonical_settlement_owner(
        &self,
        current_owner_node_id: &str,
    ) -> Result<String> {
        validate_logical_id(
            current_owner_node_id,
            "canonical settlement current owner node id",
        )?;
        let profile = self.default_coremeta_quorum_profile()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        if replicas.len() != profile.replica_count {
            bail!(
                "CoreStore canonical settlement requires a complete CoreMeta R{} cohort",
                profile.replica_count
            );
        }
        canonical_settlement_owner_from_replicas(current_owner_node_id, &replicas)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("CoreStore canonical settlement has no eligible root owner"))
    }

    pub(crate) async fn coremeta_write_route(
        &self,
        root_key_hash: &str,
    ) -> Result<CoreMetaWriteRoute> {
        validate_hash(root_key_hash, "CoreMeta write route root key hash")?;
        let Some(current) = self.read_current_root_anchor_unverified(root_key_hash)? else {
            return Ok(CoreMetaWriteRoute::Local);
        };
        if current.root_generation == 0 || current.publisher_node_id == self.node_identity.node_id {
            return Ok(CoreMetaWriteRoute::Local);
        }

        let profile = self.default_coremeta_quorum_profile()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let current_hash = hash_root_anchor_record(&current)?;
        let probe_state = self
            .probe_root_owner(&current, &current_hash, &replicas)
            .await;
        let target_node_id =
            root_write_target_node(&current.publisher_node_id, probe_state, &replicas)
                .ok_or_else(|| anyhow!("CoreStore root write route has no eligible target"))?
                .to_string();
        if target_node_id == self.node_identity.node_id {
            // Resolve failover before the public service consumes a streamed
            // request body. The owner may recover between the route probe and
            // the quorum vote; in that case the voters correctly reject the
            // failover and the request must be proxied to the healthy owner
            // rather than failing after its body can no longer be replayed.
            match self.ensure_root_publication_owner(Some(&current)).await {
                Ok(()) => return Ok(CoreMetaWriteRoute::Local),
                Err(failover_error) => {
                    let refreshed = self
                        .read_current_root_anchor_unverified(root_key_hash)?
                        .ok_or_else(|| {
                            anyhow!("CoreStore root owner disappeared during routing")
                        })?;
                    let refreshed_hash = hash_root_anchor_record(&refreshed)?;
                    let refreshed_probe = self
                        .probe_root_owner(&refreshed, &refreshed_hash, &replicas)
                        .await;
                    if refreshed_probe == OwnerProbeState::Healthy
                        && refreshed.publisher_node_id != self.node_identity.node_id
                    {
                        return remote_write_route(&refreshed.publisher_node_id, &replicas);
                    }
                    return Err(failover_error);
                }
            }
        }
        remote_write_route(&target_node_id, &replicas)
    }

    pub(super) async fn ensure_root_publication_owner(
        &self,
        current: Option<&CoreRootAnchorRecord>,
    ) -> Result<()> {
        let Some(current) = current.filter(|anchor| anchor.root_generation > 0) else {
            return Ok(());
        };
        if current.publisher_node_id == self.node_identity.node_id {
            return Ok(());
        }
        if self
            .read_root_failover_certificate(
                current,
                current.root_generation.saturating_add(1),
                &self.node_identity.node_id,
            )?
            .is_some()
        {
            return Ok(());
        }
        self.collect_root_failover_certificate(current).await
    }

    pub(super) fn root_owner_terms_for_publication(
        &self,
        root_key_hash: &str,
        target_generation: u64,
        publisher_node_id: &str,
    ) -> Result<RootOwnerTerms> {
        let Some(current) = self.read_current_root_anchor_unverified(root_key_hash)? else {
            return initial_owner_terms(publisher_node_id);
        };
        if current.root_generation == 0 {
            return initial_owner_terms(publisher_node_id);
        }
        if current.root_generation.saturating_add(1) != target_generation {
            bail!("CoreStore owner terms target generation is stale");
        }
        if current.publisher_node_id == publisher_node_id {
            return Ok(RootOwnerTerms {
                owner_node_id: publisher_node_id.to_string(),
                owner_epoch: current.publisher_epoch,
                owner_fence: current.partition_owner_fence,
            });
        }
        let certificate =
            self.read_root_failover_certificate(&current, target_generation, publisher_node_id)?;
        let granted_vote = self.read_root_failover_vote_for_anchor(&current, target_generation)?;
        let (new_owner_node_id, new_owner_fence) = if let Some(certificate) = certificate {
            (certificate.new_owner_node_id, certificate.new_owner_fence)
        } else if let Some(vote) = granted_vote.filter(|vote| {
            vote.decision == "grant" && vote.proposed_owner_node_id == publisher_node_id
        }) {
            (vote.proposed_owner_node_id, vote.proposed_owner_fence)
        } else {
            bail!(
                "CoreStore root {} is owned by {}; failover grant for {} is absent",
                root_key_hash,
                current.publisher_node_id,
                publisher_node_id
            );
        };
        Ok(RootOwnerTerms {
            owner_node_id: new_owner_node_id,
            owner_epoch: current.publisher_epoch,
            owner_fence: new_owner_fence,
        })
    }

    pub(crate) async fn vote_root_owner_failover(
        &self,
        request: &VoteFailoverRequest,
    ) -> Result<Option<FailoverVoteReceipt>> {
        validate_hash(&request.root_key_hash, "root failover key hash")?;
        validate_logical_id(
            &request.failed_owner_node_id,
            "root failover failed owner node id",
        )?;
        validate_logical_id(
            &request.candidate_owner_node_id,
            "root failover candidate owner node id",
        )?;
        if request.observed_owner_fence == 0 {
            bail!("CoreStore root failover observed owner fence must be nonzero");
        }

        if request.observed_generation == 0 {
            bail!("CoreStore root failover observed generation must be nonzero");
        }
        validate_hash(
            &request.observed_root_hash,
            "root failover observed root hash",
        )?;
        validate_hash(
            &request.register_cohort_hash,
            "root failover register cohort hash",
        )?;
        let observed_anchor = decode_root_anchor_record(&request.observed_root_anchor_record)?;
        if observed_anchor.root_key_hash != request.root_key_hash
            || observed_anchor.root_generation != request.observed_generation
            || hash_root_anchor_record(&observed_anchor)? != request.observed_root_hash
            || observed_anchor.publisher_node_id != request.failed_owner_node_id
            || observed_anchor.partition_owner_fence != request.observed_owner_fence
        {
            bail!("CoreStore root failover observation is stale");
        }
        let current = self
            .read_internal_root_anchor_by_hash(&request.root_key_hash, request.observed_generation)
            .await?;
        if current.generation != request.observed_generation
            || current.root_anchor_hash != request.observed_root_hash
        {
            bail!("CoreStore root failover observation does not match the committed root");
        }
        let profile = self.default_coremeta_quorum_profile()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let cohort_node_ids = replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        let cohort_hash = root_register_cohort_hash(
            &request.root_key_hash,
            request.observed_generation,
            &cohort_node_ids,
        );
        if cohort_hash != request.register_cohort_hash {
            bail!("CoreStore root failover register cohort changed");
        }
        let expected_candidate = failover_candidate(&request.failed_owner_node_id, &replicas)
            .ok_or_else(|| anyhow!("CoreStore root failover has no eligible owner candidate"))?;
        if expected_candidate != request.candidate_owner_node_id {
            bail!(
                "CoreStore root failover candidate mismatch: expected {expected_candidate}, got {}",
                request.candidate_owner_node_id
            );
        }

        let synthetic_owner_retired =
            if mesh_lifecycle::is_synthetic_control_node_id(&observed_anchor.publisher_node_id) {
                let canonical_activation =
                    mesh_lifecycle::canonical_topology_activation_with_core_store(self)?;
                synthetic_control_owner_retired_by_activation(
                    canonical_activation
                        .as_ref()
                        .map(|activation| activation.activated_at_unix_nanos),
                    &observed_anchor.publisher_node_id,
                    observed_anchor.created_at_unix_nanos,
                )
            } else {
                false
            };
        let now = unix_timestamp_nanos();
        let (vote_evidence, vote_at) = if synthetic_owner_retired {
            (
                RootFailoverVoteEvidence::SyntheticOwnerRetiredByCanonicalActivation,
                now,
            )
        } else {
            let failure_scope = RootOwnerFailureScope {
                register_ownership_set_hash: failover_ownership_set_hash(&replicas),
                failed_owner_node_id: request.failed_owner_node_id.clone(),
                candidate_owner_node_id: request.candidate_owner_node_id.clone(),
            };
            let confirmed = self
                .root_owner_failure_tracker
                .lock()
                .await
                .confirmed_evidence(&failure_scope, now);
            let (probe_state, failure_evidence, vote_at) =
                if let Some((evidence, refresh_started)) = confirmed {
                    if refresh_started {
                        let store = self.clone();
                        let refresh_anchor = observed_anchor.clone();
                        let refresh_root_hash = request.observed_root_hash.clone();
                        let refresh_replicas = replicas.clone();
                        let refresh_scope = failure_scope.clone();
                        tokio::spawn(async move {
                            let probe_state = store
                                .probe_root_owner(
                                    &refresh_anchor,
                                    &refresh_root_hash,
                                    &refresh_replicas,
                                )
                                .await;
                            store
                                .root_owner_failure_tracker
                                .lock()
                                .await
                                .finish_refresh(refresh_scope, probe_state, unix_timestamp_nanos());
                        });
                    }
                    (OwnerProbeState::Failed, evidence, now)
                } else {
                    let probe_state = self
                        .probe_root_owner(&observed_anchor, &request.observed_root_hash, &replicas)
                        .await;
                    let observed_at = unix_timestamp_nanos();
                    let evidence = self.root_owner_failure_tracker.lock().await.observe(
                        failure_scope,
                        probe_state,
                        observed_at,
                    );
                    (probe_state, evidence, observed_at)
                };
            (
                RootFailoverVoteEvidence::OwnerProbe {
                    probe_state,
                    failure_evidence,
                },
                vote_at,
            )
        };
        let vote_key = root_failover_vote_key(
            &request.root_key_hash,
            &request.observed_root_hash,
            request.observed_generation.saturating_add(1),
            &self.node_identity.node_id,
        );
        let _guard = self
            .acquire_named_lock("root-failover-vote", &hex::encode(&vote_key))
            .await?;
        let existing = self.read_root_failover_vote(&vote_key)?;
        let mut vote = next_failover_vote(
            existing.as_ref(),
            RootFailoverVoteScope {
                root_key_hash: &request.root_key_hash,
                current_generation: request.observed_generation,
                current_root_hash: &request.observed_root_hash,
                register_cohort_hash: &cohort_hash,
                failed_owner_node_id: &request.failed_owner_node_id,
                candidate_owner_node_id: &request.candidate_owner_node_id,
                previous_owner_fence: request.observed_owner_fence,
                voter_node_id: &self.node_identity.node_id,
            },
            vote_evidence,
            vote_at,
        )?;
        if vote.decision == "grant" && vote.voter_signature.is_empty() {
            vote.signed_payload_hash = root_failover_vote_payload_hash(&vote);
            vote.voter_signature = self.sign_internal_core_receipt(&vote.signed_payload_hash)?;
        }
        self.write_root_failover_vote(&vote_key, &vote)?;
        tracing::info!(
            root_key_hash = %vote.root_key_hash,
            current_generation = vote.current_generation,
            current_root_hash = %vote.current_root_hash,
            voter_node_id = %vote.voter_node_id,
            candidate_owner_node_id = %vote.proposed_owner_node_id,
            failed_probe_count = vote.failed_probe_count,
            first_failed_probe_unix_nanos = vote.first_failed_probe_unix_nanos,
            last_probe_unix_nanos = vote.last_probe_unix_nanos,
            decision = %vote.decision,
            reason_code = %vote.reason_code,
            "CoreStore recorded root owner failover vote"
        );
        if vote.decision != "grant" {
            return Ok(None);
        }
        Ok(Some(failover_vote_receipt(&vote)))
    }

    pub(crate) fn validate_root_owner_publication(
        &self,
        source_node_id: &str,
        new_anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        if source_node_id != new_anchor.publisher_node_id {
            bail!("CoreStore root publication source is not the declared owner");
        }
        let Some(current) = self.read_current_root_anchor_unverified(&new_anchor.root_key_hash)?
        else {
            return validate_initial_owner_transition(new_anchor);
        };
        if current.root_generation == 0 {
            return validate_initial_owner_transition(new_anchor);
        }
        if current.root_generation == new_anchor.root_generation {
            if hash_root_anchor_record(&current)? == hash_root_anchor_record(new_anchor)? {
                // Local R3 control replicas can share one physical store. Once
                // the first replica applies the CAS, the remaining replicas
                // observe the committed anchor and must treat its exact replay
                // as the same successful fenced publication.
                return Ok(());
            }
            bail!("CoreStore root owner transition conflicts at the current generation");
        }
        if current.root_generation.saturating_add(1) != new_anchor.root_generation {
            if current.root_generation < new_anchor.root_generation
                && self.coremeta_distributed_recovery_required()
            {
                // This register replica missed one or more committed
                // generations. It must stop serving the public plane until
                // anti-entropy proves and installs the missing history.
                self.mark_coremeta_recovery_unready();
            }
            bail!(
                "CoreStore root owner transition generation mismatch: root={} current={} proposed={}",
                new_anchor.root_key_hash,
                current.root_generation,
                new_anchor.root_generation
            );
        }
        let vote = self.read_root_failover_vote_for_anchor(&current, new_anchor.root_generation)?;
        if new_anchor.publisher_node_id == current.publisher_node_id {
            if new_anchor.publisher_epoch != current.publisher_epoch
                || new_anchor.partition_owner_fence != current.partition_owner_fence
            {
                bail!("CoreStore root owner changed terms without failover");
            }
            if vote.as_ref().is_some_and(|vote| vote.decision == "grant") {
                bail!("CoreStore root publication rejected stale owner fence");
            }
            return Ok(());
        }
        let vote = vote.ok_or_else(|| {
            anyhow!("CoreStore root owner transition lacks this register replica's grant")
        })?;
        if vote.decision != "grant"
            || vote.proposed_owner_node_id != new_anchor.publisher_node_id
            || vote.previous_owner_fence != current.partition_owner_fence
            || vote.proposed_owner_fence != new_anchor.partition_owner_fence
            || new_anchor.partition_owner_fence != current.partition_owner_fence.saturating_add(1)
            || new_anchor.publisher_epoch != current.publisher_epoch
        {
            bail!("CoreStore root owner transition does not match granted failover vote");
        }
        Ok(())
    }

    async fn collect_root_failover_certificate(
        &self,
        current: &CoreRootAnchorRecord,
    ) -> Result<()> {
        let profile = self.default_coremeta_quorum_profile()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let candidate = failover_candidate(&current.publisher_node_id, &replicas)
            .ok_or_else(|| anyhow!("CoreStore root failover has no eligible owner candidate"))?;
        if candidate != self.node_identity.node_id {
            bail!(
                "CoreStore root {} failover candidate is {candidate}; this node is {}",
                current.root_key_hash,
                self.node_identity.node_id
            );
        }
        let current_hash = hash_root_anchor_record(current)?;
        let cohort_node_ids = replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        let cohort_hash = root_register_cohort_hash(
            &current.root_key_hash,
            current.root_generation,
            &cohort_node_ids,
        );
        let voters = failover_voter_replicas(&current.publisher_node_id, &replicas);
        if voters.len() < profile.prepare_quorum {
            bail!(
                "CoreStore root {} failover has no surviving register quorum: required {}, available {}",
                current.root_key_hash,
                profile.prepare_quorum,
                voters.len()
            );
        }
        let mut grants = BTreeMap::<String, FailoverVoteReceipt>::new();
        let mut vote_errors = Vec::new();
        for round in 0..ROOT_FAILOVER_VOTE_ROUNDS {
            let mut pending = FuturesUnordered::new();
            for replica in &voters {
                let node_id = replica.node_id.clone();
                let current_hash = current_hash.clone();
                let cohort_hash = cohort_hash.clone();
                pending.push(async move {
                    (
                        node_id,
                        self.request_root_failover_vote(
                            replica,
                            current,
                            &current_hash,
                            &cohort_hash,
                        )
                        .await,
                    )
                });
            }
            while let Some((node_id, result)) = pending.next().await {
                match result.and_then(|receipt| {
                    self.validate_failover_vote_receipt(
                        current,
                        &current_hash,
                        &cohort_hash,
                        &receipt,
                    )?;
                    Ok(receipt)
                }) {
                    Ok(receipt) => {
                        let vote = decode_failover_vote_receipt(&receipt)?;
                        grants.insert(vote.voter_node_id, receipt);
                        if grants.len() >= profile.prepare_quorum {
                            return self.persist_root_failover_certificate(
                                current,
                                &current_hash,
                                &cohort_hash,
                                grants.into_values().collect(),
                            );
                        }
                    }
                    Err(error) => vote_errors
                        .push(format!("round {} replica {node_id}: {error:#}", round + 1)),
                }
            }
            if round + 1 < ROOT_FAILOVER_VOTE_ROUNDS {
                tokio::time::sleep(ROOT_FAILOVER_TIMEOUT / 2).await;
            }
        }
        bail!(
            "CoreStore root {} failover did not reach register quorum: required {}, received {}: {}",
            current.root_key_hash,
            profile.prepare_quorum,
            grants.len(),
            vote_errors.join("; ")
        )
    }

    async fn request_root_failover_vote(
        &self,
        replica: &LocalShardPlacement,
        current: &CoreRootAnchorRecord,
        current_hash: &str,
        cohort_hash: &str,
    ) -> Result<FailoverVoteReceipt> {
        let current_record = encode_root_anchor_record(current)?;
        let request = VoteFailoverRequest {
            header: Some(self.internal_request_header("root.vote_failover")?),
            root_key_hash: current.root_key_hash.clone(),
            failed_owner_node_id: current.publisher_node_id.clone(),
            observed_owner_fence: current.partition_owner_fence,
            candidate_owner_node_id: self.node_identity.node_id.clone(),
            observed_generation: current.root_generation,
            observed_root_hash: current_hash.to_string(),
            observed_root_anchor_record: current_record,
            register_cohort_hash: cohort_hash.to_string(),
        };
        if replica.is_local || replica.node_id == self.node_identity.node_id {
            return self
                .vote_root_owner_failover(&request)
                .await?
                .ok_or_else(|| anyhow!("CoreStore local root failover vote is pending"));
        }
        let bearer = self
            .node_identity
            .internal_bearer_token
            .as_deref()
            .ok_or_else(|| anyhow!("CoreStore root failover requires an internal bearer token"))?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let public_api_addr = replica.public_api_addr.clone();
        let timeout_addr = public_api_addr.clone();
        let operation =
            self.internal_grpc_request(&public_api_addr, "root.vote_failover", move |channel| {
                let mut client = RootRegisterInternalClient::new(channel);
                let mut request = tonic::Request::new(request.clone());
                request
                    .metadata_mut()
                    .insert("authorization", authorization.clone());
                async move {
                    client
                        .vote_failover(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            });
        tokio::time::timeout(ROOT_FAILOVER_VOTE_REQUEST_TIMEOUT, operation)
            .await
            .map_err(|_| {
                anyhow!(
                    "CoreStore root failover vote request to {timeout_addr} exceeded {:?}",
                    ROOT_FAILOVER_VOTE_REQUEST_TIMEOUT
                )
            })?
    }

    async fn probe_root_owner(
        &self,
        current: &CoreRootAnchorRecord,
        current_hash: &str,
        replicas: &[LocalShardPlacement],
    ) -> OwnerProbeState {
        if current.publisher_node_id == self.node_identity.node_id {
            return OwnerProbeState::Healthy;
        }
        let Some(owner) = replicas
            .iter()
            .find(|replica| replica.node_id == current.publisher_node_id)
        else {
            return OwnerProbeState::Failed;
        };
        let Some(bearer) = self.node_identity.internal_bearer_token.as_deref() else {
            return OwnerProbeState::Failed;
        };
        let Ok(authorization) = MetadataValue::try_from(format!("Bearer {bearer}")) else {
            return OwnerProbeState::Failed;
        };
        let request = ReadRootRequest {
            header: self.internal_request_header("root.read").ok(),
            root_key_hash: current.root_key_hash.clone(),
            min_generation: current.root_generation,
            exact_generation: None,
            committed_cache: false,
        };
        // Failure detection must be bounded independently of ordinary internal
        // request retries. Waiting through the generic retry budget for every
        // probe would make a three-probe failover take minutes and prevent the
        // register quorum from replacing an unreachable owner.
        let result = tokio::time::timeout(
            ROOT_FAILOVER_PROBE_REQUEST_TIMEOUT,
            self.internal_grpc_request(
                &owner.public_api_addr,
                "root.failover_probe",
                move |channel| {
                    let mut client = RootRegisterInternalClient::new(channel);
                    let mut request = tonic::Request::new(request.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .read_root(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            ),
        )
        .await;
        match result {
            Ok(Ok(read))
                if read.generation == current.root_generation
                    && read.root_anchor_hash == current_hash =>
            {
                OwnerProbeState::Healthy
            }
            _ => OwnerProbeState::Failed,
        }
    }

    fn validate_failover_grant_evidence(
        &self,
        current: &CoreRootAnchorRecord,
        vote: &RootFailoverVoteRowProto,
    ) -> Result<()> {
        match vote.reason_code.as_str() {
            ROOT_FAILOVER_REASON_OWNER_UNREACHABLE
                if confirmed_owner_unreachable_evidence(vote) =>
            {
                Ok(())
            }
            ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED => {
                let activation =
                    mesh_lifecycle::canonical_topology_activation_with_core_store(self)?;
                if synthetic_control_owner_retired_by_activation(
                    activation
                        .as_ref()
                        .map(|activation| activation.activated_at_unix_nanos),
                    &current.publisher_node_id,
                    current.created_at_unix_nanos,
                ) {
                    Ok(())
                } else {
                    bail!(
                        "CoreStore root failover verifier does not establish synthetic owner retirement"
                    )
                }
            }
            _ => bail!("CoreStore root failover grant evidence is invalid"),
        }
    }

    fn validate_failover_vote_receipt(
        &self,
        current: &CoreRootAnchorRecord,
        current_hash: &str,
        cohort_hash: &str,
        receipt: &FailoverVoteReceipt,
    ) -> Result<()> {
        let vote = decode_failover_vote_receipt(receipt)?;
        validate_root_failover_vote(&vote)?;
        if vote.root_key_hash != current.root_key_hash
            || vote.current_generation != current.root_generation
            || vote.current_root_hash != current_hash
            || vote.target_generation != current.root_generation.saturating_add(1)
            || vote.register_cohort_hash != cohort_hash
            || vote.observed_owner_node_id != current.publisher_node_id
            || vote.proposed_owner_node_id != self.node_identity.node_id
            || vote.previous_owner_fence != current.partition_owner_fence
            || vote.proposed_owner_fence != current.partition_owner_fence.saturating_add(1)
            || vote.decision != "grant"
        {
            bail!("CoreStore root failover vote receipt scope mismatch");
        }
        self.validate_failover_grant_evidence(current, &vote)?;
        self.verify_internal_core_receipt_signature(
            &vote.voter_node_id,
            &vote.signed_payload_hash,
            &vote.voter_signature,
        )
    }

    fn persist_root_failover_certificate(
        &self,
        current: &CoreRootAnchorRecord,
        current_hash: &str,
        cohort_hash: &str,
        receipts: Vec<FailoverVoteReceipt>,
    ) -> Result<()> {
        let mut keyed_receipts = receipts
            .into_iter()
            .map(|receipt| {
                let voter_node_id = decode_failover_vote_receipt(&receipt)?.voter_node_id;
                Ok((voter_node_id, receipt))
            })
            .collect::<Result<Vec<_>>>()?;
        keyed_receipts.sort_by(|left, right| left.0.cmp(&right.0));
        keyed_receipts.dedup_by(|left, right| left.0 == right.0);
        let receipts = keyed_receipts
            .into_iter()
            .map(|(_, receipt)| receipt)
            .collect::<Vec<_>>();
        let profile = self.default_coremeta_quorum_profile()?;
        if receipts.len() < profile.prepare_quorum {
            bail!("CoreStore root failover certificate lacks vote quorum");
        }
        let created_at = unix_timestamp_nanos();
        let target_generation = current.root_generation.saturating_add(1);
        let key = root_failover_certificate_key(
            &current.root_key_hash,
            current_hash,
            target_generation,
            &self.node_identity.node_id,
        );
        let common = local_failover_common(
            &self.node_identity,
            format!("root-failover-certificate-{current_hash}"),
            created_at,
        );
        let mut certificate = RootFailoverCertificateRowProto {
            common: Some(common),
            schema: ROOT_FAILOVER_CERTIFICATE_SCHEMA.to_string(),
            root_key_hash: current.root_key_hash.clone(),
            current_generation: current.root_generation,
            current_root_hash: current_hash.to_string(),
            target_generation,
            register_cohort_hash: cohort_hash.to_string(),
            previous_owner_node_id: current.publisher_node_id.clone(),
            new_owner_node_id: self.node_identity.node_id.clone(),
            previous_owner_fence: current.partition_owner_fence,
            new_owner_fence: current.partition_owner_fence.saturating_add(1),
            vote_receipts: receipts.iter().map(encode_deterministic_proto).collect(),
            certificate_hash: String::new(),
            created_at_unix_nanos: created_at,
        };
        certificate.certificate_hash = root_failover_certificate_hash(&certificate);
        let payload = encode_deterministic_proto(&certificate);
        validate_coremeta_operation_payload(
            CF_LEASES_FENCES,
            TABLE_ROOT_FAILOVER_CERTIFICATE_ROW,
            &key,
            &payload,
        )?;
        self.meta.write_local_committed_batch(&[CoreMetaBatchOp {
            cf: CF_LEASES_FENCES,
            table_id: TABLE_ROOT_FAILOVER_CERTIFICATE_ROW,
            tuple_key: &key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
    }

    fn read_root_failover_certificate(
        &self,
        current: &CoreRootAnchorRecord,
        target_generation: u64,
        candidate_node_id: &str,
    ) -> Result<Option<RootFailoverCertificateRowProto>> {
        let current_hash = hash_root_anchor_record(current)?;
        let key = root_failover_certificate_key(
            &current.root_key_hash,
            &current_hash,
            target_generation,
            candidate_node_id,
        );
        let Some(payload) =
            self.meta
                .get(CF_LEASES_FENCES, TABLE_ROOT_FAILOVER_CERTIFICATE_ROW, &key)?
        else {
            return Ok(None);
        };
        let certificate = decode_deterministic_proto::<RootFailoverCertificateRowProto>(
            &payload,
            "root failover certificate",
        )?;
        validate_root_failover_certificate(
            &certificate,
            current,
            &current_hash,
            target_generation,
            candidate_node_id,
        )?;
        let profile = self.default_coremeta_quorum_profile()?;
        let mut voters = BTreeSet::new();
        for encoded in &certificate.vote_receipts {
            let receipt = decode_deterministic_proto::<FailoverVoteReceipt>(
                encoded,
                "root failover vote receipt",
            )?;
            self.validate_failover_vote_receipt(
                current,
                &current_hash,
                &certificate.register_cohort_hash,
                &receipt,
            )?;
            voters.insert(decode_failover_vote_receipt(&receipt)?.voter_node_id);
        }
        if voters.len() < profile.prepare_quorum {
            bail!("CoreStore root failover certificate has duplicate voters");
        }
        Ok(Some(certificate))
    }

    fn read_root_failover_vote(&self, key: &[u8]) -> Result<Option<RootFailoverVoteRowProto>> {
        let Some(payload) = self
            .meta
            .get(CF_LEASES_FENCES, TABLE_ROOT_FAILOVER_VOTE_ROW, key)?
        else {
            return Ok(None);
        };
        let vote =
            decode_deterministic_proto::<RootFailoverVoteRowProto>(&payload, "root failover vote")?;
        validate_root_failover_vote(&vote)?;
        if vote.decision == "grant" {
            self.verify_internal_core_receipt_signature(
                &vote.voter_node_id,
                &vote.signed_payload_hash,
                &vote.voter_signature,
            )?;
        }
        Ok(Some(vote))
    }

    fn read_root_failover_vote_for_anchor(
        &self,
        current: &CoreRootAnchorRecord,
        target_generation: u64,
    ) -> Result<Option<RootFailoverVoteRowProto>> {
        let current_hash = hash_root_anchor_record(current)?;
        let key = root_failover_vote_key(
            &current.root_key_hash,
            &current_hash,
            target_generation,
            &self.node_identity.node_id,
        );
        self.read_root_failover_vote(&key)
    }

    fn write_root_failover_vote(&self, key: &[u8], vote: &RootFailoverVoteRowProto) -> Result<()> {
        validate_root_failover_vote(vote)?;
        let payload = encode_deterministic_proto(vote);
        validate_coremeta_operation_payload(
            CF_LEASES_FENCES,
            TABLE_ROOT_FAILOVER_VOTE_ROW,
            key,
            &payload,
        )?;
        self.meta.write_local_committed_batch(&[CoreMetaBatchOp {
            cf: CF_LEASES_FENCES,
            table_id: TABLE_ROOT_FAILOVER_VOTE_ROW,
            tuple_key: key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
    }

    fn read_current_root_anchor_unverified(
        &self,
        root_key_hash: &str,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let Some(payload) = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_hash_key(root_key_hash),
        )?
        else {
            return Ok(None);
        };
        let anchor = decode_root_cache_row(&payload)?;
        if anchor.root_key_hash != root_key_hash {
            bail!("CoreStore root owner state key mismatch");
        }
        Ok(Some(anchor))
    }
}

struct RootFailoverVoteScope<'a> {
    root_key_hash: &'a str,
    current_generation: u64,
    current_root_hash: &'a str,
    register_cohort_hash: &'a str,
    failed_owner_node_id: &'a str,
    candidate_owner_node_id: &'a str,
    previous_owner_fence: u64,
    voter_node_id: &'a str,
}

fn next_failover_vote(
    existing: Option<&RootFailoverVoteRowProto>,
    scope: RootFailoverVoteScope<'_>,
    evidence: RootFailoverVoteEvidence,
    now: u64,
) -> Result<RootFailoverVoteRowProto> {
    if let Some(existing) = existing {
        validate_vote_scope(existing, &scope)?;
        if existing.decision == "grant" {
            return Ok(existing.clone());
        }
    }
    let (
        failed_probe_count,
        first_failed_probe_unix_nanos,
        last_probe_unix_nanos,
        decision,
        reason_code,
    ) = match evidence {
        RootFailoverVoteEvidence::OwnerProbe {
            probe_state,
            failure_evidence,
        } => match probe_state {
            OwnerProbeState::Healthy => (
                0,
                0,
                failure_evidence.last_probe_unix_nanos,
                "reject",
                ROOT_FAILOVER_REASON_OWNER_HEALTHY,
            ),
            OwnerProbeState::Failed => {
                let count = failure_evidence.failed_probe_count;
                let first = failure_evidence.first_failed_probe_unix_nanos;
                let elapsed = failure_evidence.last_probe_unix_nanos.saturating_sub(first);
                let decision = if count >= ROOT_FAILOVER_PROBE_COUNT
                    && elapsed >= ROOT_FAILOVER_TIMEOUT.as_nanos() as u64
                {
                    "grant"
                } else {
                    "suspect"
                };
                (
                    count,
                    first,
                    failure_evidence.last_probe_unix_nanos,
                    decision,
                    ROOT_FAILOVER_REASON_OWNER_UNREACHABLE,
                )
            }
        },
        RootFailoverVoteEvidence::SyntheticOwnerRetiredByCanonicalActivation => (
            0,
            0,
            0,
            "grant",
            ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED,
        ),
    };
    let proposed_owner_fence = scope.previous_owner_fence.saturating_add(1);
    let evidence_hash = failover_evidence_hash(
        &scope,
        reason_code,
        failed_probe_count,
        first_failed_probe_unix_nanos,
        last_probe_unix_nanos,
    );
    Ok(RootFailoverVoteRowProto {
        common: Some(local_failover_common_for_voter(
            scope.voter_node_id,
            format!("root-failover-vote-{}", scope.current_root_hash),
            now,
        )),
        schema: ROOT_FAILOVER_VOTE_SCHEMA.to_string(),
        root_key_hash: scope.root_key_hash.to_string(),
        current_generation: scope.current_generation,
        current_root_hash: scope.current_root_hash.to_string(),
        target_generation: scope.current_generation.saturating_add(1),
        register_cohort_hash: scope.register_cohort_hash.to_string(),
        observed_owner_node_id: scope.failed_owner_node_id.to_string(),
        proposed_owner_node_id: scope.candidate_owner_node_id.to_string(),
        previous_owner_fence: scope.previous_owner_fence,
        proposed_owner_fence,
        evidence_hash,
        voter_node_id: scope.voter_node_id.to_string(),
        decision: decision.to_string(),
        reason_code: reason_code.to_string(),
        failed_probe_count,
        first_failed_probe_unix_nanos,
        last_probe_unix_nanos,
        signed_payload_hash: String::new(),
        voter_signature: Vec::new(),
    })
}

fn validate_vote_scope(
    vote: &RootFailoverVoteRowProto,
    scope: &RootFailoverVoteScope<'_>,
) -> Result<()> {
    if vote.root_key_hash != scope.root_key_hash
        || vote.current_generation != scope.current_generation
        || vote.current_root_hash != scope.current_root_hash
        || vote.target_generation != scope.current_generation.saturating_add(1)
        || vote.register_cohort_hash != scope.register_cohort_hash
        || vote.observed_owner_node_id != scope.failed_owner_node_id
        || vote.proposed_owner_node_id != scope.candidate_owner_node_id
        || vote.previous_owner_fence != scope.previous_owner_fence
        || vote.voter_node_id != scope.voter_node_id
    {
        bail!("CoreStore root failover vote is in doubt");
    }
    Ok(())
}

fn confirmed_owner_unreachable_evidence(vote: &RootFailoverVoteRowProto) -> bool {
    vote.failed_probe_count >= ROOT_FAILOVER_PROBE_COUNT
        && vote.first_failed_probe_unix_nanos > 0
        && vote.last_probe_unix_nanos >= vote.first_failed_probe_unix_nanos
        && vote
            .last_probe_unix_nanos
            .saturating_sub(vote.first_failed_probe_unix_nanos)
            >= ROOT_FAILOVER_TIMEOUT.as_nanos() as u64
}

fn validate_root_failover_vote_reason(vote: &RootFailoverVoteRowProto) -> Result<()> {
    let reason_is_valid = match vote.reason_code.as_str() {
        ROOT_FAILOVER_REASON_OWNER_HEALTHY => {
            vote.decision == "reject"
                && vote.failed_probe_count == 0
                && vote.first_failed_probe_unix_nanos == 0
        }
        ROOT_FAILOVER_REASON_OWNER_UNREACHABLE => {
            let has_failed_probe = vote.failed_probe_count > 0
                && vote.first_failed_probe_unix_nanos > 0
                && vote.last_probe_unix_nanos >= vote.first_failed_probe_unix_nanos;
            has_failed_probe
                && match vote.decision.as_str() {
                    "suspect" => !confirmed_owner_unreachable_evidence(vote),
                    "grant" => confirmed_owner_unreachable_evidence(vote),
                    _ => false,
                }
        }
        ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED => {
            vote.decision == "grant"
                && mesh_lifecycle::is_synthetic_control_node_id(&vote.observed_owner_node_id)
                && vote.failed_probe_count == 0
                && vote.first_failed_probe_unix_nanos == 0
                && vote.last_probe_unix_nanos == 0
        }
        _ => false,
    };
    if !reason_is_valid {
        bail!("CoreStore root failover vote reason and evidence are invalid");
    }
    Ok(())
}

fn validate_root_failover_vote(vote: &RootFailoverVoteRowProto) -> Result<()> {
    if vote.schema != ROOT_FAILOVER_VOTE_SCHEMA {
        bail!("CoreStore root failover vote schema mismatch");
    }
    validate_hash(&vote.root_key_hash, "root failover vote root key hash")?;
    validate_hash(
        &vote.current_root_hash,
        "root failover vote current root hash",
    )?;
    validate_hash(
        &vote.register_cohort_hash,
        "root failover vote register cohort hash",
    )?;
    validate_hash(&vote.evidence_hash, "root failover vote evidence hash")?;
    if vote.current_generation == 0
        || vote.target_generation != vote.current_generation.saturating_add(1)
        || vote.previous_owner_fence == 0
        || vote.proposed_owner_fence != vote.previous_owner_fence.saturating_add(1)
        || !matches!(vote.decision.as_str(), "suspect" | "reject" | "grant")
    {
        bail!("CoreStore root failover vote fields are invalid");
    }
    validate_root_failover_vote_reason(vote)?;
    let evidence_scope = RootFailoverVoteScope {
        root_key_hash: &vote.root_key_hash,
        current_generation: vote.current_generation,
        current_root_hash: &vote.current_root_hash,
        register_cohort_hash: &vote.register_cohort_hash,
        failed_owner_node_id: &vote.observed_owner_node_id,
        candidate_owner_node_id: &vote.proposed_owner_node_id,
        previous_owner_fence: vote.previous_owner_fence,
        voter_node_id: &vote.voter_node_id,
    };
    if vote.evidence_hash
        != failover_evidence_hash(
            &evidence_scope,
            &vote.reason_code,
            vote.failed_probe_count,
            vote.first_failed_probe_unix_nanos,
            vote.last_probe_unix_nanos,
        )
    {
        bail!("CoreStore root failover vote evidence hash mismatch");
    }
    if vote.decision == "grant" {
        validate_hash(
            &vote.signed_payload_hash,
            "root failover vote signed payload hash",
        )?;
        if vote.voter_signature.is_empty()
            || vote.signed_payload_hash != root_failover_vote_payload_hash(vote)
        {
            bail!("CoreStore root failover grant signature fields are invalid");
        }
    } else if !vote.signed_payload_hash.is_empty() || !vote.voter_signature.is_empty() {
        bail!("CoreStore non-grant failover vote must not be signed");
    }
    Ok(())
}

fn validate_root_failover_certificate(
    certificate: &RootFailoverCertificateRowProto,
    current: &CoreRootAnchorRecord,
    current_hash: &str,
    target_generation: u64,
    candidate_node_id: &str,
) -> Result<()> {
    if certificate.schema != ROOT_FAILOVER_CERTIFICATE_SCHEMA
        || certificate.root_key_hash != current.root_key_hash
        || certificate.current_generation != current.root_generation
        || certificate.current_root_hash != current_hash
        || certificate.target_generation != target_generation
        || certificate.previous_owner_node_id != current.publisher_node_id
        || certificate.new_owner_node_id != candidate_node_id
        || certificate.previous_owner_fence != current.partition_owner_fence
        || certificate.new_owner_fence != current.partition_owner_fence.saturating_add(1)
        || certificate.vote_receipts.len() < 2
        || certificate.certificate_hash != root_failover_certificate_hash(certificate)
    {
        bail!("CoreStore root failover certificate scope mismatch");
    }
    validate_hash(
        &certificate.register_cohort_hash,
        "root failover certificate cohort hash",
    )?;
    validate_hash(
        &certificate.certificate_hash,
        "root failover certificate hash",
    )
}

fn validate_initial_owner_transition(anchor: &CoreRootAnchorRecord) -> Result<()> {
    if anchor.publisher_epoch != LOCAL_PLACEMENT_EPOCH
        || anchor.partition_owner_fence != LOCAL_PLACEMENT_EPOCH
    {
        bail!("CoreStore initial root owner terms must start at epoch and fence one");
    }
    Ok(())
}

fn initial_owner_terms(publisher_node_id: &str) -> Result<RootOwnerTerms> {
    validate_logical_id(publisher_node_id, "CoreMeta root publisher node id")?;
    Ok(RootOwnerTerms {
        owner_node_id: publisher_node_id.to_string(),
        owner_epoch: LOCAL_PLACEMENT_EPOCH,
        owner_fence: LOCAL_PLACEMENT_EPOCH,
    })
}

fn synthetic_control_owner_retired_by_activation(
    canonical_activation_unix_nanos: Option<u64>,
    observed_owner_node_id: &str,
    observed_anchor_created_at_unix_nanos: u64,
) -> bool {
    mesh_lifecycle::is_synthetic_control_node_id(observed_owner_node_id)
        && canonical_activation_unix_nanos
            .is_some_and(|activated_at| observed_anchor_created_at_unix_nanos < activated_at)
}

fn canonical_settlement_owner_from_replicas<'a>(
    current_owner_node_id: &str,
    replicas: &'a [LocalShardPlacement],
) -> Option<&'a str> {
    replicas
        .iter()
        .find(|replica| replica.node_id == current_owner_node_id)
        .map(|replica| replica.node_id.as_str())
        .or_else(|| failover_candidate(current_owner_node_id, replicas))
}

fn failover_candidate<'a>(
    failed_owner_node_id: &str,
    replicas: &'a [LocalShardPlacement],
) -> Option<&'a str> {
    let cohort_hash = failover_ownership_set_hash(replicas);
    replicas
        .iter()
        .filter(|replica| replica.node_id != failed_owner_node_id)
        .max_by(|left, right| {
            failover_candidate_score(&cohort_hash, failed_owner_node_id, &left.node_id)
                .cmp(&failover_candidate_score(
                    &cohort_hash,
                    failed_owner_node_id,
                    &right.node_id,
                ))
                .then_with(|| left.node_id.cmp(&right.node_id))
        })
        .map(|replica| replica.node_id.as_str())
}

fn failover_voter_replicas<'a>(
    failed_owner_node_id: &str,
    replicas: &'a [LocalShardPlacement],
) -> Vec<&'a LocalShardPlacement> {
    replicas
        .iter()
        .filter(|replica| replica.node_id != failed_owner_node_id)
        .collect()
}

fn failover_candidate_score(
    register_cohort_hash: &str,
    failed_owner_node_id: &str,
    node_id: &str,
) -> [u8; 32] {
    hash32(
        format!(
            "anvil.root_owner_failover.candidate.v2\0{register_cohort_hash}\0{failed_owner_node_id}\0{node_id}"
        )
        .as_bytes(),
    )
}

fn root_write_target_node<'a>(
    current_owner_node_id: &'a str,
    probe_state: OwnerProbeState,
    replicas: &'a [LocalShardPlacement],
) -> Option<&'a str> {
    match probe_state {
        OwnerProbeState::Healthy => Some(current_owner_node_id),
        OwnerProbeState::Failed => failover_candidate(current_owner_node_id, replicas),
    }
}

fn remote_write_route(
    target_node_id: &str,
    replicas: &[LocalShardPlacement],
) -> Result<CoreMetaWriteRoute> {
    let target = replicas
        .iter()
        .find(|replica| replica.node_id == target_node_id)
        .ok_or_else(|| anyhow!("CoreStore root write target is absent from its register cohort"))?;
    if target.public_api_addr.trim().is_empty() {
        bail!("CoreStore root write target has no public API address");
    }
    Ok(CoreMetaWriteRoute::Remote(CoreMetaPeerTarget {
        node_id: target.node_id.clone(),
        public_api_addr: target.public_api_addr.clone(),
    }))
}

fn failover_ownership_set_hash(replicas: &[LocalShardPlacement]) -> String {
    let mut nodes = replicas
        .iter()
        .map(|replica| replica.node_id.as_str())
        .collect::<Vec<_>>();
    nodes.sort_unstable();
    format!(
        "sha256:{}",
        sha256_hex(format!("anvil.root_register.cohort.v1\0{}", nodes.join("\0")).as_bytes())
    )
}

fn failover_evidence_hash(
    scope: &RootFailoverVoteScope<'_>,
    reason_code: &str,
    failed_probe_count: u32,
    first_failed_probe_unix_nanos: u64,
    last_probe_unix_nanos: u64,
) -> String {
    let body = format!(
        "anvil.root_owner_failover.evidence.v2\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        scope.root_key_hash,
        scope.current_generation,
        scope.current_root_hash,
        scope.register_cohort_hash,
        scope.failed_owner_node_id,
        scope.candidate_owner_node_id,
        reason_code,
        failed_probe_count,
        first_failed_probe_unix_nanos,
        last_probe_unix_nanos,
    );
    format!("sha256:{}", sha256_hex(body.as_bytes()))
}

fn root_failover_vote_payload_hash(vote: &RootFailoverVoteRowProto) -> String {
    let mut unsigned = vote.clone();
    unsigned.signed_payload_hash.clear();
    unsigned.voter_signature.clear();
    domain_hash_bytes(
        "anvil.root_owner_failover.vote.v1",
        &encode_deterministic_proto(&unsigned),
    )
}

fn root_failover_certificate_hash(certificate: &RootFailoverCertificateRowProto) -> String {
    let mut unsigned = certificate.clone();
    unsigned.certificate_hash.clear();
    format!(
        "sha256:{}",
        sha256_hex(&encode_deterministic_proto(&unsigned))
    )
}

fn failover_vote_receipt(vote: &RootFailoverVoteRowProto) -> FailoverVoteReceipt {
    FailoverVoteReceipt {
        vote_record: encode_deterministic_proto(vote),
    }
}

fn decode_failover_vote_receipt(receipt: &FailoverVoteReceipt) -> Result<RootFailoverVoteRowProto> {
    if receipt.vote_record.is_empty() {
        bail!("CoreStore root failover vote receipt is empty");
    }
    decode_deterministic_proto(&receipt.vote_record, "root failover vote receipt")
}

fn root_failover_vote_key(
    root_key_hash: &str,
    current_root_hash: &str,
    target_generation: u64,
    voter_node_id: &str,
) -> Vec<u8> {
    meta_tuple_key(&[
        b"root-failover-vote",
        root_key_hash.as_bytes(),
        current_root_hash.as_bytes(),
        &target_generation.to_be_bytes(),
        voter_node_id.as_bytes(),
    ])
}

fn root_failover_certificate_key(
    root_key_hash: &str,
    current_root_hash: &str,
    target_generation: u64,
    candidate_node_id: &str,
) -> Vec<u8> {
    meta_tuple_key(&[
        b"root-failover-certificate",
        root_key_hash.as_bytes(),
        current_root_hash.as_bytes(),
        &target_generation.to_be_bytes(),
        candidate_node_id.as_bytes(),
    ])
}

fn local_failover_common(
    identity: &CoreStoreNodeIdentity,
    transaction_id: String,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    local_failover_common_for_voter(&identity.node_id, transaction_id, created_at_unix_nanos)
}

fn local_failover_common_for_voter(
    voter_node_id: &str,
    transaction_id: String,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("root-register/{voter_node_id}"),
        String::new(),
        0,
        transaction_id,
        created_at_unix_nanos,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(node_id: &str) -> LocalShardPlacement {
        LocalShardPlacement {
            node_id: node_id.to_string(),
            region_id: "r1".to_string(),
            cell_id: format!("cell-{node_id}"),
            failure_domain: format!("cell-{node_id}"),
            region_weight: 100,
            cell_weight: 100,
            public_api_addr: format!("http://{node_id}"),
            is_local: false,
        }
    }

    fn scope<'a>() -> RootFailoverVoteScope<'a> {
        RootFailoverVoteScope {
            root_key_hash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            current_generation: 7,
            current_root_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            register_cohort_hash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            failed_owner_node_id: "node-a",
            candidate_owner_node_id: "node-b",
            previous_owner_fence: 4,
            voter_node_id: "node-c",
        }
    }

    fn owner_failure_scope() -> RootOwnerFailureScope {
        RootOwnerFailureScope {
            register_ownership_set_hash:
                "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    .to_string(),
            failed_owner_node_id: "node-a".to_string(),
            candidate_owner_node_id: "node-b".to_string(),
        }
    }

    fn owner_probe_evidence(
        probe_state: OwnerProbeState,
        failure_evidence: RootOwnerFailureEvidence,
    ) -> RootFailoverVoteEvidence {
        RootFailoverVoteEvidence::OwnerProbe {
            probe_state,
            failure_evidence,
        }
    }

    fn structurally_sign_vote(vote: &mut RootFailoverVoteRowProto) {
        vote.signed_payload_hash = root_failover_vote_payload_hash(vote);
        vote.voter_signature = vec![1];
    }

    #[test]
    fn equal_peers_choose_one_deterministic_root_failover_candidate() {
        let mut replicas = vec![replica("node-c"), replica("node-a"), replica("node-b")];
        let first = failover_candidate("node-a", &replicas).unwrap().to_string();
        replicas.reverse();
        let second = failover_candidate("node-a", &replicas).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, "node-a");
    }

    #[test]
    fn canonical_settlement_retains_cohort_owner_or_uses_rendezvous_successor() {
        let mut replicas = vec![replica("node-c"), replica("node-a"), replica("node-b")];
        assert_eq!(
            canonical_settlement_owner_from_replicas("node-b", &replicas),
            Some("node-b")
        );

        let retired_owner = "local-control-node-1";
        let expected = failover_candidate(retired_owner, &replicas)
            .expect("canonical R3 cohort has a successor")
            .to_string();
        assert_eq!(
            canonical_settlement_owner_from_replicas(retired_owner, &replicas),
            Some(expected.as_str())
        );
        replicas.reverse();
        assert_eq!(
            canonical_settlement_owner_from_replicas(retired_owner, &replicas),
            Some(expected.as_str())
        );
    }

    #[test]
    fn one_failed_owner_has_one_successor_for_every_root() {
        let replicas = vec![replica("node-a"), replica("node-b"), replica("node-c")];
        let successor = failover_candidate("node-a", &replicas).unwrap();
        assert_eq!(
            root_write_target_node("node-a", OwnerProbeState::Failed, &replicas),
            Some(successor)
        );
        assert_eq!(
            root_write_target_node("node-a", OwnerProbeState::Healthy, &replicas),
            Some("node-a")
        );
    }

    #[test]
    fn failed_owner_does_not_block_surviving_register_voters() {
        let replicas = vec![replica("node-a"), replica("node-b"), replica("node-c")];
        let voters = failover_voter_replicas("node-a", &replicas);
        assert_eq!(
            voters
                .iter()
                .map(|replica| replica.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-b", "node-c"]
        );
    }

    #[test]
    fn ordinary_owner_failover_still_requires_three_probes_spanning_timeout() {
        let mut tracker = RootOwnerFailureTracker::default();
        let first_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 10);
        let first = next_failover_vote(
            None,
            scope(),
            owner_probe_evidence(OwnerProbeState::Failed, first_evidence),
            10,
        )
        .unwrap();
        assert_eq!(first.decision, "suspect");
        assert_eq!(first.reason_code, ROOT_FAILOVER_REASON_OWNER_UNREACHABLE);
        let second_at = 10 + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
        let second_evidence =
            tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, second_at);
        let second = next_failover_vote(
            Some(&first),
            scope(),
            owner_probe_evidence(OwnerProbeState::Failed, second_evidence),
            second_at,
        )
        .unwrap();
        assert_eq!(second.decision, "suspect");
        let third_evidence =
            tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, second_at);
        let third = next_failover_vote(
            Some(&second),
            scope(),
            owner_probe_evidence(OwnerProbeState::Failed, third_evidence),
            second_at,
        )
        .unwrap();
        assert_eq!(third.decision, "grant");
        assert_eq!(third.reason_code, ROOT_FAILOVER_REASON_OWNER_UNREACHABLE);

        let mut signed = third.clone();
        structurally_sign_vote(&mut signed);
        validate_root_failover_vote(&signed).unwrap();

        let mut premature = signed;
        premature.last_probe_unix_nanos =
            premature.first_failed_probe_unix_nanos + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64 - 1;
        structurally_sign_vote(&mut premature);
        assert!(validate_root_failover_vote(&premature).is_err());
    }

    #[test]
    fn pre_activation_synthetic_owner_retirement_grants_without_probes() {
        let activated_at = 100;
        assert!(synthetic_control_owner_retired_by_activation(
            Some(activated_at),
            "local-control-node-1",
            activated_at - 1,
        ));
        assert!(!synthetic_control_owner_retired_by_activation(
            None,
            "local-control-node-1",
            activated_at - 1,
        ));
        assert!(!synthetic_control_owner_retired_by_activation(
            Some(activated_at),
            "node-a",
            activated_at - 1,
        ));
        assert!(!synthetic_control_owner_retired_by_activation(
            Some(activated_at),
            "local-control-node-1",
            activated_at,
        ));

        let mut synthetic_scope = scope();
        synthetic_scope.failed_owner_node_id = "local-control-node-1";
        let mut vote = next_failover_vote(
            None,
            synthetic_scope,
            RootFailoverVoteEvidence::SyntheticOwnerRetiredByCanonicalActivation,
            activated_at + 1,
        )
        .unwrap();
        assert_eq!(vote.decision, "grant");
        assert_eq!(
            vote.reason_code,
            ROOT_FAILOVER_REASON_SYNTHETIC_OWNER_RETIRED
        );
        assert_eq!(vote.failed_probe_count, 0);
        assert_eq!(vote.first_failed_probe_unix_nanos, 0);
        assert_eq!(vote.last_probe_unix_nanos, 0);
        assert!(!confirmed_owner_unreachable_evidence(&vote));

        structurally_sign_vote(&mut vote);
        validate_root_failover_vote(&vote).unwrap();

        vote.reason_code = ROOT_FAILOVER_REASON_OWNER_UNREACHABLE.to_string();
        structurally_sign_vote(&mut vote);
        assert!(validate_root_failover_vote(&vote).is_err());
    }

    #[test]
    fn healthy_probe_resets_failover_suspicion() {
        let mut tracker = RootOwnerFailureTracker::default();
        let suspect_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 10);
        let suspect = next_failover_vote(
            None,
            scope(),
            owner_probe_evidence(OwnerProbeState::Failed, suspect_evidence),
            10,
        )
        .unwrap();
        let healthy_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Healthy, 20);
        let healthy = next_failover_vote(
            Some(&suspect),
            scope(),
            owner_probe_evidence(OwnerProbeState::Healthy, healthy_evidence),
            20,
        )
        .unwrap();
        assert_eq!(healthy.decision, "reject");
        assert_eq!(healthy.failed_probe_count, 0);
        let retried_evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, 30);
        let retried = next_failover_vote(
            Some(&healthy),
            scope(),
            owner_probe_evidence(OwnerProbeState::Failed, retried_evidence),
            30,
        )
        .unwrap();
        assert_eq!(retried.failed_probe_count, 1);
    }

    #[test]
    fn one_owner_failure_observation_applies_to_every_root_in_the_ownership_set() {
        let mut tracker = RootOwnerFailureTracker::default();
        let first_at = 10;
        tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, first_at);
        let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
        tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, grant_at);
        let evidence = tracker.observe(owner_failure_scope(), OwnerProbeState::Failed, grant_at);
        let second_root_scope = RootFailoverVoteScope {
            root_key_hash: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            current_generation: 11,
            current_root_hash: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            ..scope()
        };

        let vote = next_failover_vote(
            None,
            second_root_scope,
            owner_probe_evidence(OwnerProbeState::Failed, evidence),
            grant_at,
        )
        .unwrap();

        assert_eq!(vote.decision, "grant");
        assert_eq!(vote.failed_probe_count, ROOT_FAILOVER_PROBE_COUNT);
        assert_eq!(vote.first_failed_probe_unix_nanos, first_at);
    }

    #[test]
    fn confirmed_owner_failure_is_reused_with_one_background_refresh() {
        let mut tracker = RootOwnerFailureTracker::default();
        let scope = owner_failure_scope();
        let first_at = 10;
        let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
        tracker.observe(scope.clone(), OwnerProbeState::Failed, first_at);
        tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);
        tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);

        let (evidence, refresh_started) = tracker
            .confirmed_evidence(&scope, grant_at + 1)
            .expect("confirmed failure evidence remains fresh");
        assert_eq!(evidence.failed_probe_count, ROOT_FAILOVER_PROBE_COUNT);
        assert!(refresh_started);
        assert!(
            !tracker
                .confirmed_evidence(&scope, grant_at + 2)
                .expect("confirmed evidence remains reusable")
                .1,
            "only one refresh probe may run for an ownership set"
        );
    }

    #[test]
    fn confirmed_owner_failure_expires_or_is_cleared_by_health() {
        let mut tracker = RootOwnerFailureTracker::default();
        let scope = owner_failure_scope();
        let first_at = 10;
        let grant_at = first_at + ROOT_FAILOVER_TIMEOUT.as_nanos() as u64;
        tracker.observe(scope.clone(), OwnerProbeState::Failed, first_at);
        tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);
        tracker.observe(scope.clone(), OwnerProbeState::Failed, grant_at);

        assert!(
            tracker
                .confirmed_evidence(
                    &scope,
                    grant_at + ROOT_FAILOVER_CONFIRMED_EVIDENCE_TTL.as_nanos() as u64 + 1,
                )
                .is_none(),
            "stale failure evidence must not grant another root failover"
        );

        tracker
            .confirmed_evidence(&scope, grant_at + 1)
            .expect("fresh evidence starts a refresh");
        tracker.finish_refresh(scope.clone(), OwnerProbeState::Healthy, grant_at + 2);
        assert!(tracker.confirmed_evidence(&scope, grant_at + 3).is_none());
    }
}
