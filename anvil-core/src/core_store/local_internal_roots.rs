use super::*;
use crate::anvil_api::{
    CompareAndSwapRootRequest, CoreMetaRootPublicationEvidence, PrepareRootRequest,
    RootPrepareReceipt, root_register_internal_client::RootRegisterInternalClient,
};
use futures_util::StreamExt;
use std::collections::BTreeSet;
use tonic::metadata::MetadataValue;

const ROOT_REGISTER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const ROOT_REGISTER_REPLICA_CATCH_UP_GRACE: Duration = Duration::from_millis(500);

impl CoreStore {
    pub(crate) async fn next_root_generation_for_anchor(
        &self,
        root_anchor_key: &str,
    ) -> Result<u64> {
        self.read_latest_root_anchor(root_anchor_key)
            .await?
            .map_or(Ok(1), |anchor| {
                anchor
                    .root_generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("CoreStore root generation overflow"))
            })
    }

    pub async fn read_internal_root_anchor(
        &self,
        root_anchor_key: &str,
        min_generation: u64,
    ) -> Result<CoreInternalRootAnchorRead> {
        let anchor = self
            .read_latest_root_anchor(root_anchor_key)
            .await?
            .ok_or_else(|| anyhow!("CoreStore root anchor not found"))?;
        if anchor.root_generation < min_generation {
            bail!("CoreStore root anchor generation is below requested minimum");
        }
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }
    pub async fn read_internal_root_anchor_by_hash(
        &self,
        root_key_hash_value: &str,
        min_generation: u64,
    ) -> Result<CoreInternalRootAnchorRead> {
        validate_hash(root_key_hash_value, "internal root key hash")?;
        // This root-cache lookup is the visibility authority for internal root
        // reads; routing it through publication filtering would recurse.
        let Some(bytes) = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_hash_key(root_key_hash_value),
        )?
        else {
            bail!("CoreStore root anchor not found")
        };
        let anchor = decode_root_cache_row(&bytes)?;
        if anchor.root_key_hash != root_key_hash_value || anchor.root_generation < min_generation {
            bail!("CoreStore root anchor not found");
        }
        if !self
            .verify_root_anchor_direct_predecessor(
                root_key_hash_value,
                &anchor.root_anchor_key,
                &anchor,
            )
            .await?
        {
            bail!("CoreStore root anchor predecessor verification failed");
        }
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }

    pub(super) async fn publish_root_anchor_generation(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        self.publish_root_anchor_generation_with_participants(anchor, &[], None)
            .await
    }

    pub(super) async fn publish_root_anchor_generation_with_participants(
        &self,
        anchor: &CoreRootAnchorRecord,
        participant_commit_evidence: &[CoreMetaRootPublicationEvidence],
        publication_intent: Option<&RootPublicationIntent>,
    ) -> Result<()> {
        validate_root_anchor_record(anchor)?;
        if anchor.root_generation == 0 {
            return self.write_root_anchor_generation_local(anchor).await;
        }
        let anchor_bytes = encode_root_anchor_record(anchor)?;
        let expected_generation = anchor.root_generation.saturating_sub(1);
        let expected_root_hash = if anchor.previous_root_hash == ZERO_HASH {
            String::new()
        } else {
            anchor.previous_root_hash.clone()
        };
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let cohort_node_ids = replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        let cohort_hash = root_register_cohort_hash(
            &anchor.root_key_hash,
            anchor.root_generation,
            &cohort_node_ids,
        );
        let placement_epoch = anchor.publisher_epoch.max(LOCAL_PLACEMENT_EPOCH);
        let prepare_started_at = Instant::now();
        let mut local_prepare_results = Vec::new();
        for (shard_index, replica) in replicas
            .iter()
            .enumerate()
            .filter(|(_, replica)| replica.is_local || replica.public_api_addr.trim().is_empty())
        {
            let shard_index = u16::try_from(shard_index)
                .map_err(|_| anyhow!("CoreStore root-register shard index overflow"))?;
            local_prepare_results.push((
                replica.node_id.clone(),
                self.prepare_root_anchor_locally(
                    replica,
                    anchor,
                    &anchor_bytes,
                    expected_generation,
                    &expected_root_hash,
                    &cohort_node_ids,
                    &cohort_hash,
                    shard_index,
                    placement_epoch,
                )
                .await,
            ));
        }
        let mut prepare_results = replicas
            .iter()
            .enumerate()
            .filter(|(_, replica)| !replica.is_local && !replica.public_api_addr.trim().is_empty())
            .map(|(shard_index, replica)| {
                let store = self.clone();
                let replica = replica.clone();
                let anchor = anchor.clone();
                let anchor_bytes = anchor_bytes.clone();
                let expected_root_hash = expected_root_hash.clone();
                let cohort_node_ids = cohort_node_ids.clone();
                let cohort_hash = cohort_hash.clone();
                let node_id = replica.node_id.clone();
                let task = tokio::spawn(async move {
                    let shard_index = u16::try_from(shard_index)
                        .map_err(|_| anyhow!("CoreStore root-register shard index overflow"))?;
                    store
                        .prepare_root_anchor_remotely(
                            &replica,
                            &anchor,
                            &anchor_bytes,
                            expected_generation,
                            &expected_root_hash,
                            &cohort_node_ids,
                            &cohort_hash,
                            shard_index,
                            placement_epoch,
                        )
                        .await
                });
                async move {
                    let result = task
                        .await
                        .map_err(|error| anyhow!("root-register prepare task failed: {error}"))
                        .and_then(|result| result);
                    (node_id, result)
                }
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut prepare_receipts = Vec::new();
        let mut prepare_errors = Vec::new();
        for (node_id, result) in local_prepare_results {
            match result.and_then(|receipt| {
                self.verify_root_prepare_receipt(
                    anchor,
                    expected_generation,
                    &anchor_bytes,
                    &cohort_node_ids,
                    &cohort_hash,
                    placement_epoch,
                    &receipt,
                )?;
                Ok(receipt)
            }) {
                Ok(receipt) => prepare_receipts.push(receipt),
                Err(error) => prepare_errors.push(format!("{node_id}: {error}")),
            }
        }
        while let Some((node_id, result)) = prepare_results.next().await {
            match result.and_then(|receipt| {
                self.verify_root_prepare_receipt(
                    anchor,
                    expected_generation,
                    &anchor_bytes,
                    &cohort_node_ids,
                    &cohort_hash,
                    placement_epoch,
                    &receipt,
                )?;
                Ok(receipt)
            }) {
                Ok(receipt) => prepare_receipts.push(receipt),
                Err(error) => prepare_errors.push(format!("{node_id}: {error}")),
            }
            if prepare_receipts.len() >= profile.prepare_quorum {
                break;
            }
        }
        // Dropping these join handles detaches, rather than cancels, the
        // bounded replica attempts. Healthy replicas therefore continue
        // staging the generation after Q2 has allowed the publisher to move
        // on, while an unavailable replica remains outside the write latency.
        drop(prepare_results);
        if prepare_receipts.len() < profile.prepare_quorum {
            crate::perf::record_root_register_cas_duration(
                "prepare",
                profile.profile_id.as_str(),
                "quorum_failed",
                prepare_started_at.elapsed(),
            );
            crate::perf::record_failover_vote_total("root_prepare", "quorum_failed");
            return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                operation: "root_prepare",
                required: profile.prepare_quorum,
                received: prepare_receipts.len(),
                details: format!(
                    "root={}: {}",
                    anchor.root_key_hash,
                    prepare_errors.join("; ")
                ),
            }
            .into());
        }
        crate::perf::record_root_register_cas_duration(
            "prepare",
            profile.profile_id.as_str(),
            "ok",
            prepare_started_at.elapsed(),
        );
        crate::perf::record_failover_vote_total("root_prepare", "ok");
        self.validate_root_prepare_quorum(
            anchor,
            expected_generation,
            &anchor_bytes,
            &cohort_node_ids,
            &cohort_hash,
            placement_epoch,
            &prepare_receipts,
        )?;
        #[cfg(any(test, feature = "root-publication-test-control"))]
        super::local_root_publication_test_control::pause_after_root_register_commit(
            super::local_root_publication_recovery::publication_transaction_id(anchor)?,
        )
        .await;

        let certificate_hash = anchor
            .core_meta_commit_certificate_hash
            .as_deref()
            .ok_or_else(|| anyhow!("CoreStore root anchor missing CoreMeta commit certificate"))?;
        let evidence = self
            .read_coremeta_commit_evidence(certificate_hash)?
            .ok_or_else(|| anyhow!("CoreStore root anchor CoreMeta commit evidence is missing"))?;
        let certificate = decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
            &evidence.certificate_bytes,
            "CoreMeta commit certificate",
        )?;
        let certificate_persist_receipts = evidence
            .certificate_persist_receipt_bytes
            .iter()
            .map(|bytes| {
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCertificatePersistReceipt>(
                    bytes,
                    "CoreMeta certificate persist receipt",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let participant_anchor_records = participant_commit_evidence
            .iter()
            .map(|participant| participant.root_anchor_record.clone())
            .collect::<Vec<_>>();

        let mut write_count = 0usize;
        let mut completed_attempts = 0usize;
        let mut write_errors = Vec::new();
        let cas_started_at = Instant::now();
        for replica in replicas
            .iter()
            .filter(|replica| replica.is_local || replica.public_api_addr.trim().is_empty())
        {
            let result = self
                .compare_and_swap_root_anchor_locally(
                    anchor,
                    expected_generation,
                    &expected_root_hash,
                    &participant_anchor_records,
                    publication_intent,
                    &prepare_receipts,
                    &cohort_node_ids,
                    &cohort_hash,
                    placement_epoch,
                )
                .await;
            completed_attempts = completed_attempts.saturating_add(1);
            match result {
                Ok(()) => write_count = write_count.saturating_add(1),
                Err(error) => write_errors.push(format!("{}: {error}", replica.node_id)),
            }
        }
        let mut cas_results = replicas
            .iter()
            .filter(|replica| !replica.is_local && !replica.public_api_addr.trim().is_empty())
            .map(|replica| {
                let store = self.clone();
                let replica = replica.clone();
                let anchor = anchor.clone();
                let anchor_bytes = anchor_bytes.clone();
                let expected_root_hash = expected_root_hash.clone();
                let prepare_receipts = prepare_receipts.clone();
                let cohort_node_ids = cohort_node_ids.clone();
                let cohort_hash = cohort_hash.clone();
                let certificate = certificate.clone();
                let certificate_persist_receipts = certificate_persist_receipts.clone();
                let participant_commit_evidence = participant_commit_evidence.to_vec();
                let node_id = replica.node_id.clone();
                let task = tokio::spawn(async move {
                    store
                        .compare_and_swap_root_anchor_remotely(
                            &replica,
                            &anchor,
                            &anchor_bytes,
                            expected_generation,
                            &expected_root_hash,
                            &certificate,
                            &certificate_persist_receipts,
                            &prepare_receipts,
                            &participant_commit_evidence,
                            &cohort_node_ids,
                            &cohort_hash,
                            placement_epoch,
                        )
                        .await
                        .map(|_| ())
                });
                async move {
                    let result = task
                        .await
                        .map_err(|error| anyhow!("root-register CAS task failed: {error}"))
                        .and_then(|result| result);
                    (node_id, result)
                }
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        while let Some((node_id, result)) = cas_results.next().await {
            completed_attempts = completed_attempts.saturating_add(1);
            match result {
                Ok(()) => write_count += 1,
                Err(error) => write_errors.push(format!("{node_id}: {error}")),
            }
            if write_count >= profile.certificate_persist_quorum {
                break;
            }
        }
        let catch_up_completed = if completed_attempts < replicas.len() {
            tokio::time::timeout(ROOT_REGISTER_REPLICA_CATCH_UP_GRACE, async {
                while let Some((node_id, result)) = cas_results.next().await {
                    completed_attempts = completed_attempts.saturating_add(1);
                    match result {
                        Ok(()) => write_count += 1,
                        Err(error) => write_errors.push(format!("{node_id}: {error}")),
                    }
                }
            })
            .await
            .is_ok()
        } else {
            true
        };
        let repair_pending = !catch_up_completed || write_count < replicas.len();
        if repair_pending {
            tracing::warn!(
                root_key_hash = %anchor.root_key_hash,
                root_generation = anchor.root_generation,
                committed_replicas = write_count,
                replica_count = replicas.len(),
                errors = %write_errors.join("; "),
                "root-register publication reached quorum with replica repair pending"
            );
        }
        // The remaining bounded attempts stay supervised by Tokio after their
        // join handles are dropped. Receiver-side repair targets make a
        // generation gap persistent even if a later client request is
        // cancelled.
        drop(cas_results);
        if write_count < profile.certificate_persist_quorum {
            crate::perf::record_root_register_cas_duration(
                "compare_and_swap",
                profile.profile_id.as_str(),
                "quorum_failed",
                cas_started_at.elapsed(),
            );
            crate::perf::record_failover_vote_total("root_compare_and_swap", "quorum_failed");
            return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                operation: "root_compare_and_swap",
                required: profile.certificate_persist_quorum,
                received: write_count,
                details: format!("root={}: {}", anchor.root_key_hash, write_errors.join("; ")),
            }
            .into());
        }
        crate::perf::record_root_register_cas_duration(
            "compare_and_swap",
            profile.profile_id.as_str(),
            if repair_pending {
                "quorum_committed_repair_pending"
            } else {
                "ok"
            },
            cas_started_at.elapsed(),
        );
        crate::perf::record_failover_vote_total("root_compare_and_swap", "ok");
        // The Q2 durable prepare receipts commit this root generation. Install
        // the committed head locally even when this publisher is not one of
        // the three register replicas; peer repair can then discover it.
        self.compare_and_swap_root_anchor_locally(
            anchor,
            expected_generation,
            &expected_root_hash,
            &participant_anchor_records,
            publication_intent,
            &prepare_receipts,
            &cohort_node_ids,
            &cohort_hash,
            placement_epoch,
        )
        .await?;
        Ok(())
    }

    async fn prepare_root_anchor_locally(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
        cohort_node_ids: &[String],
        cohort_hash: &str,
        shard_index: u16,
        placement_epoch: u64,
    ) -> Result<RootPrepareReceipt> {
        self.validate_root_owner_publication(&self.node_identity.node_id, anchor)?;
        self.validate_root_cas_precondition(
            &anchor.root_key_hash,
            expected_generation,
            expected_root_hash,
            anchor,
        )
        .await?;
        self.persist_root_register_prepare(
            &replica.node_id,
            anchor,
            anchor_bytes,
            expected_generation,
            cohort_node_ids,
            cohort_hash,
            shard_index,
            placement_epoch,
        )
        .await
    }

    async fn prepare_root_anchor_remotely(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
        cohort_node_ids: &[String],
        cohort_hash: &str,
        shard_index: u16,
        placement_epoch: u64,
    ) -> Result<RootPrepareReceipt> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "root register remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let bearer = bearer.to_string();
        let public_api_addr = replica.public_api_addr.clone();
        let root_key_hash = anchor.root_key_hash.clone();
        let expected_root_hash = expected_root_hash.to_string();
        let new_root_anchor_record = anchor_bytes.to_vec();
        let partition_owner_fence = anchor.partition_owner_fence;
        let register_cohort_node_ids = cohort_node_ids.to_vec();
        let register_cohort_hash = cohort_hash.to_string();
        tokio::time::timeout(
            ROOT_REGISTER_REQUEST_TIMEOUT,
            self.internal_grpc_request(&public_api_addr, "root.prepare", move |channel| {
                let bearer = bearer.clone();
                let root_key_hash = root_key_hash.clone();
                let expected_root_hash = expected_root_hash.clone();
                let new_root_anchor_record = new_root_anchor_record.clone();
                let register_cohort_node_ids = register_cohort_node_ids.clone();
                let register_cohort_hash = register_cohort_hash.clone();
                async move {
                    let mut client = RootRegisterInternalClient::new(channel);
                    let mut request = tonic::Request::new(PrepareRootRequest {
                        header: Some(self.internal_request_header("root.prepare").map_err(
                            |err| tonic::Status::internal(format!("build internal header: {err}")),
                        )?),
                        root_key_hash,
                        expected_generation,
                        expected_root_hash,
                        new_root_anchor_record,
                        partition_owner_fence,
                        register_cohort_node_ids,
                        register_cohort_hash,
                        shard_index: u32::from(shard_index),
                        placement_epoch,
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        MetadataValue::try_from(format!("Bearer {bearer}")).map_err(|err| {
                            tonic::Status::internal(format!(
                                "encode root register internal bearer token: {err}"
                            ))
                        })?,
                    );
                    client
                        .prepare_root(request)
                        .await
                        .map(|response| response.into_inner())
                }
            }),
        )
        .await
        .map_err(|_| anyhow!("root.prepare request to {public_api_addr} timed out"))?
    }

    async fn compare_and_swap_root_anchor_locally(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        expected_root_hash: &str,
        participant_anchor_records: &[Vec<u8>],
        publication_intent: Option<&RootPublicationIntent>,
        prepare_receipts: &[RootPrepareReceipt],
        cohort_node_ids: &[String],
        cohort_hash: &str,
        placement_epoch: u64,
    ) -> Result<()> {
        let anchor_bytes = encode_root_anchor_record(anchor)?;
        self.validate_root_prepare_quorum(
            anchor,
            expected_generation,
            &anchor_bytes,
            cohort_node_ids,
            cohort_hash,
            placement_epoch,
            prepare_receipts,
        )?;
        self.validate_root_owner_publication(&self.node_identity.node_id, anchor)?;
        self.ensure_local_committed_root_register_shard(
            anchor,
            &anchor_bytes,
            expected_generation,
            cohort_node_ids,
            cohort_hash,
            placement_epoch,
        )
        .await?;
        if !participant_anchor_records.is_empty() {
            self.compare_and_swap_publication_group_locally(
                &anchor.root_key_hash,
                expected_generation,
                expected_root_hash,
                &anchor_bytes,
                participant_anchor_records,
                publication_intent,
                super::local_root_publication_recovery::RootPublicationAuthority::RegisterQuorum,
            )
            .await?;
            return Ok(());
        }
        self.validate_root_cas_precondition(
            &anchor.root_key_hash,
            expected_generation,
            expected_root_hash,
            anchor,
        )
        .await?;
        self.write_root_anchor_generation_local(anchor).await
    }

    async fn compare_and_swap_root_anchor_remotely(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
        certificate: &crate::anvil_api::CoreMetaCommitCertificate,
        certificate_persist_receipts: &[crate::anvil_api::CoreMetaCertificatePersistReceipt],
        prepare_receipts: &[RootPrepareReceipt],
        participant_commit_evidence: &[CoreMetaRootPublicationEvidence],
        cohort_node_ids: &[String],
        cohort_hash: &str,
        placement_epoch: u64,
    ) -> Result<crate::anvil_api::RootAnchorWrite> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "root register remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let bearer = bearer.to_string();
        let public_api_addr = replica.public_api_addr.clone();
        let root_key_hash = anchor.root_key_hash.clone();
        let expected_root_hash = expected_root_hash.to_string();
        let new_root_anchor_record = anchor_bytes.to_vec();
        let partition_owner_fence = anchor.partition_owner_fence;
        let certificate = certificate.clone();
        let certificate_persist_receipts = certificate_persist_receipts.to_vec();
        let prepare_receipts = prepare_receipts.to_vec();
        let participant_commit_evidence = participant_commit_evidence.to_vec();
        let register_cohort_node_ids = cohort_node_ids.to_vec();
        let register_cohort_hash = cohort_hash.to_string();
        tokio::time::timeout(
            ROOT_REGISTER_REQUEST_TIMEOUT,
            self.internal_grpc_request(&public_api_addr, "root.compare_and_swap", move |channel| {
                let bearer = bearer.clone();
                let root_key_hash = root_key_hash.clone();
                let expected_root_hash = expected_root_hash.clone();
                let new_root_anchor_record = new_root_anchor_record.clone();
                let certificate = certificate.clone();
                let certificate_persist_receipts = certificate_persist_receipts.clone();
                let prepare_receipts = prepare_receipts.clone();
                let participant_commit_evidence = participant_commit_evidence.clone();
                let register_cohort_node_ids = register_cohort_node_ids.clone();
                let register_cohort_hash = register_cohort_hash.clone();
                async move {
                    let mut client = RootRegisterInternalClient::new(channel);
                    let mut request = tonic::Request::new(CompareAndSwapRootRequest {
                        header: Some(
                            self.internal_request_header("root.compare_and_swap")
                                .map_err(|err| {
                                    tonic::Status::internal(format!("build internal header: {err}"))
                                })?,
                        ),
                        root_key_hash,
                        expected_generation,
                        expected_root_hash,
                        new_root_anchor_record,
                        partition_owner_fence,
                        core_meta_commit_certificate: Some(certificate.clone()),
                        core_meta_commit_certificate_hash: certificate.certificate_hash.clone(),
                        certificate_persist_receipts,
                        prepare_receipts,
                        participant_commit_evidence,
                        register_cohort_node_ids,
                        register_cohort_hash,
                        placement_epoch,
                    });
                    request.metadata_mut().insert(
                        "authorization",
                        MetadataValue::try_from(format!("Bearer {bearer}")).map_err(|err| {
                            tonic::Status::internal(format!(
                                "encode root register internal bearer token: {err}"
                            ))
                        })?,
                    );
                    client
                        .compare_and_swap_root(request)
                        .await
                        .map(|response| response.into_inner())
                }
            }),
        )
        .await
        .map_err(|_| anyhow!("root.compare_and_swap request to {public_api_addr} timed out"))?
    }

    pub(super) async fn validate_root_cas_precondition(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        if new_anchor.root_key_hash != root_key_hash_value {
            bail!("CoreStore root CAS root key hash mismatch");
        }
        if new_anchor.root_generation != expected_generation.saturating_add(1) {
            bail!("CoreStore root CAS post generation mismatch");
        }
        match self
            .read_internal_root_anchor_by_hash(root_key_hash_value, 0)
            .await
        {
            Ok(current) => {
                let new_anchor_hash = hash_root_anchor_record(new_anchor)?;
                if current.generation == new_anchor.root_generation
                    && current.root_anchor_hash == new_anchor_hash
                {
                    return Ok(());
                }
                if current.generation != expected_generation {
                    bail!("CoreStore root CAS expected generation mismatch");
                }
                if !expected_root_hash.is_empty() && current.root_anchor_hash != expected_root_hash
                {
                    bail!(
                        "CoreStore root CAS expected root hash mismatch: root_key_hash={} generation={} expected={} current={}",
                        root_key_hash_value,
                        expected_generation,
                        expected_root_hash,
                        current.root_anchor_hash
                    );
                }
                if new_anchor.previous_root_hash != current.root_anchor_hash {
                    bail!("CoreStore root CAS previous hash mismatch");
                }
            }
            Err(_) => {
                if expected_generation != 0 || !expected_root_hash.is_empty() {
                    bail!("CoreStore root CAS expected generation missing");
                }
            }
        }
        Ok(())
    }

    fn validate_root_prepare_quorum(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        anchor_bytes: &[u8],
        cohort_node_ids: &[String],
        cohort_hash: &str,
        placement_epoch: u64,
        receipts: &[RootPrepareReceipt],
    ) -> Result<()> {
        if cohort_node_ids.len() != self.default_coremeta_quorum_profile()?.replica_count
            || cohort_hash
                != root_register_cohort_hash(
                    &anchor.root_key_hash,
                    anchor.root_generation,
                    cohort_node_ids,
                )
            || placement_epoch == 0
        {
            bail!("CoreStore root prepare cohort scope mismatch");
        }
        let mut replicas = BTreeSet::new();
        for receipt in receipts {
            self.verify_root_prepare_receipt(
                anchor,
                expected_generation,
                anchor_bytes,
                cohort_node_ids,
                cohort_hash,
                placement_epoch,
                receipt,
            )?;
            replicas.insert(receipt.replica_node_id.as_str());
        }
        if replicas.len() < self.default_coremeta_quorum_profile()?.prepare_quorum {
            bail!("CoreStore root prepare quorum has duplicate replicas");
        }
        Ok(())
    }

    fn verify_root_prepare_receipt(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        anchor_bytes: &[u8],
        cohort_node_ids: &[String],
        cohort_hash: &str,
        placement_epoch: u64,
        receipt: &RootPrepareReceipt,
    ) -> Result<()> {
        let new_root_hash = format!("sha256:{}", sha256_hex(anchor_bytes));
        let shard_index = usize::try_from(receipt.shard_index)
            .map_err(|_| anyhow!("CoreStore root prepare shard index overflow"))?;
        if receipt.root_key_hash != anchor.root_key_hash
            || receipt.expected_generation != expected_generation
            || receipt.post_generation != anchor.root_generation
            || receipt.new_root_hash != new_root_hash
            || receipt.register_cohort_hash != cohort_hash
            || receipt.placement_epoch != placement_epoch
            || receipt.fsync_sequence == 0
            || cohort_node_ids.get(shard_index).map(String::as_str)
                != Some(receipt.replica_node_id.as_str())
            || receipt.signed_payload_hash != root_prepare_receipt_payload_hash(receipt)
        {
            bail!("CoreStore root prepare receipt scope mismatch");
        }
        self.verify_internal_core_receipt_signature(
            &receipt.replica_node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )
    }

    pub async fn compare_and_swap_internal_root_anchor(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_root_anchor_record: &[u8],
        participant_anchor_records: &[Vec<u8>],
    ) -> Result<CoreInternalRootAnchorRead> {
        self.compare_and_swap_internal_root_anchor_with_authority(
            root_key_hash_value,
            expected_generation,
            expected_root_hash,
            new_root_anchor_record,
            participant_anchor_records,
            super::local_root_publication_recovery::RootPublicationAuthority::LocalOwnerState,
        )
        .await
    }

    pub(crate) async fn compare_and_swap_internal_root_anchor_from_register_quorum(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_root_anchor_record: &[u8],
        participant_anchor_records: &[Vec<u8>],
    ) -> Result<CoreInternalRootAnchorRead> {
        let publication_guards = if participant_anchor_records.is_empty() {
            None
        } else {
            let mut lock_keys = BTreeSet::new();
            let coordinator = decode_root_anchor_record(new_root_anchor_record)?;
            lock_keys.insert(("root-publication".to_string(), coordinator.root_key_hash));
            for record in participant_anchor_records {
                let participant = decode_root_anchor_record(record)?;
                lock_keys.insert(("root-publication".to_string(), participant.root_key_hash));
            }
            Some(self.acquire_sorted_lock_keys(&lock_keys).await?)
        };
        let result = self
            .compare_and_swap_internal_root_anchor_with_authority(
                root_key_hash_value,
                expected_generation,
                expected_root_hash,
                new_root_anchor_record,
                participant_anchor_records,
                super::local_root_publication_recovery::RootPublicationAuthority::RegisterQuorum,
            )
            .await;
        drop(publication_guards);
        result
    }

    async fn compare_and_swap_internal_root_anchor_with_authority(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_root_anchor_record: &[u8],
        participant_anchor_records: &[Vec<u8>],
        authority: super::local_root_publication_recovery::RootPublicationAuthority,
    ) -> Result<CoreInternalRootAnchorRead> {
        validate_hash(root_key_hash_value, "internal root key hash")?;
        if !expected_root_hash.is_empty() {
            validate_hash(expected_root_hash, "internal expected root hash")?;
        }
        let anchor = decode_root_anchor_record(new_root_anchor_record)?;
        if anchor.root_key_hash != root_key_hash_value {
            bail!("CoreStore internal root CAS root key hash mismatch");
        }
        if anchor.root_generation != expected_generation.saturating_add(1) {
            bail!("CoreStore internal root CAS post generation mismatch");
        }
        if !participant_anchor_records.is_empty() {
            return self
                .compare_and_swap_publication_group_locally(
                    root_key_hash_value,
                    expected_generation,
                    expected_root_hash,
                    new_root_anchor_record,
                    participant_anchor_records,
                    None,
                    authority,
                )
                .await;
        }
        let current = self
            .read_internal_root_anchor_by_hash(root_key_hash_value, 0)
            .await
            .ok();
        if let Some(current) = current {
            if current.generation != expected_generation {
                bail!("CoreStore internal root CAS expected generation mismatch");
            }
            if !expected_root_hash.is_empty() && current.root_anchor_hash != expected_root_hash {
                bail!("CoreStore internal root CAS expected root hash mismatch");
            }
        } else if expected_generation != 0 {
            bail!("CoreStore internal root CAS missing expected generation");
        }
        self.write_root_anchor_generation_local(&anchor).await?;
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }
}
