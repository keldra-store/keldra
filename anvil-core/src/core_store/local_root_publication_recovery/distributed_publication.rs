use super::super::local_coremeta_history::CoreMetaGenerationInstallOutcome;
use super::*;

impl CoreStore {
    pub(in crate::core_store::local) async fn materialize_own_committed_publication(
        &self,
        peers: &[super::super::local_coremeta_recovery::RecoveryPeer],
        intent: &RootPublicationIntent,
        committed_coordinator_record: &[u8],
    ) -> Result<()> {
        let outcomes = self.root_publication_outcomes(intent)?;
        let committed_anchors = self
            .committed_anchors_for_local_intent(
                peers,
                intent,
                &outcomes,
                committed_coordinator_record,
            )
            .await?;

        let Some(current_intent) = self.read_root_publication_intent(&intent.transaction_id)?
        else {
            if self
                .completed_publication_matches_intent(intent, &outcomes)
                .await?
            {
                return Ok(());
            }
            bail!("committed CoreMeta publication intent disappeared before materialization");
        };
        if !publication_intent_retry_matches(&current_intent, intent)?
            || current_intent.state != intent.state
            || current_intent.terminal_reason != intent.terminal_reason
        {
            bail!("CoreMeta publication intent changed before committed materialization");
        }

        let current_outcomes = self.root_publication_outcomes(&current_intent)?;
        let current_anchors = self.validate_committed_anchors_for_intent(
            &current_intent,
            &current_outcomes,
            &committed_anchors,
            committed_coordinator_record,
        )?;
        self.materialize_committed_publication_intent(&current_intent, &current_anchors)
            .await?;
        Ok(())
    }

    async fn committed_anchors_for_local_intent(
        &self,
        peers: &[super::super::local_coremeta_recovery::RecoveryPeer],
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
        committed_coordinator_record: &[u8],
    ) -> Result<BTreeMap<String, CoreRootAnchorRecord>> {
        let coordinator = decode_root_anchor_record(committed_coordinator_record)?;
        validate_root_anchor_record(&coordinator)?;
        let coordinator_index = effective_coordinator_index(intent)?;
        let coordinator_root = &intent.roots[coordinator_index];
        let coordinator_hash = coordinator_root.publication.descriptor.root_key_hash();
        if coordinator.root_key_hash != coordinator_hash
            || coordinator.root_generation != coordinator_root.publication.post_root_generation
            || publication_transaction_id(&coordinator)? != intent.transaction_id
        {
            bail!("committed root-register anchor does not match its local publisher intent");
        }

        let mut committed = BTreeMap::from([(coordinator_hash, coordinator)]);
        for root in &intent.roots {
            let root_key_hash = root.publication.descriptor.root_key_hash();
            if committed.contains_key(&root_key_hash) {
                continue;
            }
            let anchor_record = self
                .fetch_committed_cache_anchor(
                    peers,
                    &root_key_hash,
                    root.publication.post_root_generation,
                )
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "committed CoreMeta publication participant has no root-cache quorum: root={} generation={}",
                        root_key_hash,
                        root.publication.post_root_generation
                    )
                })?;
            committed.insert(root_key_hash, decode_root_anchor_record(&anchor_record)?);
        }

        self.validate_committed_anchors_for_intent(
            intent,
            outcomes,
            &committed,
            committed_coordinator_record,
        )?;
        Ok(committed)
    }

    fn validate_committed_anchors_for_intent(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
        committed: &BTreeMap<String, CoreRootAnchorRecord>,
        committed_coordinator_record: &[u8],
    ) -> Result<Vec<CoreRootAnchorRecord>> {
        if committed.len() != intent.roots.len() {
            bail!("committed CoreMeta publication participant cardinality mismatch");
        }
        let outcomes = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        let anchors = intent
            .roots
            .iter()
            .map(|root| {
                let root_key_hash = root.publication.descriptor.root_key_hash();
                let outcome = outcomes
                    .get(root_key_hash.as_str())
                    .ok_or_else(|| anyhow!("committed CoreMeta publication outcome is missing"))?;
                let anchor = committed.get(&root_key_hash).ok_or_else(|| {
                    anyhow!("committed CoreMeta publication participant is missing")
                })?;
                let anchor_record = encode_root_anchor_record(anchor)?;
                self.recovered_committed_anchor(root, outcome, intent, &anchor_record)
            })
            .collect::<Result<Vec<_>>>()?;
        let coordinator_index = effective_coordinator_index(intent)?;
        if encode_root_anchor_record(&anchors[coordinator_index])? != committed_coordinator_record {
            bail!("committed root-register anchor changed its local publisher intent");
        }
        Ok(anchors)
    }

    pub(in crate::core_store::local) fn coremeta_recovery_intent_root_hashes(
        &self,
    ) -> Result<BTreeSet<String>> {
        let mut roots = BTreeSet::new();
        for transaction_id in self.root_publication_intent_ids()? {
            let Some(intent) = self.read_root_publication_intent(&transaction_id)? else {
                continue;
            };
            roots.extend(
                intent
                    .roots
                    .iter()
                    .map(|root| root.publication.descriptor.root_key_hash()),
            );
        }
        Ok(roots)
    }

    pub(in crate::core_store::local) fn encode_coremeta_recovery_publication_bundle(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<u8>> {
        let mut recovery_intent = intent.clone();
        for root in &mut recovery_intent.roots {
            root.certificate_hash = None;
        }
        self.encode_replica_root_publication_intent(&recovery_intent)
    }

    pub(crate) async fn publish_staged_coremeta_recovery_bundle(
        &self,
        publication_bundle: &[u8],
        committed_anchors: &BTreeMap<(String, u64), Vec<u8>>,
    ) -> Result<()> {
        let bundle = decode_coremeta_recovery_publication_bundle(publication_bundle)?;
        let publication_lock_keys = bundle
            .scopes
            .iter()
            .map(|(root_key_hash, _)| ("root-publication".to_string(), root_key_hash.clone()))
            .collect::<BTreeSet<_>>();
        let _publication_guards = self
            .acquire_sorted_lock_keys(&publication_lock_keys)
            .await?;
        let committed_certificates =
            self.recovery_committed_certificate_hashes(&bundle, committed_anchors)?;
        let mut generations = Vec::with_capacity(bundle.scopes.len());
        let mut published = 0usize;
        for (root_key_hash, generation) in &bundle.scopes {
            let prepared = self
                .read_complete_coremeta_generation_for_recovery(root_key_hash, *generation)?
                .ok_or_else(|| {
                    anyhow!(
                        "CoreMeta recovery publication generation is not completely staged: root={root_key_hash} generation={generation}"
                    )
                })?;
            if prepared.descriptor.transaction_id != bundle.transaction_id
                || prepared.descriptor.publication_bundle != publication_bundle
            {
                bail!("CoreMeta recovery publication bundle does not match its generation");
            }
            let committed_certificate = committed_certificates
                .get(&(root_key_hash.clone(), *generation))
                .ok_or_else(|| anyhow!("CoreMeta recovery committed certificate is missing"))?;
            if prepared.descriptor.certificate_hash != *committed_certificate {
                bail!("CoreMeta recovery generation is not the quorum-committed candidate");
            }
            self.validate_descriptor_commit_evidence(&prepared.descriptor)?;
            if self.root_generation_is_published(
                root_key_hash,
                *generation,
                &bundle.transaction_id,
            )? {
                published += 1;
            }
            generations.push(prepared);
        }
        if published == generations.len() {
            self.validate_staged_coremeta_generation_group_for_publication(&bundle.scopes)
                .await?;
            return Ok(());
        }
        if published != 0 {
            bail!("CoreMeta recovery publication group is only partially published");
        }

        let rows_by_root = generations
            .iter()
            .map(|prepared| {
                Ok((
                    prepared.descriptor.root_key_hash.clone(),
                    self.coremeta_generation_mutations_as_owned(
                        &prepared.descriptor,
                        &prepared.mutations,
                    )?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.stage_committed_replica_root_publication_intent(
            publication_bundle,
            &rows_by_root,
            committed_anchors,
        )?;
        self.validate_staged_coremeta_generation_group_for_publication(&bundle.scopes)
            .await?;

        let intent = self
            .read_root_publication_intent(&bundle.transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta recovery publication intent was not staged"))?;
        // The committed anchors prove physical Q2 has already linearized this
        // publication. Deadlines and mutable guards apply before that decision;
        // re-evaluating them during catch-up could revoke an irrevocable commit.
        intent.ensure_pending()?;
        if intent.publisher_node_id != bundle.publisher_node_id {
            bail!("CoreMeta recovery publisher does not match its publication bundle");
        }
        let outcomes = generations
            .iter()
            .map(|prepared| recovery_outcome_from_descriptor(&prepared.descriptor))
            .collect::<Result<Vec<_>>>()?;
        let outcomes_by_root = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        let anchors = intent
            .roots
            .iter()
            .map(|root| {
                let scope = (
                    root.publication.descriptor.root_key_hash(),
                    root.publication.post_root_generation,
                );
                let outcome = outcomes_by_root
                    .get(scope.0.as_str())
                    .ok_or_else(|| anyhow!("CoreMeta recovery participant outcome is missing"))?;
                let bytes = committed_anchors
                    .get(&scope)
                    .ok_or_else(|| anyhow!("CoreMeta recovery committed anchor is missing"))?;
                self.recovered_committed_anchor(root, outcome, &intent, bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        let evidence = root_publication_evidence(&anchors, &outcomes).with_context(|| {
            let scopes = outcomes
                .iter()
                .map(|outcome| {
                    format!(
                        "{}@{}:{}",
                        outcome.root_key_hash,
                        outcome.post_root_generation,
                        outcome.metadata_replica_node_ids.join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(";");
            format!(
                "validate recovered CoreMeta publication evidence: transaction={} scopes={scopes}",
                bundle.transaction_id
            )
        })?;
        let participant_records = self
            .install_recovered_root_publication_commit_evidence(
                &bundle.publisher_node_id,
                &bundle.transaction_id,
                &evidence,
            )
            .await?;
        // Installing the quorum evidence durably advances the staged intent
        // from "no outcomes" to "all outcomes". Continue from that durable
        // state rather than the pre-install snapshot retained above.
        let recorded_intent = self
            .read_root_publication_intent(&bundle.transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta recovery publication intent disappeared"))?;
        if !recorded_intent.all_outcomes_recorded() {
            bail!("CoreMeta recovery publication outcomes were not recorded atomically");
        }
        let coordinator = &anchors[effective_coordinator_index(&recorded_intent)?];
        let expected_root_hash = if coordinator.previous_root_hash == ZERO_HASH {
            ""
        } else {
            coordinator.previous_root_hash.as_str()
        };
        self.compare_and_swap_publication_group_locally(
            &coordinator.root_key_hash,
            coordinator.root_generation.saturating_sub(1),
            expected_root_hash,
            &encode_root_anchor_record(coordinator)?,
            &participant_records,
            Some(&recorded_intent),
            RootPublicationAuthority::RegisterQuorum,
        )
        .await?;
        let outcomes = self
            .validate_staged_coremeta_generation_group_for_publication(&bundle.scopes)
            .await?;
        if outcomes
            .iter()
            .any(|outcome| !matches!(outcome, CoreMetaGenerationInstallOutcome::Published { .. }))
        {
            bail!("CoreMeta recovery publication did not converge atomically");
        }
        Ok(())
    }

    fn recovery_committed_certificate_hashes(
        &self,
        bundle: &CoreMetaRecoveryPublicationBundle,
        committed_anchors: &BTreeMap<(String, u64), Vec<u8>>,
    ) -> Result<BTreeMap<(String, u64), String>> {
        if committed_anchors.len() < bundle.scopes.len() {
            bail!("CoreMeta recovery requires committed anchors for every participant");
        }
        let mut certificates = BTreeMap::new();
        for scope in &bundle.scopes {
            let anchor_bytes = committed_anchors
                .get(scope)
                .ok_or_else(|| anyhow!("CoreMeta recovery participant anchor is missing"))?;
            super::super::local_coremeta_recovery::validate_recovery_publication_anchor(
                bundle,
                scope,
                anchor_bytes,
            )?;
            let anchor = decode_root_anchor_record(anchor_bytes)?;
            let certificate_hash = anchor.core_meta_commit_certificate_hash.ok_or_else(|| {
                anyhow!("CoreMeta recovery participant anchor has no certificate")
            })?;
            certificates.insert(scope.clone(), certificate_hash);
        }
        Ok(certificates)
    }

    fn recovered_committed_anchor(
        &self,
        root: &RootPublicationIntentRoot,
        outcome: &CoreMetaQuorumCommitOutcome,
        intent: &RootPublicationIntent,
        committed_anchor_bytes: &[u8],
    ) -> Result<CoreRootAnchorRecord> {
        let committed = decode_root_anchor_record(committed_anchor_bytes)?;
        validate_root_anchor_record(&committed)?;
        // The immutable intent records the node that proposed the transaction,
        // while a quorum-authorized failover may commit the same generation
        // under a successor's owner terms. The exact root-register/root-cache
        // anchor is authoritative for those terms.
        let expected = self.prepared_root_anchor_with_owner_terms(
            &root.publication,
            outcome,
            &intent.transaction_id,
            super::super::local_root_failover::RootOwnerTerms {
                owner_node_id: committed.publisher_node_id.clone(),
                owner_epoch: committed.publisher_epoch,
                owner_fence: committed.partition_owner_fence,
            },
        )?;
        if committed != expected {
            bail!("CoreMeta recovery participant anchor conflicts with committed evidence");
        }
        Ok(committed)
    }
}

fn recovery_outcome_from_descriptor(
    descriptor: &crate::anvil_api::CoreMetaGenerationDescriptor,
) -> Result<CoreMetaQuorumCommitOutcome> {
    let receipts = descriptor
        .certificate_persist_evidence
        .iter()
        .map(|evidence| {
            decode_deterministic_proto::<crate::anvil_api::CoreMetaCertificatePersistReceipt>(
                &evidence.evidence,
                "CoreMeta recovery certificate persistence receipt",
            )
            .and_then(api_persist_receipt_to_core)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut metadata_replica_node_ids = receipts
        .iter()
        .map(|receipt| receipt.replica_node_id.clone())
        .collect::<Vec<_>>();
    metadata_replica_node_ids.sort();
    metadata_replica_node_ids.dedup();
    Ok(CoreMetaQuorumCommitOutcome {
        root_key_hash: descriptor.root_key_hash.clone(),
        post_root_generation: descriptor.generation,
        certificate_hash: descriptor.certificate_hash.clone(),
        committed_batch_hash: descriptor.committed_batch_hash.clone(),
        certificate_bytes: descriptor.commit_certificate.clone(),
        certificate_persist_receipt_hashes: descriptor
            .certificate_persist_evidence
            .iter()
            .map(|evidence| evidence.evidence_hash.clone())
            .collect(),
        certificate_persist_receipts: receipts,
        metadata_replica_node_ids,
    })
}
