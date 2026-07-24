use super::*;

const REQUESTED_PUBLICATION_CATCH_UP_LIMIT: usize = 32;

impl CoreStore {
    /// Records a supervised repair target before an incoming publication
    /// request performs any await. The request can then be cancelled by its
    /// publisher without cancelling the recovery work on this replica.
    pub(crate) fn incoming_root_publication_is_ready(
        &self,
        root_key_hash: &str,
        expected_generation: u64,
    ) -> Result<bool> {
        validate_hash(
            root_key_hash,
            "incoming root-publication catch-up root key hash",
        )?;
        if expected_generation == 0 {
            return Ok(true);
        }

        let local_generation = self.coremeta_recovery_published_generation(root_key_hash)?;
        if local_generation >= expected_generation {
            return Ok(true);
        }

        self.request_coremeta_root_repair(root_key_hash, expected_generation);
        Ok(false)
    }

    pub(super) async fn recover_requested_root_generations(
        &self,
        peers: &[RecoveryPeer],
    ) -> Result<usize> {
        let targets = self.coremeta_root_repair_targets();
        let mut operations = 0usize;
        for (root_key_hash, target_generation) in targets {
            let mut local_generation =
                self.coremeta_recovery_published_generation(&root_key_hash)?;
            while local_generation < target_generation
                && operations < REQUESTED_PUBLICATION_CATCH_UP_LIMIT
            {
                let next_generation = local_generation.saturating_add(1);
                let committed_anchor = self
                    .fetch_committed_register_anchor(peers, &root_key_hash, next_generation)
                    .await
                    .with_context(|| {
                        format!(
                            "resolve committed root-register repair generation: root={root_key_hash} generation={next_generation}"
                        )
                    })?;
                if let Err(error) = self
                    .catch_up_committed_publication(peers, &committed_anchor)
                    .await
                {
                    let matching_generation = self
                        .read_committed_root_anchor_generation(&root_key_hash, next_generation)
                        .await?
                        .map(|anchor| encode_root_anchor_record(&anchor))
                        .transpose()?
                        .is_some_and(|record| record == committed_anchor);
                    if !matching_generation {
                        return Err(error).with_context(|| {
                            format!(
                                "install committed root-register repair generation: root={root_key_hash} generation={next_generation}"
                            )
                        });
                    }
                }
                let advanced_generation =
                    self.coremeta_recovery_published_generation(&root_key_hash)?;
                if advanced_generation <= local_generation {
                    bail!(
                        "CoreMeta requested root repair made no progress: root={root_key_hash} local={local_generation} target={target_generation}"
                    );
                }
                local_generation = advanced_generation;
                operations = operations.saturating_add(1);
            }
            self.complete_coremeta_root_repair(&root_key_hash, local_generation);
            if operations >= REQUESTED_PUBLICATION_CATCH_UP_LIMIT {
                break;
            }
        }
        Ok(operations)
    }

    /// Installs the publication which already owns the physical Q2 decision for
    /// an exact coordinator generation. Foreground optimistic conflicts use
    /// this bounded path so their caller can retry against the winning state
    /// without waiting for the steady-state anti-entropy interval.
    pub(in crate::core_store::local) async fn catch_up_committed_publication(
        &self,
        peers: &[RecoveryPeer],
        committed_coordinator_record: &[u8],
    ) -> Result<()> {
        let coordinator = decode_root_anchor_record(committed_coordinator_record)?;
        validate_root_anchor_record(&coordinator)?;
        let coordinator_scope = (
            coordinator.root_key_hash.clone(),
            coordinator.root_generation,
        );
        let coordinator_certificate_hash = coordinator
            .core_meta_commit_certificate_hash
            .as_deref()
            .ok_or_else(|| anyhow!("committed root-register anchor has no certificate"))?;
        let publication_bundle = self
            .stage_committed_generation(
                peers,
                &coordinator.root_key_hash,
                coordinator.root_generation,
                coordinator_certificate_hash,
                None,
            )
            .await?;
        let plan = decode_coremeta_recovery_publication_bundle(&publication_bundle)?;
        if plan.coordinator_scope != coordinator_scope {
            bail!(
                "committed root-register anchor does not match its CoreMeta publication coordinator"
            );
        }
        validate_recovery_publication_anchor(
            &plan,
            &coordinator_scope,
            committed_coordinator_record,
        )?;

        let mut committed_anchors = BTreeMap::new();
        committed_anchors.insert(
            coordinator_scope.clone(),
            committed_coordinator_record.to_vec(),
        );
        for scope in &plan.scopes {
            if scope == &coordinator_scope {
                continue;
            }
            let anchor = self
                .fetch_committed_cache_anchor(peers, &scope.0, scope.1)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "committed CoreMeta publication participant has no root-cache quorum: root={} generation={}",
                        scope.0,
                        scope.1
                    )
                })?;
            validate_recovery_publication_anchor(&plan, scope, &anchor)?;
            committed_anchors.insert(scope.clone(), anchor);
        }

        for scope in &plan.scopes {
            let anchor = committed_anchors
                .get(scope)
                .ok_or_else(|| anyhow!("committed CoreMeta publication anchor is missing"))?;
            let anchor = decode_root_anchor_record(anchor)?;
            let certificate_hash = anchor
                .core_meta_commit_certificate_hash
                .as_deref()
                .ok_or_else(|| anyhow!("committed CoreMeta participant has no certificate"))?;
            let staged_bundle = self
                .stage_committed_generation(
                    peers,
                    &scope.0,
                    scope.1,
                    certificate_hash,
                    Some(&publication_bundle),
                )
                .await?;
            if staged_bundle != publication_bundle {
                bail!("committed CoreMeta publication participants disagree on their bundle");
            }
        }

        self.publish_staged_coremeta_recovery_bundle(&publication_bundle, &committed_anchors)
            .await?;
        let transaction_id = publication_transaction_id(&coordinator)?;
        for (root_key_hash, generation) in &plan.scopes {
            if !self.root_generation_is_published(root_key_hash, *generation, &transaction_id)? {
                bail!(
                    "committed CoreMeta publication catch-up did not make every participant visible"
                );
            }
        }
        Ok(())
    }

    async fn stage_committed_generation(
        &self,
        peers: &[RecoveryPeer],
        root_key_hash: &str,
        generation: u64,
        committed_certificate_hash: &str,
        committed_publication_bundle: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        if let Some(prepared) =
            self.read_complete_coremeta_generation_for_recovery(root_key_hash, generation)?
            && prepared.descriptor.certificate_hash == committed_certificate_hash
            && committed_publication_bundle
                .is_none_or(|expected| prepared.descriptor.publication_bundle == expected)
        {
            return Ok(prepared.descriptor.publication_bundle);
        }

        let local_generation = self.coremeta_recovery_published_generation(root_key_hash)?;
        if local_generation.saturating_add(1) != generation {
            bail!(
                "foreground CoreMeta catch-up requires the next generation: root={root_key_hash} local={local_generation} target={generation}"
            );
        }
        let mut round = RecoveryRound::default();
        let sources = self
            .coremeta_recovery_sources(peers, root_key_hash, &mut round)
            .await?;
        let candidates = sources.iter().filter(|source| {
            source.final_generation >= generation && source.retention_floor_generation <= generation
        });
        let mut failures = Vec::new();
        for source in candidates {
            match self
                .fetch_committed_coremeta_generation(
                    source,
                    root_key_hash,
                    generation,
                    committed_certificate_hash,
                    committed_publication_bundle,
                )
                .await
            {
                Ok(bundle) => return Ok(bundle),
                Err(error) => failures.push(format!("{}: {error:#}", source.peer.node_id)),
            }
        }
        bail!(
            "foreground CoreMeta catch-up has no source for the committed generation: root={root_key_hash} generation={generation}: {}",
            failures.join("; ")
        )
    }
}
