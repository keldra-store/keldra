use super::*;
use crate::anvil_api::{
    CoreMetaBatchGroupRequest, CoreMetaBatchRequest,
    CoreMetaCommitCertificate as ApiCoreMetaCommitCertificate, CoreMetaPersistCommitGroupRequest,
    CoreMetaPersistCommitRequest, CoreMetaRowMutation, CoreMetaStreamRequest,
    core_meta_stream_request, core_meta_stream_response,
};
use crate::mesh_lifecycle::{self, LifecycleState, NodeCapability};
use futures_util::StreamExt;

#[derive(Debug, Clone)]
pub(crate) struct CoreMetaQuorumCommitOutcome {
    pub(crate) root_key_hash: String,
    pub(crate) post_root_generation: u64,
    pub(crate) certificate_hash: String,
    pub(crate) committed_batch_hash: String,
    pub(crate) certificate_bytes: Vec<u8>,
    pub(crate) certificate_persist_receipt_hashes: Vec<String>,
    pub(crate) certificate_persist_receipts: Vec<CoreMetaCertificatePersistReceipt>,
    pub(crate) metadata_replica_node_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct CoreMetaRootCommitInput {
    root_key_hash: String,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: String,
    rows: Vec<CoreMetaEncodedOwnedRow>,
    publication: Option<PreparedRootPublication>,
}

#[derive(Debug, Clone)]
struct PreparedCoreMetaRootCommit {
    input: CoreMetaRootCommitInput,
    pending_batch_hash: String,
    row_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct CertifiedCoreMetaRootCommit {
    prepared: PreparedCoreMetaRootCommit,
    certificate: CoreMetaCommitCertificate,
    committed_batch_hash: String,
}

/// Commit feature-owned CoreMeta rows through CoreStore instead of opening the
/// local RocksDB store directly. Synchronous feature stores should become async
/// at their public write boundary and use this helper so rooted rows get quorum
/// evidence and local rows still pass through the single CoreStore boundary.
pub(crate) async fn commit_coremeta_batch_for_storage(
    storage: &crate::storage::Storage,
    transaction_id: &str,
    ops: &[CoreMetaBatchOp<'_>],
    publications: &[CoreMetaRootPublication],
) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
    let store = CoreStore::new(storage.clone()).await?;
    store
        .commit_coremeta_root_groups(transaction_id, ops, publications)
        .await
}

impl CoreStore {
    pub(crate) async fn coremeta_peer_route(
        &self,
        root_key_hash: &str,
    ) -> Result<CoreMetaPeerRoute> {
        validate_hash(root_key_hash, "CoreMeta peer route root key hash")?;
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let local_replica = replicas.iter().any(|replica| replica.is_local);
        let mut remote_targets = replicas
            .into_iter()
            .filter(|replica| !replica.is_local && !replica.public_api_addr.trim().is_empty())
            .map(|replica| CoreMetaPeerTarget {
                node_id: replica.node_id,
                public_api_addr: replica.public_api_addr,
            })
            .collect::<Vec<_>>();
        order_coremeta_peer_targets(root_key_hash, &mut remote_targets);
        Ok(CoreMetaPeerRoute {
            local_replica,
            remote_targets,
        })
    }

    pub(crate) async fn commit_coremeta_root_groups(
        &self,
        transaction_id: &str,
        ops: &[CoreMetaBatchOp<'_>],
        publications: &[CoreMetaRootPublication],
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        let root_key_hashes = publications
            .iter()
            .map(CoreMetaRootPublication::root_key_hash)
            .collect::<BTreeSet<_>>();
        let mut _root_plan_guards = Vec::with_capacity(root_key_hashes.len());
        for root_key_hash in &root_key_hashes {
            _root_plan_guards.push(
                self.acquire_named_lock("coremeta-root", root_key_hash)
                    .await?,
            );
        }
        self.commit_coremeta_root_groups_prelocked(transaction_id, ops, publications)
            .await
    }

    pub(super) async fn commit_coremeta_root_groups_prelocked(
        &self,
        transaction_id: &str,
        ops: &[CoreMetaBatchOp<'_>],
        publications: &[CoreMetaRootPublication],
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        validate_logical_id(transaction_id, "CoreMeta quorum transaction id")?;
        let mut publications_by_hash = BTreeMap::new();
        let mut coordinator_count = 0_usize;
        for publication in publications {
            publication.validate()?;
            coordinator_count += usize::from(publication.transaction_coordinator);
            let root_key_hash = publication.root_key_hash();
            if publications_by_hash
                .insert(root_key_hash.clone(), publication.clone())
                .is_some()
            {
                bail!("CoreMeta mutation contains duplicate publication for root {root_key_hash}");
            }
        }
        if coordinator_count > 1 {
            bail!("CoreMeta mutation may name at most one transaction coordinator root");
        }
        let existing_intent = self.read_root_publication_intent(transaction_id)?;
        let mut generation_bindings = BTreeMap::new();
        if let Some(intent) = existing_intent.as_ref() {
            self.validate_retry_publication_descriptors(intent, &publications_by_hash)?;
            generation_bindings.extend(intent.roots.iter().map(|root| {
                (
                    root.publication.descriptor.root_key_hash(),
                    root.publication.post_root_generation,
                )
            }));
        } else {
            for publication in publications_by_hash.values() {
                generation_bindings.insert(
                    publication.root_key_hash(),
                    self.next_root_generation_for_anchor(&publication.root_anchor_key)
                        .await?,
                );
            }
        }

        let mut local_rows = Vec::new();
        let mut replicated_rows =
            BTreeMap::<String, BTreeMap<u64, Vec<CoreMetaEncodedOwnedRow>>>::new();

        let mut encoded_rows = self.meta.encode_batch_ops(ops)?;
        self.bind_encoded_rows_to_generations(
            &mut encoded_rows,
            transaction_id,
            &generation_bindings,
        )?;
        for row in encoded_rows {
            if row.root_key_hash.is_empty() {
                local_rows.push(row);
            } else {
                replicated_rows
                    .entry(row.root_key_hash.clone())
                    .or_default()
                    .entry(row.root_generation)
                    .or_default()
                    .push(row);
            }
        }

        if replicated_rows.len() > 1 && coordinator_count != 1 {
            bail!("CoreMeta multi-root mutation must declare exactly one coordinator root");
        }
        if let Some(intent) = existing_intent {
            let mut plan_roots = Vec::with_capacity(replicated_rows.len());
            for (root_key_hash, generations) in &replicated_rows {
                if generations.len() != 1 {
                    bail!("CoreMeta idempotent retry changed its root generation plan");
                }
                let descriptor = publications_by_hash.get(root_key_hash).ok_or_else(|| {
                    anyhow!("CoreMeta idempotent retry omitted a publication root")
                })?;
                let rows = generations
                    .first_key_value()
                    .expect("one generation was checked")
                    .1
                    .clone();
                plan_roots.push((descriptor.clone(), rows));
            }
            let plan_hash = root_publication_plan_hash(transaction_id, &plan_roots, &local_rows)?;
            let local_rows_match = publication_intent_local_rows_match(&intent, &local_rows);
            if plan_hash != intent.plan_hash || !local_rows_match {
                bail!(
                    "CoreMeta idempotent retry changed its durable publication plan for \
                     transaction {transaction_id}: stored_plan={}, retry_plan={plan_hash}, \
                     local_rows_match={local_rows_match}",
                    intent.plan_hash
                );
            }
            return self.resume_root_publication_intent(intent).await;
        }

        let coordinator_scope = publications_by_hash
            .iter()
            .find(|(_, publication)| publication.transaction_coordinator)
            .map(|(root_key_hash, _)| {
                let generations = replicated_rows.get(root_key_hash).ok_or_else(|| {
                    anyhow!(
                        "CoreMeta transaction coordinator root {root_key_hash} has no rows"
                    )
                })?;
                if generations.len() != 1 {
                    bail!(
                        "CoreMeta transaction coordinator root {root_key_hash} must publish exactly one generation"
                    );
                }
                Ok::<_, anyhow::Error>((
                    root_key_hash.clone(),
                    *generations
                        .first_key_value()
                        .expect("one coordinator generation was checked")
                        .0,
                ))
            })
            .transpose()?;

        let publication_created_at = unix_timestamp_nanos();
        let mut inputs = Vec::with_capacity(replicated_rows.len());
        for (root_key_hash, mut generations) in replicated_rows {
            if generations.len() != 1 {
                let generation_summary = generations
                    .iter()
                    .map(|(generation, rows)| {
                        let tables = rows
                            .iter()
                            .map(|row| {
                                format!(
                                    "{}:{}:{}",
                                    row.cf,
                                    if row.delete_marker { "delete" } else { "put" },
                                    hex::encode(
                                        &row.core_meta_key[..row.core_meta_key.len().min(20)]
                                    )
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("|");
                        format!("{generation}:{}[{tables}]", rows.len())
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                bail!(
                    "CoreMeta logical mutation {transaction_id} assigns multiple generations to root {root_key_hash} ({generation_summary}); one mutation must publish one root generation"
                );
            }
            let (post_root_generation, rows) = generations
                .pop_first()
                .expect("one generation was checked above");
            let descriptor = publications_by_hash.remove(&root_key_hash).ok_or_else(|| {
                anyhow!(
                    "CoreMeta logical mutation {transaction_id} is missing the canonical publication descriptor for root {root_key_hash}"
                )
            })?;
            if let Some(existing_manifest) = self.root_generation_commit_manifest(
                &root_key_hash,
                post_root_generation,
                transaction_id,
            )? {
                let expected_coordinator_hash =
                    coordinator_scope.as_ref().map(|(root, _)| root.as_str());
                let expected_coordinator_generation = coordinator_scope
                    .as_ref()
                    .map(|(_, generation)| *generation);
                if existing_manifest.coordinator_root_key_hash.as_deref()
                    != expected_coordinator_hash
                    || existing_manifest.coordinator_root_generation
                        != expected_coordinator_generation
                {
                    bail!(
                        "CoreMeta idempotent retry changed coordinator scope for root {root_key_hash}"
                    );
                }
                continue;
            }
            let prepared_publication = self
                .prepare_root_publication(
                    transaction_id,
                    &descriptor,
                    post_root_generation,
                    coordinator_scope.as_ref(),
                    publication_created_at,
                )
                .await?;
            let mut rows = rows;
            rows.push(prepared_publication.transaction_manifest_row.clone());
            inputs.push(CoreMetaRootCommitInput {
                root_key_hash,
                expected_root_generation: post_root_generation.saturating_sub(1),
                post_root_generation,
                transaction_id: transaction_id.to_string(),
                rows,
                publication: Some(prepared_publication),
            });
        }
        if let Some((root_key_hash, _)) = publications_by_hash.pop_first() {
            bail!(
                "CoreMeta logical mutation {transaction_id} declares unused publication root {root_key_hash}"
            );
        }
        self.commit_coremeta_encoded_rows_for_roots(inputs, local_rows)
            .await
    }

    pub(crate) async fn commit_coremeta_encoded_rows_for_root(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        rows: Vec<CoreMetaEncodedOwnedRow>,
    ) -> Result<CoreMetaQuorumCommitOutcome> {
        let mut outcomes = self
            .commit_coremeta_encoded_rows_for_roots(
                vec![CoreMetaRootCommitInput {
                    root_key_hash: root_key_hash.to_string(),
                    expected_root_generation,
                    post_root_generation,
                    transaction_id: transaction_id.to_string(),
                    rows,
                    publication: None,
                }],
                Vec::new(),
            )
            .await?;
        outcomes
            .pop()
            .ok_or_else(|| anyhow!("CoreMeta quorum batch produced no outcome"))
    }

    fn validate_retry_publication_descriptors(
        &self,
        intent: &RootPublicationIntent,
        publications: &BTreeMap<String, CoreMetaRootPublication>,
    ) -> Result<()> {
        if intent.publisher_node_id != self.node_identity.node_id {
            bail!("CoreMeta publication retry is owned by another node");
        }
        if intent.roots.len() != publications.len() {
            bail!("CoreMeta idempotent retry changed its publication root count");
        }
        for root in &intent.roots {
            let root_key_hash = root.publication.descriptor.root_key_hash();
            if publications.get(&root_key_hash) != Some(&root.publication.descriptor) {
                bail!("CoreMeta idempotent retry changed publication root {root_key_hash}");
            }
        }
        Ok(())
    }

    pub(super) async fn resume_root_publication_intent(
        &self,
        intent: RootPublicationIntent,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        intent.ensure_pending()?;
        if intent.all_outcomes_recorded() {
            let mut guards = Vec::with_capacity(intent.roots.len());
            for root in &intent.roots {
                guards.push(
                    self.acquire_named_lock(
                        "coremeta-root-mutation",
                        &root.publication.descriptor.root_key_hash(),
                    )
                    .await?,
                );
            }
            return self.publish_recorded_root_publication_intent(&intent).await;
        }
        self.ensure_publication_intent_active(&intent).await?;
        if !intent.no_outcomes_recorded() {
            bail!("CoreMeta publication intent has a torn quorum-outcome state");
        }
        let inputs = intent
            .roots
            .iter()
            .map(|root| CoreMetaRootCommitInput {
                root_key_hash: root.publication.descriptor.root_key_hash(),
                expected_root_generation: root.expected_root_generation,
                post_root_generation: root.publication.post_root_generation,
                transaction_id: intent.transaction_id.clone(),
                rows: root.rows.clone(),
                publication: Some(root.publication.clone()),
            })
            .collect();
        self.commit_coremeta_encoded_rows_for_roots(inputs, intent.local_rows.clone())
            .await
    }

    async fn commit_coremeta_encoded_rows_for_roots(
        &self,
        inputs: Vec<CoreMetaRootCommitInput>,
        local_rows: Vec<CoreMetaEncodedOwnedRow>,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        if inputs.is_empty() {
            if !local_rows.is_empty() {
                let borrowed = borrow_encoded_rows(&local_rows);
                self.write_coremeta_encoded_rows(&borrowed)?;
            }
            return Ok(Vec::new());
        }
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;

        let mut prepared = Vec::with_capacity(inputs.len());
        let mut seen_roots = BTreeSet::new();
        for input in inputs {
            validate_hash(&input.root_key_hash, "CoreMeta quorum root key hash")?;
            validate_logical_id(&input.transaction_id, "CoreMeta quorum transaction id")?;
            if input.rows.is_empty() {
                bail!("CoreMeta quorum batch must contain at least one row");
            }
            if input.post_root_generation == 0
                || input.expected_root_generation.checked_add(1) != Some(input.post_root_generation)
            {
                bail!("CoreMeta quorum root generation must advance by exactly one");
            }
            if !seen_roots.insert(input.root_key_hash.clone()) {
                bail!("CoreMeta quorum group contains the same root more than once");
            }
            let mut seen_rows = BTreeSet::new();
            for row in &input.rows {
                if row.root_key_hash != input.root_key_hash {
                    bail!("CoreMeta quorum row root hash does not match batch root");
                }
                if row.root_generation != input.post_root_generation {
                    bail!("CoreMeta quorum row generation does not match batch root generation");
                }
                if !seen_rows.insert((row.cf.as_str(), row.core_meta_key.as_slice())) {
                    bail!("CoreMeta quorum root mutation contains a duplicate physical row");
                }
            }
            let row_hashes = encoded_row_hashes(&input.rows);
            let pending_batch_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
                root_key_hash: input.root_key_hash.clone(),
                expected_root_generation: input.expected_root_generation,
                post_root_generation: input.post_root_generation,
                transaction_id: input.transaction_id.clone(),
                row_hashes: row_hashes.clone(),
            })?;
            prepared.push(PreparedCoreMetaRootCommit {
                input,
                pending_batch_hash,
                row_hashes,
            });
        }

        prepared.sort_by(|left, right| left.input.root_key_hash.cmp(&right.input.root_key_hash));

        // A logical root mutation is already fully prepared at this point.
        // Serialize only mutations targeting the same roots, in canonical root
        // order; unrelated root groups continue concurrently.
        let mut root_guards = Vec::with_capacity(prepared.len());
        for root_key_hash in prepared
            .iter()
            .map(|root| root.input.root_key_hash.as_str())
        {
            root_guards.push(
                self.acquire_named_lock("coremeta-root-mutation", root_key_hash)
                    .await?,
            );
        }

        let publication_intent = if prepared.iter().all(|root| root.input.publication.is_some()) {
            let transaction_id = prepared
                .first()
                .map(|root| root.input.transaction_id.as_str())
                .ok_or_else(|| anyhow!("CoreMeta publication group is empty"))?;
            if prepared
                .iter()
                .any(|root| root.input.transaction_id != transaction_id)
            {
                bail!("CoreMeta publication group spans multiple transaction identities");
            }
            let created_at_unix_nanos = prepared
                .first()
                .and_then(|root| root.input.publication.as_ref())
                .map(|publication| publication.created_at_unix_nanos)
                .ok_or_else(|| anyhow!("CoreMeta publication timestamp is missing"))?;
            let mut plan_roots = Vec::with_capacity(prepared.len());
            let mut intent_roots = Vec::with_capacity(prepared.len());
            for root in &prepared {
                let publication = root
                    .input
                    .publication
                    .clone()
                    .ok_or_else(|| anyhow!("CoreMeta publication descriptor is missing"))?;
                if publication.created_at_unix_nanos != created_at_unix_nanos {
                    bail!("CoreMeta publication group timestamps are inconsistent");
                }
                let manifest_hash = core_meta_encoded_row_hash_with_delete(
                    &publication.transaction_manifest_row.cf,
                    &publication.transaction_manifest_row.core_meta_key,
                    &publication.transaction_manifest_row.value_envelope,
                    publication.transaction_manifest_row.delete_marker,
                );
                let plan_rows = root
                    .input
                    .rows
                    .iter()
                    .filter(|row| {
                        core_meta_encoded_row_hash_with_delete(
                            &row.cf,
                            &row.core_meta_key,
                            &row.value_envelope,
                            row.delete_marker,
                        ) != manifest_hash
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                plan_roots.push((publication.descriptor.clone(), plan_rows));
                intent_roots.push(RootPublicationIntentRoot {
                    ordinal: 0,
                    publication,
                    expected_root_generation: root.input.expected_root_generation,
                    rows: root.input.rows.clone(),
                    certificate_hash: None,
                });
            }
            let plan_hash = root_publication_plan_hash(transaction_id, &plan_roots, &local_rows)?;
            let existing_intent = self.read_root_publication_intent(transaction_id)?;
            let candidate_publisher = existing_intent.as_ref().map_or_else(
                || self.node_identity.node_id.clone(),
                |intent| intent.publisher_node_id.clone(),
            );
            let candidate = build_root_publication_intent(
                transaction_id,
                plan_hash,
                candidate_publisher,
                created_at_unix_nanos,
                intent_roots,
                local_rows.clone(),
            )?;
            let intent = match existing_intent {
                Some(existing) => {
                    if !publication_intent_retry_matches(&existing, &candidate)? {
                        bail!("CoreMeta retry changed its durable publication intent");
                    }
                    existing
                }
                None => {
                    for root in &candidate.roots {
                        let current = self
                            .read_latest_root_anchor(&root.publication.descriptor.root_anchor_key)
                            .await?;
                        let current_generation =
                            current.as_ref().map_or(0, |anchor| anchor.root_generation);
                        let current_hash = current
                            .as_ref()
                            .map(hash_root_anchor_record)
                            .transpose()?
                            .unwrap_or_else(|| ZERO_HASH.to_string());
                        if current_generation != root.expected_root_generation
                            || current_hash != root.publication.previous_root_hash
                        {
                            return Err(CoreStoreCommitError::RootChangedBeforeDurableStaging {
                                root_key_hash: root.publication.descriptor.root_key_hash(),
                                expected_generation: root.expected_root_generation,
                                expected_hash: root.publication.previous_root_hash.clone(),
                                actual_generation: current_generation,
                                actual_hash: current_hash,
                            }
                            .into());
                        }
                    }
                    self.persist_root_publication_intent(&candidate)?;
                    candidate
                }
            };
            if !intent.no_outcomes_recorded() {
                return self.publish_recorded_root_publication_intent(&intent).await;
            }
            Some(intent)
        } else if prepared.iter().any(|root| root.input.publication.is_some()) {
            bail!("CoreMeta quorum group mixes published and evidence-only roots");
        } else {
            None
        };

        let select_started_at = Instant::now();
        let replicas = self.select_coremeta_replicas(&profile).await?;
        crate::emit_test_timing(
            "coremeta.commit_group select_coremeta_replicas",
            select_started_at.elapsed(),
        );

        let prepare_started_at = Instant::now();
        crate::perf::record_counter(
            "anvil_coremeta_commit_group_roots",
            &[("stage", "prepare")],
            prepared.len() as u64,
        );
        let mut prepare_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.replicate_coremeta_batches_locally(
                        replica,
                        &prepared,
                        publication_intent.as_ref(),
                    )
                } else {
                    self.replicate_coremeta_batches_remotely(
                        replica,
                        &prepared,
                        publication_intent.as_ref(),
                    )
                    .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut prepare_receipts = BTreeMap::<String, Vec<CoreMetaPrepareReceipt>>::new();
        let mut prepare_errors = Vec::new();
        while let Some((node_id, result)) = prepare_results.next().await {
            match result.and_then(|receipts| {
                self.validate_prepare_receipt_group(&node_id, &prepared, &receipts)?;
                Ok(receipts)
            }) {
                Ok(receipts) => {
                    for receipt in receipts {
                        prepare_receipts
                            .entry(receipt.root_key_hash.clone())
                            .or_default()
                            .push(receipt);
                    }
                }
                Err(error) => prepare_errors.push(format!("{node_id}: {error:#}")),
            }
            if prepared.iter().all(|root| {
                prepare_receipts
                    .get(&root.input.root_key_hash)
                    .is_some_and(|receipts| receipts.len() >= profile.prepare_quorum)
            }) {
                break;
            }
        }
        drop(prepare_results);
        for root in &prepared {
            let count = prepare_receipts
                .get(&root.input.root_key_hash)
                .map_or(0, Vec::len);
            if count < profile.prepare_quorum {
                record_corestore_trace_event("coremeta.replicate_pending", "quorum_failed");
                record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
                crate::perf::record_duration(
                    "anvil_coremeta_replication_duration_ms",
                    &[
                        ("operation", "replicate_pending_group"),
                        ("root_kind", "root_group"),
                        ("profile", profile.profile_id.as_str()),
                        ("status", "quorum_failed"),
                    ],
                    prepare_started_at.elapsed(),
                );
                return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                    operation: "prepare",
                    required: profile.prepare_quorum,
                    received: count,
                    details: format!(
                        "root={}: {}",
                        root.input.root_key_hash,
                        prepare_errors.join("; ")
                    ),
                }
                .into());
            }
        }
        crate::emit_test_timing(
            "coremeta.commit_group replicate_pending_quorum",
            prepare_started_at.elapsed(),
        );
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "replicate_pending_group"),
                ("root_kind", "root_group"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            prepare_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "prepare_ok"),
            ],
            prepared.len() as u64,
        );

        let mut certified = Vec::with_capacity(prepared.len());
        for root in prepared {
            let root_receipts = prepare_receipts
                .remove(&root.input.root_key_hash)
                .unwrap_or_default();
            let certificate = build_commit_certificate(
                &profile,
                root.input.root_key_hash.clone(),
                root.input.expected_root_generation,
                root.input.post_root_generation,
                root.input.transaction_id.clone(),
                root.pending_batch_hash.clone(),
                root_receipts,
            )?;
            let committed_batch_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
                root_key_hash: root.input.root_key_hash.clone(),
                expected_root_generation: root.input.expected_root_generation,
                post_root_generation: root.input.post_root_generation,
                transaction_id: root.input.transaction_id.clone(),
                pending_batch_hash: root.pending_batch_hash.clone(),
                committed_row_hashes: root.row_hashes.clone(),
            })?;
            certified.push(CertifiedCoreMetaRootCommit {
                prepared: root,
                certificate,
                committed_batch_hash,
            });
        }

        let persist_started_at = Instant::now();
        crate::perf::record_counter(
            "anvil_coremeta_commit_group_roots",
            &[("stage", "persist")],
            certified.len() as u64,
        );
        let mut persist_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.persist_coremeta_certificates_locally(
                        replica,
                        &certified,
                        publication_intent.as_ref(),
                    )
                } else {
                    self.persist_coremeta_certificates_remotely(replica, &certified)
                        .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut persist_receipts =
            BTreeMap::<String, Vec<CoreMetaCertificatePersistReceipt>>::new();
        let mut persist_errors = Vec::new();
        while let Some((node_id, result)) = persist_results.next().await {
            match result.and_then(|receipts| {
                self.validate_persist_receipt_group(&node_id, &certified, &receipts)?;
                Ok(receipts)
            }) {
                Ok(receipts) => {
                    for receipt in receipts {
                        persist_receipts
                            .entry(receipt.root_key_hash.clone())
                            .or_default()
                            .push(receipt);
                    }
                }
                Err(error) => persist_errors.push(format!("{node_id}: {error:#}")),
            }
            if certified.iter().all(|root| {
                persist_receipts
                    .get(&root.prepared.input.root_key_hash)
                    .is_some_and(|receipts| receipts.len() >= profile.certificate_persist_quorum)
            }) {
                break;
            }
        }
        drop(persist_results);
        for root in &certified {
            let count = persist_receipts
                .get(&root.prepared.input.root_key_hash)
                .map_or(0, Vec::len);
            if count < profile.certificate_persist_quorum {
                record_corestore_trace_event(
                    "coremeta.persist_commit_certificate",
                    "quorum_failed",
                );
                record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
                crate::perf::record_duration(
                    "anvil_coremeta_replication_duration_ms",
                    &[
                        ("operation", "persist_commit_group"),
                        ("root_kind", "root_group"),
                        ("profile", profile.profile_id.as_str()),
                        ("status", "quorum_failed"),
                    ],
                    persist_started_at.elapsed(),
                );
                return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                    operation: "certificate_persistence",
                    required: profile.certificate_persist_quorum,
                    received: count,
                    details: format!(
                        "root={}: {}",
                        root.prepared.input.root_key_hash,
                        persist_errors.join("; ")
                    ),
                }
                .into());
            }
        }
        crate::emit_test_timing(
            "coremeta.commit_group persist_commit_certificate_quorum",
            persist_started_at.elapsed(),
        );
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "persist_commit_group"),
                ("root_kind", "root_group"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            persist_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "certificate_persist_ok"),
            ],
            certified.len() as u64,
        );

        let evidence_started_at = Instant::now();
        let mut evidence_rows = Vec::with_capacity(certified.len());
        let mut outcomes = Vec::with_capacity(certified.len());
        for root in certified {
            let receipts = persist_receipts
                .remove(&root.prepared.input.root_key_hash)
                .unwrap_or_default();
            validate_commit_evidence_with_verifier(
                &profile,
                &root.certificate,
                &receipts,
                |node_id, signed_payload_hash, signature| {
                    self.verify_internal_core_receipt_signature(
                        node_id,
                        signed_payload_hash,
                        signature,
                    )
                },
            )?;
            let certificate_bytes =
                encode_deterministic_proto(&core_commit_certificate_to_api(&root.certificate));
            let persist_receipt_bytes = receipts
                .iter()
                .map(|receipt| encode_deterministic_proto(&core_persist_receipt_to_api(receipt)))
                .collect::<Vec<_>>();
            let mut persist_receipt_hashes = receipts
                .iter()
                .map(certificate_persist_receipt_payload_hash)
                .collect::<Result<Vec<_>>>()?;
            persist_receipt_hashes.sort();
            persist_receipt_hashes.dedup();
            let evidence_created_at = publication_intent
                .as_ref()
                .map_or_else(unix_timestamp_nanos, |intent| intent.created_at_unix_nanos);
            evidence_rows.push(self.coremeta_commit_evidence_encoded_row_at(
                &root.prepared.input.root_key_hash,
                root.prepared.input.post_root_generation,
                &root.prepared.input.transaction_id,
                &root.certificate.certificate_hash,
                &root.committed_batch_hash,
                certificate_bytes.clone(),
                persist_receipt_hashes.clone(),
                persist_receipt_bytes,
                evidence_created_at,
            )?);
            let outcome = CoreMetaQuorumCommitOutcome {
                root_key_hash: root.prepared.input.root_key_hash,
                post_root_generation: root.prepared.input.post_root_generation,
                certificate_hash: root.certificate.certificate_hash,
                committed_batch_hash: root.committed_batch_hash,
                certificate_bytes,
                certificate_persist_receipt_hashes: persist_receipt_hashes,
                certificate_persist_receipts: receipts,
                metadata_replica_node_ids: replicas
                    .iter()
                    .take(profile.replica_count)
                    .map(|replica| replica.node_id.clone())
                    .collect(),
            };
            outcomes.push(outcome);
        }
        if let Some(intent) = publication_intent {
            let recorded = self.record_root_publication_outcomes(&intent, &outcomes)?;
            self.publish_recorded_root_publication_intent(&recorded)
                .await?;
        } else {
            self.write_coremeta_encoded_rows(&borrow_encoded_rows(&evidence_rows))?;
            if !local_rows.is_empty() {
                self.write_coremeta_encoded_rows(&borrow_encoded_rows(&local_rows))?;
            }
        }
        crate::emit_test_timing(
            "coremeta.commit_group persist_commit_evidence",
            evidence_started_at.elapsed(),
        );
        Ok(outcomes)
    }

    async fn publish_recorded_root_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        if intent.publisher_node_id == self.node_identity.node_id {
            self.publish_root_publication_intent(intent).await
        } else {
            self.publish_foreign_single_root_publication_intent(intent)
                .await
        }
    }

    pub(super) async fn select_coremeta_replicas(
        &self,
        profile: &CoreMetaQuorumProfile,
    ) -> Result<Vec<LocalShardPlacement>> {
        let canonical_activation =
            mesh_lifecycle::canonical_topology_activation_with_core_store(self)?;
        if let Some(activation) = canonical_activation.as_ref()
            && activation.mesh_id != self.node_identity.mesh_id
        {
            bail!("canonical topology activation belongs to a different mesh");
        }
        let mut candidates = self.active_coremeta_lifecycle_replicas(profile.prepare_quorum)?;

        if canonical_activation.is_none() && candidates.len() < profile.prepare_quorum {
            // During cluster genesis the mesh lifecycle rows that nominate
            // metadata replicas do not exist yet. Use the local bootstrap
            // control set only until immutable canonical activation evidence is
            // atomically installed with the lifecycle projection.
            candidates = local_control_node_ids()
                .into_iter()
                .enumerate()
                .map(|(index, node_id)| {
                    let cell_id = format!("local-control-cell-{}", index + 1);
                    LocalShardPlacement {
                        is_local: true,
                        node_id,
                        region_id: self.node_identity.region_id.clone(),
                        cell_id: cell_id.clone(),
                        failure_domain: cell_id,
                        region_weight: 100,
                        cell_weight: 100,
                        public_api_addr: String::new(),
                    }
                })
                .collect::<Vec<_>>();
        }

        if canonical_activation.is_some() && candidates.len() < profile.replica_count {
            return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                operation: "canonical_replica_selection",
                required: profile.replica_count,
                received: candidates.len(),
                details: format!(
                    "profile={} canonical topology permanently forbids synthetic fallback",
                    profile.profile_id
                ),
            }
            .into());
        }

        if candidates.len() < profile.prepare_quorum {
            return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                operation: "replica_selection",
                required: profile.prepare_quorum,
                received: candidates.len(),
                details: format!("profile={}", profile.profile_id),
            }
            .into());
        }
        let planned_replicas = candidates.len().min(profile.replica_count);
        Ok(choose_spread_placements(
            LocalErasureProfile {
                id: "metadata-r3-q2",
                codec_id: "logical-coremeta-r3-q2",
                data_shards: planned_replicas,
                parity_shards: 0,
                minimum_read_shards: profile.prepare_quorum,
                minimum_write_ack_shards: profile.prepare_quorum,
                logical_block_target_bytes: 0,
                max_shard_size_bytes: 0,
            },
            candidates,
            &[],
        )?)
    }

    pub(super) fn active_coremeta_lifecycle_replicas(
        &self,
        _prepare_quorum: usize,
    ) -> Result<Vec<LocalShardPlacement>> {
        let nodes = mesh_lifecycle::list_node_projections_with_core_store(self, None, None)?;
        let mut active_candidates = Vec::new();
        for node in nodes {
            if node.mesh_id != self.node_identity.mesh_id {
                continue;
            }
            if node.region != self.node_identity.region_id {
                continue;
            }
            if node.state != LifecycleState::Active {
                continue;
            }
            if !node.capabilities.contains(&NodeCapability::Metadata) {
                continue;
            }
            if node.public_api_addr.trim().is_empty() {
                continue;
            }
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key,
            )?;
            let is_local = node.node_id == self.node_identity.node_id;
            let placement = LocalShardPlacement {
                is_local,
                node_id: node.node_id,
                region_id: node.region,
                cell_id: node.cell_id.clone(),
                failure_domain: node.cell_id,
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: node.public_api_addr,
            };
            active_candidates.push(placement);
        }
        Self::sort_coremeta_candidates(&mut active_candidates);
        Ok(active_candidates)
    }

    fn sort_coremeta_candidates(candidates: &mut [LocalShardPlacement]) {
        candidates.sort_by(|a, b| {
            b.region_weight
                .cmp(&a.region_weight)
                .then_with(|| b.cell_weight.cmp(&a.cell_weight))
                .then_with(|| a.region_id.cmp(&b.region_id))
                .then_with(|| a.failure_domain.cmp(&b.failure_domain))
                .then_with(|| a.cell_id.cmp(&b.cell_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
    }

    fn replicate_coremeta_batches_locally(
        &self,
        replica: &LocalShardPlacement,
        roots: &[PreparedCoreMetaRootCommit],
        publication_intent: Option<&RootPublicationIntent>,
    ) -> Result<Vec<CoreMetaPrepareReceipt>> {
        if let Some(intent) = publication_intent {
            if !self
                .validate_persisted_root_publication_intent_summary(intent)
                .context("validate locally staged CoreMeta publication intent")?
            {
                bail!("CoreMeta publication intent was not staged locally");
            }
        }
        let marker_rows = roots
            .iter()
            .map(|root| {
                self.coremeta_pending_batch_marker_encoded_row(
                    &root.input.root_key_hash,
                    root.input.expected_root_generation,
                    root.input.post_root_generation,
                    &root.input.transaction_id,
                    &root.pending_batch_hash,
                    root.input.rows.len(),
                )
                .with_context(|| {
                    format!(
                        "encode CoreMeta pending batch marker for root {} generation {}",
                        root.input.root_key_hash, root.input.post_root_generation
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&marker_rows))
            .context("persist CoreMeta pending batch markers")?;
        roots
            .iter()
            .map(|root| {
                let mut receipt = CoreMetaPrepareReceipt {
                    replica_node_id: replica.node_id.clone(),
                    write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                    pending_batch_hash: root.pending_batch_hash.clone(),
                    root_key_hash: root.input.root_key_hash.clone(),
                    expected_root_generation: root.input.expected_root_generation,
                    post_root_generation: root.input.post_root_generation,
                    transaction_id: root.input.transaction_id.clone(),
                    signed_payload_hash: String::new(),
                    signature: Vec::new(),
                };
                receipt.signed_payload_hash = prepare_receipt_payload_hash(&receipt)
                    .context("hash CoreMeta prepare receipt payload")?;
                receipt.signature = self
                    .sign_internal_core_receipt(&receipt.signed_payload_hash)
                    .context("sign CoreMeta prepare receipt")?;
                Ok(receipt)
            })
            .collect()
    }

    async fn replicate_coremeta_batches_remotely(
        &self,
        replica: &LocalShardPlacement,
        roots: &[PreparedCoreMetaRootCommit],
        publication_intent: Option<&RootPublicationIntent>,
    ) -> Result<Vec<CoreMetaPrepareReceipt>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaBatchGroupRequest {
            header: Some(self.internal_request_header("coremeta.replicate_pending_batches")?),
            batches: roots
                .iter()
                .map(|root| CoreMetaBatchRequest {
                    header: None,
                    root_key_hash: root.input.root_key_hash.clone(),
                    expected_root_generation: root.input.expected_root_generation,
                    post_root_generation: root.input.post_root_generation,
                    transaction_id: root.input.transaction_id.clone(),
                    visibility_state: "pending".to_string(),
                    mutations: rows_to_api_mutations(&root.input.rows),
                    pending_batch_hash: root.pending_batch_hash.clone(),
                })
                .collect(),
            publication_intent: publication_intent
                .map(|intent| self.encode_replica_root_publication_intent(intent))
                .transpose()?
                .unwrap_or_default(),
        };
        let response = self
            .coremeta_stream_request(
                &replica.public_api_addr,
                bearer,
                "replicate CoreMeta batches",
                CoreMetaStreamRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    command: Some(core_meta_stream_request::Command::ReplicatePendingBatches(
                        request_body,
                    )),
                },
            )
            .await
            .with_context(|| format!("replicate CoreMeta batches to {}", replica.node_id))?;
        match response.result {
            Some(core_meta_stream_response::Result::PrepareReceipts(group)) => group
                .receipts
                .into_iter()
                .map(api_prepare_receipt_to_core)
                .collect(),
            _ => bail!("CoreMeta stream returned unexpected response for prepare request"),
        }
    }

    fn validate_prepare_receipt_group(
        &self,
        replica_node_id: &str,
        roots: &[PreparedCoreMetaRootCommit],
        receipts: &[CoreMetaPrepareReceipt],
    ) -> Result<()> {
        if receipts.len() != roots.len() {
            bail!("CoreMeta prepare receipt group cardinality mismatch");
        }
        let expected = roots
            .iter()
            .map(|root| (root.input.root_key_hash.as_str(), root))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        for receipt in receipts {
            let root = expected
                .get(receipt.root_key_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta prepare receipt references unknown root"))?;
            if !seen.insert(receipt.root_key_hash.as_str())
                || receipt.replica_node_id != replica_node_id
                || receipt.pending_batch_hash != root.pending_batch_hash
                || receipt.expected_root_generation != root.input.expected_root_generation
                || receipt.post_root_generation != root.input.post_root_generation
                || receipt.transaction_id != root.input.transaction_id
            {
                bail!("CoreMeta prepare receipt does not match its root batch");
            }
            self.verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.signed_payload_hash,
                &receipt.signature,
            )?;
        }
        Ok(())
    }

    fn persist_coremeta_certificates_locally(
        &self,
        replica: &LocalShardPlacement,
        roots: &[CertifiedCoreMetaRootCommit],
        publication_intent: Option<&RootPublicationIntent>,
    ) -> Result<Vec<CoreMetaCertificatePersistReceipt>> {
        let mut receipts = Vec::with_capacity(roots.len());
        let mut evidence_rows = Vec::with_capacity(roots.len());
        for root in roots {
            let mut receipt = CoreMetaCertificatePersistReceipt {
                replica_node_id: replica.node_id.clone(),
                write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                certificate_hash: root.certificate.certificate_hash.clone(),
                committed_batch_hash: root.committed_batch_hash.clone(),
                root_key_hash: root.prepared.input.root_key_hash.clone(),
                post_root_generation: root.prepared.input.post_root_generation,
                transaction_id: root.prepared.input.transaction_id.clone(),
                signed_payload_hash: String::new(),
                signature: Vec::new(),
            };
            receipt.signed_payload_hash = certificate_persist_receipt_payload_hash(&receipt)?;
            receipt.signature = self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
            let created_at_unix_nanos = publication_intent
                .map_or_else(unix_timestamp_nanos, |intent| intent.created_at_unix_nanos);
            evidence_rows.push(self.coremeta_commit_evidence_encoded_row_at(
                &root.prepared.input.root_key_hash,
                root.prepared.input.post_root_generation,
                &root.prepared.input.transaction_id,
                &root.certificate.certificate_hash,
                &root.committed_batch_hash,
                encode_deterministic_proto(&core_commit_certificate_to_api(&root.certificate)),
                vec![certificate_persist_receipt_payload_hash(&receipt)?],
                vec![encode_deterministic_proto(&core_persist_receipt_to_api(
                    &receipt,
                ))],
                created_at_unix_nanos,
            )?);
            receipts.push(receipt);
        }
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&evidence_rows))?;
        Ok(receipts)
    }

    async fn persist_coremeta_certificates_remotely(
        &self,
        replica: &LocalShardPlacement,
        roots: &[CertifiedCoreMetaRootCommit],
    ) -> Result<Vec<CoreMetaCertificatePersistReceipt>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaPersistCommitGroupRequest {
            header: Some(self.internal_request_header("coremeta.persist_commit_certificates")?),
            commits: roots
                .iter()
                .map(|root| CoreMetaPersistCommitRequest {
                    header: None,
                    commit_certificate: Some(core_commit_certificate_to_api(&root.certificate)),
                    committed_rows: rows_to_api_mutations(&root.prepared.input.rows),
                    committed_batch_hash: root.committed_batch_hash.clone(),
                })
                .collect(),
        };
        let response = self
            .coremeta_stream_request(
                &replica.public_api_addr,
                bearer,
                "persist CoreMeta certificates",
                CoreMetaStreamRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    command: Some(
                        core_meta_stream_request::Command::PersistCommitCertificates(request_body),
                    ),
                },
            )
            .await
            .with_context(|| format!("persist CoreMeta certificates on {}", replica.node_id))?;
        match response.result {
            Some(core_meta_stream_response::Result::CertificatePersistReceipts(group)) => group
                .receipts
                .into_iter()
                .map(api_persist_receipt_to_core)
                .collect(),
            _ => bail!("CoreMeta stream returned unexpected response for certificate request"),
        }
    }

    fn validate_persist_receipt_group(
        &self,
        replica_node_id: &str,
        roots: &[CertifiedCoreMetaRootCommit],
        receipts: &[CoreMetaCertificatePersistReceipt],
    ) -> Result<()> {
        if receipts.len() != roots.len() {
            bail!("CoreMeta persist receipt group cardinality mismatch");
        }
        let expected = roots
            .iter()
            .map(|root| (root.prepared.input.root_key_hash.as_str(), root))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        for receipt in receipts {
            let root = expected
                .get(receipt.root_key_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta persist receipt references unknown root"))?;
            if !seen.insert(receipt.root_key_hash.as_str())
                || receipt.replica_node_id != replica_node_id
                || receipt.certificate_hash != root.certificate.certificate_hash
                || receipt.committed_batch_hash != root.committed_batch_hash
                || receipt.post_root_generation != root.prepared.input.post_root_generation
                || receipt.transaction_id != root.prepared.input.transaction_id
            {
                bail!("CoreMeta persist receipt does not match its root certificate");
            }
            self.verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.signed_payload_hash,
                &receipt.signature,
            )?;
        }
        Ok(())
    }
}

fn order_coremeta_peer_targets(root_key_hash: &str, targets: &mut [CoreMetaPeerTarget]) {
    targets.sort_by(|left, right| {
        coremeta_peer_target_score(root_key_hash, &right.node_id)
            .cmp(&coremeta_peer_target_score(root_key_hash, &left.node_id))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
}

fn coremeta_peer_target_score(root_key_hash: &str, node_id: &str) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(root_key_hash.len() + node_id.len() + 32);
    for part in ["anvil.coremeta.peer_route.v1", root_key_hash, node_id] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    Sha256::digest(&bytes).into()
}

fn encoded_row_hashes(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<String> {
    let mut hashes = rows
        .iter()
        .map(|row| {
            core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            )
        })
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    hashes
}

fn borrow_encoded_rows(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaEncodedRow<'_>> {
    rows.iter()
        .map(|row| CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        })
        .collect()
}

fn rows_to_api_mutations(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaRowMutation> {
    rows.iter()
        .map(|row| CoreMetaRowMutation {
            column_family: row.cf.clone(),
            core_meta_key: row.core_meta_key.clone(),
            value_envelope: row.value_envelope.clone(),
            row_hash: core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            ),
            delete_marker: row.delete_marker,
        })
        .collect()
}

pub(super) fn core_commit_certificate_to_api(
    certificate: &CoreMetaCommitCertificate,
) -> ApiCoreMetaCommitCertificate {
    ApiCoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        pending_batch_hash: certificate.pending_batch_hash.clone(),
        prepare_receipts: certificate
            .prepare_receipts
            .iter()
            .map(core_prepare_receipt_to_api)
            .collect(),
        certificate_hash: certificate.certificate_hash.clone(),
    }
}

fn core_prepare_receipt_to_api(
    receipt: &CoreMetaPrepareReceipt,
) -> crate::anvil_api::CoreMetaPrepareReceipt {
    crate::anvil_api::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signature: receipt.signature.clone(),
    }
}

pub(super) fn api_prepare_receipt_to_core(
    receipt: crate::anvil_api::CoreMetaPrepareReceipt,
) -> Result<CoreMetaPrepareReceipt> {
    let mut core = CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash,
        root_key_hash: receipt.root_key_hash,
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signed_payload_hash: String::new(),
        signature: receipt.signature,
    };
    core.signed_payload_hash = prepare_receipt_payload_hash(&core)?;
    Ok(core)
}

pub(super) fn api_commit_certificate_to_core(
    certificate: crate::anvil_api::CoreMetaCommitCertificate,
) -> Result<CoreMetaCommitCertificate> {
    let prepare_receipts = certificate
        .prepare_receipts
        .into_iter()
        .map(api_prepare_receipt_to_core)
        .collect::<Result<Vec<_>>>()?;
    Ok(CoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash,
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id,
        pending_batch_hash: certificate.pending_batch_hash,
        prepare_receipts,
        certificate_hash: certificate.certificate_hash,
    })
}

pub(super) fn api_persist_receipt_to_core(
    receipt: crate::anvil_api::CoreMetaCertificatePersistReceipt,
) -> Result<CoreMetaCertificatePersistReceipt> {
    let mut core = CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash,
        committed_batch_hash: receipt.committed_batch_hash,
        root_key_hash: receipt.root_key_hash,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signed_payload_hash: String::new(),
        signature: receipt.signature,
    };
    core.signed_payload_hash = certificate_persist_receipt_payload_hash(&core)?;
    Ok(core)
}

pub(super) fn core_persist_receipt_to_api(
    receipt: &CoreMetaCertificatePersistReceipt,
) -> crate::anvil_api::CoreMetaCertificatePersistReceipt {
    crate::anvil_api::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash.clone(),
        committed_batch_hash: receipt.committed_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signature: receipt.signature.clone(),
    }
}

pub(super) fn normalise_grpc_endpoint(addr: &str) -> Result<String> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        bail!("CoreMeta replica endpoint must not be empty");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

#[cfg(test)]
mod coremeta_peer_route_tests {
    use super::*;

    #[test]
    fn peer_target_order_is_stable_and_root_scoped() {
        let original = vec![
            CoreMetaPeerTarget {
                node_id: "node-a".to_string(),
                public_api_addr: "http://node-a".to_string(),
            },
            CoreMetaPeerTarget {
                node_id: "node-b".to_string(),
                public_api_addr: "http://node-b".to_string(),
            },
            CoreMetaPeerTarget {
                node_id: "node-c".to_string(),
                public_api_addr: "http://node-c".to_string(),
            },
        ];
        let mut first = original.clone();
        let mut second = original;
        order_coremeta_peer_targets("sha256:root", &mut first);
        order_coremeta_peer_targets("sha256:root", &mut second);
        assert_eq!(first, second);
        assert_eq!(
            first
                .iter()
                .map(|target| target.node_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }
}
