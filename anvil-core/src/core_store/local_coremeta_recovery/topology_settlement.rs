use super::super::*;
use super::register_quarantine::{
    RootRegisterQuarantineIntent, quarantine_synthetic_root_register,
    resume_root_register_quarantine_intents, synthetic_root_register_inventory,
};
use super::register_quorum::RootRegisterQuorumResolution;
use super::{RECOVERY_MAX_OPERATIONS_PER_ROUND, RecoveryPeer, RecoveryRound};
use crate::mesh_lifecycle::CanonicalTopologyActivation;
use prost::Message;

const SETTLEMENT_MARKER_SCHEMA: &str = "anvil.core.canonical_topology_settlement.v1";
const SETTLEMENT_SCAN_PAGE_ROWS: usize = 256;

#[derive(Clone, PartialEq, Message)]
struct CanonicalTopologySettlementMarkerProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    activation_topology_hash: String,
    #[prost(string, tag = "4")]
    root_key_hash: String,
    #[prost(uint64, tag = "5")]
    previous_generation: u64,
    #[prost(uint64, tag = "6")]
    settlement_generation: u64,
}

#[derive(Debug, Clone)]
pub(super) struct CurrentRoot {
    anchor: CoreRootAnchorRecord,
    coordinator_scope: (String, u64),
}

#[derive(Debug, Default)]
pub(super) struct CanonicalSettlementScanState {
    activation_topology_hash: Option<String>,
    after_tuple_key: Option<Vec<u8>>,
    roots: BTreeMap<String, CurrentRoot>,
    scan_complete: bool,
    verification_index: usize,
    physical_q2_failed: bool,
    // Carry this across operation-budget boundaries and reset once the pass completes.
    settlement_requires_rescan: bool,
    completed_activation_topology_hash: Option<String>,
}

impl CanonicalSettlementScanState {
    fn prepare_for_activation(&mut self, topology_hash: &str) {
        if self.activation_topology_hash.as_deref() != Some(topology_hash) {
            self.reset_scan();
            self.activation_topology_hash = Some(topology_hash.to_string());
        }
    }

    fn reset_scan(&mut self) {
        self.after_tuple_key = None;
        self.roots.clear();
        self.scan_complete = false;
        self.verification_index = 0;
        self.physical_q2_failed = false;
        self.settlement_requires_rescan = false;
    }

    fn mark_complete(&mut self, topology_hash: &str) {
        self.completed_activation_topology_hash = Some(topology_hash.to_string());
        self.reset_scan();
        self.activation_topology_hash = Some(topology_hash.to_string());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SettlementComponent {
    coordinator_root_key_hash: String,
    root_key_hashes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonicalSettlementPassOutcome {
    Incomplete,
    RequiresRescan,
    Complete,
}

impl CoreStore {
    pub(super) async fn reconcile_canonical_topology_registers(
        &self,
        peers: &[RecoveryPeer],
        round: &mut RecoveryRound,
    ) -> Result<()> {
        let activation =
            crate::mesh_lifecycle::canonical_topology_activation_with_core_store(self)?;
        resume_root_register_quarantine_intents(
            self,
            activation
                .as_ref()
                .map(|activation| (activation.topology_hash.as_str(), activation.generation)),
        )
        .await?;

        let Some(activation) = activation else {
            let profile = self.default_coremeta_quorum_profile()?;
            if self
                .active_coremeta_lifecycle_replicas(profile.prepare_quorum)?
                .len()
                >= profile.replica_count
            {
                bail!(
                    "canonical metadata topology is present without immutable activation evidence"
                );
            }
            round.canonical_settlement_complete = true;
            round.physical_register_quorum_complete = true;
            return Ok(());
        };

        if self
            .coremeta_recovery
            .canonical_settlement
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .completed_activation_topology_hash
            .as_deref()
            == Some(activation.topology_hash.as_str())
        {
            round.canonical_settlement_complete = true;
            round.physical_register_quorum_complete = true;
            return Ok(());
        }
        if !self.advance_canonical_settlement_scan(&activation)? {
            self.mark_coremeta_recovery_unready();
            return Ok(());
        }

        let (roots, mut verification_index, mut physical_q2_failed, mut settlement_requires_rescan) = {
            let state = self
                .coremeta_recovery
                .canonical_settlement
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                state.roots.clone(),
                state.verification_index,
                state.physical_q2_failed,
                state.settlement_requires_rescan,
            )
        };
        let mut scopes = roots
            .values()
            .map(|root| root.coordinator_scope.clone())
            .collect::<BTreeSet<_>>();
        scopes.retain(|(_, generation)| *generation > 0);
        let scopes = scopes.into_iter().collect::<Vec<_>>();
        while verification_index < scopes.len()
            && round.operations < RECOVERY_MAX_OPERATIONS_PER_ROUND
        {
            let (root_key_hash, generation) = &scopes[verification_index];
            match self.root_anchor_at_generation(root_key_hash, *generation)? {
                Some(anchor) => {
                    let anchor_bytes = encode_root_anchor_record(&anchor)?;
                    let physical_q2 = self
                        .resolve_root_register_quorum(
                            peers,
                            Some(&activation.metadata_node_ids),
                            root_key_hash,
                            *generation,
                            &anchor_bytes,
                        )
                        .await?;
                    round.operations = round.operations.saturating_add(1);
                    match physical_q2 {
                        RootRegisterQuorumResolution::Committed => {}
                        RootRegisterQuorumResolution::CommittedConflict { .. } => {
                            bail!(
                                "canonical root history conflicts with a committed root-register quorum: root={root_key_hash} generation={generation}"
                            );
                        }
                        RootRegisterQuorumResolution::DefinitivelyAbsent
                            if anchor.created_at_unix_nanos
                                < activation.activated_at_unix_nanos =>
                        {
                            let historical_component =
                                settlement_component_for_scope(&roots, root_key_hash, *generation)?;
                            match self
                                .local_canonical_settlement_component(&historical_component, &roots)
                                .await?
                            {
                                Some(component) => {
                                    self.publish_canonical_topology_settlement(
                                        &activation,
                                        &component,
                                        &roots,
                                    )
                                    .await?;
                                    round.durable_progress = true;
                                }
                                None => {
                                    tracing::debug!(
                                        historical_transaction_root = %historical_component.coordinator_root_key_hash,
                                        "waiting for canonical topology settlement owners"
                                    );
                                }
                            }
                            // Components are disjoint in this snapshot; rescan after the full pass.
                            settlement_requires_rescan = true;
                        }
                        RootRegisterQuorumResolution::DefinitivelyAbsent
                        | RootRegisterQuorumResolution::Indeterminate => {
                            physical_q2_failed = true;
                        }
                    }
                }
                None => {
                    physical_q2_failed = true;
                    round.operations = round.operations.saturating_add(1);
                }
            }
            verification_index += 1;
            let mut state = self
                .coremeta_recovery
                .canonical_settlement
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.verification_index = verification_index;
            state.physical_q2_failed = physical_q2_failed;
            state.settlement_requires_rescan = settlement_requires_rescan;
        }

        match canonical_settlement_pass_outcome(
            verification_index,
            scopes.len(),
            physical_q2_failed,
            settlement_requires_rescan,
        ) {
            CanonicalSettlementPassOutcome::Incomplete => {
                self.mark_coremeta_recovery_unready();
                return Ok(());
            }
            CanonicalSettlementPassOutcome::RequiresRescan => {
                self.reset_canonical_settlement_scan(&activation.topology_hash);
                self.mark_coremeta_recovery_unready();
                return Ok(());
            }
            CanonicalSettlementPassOutcome::Complete => {}
        }
        if !self
            .quarantine_orphaned_synthetic_generations(&activation, round)
            .await?
        {
            self.mark_coremeta_recovery_unready();
            return Ok(());
        }
        self.coremeta_recovery
            .canonical_settlement
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .mark_complete(&activation.topology_hash);
        round.canonical_settlement_complete = true;
        round.physical_register_quorum_complete = true;
        Ok(())
    }

    fn advance_canonical_settlement_scan(
        &self,
        activation: &CanonicalTopologyActivation,
    ) -> Result<bool> {
        let mut state = self
            .coremeta_recovery
            .canonical_settlement
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.prepare_for_activation(&activation.topology_hash);
        if state.scan_complete {
            return Ok(true);
        }
        let page = self.meta.scan_prefix_page(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &[],
            state.after_tuple_key.as_deref(),
            SETTLEMENT_SCAN_PAGE_ROWS,
        )?;
        if page.is_empty() {
            state.scan_complete = true;
            return Ok(true);
        }
        for record in &page {
            let tuple_key = core_meta_record_tuple_key(&record.key)?;
            let anchor = decode_root_cache_row(&record.payload)?;
            if tuple_key != root_cache_hash_key(&anchor.root_key_hash)
                || anchor.root_generation == 0
            {
                continue;
            }
            let manifest = self.read_root_transaction_manifest(&anchor)?;
            let coordinator_scope = match (
                manifest.coordinator_root_key_hash,
                manifest.coordinator_root_generation,
            ) {
                (Some(root_key_hash), Some(generation)) => (root_key_hash, generation),
                (None, None) => (anchor.root_key_hash.clone(), anchor.root_generation),
                _ => bail!("CoreMeta root manifest has incomplete coordinator scope"),
            };
            state.roots.insert(
                anchor.root_key_hash.clone(),
                CurrentRoot {
                    anchor,
                    coordinator_scope,
                },
            );
        }
        state.after_tuple_key = page
            .last()
            .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
            .transpose()?;
        state.scan_complete = page.len() < SETTLEMENT_SCAN_PAGE_ROWS;
        Ok(state.scan_complete)
    }

    fn reset_canonical_settlement_scan(&self, topology_hash: &str) {
        let mut state = self
            .coremeta_recovery
            .canonical_settlement
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.reset_scan();
        state.activation_topology_hash = Some(topology_hash.to_string());
    }

    fn current_root_anchor(&self, root_key_hash: &str) -> Result<Option<CoreRootAnchorRecord>> {
        let payload = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_hash_key(root_key_hash),
        )?;
        payload
            .map(|payload| decode_root_cache_row(&payload))
            .transpose()
    }

    fn root_anchor_at_generation(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let payload = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_key(root_key_hash, generation),
        )?;
        match payload {
            Some(payload) => decode_root_cache_row(&payload).map(Some),
            None => Ok(self
                .current_root_anchor(root_key_hash)?
                .filter(|anchor| anchor.root_generation == generation)),
        }
    }

    async fn local_canonical_settlement_component(
        &self,
        component: &SettlementComponent,
        roots: &BTreeMap<String, CurrentRoot>,
    ) -> Result<Option<SettlementComponent>> {
        let mut local_roots = BTreeSet::new();
        let mut settlement_owners = BTreeMap::new();
        for root_key_hash in &component.root_key_hashes {
            let current_owner_node_id = &roots
                .get(root_key_hash)
                .ok_or_else(|| anyhow!("settlement component root is missing"))?
                .anchor
                .publisher_node_id;
            let settlement_owner = match settlement_owners.get(current_owner_node_id).cloned() {
                Some(owner) => owner,
                None => {
                    let owner = self
                        .canonical_settlement_owner(current_owner_node_id)
                        .await?;
                    settlement_owners.insert(current_owner_node_id.clone(), owner.clone());
                    owner
                }
            };
            if settlement_owner == self.node_identity.node_id {
                local_roots.insert(root_key_hash.clone());
            }
        }
        Ok(settlement_component_for_local_roots(
            component,
            &local_roots,
        ))
    }

    async fn publish_canonical_topology_settlement(
        &self,
        activation: &CanonicalTopologyActivation,
        component: &SettlementComponent,
        roots: &BTreeMap<String, CurrentRoot>,
    ) -> Result<()> {
        let transaction_id = settlement_transaction_id(activation, component, roots)?;
        let mut tuple_keys = Vec::with_capacity(component.root_key_hashes.len());
        let mut payloads = Vec::with_capacity(component.root_key_hashes.len());
        let mut publications = Vec::with_capacity(component.root_key_hashes.len());
        for root_key_hash in &component.root_key_hashes {
            let root = roots
                .get(root_key_hash)
                .ok_or_else(|| anyhow!("settlement component root is missing"))?;
            let settlement_generation = root
                .anchor
                .root_generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("settlement root generation overflow"))?;
            let marker = CanonicalTopologySettlementMarkerProto {
                common: Some(core_meta_committed_row_common(
                    format!("root/{}", root.anchor.root_anchor_key),
                    root_key_hash.clone(),
                    settlement_generation,
                    transaction_id.clone(),
                    activation.activated_at_unix_nanos,
                )),
                schema: SETTLEMENT_MARKER_SCHEMA.to_string(),
                activation_topology_hash: activation.topology_hash.clone(),
                root_key_hash: root_key_hash.clone(),
                previous_generation: root.anchor.root_generation,
                settlement_generation,
            };
            let marker_bytes = encode_deterministic_proto(&marker);
            payloads.push(encode_core_meta_inline_payload_row(
                &marker_bytes,
                marker
                    .common
                    .clone()
                    .expect("settlement marker common was constructed"),
            )?);
            tuple_keys.push(core_meta_tuple_key(&[
                CoreMetaTuplePart::Utf8("canonical-topology-settlement"),
                CoreMetaTuplePart::Hash(root_key_hash),
                CoreMetaTuplePart::U64(settlement_generation),
                CoreMetaTuplePart::Hash(&activation.topology_hash),
            ])?);
            let mut writer_families = root.anchor.writer_families.clone();
            if writer_families.is_empty() {
                writer_families.push(WriterFamily::CoreControl.as_str().to_string());
            }
            writer_families.sort();
            writer_families.dedup();
            let publication = CoreMetaRootPublication::with_writer_families(
                root.anchor.root_anchor_key.clone(),
                writer_families,
            );
            publications.push(if root_key_hash == &component.coordinator_root_key_hash {
                publication.coordinator()
            } else {
                publication
            });
        }
        let operations = tuple_keys
            .iter()
            .zip(&payloads)
            .map(|(tuple_key, payload)| CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(payload),
            })
            .collect::<Vec<_>>();
        self.commit_coremeta_root_groups(&transaction_id, &operations, &publications)
            .await?;
        Ok(())
    }

    async fn quarantine_orphaned_synthetic_generations(
        &self,
        activation: &CanonicalTopologyActivation,
        round: &mut RecoveryRound,
    ) -> Result<bool> {
        let inventory = synthetic_root_register_inventory(self).await?;
        for generation in inventory {
            if round.operations >= RECOVERY_MAX_OPERATIONS_PER_ROUND {
                return Ok(false);
            }
            quarantine_synthetic_root_register(
                self,
                RootRegisterQuarantineIntent {
                    root_key_hash: generation.root_key_hash,
                    synthetic_generation: generation.generation,
                    activation_topology_hash: activation.topology_hash.clone(),
                    activation_generation: activation.generation,
                },
            )
            .await?;
            round.operations = round.operations.saturating_add(1);
            round.durable_progress = true;
        }
        Ok(synthetic_root_register_inventory(self).await?.is_empty())
    }
}

fn canonical_settlement_pass_outcome(
    verification_index: usize,
    scope_count: usize,
    physical_q2_failed: bool,
    settlement_requires_rescan: bool,
) -> CanonicalSettlementPassOutcome {
    if verification_index < scope_count {
        CanonicalSettlementPassOutcome::Incomplete
    } else if physical_q2_failed || settlement_requires_rescan {
        CanonicalSettlementPassOutcome::RequiresRescan
    } else {
        CanonicalSettlementPassOutcome::Complete
    }
}

fn settlement_transaction_id(
    activation: &CanonicalTopologyActivation,
    component: &SettlementComponent,
    roots: &BTreeMap<String, CurrentRoot>,
) -> Result<String> {
    let mut canonical = Vec::new();
    append_settlement_part(&mut canonical, activation.topology_hash.as_bytes());
    append_settlement_part(
        &mut canonical,
        component.coordinator_root_key_hash.as_bytes(),
    );
    for root_key_hash in &component.root_key_hashes {
        let root = roots
            .get(root_key_hash)
            .ok_or_else(|| anyhow!("settlement transaction root is missing"))?;
        append_settlement_part(&mut canonical, root_key_hash.as_bytes());
        append_settlement_part(&mut canonical, &root.anchor.root_generation.to_le_bytes());
    }
    let hash = domain_hash_bytes("anvil.core.topology_settlement_transaction.v1", &canonical);
    Ok(format!(
        "topology-settlement-{}",
        hash.strip_prefix("sha256:").unwrap_or(&hash)
    ))
}

fn append_settlement_part(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

fn settlement_component_for_local_roots(
    component: &SettlementComponent,
    local_roots: &BTreeSet<String>,
) -> Option<SettlementComponent> {
    let root_key_hashes = component
        .root_key_hashes
        .iter()
        .filter(|root_key_hash| local_roots.contains(*root_key_hash))
        .cloned()
        .collect::<Vec<_>>();
    let coordinator_root_key_hash = if root_key_hashes
        .iter()
        .any(|root_key_hash| root_key_hash == &component.coordinator_root_key_hash)
    {
        component.coordinator_root_key_hash.clone()
    } else {
        root_key_hashes.first()?.clone()
    };
    Some(SettlementComponent {
        coordinator_root_key_hash,
        root_key_hashes,
    })
}

fn settlement_component_for_scope(
    roots: &BTreeMap<String, CurrentRoot>,
    coordinator_root_key_hash: &str,
    coordinator_generation: u64,
) -> Result<SettlementComponent> {
    if !roots.contains_key(coordinator_root_key_hash) {
        bail!("settlement coordinator has no current root anchor");
    }
    let coordinator_scope = (
        coordinator_root_key_hash.to_string(),
        coordinator_generation,
    );
    let members = roots
        .iter()
        .filter(|(_, root)| root.coordinator_scope == coordinator_scope)
        .map(|(root_key_hash, _)| root_key_hash.clone())
        .collect::<BTreeSet<_>>();
    let coordinator_root_key_hash = if members.contains(coordinator_root_key_hash) {
        coordinator_root_key_hash.to_string()
    } else {
        members
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("settlement scope has no current members"))?
    };
    Ok(SettlementComponent {
        coordinator_root_key_hash,
        root_key_hashes: members.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_pass_defers_rescan_until_every_scope_is_verified() {
        let scope_count = RECOVERY_MAX_OPERATIONS_PER_ROUND + 1;

        assert_eq!(
            canonical_settlement_pass_outcome(
                RECOVERY_MAX_OPERATIONS_PER_ROUND,
                scope_count,
                false,
                true,
            ),
            CanonicalSettlementPassOutcome::Incomplete
        );
        assert_eq!(
            canonical_settlement_pass_outcome(scope_count, scope_count, false, true),
            CanonicalSettlementPassOutcome::RequiresRescan
        );
    }

    #[test]
    fn settlement_pass_keeps_physical_q2_failure_as_a_rescan_gate() {
        assert_eq!(
            canonical_settlement_pass_outcome(3, 3, true, false),
            CanonicalSettlementPassOutcome::RequiresRescan
        );
        assert_eq!(
            canonical_settlement_pass_outcome(3, 3, false, false),
            CanonicalSettlementPassOutcome::Complete
        );
    }

    #[test]
    fn settlement_scan_reset_clears_accumulated_pass_progress() {
        let mut state = CanonicalSettlementScanState {
            activation_topology_hash: Some("topology-a".into()),
            after_tuple_key: Some(vec![1, 2, 3]),
            scan_complete: true,
            verification_index: 17,
            physical_q2_failed: true,
            settlement_requires_rescan: true,
            ..CanonicalSettlementScanState::default()
        };

        state.reset_scan();

        assert_eq!(
            state.activation_topology_hash.as_deref(),
            Some("topology-a")
        );
        assert!(state.after_tuple_key.is_none());
        assert!(!state.scan_complete);
        assert_eq!(state.verification_index, 0);
        assert!(!state.physical_q2_failed);
        assert!(!state.settlement_requires_rescan);
    }

    #[test]
    fn settlement_transaction_identity_is_stable_and_generation_scoped() {
        let activation = CanonicalTopologyActivation {
            schema: crate::mesh_lifecycle::CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA.to_string(),
            mesh_id: "mesh-a".to_string(),
            pre_activation_topology_head_generation: 0,
            pre_activation_topology_head_hash:
                "sha256:a6dc371ce64b84d69dc4dc667c8884f67e132d984f9daae9a05dc41a9eb87279"
                    .to_string(),
            topology_hash: format!("sha256:{}", "a".repeat(64)),
            metadata_node_ids: vec!["node-a".into(), "node-b".into(), "node-c".into()],
            quorum_profile: crate::mesh_lifecycle::CANONICAL_METADATA_QUORUM_PROFILE.to_string(),
            activated_at_unix_nanos: 10,
            generation: 1,
            payload_hash: "sha256:0895f47c0c82264cfc60fe82a3e774ffadf20629d38ca22ad88719f3050a0978"
                .to_string(),
        };
        let component = SettlementComponent {
            coordinator_root_key_hash: format!("sha256:{}", "b".repeat(64)),
            root_key_hashes: vec![format!("sha256:{}", "b".repeat(64))],
        };
        let root_hash = component.root_key_hashes[0].clone();
        let current = |generation| CurrentRoot {
            anchor: CoreRootAnchorRecord {
                schema: "anvil.core.root_anchor.v1".into(),
                root_anchor_key: "test/root".into(),
                root_key_hash: root_hash.clone(),
                root_generation: generation,
                previous_root_hash: ZERO_HASH.into(),
                transaction_manifest: None,
                checkpoint_manifest: None,
                core_meta_commit_certificate_hash: None,
                certificate_persist_receipt_hashes: Vec::new(),
                publisher_node_id: "node-a".into(),
                publisher_epoch: 1,
                partition_owner_fence: 1,
                created_at_unix_nanos: 1,
                root_state: "committed".into(),
                mutation_first: None,
                mutation_last: None,
                writer_families: vec![WriterFamily::CoreControl.as_str().into()],
                manifest_count: 0,
                final_block_count: 0,
                genesis_bundle: None,
            },
            coordinator_scope: (root_hash.clone(), generation),
        };
        let first_roots = BTreeMap::from([(root_hash.clone(), current(4))]);
        let second_roots = BTreeMap::from([(root_hash.clone(), current(5))]);
        let first = settlement_transaction_id(&activation, &component, &first_roots).unwrap();
        assert_eq!(
            first,
            settlement_transaction_id(&activation, &component, &first_roots).unwrap()
        );
        assert_ne!(
            first,
            settlement_transaction_id(&activation, &component, &second_roots).unwrap()
        );
    }

    #[test]
    fn settlement_component_is_not_coupled_to_the_old_publisher() {
        let coordinator_hash = format!("sha256:{}", "c".repeat(64));
        let participant_hash = format!("sha256:{}", "d".repeat(64));
        let unrelated_hash = format!("sha256:{}", "f".repeat(64));
        let root = |key: &str, hash: &str, publisher: &str, coordinator: (&str, u64)| CurrentRoot {
            anchor: CoreRootAnchorRecord {
                schema: "anvil.core.root_anchor.v1".into(),
                root_anchor_key: key.into(),
                root_key_hash: hash.into(),
                root_generation: 7,
                previous_root_hash: ZERO_HASH.into(),
                transaction_manifest: None,
                checkpoint_manifest: None,
                core_meta_commit_certificate_hash: None,
                certificate_persist_receipt_hashes: Vec::new(),
                publisher_node_id: publisher.into(),
                publisher_epoch: 1,
                partition_owner_fence: 1,
                created_at_unix_nanos: 1,
                root_state: "committed".into(),
                mutation_first: None,
                mutation_last: None,
                writer_families: vec![WriterFamily::CoreControl.as_str().into()],
                manifest_count: 0,
                final_block_count: 0,
                genesis_bundle: None,
            },
            coordinator_scope: (coordinator.0.into(), coordinator.1),
        };
        let roots = BTreeMap::from([
            (
                coordinator_hash.clone(),
                root(
                    "test/coordinator",
                    &coordinator_hash,
                    "offline-old-owner",
                    (&coordinator_hash, 7),
                ),
            ),
            (
                participant_hash.clone(),
                root(
                    "test/participant",
                    &participant_hash,
                    "another-old-owner",
                    (&coordinator_hash, 7),
                ),
            ),
            (
                unrelated_hash,
                root(
                    "test/unrelated",
                    &format!("sha256:{}", "f".repeat(64)),
                    "offline-third-owner",
                    (&coordinator_hash, 6),
                ),
            ),
        ]);
        let component = settlement_component_for_scope(&roots, &coordinator_hash, 7).unwrap();
        assert_eq!(
            component.root_key_hashes,
            vec![coordinator_hash, participant_hash]
        );
    }

    #[test]
    fn settlement_scope_excludes_an_independently_advanced_coordinator() {
        let coordinator_hash = format!("sha256:{}", "a".repeat(64));
        let participant_hash = format!("sha256:{}", "b".repeat(64));
        let root = |hash: &str, generation: u64, coordinator: (&str, u64)| CurrentRoot {
            anchor: CoreRootAnchorRecord {
                schema: "anvil.core.root_anchor.v1".into(),
                root_anchor_key: format!("test/{hash}"),
                root_key_hash: hash.into(),
                root_generation: generation,
                previous_root_hash: ZERO_HASH.into(),
                transaction_manifest: None,
                checkpoint_manifest: None,
                core_meta_commit_certificate_hash: None,
                certificate_persist_receipt_hashes: Vec::new(),
                publisher_node_id: "node-a".into(),
                publisher_epoch: 1,
                partition_owner_fence: 1,
                created_at_unix_nanos: 1,
                root_state: "committed".into(),
                mutation_first: None,
                mutation_last: None,
                writer_families: vec![WriterFamily::CoreControl.as_str().into()],
                manifest_count: 0,
                final_block_count: 0,
                genesis_bundle: None,
            },
            coordinator_scope: (coordinator.0.into(), coordinator.1),
        };
        let roots = BTreeMap::from([
            (
                coordinator_hash.clone(),
                root(&coordinator_hash, 8, (&coordinator_hash, 8)),
            ),
            (
                participant_hash.clone(),
                root(&participant_hash, 7, (&coordinator_hash, 7)),
            ),
        ]);

        let component = settlement_component_for_scope(&roots, &coordinator_hash, 7).unwrap();

        assert_eq!(component.coordinator_root_key_hash, participant_hash);
        assert_eq!(
            component.root_key_hashes,
            vec![component.coordinator_root_key_hash.clone()]
        );
    }

    #[test]
    fn settlement_component_keeps_the_historical_coordinator_when_locally_owned() {
        let coordinator = format!("sha256:{}", "a".repeat(64));
        let participant = format!("sha256:{}", "b".repeat(64));
        let component = SettlementComponent {
            coordinator_root_key_hash: coordinator.clone(),
            root_key_hashes: vec![coordinator.clone(), participant.clone()],
        };
        let local = BTreeSet::from([coordinator.clone(), participant.clone()]);

        let local_component = settlement_component_for_local_roots(&component, &local).unwrap();

        assert_eq!(local_component.coordinator_root_key_hash, coordinator);
        assert_eq!(local_component.root_key_hashes, component.root_key_hashes);
    }

    #[test]
    fn settlement_component_recoordinates_the_local_owner_partition() {
        let coordinator = format!("sha256:{}", "a".repeat(64));
        let participant = format!("sha256:{}", "b".repeat(64));
        let second_participant = format!("sha256:{}", "c".repeat(64));
        let component = SettlementComponent {
            coordinator_root_key_hash: coordinator.clone(),
            root_key_hashes: vec![coordinator, participant.clone(), second_participant.clone()],
        };
        let local = BTreeSet::from([participant.clone(), second_participant.clone()]);

        let local_component = settlement_component_for_local_roots(&component, &local).unwrap();

        assert_eq!(local_component.coordinator_root_key_hash, participant);
        assert_eq!(
            local_component.root_key_hashes,
            vec![
                local_component.coordinator_root_key_hash.clone(),
                second_participant
            ]
        );
        assert!(settlement_component_for_local_roots(&component, &BTreeSet::new()).is_none());
    }

    #[tokio::test]
    async fn settlement_root_scan_advances_past_the_first_bounded_page() {
        let temporary = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temporary.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let genesis = store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("genesis root");
        let genesis_row = encode_root_cache_row(&genesis).unwrap();
        for index in 0..(SETTLEMENT_SCAN_PAGE_ROWS + 20) {
            let root_anchor_key = format!("test/paged-root-{index:04}");
            store
                .meta
                .put(
                    CF_ROOT_CACHE,
                    TABLE_ROOT_CACHE_ROW,
                    &root_cache_hash_key(&root_key_hash(&root_anchor_key)),
                    &genesis_row,
                )
                .unwrap();
        }
        let activation = CanonicalTopologyActivation {
            schema: crate::mesh_lifecycle::CANONICAL_TOPOLOGY_ACTIVATION_SCHEMA.to_string(),
            mesh_id: "mesh-a".into(),
            pre_activation_topology_head_generation: 0,
            pre_activation_topology_head_hash:
                "sha256:a6dc371ce64b84d69dc4dc667c8884f67e132d984f9daae9a05dc41a9eb87279".into(),
            topology_hash: format!("sha256:{}", "e".repeat(64)),
            metadata_node_ids: vec!["node-a".into(), "node-b".into(), "node-c".into()],
            quorum_profile: crate::mesh_lifecycle::CANONICAL_METADATA_QUORUM_PROFILE.into(),
            activated_at_unix_nanos: 10,
            generation: 1,
            payload_hash: "sha256:1d7da5f14b51dcc8a30f579dd26282cef39bc596b645968234526a7c83a46af3"
                .into(),
        };

        assert!(
            !store
                .advance_canonical_settlement_scan(&activation)
                .unwrap()
        );
        assert!(
            store
                .advance_canonical_settlement_scan(&activation)
                .unwrap()
        );
    }
}
