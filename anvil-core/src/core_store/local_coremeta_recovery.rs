use super::local_coremeta_history::{inventory_page_hash, validate_descriptor};
use super::local_root_publication_recovery::{
    CoreMetaRecoveryPublicationBundle, decode_coremeta_recovery_publication_bundle,
    publication_transaction_id,
};
use super::*;
use crate::anvil_api::{
    CoreMetaBatchFrame, CoreMetaCatchUpRequest, CoreMetaHistoryCursor, CoreMetaInventory,
    CoreMetaInventoryCursor, CoreMetaInventoryRequest, ExchangeRootDirectoryRequest,
    ReadRootRequest, RootAnchorRead, RootDirectoryEntry, RootDirectoryPage,
    core_meta_replication_internal_client::CoreMetaReplicationInternalClient,
    root_register_internal_client::RootRegisterInternalClient,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tonic::metadata::MetadataValue;

#[path = "local_coremeta_recovery/generation_fetch.rs"]
mod generation_fetch;
#[path = "local_coremeta_recovery/publication_catch_up.rs"]
mod publication_catch_up;
#[path = "local_coremeta_recovery/readiness.rs"]
mod readiness;
#[path = "local_coremeta_recovery/register_quarantine.rs"]
mod register_quarantine;
#[path = "local_coremeta_recovery/register_quorum.rs"]
mod register_quorum;
#[path = "local_coremeta_recovery/topology_settlement.rs"]
mod topology_settlement;

pub(in crate::core_store::local) use readiness::validate_recovery_publication_anchor;
use readiness::*;
pub(in crate::core_store::local) use register_quorum::{
    RootRegisterGenerationResolution, RootRegisterQuorumResolution,
};

const RECOVERY_PAGE_ROWS: u32 = CORE_META_MAX_SCAN_PAGE_ROWS as u32;
const RECOVERY_PAGE_BYTES: u64 = 16 * 1024 * 1024;
const RECOVERY_MAX_OPERATIONS_PER_ROUND: usize = 256;
const RECOVERY_MAX_PAGES_PER_GENERATION: usize = 32;
const RECOVERY_RPC_TIMEOUT: Duration = Duration::from_secs(8);
const RECOVERY_QUORUM_SETTLE_INTERVAL: Duration = Duration::from_millis(100);
const RECOVERY_STEADY_INTERVAL: Duration = Duration::from_secs(10);
const RECOVERY_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const RECOVERY_MAX_BACKOFF: Duration = Duration::from_secs(15);
const RECOVERY_READINESS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ROOT_DIRECTORY_PAGE_ENTRIES: u32 = 256;
const ROOT_DIRECTORY_PAGE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMetaRecoverySnapshot {
    pub ready: bool,
    pub distributed_required: bool,
    pub in_progress: bool,
    pub reachable_peers: usize,
    pub known_roots: usize,
    pub lagging_roots: usize,
    pub root_directory_complete: bool,
    pub canonical_settlement_complete: bool,
    pub physical_register_quorum_complete: bool,
    pub completed_rounds: u64,
    pub last_error: Option<String>,
}

impl Default for CoreMetaRecoverySnapshot {
    fn default() -> Self {
        Self {
            ready: true,
            distributed_required: false,
            in_progress: false,
            reachable_peers: 0,
            known_roots: 0,
            lagging_roots: 0,
            root_directory_complete: false,
            canonical_settlement_complete: false,
            physical_register_quorum_complete: false,
            completed_rounds: 0,
            last_error: None,
        }
    }
}

#[derive(Debug)]
pub(super) struct CoreMetaRecoveryState {
    started: AtomicBool,
    distributed_required: AtomicBool,
    startup_admitted: AtomicBool,
    ready: AtomicBool,
    readiness_epoch: AtomicU64,
    completed_rounds: AtomicU64,
    wake: Notify,
    requested_generations: StdMutex<BTreeMap<String, u64>>,
    snapshot: StdMutex<CoreMetaRecoverySnapshot>,
    root_directory: StdMutex<RootDirectoryScanState>,
    canonical_settlement: StdMutex<topology_settlement::CanonicalSettlementScanState>,
}

impl Default for CoreMetaRecoveryState {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            distributed_required: AtomicBool::new(false),
            startup_admitted: AtomicBool::new(true),
            ready: AtomicBool::new(true),
            readiness_epoch: AtomicU64::new(0),
            completed_rounds: AtomicU64::new(0),
            wake: Notify::new(),
            requested_generations: StdMutex::new(BTreeMap::new()),
            snapshot: StdMutex::new(CoreMetaRecoverySnapshot::default()),
            root_directory: StdMutex::new(RootDirectoryScanState::default()),
            canonical_settlement: StdMutex::new(
                topology_settlement::CanonicalSettlementScanState::default(),
            ),
        }
    }
}

#[derive(Debug, Default)]
struct RootDirectoryScanState {
    peer_cursors: BTreeMap<String, String>,
    peers_with_complete_pass: BTreeSet<String>,
    /// Last complete, internally consistent directory snapshot per peer.
    peer_entries: BTreeMap<String, BTreeMap<String, RootDirectoryEntry>>,
    /// Entries accumulated for the next pass. A partial page sequence must not
    /// replace the last complete snapshot or revoke steady-state readiness.
    pending_peer_entries: BTreeMap<String, BTreeMap<String, RootDirectoryEntry>>,
}

impl RootDirectoryScanState {
    fn record_page(&mut self, node_id: &str, after: &str, page: &RootDirectoryPage) {
        if after.is_empty() {
            self.pending_peer_entries
                .entry(node_id.to_string())
                .or_default()
                .clear();
        }
        let peer_entries = self
            .pending_peer_entries
            .entry(node_id.to_string())
            .or_default();
        for entry in &page.entries {
            peer_entries.insert(entry.root_key_hash.clone(), entry.clone());
        }
        if page.directory_complete {
            self.peer_cursors.remove(node_id);
            self.peers_with_complete_pass.insert(node_id.to_string());
            let completed = self
                .pending_peer_entries
                .remove(node_id)
                .unwrap_or_default();
            self.peer_entries.insert(node_id.to_string(), completed);
        } else {
            self.peer_cursors
                .insert(node_id.to_string(), page.next_root_key_hash.clone());
        }
    }
}

#[derive(Debug, Default)]
struct RootDirectoryDiscovery {
    reachable_peers: BTreeSet<String>,
    heads_by_root: BTreeMap<String, BTreeMap<String, RootDirectoryEntry>>,
    complete: bool,
}

#[derive(Debug, Clone)]
pub(in crate::core_store::local) struct RecoveryPeer {
    pub(in crate::core_store::local) node_id: String,
    pub(in crate::core_store::local) public_api_addr: String,
}

#[derive(Debug, Clone)]
struct RecoverySource {
    peer: RecoveryPeer,
    final_generation: u64,
    retention_floor_generation: u64,
}

#[derive(Debug)]
struct RootAnchorQuorumCandidate {
    read: RootAnchorRead,
    replicas: BTreeMap<String, u32>,
}

#[derive(Debug, Default)]
struct RecoveryRound {
    readiness_epoch: u64,
    reachable_peers: BTreeSet<String>,
    known_roots: BTreeSet<String>,
    remote_heads: BTreeMap<String, BTreeMap<String, RootDirectoryEntry>>,
    lagging_roots: BTreeSet<String>,
    pending_bundles: BTreeMap<Vec<u8>, CoreMetaRecoveryPublicationBundle>,
    unresolved_publication_intents: BTreeSet<String>,
    pending_mutations_complete: bool,
    committed_anchors: BTreeMap<(String, u64), Vec<u8>>,
    attempted_scopes: BTreeSet<(String, u64)>,
    root_directory_complete: bool,
    canonical_settlement_complete: bool,
    physical_register_quorum_complete: bool,
    durable_progress: bool,
    operations: usize,
}

impl CoreStore {
    pub fn start_coremeta_distributed_recovery(
        &self,
        distributed_required: bool,
    ) -> Option<tokio::task::JoinHandle<()>> {
        self.set_coremeta_recovery_required(distributed_required);
        if !distributed_required {
            return None;
        }
        if self.coremeta_recovery.started.swap(true, Ordering::AcqRel) {
            return None;
        }
        let store = self.clone();
        Some(tokio::spawn(async move {
            store.run_coremeta_recovery_loop().await;
        }))
    }

    pub fn coremeta_recovery_ready(&self) -> bool {
        self.coremeta_recovery.ready.load(Ordering::Acquire)
    }

    pub(in crate::core_store::local) fn coremeta_distributed_recovery_required(&self) -> bool {
        self.coremeta_recovery
            .distributed_required
            .load(Ordering::Acquire)
    }

    pub async fn wait_for_coremeta_recovery_ready(&self) {
        while !self.coremeta_recovery_ready() {
            tokio::time::sleep(RECOVERY_READINESS_POLL_INTERVAL).await;
        }
    }

    pub fn coremeta_recovery_snapshot(&self) -> CoreMetaRecoverySnapshot {
        self.coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn set_coremeta_recovery_required(&self, distributed_required: bool) {
        let mut requested_generations = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        requested_generations.clear();
        self.coremeta_recovery
            .readiness_epoch
            .fetch_add(1, Ordering::AcqRel);
        self.coremeta_recovery
            .distributed_required
            .store(distributed_required, Ordering::Release);
        self.coremeta_recovery
            .ready
            .store(!distributed_required, Ordering::Release);
        self.coremeta_recovery
            .startup_admitted
            .store(!distributed_required, Ordering::Release);
        let mut snapshot = self
            .coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.ready = !distributed_required;
        snapshot.distributed_required = distributed_required;
        snapshot.in_progress = distributed_required;
        snapshot.last_error = None;
        drop(snapshot);
        drop(requested_generations);
    }

    async fn run_coremeta_recovery_loop(self) {
        let mut backoff = RECOVERY_INITIAL_BACKOFF;
        loop {
            self.update_coremeta_recovery_progress(true, None, None);
            match self.reconcile_coremeta_once().await {
                Ok(round) => {
                    let admitted = self
                        .coremeta_recovery
                        .startup_admitted
                        .load(Ordering::Acquire);
                    let ready = if admitted {
                        recovery_round_preserves_admitted_readiness(&round)
                    } else {
                        recovery_round_is_ready(&round)
                    };
                    let ready = self.finish_coremeta_recovery_round(&round, ready, None);
                    if ready {
                        self.coremeta_recovery
                            .startup_admitted
                            .store(true, Ordering::Release);
                    }
                    if ready {
                        backoff = RECOVERY_INITIAL_BACKOFF;
                        self.wait_for_coremeta_recovery_wake(RECOVERY_STEADY_INTERVAL)
                            .await;
                    } else if round.durable_progress {
                        backoff = RECOVERY_INITIAL_BACKOFF;
                        self.wait_for_coremeta_recovery_wake(RECOVERY_INITIAL_BACKOFF)
                            .await;
                    } else {
                        self.wait_for_coremeta_recovery_wake(backoff).await;
                        backoff = next_recovery_backoff(backoff);
                    }
                }
                Err(error) => {
                    let admitted = self
                        .coremeta_recovery
                        .startup_admitted
                        .load(Ordering::Acquire);
                    if admitted && is_stale_recovery_publication(&error) {
                        tracing::debug!(
                            error = %format_args!("{error:#}"),
                            retry_after_ms = RECOVERY_INITIAL_BACKOFF.as_millis(),
                            "CoreMeta recovery raced a foreground publication; retrying from a fresh root snapshot"
                        );
                        self.finish_stale_recovery_publication_retry(format!("{error:#}"));
                        backoff = RECOVERY_INITIAL_BACKOFF;
                        self.wait_for_coremeta_recovery_wake(RECOVERY_INITIAL_BACKOFF)
                            .await;
                        continue;
                    }
                    tracing::warn!(
                        error = %format_args!("{error:#}"),
                        retry_after_ms = backoff.as_millis(),
                        "distributed CoreMeta recovery round failed"
                    );
                    self.finish_coremeta_recovery_round(
                        &RecoveryRound::default(),
                        false,
                        Some(format!("{error:#}")),
                    );
                    self.wait_for_coremeta_recovery_wake(backoff).await;
                    backoff = next_recovery_backoff(backoff);
                }
            }
        }
    }

    async fn wait_for_coremeta_recovery_wake(&self, delay: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = self.coremeta_recovery.wake.notified() => {}
        }
    }

    async fn reconcile_coremeta_once(&self) -> Result<RecoveryRound> {
        let readiness_epoch = self.begin_coremeta_recovery_round();
        let all_peers = self.coremeta_recovery_peers()?;
        if all_peers.is_empty() {
            bail!("CoreMeta recovery has no reachable equal-peer candidates");
        }
        let requested_repair_operations = self
            .recover_requested_root_generations(&all_peers)
            .await
            .context("recover requested root-register generations")?;
        let unresolved_publication_intents = self
            .recover_distributed_root_publication_intents(&all_peers)
            .await
            .context("recover distributed root-publication intents")?;
        let pending_mutations_complete = !self.has_pending_mutations()?;
        let directory = self
            .discover_coremeta_root_directory(&all_peers)
            .await
            .context("discover CoreMeta root directory")?;
        let history_peers = all_peers
            .iter()
            .cloned()
            .filter(|peer| directory.reachable_peers.contains(&peer.node_id))
            .collect::<Vec<_>>();
        let mut round = RecoveryRound {
            readiness_epoch,
            known_roots: self.coremeta_recovery_root_hashes()?,
            reachable_peers: directory.reachable_peers,
            remote_heads: directory.heads_by_root,
            root_directory_complete: directory.complete,
            unresolved_publication_intents,
            pending_mutations_complete,
            durable_progress: requested_repair_operations > 0,
            operations: requested_repair_operations,
            ..RecoveryRound::default()
        };
        let remote_roots = round.remote_heads.keys().cloned().collect::<Vec<_>>();
        round.known_roots.extend(remote_roots);
        if round.known_roots.is_empty() {
            round
                .known_roots
                .insert(root_key_hash(core_transaction_root_anchor_key()));
        }

        loop {
            let mut made_progress = false;
            let roots = round.known_roots.iter().cloned().collect::<Vec<_>>();
            for root_key_hash in roots {
                if round.operations >= RECOVERY_MAX_OPERATIONS_PER_ROUND {
                    break;
                }
                let mut local_generation =
                    self.coremeta_recovery_published_generation(&root_key_hash)?;
                if remote_root_needs_inventory(
                    local_generation,
                    round.remote_heads.get(&root_key_hash),
                ) {
                    // A foreground publisher exposes the remote register/cache
                    // quorum before it installs the same generation locally.
                    // Wait for that publication's linearization guard and
                    // re-read before treating the node as genuinely stale.
                    local_generation = {
                        let _publication_guard = self
                            .acquire_named_lock("root-publication", &root_key_hash)
                            .await?;
                        self.coremeta_recovery_published_generation(&root_key_hash)?
                    };
                }
                if !remote_root_needs_inventory(
                    local_generation,
                    round.remote_heads.get(&root_key_hash),
                ) {
                    self.verify_root_directory_head_agreement(
                        &root_key_hash,
                        local_generation,
                        round.remote_heads.get(&root_key_hash),
                    )
                    .await?;
                    continue;
                }
                // Q2 publication may normally leave one non-quorum replica
                // behind. Recover that root without turning unrelated public
                // operations into a process-wide outage. An incoming skipped
                // register generation still revokes readiness through the
                // supervised repair-target path.
                let next_generation = local_generation.saturating_add(1);
                if round
                    .attempted_scopes
                    .contains(&(root_key_hash.clone(), next_generation))
                {
                    // A coordinator fetch can discover participant roots after
                    // this round's root snapshot. Do not repeat the all-peer
                    // inventory fan-out when that exact generation has already
                    // been staged and is waiting for its publication bundle.
                    round.lagging_roots.insert(root_key_hash);
                    continue;
                }
                let sources = self
                    .coremeta_recovery_sources(&history_peers, &root_key_hash, &mut round)
                    .await?;
                let Some(source) = sources.first() else {
                    round.lagging_roots.insert(root_key_hash);
                    continue;
                };
                if source.final_generation <= local_generation {
                    self.verify_recovery_generation_agreement(
                        source,
                        &root_key_hash,
                        local_generation,
                    )
                    .await?;
                    continue;
                }
                round.lagging_roots.insert(root_key_hash.clone());
                if source.retention_floor_generation > next_generation {
                    bail!(
                        "CoreMeta recovery history gap exceeds retention: root={root_key_hash} local={local_generation} floor={}",
                        source.retention_floor_generation
                    );
                }
                if !round
                    .attempted_scopes
                    .insert((root_key_hash.clone(), next_generation))
                {
                    continue;
                }
                let publication_bundle = self
                    .fetch_coremeta_generation(source, &root_key_hash, next_generation)
                    .await?;
                let plan = decode_coremeta_recovery_publication_bundle(&publication_bundle)?;
                let target_scope = (root_key_hash.clone(), next_generation);
                let coordinator_anchor = match round
                    .committed_anchors
                    .get(&plan.coordinator_scope)
                    .cloned()
                {
                    Some(anchor) => anchor,
                    None => {
                        let anchor = self
                            .fetch_committed_register_anchor(
                                &all_peers,
                                &plan.coordinator_scope.0,
                                plan.coordinator_scope.1,
                            )
                            .await?;
                        validate_recovery_publication_anchor(
                            &plan,
                            &plan.coordinator_scope,
                            &anchor,
                        )?;
                        round
                            .committed_anchors
                            .insert(plan.coordinator_scope.clone(), anchor.clone());
                        anchor
                    }
                };
                let committed_anchor = if target_scope == plan.coordinator_scope {
                    Some(coordinator_anchor)
                } else {
                    self.fetch_committed_cache_anchor(&all_peers, &target_scope.0, target_scope.1)
                        .await?
                };
                for (participant_root, _) in &plan.scopes {
                    round.known_roots.insert(participant_root.clone());
                }
                if let Some(committed_anchor) = committed_anchor {
                    validate_recovery_publication_anchor(&plan, &target_scope, &committed_anchor)?;
                    round
                        .committed_anchors
                        .insert(target_scope, committed_anchor);
                }
                round
                    .pending_bundles
                    .entry(publication_bundle)
                    .or_insert(plan);
                round.operations += 1;
                made_progress = true;
            }

            let bundles: Vec<(Vec<u8>, CoreMetaRecoveryPublicationBundle)> = round
                .pending_bundles
                .iter()
                .map(|(bytes, plan)| (bytes.to_vec(), plan.clone()))
                .collect();
            for (bytes, plan) in bundles {
                if round.operations >= RECOVERY_MAX_OPERATIONS_PER_ROUND {
                    break;
                }
                if self.recovery_bundle_is_ready(&plan)?
                    && recovered_anchors_are_ready(&plan, &round.committed_anchors)
                {
                    self.publish_staged_coremeta_recovery_bundle(&bytes, &round.committed_anchors)
                        .await
                        .context("publish staged CoreMeta recovery bundle")?;
                    round.durable_progress = true;
                    for (root_key_hash, _) in &plan.scopes {
                        round.lagging_roots.remove(root_key_hash);
                    }
                    round.pending_bundles.remove(&bytes);
                    round.operations += 1;
                    made_progress = true;
                }
            }
            if !made_progress || round.operations >= RECOVERY_MAX_OPERATIONS_PER_ROUND {
                break;
            }
        }

        self.verify_coremeta_recovery_convergence(&mut round)
            .await
            .context("verify CoreMeta recovery convergence")?;
        self.reconcile_canonical_topology_registers(&all_peers, &mut round)
            .await
            .context("reconcile canonical topology registers")?;
        if recovery_round_can_replay_pending_mutations(&round) && !round.pending_mutations_complete
        {
            let startup_guard = self.startup_recovery_lock.lock().await;
            self.recover_pending_mutations(&startup_guard)
                .await
                .context("recover pending mutations after canonical settlement")?;
            round.pending_mutations_complete = !self.has_pending_mutations()?;
            round.durable_progress = true;
        }
        // Earlier work in this round may itself have revoked readiness after
        // discovering lag. Adopt that epoch only before a fresh authoritative
        // directory pass; any later repair signal still invalidates the round.
        round.readiness_epoch = self.begin_coremeta_recovery_round();
        self.refresh_coremeta_recovery_convergence(&all_peers, &mut round)
            .await
            .context("refresh CoreMeta recovery convergence")?;
        Ok(round)
    }

    async fn refresh_coremeta_recovery_convergence(
        &self,
        peers: &[RecoveryPeer],
        round: &mut RecoveryRound,
    ) -> Result<()> {
        let directory = self.discover_coremeta_root_directory(peers).await?;
        round.reachable_peers = directory.reachable_peers;
        round.remote_heads = directory.heads_by_root;
        round.root_directory_complete = directory.complete;
        round.known_roots.extend(round.remote_heads.keys().cloned());
        self.verify_coremeta_recovery_convergence(round).await
    }

    pub(in crate::core_store::local) fn coremeta_recovery_peers(
        &self,
    ) -> Result<Vec<RecoveryPeer>> {
        let profile = self.default_coremeta_quorum_profile()?;
        let mut peers = self
            .active_coremeta_lifecycle_replicas(profile.prepare_quorum)?
            .into_iter()
            .filter(|peer| !peer.is_local && !peer.public_api_addr.trim().is_empty())
            .map(|peer| RecoveryPeer {
                node_id: peer.node_id,
                public_api_addr: peer.public_api_addr,
            })
            .collect::<Vec<_>>();
        peers.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        peers.dedup_by(|left, right| left.node_id == right.node_id);
        Ok(peers)
    }

    pub(crate) fn coremeta_root_directory_page(
        &self,
        after_root_key_hash: &str,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<RootDirectoryPage> {
        if max_entries == 0 || max_entries > CORE_META_MAX_SCAN_PAGE_ROWS {
            bail!("CoreMeta root-directory max_entries is outside the supported bounds");
        }
        if max_bytes == 0 || max_bytes > RECOVERY_PAGE_BYTES {
            bail!("CoreMeta root-directory max_bytes is outside the supported bounds");
        }
        let after_tuple_key = if after_root_key_hash.is_empty() {
            None
        } else {
            validate_hash(after_root_key_hash, "root-directory cursor")?;
            Some(root_cache_hash_key(after_root_key_hash))
        };
        let prefix = root_cache_hash_prefix();
        let records = self.meta.scan_prefix_page(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &prefix,
            after_tuple_key.as_deref(),
            max_entries,
        )?;
        let mut entries = Vec::with_capacity(records.len());
        let mut encoded_bytes = 0_u64;
        let mut byte_truncated = false;
        for record in &records {
            let anchor = decode_root_cache_row(&record.payload)?;
            let tuple_key = core_meta_record_tuple_key(&record.key)?;
            if tuple_key != root_cache_hash_key(&anchor.root_key_hash) {
                bail!("CoreMeta root-directory row key does not match its root hash");
            }
            let entry = RootDirectoryEntry {
                root_key_hash: anchor.root_key_hash.clone(),
                root_generation: anchor.root_generation,
                root_anchor_hash: hash_root_anchor_record(&anchor)?,
            };
            let entry_bytes =
                u64::try_from(prost::Message::encoded_len(&entry)).unwrap_or(u64::MAX);
            if encoded_bytes.saturating_add(entry_bytes) > max_bytes {
                if entries.is_empty() {
                    bail!("CoreMeta root-directory max_bytes cannot hold one entry");
                }
                byte_truncated = true;
                break;
            }
            encoded_bytes = encoded_bytes.saturating_add(entry_bytes);
            entries.push(entry);
        }
        let directory_complete = !byte_truncated && records.len() < max_entries;
        let next_root_key_hash = if directory_complete {
            String::new()
        } else {
            entries
                .last()
                .map(|entry| entry.root_key_hash.clone())
                .ok_or_else(|| anyhow!("incomplete CoreMeta root-directory page is empty"))?
        };
        let page_hash = root_directory_page_hash(
            after_root_key_hash,
            &entries,
            &next_root_key_hash,
            directory_complete,
            encoded_bytes,
        );
        Ok(RootDirectoryPage {
            entries,
            next_root_key_hash,
            directory_complete,
            page_hash,
            encoded_bytes,
        })
    }

    async fn discover_coremeta_root_directory(
        &self,
        peers: &[RecoveryPeer],
    ) -> Result<RootDirectoryDiscovery> {
        let node_ids = peers
            .iter()
            .map(|peer| peer.node_id.clone())
            .collect::<BTreeSet<_>>();
        let requests = {
            let mut state = self
                .coremeta_recovery
                .root_directory
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .peer_cursors
                .retain(|node_id, _| node_ids.contains(node_id));
            state
                .peers_with_complete_pass
                .retain(|node_id| node_ids.contains(node_id));
            state
                .peer_entries
                .retain(|node_id, _| node_ids.contains(node_id));
            state
                .pending_peer_entries
                .retain(|node_id, _| node_ids.contains(node_id));
            peers
                .iter()
                .cloned()
                .map(|peer| {
                    let after = state
                        .peer_cursors
                        .get(&peer.node_id)
                        .cloned()
                        .unwrap_or_default();
                    (peer, after)
                })
                .collect::<Vec<_>>()
        };

        let mut pending = FuturesUnordered::new();
        for (peer, after) in requests {
            pending.push(async move {
                let result = self.exchange_root_directory_page(&peer, &after).await;
                (peer, after, result)
            });
        }

        let profile = self.default_coremeta_quorum_profile()?;
        let selected_replicas = self.select_coremeta_replicas(&profile).await?;
        let local_is_replica = selected_replicas.iter().any(|replica| replica.is_local);
        let authoritative_remote_peers = selected_replicas
            .iter()
            .filter(|replica| !replica.is_local)
            .map(|replica| replica.node_id.clone())
            .collect::<BTreeSet<_>>();
        if local_is_replica {
            // Counting the local node toward Q requires its canonical root
            // directory to be readable, not merely its presence in topology.
            self.coremeta_recovery_root_hashes()?;
        }
        let minimum_remote_recovery_peers =
            remote_recovery_acknowledgements(profile.prepare_quorum, local_is_replica);
        let mut reachable_peers = BTreeSet::new();
        let mut failures = Vec::new();
        loop {
            let next = if root_directory_quorum_is_settled(
                &self.coremeta_recovery.root_directory,
                &reachable_peers,
                &authoritative_remote_peers,
                minimum_remote_recovery_peers,
            ) {
                match tokio::time::timeout(RECOVERY_QUORUM_SETTLE_INTERVAL, pending.next()).await {
                    Ok(next) => next,
                    Err(_) => break,
                }
            } else {
                pending.next().await
            };
            let Some((peer, after, result)) = next else {
                break;
            };
            match result {
                Ok(page) => {
                    self.validate_root_directory_page(&after, &page)?;
                    reachable_peers.insert(peer.node_id.clone());
                    let mut state = self
                        .coremeta_recovery
                        .root_directory
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state.record_page(&peer.node_id, &after, &page);
                }
                Err(error) => failures.push(format!("{}: {error:#}", peer.node_id)),
            }
        }

        let complete = root_directory_quorum_is_settled(
            &self.coremeta_recovery.root_directory,
            &reachable_peers,
            &authoritative_remote_peers,
            minimum_remote_recovery_peers,
        );
        let state = self
            .coremeta_recovery
            .root_directory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if reachable_peers.is_empty() && !complete {
            bail!(
                "CoreMeta root-directory discovery failed: {}",
                failures.join("; ")
            );
        }
        let mut heads_by_root = BTreeMap::<String, BTreeMap<String, RootDirectoryEntry>>::new();
        for (node_id, entries) in &state.peer_entries {
            if !reachable_peers.contains(node_id) {
                continue;
            }
            for (root_key_hash, entry) in entries {
                heads_by_root
                    .entry(root_key_hash.clone())
                    .or_default()
                    .insert(node_id.clone(), entry.clone());
            }
        }
        Ok(RootDirectoryDiscovery {
            reachable_peers,
            heads_by_root,
            complete,
        })
    }

    async fn exchange_root_directory_page(
        &self,
        peer: &RecoveryPeer,
        after_root_key_hash: &str,
    ) -> Result<RootDirectoryPage> {
        let bearer = self.coremeta_recovery_bearer()?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode root-directory recovery bearer token")?;
        let request_body = ExchangeRootDirectoryRequest {
            header: Some(self.internal_request_header("root.exchange_directory")?),
            after_root_key_hash: after_root_key_hash.to_string(),
            max_entries: ROOT_DIRECTORY_PAGE_ENTRIES,
            max_bytes: ROOT_DIRECTORY_PAGE_BYTES,
        };
        let operation = self.internal_grpc_request(
            &peer.public_api_addr,
            "exchange CoreMeta root directory",
            move |channel| {
                let mut client = RootRegisterInternalClient::new(channel);
                let body = request_body.clone();
                let authorization = authorization.clone();
                async move {
                    let mut request = tonic::Request::new(body);
                    request
                        .metadata_mut()
                        .insert("authorization", authorization);
                    client
                        .exchange_root_directory(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
        );
        tokio::time::timeout(RECOVERY_RPC_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow!("CoreMeta root-directory request timed out"))?
    }

    fn validate_root_directory_page(
        &self,
        after_root_key_hash: &str,
        page: &RootDirectoryPage,
    ) -> Result<()> {
        if page.entries.len() > ROOT_DIRECTORY_PAGE_ENTRIES as usize
            || page.encoded_bytes > ROOT_DIRECTORY_PAGE_BYTES
        {
            bail!("CoreMeta root-directory page exceeds negotiated bounds");
        }
        let mut previous = if after_root_key_hash.is_empty() {
            None
        } else {
            validate_hash(after_root_key_hash, "root-directory cursor")?;
            Some(after_root_key_hash)
        };
        let mut encoded_bytes = 0_u64;
        for entry in &page.entries {
            validate_hash(&entry.root_key_hash, "root-directory root key hash")?;
            validate_hash(&entry.root_anchor_hash, "root-directory root anchor hash")?;
            if previous.is_some_and(|value| value >= entry.root_key_hash.as_str()) {
                bail!("CoreMeta root-directory page is not strictly ordered");
            }
            encoded_bytes = encoded_bytes.saturating_add(
                u64::try_from(prost::Message::encoded_len(entry)).unwrap_or(u64::MAX),
            );
            previous = Some(entry.root_key_hash.as_str());
        }
        if encoded_bytes != page.encoded_bytes {
            bail!("CoreMeta root-directory encoded byte count mismatch");
        }
        if page.directory_complete {
            if !page.next_root_key_hash.is_empty() {
                bail!("complete CoreMeta root-directory page has a next cursor");
            }
        } else if page
            .entries
            .last()
            .map(|entry| entry.root_key_hash.as_str())
            != Some(page.next_root_key_hash.as_str())
        {
            bail!("CoreMeta root-directory next cursor is invalid");
        }
        if page.page_hash
            != root_directory_page_hash(
                after_root_key_hash,
                &page.entries,
                &page.next_root_key_hash,
                page.directory_complete,
                page.encoded_bytes,
            )
        {
            bail!("CoreMeta root-directory page hash mismatch");
        }
        Ok(())
    }

    fn coremeta_recovery_root_hashes(&self) -> Result<BTreeSet<String>> {
        let mut roots = BTreeSet::from([root_key_hash(core_transaction_root_anchor_key())]);
        roots.extend(self.coremeta_recovery_intent_root_hashes()?);
        roots.extend(self.staged_coremeta_recovery_root_hashes()?);
        let mut after = None;
        loop {
            let page = self.meta.scan_prefix_page(
                CF_ROOT_CACHE,
                TABLE_ROOT_CACHE_ROW,
                &[],
                after.as_deref(),
                CORE_META_MAX_SCAN_PAGE_ROWS,
            )?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                roots.insert(decode_root_cache_row(&record.payload)?.root_key_hash);
            }
            after = page
                .last()
                .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
                .transpose()?;
            if page.len() < CORE_META_MAX_SCAN_PAGE_ROWS {
                break;
            }
        }
        Ok(roots)
    }

    pub(in crate::core_store::local) fn coremeta_recovery_published_generation(
        &self,
        root_key_hash: &str,
    ) -> Result<u64> {
        validate_hash(root_key_hash, "CoreMeta recovery root key hash")?;
        let Some(payload) = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_hash_key(root_key_hash),
        )?
        else {
            return Ok(0);
        };
        let anchor = decode_root_cache_row(&payload)?;
        if anchor.root_key_hash != root_key_hash {
            bail!("CoreMeta recovery root-cache key does not match its anchor");
        }
        Ok(anchor.root_generation)
    }

    async fn coremeta_recovery_sources(
        &self,
        peers: &[RecoveryPeer],
        root_key_hash: &str,
        round: &mut RecoveryRound,
    ) -> Result<Vec<RecoverySource>> {
        let mut requests = FuturesUnordered::new();
        for peer in peers.iter().cloned() {
            let root_key_hash = root_key_hash.to_string();
            requests.push(async move {
                let result = self
                    .exchange_coremeta_inventory(&peer, &root_key_hash, None, 0, 1)
                    .await;
                (peer, result)
            });
        }
        let mut sources = Vec::new();
        let mut failures = Vec::new();
        while let Some((peer, result)) = requests.next().await {
            match result {
                Ok(inventory) => {
                    round.reachable_peers.insert(peer.node_id.clone());
                    sources.push(RecoverySource {
                        peer,
                        final_generation: inventory.final_generation,
                        retention_floor_generation: inventory.retention_floor_generation,
                    });
                }
                Err(error) => failures.push(format!("{}: {error:#}", peer.node_id)),
            }
        }
        sources.sort_by(|left, right| {
            right
                .final_generation
                .cmp(&left.final_generation)
                .then_with(|| left.peer.node_id.cmp(&right.peer.node_id))
        });
        if sources.is_empty() {
            bail!(
                "CoreMeta recovery inventory failed for root {root_key_hash}: {}",
                failures.join("; ")
            );
        }
        Ok(sources)
    }

    async fn fetch_committed_register_anchor(
        &self,
        peers: &[RecoveryPeer],
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Vec<u8>> {
        let profile = self.default_coremeta_quorum_profile()?;
        let required = profile.prepare_quorum;
        let mut candidates =
            BTreeMap::<(Vec<u8>, String, Vec<String>, u64), RootAnchorQuorumCandidate>::new();
        if let Some(shard) = self
            .read_exact_root_register_shard(root_key_hash, generation)
            .await?
        {
            let read = RootAnchorRead {
                root_key_hash: shard.root_key_hash,
                generation: shard.root_generation,
                root_anchor_record: shard.root_anchor_record,
                root_anchor_hash: shard.root_anchor_hash,
                shard_index: u32::from(shard.shard_index),
                register_cohort_node_ids: shard.register_cohort_nodes,
                register_cohort_hash: shard.register_cohort_hash,
                placement_epoch: shard.placement_epoch,
            };
            validate_recovery_root_anchor_read_for_node(
                &self.node_identity.node_id,
                root_key_hash,
                generation,
                &read,
            )?;
            let key = (
                read.root_anchor_record.clone(),
                read.register_cohort_hash.clone(),
                read.register_cohort_node_ids.clone(),
                read.placement_epoch,
            );
            let candidate = candidates
                .entry(key)
                .or_insert_with(|| RootAnchorQuorumCandidate {
                    read: read.clone(),
                    replicas: BTreeMap::new(),
                });
            candidate
                .replicas
                .insert(self.node_identity.node_id.clone(), read.shard_index);
        }
        let mut pending = FuturesUnordered::new();
        for peer in peers.iter().cloned() {
            let root_key_hash = root_key_hash.to_string();
            pending.push(async move {
                let result = self
                    .read_exact_root_replica(&peer, &root_key_hash, generation, false)
                    .await;
                (peer, result)
            });
        }
        let mut failures = Vec::new();
        while let Some((peer, result)) = pending.next().await {
            match result.and_then(|read| {
                let read = read.ok_or_else(|| anyhow!("physical root-register shard not found"))?;
                validate_recovery_root_anchor_read(&peer, root_key_hash, generation, &read)?;
                Ok(read)
            }) {
                Ok(read) => {
                    let key = (
                        read.root_anchor_record.clone(),
                        read.register_cohort_hash.clone(),
                        read.register_cohort_node_ids.clone(),
                        read.placement_epoch,
                    );
                    let candidate =
                        candidates
                            .entry(key)
                            .or_insert_with(|| RootAnchorQuorumCandidate {
                                read: read.clone(),
                                replicas: BTreeMap::new(),
                            });
                    candidate.replicas.insert(peer.node_id, read.shard_index);
                }
                Err(error) => failures.push(format!("{}: {error:#}", peer.node_id)),
            }
        }

        let mut committed = candidates
            .into_values()
            .filter(|candidate| {
                candidate.read.register_cohort_node_ids.len() == profile.replica_count
                    && candidate
                        .read
                        .register_cohort_node_ids
                        .iter()
                        .all(|node| !crate::mesh_lifecycle::is_synthetic_control_node_id(node))
                    && candidate.replicas.len() >= required
                    && candidate
                        .replicas
                        .values()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .len()
                        >= required
            })
            .collect::<Vec<_>>();
        if committed.len() > 1 {
            bail!(
                "root-register recovery found conflicting quorum generations: root={root_key_hash} generation={generation}"
            );
        }
        let candidate = committed.pop().ok_or_else(|| {
                anyhow!(
                    "root-register recovery has no matching quorum: root={root_key_hash} generation={generation} required={required}: {}",
                    failures.join("; ")
                )
            })?;
        let anchor = decode_root_anchor_record(&candidate.read.root_anchor_record)?;
        self.ensure_local_committed_root_register_shard(
            &anchor,
            &candidate.read.root_anchor_record,
            candidate.read.generation.saturating_sub(1),
            &candidate.read.register_cohort_node_ids,
            &candidate.read.register_cohort_hash,
            candidate.read.placement_epoch,
        )
        .await?;
        Ok(candidate.read.root_anchor_record)
    }

    pub(in crate::core_store::local) async fn fetch_committed_cache_anchor(
        &self,
        peers: &[RecoveryPeer],
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<Vec<u8>>> {
        let required = self.default_coremeta_quorum_profile()?.prepare_quorum;
        let mut candidates = BTreeMap::<Vec<u8>, BTreeSet<String>>::new();
        if let Some(anchor) = self
            .read_committed_root_anchor_generation(root_key_hash, generation)
            .await?
        {
            let anchor_record = encode_root_anchor_record(&anchor)?;
            let read = RootAnchorRead {
                root_key_hash: anchor.root_key_hash,
                generation: anchor.root_generation,
                root_anchor_hash: format!("sha256:{}", sha256_hex(&anchor_record)),
                root_anchor_record: anchor_record,
                shard_index: 0,
                register_cohort_node_ids: Vec::new(),
                register_cohort_hash: String::new(),
                placement_epoch: 0,
            };
            validate_recovery_committed_cache_read(root_key_hash, generation, &read)?;
            candidates
                .entry(read.root_anchor_record)
                .or_default()
                .insert(self.node_identity.node_id.clone());
        }
        let mut pending = FuturesUnordered::new();
        for peer in peers.iter().cloned() {
            let root_key_hash = root_key_hash.to_string();
            pending.push(async move {
                let result = self
                    .read_exact_root_replica(&peer, &root_key_hash, generation, true)
                    .await;
                (peer, result)
            });
        }
        let mut failures = Vec::new();
        while let Some((peer, result)) = pending.next().await {
            match result.and_then(|read| {
                let read =
                    read.ok_or_else(|| anyhow!("committed root-cache generation not found"))?;
                validate_recovery_committed_cache_read(root_key_hash, generation, &read)?;
                Ok(read)
            }) {
                Ok(read) => {
                    candidates
                        .entry(read.root_anchor_record)
                        .or_default()
                        .insert(peer.node_id);
                }
                Err(error) => failures.push(format!("{}: {error:#}", peer.node_id)),
            }
        }

        let mut committed = candidates
            .into_iter()
            .filter(|(_, replicas)| replicas.len() >= required)
            .collect::<Vec<_>>();
        if committed.len() > 1 {
            bail!(
                "CoreMeta recovery found conflicting committed participant roots: root={root_key_hash} generation={generation}"
            );
        }
        if committed.is_empty() && !failures.is_empty() {
            tracing::debug!(
                root_key_hash,
                generation,
                required,
                failures = %failures.join("; "),
                "CoreMeta recovery will derive a participant anchor from committed generation evidence"
            );
        }
        Ok(committed.pop().map(|(anchor, _)| anchor))
    }

    async fn read_exact_root_replica(
        &self,
        peer: &RecoveryPeer,
        root_key_hash: &str,
        generation: u64,
        committed_cache: bool,
    ) -> Result<Option<RootAnchorRead>> {
        let bearer = self.coremeta_recovery_bearer()?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode root-register recovery bearer token")?;
        let request_body = ReadRootRequest {
            header: Some(self.internal_request_header("root.read")?),
            root_key_hash: root_key_hash.to_string(),
            min_generation: generation,
            exact_generation: Some(generation),
            committed_cache,
        };
        let operation = self.internal_grpc_request(
            &peer.public_api_addr,
            "read root-register recovery replica",
            move |channel| {
                let mut client = RootRegisterInternalClient::new(channel);
                let body = request_body.clone();
                let authorization = authorization.clone();
                async move {
                    let mut request = tonic::Request::new(body);
                    request
                        .metadata_mut()
                        .insert("authorization", authorization);
                    match client.read_root(request).await {
                        Ok(response) => Ok(Some(response.into_inner())),
                        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
                        Err(status) => Err(status),
                    }
                }
            },
        );
        tokio::time::timeout(RECOVERY_RPC_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow!("root-register recovery read timed out"))?
    }

    pub(in crate::core_store::local) fn coremeta_recovery_cursor(
        &self,
        root_key_hash: &str,
    ) -> Result<Option<CoreMetaHistoryCursor>> {
        let generation = self.coremeta_recovery_published_generation(root_key_hash)?;
        if generation == 0 {
            return Ok(None);
        }
        let descriptor = self
            .read_generation_descriptor(root_key_hash, generation)?
            .ok_or_else(|| {
                anyhow!(
                    "CoreMeta recovery published generation descriptor is missing: root={root_key_hash} generation={generation}"
                )
            })?;
        Ok(Some(CoreMetaHistoryCursor {
            generation,
            ordinal: descriptor.mutation_count.saturating_sub(1),
        }))
    }

    fn recovery_bundle_is_ready(&self, bundle: &CoreMetaRecoveryPublicationBundle) -> Result<bool> {
        for (root_key_hash, generation) in &bundle.scopes {
            let published = self.coremeta_recovery_published_generation(root_key_hash)?;
            if published >= *generation {
                continue;
            }
            if published.saturating_add(1) != *generation
                || self
                    .read_complete_coremeta_generation_for_recovery(root_key_hash, *generation)?
                    .is_none()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn verify_coremeta_recovery_convergence(&self, round: &mut RecoveryRound) -> Result<()> {
        round
            .known_roots
            .extend(self.coremeta_recovery_root_hashes()?);
        round.lagging_roots.clear();
        let roots = round.known_roots.iter().cloned().collect::<Vec<_>>();
        for root_key_hash in roots {
            let local_generation = self.coremeta_recovery_published_generation(&root_key_hash)?;
            if remote_root_needs_inventory(local_generation, round.remote_heads.get(&root_key_hash))
            {
                round.lagging_roots.insert(root_key_hash);
                continue;
            }
            self.verify_root_directory_head_agreement(
                &root_key_hash,
                local_generation,
                round.remote_heads.get(&root_key_hash),
            )
            .await?;
        }
        Ok(())
    }

    async fn verify_root_directory_head_agreement(
        &self,
        root_key_hash: &str,
        local_generation: u64,
        remote_heads: Option<&BTreeMap<String, RootDirectoryEntry>>,
    ) -> Result<()> {
        if local_generation == 0 {
            return Ok(());
        }
        let Some(remote_heads) = remote_heads else {
            return Ok(());
        };
        let local = self
            .read_internal_root_anchor_by_hash(root_key_hash, local_generation)
            .await?;
        for (node_id, remote) in remote_heads {
            if remote.root_generation == local_generation
                && remote.root_anchor_hash != local.root_anchor_hash
            {
                bail!(
                    "CoreMeta equal peers diverged at root-directory head: root={root_key_hash} generation={local_generation} peer={node_id}"
                );
            }
        }
        Ok(())
    }

    async fn verify_recovery_generation_agreement(
        &self,
        source: &RecoverySource,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<()> {
        if generation == 0 || source.final_generation < generation {
            return Ok(());
        }
        let inventory = self
            .exchange_coremeta_inventory(
                &source.peer,
                root_key_hash,
                inventory_cursor_before(generation),
                generation,
                1,
            )
            .await?;
        let remote = inventory
            .descriptors
            .first()
            .ok_or_else(|| anyhow!("CoreMeta peer omitted its converged generation"))?;
        let local = self
            .read_generation_descriptor(root_key_hash, generation)?
            .ok_or_else(|| anyhow!("CoreMeta local converged descriptor is missing"))?;
        if remote.generation_hash != local.generation_hash || remote != &local {
            bail!("CoreMeta equal peers diverged: root={root_key_hash} generation={generation}");
        }
        Ok(())
    }

    fn validate_coremeta_recovery_inventory(
        &self,
        root_key_hash: &str,
        inventory: &CoreMetaInventory,
    ) -> Result<()> {
        if inventory.root_key_hash != root_key_hash
            || inventory.retention_floor_generation > inventory.final_generation
                && inventory.final_generation != 0
        {
            bail!("CoreMeta recovery inventory scope is invalid");
        }
        let mut previous = None;
        for descriptor in &inventory.descriptors {
            validate_descriptor(descriptor)?;
            if descriptor.root_key_hash != root_key_hash
                || descriptor.generation > inventory.final_generation
                || previous.is_some_and(|value| value >= descriptor.generation)
            {
                bail!("CoreMeta recovery inventory descriptors are invalid");
            }
            previous = Some(descriptor.generation);
        }
        if !inventory.descriptors.is_empty()
            && inventory.page_hash != inventory_page_hash(root_key_hash, &inventory.descriptors)
        {
            bail!("CoreMeta recovery inventory page hash mismatch");
        }
        Ok(())
    }

    fn coremeta_recovery_bearer(&self) -> Result<&str> {
        self.node_identity
            .internal_bearer_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| anyhow!("CoreMeta distributed recovery requires an internal token"))
    }

    fn update_coremeta_recovery_progress(
        &self,
        in_progress: bool,
        ready: Option<bool>,
        error: Option<String>,
    ) {
        if let Some(ready) = ready {
            self.coremeta_recovery.ready.store(ready, Ordering::Release);
        }
        let mut snapshot = self
            .coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.in_progress = in_progress;
        if let Some(ready) = ready {
            snapshot.ready = ready;
        }
        snapshot.last_error = error;
    }

    pub(in crate::core_store::local) fn mark_coremeta_recovery_unready(&self) {
        if !self
            .coremeta_recovery
            .distributed_required
            .load(Ordering::Acquire)
        {
            return;
        }
        let targets = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.coremeta_recovery
            .readiness_epoch
            .fetch_add(1, Ordering::AcqRel);
        self.set_coremeta_recovery_unready_locked();
        drop(targets);
        self.coremeta_recovery.wake.notify_one();
    }

    fn finish_coremeta_recovery_round(
        &self,
        round: &RecoveryRound,
        candidate_ready: bool,
        error: Option<String>,
    ) -> bool {
        let completed_rounds = self
            .coremeta_recovery
            .completed_rounds
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        let targets = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current_epoch = self
            .coremeta_recovery
            .readiness_epoch
            .load(Ordering::Acquire);
        let ready = candidate_ready && round.readiness_epoch == current_epoch && targets.is_empty();
        self.coremeta_recovery.ready.store(ready, Ordering::Release);
        let mut snapshot = self
            .coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.ready = ready;
        snapshot.in_progress = false;
        snapshot.reachable_peers = round.reachable_peers.len();
        snapshot.known_roots = round.known_roots.len();
        snapshot.lagging_roots = round.lagging_roots.len();
        snapshot.root_directory_complete = round.root_directory_complete;
        snapshot.canonical_settlement_complete = round.canonical_settlement_complete;
        snapshot.physical_register_quorum_complete = round.physical_register_quorum_complete;
        snapshot.completed_rounds = completed_rounds;
        snapshot.last_error = error;
        drop(snapshot);
        drop(targets);
        ready
    }

    fn finish_stale_recovery_publication_retry(&self, error: String) {
        let completed_rounds = self
            .coremeta_recovery
            .completed_rounds
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        let ready = self.coremeta_recovery.ready.load(Ordering::Acquire);
        let mut snapshot = self
            .coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.ready = ready;
        snapshot.in_progress = false;
        snapshot.completed_rounds = completed_rounds;
        snapshot.last_error = Some(error);
    }
}

fn root_directory_page_hash(
    after_root_key_hash: &str,
    entries: &[RootDirectoryEntry],
    next_root_key_hash: &str,
    directory_complete: bool,
    encoded_bytes: u64,
) -> String {
    let mut bytes = b"anvil.coremeta.root_directory.page.v1".to_vec();
    append_root_directory_hash_part(&mut bytes, after_root_key_hash.as_bytes());
    for entry in entries {
        append_root_directory_hash_part(&mut bytes, entry.root_key_hash.as_bytes());
        bytes.extend_from_slice(&entry.root_generation.to_be_bytes());
        append_root_directory_hash_part(&mut bytes, entry.root_anchor_hash.as_bytes());
    }
    append_root_directory_hash_part(&mut bytes, next_root_key_hash.as_bytes());
    bytes.push(u8::from(directory_complete));
    bytes.extend_from_slice(&encoded_bytes.to_be_bytes());
    format!("sha256:{}", sha256_hex(&bytes))
}

fn append_root_directory_hash_part(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn inventory_cursor_before(generation: u64) -> Option<CoreMetaInventoryCursor> {
    generation
        .checked_sub(1)
        .filter(|previous| *previous != 0)
        .map(|previous| CoreMetaInventoryCursor {
            generation: previous,
        })
}

#[cfg(test)]
#[path = "local_coremeta_recovery/tests.rs"]
mod tests;
