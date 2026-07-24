use super::*;

impl CoreStore {
    pub(super) fn begin_coremeta_recovery_round(&self) -> u64 {
        let _targets = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.coremeta_recovery
            .readiness_epoch
            .load(Ordering::Acquire)
    }

    pub(super) fn coremeta_root_repair_targets(&self) -> BTreeMap<String, u64> {
        self.coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub(crate) fn request_coremeta_root_repair(&self, root_key_hash: &str, target_generation: u64) {
        if !self.coremeta_distributed_recovery_required() {
            return;
        }
        let mut targets = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let target = targets.entry(root_key_hash.to_string()).or_default();
        if *target < target_generation {
            *target = target_generation;
            self.coremeta_recovery
                .readiness_epoch
                .fetch_add(1, Ordering::AcqRel);
        }
        self.set_coremeta_recovery_unready_locked();
        drop(targets);
        self.coremeta_recovery.wake.notify_one();
    }

    pub(super) fn complete_coremeta_root_repair(&self, root_key_hash: &str, local_generation: u64) {
        let mut targets = self
            .coremeta_recovery
            .requested_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if targets
            .get(root_key_hash)
            .is_some_and(|target| *target <= local_generation)
        {
            targets.remove(root_key_hash);
        }
    }

    pub(super) fn set_coremeta_recovery_unready_locked(&self) {
        self.coremeta_recovery.ready.store(false, Ordering::Release);
        let mut snapshot = self
            .coremeta_recovery
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        snapshot.ready = false;
        snapshot.in_progress = true;
    }
}

pub(super) fn next_recovery_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(RECOVERY_MAX_BACKOFF)
}

pub(super) fn is_stale_recovery_publication(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<CoreStoreCommitError>(),
            Some(CoreStoreCommitError::RootChangedBeforeDurableStaging { .. })
        )
    })
}

pub(super) fn root_directory_quorum_is_settled(
    state: &StdMutex<RootDirectoryScanState>,
    reachable_peers: &BTreeSet<String>,
    authoritative_remote_peers: &BTreeSet<String>,
    minimum_remote_recovery_peers: usize,
) -> bool {
    if minimum_remote_recovery_peers == 0 {
        return true;
    }
    if reachable_peers
        .intersection(authoritative_remote_peers)
        .count()
        < minimum_remote_recovery_peers
    {
        return false;
    }
    let state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .peers_with_complete_pass
        .iter()
        .filter(|node_id| {
            reachable_peers.contains(*node_id) && authoritative_remote_peers.contains(*node_id)
        })
        .count()
        >= minimum_remote_recovery_peers
}

pub(super) fn remote_recovery_acknowledgements(
    prepare_quorum: usize,
    local_is_replica: bool,
) -> usize {
    prepare_quorum.saturating_sub(usize::from(local_is_replica))
}

pub(super) fn recovery_round_is_ready(round: &RecoveryRound) -> bool {
    recovery_round_is_serviceable(round)
        && round.unresolved_publication_intents.is_empty()
        && round.pending_mutations_complete
}

pub(super) fn recovery_round_preserves_admitted_readiness(round: &RecoveryRound) -> bool {
    !round.reachable_peers.is_empty()
        && round.root_directory_complete
        && round.canonical_settlement_complete
        && round.physical_register_quorum_complete
}

pub(super) fn recovery_round_can_replay_pending_mutations(round: &RecoveryRound) -> bool {
    round.unresolved_publication_intents.is_empty() && recovery_round_is_serviceable(round)
}

pub(super) fn recovery_round_is_serviceable(round: &RecoveryRound) -> bool {
    !round.reachable_peers.is_empty()
        && round.root_directory_complete
        && round.canonical_settlement_complete
        && round.physical_register_quorum_complete
        && round.lagging_roots.is_empty()
        && round.pending_bundles.is_empty()
}

pub(super) fn recovered_anchors_are_ready(
    bundle: &CoreMetaRecoveryPublicationBundle,
    anchors: &BTreeMap<(String, u64), Vec<u8>>,
) -> bool {
    bundle
        .scopes
        .iter()
        .all(|scope| anchors.contains_key(scope))
}

pub(super) fn validate_recovery_root_anchor_read(
    peer: &RecoveryPeer,
    root_key_hash: &str,
    generation: u64,
    read: &RootAnchorRead,
) -> Result<()> {
    validate_recovery_root_anchor_read_for_node(&peer.node_id, root_key_hash, generation, read)
}

pub(super) fn validate_recovery_root_anchor_read_for_node(
    node_id: &str,
    root_key_hash: &str,
    generation: u64,
    read: &RootAnchorRead,
) -> Result<()> {
    let shard_index = usize::try_from(read.shard_index)
        .map_err(|_| anyhow!("root-register recovery shard index overflow"))?;
    if read.root_key_hash != root_key_hash
        || read.generation != generation
        || read.register_cohort_node_ids.len() != 3
        || shard_index >= read.register_cohort_node_ids.len()
        || read.register_cohort_node_ids[shard_index] != node_id
        || read.placement_epoch == 0
        || read.root_anchor_hash != format!("sha256:{}", sha256_hex(&read.root_anchor_record))
        || read.register_cohort_hash
            != root_register_cohort_hash(root_key_hash, generation, &read.register_cohort_node_ids)
    {
        bail!("root-register recovery replica provenance is invalid");
    }
    let anchor = decode_root_anchor_record(&read.root_anchor_record)?;
    validate_root_anchor_record(&anchor)?;
    if anchor.root_key_hash != root_key_hash || anchor.root_generation != generation {
        bail!("root-register recovery anchor scope is invalid");
    }
    Ok(())
}

pub(super) fn validate_recovery_committed_cache_read(
    root_key_hash: &str,
    generation: u64,
    read: &RootAnchorRead,
) -> Result<()> {
    if read.root_key_hash != root_key_hash
        || read.generation != generation
        || read.root_anchor_hash != format!("sha256:{}", sha256_hex(&read.root_anchor_record))
        || read.shard_index != 0
        || !read.register_cohort_node_ids.is_empty()
        || !read.register_cohort_hash.is_empty()
        || read.placement_epoch != 0
    {
        bail!("CoreMeta recovery participant-root provenance is invalid");
    }
    let anchor = decode_root_anchor_record(&read.root_anchor_record)?;
    validate_root_anchor_record(&anchor)?;
    if anchor.root_key_hash != root_key_hash || anchor.root_generation != generation {
        bail!("CoreMeta recovery participant-root scope is invalid");
    }
    Ok(())
}

pub(in crate::core_store::local) fn validate_recovery_publication_anchor(
    bundle: &CoreMetaRecoveryPublicationBundle,
    scope: &(String, u64),
    anchor_bytes: &[u8],
) -> Result<()> {
    if !bundle.scopes.contains(scope) {
        bail!("CoreMeta recovery anchor is outside its publication bundle");
    }
    let anchor = decode_root_anchor_record(anchor_bytes)?;
    validate_root_anchor_record(&anchor)?;
    if anchor.root_key_hash != scope.0
        || anchor.root_generation != scope.1
        || publication_transaction_id(&anchor)? != bundle.transaction_id
    {
        bail!("CoreMeta recovery anchor does not match its publication bundle");
    }
    Ok(())
}

pub(super) fn highest_remote_root_generation(
    remote_heads: Option<&BTreeMap<String, RootDirectoryEntry>>,
) -> u64 {
    remote_heads
        .into_iter()
        .flat_map(BTreeMap::values)
        .map(|entry| entry.root_generation)
        .max()
        .unwrap_or(0)
}

pub(super) fn remote_root_needs_inventory(
    local_generation: u64,
    remote_heads: Option<&BTreeMap<String, RootDirectoryEntry>>,
) -> bool {
    highest_remote_root_generation(remote_heads) > local_generation
}
