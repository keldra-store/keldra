use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::path::Path;

use anyhow::{Context, Result, bail};
use keldra_atomic_program::{
    CommandReceipt, EngineError, ExpandedProgramPath, InvocationContext, ObjectPath, ProgramInput,
    ProgramInvocation,
};
use keldra_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyError, ApplyResult, AtomicBundleAuthority, BeginBatch,
    BundleHash, BundleRef, Command, CommitPreparedBatch, CommittedBatch, CommittedInvocation,
    DecisionRaft, DecisionRaftError, DurabilityClass, DurabilityEvidenceHash, ExecutorNomination,
    InvocationFingerprint, InvocationId, NodeId, ParticipantManifestHash,
    ProgramHash as DecisionProgramHash, ProgramPathHash,
};
use keldra_store::{
    CommittedProgramResult, ObjectKey, ObjectMutationContext, PlacementLogId, PreparedBundleHash,
    PreparedBundleRef, ProgramBundleAuthority, ProgramCommit, ProgramDurabilityClassHash,
    ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramHash, ProgramReservation,
    ProgramStoreError, PublishedProgramVersion, Store, VerifiedProgramDefinition,
};
use serde::{Deserialize, Serialize};
use tonic::Status;
use tracing::Instrument as _;

mod builtin;
mod distributed;
mod recovery;

pub(crate) use builtin::builtin_invocation_identity;
use distributed::DistributedPrograms;

pub(crate) const MAX_PROGRAM_INPUT_BYTES: usize = 16 * 1024 * 1024;
const PROGRAM_PATH_PREFIX: &str = "_keldra/programs/";
const LOCAL_DURABILITY_CLASS: &str = "local";
const REPLICATED_DURABILITY_CLASS: &str = "replicated";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgramRuntimeTopology {
    OneNode,
    Clustered,
}
const ATOMIC_VISIBILITY_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Clone)]
pub(crate) struct ProgramCoordinator {
    store: Store,
    decisions: DecisionRaft,
    node: NodeId,
    /// Serializes the one global Raft visibility decision with local
    /// finalization. Program evaluation and exact-path locking remain
    /// concurrent before this short boundary.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    distributed: Arc<std::sync::OnceLock<DistributedPrograms>>,
    recovery_worker: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

/// Late binding used by the private join listener, which must begin accepting
/// connections before the distributed program coordinator can be assembled.
#[derive(Clone, Default)]
pub(crate) struct LateBoundProgramQuiescence {
    coordinator: Arc<std::sync::OnceLock<ProgramCoordinator>>,
}

pub(crate) struct ProgramQuiescenceGuard {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl LateBoundProgramQuiescence {
    pub(crate) fn install(
        &self,
        coordinator: ProgramCoordinator,
    ) -> Result<(), ProgramCoordinator> {
        self.coordinator.set(coordinator)
    }

    pub(crate) async fn quiesce_for_membership(&self) -> Result<ProgramQuiescenceGuard, Status> {
        self.coordinator
            .get()
            .ok_or_else(|| Status::unavailable("atomic program coordinator is not ready"))?
            .quiesce_for_membership()
            .await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct InvokedProgramResult {
    pub receipt: CommandReceipt,
    pub executor_nomination_log_index: u64,
    pub commit_log_index: u64,
    pub program_hash: [u8; 32],
    pub published_versions: BTreeMap<ObjectPath, PublishedProgramVersion>,
    pub asserted_versions: BTreeMap<ObjectPath, keldra_store::Version>,
    pub alias_targets: BTreeMap<ObjectPath, ObjectPath>,
    pub replayed: bool,
    pub replay_guarantee_expires_at_unix_millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BuiltInReplayLookup {
    pub original_index: u32,
    pub authority_kind: u16,
    pub contract_version: u16,
    pub invocation_id: [u8; 32],
    pub input_fingerprint: [u8; 32],
}

/// True only when this node's applied Raft state has no committed atomic batch
/// awaiting finalization. Callers perform their data read first and discard it
/// when this returns false.
pub(crate) fn atomic_tail_is_clear(decisions: &DecisionRaft) -> Result<bool, Status> {
    let state = decisions.state().map_err(decision_status)?;
    Ok(state.preparing_batch().is_none() && state.unfinalized_commit_len() == 0)
}

/// Wait for the locally applied atomic tail to become fully finalized. The
/// caller must re-read data afterwards because its discarded snapshot may have
/// been taken while only part of a batch was physically finalized.
pub(crate) async fn wait_for_atomic_tail(
    decisions: &DecisionRaft,
    budget: Duration,
) -> Result<(), Status> {
    wait_for_visibility(budget, || atomic_tail_is_clear(decisions)).await
}

/// Fence one exact current-head read using the program cursor already carried
/// by its mutation stamp. A node that has not yet applied that cursor waits;
/// the head is exposed only after its applied Raft state includes the global
/// FinalizedThrough decision.
pub(crate) async fn wait_for_program_cursor(
    decisions: &DecisionRaft,
    cursor: Option<u64>,
    budget: Duration,
) -> Result<(), Status> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor == 0 {
        return Err(Status::data_loss(
            "atomic-program head carries a zero commit cursor",
        ));
    }
    wait_for_visibility(budget, || program_cursor_is_visible(decisions, cursor)).await
}

pub(crate) fn program_cursor_is_visible(
    decisions: &DecisionRaft,
    cursor: u64,
) -> Result<bool, Status> {
    if cursor == 0 {
        return Err(Status::data_loss(
            "atomic-program head carries a zero commit cursor",
        ));
    }
    let state = decisions.state().map_err(decision_status)?;
    if state
        .finalized_through()
        .is_some_and(|finalized| finalized >= cursor)
    {
        return Ok(true);
    }
    if state.committed_invocation(cursor).is_some() {
        return Ok(false);
    }
    if state
        .last_commit_cursor()
        .is_some_and(|last| last >= cursor)
    {
        return Err(Status::data_loss(
            "atomic-program head names an unknown committed invocation",
        ));
    }
    Ok(false)
}

async fn wait_for_visibility(
    budget: Duration,
    mut visible: impl FnMut() -> Result<bool, Status>,
) -> Result<(), Status> {
    let deadline = Instant::now()
        .checked_add(budget)
        .ok_or_else(|| Status::invalid_argument("visibility deadline overflowed"))?;
    loop {
        if visible()? {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Status::deadline_exceeded(
                "atomic-program visibility deadline exceeded",
            ));
        }
        tokio::time::sleep(remaining.min(ATOMIC_VISIBILITY_POLL_INTERVAL)).await;
    }
}

impl ProgramCoordinator {
    pub(crate) fn cursor_is_visible(&self, cursor: u64) -> Result<bool, Status> {
        program_cursor_is_visible(&self.decisions, cursor)
    }

    pub(crate) async fn wait_for_cursor(
        &self,
        cursor: u64,
        budget: Duration,
    ) -> Result<(), Status> {
        wait_for_program_cursor(&self.decisions, Some(cursor), budget).await
    }

    /// Stop local program commits across an ACTIVE-placement cutover. The
    /// Raft leader first fences any previous executor nomination, then recovers
    /// the bounded committed tail before returning the held local gate.
    pub(crate) async fn quiesce_for_membership(&self) -> Result<ProgramQuiescenceGuard, Status> {
        let guard = self.commit_gate.clone().lock_owned().await;
        self.decisions
            .confirm_leadership()
            .await
            .map_err(decision_status)?;
        let state = self.decisions.state().map_err(decision_status)?;
        let local = state.cluster_control().nodes().get(&self.node);
        if local.is_none_or(|descriptor| descriptor.state != keldra_consensus::NodeState::Active) {
            return Err(Status::failed_precondition(
                "membership cutover leader is not an ACTIVE node",
            ));
        }
        if state
            .executor()
            .is_none_or(|nomination| nomination.executor != self.node)
        {
            let nomination = self
                .decisions
                .submit(Command::NominateExecutor {
                    executor: self.node,
                })
                .await
                .map_err(decision_status)?;
            expect_nomination(nomination.result, self.node).map_err(internal)?;
        }
        if self.distributed.get().is_some() {
            self.recover_distributed_tail_locked().await?;
        } else {
            self.recover_committed_tail_locked()
                .await
                .map_err(internal)?;
        }
        self.sweep_stale_local_reservations()
            .await
            .map_err(internal)?;
        if !atomic_tail_is_clear(&self.decisions)? {
            return Err(Status::unavailable(
                "atomic program tail is not finalized before membership cutover",
            ));
        }
        if !self
            .store
            .program_reservations()
            .map_err(mutation_status)?
            .is_empty()
        {
            return Err(Status::unavailable(
                "durable atomic reservations remain before membership cutover",
            ));
        }
        self.decisions
            .confirm_leadership()
            .await
            .map_err(decision_status)?;
        Ok(ProgramQuiescenceGuard { _guard: guard })
    }

    /// Attach the atomic-program coordinator to a Raft instance already
    /// opened and initialized by the server runtime.
    pub async fn start(store: Store, decisions: DecisionRaft, node: NodeId) -> Result<Self> {
        if decisions.state()?.executor().is_none() && decisions.current_leader() == Some(node.0) {
            let _nomination = expect_nomination(
                decisions
                    .submit(Command::NominateExecutor { executor: node })
                    .await
                    .context("nominate the leader as atomic executor")?
                    .result,
                node,
            )?;
            tracing::info!(
                monotonic_counter.keldra_atomic_program_executor_nominations_total = 1_u64,
                "atomic program executor nominated"
            );
        }

        let coordinator = Self {
            store,
            decisions,
            node,
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
            distributed: Arc::new(std::sync::OnceLock::new()),
            recovery_worker: Arc::new(std::sync::Mutex::new(None)),
        };
        coordinator
            .sweep_stale_local_reservations()
            .await
            .context("clear reservations whose Raft authority is finalized or aborted")?;
        if coordinator
            .decisions
            .state()?
            .cluster_control()
            .nodes()
            .len()
            <= 1
        {
            coordinator
                .recover_committed_tail()
                .await
                .context("recover committed atomic-program bundles")?;
        }
        coordinator.spawn_recovery_worker();
        coordinator.emit_bounded_state_metrics();
        Ok(coordinator)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn install_distributed(
        &self,
        reader: crate::cluster_object_read::ClusterObjectReader,
        objects: crate::object_distribution::ObjectDistribution,
        peers: crate::cluster_peer::ClusterPeerTransport,
        names: crate::logical_name_resolution::LogicalNameResolver,
    ) -> Result<()> {
        self.distributed
            .set(DistributedPrograms::new(
                self.node,
                self.store.clone(),
                reader,
                objects,
                peers,
                names,
            ))
            .map_err(|_| anyhow::anyhow!("distributed atomic programs were installed twice"))?;
        if self
            .decisions
            .state()?
            .executor()
            .is_some_and(|nomination| nomination.executor == self.node)
        {
            let _guard = self.commit_gate.lock().await;
            self.recover_distributed_tail_locked()
                .await
                .context("recover distributed atomic-program tail")?;
        }
        Ok(())
    }

    /// Execute only on the nominated node. The API layer performs Zanzibar
    /// checks through `authorize` after deterministic path expansion and
    /// before the evaluator takes locks or reads payloads.
    pub async fn invoke<L, C>(
        &self,
        program_key: ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        authorize_logical: L,
        authorize_canonical: C,
    ) -> Result<InvokedProgramResult, Status>
    where
        L: Fn(&ExpandedProgramPath) -> Result<(), Status>,
        C: Fn(&ExpandedProgramPath) -> Result<(), Status>,
    {
        let invocation_hash = invocation_identity(&program_key, &invocation_id);
        let invocation_hash = hex::encode(invocation_hash.0);
        let program_hash = hex::encode(expected_program_hash);
        let invocation_started = Instant::now();
        let span = tracing::info_span!(
            "keldra.atomic_program.invoke",
            invocation.hash = %invocation_hash,
            program.hash = %program_hash,
            nomination.log_index = tracing::field::Empty,
            commit.log_index = tracing::field::Empty,
        );
        let result = self
            .invoke_in_span(
                program_key,
                expected_program_hash,
                invocation_id,
                input_json,
                durability_class,
                authorize_logical,
                authorize_canonical,
            )
            .instrument(span.clone())
            .await;
        span.in_scope(|| {
            tracing::info!(
                histogram.keldra_atomic_program_total_duration_seconds =
                    invocation_started.elapsed().as_secs_f64(),
                "atomic program invocation completed"
            );
        });
        result
    }

    async fn invoke_in_span<L, C>(
        &self,
        program_key: ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        authorize_logical: L,
        authorize_canonical: C,
    ) -> Result<InvokedProgramResult, Status>
    where
        L: Fn(&ExpandedProgramPath) -> Result<(), Status>,
        C: Fn(&ExpandedProgramPath) -> Result<(), Status>,
    {
        tracing::info!(
            monotonic_counter.keldra_atomic_program_invocations_total = 1_u64,
            "atomic program invocation"
        );
        validate_program_request(
            &program_key,
            &invocation_id,
            input_json,
            durability_class,
            ProgramRuntimeTopology::OneNode,
        )?;
        self.require_generalized_atomic_paths()?;
        let nomination = self.current_nomination()?;
        tracing::Span::current().record("nomination.log_index", nomination.nomination_log_index);

        let object = self
            .store
            .get(&program_key)
            .await
            .map_err(mutation_status)?
            .ok_or_else(|| Status::not_found("program definition object was not found"))?;
        let program_hash = ProgramHash(expected_program_hash);
        let program = VerifiedProgramDefinition::from_bytes(&object.bytes, program_hash)
            .map_err(program_store_status)?;
        let context =
            InvocationContext::new(program_key.tenant()).map_err(Status::invalid_argument)?;
        let input = if input_json.is_empty() {
            ProgramInput::default()
        } else {
            serde_json::from_slice::<ProgramInput>(input_json).map_err(|error| {
                Status::invalid_argument(format!("invalid program input JSON: {error}"))
            })?
        };
        let program_path_hash = program_path_hash(&program_key);
        let invocation = ProgramInvocation::from_input(program_path_hash, invocation_id, input)
            .map_err(Status::invalid_argument)?;
        let fingerprint = decode_fingerprint(&invocation.input_fingerprint)?;
        let consensus_invocation_id = invocation_identity(&program_key, &invocation.command_id);

        let engine = self.store.program_engine(&program).map_err(engine_status)?;
        self.recover_committed_tail().await.map_err(internal)?;

        loop {
            let expanded = engine
                .expanded_paths(&context, &invocation)
                .map_err(engine_status)?;
            for path in &expanded {
                authorize_logical(path)?;
            }
            let replay_clock = current_unix_millis().map_err(internal)?;
            if let Some(invocation) = self
                .decisions
                .state()
                .map_err(decision_status)?
                .replay_entry(consensus_invocation_id, replay_clock)
            {
                let result = self
                    .replay_committed_invocation(
                        invocation,
                        fingerprint,
                        program_path_hash,
                        expected_program_hash,
                    )
                    .await?;
                authorize_sealed_canonical_paths(
                    &expanded,
                    &result.alias_targets,
                    &authorize_canonical,
                )?;
                return Ok(result);
            }

            let alias_bindings = self
                .store
                .resolve_program_alias_bindings(&expanded)
                .await
                .map_err(program_store_status)?;
            let canonical_paths = alias_bindings
                .iter()
                .map(|binding| {
                    (
                        binding.requested_path.clone(),
                        binding.canonical_path.clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            authorize_canonical_bindings(&expanded, &canonical_paths, &authorize_canonical)?;

            let prepare_started = Instant::now();
            let lease = engine
                .prepare_canonicalized(&context, &invocation, &canonical_paths)
                .await
                .map_err(engine_status)?;

            let prepared = self
                .store
                .prepare_program_bundle_with_aliases(&lease, &alias_bindings)
                .await
                .map_err(program_store_status)?;
            tracing::info!(
                histogram.keldra_atomic_program_prepare_duration_seconds =
                    prepare_started.elapsed().as_secs_f64(),
                histogram.keldra_atomic_program_prepared_bytes = prepared.bundle.length,
                "atomic program bundle prepared"
            );
            let evidence_hash = accepted_program_evidence_hash(
                &prepared.durability.scope,
                prepared.durability_evidence_hash,
                durability_class,
                self.node,
            )?;
            let record = self
                .store
                .prepared_program_record(&prepared)
                .await
                .map_err(program_store_status)?;

            let _commit_guard = self.commit_gate.lock().await;
            let replay_clock = current_unix_millis().map_err(internal)?;
            if let Some(invocation) = self
                .decisions
                .state()
                .map_err(decision_status)?
                .replay_entry(consensus_invocation_id, replay_clock)
            {
                drop(prepared);
                drop(lease);
                require_same_invocation(
                    invocation,
                    fingerprint,
                    program_path_hash,
                    expected_program_hash,
                )?;
                return self
                    .load_committed_invocation_result(invocation, true)
                    .await;
            }
            let decision_state = self.decisions.state().map_err(decision_status)?;
            let consensus_cursor = decision_state.last_commit_cursor();
            let applied_cursor = self
                .store
                .applied_program_commit_cursor()
                .map_err(program_store_status)?;
            match compare_commit_cursors(applied_cursor, consensus_cursor) {
                Ordering::Less => {
                    drop(prepared);
                    drop(lease);
                    self.recover_committed_tail_locked()
                        .await
                        .map_err(internal)?;
                    continue;
                }
                Ordering::Greater => {
                    drop(prepared);
                    drop(lease);
                    return Err(Status::data_loss(format!(
                        "local atomic cursor {applied_cursor:?} is ahead of consensus cursor {consensus_cursor:?}",
                    )));
                }
                Ordering::Equal => {}
            }

            let current = self.current_nomination()?;
            if current != nomination {
                return Err(Status::unavailable(
                    "EXECUTOR_MOVED: atomic executor moved while the program was preparing; retry the same invocation id",
                ));
            }
            let mutation_context = self.one_node_mutation_context(current)?;
            let proposal_at_unix_millis = current_unix_millis().map_err(internal)?;
            let replay_expires_at_unix_millis = proposal_at_unix_millis
                .checked_add(ATOMIC_REPLAY_RETENTION_MILLIS)
                .ok_or_else(|| Status::internal("atomic replay expiry overflow"))?;
            let commit_started = Instant::now();
            let begun = self
                .decisions
                .submit(Command::BeginBatch(BeginBatch {
                    executor: self.node,
                    nomination_log_index: nomination.nomination_log_index,
                    authority: decision_bundle_authority(prepared.authority),
                    invocation_id: consensus_invocation_id,
                    input_fingerprint: InvocationFingerprint(fingerprint),
                    bundle_ref: BundleRef {
                        hash: prepared.bundle.hash,
                        length: prepared.bundle.length,
                    },
                    bundle_hash: BundleHash(prepared.hash.0),
                    durability_class: DurabilityClass(
                        ProgramDurabilityClassHash::for_class(durability_class).0,
                    ),
                    durability_evidence_hash: DurabilityEvidenceHash(evidence_hash.0),
                    participant_manifest_hash: ParticipantManifestHash(
                        prepared.participant_manifest_hash,
                    ),
                    proposal_at_unix_millis,
                    replay_expires_at_unix_millis,
                }))
                .await;
            tracing::info!(
                histogram.keldra_atomic_program_commit_duration_seconds =
                    commit_started.elapsed().as_secs_f64(),
                "atomic program decision completed"
            );
            self.emit_bounded_state_metrics();
            let begun = begun.map_err(decision_status)?;
            let prepared_batch = match expect_batch_begun(begun.result)? {
                keldra_consensus::BeginResult::AlreadyCommitted(committed) => {
                    drop(lease);
                    return self
                        .load_committed_invocation_result(committed.invocation, true)
                        .await;
                }
                keldra_consensus::BeginResult::Prepared { batch, .. } => batch,
            };
            let reservations = record
                .reservations(
                    prepared_batch.begin_cursor,
                    consensus_invocation_id.0,
                    prepared.hash,
                    self.node.0,
                    nomination.nomination_log_index,
                    mutation_context.active_placement_log_id,
                )
                .map_err(program_store_status)?;
            if let Err(error) = self.reserve_local_participants(&reservations).await {
                self.abort_prepared_batch(prepared_batch).await?;
                self.release_local_participants(&reservations, None).await?;
                return Err(error);
            }
            let committed = self
                .decisions
                .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
                    executor: self.node,
                    nomination_log_index: nomination.nomination_log_index,
                    begin_cursor: prepared_batch.begin_cursor,
                    invocation_id: consensus_invocation_id,
                    participant_manifest_hash: ParticipantManifestHash(
                        prepared.participant_manifest_hash,
                    ),
                }))
                .await
                .map_err(decision_status)?;
            let committed = expect_batch_committed(committed.result)?;
            record_committed_span(committed.invocation);
            if committed.replayed {
                drop(lease);
                self.recover_committed_tail_locked()
                    .await
                    .map_err(internal)?;
                return self
                    .load_committed_invocation_result(committed.invocation, true)
                    .await;
            }

            self.commit_local_participants(
                &reservations,
                committed.invocation.committed_batch.commit_cursor,
            )
            .await?;

            let result = self
                .store
                .apply_program_bundle(
                    lease,
                    &prepared,
                    program_commit(applied_cursor, committed.invocation.committed_batch),
                    mutation_context,
                )
                .await
                .map_err(program_store_status)?;
            require_result_matches_consensus(&result, committed.invocation)?;
            self.advance_finalized_through(
                nomination,
                committed.invocation.committed_batch.commit_cursor,
            )
            .await?;
            self.release_local_participants(
                &reservations,
                Some(committed.invocation.committed_batch.commit_cursor),
            )
            .await?;
            return Ok(invoked_result(result, committed.invocation, false));
        }
    }

    async fn replay_committed_invocation(
        &self,
        invocation: CommittedInvocation,
        fingerprint: [u8; 32],
        program_path_hash: [u8; 32],
        program_hash: [u8; 32],
    ) -> Result<InvokedProgramResult, Status> {
        require_same_invocation(invocation, fingerprint, program_path_hash, program_hash)?;
        self.load_committed_invocation_result(invocation, true)
            .await
    }

    async fn load_committed_invocation_result(
        &self,
        invocation: CommittedInvocation,
        replayed: bool,
    ) -> Result<InvokedProgramResult, Status> {
        record_committed_span(invocation);
        let result = self
            .store
            .committed_program_result(program_commit(None, invocation.committed_batch))
            .await
            .map_err(program_store_status)?;
        require_result_matches_consensus(&result, invocation)?;
        Ok(invoked_result(result, invocation, replayed))
    }

    fn emit_bounded_state_metrics(&self) {
        let Ok(state) = self.decisions.state() else {
            tracing::warn!("atomic program bounded-state metrics unavailable");
            return;
        };
        let Ok(unfinalized_bytes) = state.unfinalized_commit_bytes() else {
            tracing::warn!("atomic program unfinalized-byte metric unavailable");
            return;
        };
        tracing::info!(
            gauge.keldra_atomic_program_replay_entries =
                u64::from(state.committed_invocation_len()),
            gauge.keldra_atomic_program_replay_bytes = state.committed_invocation_bytes(),
            gauge.keldra_atomic_program_unfinalized_entries =
                u64::from(state.unfinalized_commit_len()),
            gauge.keldra_atomic_program_unfinalized_bytes = unfinalized_bytes,
            "atomic program bounded state"
        );
    }
}

fn validate_program_request(
    key: &ObjectKey,
    invocation_id: &str,
    input_json: &[u8],
    durability_class: &str,
    topology: ProgramRuntimeTopology,
) -> Result<(), Status> {
    if !key
        .path()
        .strip_prefix(PROGRAM_PATH_PREFIX)
        .is_some_and(|name| !name.is_empty())
    {
        return Err(Status::invalid_argument(
            "program definition path must be below _keldra/programs/",
        ));
    }
    if invocation_id.is_empty()
        || invocation_id.len() > 256
        || invocation_id
            .chars()
            .any(|character| matches!(character, '/' | '\0' | '{' | '}'))
    {
        return Err(Status::invalid_argument(
            "invocation_id must be one safe path segment of at most 256 bytes",
        ));
    }
    if input_json.len() > MAX_PROGRAM_INPUT_BYTES {
        return Err(Status::resource_exhausted(format!(
            "program input exceeds {MAX_PROGRAM_INPUT_BYTES} bytes"
        )));
    }
    require_program_durability_class(durability_class, topology)
}

fn require_program_durability_class(
    value: &str,
    topology: ProgramRuntimeTopology,
) -> Result<(), Status> {
    match value {
        LOCAL_DURABILITY_CLASS => Ok(()),
        REPLICATED_DURABILITY_CLASS if topology == ProgramRuntimeTopology::Clustered => Ok(()),
        REPLICATED_DURABILITY_CLASS => Err(Status::unavailable(
            "DURABILITY_UNAVAILABLE: replicated durability is unavailable in a one-node cluster",
        )),
        _ => Err(Status::invalid_argument(
            "durability_class must be exactly `local` or `replicated`",
        )),
    }
}

fn accepted_program_evidence_hash(
    scope: &ProgramDurabilityScope,
    evidence_hash: ProgramDurabilityEvidenceHash,
    requested_class: &str,
    executor: NodeId,
) -> Result<ProgramDurabilityEvidenceHash, Status> {
    match scope {
        ProgramDurabilityScope::ExecutorLocal {
            node_id,
            synced: true,
        } if requested_class == LOCAL_DURABILITY_CLASS && u64::from(*node_id) == executor.0 => {
            Ok(evidence_hash)
        }
        ProgramDurabilityScope::ExecutorLocal { synced: false, .. } => Err(Status::unavailable(
            "DURABILITY_UNAVAILABLE: local atomic preparation was not synchronously durable",
        )),
        ProgramDurabilityScope::ExecutorLocal { .. } => Err(Status::failed_precondition(
            "executor-local durability evidence does not belong to the nominated executor",
        )),
        ProgramDurabilityScope::ConfiguredRemote { class } if class == requested_class => {
            Ok(evidence_hash)
        }
        ProgramDurabilityScope::ConfiguredRemote { .. } => Err(Status::failed_precondition(
            "prepared durability evidence does not match the requested class",
        )),
    }
}

fn program_path_hash(key: &ObjectKey) -> [u8; 32] {
    tagged_hash(
        b"keldra.program-path.v1",
        &[
            key.tenant().as_bytes(),
            key.bucket().as_bytes(),
            key.path().as_bytes(),
        ],
    )
}

fn invocation_identity(key: &ObjectKey, invocation_id: &str) -> InvocationId {
    InvocationId(tagged_hash(
        b"keldra.program-invocation.v1",
        &[
            key.tenant().as_bytes(),
            key.bucket().as_bytes(),
            key.path().as_bytes(),
            invocation_id.as_bytes(),
        ],
    ))
}

fn program_commit(previous_commit_cursor: Option<u64>, committed: CommittedBatch) -> ProgramCommit {
    ProgramCommit {
        previous_commit_cursor,
        commit_cursor: committed.commit_cursor,
        begin_cursor: committed.begin_cursor,
        bundle_ref: PreparedBundleRef {
            hash: committed.bundle_ref.hash,
            length: committed.bundle_ref.length,
        },
        bundle_hash: PreparedBundleHash(committed.bundle_hash.0),
        program_hash: ProgramHash(match committed.authority {
            AtomicBundleAuthority::StoredProgram { program_hash, .. }
            | AtomicBundleAuthority::LegacyProgramOnly { program_hash, .. } => program_hash.0,
            AtomicBundleAuthority::BuiltInObjectTransaction { .. } => [0; 32],
        }),
        authority: store_bundle_authority(committed.authority),
        participant_manifest_hash: committed.participant_manifest_hash.0,
        durability_class: ProgramDurabilityClassHash(committed.durability_class.0),
        durability_evidence_hash: ProgramDurabilityEvidenceHash(
            committed.durability_evidence_hash.0,
        ),
    }
}

pub(crate) fn store_bundle_authority(authority: AtomicBundleAuthority) -> ProgramBundleAuthority {
    match authority {
        AtomicBundleAuthority::StoredProgram {
            program_path_hash,
            program_hash,
        } => ProgramBundleAuthority::StoredProgram {
            program_path_hash: program_path_hash.0,
            program_hash: program_hash.0,
        },
        AtomicBundleAuthority::BuiltInObjectTransaction {
            kind,
            contract_version,
        } => ProgramBundleAuthority::BuiltInObjectTransaction {
            kind,
            contract_version,
        },
        AtomicBundleAuthority::LegacyProgramOnly {
            program_path_hash,
            program_hash,
        } => ProgramBundleAuthority::LegacyProgramOnly {
            program_path_hash: program_path_hash.0,
            program_hash: program_hash.0,
        },
    }
}

pub(crate) fn decision_bundle_authority(
    authority: ProgramBundleAuthority,
) -> AtomicBundleAuthority {
    match authority {
        ProgramBundleAuthority::StoredProgram {
            program_path_hash,
            program_hash,
        } => AtomicBundleAuthority::StoredProgram {
            program_path_hash: ProgramPathHash(program_path_hash),
            program_hash: DecisionProgramHash(program_hash),
        },
        ProgramBundleAuthority::BuiltInObjectTransaction {
            kind,
            contract_version,
        } => AtomicBundleAuthority::BuiltInObjectTransaction {
            kind,
            contract_version,
        },
        ProgramBundleAuthority::LegacyProgramOnly { .. } => {
            unreachable!("new atomic preparation cannot use legacy authority")
        }
    }
}

fn compare_commit_cursors(applied: Option<u64>, consensus: Option<u64>) -> Ordering {
    applied.cmp(&consensus)
}

fn tagged_hash(tag: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(tag);
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

fn decode_fingerprint(encoded: &str) -> Result<[u8; 32], Status> {
    let mut fingerprint = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut fingerprint)
        .map_err(|_| Status::internal("program engine produced an invalid fingerprint"))?;
    Ok(fingerprint)
}

fn authorize_canonical_bindings<F>(
    expanded: &[ExpandedProgramPath],
    canonical_paths: &BTreeMap<ObjectPath, ObjectPath>,
    authorize: &F,
) -> Result<(), Status>
where
    F: Fn(&ExpandedProgramPath) -> Result<(), Status>,
{
    for logical in expanded {
        let canonical = canonical_paths
            .get(&logical.path)
            .ok_or_else(|| Status::data_loss("stored-program canonical path binding is absent"))?;
        authorize(&ExpandedProgramPath {
            path: canonical.clone(),
            intent: logical.intent,
        })?;
    }
    Ok(())
}

fn authorize_sealed_canonical_paths<F>(
    expanded: &[ExpandedProgramPath],
    sealed: &BTreeMap<ObjectPath, ObjectPath>,
    authorize: &F,
) -> Result<(), Status>
where
    F: Fn(&ExpandedProgramPath) -> Result<(), Status>,
{
    authorize_canonical_bindings(expanded, sealed, authorize)
}

fn require_same_invocation(
    invocation: CommittedInvocation,
    fingerprint: [u8; 32],
    program_path_hash: [u8; 32],
    program_hash: [u8; 32],
) -> Result<(), Status> {
    if invocation.input_fingerprint != InvocationFingerprint(fingerprint) {
        return Err(Status::already_exists(
            "IDEMPOTENCY_INPUT_MISMATCH: invocation id was reused with different input",
        ));
    }
    let same_program = matches!(
        invocation.committed_batch.authority,
        AtomicBundleAuthority::StoredProgram {
            program_path_hash: committed_path,
            program_hash: committed_program,
        } | AtomicBundleAuthority::LegacyProgramOnly {
            program_path_hash: committed_path,
            program_hash: committed_program,
        } if committed_path == ProgramPathHash(program_path_hash)
            && committed_program == DecisionProgramHash(program_hash)
    );
    if !same_program {
        return Err(Status::already_exists(
            "IDEMPOTENCY_INPUT_MISMATCH: invocation id was reused for a different program",
        ));
    }
    Ok(())
}

fn require_result_matches_consensus(
    result: &CommittedProgramResult,
    invocation: CommittedInvocation,
) -> Result<(), Status> {
    let receipt_fingerprint = decode_fingerprint(&result.receipt.input_fingerprint)?;
    let committed_path = match invocation.committed_batch.authority {
        AtomicBundleAuthority::StoredProgram {
            program_path_hash, ..
        }
        | AtomicBundleAuthority::LegacyProgramOnly {
            program_path_hash, ..
        } => program_path_hash.0,
        AtomicBundleAuthority::BuiltInObjectTransaction { .. } => [0; 32],
    };
    if result.receipt.program_path_hash != committed_path
        || receipt_fingerprint != invocation.input_fingerprint.0
        || result
            .published_versions
            .values()
            .any(|published| published.version.0 == 0)
    {
        return Err(Status::data_loss(
            "prepared atomic result does not match its committed Raft invocation",
        ));
    }
    Ok(())
}

fn invoked_result(
    result: CommittedProgramResult,
    invocation: CommittedInvocation,
    replayed: bool,
) -> InvokedProgramResult {
    InvokedProgramResult {
        receipt: result.receipt,
        executor_nomination_log_index: invocation.committed_batch.nomination_log_index,
        commit_log_index: invocation.committed_batch.commit_cursor,
        program_hash: match invocation.committed_batch.authority {
            AtomicBundleAuthority::StoredProgram { program_hash, .. }
            | AtomicBundleAuthority::LegacyProgramOnly { program_hash, .. } => program_hash.0,
            AtomicBundleAuthority::BuiltInObjectTransaction { .. } => [0; 32],
        },
        published_versions: result.published_versions,
        asserted_versions: result.asserted_versions,
        alias_targets: result.alias_targets,
        replayed,
        replay_guarantee_expires_at_unix_millis: invocation.replay_expires_at_unix_millis,
    }
}

fn record_committed_span(invocation: CommittedInvocation) {
    tracing::Span::current().record(
        "nomination.log_index",
        invocation.committed_batch.nomination_log_index,
    );
    tracing::Span::current().record("commit.log_index", invocation.committed_batch.commit_cursor);
}

fn current_unix_millis() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("Unix time exceeds u64 milliseconds")
}

fn expect_nomination(result: ApplyResult, node: NodeId) -> Result<ExecutorNomination> {
    match result {
        ApplyResult::ExecutorNominated(nomination) if nomination.executor == node => Ok(nomination),
        other => bail!("unexpected executor nomination response: {other:?}"),
    }
}

fn expect_batch_committed(result: ApplyResult) -> Result<keldra_consensus::CommitResult, Status> {
    match result {
        ApplyResult::BatchCommitted(result) => Ok(result),
        other => Err(Status::internal(format!(
            "unexpected atomic commit response: {other:?}"
        ))),
    }
}

fn expect_batch_begun(result: ApplyResult) -> Result<keldra_consensus::BeginResult, Status> {
    match result {
        ApplyResult::BatchBegun(result) => Ok(result),
        other => Err(Status::internal(format!(
            "unexpected atomic begin response: {other:?}"
        ))),
    }
}

fn expect_finalization(result: ApplyResult, expected_cursor: u64) -> Result<(), Status> {
    match result {
        ApplyResult::FinalizationAdvanced {
            through_commit_cursor,
            ..
        } if through_commit_cursor == expected_cursor => Ok(()),
        other => Err(Status::internal(format!(
            "unexpected atomic finalization response: {other:?}"
        ))),
    }
}

fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::Assertion { .. } => {
            Status::failed_precondition(format!("ASSERTION_FAILED: {error}"))
        }
        EngineError::HeadPrecondition { .. } | EngineError::ProgramConcurrency { .. } => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        EngineError::LimitExceeded(_) => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        EngineError::InvalidDefinition(_)
        | EngineError::InvalidInvocation(_)
        | EngineError::Operation { .. }
        | EngineError::Return { .. } => Status::failed_precondition(error.to_string()),
        EngineError::Read(_) | EngineError::InvalidSnapshot(_) => {
            Status::internal(error.to_string())
        }
    }
}

pub(super) fn mutation_status(error: keldra_store::MutationError) -> Status {
    match error {
        keldra_store::MutationError::ProgramConcurrencyViolation => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        keldra_store::MutationError::PreconditionFailed { .. }
        | keldra_store::MutationError::AtomicReservationConflict { .. }
        | keldra_store::MutationError::Immutable
        | keldra_store::MutationError::ImmutablePolicyRequired
        | keldra_store::MutationError::ObjectHasInboundAliases
        | keldra_store::MutationError::ObjectVersioningNotEnabled => {
            Status::failed_precondition(error.to_string())
        }
        keldra_store::MutationError::CurrentTombstoneCannotBeDeleted => {
            Status::failed_precondition(format!(
                "CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED: {error}"
            ))
        }
        keldra_store::MutationError::IdempotencyConflict => {
            Status::already_exists(error.to_string())
        }
        keldra_store::MutationError::InvalidCommandId
        | keldra_store::MutationError::InvalidPolicy(_)
        | keldra_store::MutationError::InvalidObjectMutation(_) => {
            Status::invalid_argument(error.to_string())
        }
        keldra_store::MutationError::BlobNotFound => Status::not_found(error.to_string()),
        keldra_store::MutationError::DurabilityUnavailable => {
            Status::unavailable(format!("DURABILITY_UNAVAILABLE: {error}"))
        }
        keldra_store::MutationError::ReceiptCapacity
        | keldra_store::MutationError::SourceJournalCapacity => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        keldra_store::MutationError::ReceiptTooLarge { .. }
        | keldra_store::MutationError::SourceJournalRecordTooLarge { .. }
        | keldra_store::MutationError::SourceJournalTransitionTooLarge { .. } => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        keldra_store::MutationError::ObjectMutationLineageGap { .. }
        | keldra_store::MutationError::ObjectMutationSibling { .. }
        | keldra_store::MutationError::ObjectMutationConflict => {
            Status::unavailable(format!("MUTATION_REPLICA_UNAVAILABLE: {error}"))
        }
        keldra_store::MutationError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn program_store_status(error: ProgramStoreError) -> Status {
    match error {
        ProgramStoreError::InvalidDefinition(_) | ProgramStoreError::InvalidBundle(_) => {
            Status::invalid_argument(error.to_string())
        }
        ProgramStoreError::PreparedBundleNotFound(_) => Status::not_found(error.to_string()),
        ProgramStoreError::ProgramHashMismatch => {
            Status::failed_precondition(format!("PROGRAM_VERSION_MISMATCH: {error}"))
        }
        ProgramStoreError::CommitCorruption { .. } => Status::already_exists(error.to_string()),
        ProgramStoreError::ExecutorLocalDurability => {
            Status::unavailable(format!("DURABILITY_UNAVAILABLE: {error}"))
        }
        ProgramStoreError::SourceJournalCapacity => {
            Status::unavailable(format!("RESOURCE_LIMIT: {error}"))
        }
        ProgramStoreError::SourceJournalTransitionTooLarge { .. } => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        ProgramStoreError::PreconditionFailed { .. } => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        ProgramStoreError::Immutable { .. }
        | ProgramStoreError::OutOfOrderCommit { .. }
        | ProgramStoreError::DurabilityClassMismatch
        | ProgramStoreError::DurabilityEvidenceMismatch
        | ProgramStoreError::PreparedBundleMismatch => {
            Status::failed_precondition(error.to_string())
        }
        ProgramStoreError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn decision_status(error: DecisionRaftError) -> Status {
    match error {
        DecisionRaftError::ForwardToLeader { .. } | DecisionRaftError::Unavailable(_) => {
            Status::unavailable(error.to_string())
        }
        DecisionRaftError::LeaderTimeout | DecisionRaftError::SnapshotTimeout => {
            Status::deadline_exceeded(error.to_string())
        }
        DecisionRaftError::Rejected(
            ApplyError::ExecutorNotCurrentMember { .. }
            | ApplyError::ExecutorNotNominated
            | ApplyError::NotCurrentExecutor { .. }
            | ApplyError::NominationFenceMismatch { .. },
        ) => Status::unavailable(format!("EXECUTOR_MOVED: {error}")),
        DecisionRaftError::Rejected(ApplyError::IdempotencyConflict { .. }) => {
            Status::already_exists(format!("IDEMPOTENCY_INPUT_MISMATCH: {error}"))
        }
        DecisionRaftError::Rejected(ApplyError::CommitTailFull { .. }) => {
            Status::resource_exhausted(format!("FINALIZATION_LAG: {error}"))
        }
        DecisionRaftError::Rejected(ApplyError::CommittedInvocationWindowFull { .. }) => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        DecisionRaftError::Rejected(_) => Status::failed_precondition(error.to_string()),
        DecisionRaftError::InvalidNodeId | DecisionRaftError::Configuration(_) => {
            Status::invalid_argument(error.to_string())
        }
        DecisionRaftError::Storage(_) | DecisionRaftError::StatePoisoned => {
            Status::internal(error.to_string())
        }
    }
}

fn finalization_decision_status(error: DecisionRaftError) -> Status {
    let status = decision_status(error);
    match status.code() {
        tonic::Code::Unavailable if status.message().starts_with("EXECUTOR_MOVED:") => status,
        _ => Status::unavailable(format!("FINALIZATION_LAG: {}", status.message())),
    }
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests;
