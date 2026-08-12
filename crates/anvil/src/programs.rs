use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::path::Path;

use anvil_atomic_program::{
    CommandReceipt, EngineError, ExpandedProgramPath, InvocationContext, ObjectPath, ProgramInput,
    ProgramInvocation,
};
use anvil_consensus::{
    ATOMIC_REPLAY_RETENTION_MILLIS, ApplyError, ApplyResult, BundleHash, BundleRef, Command,
    CommitBatch, CommittedBatch, CommittedInvocation, DecisionRaft, DecisionRaftError,
    DurabilityClass, DurabilityEvidenceHash, ExecutorNomination, InvocationFingerprint,
    InvocationId, NodeId, ProgramHash as DecisionProgramHash, ProgramPathHash,
};
use anvil_store::{
    CommittedProgramResult, ObjectKey, PreparedBundleHash, PreparedBundleRef, ProgramCommit,
    ProgramDurabilityClassHash, ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramHash,
    ProgramStoreError, PublishedProgramVersion, Store, VerifiedProgramDefinition,
};
use anyhow::{Context, Result, bail};
use tonic::Status;
use tracing::Instrument as _;

mod distributed;

use distributed::DistributedPrograms;

pub(crate) const MAX_PROGRAM_INPUT_BYTES: usize = 16 * 1024 * 1024;
const PROGRAM_PATH_PREFIX: &str = "_anvil/programs/";
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

#[derive(Debug)]
pub(crate) struct InvokedProgramResult {
    pub receipt: CommandReceipt,
    pub executor_nomination_log_index: u64,
    pub commit_log_index: u64,
    pub program_hash: [u8; 32],
    pub published_versions: BTreeMap<ObjectPath, PublishedProgramVersion>,
    pub replayed: bool,
    pub replay_guarantee_expires_at_unix_millis: u64,
}

/// True only when this node's applied Raft state has no committed atomic batch
/// awaiting finalization. Callers perform their data read first and discard it
/// when this returns false.
pub(crate) fn atomic_tail_is_clear(decisions: &DecisionRaft) -> Result<bool, Status> {
    let state = decisions.state().map_err(decision_status)?;
    Ok(state.unfinalized_commit_len() == 0)
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
        if local.is_none_or(|descriptor| descriptor.state != anvil_consensus::NodeState::Active) {
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
        if !atomic_tail_is_clear(&self.decisions)? {
            return Err(Status::unavailable(
                "atomic program tail is not finalized before membership cutover",
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
                monotonic_counter.anvil_atomic_program_executor_nominations_total = 1_u64,
                "atomic program executor nominated"
            );
        }

        let coordinator = Self {
            store,
            decisions,
            node,
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
            distributed: Arc::new(std::sync::OnceLock::new()),
        };
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
    pub async fn invoke<F>(
        &self,
        program_key: ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        authorize: F,
    ) -> Result<InvokedProgramResult, Status>
    where
        F: Fn(&ExpandedProgramPath) -> Result<(), Status>,
    {
        let invocation_hash = invocation_identity(&program_key, &invocation_id);
        let invocation_hash = hex::encode(invocation_hash.0);
        let program_hash = hex::encode(expected_program_hash);
        let invocation_started = Instant::now();
        let span = tracing::info_span!(
            "anvil.atomic_program.invoke",
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
                authorize,
            )
            .instrument(span.clone())
            .await;
        span.in_scope(|| {
            tracing::info!(
                histogram.anvil_atomic_program_total_duration_seconds =
                    invocation_started.elapsed().as_secs_f64(),
                "atomic program invocation completed"
            );
        });
        result
    }

    async fn invoke_in_span<F>(
        &self,
        program_key: ObjectKey,
        expected_program_hash: [u8; 32],
        invocation_id: String,
        input_json: &[u8],
        durability_class: &str,
        authorize: F,
    ) -> Result<InvokedProgramResult, Status>
    where
        F: Fn(&ExpandedProgramPath) -> Result<(), Status>,
    {
        tracing::info!(
            monotonic_counter.anvil_atomic_program_invocations_total = 1_u64,
            "atomic program invocation"
        );
        validate_program_request(
            &program_key,
            &invocation_id,
            input_json,
            durability_class,
            ProgramRuntimeTopology::OneNode,
        )?;
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
            for path in engine
                .expanded_paths(&context, &invocation)
                .map_err(engine_status)?
            {
                authorize(&path)?;
            }
            let replay_clock = current_unix_millis().map_err(internal)?;
            if let Some(invocation) = self
                .decisions
                .state()
                .map_err(decision_status)?
                .replay_entry(consensus_invocation_id, replay_clock)
            {
                return self
                    .replay_committed_invocation(
                        invocation,
                        fingerprint,
                        program_path_hash,
                        expected_program_hash,
                    )
                    .await;
            }

            let prepare_started = Instant::now();
            let lease = engine
                .prepare(&context, &invocation)
                .await
                .map_err(engine_status)?;

            let prepared = self
                .store
                .prepare_program_bundle(&lease)
                .await
                .map_err(program_store_status)?;
            tracing::info!(
                histogram.anvil_atomic_program_prepare_duration_seconds =
                    prepare_started.elapsed().as_secs_f64(),
                histogram.anvil_atomic_program_prepared_bytes = prepared.bundle.length,
                "atomic program bundle prepared"
            );
            let evidence_hash = accepted_program_evidence_hash(
                &prepared.durability.scope,
                prepared.durability_evidence_hash,
                durability_class,
                self.node,
            )?;

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
            let proposal_at_unix_millis = current_unix_millis().map_err(internal)?;
            let replay_expires_at_unix_millis = proposal_at_unix_millis
                .checked_add(ATOMIC_REPLAY_RETENTION_MILLIS)
                .ok_or_else(|| Status::internal("atomic replay expiry overflow"))?;
            let commit_started = Instant::now();
            let committed = self
                .decisions
                .submit(Command::CommitBatch(CommitBatch {
                    executor: self.node,
                    nomination_log_index: nomination.nomination_log_index,
                    program_path_hash: ProgramPathHash(program_path_hash),
                    program_hash: DecisionProgramHash(expected_program_hash),
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
                    proposal_at_unix_millis,
                    replay_expires_at_unix_millis,
                }))
                .await;
            tracing::info!(
                histogram.anvil_atomic_program_commit_duration_seconds =
                    commit_started.elapsed().as_secs_f64(),
                "atomic program decision completed"
            );
            self.emit_bounded_state_metrics();
            let committed = committed.map_err(decision_status)?;
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

            let result = self
                .store
                .apply_program_bundle(
                    lease,
                    &prepared,
                    program_commit(applied_cursor, committed.invocation.committed_batch),
                )
                .await
                .map_err(program_store_status)?;
            require_result_matches_consensus(&result, committed.invocation)?;
            self.advance_finalized_through(
                nomination,
                committed.invocation.committed_batch.commit_cursor,
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

    fn current_nomination(&self) -> Result<ExecutorNomination, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(decision_status)?
            .executor()
            .ok_or_else(|| {
                Status::unavailable("EXECUTOR_MOVED: no atomic executor is nominated")
            })?;
        if nomination.executor != self.node {
            return Err(Status::unavailable(format!(
                "EXECUTOR_MOVED: atomic request belongs on nominated executor node {}",
                nomination.executor.0
            )));
        }
        Ok(nomination)
    }

    async fn recover_committed_tail(&self) -> Result<()> {
        let _guard = self.commit_gate.lock().await;
        self.recover_committed_tail_locked().await
    }

    async fn recover_committed_tail_locked(&self) -> Result<()> {
        let state = self.decisions.state().context("read decision state")?;
        let mut applied = self
            .store
            .applied_program_commit_cursor()
            .context("read applied atomic commit cursor")?;
        if let Some(finalized) = state.finalized_through()
            && applied.is_none_or(|cursor| cursor < finalized)
        {
            bail!(
                "local atomic view is behind finalized cursor {finalized}; compacted recovery data is unavailable"
            );
        }
        if let (Some(applied), Some(last)) = (applied, state.last_commit_cursor())
            && applied > last
        {
            bail!("local atomic cursor {applied} is ahead of consensus cursor {last}");
        }

        let mut finalized_through = None;
        for invocation in state.unfinalized_invocations() {
            tracing::info!(
                monotonic_counter.anvil_atomic_program_finalization_retries_total = 1_u64,
                "retry atomic program finalization"
            );
            let batch = invocation.committed_batch;
            if applied.is_some_and(|cursor| cursor >= batch.commit_cursor) {
                // The local atomic batch and compact applied cursor were
                // already installed before the previous process stopped.
                // FinalizedThrough is the only remaining work; old replay
                // entries are never reapplied.
                finalized_through = Some(batch.commit_cursor);
                continue;
            }
            let result = self
                .store
                .recover_program_bundle(program_commit(applied, batch))
                .await
                .with_context(|| format!("finalize atomic commit {}", batch.commit_cursor))?;
            require_result_matches_consensus(&result, invocation)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            applied = Some(batch.commit_cursor);
            finalized_through = Some(batch.commit_cursor);
        }
        if let Some(through_commit_cursor) = finalized_through {
            let nomination = state
                .executor()
                .context("cannot finalize atomic recovery tail without an executor")?;
            self.advance_finalized_through(nomination, through_commit_cursor)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        Ok(())
    }

    async fn advance_finalized_through(
        &self,
        nomination: ExecutorNomination,
        through_commit_cursor: u64,
    ) -> Result<(), Status> {
        let result = self
            .decisions
            .submit(Command::FinalizedThrough {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                through_commit_cursor,
            })
            .await;
        self.emit_bounded_state_metrics();
        let result = result.map_err(finalization_decision_status)?;
        expect_finalization(result.result, through_commit_cursor)
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
            gauge.anvil_atomic_program_replay_entries = u64::from(state.committed_invocation_len()),
            gauge.anvil_atomic_program_replay_bytes = state.committed_invocation_bytes(),
            gauge.anvil_atomic_program_unfinalized_entries =
                u64::from(state.unfinalized_commit_len()),
            gauge.anvil_atomic_program_unfinalized_bytes = unfinalized_bytes,
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
            "program definition path must be below _anvil/programs/",
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
        b"anvil.program-path.v1",
        &[
            key.tenant().as_bytes(),
            key.bucket().as_bytes(),
            key.path().as_bytes(),
        ],
    )
}

fn invocation_identity(key: &ObjectKey, invocation_id: &str) -> InvocationId {
    InvocationId(tagged_hash(
        b"anvil.program-invocation.v1",
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
        bundle_ref: PreparedBundleRef {
            hash: committed.bundle_ref.hash,
            length: committed.bundle_ref.length,
        },
        bundle_hash: PreparedBundleHash(committed.bundle_hash.0),
        program_hash: ProgramHash(committed.program_hash.0),
        durability_class: ProgramDurabilityClassHash(committed.durability_class.0),
        durability_evidence_hash: ProgramDurabilityEvidenceHash(
            committed.durability_evidence_hash.0,
        ),
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
    if invocation.committed_batch.program_path_hash != ProgramPathHash(program_path_hash)
        || invocation.committed_batch.program_hash != DecisionProgramHash(program_hash)
    {
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
    if result.receipt.program_path_hash != invocation.committed_batch.program_path_hash.0
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
        program_hash: invocation.committed_batch.program_hash.0,
        published_versions: result.published_versions,
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

fn expect_batch_committed(result: ApplyResult) -> Result<anvil_consensus::CommitResult, Status> {
    match result {
        ApplyResult::BatchCommitted(result) => Ok(result),
        other => Err(Status::internal(format!(
            "unexpected atomic commit response: {other:?}"
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

fn mutation_status(error: anvil_store::MutationError) -> Status {
    match error {
        anvil_store::MutationError::ProgramConcurrencyViolation => {
            Status::failed_precondition(format!("PROGRAM_CONCURRENCY_VIOLATION: {error}"))
        }
        anvil_store::MutationError::PreconditionFailed { .. }
        | anvil_store::MutationError::Immutable
        | anvil_store::MutationError::ImmutablePolicyRequired
        | anvil_store::MutationError::ObjectVersioningNotEnabled => {
            Status::failed_precondition(error.to_string())
        }
        anvil_store::MutationError::CurrentTombstoneCannotBeDeleted => Status::failed_precondition(
            format!("CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED: {error}"),
        ),
        anvil_store::MutationError::IdempotencyConflict => {
            Status::already_exists(error.to_string())
        }
        anvil_store::MutationError::InvalidCommandId
        | anvil_store::MutationError::InvalidPolicy(_)
        | anvil_store::MutationError::InvalidObjectMutation(_) => {
            Status::invalid_argument(error.to_string())
        }
        anvil_store::MutationError::BlobNotFound => Status::not_found(error.to_string()),
        anvil_store::MutationError::DurabilityUnavailable => {
            Status::unavailable(format!("DURABILITY_UNAVAILABLE: {error}"))
        }
        anvil_store::MutationError::ReceiptCapacity
        | anvil_store::MutationError::SourceJournalCapacity => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        anvil_store::MutationError::ReceiptTooLarge { .. }
        | anvil_store::MutationError::SourceJournalRecordTooLarge { .. } => {
            Status::resource_exhausted(format!("RESOURCE_LIMIT: {error}"))
        }
        anvil_store::MutationError::ObjectMutationLineageGap { .. }
        | anvil_store::MutationError::ObjectMutationSibling { .. }
        | anvil_store::MutationError::ObjectMutationConflict => {
            Status::unavailable(format!("MUTATION_REPLICA_UNAVAILABLE: {error}"))
        }
        anvil_store::MutationError::Storage(_) => Status::internal(error.to_string()),
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
        ProgramStoreError::ProgramPolicy { .. } | ProgramStoreError::PreconditionFailed { .. } => {
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
mod tests {
    use std::sync::Mutex;

    use anvil_atomic_program::{
        Cardinality, DEFINITION_SCHEMA_VERSION, DocumentAccess, DocumentRef, DocumentSpec,
        DocumentValueRef, DocumentView, ExpectedHead, InputValue, IntegerType, JsonPointerRef,
        Operation, PathBinding, PathTemplate, ProgramCaps, ProgramDefinition, ReturnDefinition,
    };
    use anvil_authz::ObjectRef;
    use anvil_store::{
        AuthzRevision, BucketPolicy, CreateBucketRequest, Durability, ObjectVersioning,
        ProvisionTenantRequest, PutMode, PutRequest, StorageTenantId, StoreOptions,
        SystemBootstrapRequest,
    };
    use serde_json::json;

    use super::*;

    fn counter_definition() -> ProgramDefinition {
        let counter = DocumentRef::one("counter");
        ProgramDefinition {
            schema_version: DEFINITION_SCHEMA_VERSION,
            documents: vec![DocumentSpec {
                name: "counter".into(),
                path: PathTemplate::new("{tenant}", "bucket", "managed/counter"),
                cardinality: Cardinality::One,
                access: DocumentAccess::ReadWrite,
                allow_initial_json: true,
            }],
            assertions: Vec::new(),
            operations: vec![Operation::CheckedIntegerAdd {
                target: JsonPointerRef::new(counter.clone(), "/value"),
                delta: InputValue::Input {
                    name: "delta".into(),
                },
                numeric_type: IntegerType::U64 {
                    min: Some(0),
                    max: None,
                },
            }],
            returns: vec![ReturnDefinition {
                name: "value".into(),
                value: DocumentValueRef {
                    value: JsonPointerRef::new(counter, "/value"),
                    view: DocumentView::Current,
                },
            }],
            caps: ProgramCaps {
                max_paths: 1,
                max_writes: 1,
                max_operations: 2,
                max_input_bytes: 64 * 1024,
                max_document_bytes: 64 * 1024,
            },
        }
    }

    fn counter_input() -> ProgramInput {
        ProgramInput {
            inputs: [("delta".into(), json!(1))].into_iter().collect(),
            bindings: [(
                "counter".into(),
                vec![PathBinding {
                    path: ObjectPath::new("tenant", "bucket", "managed/counter").unwrap(),
                    template_values: BTreeMap::new(),
                    expected_head: ExpectedHead::Absent,
                    initial_json: Some(json!({"value": 0})),
                }],
            )]
            .into_iter()
            .collect(),
            ..ProgramInput::default()
        }
    }

    async fn configured_program_store(root: &Path) -> (Store, ObjectKey, [u8; 32], Vec<u8>) {
        let store = Store::open(StoreOptions::new(root, 1)).await.unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: "bootstrap-secret-0123456789abcdef0123456789abcdef".into(),
            })
            .unwrap();
        let tenant = StorageTenantId::parse("tenant").unwrap();
        let owner = ObjectRef::opaque("app", "owner-app").unwrap();
        store
            .provision_tenant(ProvisionTenantRequest {
                storage_tenant: tenant.clone(),
                owner_app_id: "owner-app".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: "owner-secret-0123456789abcdef0123456789abcdef".into(),
                principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
                expected_authorization_revision: AuthzRevision(3),
                expected_binding_generation: 1,
            })
            .unwrap();
        store
            .create_bucket(CreateBucketRequest {
                storage_tenant: tenant,
                bucket: "bucket".into(),
                owner: owner.clone(),
                principal: owner,
                expected_authorization_revision: AuthzRevision(4),
                expected_binding_generation: 1,
                versioning: ObjectVersioning::Unversioned,
            })
            .unwrap();
        store
            .set_bucket_policy(
                "tenant",
                "bucket",
                BucketPolicy {
                    immutable_prefixes: Vec::new(),
                    program_only_prefixes: vec!["managed".into()],
                },
            )
            .await
            .unwrap();
        let program_key = ObjectKey::new("tenant", "bucket", "_anvil/programs/counter@1").unwrap();
        let definition = serde_json::to_vec(&counter_definition()).unwrap();
        let program_hash = ProgramHash::for_definition_bytes(&definition).0;
        store
            .put(PutRequest {
                key: program_key.clone(),
                bytes: definition,
                content_type: Some("application/json".into()),
                mode: PutMode::PutImmutable,
                command_id: Some("install-counter-program".into()),
                durability: Durability::Local,
            })
            .await
            .unwrap();
        (
            store,
            program_key,
            program_hash,
            serde_json::to_vec(&counter_input()).unwrap(),
        )
    }

    async fn open_test_coordinator(store: Store, root: &Path) -> ProgramCoordinator {
        open_test_coordinator_with_limits(store, root, 8, 64 * 1024).await
    }

    async fn open_test_coordinator_with_limits(
        store: Store,
        root: &Path,
        max_commit_entries: u32,
        max_commit_bytes: u64,
    ) -> ProgramCoordinator {
        let decisions = DecisionRaft::open(
            root.join("decisions"),
            1,
            max_commit_entries,
            max_commit_bytes,
        )
        .await
        .unwrap();
        decisions.ensure_one_node().await.unwrap();
        decisions
            .wait_for_leader(Duration::from_secs(10))
            .await
            .unwrap();
        ProgramCoordinator::start(store, decisions, NodeId(1))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn public_coordinator_retains_replay_then_compacts_the_recovery_tail() {
        let temporary = tempfile::tempdir().unwrap();
        let (store, program_key, program_hash, input) =
            configured_program_store(temporary.path()).await;
        let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
        let intents = Mutex::new(Vec::new());

        let first = coordinator
            .invoke(
                program_key.clone(),
                program_hash,
                "increment-1".into(),
                &input,
                LOCAL_DURABILITY_CLASS,
                |dependency| {
                    intents.lock().unwrap().push(dependency.intent);
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert!(!first.replayed);
        assert_eq!(first.receipt.outputs["value"], json!(1));
        assert_eq!(
            intents.into_inner().unwrap(),
            vec![anvil_atomic_program::ProgramPathIntent {
                get: true,
                put: true,
                delete: false,
            }]
        );
        let decision_state = coordinator.decisions.state().unwrap();
        assert_eq!(decision_state.unfinalized_commit_len(), 0);
        assert_eq!(
            decision_state.finalized_through(),
            Some(first.commit_log_index)
        );
        assert!(first.replay_guarantee_expires_at_unix_millis > current_unix_millis().unwrap());

        let replay = coordinator
            .invoke(
                program_key,
                program_hash,
                "increment-1".into(),
                &input,
                LOCAL_DURABILITY_CLASS,
                |_| Ok(()),
            )
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_log_index, first.commit_log_index);
        assert_eq!(
            replay.replay_guarantee_expires_at_unix_millis,
            first.replay_guarantee_expires_at_unix_millis
        );
        assert_eq!(replay.published_versions, first.published_versions);
        coordinator.decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn each_success_advances_finalized_through_before_the_next_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let (store, program_key, program_hash, first_input) =
            configured_program_store(temporary.path()).await;
        let coordinator =
            open_test_coordinator_with_limits(store, temporary.path(), 1, 64 * 1024).await;

        let mut input = serde_json::from_slice::<ProgramInput>(&first_input).unwrap();
        let mut previous_version: Option<u64> = None;
        for index in 0..4 {
            if let Some(version) = previous_version {
                let binding = &mut input.bindings.get_mut("counter").unwrap()[0];
                binding.expected_head = ExpectedHead::Version {
                    version: version.to_string(),
                };
                binding.initial_json = None;
            }
            let result = coordinator
                .invoke(
                    program_key.clone(),
                    program_hash,
                    format!("bounded-tail-{index}"),
                    &serde_json::to_vec(&input).unwrap(),
                    LOCAL_DURABILITY_CLASS,
                    |_| Ok(()),
                )
                .await
                .unwrap();
            previous_version = result
                .published_versions
                .values()
                .next()
                .map(|published| published.version.0);
            let state = coordinator.decisions.state().unwrap();
            assert_eq!(state.unfinalized_commit_len(), 0);
            assert_eq!(state.finalized_through(), Some(result.commit_log_index));
        }
        coordinator.decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_recovers_committed_bundle_before_advancing_finalized_through() {
        let temporary = tempfile::tempdir().unwrap();
        let (store, program_key, program_hash, input_json) =
            configured_program_store(temporary.path()).await;
        let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
        let input = serde_json::from_slice::<ProgramInput>(&input_json).unwrap();
        let path_hash = program_path_hash(&program_key);
        let invocation =
            ProgramInvocation::from_input(path_hash, "crash-before-finalize", input).unwrap();
        let fingerprint = decode_fingerprint(&invocation.input_fingerprint).unwrap();
        let invocation_id = invocation_identity(&program_key, &invocation.command_id);
        let program_object = store.get(&program_key).await.unwrap().unwrap();
        let verified =
            VerifiedProgramDefinition::from_bytes(&program_object.bytes, ProgramHash(program_hash))
                .unwrap();
        let engine = store.program_engine(&verified).unwrap();
        let lease = engine
            .prepare(&InvocationContext::new("tenant").unwrap(), &invocation)
            .await
            .unwrap();
        let prepared = store.prepare_program_bundle(&lease).await.unwrap();
        let nomination = coordinator.current_nomination().unwrap();
        let proposal_at_unix_millis = current_unix_millis().unwrap();
        let committed = coordinator
            .decisions
            .submit(Command::CommitBatch(CommitBatch {
                executor: NodeId(1),
                nomination_log_index: nomination.nomination_log_index,
                program_path_hash: ProgramPathHash(path_hash),
                program_hash: DecisionProgramHash(program_hash),
                invocation_id,
                input_fingerprint: InvocationFingerprint(fingerprint),
                bundle_ref: BundleRef {
                    hash: prepared.bundle.hash,
                    length: prepared.bundle.length,
                },
                bundle_hash: BundleHash(prepared.hash.0),
                durability_class: DurabilityClass(
                    ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS).0,
                ),
                durability_evidence_hash: DurabilityEvidenceHash(
                    prepared.durability_evidence_hash.0,
                ),
                proposal_at_unix_millis,
                replay_expires_at_unix_millis: proposal_at_unix_millis
                    + ATOMIC_REPLAY_RETENTION_MILLIS,
            }))
            .await
            .unwrap();
        let committed = expect_batch_committed(committed.result).unwrap();
        let commit_cursor = committed.invocation.committed_batch.commit_cursor;
        assert!(!coordinator.cursor_is_visible(commit_cursor).unwrap());
        drop(lease);
        drop(engine);
        coordinator.decisions.shutdown().await.unwrap();
        drop(coordinator);
        drop(store);

        let reopened_store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let reopened = open_test_coordinator(reopened_store.clone(), temporary.path()).await;
        let state = reopened.decisions.state().unwrap();
        assert_eq!(state.finalized_through(), Some(commit_cursor));
        assert_eq!(state.unfinalized_commit_len(), 0);
        assert!(reopened.cursor_is_visible(commit_cursor).unwrap());
        assert_eq!(
            reopened_store.applied_program_commit_cursor().unwrap(),
            Some(commit_cursor)
        );
        let replay = reopened
            .invoke(
                program_key,
                program_hash,
                "crash-before-finalize".into(),
                &input_json,
                LOCAL_DURABILITY_CLASS,
                |_| Ok(()),
            )
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_log_index, commit_cursor);
        assert_eq!(replay.receipt.outputs["value"], json!(1));
        reopened.decisions.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_finalizes_a_partially_recovered_multi_commit_tail() {
        let temporary = tempfile::tempdir().unwrap();
        let (store, program_key, program_hash, _) =
            configured_program_store(temporary.path()).await;
        let coordinator = open_test_coordinator(store.clone(), temporary.path()).await;
        let path_hash = program_path_hash(&program_key);
        let program_object = store.get(&program_key).await.unwrap().unwrap();
        let verified =
            VerifiedProgramDefinition::from_bytes(&program_object.bytes, ProgramHash(program_hash))
                .unwrap();
        let engine = store.program_engine(&verified).unwrap();
        let context = InvocationContext::new("tenant").unwrap();
        let nomination = coordinator.current_nomination().unwrap();

        let first_invocation =
            ProgramInvocation::from_input(path_hash, "partial-recovery-1", counter_input())
                .unwrap();
        let first_fingerprint = decode_fingerprint(&first_invocation.input_fingerprint).unwrap();
        let first_id = invocation_identity(&program_key, &first_invocation.command_id);
        let first_lease = engine.prepare(&context, &first_invocation).await.unwrap();
        let first_prepared = store.prepare_program_bundle(&first_lease).await.unwrap();
        let first_consensus = expect_batch_committed(
            coordinator
                .decisions
                .submit(Command::CommitBatch(CommitBatch {
                    executor: NodeId(1),
                    nomination_log_index: nomination.nomination_log_index,
                    program_path_hash: ProgramPathHash(path_hash),
                    program_hash: DecisionProgramHash(program_hash),
                    invocation_id: first_id,
                    input_fingerprint: InvocationFingerprint(first_fingerprint),
                    bundle_ref: BundleRef {
                        hash: first_prepared.bundle.hash,
                        length: first_prepared.bundle.length,
                    },
                    bundle_hash: BundleHash(first_prepared.hash.0),
                    durability_class: DurabilityClass(
                        ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS).0,
                    ),
                    durability_evidence_hash: DurabilityEvidenceHash(
                        first_prepared.durability_evidence_hash.0,
                    ),
                    proposal_at_unix_millis: 1_000,
                    replay_expires_at_unix_millis: 1_000 + ATOMIC_REPLAY_RETENTION_MILLIS,
                }))
                .await
                .unwrap()
                .result,
        )
        .unwrap();
        let first_applied = store
            .apply_program_bundle(
                first_lease,
                &first_prepared,
                program_commit(None, first_consensus.invocation.committed_batch),
            )
            .await
            .unwrap();
        require_result_matches_consensus(&first_applied, first_consensus.invocation).unwrap();
        let first_cursor = first_consensus.invocation.committed_batch.commit_cursor;
        let counter_path = ObjectPath::new("tenant", "bucket", "managed/counter").unwrap();
        let first_version = first_applied.published_versions[&counter_path];

        let mut second_input = counter_input();
        let second_binding = &mut second_input.bindings.get_mut("counter").unwrap()[0];
        second_binding.expected_head = ExpectedHead::Version {
            version: first_version.version.0.to_string(),
        };
        second_binding.initial_json = None;
        let second_invocation =
            ProgramInvocation::from_input(path_hash, "partial-recovery-2", second_input).unwrap();
        let second_fingerprint = decode_fingerprint(&second_invocation.input_fingerprint).unwrap();
        let second_id = invocation_identity(&program_key, &second_invocation.command_id);
        let second_lease = engine.prepare(&context, &second_invocation).await.unwrap();
        let second_prepared = store.prepare_program_bundle(&second_lease).await.unwrap();
        let second_consensus = expect_batch_committed(
            coordinator
                .decisions
                .submit(Command::CommitBatch(CommitBatch {
                    executor: NodeId(1),
                    nomination_log_index: nomination.nomination_log_index,
                    program_path_hash: ProgramPathHash(path_hash),
                    program_hash: DecisionProgramHash(program_hash),
                    invocation_id: second_id,
                    input_fingerprint: InvocationFingerprint(second_fingerprint),
                    bundle_ref: BundleRef {
                        hash: second_prepared.bundle.hash,
                        length: second_prepared.bundle.length,
                    },
                    bundle_hash: BundleHash(second_prepared.hash.0),
                    durability_class: DurabilityClass(
                        ProgramDurabilityClassHash::for_class(LOCAL_DURABILITY_CLASS).0,
                    ),
                    durability_evidence_hash: DurabilityEvidenceHash(
                        second_prepared.durability_evidence_hash.0,
                    ),
                    proposal_at_unix_millis: 2_000,
                    replay_expires_at_unix_millis: 2_000 + ATOMIC_REPLAY_RETENTION_MILLIS,
                }))
                .await
                .unwrap()
                .result,
        )
        .unwrap();
        let second_applied = store
            .apply_program_bundle(
                second_lease,
                &second_prepared,
                program_commit(
                    Some(first_cursor),
                    second_consensus.invocation.committed_batch,
                ),
            )
            .await
            .unwrap();
        require_result_matches_consensus(&second_applied, second_consensus.invocation).unwrap();
        let second_cursor = second_consensus.invocation.committed_batch.commit_cursor;
        assert_eq!(
            store.applied_program_commit_cursor().unwrap(),
            Some(second_cursor)
        );
        assert_eq!(
            coordinator
                .decisions
                .state()
                .unwrap()
                .unfinalized_commit_len(),
            2
        );

        drop(engine);
        coordinator.decisions.shutdown().await.unwrap();
        drop(coordinator);
        drop(store);

        let reopened_store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let reopened = open_test_coordinator(reopened_store, temporary.path()).await;
        let state = reopened.decisions.state().unwrap();
        assert_eq!(state.finalized_through(), Some(second_cursor));
        assert_eq!(state.unfinalized_commit_len(), 0);
        reopened.decisions.shutdown().await.unwrap();
    }

    #[test]
    fn program_and_invocation_identities_include_the_full_object_address() {
        let left = ObjectKey::new("tenant-a", "bucket", "_anvil/programs/import_osv@1").unwrap();
        let other_tenant =
            ObjectKey::new("tenant-b", "bucket", "_anvil/programs/import_osv@1").unwrap();
        let other_bucket =
            ObjectKey::new("tenant-a", "other", "_anvil/programs/import_osv@1").unwrap();

        assert_ne!(program_path_hash(&left), program_path_hash(&other_tenant));
        assert_ne!(program_path_hash(&left), program_path_hash(&other_bucket));
        assert_ne!(
            invocation_identity(&left, "same"),
            invocation_identity(&other_tenant, "same")
        );
    }

    #[test]
    fn only_nonempty_reserved_program_paths_are_accepted() {
        let valid = ObjectKey::new("tenant", "bucket", "_anvil/programs/import_osv@1").unwrap();
        assert!(
            validate_program_request(
                &valid,
                "invoke-1",
                b"{}",
                LOCAL_DURABILITY_CLASS,
                ProgramRuntimeTopology::OneNode,
            )
            .is_ok()
        );

        let outside = ObjectKey::new("tenant", "bucket", "programs/import_osv@1").unwrap();
        assert!(
            validate_program_request(
                &outside,
                "invoke-1",
                b"{}",
                LOCAL_DURABILITY_CLASS,
                ProgramRuntimeTopology::OneNode,
            )
            .is_err()
        );
    }

    #[test]
    fn program_durability_class_is_a_closed_exact_choice() {
        let key = ObjectKey::new("tenant", "bucket", "_anvil/programs/import_osv@1").unwrap();
        assert!(
            validate_program_request(
                &key,
                "invoke-1",
                b"{}",
                LOCAL_DURABILITY_CLASS,
                ProgramRuntimeTopology::OneNode,
            )
            .is_ok()
        );
        assert_eq!(
            validate_program_request(
                &key,
                "invoke-1",
                b"{}",
                REPLICATED_DURABILITY_CLASS,
                ProgramRuntimeTopology::OneNode,
            )
            .unwrap_err()
            .code(),
            tonic::Code::Unavailable
        );
        for supported in [LOCAL_DURABILITY_CLASS, REPLICATED_DURABILITY_CLASS] {
            assert!(
                validate_program_request(
                    &key,
                    "invoke-1",
                    b"{}",
                    supported,
                    ProgramRuntimeTopology::Clustered,
                )
                .is_ok()
            );
        }
        for invalid in ["", " local", "local ", "LOCAL", "remote"] {
            assert_eq!(
                validate_program_request(
                    &key,
                    "invoke-1",
                    b"{}",
                    invalid,
                    ProgramRuntimeTopology::Clustered,
                )
                .unwrap_err()
                .code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn atomic_failure_statuses_expose_stable_outcome_names() {
        let assertion = engine_status(EngineError::Assertion {
            index: 0,
            reason: "no".into(),
        });
        assert_eq!(assertion.code(), tonic::Code::FailedPrecondition);
        assert!(assertion.message().starts_with("ASSERTION_FAILED:"));

        let concurrency = engine_status(EngineError::ProgramConcurrency {
            path: ObjectPath::new("tenant", "bucket", "managed/value").unwrap(),
            reason: "dependency must use PROGRAM_ONLY policy".into(),
        });
        assert!(
            concurrency
                .message()
                .starts_with("PROGRAM_CONCURRENCY_VIOLATION:")
        );

        let version = program_store_status(ProgramStoreError::ProgramHashMismatch);
        assert!(version.message().starts_with("PROGRAM_VERSION_MISMATCH:"));

        let capacity = decision_status(DecisionRaftError::Rejected(
            ApplyError::CommittedInvocationWindowFull {
                entries: anvil_consensus::MAX_COMMITTED_INVOCATIONS,
                bytes: anvil_consensus::MAX_COMMITTED_INVOCATION_BYTES,
                required_bytes: 1,
            },
        ));
        assert_eq!(capacity.code(), tonic::Code::ResourceExhausted);
        assert!(capacity.message().starts_with("RESOURCE_LIMIT:"));

        let lag = decision_status(DecisionRaftError::Rejected(ApplyError::CommitTailFull {
            entries: 1,
            bytes: 1,
            required_bytes: 1,
            max_entries: 1,
            max_bytes: 1,
        }));
        assert_eq!(lag.code(), tonic::Code::ResourceExhausted);
        assert!(lag.message().starts_with("FINALIZATION_LAG:"));
    }

    #[test]
    fn local_durability_requires_synced_evidence_from_the_executor() {
        let evidence_hash = ProgramDurabilityEvidenceHash([7; 32]);
        assert_eq!(
            accepted_program_evidence_hash(
                &ProgramDurabilityScope::ExecutorLocal {
                    node_id: 3,
                    synced: true,
                },
                evidence_hash,
                LOCAL_DURABILITY_CLASS,
                NodeId(3),
            )
            .unwrap(),
            evidence_hash
        );

        let unsynced = accepted_program_evidence_hash(
            &ProgramDurabilityScope::ExecutorLocal {
                node_id: 3,
                synced: false,
            },
            evidence_hash,
            LOCAL_DURABILITY_CLASS,
            NodeId(3),
        )
        .unwrap_err();
        assert_eq!(unsynced.code(), tonic::Code::Unavailable);

        let wrong_node = accepted_program_evidence_hash(
            &ProgramDurabilityScope::ExecutorLocal {
                node_id: 4,
                synced: true,
            },
            evidence_hash,
            LOCAL_DURABILITY_CLASS,
            NodeId(3),
        )
        .unwrap_err();
        assert_eq!(wrong_node.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn commit_cursor_comparison_detects_pending_recovery() {
        assert_eq!(compare_commit_cursors(None, None), Ordering::Equal);
        assert_eq!(compare_commit_cursors(None, Some(10)), Ordering::Less);
        assert_eq!(compare_commit_cursors(Some(9), Some(10)), Ordering::Less);
        assert_eq!(compare_commit_cursors(Some(10), Some(10)), Ordering::Equal);
        assert_eq!(
            compare_commit_cursors(Some(11), Some(10)),
            Ordering::Greater
        );
        assert_eq!(compare_commit_cursors(Some(10), None), Ordering::Greater);
    }

    #[test]
    fn committed_batch_mapping_retains_every_storage_identity() {
        let committed = CommittedBatch {
            commit_cursor: 12,
            executor: NodeId(3),
            nomination_log_index: 7,
            program_path_hash: ProgramPathHash([1; 32]),
            program_hash: DecisionProgramHash([2; 32]),
            bundle_ref: BundleRef {
                hash: [3; 32],
                length: 33,
            },
            bundle_hash: BundleHash([4; 32]),
            durability_class: DurabilityClass([5; 32]),
            durability_evidence_hash: DurabilityEvidenceHash([6; 32]),
        };

        assert_eq!(
            program_commit(Some(11), committed),
            ProgramCommit {
                previous_commit_cursor: Some(11),
                commit_cursor: 12,
                bundle_ref: PreparedBundleRef {
                    hash: [3; 32],
                    length: 33,
                },
                bundle_hash: PreparedBundleHash([4; 32]),
                program_hash: ProgramHash([2; 32]),
                durability_class: ProgramDurabilityClassHash([5; 32]),
                durability_evidence_hash: ProgramDurabilityEvidenceHash([6; 32]),
            }
        );
    }

    #[test]
    fn prepared_replay_result_must_match_the_committed_invocation() {
        let fingerprint = [9; 32];
        let committed = CommittedBatch {
            commit_cursor: 12,
            executor: NodeId(3),
            nomination_log_index: 7,
            program_path_hash: ProgramPathHash([1; 32]),
            program_hash: DecisionProgramHash([2; 32]),
            bundle_ref: BundleRef {
                hash: [3; 32],
                length: 33,
            },
            bundle_hash: BundleHash([4; 32]),
            durability_class: DurabilityClass([5; 32]),
            durability_evidence_hash: DurabilityEvidenceHash([6; 32]),
        };
        let invocation = CommittedInvocation {
            invocation_id: InvocationId([8; 32]),
            input_fingerprint: InvocationFingerprint(fingerprint),
            proposal_at_unix_millis: 1_000,
            replay_expires_at_unix_millis: 1_000 + ATOMIC_REPLAY_RETENTION_MILLIS,
            committed_batch: committed,
        };
        let result = CommittedProgramResult {
            receipt: CommandReceipt {
                program_path_hash: committed.program_path_hash.0,
                command_id: "recover-identity".into(),
                input_fingerprint: hex::encode(fingerprint),
                outputs: BTreeMap::new(),
            },
            published_versions: BTreeMap::new(),
        };

        require_result_matches_consensus(&result, invocation).unwrap();
        let mut wrong = result;
        wrong.receipt.input_fingerprint = hex::encode([99; 32]);
        assert_eq!(
            require_result_matches_consensus(&wrong, invocation)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
}
