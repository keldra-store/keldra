use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anvil_atomic_program::{
    CommandReceipt, EngineError, ExpandedProgramPath, InvocationContext, ObjectPath, ProgramInput,
    ProgramInvocation,
};
use anvil_consensus::{
    ApplyResult, BundleHash, BundleRef, Command, CommitBatch, CommittedBatch, DecisionRaft,
    DecisionRaftError, DurabilityClass, DurabilityEvidenceHash, ExecutorNomination,
    InvocationFingerprint, InvocationId, InvocationReceipt, NoPeerTransport, NodeId, PeerNode,
    ProgramHash as DecisionProgramHash, ProgramPathHash,
};
use anvil_store::{
    AppliedProgramReceipt, ObjectKey, PreparedBundleHash, PreparedBundleRef, ProgramCommit,
    ProgramDurabilityClassHash, ProgramDurabilityEvidenceHash, ProgramDurabilityScope, ProgramHash,
    ProgramStoreError, Store, VerifiedProgramDefinition, VersionId,
};
use anyhow::{Context, Result, bail};
use tonic::Status;

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const MAX_PROGRAM_INPUT_BYTES: usize = 16 * 1024 * 1024;
const PROGRAM_PATH_PREFIX: &str = "_anvil/programs/";

#[derive(Clone)]
pub(crate) struct ProgramCoordinator {
    store: Store,
    decisions: DecisionRaft,
    node: NodeId,
    /// Serializes the one global Raft visibility decision with local
    /// finalization. Program evaluation and exact-path locking remain
    /// concurrent before this short boundary.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug)]
pub(crate) struct InvokedProgramResult {
    pub receipt: CommandReceipt,
    pub executor_nomination_log_index: u64,
    pub commit_log_index: u64,
    pub invocation_fingerprint: [u8; 32],
    pub program_path_hash: [u8; 32],
    pub program_hash: [u8; 32],
    pub bundle_ref: [u8; 32],
    pub bundle_hash: [u8; 32],
    pub published_versions: BTreeMap<ObjectPath, VersionId>,
    pub replayed: bool,
}

impl ProgramCoordinator {
    pub async fn open(
        store: Store,
        data_dir: &Path,
        node_id: u64,
        max_commit_entries: u32,
        max_commit_bytes: u64,
    ) -> Result<Self> {
        let node = NodeId(node_id);
        let decisions = DecisionRaft::open(
            data_dir.join("decisions"),
            node_id,
            max_commit_entries,
            max_commit_bytes,
            Arc::new(NoPeerTransport),
        )
        .await
        .context("open bounded atomic decision Raft")?;
        decisions
            .ensure_one_node(PeerNode::new(format!("anvil-local://{node_id}")))
            .await
            .context("bootstrap one-node decision Raft")?;
        decisions
            .wait_for_leader(LEADER_TIMEOUT)
            .await
            .context("elect decision leader")?;

        if decisions.state()?.executor().is_none() {
            expect_nomination(
                decisions
                    .submit(Command::NominateExecutor { executor: node })
                    .await
                    .context("nominate the one-node atomic executor")?
                    .result,
                node,
            )?;
        }

        let coordinator = Self {
            store,
            decisions,
            node,
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
        };
        coordinator
            .recover_committed_tail()
            .await
            .context("recover committed atomic-program bundles")?;
        Ok(coordinator)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.decisions
            .shutdown()
            .await
            .context("shut down decision Raft")
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
        validate_program_request(&program_key, &invocation_id, input_json, durability_class)?;
        let nomination = self.current_nomination()?;

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
            if let Some(receipt) = self
                .decisions
                .state()
                .map_err(decision_status)?
                .invocation_receipt(consensus_invocation_id)
            {
                return self
                    .replay_committed_invocation(
                        receipt,
                        fingerprint,
                        program_path_hash,
                        expected_program_hash,
                    )
                    .await;
            }

            let lease = engine
                .prepare(&context, &invocation)
                .await
                .map_err(engine_status)?;

            let prepared = self
                .store
                .prepare_program_bundle(&lease)
                .await
                .map_err(program_store_status)?;
            let evidence_hash = prepared
                .remote_durability_evidence_hash()
                .map_err(program_store_status)?;
            match &prepared.durability.scope {
                ProgramDurabilityScope::ConfiguredRemote { class } if class == durability_class => {
                }
                ProgramDurabilityScope::ConfiguredRemote { .. } => {
                    return Err(Status::failed_precondition(
                        "prepared durability evidence does not match the requested class",
                    ));
                }
                ProgramDurabilityScope::ExecutorLocal { .. } => {
                    return Err(Status::unavailable(
                        "atomic commit requires remotely recoverable prepared artifacts",
                    ));
                }
            }

            let _commit_guard = self.commit_gate.lock().await;
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
                    "atomic executor moved while the program was preparing; retry the same invocation id",
                ));
            }
            let committed = self
                .decisions
                .submit(Command::CommitBatch(CommitBatch {
                    executor: self.node,
                    nomination_log_index: nomination.nomination_log_index,
                    program_path_hash: ProgramPathHash(program_path_hash),
                    program_hash: DecisionProgramHash(expected_program_hash),
                    invocation_id: consensus_invocation_id,
                    input_fingerprint: InvocationFingerprint(fingerprint),
                    bundle_ref: BundleRef(prepared.bundle.hash),
                    bundle_hash: BundleHash(prepared.hash.0),
                    durability_class: DurabilityClass(
                        ProgramDurabilityClassHash::for_class(durability_class).0,
                    ),
                    durability_evidence_hash: DurabilityEvidenceHash(evidence_hash.0),
                }))
                .await
                .map_err(decision_status)?;
            let committed = expect_batch_committed(committed.result)?;
            if committed.replayed {
                drop(lease);
                self.recover_committed_tail_locked()
                    .await
                    .map_err(internal)?;
                let applied = self
                    .store
                    .applied_program_receipt(committed.receipt.committed_batch.commit_cursor)
                    .map_err(program_store_status)?
                    .ok_or_else(|| Status::data_loss("replayed program receipt is missing"))?;
                return replay_result(applied, committed.receipt);
            }

            let applied = self
                .store
                .apply_program_bundle(
                    lease,
                    &prepared,
                    program_commit(applied_cursor, committed.receipt.committed_batch),
                )
                .await
                .map_err(program_store_status)?;
            return Ok(invoked_result(applied, committed.receipt, false));
        }
    }

    async fn replay_committed_invocation(
        &self,
        receipt: InvocationReceipt,
        fingerprint: [u8; 32],
        program_path_hash: [u8; 32],
        program_hash: [u8; 32],
    ) -> Result<InvokedProgramResult, Status> {
        require_same_invocation(receipt, fingerprint, program_path_hash, program_hash)?;
        self.recover_committed_tail().await.map_err(internal)?;
        let applied = self
            .store
            .applied_program_receipt(receipt.committed_batch.commit_cursor)
            .map_err(program_store_status)?
            .ok_or_else(|| Status::data_loss("committed program receipt is missing"))?;
        replay_result(applied, receipt)
    }

    fn current_nomination(&self) -> Result<ExecutorNomination, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(decision_status)?
            .executor()
            .ok_or_else(|| Status::unavailable("no atomic executor is nominated"))?;
        if nomination.executor != self.node {
            return Err(Status::unavailable(format!(
                "atomic request belongs on nominated executor node {}",
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

        for receipt in state.committed_batches() {
            let batch = receipt.committed_batch;
            if applied.is_some_and(|cursor| cursor >= batch.commit_cursor) {
                continue;
            }
            self.store
                .recover_program_bundle(program_commit(applied, batch))
                .await
                .with_context(|| format!("finalize atomic commit {}", batch.commit_cursor))?;
            applied = Some(batch.commit_cursor);
        }
        Ok(())
    }
}

fn validate_program_request(
    key: &ObjectKey,
    invocation_id: &str,
    input_json: &[u8],
    durability_class: &str,
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
    if durability_class.trim().is_empty() || durability_class.len() > 256 {
        return Err(Status::invalid_argument(
            "durability_class must contain between 1 and 256 bytes",
        ));
    }
    Ok(())
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
        bundle_ref: PreparedBundleRef(committed.bundle_ref.0),
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
    receipt: InvocationReceipt,
    fingerprint: [u8; 32],
    program_path_hash: [u8; 32],
    program_hash: [u8; 32],
) -> Result<(), Status> {
    if receipt.input_fingerprint != InvocationFingerprint(fingerprint) {
        return Err(Status::already_exists(
            "invocation id was reused with different input",
        ));
    }
    if receipt.committed_batch.program_path_hash != ProgramPathHash(program_path_hash)
        || receipt.committed_batch.program_hash != DecisionProgramHash(program_hash)
    {
        return Err(Status::already_exists(
            "invocation id was reused for a different program",
        ));
    }
    Ok(())
}

fn replay_result(
    applied: AppliedProgramReceipt,
    receipt: InvocationReceipt,
) -> Result<InvokedProgramResult, Status> {
    let fingerprint = decode_fingerprint(&applied.receipt.input_fingerprint)?;
    let program_path_hash = applied.receipt.program_path_hash;
    require_same_invocation(
        receipt,
        fingerprint,
        program_path_hash,
        applied.program_hash.0,
    )?;
    Ok(invoked_result(applied, receipt, true))
}

fn invoked_result(
    applied: AppliedProgramReceipt,
    receipt: InvocationReceipt,
    replayed: bool,
) -> InvokedProgramResult {
    let batch = receipt.committed_batch;
    InvokedProgramResult {
        receipt: applied.receipt,
        executor_nomination_log_index: batch.nomination_log_index,
        commit_log_index: batch.commit_cursor,
        invocation_fingerprint: receipt.input_fingerprint.0,
        program_path_hash: batch.program_path_hash.0,
        program_hash: batch.program_hash.0,
        bundle_ref: batch.bundle_ref.0,
        bundle_hash: batch.bundle_hash.0,
        published_versions: applied.published_versions,
        replayed,
    }
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

fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::InvalidDefinition(_)
        | EngineError::InvalidInvocation(_)
        | EngineError::HeadPrecondition { .. }
        | EngineError::Assertion { .. }
        | EngineError::Operation { .. }
        | EngineError::Return { .. }
        | EngineError::Emission { .. }
        | EngineError::LimitExceeded(_) => Status::failed_precondition(error.to_string()),
        EngineError::Read(_) | EngineError::InvalidSnapshot(_) => {
            Status::internal(error.to_string())
        }
    }
}

fn mutation_status(error: anvil_store::MutationError) -> Status {
    match error {
        anvil_store::MutationError::PreconditionFailed { .. }
        | anvil_store::MutationError::Immutable
        | anvil_store::MutationError::ProgramConcurrencyViolation => {
            Status::failed_precondition(error.to_string())
        }
        anvil_store::MutationError::IdempotencyConflict => {
            Status::already_exists(error.to_string())
        }
        anvil_store::MutationError::InvalidCommandId
        | anvil_store::MutationError::InvalidPolicy(_) => {
            Status::invalid_argument(error.to_string())
        }
        anvil_store::MutationError::BlobNotFound => Status::not_found(error.to_string()),
        anvil_store::MutationError::Storage(_) => Status::internal(error.to_string()),
    }
}

fn program_store_status(error: ProgramStoreError) -> Status {
    match error {
        ProgramStoreError::InvalidDefinition(_) | ProgramStoreError::InvalidBundle(_) => {
            Status::invalid_argument(error.to_string())
        }
        ProgramStoreError::PreparedBundleNotFound(_)
        | ProgramStoreError::ArtifactNotFound(_)
        | ProgramStoreError::DurabilityEvidenceNotFound(_) => Status::not_found(error.to_string()),
        ProgramStoreError::ProgramHashMismatch
        | ProgramStoreError::HashCollision
        | ProgramStoreError::CommitCorruption { .. }
        | ProgramStoreError::ArtifactCorruption(_) => Status::already_exists(error.to_string()),
        ProgramStoreError::ExecutorLocalDurability => Status::unavailable(error.to_string()),
        ProgramStoreError::ProgramPolicy { .. }
        | ProgramStoreError::Immutable { .. }
        | ProgramStoreError::PreconditionFailed { .. }
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
        DecisionRaftError::Rejected(_) => Status::failed_precondition(error.to_string()),
        DecisionRaftError::InvalidNodeId | DecisionRaftError::Configuration(_) => {
            Status::invalid_argument(error.to_string())
        }
        DecisionRaftError::Storage(_) | DecisionRaftError::StatePoisoned => {
            Status::internal(error.to_string())
        }
    }
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(validate_program_request(&valid, "invoke-1", b"{}", "remote").is_ok());

        let outside = ObjectKey::new("tenant", "bucket", "programs/import_osv@1").unwrap();
        assert!(validate_program_request(&outside, "invoke-1", b"{}", "remote").is_err());
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
            bundle_ref: BundleRef([3; 32]),
            bundle_hash: BundleHash([4; 32]),
            durability_class: DurabilityClass([5; 32]),
            durability_evidence_hash: DurabilityEvidenceHash([6; 32]),
        };

        assert_eq!(
            program_commit(Some(11), committed),
            ProgramCommit {
                previous_commit_cursor: Some(11),
                commit_cursor: 12,
                bundle_ref: PreparedBundleRef([3; 32]),
                bundle_hash: PreparedBundleHash([4; 32]),
                program_hash: ProgramHash([2; 32]),
                durability_class: ProgramDurabilityClassHash([5; 32]),
                durability_evidence_hash: ProgramDurabilityEvidenceHash([6; 32]),
            }
        );
    }
}
