//! Retained state for retryable and asynchronously verified v1 publication.

use super::*;
use std::future::Future;
use std::pin::Pin;

type PostCasVerificationFuture = Pin<Box<dyn Future<Output = Result<(), Status>> + Send>>;
type PostCasVerificationFactory =
    Arc<dyn Fn() -> PostCasVerificationFuture + Send + Sync + 'static>;

#[derive(Clone, Default)]
pub(super) struct ObservedSourceProgress(
    Arc<std::sync::Mutex<BTreeMap<ProjectionPartitionIdentity, u64>>>,
);

impl ObservedSourceProgress {
    pub(super) fn replace(&self, observations: &BTreeMap<ProjectionPartitionIdentity, u64>) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = observations.clone();
    }

    pub(super) fn get(&self, partition: ProjectionPartitionIdentity) -> Option<u64> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&partition)
            .copied()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedV1ProjectionGeneration {
    pub(crate) current: ProjectionCurrent,
    pub(crate) current_object_version: VersionId,
    pub(crate) generation: ProjectionGeneration,
}

pub(crate) enum V1PublicationPredecessor<'a> {
    Initial,
    Current(&'a LoadedV1ProjectionGeneration),
    CatalogRebuild(VersionId),
}

#[derive(Clone, Debug)]
pub(super) struct ArtifactBytes {
    pub(super) path: String,
    pub(super) kind: keldra_index::v1::ProjectionArtifactKind,
    pub(super) hash: [u8; 32],
    pub(super) bytes: Bytes,
}

pub(super) struct StagedArtifact {
    pub(super) path: String,
    pub(super) kind: keldra_index::v1::ProjectionArtifactKind,
    pub(super) hash: [u8; 32],
    pub(super) blob: BlobRef,
    pub(super) needs_publication: bool,
}

pub(super) struct InlineArtifactIdentity {
    pub(super) path: String,
    pub(super) kind: keldra_index::v1::ProjectionArtifactKind,
    pub(super) hash: [u8; 32],
    pub(super) length: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ImmutableStageWindow {
    Inline { items: usize, bytes: usize },
    Unary { bytes: usize },
}

#[derive(Debug)]
pub(super) struct AtomicPublicationPlan {
    pub(super) immutable: Vec<ArtifactBytes>,
    pub(super) current_bytes: Vec<u8>,
    pub(super) current: ProjectionCurrent,
    pub(super) generation: ProjectionGeneration,
    pub(super) sealed_bytes: u64,
    pub(super) source_positions: u64,
    pub(super) _publication_credits: Option<AtomicProjectionPublicationCredits>,
    pub(super) _compaction_metadata: Option<keldra_index::v1::IndexingMemoryPermit>,
}

/// Fully encoded deterministic successor retained across transient staging,
/// replication, and Current-CAS failures. Immutable bytes use ref-counted
/// storage, so retry does not rebuild or copy the generation.
pub(crate) struct PendingV1Publication {
    pub(super) plan: AtomicPublicationPlan,
    pub(super) expected_current_version: Option<VersionId>,
    pub(super) previous_generation_hash: Option<[u8; 32]>,
    pub(super) checkpointed_source_positions: u64,
    pub(super) checkpointed_source_payload_bytes: u64,
    /// Retains compaction admission until every rebased prerequisite page is
    /// durably included by this publication attempt.
    pub(super) _compaction: Option<V1CompactionArtifacts>,
    /// Retains ordinary shared metadata admission through staging/CAS retries.
    pub(super) _atomic_cut: Option<QueryBlockCredits>,
}

/// Mandatory exact readback for one authoritative Current publication. The
/// next generation may be prepared and its immutable artifacts may be made
/// durable concurrently, but its Current CAS must await this proof.
pub(crate) struct V1PostCasVerification {
    pub(super) task: Option<tokio::task::JoinHandle<Result<(), Status>>>,
    retry: PostCasVerificationFactory,
}

impl V1PostCasVerification {
    pub(super) fn start<F, Fut>(factory: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let retry: PostCasVerificationFactory = Arc::new(move || Box::pin(factory()));
        let task = Some(tokio::spawn(retry()));
        Self { task, retry }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    async fn finish_attempt(&mut self) -> Result<(), Status> {
        let result = match self
            .task
            .as_mut()
            .expect("v1 post-CAS verifier task exists")
            .await
        {
            Ok(result) => result,
            Err(error) => Err(Status::internal(format!(
                "v1 post-CAS verifier failed: {error}"
            ))),
        };
        // Keep ownership installed across the await. Cancelling a caller must
        // neither detach this task nor erase the mandatory readback obligation.
        self.task.take();
        if result
            .as_ref()
            .is_err_and(|error| error.code() != tonic::Code::DataLoss)
        {
            self.task = Some(tokio::spawn((self.retry)()));
        }
        result
    }
}

/// Complete one mandatory exact readback. A transient failure leaves a fresh
/// attempt installed in `verification`, so the next publication cannot evade
/// the proof by retrying. Only success or a contained integrity failure clears
/// the obligation.
pub(crate) async fn finish_required_post_cas_verification(
    verification: &mut Option<V1PostCasVerification>,
) -> Result<(), Status> {
    let result = verification
        .as_mut()
        .expect("v1 post-CAS verifier exists")
        .finish_attempt()
        .await;
    if result.is_ok()
        || result
            .as_ref()
            .is_err_and(|error| error.code() == tonic::Code::DataLoss)
    {
        verification.take();
    }
    result
}

impl Drop for V1PostCasVerification {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
