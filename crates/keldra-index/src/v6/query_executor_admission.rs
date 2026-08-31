use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;

use super::{
    MAX_QUERY_DOCUMENT_PATH_BYTES, ProjectionPartitionIdentity, ProjectionQueryStreamRoot,
    QueryDocumentGate, StableDocumentKey,
};

pub(super) fn match_all_live_documents(
    gates: &BTreeMap<StableDocumentKey, QueryDocumentGate>,
) -> BTreeSet<StableDocumentKey> {
    gates
        .iter()
        .filter_map(|(key, gate)| gate.live.then_some(*key))
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCommonCut {
    pub through_atomic_position: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRootCutProof {
    pub common_cut: QueryCommonCut,
    pub selected_stream_root_hash: [u8; 32],
    /// `None` asserts that the selected root was current when pinned. A value
    /// identifies the immediate newer retained root and must be beyond the cut.
    pub next_newer_through_atomic_position: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PinnedPartitionQueryRoot {
    pub partition: ProjectionPartitionIdentity,
    pub physical_catalog_generation: [u8; 32],
    pub root: ProjectionQueryStreamRoot,
    pub cut_proof: QueryRootCutProof,
    /// Stable across predecessor and successor incarnations of one handoff.
    pub handoff_lineage_id: [u8; 32],
}

impl PinnedPartitionQueryRoot {
    pub(crate) fn validate_at(self, cut: QueryCommonCut) -> Result<(), IndexError> {
        self.partition.validate()?;
        self.root
            .validate_at(self.root.next_offset, self.root.through_atomic_position)?;
        if self.cut_proof.common_cut != cut
            || self.cut_proof.selected_stream_root_hash != self.root.stream_root_hash
            || self.root.through_atomic_position > cut.through_atomic_position
            || self.handoff_lineage_id == [0; 32]
            || self
                .cut_proof
                .next_newer_through_atomic_position
                .is_some_and(|next| next <= cut.through_atomic_position)
        {
            return Err(IndexError::InvalidQuery(
                "partition root is not the newest eligible root at the common cut".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn covered_through_source_position(self) -> Result<u64, IndexError> {
        self.root
            .next_offset
            .checked_sub(1)
            .ok_or(IndexError::Integrity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAdmissionCandidate {
    pub partition: ProjectionPartitionIdentity,
    pub handoff_lineage_id: [u8; 32],
    pub covered_through_source_position: u64,
    pub document: StableDocumentKey,
    pub material_source_version: u64,
    pub current_source_version: u64,
    pub source_path: String,
    pub result_path: String,
    pub result_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedQueryCandidate {
    pub candidate: QueryAdmissionCandidate,
    /// Exact current ordinary result identity, returned only after runtime
    /// authorization succeeds at the pinned logical definition and cut.
    pub result_path: String,
    pub result_version: u64,
}

impl AuthorizedQueryCandidate {
    pub(crate) fn validate_for(
        &self,
        expected: &QueryAdmissionCandidate,
    ) -> Result<(), IndexError> {
        if &self.candidate != expected
            || expected.source_path.is_empty()
            || expected.source_path.len() > MAX_QUERY_DOCUMENT_PATH_BYTES
            || expected.source_path.contains('\0')
            || expected.current_source_version < expected.material_source_version
            || expected.result_path.is_empty()
            || expected.result_path.len() > MAX_QUERY_DOCUMENT_PATH_BYTES
            || expected.result_path.contains('\0')
            || expected.result_version == 0
            || self.result_path.is_empty()
            || self.result_path.len() > MAX_QUERY_DOCUMENT_PATH_BYTES
            || self.result_path.contains('\0')
            || self.result_path != expected.result_path
            || self.result_version != expected.result_version
            || self.result_version == 0
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAdmissionContext {
    pub logical_index_id: u64,
    pub logical_definition_version: u64,
    pub common_cut: QueryCommonCut,
    pub candidate: QueryAdmissionCandidate,
}

/// Runtime trust-boundary admission. `None` means either not exact-current or
/// unauthorized; the executor deliberately does not distinguish those cases.
pub trait QueryCandidateAdmission: Send {
    fn admit_exact_current_authorized(
        &mut self,
        context: QueryAdmissionContext,
    ) -> impl std::future::Future<Output = Result<Option<AuthorizedQueryCandidate>, IndexError>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryArtifactKind {
    Page,
    Run,
    Block,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryArtifactLoad {
    pub kind: QueryArtifactKind,
    pub hash: [u8; 32],
    pub encoded_bytes: usize,
}
