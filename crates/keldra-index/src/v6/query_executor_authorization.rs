use std::collections::BTreeMap;

use crate::IndexError;

use super::admission::{resident_admission_context_bytes, resident_selected_candidate_bytes};
use super::{
    AuthorizedQueryCandidate, Budget, MAX_QUERY_CANDIDATE_ADMISSION_BATCH,
    ProjectionPartitionIdentity, QueryAdmissionCandidate, QueryAdmissionContext, QueryBlockCredits,
    QueryCandidate, QueryCandidateAdmission, QueryCommonCut, StableDocumentKey,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn authorize_selected_candidates<A: QueryCandidateAdmission>(
    admission: &mut A,
    selected: BTreeMap<StableDocumentKey, QueryAdmissionCandidate>,
    logical_index_id: u64,
    logical_definition_version: u64,
    common_cut: QueryCommonCut,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<
    (
        BTreeMap<(ProjectionPartitionIdentity, StableDocumentKey), AuthorizedQueryCandidate>,
        Vec<QueryCandidate>,
    ),
    IndexError,
> {
    let mut authorized = BTreeMap::new();
    let mut candidates = Vec::new();
    let mut selected = selected.into_values();
    loop {
        let batch = selected
            .by_ref()
            .take(MAX_QUERY_CANDIDATE_ADMISSION_BATCH)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        let context_bytes = batch.iter().try_fold(0usize, |total, candidate| {
            total
                .checked_add(resident_admission_context_bytes(candidate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        budget.reserve_heap(credits, context_bytes)?;
        let contexts = batch
            .iter()
            .cloned()
            .map(|candidate| QueryAdmissionContext {
                logical_index_id,
                logical_definition_version,
                common_cut,
                candidate,
            })
            .collect();
        let admitted = admission
            .admit_exact_current_authorized_batch(contexts)
            .await?;
        if admitted.len() != batch.len() {
            return Err(IndexError::Integrity);
        }
        for (candidate, admitted) in batch.into_iter().zip(admitted) {
            let selected_bytes = resident_selected_candidate_bytes(&candidate)?;
            if let Some(admitted) = admitted {
                admitted.validate_for(&candidate)?;
                budget.reserve_heap(
                    credits,
                    resident_authorized_candidate_bytes(&candidate, &admitted)?,
                )?;
                let key = (candidate.partition, candidate.document);
                authorized.insert(key, admitted);
                candidates.push(QueryCandidate {
                    partition: candidate.partition,
                    document: candidate.document,
                    material_source_version: candidate.material_source_version,
                });
            }
            budget.release_heap(credits, selected_bytes)?;
        }
        budget.release_heap(credits, context_bytes)?;
    }
    Ok((authorized, candidates))
}

fn resident_authorized_candidate_bytes(
    candidate: &QueryAdmissionCandidate,
    admitted: &AuthorizedQueryCandidate,
) -> Result<usize, IndexError> {
    std::mem::size_of::<AuthorizedQueryCandidate>()
        .checked_add(std::mem::size_of::<QueryCandidate>())
        .and_then(|bytes| {
            bytes.checked_add(std::mem::size_of::<(
                ProjectionPartitionIdentity,
                StableDocumentKey,
            )>())
        })
        .and_then(|bytes| bytes.checked_add(admitted.result_path.len()))
        .and_then(|bytes| bytes.checked_add(candidate.source_path.len()))
        .and_then(|bytes| bytes.checked_add(candidate.result_path.len()))
        .ok_or(IndexError::OffsetOverflow)
}
