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
    maximum_authorized: Option<usize>,
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
        let remaining = maximum_authorized
            .map(|maximum| maximum.saturating_sub(candidates.len()))
            .unwrap_or(MAX_QUERY_CANDIDATE_ADMISSION_BATCH);
        if remaining == 0 {
            for candidate in selected {
                budget.release_heap(credits, resident_selected_candidate_bytes(&candidate)?)?;
            }
            break;
        }
        let batch_limit = MAX_QUERY_CANDIDATE_ADMISSION_BATCH.min(remaining);
        let planned_batch = selected.len().min(batch_limit);
        let batch_vector_bytes = planned_batch
            .checked_mul(std::mem::size_of::<QueryAdmissionCandidate>())
            .ok_or(IndexError::OffsetOverflow)?;
        budget.reserve_heap(credits, batch_vector_bytes)?;
        let batch = selected.by_ref().take(batch_limit).collect::<Vec<_>>();
        if batch.is_empty() {
            budget.release_heap(credits, batch_vector_bytes)?;
            break;
        }
        let batch_len = batch.len();
        let context_bytes = batch.iter().try_fold(0usize, |total, candidate| {
            total
                .checked_add(resident_admission_context_bytes(candidate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let selected_bytes = batch.iter().try_fold(0usize, |total, candidate| {
            total
                .checked_add(resident_selected_candidate_bytes(candidate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let expected_bytes = batch_len
            .checked_mul(std::mem::size_of::<(
                ProjectionPartitionIdentity,
                StableDocumentKey,
            )>())
            .ok_or(IndexError::OffsetOverflow)?;
        budget.reserve_heap(
            credits,
            context_bytes
                .checked_add(expected_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let expected = batch
            .iter()
            .map(|candidate| (candidate.partition, candidate.document))
            .collect::<Vec<_>>();
        let contexts = batch
            .into_iter()
            .map(|candidate| QueryAdmissionContext {
                logical_index_id,
                logical_definition_version,
                common_cut,
                candidate,
            })
            .collect();
        budget.release_heap(credits, batch_vector_bytes)?;
        let admitted = admission
            .admit_snapshot_current_authorized_batch(contexts)
            .await?;
        if admitted.len() != batch_len {
            return Err(IndexError::Integrity);
        }
        for (expected, admitted) in expected.into_iter().zip(admitted) {
            if let Some(admitted) = admitted {
                admitted.validate()?;
                let candidate = &admitted.candidate;
                if (candidate.partition, candidate.document) != expected {
                    return Err(IndexError::Integrity);
                }
                budget.reserve_heap(credits, resident_authorized_candidate_bytes(&admitted)?)?;
                let key = (candidate.partition, candidate.document);
                candidates.push(QueryCandidate {
                    partition: candidate.partition,
                    document: candidate.document,
                    material_source_version: candidate.material_source_version,
                });
                authorized.insert(key, admitted);
            }
        }
        budget.release_heap(credits, selected_bytes)?;
        budget.release_heap(
            credits,
            context_bytes
                .checked_add(expected_bytes)
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
    }
    Ok((authorized, candidates))
}

fn resident_authorized_candidate_bytes(
    admitted: &AuthorizedQueryCandidate,
) -> Result<usize, IndexError> {
    let candidate = &admitted.candidate;
    std::mem::size_of::<AuthorizedQueryCandidate>()
        .checked_add(std::mem::size_of::<QueryCandidate>())
        .and_then(|bytes| {
            bytes.checked_add(std::mem::size_of::<(
                ProjectionPartitionIdentity,
                StableDocumentKey,
            )>())
        })
        .and_then(|bytes| bytes.checked_add(candidate.source_path.len()))
        .and_then(|bytes| {
            bytes.checked_add(
                candidate
                    .canonical_source_path
                    .as_ref()
                    .map_or(0, String::len),
            )
        })
        .and_then(|bytes| bytes.checked_add(candidate.result_path.len()))
        .ok_or(IndexError::OffsetOverflow)
}
