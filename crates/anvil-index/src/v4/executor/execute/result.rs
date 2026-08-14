use super::*;

pub(super) fn validate_gate(
    request: &NativeQueryRequest,
    count: usize,
    evidence: &super::super::super::CandidateGateEvidence,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<(), IndexError> {
    statistics.candidate_gate_batch();
    let checked = u64::try_from(count).map_err(|_| IndexError::OffsetOverflow)?;
    statistics.candidate_gate_checked(checked);
    let rejected = evidence.denied.checked_add(evidence.stale);
    let invisible = u64::try_from(evidence.visible.iter().filter(|visible| !**visible).count())
        .map_err(|_| IndexError::OffsetOverflow)?;
    if evidence.visible.len() != count
        || evidence.authorization_revision != request.authorization_revision
        || rejected != Some(invisible)
    {
        return Err(IndexError::InvalidQuery(
            "candidate gate returned incomplete or differently pinned evidence".into(),
        ));
    }
    statistics.candidate_gate_denied(evidence.denied);
    statistics.candidate_gate_stale(evidence.stale);
    Ok(())
}

pub(super) async fn materialize<D: ArtifactDirectoryRead>(
    request: &NativeQueryRequest,
    executions: &mut [SegmentExecution<'_, D>],
    selected: Vec<Selected>,
    facet_results: Vec<super::super::super::FacetResult>,
    aggregate_results: Vec<super::super::super::AggregateResult>,
    maximum_page_bytes: usize,
    statistics: &NativeQueryStatisticsRecorder,
) -> Result<NativeQueryPage, IndexError> {
    let selected_count = selected.len();
    let hit_capacity = selected_count.min(
        maximum_page_bytes.saturating_sub(std::mem::size_of::<NativeQueryPage>())
            / std::mem::size_of::<NativeQueryHit>(),
    );
    let mut hits = Vec::with_capacity(hit_capacity);
    let mut retained_bytes = super::super::memory::page_base_bytes(hits.capacity())?;
    let mut retained_segment: Option<usize> = None;
    let mut truncated = selected_count > hit_capacity;
    for selected in selected {
        if hits.len() == hits.capacity() {
            truncated = true;
            break;
        }
        if retained_segment != Some(selected.segment_index) {
            if let Some(previous) = retained_segment.take() {
                executions[previous].values.release_decoded();
            }
            retained_segment = Some(selected.segment_index);
        }
        let cursor = NativeQueryCursor {
            sort_values: selected.sort_values,
            result: selected.result.clone(),
            source: selected.source.clone(),
            source_record: selected.source_record,
        };
        let hit = NativeQueryHit {
            source: selected.source,
            result: selected.result,
            score: selected.score,
            cursor,
        };
        let hit_bytes = super::super::memory::hit_owned_bytes(&hit)?;
        let continuation_bytes = super::super::memory::cursor_owned_bytes(&hit.cursor)?;
        let needed = retained_bytes
            .checked_add(hit_bytes)
            .and_then(|value| value.checked_add(continuation_bytes))
            .ok_or(IndexError::OffsetOverflow)?;
        if needed > maximum_page_bytes {
            if hits.is_empty() {
                return Err(IndexError::ResourceLimit {
                    needed,
                    limit: maximum_page_bytes,
                });
            }
            truncated = true;
            break;
        }
        retained_bytes = retained_bytes
            .checked_add(hit_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        hits.push(hit);
        statistics.returned_hit();
    }
    if let Some(index) = retained_segment {
        executions[index].values.release_decoded();
    }
    let next = (truncated || hits.len() == request.limit as usize)
        .then(|| hits.last().map(|hit| hit.cursor.clone()))
        .flatten();
    Ok(NativeQueryPage {
        hits,
        next,
        authorization_revision: request.authorization_revision,
        facet_results,
        aggregate_results,
        statistics: statistics.snapshot(),
    })
}
