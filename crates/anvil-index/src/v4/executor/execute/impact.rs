use std::collections::BinaryHeap;

use crate::IndexError;

use super::{SegmentExecution, Selected, Unranked};
use crate::v4::{ArtifactDirectoryRead, DocId, IndexSemantics, NativeQuery, NativeQueryRequest};

pub(super) fn threshold(request: &NativeQueryRequest, heap: &BinaryHeap<Selected>) -> Option<f32> {
    if !matches!(&request.query, NativeQuery::FullText { .. })
        || heap.len() < request.limit as usize
    {
        return None;
    }
    heap.peek().and_then(|selected| selected.score)
}

impl<'a, D: ArtifactDirectoryRead> SegmentExecution<'a, D> {
    pub(super) async fn next_competitive(
        &mut self,
        request: &NativeQueryRequest,
        global: &super::super::score::GlobalTextStatistics,
        threshold: Option<f32>,
    ) -> Result<Option<Unranked>, IndexError> {
        let Some(threshold) = threshold else {
            return self.next_unranked().await;
        };
        let (k1, b) = match &request.schema.semantics {
            IndexSemantics::FullText {
                bm25_k1, bm25_b, ..
            } => (*bm25_k1, *bm25_b),
            _ => {
                return Err(IndexError::InvalidQuery(
                    "full-text impact plan requires full-text semantics".into(),
                ));
            }
        };
        loop {
            let doc_id = if let Some(doc_id) = self.prefetched.take() {
                Some(doc_id)
            } else {
                self.cursor.next().await?
            };
            let Some(doc_id) = doc_id else {
                return Ok(None);
            };
            let window = self.scorer.impact_window(global, doc_id, k1, b).await?;
            // Equality cannot skip: the stable object-identity tie-break may
            // still place an equal-scoring candidate inside the retained K.
            if window.upper_bound < threshold {
                let span = window
                    .through
                    .get()
                    .checked_sub(doc_id.get())
                    .and_then(|value| value.checked_add(1))
                    .ok_or(IndexError::InvalidFormat(
                        "full-text impact window precedes its candidate",
                    ))?;
                // Reuse the established low-cardinality seek/span telemetry;
                // FullText top-K has no other cursor-seek path.
                self.statistics.cursor_seek(u64::from(span));
                let Some(next) = window.through.get().checked_add(1) else {
                    return Ok(None);
                };
                self.prefetched = self.cursor.advance(DocId::new(next)).await?;
                continue;
            }
            self.statistics.candidate_doc_id();
            if !self.values.is_live(doc_id).await? {
                self.statistics.live_mask_reject();
                continue;
            }
            if let Some(predicate) = self.exact_filter {
                self.statistics.two_phase_verification();
                if !self.values.predicate(predicate, doc_id).await? {
                    continue;
                }
            }
            if !self.scorer.phrase_matches(doc_id).await? {
                continue;
            }
            return Ok(Some(Unranked {
                doc_id,
                identity: self.values.identity(doc_id).await?,
            }));
        }
    }
}
