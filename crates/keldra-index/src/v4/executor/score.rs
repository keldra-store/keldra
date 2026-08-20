use std::collections::BTreeMap;
use std::sync::Arc;

use crate::IndexError;

use super::super::{
    ArtifactDirectoryRead, DocId, FieldId, IndexSemantics, NativeQuery,
    NativeQueryStatisticsRecorder, Schema, SegmentDescriptor, SegmentStatistics, VectorMetric,
};
use super::plan::TextTermPlan;
use super::posting::PostingStream;
use super::values::SegmentValues;

#[derive(Clone, Default)]
pub(super) struct GlobalTextStatistics {
    documents: u64,
    fields: BTreeMap<FieldId, GlobalFieldStatistics>,
    document_frequencies: BTreeMap<(FieldId, u32), u64>,
}

#[derive(Clone, Default)]
struct GlobalFieldStatistics {
    documents: u64,
    total_length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct ScoreImpactWindow {
    pub(super) through: DocId,
    pub(super) upper_bound: f32,
}

impl GlobalTextStatistics {
    pub(super) fn add_segment(
        &mut self,
        statistics: &SegmentStatistics,
        terms: &[TextTermPlan],
    ) -> Result<(), IndexError> {
        self.documents = self
            .documents
            .checked_add(statistics.document_count)
            .ok_or(IndexError::OffsetOverflow)?;
        for field in &statistics.fields {
            let total = self.fields.entry(field.field_id).or_default();
            total.documents = total
                .documents
                .checked_add(field.present_documents)
                .ok_or(IndexError::OffsetOverflow)?;
            total.total_length = total
                .total_length
                .checked_add(field.total_field_length)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        for term in terms {
            let frequency = self
                .document_frequencies
                .entry((term.field_id, term.token_ordinal))
                .or_default();
            *frequency = frequency
                .checked_add(term.postings.document_frequency)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(())
    }
}

pub(super) struct SegmentScorer<'a, D> {
    directory: &'a D,
    document_count: u32,
    terms: Vec<ScoringCursor<'a, D>>,
}

struct ScoringCursor<'a, D> {
    field_id: FieldId,
    token_ordinal: u32,
    cursor: PostingStream<'a, D>,
}

impl<'a, D: ArtifactDirectoryRead> SegmentScorer<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        terms: Vec<TextTermPlan>,
        statistics: &NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        let terms = terms
            .into_iter()
            .map(|term| {
                Ok(ScoringCursor {
                    field_id: term.field_id,
                    token_ordinal: term.token_ordinal,
                    cursor: PostingStream::new(
                        directory,
                        segment,
                        term.field_id,
                        term.postings,
                        statistics.clone(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>, IndexError>>()?;
        Ok(Self {
            directory,
            document_count: segment.document_count,
            terms,
        })
    }

    pub(super) async fn score(
        &mut self,
        schema: &Schema,
        query: &NativeQuery,
        scoring_query_vector: Option<&Arc<[f32]>>,
        global: &GlobalTextStatistics,
        values: &mut SegmentValues<'a, D>,
        doc_id: DocId,
    ) -> Result<Option<f32>, IndexError> {
        match query {
            NativeQuery::FullText { .. } => {
                let (k1, b) = match &schema.semantics {
                    IndexSemantics::FullText {
                        bm25_k1, bm25_b, ..
                    } => (*bm25_k1, *bm25_b),
                    _ => {
                        return Err(IndexError::InvalidQuery(
                            "full-text schema semantics".into(),
                        ));
                    }
                };
                Ok(Some(self.bm25(global, values, doc_id, k1, b).await?))
            }
            NativeQuery::Vector { .. } => {
                let (field, metric) = vector_field_and_metric(schema)?;
                let Some(stored) = values.vector(field, doc_id).await? else {
                    return Ok(None);
                };
                let query = Arc::clone(scoring_query_vector.ok_or_else(|| {
                    IndexError::InvalidQuery("vector query has no values".into())
                })?);
                Ok(Some(
                    self.directory
                        .run_query_cpu(move || vector_score(metric, &query, &stored))
                        .await?,
                ))
            }
            NativeQuery::Hybrid {
                text,
                vector: query_vector,
            } => {
                let IndexSemantics::Hybrid {
                    bm25_k1,
                    bm25_b,
                    metric,
                    lexical_weight,
                    vector_weight,
                    ..
                } = &schema.semantics
                else {
                    return Err(IndexError::InvalidQuery("hybrid schema semantics".into()));
                };
                let lexical = if text.trim().is_empty() {
                    0.0
                } else {
                    self.bm25(global, values, doc_id, *bm25_k1, *bm25_b).await? as f64
                };
                let vector = if query_vector.is_empty() {
                    0.0
                } else {
                    let field = vector_field(schema)?;
                    let Some(stored) = values.vector(field, doc_id).await? else {
                        return Ok(None);
                    };
                    let query = Arc::clone(scoring_query_vector.ok_or_else(|| {
                        IndexError::InvalidQuery("hybrid query has no vector values".into())
                    })?);
                    let metric = *metric;
                    f64::from(
                        self.directory
                            .run_query_cpu(move || vector_score(metric, &query, &stored))
                            .await?,
                    )
                };
                let score = lexical_weight.mul_add(lexical, vector_weight * vector);
                if !score.is_finite() {
                    return Err(IndexError::InvalidFormat("hybrid score is not finite"));
                }
                Ok(Some(score as f32))
            }
            _ => Ok(None),
        }
    }

    pub(super) async fn impact_window(
        &mut self,
        global: &GlobalTextStatistics,
        target: DocId,
        k1: f64,
        b: f64,
    ) -> Result<ScoreImpactWindow, IndexError> {
        let mut through =
            self.document_count
                .checked_sub(1)
                .map(DocId::new)
                .ok_or(IndexError::InvalidFormat(
                    "full-text candidate belongs to an empty segment",
                ))?;
        let mut upper_bound = 0.0f64;
        for term in &mut self.terms {
            let window = term.cursor.impact_window(target).await?;
            let Some((impact, block_end)) = window else {
                continue;
            };
            through = through.min(block_end);
            upper_bound += bm25_term_score(
                global,
                term.field_id,
                term.token_ordinal,
                f64::from(impact.maximum_frequency),
                // Exact scoring treats an absent norm as length one. Keep the
                // bound conservative even for such a malformed projection;
                // persisted zero-length minima remain more conservative.
                f64::from(impact.minimum_field_length.min(1)),
                k1,
                b,
            )?;
        }
        let upper_bound = round_score_up(upper_bound)?;
        Ok(ScoreImpactWindow {
            through,
            upper_bound,
        })
    }

    async fn bm25(
        &mut self,
        global: &GlobalTextStatistics,
        values: &mut SegmentValues<'a, D>,
        doc_id: DocId,
        k1: f64,
        b: f64,
    ) -> Result<f32, IndexError> {
        let mut score = 0.0f64;
        for term in &mut self.terms {
            if term.cursor.advance(doc_id).await? != Some(doc_id) {
                continue;
            }
            let tf = f64::from(term.cursor.current_frequency().unwrap_or(1));
            let length = f64::from(values.norm(term.field_id, doc_id).await?.unwrap_or(1));
            score += bm25_term_score(global, term.field_id, term.token_ordinal, tf, length, k1, b)?;
        }
        if !score.is_finite() {
            return Err(IndexError::InvalidFormat("BM25 score is not finite"));
        }
        Ok(score as f32)
    }

    pub(super) fn release_decoded(&mut self) -> Result<(), IndexError> {
        for term in &mut self.terms {
            term.cursor.release_decoded()?;
        }
        Ok(())
    }
}

fn bm25_term_score(
    global: &GlobalTextStatistics,
    field_id: FieldId,
    token_ordinal: u32,
    tf: f64,
    length: f64,
    k1: f64,
    b: f64,
) -> Result<f64, IndexError> {
    let field = global.fields.get(&field_id).cloned().unwrap_or_default();
    let df = *global
        .document_frequencies
        .get(&(field_id, token_ordinal))
        .unwrap_or(&1) as f64;
    let documents = global.documents.max(1) as f64;
    let average = if field.documents == 0 {
        1.0
    } else {
        (field.total_length as f64 / field.documents as f64).max(1.0)
    };
    let idf = (1.0 + (documents - df + 0.5).max(0.0) / (df + 0.5)).ln();
    let denominator = tf + k1 * (1.0 - b + b * length / average);
    let score = idf * tf * (k1 + 1.0) / denominator.max(f64::MIN_POSITIVE);
    if !score.is_finite() || score < 0.0 {
        return Err(IndexError::InvalidFormat("BM25 score is not finite"));
    }
    Ok(score)
}

fn round_score_up(value: f64) -> Result<f32, IndexError> {
    if !value.is_finite() || value < 0.0 || value > f64::from(f32::MAX) {
        return Err(IndexError::InvalidFormat("BM25 impact bound is not finite"));
    }
    let rounded = value as f32;
    if f64::from(rounded) < value {
        Ok(f32::from_bits(rounded.to_bits().checked_add(1).ok_or(
            IndexError::InvalidFormat("BM25 impact bound overflow"),
        )?))
    } else {
        Ok(rounded)
    }
}

fn vector_field(schema: &Schema) -> Result<FieldId, IndexError> {
    let mut fields = schema.fields.iter().filter(|field| {
        field
            .components
            .contains(super::super::FieldComponents::VECTOR)
    });
    let field = fields
        .next()
        .ok_or_else(|| IndexError::InvalidDefinition("vector schema has no vector field".into()))?;
    if fields.next().is_some() {
        return Err(IndexError::InvalidDefinition(
            "vector schema has multiple vector fields".into(),
        ));
    }
    Ok(field.id)
}

fn vector_field_and_metric(schema: &Schema) -> Result<(FieldId, VectorMetric), IndexError> {
    let metric = match &schema.semantics {
        IndexSemantics::Vector { metric, .. } => *metric,
        _ => return Err(IndexError::InvalidQuery("vector schema semantics".into())),
    };
    Ok((vector_field(schema)?, metric))
}

fn vector_score(metric: VectorMetric, query: &[f32], stored: &[f32]) -> Result<f32, IndexError> {
    if query.len() != stored.len() {
        return Err(IndexError::InvalidFormat("stored vector dimensions"));
    }
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    let mut distance = 0.0f64;
    for (left, right) in query.iter().zip(stored) {
        let (left, right) = (f64::from(*left), f64::from(*right));
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
        let delta = left - right;
        distance += delta * delta;
    }
    let score = match metric {
        VectorMetric::DotProduct => dot,
        VectorMetric::Cosine if left_norm == 0.0 || right_norm == 0.0 => 0.0,
        VectorMetric::Cosine => dot / (left_norm.sqrt() * right_norm.sqrt()),
        VectorMetric::Euclidean => -distance.sqrt(),
    };
    if !score.is_finite() || score < f32::MIN as f64 || score > f32::MAX as f64 {
        return Err(IndexError::InvalidFormat("vector score is not finite"));
    }
    Ok(score as f32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statistics() -> GlobalTextStatistics {
        let field_id = FieldId::new(0);
        GlobalTextStatistics {
            documents: 1_000,
            fields: BTreeMap::from([(
                field_id,
                GlobalFieldStatistics {
                    documents: 1_000,
                    total_length: 100_000,
                },
            )]),
            document_frequencies: BTreeMap::from([((field_id, 0), 10)]),
        }
    }

    #[test]
    fn block_inputs_conservatively_bound_bm25_scores() {
        let statistics = statistics();
        for (k1, b) in [(0.0, 0.0), (1.2, 0.75), (2.0, 1.0)] {
            let bound = bm25_term_score(&statistics, FieldId::new(0), 0, 8.0, 2.0, k1, b).unwrap();
            for frequency in 1..=8 {
                for length in [2, 10, 100, 1_000] {
                    let exact = bm25_term_score(
                        &statistics,
                        FieldId::new(0),
                        0,
                        f64::from(frequency),
                        f64::from(length),
                        k1,
                        b,
                    )
                    .unwrap();
                    assert!(exact <= bound);
                }
            }
        }
    }

    #[test]
    fn impact_bound_rounds_toward_positive_infinity() {
        let exact = f64::from(1.0f32) + f64::from(f32::EPSILON) / 4.0;
        let rounded = round_score_up(exact).unwrap();
        assert!(f64::from(rounded) >= exact);
        assert_eq!(rounded.to_bits(), 1.0f32.to_bits() + 1);
        assert_eq!(round_score_up(1.0).unwrap(), 1.0);
    }
}
