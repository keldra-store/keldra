//! Hybrid full-text and vector scoring over one immutable generation.

use std::collections::BTreeMap;

use crate::full_text::{FullTextDocument, FullTextEngine, FullTextQuery};
use crate::vector::{VectorDefinition, VectorDocument, VectorEngine};
use crate::{DocumentRef, IndexArtifacts, IndexDirectoryRead, IndexError};

#[derive(Clone, Debug, PartialEq)]
pub struct HybridDocument {
    pub document: DocumentRef,
    pub text_fields: BTreeMap<String, String>,
    pub vector: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridDefinition {
    pub vector: VectorDefinition,
    pub text_weight: f32,
    pub vector_weight: f32,
}

impl HybridDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.vector.validate()?;
        if !self.text_weight.is_finite()
            || !self.vector_weight.is_finite()
            || self.text_weight < 0.0
            || self.vector_weight < 0.0
            || self.text_weight + self.vector_weight <= 0.0
        {
            return Err(IndexError::InvalidDefinition(
                "hybrid weights must be finite, non-negative and not both zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridQuery<'a> {
    pub text: &'a str,
    pub vector: &'a [f32],
    pub fields: &'a [String],
    pub phrase: bool,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HybridHit {
    pub document: DocumentRef,
    pub score: f32,
    pub text_score: Option<f32>,
    pub vector_score: Option<f32>,
}

pub struct HybridEngine;

impl HybridEngine {
    pub fn build(
        definition: &HybridDefinition,
        documents: impl IntoIterator<Item = HybridDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        definition.validate()?;
        let documents = documents.into_iter().collect::<Vec<_>>();
        let text = FullTextEngine::build(documents.iter().map(|document| FullTextDocument {
            document: document.document.clone(),
            fields: document.text_fields.clone(),
        }))?;
        let vectors = VectorEngine::build(
            &definition.vector,
            documents.iter().map(|document| VectorDocument {
                document: document.document.clone(),
                values: document.vector.clone(),
            }),
        )?;
        let mut artifacts = IndexArtifacts::default();
        for file in text.into_files().chain(vectors.into_files()) {
            artifacts.insert(file.name, file.bytes)?;
        }
        Ok(artifacts)
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        definition: &HybridDefinition,
        query: HybridQuery<'_>,
    ) -> Result<Vec<HybridHit>, IndexError> {
        definition.validate()?;
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        if query.text.trim().is_empty() && query.vector.is_empty() {
            return Err(IndexError::InvalidQuery(
                "hybrid query needs text, a vector, or both".into(),
            ));
        }
        // The minimum engine evaluates every candidate before fusion. A later
        // threshold algorithm may make this faster, but a fixed per-side cap
        // could silently omit the true combined top-k.
        let candidate_limit = usize::MAX;
        let text_hits = if query.text.trim().is_empty() {
            Vec::new()
        } else {
            FullTextEngine::query(
                directory,
                FullTextQuery {
                    text: query.text,
                    fields: query.fields,
                    phrase: query.phrase,
                    match_all_terms: false,
                    limit: candidate_limit,
                },
            )
            .await?
        };
        let vector_hits = if query.vector.is_empty() {
            Vec::new()
        } else {
            VectorEngine::query(directory, &definition.vector, query.vector, candidate_limit)
                .await?
        };
        let maximum_text = text_hits.iter().map(|hit| hit.score).fold(0.0f32, f32::max);
        let vector_range = vector_hits.iter().fold(None, |range, hit| match range {
            None => Some((hit.score, hit.score)),
            Some((minimum, maximum)) => Some((minimum.min(hit.score), maximum.max(hit.score))),
        });
        let mut combined = BTreeMap::<DocumentRef, HybridHit>::new();
        for hit in text_hits {
            let normalized = if maximum_text > 0.0 {
                hit.score / maximum_text
            } else {
                0.0
            };
            combined.insert(
                hit.document.clone(),
                HybridHit {
                    document: hit.document,
                    score: normalized * definition.text_weight,
                    text_score: Some(normalized),
                    vector_score: None,
                },
            );
        }
        for hit in vector_hits {
            let normalized = normalize_vector_score(hit.score, vector_range);
            let combined_hit = combined.entry(hit.document.clone()).or_insert(HybridHit {
                document: hit.document,
                score: 0.0,
                text_score: None,
                vector_score: None,
            });
            combined_hit.vector_score = Some(normalized);
            combined_hit.score += normalized * definition.vector_weight;
        }
        let denominator = definition.text_weight + definition.vector_weight;
        let mut hits = combined.into_values().collect::<Vec<_>>();
        for hit in &mut hits {
            hit.score /= denominator;
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.document.cmp(&right.document))
        });
        hits.truncate(query.limit);
        Ok(hits)
    }
}

fn normalize_vector_score(score: f32, range: Option<(f32, f32)>) -> f32 {
    match range {
        Some((minimum, maximum)) if maximum > minimum => (score - minimum) / (maximum - minimum),
        Some(_) => 1.0,
        None => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryDirectory;
    use crate::vector::VectorMetric;

    use super::*;

    #[tokio::test]
    async fn hybrid_generation_fuses_text_and_vector_results() {
        let definition = HybridDefinition {
            vector: VectorDefinition {
                dimension: 2,
                metric: VectorMetric::Cosine,
            },
            text_weight: 0.6,
            vector_weight: 0.4,
        };
        let artifacts = HybridEngine::build(
            &definition,
            [
                HybridDocument {
                    document: DocumentRef {
                        path: "/strong".into(),
                        version: 1,
                    },
                    text_fields: BTreeMap::from([("body".into(), "rust storage engine".into())]),
                    vector: vec![1.0, 0.0],
                },
                HybridDocument {
                    document: DocumentRef {
                        path: "/weak".into(),
                        version: 2,
                    },
                    text_fields: BTreeMap::from([("body".into(), "rust application".into())]),
                    vector: vec![0.0, 1.0],
                },
            ],
        )
        .unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));
        let hits = HybridEngine::query(
            &directory,
            &definition,
            HybridQuery {
                text: "rust storage",
                vector: &[1.0, 0.0],
                fields: &[],
                phrase: false,
                limit: 2,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits[0].document.path, "/strong");
        assert!(hits[0].text_score.is_some() && hits[0].vector_score.is_some());
    }

    #[tokio::test]
    async fn valid_query_against_empty_generation_returns_no_hits() {
        let definition = HybridDefinition {
            vector: VectorDefinition {
                dimension: 2,
                metric: VectorMetric::Cosine,
            },
            text_weight: 0.6,
            vector_weight: 0.4,
        };
        let artifacts = HybridEngine::build(&definition, Vec::<HybridDocument>::new()).unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));

        let hits = HybridEngine::query(
            &directory,
            &definition,
            HybridQuery {
                text: "rust search",
                vector: &[1.0, 0.0],
                fields: &[],
                phrase: false,
                limit: 10,
            },
        )
        .await
        .unwrap();

        assert!(hits.is_empty());
    }
}
