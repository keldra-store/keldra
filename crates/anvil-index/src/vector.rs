//! Minimal exact vector index.
//!
//! The first implementation deliberately uses a page-streamed exact scan. It
//! is correct and usable for small indexes; an HNSW layout can replace this
//! engine without changing placement, generation, cache, or file APIs.

use std::cmp::Ordering;

use crate::{
    DocumentRef, IndexArtifacts, IndexDirectoryRead, IndexError, PagedMap, PagedMapBuilder,
};

pub const VECTOR_FILE: &str = "vector/vectors.map";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VectorDefinition {
    pub dimension: usize,
    pub metric: VectorMetric,
}

impl VectorDefinition {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.dimension == 0 || self.dimension > u32::MAX as usize {
            return Err(IndexError::InvalidDefinition(
                "vector dimension must be between 1 and u32::MAX".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorDocument {
    pub document: DocumentRef,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorHit {
    pub document: DocumentRef,
    pub score: f32,
}

pub struct VectorEngine;

impl VectorEngine {
    pub fn build(
        definition: &VectorDefinition,
        documents: impl IntoIterator<Item = VectorDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        definition.validate()?;
        let mut map = PagedMapBuilder::default();
        for document in documents {
            validate_vector(&document.values, definition.dimension)?;
            map.insert(
                document.document.path.as_bytes().to_vec(),
                encode_vector(document.document.version, &document.values)?,
            )?;
        }
        let mut artifacts = IndexArtifacts::default();
        artifacts.insert(VECTOR_FILE, map.finish()?)?;
        Ok(artifacts)
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        definition: &VectorDefinition,
        query: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorHit>, IndexError> {
        definition.validate()?;
        validate_vector(query, definition.dimension)
            .map_err(|_| IndexError::InvalidQuery("query vector has the wrong dimension".into()))?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let map = PagedMap::open(directory.open_file(VECTOR_FILE).await?).await?;
        let mut hits = Vec::new();
        for page_index in 0..map.page_count() {
            for (path, encoded) in map.page(page_index).await? {
                let (version, vector) = decode_vector(&encoded, definition.dimension)?;
                let score = similarity(query, &vector, definition.metric);
                hits.push(VectorHit {
                    document: DocumentRef {
                        path: String::from_utf8(path)
                            .map_err(|_| IndexError::InvalidFormat("vector path UTF-8"))?,
                        version,
                    },
                    score,
                });
            }
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.document.cmp(&right.document))
        });
        hits.truncate(limit);
        Ok(hits)
    }
}

fn validate_vector(values: &[f32], dimension: usize) -> Result<(), IndexError> {
    if values.len() != dimension || values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidDefinition(
            "vector values must be finite and match the configured dimension".into(),
        ));
    }
    Ok(())
}

fn encode_vector(version: u64, values: &[f32]) -> Result<Vec<u8>, IndexError> {
    let mut encoded = Vec::with_capacity(12 + values.len() * 4);
    encoded.extend_from_slice(&version.to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(values.len())
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_le_bytes(),
    );
    for value in values {
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

fn decode_vector(bytes: &[u8], expected_dimension: usize) -> Result<(u64, Vec<f32>), IndexError> {
    if bytes.len() < 12 {
        return Err(IndexError::InvalidFormat("truncated vector record"));
    }
    let version = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let dimension = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if dimension != expected_dimension || bytes.len() != 12 + dimension * 4 {
        return Err(IndexError::InvalidFormat("vector record dimension"));
    }
    let values = bytes[12..]
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    if values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidFormat("non-finite vector value"));
    }
    Ok((version, values))
}

fn similarity(left: &[f32], right: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::DotProduct => left
            .iter()
            .zip(right)
            .map(|(left, right)| left * right)
            .sum(),
        VectorMetric::Euclidean => -left
            .iter()
            .zip(right)
            .map(|(left, right)| (left - right).powi(2))
            .sum::<f32>()
            .sqrt(),
        VectorMetric::Cosine => {
            let dot = left
                .iter()
                .zip(right)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
            let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();
            if left_norm == 0.0 || right_norm == 0.0 {
                0.0
            } else {
                dot / (left_norm * right_norm)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryDirectory;

    use super::*;

    #[tokio::test]
    async fn exact_vector_scan_returns_nearest_documents() {
        let definition = VectorDefinition {
            dimension: 3,
            metric: VectorMetric::Cosine,
        };
        let artifacts = VectorEngine::build(
            &definition,
            [
                VectorDocument {
                    document: DocumentRef {
                        path: "/x".into(),
                        version: 1,
                    },
                    values: vec![1.0, 0.0, 0.0],
                },
                VectorDocument {
                    document: DocumentRef {
                        path: "/y".into(),
                        version: 2,
                    },
                    values: vec![0.0, 1.0, 0.0],
                },
            ],
        )
        .unwrap();
        let directory =
            MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)));
        let hits = VectorEngine::query(&directory, &definition, &[0.9, 0.1, 0.0], 1)
            .await
            .unwrap();
        assert_eq!(hits[0].document.path, "/x");
    }

    #[test]
    fn vectors_reject_non_finite_values_and_wrong_dimensions() {
        let definition = VectorDefinition {
            dimension: 2,
            metric: VectorMetric::DotProduct,
        };
        assert!(
            VectorEngine::build(
                &definition,
                [VectorDocument {
                    document: DocumentRef {
                        path: "/bad".into(),
                        version: 1
                    },
                    values: vec![f32::NAN, 1.0],
                }]
            )
            .is_err()
        );
    }
}
