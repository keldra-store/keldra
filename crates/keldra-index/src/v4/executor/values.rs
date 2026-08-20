use std::collections::BTreeMap;

use crate::IndexError;

use super::super::{
    ArtifactDirectoryRead, DocId, DocValueBlock, DocValueCell, DocumentIdentity, FieldId,
    IdentityBlock, LiveMaskBlock, NativeQueryStatisticsRecorder, NormBlock, ScalarValue,
    SegmentComponentReader, SegmentDescriptor, SortValue, VectorBlock,
};

pub(super) struct SegmentValues<'a, D> {
    reader: SegmentComponentReader<'a, D>,
    identity: Option<IdentityBlock>,
    live: Option<LiveMaskBlock>,
    doc_values: BTreeMap<FieldId, DocValueBlock>,
    norms: BTreeMap<FieldId, NormBlock>,
    vectors: BTreeMap<FieldId, VectorBlock>,
    statistics: NativeQueryStatisticsRecorder,
}

impl<'a, D: ArtifactDirectoryRead> SegmentValues<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        segment: &'a SegmentDescriptor,
        statistics: NativeQueryStatisticsRecorder,
    ) -> Result<Self, IndexError> {
        Ok(Self {
            reader: SegmentComponentReader::new(directory, segment)?,
            identity: None,
            live: None,
            doc_values: BTreeMap::new(),
            norms: BTreeMap::new(),
            vectors: BTreeMap::new(),
            statistics,
        })
    }

    pub(super) async fn is_live(&mut self, doc_id: DocId) -> Result<bool, IndexError> {
        if self
            .live
            .as_ref()
            .and_then(|block| block.is_live(doc_id))
            .is_none()
        {
            let blocks = self
                .reader
                .live_mask_blocks(Some(doc_id.get()), Some(doc_id.get()))
                .await?;
            self.statistics.live_mask_blocks_decoded(
                u64::try_from(blocks.len()).map_err(|_| IndexError::OffsetOverflow)?,
            );
            self.live = Some(one(
                blocks,
                "live-mask DocId did not resolve to exactly one block",
            )?);
        }
        self.live
            .as_ref()
            .and_then(|block| block.is_live(doc_id))
            .ok_or(IndexError::InvalidFormat(
                "live-mask block does not cover requested DocId",
            ))
    }

    pub(super) async fn identity(&mut self, doc_id: DocId) -> Result<DocumentIdentity, IndexError> {
        if self
            .identity
            .as_ref()
            .and_then(|block| block.get(doc_id))
            .is_none()
        {
            self.identity = Some(one(
                self.reader
                    .identity_blocks(Some(doc_id.get()), Some(doc_id.get()))
                    .await?,
                "identity DocId did not resolve to exactly one block",
            )?);
        }
        self.identity
            .as_ref()
            .and_then(|block| block.get(doc_id))
            .cloned()
            .ok_or(IndexError::InvalidFormat(
                "identity block does not cover requested DocId",
            ))
    }

    pub(super) async fn doc_value(
        &mut self,
        field_id: FieldId,
        doc_id: DocId,
    ) -> Result<DocValueCell, IndexError> {
        if self
            .doc_values
            .get(&field_id)
            .and_then(|block| block.get(doc_id))
            .is_none()
        {
            let blocks = self
                .reader
                .doc_value_blocks(field_id, Some(doc_id.get()), Some(doc_id.get()))
                .await?;
            self.statistics.doc_value_blocks_decoded(
                u64::try_from(blocks.len()).map_err(|_| IndexError::OffsetOverflow)?,
            );
            let block = one(
                blocks,
                "doc-value DocId did not resolve to exactly one block",
            )?;
            if block.field_id != field_id {
                return Err(IndexError::InvalidFormat("doc-value field identity"));
            }
            self.doc_values.insert(field_id, block);
        }
        self.doc_values
            .get(&field_id)
            .and_then(|block| block.get(doc_id))
            .cloned()
            .ok_or(IndexError::InvalidFormat(
                "doc-value block does not cover requested DocId",
            ))
    }

    pub(super) async fn sort_value(
        &mut self,
        field_id: FieldId,
        doc_id: DocId,
    ) -> Result<SortValue, IndexError> {
        let cell = self.doc_value(field_id, doc_id).await?;
        if !cell.present {
            return Ok(SortValue::Missing);
        }
        if cell.null {
            return Ok(SortValue::Value(ScalarValue::Null));
        }
        if cell.values.len() != 1 {
            return Err(IndexError::InvalidFormat(
                "ordered single-valued field has multiple values",
            ));
        }
        Ok(SortValue::Value(cell.values[0].clone()))
    }

    pub(super) async fn norm(
        &mut self,
        field_id: FieldId,
        doc_id: DocId,
    ) -> Result<Option<u32>, IndexError> {
        if self
            .norms
            .get(&field_id)
            .is_none_or(|block| !norm_contains(block, doc_id))
        {
            let blocks = self
                .reader
                .norm_blocks(field_id, Some(doc_id.get()), Some(doc_id.get()))
                .await?;
            if blocks.is_empty() {
                return Ok(None);
            }
            let block = one(blocks, "norm DocId resolved to multiple blocks")?;
            self.norms.insert(field_id, block);
        }
        Ok(self
            .norms
            .get(&field_id)
            .and_then(|block| block.get(doc_id)))
    }

    pub(super) async fn vector(
        &mut self,
        field_id: FieldId,
        doc_id: DocId,
    ) -> Result<Option<Vec<f32>>, IndexError> {
        if self
            .vectors
            .get(&field_id)
            .and_then(|block| block.get(doc_id))
            .is_none()
        {
            let blocks = self
                .reader
                .vector_blocks(field_id, Some(doc_id.get()), Some(doc_id.get()))
                .await?;
            if blocks.is_empty() {
                return Ok(None);
            }
            let block = one(blocks, "vector DocId resolved to multiple blocks")?;
            if block.field_id != field_id {
                return Err(IndexError::InvalidFormat("vector field identity"));
            }
            self.vectors.insert(field_id, block);
        }
        Ok(self
            .vectors
            .get(&field_id)
            .and_then(|block| block.get(doc_id))
            .map(<[f32]>::to_vec))
    }

    /// Release disposable decoded blocks without changing the segment or
    /// cursor authority. The next lookup reopens the exact immutable block.
    pub(super) fn release_decoded(&mut self) {
        self.identity = None;
        self.live = None;
        self.doc_values.clear();
        self.norms.clear();
        self.vectors.clear();
    }
}

fn norm_contains(block: &NormBlock, doc_id: DocId) -> bool {
    let offset = doc_id.get().checked_sub(block.first_doc_id.get());
    offset.is_some_and(|offset| offset < block.values().len() as u32)
}

fn one<T>(mut values: Vec<T>, message: &'static str) -> Result<T, IndexError> {
    if values.len() != 1 {
        return Err(IndexError::InvalidFormat(message));
    }
    Ok(values.pop().unwrap())
}
