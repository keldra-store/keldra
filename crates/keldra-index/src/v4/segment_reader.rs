use crate::IndexError;

use super::{
    ArtifactDirectoryRead, ComponentKind, ComponentStream, DocValueBlock, FieldId, IdentityBlock,
    LiveMaskBlock, NormBlock, PointBlock, PositionsBlock, PostingBlock, SegmentDescriptor,
    SegmentStatistics, TermDictionary, VectorBlock, component_ordinal_key, read_artifact_component,
};

/// Lazy checked access to the component streams of one immutable segment.
pub struct SegmentComponentReader<'a, D> {
    directory: &'a D,
    segment: &'a SegmentDescriptor,
}

impl<'a, D: ArtifactDirectoryRead> SegmentComponentReader<'a, D> {
    pub fn new(directory: &'a D, segment: &'a SegmentDescriptor) -> Result<Self, IndexError> {
        segment.validate()?;
        Ok(Self { directory, segment })
    }

    pub fn descriptor(&self) -> &SegmentDescriptor {
        self.segment
    }

    pub async fn identity_blocks(
        &self,
        first: Option<u32>,
        last: Option<u32>,
    ) -> Result<Vec<IdentityBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::IDENTITY_TABLE,
            None,
            doc_bound(first),
            doc_bound(last),
            IdentityBlock::decode_payload,
        )
        .await
    }

    pub async fn live_mask_blocks(
        &self,
        first: Option<u32>,
        last: Option<u32>,
    ) -> Result<Vec<LiveMaskBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::LIVE_MASK,
            None,
            doc_bound(first),
            doc_bound(last),
            LiveMaskBlock::decode_payload,
        )
        .await
    }

    pub async fn term_dictionaries(
        &self,
        field_id: FieldId,
        minimum: Option<Vec<u8>>,
        maximum: Option<Vec<u8>>,
    ) -> Result<Vec<TermDictionary>, IndexError> {
        self.decode_stream(
            ComponentKind::TERM_DICTIONARY,
            Some(field_id),
            minimum,
            maximum,
            TermDictionary::decode_payload,
        )
        .await
    }

    pub async fn posting_blocks(
        &self,
        field_id: FieldId,
        first_ordinal: u32,
        component_count: u32,
    ) -> Result<Vec<PostingBlock>, IndexError> {
        if component_count == 0 {
            return Err(IndexError::InvalidQuery(
                "posting reference contains no components".into(),
            ));
        }
        let last = first_ordinal
            .checked_add(component_count - 1)
            .ok_or(IndexError::OffsetOverflow)?;
        let blocks = self
            .decode_stream(
                ComponentKind::POSTINGS,
                Some(field_id),
                Some(component_ordinal_key(first_ordinal).to_vec()),
                Some(component_ordinal_key(last).to_vec()),
                PostingBlock::decode_payload,
            )
            .await?;
        if blocks.len() != component_count as usize {
            return Err(IndexError::InvalidFormat(
                "posting reference does not resolve to its declared component count",
            ));
        }
        Ok(blocks)
    }

    pub async fn position_blocks(
        &self,
        field_id: FieldId,
        first_ordinal: u32,
        component_count: u32,
    ) -> Result<Vec<PositionsBlock>, IndexError> {
        if component_count == 0 {
            return Ok(Vec::new());
        }
        let Some(_) = self.component(ComponentKind::POSITIONS, Some(field_id)) else {
            return Ok(Vec::new());
        };
        let last = first_ordinal
            .checked_add(component_count - 1)
            .ok_or(IndexError::OffsetOverflow)?;
        self.decode_stream(
            ComponentKind::POSITIONS,
            Some(field_id),
            Some(component_ordinal_key(first_ordinal).to_vec()),
            Some(component_ordinal_key(last).to_vec()),
            PositionsBlock::decode_payload,
        )
        .await
    }

    pub async fn doc_value_blocks(
        &self,
        field_id: FieldId,
        first: Option<u32>,
        last: Option<u32>,
    ) -> Result<Vec<DocValueBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::DOC_VALUES,
            Some(field_id),
            doc_bound(first),
            doc_bound(last),
            DocValueBlock::decode_payload,
        )
        .await
    }

    pub async fn point_blocks(
        &self,
        field_id: FieldId,
        minimum: Option<Vec<u8>>,
        maximum: Option<Vec<u8>>,
    ) -> Result<Vec<PointBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::POINTS,
            Some(field_id),
            minimum,
            maximum,
            PointBlock::decode_payload,
        )
        .await
    }

    pub async fn norm_blocks(
        &self,
        field_id: FieldId,
        first: Option<u32>,
        last: Option<u32>,
    ) -> Result<Vec<NormBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::NORMS,
            Some(field_id),
            doc_bound(first),
            doc_bound(last),
            NormBlock::decode_payload,
        )
        .await
    }

    pub async fn vector_blocks(
        &self,
        field_id: FieldId,
        first: Option<u32>,
        last: Option<u32>,
    ) -> Result<Vec<VectorBlock>, IndexError> {
        self.decode_stream(
            ComponentKind::VECTORS,
            Some(field_id),
            doc_bound(first),
            doc_bound(last),
            VectorBlock::decode_payload,
        )
        .await
    }

    pub async fn statistics(&self) -> Result<SegmentStatistics, IndexError> {
        let mut blocks = self
            .decode_stream(
                ComponentKind::SCORING_STATISTICS,
                None,
                None,
                None,
                SegmentStatistics::decode_payload,
            )
            .await?;
        if blocks.len() != 1 {
            return Err(IndexError::InvalidFormat(
                "segment statistics stream must contain exactly one component",
            ));
        }
        let statistics = blocks.remove(0);
        if statistics.document_count != u64::from(self.segment.document_count) {
            return Err(IndexError::InvalidFormat(
                "segment statistics document count differs from its descriptor",
            ));
        }
        Ok(statistics)
    }

    async fn decode_stream<T: Send + 'static>(
        &self,
        role: ComponentKind,
        field_id: Option<FieldId>,
        minimum: Option<Vec<u8>>,
        maximum: Option<Vec<u8>>,
        decode: fn(&[u8]) -> Result<T, IndexError>,
    ) -> Result<Vec<T>, IndexError> {
        let component = self
            .component(role, field_id)
            .ok_or(IndexError::InvalidFormat(
                "format-v4 segment lacks a required component stream",
            ))?;
        let mut stream = ComponentStream::new(
            self.directory,
            self.segment.identity,
            &self.segment.packs,
            role,
            component.artifact.clone(),
            minimum,
            maximum,
        )?;
        let mut values = Vec::new();
        while let Some(leaf) = stream.next_leaf().await? {
            let component = read_artifact_component(
                self.directory,
                self.segment.identity,
                &self.segment.packs,
                &leaf.descriptor,
                role,
            )
            .await?;
            values.push(
                self.directory
                    .run_query_cpu(move || decode(&component.payload))
                    .await?,
            );
        }
        Ok(values)
    }

    fn component(
        &self,
        role: ComponentKind,
        field_id: Option<FieldId>,
    ) -> Option<&super::SegmentComponent> {
        self.segment
            .components
            .binary_search_by_key(&(role, field_id, 0), |component| {
                (component.role, component.field_id, component.ordinal)
            })
            .ok()
            .map(|index| &self.segment.components[index])
    }
}

fn doc_bound(value: Option<u32>) -> Option<Vec<u8>> {
    value.map(|value| value.to_be_bytes().to_vec())
}
