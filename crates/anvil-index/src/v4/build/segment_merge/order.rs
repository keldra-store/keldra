use std::cmp::Ordering;

use crate::IndexError;

use super::super::super::{
    ArtifactDirectoryRead, Cardinality, ComponentKind, DocValueBlock, DocValueCell,
    DocumentIdentity, IdentityBlock, LiveMaskBlock, ScalarValue, Schema, SegmentDescriptor,
    SortValue, encode_physical_order_key,
};
use super::io::{RoutedBlockStream, required_stream};

#[derive(Clone, Debug)]
pub(super) struct OrderedLiveDocument {
    pub(super) old_doc_id: u32,
    pub(super) identity: DocumentIdentity,
    order_key: Vec<u8>,
}

impl OrderedLiveDocument {
    pub(super) fn order_key(&self) -> &[u8] {
        &self.order_key
    }
}

pub(super) struct OrderedInput<'a, D> {
    descriptor: &'a SegmentDescriptor,
    identity: IdentityCursor<'a, D>,
    live: LiveCursor<'a, D>,
    order_columns: Vec<(super::super::super::OrderDirection, ColumnCursor<'a, D>)>,
    next_old_doc_id: u32,
    current: Option<OrderedLiveDocument>,
    ended: bool,
}

impl<'a, D: ArtifactDirectoryRead> OrderedInput<'a, D> {
    pub(super) fn new(
        directory: &'a D,
        descriptor: &'a SegmentDescriptor,
        schema: &Schema,
    ) -> Result<Self, IndexError> {
        let identity = IdentityCursor::new(required_stream(
            directory,
            descriptor,
            ComponentKind::IDENTITY_TABLE,
            None,
            None,
        )?);
        let live = LiveCursor::new(required_stream(
            directory,
            descriptor,
            ComponentKind::LIVE_MASK,
            None,
            None,
        )?);
        let mut order_columns = Vec::with_capacity(schema.physical_order.len());
        for order in &schema.physical_order {
            let field = schema
                .fields
                .get(order.field_id.get() as usize)
                .ok_or(IndexError::InvalidFormat("physical-order field"))?;
            order_columns.push((
                order.direction,
                ColumnCursor::new(
                    required_stream(
                        directory,
                        descriptor,
                        ComponentKind::DOC_VALUES,
                        Some(order.field_id),
                        None,
                    )?,
                    order.field_id,
                    field.cardinality == Cardinality::Multi,
                ),
            ));
        }
        Ok(Self {
            descriptor,
            identity,
            live,
            order_columns,
            next_old_doc_id: 0,
            current: None,
            ended: false,
        })
    }

    pub(super) fn current(&self) -> Option<&OrderedLiveDocument> {
        self.current.as_ref()
    }

    pub(super) async fn advance(&mut self) -> Result<(), IndexError> {
        self.current = None;
        while self.next_old_doc_id < self.descriptor.document_count {
            let old_doc_id = self.next_old_doc_id;
            let identity = self.identity.next(old_doc_id).await?;
            let live = self.live.next(old_doc_id).await?;
            let mut order_values = Vec::with_capacity(self.order_columns.len());
            for (direction, column) in &mut self.order_columns {
                let cell = column.next(old_doc_id).await?;
                order_values.push((order_value(&cell)?, *direction));
            }
            self.next_old_doc_id = self
                .next_old_doc_id
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            if !live {
                continue;
            }
            let order_key = if order_values.is_empty() {
                identity.result_or_source().path.as_bytes().to_vec()
            } else {
                encode_physical_order_key(&order_values)?
            };
            self.current = Some(OrderedLiveDocument {
                old_doc_id,
                identity,
                order_key,
            });
            return Ok(());
        }
        if !self.ended {
            self.identity.finish().await?;
            self.live.finish().await?;
            for (_, column) in &mut self.order_columns {
                column.finish().await?;
            }
            self.ended = true;
        }
        Ok(())
    }
}

pub(super) fn compare_current(
    left_input: usize,
    left: &OrderedLiveDocument,
    right_input: usize,
    right: &OrderedLiveDocument,
) -> Ordering {
    left.order_key
        .cmp(&right.order_key)
        .then_with(|| {
            left.identity
                .result_or_source()
                .path
                .cmp(&right.identity.result_or_source().path)
        })
        .then_with(|| {
            left.identity
                .result_or_source()
                .version
                .cmp(&right.identity.result_or_source().version)
        })
        .then_with(|| left.identity.source.path.cmp(&right.identity.source.path))
        .then_with(|| {
            left.identity
                .source
                .version
                .cmp(&right.identity.source.version)
        })
        .then_with(|| {
            left.identity
                .source_record
                .cmp(&right.identity.source_record)
        })
        // Source-record ordinal preserves order within a source. A live source
        // cannot span inputs, so these are corruption-safe final tie breaks.
        .then_with(|| left_input.cmp(&right_input))
        .then_with(|| left.old_doc_id.cmp(&right.old_doc_id))
}

fn order_value(cell: &DocValueCell) -> Result<SortValue, IndexError> {
    if !cell.present {
        return Ok(SortValue::Missing);
    }
    if cell.null && cell.values.is_empty() {
        return Ok(SortValue::Value(ScalarValue::Null));
    }
    if !cell.null && cell.values.len() == 1 {
        return Ok(SortValue::Value(cell.values[0].clone()));
    }
    Err(IndexError::InvalidFormat(
        "single-valued physical-order column has no exact scalar state",
    ))
}

struct IdentityCursor<'a, D> {
    stream: RoutedBlockStream<'a, D>,
    block: Option<IdentityBlock>,
    offset: usize,
}

impl<'a, D: ArtifactDirectoryRead> IdentityCursor<'a, D> {
    fn new(stream: RoutedBlockStream<'a, D>) -> Self {
        Self {
            stream,
            block: None,
            offset: 0,
        }
    }

    async fn next(&mut self, expected: u32) -> Result<DocumentIdentity, IndexError> {
        if self
            .block
            .as_ref()
            .is_none_or(|block| self.offset == block.entries().len())
        {
            let (_, block) = self
                .stream
                .next(IdentityBlock::decode_payload)
                .await?
                .ok_or(IndexError::InvalidFormat("identity stream ended early"))?;
            if block.first_doc_id.get() != expected {
                return Err(IndexError::InvalidFormat(
                    "identity blocks are not dense and ordered",
                ));
            }
            self.block = Some(block);
            self.offset = 0;
        }
        let identity = self.block.as_ref().unwrap().entries()[self.offset].clone();
        self.offset += 1;
        Ok(identity)
    }

    async fn finish(&mut self) -> Result<(), IndexError> {
        if self
            .block
            .as_ref()
            .is_some_and(|block| self.offset != block.entries().len())
            || self
                .stream
                .next(IdentityBlock::decode_payload)
                .await?
                .is_some()
        {
            return Err(IndexError::InvalidFormat(
                "identity stream exceeds segment document count",
            ));
        }
        Ok(())
    }
}

struct LiveCursor<'a, D> {
    stream: RoutedBlockStream<'a, D>,
    block: Option<LiveMaskBlock>,
    offset: u32,
}

impl<'a, D: ArtifactDirectoryRead> LiveCursor<'a, D> {
    fn new(stream: RoutedBlockStream<'a, D>) -> Self {
        Self {
            stream,
            block: None,
            offset: 0,
        }
    }

    async fn next(&mut self, expected: u32) -> Result<bool, IndexError> {
        if self
            .block
            .as_ref()
            .is_none_or(|block| self.offset == block.document_count)
        {
            let (_, block) = self
                .stream
                .next(LiveMaskBlock::decode_payload)
                .await?
                .ok_or(IndexError::InvalidFormat("live-mask stream ended early"))?;
            if block.first_doc_id.get() != expected {
                return Err(IndexError::InvalidFormat(
                    "live-mask blocks are not dense and ordered",
                ));
            }
            self.block = Some(block);
            self.offset = 0;
        }
        let value = self
            .block
            .as_ref()
            .unwrap()
            .is_live(super::super::super::DocId::new(expected))
            .ok_or(IndexError::InvalidFormat("live-mask range mismatch"))?;
        self.offset += 1;
        Ok(value)
    }

    async fn finish(&mut self) -> Result<(), IndexError> {
        if self
            .block
            .as_ref()
            .is_some_and(|block| self.offset != block.document_count)
            || self
                .stream
                .next(LiveMaskBlock::decode_payload)
                .await?
                .is_some()
        {
            return Err(IndexError::InvalidFormat(
                "live-mask stream exceeds segment document count",
            ));
        }
        Ok(())
    }
}

struct ColumnCursor<'a, D> {
    stream: RoutedBlockStream<'a, D>,
    field_id: super::super::super::FieldId,
    multi_valued: bool,
    block: Option<DocValueBlock>,
    offset: usize,
}

impl<'a, D: ArtifactDirectoryRead> ColumnCursor<'a, D> {
    fn new(
        stream: RoutedBlockStream<'a, D>,
        field_id: super::super::super::FieldId,
        multi_valued: bool,
    ) -> Self {
        Self {
            stream,
            field_id,
            multi_valued,
            block: None,
            offset: 0,
        }
    }

    async fn next(&mut self, expected: u32) -> Result<DocValueCell, IndexError> {
        if self
            .block
            .as_ref()
            .is_none_or(|block| self.offset == block.cells().len())
        {
            let (_, block) = self
                .stream
                .next(DocValueBlock::decode_payload)
                .await?
                .ok_or(IndexError::InvalidFormat("fast-column stream ended early"))?;
            if block.field_id != self.field_id
                || block.multi_valued != self.multi_valued
                || block.first_doc_id.get() != expected
            {
                return Err(IndexError::InvalidFormat(
                    "doc-value blocks are not dense or schema-compatible",
                ));
            }
            self.block = Some(block);
            self.offset = 0;
        }
        let cell = self.block.as_ref().unwrap().cells()[self.offset].clone();
        self.offset += 1;
        Ok(cell)
    }

    async fn finish(&mut self) -> Result<(), IndexError> {
        if self
            .block
            .as_ref()
            .is_some_and(|block| self.offset != block.cells().len())
            || self
                .stream
                .next(DocValueBlock::decode_payload)
                .await?
                .is_some()
        {
            return Err(IndexError::InvalidFormat(
                "doc-value stream exceeds segment document count",
            ));
        }
        Ok(())
    }
}
