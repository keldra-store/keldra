//! Advanceable dense postings over the shared immutable reader core.

use crate::IndexError;

use super::{
    DecodedRecordKey, QueryBlockKind, SegmentDocumentId, SegmentLiveDocuments, SegmentReader,
    decode_dense_posting_value,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DenseSegmentPosting {
    pub document: SegmentDocumentId,
    pub material_source_version: u64,
    pub live: bool,
    pub position_block_hash: Option<[u8; 32]>,
    pub positions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DenseSegmentPoint {
    pub document: SegmentDocumentId,
    pub value: super::ScalarValue,
    pub material_source_version: u64,
    pub live: bool,
}

/// IDs and skip targets are local to `reader.documents()`. Stable object
/// identities need resolving only when a candidate crosses that boundary.
pub struct SegmentPostingCursor<'a> {
    reader: &'a SegmentReader,
    next: usize,
    live: Option<&'a SegmentLiveDocuments>,
}

impl SegmentReader {
    pub fn points(
        &self,
    ) -> Result<impl Iterator<Item = Result<DenseSegmentPoint, IndexError>> + '_, IndexError> {
        if self.kind != QueryBlockKind::Point {
            return Err(IndexError::InvalidFormat(
                "segment reader is not a point lane",
            ));
        }
        Ok(self.records.iter().map(|record| {
            let DecodedRecordKey::Point(range, id) = &record.key else {
                return Err(IndexError::Integrity);
            };
            let (value, used) = super::decode_scalar_sort_key(&self.bytes[range.clone()])?;
            if used != range.len() {
                return Err(IndexError::Integrity);
            }
            let encoded = &self.bytes[record.value.clone()];
            if encoded.len() != 9 {
                return Err(IndexError::InvalidFormat("v1 point record"));
            }
            let document = SegmentDocumentId(*id);
            let material_source_version = super::read_u64(encoded, 0)?;
            if material_source_version != self.documents.material_version(document)? {
                return Err(IndexError::Integrity);
            }
            let live = match encoded[8] {
                0 => false,
                1 => true,
                _ => return Err(IndexError::InvalidFormat("v1 point liveness")),
            };
            Ok(DenseSegmentPoint {
                document,
                value,
                material_source_version,
                live,
            })
        }))
    }

    /// Typed columns are addressed by local identity after candidate selection;
    /// unrelated rows are neither copied nor decoded.
    pub fn doc_value(
        &self,
        document: SegmentDocumentId,
        limits: super::QueryBlockLimits,
    ) -> Result<Option<super::QueryDocValue>, IndexError> {
        if self.kind != QueryBlockKind::DocValue {
            return Err(IndexError::InvalidFormat(
                "segment reader is not a value column",
            ));
        }
        let index = self.records.partition_point(
            |record| matches!(&record.key, DecodedRecordKey::Document(id) if *id < document.0),
        );
        let Some(record) = self.records.get(index) else {
            return Ok(None);
        };
        if !matches!(&record.key, DecodedRecordKey::Document(id) if *id == document.0) {
            return Ok(None);
        }
        super::super::decode_doc_value(self.record_ref(record), limits).map(Some)
    }

    pub fn posting_cursor(&self) -> Result<SegmentPostingCursor<'_>, IndexError> {
        if self.kind != QueryBlockKind::Posting {
            return Err(IndexError::InvalidFormat(
                "segment reader is not a posting lane",
            ));
        }
        Ok(SegmentPostingCursor {
            reader: self,
            next: 0,
            live: None,
        })
    }
}

impl<'a> SegmentPostingCursor<'a> {
    pub fn with_live_documents(
        mut self,
        live: &'a SegmentLiveDocuments,
    ) -> Result<Self, IndexError> {
        if live.document_table_identity() != self.reader.documents.identity() {
            return Err(IndexError::Integrity);
        }
        self.live = Some(live);
        Ok(self)
    }

    pub fn advance(
        &mut self,
        minimum: SegmentDocumentId,
    ) -> Result<Option<DenseSegmentPosting>, IndexError> {
        self.next += self.reader.records[self.next..].partition_point(
            |record| matches!(&record.key, DecodedRecordKey::Document(id) if *id < minimum.0),
        );
        self.next()
    }

    pub fn next(&mut self) -> Result<Option<DenseSegmentPosting>, IndexError> {
        while let Some(record) = self.reader.records.get(self.next) {
            self.next += 1;
            let DecodedRecordKey::Document(id) = &record.key else {
                return Err(IndexError::Integrity);
            };
            let document = SegmentDocumentId(*id);
            if self.live.is_some_and(|live| !live.is_live(document)) {
                continue;
            }
            let posting =
                decode_dense_posting_value(document, &self.reader.bytes[record.value.clone()])?;
            if posting.material_source_version
                != self.reader.documents.material_version(document)?
            {
                return Err(IndexError::Integrity);
            }
            return Ok(Some(posting));
        }
        Ok(None)
    }
}
