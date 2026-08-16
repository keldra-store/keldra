use std::cmp::Ordering;

use crate::IndexError;
use crate::v4::{ComponentStatistics, FieldStatistics, Schema, SegmentComponent};

use super::super::{ProjectedRecord, ProjectedSource};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DocumentRef {
    pub source_ordinal: u32,
    pub source_record: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SourceDocRef {
    pub source_ordinal: u32,
    pub doc_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TermRef {
    pub doc_id: u32,
    pub term_ordinal: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PointRef {
    pub doc_id: u32,
    pub point_ordinal: u32,
    pub kind: PointRefKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PointRefKind {
    Presence,
    Null,
    Value(u32),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct WriterCharge {
    schema_workspace_bytes: usize,
    retained_source_bytes: usize,
    source_slots_bytes: usize,
    document_count: usize,
    term_count: usize,
    point_count: usize,
}

impl WriterCharge {
    pub fn for_schema(schema: &Schema) -> Result<Self, IndexError> {
        let shape = schema.segment_shape()?;
        let field_capacity = charged_vec_capacity(0, shape.field_count)?;
        let component_capacity = charged_vec_capacity(0, shape.component_count)?;
        let statistics_capacity = charged_vec_capacity(0, shape.component_statistics_count)?;
        let fields = field_capacity
            .checked_mul(std::mem::size_of::<FieldStatistics>())
            .ok_or(IndexError::OffsetOverflow)?;
        let seen = field_capacity
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or(IndexError::OffsetOverflow)?;
        let assembly = component_capacity
            .checked_mul(std::mem::size_of::<SegmentComponent>())
            .and_then(|bytes| {
                bytes.checked_add(
                    statistics_capacity.checked_mul(std::mem::size_of::<ComponentStatistics>())?,
                )
            })
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(Self {
            schema_workspace_bytes: fields
                .checked_add(seen.max(assembly))
                .ok_or(IndexError::OffsetOverflow)?,
            ..Self::default()
        })
    }

    pub fn with_source(
        self,
        source: &ProjectedSource,
        source_capacity: usize,
        path_ordinal_capacity: usize,
    ) -> Result<Self, IndexError> {
        let document_count = self
            .document_count
            .checked_add(source.records.len())
            .ok_or(IndexError::OffsetOverflow)?;
        let term_count = source
            .records
            .iter()
            .try_fold(self.term_count, |count, record| {
                count
                    .checked_add(record.terms.len())
                    .ok_or(IndexError::OffsetOverflow)
            })?;
        let point_count = source
            .records
            .iter()
            .try_fold(self.point_count, |count, record| {
                record.points.iter().try_fold(count, |count, point| {
                    count
                        .checked_add(
                            point
                                .values
                                .len()
                                .saturating_add(1)
                                .saturating_add(usize::from(point.null)),
                        )
                        .ok_or(IndexError::OffsetOverflow)
                })
            })?;
        Ok(Self {
            schema_workspace_bytes: self.schema_workspace_bytes,
            retained_source_bytes: self
                .retained_source_bytes
                .checked_add(source.retained_dynamic_bytes()?)
                .ok_or(IndexError::OffsetOverflow)?,
            source_slots_bytes: source_capacity
                .checked_mul(std::mem::size_of::<ProjectedSource>())
                .and_then(|bytes| {
                    bytes
                        .checked_add(path_ordinal_capacity.checked_mul(std::mem::size_of::<u32>())?)
                })
                .ok_or(IndexError::OffsetOverflow)?,
            document_count,
            term_count,
            point_count,
        })
    }

    pub fn peak_bytes(self) -> Result<usize, IndexError> {
        let document_capacity = charged_vec_capacity(0, self.document_count)?;
        let term_capacity = charged_vec_capacity(0, self.term_count)?;
        let point_capacity = charged_vec_capacity(0, self.point_count)?;
        let documents = document_capacity
            .checked_mul(std::mem::size_of::<DocumentRef>())
            .ok_or(IndexError::OffsetOverflow)?;
        let term_refs = term_capacity
            .checked_mul(std::mem::size_of::<TermRef>())
            .ok_or(IndexError::OffsetOverflow)?;
        let point_refs = point_capacity
            .checked_mul(std::mem::size_of::<PointRef>())
            .ok_or(IndexError::OffsetOverflow)?;
        let locator_refs = document_capacity
            .checked_mul(std::mem::size_of::<SourceDocRef>())
            .ok_or(IndexError::OffsetOverflow)?;
        self.retained_source_bytes
            .checked_add(self.schema_workspace_bytes)
            .and_then(|bytes| bytes.checked_add(self.source_slots_bytes))
            .and_then(|bytes| bytes.checked_add(documents))
            .and_then(|bytes| bytes.checked_add(term_refs.max(point_refs).max(locator_refs)))
            .ok_or(IndexError::OffsetOverflow)
    }

    pub fn document_count(self) -> usize {
        self.document_count
    }

    pub fn term_count(self) -> usize {
        self.term_count
    }

    pub fn point_count(self) -> usize {
        self.point_count
    }
}

pub(super) fn charged_vec_capacity(current: usize, required: usize) -> Result<usize, IndexError> {
    if current >= required {
        return Ok(current);
    }
    required
        .checked_next_power_of_two()
        .map(|capacity| capacity.max(4))
        .ok_or(IndexError::OffsetOverflow)
}

pub(super) fn charged_vec<T>(required: usize) -> Result<Vec<T>, IndexError> {
    let charged = charged_vec_capacity(0, required)?;
    let output = Vec::with_capacity(charged);
    if output.capacity() != charged {
        return Err(IndexError::ResourceLimit {
            needed: output.capacity().saturating_mul(std::mem::size_of::<T>()),
            limit: charged.saturating_mul(std::mem::size_of::<T>()),
        });
    }
    Ok(output)
}

pub(super) fn build_document_layout(
    sources: &[ProjectedSource],
    document_count: usize,
) -> Result<Vec<DocumentRef>, IndexError> {
    let mut documents = charged_vec(document_count)?;
    for (source_ordinal, source) in sources.iter().enumerate() {
        let source_ordinal =
            u32::try_from(source_ordinal).map_err(|_| IndexError::OffsetOverflow)?;
        for source_record in 0..source.records.len() {
            documents.push(DocumentRef {
                source_ordinal,
                source_record: u32::try_from(source_record)
                    .map_err(|_| IndexError::OffsetOverflow)?,
            });
        }
    }
    documents.sort_unstable_by(|left, right| compare_documents(sources, *left, *right));
    Ok(documents)
}

fn compare_documents(
    sources: &[ProjectedSource],
    left: DocumentRef,
    right: DocumentRef,
) -> Ordering {
    let left_source = source(sources, left);
    let right_source = source(sources, right);
    let left_record = record(sources, left);
    let right_record = record(sources, right);
    order_bytes(left_source, left_record)
        .cmp(order_bytes(right_source, right_record))
        .then_with(|| {
            result_identity(left_source, left_record)
                .path
                .cmp(&result_identity(right_source, right_record).path)
        })
        .then_with(|| {
            result_identity(left_source, left_record)
                .version
                .cmp(&result_identity(right_source, right_record).version)
        })
        .then_with(|| {
            left_source
                .source_identity
                .path
                .cmp(&right_source.source_identity.path)
        })
        .then_with(|| {
            left_source
                .source_identity
                .version
                .cmp(&right_source.source_identity.version)
        })
        .then_with(|| left.source_record.cmp(&right.source_record))
}

pub(super) fn source(sources: &[ProjectedSource], reference: DocumentRef) -> &ProjectedSource {
    &sources[reference.source_ordinal as usize]
}

pub(super) fn record(sources: &[ProjectedSource], reference: DocumentRef) -> &ProjectedRecord {
    &source(sources, reference).records[reference.source_record as usize]
}

pub(super) fn result_identity<'a>(
    source: &'a ProjectedSource,
    record: &'a ProjectedRecord,
) -> &'a super::super::super::ObjectIdentity {
    record
        .result_identity
        .as_ref()
        .unwrap_or(&source.source_identity)
}

pub(super) fn order_bytes<'a>(
    source: &'a ProjectedSource,
    record: &'a ProjectedRecord,
) -> &'a [u8] {
    if record.order_key.is_empty() {
        result_identity(source, record).path.as_bytes()
    } else {
        &record.order_key
    }
}

pub(super) fn build_source_doc_refs(
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
) -> Result<Vec<SourceDocRef>, IndexError> {
    let mut references = charged_vec(documents.len())?;
    for (doc_id, document) in documents.iter().copied().enumerate() {
        references.push(SourceDocRef {
            source_ordinal: document.source_ordinal,
            doc_id: u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?,
        });
    }
    references.sort_unstable_by(|left, right| {
        sources[left.source_ordinal as usize]
            .source_identity
            .path
            .cmp(&sources[right.source_ordinal as usize].source_identity.path)
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    for pair in references.windows(2) {
        let left = &sources[pair[0].source_ordinal as usize]
            .source_identity
            .path;
        let right = &sources[pair[1].source_ordinal as usize]
            .source_identity
            .path;
        if left == right && pair[0].source_ordinal != pair[1].source_ordinal {
            return Err(IndexError::InvalidDefinition(
                "one segment cannot contain two versions of a source path".into(),
            ));
        }
    }
    Ok(references)
}

pub(super) fn build_term_refs(
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    term_count: usize,
) -> Result<Vec<TermRef>, IndexError> {
    let mut references = charged_vec(term_count)?;
    for (doc_id, document) in documents.iter().copied().enumerate() {
        for term_ordinal in 0..record(sources, document).terms.len() {
            references.push(TermRef {
                doc_id: u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?,
                term_ordinal: u32::try_from(term_ordinal)
                    .map_err(|_| IndexError::OffsetOverflow)?,
            });
        }
    }
    references.sort_unstable_by(|left, right| {
        let left_term = term(sources, documents, *left);
        let right_term = term(sources, documents, *right);
        (left_term.field_id, left_term.term_type)
            .cmp(&(right_term.field_id, right_term.term_type))
            .then_with(|| left_term.term.cmp(&right_term.term))
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    for pair in references.windows(2) {
        let left = term(sources, documents, pair[0]);
        let right = term(sources, documents, pair[1]);
        if pair[0].doc_id == pair[1].doc_id
            && left.field_id == right.field_id
            && left.term_type == right.term_type
            && left.term == right.term
        {
            return Err(IndexError::InvalidDefinition(
                "one projected record contains a duplicate term".into(),
            ));
        }
    }
    Ok(references)
}

pub(super) fn term<'a>(
    sources: &'a [ProjectedSource],
    documents: &[DocumentRef],
    reference: TermRef,
) -> &'a super::super::ProjectedTerm {
    &record(sources, documents[reference.doc_id as usize]).terms[reference.term_ordinal as usize]
}

pub(super) fn build_point_refs(
    sources: &[ProjectedSource],
    documents: &[DocumentRef],
    point_count: usize,
) -> Result<Vec<PointRef>, IndexError> {
    let mut references = charged_vec(point_count)?;
    for (doc_id, document) in documents.iter().copied().enumerate() {
        for (point_ordinal, point) in record(sources, document).points.iter().enumerate() {
            references.push(PointRef {
                doc_id: u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?,
                point_ordinal: u32::try_from(point_ordinal)
                    .map_err(|_| IndexError::OffsetOverflow)?,
                kind: PointRefKind::Presence,
            });
            if point.null {
                references.push(PointRef {
                    doc_id: u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?,
                    point_ordinal: u32::try_from(point_ordinal)
                        .map_err(|_| IndexError::OffsetOverflow)?,
                    kind: PointRefKind::Null,
                });
            }
            for value_ordinal in 0..point.values.len() {
                references.push(PointRef {
                    doc_id: u32::try_from(doc_id).map_err(|_| IndexError::OffsetOverflow)?,
                    point_ordinal: u32::try_from(point_ordinal)
                        .map_err(|_| IndexError::OffsetOverflow)?,
                    kind: PointRefKind::Value(
                        u32::try_from(value_ordinal).map_err(|_| IndexError::OffsetOverflow)?,
                    ),
                });
            }
        }
    }
    references.sort_unstable_by(|left, right| {
        let (left_field, left_value) = point(sources, documents, *left);
        let (right_field, right_value) = point(sources, documents, *right);
        left_field
            .cmp(&right_field)
            .then_with(|| left_value.cmp(&right_value))
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    Ok(references)
}

pub(super) fn point<'a>(
    sources: &'a [ProjectedSource],
    documents: &[DocumentRef],
    reference: PointRef,
) -> (crate::v4::FieldId, crate::v4::PointValue) {
    let point = &record(sources, documents[reference.doc_id as usize]).points
        [reference.point_ordinal as usize];
    (
        point.field_id,
        match reference.kind {
            PointRefKind::Presence => crate::v4::PointValue::Presence,
            PointRefKind::Null => crate::v4::PointValue::Null,
            PointRefKind::Value(ordinal) => {
                crate::v4::PointValue::Value(point.values[ordinal as usize].clone())
            }
        },
    )
}
