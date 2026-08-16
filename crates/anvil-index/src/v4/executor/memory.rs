use std::collections::BTreeSet;

use crate::IndexError;

use super::super::{
    FieldId, INDEX_DECODE_BYTES, INDEX_ROUTING_KEY_BYTES, NativeQuery, NativeQueryCursor,
    NativeQueryHit, NativeQueryPage, NativeQueryRequest, ObjectIdentity, Predicate, ScalarValue,
    SortValue,
};
use super::execute::NativeQueryLimits;

const FIXED_EXECUTOR_BYTES: usize = 1024 * 1024;
// An inactive posting cursor retains two bounded locator keys plus its root
// descriptor and traversal bookkeeping, but no decoded component. Physical
// execution keeps decoded components for exactly one advancing segment.
const CURSOR_STATE_BYTES: usize = 2 * INDEX_ROUTING_KEY_BYTES + 1024;
const OWNED_VALUE_OVERHEAD_BYTES: usize = 32;
const OWNED_IDENTITY_OVERHEAD_BYTES: usize = 64;

/// Retained response bytes admitted for one native page.
pub(super) const DEFAULT_MAXIMUM_PAGE_BYTES: usize = INDEX_DECODE_BYTES;

pub(crate) fn estimate_working_memory(
    request: &NativeQueryRequest,
    limits: NativeQueryLimits,
    query_parallelism: usize,
) -> Result<usize, IndexError> {
    request.validate()?;
    let mut fields = BTreeSet::new();
    let leaves = query_shape(request, &mut fields)?;
    let physical = match &request.query {
        NativeQuery::Path { .. } => true,
        NativeQuery::Filter { order, .. } => {
            !order.is_empty() && *order == request.schema.physical_order
        }
        _ => false,
    };
    let sort_values = match &request.query {
        NativeQuery::Filter { order, .. } => order.len(),
        _ => 1,
    };
    let requested_lanes = query_parallelism.max(1).min(request.segments.len().max(1));
    let lanes = if matches!(&request.query, NativeQuery::FullText { .. }) {
        1
    } else {
        requested_lanes
    };
    // Physical k-way execution retains a small cursor and owned merge head for
    // every segment. Decoded immutable blocks belong only to the segment being
    // advanced and are released once its next head has been extracted.
    let resident_components = 2usize
        .checked_add(fields.len())
        .and_then(|value| value.checked_add(leaves))
        .ok_or(IndexError::OffsetOverflow)?;
    let decoded = resident_components
        .checked_mul(INDEX_DECODE_BYTES)
        .and_then(|value| value.checked_mul(lanes))
        .ok_or(IndexError::OffsetOverflow)?;
    let per_segment_cursor = leaves
        .max(1)
        .checked_mul(CURSOR_STATE_BYTES)
        .ok_or(IndexError::OffsetOverflow)?;
    let identity_bytes = INDEX_ROUTING_KEY_BYTES
        .checked_mul(2)
        .and_then(|value| value.checked_add(OWNED_IDENTITY_OVERHEAD_BYTES))
        .ok_or(IndexError::OffsetOverflow)?;
    let sort_bytes = sort_values
        .checked_mul(
            INDEX_ROUTING_KEY_BYTES
                .checked_add(OWNED_VALUE_OVERHEAD_BYTES)
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)?;
    let candidate_bytes = identity_bytes
        .checked_add(sort_bytes)
        .and_then(|value| value.checked_add(OWNED_VALUE_OVERHEAD_BYTES))
        .ok_or(IndexError::OffsetOverflow)?;
    let per_segment_state = per_segment_cursor
        .checked_add(if physical { candidate_bytes } else { 0 })
        .ok_or(IndexError::OffsetOverflow)?;
    let cursor_state = request
        .segments
        .len()
        .checked_mul(per_segment_state)
        .ok_or(IndexError::OffsetOverflow)?;
    let document_sets = document_set_bytes(request)?;
    let heap = (request.limit as usize)
        .checked_mul(candidate_bytes)
        .and_then(|value| {
            value.checked_mul(if physical {
                1
            } else {
                // One bounded heap in every active segment lane, plus the
                // coordinator's final merge heap.
                lanes.saturating_add(1)
            })
        })
        .ok_or(IndexError::OffsetOverflow)?;
    // A gate batch temporarily retains candidates and cloned gate references.
    let gate = limits
        .candidate_gate_batch
        .checked_mul(candidate_bytes)
        .and_then(|value| value.checked_mul(2))
        .and_then(|value| value.checked_mul(if physical { 1 } else { lanes }))
        .ok_or(IndexError::OffsetOverflow)?;
    let vector_workspace = vector_workspace_bytes(&request.query)?
        .checked_mul(lanes)
        .ok_or(IndexError::OffsetOverflow)?;
    FIXED_EXECUTOR_BYTES
        .checked_add(decoded)
        .and_then(|value| value.checked_add(cursor_state))
        .and_then(|value| value.checked_add(document_sets))
        .and_then(|value| value.checked_add(heap))
        .and_then(|value| value.checked_add(gate))
        .and_then(|value| value.checked_add(vector_workspace))
        .and_then(|value| value.checked_add(limits.maximum_page_bytes))
        .ok_or(IndexError::OffsetOverflow)
}

/// Term-range and point predicates materialize one dense segment-local DocId
/// set. This is the same bounded cost whether a range matches one value or
/// millions, and prevents repeatedly traversing immutable leaves for every
/// small result batch.
fn document_set_bytes(request: &NativeQueryRequest) -> Result<usize, IndexError> {
    let sets = query_document_sets(request)?;
    if sets == 0 {
        return Ok(0);
    }
    request.segments.iter().try_fold(0usize, |total, segment| {
        let words = usize::try_from(segment.document_count)
            .map_err(|_| IndexError::OffsetOverflow)?
            .div_ceil(u64::BITS as usize);
        total
            .checked_add(
                words
                    .checked_mul(std::mem::size_of::<u64>())
                    .and_then(|value| value.checked_mul(sets))
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn query_document_sets(request: &NativeQueryRequest) -> Result<usize, IndexError> {
    Ok(match &request.query {
        NativeQuery::Path { .. } => 1,
        NativeQuery::Filter { predicate, .. } => predicate
            .as_ref()
            .map_or(Ok(0), |value| predicate_document_sets(value, request))?,
        NativeQuery::GitSource { prefix, .. } => usize::from(*prefix),
        NativeQuery::FullText { .. }
        | NativeQuery::Vector { .. }
        | NativeQuery::Hybrid { .. }
        | NativeQuery::Tensor { .. } => 0,
    })
}

fn predicate_document_sets(
    predicate: &Predicate,
    request: &NativeQueryRequest,
) -> Result<usize, IndexError> {
    let point_field = |field_id: FieldId| {
        request
            .schema
            .fields
            .get(field_id.get() as usize)
            .is_some_and(|field| {
                field
                    .components
                    .contains(super::super::FieldComponents::POINTS)
            })
    };
    Ok(match predicate {
        Predicate::Equal { field_id, .. } => usize::from(point_field(*field_id)),
        Predicate::In {
            field_id, values, ..
        } => {
            if point_field(*field_id) {
                values.len()
            } else {
                0
            }
        }
        Predicate::Prefix { .. } | Predicate::Range { .. } => 1,
        Predicate::Exists { field_id, .. } => usize::from(point_field(*field_id)),
        Predicate::FullText { .. } | Predicate::Phrase { .. } => 0,
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().try_fold(0usize, |total, child| {
                total
                    .checked_add(predicate_document_sets(child, request)?)
                    .ok_or(IndexError::OffsetOverflow)
            })?
        }
        Predicate::Not(child) => predicate_document_sets(child, request)?,
    })
}

/// A normalized query vector and one owned stored vector coexist while a
/// vector candidate is scored on the CPU pool. The decoded vector block is
/// charged separately as a resident component above.
fn vector_workspace_bytes(query: &NativeQuery) -> Result<usize, IndexError> {
    let dimensions = match query {
        NativeQuery::Vector { values } => values.len(),
        NativeQuery::Hybrid { vector, .. } => vector.len(),
        _ => 0,
    };
    if dimensions == 0 {
        return Ok(0);
    }
    dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| {
            bytes.checked_add(
                std::mem::size_of::<std::sync::Arc<[f32]>>() + std::mem::size_of::<Vec<f32>>(),
            )
        })
        .ok_or(IndexError::OffsetOverflow)
}

/// Fixed page structures and exact retained `NativeQueryHit` slots.
pub(super) fn page_base_bytes(hit_capacity: usize) -> Result<usize, IndexError> {
    std::mem::size_of::<NativeQueryPage>()
        .checked_add(
            hit_capacity
                .checked_mul(std::mem::size_of::<NativeQueryHit>())
                .ok_or(IndexError::OffsetOverflow)?,
        )
        .ok_or(IndexError::OffsetOverflow)
}

/// Heap allocations owned by a retained hit, excluding the hit struct itself.
pub(super) fn hit_owned_bytes(hit: &NativeQueryHit) -> Result<usize, IndexError> {
    let cursor = cursor_owned_bytes(&hit.cursor)?;
    identity_owned_bytes(&hit.source)
        .checked_add(identity_owned_bytes(&hit.result))
        .and_then(|value| value.checked_add(cursor))
        .ok_or(IndexError::OffsetOverflow)
}

/// Heap allocations added by cloning the last hit's cursor into `page.next`.
pub(super) fn cursor_owned_bytes(cursor: &NativeQueryCursor) -> Result<usize, IndexError> {
    let values = cursor
        .sort_values
        .capacity()
        .checked_mul(std::mem::size_of::<SortValue>())
        .ok_or(IndexError::OffsetOverflow)?;
    cursor
        .sort_values
        .iter()
        .try_fold(values, |total, value| {
            total
                .checked_add(match value {
                    SortValue::Value(ScalarValue::String(value)) => value.capacity(),
                    _ => 0,
                })
                .ok_or(IndexError::OffsetOverflow)
        })?
        .checked_add(identity_owned_bytes(&cursor.result))
        .and_then(|value| value.checked_add(identity_owned_bytes(&cursor.source)))
        .ok_or(IndexError::OffsetOverflow)
}

#[cfg(test)]
pub(super) fn retained_page_bytes(page: &NativeQueryPage) -> Result<usize, IndexError> {
    let hits =
        page.hits
            .iter()
            .try_fold(page_base_bytes(page.hits.capacity())?, |total, hit| {
                total
                    .checked_add(hit_owned_bytes(hit)?)
                    .ok_or(IndexError::OffsetOverflow)
            })?;
    page.next.as_ref().map_or(Ok(hits), |cursor| {
        hits.checked_add(cursor_owned_bytes(cursor)?)
            .ok_or(IndexError::OffsetOverflow)
    })
}

fn identity_owned_bytes(identity: &ObjectIdentity) -> usize {
    identity.path.capacity()
}

fn query_shape(
    request: &NativeQueryRequest,
    fields: &mut BTreeSet<FieldId>,
) -> Result<usize, IndexError> {
    Ok(match &request.query {
        NativeQuery::Path { .. } => {
            fields.insert(FieldId::new(0));
            1
        }
        NativeQuery::Filter { predicate, order } => {
            fields.extend(order.iter().map(|value| value.field_id));
            predicate
                .as_ref()
                .map_or(Ok(0), |value| predicate_shape(value, fields))?
        }
        NativeQuery::FullText { text, .. } => {
            let tokens = text.split_whitespace().count().max(1);
            let text_fields = request
                .schema
                .fields
                .iter()
                .filter(|field| {
                    field
                        .components
                        .contains(super::super::FieldComponents::POSITIONS)
                })
                .map(|field| {
                    fields.insert(field.id);
                })
                .count();
            tokens
                .checked_mul(text_fields)
                // Candidate matching (including exact positional phrases) and
                // BM25 scoring each own one cursor per term.
                .and_then(|value| value.checked_mul(2))
                .ok_or(IndexError::OffsetOverflow)?
        }
        NativeQuery::Vector { .. } => {
            fields.extend(request.schema.fields.iter().filter_map(|field| {
                field
                    .components
                    .contains(super::super::FieldComponents::VECTOR)
                    .then_some(field.id)
            }));
            0
        }
        NativeQuery::Hybrid { text, .. } => {
            let tokens = text.split_whitespace().count();
            let text_fields = request
                .schema
                .fields
                .iter()
                .filter(|field| {
                    let positional = field
                        .components
                        .contains(super::super::FieldComponents::POSITIONS);
                    if positional
                        || field
                            .components
                            .contains(super::super::FieldComponents::VECTOR)
                    {
                        fields.insert(field.id);
                    }
                    positional
                })
                .count();
            tokens
                .checked_mul(text_fields)
                // Candidate intersection and lexical scoring own independent
                // cursor state over the same immutable postings.
                .and_then(|value| value.checked_mul(2))
                .ok_or(IndexError::OffsetOverflow)?
        }
        NativeQuery::GitSource { .. } => {
            fields.extend([FieldId::new(0), FieldId::new(1), FieldId::new(2)]);
            3
        }
        NativeQuery::Tensor { .. } => {
            fields.extend([FieldId::new(0), FieldId::new(1)]);
            2
        }
    })
}

fn predicate_shape(
    predicate: &Predicate,
    fields: &mut BTreeSet<FieldId>,
) -> Result<usize, IndexError> {
    Ok(match predicate {
        Predicate::Equal { field_id, .. } => {
            fields.insert(*field_id);
            1
        }
        Predicate::In {
            field_id, values, ..
        } => {
            fields.insert(*field_id);
            values.len()
        }
        Predicate::Prefix { field_id, .. } | Predicate::Range { field_id, .. } => {
            fields.insert(*field_id);
            1
        }
        Predicate::Exists { field_id, .. } => {
            fields.insert(*field_id);
            0
        }
        Predicate::FullText { field_id, text, .. } | Predicate::Phrase { field_id, text, .. } => {
            fields.insert(*field_id);
            text.split_whitespace().count().max(1)
        }
        Predicate::And(children) | Predicate::Or(children) => {
            children.iter().try_fold(0usize, |total, child| {
                total
                    .checked_add(predicate_shape(child, fields)?)
                    .ok_or(IndexError::OffsetOverflow)
            })?
        }
        Predicate::Not(child) => predicate_shape(child, fields)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_workspace_charges_normalized_query_and_stored_candidate() {
        let dimensions = 1_024usize;
        let expected = 2 * dimensions * std::mem::size_of::<f32>()
            + std::mem::size_of::<std::sync::Arc<[f32]>>()
            + std::mem::size_of::<Vec<f32>>();

        assert_eq!(
            vector_workspace_bytes(&NativeQuery::Vector {
                values: vec![0.0; dimensions],
            })
            .unwrap(),
            expected
        );
        assert_eq!(
            vector_workspace_bytes(&NativeQuery::Hybrid {
                text: "terms".into(),
                vector: vec![0.0; dimensions],
            })
            .unwrap(),
            expected
        );
        assert_eq!(
            vector_workspace_bytes(&NativeQuery::Path {
                prefix: String::new(),
                start_after: None,
            })
            .unwrap(),
            0
        );
    }
}
