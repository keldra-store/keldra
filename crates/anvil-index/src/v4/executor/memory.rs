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

/// Retained response bytes admitted for one native page. Four MiB permits
/// ordinary small pages to reach the public hit limit while forcing unusually
/// large stored projections to continue at an exact hit boundary.
pub(super) const DEFAULT_MAXIMUM_PAGE_BYTES: usize = INDEX_DECODE_BYTES;

pub(crate) fn estimate_working_memory(
    request: &NativeQueryRequest,
    limits: NativeQueryLimits,
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
    // Physical k-way execution retains only owned heads and compact cursor
    // continuation per segment. Exactly one advancing segment retains decoded
    // blocks, so consecutive candidates reuse them without multiplying the
    // component ceiling by the generation's segment count.
    let resident_components = 2usize
        .checked_add(fields.len())
        .and_then(|value| value.checked_add(leaves))
        .ok_or(IndexError::OffsetOverflow)?;
    let decoded = resident_components
        .checked_mul(INDEX_DECODE_BYTES)
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
    let heap = (request.limit as usize)
        .checked_mul(candidate_bytes)
        .ok_or(IndexError::OffsetOverflow)?;
    // A gate batch temporarily retains candidates and cloned gate references.
    let gate = limits
        .candidate_gate_batch
        .checked_mul(candidate_bytes)
        .and_then(|value| value.checked_mul(2))
        .ok_or(IndexError::OffsetOverflow)?;
    let vector_workspace = vector_workspace_bytes(&request.query)?;
    FIXED_EXECUTOR_BYTES
        .checked_add(decoded)
        .and_then(|value| value.checked_add(cursor_state))
        .and_then(|value| value.checked_add(heap))
        .and_then(|value| value.checked_add(gate))
        .and_then(|value| value.checked_add(vector_workspace))
        .and_then(|value| value.checked_add(limits.maximum_page_bytes))
        .ok_or(IndexError::OffsetOverflow)
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

/// Fixed page structures and the exact retained `NativeQueryHit` slots. Owned
/// strings, sort values, and stored bytes are charged separately as hits are
/// materialized.
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
        .and_then(|value| value.checked_add(hit.fields_json.capacity()))
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
        NativeQuery::FullText { text, phrase } => {
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
                // Candidate intersection and BM25 each own a cursor; phrase
                // verification owns one additional cursor per term.
                .and_then(|value| value.checked_mul(2 + usize::from(*phrase)))
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
