use keldra_index::IndexError;
use keldra_index::typed_json::{
    AggregateOperation, Predicate, RangeBound, ScalarValue, encode_scalar_sort_key,
};
use keldra_index::v1::{
    ExplicitQuerySearchAfter, LogicalProjectionBinding, MAX_QUERY_PARTITIONS,
    QuerySnapshotIdentity, StableDocumentKey,
};
use tonic::Status;

use super::super::v1_query_compile::CompiledV1Query;
use super::PinnedRootVector;

const MAGIC: &[u8; 8] = b"K1QPOS01";
// Pre-1.0 clean break: this is the sole supported cursor format. There is no
// legacy decoder or alternate on-disk identity.
const FORMAT: u16 = 1;
const FIXED_BYTES: usize = 8 + 2 + 32 + 32 + 1 + 32 + 4 + 4;
const ROOT_PROOF_BYTES: usize = 8;
const MAX_ORDER_VALUES: usize = 64;
const MAX_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct QueryPositionRoot {
    pub(super) generation_hash: [u8; 32],
    pub(super) next_newer_through_atomic_position: Option<u64>,
    pub(super) realtime_overlay_generation_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum QueryContinuation {
    Natural(StableDocumentKey),
    Explicit(ExplicitQuerySearchAfter),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct QueryPosition {
    pub(super) snapshot: QuerySnapshotIdentity,
    pub(super) query_binding: [u8; 32],
    pub(super) continuation: QueryContinuation,
    pub(super) roots: Vec<QueryPositionRoot>,
}

pub(super) fn normalized_query_binding(
    logical: &LogicalProjectionBinding,
    query: &CompiledV1Query,
) -> Result<[u8; 32], IndexError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"keldra.index.v1.query-position-binding/v1\0");
    bytes.extend_from_slice(&logical.logical_index_id.to_be_bytes());
    bytes.extend_from_slice(&logical.logical_definition_version.to_be_bytes());
    bytes.extend_from_slice(&logical.family_id);
    bytes.extend_from_slice(&logical.physical_catalog_generation);
    encode_optional_predicate(&mut bytes, query.predicate.as_ref())?;
    put_len(&mut bytes, query.order.len())?;
    for field in &query.order {
        bytes.extend_from_slice(&field.field_id.get().to_be_bytes());
        bytes.push(field.direction as u8);
    }
    put_len(&mut bytes, query.facets.len())?;
    for facet in &query.facets {
        bytes.extend_from_slice(&facet.field_id.get().to_be_bytes());
        bytes.extend_from_slice(&facet.limit.to_be_bytes());
    }
    put_len(&mut bytes, query.aggregates.len())?;
    for aggregate in &query.aggregates {
        bytes.extend_from_slice(&aggregate.field_id.get().to_be_bytes());
        bytes.push(match aggregate.operation {
            AggregateOperation::Count => 0,
            AggregateOperation::Minimum => 1,
            AggregateOperation::Maximum => 2,
            AggregateOperation::Sum => 3,
            AggregateOperation::Average => 4,
        });
    }
    Ok(*keldra_index::profiled_blake3_hash!(&bytes).as_bytes())
}

fn encode_optional_predicate(
    output: &mut Vec<u8>,
    predicate: Option<&Predicate>,
) -> Result<(), IndexError> {
    match predicate {
        Some(predicate) => {
            output.push(1);
            encode_predicate(output, predicate)
        }
        None => {
            output.push(0);
            Ok(())
        }
    }
}

fn encode_predicate(output: &mut Vec<u8>, predicate: &Predicate) -> Result<(), IndexError> {
    match predicate {
        Predicate::Equal {
            id,
            field_id,
            value,
        } => {
            output.push(0);
            encode_leaf(output, id.get(), field_id.get());
            encode_scalar(output, value)?;
        }
        Predicate::In {
            id,
            field_id,
            values,
        } => {
            output.push(1);
            encode_leaf(output, id.get(), field_id.get());
            put_len(output, values.len())?;
            for value in values {
                encode_scalar(output, value)?;
            }
        }
        Predicate::Prefix {
            id,
            field_id,
            prefix,
        } => {
            output.push(2);
            encode_leaf(output, id.get(), field_id.get());
            put_bytes(output, prefix.as_bytes())?;
        }
        Predicate::Range {
            id,
            field_id,
            lower,
            upper,
        } => {
            output.push(3);
            encode_leaf(output, id.get(), field_id.get());
            encode_bound(output, lower.as_ref())?;
            encode_bound(output, upper.as_ref())?;
        }
        Predicate::Exists { id, field_id } => {
            output.push(4);
            encode_leaf(output, id.get(), field_id.get());
        }
        Predicate::FullText { id, field_id, text } => {
            output.push(5);
            encode_leaf(output, id.get(), field_id.get());
            put_bytes(output, text.as_bytes())?;
        }
        Predicate::Phrase { id, field_id, text } => {
            output.push(6);
            encode_leaf(output, id.get(), field_id.get());
            put_bytes(output, text.as_bytes())?;
        }
        Predicate::And(children) | Predicate::Or(children) => {
            output.push(if matches!(predicate, Predicate::And(_)) {
                7
            } else {
                8
            });
            put_len(output, children.len())?;
            for child in children {
                encode_predicate(output, child)?;
            }
        }
        Predicate::Not(child) => {
            output.push(9);
            encode_predicate(output, child)?;
        }
    }
    Ok(())
}

fn encode_leaf(output: &mut Vec<u8>, predicate: u32, field: u32) {
    output.extend_from_slice(&predicate.to_be_bytes());
    output.extend_from_slice(&field.to_be_bytes());
}

fn encode_bound(output: &mut Vec<u8>, bound: Option<&RangeBound>) -> Result<(), IndexError> {
    match bound {
        Some(bound) => {
            output.extend_from_slice(&[1, u8::from(bound.inclusive)]);
            encode_scalar(output, &bound.value)
        }
        None => {
            output.push(0);
            Ok(())
        }
    }
}

fn encode_scalar(output: &mut Vec<u8>, value: &ScalarValue) -> Result<(), IndexError> {
    put_bytes(output, &encode_scalar_sort_key(value)?)
}

fn put_len(output: &mut Vec<u8>, value: usize) -> Result<(), IndexError> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_be_bytes(),
    );
    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), IndexError> {
    put_len(output, value.len())?;
    output.extend_from_slice(value);
    Ok(())
}

pub(super) fn decode_query_position(position: &[u8]) -> Result<QueryPosition, Status> {
    if position.len() < FIXED_BYTES
        || position.len() > MAX_BYTES
        || &position[..8] != MAGIC
        || u16::from_be_bytes(position[8..10].try_into().expect("fixed format width")) != FORMAT
    {
        return Err(Status::invalid_argument("v1 query cursor is invalid"));
    }
    let snapshot = QuerySnapshotIdentity::from_bytes(position[10..42].try_into().unwrap())
        .map_err(super::index_status)?;
    let query_binding = position[42..74].try_into().unwrap();
    let kind = position[74];
    let document = StableDocumentKey::from_bytes(position[75..107].try_into().unwrap())
        .map_err(super::index_status)?;
    let value_count =
        usize::try_from(u32::from_be_bytes(position[107..111].try_into().unwrap()))
            .map_err(|_| Status::invalid_argument("v1 query cursor value count is invalid"))?;
    let root_count = usize::try_from(u32::from_be_bytes(position[111..115].try_into().unwrap()))
        .map_err(|_| Status::invalid_argument("v1 query cursor root count is invalid"))?;
    if query_binding == [0; 32]
        || root_count == 0
        || root_count > MAX_QUERY_PARTITIONS
        || value_count > MAX_ORDER_VALUES
        || (kind == 0 && value_count != 0)
        || kind > 1
    {
        return Err(Status::invalid_argument("v1 query cursor shape is invalid"));
    }
    let mut offset = FIXED_BYTES;
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let present = take(position, &mut offset, 1)?[0];
        match present {
            0 => values.push(None),
            1 => {
                let length = usize::try_from(u32::from_be_bytes(
                    take(position, &mut offset, 4)?.try_into().unwrap(),
                ))
                .map_err(|_| Status::invalid_argument("v1 query cursor value is invalid"))?;
                let encoded = take(position, &mut offset, length)?;
                let (value, consumed) = keldra_index::typed_json::decode_scalar_sort_key(encoded)
                    .map_err(|_| {
                    Status::invalid_argument("v1 query cursor value is invalid")
                })?;
                if consumed != encoded.len() {
                    return Err(Status::invalid_argument("v1 query cursor value is invalid"));
                }
                values.push(Some(value));
            }
            _ => return Err(Status::invalid_argument("v1 query cursor value is invalid")),
        }
    }
    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        let generation_hash = take(position, &mut offset, 32)?.try_into().unwrap();
        if generation_hash == [0; 32] {
            return Err(Status::invalid_argument(
                "v1 query cursor generation is invalid",
            ));
        }
        let next_newer_through_atomic_position = match take(position, &mut offset, 1)?[0] {
            0 => None,
            1 => Some(u64::from_be_bytes(
                take(position, &mut offset, ROOT_PROOF_BYTES)?
                    .try_into()
                    .unwrap(),
            )),
            _ => {
                return Err(Status::invalid_argument(
                    "v1 query cursor root proof is invalid",
                ));
            }
        };
        let realtime_overlay_generation_hash = match take(position, &mut offset, 1)?[0] {
            0 => None,
            1 => {
                let hash = take(position, &mut offset, 32)?.try_into().unwrap();
                if hash == [0; 32] {
                    return Err(Status::invalid_argument(
                        "v1 query cursor overlay generation is invalid",
                    ));
                }
                Some(hash)
            }
            _ => {
                return Err(Status::invalid_argument(
                    "v1 query cursor overlay proof is invalid",
                ));
            }
        };
        roots.push(QueryPositionRoot {
            generation_hash,
            next_newer_through_atomic_position,
            realtime_overlay_generation_hash,
        });
    }
    if offset != position.len() {
        return Err(Status::invalid_argument(
            "v1 query cursor contains trailing bytes",
        ));
    }
    Ok(QueryPosition {
        snapshot,
        query_binding,
        continuation: if kind == 0 {
            QueryContinuation::Natural(document)
        } else {
            QueryContinuation::Explicit(ExplicitQuerySearchAfter { values, document })
        },
        roots,
    })
}

fn take<'a>(input: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8], Status> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| Status::invalid_argument("v1 query cursor is too large"))?;
    let value = input
        .get(*offset..end)
        .ok_or_else(|| Status::invalid_argument("v1 query cursor is truncated"))?;
    *offset = end;
    Ok(value)
}

pub(super) fn encode_query_position(
    snapshot: QuerySnapshotIdentity,
    query_binding: [u8; 32],
    continuation: &QueryContinuation,
    pinned: &PinnedRootVector,
) -> Result<Vec<u8>, Status> {
    if pinned.roots.is_empty()
        || pinned.roots.len() != pinned.generation_hashes.len()
        || pinned.roots.len() != pinned.overlay_generation_hashes.len()
        || pinned.roots.len() > MAX_QUERY_PARTITIONS
    {
        return Err(Status::resource_exhausted(
            "v1 query root vector is too large for a bounded continuation",
        ));
    }
    let (kind, document, values) = match continuation {
        QueryContinuation::Natural(document) => (0, *document, &[][..]),
        QueryContinuation::Explicit(cursor) => (1, cursor.document, cursor.values.as_slice()),
    };
    if values.len() > MAX_ORDER_VALUES {
        return Err(Status::resource_exhausted(
            "v1 query order exceeds the bounded continuation",
        ));
    }
    let mut position = Vec::with_capacity(FIXED_BYTES);
    position.extend_from_slice(MAGIC);
    position.extend_from_slice(&FORMAT.to_be_bytes());
    position.extend_from_slice(&snapshot.bytes());
    position.extend_from_slice(&query_binding);
    position.push(kind);
    position.extend_from_slice(&document.bytes());
    position.extend_from_slice(&(values.len() as u32).to_be_bytes());
    position.extend_from_slice(&(pinned.roots.len() as u32).to_be_bytes());
    for value in values {
        match value {
            None => position.push(0),
            Some(value) => {
                position.push(1);
                let encoded = encode_scalar_sort_key(value).map_err(super::index_status)?;
                put_bytes(&mut position, &encoded).map_err(super::index_status)?;
            }
        }
    }
    for ((root, generation_hash), overlay_hash) in pinned
        .roots
        .iter()
        .zip(&pinned.generation_hashes)
        .zip(&pinned.overlay_generation_hashes)
    {
        if *generation_hash == [0; 32] {
            return Err(Status::data_loss(
                "v1 query pinned an invalid generation identity",
            ));
        }
        position.extend_from_slice(generation_hash);
        match root.cut_proof.next_newer_through_atomic_position {
            Some(next) => {
                position.push(1);
                position.extend_from_slice(&next.to_be_bytes());
            }
            None => position.push(0),
        }
        match overlay_hash {
            Some(hash) => {
                position.push(1);
                position.extend_from_slice(hash);
            }
            None => position.push(0),
        }
    }
    if position.len() > MAX_BYTES {
        return Err(Status::resource_exhausted(
            "v1 query continuation exceeds its bounded size",
        ));
    }
    Ok(position)
}
