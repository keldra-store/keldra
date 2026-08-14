use std::cmp::Ordering;

use crate::IndexError;

use super::{SegmentValues, Selected};
use crate::v4::{
    ArtifactDirectoryRead, DocId, FieldId, NativeQuery, NativeQueryCursor, NativeQueryRequest,
    ObjectIdentity, OrderDirection, OrderField, ScalarValue, SortValue,
};

pub(super) fn compare_selected(left: &Selected, right: &Selected) -> Ordering {
    compare_parts(
        &left.sort_values,
        &left.result,
        &left.source,
        left.source_record,
        &right.sort_values,
        &right.result,
        &right.source,
        right.source_record,
        &left.directions,
    )
}

pub(super) fn compare_to_cursor(candidate: &Selected, cursor: &NativeQueryCursor) -> Ordering {
    compare_parts(
        &candidate.sort_values,
        &candidate.result,
        &candidate.source,
        candidate.source_record,
        &cursor.sort_values,
        &cursor.result,
        &cursor.source,
        cursor.source_record,
        &candidate.directions,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compare_parts(
    left_values: &[SortValue],
    left_result: &ObjectIdentity,
    left_source: &ObjectIdentity,
    left_source_record: u32,
    right_values: &[SortValue],
    right_result: &ObjectIdentity,
    right_source: &ObjectIdentity,
    right_source_record: u32,
    directions: &[OrderDirection],
) -> Ordering {
    for ((left, right), direction) in left_values.iter().zip(right_values).zip(directions) {
        let order = compare_sort_value(left, right, *direction);
        if order != Ordering::Equal {
            return order;
        }
    }
    left_result
        .path
        .as_bytes()
        .cmp(right_result.path.as_bytes())
        .then_with(|| left_result.version.cmp(&right_result.version))
        .then_with(|| {
            left_source
                .path
                .as_bytes()
                .cmp(right_source.path.as_bytes())
        })
        .then_with(|| left_source.version.cmp(&right_source.version))
        .then_with(|| left_source_record.cmp(&right_source_record))
}

fn compare_sort_value(left: &SortValue, right: &SortValue, direction: OrderDirection) -> Ordering {
    let ascending = match (left, right) {
        (SortValue::Missing, SortValue::Missing) => Ordering::Equal,
        (SortValue::Missing, _) => Ordering::Greater,
        (_, SortValue::Missing) => Ordering::Less,
        (SortValue::Value(left), SortValue::Value(right)) => left.cmp(right),
    };
    if direction == OrderDirection::Descending {
        ascending.reverse()
    } else {
        ascending
    }
}

pub(super) fn minimum_head(heads: &[Option<Selected>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.as_ref().map(|value| (index, value)))
        .min_by(|left, right| compare_selected(left.1, right.1))
        .map(|(index, _)| index)
}

pub(super) async fn rank_values<D: ArtifactDirectoryRead>(
    request: &NativeQueryRequest,
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    result: &ObjectIdentity,
    score: Option<f32>,
) -> Result<Vec<SortValue>, IndexError> {
    match &request.query {
        NativeQuery::Filter { order, .. } => order_values(values, doc_id, order).await,
        NativeQuery::FullText { .. } | NativeQuery::Vector { .. } | NativeQuery::Hybrid { .. } => {
            Ok(vec![SortValue::Value(ScalarValue::number(f64::from(
                score.ok_or(IndexError::InvalidFormat("ranked query has no score"))?,
            ))?)])
        }
        NativeQuery::Path { .. } => Ok(vec![SortValue::Value(ScalarValue::String(
            result.path.clone(),
        ))]),
        NativeQuery::GitSource { .. } => {
            Ok(vec![values.sort_value(FieldId::new(2), doc_id).await?])
        }
        NativeQuery::Tensor { .. } => Ok(vec![values.sort_value(FieldId::new(1), doc_id).await?]),
    }
}

pub(super) async fn physical_values<D: ArtifactDirectoryRead>(
    request: &NativeQueryRequest,
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    result: &ObjectIdentity,
    order: &[OrderField],
) -> Result<Vec<SortValue>, IndexError> {
    match request.query {
        NativeQuery::Path { .. } => Ok(vec![SortValue::Value(ScalarValue::String(
            result.path.clone(),
        ))]),
        _ => order_values(values, doc_id, order).await,
    }
}

async fn order_values<D: ArtifactDirectoryRead>(
    values: &mut SegmentValues<'_, D>,
    doc_id: DocId,
    order: &[OrderField],
) -> Result<Vec<SortValue>, IndexError> {
    let mut output = Vec::with_capacity(order.len());
    for field in order {
        output.push(values.sort_value(field.field_id, doc_id).await?);
    }
    Ok(output)
}

pub(super) fn physical_after(
    request: &NativeQueryRequest,
    directions: &[OrderDirection],
) -> Result<Option<NativeQueryCursor>, IndexError> {
    let mut after = request.after.clone();
    if let NativeQuery::Path {
        start_after: Some(path),
        ..
    } = &request.query
    {
        let path_cursor = NativeQueryCursor {
            sort_values: vec![SortValue::Value(ScalarValue::String(path.clone()))],
            result: ObjectIdentity {
                path: path.clone(),
                version: u64::MAX,
            },
            source: ObjectIdentity {
                path: path.clone(),
                version: u64::MAX,
            },
            source_record: u32::MAX,
        };
        if after.as_ref().is_none_or(|current| {
            compare_parts(
                &current.sort_values,
                &current.result,
                &current.source,
                current.source_record,
                &path_cursor.sort_values,
                &path_cursor.result,
                &path_cursor.source,
                path_cursor.source_record,
                directions,
            ) == Ordering::Less
        }) {
            after = Some(path_cursor);
        }
    }
    Ok(after)
}
