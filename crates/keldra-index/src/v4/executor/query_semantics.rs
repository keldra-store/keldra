use std::sync::Arc;

use crate::IndexError;

use super::super::{
    IndexSemantics, NativeQuery, NativeQueryRequest, OrderDirection, OrderField,
    VectorNormalization,
};

pub(super) fn query_directions(request: &NativeQueryRequest) -> Vec<OrderDirection> {
    match &request.query {
        NativeQuery::Filter { order, .. } => order.iter().map(|value| value.direction).collect(),
        NativeQuery::FullText { .. } | NativeQuery::Vector { .. } | NativeQuery::Hybrid { .. } => {
            vec![OrderDirection::Descending]
        }
        NativeQuery::Path { .. } | NativeQuery::GitSource { .. } | NativeQuery::Tensor { .. } => {
            vec![OrderDirection::Ascending]
        }
    }
}

pub(super) fn text_scoring_active(query: &NativeQuery) -> bool {
    match query {
        NativeQuery::FullText { .. } => true,
        NativeQuery::Hybrid { text, .. } => !text.trim().is_empty(),
        _ => false,
    }
}

pub(super) fn scoring_query_vector(
    schema: &super::super::Schema,
    query: &NativeQuery,
) -> Result<Option<Arc<[f32]>>, IndexError> {
    let (values, normalization) = match (query, &schema.semantics) {
        (NativeQuery::Vector { values }, IndexSemantics::Vector { normalization, .. }) => {
            (values.as_slice(), *normalization)
        }
        (NativeQuery::Hybrid { vector, .. }, IndexSemantics::Hybrid { normalization, .. })
            if !vector.is_empty() =>
        {
            (vector.as_slice(), *normalization)
        }
        (NativeQuery::Hybrid { vector, .. }, IndexSemantics::Hybrid { .. })
            if vector.is_empty() =>
        {
            return Ok(None);
        }
        (NativeQuery::Vector { .. }, _) | (NativeQuery::Hybrid { .. }, _) => {
            return Err(IndexError::InvalidQuery(
                "vector query does not match schema semantics".into(),
            ));
        }
        _ => return Ok(None),
    };
    let mut values = Arc::<[f32]>::from(values);
    if normalization == VectorNormalization::L2 {
        // Match construction exactly: projections use an f32 sum and norm,
        // then divide every coordinate by that same norm.
        let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
        if norm == 0.0 || !norm.is_finite() {
            return Err(IndexError::InvalidQuery(
                "L2-normalized query vector requires a finite non-zero norm".into(),
            ));
        }
        Arc::get_mut(&mut values)
            .expect("new query-vector allocation is uniquely owned")
            .iter_mut()
            .for_each(|value| *value /= norm);
    }
    Ok(Some(values))
}

pub(super) fn physical_order(request: &NativeQueryRequest) -> Option<&[OrderField]> {
    match &request.query {
        NativeQuery::Path { .. } => Some(&[]),
        NativeQuery::Filter { order, .. }
            if !order.is_empty() && *order == request.schema.physical_order =>
        {
            Some(order)
        }
        _ => None,
    }
}
