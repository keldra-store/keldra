use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;

use super::*;
use crate::v4::{
    AggregateOperation, AggregateResult, FacetBucket, FacetResult, FieldType, ScalarValue,
};

pub(super) struct ComputationState {
    facets: Vec<FacetState>,
    aggregates: Vec<AggregateState>,
    retained_bytes: usize,
    maximum_bytes: usize,
}

struct FacetState {
    field_id: FieldId,
    limit: u32,
    counts: BTreeMap<ScalarValue, u64>,
}

struct AggregateState {
    field_id: FieldId,
    operation: AggregateOperation,
    field_type: FieldType,
    count: u64,
    minimum: Option<ScalarValue>,
    maximum: Option<ScalarValue>,
    signed_sum: i128,
    unsigned_sum: u128,
    float_sum: f64,
}

impl ComputationState {
    pub(super) fn new(request: &NativeQueryRequest, maximum_bytes: usize) -> Result<Self, IndexError> {
        let facets = request
            .facets
            .iter()
            .map(|request| FacetState {
                field_id: request.field_id,
                limit: request.limit,
                counts: BTreeMap::new(),
            })
            .collect();
        let schema = &request.schema;
        let aggregates = request
            .aggregates
            .iter()
            .map(|request| {
                let field_type = schema
                    .fields
                    .get(request.field_id.get() as usize)
                    .map(|field| field.field_type)
                    .expect("request validation established aggregate field");
                AggregateState {
                    field_id: request.field_id,
                    operation: request.operation,
                    field_type,
                    count: 0,
                    minimum: None,
                    maximum: None,
                    signed_sum: 0,
                    unsigned_sum: 0,
                    float_sum: 0.0,
                }
            })
            .collect();
        let retained_bytes = facets
            .len()
            .checked_mul(std::mem::size_of::<FacetState>())
            .and_then(|bytes| {
                bytes.checked_add(
                    aggregates
                        .len()
                        .checked_mul(std::mem::size_of::<AggregateState>())?,
                )
            })
            .ok_or(IndexError::OffsetOverflow)?;
        if retained_bytes > maximum_bytes {
            return Err(IndexError::ResourceLimit {
                needed: retained_bytes,
                limit: maximum_bytes,
            });
        }
        Ok(Self {
            facets,
            aggregates,
            retained_bytes,
            maximum_bytes,
        })
    }

    pub(super) async fn observe<D: ArtifactDirectoryRead>(
        &mut self,
        values: &mut SegmentValues<'_, D>,
        doc_id: DocId,
    ) -> Result<(), IndexError> {
        for facet in &mut self.facets {
            let cell = values.doc_value(facet.field_id, doc_id).await?;
            let mut distinct = BTreeSet::new();
            if cell.present && cell.null {
                distinct.insert(ScalarValue::Null);
            }
            distinct.extend(cell.values);
            for value in distinct {
                if !facet.counts.contains_key(&value) {
                    let additional = scalar_owned_bytes(&value)?
                        .checked_add(std::mem::size_of::<(ScalarValue, u64)>() + 48)
                        .ok_or(IndexError::OffsetOverflow)?;
                    let needed = self
                        .retained_bytes
                        .checked_add(additional)
                        .ok_or(IndexError::OffsetOverflow)?;
                    if needed > self.maximum_bytes {
                        return Err(IndexError::ResourceLimit {
                            needed,
                            limit: self.maximum_bytes,
                        });
                    }
                    self.retained_bytes = needed;
                }
                let count = facet.counts.entry(value).or_default();
                *count = count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
            }
        }
        for aggregate in &mut self.aggregates {
            let cell = values.doc_value(aggregate.field_id, doc_id).await?;
            for value in cell.values {
                aggregate.observe(value)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<(Vec<FacetResult>, Vec<AggregateResult>), IndexError> {
        let facets = self
            .facets
            .into_iter()
            .map(|facet| {
                let mut buckets = facet
                    .counts
                    .into_iter()
                    .map(|(value, count)| FacetBucket { value, count })
                    .collect::<Vec<_>>();
                buckets.sort_by(|left, right| {
                    right
                        .count
                        .cmp(&left.count)
                        .then_with(|| left.value.cmp(&right.value))
                });
                buckets.truncate(facet.limit as usize);
                FacetResult {
                    field_id: facet.field_id,
                    buckets,
                }
            })
            .collect();
        let aggregates = self
            .aggregates
            .into_iter()
            .map(AggregateState::finish)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((facets, aggregates))
    }
}

impl AggregateState {
    fn observe(&mut self, value: ScalarValue) -> Result<(), IndexError> {
        self.count = self.count.checked_add(1).ok_or(IndexError::OffsetOverflow)?;
        if self.minimum.as_ref().is_none_or(|current| value < *current) {
            self.minimum = Some(value.clone());
        }
        if self.maximum.as_ref().is_none_or(|current| value > *current) {
            self.maximum = Some(value.clone());
        }
        match value {
            ScalarValue::Signed(value) => {
                self.signed_sum = self
                    .signed_sum
                    .checked_add(i128::from(value))
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            ScalarValue::Unsigned(value) => {
                self.unsigned_sum = self
                    .unsigned_sum
                    .checked_add(u128::from(value))
                    .ok_or(IndexError::OffsetOverflow)?;
            }
            ScalarValue::Number(bits) => {
                self.float_sum += f64::from_bits(bits);
                if !self.float_sum.is_finite() {
                    return Err(IndexError::InvalidQuery(
                        "floating aggregate produced a non-finite result".into(),
                    ));
                }
            }
            _ => {
                return Err(IndexError::InvalidFormat(
                    "aggregate doc value is not numeric",
                ));
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<AggregateResult, IndexError> {
        let value = match self.operation {
            AggregateOperation::Count => Some(ScalarValue::Unsigned(self.count)),
            AggregateOperation::Minimum => self.minimum,
            AggregateOperation::Maximum => self.maximum,
            AggregateOperation::Sum if self.count == 0 => None,
            AggregateOperation::Sum => Some(self.sum_value()?),
            AggregateOperation::Average if self.count == 0 => None,
            AggregateOperation::Average => {
                let sum = match self.field_type {
                    FieldType::SignedInteger => self.signed_sum as f64,
                    FieldType::UnsignedInteger => self.unsigned_sum as f64,
                    FieldType::Float => self.float_sum,
                    _ => return Err(IndexError::InvalidFormat("aggregate field is not numeric")),
                };
                ScalarValue::number(sum / self.count as f64).map(Some)?
            }
        };
        Ok(AggregateResult {
            field_id: self.field_id,
            operation: self.operation,
            value,
            contributing_count: self.count,
        })
    }

    fn sum_value(&self) -> Result<ScalarValue, IndexError> {
        match self.field_type {
            FieldType::SignedInteger => i64::try_from(self.signed_sum)
                .map(ScalarValue::Signed)
                .map_err(|_| IndexError::InvalidQuery("signed aggregate sum overflow".into())),
            FieldType::UnsignedInteger => u64::try_from(self.unsigned_sum)
                .map(ScalarValue::Unsigned)
                .map_err(|_| IndexError::InvalidQuery("unsigned aggregate sum overflow".into())),
            FieldType::Float => ScalarValue::number(self.float_sum),
            _ => Err(IndexError::InvalidFormat("aggregate field is not numeric")),
        }
    }
}

fn scalar_owned_bytes(value: &ScalarValue) -> Result<usize, IndexError> {
    Ok(match value {
        ScalarValue::String(value) => value.capacity(),
        _ => 0,
    })
}
