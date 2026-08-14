use std::collections::BTreeSet;
use std::future::Future;

use crate::IndexError;

use super::codec::{Decoder, Encoder};
use super::{
    INDEX_COMPONENT_BYTES, INDEX_ROUTING_KEY_BYTES, ObjectIdentity, OrderField, Predicate,
    ScalarValue, Schema, SegmentDescriptor, SortValue,
};

const CURSOR_MAGIC: &[u8; 8] = b"ANVLQCR4";
const CURSOR_CODEC_VERSION: u16 = 2;

#[derive(Clone, Debug, PartialEq)]
pub enum NativeQuery {
    Path {
        prefix: String,
        start_after: Option<String>,
    },
    Filter {
        predicate: Option<Predicate>,
        order: Vec<OrderField>,
    },
    FullText {
        text: String,
        phrase: bool,
    },
    Vector {
        values: Vec<f32>,
    },
    Hybrid {
        text: String,
        vector: Vec<f32>,
    },
    GitSource {
        repository_id: String,
        commit_id: String,
        tree_path: String,
        prefix: bool,
    },
    Tensor {
        model_id: String,
        tensor_name: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeQueryCursor {
    pub sort_values: Vec<SortValue>,
    pub result: ObjectIdentity,
    pub source: ObjectIdentity,
    pub source_record: u32,
}

impl NativeQueryCursor {
    /// Stable opaque engine position embedded by the outer generation-bound
    /// page token. It contains no DocId or Rust/serde representation.
    pub fn encode(&self) -> Result<Vec<u8>, IndexError> {
        self.result.validate()?;
        self.source.validate()?;
        let mut out = Encoder::default();
        out.raw(CURSOR_MAGIC);
        out.u16(CURSOR_CODEC_VERSION);
        out.u16(0);
        out.usize_u32(self.sort_values.len())?;
        for value in &self.sort_values {
            encode_sort_value(&mut out, value)?;
        }
        out.string(&self.result.path)?;
        out.u64(self.result.version);
        out.string(&self.source.path)?;
        out.u64(self.source.version);
        out.u32(self.source_record);
        let bytes = out.finish();
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: bytes.len(),
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.take(8)? != CURSOR_MAGIC
            || input.u16()? != CURSOR_CODEC_VERSION
            || input.u16()? != 0
        {
            return Err(IndexError::InvalidFormat("format-v4 query cursor header"));
        }
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        if count > INDEX_COMPONENT_BYTES {
            return Err(IndexError::InvalidFormat(
                "format-v4 query cursor value count",
            ));
        }
        input.claim(
            count
                .checked_mul(std::mem::size_of::<SortValue>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut sort_values = Vec::with_capacity(count);
        for _ in 0..count {
            sort_values.push(decode_sort_value(&mut input)?);
        }
        let path = input.string()?;
        let result = ObjectIdentity {
            path,
            version: input.u64()?,
        };
        let source = ObjectIdentity {
            path: input.string()?,
            version: input.u64()?,
        };
        let source_record = input.u32()?;
        input.finish()?;
        result.validate()?;
        source.validate()?;
        Ok(Self {
            sort_values,
            result,
            source,
            source_record,
        })
    }
}

fn encode_sort_value(out: &mut Encoder, value: &SortValue) -> Result<(), IndexError> {
    match value {
        SortValue::Missing => out.u8(0),
        SortValue::Value(ScalarValue::Null) => out.u8(1),
        SortValue::Value(ScalarValue::Boolean(false)) => out.u8(2),
        SortValue::Value(ScalarValue::Boolean(true)) => out.u8(3),
        SortValue::Value(ScalarValue::Number(bits)) => {
            require_canonical_number(*bits)?;
            out.u8(4);
            out.u64(*bits);
        }
        SortValue::Value(ScalarValue::Unsigned(value)) => {
            out.u8(5);
            out.u64(*value);
        }
        SortValue::Value(ScalarValue::String(value)) => {
            if value.len() > INDEX_ROUTING_KEY_BYTES {
                return Err(IndexError::InvalidQuery(
                    "query cursor string exceeds the routing-key bound".into(),
                ));
            }
            out.u8(6);
            out.string(value)?;
        }
    }
    Ok(())
}

fn decode_sort_value(input: &mut Decoder<'_>) -> Result<SortValue, IndexError> {
    Ok(match input.u8()? {
        0 => SortValue::Missing,
        1 => SortValue::Value(ScalarValue::Null),
        2 => SortValue::Value(ScalarValue::Boolean(false)),
        3 => SortValue::Value(ScalarValue::Boolean(true)),
        4 => {
            let bits = input.u64()?;
            require_canonical_number(bits)?;
            SortValue::Value(ScalarValue::Number(bits))
        }
        5 => SortValue::Value(ScalarValue::Unsigned(input.u64()?)),
        6 => {
            let value = input.string()?;
            if value.len() > INDEX_ROUTING_KEY_BYTES {
                return Err(IndexError::InvalidFormat(
                    "format-v4 query cursor string bound",
                ));
            }
            SortValue::Value(ScalarValue::String(value))
        }
        _ => {
            return Err(IndexError::InvalidFormat(
                "format-v4 query cursor value tag",
            ));
        }
    })
}

fn require_canonical_number(bits: u64) -> Result<(), IndexError> {
    let value = f64::from_bits(bits);
    if ScalarValue::number(value)? != ScalarValue::Number(bits) {
        return Err(IndexError::InvalidFormat("format-v4 query cursor number"));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct NativeQueryRequest {
    pub schema: Schema,
    pub segments: Vec<SegmentDescriptor>,
    pub query: NativeQuery,
    pub after: Option<NativeQueryCursor>,
    pub limit: u32,
    /// Revision established by query admission or the validated page token.
    pub authorization_revision: u64,
}

impl NativeQueryRequest {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.limit == 0
            || self.authorization_revision == 0
            || self
                .segments
                .windows(2)
                .any(|pair| pair[0].identity.segment_id >= pair[1].identity.segment_id)
        {
            return Err(IndexError::InvalidQuery(
                "native query requires ordered segments, a limit, and authorization evidence"
                    .into(),
            ));
        }
        self.schema.validate()?;
        self.validate_query_shape()?;
        let Some(first) = self.segments.first().map(|segment| segment.identity) else {
            // A complete checkpoint-only generation is a valid empty index.
            return Ok(());
        };
        if self.schema.fingerprint()? != first.schema_fingerprint {
            return Err(IndexError::InvalidQuery(
                "native query schema does not match its segments".into(),
            ));
        }
        for segment in &self.segments {
            segment.validate()?;
            if segment.identity.index_id != first.index_id
                || segment.identity.definition_version != first.definition_version
                || segment.identity.schema_fingerprint != first.schema_fingerprint
            {
                return Err(IndexError::InvalidQuery(
                    "native query segments do not belong to one generation schema".into(),
                ));
            }
        }
        if let Some(after) = &self.after {
            after.result.validate()?;
            after.source.validate()?;
            if after.sort_values.len() != self.expected_cursor_values() {
                return Err(IndexError::InvalidQuery(
                    "native query cursor does not match query order".into(),
                ));
            }
        }
        Ok(())
    }

    /// Conservative checked reservation for the default native executor.
    /// Custom executor limits should use `NativeQueryExecutor::working_memory_bytes`.
    pub fn working_memory_bytes(&self) -> Result<usize, IndexError> {
        super::executor::estimate_working_memory(self, super::NativeQueryLimits::default())
    }

    fn expected_cursor_values(&self) -> usize {
        match &self.query {
            NativeQuery::Filter { order, .. } => order.len(),
            NativeQuery::Path { .. }
            | NativeQuery::FullText { .. }
            | NativeQuery::Vector { .. }
            | NativeQuery::Hybrid { .. }
            | NativeQuery::GitSource { .. }
            | NativeQuery::Tensor { .. } => 1,
        }
    }

    fn validate_query_shape(&self) -> Result<(), IndexError> {
        match &self.query {
            NativeQuery::Path {
                prefix,
                start_after,
            } => {
                validate_query_string(prefix, true)?;
                if let Some(value) = start_after {
                    validate_query_string(value, false)?;
                }
            }
            NativeQuery::Filter { predicate, order } => {
                if let Some(predicate) = predicate {
                    predicate.validate()?;
                }
                let mut fields = BTreeSet::new();
                for ordered in order {
                    let field = self
                        .schema
                        .fields
                        .get(ordered.field_id.get() as usize)
                        .ok_or_else(|| IndexError::InvalidQuery("unknown order field".into()))?;
                    if field.id != ordered.field_id
                        || field.cardinality != super::Cardinality::Single
                        || !field
                            .components
                            .contains(super::FieldComponents::FAST_COLUMN)
                        || !fields.insert(field.id)
                    {
                        return Err(IndexError::InvalidQuery(
                            "query order requires unique single-valued fast columns".into(),
                        ));
                    }
                }
            }
            NativeQuery::FullText { text, .. } if text.trim().is_empty() => {
                return Err(IndexError::InvalidQuery(
                    "full-text query must not be empty".into(),
                ));
            }
            NativeQuery::Vector { values } => validate_finite_vector(values)?,
            NativeQuery::Hybrid { text, vector } => {
                if text.trim().is_empty() && vector.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "hybrid query requires text or vector input".into(),
                    ));
                }
                validate_finite_vector(vector)?;
            }
            NativeQuery::GitSource {
                repository_id,
                commit_id,
                tree_path,
                ..
            } => {
                validate_query_string(repository_id, false)?;
                validate_query_string(commit_id, false)?;
                validate_query_string(tree_path, true)?;
            }
            NativeQuery::Tensor {
                model_id,
                tensor_name,
            } => {
                validate_query_string(model_id, false)?;
                validate_query_string(tensor_name, false)?;
            }
            _ => {}
        }
        Ok(())
    }
}

fn validate_query_string(value: &str, allow_empty: bool) -> Result<(), IndexError> {
    if (!allow_empty && value.is_empty())
        || value.len() > INDEX_ROUTING_KEY_BYTES
        || value.contains('\0')
    {
        return Err(IndexError::InvalidQuery(
            "native query string is empty, too long, or contains NUL".into(),
        ));
    }
    Ok(())
}

fn validate_finite_vector(values: &[f32]) -> Result<(), IndexError> {
    if values.iter().any(|value| !value.is_finite()) {
        return Err(IndexError::InvalidQuery(
            "native query vector contains a non-finite value".into(),
        ));
    }
    Ok(())
}

/// Stable identities supplied to the mandatory authorization/exact-current
/// boundary before a candidate can enter a top-K heap or returned page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateReference {
    pub source: ObjectIdentity,
    pub result: ObjectIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateGateEvidence {
    pub visible: Vec<bool>,
    pub authorization_revision: u64,
    /// Candidates rejected by authorization before exact-current evaluation.
    pub denied: u64,
    /// Authorized candidates rejected because their source is no longer current.
    pub stale: u64,
}

/// Required query-execution gate. The engine has no ungated public execution
/// entry point: callers must provide Zanzibar and exact-current evaluation.
pub trait CandidateGate: Send + Sync {
    type Error;

    fn evaluate(
        &self,
        candidates: &[CandidateReference],
    ) -> impl Future<Output = Result<CandidateGateEvidence, Self::Error>> + Send;
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeQueryHit {
    pub source: ObjectIdentity,
    pub result: ObjectIdentity,
    pub score: Option<f32>,
    pub fields_json: Vec<u8>,
    pub cursor: NativeQueryCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeQueryPage {
    pub hits: Vec<NativeQueryHit>,
    pub next: Option<NativeQueryCursor>,
    pub authorization_revision: u64,
    pub statistics: super::NativeQueryStatistics,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> NativeQueryCursor {
        NativeQueryCursor {
            sort_values: vec![
                SortValue::Missing,
                SortValue::Value(ScalarValue::Null),
                SortValue::Value(ScalarValue::Boolean(true)),
                SortValue::Value(ScalarValue::number(1.5).unwrap()),
                SortValue::Value(ScalarValue::Unsigned(9)),
                SortValue::Value(ScalarValue::String("alpha".into())),
            ],
            result: ObjectIdentity {
                path: "docs/a".into(),
                version: 7,
            },
            source: ObjectIdentity {
                path: "manifests/a".into(),
                version: 11,
            },
            source_record: 4,
        }
    }

    #[test]
    fn cursor_round_trip_is_explicit_and_checked() {
        let value = cursor();
        let bytes = value.encode().unwrap();
        assert_eq!(&bytes[..8], CURSOR_MAGIC);
        assert_eq!(NativeQueryCursor::decode(&bytes).unwrap(), value);
    }

    #[test]
    fn cursor_rejects_trailing_malformed_and_noncanonical_numbers() {
        let mut trailing = cursor().encode().unwrap();
        trailing.push(0);
        assert!(NativeQueryCursor::decode(&trailing).is_err());

        let invalid = NativeQueryCursor {
            sort_values: vec![SortValue::Value(ScalarValue::Number((-0.0_f64).to_bits()))],
            result: ObjectIdentity {
                path: "docs/a".into(),
                version: 7,
            },
            source: ObjectIdentity {
                path: "manifests/a".into(),
                version: 11,
            },
            source_record: 4,
        };
        assert!(invalid.encode().is_err());

        let mut malformed = cursor().encode().unwrap();
        malformed[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(NativeQueryCursor::decode(&malformed).is_err());
    }
}
