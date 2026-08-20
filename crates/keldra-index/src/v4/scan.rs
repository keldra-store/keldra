use std::collections::{BTreeMap, BTreeSet};

use crate::IndexError;

use super::columns::{DocValueCell, ScalarValue};
use super::model::{INDEX_DECODE_BYTES, INDEX_ROUTING_KEY_BYTES, INDEX_TERM_BYTES};
use super::schema::{FieldId, OrderField};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct PredicateId(u32);

impl PredicateId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Equal {
        id: PredicateId,
        field_id: FieldId,
        value: ScalarValue,
    },
    In {
        id: PredicateId,
        field_id: FieldId,
        values: Vec<ScalarValue>,
    },
    Prefix {
        id: PredicateId,
        field_id: FieldId,
        prefix: String,
    },
    Range {
        id: PredicateId,
        field_id: FieldId,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
    },
    Exists {
        id: PredicateId,
        field_id: FieldId,
    },
    FullText {
        id: PredicateId,
        field_id: FieldId,
        text: String,
    },
    Phrase {
        id: PredicateId,
        field_id: FieldId,
        text: String,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.collect_ids(&mut BTreeSet::new())
    }

    pub fn leaf_id(&self) -> Option<PredicateId> {
        match self {
            Self::Equal { id, .. }
            | Self::In { id, .. }
            | Self::Prefix { id, .. }
            | Self::Range { id, .. }
            | Self::Exists { id, .. }
            | Self::FullText { id, .. }
            | Self::Phrase { id, .. } => Some(*id),
            Self::And(_) | Self::Or(_) | Self::Not(_) => None,
        }
    }

    fn collect_ids(&self, output: &mut BTreeSet<PredicateId>) -> Result<(), IndexError> {
        if let Some(id) = self.leaf_id()
            && !output.insert(id)
        {
            return Err(IndexError::InvalidQuery(
                "predicate IDs must be unique within one scan".into(),
            ));
        }
        match self {
            Self::In { values, .. } if values.is_empty() => Err(IndexError::InvalidQuery(
                "IN predicate requires at least one value".into(),
            )),
            Self::Prefix { prefix, .. } if prefix.is_empty() || prefix.len() > INDEX_TERM_BYTES => {
                Err(IndexError::InvalidQuery(
                    "prefix predicate is empty or too long".into(),
                ))
            }
            Self::Range { lower, upper, .. } if lower.is_none() && upper.is_none() => Err(
                IndexError::InvalidQuery("range predicate requires a bound".into()),
            ),
            Self::FullText { text, .. } | Self::Phrase { text, .. } if text.trim().is_empty() => {
                Err(IndexError::InvalidQuery(
                    "full-text predicate must not be empty".into(),
                ))
            }
            Self::And(children) | Self::Or(children) => {
                if children.is_empty() {
                    return Err(IndexError::InvalidQuery(
                        "Boolean predicate requires a child".into(),
                    ));
                }
                for child in children {
                    child.collect_ids(output)?;
                }
                Ok(())
            }
            Self::Not(child) => child.collect_ids(output),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationSelection {
    pub index_id: u64,
    pub definition_version: u64,
    pub generation: u64,
    pub schema_fingerprint: [u8; 32],
    pub manifest_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationScope {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub path_prefix: String,
    pub zanzibar_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SortValue {
    /// Missing has definition-fixed placement and is not JSON null.
    Missing,
    Value(ScalarValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateIdentity {
    pub tenant_id: u64,
    pub bucket_id: u64,
    pub path: String,
    pub object_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SortCursor {
    pub values: Vec<SortValue>,
    pub identity: CandidateIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanRequest {
    pub generation: GenerationSelection,
    pub authorization_scope: AuthorizationScope,
    pub required_doc_value_field_ids: Vec<FieldId>,
    pub predicate_expression: Option<Predicate>,
    pub required_order: Vec<OrderField>,
    pub after: Option<SortCursor>,
    pub limit: u32,
    pub target_batch_bytes: u32,
}

impl ScanRequest {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.generation.index_id == 0
            || self.generation.definition_version == 0
            || self.generation.generation == 0
            || self.generation.schema_fingerprint == [0; 32]
            || self.generation.manifest_hash == [0; 32]
            || self.authorization_scope.tenant_id == 0
            || self.authorization_scope.bucket_id == 0
            || self.authorization_scope.zanzibar_revision == 0
            || self.authorization_scope.path_prefix.len() > INDEX_ROUTING_KEY_BYTES
            || self.authorization_scope.path_prefix.contains('\0')
            || self.limit == 0
            || self.target_batch_bytes == 0
            || self.target_batch_bytes as usize > INDEX_DECODE_BYTES
        {
            return Err(IndexError::InvalidQuery(
                "invalid scan request bounds".into(),
            ));
        }
        if self
            .required_doc_value_field_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != self.required_doc_value_field_ids.len()
        {
            return Err(IndexError::InvalidQuery(
                "scan projection FieldIds must be unique".into(),
            ));
        }
        if let Some(predicate) = &self.predicate_expression {
            predicate.validate()?;
        }
        if let Some(cursor) = &self.after
            && (cursor.values.len() != self.required_order.len()
                || cursor.identity.tenant_id != self.authorization_scope.tenant_id
                || cursor.identity.bucket_id != self.authorization_scope.bucket_id
                || cursor.identity.object_version == 0
                || cursor.identity.path.len() > INDEX_ROUTING_KEY_BYTES
                || cursor.identity.path.contains('\0'))
        {
            return Err(IndexError::InvalidQuery(
                "scan cursor does not match its order or scope".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicatePushdown {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanCapabilities {
    pub predicate_pushdown: BTreeMap<PredicateId, PredicatePushdown>,
    pub residual_expression: Option<Predicate>,
    pub per_partition_order: Vec<OrderField>,
    pub globally_merged_order: bool,
    pub partitions: u32,
    pub estimated_documents: u64,
    pub estimated_bytes: u64,
    pub estimated_cached_bytes: u64,
    pub estimated_remote_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanColumn {
    pub field_id: FieldId,
    pub cells: Vec<DocValueCell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanBatch {
    pub identities: Vec<CandidateIdentity>,
    pub columns: Vec<ScanColumn>,
    pub cursors: Vec<SortCursor>,
    pub encoded_bytes: u64,
}

impl ScanBatch {
    pub fn validate(&self) -> Result<(), IndexError> {
        let rows = self.identities.len();
        if rows == 0
            || self.encoded_bytes as usize > INDEX_DECODE_BYTES
            || self.columns.iter().any(|column| column.cells.len() != rows)
            || !self.cursors.is_empty() && self.cursors.len() != rows
            || self
                .columns
                .iter()
                .map(|column| column.field_id)
                .collect::<BTreeSet<_>>()
                .len()
                != self.columns.len()
            || self.identities.iter().any(|identity| {
                identity.tenant_id == 0
                    || identity.bucket_id == 0
                    || identity.object_version == 0
                    || identity.path.is_empty()
                    || identity.path.len() > INDEX_ROUTING_KEY_BYTES
                    || identity.path.contains('\0')
            })
        {
            return Err(IndexError::InvalidFormat("invalid scan batch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_request_local_predicate_ids_are_rejected() {
        let leaf = Predicate::Exists {
            id: PredicateId::new(1),
            field_id: FieldId::new(2),
        };
        let request = ScanRequest {
            generation: GenerationSelection {
                index_id: 1,
                definition_version: 2,
                generation: 3,
                schema_fingerprint: [4; 32],
                manifest_hash: [5; 32],
            },
            authorization_scope: AuthorizationScope {
                tenant_id: 6,
                bucket_id: 7,
                path_prefix: "/".into(),
                zanzibar_revision: 8,
            },
            required_doc_value_field_ids: vec![FieldId::new(0)],
            predicate_expression: Some(Predicate::And(vec![leaf.clone(), leaf])),
            required_order: vec![],
            after: None,
            limit: 10,
            target_batch_bytes: 4096,
        };
        assert!(request.validate().is_err());
    }

    #[test]
    fn batch_requires_aligned_bounded_columns() {
        let batch = ScanBatch {
            identities: vec![CandidateIdentity {
                tenant_id: 1,
                bucket_id: 2,
                path: "a".into(),
                object_version: 3,
            }],
            columns: vec![ScanColumn {
                field_id: FieldId::new(0),
                cells: vec![],
            }],
            cursors: vec![],
            encoded_bytes: 0,
        };
        assert!(batch.validate().is_err());
    }
}
