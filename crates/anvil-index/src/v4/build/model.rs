use crate::FIXED_INDEX_SEAL_WORKSPACE_BYTES;
use crate::IndexError;

use super::super::{
    DocValueCell, FieldId, INDEX_COMPONENT_BYTES, INDEX_TERM_BYTES, ObjectIdentity, ScalarValue,
    TERM_TYPE_BOOLEAN, TERM_TYPE_FIELD_PRESENCE, TERM_TYPE_HASHED_KEYWORD, TERM_TYPE_NULL,
    TERM_TYPE_NUMBER, TERM_TYPE_SIGNED, TERM_TYPE_STRING, TERM_TYPE_TEXT, TERM_TYPE_UNSIGNED,
    canonical_term_key,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuildLimits {
    total_resident_bytes: usize,
    maximum_buffered_source_bytes: usize,
    reserved_workspace_bytes: usize,
}

impl BuildLimits {
    #[cfg(test)]
    pub(crate) fn new(total_resident_bytes: usize) -> Result<Self, IndexError> {
        let maximum_buffered_source_bytes = total_resident_bytes
            .checked_sub(FIXED_INDEX_SEAL_WORKSPACE_BYTES)
            .ok_or(IndexError::OffsetOverflow)?;
        Self::with_resident_limits(
            total_resident_bytes,
            maximum_buffered_source_bytes,
            FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        )
    }

    pub fn with_resident_limits(
        total_resident_bytes: usize,
        maximum_buffered_source_bytes: usize,
        reserved_workspace_bytes: usize,
    ) -> Result<Self, IndexError> {
        let limits = Self {
            total_resident_bytes,
            maximum_buffered_source_bytes,
            reserved_workspace_bytes,
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        let admitted = self
            .maximum_buffered_source_bytes
            .checked_add(self.reserved_workspace_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.total_resident_bytes < 8 * INDEX_COMPONENT_BYTES
            || self.maximum_buffered_source_bytes == 0
            || self.reserved_workspace_bytes < FIXED_INDEX_SEAL_WORKSPACE_BYTES
            || admitted > self.total_resident_bytes
        {
            return Err(IndexError::InvalidDefinition(
                "native segment build limits are invalid".into(),
            ));
        }
        Ok(())
    }

    pub fn total_resident_bytes(&self) -> usize {
        self.total_resident_bytes
    }

    pub fn maximum_buffered_source_bytes(&self) -> usize {
        self.maximum_buffered_source_bytes
    }

    pub fn reserved_workspace_bytes(&self) -> usize {
        self.reserved_workspace_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedTerm {
    pub field_id: FieldId,
    /// Stable scalar/token type tag included in the canonical term key.
    pub term_type: u8,
    pub term: Vec<u8>,
    pub frequency: u32,
    pub positions: Vec<u32>,
}

impl ProjectedTerm {
    fn validate(&self) -> Result<(), IndexError> {
        let valid_width = match self.term_type {
            TERM_TYPE_NULL | TERM_TYPE_BOOLEAN | TERM_TYPE_FIELD_PRESENCE => self.term.len() == 1,
            TERM_TYPE_NUMBER | TERM_TYPE_SIGNED | TERM_TYPE_UNSIGNED => self.term.len() == 8,
            TERM_TYPE_STRING => {
                self.term.first() == Some(&0) && self.term.len() <= INDEX_TERM_BYTES + 1
            }
            TERM_TYPE_TEXT => self.term.len() <= INDEX_TERM_BYTES,
            TERM_TYPE_HASHED_KEYWORD => self.term.len() == 40,
            _ => false,
        };
        if self.term_type == 0
            || self.term.is_empty()
            || !valid_width
            || self.frequency == 0
            || !self.positions.is_empty() && self.positions.len() != self.frequency as usize
            || self.positions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(IndexError::InvalidDefinition(
                "projected term frequency or positions are invalid".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn canonical_key(&self) -> Result<Vec<u8>, IndexError> {
        self.validate()?;
        canonical_term_key(self.field_id, self.term_type, &self.term)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedDocValue {
    pub field_id: FieldId,
    pub multi_valued: bool,
    pub cell: DocValueCell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedPoint {
    pub field_id: FieldId,
    /// True when the source field exists, including an explicit JSON null.
    pub present: bool,
    /// At least one explicit JSON null occurred. This is separate from an
    /// empty multi-valued field.
    pub null: bool,
    pub values: Vec<ScalarValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedVector {
    pub field_id: FieldId,
    pub values: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedRecord {
    pub result_identity: Option<ObjectIdentity>,
    /// Canonical physical-order bytes. Empty means stable-identity order.
    pub order_key: Vec<u8>,
    pub terms: Vec<ProjectedTerm>,
    pub points: Vec<ProjectedPoint>,
    pub doc_values: Vec<ProjectedDocValue>,
    pub vectors: Vec<ProjectedVector>,
    /// Token/field length used by full-text norms.
    pub field_lengths: Vec<(FieldId, u32)>,
}

impl ProjectedRecord {
    pub(crate) fn validate(&self) -> Result<(), IndexError> {
        if let Some(result) = &self.result_identity {
            result.validate()?;
        }
        for term in &self.terms {
            term.validate()?;
        }
        if self.order_key.len() > INDEX_COMPONENT_BYTES
            || self
                .points
                .windows(2)
                .any(|pair| pair[0].field_id >= pair[1].field_id)
            || self
                .doc_values
                .windows(2)
                .any(|pair| pair[0].field_id >= pair[1].field_id)
            || self
                .field_lengths
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(IndexError::InvalidDefinition(
                "projected fields must be unique, ordered, and valid".into(),
            ));
        }
        for point in &self.points {
            if !point.present
                || point.values.iter().any(|value| {
                    !matches!(
                        value,
                        ScalarValue::Signed(_) | ScalarValue::Unsigned(_) | ScalarValue::Number(_)
                    )
                })
            {
                return Err(IndexError::InvalidDefinition(
                    "projected point values must be present and numeric".into(),
                ));
            }
        }
        for column in &self.doc_values {
            column.cell.validate(column.multi_valued)?;
        }
        let mut previous_vector = None;
        for vector in &self.vectors {
            if previous_vector.is_some_and(|field| field >= vector.field_id)
                || vector.values.is_empty()
                || vector.values.iter().any(|value| !value.is_finite())
            {
                return Err(IndexError::InvalidDefinition(
                    "projected vectors must be unique, ordered, non-empty, and finite".into(),
                ));
            }
            previous_vector = Some(vector.field_id);
        }
        Ok(())
    }

    pub(crate) fn retained_capacity_bytes(&self) -> Result<usize, IndexError> {
        let mut bytes = self.order_key.capacity();
        if let Some(result) = &self.result_identity {
            bytes = bytes
                .checked_add(result.path.capacity())
                .ok_or(IndexError::OffsetOverflow)?;
        }
        bytes = bytes
            .checked_add(
                self.terms
                    .capacity()
                    .checked_mul(std::mem::size_of::<ProjectedTerm>())
                    .ok_or(IndexError::OffsetOverflow)?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.points
                        .capacity()
                        .checked_mul(std::mem::size_of::<ProjectedPoint>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.doc_values
                        .capacity()
                        .checked_mul(std::mem::size_of::<ProjectedDocValue>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.vectors
                        .capacity()
                        .checked_mul(std::mem::size_of::<ProjectedVector>())?,
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.field_lengths
                        .capacity()
                        .checked_mul(std::mem::size_of::<(FieldId, u32)>())?,
                )
            })
            .ok_or(IndexError::OffsetOverflow)?;
        for term in &self.terms {
            bytes = bytes
                .checked_add(term.term.capacity())
                .and_then(|bytes| {
                    bytes.checked_add(
                        term.positions
                            .capacity()
                            .checked_mul(std::mem::size_of::<u32>())?,
                    )
                })
                .ok_or(IndexError::OffsetOverflow)?;
        }
        for point in &self.points {
            bytes = bytes
                .checked_add(
                    point
                        .values
                        .capacity()
                        .checked_mul(std::mem::size_of::<ScalarValue>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?;
        }
        for column in &self.doc_values {
            bytes = bytes
                .checked_add(
                    column
                        .cell
                        .values
                        .capacity()
                        .checked_mul(std::mem::size_of::<ScalarValue>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?;
            for value in &column.cell.values {
                if let super::super::ScalarValue::String(value) = value {
                    bytes = bytes
                        .checked_add(value.capacity())
                        .ok_or(IndexError::OffsetOverflow)?;
                }
            }
        }
        for vector in &self.vectors {
            bytes = bytes
                .checked_add(
                    vector
                        .values
                        .capacity()
                        .checked_mul(std::mem::size_of::<f32>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?;
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProjectedSource {
    pub source_identity: ObjectIdentity,
    pub records: Vec<ProjectedRecord>,
}

impl ProjectedSource {
    pub fn validate(&self) -> Result<(), IndexError> {
        self.source_identity.validate()?;
        if self.records.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "one source projection must contain at least one record".into(),
            ));
        }
        for record in &self.records {
            record.validate()?;
        }
        Ok(())
    }

    pub fn resident_bytes(&self) -> Result<usize, IndexError> {
        std::mem::size_of::<Self>()
            .checked_add(self.retained_dynamic_bytes()?)
            .ok_or(IndexError::OffsetOverflow)
    }

    pub(crate) fn retained_dynamic_bytes(&self) -> Result<usize, IndexError> {
        self.records.iter().try_fold(
            self.source_identity
                .path
                .capacity()
                .checked_add(
                    self.records
                        .capacity()
                        .checked_mul(std::mem::size_of::<ProjectedRecord>())
                        .ok_or(IndexError::OffsetOverflow)?,
                )
                .ok_or(IndexError::OffsetOverflow)?,
            |bytes, record| {
                bytes
                    .checked_add(record.retained_capacity_bytes()?)
                    .ok_or(IndexError::OffsetOverflow)
            },
        )
    }
}

#[derive(Debug, PartialEq)]
pub enum SourcePush {
    Accepted,
    Full(ProjectedSource),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_build_limits_reject_hidden_oversubscription() {
        let total = 64 * 1024 * 1024;
        let limits = BuildLimits::with_resident_limits(
            total,
            24 * 1024 * 1024,
            FIXED_INDEX_SEAL_WORKSPACE_BYTES,
        )
        .unwrap();
        assert_eq!(limits.total_resident_bytes(), total);
        assert_eq!(limits.maximum_buffered_source_bytes(), 24 * 1024 * 1024);
        assert_eq!(
            limits.reserved_workspace_bytes(),
            FIXED_INDEX_SEAL_WORKSPACE_BYTES
        );
        assert!(
            BuildLimits::with_resident_limits(
                total,
                56 * 1024 * 1024,
                FIXED_INDEX_SEAL_WORKSPACE_BYTES
            )
            .is_err()
        );
        assert!(
            BuildLimits::with_resident_limits(total, 24 * 1024 * 1024, 16 * 1024 * 1024).is_err()
        );
    }

    #[test]
    fn projected_keyword_accepts_the_exact_raw_length_boundary() {
        let (term_type, term) =
            super::super::super::scalar_term(&ScalarValue::String("x".repeat(INDEX_TERM_BYTES)))
                .unwrap();
        assert_eq!(term.len(), INDEX_TERM_BYTES + 1);
        let projected = ProjectedTerm {
            field_id: FieldId::new(1),
            term_type,
            term,
            frequency: 1,
            positions: Vec::new(),
        };
        assert!(projected.validate().is_ok());
        assert!(projected.canonical_key().is_ok());
    }
}
