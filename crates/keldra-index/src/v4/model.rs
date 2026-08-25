use crate::IndexError;

pub const INDEX_FORMAT_VERSION: u16 = 4;
pub const INDEX_COMPONENT_BYTES: usize = 512 * 1024;
pub const INDEX_DECODE_BYTES: usize = 4 * 1024 * 1024;
pub const INDEX_ARTIFACT_PACK_BYTES: usize = 16 * 1024 * 1024;
pub const INDEX_ROUTING_KEY_BYTES: usize = 4096;
pub const INDEX_TERM_BYTES: usize = 32_766;
/// FieldId, term type, and the largest bounded keyword representation.
pub(crate) const INDEX_TERM_ROUTING_BYTES: usize = INDEX_TERM_BYTES + 6;
pub const INDEX_ROUTING_FANOUT: usize = 32;
pub const INDEX_ROUTING_HEIGHT: u8 = 8;
pub const INDEX_COMMIT_SEGMENTS: usize = 4_096;

/// Stable format-v4 component identifiers.
///
/// Values are part of the persistent format. Additions use new values; an
/// existing value is never repurposed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ComponentKind(u16);

impl ComponentKind {
    pub const SEGMENT_ROOT: Self = Self(1);
    pub const ROUTING_NODE: Self = Self(2);
    pub const IDENTITY_TABLE: Self = Self(3);
    pub const LIVE_MASK: Self = Self(4);
    pub const PATH_LOCATOR: Self = Self(5);
    pub const TERM_DICTIONARY: Self = Self(6);
    pub const POSTINGS: Self = Self(7);
    pub const POINTS: Self = Self(8);
    pub const DOC_VALUES: Self = Self(9);
    pub const POSITIONS: Self = Self(10);
    pub const NORMS: Self = Self(11);
    pub const VECTORS: Self = Self(12);
    pub const SCORING_STATISTICS: Self = Self(13);

    pub fn new(value: u16) -> Result<Self, IndexError> {
        match value {
            1 => Ok(Self::SEGMENT_ROOT),
            2 => Ok(Self::ROUTING_NODE),
            3 => Ok(Self::IDENTITY_TABLE),
            4 => Ok(Self::LIVE_MASK),
            5 => Ok(Self::PATH_LOCATOR),
            6 => Ok(Self::TERM_DICTIONARY),
            7 => Ok(Self::POSTINGS),
            8 => Ok(Self::POINTS),
            9 => Ok(Self::DOC_VALUES),
            10 => Ok(Self::POSITIONS),
            11 => Ok(Self::NORMS),
            12 => Ok(Self::VECTORS),
            13 => Ok(Self::SCORING_STATISTICS),
            _ => Err(IndexError::InvalidDefinition(
                "unknown format-v4 component kind".into(),
            )),
        }
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct DocId(u32);

impl DocId {
    pub const MIN: Self = Self(0);

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self, IndexError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(IndexError::ResourceLimit {
                needed: usize::MAX,
                limit: u32::MAX as usize,
            })
    }
}

/// Identity shared by every immutable component of one segment.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SegmentIdentity {
    pub index_id: u64,
    pub definition_version: u64,
    pub schema_fingerprint: [u8; 32],
    pub segment_id: u64,
}

impl SegmentIdentity {
    pub fn new(
        index_id: u64,
        definition_version: u64,
        schema_fingerprint: [u8; 32],
        segment_id: u64,
    ) -> Result<Self, IndexError> {
        let value = Self {
            index_id,
            definition_version,
            schema_fingerprint,
            segment_id,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.index_id == 0
            || self.definition_version == 0
            || self.segment_id == 0
            || self.schema_fingerprint == [0; 32]
        {
            return Err(IndexError::InvalidDefinition(
                "format-v4 segment identity values must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_routing_key(key: &[u8]) -> Result<(), IndexError> {
    if key.is_empty() || key.len() > INDEX_ROUTING_KEY_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: key.len(),
            limit: INDEX_ROUTING_KEY_BYTES,
        });
    }
    Ok(())
}

/// Validate one logical term boundary before its bounded radix encoding.
/// Ordinary routing keys remain subject to [`validate_routing_key`].
pub(crate) fn validate_term_routing_key(key: &[u8]) -> Result<(), IndexError> {
    if key.is_empty() || key.len() > INDEX_TERM_ROUTING_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: key.len(),
            limit: INDEX_TERM_ROUTING_BYTES,
        });
    }
    Ok(())
}

/// Canonical key for streams addressed by a component ordinal rather than a
/// domain value (notably postings and positions). Big-endian bytes preserve
/// numeric order in routing nodes.
pub const fn component_ordinal_key(ordinal: u32) -> [u8; 4] {
    ordinal.to_be_bytes()
}

pub fn decode_component_ordinal_key(key: &[u8]) -> Result<u32, IndexError> {
    let bytes: [u8; 4] = key
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("component ordinal routing key"))?;
    Ok(u32::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_component_kinds_fail_closed() {
        let known = [
            ComponentKind::SEGMENT_ROOT,
            ComponentKind::ROUTING_NODE,
            ComponentKind::IDENTITY_TABLE,
            ComponentKind::LIVE_MASK,
            ComponentKind::PATH_LOCATOR,
            ComponentKind::TERM_DICTIONARY,
            ComponentKind::POSTINGS,
            ComponentKind::POINTS,
            ComponentKind::DOC_VALUES,
            ComponentKind::POSITIONS,
            ComponentKind::NORMS,
            ComponentKind::VECTORS,
            ComponentKind::SCORING_STATISTICS,
        ];
        for kind in known {
            assert_eq!(ComponentKind::new(kind.get()), Ok(kind));
        }
        for unknown in [0, 14, 15, u16::MAX] {
            assert!(ComponentKind::new(unknown).is_err());
        }
    }
}
