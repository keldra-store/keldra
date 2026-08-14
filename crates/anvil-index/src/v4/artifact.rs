use std::collections::BTreeSet;

use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, ComponentHeader};
use super::model::{
    ComponentKind, INDEX_ARTIFACT_PACK_BYTES, INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES,
    SegmentIdentity,
};
use super::schema::FieldId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactDescriptor {
    pub path: String,
    pub object_version: u64,
    pub object_content_hash: [u8; 32],
    pub object_length: u64,
    pub offset: u64,
    /// Complete component bytes, including the 120-byte envelope.
    pub encoded_length: u64,
    pub logical_length: u64,
    pub component_kind: ComponentKind,
    pub codec_version: u16,
    pub checksum: [u8; 32],
}

pub type ArtifactReference = ArtifactDescriptor;

impl ArtifactDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index_id: u64,
        path: String,
        object_version: u64,
        object_content_hash: [u8; 32],
        object_length: u64,
        offset: u64,
        encoded_length: u64,
        logical_length: u64,
        component_kind: ComponentKind,
        codec_version: u16,
        checksum: [u8; 32],
    ) -> Result<Self, IndexError> {
        let value = Self {
            path,
            object_version,
            object_content_hash,
            object_length,
            offset,
            encoded_length,
            logical_length,
            component_kind,
            codec_version,
            checksum,
        };
        value.validate(index_id)?;
        Ok(value)
    }

    pub fn validate(&self, index_id: u64) -> Result<(), IndexError> {
        if index_id == 0
            || self.object_version == 0
            || self.codec_version == 0
            || self.object_length == 0
            || usize::try_from(self.object_length)
                .ok()
                .is_none_or(|length| length > INDEX_ARTIFACT_PACK_BYTES)
            || usize::try_from(self.encoded_length)
                .ok()
                .is_none_or(|length| {
                    !(COMPONENT_HEADER_BYTES..=INDEX_COMPONENT_BYTES).contains(&length)
                })
            || usize::try_from(self.logical_length)
                .ok()
                .is_none_or(|length| length > INDEX_DECODE_BYTES)
            || self
                .offset
                .checked_add(self.encoded_length)
                .is_none_or(|end| end > self.object_length)
            || self.path != artifact_path(index_id, self.object_content_hash)
        {
            return Err(IndexError::InvalidFormat("format-v4 artifact descriptor"));
        }
        Ok(())
    }

    /// Verify the exact component range after the ordinary object layer has
    /// verified the enclosing pack's `object_content_hash`. `checksum` is the
    /// BLAKE3 checksum of the component payload (not the envelope or pack), so
    /// it must equal the checksum carried by the validated envelope.
    pub fn verify_component_bytes(
        &self,
        identity: SegmentIdentity,
        bytes: &[u8],
    ) -> Result<(), IndexError> {
        self.validate(identity.index_id)?;
        if u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?
            != self.encoded_length
        {
            return Err(IndexError::InvalidFormat("artifact component range length"));
        }
        let decoded =
            super::decode_component(bytes, identity, self.component_kind, self.codec_version)?;
        if decoded.header.logical_length != self.logical_length
            || decoded.header.payload_checksum != self.checksum
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}

/// A move-only encoded component before ordinary-object placement.
#[derive(Debug, Eq, PartialEq)]
pub struct GeneratedComponent {
    header: ComponentHeader,
    bytes: Vec<u8>,
}

impl GeneratedComponent {
    pub(crate) fn from_encoded(
        header: ComponentHeader,
        bytes: Vec<u8>,
    ) -> Result<Self, IndexError> {
        let payload_length =
            usize::try_from(header.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
        if bytes.len() != COMPONENT_HEADER_BYTES.saturating_add(payload_length)
            || bytes.len() > INDEX_COMPONENT_BYTES
        {
            return Err(IndexError::InvalidFormat(
                "format-v4 generated component length",
            ));
        }
        Ok(Self { header, bytes })
    }

    pub fn header(&self) -> ComponentHeader {
        self.header
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn placed(
        &self,
        path: String,
        object_version: u64,
        object_content_hash: [u8; 32],
        object_length: u64,
        offset: u64,
    ) -> Result<ArtifactDescriptor, IndexError> {
        ArtifactDescriptor::new(
            self.header.identity.index_id,
            path,
            object_version,
            object_content_hash,
            object_length,
            offset,
            u64::try_from(self.bytes.len()).map_err(|_| IndexError::OffsetOverflow)?,
            self.header.logical_length,
            self.header.component_kind,
            self.header.codec_version,
            self.header.payload_checksum,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentComponent {
    /// Logical stream represented by this root. The referenced artifact is
    /// either a direct block of this kind or a routing node.
    pub role: ComponentKind,
    pub field_id: Option<FieldId>,
    pub ordinal: u32,
    pub artifact: ArtifactDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    pub identity: SegmentIdentity,
    pub document_count: u32,
    pub live_document_count: u32,
    pub components: Vec<SegmentComponent>,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
}

impl SegmentDescriptor {
    pub fn new(
        identity: SegmentIdentity,
        document_count: u32,
        live_document_count: u32,
        components: Vec<SegmentComponent>,
        encoded_bytes: u64,
        logical_bytes: u64,
    ) -> Result<Self, IndexError> {
        identity.validate()?;
        let value = Self {
            identity,
            document_count,
            live_document_count,
            components,
            encoded_bytes,
            logical_bytes,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        self.identity.validate()?;
        if self.document_count == 0
            || self.live_document_count > self.document_count
            || self.components.is_empty()
        {
            return Err(IndexError::InvalidFormat("format-v4 segment counts"));
        }
        let mut identities = 0usize;
        let mut live_masks = 0usize;
        let mut statistics = 0usize;
        let mut keys = BTreeSet::new();
        let mut root_encoded = 0u64;
        let mut root_logical = 0u64;
        let mut previous_key = None;
        for component in &self.components {
            component.artifact.validate(self.identity.index_id)?;
            if component.artifact.component_kind != ComponentKind::ROUTING_NODE {
                return Err(IndexError::InvalidFormat(
                    "segment component stream must have a routing root",
                ));
            }
            match component.role {
                ComponentKind::IDENTITY_TABLE
                    if component.field_id.is_none() && component.ordinal == 0 =>
                {
                    identities += 1;
                }
                ComponentKind::LIVE_MASK
                    if component.field_id.is_none() && component.ordinal == 0 =>
                {
                    live_masks += 1;
                }
                ComponentKind::SCORING_STATISTICS
                    if component.field_id.is_none() && component.ordinal == 0 =>
                {
                    statistics += 1;
                }
                ComponentKind::IDENTITY_TABLE
                | ComponentKind::LIVE_MASK
                | ComponentKind::SCORING_STATISTICS => {
                    return Err(IndexError::InvalidFormat(
                        "non-canonical mandatory segment component",
                    ));
                }
                _ => {}
            }
            let key = (component.role, component.field_id, component.ordinal);
            if previous_key.is_some_and(|previous| previous >= key) || !keys.insert(key) {
                return Err(IndexError::InvalidFormat(
                    "format-v4 segment components are not canonical ordered",
                ));
            }
            previous_key = Some(key);
            // Content-addressed component streams may legitimately share one
            // root. For example, two indexed fields with identical posting
            // DocIds encode to the same immutable posting and routing bytes.
            // The canonical `(role, field_id, ordinal)` key above remains the
            // semantic identity; rejecting the shared bytes would defeat
            // ordinary-object deduplication without adding integrity.
            root_encoded = root_encoded
                .checked_add(component.artifact.encoded_length)
                .ok_or(IndexError::OffsetOverflow)?;
            root_logical = root_logical
                .checked_add(component.artifact.logical_length)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if identities != 1
            || live_masks != 1
            || statistics != 1
            || self.encoded_bytes < root_encoded
            || self.logical_bytes < root_logical
        {
            return Err(IndexError::InvalidFormat(
                "format-v4 segment component summary",
            ));
        }
        Ok(())
    }
}

pub fn definition_path(name: &str) -> Result<String, IndexError> {
    if name.is_empty() || name.len() > 255 || name.contains('/') || name.contains('\0') {
        return Err(IndexError::InvalidDefinition(
            "index definition name must be one non-empty path segment".into(),
        ));
    }
    Ok(format!("_anvil/indexes/v4/definitions/{name}"))
}

pub fn artifact_path(index_id: u64, hash: [u8; 32]) -> String {
    format!("_anvil/indexes/v4/{index_id}/artifacts/{}", hex(hash))
}

pub fn manifest_path(index_id: u64, hash: [u8; 32]) -> String {
    format!("_anvil/indexes/v4/{index_id}/manifests/{}", hex(hash))
}

pub fn current_path(index_id: u64) -> String {
    format!("_anvil/indexes/v4/{index_id}/current")
}

fn hex(hash: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in hash {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::encode_component;

    #[test]
    fn placement_binds_component_to_canonical_object_reference() {
        let identity = SegmentIdentity::new(3, 4, [5; 32], 6).unwrap();
        let component =
            encode_component(identity, ComponentKind::IDENTITY_TABLE, 1, 0, 8, vec![9; 8]).unwrap();
        let object = [7; 32];
        let descriptor = component
            .placed(artifact_path(3, object), 11, object, 4096, 17)
            .unwrap();
        assert_eq!(descriptor.offset, 17);
        assert_eq!(descriptor.encoded_length, 128);
        assert_eq!(descriptor.logical_length, 8);
    }

    #[test]
    fn noncanonical_reference_or_pack_overrun_fails_closed() {
        assert!(
            ArtifactDescriptor::new(
                3,
                "_anvil/indexes/v4/03/artifacts/no".into(),
                1,
                [7; 32],
                130,
                20,
                120,
                0,
                ComponentKind::POSTINGS,
                1,
                [8; 32],
            )
            .is_err()
        );
    }

    #[test]
    fn reserved_paths_are_canonical() {
        assert_eq!(
            definition_path("search").unwrap(),
            "_anvil/indexes/v4/definitions/search"
        );
        assert!(definition_path("a/b").is_err());
        assert_eq!(current_path(12), "_anvil/indexes/v4/12/current");
        assert!(manifest_path(12, [0; 32]).ends_with(&"0".repeat(64)));
    }

    fn root(index_id: u64, role: ComponentKind, ordinal: u32, routed: bool) -> SegmentComponent {
        let hash = [role.get() as u8 + ordinal as u8; 32];
        SegmentComponent {
            role,
            field_id: None,
            ordinal,
            artifact: ArtifactDescriptor::new(
                index_id,
                artifact_path(index_id, hash),
                1,
                hash,
                4096,
                0,
                120,
                0,
                if routed {
                    ComponentKind::ROUTING_NODE
                } else {
                    role
                },
                1,
                [3; 32],
            )
            .unwrap(),
        }
    }

    #[test]
    fn segment_accepts_canonical_routing_roots() {
        let identity = SegmentIdentity::new(5, 6, [7; 32], 8).unwrap();
        let components = vec![
            root(5, ComponentKind::IDENTITY_TABLE, 0, true),
            root(5, ComponentKind::LIVE_MASK, 0, true),
            root(5, ComponentKind::POSTINGS, 1, true),
            root(5, ComponentKind::SCORING_STATISTICS, 0, true),
        ];
        let segment = SegmentDescriptor::new(identity, 10, 9, components, 1024, 128).unwrap();
        assert_eq!(segment.components.len(), 4);
    }

    #[test]
    fn segment_rejects_a_bare_component_root() {
        let identity = SegmentIdentity::new(5, 6, [7; 32], 8).unwrap();
        let components = vec![
            root(5, ComponentKind::IDENTITY_TABLE, 0, true),
            root(5, ComponentKind::LIVE_MASK, 0, false),
            root(5, ComponentKind::SCORING_STATISTICS, 0, true),
        ];
        assert!(SegmentDescriptor::new(identity, 1, 1, components, 1024, 128).is_err());
    }

    #[test]
    fn segment_rejects_noncanonical_mandatory_root() {
        let identity = SegmentIdentity::new(5, 6, [7; 32], 8).unwrap();
        let mut components = vec![
            root(5, ComponentKind::IDENTITY_TABLE, 0, false),
            root(5, ComponentKind::LIVE_MASK, 0, false),
            root(5, ComponentKind::SCORING_STATISTICS, 0, false),
        ];
        components[0].ordinal = 1;
        assert!(SegmentDescriptor::new(identity, 1, 1, components, 1024, 128).is_err());
    }
}
