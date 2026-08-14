use crate::IndexError;

use super::artifact::ArtifactDescriptor;
use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{
    ComponentKind, INDEX_COMPONENT_BYTES, INDEX_ROUTING_FANOUT, INDEX_ROUTING_HEIGHT,
    INDEX_ROUTING_KEY_BYTES, validate_term_routing_key,
};

const ROUTING_CODEC_VERSION: u16 = 1;
const INLINE_ROUTING_KEY: u8 = 0;
const RADIX_ROUTING_KEY: u8 = 1;
const CANONICAL_TERM_PREFIX_BYTES: usize = 5;
const MAXIMUM_RADIX_FRAGMENTS: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingEntry {
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub child: ArtifactDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingNode {
    pub index_id: u64,
    /// One points at data blocks; larger values point at routing nodes.
    pub height: u8,
    entries: Vec<RoutingEntry>,
}

impl RoutingNode {
    pub fn new(index_id: u64, height: u8, entries: Vec<RoutingEntry>) -> Result<Self, IndexError> {
        if index_id == 0
            || height == 0
            || height > INDEX_ROUTING_HEIGHT
            || entries.is_empty()
            || entries.len() > INDEX_ROUTING_FANOUT
        {
            return Err(IndexError::InvalidDefinition(
                "routing node identity, height, or fanout is invalid".into(),
            ));
        }
        let mut previous_max: Option<&[u8]> = None;
        let mut leaf_kind = None;
        for entry in &entries {
            validate_term_routing_key(&entry.minimum_key)?;
            validate_term_routing_key(&entry.maximum_key)?;
            entry.child.validate(index_id)?;
            if entry.element_count == 0
                || entry.minimum_key > entry.maximum_key
                || previous_max.is_some_and(|previous| previous >= entry.minimum_key.as_slice())
                || height > 1 && entry.child.component_kind != ComponentKind::ROUTING_NODE
            {
                return Err(IndexError::InvalidDefinition(
                    "routing child ranges must be non-empty, ordered, and non-overlapping".into(),
                ));
            }
            if height == 1 {
                if leaf_kind.is_some_and(|kind| kind != entry.child.component_kind) {
                    return Err(IndexError::InvalidDefinition(
                        "routing leaves must have one logical component kind".into(),
                    ));
                }
                leaf_kind = Some(entry.child.component_kind);
            }
            previous_max = Some(&entry.maximum_key);
        }
        let node = Self {
            index_id,
            height,
            entries,
        };
        let needed = node.encode_payload()?.len();
        if needed + COMPONENT_HEADER_BYTES > INDEX_COMPONENT_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: needed + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(node)
    }

    pub fn entries(&self) -> &[RoutingEntry] {
        &self.entries
    }

    pub fn child_for(&self, key: &[u8]) -> Option<&RoutingEntry> {
        let offset = self
            .entries
            .partition_point(|entry| entry.maximum_key.as_slice() < key);
        self.entries
            .get(offset)
            .filter(|entry| entry.minimum_key.as_slice() <= key)
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(ROUTING_CODEC_VERSION);
        out.u8(self.height);
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            encode_routing_key(&mut out, &entry.minimum_key)?;
            encode_routing_key(&mut out, &entry.maximum_key)?;
            out.u64(entry.element_count);
            encode_artifact(&mut out, &entry.child)?;
        }
        Ok(out.finish())
    }

    pub fn decode_payload(index_id: u64, bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != ROUTING_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("routing codec version"));
        }
        let height = input.u8()?;
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<RoutingEntry>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(RoutingEntry {
                minimum_key: decode_routing_key(&mut input)?,
                maximum_key: decode_routing_key(&mut input)?,
                element_count: input.u64()?,
                child: decode_artifact(index_id, &mut input)?,
            });
        }
        input.finish()?;
        Self::new(index_id, height, entries)
    }
}

/// Encode one logical boundary without ever placing more than 4,096 bytes in
/// one routing key. Long canonical terms keep their fixed FieldId/type prefix
/// once and split the ordered value into at most eight radix fragments.
fn encode_routing_key(out: &mut Encoder, key: &[u8]) -> Result<(), IndexError> {
    validate_term_routing_key(key)?;
    if key.len() <= INDEX_ROUTING_KEY_BYTES {
        out.u8(INLINE_ROUTING_KEY);
        out.bytes(key)?;
        return Ok(());
    }
    let (prefix, value) = key
        .split_at_checked(CANONICAL_TERM_PREFIX_BYTES)
        .ok_or(IndexError::InvalidDefinition(
            "long routing key has no canonical term prefix".into(),
        ))?;
    let fragments = value.len().div_ceil(INDEX_ROUTING_KEY_BYTES);
    if fragments == 0 || fragments > MAXIMUM_RADIX_FRAGMENTS {
        return Err(IndexError::ResourceLimit {
            needed: fragments,
            limit: MAXIMUM_RADIX_FRAGMENTS,
        });
    }
    out.u8(RADIX_ROUTING_KEY);
    out.raw(prefix);
    out.u8(u8::try_from(fragments).map_err(|_| IndexError::OffsetOverflow)?);
    for fragment in value.chunks(INDEX_ROUTING_KEY_BYTES) {
        out.bytes(fragment)?;
    }
    Ok(())
}

fn decode_routing_key(input: &mut Decoder<'_>) -> Result<Vec<u8>, IndexError> {
    match input.u8()? {
        INLINE_ROUTING_KEY => {
            let key = input.owned_bytes()?;
            if key.len() > INDEX_ROUTING_KEY_BYTES {
                return Err(IndexError::InvalidFormat("inline routing key length"));
            }
            validate_term_routing_key(&key)
                .map_err(|_| IndexError::InvalidFormat("inline routing key"))?;
            Ok(key)
        }
        RADIX_ROUTING_KEY => {
            let prefix = input.take(CANONICAL_TERM_PREFIX_BYTES)?;
            let fragment_count = usize::from(input.u8()?);
            if fragment_count == 0 || fragment_count > MAXIMUM_RADIX_FRAGMENTS {
                return Err(IndexError::InvalidFormat("routing radix fragment count"));
            }
            let mut fragments = Vec::with_capacity(fragment_count);
            let mut length = CANONICAL_TERM_PREFIX_BYTES;
            for _ in 0..fragment_count {
                let fragment = input.bytes()?;
                if fragment.is_empty() || fragment.len() > INDEX_ROUTING_KEY_BYTES {
                    return Err(IndexError::InvalidFormat("routing radix fragment length"));
                }
                length = length
                    .checked_add(fragment.len())
                    .ok_or(IndexError::OffsetOverflow)?;
                fragments.push(fragment);
            }
            if length <= INDEX_ROUTING_KEY_BYTES
                || fragments[..fragments.len() - 1]
                    .iter()
                    .any(|fragment| fragment.len() != INDEX_ROUTING_KEY_BYTES)
            {
                return Err(IndexError::InvalidFormat("non-canonical routing radix"));
            }
            input.claim(
                length
                    .checked_add(
                        fragments
                            .len()
                            .checked_mul(std::mem::size_of::<&[u8]>())
                            .ok_or(IndexError::OffsetOverflow)?,
                    )
                    .ok_or(IndexError::OffsetOverflow)?,
            )?;
            let mut key = Vec::with_capacity(length);
            key.extend_from_slice(prefix);
            for fragment in fragments {
                key.extend_from_slice(fragment);
            }
            validate_term_routing_key(&key)
                .map_err(|_| IndexError::InvalidFormat("routing radix key"))?;
            Ok(key)
        }
        _ => Err(IndexError::InvalidFormat("routing key encoding")),
    }
}

pub(crate) fn encode_artifact(
    out: &mut Encoder,
    descriptor: &ArtifactDescriptor,
) -> Result<(), IndexError> {
    out.string(&descriptor.path)?;
    out.u64(descriptor.object_version);
    out.raw(&descriptor.object_content_hash);
    out.u64(descriptor.object_length);
    out.u64(descriptor.offset);
    out.u64(descriptor.encoded_length);
    out.u64(descriptor.logical_length);
    out.u16(descriptor.component_kind.get());
    out.u16(descriptor.codec_version);
    out.raw(&descriptor.checksum);
    Ok(())
}

pub(crate) fn decode_artifact(
    index_id: u64,
    input: &mut Decoder<'_>,
) -> Result<ArtifactDescriptor, IndexError> {
    let path = input.string()?;
    let object_version = input.u64()?;
    let object_content_hash = input
        .take(32)?
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("artifact object hash"))?;
    let object_length = input.u64()?;
    let offset = input.u64()?;
    let encoded_length = input.u64()?;
    let logical_length = input.u64()?;
    let component_kind = ComponentKind::new(input.u16()?)
        .map_err(|_| IndexError::InvalidFormat("artifact component kind"))?;
    let codec_version = input.u16()?;
    let checksum = input
        .take(32)?
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("artifact checksum"))?;
    ArtifactDescriptor::new(
        index_id,
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::artifact_path;

    fn artifact(index_id: u64, kind: ComponentKind, hash: u8) -> ArtifactDescriptor {
        ArtifactDescriptor::new(
            index_id,
            artifact_path(index_id, [hash; 32]),
            2,
            [hash; 32],
            4096,
            0,
            120,
            0,
            kind,
            1,
            [9; 32],
        )
        .unwrap()
    }

    #[test]
    fn bounded_routing_round_trips_and_seeks() {
        let node = RoutingNode::new(
            7,
            1,
            vec![
                RoutingEntry {
                    minimum_key: b"a".to_vec(),
                    maximum_key: b"c".to_vec(),
                    element_count: 3,
                    child: artifact(7, ComponentKind::TERM_DICTIONARY, 1),
                },
                RoutingEntry {
                    minimum_key: b"d".to_vec(),
                    maximum_key: b"z".to_vec(),
                    element_count: 20,
                    child: artifact(7, ComponentKind::TERM_DICTIONARY, 2),
                },
            ],
        )
        .unwrap();
        let decoded = RoutingNode::decode_payload(7, &node.encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.child_for(b"e").unwrap().minimum_key, b"d");
        assert!(decoded.child_for(b"cc").is_none());
    }

    #[test]
    fn overlapping_ranges_and_excess_fanout_are_rejected() {
        let child = artifact(7, ComponentKind::TERM_DICTIONARY, 1);
        let overlapping = vec![
            RoutingEntry {
                minimum_key: b"a".to_vec(),
                maximum_key: b"d".to_vec(),
                element_count: 1,
                child: child.clone(),
            },
            RoutingEntry {
                minimum_key: b"c".to_vec(),
                maximum_key: b"z".to_vec(),
                element_count: 1,
                child,
            },
        ];
        assert!(RoutingNode::new(7, 1, overlapping).is_err());
    }

    #[test]
    fn long_term_boundaries_use_bounded_radix_fragments_and_seek_exactly() {
        let prefix = [0, 0, 0, 2, 5];
        let mut first = prefix.to_vec();
        first.extend(std::iter::repeat_n(b'a', 12_000));
        let mut second = prefix.to_vec();
        second.extend(std::iter::repeat_n(b'b', super::super::model::INDEX_TERM_BYTES));
        let node = RoutingNode::new(
            7,
            1,
            vec![
                RoutingEntry {
                    minimum_key: first.clone(),
                    maximum_key: first.clone(),
                    element_count: 1,
                    child: artifact(7, ComponentKind::TERM_DICTIONARY, 1),
                },
                RoutingEntry {
                    minimum_key: second.clone(),
                    maximum_key: second.clone(),
                    element_count: 1,
                    child: artifact(7, ComponentKind::TERM_DICTIONARY, 2),
                },
            ],
        )
        .unwrap();
        let encoded = node.encode_payload().unwrap();
        let decoded = RoutingNode::decode_payload(7, &encoded).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(decoded.child_for(&second).unwrap().minimum_key, second);
    }
}
