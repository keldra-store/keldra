use crate::IndexError;

use super::model::{ComponentKind, INDEX_COMPONENT_BYTES, INDEX_DECODE_BYTES, SegmentIdentity};
use super::stored_fields::{
    MAX_STORED_FIELDS_PAYLOAD_BYTES, STORED_FIELDS_COMPONENT_CODEC_VERSION,
};

const COMPONENT_MAGIC: &[u8; 8] = b"ANVLIDX4";
pub const COMPONENT_HEADER_BYTES: usize = 120;
const COMPONENT_FLAG_LZ4_BLOCK: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentHeader {
    pub component_kind: ComponentKind,
    pub codec_version: u16,
    pub flags: u32,
    pub identity: SegmentIdentity,
    pub logical_length: u64,
    /// Encoded payload length, excluding the fixed envelope.
    pub encoded_length: u64,
    pub payload_checksum: [u8; 32],
}

impl ComponentHeader {
    fn validate(&self) -> Result<(), IndexError> {
        self.identity.validate()?;
        if self.codec_version == 0 {
            return Err(IndexError::InvalidFormat(
                "format-v4 component codec version",
            ));
        }
        let logical =
            usize::try_from(self.logical_length).map_err(|_| IndexError::OffsetOverflow)?;
        if logical > INDEX_DECODE_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: logical,
                limit: INDEX_DECODE_BYTES,
            });
        }
        let encoded =
            usize::try_from(self.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
        if COMPONENT_HEADER_BYTES
            .checked_add(encoded)
            .is_none_or(|length| length > INDEX_COMPONENT_BYTES)
        {
            return Err(IndexError::ResourceLimit {
                needed: COMPONENT_HEADER_BYTES.saturating_add(encoded),
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        if self.component_kind == ComponentKind::STORED_FIELDS
            && self.codec_version == STORED_FIELDS_COMPONENT_CODEC_VERSION
        {
            if logical > MAX_STORED_FIELDS_PAYLOAD_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: logical,
                    limit: MAX_STORED_FIELDS_PAYLOAD_BYTES,
                });
            }
            match self.flags {
                0 if encoded == logical => {}
                COMPONENT_FLAG_LZ4_BLOCK if encoded < logical => {}
                0 | COMPONENT_FLAG_LZ4_BLOCK => {
                    return Err(IndexError::InvalidFormat("stored-fields component lengths"));
                }
                _ => {
                    return Err(IndexError::InvalidFormat("stored-fields component flags"));
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn decode_component_header(bytes: &[u8]) -> Result<ComponentHeader, IndexError> {
    if bytes.len() < COMPONENT_HEADER_BYTES {
        return Err(IndexError::InvalidFormat("format-v4 component header"));
    }
    if &bytes[..8] != COMPONENT_MAGIC {
        return Err(IndexError::InvalidFormat("format-v4 component magic"));
    }
    let component_kind = ComponentKind::new(read_u16(bytes, 8)?)
        .map_err(|_| IndexError::InvalidFormat("format-v4 component kind"))?;
    let codec_version = read_u16(bytes, 10)?;
    let flags = read_u32(bytes, 12)?;
    let identity = SegmentIdentity {
        index_id: read_u64(bytes, 16)?,
        definition_version: read_u64(bytes, 24)?,
        schema_fingerprint: bytes[32..64]
            .try_into()
            .map_err(|_| IndexError::InvalidFormat("format-v4 schema fingerprint"))?,
        segment_id: read_u64(bytes, 64)?,
    };
    let logical_length = read_u64(bytes, 72)?;
    let encoded_length = read_u64(bytes, 80)?;
    let payload_checksum = bytes[88..120]
        .try_into()
        .map_err(|_| IndexError::InvalidFormat("format-v4 payload checksum"))?;
    let header = ComponentHeader {
        component_kind,
        codec_version,
        flags,
        identity,
        logical_length,
        encoded_length,
        payload_checksum,
    };
    header.validate()?;
    Ok(header)
}

#[derive(Debug, Eq, PartialEq)]
pub struct DecodedComponent<'a> {
    pub header: ComponentHeader,
    pub payload: &'a [u8],
}

pub fn encode_component(
    identity: SegmentIdentity,
    component_kind: ComponentKind,
    codec_version: u16,
    flags: u32,
    logical_length: u64,
    payload: Vec<u8>,
) -> Result<super::GeneratedComponent, IndexError> {
    let encoded_length = u64::try_from(payload.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let header = ComponentHeader {
        component_kind,
        codec_version,
        flags,
        identity,
        logical_length,
        encoded_length,
        payload_checksum: *blake3::hash(&payload).as_bytes(),
    };
    header.validate()?;
    let mut bytes = Vec::with_capacity(
        COMPONENT_HEADER_BYTES
            .checked_add(payload.len())
            .ok_or(IndexError::OffsetOverflow)?,
    );
    bytes.extend_from_slice(COMPONENT_MAGIC);
    bytes.extend_from_slice(&component_kind.get().to_le_bytes());
    bytes.extend_from_slice(&codec_version.to_le_bytes());
    bytes.extend_from_slice(&flags.to_le_bytes());
    bytes.extend_from_slice(&identity.index_id.to_le_bytes());
    bytes.extend_from_slice(&identity.definition_version.to_le_bytes());
    bytes.extend_from_slice(&identity.schema_fingerprint);
    bytes.extend_from_slice(&identity.segment_id.to_le_bytes());
    bytes.extend_from_slice(&logical_length.to_le_bytes());
    bytes.extend_from_slice(&encoded_length.to_le_bytes());
    bytes.extend_from_slice(&header.payload_checksum);
    debug_assert_eq!(bytes.len(), COMPONENT_HEADER_BYTES);
    bytes.extend_from_slice(&payload);
    super::GeneratedComponent::from_encoded(header, bytes)
}

pub fn decode_component<'a>(
    bytes: &'a [u8],
    expected_identity: SegmentIdentity,
    expected_kind: ComponentKind,
    expected_codec_version: u16,
) -> Result<DecodedComponent<'a>, IndexError> {
    if bytes.len() < COMPONENT_HEADER_BYTES || bytes.len() > INDEX_COMPONENT_BYTES {
        return Err(IndexError::InvalidFormat("format-v4 component length"));
    }
    let header = decode_component_header(bytes)?;
    if header.identity != expected_identity
        || header.component_kind != expected_kind
        || header.codec_version != expected_codec_version
    {
        return Err(IndexError::InvalidFormat("format-v4 component identity"));
    }
    let encoded = usize::try_from(header.encoded_length).map_err(|_| IndexError::OffsetOverflow)?;
    if COMPONENT_HEADER_BYTES.checked_add(encoded) != Some(bytes.len()) {
        return Err(IndexError::InvalidFormat("format-v4 encoded length"));
    }
    let payload = &bytes[COMPONENT_HEADER_BYTES..];
    if blake3::hash(payload).as_bytes() != &header.payload_checksum {
        return Err(IndexError::Integrity);
    }
    Ok(DecodedComponent { header, payload })
}

/// Prepare one independently routed logical component payload. Stored fields
/// use bounded LZ4 blocks only when doing so reduces the bytes written; every
/// other component remains byte-for-byte raw.
pub(crate) fn prepare_component_payload(
    component_kind: ComponentKind,
    codec_version: u16,
    payload: Vec<u8>,
) -> Result<(u32, u64, Vec<u8>), IndexError> {
    let logical_length = u64::try_from(payload.len()).map_err(|_| IndexError::OffsetOverflow)?;
    if component_kind != ComponentKind::STORED_FIELDS {
        return Ok((0, logical_length, payload));
    }
    if codec_version != STORED_FIELDS_COMPONENT_CODEC_VERSION {
        return Err(IndexError::InvalidDefinition(
            "stored-fields component requires codec version 2".into(),
        ));
    }
    if payload.len() > MAX_STORED_FIELDS_PAYLOAD_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: payload.len(),
            limit: MAX_STORED_FIELDS_PAYLOAD_BYTES,
        });
    }
    if payload.is_empty() {
        return Ok((0, logical_length, payload));
    }
    let maximum = lz4_flex::block::get_maximum_output_size(payload.len());
    let mut compressed = vec![0u8; maximum];
    let encoded = lz4_flex::block::compress_into(&payload, &mut compressed)
        .map_err(|error| IndexError::Encode(error.to_string()))?;
    compressed.truncate(encoded);
    if compressed.len() < payload.len() {
        Ok((COMPONENT_FLAG_LZ4_BLOCK, logical_length, compressed))
    } else {
        Ok((0, logical_length, payload))
    }
}

/// Materialize the checked logical payload after the ordinary-object range,
/// component envelope, descriptor, and checksum have all been validated.
pub(crate) fn materialize_component_payload(
    header: ComponentHeader,
    payload: &[u8],
) -> Result<Vec<u8>, IndexError> {
    if header.component_kind != ComponentKind::STORED_FIELDS {
        return Ok(payload.to_vec());
    }
    if header.codec_version != STORED_FIELDS_COMPONENT_CODEC_VERSION {
        return Err(IndexError::InvalidFormat(
            "stored-fields component codec version",
        ));
    }
    let logical = usize::try_from(header.logical_length).map_err(|_| IndexError::OffsetOverflow)?;
    if logical > MAX_STORED_FIELDS_PAYLOAD_BYTES {
        return Err(IndexError::ResourceLimit {
            needed: logical,
            limit: MAX_STORED_FIELDS_PAYLOAD_BYTES,
        });
    }
    match header.flags {
        0 => {
            if payload.len() != logical {
                return Err(IndexError::InvalidFormat(
                    "stored-fields raw payload length",
                ));
            }
            Ok(payload.to_vec())
        }
        COMPONENT_FLAG_LZ4_BLOCK => {
            if payload.len() >= logical {
                return Err(IndexError::InvalidFormat(
                    "stored-fields compressed payload length",
                ));
            }
            let mut decoded = vec![0u8; logical];
            let written = lz4_flex::block::decompress_into(payload, &mut decoded)
                .map_err(|error| IndexError::Decode(error.to_string()))?;
            if written != logical {
                return Err(IndexError::InvalidFormat(
                    "stored-fields decoded payload length",
                ));
            }
            Ok(decoded)
        }
        _ => Err(IndexError::InvalidFormat("stored-fields component flags")),
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, IndexError> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .and_then(|value| value.try_into().ok())
            .ok_or(IndexError::InvalidFormat("truncated format-v4 integer"))?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .and_then(|value| value.try_into().ok())
            .ok_or(IndexError::InvalidFormat("truncated format-v4 integer"))?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .and_then(|value| value.try_into().ok())
            .ok_or(IndexError::InvalidFormat("truncated format-v4 integer"))?,
    ))
}

/// Checked portable payload encoder shared by format-v4 component codecs.
#[derive(Debug, Default)]
pub(crate) struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    pub(crate) fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub(crate) fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    pub(crate) fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn usize_u32(&mut self, value: usize) -> Result<(), IndexError> {
        self.u32(u32::try_from(value).map_err(|_| IndexError::OffsetOverflow)?);
        Ok(())
    }

    pub(crate) fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), IndexError> {
        self.usize_u32(value.len())?;
        self.raw(value);
        Ok(())
    }

    pub(crate) fn string(&mut self, value: &str) -> Result<(), IndexError> {
        self.bytes(value.as_bytes())
    }

    pub(crate) fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Checked decoder with one hard aggregate allocation claim per logical block.
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
    claimed: usize,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, IndexError> {
        if bytes.len() > INDEX_COMPONENT_BYTES {
            return Err(IndexError::InvalidFormat("format-v4 payload bound"));
        }
        Ok(Self {
            bytes,
            cursor: 0,
            claimed: 0,
        })
    }

    pub(crate) fn claim(&mut self, bytes: usize) -> Result<(), IndexError> {
        self.claimed = self
            .claimed
            .checked_add(bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if self.claimed > INDEX_DECODE_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: self.claimed,
                limit: INDEX_DECODE_BYTES,
            });
        }
        Ok(())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, IndexError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn bool(&mut self) -> Result<bool, IndexError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IndexError::InvalidFormat("non-canonical format-v4 boolean")),
        }
    }

    pub(crate) fn u16(&mut self) -> Result<u16, IndexError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("checked slice length"),
        ))
    }

    pub(crate) fn u32(&mut self) -> Result<u32, IndexError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("checked slice length"),
        ))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, IndexError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("checked slice length"),
        ))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], IndexError> {
        let length = usize::try_from(self.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        self.take(length)
    }

    pub(crate) fn owned_bytes(&mut self) -> Result<Vec<u8>, IndexError> {
        let value = self.bytes()?;
        self.claim(value.len())?;
        Ok(value.to_vec())
    }

    pub(crate) fn string(&mut self) -> Result<String, IndexError> {
        let value = self.bytes()?;
        self.claim(value.len())?;
        String::from_utf8(value.to_vec()).map_err(|_| IndexError::InvalidFormat("format-v4 UTF-8"))
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], IndexError> {
        let end = self
            .cursor
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        let value = self
            .bytes
            .get(self.cursor..end)
            .ok_or(IndexError::InvalidFormat("truncated format-v4 payload"))?;
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), IndexError> {
        if self.cursor != self.bytes.len() {
            return Err(IndexError::InvalidFormat(
                "trailing format-v4 payload bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> SegmentIdentity {
        SegmentIdentity::new(7, 9, [0xAB; 32], 11).unwrap()
    }

    #[test]
    fn envelope_has_exact_golden_layout() {
        let component = encode_component(
            identity(),
            ComponentKind::POSTINGS,
            3,
            0x0506_0708,
            99,
            vec![1, 2, 3],
        )
        .unwrap();
        let bytes = component.bytes();
        assert_eq!(bytes.len(), COMPONENT_HEADER_BYTES + 3);
        assert_eq!(&bytes[..8], b"ANVLIDX4");
        assert_eq!(&bytes[8..10], &7u16.to_le_bytes());
        assert_eq!(&bytes[10..12], &3u16.to_le_bytes());
        assert_eq!(&bytes[12..16], &0x0506_0708u32.to_le_bytes());
        assert_eq!(&bytes[16..24], &7u64.to_le_bytes());
        assert_eq!(&bytes[24..32], &9u64.to_le_bytes());
        assert_eq!(&bytes[32..64], &[0xAB; 32]);
        assert_eq!(&bytes[64..72], &11u64.to_le_bytes());
        assert_eq!(&bytes[72..80], &99u64.to_le_bytes());
        assert_eq!(&bytes[80..88], &3u64.to_le_bytes());
        assert_eq!(&bytes[88..120], blake3::hash(&[1, 2, 3]).as_bytes());
        assert_eq!(&bytes[120..], &[1, 2, 3]);
        let decoded = decode_component(bytes, identity(), ComponentKind::POSTINGS, 3).unwrap();
        assert_eq!(decoded.payload, &[1, 2, 3]);
        assert_eq!(decoded.header.logical_length, 99);
    }

    #[test]
    fn every_identity_field_and_corruption_fail_closed() {
        let component = encode_component(
            identity(),
            ComponentKind::POSTINGS,
            1,
            0,
            4,
            vec![1, 2, 3, 4],
        )
        .unwrap();
        for offset in [8usize, 10, 16, 24, 32, 64, 80, 88, 120] {
            let mut corrupt = component.bytes().to_vec();
            corrupt[offset] ^= 1;
            assert!(
                decode_component(&corrupt, identity(), ComponentKind::POSTINGS, 1).is_err(),
                "offset {offset} must be checked"
            );
        }
        let mut trailing = component.bytes().to_vec();
        trailing.push(0);
        assert!(decode_component(&trailing, identity(), ComponentKind::POSTINGS, 1).is_err());
    }

    #[test]
    fn logical_and_encoded_bounds_are_hard() {
        assert!(
            encode_component(
                identity(),
                ComponentKind::POSTINGS,
                1,
                0,
                (INDEX_DECODE_BYTES + 1) as u64,
                vec![],
            )
            .is_err()
        );
        assert!(
            encode_component(
                identity(),
                ComponentKind::POSTINGS,
                1,
                0,
                1,
                vec![0; INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES + 1],
            )
            .is_err()
        );
    }

    #[test]
    fn stored_fields_choose_smaller_lz4_or_raw_and_round_trip() {
        let repetitive = vec![0x5a; 64 * 1024];
        let (flags, logical, encoded) = prepare_component_payload(
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
            repetitive.clone(),
        )
        .unwrap();
        assert_eq!(flags, COMPONENT_FLAG_LZ4_BLOCK);
        assert!(encoded.len() < repetitive.len());
        let component = encode_component(
            identity(),
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
            flags,
            logical,
            encoded,
        )
        .unwrap();
        let decoded = decode_component(
            component.bytes(),
            identity(),
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
        )
        .unwrap();
        assert_eq!(
            materialize_component_payload(decoded.header, decoded.payload).unwrap(),
            repetitive
        );

        let tiny = vec![1, 2, 3];
        let (flags, logical, encoded) = prepare_component_payload(
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
            tiny.clone(),
        )
        .unwrap();
        assert_eq!(flags, 0);
        assert_eq!(encoded, tiny);
        assert_eq!(logical, 3);
    }

    #[test]
    fn stored_fields_compression_metadata_fails_closed() {
        let malformed = encode_component(
            identity(),
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
            COMPONENT_FLAG_LZ4_BLOCK,
            100,
            vec![0xff; 4],
        )
        .unwrap();
        let decoded = decode_component(
            malformed.bytes(),
            identity(),
            ComponentKind::STORED_FIELDS,
            STORED_FIELDS_COMPONENT_CODEC_VERSION,
        )
        .unwrap();
        assert!(materialize_component_payload(decoded.header, decoded.payload).is_err());

        assert!(
            encode_component(
                identity(),
                ComponentKind::STORED_FIELDS,
                STORED_FIELDS_COMPONENT_CODEC_VERSION,
                2,
                8,
                vec![0; 4],
            )
            .is_err()
        );
        assert!(
            encode_component(
                identity(),
                ComponentKind::STORED_FIELDS,
                STORED_FIELDS_COMPONENT_CODEC_VERSION,
                0,
                (MAX_STORED_FIELDS_PAYLOAD_BYTES + 1) as u64,
                vec![0; MAX_STORED_FIELDS_PAYLOAD_BYTES + 1],
            )
            .is_err()
        );
    }
}
