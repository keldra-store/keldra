//! Versioned compact encoding for authoritative source-journal transitions.
//!
//! The fixed header retains the exact bare-`LocalChange` JSON byte length used
//! by the existing peer protocol. Readers can therefore enforce page budgets
//! without serializing the decoded transition a second time.

use thiserror::Error;

use super::{
    AccountingHeadTransition, AggregateChanged, AggregateKind, ContentLifecycleChanged,
    LocalChange, ObjectHeadChange, ObjectHeadChangeKind, RetainedVersionDeletedChange,
};
use crate::{
    BlobRef, DefinitionKind, DefinitionOperation, DefinitionTransition, ReferenceDelta, VersionId,
};

const MAGIC: &[u8; 4] = b"ANVJ";
const FORMAT: u16 = 2;
const RESERVED: u8 = 0;
const HEADER_BYTES: usize = 4 + 2 + 1 + 1 + 8 + 8;

const OBJECT_HEAD: u8 = 1;
const RETAINED_VERSION_DELETED: u8 = 2;
const AGGREGATE_CHANGED: u8 = 3;
const CONTENT_LIFECYCLE_CHANGED: u8 = 4;

const PUT: u8 = 1;
const DELETE: u8 = 2;
const ZANZIBAR_REALM: u8 = 1;
const LOGICAL_RECORD: u8 = 2;

#[derive(Debug, Error)]
pub(crate) enum LocalChangeCodecError {
    #[error("local change record is malformed: {0}")]
    Malformed(String),
    #[error("cannot measure local change peer encoding: {0}")]
    PeerEncoding(serde_json::Error),
    #[error("unsupported local change format {0}")]
    UnsupportedFormat(u16),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedLocalChange {
    pub change: LocalChange,
    pub peer_encoded_bytes: u64,
}

pub(crate) fn encode_local_change(change: &LocalChange) -> Result<Vec<u8>, LocalChangeCodecError> {
    let peer_encoded_bytes = encoded_change_len(change)?;
    let (kind, body) = encode_body(change)?;
    let body_bytes = u64::try_from(body.len())
        .map_err(|_| malformed("local change body length is exhausted"))?;
    let capacity = HEADER_BYTES
        .checked_add(body.len())
        .ok_or_else(|| malformed("local change record length is exhausted"))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(MAGIC);
    put_u16(&mut encoded, FORMAT);
    put_u8(&mut encoded, kind);
    put_u8(&mut encoded, RESERVED);
    put_u64(&mut encoded, body_bytes);
    put_u64(&mut encoded, peer_encoded_bytes);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

pub(crate) fn decode_local_change(encoded: &[u8]) -> Result<LocalChange, LocalChangeCodecError> {
    Ok(decode_local_change_with_length(encoded)?.change)
}

pub(crate) fn decode_local_change_with_length(
    encoded: &[u8],
) -> Result<DecodedLocalChange, LocalChangeCodecError> {
    let mut input = Input::new(encoded);
    if input.array::<4>()? != *MAGIC {
        return Err(malformed("magic is invalid"));
    }
    let format = input.u16()?;
    if format != FORMAT {
        return Err(LocalChangeCodecError::UnsupportedFormat(format));
    }
    let kind = input.u8()?;
    if input.u8()? != RESERVED {
        return Err(malformed("reserved header byte is non-zero"));
    }
    let body_bytes = input.length("body")?;
    let peer_encoded_bytes = input.u64()?;
    if peer_encoded_bytes == 0 {
        return Err(malformed("peer encoded length is zero"));
    }
    if body_bytes != input.remaining() {
        return Err(malformed("body length disagrees with the record length"));
    }
    let change = decode_body(kind, &mut input)?;
    input.finish()?;
    Ok(DecodedLocalChange {
        change,
        peer_encoded_bytes,
    })
}

pub(crate) fn encoded_change_len(change: &LocalChange) -> Result<u64, LocalChangeCodecError> {
    let mut counter = ChangeByteCounter(0);
    serde_json::to_writer(&mut counter, change).map_err(LocalChangeCodecError::PeerEncoding)?;
    Ok(counter.0)
}

fn encode_body(change: &LocalChange) -> Result<(u8, Vec<u8>), LocalChangeCodecError> {
    let mut body = Vec::new();
    match change {
        LocalChange::ObjectHead(change) => {
            put_u64(&mut body, change.offset);
            put_u64(&mut body, change.tenant_id);
            put_u64(&mut body, change.bucket_id);
            put_string(&mut body, &change.exact_path)?;
            put_u64(&mut body, change.path_version.0);
            put_u8(
                &mut body,
                match change.kind {
                    ObjectHeadChangeKind::Put => PUT,
                    ObjectHeadChangeKind::Delete => DELETE,
                },
            );
            put_reference_deltas(&mut body, &change.reference_deltas)?;
            put_accounting_transition(&mut body, change.accounting_transition);
            put_definition_transition(&mut body, change.definition_transition.as_ref())?;
            Ok((OBJECT_HEAD, body))
        }
        LocalChange::RetainedVersionDeleted(change) => {
            put_u64(&mut body, change.offset);
            put_u64(&mut body, change.tenant_id);
            put_u64(&mut body, change.bucket_id);
            put_string(&mut body, &change.exact_path)?;
            put_u64(&mut body, change.deleted_version.0);
            put_optional_u64(
                &mut body,
                change.resulting_head_version.map(|version| version.0),
            );
            put_reference_deltas(&mut body, &change.reference_deltas)?;
            put_accounting_transition(&mut body, change.accounting_transition);
            Ok((RETAINED_VERSION_DELETED, body))
        }
        LocalChange::AggregateChanged(change) => {
            put_u64(&mut body, change.offset);
            put_u8(
                &mut body,
                match change.aggregate_kind {
                    AggregateKind::ZanzibarRealm => ZANZIBAR_REALM,
                    AggregateKind::LogicalRecord => LOGICAL_RECORD,
                },
            );
            put_bytes(&mut body, &change.aggregate_key)?;
            put_u64(&mut body, change.revision);
            Ok((AGGREGATE_CHANGED, body))
        }
        LocalChange::ContentLifecycleChanged(change) => {
            put_u64(&mut body, change.offset);
            put_bytes(&mut body, &change.blob_identity)?;
            put_u64(&mut body, change.revision);
            put_reference_deltas(&mut body, &change.reference_deltas)?;
            Ok((CONTENT_LIFECYCLE_CHANGED, body))
        }
    }
}

fn decode_body(kind: u8, input: &mut Input<'_>) -> Result<LocalChange, LocalChangeCodecError> {
    match kind {
        OBJECT_HEAD => Ok(LocalChange::ObjectHead(ObjectHeadChange {
            offset: input.u64()?,
            tenant_id: input.u64()?,
            bucket_id: input.u64()?,
            exact_path: input.string()?,
            path_version: VersionId(input.u64()?),
            kind: match input.u8()? {
                PUT => ObjectHeadChangeKind::Put,
                DELETE => ObjectHeadChangeKind::Delete,
                _ => return Err(malformed("object-head operation is unknown")),
            },
            reference_deltas: input.reference_deltas()?,
            accounting_transition: input.accounting_transition()?,
            definition_transition: input.definition_transition()?,
        })),
        RETAINED_VERSION_DELETED => Ok(LocalChange::RetainedVersionDeleted(
            RetainedVersionDeletedChange {
                offset: input.u64()?,
                tenant_id: input.u64()?,
                bucket_id: input.u64()?,
                exact_path: input.string()?,
                deleted_version: VersionId(input.u64()?),
                resulting_head_version: input.optional_u64()?.map(VersionId),
                reference_deltas: input.reference_deltas()?,
                accounting_transition: input.accounting_transition()?,
            },
        )),
        AGGREGATE_CHANGED => Ok(LocalChange::AggregateChanged(AggregateChanged {
            offset: input.u64()?,
            aggregate_kind: match input.u8()? {
                ZANZIBAR_REALM => AggregateKind::ZanzibarRealm,
                LOGICAL_RECORD => AggregateKind::LogicalRecord,
                _ => return Err(malformed("aggregate kind is unknown")),
            },
            aggregate_key: input.bytes()?.to_vec(),
            revision: input.u64()?,
        })),
        CONTENT_LIFECYCLE_CHANGED => Ok(LocalChange::ContentLifecycleChanged(
            ContentLifecycleChanged {
                offset: input.u64()?,
                blob_identity: input.bytes()?.to_vec(),
                revision: input.u64()?,
                reference_deltas: input.reference_deltas()?,
            },
        )),
        _ => Err(malformed("local change kind is unknown")),
    }
}

fn put_reference_deltas(
    output: &mut Vec<u8>,
    deltas: &[ReferenceDelta],
) -> Result<(), LocalChangeCodecError> {
    put_u64(
        output,
        u64::try_from(deltas.len()).map_err(|_| malformed("reference-delta count is exhausted"))?,
    );
    for delta in deltas {
        output.extend_from_slice(&delta.blob.hash);
        put_u64(output, delta.blob.length);
        output.extend_from_slice(&delta.change.to_be_bytes());
    }
    Ok(())
}

fn put_accounting_transition(output: &mut Vec<u8>, transition: Option<AccountingHeadTransition>) {
    match transition {
        Some(transition) => {
            put_u8(output, 1);
            put_u8(output, AccountingHeadTransition::FORMAT);
            put_optional_u64(output, transition.previous_live_length);
            put_optional_u64(output, transition.current_live_length);
        }
        None => put_u8(output, 0),
    }
}

fn put_definition_transition(
    output: &mut Vec<u8>,
    transition: Option<&DefinitionTransition>,
) -> Result<(), LocalChangeCodecError> {
    let Some(transition) = transition else {
        put_u8(output, 0);
        return Ok(());
    };
    put_u8(output, 1);
    put_u8(output, transition.kind as u8);
    put_u64(output, transition.tenant_id);
    put_u64(output, transition.bucket_id);
    put_u64(output, transition.definition_id);
    put_string(output, &transition.path)?;
    put_u64(output, transition.object_version.0);
    put_u8(output, transition.operation as u8);
    Ok(())
}

fn put_optional_u64(output: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            put_u8(output, 1);
            put_u64(output, value);
        }
        None => put_u8(output, 0),
    }
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<(), LocalChangeCodecError> {
    put_bytes(output, value.as_bytes())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), LocalChangeCodecError> {
    put_u64(
        output,
        u64::try_from(value.len()).map_err(|_| malformed("byte-string length is exhausted"))?,
    );
    output.extend_from_slice(value);
    Ok(())
}

fn put_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

struct Input<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn finish(&self) -> Result<(), LocalChangeCodecError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(malformed("trailing bytes are present"))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], LocalChangeCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| malformed("field length is exhausted"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| malformed("record is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], LocalChangeCodecError> {
        Ok(self.take(N)?.try_into().expect("exact fixed-width slice"))
    }

    fn u8(&mut self) -> Result<u8, LocalChangeCodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, LocalChangeCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, LocalChangeCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn length(&mut self, field: &str) -> Result<usize, LocalChangeCodecError> {
        let length = usize::try_from(self.u64()?)
            .map_err(|_| malformed(format!("{field} length does not fit this platform")))?;
        if length > self.remaining() {
            return Err(malformed(format!(
                "{field} length exceeds the remaining record"
            )));
        }
        Ok(length)
    }

    fn bytes(&mut self) -> Result<&'a [u8], LocalChangeCodecError> {
        let length = self.length("byte string")?;
        self.take(length)
    }

    fn string(&mut self) -> Result<String, LocalChangeCodecError> {
        let bytes = self.bytes()?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| malformed("string field is not valid UTF-8"))?;
        Ok(value.to_owned())
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, LocalChangeCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(malformed("optional integer tag is invalid")),
        }
    }

    fn reference_deltas(&mut self) -> Result<Vec<ReferenceDelta>, LocalChangeCodecError> {
        const DELTA_BYTES: usize = 32 + 8 + 8;
        let count = self.length_count("reference-delta", DELTA_BYTES)?;
        let mut deltas = Vec::new();
        deltas
            .try_reserve_exact(count)
            .map_err(|_| malformed("reference-delta allocation failed"))?;
        for _ in 0..count {
            deltas.push(ReferenceDelta {
                blob: BlobRef {
                    hash: self.array()?,
                    length: self.u64()?,
                },
                change: i64::from_be_bytes(self.array()?),
            });
        }
        Ok(deltas)
    }

    fn length_count(
        &mut self,
        field: &str,
        minimum_item_bytes: usize,
    ) -> Result<usize, LocalChangeCodecError> {
        let count = usize::try_from(self.u64()?)
            .map_err(|_| malformed(format!("{field} count does not fit this platform")))?;
        if count > self.remaining() / minimum_item_bytes {
            return Err(malformed(format!(
                "{field} count exceeds the remaining record"
            )));
        }
        Ok(count)
    }

    fn accounting_transition(
        &mut self,
    ) -> Result<Option<AccountingHeadTransition>, LocalChangeCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let format = self.u8()?;
                if format != AccountingHeadTransition::FORMAT {
                    return Err(malformed("accounting transition format is unsupported"));
                }
                Ok(Some(AccountingHeadTransition::new(
                    self.optional_u64()?,
                    self.optional_u64()?,
                )))
            }
            _ => Err(malformed("accounting transition tag is invalid")),
        }
    }

    fn definition_transition(
        &mut self,
    ) -> Result<Option<DefinitionTransition>, LocalChangeCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(DefinitionTransition {
                kind: DefinitionKind::from_byte(self.u8()?)
                    .map_err(|error| malformed(error.to_string()))?,
                tenant_id: self.u64()?,
                bucket_id: self.u64()?,
                definition_id: self.u64()?,
                path: self.string()?,
                object_version: VersionId(self.u64()?),
                operation: DefinitionOperation::from_byte(self.u8()?)
                    .map_err(|error| malformed(error.to_string()))?,
            })),
            _ => Err(malformed("definition transition tag is invalid")),
        }
    }
}

struct ChangeByteCounter(u64);

impl std::io::Write for ChangeByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("local change length overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn malformed(message: impl Into<String>) -> LocalChangeCodecError {
    LocalChangeCodecError::Malformed(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_change() -> LocalChange {
        LocalChange::object_head(
            7,
            11,
            12,
            "documents/one".into(),
            VersionId(41),
            false,
            vec![ReferenceDelta {
                blob: BlobRef {
                    hash: [9; 32],
                    length: 1_024,
                },
                change: 1,
            }],
            Some(AccountingHeadTransition::new(None, Some(1_024))),
            Some(DefinitionTransition {
                kind: DefinitionKind::Index,
                tenant_id: 11,
                bucket_id: 12,
                definition_id: 13,
                path: "_keldra/indexes/13".into(),
                object_version: VersionId(41),
                operation: DefinitionOperation::Upsert,
            }),
        )
    }

    #[test]
    fn every_change_kind_round_trips_with_exact_peer_length() {
        let changes = [
            object_change(),
            LocalChange::retained_version_deleted(
                8,
                11,
                12,
                "documents/one".into(),
                VersionId(40),
                Some(VersionId(41)),
                Vec::new(),
                Some(AccountingHeadTransition::new(Some(10), Some(12))),
            ),
            LocalChange::aggregate_changed(9, AggregateKind::LogicalRecord, vec![1, 2, 3], 4),
            LocalChange::content_lifecycle_changed(
                10,
                vec![4, 5, 6],
                5,
                vec![ReferenceDelta {
                    blob: BlobRef {
                        hash: [8; 32],
                        length: 55,
                    },
                    change: -1,
                }],
            ),
        ];

        for change in changes {
            let encoded = encode_local_change(&change).unwrap();
            let decoded = decode_local_change_with_length(&encoded).unwrap();
            assert_eq!(decoded.change, change);
            assert_eq!(
                decoded.peer_encoded_bytes,
                serde_json::to_vec(&change).unwrap().len() as u64
            );
        }
    }

    #[test]
    fn aggregate_change_has_an_architecture_independent_golden_encoding() {
        let change =
            LocalChange::aggregate_changed(9, AggregateKind::LogicalRecord, vec![1, 2, 3], 4);
        let expected = vec![
            b'A',
            b'N',
            b'V',
            b'J', // magic
            0,
            2, // format
            AGGREGATE_CHANGED,
            0, // reserved
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            28, // body bytes
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            121, // peer JSON bytes
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            9, // offset
            LOGICAL_RECORD,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            3, // aggregate-key bytes
            1,
            2,
            3,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            4, // revision
        ];
        assert_eq!(encode_local_change(&change).unwrap(), expected);
        assert_eq!(decode_local_change(&expected).unwrap(), change);
    }

    #[test]
    fn malformed_headers_lengths_tags_and_utf8_fail_closed() {
        let encoded = encode_local_change(&object_change()).unwrap();
        let mutations: [fn(&mut Vec<u8>); 4] = [
            |bytes| bytes[0] ^= 1,
            |bytes| bytes[7] = 1,
            |bytes| bytes[15] = bytes[15].wrapping_add(1),
            |bytes| bytes[6] = 99,
        ];
        for mutation in mutations {
            let mut corrupt = encoded.clone();
            mutation(&mut corrupt);
            assert!(decode_local_change(&corrupt).is_err());
        }

        let mut truncated = encoded.clone();
        truncated.pop();
        assert!(decode_local_change(&truncated).is_err());

        // Object body begins with three u64 fields; the path starts after its
        // u64 length. Corrupt one UTF-8 byte without changing any length.
        let mut invalid_utf8 = encoded;
        invalid_utf8[HEADER_BYTES + 32] = 0xff;
        assert!(decode_local_change(&invalid_utf8).is_err());
    }
}
