//! Compact persistence encoding for object heads and version descriptors.
//!
//! Both records use one architecture-independent v1 layout. Integers are
//! big-endian, optional fields are named by flag bits, and decoding requires
//! the complete input to be consumed. These are internal RocksDB values, so a
//! clean pre-1.0 format break intentionally has no legacy JSON decoder.

use super::{StoredVersion, StoredVersionRetention};
use crate::{
    BlobRef, Head, MAX_CONTENT_TYPE_BYTES, MUTATION_STAMP_FORMAT, MutationError, MutationStamp,
    PlacementLogId, SourceId, Version, VersionId,
};

pub(crate) const OBJECT_METADATA_FORMAT_KEY: &[u8] = b"object_metadata_format_current_v1";
const OBJECT_METADATA_FORMAT: u8 = 1;
const HEAD_MAGIC: &[u8; 4] = b"KHED";
const VERSION_MAGIC: &[u8; 4] = b"KVER";
const FORMAT: u8 = 1;

const HEAD_DELETED: u8 = 1 << 0;
const HEAD_HAS_STAMP: u8 = 1 << 1;
const HEAD_HAS_PREDECESSOR: u8 = 1 << 2;
const HEAD_HAS_PROGRAM_CURSOR: u8 = 1 << 3;
const HEAD_FLAGS: u8 =
    HEAD_DELETED | HEAD_HAS_STAMP | HEAD_HAS_PREDECESSOR | HEAD_HAS_PROGRAM_CURSOR;

const VERSION_RETENTION_MASK: u8 = 0b0000_0011;
const VERSION_HAS_BLOB: u8 = 1 << 2;
const VERSION_HAS_CONTENT_TYPE: u8 = 1 << 3;
const VERSION_DELETED: u8 = 1 << 4;
const VERSION_PROTECTED_LINK_DESCRIPTOR: u8 = 1 << 5;
const VERSION_FLAGS: u8 = VERSION_RETENTION_MASK
    | VERSION_HAS_BLOB
    | VERSION_HAS_CONTENT_TYPE
    | VERSION_DELETED
    | VERSION_PROTECTED_LINK_DESCRIPTOR;

pub(crate) fn encode_head(head: &Head) -> Result<Vec<u8>, MutationError> {
    validate_head(head)?;
    let mut flags = u8::from(head.deleted) * HEAD_DELETED;
    if let Some(stamp) = head.mutation_stamp {
        flags |= HEAD_HAS_STAMP;
        if stamp.predecessor_version.is_some() {
            flags |= HEAD_HAS_PREDECESSOR;
        }
        if stamp.program_commit_cursor.is_some() {
            flags |= HEAD_HAS_PROGRAM_CURSOR;
        }
    }

    let mut encoded = Vec::with_capacity(if head.mutation_stamp.is_some() {
        130
    } else {
        14
    });
    encoded.extend_from_slice(HEAD_MAGIC);
    encoded.push(FORMAT);
    encoded.push(flags);
    put_u64(&mut encoded, head.version.0);
    if let Some(stamp) = head.mutation_stamp {
        put_u16(&mut encoded, stamp.format);
        if let Some(predecessor) = stamp.predecessor_version {
            put_u64(&mut encoded, predecessor.0);
        }
        if let Some(cursor) = stamp.program_commit_cursor {
            put_u64(&mut encoded, cursor);
        }
        encoded.extend_from_slice(&stamp.mutation_fingerprint);
        put_u64(&mut encoded, stamp.active_placement_log_id.term);
        put_u64(&mut encoded, stamp.active_placement_log_id.index);
        put_u64(&mut encoded, stamp.serving_fence_term);
        put_u16(&mut encoded, stamp.source_id.node_id);
        encoded.extend_from_slice(&stamp.source_id.source_epoch);
        put_u64(&mut encoded, stamp.source_journal_position);
    }
    Ok(encoded)
}

pub(crate) fn decode_head(encoded: &[u8]) -> Result<Head, MutationError> {
    let mut input = Input::new(encoded, "object head");
    input.magic(HEAD_MAGIC)?;
    input.format()?;
    let flags = input.u8()?;
    if flags & !HEAD_FLAGS != 0 {
        return Err(malformed("object head flags contain unknown bits"));
    }
    let has_stamp = flags & HEAD_HAS_STAMP != 0;
    if !has_stamp && flags & (HEAD_HAS_PREDECESSOR | HEAD_HAS_PROGRAM_CURSOR) != 0 {
        return Err(malformed(
            "object head stamp fields are present without a mutation stamp",
        ));
    }
    let version = VersionId(input.u64()?);
    let mutation_stamp = if has_stamp {
        let stamp_format = input.u16()?;
        if stamp_format != MUTATION_STAMP_FORMAT {
            return Err(malformed(
                "object head mutation stamp format is unsupported",
            ));
        }
        Some(MutationStamp {
            format: stamp_format,
            predecessor_version: if flags & HEAD_HAS_PREDECESSOR != 0 {
                Some(VersionId(input.u64()?))
            } else {
                None
            },
            program_commit_cursor: if flags & HEAD_HAS_PROGRAM_CURSOR != 0 {
                Some(input.u64()?)
            } else {
                None
            },
            mutation_fingerprint: input.array()?,
            active_placement_log_id: PlacementLogId {
                term: input.u64()?,
                index: input.u64()?,
            },
            serving_fence_term: input.u64()?,
            source_id: SourceId {
                node_id: input.u16()?,
                source_epoch: input.array()?,
            },
            source_journal_position: input.u64()?,
        })
    } else {
        None
    };
    input.finish()?;
    let head = Head {
        version,
        deleted: flags & HEAD_DELETED != 0,
        mutation_stamp,
    };
    validate_head(&head)?;
    Ok(head)
}

pub(crate) fn validate_head(head: &Head) -> Result<(), MutationError> {
    if head.version.0 == 0 {
        return Err(malformed("object head version is zero"));
    }
    let Some(stamp) = head.mutation_stamp else {
        return Ok(());
    };
    if stamp.format != MUTATION_STAMP_FORMAT {
        return Err(malformed(
            "object head mutation stamp format is unsupported",
        ));
    }
    if stamp
        .predecessor_version
        .is_some_and(|predecessor| predecessor.0 == 0 || predecessor >= head.version)
    {
        return Err(malformed(
            "object head predecessor does not precede the head version",
        ));
    }
    if stamp.program_commit_cursor == Some(0) {
        return Err(malformed("object head program commit cursor is zero"));
    }
    if stamp.active_placement_log_id.term == 0 || stamp.active_placement_log_id.index == 0 {
        return Err(malformed("object head placement log identity is zero"));
    }
    if stamp.serving_fence_term == 0 {
        return Err(malformed("object head serving fence term is zero"));
    }
    if stamp.source_id.node_id == 0 || stamp.source_id.source_epoch == [0; 32] {
        return Err(malformed("object head source identity is zero"));
    }
    if stamp.source_journal_position == 0 {
        return Err(malformed("object head source journal position is zero"));
    }
    Ok(())
}

pub(crate) fn initialize_object_metadata_format(
    db: &rocksdb::DB,
    metadata: &impl rocksdb::AsColumnFamilyRef,
    existing_database: bool,
    sync_writes: bool,
) -> anyhow::Result<()> {
    match db.get_cf(metadata, OBJECT_METADATA_FORMAT_KEY)? {
        Some(encoded) if encoded.as_ref() == [OBJECT_METADATA_FORMAT] => Ok(()),
        Some(_) => anyhow::bail!("object metadata persistence format marker is unsupported"),
        None if existing_database => {
            anyhow::bail!("existing Keldra volume has no object metadata persistence format marker")
        }
        None => {
            let mut write = rocksdb::WriteOptions::default();
            write.set_sync(sync_writes);
            db.put_cf_opt(
                metadata,
                OBJECT_METADATA_FORMAT_KEY,
                [OBJECT_METADATA_FORMAT],
                &write,
            )?;
            Ok(())
        }
    }
}

pub(crate) fn encode_stored_version(stored: &StoredVersion) -> Result<Vec<u8>, MutationError> {
    crate::model::validate_version_descriptor(&stored.version)?;
    let retention = match stored.retention {
        StoredVersionRetention::JournalPending => 0,
        StoredVersionRetention::JournalReleased => 1,
        StoredVersionRetention::UserRetained => 2,
    };
    let version = &stored.version;
    let mut flags = retention;
    if version.blob.is_some() {
        flags |= VERSION_HAS_BLOB;
    }
    if version.content_type.is_some() {
        flags |= VERSION_HAS_CONTENT_TYPE;
    }
    if version.deleted {
        flags |= VERSION_DELETED;
    }
    if version.protected_link_descriptor {
        flags |= VERSION_PROTECTED_LINK_DESCRIPTOR;
    }

    let content_bytes = version
        .content_type
        .as_deref()
        .map(str::as_bytes)
        .unwrap_or_default();
    let content_length = u16::try_from(content_bytes.len())
        .map_err(|_| malformed("stored version content type exceeds the format bound"))?;
    let mut encoded = Vec::with_capacity(
        22 + version.blob.as_ref().map_or(0, |_| 40)
            + version
                .content_type
                .as_ref()
                .map_or(0, |_| 2 + content_bytes.len()),
    );
    encoded.extend_from_slice(VERSION_MAGIC);
    encoded.push(FORMAT);
    encoded.push(flags);
    put_u64(&mut encoded, version.id.0);
    put_u64(&mut encoded, version.committed_at_unix_millis);
    if let Some(blob) = &version.blob {
        encoded.extend_from_slice(&blob.hash);
        put_u64(&mut encoded, blob.length);
    }
    if version.content_type.is_some() {
        put_u16(&mut encoded, content_length);
        encoded.extend_from_slice(content_bytes);
    }
    Ok(encoded)
}

pub(crate) fn decode_stored_version(encoded: &[u8]) -> Result<StoredVersion, MutationError> {
    let mut input = Input::new(encoded, "stored version");
    input.magic(VERSION_MAGIC)?;
    input.format()?;
    let flags = input.u8()?;
    if flags & !VERSION_FLAGS != 0 {
        return Err(malformed("stored version flags contain unknown bits"));
    }
    let retention = match flags & VERSION_RETENTION_MASK {
        0 => StoredVersionRetention::JournalPending,
        1 => StoredVersionRetention::JournalReleased,
        2 => StoredVersionRetention::UserRetained,
        _ => return Err(malformed("stored version retention tag is invalid")),
    };
    let id = VersionId(input.u64()?);
    let committed_at_unix_millis = input.u64()?;
    let blob = if flags & VERSION_HAS_BLOB != 0 {
        Some(BlobRef {
            hash: input.array()?,
            length: input.u64()?,
        })
    } else {
        None
    };
    let content_type = if flags & VERSION_HAS_CONTENT_TYPE != 0 {
        let length = usize::from(input.u16()?);
        if length > MAX_CONTENT_TYPE_BYTES {
            return Err(malformed(
                "stored version content type exceeds the format bound",
            ));
        }
        let bytes = input.take(length)?;
        Some(
            std::str::from_utf8(bytes)
                .map_err(|_| malformed("stored version content type is not UTF-8"))?
                .to_owned(),
        )
    } else {
        None
    };
    input.finish()?;
    let stored = StoredVersion::new(
        Version {
            id,
            blob,
            content_type,
            deleted: flags & VERSION_DELETED != 0,
            committed_at_unix_millis,
            protected_link_descriptor: flags & VERSION_PROTECTED_LINK_DESCRIPTOR != 0,
        },
        retention,
    );
    crate::model::validate_version_descriptor(&stored.version)
        .map_err(|_| malformed("stored version contains a malformed version descriptor"))?;
    Ok(stored)
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
    record: &'static str,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8], record: &'static str) -> Self {
        Self {
            bytes,
            position: 0,
            record,
        }
    }

    fn magic(&mut self, expected: &[u8; 4]) -> Result<(), MutationError> {
        if self.array::<4>()? == *expected {
            Ok(())
        } else {
            Err(malformed(format!("{} magic is invalid", self.record)))
        }
    }

    fn format(&mut self) -> Result<(), MutationError> {
        let format = self.u8()?;
        if format == FORMAT {
            Ok(())
        } else {
            Err(MutationError::Storage(format!(
                "unsupported {} persistence format {format}",
                self.record
            )))
        }
    }

    fn finish(&self) -> Result<(), MutationError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(malformed(format!("{} has trailing bytes", self.record)))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], MutationError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| malformed(format!("{} field length is exhausted", self.record)))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| malformed(format!("{} is truncated", self.record)))?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MutationError> {
        Ok(self.take(N)?.try_into().expect("exact fixed-width slice"))
    }

    fn u8(&mut self) -> Result<u8, MutationError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, MutationError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, MutationError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
}

fn malformed(message: impl Into<String>) -> MutationError {
    MutationError::Storage(format!("object metadata is malformed: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MUTATION_STAMP_FORMAT;

    #[test]
    fn local_head_has_a_compact_golden_encoding() {
        let head = Head {
            version: VersionId(7),
            deleted: true,
            mutation_stamp: None,
        };
        let expected = vec![
            b'K',
            b'H',
            b'E',
            b'D',
            1,
            HEAD_DELETED,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            7,
        ];
        assert_eq!(encode_head(&head).unwrap(), expected);
        assert_eq!(decode_head(&expected).unwrap(), head);
    }

    #[test]
    fn distributed_head_round_trips_every_stamp_field() {
        let head = Head {
            version: VersionId(17),
            deleted: false,
            mutation_stamp: Some(MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(VersionId(16)),
                program_commit_cursor: Some(15),
                mutation_fingerprint: [14; 32],
                active_placement_log_id: PlacementLogId {
                    term: 13,
                    index: 12,
                },
                serving_fence_term: 11,
                source_id: SourceId {
                    node_id: 10,
                    source_epoch: [9; 32],
                },
                source_journal_position: 8,
            }),
        };
        let encoded = encode_head(&head).unwrap();
        assert_eq!(encoded.len(), 130);
        assert_eq!(decode_head(&encoded).unwrap(), head);
    }

    #[test]
    fn stored_version_has_a_compact_golden_encoding() {
        let stored = StoredVersion::new(
            Version {
                id: VersionId(2),
                blob: Some(BlobRef {
                    hash: [3; 32],
                    length: 4,
                }),
                content_type: Some("x/y".into()),
                deleted: false,
                committed_at_unix_millis: 5,
                protected_link_descriptor: false,
            },
            StoredVersionRetention::UserRetained,
        );
        let mut expected = vec![
            b'K',
            b'V',
            b'E',
            b'R',
            1,
            2 | VERSION_HAS_BLOB | VERSION_HAS_CONTENT_TYPE,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            5,
        ];
        expected.extend_from_slice(&[3; 32]);
        expected.extend_from_slice(&4_u64.to_be_bytes());
        expected.extend_from_slice(&3_u16.to_be_bytes());
        expected.extend_from_slice(b"x/y");
        assert_eq!(encode_stored_version(&stored).unwrap(), expected);
        assert_eq!(decode_stored_version(&expected).unwrap(), stored);
    }

    #[test]
    fn tombstone_stored_version_round_trips_without_optional_fields() {
        let stored = StoredVersion::new(
            Version {
                id: VersionId(9),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: 10,
                protected_link_descriptor: false,
            },
            StoredVersionRetention::JournalReleased,
        );
        let encoded = encode_stored_version(&stored).unwrap();
        assert_eq!(encoded.len(), 22);
        assert_eq!(decode_stored_version(&encoded).unwrap(), stored);
    }

    #[test]
    fn legacy_json_and_malformed_binary_are_rejected() {
        let legacy_head = br#"{"version":7,"deleted":false,"mutation_stamp":null}"#;
        let legacy_stored_version = br#"{"format":1,"retention":"journal_pending","version":{"id":1,"blob":null,"content_type":null,"deleted":true,"committed_at_unix_millis":1,"protected_link_descriptor":false}}"#;
        assert!(decode_head(legacy_head).is_err());
        assert!(decode_stored_version(legacy_stored_version).is_err());

        let mut head = encode_head(&Head {
            version: VersionId(1),
            deleted: false,
            mutation_stamp: None,
        })
        .unwrap();
        head[5] = 0x80;
        assert!(decode_head(&head).is_err());

        let mut version = encode_stored_version(&StoredVersion::new(
            Version {
                id: VersionId(1),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: 1,
                protected_link_descriptor: false,
            },
            StoredVersionRetention::JournalPending,
        ))
        .unwrap();
        version.push(0);
        assert!(decode_stored_version(&version).is_err());
    }

    #[test]
    fn malformed_head_lineage_is_rejected_on_encode_and_decode() {
        let mut head = Head {
            version: VersionId(17),
            deleted: false,
            mutation_stamp: Some(MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(VersionId(16)),
                program_commit_cursor: None,
                mutation_fingerprint: [14; 32],
                active_placement_log_id: PlacementLogId {
                    term: 13,
                    index: 12,
                },
                serving_fence_term: 11,
                source_id: SourceId {
                    node_id: 10,
                    source_epoch: [9; 32],
                },
                source_journal_position: 8,
            }),
        };
        let encoded = encode_head(&head).unwrap();

        head.mutation_stamp
            .as_mut()
            .unwrap()
            .source_journal_position = 0;
        assert!(encode_head(&head).is_err());

        let mut malformed = encoded;
        let journal_position = malformed.len() - 8;
        malformed[journal_position..].fill(0);
        assert!(decode_head(&malformed).is_err());
    }

    #[test]
    fn every_truncated_record_and_unknown_format_is_rejected() {
        let head = Head {
            version: VersionId(17),
            deleted: false,
            mutation_stamp: Some(MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(VersionId(16)),
                program_commit_cursor: None,
                mutation_fingerprint: [14; 32],
                active_placement_log_id: PlacementLogId {
                    term: 13,
                    index: 12,
                },
                serving_fence_term: 11,
                source_id: SourceId {
                    node_id: 10,
                    source_epoch: [9; 32],
                },
                source_journal_position: 8,
            }),
        };
        let encoded_head = encode_head(&head).unwrap();
        for end in 0..encoded_head.len() {
            assert!(
                decode_head(&encoded_head[..end]).is_err(),
                "head prefix {end}"
            );
        }
        let mut unknown_head = encoded_head;
        unknown_head[4] = 2;
        assert!(decode_head(&unknown_head).is_err());

        let stored = StoredVersion::new(
            Version {
                id: VersionId(2),
                blob: Some(BlobRef {
                    hash: [3; 32],
                    length: 4,
                }),
                content_type: Some("application/json".into()),
                deleted: false,
                committed_at_unix_millis: 5,
                protected_link_descriptor: false,
            },
            StoredVersionRetention::UserRetained,
        );
        let encoded_version = encode_stored_version(&stored).unwrap();
        for end in 0..encoded_version.len() {
            assert!(
                decode_stored_version(&encoded_version[..end]).is_err(),
                "stored-version prefix {end}"
            );
        }
        let mut unknown_version = encoded_version;
        unknown_version[5] =
            (unknown_version[5] & !VERSION_RETENTION_MASK) | VERSION_RETENTION_MASK;
        assert!(decode_stored_version(&unknown_version).is_err());
    }
}
