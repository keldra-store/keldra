//! Stable compact encoding for an authoritative [`ObjectMutation`].
//!
//! `KOMU` v1 has a 12-byte header: magic `[u8; 4]`, codec version `u16`,
//! zero flags `u16`, and body length `u32`. The body follows the declaration
//! order of `ObjectMutation` and every nested struct. Integers are big-endian,
//! strings are `u32 length + UTF-8`, vectors are `u32 count + items`, options
//! use tags 0/1, and booleans use values 0/1. Enum tags are explicit below.
//!
//! Body field map:
//!
//! ```text
//! mutation: format:u16, tenant:u64, bucket:u64, path:str, command:str,
//!   input_fingerprint:[32], version, receipt_expiry:u64, stamp,
//!   reference_deltas:vec, accounting:opt, definition:opt, alias_snapshot:opt
//! version: id:u64, blob:opt(hash:[32], length:u64), content_type:opt(str),
//!   deleted:bool, committed_at:u64, protected_link_descriptor:bool
//! stamp: format:u16, predecessor:opt(u64), program_cursor:opt(u64),
//!   mutation_fingerprint:[32], placement_term:u64, placement_index:u64,
//!   serving_fence_term:u64, source_node:u16, source_epoch:[32], position:u64
//! reference_delta: blob(hash:[32], length:u64), change:i64
//! accounting: format:u8, previous_live_length:opt(u64), current_live_length:opt(u64)
//! definition: kind:u8 (index=1, accounting=2), tenant:u64, bucket:u64,
//!   definition_id:u64, path:str, object_version:u64,
//!   operation:u8 (upsert=1, delete=2)
//! alias_snapshot: registry, canonical_version
//! registry: format:u16, revision:u64, aliases:vec(str), program_cursor:opt(u64)
//! ```

use thiserror::Error;

use super::*;
use crate::{
    DefinitionKind, DefinitionOperation, MAX_CONTENT_TYPE_BYTES, MAX_INBOUND_OBJECT_LINKS,
    MAX_OBJECT_MUTATION_REFERENCE_DELTAS, MutationStamp, ObjectAliasRegistry, ObjectAliasSnapshot,
    ObjectMutation, PlacementLogId,
};

const MAGIC: &[u8; 4] = b"KOMU";
const CODEC_FORMAT: u16 = 1;
const FLAGS: u16 = 0;
const HEADER_BYTES: usize = 4 + 2 + 2 + 4;
const MAX_ENCODED_BYTES: usize = 16 * 1024 * 1024;
const MAX_COMMAND_ID_BYTES: usize = 256;

const DEFINITION_INDEX: u8 = 1;
const DEFINITION_ACCOUNTING: u8 = 2;
const DEFINITION_UPSERT: u8 = 1;
const DEFINITION_DELETE: u8 = 2;

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ObjectMutationCodecError {
    #[error("object mutation record is malformed: {0}")]
    Malformed(String),
    #[error("unsupported object mutation codec format {0}")]
    UnsupportedFormat(u16),
}

pub(crate) fn encode_object_mutation(
    mutation: &ObjectMutation,
) -> Result<Vec<u8>, ObjectMutationCodecError> {
    mutation
        .validate()
        .map_err(|error| malformed(error.to_string()))?;

    let mut body = Vec::new();
    put_u16(&mut body, mutation.format);
    put_u64(&mut body, mutation.tenant_id);
    put_u64(&mut body, mutation.bucket_id);
    put_string(&mut body, &mutation.exact_path)?;
    put_string(&mut body, &mutation.command_id)?;
    body.extend_from_slice(&mutation.input_fingerprint);
    put_version(&mut body, &mutation.version)?;
    put_u64(&mut body, mutation.receipt_expires_at_unix_millis);
    put_stamp(&mut body, mutation.stamp);
    put_u32(
        &mut body,
        u32::try_from(mutation.reference_deltas.len())
            .map_err(|_| malformed("reference-delta count is exhausted"))?,
    );
    for delta in &mutation.reference_deltas {
        put_blob(&mut body, &delta.blob);
        put_i64(&mut body, delta.change);
    }
    put_accounting_transition(&mut body, mutation.accounting_transition);
    put_definition_transition(&mut body, mutation.definition_transition.as_ref())?;
    put_alias_snapshot(&mut body, mutation.alias_snapshot.as_ref())?;

    let total_bytes = HEADER_BYTES
        .checked_add(body.len())
        .ok_or_else(|| malformed("record length is exhausted"))?;
    if total_bytes > MAX_ENCODED_BYTES {
        return Err(malformed("record exceeds the 16 MiB format bound"));
    }
    let body_bytes =
        u32::try_from(body.len()).map_err(|_| malformed("body length does not fit the format"))?;
    let mut encoded = Vec::with_capacity(total_bytes);
    encoded.extend_from_slice(MAGIC);
    put_u16(&mut encoded, CODEC_FORMAT);
    put_u16(&mut encoded, FLAGS);
    put_u32(&mut encoded, body_bytes);
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

pub(crate) fn decode_object_mutation(
    encoded: &[u8],
) -> Result<ObjectMutation, ObjectMutationCodecError> {
    if encoded.len() > MAX_ENCODED_BYTES {
        return Err(malformed("record exceeds the 16 MiB format bound"));
    }
    let mut input = Input::new(encoded);
    if input.array::<4>()? != *MAGIC {
        return Err(malformed("magic is invalid"));
    }
    let codec_format = input.u16()?;
    if codec_format != CODEC_FORMAT {
        return Err(ObjectMutationCodecError::UnsupportedFormat(codec_format));
    }
    if input.u16()? != FLAGS {
        return Err(malformed("header flags are non-zero"));
    }
    let body_bytes = input.u32_length("body")?;
    if body_bytes != input.remaining() {
        return Err(malformed("body length disagrees with the record length"));
    }

    let mutation = ObjectMutation {
        format: input.u16()?,
        tenant_id: input.u64()?,
        bucket_id: input.u64()?,
        exact_path: input.string("exact path", keldra_atomic_program::MAX_OBJECT_PATH_BYTES)?,
        command_id: input.string("command ID", MAX_COMMAND_ID_BYTES)?,
        input_fingerprint: input.array()?,
        version: input.version()?,
        receipt_expires_at_unix_millis: input.u64()?,
        stamp: input.stamp()?,
        reference_deltas: input.reference_deltas()?,
        accounting_transition: input.accounting_transition()?,
        definition_transition: input.definition_transition()?,
        alias_snapshot: input.alias_snapshot()?,
    };
    input.finish()?;
    mutation
        .validate()
        .map_err(|error| malformed(error.to_string()))?;
    Ok(mutation)
}

fn put_version(output: &mut Vec<u8>, version: &Version) -> Result<(), ObjectMutationCodecError> {
    put_u64(output, version.id.0);
    put_optional(output, version.blob.as_ref(), put_blob);
    put_optional_result(output, version.content_type.as_deref(), put_string)?;
    put_bool(output, version.deleted);
    put_u64(output, version.committed_at_unix_millis);
    put_bool(output, version.protected_link_descriptor);
    Ok(())
}

fn put_stamp(output: &mut Vec<u8>, stamp: MutationStamp) {
    put_u16(output, stamp.format);
    put_optional_u64(output, stamp.predecessor_version.map(|version| version.0));
    put_optional_u64(output, stamp.program_commit_cursor);
    output.extend_from_slice(&stamp.mutation_fingerprint);
    put_u64(output, stamp.active_placement_log_id.term);
    put_u64(output, stamp.active_placement_log_id.index);
    put_u64(output, stamp.serving_fence_term);
    put_u16(output, stamp.source_id.node_id);
    output.extend_from_slice(&stamp.source_id.source_epoch);
    put_u64(output, stamp.source_journal_position);
}

fn put_blob(output: &mut Vec<u8>, blob: &BlobRef) {
    output.extend_from_slice(&blob.hash);
    put_u64(output, blob.length);
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
) -> Result<(), ObjectMutationCodecError> {
    let Some(transition) = transition else {
        put_u8(output, 0);
        return Ok(());
    };
    put_u8(output, 1);
    put_u8(
        output,
        match transition.kind {
            DefinitionKind::Index => DEFINITION_INDEX,
            DefinitionKind::Accounting => DEFINITION_ACCOUNTING,
        },
    );
    put_u64(output, transition.tenant_id);
    put_u64(output, transition.bucket_id);
    put_u64(output, transition.definition_id);
    put_string(output, &transition.path)?;
    put_u64(output, transition.object_version.0);
    put_u8(
        output,
        match transition.operation {
            DefinitionOperation::Upsert => DEFINITION_UPSERT,
            DefinitionOperation::Delete => DEFINITION_DELETE,
        },
    );
    Ok(())
}

fn put_alias_snapshot(
    output: &mut Vec<u8>,
    snapshot: Option<&ObjectAliasSnapshot>,
) -> Result<(), ObjectMutationCodecError> {
    let Some(snapshot) = snapshot else {
        put_u8(output, 0);
        return Ok(());
    };
    put_u8(output, 1);
    put_u16(output, snapshot.registry.format);
    put_u64(output, snapshot.registry.revision);
    put_u32(
        output,
        u32::try_from(snapshot.registry.aliases.len())
            .map_err(|_| malformed("alias count is exhausted"))?,
    );
    for alias in &snapshot.registry.aliases {
        put_string(output, alias)?;
    }
    put_optional_u64(output, snapshot.registry.program_commit_cursor);
    put_version(output, &snapshot.canonical_version)
}

fn put_optional<T>(output: &mut Vec<u8>, value: Option<&T>, put: fn(&mut Vec<u8>, &T)) {
    match value {
        Some(value) => {
            put_u8(output, 1);
            put(output, value);
        }
        None => put_u8(output, 0),
    }
}

fn put_optional_result<T: ?Sized>(
    output: &mut Vec<u8>,
    value: Option<&T>,
    put: fn(&mut Vec<u8>, &T) -> Result<(), ObjectMutationCodecError>,
) -> Result<(), ObjectMutationCodecError> {
    match value {
        Some(value) => {
            put_u8(output, 1);
            put(output, value)
        }
        None => {
            put_u8(output, 0);
            Ok(())
        }
    }
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

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<(), ObjectMutationCodecError> {
    let length = u32::try_from(value.len())
        .map_err(|_| malformed("string length does not fit the format"))?;
    put_u32(output, length);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_bool(output: &mut Vec<u8>, value: bool) {
    put_u8(output, u8::from(value));
}

fn put_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_i64(output: &mut Vec<u8>, value: i64) {
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

    fn finish(&self) -> Result<(), ObjectMutationCodecError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(malformed("trailing bytes are present"))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ObjectMutationCodecError> {
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

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ObjectMutationCodecError> {
        Ok(self.take(N)?.try_into().expect("exact fixed-width slice"))
    }

    fn u8(&mut self) -> Result<u8, ObjectMutationCodecError> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Result<u16, ObjectMutationCodecError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, ObjectMutationCodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, ObjectMutationCodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, ObjectMutationCodecError> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn u32_length(&mut self, field: &str) -> Result<usize, ObjectMutationCodecError> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| malformed(format!("{field} length does not fit this platform")))?;
        if length > self.remaining() {
            return Err(malformed(format!(
                "{field} length exceeds the remaining record"
            )));
        }
        Ok(length)
    }

    fn string(&mut self, field: &str, maximum: usize) -> Result<String, ObjectMutationCodecError> {
        let length = self.u32_length(field)?;
        if length > maximum {
            return Err(malformed(format!("{field} exceeds its format bound")));
        }
        let bytes = self.take(length)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| malformed(format!("{field} is not valid UTF-8")))?;
        Ok(value.to_owned())
    }

    fn bool(&mut self, field: &str) -> Result<bool, ObjectMutationCodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(malformed(format!("{field} boolean is invalid"))),
        }
    }

    fn optional_u64(&mut self, field: &str) -> Result<Option<u64>, ObjectMutationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(malformed(format!("{field} option tag is invalid"))),
        }
    }

    fn blob(&mut self) -> Result<BlobRef, ObjectMutationCodecError> {
        Ok(BlobRef {
            hash: self.array()?,
            length: self.u64()?,
        })
    }

    fn version(&mut self) -> Result<Version, ObjectMutationCodecError> {
        Ok(Version {
            id: VersionId(self.u64()?),
            blob: match self.u8()? {
                0 => None,
                1 => Some(self.blob()?),
                _ => return Err(malformed("blob option tag is invalid")),
            },
            content_type: match self.u8()? {
                0 => None,
                1 => Some(self.string("content type", MAX_CONTENT_TYPE_BYTES)?),
                _ => return Err(malformed("content-type option tag is invalid")),
            },
            deleted: self.bool("version deleted")?,
            committed_at_unix_millis: self.u64()?,
            protected_link_descriptor: self.bool("protected link descriptor")?,
        })
    }

    fn stamp(&mut self) -> Result<MutationStamp, ObjectMutationCodecError> {
        Ok(MutationStamp {
            format: self.u16()?,
            predecessor_version: self.optional_u64("predecessor")?.map(VersionId),
            program_commit_cursor: self.optional_u64("program cursor")?,
            mutation_fingerprint: self.array()?,
            active_placement_log_id: PlacementLogId {
                term: self.u64()?,
                index: self.u64()?,
            },
            serving_fence_term: self.u64()?,
            source_id: SourceId {
                node_id: self.u16()?,
                source_epoch: self.array()?,
            },
            source_journal_position: self.u64()?,
        })
    }

    fn reference_deltas(&mut self) -> Result<Vec<ReferenceDelta>, ObjectMutationCodecError> {
        const DELTA_BYTES: usize = 32 + 8 + 8;
        let count = self.bounded_count(
            "reference-delta",
            MAX_OBJECT_MUTATION_REFERENCE_DELTAS,
            DELTA_BYTES,
        )?;
        let mut deltas = Vec::new();
        deltas
            .try_reserve_exact(count)
            .map_err(|_| malformed("reference-delta allocation failed"))?;
        for _ in 0..count {
            deltas.push(ReferenceDelta {
                blob: self.blob()?,
                change: self.i64()?,
            });
        }
        Ok(deltas)
    }

    fn accounting_transition(
        &mut self,
    ) -> Result<Option<AccountingHeadTransition>, ObjectMutationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let format = self.u8()?;
                if format != AccountingHeadTransition::FORMAT {
                    return Err(malformed("accounting transition format is unsupported"));
                }
                Ok(Some(AccountingHeadTransition::new(
                    self.optional_u64("previous live length")?,
                    self.optional_u64("current live length")?,
                )))
            }
            _ => Err(malformed("accounting transition option tag is invalid")),
        }
    }

    fn definition_transition(
        &mut self,
    ) -> Result<Option<DefinitionTransition>, ObjectMutationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(DefinitionTransition {
                kind: match self.u8()? {
                    DEFINITION_INDEX => DefinitionKind::Index,
                    DEFINITION_ACCOUNTING => DefinitionKind::Accounting,
                    _ => return Err(malformed("definition kind is unknown")),
                },
                tenant_id: self.u64()?,
                bucket_id: self.u64()?,
                definition_id: self.u64()?,
                path: self.string(
                    "definition path",
                    keldra_atomic_program::MAX_OBJECT_PATH_BYTES,
                )?,
                object_version: VersionId(self.u64()?),
                operation: match self.u8()? {
                    DEFINITION_UPSERT => DefinitionOperation::Upsert,
                    DEFINITION_DELETE => DefinitionOperation::Delete,
                    _ => return Err(malformed("definition operation is unknown")),
                },
            })),
            _ => Err(malformed("definition transition option tag is invalid")),
        }
    }

    fn alias_snapshot(&mut self) -> Result<Option<ObjectAliasSnapshot>, ObjectMutationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => {
                let format = self.u16()?;
                let revision = self.u64()?;
                let count = self.bounded_count("alias", MAX_INBOUND_OBJECT_LINKS, 4)?;
                let mut aliases = Vec::new();
                aliases
                    .try_reserve_exact(count)
                    .map_err(|_| malformed("alias allocation failed"))?;
                for _ in 0..count {
                    aliases.push(
                        self.string("alias path", keldra_atomic_program::MAX_OBJECT_PATH_BYTES)?,
                    );
                }
                let program_commit_cursor = self.optional_u64("alias program cursor")?;
                let canonical_version = self.version()?;
                Ok(Some(ObjectAliasSnapshot {
                    registry: ObjectAliasRegistry {
                        format,
                        revision,
                        aliases,
                        program_commit_cursor,
                    },
                    canonical_version,
                }))
            }
            _ => Err(malformed("alias snapshot option tag is invalid")),
        }
    }

    fn bounded_count(
        &mut self,
        field: &str,
        maximum: usize,
        minimum_item_bytes: usize,
    ) -> Result<usize, ObjectMutationCodecError> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| malformed(format!("{field} count does not fit this platform")))?;
        if count > maximum {
            return Err(malformed(format!("{field} count exceeds its format bound")));
        }
        if count > self.remaining() / minimum_item_bytes {
            return Err(malformed(format!(
                "{field} count exceeds the remaining record"
            )));
        }
        Ok(count)
    }
}

fn malformed(message: impl Into<String>) -> ObjectMutationCodecError {
    ObjectMutationCodecError::Malformed(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LEGACY_OBJECT_MUTATION_FORMAT, MUTATION_STAMP_FORMAT, OBJECT_ALIAS_REGISTRY_FORMAT,
        OBJECT_MUTATION_FORMAT,
    };

    fn base_mutation(format: u16) -> ObjectMutation {
        let predecessor = Version {
            id: VersionId(40),
            blob: Some(BlobRef {
                hash: [4; 32],
                length: 90,
            }),
            content_type: Some("text/plain".into()),
            deleted: false,
            committed_at_unix_millis: 900,
            protected_link_descriptor: false,
        };
        let mut mutation = ObjectMutation {
            format,
            tenant_id: 11,
            bucket_id: 12,
            exact_path: "documents/one".into(),
            command_id: "command-1".into(),
            input_fingerprint: [1; 32],
            version: Version {
                id: VersionId(41),
                blob: Some(BlobRef {
                    hash: [2; 32],
                    length: 1_024,
                }),
                content_type: Some("application/json".into()),
                deleted: false,
                committed_at_unix_millis: 1_000,
                protected_link_descriptor: false,
            },
            receipt_expires_at_unix_millis: 2_000,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(VersionId(40)),
                program_commit_cursor: None,
                mutation_fingerprint: [0; 32],
                active_placement_log_id: PlacementLogId { term: 3, index: 4 },
                serving_fence_term: 5,
                source_id: SourceId {
                    node_id: 6,
                    source_epoch: [7; 32],
                },
                source_journal_position: 8,
            },
            reference_deltas: vec![
                ReferenceDelta {
                    blob: BlobRef {
                        hash: [4; 32],
                        length: 90,
                    },
                    change: -1,
                },
                ReferenceDelta {
                    blob: BlobRef {
                        hash: [2; 32],
                        length: 1_024,
                    },
                    change: 1,
                },
            ],
            accounting_transition: Some(AccountingHeadTransition::new(Some(90), Some(1_024))),
            definition_transition: None,
            alias_snapshot: (format == OBJECT_MUTATION_FORMAT).then_some(ObjectAliasSnapshot {
                registry: ObjectAliasRegistry {
                    format: OBJECT_ALIAS_REGISTRY_FORMAT,
                    revision: 9,
                    aliases: vec!["aliases/one".into(), "aliases/two".into()],
                    program_commit_cursor: Some(10),
                },
                canonical_version: predecessor,
            }),
        };
        mutation.set_computed_fingerprint();
        mutation.validate().unwrap();
        mutation
    }

    fn golden_mutation() -> ObjectMutation {
        let mut mutation = ObjectMutation {
            format: LEGACY_OBJECT_MUTATION_FORMAT,
            tenant_id: 1,
            bucket_id: 2,
            exact_path: "a".into(),
            command_id: "b".into(),
            input_fingerprint: [0; 32],
            version: Version {
                id: VersionId(2),
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: 1,
                protected_link_descriptor: false,
            },
            receipt_expires_at_unix_millis: 2,
            stamp: MutationStamp {
                format: MUTATION_STAMP_FORMAT,
                predecessor_version: Some(VersionId(1)),
                program_commit_cursor: None,
                mutation_fingerprint: [0; 32],
                active_placement_log_id: PlacementLogId { term: 1, index: 1 },
                serving_fence_term: 1,
                source_id: SourceId {
                    node_id: 1,
                    source_epoch: [1; 32],
                },
                source_journal_position: 1,
            },
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
            alias_snapshot: None,
        };
        mutation.set_computed_fingerprint();
        mutation.validate().unwrap();
        mutation
    }

    #[test]
    fn minimal_mutation_has_an_architecture_independent_golden_encoding() {
        let mutation = golden_mutation();
        let mut expected = vec![
            b'K', b'O', b'M', b'U', // magic
            0, 1, // codec format
            0, 0, // flags
            0, 0, 0, 205, // body bytes
            0, 2, // semantic mutation format
            0, 0, 0, 0, 0, 0, 0, 1, // tenant
            0, 0, 0, 0, 0, 0, 0, 2, // bucket
            0, 0, 0, 1, b'a', // exact path
            0, 0, 0, 1, b'b', // command ID
        ];
        expected.extend_from_slice(&[0; 32]); // input fingerprint
        expected.extend_from_slice(&[
            0, 0, 0, 0, 0, 0, 0, 2, // version ID
            0, // blob absent
            0, // content type absent
            1, // deleted
            0, 0, 0, 0, 0, 0, 0, 1, // committed at
            0, // not a protected link descriptor
            0, 0, 0, 0, 0, 0, 0, 2, // receipt expiry
            0, 1, // mutation stamp format
            1, 0, 0, 0, 0, 0, 0, 0, 1, // predecessor version
            0, // program cursor absent
        ]);
        // The canonical fingerprint is independently specified by ObjectMutation;
        // this golden vector freezes its position and exact 32-byte width.
        expected.extend_from_slice(&mutation.stamp.mutation_fingerprint);
        expected.extend_from_slice(&[
            0, 0, 0, 0, 0, 0, 0, 1, // placement term
            0, 0, 0, 0, 0, 0, 0, 1, // placement index
            0, 0, 0, 0, 0, 0, 0, 1, // serving fence term
            0, 1, // source node
        ]);
        expected.extend_from_slice(&[1; 32]); // source epoch
        expected.extend_from_slice(&[
            0, 0, 0, 0, 0, 0, 0, 1, // source journal position
            0, 0, 0, 0, // no reference deltas
            0, // accounting transition absent
            0, // definition transition absent
            0, // alias snapshot absent
        ]);

        assert_eq!(encode_object_mutation(&mutation).unwrap(), expected);
        assert_eq!(decode_object_mutation(&expected).unwrap(), mutation);
    }

    #[test]
    fn semantic_formats_two_and_three_round_trip() {
        for format in [LEGACY_OBJECT_MUTATION_FORMAT, OBJECT_MUTATION_FORMAT] {
            let mutation = base_mutation(format);
            let encoded = encode_object_mutation(&mutation).unwrap();
            assert_eq!(decode_object_mutation(&encoded).unwrap(), mutation);
        }
    }

    #[test]
    fn optional_and_enum_shapes_round_trip() {
        let mut mutation = base_mutation(LEGACY_OBJECT_MUTATION_FORMAT);
        mutation.exact_path = "_keldra/indexes/17".into();
        mutation.version = Version {
            id: VersionId(41),
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 1_000,
            protected_link_descriptor: false,
        };
        mutation.reference_deltas.truncate(1);
        mutation.accounting_transition = Some(AccountingHeadTransition::new(Some(90), None));
        mutation.definition_transition = Some(DefinitionTransition {
            kind: DefinitionKind::Index,
            tenant_id: 11,
            bucket_id: 12,
            definition_id: 17,
            path: mutation.exact_path.clone(),
            object_version: mutation.version.id,
            operation: DefinitionOperation::Delete,
        });
        mutation.set_computed_fingerprint();
        mutation.validate().unwrap();

        let encoded = encode_object_mutation(&mutation).unwrap();
        assert_eq!(decode_object_mutation(&encoded).unwrap(), mutation);
    }

    #[test]
    fn every_definition_enum_tag_round_trips() {
        let mut mutation = base_mutation(LEGACY_OBJECT_MUTATION_FORMAT);
        mutation.exact_path = "_keldra/accounting/18".into();
        mutation.definition_transition = Some(DefinitionTransition {
            kind: DefinitionKind::Accounting,
            tenant_id: mutation.tenant_id,
            bucket_id: mutation.bucket_id,
            definition_id: 18,
            path: mutation.exact_path.clone(),
            object_version: mutation.version.id,
            operation: DefinitionOperation::Upsert,
        });
        mutation.set_computed_fingerprint();
        mutation.validate().unwrap();

        let encoded = encode_object_mutation(&mutation).unwrap();
        assert_eq!(decode_object_mutation(&encoded).unwrap(), mutation);
    }

    #[test]
    fn malformed_headers_lengths_tags_and_trailing_bytes_are_rejected() {
        let mutation = base_mutation(LEGACY_OBJECT_MUTATION_FORMAT);
        let encoded = encode_object_mutation(&mutation).unwrap();

        for length in 0..encoded.len() {
            assert!(
                decode_object_mutation(&encoded[..length]).is_err(),
                "length {length}"
            );
        }

        let mut bad_magic = encoded.clone();
        bad_magic[0] = b'X';
        assert!(decode_object_mutation(&bad_magic).is_err());

        let mut bad_format = encoded.clone();
        bad_format[5] = 2;
        assert_eq!(
            decode_object_mutation(&bad_format),
            Err(ObjectMutationCodecError::UnsupportedFormat(2))
        );

        let mut bad_flags = encoded.clone();
        bad_flags[7] = 1;
        assert!(decode_object_mutation(&bad_flags).is_err());

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_object_mutation(&trailing).is_err());

        let mut bad_path_length = encoded.clone();
        // Header + format + tenant + bucket is the exact-path length field.
        bad_path_length[30..34].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_object_mutation(&bad_path_length).is_err());
    }

    #[test]
    fn count_bounds_are_checked_before_allocation() {
        let mutation = base_mutation(LEGACY_OBJECT_MUTATION_FORMAT);
        let mut encoded = encode_object_mutation(&mutation).unwrap();
        let first_delta_hash = encoded
            .windows(32)
            .rposition(|window| window == [4; 32])
            .expect("old blob hash is present");
        let count_offset = first_delta_hash - 4;
        encoded[count_offset..first_delta_hash].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_object_mutation(&encoded).is_err());
    }

    #[test]
    fn primitive_decoders_reject_every_unknown_tag_before_constructing_values() {
        assert!(Input::new(&[2]).bool("test").is_err());
        assert!(Input::new(&[2]).optional_u64("test").is_err());

        let mut invalid_blob = vec![0; 8];
        invalid_blob.push(2);
        assert!(Input::new(&invalid_blob).version().is_err());

        let mut invalid_content_type = vec![0; 8];
        invalid_content_type.extend_from_slice(&[0, 2]);
        assert!(Input::new(&invalid_content_type).version().is_err());

        assert!(Input::new(&[2]).accounting_transition().is_err());
        assert!(Input::new(&[1, 2]).accounting_transition().is_err());
        assert!(Input::new(&[2]).definition_transition().is_err());
        assert!(Input::new(&[1, 99]).definition_transition().is_err());
        assert!(Input::new(&[2]).alias_snapshot().is_err());
    }

    #[test]
    fn total_record_bound_is_enforced_before_body_decoding() {
        let oversized = vec![0; MAX_ENCODED_BYTES + 1];
        assert!(decode_object_mutation(&oversized).is_err());
    }

    #[test]
    fn semantic_validation_runs_after_decode() {
        let mutation = base_mutation(LEGACY_OBJECT_MUTATION_FORMAT);
        let mut encoded = encode_object_mutation(&mutation).unwrap();
        // The semantic mutation format is the first body field.
        encoded[HEADER_BYTES..HEADER_BYTES + 2].copy_from_slice(&99_u16.to_be_bytes());
        assert!(decode_object_mutation(&encoded).is_err());
    }
}
