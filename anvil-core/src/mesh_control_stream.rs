use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreMutationRootPublication, CoreStore, TABLE_MESH_PARTITION_ROW,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
    core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
};
use crate::formats::writer::WriterFamily;
use crate::storage::Storage;
use anyhow::{Context, Result as AnyhowResult, anyhow};
use prost::Message;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;

pub const CONTROL_STREAM_MAGIC: &[u8; 8] = b"ANVCTL1\0";
pub const CONTROL_STREAM_VERSION: u16 = 1;
pub const CONTROL_STREAM_FIXED_HEADER_LEN: usize = 8 + 2 + 4 + 8 + 4 + 4;
pub const MAX_CONTROL_PROTO_PAYLOAD_LEN: usize = 64 * 1024;
pub const CONTROL_CHECKPOINT_SCHEMA: &str = "anvil.mesh.control_checkpoint.v1";
const CONTROL_STREAM_ID_PREFIX: &str = "mesh_control_stream:";
const CONTROL_CHECKPOINT_ROW_PREFIX: &str = "mesh-control-checkpoint";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ControlStreamFrameError {
    #[error("buffer is too short for {context}: need at least {needed} bytes, got {actual}")]
    TooShort {
        context: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("invalid control stream frame magic")]
    InvalidMagic,
    #[error("unsupported control stream frame version {0}")]
    UnsupportedVersion(u16),
    #[error("declared length is invalid for {context}")]
    InvalidDeclaredLength { context: &'static str },
    #[error("header CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    HeaderCrc32Mismatch { expected: u32, actual: u32 },
    #[error("payload CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    PayloadCrc32Mismatch { expected: u32, actual: u32 },
    #[error("control stream sequence must be greater than zero")]
    InvalidSequence,
    #[error("control stream digest is invalid")]
    InvalidDigest,
    #[error("control stream frame header metadata is missing field {field}")]
    MissingHeaderField { field: &'static str },
    #[error("control stream frame header protobuf is invalid")]
    InvalidHeaderProtobuf,
    #[error("control stream frame header protobuf is not deterministic")]
    NonDeterministicHeaderProtobuf,
    #[error("control stream protobuf payload is too large: {actual} bytes exceeds {max} bytes")]
    PayloadTooLarge { actual: usize, max: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlStreamSequence(u64);

impl ControlStreamSequence {
    pub fn new(value: u64) -> std::result::Result<Self, ControlStreamFrameError> {
        if value == 0 {
            return Err(ControlStreamFrameError::InvalidSequence);
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for ControlStreamSequence {
    type Error = ControlStreamFrameError;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ControlStreamSequence> for u64 {
    fn from(sequence: ControlStreamSequence) -> Self {
        sequence.0
    }
}

impl Serialize for ControlStreamSequence {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ControlStreamSequence {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlRecordDigest(String);

impl ControlRecordDigest {
    pub fn new(value: impl Into<String>) -> std::result::Result<Self, ControlStreamFrameError> {
        let value = value.into();
        if !is_valid_digest(&value) {
            return Err(ControlStreamFrameError::InvalidDigest);
        }
        Ok(Self(value))
    }

    pub fn blake3(bytes: &[u8]) -> Self {
        Self(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ControlRecordDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ControlRecordDigest {
    type Error = ControlStreamFrameError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ControlRecordDigest {
    type Error = ControlStreamFrameError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for ControlRecordDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ControlRecordDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlStreamPosition {
    pub sequence: ControlStreamSequence,
    pub digest: ControlRecordDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFrameMetadata {
    pub sequence: ControlStreamSequence,
    pub record_digest: ControlRecordDigest,
}

impl From<ControlFrameMetadata> for ControlStreamPosition {
    fn from(metadata: ControlFrameMetadata) -> Self {
        Self {
            sequence: metadata.sequence,
            digest: metadata.record_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamFrame {
    pub header_proto: Vec<u8>,
    pub payload_proto: Vec<u8>,
}

impl ControlStreamFrame {
    pub fn new(header_proto: Vec<u8>, payload_proto: Vec<u8>) -> Self {
        Self {
            header_proto,
            payload_proto,
        }
    }

    pub fn metadata(&self) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
        metadata_from_header_proto(&self.header_proto)
    }

    pub fn encoded_len(&self) -> std::result::Result<usize, ControlStreamFrameError> {
        encoded_frame_len(self.header_proto.len(), self.payload_proto.len())
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
        encode_control_stream_frame(&self.header_proto, &self.payload_proto)
    }

    pub fn decode(input: &[u8]) -> std::result::Result<(Self, usize), ControlStreamFrameError> {
        decode_control_stream_frame(input)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ControlFrameHeaderProto {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(string, tag = "2")]
    pub mesh_id: String,
    #[prost(string, tag = "3")]
    pub stream_family: String,
    #[prost(string, tag = "4")]
    pub partition: String,
    #[prost(uint64, tag = "5")]
    pub sequence: u64,
    #[prost(string, tag = "6")]
    pub record_key: String,
    #[prost(string, tag = "7")]
    pub operation: String,
    #[prost(uint64, optional, tag = "8")]
    pub expected_generation: Option<u64>,
    #[prost(uint64, tag = "9")]
    pub new_generation: u64,
    #[prost(string, tag = "10")]
    pub writer_node_id: String,
    #[prost(uint64, tag = "11")]
    pub writer_fence: u64,
    #[prost(string, optional, tag = "12")]
    pub idempotency_key: Option<String>,
    #[prost(string, tag = "13")]
    pub record_digest: String,
    #[prost(string, tag = "14")]
    pub created_at: String,
    #[prost(uint64, tag = "15")]
    pub byte_offset: u64,
}

pub struct ControlMutationHeaderInput<'a> {
    pub schema: &'a str,
    pub mesh_id: &'a str,
    pub stream_family: &'a str,
    pub partition: &'a str,
    pub sequence: ControlStreamSequence,
    pub record_key: &'a str,
    pub operation: &'a str,
    pub expected_generation: Option<u64>,
    pub new_generation: u64,
    pub writer_node_id: &'a str,
    pub writer_fence: u64,
    pub idempotency_key: Option<&'a str>,
    pub record_digest: &'a ControlRecordDigest,
    pub created_at: &'a str,
    pub byte_offset: u64,
}

pub fn encode_control_mutation_header(input: ControlMutationHeaderInput<'_>) -> Vec<u8> {
    ControlFrameHeaderProto {
        schema: input.schema.to_string(),
        mesh_id: input.mesh_id.to_string(),
        stream_family: input.stream_family.to_string(),
        partition: input.partition.to_string(),
        sequence: input.sequence.get(),
        record_key: input.record_key.to_string(),
        operation: input.operation.to_string(),
        expected_generation: input.expected_generation,
        new_generation: input.new_generation,
        writer_node_id: input.writer_node_id.to_string(),
        writer_fence: input.writer_fence,
        idempotency_key: input.idempotency_key.map(str::to_string),
        record_digest: input.record_digest.as_str().to_string(),
        created_at: input.created_at.to_string(),
        byte_offset: input.byte_offset,
    }
    .encode_to_vec()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamLogRecord {
    pub offset: u64,
    pub encoded_len: usize,
    pub metadata: ControlFrameMetadata,
    pub frame: ControlStreamFrame,
}

impl ControlStreamLogRecord {
    pub fn position(&self) -> ControlStreamPosition {
        self.metadata.clone().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialFinalFrame {
    pub offset: u64,
    pub expected_len: usize,
    pub actual_len: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlStreamLog {
    pub records: Vec<ControlStreamLogRecord>,
    pub complete_len: u64,
    pub partial_final_frame: Option<PartialFinalFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamAppend {
    pub offset: u64,
    pub encoded_len: usize,
    pub position: ControlStreamPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamAppendCursor {
    pub sequence: ControlStreamSequence,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamLogPage {
    pub records: Vec<ControlStreamLogRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamPartitionPage {
    pub partitions: Vec<String>,
    pub next_stream_id: Option<String>,
}

mod store;

pub use store::{
    ControlStreamCurrentPage, ControlStreamCurrentRecord, control_stream_append_cursor,
    latest_projected_record_from_control_stream, list_control_stream_partitions_page,
    list_current_control_stream_records_page, read_control_stream_page,
};
pub(crate) use store::{
    PreparedControlStreamAppend, append_control_stream_frame,
    control_stream_append_cursor_visible_to_transaction, finish_control_stream_append,
    prepare_control_stream_append,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlCheckpointRecord {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub stream_family: String,
    pub partition: String,
    pub last_sequence: ControlStreamSequence,
    pub last_digest: ControlRecordDigest,
    pub updated_at: String,
}

impl ControlCheckpointRecord {
    pub fn new(
        mesh_id: impl Into<String>,
        region: impl Into<String>,
        stream_family: impl Into<String>,
        partition: impl Into<String>,
        last_sequence: ControlStreamSequence,
        last_digest: ControlRecordDigest,
        updated_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: CONTROL_CHECKPOINT_SCHEMA.to_string(),
            mesh_id: mesh_id.into(),
            region: region.into(),
            stream_family: stream_family.into(),
            partition: partition.into(),
            last_sequence,
            last_digest,
            updated_at: updated_at.into(),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct ControlCheckpointProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    mesh_id: String,
    #[prost(string, tag = "4")]
    region: String,
    #[prost(string, tag = "5")]
    stream_family: String,
    #[prost(string, tag = "6")]
    partition: String,
    #[prost(uint64, tag = "7")]
    last_sequence: u64,
    #[prost(string, tag = "8")]
    last_digest: String,
    #[prost(string, tag = "9")]
    updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProjectionRecord {
    pub record_key: String,
    pub generation: u64,
    pub payload_json: Vec<u8>,
    pub deleted: bool,
}

impl ControlProjectionRecord {
    pub fn new(
        record_key: impl Into<String>,
        generation: u64,
        payload_json: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            record_key: record_key.into(),
            generation,
            payload_json: payload_json.into(),
            deleted: false,
        }
    }

    pub fn tombstone(record_key: impl Into<String>, generation: u64) -> Self {
        Self {
            record_key: record_key.into(),
            generation,
            payload_json: Vec::new(),
            deleted: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProjectionDiagnostic {
    pub stream_family: String,
    pub partition: String,
    pub record_key: String,
    pub severity: &'static str,
    pub code: &'static str,
    pub message: String,
    pub stream_sequence: Option<u64>,
    pub stream_generation: Option<u64>,
    pub stream_digest: Option<String>,
    pub projection_generation: Option<u64>,
    pub projection_digest: Option<String>,
    pub repair_safe: bool,
    pub proposed_action: &'static str,
}

pub async fn diagnose_control_stream_projection(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    projected_records: &[ControlProjectionRecord],
) -> AnyhowResult<Vec<ControlProjectionDiagnostic>> {
    let mut diagnostics = Vec::new();
    let mut latest_by_key = BTreeMap::new();
    let mut expected_sequence = 1_u64;
    let mut after_sequence = 0;
    loop {
        let page = read_control_stream_page(storage, stream_family, partition, after_sequence, 512)
            .await?;
        for record in &page.records {
            let sequence = record.metadata.sequence.get();
            if sequence != expected_sequence {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: String::new(),
                    severity: "error",
                    code: "mesh_control_stream_sequence_gap",
                    message: format!(
                        "control stream {stream_family}/{partition} expected sequence {expected_sequence} but found {sequence}"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: None,
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }
            expected_sequence = sequence.saturating_add(1);

            let header = match decode_control_mutation_header(&record.frame.header_proto) {
                Ok(header) => header,
                Err(err) => {
                    diagnostics.push(ControlProjectionDiagnostic {
                        stream_family: stream_family.to_string(),
                        partition: partition.to_string(),
                        record_key: String::new(),
                        severity: "error",
                        code: "mesh_control_stream_header_invalid",
                        message: format!(
                            "control stream {stream_family}/{partition} sequence {sequence} has invalid mutation header: {err}"
                        ),
                        stream_sequence: Some(sequence),
                        stream_generation: None,
                        stream_digest: Some(record.metadata.record_digest.to_string()),
                        projection_generation: None,
                        projection_digest: None,
                        repair_safe: false,
                        proposed_action: "manual_review_rebuild_not_implemented",
                    });
                    continue;
                }
            };

            if header.stream_family != stream_family || header.partition != partition {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: header.record_key.clone(),
                    severity: "error",
                    code: "mesh_control_stream_header_scope_mismatch",
                    message: format!(
                        "control stream header scope {}/{} does not match path {stream_family}/{partition}",
                        header.stream_family, header.partition
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }

            let record_key = header.record_key.clone();
            if record_key.trim().is_empty() {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: String::new(),
                    severity: "error",
                    code: "mesh_control_stream_record_key_missing",
                    message: format!(
                        "control stream {stream_family}/{partition} sequence {sequence} is missing record_key"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
                continue;
            }

            let payload_digest = ControlRecordDigest::blake3(&record.frame.payload_proto);
            if payload_digest.as_str() != record.metadata.record_digest.as_str() {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: record_key.clone(),
                    severity: "error",
                    code: "mesh_control_stream_digest_mismatch",
                    message: format!(
                        "control stream {stream_family}/{partition} sequence {sequence} payload digest does not match header digest"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: Some(payload_digest.to_string()),
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }

            let payload_json = match control_payload_operator_json(
                stream_family,
                &record_key,
                &record.frame.payload_proto,
            ) {
                Ok(payload_json) => payload_json,
                Err(err) => {
                    diagnostics.push(ControlProjectionDiagnostic {
                        stream_family: stream_family.to_string(),
                        partition: partition.to_string(),
                        record_key: record_key.clone(),
                        severity: "error",
                        code: "mesh_control_stream_payload_invalid",
                        message: format!(
                            "control stream {stream_family}/{partition} sequence {sequence} has invalid protobuf payload: {err}"
                        ),
                        stream_sequence: Some(sequence),
                        stream_generation: Some(header.new_generation),
                        stream_digest: Some(record.metadata.record_digest.to_string()),
                        projection_generation: None,
                        projection_digest: None,
                        repair_safe: false,
                        proposed_action: "manual_review_rebuild_not_implemented",
                    });
                    continue;
                }
            };

            latest_by_key.insert(
                record_key,
                StreamProjectionEntry {
                    sequence,
                    generation: Some(header.new_generation),
                    digest: record.metadata.record_digest.to_string(),
                    operation: header.operation,
                    payload_json,
                },
            );
        }
        if !page.has_more {
            break;
        }
        after_sequence = page.next_sequence;
    }

    let mut projection_by_key = BTreeMap::new();
    for projection in projected_records {
        if projection_by_key
            .insert(projection.record_key.clone(), projection)
            .is_some()
        {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_duplicate_record",
                message: format!(
                    "projection for {stream_family}/{partition} contains duplicate record {}",
                    projection.record_key
                ),
                stream_sequence: None,
                stream_generation: None,
                stream_digest: None,
                projection_generation: Some(projection.generation),
                projection_digest: projection_digest(&projection.payload_json).ok(),
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
        }
    }

    for projection in projected_records {
        let projection_digest = projection_digest(&projection.payload_json)?;
        let Some(stream_entry) = latest_by_key.get(&projection.record_key) else {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_missing_stream_record",
                message: format!(
                    "projection record {} has no matching control stream mutation in {stream_family}/{partition}",
                    projection.record_key
                ),
                stream_sequence: None,
                stream_generation: None,
                stream_digest: None,
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest),
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
            continue;
        };

        if stream_entry.generation != Some(projection.generation) {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_generation_mismatch",
                message: format!(
                    "projection record {} generation {} does not match latest stream generation {:?}",
                    projection.record_key, projection.generation, stream_entry.generation
                ),
                stream_sequence: Some(stream_entry.sequence),
                stream_generation: stream_entry.generation,
                stream_digest: Some(stream_entry.digest.clone()),
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest.clone()),
                repair_safe: true,
                proposed_action: "repair_routing_record_from_control_stream",
            });
        }

        if semantic_json(&stream_entry.payload_json)? != semantic_json(&projection.payload_json)? {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_payload_mismatch",
                message: format!(
                    "projection record {} payload does not match latest control stream payload",
                    projection.record_key
                ),
                stream_sequence: Some(stream_entry.sequence),
                stream_generation: stream_entry.generation,
                stream_digest: Some(stream_entry.digest.clone()),
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest),
                repair_safe: true,
                proposed_action: "repair_routing_record_from_control_stream",
            });
        }
    }

    for (record_key, stream_entry) in latest_by_key {
        if projection_by_key.contains_key(&record_key)
            || is_delete_operation(&stream_entry.operation)
        {
            continue;
        }
        diagnostics.push(ControlProjectionDiagnostic {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            record_key: record_key.clone(),
            severity: "error",
            code: "mesh_control_projection_missing_record",
            message: format!(
                "control stream latest mutation for {record_key} has no projected descriptor"
            ),
            stream_sequence: Some(stream_entry.sequence),
            stream_generation: stream_entry.generation,
            stream_digest: Some(stream_entry.digest),
            projection_generation: None,
            projection_digest: None,
            repair_safe: true,
            proposed_action: "repair_routing_record_from_control_stream",
        });
    }

    Ok(diagnostics)
}

pub async fn write_control_checkpoint(
    storage: &Storage,
    checkpoint: &ControlCheckpointRecord,
) -> AnyhowResult<()> {
    validate_control_checkpoint(checkpoint)?;
    let row_key = control_checkpoint_row_key(
        &checkpoint.region,
        &checkpoint.stream_family,
        &checkpoint.partition,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    let current_payload = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?;
    if let Some(existing) = current_payload
        .as_deref()
        .map(decode_control_checkpoint_proto)
        .transpose()?
    {
        ensure_control_checkpoint_scope(
            &existing,
            &checkpoint.region,
            &checkpoint.stream_family,
            &checkpoint.partition,
        )?;
        if checkpoint.last_sequence < existing.last_sequence {
            return Err(anyhow!(
                "control checkpoint cannot move backwards for {}/{}/{}: existing sequence {}, new sequence {}",
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition,
                existing.last_sequence.get(),
                checkpoint.last_sequence.get()
            ));
        }
        if checkpoint.last_sequence == existing.last_sequence {
            if checkpoint.last_digest.as_str() != existing.last_digest.as_str() {
                return Err(anyhow!(
                    "ControlStreamDivergence: control checkpoint {}/{}/{} sequence {} has digest {}, existing digest {}",
                    checkpoint.region,
                    checkpoint.stream_family,
                    checkpoint.partition,
                    checkpoint.last_sequence.get(),
                    checkpoint.last_digest,
                    existing.last_digest
                ));
            }
            return Ok(());
        }
    }
    let payload = encode_control_checkpoint_proto(checkpoint)?;
    let partition_id = control_checkpoint_partition_id(
        &checkpoint.region,
        &checkpoint.stream_family,
        &checkpoint.partition,
    )?;
    let root_publications = control_checkpoint_root_publications(
        partition_id.clone(),
        control_checkpoint_root_anchor_key(
            &checkpoint.region,
            &checkpoint.stream_family,
            &checkpoint.partition,
        ),
    );
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "mesh-control-checkpoint:{}:{}:{}:{}:{}",
                checkpoint.mesh_id,
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition,
                checkpoint.last_sequence.get()
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: "mesh-control-checkpoint".to_string(),
            root_publications,
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: current_payload
                    .as_ref()
                    .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
                require_absent: current_payload.is_none(),
                require_present: current_payload.is_some(),
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id,
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key,
                payload,
            }],
        })
        .await?;
    Ok(())
}

pub async fn read_control_checkpoint(
    storage: &Storage,
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<Option<ControlCheckpointRecord>> {
    let row_key = control_checkpoint_row_key(region, stream_family, partition)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?
    else {
        return Ok(None);
    };
    let checkpoint = decode_control_checkpoint_proto(&payload)?;
    ensure_control_checkpoint_scope(&checkpoint, region, stream_family, partition)?;
    Ok(Some(checkpoint))
}

fn ensure_control_checkpoint_scope(
    checkpoint: &ControlCheckpointRecord,
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<()> {
    if checkpoint.region != region
        || checkpoint.stream_family != stream_family
        || checkpoint.partition != partition
    {
        return Err(anyhow!(
            "control checkpoint path does not match checkpoint body: expected {region}/{stream_family}/{partition}, got {}/{}/{}",
            checkpoint.region,
            checkpoint.stream_family,
            checkpoint.partition
        ));
    }
    Ok(())
}

fn validate_control_checkpoint(checkpoint: &ControlCheckpointRecord) -> AnyhowResult<()> {
    if checkpoint.schema != CONTROL_CHECKPOINT_SCHEMA {
        return Err(anyhow!(
            "control checkpoint schema must be {CONTROL_CHECKPOINT_SCHEMA}"
        ));
    }
    if checkpoint.mesh_id.trim().is_empty() {
        return Err(anyhow!("control checkpoint mesh_id must not be empty"));
    }
    if checkpoint.region.trim().is_empty() {
        return Err(anyhow!("control checkpoint region must not be empty"));
    }
    if checkpoint.stream_family.trim().is_empty() {
        return Err(anyhow!(
            "control checkpoint stream_family must not be empty"
        ));
    }
    if checkpoint.partition.trim().is_empty() {
        return Err(anyhow!("control checkpoint partition must not be empty"));
    }
    if checkpoint.updated_at.trim().is_empty() {
        return Err(anyhow!("control checkpoint updated_at must not be empty"));
    }
    Ok(())
}

fn encode_control_checkpoint_proto(checkpoint: &ControlCheckpointRecord) -> AnyhowResult<Vec<u8>> {
    validate_control_checkpoint(checkpoint)?;
    Ok(encode_deterministic_proto(&ControlCheckpointProto {
        common: Some(core_meta_committed_row_common(
            "mesh",
            core_meta_root_key_hash(&control_checkpoint_root_anchor_key(
                &checkpoint.region,
                &checkpoint.stream_family,
                &checkpoint.partition,
            )),
            checkpoint.last_sequence.get(),
            format!(
                "mesh-control-checkpoint:{}:{}:{}:{}",
                checkpoint.mesh_id,
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition
            ),
            0,
        )),
        schema: checkpoint.schema.clone(),
        mesh_id: checkpoint.mesh_id.clone(),
        region: checkpoint.region.clone(),
        stream_family: checkpoint.stream_family.clone(),
        partition: checkpoint.partition.clone(),
        last_sequence: checkpoint.last_sequence.get(),
        last_digest: checkpoint.last_digest.to_string(),
        updated_at: checkpoint.updated_at.clone(),
    }))
}

fn decode_control_checkpoint_proto(bytes: &[u8]) -> AnyhowResult<ControlCheckpointRecord> {
    let proto = decode_deterministic_proto::<ControlCheckpointProto>(
        bytes,
        "control checkpoint CoreMeta row",
    )?;
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("control checkpoint CoreMeta row is missing common metadata"))?;
    let checkpoint = ControlCheckpointRecord {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        region: proto.region,
        stream_family: proto.stream_family,
        partition: proto.partition,
        last_sequence: ControlStreamSequence::new(proto.last_sequence)?,
        last_digest: ControlRecordDigest::new(proto.last_digest)?,
        updated_at: proto.updated_at,
    };
    validate_control_checkpoint(&checkpoint)?;
    Ok(checkpoint)
}

pub fn encode_control_stream_frame(
    header_proto: &[u8],
    payload_proto: &[u8],
) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
    let header_len = u32::try_from(header_proto.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_proto",
        }
    })?;
    if payload_proto.len() > MAX_CONTROL_PROTO_PAYLOAD_LEN {
        return Err(ControlStreamFrameError::PayloadTooLarge {
            actual: payload_proto.len(),
            max: MAX_CONTROL_PROTO_PAYLOAD_LEN,
        });
    }
    let payload_len = u64::try_from(payload_proto.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        }
    })?;
    let mut out = Vec::with_capacity(encoded_frame_len(header_proto.len(), payload_proto.len())?);
    out.extend_from_slice(CONTROL_STREAM_MAGIC);
    out.extend_from_slice(&CONTROL_STREAM_VERSION.to_be_bytes());
    out.extend_from_slice(&header_len.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&crc32(header_proto).to_be_bytes());
    out.extend_from_slice(&crc32(payload_proto).to_be_bytes());
    out.extend_from_slice(header_proto);
    out.extend_from_slice(payload_proto);
    Ok(out)
}

pub fn decode_control_stream_frame(
    input: &[u8],
) -> std::result::Result<(ControlStreamFrame, usize), ControlStreamFrameError> {
    let fixed = decode_fixed_header(input)?;
    let header_start = CONTROL_STREAM_FIXED_HEADER_LEN;
    let header_end = header_start.checked_add(fixed.header_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_proto",
        },
    )?;
    let payload_end = header_end.checked_add(fixed.payload_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        },
    )?;
    if input.len() < payload_end {
        return Err(ControlStreamFrameError::TooShort {
            context: "control stream frame payload_proto",
            needed: payload_end,
            actual: input.len(),
        });
    }

    let header_proto = input[header_start..header_end].to_vec();
    let payload_proto = input[header_end..payload_end].to_vec();
    if payload_proto.len() > MAX_CONTROL_PROTO_PAYLOAD_LEN {
        return Err(ControlStreamFrameError::PayloadTooLarge {
            actual: payload_proto.len(),
            max: MAX_CONTROL_PROTO_PAYLOAD_LEN,
        });
    }
    let actual_header_crc32 = crc32(&header_proto);
    if actual_header_crc32 != fixed.header_crc32 {
        return Err(ControlStreamFrameError::HeaderCrc32Mismatch {
            expected: fixed.header_crc32,
            actual: actual_header_crc32,
        });
    }
    let actual_payload_crc32 = crc32(&payload_proto);
    if actual_payload_crc32 != fixed.payload_crc32 {
        return Err(ControlStreamFrameError::PayloadCrc32Mismatch {
            expected: fixed.payload_crc32,
            actual: actual_payload_crc32,
        });
    }

    Ok((
        ControlStreamFrame {
            header_proto,
            payload_proto,
        },
        payload_end,
    ))
}

pub fn decode_control_stream_log(
    input: &[u8],
) -> std::result::Result<ControlStreamLog, ControlStreamFrameError> {
    let mut offset = 0usize;
    let mut records = Vec::new();
    while offset < input.len() {
        match decode_control_stream_frame(&input[offset..]) {
            Ok((frame, encoded_len)) => {
                let metadata = frame.metadata()?;
                records.push(ControlStreamLogRecord {
                    offset: offset as u64,
                    encoded_len,
                    metadata,
                    frame,
                });
                offset = offset.checked_add(encoded_len).ok_or(
                    ControlStreamFrameError::InvalidDeclaredLength {
                        context: "control stream log offset",
                    },
                )?;
            }
            Err(ControlStreamFrameError::TooShort { needed, actual, .. }) => {
                return Ok(ControlStreamLog {
                    records,
                    complete_len: offset as u64,
                    partial_final_frame: Some(PartialFinalFrame {
                        offset: offset as u64,
                        expected_len: needed,
                        actual_len: actual,
                    }),
                });
            }
            Err(err) => return Err(err),
        }
    }
    Ok(ControlStreamLog {
        records,
        complete_len: offset as u64,
        partial_final_frame: None,
    })
}

fn decode_fixed_header(
    input: &[u8],
) -> std::result::Result<FixedControlFrameHeader, ControlStreamFrameError> {
    if input.len() < CONTROL_STREAM_FIXED_HEADER_LEN {
        return Err(ControlStreamFrameError::TooShort {
            context: "control stream frame header",
            needed: CONTROL_STREAM_FIXED_HEADER_LEN,
            actual: input.len(),
        });
    }
    if &input[0..8] != CONTROL_STREAM_MAGIC {
        return Err(ControlStreamFrameError::InvalidMagic);
    }
    let version = u16::from_be_bytes(input[8..10].try_into().unwrap());
    if version != CONTROL_STREAM_VERSION {
        return Err(ControlStreamFrameError::UnsupportedVersion(version));
    }
    let header_len = u32::from_be_bytes(input[10..14].try_into().unwrap()) as usize;
    let payload_len = usize::try_from(u64::from_be_bytes(input[14..22].try_into().unwrap()))
        .map_err(|_| ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        })?;
    Ok(FixedControlFrameHeader {
        header_len,
        payload_len,
        header_crc32: u32::from_be_bytes(input[22..26].try_into().unwrap()),
        payload_crc32: u32::from_be_bytes(input[26..30].try_into().unwrap()),
    })
}

fn encoded_frame_len(
    header_len: usize,
    payload_len: usize,
) -> std::result::Result<usize, ControlStreamFrameError> {
    CONTROL_STREAM_FIXED_HEADER_LEN
        .checked_add(header_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or(ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame",
        })
}

fn metadata_from_header_proto(
    header_proto: &[u8],
) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
    let raw = ControlFrameHeaderProto::decode(header_proto)
        .map_err(|_| ControlStreamFrameError::InvalidHeaderProtobuf)?;
    if raw.encode_to_vec() != header_proto {
        return Err(ControlStreamFrameError::NonDeterministicHeaderProtobuf);
    }
    let sequence = ControlStreamSequence::new(raw.sequence)?;
    if raw.record_digest.is_empty() {
        return Err(ControlStreamFrameError::MissingHeaderField {
            field: "record_digest",
        });
    }
    let record_digest = ControlRecordDigest::new(raw.record_digest)?;
    Ok(ControlFrameMetadata {
        sequence,
        record_digest,
    })
}

fn is_valid_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("blake3:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[derive(Debug, Clone, Copy)]
struct FixedControlFrameHeader {
    header_len: usize,
    payload_len: usize,
    header_crc32: u32,
    payload_crc32: u32,
}

#[derive(Debug, Clone)]
struct StreamProjectionEntry {
    sequence: u64,
    generation: Option<u64>,
    digest: String,
    operation: String,
    payload_json: Vec<u8>,
}

pub fn decode_control_mutation_header(
    header_proto: &[u8],
) -> AnyhowResult<ControlFrameHeaderProto> {
    let header =
        ControlFrameHeaderProto::decode(header_proto).context("parse control mutation header")?;
    if header.encode_to_vec() != header_proto {
        return Err(anyhow!(
            "control mutation header protobuf is not deterministic or contains unknown fields"
        ));
    }
    Ok(header)
}

fn semantic_json(payload_json: &[u8]) -> AnyhowResult<serde_json::Value> {
    serde_json::from_slice(payload_json).context("parse projected control payload")
}

fn control_payload_operator_json(
    stream_family: &str,
    record_key: &str,
    payload_proto: &[u8],
) -> AnyhowResult<Vec<u8>> {
    if let Some(family) =
        crate::mesh_directory::RoutingRecordFamily::from_stream_family(stream_family)
    {
        return crate::mesh_directory::control_payload_operator_json(
            family,
            record_key,
            payload_proto,
        )
        .map_err(Into::into);
    }
    if crate::mesh_lifecycle::lifecycle_control_stream_families().contains(&stream_family) {
        return crate::mesh_lifecycle::control_payload_operator_json(
            stream_family,
            record_key,
            payload_proto,
        )
        .map_err(Into::into);
    }
    Err(anyhow!(
        "unknown mesh control stream family {stream_family}"
    ))
}

fn projection_digest(payload_json: &[u8]) -> AnyhowResult<String> {
    semantic_json(payload_json)?;
    Ok(ControlRecordDigest::blake3(payload_json).to_string())
}

fn is_delete_operation(operation: &str) -> bool {
    matches!(operation, "delete" | "deleted" | "tombstone")
}

fn control_stream_id(stream_family: &str, partition: &str) -> AnyhowResult<String> {
    validate_control_stream_scope(stream_family, "control stream family")?;
    validate_control_stream_partition(partition)?;
    Ok(format!(
        "{CONTROL_STREAM_ID_PREFIX}{stream_family}:{partition}"
    ))
}

fn control_stream_prefix(stream_family: &str) -> String {
    format!("{CONTROL_STREAM_ID_PREFIX}{stream_family}:")
}

fn control_checkpoint_row_key(
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<Vec<u8>> {
    validate_control_stream_scope(region, "control checkpoint region")?;
    validate_control_stream_scope(stream_family, "control checkpoint stream family")?;
    validate_control_stream_partition(partition)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CONTROL_CHECKPOINT_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(region),
        CoreMetaTuplePart::Utf8(stream_family),
        CoreMetaTuplePart::Utf8(partition),
    ])
}

fn control_checkpoint_partition_id(
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<String> {
    validate_control_stream_scope(region, "control checkpoint region")?;
    validate_control_stream_scope(stream_family, "control checkpoint stream family")?;
    validate_control_stream_partition(partition)?;
    Ok(format!(
        "mesh-control-checkpoint:{region}:{stream_family}:{partition}"
    ))
}

fn control_checkpoint_root_anchor_key(
    region: &str,
    stream_family: &str,
    partition: &str,
) -> String {
    format!("mesh/control-checkpoint/{region}/{stream_family}/{partition}")
}

fn control_checkpoint_root_publications(
    coordinator_root: String,
    data_root: String,
) -> Vec<CoreMutationRootPublication> {
    if coordinator_root == data_root {
        return vec![CoreMutationRootPublication {
            root_anchor_key: coordinator_root,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::MeshControl.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }
    vec![
        CoreMutationRootPublication::new(coordinator_root, WriterFamily::CoreControl.as_str())
            .coordinator(),
        CoreMutationRootPublication::new(data_root, WriterFamily::MeshControl.as_str()),
    ]
}

fn validate_control_stream_scope(value: &str, context: &str) -> AnyhowResult<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{context} is not a safe path component"));
    }
    Ok(())
}

fn validate_control_stream_partition(value: &str) -> AnyhowResult<()> {
    if value.len() != 4
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(anyhow!(
            "control stream partition must be four lowercase hex characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
