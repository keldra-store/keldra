use super::*;
use prost::{Message, Oneof};

pub(super) fn parse_stream_cursor(cursor: &str) -> Result<(String, u64)> {
    let (stream_id, sequence) = cursor
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("CoreStore watch cursor is malformed"))?;
    validate_logical_id(stream_id, "watch cursor stream id")?;
    if sequence.len() != 20 || !sequence.as_bytes().iter().all(u8::is_ascii_digit) {
        bail!("CoreStore watch cursor sequence is malformed");
    }
    let sequence = sequence.parse::<u64>()?;
    if sequence == 0 {
        bail!("CoreStore watch cursor sequence must be nonzero");
    }
    Ok((stream_id.to_string(), sequence))
}

pub(super) fn stream_head_from_records(records: &[StreamRecord]) -> (u64, String) {
    records
        .last()
        .map(|record| (record.sequence, record.event_hash.clone()))
        .unwrap_or_else(|| (0, ZERO_HASH.to_string()))
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationBatchProto {
    #[prost(string, tag = "1")]
    transaction_id: String,
    #[prost(string, tag = "2")]
    scope_partition: String,
    #[prost(string, tag = "3")]
    committed_by_principal: String,
    #[prost(message, repeated, tag = "4")]
    preconditions: Vec<CoreMutationPreconditionProto>,
    #[prost(message, repeated, tag = "5")]
    operations: Vec<CoreMutationOperationProto>,
    #[prost(message, repeated, tag = "6")]
    root_publications: Vec<CoreMutationRootPublicationProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationPreconditionsProto {
    #[prost(message, repeated, tag = "1")]
    preconditions: Vec<CoreMutationPreconditionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationOperationsProto {
    #[prost(message, repeated, tag = "1")]
    operations: Vec<CoreMutationOperationProto>,
    #[prost(message, repeated, tag = "2")]
    root_publications: Vec<CoreMutationRootPublicationProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationRootPublicationProto {
    #[prost(string, tag = "1")]
    root_anchor_key: String,
    #[prost(string, repeated, tag = "2")]
    writer_families: Vec<String>,
    #[prost(bool, tag = "3")]
    transaction_coordinator: bool,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationPreconditionProto {
    #[prost(oneof = "mutation_precondition_proto::Kind", tags = "2, 3, 4, 5")]
    kind: Option<mutation_precondition_proto::Kind>,
}

mod mutation_precondition_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        Fence(super::CoreMutationFencePreconditionProto),
        #[prost(message, tag = "3")]
        StreamHead(super::CoreMutationStreamHeadPreconditionProto),
        #[prost(message, tag = "4")]
        CoreMetaRow(super::CoreMutationCoreMetaRowPreconditionProto),
        #[prost(message, tag = "5")]
        CoreMetaLease(super::CoreMutationCoreMetaLeasePreconditionProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreFencePreconditionProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(string, tag = "3")]
    authenticated_principal: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationFencePreconditionProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaRowPreconditionProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    expected_payload_hash: Option<String>,
    #[prost(bool, tag = "5")]
    require_absent: bool,
    #[prost(bool, tag = "6")]
    require_present: bool,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaLeasePreconditionProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, tag = "4")]
    expected_payload_hash: String,
    #[prost(uint64, tag = "5")]
    expires_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationStreamHeadPreconditionProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(uint64, tag = "2")]
    expected_last_sequence: u64,
    #[prost(string, tag = "3")]
    expected_last_event_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationOperationProto {
    #[prost(oneof = "mutation_operation_proto::Kind", tags = "2, 3, 4")]
    kind: Option<mutation_operation_proto::Kind>,
}

mod mutation_operation_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        StreamAppend(super::CoreMutationStreamAppendOperationProto),
        #[prost(message, tag = "3")]
        CoreMetaPut(super::CoreMutationCoreMetaPutOperationProto),
        #[prost(message, tag = "4")]
        CoreMetaDelete(super::CoreMutationCoreMetaDeleteOperationProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationStreamAppendOperationProto {
    #[prost(string, tag = "1")]
    partition_id: String,
    #[prost(string, tag = "2")]
    stream_id: String,
    #[prost(string, tag = "3")]
    record_kind: String,
    #[prost(bytes, tag = "4")]
    payload: Vec<u8>,
    #[prost(string, optional, tag = "5")]
    idempotency_key: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaPutOperationProto {
    #[prost(string, tag = "1")]
    partition_id: String,
    #[prost(string, tag = "2")]
    cf: String,
    #[prost(uint32, tag = "3")]
    table_id: u32,
    #[prost(bytes, tag = "4")]
    tuple_key: Vec<u8>,
    #[prost(bytes, tag = "5")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaDeleteOperationProto {
    #[prost(string, tag = "1")]
    partition_id: String,
    #[prost(string, tag = "2")]
    cf: String,
    #[prost(uint32, tag = "3")]
    table_id: u32,
    #[prost(bytes, tag = "4")]
    tuple_key: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefTargetProto {
    #[prost(string, tag = "1")]
    hash: String,
    #[prost(uint64, tag = "2")]
    logical_size: u64,
    #[prost(string, tag = "3")]
    manifest_ref: String,
    #[prost(message, optional, tag = "4")]
    encoding: Option<CoreObjectRefEncodingProto>,
    #[prost(message, repeated, tag = "5")]
    placements: Vec<CoreObjectRefPlacementProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefEncodingProto {
    #[prost(string, tag = "1")]
    block_id: String,
    #[prost(string, tag = "2")]
    profile_id: String,
    #[prost(uint32, tag = "3")]
    data_shards: u32,
    #[prost(uint32, tag = "4")]
    parity_shards: u32,
    #[prost(uint32, tag = "5")]
    minimum_read_shards: u32,
    #[prost(uint32, tag = "6")]
    minimum_write_ack_shards: u32,
    #[prost(uint64, tag = "7")]
    stripe_size: u64,
    #[prost(string, tag = "8")]
    placement_scope: String,
    #[prost(string, tag = "9")]
    repair_priority: String,
    #[prost(string, tag = "10")]
    encryption: String,
    #[prost(string, tag = "11")]
    stored_hash: String,
    #[prost(message, optional, tag = "12")]
    compression: Option<CoreObjectRefCompressionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefCompressionProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(uint32, tag = "2")]
    level: u32,
    #[prost(uint64, tag = "3")]
    uncompressed_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(string, tag = "5")]
    dictionary_id: String,
    #[prost(string, tag = "6")]
    descriptor_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefPlacementProto {
    #[prost(uint32, tag = "1")]
    shard_index: u32,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(string, tag = "3")]
    region_id: String,
    #[prost(string, tag = "4")]
    cell_id: String,
    #[prost(string, tag = "5")]
    shard_hash: String,
    #[prost(uint64, tag = "6")]
    stored_size: u64,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(uint64, tag = "8")]
    placement_epoch: u64,
    #[prost(uint64, tag = "9")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "10")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "11")]
    signed_payload_hash: String,
    #[prost(string, tag = "12")]
    signature_algorithm: String,
    #[prost(bytes, tag = "13")]
    receipt_signature: Vec<u8>,
}

pub(super) fn encode_core_mutation_batch(batch: &CoreMutationBatch) -> Result<Vec<u8>> {
    encode_deterministic(core_mutation_batch_to_proto(batch)?)
}

pub(super) fn decode_core_mutation_batch(bytes: &[u8]) -> Result<CoreMutationBatch> {
    let proto = CoreMutationBatchProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "CoreStore mutation batch")?;
    core_mutation_batch_from_proto(proto)
}

fn core_mutation_batch_to_proto(batch: &CoreMutationBatch) -> Result<CoreMutationBatchProto> {
    Ok(CoreMutationBatchProto {
        transaction_id: batch.transaction_id.clone(),
        scope_partition: batch.scope_partition.clone(),
        committed_by_principal: batch.committed_by_principal.clone(),
        preconditions: batch
            .preconditions
            .iter()
            .map(core_mutation_precondition_to_proto)
            .collect(),
        operations: batch
            .operations
            .iter()
            .map(core_mutation_operation_to_proto)
            .collect::<Result<Vec<_>>>()?,
        root_publications: batch
            .root_publications
            .iter()
            .map(core_mutation_root_publication_to_proto)
            .collect(),
    })
}

fn core_mutation_batch_from_proto(proto: CoreMutationBatchProto) -> Result<CoreMutationBatch> {
    Ok(CoreMutationBatch {
        transaction_id: proto.transaction_id,
        scope_partition: proto.scope_partition,
        committed_by_principal: proto.committed_by_principal,
        preconditions: proto
            .preconditions
            .into_iter()
            .map(core_mutation_precondition_from_proto)
            .collect::<Result<Vec<_>>>()?,
        operations: proto
            .operations
            .into_iter()
            .map(core_mutation_operation_from_proto)
            .collect::<Result<Vec<_>>>()?,
        root_publications: proto
            .root_publications
            .into_iter()
            .map(core_mutation_root_publication_from_proto)
            .collect(),
    })
}

pub(super) fn core_mutation_preconditions_hash(
    preconditions: &[CoreMutationPrecondition],
) -> Result<String> {
    let proto = CoreMutationPreconditionsProto {
        preconditions: preconditions
            .iter()
            .map(core_mutation_precondition_to_proto)
            .collect(),
    };
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_deterministic(proto)?)
    ))
}

pub(super) fn core_mutation_operations_hash(
    operations: &[CoreMutationOperation],
    root_publications: &[CoreMutationRootPublication],
) -> Result<String> {
    let proto = CoreMutationOperationsProto {
        operations: operations
            .iter()
            .map(core_mutation_operation_to_proto)
            .collect::<Result<Vec<_>>>()?,
        root_publications: root_publications
            .iter()
            .map(core_mutation_root_publication_to_proto)
            .collect(),
    };
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_deterministic(proto)?)
    ))
}

/// Hash the caller's logical mutation plan independently of the publication
/// generations and transaction id that CoreStore binds during admission.
pub(super) fn core_mutation_logical_operations_hash(
    operations: &[CoreMutationOperation],
    root_publications: &[CoreMutationRootPublication],
) -> Result<String> {
    let mut logical_operations = operations.to_vec();
    for operation in &mut logical_operations {
        let CoreMutationOperation::CoreMetaPut { payload, .. } = operation else {
            continue;
        };
        let mut common = core_meta_row_common_from_payload(payload)?;
        common.root_generation = 0;
        common.transaction_id.clear();
        *payload = replace_core_meta_row_common(payload, &common)?;
    }
    core_mutation_operations_hash(&logical_operations, root_publications)
}

fn core_mutation_root_publication_to_proto(
    publication: &CoreMutationRootPublication,
) -> CoreMutationRootPublicationProto {
    CoreMutationRootPublicationProto {
        root_anchor_key: publication.root_anchor_key.clone(),
        writer_families: publication.writer_families.clone(),
        transaction_coordinator: publication.transaction_coordinator,
    }
}

fn core_mutation_root_publication_from_proto(
    publication: CoreMutationRootPublicationProto,
) -> CoreMutationRootPublication {
    CoreMutationRootPublication {
        root_anchor_key: publication.root_anchor_key,
        writer_families: publication.writer_families,
        transaction_coordinator: publication.transaction_coordinator,
    }
}

fn core_mutation_precondition_to_proto(
    precondition: &CoreMutationPrecondition,
) -> CoreMutationPreconditionProto {
    let kind = match precondition {
        CoreMutationPrecondition::Fence {
            fence_name,
            fence_token,
        } => mutation_precondition_proto::Kind::Fence(CoreMutationFencePreconditionProto {
            fence_name: fence_name.clone(),
            fence_token: *fence_token,
        }),
        CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } => mutation_precondition_proto::Kind::CoreMetaRow(
            CoreMutationCoreMetaRowPreconditionProto {
                cf: cf.clone(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload_hash.clone(),
                require_absent: *require_absent,
                require_present: *require_present,
            },
        ),
        CoreMutationPrecondition::CoreMetaLease {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            expires_at_unix_nanos,
        } => mutation_precondition_proto::Kind::CoreMetaLease(
            CoreMutationCoreMetaLeasePreconditionProto {
                cf: cf.clone(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload_hash.clone(),
                expires_at_unix_nanos: *expires_at_unix_nanos,
            },
        ),
        CoreMutationPrecondition::StreamHead {
            stream_id,
            expected_last_sequence,
            expected_last_event_hash,
        } => {
            mutation_precondition_proto::Kind::StreamHead(CoreMutationStreamHeadPreconditionProto {
                stream_id: stream_id.clone(),
                expected_last_sequence: *expected_last_sequence,
                expected_last_event_hash: expected_last_event_hash.clone(),
            })
        }
    };
    CoreMutationPreconditionProto { kind: Some(kind) }
}

fn core_mutation_precondition_from_proto(
    proto: CoreMutationPreconditionProto,
) -> Result<CoreMutationPrecondition> {
    Ok(
        match proto
            .kind
            .ok_or_else(|| anyhow!("CoreStore mutation precondition is missing kind"))?
        {
            mutation_precondition_proto::Kind::Fence(value) => CoreMutationPrecondition::Fence {
                fence_name: value.fence_name,
                fence_token: value.fence_token,
            },
            mutation_precondition_proto::Kind::CoreMetaRow(value) => {
                CoreMutationPrecondition::CoreMetaRow {
                    cf: value.cf,
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta row precondition table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    expected_payload_hash: value.expected_payload_hash,
                    require_absent: value.require_absent,
                    require_present: value.require_present,
                }
            }
            mutation_precondition_proto::Kind::CoreMetaLease(value) => {
                CoreMutationPrecondition::CoreMetaLease {
                    cf: value.cf,
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta lease precondition table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    expected_payload_hash: value.expected_payload_hash,
                    expires_at_unix_nanos: value.expires_at_unix_nanos,
                }
            }
            mutation_precondition_proto::Kind::StreamHead(value) => {
                CoreMutationPrecondition::StreamHead {
                    stream_id: value.stream_id,
                    expected_last_sequence: value.expected_last_sequence,
                    expected_last_event_hash: value.expected_last_event_hash,
                }
            }
        },
    )
}

fn core_fence_precondition_to_proto(value: &CoreFencePrecondition) -> CoreFencePreconditionProto {
    CoreFencePreconditionProto {
        fence_name: value.fence_name.clone(),
        fence_token: value.fence_token,
        authenticated_principal: value.authenticated_principal.clone(),
    }
}

fn core_fence_precondition_from_proto(value: CoreFencePreconditionProto) -> CoreFencePrecondition {
    CoreFencePrecondition {
        fence_name: value.fence_name,
        fence_token: value.fence_token,
        authenticated_principal: value.authenticated_principal,
    }
}

fn core_mutation_operation_to_proto(
    operation: &CoreMutationOperation,
) -> Result<CoreMutationOperationProto> {
    let kind = match operation {
        CoreMutationOperation::StreamAppend {
            partition_id,
            stream_id,
            record_kind,
            payload,
            idempotency_key,
        } => mutation_operation_proto::Kind::StreamAppend(CoreMutationStreamAppendOperationProto {
            partition_id: partition_id.clone(),
            stream_id: stream_id.clone(),
            record_kind: record_kind.clone(),
            payload: payload.clone(),
            idempotency_key: idempotency_key.clone(),
        }),
        CoreMutationOperation::CoreMetaPut {
            partition_id,
            cf,
            table_id,
            tuple_key,
            payload,
        } => mutation_operation_proto::Kind::CoreMetaPut(CoreMutationCoreMetaPutOperationProto {
            partition_id: partition_id.clone(),
            cf: cf.clone(),
            table_id: u32::from(*table_id),
            tuple_key: tuple_key.clone(),
            payload: payload.clone(),
        }),
        CoreMutationOperation::CoreMetaDelete {
            partition_id,
            cf,
            table_id,
            tuple_key,
        } => mutation_operation_proto::Kind::CoreMetaDelete(
            CoreMutationCoreMetaDeleteOperationProto {
                partition_id: partition_id.clone(),
                cf: cf.clone(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
            },
        ),
    };
    Ok(CoreMutationOperationProto { kind: Some(kind) })
}

fn core_mutation_operation_from_proto(
    proto: CoreMutationOperationProto,
) -> Result<CoreMutationOperation> {
    Ok(
        match proto
            .kind
            .ok_or_else(|| anyhow!("CoreStore mutation operation is missing kind"))?
        {
            mutation_operation_proto::Kind::StreamAppend(value) => {
                CoreMutationOperation::StreamAppend {
                    partition_id: value.partition_id,
                    stream_id: value.stream_id,
                    record_kind: value.record_kind,
                    payload: value.payload,
                    idempotency_key: value.idempotency_key,
                }
            }
            mutation_operation_proto::Kind::CoreMetaPut(value) => {
                CoreMutationOperation::CoreMetaPut {
                    partition_id: value.partition_id,
                    cf: value.cf,
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta put operation table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    payload: value.payload,
                }
            }
            mutation_operation_proto::Kind::CoreMetaDelete(value) => {
                CoreMutationOperation::CoreMetaDelete {
                    partition_id: value.partition_id,
                    cf: value.cf,
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta delete operation table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                }
            }
        },
    )
}

fn encode_deterministic(message: impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_round_trips(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministically encoded");
    }
    Ok(())
}

pub(super) fn validate_batch_partitions(batch: &CoreMutationBatch) -> Result<()> {
    let mut coremeta_ops = BTreeSet::new();
    for precondition in &batch.preconditions {
        match precondition {
            CoreMutationPrecondition::Fence { fence_name, .. } => {
                validate_logical_id(fence_name, "precondition fence name")?;
            }
            CoreMutationPrecondition::CoreMetaRow {
                cf,
                expected_payload_hash,
                require_absent,
                require_present,
                ..
            } => {
                validate_logical_id(cf, "precondition CoreMeta column family")?;
                if *require_absent && *require_present {
                    bail!("CoreMeta row precondition cannot require both absent and present");
                }
                if let Some(hash) = expected_payload_hash {
                    validate_coremeta_digest(hash, "precondition CoreMeta payload hash")?;
                }
            }
            CoreMutationPrecondition::CoreMetaLease {
                cf,
                expected_payload_hash,
                expires_at_unix_nanos,
                ..
            } => {
                validate_logical_id(cf, "precondition CoreMeta lease column family")?;
                validate_coremeta_digest(
                    expected_payload_hash,
                    "precondition CoreMeta lease payload hash",
                )?;
                if *expires_at_unix_nanos == 0 {
                    bail!("CoreMeta lease precondition expiry must be nonzero");
                }
            }
            CoreMutationPrecondition::StreamHead {
                stream_id,
                expected_last_event_hash,
                ..
            } => {
                validate_logical_id(stream_id, "precondition stream id")?;
                validate_hash(expected_last_event_hash, "precondition stream head hash")?;
            }
        }
    }
    for operation in &batch.operations {
        match operation {
            CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                ..
            } => {
                validate_logical_id(partition_id, "operation partition id")?;
                validate_logical_id(stream_id, "operation stream id")?;
                if partition_id != &batch.scope_partition {
                    bail!("CrossPartitionAtomicMutationUnsupported");
                }
            }
            CoreMutationOperation::CoreMetaPut {
                partition_id,
                cf,
                table_id,
                tuple_key,
                payload,
            } => {
                validate_logical_id(partition_id, "operation partition id")?;
                validate_logical_id(cf, "operation CoreMeta column family")?;
                if partition_id != &batch.scope_partition {
                    bail!("CrossPartitionAtomicMutationUnsupported");
                }
                crate::core_store::validate_coremeta_operation_payload(
                    cf, *table_id, tuple_key, payload,
                )?;
                if !coremeta_ops.insert((cf.clone(), *table_id, tuple_key.clone())) {
                    bail!("CoreStore mutation batch updates a CoreMeta row more than once");
                }
            }
            CoreMutationOperation::CoreMetaDelete {
                partition_id,
                cf,
                table_id,
                tuple_key,
            } => {
                validate_logical_id(partition_id, "operation partition id")?;
                validate_logical_id(cf, "operation CoreMeta column family")?;
                if partition_id != &batch.scope_partition {
                    bail!("CrossPartitionAtomicMutationUnsupported");
                }
                crate::core_store::validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                if !coremeta_ops.insert((cf.clone(), *table_id, tuple_key.clone())) {
                    bail!("CoreStore mutation batch updates a CoreMeta row more than once");
                }
            }
        }
    }
    Ok(())
}

fn validate_coremeta_digest(hash: &str, label: &str) -> Result<()> {
    let Some((algorithm, value)) = hash.split_once(':') else {
        bail!("CoreStore {label} must use algorithm:hex encoding");
    };
    if !matches!(algorithm, "sha256" | "blake3")
        || value.len() != 64
        || !hash.is_ascii()
        || !value.as_bytes().iter().all(u8::is_ascii_hexdigit)
    {
        bail!("CoreStore {label} must be a sha256 or blake3 digest");
    }
    Ok(())
}

pub(super) fn root_catalog_stream_id(mesh_id: &str) -> String {
    format!("core_root_catalog_history:{mesh_id}")
}

pub(super) fn quorum_profile_stream_id(placement_group: &str) -> String {
    format!("core_quorum_profile_history:{placement_group}")
}

pub(crate) fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "core-object-ref:{}",
        URL_SAFE_NO_PAD.encode(encode_deterministic(core_object_ref_to_proto(object_ref))?)
    ))
}

pub(crate) fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix("core-object-ref:")
        .ok_or_else(|| anyhow!("CoreStore object target is not a CoreObjectRef"))?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
    let proto = CoreObjectRefTargetProto::decode(bytes.as_slice())?;
    ensure_round_trips(&proto, &bytes, "CoreStore object ref target")?;
    core_object_ref_from_proto(proto)
}

fn core_object_ref_to_proto(value: &CoreObjectRef) -> CoreObjectRefTargetProto {
    CoreObjectRefTargetProto {
        hash: value.hash.clone(),
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref.clone(),
        encoding: Some(CoreObjectRefEncodingProto {
            block_id: value.encoding.block_id.clone(),
            profile_id: value.encoding.profile_id.clone(),
            data_shards: u32::from(value.encoding.data_shards),
            parity_shards: u32::from(value.encoding.parity_shards),
            minimum_read_shards: u32::from(value.encoding.minimum_read_shards),
            minimum_write_ack_shards: u32::from(value.encoding.minimum_write_ack_shards),
            stripe_size: value.encoding.stripe_size,
            placement_scope: value.encoding.placement_scope.clone(),
            repair_priority: value.encoding.repair_priority.clone(),
            stored_hash: value.encoding.stored_hash.clone(),
            compression: Some(core_object_ref_compression_to_proto(
                &value.encoding.compression,
            )),
            encryption: value.encoding.encryption.clone(),
        }),
        placements: value
            .placements
            .iter()
            .map(|placement| CoreObjectRefPlacementProto {
                shard_index: u32::from(placement.shard_index),
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: placement.shard_hash.clone(),
                stored_size: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
            })
            .collect(),
    }
}

fn core_object_ref_from_proto(proto: CoreObjectRefTargetProto) -> Result<CoreObjectRef> {
    let encoding = proto
        .encoding
        .ok_or_else(|| anyhow!("CoreStore object ref target is missing encoding"))?;
    Ok(CoreObjectRef {
        hash: proto.hash,
        logical_size: proto.logical_size,
        manifest_ref: proto.manifest_ref,
        encoding: CoreObjectEncoding {
            block_id: encoding.block_id,
            profile_id: encoding.profile_id,
            data_shards: encoding
                .data_shards
                .try_into()
                .map_err(|_| anyhow!("CoreStore object ref target data_shards exceeds u16"))?,
            parity_shards: encoding
                .parity_shards
                .try_into()
                .map_err(|_| anyhow!("CoreStore object ref target parity_shards exceeds u16"))?,
            minimum_read_shards: encoding.minimum_read_shards.try_into().map_err(|_| {
                anyhow!("CoreStore object ref target minimum_read_shards exceeds u16")
            })?,
            minimum_write_ack_shards: encoding.minimum_write_ack_shards.try_into().map_err(
                |_| anyhow!("CoreStore object ref target minimum_write_ack_shards exceeds u16"),
            )?,
            stripe_size: encoding.stripe_size,
            placement_scope: encoding.placement_scope,
            repair_priority: encoding.repair_priority,
            stored_hash: encoding.stored_hash,
            compression: core_object_ref_compression_from_proto(encoding.compression.ok_or_else(
                || anyhow!("CoreStore object ref target is missing compression descriptor"),
            )?),
            encryption: encoding.encryption,
        },
        placements: proto
            .placements
            .into_iter()
            .map(|placement| {
                Ok(CoreObjectPlacement {
                    shard_index: placement.shard_index.try_into().map_err(|_| {
                        anyhow!("CoreStore object ref target shard_index exceeds u16")
                    })?,
                    node_id: placement.node_id,
                    region_id: placement.region_id,
                    cell_id: placement.cell_id,
                    shard_hash: placement.shard_hash,
                    stored_size: placement.stored_size,
                    generation: placement.generation,
                    placement_epoch: placement.placement_epoch,
                    fsync_sequence: placement.fsync_sequence,
                    written_at_unix_nanos: placement.written_at_unix_nanos,
                    signed_payload_hash: placement.signed_payload_hash,
                    signature_algorithm: placement.signature_algorithm,
                    receipt_signature: placement.receipt_signature,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn core_object_ref_compression_to_proto(
    value: &CoreCompressionDescriptor,
) -> CoreObjectRefCompressionProto {
    CoreObjectRefCompressionProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn core_object_ref_compression_from_proto(
    value: CoreObjectRefCompressionProto,
) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

#[cfg(test)]
pub(super) fn encode_manifest_ref(hash: &str) -> String {
    encode_manifest_ref_with_profile(hash, LOCAL_ERASURE_PROFILE_ID)
}

pub(super) fn encode_manifest_ref_with_profile(hash: &str, profile_id: &str) -> String {
    format!("core-manifest-sha256:{hash}:profile:{profile_id}")
}

pub(super) fn decode_manifest_ref(manifest_ref: &str) -> Result<&str> {
    Ok(decode_manifest_ref_parts(manifest_ref)?.0)
}

pub(super) fn decode_manifest_ref_profile(manifest_ref: &str) -> Result<&str> {
    Ok(decode_manifest_ref_parts(manifest_ref)?.1)
}

pub(super) fn decode_manifest_ref_parts(manifest_ref: &str) -> Result<(&str, &str)> {
    let raw = manifest_ref
        .strip_prefix("core-manifest-sha256:")
        .ok_or_else(|| anyhow!("CoreStore manifest_ref is not a CoreStore manifest reference"))?;
    let Some((hash, profile)) = raw.split_once(":profile:") else {
        bail!("CoreStore manifest_ref is missing erasure profile");
    };
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore manifest_ref hash is invalid");
    }
    validate_logical_id(profile, "manifest erasure profile")?;
    Ok((hash, profile))
}
