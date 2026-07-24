use super::*;

pub(super) fn encode_multipart_event(
    event: MultipartMutationKind,
    upload: Option<&MultipartUpload>,
    part: Option<&MultipartUploadPart>,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = MultipartEventProto {
        schema: MULTIPART_EVENT_SCHEMA.to_string(),
        event: event.as_str().to_string(),
        upload: upload.map(upload_to_proto).transpose()?,
        part: part.map(part_to_proto).transpose()?,
        emitted_at_unix_nanos: datetime_to_unix_nanos(&Utc::now())?,
        fence_token,
        mutation_id: mutation_id.to_string(),
    };
    encode_proto(&proto, "multipart event")
}

#[cfg(test)]
pub(super) fn decode_multipart_event(bytes: &[u8]) -> Result<MultipartEventProto> {
    let proto = decode_deterministic_proto::<MultipartEventProto>(bytes, "multipart event")?;
    if proto.schema != MULTIPART_EVENT_SCHEMA {
        anyhow::bail!("multipart event has invalid schema");
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("multipart event has invalid mutation id"))?;
    match proto.event.as_str() {
        "create_upload" | "complete_upload" | "abort_upload" => {
            let upload = proto
                .upload
                .clone()
                .ok_or_else(|| anyhow!("multipart upload event is missing upload"))?;
            let _ = upload_from_proto(upload)?;
        }
        "upsert_part" => {
            let part = proto
                .part
                .clone()
                .ok_or_else(|| anyhow!("multipart part event is missing part"))?;
            let _ = part_from_proto(part)?;
        }
        other => anyhow::bail!("unknown multipart metadata event {other}"),
    }
    Ok(proto)
}

#[cfg(test)]
pub(super) fn decode_multipart_event_fence(bytes: &[u8]) -> Result<u64> {
    Ok(decode_multipart_event(bytes)?.fence_token)
}

pub(super) fn current_upload_payload(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
) -> Result<(Option<Vec<u8>>, Option<MultipartUploadCurrentRow>)> {
    let payload = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_upload_row_key(tenant_id, bucket_id, upload_row_id)?,
    )?;
    let row = payload
        .as_deref()
        .map(decode_committed_upload_current_row)
        .transpose()?;
    Ok((payload, row))
}

pub(super) fn current_part_payload(
    store: &CoreStore,
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number: i32,
) -> Result<(Option<Vec<u8>>, Option<MultipartPartCurrentRow>)> {
    let payload = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_PART_CURRENT_ROW,
        &multipart_part_row_key(tenant_id, bucket_id, upload_row_id, part_number)?,
    )?;
    let row = payload
        .as_deref()
        .map(decode_committed_part_current_row)
        .transpose()?;
    Ok((payload, row))
}

pub(super) fn decode_upload_current_record(
    record: &CoreMetaRecord,
) -> Result<MultipartUploadCurrentRow> {
    let row = decode_committed_upload_current_row(&record.payload)?;
    let tuple_key = core_meta_record_tuple_key(&record.key)?;
    if tuple_key
        != multipart_upload_row_key(row.upload.tenant_id, row.upload.bucket_id, row.upload.id)?
    {
        return Err(anyhow!(
            "multipart upload current CoreMeta row key mismatch"
        ));
    }
    Ok(row)
}

pub(super) fn decode_part_current_record(
    record: &CoreMetaRecord,
) -> Result<MultipartPartCurrentRow> {
    let row = decode_committed_part_current_row(&record.payload)?;
    let tuple_key = core_meta_record_tuple_key(&record.key)?;
    if tuple_key
        != multipart_part_row_key(
            row.tenant_id,
            row.bucket_id,
            row.part.upload_id,
            row.part.part_number,
        )?
    {
        return Err(anyhow!("multipart part current CoreMeta row key mismatch"));
    }
    Ok(row)
}

pub(super) fn encode_upload_current_row(row: &MultipartUploadCurrentRow) -> Result<Vec<u8>> {
    let proto = MultipartUploadCurrentRowProto {
        schema: MULTIPART_UPLOAD_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(core_meta_committed_row_common(
            multipart_realm_id(row.upload.tenant_id),
            core_meta_root_key_hash(&multipart_current_root_key(
                row.upload.tenant_id,
                row.upload.bucket_id,
            )),
            MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION,
            MULTIPART_CURRENT_ROW_CANDIDATE_TRANSACTION_ID,
            0,
        )),
        upload: Some(upload_to_proto(&row.upload)?),
        logical_revision: row.logical_revision,
    };
    let bytes = encode_proto(&proto, "multipart upload current CoreMeta row")?;
    ensure_current_payload_size(&bytes, "multipart upload current CoreMeta row")?;
    Ok(bytes)
}

pub(super) fn decode_upload_current_row(bytes: &[u8]) -> Result<MultipartUploadCurrentRow> {
    decode_upload_current_row_with_common(bytes).map(|(row, _)| row)
}

pub(super) fn decode_committed_upload_current_row(
    bytes: &[u8],
) -> Result<MultipartUploadCurrentRow> {
    let (row, common) = decode_upload_current_row_with_common(bytes)?;
    validate_committed_current_row_common(&common)?;
    Ok(row)
}

fn decode_upload_current_row_with_common(
    bytes: &[u8],
) -> Result<(
    MultipartUploadCurrentRow,
    crate::core_store::CoreMetaRowCommonProto,
)> {
    ensure_current_payload_size(bytes, "multipart upload current CoreMeta row")?;
    let proto = decode_deterministic_proto::<MultipartUploadCurrentRowProto>(
        bytes,
        "multipart upload current CoreMeta row",
    )?;
    if proto.schema != MULTIPART_UPLOAD_CURRENT_ROW_SCHEMA {
        anyhow::bail!("multipart upload current CoreMeta row has invalid schema");
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("multipart upload current CoreMeta row missing common metadata"))?;
    let upload = upload_from_proto(
        proto
            .upload
            .ok_or_else(|| anyhow!("multipart upload current CoreMeta row missing upload"))?,
    )?;
    validate_current_row_common(&common, upload.tenant_id, upload.bucket_id)?;
    if proto.logical_revision == 0 {
        anyhow::bail!("multipart upload current row logical revision is zero");
    }
    Ok((
        MultipartUploadCurrentRow {
            upload,
            logical_revision: proto.logical_revision,
        },
        common,
    ))
}

pub(super) fn encode_part_current_row(row: &MultipartPartCurrentRow) -> Result<Vec<u8>> {
    let proto = MultipartPartCurrentRowProto {
        schema: MULTIPART_PART_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(core_meta_committed_row_common(
            multipart_realm_id(row.tenant_id),
            core_meta_root_key_hash(&multipart_current_root_key(row.tenant_id, row.bucket_id)),
            MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION,
            MULTIPART_CURRENT_ROW_CANDIDATE_TRANSACTION_ID,
            0,
        )),
        tenant_id: row.tenant_id,
        bucket_id: row.bucket_id,
        part: Some(part_to_proto(&row.part)?),
        logical_revision: row.logical_revision,
    };
    let bytes = encode_proto(&proto, "multipart part current CoreMeta row")?;
    ensure_current_payload_size(&bytes, "multipart part current CoreMeta row")?;
    Ok(bytes)
}

pub(super) fn decode_part_current_row(bytes: &[u8]) -> Result<MultipartPartCurrentRow> {
    decode_part_current_row_with_common(bytes).map(|(row, _)| row)
}

pub(super) fn decode_committed_part_current_row(bytes: &[u8]) -> Result<MultipartPartCurrentRow> {
    let (row, common) = decode_part_current_row_with_common(bytes)?;
    validate_committed_current_row_common(&common)?;
    Ok(row)
}

fn decode_part_current_row_with_common(
    bytes: &[u8],
) -> Result<(
    MultipartPartCurrentRow,
    crate::core_store::CoreMetaRowCommonProto,
)> {
    ensure_current_payload_size(bytes, "multipart part current CoreMeta row")?;
    let proto = decode_deterministic_proto::<MultipartPartCurrentRowProto>(
        bytes,
        "multipart part current CoreMeta row",
    )?;
    if proto.schema != MULTIPART_PART_CURRENT_ROW_SCHEMA {
        anyhow::bail!("multipart part current CoreMeta row has invalid schema");
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("multipart part current CoreMeta row missing common metadata"))?;
    validate_current_row_common(&common, proto.tenant_id, proto.bucket_id)?;
    let part = part_from_proto(
        proto
            .part
            .ok_or_else(|| anyhow!("multipart part current CoreMeta row missing part"))?,
    )?;
    if proto.logical_revision == 0 {
        anyhow::bail!("multipart part current row logical revision is zero");
    }
    Ok((
        MultipartPartCurrentRow {
            tenant_id: proto.tenant_id,
            bucket_id: proto.bucket_id,
            part,
            logical_revision: proto.logical_revision,
        },
        common,
    ))
}

pub(super) fn validate_current_row_common(
    common: &crate::core_store::CoreMetaRowCommonProto,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<()> {
    if common.realm_id != multipart_realm_id(tenant_id) {
        anyhow::bail!("multipart current CoreMeta row has invalid realm");
    }
    if common.root_key_hash
        != core_meta_root_key_hash(&multipart_current_root_key(tenant_id, bucket_id))
    {
        anyhow::bail!("multipart current CoreMeta row has invalid root key hash");
    }
    if common.visibility_state != crate::core_store::CoreMetaVisibilityState::Committed as i32 {
        anyhow::bail!("multipart current CoreMeta row is not committed");
    }
    let expected = core_meta_committed_row_common(
        multipart_realm_id(tenant_id),
        core_meta_root_key_hash(&multipart_current_root_key(tenant_id, bucket_id)),
        MULTIPART_CURRENT_ROW_CANDIDATE_GENERATION,
        MULTIPART_CURRENT_ROW_CANDIDATE_TRANSACTION_ID,
        0,
    );
    if common.transaction_id.is_empty()
        || common.payload_schema_version != expected.payload_schema_version
    {
        anyhow::bail!("multipart current CoreMeta row has invalid common metadata");
    }
    Ok(())
}

fn validate_committed_current_row_common(
    common: &crate::core_store::CoreMetaRowCommonProto,
) -> Result<()> {
    if common.root_generation == 0 {
        anyhow::bail!("multipart committed CoreMeta row has zero publication generation");
    }
    Ok(())
}

fn encode_proto<M>(message: &M, label: &str) -> Result<Vec<u8>>
where
    M: Message + Default,
{
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    let decoded = M::decode(bytes.as_slice())?;
    let mut canonical = Vec::new();
    decoded.encode(&mut canonical)?;
    if canonical != bytes {
        anyhow::bail!("{label} is not deterministic protobuf");
    }
    Ok(bytes)
}

fn decode_deterministic_proto<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(bytes)?;
    let mut canonical = Vec::new();
    value.encode(&mut canonical)?;
    if canonical != bytes {
        anyhow::bail!("{label} is not deterministic protobuf");
    }
    Ok(value)
}

pub(super) fn ensure_current_payload_size(bytes: &[u8], label: &str) -> Result<()> {
    if bytes.len() > MULTIPART_MAX_CURRENT_PROTO_BYTES {
        anyhow::bail!(
            "{label} is {} bytes, exceeding {} bytes",
            bytes.len(),
            MULTIPART_MAX_CURRENT_PROTO_BYTES
        );
    }
    Ok(())
}

pub(super) fn upload_to_proto(upload: &MultipartUpload) -> Result<MultipartUploadProto> {
    Ok(MultipartUploadProto {
        schema: MULTIPART_UPLOAD_SCHEMA.to_string(),
        id: upload.id,
        tenant_id: upload.tenant_id,
        bucket_id: upload.bucket_id,
        key: upload.key.clone(),
        upload_uuid: upload.upload_id.as_bytes().to_vec(),
        created_at_unix_nanos: datetime_to_unix_nanos(&upload.created_at)?,
        completed_at_unix_nanos: upload
            .completed_at
            .as_ref()
            .map(datetime_to_unix_nanos)
            .transpose()?,
        aborted_at_unix_nanos: upload
            .aborted_at
            .as_ref()
            .map(datetime_to_unix_nanos)
            .transpose()?,
    })
}

pub(super) fn upload_from_proto(proto: MultipartUploadProto) -> Result<MultipartUpload> {
    if proto.schema != MULTIPART_UPLOAD_SCHEMA {
        anyhow::bail!("multipart upload current state has invalid schema");
    }
    let upload_uuid: [u8; 16] = proto
        .upload_uuid
        .try_into()
        .map_err(|_| anyhow!("multipart upload current state has invalid uuid"))?;
    Ok(MultipartUpload {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        key: proto.key,
        upload_id: uuid::Uuid::from_bytes(upload_uuid),
        created_at: datetime_from_unix_nanos(proto.created_at_unix_nanos)?,
        completed_at: proto
            .completed_at_unix_nanos
            .map(datetime_from_unix_nanos)
            .transpose()?,
        aborted_at: proto
            .aborted_at_unix_nanos
            .map(datetime_from_unix_nanos)
            .transpose()?,
    })
}

pub(super) fn part_to_proto(part: &MultipartUploadPart) -> Result<MultipartPartProto> {
    Ok(MultipartPartProto {
        schema: MULTIPART_PART_SCHEMA.to_string(),
        id: part.id,
        upload_id: part.upload_id,
        part_number: part.part_number,
        content_hash: part.content_hash.clone(),
        object_ref: Some(object_ref_to_proto(&part.object_ref)),
        size: part.size,
        etag: part.etag.clone(),
        created_at_unix_nanos: datetime_to_unix_nanos(&part.created_at)?,
    })
}

pub(super) fn part_from_proto(proto: MultipartPartProto) -> Result<MultipartUploadPart> {
    if proto.schema != MULTIPART_PART_SCHEMA {
        anyhow::bail!("multipart part current state has invalid schema");
    }
    Ok(MultipartUploadPart {
        id: proto.id,
        upload_id: proto.upload_id,
        part_number: proto.part_number,
        content_hash: proto.content_hash,
        object_ref: object_ref_from_proto(
            proto
                .object_ref
                .ok_or_else(|| anyhow!("multipart part current state missing object ref"))?,
        )?,
        size: proto.size,
        etag: proto.etag,
        created_at: datetime_from_unix_nanos(proto.created_at_unix_nanos)?,
    })
}

pub(super) fn object_ref_to_proto(value: &CoreObjectRef) -> CoreObjectRefProto {
    CoreObjectRefProto {
        hash: value.hash.clone(),
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref.clone(),
        encoding: Some(CoreObjectEncodingProto {
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
            compression: Some(object_compression_to_proto(&value.encoding.compression)),
            encryption: value.encoding.encryption.clone(),
        }),
        placements: value
            .placements
            .iter()
            .map(|placement| CoreObjectPlacementProto {
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

pub(super) fn object_ref_from_proto(proto: CoreObjectRefProto) -> Result<CoreObjectRef> {
    let encoding = proto
        .encoding
        .ok_or_else(|| anyhow!("multipart object ref missing encoding"))?;
    Ok(CoreObjectRef {
        hash: proto.hash,
        logical_size: proto.logical_size,
        manifest_ref: proto.manifest_ref,
        encoding: CoreObjectEncoding {
            block_id: encoding.block_id,
            profile_id: encoding.profile_id,
            data_shards: u16::try_from(encoding.data_shards)
                .map_err(|_| anyhow!("multipart object ref data shard count overflows u16"))?,
            parity_shards: u16::try_from(encoding.parity_shards)
                .map_err(|_| anyhow!("multipart object ref parity shard count overflows u16"))?,
            minimum_read_shards: u16::try_from(encoding.minimum_read_shards).map_err(|_| {
                anyhow!("multipart object ref minimum read shard count overflows u16")
            })?,
            minimum_write_ack_shards: u16::try_from(encoding.minimum_write_ack_shards).map_err(
                |_| anyhow!("multipart object ref minimum write ack shard count overflows u16"),
            )?,
            stripe_size: encoding.stripe_size,
            placement_scope: encoding.placement_scope,
            repair_priority: encoding.repair_priority,
            stored_hash: encoding.stored_hash,
            compression: object_compression_from_proto(
                encoding.compression.ok_or_else(|| {
                    anyhow!("multipart object ref missing compression descriptor")
                })?,
            ),
            encryption: encoding.encryption,
        },
        placements: proto
            .placements
            .into_iter()
            .map(|placement| {
                Ok(CoreObjectPlacement {
                    shard_index: u16::try_from(placement.shard_index).map_err(|_| {
                        anyhow!("multipart object ref placement shard index overflows u16")
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

pub(super) fn object_compression_to_proto(
    value: &CoreCompressionDescriptor,
) -> CoreObjectCompressionProto {
    CoreObjectCompressionProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

pub(super) fn object_compression_from_proto(
    value: CoreObjectCompressionProto,
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

pub(super) fn datetime_to_unix_nanos(value: &DateTime<Utc>) -> Result<i64> {
    value
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("multipart timestamp is outside supported range"))
}

pub(super) fn datetime_from_unix_nanos(value: i64) -> Result<DateTime<Utc>> {
    let secs = value.div_euclid(1_000_000_000);
    let sub_nanos = value.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, sub_nanos)
        .ok_or_else(|| anyhow!("multipart timestamp is outside supported range"))
}

pub(super) fn multipart_all_upload_rows_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("upload"),
    ])
}

pub(super) fn multipart_upload_rows_prefix(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("upload"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

pub(super) fn multipart_upload_row_key(
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("upload"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::I64(upload_row_id),
    ])
}

pub(super) fn multipart_part_rows_prefix(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("part"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

pub(super) fn multipart_part_row_key(
    tenant_id: i64,
    bucket_id: i64,
    upload_row_id: i64,
    part_number: i32,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MULTIPART_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::Utf8("part"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::I64(upload_row_id),
        CoreMetaTuplePart::I64(i64::from(part_number)),
    ])
}

pub(super) fn multipart_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

pub(super) fn multipart_current_root_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/multipart/current")
}

pub(super) fn next_upload_id(state: &MultipartState) -> Result<i64> {
    state
        .uploads
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart upload id overflow"))
}

pub(super) fn next_part_id(state: &MultipartState) -> Result<i64> {
    state
        .parts
        .values()
        .map(|part| part.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart part id overflow"))
}

pub fn multipart_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/multipart").as_bytes())
}

pub(super) fn multipart_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("multipart_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

pub(super) fn multipart_metadata_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:multipart_metadata:{tenant_id}:{bucket_id}")
}
