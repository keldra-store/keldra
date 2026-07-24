use super::*;
use crate::core_store::{CoreObjectRef, GetBlob};
use base64::Engine;

const METADATA_JOURNAL_PAGE_ROWS: usize = 512;

#[derive(Debug)]
pub(super) struct MetadataJournalRecordPage {
    pub records: Vec<ObjectMetadataRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

pub(super) fn version_sorts_after_marker(
    order: usize,
    body: &ObjectVersionBody,
    marker_order: usize,
    marker_body: &ObjectVersionBody,
) -> Result<bool> {
    let created_at = parse_body_timestamp(&body.created_at)?;
    let marker_created_at = parse_body_timestamp(&marker_body.created_at)?;
    Ok(created_at < marker_created_at || (created_at == marker_created_at && order < marker_order))
}

pub(super) async fn stage_segment_file(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    family: FileFamily,
    header: SegmentHeader,
    records: &[SegmentRecord],
    created_at_unix_nanos: u64,
) -> Result<WrittenSegment> {
    let body = encode_object_segment_body_table(family, records)?;
    let (first_record_hash, last_record_hash) = segment_record_hash_bounds(records);
    let body_hash = hash32(&body);
    let stable_name = format!(
        "object-metadata:{}:{}:{}:{}",
        bucket.tenant_id,
        bucket.id,
        file_family_name(family),
        generation
    );
    let range_index = single_body_range_index(
        body.len(),
        records.len() as u64,
        first_record_hash,
        last_record_hash,
    )?;
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::ObjectBlob,
        generation,
        &stable_name,
        &body_hash,
    );
    let header_proto = encode_segment_header_proto(family, &logical_file_id, &header);
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: family,
        writer_family: WriterFamily::ObjectBlob,
        writer_generation: generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: records.len() as u64,
        first_record_hash,
        last_record_hash,
        boundary_values: Vec::new(),
        mutation_id: format!(
            "metadata-segment:{}:{}:{}",
            bucket.tenant_id, bucket.id, generation
        ),
        region_id: "local".to_string(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
    })?;
    let file_hash = hex::encode(built_segment.encoded.segment_hash);
    let ref_name = metadata_segment_ref_name(bucket, generation, family, &file_hash)?;

    let store = CoreStore::new(storage.clone()).await?;
    let receipt = store
        .write_format_build_output(WriterBuildOutput {
            logical_files: vec![built_segment.logical_file],
            core_meta_mutations: Vec::new(),
            core_meta_root_publications: Vec::new(),
        })
        .await?;
    let object_ref = receipt
        .written_object_refs
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no object metadata object"))?;
    let catalog_record = WriterSegmentCatalogRecord {
        family: OBJECT_METADATA_SEGMENT_CATALOG_FAMILY.to_string(),
        scope: object_metadata_segment_scope(&ref_name)?,
        segment_ref: ref_name.clone(),
        core_object_ref_target: encode_core_object_ref_target(&object_ref)?,
        segment_hash: file_hash.clone(),
        segment_length: object_ref.logical_size,
        generation,
        source_cursor: generation,
        created_at_unix_nanos,
    };
    Ok(WrittenSegment {
        family,
        ref_name,
        record_count: records.len() as u64,
        file_hash,
        catalog_record,
    })
}

pub(super) async fn publish_segment_catalog(
    storage: &Storage,
    segment: &WrittenSegment,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    write_writer_segment_catalog_record(storage, &segment.catalog_record, additional_preconditions)
        .await
}

pub(super) fn encode_object_segment_body_table(
    family: FileFamily,
    records: &[SegmentRecord],
) -> Result<Vec<u8>> {
    let table_id = object_segment_table_id(family)?;
    let rows = records
        .iter()
        .map(|record| crate::formats::table::TableRow {
            key: record.key.clone(),
            value: record.value.clone(),
        })
        .collect::<Vec<_>>();
    crate::formats::table::encode_writer_body_tables(&object_segment_tables(table_id, rows))
        .map_err(anyhow::Error::from)
}

pub(super) fn decode_object_segment_body_table(body: &[u8]) -> Result<Vec<SegmentRecord>> {
    Ok(crate::formats::table::decode_writer_body_tables(body)?
        .into_iter()
        .flat_map(|table| table.rows)
        .map(|row| SegmentRecord::new(row.key, row.value))
        .collect::<Vec<_>>())
}

fn object_segment_table_id(family: FileFamily) -> Result<u16> {
    match family {
        FileFamily::MetadataSegment => Ok(0x0101),
        FileFamily::DirectorySegment => Ok(0x0103),
        _ => Err(anyhow!("unsupported object segment table family")),
    }
}

fn object_segment_tables(
    active_table_id: u16,
    active_rows: Vec<crate::formats::table::TableRow>,
) -> Vec<crate::formats::table::WriterBodyTable> {
    [0x0101, 0x0102, 0x0103]
        .into_iter()
        .map(|table_id| crate::formats::table::WriterBodyTable {
            table_id,
            row_type_id: table_id,
            rows: if table_id == active_table_id {
                active_rows.clone()
            } else {
                Vec::new()
            },
        })
        .collect()
}

pub(super) async fn stage_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    records: &[ObjectMetadataRecord],
    segments: &[WrittenSegment],
    manifest_signing_key: &[u8],
    fence_token: u64,
    published_at: String,
) -> Result<StagedPartitionManifest> {
    if manifest_signing_key.is_empty() {
        return Err(anyhow!("partition manifest signing key must not be empty"));
    }
    let last_record_hash = records
        .last()
        .map(|record| record.event_hash.clone())
        .ok_or_else(|| anyhow!("partition manifest requires at least one stream record"))?;
    let journal_ref = ManifestJournalRef {
        path: format!(
            "corestream:{}",
            object_metadata_stream_id(bucket.tenant_id, bucket.id)
        ),
        first_sequence: records
            .first()
            .map(|record| record.partition_sequence)
            .unwrap_or(0),
        last_sequence: generation,
        last_record_hash: last_record_hash.clone(),
    };
    let segment_refs = segments
        .iter()
        .map(|segment| {
            Ok(ManifestSegmentRef {
                family: file_family_name(segment.family).to_string(),
                path: format!("{MANIFEST_SEGMENT_REF_PREFIX}{}", segment.ref_name),
                generation,
                record_count: segment.record_count,
                file_hash: segment.file_hash.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut manifest = PartitionManifest {
        format_version: 1,
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        generation,
        fence_token,
        sealed_journals: vec![journal_ref],
        active_journal: None,
        segments: segment_refs,
        compacted_through_sequence: generation,
        last_record_hash,
        published_at,
        manifest_hash: None,
        manifest_signature: None,
    };
    let manifest_hash = compute_manifest_hash(&manifest)?;
    let manifest_signature = sign_manifest(&manifest_hash, &manifest, manifest_signing_key)?;
    manifest.manifest_hash = Some(manifest_hash.clone());
    manifest.manifest_signature = Some(manifest_signature);
    let encoded = encode_partition_manifest(&manifest)?;
    let manifest_ref = metadata_manifest_ref_name(bucket)?;
    let manifest_bytes_hash = hash32(&encoded);
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::ObjectBlob,
        generation,
        &manifest_ref,
        &manifest_bytes_hash,
    );
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: WriterFamily::ObjectBlob.as_str().to_string(),
            generation,
            logical_file_id,
            source: encoded,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "metadata-manifest:{}:{}:{}",
                bucket.tenant_id, bucket.id, generation
            ),
            region_id: "local".to_string(),
        })
        .await?;
    let manifest_target = encode_core_object_ref_target(&object_ref)?;
    let manifest_row = ObjectMetadataPartitionManifestRow {
        manifest_ref: manifest_ref.clone(),
        object_ref_target: manifest_target,
        manifest_hash: hex::encode(manifest_bytes_hash),
        generation,
        published_at: manifest.published_at.clone(),
    };
    let manifest_payload = encode_object_metadata_partition_manifest_row(bucket, &manifest_row)?;
    let manifest_root_anchor_key = format!(
        "object-metadata-manifest/{}/{}",
        bucket.tenant_id, bucket.id
    );
    Ok(StagedPartitionManifest {
        manifest,
        manifest_ref,
        manifest_payload,
        manifest_tuple_key: object_metadata_partition_manifest_row_key(bucket)?,
        manifest_root_anchor_key,
        transaction_id: format!(
            "metadata-manifest:{}:{}:{}:{}",
            bucket.tenant_id, bucket.id, generation, manifest_hash
        ),
    })
}

pub(super) async fn publish_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    staged: &StagedPartitionManifest,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let current = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
        &staged.manifest_tuple_key,
    )?;
    if let Some(current) = current.as_ref()
        && decode_object_metadata_partition_manifest_row(bucket, current)?
            == decode_object_metadata_partition_manifest_row(bucket, &staged.manifest_payload)?
    {
        return Ok(());
    }
    let mut preconditions = Vec::with_capacity(additional_preconditions.len() + 1);
    preconditions.push(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_OBJECT_HEADS.to_string(),
        table_id: TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
        tuple_key: staged.manifest_tuple_key.clone(),
        expected_payload_hash: current.as_ref().map(|payload| {
            core_meta_payload_digest(TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW, payload)
        }),
        require_absent: current.is_none(),
        require_present: current.is_some(),
    });
    preconditions.extend_from_slice(additional_preconditions);
    let transaction_id =
        core_mutation_publication_attempt_id(&staged.transaction_id, &preconditions)?;
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: staged.manifest.partition_id.clone(),
            committed_by_principal: object_metadata_partition_principal(bucket),
            root_publications: object_metadata_manifest_root_publications(
                &staged.manifest.partition_id,
                &staged.manifest_root_anchor_key,
            ),
            preconditions,
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: staged.manifest.partition_id.clone(),
                cf: CF_OBJECT_HEADS.to_string(),
                table_id: TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
                tuple_key: staged.manifest_tuple_key.clone(),
                payload: staged.manifest_payload.clone(),
            }],
        })
        .await?;
    if receipt.state != CoreTransactionState::Committed {
        return Err(anyhow!(
            "object metadata manifest publication {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        ));
    }
    Ok(())
}

fn object_metadata_manifest_root_publications(
    coordinator_root: &str,
    data_root: &str,
) -> Vec<crate::core_store::CoreMutationRootPublication> {
    if coordinator_root == data_root {
        return vec![crate::core_store::CoreMutationRootPublication {
            root_anchor_key: data_root.to_string(),
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::ObjectBlob.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }];
    }
    vec![
        crate::core_store::CoreMutationRootPublication::new(
            coordinator_root,
            WriterFamily::CoreControl.as_str(),
        )
        .coordinator(),
        crate::core_store::CoreMutationRootPublication::new(
            data_root,
            WriterFamily::ObjectBlob.as_str(),
        ),
    ]
}

pub fn decode_partition_manifest(
    input: &[u8],
    manifest_signing_key: &[u8],
) -> Result<PartitionManifest> {
    let proto = PartitionManifestProto::decode(input)?;
    ensure_deterministic_proto(&proto, input, "partition manifest")?;
    let manifest = partition_manifest_from_proto(proto)?;
    verify_partition_manifest(&manifest, manifest_signing_key)?;
    Ok(manifest)
}

pub(crate) async fn read_latest_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Option<PartitionManifest>> {
    let Some(record) = read_object_metadata_partition_manifest_row(storage, bucket).await? else {
        return Ok(None);
    };
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.object_ref_target)?,
        })
        .await?;
    Ok(Some(decode_partition_manifest(
        &bytes,
        manifest_signing_key,
    )?))
}

pub(super) async fn partition_manifest_exists(storage: &Storage, bucket: &Bucket) -> Result<bool> {
    Ok(read_object_metadata_partition_manifest_row(storage, bucket)
        .await?
        .is_some())
}

pub(super) async fn read_manifest_segment(
    storage: &Storage,
    segment: &ManifestSegmentRef,
) -> Result<Vec<u8>> {
    let ref_name = segment
        .path
        .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
        .ok_or_else(|| anyhow!("partition segment manifest entry is not a CoreMeta segment ref"))?;
    let record = read_writer_segment_catalog_record(
        storage,
        OBJECT_METADATA_SEGMENT_CATALOG_FAMILY,
        &object_metadata_segment_scope(ref_name)?,
        object_metadata_segment_generation(ref_name)?,
        ref_name,
    )
    .await?
    .ok_or_else(|| anyhow!("partition segment catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.core_object_ref_target)?,
        })
        .await
}

#[cfg(test)]
pub(super) async fn read_core_ref_uri_payload(storage: &Storage, ref_uri: &str) -> Result<Vec<u8>> {
    let ref_name = ref_uri
        .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
        .unwrap_or(ref_uri);
    let record = read_writer_segment_catalog_record(
        storage,
        OBJECT_METADATA_SEGMENT_CATALOG_FAMILY,
        &object_metadata_segment_scope(ref_name)?,
        object_metadata_segment_generation(ref_name)?,
        ref_name,
    )
    .await?
    .ok_or_else(|| anyhow!("CoreStore writer segment catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.core_object_ref_target)?,
        })
        .await
}

pub fn verify_partition_manifest(
    manifest: &PartitionManifest,
    manifest_signing_key: &[u8],
) -> Result<()> {
    let expected_hash = compute_manifest_hash(manifest)?;
    if manifest.manifest_hash.as_deref() != Some(expected_hash.as_str()) {
        return Err(anyhow!("partition manifest hash mismatch"));
    }
    let expected_signature = sign_manifest(&expected_hash, manifest, manifest_signing_key)?;
    if manifest.manifest_signature.as_deref() != Some(expected_signature.as_str()) {
        return Err(anyhow!("partition manifest signature mismatch"));
    }
    Ok(())
}

pub(super) fn compute_manifest_hash(manifest: &PartitionManifest) -> Result<String> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_hash = None;
    unsigned.manifest_signature = None;
    Ok(hex::encode(hash32(&encode_partition_manifest(&unsigned)?)))
}

pub(super) fn sign_manifest(
    manifest_hash: &str,
    manifest: &PartitionManifest,
    manifest_signing_key: &[u8],
) -> Result<String> {
    if manifest_signing_key.is_empty() {
        return Err(anyhow!("partition manifest signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(manifest_signing_key)?;
    mac.update(manifest_hash.as_bytes());
    mac.update(b"\0");
    mac.update(manifest.partition_id.as_bytes());
    mac.update(b"\0");
    mac.update(&manifest.generation.to_le_bytes());
    mac.update(&manifest.fence_token.to_le_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub(super) fn file_family_name(family: FileFamily) -> &'static str {
    match family {
        FileFamily::MetadataSegment => "metadata_segment",
        FileFamily::DirectorySegment => "directory_segment",
        FileFamily::FullTextSegment => "full_text_segment",
        FileFamily::VectorSegment => "vector_segment",
        FileFamily::AuthzTupleSegment => "authz_tuple_segment",
        FileFamily::WatchSegment => "watch_segment",
        FileFamily::PersonalDbLogSegment => "personaldb_log_segment",
        FileFamily::PersonalDbRowIndex => "personaldb_row_index",
        FileFamily::GitSourceIndex => "git_source_index",
        FileFamily::TypedFieldSegment => "typed_field_segment",
        FileFamily::RegistrySegment => "registry_segment",
        FileFamily::MeshControlSegment => "mesh_control_segment",
    }
}

pub(super) fn file_family_from_manifest_name(name: &str) -> Result<FileFamily> {
    match name {
        "metadata_segment" => Ok(FileFamily::MetadataSegment),
        "directory_segment" => Ok(FileFamily::DirectorySegment),
        other => Err(anyhow!(
            "unsupported segment family in partition manifest: {other}"
        )),
    }
}

pub(super) fn object_from_body(body: &ObjectVersionBody) -> Result<Object> {
    Ok(Object {
        id: body.id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        key: body.object_key.clone(),
        kind: body.kind,
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        version_id: uuid::Uuid::parse_str(&body.version_id)?,
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        record_hash: body.record_hash.clone(),
        created_at: parse_body_timestamp(&body.created_at)?,
        deleted_at: body
            .deleted_at
            .as_deref()
            .map(parse_body_timestamp)
            .transpose()?,
        storage_class: body.storage_class.clone(),
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: body.checksum.clone(),
        link: body.link.clone(),
    })
}

pub(super) fn parse_body_timestamp(value: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc))
}

pub(super) fn segment_header(
    bucket: &Bucket,
    generation: u64,
    partition_family: &'static str,
    key_order: &'static str,
) -> SegmentHeader {
    SegmentHeader {
        tenant_id: bucket.tenant_id.to_string(),
        bucket_id: bucket.id.to_string(),
        partition_family,
        partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        generation,
        key_order,
        compression: "none",
        block_size_uncompressed: 64 * 1024,
        bloom_bits_per_key: 0,
    }
}

fn encode_segment_header_proto(
    family: FileFamily,
    logical_file_id: &str,
    header: &SegmentHeader,
) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.object_metadata.segment_header.v1",
        logical_file_id,
        family,
        header.generation,
        None,
        None,
        0,
        vec![
            header_field_string("tenant_id", header.tenant_id.clone()),
            header_field_string("bucket_id", header.bucket_id.clone()),
            header_field_string("partition_family", header.partition_family),
            header_field_string("partition_id", header.partition_id.clone()),
            header_field_string("key_order", header.key_order),
            header_field_string("compression", header.compression),
            header_field_u64(
                "block_size_uncompressed",
                u64::from(header.block_size_uncompressed),
            ),
            header_field_u64("bloom_bits_per_key", u64::from(header.bloom_bits_per_key)),
        ],
    )
}

pub(super) fn segment_record_hash_bounds(records: &[SegmentRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

pub(super) fn metadata_segment_key(body: &ObjectVersionBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/object/{}/version/{}",
        body.tenant_id, body.bucket_id, body.object_key, body.version_id
    )
    .into_bytes()
}

pub(super) fn directory_segment_key(body: &DirectoryEntryBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/directory/{}",
        body.tenant_id, body.bucket_id, body.object_key
    )
    .into_bytes()
}

pub(super) fn metadata_segment_ref_name(
    bucket: &Bucket,
    generation: u64,
    family: FileFamily,
    file_hash: &str,
) -> Result<String> {
    validate_hex32(file_hash, "metadata segment file hash")?;
    let prefix = match family {
        FileFamily::MetadataSegment => METADATA_SEGMENT_REF_PREFIX,
        FileFamily::DirectorySegment => DIRECTORY_SEGMENT_REF_PREFIX,
        _ => return Err(anyhow!("unsupported object metadata segment family")),
    };
    Ok(format!(
        "{prefix}tenant:{}:bucket:{}:generation:{generation:020}:hash:{file_hash}",
        bucket.tenant_id, bucket.id
    ))
}

pub(super) fn metadata_manifest_ref_name(bucket: &Bucket) -> Result<String> {
    Ok(format!(
        "{METADATA_MANIFEST_REF_PREFIX}tenant:{}:bucket:{}",
        bucket.tenant_id, bucket.id
    ))
}

pub(super) fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

pub(super) fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

pub(super) fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}

pub async fn active_object_journal_stats(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<ActiveObjectJournalStats> {
    let mut compacted_through_sequence = 0;
    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        compacted_through_sequence = manifest.compacted_through_sequence;
    }

    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let core_store = CoreStore::new(storage.clone()).await?;
    let last_sequence = core_store
        .visible_stream_head_metadata(&stream_id)
        .await?
        .map(|metadata| metadata.sequence)
        .unwrap_or(0);
    let mut stats = ActiveObjectJournalStats {
        last_sequence,
        compacted_through_sequence,
        ..ActiveObjectJournalStats::default()
    };
    let mut tombstone_debt = 0_u64;
    let mut after_sequence = compacted_through_sequence;
    while after_sequence < last_sequence {
        // The published head bounds this internal accounting scan; payload bytes are unnecessary.
        let page = core_store.raw_stream_record_metadata_range(
            &stream_id,
            after_sequence,
            last_sequence,
            METADATA_JOURNAL_PAGE_ROWS,
        )?;
        if page.is_empty() {
            break;
        }
        for record in &page {
            stats.uncompacted_frame_count = stats.uncompacted_frame_count.saturating_add(1);
            stats.uncompacted_encoded_bytes = stats
                .uncompacted_encoded_bytes
                .saturating_add(record.payload_len);
            if record.record_kind == DELETE_MARKER_RECORD_KIND {
                tombstone_debt = tombstone_debt.saturating_add(1);
            }
        }
        after_sequence = page
            .last()
            .ok_or_else(|| anyhow!("metadata journal page unexpectedly empty"))?
            .sequence;
    }
    crate::perf::record_tombstone_debt("object_blob", tombstone_debt);
    Ok(stats)
}

pub async fn object_metadata_source_cursor(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<u128> {
    let compacted_through_sequence = if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        manifest.compacted_through_sequence
    } else {
        0
    };
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let core_store = CoreStore::new(storage.clone()).await?;
    let active_sequence = core_store
        .visible_stream_head_metadata(&stream_id)
        .await?
        .map(|metadata| metadata.sequence)
        .unwrap_or(0);
    Ok(u128::from(active_sequence.max(compacted_through_sequence)))
}

pub async fn object_metadata_source_checkpoint_hash(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<String> {
    let max_sequence = u64::try_from(max_sequence)
        .map_err(|_| anyhow!("object metadata source cursor exceeds u64 sequence range"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.object_metadata.source_checkpoint.v2");
    hasher.update(&bucket.tenant_id.to_le_bytes());
    hasher.update(&bucket.id.to_le_bytes());
    hasher.update(&max_sequence.to_le_bytes());

    let mut compacted_through_sequence = 0u64;
    let mut compacted_event_hash = String::new();
    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        compacted_through_sequence = manifest.compacted_through_sequence;
        if compacted_through_sequence > max_sequence {
            return Err(anyhow!(
                "object metadata source cursor is older than manifest checkpoint"
            ));
        }
        hasher.update(manifest.manifest_hash.as_deref().unwrap_or("").as_bytes());
        compacted_event_hash = manifest.last_record_hash;
    } else {
        hasher.update(&[0; 32]);
    }
    hasher.update(&compacted_through_sequence.to_le_bytes());

    let checkpoint_event_hash = if max_sequence == compacted_through_sequence {
        compacted_event_hash
    } else if max_sequence > compacted_through_sequence {
        let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
        let core_store = CoreStore::new(storage.clone()).await?;
        core_store
            .visible_stream_record_metadata(&stream_id, max_sequence)
            .await?
            .ok_or_else(|| anyhow!("object metadata source cursor is not visible"))?
            .event_hash
    } else {
        String::new()
    };
    hasher.update(checkpoint_event_hash.as_bytes());

    Ok(hasher.finalize().to_hex().to_string())
}

pub(super) async fn read_manifest_journal_ref_records(
    storage: &Storage,
    journal_ref: &ManifestJournalRef,
) -> Result<Vec<ObjectMetadataRecord>> {
    let stream_id = journal_ref
        .path
        .strip_prefix("corestream:")
        .ok_or_else(|| anyhow!("object metadata manifest journal ref must use corestream:"))?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut records = Vec::new();
    let mut after_sequence = journal_ref.first_sequence.saturating_sub(1);
    while after_sequence < journal_ref.last_sequence {
        let page = read_metadata_journal_records_page(
            &core_store,
            stream_id,
            after_sequence,
            METADATA_JOURNAL_PAGE_ROWS,
        )
        .await?;
        for record in page.records {
            if record.partition_sequence > journal_ref.last_sequence {
                break;
            }
            records.push(record);
        }
        if !page.has_more
            || page.next_sequence >= journal_ref.last_sequence
            || page.next_sequence <= after_sequence
        {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(records)
}

pub(super) async fn read_metadata_journal_records_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<ObjectMetadataRecord>> {
    let mut decoded = Vec::new();
    let mut after_sequence = 0;
    loop {
        let page = read_metadata_journal_records_page(
            core_store,
            stream_id,
            after_sequence,
            METADATA_JOURNAL_PAGE_ROWS,
        )
        .await?;
        decoded.extend(page.records);
        if !page.has_more || page.next_sequence <= after_sequence {
            break;
        }
        after_sequence = page.next_sequence;
    }
    Ok(decoded)
}

pub(super) async fn read_metadata_journal_records_page(
    core_store: &CoreStore,
    stream_id: &str,
    after_sequence: u64,
    limit: usize,
) -> Result<MetadataJournalRecordPage> {
    if !(1..=4_096).contains(&limit) {
        return Err(anyhow!(
            "object metadata journal page size must be between 1 and 4096"
        ));
    }
    let page = core_store
        .read_stream_page(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence,
            limit,
        })
        .await?;
    let mut decoded = Vec::with_capacity(page.records.len());
    for record in page.records {
        if record.record_kind.starts_with("object_metadata.") {
            decoded.push(metadata_record_from_stream_record(record)?);
        }
    }
    Ok(MetadataJournalRecordPage {
        records: decoded,
        next_sequence: page.next_sequence,
        has_more: page.has_more,
    })
}

pub(super) fn metadata_record_from_stream_record(
    record: crate::core_store::StreamRecord,
) -> Result<ObjectMetadataRecord> {
    let record_kind = ObjectMetadataRecordKind::from_str(&record.record_kind)?;
    let body = decode_object_metadata_body_proto(&record.payload)?;
    Ok(ObjectMetadataRecord {
        partition_sequence: record.sequence,
        event_hash: record.event_hash,
        record_kind,
        payload: record.payload,
        body,
    })
}

pub(super) fn object_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

pub(super) fn object_metadata_partition_principal(bucket: &Bucket) -> String {
    format!(
        "partition-owner:object_metadata:{}:{}",
        bucket.tenant_id, bucket.id
    )
}

pub(super) fn object_metadata_manifest_scope(bucket: &Bucket) -> String {
    format!("tenant/{}/bucket/{}", bucket.tenant_id, bucket.id)
}

pub(super) fn object_metadata_segment_scope(segment_ref: &str) -> Result<String> {
    let parsed = parse_object_metadata_segment_ref(segment_ref)?;
    Ok(format!(
        "tenant/{}/bucket/{}/family/{}",
        parsed.tenant_id, parsed.bucket_id, parsed.family
    ))
}

pub(super) fn object_metadata_segment_generation(segment_ref: &str) -> Result<u64> {
    Ok(parse_object_metadata_segment_ref(segment_ref)?.generation)
}

struct ParsedObjectMetadataSegmentRef<'a> {
    family: &'a str,
    tenant_id: i64,
    bucket_id: i64,
    generation: u64,
}

fn parse_object_metadata_segment_ref(
    segment_ref: &str,
) -> Result<ParsedObjectMetadataSegmentRef<'_>> {
    let parts = segment_ref.split(':').collect::<Vec<_>>();
    if parts.len() != 9
        || !matches!(parts[0], "metadata_segment" | "directory_segment")
        || parts[1] != "tenant"
        || parts[3] != "bucket"
        || parts[5] != "generation"
        || parts[7] != "hash"
    {
        return Err(anyhow!("object metadata segment ref is malformed"));
    }
    let tenant_id = parts[2]
        .parse::<i64>()
        .map_err(|_| anyhow!("object metadata segment tenant id is invalid"))?;
    let bucket_id = parts[4]
        .parse::<i64>()
        .map_err(|_| anyhow!("object metadata segment bucket id is invalid"))?;
    let generation = parts[6]
        .parse::<u64>()
        .map_err(|_| anyhow!("object metadata segment generation is invalid"))?;
    if tenant_id < 0 || bucket_id < 0 || generation == 0 {
        return Err(anyhow!("object metadata segment scope is invalid"));
    }
    validate_hex32(parts[8], "object metadata segment hash")?;
    Ok(ParsedObjectMetadataSegmentRef {
        family: parts[0],
        tenant_id,
        bucket_id,
        generation,
    })
}

pub(super) fn object_metadata_partition_manifest_row_key(bucket: &Bucket) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("object-metadata-manifest"),
        CoreMetaTuplePart::I64(bucket.tenant_id),
        CoreMetaTuplePart::I64(bucket.id),
    ])
}

pub(super) fn encode_object_metadata_partition_manifest_row(
    bucket: &Bucket,
    row: &ObjectMetadataPartitionManifestRow,
) -> Result<Vec<u8>> {
    if row.manifest_ref != metadata_manifest_ref_name(bucket)? {
        return Err(anyhow!("object metadata manifest row ref scope mismatch"));
    }
    validate_hex32(&row.manifest_hash, "object metadata manifest payload hash")?;
    encode_deterministic_proto(&ObjectMetadataPartitionManifestRowProto {
        common: Some(core_meta_committed_row_common(
            object_metadata_manifest_scope(bucket),
            core_meta_root_key_hash(&format!(
                "object-metadata-manifest/{}/{}",
                bucket.tenant_id, bucket.id
            )),
            row.generation,
            format!(
                "object-metadata-manifest:{}:{}:{}",
                bucket.tenant_id, bucket.id, row.generation
            ),
            unix_nanos_from_rfc3339(&row.published_at),
        )),
        schema: OBJECT_METADATA_PARTITION_MANIFEST_ROW_SCHEMA.to_string(),
        manifest_ref: row.manifest_ref.clone(),
        object_ref_target: row.object_ref_target.clone(),
        manifest_hash: row.manifest_hash.clone(),
        generation: row.generation,
        published_at: row.published_at.clone(),
    })
}

pub(super) fn decode_object_metadata_partition_manifest_row(
    bucket: &Bucket,
    bytes: &[u8],
) -> Result<ObjectMetadataPartitionManifestRow> {
    let proto = decode_deterministic_proto::<ObjectMetadataPartitionManifestRowProto>(
        bytes,
        "object metadata partition manifest row",
    )?;
    if proto.schema != OBJECT_METADATA_PARTITION_MANIFEST_ROW_SCHEMA {
        return Err(anyhow!(
            "object metadata partition manifest row has invalid schema"
        ));
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("object metadata partition manifest row missing CoreMeta common"))?;
    if common.realm_id != object_metadata_manifest_scope(bucket) {
        return Err(anyhow!(
            "object metadata partition manifest row realm mismatch"
        ));
    }
    if proto.manifest_ref != metadata_manifest_ref_name(bucket)? {
        return Err(anyhow!(
            "object metadata partition manifest row ref scope mismatch"
        ));
    }
    validate_hex32(
        &proto.manifest_hash,
        "object metadata manifest payload hash",
    )?;
    Ok(ObjectMetadataPartitionManifestRow {
        manifest_ref: proto.manifest_ref,
        object_ref_target: proto.object_ref_target,
        manifest_hash: proto.manifest_hash,
        generation: proto.generation,
        published_at: proto.published_at,
    })
}

pub(super) async fn read_object_metadata_partition_manifest_row(
    storage: &Storage,
    bucket: &Bucket,
) -> Result<Option<ObjectMetadataPartitionManifestRow>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(
        CF_OBJECT_HEADS,
        TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
        &object_metadata_partition_manifest_row_key(bucket)?,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_object_metadata_partition_manifest_row(
        bucket, &payload,
    )?))
}

#[cfg(test)]
pub(crate) async fn read_object_metadata_record_fences_for_test(
    storage: &Storage,
    bucket: &Bucket,
) -> Result<Vec<u64>> {
    Ok(read_all_metadata_journal_records(storage, bucket)
        .await?
        .into_iter()
        .map(|record| record.body.fence_token)
        .collect())
}

pub fn object_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&tenant_id.to_le_bytes());
    bytes.extend_from_slice(&bucket_id.to_le_bytes());
    hash32(&bytes)
}

pub(super) fn require_object_metadata_permit(
    bucket: &Bucket,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if permit.partition_family != "object_metadata" || permit.partition_id != expected_partition_id
    {
        return Err(anyhow!(
            "partition write permit does not target this object metadata partition"
        ));
    }
    Ok(())
}

pub(super) fn segment_payload_bytes(records: &[SegmentRecord]) -> u64 {
    records.iter().fold(0_u64, |total, record| {
        total
            .saturating_add(record.key.len() as u64)
            .saturating_add(record.value.len() as u64)
            .saturating_add(record.value_hash.len() as u64)
    })
}
