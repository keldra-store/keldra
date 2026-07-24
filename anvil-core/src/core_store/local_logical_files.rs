use super::*;
use crate::formats::writer::WriterFamily;

pub(super) fn logical_file_manifest_from_object_manifests(
    request: &WriteLogicalFileRequest,
    blocks: &[MaterializedLogicalBlock],
    plaintext_hash: String,
    plaintext_len: u64,
) -> Result<CoreLogicalFileManifest> {
    if blocks.is_empty() {
        bail!("CoreStore logical file manifest must contain at least one materialised block");
    }
    for block in blocks {
        validate_manifest_for_object_ref(
            &block.object_manifest,
            &object_ref_from_object_manifest(&block.object_manifest)?,
            strip_sha256_prefix(&block.object_manifest.object_hash)?,
        )?;
    }

    let first_manifest = &blocks[0].object_manifest;
    let data_shards = u32::from(first_manifest.encoding.data_shards);
    let parity_shards = u32::from(first_manifest.encoding.parity_shards);
    let profile = local_erasure_profile_for_counts(
        &first_manifest.encoding.profile_id,
        data_shards as usize,
        parity_shards as usize,
    )?;
    if blocks.iter().any(|block| {
        block.object_manifest.encoding.profile_id != first_manifest.encoding.profile_id
            || block.object_manifest.encoding.data_shards != first_manifest.encoding.data_shards
            || block.object_manifest.encoding.parity_shards != first_manifest.encoding.parity_shards
    }) {
        bail!("CoreStore logical file blocks must use one erasure profile");
    }
    let logical_blocks = blocks
        .iter()
        .map(|block| logical_block_ref_from_materialized_block(block, profile))
        .collect::<Result<Vec<_>>>()?;
    let boundary_schema_generation = request
        .boundary_values
        .iter()
        .map(|value| value.schema_generation)
        .max()
        .unwrap_or(0);
    let ranges = if request.range_hints.is_empty() {
        vec![CoreLogicalRange {
            range_id: "full".to_string(),
            byte_start: 0,
            byte_end: plaintext_len,
            writer_record_kind: request.writer_family.clone(),
            boundary_values: request.boundary_values.clone(),
            writer_statistics: Vec::new(),
            block_ids: logical_blocks
                .iter()
                .map(|block| block.block_id.clone())
                .collect(),
            prefetch_next_range_ids: Vec::new(),
            preferred_block_boundary: "writer_defined".to_string(),
            boundary_dimension_ids: Vec::new(),
            shared_range: None,
        }]
    } else {
        request
            .range_hints
            .iter()
            .map(|hint| {
                validate_logical_range_hint(hint)?;
                if hint.byte_start > hint.byte_end {
                    bail!("CoreStore logical range hint start must be <= end");
                }
                if hint.byte_end > plaintext_len {
                    bail!("CoreStore logical range hint exceeds logical file size");
                }
                Ok(CoreLogicalRange {
                    range_id: hint.range_id.clone(),
                    byte_start: hint.byte_start,
                    byte_end: hint.byte_end,
                    writer_record_kind: hint.writer_record_kind.clone(),
                    boundary_values: hint.boundary_values.clone(),
                    writer_statistics: hint.writer_statistics.clone(),
                    block_ids: logical_block_ids_for_range(
                        &logical_blocks,
                        hint.byte_start,
                        hint.byte_end,
                    ),
                    prefetch_next_range_ids: hint.prefetch_next_range_ids.clone(),
                    preferred_block_boundary: hint.preferred_block_boundary.clone(),
                    boundary_dimension_ids: hint.boundary_dimension_ids.clone(),
                    shared_range: hint.shared_range.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?
    };

    let encryption = logical_file_encryption_descriptor(&logical_blocks, &plaintext_hash)?;
    let compression =
        logical_file_compression_descriptor(&logical_blocks, plaintext_len, &plaintext_hash)?;

    Ok(CoreLogicalFileManifest {
        schema: CORE_LOGICAL_FILE_MANIFEST_SCHEMA.to_string(),
        logical_file_id: request.logical_file_id.clone(),
        writer_family: request.writer_family.clone(),
        writer_generation: request.generation,
        logical_size: plaintext_len,
        content_hash: plaintext_hash.clone(),
        boundary_schema_generation,
        ranges,
        blocks: logical_blocks,
        compression,
        encryption,
        erasure_profile_id: first_manifest.encoding.profile_id.clone(),
        placement_epoch: LOCAL_PLACEMENT_EPOCH,
        created_by_mutation_id: request.mutation_id.clone(),
        codec_id: profile.codec_id.to_string(),
        data_shards,
        parity_shards,
    })
}

pub(super) fn logical_block_ref_from_materialized_block(
    block: &MaterializedLogicalBlock,
    profile: LocalErasureProfile,
) -> Result<CoreLogicalBlockRef> {
    let object_manifest = &block.object_manifest;
    let boundary_summary_hash = boundary_summary_hash(&object_manifest.boundary_values)?;
    let boundary_values_b64 = encode_boundary_values_b64(&object_manifest.boundary_values)?;
    let shard_payload_len = object_manifest
        .placements
        .iter()
        .map(|placement| placement.stored_size)
        .max()
        .unwrap_or(0);
    let data_shards = u32::from(object_manifest.encoding.data_shards);
    let parity_shards = u32::from(object_manifest.encoding.parity_shards);
    Ok(CoreLogicalBlockRef {
        block_id: object_manifest.encoding.block_id.clone(),
        logical_offset: block.logical_offset,
        logical_length: block.logical_length,
        compressed_length: block.compressed_length,
        encrypted_length: object_manifest.logical_size,
        content_hash: block.plaintext_hash.clone(),
        compression: block.compression.clone(),
        encryption: block.encryption.clone(),
        erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
        shards: object_manifest
            .placements
            .iter()
            .map(|placement| CoreLogicalShardRef {
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_index: u32::from(placement.shard_index),
                shard_hash: placement.shard_hash.clone(),
                stored_length: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
                boundary_summary_hash: boundary_summary_hash.clone(),
                boundary_values_b64: boundary_values_b64.clone(),
            })
            .collect(),
        codec_id: profile.codec_id.to_string(),
        data_shards,
        parity_shards,
        plaintext_block_len: block.logical_length,
        shard_payload_len,
        padding_len: shard_payload_len
            .saturating_mul(u64::from(data_shards))
            .saturating_sub(object_manifest.logical_size),
        block_encoded_hash: object_manifest.object_hash.clone(),
        boundary_summary_hash,
        boundary_values_b64,
    })
}

pub(super) fn logical_file_compression_descriptor(
    blocks: &[CoreLogicalBlockRef],
    file_plaintext_len: u64,
    file_plaintext_hash: &str,
) -> Result<CoreCompressionDescriptor> {
    let Some(first) = blocks.first() else {
        bail!("CoreStore logical file compression descriptor requires at least one block");
    };
    let algorithm = first.compression.algorithm.clone();
    if blocks
        .iter()
        .any(|block| block.compression.algorithm != algorithm)
    {
        bail!("CoreStore logical file blocks must use one compression algorithm");
    }
    let compressed_length = blocks
        .iter()
        .map(|block| block.compression.compressed_length)
        .sum::<u64>();
    if algorithm == "none" {
        if compressed_length != file_plaintext_len {
            bail!("CoreStore none-compressed logical file blocks do not match logical size");
        }
        return Ok(none_compression_descriptor_from_hash(
            file_plaintext_len,
            file_plaintext_hash,
        ));
    }
    if algorithm != "zstd" {
        bail!("CoreStore unsupported logical file compression descriptor {algorithm}");
    }
    let level = first.compression.level;
    if level == 0 || blocks.iter().any(|block| block.compression.level != level) {
        bail!("CoreStore zstd logical file blocks must use one nonzero compression level");
    }
    if blocks.iter().any(|block| {
        block.compression.uncompressed_length != block.logical_length
            || block.compression.compressed_length != block.compressed_length
    }) {
        bail!("CoreStore logical file block compression descriptors do not match block lengths");
    }
    let descriptor_hash = descriptor_hash(
        &blocks
            .iter()
            .map(|block| block.compression.descriptor_hash.as_str())
            .chain(std::iter::once(file_plaintext_hash))
            .collect::<Vec<_>>(),
    );
    Ok(CoreCompressionDescriptor {
        algorithm,
        level,
        uncompressed_length: file_plaintext_len,
        compressed_length,
        dictionary_id: String::new(),
        descriptor_hash,
    })
}

pub(super) fn logical_file_encryption_descriptor(
    blocks: &[CoreLogicalBlockRef],
    file_plaintext_hash: &str,
) -> Result<CoreEncryptionDescriptor> {
    let Some(first) = blocks.first() else {
        bail!("CoreStore logical file encryption descriptor requires at least one block");
    };
    let algorithm = first.encryption.algorithm.clone();
    if blocks
        .iter()
        .any(|block| block.encryption.algorithm != algorithm)
    {
        bail!("CoreStore logical file blocks must use one encryption algorithm");
    }
    if algorithm == "none" {
        let ciphertext_hash = descriptor_hash(
            &blocks
                .iter()
                .map(|block| block.encryption.ciphertext_hash.as_str())
                .collect::<Vec<_>>(),
        );
        return Ok(CoreEncryptionDescriptor {
            algorithm,
            key_id: String::new(),
            nonce: Vec::new(),
            aad_hash: String::new(),
            plaintext_hash: file_plaintext_hash.to_string(),
            ciphertext_hash,
            descriptor_hash: descriptor_hash(&["encryption", "none", file_plaintext_hash]),
        });
    }
    if algorithm != "aes_gcm_siv" {
        bail!("CoreStore unsupported logical file encryption descriptor {algorithm}");
    }
    let key_id = first.encryption.key_id.clone();
    if blocks.iter().any(|block| block.encryption.key_id != key_id) {
        bail!("CoreStore logical file encrypted blocks must use one key id");
    }
    let block_descriptor_hash = descriptor_hash(
        &blocks
            .iter()
            .map(|block| block.encryption.descriptor_hash.as_str())
            .collect::<Vec<_>>(),
    );
    Ok(CoreEncryptionDescriptor {
        algorithm,
        key_id,
        nonce: Vec::new(),
        aad_hash: block_descriptor_hash.clone(),
        plaintext_hash: file_plaintext_hash.to_string(),
        ciphertext_hash: descriptor_hash(
            &blocks
                .iter()
                .map(|block| block.encryption.ciphertext_hash.as_str())
                .collect::<Vec<_>>(),
        ),
        descriptor_hash: descriptor_hash(&[
            "encryption",
            "aes_gcm_siv",
            file_plaintext_hash,
            &block_descriptor_hash,
        ]),
    })
}

pub(super) fn logical_block_ids_for_range(
    blocks: &[CoreLogicalBlockRef],
    range_start: u64,
    range_end: u64,
) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| {
            let block_start = block.logical_offset;
            let block_end = block.logical_offset.saturating_add(block.logical_length);
            range_start.max(block_start) < range_end.min(block_end)
        })
        .map(|block| block.block_id.clone())
        .collect()
}

pub(super) fn validate_logical_file_manifest_shape(
    manifest: &CoreLogicalFileManifest,
) -> Result<()> {
    if manifest.schema != CORE_LOGICAL_FILE_MANIFEST_SCHEMA {
        bail!("CoreStore logical file manifest has invalid schema");
    }
    validate_logical_file_id(&manifest.logical_file_id, "logical file id")?;
    validate_writer_family(&manifest.writer_family, "writer family")?;
    let profile = local_erasure_profile_for_counts(
        &manifest.erasure_profile_id,
        manifest.data_shards as usize,
        manifest.parity_shards as usize,
    )?;
    if manifest.codec_id != profile.codec_id {
        bail!("CoreStore logical file manifest codec id does not match erasure profile");
    }
    if manifest.blocks.is_empty() {
        bail!("CoreStore logical file manifest must contain at least one block");
    }
    match manifest.compression.algorithm.as_str() {
        "none" => {
            if manifest.compression.level != 0
                || manifest.compression.uncompressed_length != manifest.logical_size
                || manifest.compression.compressed_length != manifest.logical_size
            {
                bail!("CoreStore none compression descriptor does not match logical size");
            }
        }
        "zstd" => {
            if manifest.compression.level == 0
                || manifest.compression.uncompressed_length != manifest.logical_size
            {
                bail!("CoreStore zstd compression descriptor is invalid");
            }
        }
        other => bail!("CoreStore unsupported logical file compression descriptor {other}"),
    }
    let mut ordered_blocks = manifest.blocks.iter().collect::<Vec<_>>();
    ordered_blocks.sort_by_key(|block| block.logical_offset);
    let mut expected_offset = 0u64;
    let mut stored_len = 0u64;
    for block in ordered_blocks {
        if block.logical_offset != expected_offset {
            bail!("CoreStore logical file blocks must cover the file without gaps or overlap");
        }
        expected_offset = expected_offset.saturating_add(block.logical_length);
        stored_len = stored_len.saturating_add(block.compressed_length);
        if block.data_shards != manifest.data_shards
            || block.parity_shards != manifest.parity_shards
        {
            bail!("CoreStore logical file block shard counts mismatch manifest");
        }
        if block.codec_id != profile.codec_id {
            bail!("CoreStore logical file block codec id does not match erasure profile");
        }
        validate_logical_block_encryption(block)?;
        validate_logical_block_compression(block)?;
        if block.encrypted_length < block.compressed_length {
            bail!(
                "CoreStore logical file encrypted length cannot be smaller than compressed length"
            );
        }
        if block.shards.len() < profile.minimum_read_shards {
            bail!("CoreStore logical file block does not contain enough shard receipts");
        }
        validate_boundary_summary_fields(&block.boundary_summary_hash, &block.boundary_values_b64)?;
        for shard in &block.shards {
            if shard.boundary_summary_hash != block.boundary_summary_hash
                || shard.boundary_values_b64 != block.boundary_values_b64
            {
                bail!("CoreStore logical file shard boundary summary does not match block");
            }
            if shard.placement_epoch != LOCAL_PLACEMENT_EPOCH {
                bail!("CoreStore logical file shard has stale placement epoch");
            }
            if shard.fsync_sequence == 0 {
                bail!("CoreStore logical file shard is missing fsync evidence");
            }
        }
    }
    if expected_offset != manifest.logical_size {
        bail!("CoreStore logical file blocks must cover the complete logical file");
    }
    if stored_len != manifest.compression.compressed_length {
        bail!("CoreStore logical file block stored lengths do not match compression descriptor");
    }
    if logical_file_compression_descriptor(
        &manifest.blocks,
        manifest.logical_size,
        &manifest.content_hash,
    )? != manifest.compression
    {
        bail!("CoreStore logical file compression descriptor does not match block descriptors");
    }
    validate_logical_file_encryption_descriptor(manifest)?;
    Ok(())
}

pub(super) fn validate_writer_family(value: &str, label: &str) -> Result<()> {
    validate_logical_id(value, label)?;
    if WriterFamily::from_name(value).is_none() {
        bail!("CoreStore {label} {value:?} is not a registered writer family");
    }
    Ok(())
}

pub(super) fn validate_logical_file_id(value: &str, label: &str) -> Result<()> {
    validate_logical_id(value, label)?;
    let Some(hash) = value.strip_prefix("lf_") else {
        bail!("CoreStore {label} must use canonical lf_ prefix");
    };
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} must be lf_ followed by a 64 byte hex digest");
    }
    Ok(())
}

pub(super) fn is_canonical_logical_file_id(value: &str) -> bool {
    let Some(hash) = value.strip_prefix("lf_") else {
        return false;
    };
    hash.len() == 64 && hash.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

pub(super) fn validate_logical_block_encryption(block: &CoreLogicalBlockRef) -> Result<()> {
    match block.encryption.algorithm.as_str() {
        "none" => {
            if !block.encryption.key_id.is_empty()
                || !block.encryption.nonce.is_empty()
                || !block.encryption.aad_hash.is_empty()
            {
                bail!(
                    "CoreStore none encrypted block descriptor must not carry key material fields"
                );
            }
            if block.encryption.plaintext_hash.is_empty()
                || block.encryption.ciphertext_hash.is_empty()
            {
                bail!("CoreStore none encrypted block descriptor is incomplete");
            }
            if block.encryption.ciphertext_hash != block.block_encoded_hash {
                bail!(
                    "CoreStore none encrypted block ciphertext hash must match encoded block hash"
                );
            }
        }
        "aes_gcm_siv" => {
            validate_pipeline_key_id(block.encryption.key_id.clone())?;
            if block.encryption.nonce.len() != CORE_PIPELINE_NONCE_LEN {
                bail!("CoreStore aes_gcm_siv block nonce has invalid length");
            }
            if block.encryption.aad_hash.is_empty()
                || block.encryption.plaintext_hash.is_empty()
                || block.encryption.ciphertext_hash.is_empty()
            {
                bail!("CoreStore aes_gcm_siv block descriptor is incomplete");
            }
            if block.encryption.ciphertext_hash != block.block_encoded_hash {
                bail!("CoreStore aes_gcm_siv block ciphertext hash must match encoded block hash");
            }
        }
        other => bail!("CoreStore unsupported logical file encryption descriptor {other}"),
    }
    Ok(())
}

pub(super) fn validate_logical_block_compression(block: &CoreLogicalBlockRef) -> Result<()> {
    match block.compression.algorithm.as_str() {
        "none" => {
            if block.compression.level != 0
                || !block.compression.dictionary_id.is_empty()
                || block.compression.uncompressed_length != block.logical_length
                || block.compression.compressed_length != block.logical_length
                || block.compressed_length != block.logical_length
            {
                bail!("CoreStore none-compressed block descriptor does not match block length");
            }
        }
        "zstd" => {
            if block.compression.level == 0
                || block.compression.uncompressed_length != block.logical_length
                || block.compression.compressed_length != block.compressed_length
                || block.compressed_length == 0 && block.logical_length != 0
            {
                bail!("CoreStore zstd block descriptor does not match block lengths");
            }
        }
        other => bail!("CoreStore unsupported logical file block compression descriptor {other}"),
    }
    if block.compression.descriptor_hash.is_empty() {
        bail!("CoreStore logical file block compression descriptor hash is missing");
    }
    Ok(())
}

pub(super) fn validate_logical_file_encryption_descriptor(
    manifest: &CoreLogicalFileManifest,
) -> Result<()> {
    let expected = logical_file_encryption_descriptor(&manifest.blocks, &manifest.content_hash)?;
    if expected != manifest.encryption {
        bail!("CoreStore logical file encryption descriptor does not match block descriptors");
    }
    Ok(())
}

pub(super) fn object_ref_from_logical_block_ref(
    block: &CoreLogicalBlockRef,
    erasure_profile_id: &str,
) -> Result<CoreObjectRef> {
    Ok(CoreObjectRef {
        hash: block.block_encoded_hash.clone(),
        logical_size: block.encrypted_length,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&block.block_encoded_hash)?,
            erasure_profile_id,
        ),
        encoding: CoreObjectEncoding {
            block_id: block.block_id.clone(),
            profile_id: erasure_profile_id.to_string(),
            data_shards: block.data_shards as u16,
            parity_shards: block.parity_shards as u16,
            minimum_read_shards: block.data_shards as u16,
            minimum_write_ack_shards: (block.data_shards + block.parity_shards) as u16,
            stripe_size: block
                .shard_payload_len
                .saturating_mul(u64::from(block.data_shards)),
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            stored_hash: block.block_encoded_hash.clone(),
            compression: none_compression_descriptor_from_hash(
                block.encrypted_length,
                &block.block_encoded_hash,
            ),
            encryption: block.encryption.algorithm.clone(),
        },
        placements: block
            .shards
            .iter()
            .map(|shard| CoreObjectPlacement {
                shard_index: shard.shard_index as u16,
                node_id: shard.node_id.clone(),
                region_id: shard.region_id.clone(),
                cell_id: shard.cell_id.clone(),
                shard_hash: shard.shard_hash.clone(),
                stored_size: shard.stored_length,
                generation: shard.generation,
                placement_epoch: shard.placement_epoch,
                fsync_sequence: shard.fsync_sequence,
                written_at_unix_nanos: shard.written_at_unix_nanos,
                signed_payload_hash: shard.signed_payload_hash.clone(),
                signature_algorithm: shard.signature_algorithm.clone(),
                receipt_signature: shard.receipt_signature.clone(),
            })
            .collect(),
    })
}

pub(super) fn object_ref_from_object_manifest(
    manifest: &CoreObjectManifest,
) -> Result<CoreObjectRef> {
    Ok(CoreObjectRef {
        hash: manifest.object_hash.clone(),
        logical_size: manifest.logical_size,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&manifest.object_hash)?,
            &manifest.encoding.profile_id,
        ),
        encoding: manifest.encoding.clone(),
        placements: manifest.placements.clone(),
    })
}

pub(super) fn encode_logical_file_manifest_bytes(
    manifest: &CoreLogicalFileManifest,
) -> Result<Vec<u8>> {
    encode_logical_file_manifest_proto(manifest)
}

pub(super) fn decode_logical_file_manifest_bytes(
    bytes: &[u8],
    manifest_encoding: &str,
) -> Result<CoreLogicalFileManifest> {
    match manifest_encoding {
        "deterministic-protobuf" | CORE_INLINE_MANIFEST_BODY_ENCODING => {
            decode_logical_file_manifest_proto(bytes)
        }
        other => bail!("CoreStore unsupported logical file manifest encoding {other}"),
    }
}

pub(super) const CORE_INLINE_MANIFEST_BODY_SCHEMA: &str = "anvil.core.inline_manifest_body.v1";
pub(super) const CORE_INLINE_MANIFEST_BODY_ENCODING: &str =
    "coremeta-inline-deterministic-protobuf";

pub(super) fn is_inline_manifest_body_locator(locator: &CoreManifestLocator) -> bool {
    locator.manifest_encoding == CORE_INLINE_MANIFEST_BODY_ENCODING
}

pub(super) fn inline_manifest_body_key(manifest_hash: &str) -> Result<Vec<u8>> {
    validate_hash(manifest_hash, "inline manifest body hash")?;
    Ok(meta_tuple_key(&[
        b"inline-manifest-body",
        manifest_hash.as_bytes(),
    ]))
}

pub(super) fn inline_manifest_locator_from_body(
    logical_file_id: String,
    writer_family: String,
    writer_generation: u64,
    body: &[u8],
) -> Result<CoreManifestLocator> {
    validate_logical_file_id(&logical_file_id, "inline manifest body logical file id")?;
    validate_writer_family(&writer_family, "inline manifest body writer family")?;
    if body.is_empty() {
        bail!("CoreStore inline manifest body must not be empty");
    }
    if body.len() > CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES {
        bail!(
            "CoreStore inline manifest body is {} bytes, exceeding {} bytes",
            body.len(),
            CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES
        );
    }
    let manifest_hash = format!("sha256:{}", sha256_hex(body));
    Ok(CoreManifestLocator {
        manifest_ref: CoreManifestRef {
            logical_file_id,
            writer_family,
            writer_generation,
            manifest_hash: manifest_hash.clone(),
        },
        manifest_encoding: CORE_INLINE_MANIFEST_BODY_ENCODING.to_string(),
        manifest_length: body.len() as u64,
        manifest_hash,
        block_locators: Vec::new(),
    })
}

pub(super) fn manifest_locator_from_manifest_and_ref(
    manifest: &CoreLogicalFileManifest,
    manifest_object_ref: &CoreObjectRef,
    manifest_hash: &str,
) -> Result<CoreManifestLocator> {
    validate_hash(manifest_hash, "logical file manifest hash")?;
    let manifest_bytes_len = manifest_object_ref.logical_size;
    let block_locators = vec![block_locator_from_manifest_object_ref(
        manifest,
        manifest_object_ref,
        manifest_hash,
    )?];

    Ok(CoreManifestLocator {
        manifest_ref: CoreManifestRef {
            logical_file_id: manifest.logical_file_id.clone(),
            writer_family: manifest.writer_family.clone(),
            writer_generation: manifest.writer_generation,
            manifest_hash: manifest_hash.to_string(),
        },
        manifest_encoding: "deterministic-protobuf".to_string(),
        manifest_length: manifest_bytes_len,
        manifest_hash: manifest_hash.to_string(),
        block_locators,
    })
}

pub(super) fn is_local_shard_node_id(node_id: &str) -> bool {
    node_id
        .strip_prefix(LOCAL_NODE_ID_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
}

pub(super) fn is_local_control_node_id(node_id: &str) -> bool {
    node_id
        .strip_prefix(LOCAL_CONTROL_NODE_ID_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
}

pub(super) fn validate_shard_receipt_common(
    node_id: &str,
    region_id: &str,
    cell_id: &str,
    shard_hash: &str,
    _shard_length: u64,
    fsync_sequence: u64,
    written_at_unix_nanos: u64,
    signed_payload_hash: &str,
    signature_algorithm: &str,
    receipt_signature: &[u8],
    expected_signed_payload_hash: &str,
) -> Result<()> {
    validate_logical_id(node_id, "shard receipt node id")?;
    validate_logical_id(region_id, "shard receipt region id")?;
    validate_logical_id(cell_id, "shard receipt cell id")?;
    validate_hash(shard_hash, "shard receipt hash")?;
    if fsync_sequence == 0 {
        bail!("CoreStore shard receipt fsync sequence must be nonzero");
    }
    if written_at_unix_nanos == 0 {
        bail!("CoreStore shard receipt timestamp must be nonzero");
    }
    validate_hash(signed_payload_hash, "shard receipt payload hash")?;
    if signature_algorithm != "ed25519" {
        bail!(
            "CoreStore shard receipt uses unsupported signature algorithm {}",
            signature_algorithm
        );
    }
    if receipt_signature.is_empty() {
        bail!("CoreStore shard receipt signature must not be empty");
    }
    if signed_payload_hash != expected_signed_payload_hash {
        bail!("CoreStore shard receipt signed payload hash mismatch");
    }
    Ok(())
}

pub(super) fn validate_local_shard_receipt_placement(
    profile: LocalErasureProfile,
    shard_index: usize,
    node_id: &str,
    region_id: &str,
    cell_id: &str,
) -> Result<()> {
    let expected = plan_local_shard_placements(profile)?
        .into_iter()
        .nth(shard_index)
        .ok_or_else(|| anyhow!("CoreStore shard receipt index exceeds placement plan"))?;
    // The static local placement plan defines deterministic shard node/cell
    // assignments. The region comes from the active node identity in real
    // deployments, so receipts must validate it as an identity rather than
    // force the unit-test-only "local" region.
    validate_logical_id(region_id, "local shard receipt region id")?;
    if expected.node_id != node_id || expected.cell_id != cell_id {
        bail!(
            "CoreStore shard receipt placement mismatch for shard {}: expected */{}/{}, got {}/{}/{}",
            shard_index,
            expected.cell_id,
            expected.node_id,
            region_id,
            cell_id,
            node_id
        );
    }
    Ok(())
}

pub(super) fn validate_manifest_locator(locator: &CoreManifestLocator) -> Result<()> {
    validate_logical_file_id(
        &locator.manifest_ref.logical_file_id,
        "manifest locator logical file id",
    )?;
    validate_writer_family(
        &locator.manifest_ref.writer_family,
        "manifest locator writer family",
    )?;
    validate_hash(
        &locator.manifest_ref.manifest_hash,
        "manifest locator ref hash",
    )?;
    validate_hash(&locator.manifest_hash, "manifest locator hash")?;
    if locator.manifest_hash != locator.manifest_ref.manifest_hash {
        bail!("CoreStore manifest locator hash must match manifest ref hash");
    }
    match locator.manifest_encoding.as_str() {
        "deterministic-protobuf" | "writer-segment" | CORE_INLINE_MANIFEST_BODY_ENCODING => {}
        other => bail!("CoreStore unsupported manifest locator encoding {other}"),
    }
    if locator.manifest_length == 0 {
        bail!("CoreStore manifest locator length must be nonzero");
    }
    if is_inline_manifest_body_locator(locator) {
        if !locator.block_locators.is_empty() {
            bail!("CoreStore inline manifest body locator must not include block locators");
        }
        if locator.manifest_length as usize > CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES {
            bail!(
                "CoreStore inline manifest body locator length is {} bytes, exceeding {} bytes",
                locator.manifest_length,
                CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES
            );
        }
        return Ok(());
    }
    if locator.block_locators.is_empty() {
        bail!("CoreStore manifest locator must include block locators");
    }
    let mut expected_start = 0u64;
    for block in &locator.block_locators {
        if block.logical_start != expected_start || block.logical_end <= block.logical_start {
            bail!("CoreStore manifest locator block ranges must be contiguous and non-empty");
        }
        expected_start = block.logical_end;
        validate_hash(&block.block_plain_hash, "manifest locator block plain hash")?;
        validate_hash(
            &block.block_encoded_hash,
            "manifest locator block encoded hash",
        )?;
        if block.data_shards == 0 {
            bail!("CoreStore manifest locator block must include data shards");
        }
        let profile = local_erasure_profile_for_counts(
            &block.erasure_profile_id,
            block.data_shards as usize,
            block.parity_shards as usize,
        )?;
        if block.codec_id != profile.codec_id {
            bail!("CoreStore manifest locator block codec id does not match erasure profile");
        }
        let block_len = block.logical_end - block.logical_start;
        if block.plaintext_block_len != block_len {
            bail!("CoreStore manifest locator plaintext block length mismatch");
        }
        if block.shard_payload_len == 0 {
            bail!("CoreStore manifest locator shard payload length must be nonzero");
        }
        let expected_padding = block
            .shard_payload_len
            .saturating_mul(u64::from(block.data_shards))
            .saturating_sub(block.plaintext_block_len);
        if block.padding_len != expected_padding {
            bail!("CoreStore manifest locator padding length mismatch");
        }
        if block.placement_epoch == 0 {
            bail!("CoreStore manifest locator block placement epoch must be nonzero");
        }
        validate_boundary_summary_fields(&block.boundary_summary_hash, &block.boundary_values_b64)?;
        if block.shard_receipts.len() < profile.minimum_write_ack_shards {
            bail!("CoreStore manifest locator block has too few shard receipts");
        }
        let mut seen_shards = BTreeSet::new();
        for receipt in &block.shard_receipts {
            if !seen_shards.insert(receipt.shard_index) {
                bail!("CoreStore manifest locator shard receipt index is duplicated");
            }
            let shard_index = u16::try_from(receipt.shard_index)
                .map_err(|_| anyhow!("CoreStore manifest locator shard index exceeds u16"))?;
            if receipt.shard_length == 0 && block.logical_end != block.logical_start {
                bail!("CoreStore manifest locator shard receipt length must be nonzero");
            }
            if receipt.boundary_summary_hash != block.boundary_summary_hash
                || receipt.boundary_values_b64 != block.boundary_values_b64
            {
                bail!(
                    "CoreStore manifest locator shard receipt boundary summary does not match block"
                );
            }
            if is_local_shard_node_id(&receipt.node_id) {
                validate_local_shard_receipt_placement(
                    profile,
                    usize::from(shard_index),
                    &receipt.node_id,
                    &receipt.region_id,
                    &receipt.cell_id,
                )?;
            } else {
                validate_logical_id(&receipt.node_id, "manifest locator receipt node id")?;
                validate_logical_id(&receipt.region_id, "manifest locator receipt region id")?;
                validate_logical_id(&receipt.cell_id, "manifest locator receipt cell id")?;
            }
            let expected_signed_payload_hash =
                shard_receipt_payload_hash(ShardReceiptPayloadInput {
                    block_id: &block.block_id,
                    shard_index,
                    erasure_profile: &block.erasure_profile_id,
                    node_id: &receipt.node_id,
                    region_id: &receipt.region_id,
                    cell_id: &receipt.cell_id,
                    placement_epoch: block.placement_epoch,
                    shard_length: receipt.shard_length,
                    shard_hash: &receipt.shard_hash,
                    fsync_sequence: receipt.fsync_sequence,
                    written_at_unix_nanos: receipt.written_at_unix_nanos,
                    boundary_summary_hash: &receipt.boundary_summary_hash,
                });
            validate_shard_receipt_common(
                &receipt.node_id,
                &receipt.region_id,
                &receipt.cell_id,
                &receipt.shard_hash,
                receipt.shard_length,
                receipt.fsync_sequence,
                receipt.written_at_unix_nanos,
                &receipt.signed_payload_hash,
                &receipt.signature_algorithm,
                &receipt.receipt_signature,
                &expected_signed_payload_hash,
            )?;
        }
    }
    if expected_start != locator.manifest_length {
        bail!("CoreStore manifest locator block ranges must cover the manifest bytes exactly");
    }
    Ok(())
}

pub(super) fn object_ref_from_manifest_block_locator(
    block: &CoreBlockLocator,
) -> Result<CoreObjectRef> {
    let profile = local_erasure_profile_for_counts(
        &block.erasure_profile_id,
        block.data_shards as usize,
        block.parity_shards as usize,
    )?;
    Ok(CoreObjectRef {
        hash: block.block_encoded_hash.clone(),
        logical_size: block.plaintext_block_len,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&block.block_encoded_hash)?,
            &block.erasure_profile_id,
        ),
        encoding: CoreObjectEncoding {
            block_id: block.block_id.clone(),
            profile_id: block.erasure_profile_id.clone(),
            data_shards: block.data_shards as u16,
            parity_shards: block.parity_shards as u16,
            minimum_read_shards: profile.minimum_read_shards as u16,
            minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
            stripe_size: block.shard_payload_len * u64::from(block.data_shards),
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            stored_hash: block.block_encoded_hash.clone(),
            compression: block.compression.clone(),
            encryption: block.encryption.algorithm.clone(),
        },
        placements: block
            .shard_receipts
            .iter()
            .map(|receipt| CoreObjectPlacement {
                shard_index: receipt.shard_index as u16,
                node_id: receipt.node_id.clone(),
                region_id: receipt.region_id.clone(),
                cell_id: receipt.cell_id.clone(),
                shard_hash: receipt.shard_hash.clone(),
                stored_size: receipt.shard_length,
                generation: 1,
                placement_epoch: block.placement_epoch,
                fsync_sequence: receipt.fsync_sequence,
                written_at_unix_nanos: receipt.written_at_unix_nanos,
                signed_payload_hash: receipt.signed_payload_hash.clone(),
                signature_algorithm: receipt.signature_algorithm.clone(),
                receipt_signature: receipt.receipt_signature.clone(),
            })
            .collect(),
    })
}

pub(super) fn block_locator_from_manifest_object_ref(
    manifest: &CoreLogicalFileManifest,
    manifest_object_ref: &CoreObjectRef,
    manifest_hash: &str,
) -> Result<CoreBlockLocator> {
    validate_hash(manifest_hash, "logical file manifest hash")?;
    validate_hash(&manifest_object_ref.hash, "logical manifest block hash")?;
    let boundary_values = manifest_boundary_values(manifest);
    let boundary_summary_hash = boundary_summary_hash(&boundary_values)?;
    let boundary_values_b64 = encode_boundary_values_b64(&boundary_values)?;
    Ok(CoreBlockLocator {
        logical_start: 0,
        logical_end: manifest_object_ref.logical_size,
        block_id: manifest_object_ref.encoding.block_id.clone(),
        codec_id: local_erasure_profile_for_counts(
            &manifest_object_ref.encoding.profile_id,
            usize::from(manifest_object_ref.encoding.data_shards),
            usize::from(manifest_object_ref.encoding.parity_shards),
        )?
        .codec_id
        .to_string(),
        data_shards: u32::from(manifest_object_ref.encoding.data_shards),
        parity_shards: u32::from(manifest_object_ref.encoding.parity_shards),
        plaintext_block_len: manifest_object_ref.logical_size,
        shard_payload_len: manifest_object_ref
            .placements
            .iter()
            .map(|placement| placement.stored_size)
            .max()
            .unwrap_or(0),
        padding_len: manifest_object_ref
            .encoding
            .stripe_size
            .saturating_sub(manifest_object_ref.logical_size),
        block_plain_hash: manifest_hash.to_string(),
        block_encoded_hash: manifest_object_ref.hash.clone(),
        compression: none_compression_descriptor_from_hash(
            manifest_object_ref.logical_size,
            manifest_hash,
        ),
        encryption: none_encryption_descriptor(manifest_hash, &manifest_object_ref.hash),
        erasure_profile_id: manifest_object_ref.encoding.profile_id.clone(),
        placement_epoch: manifest.placement_epoch,
        boundary_summary_hash: boundary_summary_hash.clone(),
        boundary_values_b64: boundary_values_b64.clone(),
        shard_receipts: manifest_object_ref
            .placements
            .iter()
            .map(|placement| {
                shard_receipt_summary_from_object_placement(
                    placement,
                    &boundary_summary_hash,
                    &boundary_values_b64,
                )
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

pub(super) fn shard_receipt_summary_from_object_placement(
    shard: &CoreObjectPlacement,
    boundary_summary_hash: &str,
    boundary_values_b64: &str,
) -> Result<CoreShardReceiptSummary> {
    validate_hash(&shard.shard_hash, "logical shard hash")?;
    validate_hash(
        &shard.signed_payload_hash,
        "logical shard receipt payload hash",
    )?;
    validate_boundary_summary_fields(boundary_summary_hash, boundary_values_b64)?;
    if shard.signature_algorithm != "ed25519" {
        bail!(
            "CoreStore shard receipt uses unsupported signature algorithm {}",
            shard.signature_algorithm
        );
    }
    if shard.receipt_signature.is_empty() {
        bail!("CoreStore shard receipt signature must not be empty");
    }
    Ok(CoreShardReceiptSummary {
        node_id: shard.node_id.clone(),
        region_id: shard.region_id.clone(),
        cell_id: shard.cell_id.clone(),
        shard_index: u32::from(shard.shard_index),
        shard_hash: shard.shard_hash.clone(),
        shard_length: shard.stored_size,
        fsync_sequence: shard.fsync_sequence,
        written_at_unix_nanos: shard.written_at_unix_nanos,
        signed_payload_hash: shard.signed_payload_hash.clone(),
        signature_algorithm: shard.signature_algorithm.clone(),
        receipt_signature: shard.receipt_signature.clone(),
        boundary_summary_hash: boundary_summary_hash.to_string(),
        boundary_values_b64: boundary_values_b64.to_string(),
    })
}

pub(super) fn manifest_boundary_values(
    manifest: &CoreLogicalFileManifest,
) -> Vec<CoreBoundaryValue> {
    let mut values = BTreeSet::new();
    for range in &manifest.ranges {
        values.extend(range.boundary_values.iter().cloned());
    }
    values.into_iter().collect()
}

pub(super) fn ensure_range_is_inside_expected_boundary(
    manifest: &CoreLogicalFileManifest,
    range: &CoreByteRange,
    expected_boundary: &[CoreBoundaryValue],
) -> Result<()> {
    if expected_boundary.is_empty() {
        return Ok(());
    }
    let matching_range = manifest.ranges.iter().any(|candidate| {
        candidate.byte_start <= range.start
            && range.end_exclusive <= candidate.byte_end
            && expected_boundary
                .iter()
                .all(|expected| candidate.boundary_values.contains(expected))
    });
    if !matching_range {
        bail!("CoreStore logical range is outside expected boundary values");
    }
    Ok(())
}

pub(super) fn encode_logical_file_source(
    compression: &str,
    source: Vec<u8>,
) -> Result<(Vec<u8>, CoreCompressionDescriptor)> {
    let uncompressed_length = source.len() as u64;
    let uncompressed_hash = format!("sha256:{}", sha256_hex(&source));
    match compression {
        "none" => Ok((
            source,
            CoreCompressionDescriptor {
                algorithm: "none".to_string(),
                level: 0,
                uncompressed_length,
                compressed_length: uncompressed_length,
                dictionary_id: String::new(),
                descriptor_hash: descriptor_hash(&[
                    "compression",
                    "none",
                    &uncompressed_length.to_string(),
                    &uncompressed_hash,
                ]),
            },
        )),
        "zstd" => {
            let level = 3;
            let compressed = zstd::stream::encode_all(Cursor::new(&source), level)?;
            let compressed_length = compressed.len() as u64;
            let compressed_hash = format!("sha256:{}", sha256_hex(&compressed));
            Ok((
                compressed,
                CoreCompressionDescriptor {
                    algorithm: "zstd".to_string(),
                    level: level as u32,
                    uncompressed_length,
                    compressed_length,
                    dictionary_id: String::new(),
                    descriptor_hash: descriptor_hash(&[
                        "compression",
                        "zstd",
                        &level.to_string(),
                        &uncompressed_length.to_string(),
                        &compressed_length.to_string(),
                        &uncompressed_hash,
                        &compressed_hash,
                    ]),
                },
            ))
        }
        other => bail!("CoreStore unsupported logical file compression policy {other}"),
    }
}

pub(super) fn none_compression_descriptor(source: &[u8]) -> CoreCompressionDescriptor {
    let uncompressed_length = source.len() as u64;
    let uncompressed_hash = format!("sha256:{}", sha256_hex(source));
    none_compression_descriptor_from_hash(uncompressed_length, &uncompressed_hash)
}

pub(super) fn none_compression_descriptor_from_hash(
    uncompressed_length: u64,
    uncompressed_hash: &str,
) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: "none".to_string(),
        level: 0,
        uncompressed_length,
        compressed_length: uncompressed_length,
        dictionary_id: String::new(),
        descriptor_hash: descriptor_hash(&[
            "compression",
            "none",
            &uncompressed_length.to_string(),
            &uncompressed_hash,
        ]),
    }
}

pub(super) fn decode_logical_file_source(compression: &str, stored: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        "none" => Ok(stored),
        "zstd" => Ok(zstd::stream::decode_all(Cursor::new(stored))?),
        other => bail!("CoreStore unsupported logical file compression descriptor {other}"),
    }
}

pub(super) fn none_encryption_descriptor(
    plaintext_hash: &str,
    ciphertext_hash: &str,
) -> CoreEncryptionDescriptor {
    CoreEncryptionDescriptor {
        algorithm: "none".to_string(),
        key_id: String::new(),
        nonce: Vec::new(),
        aad_hash: String::new(),
        plaintext_hash: plaintext_hash.to_string(),
        ciphertext_hash: ciphertext_hash.to_string(),
        descriptor_hash: descriptor_hash(&["encryption", "none", plaintext_hash, ciphertext_hash]),
    }
}

pub(super) fn validate_pipeline_key_id(key_id: String) -> Result<String> {
    if key_id.is_empty()
        || key_id.len() > 128
        || key_id.contains(':')
        || key_id.contains(',')
        || key_id.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        bail!("CoreStore pipeline key id must be 1-128 visible chars excluding ':' and ','");
    }
    Ok(key_id)
}

pub(super) fn decode_pipeline_key_hex(key_hex: &str) -> Result<[u8; CORE_PIPELINE_KEY_LEN]> {
    let key = hex::decode(key_hex.trim()).context("CoreStore pipeline key must be hex encoded")?;
    if key.len() != CORE_PIPELINE_KEY_LEN {
        bail!("CoreStore pipeline key must be exactly 32 bytes");
    }
    let mut out = [0u8; CORE_PIPELINE_KEY_LEN];
    out.copy_from_slice(&key);
    Ok(out)
}

pub(super) fn pipeline_block_aad(
    logical_file_id: &str,
    logical_offset: u64,
    logical_length: u64,
    plaintext_hash: &str,
) -> Vec<u8> {
    let mut aad = Vec::new();
    for part in [
        "anvil.core.pipeline_block.v1",
        logical_file_id,
        &logical_offset.to_string(),
        &logical_length.to_string(),
        plaintext_hash,
    ] {
        aad.extend_from_slice(&(part.len() as u64).to_le_bytes());
        aad.extend_from_slice(part.as_bytes());
    }
    aad
}

pub(super) fn encryption_descriptor_hash(
    algorithm: &str,
    key_id: &str,
    nonce: &[u8],
    aad_hash: &str,
    plaintext_hash: &str,
    ciphertext_hash: &str,
) -> String {
    descriptor_hash(&[
        "encryption",
        algorithm,
        key_id,
        &hex::encode(nonce),
        aad_hash,
        plaintext_hash,
        ciphertext_hash,
    ])
}

pub(super) fn validate_pipeline_policy(
    policy: &CorePipelinePolicy,
    _profile: LocalErasureProfile,
) -> Result<()> {
    match policy.compression.as_str() {
        "none" | "zstd" => {}
        other => bail!("CoreStore unsupported logical file compression policy {other}"),
    }
    match policy.encryption.as_str() {
        "none" | "aes_gcm_siv" => {}
        other => bail!("CoreStore unsupported logical file encryption policy {other}"),
    }
    match policy.boundary_mode.as_str() {
        "honour" | "prefer" | "ignore_for_diagnostic_only" => {}
        other => bail!("CoreStore unsupported boundary mode {other}"),
    }
    if policy.target_block_size == 0 {
        bail!("CoreStore target_block_size must be greater than zero");
    }
    Ok(())
}

pub(super) fn validate_object_blob_pipeline_options(
    compression: &str,
    encryption: &str,
) -> Result<()> {
    match compression {
        "none" | "zstd" => {}
        other => bail!("CoreStore unsupported object blob compression policy {other}"),
    }
    match encryption {
        "none" | "aes_gcm_siv" => {}
        other => bail!("CoreStore unsupported object blob encryption policy {other}"),
    }
    Ok(())
}

pub(super) fn effective_target_block_size(
    policy: &CorePipelinePolicy,
    profile: LocalErasureProfile,
) -> u64 {
    policy
        .target_block_size
        .min(profile.logical_block_target_bytes)
}

pub(super) fn logical_block_ranges_for_source(
    source: &[u8],
    request: &WriteLogicalFileRequest,
    target_block_size: usize,
) -> Result<Vec<(usize, usize)>> {
    let len = source.len();
    if len == 0 {
        return Ok(vec![(0, 0)]);
    }

    let mut cuts = BTreeSet::from([0usize, len]);
    for (start, end) in content_defined_chunk_ranges(source, target_block_size) {
        if start > 0 && start < len {
            cuts.insert(start);
        }
        if end > 0 && end < len {
            cuts.insert(end);
        }
    }
    for hint in &request.range_hints {
        let honours_hint = matches!(
            hint.preferred_block_boundary.as_str(),
            "required" | "preferred"
        );
        if !honours_hint {
            continue;
        }
        for boundary in [hint.byte_start, hint.byte_end] {
            if boundary == 0 || boundary >= len as u64 {
                continue;
            }
            if let Ok(boundary) = usize::try_from(boundary) {
                cuts.insert(boundary);
            }
        }
    }

    let ordered = cuts.into_iter().collect::<Vec<_>>();
    let ranges = ordered
        .windows(2)
        .filter_map(|window| match window {
            [start, end] if start < end => Some((*start, *end)),
            _ => None,
        })
        .collect::<Vec<_>>();
    validate_boundary_constraints_for_block_ranges(&ranges, request)?;
    Ok(ranges)
}

pub(super) fn logical_block_ranges_for_len(
    len: u64,
    range_hints: &[CoreLogicalRangeHint],
    boundary_values: &[CoreBoundaryValue],
    target_block_size: usize,
) -> Result<Vec<(usize, usize)>> {
    if target_block_size == 0 {
        bail!("CoreStore target block size must be non-zero");
    }
    let len_usize =
        usize::try_from(len).map_err(|_| anyhow!("CoreStore logical file length exceeds usize"))?;
    if len_usize == 0 {
        return Ok(vec![(0, 0)]);
    }

    let mut cuts = BTreeSet::from([0usize, len_usize]);
    let mut offset = target_block_size;
    while offset < len_usize {
        cuts.insert(offset);
        offset = offset.saturating_add(target_block_size);
        if offset == usize::MAX {
            break;
        }
    }
    for hint in range_hints {
        let honours_hint = matches!(
            hint.preferred_block_boundary.as_str(),
            "required" | "preferred"
        );
        if !honours_hint {
            continue;
        }
        for boundary in [hint.byte_start, hint.byte_end] {
            if boundary == 0 || boundary >= len {
                continue;
            }
            if let Ok(boundary) = usize::try_from(boundary) {
                cuts.insert(boundary);
            }
        }
    }

    let ranges = cuts
        .into_iter()
        .collect::<Vec<_>>()
        .windows(2)
        .filter_map(|window| match window {
            [start, end] if start < end => Some((*start, *end)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let request = WriteLogicalFileRequest {
        writer_family: "validation".to_string(),
        generation: 0,
        logical_file_id: "validation".to_string(),
        source: Vec::new(),
        range_hints: range_hints.to_vec(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
        boundary_values: boundary_values.to_vec(),
        mutation_id: "validation".to_string(),
        region_id: "validation".to_string(),
    };
    validate_boundary_constraints_for_block_ranges(&ranges, &request)?;
    Ok(ranges)
}

pub(super) fn validate_boundary_constraints_for_block_ranges(
    block_ranges: &[(usize, usize)],
    request: &WriteLogicalFileRequest,
) -> Result<()> {
    for (block_start, block_end) in block_ranges {
        let overlapping =
            boundary_vectors_for_block(*block_start as u64, *block_end as u64, request);
        validate_block_boundary_value_sets(*block_start as u64, *block_end as u64, &overlapping)?;
    }
    Ok(())
}

fn boundary_vectors_for_block<'a>(
    block_start: u64,
    block_end: u64,
    request: &'a WriteLogicalFileRequest,
) -> Vec<(
    &'a str,
    Option<&'a CoreSharedRangeMarker>,
    &'a [CoreBoundaryValue],
)> {
    let mut values = Vec::new();
    for hint in &request.range_hints {
        if block_start.max(hint.byte_start) < block_end.min(hint.byte_end) {
            values.push((
                hint.writer_record_kind.as_str(),
                hint.shared_range.as_ref(),
                hint.boundary_values.as_slice(),
            ));
        }
    }
    if values.is_empty() && !request.boundary_values.is_empty() {
        values.push((
            request.writer_family.as_str(),
            None,
            request.boundary_values.as_slice(),
        ));
    }
    values
}

fn validate_block_boundary_value_sets(
    block_start: u64,
    block_end: u64,
    vectors: &[(&str, Option<&CoreSharedRangeMarker>, &[CoreBoundaryValue])],
) -> Result<()> {
    let mut by_name: BTreeMap<
        &str,
        Vec<(&str, Option<&CoreSharedRangeMarker>, &CoreBoundaryValue)>,
    > = BTreeMap::new();
    for (record_kind, shared, values) in vectors {
        for value in *values {
            by_name
                .entry(value.name.as_str())
                .or_default()
                .push((*record_kind, *shared, value));
        }
    }

    for (dimension, values) in by_name {
        let distinct_values = values
            .iter()
            .map(|(_, _, value)| value.value.as_str())
            .collect::<BTreeSet<_>>();
        if distinct_values.len() <= 1 {
            continue;
        }

        let exemplar = values[0].2;
        let max_values = values
            .iter()
            .filter_map(|(_, _, value)| {
                (value.max_values_per_block > 0).then_some(value.max_values_per_block as usize)
            })
            .min()
            .unwrap_or(usize::MAX);
        if distinct_values.len() > max_values {
            bail!(
                "{}: CoreStore block {block_start}..{block_end} carries {} values for boundary {dimension}; max_values_per_block is {max_values}",
                AnvilErrorCode::BoundaryBlockLimitUnsatisfied.as_str(),
                distinct_values.len()
            );
        }

        let hard_security = exemplar
            .categories
            .iter()
            .any(|category| category == "security_realm");
        let hard_compaction = values
            .iter()
            .any(|(_, _, value)| value.compaction_scope == "require_same_value");
        if (hard_security || hard_compaction) && !boundary_values_are_explicitly_shared(&values) {
            let code = if hard_compaction {
                AnvilErrorCode::BoundaryRequiredSingleValueViolation.as_str()
            } else {
                AnvilErrorCode::BoundaryBlockLimitUnsatisfied.as_str()
            };
            bail!(
                "{code}: CoreStore block {block_start}..{block_end} mixes hard boundary {dimension} values without an allowed shared range marker"
            );
        }
    }
    Ok(())
}

fn boundary_values_are_explicitly_shared(
    values: &[(&str, Option<&CoreSharedRangeMarker>, &CoreBoundaryValue)],
) -> bool {
    values.iter().all(|(record_kind, shared, value)| {
        let Some(marker) = shared.as_ref().copied() else {
            return false;
        };
        value.shared_ranges_allowed
            && (marker.record_kind == *record_kind
                || value.shared_record_kinds.iter().any(|allowed| {
                    allowed == &marker.record_kind || allowed.as_str() == *record_kind
                }))
    })
}

pub(super) fn content_defined_chunk_ranges(
    source: &[u8],
    target_block_size: usize,
) -> Vec<(usize, usize)> {
    if source.is_empty() {
        return vec![(0, 0)];
    }
    let target = target_block_size.max(1024);
    if source.len() <= target {
        return vec![(0, source.len())];
    }
    let min_size = (target / 4).max(1024).min(target);
    let max_size = target.saturating_mul(2).max(target + 1);
    let mask = content_defined_chunk_mask(target);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut hash = 0u64;

    for (idx, byte) in source.iter().enumerate() {
        hash = hash.rotate_left(1).wrapping_add(gear_hash_byte(*byte));
        let len = idx + 1 - start;
        let at_max = len >= max_size;
        let at_boundary = len >= min_size && (hash & mask) == 0;
        if (at_boundary || at_max) && idx + 1 < source.len() {
            ranges.push((start, idx + 1));
            start = idx + 1;
            hash = 0;
        }
    }
    if start < source.len() {
        ranges.push((start, source.len()));
    }
    ranges
}

pub(super) fn content_defined_chunk_mask(target_block_size: usize) -> u64 {
    let average = target_block_size.next_power_of_two().max(1024) as u64;
    average.saturating_sub(1)
}

pub(super) fn gear_hash_byte(byte: u8) -> u64 {
    let mut x = u64::from(byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub(super) fn validate_logical_range_hint(hint: &CoreLogicalRangeHint) -> Result<()> {
    validate_logical_id(&hint.range_id, "logical range id")?;
    validate_logical_id(&hint.writer_record_kind, "logical range writer record kind")?;
    match hint.preferred_block_boundary.as_str() {
        "required" | "preferred" | "writer_defined" | "none" => {}
        other => bail!("CoreStore unsupported preferred block boundary {other}"),
    }
    if let Some(shared) = &hint.shared_range {
        validate_logical_id(&shared.record_kind, "shared range record kind")?;
        if shared.reason.trim().is_empty() {
            bail!("CoreStore shared range marker reason must not be empty");
        }
        if shared.boundary_dimension_ids.is_empty() {
            bail!("CoreStore shared range marker must name crossed dimensions");
        }
    }
    Ok(())
}

pub(super) fn descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(super) fn validate_manifest_for_object_ref(
    manifest: &CoreObjectManifest,
    object_ref: &CoreObjectRef,
    expected_hash: &str,
) -> Result<()> {
    if manifest.object_hash != object_ref.hash {
        bail!(
            "CoreStore manifest hash mismatch: ref {}, manifest {}",
            object_ref.hash,
            manifest.object_hash
        );
    }
    if strip_sha256_prefix(&manifest.object_hash)? != expected_hash {
        bail!("CoreStore manifest hash does not match requested object hash");
    }
    if manifest.logical_size != object_ref.logical_size {
        bail!(
            "CoreStore manifest size mismatch: ref {}, manifest {}",
            object_ref.logical_size,
            manifest.logical_size
        );
    }
    validate_logical_id(&manifest.encoding.block_id, "CoreStore manifest block id")?;
    if manifest.encoding.block_id != object_ref.encoding.block_id {
        bail!(
            "CoreStore manifest block id mismatch: ref {}, manifest {}",
            object_ref.encoding.block_id,
            manifest.encoding.block_id
        );
    }
    if manifest.encoding.stored_hash != object_ref.encoding.stored_hash {
        bail!(
            "CoreStore manifest stored hash mismatch: ref {}, manifest {}",
            object_ref.encoding.stored_hash,
            manifest.encoding.stored_hash
        );
    }
    if manifest.encoding.compression != object_ref.encoding.compression {
        bail!("CoreStore manifest compression descriptor mismatch");
    }
    let manifest_ref_profile = decode_manifest_ref_profile(&object_ref.manifest_ref)?;
    if is_inline_object_ref(object_ref) {
        if manifest_ref_profile != LOCAL_INLINE_PAYLOAD_PROFILE_ID
            || manifest.encoding.profile_id != LOCAL_INLINE_PAYLOAD_PROFILE_ID
            || manifest.encoding.data_shards != 0
            || manifest.encoding.parity_shards != 0
            || manifest.encoding.minimum_read_shards != 0
            || manifest.encoding.minimum_write_ack_shards != 0
            || !manifest.placements.is_empty()
        {
            bail!("CoreStore inline object manifest has invalid inline encoding");
        }
        return Ok(());
    }
    if manifest.encoding.compression.uncompressed_length != manifest.logical_size {
        bail!("CoreStore object compression descriptor logical length mismatch");
    }
    if manifest.encoding.compression.compressed_length == 0 && manifest.logical_size != 0 {
        bail!("CoreStore object compression descriptor has zero encoded length");
    }
    if manifest_ref_profile != manifest.encoding.profile_id {
        bail!(
            "CoreStore manifest profile mismatch: ref {}, manifest {}",
            manifest_ref_profile,
            manifest.encoding.profile_id
        );
    }
    let data_shards = usize::from(manifest.encoding.data_shards);
    let parity_shards = usize::from(manifest.encoding.parity_shards);
    let profile = local_erasure_profile_for_counts(
        &manifest.encoding.profile_id,
        data_shards,
        parity_shards,
    )?;
    let minimum_read_shards = usize::from(manifest.encoding.minimum_read_shards);
    let minimum_write_ack_shards = usize::from(manifest.encoding.minimum_write_ack_shards);
    if data_shards == 0 || parity_shards == 0 {
        bail!("CoreStore erasure profile must include data and parity shards");
    }
    if minimum_read_shards != data_shards {
        bail!(
            "CoreStore unsupported minimum_read_shards {}; expected {}",
            minimum_read_shards,
            data_shards
        );
    }
    if minimum_write_ack_shards != profile.minimum_write_ack_shards {
        bail!(
            "CoreStore minimum_write_ack_shards {} does not match profile {} requirement {}",
            minimum_write_ack_shards,
            profile.id,
            profile.minimum_write_ack_shards
        );
    }
    if manifest.encoding.placement_scope != "region" {
        bail!(
            "CoreStore unsupported placement_scope {}",
            manifest.encoding.placement_scope
        );
    }
    if manifest.encoding.repair_priority.is_empty() {
        bail!("CoreStore repair_priority must not be empty");
    }
    Ok(())
}

pub(super) fn strip_sha256_prefix(hash: &str) -> Result<&str> {
    hash.strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("CoreStore hash must have sha256: prefix"))
}

pub(super) fn validate_hash(hash: &str, label: &str) -> Result<()> {
    let value = strip_sha256_prefix(hash)?;
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} must be a sha256 hash");
    }
    Ok(())
}

pub(super) fn logical_file_name(value: &str) -> String {
    sha256_hex(value.as_bytes())
}
