use super::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

impl CoreStore {
    pub async fn write_logical_file_path_with_locator(
        &self,
        mut request: WriteLogicalFilePathRequest,
    ) -> Result<CoreLogicalFileWrite> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "write_logical_file_path")],
        );
        validate_writer_family(&request.writer_family, "writer family")?;
        let family = WriterFamily::from_name(&request.writer_family)
            .ok_or_else(|| anyhow!("CoreStore writer family is not registered"))?;
        validate_hash(&request.source_hash, "logical file source hash")?;
        let metadata = fs::metadata(&request.source_path).await.with_context(|| {
            format!(
                "inspect CoreStore logical source file {}",
                request.source_path.display()
            )
        })?;
        if !metadata.is_file() {
            bail!("CoreStore logical source path is not a regular file");
        }
        if metadata.len() != request.source_len {
            bail!(
                "CoreStore logical source length mismatch: request {}, file {}",
                request.source_len,
                metadata.len()
            );
        }
        if !is_canonical_logical_file_id(&request.logical_file_id) {
            request.logical_file_id = canonical_logical_file_id(
                family,
                request.generation,
                &request.logical_file_id,
                request.source_hash.as_bytes(),
            );
        }
        validate_logical_file_id(&request.logical_file_id, "logical file id")?;
        validate_logical_id(&request.mutation_id, "logical file mutation id")?;
        let profile = local_erasure_profile(&request.pipeline_policy.erasure_profile_id)?;
        validate_pipeline_policy(&request.pipeline_policy, profile)?;

        let blocks = self
            .write_logical_file_blocks_from_path(&request, profile)
            .await?;
        let manifest_request = WriteLogicalFileRequest {
            writer_family: request.writer_family.clone(),
            generation: request.generation,
            logical_file_id: request.logical_file_id.clone(),
            source: Vec::new(),
            range_hints: request.range_hints.clone(),
            pipeline_policy: request.pipeline_policy.clone(),
            trace_context: request.trace_context.clone(),
            boundary_values: request.boundary_values.clone(),
            mutation_id: request.mutation_id.clone(),
            region_id: request.region_id.clone(),
        };
        let manifest = logical_file_manifest_from_object_manifests(
            &manifest_request,
            &blocks,
            request.source_hash.clone(),
            request.source_len,
        )?;
        let locator = self
            .publish_logical_file_manifest(&manifest, &request.pipeline_policy)
            .await?;
        Ok(CoreLogicalFileWrite { manifest, locator })
    }

    pub(super) async fn write_logical_file_blocks_from_path(
        &self,
        request: &WriteLogicalFilePathRequest,
        profile: LocalErasureProfile,
    ) -> Result<Vec<MaterializedLogicalBlock>> {
        let target_block_size = usize::try_from(effective_target_block_size(
            &request.pipeline_policy,
            profile,
        ))
        .map_err(|_| anyhow!("CoreStore target_block_size exceeds usize"))?;
        let ranges = logical_block_ranges_for_path(
            &request.source_path,
            request.source_len,
            &request.range_hints,
            &request.boundary_values,
            target_block_size,
        )
        .await?;
        let mut file = fs::File::open(&request.source_path)
            .await
            .with_context(|| {
                format!(
                    "open CoreStore logical source file {}",
                    request.source_path.display()
                )
            })?;
        let mut blocks = Vec::new();
        let mut source_hasher = Sha256::new();
        for (index, (start, end)) in ranges.into_iter().enumerate() {
            let logical_offset = start as u64;
            let len = end.saturating_sub(start);
            file.seek(std::io::SeekFrom::Start(logical_offset)).await?;
            let mut chunk_bytes = vec![0_u8; len];
            file.read_exact(&mut chunk_bytes).await?;
            source_hasher.update(&chunk_bytes);
            let chunk_hash = format!("sha256:{}", sha256_hex(&chunk_bytes));
            let compression_started_at = Instant::now();
            let (stored_chunk, compression) =
                encode_logical_file_source(&request.pipeline_policy.compression, chunk_bytes)?;
            record_byte_pipeline_stage_duration(
                "compress",
                &request.writer_family,
                &request.pipeline_policy.compression,
                &request.pipeline_policy.encryption,
                profile.id,
                compression_started_at.elapsed(),
            );
            crate::perf::record_compression_ratio(
                &request.writer_family,
                &compression.algorithm,
                profile.id,
                len as u64,
                stored_chunk.len() as u64,
            );
            if compression.algorithm != "none" {
                record_corestore_trace_event("byte_pipeline.compress", "ok");
            }
            let block_plain_hash = format!("sha256:{}", sha256_hex(&stored_chunk));
            let encryption_started_at = Instant::now();
            let pipeline_block = self.encrypt_pipeline_block(
                &request.pipeline_policy,
                &request.logical_file_id,
                index,
                logical_offset,
                len as u64,
                &block_plain_hash,
                stored_chunk,
            )?;
            record_byte_pipeline_stage_duration(
                "encrypt",
                &request.writer_family,
                &request.pipeline_policy.compression,
                &pipeline_block.encryption.algorithm,
                profile.id,
                encryption_started_at.elapsed(),
            );
            record_corestore_trace_event("byte_pipeline.encrypt", "ok");
            let logical_request = WriteLogicalFileRequest {
                writer_family: request.writer_family.clone(),
                generation: request.generation,
                logical_file_id: request.logical_file_id.clone(),
                source: Vec::new(),
                range_hints: request.range_hints.clone(),
                pipeline_policy: request.pipeline_policy.clone(),
                trace_context: request.trace_context.clone(),
                boundary_values: request.boundary_values.clone(),
                mutation_id: request.mutation_id.clone(),
                region_id: request.region_id.clone(),
            };
            let object_ref = self
                .put_logical_file_block_with_profile(
                    &logical_request,
                    index,
                    logical_offset,
                    pipeline_block.stored,
                    block_plain_hash,
                    pipeline_block.encryption.algorithm.clone(),
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset,
                logical_length: len as u64,
                compressed_length: compression.compressed_length,
                plaintext_hash: chunk_hash,
                compression,
                encryption: pipeline_block.encryption,
            });
        }
        let actual_source_hash = format!("sha256:{}", hex::encode(source_hasher.finalize()));
        if actual_source_hash != request.source_hash {
            bail!("CoreStore logical source file hash mismatch");
        }
        Ok(blocks)
    }
}

async fn logical_block_ranges_for_path(
    source_path: &std::path::Path,
    source_len: u64,
    range_hints: &[CoreLogicalRangeHint],
    boundary_values: &[CoreBoundaryValue],
    target_block_size: usize,
) -> Result<Vec<(usize, usize)>> {
    if target_block_size == 0 {
        bail!("CoreStore target block size must be non-zero");
    }
    let len_usize = usize::try_from(source_len)
        .map_err(|_| anyhow!("CoreStore logical source file length exceeds usize"))?;
    if len_usize == 0 {
        return Ok(vec![(0, 0)]);
    }

    let target = target_block_size.max(1024);
    let min_size = (target / 4).max(1024).min(target);
    let max_size = target.saturating_mul(2).max(target + 1);
    let mask = content_defined_chunk_mask(target);
    let mut cuts = BTreeSet::from([0usize, len_usize]);
    let mut file = fs::File::open(source_path)
        .await
        .with_context(|| format!("open CoreStore source file {}", source_path.display()))?;
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut start = 0usize;
    let mut absolute = 0usize;
    let mut hash = 0u64;

    loop {
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("read CoreStore source file {}", source_path.display()))?;
        if read == 0 {
            break;
        }
        for byte in &buffer[..read] {
            hash = hash.rotate_left(1).wrapping_add(gear_hash_byte(*byte));
            absolute = absolute.saturating_add(1);
            let len = absolute.saturating_sub(start);
            let at_max = len >= max_size;
            let at_boundary = len >= min_size && (hash & mask) == 0;
            if (at_boundary || at_max) && absolute < len_usize {
                cuts.insert(absolute);
                start = absolute;
                hash = 0;
            }
        }
    }
    if absolute != len_usize {
        bail!(
            "CoreStore logical source file changed while chunking: expected {} bytes, read {} bytes",
            len_usize,
            absolute
        );
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
            if boundary == 0 || boundary >= source_len {
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
