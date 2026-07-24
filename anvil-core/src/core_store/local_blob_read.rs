use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};

impl CoreStore {
    pub async fn get_blob(&self, input: GetBlob) -> Result<Vec<u8>> {
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "get_blob")]);
        if is_inline_object_ref(&input.object_ref) {
            return self.read_inline_blob(&input.object_ref);
        }
        if let Some(manifest) =
            Box::pin(self.logical_file_manifest_from_object_ref(&input.object_ref)).await?
        {
            return Box::pin(self.read_logical_range(ReadLogicalRangeRequest {
                ranges: vec![CoreByteRange {
                    start: 0,
                    end_exclusive: manifest.logical_size,
                }],
                manifest,
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "system".to_string(),
                    authz_realm_id: "corestore".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            }))
            .await;
        }
        let expected_hash = strip_sha256_prefix(&input.object_ref.hash)?;
        let manifest = self.read_object_manifest(&input.object_ref).await?;
        if manifest.object_hash != input.object_ref.hash {
            bail!(
                "CoreStore manifest hash mismatch: ref {}, manifest {}",
                input.object_ref.hash,
                manifest.object_hash
            );
        }
        if manifest.logical_size != input.object_ref.logical_size {
            bail!(
                "CoreStore manifest size mismatch: ref {}, manifest {}",
                input.object_ref.logical_size,
                manifest.logical_size
            );
        }
        let profile = local_erasure_profile(&manifest.encoding.profile_id)?;

        let data_shards = usize::from(manifest.encoding.data_shards);
        let parity_shards = usize::from(manifest.encoding.parity_shards);
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
        if minimum_write_ack_shards > data_shards + parity_shards {
            bail!(
                "CoreStore minimum_write_ack_shards {} exceeds total shard count {}",
                minimum_write_ack_shards,
                data_shards + parity_shards
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
        let manifest_boundary_summary_hash = boundary_summary_hash(&manifest.boundary_values)?;
        let manifest_boundary_values_b64 = encode_boundary_values_b64(&manifest.boundary_values)?;
        let total_shards = data_shards + parity_shards;
        let mut shards = vec![None; total_shards];
        let mut shard_failures = Vec::new();
        let mut pending_reads = FuturesUnordered::new();
        let block_id = manifest.encoding.block_id.as_str();
        let boundary_summary_hash = manifest_boundary_summary_hash.as_str();
        let boundary_values_b64 = manifest_boundary_values_b64.as_str();
        for placement in &manifest.placements {
            self.verify_object_placement_receipt(
                &manifest.encoding.block_id,
                profile.id,
                placement,
                &manifest_boundary_summary_hash,
            )?;
            let index = usize::from(placement.shard_index);
            if index >= total_shards {
                bail!(
                    "CoreStore manifest placement index {} exceeds total shard count {}",
                    index,
                    total_shards
                );
            }
            pending_reads.push(async move {
                let block_read_started_at = Instant::now();
                let result = self
                    .read_shard_from_placement(ReadShardFromPlacement {
                        block_id,
                        profile,
                        placement,
                        boundary_summary_hash,
                        boundary_values_b64,
                        range: None,
                        operation: "read_blob_shard",
                    })
                    .await;
                (placement, result, block_read_started_at.elapsed())
            });
        }
        while let Some((placement, result, elapsed)) = pending_reads.next().await {
            let index = usize::from(placement.shard_index);
            match result {
                Ok(shard_bytes) => {
                    record_block_read_duration(
                        &placement.node_id,
                        &placement.region_id,
                        &placement.cell_id,
                        "read_blob_shard",
                        "distributed",
                        "ok",
                        elapsed,
                    );
                    shards[index] = Some(shard_bytes);
                    if shards.iter().filter(|shard| shard.is_some()).count() >= data_shards {
                        break;
                    }
                }
                Err(err) => {
                    record_block_read_duration(
                        &placement.node_id,
                        &placement.region_id,
                        &placement.cell_id,
                        "read_blob_shard",
                        "distributed",
                        "unavailable",
                        elapsed,
                    );
                    shard_failures.push(format!(
                        "{}:{} on {}: {err:#}",
                        block_id, placement.shard_index, placement.node_id
                    ));
                }
            }
        }
        let present = shards.iter().filter(|shard| shard.is_some()).count();
        if present < data_shards {
            return Err(CoreStoreAvailabilityError::ShardQuorumUnavailable {
                operation: "object_read",
                required: data_shards,
                received: present,
                details: format!(
                    "object {} unavailable or invalid shards: {}",
                    input.object_ref.hash,
                    shard_failures.join("; ")
                ),
            }
            .into());
        }
        let profile = local_erasure_profile_for_counts(
            &manifest.encoding.profile_id,
            data_shards,
            parity_shards,
        )?;
        let missing_shards = shards
            .iter()
            .filter(|shard| shard.is_none())
            .count()
            .to_string();
        let reconstruct_started_at = Instant::now();
        reconstruct_data_shards(&mut shards, profile)?;
        crate::perf::record_duration(
            "anvil_erasure_reconstruct_duration_ms",
            &[
                ("erasure_profile", profile.id),
                ("missing_shards", &missing_shards),
                ("range_read", "false"),
            ],
            reconstruct_started_at.elapsed(),
        );
        crate::perf::record_erasure_reconstruction_total(profile.id, "ok");
        record_corestore_trace_event("erasure.decode", "ok");
        let mut data = Vec::with_capacity(
            data_shards.saturating_mul(
                shards
                    .iter()
                    .find_map(|shard| shard.as_ref().map(Vec::len))
                    .unwrap_or_default(),
            ),
        );
        for shard in shards.iter().take(data_shards) {
            let Some(shard) = shard else {
                bail!("CoreStore erasure reconstruction left a missing data shard");
            };
            data.extend_from_slice(shard);
        }
        let stored_size = usize::try_from(manifest.encoding.compression.compressed_length)
            .map_err(|_| anyhow!("CoreStore encoded object size exceeds usize"))?;
        if data.len() < stored_size {
            bail!("CoreStore reconstructed object is shorter than encoded length");
        }
        data.truncate(stored_size);
        let expected_stored_hash = strip_sha256_prefix(&manifest.encoding.stored_hash)?;
        let actual_stored_hash = sha256_hex(&data);
        if actual_stored_hash != expected_stored_hash {
            bail!(
                "CoreStore stored blob hash mismatch: expected {expected_stored_hash}, got {actual_stored_hash}"
            );
        }
        let decoded = decode_logical_file_source(&manifest.encoding.compression.algorithm, data)?;
        if decoded.len() as u64 != manifest.logical_size {
            bail!("CoreStore decoded object length does not match manifest logical size");
        }
        let actual = sha256_hex(&decoded);
        if actual != expected_hash {
            bail!("CoreStore blob hash mismatch: expected {expected_hash}, got {actual}");
        }
        Ok(decoded)
    }

    pub async fn get_blob_range(&self, input: GetBlobRange) -> Result<Vec<u8>> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "get_blob_range")]);
        if input.range.start > input.range.end_exclusive {
            bail!("CoreStore range start must be <= end_exclusive");
        }
        if input.range.end_exclusive > input.object_ref.logical_size {
            bail!("CoreStore range end_exclusive exceeds logical object size");
        }
        if input.range.start == input.range.end_exclusive {
            return Ok(Vec::new());
        }
        if is_inline_object_ref(&input.object_ref) {
            let full = self.read_inline_blob(&input.object_ref)?;
            let start = usize::try_from(input.range.start)
                .map_err(|_| anyhow!("CoreStore inline range start exceeds usize"))?;
            let end = usize::try_from(input.range.end_exclusive)
                .map_err(|_| anyhow!("CoreStore inline range end exceeds usize"))?;
            return Ok(full[start..end].to_vec());
        }
        if let Some(manifest) =
            Box::pin(self.logical_file_manifest_from_object_ref(&input.object_ref)).await?
        {
            return Box::pin(self.read_logical_range(ReadLogicalRangeRequest {
                ranges: vec![input.range],
                manifest,
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "system".to_string(),
                    authz_realm_id: "corestore".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            }))
            .await;
        }

        let expected_hash = strip_sha256_prefix(&input.object_ref.hash)?;
        let manifest = self.read_object_manifest(&input.object_ref).await?;
        validate_manifest_for_object_ref(&manifest, &input.object_ref, expected_hash)?;
        if manifest.encoding.compression.algorithm != "none" {
            return self.get_blob_range_via_full_reconstruction(input).await;
        }
        let profile = local_erasure_profile(&manifest.encoding.profile_id)?;
        let manifest_boundary_summary_hash = boundary_summary_hash(&manifest.boundary_values)?;
        let manifest_boundary_values_b64 = encode_boundary_values_b64(&manifest.boundary_values)?;

        let data_shards = usize::from(manifest.encoding.data_shards);
        let shard_len = input
            .object_ref
            .logical_size
            .div_ceil(data_shards as u64)
            .max(1);
        let mut out = Vec::with_capacity(
            usize::try_from(input.range.end_exclusive - input.range.start).unwrap_or(usize::MAX),
        );

        for shard_index in 0..data_shards {
            let shard_logical_start = shard_index as u64 * shard_len;
            let shard_logical_end =
                (shard_logical_start + shard_len).min(input.object_ref.logical_size);
            let overlap_start = input.range.start.max(shard_logical_start);
            let overlap_end = input.range.end_exclusive.min(shard_logical_end);
            if overlap_start >= overlap_end {
                continue;
            }

            let Some(placement) = manifest
                .placements
                .iter()
                .find(|placement| usize::from(placement.shard_index) == shard_index)
            else {
                return self.get_blob_range_via_full_reconstruction(input).await;
            };
            let block_read_started_at = Instant::now();
            let shard_bytes = match self
                .read_shard_from_placement(ReadShardFromPlacement {
                    block_id: &manifest.encoding.block_id,
                    profile,
                    placement,
                    boundary_summary_hash: &manifest_boundary_summary_hash,
                    boundary_values_b64: &manifest_boundary_values_b64,
                    range: None,
                    operation: "read_blob_range_shard",
                })
                .await
            {
                Ok(bytes) => {
                    record_block_read_duration(
                        &placement.node_id,
                        &placement.region_id,
                        &placement.cell_id,
                        "read_blob_range_shard",
                        "local",
                        "ok",
                        block_read_started_at.elapsed(),
                    );
                    bytes
                }
                Err(_err) => {
                    record_block_read_duration(
                        &placement.node_id,
                        &placement.region_id,
                        &placement.cell_id,
                        "read_blob_range_shard",
                        "local",
                        "fallback_reconstruction",
                        block_read_started_at.elapsed(),
                    );
                    return self.get_blob_range_via_full_reconstruction(input).await;
                }
            };
            let shard_offset = usize::try_from(overlap_start - shard_logical_start)
                .map_err(|_| anyhow!("CoreStore range offset exceeds usize"))?;
            let shard_end = usize::try_from(overlap_end - shard_logical_start)
                .map_err(|_| anyhow!("CoreStore range end exceeds usize"))?;
            out.extend_from_slice(&shard_bytes[shard_offset..shard_end]);
        }

        Ok(out)
    }

    pub async fn read_object_ref_chunks<F, Fut>(
        &self,
        object_ref: CoreObjectRef,
        range: Option<CoreByteRange>,
        chunk_size: usize,
        mut on_chunk: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_object_ref_chunks")],
        );
        if chunk_size == 0 {
            bail!("CoreStore object ref chunk size must be greater than zero");
        }
        let read_range = range.unwrap_or(CoreByteRange {
            start: 0,
            end_exclusive: object_ref.logical_size,
        });
        if read_range.start > read_range.end_exclusive {
            bail!("CoreStore object ref range start must be <= end_exclusive");
        }
        if read_range.end_exclusive > object_ref.logical_size {
            bail!("CoreStore object ref range exceeds object size");
        }
        if read_range.start == read_range.end_exclusive {
            return Ok(());
        }

        if is_inline_object_ref(&object_ref) {
            let full = self.read_inline_blob(&object_ref)?;
            let start = usize::try_from(read_range.start)
                .map_err(|_| anyhow!("CoreStore inline range start exceeds usize"))?;
            let end = usize::try_from(read_range.end_exclusive)
                .map_err(|_| anyhow!("CoreStore inline range end exceeds usize"))?;
            for chunk in full[start..end].chunks(chunk_size) {
                on_chunk(chunk.to_vec()).await?;
            }
            return Ok(());
        }
        if let Some(manifest) = self
            .logical_file_manifest_from_object_ref(&object_ref)
            .await?
        {
            return self
                .read_logical_range_chunks(
                    ReadLogicalRangeRequest {
                        ranges: vec![read_range],
                        manifest,
                        authz_scope: AuthzScopeRef {
                            anvil_storage_tenant_id: "system".to_string(),
                            authz_realm_id: "corestore".to_string(),
                        },
                        expected_boundary: None,
                        prefetch_policy: CorePrefetchPolicy::default(),
                        trace_context: CoreTraceContext::default(),
                    },
                    chunk_size,
                    on_chunk,
                )
                .await;
        }

        let manifest = self.read_object_manifest(&object_ref).await?;
        validate_manifest_for_object_ref(
            &manifest,
            &object_ref,
            strip_sha256_prefix(&object_ref.hash)?,
        )?;

        // A range read currently validates a complete systematic data shard.
        // Batch callbacks at shard boundaries so a small response chunk does
        // not cause the same shard to be read and hashed repeatedly.
        let data_shards = usize::from(manifest.encoding.data_shards);
        if data_shards == 0 {
            bail!("CoreStore range read requires at least one data shard");
        }
        if manifest.encoding.compression.algorithm != "none" {
            let batch = self
                .get_blob_range(GetBlobRange {
                    object_ref,
                    range: read_range,
                })
                .await?;
            for chunk in batch.chunks(chunk_size) {
                on_chunk(chunk.to_vec()).await?;
            }
            return Ok(());
        }
        let shard_len = object_ref.logical_size.div_ceil(data_shards as u64).max(1);
        let mut cursor = read_range.start;
        while cursor < read_range.end_exclusive {
            let shard_end = cursor
                .checked_div(shard_len)
                .and_then(|index| index.checked_add(1))
                .and_then(|index| index.checked_mul(shard_len))
                .unwrap_or(object_ref.logical_size)
                .min(object_ref.logical_size);
            let batch_end = shard_end.min(read_range.end_exclusive);
            let batch = self
                .get_blob_range(GetBlobRange {
                    object_ref: object_ref.clone(),
                    range: CoreByteRange {
                        start: cursor,
                        end_exclusive: batch_end,
                    },
                })
                .await?;
            for chunk in batch.chunks(chunk_size) {
                on_chunk(chunk.to_vec()).await?;
            }
            cursor = batch_end;
        }
        Ok(())
    }

    pub(super) fn read_inline_blob(&self, object_ref: &CoreObjectRef) -> Result<Vec<u8>> {
        if !is_inline_object_ref(object_ref) {
            bail!("CoreStore object ref is not an inline payload ref");
        }
        let manifest_hash = decode_manifest_ref(&object_ref.manifest_ref)?;
        let object_hash = strip_sha256_prefix(&object_ref.hash)?;
        if object_hash != manifest_hash {
            bail!("CoreStore inline object manifest ref/hash mismatch");
        }
        let tuple_key = inline_payload_meta_key(object_ref);
        // CoreMetaStore owns the specialised inline-payload decoder. The raw
        // value is not returned until the publication-aware row read succeeds.
        let bytes = self
            .meta
            .get_inline_payload(&tuple_key)?
            .ok_or_else(|| anyhow!("CoreStore inline payload row is missing"))?;
        let visible_payload = self
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)?
            .ok_or_else(|| anyhow!("CoreStore inline payload row is not published"))?;
        let visible_common = core_meta_row_common_from_payload(&visible_payload)?;
        if encode_core_meta_inline_payload_row(&bytes, visible_common)? != visible_payload {
            bail!("CoreStore inline payload row changed during publication-aware read");
        }
        if bytes.len() as u64 != object_ref.logical_size {
            bail!("CoreStore inline payload length mismatch");
        }
        if sha256_hex(&bytes) != object_hash {
            bail!("CoreStore inline payload hash mismatch");
        }
        Ok(bytes)
    }

    pub(super) fn embedded_logical_file_manifest_from_object_ref(
        object_ref: &CoreObjectRef,
    ) -> Result<Option<CoreLogicalFileManifest>> {
        let Some(encoded_manifest) = object_ref
            .manifest_ref
            .strip_prefix(CORE_LOGICAL_FILE_INLINE_REF_PREFIX)
        else {
            return Ok(None);
        };
        let manifest_bytes = URL_SAFE_NO_PAD.decode(encoded_manifest)?;
        let manifest = decode_logical_file_manifest_proto(&manifest_bytes)?;
        validate_logical_file_manifest_shape(&manifest)?;
        validate_logical_file_object_ref(&manifest, object_ref, "embedded")?;
        Ok(Some(manifest))
    }

    pub(super) async fn logical_file_manifest_from_object_ref(
        &self,
        object_ref: &CoreObjectRef,
    ) -> Result<Option<CoreLogicalFileManifest>> {
        if let Some(manifest) = Self::embedded_logical_file_manifest_from_object_ref(object_ref)? {
            return Ok(Some(manifest));
        }
        let Some(encoded_locator) = object_ref
            .manifest_ref
            .strip_prefix(CORE_LOGICAL_FILE_LOCATOR_REF_PREFIX)
        else {
            return Ok(None);
        };
        let locator_bytes = URL_SAFE_NO_PAD.decode(encoded_locator)?;
        let locator = decode_manifest_locator_proto(&locator_bytes)?;
        let manifest = Box::pin(self.read_logical_file_manifest(&locator)).await?;
        validate_logical_file_object_ref(&manifest, object_ref, "locator")?;
        Ok(Some(manifest))
    }

    pub(super) async fn get_blob_range_via_full_reconstruction(
        &self,
        input: GetBlobRange,
    ) -> Result<Vec<u8>> {
        let full = self
            .get_blob(GetBlob {
                object_ref: input.object_ref,
            })
            .await?;
        let start = usize::try_from(input.range.start)
            .map_err(|_| anyhow!("CoreStore range start exceeds usize"))?;
        let end = usize::try_from(input.range.end_exclusive)
            .map_err(|_| anyhow!("CoreStore range end exceeds usize"))?;
        Ok(full[start..end].to_vec())
    }

    pub async fn read_logical_range(&self, request: ReadLogicalRangeRequest) -> Result<Vec<u8>> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_logical_range")],
        );
        validate_logical_file_manifest_shape(&request.manifest)?;
        crate::perf::record_counter(
            "anvil_query_ranges_read_total",
            &[
                ("index_kind", request.manifest.writer_family.as_str()),
                ("writer_family", request.manifest.writer_family.as_str()),
            ],
            request.ranges.len() as u64,
        );
        let mut out = Vec::new();
        for range in request.ranges.iter().copied() {
            if let Some(expected_boundary) = request.expected_boundary.as_ref() {
                ensure_range_is_inside_expected_boundary(
                    &request.manifest,
                    &range,
                    expected_boundary,
                )?;
            }
            out.extend(
                Box::pin(self.read_logical_range_from_blocks(&request.manifest, range)).await?,
            );
        }
        self.schedule_logical_range_prefetch(&request);
        Ok(out)
    }

    pub async fn read_logical_range_chunks<F, Fut>(
        &self,
        request: ReadLogicalRangeRequest,
        chunk_size: usize,
        mut on_chunk: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_logical_range_chunks")],
        );
        validate_logical_file_manifest_shape(&request.manifest)?;
        if chunk_size == 0 {
            bail!("CoreStore logical range chunk size must be greater than zero");
        }
        crate::perf::record_counter(
            "anvil_query_ranges_read_total",
            &[
                ("index_kind", request.manifest.writer_family.as_str()),
                ("writer_family", request.manifest.writer_family.as_str()),
            ],
            request.ranges.len() as u64,
        );
        for range in request.ranges.iter().copied() {
            if let Some(expected_boundary) = request.expected_boundary.as_ref() {
                ensure_range_is_inside_expected_boundary(
                    &request.manifest,
                    &range,
                    expected_boundary,
                )?;
            }
            self.read_logical_range_from_blocks_chunked(
                &request.manifest,
                range,
                chunk_size,
                &mut on_chunk,
            )
            .await?;
        }
        self.schedule_logical_range_prefetch(&request);
        Ok(())
    }

    fn schedule_logical_range_prefetch(&self, request: &ReadLogicalRangeRequest) {
        if !request.prefetch_policy.enabled || request.prefetch_policy.max_ranges == 0 {
            return;
        }
        let requested_ids = request
            .ranges
            .iter()
            .filter_map(|range| {
                request
                    .manifest
                    .ranges
                    .iter()
                    .find(|candidate| {
                        candidate.byte_start == range.start
                            && candidate.byte_end == range.end_exclusive
                    })
                    .map(|range| range.range_id.clone())
            })
            .collect::<BTreeSet<_>>();
        let mut prefetches = Vec::new();
        for manifest_range in &request.manifest.ranges {
            if !requested_ids.contains(&manifest_range.range_id) {
                continue;
            }
            for next_id in &manifest_range.prefetch_next_range_ids {
                if prefetches.len() >= request.prefetch_policy.max_ranges as usize {
                    break;
                }
                if let Some(next) = request
                    .manifest
                    .ranges
                    .iter()
                    .find(|candidate| candidate.range_id == *next_id)
                {
                    prefetches.push(CoreByteRange {
                        start: next.byte_start,
                        end_exclusive: next.byte_end,
                    });
                }
            }
        }
        if prefetches.is_empty() {
            return;
        }
        let store = self.clone();
        let manifest = request.manifest.clone();
        tokio::spawn(async move {
            for range in prefetches {
                let started_at = Instant::now();
                let status = if store
                    .read_logical_range_from_blocks(&manifest, range)
                    .await
                    .is_ok()
                {
                    "ok"
                } else {
                    "error"
                };
                crate::perf::record_duration(
                    "anvil_block_read_duration_ms",
                    &[
                        ("node_id", "prefetch"),
                        ("region_id", "prefetch"),
                        ("cell_id", "prefetch"),
                        ("operation", "logical_range_prefetch"),
                        ("cache_status", "scheduled"),
                        ("status", status),
                    ],
                    started_at.elapsed(),
                );
            }
        });
    }

    pub async fn read_logical_file_manifest(
        &self,
        locator: &CoreManifestLocator,
    ) -> Result<CoreLogicalFileManifest> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_logical_file_manifest")],
        );
        validate_manifest_locator(locator)?;
        self.verify_manifest_locator_receipts(locator)?;
        let bytes = self.read_manifest_locator_bytes(locator).await?;
        if bytes.len() as u64 != locator.manifest_length {
            bail!("CoreStore manifest locator length mismatch");
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&bytes));
        if actual_hash != locator.manifest_hash || actual_hash != locator.manifest_ref.manifest_hash
        {
            bail!("CoreStore manifest locator hash mismatch");
        }
        let manifest = decode_logical_file_manifest_bytes(&bytes, &locator.manifest_encoding)?;
        validate_logical_file_manifest_shape(&manifest)?;
        if manifest.logical_file_id != locator.manifest_ref.logical_file_id
            || manifest.writer_family != locator.manifest_ref.writer_family
            || manifest.writer_generation != locator.manifest_ref.writer_generation
        {
            bail!("CoreStore manifest locator identity mismatch");
        }
        Ok(manifest)
    }

    pub(super) async fn read_manifest_locator_bytes(
        &self,
        locator: &CoreManifestLocator,
    ) -> Result<Vec<u8>> {
        if is_inline_manifest_body_locator(locator) {
            return self.read_inline_manifest_body(locator);
        }
        let mut out = Vec::with_capacity(locator.manifest_length as usize);
        for block in &locator.block_locators {
            let object_ref = object_ref_from_manifest_block_locator(block)?;
            let block_bytes = Box::pin(self.get_blob(GetBlob { object_ref })).await?;
            let expected_len = block.logical_end.saturating_sub(block.logical_start);
            if block_bytes.len() as u64 != expected_len {
                bail!("CoreStore manifest locator block length mismatch");
            }
            let block_hash = format!("sha256:{}", sha256_hex(&block_bytes));
            if block_hash != block.block_plain_hash {
                bail!("CoreStore manifest locator block plain hash mismatch");
            }
            out.extend_from_slice(&block_bytes);
        }
        Ok(out)
    }

    pub(super) fn read_inline_manifest_body(
        &self,
        locator: &CoreManifestLocator,
    ) -> Result<Vec<u8>> {
        validate_manifest_locator(locator)?;
        if !is_inline_manifest_body_locator(locator) {
            bail!("CoreStore manifest locator is not an inline manifest body locator");
        }
        let key = inline_manifest_body_key(&locator.manifest_hash)?;
        let body = if let Some(bytes) =
            self.meta
                .get(CF_TRANSACTIONS, TABLE_INLINE_MANIFEST_BODY_ROW, &key)?
        {
            let row = decode_inline_manifest_body_row(&bytes)?;
            if row.schema != CORE_INLINE_MANIFEST_BODY_SCHEMA
                || row.logical_file_id != locator.manifest_ref.logical_file_id
                || row.writer_family != locator.manifest_ref.writer_family
                || row.writer_generation != locator.manifest_ref.writer_generation
                || row.manifest_hash != locator.manifest_hash
                || row.manifest_encoding != locator.manifest_encoding
                || row.manifest_length != locator.manifest_length
            {
                bail!("CoreStore inline manifest body row identity mismatch");
            }
            row.body
        } else {
            let key = super::local_root_publication::transaction_manifest_body_key(
                &locator.manifest_hash,
            )?;
            let bytes = self
                .meta
                .get(CF_TRANSACTIONS, TABLE_TRANSACTION_MANIFEST_BODY_ROW, &key)?
                .ok_or_else(|| {
                    anyhow!(
                        "CoreStore inline manifest body row is missing for {}",
                        locator.manifest_hash
                    )
                })?;
            super::local_root_publication::validate_transaction_manifest_body_row(&bytes)?;
            let (common, body) =
                super::local_root_publication::decode_transaction_manifest_body_row(&bytes)?;
            if !self.root_generation_is_published(
                &common.root_key_hash,
                common.root_generation,
                &common.transaction_id,
            )? {
                bail!("CoreStore inline transaction manifest body is not published");
            }
            body
        };
        if body.len() as u64 != locator.manifest_length {
            bail!("CoreStore inline manifest body row length mismatch");
        }
        if body.len() > CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES {
            bail!("CoreStore inline manifest body row exceeds bounded CoreMeta size");
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&body));
        if actual_hash != locator.manifest_hash || actual_hash != locator.manifest_ref.manifest_hash
        {
            bail!("CoreStore inline manifest body row hash mismatch");
        }
        Ok(body)
    }

    pub(super) async fn read_logical_range_from_blocks(
        &self,
        manifest: &CoreLogicalFileManifest,
        range: CoreByteRange,
    ) -> Result<Vec<u8>> {
        if range.start > range.end_exclusive {
            bail!("CoreStore logical range start must be <= end_exclusive");
        }
        if range.end_exclusive > manifest.logical_size {
            bail!("CoreStore logical range exceeds logical file size");
        }
        if range.start == range.end_exclusive {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(
            usize::try_from(range.end_exclusive - range.start).unwrap_or(usize::MAX),
        );
        let mut blocks = manifest.blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|block| block.logical_offset);
        for block in blocks {
            let block_start = block.logical_offset;
            let block_end = block.logical_offset.saturating_add(block.logical_length);
            let overlap_start = range.start.max(block_start);
            let overlap_end = range.end_exclusive.min(block_end);
            if overlap_start >= overlap_end {
                continue;
            }
            if block.compression.algorithm == "none" && block.encryption.algorithm == "none" {
                let object_ref =
                    object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)?;
                out.extend(
                    Box::pin(self.get_blob_range(GetBlobRange {
                        object_ref,
                        range: CoreByteRange {
                            start: overlap_start - block_start,
                            end_exclusive: overlap_end - block_start,
                        },
                    }))
                    .await?,
                );
            } else {
                let block_plaintext = self.read_logical_block_plaintext(manifest, block).await?;
                let start = usize::try_from(overlap_start - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range start exceeds usize"))?;
                let end = usize::try_from(overlap_end - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range end exceeds usize"))?;
                out.extend_from_slice(&block_plaintext[start..end]);
            }
        }
        Ok(out)
    }

    pub(super) async fn read_logical_range_from_blocks_chunked<F, Fut>(
        &self,
        manifest: &CoreLogicalFileManifest,
        range: CoreByteRange,
        chunk_size: usize,
        on_chunk: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        if range.start > range.end_exclusive {
            bail!("CoreStore logical range start must be <= end_exclusive");
        }
        if range.end_exclusive > manifest.logical_size {
            bail!("CoreStore logical range exceeds logical file size");
        }
        if range.start == range.end_exclusive {
            return Ok(());
        }
        if chunk_size == 0 {
            bail!("CoreStore logical range chunk size must be greater than zero");
        }

        let mut blocks = manifest.blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|block| block.logical_offset);
        for block in blocks {
            let block_start = block.logical_offset;
            let block_end = block.logical_offset.saturating_add(block.logical_length);
            let overlap_start = range.start.max(block_start);
            let overlap_end = range.end_exclusive.min(block_end);
            if overlap_start >= overlap_end {
                continue;
            }
            if block.compression.algorithm == "none" && block.encryption.algorithm == "none" {
                let object_ref =
                    object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)?;
                let mut cursor = overlap_start - block_start;
                let end = overlap_end - block_start;
                while cursor < end {
                    let next = (cursor + chunk_size as u64).min(end);
                    let chunk = Box::pin(self.get_blob_range(GetBlobRange {
                        object_ref: object_ref.clone(),
                        range: CoreByteRange {
                            start: cursor,
                            end_exclusive: next,
                        },
                    }))
                    .await?;
                    on_chunk(chunk).await?;
                    cursor = next;
                }
            } else {
                let block_plaintext = self.read_logical_block_plaintext(manifest, block).await?;
                let start = usize::try_from(overlap_start - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range start exceeds usize"))?;
                let end = usize::try_from(overlap_end - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range end exceeds usize"))?;
                for chunk in block_plaintext[start..end].chunks(chunk_size) {
                    on_chunk(chunk.to_vec()).await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn read_logical_file_plaintext(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> Result<Vec<u8>> {
        validate_logical_file_manifest_shape(manifest)?;
        let mut blocks = manifest.blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|block| block.logical_offset);
        let mut plaintext = Vec::with_capacity(usize::try_from(manifest.logical_size).unwrap_or(0));
        for block in blocks {
            plaintext.extend(Box::pin(self.read_logical_block_plaintext(manifest, block)).await?);
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&plaintext));
        if actual_hash != manifest.content_hash {
            bail!(
                "CoreStore logical file content hash mismatch: expected {}, got {}",
                manifest.content_hash,
                actual_hash
            );
        }
        Ok(plaintext)
    }

    pub(super) async fn read_logical_block_plaintext(
        &self,
        manifest: &CoreLogicalFileManifest,
        block: &CoreLogicalBlockRef,
    ) -> Result<Vec<u8>> {
        let object_ref = object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)?;
        let stored = Box::pin(self.get_blob(GetBlob { object_ref })).await?;
        let compressed = self.decrypt_pipeline_block(&manifest.logical_file_id, block, stored)?;
        if compressed.len() as u64 != block.compressed_length {
            bail!(
                "CoreStore decrypted block length mismatch: expected {}, got {}",
                block.compressed_length,
                compressed.len()
            );
        }
        let compressed_hash = format!("sha256:{}", sha256_hex(&compressed));
        if compressed_hash != block.encryption.plaintext_hash {
            bail!(
                "CoreStore decrypted block hash mismatch: expected {}, got {}",
                block.encryption.plaintext_hash,
                compressed_hash
            );
        }
        let plaintext = decode_logical_file_source(&block.compression.algorithm, compressed)?;
        if plaintext.len() as u64 != block.logical_length {
            bail!(
                "CoreStore decoded block length mismatch: expected {}, got {}",
                block.logical_length,
                plaintext.len()
            );
        }
        let actual_plaintext_hash = format!("sha256:{}", sha256_hex(&plaintext));
        if actual_plaintext_hash != block.content_hash {
            bail!(
                "CoreStore logical block content hash mismatch: expected {}, got {}",
                block.content_hash,
                actual_plaintext_hash
            );
        }
        Ok(plaintext)
    }
}
fn validate_logical_file_object_ref(
    manifest: &CoreLogicalFileManifest,
    object_ref: &CoreObjectRef,
    ref_kind: &str,
) -> Result<()> {
    if manifest.content_hash != object_ref.hash {
        bail!("CoreStore {ref_kind} logical file object ref hash mismatch");
    }
    if manifest.logical_size != object_ref.logical_size {
        bail!("CoreStore {ref_kind} logical file object ref size mismatch");
    }
    Ok(())
}
