use super::local_tx_rows::{OwnedCoreMetaBatchOp, borrow_owned_coremeta_batch_ops};
use super::*;
use crate::formats::{
    hash32,
    writer::{WriterFamily, canonical_logical_file_id},
};

#[path = "local_admission/finalisation.rs"]
mod finalisation;
#[path = "local_admission/point_state.rs"]
mod point_state;
#[path = "local_admission/publication.rs"]
mod publication;

use point_state::*;

impl CoreStore {
    pub(super) async fn admit_core_mutation(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: CorePendingMutationTarget,
        mutation_id: String,
        idempotency_key: Option<String>,
        payload: CorePendingMutationPayload<'_>,
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CorePendingMutationRecord> {
        let mutation_id_for_error = mutation_id.clone();
        match self
            .admit_core_mutation_outcome(
                operation_family,
                writer_family,
                target,
                mutation_id,
                idempotency_key,
                payload,
                boundary_values,
            )
            .await?
        {
            CoreAdmissionOutcome::Pending(record) => Ok(record),
            CoreAdmissionOutcome::Finalised(finalisation) => bail!(
                "CoreStore mutation {mutation_id_for_error} has already been finalised in state {}",
                finalisation.state
            ),
        }
    }

    pub(super) async fn admit_core_mutation_outcome(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: CorePendingMutationTarget,
        mutation_id: String,
        idempotency_key: Option<String>,
        payload: CorePendingMutationPayload<'_>,
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CoreAdmissionOutcome> {
        let admission_started_at = Instant::now();
        let boundary_schema_generation = boundary_values
            .iter()
            .map(|value| value.schema_generation)
            .max()
            .unwrap_or(0)
            .to_string();
        record_corestore_trace_event("admission.validate", "ok");
        record_corestore_trace_event("admission.boundary_extract", "ok");
        validate_writer_family(writer_family, "pending_mutation writer family")?;
        validate_logical_id(&mutation_id, "pending_mutation mutation id")?;
        let admission_shard = target.admission_shard();
        let mut landed_source_bytes = None;
        let mut landed_guard = None;
        let (inline_payload, landed_bytes) = match payload {
            CorePendingMutationPayload::Empty => (Vec::new(), Vec::new()),
            CorePendingMutationPayload::Inline(bytes)
                if bytes.len() <= CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES =>
            {
                (bytes.to_vec(), Vec::new())
            }
            CorePendingMutationPayload::Inline(bytes)
            | CorePendingMutationPayload::Landed(bytes) => {
                landed_source_bytes = Some(bytes);
                let hash = sha256_hex(bytes);
                landed_guard = Some(self.acquire_named_lock("landed-bytes", &hash).await?);
                let landed = self
                    .land_core_bytes_unlocked(bytes, &admission_shard.hash, &mutation_id)
                    .await?;
                (Vec::new(), vec![landed])
            }
        };
        let result = self
            .append_core_pending_mutation_record(
                operation_family,
                writer_family,
                target,
                mutation_id,
                idempotency_key,
                landed_bytes,
                &inline_payload,
                boundary_values,
            )
            .await;
        record_admission_duration(
            operation_family,
            writer_family,
            &boundary_schema_generation,
            if result.is_ok() { "ok" } else { "error" },
            admission_started_at.elapsed(),
        );
        let outcome = result?;
        if let (Some(bytes), CoreAdmissionOutcome::Pending(record)) =
            (landed_source_bytes, &outcome)
        {
            for landed in &record.landed_bytes {
                let hash = strip_sha256_prefix(&landed.sha256)?.to_string();
                let final_path = self.landed_bytes_path(&hash);
                self.ensure_landed_bytes_file(&final_path, bytes, &hash)
                    .await
                    .with_context(|| {
                        format!(
                            "verify CoreStore landed bytes after pending record commit landing_id={} path={}",
                            landed.landing_id,
                            final_path.display()
                        )
                })?;
            }
        }
        drop(landed_guard);
        Ok(outcome)
    }

    async fn land_core_bytes_unlocked(
        &self,
        bytes: &[u8],
        admission_shard_hash: &str,
        mutation_id: &str,
    ) -> Result<CorePendingLandedByte> {
        let hash = sha256_hex(bytes);
        let final_path = self.landed_bytes_path(&hash);
        let landing_id = format!("{admission_shard_hash}:{mutation_id}:{hash}");
        self.ensure_landed_bytes_file(&final_path, bytes, &hash)
            .await?;
        let relative_path = self.storage.relative_storage_path(&final_path)?;
        let landed = CorePendingLandedByte {
            sha256: format!("sha256:{hash}"),
            length: bytes.len() as u64,
            landing_id,
            relative_path,
        };
        Ok(landed)
    }

    async fn ensure_landed_bytes_file(
        &self,
        final_path: &std::path::Path,
        bytes: &[u8],
        hash: &str,
    ) -> Result<()> {
        match fs::metadata(final_path).await {
            Ok(metadata) => {
                if metadata.len() != bytes.len() as u64 {
                    bail!("CoreStore landed bytes existing length mismatch");
                }
                let existing = read_file(
                    &final_path.to_path_buf(),
                    "core_store",
                    "landed_file_verify_existing",
                )
                .await?;
                if sha256_hex(&existing) != hash {
                    bail!("CoreStore landed bytes existing hash mismatch");
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                let tmp_path =
                    final_path.with_extension(format!("landed.{}.tmp", uuid::Uuid::new_v4()));
                let started_at = Instant::now();
                let mut file = fs::File::create(&tmp_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_create",
                    &tmp_path,
                    0,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.write_all(bytes).await?;
                record_landed_bytes_duration(
                    "write",
                    "ok",
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                record_corestore_trace_event("admission.landed_write", "ok");
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_write",
                    &tmp_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.sync_all().await?;
                let elapsed = started_at.elapsed();
                record_landed_bytes_duration("fsync", "ok", bytes.len() as u64, elapsed);
                record_corestore_trace_event("admission.landed_fsync", "ok");
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_sync",
                    &tmp_path,
                    bytes.len() as u64,
                    elapsed,
                );
                crate::perf::record_fsync_duration(
                    "core_store",
                    "landed_bytes",
                    "landed_file_sync",
                    elapsed,
                );
                drop(file);
                let started_at = Instant::now();
                fs::rename(&tmp_path, final_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_rename",
                    final_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                sync_parent_dir(&final_path.to_path_buf(), "landed_file_sync_parent_dir").await?;
                Ok(())
            }
            Err(err) => Err(err).with_context(|| {
                format!("inspect CoreStore landed bytes {}", final_path.display())
            }),
        }
    }

    pub(super) async fn enforce_admission_capacity_with_limits(
        &self,
        admission_shard_hash: &str,
        incoming_pending_mutation_bytes: u64,
        incoming_landed_bytes: u64,
        limits: CoreAdmissionCapacityLimits,
    ) -> Result<()> {
        let state = self.load_admission_point_state_foreground(admission_shard_hash)?;
        self.enforce_admission_capacity_for_state(
            &state,
            incoming_pending_mutation_bytes,
            incoming_landed_bytes,
            limits,
        )
        .await
    }

    async fn enforce_admission_capacity_for_state(
        &self,
        state: &AdmissionPointState,
        incoming_pending_mutation_bytes: u64,
        incoming_landed_bytes: u64,
        limits: CoreAdmissionCapacityLimits,
    ) -> Result<()> {
        let pending_mutation_rows = state.pending_rows;
        let pending_mutation_bytes = state.pending_bytes;
        let landed_bytes = state.landed_bytes;
        let projected_pending_mutation_rows = pending_mutation_rows.saturating_add(1);
        let projected_pending_mutation_bytes =
            pending_mutation_bytes.saturating_add(incoming_pending_mutation_bytes);
        let projected_landed_bytes = landed_bytes.saturating_add(incoming_landed_bytes);
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_rows",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(pending_mutation_rows).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_rows",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_pending_mutation_rows).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_bytes",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(pending_mutation_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_bytes",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_pending_mutation_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_landed_bytes_pending",
            &[
                ("kind", "landed_bytes"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(landed_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_landed_bytes_pending",
            &[
                ("kind", "landed_bytes"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_landed_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_pending_state(
            &self.node_identity.node_id,
            "pending_mutations",
            pending_mutation_rows,
            pending_mutation_bytes,
            landed_bytes,
        );

        if projected_pending_mutation_rows > limits.pending_mutation_hard_limit_rows {
            bail!(
                "{}: CoreStore admission pending mutation row hard limit exceeded: current={}, projected={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                pending_mutation_rows,
                projected_pending_mutation_rows,
                limits.pending_mutation_hard_limit_rows
            );
        }

        if projected_pending_mutation_bytes > limits.pending_mutation_hard_limit_bytes {
            bail!(
                "{}: CoreStore admission pending mutation byte hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                pending_mutation_bytes,
                incoming_pending_mutation_bytes,
                limits.pending_mutation_hard_limit_bytes
            );
        }

        if projected_landed_bytes > limits.landed_bytes_hard_limit_bytes {
            bail!(
                "{}: CoreStore landed bytes hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                landed_bytes,
                incoming_landed_bytes,
                limits.landed_bytes_hard_limit_bytes
            );
        }

        let pending_mutation_lag_seconds = state.lag_seconds(unix_timestamp_nanos());
        if let Some(lag_seconds) = pending_mutation_lag_seconds {
            crate::perf::record_gauge(
                "anvil_materialisation_lag_seconds",
                &[("queue", "pending_mutations"), ("status", "measured")],
                i64::try_from(lag_seconds).unwrap_or(i64::MAX),
            );
            crate::perf::record_materialisation_lag_ms(
                "pending_mutations",
                lag_seconds.saturating_mul(1_000),
            );
        }
        if let Some(lag_seconds) = pending_mutation_lag_seconds
            && lag_seconds > limits.pending_mutation_hard_lag_seconds
        {
            bail!(
                "{}: CoreStore pending mutation materialisation lag hard limit exceeded: lag_seconds={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                lag_seconds,
                limits.pending_mutation_hard_lag_seconds
            );
        }

        if projected_pending_mutation_rows > limits.pending_mutation_soft_limit_rows
            || projected_pending_mutation_bytes > limits.pending_mutation_soft_limit_bytes
            || projected_landed_bytes > limits.landed_bytes_soft_limit_bytes
            || pending_mutation_lag_seconds
                .is_some_and(|lag_seconds| lag_seconds > limits.pending_mutation_soft_lag_seconds)
        {
            tokio::time::sleep(CORE_PENDING_MUTATION_SOFT_BACKPRESSURE_DELAY).await;
        }

        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn pending_mutation_count(&self) -> Result<u64> {
        Ok(self.admission_accounting_totals_for_tests()?.0)
    }

    pub(super) fn has_pending_mutations(&self) -> Result<bool> {
        Ok(!self.read_all_pending_mutation_page(None, 1)?.is_empty())
    }

    #[cfg(test)]
    pub(super) async fn pending_mutation_bytes(&self) -> Result<u64> {
        Ok(self.admission_accounting_totals_for_tests()?.1)
    }

    #[cfg(test)]
    pub(super) async fn admission_landed_bytes(&self) -> Result<u64> {
        Ok(self.admission_accounting_totals_for_tests()?.2)
    }

    #[cfg(test)]
    pub(super) async fn admission_materialisation_lag_seconds(&self) -> Result<Option<u64>> {
        Ok(self.admission_accounting_totals_for_tests()?.3)
    }

    #[cfg(test)]
    pub(super) fn admission_point_state_for_tests(
        &self,
        admission_shard_hash: &str,
    ) -> Result<(u64, u64, u64, u64, Option<u64>, Option<u64>)> {
        let state = self.load_admission_point_state_foreground(admission_shard_hash)?;
        Ok((
            state.last_sequence,
            state.pending_rows,
            state.pending_bytes,
            state.landed_bytes,
            state.oldest_pending_sequence,
            state.oldest_pending_created_at_unix_nanos,
        ))
    }

    #[cfg(test)]
    pub(super) fn landed_byte_reference_count_for_tests(
        &self,
        admission_shard_hash: &str,
        sha256: &str,
    ) -> Result<Option<u64>> {
        Ok(self
            .read_landed_byte_head(admission_shard_hash, sha256)?
            .map(|head| head.reference_count))
    }

    #[cfg(test)]
    pub(super) fn validate_admission_recovery_state_for_tests(&self) -> Result<usize> {
        Ok(self.validate_admission_recovery_state()?.len())
    }

    #[cfg(test)]
    pub(super) async fn read_pending_mutation_records_with_payload(
        &self,
    ) -> Result<Vec<(CorePendingMutationRecord, Vec<u8>)>> {
        let mut out = Vec::new();
        let mut after = None;
        loop {
            let page = self
                .read_all_pending_mutation_page(after.as_deref(), ADMISSION_RECOVERY_PAGE_ROWS)?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            after = page.last().map(|row| row.tuple_key.clone());
            out.extend(page.into_iter().map(|row| (row.record, row.inline_payload)));
            if page_len < ADMISSION_RECOVERY_PAGE_ROWS {
                break;
            }
        }
        out.sort_by_key(|(record, _)| (record.target.admission_shard().hash, record.sequence));
        Ok(out)
    }

    pub(in crate::core_store::local) async fn publish_pending_mutation_finalisation_transaction_record_locally(
        &self,
        finalisation: &CorePendingMutationFinalisationRecord,
    ) -> Result<CorePendingMutationFinalisationRecord> {
        if let Some(existing) = self
            .read_published_pending_mutation_finalisation(finalisation)
            .await?
        {
            return Ok(existing);
        }

        if finalisation.finalised_at_unix_nanos != 0 {
            bail!(
                "CoreStore pending mutation finalisation proposal already has an owner timestamp"
            );
        }
        // Finalisations from unrelated mutations can run concurrently. They
        // still append to one root-anchored stream, so sequence allocation and
        // root publication must share the same cross-process stream lock as a
        // public append.
        let _stream_guard = self
            .acquire_named_lock("stream", CORE_TRANSACTION_STREAM_ID)
            .await?;
        if let Some(existing) = self
            .read_published_pending_mutation_finalisation(finalisation)
            .await?
        {
            return Ok(existing);
        }
        let mut canonical = finalisation.clone();
        canonical.finalised_at_unix_nanos = unix_timestamp_nanos();
        let payload = encode_pending_mutation_finalisation_record(&canonical)?;
        let append_result = Box::pin(self.append_stream_unlocked(AppendStreamRecord {
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            partition_id: "system/core-control".to_string(),
            record_kind: CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND.to_string(),
            payload,
            content_type: Some("application/protobuf".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            // This event records the outcome of the source mutation; it is not
            // another write in that mutation's publication plan. Reusing the
            // source mutation id here lets two distinct root plans collide if
            // the first plan is being resumed after a lost acknowledgement.
            transaction_id: None,
            idempotency_key: Some(pending_mutation_finalisation_idempotency_key(&canonical)),
        }))
        .await;
        match append_result {
            Ok(_) => Ok(canonical),
            Err(append_error) => {
                // The stream publication may have committed while the caller
                // lost its acknowledgement or before the shard-local marker
                // landed. Recover the canonical event rather than rebuilding
                // it with a different wall-clock timestamp.
                if let Some(existing) = self
                    .read_published_pending_mutation_finalisation(finalisation)
                    .await?
                {
                    return Ok(existing);
                }
                Err(append_error)
            }
        }
    }

    async fn read_published_pending_mutation_finalisation(
        &self,
        requested: &CorePendingMutationFinalisationRecord,
    ) -> Result<Option<CorePendingMutationFinalisationRecord>> {
        let idempotency_key = pending_mutation_finalisation_idempotency_key(requested);
        let Some(record) = self
            .read_stream_record_by_idempotency_key(CORE_TRANSACTION_STREAM_ID, &idempotency_key)
            .await?
        else {
            return Ok(None);
        };
        if record.record_kind != CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND
            || record.transaction_id.is_some()
        {
            bail!("CoreStore pending mutation finalisation stream identity conflict");
        }
        let existing = decode_pending_mutation_finalisation_record(&record.payload)?;
        if !same_pending_mutation_finalisation(requested, &existing) {
            bail!(
                "CoreStore pending mutation finalisation stream payload conflicts with source identity"
            );
        }
        Ok(Some(existing))
    }

    pub(super) async fn read_landed_bytes(
        &self,
        landed: &CorePendingLandedByte,
    ) -> Result<Vec<u8>> {
        validate_hash(&landed.sha256, "landed bytes hash")?;
        let (admission_shard_hash, mutation_id) = landed_admission_scope(&landed.landing_id)?;
        self.verify_landed_bytes_ref_row(
            admission_shard_hash,
            &landed.landing_id,
            mutation_id,
            &landed.sha256,
            landed.length,
            &[],
        )?;
        let path = self
            .storage
            .resolve_relative_storage_path(&landed.relative_path)?;
        let bytes = read_file(&path, "core_store", "read_landed_bytes")
            .await
            .with_context(|| {
                format!(
                    "read CoreStore landed bytes landing_id={} relative_path={} resolved_path={}",
                    landed.landing_id,
                    landed.relative_path,
                    path.display()
                )
            })?;
        if bytes.len() as u64 != landed.length {
            bail!("CoreStore landed bytes length mismatch");
        }
        let actual = format!("sha256:{}", sha256_hex(&bytes));
        if actual != landed.sha256 {
            bail!("CoreStore landed bytes hash mismatch");
        }
        Ok(bytes)
    }

    pub(super) async fn recover_pending_mutations(
        &self,
        _startup_guard: &tokio::sync::MutexGuard<'_, ()>,
    ) -> Result<()> {
        let recovery_started_at = Instant::now();
        let _recovery_guard = self
            .acquire_named_lock("pending_mutation", "recovery")
            .await?;
        let result = async {
            let referenced_landed_bytes = self.validate_admission_recovery_state()?;
            self.reconcile_landed_bytes_after_rocksdb_recovery(&referenced_landed_bytes)
                .await?;
            let mut after = None;
            loop {
                let page = self.read_all_pending_mutation_page(
                    after.as_deref(),
                    ADMISSION_RECOVERY_PAGE_ROWS,
                )?;
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|row| row.tuple_key.clone());
                for row in page {
                    let record = row.record;
                    let payload = row.inline_payload;
                    let record_key = CorePendingMutationKey::from(&record);
                    let pending_mutation_hash_input =
                        encode_pending_mutation_hash_input(&record, &payload)?;
                    if let Err(error) =
                        self.verify_local_admission_evidence(&record, &pending_mutation_hash_input)
                    {
                        if self
                            .wait_for_pending_mutation_finalisation(&record_key)
                            .await?
                        {
                            continue;
                        }
                        return Err(error);
                    }
                    let replay = match self
                        .replay_pending_mutation_record_unlocked(&record, &payload)
                        .await
                    {
                        Ok(replay) => replay,
                        Err(error) => {
                            if self
                                .wait_for_pending_mutation_finalisation(&record_key)
                                .await?
                            {
                                continue;
                            }
                            return Err(error).with_context(|| {
                                format!(
                                    "replay CoreStore pending mutation mutation {} sequence {}",
                                    record.mutation_id, record.sequence
                                )
                            });
                        }
                    };
                    if let Err(error) = self
                        .mark_pending_mutation_finalised_with_result_unlocked(
                            &record,
                            replay.state,
                            replay.result,
                        )
                        .await
                    {
                        if self
                            .wait_for_pending_mutation_finalisation(&record_key)
                            .await?
                        {
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
            Ok(())
        }
        .await;
        crate::perf::record_recovery_duration(
            "pending_mutations",
            if result.is_ok() { "ok" } else { "error" },
            recovery_started_at.elapsed(),
        );
        result
    }

    pub(super) async fn wait_for_pending_mutation_finalisation(
        &self,
        key: &CorePendingMutationKey,
    ) -> Result<bool> {
        for _ in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            if self
                .pending_mutation_finalisation_index_point(key)?
                .is_some()
            {
                return Ok(true);
            }
            tokio::time::sleep(CORE_PROCESS_LOCK_RETRY_DELAY).await;
        }
        Ok(false)
    }

    pub(super) async fn replay_pending_mutation_record_unlocked(
        &self,
        record: &CorePendingMutationRecord,
        payload: &[u8],
    ) -> Result<CorePendingMutationReplayOutcome> {
        match &record.target {
            CorePendingMutationTarget::ObjectPut {
                logical_name,
                erasure_profile_id,
                encryption,
                block_plain_hash,
                object_hash,
                object_logical_size,
                compression,
                writer_generation,
                block_ordinal,
                logical_offset,
                region_id: _,
            } => {
                if record.operation_family != "object.put" {
                    bail!("CoreStore pending mutation target/family mismatch for object.put");
                }
                let profile = local_erasure_profile(erasure_profile_id)?;
                let materialised_bytes =
                    self.pending_mutation_payload_bytes(record, payload).await?;
                let hash = sha256_hex(&materialised_bytes);
                if let Some(landed) = record.landed_bytes.first() {
                    let landed_hash = strip_sha256_prefix(&landed.sha256)?;
                    if landed_hash != hash {
                        bail!("CoreStore pending mutation object.put landed hash mismatch");
                    }
                }
                let writer = WriterFamily::from_name(&record.writer_family).ok_or_else(|| {
                    anyhow!(
                        "CoreStore pending mutation unknown writer family {}",
                        record.writer_family
                    )
                })?;
                let replay_logical_file_id =
                    if super::local_logical_files::is_canonical_logical_file_id(logical_name) {
                        logical_name.clone()
                    } else {
                        canonical_logical_file_id(
                            writer,
                            *writer_generation,
                            logical_name,
                            &hash32(&materialised_bytes),
                        )
                    };
                let materialisation = self
                    .materialise_object_blob_bytes(
                        &replay_logical_file_id,
                        *writer_generation,
                        *block_ordinal,
                        *logical_offset,
                        block_plain_hash,
                        &hash,
                        &materialised_bytes,
                        object_hash,
                        *object_logical_size,
                        compression.clone(),
                        &record.boundary_values,
                        &record.mutation_id,
                        profile,
                        encryption,
                        &record.writer_family,
                    )
                    .await;
                match materialisation {
                    Ok(object_ref) => Ok(CorePendingMutationReplayOutcome {
                        state: "committed",
                        result: Some(CorePendingMutationFinalisationResult::ObjectRef(object_ref)),
                    }),
                    Err(error) => {
                        if super::local_root_publication_recovery::publication_terminal_reason(
                            &error,
                        ) == Some("PublicationSupersededByCommittedRoot")
                        {
                            return Ok(CorePendingMutationReplayOutcome {
                                state: "superseded",
                                result: None,
                            });
                        }
                        Err(error)
                    }
                }
            }
            CorePendingMutationTarget::StreamAppend {
                stream_id,
                partition_id,
                record_kind,
                transaction_id,
            } => {
                if record.operation_family != "stream.append" {
                    bail!("CoreStore pending mutation target/family mismatch for stream.append");
                }
                let payload = self.pending_mutation_payload_bytes(record, payload).await?;
                let outcome = self
                    .append_stream_unlocked_with_idempotency_hash(
                        AppendStreamRecord {
                            stream_id: stream_id.clone(),
                            partition_id: partition_id.clone(),
                            record_kind: record_kind.clone(),
                            payload,
                            content_type: None,
                            user_metadata_json: "{}".to_string(),
                            fence: None,
                            transaction_id: transaction_id.clone(),
                            idempotency_key: None,
                        },
                        record.idempotency_key_hash.clone(),
                    )
                    .await?;
                Ok(CorePendingMutationReplayOutcome {
                    state: "committed",
                    result: outcome.state_locator.as_ref().map(|locator| {
                        CorePendingMutationFinalisationResult::StreamStateLocator(locator.clone())
                    }),
                })
            }
            CorePendingMutationTarget::MutationBatch { .. } => {
                if record.operation_family != "mutation.batch" {
                    bail!("CoreStore pending mutation target/family mismatch for mutation.batch");
                }
                let payload = self.pending_mutation_payload_bytes(record, payload).await?;
                let batch = decode_core_mutation_batch(&payload)?;
                let receipt = self.recover_admitted_mutation_batch(batch, record).await?;
                Ok(CorePendingMutationReplayOutcome {
                    state: core_transaction_state_name(receipt.state),
                    result: None,
                })
            }
        }
    }

    pub(in crate::core_store::local) async fn reconcile_landed_bytes_after_rocksdb_recovery(
        &self,
        referenced_relative_paths: &BTreeMap<String, (String, u64)>,
    ) -> Result<()> {
        let mut missing_referenced_paths = referenced_relative_paths.clone();
        let files = collect_landed_byte_files(&self.storage.core_store_landed_bytes_path()).await?;
        for path in files {
            let relative = self.storage.relative_storage_path(&path)?;
            if let Some((expected_hash, expected_len)) = referenced_relative_paths.get(&relative) {
                let actual = read_file(&path, "core_store", "landed_recovery_read").await?;
                if format!("sha256:{}", sha256_hex(&actual)) != *expected_hash
                    || actual.len() as u64 != *expected_len
                {
                    bail!("CoreStore referenced landed bytes failed recovery validation");
                }
                missing_referenced_paths.remove(&relative);
                continue;
            }
            let hash = landed_bytes_file_hash(&path)?;
            let _landed_guard = self.acquire_named_lock("landed-bytes", &hash).await?;
            let actual = read_file(&path, "core_store", "landed_recovery_recheck").await?;
            let actual_hash = format!("sha256:{}", sha256_hex(&actual));
            if actual_hash != format!("sha256:{hash}") {
                bail!("CoreStore unreferenced landed bytes filename/hash mismatch");
            }
            if self.live_landed_bytes_reference(&relative, &actual_hash, actual.len() as u64)? {
                continue;
            }
            quarantine_landed_bytes_file(&path).await?;
        }
        if !missing_referenced_paths.is_empty() {
            bail!("CoreStore recovery found a missing referenced landed byte file");
        }
        Ok(())
    }

    fn live_landed_bytes_reference(
        &self,
        relative_path: &str,
        expected_hash: &str,
        expected_len: u64,
    ) -> Result<bool> {
        let prefix = landed_byte_ref_prefix();
        let mut after = None;
        loop {
            let rows = self.meta.scan_prefix_page(
                CF_MATERIALISATION,
                TABLE_LANDED_BYTE_REF_ROW,
                &prefix,
                after.as_deref(),
                ADMISSION_RECOVERY_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                return Ok(false);
            }
            for record in &rows {
                let row = decode_landed_byte_ref_row(&record.payload)?;
                if core_meta_record_tuple_key(&record.key)?
                    != landed_byte_ref_key(&row.admission_shard_hash, &row.landed.landing_id)
                        .as_slice()
                {
                    bail!("CoreStore landed byte reference row has invalid key scope");
                }
                if row.landed.relative_path != relative_path {
                    continue;
                }
                if row.landed.sha256 != expected_hash || row.landed.length != expected_len {
                    bail!("CoreStore live landed byte reference descriptor mismatch");
                }
                return Ok(true);
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(|key| key.to_vec()))
                .transpose()?;
            if rows.len() < ADMISSION_RECOVERY_PAGE_ROWS {
                return Ok(false);
            }
        }
    }

    pub(super) async fn pending_mutation_payload_bytes(
        &self,
        record: &CorePendingMutationRecord,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        if !payload.is_empty() {
            return Ok(payload.to_vec());
        }
        let mut bytes = Vec::new();
        for landed in &record.landed_bytes {
            bytes.extend_from_slice(&self.read_landed_bytes(landed).await?);
        }
        Ok(bytes)
    }

    pub(super) async fn next_core_mutation_sequence(
        &self,
        target: &CorePendingMutationTarget,
    ) -> Result<u64> {
        self.load_admission_point_state_foreground(&target.admission_shard().hash)?
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore pending mutation sequence overflow"))
    }
}

fn pending_mutation_finalisation_idempotency_key(
    finalisation: &CorePendingMutationFinalisationRecord,
) -> String {
    format!(
        "pending-finalisation:{}:{}:{}:{}",
        finalisation.target.admission_shard().hash,
        finalisation.node_id,
        finalisation.mutation_epoch,
        finalisation.mutation_sequence
    )
}

pub(in crate::core_store::local) fn same_pending_mutation_finalisation(
    requested: &CorePendingMutationFinalisationRecord,
    existing: &CorePendingMutationFinalisationRecord,
) -> bool {
    let mut requested = requested.clone();
    let mut existing = existing.clone();
    requested.finalised_at_unix_nanos = 0;
    existing.finalised_at_unix_nanos = 0;
    requested == existing
}

impl CoreStore {
    pub(super) fn verify_landed_bytes_ref_row(
        &self,
        admission_shard_hash: &str,
        landing_id: &str,
        mutation_id: &str,
        sha256: &str,
        length: u64,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<()> {
        let landed_key = landed_byte_ref_key(admission_shard_hash, landing_id);
        // Landed-byte references are node-local admission staging and must be
        // verified before any rooted mutation is eligible for publication.
        let Some(bytes) =
            self.meta
                .get(CF_MATERIALISATION, TABLE_LANDED_BYTE_REF_ROW, &landed_key)?
        else {
            bail!("CoreStore landed byte CoreMeta row is missing");
        };
        self.verify_landed_bytes_ref_payload(
            admission_shard_hash,
            landing_id,
            mutation_id,
            sha256,
            length,
            boundary_values,
            &bytes,
        )
    }

    pub(in crate::core_store::local) fn verify_landed_bytes_ref_payload(
        &self,
        admission_shard_hash: &str,
        landing_id: &str,
        mutation_id: &str,
        sha256: &str,
        length: u64,
        boundary_values: &[CoreBoundaryValue],
        bytes: &[u8],
    ) -> Result<()> {
        let row = decode_landed_byte_ref_row(bytes)?;
        if row.admission_shard_hash != admission_shard_hash || row.admission_sequence == 0 {
            bail!("CoreStore landed byte CoreMeta row has invalid admission scope");
        }
        if !mutation_id.is_empty() && row.mutation_id != mutation_id {
            bail!("CoreStore landed byte CoreMeta mutation id mismatch");
        }
        if row.landed.landing_id != landing_id {
            bail!("CoreStore landed byte CoreMeta landing id mismatch");
        }
        if row.landed.sha256 != sha256 {
            bail!("CoreStore landed byte CoreMeta hash mismatch");
        }
        if row.landed.length != length {
            bail!("CoreStore landed byte CoreMeta length mismatch");
        }
        if !boundary_values.is_empty()
            && boundary_summary_hash(&row.boundary_values)?
                != boundary_summary_hash(boundary_values)?
        {
            bail!("CoreStore landed byte CoreMeta boundary summary mismatch");
        }
        Ok(())
    }
}

fn landed_admission_scope(landing_id: &str) -> Result<(&str, &str)> {
    const SHA256_TEXT_LENGTH: usize = "sha256:".len() + 64;
    if landing_id.len() <= SHA256_TEXT_LENGTH + 2 {
        bail!("CoreStore landed byte landing id is missing admission scope");
    }
    let (shard_hash, remainder) = landing_id.split_at(SHA256_TEXT_LENGTH);
    validate_hash(shard_hash, "landed byte admission shard hash")?;
    let remainder = remainder
        .strip_prefix(':')
        .ok_or_else(|| anyhow!("CoreStore landed byte landing id has invalid shard delimiter"))?;
    let (mutation_id, content_hash) = remainder
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("CoreStore landed byte landing id is missing content hash"))?;
    validate_logical_id(mutation_id, "landed byte mutation id")?;
    if content_hash.len() != 64 || !content_hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore landed byte landing id has invalid content hash");
    }
    Ok((shard_hash, mutation_id))
}

fn admission_contention_retry_delay(admission_shard_hash: &str, attempt: usize) -> Duration {
    let base_micros = 25_u64.saturating_mul(1_u64 << attempt.min(8));
    let shard_jitter = admission_shard_hash
        .as_bytes()
        .iter()
        .rev()
        .take(8)
        .fold(0_u64, |value, byte| value.rotate_left(5) ^ u64::from(*byte))
        % base_micros.max(1);
    Duration::from_micros(base_micros.saturating_add(shard_jitter))
}

async fn collect_landed_byte_files(root: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.clone()];
    while let Some(dir) = pending.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read landed bytes dir {}", dir.display()));
            }
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "landed")
            {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn landed_bytes_file_hash(path: &std::path::Path) -> Result<String> {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("CoreStore landed bytes path has no UTF-8 filename"))?;
    let hash = filename
        .strip_suffix(".landed")
        .ok_or_else(|| anyhow!("CoreStore landed bytes filename has invalid suffix"))?;
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore landed bytes filename has invalid SHA-256 hash");
    }
    Ok(hash.to_ascii_lowercase())
}

async fn quarantine_landed_bytes_file(path: &PathBuf) -> Result<()> {
    let quarantine_path = path.with_extension(format!(
        "{}.quarantine",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("landed")
    ));
    match fs::rename(path, &quarantine_path).await {
        Ok(()) => {
            sync_parent_dir(&quarantine_path, "landed_quarantine_sync_parent_dir").await?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "quarantine CoreStore landed bytes {} -> {}",
                path.display(),
                quarantine_path.display()
            )
        }),
    }
}
