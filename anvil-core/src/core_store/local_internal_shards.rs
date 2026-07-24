use super::*;
use crate::core_store::block_shard::{
    BlockShardRepairRecord, BlockShardStoredState, BlockShardValidState,
    CORE_BLOCK_SHARD_MAX_HEADER_BYTES, CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES,
    block_shard_file_hash, block_shard_repair_head_path, block_shard_repair_operation_path,
    encode_block_shard_repair_record, read_block_shard_file_bounded,
    read_block_shard_repair_record, read_block_shard_stored_state_bounded,
};
use anyhow::Context;

#[derive(Debug, thiserror::Error)]
#[error("CoreStore internal shard write precondition failed: {0}")]
struct InternalShardWritePrecondition(String);

#[derive(Debug, Clone, Copy)]
enum InternalShardWriteMode<'a> {
    Put,
    Repair { operation_id: &'a str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InternalShardEpochDecision {
    Apply {
        expected: Option<BlockShardValidState>,
    },
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InternalShardRepairDecision {
    Apply {
        expected: Option<BlockShardValidState>,
    },
    Resume(BlockShardRepairRecord),
    Replay {
        record: BlockShardRepairRecord,
        persist_head: bool,
        persist_operation: bool,
    },
}

impl CoreStore {
    pub(crate) async fn put_internal_shard(
        &self,
        request: CoreInternalPutShard,
    ) -> Result<CoreInternalShardReceipt> {
        self.write_internal_shard(request, InternalShardWriteMode::Put)
            .await
    }

    pub(crate) async fn repair_internal_shard(
        &self,
        request: CoreInternalPutShard,
        repair_operation_id: &str,
    ) -> Result<CoreInternalShardReceipt> {
        if repair_operation_id.trim().is_empty() {
            bail!("CoreStore internal shard repair operation id is required");
        }
        if repair_operation_id.len() as u64 > CORE_BLOCK_SHARD_MAX_REPAIR_RECORD_BYTES {
            bail!("CoreStore internal shard repair operation id exceeds bounded size");
        }
        self.write_internal_shard(
            request,
            InternalShardWriteMode::Repair {
                operation_id: repair_operation_id,
            },
        )
        .await
    }

    async fn write_internal_shard(
        &self,
        request: CoreInternalPutShard,
        mode: InternalShardWriteMode<'_>,
    ) -> Result<CoreInternalShardReceipt> {
        let profile = validate_internal_shard_write(&request)?;
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let shard_path =
            self.shard_path(&placement.node_id, &request.block_id, request.shard_index);
        let repair_head_path = block_shard_repair_head_path(&shard_path);
        let shard_file = encode_block_shard_file(
            block_shard_header_from_internal_request(&request),
            &request.shard_bytes,
        )?;
        let new_file_hash = block_shard_file_hash(&shard_file)?;
        let lock_id = format!(
            "{}:{}:{}",
            placement.node_id, request.block_id, request.shard_index
        );
        let _write_guard = self
            .acquire_named_lock("internal-shard-write", &lock_id)
            .await?;
        let current =
            read_block_shard_stored_state_bounded(&shard_path, profile.max_shard_size_bytes)
                .await?;
        if let BlockShardStoredState::Valid(current) = &current
            && (current.block_id != request.block_id || current.shard_index != request.shard_index)
        {
            bail!("CoreStore internal shard write found a mismatched shard scope");
        }
        let persisted_head = read_block_shard_repair_record(&repair_head_path).await?;
        if let Some(record) = persisted_head.as_ref() {
            self.validate_internal_shard_repair_storage_scope(record, &request, &placement)?;
        }

        let InternalShardWriteMode::Repair { operation_id } = mode else {
            return match decide_internal_shard_epoch(
                &current,
                persisted_head.as_ref(),
                request.placement_epoch,
                &new_file_hash,
                false,
            )? {
                InternalShardEpochDecision::Replay => {
                    self.build_internal_shard_receipt(&request, &placement, unix_timestamp_nanos())
                }
                InternalShardEpochDecision::Apply { .. } => {
                    write_file_atomic(&shard_path, &shard_file).await?;
                    self.build_internal_shard_receipt(&request, &placement, unix_timestamp_nanos())
                }
            };
        };

        let repair_operation_path = block_shard_repair_operation_path(&shard_path, operation_id);
        let persisted_operation = read_block_shard_repair_record(&repair_operation_path).await?;
        if let Some(record) = persisted_operation.as_ref() {
            self.validate_internal_shard_repair_storage_scope(record, &request, &placement)?;
        }
        let decision = decide_internal_shard_repair(
            &current,
            persisted_head.as_ref(),
            persisted_operation.as_ref(),
            operation_id,
            request.placement_epoch,
            &new_file_hash,
        )?;
        match decision {
            InternalShardRepairDecision::Replay {
                record,
                persist_head,
                persist_operation,
            } => {
                self.validate_internal_shard_repair_record(
                    &record,
                    &request,
                    &placement,
                    &new_file_hash,
                )?;
                validate_internal_shard_replay_state(&record, &current)?;
                if persist_head || persist_operation {
                    let record_bytes = encode_block_shard_repair_record(&record)?;
                    if persist_head {
                        write_file_atomic(&repair_head_path, &record_bytes).await?;
                    }
                    if persist_operation {
                        write_file_atomic(&repair_operation_path, &record_bytes).await?;
                    }
                }
                Ok(record.receipt)
            }
            InternalShardRepairDecision::Resume(record) => {
                self.validate_internal_shard_repair_record(
                    &record,
                    &request,
                    &placement,
                    &new_file_hash,
                )?;
                let record_bytes = encode_block_shard_repair_record(&record)?;
                write_file_atomic(&repair_head_path, &record_bytes).await?;
                write_file_atomic(&repair_operation_path, &record_bytes).await?;
                write_file_atomic(&shard_path, &shard_file).await?;
                Ok(record.receipt)
            }
            InternalShardRepairDecision::Apply { expected } => {
                let receipt = self.build_internal_shard_receipt(
                    &request,
                    &placement,
                    unix_timestamp_nanos(),
                )?;
                let record = BlockShardRepairRecord {
                    operation_id: operation_id.to_string(),
                    expected_file_present: expected.is_some(),
                    expected_placement_epoch: expected
                        .as_ref()
                        .map_or(0, |state| state.placement_epoch),
                    expected_file_hash: expected
                        .as_ref()
                        .map_or_else(String::new, |state| state.file_hash.clone()),
                    new_placement_epoch: request.placement_epoch,
                    new_file_hash,
                    receipt: receipt.clone(),
                };
                let record_bytes = encode_block_shard_repair_record(&record)?;
                // Persist both the epoch head and immutable operation receipt before
                // publishing the shard so a retry can finish an interrupted rename.
                write_file_atomic(&repair_head_path, &record_bytes).await?;
                write_file_atomic(&repair_operation_path, &record_bytes).await?;
                write_file_atomic(&shard_path, &shard_file).await?;
                Ok(receipt)
            }
        }
    }

    pub(crate) fn is_internal_shard_repair_precondition(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<InternalShardWritePrecondition>()
            .is_some()
    }

    fn build_internal_shard_receipt(
        &self,
        request: &CoreInternalPutShard,
        placement: &LocalShardPlacement,
        written_at_unix_nanos: u64,
    ) -> Result<CoreInternalShardReceipt> {
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &request.block_id,
            shard_index: request.shard_index,
            erasure_profile: &request.erasure_profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: request.placement_epoch,
            shard_length: request.shard_bytes.len() as u64,
            shard_hash: &request.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: &request.boundary_summary_hash,
        });
        let signature = self.sign_core_receipt(&signed_payload_hash)?;
        Ok(CoreInternalShardReceipt {
            node_id: placement.node_id.clone(),
            region_id: placement.region_id.clone(),
            cell_id: placement.cell_id.clone(),
            block_id: request.block_id.clone(),
            shard_index: request.shard_index,
            shard_hash: request.shard_hash.clone(),
            shard_length: request.shard_bytes.len() as u64,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            signed_payload_hash,
            signature,
        })
    }

    fn validate_internal_shard_repair_record(
        &self,
        record: &BlockShardRepairRecord,
        request: &CoreInternalPutShard,
        placement: &LocalShardPlacement,
        new_file_hash: &str,
    ) -> Result<()> {
        self.validate_internal_shard_repair_storage_scope(record, request, placement)?;
        let receipt = &record.receipt;
        if record.new_placement_epoch != request.placement_epoch
            || record.new_file_hash != new_file_hash
            || receipt.shard_hash != request.shard_hash
            || receipt.shard_length != request.shard_bytes.len() as u64
        {
            bail!("CoreStore internal shard repair record scope mismatch");
        }
        let expected_signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &request.block_id,
            shard_index: request.shard_index,
            erasure_profile: &request.erasure_profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: request.placement_epoch,
            shard_length: request.shard_bytes.len() as u64,
            shard_hash: &request.shard_hash,
            fsync_sequence: receipt.fsync_sequence,
            written_at_unix_nanos: receipt.written_at_unix_nanos,
            boundary_summary_hash: &request.boundary_summary_hash,
        });
        if receipt.signed_payload_hash != expected_signed_payload_hash {
            bail!("CoreStore internal shard repair record receipt hash mismatch");
        }
        Ok(())
    }

    fn validate_internal_shard_repair_storage_scope(
        &self,
        record: &BlockShardRepairRecord,
        request: &CoreInternalPutShard,
        placement: &LocalShardPlacement,
    ) -> Result<()> {
        let receipt = &record.receipt;
        if receipt.node_id != placement.node_id
            || receipt.region_id != placement.region_id
            || receipt.cell_id != placement.cell_id
            || receipt.block_id != request.block_id
            || receipt.shard_index != request.shard_index
            || receipt.fsync_sequence != LOCAL_SHARD_FSYNC_SEQUENCE
        {
            bail!("CoreStore internal shard repair record storage scope mismatch");
        }
        self.verify_core_receipt_signature(
            &receipt.node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )
    }

    pub(crate) async fn read_internal_shard_range(
        &self,
        request: CoreInternalGetShard,
    ) -> Result<Vec<u8>> {
        validate_logical_id(&request.block_id, "internal shard block id")?;
        validate_hash(&request.shard_hash, "internal shard hash")?;
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        if usize::from(request.shard_index) >= profile.total_shards() {
            bail!("CoreStore internal shard index exceeds erasure profile shard count");
        }
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let shard_path =
            self.shard_path(&placement.node_id, &request.block_id, request.shard_index);
        let bytes = read_block_shard_file_bounded(
            &shard_path,
            BlockShardExpectation {
                block_id: &request.block_id,
                shard_index: request.shard_index,
                erasure_profile_id: &request.erasure_profile_id,
                placement_epoch: request.placement_epoch,
                payload_hash: &request.shard_hash,
                payload_len: 0,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            profile.max_shard_size_bytes,
            "internal_get_shard",
        )
        .await?;
        if let Some(range) = request.range {
            let start =
                usize::try_from(range.start).context("internal shard range start exceeds usize")?;
            let end = usize::try_from(range.end_exclusive)
                .context("internal shard range end exceeds usize")?;
            if start > end || end > bytes.len() {
                bail!("CoreStore internal shard requested range is out of bounds");
            }
            Ok(bytes[start..end].to_vec())
        } else {
            Ok(bytes)
        }
    }

    pub(crate) async fn get_internal_shard_receipt(
        &self,
        request: CoreInternalGetShard,
    ) -> Result<CoreInternalShardReceipt> {
        let bytes = self.read_internal_shard_range(request.clone()).await?;
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let boundary_summary_hash = request.boundary_summary_hash.unwrap_or_default();
        let written_at_unix_nanos = unix_timestamp_nanos();
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &request.block_id,
            shard_index: request.shard_index,
            erasure_profile: &request.erasure_profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: request.placement_epoch,
            shard_length: bytes.len() as u64,
            shard_hash: &request.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: &boundary_summary_hash,
        });
        let signature = self.sign_core_receipt(&signed_payload_hash)?;
        Ok(CoreInternalShardReceipt {
            node_id: placement.node_id,
            region_id: placement.region_id,
            cell_id: placement.cell_id,
            block_id: request.block_id,
            shard_index: request.shard_index,
            shard_hash: request.shard_hash,
            shard_length: bytes.len() as u64,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            signed_payload_hash,
            signature,
        })
    }
    pub(super) fn internal_shard_placement(
        &self,
        profile: LocalErasureProfile,
        shard_index: u16,
    ) -> LocalShardPlacement {
        if self.node_identity == CoreStoreNodeIdentity::default() {
            LocalShardPlacement {
                node_id: format!("{LOCAL_NODE_ID_PREFIX}-{}", usize::from(shard_index) + 1),
                region_id: "local".to_string(),
                cell_id: local_cell_id_for_shard(profile, usize::from(shard_index)),
                failure_domain: local_cell_id_for_shard(profile, usize::from(shard_index)),
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: String::new(),
                is_local: true,
            }
        } else {
            LocalShardPlacement {
                node_id: self.node_identity.node_id.clone(),
                region_id: self.node_identity.region_id.clone(),
                cell_id: self.node_identity.cell_id.clone(),
                failure_domain: self.node_identity.cell_id.clone(),
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: self.node_identity.public_api_addr.clone(),
                is_local: true,
            }
        }
    }
}

fn validate_internal_shard_write(request: &CoreInternalPutShard) -> Result<LocalErasureProfile> {
    validate_logical_id(&request.block_id, "internal shard block id")?;
    validate_logical_file_id(&request.logical_file_id, "internal shard logical file id")?;
    validate_writer_family(&request.writer_family, "internal shard writer family")?;
    validate_hash(&request.shard_hash, "internal shard hash")?;
    if request.placement_epoch == 0 {
        bail!("CoreStore internal shard placement epoch must be greater than zero");
    }
    let profile = local_erasure_profile(&request.erasure_profile_id)?;
    if usize::from(request.shard_index) >= profile.total_shards() {
        bail!("CoreStore internal shard index exceeds erasure profile shard count");
    }
    validate_internal_shard_size(request.shard_bytes.len(), profile.max_shard_size_bytes)?;
    validate_internal_shard_header_size(request)?;
    validate_boundary_summary_fields(&request.boundary_summary_hash, &request.boundary_values_b64)?;
    validate_object_blob_pipeline_options(
        &request.compression_algorithm,
        &request.encryption_algorithm,
    )?;
    let actual_hash = format!("sha256:{}", sha256_hex(&request.shard_bytes));
    if actual_hash != request.shard_hash {
        bail!("CoreStore internal shard hash mismatch");
    }
    Ok(profile)
}

fn validate_internal_shard_size(shard_len: usize, max_shard_size_bytes: u64) -> Result<()> {
    let shard_len = u64::try_from(shard_len)
        .map_err(|_| anyhow!("CoreStore internal shard size exceeds u64"))?;
    if shard_len > max_shard_size_bytes {
        bail!(
            "CoreStore internal shard size {shard_len} exceeds erasure profile maximum {max_shard_size_bytes}"
        );
    }
    Ok(())
}

fn validate_internal_shard_header_size(request: &CoreInternalPutShard) -> Result<()> {
    let fields = [
        request.block_id.as_str(),
        LOCAL_ERASURE_SET_ID,
        request.erasure_profile_id.as_str(),
        request.logical_file_id.as_str(),
        request.shard_hash.as_str(),
        request.shard_hash.as_str(),
        request.compression_algorithm.as_str(),
        request.encryption_algorithm.as_str(),
        request.boundary_summary_hash.as_str(),
        request.boundary_values_b64.as_str(),
        request.writer_family.as_str(),
        request.mutation_id.as_str(),
    ];
    let field_bytes = fields.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value.len())
            .ok_or_else(|| anyhow!("CoreStore internal shard header length overflow"))
    })?;
    if field_bytes > CORE_BLOCK_SHARD_MAX_HEADER_BYTES {
        bail!("CoreStore internal shard header exceeds bounded size");
    }
    Ok(())
}

fn decide_internal_shard_repair(
    current: &BlockShardStoredState,
    persisted_head: Option<&BlockShardRepairRecord>,
    persisted_operation: Option<&BlockShardRepairRecord>,
    operation_id: &str,
    new_placement_epoch: u64,
    new_file_hash: &str,
) -> std::result::Result<InternalShardRepairDecision, InternalShardWritePrecondition> {
    if let Some(record) = persisted_operation
        .or_else(|| persisted_head.filter(|record| record.operation_id == operation_id))
    {
        if record.operation_id != operation_id {
            return Err(InternalShardWritePrecondition(
                "repair operation record identity mismatch".to_string(),
            ));
        }
        if let Some(head) = persisted_head
            && head.operation_id == operation_id
            && head != record
        {
            return Err(InternalShardWritePrecondition(
                "repair operation has conflicting durable records".to_string(),
            ));
        }
        if record.new_placement_epoch != new_placement_epoch
            || record.new_file_hash != new_file_hash
        {
            return Err(InternalShardWritePrecondition(
                "repair operation identity was already used for a different epoch or shard"
                    .to_string(),
            ));
        }
        if let Some(head) = persisted_head
            && head.new_placement_epoch == record.new_placement_epoch
            && head.operation_id != operation_id
        {
            return Err(InternalShardWritePrecondition(format!(
                "repair placement epoch {} conflicts with existing state",
                record.new_placement_epoch
            )));
        }
        return match decide_internal_shard_epoch(
            current,
            persisted_head,
            new_placement_epoch,
            new_file_hash,
            true,
        )? {
            InternalShardEpochDecision::Replay => {
                if !repair_record_matches_new_state(record, current) {
                    return Err(InternalShardWritePrecondition(
                        "repair replay does not match the fully validated shard".to_string(),
                    ));
                }
                Ok(InternalShardRepairDecision::Replay {
                    record: record.clone(),
                    persist_head: persisted_head != Some(record),
                    persist_operation: persisted_operation.is_none(),
                })
            }
            InternalShardEpochDecision::Apply { .. }
                if repair_record_can_resume(record, current, persisted_head) =>
            {
                Ok(InternalShardRepairDecision::Resume(record.clone()))
            }
            InternalShardEpochDecision::Apply { .. } => Err(InternalShardWritePrecondition(
                "repair operation expected shard state no longer matches".to_string(),
            )),
        };
    }

    if let Some(head) = persisted_head
        && head.new_placement_epoch == new_placement_epoch
    {
        return Err(InternalShardWritePrecondition(format!(
            "repair placement epoch {new_placement_epoch} conflicts with existing operation {}",
            head.operation_id
        )));
    }
    match decide_internal_shard_epoch(
        current,
        persisted_head,
        new_placement_epoch,
        new_file_hash,
        true,
    )? {
        InternalShardEpochDecision::Replay => Err(InternalShardWritePrecondition(format!(
            "repair placement epoch {new_placement_epoch} is already durable under a different operation"
        ))),
        InternalShardEpochDecision::Apply { expected } => {
            Ok(InternalShardRepairDecision::Apply { expected })
        }
    }
}

fn decide_internal_shard_epoch(
    current: &BlockShardStoredState,
    persisted_head: Option<&BlockShardRepairRecord>,
    new_placement_epoch: u64,
    new_file_hash: &str,
    allow_corrupt_replacement: bool,
) -> std::result::Result<InternalShardEpochDecision, InternalShardWritePrecondition> {
    let current_valid = match current {
        BlockShardStoredState::Absent => None,
        BlockShardStoredState::Valid(state) => Some(state),
        BlockShardStoredState::Corrupt if allow_corrupt_replacement => None,
        BlockShardStoredState::Corrupt => {
            return Err(InternalShardWritePrecondition(
                "stored shard is corrupt and requires the repair path".to_string(),
            ));
        }
    };
    let current_epoch = current_valid.map_or(0, |state| state.placement_epoch);
    let head_epoch = persisted_head.map_or(0, |record| record.new_placement_epoch);
    let epoch_floor = current_epoch.max(head_epoch);
    if new_placement_epoch < epoch_floor {
        return Err(InternalShardWritePrecondition(format!(
            "placement epoch {new_placement_epoch} is stale; current epoch is at least {epoch_floor}"
        )));
    }
    if new_placement_epoch > epoch_floor {
        return Ok(InternalShardEpochDecision::Apply {
            expected: current_valid.cloned(),
        });
    }

    let current_at_epoch = current_valid.filter(|state| state.placement_epoch == epoch_floor);
    let head_at_epoch = persisted_head.filter(|head| head.new_placement_epoch == epoch_floor);
    if current_at_epoch.is_some_and(|state| state.file_hash != new_file_hash)
        || head_at_epoch.is_some_and(|head| head.new_file_hash != new_file_hash)
    {
        return Err(InternalShardWritePrecondition(format!(
            "placement epoch {new_placement_epoch} conflicts with different shard bytes"
        )));
    }
    if current_at_epoch.is_some() {
        return Ok(InternalShardEpochDecision::Replay);
    }
    if head_at_epoch.is_some() {
        return Ok(InternalShardEpochDecision::Apply {
            expected: current_valid.cloned(),
        });
    }
    Ok(InternalShardEpochDecision::Apply {
        expected: current_valid.cloned(),
    })
}

fn repair_record_matches_expected_state(
    record: &BlockShardRepairRecord,
    current: &BlockShardStoredState,
) -> bool {
    match (record.expected_file_present, current) {
        (false, BlockShardStoredState::Absent) => true,
        (true, BlockShardStoredState::Valid(current)) => {
            current.placement_epoch == record.expected_placement_epoch
                && current.file_hash == record.expected_file_hash
        }
        _ => false,
    }
}

fn repair_record_can_resume(
    record: &BlockShardRepairRecord,
    current: &BlockShardStoredState,
    persisted_head: Option<&BlockShardRepairRecord>,
) -> bool {
    repair_record_matches_expected_state(record, current)
        || matches!(current, BlockShardStoredState::Corrupt)
        || (matches!(current, BlockShardStoredState::Absent) && persisted_head == Some(record))
}

fn repair_record_matches_new_state(
    record: &BlockShardRepairRecord,
    current: &BlockShardStoredState,
) -> bool {
    matches!(
        current,
        BlockShardStoredState::Valid(current)
            if current.placement_epoch == record.new_placement_epoch
                && current.file_hash == record.new_file_hash
    )
}

fn validate_internal_shard_replay_state(
    record: &BlockShardRepairRecord,
    current: &BlockShardStoredState,
) -> Result<()> {
    if !repair_record_matches_new_state(record, current) {
        bail!("CoreStore internal shard repair replay requires a fully validated exact shard");
    }
    Ok(())
}

fn block_shard_header_from_internal_request(
    request: &CoreInternalPutShard,
) -> BlockShardHeaderInput {
    BlockShardHeaderInput {
        block_id: request.block_id.clone(),
        erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
        shard_index: request.shard_index,
        erasure_profile_id: request.erasure_profile_id.clone(),
        logical_file_id: request.logical_file_id.clone(),
        logical_offset: request.logical_offset,
        logical_length: request.shard_bytes.len() as u64,
        payload_plain_hash: request.shard_hash.clone(),
        payload_stored_hash: request.shard_hash.clone(),
        compression: request.compression_algorithm.clone(),
        encryption: request.encryption_algorithm.clone(),
        placement_epoch: request.placement_epoch,
        boundary_summary_hash: request.boundary_summary_hash.clone(),
        boundary_values_b64: request.boundary_values_b64.clone(),
        writer_family: request.writer_family.clone(),
        created_by_mutation_id: request.mutation_id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OLD_FILE_HASH: &str =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const NEW_FILE_HASH: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const NEWER_FILE_HASH: &str =
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn internal_shard_header_preserves_stream_block_identity() {
        let request = CoreInternalPutShard {
            logical_file_id: "lf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            logical_offset: 32_768,
            block_id: "blk_stream".to_string(),
            shard_index: 3,
            erasure_profile_id: LOCAL_ERASURE_PROFILE_ID.to_string(),
            placement_epoch: 5,
            shard_bytes: b"stream-shard".to_vec(),
            shard_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_string(),
            boundary_summary_hash:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            boundary_values_b64: "boundary-values".to_string(),
            compression_algorithm: "zstd".to_string(),
            encryption_algorithm: "aes_gcm_siv".to_string(),
            writer_family: WriterFamily::Stream.as_str().to_string(),
            mutation_id: "original-stream-mutation".to_string(),
        };

        let header = block_shard_header_from_internal_request(&request);

        assert_eq!(header.logical_file_id, request.logical_file_id);
        assert_eq!(header.block_id, "blk_stream");
        assert_eq!(header.logical_offset, 32_768);
        assert_eq!(header.logical_length, request.shard_bytes.len() as u64);
        assert_eq!(header.compression, "zstd");
        assert_eq!(header.encryption, "aes_gcm_siv");
        assert_eq!(header.writer_family, WriterFamily::Stream.as_str());
        assert_eq!(header.created_by_mutation_id, "original-stream-mutation");
    }

    #[test]
    fn exact_repair_replay_returns_the_persisted_receipt() {
        let current = stored_state(8, NEW_FILE_HASH);
        let record = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);

        let decision = decide_internal_shard_repair(
            &current,
            Some(&record),
            Some(&record),
            "repair-finding",
            8,
            NEW_FILE_HASH,
        )
        .unwrap();

        assert!(matches!(
            decision,
            InternalShardRepairDecision::Replay {
                record: replayed,
                persist_head: false,
                persist_operation: false,
            } if replayed.receipt == record.receipt
        ));
    }

    #[test]
    fn interrupted_exact_repair_resumes_with_the_persisted_receipt() {
        let current = stored_state(7, OLD_FILE_HASH);
        let record = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);

        let decision = decide_internal_shard_repair(
            &current,
            Some(&record),
            Some(&record),
            "repair-finding",
            8,
            NEW_FILE_HASH,
        )
        .unwrap();

        assert!(matches!(
            decision,
            InternalShardRepairDecision::Resume(resumed)
                if resumed.receipt == record.receipt
        ));
    }

    #[test]
    fn stale_and_conflicting_same_epoch_repairs_fail() {
        let current = stored_state(8, NEW_FILE_HASH);

        let stale =
            decide_internal_shard_repair(&current, None, None, "stale-repair", 7, OLD_FILE_HASH)
                .unwrap_err();
        let conflicting = decide_internal_shard_repair(
            &current,
            None,
            None,
            "conflicting-repair",
            8,
            NEWER_FILE_HASH,
        )
        .unwrap_err();

        assert!(stale.to_string().contains("stale"));
        assert!(conflicting.to_string().contains("conflicts"));
    }

    #[test]
    fn only_a_strictly_newer_repair_advances_the_durable_epoch_floor() {
        let current = stored_state(8, OLD_FILE_HASH);
        let pending = repair_record("pending-repair", 8, OLD_FILE_HASH, 9, NEW_FILE_HASH);

        let conflicting = decide_internal_shard_repair(
            &current,
            Some(&pending),
            None,
            "other-repair",
            9,
            NEW_FILE_HASH,
        )
        .unwrap_err();
        let newer = decide_internal_shard_repair(
            &current,
            Some(&pending),
            None,
            "other-repair",
            10,
            NEWER_FILE_HASH,
        )
        .unwrap();

        assert!(conflicting.to_string().contains("conflicts"));
        assert_eq!(
            newer,
            InternalShardRepairDecision::Apply {
                expected: Some(valid_state(8, OLD_FILE_HASH)),
            }
        );
    }

    #[test]
    fn repair_operation_identity_cannot_be_reused_for_different_input() {
        let current = stored_state(9, NEWER_FILE_HASH);
        let old_operation = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);
        let head = repair_record("newer-finding", 8, NEW_FILE_HASH, 9, NEWER_FILE_HASH);

        let stale_replay = decide_internal_shard_repair(
            &current,
            Some(&head),
            Some(&old_operation),
            "repair-finding",
            8,
            NEW_FILE_HASH,
        )
        .unwrap_err();
        let error = decide_internal_shard_repair(
            &current,
            Some(&head),
            Some(&old_operation),
            "repair-finding",
            10,
            OLD_FILE_HASH,
        )
        .unwrap_err();

        assert!(stale_replay.to_string().contains("stale"));
        assert!(error.to_string().contains("operation identity"));
    }

    #[test]
    fn normal_put_uses_repair_epoch_floor_and_allows_only_exact_replay() {
        let current = stored_state(8, NEW_FILE_HASH);
        let repaired_head = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);

        let replay =
            decide_internal_shard_epoch(&current, Some(&repaired_head), 8, NEW_FILE_HASH, false)
                .unwrap();
        let stale =
            decide_internal_shard_epoch(&current, Some(&repaired_head), 7, OLD_FILE_HASH, false)
                .unwrap_err();
        let conflicting =
            decide_internal_shard_epoch(&current, Some(&repaired_head), 8, NEWER_FILE_HASH, false)
                .unwrap_err();

        assert_eq!(replay, InternalShardEpochDecision::Replay);
        assert!(stale.to_string().contains("stale"));
        assert!(conflicting.to_string().contains("different shard bytes"));
    }

    #[test]
    fn pending_repair_head_fences_delayed_put_before_shard_publish() {
        let old_current = stored_state(7, OLD_FILE_HASH);
        let pending = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);

        let delayed =
            decide_internal_shard_epoch(&old_current, Some(&pending), 7, OLD_FILE_HASH, false)
                .unwrap_err();
        let conflicting =
            decide_internal_shard_epoch(&old_current, Some(&pending), 8, NEWER_FILE_HASH, false)
                .unwrap_err();
        let exact_completion =
            decide_internal_shard_epoch(&old_current, Some(&pending), 8, NEW_FILE_HASH, false)
                .unwrap();

        assert!(delayed.to_string().contains("stale"));
        assert!(conflicting.to_string().contains("different shard bytes"));
        assert_eq!(
            exact_completion,
            InternalShardEpochDecision::Apply {
                expected: Some(valid_state(7, OLD_FILE_HASH)),
            }
        );
    }

    #[test]
    fn only_repair_can_replace_corrupt_stored_state() {
        let current = BlockShardStoredState::Corrupt;

        let put_error =
            decide_internal_shard_epoch(&current, None, 8, NEW_FILE_HASH, false).unwrap_err();
        let repair =
            decide_internal_shard_repair(&current, None, None, "repair-corrupt", 8, NEW_FILE_HASH)
                .unwrap();

        assert!(put_error.to_string().contains("requires the repair path"));
        assert_eq!(
            repair,
            InternalShardRepairDecision::Apply { expected: None }
        );
    }

    #[test]
    fn persisted_exact_repair_restores_corrupt_stored_state() {
        let record = repair_record("repair-finding", 7, OLD_FILE_HASH, 8, NEW_FILE_HASH);

        let decision = decide_internal_shard_repair(
            &BlockShardStoredState::Corrupt,
            Some(&record),
            Some(&record),
            "repair-finding",
            8,
            NEW_FILE_HASH,
        )
        .unwrap();

        assert!(matches!(
            decision,
            InternalShardRepairDecision::Resume(resumed) if resumed == record
        ));
    }

    #[test]
    fn shard_size_is_bounded_by_the_selected_profile() {
        assert!(validate_internal_shard_size(16, 16).is_ok());
        assert!(validate_internal_shard_size(17, 16).is_err());
    }

    fn stored_state(placement_epoch: u64, file_hash: &str) -> BlockShardStoredState {
        BlockShardStoredState::Valid(valid_state(placement_epoch, file_hash))
    }

    fn valid_state(placement_epoch: u64, file_hash: &str) -> BlockShardValidState {
        BlockShardValidState {
            block_id: "blk_object".to_string(),
            shard_index: 2,
            placement_epoch,
            file_hash: file_hash.to_string(),
        }
    }

    fn repair_record(
        operation_id: &str,
        expected_placement_epoch: u64,
        expected_file_hash: &str,
        new_placement_epoch: u64,
        new_file_hash: &str,
    ) -> BlockShardRepairRecord {
        BlockShardRepairRecord {
            operation_id: operation_id.to_string(),
            expected_file_present: true,
            expected_placement_epoch,
            expected_file_hash: expected_file_hash.to_string(),
            new_placement_epoch,
            new_file_hash: new_file_hash.to_string(),
            receipt: CoreInternalShardReceipt {
                node_id: "node-a".to_string(),
                region_id: "region-a".to_string(),
                cell_id: "cell-a".to_string(),
                block_id: "blk_object".to_string(),
                shard_index: 2,
                shard_hash: NEWER_FILE_HASH.to_string(),
                shard_length: 1024,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                written_at_unix_nanos: 123_456,
                signed_payload_hash: OLD_FILE_HASH.to_string(),
                signature: vec![1, 2, 3],
            },
        }
    }
}
