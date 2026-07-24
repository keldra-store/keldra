use super::*;
use crate::anvil_api::{
    GetShardRequest, InternalRequestHeader, PutShardRequest, RepairShardRequest, ShardReceipt,
    block_store_internal_client::BlockStoreInternalClient,
};
use crate::mesh_lifecycle::{self, LifecycleState, NodeCapability};
use futures_util::StreamExt;
use tonic::metadata::MetadataValue;

const MAX_SHARD_CONTEXT_FIELD_BYTES: usize = 16 * 1024 * 1024;
const MAX_SHARD_IDENTITY_FIELD_BYTES: usize = 1024;
const MAX_REPAIR_FINDING_ID_BYTES: usize = 256;

impl CoreStore {
    pub(super) async fn plan_publish_shard_placements(
        &self,
        profile: LocalErasureProfile,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<Vec<LocalShardPlacement>> {
        let candidates = self.active_shard_candidates(profile).await?;
        let placements = choose_spread_placements(profile, candidates, boundary_values)?;
        validate_local_publish_placements(profile, &placements)?;
        Ok(placements)
    }

    pub(super) async fn write_shard_to_placement(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        validate_shard_write_input(ShardWriteInput {
            logical_file_id: input.logical_file_id,
            block_id: input.block_id,
            shard_index: input.shard_index,
            shard: input.shard,
            shard_hash: input.shard_hash,
            profile: input.profile,
            boundary_summary_hash: input.boundary_summary_hash,
            boundary_values_b64: input.boundary_values_b64,
            mutation_id: input.mutation_id,
            compression_algorithm: input.compression_algorithm,
            encryption_algorithm: input.encryption_algorithm,
            writer_family: input.writer_family,
        })?;
        if input.placement.is_local || input.placement.public_api_addr.trim().is_empty() {
            self.write_local_block_shard(input).await
        } else {
            self.write_remote_block_shard(input).await
        }
    }

    pub(super) async fn read_shard_from_placement(
        &self,
        input: ReadShardFromPlacement<'_>,
    ) -> Result<Vec<u8>> {
        if self
            .shard_path(
                &input.placement.node_id,
                input.block_id,
                input.placement.shard_index,
            )
            .exists()
        {
            return self.read_local_block_shard(input).await;
        }
        let endpoint = self
            .placement_endpoint(&input.placement.node_id)
            .await?
            .or_else(|| {
                if input.placement.node_id == self.node_identity.node_id {
                    Some(self.node_identity.public_api_addr.clone())
                } else {
                    None
                }
            });
        let Some(endpoint) = endpoint else {
            return self.read_local_block_shard(input).await;
        };
        self.read_remote_block_shard(input, &endpoint).await
    }

    pub(super) async fn repair_shard_to_placement(
        &self,
        input: RepairShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        validate_shard_write_input(ShardWriteInput {
            logical_file_id: input.logical_file_id,
            block_id: input.block_id,
            shard_index: input.shard_index,
            shard: input.shard,
            shard_hash: input.shard_hash,
            profile: input.profile,
            boundary_summary_hash: input.boundary_summary_hash,
            boundary_values_b64: input.boundary_values_b64,
            mutation_id: input.mutation_id,
            compression_algorithm: input.compression_algorithm,
            encryption_algorithm: input.encryption_algorithm,
            writer_family: input.writer_family,
        })?;
        validate_repair_finding_id(input.repair_finding_id)?;
        let receipt =
            if input.placement.is_local || input.placement.public_api_addr.trim().is_empty() {
                let receipt = self
                    .repair_internal_shard(
                        CoreInternalPutShard {
                            logical_file_id: input.logical_file_id.to_string(),
                            logical_offset: input.logical_offset,
                            block_id: input.block_id.to_string(),
                            shard_index: input.shard_index,
                            erasure_profile_id: input.profile.id.to_string(),
                            placement_epoch: input.placement_epoch,
                            shard_bytes: input.shard.to_vec(),
                            shard_hash: input.shard_hash.to_string(),
                            boundary_summary_hash: input.boundary_summary_hash.to_string(),
                            boundary_values_b64: input.boundary_values_b64.to_string(),
                            compression_algorithm: input.compression_algorithm.to_string(),
                            encryption_algorithm: input.encryption_algorithm.to_string(),
                            writer_family: input.writer_family.to_string(),
                            mutation_id: input.mutation_id.to_string(),
                        },
                        input.repair_finding_id,
                    )
                    .await?;
                if receipt.region_id != input.placement.region_id
                    || receipt.cell_id != input.placement.cell_id
                {
                    bail!("CoreStore local shard repair receipt placement context mismatch");
                }
                ShardReceipt {
                    node_id: receipt.node_id,
                    block_id: receipt.block_id,
                    shard_index: u32::from(receipt.shard_index),
                    shard_hash: receipt.shard_hash,
                    shard_length: receipt.shard_length,
                    fsync_sequence: receipt.fsync_sequence,
                    written_at_unix_nanos: receipt.written_at_unix_nanos,
                    signed_payload_hash: receipt.signed_payload_hash,
                    signature: receipt.signature,
                }
            } else {
                self.repair_remote_block_shard(input).await?
            };
        self.placement_from_receipt(
            ReceiptPlacementInput {
                block_id: input.block_id,
                shard_index: input.shard_index,
                profile: input.profile,
                placement: input.placement,
                placement_epoch: input.placement_epoch,
                boundary_summary_hash: input.boundary_summary_hash,
                expected_shard_hash: input.shard_hash,
                expected_shard_length: u64::try_from(input.shard.len())
                    .context("CoreStore repaired shard length exceeds u64")?,
                generation: input.generation,
            },
            receipt,
        )
    }

    pub(super) async fn active_shard_candidates(
        &self,
        profile: LocalErasureProfile,
    ) -> Result<Vec<LocalShardPlacement>> {
        let active = self.active_object_peer_placements().await?;
        let mut out = if active.len() >= profile.total_shards() {
            active
        } else if active.len() <= 1 {
            plan_local_shard_placements(profile)?
        } else {
            bail!(
                "CoreStore placement for {} requires {} active object nodes, got {}",
                profile.id,
                profile.total_shards(),
                active.len()
            );
        };
        sort_shard_candidates(&mut out);
        Ok(out)
    }

    pub(super) async fn active_object_peer_placements(&self) -> Result<Vec<LocalShardPlacement>> {
        let mut active = Vec::new();
        for node in mesh_lifecycle::list_node_projections_with_core_store(self, None, None)? {
            if node.mesh_id != self.node_identity.mesh_id {
                continue;
            }
            if node.region != self.node_identity.region_id {
                continue;
            }
            if node.state != LifecycleState::Active {
                continue;
            }
            if !node.capabilities.contains(&NodeCapability::Object) {
                continue;
            }
            if node.public_api_addr.trim().is_empty() {
                continue;
            }
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key,
            )?;
            let placement = LocalShardPlacement {
                is_local: node.node_id == self.node_identity.node_id,
                node_id: node.node_id,
                region_id: node.region,
                cell_id: node.cell_id.clone(),
                failure_domain: node.cell_id,
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: node.public_api_addr,
            };
            active.push(placement);
        }
        sort_shard_candidates(&mut active);
        Ok(active)
    }

    pub(super) async fn active_placement_cells(
        &self,
    ) -> Result<BTreeMap<(String, String), LocalPlacementCellInfo>> {
        let mut out = BTreeMap::new();
        for node in mesh_lifecycle::list_node_projections_with_core_store(self, None, None)? {
            if node.mesh_id != self.node_identity.mesh_id
                || node.region != self.node_identity.region_id
                || !matches!(node.state, LifecycleState::Active | LifecycleState::Joining)
                || !node.capabilities.contains(&NodeCapability::Object)
            {
                continue;
            }
            out.insert(
                (node.region.clone(), node.cell_id.clone()),
                LocalPlacementCellInfo {
                    failure_domain: node.cell_id,
                    region_weight: 100,
                    cell_weight: 100,
                },
            );
        }
        if !out.is_empty() {
            return Ok(out);
        }
        out.insert(
            (
                self.node_identity.region_id.clone(),
                self.node_identity.cell_id.clone(),
            ),
            LocalPlacementCellInfo {
                failure_domain: self.node_identity.cell_id.clone(),
                region_weight: 1,
                cell_weight: 1,
            },
        );
        Ok(out)
    }

    async fn write_local_block_shard(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        let shard_file = encode_block_shard_file(
            BlockShardHeaderInput {
                block_id: input.block_id.to_string(),
                erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
                shard_index: input.shard_index,
                erasure_profile_id: input.profile.id.to_string(),
                logical_file_id: input.logical_file_id.to_string(),
                logical_offset: input.logical_offset,
                logical_length: input.shard.len() as u64,
                payload_plain_hash: input.shard_hash.to_string(),
                payload_stored_hash: input.shard_hash.to_string(),
                compression: input.compression_algorithm.to_string(),
                encryption: input.encryption_algorithm.to_string(),
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                boundary_summary_hash: input.boundary_summary_hash.to_string(),
                boundary_values_b64: input.boundary_values_b64.to_string(),
                writer_family: input.writer_family.to_string(),
                created_by_mutation_id: input.mutation_id.to_string(),
            },
            input.shard,
        )?;
        let shard_path =
            self.shard_path(&input.placement.node_id, input.block_id, input.shard_index);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let dedupe_started_at = Instant::now();
        let dedupe_hit = read_block_shard_file(
            &shard_path,
            BlockShardExpectation {
                block_id: input.block_id,
                shard_index: input.shard_index,
                erasure_profile_id: input.profile.id,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                payload_hash: input.shard_hash,
                payload_len: input.shard.len() as u64,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            "dedupe_existing_block_shard",
        )
        .await
        .is_ok();
        record_byte_pipeline_stage_duration(
            if dedupe_hit {
                "dedupe_hit"
            } else {
                "dedupe_miss"
            },
            input.writer_family,
            "preencoded",
            input.encryption_algorithm,
            input.profile.id,
            dedupe_started_at.elapsed(),
        );
        crate::perf::record_dedupe_hit_ratio(
            input.writer_family,
            input.profile.id,
            if dedupe_hit { 1.0 } else { 0.0 },
        );
        if dedupe_hit {
            record_corestore_trace_event("byte_pipeline.dedupe", "hit");
        } else {
            record_corestore_trace_event("byte_pipeline.dedupe", "miss");
            let block_write_started_at = Instant::now();
            write_file_atomic(&shard_path, &shard_file).await?;
            record_block_write_duration(
                &input.placement.node_id,
                &input.placement.region_id,
                &input.placement.cell_id,
                "shard_write",
                "ok",
                block_write_started_at.elapsed(),
            );
            record_corestore_trace_event("block.shard_write", "ok");
            record_corestore_trace_event("block.shard_fsync", "ok");
        }
        let written_at_unix_nanos = unix_timestamp_nanos();
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: input.block_id,
            shard_index: input.shard_index,
            erasure_profile: input.profile.id,
            node_id: &input.placement.node_id,
            region_id: &input.placement.region_id,
            cell_id: &input.placement.cell_id,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            shard_length: input.shard.len() as u64,
            shard_hash: input.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: input.boundary_summary_hash,
        });
        let receipt_signature = self.sign_core_receipt(&signed_payload_hash)?;
        self.placement_from_write_receipt(
            input,
            ShardReceipt {
                node_id: input.placement.node_id.clone(),
                block_id: input.block_id.to_string(),
                shard_index: u32::from(input.shard_index),
                shard_hash: input.shard_hash.to_string(),
                shard_length: u64::try_from(input.shard.len())
                    .context("CoreStore local shard length exceeds u64")?,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                written_at_unix_nanos,
                signed_payload_hash,
                signature: receipt_signature,
            },
        )
    }

    async fn write_remote_block_shard(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreStore remote shard placement selected {}, but no internal bearer token is configured",
                input.placement.node_id
            )
        })?;
        let request_body =
            put_shard_request(self.internal_request_header("block.put_shard")?, input);
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let receipt = self
            .internal_grpc_request(
                &input.placement.public_api_addr,
                "put CoreStore shard",
                move |channel| {
                    let mut client = BlockStoreInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .put_shard(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await
            .with_context(|| {
                format!(
                    "put CoreStore shard {}:{} to {}",
                    input.block_id, input.shard_index, input.placement.node_id
                )
            })?;
        self.placement_from_write_receipt(input, receipt)
    }

    async fn repair_remote_block_shard(
        &self,
        input: RepairShardToPlacement<'_>,
    ) -> Result<ShardReceipt> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreStore remote shard repair selected {}, but no internal bearer token is configured",
                input.placement.node_id
            )
        })?;
        let request_body =
            repair_shard_request(self.internal_request_header("block.repair_shard")?, input);
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        self.internal_grpc_request(
            &input.placement.public_api_addr,
            "repair CoreStore shard",
            move |channel| {
                let mut client = BlockStoreInternalClient::new(channel);
                let mut request = tonic::Request::new(request_body.clone());
                request
                    .metadata_mut()
                    .insert("authorization", authorization.clone());
                async move {
                    client
                        .repair_shard(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
        )
        .await
        .with_context(|| {
            format!(
                "repair CoreStore shard {}:{} on {}",
                input.block_id, input.shard_index, input.placement.node_id
            )
        })
    }

    async fn read_local_block_shard(&self, input: ReadShardFromPlacement<'_>) -> Result<Vec<u8>> {
        let shard_path = self.shard_path(
            &input.placement.node_id,
            input.block_id,
            input.placement.shard_index,
        );
        read_block_shard_file(
            &shard_path,
            BlockShardExpectation {
                block_id: input.block_id,
                shard_index: input.placement.shard_index,
                erasure_profile_id: input.profile.id,
                placement_epoch: input.placement.placement_epoch,
                payload_hash: &input.placement.shard_hash,
                payload_len: input.placement.stored_size,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            input.operation,
        )
        .await
    }

    async fn read_remote_block_shard(
        &self,
        input: ReadShardFromPlacement<'_>,
        endpoint: &str,
    ) -> Result<Vec<u8>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreStore remote shard read selected {}, but no internal bearer token is configured",
                input.placement.node_id
            )
        })?;
        let expected_length = expected_shard_read_length(input)?;
        let (range_start, range_end_exclusive) = input
            .range
            .map(|range| (range.start, range.end_exclusive))
            .unwrap_or((0, 0));
        let block_id = input.block_id.to_string();
        let shard_index = u32::from(input.placement.shard_index);
        let erasure_profile_id = input.profile.id.to_string();
        let placement_epoch = input.placement.placement_epoch;
        let shard_hash = input.placement.shard_hash.clone();
        let boundary_summary_hash = input.boundary_summary_hash.to_string();
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let bytes = self
            .internal_grpc_request(endpoint, "get CoreStore shard", move |channel| {
                let block_id = block_id.clone();
                let erasure_profile_id = erasure_profile_id.clone();
                let shard_hash = shard_hash.clone();
                let boundary_summary_hash = boundary_summary_hash.clone();
                let authorization = authorization.clone();
                async move {
                    let mut client = BlockStoreInternalClient::new(channel);
                    let mut request = tonic::Request::new(GetShardRequest {
                        header: Some(self.internal_request_header("block.get_shard").map_err(
                            |err| tonic::Status::internal(format!("build internal header: {err}")),
                        )?),
                        block_id: block_id.clone(),
                        shard_index,
                        range_start,
                        range_end_exclusive,
                        erasure_profile_id,
                        placement_epoch,
                        shard_hash,
                        boundary_summary_hash,
                    });
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    let mut stream = client.get_shard(request).await?.into_inner();
                    let capacity = usize::try_from(expected_length).map_err(|_| {
                        tonic::Status::internal("CoreStore remote shard length exceeds usize")
                    })?;
                    let mut bytes = Vec::with_capacity(capacity);
                    let mut saw_eof = false;
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk?;
                        if chunk.block_id != block_id || chunk.shard_index != shard_index {
                            return Err(tonic::Status::data_loss(
                                "CoreStore remote shard chunk scope mismatch",
                            ));
                        }
                        let received = u64::try_from(bytes.len()).map_err(|_| {
                            tonic::Status::internal("CoreStore remote shard length exceeds u64")
                        })?;
                        let expected_offset =
                            range_start.checked_add(received).ok_or_else(|| {
                                tonic::Status::data_loss("CoreStore remote shard offset overflow")
                            })?;
                        if chunk.offset != expected_offset {
                            return Err(tonic::Status::data_loss(
                                "CoreStore remote shard chunk offset mismatch",
                            ));
                        }
                        let chunk_length = u64::try_from(chunk.data.len()).map_err(|_| {
                            tonic::Status::internal(
                                "CoreStore remote shard chunk length exceeds u64",
                            )
                        })?;
                        if received
                            .checked_add(chunk_length)
                            .is_none_or(|length| length > expected_length)
                        {
                            return Err(tonic::Status::data_loss(
                                "CoreStore remote shard response exceeds requested length",
                            ));
                        }
                        bytes.extend_from_slice(&chunk.data);
                        if chunk.eof {
                            saw_eof = true;
                            break;
                        }
                    }
                    if !saw_eof {
                        return Err(tonic::Status::data_loss(
                            "CoreStore remote shard response ended before eof",
                        ));
                    }
                    Ok(bytes)
                }
            })
            .await
            .with_context(|| {
                format!(
                    "read CoreStore shard {}:{} from {}",
                    input.block_id, input.placement.shard_index, input.placement.node_id
                )
            })?;
        if u64::try_from(bytes.len()).ok() != Some(expected_length) {
            bail!("CoreStore remote shard response length mismatch");
        }
        if input.range.is_some() {
            return Ok(bytes);
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&bytes));
        if actual_hash != input.placement.shard_hash {
            bail!("CoreStore remote shard hash mismatch");
        }
        Ok(bytes)
    }

    pub(super) async fn placement_endpoint(&self, node_id: &str) -> Result<Option<String>> {
        let nodes = mesh_lifecycle::list_nodes(&self.storage, None, None)
            .await
            .unwrap_or_default();
        if let Some(node) = nodes.into_iter().find(|node| node.node_id == node_id) {
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key,
            )?;
            Ok(Some(node.public_api_addr))
        } else {
            Ok(None)
        }
    }

    fn placement_from_write_receipt(
        &self,
        input: WriteShardToPlacement<'_>,
        receipt: ShardReceipt,
    ) -> Result<CoreObjectPlacement> {
        self.placement_from_receipt(
            ReceiptPlacementInput {
                block_id: input.block_id,
                shard_index: input.shard_index,
                profile: input.profile,
                placement: input.placement,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                boundary_summary_hash: input.boundary_summary_hash,
                expected_shard_hash: input.shard_hash,
                expected_shard_length: u64::try_from(input.shard.len())
                    .context("CoreStore remote shard length exceeds u64")?,
                generation: 1,
            },
            receipt,
        )
    }

    fn placement_from_receipt(
        &self,
        input: ReceiptPlacementInput<'_>,
        receipt: ShardReceipt,
    ) -> Result<CoreObjectPlacement> {
        validate_shard_receipt_binding(input, &receipt)?;
        self.verify_core_receipt_signature(
            &input.placement.node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )?;
        Ok(CoreObjectPlacement {
            shard_index: input.shard_index,
            node_id: input.placement.node_id.clone(),
            region_id: input.placement.region_id.clone(),
            cell_id: input.placement.cell_id.clone(),
            shard_hash: input.expected_shard_hash.to_string(),
            stored_size: input.expected_shard_length,
            generation: input.generation,
            placement_epoch: input.placement_epoch,
            fsync_sequence: receipt.fsync_sequence,
            written_at_unix_nanos: receipt.written_at_unix_nanos,
            signed_payload_hash: receipt.signed_payload_hash,
            signature_algorithm: "ed25519".to_string(),
            receipt_signature: receipt.signature,
        })
    }

    pub(super) fn internal_request_header(&self, operation: &str) -> Result<InternalRequestHeader> {
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        let signed_payload_hash = internal_request_payload_hash(
            operation,
            &request_id,
            &self.node_identity.node_id,
            LOCAL_PLACEMENT_EPOCH,
        );
        Ok(InternalRequestHeader {
            request_id,
            trace_id: String::new(),
            source_node_id: self.node_identity.node_id.clone(),
            membership_epoch: LOCAL_PLACEMENT_EPOCH,
            source_node_fence: 0,
            signature: self.sign_internal_core_receipt(&signed_payload_hash)?,
        })
    }
}

fn validate_shard_receipt_binding(
    input: ReceiptPlacementInput<'_>,
    receipt: &ShardReceipt,
) -> Result<()> {
    if receipt.node_id != input.placement.node_id {
        bail!(
            "CoreStore shard receipt node mismatch: expected {}, got {}",
            input.placement.node_id,
            receipt.node_id
        );
    }
    if receipt.block_id != input.block_id || receipt.shard_index != u32::from(input.shard_index) {
        bail!("CoreStore shard receipt scope mismatch");
    }
    if receipt.shard_hash != input.expected_shard_hash {
        bail!(
            "CoreStore shard receipt hash mismatch: expected {}, got {}",
            input.expected_shard_hash,
            receipt.shard_hash
        );
    }
    if receipt.shard_length != input.expected_shard_length {
        bail!(
            "CoreStore shard receipt length mismatch: expected {}, got {}",
            input.expected_shard_length,
            receipt.shard_length
        );
    }
    let expected_signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
        block_id: input.block_id,
        shard_index: input.shard_index,
        erasure_profile: input.profile.id,
        node_id: &input.placement.node_id,
        region_id: &input.placement.region_id,
        cell_id: &input.placement.cell_id,
        placement_epoch: input.placement_epoch,
        shard_length: input.expected_shard_length,
        shard_hash: input.expected_shard_hash,
        fsync_sequence: receipt.fsync_sequence,
        written_at_unix_nanos: receipt.written_at_unix_nanos,
        boundary_summary_hash: input.boundary_summary_hash,
    });
    validate_shard_receipt_common(
        &input.placement.node_id,
        &input.placement.region_id,
        &input.placement.cell_id,
        input.expected_shard_hash,
        input.expected_shard_length,
        receipt.fsync_sequence,
        receipt.written_at_unix_nanos,
        &receipt.signed_payload_hash,
        "ed25519",
        &receipt.signature,
        &expected_signed_payload_hash,
    )
}

#[derive(Clone, Copy)]
pub(super) struct WriteShardToPlacement<'a> {
    pub logical_file_id: &'a str,
    pub block_id: &'a str,
    pub shard_index: u16,
    pub shard: &'a [u8],
    pub shard_hash: &'a str,
    pub logical_offset: u64,
    pub profile: LocalErasureProfile,
    pub placement: &'a LocalShardPlacement,
    pub boundary_summary_hash: &'a str,
    pub boundary_values_b64: &'a str,
    pub mutation_id: &'a str,
    pub compression_algorithm: &'a str,
    pub encryption_algorithm: &'a str,
    pub writer_family: &'a str,
}

#[derive(Clone, Copy)]
pub(super) struct ReadShardFromPlacement<'a> {
    pub block_id: &'a str,
    pub profile: LocalErasureProfile,
    pub placement: &'a CoreObjectPlacement,
    pub boundary_summary_hash: &'a str,
    pub boundary_values_b64: &'a str,
    pub range: Option<CoreByteRange>,
    pub operation: &'static str,
}

#[derive(Clone, Copy)]
pub(super) struct RepairShardToPlacement<'a> {
    pub logical_file_id: &'a str,
    pub block_id: &'a str,
    pub shard_index: u16,
    pub shard: &'a [u8],
    pub shard_hash: &'a str,
    pub logical_offset: u64,
    pub profile: LocalErasureProfile,
    pub placement: &'a LocalShardPlacement,
    pub placement_epoch: u64,
    pub generation: u64,
    pub boundary_summary_hash: &'a str,
    pub boundary_values_b64: &'a str,
    pub mutation_id: &'a str,
    pub repair_finding_id: &'a str,
    pub compression_algorithm: &'a str,
    pub encryption_algorithm: &'a str,
    pub writer_family: &'a str,
}

#[derive(Clone, Copy)]
struct ShardWriteInput<'a> {
    logical_file_id: &'a str,
    block_id: &'a str,
    shard_index: u16,
    shard: &'a [u8],
    shard_hash: &'a str,
    profile: LocalErasureProfile,
    boundary_summary_hash: &'a str,
    boundary_values_b64: &'a str,
    mutation_id: &'a str,
    compression_algorithm: &'a str,
    encryption_algorithm: &'a str,
    writer_family: &'a str,
}

fn validate_shard_write_input(input: ShardWriteInput<'_>) -> Result<u64> {
    for (value, label) in [
        (input.logical_file_id, "logical file id"),
        (input.block_id, "block id"),
        (input.shard_hash, "shard hash"),
        (input.boundary_summary_hash, "boundary summary hash"),
        (input.mutation_id, "mutation id"),
        (input.compression_algorithm, "compression algorithm"),
        (input.encryption_algorithm, "encryption algorithm"),
        (input.writer_family, "writer family"),
    ] {
        if value.len() > MAX_SHARD_IDENTITY_FIELD_BYTES {
            bail!("CoreStore shard {label} exceeds bounded size");
        }
    }
    if input.boundary_values_b64.len() > MAX_SHARD_CONTEXT_FIELD_BYTES {
        bail!("CoreStore shard boundary values exceed bounded size");
    }

    validate_logical_file_id(input.logical_file_id, "shard logical file id")?;
    validate_logical_id(input.block_id, "shard block id")?;
    validate_hash(input.shard_hash, "shard hash")?;
    validate_logical_id(input.mutation_id, "shard mutation id")?;
    validate_writer_family(input.writer_family, "shard writer family")?;
    validate_object_blob_pipeline_options(input.compression_algorithm, input.encryption_algorithm)?;
    validate_boundary_summary_fields(input.boundary_summary_hash, input.boundary_values_b64)?;
    if usize::from(input.shard_index) >= input.profile.total_shards() {
        bail!("CoreStore shard index exceeds erasure profile shard count");
    }

    let shard_length = validate_shard_payload_length(input.profile, input.shard.len())?;
    let actual_hash = format!("sha256:{}", sha256_hex(input.shard));
    if actual_hash != input.shard_hash {
        bail!("CoreStore shard payload hash does not match requested shard hash");
    }
    Ok(shard_length)
}

fn validate_shard_payload_length(profile: LocalErasureProfile, length: usize) -> Result<u64> {
    let length = u64::try_from(length).context("CoreStore shard length exceeds u64")?;
    if length > profile.max_shard_size_bytes {
        bail!(
            "CoreStore shard length {length} exceeds {} byte profile maximum",
            profile.max_shard_size_bytes
        );
    }
    Ok(length)
}

fn validate_repair_finding_id(finding_id: &str) -> Result<()> {
    if finding_id.is_empty()
        || finding_id.len() > MAX_REPAIR_FINDING_ID_BYTES
        || finding_id.trim() != finding_id
        || finding_id.contains('/')
        || finding_id.contains('\\')
        || finding_id.chars().any(char::is_control)
    {
        bail!("CoreStore shard repair finding id is not a safe bounded identity");
    }
    Ok(())
}

fn expected_shard_read_length(input: ReadShardFromPlacement<'_>) -> Result<u64> {
    if input.placement.stored_size > input.profile.max_shard_size_bytes {
        bail!("CoreStore shard placement exceeds the bounded profile shard size");
    }
    let Some(range) = input.range else {
        return Ok(input.placement.stored_size);
    };
    if range.start > range.end_exclusive || range.end_exclusive > input.placement.stored_size {
        bail!("CoreStore shard range is outside the stored shard");
    }
    let length = range.end_exclusive - range.start;
    if length > input.profile.max_shard_size_bytes {
        bail!("CoreStore shard range exceeds the bounded profile shard size");
    }
    Ok(length)
}

fn put_shard_request(
    header: InternalRequestHeader,
    input: WriteShardToPlacement<'_>,
) -> PutShardRequest {
    PutShardRequest {
        header: Some(header),
        logical_file_id: input.logical_file_id.to_string(),
        logical_offset: input.logical_offset,
        block_id: input.block_id.to_string(),
        shard_index: u32::from(input.shard_index),
        erasure_profile_id: input.profile.id.to_string(),
        placement_epoch: LOCAL_PLACEMENT_EPOCH,
        shard_bytes: input.shard.to_vec(),
        shard_hash: input.shard_hash.to_string(),
        boundary_summary_hash: input.boundary_summary_hash.to_string(),
        boundary_values_b64: input.boundary_values_b64.to_string(),
        compression_algorithm: input.compression_algorithm.to_string(),
        encryption_algorithm: input.encryption_algorithm.to_string(),
        writer_family: input.writer_family.to_string(),
        mutation_id: input.mutation_id.to_string(),
    }
}

fn repair_shard_request(
    header: InternalRequestHeader,
    input: RepairShardToPlacement<'_>,
) -> RepairShardRequest {
    RepairShardRequest {
        header: Some(header),
        block_id: input.block_id.to_string(),
        shard_index: u32::from(input.shard_index),
        shard_bytes: input.shard.to_vec(),
        shard_hash: input.shard_hash.to_string(),
        repair_finding_id: input.repair_finding_id.to_string(),
        erasure_profile_id: input.profile.id.to_string(),
        placement_epoch: input.placement_epoch,
        boundary_summary_hash: input.boundary_summary_hash.to_string(),
        writer_family: input.writer_family.to_string(),
        mutation_id: input.mutation_id.to_string(),
        logical_file_id: input.logical_file_id.to_string(),
        boundary_values_b64: input.boundary_values_b64.to_string(),
        logical_offset: input.logical_offset,
        encryption_algorithm: input.encryption_algorithm.to_string(),
        compression_algorithm: input.compression_algorithm.to_string(),
    }
}

#[derive(Clone, Copy)]
struct ReceiptPlacementInput<'a> {
    block_id: &'a str,
    shard_index: u16,
    profile: LocalErasureProfile,
    placement: &'a LocalShardPlacement,
    placement_epoch: u64,
    boundary_summary_hash: &'a str,
    expected_shard_hash: &'a str,
    expected_shard_length: u64,
    generation: u64,
}

fn sort_shard_candidates(candidates: &mut [LocalShardPlacement]) {
    candidates.sort_by(|a, b| {
        b.region_weight
            .cmp(&a.region_weight)
            .then_with(|| b.cell_weight.cmp(&a.cell_weight))
            .then_with(|| a.region_id.cmp(&b.region_id))
            .then_with(|| a.failure_domain.cmp(&b.failure_domain))
            .then_with(|| a.cell_id.cmp(&b.cell_id))
            .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
    });
}

pub(super) fn choose_spread_placements(
    profile: LocalErasureProfile,
    candidates: Vec<LocalShardPlacement>,
    boundary_values: &[CoreBoundaryValue],
) -> Result<Vec<LocalShardPlacement>> {
    let total = profile.total_shards();
    if candidates.len() < total {
        bail!(
            "CoreStore placement for {} requires {} active nodes, got {}",
            profile.id,
            total,
            candidates.len()
        );
    }
    let candidates = boundary_rotated_candidates(candidates, boundary_values);
    let mut by_failure_domain = BTreeMap::<String, Vec<LocalShardPlacement>>::new();
    for candidate in candidates {
        by_failure_domain
            .entry(candidate.failure_domain.clone())
            .or_default()
            .push(candidate);
    }
    for nodes in by_failure_domain.values_mut() {
        nodes.sort_by(|a, b| {
            b.region_weight
                .cmp(&a.region_weight)
                .then_with(|| b.cell_weight.cmp(&a.cell_weight))
                .then_with(|| a.region_id.cmp(&b.region_id))
                .then_with(|| a.cell_id.cmp(&b.cell_id))
                .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
        });
    }
    let mut placements = Vec::with_capacity(total);
    while placements.len() < total {
        let mut made_progress = false;
        let failure_domains = by_failure_domain.keys().cloned().collect::<Vec<_>>();
        for failure_domain in failure_domains {
            if placements.len() == total {
                break;
            }
            if let Some(nodes) = by_failure_domain.get_mut(&failure_domain) {
                if !nodes.is_empty() {
                    placements.push(nodes.remove(0));
                    made_progress = true;
                }
            }
        }
        if !made_progress {
            break;
        }
    }
    if placements.len() != total {
        bail!("CoreStore placement planner exhausted candidates");
    }
    Ok(placements)
}

fn boundary_rotated_candidates(
    mut candidates: Vec<LocalShardPlacement>,
    boundary_values: &[CoreBoundaryValue],
) -> Vec<LocalShardPlacement> {
    if candidates.len() < 2 {
        return candidates;
    }
    let seed_parts = boundary_values
        .iter()
        .filter(|value| {
            (!value.placement_affinity.is_empty() && value.placement_affinity != "none")
                || value
                    .categories
                    .iter()
                    .any(|category| category == "placement_affinity")
        })
        .map(|value| {
            format!(
                "{}\0{}\0{}\0{}",
                value.name, value.value_type, value.value, value.placement_affinity
            )
        })
        .collect::<Vec<_>>();
    if seed_parts.is_empty() {
        return candidates;
    }
    candidates.sort_by(|a, b| {
        b.region_weight
            .cmp(&a.region_weight)
            .then_with(|| b.cell_weight.cmp(&a.cell_weight))
            .then_with(|| a.region_id.cmp(&b.region_id))
            .then_with(|| a.failure_domain.cmp(&b.failure_domain))
            .then_with(|| a.cell_id.cmp(&b.cell_id))
            .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
    });
    let seed = sha256_hex(seed_parts.join("\u{1f}").as_bytes());
    let rotation = usize::from_str_radix(&seed[0..8], 16).unwrap_or(0) % candidates.len();
    candidates.rotate_left(rotation);
    candidates
}

fn compare_node_ids(left: &str, right: &str) -> std::cmp::Ordering {
    match (local_node_ordinal(left), local_node_ordinal(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn local_node_ordinal(node_id: &str) -> Option<u64> {
    node_id
        .strip_prefix(LOCAL_NODE_ID_PREFIX)?
        .strip_prefix('-')?
        .parse()
        .ok()
}

fn internal_request_payload_hash(
    operation: &str,
    request_id: &str,
    source_node_id: &str,
    membership_epoch: u64,
) -> String {
    let mut bytes = Vec::new();
    for part in [
        "anvil.internal.request.v1",
        operation,
        request_id,
        source_node_id,
        &membership_epoch.to_string(),
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    format!("sha256:{}", sha256_hex(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_placement() -> LocalShardPlacement {
        LocalShardPlacement {
            node_id: "node-b".to_string(),
            region_id: "r1".to_string(),
            cell_id: "c2".to_string(),
            failure_domain: "c2".to_string(),
            region_weight: 100,
            cell_weight: 100,
            public_api_addr: "http://node-b".to_string(),
            is_local: false,
        }
    }

    #[test]
    fn remote_put_preserves_core_control_block_identity() {
        let placement = remote_placement();
        let request = put_shard_request(
            InternalRequestHeader::default(),
            WriteShardToPlacement {
                logical_file_id: "lf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                block_id: "blk_core_control",
                shard_index: 2,
                shard: b"core-control-shard",
                shard_hash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                logical_offset: 8_192,
                profile: LOCAL_EC_4_2_PROFILE,
                placement: &placement,
                boundary_summary_hash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                boundary_values_b64: "boundary-values",
                mutation_id: "core-control-mutation",
                compression_algorithm: "zstd",
                encryption_algorithm: "none",
                writer_family: WriterFamily::CoreControl.as_str(),
            },
        );

        assert_eq!(
            request.logical_file_id,
            "lf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(request.logical_offset, 8_192);
        assert_eq!(request.compression_algorithm, "zstd");
        assert_eq!(request.encryption_algorithm, "none");
        assert_eq!(request.writer_family, WriterFamily::CoreControl.as_str());
        assert_eq!(request.mutation_id, "core-control-mutation");
    }

    #[test]
    fn remote_repair_preserves_stream_block_identity() {
        let placement = remote_placement();
        let request = repair_shard_request(
            InternalRequestHeader::default(),
            RepairShardToPlacement {
                logical_file_id: "lf_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                block_id: "blk_stream",
                shard_index: 4,
                shard: b"stream-shard",
                shard_hash: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                logical_offset: 65_536,
                profile: LOCAL_EC_4_2_PROFILE,
                placement: &placement,
                placement_epoch: 7,
                generation: 3,
                boundary_summary_hash: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                boundary_values_b64: "boundary-values",
                mutation_id: "original-stream-mutation",
                repair_finding_id: "repair-finding",
                compression_algorithm: "zstd",
                encryption_algorithm: "aes_gcm_siv",
                writer_family: WriterFamily::Stream.as_str(),
            },
        );

        assert_eq!(
            request.logical_file_id,
            "lf_dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );
        assert_eq!(request.logical_offset, 65_536);
        assert_eq!(request.compression_algorithm, "zstd");
        assert_eq!(request.encryption_algorithm, "aes_gcm_siv");
        assert_eq!(request.writer_family, WriterFamily::Stream.as_str());
        assert_eq!(request.mutation_id, "original-stream-mutation");
        assert_eq!(request.repair_finding_id, "repair-finding");
    }

    #[test]
    fn receipt_binding_uses_requested_hash_length_and_boundary_context() {
        const SHARD_HASH: &str =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const OTHER_HASH: &str =
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        const BOUNDARY_HASH: &str =
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        const OTHER_BOUNDARY_HASH: &str =
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

        let placement = remote_placement();
        let input = ReceiptPlacementInput {
            block_id: "blk_receipt",
            shard_index: 2,
            profile: LOCAL_EC_4_2_PROFILE,
            placement: &placement,
            placement_epoch: 7,
            boundary_summary_hash: BOUNDARY_HASH,
            expected_shard_hash: SHARD_HASH,
            expected_shard_length: 4096,
            generation: 3,
        };
        let receipt = receipt_for_binding(input);

        validate_shard_receipt_binding(input, &receipt).unwrap();

        let mut wrong_hash = receipt.clone();
        wrong_hash.shard_hash = OTHER_HASH.to_string();
        assert!(
            validate_shard_receipt_binding(input, &wrong_hash)
                .unwrap_err()
                .to_string()
                .contains("receipt hash mismatch")
        );

        let mut wrong_length = receipt.clone();
        wrong_length.shard_length += 1;
        assert!(
            validate_shard_receipt_binding(input, &wrong_length)
                .unwrap_err()
                .to_string()
                .contains("receipt length mismatch")
        );

        let wrong_boundary = ReceiptPlacementInput {
            boundary_summary_hash: OTHER_BOUNDARY_HASH,
            ..input
        };
        assert!(
            validate_shard_receipt_binding(wrong_boundary, &receipt)
                .unwrap_err()
                .to_string()
                .contains("signed payload hash mismatch")
        );
    }

    #[test]
    fn shard_payload_length_is_bounded_by_the_compiled_profile() {
        let maximum = usize::try_from(LOCAL_EC_4_2_PROFILE.max_shard_size_bytes).unwrap();

        assert_eq!(
            validate_shard_payload_length(LOCAL_EC_4_2_PROFILE, maximum).unwrap(),
            LOCAL_EC_4_2_PROFILE.max_shard_size_bytes
        );
        assert!(
            validate_shard_payload_length(LOCAL_EC_4_2_PROFILE, maximum + 1)
                .unwrap_err()
                .to_string()
                .contains("profile maximum")
        );
    }

    fn receipt_for_binding(input: ReceiptPlacementInput<'_>) -> ShardReceipt {
        let fsync_sequence = 11;
        let written_at_unix_nanos = 123_456;
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: input.block_id,
            shard_index: input.shard_index,
            erasure_profile: input.profile.id,
            node_id: &input.placement.node_id,
            region_id: &input.placement.region_id,
            cell_id: &input.placement.cell_id,
            placement_epoch: input.placement_epoch,
            shard_length: input.expected_shard_length,
            shard_hash: input.expected_shard_hash,
            fsync_sequence,
            written_at_unix_nanos,
            boundary_summary_hash: input.boundary_summary_hash,
        });
        ShardReceipt {
            node_id: input.placement.node_id.clone(),
            block_id: input.block_id.to_string(),
            shard_index: u32::from(input.shard_index),
            shard_hash: input.expected_shard_hash.to_string(),
            shard_length: input.expected_shard_length,
            fsync_sequence,
            written_at_unix_nanos,
            signed_payload_hash,
            signature: vec![1],
        }
    }
}
