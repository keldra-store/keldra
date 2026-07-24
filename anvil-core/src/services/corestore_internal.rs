use crate::anvil_api::anti_entropy_internal_server::AntiEntropyInternal;
use crate::anvil_api::block_store_internal_server::BlockStoreInternal;
use crate::anvil_api::core_meta_replication_internal_server::CoreMetaReplicationInternal;
use crate::anvil_api::root_register_internal_server::RootRegisterInternal;
use crate::anvil_api::*;
use crate::core_store::{self, CoreInternalGetShard, CoreInternalPutShard, CoreMetaEncodedRow};
use crate::mesh_lifecycle::NodeCapability;
use crate::{AppState, auth, diagnostic_store, system_realm, task_lease};
use futures_util::StreamExt;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

mod cross_region_proxy;
mod pending_finalisation;
mod request_validation;
use self::request_validation::*;

#[tonic::async_trait]
impl BlockStoreInternal for AppState {
    type GetShardStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ShardChunk, Status>> + Send>>;

    async fn put_shard(
        &self,
        request: Request<PutShardRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        validate_put_shard_request(&req)?;
        let writer_family = if req.writer_family.trim().is_empty() {
            return Err(Status::invalid_argument("writer_family is required"));
        } else {
            req.writer_family
        };
        let mutation_id = if req.mutation_id.trim().is_empty() {
            request_id_from_header(req.header.as_ref())
        } else {
            req.mutation_id
        };
        let receipt = self
            .core_store
            .put_internal_shard(CoreInternalPutShard {
                logical_file_id: req.logical_file_id,
                logical_offset: req.logical_offset,
                block_id: req.block_id,
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_bytes: req.shard_bytes,
                shard_hash: req.shard_hash,
                boundary_summary_hash: req.boundary_summary_hash,
                boundary_values_b64: req.boundary_values_b64,
                compression_algorithm: req.compression_algorithm,
                encryption_algorithm: req.encryption_algorithm,
                writer_family,
                mutation_id,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<Self::GetShardStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        validate_shard_read_scope(
            &req.block_id,
            req.shard_index,
            &req.erasure_profile_id,
            &req.shard_hash,
            &req.boundary_summary_hash,
        )?;
        let range = bounded_shard_range(req.range_start, req.range_end_exclusive)?;
        let bytes = self
            .core_store
            .read_internal_shard_range(CoreInternalGetShard {
                block_id: req.block_id.clone(),
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range,
            })
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ShardChunk {
                    block_id: req.block_id,
                    shard_index: req.shard_index,
                    offset: req.range_start,
                    data: bytes,
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_shard_receipt(
        &self,
        request: Request<GetShardReceiptRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        validate_shard_read_scope(
            &req.block_id,
            req.shard_index,
            &req.erasure_profile_id,
            &req.shard_hash,
            &req.boundary_summary_hash,
        )?;
        let receipt = self
            .core_store
            .get_internal_shard_receipt(CoreInternalGetShard {
                block_id: req.block_id,
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range: None,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }

    async fn repair_shard(
        &self,
        request: Request<RepairShardRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        validate_repair_shard_request(&req)?;
        let mutation_id = repair_mutation_id(&req)?;
        let repair_operation_id = req.repair_finding_id.clone();
        let receipt = self
            .core_store
            .repair_internal_shard(
                CoreInternalPutShard {
                    logical_file_id: req.logical_file_id,
                    logical_offset: req.logical_offset,
                    block_id: req.block_id,
                    shard_index: u16::try_from(req.shard_index)
                        .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                    erasure_profile_id: req.erasure_profile_id,
                    placement_epoch: req.placement_epoch,
                    shard_bytes: req.shard_bytes,
                    shard_hash: req.shard_hash,
                    boundary_summary_hash: req.boundary_summary_hash,
                    boundary_values_b64: req.boundary_values_b64,
                    compression_algorithm: req.compression_algorithm,
                    encryption_algorithm: req.encryption_algorithm,
                    writer_family: req.writer_family,
                    mutation_id,
                },
                &repair_operation_id,
            )
            .await
            .map_err(repair_shard_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }
}

#[tonic::async_trait]
impl CoreMetaReplicationInternal for AppState {
    type CoreMetaStreamStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<CoreMetaStreamResponse, Status>> + Send>>;

    async fn replicate_pending_batches(
        &self,
        request: Request<CoreMetaBatchGroupRequest>,
    ) -> Result<Response<CoreMetaPrepareReceiptGroup>, Status> {
        let total_started_at = Instant::now();
        let auth_started_at = Instant::now();
        ensure_internal_node_request(self, &request).await?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches authorise",
            auth_started_at.elapsed(),
        );
        let validation_started_at = Instant::now();
        let req = request.into_inner();
        validate_coremeta_batch_group_bounds(&req)?;
        if req.batches.is_empty() {
            return Err(Status::invalid_argument(
                "CoreMeta batch group must not be empty",
            ));
        }
        if req.publication_intent.is_empty() {
            return Err(Status::invalid_argument(
                "CoreMeta batch group must include a durable publication intent",
            ));
        }
        let mut marker_rows = Vec::with_capacity(req.batches.len());
        let mut rows_by_root = BTreeMap::new();
        let mut roots = BTreeSet::new();
        for batch in &req.batches {
            if !roots.insert(batch.root_key_hash.as_str()) {
                return Err(Status::invalid_argument(
                    "CoreMeta batch group contains a duplicate root",
                ));
            }
            if batch.visibility_state != "pending" {
                return Err(Status::invalid_argument(
                    "CoreMeta batch group entries must have pending visibility",
                ));
            }
            let rows = request_rows_checked(&batch.mutations)?;
            let delete_common = core_store::core_meta_committed_row_common(
                "system/coremeta-publication-intent",
                &batch.root_key_hash,
                batch.post_root_generation,
                &batch.transaction_id,
                1,
            );
            let owned_rows = self
                .core_store
                .validate_and_own_coremeta_encoded_rows(&rows, Some(&delete_common))
                .map_err(internal_status)?;
            let row_hashes = batch
                .mutations
                .iter()
                .map(|row| row.row_hash.clone())
                .collect();
            let expected_pending_hash =
                core_store::pending_batch_hash(&core_store::CoreMetaPendingBatchInput {
                    root_key_hash: batch.root_key_hash.clone(),
                    expected_root_generation: batch.expected_root_generation,
                    post_root_generation: batch.post_root_generation,
                    transaction_id: batch.transaction_id.clone(),
                    row_hashes,
                })
                .map_err(internal_status)?;
            if batch.pending_batch_hash != expected_pending_hash {
                return Err(Status::failed_precondition(
                    "CoreMeta pending batch hash mismatch",
                ));
            }
            marker_rows.push(
                self.core_store
                    .coremeta_pending_batch_marker_encoded_row(
                        &batch.root_key_hash,
                        batch.expected_root_generation,
                        batch.post_root_generation,
                        &batch.transaction_id,
                        &batch.pending_batch_hash,
                        rows.len(),
                    )
                    .map_err(internal_status)?,
            );
            rows_by_root.insert(batch.root_key_hash.clone(), owned_rows);
        }
        self.core_store
            .stage_replica_root_publication_intent(&req.publication_intent, &rows_by_root)
            .map_err(internal_status)?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches validate",
            validation_started_at.elapsed(),
        );
        let borrowed = marker_rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        let write_started_at = Instant::now();
        self.core_store
            .write_coremeta_encoded_rows(&borrowed)
            .map_err(internal_status)?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches rocksdb_write",
            write_started_at.elapsed(),
        );
        let receipt_started_at = Instant::now();
        let receipts = req
            .batches
            .iter()
            .map(|batch| local_prepare_receipt(&self.core_store, &self.config.node_id, batch, 1))
            .collect::<Result<Vec<_>, _>>()?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches sign_receipts",
            receipt_started_at.elapsed(),
        );
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches total",
            total_started_at.elapsed(),
        );
        Ok(Response::new(CoreMetaPrepareReceiptGroup { receipts }))
    }

    async fn persist_commit_certificates(
        &self,
        request: Request<CoreMetaPersistCommitGroupRequest>,
    ) -> Result<Response<CoreMetaCertificatePersistReceiptGroup>, Status> {
        let total_started_at = Instant::now();
        let auth_started_at = Instant::now();
        ensure_internal_node_request(self, &request).await?;
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates authorise",
            auth_started_at.elapsed(),
        );
        let validation_started_at = Instant::now();
        let req = request.into_inner();
        validate_coremeta_commit_group_bounds(&req)?;
        if req.commits.is_empty() {
            return Err(Status::invalid_argument(
                "CoreMeta commit group must not be empty",
            ));
        }
        let mut receipts = Vec::with_capacity(req.commits.len());
        let mut evidence_rows = Vec::with_capacity(req.commits.len());
        let mut committed_rows_by_root = BTreeMap::new();
        let mut roots = BTreeSet::new();
        for commit in &req.commits {
            let cert = commit
                .commit_certificate
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("commit_certificate is required"))?;
            if !roots.insert(cert.root_key_hash.as_str()) {
                return Err(Status::invalid_argument(
                    "CoreMeta commit group contains a duplicate root",
                ));
            }
            let rows = request_rows_checked(&commit.committed_rows)?;
            let delete_common = core_store::core_meta_committed_row_common(
                "system/coremeta-publication-intent",
                &cert.root_key_hash,
                cert.post_root_generation,
                &cert.transaction_id,
                1,
            );
            let owned_rows = self
                .core_store
                .validate_and_own_coremeta_encoded_rows(&rows, Some(&delete_common))
                .map_err(internal_status)?;
            committed_rows_by_root.insert(cert.root_key_hash.clone(), owned_rows);
            let row_hashes = commit
                .committed_rows
                .iter()
                .map(|row| row.row_hash.clone())
                .collect();
            let expected_committed_hash =
                core_store::committed_batch_hash(&core_store::CoreMetaCommittedBatchInput {
                    root_key_hash: cert.root_key_hash.clone(),
                    expected_root_generation: cert.expected_root_generation,
                    post_root_generation: cert.post_root_generation,
                    transaction_id: cert.transaction_id.clone(),
                    pending_batch_hash: cert.pending_batch_hash.clone(),
                    committed_row_hashes: row_hashes,
                })
                .map_err(internal_status)?;
            if commit.committed_batch_hash != expected_committed_hash {
                return Err(Status::failed_precondition(
                    "CoreMeta committed batch hash mismatch",
                ));
            }
            let core_cert = api_commit_certificate_to_core(cert)?;
            core_store::validate_commit_certificate_with_verifier(
                &self
                    .core_store
                    .default_coremeta_quorum_profile()
                    .map_err(internal_status)?,
                &core_cert,
                |node_id, signed_payload_hash, signature| {
                    self.core_store.verify_internal_core_receipt_signature(
                        node_id,
                        signed_payload_hash,
                        signature,
                    )
                },
            )
            .map_err(internal_status)?;
            let receipt = local_persist_receipt(
                &self.core_store,
                &self.config.node_id,
                cert,
                &commit.committed_batch_hash,
                1,
            )?;
            let created_at_unix_nanos = self
                .core_store
                .root_publication_intent_created_at(&cert.transaction_id)
                .map_err(internal_status)?;
            evidence_rows.push(
                self.core_store
                    .coremeta_commit_evidence_encoded_row_at(
                        &cert.root_key_hash,
                        cert.post_root_generation,
                        &cert.transaction_id,
                        &cert.certificate_hash,
                        &commit.committed_batch_hash,
                        core_store::encode_deterministic_proto(cert),
                        vec![
                            core_store::certificate_persist_receipt_payload_hash(
                                &api_persist_receipt_to_core(&receipt)?,
                            )
                            .map_err(internal_status)?,
                        ],
                        vec![core_store::encode_deterministic_proto(&receipt)],
                        created_at_unix_nanos,
                    )
                    .map_err(internal_status)?,
            );
            receipts.push(receipt);
        }
        let transaction_id = req
            .commits
            .first()
            .and_then(|commit| commit.commit_certificate.as_ref())
            .map(|certificate| certificate.transaction_id.as_str())
            .ok_or_else(|| Status::invalid_argument("commit_certificate is required"))?;
        if req.commits.iter().any(|commit| {
            commit
                .commit_certificate
                .as_ref()
                .map_or(true, |certificate| {
                    certificate.transaction_id != transaction_id
                })
        }) {
            return Err(Status::invalid_argument(
                "CoreMeta commit group spans multiple publication intents",
            ));
        }
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates validate_and_sign",
            validation_started_at.elapsed(),
        );
        let write_started_at = Instant::now();
        self.core_store
            .persist_replica_publication_certificate_evidence(
                transaction_id,
                &committed_rows_by_root,
                &evidence_rows,
            )
            .map_err(internal_status)?;
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates rocksdb_write",
            write_started_at.elapsed(),
        );
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates total",
            total_started_at.elapsed(),
        );
        Ok(Response::new(CoreMetaCertificatePersistReceiptGroup {
            receipts,
        }))
    }

    async fn core_meta_stream(
        &self,
        request: Request<tonic::Streaming<CoreMetaStreamRequest>>,
    ) -> Result<Response<Self::CoreMetaStreamStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let state = self.clone();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                let frame = match item {
                    Ok(frame) => frame,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                if let Err(status) = ensure_message_size(
                    &frame,
                    MAX_INTERNAL_RPC_REQUEST_BYTES,
                    "CoreMeta stream frame",
                ) {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
                let request_id = frame.request_id.clone();
                if request_id.trim().is_empty() {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "CoreMeta stream request_id is required",
                        )))
                        .await;
                    return;
                }
                if request_id.len() > MAX_INTERNAL_HEADER_FIELD_BYTES {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "CoreMeta stream request_id exceeds bounded size",
                        )))
                        .await;
                    return;
                }

                let result = match frame.command {
                    Some(core_meta_stream_request::Command::ReplicatePendingBatches(command)) => {
                        let mut request = Request::new(command);
                        request.extensions_mut().insert(claims.clone());
                        CoreMetaReplicationInternal::replicate_pending_batches(&state, request)
                            .await
                            .map(|response| CoreMetaStreamResponse {
                                request_id,
                                result: Some(core_meta_stream_response::Result::PrepareReceipts(
                                    response.into_inner(),
                                )),
                            })
                    }
                    Some(core_meta_stream_request::Command::PersistCommitCertificates(command)) => {
                        let mut request = Request::new(command);
                        request.extensions_mut().insert(claims.clone());
                        CoreMetaReplicationInternal::persist_commit_certificates(&state, request)
                            .await
                            .map(|response| CoreMetaStreamResponse {
                                request_id,
                                result: Some(
                                    core_meta_stream_response::Result::CertificatePersistReceipts(
                                        response.into_inner(),
                                    ),
                                ),
                            })
                    }
                    None => Err(Status::invalid_argument(
                        "CoreMeta stream command is required",
                    )),
                };

                match result {
                    Ok(response) => {
                        if tx.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn abort_pending_batch(
        &self,
        request: Request<CoreMetaAbortRequest>,
    ) -> Result<Response<crate::anvil_api::CoreMetaPrepareReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let batch = CoreMetaBatchRequest {
            header: req.header,
            root_key_hash: req.root_key_hash,
            expected_root_generation: req.expected_root_generation,
            post_root_generation: req.post_root_generation,
            transaction_id: req.transaction_id,
            visibility_state: "aborted".to_string(),
            mutations: Vec::new(),
            pending_batch_hash: req.pending_batch_hash,
        };
        let receipt = local_prepare_receipt(&self.core_store, &self.config.node_id, &batch, 1)?;
        Ok(Response::new(receipt))
    }

    type CatchUpPartitionStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<CoreMetaBatchFrame, Status>> + Send>>;

    async fn catch_up_partition(
        &self,
        request: Request<CoreMetaCatchUpRequest>,
    ) -> Result<Response<Self::CatchUpPartitionStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let max_rows =
            bounded_coremeta_history_page(req.max_rows, req.max_bytes, "CoreMeta catch-up")?;
        let frames = self
            .core_store
            .catch_up_coremeta_generation_history(
                &req.root_key_hash,
                req.after.as_ref(),
                req.through_generation,
                max_rows,
                req.max_bytes,
            )
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(frames.len().max(1));
        tokio::spawn(async move {
            for frame in frames {
                if tx.send(Ok(frame)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_core_meta_inventory(
        &self,
        request: Request<CoreMetaInventoryRequest>,
    ) -> Result<Response<CoreMetaInventory>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let max_entries =
            bounded_coremeta_history_page(req.max_entries, req.max_bytes, "CoreMeta inventory")?;
        let inventory = self
            .core_store
            .coremeta_generation_inventory(
                &req.root_key_hash,
                req.after.as_ref(),
                req.through_generation,
                max_entries,
                req.max_bytes,
            )
            .map_err(internal_status)?;
        Ok(Response::new(inventory))
    }

    async fn publish_pending_mutation_finalisation(
        &self,
        request: Request<PublishPendingMutationFinalisationRequest>,
    ) -> Result<Response<PublishPendingMutationFinalisationResponse>, Status> {
        pending_finalisation::handle(self, request).await
    }
}

trait InternalHeaderCarrier: Message {
    fn internal_header(&self) -> Option<&InternalRequestHeader>;
    fn internal_operation(&self) -> &'static str;
}

macro_rules! impl_internal_header_carrier {
    ($($ty:ty => $operation:literal),+ $(,)?) => {
        $(
            impl InternalHeaderCarrier for $ty {
                fn internal_header(&self) -> Option<&InternalRequestHeader> {
                    self.header.as_ref()
                }

                fn internal_operation(&self) -> &'static str {
                    $operation
                }
            }
        )+
    };
}

impl_internal_header_carrier!(
    PutShardRequest => "block.put_shard",
    GetShardRequest => "block.get_shard",
    GetShardReceiptRequest => "block.get_shard_receipt",
    RepairShardRequest => "block.repair_shard",
    CoreMetaBatchGroupRequest => "coremeta.replicate_pending_batches",
    CoreMetaPersistCommitGroupRequest => "coremeta.persist_commit_certificates",
    CoreMetaAbortRequest => "coremeta.abort_pending_batch",
    CoreMetaCatchUpRequest => "coremeta.catch_up_partition",
    CoreMetaInventoryRequest => "coremeta.exchange_inventory",
    PublishPendingMutationFinalisationRequest => "coremeta.publish_pending_mutation_finalisation",
    ReadRootRequest => "root.read",
    PrepareRootRequest => "root.prepare",
    CompareAndSwapRootRequest => "root.compare_and_swap",
    VoteFailoverRequest => "root.vote_failover",
    ExchangeRootInventoryRequest => "root.exchange_inventory",
    ExchangeRootDirectoryRequest => "root.exchange_directory",
    ExchangeInventoryRequest => "anti_entropy.exchange_inventory",
    PublishRepairFindingRequest => "anti_entropy.publish_repair_finding",
    ClaimRepairRequest => "anti_entropy.claim_repair",
    ProxyNativeRequest => "proxy.native",
    ProxyObjectReadRequest => "proxy.object_read",
    ProxyShardRangeRequest => "proxy.shard_range",
);

async fn ensure_internal_node_request<T: InternalHeaderCarrier>(
    state: &AppState,
    request: &Request<T>,
) -> Result<(), Status> {
    let total_started_at = Instant::now();
    ensure_message_size(
        request.get_ref(),
        MAX_INTERNAL_RPC_REQUEST_BYTES,
        "internal RPC request",
    )?;
    let header = request
        .get_ref()
        .internal_header()
        .ok_or_else(|| Status::unauthenticated("internal request header required"))?;
    validate_internal_header_bounds(header)?;
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    if header.source_node_id.trim().is_empty() || header.request_id.trim().is_empty() {
        return Err(Status::unauthenticated(
            "internal request source node and request id are required",
        ));
    }
    if header.signature.is_empty() {
        return Err(Status::unauthenticated(
            "internal request header signature is required",
        ));
    }
    if claims.sub != header.source_node_id {
        return Err(Status::permission_denied(
            "internal request source node must match authenticated principal",
        ));
    }

    let signed_payload_hash = internal_request_payload_hash(
        request.get_ref().internal_operation(),
        &header.request_id,
        &header.source_node_id,
        header.membership_epoch,
    );
    state
        .core_store
        .verify_internal_core_receipt_signature(
            &header.source_node_id,
            &signed_payload_hash,
            &header.signature,
        )
        .map_err(core_store_internal_status)?;

    let relation_started_at = Instant::now();
    let allowed = system_realm::check_internal_node_access(
        &state.storage,
        &state.core_store,
        &state.config.mesh_id,
        claims,
        &header.source_node_id,
        required_internal_node_capability(request.get_ref().internal_operation()),
    )
    .await
    .map_err(internal_status)?;
    crate::emit_test_timing(
        "coremeta.internal.authorise node_membership",
        relation_started_at.elapsed(),
    );
    if !allowed {
        return Err(Status::permission_denied(
            "active system-realm node grant and operation capability required",
        ));
    }
    crate::emit_test_timing(
        "coremeta.internal.authorise total",
        total_started_at.elapsed(),
    );
    Ok(())
}

fn required_internal_node_capability(operation: &str) -> NodeCapability {
    if operation.starts_with("block.") || operation.starts_with("anti_entropy.") {
        NodeCapability::Object
    } else if operation.starts_with("proxy.") {
        NodeCapability::Gateway
    } else {
        NodeCapability::Metadata
    }
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
    format!("sha256:{}", core_store::sha256_hex(&bytes))
}

fn validate_root_prepare_receipts(
    state: &AppState,
    root_key_hash: &str,
    expected_generation: u64,
    new_root_anchor_record: &[u8],
    register_cohort_node_ids: &[String],
    register_cohort_hash: &str,
    placement_epoch: u64,
    receipts: &[RootPrepareReceipt],
) -> Result<(), Status> {
    if receipts.len() < core_store::CORE_META_DEFAULT_QUORUM {
        return Err(Status::failed_precondition(
            "root prepare quorum not reached",
        ));
    }
    let new_root_hash = format!("sha256:{}", core_store::sha256_hex(new_root_anchor_record));
    let post_generation = expected_generation.saturating_add(1);
    if register_cohort_node_ids.len() != 3
        || placement_epoch == 0
        || register_cohort_hash
            != core_store::root_register_cohort_hash(
                root_key_hash,
                post_generation,
                register_cohort_node_ids,
            )
    {
        return Err(Status::failed_precondition(
            "root-register cohort scope mismatch",
        ));
    }
    let mut replicas = BTreeSet::new();
    let mut shard_indices = BTreeSet::new();
    for receipt in receipts {
        let shard_index = usize::try_from(receipt.shard_index)
            .map_err(|_| Status::failed_precondition("root prepare shard index overflow"))?;
        if receipt.root_key_hash != root_key_hash
            || receipt.expected_generation != expected_generation
            || receipt.post_generation != post_generation
            || receipt.new_root_hash != new_root_hash
            || receipt.register_cohort_hash != register_cohort_hash
            || receipt.placement_epoch != placement_epoch
            || receipt.fsync_sequence == 0
            || register_cohort_node_ids
                .get(shard_index)
                .map(String::as_str)
                != Some(receipt.replica_node_id.as_str())
            || receipt.signed_payload_hash != core_store::root_prepare_receipt_payload_hash(receipt)
        {
            return Err(Status::failed_precondition(
                "root prepare receipt scope mismatch",
            ));
        }
        state
            .core_store
            .verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.signed_payload_hash,
                &receipt.signature,
            )
            .map_err(internal_status)?;
        replicas.insert(receipt.replica_node_id.as_str());
        shard_indices.insert(receipt.shard_index);
    }
    if replicas.len() < core_store::CORE_META_DEFAULT_QUORUM
        || shard_indices.len() < core_store::CORE_META_DEFAULT_QUORUM
    {
        return Err(Status::failed_precondition(
            "root prepare quorum not reached",
        ));
    }
    Ok(())
}

fn validate_coordinator_publication_evidence(
    certificate: &CoreMetaCommitCertificate,
    request: &CompareAndSwapRootRequest,
) -> Result<(), Status> {
    let mut matches = request
        .participant_commit_evidence
        .iter()
        .filter(|participant| participant.root_anchor_record == request.new_root_anchor_record);
    let participant = matches.next().ok_or_else(|| {
        Status::failed_precondition("coordinator publication evidence is missing")
    })?;
    if matches.next().is_some() {
        return Err(Status::failed_precondition(
            "coordinator publication evidence is duplicated",
        ));
    }
    let participant_certificate = participant.commit_certificate.as_ref().ok_or_else(|| {
        Status::failed_precondition("coordinator publication certificate is missing")
    })?;
    if core_store::encode_deterministic_proto(participant_certificate)
        != core_store::encode_deterministic_proto(certificate)
    {
        return Err(Status::failed_precondition(
            "coordinator publication certificate mismatch",
        ));
    }
    let mut request_receipts = request
        .certificate_persist_receipts
        .iter()
        .map(core_store::encode_deterministic_proto)
        .collect::<Vec<_>>();
    request_receipts.sort();
    request_receipts.dedup();
    let mut participant_receipts = participant
        .certificate_persist_receipts
        .iter()
        .map(core_store::encode_deterministic_proto)
        .collect::<Vec<_>>();
    participant_receipts.sort();
    participant_receipts.dedup();
    if request_receipts != participant_receipts
        || request
            .certificate_persist_receipts
            .iter()
            .any(|receipt| receipt.committed_batch_hash != participant.committed_batch_hash)
    {
        return Err(Status::failed_precondition(
            "coordinator publication persist evidence mismatch",
        ));
    }
    Ok(())
}

fn request_rows_checked(
    mutations: &[CoreMetaRowMutation],
) -> Result<Vec<CoreMetaEncodedRow<'_>>, Status> {
    mutations
        .iter()
        .map(|row| {
            let actual = core_store::core_meta_encoded_row_hash_with_delete(
                &row.column_family,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            );
            if actual != row.row_hash {
                return Err(Status::invalid_argument("CoreMeta row_hash mismatch"));
            }
            Ok(CoreMetaEncodedRow {
                cf: row.column_family.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
        })
        .collect()
}

fn hash_string_list(domain: &str, values: &[String]) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(domain.as_bytes());
    for value in values {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    format!("sha256:{}", core_store::sha256_hex(&bytes))
}

fn complete_coremeta_history_inventory_hash(
    store: &core_store::CoreStore,
    root_key_hash: &str,
) -> Result<String, Status> {
    let mut after = None;
    let mut through_generation = 0;
    let mut generation_hashes = Vec::new();
    loop {
        let inventory = store
            .coremeta_generation_inventory(
                root_key_hash,
                after.as_ref(),
                through_generation,
                MAX_COREMETA_HISTORY_PAGE_ROWS,
                MAX_COREMETA_HISTORY_PAGE_BYTES,
            )
            .map_err(internal_status)?;
        if through_generation == 0 {
            through_generation = inventory.final_generation;
        }
        generation_hashes.extend(
            inventory.descriptors.iter().map(|descriptor| {
                format!("{}:{}", descriptor.generation, descriptor.generation_hash)
            }),
        );
        if inventory.inventory_complete {
            break;
        }
        after = inventory.next_cursor;
    }
    Ok(hash_string_list(
        "anvil.antientropy.coremeta.inventory.v1",
        &generation_hashes,
    ))
}

fn local_prepare_receipt(
    core_store: &core_store::CoreStore,
    replica_node_id: &str,
    request: &CoreMetaBatchRequest,
    write_sequence: u64,
) -> Result<crate::anvil_api::CoreMetaPrepareReceipt, Status> {
    let mut receipt = core_store::CoreMetaPrepareReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence,
        pending_batch_hash: request.pending_batch_hash.clone(),
        root_key_hash: request.root_key_hash.clone(),
        expected_root_generation: request.expected_root_generation,
        post_root_generation: request.post_root_generation,
        transaction_id: request.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash =
        core_store::prepare_receipt_payload_hash(&receipt).map_err(internal_status)?;
    let signature = core_store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .map_err(internal_status)?;
    Ok(crate::anvil_api::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash,
        root_key_hash: receipt.root_key_hash,
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signature,
    })
}

fn local_persist_receipt(
    core_store: &core_store::CoreStore,
    replica_node_id: &str,
    certificate: &crate::anvil_api::CoreMetaCommitCertificate,
    committed_batch_hash: &str,
    write_sequence: u64,
) -> Result<crate::anvil_api::CoreMetaCertificatePersistReceipt, Status> {
    let mut receipt = core_store::CoreMetaCertificatePersistReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence,
        certificate_hash: certificate.certificate_hash.clone(),
        committed_batch_hash: committed_batch_hash.to_string(),
        root_key_hash: certificate.root_key_hash.clone(),
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash =
        core_store::certificate_persist_receipt_payload_hash(&receipt).map_err(internal_status)?;
    let signature = core_store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .map_err(internal_status)?;
    Ok(crate::anvil_api::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash,
        committed_batch_hash: receipt.committed_batch_hash,
        root_key_hash: receipt.root_key_hash,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signature,
    })
}

fn api_prepare_receipt_to_core(
    receipt: &crate::anvil_api::CoreMetaPrepareReceipt,
) -> Result<core_store::CoreMetaPrepareReceipt, Status> {
    let mut core = core_store::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: receipt.signature.clone(),
    };
    core.signed_payload_hash =
        core_store::prepare_receipt_payload_hash(&core).map_err(internal_status)?;
    Ok(core)
}

fn api_commit_certificate_to_core(
    certificate: &crate::anvil_api::CoreMetaCommitCertificate,
) -> Result<core_store::CoreMetaCommitCertificate, Status> {
    let prepare_receipts = certificate
        .prepare_receipts
        .iter()
        .map(api_prepare_receipt_to_core)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(core_store::CoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        pending_batch_hash: certificate.pending_batch_hash.clone(),
        prepare_receipts,
        certificate_hash: certificate.certificate_hash.clone(),
    })
}

fn api_persist_receipt_to_core(
    receipt: &crate::anvil_api::CoreMetaCertificatePersistReceipt,
) -> Result<core_store::CoreMetaCertificatePersistReceipt, Status> {
    let mut core = core_store::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash.clone(),
        committed_batch_hash: receipt.committed_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: receipt.signature.clone(),
    };
    core.signed_payload_hash =
        core_store::certificate_persist_receipt_payload_hash(&core).map_err(internal_status)?;
    Ok(core)
}

fn shard_receipt_from_core(receipt: core_store::CoreInternalShardReceipt) -> ShardReceipt {
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
}

#[tonic::async_trait]
impl RootRegisterInternal for AppState {
    async fn read_root(
        &self,
        request: Request<ReadRootRequest>,
    ) -> Result<Response<RootAnchorRead>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        if let Some(generation) = req.exact_generation {
            if generation < req.min_generation {
                return Err(Status::invalid_argument(
                    "exact root generation is below the requested minimum",
                ));
            }
            if req.committed_cache {
                let anchor = self
                    .core_store
                    .read_committed_root_anchor_generation(&req.root_key_hash, generation)
                    .await
                    .map_err(internal_status)?
                    .ok_or_else(|| {
                        Status::not_found("committed root-cache generation not found")
                    })?;
                let root_anchor_record =
                    core_store::encode_root_anchor_record(&anchor).map_err(internal_status)?;
                return Ok(Response::new(RootAnchorRead {
                    root_key_hash: anchor.root_key_hash,
                    generation: anchor.root_generation,
                    root_anchor_hash: format!(
                        "sha256:{}",
                        core_store::sha256_hex(&root_anchor_record)
                    ),
                    root_anchor_record,
                    shard_index: 0,
                    register_cohort_node_ids: Vec::new(),
                    register_cohort_hash: String::new(),
                    placement_epoch: 0,
                }));
            }
            let shard = self
                .core_store
                .read_exact_root_register_shard(&req.root_key_hash, generation)
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("physical root-register shard not found"))?;
            return Ok(Response::new(RootAnchorRead {
                root_key_hash: shard.root_key_hash,
                generation: shard.root_generation,
                root_anchor_record: shard.root_anchor_record,
                root_anchor_hash: shard.root_anchor_hash,
                shard_index: u32::from(shard.shard_index),
                register_cohort_node_ids: shard.register_cohort_nodes,
                register_cohort_hash: shard.register_cohort_hash,
                placement_epoch: shard.placement_epoch,
            }));
        }
        if req.committed_cache {
            return Err(Status::invalid_argument(
                "committed-cache root reads require an exact generation",
            ));
        }
        let read = self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, req.min_generation)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RootAnchorRead {
            root_key_hash: read.root_key_hash,
            generation: read.generation,
            root_anchor_record: read.root_anchor_record,
            root_anchor_hash: read.root_anchor_hash,
            shard_index: 0,
            register_cohort_node_ids: Vec::new(),
            register_cohort_hash: String::new(),
            placement_epoch: 0,
        }))
    }

    async fn prepare_root(
        &self,
        request: Request<PrepareRootRequest>,
    ) -> Result<Response<RootPrepareReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let source_node_id = request
            .get_ref()
            .header
            .as_ref()
            .map(|header| header.source_node_id.clone())
            .ok_or_else(|| Status::unauthenticated("internal request header required"))?;
        let req = request.into_inner();
        let post_generation = req.expected_generation.saturating_add(1);
        let new_root_hash = format!(
            "sha256:{}",
            core_store::sha256_hex(&req.new_root_anchor_record)
        );
        let new_anchor = core_store::decode_root_anchor_record(&req.new_root_anchor_record)
            .map_err(internal_status)?;
        if new_anchor.root_key_hash != req.root_key_hash {
            return Err(Status::failed_precondition("root key hash mismatch"));
        }
        if new_anchor.root_generation != post_generation {
            return Err(Status::failed_precondition("root post generation mismatch"));
        }
        if req.partition_owner_fence != new_anchor.partition_owner_fence {
            return Err(Status::failed_precondition(
                "root partition owner fence mismatch",
            ));
        }
        if source_node_id != new_anchor.publisher_node_id {
            return Err(Status::failed_precondition(
                "root publication source is not the declared owner",
            ));
        }
        if !self
            .core_store
            .incoming_root_publication_is_ready(&req.root_key_hash, req.expected_generation)
            .map_err(internal_status)?
        {
            return Err(Status::unavailable(
                "root-register replica catch-up has been queued",
            ));
        }
        self.core_store
            .validate_root_owner_publication(&source_node_id, &new_anchor)
            .map_err(internal_status)?;
        match self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, 0)
            .await
        {
            Ok(current) => {
                if current.generation != req.expected_generation {
                    return Err(Status::failed_precondition(format!(
                        "root expected generation mismatch: root_key_hash={} expected={} current={}",
                        req.root_key_hash, req.expected_generation, current.generation
                    )));
                }
                if !req.expected_root_hash.is_empty()
                    && current.root_anchor_hash != req.expected_root_hash
                {
                    return Err(Status::failed_precondition(format!(
                        "root expected hash mismatch: root_key_hash={} generation={} expected={} current={}",
                        req.root_key_hash,
                        req.expected_generation,
                        req.expected_root_hash,
                        current.root_anchor_hash
                    )));
                }
                if new_anchor.previous_root_hash != current.root_anchor_hash {
                    return Err(Status::failed_precondition("root previous hash mismatch"));
                }
            }
            Err(_) => {
                if req.expected_generation != 0 || !req.expected_root_hash.is_empty() {
                    return Err(Status::failed_precondition(
                        "root expected generation missing",
                    ));
                }
            }
        }
        let shard_index = u16::try_from(req.shard_index)
            .map_err(|_| Status::invalid_argument("root-register shard index overflow"))?;
        let receipt = self
            .core_store
            .persist_root_register_prepare(
                &self.config.node_id,
                &new_anchor,
                &req.new_root_anchor_record,
                req.expected_generation,
                &req.register_cohort_node_ids,
                &req.register_cohort_hash,
                shard_index,
                req.placement_epoch,
            )
            .await
            .map_err(internal_status)?;
        debug_assert_eq!(receipt.new_root_hash, new_root_hash);
        Ok(Response::new(receipt))
    }

    async fn compare_and_swap_root(
        &self,
        request: Request<CompareAndSwapRootRequest>,
    ) -> Result<Response<RootAnchorWrite>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let source_node_id = request
            .get_ref()
            .header
            .as_ref()
            .map(|header| header.source_node_id.clone())
            .ok_or_else(|| Status::unauthenticated("internal request header required"))?;
        let req = request.into_inner();
        let new_anchor = core_store::decode_root_anchor_record(&req.new_root_anchor_record)
            .map_err(internal_status)?;
        if req.partition_owner_fence != new_anchor.partition_owner_fence {
            return Err(Status::failed_precondition(
                "root partition owner fence mismatch",
            ));
        }
        if source_node_id != new_anchor.publisher_node_id {
            return Err(Status::failed_precondition(
                "root publication source is not the declared owner",
            ));
        }
        if !self
            .core_store
            .incoming_root_publication_is_ready(&req.root_key_hash, req.expected_generation)
            .map_err(internal_status)?
        {
            // Unlike prepare, a CAS request carries evidence for the post
            // generation. Once Q2 commits elsewhere, supervised recovery can
            // therefore advance through the generation this replica rejected,
            // rather than remaining permanently one publication behind.
            self.core_store
                .request_coremeta_root_repair(&req.root_key_hash, new_anchor.root_generation);
            return Err(Status::unavailable(
                "root-register replica catch-up has been queued",
            ));
        }
        self.core_store
            .validate_root_owner_publication(&source_node_id, &new_anchor)
            .map_err(internal_status)?;
        let certificate = req
            .core_meta_commit_certificate
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("core_meta_commit_certificate is required"))?;
        if !req.core_meta_commit_certificate_hash.is_empty()
            && req.core_meta_commit_certificate_hash != certificate.certificate_hash
        {
            return Err(Status::failed_precondition(
                "core_meta_commit_certificate_hash mismatch",
            ));
        }
        let core_certificate = api_commit_certificate_to_core(certificate)?;
        let persist_receipts = req
            .certificate_persist_receipts
            .iter()
            .map(api_persist_receipt_to_core)
            .collect::<Result<Vec<_>, _>>()?;
        core_store::validate_commit_evidence_with_verifier(
            &self
                .core_store
                .default_coremeta_quorum_profile()
                .map_err(internal_status)?,
            &core_certificate,
            &persist_receipts,
            |node_id, signed_payload_hash, signature| {
                self.core_store.verify_internal_core_receipt_signature(
                    node_id,
                    signed_payload_hash,
                    signature,
                )
            },
        )
        .map_err(internal_status)?;
        let committed_batch_hash = persist_receipts
            .first()
            .map(|receipt| receipt.committed_batch_hash.clone())
            .ok_or_else(|| Status::failed_precondition("certificate persist receipts required"))?;
        let persist_receipt_hashes = persist_receipts
            .iter()
            .map(core_store::certificate_persist_receipt_payload_hash)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_status)?;
        let persist_receipt_bytes = req
            .certificate_persist_receipts
            .iter()
            .map(core_store::encode_deterministic_proto)
            .collect::<Vec<_>>();
        let evidence_created_at = self
            .core_store
            .root_publication_intent_created_at(&certificate.transaction_id)
            .unwrap_or_else(|_| u64::try_from(now_unix_nanos()).unwrap_or(u64::MAX));
        validate_root_prepare_receipts(
            self,
            &req.root_key_hash,
            req.expected_generation,
            &req.new_root_anchor_record,
            &req.register_cohort_node_ids,
            &req.register_cohort_hash,
            req.placement_epoch,
            &req.prepare_receipts,
        )?;
        self.core_store
            .ensure_local_committed_root_register_shard(
                &new_anchor,
                &req.new_root_anchor_record,
                req.expected_generation,
                &req.register_cohort_node_ids,
                &req.register_cohort_hash,
                req.placement_epoch,
            )
            .await
            .map_err(internal_status)?;
        let participant_anchor_records = if req.participant_commit_evidence.is_empty() {
            self.core_store
                .persist_coremeta_commit_evidence_at(
                    &certificate.root_key_hash,
                    certificate.post_root_generation,
                    &certificate.transaction_id,
                    &certificate.certificate_hash,
                    &committed_batch_hash,
                    core_store::encode_deterministic_proto(certificate),
                    persist_receipt_hashes,
                    persist_receipt_bytes,
                    evidence_created_at,
                )
                .map_err(internal_status)?;
            Vec::new()
        } else {
            validate_coordinator_publication_evidence(certificate, &req)?;
            self.core_store
                .install_root_publication_commit_evidence(
                    &source_node_id,
                    &certificate.transaction_id,
                    &req.participant_commit_evidence,
                )
                .await
                .map_err(internal_status)?
        };
        let read = self
            .core_store
            .compare_and_swap_internal_root_anchor_from_register_quorum(
                &req.root_key_hash,
                req.expected_generation,
                &req.expected_root_hash,
                &req.new_root_anchor_record,
                &participant_anchor_records,
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RootAnchorWrite {
            root_key_hash: read.root_key_hash,
            generation: read.generation,
            root_anchor_hash: read.root_anchor_hash,
            prepare_receipts: Vec::new(),
        }))
    }

    async fn vote_failover(
        &self,
        request: Request<VoteFailoverRequest>,
    ) -> Result<Response<FailoverVoteReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let receipt = self
            .core_store
            .vote_root_owner_failover(&req)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| {
                Status::failed_precondition(
                    "root owner failure evidence has not reached the voting threshold",
                )
            })?;
        Ok(Response::new(receipt))
    }

    async fn exchange_root_inventory(
        &self,
        request: Request<ExchangeRootInventoryRequest>,
    ) -> Result<Response<RootInventory>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let read = self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, req.from_generation)
            .await
            .map_err(internal_status)?;
        let inventory_parts = if read.generation <= req.to_generation || req.to_generation == 0 {
            vec![format!(
                "{}:{}:{}",
                read.root_key_hash, read.generation, read.root_anchor_hash
            )]
        } else {
            Vec::new()
        };
        Ok(Response::new(RootInventory {
            root_key_hash: req.root_key_hash.clone(),
            from_generation: req.from_generation,
            to_generation: req.to_generation,
            inventory_hash: hash_string_list("anvil.root.inventory.v1", &inventory_parts),
        }))
    }

    async fn exchange_root_directory(
        &self,
        request: Request<ExchangeRootDirectoryRequest>,
    ) -> Result<Response<RootDirectoryPage>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let max_entries = bounded_root_directory_page(req.max_entries, req.max_bytes)?;
        let page = self
            .core_store
            .coremeta_root_directory_page(&req.after_root_key_hash, max_entries, req.max_bytes)
            .map_err(internal_status)?;
        Ok(Response::new(page))
    }
}

#[tonic::async_trait]
impl AntiEntropyInternal for AppState {
    async fn exchange_inventory(
        &self,
        request: Request<ExchangeInventoryRequest>,
    ) -> Result<Response<InventoryDiff>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        if req.namespace == "shard" {
            let state = self
                .core_store
                .shard_inventory_state(&req.partition)
                .await
                .map_err(internal_status)?;
            return Ok(Response::new(core_store::shard_inventory_response(
                &req.partition,
                state,
            )));
        }
        let local_hash = match req.namespace.as_str() {
            "coremeta" => {
                complete_coremeta_history_inventory_hash(&self.core_store, &req.partition)?
            }
            "root" => {
                let read = self
                    .core_store
                    .read_internal_root_anchor_by_hash(&req.partition, 0)
                    .await
                    .map_err(internal_status)?;
                hash_string_list(
                    "anvil.antientropy.root.inventory.v1",
                    &[format!(
                        "{}:{}:{}",
                        read.root_key_hash, read.generation, read.root_anchor_hash
                    )],
                )
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unsupported anti-entropy namespace {other}"
                )));
            }
        };

        let divergent_ids = if req.inventory_hash == local_hash {
            Vec::new()
        } else {
            vec![req.partition]
        };
        Ok(Response::new(InventoryDiff {
            missing_ids: Vec::new(),
            divergent_ids,
            inventory_hash: local_hash,
        }))
    }

    async fn publish_repair_finding(
        &self,
        request: Request<PublishRepairFindingRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_len_at_most(
            req.finding_json.len(),
            MAX_REPAIR_FINDING_JSON_BYTES,
            "finding_json",
        )?;
        let details: serde_json::Value =
            serde_json::from_str(&req.finding_json).map_err(|error| {
                Status::invalid_argument(format!("finding_json must be valid JSON: {error}"))
            })?;
        let finding_id = details
            .get("finding_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let scope_kind = details
            .get("scope_kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("anti_entropy")
            .to_string();
        let scope_id = details
            .get("scope_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("cluster")
            .to_string();
        let diagnostic = diagnostic_store::write_diagnostic_object(
            &self.storage,
            diagnostic_store::DiagnosticWrite {
                diagnostic_id: finding_id.clone(),
                scope_kind,
                scope_id,
                source: "anvil.internal.anti_entropy".to_string(),
                severity: diagnostic_store::DiagnosticSeverity::Warning,
                code: "RepairFindingPublished".to_string(),
                message: "repair finding published by internal anti-entropy service".to_string(),
                object_ref: None,
                details,
                created_at_nanos: now_unix_nanos(),
            },
            self.config.anvil_secret_encryption_key.as_bytes(),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(WriteResponse {
            request_id: request_id_from_header(req.header.as_ref()),
            mutation_id: finding_id,
            state: WriteState::Committed as i32,
            root_generation: None,
            transaction_manifest_ref: diagnostic.diagnostic_hash,
            idempotency_outcome: "accepted".to_string(),
            retry_after_hint: None,
            finalisation_error: None,
            saga: None,
        }))
    }

    async fn claim_repair(
        &self,
        request: Request<ClaimRepairRequest>,
    ) -> Result<Response<RepairClaim>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        validate_repair_finding_id(&req.finding_id)?;
        let owner = repair_claim_owner(req.header.as_ref())?;
        let claimant_node_id = owner.principal_id.clone();
        let lease = self
            .persistence
            .acquire_named_task_lease(task_lease::TaskLeaseAcquire {
                task_id: format!("repair:{}", req.finding_id),
                task_kind: "repair".to_string(),
                partition_family: "anti_entropy".to_string(),
                partition_id: core_store::sha256_hex(req.finding_id.as_bytes()),
                owner,
                source_cursor: 0,
                now_nanos: now_unix_nanos(),
                ttl_nanos: 60_000_000_000,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RepairClaim {
            finding_id: req.finding_id,
            claimant_node_id,
            fence_token: lease.fence_token,
        }))
    }
}

fn repair_claim_owner(
    header: Option<&InternalRequestHeader>,
) -> Result<task_lease::TaskLeaseOwner, Status> {
    let claimant_node_id = header
        .map(|header| header.source_node_id.clone())
        .filter(|node_id| !node_id.trim().is_empty())
        .ok_or_else(|| Status::invalid_argument("source_node_id is required"))?;
    Ok(task_lease::TaskLeaseOwner::node(claimant_node_id))
}

fn now_unix_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}
