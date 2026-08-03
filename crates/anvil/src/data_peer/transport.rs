//! Cached mandatory-mTLS client for typed peer storage operations.

use super::*;
use anvil_store::{
    AuthzRealmCursor, AuthzRealmKeyPage, AuthzRealmTransferManifest, AuthzScope,
    LogicalRecordCandidate, LogicalRecordCursor, LogicalRecordExport, LogicalRecordExportPage,
    LogicalRecordId, ObjectRecordCursor, ObjectRecordExport, ObjectRecordExportPage,
    PayloadArtifactCursor, PayloadArtifactSnapshot, PayloadArtifactSnapshotPage,
};

#[derive(Clone)]
#[allow(
    dead_code,
    reason = "the distributed coordinators consume this transport in the immediately following integration slice"
)]
pub(crate) struct DataPeerTransport {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    tls: PeerTlsConnector,
    channels: Arc<Mutex<BTreeMap<u64, (String, Channel)>>>,
    handoff: Option<wire::HandoffScope>,
}

#[allow(
    dead_code,
    reason = "the distributed coordinators consume this transport in the immediately following integration slice"
)]
impl DataPeerTransport {
    pub(crate) fn new(
        cluster_id: ClusterId,
        source_node_id: NodeId,
        tls: PeerTlsConnector,
    ) -> Result<Self, anyhow::Error> {
        anyhow::ensure!(cluster_id.0 != [0; 16], "cluster id must not be all zero");
        anyhow::ensure!(source_node_id.0 != 0, "source node id must not be zero");
        Ok(Self {
            cluster_id,
            source_node_id,
            tls,
            channels: Arc::new(Mutex::new(BTreeMap::new())),
            handoff: None,
        })
    }

    pub(crate) fn for_handoff(&self, joining_node_id: NodeId, started_log_index: u64) -> Self {
        let mut scoped = self.clone();
        scoped.handoff = Some(wire::HandoffScope {
            joining_node_id: joining_node_id.0,
            started_log_index,
        });
        scoped
    }

    fn handoff(&self) -> Result<wire::HandoffScope, Status> {
        self.handoff
            .clone()
            .ok_or_else(|| Status::failed_precondition("data transport is not handoff-scoped"))
    }

    pub(super) fn context(&self) -> wire::PeerContext {
        wire::PeerContext {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            cluster_id: self.cluster_id.into_bytes().to_vec(),
            source_node_id: self.source_node_id.0,
        }
    }

    pub(crate) fn peer_identity(&self) -> (ClusterId, NodeId) {
        (self.cluster_id, self.source_node_id)
    }

    pub(crate) fn channel(&self, target: NodeId, address: &str) -> Result<Channel, Status> {
        if target.0 == 0 {
            return Err(Status::invalid_argument("target node id must not be zero"));
        }
        if address.is_empty() {
            return Err(Status::invalid_argument("target peer address is empty"));
        }
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("data-peer channel lock is poisoned"))?;
        if let Some((cached_address, channel)) = channels.get(&target.0)
            && cached_address == address
        {
            return Ok(channel.clone());
        }
        let connector = DataPeerChannelConnector {
            tls: self.tls.clone(),
            target,
            address: address.to_owned(),
        };
        let channel = Endpoint::from_static("http://anvil-peer.invalid")
            .connect_with_connector_lazy(connector);
        channels.insert(target.0, (address.to_owned(), channel.clone()));
        Ok(channel)
    }

    pub(super) fn client(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<wire::data_peer_client::DataPeerClient<Channel>, Status> {
        Ok(
            wire::data_peer_client::DataPeerClient::new(self.channel(target, address)?)
                .max_decoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_DATA_PEER_MESSAGE_BYTES),
        )
    }

    pub(crate) async fn apply_object_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &ObjectMutation,
    ) -> Result<ReplicaObjectMutationApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_object_mutation(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaObjectMutationApplied {
            version: anvil_store::VersionId(response.version),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn apply_authz_realm_mutation(
        &self,
        target: NodeId,
        address: &str,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_authz_realm_mutation(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReplicaAuthzRealmMutationApplied {
            revision: anvil_store::AuthzRevision(response.revision),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn apply_reference_deltas(
        &self,
        target: NodeId,
        address: &str,
        mutation: &ReferenceDeltaBatch,
    ) -> Result<ReferenceDeltaApplied, Status> {
        let response = self
            .client(target, address)?
            .apply_reference_deltas(wire::TypedMutationRequest {
                peer: Some(self.context()),
                mutation_json: encode_typed(mutation)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReferenceDeltaApplied {
            through: response.through,
            replayed: response.replayed,
        })
    }

    pub(crate) async fn reference_delta_status(
        &self,
        target: NodeId,
        address: &str,
        source: SourceId,
    ) -> Result<u64, Status> {
        let response = self
            .client(target, address)?
            .get_reference_delta_status(wire::ReferenceDeltaStatusRequest {
                peer: Some(self.context()),
                source_id_json: encode_typed(&source)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.through)
    }

    pub(crate) async fn source_journal_status(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<WatchJournalStatus, Status> {
        let response = self
            .client(target, address)?
            .get_source_journal_status(wire::SourceJournalStatusRequest {
                peer: Some(self.context()),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(WatchJournalStatus {
            source_id: decode_typed(&response.source_id_json)?,
            tail: response.tail,
            retention_floor: response.retention_floor,
            retained_entries: response.retained_entries,
            retained_bytes: response.retained_bytes,
        })
    }

    pub(crate) async fn read_source_journal(
        &self,
        target: NodeId,
        address: &str,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalChange>, Status> {
        let limit = u32::try_from(limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS))
            .expect("source journal limit fits u32");
        let response = self
            .client(target, address)?
            .read_source_journal(wire::SourceJournalReadRequest {
                peer: Some(self.context()),
                after_offset,
                limit,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        response
            .changes_json
            .iter()
            .map(|encoded| decode_typed(encoded))
            .collect()
    }

    pub(crate) async fn small_content_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .small_content_exists(wire::ContentRequest {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.exists)
    }

    pub(crate) async fn put_small_content(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), Status> {
        if bytes.len() > anvil_store::SMALL_BLOB_MAX_BYTES
            || bytes.len() as u64 != reference.length
            || blake3::hash(bytes).as_bytes() != &reference.hash
        {
            return Err(Status::invalid_argument(
                "small content does not match its immutable identity",
            ));
        }
        let mut frames = Vec::new();
        if bytes.is_empty() {
            frames.push(wire::SmallContentPutFrame {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
                offset: 0,
                content: Vec::new(),
                end: true,
            });
        } else {
            for (index, content) in bytes.chunks(DATA_PEER_FRAME_BYTES).enumerate() {
                let offset = index * DATA_PEER_FRAME_BYTES;
                frames.push(wire::SmallContentPutFrame {
                    peer: Some(self.context()),
                    blob: Some(wire_blob(reference)),
                    offset: offset as u64,
                    content: content.to_vec(),
                    end: offset + content.len() == bytes.len(),
                });
            }
        }
        let response = self
            .client(target, address)?
            .put_small_content(tokio_stream::iter(frames))
            .await?
            .into_inner();
        require_response_schema(response.schema_version)
    }

    pub(crate) async fn get_small_content(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<Vec<u8>, Status> {
        if reference.length > anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            return Err(Status::invalid_argument(
                "content identity is not a small blob",
            ));
        }
        let mut stream = self
            .client(target, address)?
            .get_small_content(wire::ContentRequest {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
            })
            .await?
            .into_inner();
        let mut bytes = Vec::with_capacity(reference.length as usize);
        while let Some(frame) = stream.message().await? {
            require_response_schema(frame.schema_version)?;
            if frame.offset != bytes.len() as u64 || frame.content.len() > DATA_PEER_FRAME_BYTES {
                return Err(Status::data_loss("small-content stream is not contiguous"));
            }
            bytes.extend_from_slice(&frame.content);
            if bytes.len() > anvil_store::SMALL_BLOB_MAX_BYTES {
                return Err(Status::resource_exhausted(
                    "small-content response is too large",
                ));
            }
            if frame.end {
                if bytes.len() as u64 != reference.length
                    || blake3::hash(&bytes).as_bytes() != &reference.hash
                {
                    return Err(Status::data_loss(
                        "small-content response failed identity verification",
                    ));
                }
                return Ok(bytes);
            }
        }
        Err(Status::data_loss(
            "small-content stream ended without an end frame",
        ))
    }

    pub(crate) async fn put_complete_source(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        mut reader: BlobReader,
    ) -> Result<CompleteCopySealOutcome, Status> {
        if reference.length <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            return Err(Status::invalid_argument(
                "complete-source operation requires a large blob",
            ));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let context = self.context();
        let blob = wire_blob(reference);
        let producer = tokio::spawn(async move {
            let mut offset = 0_u64;
            let mut current = vec![0_u8; DATA_PEER_FRAME_BYTES];
            let mut next = vec![0_u8; DATA_PEER_FRAME_BYTES];
            let mut current_length = reader
                .read(&mut current)
                .await
                .map_err(|error| Status::data_loss(error.to_string()))?;
            loop {
                let next_length = if current_length == 0 {
                    0
                } else {
                    reader
                        .read(&mut next)
                        .await
                        .map_err(|error| Status::data_loss(error.to_string()))?
                };
                let end = current_length == 0 || next_length == 0;
                sender
                    .send(wire::CompleteSourcePutFrame {
                        peer: Some(context.clone()),
                        blob: Some(blob.clone()),
                        offset,
                        content: current[..current_length].to_vec(),
                        end,
                    })
                    .await
                    .map_err(|_| Status::cancelled("complete-source peer stream closed"))?;
                if end {
                    return Ok::<(), Status>(());
                }
                offset += current_length as u64;
                std::mem::swap(&mut current, &mut next);
                current_length = next_length;
            }
        });
        let response = self
            .client(target, address)?
            .put_complete_source(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await;
        producer.await.map_err(|error| {
            Status::internal(format!("join complete-source producer: {error}"))
        })??;
        let response = response?.into_inner();
        require_response_schema(response.schema_version)?;
        Ok(if response.already_present {
            CompleteCopySealOutcome::AlreadyPresent
        } else {
            CompleteCopySealOutcome::Created
        })
    }

    pub(crate) async fn get_complete_source(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<Streaming<wire::ContentFrame>, Status> {
        if reference.length <= anvil_store::SMALL_BLOB_MAX_BYTES as u64 {
            return Err(Status::invalid_argument(
                "complete-source operation requires a large blob",
            ));
        }
        self.client(target, address)?
            .get_complete_source(wire::ContentRequest {
                peer: Some(self.context()),
                blob: Some(wire_blob(reference)),
            })
            .await
            .map(Response::into_inner)
    }

    pub(crate) async fn shard_exists(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .shard_exists(wire_shard(self.context(), identity))
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.exists)
    }

    pub(crate) async fn get_shard(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<Streaming<wire::ContentFrame>, Status> {
        self.client(target, address)?
            .get_shard(wire_shard(self.context(), identity))
            .await
            .map(Response::into_inner)
    }

    pub(crate) async fn put_shard<R>(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        mut reader: R,
    ) -> Result<ShardSealOutcome, Status>
    where
        R: Read + Send + 'static,
    {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let shard = wire_shard(self.context(), identity);
        let producer = tokio::task::spawn_blocking(move || {
            let mut offset = 0_u64;
            let mut current = vec![0_u8; DATA_PEER_FRAME_BYTES];
            let mut next = vec![0_u8; DATA_PEER_FRAME_BYTES];
            let mut current_length = reader.read(&mut current)?;
            loop {
                let next_length = if current_length == 0 {
                    0
                } else {
                    reader.read(&mut next)?
                };
                let end = current_length == 0 || next_length == 0;
                sender
                    .blocking_send(wire::ShardPutFrame {
                        shard: Some(shard.clone()),
                        offset,
                        content: current[..current_length].to_vec(),
                        end,
                    })
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "shard peer stream closed")
                    })?;
                if end {
                    return Ok::<(), io::Error>(());
                }
                offset += current_length as u64;
                std::mem::swap(&mut current, &mut next);
                current_length = next_length;
            }
        });
        let response = self
            .client(target, address)?
            .put_shard(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await;
        producer
            .await
            .map_err(|error| Status::internal(format!("join shard producer: {error}")))?
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let response = response?.into_inner();
        require_response_schema(response.schema_version)?;
        Ok(if response.already_present {
            ShardSealOutcome::AlreadyPresent
        } else {
            ShardSealOutcome::Created
        })
    }

    pub(crate) async fn export_object_records(
        &self,
        target: NodeId,
        address: &str,
        cursor: Option<&ObjectRecordCursor>,
    ) -> Result<ObjectRecordExportPage, Status> {
        let cursor = cursor.map(encode_typed).transpose()?.unwrap_or_default();
        let response = self
            .client(target, address)?
            .export_object_records(handoff_page_request(
                self.context(),
                self.handoff()?,
                cursor,
            ))
            .await?
            .into_inner();
        decode_handoff_page(response)
    }

    pub(crate) async fn read_handoff_object_path_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let response = self
            .client(target, address)?
            .read_handoff_object_path_snapshot(wire::HandoffObjectPathSnapshotRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
                tenant_id,
                bucket_id,
                exact_path: exact_path.to_owned(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        decode_typed(&response.snapshot_json)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn repair_handoff_object_path_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        expected: Option<&ObjectPathSnapshot>,
        selected: Option<&ObjectPathSnapshot>,
    ) -> Result<ObjectSnapshotApplied, Status> {
        let response = self
            .client(target, address)?
            .repair_handoff_object_path_snapshot(wire::RepairHandoffObjectPathSnapshotRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
                tenant_id,
                bucket_id,
                exact_path: exact_path.to_owned(),
                expected_snapshot_json: encode_typed(&expected)?,
                selected_snapshot_json: encode_typed(&selected)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ObjectSnapshotApplied {
            retained: response.present,
            version: response
                .present
                .then_some(anvil_store::VersionId(response.version)),
            replayed: response.replayed,
        })
    }

    pub(crate) async fn handoff_source_journal_status(
        &self,
        target: NodeId,
        address: &str,
    ) -> Result<WatchJournalStatus, Status> {
        let response = self
            .client(target, address)?
            .get_handoff_source_journal_status(wire::HandoffSourceJournalStatusRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(WatchJournalStatus {
            source_id: decode_typed(&response.source_id_json)?,
            tail: response.tail,
            retention_floor: response.retention_floor,
            retained_entries: response.retained_entries,
            retained_bytes: response.retained_bytes,
        })
    }

    pub(crate) async fn read_handoff_source_journal(
        &self,
        target: NodeId,
        address: &str,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LocalChange>, Status> {
        let limit = u32::try_from(limit.min(MAX_LOCAL_INVALIDATION_SCAN_RECORDS))
            .expect("source journal limit fits u32");
        let response = self
            .client(target, address)?
            .read_handoff_source_journal(wire::HandoffSourceJournalReadRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
                after_offset,
                limit,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        response
            .changes_json
            .iter()
            .map(|encoded| decode_typed(encoded))
            .collect()
    }

    pub(crate) async fn handoff_reference_cursor(
        &self,
        target: NodeId,
        address: &str,
        source: SourceId,
    ) -> Result<u64, Status> {
        let response = self
            .client(target, address)?
            .get_handoff_reference_cursor(wire::HandoffReferenceCursorRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
                source_id_json: encode_typed(&source)?,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.through)
    }

    pub(crate) async fn advance_handoff_reference_cursor(
        &self,
        target: NodeId,
        address: &str,
        source: SourceId,
        through: u64,
    ) -> Result<ReferenceDeltaApplied, Status> {
        let response = self
            .client(target, address)?
            .advance_handoff_reference_cursor(wire::HandoffReferenceCursorAdvanceRequest {
                peer: Some(self.context()),
                handoff: Some(self.handoff()?),
                source_id_json: encode_typed(&source)?,
                through,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(ReferenceDeltaApplied {
            through: response.through,
            replayed: response.replayed,
        })
    }

    pub(crate) async fn install_object_record(
        &self,
        target: NodeId,
        address: &str,
        record: &ObjectRecordExport,
    ) -> Result<bool, Status> {
        self.install_handoff_record(
            target,
            address,
            encode_typed(record)?,
            HandoffInstallKind::Object,
        )
        .await
    }

    pub(crate) async fn export_logical_records(
        &self,
        target: NodeId,
        address: &str,
        cursor: Option<&LogicalRecordCursor>,
    ) -> Result<LogicalRecordExportPage, Status> {
        let cursor = cursor.map(encode_typed).transpose()?.unwrap_or_default();
        let response = self
            .client(target, address)?
            .export_logical_records(handoff_page_request(
                self.context(),
                self.handoff()?,
                cursor,
            ))
            .await?
            .into_inner();
        decode_handoff_page(response)
    }

    pub(crate) async fn install_logical_record(
        &self,
        target: NodeId,
        address: &str,
        record: &LogicalRecordExport,
    ) -> Result<bool, Status> {
        self.install_handoff_record(
            target,
            address,
            encode_typed(record)?,
            HandoffInstallKind::Logical,
        )
        .await
    }

    pub(crate) async fn read_logical_record(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
    ) -> Result<Option<LogicalRecordCandidate>, Status> {
        let response = self
            .client(target, address)?
            .read_logical_record(wire::LogicalRecordRequest {
                peer: Some(self.context()),
                id_json: encode_typed(id)?,
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        match (response.present, response.candidate_json.is_empty()) {
            (true, false) => decode_typed(&response.candidate_json).map(Some),
            (false, true) => Ok(None),
            _ => Err(Status::data_loss(
                "logical peer presence and candidate disagree",
            )),
        }
    }

    pub(crate) async fn repair_logical_record(
        &self,
        target: NodeId,
        address: &str,
        id: &LogicalRecordId,
        candidate: Option<&LogicalRecordCandidate>,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .repair_logical_record(wire::RepairLogicalRecordRequest {
                peer: Some(self.context()),
                id_json: encode_typed(id)?,
                present: candidate.is_some(),
                candidate_json: candidate.map(encode_typed).transpose()?.unwrap_or_default(),
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.replayed)
    }

    pub(crate) async fn export_authz_realm_keys(
        &self,
        target: NodeId,
        address: &str,
        cursor: Option<&AuthzRealmCursor>,
    ) -> Result<AuthzRealmKeyPage, Status> {
        let cursor = cursor.map(encode_typed).transpose()?.unwrap_or_default();
        let response = self
            .client(target, address)?
            .export_authz_realm_keys(handoff_page_request(
                self.context(),
                self.handoff()?,
                cursor,
            ))
            .await?
            .into_inner();
        decode_handoff_page(response)
    }

    pub(crate) async fn authz_realm_manifest(
        &self,
        target: NodeId,
        address: &str,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmTransferManifest>, Status> {
        let response = self
            .client(target, address)?
            .read_authz_realm_manifest(wire::AuthzRealmRequest {
                peer: Some(self.context()),
                scope_json: encode_typed(scope)?,
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        match response.present {
            true if response.manifest_json.is_empty() => Err(Status::data_loss(
                "present authorization realm omitted its manifest",
            )),
            true => decode_typed(&response.manifest_json).map(Some),
            false if response.manifest_json.is_empty() => Ok(None),
            false => Err(Status::data_loss(
                "absent authorization realm returned a manifest",
            )),
        }
    }

    pub(crate) async fn repair_authz_realm_absence(
        &self,
        target: NodeId,
        address: &str,
        scope: &AuthzScope,
    ) -> Result<bool, Status> {
        let response = self
            .client(target, address)?
            .repair_authz_realm_absence(wire::AuthzRealmRequest {
                peer: Some(self.context()),
                scope_json: encode_typed(scope)?,
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.replayed)
    }

    pub(crate) async fn copy_authz_realm(
        &self,
        source: NodeId,
        source_address: &str,
        target: NodeId,
        target_address: &str,
        scope: &AuthzScope,
        expected: &AuthzRealmTransferManifest,
    ) -> Result<bool, Status> {
        let mut source_stream = self
            .client(source, source_address)?
            .get_authz_realm(wire::AuthzRealmRequest {
                peer: Some(self.context()),
                scope_json: encode_typed(scope)?,
                handoff: Some(self.handoff()?),
            })
            .await?
            .into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let context = self.context();
        let handoff = self.handoff()?;
        let expected = expected.clone();
        let producer = tokio::spawn(async move {
            let mut offset = 0_u64;
            let mut first = true;
            while let Some(frame) = source_stream.message().await? {
                require_response_schema(frame.schema_version)?;
                if first {
                    let observed: AuthzRealmTransferManifest = decode_typed(&frame.manifest_json)?;
                    if observed != expected
                        || frame.offset != 0
                        || !frame.content.is_empty()
                        || frame.end
                    {
                        return Err(Status::data_loss(
                            "authorization source did not stream the selected quorum candidate",
                        ));
                    }
                    first = false;
                } else if !frame.manifest_json.is_empty() || frame.offset != offset {
                    return Err(Status::data_loss(
                        "authorization source stream is not contiguous",
                    ));
                }
                offset = offset
                    .checked_add(frame.content.len() as u64)
                    .ok_or_else(|| Status::resource_exhausted("authorization stream overflow"))?;
                sender
                    .send(wire::AuthzRealmPutFrame {
                        peer: Some(context.clone()),
                        offset: frame.offset,
                        content: frame.content,
                        end: frame.end,
                        manifest_json: frame.manifest_json,
                        handoff: Some(handoff.clone()),
                    })
                    .await
                    .map_err(|_| Status::cancelled("authorization destination stream closed"))?;
                if frame.end {
                    return Ok::<(), Status>(());
                }
            }
            Err(Status::data_loss(
                "authorization source stream ended without a final frame",
            ))
        });
        let response = self
            .client(target, target_address)?
            .put_authz_realm(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await;
        producer.await.map_err(|error| {
            Status::internal(format!("realm forwarding task failed: {error}"))
        })??;
        let response = response?.into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.replayed)
    }

    pub(crate) async fn export_payload_artifacts(
        &self,
        target: NodeId,
        address: &str,
        cursor: Option<&PayloadArtifactCursor>,
    ) -> Result<PayloadArtifactSnapshotPage, Status> {
        let cursor = cursor.map(encode_typed).transpose()?.unwrap_or_default();
        let response = self
            .client(target, address)?
            .export_payload_artifacts(handoff_page_request(
                self.context(),
                self.handoff()?,
                cursor,
            ))
            .await?
            .into_inner();
        decode_handoff_page(response)
    }

    pub(crate) async fn install_payload_lifecycle(
        &self,
        target: NodeId,
        address: &str,
        artifact: &PayloadArtifactSnapshot,
    ) -> Result<bool, Status> {
        self.install_handoff_record(
            target,
            address,
            encode_typed(artifact)?,
            HandoffInstallKind::PayloadLifecycle,
        )
        .await
    }

    pub(crate) async fn copy_shard(
        &self,
        source: NodeId,
        source_address: &str,
        target: NodeId,
        target_address: &str,
        identity: &ShardIdentity,
    ) -> Result<ShardSealOutcome, Status> {
        let mut source_stream = self
            .client(source, source_address)?
            .get_shard(wire_shard(self.context(), identity))
            .await?
            .into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let request = wire_shard(self.context(), identity);
        let producer = tokio::spawn(async move {
            let mut offset = 0_u64;
            while let Some(frame) = source_stream.message().await? {
                require_response_schema(frame.schema_version)?;
                if frame.offset != offset || frame.content.len() > DATA_PEER_FRAME_BYTES {
                    return Err(Status::data_loss("source shard stream is not contiguous"));
                }
                offset = offset
                    .checked_add(frame.content.len() as u64)
                    .ok_or_else(|| Status::resource_exhausted("shard stream overflow"))?;
                sender
                    .send(wire::ShardPutFrame {
                        shard: Some(request.clone()),
                        offset: frame.offset,
                        content: frame.content,
                        end: frame.end,
                    })
                    .await
                    .map_err(|_| Status::cancelled("shard destination stream closed"))?;
                if frame.end {
                    return Ok::<(), Status>(());
                }
            }
            Err(Status::data_loss(
                "source shard stream ended without a final frame",
            ))
        });
        let response = self
            .client(target, target_address)?
            .put_shard(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .await;
        producer.await.map_err(|error| {
            Status::internal(format!("shard forwarding task failed: {error}"))
        })??;
        let response = response?.into_inner();
        require_response_schema(response.schema_version)?;
        Ok(if response.already_present {
            ShardSealOutcome::AlreadyPresent
        } else {
            ShardSealOutcome::Created
        })
    }

    async fn install_handoff_record(
        &self,
        target: NodeId,
        address: &str,
        record_json: Vec<u8>,
        kind: HandoffInstallKind,
    ) -> Result<bool, Status> {
        let mut client = self.client(target, address)?;
        let request = wire::HandoffRecordRequest {
            peer: Some(self.context()),
            record_json,
            handoff: Some(self.handoff()?),
        };
        let response = match kind {
            HandoffInstallKind::Object => client.install_object_record(request).await?,
            HandoffInstallKind::Logical => client.install_logical_record(request).await?,
            HandoffInstallKind::PayloadLifecycle => {
                client.install_payload_lifecycle(request).await?
            }
        }
        .into_inner();
        require_response_schema(response.schema_version)?;
        Ok(response.replayed)
    }
}

#[derive(Clone, Copy)]
enum HandoffInstallKind {
    Object,
    Logical,
    PayloadLifecycle,
}

fn handoff_page_request(
    peer: wire::PeerContext,
    handoff: wire::HandoffScope,
    cursor_json: Vec<u8>,
) -> wire::HandoffPageRequest {
    wire::HandoffPageRequest {
        peer: Some(peer),
        cursor_json,
        max_records: 1_000,
        max_bytes: 4 * 1024 * 1024,
        handoff: Some(handoff),
    }
}

fn decode_handoff_page<T: serde::de::DeserializeOwned>(
    response: wire::HandoffPage,
) -> Result<T, Status> {
    require_response_schema(response.schema_version)?;
    require_typed_bound(&response.page_json)?;
    decode_typed(&response.page_json)
}

#[derive(Clone)]
struct DataPeerChannelConnector {
    tls: PeerTlsConnector,
    target: NodeId,
    address: String,
}

impl Service<Uri> for DataPeerChannelConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;
    type Error = PeerTlsError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        let target = self.target;
        let address = self.address.clone();
        Box::pin(async move {
            tls.connect(target, &address)
                .await
                .map(|peer| TokioIo::new(peer.stream))
        })
    }
}
