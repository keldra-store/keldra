use super::*;

pub(super) fn require_object_snapshot_bound(encoded: &[u8]) -> Result<(), Status> {
    if encoded.is_empty() || encoded.len() > MAX_OBJECT_SNAPSHOT_BYTES {
        return Err(Status::resource_exhausted(
            "object snapshot exceeds the private peer limit",
        ));
    }
    Ok(())
}

pub(super) fn encode_object_snapshot<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    let encoded = encode_typed(value)?;
    require_object_snapshot_bound(&encoded)?;
    Ok(encoded)
}

fn validate_snapshot_identity(
    snapshot: Option<&ObjectPathSnapshot>,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> Result<(), Status> {
    if tenant_id == 0 || bucket_id == 0 || ObjectKey::new("t", "b", exact_path).is_err() {
        return Err(Status::invalid_argument(
            "object snapshot exact-path identity is invalid",
        ));
    }
    if let Some(snapshot) = snapshot {
        snapshot.validate().map_err(map_object_snapshot_error)?;
        if snapshot.tenant_id != tenant_id
            || snapshot.bucket_id != bucket_id
            || snapshot.exact_path != exact_path
        {
            return Err(Status::invalid_argument(
                "object snapshot does not match its requested exact path",
            ));
        }
    }
    Ok(())
}

pub(super) fn map_object_snapshot_error(error: ObjectSnapshotError) -> Status {
    match error {
        ObjectSnapshotError::InvalidCursor
        | ObjectSnapshotError::InvalidExportLimit(_)
        | ObjectSnapshotError::InvalidRecord(_) => Status::invalid_argument(error.to_string()),
        ObjectSnapshotError::ExportRecordTooLarge { .. } => {
            Status::resource_exhausted(error.to_string())
        }
        ObjectSnapshotError::SnapshotConflict => Status::failed_precondition(error.to_string()),
        ObjectSnapshotError::RepairPreconditionFailed => Status::unavailable(error.to_string()),
        ObjectSnapshotError::Storage(_) => Status::internal(error.to_string()),
    }
}

impl DataPeerService {
    pub(super) async fn read_object_path_snapshot_call(
        &self,
        mut request: Request<wire::ObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let store = self.store.clone();
        let snapshot = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    store.export_object_path_record(
                        request.tenant_id,
                        request.bucket_id,
                        &request.exact_path,
                    )
                })
                .await
                .map_err(|error| Status::internal(format!("object snapshot read: {error}")))?
                .map_err(map_object_snapshot_error)
            })
            .await?;
        let snapshot_json = encode_object_snapshot(&snapshot)?;
        Ok(Response::new(wire::ObjectPathSnapshotResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            snapshot_json,
        }))
    }

    pub(super) async fn repair_object_path_snapshot_call(
        &self,
        mut request: Request<wire::RepairObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotApplied>, Status> {
        let _permit = self.cutover_admission.enter_continuation()?;
        let peer = request.get_ref().peer.clone();
        let peer = self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let placement_fence = self.mutation_admission.object_repair(
            peer,
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().exact_path,
            anvil_store::PlacementLogId {
                term: request.get_ref().placement_fence_term,
                index: request.get_ref().placement_fence_index,
            },
        )?;
        require_object_snapshot_bound(&request.get_ref().expected_snapshot_json)?;
        require_object_snapshot_bound(&request.get_ref().selected_snapshot_json)?;
        let expected: Option<ObjectPathSnapshot> =
            decode_typed(&request.get_ref().expected_snapshot_json)?;
        let selected: Option<ObjectPathSnapshot> =
            decode_typed(&request.get_ref().selected_snapshot_json)?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let store = self.store.clone();
        let admission = self.mutation_admission.clone();
        let applied = self
            .bounded(&metadata, async move {
                admission.require_fence(placement_fence)?;
                let applied = store
                    .repair_object_path_snapshot(
                        request.tenant_id,
                        request.bucket_id,
                        &request.exact_path,
                        expected.as_ref(),
                        selected.as_ref(),
                    )
                    .await
                    .map_err(map_object_snapshot_error)?;
                admission.require_fence(placement_fence)?;
                Ok(applied)
            })
            .await?;
        Ok(Response::new(wire::ObjectPathSnapshotApplied {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            present: applied.retained,
            version: applied.version.map_or(0, |version| version.0),
            replayed: applied.replayed,
        }))
    }
}

impl DataPeerTransport {
    pub(crate) async fn read_object_path_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let response = self
            .client(target, address)?
            .read_object_path_snapshot(wire::ObjectPathSnapshotRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_path: exact_path.to_owned(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        require_object_snapshot_bound(&response.snapshot_json)?;
        let snapshot: Option<ObjectPathSnapshot> = decode_typed(&response.snapshot_json)?;
        validate_snapshot_identity(snapshot.as_ref(), tenant_id, bucket_id, exact_path)?;
        Ok(snapshot)
    }

    pub(crate) async fn repair_object_path_snapshot(
        &self,
        target: NodeId,
        address: &str,
        placement_fence: anvil_store::PlacementLogId,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        expected: Option<&ObjectPathSnapshot>,
        selected: Option<&ObjectPathSnapshot>,
    ) -> Result<ObjectSnapshotApplied, Status> {
        validate_snapshot_identity(expected, tenant_id, bucket_id, exact_path)?;
        validate_snapshot_identity(selected, tenant_id, bucket_id, exact_path)?;
        let response = self
            .client(target, address)?
            .repair_object_path_snapshot(wire::RepairObjectPathSnapshotRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_path: exact_path.to_owned(),
                expected_snapshot_json: encode_object_snapshot(&expected)?,
                selected_snapshot_json: encode_object_snapshot(&selected)?,
                placement_fence_term: placement_fence.term,
                placement_fence_index: placement_fence.index,
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        if response.present != selected.is_some()
            || response.version != selected.map_or(0, |snapshot| snapshot.head.version.0)
        {
            return Err(Status::data_loss(
                "object snapshot repair acknowledgement disagrees with the selected state",
            ));
        }
        Ok(ObjectSnapshotApplied {
            version: response
                .present
                .then_some(anvil_store::VersionId(response.version)),
            replayed: response.replayed,
            retained: response.present,
        })
    }
}
