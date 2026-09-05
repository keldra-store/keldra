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

fn validate_current_snapshot_identity(
    snapshot: Option<&CurrentObjectSnapshot>,
    tenant_id: u64,
    bucket_id: u64,
    exact_path: &str,
) -> Result<(), Status> {
    if tenant_id == 0 || bucket_id == 0 || ObjectKey::new("t", "b", exact_path).is_err() {
        return Err(Status::invalid_argument(
            "current object snapshot exact-path identity is invalid",
        ));
    }
    if let Some(snapshot) = snapshot {
        snapshot.validate().map_err(map_object_snapshot_error)?;
        if snapshot.tenant_id != tenant_id
            || snapshot.bucket_id != bucket_id
            || snapshot.exact_path != exact_path
        {
            return Err(Status::invalid_argument(
                "current object snapshot does not match its requested exact path",
            ));
        }
    }
    Ok(())
}

fn validate_snapshot_batch_request(
    tenant_id: u64,
    bucket_id: u64,
    exact_paths: &[String],
) -> Result<(), Status> {
    if tenant_id == 0
        || bucket_id == 0
        || exact_paths.is_empty()
        || exact_paths.len() > MAX_OBJECT_MUTATION_BATCH_ITEMS
    {
        return Err(Status::invalid_argument(format!(
            "object snapshot batch must contain 1..={MAX_OBJECT_MUTATION_BATCH_ITEMS} paths under valid stable IDs"
        )));
    }
    let mut request_bytes = 0_usize;
    for exact_path in exact_paths {
        if ObjectKey::new("t", "b", exact_path).is_err() {
            return Err(Status::invalid_argument(
                "object snapshot batch contains an invalid exact path",
            ));
        }
        request_bytes = request_bytes
            .checked_add(exact_path.len())
            .ok_or_else(|| Status::resource_exhausted("snapshot batch byte count overflow"))?;
        if request_bytes > MAX_OBJECT_SNAPSHOT_BYTES {
            return Err(Status::resource_exhausted(
                "current object snapshot batch exceeds the private peer limit",
            ));
        }
    }
    Ok(())
}

fn validate_exact_version_batch_request(
    tenant_id: u64,
    bucket_id: u64,
    exact_paths: &[String],
    version_ids: &[u64],
) -> Result<(), Status> {
    validate_snapshot_batch_request(tenant_id, bucket_id, exact_paths)?;
    if version_ids.len() != exact_paths.len() || version_ids.contains(&0) {
        return Err(Status::invalid_argument(
            "exact-version batch paths and non-zero version IDs must have equal lengths",
        ));
    }
    Ok(())
}

fn validate_snapshot_batch(
    snapshots: &[Option<ObjectPathSnapshot>],
    tenant_id: u64,
    bucket_id: u64,
    exact_paths: &[String],
) -> Result<(), Status> {
    if snapshots.len() != exact_paths.len() {
        return Err(Status::data_loss(
            "object snapshot batch result count disagrees with its request",
        ));
    }
    for (snapshot, exact_path) in snapshots.iter().zip(exact_paths) {
        validate_snapshot_identity(snapshot.as_ref(), tenant_id, bucket_id, exact_path)?;
    }
    Ok(())
}

fn validate_exact_version_batch(
    versions: &[Option<keldra_store::Version>],
    expected: &[u64],
) -> Result<(), Status> {
    if versions.len() != expected.len()
        || versions.iter().zip(expected).any(|(version, expected)| {
            version
                .as_ref()
                .is_some_and(|version| version.id.0 != *expected)
        })
    {
        return Err(Status::data_loss(
            "exact-version batch result disagrees with its request",
        ));
    }
    Ok(())
}

fn validate_current_snapshot_batch(
    snapshots: &[Option<CurrentObjectSnapshot>],
    tenant_id: u64,
    bucket_id: u64,
    exact_paths: &[String],
) -> Result<(), Status> {
    if snapshots.len() != exact_paths.len() {
        return Err(Status::data_loss(
            "current object snapshot batch result count disagrees with its request",
        ));
    }
    for (snapshot, exact_path) in snapshots.iter().zip(exact_paths) {
        validate_current_snapshot_identity(snapshot.as_ref(), tenant_id, bucket_id, exact_path)?;
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

    pub(super) async fn read_current_object_snapshot_call(
        &self,
        mut request: Request<wire::ObjectPathSnapshotRequest>,
    ) -> Result<Response<wire::CurrentObjectSnapshotResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let store = self.store.clone();
        let snapshot = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    store.export_current_object_snapshot(
                        request.tenant_id,
                        request.bucket_id,
                        &request.exact_path,
                    )
                })
                .await
                .map_err(|error| {
                    Status::internal(format!("current object snapshot read: {error}"))
                })?
                .map_err(map_object_snapshot_error)
            })
            .await?;
        let snapshot_json = encode_object_snapshot(&snapshot)?;
        Ok(Response::new(wire::CurrentObjectSnapshotResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            snapshot_json,
        }))
    }

    pub(super) async fn read_object_path_snapshots_call(
        &self,
        mut request: Request<wire::ObjectPathSnapshotBatchRequest>,
    ) -> Result<Response<wire::ObjectPathSnapshotBatchResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        validate_snapshot_batch_request(
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().exact_paths,
        )?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let tenant_id = request.tenant_id;
        let bucket_id = request.bucket_id;
        let exact_paths = request.exact_paths;
        let store = self.store.clone();
        let snapshots = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    store.export_object_path_records(tenant_id, bucket_id, &exact_paths)
                })
                .await
                .map_err(|error| Status::internal(format!("object snapshot batch read: {error}")))?
                .map_err(map_object_snapshot_error)
            })
            .await?;
        let snapshots_json = encode_object_snapshot(&snapshots)?;
        Ok(Response::new(wire::ObjectPathSnapshotBatchResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            snapshots_json,
        }))
    }

    pub(super) async fn read_current_object_snapshots_call(
        &self,
        mut request: Request<wire::CurrentObjectSnapshotBatchRequest>,
    ) -> Result<Response<wire::CurrentObjectSnapshotBatchResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        validate_snapshot_batch_request(
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().exact_paths,
        )?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let tenant_id = request.tenant_id;
        let bucket_id = request.bucket_id;
        let exact_paths = request.exact_paths;
        let store = self.store.clone();
        let snapshots = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    store.export_current_object_snapshots(tenant_id, bucket_id, &exact_paths)
                })
                .await
                .map_err(|error| {
                    Status::internal(format!("current object snapshot batch read: {error}"))
                })?
                .map_err(map_object_snapshot_error)
            })
            .await?;
        let snapshots_json = encode_object_snapshot(&snapshots)?;
        Ok(Response::new(wire::CurrentObjectSnapshotBatchResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            snapshots_json,
        }))
    }

    pub(super) async fn read_exact_object_versions_call(
        &self,
        mut request: Request<wire::ExactObjectVersionBatchRequest>,
    ) -> Result<Response<wire::ExactObjectVersionBatchResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        validate_exact_version_batch_request(
            request.get_ref().tenant_id,
            request.get_ref().bucket_id,
            &request.get_ref().exact_paths,
            &request.get_ref().version_ids,
        )?;
        let metadata = request.metadata().clone();
        let request = request.into_inner();
        let selections = request
            .exact_paths
            .into_iter()
            .zip(request.version_ids.into_iter().map(keldra_store::VersionId))
            .collect::<Vec<_>>();
        let store = self.store.clone();
        let versions = self
            .bounded(&metadata, async move {
                tokio::task::spawn_blocking(move || {
                    store.export_exact_object_versions(
                        request.tenant_id,
                        request.bucket_id,
                        &selections,
                    )
                })
                .await
                .map_err(|error| Status::internal(format!("exact-version batch read: {error}")))?
                .map_err(map_object_snapshot_error)
            })
            .await?;
        let versions_json = encode_object_snapshot(&versions)?;
        Ok(Response::new(wire::ExactObjectVersionBatchResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            versions_json,
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
            keldra_store::PlacementLogId {
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

    pub(crate) async fn read_current_object_snapshot(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        let response = self
            .client(target, address)?
            .read_current_object_snapshot(wire::ObjectPathSnapshotRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_path: exact_path.to_owned(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        require_object_snapshot_bound(&response.snapshot_json)?;
        let snapshot: Option<CurrentObjectSnapshot> = decode_typed(&response.snapshot_json)?;
        validate_current_snapshot_identity(snapshot.as_ref(), tenant_id, bucket_id, exact_path)?;
        Ok(snapshot)
    }

    pub(crate) async fn read_object_path_snapshots(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_paths: &[String],
    ) -> Result<Vec<Option<ObjectPathSnapshot>>, Status> {
        validate_snapshot_batch_request(tenant_id, bucket_id, exact_paths)?;
        let response = self
            .client(target, address)?
            .read_object_path_snapshots(wire::ObjectPathSnapshotBatchRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_paths: exact_paths.to_vec(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        require_object_snapshot_bound(&response.snapshots_json)?;
        let snapshots: Vec<Option<ObjectPathSnapshot>> = decode_typed(&response.snapshots_json)?;
        validate_snapshot_batch(&snapshots, tenant_id, bucket_id, exact_paths)?;
        Ok(snapshots)
    }

    pub(crate) async fn read_current_object_snapshots(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        exact_paths: &[String],
    ) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
        validate_snapshot_batch_request(tenant_id, bucket_id, exact_paths)?;
        let response = self
            .client(target, address)?
            .read_current_object_snapshots(wire::CurrentObjectSnapshotBatchRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_paths: exact_paths.to_vec(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        require_object_snapshot_bound(&response.snapshots_json)?;
        let snapshots: Vec<Option<CurrentObjectSnapshot>> = decode_typed(&response.snapshots_json)?;
        validate_current_snapshot_batch(&snapshots, tenant_id, bucket_id, exact_paths)?;
        Ok(snapshots)
    }

    pub(crate) async fn read_exact_object_versions(
        &self,
        target: NodeId,
        address: &str,
        tenant_id: u64,
        bucket_id: u64,
        selections: &[(String, keldra_store::VersionId)],
    ) -> Result<Vec<Option<keldra_store::Version>>, Status> {
        let exact_paths = selections
            .iter()
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        let version_ids = selections
            .iter()
            .map(|(_, version)| version.0)
            .collect::<Vec<_>>();
        validate_exact_version_batch_request(tenant_id, bucket_id, &exact_paths, &version_ids)?;
        let response = self
            .client(target, address)?
            .read_exact_object_versions(wire::ExactObjectVersionBatchRequest {
                peer: Some(self.context()),
                tenant_id,
                bucket_id,
                exact_paths,
                version_ids: version_ids.clone(),
            })
            .await?
            .into_inner();
        require_response_schema(response.schema_version)?;
        require_object_snapshot_bound(&response.versions_json)?;
        let versions: Vec<Option<keldra_store::Version>> = decode_typed(&response.versions_json)?;
        validate_exact_version_batch(&versions, &version_ids)?;
        Ok(versions)
    }

    pub(crate) async fn repair_object_path_snapshot(
        &self,
        target: NodeId,
        address: &str,
        placement_fence: keldra_store::PlacementLogId,
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
                .then_some(keldra_store::VersionId(response.version)),
            replayed: response.replayed,
            retained: response.present,
        })
    }
}
