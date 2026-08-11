use super::*;

pub(super) async fn start_put(
    service: &ObjectServiceImpl,
    request: Request<PutHeader>,
) -> Result<Response<PutToken>, Status> {
    let path_access = object_path_access::access_for(&request);
    let surface = UploadSurface::from_internal(object_path_access::is_internal(&path_access));
    let started = Instant::now();
    let result = async {
        let caller = authenticated_caller(&request)?;
        let metadata = put_metadata(request.into_inner())?;
        object_path_access::require_key(&path_access, &metadata.key)?;
        service
            .authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;
        service
            .issue_upload_token(&caller, &metadata)
            .map(Response::new)
    }
    .await;
    record_upload_phase(UploadPhase::StartPut, surface, started, 0, &result);
    result
}

pub(super) async fn put(
    service: &ObjectServiceImpl,
    request: Request<Streaming<ApiPutRequest>>,
) -> Result<Response<PutToken>, Status> {
    let path_access = object_path_access::access_for(&request);
    let surface = UploadSurface::from_internal(object_path_access::is_internal(&path_access));
    let started = Instant::now();
    let mut received_bytes = 0_u64;
    let result = async {
        let caller = authenticated_caller(&request)?;
        let mut stream = request.into_inner();
        let first = tokio::time::timeout(PUT_TOKEN_LIFETIME, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("put stream inactivity lease expired"))??
            .ok_or_else(|| Status::invalid_argument("put stream is empty"))?;
        received_bytes = received_bytes.saturating_add(first.chunk.len() as u64);
        let token = required_put_token(first.token)?;
        let capability = service.verify_put_token(&caller, &token)?;
        let header = require_upload_phase(capability)?;
        let metadata = header.to_metadata()?;
        object_path_access::require_key(&path_access, &metadata.key)?;
        service
            .authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;

        let mut upload = service.store.begin_blob_upload().await.map_err(status)?;
        let mut length = 0_u64;
        write_upload_chunk(
            &mut upload,
            &mut length,
            &first.chunk,
            service.max_blob_bytes,
        )
        .await?;
        loop {
            let frame = tokio::time::timeout(PUT_TOKEN_LIFETIME, stream.message())
                .await
                .map_err(|_| Status::deadline_exceeded("put stream inactivity lease expired"))??;
            let Some(frame) = frame else {
                break;
            };
            received_bytes = received_bytes.saturating_add(frame.chunk.len() as u64);
            let frame_token = required_put_token(frame.token)?;
            if frame_token != token {
                return Err(Status::invalid_argument(
                    "put stream contains a missing or different upload token",
                ));
            }
            write_upload_chunk(
                &mut upload,
                &mut length,
                &frame.chunk,
                service.max_blob_bytes,
            )
            .await?;
        }
        let blob = service
            .store
            .seal_blob_upload(upload)
            .await
            .map_err(status)?;
        if !object_path_access::is_internal(&path_access) {
            service.record_accounting_inbound(&metadata.key, length);
        }
        service
            .issue_ready_token(&caller, header, &blob)
            .map(Response::new)
    }
    .await;
    record_upload_phase(UploadPhase::Put, surface, started, received_bytes, &result);
    result
}

pub(super) async fn put_end(
    service: &ObjectServiceImpl,
    request: Request<PutToken>,
) -> Result<Response<ApiMutationReceipt>, Status> {
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let path_access = object_path_access::access_for(&request);
    let surface = if peer_routed {
        UploadSurface::Peer
    } else {
        UploadSurface::from_internal(object_path_access::is_internal(&path_access))
    };
    let started = Instant::now();
    let mut payload_bytes = 0_u64;
    let result = async {
        let caller = authenticated_caller(&request)?;
        let bearer = OriginalBearer::from_metadata(request.metadata())?;
        let remaining =
            effective_atomic_program_timeout(request.metadata(), service.atomic_program_timeout);
        let token = required_put_token(Some(request.into_inner()))?;
        let capability = service.verify_put_token(&caller, &token)?;
        let ready = require_ready_phase(capability)?;
        payload_bytes = ready.blob_length;
        let metadata = ready.header.to_metadata()?;
        object_path_access::require_key(&path_access, &metadata.key)?;
        service
            .authorize_object(&caller, &metadata.key, ObjectPermission::Put)
            .await?;
        let publish = PublishRequest {
            key: metadata.key,
            blob: BlobRef {
                hash: ready.blob_hash,
                length: ready.blob_length,
            },
            content_type: metadata.content_type,
            mode: metadata.mode,
            command_id: Some(metadata.command_id),
            durability: metadata.durability,
        };
        let receipt = match service.distribution.routing_target(&publish.key)? {
            Some(_) if peer_routed => {
                return Err(Status::failed_precondition(
                    "a routed PutEnd reached a node that is not its coordinator",
                ));
            }
            Some((target, address)) => {
                if object_path_access::is_internal(&path_access) {
                    service
                        .cluster_peers
                        .route_internal_put_end(
                            target,
                            &address,
                            bearer.signed_token(),
                            token,
                            remaining,
                        )
                        .await?
                } else {
                    service
                        .cluster_peers
                        .route_put_end(target, &address, bearer.signed_token(), token, remaining)
                        .await?
                }
            }
            None => {
                let governance = service
                    .bucket_governance
                    .resolve(publish.key.tenant(), publish.key.bucket())
                    .await?;
                api_receipt(
                    service
                        .distribution
                        .publish_from_source_with_governance(
                            publish,
                            anvil_consensus::NodeId(ready.upload_source_node_id),
                            governance,
                        )
                        .await?,
                )
            }
        };
        Ok(Response::new(receipt))
    }
    .await;
    record_upload_phase(
        UploadPhase::PutEnd,
        surface,
        started,
        payload_bytes,
        &result,
    );
    if result.is_ok() && surface == UploadSurface::Public {
        tracing::debug!(
            monotonic_counter.anvil_object_publications_total = 1_u64,
            monotonic_counter.anvil_object_publication_payload_bytes_total = payload_bytes,
            "public object publication completed"
        );
    }
    result
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadPhase {
    StartPut,
    Put,
    PutEnd,
}

impl UploadPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::StartPut => "start_put",
            Self::Put => "put",
            Self::PutEnd => "put_end",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadSurface {
    Public,
    Internal,
    Peer,
}

impl UploadSurface {
    fn from_internal(internal: bool) -> Self {
        if internal {
            Self::Internal
        } else {
            Self::Public
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Peer => "peer",
        }
    }
}

fn record_upload_phase<T>(
    phase: UploadPhase,
    surface: UploadSurface,
    started: Instant,
    payload_bytes: u64,
    result: &Result<T, Status>,
) {
    let (outcome, status_code) = match result {
        Ok(_) => ("success", "OK"),
        Err(status) => ("failure", grpc_status_code_name(status.code())),
    };
    tracing::debug!(
        monotonic_counter.anvil_object_upload_phase_requests_total = 1_u64,
        histogram.anvil_object_upload_phase_duration_seconds = started.elapsed().as_secs_f64(),
        phase = phase.as_str(),
        surface = surface.as_str(),
        result = outcome,
        grpc_status_code = status_code,
        "object upload phase completed"
    );
    if phase == UploadPhase::Put {
        tracing::debug!(
            monotonic_counter.anvil_object_upload_received_bytes_total = payload_bytes,
            surface = surface.as_str(),
            result = outcome,
            grpc_status_code = status_code,
            "object upload stream bytes received"
        );
    }
}

fn grpc_status_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_metric_dimensions_are_fixed_and_bounded() {
        assert_eq!(UploadPhase::StartPut.as_str(), "start_put");
        assert_eq!(UploadPhase::Put.as_str(), "put");
        assert_eq!(UploadPhase::PutEnd.as_str(), "put_end");
        assert_eq!(UploadSurface::Public.as_str(), "public");
        assert_eq!(UploadSurface::Internal.as_str(), "internal");
        assert_eq!(UploadSurface::Peer.as_str(), "peer");
        assert_eq!(
            grpc_status_code_name(tonic::Code::ResourceExhausted),
            "RESOURCE_EXHAUSTED"
        );
    }
}
