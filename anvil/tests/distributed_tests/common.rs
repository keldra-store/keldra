use std::time::Duration;

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, GetObjectRequest, ListObjectVersionsRequest, ListObjectVersionsResponse,
    NativeMutationContext, ObjectMetadata, PutObjectRequest, PutObjectResponse, ReadConsistency,
};
use anvil_test_utils::{
    DockerObjectObservation, DockerTestCluster, DockerTestStorageActor,
    create_docker_storage_test_actor, unique_test_name,
};
use futures::StreamExt;
use tonic::{Request, Status};

pub(super) const DISTRIBUTED_WAIT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(super) struct MutationIdentity {
    pub request_id: String,
    pub idempotency_key: String,
}

impl MutationIdentity {
    pub(super) fn unique(tag: &str) -> Self {
        let nonce = uuid::Uuid::new_v4();
        Self {
            request_id: format!("{tag}-{nonce}-request"),
            idempotency_key: format!("{tag}-{nonce}-idempotency"),
        }
    }
}

pub(super) struct DistributedFixture {
    pub actor: DockerTestStorageActor,
    pub bucket_name: String,
    pub bucket_id: i64,
}

pub(super) async fn create_fixture(cluster: &DockerTestCluster, label: &str) -> DistributedFixture {
    let actor = create_docker_storage_test_actor(cluster, label).await;
    let bucket_name = unique_test_name(&format!("{label}-bucket"));
    let mut client = BucketServiceClient::connect(cluster.equal_peer(1).grpc_addr)
        .await
        .expect("connect bucket service on equal peer 1");
    let mut request = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    add_actor_bearer(&mut request, &actor);
    request.set_timeout(DISTRIBUTED_WAIT);
    let bucket_id = client
        .create_bucket(request)
        .await
        .expect("create distributed-test bucket")
        .into_inner()
        .bucket_id;
    DistributedFixture {
        actor,
        bucket_name,
        bucket_id,
    }
}

pub(super) fn deterministic_bytes(len: usize, salt: u8) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ u64::from(salt);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

pub(super) async fn put_object_at(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
    content: &[u8],
    identity: &MutationIdentity,
) -> Result<PutObjectResponse, Status> {
    put_object_at_with_transaction(endpoint, fixture, object_key, content, identity, None).await
}

pub(super) async fn put_object_in_transaction_at(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
    content: &[u8],
    identity: &MutationIdentity,
    transaction_id: &str,
) -> Result<PutObjectResponse, Status> {
    put_object_at_with_transaction(
        endpoint,
        fixture,
        object_key,
        content,
        identity,
        Some(transaction_id),
    )
    .await
}

async fn put_object_at_with_transaction(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
    content: &[u8],
    identity: &MutationIdentity,
    transaction_id: Option<&str>,
) -> Result<PutObjectResponse, Status> {
    let mut client = ObjectServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let metadata = ObjectMetadata {
        bucket_name: fixture.bucket_name.clone(),
        object_key: object_key.to_string(),
        mutation_context: Some(native_mutation_context(fixture, identity, transaction_id)),
        content_type: Some("application/octet-stream".to_string()),
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let mut frames = vec![PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
            metadata,
        )),
    }];
    frames.extend(content.chunks(64 * 1024).map(|chunk| PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
            chunk.to_vec(),
        )),
    }));
    let mut request = Request::new(tokio_stream::iter(frames));
    add_actor_bearer(&mut request, &fixture.actor);
    request.set_timeout(DISTRIBUTED_WAIT);
    client
        .put_object(request)
        .await
        .map(tonic::Response::into_inner)
}

pub(super) async fn put_and_observe(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
    content: &[u8],
    identity: &MutationIdentity,
) -> DockerObjectObservation {
    let response = put_object_at(endpoint, fixture, object_key, content, identity)
        .await
        .unwrap_or_else(|status| panic!("put {object_key}: {status:?}"));
    DockerObjectObservation::from_put_response(&response, content.len())
}

pub(super) async fn get_object_at(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
) -> Result<Vec<u8>, Status> {
    let mut client = ObjectServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut request = Request::new(GetObjectRequest {
        bucket_name: fixture.bucket_name.clone(),
        object_key: object_key.to_string(),
        version_id: None,
        range: None,
        consistency: Some(latest_consistency()),
    });
    add_actor_bearer(&mut request, &fixture.actor);
    request.set_timeout(DISTRIBUTED_WAIT);
    let mut stream = client.get_object(request).await?.into_inner();
    let mut content = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = frame.data {
            content.extend_from_slice(&bytes);
        }
    }
    Ok(content)
}

pub(super) async fn list_object_versions_at(
    endpoint: &str,
    fixture: &DistributedFixture,
    object_key: &str,
) -> Result<ListObjectVersionsResponse, Status> {
    let mut client = ObjectServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut request = Request::new(ListObjectVersionsRequest {
        bucket_name: fixture.bucket_name.clone(),
        prefix: object_key.to_string(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
        consistency: Some(latest_consistency()),
        page_token: String::new(),
    });
    add_actor_bearer(&mut request, &fixture.actor);
    request.set_timeout(DISTRIBUTED_WAIT);
    client
        .list_object_versions(request)
        .await
        .map(tonic::Response::into_inner)
}

pub(super) fn assert_retryable_closed_failure(status: &Status, operation: &str) {
    assert!(
        matches!(
            status.code(),
            tonic::Code::Aborted
                | tonic::Code::DeadlineExceeded
                | tonic::Code::FailedPrecondition
                | tonic::Code::Unavailable
                | tonic::Code::Unknown
        ),
        "{operation} failed with an unexpected status: {status:?}"
    );
}

fn native_mutation_context(
    fixture: &DistributedFixture,
    identity: &MutationIdentity,
    transaction_id: Option<&str>,
) -> NativeMutationContext {
    NativeMutationContext {
        tenant_id: fixture.actor.tenant_id,
        bucket_id: fixture.bucket_id,
        principal: fixture.actor.app_id.clone(),
        request_id: identity.request_id.clone(),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: identity.idempotency_key.clone(),
        transaction_id: transaction_id.map(ToOwned::to_owned),
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    }
}

fn latest_consistency() -> ReadConsistency {
    ReadConsistency {
        mode: Some(anvil::anvil_api::read_consistency::Mode::Latest(true)),
    }
}

fn add_actor_bearer<T>(request: &mut Request<T>, actor: &DockerTestStorageActor) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token)
            .parse()
            .expect("actor bearer metadata is valid"),
    );
}
