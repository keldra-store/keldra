#![recursion_limit = "256"]

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::coordination_service_client::CoordinationServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    self, AbortMultipartRequest, AcquireTaskLeaseRequest, AppendStreamRecordRequest,
    CompareAndSwapManifestRequest, CompleteMultipartPart, CompleteMultipartRequest,
    ComposeObjectRequest, ComposeObjectSource, CopyObjectRequest, CreateAppendStreamRequest,
    CreateBucketRequest, CreateIndexRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectRequest, IndexKind, InitiateMultipartRequest, LeaseFencePrecondition,
    ListObjectVersionsRequest, ListObjectsRequest, MutationBatchOperation,
    MutationBatchPatchJsonObject, MutationBatchPutObject, MutationBatchRequest,
    NativeMutationContext, ObjectMetadata, ObjectVersionPrecondition, PatchJsonObjectRequest,
    PutObjectRequest, ReadAppendStreamRequest, RepairDirectoryIndexRequest,
    SealAppendStreamSegmentRequest, TailAppendStreamRequest, UploadPartMetadata, UploadPartRequest,
    WatchPrefixRequest, WritePrecondition, WriteVisibilityOptions,
};
use futures_util::StreamExt;
use std::{future::Future, pin::Pin, time::Duration};
use tonic::Request;

use anvil::observability::{
    OBJECT_READ_LATENCY, OBJECT_WRITE_LATENCY, PREFIX_LIST_LATENCY,
    RESERVED_NAMESPACE_REJECTION_COUNT,
};
use anvil::routing::CrossRegionRoutingPolicy;
use anvil::{
    auth::Claims,
    mesh_directory::{
        self, BucketId, BucketLocatorDescriptor, BucketName, CellId, MeshControlWriteAuthority,
        MeshId, RegionName, RoutingRecordFamily, TenantId,
    },
    partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    },
};
use anvil_test_utils::*;
use tonic::{Code, Status};

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn assert_reserved_namespace_status<T>(result: Result<T, Status>) {
    let err = match result {
        Ok(_) => panic!("reserved namespace operation must fail"),
        Err(err) => err,
    };
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(
        err.message().contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace, got {err:?}"
    );
}

async fn put_native_object_bytes(
    actor: &ObjectTestActor,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    bucket_id: i64,
    object_key: &str,
    bytes: Vec<u8>,
    content_type: Option<&str>,
) -> anvil_api::PutObjectResponse {
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    mutation_context: Some(native_mutation_context(
                        actor,
                        bucket_id,
                        "object-metadata",
                    )),
                    content_type: content_type.map(ToOwned::to_owned),
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(bytes)),
        },
    ];
    let mut request = Request::new(tokio_stream::iter(chunks));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    client
        .put_object(request)
        .await
        .expect("put object through native service")
        .into_inner()
}

#[tokio::test]
async fn native_object_routes_use_mesh_locator_before_local_bucket_metadata() {
    let cluster = isolated_test_cluster_with_config(
        "uses a custom cross-region routing policy and direct mesh locator writes",
        &["us-west-2"],
        |config| {
            config.cross_region_routing_policy = CrossRegionRoutingPolicy::ProxyRequired;
        },
    )
    .await;
    let state = &cluster.states[0];
    let signing_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
    let bucket_name = BucketName::canonicalize("remote-bucket").unwrap();
    let locator = BucketLocatorDescriptor::active(
        MeshId::new("default").unwrap(),
        TenantId::new("1").unwrap(),
        bucket_name.clone(),
        BucketId::new("bucket-remote").unwrap(),
        RegionName::new("eu-west-1").unwrap(),
        CellId::new("default").unwrap(),
        "regional-primary",
        "objects/1/remote-bucket/",
        "2026-07-02T00:00:00Z",
    )
    .unwrap();
    let partition = locator.partition();
    let control_partition_id = mesh_directory::control_partition_id(
        RoutingRecordFamily::BucketLocator.stream_family(),
        &partition,
    );
    let recovering = acquire_partition_recovery(
        &state.storage,
        PartitionRecoveryAcquire {
            partition_family: mesh_directory::CONTROL_PARTITION_FAMILY.to_string(),
            partition_id: control_partition_id,
            owner_node_id: state.config.node_id.clone(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 1,
        },
        &signing_key,
    )
    .await
    .unwrap();
    let ready = publish_partition_ready(
        &state.storage,
        &recovering.partition_family,
        &recovering.partition_id,
        &state.config.node_id,
        recovering.fence_token,
        0,
        &hex::encode([0; 32]),
        2,
        &signing_key,
    )
    .await
    .unwrap();
    mesh_directory::write_bucket_locator(
        &state.storage,
        &locator,
        MeshControlWriteAuthority {
            permit: &ready.write_permit().unwrap(),
            signing_key: &signing_key,
        },
    )
    .await
    .unwrap();

    let err = state
        .object_manager
        .list_objects_for_tenant(
            Some(Claims {
                sub: "test-app".to_string(),
                exp: usize::MAX,
                tenant_id: 1,
                jti: None,
            }),
            Some(1),
            bucket_name.as_str(),
            "",
            "",
            10,
            "",
            anvil::object_manager::ObjectReadConsistency::Latest,
        )
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata()
            .get("x-anvil-bucket-region")
            .unwrap()
            .to_str()
            .unwrap(),
        "eu-west-1"
    );
    assert_eq!(
        err.metadata()
            .get("x-anvil-cross-region-action")
            .unwrap()
            .to_str()
            .unwrap(),
        "proxy_unavailable"
    );

    let write_err = state
        .object_manager
        .initiate_multipart_upload(
            &Claims {
                sub: "test-app".to_string(),
                exp: usize::MAX,
                tenant_id: 1,
                jti: None,
            },
            bucket_name.as_str(),
            "upload.bin",
            None,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(write_err.code(), Code::Unavailable);
    assert_eq!(
        write_err
            .metadata()
            .get("x-anvil-bucket-region")
            .unwrap()
            .to_str()
            .unwrap(),
        "eu-west-1"
    );
}

type ObjectTestActor = TestStorageActor;

trait ObjectActorCluster {
    fn create_object_actor<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = ObjectTestActor> + Send + 'a>>;
}

impl ObjectActorCluster for TestCluster {
    fn create_object_actor<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = ObjectTestActor> + Send + 'a>> {
        Box::pin(async move { create_storage_test_actor(self, label).await })
    }
}

impl ObjectActorCluster for DockerTestCluster {
    fn create_object_actor<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = ObjectTestActor> + Send + 'a>> {
        Box::pin(async move { create_docker_storage_test_actor(self, label).await.into() })
    }
}

impl ObjectActorCluster for SharedTestCluster {
    fn create_object_actor<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = ObjectTestActor> + Send + 'a>> {
        Box::pin(async move { create_storage_test_actor(self, label).await })
    }
}

impl ObjectActorCluster for SharedDockerTestCluster {
    fn create_object_actor<'a>(
        &'a self,
        label: &'a str,
    ) -> Pin<Box<dyn Future<Output = ObjectTestActor> + Send + 'a>> {
        Box::pin(async move { create_docker_storage_test_actor(self, label).await.into() })
    }
}

async fn create_object_test_actor<C>(cluster: &C, label: &str) -> ObjectTestActor
where
    C: ObjectActorCluster + ?Sized,
{
    cluster.create_object_actor(label).await
}

fn native_mutation_context(
    actor: &ObjectTestActor,
    bucket_id: i64,
    tag: &str,
) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: actor.tenant_id,
        bucket_id,
        principal: actor.app_id.clone(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: Some(strict_write_visibility()),
    }
}

fn strict_write_visibility() -> WriteVisibilityOptions {
    WriteVisibilityOptions {
        indexes: 1,
        watches: 1,
        authz_materialization: 1,
        boundary_extraction: 1,
        index_policy_snapshot: 1,
        authz_revision: 1,
    }
}

fn native_mutation_context_with_precondition(
    actor: &ObjectTestActor,
    bucket_id: i64,
    tag: &str,
    precondition: &str,
) -> NativeMutationContext {
    let mut context = native_mutation_context(actor, bucket_id, tag);
    context.precondition = precondition.to_string();
    context
}

fn put_object_chunks(
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
    mutation_context: Option<NativeMutationContext>,
) -> Vec<PutObjectRequest> {
    vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    mutation_context,
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                payload.to_vec(),
            )),
        },
    ]
}

async fn put_object_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
    mutation_context: NativeMutationContext,
) -> Result<anvil_api::PutObjectResponse, Status> {
    let request = authorized(
        tokio_stream::iter(put_object_chunks(
            bucket_name,
            object_key,
            payload,
            Some(mutation_context),
        )),
        token,
    );
    object_client
        .put_object(request)
        .await
        .map(|response| response.into_inner())
}

async fn get_object_bytes_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    version_id: Option<String>,
) -> Vec<u8> {
    let mut stream = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
                version_id,
                range: None,

                ..Default::default()
            },
            token,
        ))
        .await
        .expect("get object")
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("object chunk");
        if let Some(anvil_api::get_object_response::Data::Chunk(data)) = chunk.data {
            bytes.extend_from_slice(&data);
        }
    }
    bytes
}

async fn get_object_metadata_and_bytes_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    version_id: Option<String>,
) -> (anvil_api::ObjectInfo, Vec<u8>) {
    let mut stream = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
                version_id,
                range: None,

                ..Default::default()
            },
            token,
        ))
        .await
        .expect("get object")
        .into_inner();
    let mut metadata = None;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("object chunk");
        match chunk.data {
            Some(anvil_api::get_object_response::Data::Metadata(info)) => {
                metadata = Some(info);
            }
            Some(anvil_api::get_object_response::Data::Chunk(data)) => {
                bytes.extend_from_slice(&data);
            }
            None => {}
        }
    }
    (metadata.expect("get object metadata"), bytes)
}

macro_rules! assert_native_mutation_response {
    ($response:expr) => {{
        assert!(!$response.mutation_id.is_empty());
        assert!(!$response.payload_hash.is_empty());
        assert!(!$response.record_hash.is_empty());
        assert!($response.watch_cursor > 0);
    }};
}

#[path = "object_tests/batch_cas_multipart.rs"]
mod batch_cas_multipart;
#[path = "object_tests/copy_private_watch_stream.rs"]
mod copy_private_watch_stream;
#[path = "object_tests/mutation_batch_transactions.rs"]
mod mutation_batch_transactions;
#[path = "object_tests/native_delete_listing.rs"]
mod native_delete_listing;
#[path = "object_tests/patch_and_list.rs"]
mod patch_and_list;
#[path = "object_tests/planner_listing.rs"]
mod planner_listing;
#[path = "object_tests/reserved_head_core.rs"]
mod reserved_head_core;
