#![recursion_limit = "256"]

use anvil::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, GetObjectRequest, NativeMutationContext, ObjectMetadata, ProxyHeader,
    ProxyRequestChunk, ProxyRequestHeader, bucket_service_client::BucketServiceClient,
    proxy_request_chunk, proxy_response_chunk,
};
use anvil::auth::Claims;
use anvil_test_utils::{
    DockerTestCluster, DockerTestStorageActor, create_docker_storage_test_actor,
    shared_docker_test_cluster, unique_test_name,
};
use futures_util::StreamExt;
use tokio_stream::iter;

fn with_bearer<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn proxy_header(name: &str, value: impl AsRef<[u8]>) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.as_ref().to_vec(),
    }
}

fn proxy_authz_context(claims: &Claims) -> Vec<u8> {
    anvil::services::internal_proxy::encode_proxy_authz_context(claims).unwrap()
}

fn canonical_proxy_host(
    cluster: &DockerTestCluster,
    actor: &DockerTestStorageActor,
    bucket_name: &str,
) -> String {
    format!(
        "{bucket_name}.{}.{}",
        actor.tenant_id, cluster.public_region_host
    )
}

async fn create_proxy_bucket(actor: &DockerTestStorageActor, prefix: &str) -> String {
    create_proxy_bucket_with_id(actor, prefix).await.0
}

async fn create_proxy_bucket_with_id(
    actor: &DockerTestStorageActor,
    prefix: &str,
) -> (String, i64) {
    let bucket_name = unique_test_name(prefix);
    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let bucket_id = bucket_client
        .create_bucket(with_bearer(
            tonic::Request::new(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: actor.region.clone(),
                options: None,
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;
    (bucket_name, bucket_id)
}

fn actor_claims(actor: &DockerTestStorageActor, jti: Option<&str>) -> Claims {
    Claims {
        sub: actor.app_id.clone(),
        exp: usize::MAX,
        tenant_id: actor.tenant_id,
        jti: jti.map(ToOwned::to_owned),
    }
}

#[tokio::test]
async fn internal_proxy_put_and_get_preserve_original_principal_authority() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "proxy-bucket").await;
    let bucket_name = create_proxy_bucket(&actor, "proxy-bucket").await;

    let original_claims = actor_claims(&actor, Some("original-jti"));
    let internal_token = cluster.admin_token();

    let mut proxy_client = InternalProxyServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let put_request_id = unique_test_name("proxy-put");
    let put_header = ProxyRequestHeader {
        request_id: put_request_id.clone(),
        idempotency_key: put_request_id,
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: bucket_name.clone(),
        object_key: "via-proxy.txt".to_string(),
        method: "PUT".to_string(),
        canonical_host: canonical_proxy_host(&cluster, &actor, &bucket_name),
        canonical_path: "/via-proxy.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![proxy_header("content-type", "text/plain")],
        authz_context: proxy_authz_context(&original_claims),
        native_object_metadata: None,
    };
    let put_stream = iter(vec![
        ProxyRequestChunk {
            part: Some(proxy_request_chunk::Part::Header(put_header)),
        },
        ProxyRequestChunk {
            part: Some(proxy_request_chunk::Part::Body(
                b"written through proxy".to_vec(),
            )),
        },
    ]);
    let mut put_response = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(put_stream),
            &internal_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let put_header = put_response.next().await.unwrap().unwrap();
    let proxy_response_chunk::Part::Header(put_response_header) = put_header.part.unwrap() else {
        panic!("proxy put must return a response header first");
    };
    assert_eq!(put_response_header.status, 200);
    assert!(put_response_header.committed);
    assert!(put_response_header.headers.iter().any(|h| h.name == "etag"));
    assert!(put_response.next().await.is_none());

    let get_header = ProxyRequestHeader {
        request_id: unique_test_name("proxy-get"),
        idempotency_key: "".to_string(),
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: bucket_name.clone(),
        object_key: "via-proxy.txt".to_string(),
        method: "GET".to_string(),
        canonical_host: canonical_proxy_host(&cluster, &actor, &bucket_name),
        canonical_path: "/via-proxy.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![],
        authz_context: proxy_authz_context(&original_claims),
        native_object_metadata: None,
    };
    let get_stream = iter(vec![ProxyRequestChunk {
        part: Some(proxy_request_chunk::Part::Header(get_header)),
    }]);
    let mut get_response = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(get_stream),
            &internal_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let first = get_response.next().await.unwrap().unwrap();
    let proxy_response_chunk::Part::Header(get_response_header) = first.part.unwrap() else {
        panic!("proxy get must return a response header first");
    };
    assert_eq!(get_response_header.status, 200);
    assert_eq!(
        get_response_header
            .headers
            .iter()
            .find(|h| h.name == "content-type")
            .map(|h| String::from_utf8_lossy(&h.value).to_string())
            .as_deref(),
        Some("text/plain")
    );
    let mut body = Vec::new();
    while let Some(chunk) = get_response.next().await {
        let chunk = chunk.unwrap();
        if let Some(proxy_response_chunk::Part::Body(bytes)) = chunk.part {
            body.extend_from_slice(&bytes);
        }
    }
    assert_eq!(body, b"written through proxy");

    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let normal_get = object_client
        .get_object(with_bearer(
            tonic::Request::new(GetObjectRequest {
                bucket_name,
                object_key: "via-proxy.txt".to_string(),
                version_id: None,
                range: None,

                ..Default::default()
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    tokio::pin!(normal_get);
    let mut normal_body = Vec::new();
    while let Some(chunk) = normal_get.next().await {
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.unwrap().data
        {
            normal_body.extend_from_slice(&bytes);
        }
    }
    assert_eq!(normal_body, b"written through proxy");
}

#[tokio::test]
async fn internal_proxy_native_put_preserves_context_and_idempotent_response() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "native-proxy-bucket").await;
    let (bucket_name, bucket_id) = create_proxy_bucket_with_id(&actor, "native-proxy-bucket").await;
    let original_claims = actor_claims(&actor, Some("native-proxy-jti"));
    let internal_token = cluster.admin_token();
    let request_id = unique_test_name("native-proxy-put");
    let idempotency_key = unique_test_name("native-proxy-idempotency");
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: "native-via-proxy.txt".to_string(),
        mutation_context: Some(NativeMutationContext {
            tenant_id: actor.tenant_id,
            bucket_id,
            principal: actor.app_id.clone(),
            request_id: request_id.clone(),
            precondition: "none".to_string(),
            authz_zookie_optional: String::new(),
            idempotency_key: idempotency_key.clone(),
            transaction_id: None,
            saga_operation: None,
            saga_compensation_operation: None,
            write_visibility: None,
        }),
        content_type: Some("text/plain".to_string()),
        user_metadata_json: r#"{"source":"native-proxy-test"}"#.to_string(),
        storage_class: None,
    };
    let header = ProxyRequestHeader {
        request_id,
        idempotency_key,
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: bucket_name.clone(),
        object_key: metadata.object_key.clone(),
        method: "NATIVE_PUT".to_string(),
        canonical_host: canonical_proxy_host(&cluster, &actor, &bucket_name),
        canonical_path: "/anvil.ObjectService/PutObject".to_string(),
        bucket_locator_generation: 1,
        headers: Vec::new(),
        authz_context: proxy_authz_context(&original_claims),
        native_object_metadata: Some(metadata),
    };
    let body = b"native context survives peer routing".to_vec();
    let mut expected_response = None;
    for _ in 0..2 {
        let mut proxy_client = InternalProxyServiceClient::connect(actor.grpc_addr.clone())
            .await
            .unwrap();
        let stream = iter(vec![
            ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(header.clone())),
            },
            ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Body(body.clone())),
            },
        ]);
        let mut response = proxy_client
            .proxy_object(with_bearer(tonic::Request::new(stream), &internal_token))
            .await
            .unwrap()
            .into_inner();
        let first = response.next().await.unwrap().unwrap();
        let proxy_response_chunk::Part::Header(response_header) = first.part.unwrap() else {
            panic!("native proxy put must return a response header first");
        };
        assert_eq!(response_header.status, 200);
        assert!(response_header.committed);
        let native_response = response_header
            .native_put_response
            .expect("native proxy response");
        assert!(!native_response.version_id.is_empty());
        assert!(!native_response.mutation_id.is_empty());
        assert!(!native_response.payload_hash.is_empty());
        if let Some(expected) = expected_response.as_ref() {
            assert_eq!(&native_response, expected);
        } else {
            expected_response = Some(native_response);
        }
        assert!(response.next().await.is_none());
    }

    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let object_stream = object_client
        .get_object(with_bearer(
            tonic::Request::new(GetObjectRequest {
                bucket_name,
                object_key: "native-via-proxy.txt".to_string(),
                ..Default::default()
            }),
            &actor.token,
        ))
        .await
        .unwrap()
        .into_inner();
    tokio::pin!(object_stream);
    let mut loaded = Vec::new();
    while let Some(chunk) = object_stream.next().await {
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.unwrap().data
        {
            loaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(loaded, body);
}

#[tokio::test]
async fn internal_proxy_rejects_mismatched_original_principal() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "proxy-auth-bucket").await;
    let bucket_name = create_proxy_bucket(&actor, "proxy-auth-bucket").await;

    let original_claims = actor_claims(&actor, None);
    let internal_token = cluster.admin_token();

    let mut proxy_client = InternalProxyServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let get_header = ProxyRequestHeader {
        request_id: unique_test_name("proxy-bad-principal"),
        idempotency_key: "".to_string(),
        principal_id: "other-app".to_string(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: bucket_name.clone(),
        object_key: "missing.txt".to_string(),
        method: "GET".to_string(),
        canonical_host: canonical_proxy_host(&cluster, &actor, &bucket_name),
        canonical_path: "/missing.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![],
        authz_context: proxy_authz_context(&original_claims),
        native_object_metadata: None,
    };
    let err = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(iter(vec![ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(get_header)),
            }])),
            &internal_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn internal_proxy_rejects_magic_internal_principal_without_system_realm_authority() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "proxy-no-magic-bucket").await;
    let bucket_name = create_proxy_bucket(&actor, "proxy-no-magic-bucket").await;

    let original_claims = actor_claims(&actor, None);
    let unauthorised_internal_token = actor.token.clone();

    let mut proxy_client = InternalProxyServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let get_header = ProxyRequestHeader {
        request_id: unique_test_name("proxy-no-magic"),
        idempotency_key: "".to_string(),
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: bucket_name.clone(),
        object_key: "missing.txt".to_string(),
        method: "GET".to_string(),
        canonical_host: canonical_proxy_host(&cluster, &actor, &bucket_name),
        canonical_path: "/missing.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![],
        authz_context: proxy_authz_context(&original_claims),
        native_object_metadata: None,
    };
    let err = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(iter(vec![ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(get_header)),
            }])),
            &unauthorised_internal_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
