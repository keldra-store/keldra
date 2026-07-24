use super::bucket::*;
use super::guard::*;
use super::object::*;
use super::preconditions::*;
use super::routing::*;
use super::util::*;
use super::*;
use anvil_core::{
    mesh_directory::{
        self, BucketId, BucketLocatorDescriptor, BucketName, CellId, MeshControlWriteAuthority,
        MeshId, RegionName, RoutingRecordFamily, TenantId,
    },
    partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    },
};
use anvil_test_utils::personaldb_test_protocol_keyring;
use futures_util::TryStreamExt;
use tempfile::tempdir;

fn run_s3_gateway_async_test(future: impl std::future::Future<Output = ()> + Send + 'static) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build S3 gateway unit test runtime");
    runtime.block_on(async move {
        tokio::spawn(future)
            .await
            .expect("S3 gateway unit test task should not panic");
    });
}

fn request(uri: &str) -> Request {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

fn host_request(host: &str, remote: &str, forwarded_host: Option<&str>) -> Request {
    let mut builder = Request::builder().uri("/object.txt").header("host", host);
    if let Some(forwarded_host) = forwarded_host {
        builder = builder.header("x-forwarded-host", forwarded_host);
    }
    let mut req = builder.body(Body::empty()).unwrap();
    req.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        remote.parse().unwrap(),
        41_000,
    )));
    req
}

fn routing_config_with_trusted_ranges(ranges: &[&str]) -> anvil_core::config::Config {
    anvil_core::config::Config {
        trusted_proxy_source_ranges: ranges.iter().map(|range| range.to_string()).collect(),
        ..anvil_core::config::Config::default()
    }
}

fn routing_config_with_policy(
    storage_path: &std::path::Path,
    policy: CrossRegionRoutingPolicy,
) -> anvil_core::config::Config {
    anvil_core::config::Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        public_api_addr: "test-node".to_string(),
        api_listen_addr: "127.0.0.1:0".to_string(),
        region: "us-east-1".to_string(),
        storage_path: storage_path.to_string_lossy().to_string(),
        cross_region_routing_policy: policy,
        bootstrap_system_admin_subject_kind: "app".to_string(),
        bootstrap_system_admin_subject_id: "admin-principal".to_string(),
        ..anvil_core::config::Config::default()
    }
}

async fn seeded_remote_bucket_route(
    policy: CrossRegionRoutingPolicy,
) -> (tempfile::TempDir, AppState, Claims, ObjectRoute) {
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let state = AppState::new(
        routing_config_with_policy(&storage_path, policy),
        personaldb_test_protocol_keyring(),
    )
    .await
    .unwrap();
    let tenant = state
        .persistence
        .create_tenant("acme", "remote-bucket-test")
        .await
        .unwrap();
    state
        .persistence
        .create_bucket(tenant.id, "releases", "eu-west-1")
        .await
        .unwrap();
    let claims = Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        tenant_id: tenant.id,
        jti: None,
    };
    let route = ObjectRoute {
        tenant: "acme".to_string(),
        bucket: "releases".to_string(),
        region: "us-east-1".to_string(),
        key: "object.txt".to_string(),
        source: RouteSource::PathStyle,
    };
    (temp, state, claims, route)
}

async fn seed_active_proxy_node(state: &AppState, region: &str, endpoint: &str) {
    use anvil_core::mesh_lifecycle::{
        CreateRegionDescriptor, LifecycleState, NodeCapability, RegisterCellDescriptor,
        RegisterNodeDescriptor,
    };

    state
        .persistence
        .create_region_descriptor(CreateRegionDescriptor {
            mesh_id: "default".to_string(),
            region: region.to_string(),
            public_base_url: format!("https://{region}.anvil-storage.test"),
            virtual_host_suffix: format!("{region}.anvil-storage.test"),
            placement_weight: 100,
            default_cell: Some("default".to_string()),
        })
        .await
        .unwrap();
    state
        .persistence
        .register_cell_descriptor(RegisterCellDescriptor {
            mesh_id: "default".to_string(),
            region: region.to_string(),
            cell_id: "default".to_string(),
            placement_weight: 100,
            failure_domain: "rack-a".to_string(),
        })
        .await
        .unwrap();
    state
        .persistence
        .transition_cell_descriptor(region, "default", 1, LifecycleState::Active)
        .await
        .unwrap();
    state
        .persistence
        .register_node_descriptor(RegisterNodeDescriptor {
            mesh_id: "default".to_string(),
            node_id: "remote-object-node".to_string(),
            region: region.to_string(),
            cell_id: "default".to_string(),
            receipt_signing_public_key: anvil_core::node_signing::NodeSigningKeypair::generate()
                .unwrap()
                .public_key_bytes()
                .to_vec(),
            public_api_addr: endpoint.to_string(),
            capabilities: vec![NodeCapability::Object],
            capacity_json: "{}".to_string(),
        })
        .await
        .unwrap();
    state
        .persistence
        .transition_node_descriptor("remote-object-node", 1, LifecycleState::Active, None)
        .await
        .unwrap();
}

async fn seeded_remote_bucket_locator_only(
    policy: CrossRegionRoutingPolicy,
) -> (tempfile::TempDir, AppState, Claims, String) {
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let state = AppState::new(
        routing_config_with_policy(&storage_path, policy),
        personaldb_test_protocol_keyring(),
    )
    .await
    .unwrap();
    let tenant = state
        .persistence
        .create_tenant("acme", "remote-locator-only-test")
        .await
        .unwrap();
    let bucket_name = BucketName::canonicalize("releases").unwrap();
    let object_prefix = format!("objects/{}/{}/", tenant.id, bucket_name.as_str());
    let locator = BucketLocatorDescriptor::active(
        MeshId::new("default").unwrap(),
        TenantId::new(tenant.id.to_string()).unwrap(),
        bucket_name.clone(),
        BucketId::new("remote-bucket-id").unwrap(),
        RegionName::new("eu-west-1").unwrap(),
        CellId::new("default").unwrap(),
        "regional-primary",
        object_prefix,
        "2026-07-02T00:00:00Z",
    )
    .unwrap();
    let partition = locator.partition();
    let control_partition_id = mesh_directory::control_partition_id(
        RoutingRecordFamily::BucketLocator.stream_family(),
        &partition,
    );
    let signing_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
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

    let claims = Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        tenant_id: tenant.id,
        jti: None,
    };
    anvil_core::access_control::grant_storage_tenant_owner(
        &state.persistence,
        tenant.id,
        &claims.sub,
        "test",
        "s3 gateway remote locator seed",
    )
    .await
    .unwrap();
    (temp, state, claims, bucket_name.as_str().to_string())
}

async fn seeded_local_object_link() -> (tempfile::TempDir, AppState, Claims, String, String) {
    let temp = tempdir().unwrap();
    let storage_path = temp.path().join("storage");
    let state = AppState::new(
        routing_config_with_policy(&storage_path, CrossRegionRoutingPolicy::RedirectPreferred),
        personaldb_test_protocol_keyring(),
    )
    .await
    .unwrap();
    let tenant = state
        .persistence
        .create_tenant("acme", "local-link-test")
        .await
        .unwrap();
    let bucket = state
        .persistence
        .create_bucket(tenant.id, "releases", "us-east-1")
        .await
        .unwrap();
    let claims = Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        tenant_id: tenant.id,
        jti: None,
    };
    anvil_core::access_control::grant_storage_tenant_owner(
        &state.persistence,
        tenant.id,
        &claims.sub,
        "test",
        "s3 gateway link seed",
    )
    .await
    .unwrap();
    anvil_core::access_control::grant_bucket_defaults(
        &state.persistence,
        &bucket,
        &claims.sub,
        "test",
        "s3 gateway link seed",
    )
    .await
    .unwrap();
    state
        .object_manager
        .put_object(
            &claims,
            &bucket.name,
            "versions/app-v1.bin",
            tokio_stream::iter(vec![Ok(b"linked payload".to_vec())]),
            anvil_core::object_manager::ObjectWriteOptions {
                content_type: Some("application/octet-stream".to_string()),
                user_metadata: None,
                transaction_id: None,
                transaction_principal: None,
                storage_class_id: None,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    state
        .persistence
        .put_object_link(object_links::PutObjectLinkRequest {
            tenant_id: tenant.id,
            bucket_id: bucket.id,
            link_key: "latest.bin".to_string(),
            target_key: "versions/app-v1.bin".to_string(),
            target_version: None,
            resolution: object_links::ObjectLinkResolution::Follow,
            expected_generation: None,
            create_only: true,
            allow_dangling: false,
            idempotency_key: "local-link".to_string(),
            created_by: "principal:test".to_string(),
            transaction_id: None,
            transaction_principal: None,
        })
        .await
        .unwrap();
    (temp, state, claims, bucket.name, "latest.bin".to_string())
}

async fn response_xml(response: Response) -> String {
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    std::str::from_utf8(&body).unwrap().to_string()
}

async fn response_body(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap()
        .to_vec()
}

fn request_with_copy_source(uri: &str, copy_source: &str) -> Request {
    Request::builder()
        .uri(uri)
        .header("x-amz-copy-source", copy_source)
        .body(Body::empty())
        .unwrap()
}

fn range_headers(value: &str) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(axum::http::header::RANGE, value.parse().unwrap());
    headers
}

fn etag_headers(name: axum::http::header::HeaderName, value: &str) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(name, value.parse().unwrap());
    headers
}

fn x_amz_headers(name: &'static str, value: &str) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::HeaderName::from_static(name),
        value.parse().unwrap(),
    );
    headers
}

fn http_date_headers(
    name: axum::http::header::HeaderName,
    value: std::time::SystemTime,
) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(name, httpdate::fmt_http_date(value).parse().unwrap());
    headers
}

#[test]
fn s3_host_routing_accepts_forwarded_host_only_from_trusted_ranges() {
    let config = routing_config_with_trusted_ranges(&["127.0.0.1/32"]);
    let req = host_request(
        "internal.anvil-storage.test",
        "127.0.0.1",
        Some("Bucket.Default.Test-Region-1.Anvil-Storage.Test"),
    );

    let host = request_host(&req, &config).expect("effective host");

    assert_eq!(
        host.as_deref(),
        Some("bucket.default.test-region-1.anvil-storage.test")
    );
}

#[test]
fn s3_host_routing_ignores_untrusted_forwarded_host() {
    let config = routing_config_with_trusted_ranges(&["10.0.0.0/8"]);
    let req = host_request(
        "internal.anvil-storage.test",
        "127.0.0.1",
        Some("bucket.default.test-region-1.anvil-storage.test"),
    );

    let host = request_host(&req, &config).expect("effective host");

    assert_eq!(host.as_deref(), Some("internal.anvil-storage.test"));
}

#[test]
fn s3_host_routing_rejects_ambiguous_forwarded_host_chains() {
    let config = routing_config_with_trusted_ranges(&["127.0.0.1/32"]);
    let req = host_request(
        "internal.anvil-storage.test",
        "127.0.0.1",
        Some("one.example.test, two.example.test"),
    );

    let err = request_host(&req, &config).unwrap_err();

    assert_eq!(err, RoutingError::AmbiguousForwardedHost);
}

#[test]
fn s3_error_responses_include_request_id_in_header_and_xml() {
    run_s3_gateway_async_test(async move {
        let response = s3_error(
            "AccessDenied",
            "denied <unsafe>",
            axum::http::StatusCode::FORBIDDEN,
        );
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
        let request_id = response
            .headers()
            .get("x-amz-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(request_id.len(), 32);
        assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("<Message>denied &lt;unsafe&gt;</Message>"));
        assert!(xml.contains(&format!("<RequestId>{request_id}</RequestId>")));
    });
}

#[test]
fn s3_not_found_errors_do_not_leak_existence_to_unauthenticated_callers() {
    run_s3_gateway_async_test(async move {
        let unauthenticated = s3_status_to_response_for_auth(
            tonic::Status::not_found("missing protected object"),
            false,
            "NoSuchKey",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );
        assert_eq!(unauthenticated.status(), axum::http::StatusCode::FORBIDDEN);
        assert!(unauthenticated.headers().contains_key("x-amz-request-id"));
        let body = axum::body::to_bytes(unauthenticated.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(!xml.contains("NoSuchKey"));

        let authenticated = s3_status_to_response_for_auth(
            tonic::Status::not_found("missing visible object"),
            true,
            "NoSuchKey",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );
        assert_eq!(authenticated.status(), axum::http::StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(authenticated.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
    });
}

#[test]
fn remote_bucket_locator_local_only_rejects_cross_region_route() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::LocalOnly).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>InvalidRequest</Code>"));
        assert!(xml.contains("local_only"));
    });
}

#[test]
fn remote_bucket_locator_redirect_preferred_returns_s3_wrong_region_response() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::RedirectPreferred).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        assert!(response.headers().contains_key("x-amz-request-id"));
        assert!(!response.headers().contains_key("x-anvil-bucket-region"));
        assert!(
            !response
                .headers()
                .contains_key("x-anvil-cross-region-action")
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
        assert!(xml.contains("<RequestId>"));
    });
}

#[test]
fn remote_bucket_locator_proxy_preferred_redirects_when_proxy_is_absent() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyPreferred).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
    });
}

#[test]
fn remote_bucket_locator_proxy_required_reports_unavailable_without_proxy() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyRequired).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
        assert!(xml.contains("no eligible proxy target is available"));
    });
}

#[test]
fn remote_bucket_locator_proxy_required_selects_active_remote_object_node() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyRequired).await;
        seed_active_proxy_node(&state, "eu-west-1", "127.0.0.1:50091").await;

        let checked = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap();
        let target = checked
            .remote_bucket
            .expect("proxy_required with active object node must select proxy target");

        assert_eq!(target.region, "eu-west-1");
        assert_eq!(target.endpoint, "http://127.0.0.1:50091");
        assert!(target.bucket_locator_generation > 0);
    });
}

#[test]
fn remote_bucket_status_metadata_maps_to_s3_without_private_headers() {
    run_s3_gateway_async_test(async move {
        let mut status =
            tonic::Status::unavailable("Bucket is in region eu-west-1; proxy details hidden");
        status
            .metadata_mut()
            .insert("x-anvil-bucket-region", "eu-west-1".parse().unwrap());
        status.metadata_mut().insert(
            "x-anvil-cross-region-action",
            "proxy_unavailable".parse().unwrap(),
        );

        let response = s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            CrossRegionRoutingPolicy::ProxyRequired,
        );

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        assert!(!response.headers().contains_key("x-anvil-bucket-region"));
        assert!(
            !response
                .headers()
                .contains_key("x-anvil-cross-region-action")
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
        assert!(!xml.contains("proxy details hidden"));
    });
}

#[test]
fn remote_bucket_message_parser_strips_internal_suffix_for_redirects() {
    run_s3_gateway_async_test(async move {
        let status =
            tonic::Status::failed_precondition("Bucket is in region eu-west-1; redirect required");

        let response = s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
        assert!(!xml.contains("redirect required"));
    });
}

#[test]
fn head_bucket_uses_remote_locator_before_local_bucket_metadata() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, bucket) =
            seeded_remote_bucket_locator_only(CrossRegionRoutingPolicy::RedirectPreferred).await;
        let mut req = Request::builder()
            .uri(format!("/{bucket}"))
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(S3HostRoute(ObjectRoute {
            tenant: "acme".to_string(),
            bucket: bucket.clone(),
            region: "us-east-1".to_string(),
            key: String::new(),
            source: RouteSource::PathStyle,
        }));
        req.extensions_mut().insert(claims);

        let response = head_bucket(State(state), Path(bucket), req).await;

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
    });
}

#[test]
fn object_link_get_and_head_follow_by_default_with_link_headers() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, bucket, link_key) = seeded_local_object_link().await;
        let mut get_req = Request::builder()
            .uri(format!("/{bucket}/{link_key}"))
            .body(Body::empty())
            .unwrap();
        get_req.extensions_mut().insert(claims.clone());

        let get_response = get_object(
            State(state.clone()),
            Path((bucket.clone(), link_key.clone())),
            Query(HashMap::new()),
            get_req,
        )
        .await;

        assert_eq!(get_response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            get_response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        assert_eq!(
            get_response.headers().get("x-anvil-link-key").unwrap(),
            "latest.bin"
        );
        assert_eq!(
            get_response
                .headers()
                .get("x-anvil-link-generation")
                .unwrap(),
            "1"
        );
        assert!(
            get_response
                .headers()
                .get("ETag")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("link-follow-")
        );
        assert_eq!(response_body(get_response).await, b"linked payload");

        let mut head_req = Request::builder()
            .method(axum::http::Method::HEAD)
            .uri(format!("/{bucket}/{link_key}"))
            .body(Body::empty())
            .unwrap();
        head_req.extensions_mut().insert(claims);

        let head_response = head_object(
            State(state),
            Path((bucket, link_key)),
            Query(HashMap::new()),
            head_req,
        )
        .await;

        assert_eq!(head_response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            head_response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        assert_eq!(head_response.headers().get("Content-Length").unwrap(), "14");
        assert!(response_body(head_response).await.is_empty());
    });
}

#[test]
fn object_link_metadata_mode_returns_descriptor_json() {
    run_s3_gateway_async_test(async move {
        let (_temp, state, claims, bucket, link_key) = seeded_local_object_link().await;
        let mut req = Request::builder()
            .uri(format!("/{bucket}/{link_key}"))
            .header("x-anvil-link-mode", "metadata")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims);

        let response = get_object(
            State(state),
            Path((bucket, link_key)),
            Query(HashMap::new()),
            req,
        )
        .await;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            object_links::LINK_METADATA_CONTENT_TYPE
        );
        assert_eq!(
            response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        let descriptor: serde_json::Value =
            serde_json::from_slice(&response_body(response).await).unwrap();
        assert_eq!(descriptor["schema"], "anvil.object_link.v1");
        assert_eq!(descriptor["link_key"], "latest.bin");
        assert_eq!(descriptor["target_key"], "versions/app-v1.bin");
        assert_eq!(descriptor["resolution"], "follow");
    });
}

#[test]
fn reserved_namespace_guard_detects_object_keys() {
    assert!(request_targets_reserved_namespace(&request(
        "/bucket/_anvil/authz/tuples"
    )));

    let mut tenant_routed = request("/releases/_anvil/authz/tuples");
    tenant_routed
        .extensions_mut()
        .insert(S3HostRoute(ObjectRoute {
            tenant: "tenant".to_string(),
            bucket: "bucket".to_string(),
            region: "test-region-1".to_string(),
            key: "_anvil/authz/tuples".to_string(),
            source: RouteSource::PathStyle,
        }));
    assert!(request_targets_reserved_namespace(&tenant_routed));

    assert!(request_targets_reserved_namespace(&request(
        "/bucket/_anvil/personaldb/group"
    )));
    assert!(!request_targets_reserved_namespace(&request(
        "/bucket/customer/_anvil/authz/visible"
    )));
}

#[test]
fn reserved_namespace_guard_detects_native_routed_keys_before_auth() {
    run_s3_gateway_async_test(async move {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let mut config =
            routing_config_with_policy(&storage_path, CrossRegionRoutingPolicy::RedirectPreferred);
        config.public_region_base_domain = "us-east-1.anvil-storage.test".to_string();
        let state = AppState::new(config, personaldb_test_protocol_keyring())
            .await
            .unwrap();

        let reserved = Request::builder()
            .uri("/default/releases/_anvil/authz/tuples")
            .header("host", "us-east-1.anvil-storage.test")
            .body(Body::empty())
            .unwrap();
        assert!(request_targets_native_routed_reserved_namespace(
            &state, &reserved
        ));

        let visible = Request::builder()
            .uri("/default/releases/customer/_anvil/authz/visible")
            .header("host", "us-east-1.anvil-storage.test")
            .body(Body::empty())
            .unwrap();
        assert!(!request_targets_native_routed_reserved_namespace(
            &state, &visible
        ));
    });
}

#[test]
fn reserved_namespace_guard_detects_list_prefixes() {
    assert!(request_targets_reserved_namespace(&request(
        "/bucket?list-type=2&prefix=_anvil%2Fauthz%2F"
    )));
    assert!(request_targets_reserved_namespace(&request(
        "/bucket?prefix=_anvil/personaldb/"
    )));
    assert!(!request_targets_reserved_namespace(&request(
        "/bucket?prefix=customer%2F_anvil%2Fauthz%2F"
    )));
}

#[test]
fn reserved_namespace_guard_detects_copy_source_keys() {
    assert!(request_targets_reserved_namespace(
        &request_with_copy_source("/bucket/destination", "source/_anvil/authz/tuples")
    ));
    assert!(request_targets_reserved_namespace(
        &request_with_copy_source("/bucket/destination", "/source/_anvil%2Fauthz%2Ftuples")
    ));
    assert!(!request_targets_reserved_namespace(
        &request_with_copy_source(
            "/bucket/destination",
            "source/customer/_anvil/authz/visible"
        )
    ));
    assert!(!request_targets_reserved_namespace(
        &request_with_copy_source("/bucket/destination", "malformed-copy-source")
    ));
}

#[test]
fn range_parser_resolves_standard_and_suffix_ranges() {
    let standard = parse_http_range(&range_headers("bytes=2-5"), Some(10))
        .unwrap()
        .unwrap()
        .resolve(10)
        .unwrap();
    assert_eq!(standard, ByteRange { start: 2, end: 5 });

    let open_ended = parse_http_range(&range_headers("bytes=7-"), Some(10))
        .unwrap()
        .unwrap()
        .resolve(10)
        .unwrap();
    assert_eq!(open_ended, ByteRange { start: 7, end: 9 });

    let suffix = parse_http_range(&range_headers("bytes=-4"), Some(10))
        .unwrap()
        .unwrap()
        .resolve(10)
        .unwrap();
    assert_eq!(suffix, ByteRange { start: 6, end: 9 });
}

#[test]
fn range_parser_rejects_multi_ranges_and_unsatisfied_ranges() {
    assert!(parse_http_range(&range_headers("bytes=0-1,4-5"), Some(10)).is_err());
    assert!(
        parse_http_range(&range_headers("bytes=20-30"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .is_err()
    );
}

#[test]
fn invalid_range_error_includes_request_id_and_content_range() {
    run_s3_gateway_async_test(async move {
        let response = invalid_range_response(10);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok()),
            Some("bytes */10")
        );
        let request_id = response
            .headers()
            .get("x-amz-request-id")
            .expect("S3 invalid range errors must include request id")
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(request_id.len(), 32);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>InvalidRange</Code>"));
        assert!(xml.contains(&format!("<RequestId>{request_id}</RequestId>")));
    });
}

#[test]
fn etag_preconditions_match_strong_weak_and_list_values() {
    assert!(etag_condition_matches("\"abc\"", "abc"));
    assert!(etag_condition_matches("W/\"abc\"", "abc"));
    assert!(etag_condition_matches("\"nope\", \"abc\"", "abc"));
    assert!(etag_condition_matches("*", "abc"));
    assert!(!etag_condition_matches("\"nope\"", "abc"));
}

#[test]
fn etag_preconditions_return_s3_status_responses() {
    let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
    let failed = evaluate_object_preconditions(
        &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
        "abc",
        last_modified,
    )
    .expect("if-match mismatch should fail");
    assert_eq!(failed.status(), axum::http::StatusCode::PRECONDITION_FAILED);

    let not_modified = evaluate_object_preconditions(
        &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
        "abc",
        last_modified,
    )
    .expect("if-none-match match should return not modified");
    assert_eq!(not_modified.status(), axum::http::StatusCode::NOT_MODIFIED);

    assert!(
        evaluate_object_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"other\""),
            "abc",
            last_modified,
        )
        .is_none()
    );
}

#[test]
fn write_etag_preconditions_require_existing_match() {
    let failed_missing = evaluate_write_etag_preconditions(
        &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
        None,
    )
    .expect("If-Match without current object should fail");
    assert_eq!(
        failed_missing.status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );

    assert!(
        evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
            Some("abc"),
        )
        .is_none()
    );

    let failed_mismatch = evaluate_write_etag_preconditions(
        &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
        Some("abc"),
    )
    .expect("If-Match mismatch should fail");
    assert_eq!(
        failed_mismatch.status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
}

#[test]
fn write_etag_preconditions_enforce_if_none_match() {
    assert!(
        evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
            None,
        )
        .is_none()
    );

    let failed_existing = evaluate_write_etag_preconditions(
        &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
        Some("abc"),
    )
    .expect("matching If-None-Match should fail writes");
    assert_eq!(
        failed_existing.status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );

    let failed_star = evaluate_write_etag_preconditions(
        &etag_headers(axum::http::header::IF_NONE_MATCH, "*"),
        Some("abc"),
    )
    .expect("If-None-Match wildcard should fail existing object writes");
    assert_eq!(
        failed_star.status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
}

#[test]
fn copy_source_preconditions_return_precondition_failed() {
    let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let before = exact_second - std::time::Duration::from_secs(1);
    let after = exact_second + std::time::Duration::from_secs(1);

    assert!(
        evaluate_copy_source_preconditions(
            &x_amz_headers("x-amz-copy-source-if-match", "\"abc\""),
            "abc",
            last_modified,
        )
        .is_none()
    );
    assert_eq!(
        evaluate_copy_source_preconditions(
            &x_amz_headers("x-amz-copy-source-if-match", "\"other\""),
            "abc",
            last_modified,
        )
        .expect("source If-Match mismatch should fail")
        .status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        evaluate_copy_source_preconditions(
            &x_amz_headers("x-amz-copy-source-if-none-match", "\"abc\""),
            "abc",
            last_modified,
        )
        .expect("source If-None-Match hit should fail")
        .status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        evaluate_copy_source_preconditions(
            &x_amz_headers(
                "x-amz-copy-source-if-unmodified-since",
                &httpdate::fmt_http_date(before),
            ),
            "abc",
            last_modified,
        )
        .expect("source If-Unmodified-Since before modification should fail")
        .status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
    assert_eq!(
        evaluate_copy_source_preconditions(
            &x_amz_headers(
                "x-amz-copy-source-if-modified-since",
                &httpdate::fmt_http_date(after),
            ),
            "abc",
            last_modified,
        )
        .expect("source If-Modified-Since after modification should fail")
        .status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );
}

#[test]
fn date_preconditions_compare_against_second_precision_last_modified() {
    let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 999_000_000).unwrap();
    let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    let before = exact_second - std::time::Duration::from_secs(1);
    let after = exact_second + std::time::Duration::from_secs(1);

    let unmodified_since_before = evaluate_object_preconditions(
        &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, before),
        "abc",
        last_modified,
    )
    .expect("older if-unmodified-since should fail");
    assert_eq!(
        unmodified_since_before.status(),
        axum::http::StatusCode::PRECONDITION_FAILED
    );

    assert!(
        evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, exact_second),
            "abc",
            last_modified,
        )
        .is_none()
    );

    let modified_since_exact = evaluate_object_preconditions(
        &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, exact_second),
        "abc",
        last_modified,
    )
    .expect("equal if-modified-since should be not modified");
    assert_eq!(
        modified_since_exact.status(),
        axum::http::StatusCode::NOT_MODIFIED
    );

    assert!(
        evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, before),
            "abc",
            last_modified,
        )
        .is_none()
    );
    assert_eq!(
        evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, after),
            "abc",
            last_modified,
        )
        .expect("future if-modified-since should be not modified")
        .status(),
        axum::http::StatusCode::NOT_MODIFIED
    );
}

#[test]
fn range_stream_slices_across_chunk_boundaries() {
    run_s3_gateway_async_test(async move {
        let stream = Box::pin(futures_util::stream::iter(vec![
            Ok(b"abc".to_vec()),
            Ok(b"defg".to_vec()),
            Ok(b"hij".to_vec()),
        ]));
        let body = slice_stream_by_range(stream, ByteRange { start: 2, end: 7 })
            .try_concat()
            .await
            .unwrap();
        assert_eq!(body, b"cdefgh");
    });
}

#[test]
fn copy_source_parser_accepts_encoded_bucket_key_and_version() {
    let (bucket, key, version_id) = parse_copy_source(
        "/source-bucket/path%20with%20space/file.txt?versionId=550e8400-e29b-41d4-a716-446655440000",
    )
    .unwrap();
    assert_eq!(bucket, "source-bucket");
    assert_eq!(key, "path with space/file.txt");
    assert_eq!(
        version_id.unwrap(),
        uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    );
}

#[test]
fn copy_source_parser_rejects_missing_key() {
    assert!(parse_copy_source("/source-bucket").is_err());
}
