use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use anvil::authentication::{JwtManager, RateLimitConfig};
use anvil::{ServerConfig, serve};
use anvil_api::v1::administration_service_client::AdministrationServiceClient;
use anvil_api::v1::{ProvisionTenantRequest, SetBucketPublicReadRequest};
use anvil_store::{ErasureProfile, StorageTenantId, Store, StoreOptions, SystemBootstrapRequest};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

const SIGNING_KEY: &[u8] = b"anvil-s3-gateway-qualification-signing-key";
const BOOTSTRAP_SECRET: &str = "bootstrap-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const OWNER_SECRET: &str = "owner-secret-0123456789abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn official_client_exercises_minimum_s3_surface_and_public_read() {
    let fixture = Fixture::start().await;
    let client = fixture.s3_client();

    client
        .create_bucket()
        .bucket("objects")
        .send()
        .await
        .expect("official S3 client creates a bucket");
    client
        .put_object()
        .bucket("objects")
        .key("docs/first.txt")
        .content_type("text/plain")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await
        .expect("official S3 client uploads an object");
    client
        .put_object()
        .bucket("objects")
        .key("docs/second.txt")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await
        .expect("official S3 client uploads a second object");
    client
        .put_object()
        .bucket("objects")
        .key("wire/grpc-payload.bin")
        .content_type("application/grpc")
        .body(ByteStream::from_static(b"opaque grpc media bytes"))
        .send()
        .await
        .expect("S3 content type does not select the gRPC router");

    let grpc_media = client
        .get_object()
        .bucket("objects")
        .key("wire/grpc-payload.bin")
        .send()
        .await
        .expect("official S3 client reads the gRPC-media object");
    assert_eq!(grpc_media.content_type(), Some("application/grpc"));
    assert_eq!(
        grpc_media
            .body
            .collect()
            .await
            .expect("gRPC-media object body streams")
            .into_bytes()
            .as_ref(),
        b"opaque grpc media bytes"
    );

    let head = client
        .head_object()
        .bucket("objects")
        .key("docs/first.txt")
        .send()
        .await
        .expect("official S3 client heads an object");
    assert_eq!(head.content_length(), Some(5));
    assert_eq!(head.content_type(), Some("text/plain"));

    let downloaded = client
        .get_object()
        .bucket("objects")
        .key("docs/first.txt")
        .send()
        .await
        .expect("official S3 client gets an object")
        .body
        .collect()
        .await
        .expect("object body streams")
        .into_bytes();
    assert_eq!(downloaded.as_ref(), b"first");

    let first_page = client
        .list_objects_v2()
        .bucket("objects")
        .prefix("docs/")
        .max_keys(1)
        .send()
        .await
        .expect("official S3 client lists the first page");
    assert!(first_page.is_truncated().unwrap_or_default());
    assert_eq!(first_page.contents().len(), 1);
    let second_page = client
        .list_objects_v2()
        .bucket("objects")
        .prefix("docs/")
        .max_keys(1)
        .continuation_token(
            first_page
                .next_continuation_token()
                .expect("truncated page has a continuation token"),
        )
        .send()
        .await
        .expect("official S3 client lists the second page");
    assert_eq!(second_page.contents().len(), 1);

    fixture.enable_public_read("objects").await;
    let response = raw_get(fixture.gateway, "/acme/objects/docs/first.txt").await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.ends_with("\r\n\r\nfirst"), "{response}");

    client
        .delete_object()
        .bucket("objects")
        .key("docs/first.txt")
        .send()
        .await
        .expect("official S3 client deletes an object");
    assert!(
        client
            .head_object()
            .bucket("objects")
            .key("docs/first.txt")
            .send()
            .await
            .is_err()
    );

    fixture.stop().await;
}

struct Fixture {
    _directory: TempDir,
    grpc: Channel,
    gateway: SocketAddr,
    tokens: JwtManager,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Fixture {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        seed_system(&directory).await;
        let tokens = JwtManager::new(SIGNING_KEY).unwrap();
        let listen = unused_loopback_address();
        let peer = distinct_address(&[listen]);
        let server = tokio::spawn(serve(test_server_config(
            &directory,
            listen,
            peer,
            tokens.clone(),
        )));
        let grpc = connect_when_ready(listen).await;
        let bootstrap = tokens
            .mint(StorageTenantId::system(), "bootstrap-app")
            .unwrap();
        let mut administration = AdministrationServiceClient::new(grpc.clone());
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let result = administration
                .provision_tenant(authorized(
                    ProvisionTenantRequest {
                        storage_tenant: "acme".into(),
                        owner_app_id: "owner-app".into(),
                        owner_client_id: "owner-client".into(),
                        owner_client_secret: OWNER_SECRET.into(),
                    },
                    &bootstrap,
                ))
                .await;
            match result {
                Ok(_) => break,
                Err(error)
                    if Instant::now() < deadline
                        && matches!(
                            error.code(),
                            tonic::Code::Unavailable | tonic::Code::FailedPrecondition
                        ) =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => panic!("provision tenant through distributed control: {error}"),
            }
        }
        wait_for_http(listen).await;
        Self {
            _directory: directory,
            grpc,
            gateway: listen,
            tokens,
            server,
        }
    }

    fn s3_client(&self) -> Client {
        let credentials = aws_sdk_s3::config::Credentials::new(
            "owner-client",
            OWNER_SECRET,
            None,
            None,
            "anvil-test",
        );
        let configuration = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::config::Region::new("eu-west-2"))
            .endpoint_url(format!("http://{}", self.gateway))
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Client::from_conf(configuration)
    }

    async fn enable_public_read(&self, bucket: &str) {
        let owner = self
            .tokens
            .mint(StorageTenantId::parse("acme").unwrap(), "owner-app")
            .unwrap();
        AdministrationServiceClient::new(self.grpc.clone())
            .set_bucket_public_read(authorized(
                SetBucketPublicReadRequest {
                    bucket: bucket.into(),
                    enabled: true,
                },
                &owner,
            ))
            .await
            .expect("owner enables public reads");
    }

    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

async fn seed_system(directory: &TempDir) {
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: BOOTSTRAP_SECRET.into(),
        })
        .unwrap();
}

fn test_server_config(
    directory: &TempDir,
    listen: SocketAddr,
    peer_listen: SocketAddr,
    token_manager: JwtManager,
) -> ServerConfig {
    ServerConfig {
        listen,
        peer_listen,
        peer_advertise: None,
        join_bundle: None,
        storage: anvil::StoragePaths::under(directory.path(), 8 * 1024 * 1024),
        explicit_authoritative_paths: anvil::ExplicitAuthoritativePaths::default(),
        run_system_bootstrap: true,
        system_bootstrap_credential_output: None,
        node_id: 1,
        max_atomic_commit_entries: 128,
        max_atomic_commit_bytes: 1024 * 1024,
        atomic_program_timeout: Duration::from_secs(30),
        index_query_timeout: Duration::from_secs(300),
        token_manager,
        rate_limits: RateLimitConfig::default(),
        index_runtime: anvil::IndexRuntimeConfig::default(),
        max_blob_bytes: 8 * 1024 * 1024,
        erasure_profile: ErasureProfile::default(),
        awaiting_publish_ttl_seconds: anvil_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        mutation_receipt_retention_seconds: 60,
        max_mutation_receipt_entries: 512,
        max_mutation_receipt_bytes: 1024 * 1024,
        source_journal_max_entries: 512,
        source_journal_max_bytes: 1024 * 1024,
    }
}

fn unused_loopback_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn distinct_address(existing: &[SocketAddr]) -> SocketAddr {
    loop {
        let candidate = unused_loopback_address();
        if !existing.contains(&candidate) {
            return candidate;
        }
    }
}

async fn connect_when_ready(listen: SocketAddr) -> Channel {
    let endpoint = Endpoint::from_shared(format!("http://{listen}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return channel,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Anvil test server did not start: {error}"),
        }
    }
}

async fn wait_for_http(listen: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match tokio::net::TcpStream::connect(listen).await {
            Ok(_) => return,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Anvil HTTP gateway did not start: {error}"),
        }
    }
}

fn authorized<T>(value: T, token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn raw_get(address: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}
