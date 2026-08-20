use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::Output;
use std::time::{Duration, Instant};

use anvil::authentication::{JwtManager, RateLimitConfig};
use anvil::{ServerConfig, serve};
use anvil_api::v1::administration_service_client::AdministrationServiceClient;
use anvil_api::v1::{
    CreateBucketRequest, ObjectVersioning, ProvisionTenantRequest, SetBucketPublicReadRequest,
};
use anvil_store::{ErasureProfile, StorageTenantId, Store, StoreOptions, SystemBootstrapRequest};
use tempfile::TempDir;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

const SIGNING_KEY: &[u8] = b"anvil-git-gateway-qualification-signing-key";
const BOOTSTRAP_SECRET: &str = "bootstrap-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const OWNER_SECRET: &str = "owner-secret-0123456789abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn real_git_client_pushes_pulls_and_clones_a_public_repository() {
    if CommandAvailability::git().await.is_none() {
        eprintln!("skipping Git gateway qualification because git is not installed");
        return;
    }
    let fixture = Fixture::start().await;
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    git_ok(["init", "--initial-branch=master", path(&source)]).await;
    git_ok(["-C", path(&source), "config", "user.name", "Anvil Test"]).await;
    git_ok([
        "-C",
        path(&source),
        "config",
        "user.email",
        "anvil@example.invalid",
    ])
    .await;
    tokio::fs::write(source.join("README.md"), b"hello from Anvil Git\n")
        .await
        .unwrap();
    git_ok(["-C", path(&source), "add", "README.md"]).await;
    git_ok(["-C", path(&source), "commit", "-m", "initial"]).await;

    let authenticated = format!(
        "http://owner-client:{OWNER_SECRET}@{}/git/acme/repositories/demo.git",
        fixture.gateway
    );
    git_ok([
        "-C",
        path(&source),
        "remote",
        "add",
        "origin",
        &authenticated,
    ])
    .await;
    git_ok(["-C", path(&source), "push", "origin", "master"]).await;

    let authenticated_clone = work.path().join("authenticated-clone");
    git_ok([
        "clone",
        "--branch",
        "master",
        &authenticated,
        path(&authenticated_clone),
    ])
    .await;
    assert_eq!(
        tokio::fs::read(authenticated_clone.join("README.md"))
            .await
            .unwrap(),
        b"hello from Anvil Git\n"
    );

    let public = format!("http://{}/git/acme/repositories/demo.git", fixture.gateway);
    let denied_clone = work.path().join("denied-clone");
    git_fails(["clone", "--branch", "master", &public, path(&denied_clone)]).await;

    fixture.enable_public_read().await;
    let public_clone = work.path().join("public-clone");
    git_ok(["clone", "--branch", "master", &public, path(&public_clone)]).await;
    assert_eq!(
        tokio::fs::read(public_clone.join("README.md"))
            .await
            .unwrap(),
        b"hello from Anvil Git\n"
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
        retry_provision(&mut administration, &bootstrap).await;
        let owner = tokens
            .mint(StorageTenantId::parse("acme").unwrap(), "owner-app")
            .unwrap();
        retry_bucket(&mut administration, &owner).await;
        wait_for_http(listen).await;
        Self {
            _directory: directory,
            grpc,
            gateway: listen,
            tokens,
            server,
        }
    }

    async fn enable_public_read(&self) {
        let owner = self
            .tokens
            .mint(StorageTenantId::parse("acme").unwrap(), "owner-app")
            .unwrap();
        AdministrationServiceClient::new(self.grpc.clone())
            .set_bucket_public_read(authorized(
                SetBucketPublicReadRequest {
                    bucket: "repositories".into(),
                    enabled: true,
                },
                &owner,
            ))
            .await
            .expect("owner enables public repository reads");
    }

    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

async fn retry_provision(administration: &mut AdministrationServiceClient<Channel>, token: &str) {
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
                token,
            ))
            .await;
        match result {
            Ok(_) => return,
            Err(error)
                if Instant::now() < deadline
                    && matches!(
                        error.code(),
                        tonic::Code::Unavailable | tonic::Code::FailedPrecondition
                    ) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("provision Git test tenant: {error}"),
        }
    }
}

async fn retry_bucket(administration: &mut AdministrationServiceClient<Channel>, token: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let result = administration
            .create_bucket(authorized(
                CreateBucketRequest {
                    bucket: "repositories".into(),
                    versioning: ObjectVersioning::Unversioned as i32,
                },
                token,
            ))
            .await;
        match result {
            Ok(_) => return,
            Err(error)
                if Instant::now() < deadline
                    && matches!(
                        error.code(),
                        tonic::Code::Unavailable | tonic::Code::FailedPrecondition
                    ) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("create Git test bucket: {error}"),
        }
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
        plugin_gateway: anvil::PluginGatewayConfig::default(),
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

fn authorized<T>(value: T, token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
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

struct CommandAvailability;

impl CommandAvailability {
    async fn git() -> Option<()> {
        tokio::process::Command::new("git")
            .arg("--version")
            .output()
            .await
            .ok()
            .filter(|output| output.status.success())
            .map(|_| ())
    }
}

async fn git_ok<const N: usize>(arguments: [&str; N]) -> Output {
    let output = tokio::process::Command::new("git")
        .args(arguments)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "git failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

async fn git_fails<const N: usize>(arguments: [&str; N]) -> Output {
    let output = tokio::process::Command::new("git")
        .args(arguments)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await
        .unwrap();
    assert!(
        !output.status.success(),
        "private Git operation unexpectedly succeeded:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn path(value: &Path) -> &str {
    value.to_str().expect("temporary test path is UTF-8")
}
