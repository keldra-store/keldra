#![recursion_limit = "512"]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::time::{Duration, Instant};

use anvil::anvil_api::GetAccessTokenRequest;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::mesh_control_service_client::MeshControlServiceClient;
use anvil_core::{AppState, access_control};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tracing_subscriber::{self, EnvFilter};

mod coremeta_bootstrap;
mod docker_cluster_control;
mod docker_cluster_startup;
mod docker_image;
mod docker_observation;
mod docker_process;
mod docker_response_fault;
mod docker_topology;
pub mod mvcc_cluster;
pub mod mvcc_process_cluster;

use coremeta_bootstrap::*;
pub use docker_cluster_control::{DockerNetworkPartition, DockerPeer};
pub use docker_cluster_startup::{
    isolated_docker_test_cluster, isolated_docker_test_cluster_with_deferred_peer,
};
use docker_image::require_docker_image;
pub use docker_observation::DockerObjectObservation;
use docker_process::*;
pub use docker_response_fault::GrpcLostResponseProxy;
use docker_topology::{ensure_docker_topology, prepare_docker_topology_with_deferred_peer};

static INIT_LOGGER: Once = Once::new();
static TEST_CLUSTER_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
static SHARED_TEST_THROTTLE: OnceLock<Option<Arc<Semaphore>>> = OnceLock::new();
static SHARED_DEFAULT_CLUSTER: OnceLock<Arc<TestCluster>> = OnceLock::new();
static SHARED_PUBLIC_REGION_CLUSTER: OnceLock<Arc<TestCluster>> = OnceLock::new();
static SHARED_DOCKER_CLUSTER: OnceLock<Arc<DockerTestCluster>> = OnceLock::new();

const DEFAULT_TEST_REGION: &str = "test-region-1";
pub const ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const TEST_CLUSTER_RAFT_VOTER_COUNT: usize = 3;
const SHARED_CLUSTER_REGIONS: [&str; 6] = [
    DEFAULT_TEST_REGION,
    DEFAULT_TEST_REGION,
    DEFAULT_TEST_REGION,
    DEFAULT_TEST_REGION,
    DEFAULT_TEST_REGION,
    DEFAULT_TEST_REGION,
];

pub fn personaldb_test_protocol_keyring()
-> anvil_core::personaldb_signing::PersonalDbProtocolKeyring {
    use anvil_core::personaldb_signing::PersonalDbProtocolKeyring;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use personaldb_protocol::{
        Ed25519ProtocolSigner, Ed25519PublicKey, KeyGeneration, KeyTrustPolicy, ProtocolSigner,
        PublicKeyTrustRecord, PublicKeyTrustStore, SignaturePurpose,
    };

    let signer = |private_key_b64: &str,
                  public_key_b64u: &str,
                  purpose: SignaturePurpose|
     -> Arc<dyn ProtocolSigner> {
        let private_key = STANDARD.decode(private_key_b64).unwrap();
        let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(public_key_b64u)
            .unwrap();
        let public_key = Ed25519PublicKey::try_from(public_key.as_slice()).unwrap();
        let policy = KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0);
        let record = PublicKeyTrustRecord::new(public_key, policy);
        Arc::new(
            Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(&private_key, record).unwrap(),
        )
    };
    let signers = [
        signer(
            "MC4CAQAwBQYDK2VwBCIEIBERERERERERERERERERERERERERERERERERERERERER",
            "0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc",
            SignaturePurpose::GroupControl,
        ),
        signer(
            "MC4CAQAwBQYDK2VwBCIEICIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi",
            "oJql9HpnWYAv-VX43C0qFKXJnSO-l_hkEn_5ODRVpPA",
            SignaturePurpose::Snapshot,
        ),
        signer(
            "MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz",
            "F8t5-ytBIPKx7GXkGY1uCLKOgT_rAeSkAIObheGAgM4",
            SignaturePurpose::Witness,
        ),
    ];
    let trust_store = PublicKeyTrustStore::from_records(
        signers.iter().map(|signer| signer.trust_record().clone()),
    )
    .unwrap();
    PersonalDbProtocolKeyring::new_test_only(trust_store, signers).unwrap()
}

async fn connect_docker_admin(addr: &str) -> AdminServiceClient<Channel> {
    let mut last_error = None;
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut attempt = 1_u64;
    while Instant::now() < deadline {
        match AdminServiceClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis((100 * attempt).min(1_000))).await;
                attempt += 1;
            }
        }
    }
    panic!("connect Docker admin endpoint {addr}: {last_error:?}");
}

async fn wait_for_docker_admin_ready(addr: &str, token: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let attempt = tokio::time::timeout(Duration::from_secs(2), async {
            let Ok(mut client) = AdminServiceClient::connect(addr.to_string()).await else {
                return false;
            };
            let mut request =
                tonic::Request::new(anvil::anvil_api::GetLocalNodeDescriptorRequest {});
            add_docker_admin_bearer(&mut request, token);
            client.get_local_node_descriptor(request).await.is_ok()
        })
        .await;
        if matches!(attempt, Ok(true)) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

fn dump_docker_cluster_diagnostics(
    compose_file: &std::path::Path,
    project_name: &str,
    compose_env: &[(String, String)],
) {
    for (label, args) in [
        ("ps", vec!["ps", "-a"]),
        ("logs", vec!["logs", "--tail=160"]),
    ] {
        let output = docker_compose_output_with_env(compose_file, project_name, &args, compose_env);
        eprintln!(
            "[anvil-test] docker compose {label}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn test_cluster_limit() -> usize {
    std::env::var("ANVIL_TEST_CLUSTER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

async fn acquire_test_cluster_permit() -> OwnedSemaphorePermit {
    TEST_CLUSTER_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(test_cluster_limit())))
        .clone()
        .acquire_owned()
        .await
        .expect("test cluster semaphore is not closed")
}

fn shared_test_limit() -> Option<usize> {
    std::env::var("ANVIL_SHARED_TEST_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

async fn acquire_shared_test_throttle() -> Option<OwnedSemaphorePermit> {
    let semaphore = SHARED_TEST_THROTTLE
        .get_or_init(|| shared_test_limit().map(|limit| Arc::new(Semaphore::new(limit))))
        .clone()?;
    Some(
        semaphore
            .acquire_owned()
            .await
            .expect("shared test throttle semaphore is not closed"),
    )
}

pub struct SharedTestCluster {
    cluster: Arc<TestCluster>,
    _debug_throttle: Option<OwnedSemaphorePermit>,
}

impl std::ops::Deref for SharedTestCluster {
    type Target = TestCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

pub struct SharedDockerTestCluster {
    cluster: Arc<DockerTestCluster>,
    _debug_throttle: Option<OwnedSemaphorePermit>,
}

impl std::ops::Deref for SharedDockerTestCluster {
    type Target = DockerTestCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

/// Shared Docker-backed integration cluster for public API, gateway and CLI
/// tests. Test binaries are clients only; the Anvil nodes run as real services
/// inside Docker Compose and are reused through a filesystem startup lock.
pub async fn shared_docker_test_cluster() -> SharedDockerTestCluster {
    let debug_throttle = acquire_shared_test_throttle().await;
    let cluster = tokio::task::spawn_blocking(|| {
        SHARED_DOCKER_CLUSTER
            .get_or_init(|| DockerTestCluster::start_shared())
            .clone()
    })
    .await
    .expect("shared Docker Anvil test cluster initialization panicked");
    SharedDockerTestCluster {
        cluster,
        _debug_throttle: debug_throttle,
    }
}

/// Shared default integration cluster for tests that only need normal Anvil
/// behaviour. This is intentionally long-lived for the whole test binary so
/// individual tests do not pay cluster bootstrap cost.
/// It uses six nodes because the current erasure profile requires six active
/// object nodes for distributed logical-file writes.
///
/// Tests using this helper must create unique tenants/apps/buckets/object keys
/// and must not assert global counts or global ordering. Shared-cluster tests run
/// concurrently by default. ANVIL_SHARED_TEST_CONCURRENCY is a local debugging
/// throttle for resource pressure; it must not be required for correctness.
pub async fn shared_default_test_cluster() -> SharedTestCluster {
    let debug_throttle = acquire_shared_test_throttle().await;
    let cluster = tokio::task::spawn_blocking(|| {
        SHARED_DEFAULT_CLUSTER
            .get_or_init(|| start_shared_cluster_thread(SharedClusterProfile::Default))
            .clone()
    })
    .await
    .expect("shared default Anvil test cluster initialization panicked");
    SharedTestCluster {
        cluster,
        _debug_throttle: debug_throttle,
    }
}

/// Shared cluster for S3/public-host tests. Use this instead of a custom
/// per-test config when the only required config difference is regional/public
/// host routing support. It follows the same concurrent-by-default rule as the
/// default shared cluster.
pub async fn shared_public_region_test_cluster() -> SharedTestCluster {
    let debug_throttle = acquire_shared_test_throttle().await;
    let cluster = tokio::task::spawn_blocking(|| {
        SHARED_PUBLIC_REGION_CLUSTER
            .get_or_init(|| start_shared_cluster_thread(SharedClusterProfile::PublicRegion))
            .clone()
    })
    .await
    .expect("shared public-region Anvil test cluster initialization panicked");
    SharedTestCluster {
        cluster,
        _debug_throttle: debug_throttle,
    }
}

/// Create a fresh cluster only when a test has a real isolation requirement:
/// restart, destructive topology mutation, custom storage/worker config, or
/// assertions over exact global state.
pub async fn isolated_test_cluster(reason: &str, regions: &[&str]) -> TestCluster {
    assert!(
        !reason.trim().is_empty(),
        "isolated_test_cluster requires a justification"
    );
    eprintln!("[anvil-test] isolated cluster: {reason}");
    TestCluster::new(regions).await
}

/// Create a fresh custom cluster. Prefer a shared profile unless the test needs
/// the config difference being supplied here.
pub async fn isolated_test_cluster_with_config(
    reason: &str,
    regions: &[&str],
    configure: impl FnOnce(&mut anvil_core::config::Config),
) -> TestCluster {
    assert!(
        !reason.trim().is_empty(),
        "isolated_test_cluster_with_config requires a justification"
    );
    eprintln!("[anvil-test] isolated cluster: {reason}");
    TestCluster::new_with_config(regions, configure).await
}

/// Generate per-test resource names for tests sharing a cluster.
pub fn unique_test_name(prefix: &str) -> String {
    format!("{}-{}", prefix, uuid::Uuid::new_v4().simple())
}

/// Identity and routing context for one concurrent test actor.
///
/// Use this for shared-cluster tests instead of the cluster's default app when
/// the test creates buckets or objects. Each actor owns a unique storage tenant
/// and app, so concurrent tests cannot observe each other's bucket/object state.
#[derive(Debug, Clone)]
pub struct TestStorageActor {
    pub tenant_id: i64,
    pub app_id: String,
    pub token: String,
    pub grpc_addr: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct DockerTestStorageActor {
    pub tenant_id: i64,
    pub tenant_name: Option<String>,
    pub app_id: String,
    pub app_name: String,
    pub client_id: String,
    pub client_secret: String,
    pub token: String,
    pub grpc_addr: String,
    pub region: String,
}

#[derive(Debug)]
pub struct DockerTestCluster {
    pub project_name: String,
    pub compose_file: PathBuf,
    pub grpc_addrs: Vec<String>,
    pub admin_addrs: Vec<String>,
    pub region: String,
    pub public_region_host: String,
    admin_token: String,
    compose_env: Vec<(String, String)>,
    deferred_topologies:
        Mutex<std::collections::BTreeMap<u8, anvil::anvil_api::BootstrapMeshTopologyRequest>>,
    cleanup_on_drop: bool,
    _cluster_permit: Option<OwnedSemaphorePermit>,
}

mod docker_test_cluster_impl;
impl Drop for DockerTestCluster {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let failed = std::thread::panicking();
            if failed {
                dump_docker_cluster_diagnostics(
                    &self.compose_file,
                    &self.project_name,
                    &self.compose_env,
                );
            }
            if failed && std::env::var_os("ANVIL_TEST_PRESERVE_FAILED_DOCKER_CLUSTER").is_some() {
                eprintln!(
                    "[anvil-test] preserving failed Docker project {}",
                    self.project_name
                );
                return;
            }
            docker_compose_with_env(
                &self.compose_file,
                &self.project_name,
                &["down", "-v", "--remove-orphans"],
                &self.compose_env,
            );
        }
    }
}

pub async fn create_docker_storage_test_actor(
    cluster: &DockerTestCluster,
    label: &str,
) -> DockerTestStorageActor {
    cluster.create_storage_actor(label).await
}

impl From<DockerTestStorageActor> for TestStorageActor {
    fn from(actor: DockerTestStorageActor) -> Self {
        Self {
            tenant_id: actor.tenant_id,
            app_id: actor.app_id,
            token: actor.token,
            grpc_addr: actor.grpc_addr,
            region: actor.region,
        }
    }
}

const DOCKER_AUTHZ_BOOTSTRAP_ACTIONS: &[&str] = &[
    "authz:tuple_write",
    "authz:tuple_read",
    "authz:check",
    "authz:watch",
    "authz:schema_read",
    "authz:schema_write",
];

/// Select the public gRPC endpoint for ordinary shared-cluster tests.
///
/// Admin-created credentials are anchored through the first node in the
/// in-process harness. Use that same public endpoint for ordinary API tests so
/// they do not depend on cross-node control projection timing. Dedicated
/// distributed tests should address individual nodes explicitly.
pub fn grpc_addr_for_test(cluster: &TestCluster, _label: &str) -> String {
    assert!(
        !cluster.grpc_addrs.is_empty(),
        "test cluster must be started before selecting a gRPC address"
    );
    cluster.grpc_addrs[0].clone()
}

/// Create a unique tenant/app/token for a shared-cluster test.
pub async fn create_storage_test_actor(cluster: &TestCluster, label: &str) -> TestStorageActor {
    let label = compact_resource_label(label, 18);
    let tenant_name = unique_test_name(&format!("{label}-tenant"));
    let tenant_id = create_test_tenant(cluster, &tenant_name).await;
    let app_name = unique_test_name(&format!("{label}-app"));
    let (app_id, client_id, client_secret) = cluster
        .create_application_with_storage_tenant_owner_id(&tenant_id.to_string(), &app_name)
        .await;
    let grpc_addr = grpc_addr_for_test(cluster, &label);
    let token = get_access_token_for_test(&grpc_addr, &client_id, &client_secret).await;

    TestStorageActor {
        tenant_id,
        app_id,
        token,
        grpc_addr,
        region: DEFAULT_TEST_REGION.to_string(),
    }
}

fn compact_resource_label(label: &str, max_len: usize) -> String {
    let mut compact = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .filter(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || *ch == '-')
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    compact.truncate(max_len);
    while compact.ends_with('-') {
        compact.pop();
    }
    if compact.is_empty() {
        "test".to_string()
    } else {
        compact
    }
}

pub async fn create_test_tenant(cluster: &TestCluster, tenant_name: &str) -> i64 {
    let mut admin = AdminServiceClient::connect(cluster.admin_addrs[0].clone())
        .await
        .unwrap();
    let mut request = tonic::Request::new(anvil::anvil_api::CreateTenantRequest {
        context: Some(anvil::anvil_api::AdminRequestContext {
            request_id: format!("create-tenant-{tenant_name}-{}", uuid::Uuid::new_v4()),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            audit_reason: format!("test create tenant {tenant_name}"),
            expected_generation: 0,
        }),
        name: tenant_name.to_string(),
        home_region: DEFAULT_TEST_REGION.to_string(),
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.admin_token()).parse().unwrap(),
    );

    admin
        .create_tenant(request)
        .await
        .unwrap()
        .into_inner()
        .tenant
        .expect("created tenant")
        .tenant_id
        .parse::<i64>()
        .expect("tenant id should be numeric")
}

pub async fn get_access_token_for_test(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
) -> String {
    let mut last_error = None;
    for attempt in 0..120 {
        match AuthServiceClient::connect(grpc_addr.to_string()).await {
            Ok(mut client) => {
                match client
                    .get_access_token(GetAccessTokenRequest {
                        client_id: client_id.to_string(),
                        client_secret: client_secret.to_string(),
                    })
                    .await
                {
                    Ok(response) => return response.into_inner().access_token,
                    Err(status)
                        if status.code() == tonic::Code::Unauthenticated
                            && status.message().contains("Invalid client ID") =>
                    {
                        last_error = Some(status.to_string());
                    }
                    Err(status) if status.code() == tonic::Code::Unavailable => {
                        // A newly started distributed peer can accept gRPC
                        // connections before CoreMeta recovery admits public
                        // reads. Treat that explicit retryable response like
                        // the connection races handled by this helper.
                        last_error = Some(status.to_string());
                    }
                    Err(status) => panic!("get access token failed: {status:?}"),
                }
            }
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }

        let delay_ms = if attempt < 10 { 50 } else { 250 };
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }

    panic!(
        "get access token did not observe freshly created client_id {client_id} at {grpc_addr}; last error: {}",
        last_error.unwrap_or_else(|| "unknown".to_string())
    );
}

pub fn authenticated_request<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

pub fn configure_test_public_region(config: &mut anvil_core::config::Config) {
    config.public_region_base_domain = "anvil-storage.test".to_string();
}

async fn start_shared_profile<F, Fut>(profile: &'static str, start: F) -> Arc<TestCluster>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Arc<TestCluster>>,
{
    let _guard = TestClusterStartupLock::acquire(profile).await;
    start().await
}

#[derive(Clone, Copy)]
enum SharedClusterProfile {
    Default,
    PublicRegion,
}

fn start_shared_cluster_thread(profile: SharedClusterProfile) -> Arc<TestCluster> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let name = match profile {
        SharedClusterProfile::Default => "anvil-shared-default-cluster",
        SharedClusterProfile::PublicRegion => "anvil-shared-public-cluster",
    };
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(shared_cluster_runtime_threads())
                .thread_name(name)
                .enable_all()
                .build()
                .expect("build shared Anvil test cluster runtime");
            let cluster = runtime.block_on(async move {
                match profile {
                    SharedClusterProfile::Default => {
                        start_shared_profile("default-6-node", || async {
                            let mut cluster =
                                TestCluster::new_shared(&SHARED_CLUSTER_REGIONS).await;
                            cluster.start_and_converge(Duration::from_secs(20)).await;
                            Arc::new(cluster)
                        })
                        .await
                    }
                    SharedClusterProfile::PublicRegion => {
                        start_shared_profile("public-region-6-node", || async {
                            let mut cluster = TestCluster::new_shared_with_config(
                                &SHARED_CLUSTER_REGIONS,
                                configure_test_public_region,
                            )
                            .await;
                            cluster.start_and_converge(Duration::from_secs(20)).await;
                            Arc::new(cluster)
                        })
                        .await
                    }
                }
            });
            tx.send(cluster).expect("send shared test cluster handle");
            // Keep the runtime alive after the initializing test runtime exits.
            loop {
                std::thread::park();
            }
            #[allow(unreachable_code)]
            drop(runtime);
        })
        .expect("spawn shared Anvil test cluster thread");
    rx.recv().expect("receive shared Anvil test cluster handle")
}

fn shared_cluster_runtime_threads() -> usize {
    std::env::var("ANVIL_SHARED_CLUSTER_RUNTIME_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|parallelism| parallelism.get().saturating_mul(2).clamp(8, 32))
                .unwrap_or(12)
        })
}

struct TestClusterStartupLock {
    path: PathBuf,
}

impl TestClusterStartupLock {
    async fn acquire(profile: &str) -> Self {
        let dir = std::env::temp_dir().join("anvil-test-cluster-locks");
        std::fs::create_dir_all(&dir).expect("create anvil test cluster lock dir");
        let path = dir.join(format!("{profile}.lock"));
        let start = Instant::now();
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    use std::io::Write;
                    let _ = writeln!(file, "pid={}", std::process::id());
                    return Self { path };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if startup_lock_owner_is_dead(&path)
                        || start.elapsed() > Duration::from_secs(30)
                    {
                        let _ = std::fs::remove_file(&path);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("acquire anvil test cluster startup lock {path:?}: {error}"),
            }
        }
    }
}

fn startup_lock_owner_is_dead(path: &std::path::Path) -> bool {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return true;
    };
    let Some(pid) = raw
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|value| value.trim().parse::<u32>().ok())
    else {
        return true;
    };
    !process_is_alive(pid)
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

impl Drop for TestClusterStartupLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub fn test_timing_enabled() -> bool {
    std::env::var("ANVIL_TEST_TIMINGS").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

pub fn emit_test_timing(label: impl AsRef<str>, elapsed: Duration) {
    let label = label.as_ref();
    if test_timing_enabled() {
        eprintln!("[timing] {label}={elapsed:?}");
    }
    if anvil_core::perf::enabled() {
        anvil_core::perf::record_duration("anvil_test_span", &[("span", label)], elapsed);
    }
}

#[allow(dead_code)]
pub async fn get_auth_token(_admin_state_path: &str, grpc_addr: &str) -> String {
    let grpc_url = if grpc_addr.ends_with("/grpc") {
        grpc_addr.to_string()
    } else {
        format!("{}/grpc", grpc_addr.trim_end_matches('/'))
    };
    let mut auth_client = AuthServiceClient::connect(grpc_url).await.unwrap();
    let token_res = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: "test-app".to_string(),
            client_secret: "test-secret".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    token_res.access_token
}

#[allow(dead_code)]
#[allow(unused)]
pub struct TestCluster {
    pub nodes: Vec<JoinHandle<()>>,
    pub states: Vec<AppState>,
    pub grpc_addrs: Vec<String>,
    pub admin_addrs: Vec<String>,
    pub token: String,
    pub admin_state_path: String,
    pub config: Arc<anvil_core::config::Config>,
    pub storage_path: PathBuf,
    cleanup_path: PathBuf,
    pending_listeners: Vec<tokio::net::TcpListener>,
    pending_admin_listeners: Vec<tokio::net::TcpListener>,
    mvcc_membership_initialized: bool,
    _cluster_permit: Option<OwnedSemaphorePermit>,
}

fn test_cluster_lifecycle_projection(
    states: &[AppState],
) -> anvil_core::mesh_lifecycle::BootstrapMeshLifecycleProjection {
    let mut seen_regions = BTreeSet::new();
    let mut regions = Vec::new();
    let mut seen_cells = BTreeSet::new();
    let mut cells = Vec::new();
    let mut nodes = Vec::new();

    for source in states {
        if seen_regions.insert(source.config.region.clone()) {
            regions.push(anvil_core::mesh_lifecycle::CreateRegionDescriptor {
                mesh_id: source.config.mesh_id.clone(),
                region: source.config.region.clone(),
                public_base_url: source.config.public_api_addr.clone(),
                virtual_host_suffix: format!("{}.test.anvil.local", source.config.region),
                placement_weight: 100,
                default_cell: Some(source.config.cell_id.clone()),
            });
        }

        let cell_key = format!("{}/{}", source.config.region, source.config.cell_id);
        if seen_cells.insert(cell_key) {
            cells.push(anvil_core::mesh_lifecycle::RegisterCellDescriptor {
                mesh_id: source.config.mesh_id.clone(),
                region: source.config.region.clone(),
                cell_id: source.config.cell_id.clone(),
                placement_weight: 100,
                failure_domain: source.config.cell_id.clone(),
            });
        }

        nodes.push(anvil_core::mesh_lifecycle::RegisterNodeDescriptor {
            mesh_id: source.config.mesh_id.clone(),
            node_id: source.config.node_id.clone(),
            region: source.config.region.clone(),
            cell_id: source.config.cell_id.clone(),
            receipt_signing_public_key: source.core_store.local_receipt_signing_public_key(),
            public_api_addr: source.config.public_api_addr.clone(),
            capabilities: vec![
                anvil_core::mesh_lifecycle::NodeCapability::Object,
                anvil_core::mesh_lifecycle::NodeCapability::Index,
                anvil_core::mesh_lifecycle::NodeCapability::PersonalDb,
                anvil_core::mesh_lifecycle::NodeCapability::Metadata,
                anvil_core::mesh_lifecycle::NodeCapability::Gateway,
                anvil_core::mesh_lifecycle::NodeCapability::Admin,
            ],
            capacity_json: "{}".to_string(),
        });
    }

    anvil_core::mesh_lifecycle::BootstrapMeshLifecycleProjection {
        regions,
        cells,
        nodes,
    }
}

fn install_test_cluster_physical_lifecycle_projection(
    states: &[AppState],
    projection: &anvil_core::mesh_lifecycle::BootstrapMeshLifecycleProjection,
) {
    for target in states {
        for source in states {
            target
                .core_store
                .register_node_receipt_signing_public_key(
                    &source.config.node_id,
                    &source.core_store.local_receipt_signing_public_key(),
                )
                .unwrap();
        }
        anvil_core::mesh_lifecycle::install_bootstrap_lifecycle_projection(
            &target.storage,
            &target.core_store,
            projection.clone(),
        )
        .unwrap();
    }
}

impl TestCluster {
    pub async fn create_bucket(&self, bucket_name: &str, region: &str) {
        let mut bucket_client =
            anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
                self.grpc_addrs[0].clone(),
            )
            .await
            .unwrap();
        let mut create_req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
            region: region.to_string(),

            options: None,
        });
        create_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.token).parse().unwrap(),
        );
        bucket_client.create_bucket(create_req).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn new(regions: &[&str]) -> Self {
        Self::new_with_config(regions, |_| {}).await
    }

    async fn new_inner(
        regions: &[&str],
        configure: impl FnOnce(&mut anvil_core::config::Config),
        cluster_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        let cluster_permit = match cluster_permit {
            Some(permit) => Some(permit),
            None => Some(acquire_test_cluster_permit().await),
        };
        Self::new_with_optional_permit(regions, configure, cluster_permit).await
    }

    async fn new_shared(regions: &[&str]) -> Self {
        Self::new_shared_with_config(regions, |_| {}).await
    }

    async fn new_shared_with_config(
        regions: &[&str],
        configure: impl FnOnce(&mut anvil_core::config::Config),
    ) -> Self {
        Self::new_with_optional_permit(regions, configure, None).await
    }

    #[allow(dead_code)]
    pub async fn new_with_config(
        regions: &[&str],
        configure: impl FnOnce(&mut anvil_core::config::Config),
    ) -> Self {
        Self::new_inner(regions, configure, None).await
    }

    async fn new_with_optional_permit(
        regions: &[&str],
        configure: impl FnOnce(&mut anvil_core::config::Config),
        cluster_permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        INIT_LOGGER.call_once(|| {
            let filter = std::env::var("ANVIL_TEST_LOG")
                .or_else(|_| std::env::var("RUST_LOG"))
                .unwrap_or_else(|_| "warn,anvil=debug,anvil_core=debug".to_string());
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new(filter))
                .try_init();
        });

        let cluster_id = uuid::Uuid::new_v4().simple().to_string();
        let cluster_storage_root =
            std::env::temp_dir().join(format!("anvil-test-storage-{cluster_id}"));
        let mut grpc_addrs = Vec::with_capacity(regions.len());
        let mut admin_addrs = Vec::with_capacity(regions.len());
        let mut pending_listeners = Vec::with_capacity(regions.len());
        let mut pending_admin_listeners = Vec::with_capacity(regions.len());
        for _ in regions {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            grpc_addrs.push(format!("http://{}", listener.local_addr().unwrap()));
            admin_addrs.push(format!("http://{}", admin_listener.local_addr().unwrap()));
            pending_listeners.push(listener);
            pending_admin_listeners.push(admin_listener);
        }
        let mut config = anvil_core::config::Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            corestore_internal_bearer_token: "test-corestore-internal-token".to_string(),
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            mesh_id: "test-mesh".to_string(),
            region: "".to_string(),
            cell_id: "test-cell-1".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            storage_path: cluster_storage_root
                .join("template")
                .to_string_lossy()
                .into_owned(),
            personaldb_snapshot_entry_threshold: 1024,
            personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
            allow_test_only_embedding_provider: true,
            run_background_worker: true,
            ..anvil_core::config::Config::default()
        };
        configure(&mut config);
        let mvcc_cluster_id = format!("test-{cluster_id}");
        let node_ids = (0..regions.len())
            .map(|node_index| format!("test-{cluster_id}-node-{node_index:03}"))
            .collect::<Vec<_>>();
        let mvcc_peers_json = serde_json::to_string(
            &node_ids
                .iter()
                .enumerate()
                .map(|(node_index, node_id)| {
                    serde_json::json!({
                        "cluster_id": mvcc_cluster_id,
                        "raft_node_id": node_index + 1,
                        "node_id": node_id,
                        "incarnation": 1,
                        "endpoint": grpc_addrs[node_index],
                        "failure_domain": format!("test-cell-{}", node_index + 1),
                        "voter": node_index < TEST_CLUSTER_RAFT_VOTER_COUNT,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        config.mvcc_cluster_id = mvcc_cluster_id;
        // The production bootstrap contract explicitly grants every genesis
        // node. Keep the in-process fixture equivalent so node-to-node
        // authorization does not depend on topology seeded after readiness.
        config.bootstrap_node_ids = node_ids.clone();
        if regions.len() > 1
            && config.mvcc_bundle_quorum_holders == 1
            && config.mvcc_tolerated_failure_domains == 0
        {
            config.mvcc_bundle_quorum_holders = 2;
            config.mvcc_tolerated_failure_domains = 1;
        }
        // This in-process fixture binds ephemeral plaintext loopback peers.
        // Production configurations retain the secure default and bootstrap
        // still rejects non-TLS peer endpoints unless this test-only flag is set.
        config.allow_test_only_insecure_mvcc_transport = true;
        let config = Arc::new(config);

        let unique_regions: HashSet<String> = regions.iter().map(|s| s.to_string()).collect();
        let first_region = regions.first().copied().unwrap_or("default");
        let admin_state_path = cluster_storage_root.join(format!("node-000-{first_region}"));
        let mut states = Vec::new();
        for (node_index, region_name) in regions.iter().enumerate() {
            let mut node_config = config.deref().clone();
            node_config.node_id = node_ids[node_index].clone();
            node_config.mvcc_raft_node_id = node_index as u64 + 1;
            node_config.mvcc_failure_domain = format!("test-cell-{}", node_index + 1);
            node_config.mvcc_peers_json = mvcc_peers_json.clone();
            node_config.mvcc_bootstrap_membership = regions.len() == 1;
            node_config.region = (*region_name).to_string();
            node_config.cell_id = format!("test-cell-{}", node_index + 1);
            node_config.storage_path = cluster_storage_root
                .join(format!("node-{node_index:03}-{region_name}"))
                .to_string_lossy()
                .into_owned();
            node_config.public_api_addr = grpc_addrs[node_index].clone();
            node_config.api_listen_addr = grpc_addrs[node_index]
                .trim_start_matches("http://")
                .to_string();
            node_config.admin_listen_addr = admin_addrs[node_index]
                .trim_start_matches("http://")
                .to_string();
            node_config.corestore_internal_bearer_token =
                anvil_core::auth::JwtManager::new(node_config.jwt_secret.clone())
                    .mint_token(node_config.node_id.clone(), 0)
                    .unwrap();
            let state = AppState::new(node_config, personaldb_test_protocol_keyring())
                .await
                .unwrap();
            if regions.len() == 1 {
                state.persistence.create_region(region_name).await.unwrap();
                let tenant = if let Some(existing) = state
                    .persistence
                    .get_tenant_by_name("default")
                    .await
                    .unwrap()
                {
                    existing
                } else {
                    state
                        .persistence
                        .create_tenant("default", "default-key")
                        .await
                        .unwrap()
                };
                if state
                    .persistence
                    .get_app_by_client_id("test-app")
                    .await
                    .unwrap()
                    .is_none()
                {
                    let encrypted_secret = state.secret_keyring.encrypt(b"test-secret").unwrap();
                    let app = state
                        .persistence
                        .create_app(
                            tenant.id,
                            "test-app",
                            "test-app",
                            &encrypted_secret,
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    access_control::grant_storage_tenant_owner(
                        &state.persistence,
                        tenant.id,
                        &app.id.to_string(),
                        "test-cluster",
                        "grant test app ownership of its storage tenant",
                    )
                    .await
                    .unwrap();
                }
            }
            states.push(state);
        }
        if regions.len() == 1 {
            for region in unique_regions {
                for state in &states {
                    state.persistence.create_region(&region).await.unwrap();
                }
            }
            install_canonical_coremeta_bootstrap_snapshot(&states);
        }
        // Physical placement and receipt verification are bootstrap inputs,
        // not post-readiness data. Install them before listeners can admit
        // background materialization work.
        let physical_projection = test_cluster_lifecycle_projection(&states);
        install_test_cluster_physical_lifecycle_projection(&states, &physical_projection);

        Self {
            nodes: Vec::new(),
            states,
            grpc_addrs,
            admin_addrs,
            token: String::new(),
            admin_state_path: admin_state_path.to_string_lossy().into_owned(),
            config,
            storage_path: admin_state_path,
            cleanup_path: cluster_storage_root,
            pending_listeners,
            pending_admin_listeners,
            mvcc_membership_initialized: regions.len() == 1,
            _cluster_permit: cluster_permit,
        }
    }

    pub async fn start_and_converge(&mut self, timeout: Duration) {
        self.start_and_converge_no_new_token(timeout, true).await
    }

    pub async fn start_and_converge_no_new_token(
        &mut self,
        timeout: Duration,
        get_new_token: bool,
    ) {
        let total_start = Instant::now();
        let node_count = self.states.len();

        let node_spawn_start = Instant::now();
        let mut listeners = std::mem::take(&mut self.pending_listeners);
        let mut admin_listeners = std::mem::take(&mut self.pending_admin_listeners);
        if listeners.is_empty() && admin_listeners.is_empty() {
            for (grpc_addr, admin_addr) in self.grpc_addrs.iter().zip(&self.admin_addrs) {
                listeners.push(
                    tokio::net::TcpListener::bind(grpc_addr.trim_start_matches("http://"))
                        .await
                        .unwrap(),
                );
                admin_listeners.push(
                    tokio::net::TcpListener::bind(admin_addr.trim_start_matches("http://"))
                        .await
                        .unwrap(),
                );
            }
        }
        assert_eq!(listeners.len(), self.states.len());
        assert_eq!(admin_listeners.len(), self.states.len());

        for i in 0..self.states.len() {
            let state = self.states[i].clone();
            let listener = listeners.remove(0);
            let admin_listener = admin_listeners.remove(0);

            let handle = tokio::spawn(async move {
                anvil::start_node_with_admin_listener(listener, Some(admin_listener), state)
                    .await
                    .unwrap();
            });
            self.nodes.push(handle);
        }
        emit_test_timing(
            format!("start_and_converge node_spawn nodes={node_count}"),
            node_spawn_start.elapsed(),
        );

        let token_start = Instant::now();
        if get_new_token && self.states.len() == 1 {
            let test_app = self.states[0]
                .persistence
                .get_app_by_client_id("test-app")
                .await
                .unwrap()
                .expect("test-app is seeded before cluster start");
            self.token = self.states[0]
                .jwt_manager
                .mint_token(test_app.id.to_string(), test_app.tenant_id)
                .unwrap();
        }
        emit_test_timing(
            format!("start_and_converge token nodes={node_count}"),
            token_start.elapsed(),
        );

        let start = Instant::now();
        loop {
            let mut all_ports_ready = true;
            for addr_str in self.grpc_addrs.iter().chain(self.admin_addrs.iter()) {
                let addr: SocketAddr = addr_str.replace("http://", "").parse().unwrap();
                if !is_port_open(addr).await {
                    all_ports_ready = false;
                    break;
                }
            }
            if all_ports_ready {
                emit_test_timing(
                    format!("start_and_converge port_ready nodes={node_count}"),
                    start.elapsed(),
                );
                let transport_ready_start = Instant::now();
                let reachable_addrs = self
                    .grpc_addrs
                    .iter()
                    .chain(self.admin_addrs.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(
                    wait_for_all_http_reachable(
                        &reachable_addrs,
                        timeout.saturating_sub(start.elapsed())
                    )
                    .await,
                    "Cluster ports opened but HTTP services did not start on every node"
                );
                emit_test_timing(
                    format!("start_and_converge transport_ready nodes={node_count}"),
                    transport_ready_start.elapsed(),
                );
                if !self.mvcc_membership_initialized {
                    self.states[0]
                        .mvcc
                        .initialize_configured_test_membership(
                            &self.config.mvcc_cluster_id,
                            self.config.mvcc_bundle_quorum_holders,
                            self.config.mvcc_tolerated_failure_domains,
                        )
                        .await
                        .expect("initialize multi-node test MVCC membership");
                    self.mvcc_membership_initialized = true;
                    self.seed_multi_node_mvcc_bootstrap(get_new_token).await;
                }
                let http_ready_start = Instant::now();
                assert!(
                    wait_for_all_http_ready(
                        &reachable_addrs,
                        timeout.saturating_sub(start.elapsed())
                    )
                    .await,
                    "Cluster listeners opened but HTTP readiness probes did not pass for all nodes"
                );
                emit_test_timing(
                    format!("start_and_converge http_ready nodes={node_count}"),
                    http_ready_start.elapsed(),
                );
                let lifecycle_seed_start = Instant::now();
                self.seed_corestore_mesh_lifecycle().await;
                emit_test_timing(
                    format!("start_and_converge mesh_lifecycle_seed nodes={node_count}"),
                    lifecycle_seed_start.elapsed(),
                );
                let stabilization_start = Instant::now();
                tokio::time::sleep(Duration::from_secs(3)).await;
                emit_test_timing(
                    format!("start_and_converge stabilization_sleep nodes={node_count}"),
                    stabilization_start.elapsed(),
                );
                emit_test_timing(
                    format!("start_and_converge total nodes={node_count}"),
                    total_start.elapsed(),
                );
                return;
            }
            if start.elapsed() >= timeout {
                let recovery = self
                    .states
                    .iter()
                    .map(|state| state.core_store.coremeta_recovery_snapshot())
                    .collect::<Vec<_>>();
                panic!(
                    "Cluster ports did not become ready in time; CoreMeta recovery snapshots: {:?}",
                    recovery
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn seed_multi_node_mvcc_bootstrap(&mut self, mint_token: bool) {
        debug_assert!(self.states.len() > 1);
        let leader = &self.states[0];
        let regions = self
            .states
            .iter()
            .map(|state| state.config.region.clone())
            .collect::<BTreeSet<_>>();
        for region in &regions {
            leader.persistence.create_region(region).await.unwrap();
        }
        let tenant = if let Some(existing) = leader
            .persistence
            .get_tenant_by_name("default")
            .await
            .unwrap()
        {
            existing
        } else {
            leader
                .persistence
                .create_tenant("default", "default-key")
                .await
                .unwrap()
        };
        let app_id = if let Some(existing) = leader
            .persistence
            .get_app_by_client_id("test-app")
            .await
            .unwrap()
        {
            existing.id
        } else {
            let encrypted_secret = leader.secret_keyring.encrypt(b"test-secret").unwrap();
            leader
                .persistence
                .create_app(
                    tenant.id,
                    "test-app",
                    "test-app",
                    &encrypted_secret,
                    None,
                    None,
                )
                .await
                .unwrap()
                .id
        };
        access_control::grant_storage_tenant_owner(
            &leader.persistence,
            tenant.id,
            &app_id.to_string(),
            "test-cluster",
            "grant test app ownership of its storage tenant",
        )
        .await
        .unwrap();

        let committed_version = anvil_mvcc_consensus::Consensus::observed_commit_version(
            leader.mvcc.consensus.as_ref(),
        )
        .0;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut converged = true;
                for state in &self.states {
                    let applied = state
                        .mvcc
                        .runtime
                        .local_store()
                        .decision_watermark()
                        .unwrap();
                    let regions_visible = state
                        .persistence
                        .list_regions()
                        .await
                        .unwrap()
                        .into_iter()
                        .collect::<BTreeSet<_>>();
                    let tenant_visible = state
                        .persistence
                        .get_tenant_by_name("default")
                        .await
                        .unwrap()
                        .is_some();
                    let app_visible = state
                        .persistence
                        .get_app_by_client_id("test-app")
                        .await
                        .unwrap()
                        .is_some();
                    if applied < committed_version
                        || !regions.is_subset(&regions_visible)
                        || !tenant_visible
                        || !app_visible
                    {
                        converged = false;
                        break;
                    }
                }
                if converged {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("multi-node test MVCC bootstrap did not materialize on every replica");
        let minted_token = mint_token.then(|| {
            leader
                .jwt_manager
                .mint_token(app_id.to_string(), tenant.id)
                .unwrap()
        });
        if let Some(token) = minted_token {
            self.token = token;
        }
    }

    /// Seed canonical lifecycle state without starting listeners or the
    /// background worker. State-only tests use this before calling persistence
    /// APIs whose production admission checks require an active region.
    pub async fn seed_corestore_mesh_lifecycle_for_state_only_tests(&self) {
        self.seed_corestore_mesh_lifecycle().await;
    }

    async fn seed_corestore_mesh_lifecycle(&self) {
        let projection = test_cluster_lifecycle_projection(&self.states);
        let regions = projection.regions.clone();
        let cells = projection.cells.clone();
        let nodes = projection.nodes.clone();

        let node_default_grants = self
            .states
            .iter()
            .map(|source| {
                (
                    source.config.region.clone(),
                    source.config.cell_id.clone(),
                    source.config.node_id.clone(),
                )
            })
            .collect::<Vec<_>>();

        let Some(canonical) = self.states.first() else {
            return;
        };
        anvil_core::access_control::grant_node_defaults_batch(
            &canonical.persistence,
            &node_default_grants,
            "test-cluster",
            "test cluster node bootstrap",
        )
        .await
        .unwrap_or_else(|err| {
            panic!(
                "grant_node_defaults_batch target={}: {err:?}",
                canonical.config.node_id
            )
        });
        install_test_cluster_physical_lifecycle_projection(&self.states, &projection);

        // Keep the physical bootstrap projection while CoreStore shard
        // placement still reads it, but seed the authoritative topology through
        // the same MVCC persistence APIs as production. Existing non-joining
        // lifecycle state is preserved across TestCluster restarts.
        let mut existing_regions = canonical
            .persistence
            .list_region_descriptors()
            .await
            .unwrap()
            .into_iter()
            .map(|descriptor| (descriptor.region.clone(), descriptor))
            .collect::<BTreeMap<_, _>>();
        let mut seeded_regions = Vec::with_capacity(regions.len());
        for region in &regions {
            let descriptor = match existing_regions.remove(&region.region) {
                Some(descriptor) => descriptor,
                None => canonical
                    .persistence
                    .create_region_descriptor(region.clone())
                    .await
                    .unwrap_or_else(|error| panic!("seed MVCC region descriptor: {error:?}")),
            };
            seeded_regions.push(descriptor);
        }

        let mut existing_cells = canonical
            .persistence
            .list_cell_descriptors(None)
            .await
            .unwrap()
            .into_iter()
            .map(|descriptor| {
                (
                    (descriptor.region.clone(), descriptor.cell_id.clone()),
                    descriptor,
                )
            })
            .collect::<BTreeMap<_, _>>();
        for cell in &cells {
            let key = (cell.region.clone(), cell.cell_id.clone());
            let descriptor = match existing_cells.remove(&key) {
                Some(descriptor) => descriptor,
                None => canonical
                    .persistence
                    .register_cell_descriptor(cell.clone())
                    .await
                    .unwrap_or_else(|error| panic!("seed MVCC cell descriptor: {error:?}")),
            };
            if descriptor.state == anvil_core::mesh_lifecycle::LifecycleState::Joining {
                canonical
                    .persistence
                    .transition_cell_descriptor(
                        &descriptor.region,
                        &descriptor.cell_id,
                        descriptor.generation,
                        anvil_core::mesh_lifecycle::LifecycleState::Active,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("activate MVCC cell descriptor: {error:?}"));
            }
        }

        for descriptor in seeded_regions {
            if descriptor.state == anvil_core::mesh_lifecycle::LifecycleState::Joining {
                canonical
                    .persistence
                    .transition_region_descriptor(
                        &descriptor.region,
                        descriptor.generation,
                        anvil_core::mesh_lifecycle::LifecycleState::Active,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("activate MVCC region descriptor: {error:?}"));
            }
        }

        let mut existing_nodes = canonical
            .persistence
            .list_node_descriptors(None, None)
            .await
            .unwrap()
            .into_iter()
            .map(|descriptor| (descriptor.node_id.clone(), descriptor))
            .collect::<BTreeMap<_, _>>();
        for node in &nodes {
            let descriptor = match existing_nodes.remove(&node.node_id) {
                Some(descriptor) => descriptor,
                None => canonical
                    .persistence
                    .register_node_descriptor(node.clone())
                    .await
                    .unwrap_or_else(|error| panic!("seed MVCC node descriptor: {error:?}")),
            };
            if descriptor.state == anvil_core::mesh_lifecycle::LifecycleState::Joining {
                canonical
                    .persistence
                    .transition_node_descriptor(
                        &descriptor.node_id,
                        descriptor.generation,
                        anvil_core::mesh_lifecycle::LifecycleState::Active,
                        None,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("activate MVCC node descriptor: {error:?}"));
            }
        }
    }

    #[allow(unused)]
    pub async fn get_s3_client(
        &self,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Client {
        let credentials = Credentials::new(access_key, secret_key, None, None, "static");
        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .endpoint_url(&self.grpc_addrs[0])
            .force_path_style(true)
            .behavior_version(BehaviorVersion::latest())
            .build();
        S3Client::from_conf(config)
    }

    #[allow(unused)]
    pub async fn restart(&mut self, timeout: Duration) {
        let nodes = self.nodes.drain(..).collect::<Vec<_>>();
        for node in &nodes {
            node.abort();
        }
        for node in nodes {
            let _ = node.await;
        }
        self.start_and_converge(timeout).await;
    }

    pub fn admin_token(&self) -> String {
        self.states[0]
            .jwt_manager
            .mint_token("admin-principal".to_string(), 0)
            .unwrap()
    }

    pub async fn create_application(&self, tenant_id: &str, app_name: &str) -> (String, String) {
        let (_app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_id, app_name).await;
        (client_id, client_secret)
    }

    pub async fn create_application_with_id(
        &self,
        tenant_id: &str,
        app_name: &str,
    ) -> (String, String, String) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::CreateApplicationRequest {
            context: Some(test_admin_context(&format!("create-app-{app_name}"), 0)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        let response = client
            .create_application(request)
            .await
            .unwrap()
            .into_inner();
        (response.app_id, response.client_id, response.client_secret)
    }

    pub async fn grant_application_policy(
        &self,
        tenant_id: &str,
        app_name: &str,
        action: &str,
        resource: &str,
    ) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::GrantApplicationPolicyRequest {
            context: Some(test_admin_context(&format!("grant-{app_name}"), 0)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        client.grant_application_policy(request).await.unwrap();
    }

    pub async fn rotate_application_secret(
        &self,
        tenant_id: &str,
        app_name: &str,
    ) -> (String, String) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::RotateApplicationSecretRequest {
            context: Some(test_admin_context(&format!("rotate-app-{app_name}"), 1)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        let response = client
            .rotate_application_secret(request)
            .await
            .unwrap()
            .into_inner();
        (response.client_id, response.client_secret)
    }

    pub async fn create_application_with_policy(
        &self,
        tenant_id: &str,
        app_name: &str,
        action: &str,
        resource: &str,
    ) -> (String, String) {
        let credentials = self.create_application(tenant_id, app_name).await;
        self.grant_application_policy(tenant_id, app_name, action, resource)
            .await;
        credentials
    }

    pub async fn create_application_with_storage_tenant_owner(
        &self,
        tenant_ref: &str,
        app_name: &str,
    ) -> (String, String) {
        let (_app_id, client_id, client_secret) = self
            .create_application_with_storage_tenant_owner_id(tenant_ref, app_name)
            .await;
        (client_id, client_secret)
    }

    pub async fn create_application_with_storage_tenant_owner_id(
        &self,
        tenant_ref: &str,
        app_name: &str,
    ) -> (String, String, String) {
        let (app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_ref, app_name).await;
        let tenant_id = if let Ok(tenant_id) = tenant_ref.parse::<i64>() {
            tenant_id
        } else {
            self.states[0]
                .persistence
                .get_tenant_by_name(tenant_ref)
                .await
                .unwrap()
                .expect("tenant exists")
                .id
        };
        access_control::grant_storage_tenant_owner(
            &self.states[0].persistence,
            tenant_id,
            &app_id,
            "test-admin",
            "test grant storage tenant owner",
        )
        .await
        .unwrap();
        (app_id, client_id, client_secret)
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            node.abort();
        }
        if let Err(e) = std::fs::remove_dir_all(&self.cleanup_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "Failed to remove test storage {}: {}",
                    self.cleanup_path.display(),
                    e
                );
            }
        }
    }
}

#[allow(dead_code)]
pub async fn is_port_open(addr: SocketAddr) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::net::TcpStream::connect(addr)
        )
        .await,
        Ok(Ok(_))
    )
}

pub async fn wait_for_port(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

pub async fn wait_for_http_ready(base_url: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    let ready_url = format!("{}/ready", base_url.trim_end_matches('/'));
    while start.elapsed() < timeout {
        if let Ok(Ok(response)) =
            tokio::time::timeout(Duration::from_secs(2), reqwest::get(&ready_url)).await
            && response.status().is_success()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn wait_for_http_reachable(base_url: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    let health_url = format!("{}/health", base_url.trim_end_matches('/'));
    while start.elapsed() < timeout {
        if matches!(
            tokio::time::timeout(Duration::from_secs(2), reqwest::get(&health_url)).await,
            Ok(Ok(_))
        ) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

async fn wait_for_all_http_ready(base_urls: &[String], timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let mut all_ready = true;
        for base_url in base_urls {
            let ready_url = format!("{}/ready", base_url.trim_end_matches('/'));
            match tokio::time::timeout(Duration::from_secs(2), reqwest::get(&ready_url)).await {
                Ok(Ok(response)) if response.status().is_success() => {}
                _ => {
                    all_ready = false;
                    break;
                }
            }
        }
        if all_ready {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn wait_for_all_http_reachable(base_urls: &[String], timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let mut all_reachable = true;
        for base_url in base_urls {
            let health_url = format!("{}/health", base_url.trim_end_matches('/'));
            if !matches!(
                tokio::time::timeout(Duration::from_secs(2), reqwest::get(&health_url)).await,
                Ok(Ok(_))
            ) {
                all_reachable = false;
                break;
            }
        }
        if all_reachable {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
