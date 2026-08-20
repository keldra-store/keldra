use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use keldra::authentication::{JwtManager, RateLimitConfig};
use keldra::{ServerConfig, serve};
use keldra_api::v1::administration_service_client::AdministrationServiceClient;
use keldra_api::v1::object_service_client::ObjectServiceClient;
use keldra_api::v1::put_header::Operation as PutOperationValue;
use keldra_api::v1::{
    CreateApplicationRequest, CreateBucketRequest, Durability, GetObjectRequest, ObjectAddress,
    ObjectVersioning, ProvisionTenantRequest, PutHeader, PutOperation,
    RecoverApplicationCredentialRequest, RotateApplicationCredentialRequest,
};
use keldra_store::{Store, StoreOptions, SystemBootstrapRequest};
use tempfile::TempDir;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

const SIGNING_KEY: &[u8] = b"keldra-credential-recovery-security-test-key";
const SYSTEM_SECRET: &str = "system-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const ACME_SECRET: &str = "acme-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const BETA_SECRET: &str = "beta-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const AUDITOR_SECRET: &str = "auditor-secret-0123456789abcdef0123456789abcdef0123456789abcdef";
const WORKER_SECRET: &str = "worker-secret-0123456789abcdef0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn hostile_callers_fail_closed_and_system_recovery_is_exact_and_idempotent() {
    let fixture = Fixture::start().await;
    let system_token = exchange(&fixture.channel, "bootstrap-client", SYSTEM_SECRET)
        .await
        .expect("bootstrap credential must exchange");
    let mut system = administration(&fixture.channel, &system_token);

    system
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: "acme".into(),
            owner_app_id: "acme-owner".into(),
            owner_client_id: "acme-owner-client".into(),
            owner_client_secret: ACME_SECRET.into(),
        })
        .await
        .unwrap();
    system
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: "beta".into(),
            owner_app_id: "beta-owner".into(),
            owner_client_id: "beta-owner-client".into(),
            owner_client_secret: BETA_SECRET.into(),
        })
        .await
        .unwrap();
    system
        .create_application(CreateApplicationRequest {
            app_id: "system-auditor".into(),
            client_id: "system-auditor-client".into(),
            client_secret: AUDITOR_SECRET.into(),
        })
        .await
        .unwrap();

    let acme_token = exchange(&fixture.channel, "acme-owner-client", ACME_SECRET)
        .await
        .unwrap();
    let beta_token = exchange(&fixture.channel, "beta-owner-client", BETA_SECRET)
        .await
        .unwrap();
    let auditor_token = exchange(&fixture.channel, "system-auditor-client", AUDITOR_SECRET)
        .await
        .unwrap();

    let mut acme_admin = administration(&fixture.channel, &acme_token);
    acme_admin
        .create_bucket(CreateBucketRequest {
            bucket: "objects".into(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await
        .unwrap();
    acme_admin
        .create_application(CreateApplicationRequest {
            app_id: "acme-worker".into(),
            client_id: "acme-worker-client".into(),
            client_secret: WORKER_SECRET.into(),
        })
        .await
        .unwrap();

    let preserved = ObjectAddress {
        tenant: "acme".into(),
        bucket: "objects".into(),
        path: "security/preserved".into(),
    };
    let mut acme_objects = keldra_storage::object_client(fixture.channel.clone(), &acme_token)
        .expect("owner token must form valid metadata");
    keldra_storage::put_chunks(
        &mut acme_objects,
        PutHeader {
            address: Some(preserved.clone()),
            content_type: "application/octet-stream".into(),
            command_id: "credential-recovery-preserved-object".into(),
            durability: Durability::Local as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        },
        [b"preserved".to_vec()],
    )
    .await
    .unwrap();

    let target = RecoverApplicationCredentialRequest {
        storage_tenant: "acme".into(),
        app_id: "acme-owner".into(),
        client_id: "acme-owner-client".into(),
        client_secret: "replacement-0123456789abcdef0123456789abcdef0123456789abcdef".into(),
    };

    let mut anonymous = AdministrationServiceClient::new(fixture.channel.clone());
    assert_eq!(
        anonymous
            .recover_application_credential(target.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );

    let mut malformed = Request::new(target.clone());
    malformed
        .metadata_mut()
        .insert("authorization", "Bearer attacker-garbage".parse().unwrap());
    assert_eq!(
        anonymous
            .recover_application_credential(malformed)
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );

    let deliberately_malformed_target = RecoverApplicationCredentialRequest {
        storage_tenant: "NOT/A/TENANT".into(),
        ..target.clone()
    };
    assert_eq!(
        administration(&fixture.channel, &acme_token)
            .recover_application_credential(deliberately_malformed_target.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "tenant callers must be rejected before target parsing"
    );
    assert_eq!(
        administration(&fixture.channel, &auditor_token)
            .recover_application_credential(deliberately_malformed_target.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "an unprivileged system-tenant app is not a system administrator"
    );
    assert_eq!(
        system
            .recover_application_credential(deliberately_malformed_target)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument,
        "an authorized system caller reaches canonical target validation"
    );

    for hostile_target in [
        RecoverApplicationCredentialRequest {
            storage_tenant: "missing".into(),
            ..target.clone()
        },
        RecoverApplicationCredentialRequest {
            app_id: "missing-app".into(),
            ..target.clone()
        },
        RecoverApplicationCredentialRequest {
            storage_tenant: "beta".into(),
            ..target.clone()
        },
        RecoverApplicationCredentialRequest {
            client_id: "beta-owner-client".into(),
            ..target.clone()
        },
    ] {
        assert!(
            system
                .recover_application_credential(hostile_target)
                .await
                .is_err(),
            "an inexact tenant/application/client binding must fail closed"
        );
    }
    let too_short = RecoverApplicationCredentialRequest {
        client_secret: "short".into(),
        ..target.clone()
    };
    assert_eq!(
        system
            .recover_application_credential(too_short)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert!(
        exchange(&fixture.channel, "acme-owner-client", ACME_SECRET)
            .await
            .is_ok(),
        "failed hostile requests must not alter the existing credential"
    );

    let rotated_worker_secret = "worker-rotated-0123456789abcdef0123456789abcdef0123456789abcdef";
    let rotation = acme_admin
        .rotate_application_credential(RotateApplicationCredentialRequest {
            app_id: "acme-worker".into(),
            client_id: "acme-worker-client".into(),
            client_secret: rotated_worker_secret.into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!rotation.replayed);
    assert!(
        exchange(&fixture.channel, "acme-worker-client", WORKER_SECRET)
            .await
            .is_err()
    );
    assert!(
        exchange(
            &fixture.channel,
            "acme-worker-client",
            rotated_worker_secret
        )
        .await
        .is_ok(),
        "ordinary same-tenant rotation must remain available"
    );

    let first_request = authorized(target.clone(), &system_token);
    let second_request = authorized(target.clone(), &system_token);
    let mut first_client = AdministrationServiceClient::new(fixture.channel.clone());
    let mut second_client = AdministrationServiceClient::new(fixture.channel.clone());
    let (first, second) = tokio::join!(
        first_client.recover_application_credential(first_request),
        second_client.recover_application_credential(second_request)
    );
    let first = first.unwrap().into_inner();
    let second = second.unwrap().into_inner();
    assert_eq!(
        [first.replayed, second.replayed]
            .into_iter()
            .filter(|replayed| *replayed)
            .count(),
        1,
        "concurrent identical recovery must produce one mutation and one replay"
    );
    for recovered in [&first, &second] {
        assert_eq!(recovered.storage_tenant, "acme");
        assert_eq!(recovered.app_id, "acme-owner");
        assert_eq!(recovered.client_id, "acme-owner-client");
        assert!(recovered.active);
    }

    assert_eq!(
        exchange(&fixture.channel, "acme-owner-client", ACME_SECRET)
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated,
        "the replaced secret must stop minting tokens"
    );
    // A rejected exchange consumes the same per-client credential admission
    // token as a successful exchange. Keep this security test independent of
    // the production limiter's refill timing before proving the replacement.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let replacement_token = exchange(&fixture.channel, "acme-owner-client", &target.client_secret)
        .await
        .expect("the replacement secret must mint tokens");

    let replay = system
        .recover_application_credential(target)
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);

    let mut pre_rotation_client = ObjectServiceClient::new(fixture.channel.clone());
    pre_rotation_client
        .get_object(authorized(
            GetObjectRequest {
                address: Some(preserved.clone()),
                version: None,
            },
            &acme_token,
        ))
        .await
        .expect("a bearer issued before rotation remains valid until its expiry");
    let mut replacement_client = ObjectServiceClient::new(fixture.channel.clone());
    replacement_client
        .get_object(authorized(
            GetObjectRequest {
                address: Some(preserved),
                version: None,
            },
            &replacement_token,
        ))
        .await
        .expect("the replacement identity retains the tenant's roles and objects");

    administration(&fixture.channel, &beta_token)
        .create_bucket(CreateBucketRequest {
            bucket: "unaffected".into(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await
        .expect("another tenant remains unaffected by recovery probes");

    fixture.stop().await;
}

struct Fixture {
    _directory: TempDir,
    channel: Channel,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Fixture {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        store
            .bootstrap_system(SystemBootstrapRequest {
                app_id: "bootstrap-app".into(),
                client_id: "bootstrap-client".into(),
                client_secret: SYSTEM_SECRET.into(),
            })
            .unwrap();
        drop(store);

        let token_manager = JwtManager::new(SIGNING_KEY).unwrap();
        let listen = unused_loopback_address();
        let server = tokio::spawn(serve(test_server_config(&directory, listen, token_manager)));
        let channel = connect_when_ready(listen).await;
        Self {
            _directory: directory,
            channel,
            server,
        }
    }

    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

fn administration(channel: &Channel, token: &str) -> keldra_storage::RawAdministrationClient {
    keldra_storage::administration_client(channel.clone(), token).unwrap()
}

async fn exchange(
    channel: &Channel,
    client_id: &str,
    client_secret: &str,
) -> Result<String, tonic::Status> {
    keldra_storage::exchange_client_credentials(
        channel.clone(),
        client_id.to_owned(),
        client_secret.to_owned(),
    )
    .await
    .map(|token| token.access_token)
}

fn authorized<T>(value: T, access_token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {access_token}").parse().unwrap(),
    );
    request
}

fn test_server_config(
    directory: &TempDir,
    listen: SocketAddr,
    token_manager: JwtManager,
) -> ServerConfig {
    let mut peer_listen = unused_loopback_address();
    while peer_listen == listen {
        peer_listen = unused_loopback_address();
    }
    ServerConfig {
        listen,
        peer_listen,
        peer_advertise: None,
        join_bundle: None,
        storage: keldra::StoragePaths::under(directory.path(), 8 * 1024 * 1024),
        explicit_authoritative_paths: keldra::ExplicitAuthoritativePaths::default(),
        run_system_bootstrap: true,
        system_bootstrap_credential_output: None,
        node_id: 1,
        max_atomic_commit_entries: 128,
        max_atomic_commit_bytes: 1024 * 1024,
        atomic_program_timeout: Duration::from_secs(30),
        index_query_timeout: Duration::from_secs(300),
        token_manager,
        rate_limits: RateLimitConfig::default(),
        index_runtime: keldra::IndexRuntimeConfig::default(),
        plugin_gateway: keldra::PluginGatewayConfig::default(),
        max_blob_bytes: 1024 * 1024,
        erasure_profile: keldra_store::ErasureProfile::default(),
        awaiting_publish_ttl_seconds: keldra_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        mutation_receipt_retention_seconds: 60,
        max_mutation_receipt_entries: 512,
        max_mutation_receipt_bytes: 1024 * 1024,
        source_journal_max_entries: 512,
        source_journal_max_bytes: 1024 * 1024,
    }
}

fn unused_loopback_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn connect_when_ready(listen: SocketAddr) -> Channel {
    let endpoint = Endpoint::from_shared(format!("http://{listen}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100));
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return channel,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Keldra test server did not start: {error}"),
        }
    }
}
