use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use anvil::authentication::{JwtManager, RateLimitConfig};
use anvil::{ServerConfig, serve};
use anvil_api::v1::object_head::State as HeadState;
use anvil_api::v1::object_service_client::ObjectServiceClient;
use anvil_api::v1::put_header::Operation;
use anvil_api::v1::{
    Durability, HeadObjectRequest, ObjectAddress, PutHeader, PutOperation, PutRequest,
};
use anvil_authz::ObjectRef;
use anvil_store::{
    AuthzRevision, CreateBucketRequest, ObjectVersioning, ProvisionTenantRequest, StorageTenantId,
    Store, StoreOptions, SystemBootstrapRequest,
};
use tempfile::TempDir;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

const SIGNING_KEY: &[u8] = b"anvil-putend-visibility-test-key";
const OWNER_SECRET: &str = "owner-secret-0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn streamed_bytes_are_invisible_until_put_end_and_replay_publishes_once() {
    let directory = tempfile::tempdir().unwrap();
    seed_authorized_bucket(&directory).await;

    let token_manager = JwtManager::new(SIGNING_KEY).unwrap();
    let access_token = token_manager
        .mint(StorageTenantId::parse("acme").unwrap(), "owner-app")
        .unwrap();
    let listen = unused_loopback_address();
    let server = tokio::spawn(serve(test_server_config(&directory, listen, token_manager)));
    let channel = connect_when_ready(listen).await;
    let mut client = ObjectServiceClient::new(channel);
    let address = ObjectAddress {
        tenant: "acme".into(),
        bucket: "objects".into(),
        path: "three-phase.txt".into(),
    };

    let upload = client
        .start_put(authorized(
            PutHeader {
                address: Some(address.clone()),
                content_type: "text/plain".into(),
                command_id: "three-phase-put".into(),
                durability: Durability::Local as i32,
                operation: Some(Operation::Put(PutOperation {})),
            },
            &access_token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_never_existed(head(&mut client, &address, &access_token).await);

    let ready = client
        .put(authorized(
            tokio_stream::iter([PutRequest {
                token: Some(upload),
                chunk: b"sealed but not published".to_vec(),
            }]),
            &access_token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_never_existed(head(&mut client, &address, &access_token).await);

    let first = client
        .put_end(authorized(ready.clone(), &access_token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.command_id, "three-phase-put");
    assert_ne!(first.version, 0);
    assert!(!first.replayed);
    let published_version = first.version;

    let after_publish = head(&mut client, &address, &access_token).await;
    let Some(HeadState::Present(present)) = after_publish.state else {
        panic!("PutEnd must make the sealed object visible as Present");
    };
    assert_eq!(present.version, published_version);
    assert_eq!(present.content_length, 24);
    assert_eq!(present.content_type, "text/plain");
    assert_eq!(
        present.content_hash.as_slice(),
        blake3::hash(b"sealed but not published").as_bytes()
    );

    let replay = client
        .put_end(authorized(ready, &access_token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.command_id, first.command_id);
    assert_eq!(replay.version, first.version);
    assert!(replay.replayed);

    let after_replay = head(&mut client, &address, &access_token).await;
    let Some(HeadState::Present(present)) = after_replay.state else {
        panic!("replayed PutEnd must leave the original object visible");
    };
    assert_eq!(present.version, published_version);

    server.abort();
    let _ = server.await;
}

async fn seed_authorized_bucket(directory: &TempDir) {
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: "bootstrap-secret-0123456789abcdef0123456789abcdef".into(),
        })
        .unwrap();
    let owner = ObjectRef::opaque("app", "owner-app").unwrap();
    store
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            owner_app_id: "owner-app".into(),
            owner_client_id: "owner-client".into(),
            owner_client_secret: OWNER_SECRET.into(),
            principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
            expected_authorization_revision: AuthzRevision(3),
            expected_binding_generation: 1,
        })
        .unwrap();
    store
        .create_bucket(CreateBucketRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            bucket: "objects".into(),
            owner: owner.clone(),
            principal: owner,
            expected_authorization_revision: AuthzRevision(4),
            expected_binding_generation: 1,
            versioning: ObjectVersioning::Unversioned,
        })
        .unwrap();
}

fn test_server_config(
    directory: &TempDir,
    listen: SocketAddr,
    token_manager: JwtManager,
) -> ServerConfig {
    ServerConfig {
        listen,
        data_dir: directory.path().to_owned(),
        run_system_bootstrap: false,
        system_bootstrap_credential_output: None,
        node_id: 1,
        max_atomic_commit_entries: 128,
        max_atomic_commit_bytes: 1024 * 1024,
        atomic_program_timeout: Duration::from_secs(30),
        token_manager,
        rate_limits: RateLimitConfig::default(),
        max_blob_bytes: 1024 * 1024,
        awaiting_publish_ttl_seconds: anvil_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        mutation_receipt_retention_seconds: 60,
        max_mutation_receipt_entries: 128,
        max_mutation_receipt_bytes: 1024 * 1024,
        watch_max_entries: 128,
        watch_max_bytes: 1024 * 1024,
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
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Anvil test server did not start: {error}"),
        }
    }
}

fn authorized<T>(value: T, access_token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {access_token}").parse().unwrap(),
    );
    request
}

async fn head(
    client: &mut ObjectServiceClient<Channel>,
    address: &ObjectAddress,
    access_token: &str,
) -> anvil_api::v1::ObjectHead {
    client
        .head_object(authorized(
            HeadObjectRequest {
                address: Some(address.clone()),
            },
            access_token,
        ))
        .await
        .unwrap()
        .into_inner()
}

fn assert_never_existed(head: anvil_api::v1::ObjectHead) {
    assert!(matches!(head.state, Some(HeadState::NeverExisted(_))));
}
