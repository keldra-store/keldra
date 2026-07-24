use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil::anvil_api::{
    BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest, ConsistencyMode,
    GetTransactionRequest, ListObjectVersionsResponse, TransactionScope, TransactionStatus,
    WriteResponse, WriteState,
};
use anvil_test_utils::{DockerTestStorageActor, isolated_docker_test_cluster};
use serde::Deserialize;
use tonic::{Request, Status};

use super::common::*;

// Distributed setup and the initial publication can consume several seconds on
// loaded CI hosts. Leave enough lease time for the test to reach its deliberate
// post-Q2 crash point before waiting for expiry.
const EXPLICIT_TRANSACTION_TTL_MS: u64 = 60_000;
const TEST_CONTROL_TOKEN: &str = "distributed-root-publication-test";
const TEST_CONTROL_TOKEN_HEADER: &str = "x-anvil-test-control-token";

#[derive(Debug, Deserialize)]
struct RootPublicationStatus {
    transaction_id: String,
    armed: bool,
    pause_reached: bool,
    intent_present: bool,
    recovery_ready: bool,
}

#[tokio::test]
async fn docker_restart_recovers_commit_after_root_register_q2_and_deadline() {
    let cluster = isolated_docker_test_cluster("post-root-register-q2", "test-region-1").await;
    let fixture = create_fixture(&cluster, "post-root-register-q2").await;
    let publisher = cluster.equal_peer(1);
    let object_key = "committed-before-publisher-crash";
    let content = b"root-register Q2 remains committed across publisher restart";
    let identity = MutationIdentity::unique(object_key);
    let transaction = begin_object_transaction(
        &publisher.grpc_addr,
        &fixture,
        &identity,
        EXPLICIT_TRANSACTION_TTL_MS,
    )
    .await;
    assert_eq!(transaction.state, "open");

    let control_client = reqwest::Client::new();
    let armed = arm_pause_after_q2(
        &control_client,
        &publisher.admin_addr,
        &transaction.transaction_id,
    )
    .await;
    assert!(armed.armed);
    assert!(!armed.pause_reached);
    assert!(!armed.intent_present);

    let staged = put_object_in_transaction_at(
        &publisher.grpc_addr,
        &fixture,
        object_key,
        content,
        &identity,
        &transaction.transaction_id,
    )
    .await
    .expect("stage object in explicit transaction");
    assert_eq!(
        WriteState::try_from(staged.write_state),
        Ok(WriteState::Staged)
    );

    let commit_endpoint = publisher.grpc_addr.clone();
    let commit_actor = fixture.actor.clone();
    let commit_transaction_id = transaction.transaction_id.clone();
    let mut commit = tokio::spawn(async move {
        commit_transaction_at(&commit_endpoint, &commit_actor, &commit_transaction_id).await
    });
    let paused = wait_for_pause_after_q2(
        &control_client,
        &publisher.admin_addr,
        &transaction.transaction_id,
        &mut commit,
        DISTRIBUTED_WAIT,
    )
    .await;
    assert!(!paused.armed);
    assert!(paused.pause_reached);
    assert!(
        paused.intent_present,
        "the durable publication intent must still exist at the post-Q2 crash point"
    );

    cluster.stop_node(1).await;
    let interrupted_commit = tokio::time::timeout(Duration::from_secs(20), commit)
        .await
        .expect("commit RPC did not finish after publisher crash")
        .expect("commit RPC task panicked");
    assert!(
        interrupted_commit.is_err(),
        "publisher acknowledged a commit while paused at the crash point"
    );

    wait_until_after(transaction.expires_at_unix_nanos).await;
    cluster.start_node(1).await;

    let recovered = publication_status(
        &control_client,
        &publisher.admin_addr,
        &transaction.transaction_id,
    )
    .await;
    assert_eq!(recovered.transaction_id, transaction.transaction_id);
    assert!(recovered.pause_reached);
    assert!(
        !recovered.intent_present,
        "startup recovery did not clear the committed publication intent"
    );
    assert!(
        recovered.recovery_ready,
        "publisher advertised test-control availability before CoreMeta recovery was ready"
    );
    assert_public_ready(&control_client, &publisher.grpc_addr).await;

    let committed = get_transaction_at(
        &publisher.grpc_addr,
        &fixture.actor,
        &transaction.transaction_id,
    )
    .await
    .expect("read recovered explicit transaction");
    assert_eq!(committed.state, "committed");
    assert!(committed.committed_root_generation.is_some());

    let recovered_content = get_object_at(&publisher.grpc_addr, &fixture, object_key)
        .await
        .expect("read object materialized by startup recovery");
    assert_eq!(recovered_content, content);
    let recovered_observation = cluster
        .head_object_at_peer(
            &fixture.actor,
            publisher.ordinal,
            &fixture.bucket_name,
            object_key,
        )
        .await
        .expect("observe object materialized by startup recovery");
    cluster
        .wait_for_all_peer_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &recovered_observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    let recovered_versions = list_object_versions_at(&publisher.grpc_addr, &fixture, object_key)
        .await
        .expect("list versions materialized by startup recovery");
    assert_exact_object_version(&recovered_versions, object_key, &staged.version_id);

    let retry = commit_transaction_at(
        &publisher.grpc_addr,
        &fixture.actor,
        &transaction.transaction_id,
    )
    .await
    .expect("retry committed transaction after recovery");
    assert_eq!(retry.mutation_id, transaction.transaction_id);
    assert_eq!(WriteState::try_from(retry.state), Ok(WriteState::Committed));
    let versions_after_retry = list_object_versions_at(&publisher.grpc_addr, &fixture, object_key)
        .await
        .expect("list versions after recovered commit retry");
    assert_exact_object_version(&versions_after_retry, object_key, &staged.version_id);
}

async fn begin_object_transaction(
    endpoint: &str,
    fixture: &DistributedFixture,
    identity: &MutationIdentity,
    ttl_ms: u64,
) -> BeginTransactionResponse {
    let root_anchor_key = hex::encode(anvil::metadata_journal::object_metadata_partition_id(
        fixture.actor.tenant_id,
        fixture.bucket_id,
    ));
    let mut client = TransactionServiceClient::connect(endpoint.to_string())
        .await
        .expect("connect transaction service for begin");
    let mut request = Request::new(BeginTransactionRequest {
        idempotency_key: identity.idempotency_key.clone(),
        scope: Some(TransactionScope {
            root_key_hash: anvil::core_store::CoreStore::root_key_hash_for_anchor(&root_anchor_key),
            root_anchor_key,
        }),
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        ttl_ms,
        purpose: "distributed post-root-register-Q2 recovery".to_string(),
    });
    add_actor_bearer(&mut request, &fixture.actor);
    client
        .begin_transaction(request)
        .await
        .expect("begin explicit object transaction")
        .into_inner()
}

async fn commit_transaction_at(
    endpoint: &str,
    actor: &DockerTestStorageActor,
    transaction_id: &str,
) -> Result<WriteResponse, Status> {
    let mut client = TransactionServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut request = Request::new(CommitTransactionRequest {
        transaction_id: transaction_id.to_string(),
        consistency: ConsistencyMode::Committed as i32,
        wait_for_finalization: false,
        final_preconditions: Vec::new(),
    });
    add_actor_bearer(&mut request, actor);
    client
        .commit_transaction(request)
        .await
        .map(tonic::Response::into_inner)
}

async fn get_transaction_at(
    endpoint: &str,
    actor: &DockerTestStorageActor,
    transaction_id: &str,
) -> Result<TransactionStatus, Status> {
    let mut client = TransactionServiceClient::connect(endpoint.to_string())
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut request = Request::new(GetTransactionRequest {
        transaction_id: transaction_id.to_string(),
    });
    add_actor_bearer(&mut request, actor);
    client
        .get_transaction(request)
        .await
        .map(tonic::Response::into_inner)
}

async fn arm_pause_after_q2(
    client: &reqwest::Client,
    admin_addr: &str,
    transaction_id: &str,
) -> RootPublicationStatus {
    let response = client
        .post(format!(
            "{admin_addr}/__anvil_test/root-publication/arm-after-q2"
        ))
        .header(TEST_CONTROL_TOKEN_HEADER, TEST_CONTROL_TOKEN)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::to_vec(&serde_json::json!({ "transaction_id": transaction_id }))
                .expect("serialize root-publication test arm request"),
        )
        .send()
        .await
        .expect("send root-publication test arm request");
    decode_control_response(response, "arm root-publication pause").await
}

async fn publication_status(
    client: &reqwest::Client,
    admin_addr: &str,
    transaction_id: &str,
) -> RootPublicationStatus {
    let response = client
        .get(format!(
            "{admin_addr}/__anvil_test/root-publication/status?transaction_id={transaction_id}"
        ))
        .header(TEST_CONTROL_TOKEN_HEADER, TEST_CONTROL_TOKEN)
        .send()
        .await
        .expect("send root-publication test status request");
    decode_control_response(response, "inspect root-publication status").await
}

async fn decode_control_response(
    response: reqwest::Response,
    operation: &str,
) -> RootPublicationStatus {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .unwrap_or_else(|error| panic!("{operation} response body: {error}"));
    assert!(
        status.is_success(),
        "{operation} failed with {status}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body)
        .unwrap_or_else(|error| panic!("decode {operation} response: {error}"))
}

async fn wait_for_pause_after_q2(
    client: &reqwest::Client,
    admin_addr: &str,
    transaction_id: &str,
    commit: &mut tokio::task::JoinHandle<Result<WriteResponse, Status>>,
    timeout: Duration,
) -> RootPublicationStatus {
    let deadline = Instant::now() + timeout;
    loop {
        let status = publication_status(client, admin_addr, transaction_id).await;
        if status.pause_reached {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "publisher did not reach the post-root-register-Q2 pause before timeout"
        );
        tokio::select! {
            result = &mut *commit => {
                panic!("publisher commit completed before the post-Q2 pause: {result:?}");
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

async fn wait_until_after(expires_at_unix_nanos: u64) {
    let now = unix_timestamp_nanos();
    let remaining = expires_at_unix_nanos.saturating_sub(now);
    tokio::time::sleep(Duration::from_nanos(remaining) + Duration::from_millis(100)).await;
    assert!(unix_timestamp_nanos() > expires_at_unix_nanos);
}

fn unix_timestamp_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after Unix epoch")
            .as_nanos(),
    )
    .expect("Unix timestamp fits u64")
}

async fn assert_public_ready(client: &reqwest::Client, endpoint: &str) {
    let response = client
        .get(format!("{endpoint}/ready"))
        .send()
        .await
        .expect("query recovered publisher readiness");
    let status = response.status();
    let body = response.text().await.expect("read readiness response");
    assert!(
        status.is_success(),
        "publisher is not ready: {status} {body}"
    );
    assert_eq!(body, "READY");
}

fn assert_exact_object_version(
    versions: &ListObjectVersionsResponse,
    object_key: &str,
    version_id: &str,
) {
    let matching = versions
        .versions
        .iter()
        .filter(|version| version.key == object_key)
        .collect::<Vec<_>>();
    assert_eq!(
        matching.len(),
        1,
        "recovered transaction must materialize exactly one object version"
    );
    assert_eq!(matching[0].version_id, version_id);
}

fn add_actor_bearer<T>(request: &mut Request<T>, actor: &DockerTestStorageActor) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token)
            .parse()
            .expect("actor bearer metadata is valid"),
    );
}
