#![recursion_limit = "256"]

use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, CreateHfKeyRequest, MvccDurability,
    MvccReadConsistency, WriteOptions, WriteState, write_options,
};
use anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil_test_utils::{TestCluster, isolated_test_cluster, unique_test_name};

fn authorized<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn begin(cluster: &TestCluster, label: &str) -> String {
    let mut client = TransactionServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    client
        .begin_transaction(authorized(
            tonic::Request::new(BeginTransactionRequest {
                idempotency_key: format!("{label}-{}", uuid::Uuid::new_v4()),
                ttl_ms: 30_000,
                read_consistency: MvccReadConsistency::Linearized as i32,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
                durability: MvccDurability::Quorum as i32,
            }),
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .transaction_id
}

fn options(transaction_id: Option<&str>, idempotency_key: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: idempotency_key.to_string(),
        consistency: 0,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: transaction_id.map(|id| {
            write_options::Execution::TransactionId(id.to_string())
        }),
    }
}

async fn create_key(
    cluster: &TestCluster,
    client: &mut HuggingFaceKeyServiceClient<tonic::transport::Channel>,
    name: &str,
    options: WriteOptions,
) -> anvil::anvil_api::CreateHfKeyResponse {
    client
        .create_key(authorized(
            tonic::Request::new(CreateHfKeyRequest {
                name: name.to_string(),
                token: "hf-test-token".to_string(),
                note: "transaction test".to_string(),
                options: Some(options),
            }),
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn hf_key_writes_stage_and_conflicting_transactions_abort() {
    let cluster = isolated_test_cluster("hf-key-transactions", &["test-region-1"]).await;
    let key_name = unique_test_name("hf-key-conflict");
    let first = begin(&cluster, "hf-first").await;
    let second = begin(&cluster, "hf-second").await;
    let mut keys = HuggingFaceKeyServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    assert_eq!(
        create_key(&cluster, &mut keys, &key_name, options(Some(&first), "first"))
            .await
            .write_state,
        WriteState::Staged as i32
    );
    assert_eq!(
        create_key(
            &cluster,
            &mut keys,
            &key_name,
            options(Some(&second), "second"),
        )
        .await
        .write_state,
        WriteState::Staged as i32
    );

    let mut transactions = TransactionServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    transactions
        .commit_transaction(authorized(
            tonic::Request::new(CommitTransactionRequest {
                transaction_id: first,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
            }),
            &cluster.token,
        ))
        .await
        .unwrap();
    let conflict = transactions
        .commit_transaction(authorized(
            tonic::Request::new(CommitTransactionRequest {
                transaction_id: second,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
            }),
            &cluster.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::Aborted);
}

#[tokio::test]
async fn hf_key_implicit_retry_reconstructs_the_committed_response() {
    let cluster = isolated_test_cluster("hf-key-lost-response", &["test-region-1"]).await;
    let key_name = unique_test_name("hf-key-retry");
    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let mut keys = HuggingFaceKeyServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let first = create_key(
        &cluster,
        &mut keys,
        &key_name,
        options(None, &idempotency_key),
    )
    .await;
    let retry = create_key(
        &cluster,
        &mut keys,
        &key_name,
        options(None, &idempotency_key),
    )
    .await;
    assert_eq!(retry, first);
    assert_eq!(retry.write_state, WriteState::Committed as i32);
}
