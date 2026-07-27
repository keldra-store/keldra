#![recursion_limit = "256"]

use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, MvccDurability, MvccReadConsistency,
    PutBoundarySchemaRequest,
};
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil_test_utils::{TestCluster, isolated_test_cluster, unique_test_name};

fn authorized<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn begin(cluster: &TestCluster, label: &str) -> String {
    let mut transactions = TransactionServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    transactions
        .begin_transaction(authorized(
            tonic::Request::new(BeginTransactionRequest {
                idempotency_key: format!("{label}-{}", uuid::Uuid::new_v4()),
                ttl_ms: 30_000,
                consistency: MvccReadConsistency::Linearized as i32,
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

async fn put(
    cluster: &TestCluster,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    bucket_name: &str,
    mutation_id: &str,
    transaction_id: Option<String>,
) -> anvil::anvil_api::BoundarySchemaResponse {
    client
        .put_boundary_schema(authorized(
            tonic::Request::new(PutBoundarySchemaRequest {
                bucket_name: bucket_name.to_string(),
                expected_generation: None,
                dimensions: Vec::new(),
                mutation_id: mutation_id.to_string(),
                transaction_id,
            }),
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn implicit_boundary_schema_retry_reconstructs_the_committed_schema() {
    let cluster =
        isolated_test_cluster("boundary-schema-lost-response", &["test-region-1"]).await;
    let bucket_name = unique_test_name("boundary-retry");
    cluster.create_bucket(&bucket_name, "test-region-1").await;
    let mutation_id = uuid::Uuid::new_v4().to_string();
    let mut objects = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let first = put(
        &cluster,
        &mut objects,
        &bucket_name,
        &mutation_id,
        None,
    )
    .await;
    let retry = put(
        &cluster,
        &mut objects,
        &bucket_name,
        &mutation_id,
        None,
    )
    .await;
    assert_eq!(retry.schema, first.schema);
    assert_eq!(retry.schema.unwrap().generation, 1);
}

#[tokio::test]
async fn concurrent_boundary_schema_transactions_conflict() {
    let cluster = isolated_test_cluster("boundary-schema-conflict", &["test-region-1"]).await;
    let bucket_name = unique_test_name("boundary-conflict");
    cluster.create_bucket(&bucket_name, "test-region-1").await;
    let first = begin(&cluster, "boundary-first").await;
    let second = begin(&cluster, "boundary-second").await;
    let mut objects = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    put(
        &cluster,
        &mut objects,
        &bucket_name,
        "boundary-first",
        Some(first.clone()),
    )
    .await;
    put(
        &cluster,
        &mut objects,
        &bucket_name,
        "boundary-second",
        Some(second.clone()),
    )
    .await;

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
