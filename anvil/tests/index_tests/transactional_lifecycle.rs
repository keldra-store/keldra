use super::*;
use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, MvccDurability, MvccReadConsistency,
    WriteOptions, write_options,
};
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use tonic::Code;

async fn begin_index_transaction(
    client: &mut TransactionServiceClient<tonic::transport::Channel>,
    token: &str,
    cluster_id: &str,
    label: &str,
) -> String {
    client
        .begin_transaction(authorized(
            BeginTransactionRequest {
                idempotency_key: unique_test_name(label),
                ttl_ms: 30_000,
                read_consistency: MvccReadConsistency::Linearized as i32,
                cluster_id: cluster_id.to_string(),
                durability: MvccDurability::Quorum as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .transaction_id
}

fn transactional_index_request(
    bucket_name: &str,
    index_name: &str,
    transaction_id: String,
) -> CreateIndexRequest {
    CreateIndexRequest {
        bucket_name: bucket_name.to_string(),
        name: index_name.to_string(),
        kind: IndexKind::Path as i32,
        selector_json: serde_json::json!({"prefix": ""}).to_string(),
        extractor_json: serde_json::json!({}).to_string(),
        authorization_mode: "inherit_object".to_string(),
        build_policy_json: serde_json::json!({}).to_string(),
        options: Some(WriteOptions {
            idempotency_key: unique_test_name("index-stage"),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: Some(write_options::Execution::TransactionId(transaction_id)),
        }),
    }
}

fn implicit_index_request(
    bucket_name: &str,
    index_name: &str,
    idempotency_key: &str,
) -> CreateIndexRequest {
    let mut request = transactional_index_request(bucket_name, index_name, String::new());
    request.options = Some(WriteOptions {
        idempotency_key: idempotency_key.to_string(),
        consistency: 0,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: None,
    });
    request
}

#[tokio::test]
async fn explicit_index_transaction_publishes_definition_and_finalises_after_commit() {
    let cluster = shared_default_test_cluster().await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut indexes = IndexServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut transactions = TransactionServiceClient::connect(endpoint).await.unwrap();
    let bucket_name = unique_test_name("transactional-index-bucket");
    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let transaction_id =
        begin_index_transaction(&mut transactions, &token, &cluster_id, "index-success").await;
    let index_name = unique_test_name("transactional-index");
    let index = indexes
        .create_index(authorized(
            transactional_index_request(&bucket_name, &index_name, transaction_id.clone()),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("staged index definition");
    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction_id.clone(),
                cluster_id: cluster_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap();
    drop(transactions);
    indexes
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: index_name,
                selector_json: serde_json::json!({"prefix": "committed/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .expect("committed transactional index grants creator ownership");
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("committed index bucket");
    wait_for_index_builds_for_indexes(
        &cluster,
        INDEX_EVENTUAL_CONSISTENCY_TIMEOUT,
        1,
        bucket.id,
        &[index.id],
    )
    .await;
}

#[tokio::test]
async fn conflicting_explicit_index_transactions_publish_only_one_definition() {
    let cluster = shared_default_test_cluster().await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut indexes = IndexServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut transactions = TransactionServiceClient::connect(endpoint).await.unwrap();
    let bucket_name = unique_test_name("conflicting-index-bucket");
    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let first = begin_index_transaction(&mut transactions, &token, &cluster_id, "index-first").await;
    let second =
        begin_index_transaction(&mut transactions, &token, &cluster_id, "index-second").await;
    let index_name = unique_test_name("conflicting-index");
    indexes
        .create_index(authorized(
            transactional_index_request(&bucket_name, &index_name, first.clone()),
            &token,
        ))
        .await
        .unwrap();
    indexes
        .create_index(authorized(
            transactional_index_request(&bucket_name, &index_name, second.clone()),
            &token,
        ))
        .await
        .unwrap();
    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: first,
                cluster_id: cluster_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap();
    let conflict = transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: second,
                cluster_id,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), Code::Aborted);
}

#[tokio::test]
async fn implicit_index_retry_reconstructs_the_committed_definition() {
    let cluster = shared_default_test_cluster().await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut indexes = IndexServiceClient::connect(endpoint).await.unwrap();
    let bucket_name = unique_test_name("implicit-index-bucket");
    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let index_name = unique_test_name("implicit-index");
    let idempotency_key = unique_test_name("implicit-index-key");
    let request = implicit_index_request(&bucket_name, &index_name, &idempotency_key);
    let first = indexes
        .create_index(authorized(request.clone(), &token))
        .await
        .unwrap()
        .into_inner();
    let retry = indexes
        .create_index(authorized(request, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry, first);

    let changed = indexes
        .create_index(authorized(
            implicit_index_request(
                &bucket_name,
                &unique_test_name("changed-index"),
                &idempotency_key,
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(changed.code(), Code::AlreadyExists);

    let update_key = unique_test_name("implicit-index-update");
    let update = UpdateIndexRequest {
        bucket_name: bucket_name.clone(),
        name: index_name.clone(),
        selector_json: serde_json::json!({"prefix": "updated/"}).to_string(),
        extractor_json: serde_json::json!({}).to_string(),
        authorization_mode: "inherit_object".to_string(),
        build_policy_json: serde_json::json!({}).to_string(),
        options: Some(WriteOptions {
            idempotency_key: update_key,
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: None,
        }),
    };
    let first_update = indexes
        .update_index(authorized(update.clone(), &token))
        .await
        .unwrap()
        .into_inner();
    let retry_update = indexes
        .update_index(authorized(update, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry_update, first_update);

    let disable = DisableIndexRequest {
        bucket_name: bucket_name.clone(),
        name: index_name.clone(),
        options: Some(WriteOptions {
            idempotency_key: unique_test_name("implicit-index-disable"),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: None,
        }),
    };
    let first_disable = indexes
        .disable_index(authorized(disable.clone(), &token))
        .await
        .unwrap()
        .into_inner();
    let retry_disable = indexes
        .disable_index(authorized(disable, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry_disable, first_disable);

    let drop_request = DropIndexRequest {
        bucket_name,
        name: index_name,
        options: Some(WriteOptions {
            idempotency_key: unique_test_name("implicit-index-drop"),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: None,
        }),
    };
    indexes
        .drop_index(authorized(drop_request.clone(), &token))
        .await
        .unwrap();
    indexes
        .drop_index(authorized(drop_request, &token))
        .await
        .unwrap();
}
