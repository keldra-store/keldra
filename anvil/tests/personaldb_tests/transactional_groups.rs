use super::*;

async fn begin_personaldb_transaction(
    client: &mut TransactionServiceClient<tonic::transport::Channel>,
    token: &str,
    cluster_id: &str,
    label: &str,
) -> String {
    client
        .begin_transaction(authorized(
            BeginTransactionRequest {
                idempotency_key: format!("{label}-{}", uuid::Uuid::new_v4()),
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

fn transaction_options(transaction_id: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        consistency: 0,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: Some(write_options::Execution::TransactionId(
            transaction_id.to_string(),
        )),
    }
}

fn transactional_group_request(database_id: &str, transaction_id: &str) -> CreatePersonalDbGroupRequest {
    CreatePersonalDbGroupRequest {
        database_id: database_id.to_string(),
        schema_hash: personaldb_test_schema_hash(),
        genesis_hash: hex::encode(hash32(format!("genesis:{database_id}").as_bytes())),
        schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
        options: Some(transaction_options(transaction_id)),
    }
}

#[tokio::test]
async fn bucket_and_personaldb_group_commit_atomically_in_one_public_transaction() {
    let cluster = isolated_test_cluster("bucket-personaldb-transaction", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(endpoint).await.unwrap();
    let transaction_id =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "mixed-success").await;
    let bucket_name = format!("mixed-{}", uuid::Uuid::new_v4().simple());
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());

    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
                options: Some(transaction_options(&transaction_id)),
            },
            &token,
        ))
        .await
        .unwrap();
    let staged = personaldb
        .create_personal_db_group(authorized(
            transactional_group_request(&database_id, &transaction_id),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(staged.write_state, anvil::anvil_api::WriteState::Staged as i32);
    assert!(
        buckets
            .list_buckets(authorized(
                ListBucketsRequest { page: None },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .buckets
            .iter()
            .all(|bucket| bucket.name != bucket_name)
    );

    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id,
                cluster_id,
            },
            &token,
        ))
        .await
        .unwrap();
    assert!(
        buckets
            .list_buckets(authorized(
                ListBucketsRequest { page: None },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .buckets
            .iter()
            .any(|bucket| bucket.name == bucket_name)
    );
    personaldb
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id,
            },
            &token,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn personaldb_conflict_aborts_the_other_transactions_bucket_write() {
    let cluster = isolated_test_cluster("bucket-personaldb-conflict", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(endpoint).await.unwrap();
    let first =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "mixed-first").await;
    let second =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "mixed-second").await;
    let database_id = format!("db-conflict-{}", uuid::Uuid::new_v4().simple());
    let first_bucket = format!("winner-{}", uuid::Uuid::new_v4().simple());
    let second_bucket = format!("loser-{}", uuid::Uuid::new_v4().simple());

    for (transaction_id, bucket_name) in [(&first, &first_bucket), (&second, &second_bucket)] {
        buckets
            .create_bucket(authorized(
                CreateBucketRequest {
                    bucket_name: bucket_name.clone(),
                    region: "test-region-1".to_string(),
                    options: Some(transaction_options(transaction_id)),
                },
                &token,
            ))
            .await
            .unwrap();
        personaldb
            .create_personal_db_group(authorized(
                transactional_group_request(&database_id, transaction_id),
                &token,
            ))
            .await
            .unwrap();
    }
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
    assert_eq!(conflict.code(), tonic::Code::Aborted);

    let names = buckets
        .list_buckets(authorized(ListBucketsRequest { page: None }, &token))
        .await
        .unwrap()
        .into_inner()
        .buckets
        .into_iter()
        .map(|bucket| bucket.name)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(names.contains(&first_bucket));
    assert!(!names.contains(&second_bucket));
}
