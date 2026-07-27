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

#[tokio::test]
async fn bucket_and_personaldb_submit_commit_atomically_and_conflict_as_one_bundle() {
    let cluster = isolated_test_cluster("bucket-personaldb-submit", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(endpoint).await.unwrap();
    let database_id = format!("db-submit-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut personaldb, &token, &database_id).await;
    let first =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "submit-first").await;
    let second =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "submit-second").await;
    let first_bucket = format!("submit-winner-{}", uuid::Uuid::new_v4().simple());
    let second_bucket = format!("submit-loser-{}", uuid::Uuid::new_v4().simple());

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
        let mut submit = valid_submit_request(&database_id, &genesis_hash, &token);
        submit.options = Some(transaction_options(transaction_id));
        let staged = personaldb
            .submit_personal_db_changeset(authorized(submit, &token))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(staged.write_state, anvil::anvil_api::WriteState::Staged as i32);
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
    let group = personaldb
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(group.committed_head.unwrap().log_index, 1);
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

async fn create_projection_groups(
    client: &mut PersonalDbServiceClient<tonic::transport::Channel>,
    token: &str,
    label: &str,
) -> (String, String) {
    let source = format!("{label}-source-{}", uuid::Uuid::new_v4().simple());
    let target = format!("{label}-target-{}", uuid::Uuid::new_v4().simple());
    create_group(client, token, &source).await;
    create_group_with_schema(
        client,
        token,
        &target,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;
    (source, target)
}

fn transactional_projection_request(
    source_database_id: &str,
    projection_database_id: &str,
    transaction_id: &str,
) -> CreatePersonalDbProjectionRequest {
    CreatePersonalDbProjectionRequest {
        tenant_id: 1,
        database_id: projection_database_id.to_string(),
        projection_definition_json: serde_json::to_string(&projection_definition(
            projection_database_id,
            source_database_id,
        ))
        .unwrap(),
        options: Some(transaction_options(transaction_id)),
    }
}

#[tokio::test]
async fn personaldb_projection_definition_is_invisible_until_transaction_commit() {
    let cluster = isolated_test_cluster("personaldb-projection-transaction", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(endpoint).await.unwrap();
    let source = format!("projection-atomic-source-{}", uuid::Uuid::new_v4().simple());
    let target = format!("projection-atomic-target-{}", uuid::Uuid::new_v4().simple());
    let transaction_id =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "projection").await;

    personaldb
        .create_personal_db_group(authorized(
            transactional_group_request(&source, &transaction_id),
            &token,
        ))
        .await
        .unwrap();
    personaldb
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: target.clone(),
                schema_hash: personaldb_projection_test_schema_hash(),
                genesis_hash: hex::encode(hash32(format!("genesis:{target}").as_bytes())),
                schema_sql: PERSONALDB_PROJECTION_TEST_SCHEMA_SQL.to_string(),
                options: Some(transaction_options(&transaction_id)),
            },
            &token,
        ))
        .await
        .unwrap();
    let staged = personaldb
        .create_personal_db_projection(authorized(
            transactional_projection_request(&source, &target, &transaction_id),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(staged.write_state, anvil::anvil_api::WriteState::Staged as i32);
    let invisible = personaldb
        .get_personal_db_projection(authorized(
            GetPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: target.clone(),
                projection_id: "projection-items".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invisible.code(), tonic::Code::NotFound);

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
    personaldb
        .get_personal_db_projection(authorized(
            GetPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: target,
                projection_id: "projection-items".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn projection_conflict_aborts_an_unrelated_bucket_in_the_losing_transaction() {
    let cluster = isolated_test_cluster("personaldb-projection-conflict", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone()).await.unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(endpoint).await.unwrap();
    let (source, target) =
        create_projection_groups(&mut personaldb, &token, "projection-conflict").await;
    let first =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "projection-first")
            .await;
    let second =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "projection-second")
            .await;
    let losing_bucket = format!("projection-loser-{}", uuid::Uuid::new_v4().simple());

    for transaction_id in [&first, &second] {
        personaldb
            .create_personal_db_projection(authorized(
                transactional_projection_request(&source, &target, transaction_id),
                &token,
            ))
            .await
            .unwrap();
    }
    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: losing_bucket.clone(),
                region: "test-region-1".to_string(),
                options: Some(transaction_options(&second)),
            },
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
    assert_eq!(conflict.code(), tonic::Code::Aborted);
    assert!(
        buckets
            .list_buckets(authorized(ListBucketsRequest { page: None }, &token))
            .await
            .unwrap()
            .into_inner()
            .buckets
            .iter()
            .all(|bucket| bucket.name != losing_bucket)
    );
}
