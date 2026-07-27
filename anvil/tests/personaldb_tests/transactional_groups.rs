use super::*;

async fn create_transactional_personaldb_actor(
    cluster: &TestCluster,
    label: &str,
) -> TestStorageActor {
    create_storage_test_actor(cluster, label).await
}

fn valid_submit_request_for_transactional_actor(
    actor: &TestStorageActor,
    database_id: &str,
    genesis_hash: &str,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_transactional_actor(
        actor,
        database_id,
        0,
        genesis_hash,
        sqlite_insert_changeset(),
    )
}

fn submit_request_at_base_for_transactional_actor(
    actor: &TestStorageActor,
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base_for_tenant_and_principal(
        actor.tenant_id,
        database_id,
        base_log_index,
        base_log_hash,
        &actor.app_id,
        &actor.token,
        changeset_bytes,
    )
}

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

fn transactional_group_request(
    database_id: &str,
    transaction_id: &str,
) -> CreatePersonalDbGroupRequest {
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
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
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
    assert_eq!(
        staged.write_state,
        anvil::anvil_api::WriteState::Staged as i32
    );
    assert!(
        buckets
            .list_buckets(authorized(ListBucketsRequest { page: None }, &token,))
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
            .list_buckets(authorized(ListBucketsRequest { page: None }, &token,))
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
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
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
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
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
        assert_eq!(
            staged.write_state,
            anvil::anvil_api::WriteState::Staged as i32
        );
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
    let cluster =
        isolated_test_cluster("personaldb-projection-transaction", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
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
    assert_eq!(
        staged.write_state,
        anvil::anvil_api::WriteState::Staged as i32
    );
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
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
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

#[tokio::test]
async fn projection_writeback_stages_source_and_target_heads_in_one_transaction() {
    let cluster =
        isolated_test_cluster("projection-writeback-transaction", &["test-region-1"]).await;
    let actor =
        create_transactional_personaldb_actor(&cluster, "projection-writeback-transaction").await;
    let token = actor.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source = format!("writeback-source-{}", uuid::Uuid::new_v4().simple());
    let target = format!("writeback-target-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut personaldb, &token, &source).await;
    create_group_with_schema(
        &mut personaldb,
        &token,
        &target,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;
    let definition =
        projection_definition_allowing_name_writeback_for_tenant(actor.tenant_id, &target, &source);
    personaldb
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: target.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    personaldb
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_transactional_actor(&actor, &source, &source_genesis),
            &token,
        ))
        .await
        .unwrap();
    let target_head = personaldb
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: target.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .unwrap();
    assert_eq!(target_head.log_index, 1);
    let transaction_id = begin_personaldb_transaction(
        &mut transactions,
        &token,
        &cluster_id,
        "projection-writeback",
    )
    .await;
    let mut request = submit_request_at_base_for_transactional_actor(
        &actor,
        &target,
        target_head.log_index,
        &target_head.log_hash,
        sqlite_projection_update_changeset(),
    );
    request.options = Some(transaction_options(&transaction_id));
    let staged = personaldb
        .submit_personal_db_changeset(authorized(request.clone(), &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        staged.write_state,
        anvil::anvil_api::WriteState::Staged as i32
    );
    let retry = personaldb
        .submit_personal_db_changeset(authorized(request, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retry.write_state,
        anvil::anvil_api::WriteState::Staged as i32
    );
    for database_id in [&source, &target] {
        let head = personaldb
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: actor.tenant_id,
                    database_id: database_id.clone(),
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .committed_head
            .unwrap();
        assert_eq!(head.log_index, 1);
    }

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
    for database_id in [&source, &target] {
        let head = personaldb
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: actor.tenant_id,
                    database_id: database_id.clone(),
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .committed_head
            .unwrap();
        assert_eq!(head.log_index, 2);
    }
}

#[tokio::test]
async fn projection_writeback_conflict_aborts_both_groups_and_unrelated_writes() {
    let cluster = isolated_test_cluster("projection-writeback-conflict", &["test-region-1"]).await;
    let actor =
        create_transactional_personaldb_actor(&cluster, "projection-writeback-conflict").await;
    let token = actor.token.clone();
    let cluster_id = cluster.states[0].mvcc.cluster_id().to_string();
    let mut transactions = TransactionServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut personaldb = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source = format!(
        "writeback-conflict-source-{}",
        uuid::Uuid::new_v4().simple()
    );
    let target = format!(
        "writeback-conflict-target-{}",
        uuid::Uuid::new_v4().simple()
    );
    let source_genesis = create_group(&mut personaldb, &token, &source).await;
    create_group_with_schema(
        &mut personaldb,
        &token,
        &target,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;
    let definition =
        projection_definition_allowing_name_writeback_for_tenant(actor.tenant_id, &target, &source);
    personaldb
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: target.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    personaldb
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_transactional_actor(&actor, &source, &source_genesis),
            &token,
        ))
        .await
        .unwrap();
    let target_head = personaldb
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: target.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .unwrap();
    let first =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "writeback-first")
            .await;
    let second =
        begin_personaldb_transaction(&mut transactions, &token, &cluster_id, "writeback-second")
            .await;
    let losing_bucket = format!("writeback-loser-{}", uuid::Uuid::new_v4().simple());
    for transaction_id in [&first, &second] {
        let mut request = submit_request_at_base_for_transactional_actor(
            &actor,
            &target,
            target_head.log_index,
            &target_head.log_hash,
            sqlite_projection_update_changeset(),
        );
        request.options = Some(transaction_options(transaction_id));
        personaldb
            .submit_personal_db_changeset(authorized(request, &token))
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
    for database_id in [&source, &target] {
        let head = personaldb
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: actor.tenant_id,
                    database_id: database_id.clone(),
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .committed_head
            .unwrap();
        assert_eq!(head.log_index, 2);
    }
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
