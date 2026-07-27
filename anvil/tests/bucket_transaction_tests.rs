#![recursion_limit = "512"]

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, CreateBucketRequest, DeleteBucketRequest,
    GetBucketPolicyRequest, ListBucketsRequest, MvccDurability, MvccReadConsistency,
    PutBucketPolicyRequest, WriteOptions, write_options,
};
use anvil::system_realm::{SYSTEM_BUCKET_NAMESPACE, SYSTEM_STORAGE_TENANT_ID};
use anvil_test_utils::{TestCluster, isolated_test_cluster};
use tonic::{Code, Request};

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

async fn begin_transaction(
    cluster: &TestCluster,
    transactions: &mut TransactionServiceClient<tonic::transport::Channel>,
    label: &str,
) -> String {
    transactions
        .begin_transaction(authorized(
            BeginTransactionRequest {
                idempotency_key: format!("{label}-{}", uuid::Uuid::new_v4()),
                ttl_ms: 30_000,
                read_consistency: MvccReadConsistency::Linearized as i32,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
                durability: MvccDurability::Quorum as i32,
            },
            &cluster.token,
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

fn implicit_options(idempotency_key: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: idempotency_key.to_string(),
        consistency: 0,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: None,
    }
}

async fn stage_bucket(
    buckets: &mut BucketServiceClient<tonic::transport::Channel>,
    token: &str,
    transaction_id: &str,
    bucket_name: &str,
) -> i64 {
    buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.to_string(),
                region: "test-region-1".to_string(),
                options: Some(transaction_options(transaction_id)),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id
}

async fn commit(
    cluster: &TestCluster,
    transactions: &mut TransactionServiceClient<tonic::transport::Channel>,
    transaction_id: String,
) -> Result<(), tonic::Status> {
    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
            },
            &cluster.token,
        ))
        .await
        .map(|_| ())
}

#[tokio::test]
async fn bucket_create_and_policy_use_one_transaction_overlay_and_publish_default_grants() {
    let cluster = isolated_test_cluster("bucket-transaction-overlay", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint).await.unwrap();
    let transaction_id = begin_transaction(&cluster, &mut transactions, "bucket-overlay").await;
    let bucket_name = format!("bucket-overlay-{}", uuid::Uuid::new_v4().simple());
    let bucket_id = stage_bucket(&mut buckets, &cluster.token, &transaction_id, &bucket_name).await;

    // This update must resolve the bucket staged above through the caller's
    // overlay. Reading only committed state would return NOT_FOUND here.
    buckets
        .put_bucket_policy(authorized(
            PutBucketPolicyRequest {
                bucket_name: bucket_name.clone(),
                policy_json: serde_json::json!({"is_public_read": true}).to_string(),
                options: Some(transaction_options(&transaction_id)),
            },
            &cluster.token,
        ))
        .await
        .unwrap();
    assert!(
        buckets
            .list_buckets(authorized(
                ListBucketsRequest { page: None },
                &cluster.token
            ))
            .await
            .unwrap()
            .into_inner()
            .buckets
            .iter()
            .all(|bucket| bucket.name != bucket_name),
        "staged bucket metadata must remain invisible before commit"
    );

    commit(&cluster, &mut transactions, transaction_id)
        .await
        .unwrap();
    let policy = buckets
        .get_bucket_policy(authorized(
            GetBucketPolicyRequest {
                bucket_name: bucket_name.clone(),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&policy.policy_json).unwrap()["is_public_read"],
        true
    );

    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("committed bucket");
    assert_eq!(bucket.id, bucket_id);
    let bucket_object_id = anvil::access_control::bucket_object_id(&bucket);
    let records = cluster.states[0]
        .persistence
        .list_authz_tuple_log(
            SYSTEM_STORAGE_TENANT_ID,
            0,
            &anvil::access_control::system_realm_namespace(SYSTEM_BUCKET_NAMESPACE),
            10_000,
        )
        .await
        .unwrap();
    assert!(records.iter().any(|record| {
        record.object_id == bucket_object_id
            && record.relation == "parent_tenant"
            && record.operation == "add"
    }));
    assert!(records.iter().any(|record| {
        record.object_id == bucket_object_id
            && record.relation == "owner"
            && record.operation == "add"
    }));
    assert!(records.iter().any(|record| {
        record.object_id == bucket_object_id
            && record.relation == "reader"
            && record.operation == "add"
    }));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if cluster.states[0]
                .persistence
                .get_mesh_bucket_locator(1, &bucket_name)
                .await
                .unwrap()
                .is_some_and(|locator| {
                    locator.bucket_id.as_str() == bucket_id.to_string()
                        && locator.status == anvil::mesh_directory::BucketLocatorStatus::Active
                })
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("committed bucket locator finalization completes");
}

#[tokio::test]
async fn bucket_name_conflict_aborts_every_bucket_mutation_in_losing_transaction() {
    let cluster = isolated_test_cluster("bucket-transaction-conflict", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut buckets = BucketServiceClient::connect(endpoint).await.unwrap();
    let first = begin_transaction(&cluster, &mut transactions, "bucket-first").await;
    let second = begin_transaction(&cluster, &mut transactions, "bucket-second").await;
    let shared = format!("shared-{}", uuid::Uuid::new_v4().simple());
    let first_only = format!("first-{}", uuid::Uuid::new_v4().simple());
    let second_only = format!("second-{}", uuid::Uuid::new_v4().simple());

    for name in [&shared, &first_only] {
        stage_bucket(&mut buckets, &cluster.token, &first, name).await;
    }
    for name in [&shared, &second_only] {
        stage_bucket(&mut buckets, &cluster.token, &second, name).await;
    }
    commit(&cluster, &mut transactions, first).await.unwrap();
    let conflict = commit(&cluster, &mut transactions, second)
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), Code::Aborted);

    let names = buckets
        .list_buckets(authorized(
            ListBucketsRequest { page: None },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .buckets
        .into_iter()
        .map(|bucket| bucket.name)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(names.contains(&shared));
    assert!(names.contains(&first_only));
    assert!(!names.contains(&second_only));
}

#[tokio::test]
async fn implicit_bucket_mutations_reconstruct_committed_outcomes_after_lost_responses() {
    let cluster = isolated_test_cluster("bucket-implicit-retry", &["test-region-1"]).await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut buckets = BucketServiceClient::connect(endpoint).await.unwrap();
    let bucket_name = format!("bucket-retry-{}", uuid::Uuid::new_v4().simple());
    let create_key = uuid::Uuid::new_v4().to_string();
    let create = CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
        options: Some(implicit_options(&create_key)),
    };
    let first = buckets
        .create_bucket(authorized(create.clone(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    let retry = buckets
        .create_bucket(authorized(create, &cluster.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry.bucket_id, first.bucket_id);
    let changed_input = buckets
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: format!("changed-{}", uuid::Uuid::new_v4().simple()),
                region: "test-region-1".to_string(),
                options: Some(implicit_options(&create_key)),
            },
            &cluster.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(changed_input.code(), Code::AlreadyExists);

    let policy_key = uuid::Uuid::new_v4().to_string();
    let policy = PutBucketPolicyRequest {
        bucket_name: bucket_name.clone(),
        policy_json: serde_json::json!({"is_public_read": true}).to_string(),
        options: Some(implicit_options(&policy_key)),
    };
    buckets
        .put_bucket_policy(authorized(policy.clone(), &cluster.token))
        .await
        .unwrap();
    buckets
        .put_bucket_policy(authorized(policy, &cluster.token))
        .await
        .unwrap();

    let delete_key = uuid::Uuid::new_v4().to_string();
    let delete = DeleteBucketRequest {
        bucket_name,
        options: Some(implicit_options(&delete_key)),
    };
    buckets
        .delete_bucket(authorized(delete.clone(), &cluster.token))
        .await
        .unwrap();
    buckets
        .delete_bucket(authorized(delete, &cluster.token))
        .await
        .unwrap();
}
