#![recursion_limit = "512"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil::anvil_api::{
    BeginTransactionRequest, CommitTransactionRequest, CreateApplicationCredentialRequest,
    DeleteApplicationCredentialRequest, GetAccessTokenRequest, MvccDurability, MvccReadConsistency,
    RotateApplicationCredentialSecretRequest, WriteOptions, WriteState, write_options,
};
use anvil_test_utils::{ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, TestCluster, isolated_test_cluster};
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

#[tokio::test]
async fn credential_create_is_invisible_until_its_caller_transaction_commits() {
    let mut cluster = isolated_test_cluster("credential-transaction", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut transactions = TransactionServiceClient::connect(endpoint.clone())
        .await
        .unwrap();
    let mut auth = AuthServiceClient::connect(endpoint).await.unwrap();
    let transaction_id = begin_transaction(&cluster, &mut transactions, "credential-create").await;
    let app_name = format!("credential-tx-{}", uuid::Uuid::new_v4().simple());

    let staged = auth
        .create_application_credential(authorized(
            CreateApplicationCredentialRequest {
                app_name,
                request_id: "credential-create-request".to_string(),
                idempotency_key: "credential-create-stage".to_string(),
                options: Some(transaction_options(&transaction_id)),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(staged.write_state, WriteState::Staged as i32);

    let invisible = auth
        .get_access_token(Request::new(GetAccessTokenRequest {
            client_id: staged.client_id.clone(),
            client_secret: staged.client_secret.clone(),
        }))
        .await
        .unwrap_err();
    assert_eq!(invisible.code(), Code::Unauthenticated);

    transactions
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id,
                cluster_id: cluster.states[0].mvcc.cluster_id().to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap();

    auth.get_access_token(Request::new(GetAccessTokenRequest {
        client_id: staged.client_id,
        client_secret: staged.client_secret,
    }))
    .await
    .unwrap();
}

#[tokio::test]
async fn implicit_credential_retry_reconstructs_secret_and_rejects_changed_input() {
    let mut cluster = isolated_test_cluster("credential-idempotency", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut auth = AuthServiceClient::connect(endpoint).await.unwrap();
    let app_name = format!("credential-retry-{}", uuid::Uuid::new_v4().simple());
    let request_id = "credential-stable-response".to_string();
    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let request = || CreateApplicationCredentialRequest {
        app_name: app_name.clone(),
        request_id: request_id.clone(),
        idempotency_key: idempotency_key.clone(),
        options: None,
    };

    let first = auth
        .create_application_credential(authorized(request(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    let replay = auth
        .create_application_credential(authorized(request(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.client_id, replay.client_id);
    assert_eq!(first.client_secret, replay.client_secret);
    assert_eq!(first.audit_event_id, replay.audit_event_id);

    let changed = auth
        .create_application_credential(authorized(
            CreateApplicationCredentialRequest {
                app_name: format!("{app_name}-different"),
                request_id,
                idempotency_key,
                options: None,
            },
            &cluster.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(changed.code(), Code::AlreadyExists);
}

#[tokio::test]
async fn implicit_rotate_and_delete_retries_reconstruct_their_original_responses() {
    let mut cluster = isolated_test_cluster("credential-rotate-delete", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let endpoint = cluster.grpc_addrs[0].clone();
    let mut auth = AuthServiceClient::connect(endpoint).await.unwrap();
    let app_name = format!("credential-lifecycle-{}", uuid::Uuid::new_v4().simple());
    auth.create_application_credential(authorized(
        CreateApplicationCredentialRequest {
            app_name: app_name.clone(),
            request_id: "credential-lifecycle-create".to_string(),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            options: None,
        },
        &cluster.token,
    ))
    .await
    .unwrap();

    let rotate_key = uuid::Uuid::new_v4().to_string();
    let rotate = || RotateApplicationCredentialSecretRequest {
        app_name: app_name.clone(),
        request_id: "credential-lifecycle-rotate".to_string(),
        idempotency_key: rotate_key.clone(),
        options: None,
    };
    let first_rotate = auth
        .rotate_application_credential_secret(authorized(rotate(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    let replayed_rotate = auth
        .rotate_application_credential_secret(authorized(rotate(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_rotate.client_secret, replayed_rotate.client_secret);
    assert_eq!(first_rotate.audit_event_id, replayed_rotate.audit_event_id);

    let delete_key = uuid::Uuid::new_v4().to_string();
    let delete = || DeleteApplicationCredentialRequest {
        app_name: app_name.clone(),
        request_id: "credential-lifecycle-delete".to_string(),
        idempotency_key: delete_key.clone(),
        options: None,
    };
    let first_delete = auth
        .delete_application_credential(authorized(delete(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    let replayed_delete = auth
        .delete_application_credential(authorized(delete(), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_delete.app_id, replayed_delete.app_id);
    assert_eq!(replayed_delete.write_state, WriteState::Committed as i32);

    let changed_delete = auth
        .delete_application_credential(authorized(
            DeleteApplicationCredentialRequest {
                app_name: format!("{app_name}-different"),
                request_id: "credential-lifecycle-delete".to_string(),
                idempotency_key: delete_key,
                options: None,
            },
            &cluster.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(changed_delete.code(), Code::AlreadyExists);
}
