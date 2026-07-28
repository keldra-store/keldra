#![recursion_limit = "256"]

use anvil::anvil_api::registry_service_client::RegistryServiceClient;
use anvil::anvil_api::{PutPackageBlobRequest, WriteOptions};
use anvil_test_utils::{
    ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT, isolated_test_cluster, unique_test_name,
};
use sha2::{Digest, Sha256};

fn authorized<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[tokio::test]
async fn implicit_registry_blob_retry_reconstructs_the_committed_outcome() {
    let mut cluster = isolated_test_cluster("registry-implicit-retry", &["test-region-1"]).await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;
    let mut registry = RegistryServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let body = b"registry transaction body".to_vec();
    let digest = format!("sha256:{:x}", Sha256::digest(&body));
    let request = PutPackageBlobRequest {
        registry_kind: "oci".to_string(),
        namespace: unique_test_name("registry"),
        digest,
        inline_body: body,
        media_type: "application/octet-stream".to_string(),
        options: Some(WriteOptions {
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: None,
        }),
    };
    let first = registry
        .put_package_blob(authorized(
            tonic::Request::new(request.clone()),
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner();
    let retry = registry
        .put_package_blob(authorized(tonic::Request::new(request), &cluster.token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry.mutation_id, first.mutation_id);
    assert_eq!(retry.state, first.state);
    assert_eq!(retry.idempotency_outcome, first.idempotency_outcome);
}
