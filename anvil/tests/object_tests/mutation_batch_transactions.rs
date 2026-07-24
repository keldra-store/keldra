use super::*;

use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use anvil::anvil_api::{
    BeginTransactionRequest, BeginTransactionResponse, CommitTransactionRequest, ConsistencyMode,
    GetTransactionRequest, ReadConsistency, RollbackTransactionRequest, TransactionScope,
    WriteState,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const EXPLICIT_TRANSACTION_TTL_MS: u64 = 30_000;
const SMALL_PUT_COUNT: usize = 20;
const SMALL_BATCH_DEADLINE: Duration = Duration::from_secs(10);

struct SingleNodeMutationBatchFixture {
    _cluster: TestCluster,
    actor: ObjectTestActor,
    bucket_name: String,
    bucket_id: i64,
}

impl SingleNodeMutationBatchFixture {
    async fn new(label: &str) -> Self {
        let mut cluster = isolated_test_cluster(
            "exercises explicit MutationBatch semantics on a local single-node topology",
            &["test-region-1"],
        )
        .await;
        cluster
            .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
            .await;

        let actor = create_object_test_actor(&cluster, label).await;
        let bucket_name = unique_test_name("tx-mutation-batch");
        let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
            .await
            .expect("connect bucket service");
        let bucket_id = bucket_client
            .create_bucket(authorized(
                CreateBucketRequest {
                    bucket_name: bucket_name.clone(),
                    region: actor.region.clone(),
                    options: None,
                },
                &actor.token,
            ))
            .await
            .expect("create transaction test bucket")
            .into_inner()
            .bucket_id;

        Self {
            _cluster: cluster,
            actor,
            bucket_name,
            bucket_id,
        }
    }

    async fn object_client(&self) -> ObjectServiceClient<tonic::transport::Channel> {
        ObjectServiceClient::connect(self.actor.grpc_addr.clone())
            .await
            .expect("connect object service")
    }

    async fn transaction_client(&self) -> TransactionServiceClient<tonic::transport::Channel> {
        TransactionServiceClient::connect(self.actor.grpc_addr.clone())
            .await
            .expect("connect transaction service")
    }

    async fn coordination_client(&self) -> CoordinationServiceClient<tonic::transport::Channel> {
        CoordinationServiceClient::connect(self.actor.grpc_addr.clone())
            .await
            .expect("connect coordination service")
    }

    async fn begin_transaction(
        &self,
        client: &mut TransactionServiceClient<tonic::transport::Channel>,
        tag: &str,
        ttl_ms: u64,
    ) -> BeginTransactionResponse {
        self.begin_transaction_as(client, tag, ttl_ms, &self.actor.token)
            .await
    }

    async fn begin_transaction_as(
        &self,
        client: &mut TransactionServiceClient<tonic::transport::Channel>,
        tag: &str,
        ttl_ms: u64,
        token: &str,
    ) -> BeginTransactionResponse {
        let root_anchor_key = hex::encode(anvil::metadata_journal::object_metadata_partition_id(
            self.actor.tenant_id,
            self.bucket_id,
        ));
        let response = client
            .begin_transaction(authorized(
                BeginTransactionRequest {
                    idempotency_key: format!("{tag}-{}", uuid::Uuid::new_v4()),
                    scope: Some(TransactionScope {
                        root_key_hash: anvil::core_store::CoreStore::root_key_hash_for_anchor(
                            &root_anchor_key,
                        ),
                        root_anchor_key,
                    }),
                    preconditions: Vec::new(),
                    boundary_values: Vec::new(),
                    ttl_ms,
                    purpose: tag.to_string(),
                },
                token,
            ))
            .await
            .expect("begin explicit object transaction")
            .into_inner();
        assert_eq!(response.state, "open");
        response
    }

    fn mutation_context(&self, tag: &str, transaction_id: &str) -> NativeMutationContext {
        self.mutation_context_as(tag, transaction_id, &self.actor.app_id)
    }

    fn mutation_context_as(
        &self,
        tag: &str,
        transaction_id: &str,
        principal: &str,
    ) -> NativeMutationContext {
        let mut context = native_mutation_context(&self.actor, self.bucket_id, tag);
        context.principal = principal.to_string();
        context.transaction_id = Some(transaction_id.to_string());
        context.write_visibility = None;
        context
    }
}

fn small_put(object_key: impl Into<String>, payload: impl Into<Vec<u8>>) -> MutationBatchOperation {
    MutationBatchOperation {
        op: Some(anvil_api::mutation_batch_operation::Op::PutObject(
            MutationBatchPutObject {
                object_key: object_key.into(),
                payload: payload.into(),
                content_type: Some("application/json".to_string()),
                user_metadata_json: "{}".to_string(),
                storage_class: None,
            },
        )),
    }
}

fn published_watch_visibility() -> WriteVisibilityOptions {
    WriteVisibilityOptions {
        watches: 1,
        ..Default::default()
    }
}

async fn list_keys(
    fixture: &SingleNodeMutationBatchFixture,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    prefix: &str,
    consistency: Option<ReadConsistency>,
) -> Vec<String> {
    client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name: fixture.bucket_name.clone(),
                prefix: prefix.to_string(),
                delimiter: String::new(),
                start_after: String::new(),
                max_keys: 100,
                consistency,
                page_token: String::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("list transaction test objects")
        .into_inner()
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect()
}

async fn assert_object_missing(
    fixture: &SingleNodeMutationBatchFixture,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    object_key: &str,
) {
    let error = client
        .head_object(authorized(
            HeadObjectRequest {
                bucket_name: fixture.bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                consistency: None,
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("object must be absent");
    assert_eq!(error.code(), Code::NotFound);
}

async fn watch_prefix(
    fixture: &SingleNodeMutationBatchFixture,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    prefix: &str,
    after_cursor: u64,
) -> tonic::Streaming<anvil_api::WatchPrefixResponse> {
    client
        .watch_prefix(authorized(
            WatchPrefixRequest {
                bucket_name: fixture.bucket_name.clone(),
                prefix: prefix.to_string(),
                after_cursor,
            },
            &fixture.actor.token,
        ))
        .await
        .expect("open object-prefix watch")
        .into_inner()
}

async fn next_watch_event(
    stream: &mut tonic::Streaming<anvil_api::WatchPrefixResponse>,
) -> anvil_api::WatchPrefixResponse {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("watch event must arrive before timeout")
        .expect("watch stream must remain open")
        .expect("watch event must be valid")
}

fn at_root_generation(generation: u64) -> Option<ReadConsistency> {
    Some(ReadConsistency {
        mode: Some(anvil_api::read_consistency::Mode::AtRootGeneration(
            generation,
        )),
    })
}

fn unix_time_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after Unix epoch")
            .as_nanos(),
    )
    .expect("Unix timestamp fits u64")
}

async fn wait_until_expired(expires_at_unix_nanos: u64) {
    let remaining = expires_at_unix_nanos.saturating_sub(unix_time_nanos());
    tokio::time::sleep(Duration::from_nanos(remaining.saturating_add(5_000_000))).await;
}

#[tokio::test]
async fn mutation_batch_preserves_same_key_put_order() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-same-key-order").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "same-key ordered puts",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let object_key = "ordered/same-key.json";
    let first_payload = br#"{"step":1}"#.to_vec();
    let final_payload = br#"{"step":2}"#.to_vec();

    let response = tokio::time::timeout(
        SMALL_BATCH_DEADLINE,
        object_client.mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("same-key-order", &transaction.transaction_id),
                ),
                precondition: None,
                operations: vec![
                    small_put(object_key, first_payload.clone()),
                    small_put(object_key, final_payload.clone()),
                ],
            },
            &fixture.actor.token,
        )),
    )
    .await
    .expect("same-key MutationBatch must not wait on its own target lock")
    .expect("stage same-key MutationBatch")
    .into_inner();

    assert_eq!(response.write_state, WriteState::Staged as i32);
    assert_eq!(response.operation_receipts.len(), 2);
    assert_eq!(
        response
            .operation_receipts
            .iter()
            .map(|receipt| (receipt.operation.as_str(), receipt.object_key.as_str()))
            .collect::<Vec<_>>(),
        vec![("put_object", object_key), ("put_object", object_key)]
    );
    let first_receipt = &response.operation_receipts[0];
    let final_receipt = &response.operation_receipts[1];
    assert_ne!(first_receipt, final_receipt);
    assert_ne!(
        first_receipt.version_id, final_receipt.version_id,
        "each ordered write must have a distinct version receipt"
    );
    assert_ne!(
        first_receipt.mutation_id, final_receipt.mutation_id,
        "each ordered write must have a distinct mutation receipt"
    );
    assert_ne!(
        first_receipt.payload_hash, final_receipt.payload_hash,
        "different ordered payloads must have distinct payload receipts"
    );
    assert_object_missing(&fixture, &mut object_client, object_key).await;
    assert!(
        list_keys(&fixture, &mut object_client, "ordered/", None)
            .await
            .is_empty(),
        "same-key writes must remain invisible while their transaction is open"
    );

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("commit same-key MutationBatch transaction")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);

    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            Some(first_receipt.version_id.clone()),
        )
        .await,
        first_payload,
        "the first receipt must identify the first requested payload"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            Some(final_receipt.version_id.clone()),
        )
        .await,
        final_payload,
        "the second receipt must identify the second requested payload"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            None,
        )
        .await,
        final_payload
    );
}

#[tokio::test]
async fn implicit_mutation_batch_preserves_same_key_put_order() {
    let fixture = SingleNodeMutationBatchFixture::new("implicit-batch-same-key-order").await;
    let mut object_client = fixture.object_client().await;
    let object_key = "implicit-ordered/same-key.json";
    let first_payload = br#"{"step":1}"#.to_vec();
    let final_payload = br#"{"step":2}"#.to_vec();
    let mut context =
        native_mutation_context(&fixture.actor, fixture.bucket_id, "implicit-same-key-order");
    context.write_visibility = Some(published_watch_visibility());

    let response = tokio::time::timeout(
        SMALL_BATCH_DEADLINE,
        object_client.mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(context),
                precondition: None,
                operations: vec![
                    small_put(object_key, first_payload.clone()),
                    small_put(object_key, final_payload.clone()),
                ],
            },
            &fixture.actor.token,
        )),
    )
    .await
    .expect("implicit same-key MutationBatch must not wait on its own target lock")
    .expect("apply implicit same-key MutationBatch")
    .into_inner();

    assert_eq!(response.write_state, WriteState::Finalised as i32);
    assert_eq!(response.operation_receipts.len(), 2);
    assert!(
        response.watch_cursor > 0,
        "implicit committed batch must return its durable watch cursor"
    );
    let first_receipt = &response.operation_receipts[0];
    let final_receipt = &response.operation_receipts[1];
    assert_ne!(first_receipt.version_id, final_receipt.version_id);
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            Some(first_receipt.version_id.clone()),
        )
        .await,
        first_payload
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            Some(final_receipt.version_id.clone()),
        )
        .await,
        final_payload
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            None,
        )
        .await,
        final_payload
    );
}

#[tokio::test]
async fn implicit_mutation_batch_replays_without_publishing_new_versions() {
    let fixture = SingleNodeMutationBatchFixture::new("implicit-batch-idempotency").await;
    let mut object_client = fixture.object_client().await;
    let object_key = "implicit-replay/item.json";
    let mut context = native_mutation_context(&fixture.actor, fixture.bucket_id, "implicit-replay");
    context.write_visibility = Some(published_watch_visibility());
    let request = MutationBatchRequest {
        bucket_name: fixture.bucket_name.clone(),
        mutation_context: Some(context),
        precondition: None,
        operations: vec![small_put(object_key, br#"{"attempt":1}"#.to_vec())],
    };

    let first = object_client
        .mutation_batch(authorized(request.clone(), &fixture.actor.token))
        .await
        .expect("first implicit batch")
        .into_inner();
    let replay = object_client
        .mutation_batch(authorized(request, &fixture.actor.token))
        .await
        .expect("idempotent implicit batch replay")
        .into_inner();

    assert!(
        first.watch_cursor > 0,
        "implicit committed batch must return its durable watch cursor"
    );
    assert_eq!(replay, first);
    let versions = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name: fixture.bucket_name.clone(),
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 10,
                version_id_marker: String::new(),
                ..Default::default()
            },
            &fixture.actor.token,
        ))
        .await
        .expect("list replayed object versions")
        .into_inner()
        .versions;
    assert_eq!(
        versions.len(),
        1,
        "idempotent replay must not publish another object version"
    );
    assert_eq!(
        versions[0].version_id,
        first.operation_receipts[0].version_id
    );
}

#[tokio::test]
async fn implicit_mutation_batch_supports_non_inline_payloads_atomically() {
    let fixture = SingleNodeMutationBatchFixture::new("implicit-batch-non-inline").await;
    let mut object_client = fixture.object_client().await;
    let first_payload = vec![0x41; 40 * 1024];
    let second_payload = vec![0x42; 40 * 1024];
    let mut context =
        native_mutation_context(&fixture.actor, fixture.bucket_id, "implicit-non-inline");
    context.write_visibility = Some(published_watch_visibility());

    let response = tokio::time::timeout(
        SMALL_BATCH_DEADLINE,
        object_client.mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(context),
                precondition: None,
                operations: vec![
                    small_put("implicit-large/one.bin", first_payload.clone()),
                    small_put("implicit-large/two.bin", second_payload.clone()),
                ],
            },
            &fixture.actor.token,
        )),
    )
    .await
    .expect("non-inline implicit batch must finish before the batch deadline")
    .expect("publish non-inline implicit batch")
    .into_inner();

    assert_eq!(response.write_state, WriteState::Finalised as i32);
    assert_eq!(response.operation_receipts.len(), 2);
    assert!(
        response.watch_cursor > 0,
        "non-inline implicit batch must return its durable watch cursor"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            "implicit-large/one.bin",
            None,
        )
        .await,
        first_payload
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            "implicit-large/two.bin",
            None,
        )
        .await,
        second_payload
    );
}

#[tokio::test]
async fn implicit_non_put_batches_require_an_explicit_object_transaction() {
    let fixture = SingleNodeMutationBatchFixture::new("implicit-batch-requires-transaction").await;
    let mut object_client = fixture.object_client().await;
    let object_key = "implicit-policy/item.json";
    put_object_for_test(
        &mut object_client,
        &fixture.actor.token,
        &fixture.bucket_name,
        object_key,
        br#"{"state":"original"}"#,
        native_mutation_context(&fixture.actor, fixture.bucket_id, "implicit-policy-seed"),
    )
    .await
    .expect("seed object for implicit patch policy");
    let mut context =
        native_mutation_context(&fixture.actor, fixture.bucket_id, "implicit-patch-policy");
    context.write_visibility = None;

    let error = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(context),
                precondition: None,
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PatchJsonObject(
                        MutationBatchPatchJsonObject {
                            object_key: object_key.to_string(),
                            base_version_id: None,
                            merge_patch_json: r#"{"state":"changed"}"#.to_string(),
                        },
                    )),
                }],
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("implicit non-put batch must not execute sequentially");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert_eq!(
        error.message(),
        "ExplicitTransactionRequiredForNonPutMutationBatch"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            object_key,
            None,
        )
        .await,
        br#"{"state":"original"}"#
    );
}

#[tokio::test]
async fn mutation_batch_rejects_coordination_operations_before_execution() {
    let fixture = SingleNodeMutationBatchFixture::new("batch-rejects-coordination-operation").await;
    let mut object_client = fixture.object_client().await;
    let mut context =
        native_mutation_context(&fixture.actor, fixture.bucket_id, "coordination-policy");
    context.write_visibility = None;

    let error = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(context),
                precondition: None,
                operations: vec![MutationBatchOperation {
                    op: Some(
                        anvil_api::mutation_batch_operation::Op::CheckpointTaskLease(
                            anvil_api::MutationBatchCheckpointTaskLease {
                                task_id: "not-executed".to_string(),
                                fence_token: 1,
                                checkpoint_cursor_low: 1,
                                checkpoint_cursor_high: 0,
                                expected_root_generation: 1,
                                expected_lease_epoch: 1,
                                expected_expires_at_nanos: i64::MAX,
                                expected_lease_hash: "not-read".to_string(),
                            },
                        ),
                    ),
                }],
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("coordination operation must use CoordinationService");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("use CoordinationService"));
}

#[tokio::test]
async fn twenty_put_explicit_batch_finishes_before_ttl_and_commits_one_generation() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-twenty-put").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "twenty small puts",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let objects = (0..SMALL_PUT_COUNT)
        .map(|index| {
            (
                format!("atomic/item-{index:02}.json"),
                format!(r#"{{"index":{index}}}"#).into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let operations = objects
        .iter()
        .map(|(key, payload)| small_put(key.clone(), payload.clone()))
        .collect::<Vec<_>>();

    let started_at = Instant::now();
    let response = tokio::time::timeout(
        SMALL_BATCH_DEADLINE,
        object_client.mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("twenty-small-put", &transaction.transaction_id),
                ),
                precondition: None,
                operations,
            },
            &fixture.actor.token,
        )),
    )
    .await
    .expect("20 small puts must complete well below the 30 second transaction TTL")
    .expect("stage 20-put MutationBatch")
    .into_inner();
    let batch_elapsed = started_at.elapsed();

    assert!(
        batch_elapsed < SMALL_BATCH_DEADLINE,
        "20-put MutationBatch took {batch_elapsed:?}, expected less than {SMALL_BATCH_DEADLINE:?}"
    );
    let remaining_ttl_nanos = transaction
        .expires_at_unix_nanos
        .saturating_sub(unix_time_nanos());
    assert!(
        remaining_ttl_nanos > Duration::from_secs(15).as_nanos() as u64,
        "20-put MutationBatch consumed too much of its TTL: {batch_elapsed:?} elapsed"
    );
    assert_eq!(response.write_state, WriteState::Staged as i32);
    assert_eq!(response.operation_receipts.len(), SMALL_PUT_COUNT);
    assert_eq!(
        response
            .operation_receipts
            .iter()
            .map(|receipt| receipt.object_key.as_str())
            .collect::<Vec<_>>(),
        objects
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        list_keys(&fixture, &mut object_client, "atomic/", None)
            .await
            .is_empty(),
        "staged batch leaked objects before commit"
    );

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("commit 20-put transaction")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);
    let committed_generation = committed
        .root_generation
        .expect("committed transaction reports its root generation");
    assert!(committed_generation > 0);

    let expected_keys = objects
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(
        list_keys(
            &fixture,
            &mut object_client,
            "atomic/",
            at_root_generation(committed_generation - 1),
        )
        .await
        .is_empty(),
        "the generation before commit must contain none of the batch"
    );
    assert_eq!(
        list_keys(
            &fixture,
            &mut object_client,
            "atomic/",
            at_root_generation(committed_generation),
        )
        .await,
        expected_keys,
        "the commit generation must expose the whole batch"
    );
}

#[tokio::test]
async fn object_version_precondition_is_revalidated_at_transaction_publication() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-durable-object-precondition").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let guard_key = "preconditions/guard.json";
    let target_key = "preconditions/target.json";
    let original_guard = put_object_for_test(
        &mut object_client,
        &fixture.actor.token,
        &fixture.bucket_name,
        guard_key,
        br#"{"revision":1}"#,
        native_mutation_context(
            &fixture.actor,
            fixture.bucket_id,
            "durable-precondition-guard",
        ),
    )
    .await
    .expect("create object used as the transaction guard");
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "durable object version precondition",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;

    let staged =
        object_client
            .mutation_batch(authorized(
                MutationBatchRequest {
                    bucket_name: fixture.bucket_name.clone(),
                    mutation_context: Some(fixture.mutation_context(
                        "durable-object-precondition",
                        &transaction.transaction_id,
                    )),
                    precondition: Some(WritePrecondition {
                        object_versions: vec![ObjectVersionPrecondition {
                            bucket_name: fixture.bucket_name.clone(),
                            object_key: guard_key.to_string(),
                            expected_version_id: Some(original_guard.version_id),
                            must_not_exist: false,
                        }],
                        lease_fence: None,
                    }),
                    operations: vec![small_put(target_key, br#"{"accepted":true}"#.to_vec())],
                },
                &fixture.actor.token,
            ))
            .await
            .expect("stage a write protected by another object's exact version")
            .into_inner();
    assert_eq!(staged.write_state, WriteState::Staged as i32);

    put_object_for_test(
        &mut object_client,
        &fixture.actor.token,
        &fixture.bucket_name,
        guard_key,
        br#"{"revision":2}"#,
        native_mutation_context(
            &fixture.actor,
            fixture.bucket_id,
            "invalidate-durable-precondition",
        ),
    )
    .await
    .expect("advance the guard after transaction staging");

    let conflict = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("a guard changed on another writer must reject publication");
    assert_eq!(conflict.code(), Code::Aborted);
    assert_object_missing(&fixture, &mut object_client, target_key).await;
}

#[tokio::test]
async fn lease_fence_is_revalidated_at_transaction_publication() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-durable-lease-fence").await;
    let mut coordination_client = fixture.coordination_client().await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let task_id = unique_test_name("durable-lease-fence");
    let lease = coordination_client
        .acquire_task_lease(authorized(
            AcquireTaskLeaseRequest {
                task_id: task_id.clone(),
                task_kind: "mutation_batch_test".to_string(),
                partition_family: "mutation_batch".to_string(),
                partition_id: hex::encode([7_u8; 32]),
                owner_label: "batch-worker".to_string(),
                source_cursor_low: 0,
                source_cursor_high: 0,
                requested_ttl_nanos: 60_000_000_000,
            },
            &fixture.actor.token,
        ))
        .await
        .expect("acquire lease used as publication fence")
        .into_inner()
        .lease
        .expect("lease response");
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "durable lease fence",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let target_key = "preconditions/lease-guarded.json";

    let staged = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("durable-lease-fence", &transaction.transaction_id),
                ),
                precondition: Some(WritePrecondition {
                    object_versions: Vec::new(),
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id: task_id.clone(),
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![small_put(target_key, br#"{"guarded":true}"#.to_vec())],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("stage lease-fenced object mutation")
        .into_inner();
    assert_eq!(staged.write_state, WriteState::Staged as i32);

    coordination_client
        .commit_task_lease(authorized(
            anvil_api::CommitTaskLeaseRequest {
                task_id,
                fence_token: lease.fence_token,
                committed_cursor_low: 1,
                committed_cursor_high: 0,
                expected_root_generation: lease.root_generation,
                expected_lease_epoch: lease.lease_epoch,
                expected_expires_at_nanos: lease.expires_at_nanos,
                expected_lease_hash: lease.lease_hash,
            },
            &fixture.actor.token,
        ))
        .await
        .expect("advance the lease after transaction staging");

    let conflict = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("a changed lease fence must reject object publication");
    assert_eq!(conflict.code(), Code::Aborted);
    assert_object_missing(&fixture, &mut object_client, target_key).await;
}

#[tokio::test]
async fn later_batch_can_precondition_on_an_earlier_staged_version() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-repeated-staging").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "repeated transaction batches",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;

    let first = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("repeated-batch-first", &transaction.transaction_id),
                ),
                precondition: None,
                operations: vec![small_put("repeated/first.json", br#"{"batch":1}"#.to_vec())],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("stage the first batch")
        .into_inner();
    let staged_version = first.operation_receipts[0].version_id.clone();

    let second = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("repeated-batch-second", &transaction.transaction_id),
                ),
                precondition: Some(WritePrecondition {
                    object_versions: vec![ObjectVersionPrecondition {
                        bucket_name: fixture.bucket_name.clone(),
                        object_key: "repeated/first.json".to_string(),
                        expected_version_id: Some(staged_version),
                        must_not_exist: false,
                    }],
                    lease_fence: None,
                }),
                operations: vec![small_put(
                    "repeated/second.json",
                    br#"{"batch":2}"#.to_vec(),
                )],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("a later batch must see the earlier transaction-local version")
        .into_inner();
    assert_eq!(second.write_state, WriteState::Staged as i32);

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("commit repeated transaction batches")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);
    assert_eq!(
        list_keys(&fixture, &mut object_client, "repeated/", None).await,
        vec![
            "repeated/first.json".to_string(),
            "repeated/second.json".to_string(),
        ]
    );
}

#[tokio::test]
async fn duplicate_payload_hashes_use_the_safe_fallback_without_losing_atomicity() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-duplicate-payloads").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "duplicate payload hashes",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let payload = br#"{"shared":true}"#.to_vec();
    let staged = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("duplicate-payloads", &transaction.transaction_id),
                ),
                precondition: None,
                operations: vec![
                    small_put("duplicate/one.json", payload.clone()),
                    small_put("duplicate/two.json", payload.clone()),
                ],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("stage duplicate payload hashes through the established object path")
        .into_inner();
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    assert_eq!(staged.operation_receipts.len(), 2);
    assert_object_missing(&fixture, &mut object_client, "duplicate/one.json").await;
    assert_object_missing(&fixture, &mut object_client, "duplicate/two.json").await;

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("commit duplicate payload objects atomically")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            "duplicate/one.json",
            None,
        )
        .await,
        payload
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            "duplicate/two.json",
            None,
        )
        .await,
        payload
    );
}

#[tokio::test]
async fn committed_batch_watch_events_preserve_request_order_and_retry_cursor() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-watch-order").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let transaction = fixture
        .begin_transaction(
            &mut transaction_client,
            "ordered batch watch events",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let prefix = "watch-order/";
    let requested_objects = [
        ("watch-order/third.json", br#"{"order":1}"#.to_vec()),
        ("watch-order/first.json", br#"{"order":2}"#.to_vec()),
        ("watch-order/second.json", br#"{"order":3}"#.to_vec()),
    ];
    let request = MutationBatchRequest {
        bucket_name: fixture.bucket_name.clone(),
        mutation_context: Some(
            fixture.mutation_context("ordered-watch-batch", &transaction.transaction_id),
        ),
        precondition: None,
        operations: requested_objects
            .iter()
            .map(|(key, payload)| small_put(*key, payload.clone()))
            .collect(),
    };

    let staged = object_client
        .mutation_batch(authorized(request.clone(), &fixture.actor.token))
        .await
        .expect("stage ordered watch MutationBatch")
        .into_inner();
    let retried = object_client
        .mutation_batch(authorized(request, &fixture.actor.token))
        .await
        .expect("retry ordered watch MutationBatch")
        .into_inner();
    assert_eq!(
        retried, staged,
        "an idempotent retry must replay its receipt"
    );
    assert_eq!(staged.write_state, WriteState::Staged as i32);
    assert_eq!(staged.operation_receipts.len(), requested_objects.len());

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("commit ordered watch MutationBatch")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);

    let mut initial_watch = watch_prefix(&fixture, &mut object_client, prefix, 0).await;
    let first = next_watch_event(&mut initial_watch).await;
    drop(initial_watch);

    let mut resumed_watch = watch_prefix(&fixture, &mut object_client, prefix, first.cursor).await;
    let second = next_watch_event(&mut resumed_watch).await;
    let third = next_watch_event(&mut resumed_watch).await;
    let events = [&first, &second, &third];

    assert_eq!(
        events
            .iter()
            .map(|event| event.object_key.as_str())
            .collect::<Vec<_>>(),
        requested_objects
            .iter()
            .map(|(key, _)| *key)
            .collect::<Vec<_>>(),
        "watch events must retain MutationBatch request order rather than key order"
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.version_id.as_str())
            .collect::<Vec<_>>(),
        staged
            .operation_receipts
            .iter()
            .map(|receipt| receipt.version_id.as_str())
            .collect::<Vec<_>>(),
        "watch events must identify the ordered operation receipts"
    );
    assert!(events.iter().all(|event| event.event_type == "put"));
    assert!(first.cursor < second.cursor && second.cursor < third.cursor);

    match tokio::time::timeout(Duration::from_secs(5), resumed_watch.next()).await {
        Err(_) => {}
        Ok(None) => panic!("resumed object-prefix watch closed unexpectedly"),
        Ok(Some(Err(error))) => panic!("resumed object-prefix watch failed: {error}"),
        Ok(Some(Ok(event))) => panic!(
            "idempotent MutationBatch retry emitted an extra watch event at cursor {} for {}",
            event.cursor, event.object_key
        ),
    }
}

#[tokio::test]
async fn authorization_failure_leaves_every_batch_operation_invisible() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-authorization-atomicity").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;
    let allowed_key = "authorization/allowed.json";
    let control_key = "authorization/control.json";
    let denied_key = "authorization/denied.json";
    let original_payload = br#"{"state":"original"}"#.to_vec();
    let attempted_payload = br#"{"state":"mutated"}"#.to_vec();
    let control_payload = br#"{"state":"control"}"#.to_vec();

    put_object_for_test(
        &mut object_client,
        &fixture.actor.token,
        &fixture.bucket_name,
        allowed_key,
        &original_payload,
        native_mutation_context(&fixture.actor, fixture.bucket_id, "authorization-baseline"),
    )
    .await
    .expect("seed the operation this test grants permission to update");

    let tenant_ref = fixture.actor.tenant_id.to_string();
    let app_name = unique_test_name("partial-batch-writer");
    let (app_id, client_id, client_secret) = fixture
        ._cluster
        .create_application_with_id(&tenant_ref, &app_name)
        .await;
    fixture
        ._cluster
        .grant_application_policy(
            &tenant_ref,
            &app_name,
            "object:write",
            &format!("{}/{}", fixture.bucket_name, allowed_key),
        )
        .await;
    fixture
        ._cluster
        .grant_application_policy(
            &tenant_ref,
            &app_name,
            "object:write",
            &format!("{}/{}", fixture.bucket_name, control_key),
        )
        .await;
    let partial_writer_token =
        get_access_token_for_test(&fixture.actor.grpc_addr, &client_id, &client_secret).await;
    let transaction = fixture
        .begin_transaction_as(
            &mut transaction_client,
            "partially authorized batch",
            EXPLICIT_TRANSACTION_TTL_MS,
            &partial_writer_token,
        )
        .await;

    let denied = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(fixture.mutation_context_as(
                    "partially-authorized-batch",
                    &transaction.transaction_id,
                    &app_id,
                )),
                precondition: None,
                operations: vec![
                    small_put(allowed_key, attempted_payload),
                    small_put(denied_key, br#"{"state":"forbidden"}"#.to_vec()),
                ],
            },
            &partial_writer_token,
        ))
        .await
        .expect_err("one unauthorized operation must reject the whole MutationBatch");
    assert_eq!(denied.code(), Code::PermissionDenied);

    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            allowed_key,
            None,
        )
        .await,
        original_payload,
        "the authorized operation must not leak from a rejected batch"
    );
    assert_object_missing(&fixture, &mut object_client, denied_key).await;

    let control = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(fixture.mutation_context_as(
                    "authorized-control-batch",
                    &transaction.transaction_id,
                    &app_id,
                )),
                precondition: None,
                operations: vec![small_put(control_key, control_payload.clone())],
            },
            &partial_writer_token,
        ))
        .await
        .expect("the transaction must remain usable for an authorized operation")
        .into_inner();
    assert_eq!(control.write_state, WriteState::Staged as i32);
    assert_eq!(control.operation_receipts.len(), 1);

    let committed = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: transaction.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &partial_writer_token,
        ))
        .await
        .expect("commit the transaction after its rejected batch")
        .into_inner();
    assert_eq!(committed.state, WriteState::Committed as i32);

    assert_eq!(
        list_keys(&fixture, &mut object_client, "authorization/", None).await,
        vec![allowed_key.to_string(), control_key.to_string()],
        "commit must publish only the separately authorized control operation"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            allowed_key,
            None,
        )
        .await,
        original_payload,
        "committing after rejection must not publish the allowed operation"
    );
    assert_eq!(
        get_object_bytes_for_test(
            &mut object_client,
            &fixture.actor.token,
            &fixture.bucket_name,
            control_key,
            None,
        )
        .await,
        control_payload,
        "the authorized control operation must still commit"
    );
    assert_object_missing(&fixture, &mut object_client, denied_key).await;
}

#[tokio::test]
async fn rollback_and_expiry_never_publish_staged_mutation_batches() {
    let fixture = SingleNodeMutationBatchFixture::new("tx-batch-terminal-states").await;
    let mut transaction_client = fixture.transaction_client().await;
    let mut object_client = fixture.object_client().await;

    let cancelled = fixture
        .begin_transaction(
            &mut transaction_client,
            "cancel staged MutationBatch",
            EXPLICIT_TRANSACTION_TTL_MS,
        )
        .await;
    let cancelled_key = "terminal/cancelled.json";
    object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("cancelled-batch", &cancelled.transaction_id),
                ),
                precondition: None,
                operations: vec![small_put(cancelled_key, br#"{"cancelled":true}"#.to_vec())],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("stage MutationBatch before rollback");
    assert_object_missing(&fixture, &mut object_client, cancelled_key).await;

    let rolled_back = transaction_client
        .rollback_transaction(authorized(
            RollbackTransactionRequest {
                transaction_id: cancelled.transaction_id.clone(),
                reason: "client cancelled test transaction".to_string(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect("rollback staged MutationBatch")
        .into_inner();
    assert_eq!(rolled_back.state, "rolled_back");
    assert_object_missing(&fixture, &mut object_client, cancelled_key).await;
    let cancelled_commit = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: cancelled.transaction_id,
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("rolled-back transaction must not commit");
    assert_eq!(cancelled_commit.code(), Code::FailedPrecondition);

    let expiring = fixture
        .begin_transaction(
            &mut transaction_client,
            "expire staged MutationBatch",
            3_000,
        )
        .await;
    let expired_key = "terminal/expired.json";
    object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: fixture.bucket_name.clone(),
                mutation_context: Some(
                    fixture.mutation_context("expired-batch", &expiring.transaction_id),
                ),
                precondition: None,
                operations: vec![small_put(expired_key, br#"{"expired":true}"#.to_vec())],
            },
            &fixture.actor.token,
        ))
        .await
        .expect("stage MutationBatch before expiry");
    assert!(
        unix_time_nanos() < expiring.expires_at_unix_nanos,
        "test transaction expired before the batch was staged"
    );
    assert_object_missing(&fixture, &mut object_client, expired_key).await;

    wait_until_expired(expiring.expires_at_unix_nanos).await;
    let expired_commit = transaction_client
        .commit_transaction(authorized(
            CommitTransactionRequest {
                transaction_id: expiring.transaction_id.clone(),
                consistency: ConsistencyMode::Committed as i32,
                wait_for_finalization: false,
                final_preconditions: Vec::new(),
            },
            &fixture.actor.token,
        ))
        .await
        .expect_err("expired transaction must not commit");
    assert_eq!(expired_commit.code(), Code::FailedPrecondition);

    let expired_status = transaction_client
        .get_transaction(authorized(
            GetTransactionRequest {
                transaction_id: expiring.transaction_id,
            },
            &fixture.actor.token,
        ))
        .await
        .expect("read expired transaction status")
        .into_inner();
    assert_eq!(expired_status.state, "expired");
    assert!(expired_status.error.is_some());
    assert_object_missing(&fixture, &mut object_client, expired_key).await;
}
