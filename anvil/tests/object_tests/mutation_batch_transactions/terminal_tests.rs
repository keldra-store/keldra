use super::*;

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
            fixture.commit_request(transaction.transaction_id),
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
            fixture.commit_request(transaction.transaction_id),
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
            fixture.rollback_request(
                cancelled.transaction_id.clone(),
                "client cancelled test transaction",
            ),
            &fixture.actor.token,
        ))
        .await
        .expect("rollback staged MutationBatch")
        .into_inner();
    assert_eq!(rolled_back.state, "rolled_back");
    assert_object_missing(&fixture, &mut object_client, cancelled_key).await;
    let cancelled_commit = transaction_client
        .commit_transaction(authorized(
            fixture.commit_request(cancelled.transaction_id),
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
        unix_time_ms() < expiring.expires_at_unix_ms,
        "test transaction expired before the batch was staged"
    );
    assert_object_missing(&fixture, &mut object_client, expired_key).await;

    wait_until_expired(expiring.expires_at_unix_ms).await;
    let expired_commit = transaction_client
        .commit_transaction(authorized(
            fixture.commit_request(expiring.transaction_id.clone()),
            &fixture.actor.token,
        ))
        .await
        .expect_err("expired transaction must not commit");
    assert_eq!(expired_commit.code(), Code::FailedPrecondition);

    let expired_status = transaction_client
        .get_transaction(authorized(
            fixture.get_transaction_request(expiring.transaction_id),
            &fixture.actor.token,
        ))
        .await
        .expect("read expired transaction status")
        .into_inner();
    assert_eq!(expired_status.state, "expired");
    assert_object_missing(&fixture, &mut object_client, expired_key).await;
}
