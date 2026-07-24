use super::*;

#[tokio::test]
// Internal-only: appends a reserved PersonalDB watch event directly through
// local storage before reading it over the public watch API.
async fn personaldb_group_watch_streams_reserved_internal_events_through_native_api() {
    let cluster = shared_default_test_cluster().await;

    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let token = cluster.token.clone();
    let mut setup_client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    create_group(&mut setup_client, &token, &database_id).await;

    let payload = PersonalDbGroupWatchPayload {
        database_id: database_id.clone(),
        event_type: "commit".to_string(),
        log_index: 7,
        log_hash: hex::encode([7; 32]),
        changeset_payload_hash: hex::encode([8; 32]),
        certificate_hash: hex::encode([9; 32]),
        committed_head_hash: hex::encode([10; 32]),
        emitted_at: "2026-06-28T00:00:00Z".to_string(),
    };
    append_personaldb_group_watch_record(
        &cluster.states[0].storage,
        1,
        &database_id,
        *uuid::Uuid::new_v4().as_bytes(),
        42,
        payload,
    )
    .await
    .unwrap();

    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let response = client
        .watch_personal_db_group(authorized(
            WatchPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = response.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, database_id);
    assert_eq!(event.cursor_low, 42);
    assert_eq!(event.cursor_high, 0);
    assert_eq!(event.event_type, "commit");
    assert_eq!(event.log_index, 7);
    assert_eq!(event.authz_revision, 11);
}
