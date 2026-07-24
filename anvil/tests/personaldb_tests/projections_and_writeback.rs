use super::*;

#[tokio::test]
// Internal-only: seeds a projection watch record directly through local
// cluster storage to assert reserved watch-envelope details.
async fn personaldb_projection_definition_create_get_and_watch_are_native_api_backed() {
    let cluster = shared_default_test_cluster().await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition(&projection_database_id, &source_database_id);
    let created = client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let created_definition: ProjectionDefinition =
        serde_json::from_str(&created.projection_definition_json).unwrap();
    created_definition.verify().unwrap();
    assert_eq!(created_definition.database_id, projection_database_id);
    assert_eq!(
        created_definition.source_database_ids,
        vec![source_database_id.clone()]
    );
    assert!(created_definition.definition_hash.as_ref().unwrap().len() == 64);

    let fetched = client
        .get_personal_db_projection(authorized(
            GetPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let fetched_definition: ProjectionDefinition =
        serde_json::from_str(&fetched.projection_definition_json).unwrap();
    assert_eq!(fetched_definition, created_definition);

    let payload = PersonalDbProjectionWatchPayload {
        database_id: projection_database_id.clone(),
        projection_id: "projection-items".to_string(),
        event_type: "projection_committed".to_string(),
        source_database_id: source_database_id.clone(),
        source_log_index: 1,
        source_log_hash: hex::encode([1; 32]),
        projection_log_index: 1,
        projection_log_hash: hex::encode([2; 32]),
        definition_hash: created_definition.definition_hash.unwrap(),
        emitted_at: "2026-06-28T00:00:00Z".to_string(),
    };
    append_personaldb_projection_watch_record(
        &cluster.states[0].storage,
        1,
        &projection_database_id,
        "projection-items",
        *uuid::Uuid::new_v4().as_bytes(),
        77,
        payload,
    )
    .await
    .unwrap();
    let watch = client
        .watch_personal_db_projection(authorized(
            WatchPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.cursor_low, 77);
    assert_eq!(event.cursor_high, 0);
    assert_eq!(event.database_id, projection_database_id);
    assert_eq!(event.projection_id, "projection-items");
    assert_eq!(event.source_database_id, source_database_id);
    assert_eq!(event.authz_revision, 12);
    let envelope = event
        .envelope
        .as_ref()
        .expect("PersonalDB projection watch envelope");
    assert_eq!(envelope.watch_stream_id, "personaldb_projection");
    assert_eq!(envelope.partition_family, "personaldb_projection");
    assert_eq!(envelope.cursor_low, event.cursor_low);
    assert_eq!(envelope.personaldb_log_index, event.projection_log_index);
    assert_eq!(envelope.authz_revision, event.authz_revision);
    assert_eq!(envelope.record_kind, "personaldb_projection");
    assert!(!envelope.payload_hash.is_empty());
}

#[tokio::test]
async fn personaldb_source_commit_builds_projection_group_and_watch_event() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    let created = client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let created_definition: ProjectionDefinition =
        serde_json::from_str(&created.projection_definition_json).unwrap();

    let source_commit = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_commit.log_index, 1);

    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-projection".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!projected.snapshot_required);
    assert_eq!(projected.entries.len(), 1);
    assert_eq!(
        projected.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    projection_db
        .apply_strm(
            &mut std::io::Cursor::new(&projected.entries[0].changeset_bytes),
            None::<fn(&str) -> bool>,
            |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
        )
        .unwrap();
    let name: String = projection_db
        .query_row(
            "SELECT name FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(name, "alpha");

    let watch = client
        .watch_personal_db_projection(authorized(
            WatchPersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, projection_database_id);
    assert_eq!(event.projection_id, "projection-items");
    assert_eq!(event.event_type, "projection_committed");
    assert_eq!(event.source_database_id, source_database_id);
    assert_eq!(event.source_log_index, 1);
    assert_eq!(event.source_log_hash, source_commit.log_hash);
    assert_eq!(event.projection_log_index, 1);
    assert_eq!(
        event.definition_hash,
        created_definition.definition_hash.unwrap()
    );
}

#[tokio::test]
async fn personaldb_projection_resource_relation_filter_uses_authz_index() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection-authz").await;
    grant_default_authz_tuple_writer(&cluster, &actor).await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_with_resource_filter_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: "personaldb_row".to_string(),
                object_id: format!("tenant-{}/{source_database_id}/items/1", actor.tenant_id),
                relation: "viewer".to_string(),
                subject_kind: "app".to_string(),
                subject_id: "scope-primary".to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "allow projection target".to_string(),
                scope: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let source_commit = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_commit.log_index, 1);

    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-projection-authz".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!projected.snapshot_required);
    assert_eq!(projected.entries.len(), 1);
    assert_eq!(
        projected.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
}

#[tokio::test]
async fn personaldb_projection_group_submit_rejects_direct_writeback_when_policy_denies() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-writeback-deny").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap();

    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(projection_head_before.log_index, 1);

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    let projection_head_after = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(
        projection_head_after.log_index,
        projection_head_before.log_index
    );
    assert_eq!(
        projection_head_after.log_hash,
        projection_head_before.log_hash
    );
}

#[tokio::test]
async fn personaldb_projection_writeback_updates_source_and_rebuilds_projection_when_allowed() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-writeback").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_allowing_name_writeback_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    let source_insert = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let projection_head = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(projection_head.log_index, 1);

    let source_writeback = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                projection_head.log_index,
                &projection_head.log_hash,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_insert.log_index, 1);
    assert_eq!(source_writeback.log_index, 2);

    let source_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: source_database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-source".to_string(),
                have_log_index: 0,
                have_log_hash: source_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_catchup.entries.len(), 2);
    let source_db = Connection::open_in_memory().unwrap();
    source_db.execute_batch(PERSONALDB_TEST_SCHEMA_SQL).unwrap();
    for entry in &source_catchup.entries {
        source_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let source_name: String = source_db
        .query_row("SELECT name FROM items WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(source_name, "beta");

    let projection_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
                principal: actor.app_id.clone(),
                replica_id: "replica-projection".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_catchup.entries.len(), 2);
    assert_eq!(
        projection_catchup.entries[1]
            .log_record
            .as_ref()
            .unwrap()
            .log_index,
        2
    );
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    for entry in &projection_catchup.entries {
        projection_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let projection_name: String = projection_db
        .query_row(
            "SELECT name FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projection_name, "beta");
}

#[tokio::test]
async fn personaldb_projection_writeback_insert_and_delete_round_trip_through_source_group() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-insert-delete").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_allowing_id_name_writeback_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    let inserted = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                0,
                &projection_genesis,
                sqlite_projection_insert_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inserted.log_index, 1);

    let projection_head_after_insert = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head after insert");
    assert_eq!(projection_head_after_insert.log_index, 1);

    let deleted = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                projection_head_after_insert.log_index,
                &projection_head_after_insert.log_hash,
                sqlite_projection_delete_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.log_index, 2);

    let source_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: source_database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-source-insert-delete".to_string(),
                have_log_index: 0,
                have_log_hash: source_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_catchup.entries.len(), 2);
    let source_db = Connection::open_in_memory().unwrap();
    source_db.execute_batch(PERSONALDB_TEST_SCHEMA_SQL).unwrap();
    for entry in &source_catchup.entries {
        source_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let source_count: i64 = source_db
        .query_row("SELECT COUNT(*) FROM items WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(source_count, 0);

    let projection_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
                principal: actor.app_id.clone(),
                replica_id: "replica-projection-insert-delete".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_catchup.entries.len(), 2);
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    for entry in &projection_catchup.entries {
        projection_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let projection_count: i64 = projection_db
        .query_row(
            "SELECT COUNT(*) FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projection_count, 0);
}

#[tokio::test]
async fn personaldb_projection_writeback_rejects_protected_column_mutation() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-protected").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_allowing_name_writeback_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap();

    let source_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: source_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("source committed head");
    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                sqlite_projection_id_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    let source_head_after = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: source_database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("source committed head");
    assert_eq!(source_head_after.log_index, source_head_before.log_index);
    assert_eq!(source_head_after.log_hash, source_head_before.log_hash);
}

#[tokio::test]
async fn personaldb_projection_writeback_rejects_ambiguous_source_binding() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-ambiguous").await;

    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let first_source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let second_source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let first_source_genesis = create_group(&mut client, &token, &first_source_database_id).await;
    let second_source_genesis = create_group(&mut client, &token, &second_source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_with_ambiguous_writeback_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &first_source_database_id,
        &second_source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    for (database_id, genesis_hash) in [
        (&first_source_database_id, &first_source_genesis),
        (&second_source_database_id, &second_source_genesis),
    ] {
        let source_head = client
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: actor.tenant_id,
                    database_id: database_id.to_string(),
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .committed_head
            .expect("source committed head");
        assert_eq!(source_head.log_index, 0);
        assert_eq!(&source_head.log_hash, genesis_hash);
    }
}

#[tokio::test]
async fn personaldb_api_rejects_cross_tenant_request_scope() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-cross-tenant").await;

    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let err = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id + 1,
                database_id: "db-alpha".to_string(),
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}
