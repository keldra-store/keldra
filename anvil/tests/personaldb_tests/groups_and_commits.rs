use super::*;

#[tokio::test]
async fn personaldb_group_create_get_and_catch_up_are_native_api_backed() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-group").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let schema_hash = personaldb_test_schema_hash();
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));

    let created = client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: schema_hash.clone(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let manifest = created.manifest.expect("group manifest");
    assert_eq!(manifest.tenant_id, actor.tenant_id.to_string());
    assert_eq!(manifest.database_id, database_id);
    assert_eq!(manifest.schema_hash, schema_hash);
    assert_eq!(manifest.genesis_hash, genesis_hash);
    assert_eq!(manifest.consistency_policy, "StrictWitnessed");
    assert!(!manifest.manifest_hash.is_empty());
    let signature = manifest.manifest_signature.expect("manifest signature");
    assert_eq!(signature.signature.len(), 64);
    assert!(signature.key_id.starts_with("sha256:"));

    let head = created.committed_head.expect("committed head");
    assert_eq!(head.log_index, 0);
    assert_eq!(head.log_hash, genesis_hash);
    assert_eq!(head.segment_ref, "");
    assert_eq!(head.policy_epoch, 1);
    assert_eq!(head.membership_epoch, 1);
    assert!(!head.head_hash.is_empty());
    assert_eq!(
        head.head_signature
            .expect("committed head signature")
            .signature
            .len(),
        64
    );

    let fetched = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.manifest.unwrap().database_id, database_id);
    assert_eq!(fetched.committed_head.unwrap().log_hash, genesis_hash);

    let caught_up = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash.clone(),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert!(caught_up.entries.is_empty());
    assert_eq!(caught_up.committed_head.unwrap().log_hash, genesis_hash);

    let divergent = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id,
                principal: actor.app_id.clone(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: hex::encode([9; 32]),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(divergent.snapshot_required);
    assert_eq!(divergent.snapshot_reason, "divergent_replica");
}

#[tokio::test]
// Internal-only: mints custom JWTs and reads the row index from local storage.
async fn personaldb_submit_commits_and_is_available_to_catch_up_and_watch() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token("reader-app".to_string(), 1)
        .unwrap();
    let permission_denied = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &limited_token),
            &limited_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(permission_denied.code(), Code::PermissionDenied);

    let malformed = client
        .submit_personal_db_changeset(authorized(
            malformed_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), Code::InvalidArgument);

    let session_mismatch = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, "not-the-bearer-token"),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(session_mismatch.code(), Code::Unauthenticated);

    let commit_only_token = cluster.states[0]
        .jwt_manager
        .mint_token("test-app".to_string(), 1)
        .unwrap();
    let row_permission_denied = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &commit_only_token),
            &commit_only_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(row_permission_denied.code(), Code::PermissionDenied);

    let committed = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.log_index, 1);
    assert_eq!(committed.changeset_payload_hash.len(), 64);
    assert_eq!(committed.verified_envelope_hash.len(), 64);
    assert_eq!(committed.certificate_hash.len(), 64);
    assert_eq!(committed.watch_cursor_low, 1);
    assert_eq!(committed.watch_cursor_high, 0);
    assert_eq!(committed.certificate.as_ref().unwrap().log_index, 1);
    assert_eq!(committed.committed_head.as_ref().unwrap().log_index, 1);
    assert_eq!(
        committed
            .committed_head
            .as_ref()
            .unwrap()
            .row_index_generation,
        1
    );

    let row_index_data_id =
        personaldb_row_index_data_id(1, &database_id, 1, &committed.log_hash).unwrap();
    let mvcc = cluster.states[0].mvcc.as_ref();
    let snapshot_version = mvcc.runtime.local_store().readable_version().unwrap();
    let row_index = read_personaldb_row_index(
        &cluster.states[0].storage,
        mvcc,
        &row_index_data_id,
        snapshot_version,
    )
    .await
    .unwrap();
    assert_eq!(row_index.header.generation, 1);
    assert_eq!(row_index.records.len(), 1);
    assert_eq!(row_index.records[0].database_id, database_id.as_bytes());

    let stale_base = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_base.code(), Code::FailedPrecondition);

    let fetched = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.committed_head.unwrap().log_index, 1);

    let caught_up = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "2".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert_eq!(caught_up.entries.len(), 1);
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    assert_eq!(
        caught_up.entries[0].changeset_bytes,
        sqlite_insert_changeset()
    );
    assert_eq!(
        caught_up.entries[0].certificate.as_ref().unwrap().log_index,
        1
    );

    let watch = client
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
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, database_id);
    assert_eq!(event.event_type, "commit");
    assert_eq!(event.log_index, 1);
    assert_eq!(event.log_hash, committed.log_hash);
    let envelope = event
        .envelope
        .as_ref()
        .expect("PersonalDB group watch envelope");
    assert_eq!(envelope.watch_stream_id, "personaldb_group");
    assert_eq!(envelope.partition_family, "personaldb_group");
    assert_eq!(envelope.cursor_low, event.cursor_low);
    assert_eq!(envelope.personaldb_log_index, event.log_index);
    assert_eq!(envelope.authz_revision, event.authz_revision);
    assert_eq!(envelope.record_kind, "personaldb_group");
    assert!(!envelope.payload_hash.is_empty());
}

#[tokio::test]
async fn personaldb_lost_submit_response_retry_survives_later_head_advances() {
    let cluster = shared_default_test_cluster().await;
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut client, &token, &database_id).await;

    let original_request = submit_request(
        &database_id,
        &genesis_hash,
        &token,
        sqlite_insert_changeset_with_item(1, "alpha", &[1, 2, 3]),
    );
    let original = client
        .submit_personal_db_changeset(authorized(original_request.clone(), &token))
        .await
        .unwrap()
        .into_inner();
    let later = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &database_id,
                original.log_index,
                &original.log_hash,
                &token,
                sqlite_item_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(later.log_index, 2);

    let retried = client
        .submit_personal_db_changeset(authorized(original_request, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retried, original);
}

#[tokio::test]
async fn personaldb_create_retry_replays_grants_after_post_commit_failure() {
    struct ClearFault;
    impl Drop for ClearFault {
        fn drop(&mut self) {
            anvil::mvcc_fault_injection::clear();
        }
    }

    let cluster = shared_default_test_cluster().await;
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    let request = CreatePersonalDbGroupRequest {
        database_id: database_id.clone(),
        schema_hash: personaldb_test_schema_hash(),
        genesis_hash: genesis_hash.clone(),
        schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
    };

    let _clear_fault = ClearFault;
    anvil::mvcc_fault_injection::install(
        anvil::mvcc_fault_injection::DeterministicFaults::default().fail_at(
            anvil::mvcc_fault_injection::FaultPoint::PersonalDbAfterCreateCommit,
            1,
        ),
    );
    let lost_response = client
        .create_personal_db_group(authorized(request.clone(), &token))
        .await
        .unwrap_err();
    assert_eq!(lost_response.code(), Code::Internal);
    anvil::mvcc_fault_injection::clear();

    let replayed = client
        .create_personal_db_group(authorized(request, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replayed.committed_head.unwrap().log_index, 0);

    let committed = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.log_index, 1);
}

#[tokio::test]
async fn personaldb_concurrent_same_base_submits_publish_one_witness_commit() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-concurrent").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut setup_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut setup_client, &token, &database_id).await;

    let mut first_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut second_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let first = first_client.submit_personal_db_changeset(authorized(
        submit_request_for_actor(
            &actor,
            &database_id,
            &genesis_hash,
            sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3]),
        ),
        &token,
    ));
    let second = second_client.submit_personal_db_changeset(authorized(
        submit_request_for_actor(
            &actor,
            &database_id,
            &genesis_hash,
            sqlite_insert_changeset_with_item(2, "beta", &[4_u8, 5, 6]),
        ),
        &token,
    ));

    let (first, second) = tokio::join!(first, second);
    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for result in [first, second] {
        match result {
            Ok(response) => successes.push(response.into_inner()),
            Err(status) => failures.push(status),
        }
    }

    assert_eq!(
        successes.len(),
        1,
        "only one same-base submit can publish a witnessed commit"
    );
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].code(), Code::FailedPrecondition);
    assert_eq!(successes[0].log_index, 1);

    let fetched = setup_client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let committed_head = fetched.committed_head.unwrap();
    assert_eq!(committed_head.log_index, 1);
    assert_eq!(committed_head.log_hash, successes[0].log_hash);

    let caught_up = setup_client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: database_id.clone(),
                principal: actor.app_id.clone(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert_eq!(
        caught_up.entries.len(),
        1,
        "canonical log must not contain duplicate witness commits"
    );
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().entry_hash,
        successes[0].log_hash
    );
}

#[tokio::test]
async fn personaldb_row_mutation_can_be_authorized_by_relationship_tuple() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-row-auth").await;
    grant_default_authz_tuple_writer(&cluster, &actor).await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut personaldb = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(grpc_addr).await.unwrap();

    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    personaldb
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let inserted = personaldb
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &database_id, &genesis_hash),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inserted.log_index, 1);

    let delegate = cluster
        .create_actor_in_tenant(
            actor.tenant_id,
            "personaldb-row-delegate",
            &[("personaldb:commit", &database_id)],
        )
        .await;
    let delegate_principal = delegate.app_id.as_str();
    let delegate_token = delegate.token.as_str();

    let changeset_bytes = sqlite_item_update_changeset();
    let denied = personaldb
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_tenant_and_principal(
                actor.tenant_id,
                &database_id,
                inserted.log_index,
                &inserted.log_hash,
                delegate_principal,
                &delegate_token,
                changeset_bytes.clone(),
            ),
            &delegate_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let changes = iterate_changeset(&changeset_bytes).unwrap();
    let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
        tenant_id: actor.tenant_id,
        database_id: &database_id,
        principal: delegate_principal,
        base_log_index: inserted.log_index,
        proposed_log_index: inserted.log_index + 1,
        changeset_payload_hash: hash32(&changeset_bytes),
        schema_hash: &personaldb_test_schema_hash(),
        policy_epoch: 1,
        authz_revision: 1,
        changes: &changes,
        updated_at_nanos: 1,
    })
    .unwrap();
    let effect = envelope
        .table_effects
        .first()
        .expect("update changeset should derive one effect");
    let binding = &effect.source_resource_binding;
    let resource = format!(
        "tenant-{}/{}/{}/{}",
        actor.tenant_id, database_id, binding.resource_type, binding.resource_id
    );
    let permission = effect
        .required_permissions
        .first()
        .expect("effect should require a row mutation permission")
        .clone();

    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
            context: None,
                namespace: "personaldb_row".to_string(),
                object_id: resource,
                relation: permission,
                subject_kind: "app".to_string(),
                subject_id: delegate_principal.to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "test".to_string(),
                scope: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let committed = personaldb
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_tenant_and_principal(
                actor.tenant_id,
                &database_id,
                inserted.log_index,
                &inserted.log_hash,
                delegate_principal,
                &delegate_token,
                changeset_bytes,
            ),
            &delegate_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.log_index, 2);
    assert_eq!(committed.verified_envelope_hash.len(), 64);
}

#[tokio::test]
// Internal-only: requires custom snapshot-threshold config and reads snapshot
// manifests/objects from local storage.
async fn personaldb_submit_builds_snapshot_when_threshold_is_reached() {
    // Keep isolated: the snapshot threshold is lowered to force snapshot
    // creation after one commit without changing the shared cluster profile.
    let mut cluster = isolated_test_cluster_with_config(
        "PersonalDB snapshot test lowers the snapshot threshold for this topology",
        &["test-region-1"],
        |config| {
            config.personaldb_snapshot_entry_threshold = 1;
        },
    )
    .await;
    cluster
        .start_and_converge(ISOLATED_TEST_CLUSTER_STARTUP_TIMEOUT)
        .await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let committed = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let divergent = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "2".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: hex::encode([9; 32]),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(divergent.snapshot_required);
    assert_eq!(divergent.snapshot_reason, "divergent_replica");
    let snapshots_head = divergent.snapshots_head.expect("snapshots head");
    assert_eq!(snapshots_head.latest_snapshot_log_index, 1);
    assert_eq!(snapshots_head.latest_snapshot_log_hash, committed.log_hash);
    let mvcc = cluster.states[0].mvcc.as_ref();
    let snapshot_version = mvcc.runtime.local_store().readable_version().unwrap();
    let snapshot_manifest = read_personaldb_snapshot_manifest_by_ref(
        &cluster.states[0].storage,
        mvcc,
        &snapshots_head.latest_snapshot_manifest_ref,
        cluster.states[0].personaldb_protocol_keyring.trust_store(),
        snapshot_version,
    )
    .await
    .unwrap()
    .expect("snapshot manifest ref exists");
    assert_eq!(
        snapshot_manifest.log_index,
        snapshots_head.latest_snapshot_log_index
    );
    assert_eq!(
        snapshot_manifest.log_hash,
        snapshots_head.latest_snapshot_log_hash
    );
    assert_eq!(snapshot_manifest.database_id, database_id);

    let snapshot_object = read_personaldb_snapshot_object(
        &cluster.states[0].storage,
        mvcc,
        1,
        &database_id,
        &snapshot_manifest,
        cluster.states[0].personaldb_protocol_keyring.trust_store(),
        snapshot_version,
    )
    .await
    .unwrap()
    .expect("snapshot object ref exists");
    assert!(!snapshot_object.is_empty());
}
