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
// Internal-only: seeds and reads ownership fences through cluster storage and
// the local secret key, which are not exposed by public/admin APIs.
async fn personaldb_group_creation_requires_current_rfc_ownership_fence() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let conflicting_owner_id = unique_test_name("other-node-personaldb-owner");
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::PersonalDbGroup,
        resource_id: format!("tenant/1/personaldb/{database_id}"),
    };
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    acquire_ownership(
        &cluster.states[0].storage,
        AcquireOwnership {
            request_id: format!("{conflicting_owner_id}-request"),
            idempotency_key: format!("{conflicting_owner_id}-idempotency"),
            resource: resource.clone(),
            owner: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: conflicting_owner_id.clone(),
                actor_instance_id: conflicting_owner_id.clone(),
                display_name: conflicting_owner_id.clone(),
                region: "test-region-1".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap()
                .saturating_mul(1_000_000),
        },
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap();

    let err = client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: hex::encode(hash32(format!("genesis:{database_id}").as_bytes())),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("OwnershipHeld"),
        "unexpected error: {err}"
    );

    let fence = read_ownership_fence(
        &cluster.states[0].storage,
        0,
        &resource,
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap()
    .expect("conflicting owner fence remains durable");
    assert_eq!(fence.owner.principal_id, conflicting_owner_id);
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
    let row_index = read_personaldb_row_index(&cluster.states[0].storage, &row_index_data_id)
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
// Internal-only: removes a PersonalDB payload locator directly from storage to
// force the repair finding under test.
async fn personaldb_repair_verifies_log_chain_and_reports_missing_payload() {
    let cluster = shared_default_test_cluster().await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut personaldb = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut repair = RepairServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut personaldb, &token, &database_id).await;
    let committed = personaldb
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let healthy = repair
        .repair_personal_db_log_chain(authorized(
            RepairPersonalDbLogChainRequest {
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(healthy.status, "up_to_date");
    assert_eq!(healthy.committed_log_index, 1);
    assert_eq!(healthy.verified_log_index, 1);
    assert_eq!(healthy.committed_log_hash, committed.log_hash);
    assert!(healthy.finding.is_none());

    let payload_ref = personaldb_changeset_payload_by_index_ref_name(
        1,
        &database_id,
        committed.log_index,
        &committed.changeset_payload_hash,
    )
    .unwrap();
    delete_personaldb_data_locator_row(
        &cluster.states[0].storage,
        1,
        &database_id,
        &payload_ref,
        "personaldb-repair-remove-payload",
    )
    .await
    .unwrap();

    let report = repair
        .repair_personal_db_log_chain(authorized(
            RepairPersonalDbLogChainRequest {
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "needs_review");
    assert_eq!(report.reason, "PersonalDbChangesetPayloadMissing");
    assert_eq!(report.committed_log_index, 1);
    assert_eq!(report.verified_log_index, 0);
    let finding = report.finding.expect("repair finding");
    assert_eq!(finding.scope_kind, "personaldb");
    assert_eq!(finding.scope_id, format!("tenant-1-database-{database_id}"));
    assert_eq!(finding.status, "RequiresOperatorReview");
    assert_eq!(finding.proposed_action, "VerifyOnly");
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
async fn personaldb_group_commit_uses_partition_owner_signing_key() {
    let cluster = shared_default_test_cluster().await;

    let token = cluster.token.clone();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let genesis_hash = create_group(&mut client, &token, &database_id).await;
    let first = client
        .submit_personal_db_changeset(authorized(
            submit_request(
                &database_id,
                &genesis_hash,
                &token,
                sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3]),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.log_index, 1);

    let partition_id = personaldb_group_partition_id_for_test(1, &database_id);
    let partition_owner_signing_key =
        hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let owner = read_partition_owner(
        &cluster.states[0].storage,
        "personaldb_group",
        &partition_id,
        &partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("PersonalDB commit writes a partition-owner row");
    assert_eq!(owner.owner_node_id, cluster.states[0].config.node_id);
    assert!(owner.fence_token > 0);
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
    let snapshot_manifest = read_personaldb_snapshot_manifest_by_ref(
        &cluster.states[0].storage,
        &snapshots_head.latest_snapshot_manifest_ref,
        cluster.states[0].personaldb_protocol_keyring.trust_store(),
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
        1,
        &database_id,
        &snapshot_manifest,
        cluster.states[0].personaldb_protocol_keyring.trust_store(),
    )
    .await
    .unwrap()
    .expect("snapshot object ref exists");
    assert!(!snapshot_object.is_empty());
}
