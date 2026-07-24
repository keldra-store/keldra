use super::*;
use crate::storage::Storage;

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

#[tokio::test]
async fn gateway_repository_blob_tag_and_upload_session_are_corestore_records() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let repository = create_gateway_repository(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        "service-account/deployer",
    )
    .await
    .unwrap();
    assert_eq!(repository.schema, GATEWAY_REPOSITORY_SCHEMA);
    assert_eq!(
        read_gateway_repository(&storage, 1, "docker", "registry-a", "containers/api")
            .await
            .unwrap()
            .unwrap(),
        repository
    );

    let payload = b"container layer bytes";
    let digest = digest(payload);
    let blob = put_gateway_blob(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &digest,
        "application/vnd.oci.image.layer.v1.tar+gzip",
        payload,
        "service-account/deployer",
    )
    .await
    .unwrap();
    assert_eq!(blob.schema, GATEWAY_BLOB_SCHEMA);
    let (read_blob, read_payload) = read_gateway_blob(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &digest,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(read_blob, blob);
    assert_eq!(read_payload, payload);

    let first = update_gateway_tag(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        "latest",
        &digest,
        "service-account/deployer",
        None,
    )
    .await
    .unwrap();
    let (_tag, ref_value) = read_gateway_tag(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        "latest",
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ref_value.generation, first.generation);
    let stale = update_gateway_tag(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        "latest",
        &digest,
        "service-account/deployer",
        Some(first.generation.saturating_sub(1)),
    )
    .await;
    assert!(stale.is_err(), "stale tag generation must be rejected");

    let upload = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&digest),
        "service-account/deployer",
        "start-upload-main",
        3600,
    )
    .await
    .unwrap();
    assert_eq!(upload.schema, GATEWAY_UPLOAD_SESSION_SCHEMA);
    assert_eq!(upload.expected_digest.as_deref(), Some(digest.as_str()));
    assert_eq!(upload.state, GatewayUploadSessionState::Open);
}

#[tokio::test]
async fn gateway_upload_finalisation_is_digest_checked_and_commits_session_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let payload = b"first second";
    let expected_digest = digest(payload);
    let upload = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&expected_digest),
        "service-account/deployer",
        "start-upload-finalise",
        3600,
    )
    .await
    .unwrap();

    let first = append_gateway_upload_part(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &upload.upload_id,
        "part_000001",
        0,
        b"first ",
        "idem-first",
    )
    .await
    .unwrap();
    assert_eq!(first.record.state, GatewayUploadSessionState::Receiving);
    let second = append_gateway_upload_part(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &upload.upload_id,
        "part_000002",
        6,
        b"second",
        "idem-second",
    )
    .await
    .unwrap();
    assert_eq!(second.record.received_bytes, payload.len() as u64);

    let wrong_digest = digest(b"wrong");
    let wrong = finalise_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &upload.upload_id,
        Some(&wrong_digest),
        "application/vnd.oci.image.layer.v1.tar+gzip",
        "service-account/deployer",
    )
    .await;
    assert!(
        wrong.is_err(),
        "digest mismatch must reject upload finalisation"
    );

    let blob = finalise_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &upload.upload_id,
        None,
        "application/vnd.oci.image.layer.v1.tar+gzip",
        "service-account/deployer",
    )
    .await
    .unwrap();
    assert_eq!(blob.digest, expected_digest);
    let (_record, bytes) = read_gateway_blob(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &expected_digest,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(bytes, payload);
    let (session, _) = read_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &upload.upload_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(session.state, GatewayUploadSessionState::Committed);
    assert_eq!(
        session.committed_digest.as_deref(),
        Some(expected_digest.as_str())
    );
}

#[tokio::test]
async fn gateway_identifiers_reject_reserved_and_traversal_forms() {
    for bad in [
        "../secret",
        "containers//api",
        "containers/%2e%2e/api",
        "containers\\api",
        "_anvil",
        "_system",
        "_authz",
        "_credentials",
        "containers/_gateway/api",
    ] {
        assert!(
            normalize_gateway_identifier(bad, "test").is_err(),
            "{bad} must be rejected"
        );
    }
    assert_eq!(
        normalize_gateway_identifier("containers/api", "repository").unwrap(),
        "containers/api"
    );
}

#[tokio::test]
async fn gateway_upload_session_start_abort_and_expire_are_corestore_state_transitions() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let payload = b"idempotent upload";
    let expected_digest = digest(payload);

    let first = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&expected_digest),
        "service-account/deployer",
        "same-start-key",
        3600,
    )
    .await
    .unwrap();
    let retry = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&expected_digest),
        "service-account/deployer",
        "same-start-key",
        3600,
    )
    .await
    .unwrap();
    assert_eq!(retry.upload_id, first.upload_id);
    assert_eq!(retry.idempotency_key_hash, first.idempotency_key_hash);

    let conflict = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&digest(b"different")),
        "service-account/deployer",
        "same-start-key",
        3600,
    )
    .await;
    assert!(
        conflict.is_err(),
        "same start idempotency key cannot change target digest"
    );

    let aborted = abort_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &first.upload_id,
    )
    .await
    .unwrap();
    assert_eq!(aborted.record.state, GatewayUploadSessionState::Aborted);
    let append_after_abort = append_gateway_upload_part(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &first.upload_id,
        "part_000001",
        0,
        payload,
        "append-after-abort",
    )
    .await;
    assert!(append_after_abort.is_err());

    let expired = create_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        Some(&expected_digest),
        "service-account/deployer",
        "expired-start-key",
        1,
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let expired = expire_gateway_upload_session(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        &expired.upload_id,
    )
    .await
    .unwrap();
    assert_eq!(expired.record.state, GatewayUploadSessionState::Expired);
}

#[tokio::test]
async fn gateway_audit_records_are_corestore_append_stream_records() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let payload = b"gateway audited payload";
    let expected_digest = digest(payload);
    let record = GatewayAuditRecord {
        schema: String::new(),
        tenant_id: 1,
        gateway: "docker".to_string(),
        registry_instance_id: "registry-a".to_string(),
        operation: "manifest_put".to_string(),
        repository: "containers/api".to_string(),
        package: None,
        version_or_reference: Some("latest".to_string()),
        digest: Some(expected_digest.clone()),
        subject_principal: "service-account/deployer".to_string(),
        credential_id: Some("cred-a".to_string()),
        request_id: "req-0001".to_string(),
        result: "success".to_string(),
        created_at: String::new(),
        record_hash: String::new(),
    };

    let first = append_gateway_audit_record(&storage, record.clone(), Some("request-req-0001"))
        .await
        .unwrap();
    assert_eq!(first.record.schema, GATEWAY_AUDIT_SCHEMA);
    assert_eq!(first.stream.sequence, 1);
    assert!(!first.stream.idempotent_replay);
    assert_eq!(
        first.record.digest.as_deref(),
        Some(expected_digest.as_str())
    );

    let replay = append_gateway_audit_record(&storage, record.clone(), Some("request-req-0001"))
        .await
        .unwrap();
    assert_eq!(replay.stream.sequence, first.stream.sequence);
    assert!(replay.stream.idempotent_replay);

    let read = read_gateway_audit_page(&storage, 1, "docker", "registry-a", 0, 100)
        .await
        .unwrap()
        .records;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].audit, first.record);
    assert_eq!(read[0].stream.record_kind, GATEWAY_AUDIT_SCHEMA);
    let expected_payload = encode_gateway_record(&first.record).unwrap();
    assert_eq!(read[0].stream.payload, expected_payload);
    assert_eq!(
        decode_gateway_record::<GatewayAuditRecord>(&read[0].stream.payload).unwrap(),
        first.record
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&read[0].stream.payload).is_err(),
        "gateway audit records must not be persisted as JSON payloads"
    );
    assert_eq!(
        read[0].stream.payload_hash,
        format!("sha256:{}", sha256_hex(&read[0].stream.payload))
    );

    let bad_record = GatewayAuditRecord {
        repository: "_authz".to_string(),
        request_id: "req-0002".to_string(),
        ..record
    };
    assert!(
        append_gateway_audit_record(&storage, bad_record, None)
            .await
            .is_err(),
        "reserved gateway audit repository names must be rejected"
    );
}

#[tokio::test]
async fn gateway_credential_record_is_corestore_backed_and_hash_checked() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let record = GatewayCredentialRecord {
        schema: GATEWAY_CREDENTIAL_SCHEMA.to_string(),
        tenant_id: 1,
        credential_id: "cred-a".to_string(),
        gateway: "docker".to_string(),
        subject_principal: "service-account/deployer".to_string(),
        secret_hash: hash_gateway_credential_secret("gateway-secret").unwrap(),
        created_at: now_rfc3339(),
        revoked_at: None,
        record_hash: String::new(),
    };
    let generation = put_gateway_credential_record(&storage, record, None)
        .await
        .unwrap();
    assert_eq!(generation, 1);
    let (credential, ref_value) = read_gateway_credential_record(&storage, 1, "docker", "cred-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.subject_principal, "service-account/deployer");
    let revoked_generation =
        revoke_gateway_credential_record(&storage, 1, "docker", "cred-a", ref_value.generation)
            .await
            .unwrap();
    assert!(revoked_generation > generation);
    let (revoked, _) = read_gateway_credential_record(&storage, 1, "docker", "cred-a")
        .await
        .unwrap()
        .unwrap();
    assert!(revoked.revoked_at.is_some());
}

#[tokio::test]
async fn gateway_token_challenge_maps_credential_to_principal_and_revocation() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let record = GatewayCredentialRecord {
        schema: GATEWAY_CREDENTIAL_SCHEMA.to_string(),
        tenant_id: 1,
        credential_id: "cred-token".to_string(),
        gateway: "docker".to_string(),
        subject_principal: "service-account/deployer".to_string(),
        secret_hash: hash_gateway_credential_secret("gateway-secret").unwrap(),
        created_at: now_rfc3339(),
        revoked_at: None,
        record_hash: String::new(),
    };
    put_gateway_credential_record(&storage, record, None)
        .await
        .unwrap();

    let requested_actions = vec!["pull".to_string(), "push".to_string()];
    let token = issue_gateway_access_token(
        &storage,
        1,
        "docker",
        "registry-a",
        "containers/api",
        "cred-token",
        "gateway-secret",
        &requested_actions,
        GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS + 30,
        "gateway-signing-secret",
    )
    .await
    .unwrap();
    assert_eq!(
        token.expires_in_seconds,
        GATEWAY_ACCESS_TOKEN_MAX_TTL_SECONDS
    );
    assert_eq!(token.claims.subject_principal, "service-account/deployer");
    assert_eq!(token.claims.actions, vec!["pull", "push"]);

    let requirement = GatewayTokenRequirement {
        tenant_id: 1,
        gateway: "docker".to_string(),
        registry_instance_id: "registry-a".to_string(),
        repository: "containers/api".to_string(),
        action: "pull".to_string(),
    };
    let claims = validate_gateway_access_token(
        &storage,
        &token.access_token,
        "gateway-signing-secret",
        Some(&requirement),
    )
    .await
    .unwrap();
    assert_eq!(claims.credential_id, "cred-token");

    let wrong_action = GatewayTokenRequirement {
        action: "delete".to_string(),
        ..requirement.clone()
    };
    assert!(
        validate_gateway_access_token(
            &storage,
            &token.access_token,
            "gateway-signing-secret",
            Some(&wrong_action),
        )
        .await
        .is_err(),
        "gateway token scopes must not bypass route-level action checks"
    );

    let (_credential, ref_value) =
        read_gateway_credential_record(&storage, 1, "docker", "cred-token")
            .await
            .unwrap()
            .unwrap();
    revoke_gateway_credential_record(&storage, 1, "docker", "cred-token", ref_value.generation)
        .await
        .unwrap();
    assert!(
        validate_gateway_access_token(
            &storage,
            &token.access_token,
            "gateway-signing-secret",
            Some(&requirement),
        )
        .await
        .is_err(),
        "credential revocation must invalidate previously issued gateway tokens"
    );
}

#[tokio::test]
async fn gateway_mount_resolution_uses_corestore_records_and_fixed_priority() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut exact = GatewayMountRecord {
        schema: GATEWAY_MOUNT_SCHEMA.to_string(),
        mount_id: "docker-primary".to_string(),
        gateway: "docker".to_string(),
        hosts: vec!["registry.example.test".to_string()],
        path_prefixes: vec!["/".to_string(), "/v2/".to_string()],
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        anvil_storage_tenant_id: "storage-tenant-a".to_string(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: "storage-tenant-a".to_string(),
            authz_realm_id: "realm-a".to_string(),
        },
        tenant_id: "tenant-a".to_string(),
        registry_instance_id: "registry-a".to_string(),
        default_bucket: "packages".to_string(),
        repository_prefix: String::new(),
        state: GatewayMountState::Active,
        generation: 0,
        record_hash: String::new(),
    };
    let first_generation = put_gateway_mount_record(&storage, exact.clone(), None)
        .await
        .unwrap();
    assert_eq!(first_generation, 1);

    exact.default_bucket = "packages-v2".to_string();
    let stale =
        put_gateway_mount_record(&storage, exact.clone(), Some(first_generation + 10)).await;
    assert!(stale.is_err(), "stale gateway mount generation is rejected");
    let second_generation = put_gateway_mount_record(&storage, exact, Some(first_generation))
        .await
        .unwrap();
    assert_eq!(second_generation, 2);

    let exact_resolution =
        resolve_gateway_mount(&storage, "REGISTRY.EXAMPLE.TEST.", "/v2/containers/api")
            .await
            .unwrap()
            .expect("exact host alias mount");
    assert_eq!(
        exact_resolution.match_kind,
        GatewayMountMatchKind::ExactHostAlias
    );
    assert_eq!(exact_resolution.matched_path_prefix, "/v2/");
    assert_eq!(exact_resolution.row_generation, 2);
    assert_eq!(
        exact_resolution.record.authz_scope.authz_realm_id,
        "realm-a"
    );
    assert_eq!(exact_resolution.record.default_bucket, "packages-v2");

    let virtual_mount = GatewayMountRecord {
        schema: GATEWAY_MOUNT_SCHEMA.to_string(),
        mount_id: "docker-virtual".to_string(),
        gateway: "docker".to_string(),
        hosts: vec![],
        path_prefixes: vec!["/".to_string()],
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        anvil_storage_tenant_id: "storage-tenant-b".to_string(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: "storage-tenant-b".to_string(),
            authz_realm_id: "realm-b".to_string(),
        },
        tenant_id: "tenant-b".to_string(),
        registry_instance_id: "registry-b".to_string(),
        default_bucket: "packages".to_string(),
        repository_prefix: String::new(),
        state: GatewayMountState::Active,
        generation: 0,
        record_hash: String::new(),
    };
    put_gateway_mount_record(&storage, virtual_mount, None)
        .await
        .unwrap();
    let virtual_resolution = resolve_gateway_mount(
        &storage,
        "registry-b.tenant-b.eu-west-1.anvil-storage.com",
        "/v2/containers/api/manifests/latest",
    )
    .await
    .unwrap()
    .expect("virtual host regional mount");
    assert_eq!(
        virtual_resolution.match_kind,
        GatewayMountMatchKind::VirtualHostRegional
    );
    assert_eq!(
        virtual_resolution.record.authz_scope.authz_realm_id,
        "realm-b"
    );

    let path_style_resolution = resolve_gateway_mount(
        &storage,
        "eu-west-1.anvil-storage.com",
        "/tenant-b/_gateway/docker/registry-b/v2/containers/api/tags/list",
    )
    .await
    .unwrap()
    .expect("path-style regional mount");
    assert_eq!(
        path_style_resolution.match_kind,
        GatewayMountMatchKind::PathStyleRegional
    );
    assert_eq!(
        path_style_resolution.matched_path_prefix,
        "/tenant-b/_gateway/docker/registry-b/"
    );

    assert!(
        resolve_gateway_mount(
            &storage,
            "eu-west-1.anvil-storage.com",
            "/tenant-b/_gateway/npm/registry-b/package",
        )
        .await
        .unwrap()
        .is_none(),
        "route parsing must not infer another gateway from caller-controlled path text"
    );

    let (mut disabled, handle) = read_gateway_mount_record(&storage, "docker-primary")
        .await
        .unwrap()
        .expect("stored exact mount");
    disabled.state = GatewayMountState::Disabled;
    put_gateway_mount_record(&storage, disabled, Some(handle.generation))
        .await
        .unwrap();
    assert!(
        resolve_gateway_mount(&storage, "registry.example.test", "/v2/containers/api")
            .await
            .unwrap()
            .is_none(),
        "disabling a mount must atomically remove its route projections"
    );
}

#[tokio::test]
async fn gateway_mount_resolution_rejects_ambiguous_route_claims() {
    let temp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for mount_id in ["first", "second"] {
        put_gateway_mount_record(
            &storage,
            GatewayMountRecord {
                schema: GATEWAY_MOUNT_SCHEMA.to_string(),
                mount_id: mount_id.to_string(),
                gateway: "docker".to_string(),
                hosts: vec!["registry.example.test".to_string()],
                path_prefixes: vec!["/v2/".to_string()],
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                anvil_storage_tenant_id: format!("storage-{mount_id}"),
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: format!("storage-{mount_id}"),
                    authz_realm_id: format!("realm-{mount_id}"),
                },
                tenant_id: format!("tenant-{mount_id}"),
                registry_instance_id: format!("registry-{mount_id}"),
                default_bucket: "packages".to_string(),
                repository_prefix: String::new(),
                state: GatewayMountState::Active,
                generation: 0,
                record_hash: String::new(),
            },
            None,
        )
        .await
        .unwrap();
    }

    let error = resolve_gateway_mount(&storage, "registry.example.test", "/v2/package")
        .await
        .expect_err("ambiguous exact host route must fail closed");
    assert!(error.to_string().contains("ambiguous"));
}
