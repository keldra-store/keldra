#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use keldra_api::v1::administration_service_server::AdministrationService;
use keldra_consensus::{
    ClusterId, Command, CommittedPeerPins, JoinCapabilityHash, NodeDescriptor, NodeState,
    PeerSpkiSha256,
};
use keldra_store::{StoreOptions, SystemBootstrapRequest};

use super::*;

const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

async fn service() -> (tempfile::TempDir, Store, AdministrationServiceImpl) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: SECRET.into(),
        })
        .unwrap();
    let decisions = DecisionRaft::open(directory.path().join("decisions"), 1, 16, 64 * 1024)
        .await
        .unwrap();
    decisions.ensure_one_node().await.unwrap();
    decisions
        .wait_for_leader(Duration::from_secs(5))
        .await
        .unwrap();
    decisions
        .submit(Command::InitializeCluster {
            cluster_id: ClusterId([12; 16]),
        })
        .await
        .unwrap();
    let admitted = decisions
        .submit(Command::BeginAddNode {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            descriptor: NodeDescriptor {
                node_id: NodeId(1),
                peer_address: PeerAddress("keldra-local://1".into()),
                storage_weight_millionths: 1_000_000,
                state: NodeState::Joining,
                current_peer_spki_sha256: PeerSpkiSha256([1; 32]),
                overlap_peer_spki_sha256: None,
                join_capability_hash: Some(JoinCapabilityHash([1; 32])),
                supported_protocol: PEER_PROTOCOL_CAPABILITY,
                supported_storage_format: STORAGE_FORMAT_CAPABILITY,
            },
        })
        .await
        .unwrap();
    for _ in 0..2 {
        decisions
            .submit(Command::CompleteMembershipTransition {
                format_version: CLUSTER_CONTROL_COMMAND_VERSION,
                started_log_index: admitted.log_index,
            })
            .await
            .unwrap();
    }
    let service =
        AdministrationServiceImpl::new(store.clone(), decisions, directory.path().to_path_buf());
    (directory, store, service)
}

fn authenticated<T>(tenant: StorageTenantId, app_id: &str, body: T) -> Request<T> {
    let mut request = Request::new(body);
    request
        .extensions_mut()
        .insert(Caller::from_authenticated_application(tenant, app_id).unwrap());
    request
}

fn prepare_request() -> api::PrepareNodeRequest {
    api::PrepareNodeRequest {
        node_id: 2,
        peer_address: "127.0.0.1:50062".into(),
        storage_weight_millionths: 500_000,
    }
}

#[tokio::test]
async fn cluster_capability_status_and_activation_are_explicit_and_fenced() {
    let (_directory, _store, service) = service().await;
    let status = service
        .get_cluster_capabilities(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::GetClusterCapabilitiesRequest {},
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.active_protocol_version, 1);
    assert_eq!(status.active_storage_format, 1);
    assert_eq!(status.target_protocol_version, 2);
    assert_eq!(status.target_storage_format, 2);
    assert!(status.ready_for_target_activation);
    assert!(status.blocking_active_node_ids.is_empty());

    let activated = service
        .activate_cluster_capabilities(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ActivateClusterCapabilitiesRequest {
                protocol_version: status.target_protocol_version,
                storage_format: status.target_storage_format,
                expected_placement_term: status.active_placement_term,
                expected_placement_index: status.active_placement_index,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(activated.active_protocol_version, 2);
    assert_eq!(activated.active_storage_format, 2);
}

#[tokio::test]
async fn prepare_node_is_authorized_private_redacted_and_exactly_retryable() {
    let (_directory, _store, service) = service().await;
    let first = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.cluster_id, vec![12; 16]);
    assert_eq!(first.node_id, 2);
    assert_eq!(first.peer_spki_sha256.len(), 32);
    let path = PathBuf::from(&first.join_bundle_path);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
    let encoded = std::fs::read(&path).unwrap();
    let bundle = join_bundle::load(&path).unwrap();
    let state = service.decisions.state().unwrap();
    let descriptor = &state.cluster_control().nodes()[&NodeId(2)];
    assert_eq!(descriptor.state, NodeState::Joining);
    assert_eq!(
        descriptor.current_peer_spki_sha256.0.to_vec(),
        first.peer_spki_sha256
    );
    assert_eq!(
        descriptor.join_capability_hash,
        Some(bundle.capability_hash())
    );
    assert_ne!(
        descriptor.join_capability_hash.unwrap().0,
        bundle.capability()
    );
    assert_eq!(
        state.cluster_control().transition().unwrap().node_id,
        NodeId(2)
    );

    let retry = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry, first);
    assert_eq!(std::fs::read(&path).unwrap(), encoded);

    let mut changed = prepare_request();
    changed.storage_weight_millionths += 1;
    let rejected = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            changed,
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    assert_eq!(std::fs::read(path).unwrap(), encoded);
}

#[tokio::test]
async fn stale_joining_preparation_refreshes_only_unused_material_and_seed_view() {
    let (directory, _store, service) = service().await;
    let first = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    let first_path = PathBuf::from(&first.join_bundle_path);
    let first_bundle = join_bundle::load(&first_path).unwrap();
    // This is the documented operator handoff: copy the bundle to the new
    // host, then delete the generated server-side file.
    std::fs::remove_file(&first_path).unwrap();
    std::fs::File::open(directory.path())
        .unwrap()
        .sync_all()
        .unwrap();
    let before = service.decisions.state().unwrap();
    let original_descriptor = before.cluster_control().nodes()[&NodeId(2)].clone();
    let original_transition = before.cluster_control().transition().cloned().unwrap();

    let new_seed_pin = PeerSpkiSha256([99; 32]);
    service
        .decisions
        .submit(Command::StagePeerSpkiOverlap {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            node_id: NodeId(1),
            expected_current: PeerSpkiSha256([1; 32]),
            overlap: new_seed_pin,
        })
        .await
        .unwrap();
    let fresh_state = service.decisions.state().unwrap();
    let fresh_seeds = active_join_seeds(&fresh_state).unwrap();

    // Simulate a crash after fsyncing the candidate but before proposing
    // the one replacement command. The API must reuse these exact bytes.
    let (_, prepared) = join_bundle::prepare_refresh(
        directory.path(),
        ClusterId([12; 16]),
        NodeId(2),
        PeerAddress("127.0.0.1:50062".into()),
        500_000,
        fresh_seeds.clone(),
    )
    .unwrap();
    let prepared_pin = prepared.peer_spki_sha256().unwrap();
    let prepared_capability = prepared.capability_hash();

    let refreshed = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    let refreshed_bundle = join_bundle::load(&first_path).unwrap();
    assert_eq!(refreshed_bundle, prepared);
    assert_eq!(refreshed_bundle.seeds(), fresh_seeds.as_slice());
    assert!(!join_bundle::refresh_path(directory.path(), NodeId(2)).exists());
    assert_ne!(refreshed.peer_spki_sha256, first.peer_spki_sha256);
    assert_ne!(
        refreshed_bundle.capability_hash(),
        first_bundle.capability_hash()
    );

    let after = service.decisions.state().unwrap();
    let replacement_descriptor = &after.cluster_control().nodes()[&NodeId(2)];
    let mut expected = original_descriptor.clone();
    expected.current_peer_spki_sha256 = prepared_pin;
    expected.join_capability_hash = Some(prepared_capability);
    assert_eq!(replacement_descriptor, &expected);
    assert_eq!(
        after.cluster_control().transition(),
        Some(&original_transition),
        "refresh must preserve the original ADD transition identity"
    );
    assert!(
        !CommittedPeerPins {
            current: replacement_descriptor.current_peer_spki_sha256,
            overlap: replacement_descriptor.overlap_peer_spki_sha256,
        }
        .contains(original_descriptor.current_peer_spki_sha256)
    );

    let encoded = std::fs::read(&first_path).unwrap();
    let exact_retry = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(exact_retry, refreshed);
    assert_eq!(std::fs::read(first_path).unwrap(), encoded);
}

#[tokio::test]
async fn committed_refresh_is_installed_after_restart_style_retry() {
    let (directory, _store, service) = service().await;
    let first = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    let final_path = PathBuf::from(&first.join_bundle_path);
    let old_bytes = std::fs::read(&final_path).unwrap();
    service
        .decisions
        .submit(Command::StagePeerSpkiOverlap {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            node_id: NodeId(1),
            expected_current: PeerSpkiSha256([1; 32]),
            overlap: PeerSpkiSha256([98; 32]),
        })
        .await
        .unwrap();
    let state = service.decisions.state().unwrap();
    let existing = state.cluster_control().nodes()[&NodeId(2)].clone();
    let transition = state.cluster_control().transition().cloned().unwrap();
    let (_, prepared) = join_bundle::prepare_refresh(
        directory.path(),
        ClusterId([12; 16]),
        NodeId(2),
        PeerAddress("127.0.0.1:50062".into()),
        500_000,
        active_join_seeds(&state).unwrap(),
    )
    .unwrap();
    let replacement = joining_descriptor(&prepared).unwrap();
    service
        .decisions
        .submit(Command::RefreshJoiningNodePreparation {
            format_version: CLUSTER_CONTROL_COMMAND_VERSION,
            node_id: NodeId(2),
            started_log_index: transition.started_log_index,
            expected_peer_spki_sha256: existing.current_peer_spki_sha256,
            expected_join_capability_hash: existing.join_capability_hash.unwrap(),
            replacement_peer_spki_sha256: replacement.current_peer_spki_sha256,
            replacement_join_capability_hash: replacement.join_capability_hash.unwrap(),
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(&final_path).unwrap(), old_bytes);
    assert!(join_bundle::refresh_path(directory.path(), NodeId(2)).exists());

    let retried = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            prepare_request(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retried.peer_spki_sha256,
        replacement.current_peer_spki_sha256.0.to_vec()
    );
    assert_eq!(join_bundle::load(&final_path).unwrap(), prepared);
    assert!(!join_bundle::refresh_path(directory.path(), NodeId(2)).exists());
    assert_eq!(
        std::fs::metadata(final_path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
}

#[tokio::test]
async fn prepare_node_requires_manage_system_before_creating_private_material() {
    let (directory, _store, service) = service().await;
    let missing = service
        .prepare_node(Request::new(prepare_request()))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::Unauthenticated);

    let denied = service
        .prepare_node(authenticated(
            StorageTenantId::system(),
            "not-an-admin",
            prepare_request(),
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert!(!join_bundle::generated_path(directory.path(), NodeId(2)).exists());
    let state = service.decisions.state().unwrap();
    assert!(!state.cluster_control().nodes().contains_key(&NodeId(2)));
    assert!(state.cluster_control().transition().is_none());
}

#[tokio::test]
async fn bootstrap_admin_provisions_tenant_and_owner_can_manage_its_resources() {
    let (_directory, store, service) = service().await;
    let provisioned = service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "acme-owner".into(),
                owner_client_id: "acme-owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(provisioned.authorization_revision, 4);
    assert_eq!(provisioned.credential.unwrap().storage_tenant, "acme");

    let acme = StorageTenantId::parse("acme").unwrap();
    let worker = service
        .create_application(authenticated(
            acme.clone(),
            "acme-owner",
            api::CreateApplicationRequest {
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(worker.app_id, "worker");
    assert!(worker.active);
    let bucket = service
        .create_bucket(authenticated(
            acme.clone(),
            "acme-owner",
            api::CreateBucketRequest {
                bucket: "objects".into(),
                versioning: api::ObjectVersioning::Unversioned as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(bucket.authorization_revision, 5);
    assert_eq!(bucket.versioning, api::ObjectVersioning::Unversioned as i32);
    let unauthorized = service
        .set_bucket_versioning(authenticated(
            acme.clone(),
            "worker",
            api::SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: api::ObjectVersioning::Enabled as i32,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(unauthorized.code(), tonic::Code::PermissionDenied);
    let versioning = service
        .set_bucket_versioning(authenticated(
            acme.clone(),
            "acme-owner",
            api::SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: api::ObjectVersioning::Enabled as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(versioning.changed);
    assert_eq!(versioning.storage_tenant, "acme");
    assert_eq!(versioning.bucket, "objects");
    assert_eq!(versioning.versioning, api::ObjectVersioning::Enabled as i32);
    let replay = service
        .set_bucket_versioning(authenticated(
            acme.clone(),
            "acme-owner",
            api::SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: api::ObjectVersioning::Enabled as i32,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!replay.changed);
    let disable = service
        .set_bucket_versioning(authenticated(
            acme.clone(),
            "acme-owner",
            api::SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: api::ObjectVersioning::Unversioned as i32,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(disable.code(), tonic::Code::InvalidArgument);
    let role = service
        .grant_application_role(authenticated(
            acme,
            "acme-owner",
            api::ApplicationRoleRequest {
                app_id: "worker".into(),
                target: Some(api::application_role_request::Target::Bucket(
                    api::BucketApplicationRoleTarget {
                        bucket: "objects".into(),
                        role: api::BucketApplicationRole::Writer.into(),
                    },
                )),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(role.authorization_revision, 6);
    let revoked = service
        .revoke_application_role(authenticated(
            StorageTenantId::parse("acme").unwrap(),
            "acme-owner",
            api::ApplicationRoleRequest {
                app_id: "worker".into(),
                target: Some(api::application_role_request::Target::Bucket(
                    api::BucketApplicationRoleTarget {
                        bucket: "objects".into(),
                        role: api::BucketApplicationRole::Writer.into(),
                    },
                )),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revoked.authorization_revision, 7);
    assert!(!revoked.replayed);
    let denied_public = service
        .set_bucket_public_read(authenticated(
            StorageTenantId::parse("acme").unwrap(),
            "worker",
            api::SetBucketPublicReadRequest {
                bucket: "objects".into(),
                enabled: true,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_public.code(), tonic::Code::PermissionDenied);
    let public = service
        .set_bucket_public_read(authenticated(
            StorageTenantId::parse("acme").unwrap(),
            "acme-owner",
            api::SetBucketPublicReadRequest {
                bucket: "objects".into(),
                enabled: true,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.authorization_revision, 8);
    assert!(public.enabled);
    assert!(!public.replayed);
    let replay = service
        .set_bucket_public_read(authenticated(
            StorageTenantId::parse("acme").unwrap(),
            "acme-owner",
            api::SetBucketPublicReadRequest {
                bucket: "objects".into(),
                enabled: true,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.authorization_revision, 8);
    assert!(replay.replayed);
    let private = service
        .set_bucket_public_read(authenticated(
            StorageTenantId::parse("acme").unwrap(),
            "acme-owner",
            api::SetBucketPublicReadRequest {
                bucket: "objects".into(),
                enabled: false,
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(private.authorization_revision, 9);
    assert!(!private.enabled);
    assert!(
        store
            .application(&StorageTenantId::parse("acme").unwrap(), "worker")
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn tenant_provisioning_rejects_noncanonical_spelling_without_claiming_an_alias() {
    let (_directory, _store, service) = service().await;
    let rejected = service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ProvisionTenantRequest {
                storage_tenant: "Acme".into(),
                owner_app_id: "owner".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);

    let canonical = service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "owner".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(canonical.credential.unwrap().storage_tenant, "acme");
    assert!(!canonical.replayed);
}

#[tokio::test]
async fn requests_without_identity_and_unprivileged_apps_fail_closed() {
    let (_directory, _store, service) = service().await;
    let missing = service
        .provision_tenant(Request::new(api::ProvisionTenantRequest {
            storage_tenant: "acme".into(),
            owner_app_id: "owner".into(),
            owner_client_id: "owner-client".into(),
            owner_client_secret: SECRET.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::Unauthenticated);

    let create_application = service
        .create_application(Request::new(api::CreateApplicationRequest::default()))
        .await
        .unwrap_err();
    let rotate = service
        .rotate_application_credential(Request::new(
            api::RotateApplicationCredentialRequest::default(),
        ))
        .await
        .unwrap_err();
    let recover = service
        .recover_application_credential(Request::new(
            api::RecoverApplicationCredentialRequest::default(),
        ))
        .await
        .unwrap_err();
    let disable = service
        .disable_application_credential(Request::new(
            api::DisableApplicationCredentialRequest::default(),
        ))
        .await
        .unwrap_err();
    let bucket = service
        .create_bucket(Request::new(api::CreateBucketRequest::default()))
        .await
        .unwrap_err();
    let versioning = service
        .set_bucket_versioning(Request::new(api::SetBucketVersioningRequest::default()))
        .await
        .unwrap_err();
    let grant = service
        .grant_application_role(Request::new(api::ApplicationRoleRequest::default()))
        .await
        .unwrap_err();
    let revoke = service
        .revoke_application_role(Request::new(api::ApplicationRoleRequest::default()))
        .await
        .unwrap_err();
    for status in [
        create_application,
        rotate,
        recover,
        disable,
        bucket,
        versioning,
        grant,
        revoke,
    ] {
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    let denied = service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "not-an-admin",
            api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "owner".into(),
                owner_client_id: "owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn application_rotation_disable_and_system_role_assignment_are_typed_and_authorized() {
    let (_directory, store, service) = service().await;
    let system_app = service
        .create_application(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::CreateApplicationRequest {
                app_id: "system-admin".into(),
                client_id: "system-admin-client".into(),
                client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(system_app.storage_tenant, "_keldra");

    let role = service
        .grant_application_role(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ApplicationRoleRequest {
                app_id: "system-admin".into(),
                target: Some(api::application_role_request::Target::System(
                    api::SystemApplicationRoleTarget {
                        role: api::SystemApplicationRole::Admin.into(),
                    },
                )),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(role.authorization_revision, 4);

    service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "system-admin",
            api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "acme-owner".into(),
                owner_client_id: "acme-owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap();
    let acme = StorageTenantId::parse("acme").unwrap();
    service
        .create_application(authenticated(
            acme.clone(),
            "acme-owner",
            api::CreateApplicationRequest {
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap();

    let replacement = "replacement-0123456789abcdef0123456789abcdef0123456789abcdef";
    let rotated = service
        .rotate_application_credential(authenticated(
            acme.clone(),
            "acme-owner",
            api::RotateApplicationCredentialRequest {
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: replacement.into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(rotated.active);
    assert!(
        store
            .verify_credential("worker-client", replacement)
            .unwrap()
            .is_some()
    );
    let disabled = service
        .disable_application_credential(authenticated(
            acme.clone(),
            "acme-owner",
            api::DisableApplicationCredentialRequest {
                app_id: "worker".into(),
                client_id: "worker-client".into(),
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!disabled.active);
    assert!(
        store
            .verify_credential("worker-client", replacement)
            .unwrap()
            .is_none()
    );

    let denied = service
        .grant_application_role(authenticated(
            acme,
            "acme-owner",
            api::ApplicationRoleRequest {
                app_id: "system-admin".into(),
                target: Some(api::application_role_request::Target::System(
                    api::SystemApplicationRoleTarget {
                        role: api::SystemApplicationRole::Admin.into(),
                    },
                )),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn credential_recovery_is_system_only_exact_and_idempotent() {
    let (_directory, store, service) = service().await;
    service
        .provision_tenant(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            api::ProvisionTenantRequest {
                storage_tenant: "acme".into(),
                owner_app_id: "acme-owner".into(),
                owner_client_id: "acme-owner-client".into(),
                owner_client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap();
    let acme = StorageTenantId::parse("acme").unwrap();
    service
        .create_application(authenticated(
            acme.clone(),
            "acme-owner",
            api::CreateApplicationRequest {
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: SECRET.into(),
            },
        ))
        .await
        .unwrap();

    let denied = service
        .recover_application_credential(authenticated(
            acme,
            "acme-owner",
            api::RecoverApplicationCredentialRequest {
                storage_tenant: "not/a/canonical/tenant".into(),
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: "replacement-0123456789abcdef0123456789abcdef".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let unprivileged_system = service
        .recover_application_credential(authenticated(
            StorageTenantId::system(),
            "not-an-admin",
            api::RecoverApplicationCredentialRequest {
                storage_tenant: "not/a/canonical/tenant".into(),
                app_id: "worker".into(),
                client_id: "worker-client".into(),
                client_secret: "replacement-0123456789abcdef0123456789abcdef".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(unprivileged_system.code(), tonic::Code::PermissionDenied);

    for request in [
        api::RecoverApplicationCredentialRequest {
            storage_tenant: "missing".into(),
            app_id: "worker".into(),
            client_id: "worker-client".into(),
            client_secret: "replacement-0123456789abcdef0123456789abcdef".into(),
        },
        api::RecoverApplicationCredentialRequest {
            storage_tenant: "acme".into(),
            app_id: "missing".into(),
            client_id: "worker-client".into(),
            client_secret: "replacement-0123456789abcdef0123456789abcdef".into(),
        },
        api::RecoverApplicationCredentialRequest {
            storage_tenant: "acme".into(),
            app_id: "worker".into(),
            client_id: "acme-owner-client".into(),
            client_secret: "replacement-0123456789abcdef0123456789abcdef".into(),
        },
    ] {
        assert!(
            service
                .recover_application_credential(authenticated(
                    StorageTenantId::system(),
                    "bootstrap-app",
                    request,
                ))
                .await
                .is_err()
        );
    }

    let replacement = "replacement-0123456789abcdef0123456789abcdef";
    let request = api::RecoverApplicationCredentialRequest {
        storage_tenant: "acme".into(),
        app_id: "worker".into(),
        client_id: "worker-client".into(),
        client_secret: replacement.into(),
    };
    let recovered = service
        .recover_application_credential(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            request.clone(),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(recovered.storage_tenant, "acme");
    assert_eq!(recovered.app_id, "worker");
    assert_eq!(recovered.client_id, "worker-client");
    assert!(recovered.active);
    assert!(!recovered.replayed);
    assert!(
        store
            .verify_credential("worker-client", SECRET)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .verify_credential("worker-client", replacement)
            .unwrap()
            .is_some()
    );

    let replay = service
        .recover_application_credential(authenticated(
            StorageTenantId::system(),
            "bootstrap-app",
            request,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);
    let application = store.application(&StorageTenantId::parse("acme").unwrap(), "worker");
    let application = application.unwrap().unwrap();
    assert_eq!(application.client_id, "worker-client");
}

#[test]
fn unspecified_and_unknown_roles_are_rejected_before_storage() {
    for role in [0, 999] {
        let error = role_target_from_api(Some(api::application_role_request::Target::System(
            api::SystemApplicationRoleTarget { role },
        )))
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
