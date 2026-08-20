use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use keldra::authentication::{JwtManager, RateLimitConfig};
use keldra::{ServerConfig, serve};
use keldra_api::v1::accounting_service_client::AccountingServiceClient;
use keldra_api::v1::administration_service_client::AdministrationServiceClient;
use keldra_api::v1::batch_get_outcome::Outcome as BatchOutcomeValue;
use keldra_api::v1::bulk_operation::Operation as BulkOperationValue;
use keldra_api::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use keldra_api::v1::index_service_client::IndexServiceClient;
use keldra_api::v1::index_specification::Specification as IndexSpecificationValue;
use keldra_api::v1::object_head::State as HeadState;
use keldra_api::v1::object_service_client::ObjectServiceClient;
use keldra_api::v1::put_header::Operation as PutOperationValue;
use keldra_api::v1::watch_message::Message as WatchMessageValue;
use keldra_api::v1::watch_prefix_request::Start as WatchStart;
use keldra_api::v1::{
    BatchGetRequest, BucketPolicy, BulkOperation, BulkPutRequest, BulkWriteRequest,
    CreateIndexRequest, DeleteIfVersionRequest, DeleteRequest, DeleteVersionRequest,
    DisableAccountingRequest, Durability, EnableAccountingRequest, GetAccountingRequest,
    GetIndexRequest, GetObjectRequest, HeadObjectRequest, IndexQuery, IndexSpecification,
    InvokeProgramRequest, ListObjectVersionsRequest, ListObjectsRequest, MutationFailureCode,
    ObjectAddress, ObjectVersioning as ApiObjectVersioning, PathIndexQuery, PathIndexSpec,
    PutHeader, PutIfAbsentOperation, PutIfVersionOperation, PutImmutableOperation, PutOperation,
    PutRequest, PutToken, QueryIndexRequest, ReadFailureCode, RebuildIndexRequest,
    SetBucketPolicyRequest, SetBucketVersioningRequest, UpdateIndexRequest, WatchNow,
    WatchPrefixRequest, WatchStateHint,
};
use keldra_authz::ObjectRef;
use keldra_store::{
    AuthzRevision, CreateBucketRequest, ObjectVersioning as StoreObjectVersioning,
    ProvisionTenantRequest, StorageTenantId, Store, StoreOptions, SystemBootstrapRequest,
};
use tempfile::TempDir;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status};

const SIGNING_KEY: &[u8] = b"keldra-public-object-surface-test-key";
const OWNER_SECRET: &str = "owner-secret-0123456789abcdef0123456789abcdef";

#[tokio::test]
async fn object_rpc_authentication_distinguishes_anonymous_reads_from_protected_operations() {
    let fixture = Fixture::start().await;
    let mut client = ObjectServiceClient::new(fixture.channel.clone());

    assert_unauthenticated(client.start_put(PutHeader::default()).await);
    assert_unauthenticated(
        client
            .put(tokio_stream::iter([PutRequest::default()]))
            .await,
    );
    assert_unauthenticated(client.put_end(PutToken::default()).await);
    assert_unauthenticated(client.delete(DeleteRequest::default()).await);
    assert_unauthenticated(
        client
            .delete_if_version(DeleteIfVersionRequest::default())
            .await,
    );
    assert_unauthenticated(client.delete_version(DeleteVersionRequest::default()).await);
    assert_unauthenticated(client.bulk_write(BulkWriteRequest::default()).await);
    assert_unauthenticated(client.watch_prefix(WatchPrefixRequest::default()).await);
    assert_unauthenticated(
        client
            .set_bucket_policy(SetBucketPolicyRequest::default())
            .await,
    );
    assert_unauthenticated(client.invoke_program(InvokeProgramRequest::default()).await);

    let mut indexes = IndexServiceClient::new(fixture.channel.clone());
    assert_unauthenticated(indexes.rebuild_index(RebuildIndexRequest::default()).await);

    let private_object = address("private/read.txt");
    assert_permission_denied(
        client
            .head_object(HeadObjectRequest {
                address: Some(private_object.clone()),
            })
            .await,
    );
    assert_permission_denied(
        client
            .list_objects(ListObjectsRequest {
                tenant: "acme".into(),
                bucket: "objects".into(),
                prefix: "private/".into(),
                start_after: None,
                limit: 100,
            })
            .await,
    );
    assert_permission_denied(
        client
            .get_object(GetObjectRequest {
                address: Some(private_object.clone()),
                version: None,
            })
            .await,
    );
    assert_permission_denied(
        client
            .list_object_versions(ListObjectVersionsRequest {
                address: Some(private_object.clone()),
            })
            .await,
    );

    let batch = client
        .batch_get(BatchGetRequest {
            objects: vec![GetObjectRequest {
                address: Some(private_object.clone()),
                version: None,
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        batch.outcomes.as_slice(),
        [outcome]
            if matches!(
                &outcome.outcome,
                Some(BatchOutcomeValue::Failure(failure))
                    if failure.code == ReadFailureCode::AuthorizationDenied as i32
            )
    ));

    let mut invalid_bearer = Request::new(HeadObjectRequest {
        address: Some(private_object),
    });
    invalid_bearer
        .metadata_mut()
        .insert("authorization", "Bearer not-a-valid-jwt".parse().unwrap());
    assert_unauthenticated(client.head_object(invalid_bearer).await);

    fixture.stop().await;
}

#[tokio::test]
async fn one_node_start_put_accepts_local_and_rejects_replicated_before_upload() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let mut client = ObjectServiceClient::new(fixture.channel.clone());

    let local = client
        .start_put(authorized(
            PutHeader {
                address: Some(address("durability/local-upload")),
                content_type: "application/octet-stream".into(),
                command_id: "local-upload-admission".into(),
                durability: Durability::Local as i32,
                operation: Some(PutOperationValue::Put(PutOperation {})),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!local.value.is_empty());

    let replicated = client
        .start_put(authorized(
            PutHeader {
                address: Some(address("durability/replicated-upload")),
                content_type: "application/octet-stream".into(),
                command_id: "replicated-upload-admission".into(),
                durability: Durability::Replicated as i32,
                operation: Some(PutOperationValue::Put(PutOperation {})),
            },
            token,
        ))
        .await
        .unwrap_err();
    assert_eq!(replicated.code(), Code::Unavailable);

    fixture.stop().await;
}

#[tokio::test]
async fn one_node_delete_version_accepts_local_and_rejects_replicated_before_mutation() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let mut administration = AdministrationServiceClient::new(fixture.channel.clone());
    administration
        .set_bucket_versioning(authorized(
            SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: ApiObjectVersioning::Enabled as i32,
            },
            token,
        ))
        .await
        .unwrap();

    let mut client = ObjectServiceClient::new(fixture.channel.clone());
    let object = address("durability/retained-version");
    let first = put_object(
        &mut client,
        token,
        object.clone(),
        b"first",
        "retained-version-first",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let second = put_object(
        &mut client,
        token,
        object.clone(),
        b"second",
        "retained-version-second",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();

    let replicated = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object.clone()),
                version: first.version,
                durability: Durability::Replicated as i32,
            },
            token,
        ))
        .await
        .unwrap_err();
    assert_eq!(replicated.code(), Code::Unavailable);
    assert_eq!(
        list_version_ids(&mut client, &object, token).await,
        [first.version, second.version]
    );

    let local = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object.clone()),
                version: first.version,
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(local.deleted);
    assert_eq!(local.replacement_tombstone_version, None);
    assert_eq!(
        list_version_ids(&mut client, &object, token).await,
        [second.version]
    );

    fixture.stop().await;
}

#[tokio::test]
async fn public_accounting_lifecycle_materializes_scalar_usage() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let mut accounting = AccountingServiceClient::new(fixture.channel.clone());
    let mut objects = ObjectServiceClient::new(fixture.channel.clone());

    let definition = accounting
        .enable_accounting(authorized(
            EnableAccountingRequest {
                bucket: "objects".into(),
                path_prefix: "billable".into(),
                command_id: "accounting-enable-billable".into(),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(definition.accounting_id, 0);
    assert_ne!(definition.version, 0);

    // Definition discovery is deliberately asynchronous. Once discovered,
    // the public traffic meter and scalar worker consume the same definition.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let payload = b"billable payload";
    put_object(
        &mut objects,
        token,
        address("billable/one.bin"),
        payload,
        "accounting-billable-put",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();

    // Idle accounting workers yield their bounded lease. Relevant journal
    // effects wake them through the bounded accounting handoff.
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let snapshot = accounting
            .get_accounting(authorized(
                GetAccountingRequest {
                    bucket: "objects".into(),
                    path_prefix: "billable".into(),
                },
                token,
            ))
            .await
            .unwrap()
            .into_inner();
        if snapshot.object_count == 1
            && snapshot.logical_stored_bytes == payload.len() as u64
            && snapshot.accepted_inbound_bytes >= payload.len() as u64
            && snapshot
                .freshness
                .as_ref()
                .is_some_and(|value| value.complete)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "accounting rollup did not converge: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let disabled = accounting
        .disable_accounting(authorized(
            DisableAccountingRequest {
                bucket: "objects".into(),
                path_prefix: "billable".into(),
                expected_version: definition.version,
                command_id: "accounting-disable-billable".into(),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(disabled.disabled);
    assert_ne!(disabled.tombstone_version, 0);

    fixture.stop().await;
}

#[tokio::test]
async fn raw_reserved_objects_are_denied_but_trusted_adapters_still_work() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let reserved = address("_keldra/internal/00");
    let mut objects = ObjectServiceClient::new(fixture.channel.clone());

    assert_permission_denied(
        objects
            .start_put(authorized(
                PutHeader {
                    address: Some(reserved.clone()),
                    content_type: "application/octet-stream".into(),
                    command_id: "raw-reserved-put".into(),
                    durability: Durability::Local as i32,
                    operation: Some(PutOperationValue::Put(PutOperation {})),
                },
                token,
            ))
            .await,
    );
    assert_permission_denied(
        objects
            .head_object(authorized(
                HeadObjectRequest {
                    address: Some(reserved.clone()),
                },
                token,
            ))
            .await,
    );
    assert_permission_denied(
        objects
            .get_object(authorized(
                GetObjectRequest {
                    address: Some(reserved.clone()),
                    version: None,
                },
                token,
            ))
            .await,
    );
    assert_permission_denied(
        objects
            .delete(authorized(
                DeleteRequest {
                    address: Some(reserved.clone()),
                    command_id: "raw-reserved-delete".into(),
                    durability: Durability::Local as i32,
                },
                token,
            ))
            .await,
    );

    let bulk = objects
        .bulk_write(authorized(
            BulkWriteRequest {
                operations: vec![BulkOperation {
                    operation: Some(BulkOperationValue::Put(bulk_put(
                        reserved.clone(),
                        b"must not persist",
                        "raw-reserved-bulk",
                    ))),
                }],
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        bulk.outcomes[0].outcome,
        Some(BulkOutcomeValue::Failure(ref failure))
            if failure.code == MutationFailureCode::AuthorizationDenied as i32
    ));

    let batch = objects
        .batch_get(authorized(
            BatchGetRequest {
                objects: vec![GetObjectRequest {
                    address: Some(reserved),
                    version: None,
                }],
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        batch.outcomes[0].outcome,
        Some(BatchOutcomeValue::Failure(ref failure))
            if failure.code == keldra_api::v1::ReadFailureCode::AuthorizationDenied as i32
    ));

    // The public IndexService is a trusted adapter: it may persist its
    // definition through the same ordinary object pipeline, while callers
    // still cannot address that reserved object through ObjectService.
    let mut indexes = IndexServiceClient::new(fixture.channel.clone());
    let definition = indexes
        .create_index(authorized(
            CreateIndexRequest {
                bucket: "objects".into(),
                name: "reserved-boundary".into(),
                path_prefix: "docs/".into(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(IndexSpecificationValue::Path(PathIndexSpec {})),
                }),
                command_id: "create-reserved-boundary".into(),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(definition.version, 0);
    assert_permission_denied(
        objects
            .head_object(authorized(
                HeadObjectRequest {
                    address: Some(address("_keldra/indexes/definitions/reserved-boundary")),
                },
                token,
            ))
            .await,
    );

    // Program definitions are the one Zanzibar-authorized public exception.
    let program = address("_keldra/programs/reserved-boundary@1");
    put_object(
        &mut objects,
        token,
        program.clone(),
        br#"{"steps":[]}"#,
        "public-program-definition",
        PutOperationValue::PutImmutable(PutImmutableOperation {}),
    )
    .await
    .unwrap();
    assert!(matches!(
        head(&mut objects, &program, token).await.state,
        Some(HeadState::Present(_))
    ));

    fixture.stop().await;
}

#[tokio::test]
async fn index_lifecycle_requires_zanzibar_access_to_the_definition_object() {
    let fixture = Fixture::start().await;
    let owner_token = fixture.access_token.as_str();
    let denied_token = JwtManager::new(SIGNING_KEY)
        .unwrap()
        .mint(StorageTenantId::parse("acme").unwrap(), "unprivileged-app")
        .unwrap();
    let mut indexes = IndexServiceClient::new(fixture.channel.clone());

    let create = CreateIndexRequest {
        bucket: "objects".into(),
        name: "authorization-boundary".into(),
        path_prefix: "docs/".into(),
        content_type: String::new(),
        specification: Some(IndexSpecification {
            specification: Some(IndexSpecificationValue::Path(PathIndexSpec {})),
        }),
        command_id: "create-authorization-boundary".into(),
    };
    assert_permission_denied(
        indexes
            .create_index(authorized(create.clone(), &denied_token))
            .await,
    );

    let created = indexes
        .create_index(authorized(create, owner_token))
        .await
        .unwrap()
        .into_inner();
    assert_permission_denied(
        indexes
            .get_index(authorized(
                GetIndexRequest {
                    bucket: "objects".into(),
                    name: "authorization-boundary".into(),
                },
                &denied_token,
            ))
            .await,
    );
    assert_permission_denied(
        indexes
            .update_index(authorized(
                UpdateIndexRequest {
                    bucket: "objects".into(),
                    name: "authorization-boundary".into(),
                    expected_version: created.version,
                    path_prefix: "docs/updated".into(),
                    content_type: String::new(),
                    specification: Some(IndexSpecification {
                        specification: Some(IndexSpecificationValue::Path(PathIndexSpec {})),
                    }),
                    command_id: "update-authorization-boundary".into(),
                },
                &denied_token,
            ))
            .await,
    );
    assert_permission_denied(
        indexes
            .rebuild_index(authorized(
                RebuildIndexRequest {
                    bucket: "objects".into(),
                    name: "authorization-boundary".into(),
                    expected_version: created.version,
                    command_id: "rebuild-authorization-denied".into(),
                },
                &denied_token,
            ))
            .await,
    );
    assert_permission_denied(
        indexes
            .query_index(authorized(
                QueryIndexRequest {
                    bucket: "objects".into(),
                    index_name: "authorization-boundary".into(),
                    query: Some(IndexQuery {
                        query: Some(keldra_api::v1::index_query::Query::Path(PathIndexQuery {
                            prefix: String::new(),
                            start_after: None,
                        })),
                    }),
                    limit: 10,
                    page_token: Vec::new(),
                    tenant: String::new(),
                },
                &denied_token,
            ))
            .await,
    );

    let rebuilt = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: created.version,
                command_id: "rebuild-authorization-boundary".into(),
            },
            owner_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(rebuilt.version > created.version);
    let mut expected = created.clone();
    expected.version = rebuilt.version;
    assert_eq!(rebuilt, expected);
    let replayed = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: created.version,
                command_id: "rebuild-authorization-boundary".into(),
            },
            owner_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replayed, rebuilt);
    assert_eq!(
        indexes
            .get_index(authorized(
                GetIndexRequest {
                    bucket: "objects".into(),
                    name: "authorization-boundary".into(),
                },
                owner_token,
            ))
            .await
            .unwrap()
            .into_inner(),
        rebuilt
    );
    let stale = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: created.version,
                command_id: "rebuild-authorization-stale".into(),
            },
            owner_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition, "{stale:?}");
    let rate_limited = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: rebuilt.version,
                command_id: "rebuild-authorization-rate-limited".into(),
            },
            owner_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(rate_limited.code(), Code::ResourceExhausted);
    assert!(
        rate_limited
            .message()
            .contains("index rebuild is rate limited")
    );

    let updated = indexes
        .update_index(authorized(
            UpdateIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: rebuilt.version,
                path_prefix: "docs/updated".into(),
                content_type: String::new(),
                specification: Some(IndexSpecification {
                    specification: Some(IndexSpecificationValue::Path(PathIndexSpec {})),
                }),
                command_id: "update-after-rebuild".into(),
            },
            owner_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let replay_after_update = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: created.version,
                command_id: "rebuild-authorization-boundary".into(),
            },
            owner_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(replay_after_update.code(), Code::AlreadyExists);
    let rate_limited_after_update = indexes
        .rebuild_index(authorized(
            RebuildIndexRequest {
                bucket: "objects".into(),
                name: "authorization-boundary".into(),
                expected_version: updated.version,
                command_id: "rebuild-after-semantic-update".into(),
            },
            owner_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(rate_limited_after_update.code(), Code::ResourceExhausted);

    fixture.stop().await;
}

#[tokio::test]
async fn explicit_put_modes_cas_delete_head_batch_bulk_and_list_work_over_grpc() {
    let fixture = Fixture::start().await;
    let mut client = ObjectServiceClient::new(fixture.channel.clone());
    let token = fixture.access_token.as_str();

    let never = head(&mut client, &address("head/never"), token).await;
    assert!(matches!(never.state, Some(HeadState::NeverExisted(_))));

    let put_address = address("modes/put");
    let put_first = put_object(
        &mut client,
        token,
        put_address.clone(),
        b"one",
        "mode-put-one",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let put_second = put_object(
        &mut client,
        token,
        put_address.clone(),
        b"two",
        "mode-put-two",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    assert!(put_second.version > put_first.version);
    let present = assert_present(head(&mut client, &put_address, token).await);
    assert_eq!(present.version, put_second.version);
    assert_eq!(present.content_length, 3);

    let absent_address = address("modes/if-absent");
    let absent = put_object(
        &mut client,
        token,
        absent_address.clone(),
        b"created",
        "mode-absent-one",
        PutOperationValue::PutIfAbsent(PutIfAbsentOperation {}),
    )
    .await
    .unwrap();
    let condition = put_object(
        &mut client,
        token,
        absent_address,
        b"must-not-replace",
        "mode-absent-two",
        PutOperationValue::PutIfAbsent(PutIfAbsentOperation {}),
    )
    .await
    .unwrap_err();
    assert_eq!(condition.code(), Code::FailedPrecondition);

    let cas_address = address("modes/if-version");
    let cas_base = put_object(
        &mut client,
        token,
        cas_address.clone(),
        b"base",
        "mode-cas-base",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let cas = put_object(
        &mut client,
        token,
        cas_address.clone(),
        b"replacement",
        "mode-cas-replace",
        PutOperationValue::PutIfVersion(PutIfVersionOperation {
            expected_version: cas_base.version,
        }),
    )
    .await
    .unwrap();
    let stale = put_object(
        &mut client,
        token,
        cas_address,
        b"stale",
        "mode-cas-stale",
        PutOperationValue::PutIfVersion(PutIfVersionOperation {
            expected_version: cas_base.version,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert!(cas.version > cas_base.version);

    let policy = client
        .set_bucket_policy(authorized(
            SetBucketPolicyRequest {
                tenant: "acme".into(),
                bucket: "objects".into(),
                policy: Some(BucketPolicy {
                    immutable_path_prefixes: vec!["immutable".into()],
                    program_only_path_prefixes: Vec::new(),
                }),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(policy.immutable_path_prefixes, ["immutable"]);
    let immutable_address = address("immutable/entry");
    let immutable = put_object(
        &mut client,
        token,
        immutable_address.clone(),
        b"write-once",
        "mode-immutable-one",
        PutOperationValue::PutImmutable(PutImmutableOperation {}),
    )
    .await
    .unwrap();
    let identical = put_object(
        &mut client,
        token,
        immutable_address.clone(),
        b"write-once",
        "mode-immutable-identical",
        PutOperationValue::PutImmutable(PutImmutableOperation {}),
    )
    .await
    .unwrap();
    assert_eq!(identical.version, immutable.version);
    assert!(identical.replayed);
    let immutable_conflict = put_object(
        &mut client,
        token,
        immutable_address,
        b"different",
        "mode-immutable-different",
        PutOperationValue::PutImmutable(PutImmutableOperation {}),
    )
    .await
    .unwrap_err();
    assert_eq!(immutable_conflict.code(), Code::FailedPrecondition);

    let delete_address = address("delete/cas");
    let before_delete = put_object(
        &mut client,
        token,
        delete_address.clone(),
        b"delete me",
        "delete-cas-create",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let deleted = client
        .delete_if_version(authorized(
            DeleteIfVersionRequest {
                address: Some(delete_address.clone()),
                command_id: "delete-cas-exact".into(),
                durability: Durability::Local as i32,
                expected_version: before_delete.version,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(deleted.deleted);
    assert!(deleted.version > before_delete.version);
    assert_eq!(
        assert_deleted(head(&mut client, &delete_address, token).await),
        deleted.version
    );
    let stale_delete = client
        .delete_if_version(authorized(
            DeleteIfVersionRequest {
                address: Some(delete_address.clone()),
                command_id: "delete-cas-stale".into(),
                durability: Durability::Local as i32,
                expected_version: before_delete.version,
            },
            token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_delete.code(), Code::FailedPrecondition);
    let recreated = put_object(
        &mut client,
        token,
        delete_address.clone(),
        b"recreated",
        "delete-cas-recreate",
        PutOperationValue::PutIfVersion(PutIfVersionOperation {
            expected_version: deleted.version,
        }),
    )
    .await
    .unwrap();
    let unconditional = client
        .delete(authorized(
            DeleteRequest {
                address: Some(delete_address.clone()),
                command_id: "delete-unconditional".into(),
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(unconditional.deleted);
    assert!(unconditional.version > recreated.version);

    let batch_live_address = address("batch/live");
    put_object(
        &mut client,
        token,
        batch_live_address.clone(),
        b"batch-live",
        "batch-live-put",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let batch_deleted_address = address("batch/deleted");
    put_object(
        &mut client,
        token,
        batch_deleted_address.clone(),
        b"batch-deleted",
        "batch-deleted-put",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    client
        .delete(authorized(
            DeleteRequest {
                address: Some(batch_deleted_address.clone()),
                command_id: "batch-deleted-delete".into(),
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap();
    let batch_never_address = address("batch/never");
    let batch = client
        .batch_get(authorized(
            BatchGetRequest {
                objects: vec![
                    GetObjectRequest {
                        address: Some(batch_live_address.clone()),
                        version: None,
                    },
                    GetObjectRequest {
                        address: Some(batch_deleted_address.clone()),
                        version: None,
                    },
                    GetObjectRequest {
                        address: Some(batch_never_address),
                        version: None,
                    },
                ],
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(batch.outcomes.len(), 3);
    for (expected, outcome) in batch.outcomes.iter().enumerate() {
        assert_eq!(outcome.index, expected as u32);
    }
    let Some(BatchOutcomeValue::Object(live)) = batch.outcomes[0].outcome.as_ref() else {
        panic!("live batch read did not return an object");
    };
    assert_eq!(live.bytes, b"batch-live");
    assert!(matches!(
        live.head.as_ref().and_then(|head| head.state.as_ref()),
        Some(HeadState::Present(_))
    ));
    let Some(BatchOutcomeValue::Object(deleted)) = batch.outcomes[1].outcome.as_ref() else {
        panic!("deleted batch read did not return an object state");
    };
    assert!(deleted.bytes.is_empty());
    assert!(matches!(
        deleted.head.as_ref().and_then(|head| head.state.as_ref()),
        Some(HeadState::Deleted(_))
    ));
    let Some(BatchOutcomeValue::Object(never)) = batch.outcomes[2].outcome.as_ref() else {
        panic!("never-existed batch read did not return an object state");
    };
    assert!(never.bytes.is_empty());
    assert!(matches!(
        never.head.as_ref().and_then(|head| head.state.as_ref()),
        Some(HeadState::NeverExisted(_))
    ));

    let bulk_existing_address = address("bulk/existing");
    let bulk_existing = put_object(
        &mut client,
        token,
        bulk_existing_address.clone(),
        b"existing",
        "bulk-existing-seed",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let bulk_new_address = address("bulk/new");
    let bulk = client
        .bulk_write(authorized(
            BulkWriteRequest {
                operations: vec![
                    BulkOperation {
                        operation: Some(BulkOperationValue::Put(bulk_put(
                            bulk_new_address.clone(),
                            b"new",
                            "bulk-new-put",
                        ))),
                    },
                    BulkOperation {
                        operation: Some(BulkOperationValue::PutIfAbsent(bulk_put(
                            bulk_existing_address.clone(),
                            b"must-fail",
                            "bulk-existing-absent",
                        ))),
                    },
                    BulkOperation {
                        operation: Some(BulkOperationValue::DeleteIfVersion(
                            DeleteIfVersionRequest {
                                address: Some(bulk_existing_address.clone()),
                                command_id: "bulk-existing-delete".into(),
                                durability: Durability::Local as i32,
                                expected_version: bulk_existing.version,
                            },
                        )),
                    },
                ],
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(bulk.outcomes.len(), 3);
    let Some(BulkOutcomeValue::Receipt(created)) = bulk.outcomes[0].outcome.as_ref() else {
        panic!("first independent bulk operation did not commit");
    };
    assert!(!created.deleted);
    let Some(BulkOutcomeValue::Failure(failure)) = bulk.outcomes[1].outcome.as_ref() else {
        panic!("failing independent bulk operation did not report a failure");
    };
    // 0.5.1 preserves the failed mutation but may lose its condition-specific
    // classification while crossing the coordinator boundary.
    assert!(matches!(
        MutationFailureCode::try_from(failure.code).unwrap(),
        MutationFailureCode::ConditionFailed | MutationFailureCode::Invalid
    ));
    let Some(BulkOutcomeValue::Receipt(deleted)) = bulk.outcomes[2].outcome.as_ref() else {
        panic!("third independent bulk operation did not commit");
    };
    assert!(deleted.deleted);
    assert!(matches!(
        head(&mut client, &bulk_new_address, token).await.state,
        Some(HeadState::Present(_))
    ));
    assert!(matches!(
        head(&mut client, &bulk_existing_address, token).await.state,
        Some(HeadState::Deleted(_))
    ));

    for (path, command) in [
        ("list/alpha", "list-alpha"),
        ("list/bravo", "list-bravo"),
        ("list/charlie", "list-charlie"),
        ("list/delta", "list-delta"),
        ("outside/echo", "list-outside"),
    ] {
        put_object(
            &mut client,
            token,
            address(path),
            path.as_bytes(),
            command,
            PutOperationValue::Put(PutOperation {}),
        )
        .await
        .unwrap();
    }
    client
        .delete(authorized(
            DeleteRequest {
                address: Some(address("list/bravo")),
                command_id: "list-bravo-delete".into(),
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap();
    let first_page = client
        .list_objects(authorized(
            ListObjectsRequest {
                tenant: "acme".into(),
                bucket: "objects".into(),
                prefix: "list/".into(),
                start_after: None,
                limit: 2,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.paths, ["list/alpha", "list/charlie"]);
    assert!(first_page.has_more);
    let second_page = client
        .list_objects(authorized(
            ListObjectsRequest {
                tenant: "acme".into(),
                bucket: "objects".into(),
                prefix: "list/".into(),
                start_after: first_page.paths.last().cloned(),
                limit: 2,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.paths, ["list/delta"]);
    assert!(!second_page.has_more);

    assert_ne!(absent.version, 0);
    fixture.stop().await;
}

#[tokio::test]
async fn versioned_delete_version_never_resurrects_an_older_payload() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let mut administration = AdministrationServiceClient::new(fixture.channel.clone());
    let enabled = administration
        .set_bucket_versioning(authorized(
            SetBucketVersioningRequest {
                bucket: "objects".into(),
                versioning: ApiObjectVersioning::Enabled as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(enabled.changed);
    assert_eq!(enabled.versioning, ApiObjectVersioning::Enabled as i32);

    let mut client = ObjectServiceClient::new(fixture.channel.clone());
    let object = address("versioned/document");
    let first = put_object(
        &mut client,
        token,
        object.clone(),
        b"first",
        "versioned-first",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let second = put_object(
        &mut client,
        token,
        object.clone(),
        b"second",
        "versioned-second",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    assert!(second.version > first.version);
    assert_eq!(
        list_version_ids(&mut client, &object, token).await,
        [first.version, second.version]
    );

    let non_current = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object.clone()),
                version: first.version,
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(non_current.deleted);
    assert_eq!(non_current.replacement_tombstone_version, None);
    assert_eq!(
        assert_present(head(&mut client, &object, token).await).version,
        second.version
    );

    let current = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object.clone()),
                version: second.version,
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(current.deleted);
    let tombstone = current
        .replacement_tombstone_version
        .expect("deleting the live head must publish a replacement tombstone");
    assert!(tombstone > second.version);
    assert_eq!(
        assert_deleted(head(&mut client, &object, token).await),
        tombstone
    );
    assert_eq!(
        list_version_ids(&mut client, &object, token).await,
        [tombstone]
    );

    let fence = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object.clone()),
                version: tombstone,
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap_err();
    assert_eq!(fence.code(), Code::FailedPrecondition);
    // The 0.5.1 coordinator path does not preserve the stable tombstone error
    // prefix; the failed precondition and unchanged head are the guarantee.
    assert_eq!(
        assert_deleted(head(&mut client, &object, token).await),
        tombstone
    );

    let missing = client
        .delete_version(authorized(
            DeleteVersionRequest {
                address: Some(object),
                version: u64::MAX,
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!missing.deleted);
    assert_eq!(missing.replacement_tombstone_version, None);

    fixture.stop().await;
}

#[tokio::test]
async fn watch_prefix_streams_present_and_deleted_invalidations_with_checkpoints() {
    let fixture = Fixture::start().await;
    let token = fixture.access_token.as_str();
    let watched = address("watched/item");
    let mut watch_client = ObjectServiceClient::new(fixture.channel.clone());
    let mut writer = ObjectServiceClient::new(fixture.channel.clone());
    let mut watch = watch_client
        .watch_prefix(authorized(
            WatchPrefixRequest {
                prefix: Some(address("watched")),
                start: Some(WatchStart::Now(WatchNow {})),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();

    let initial_checkpoint = checkpoint(next_watch(&mut watch).await);
    assert!(!initial_checkpoint.is_empty());
    let created = put_object(
        &mut writer,
        token,
        watched.clone(),
        b"watched",
        "watch-create",
        PutOperationValue::Put(PutOperation {}),
    )
    .await
    .unwrap();
    let present = next_invalidation(&mut watch).await;
    assert_eq!(present.address, Some(watched.clone()));
    assert_eq!(present.minimum_path_version, created.version);
    assert_eq!(
        WatchStateHint::try_from(present.state_hint).unwrap(),
        WatchStateHint::Present
    );
    let resume_token = checkpoint(next_watch(&mut watch).await);
    assert!(!resume_token.is_empty());
    drop(watch);

    let mut resumed = watch_client
        .watch_prefix(authorized(
            WatchPrefixRequest {
                prefix: Some(address("watched")),
                start: Some(WatchStart::ResumeToken(resume_token)),
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!checkpoint(next_watch(&mut resumed).await).is_empty());
    let deleted = writer
        .delete(authorized(
            DeleteRequest {
                address: Some(watched.clone()),
                command_id: "watch-delete".into(),
                durability: Durability::Local as i32,
            },
            token,
        ))
        .await
        .unwrap()
        .into_inner();
    let invalidated = next_invalidation(&mut resumed).await;
    assert_eq!(invalidated.address, Some(watched));
    assert_eq!(invalidated.minimum_path_version, deleted.version);
    assert_eq!(
        WatchStateHint::try_from(invalidated.state_hint).unwrap(),
        WatchStateHint::Deleted
    );
    assert!(!checkpoint(next_watch(&mut resumed).await).is_empty());

    fixture.stop().await;
}

struct Fixture {
    _directory: TempDir,
    channel: Channel,
    access_token: String,
    server: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Fixture {
    async fn start() -> Self {
        let directory = tempfile::tempdir().unwrap();
        seed_authorized_bucket(&directory).await;
        let token_manager = JwtManager::new(SIGNING_KEY).unwrap();
        let access_token = token_manager
            .mint(StorageTenantId::parse("acme").unwrap(), "owner-app")
            .unwrap();
        let listen = unused_loopback_address();
        let server = tokio::spawn(serve(test_server_config(&directory, listen, token_manager)));
        let channel = connect_when_ready(listen).await;
        Self {
            _directory: directory,
            channel,
            access_token,
            server,
        }
    }

    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }
}

async fn seed_authorized_bucket(directory: &TempDir) {
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    store
        .bootstrap_system(SystemBootstrapRequest {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            client_secret: "bootstrap-secret-0123456789abcdef0123456789abcdef".into(),
        })
        .unwrap();
    let owner = ObjectRef::opaque("app", "owner-app").unwrap();
    store
        .provision_tenant(ProvisionTenantRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            owner_app_id: "owner-app".into(),
            owner_client_id: "owner-client".into(),
            owner_client_secret: OWNER_SECRET.into(),
            principal: ObjectRef::opaque("app", "bootstrap-app").unwrap(),
            expected_authorization_revision: AuthzRevision(3),
            expected_binding_generation: 1,
        })
        .unwrap();
    store
        .create_bucket(CreateBucketRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            bucket: "objects".into(),
            owner: owner.clone(),
            principal: owner,
            expected_authorization_revision: AuthzRevision(4),
            expected_binding_generation: 1,
            versioning: StoreObjectVersioning::Unversioned,
        })
        .unwrap();
}

fn test_server_config(
    directory: &TempDir,
    listen: SocketAddr,
    token_manager: JwtManager,
) -> ServerConfig {
    let mut peer_listen = unused_loopback_address();
    while peer_listen == listen {
        peer_listen = unused_loopback_address();
    }
    ServerConfig {
        listen,
        peer_listen,
        peer_advertise: None,
        join_bundle: None,
        storage: keldra::StoragePaths::under(directory.path(), 8 * 1024 * 1024),
        explicit_authoritative_paths: keldra::ExplicitAuthoritativePaths::default(),
        run_system_bootstrap: true,
        system_bootstrap_credential_output: None,
        node_id: 1,
        max_atomic_commit_entries: 128,
        max_atomic_commit_bytes: 1024 * 1024,
        atomic_program_timeout: Duration::from_secs(30),
        index_query_timeout: Duration::from_secs(300),
        token_manager,
        rate_limits: RateLimitConfig::default(),
        index_runtime: keldra::IndexRuntimeConfig::default(),
        plugin_gateway: keldra::PluginGatewayConfig::default(),
        max_blob_bytes: 1024 * 1024,
        erasure_profile: keldra_store::ErasureProfile::default(),
        awaiting_publish_ttl_seconds: keldra_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS,
        mutation_receipt_retention_seconds: 60,
        max_mutation_receipt_entries: 512,
        max_mutation_receipt_bytes: 1024 * 1024,
        source_journal_max_entries: 512,
        source_journal_max_bytes: 1024 * 1024,
    }
}

fn unused_loopback_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn connect_when_ready(listen: SocketAddr) -> Channel {
    let endpoint = Endpoint::from_shared(format!("http://{listen}"))
        .unwrap()
        .connect_timeout(Duration::from_millis(100));
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return channel,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("Keldra test server did not start: {error}"),
        }
    }
}

fn authorized<T>(value: T, access_token: &str) -> Request<T> {
    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {access_token}").parse().unwrap(),
    );
    request
}

fn assert_unauthenticated<T>(result: Result<Response<T>, Status>) {
    match result {
        Ok(_) => panic!("protected object RPC accepted an unauthenticated request"),
        Err(status) => assert_eq!(status.code(), Code::Unauthenticated),
    }
}

fn assert_permission_denied<T>(result: Result<Response<T>, Status>) {
    match result {
        Ok(_) => panic!("reserved object RPC accepted a raw public request"),
        Err(status) => assert_eq!(status.code(), Code::PermissionDenied),
    }
}

fn address(path: &str) -> ObjectAddress {
    ObjectAddress {
        tenant: "acme".into(),
        bucket: "objects".into(),
        path: path.into(),
    }
}

async fn put_object(
    client: &mut ObjectServiceClient<Channel>,
    access_token: &str,
    address: ObjectAddress,
    bytes: &[u8],
    command_id: &str,
    operation: PutOperationValue,
) -> Result<keldra_api::v1::MutationReceipt, Status> {
    let upload = client
        .start_put(authorized(
            PutHeader {
                address: Some(address),
                content_type: "application/octet-stream".into(),
                command_id: command_id.into(),
                durability: Durability::Local as i32,
                operation: Some(operation),
            },
            access_token,
        ))
        .await?
        .into_inner();
    let ready = client
        .put(authorized(
            tokio_stream::iter([PutRequest {
                token: Some(upload),
                chunk: bytes.to_vec(),
            }]),
            access_token,
        ))
        .await?
        .into_inner();
    client
        .put_end(authorized(ready, access_token))
        .await
        .map(Response::into_inner)
}

async fn head(
    client: &mut ObjectServiceClient<Channel>,
    address: &ObjectAddress,
    access_token: &str,
) -> keldra_api::v1::ObjectHead {
    client
        .head_object(authorized(
            HeadObjectRequest {
                address: Some(address.clone()),
            },
            access_token,
        ))
        .await
        .unwrap()
        .into_inner()
}

fn assert_present(head: keldra_api::v1::ObjectHead) -> keldra_api::v1::PresentObject {
    let Some(HeadState::Present(present)) = head.state else {
        panic!("object head was not present");
    };
    present
}

fn assert_deleted(head: keldra_api::v1::ObjectHead) -> u64 {
    let Some(HeadState::Deleted(deleted)) = head.state else {
        panic!("object head was not deleted");
    };
    deleted.version
}

fn bulk_put(address: ObjectAddress, bytes: &[u8], command_id: &str) -> BulkPutRequest {
    BulkPutRequest {
        address: Some(address),
        bytes: bytes.to_vec(),
        content_type: "application/octet-stream".into(),
        command_id: command_id.into(),
        durability: Durability::Local as i32,
    }
}

async fn list_version_ids(
    client: &mut ObjectServiceClient<Channel>,
    address: &ObjectAddress,
    access_token: &str,
) -> Vec<u64> {
    let mut stream = client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                address: Some(address.clone()),
            },
            access_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut versions = Vec::new();
    while let Some(version) = stream.message().await.unwrap() {
        let id = match version.state {
            Some(keldra_api::v1::object_version::State::Present(present)) => present.version,
            Some(keldra_api::v1::object_version::State::Deleted(deleted)) => deleted.version,
            None => panic!("version stream returned an empty state"),
        };
        versions.push(id);
    }
    versions
}

async fn next_watch(
    stream: &mut tonic::Streaming<keldra_api::v1::WatchMessage>,
) -> keldra_api::v1::WatchMessage {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("watch message timed out")
        .expect("watch stream failed")
        .expect("watch stream ended")
}

fn checkpoint(message: keldra_api::v1::WatchMessage) -> Vec<u8> {
    let Some(WatchMessageValue::Checkpoint(checkpoint)) = message.message else {
        panic!("watch message was not a checkpoint");
    };
    checkpoint.resume_token
}

async fn next_invalidation(
    stream: &mut tonic::Streaming<keldra_api::v1::WatchMessage>,
) -> keldra_api::v1::WatchInvalidation {
    loop {
        match next_watch(stream).await.message {
            Some(WatchMessageValue::Invalidation(invalidation)) => return invalidation,
            Some(WatchMessageValue::Checkpoint(_)) => {}
            None => panic!("watch message was empty"),
        }
    }
}
