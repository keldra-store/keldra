use super::*;
use anvil_api::v1::{
    PutIfAbsentOperation, PutIfVersionOperation, PutImmutableOperation, PutOperation,
};
use anvil_store::{Head, ObjectPathSnapshot};
use tokio_stream::StreamExt;

fn address(path: &str) -> Option<ObjectAddress> {
    Some(ObjectAddress {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        path: path.into(),
    })
}

#[test]
fn never_existed_and_deleted_remain_distinct() {
    assert!(matches!(
        never_existed().state,
        Some(ObjectState::NeverExisted(_))
    ));
    let deleted = Version {
        id: VersionId(9),
        blob: None,
        content_type: None,
        deleted: true,
        committed_at_unix_millis: 0,
    };
    assert!(matches!(
        api_head(&deleted).unwrap().state,
        Some(ObjectState::Deleted(DeletedObject { version: 9 }))
    ));
}

#[test]
fn list_objects_contract_defaults_and_bounds_its_stateless_page() {
    let defaulted = list_objects_query(ListObjectsRequest {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        prefix: "reports/".into(),
        start_after: None,
        limit: 0,
    })
    .unwrap();
    assert_eq!(defaulted.limit, DEFAULT_LIST_OBJECTS_LIMIT);
    assert_eq!(defaulted.prefix, "reports/");

    let bounded = list_objects_query(ListObjectsRequest {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        prefix: String::new(),
        start_after: Some("reports/last.json".into()),
        limit: MAX_LIST_OBJECTS as u32,
    })
    .unwrap();
    assert_eq!(bounded.start_after.as_deref(), Some("reports/last.json"));
    assert_eq!(bounded.limit, MAX_LIST_OBJECTS);

    let too_many = list_objects_query(ListObjectsRequest {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        limit: MAX_LIST_OBJECTS as u32 + 1,
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(too_many.code(), tonic::Code::InvalidArgument);
    let empty_cursor = list_objects_query(ListObjectsRequest {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        start_after: Some(String::new()),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(empty_cursor.code(), tonic::Code::InvalidArgument);
}

#[test]
fn anonymous_object_identity_is_not_accepted_by_mutation_helpers() {
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert(crate::authentication::AnonymousObjectRequest);

    assert_eq!(
        authenticated_caller(&request).unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
}

#[test]
fn present_head_contains_public_hash_and_length_without_a_blob_reference() {
    let present = Version {
        id: VersionId(4),
        blob: Some(BlobRef {
            hash: *blake3::hash(b"payload").as_bytes(),
            length: 7,
        }),
        content_type: Some("application/octet-stream".into()),
        deleted: false,
        committed_at_unix_millis: 0,
    };
    let Some(ObjectState::Present(head)) = api_head(&present).unwrap().state else {
        panic!("present version must produce a present head");
    };
    assert_eq!(head.version, 4);
    assert_eq!(
        head.content_hash.as_slice(),
        blake3::hash(b"payload").as_bytes()
    );
    assert_eq!(head.content_length, 7);
}

#[test]
fn retained_version_metadata_and_delete_outcomes_preserve_public_semantics() {
    let present = Version {
        id: VersionId(4),
        blob: Some(BlobRef {
            hash: *blake3::hash(b"payload").as_bytes(),
            length: 7,
        }),
        content_type: Some("application/octet-stream".into()),
        deleted: false,
        committed_at_unix_millis: 0,
    };
    assert!(matches!(
        api_object_version(&present).unwrap().state,
        Some(ObjectVersionState::Present(PresentObject {
            version: 4,
            ..
        }))
    ));

    assert_eq!(
        api_delete_version_outcome(DeleteRetainedVersionOutcome::NotFound),
        DeleteVersionResponse {
            deleted: false,
            replacement_tombstone_version: None,
        }
    );
    assert_eq!(
        api_delete_version_outcome(DeleteRetainedVersionOutcome::DeletedNonCurrent),
        DeleteVersionResponse {
            deleted: true,
            replacement_tombstone_version: None,
        }
    );
    assert_eq!(
        api_delete_version_outcome(DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone {
            version: VersionId(12),
        }),
        DeleteVersionResponse {
            deleted: true,
            replacement_tombstone_version: Some(12),
        }
    );
}

#[test]
fn typed_put_operations_map_one_to_one_to_store_modes() {
    let cases = [
        (ApiPutOperation::Put(PutOperation {}), PutMode::Put),
        (
            ApiPutOperation::PutIfAbsent(PutIfAbsentOperation {}),
            PutMode::PutIfAbsent,
        ),
        (
            ApiPutOperation::PutIfVersion(PutIfVersionOperation {
                expected_version: 17,
            }),
            PutMode::PutIfVersion(VersionId(17)),
        ),
        (
            ApiPutOperation::PutImmutable(PutImmutableOperation {}),
            PutMode::PutImmutable,
        ),
    ];
    for (operation, expected) in cases {
        let metadata = put_metadata(PutHeader {
            address: address("object"),
            command_id: "command".into(),
            operation: Some(operation),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(metadata.mode, expected);
        assert_eq!(metadata.durability, StoreDurability::Local);
    }
}

#[test]
fn put_requires_an_explicit_operation_and_command_id() {
    let missing_operation = put_metadata(PutHeader {
        address: address("object"),
        command_id: "command".into(),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(missing_operation.code(), tonic::Code::InvalidArgument);

    let missing_command = put_metadata(PutHeader {
        address: address("object"),
        operation: Some(ApiPutOperation::Put(PutOperation {})),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(missing_command.code(), tonic::Code::InvalidArgument);
    assert!(required_command_id("a".repeat(256)).is_ok());
    assert!(required_command_id("a".repeat(257)).is_err());
    assert!(required_command_id("nul\0command".into()).is_err());
}

#[test]
fn content_type_is_bounded_by_utf8_bytes_before_a_put_token_can_be_issued() {
    let exactly_512_bytes = "é".repeat(256);
    let too_large = format!("{exactly_512_bytes}a");
    assert_eq!(exactly_512_bytes.len(), MAX_CONTENT_TYPE_BYTES);
    assert_eq!(too_large.len(), MAX_CONTENT_TYPE_BYTES + 1);

    let accepted = put_metadata(PutHeader {
        address: address("object"),
        content_type: exactly_512_bytes.clone(),
        command_id: "command".into(),
        operation: Some(ApiPutOperation::Put(PutOperation {})),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        accepted.content_type.as_deref(),
        Some(exactly_512_bytes.as_str())
    );

    let rejected = put_metadata(PutHeader {
        address: address("object"),
        content_type: too_large.clone(),
        command_id: "command".into(),
        operation: Some(ApiPutOperation::Put(PutOperation {})),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);

    let bulk_rejected = batch_operation(
        BulkOperation {
            operation: Some(anvil_api::v1::bulk_operation::Operation::PutIfVersion(
                BulkPutIfVersionRequest {
                    address: address("object"),
                    content_type: too_large,
                    command_id: "command".into(),
                    expected_version: 1,
                    ..Default::default()
                },
            )),
        },
        u64::MAX,
    )
    .unwrap_err();
    assert_eq!(bulk_rejected.code(), tonic::Code::InvalidArgument);
}

#[test]
fn durability_is_a_closed_typed_choice() {
    assert_eq!(durability(0).unwrap(), StoreDurability::Local);
    assert_eq!(
        durability(ApiDurability::Replicated as i32).unwrap(),
        StoreDurability::Replicated
    );
    assert_eq!(
        durability(99).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        token_durability(StoreDurability::Replicated).unwrap(),
        TokenDurability::Replicated
    );

    let metadata = put_metadata(PutHeader {
        address: address("object"),
        command_id: "command".into(),
        durability: ApiDurability::Replicated as i32,
        operation: Some(ApiPutOperation::Put(PutOperation {})),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(metadata.durability, StoreDurability::Replicated);
}

#[test]
fn atomic_program_timeout_uses_the_shorter_client_or_server_budget() {
    let server_maximum = Duration::from_secs(30);
    let mut metadata = MetadataMap::new();
    assert_eq!(
        effective_atomic_program_timeout(&metadata, server_maximum),
        server_maximum
    );

    metadata.insert("grpc-timeout", "250m".parse().unwrap());
    assert_eq!(
        effective_atomic_program_timeout(&metadata, server_maximum),
        Duration::from_millis(250)
    );

    metadata.insert("grpc-timeout", "2M".parse().unwrap());
    assert_eq!(
        effective_atomic_program_timeout(&metadata, server_maximum),
        server_maximum
    );

    metadata.insert("grpc-timeout", "invalid".parse().unwrap());
    assert_eq!(
        effective_atomic_program_timeout(&metadata, server_maximum),
        server_maximum
    );
}

#[tokio::test]
async fn atomic_program_timeout_returns_grpc_deadline_exceeded() {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(1);
    let error = run_atomic_program_until(deadline, async {
        std::future::pending::<Result<(), Status>>().await
    })
    .await
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);

    let expected = Status::failed_precondition("program rejected");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let error = run_atomic_program_until(deadline, async { Err::<(), _>(expected) })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "program rejected");
}

#[test]
fn all_six_bulk_operations_are_explicit_and_zero_byte_puts_are_valid() {
    let put = || BulkPutRequest {
        address: address("object"),
        command_id: "command".into(),
        ..Default::default()
    };
    let operations = [
        anvil_api::v1::bulk_operation::Operation::Put(put()),
        anvil_api::v1::bulk_operation::Operation::PutIfAbsent(put()),
        anvil_api::v1::bulk_operation::Operation::PutIfVersion(BulkPutIfVersionRequest {
            address: address("object"),
            command_id: "command".into(),
            expected_version: 7,
            ..Default::default()
        }),
        anvil_api::v1::bulk_operation::Operation::PutImmutable(put()),
        anvil_api::v1::bulk_operation::Operation::Delete(ApiDeleteRequest {
            address: address("object"),
            command_id: "command".into(),
            ..Default::default()
        }),
        anvil_api::v1::bulk_operation::Operation::DeleteIfVersion(DeleteIfVersionRequest {
            address: address("object"),
            command_id: "command".into(),
            expected_version: 9,
            ..Default::default()
        }),
    ];
    let converted = operations
        .into_iter()
        .map(|operation| {
            batch_operation(
                BulkOperation {
                    operation: Some(operation),
                },
                0,
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(matches!(converted[0], BatchOperation::Put(ref r) if r.mode == PutMode::Put));
    assert!(matches!(converted[1], BatchOperation::Put(ref r) if r.mode == PutMode::PutIfAbsent));
    assert!(
        matches!(converted[2], BatchOperation::Put(ref r) if r.mode == PutMode::PutIfVersion(VersionId(7)))
    );
    assert!(matches!(converted[3], BatchOperation::Put(ref r) if r.mode == PutMode::PutImmutable));
    assert!(
        matches!(converted[4], BatchOperation::Delete(ref r) if r.precondition == Precondition::Any)
    );
    assert!(
        matches!(converted[5], BatchOperation::Delete(ref r) if r.precondition == Precondition::Version(VersionId(9)))
    );
}

#[test]
fn bulk_limit_accounts_for_every_encoded_operation_byte_without_cloning_payloads() {
    let operations = vec![
        BulkOperation {
            operation: Some(anvil_api::v1::bulk_operation::Operation::Put(
                BulkPutRequest {
                    address: address("one"),
                    bytes: vec![1, 2, 3],
                    content_type: "application/json".into(),
                    command_id: "put-one".into(),
                    durability: ApiDurability::Local as i32,
                },
            )),
        },
        BulkOperation {
            operation: Some(anvil_api::v1::bulk_operation::Operation::Delete(
                ApiDeleteRequest {
                    address: address("two"),
                    command_id: "delete-two".into(),
                    durability: ApiDurability::Local as i32,
                },
            )),
        },
    ];
    let expected = prost::Message::encoded_len(&BulkWriteRequest {
        operations: operations.clone(),
    });
    let accounted = bulk_encoded_len(&operations).unwrap();
    assert_eq!(accounted, expected);
    assert!(
        accounted > 3,
        "metadata and protobuf framing must be counted"
    );
    assert!(enforce_bulk_encoded_limit(MAX_BULK_BYTES).is_ok());
    assert_eq!(
        enforce_bulk_encoded_limit(MAX_BULK_BYTES + 1)
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
}

#[test]
fn bulk_metrics_partition_success_failure_and_replay_outcomes() {
    let response = Ok(Response::new(BulkWriteResponse {
        outcomes: vec![
            BulkOutcome {
                index: 0,
                outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Receipt(
                    ApiMutationReceipt::default(),
                )),
            },
            BulkOutcome {
                index: 1,
                outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Receipt(
                    ApiMutationReceipt {
                        replayed: true,
                        ..Default::default()
                    },
                )),
            },
            BulkOutcome {
                index: 2,
                outcome: Some(anvil_api::v1::bulk_outcome::Outcome::Failure(
                    MutationFailure::default(),
                )),
            },
        ],
    }));
    assert_eq!(
        bulk_metric_counts(3, &response),
        BulkMetricCounts {
            successful: 1,
            failed: 1,
            replayed: 1,
        }
    );

    let failed_request = Err(Status::unavailable("store unavailable"));
    assert_eq!(
        bulk_metric_counts(3, &failed_request),
        BulkMetricCounts {
            successful: 0,
            failed: 3,
            replayed: 0,
        }
    );
}

#[test]
fn watch_lag_is_an_observation_against_the_settled_journal_cut() {
    let status = WatchJournalStatus {
        source_id: anvil_store::SourceId {
            node_id: 1,
            source_epoch: [0; 32],
        },
        tail: 19,
        settled_through: 17,
        retention_floor: 4,
        retained_entries: 15,
        retained_bytes: 900,
    };
    assert_eq!(watch_consumer_lag(&status, 7), Some(10));
    assert_eq!(watch_consumer_lag(&status, 20), None);
}

#[test]
fn oversized_bulk_items_fail_and_replicated_durability_is_preserved() {
    let operation = BulkOperation {
        operation: Some(anvil_api::v1::bulk_operation::Operation::Put(
            BulkPutRequest {
                address: address("object"),
                bytes: vec![0; 2],
                command_id: "command".into(),
                ..Default::default()
            },
        )),
    };
    let failure = api_request_failure(batch_operation(operation, 1).unwrap_err());
    assert_eq!(failure.code, MutationFailureCode::ResourceLimit as i32);

    let operation = BulkOperation {
        operation: Some(anvil_api::v1::bulk_operation::Operation::Delete(
            ApiDeleteRequest {
                address: address("delete"),
                command_id: "delete-command".into(),
                durability: ApiDurability::Replicated as i32,
            },
        )),
    };
    let BatchOperation::Delete(request) = batch_operation(operation, u64::MAX).unwrap() else {
        panic!("expected a delete operation")
    };
    assert_eq!(request.durability, StoreDurability::Replicated);
}

#[test]
fn put_token_helper_rejects_missing_values() {
    assert_eq!(
        required_put_token(None).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        required_put_token(Some(PutToken::default()))
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn canonical_token_header_preserves_caller_selected_operation() {
    let header = CanonicalPutHeader {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        path: "object".into(),
        content_type: Some("application/json".into()),
        command_id: "command".into(),
        durability: TokenDurability::Local,
        operation: TokenPutOperation::PutIfVersion {
            expected_version: 31,
        },
    };
    let metadata = header.to_metadata().unwrap();
    assert_eq!(metadata.mode, PutMode::PutIfVersion(VersionId(31)));
    assert_eq!(metadata.content_type.as_deref(), Some("application/json"));
}

#[test]
fn upload_and_ready_tokens_have_disjoint_strict_phases() {
    let header = CanonicalPutHeader {
        tenant: "tenant".into(),
        bucket: "bucket".into(),
        path: "object".into(),
        content_type: None,
        command_id: "command".into(),
        durability: TokenDurability::Local,
        operation: TokenPutOperation::Put,
    };
    let upload = CanonicalPutCapability {
        format_version: PUT_TOKEN_FORMAT_VERSION,
        phase: PutTokenPhase::Upload(UploadCapability {
            header: header.clone(),
        }),
    };
    let ready = CanonicalPutCapability {
        format_version: PUT_TOKEN_FORMAT_VERSION,
        phase: PutTokenPhase::Ready(ReadyCapability {
            header,
            blob_hash: [9; 32],
            blob_length: 42,
            upload_source_node_id: 7,
        }),
    };
    let upload: CanonicalPutCapability =
        serde_json::from_slice(&serde_json::to_vec(&upload).unwrap()).unwrap();
    let ready: CanonicalPutCapability =
        serde_json::from_slice(&serde_json::to_vec(&ready).unwrap()).unwrap();
    assert!(matches!(upload.phase, PutTokenPhase::Upload(_)));
    assert!(matches!(ready.phase, PutTokenPhase::Ready(_)));
    assert!(require_upload_phase(upload.clone()).is_ok());
    assert_eq!(
        require_ready_phase(upload).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert!(require_ready_phase(ready.clone()).is_ok());
    assert_eq!(
        require_upload_phase(ready).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert!(
        serde_json::from_slice::<CanonicalPutCapability>(
            br#"{"format_version":1,"phase":{"unknown":{}}}"#
        )
        .is_err()
    );
}

#[test]
fn atomic_program_hash_is_exact_and_nonzero() {
    assert_eq!(required_hash(&[7; 32], "program_hash").unwrap(), [7; 32]);
    for invalid in [Vec::new(), vec![7; 31], vec![7; 33], vec![0; 32]] {
        assert_eq!(
            required_hash(&invalid, "program_hash").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn batch_get_payload_limit_accepts_the_boundary_and_rejects_larger_totals() {
    assert!(enforce_batch_get_payload_limit(MAX_BATCH_GET_BYTES as u64).is_ok());
    let error = enforce_batch_get_payload_limit(MAX_BATCH_GET_BYTES as u64 + 1).unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
}

#[test]
fn distributed_batch_preflight_selects_current_and_exact_descriptors() {
    let key = ObjectKey::new("tenant", "bucket", "object").unwrap();
    let live = Version {
        id: VersionId(4),
        blob: Some(BlobRef {
            hash: *blake3::hash(b"payload").as_bytes(),
            length: 7,
        }),
        content_type: None,
        deleted: false,
        committed_at_unix_millis: 4,
    };
    let deleted = Version {
        id: VersionId(5),
        blob: None,
        content_type: None,
        deleted: true,
        committed_at_unix_millis: 5,
    };
    let snapshot = ObjectPathSnapshot {
        tenant_id: 11,
        bucket_id: 12,
        exact_path: key.path().into(),
        head: Head {
            version: deleted.id,
            deleted: true,
            mutation_stamp: None,
        },
        versions: vec![live, deleted],
        definition_locator: None,
    };

    assert_eq!(
        distributed_reads::declared_payload_length(Some(&snapshot), &key, None).unwrap(),
        0
    );
    assert_eq!(
        distributed_reads::declared_payload_length(Some(&snapshot), &key, Some(VersionId(4)))
            .unwrap(),
        7
    );
    assert_eq!(
        distributed_reads::declared_payload_length(Some(&snapshot), &key, Some(VersionId(99)))
            .unwrap(),
        0
    );
}

#[test]
fn distributed_read_preflight_rejects_another_exact_path() {
    let key = ObjectKey::new("tenant", "bucket", "object").unwrap();
    let version = Version {
        id: VersionId(4),
        blob: None,
        content_type: None,
        deleted: true,
        committed_at_unix_millis: 4,
    };
    let snapshot = ObjectPathSnapshot {
        tenant_id: 11,
        bucket_id: 12,
        exact_path: "another".into(),
        head: Head {
            version: version.id,
            deleted: true,
            mutation_stamp: None,
        },
        versions: vec![version],
        definition_locator: None,
    };

    let error =
        distributed_reads::declared_payload_length(Some(&snapshot), &key, None).unwrap_err();
    assert_eq!(error.code(), tonic::Code::DataLoss);
    assert_eq!(
        distributed_reads::status_failure(error).code,
        ReadFailureCode::DataLoss as i32
    );
}

#[test]
fn exact_get_missing_version_fails_before_opening_a_stream() {
    let error = match distributed_reads::get_object_response(None, true) {
        Err(error) => error,
        Ok(_) => panic!("an exact missing version must fail before streaming"),
    };
    assert_eq!(error.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn current_get_of_a_never_created_path_streams_only_the_head() {
    let mut stream = distributed_reads::get_object_response(None, false)
        .unwrap()
        .into_inner();
    let first = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        first.value,
        Some(ObjectChunkValue::Head(ObjectHead {
            state: Some(ObjectState::NeverExisted(_))
        }))
    ));
    assert!(stream.next().await.is_none());
}

#[test]
fn source_journal_capacity_is_a_resource_limit() {
    let error = status(MutationError::SourceJournalCapacity);
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        api_failure(MutationError::SourceJournalCapacity).code,
        MutationFailureCode::ResourceLimit as i32
    );
}

#[test]
fn watch_errors_expose_a_stable_resume_expired_outcome() {
    let expired = watch_status(WatchError::ResumeExpired);
    assert_eq!(expired.code(), tonic::Code::FailedPrecondition);
    assert_eq!(expired.message(), "RESUME_EXPIRED");
    assert_eq!(
        watch_status(WatchError::InvalidResumeToken).code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn direct_program_only_writes_expose_the_stable_concurrency_outcome() {
    let error = status(MutationError::ProgramConcurrencyViolation);
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .starts_with("PROGRAM_CONCURRENCY_VIOLATION:")
    );
    assert_eq!(
        api_failure(MutationError::ImmutablePolicyRequired).code,
        MutationFailureCode::ImmutablePolicyRequired as i32
    );
}

#[test]
fn current_tombstone_delete_exposes_the_stable_public_failure_name() {
    let error = status(MutationError::CurrentTombstoneCannotBeDeleted);
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .starts_with("CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED:")
    );
}
