use super::*;

#[tokio::test]
async fn persistence_global_journal_writes_use_current_fence_tokens() {
    Box::pin(async {
        let temp = tempdir().unwrap();
        let persistence = Persistence::new(&test_config(temp.path())).unwrap();

        persistence.create_region("local").await.unwrap();
        bind_persistence_test_authz_schema(&persistence, 1).await;
        let bucket = persistence
            .create_bucket(1, "bucket-a", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                1,
                bucket.id,
                "objects/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        persistence
            .soft_delete_object(bucket.id, &object.key)
            .await
            .unwrap();
        let upload = persistence
            .create_multipart_upload(1, bucket.id, "objects/large.bin")
            .await
            .unwrap()
            .upload;
        persistence
            .upsert_multipart_part(upload.id, 1, payload_ref("part-hash", 12), 12, "part-etag")
            .await
            .unwrap();
        persistence
            .complete_multipart_upload(upload.id)
            .await
            .unwrap();
        let stream = persistence
            .create_append_stream(1, bucket.id, &bucket.name, "stream-a")
            .await
            .unwrap()
            .stream;
        persistence
            .append_stream_record(
                1,
                bucket.id,
                &stream,
                payload_ref("payload-hash", 13),
                13,
                None,
                None,
                "tenant/1/principal/test",
            )
            .await
            .unwrap();
        persistence
            .seal_append_stream(1, bucket.id, &stream, "segment-hash")
            .await
            .unwrap();
        persistence
            .compare_and_swap_manifest(
                1,
                bucket.id,
                &bucket.name,
                "manifest.json",
                0,
                json!({"version": 1}),
                "manifest-hash",
            )
            .await
            .unwrap();
        let mutation = IndexDefinitionMutation::Create {
            name: "body".to_string(),
            kind: "full_text".to_string(),
            selector: json!({"prefix": "objects/"}),
            extractor: json!({"field": "body"}),
            authorization_mode: "inherit".to_string(),
            build_policy: json!({"mode": "sync"}),
        };
        let IndexDefinitionMutationOutcome::Published { index, .. } = persistence
            .apply_index_definition_mutation(&bucket, &mutation, None, None)
            .await
            .unwrap()
        else {
            panic!("index definition create should publish");
        };
        persistence
            .create_index_diagnostic(
                1,
                bucket.id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &object.key,
                Some(object.version_id),
                "warning",
                "test-warning",
                "diagnostic",
                json!({"source": "test"}),
            )
            .await
            .unwrap();
        persistence
            .write_authz_tuple(
                1,
                "object",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
                "add",
                "test",
                "test grant",
            )
            .await
            .unwrap();
        persistence
            .enqueue_task(
                crate::tasks::TaskType::DeleteBucket,
                json!({"bucket_id": 7}),
                1,
            )
            .await
            .unwrap();
        persistence
            .create_model_artifact("artifact-a", 1, "models/a", &model_manifest())
            .await
            .unwrap();
        persistence
            .hf_create_key(1, "primary", b"secret", Some("note"))
            .await
            .unwrap();

        let control_fences =
            crate::control_journal::read_control_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(control_fences.iter().all(|fence| *fence > 0));
        let task_fences =
            crate::task_journal::read_task_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(task_fences.iter().all(|fence| *fence > 0));
        let model_fences =
            crate::model_journal::read_model_frame_fences_for_test(&persistence.storage)
                .await
                .unwrap();
        assert!(model_fences.iter().all(|fence| *fence > 0));
        let hf_fences = crate::hf_journal::read_hf_frame_fences_for_test(&persistence.storage)
            .await
            .expect("hf metadata journal fences");
        assert!(hf_fences.iter().all(|fence| *fence > 0));
        let (tenant_bucket_fences, global_bucket_fences) =
            crate::bucket_journal::read_bucket_frame_fences_for_test(&persistence.storage, 1)
                .await
                .unwrap();
        assert!(tenant_bucket_fences.iter().all(|fence| *fence > 0));
        assert!(global_bucket_fences.iter().all(|fence| *fence > 0));
        let object_fences = crate::metadata_journal::read_object_metadata_record_fences_for_test(
            &persistence.storage,
            &bucket,
        )
        .await
        .expect("object metadata journal fences");
        assert!(object_fences.iter().all(|fence| *fence > 0));
        let multipart_fences = crate::multipart_journal::read_multipart_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .expect("multipart journal fences");
        assert!(multipart_fences.iter().all(|fence| *fence > 0));
        let append_fences = crate::append_journal::read_append_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(append_fences.iter().all(|fence| *fence > 0));
        let manifest_fences = crate::manifest_journal::read_manifest_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(manifest_fences.iter().all(|fence| *fence > 0));
        let index_fences = crate::index_journal::read_index_frame_fences_for_test(
            &persistence.storage,
            1,
            bucket.id,
        )
        .await
        .unwrap();
        assert!(index_fences.iter().all(|fence| *fence > 0));
        let diagnostic_fences =
            crate::index_diagnostic_journal::read_index_diagnostic_frame_fences_for_test(
                &persistence.storage,
                1,
                bucket.id,
            )
            .await
            .unwrap();
        assert!(diagnostic_fences.iter().all(|fence| *fence > 0));
        let authz_fences =
            crate::authz_journal::read_authz_frame_fences_for_test(&persistence.storage, 1)
                .await
                .expect("authz tuple journal fences");
        assert!(authz_fences.iter().all(|fence| *fence > 0));
    })
    .await
}

#[tokio::test]
async fn force_expired_partition_is_recovered_under_a_higher_fence() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let partition_family = "object_metadata";
    let partition_id = hex::encode([42; 32]);
    let manifest_hash = hex::encode([7; 32]);
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap();

    let recovering = crate::partition_fence::acquire_partition_recovery(
        &persistence.storage,
        crate::partition_fence::PartitionRecoveryAcquire {
            partition_family: partition_family.to_string(),
            partition_id: partition_id.clone(),
            owner_node_id: "unreachable-node".to_string(),
            recovered_through_sequence: 17,
            recovered_manifest_hash: manifest_hash.clone(),
            now_nanos,
        },
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    let previous = crate::partition_fence::publish_partition_ready(
        &persistence.storage,
        partition_family,
        &partition_id,
        "unreachable-node",
        recovering.fence_token,
        17,
        &manifest_hash,
        now_nanos.saturating_add(1),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();
    crate::partition_fence::force_expire_partition_owner_for_node(
        &persistence.storage,
        partition_family,
        &partition_id,
        "unreachable-node",
        now_nanos.saturating_add(2),
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap();

    let permit = persistence
        .global_write_permit(partition_family, partition_id.clone())
        .await
        .unwrap();
    let current = crate::partition_fence::read_partition_owner(
        &persistence.storage,
        partition_family,
        &partition_id,
        &persistence.partition_owner_signing_key,
    )
    .await
    .unwrap()
    .expect("replacement partition owner");

    assert_eq!(permit.owner_node_id, persistence.owner_node_id);
    assert_eq!(current.owner_node_id, persistence.owner_node_id);
    assert_eq!(
        current.status,
        crate::partition_fence::PartitionOwnerStatus::Ready
    );
    assert_eq!(current.recovered_through_sequence, 17);
    assert_eq!(current.recovered_manifest_hash, manifest_hash);
    assert!(current.fence_token > previous.fence_token);
    assert_eq!(permit.fence_token, current.fence_token);
}
