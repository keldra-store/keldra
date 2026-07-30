use tempfile::tempdir;

use super::*;
use crate::mvcc_transaction::{HierarchicalRangeStampScheme, TransactionBundleBuilder};

fn key(table_id: u16, application_key: &[u8]) -> LogicalKey {
    LogicalKey {
        table_id,
        application_key: application_key.to_vec(),
    }
}

fn bundle(
    transaction_id: &str,
    writes: impl FnOnce(&mut TransactionBundleBuilder),
) -> TransactionBundle {
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        transaction_id,
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    writes(&mut builder);
    builder.build().unwrap()
}

fn git_source_job(transaction_id: &str, generation: u64) -> GitSourcePostCommitJob {
    let source_hash = hex::encode([generation as u8; 32]);
    GitSourcePostCommitJob {
        schema: GitSourcePostCommitJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: transaction_id.into(),
        tenant_id: 1,
        repository_id: "repository-a".into(),
        bucket_name: "git-packs".into(),
        object_key: format!("repository-a/{generation}.pack"),
        pack_object_version_id: uuid::Uuid::from_u128(100 + generation as u128).to_string(),
        pack_mutation_id: uuid::Uuid::from_u128(200 + generation as u128).to_string(),
        source_hash: source_hash.clone(),
        generation,
        record_count: generation,
        index_path: crate::git_source_index::git_source_index_ref_name(
            1,
            "repository-a",
            generation,
            &source_hash,
        )
        .unwrap(),
        authz_revision: 7,
        emitted_at: "2026-07-27T12:00:00Z".into(),
    }
}

fn hf_ingestion_job(transaction_id: &str, ingestion_id: i64) -> HfIngestionPostCommitJob {
    HfIngestionPostCommitJob {
        schema: HfIngestionPostCommitJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: transaction_id.into(),
        ingestion_id,
        tenant_id: 1,
        priority: 100,
    }
}

fn object_link_job(transaction_id: &str, generation: u64) -> ObjectLinkFinalizationJob {
    ObjectLinkFinalizationJob {
        schema: ObjectLinkFinalizationJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: transaction_id.into(),
        tenant_id: 1,
        bucket_id: 2,
        bucket_name: "bucket".into(),
        link_key: "links/latest".into(),
        generation,
        operation: crate::object_link_finalization_job::ObjectLinkFinalizationOperation::Put,
        target_key: Some(format!("objects/{generation}")),
        target_version_id: Some(uuid::Uuid::from_u128(100 + generation as u128).to_string()),
        mutation_id: uuid::Uuid::from_u128(200 + generation as u128).to_string(),
        consequences: crate::object_link_finalization_job::ObjectLinkFinalizationConsequences {
            maintain_indexes: true,
            compact_metadata: true,
        },
    }
}

fn bucket_locator_job(
    transaction_id: &str,
    operation_sequence: u64,
    operation: crate::bucket_locator_finalization_job::BucketLocatorFinalizationOperation,
) -> BucketLocatorFinalizationJob {
    use chrono::TimeZone;

    BucketLocatorFinalizationJob {
        schema: BucketLocatorFinalizationJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: transaction_id.into(),
        operation_sequence,
        operation,
        frozen_bucket: crate::persistence::Bucket {
            id: 17,
            tenant_id: 1,
            name: "bucket-a".into(),
            region: "region-a".into(),
            created_at: chrono::Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            is_public_read: false,
        },
    }
}

#[test]
fn snapshot_reads_select_the_newest_visible_immutable_version() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(7, b"account");
    store
        .apply_certified_bundle(
            2,
            &bundle("v2", |b| {
                b.put(row.clone(), b"old".to_vec());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            5,
            &bundle("v5", |b| {
                b.put(row.clone(), b"new".to_vec());
            }),
        )
        .unwrap();

    assert_eq!(store.read_at(&row, 2).unwrap().unwrap().value, b"old");
    assert_eq!(store.read_at(&row, 4).unwrap().unwrap().value, b"old");
    assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"new");
}

#[test]
fn replacement_checkpoint_retry_can_advance_but_never_roll_back() {
    let source_directory = tempdir().unwrap();
    let source = MvccStore::open(source_directory.path()).unwrap();
    let row = key(7, b"checkpoint-retry");
    source
        .apply_certified_bundle_and_advance(
            1,
            &bundle("checkpoint-v1", |builder| {
                builder.put(row.clone(), b"one".to_vec());
            }),
            1,
        )
        .unwrap();
    let first = source.export_checkpoint().unwrap();

    let target_directory = tempdir().unwrap();
    let target = MvccStore::open(target_directory.path()).unwrap();
    assert_eq!(
        target.install_checkpoint(&first).unwrap(),
        MvccCheckpointInstallOutcome::Installed
    );

    source
        .apply_certified_bundle_and_advance(
            2,
            &bundle("checkpoint-v2", |builder| {
                builder.put(row.clone(), b"two".to_vec());
            }),
            2,
        )
        .unwrap();
    let second = source.export_checkpoint().unwrap();
    assert_eq!(
        target.install_checkpoint(&second).unwrap(),
        MvccCheckpointInstallOutcome::Installed
    );
    assert_eq!(
        target.read_latest(&row).unwrap().unwrap().value,
        b"two",
        "a retry resumes from the newer donor checkpoint"
    );
    assert!(
        target.install_checkpoint(&first).is_err(),
        "a stale retry cannot roll back an installed checkpoint"
    );
    assert_eq!(
        target.install_checkpoint(&second).unwrap(),
        MvccCheckpointInstallOutcome::Replayed
    );
}

#[test]
fn checkpoint_after_gc_restores_current_rows_and_accepts_ordered_deltas() {
    let source_directory = tempdir().unwrap();
    let source = MvccStore::open(source_directory.path()).unwrap();
    let row = key(8, b"checkpoint-after-gc");
    source
        .apply_certified_bundle_and_advance(
            1,
            &bundle("checkpoint-old", |builder| {
                builder.put(row.clone(), b"old".to_vec());
            }),
            1,
        )
        .unwrap();
    source
        .apply_certified_bundle_and_advance(
            2,
            &bundle("checkpoint-current", |builder| {
                builder.put(row.clone(), b"current".to_vec());
            }),
            2,
        )
        .unwrap();
    source.garbage_collect(2).unwrap();
    let bytes = source.export_checkpoint_bytes().unwrap();

    let target_directory = tempdir().unwrap();
    let target = MvccStore::open(target_directory.path()).unwrap();
    assert_eq!(
        target.install_checkpoint_bytes(&bytes).unwrap(),
        MvccCheckpointInstallOutcome::Installed
    );
    assert_eq!(target.decision_watermark().unwrap(), 2);
    assert_eq!(target.applied_version().unwrap(), 2);
    assert_eq!(target.gc_watermark().unwrap(), 2);
    assert_eq!(target.read_latest(&row).unwrap().unwrap().value, b"current");

    target
        .apply_certified_bundle_and_advance(
            3,
            &bundle("checkpoint-delta", |builder| {
                builder.put(row.clone(), b"delta".to_vec());
            }),
            3,
        )
        .unwrap();
    assert_eq!(target.read_latest(&row).unwrap().unwrap().value, b"delta");
}

#[test]
fn checkpoint_decode_and_install_reject_corruption_and_foreign_clusters() {
    let source_directory = tempdir().unwrap();
    let source = MvccStore::open(source_directory.path()).unwrap();
    source.advance_decision_watermark(1).unwrap();
    let mut bytes = source.export_checkpoint_bytes().unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;

    let target_directory = tempdir().unwrap();
    let target = MvccStore::open(target_directory.path()).unwrap();
    assert!(target.install_checkpoint_bytes(&bytes).is_err());
    assert_eq!(target.decision_watermark().unwrap(), 0);

    let mut foreign = source.export_checkpoint().unwrap();
    foreign.cluster_id = "another-cluster".into();
    assert!(target.install_checkpoint(&foreign).is_err());
    assert_eq!(target.decision_watermark().unwrap(), 0);
}

#[test]
fn checkpoint_encoding_is_canonical_and_bounded_by_the_input() {
    let directory = tempdir().unwrap();
    let store = MvccStore::open(directory.path()).unwrap();
    store.advance_decision_watermark(1).unwrap();
    let checkpoint = store.export_checkpoint().unwrap();
    let encoded = checkpoint.encode().unwrap();

    assert_eq!(MvccCheckpoint::decode(&encoded).unwrap(), checkpoint);
    for prefix_length in 0..encoded.len() {
        assert!(
            MvccCheckpoint::decode(&encoded[..prefix_length]).is_err(),
            "truncated checkpoint prefix {prefix_length} unexpectedly decoded"
        );
    }

    let mut with_trailing_byte = encoded;
    with_trailing_byte.push(0);
    assert!(MvccCheckpoint::decode(&with_trailing_byte).is_err());

    let hostile_count_bytes = u64::MAX.to_be_bytes();
    let mut hostile_count = MvccCheckpointDecoder::new(&hostile_count_bytes);
    assert!(
        hostile_count
            .collection_len("hostile collection", 1)
            .is_err()
    );
}

#[test]
fn git_source_postcommit_jobs_are_durable_ordered_retryable_and_gc_pinned() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    for (commit_version, transaction_id, generation) in
        [(2, "git-source-1", 1), (3, "git-source-2", 2)]
    {
        store
            .apply_certified_bundle(
                commit_version,
                &bundle(transaction_id, |builder| {
                    builder.add_materialisation_job(
                        git_source_job(transaction_id, generation).encode().unwrap(),
                    );
                }),
            )
            .unwrap();
    }
    store
        .apply_certified_bundle(5, &bundle("advance", |_| {}))
        .unwrap();

    let expected_partition = crate::mvcc_worker_authority::work_partition_id(
        "git-source-postcommit",
        "tenant/1/git-source/repository-a/generation/1",
    )
    .unwrap();
    assert!(
        store
            .required_background_work_partitions()
            .unwrap()
            .contains(&expected_partition)
    );
    assert_eq!(
        store
            .unfinished_work_pins()
            .unwrap()
            .materialisation_snapshots,
        [2_u64, 3].into_iter().collect()
    );
    assert!(store.garbage_collect(5).is_err());

    let (first_id, first) = store
        .claim_git_source_postcommit_authorized("worker", 10, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.generation, 1);
    assert_eq!(first.lease_owner.as_deref(), Some("assignment-owner"));
    assert!(
        store
            .complete_git_source_postcommit(&first_id, "worker")
            .is_err()
    );
    store
        .retry_git_source_postcommit(&first_id, "assignment-owner", 20, "retry")
        .unwrap();
    assert!(
        store
            .claim_git_source_postcommit_authorized("worker", 19, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
    let (first_id, first) = store
        .claim_git_source_postcommit_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.generation, 1);
    store
        .complete_git_source_postcommit(&first_id, "assignment-owner")
        .unwrap();

    let (second_id, second) = store
        .claim_git_source_postcommit_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(second.job.generation, 2);
    store
        .complete_git_source_postcommit(&second_id, "assignment-owner")
        .unwrap();
    assert!(store.unfinished_work_pins().unwrap().all().is_empty());
    store.garbage_collect(5).unwrap();
    assert!(
        store
            .claim_git_source_postcommit_authorized("worker", 30, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
}

#[test]
fn object_link_jobs_survive_reopen_and_are_ordered_fenced_retryable_and_gc_pinned() {
    let temp = tempdir().unwrap();
    {
        let store = MvccStore::open(temp.path()).unwrap();
        for (version, transaction_id, generation) in [(2, "link-1", 1), (3, "link-2", 2)] {
            store
                .apply_certified_bundle(
                    version,
                    &bundle(transaction_id, |builder| {
                        builder.add_materialisation_job(
                            object_link_job(transaction_id, generation)
                                .encode()
                                .unwrap(),
                        );
                    }),
                )
                .unwrap();
        }
        store
            .apply_certified_bundle(5, &bundle("advance-link-jobs", |_| {}))
            .unwrap();
    }

    let store = MvccStore::open(temp.path()).unwrap();
    let partition = crate::mvcc_worker_authority::work_partition_id(
        "object-link-finalization",
        "tenant/1/bucket/2/object-link/links/latest/generation/1",
    )
    .unwrap();
    assert!(
        store
            .required_background_work_partitions()
            .unwrap()
            .contains(&partition)
    );
    assert_eq!(
        store
            .unfinished_work_pins()
            .unwrap()
            .materialisation_snapshots,
        [2_u64, 3].into_iter().collect()
    );
    assert!(store.garbage_collect(5).is_err());

    let (first_id, first) = store
        .claim_object_link_finalization_authorized("worker", 10, 5, |_| {
            Some("assignment-7/owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.generation, 1);
    assert_eq!(first.lease_owner.as_deref(), Some("assignment-7/owner"));
    assert!(
        store
            .complete_object_link_finalization(&first_id, "stale-owner")
            .is_err()
    );
    store
        .retry_object_link_finalization(&first_id, "assignment-7/owner", 20, "retry")
        .unwrap();
    assert!(
        store
            .claim_object_link_finalization_authorized("worker", 19, 5, |_| {
                Some("assignment-7/owner".into())
            })
            .unwrap()
            .is_none()
    );
    let (first_id, first) = store
        .claim_object_link_finalization_authorized("worker", 20, 5, |_| {
            Some("assignment-8/owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.generation, 1);
    store
        .complete_object_link_finalization(&first_id, "assignment-8/owner")
        .unwrap();

    let (second_id, second) = store
        .claim_object_link_finalization_authorized("worker", 20, 5, |_| {
            Some("assignment-8/owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(second.job.generation, 2);
    store
        .complete_object_link_finalization(&second_id, "assignment-8/owner")
        .unwrap();
    assert!(store.unfinished_work_pins().unwrap().all().is_empty());
    store.garbage_collect(5).unwrap();
    assert!(
        store
            .claim_object_link_finalization_authorized("worker", 30, 5, |_| {
                Some("assignment-8/owner".into())
            })
            .unwrap()
            .is_none()
    );
}

#[test]
fn hf_ingestion_postcommit_jobs_are_durable_ordered_retryable_and_gc_pinned() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    for (commit_version, transaction_id, ingestion_id) in
        [(2, "hf-ingestion-1", 41), (3, "hf-ingestion-2", 42)]
    {
        store
            .apply_certified_bundle(
                commit_version,
                &bundle(transaction_id, |builder| {
                    builder.add_materialisation_job(
                        hf_ingestion_job(transaction_id, ingestion_id)
                            .encode()
                            .unwrap(),
                    );
                }),
            )
            .unwrap();
    }
    store
        .apply_certified_bundle(5, &bundle("advance-hf", |_| {}))
        .unwrap();
    drop(store);
    let store = MvccStore::open(temp.path()).unwrap();
    assert_eq!(
        store
            .find_hf_ingestion_postcommit_by_transaction("hf-ingestion-1")
            .unwrap()
            .unwrap()
            .job
            .ingestion_id,
        41
    );

    let expected_partition = crate::mvcc_worker_authority::work_partition_id(
        "hf-ingestion-postcommit",
        "tenant/1/hf-ingestion/41",
    )
    .unwrap();
    assert!(
        store
            .required_background_work_partitions()
            .unwrap()
            .contains(&expected_partition)
    );
    assert_eq!(
        store
            .unfinished_work_pins()
            .unwrap()
            .materialisation_snapshots,
        [2_u64, 3].into_iter().collect()
    );
    assert!(store.garbage_collect(5).is_err());

    let (first_id, first) = store
        .claim_hf_ingestion_postcommit_authorized("worker", 10, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.ingestion_id, 41);
    assert_eq!(first.lease_owner.as_deref(), Some("assignment-owner"));
    assert!(
        store
            .complete_hf_ingestion_postcommit(&first_id, "worker")
            .is_err()
    );
    store
        .retry_hf_ingestion_postcommit(&first_id, "assignment-owner", 20, "retry")
        .unwrap();
    assert!(
        store
            .claim_hf_ingestion_postcommit_authorized("worker", 19, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
    let (first_id, first) = store
        .claim_hf_ingestion_postcommit_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(first.job.ingestion_id, 41);
    store
        .complete_hf_ingestion_postcommit(&first_id, "assignment-owner")
        .unwrap();

    let (second_id, second) = store
        .claim_hf_ingestion_postcommit_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(second.job.ingestion_id, 42);
    store
        .complete_hf_ingestion_postcommit(&second_id, "assignment-owner")
        .unwrap();
    assert!(store.unfinished_work_pins().unwrap().all().is_empty());
    store.garbage_collect(5).unwrap();
    assert!(
        store
            .claim_hf_ingestion_postcommit_authorized("worker", 30, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
}

#[test]
fn bucket_locator_jobs_are_durable_ordered_retryable_and_gc_pinned() {
    use crate::bucket_locator_finalization_job::BucketLocatorFinalizationOperation;

    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    for (commit_version, transaction_id, operation) in [
        (
            2,
            "bucket-create",
            BucketLocatorFinalizationOperation::Publish,
        ),
        (
            3,
            "bucket-delete",
            BucketLocatorFinalizationOperation::Delete,
        ),
    ] {
        store
            .apply_certified_bundle(
                commit_version,
                &bundle(transaction_id, |builder| {
                    builder.add_materialisation_job(
                        bucket_locator_job(transaction_id, commit_version, operation)
                            .encode()
                            .unwrap(),
                    );
                }),
            )
            .unwrap();
    }
    store
        .apply_certified_bundle(5, &bundle("advance", |_| {}))
        .unwrap();

    let expected_partition = crate::mvcc_worker_authority::work_partition_id(
        "bucket-locator-finalization",
        "tenant/1/bucket/17/locator",
    )
    .unwrap();
    assert!(
        store
            .required_background_work_partitions()
            .unwrap()
            .contains(&expected_partition)
    );
    assert_eq!(
        store
            .unfinished_work_pins()
            .unwrap()
            .materialisation_snapshots,
        [2_u64, 3].into_iter().collect()
    );
    assert!(store.garbage_collect(5).is_err());

    let (create_id, create) = store
        .claim_bucket_locator_finalization_authorized("worker", 10, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        create.job.operation,
        BucketLocatorFinalizationOperation::Publish
    );
    store
        .retry_bucket_locator_finalization(&create_id, "assignment-owner", 20, "retry")
        .unwrap();
    assert!(
        store
            .claim_bucket_locator_finalization_authorized("worker", 19, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
    let (create_id, _) = store
        .claim_bucket_locator_finalization_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    store
        .complete_bucket_locator_finalization(&create_id, "assignment-owner")
        .unwrap();

    let (delete_id, delete) = store
        .claim_bucket_locator_finalization_authorized("worker", 20, 5, |_| {
            Some("assignment-owner".into())
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        delete.job.operation,
        BucketLocatorFinalizationOperation::Delete
    );
    store
        .complete_bucket_locator_finalization(&delete_id, "assignment-owner")
        .unwrap();
    assert!(store.unfinished_work_pins().unwrap().all().is_empty());
    store.garbage_collect(5).unwrap();
    assert!(
        store
            .claim_bucket_locator_finalization_authorized("worker", 30, 5, |_| {
                Some("assignment-owner".into())
            })
            .unwrap()
            .is_none()
    );
}

#[test]
fn committed_local_object_promotion_is_queryable_by_content_identity() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let object_hash = format!("sha256:{}", "a".repeat(64));
    let job = LocalDurabilityUpgradeJob {
        schema: LocalDurabilityUpgradeJob::SCHEMA.to_string(),
        cluster_id: "cluster".to_string(),
        transaction_id: "local-object".to_string(),
        commit_version: 0,
        bundle: None,
        target: crate::mvcc_transaction::DurabilityLevel::Erasure,
        objects: vec![
            crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeObject {
                object_identity: uuid::Uuid::from_u128(7),
                local_manifest: crate::local_object_store::LocalObjectManifest {
                    schema_version: 1,
                    cluster_id: "cluster".to_string(),
                    object_hash: object_hash.clone(),
                    object_length: 5,
                    node: crate::mvcc_transaction::NodeIncarnation {
                        node_id: "node-a".to_string(),
                        incarnation: 1,
                    },
                    failure_domain: "zone-a".to_string(),
                },
            },
        ],
        requested_at_unix_ms: 10,
    };
    store
        .apply_certified_bundle(
            3,
            &bundle("local-object", |builder| {
                builder.add_materialisation_job(job.canonical_bytes().unwrap());
            }),
        )
        .unwrap();

    let (promotion_id, record) = store
        .local_durability_upgrade_for_object(&object_hash)
        .unwrap()
        .expect("committed local object has a durable promotion");
    assert_eq!(promotion_id, record.job.job_id().unwrap());
    assert_eq!(record.job.commit_version, 3);
    assert!(record.job.bundle.is_some());
    assert_eq!(record.state, LocalDurabilityUpgradeState::Pending);
    let (_, claimed) = store
        .claim_local_durability_upgrade("worker", 10, 5)
        .unwrap()
        .expect("promotion is claimable");
    store
        .retry_local_durability_upgrade(
            &promotion_id,
            claimed.lease_owner.as_deref().unwrap(),
            99,
            "temporary failure",
        )
        .unwrap();
    let (_, requested) = store
        .request_local_durability_upgrade_for_object(
            &object_hash,
            crate::mvcc_transaction::DurabilityLevel::Quorum,
        )
        .unwrap()
        .expect("explicit request reuses the committed promotion");
    assert_eq!(requested.next_attempt_unix_ms, 0);
    assert_eq!(requested.last_error.as_deref(), Some("temporary failure"));
    assert!(
        store
            .local_durability_upgrade_for_object(&format!("sha256:{}", "b".repeat(64)))
            .unwrap()
            .is_none()
    );
}

#[test]
fn outbox_events_install_atomically_and_claim_durably() {
    let temp = tempdir().unwrap();
    let row = key(7, b"account");
    let store = MvccStore::open(temp.path()).unwrap();
    store
        .apply_certified_bundle(
            3,
            &bundle("with-outbox", |builder| {
                builder.put(row.clone(), b"visible".to_vec());
                builder.add_outbox_event(
                    crate::mvcc_outbox::StreamOutboxEvent::new(
                        7,
                        "events",
                        "partition-7",
                        "account.changed",
                        b"notify-account".to_vec(),
                    )
                    .unwrap()
                    .encode()
                    .unwrap(),
                );
            }),
        )
        .unwrap();

    assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"visible");
    let records = store.outbox_records_after(0, 10).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].commit_version, 3);
    assert_eq!(
        crate::mvcc_outbox::StreamOutboxEvent::decode(&records[0].payload)
            .unwrap()
            .payload,
        b"notify-account"
    );
    assert_eq!(records[0].state, OutboxState::Pending);

    let first = store.claim_outbox("worker-a", 10, 5).unwrap().unwrap();
    assert_eq!(first.state, OutboxState::Running);
    assert_eq!(first.attempts, 1);
    assert!(store.claim_outbox("worker-b", 14, 5).unwrap().is_none());
    let reclaimed = store.claim_outbox("worker-b", 15, 5).unwrap().unwrap();
    assert_eq!(reclaimed.event_id, first.event_id);
    assert_eq!(reclaimed.attempts, 2);
    store.complete_outbox(&reclaimed, "worker-b").unwrap();
    store.complete_outbox(&reclaimed, "worker-b").unwrap();
    assert!(store.claim_outbox("worker-a", 100, 5).unwrap().is_none());
    assert_eq!(
        store.outbox_records_after(0, 10).unwrap()[0].state,
        OutboxState::Delivered
    );

    drop(store);
    let reopened = MvccStore::open(temp.path()).unwrap();
    let persisted = reopened.outbox_records_after(0, 10).unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].state, OutboxState::Delivered);
    assert_eq!(persisted[0].event_id, first.event_id);
}

#[test]
fn table_prefix_scan_is_filtered_and_snapshot_consistent() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    store
        .apply_certified_bundle(
            1,
            &bundle("initial", |builder| {
                builder.put(key(7, b"part/a"), b"a1".to_vec());
                builder.put(key(7, b"part/b"), b"b1".to_vec());
                builder.put(key(7, b"other/c"), b"c1".to_vec());
                builder.put(key(8, b"part/d"), b"d1".to_vec());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            2,
            &bundle("update", |builder| {
                builder.put(key(7, b"part/a"), b"a2".to_vec());
                builder.delete(key(7, b"part/b"));
            }),
        )
        .unwrap();

    let at_one = store.scan_table_prefix_at(7, b"part/", 1).unwrap();
    assert_eq!(at_one.len(), 2);
    assert_eq!(at_one[0].1.value, b"a1");
    assert_eq!(at_one[1].1.value, b"b1");

    let at_two = store.scan_table_prefix_at(7, b"part/", 2).unwrap();
    assert_eq!(at_two.len(), 1);
    assert_eq!(at_two[0].0.application_key, b"part/a");
    assert_eq!(at_two[0].1.value, b"a2");

    let bounded = store
        .scan_table_prefix_at_bounded(7, b"part/", 1, 1)
        .unwrap();
    assert_eq!(bounded.len(), 1);
    assert_eq!(
        store
            .scan_table_prefix_at_bounded(7, b"part/", 1, 0)
            .unwrap(),
        Vec::new()
    );
}

#[test]
fn materialisation_leases_retry_and_recover_after_expiry() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let job = ObjectMaterialisationJob {
        schema: ObjectMaterialisationJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: "jobs".into(),
        tenant_id: 1,
        bucket_id: 2,
        bucket_name: "bucket".into(),
        object_key: "object".into(),
        object_version_id: "version".into(),
        target_logical_identity: "tenant/1/bucket/2/object/object/version/version".into(),
        representation: serde_json::json!({"schema": "local"}),
        content_hash: "sha256:payload".into(),
        payload_length: 3,
        frozen_object: serde_json::json!({
            "version_id": "version",
            "content_hash": "sha256:payload",
            "size": 3,
        }),
        source_manifest_hash: "0000000000000000000000000000000000000000000000000000000000000000"
            .into(),
        content_type: Some("application/json".into()),
        user_metadata: serde_json::json!({}),
        index_policy_snapshot: serde_json::json!({}),
        originating_snapshot_version: 0,
        frozen_index_definitions: Vec::new(),
        authz_revision: 1,
        boundary_schema: None,
        boundary_schema_generation: 0,
        boundary_schema_hash: None,
        requested_operations: crate::object_materialisation::ObjectMaterialisationOperations {
            extract_boundaries: true,
            maintain_indexes: true,
        },
        requested_at_unix_ms: 1,
    };
    let id = job.job_id().unwrap();
    store
        .apply_certified_bundle(
            1,
            &bundle("jobs", |builder| {
                builder.add_materialisation_job(job.canonical_bytes().unwrap());
            }),
        )
        .unwrap();

    let (_, first) = store
        .claim_object_materialisation("worker-a", 10, 10)
        .unwrap()
        .unwrap();
    assert_eq!(first.attempts, 1);
    assert!(
        store
            .claim_object_materialisation("worker-b", 19, 10)
            .unwrap()
            .is_none()
    );
    let (_, recovered) = store
        .claim_object_materialisation("worker-b", 20, 10)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.attempts, 2);
    store
        .retry_object_materialisation(&id, "worker-b", 40, "transient")
        .unwrap();
    assert!(
        store
            .claim_object_materialisation("worker-a", 39, 10)
            .unwrap()
            .is_none()
    );
    store
        .claim_object_materialisation("worker-a", 40, 10)
        .unwrap()
        .unwrap();
    store
        .complete_object_materialisation(&id, "worker-a")
        .unwrap();
    assert!(
        store
            .claim_object_materialisation("worker-b", 100, 10)
            .unwrap()
            .is_none()
    );
}

#[test]
fn materialisation_claims_oldest_snapshot_before_newer_hot_key_work() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let job = |transaction_id: &str, snapshot: u64| ObjectMaterialisationJob {
        schema: ObjectMaterialisationJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: transaction_id.into(),
        tenant_id: 1,
        bucket_id: 2,
        bucket_name: "bucket".into(),
        object_key: format!("objects/{transaction_id}"),
        object_version_id: format!("version-{transaction_id}"),
        target_logical_identity: format!(
            "tenant/1/bucket/2/object/objects/{transaction_id}/version/version-{transaction_id}"
        ),
        representation: serde_json::json!({"schema": "local"}),
        content_hash: format!("sha256:{transaction_id}"),
        payload_length: 3,
        frozen_object: serde_json::json!({
            "version_id": format!("version-{transaction_id}"),
            "content_hash": format!("sha256:{transaction_id}"),
            "size": 3,
        }),
        source_manifest_hash: "0000000000000000000000000000000000000000000000000000000000000000"
            .into(),
        content_type: Some("application/json".into()),
        user_metadata: serde_json::json!({}),
        index_policy_snapshot: serde_json::json!({}),
        originating_snapshot_version: snapshot,
        frozen_index_definitions: Vec::new(),
        authz_revision: 1,
        boundary_schema: None,
        boundary_schema_generation: 0,
        boundary_schema_hash: None,
        requested_operations: crate::object_materialisation::ObjectMaterialisationOperations {
            extract_boundaries: true,
            maintain_indexes: true,
        },
        requested_at_unix_ms: snapshot,
    };
    let older = job("older", 10);
    let newer = job("newer", 20);
    store
        .apply_certified_bundle(
            1,
            &bundle("older", |builder| {
                builder.add_materialisation_job(older.canonical_bytes().unwrap());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            2,
            &bundle("newer", |builder| {
                builder.add_materialisation_job(newer.canonical_bytes().unwrap());
            }),
        )
        .unwrap();

    let (_, claimed) = store
        .claim_object_materialisation("worker", 100, 10)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.job.transaction_id, "older");
}

#[test]
fn rejects_a_bundle_from_another_cluster_before_writing() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let mut builder = TransactionBundleBuilder::new(
        "foreign",
        "tx",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    builder.put(key(1, b"key"), b"value".to_vec());

    let error = store
        .apply_certified_bundle(1, &builder.build().unwrap())
        .unwrap_err();
    assert!(error.to_string().contains("another cluster"));
    assert_eq!(store.applied_version().unwrap(), 0);
}

#[test]
fn tombstones_hide_only_snapshots_at_and_after_the_delete() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(1, b"k");
    store
        .apply_certified_bundle(
            3,
            &bundle("put", |b| {
                b.put(row.clone(), b"value".to_vec());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            8,
            &bundle("delete", |b| {
                b.delete(row.clone());
            }),
        )
        .unwrap();

    assert!(store.read_at(&row, 7).unwrap().is_some());
    assert_eq!(store.read_at(&row, 8).unwrap(), None);
    assert_eq!(store.read_latest(&row).unwrap(), None);
}

#[test]
fn point_snapshots_retain_tombstone_commit_versions() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(1, b"deleted");
    let never_written = key(1, b"never-written");
    store
        .apply_certified_bundle(
            3,
            &bundle("put-before-point-delete", |builder| {
                builder.put(row.clone(), b"value".to_vec());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            8,
            &bundle("point-delete", |builder| {
                builder.delete(row.clone());
            }),
        )
        .unwrap();

    assert_eq!(
        store.read_point_at(&row, 7).unwrap(),
        PointSnapshot::Value(VisibleRow {
            commit_version: 3,
            value: b"value".to_vec(),
        })
    );
    assert_eq!(
        store.read_point_at(&row, 8).unwrap(),
        PointSnapshot::Tombstone { commit_version: 8 }
    );
    assert_eq!(
        store.read_point_at(&never_written, 8).unwrap(),
        PointSnapshot::Unwritten
    );
    assert_eq!(store.read_at(&row, 8).unwrap(), None);
}

#[test]
fn non_data_decisions_advance_the_readable_snapshot_watermark() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(1, b"missing");

    store.advance_decision_watermark(1).unwrap();

    assert_eq!(store.applied_version().unwrap(), 0);
    assert_eq!(store.decision_watermark().unwrap(), 1);
    assert_eq!(store.readable_version().unwrap(), 1);
    assert_eq!(store.read_at(&row, 1).unwrap(), None);
    assert!(
        store
            .read_at(&row, 2)
            .unwrap_err()
            .to_string()
            .contains("snapshot 2 is above local readable version 1")
    );
}

#[test]
fn decision_watermark_cannot_advance_over_a_missing_position() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();

    let error = store.advance_decision_watermark(2).unwrap_err();

    assert!(error.to_string().contains("MVCC decision gap"));
    assert_eq!(store.decision_watermark().unwrap(), 0);
    assert_eq!(store.readable_version().unwrap(), 0);

    store.advance_decision_watermark(1).unwrap();
    store.advance_decision_watermark(2).unwrap();
    assert_eq!(store.decision_watermark().unwrap(), 2);
}

#[test]
fn committed_bundle_cannot_skip_an_unapplied_decision() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let first_key = key(1, b"first");
    let second_key = key(1, b"second");
    let first = bundle("first", |builder| {
        builder.put(first_key.clone(), b"first".to_vec());
    });
    let second = bundle("second", |builder| {
        builder.put(second_key.clone(), b"second".to_vec());
    });

    let error = store
        .apply_certified_bundle_and_advance(2, &second, 2)
        .unwrap_err();

    assert!(error.to_string().contains("MVCC decision gap"));
    assert_eq!(store.applied_version().unwrap(), 0);
    assert_eq!(store.decision_watermark().unwrap(), 0);
    assert_eq!(store.readable_version().unwrap(), 0);
    assert!(store.read_latest(&second_key).unwrap().is_none());
    assert!(store.read_at(&second_key, 2).is_err());

    store
        .apply_certified_bundle_and_advance(1, &first, 1)
        .unwrap();
    store
        .apply_certified_bundle_and_advance(2, &second, 2)
        .unwrap();

    assert_eq!(store.applied_version().unwrap(), 2);
    assert_eq!(store.decision_watermark().unwrap(), 2);
    assert_eq!(
        store.read_latest(&first_key).unwrap().unwrap().value,
        b"first"
    );
    assert_eq!(
        store.read_latest(&second_key).unwrap().unwrap().value,
        b"second"
    );
}

#[test]
fn stale_worker_replay_cannot_regress_or_fail_the_decision_watermark() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let committed = bundle("simultaneous-workers", |builder| {
        builder.put(key(1, b"row"), b"value".to_vec());
    });
    assert_eq!(
        store
            .apply_certified_bundle_and_advance(1, &committed, 1)
            .unwrap(),
        ApplyOutcome::Applied
    );

    // Model the interleaving where another worker has already applied a
    // later non-data decision before this worker replays decision one.
    store.advance_decision_watermark(2).unwrap();
    assert_eq!(
        store
            .apply_certified_bundle_and_advance(1, &committed, 1)
            .unwrap(),
        ApplyOutcome::Replayed
    );
    assert_eq!(store.decision_watermark().unwrap(), 2);
    assert_eq!(store.applied_version().unwrap(), 1);
}

#[test]
fn applying_a_bundle_is_idempotent_but_version_reuse_is_rejected() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let first = bundle("first", |b| {
        b.put(key(1, b"a"), b"a".to_vec());
    });
    assert_eq!(
        store.apply_certified_bundle(4, &first).unwrap(),
        ApplyOutcome::Applied
    );
    assert_eq!(
        store.apply_certified_bundle(4, &first).unwrap(),
        ApplyOutcome::Replayed
    );
    let other = bundle("other", |b| {
        b.put(key(1, b"a"), b"b".to_vec());
    });
    assert!(store.apply_certified_bundle(4, &other).is_err());
    assert_eq!(store.applied_version().unwrap(), 4);
}

#[test]
fn committed_idempotency_results_apply_atomically_and_follow_gc_watermark() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let transaction_id = "result-transaction";
    let result = crate::mvcc_transaction::IdempotencyResult {
        namespace: "bucket.create".into(),
        key: "request-1".into(),
        payload: b"response".to_vec(),
    };
    store
        .apply_certified_bundle(
            1,
            &bundle(transaction_id, |builder| {
                builder.add_idempotency_result(result.clone());
            }),
        )
        .unwrap();
    assert_eq!(
        store
            .committed_idempotency_result(transaction_id, &result.namespace, &result.key,)
            .unwrap()
            .unwrap()
            .result,
        result
    );

    store
        .apply_certified_bundle(2, &bundle("advance-result-watermark", |_| {}))
        .unwrap();
    store.garbage_collect(2).unwrap();
    assert!(
        store
            .committed_idempotency_result(transaction_id, "bucket.create", "request-1",)
            .unwrap()
            .is_none()
    );
}

#[test]
fn gc_keeps_the_visibility_anchor_and_newer_history() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(2, b"row");
    for (version, value) in [(2, b"two".as_slice()), (5, b"five"), (9, b"nine")] {
        store
            .apply_certified_bundle(
                version,
                &bundle(&format!("v{version}"), |b| {
                    b.put(row.clone(), value.to_vec());
                }),
            )
            .unwrap();
    }

    assert_eq!(store.garbage_collect(6).unwrap(), 3);
    assert_eq!(store.gc_watermark().unwrap(), 6);
    assert_eq!(store.read_at(&row, 6).unwrap().unwrap().value, b"five");
    assert_eq!(store.read_latest(&row).unwrap().unwrap().value, b"nine");
    assert!(store.garbage_collect(5).is_err());
}

#[test]
fn gc_preserves_tombstone_anchor_at_the_watermark() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let row = key(2, b"deleted");
    store
        .apply_certified_bundle(
            2,
            &bundle("put-before-delete", |builder| {
                builder.put(row.clone(), b"value".to_vec());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(
            5,
            &bundle("delete-anchor", |builder| {
                builder.delete(row.clone());
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(8, &bundle("later-unrelated", |_| {}))
        .unwrap();

    store.garbage_collect(6).unwrap();
    assert_eq!(
        store.read_point_at(&row, 6).unwrap(),
        PointSnapshot::Tombstone { commit_version: 5 }
    );
    assert_eq!(store.read_at(&row, 6).unwrap(), None);
    assert_eq!(store.read_latest(&row).unwrap(), None);
}

#[test]
fn published_repair_releases_replica_pin_and_retires_only_the_old_target() {
    use crate::{
        mvcc_shard_repair::{MissingShardTarget, ShardMaintenanceKind, ShardPlacementOverlay},
        object_shard_manifest::{
            OBJECT_SHARD_MANIFEST_SCHEMA, PhysicalObjectShardManifest, PhysicalShardPlacement,
        },
        shard_placement::ShardTarget,
    };

    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    let object_identity = uuid::Uuid::from_u128(1);
    let transfer_id = uuid::Uuid::from_u128(2);
    let payload_hash = [7; 32];
    let old_placement = PhysicalShardPlacement {
        stripe_ordinal: 0,
        shard_ordinal: 0,
        payload_length: 4,
        payload_hash,
        transfer_id,
        node_id: "node-old".into(),
        node_incarnation: 1,
        failure_domain: "rack-old".into(),
    };
    let parity_placement = PhysicalShardPlacement {
        stripe_ordinal: 0,
        shard_ordinal: 1,
        payload_length: 4,
        payload_hash: [8; 32],
        transfer_id: uuid::Uuid::from_u128(3),
        node_id: "node-parity".into(),
        node_incarnation: 1,
        failure_domain: "rack-parity".into(),
    };
    let source = PhysicalObjectShardManifest {
        schema_version: OBJECT_SHARD_MANIFEST_SCHEMA,
        cluster_id: "cluster".into(),
        object_identity,
        object_hash: format!("sha256:{}", hex::encode([9; 32])),
        object_length: 4,
        encoding_generation: 1,
        data_shards: 1,
        parity_shards: 1,
        shard_bytes: 4,
        stripe_count: 1,
        placements: vec![old_placement.clone(), parity_placement.clone()],
    };
    let new_placement = PhysicalShardPlacement {
        node_id: "node-new".into(),
        node_incarnation: 1,
        failure_domain: "rack-new".into(),
        ..old_placement.clone()
    };
    let target_logical_identity = format!("cluster/cluster/object/{}", source.object_hash);
    let source_manifest_hash =
        hex::encode(blake3::hash(&source.canonical_bytes().unwrap()).as_bytes());
    let job = ShardRepairJob {
        schema: ShardRepairJob::SCHEMA.into(),
        cluster_id: "cluster".into(),
        transaction_id: "repair".into(),
        kind: ShardMaintenanceKind::Rebalance,
        target_logical_identity: target_logical_identity.clone(),
        source_manifest: source.clone(),
        source_manifest_hash: source_manifest_hash.clone(),
        missing: vec![MissingShardTarget {
            stripe_ordinal: 0,
            shard_ordinal: 0,
            target: ShardTarget {
                cluster_id: "cluster".into(),
                node: NodeIncarnation {
                    node_id: "node-new".into(),
                    incarnation: 1,
                },
                failure_domain: "rack-new".into(),
            },
        }],
        retiring: vec![old_placement.clone()],
        originating_snapshot_version: 1,
        requested_at_unix_ms: 10,
    };
    let job_id = job.job_id().unwrap();
    store
        .apply_certified_bundle(
            2,
            &bundle("repair", |builder| {
                builder.add_materialisation_job(job.canonical_bytes().unwrap());
            }),
        )
        .unwrap();
    assert_eq!(
        store.unfinished_work_pins().unwrap().repair_snapshots,
        BTreeSet::from([1])
    );

    let replacement = PhysicalObjectShardManifest {
        placements: vec![new_placement, parity_placement],
        ..source
    };
    let overlay_key = LogicalKey {
        table_id: ShardPlacementOverlay::TABLE_ID,
        application_key: target_logical_identity.into_bytes(),
    };
    let overlay = ShardPlacementOverlay {
        schema: ShardPlacementOverlay::SCHEMA.into(),
        cluster_id: "cluster".into(),
        target_logical_identity: String::from_utf8(overlay_key.application_key.clone()).unwrap(),
        source_manifest_hash,
        replacement_manifest: replacement,
        retired_after_commit: vec![old_placement],
    };
    store
        .apply_certified_bundle(
            3,
            &bundle("publish-repair", |builder| {
                builder.put(overlay_key, serde_json::to_vec(&overlay).unwrap());
            }),
        )
        .unwrap();

    assert_eq!(
        store.shard_repair_record(&job_id).unwrap().unwrap().state,
        ShardRepairState::Pending,
        "only the assigned worker completes its replica-local journal row"
    );
    assert!(
        store.unfinished_work_pins().unwrap().all().is_empty(),
        "the committed overlay is the cluster-wide completion fact"
    );
    store.garbage_collect(3).unwrap();
    assert!(store.shard_repair_record(&job_id).unwrap().is_none());

    let old_node = NodeIncarnation {
        node_id: "node-old".into(),
        incarnation: 1,
    };
    let new_node = NodeIncarnation {
        node_id: "node-new".into(),
        incarnation: 1,
    };
    assert_eq!(
        store.retirable_object_shard_transfers(&old_node).unwrap(),
        BTreeSet::from([transfer_id])
    );
    assert!(
        store
            .retirable_object_shard_transfers(&new_node)
            .unwrap()
            .is_empty(),
        "the same deterministic transfer ID remains live on its replacement node"
    );
    assert!(
        store
            .protected_object_shard_transfers(&new_node)
            .unwrap()
            .contains(&transfer_id)
    );
}

#[test]
fn pending_outbox_survives_gc_without_pinning_mvcc_history() {
    let temp = tempdir().unwrap();
    let store = MvccStore::open(temp.path()).unwrap();
    store
        .apply_certified_bundle(
            2,
            &bundle("outbox", |builder| {
                builder.add_outbox_event(
                    crate::mvcc_outbox::StreamOutboxEvent::new(
                        7,
                        "events",
                        "partition-7",
                        "test.event",
                        b"event".to_vec(),
                    )
                    .unwrap()
                    .encode()
                    .unwrap(),
                );
            }),
        )
        .unwrap();
    store
        .apply_certified_bundle(5, &bundle("advance", |_| {}))
        .unwrap();

    let pins = store.unfinished_work_pins().unwrap();
    assert!(pins.all().is_empty());
    assert!(pins.transaction_ids.is_empty());
    store.garbage_collect(5).unwrap();
    assert_eq!(
        store.outbox_records_after(0, 10).unwrap().len(),
        1,
        "pending self-contained event payload remains available after MVCC history GC"
    );

    let record = store.claim_outbox("worker", 10, 10).unwrap().unwrap();
    store.complete_outbox(&record, "worker").unwrap();
    assert!(store.unfinished_work_pins().unwrap().all().is_empty());
    store.garbage_collect(5).unwrap();
    assert!(store.outbox_records_after(0, 10).unwrap().is_empty());
}

#[test]
fn one_batch_updates_multiple_tables_and_survives_reopen() {
    let temp = tempdir().unwrap();
    let a = key(1, b"same");
    let b = key(9, b"same");
    {
        let store = MvccStore::open(temp.path()).unwrap();
        let transaction = bundle("cross-table", |builder| {
            builder.put(a.clone(), b"a".to_vec());
            builder.put(b.clone(), b"b".to_vec());
        });
        store.apply_certified_bundle(11, &transaction).unwrap();
    }
    let reopened = MvccStore::open(temp.path()).unwrap();
    assert_eq!(reopened.applied_version().unwrap(), 11);
    assert_eq!(reopened.read_latest(&a).unwrap().unwrap().value, b"a");
    assert_eq!(reopened.read_latest(&b).unwrap().unwrap().value, b"b");
}
