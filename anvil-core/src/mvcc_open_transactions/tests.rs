use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use tempfile::tempdir;
use tokio::sync::Notify;

use super::*;
use crate::mvcc_store::ApplyOutcome;

struct Runtime {
    snapshot: u64,
    committed: Mutex<Vec<TransactionBundle>>,
}

#[async_trait]
impl TransactionRuntime for Runtime {
    async fn transaction_snapshot(&self, _consistency: ReadConsistency) -> Result<CommitVersion> {
        Ok(self.snapshot)
    }

    async fn commit_transaction_bundle(
        &self,
        bundle: TransactionBundle,
        _durability: DurabilityLevel,
    ) -> Result<CommitOutcome> {
        self.committed.lock().unwrap().push(bundle);
        Ok(CommitOutcome {
            certification: CertificationResult::Committed { commit_version: 12 },
            local_apply: Some(ApplyOutcome::Applied),
        })
    }

    fn apply_transaction_decision(
        &self,
        bundle: TransactionBundle,
        result: CertificationResult,
    ) -> Result<CommitOutcome> {
        self.committed.lock().unwrap().push(bundle);
        Ok(CommitOutcome {
            local_apply: matches!(result, CertificationResult::Committed { .. })
                .then_some(ApplyOutcome::Replayed),
            certification: result,
        })
    }
}

fn runtime() -> Runtime {
    Runtime {
        snapshot: 9,
        committed: Mutex::new(Vec::new()),
    }
}

fn index_finalization_job(transaction_id: &str) -> Vec<u8> {
    crate::index_finalization_job::IndexFinalizationJob {
        schema: crate::index_finalization_job::IndexFinalizationJob::SCHEMA.to_string(),
        cluster_id: "cluster".to_string(),
        transaction_id: transaction_id.to_string(),
        tenant_id: 1,
        bucket_id: 1,
        bucket_name: "bucket".to_string(),
        index_name: "index".to_string(),
        index_id: 1,
        index_version: 1,
        event_type: "created".to_string(),
        creator_principal: "alice".to_string(),
        frozen_definition: serde_json::json!({}),
    }
    .encode()
    .unwrap()
}

fn physical_manifest(
    cluster_id: &str,
) -> crate::object_shard_manifest::PhysicalObjectShardManifest {
    crate::object_shard_manifest::PhysicalObjectShardManifest {
        schema_version: crate::object_shard_manifest::OBJECT_SHARD_MANIFEST_SCHEMA,
        cluster_id: cluster_id.to_string(),
        object_identity: uuid::Uuid::from_bytes([7; 16]),
        object_hash: "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            .to_string(),
        object_length: 3,
        encoding_generation: 4,
        data_shards: 1,
        parity_shards: 1,
        shard_bytes: 256 * 1024,
        stripe_count: 1,
        placements: vec![
            crate::object_shard_manifest::PhysicalShardPlacement {
                stripe_ordinal: 0,
                shard_ordinal: 0,
                payload_length: 3,
                payload_hash: [1; 32],
                transfer_id: uuid::Uuid::from_bytes([1; 16]),
                node_id: "node-a".to_string(),
                node_incarnation: 1,
                failure_domain: "zone-a".to_string(),
            },
            crate::object_shard_manifest::PhysicalShardPlacement {
                stripe_ordinal: 0,
                shard_ordinal: 1,
                payload_length: 3,
                payload_hash: [2; 32],
                transfer_id: uuid::Uuid::from_bytes([2; 16]),
                node_id: "node-b".to_string(),
                node_incarnation: 1,
                failure_domain: "zone-b".to_string(),
            },
        ],
    }
}

async fn stage_manifest_draft(
    registry: &OpenTransactionRegistry,
    idempotency_key: &str,
    reference: ObjectShardManifestReference,
    catalog_value: Vec<u8>,
) -> String {
    let handle = registry
        .begin(
            &runtime(),
            "cluster",
            "alice",
            idempotency_key,
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::LocalSnapshot,
            1_000,
        )
        .await
        .unwrap();
    registry
        .add_manifest(&handle.transaction_id, "cluster", reference.clone(), 1_001)
        .unwrap();
    registry
        .put(
            &handle.transaction_id,
            "cluster",
            LogicalKey {
                table_id: crate::mvcc_shard_repair::SHARD_MANIFEST_CATALOG_TABLE_ID,
                application_key: format!("manifest/{}", reference.object_hash).into_bytes(),
            },
            catalog_value,
            1_001,
        )
        .unwrap();
    handle.transaction_id
}

fn recovery_local_store(path: impl AsRef<Path>) -> crate::local_object_store::LocalObjectStore {
    crate::local_object_store::LocalObjectStore::open(
        path,
        "cluster",
        crate::mvcc_transaction::NodeIncarnation {
            node_id: "node-a".to_string(),
            incarnation: 1,
        },
        "zone-a",
    )
    .unwrap()
}

struct GatedRuntime {
    calls: AtomicUsize,
    first_started: Notify,
    release_first: Notify,
}

#[async_trait]
impl TransactionRuntime for GatedRuntime {
    async fn transaction_snapshot(&self, _consistency: ReadConsistency) -> Result<CommitVersion> {
        Ok(9)
    }

    async fn commit_transaction_bundle(
        &self,
        _bundle: TransactionBundle,
        _durability: DurabilityLevel,
    ) -> Result<CommitOutcome> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        Ok(CommitOutcome {
            certification: CertificationResult::Committed { commit_version: 12 },
            local_apply: Some(ApplyOutcome::Applied),
        })
    }

    fn apply_transaction_decision(
        &self,
        _bundle: TransactionBundle,
        result: CertificationResult,
    ) -> Result<CommitOutcome> {
        Ok(CommitOutcome {
            certification: result,
            local_apply: Some(ApplyOutcome::Replayed),
        })
    }
}

struct FailingCommitRuntime;

#[async_trait]
impl TransactionRuntime for FailingCommitRuntime {
    async fn transaction_snapshot(&self, _consistency: ReadConsistency) -> Result<CommitVersion> {
        Ok(9)
    }

    async fn commit_transaction_bundle(
        &self,
        _bundle: TransactionBundle,
        _durability: DurabilityLevel,
    ) -> Result<CommitOutcome> {
        bail!("leave the durable draft in Committing for restart recovery")
    }

    fn apply_transaction_decision(
        &self,
        _bundle: TransactionBundle,
        _result: CertificationResult,
    ) -> Result<CommitOutcome> {
        bail!("the failing test runtime has no prior decision")
    }
}

#[tokio::test]
async fn durable_open_sessions_pin_snapshots_until_resolution_or_expiry() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let live = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "live-pin",
            Duration::from_secs(30),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            1_000,
        )
        .await
        .unwrap();

    assert_eq!(
        registry.active_snapshot_pins(1_001).unwrap(),
        [9].into_iter().collect()
    );
    registry
        .commit(&runtime, &live.transaction_id, "alice", 1_002)
        .await
        .unwrap();
    assert!(registry.active_snapshot_pins(1_003).unwrap().is_empty());

    registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "expired-pin",
            Duration::from_millis(5),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            2_000,
        )
        .await
        .unwrap();
    assert!(registry.active_snapshot_pins(2_005).unwrap().is_empty());
}

#[tokio::test]
async fn restart_restores_only_an_exact_durable_catalog_manifest() {
    let temp = tempdir().unwrap();
    let manifest = physical_manifest("cluster");
    let reference = manifest.reference().unwrap();
    {
        let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
        let transaction_id = stage_manifest_draft(
            &registry,
            "recover-manifest",
            reference.clone(),
            manifest.canonical_bytes().unwrap(),
        )
        .await;
        registry
            .commit(&FailingCommitRuntime, &transaction_id, "alice", 1_002)
            .await
            .unwrap_err();
    }

    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let evidence = crate::bundle_replication::ObjectEvidenceRegistry::default();
    let local_objects = recovery_local_store(temp.path().join("local-objects"));
    assert_eq!(
        crate::mvcc_physical_payload::restore_durable_object_evidence(
            &registry,
            &evidence,
            &local_objects,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        evidence
            .evidence_for_test(std::slice::from_ref(&reference))
            .unwrap()
            .len(),
        manifest.placements.len()
    );
}

#[tokio::test]
async fn restart_rejects_mismatched_or_malformed_catalog_manifest() {
    let mismatched = tempdir().unwrap();
    let manifest = physical_manifest("cluster");
    let mut wrong_reference = manifest.reference().unwrap();
    wrong_reference.data_shards += 1;
    let registry = OpenTransactionRegistry::open(mismatched.path()).unwrap();
    let transaction_id = stage_manifest_draft(
        &registry,
        "mismatched-manifest",
        wrong_reference,
        manifest.canonical_bytes().unwrap(),
    )
    .await;
    registry
        .commit(&FailingCommitRuntime, &transaction_id, "alice", 1_002)
        .await
        .unwrap_err();
    let local_objects = recovery_local_store(mismatched.path().join("local-objects"));
    let error = crate::mvcc_physical_payload::restore_durable_object_evidence(
        &registry,
        &crate::bundle_replication::ObjectEvidenceRegistry::default(),
        &local_objects,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match its bundle reference")
    );

    let malformed = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(malformed.path()).unwrap();
    let transaction_id = stage_manifest_draft(
        &registry,
        "malformed-manifest",
        manifest.reference().unwrap(),
        b"not-a-manifest".to_vec(),
    )
    .await;
    registry
        .commit(&FailingCommitRuntime, &transaction_id, "alice", 1_002)
        .await
        .unwrap_err();
    let local_objects = recovery_local_store(malformed.path().join("local-objects"));
    let error = crate::mvcc_physical_payload::restore_durable_object_evidence(
        &registry,
        &crate::bundle_replication::ObjectEvidenceRegistry::default(),
        &local_objects,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("decode staged physical object shard manifest")
    );
}

#[tokio::test]
async fn restart_skips_an_incomplete_open_manifest_but_rejects_it_once_committing() {
    let temp = tempdir().unwrap();
    let manifest = physical_manifest("cluster");
    let reference = manifest.reference().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let handle = registry
        .begin(
            &runtime(),
            "cluster",
            "alice",
            "partial-open",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::LocalSnapshot,
            1_000,
        )
        .await
        .unwrap();
    registry
        .add_manifest(&handle.transaction_id, "cluster", reference, 1_001)
        .unwrap();
    let evidence = crate::bundle_replication::ObjectEvidenceRegistry::default();
    let local_objects = recovery_local_store(temp.path().join("local-objects"));
    assert_eq!(
        crate::mvcc_physical_payload::restore_durable_object_evidence(
            &registry,
            &evidence,
            &local_objects,
        )
        .unwrap(),
        0
    );

    registry
        .commit(
            &FailingCommitRuntime,
            &handle.transaction_id,
            "alice",
            1_002,
        )
        .await
        .unwrap_err();
    let error = crate::mvcc_physical_payload::restore_durable_object_evidence(
        &registry,
        &evidence,
        &local_objects,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("lacks the staged catalog manifest")
    );
}

#[tokio::test]
async fn restart_restores_local_evidence_only_after_reverifying_the_file() {
    let temp = tempdir().unwrap();
    let object_directory = temp.path().join("local-objects");
    let local_objects = recovery_local_store(&object_directory);
    let mut reader = tokio::io::BufReader::new(&b"abc"[..]);
    let ingest = local_objects.persist(&mut reader).await.unwrap();
    let reference = ingest.reference.clone();

    let transaction_id = {
        let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
        let handle = registry
            .begin(
                &runtime(),
                "cluster",
                "alice",
                "recover-local-manifest",
                Duration::from_secs(30),
                DurabilityLevel::Local,
                ReadConsistency::LocalSnapshot,
                1_000,
            )
            .await
            .unwrap();
        registry
            .add_manifest(&handle.transaction_id, "cluster", reference.clone(), 1_001)
            .unwrap();
        let job = crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeJob {
            schema: crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeJob::SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: handle.transaction_id.clone(),
            commit_version: 0,
            bundle: None,
            target: DurabilityLevel::Erasure,
            objects: vec![
                crate::mvcc_local_durability_upgrade::LocalDurabilityUpgradeObject {
                    object_identity: uuid::Uuid::from_bytes([9; 16]),
                    local_manifest: ingest.manifest,
                },
            ],
            requested_at_unix_ms: 1_001,
        };
        registry
            .add_job(
                &handle.transaction_id,
                job.canonical_bytes().unwrap(),
                1_001,
            )
            .unwrap();
        registry
            .commit(
                &FailingCommitRuntime,
                &handle.transaction_id,
                "alice",
                1_002,
            )
            .await
            .unwrap_err();
        handle.transaction_id
    };

    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let evidence = crate::bundle_replication::ObjectEvidenceRegistry::default();
    assert_eq!(
        crate::mvcc_physical_payload::restore_durable_object_evidence(
            &registry,
            &evidence,
            &local_objects,
        )
        .unwrap(),
        1
    );
    assert!(matches!(
        evidence
            .evidence_for_test(std::slice::from_ref(&reference))
            .unwrap()
            .as_slice(),
        [crate::mvcc_transaction::ObjectDurabilityEvidence::LocalRepresentation { .. }]
    ));
    assert!(
        registry
            .status(&transaction_id, "alice", 1_003)
            .unwrap()
            .state
            == "committing"
    );

    let digest = reference.object_hash.strip_prefix("sha256:").unwrap();
    std::fs::remove_file(object_directory.join(format!("{digest}.object"))).unwrap();
    let error = crate::mvcc_physical_payload::restore_durable_object_evidence(
        &registry,
        &crate::bundle_replication::ObjectEvidenceRegistry::default(),
        &local_objects,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("No such file")
            || error.to_string().contains("cannot find the path")
    );
}

#[tokio::test]
async fn committed_idempotency_result_survives_registry_reopen() {
    let temp = tempdir().unwrap();
    let runtime = runtime();
    let transaction_id = {
        let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
        let handle = registry
            .begin(
                &runtime,
                "cluster",
                "alice",
                "durable-result",
                Duration::from_secs(30),
                DurabilityLevel::Local,
                ReadConsistency::LocalSnapshot,
                1_000,
            )
            .await
            .unwrap();
        registry
            .stage_logical_mutations(
                &handle.transaction_id,
                "alice",
                "cluster",
                vec![StagedLogicalMutation {
                    key: LogicalKey {
                        table_id: 1,
                        application_key: b"key".to_vec(),
                    },
                    observed_version: None,
                    value: Some(b"value".to_vec()),
                }],
                1_001,
            )
            .unwrap();
        registry
            .add_idempotency_result(
                &handle.transaction_id,
                "alice",
                crate::mvcc_transaction::IdempotencyResult {
                    namespace: "bucket.create".into(),
                    key: "request-1".into(),
                    payload: b"response".to_vec(),
                },
                1_001,
            )
            .unwrap();
        registry
            .commit(&runtime, &handle.transaction_id, "alice", 1_002)
            .await
            .unwrap();
        handle.transaction_id
    };

    let reopened = OpenTransactionRegistry::open(temp.path()).unwrap();
    assert_eq!(
        reopened
            .resolved_idempotency_result(&transaction_id, "alice", "bucket.create", "request-1",)
            .unwrap()
            .unwrap()
            .payload,
        b"response"
    );
}

#[tokio::test]
async fn read_only_commit_accepts_snapshot_without_consensus_entry() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let handle = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "read-only",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1_000,
        )
        .await
        .unwrap();

    let first = registry
        .commit(&runtime, &handle.transaction_id, "alice", 1_001)
        .await
        .unwrap();
    let retry = registry
        .commit(&runtime, &handle.transaction_id, "alice", 1_002)
        .await
        .unwrap();

    assert_eq!(
        first.certification,
        CertificationResult::Committed { commit_version: 9 }
    );
    assert_eq!(retry.certification, first.certification);
    assert_eq!(first.local_apply, None);
    assert_eq!(retry.local_apply, None);
    assert!(runtime.committed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cross_table_draft_freezes_one_bundle() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let handle = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "request-1",
            Duration::from_secs(30),
            DurabilityLevel::Local,
            ReadConsistency::Linearized,
            1_000,
        )
        .await
        .unwrap();
    registry
        .put(
            &handle.transaction_id,
            "cluster",
            LogicalKey {
                table_id: 1,
                application_key: b"a".to_vec(),
            },
            b"one".to_vec(),
            1_001,
        )
        .unwrap();
    registry
        .put(
            &handle.transaction_id,
            "cluster",
            LogicalKey {
                table_id: 8,
                application_key: b"b".to_vec(),
            },
            b"two".to_vec(),
            1_002,
        )
        .unwrap();
    registry
        .add_stream_event(
            &handle.transaction_id,
            crate::mvcc_outbox::StreamOutboxEvent::new(
                7,
                "events",
                "partition-7",
                "test.event",
                b"event".to_vec(),
            )
            .unwrap(),
            1_003,
        )
        .unwrap();
    let job = index_finalization_job(&handle.transaction_id);
    registry
        .add_job(&handle.transaction_id, job.clone(), 1_004)
        .unwrap();
    registry
        .commit(&runtime, &handle.transaction_id, "alice", 1_005)
        .await
        .unwrap();

    let bundles = runtime.committed.lock().unwrap();
    assert_eq!(bundles[0].snapshot_version, 9);
    assert_eq!(bundles[0].writes.len(), 2);
    assert_eq!(bundles[0].outbox_events.len(), 1);
    assert_eq!(
        crate::mvcc_outbox::StreamOutboxEvent::decode(&bundles[0].outbox_events[0])
            .unwrap()
            .payload,
        b"event"
    );
    assert_eq!(bundles[0].materialisation_jobs, [job]);
}

#[tokio::test]
async fn one_atomic_stage_spans_product_tables_and_is_retry_safe() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let handle = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "cross-feature",
            Duration::from_secs(30),
            DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            1_000,
        )
        .await
        .unwrap();
    let mutations = [0x8804, 0x8810, 0x8842, 0x8860]
        .into_iter()
        .enumerate()
        .map(|(ordinal, table_id)| StagedLogicalMutation {
            key: LogicalKey {
                table_id,
                application_key: format!("feature/{ordinal}").into_bytes(),
            },
            observed_version: (ordinal != 0).then_some(ordinal as u64),
            value: Some(format!("value/{ordinal}").into_bytes()),
        })
        .collect::<Vec<_>>();
    registry
        .stage_logical_mutations(
            &handle.transaction_id,
            "alice",
            "cluster",
            mutations.clone(),
            1_001,
        )
        .unwrap();
    registry
        .stage_logical_mutations(&handle.transaction_id, "alice", "cluster", mutations, 1_002)
        .unwrap();
    assert!(
        registry
            .stage_logical_mutations(
                &handle.transaction_id,
                "mallory",
                "cluster",
                Vec::new(),
                1_003,
            )
            .is_err()
    );

    registry
        .commit(&runtime, &handle.transaction_id, "alice", 1_004)
        .await
        .unwrap();
    let bundles = runtime.committed.lock().unwrap();
    let bundle = &bundles[0];
    assert_eq!(bundle.point_observations.len(), 4);
    assert_eq!(bundle.writes.len(), 4);
    assert_eq!(
        bundle
            .writes
            .iter()
            .map(|write| write.key().table_id)
            .collect::<Vec<_>>(),
        vec![0x8804, 0x8810, 0x8842, 0x8860],
    );
}

#[tokio::test]
async fn expiry_is_durable_and_blocks_mutation_and_commit() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let handle = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "expires",
            Duration::from_millis(5),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            100,
        )
        .await
        .unwrap();
    assert!(
        registry
            .put(
                &handle.transaction_id,
                "cluster",
                LogicalKey {
                    table_id: 1,
                    application_key: b"k".to_vec(),
                },
                vec![],
                105,
            )
            .is_err()
    );
    assert!(
        registry
            .commit(&runtime, &handle.transaction_id, "alice", 105,)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn begin_idempotency_is_principal_bound() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let first = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "same",
            Duration::from_secs(1),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            10,
        )
        .await
        .unwrap();
    let retry = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "same",
            Duration::from_secs(99),
            DurabilityLevel::Local,
            ReadConsistency::Linearized,
            500,
        )
        .await
        .unwrap();
    assert_eq!(first, retry);
    assert!(
        registry
            .begin(
                &runtime,
                "cluster",
                "mallory",
                "same",
                Duration::from_secs(1),
                DurabilityLevel::Local,
                ReadConsistency::LocalSnapshot,
                10,
            )
            .await
            .is_err()
    );
}

#[test]
fn idempotency_status_lookup_is_read_only() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    assert!(
        registry
            .status_by_idempotency("cluster", "missing", "alice", 10)
            .unwrap()
            .is_none()
    );
    assert!(registry.active_snapshot_pins(10).unwrap().is_empty());
}

#[tokio::test]
async fn committing_idempotency_retry_resumes_to_the_same_outcome() {
    let temp = tempdir().unwrap();
    let registry = Arc::new(OpenTransactionRegistry::open(temp.path()).unwrap());
    let runtime = Arc::new(GatedRuntime {
        calls: AtomicUsize::new(0),
        first_started: Notify::new(),
        release_first: Notify::new(),
    });
    let handle = registry
        .begin(
            runtime.as_ref(),
            "cluster",
            "alice",
            "resume",
            Duration::from_secs(30),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            10,
        )
        .await
        .unwrap();
    registry
        .put(
            &handle.transaction_id,
            "cluster",
            LogicalKey {
                table_id: 7,
                application_key: b"row".to_vec(),
            },
            b"value".to_vec(),
            11,
        )
        .unwrap();

    let first_registry = registry.clone();
    let first_runtime = runtime.clone();
    let transaction_id = handle.transaction_id.clone();
    let first = tokio::spawn(async move {
        first_registry
            .commit(first_runtime.as_ref(), &transaction_id, "alice", 12)
            .await
            .unwrap()
    });
    runtime.first_started.notified().await;
    assert_eq!(
        registry.active_snapshot_pins(50_000).unwrap(),
        BTreeSet::from([handle.snapshot_version]),
        "a committing transaction remains pinned after its client TTL"
    );
    assert_eq!(
        registry.prepared_bundle_transaction_pins().unwrap(),
        BTreeSet::from([handle.transaction_id.clone()])
    );
    let status = registry
        .status_by_idempotency("cluster", "resume", "alice", 12)
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "committing");

    let retry = registry
        .commit(runtime.as_ref(), &handle.transaction_id, "alice", 12)
        .await
        .unwrap();
    runtime.release_first.notify_one();
    let first = first.await.unwrap();
    assert_eq!(retry.certification, first.certification);
    assert_eq!(
        retry.certification,
        CertificationResult::Committed { commit_version: 12 }
    );
    assert!(
        registry
            .prepared_bundle_transaction_pins()
            .unwrap()
            .is_empty()
    );
    assert!(registry.active_snapshot_pins(50_000).unwrap().is_empty());
}

#[tokio::test]
async fn staging_rejects_resources_owned_by_another_cluster() {
    let temp = tempdir().unwrap();
    let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
    let runtime = runtime();
    let handle = registry
        .begin(
            &runtime,
            "cluster",
            "alice",
            "cluster-bound",
            Duration::from_secs(1),
            DurabilityLevel::Local,
            ReadConsistency::LocalSnapshot,
            10,
        )
        .await
        .unwrap();
    let error = registry
        .put(
            &handle.transaction_id,
            "foreign",
            LogicalKey {
                table_id: 1,
                application_key: b"k".to_vec(),
            },
            b"value".to_vec(),
            11,
        )
        .unwrap_err();
    assert!(error.to_string().contains("another cluster"));
}

#[tokio::test]
async fn restart_resumes_open_and_resolves_frozen_transactions() {
    let temp = tempdir().unwrap();
    let runtime = runtime();
    let transaction_id = {
        let registry = OpenTransactionRegistry::open(temp.path()).unwrap();
        let handle = registry
            .begin(
                &runtime,
                "cluster",
                "alice",
                "restart",
                Duration::from_secs(60),
                DurabilityLevel::Local,
                ReadConsistency::LocalSnapshot,
                0,
            )
            .await
            .unwrap();
        registry
            .put(
                &handle.transaction_id,
                "cluster",
                LogicalKey {
                    table_id: 2,
                    application_key: b"k".to_vec(),
                },
                b"v".to_vec(),
                1,
            )
            .unwrap();
        handle.transaction_id
    };
    let reopened = OpenTransactionRegistry::open(temp.path()).unwrap();
    assert_eq!(
        reopened.handle(&transaction_id).unwrap().snapshot_version,
        9
    );
    let first = reopened
        .commit(&runtime, &transaction_id, "alice", 2)
        .await
        .unwrap();
    assert_eq!(first.local_apply, Some(ApplyOutcome::Applied));
    drop(reopened);

    let reopened = OpenTransactionRegistry::open(temp.path()).unwrap();
    let retry = reopened
        .commit(&runtime, &transaction_id, "alice", 3)
        .await
        .unwrap();
    assert_eq!(retry.local_apply, Some(ApplyOutcome::Replayed));
    let committed = runtime.committed.lock().unwrap();
    assert_eq!(
        committed[0].identity().unwrap(),
        committed[1].identity().unwrap()
    );
}
