use std::sync::Mutex;

use super::*;

fn restrictive_limits() -> TransactionResourceLimits {
    TransactionResourceLimits {
        max_point_observations: 1,
        max_range_observations: 1,
        max_written_keys: 1,
        max_certification_command_bytes: 128,
        max_bundle_bytes: 1024,
        max_raw_payload_bytes: 1,
    }
}

#[test]
fn resource_limits_cover_conflict_sets_bundle_and_raw_payload_bytes() {
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        "limited",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    builder.put(
        LogicalKey {
            table_id: 1,
            application_key: b"key".to_vec(),
        },
        b"two".to_vec(),
    );
    let bundle = builder.build().unwrap();
    let bytes = bundle.canonical_bytes().unwrap();
    let limits = restrictive_limits();
    assert!(
        limits
            .validate_bundle(&bundle, &bytes)
            .unwrap_err()
            .to_string()
            .contains("raw payload byte limit")
    );

    let mut oversized_bundle = limits;
    oversized_bundle.max_raw_payload_bytes = usize::MAX;
    oversized_bundle.max_bundle_bytes = 1;
    assert!(
        oversized_bundle
            .validate_bundle(&bundle, &bytes)
            .unwrap_err()
            .to_string()
            .contains("canonical bundle byte limit")
    );

    let mut two_writes = TransactionBundleBuilder::new(
        "cluster",
        "two-writes",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    two_writes.put(
        LogicalKey {
            table_id: 1,
            application_key: b"a".to_vec(),
        },
        Vec::new(),
    );
    two_writes.put(
        LogicalKey {
            table_id: 1,
            application_key: b"b".to_vec(),
        },
        Vec::new(),
    );
    let two_writes = two_writes.build().unwrap();
    let two_writes_bytes = two_writes.canonical_bytes().unwrap();
    let mut write_limit = TransactionResourceLimits::default();
    write_limit.max_written_keys = 1;
    assert!(
        write_limit
            .validate_bundle(&two_writes, &two_writes_bytes)
            .unwrap_err()
            .to_string()
            .contains("written key limit")
    );

    let mut observations = TransactionBundleBuilder::new(
        "cluster",
        "observations",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    observations
        .observe_point(
            LogicalKey {
                table_id: 1,
                application_key: b"a".to_vec(),
            },
            None,
        )
        .observe_point(
            LogicalKey {
                table_id: 1,
                application_key: b"b".to_vec(),
            },
            None,
        );
    observations
        .observe_range(1, b"a".to_vec(), b"m".to_vec(), None)
        .unwrap();
    observations
        .observe_range(1, b"m".to_vec(), b"z".to_vec(), None)
        .unwrap();
    let observed = observations.build().unwrap();
    let observed_bytes = observed.canonical_bytes().unwrap();
    let mut point_limit = TransactionResourceLimits::default();
    point_limit.max_point_observations = 1;
    assert!(
        point_limit
            .validate_bundle(&observed, &observed_bytes)
            .unwrap_err()
            .to_string()
            .contains("point observation limit")
    );
    let mut range_limit = TransactionResourceLimits::default();
    range_limit.max_range_observations = 1;
    assert!(
        range_limit
            .validate_bundle(&observed, &observed_bytes)
            .unwrap_err()
            .to_string()
            .contains("range observation limit")
    );
}

#[test]
fn idempotency_results_are_canonical_and_identity_unique() {
    let mut first = TransactionBundleBuilder::new(
        "cluster",
        "idempotency",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    first
        .add_idempotency_result(IdempotencyResult {
            namespace: "service.method".into(),
            key: "b".into(),
            payload: b"second".to_vec(),
        })
        .add_idempotency_result(IdempotencyResult {
            namespace: "service.method".into(),
            key: "a".into(),
            payload: b"first".to_vec(),
        });
    let canonical = first.build().unwrap();
    assert_eq!(canonical.idempotency_results[0].key, "a");

    let mut duplicate = canonical.clone();
    duplicate.idempotency_results.push(IdempotencyResult {
        namespace: "service.method".into(),
        key: "a".into(),
        payload: b"different".to_vec(),
    });
    assert!(
        duplicate
            .canonicalize()
            .unwrap_err()
            .to_string()
            .contains("idempotency result")
    );
}

#[test]
fn canonical_bundle_rejects_missing_or_forged_ownership_claims() {
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        "ownership",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    builder.put(
        LogicalKey {
            table_id: 1,
            application_key: b"key".to_vec(),
        },
        b"value".to_vec(),
    );
    let mut missing = builder.build().unwrap();
    missing.ownership_claims.clear();
    assert!(
        missing
            .canonicalize()
            .unwrap_err()
            .to_string()
            .contains("ownership claims")
    );
}

#[test]
fn canonical_bundle_rejects_unversioned_outbox_payloads() {
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        "invalid-outbox",
        0,
        "principal",
        HierarchicalRangeStampScheme::new(),
    );
    builder.add_outbox_event(b"arbitrary event bytes".to_vec());
    assert!(
        builder
            .build()
            .unwrap_err()
            .to_string()
            .contains("decode stream outbox event")
    );
}

struct Store;

#[async_trait]
impl PreparedBundleStore for Store {
    async fn persist(
        &self,
        _identity: &BundleIdentity,
        _bytes: &[u8],
    ) -> Result<BundleDurabilityEvidence> {
        Ok(bundle_holder("a", "zone-a"))
    }
}

struct Replicator(ReplicationEvidence);

#[async_trait]
impl BundleReplicator for Replicator {
    async fn replicate(
        &self,
        _identity: &BundleIdentity,
        _bytes: &[u8],
        _objects: &[ObjectShardManifestReference],
        _durability: DurabilityLevel,
    ) -> Result<ReplicationEvidence> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
struct Certifier {
    request: Mutex<Option<CertificationRequest>>,
}

#[async_trait]
impl TransactionCertifier for Certifier {
    async fn observed_commit_version(
        &self,
        _consistency: ReadConsistency,
    ) -> Result<CommitVersion> {
        Ok(41)
    }

    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResult> {
        *self.request.lock().unwrap() = Some(request);
        Ok(CertificationResult::Committed { commit_version: 42 })
    }
}

fn bundle_holder(node_id: &str, failure_domain: &str) -> BundleDurabilityEvidence {
    BundleDurabilityEvidence {
        cluster_id: "cluster".into(),
        node: NodeIncarnation {
            node_id: node_id.to_string(),
            incarnation: 1,
        },
        failure_domain: failure_domain.to_string(),
        complete: true,
        hash_verified: true,
        fsynced: true,
    }
}

fn shard(shard_ordinal: u16, node_id: &str, failure_domain: &str) -> ObjectDurabilityEvidence {
    ObjectDurabilityEvidence::ShardPlacement {
        cluster_id: "cluster".into(),
        object_hash: test_object_hash(),
        encoding_generation: 1,
        stripe_ordinal: 0,
        shard_ordinal,
        data_shards: 2,
        parity_shards: 2,
        node: NodeIncarnation {
            node_id: node_id.to_string(),
            incarnation: 1,
        },
        failure_domain: failure_domain.to_string(),
        complete: true,
        hash_verified: true,
        fsynced: true,
    }
}

fn test_object_hash() -> String {
    format!("sha256:{}", "a".repeat(64))
}

struct RejectTableNine;

impl ClusterOwnershipResolver for RejectTableNine {
    fn validate_claim(
        &self,
        _transaction_cluster_id: &str,
        claim: &ClusterOwnershipClaim,
    ) -> Result<()> {
        if matches!(
            claim.resource(),
            OwnedResource::LogicalKey(LogicalKey { table_id: 9, .. })
        ) {
            bail!("routing resolved resource to another cluster");
        }
        Ok(())
    }
}

fn bundle(with_object: bool) -> TransactionBundle {
    let mut builder = TransactionBundleBuilder::new(
        "cluster",
        "tx-1",
        41,
        "tenant/1/principal/app",
        HierarchicalRangeStampScheme::new(),
    );
    builder.put(
        LogicalKey {
            table_id: 9,
            application_key: b"partition-b/key".to_vec(),
        },
        b"second".to_vec(),
    );
    builder.put(
        LogicalKey {
            table_id: 3,
            application_key: b"partition-a/key".to_vec(),
        },
        b"first".to_vec(),
    );
    if with_object {
        builder.add_shard_manifest(ObjectShardManifestReference {
            object_hash: test_object_hash(),
            manifest_hash: format!("sha256:{}", "b".repeat(64)),
            object_length: 1024,
            encoding_generation: 1,
            data_shards: 2,
            parity_shards: 2,
            stripe_count: 1,
        });
    }
    builder.build().unwrap()
}

#[tokio::test]
async fn one_certification_covers_unrelated_tables_and_partitions() {
    let coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: vec![bundle_holder("b", "zone-b")],
            objects: Vec::new(),
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();

    let result = coordinator
        .commit(bundle(false), DurabilityLevel::Quorum)
        .await
        .unwrap();
    assert_eq!(
        result,
        CertificationResult::Committed { commit_version: 42 }
    );
    let request = coordinator.certifier.request.lock().unwrap();
    let request = request.as_ref().unwrap();
    assert_eq!(request.written_keys.len(), 2);
    assert_eq!(request.written_keys[0].table_id, 3);
    assert_eq!(request.written_keys[1].table_id, 9);
}

#[tokio::test]
async fn routing_resolver_rejects_foreign_resource_before_preparation() {
    let coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence::default()),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 1,
            tolerated_failure_domains: 0,
        },
    )
    .unwrap()
    .with_ownership_resolver(Arc::new(RejectTableNine));

    let error = coordinator
        .commit(bundle(false), DurabilityLevel::Local)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("another cluster"));
}

#[tokio::test]
async fn consensus_is_not_called_before_quorum_durability() {
    let coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence::default()),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();

    let error = coordinator
        .commit(bundle(false), DurabilityLevel::Quorum)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("bundle durability"));
    assert!(coordinator.certifier.request.lock().unwrap().is_none());
}

#[tokio::test]
async fn quorum_requires_reconstruction_after_each_tolerated_domain_loss() {
    let unsafe_coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: vec![bundle_holder("b", "zone-b")],
            objects: vec![
                shard(0, "a", "zone-a"),
                shard(1, "b", "zone-a"),
                shard(2, "c", "zone-b"),
            ],
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();
    let error = unsafe_coordinator
        .commit(bundle(true), DurabilityLevel::Quorum)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not reconstructable"));
    assert!(
        unsafe_coordinator
            .certifier
            .request
            .lock()
            .unwrap()
            .is_none()
    );

    let safe_coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: vec![bundle_holder("b", "zone-b")],
            objects: vec![
                shard(0, "a", "zone-a"),
                shard(1, "b", "zone-b"),
                shard(2, "c", "zone-c"),
            ],
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();
    assert_eq!(
        safe_coordinator
            .commit(bundle(true), DurabilityLevel::Quorum)
            .await
            .unwrap(),
        CertificationResult::Committed { commit_version: 42 }
    );
}

#[tokio::test]
async fn erasure_requires_every_planned_shard() {
    let coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: vec![bundle_holder("b", "zone-b")],
            objects: vec![
                shard(0, "a", "zone-a"),
                shard(1, "b", "zone-b"),
                shard(2, "c", "zone-c"),
            ],
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();
    let error = coordinator
        .commit(bundle(true), DurabilityLevel::Erasure)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("complete k+m placement"));

    let complete = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: vec![bundle_holder("b", "zone-b")],
            objects: vec![
                shard(0, "a", "zone-a"),
                shard(1, "b", "zone-b"),
                shard(2, "c", "zone-c"),
                shard(3, "d", "zone-d"),
            ],
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();
    assert_eq!(
        complete
            .commit(bundle(true), DurabilityLevel::Erasure)
            .await
            .unwrap(),
        CertificationResult::Committed { commit_version: 42 }
    );
}

#[tokio::test]
async fn local_requires_verified_fsynced_local_representation() {
    let coordinator = TransactionCoordinator::new(
        Store,
        Replicator(ReplicationEvidence {
            bundle_holders: Vec::new(),
            objects: vec![ObjectDurabilityEvidence::LocalRepresentation {
                cluster_id: "cluster".into(),
                object_hash: test_object_hash(),
                node: NodeIncarnation {
                    node_id: "a".to_string(),
                    incarnation: 1,
                },
                failure_domain: "zone-a".to_string(),
                complete: true,
                hash_verified: true,
                fsynced: true,
            }],
        }),
        Certifier::default(),
        DurabilityPolicy {
            bundle_quorum_holders: 2,
            tolerated_failure_domains: 1,
        },
    )
    .unwrap();
    assert_eq!(
        coordinator
            .commit(bundle(true), DurabilityLevel::Local)
            .await
            .unwrap(),
        CertificationResult::Committed { commit_version: 42 }
    );
}

#[test]
fn canonical_identity_does_not_depend_on_input_write_order() {
    let first = bundle(false);
    let mut second = first.clone();
    second.writes.reverse();
    assert_eq!(first.identity().unwrap(), second.identity().unwrap());
}

#[test]
fn hierarchical_scan_stamp_is_advanced_by_every_overlapping_write() {
    let scheme = HierarchicalRangeStampScheme::new();
    let observed = scheme
        .observation_key(7, Some(b"orders/a"), Some(b"orders/z"))
        .unwrap();
    assert_eq!(observed.key_prefix, b"orders/".to_vec());

    let overlapping = LogicalKey {
        table_id: 7,
        application_key: b"orders/m".to_vec(),
    };
    assert!(scheme.write_keys(&overlapping).contains(&observed));

    let unrelated = LogicalKey {
        table_id: 7,
        application_key: b"profiles/m".to_vec(),
    };
    assert!(!scheme.write_keys(&unrelated).contains(&observed));
    let full_table = scheme.observation_key(7, None, None).unwrap();
    assert!(full_table.key_prefix.is_empty());
    assert!(scheme.write_keys(&unrelated).contains(&full_table));
}

#[test]
fn builder_advances_delete_and_cross_table_rename_stamps() {
    let scheme = HierarchicalRangeStampScheme::new();
    let old_key = LogicalKey {
        table_id: 3,
        application_key: b"old/key".to_vec(),
    };
    let new_key = LogicalKey {
        table_id: 9,
        application_key: b"new/key".to_vec(),
    };
    let mut builder =
        TransactionBundleBuilder::new("cluster", "rename", 10, "tenant/1/principal/app", scheme);
    builder.rename(old_key.clone(), new_key.clone(), b"value".to_vec());
    let bundle = builder.build().unwrap();

    assert_eq!(bundle.writes.len(), 2);
    assert!(
        scheme
            .write_keys(&old_key)
            .iter()
            .all(|stamp| bundle.advanced_range_stamps.contains(stamp))
    );
    assert!(
        scheme
            .write_keys(&new_key)
            .iter()
            .all(|stamp| bundle.advanced_range_stamps.contains(stamp))
    );
    assert!(bundle.advanced_range_stamps.contains(&RangeStampKey {
        scheme_version: HierarchicalRangeStampScheme::SCHEME_VERSION,
        table_id: 3,
        key_prefix: Vec::new(),
    }));
    assert!(bundle.advanced_range_stamps.contains(&RangeStampKey {
        scheme_version: HierarchicalRangeStampScheme::SCHEME_VERSION,
        table_id: 9,
        key_prefix: Vec::new(),
    }));
}
