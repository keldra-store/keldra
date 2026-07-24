use super::*;

#[tokio::test]
async fn core_store_node_signing_keypair_is_rocksdb_metadata_not_sidecar() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let first = CoreStore::new(storage.clone()).await.unwrap();
    let first_public_key = first.node_signing_keypair.public_key_bytes();
    let first_admission_epoch = first.admission_mutation_epoch;
    assert_ne!(first_admission_epoch, 0);
    assert!(
        !storage
            .core_store_root_path()
            .join("node-signing-keypair.pb")
            .exists(),
        "CoreStore node signing keypair must not be persisted as a sidecar file"
    );
    assert!(
        storage.core_store_meta_path().exists(),
        "CoreStore node signing keypair should live in the RocksDB metadata plane"
    );

    drop(first);
    let restarted = CoreStore::new(storage).await.unwrap();
    assert_eq!(
        first_public_key,
        restarted.node_signing_keypair.public_key_bytes()
    );
    assert_eq!(first_admission_epoch, restarted.admission_mutation_epoch);
}

#[tokio::test]
async fn fresh_node_storage_has_a_distinct_admission_incarnation() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first = CoreStore::new(Storage::new_at(first_dir.path()).await.unwrap())
        .await
        .unwrap();
    let second = CoreStore::new(Storage::new_at(second_dir.path()).await.unwrap())
        .await
        .unwrap();

    assert_eq!(first.node_identity.node_id, second.node_identity.node_id);
    assert_ne!(
        first.admission_mutation_epoch, second.admission_mutation_epoch,
        "a fresh local store must not reuse admission identities from an older node incarnation"
    );

    let admitted = first
        .admit_core_mutation(
            "test.identity",
            WriterFamily::CoreControl.as_str(),
            test_mutation_target(),
            "node-scoped-admission".to_string(),
            Some("node-scoped-admission".to_string()),
            CorePendingMutationPayload::Inline(b"node-scoped-admission"),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(admitted.node_id, first.node_identity.node_id);
    assert_eq!(admitted.mutation_epoch, first.admission_mutation_epoch);
}

#[tokio::test]
async fn unchanged_receipt_signing_key_registration_is_a_storage_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let node_id = "receipt-node-a";
    let keypair = crate::node_signing::NodeSigningKeypair::generate().unwrap();
    let public_key = keypair.public_key_bytes().to_vec();

    store
        .register_node_receipt_signing_public_key(node_id, &public_key)
        .unwrap();
    let tuple_key = node_receipt_signing_public_key_key(node_id);
    let first = store
        .meta
        .get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &tuple_key)
        .unwrap()
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(2));
    store
        .register_node_receipt_signing_public_key(node_id, &public_key)
        .unwrap();
    let second = store
        .meta
        .get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &tuple_key)
        .unwrap()
        .unwrap();

    assert_eq!(
        first, second,
        "registering an unchanged key must not rewrite its RocksDB row"
    );
}

#[tokio::test]
async fn receipt_signing_identity_replacement_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let node_id = "receipt-node-immutable";
    let first_public_key = crate::node_signing::NodeSigningKeypair::generate()
        .unwrap()
        .public_key_bytes()
        .to_vec();
    let replacement_public_key = crate::node_signing::NodeSigningKeypair::generate()
        .unwrap()
        .public_key_bytes()
        .to_vec();

    store
        .register_node_receipt_signing_public_key(node_id, &first_public_key)
        .unwrap();
    let error = store
        .register_node_receipt_signing_public_key(node_id, &replacement_public_key)
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("receipt signing identity replacement rejected")
    );
    assert_eq!(
        load_node_receipt_signing_public_key(&store.meta, node_id)
            .unwrap()
            .unwrap()
            .to_bytes()
            .to_vec(),
        first_public_key,
        "rejected registration must preserve the original verification identity"
    );
}

#[tokio::test]
async fn receipt_verification_reports_topology_unavailable_before_bootstrap() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreStore::new(Storage::new_at(tmp.path()).await.unwrap())
        .await
        .unwrap();
    let remote = crate::node_signing::NodeSigningKeypair::generate().unwrap();
    let payload_hash = "sha256:bootstrap-race";
    let signature = remote.sign(payload_hash.as_bytes());

    let error = store
        .verify_internal_core_receipt_signature("node-b", payload_hash, &signature)
        .unwrap_err();

    assert!(is_core_store_unavailable(&error));
    assert!(
        error
            .to_string()
            .contains("mesh topology is not ready to verify node node-b")
    );
}

#[tokio::test]
async fn receipt_verification_materialises_canonical_bootstrap_key_on_demand() {
    use crate::mesh_lifecycle::{
        BootstrapMeshLifecycleProjection, CreateRegionDescriptor, NodeCapability,
        RegisterCellDescriptor, RegisterNodeDescriptor, install_bootstrap_lifecycle_projection,
    };

    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let remote = crate::node_signing::NodeSigningKeypair::generate().unwrap();
    let remote_public_key = remote.public_key_bytes().to_vec();

    install_bootstrap_lifecycle_projection(
        &storage,
        &store,
        BootstrapMeshLifecycleProjection {
            regions: vec![CreateRegionDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil.test".to_string(),
                placement_weight: 100,
                default_cell: Some("cell-a".to_string()),
            }],
            cells: vec![RegisterCellDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
                failure_domain: "rack-a".to_string(),
            }],
            nodes: vec![RegisterNodeDescriptor {
                mesh_id: "mesh-a".to_string(),
                node_id: "node-b".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                receipt_signing_public_key: remote_public_key,
                public_api_addr: "http://127.0.0.1:50052".to_string(),
                capabilities: vec![NodeCapability::Metadata, NodeCapability::Object],
                capacity_json: "{}".to_string(),
            }],
        },
    )
    .unwrap();

    assert!(
        load_node_receipt_signing_public_key(&store.meta, "node-b")
            .unwrap()
            .is_none(),
        "bootstrap projection should remain the canonical source until first verification"
    );

    let payload_hash = "sha256:canonical-bootstrap-key";
    let signature = remote.sign(payload_hash.as_bytes());
    store
        .verify_internal_core_receipt_signature("node-b", payload_hash, &signature)
        .unwrap();

    assert!(
        load_node_receipt_signing_public_key(&store.meta, "node-b")
            .unwrap()
            .is_some(),
        "successful verification should materialise the canonical lifecycle key"
    );

    let unknown = store
        .verify_internal_core_receipt_signature("node-c", payload_hash, &signature)
        .unwrap_err();
    assert!(!is_core_store_unavailable(&unknown));
    assert!(unknown.to_string().contains("unknown node node-c"));
}

#[tokio::test]
async fn coremeta_quorum_commits_independent_roots_as_one_group() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let first_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("group-first")]).unwrap();
    let second_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("group-second")]).unwrap();
    let first_payload = encode_core_meta_inline_payload_row(
        b"first",
        core_meta_committed_row_common(
            "test/group",
            core_meta_root_key_hash("test/group/first"),
            1,
            "grouped-root-commit",
            1,
        ),
    )
    .unwrap();
    let second_payload = encode_core_meta_inline_payload_row(
        b"second",
        core_meta_committed_row_common(
            "test/group",
            core_meta_root_key_hash("test/group/second"),
            1,
            "grouped-root-commit",
            1,
        ),
    )
    .unwrap();
    let operations = [
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &first_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&first_payload),
        },
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &second_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&second_payload),
        },
    ];
    let publications = [
        CoreMetaRootPublication::new("test/group/first", WriterFamily::CoreControl).coordinator(),
        CoreMetaRootPublication::new("test/group/second", WriterFamily::CoreControl),
    ];

    let outcomes = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        store.commit_coremeta_root_groups("grouped-root-commit", &operations, &publications),
    )
    .await
    .expect("multi-root quorum publication must not retain speculative CAS locks")
    .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert_ne!(outcomes[0].root_key_hash, outcomes[1].root_key_hash);
    assert!(outcomes.iter().all(|outcome| {
        outcome.post_root_generation == 1
            && !outcome.certificate_hash.is_empty()
            && outcome.certificate_persist_receipt_hashes.len()
                >= crate::core_store::CORE_META_DEFAULT_QUORUM
    }));
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &first_key)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &second_key)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn coremeta_quorum_centrally_binds_one_generation_for_one_logical_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_key_hash = core_meta_root_key_hash("test/group/shared");
    let first_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("shared-first")]).unwrap();
    let second_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("shared-second")]).unwrap();
    let first_payload = encode_core_meta_inline_payload_row(
        b"first",
        core_meta_committed_row_common(
            "test/group",
            root_key_hash.clone(),
            1,
            "successive-root-commit",
            1,
        ),
    )
    .unwrap();
    let second_payload = encode_core_meta_inline_payload_row(
        b"second",
        core_meta_committed_row_common(
            "test/group",
            root_key_hash.clone(),
            2,
            "successive-root-commit",
            2,
        ),
    )
    .unwrap();
    let operations = [
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &first_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&first_payload),
        },
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &second_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&second_payload),
        },
    ];
    let publications =
        [
            CoreMetaRootPublication::new("test/group/shared", WriterFamily::CoreControl)
                .coordinator(),
        ];

    let outcomes = store
        .commit_coremeta_root_groups("successive-root-commit", &operations, &publications)
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].post_root_generation, 1);
    for tuple_key in [&first_key, &second_key] {
        let payload = store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, tuple_key)
            .unwrap()
            .expect("centrally bound row is visible");
        let common = core_meta_row_common_from_payload(&payload).unwrap();
        assert_eq!(common.root_key_hash, root_key_hash);
        assert_eq!(common.root_generation, 1);
        assert_eq!(common.transaction_id, "successive-root-commit");
    }
}

#[tokio::test]
async fn coremeta_quorum_batches_one_root_generation_into_one_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_key_hash = core_meta_root_key_hash("test/group/one-generation");
    let first_key =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("one-generation-first")]).unwrap();
    let second_key =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("one-generation-second")]).unwrap();
    let common = core_meta_committed_row_common(
        "test/group",
        root_key_hash.clone(),
        1,
        "one-generation-commit",
        1,
    );
    let first_payload = encode_core_meta_inline_payload_row(b"first", common.clone()).unwrap();
    let second_payload = encode_core_meta_inline_payload_row(b"second", common).unwrap();
    let operations = [
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &first_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&first_payload),
        },
        CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &second_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&second_payload),
        },
    ];
    let publications =
        [
            CoreMetaRootPublication::new("test/group/one-generation", WriterFamily::CoreControl)
                .coordinator(),
        ];

    let outcomes = store
        .commit_coremeta_root_groups("one-generation-commit", &operations, &publications)
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].root_key_hash, root_key_hash);
    assert_eq!(outcomes[0].post_root_generation, 1);
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &first_key)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &second_key)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn direct_and_admitted_writers_publish_contiguous_root_generations() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_anchor_key = "test/group/concurrent-plan";
    let root_hash = core_meta_root_key_hash(root_anchor_key);
    let direct_key = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("concurrent-direct")]).unwrap();
    let admitted_key =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("concurrent-admitted")]).unwrap();
    let direct_payload = encode_core_meta_inline_payload_row(
        b"direct",
        core_meta_committed_row_common(
            "test/group",
            root_hash.clone(),
            91,
            "caller-placeholder",
            1,
        ),
    )
    .unwrap();
    let admitted_payload = encode_core_meta_inline_payload_row(
        b"admitted",
        core_meta_committed_row_common(
            "test/group",
            root_hash.clone(),
            47,
            "caller-placeholder",
            2,
        ),
    )
    .unwrap();

    let direct_store = store.clone();
    let direct = async move {
        let operations = [CoreMetaBatchOp {
            cf: CF_INLINE_PAYLOADS,
            table_id: TABLE_INLINE_PAYLOAD_ROW,
            tuple_key: &direct_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&direct_payload),
        }];
        direct_store
            .commit_coremeta_root_groups(
                "concurrent-direct-commit",
                &operations,
                &[
                    CoreMetaRootPublication::new(root_anchor_key, WriterFamily::CoreControl)
                        .coordinator(),
                ],
            )
            .await
            .map(|outcomes| (direct_key, outcomes))
    };
    let admitted_store = store.clone();
    let admitted = async move {
        admitted_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "concurrent-admitted-commit".to_string(),
                scope_partition: root_anchor_key.to_string(),
                committed_by_principal: "principal:concurrent-writer".to_string(),
                root_publications: vec![
                    CoreMutationRootPublication::new(
                        root_anchor_key,
                        WriterFamily::CoreControl.as_str(),
                    )
                    .coordinator(),
                ],
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::CoreMetaPut {
                    partition_id: root_anchor_key.to_string(),
                    cf: CF_INLINE_PAYLOADS.to_string(),
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key: admitted_key.clone(),
                    payload: admitted_payload,
                }],
            })
            .await
            .map(|receipt| (admitted_key, receipt))
    };

    let ((direct_key, direct_outcomes), (admitted_key, admitted_receipt)) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (direct, admitted) = tokio::join!(direct, admitted);
            (direct.unwrap(), admitted.unwrap())
        })
        .await
        .expect("same-root writers must serialize without deadlocking");
    assert_eq!(direct_outcomes.len(), 1);
    assert_eq!(admitted_receipt.state, CoreTransactionState::Committed);

    let anchor = store
        .read_latest_root_anchor(root_anchor_key)
        .await
        .unwrap()
        .expect("both root generations are published");
    assert_eq!(anchor.root_generation, 2);
    let mut published = [
        (direct_key, "concurrent-direct-commit"),
        (admitted_key, "concurrent-admitted-commit"),
    ]
    .into_iter()
    .map(|(tuple_key, transaction_id)| {
        let payload = store
            .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
            .unwrap()
            .expect("concurrent row is visible");
        let common = core_meta_row_common_from_payload(&payload).unwrap();
        assert_eq!(common.root_key_hash, root_hash);
        assert_eq!(common.transaction_id, transaction_id);
        common.root_generation
    })
    .collect::<Vec<_>>();
    published.sort_unstable();
    assert_eq!(published, vec![1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_same_root_writers_reserve_successor_generations_serially() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage).await.unwrap());
    let root_anchor_key = "test/group/admitted-successors";
    let root_hash = core_meta_root_key_hash(root_anchor_key);
    let make_batch = |ordinal: u64| {
        let transaction_id = format!("admitted-successor-{ordinal}");
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("admitted-successor"),
            CoreMetaTuplePart::U64(ordinal),
        ])
        .unwrap();
        let payload = encode_core_meta_inline_payload_row(
            format!("value-{ordinal}").as_bytes(),
            core_meta_committed_row_common(
                "test/group",
                root_hash.clone(),
                0,
                transaction_id.clone(),
                ordinal,
            ),
        )
        .unwrap();
        (
            tuple_key.clone(),
            CoreMutationBatch {
                transaction_id,
                scope_partition: root_anchor_key.to_string(),
                committed_by_principal: "principal:successor-writer".to_string(),
                root_publications: vec![
                    CoreMutationRootPublication::new(
                        root_anchor_key,
                        WriterFamily::CoreControl.as_str(),
                    )
                    .coordinator(),
                ],
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::CoreMetaPut {
                    partition_id: root_anchor_key.to_string(),
                    cf: CF_INLINE_PAYLOADS.to_string(),
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key,
                    payload,
                }],
            },
        )
    };
    let (first_key, first_batch) = make_batch(1);
    let first_transaction_id = first_batch.transaction_id.clone();
    let pause =
        super::super::local_root_publication_test_control::pause_publication(&first_transaction_id);
    let first_store = Arc::clone(&store);
    let first = tokio::spawn(async move { first_store.commit_mutation_batch(first_batch).await });
    tokio::time::timeout(Duration::from_secs(10), pause.wait_until_reached())
        .await
        .expect("first publication did not reach its coordinator pause");

    let (second_key, second_batch) = make_batch(2);
    let second_store = Arc::clone(&store);
    let second =
        tokio::spawn(async move { second_store.commit_mutation_batch(second_batch).await });
    tokio::task::yield_now().await;
    pause.release();

    let (first_receipt, second_receipt) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(first, second)
    })
    .await
    .expect("same-root admitted writers did not complete");
    assert_eq!(
        first_receipt.unwrap().unwrap().state,
        CoreTransactionState::Committed
    );
    assert_eq!(
        second_receipt.unwrap().unwrap().state,
        CoreTransactionState::Committed
    );

    let anchor = store
        .read_latest_root_anchor(root_anchor_key)
        .await
        .unwrap()
        .expect("both successor generations are published");
    assert_eq!(anchor.root_generation, 2);
    let generations = [first_key, second_key]
        .into_iter()
        .map(|tuple_key| {
            let payload = store
                .read_coremeta_row(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &tuple_key)
                .unwrap()
                .expect("admitted successor row is visible");
            core_meta_row_common_from_payload(&payload)
                .unwrap()
                .root_generation
        })
        .collect::<Vec<_>>();
    assert_eq!(generations, vec![1, 2]);
}

#[tokio::test]
async fn core_store_put_get_blob_verifies_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:a".to_string(),
            bytes: b"hello corestore".to_vec(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "mut-1".to_string(),
        })
        .await
        .unwrap();
    assert!(
        read_test_pending_mutation_records(&store).await.is_empty(),
        "finalised put_blob records must be checkpointed out of RocksDB admission metadata"
    );
    assert_eq!(
        store.admission_landed_bytes().await.unwrap(),
        0,
        "finalised put_blob landed bytes must be reclaimed after CoreStore shards are durable"
    );
    let bytes = store.get_blob(GetBlob { object_ref }).await.unwrap();
    assert_eq!(bytes, b"hello corestore");
}

#[tokio::test]
async fn core_store_logical_file_aes_gcm_siv_round_trips_without_plaintext_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let keyring = CorePipelineKeyring::from_hex_config(
        "k1",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "",
    )
    .unwrap();
    let store = CoreStore::new_with_pipeline_keyring(storage.clone(), keyring)
        .await
        .unwrap();
    let source = b"alpha tenant boundary data; beta tenant boundary data; gamma".repeat(96);
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "object_blob".to_string(),
            generation: 1,
            logical_file_id: "lf_encrypted_object".to_string(),
            source: source.clone(),
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "encrypted-middle".to_string(),
                byte_start: 1024,
                byte_end: 2048,
                writer_record_kind: "object_chunk".to_string(),
                boundary_values: Vec::new(),
                writer_statistics: Vec::new(),
                preferred_block_boundary: "required".to_string(),
                boundary_dimension_ids: Vec::new(),
                prefetch_next_range_ids: Vec::new(),
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy {
                encryption: "aes_gcm_siv".to_string(),
                target_block_size: 1024,
                ..CorePipelinePolicy::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "mut-encrypted-logical-file".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(manifest.encryption.algorithm, "aes_gcm_siv");
    assert!(manifest.blocks.len() > 1);
    assert!(
        manifest
            .blocks
            .iter()
            .all(|block| block.encryption.algorithm == "aes_gcm_siv")
    );
    assert!(
        manifest
            .blocks
            .iter()
            .all(|block| block.encrypted_length > block.compressed_length)
    );

    let first_block = &manifest.blocks[0];
    let first_object_ref =
        object_ref_from_logical_block_ref(first_block, &manifest.erasure_profile_id).unwrap();
    let stored = store
        .get_blob(GetBlob {
            object_ref: first_object_ref,
        })
        .await
        .unwrap();
    assert_ne!(
        &stored[..first_block.compressed_length as usize],
        &source[..first_block.compressed_length as usize]
    );

    let whole = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest: manifest.clone(),
            ranges: vec![CoreByteRange {
                start: 0,
                end_exclusive: source.len() as u64,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(whole, source);

    let slice = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest,
            ranges: vec![CoreByteRange {
                start: 7,
                end_exclusive: 53,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(slice, source[7..53]);
}

#[tokio::test]
async fn core_store_logical_file_aes_gcm_siv_requires_keyring() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let err = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "object_blob".to_string(),
            generation: 1,
            logical_file_id: "lf_encryption_requires_key".to_string(),
            source: b"secret".to_vec(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy {
                encryption: "aes_gcm_siv".to_string(),
                ..CorePipelinePolicy::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "mut-encryption-requires-key".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("requires a configured keyring"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn core_store_range_read_does_not_require_unrelated_data_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = [b"aaaabbbbccccdddd".as_slice(), &vec![0x5a; 80 * 1024]].concat();
    let object_ref = store
        .put_blob_with_profile_and_encoding(
            PutBlob {
                logical_name: "tenant:t/bucket:b/object:range".to_string(),
                bytes: payload.clone(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "mut-range-1".to_string(),
            },
            LOCAL_EC_4_2_PROFILE,
            "none",
            WriterFamily::ObjectBlob.as_str(),
        )
        .await
        .unwrap();
    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    for placement in manifest.placements.iter().filter(|placement| {
        placement.shard_index > 0 && placement.shard_index < LOCAL_DATA_SHARDS as u16
    }) {
        let shard_path = store.shard_path(
            &placement.node_id,
            &manifest.encoding.block_id,
            placement.shard_index,
        );
        fs::write(&shard_path, vec![0xee; placement.stored_size as usize])
            .await
            .unwrap();
    }

    let range = store
        .get_blob_range(GetBlobRange {
            object_ref: object_ref.clone(),
            range: CoreByteRange {
                start: 1,
                end_exclusive: 3,
            },
        })
        .await
        .unwrap();
    assert_eq!(range, b"aa");
    assert!(
        store.get_blob(GetBlob { object_ref }).await.is_err(),
        "a full read must fail after unrelated data shards are corrupted; the range read above proves it did not materialise the full object"
    );
}

#[tokio::test]
async fn core_store_logical_file_api_writes_verifies_and_reads_ranges() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = b"alpha beta gamma delta epsilon zeta".to_vec();
    let boundary = CoreBoundaryValue {
        schema_generation: 7,
        name: "customer_tenant".to_string(),
        value_type: "string".to_string(),
        value: "tenant-a".to_string(),
        categories: vec!["query_pruning".to_string()],
        source_kind: "user_metadata".to_string(),
        required: true,
        max_values_per_block: 1,
        placement_affinity: "none".to_string(),
        compaction_scope: "none".to_string(),
        shared_ranges_allowed: false,
        shared_record_kinds: Vec::new(),
    };
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "full_text".to_string(),
            generation: 3,
            logical_file_id: "index/full-text/main/segment-3".to_string(),
            source: payload.clone(),
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "postings-a".to_string(),
                byte_start: 6,
                byte_end: 16,
                writer_record_kind: "postings".to_string(),
                boundary_values: vec![boundary.clone()],
                writer_statistics: Vec::new(),
                preferred_block_boundary: "preferred".to_string(),
                boundary_dimension_ids: vec![1],
                prefetch_next_range_ids: vec!["postings-b".to_string()],
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: vec![boundary.clone()],
            mutation_id: "logical-file-api-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(manifest.schema, CORE_LOGICAL_FILE_MANIFEST_SCHEMA);
    assert_eq!(manifest.writer_family, "full_text");
    assert_eq!(manifest.writer_generation, 3);
    assert_eq!(manifest.boundary_schema_generation, 7);
    assert_eq!(manifest.blocks.len(), 3);
    assert_eq!(manifest.ranges[0].preferred_block_boundary, "preferred");
    assert_eq!(manifest.ranges[0].boundary_dimension_ids, vec![1]);
    assert_eq!(
        manifest.blocks[0].shards.len(),
        LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
    );

    let report = store.verify_logical_file_manifest(&manifest).await.unwrap();
    assert!(report.verified);
    assert_eq!(report.checked_blocks, 3);
    assert_eq!(
        report.checked_shards,
        (3 * (LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS)) as u64
    );

    let slice = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest,
            ranges: vec![CoreByteRange {
                start: 6,
                end_exclusive: 16,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: Some(vec![boundary]),
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(slice, payload[6..16].to_vec());
}

#[tokio::test]
async fn core_store_logical_file_publish_returns_self_contained_manifest_locator() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let write = store
        .write_logical_file_with_locator(WriteLogicalFileRequest {
            writer_family: "object_blob".to_string(),
            generation: 9,
            logical_file_id: "objects/reports/report-9".to_string(),
            source: b"manifest locator payload".to_vec(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-locator-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(
        write.locator.manifest_ref.logical_file_id,
        write.manifest.logical_file_id
    );
    assert_eq!(
        write.locator.manifest_ref.writer_family,
        write.manifest.writer_family
    );
    assert_eq!(
        write.locator.manifest_ref.writer_generation,
        write.manifest.writer_generation
    );
    assert_eq!(
        write.locator.manifest_hash,
        write.locator.manifest_ref.manifest_hash
    );
    assert_eq!(write.locator.manifest_encoding, "deterministic-protobuf");
    assert_eq!(write.locator.block_locators.len(), 1);
    let block = &write.locator.block_locators[0];
    assert_eq!(block.logical_start, 0);
    assert_eq!(block.logical_end, write.locator.manifest_length);
    assert_eq!(block.block_plain_hash, write.locator.manifest_hash);
    assert_eq!(
        block.shard_receipts.len(),
        LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
    );
    for receipt in &block.shard_receipts {
        assert_ne!(receipt.written_at_unix_nanos, 0);
        assert!(receipt.signed_payload_hash.starts_with("sha256:"));
        assert_eq!(receipt.signature_algorithm, "ed25519");
        assert!(!receipt.receipt_signature.is_empty());
    }
    assert_ne!(
        block.block_id, write.manifest.blocks[0].block_id,
        "manifest locator must point at the published manifest bytes, not the data block"
    );
    let manifest_object_ref = object_ref_from_manifest_block_locator(block).unwrap();
    let manifest_bytes = store
        .get_blob(GetBlob {
            object_ref: manifest_object_ref,
        })
        .await
        .unwrap();
    assert!(decode_logical_file_manifest_proto(&manifest_bytes).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(&manifest_bytes).is_err());

    let stored_manifest = store
        .read_logical_file_manifest(&write.locator)
        .await
        .unwrap();
    assert_eq!(stored_manifest, write.manifest);
}

#[tokio::test]
async fn core_store_manifest_locator_rejects_invalid_shard_receipts() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let write = store
        .write_logical_file_with_locator(WriteLogicalFileRequest {
            writer_family: "object_blob".to_string(),
            generation: 10,
            logical_file_id: "objects/reports/report-10".to_string(),
            source: b"manifest locator receipt validation".to_vec(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-locator-mut-10".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    let mut stale_epoch = write.locator.clone();
    stale_epoch.block_locators[0].placement_epoch = 0;
    assert!(
        store
            .read_logical_file_manifest(&stale_epoch)
            .await
            .is_err()
    );

    let mut bad_codec = write.locator.clone();
    bad_codec.block_locators[0].codec_id = "wrong-codec".to_string();
    assert!(
        store
            .read_logical_file_manifest(&bad_codec)
            .await
            .unwrap_err()
            .to_string()
            .contains("codec id")
    );

    let mut missing_fsync = write.locator.clone();
    missing_fsync.block_locators[0].shard_receipts[0].fsync_sequence = 0;
    assert!(
        store
            .read_logical_file_manifest(&missing_fsync)
            .await
            .is_err()
    );

    let mut bad_hash = write.locator.clone();
    bad_hash.block_locators[0].shard_receipts[0].shard_hash =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
    assert!(store.read_logical_file_manifest(&bad_hash).await.is_err());

    let mut bad_signature = write.locator.clone();
    bad_signature.block_locators[0].shard_receipts[0].receipt_signature[0] ^= 0x01;
    assert!(
        store
            .read_logical_file_manifest(&bad_signature)
            .await
            .unwrap_err()
            .to_string()
            .contains("signature verification failed")
    );

    let mut wrong_node = write.locator.clone();
    wrong_node.block_locators[0].shard_receipts[0].node_id = "local-node-999".to_string();
    assert!(
        store
            .read_logical_file_manifest(&wrong_node)
            .await
            .unwrap_err()
            .to_string()
            .contains("placement mismatch")
    );

    let mut duplicate = write.locator.clone();
    duplicate.block_locators[0].shard_receipts[1].shard_index =
        duplicate.block_locators[0].shard_receipts[0].shard_index;
    assert!(store.read_logical_file_manifest(&duplicate).await.is_err());
}

#[tokio::test]
async fn core_store_manifest_locator_reads_multiple_contiguous_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let write = store
        .write_logical_file_with_locator(WriteLogicalFileRequest {
            writer_family: "object_blob".to_string(),
            generation: 11,
            logical_file_id: "objects/reports/report-11".to_string(),
            source: b"manifest locator split block proof".to_vec(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-locator-mut-11".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();
    let manifest_bytes = encode_logical_file_manifest_bytes(&write.manifest).unwrap();
    let split_at = manifest_bytes.len() / 2;
    let chunks = [&manifest_bytes[..split_at], &manifest_bytes[split_at..]];
    let profile = local_erasure_profile(LOCAL_ERASURE_PROFILE_ID).unwrap();
    let mut block_locators = Vec::new();
    let mut logical_start = 0u64;
    for (index, chunk) in chunks.iter().enumerate() {
        let chunk_hash = format!("sha256:{}", sha256_hex(chunk));
        let chunk_hash_hex = strip_sha256_prefix(&chunk_hash).unwrap();
        let object_ref = store
            .materialise_object_blob_bytes(
                &canonical_logical_file_id(
                    WriterFamily::CoreControl,
                    write.manifest.writer_generation,
                    &format!("manifest_split_{index}"),
                    chunk_hash.as_bytes(),
                ),
                write.manifest.writer_generation,
                index as u64,
                logical_start,
                &chunk_hash,
                chunk_hash_hex,
                chunk,
                &chunk_hash,
                chunk.len() as u64,
                none_compression_descriptor(chunk),
                &[],
                &format!("manifest_split_{index}"),
                profile,
                "none",
                "core_control",
            )
            .await
            .unwrap();
        let mut block =
            block_locator_from_manifest_object_ref(&write.manifest, &object_ref, &chunk_hash)
                .unwrap();
        block.logical_start = logical_start;
        block.logical_end = logical_start + chunk.len() as u64;
        logical_start = block.logical_end;
        block_locators.push(block);
    }
    let split_locator = CoreManifestLocator {
        manifest_ref: write.locator.manifest_ref.clone(),
        manifest_encoding: write.locator.manifest_encoding.clone(),
        manifest_length: manifest_bytes.len() as u64,
        manifest_hash: write.locator.manifest_hash.clone(),
        block_locators,
    };

    let manifest = store
        .read_logical_file_manifest(&split_locator)
        .await
        .unwrap();
    assert_eq!(manifest, write.manifest);

    let mut gap = split_locator.clone();
    gap.block_locators[1].logical_start += 1;
    assert!(store.read_logical_file_manifest(&gap).await.is_err());
}

#[tokio::test]
async fn core_store_logical_file_pipeline_splits_blocks_and_reads_cross_block_ranges() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = (0..96).map(|value| value as u8).collect::<Vec<_>>();
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "typed_index".to_string(),
            generation: 4,
            logical_file_id: "index/typed/split/segment-4".to_string(),
            source: payload.clone(),
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "cross-block-window".to_string(),
                byte_start: 24,
                byte_end: 72,
                writer_record_kind: "typed_column_page".to_string(),
                boundary_values: Vec::new(),
                writer_statistics: Vec::new(),
                preferred_block_boundary: "preferred".to_string(),
                boundary_dimension_ids: Vec::new(),
                prefetch_next_range_ids: Vec::new(),
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy {
                target_block_size: 32,
                ..Default::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-split-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(manifest.blocks.len(), 3);
    assert_eq!(
        manifest
            .blocks
            .iter()
            .map(|block| (block.logical_offset, block.logical_length))
            .collect::<Vec<_>>(),
        vec![(0, 24), (24, 48), (72, 24)]
    );
    assert_eq!(manifest.ranges[0].block_ids.len(), 1);

    let slice = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest: manifest.clone(),
            ranges: vec![CoreByteRange {
                start: 24,
                end_exclusive: 72,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(slice, payload[24..72].to_vec());
    store.verify_logical_file_manifest(&manifest).await.unwrap();
}

#[tokio::test]
async fn core_store_logical_file_pipeline_honours_required_writer_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = (0..96).map(|value| value as u8).collect::<Vec<_>>();
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "stream".to_string(),
            generation: 2,
            logical_file_id: "streams/required-boundary/segment-2".to_string(),
            source: payload,
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "record-frame-1".to_string(),
                byte_start: 24,
                byte_end: 72,
                writer_record_kind: "record_frame".to_string(),
                boundary_values: Vec::new(),
                writer_statistics: Vec::new(),
                preferred_block_boundary: "required".to_string(),
                boundary_dimension_ids: Vec::new(),
                prefetch_next_range_ids: Vec::new(),
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy {
                target_block_size: 64,
                ..Default::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-required-boundary-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(
        manifest
            .blocks
            .iter()
            .map(|block| (block.logical_offset, block.logical_length))
            .collect::<Vec<_>>(),
        vec![(0, 24), (24, 48), (72, 24)]
    );
    assert_eq!(manifest.ranges[0].block_ids.len(), 1);
}

#[tokio::test]
async fn core_store_logical_range_read_does_not_materialise_unrelated_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = (0..4096)
        .map(|value| (value % 251) as u8)
        .collect::<Vec<_>>();
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "vector".to_string(),
            generation: 5,
            logical_file_id: "index/vector/range/segment-5".to_string(),
            source: payload.clone(),
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "range-read-first-block".to_string(),
                byte_start: 1024,
                byte_end: 2048,
                writer_record_kind: "vector_page".to_string(),
                boundary_values: Vec::new(),
                writer_statistics: Vec::new(),
                preferred_block_boundary: "required".to_string(),
                boundary_dimension_ids: Vec::new(),
                prefetch_next_range_ids: Vec::new(),
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy {
                target_block_size: 1024,
                ..Default::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-range-only-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    let unrelated = manifest
        .blocks
        .iter()
        .find(|block| block.logical_offset >= 2048)
        .unwrap();
    let unrelated_ref =
        object_ref_from_logical_block_ref(unrelated, &manifest.erasure_profile_id).unwrap();
    for placement in unrelated_ref
        .placements
        .iter()
        .filter(|placement| placement.shard_index < 3)
    {
        let shard_path = store.shard_path(
            &placement.node_id,
            &unrelated_ref.encoding.block_id,
            placement.shard_index,
        );
        fs::write(&shard_path, vec![0xee; placement.stored_size as usize])
            .await
            .unwrap();
    }

    let slice = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest: manifest.clone(),
            ranges: vec![CoreByteRange {
                start: 0,
                end_exclusive: 16,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(slice, payload[0..16].to_vec());
    assert!(
        store.verify_logical_file_manifest(&manifest).await.is_err(),
        "full verification must fail after corrupting a block not needed by the range read"
    );
}

#[tokio::test]
async fn core_store_logical_file_api_supports_zstd_compression() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let payload = b"alpha alpha alpha alpha beta beta beta beta gamma gamma gamma gamma".repeat(64);
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "full_text".to_string(),
            generation: 9,
            logical_file_id: "index/full-text/compressed/segment-9".to_string(),
            source: payload.clone(),
            range_hints: vec![CoreLogicalRangeHint {
                range_id: "beta-window".to_string(),
                byte_start: 12,
                byte_end: 32,
                writer_record_kind: "postings".to_string(),
                boundary_values: Vec::new(),
                writer_statistics: Vec::new(),
                preferred_block_boundary: "preferred".to_string(),
                boundary_dimension_ids: Vec::new(),
                prefetch_next_range_ids: Vec::new(),
                shared_range: None,
            }],
            pipeline_policy: CorePipelinePolicy {
                compression: "zstd".to_string(),
                ..Default::default()
            },
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: "logical-file-zstd-mut-1".to_string(),
            region_id: "local".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(manifest.logical_size, payload.len() as u64);
    assert_eq!(
        manifest.content_hash,
        format!("sha256:{}", sha256_hex(&payload))
    );
    assert_eq!(manifest.compression.algorithm, "zstd");
    assert_eq!(
        manifest.compression.uncompressed_length,
        payload.len() as u64
    );
    assert!(manifest.compression.compressed_length < payload.len() as u64);
    assert_eq!(
        manifest
            .blocks
            .iter()
            .map(|block| block.logical_length)
            .sum::<u64>(),
        payload.len() as u64
    );
    assert!(
        manifest
            .blocks
            .iter()
            .any(|block| block.compressed_length < block.logical_length)
    );
    assert_ne!(manifest.blocks[0].block_encoded_hash, manifest.content_hash);

    store.verify_logical_file_manifest(&manifest).await.unwrap();
    let slice = store
        .read_logical_range(ReadLogicalRangeRequest {
            manifest,
            ranges: vec![CoreByteRange {
                start: 12,
                end_exclusive: 32,
            }],
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: "local".to_string(),
                authz_realm_id: "system".to_string(),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
        .unwrap();
    assert_eq!(slice, payload[12..32].to_vec());
}

#[test]
fn core_store_erasure_codec_matches_rfc_golden_vectors() {
    let ec_4_2_payload =
        hex::decode(concat!("00010203", "10111213", "20212223", "30313233")).unwrap();
    let ec_4_2 = encode_erasure_shards(&ec_4_2_payload, LOCAL_EC_4_2_PROFILE).unwrap();
    assert_eq!(hex::encode(&ec_4_2[0]), "00010203");
    assert_eq!(hex::encode(&ec_4_2[1]), "10111213");
    assert_eq!(hex::encode(&ec_4_2[2]), "20212223");
    assert_eq!(hex::encode(&ec_4_2[3]), "30313233");
    assert_eq!(hex::encode(&ec_4_2[4]), "00000000");
    assert_eq!(hex::encode(&ec_4_2[5]), "8084888c");

    let ec_8_3_payload = hex::decode(concat!(
        "00010203", "10111213", "20212223", "30313233", "40414243", "50515253", "60616263",
        "70717273"
    ))
    .unwrap();
    let ec_8_3 = encode_erasure_shards(&ec_8_3_payload, LOCAL_EC_8_3_PROFILE).unwrap();
    assert_eq!(hex::encode(&ec_8_3[0]), "00010203");
    assert_eq!(hex::encode(&ec_8_3[1]), "10111213");
    assert_eq!(hex::encode(&ec_8_3[2]), "20212223");
    assert_eq!(hex::encode(&ec_8_3[3]), "30313233");
    assert_eq!(hex::encode(&ec_8_3[4]), "40414243");
    assert_eq!(hex::encode(&ec_8_3[5]), "50515253");
    assert_eq!(hex::encode(&ec_8_3[6]), "60616263");
    assert_eq!(hex::encode(&ec_8_3[7]), "70717273");
    assert_eq!(hex::encode(&ec_8_3[8]), "00000000");
    assert_eq!(hex::encode(&ec_8_3[9]), "bab2aaa2");
    assert_eq!(hex::encode(&ec_8_3[10]), "2565a5e5");

    let replicated =
        encode_erasure_shards(b"replicated profile payload", LOCAL_REPLICATED_3_PROFILE).unwrap();
    assert_eq!(replicated[0], replicated[1]);
    assert_eq!(replicated[0], replicated[2]);
}

#[test]
fn core_store_erasure_codec_recovers_every_allowed_missing_shard_set() {
    for profile in [
        LOCAL_EC_4_2_PROFILE,
        LOCAL_EC_8_3_PROFILE,
        LOCAL_REPLICATED_3_PROFILE,
    ] {
        let payload_len = profile.data_shards * 17 + 5;
        let payload = (0..payload_len)
            .map(|index| (index.wrapping_mul(37) % 251) as u8)
            .collect::<Vec<_>>();
        let original = encode_erasure_shards(&payload, profile).unwrap();
        let missing_sets = shard_missing_sets(profile.total_shards(), profile.parity_shards);

        for missing in missing_sets {
            let mut shards = original
                .iter()
                .cloned()
                .map(Some)
                .collect::<Vec<Option<Vec<u8>>>>();
            for index in &missing {
                shards[*index] = None;
            }
            reconstruct_data_shards(&mut shards, profile).unwrap_or_else(|error| {
                panic!(
                    "profile {} failed to recover missing {:?}: {error}",
                    profile.id, missing
                )
            });
            for shard_index in 0..profile.data_shards {
                assert_eq!(
                    shards[shard_index].as_ref().unwrap(),
                    &original[shard_index],
                    "profile {} recovered wrong data shard {} with missing {:?}",
                    profile.id,
                    shard_index,
                    missing
                );
            }
        }
    }
}

#[test]
fn core_store_local_placement_satisfies_profile_failure_domains() {
    let ec_4_2 = plan_local_shard_placements(LOCAL_EC_4_2_PROFILE).unwrap();
    assert_eq!(ec_4_2.len(), 6);
    assert_eq!(
        failure_domain_counts(&ec_4_2),
        BTreeMap::from([
            ("local-cell-1", 2),
            ("local-cell-2", 2),
            ("local-cell-3", 2)
        ])
    );

    let ec_8_3 = plan_local_shard_placements(LOCAL_EC_8_3_PROFILE).unwrap();
    assert_eq!(ec_8_3.len(), 11);
    assert_eq!(
        failure_domain_counts(&ec_8_3),
        BTreeMap::from([
            ("local-cell-1", 3),
            ("local-cell-2", 3),
            ("local-cell-3", 3),
            ("local-cell-4", 2),
        ])
    );

    let replicated = plan_local_shard_placements(LOCAL_REPLICATED_3_PROFILE).unwrap();
    assert_eq!(replicated.len(), 3);
    assert_eq!(
        replicated
            .iter()
            .map(|placement| placement.node_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
}

fn failure_domain_counts(placements: &[LocalShardPlacement]) -> BTreeMap<&str, usize> {
    let mut counts = BTreeMap::new();
    for placement in placements {
        *counts.entry(placement.failure_domain.as_str()).or_default() += 1;
    }
    counts
}

fn shard_missing_sets(total_shards: usize, max_missing: usize) -> Vec<Vec<usize>> {
    fn visit(
        total_shards: usize,
        remaining: usize,
        start: usize,
        current: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        out.push(current.clone());
        if remaining == 0 {
            return;
        }
        for index in start..total_shards {
            current.push(index);
            visit(total_shards, remaining - 1, index + 1, current, out);
            current.pop();
        }
    }

    let mut out = Vec::new();
    visit(total_shards, max_missing, 0, &mut Vec::new(), &mut out);
    out
}

#[tokio::test]
async fn core_store_logical_file_api_accepts_all_normative_erasure_profiles() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    for (profile_id, data_shards, parity_shards, codec_id) in [
        ("ec-4-2", 4, 2, "rs-gf256-vandermonde-0x11d-v1/ec-4-2"),
        ("ec-8-3", 8, 3, "rs-gf256-vandermonde-0x11d-v1/ec-8-3"),
        (
            "replicated-3",
            1,
            2,
            "rs-gf256-vandermonde-0x11d-v1/replicated-3",
        ),
    ] {
        let payload = format!("profile:{profile_id}:logical-file-payload").into_bytes();
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 1,
                logical_file_id: format!("profile-test/{profile_id}/segment-1"),
                source: payload.clone(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy {
                    erasure_profile_id: profile_id.to_string(),
                    ..Default::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: format!("profile-test-{profile_id}"),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(manifest.erasure_profile_id, profile_id);
        assert_eq!(manifest.data_shards, data_shards);
        assert_eq!(manifest.parity_shards, parity_shards);
        assert_eq!(manifest.codec_id, codec_id);
        assert_eq!(manifest.blocks[0].codec_id, codec_id);
        assert_eq!(
            manifest.blocks[0].shards.len(),
            (data_shards + parity_shards) as usize
        );
        assert!(
            core_object_ref_from_logical_file_manifest(&manifest)
                .manifest_ref
                .starts_with(crate::core_store::CORE_LOGICAL_FILE_INLINE_REF_PREFIX)
        );

        store.verify_logical_file_manifest(&manifest).await.unwrap();
        let read_back = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest,
                ranges: vec![CoreByteRange {
                    start: 0,
                    end_exclusive: payload.len() as u64,
                }],
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "local".to_string(),
                    authz_realm_id: "system".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            })
            .await
            .unwrap();
        assert_eq!(read_back, payload);
    }
}

#[tokio::test]
async fn core_store_boundary_schema_round_trips_through_corestore() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let receipt = store
        .put_boundary_schema(PutBoundarySchema {
            schema: sample_boundary_schema("customer-documents", 1),
            expected_generation: None,
            mutation_id: "boundary-schema-genesis".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(receipt.bucket, "customer-documents");
    assert_eq!(receipt.generation, 1);
    assert!(receipt.schema_hash.starts_with("sha256:"));

    let schema = store
        .read_boundary_schema("customer-documents")
        .await
        .unwrap()
        .expect("boundary schema");
    assert_eq!(schema.generation, 1);
    assert_eq!(schema.dimensions[0].name, "customer_tenant");
    assert_eq!(schema.dimensions[0].categories[0], "security_realm");
}

#[tokio::test]
async fn core_store_boundary_schema_allows_optional_dimension_evolution() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .put_boundary_schema(PutBoundarySchema {
            schema: sample_boundary_schema("customer-documents", 1),
            expected_generation: None,
            mutation_id: "boundary-schema-genesis".to_string(),
        })
        .await
        .unwrap();
    let mut next = sample_boundary_schema("customer-documents", 2);
    next.dimensions.push(CoreBoundaryDimension {
        name: "project".to_string(),
        source: CoreBoundarySource::PathTemplate {
            template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
        },
        value_type: "string".to_string(),
        categories: vec!["storage_partition".to_string(), "query_prune".to_string()],
        required: false,
        cardinality: "high".to_string(),
        max_values_per_block: 8,
        placement_affinity: "prefer_colocate".to_string(),
        compaction_scope: "prefer_same_value".to_string(),
        shared_ranges_allowed: false,
        shared_record_kinds: Vec::new(),
        deprecated: false,
    });

    store
        .put_boundary_schema(PutBoundarySchema {
            schema: next,
            expected_generation: Some(1),
            mutation_id: "boundary-schema-add-project".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .read_boundary_schema("customer-documents")
            .await
            .unwrap()
            .unwrap()
            .dimensions
            .len(),
        2
    );
}

#[tokio::test]
async fn core_store_boundary_schema_rejects_incompatible_evolution() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .put_boundary_schema(PutBoundarySchema {
            schema: sample_boundary_schema("customer-documents", 1),
            expected_generation: None,
            mutation_id: "boundary-schema-genesis".to_string(),
        })
        .await
        .unwrap();

    let mut required_addition = sample_boundary_schema("customer-documents", 2);
    required_addition.dimensions.push(CoreBoundaryDimension {
        name: "project".to_string(),
        source: CoreBoundarySource::PathTemplate {
            template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
        },
        value_type: "string".to_string(),
        categories: vec!["query_prune".to_string()],
        required: true,
        cardinality: "high".to_string(),
        max_values_per_block: 8,
        placement_affinity: "prefer_colocate".to_string(),
        compaction_scope: "prefer_same_value".to_string(),
        shared_ranges_allowed: false,
        shared_record_kinds: Vec::new(),
        deprecated: false,
    });
    let err = store
        .put_boundary_schema(PutBoundarySchema {
            schema: required_addition,
            expected_generation: Some(1),
            mutation_id: "boundary-schema-add-required".to_string(),
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains(AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str())
    );

    let mut type_change = sample_boundary_schema("customer-documents", 2);
    type_change.dimensions[0].value_type = "string".to_string();
    let err = store
        .put_boundary_schema(PutBoundarySchema {
            schema: type_change,
            expected_generation: Some(1),
            mutation_id: "boundary-schema-type-change".to_string(),
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains(AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str())
    );
}
