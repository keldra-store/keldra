use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use keldra_consensus::{
    CommittedPeerPins, NodeState, PeerTlsAcceptor, PeerTlsConfig, PeerTlsIdentity,
};
use keldra_store::StoreOptions;
use tonic::Code;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

use super::*;
use crate::node_identity;

#[path = "tests/support.rs"]
mod support;

use support::*;

#[test]
fn joining_callers_have_only_join_control_authority() {
    let cluster_id = ClusterId(*b"join-control-tst");
    let pins = TestPins::new(cluster_id);
    pins.install(NodeId(2), PeerSpkiSha256([7; 32]), NodeState::Joining);
    assert!(
        pins.authorized_rpc_pins(cluster_id, NodeId(2), PeerRpcKind::JoinControl)
            .is_some()
    );
    for denied in [
        PeerRpcKind::AppendEntries,
        PeerRpcKind::Vote,
        PeerRpcKind::InstallSnapshot,
        PeerRpcKind::ServingLease,
        PeerRpcKind::DataPlane,
        PeerRpcKind::StateTransfer,
    ] {
        assert!(
            pins.authorized_rpc_pins(cluster_id, NodeId(2), denied)
                .is_none(),
            "JOINING caller received {denied:?} authority"
        );
    }
}

async fn assert_joining_denied_by_every_rpc(transport: &DataPeerTransport, address: &str) {
    let mut client = transport.client(NodeId(1), address).unwrap();
    let peer = transport.context();
    let typed = || wire::TypedMutationRequest {
        peer: Some(peer.clone()),
        mutation_json: Vec::new(),
    };
    let page = || wire::HandoffPageRequest {
        peer: Some(peer.clone()),
        cursor_json: Vec::new(),
        max_records: 0,
        max_bytes: 0,
        handoff: None,
    };
    let record = || wire::HandoffRecordRequest {
        peer: Some(peer.clone()),
        record_json: Vec::new(),
        handoff: None,
    };
    let content = || wire::ContentRequest {
        peer: Some(peer.clone()),
        blob: None,
    };
    let shard = || wire::ShardRequest {
        peer: Some(peer.clone()),
        fragment_format_version: 0,
        blob: None,
        ordinal: 0,
    };
    let realm = || wire::AuthzRealmRequest {
        peer: Some(peer.clone()),
        scope_json: Vec::new(),
        handoff: None,
    };
    let catalogue = || wire::AuthzSchemaCatalogueRequest {
        peer: Some(peer.clone()),
        storage_tenant: "tenant".into(),
        handoff: None,
    };
    let mut denied = 0_usize;
    let mut tested_rpcs = std::collections::BTreeSet::new();
    macro_rules! require_denied {
        ($operation:expr, $name:literal) => {
            match $operation.await {
                Err(status) => {
                    assert_eq!(
                        status.code(),
                        Code::PermissionDenied,
                        "{} returned {status}",
                        $name
                    );
                    denied += 1;
                    assert!(
                        tested_rpcs.insert($name),
                        "duplicate denial coverage: {}",
                        $name
                    );
                }
                Ok(_) => panic!("{} accepted a JOINING caller", $name),
            }
        };
    }

    let d = || wire::MutationDrainRequest {
        peer: Some(peer.clone()),
        handoff: None,
    };
    require_denied!(client.drain_mutations(d()), "DrainMutations");
    require_denied!(client.release_mutation_drain(d()), "ReleaseMutationDrain");
    require_denied!(client.apply_object_mutation(typed()), "ApplyObjectMutation");
    require_denied!(
        client.apply_object_mutation_batch(wire::TypedMutationBatchRequest {
            peer: Some(peer.clone()),
            mutation_json: Vec::new(),
        }),
        "ApplyObjectMutationBatch"
    );
    require_denied!(
        client.apply_retained_version_delete(typed()),
        "ApplyRetainedVersionDelete"
    );
    require_denied!(
        client.read_exact_object_versions(wire::ExactObjectVersionBatchRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_paths: Vec::new(),
            version_ids: Vec::new(),
        }),
        "ReadExactObjectVersions"
    );
    require_denied!(
        client.read_object_path_snapshot(wire::ObjectPathSnapshotRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_path: String::new(),
        }),
        "ReadObjectPathSnapshot"
    );
    require_denied!(
        client.read_object_path_snapshots(wire::ObjectPathSnapshotBatchRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_paths: Vec::new(),
        }),
        "ReadObjectPathSnapshots"
    );
    require_denied!(
        client.read_current_object_snapshot(wire::ObjectPathSnapshotRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_path: String::new(),
        }),
        "ReadCurrentObjectSnapshot"
    );
    require_denied!(
        client.read_current_object_snapshots(wire::CurrentObjectSnapshotBatchRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_paths: Vec::new(),
        }),
        "ReadCurrentObjectSnapshots"
    );
    require_denied!(
        client.repair_object_path_snapshot(wire::RepairObjectPathSnapshotRequest {
            peer: Some(peer.clone()),
            tenant_id: 0,
            bucket_id: 0,
            exact_path: String::new(),
            expected_snapshot_json: Vec::new(),
            selected_snapshot_json: Vec::new(),
            placement_fence_term: 0,
            placement_fence_index: 0,
        }),
        "RepairObjectPathSnapshot"
    );
    require_denied!(
        client.apply_authz_realm_mutation(typed()),
        "ApplyAuthzRealmMutation"
    );
    require_denied!(
        client.apply_reference_deltas(typed()),
        "ApplyReferenceDeltas"
    );
    require_denied!(
        client.get_reference_delta_status(wire::ReferenceDeltaStatusRequest {
            peer: Some(peer.clone()),
            source_id_json: Vec::new(),
        }),
        "GetReferenceDeltaStatus"
    );
    require_denied!(
        client.get_source_journal_status(wire::SourceJournalStatusRequest {
            peer: Some(peer.clone()),
        }),
        "GetSourceJournalStatus"
    );
    require_denied!(
        client.read_source_journal(wire::SourceJournalReadRequest {
            peer: Some(peer.clone()),
            after_offset: 0,
            limit: 0,
            max_bytes: 1,
        }),
        "ReadSourceJournal"
    );
    definition_coordination::denied_test_calls!(client, peer, require_denied);
    derived_consumer::denied_test_call!(client, peer, require_denied);
    require_denied!(client.small_content_exists(content()), "SmallContentExists");
    require_denied!(client.get_small_content(content()), "GetSmallContent");
    require_denied!(
        client.get_payload_range(wire::PayloadRangeRequest {
            peer: Some(peer.clone()),
            blob: None,
            placement_fence_term: 0,
            placement_fence_index: 0,
            offset: 0,
            length: 1,
            shard_ordinal: None,
        }),
        "GetPayloadRange"
    );
    require_denied!(
        client.get_complete_source_state(wire::CompleteSourceStateRequest {
            peer: Some(peer.clone()),
            blob: None,
            placement_fence_term: 0,
            placement_fence_index: 0,
        }),
        "GetCompleteSourceState"
    );
    require_denied!(
        client.put_small_content(tokio_stream::iter([wire::SmallContentPutFrame {
            peer: Some(peer.clone()),
            blob: None,
            offset: 0,
            content: Vec::new(),
            end: true,
        }])),
        "PutSmallContent"
    );
    require_denied!(client.get_complete_source(content()), "GetCompleteSource");
    require_denied!(
        client.put_complete_source(tokio_stream::iter([wire::CompleteSourcePutFrame {
            peer: Some(peer.clone()),
            blob: None,
            offset: 0,
            content: Vec::new(),
            end: true,
        }])),
        "PutCompleteSource"
    );
    require_denied!(client.shard_exists(shard()), "ShardExists");
    require_denied!(client.get_shard(shard()), "GetShard");
    require_denied!(
        client.put_shard(tokio_stream::iter([wire::ShardPutFrame {
            shard: Some(shard()),
            offset: 0,
            content: Vec::new(),
            end: true,
        }])),
        "PutShard"
    );
    require_denied!(client.export_object_records(page()), "ExportObjectRecords");
    require_denied!(
        client.install_object_record(record()),
        "InstallObjectRecord"
    );
    require_denied!(
        client.read_handoff_object_path_snapshot(wire::HandoffObjectPathSnapshotRequest {
            peer: Some(peer.clone()),
            handoff: None,
            tenant_id: 0,
            bucket_id: 0,
            exact_path: String::new(),
        }),
        "ReadHandoffObjectPathSnapshot"
    );
    require_denied!(
        client.repair_handoff_object_path_snapshot(wire::RepairHandoffObjectPathSnapshotRequest {
            peer: Some(peer.clone()),
            handoff: None,
            tenant_id: 0,
            bucket_id: 0,
            exact_path: String::new(),
            expected_snapshot_json: Vec::new(),
            selected_snapshot_json: Vec::new(),
        }),
        "RepairHandoffObjectPathSnapshot"
    );
    require_denied!(
        client.get_handoff_source_journal_status(wire::HandoffSourceJournalStatusRequest {
            peer: Some(peer.clone()),
            handoff: None,
        }),
        "GetHandoffSourceJournalStatus"
    );
    require_denied!(
        client.complete_system_bootstrap_handoff(wire::CompleteSystemBootstrapHandoffRequest {
            peer: Some(peer.clone()),
            handoff: None,
        }),
        "CompleteSystemBootstrapHandoff"
    );
    require_denied!(
        client.read_handoff_source_journal(wire::HandoffSourceJournalReadRequest {
            peer: Some(peer.clone()),
            handoff: None,
            after_offset: 0,
            limit: 0,
            max_bytes: 1,
        }),
        "ReadHandoffSourceJournal"
    );
    require_denied!(
        client.get_handoff_reference_cursor(wire::HandoffReferenceCursorRequest {
            peer: Some(peer.clone()),
            handoff: None,
            source_id_json: Vec::new(),
        }),
        "GetHandoffReferenceCursor"
    );
    require_denied!(
        client.advance_handoff_reference_cursor(wire::HandoffReferenceCursorAdvanceRequest {
            peer: Some(peer.clone()),
            handoff: None,
            source_id_json: Vec::new(),
            through: 0,
        }),
        "AdvanceHandoffReferenceCursor"
    );
    require_denied!(
        client.export_logical_records(page()),
        "ExportLogicalRecords"
    );
    require_denied!(
        client.install_logical_record(record()),
        "InstallLogicalRecord"
    );
    require_denied!(
        client.read_logical_record(wire::LogicalRecordRequest {
            peer: Some(peer.clone()),
            id_json: Vec::new(),
            handoff: None,
        }),
        "ReadLogicalRecord"
    );
    require_denied!(
        client.repair_logical_record(wire::RepairLogicalRecordRequest {
            peer: Some(peer.clone()),
            id_json: Vec::new(),
            present: false,
            candidate_json: Vec::new(),
            handoff: None,
        }),
        "RepairLogicalRecord"
    );
    require_denied!(
        client.export_authz_realm_keys(page()),
        "ExportAuthzRealmKeys"
    );
    require_denied!(
        client.read_authz_schema_catalogue(catalogue()),
        "ReadAuthzSchemaCatalogue"
    );
    require_denied!(
        client.repair_authz_schema_catalogue(wire::RepairAuthzSchemaCatalogueRequest {
            peer: Some(peer.clone()),
            storage_tenant: "tenant".into(),
            present: false,
            catalogue_json: Vec::new(),
            handoff: None,
        }),
        "RepairAuthzSchemaCatalogue"
    );
    require_denied!(
        client.read_authz_realm_manifest(realm()),
        "ReadAuthzRealmManifest"
    );
    require_denied!(
        client.repair_authz_realm_absence(realm()),
        "RepairAuthzRealmAbsence"
    );
    require_denied!(client.get_authz_realm(realm()), "GetAuthzRealm");
    require_denied!(
        client.put_authz_realm(tokio_stream::iter([wire::AuthzRealmPutFrame {
            peer: Some(peer.clone()),
            offset: 0,
            content: Vec::new(),
            end: true,
            manifest_json: Vec::new(),
            handoff: None,
        }])),
        "PutAuthzRealm"
    );
    require_denied!(
        client.export_payload_artifacts(page()),
        "ExportPayloadArtifacts"
    );
    require_denied!(
        client.install_payload_lifecycle(record()),
        "InstallPayloadLifecycle"
    );
    assert_eq!(
        denied, 56,
        "the DataPeer RPC list changed without updating this test"
    );
    let production_rpcs = include_str!("../../proto/data_peer.proto")
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("rpc "))
        .filter_map(|line| line.split_once('(').map(|(name, _)| name.trim()))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        tested_rpcs, production_rpcs,
        "every production RPC must enforce JOINING denial before parsing its payload"
    );
}

#[tokio::test]
async fn real_mtls_binds_claimed_node_and_rechecks_rpc_class_and_membership() {
    let cluster_id = ClusterId(*b"data-peer-test01");
    let server_id = identity(cluster_id, NodeId(1));
    let joining_id = identity(cluster_id, NodeId(2));
    let wrong_id = identity(cluster_id, NodeId(3));
    let pins = Arc::new(TestPins::new(cluster_id));
    pins.install(NodeId(1), server_id.spki_sha256(), NodeState::Active);
    pins.install(NodeId(2), joining_id.spki_sha256(), NodeState::Joining);

    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(directory.path(), 1))
        .await
        .unwrap();
    let (address, shutdown, server) = start_server(server_id, pins.clone(), store.clone()).await;
    let address = address.to_string();
    let joining = DataPeerTransport::new(
        cluster_id,
        NodeId(2),
        PeerTlsConnector::new(joining_id.clone(), pins.clone(), PeerTlsConfig::default()).unwrap(),
    )
    .unwrap();

    assert_joining_denied_by_every_rpc(&joining, &address).await;

    pins.set_state(NodeId(2), NodeState::Active);
    let status = joining
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap();
    assert_eq!(status.source_id.node_id, 1);
    let spoofed = ReferenceDeltaBatch {
        source: status.source_id,
        after: 0,
        through: 0,
        deltas: Vec::new(),
    };
    let denied = joining
        .apply_reference_deltas(NodeId(1), &address, &spoofed)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    let client_source = SourceId {
        node_id: 2,
        source_epoch: [2; 32],
    };
    let empty = ReferenceDeltaBatch {
        source: client_source,
        after: 0,
        through: 0,
        deltas: Vec::new(),
    };
    let applied = joining
        .apply_reference_deltas(NodeId(1), &address, &empty)
        .await
        .unwrap();
    assert_eq!(applied.through, 0);
    assert_eq!(
        joining
            .reference_delta_status(NodeId(1), &address, client_source)
            .await
            .unwrap(),
        0
    );
    let journal = joining
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap();
    assert!(
        joining
            .read_source_journal(
                NodeId(1),
                &address,
                journal.source_id,
                0,
                16,
                MAX_TYPED_MUTATION_BYTES as u64,
            )
            .await
            .unwrap()
            .changes
            .is_empty()
    );

    let mut raw_active = joining.client(NodeId(1), &address).unwrap();
    let invalid = raw_active
        .export_object_records(wire::HandoffPageRequest {
            peer: Some(joining.context()),
            cursor_json: Vec::new(),
            max_records: 0,
            max_bytes: 1,
            handoff: None,
        })
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
    let oversized = raw_active
        .install_object_record(wire::HandoffRecordRequest {
            peer: Some(joining.context()),
            record_json: vec![0; MAX_TYPED_MUTATION_BYTES + 1],
            handoff: None,
        })
        .await
        .unwrap_err();
    assert_eq!(oversized.code(), Code::InvalidArgument);

    let tenant_id = 11;
    let bucket_id = 22;
    let exact_path = "peer-snapshot";
    let placement_fence = keldra_store::PlacementLogId { term: 1, index: 1 };
    let snapshot = Some(ObjectPathSnapshot {
        tenant_id,
        bucket_id,
        exact_path: exact_path.into(),
        head: keldra_store::Head {
            version: keldra_store::VersionId(1),
            deleted: true,
            mutation_stamp: None,
        },
        versions: vec![keldra_store::Version {
            id: keldra_store::VersionId(1),
            blob: None,
            content_type: None,
            deleted: true,
            committed_at_unix_millis: 1,
            protected_link_descriptor: false,
        }],
        journal_pending_versions: Vec::new(),
        journal_released_versions: Vec::new(),
        definition_locator: None,
        alias_registry: None,
        alias_registry_transition: None,
    });
    store
        .repair_object_path_snapshot(tenant_id, bucket_id, exact_path, None, snapshot.as_ref())
        .await
        .unwrap();
    assert_eq!(
        joining
            .read_object_path_snapshot(NodeId(1), &address, tenant_id, bucket_id, exact_path,)
            .await
            .unwrap(),
        snapshot
    );
    let current = joining
        .read_current_object_snapshot(NodeId(1), &address, tenant_id, bucket_id, exact_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.exact_path, exact_path);
    assert_eq!(current.head, snapshot.as_ref().unwrap().head);
    assert_eq!(current.version, snapshot.as_ref().unwrap().versions[0]);
    let stale = joining
        .repair_object_path_snapshot(
            NodeId(1),
            &address,
            keldra_store::PlacementLogId { term: 1, index: 0 },
            tenant_id,
            bucket_id,
            exact_path,
            snapshot.as_ref(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Unavailable);
    joining
        .repair_object_path_snapshot(
            NodeId(1),
            &address,
            placement_fence,
            tenant_id,
            bucket_id,
            exact_path,
            snapshot.as_ref(),
            None,
        )
        .await
        .unwrap();
    assert!(
        store
            .export_object_path_record(tenant_id, bucket_id, exact_path)
            .unwrap()
            .is_none()
    );
    joining
        .repair_object_path_snapshot(
            NodeId(1),
            &address,
            placement_fence,
            tenant_id,
            bucket_id,
            exact_path,
            None,
            snapshot.as_ref(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .export_object_path_record(tenant_id, bucket_id, exact_path)
            .unwrap(),
        snapshot
    );

    let bytes = b"typed data peer over real mutual TLS";
    let reference = BlobRef {
        hash: *blake3::hash(bytes).as_bytes(),
        length: bytes.len() as u64,
    };
    joining
        .put_small_content(NodeId(1), &address, &reference, bytes)
        .await
        .unwrap();
    assert!(
        joining
            .small_content_exists(NodeId(1), &address, &reference)
            .await
            .unwrap()
    );
    assert_eq!(
        joining
            .get_small_content(NodeId(1), &address, &reference)
            .await
            .unwrap(),
        bytes
    );
    assert_eq!(
        joining
            .get_payload_range(
                NodeId(1),
                &address,
                placement_fence,
                &reference,
                6,
                8,
                None,
                None,
                8
            )
            .await
            .unwrap(),
        bytes[6..14]
    );
    let invalid_range = joining
        .get_payload_range(
            NodeId(1),
            &address,
            placement_fence,
            &reference,
            u64::MAX,
            1,
            None,
            None,
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(invalid_range.code(), Code::ResourceExhausted);
    let error = joining
        .shard_exists(
            NodeId(1),
            &address,
            &ShardIdentity::new(reference.clone(), 0),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    let mismatched = DataPeerTransport::new(
        cluster_id,
        NodeId(2),
        PeerTlsConnector::new(wrong_id, pins.clone(), PeerTlsConfig::default()).unwrap(),
    )
    .unwrap();
    let denied = mismatched
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let wrong_cluster = DataPeerTransport::new(
        ClusterId(*b"other-cluster-id"),
        NodeId(2),
        PeerTlsConnector::new(joining_id.clone(), pins.clone(), PeerTlsConfig::default()).unwrap(),
    )
    .unwrap();
    let denied = wrong_cluster
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let wrong_node = DataPeerTransport::new(
        cluster_id,
        NodeId(99),
        PeerTlsConnector::new(joining_id, pins.clone(), PeerTlsConfig::default()).unwrap(),
    )
    .unwrap();
    let denied = wrong_node
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut wrong_schema = joining.client(NodeId(1), &address).unwrap();
    let denied_range = wrong_node
        .get_payload_range(
            NodeId(1),
            &address,
            placement_fence,
            &reference,
            6,
            8,
            None,
            None,
            8,
        )
        .await
        .unwrap_err();
    assert_eq!(denied_range.code(), Code::PermissionDenied);
    let denied = wrong_schema
        .get_source_journal_status(wire::SourceJournalStatusRequest {
            peer: Some(wire::PeerContext {
                schema_version: DATA_PEER_SCHEMA_VERSION + 1,
                ..joining.context()
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::FailedPrecondition);

    pins.remove(NodeId(2));
    let denied = joining
        .source_journal_status(NodeId(1), &address)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let _ = shutdown.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn real_mtls_large_source_and_shard_streams_are_exact_and_restart_safe() {
    let cluster_id = ClusterId(*b"peer-payload-tst");
    let server_id = identity(cluster_id, NodeId(1));
    let client_id = identity(cluster_id, NodeId(2));
    let pins = Arc::new(TestPins::new(cluster_id));
    pins.install(NodeId(1), server_id.spki_sha256(), NodeState::Active);
    pins.install(NodeId(2), client_id.spki_sha256(), NodeState::Active);

    let source_directory = tempfile::tempdir().unwrap();
    let destination_directory = tempfile::tempdir().unwrap();
    let destination_root = destination_directory.path().join("store");
    let source_store = Store::open(StoreOptions::new(source_directory.path(), 2))
        .await
        .unwrap();
    let destination = Store::open(StoreOptions::new(&destination_root, 1))
        .await
        .unwrap();
    let source = (0..2 * DATA_PEER_FRAME_BYTES + 333)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let reference = source_store.stage_blob(&source).await.unwrap();
    let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
    let mut shards = vec![Vec::new(); usize::from(codec.profile().total_shards())];
    source_store
        .encode_sealed_source(&codec, &reference, &mut shards)
        .await
        .unwrap();

    let (source_address, source_shutdown, source_server) =
        start_server(client_id.clone(), pins.clone(), source_store.clone()).await;
    let source_address = source_address.to_string();
    let (address, shutdown, server) =
        start_server(server_id, pins.clone(), destination.clone()).await;
    let address = address.to_string();
    let transport = DataPeerTransport::new(
        cluster_id,
        NodeId(2),
        PeerTlsConnector::new(client_id, pins, PeerTlsConfig::default()).unwrap(),
    )
    .unwrap();
    let placement_fence = keldra_store::PlacementLogId { term: 1, index: 1 };
    assert_eq!(
        transport
            .complete_source_state(NodeId(1), &address, placement_fence, &reference)
            .await
            .unwrap(),
        keldra_store::PayloadArtifactState::Missing
    );

    assert_eq!(
        transport
            .copy_complete_source(NodeId(2), &source_address, NodeId(1), &address, &reference,)
            .await
            .unwrap(),
        CompleteCopySealOutcome::Created
    );
    assert_eq!(
        transport
            .complete_source_state(NodeId(1), &address, placement_fence, &reference)
            .await
            .unwrap(),
        keldra_store::PayloadArtifactState::Valid
    );
    assert_eq!(
        transport
            .complete_source_state(
                NodeId(1),
                &address,
                keldra_store::PlacementLogId { term: 1, index: 0 },
                &reference,
            )
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(
        transport
            .copy_complete_source(NodeId(2), &source_address, NodeId(1), &address, &reference,)
            .await
            .unwrap(),
        CompleteCopySealOutcome::AlreadyPresent
    );

    let source_reader = source_store.open_blob(&reference).await.unwrap();
    assert_eq!(
        transport
            .put_complete_source(NodeId(1), &address, &reference, source_reader)
            .await
            .unwrap(),
        CompleteCopySealOutcome::AlreadyPresent
    );
    let retry_reader = source_store.open_blob(&reference).await.unwrap();
    assert_eq!(
        transport
            .put_complete_source(NodeId(1), &address, &reference, retry_reader)
            .await
            .unwrap(),
        CompleteCopySealOutcome::AlreadyPresent
    );
    assert_eq!(
        collect_content(
            transport
                .get_complete_source(NodeId(1), &address, &reference)
                .await
                .unwrap()
        )
        .await,
        source
    );

    let first = ShardIdentity::new(reference.clone(), 0);
    assert_eq!(
        transport
            .put_shard(NodeId(1), &address, &first, Cursor::new(shards[0].clone()),)
            .await
            .unwrap(),
        ShardSealOutcome::Created
    );
    assert_eq!(
        transport
            .put_shard(NodeId(1), &address, &first, Cursor::new(shards[0].clone()),)
            .await
            .unwrap(),
        ShardSealOutcome::AlreadyPresent
    );
    assert_eq!(
        collect_content(
            transport
                .get_shard(NodeId(1), &address, &first)
                .await
                .unwrap()
        )
        .await,
        shards[0]
    );

    let second = ShardIdentity::new(reference.clone(), 1);
    let mut corrupt = shards[1].clone();
    *corrupt.last_mut().unwrap() ^= 0xff;
    let error = transport
        .put_shard(NodeId(1), &address, &second, Cursor::new(corrupt))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        !transport
            .shard_exists(NodeId(1), &address, &second)
            .await
            .unwrap()
    );

    let third = ShardIdentity::new(reference.clone(), 2);
    let truncated = shards[2][..shards[2].len() - 1].to_vec();
    let error = transport
        .put_shard(NodeId(1), &address, &third, Cursor::new(truncated))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DataLoss);
    assert!(
        !transport
            .shard_exists(NodeId(1), &address, &third)
            .await
            .unwrap()
    );

    let error = transport
        .client(NodeId(1), &address)
        .unwrap()
        .put_shard(tokio_stream::iter([
            shard_frame(&transport, &second, 0, b"a", false),
            shard_frame(&transport, &second, 2, b"b", false),
        ]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    let error = transport
        .client(NodeId(1), &address)
        .unwrap()
        .put_shard(tokio_stream::iter([
            shard_frame(&transport, &second, 0, b"a", false),
            shard_frame(&transport, &third, 1, b"b", false),
        ]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    let _ = shutdown.send(());
    server.await.unwrap();
    let _ = source_shutdown.send(());
    source_server.await.unwrap();
    drop(destination);
    let reopened = Store::open(StoreOptions::new(&destination_root, 1))
        .await
        .unwrap();
    let mut reader = reopened.open_blob(&reference).await.unwrap();
    let mut recovered = Vec::new();
    let mut buffer = vec![0_u8; DATA_PEER_FRAME_BYTES];
    loop {
        let read = reader.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        recovered.extend_from_slice(&buffer[..read]);
    }
    assert_eq!(recovered, source);
    assert!(reopened.get_shard(&codec, &first).is_ok());
}
