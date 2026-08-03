use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};

use anvil_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, RealmId, RelationDefinition, RewriteRule,
    Schema, Tuple,
};
use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{
    AuthzRealmMutationContext, AuthzRealmSnapshotError, PlacementLogId, PublishSchemaRequest,
    SchemaId, SourceId, StorageTenantId, Store, StoreOptions, TupleMutation, TupleMutationKind,
};

use super::*;
use crate::placement::PlacementNode;

#[derive(Default)]
struct StoreTransport {
    stores: BTreeMap<NodeId, Store>,
    failed_applies: RwLock<BTreeSet<NodeId>>,
}

impl StoreTransport {
    fn set_apply_failure(&self, node_id: NodeId, failed: bool) {
        let mut failures = self.failed_applies.write().unwrap();
        if failed {
            failures.insert(node_id);
        } else {
            failures.remove(&node_id);
        }
    }

    fn store(&self, node_id: NodeId) -> Result<&Store, Status> {
        self.stores
            .get(&node_id)
            .ok_or_else(|| Status::unavailable("test replica is missing"))
    }
}

#[tonic::async_trait]
impl AuthzReplicaTransport for StoreTransport {
    async fn apply_realm_mutation(
        &self,
        target: NodeId,
        _address: &str,
        mutation: &AuthzRealmMutation,
    ) -> Result<ReplicaAuthzRealmMutationApplied, Status> {
        if self.failed_applies.read().unwrap().contains(&target) {
            return Err(Status::unavailable("injected replica partition"));
        }
        self.store(target)?
            .authz()
            .apply_authz_realm_mutation_replica(mutation)
            .map_err(authz_status)
    }

    async fn read_realm_candidate(
        &self,
        target: NodeId,
        _address: &str,
        scope: &AuthzScope,
    ) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
        candidate(&self.store(target)?.authz(), scope)
    }

    async fn install_realm_candidate(
        &self,
        target: NodeId,
        _address: &str,
        source: Option<(NodeId, String)>,
        scope: &AuthzScope,
        winner: Option<&AuthzRealmReplicaCandidate>,
    ) -> Result<AuthzRealmSnapshotApplied, Status> {
        let target = self.store(target)?.authz();
        let Some(winner) = winner else {
            return target
                .install_quorum_reconciled_authz_realm_candidate(scope, None)
                .map_err(snapshot_status);
        };
        let source = source
            .ok_or_else(|| Status::internal("present winner has no source"))?
            .0;
        let source = self.store(source)?.authz();
        let aggregate = source
            .export_authz_realm(scope)
            .map_err(snapshot_status)?
            .ok_or_else(|| Status::data_loss("winner source lost its realm"))?;
        let mut bytes = Vec::new();
        let manifest = source
            .export_authz_realm_stream(scope, &mut bytes)
            .map_err(snapshot_status)?
            .ok_or_else(|| Status::data_loss("winner source lost its realm"))?;
        let observed = AuthzRealmReplicaCandidate::from_aggregate(&aggregate, manifest.clone())?;
        if &observed != winner {
            return Err(Status::data_loss("winner source changed during repair"));
        }
        target
            .install_quorum_reconciled_authz_realm_stream(&manifest, std::io::Cursor::new(bytes))
            .map_err(snapshot_status)
    }
}

fn snapshot_status(error: AuthzRealmSnapshotError) -> Status {
    Status::failed_precondition(error.to_string())
}

fn candidate(
    repository: &AuthzRepository,
    scope: &AuthzScope,
) -> Result<Option<AuthzRealmReplicaCandidate>, Status> {
    let Some(aggregate) = repository
        .export_authz_realm(scope)
        .map_err(snapshot_status)?
    else {
        return Ok(None);
    };
    let manifest = repository
        .export_authz_realm_stream(scope, std::io::sink())
        .map_err(snapshot_status)?
        .ok_or_else(|| Status::data_loss("realm disappeared during candidate read"))?;
    AuthzRealmReplicaCandidate::from_aggregate(&aggregate, manifest).map(Some)
}

fn tenant() -> StorageTenantId {
    StorageTenantId::parse("tenant").unwrap()
}

fn scope() -> AuthzScope {
    AuthzScope::new(tenant(), RealmId::parse("documents").unwrap()).unwrap()
}

fn schema() -> Schema {
    Schema::new([NamespaceDefinition::new(
        "document",
        [
            RelationDefinition::direct("viewer", [AllowedSubject::any_object("app")]),
            RelationDefinition::permission(
                "view",
                [RewriteRule::Inherit {
                    relation: "viewer".into(),
                }],
            ),
        ],
    )])
}

fn principal(name: &str) -> ObjectRef {
    ObjectRef::opaque("app", name).unwrap()
}

fn tuple(document: &str, principal_name: &str) -> Tuple {
    Tuple::new(
        ObjectRef::opaque("document", document).unwrap(),
        "viewer",
        principal(principal_name),
    )
}

fn context(node_id: NodeId, command: &str, position: u64) -> AuthzRealmMutationContext {
    AuthzRealmMutationContext {
        command_id: command.into(),
        active_placement_log_id: PlacementLogId { term: 2, index: 9 },
        serving_fence_term: 2,
        source_id: SourceId {
            node_id: node_id.0 as u16,
            source_epoch: [node_id.0 as u8; 32],
        },
        source_journal_position: position,
    }
}

fn tuple_request(operation: &str, revision: u64, document: &str) -> TupleBatchRequest {
    TupleBatchRequest {
        scope: scope(),
        principal: principal("writer"),
        expected_revision: Some(AuthzRevision(revision)),
        expected_binding_generation: 1,
        operation_id: Some(operation.into()),
        mutations: vec![TupleMutation {
            kind: TupleMutationKind::Add,
            tuple: tuple(document, "alice"),
        }],
    }
}

fn replica_set(tenant_id: u64) -> TenantReplicaSet {
    let nodes = [1_u64, 2, 3]
        .into_iter()
        .map(|node_id| PlacementNode::new(NodeId(node_id), NonZeroU32::new(1_000_000).unwrap()))
        .collect::<Vec<_>>();
    let group = MutableRecordReplicaGroup::select(
        PlacementKind::ZanzibarRealm,
        ClusterId([7; 16]),
        &tenant_id.to_be_bytes(),
        &nodes,
    )
    .unwrap();
    let endpoints = group
        .replicas()
        .iter()
        .map(|node_id| ReplicaEndpoint {
            node_id: *node_id,
            address: format!("node-{}", node_id.0),
        })
        .collect();
    TenantReplicaSet { group, endpoints }
}

async fn stores() -> (tempfile::TempDir, BTreeMap<NodeId, Store>) {
    let root = tempfile::tempdir().unwrap();
    let mut stores = BTreeMap::new();
    for node_id in [NodeId(1), NodeId(2), NodeId(3)] {
        stores.insert(
            node_id,
            Store::open(StoreOptions::new(
                root.path().join(format!("node-{}", node_id.0)),
                node_id.0 as u16,
            ))
            .await
            .unwrap(),
        );
    }
    (root, stores)
}

#[tokio::test]
async fn exact_quorum_repairs_minority_sibling_and_multi_revision_staleness() {
    let (_root, stores) = stores().await;
    let replicas = replica_set(42);
    let coordinator = replicas.group.coordinator();
    let third = replicas.group.replicas()[2];
    let transport = Arc::new(StoreTransport {
        stores: stores.clone(),
        ..Default::default()
    });
    let core = AuthzDistributionCore {
        local_node: coordinator,
        repository: stores[&coordinator].authz(),
        peers: transport.clone(),
        coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
    };
    let published = core
        .repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant: tenant(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: schema(),
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap();
    let bind_scope = scope();
    let bind = core
        .repository
        .coordinate_bind_schema_mutation(
            BindSchemaRequest {
                scope: bind_scope.clone(),
                schema_ref: published.schema_ref,
                expected_generation: Some(0),
                expected_revision: Some(AuthzRevision(1)),
            },
            context(coordinator, "bind", 1),
        )
        .unwrap();
    transport.set_apply_failure(third, true);
    core.replicate(&replicas, &bind_scope, &bind).await.unwrap();
    transport.set_apply_failure(third, false);
    core.reconcile(&replicas, &bind_scope).await.unwrap();

    transport.set_apply_failure(third, true);
    let left = core
        .repository
        .coordinate_tuple_mutation(
            tuple_request("left", 2, "left"),
            context(coordinator, "left", 2),
        )
        .unwrap();
    core.replicate(&replicas, &bind_scope, &left).await.unwrap();
    let sibling = stores[&third]
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("sibling", 2, "sibling"),
            context(third, "sibling", 2),
        )
        .unwrap();
    assert_ne!(
        left.mutation.as_ref().unwrap().stamp.mutation_fingerprint,
        sibling
            .mutation
            .as_ref()
            .unwrap()
            .stamp
            .mutation_fingerprint
    );
    transport.set_apply_failure(third, false);
    let winner = core.reconcile(&replicas, &bind_scope).await.unwrap();
    assert!(winner.is_some());
    assert_eq!(
        candidate(&stores[&third].authz(), &bind_scope).unwrap(),
        winner
    );

    transport.set_apply_failure(third, true);
    for (revision, operation) in [(3, "next"), (4, "newest")] {
        let coordinated = core
            .repository
            .coordinate_tuple_mutation(
                tuple_request(operation, revision, operation),
                context(coordinator, operation, revision),
            )
            .unwrap();
        core.replicate(&replicas, &bind_scope, &coordinated)
            .await
            .unwrap();
    }
    transport.set_apply_failure(third, false);
    let winner = core.reconcile(&replicas, &bind_scope).await.unwrap();
    assert_eq!(
        candidate(&stores[&third].authz(), &bind_scope).unwrap(),
        winner
    );

    let (allowed, revision) = core
        .fresh_check(
            &replicas,
            bind_scope,
            AuthzConsistency::Latest,
            AuthorizationCheck::new(
                principal("alice"),
                ObjectRef::opaque("document", "left").unwrap(),
                "view",
            ),
        )
        .await
        .unwrap();
    assert!(allowed);
    assert_eq!(revision, AuthzRevision(5));
}

#[tokio::test]
async fn stale_coordinator_is_reconciled_before_it_constructs_the_next_mutation() {
    let (_root, stores) = stores().await;
    let replicas = replica_set(314);
    let coordinator = replicas.group.coordinator();
    let second = replicas.group.replicas()[1];
    let third = replicas.group.replicas()[2];
    let transport = Arc::new(StoreTransport {
        stores: stores.clone(),
        ..Default::default()
    });
    let core = AuthzDistributionCore {
        local_node: coordinator,
        repository: stores[&coordinator].authz(),
        peers: transport,
        coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
    };
    let published = core
        .repository
        .publish_schema(PublishSchemaRequest {
            storage_tenant: tenant(),
            schema_id: SchemaId::parse("documents").unwrap(),
            schema: schema(),
            expected_revision: Some(AuthzRevision::ZERO),
        })
        .unwrap();
    let realm = scope();
    let bind = core
        .repository
        .coordinate_bind_schema_mutation(
            BindSchemaRequest {
                scope: realm.clone(),
                schema_ref: published.schema_ref,
                expected_generation: Some(0),
                expected_revision: Some(AuthzRevision(1)),
            },
            context(coordinator, "bind-stale", 1),
        )
        .unwrap();
    core.replicate(&replicas, &realm, &bind).await.unwrap();

    let majority = stores[&second]
        .authz()
        .coordinate_tuple_mutation(
            tuple_request("majority", 2, "majority"),
            context(second, "majority", 2),
        )
        .unwrap()
        .mutation
        .unwrap();
    stores[&third]
        .authz()
        .apply_authz_realm_mutation_replica(&majority)
        .unwrap();
    core.repository
        .coordinate_tuple_mutation(
            tuple_request("minority", 2, "minority"),
            context(coordinator, "minority", 2),
        )
        .unwrap();

    core.reconcile(&replicas, &realm).await.unwrap();
    assert_eq!(
        candidate(&core.repository, &realm).unwrap(),
        candidate(&stores[&second].authz(), &realm).unwrap()
    );
    let next = core
        .repository
        .coordinate_tuple_mutation(
            tuple_request("after-repair", 3, "after-repair"),
            context(coordinator, "after-repair", 3),
        )
        .unwrap();
    assert_eq!(next.mutation.unwrap().revision(), AuthzRevision(4));
}

#[tokio::test]
async fn coordinator_gate_serializes_complete_local_sequences() {
    let (_root, stores) = stores().await;
    let replicas = replica_set(2718);
    let coordinator = replicas.group.coordinator();
    let core = AuthzDistributionCore {
        local_node: coordinator,
        repository: stores[&coordinator].authz(),
        peers: Arc::new(StoreTransport {
            stores,
            ..Default::default()
        }),
        coordinator_serial: Arc::new(tokio::sync::Mutex::new(())),
    };
    let first = core.coordinator_serial.lock().await;
    let contender = core.clone();
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::channel(1);
    let task = tokio::spawn(async move {
        let _second = contender.coordinator_serial.lock().await;
        entered_tx.send(()).await.unwrap();
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), entered_rx.recv())
            .await
            .is_err()
    );
    drop(first);
    tokio::time::timeout(std::time::Duration::from_secs(1), entered_rx.recv())
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap();
}

#[test]
fn all_realms_for_one_tenant_share_one_group_and_split_candidates_fail_closed() {
    let first = replica_set(99);
    let second = replica_set(99);
    assert_eq!(first.group, second.group);

    let scope = scope();
    let manifest = |revision, hash| AuthzRealmTransferManifest {
        format: anvil_store::AUTHZ_REALM_TRANSFER_MANIFEST_FORMAT,
        scope: scope.clone(),
        revision: AuthzRevision(revision),
        encoded_bytes: 1,
        content_hash: [hash; 32],
    };
    let left = Some(AuthzRealmReplicaCandidate {
        manifest: manifest(3, 1),
        predecessor_revision: Some(AuthzRevision(2)),
        mutation_fingerprint: Some([1; 32]),
    });
    let sibling = Some(AuthzRealmReplicaCandidate {
        manifest: manifest(3, 2),
        predecessor_revision: Some(AuthzRevision(2)),
        mutation_fingerprint: Some([2; 32]),
    });
    let newer = Some(AuthzRealmReplicaCandidate {
        manifest: manifest(4, 3),
        predecessor_revision: Some(AuthzRevision(3)),
        mutation_fingerprint: Some([3; 32]),
    });
    assert!(exact_quorum_candidate(&[&left, &sibling, &newer], 2).is_err());
    assert_eq!(
        exact_quorum_candidate(&[&left, &left, &sibling], 2).unwrap(),
        left
    );
}
