use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use keldra_store::{
    LocalChange, ObjectHeadChange, ObjectHeadChangeKind, RetainedVersionDeletedChange, VersionId,
};

use super::*;

#[derive(Clone)]
struct MutablePlacement(Arc<RwLock<WatchPlacement>>);

impl MutablePlacement {
    fn new(placement: WatchPlacement) -> Self {
        Self(Arc::new(RwLock::new(placement)))
    }

    fn set(&self, placement: WatchPlacement) {
        *self.0.write().unwrap() = placement;
    }
}

impl WatchPlacementAuthority for MutablePlacement {
    fn current(&self) -> Result<WatchPlacement, String> {
        Ok(self.0.read().unwrap().clone())
    }
}

#[derive(Clone)]
struct SequencedPlacement {
    placements: Arc<Mutex<Vec<WatchPlacement>>>,
}

impl SequencedPlacement {
    fn new(placements: Vec<WatchPlacement>) -> Self {
        Self {
            placements: Arc::new(Mutex::new(placements)),
        }
    }
}

impl WatchPlacementAuthority for SequencedPlacement {
    fn current(&self) -> Result<WatchPlacement, String> {
        let mut placements = self.placements.lock().unwrap();
        if placements.len() > 1 {
            Ok(placements.remove(0))
        } else {
            Ok(placements[0].clone())
        }
    }
}

#[derive(Clone)]
struct Journal {
    epoch: [u8; 32],
    floor: u64,
    changes: Vec<LocalChange>,
    delay: Duration,
    failure: Option<WatchSourceError>,
}

impl Journal {
    fn status(&self, node: NodeId) -> WatchJournalStatus {
        let tail = self
            .changes
            .last()
            .map(LocalChange::offset)
            .unwrap_or(self.floor);
        WatchJournalStatus {
            source_id: SourceId {
                node_id: u16::try_from(node.0).unwrap(),
                source_epoch: self.epoch,
            },
            tail,
            settled_through: tail,
            retention_floor: self.floor,
            retained_entries: tail - self.floor,
            retained_bytes: 0,
        }
    }
}

#[derive(Clone, Default)]
struct MemorySources {
    journals: Arc<RwLock<BTreeMap<NodeId, Journal>>>,
}

impl MemorySources {
    fn insert(&self, node: NodeId, delay_ms: u64) {
        self.journals.write().unwrap().insert(
            node,
            Journal {
                epoch: [node.0 as u8; 32],
                floor: 0,
                changes: Vec::new(),
                delay: Duration::from_millis(delay_ms),
                failure: None,
            },
        );
    }

    fn append_head(&self, node: NodeId, path: &str, kind: ObjectHeadChangeKind) {
        let mut journals = self.journals.write().unwrap();
        let journal = journals.get_mut(&node).unwrap();
        let offset = journal
            .changes
            .last()
            .map(LocalChange::offset)
            .unwrap_or(journal.floor)
            + 1;
        journal
            .changes
            .push(LocalChange::ObjectHead(ObjectHeadChange {
                offset,
                tenant_id: 11,
                bucket_id: 22,
                exact_path: path.into(),
                path_version: VersionId(100 + offset),
                kind,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }));
    }

    fn fail(&self, node: NodeId, error: WatchSourceError) {
        self.journals
            .write()
            .unwrap()
            .get_mut(&node)
            .unwrap()
            .failure = Some(error);
    }

    fn set_epoch(&self, node: NodeId, epoch: [u8; 32]) {
        self.journals.write().unwrap().get_mut(&node).unwrap().epoch = epoch;
    }

    fn set_floor(&self, node: NodeId, floor: u64) {
        let mut journals = self.journals.write().unwrap();
        let journal = journals.get_mut(&node).unwrap();
        journal.floor = floor;
        journal
            .changes
            .retain(|change| change.offset() > journal.floor);
    }

    fn snapshot(&self, node: NodeId) -> Result<Journal, WatchSourceError> {
        self.journals
            .read()
            .unwrap()
            .get(&node)
            .cloned()
            .ok_or_else(|| WatchSourceError::Unavailable("source is absent".into()))
    }
}

#[tonic::async_trait]
impl ClusterWatchSources for MemorySources {
    async fn status(
        &self,
        target: NodeId,
        _address: &str,
        membership_revision: PlacementLogId,
        _bearer: OriginalBearer,
        _scope: DistributedWatchScope,
    ) -> Result<WatchSourceStatus, WatchSourceError> {
        let journal = self.snapshot(target)?;
        if let Some(error) = journal.failure {
            return Err(error);
        }
        Ok(WatchSourceStatus {
            source_node: target,
            membership_revision,
            status: journal.status(target),
        })
    }

    async fn read_page(
        &self,
        target: NodeId,
        _address: &str,
        _bearer: OriginalBearer,
        query: WatchSourceQuery,
    ) -> Result<WatchSourcePage, WatchSourceError> {
        let journal = self.snapshot(target)?;
        if let Some(error) = journal.failure.clone() {
            return Err(error);
        }
        tokio::time::sleep(journal.delay).await;
        let status = journal.status(target);
        if query.next_offset <= status.retention_floor {
            return Err(WatchSourceError::ResumeExpired);
        }
        let after_tail = status.tail + 1;
        let next_offset = query
            .next_offset
            .saturating_add(query.max_records as u64)
            .min(after_tail);
        let represented = journal
            .changes
            .into_iter()
            .filter(|change| change.offset() >= query.next_offset && change.offset() < next_offset)
            .collect();
        Ok(WatchSourcePage {
            source_node: target,
            membership_revision: query.membership_revision,
            status,
            next_offset,
            object_heads: filter_public_changes(&query.scope, represented),
        })
    }
}

#[derive(Default)]
struct MemoryCodec {
    state: Mutex<CodecState>,
}

#[derive(Default)]
struct CodecState {
    next: u64,
    tokens: HashMap<Vec<u8>, WatchCheckpointClaims>,
}

impl WatchCheckpointCodec for MemoryCodec {
    fn seal(&self, claims: &WatchCheckpointClaims) -> Result<Vec<u8>, String> {
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let token = format!("opaque-watch-token-{}", state.next).into_bytes();
        state.tokens.insert(token.clone(), claims.clone());
        Ok(token)
    }

    fn open(&self, token: &[u8]) -> Result<WatchCheckpointClaims, String> {
        self.state
            .lock()
            .unwrap()
            .tokens
            .get(token)
            .cloned()
            .ok_or_else(|| "unknown or modified token".into())
    }
}

fn revision(index: u64) -> PlacementLogId {
    PlacementLogId { term: 4, index }
}

fn placement(index: u64, nodes: &[u64]) -> WatchPlacement {
    WatchPlacement::new(
        ClusterId([9; 16]),
        revision(index),
        nodes
            .iter()
            .map(|node| (NodeId(*node), format!("node-{node}:50052")))
            .collect(),
    )
    .unwrap()
}

fn scope(prefix: &str) -> DistributedWatchScope {
    DistributedWatchScope::new(
        &WatchScope::new("tenant", "bucket", prefix).unwrap(),
        11,
        22,
    )
    .unwrap()
}

fn bearer() -> OriginalBearer {
    OriginalBearer::from_signed_token("test-bearer")
}

fn watch(
    placement: Arc<dyn WatchPlacementAuthority>,
    sources: Arc<MemorySources>,
    codec: Arc<MemoryCodec>,
) -> DistributedWatch {
    DistributedWatch::new(placement, sources, codec).with_page_size(8)
}

#[tokio::test]
async fn retained_beginning_starts_at_each_sources_retention_floor() {
    let placement = Arc::new(MutablePlacement::new(placement(10, &[1, 2])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 0);
    sources.insert(NodeId(2), 0);
    sources.append_head(NodeId(1), "old", ObjectHeadChangeKind::Put);
    sources.append_head(NodeId(1), "retained", ObjectHeadChangeKind::Put);
    sources.append_head(NodeId(2), "other", ObjectHeadChangeKind::Put);
    sources.set_floor(NodeId(1), 1);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(placement, sources, codec.clone());

    let checkpoint = watch
        .start_retained_beginning(scope(""), bearer())
        .await
        .unwrap();
    let claims = codec.open(&checkpoint).unwrap();
    assert_eq!(claims.sources[0].next_offset, 2);
    assert_eq!(claims.sources[1].next_offset, 1);

    let batch = watch
        .poll_once(scope(""), &checkpoint, bearer())
        .await
        .unwrap();
    let mut paths = batch
        .invalidations
        .iter()
        .map(|invalidation| invalidation.key.path())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    assert_eq!(paths, ["other", "retained"]);
}

#[tokio::test]
async fn checkpoint_resumes_on_another_ingress_and_delivery_is_at_least_once() {
    let placement = Arc::new(MutablePlacement::new(placement(10, &[1, 2, 3])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 30);
    sources.insert(NodeId(2), 5);
    sources.insert(NodeId(3), 15);
    let codec = Arc::new(MemoryCodec::default());
    let first_ingress = watch(placement.clone(), sources.clone(), codec.clone());
    let checkpoint = first_ingress
        .start_now(scope("docs"), bearer())
        .await
        .unwrap();

    sources.append_head(NodeId(1), "docs/slow", ObjectHeadChangeKind::Put);
    sources.append_head(NodeId(2), "docs/fast", ObjectHeadChangeKind::Delete);
    sources.append_head(NodeId(3), "docs/_keldra/private", ObjectHeadChangeKind::Put);

    let second_ingress = watch(placement, sources, codec);
    let batch = second_ingress
        .poll_once(scope("docs"), &checkpoint, bearer())
        .await
        .unwrap();
    assert_eq!(
        batch
            .invalidations
            .iter()
            .map(|event| event.key.path())
            .collect::<Vec<_>>(),
        ["docs/fast", "docs/slow"]
    );
    assert_eq!(
        batch.invalidations[0].state_hint,
        InvalidationStateHint::Deleted
    );

    // Reusing the last durably stored checkpoint replays the same events.
    let replay = second_ingress
        .poll_once(scope("docs"), &checkpoint, bearer())
        .await
        .unwrap();
    assert_eq!(replay.invalidations, batch.invalidations);

    // The newly returned vector represents every source, including the source
    // whose only event was hidden by the reserved namespace filter.
    let caught_up = second_ingress
        .poll_once(scope("docs"), &batch.checkpoint, bearer())
        .await
        .unwrap();
    assert!(caught_up.invalidations.is_empty());
}

#[tokio::test]
async fn one_required_source_failure_never_returns_a_partial_batch() {
    let placement = Arc::new(MutablePlacement::new(placement(10, &[1, 2])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 1);
    sources.insert(NodeId(2), 1);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(placement, sources.clone(), codec);
    let checkpoint = watch.start_now(scope(""), bearer()).await.unwrap();
    sources.append_head(NodeId(1), "visible", ObjectHeadChangeKind::Put);
    sources.fail(
        NodeId(2),
        WatchSourceError::Unavailable("peer is down".into()),
    );

    assert_eq!(
        watch
            .poll_once(scope(""), &checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::SourceUnavailable {
            node_id: Some(NodeId(2)),
            message: "peer is down".into(),
        }
    );
}

#[tokio::test]
async fn source_epoch_or_retention_floor_loss_expires_resume() {
    let placement = Arc::new(MutablePlacement::new(placement(10, &[1])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 1);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(placement, sources.clone(), codec);
    let epoch_checkpoint = watch.start_now(scope(""), bearer()).await.unwrap();
    sources.set_epoch(NodeId(1), [77; 32]);
    assert_eq!(
        watch
            .poll_once(scope(""), &epoch_checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::ResumeExpired
    );

    sources.set_epoch(NodeId(1), [1; 32]);
    sources.append_head(NodeId(1), "one", ObjectHeadChangeKind::Put);
    let floor_checkpoint = watch.start_now(scope(""), bearer()).await.unwrap();
    sources.append_head(NodeId(1), "two", ObjectHeadChangeKind::Put);
    sources.append_head(NodeId(1), "three", ObjectHeadChangeKind::Put);
    sources.set_floor(NodeId(1), 2);
    assert_eq!(
        watch
            .poll_once(scope(""), &floor_checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::ResumeExpired
    );
}

#[tokio::test]
async fn membership_change_requires_evidence_for_every_new_required_source() {
    let authority = Arc::new(MutablePlacement::new(placement(10, &[1, 2])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 0);
    sources.insert(NodeId(2), 0);
    sources.insert(NodeId(3), 0);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(authority.clone(), sources, codec.clone());
    let original = watch.start_now(scope(""), bearer()).await.unwrap();

    // Reweight/same-source cutover has complete vector evidence.
    authority.set(placement(11, &[1, 2]));
    let reweighted = watch
        .poll_once(scope(""), &original, bearer())
        .await
        .unwrap();
    let reweighted_claims = codec.open(&reweighted.checkpoint).unwrap();
    assert_eq!(reweighted_claims.membership_revision, revision(11));

    // Removal also has evidence for every source still required.
    authority.set(placement(12, &[1]));
    let removed = watch
        .poll_once(scope(""), &reweighted.checkpoint, bearer())
        .await
        .unwrap();
    assert_eq!(codec.open(&removed.checkpoint).unwrap().sources.len(), 1);

    // Addition has no cursor/source-epoch evidence for the new source.
    authority.set(placement(13, &[1, 3]));
    assert_eq!(
        watch
            .poll_once(scope(""), &removed.checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::ResumeExpired
    );
}

#[tokio::test]
async fn tokens_are_opaque_integrity_checked_and_bound_to_scope_and_cluster() {
    let authority = Arc::new(MutablePlacement::new(placement(10, &[1])));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 0);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(authority.clone(), sources, codec);
    let checkpoint = watch.start_now(scope("docs"), bearer()).await.unwrap();

    let mut modified = checkpoint.clone();
    modified.push(b'x');
    assert_eq!(
        watch
            .poll_once(scope("docs"), &modified, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::InvalidCheckpoint
    );
    assert_eq!(
        watch
            .poll_once(scope("other"), &checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::InvalidCheckpoint
    );

    authority.set(
        WatchPlacement::new(
            ClusterId([8; 16]),
            revision(10),
            BTreeMap::from([(NodeId(1), "node-1:50052".into())]),
        )
        .unwrap(),
    );
    assert_eq!(
        watch
            .poll_once(scope("docs"), &checkpoint, bearer())
            .await
            .unwrap_err(),
        DistributedWatchError::InvalidCheckpoint
    );
}

#[tokio::test]
async fn cutover_during_collection_fails_instead_of_returning_old_partial_state() {
    let old = placement(10, &[1]);
    let new = placement(11, &[1]);
    let authority = Arc::new(SequencedPlacement::new(vec![old, new]));
    let sources = Arc::new(MemorySources::default());
    sources.insert(NodeId(1), 0);
    let codec = Arc::new(MemoryCodec::default());
    let watch = watch(authority, sources, codec);

    assert_eq!(
        watch.start_now(scope(""), bearer()).await.unwrap_err(),
        DistributedWatchError::MembershipChanged
    );
}

#[test]
fn public_filter_keeps_only_matching_object_heads() {
    let changes = vec![
        LocalChange::ObjectHead(ObjectHeadChange {
            offset: 1,
            tenant_id: 11,
            bucket_id: 22,
            exact_path: "docs/visible".into(),
            path_version: VersionId(1),
            kind: ObjectHeadChangeKind::Put,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        }),
        LocalChange::RetainedVersionDeleted(RetainedVersionDeletedChange {
            offset: 2,
            tenant_id: 11,
            bucket_id: 22,
            exact_path: "docs/old".into(),
            deleted_version: VersionId(1),
            resulting_head_version: Some(VersionId(2)),
            reference_deltas: Vec::new(),
            accounting_transition: None,
        }),
        LocalChange::ObjectHead(ObjectHeadChange {
            offset: 3,
            tenant_id: 11,
            bucket_id: 22,
            exact_path: "docs/_keldra/meta.json".into(),
            path_version: VersionId(3),
            kind: ObjectHeadChangeKind::Put,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        }),
        LocalChange::ObjectHead(ObjectHeadChange {
            offset: 4,
            tenant_id: 99,
            bucket_id: 22,
            exact_path: "docs/other-tenant".into(),
            path_version: VersionId(4),
            kind: ObjectHeadChangeKind::Put,
            reference_deltas: Vec::new(),
            accounting_transition: None,
            definition_transition: None,
        }),
    ];

    let filtered = filter_public_changes(&scope("docs"), changes);
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].exact_path, "docs/visible");
    assert_eq!(filtered[0].offset, 1);
}

#[tokio::test]
async fn malformed_filtered_page_fails_closed() {
    #[derive(Clone)]
    struct BadSource;

    #[tonic::async_trait]
    impl ClusterWatchSources for BadSource {
        async fn status(
            &self,
            target: NodeId,
            _address: &str,
            revision: PlacementLogId,
            _bearer: OriginalBearer,
            _scope: DistributedWatchScope,
        ) -> Result<WatchSourceStatus, WatchSourceError> {
            Ok(WatchSourceStatus {
                source_node: target,
                membership_revision: revision,
                status: WatchJournalStatus {
                    source_id: SourceId {
                        node_id: target.0 as u16,
                        source_epoch: [1; 32],
                    },
                    tail: 0,
                    settled_through: 0,
                    retention_floor: 0,
                    retained_entries: 0,
                    retained_bytes: 0,
                },
            })
        }

        async fn read_page(
            &self,
            target: NodeId,
            _address: &str,
            _bearer: OriginalBearer,
            query: WatchSourceQuery,
        ) -> Result<WatchSourcePage, WatchSourceError> {
            Ok(WatchSourcePage {
                source_node: target,
                membership_revision: query.membership_revision,
                status: WatchJournalStatus {
                    source_id: query.expected_source,
                    tail: 1,
                    settled_through: 1,
                    retention_floor: 0,
                    retained_entries: 1,
                    retained_bytes: 0,
                },
                next_offset: 2,
                object_heads: vec![ObjectHeadChange {
                    offset: 1,
                    tenant_id: 11,
                    bucket_id: 22,
                    exact_path: "outside".into(),
                    path_version: VersionId(1),
                    kind: ObjectHeadChangeKind::Put,
                    reference_deltas: Vec::new(),
                    accounting_transition: None,
                    definition_transition: None,
                }],
            })
        }
    }

    let authority = Arc::new(MutablePlacement::new(placement(10, &[1])));
    let codec = Arc::new(MemoryCodec::default());
    let good = Arc::new(MemorySources::default());
    good.insert(NodeId(1), 0);
    let checkpoint = watch(authority.clone(), good, codec.clone())
        .start_now(scope("docs"), bearer())
        .await
        .unwrap();
    let watch = DistributedWatch::new(authority, Arc::new(BadSource), codec);
    assert!(matches!(
        watch.poll_once(scope("docs"), &checkpoint, bearer()).await,
        Err(DistributedWatchError::InvalidSource {
            node_id: NodeId(1),
            ..
        })
    ));
}

#[test]
fn sparse_page_can_prove_a_large_irrelevant_offset_range() {
    let source = SourceId {
        node_id: 1,
        source_epoch: [1; 32],
    };
    let query = WatchSourceQuery {
        membership_revision: revision(10),
        expected_source: source,
        scope: scope("docs"),
        next_offset: 1,
        max_records: 1,
    };
    let page = WatchSourcePage {
        source_node: NodeId(1),
        membership_revision: revision(10),
        status: WatchJournalStatus {
            source_id: source,
            tail: 1_000_000,
            settled_through: 1_000_000,
            retention_floor: 0,
            retained_entries: 1_000_000,
            retained_bytes: 0,
        },
        next_offset: 1_000_001,
        object_heads: Vec::new(),
    };

    let validated = validate_page(NodeId(1), &query, page).unwrap();
    assert_eq!(validated.next_offset, 1_000_001);
    assert!(validated.invalidations.is_empty());
}

#[test]
fn public_page_cannot_advance_into_an_unsettled_source_suffix() {
    let source = SourceId {
        node_id: 1,
        source_epoch: [1; 32],
    };
    let query = WatchSourceQuery {
        membership_revision: revision(10),
        expected_source: source,
        scope: scope("docs"),
        next_offset: 2,
        max_records: 1,
    };
    let page = WatchSourcePage {
        source_node: NodeId(1),
        membership_revision: revision(10),
        status: WatchJournalStatus {
            source_id: source,
            tail: 2,
            settled_through: 1,
            retention_floor: 0,
            retained_entries: 2,
            retained_bytes: 0,
        },
        next_offset: 3,
        object_heads: Vec::new(),
    };

    assert!(matches!(
        validate_page(NodeId(1), &query, page),
        Err(DistributedWatchError::InvalidSource { .. })
    ));
}
