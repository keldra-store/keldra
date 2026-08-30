//! Sparse definition delivery, assignment transfer, and local index selection.
//!
//! The source journal and ordinary definition objects remain authoritative.
//! This module keeps only disposable assignment projections and never scans
//! unrelated object heads.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::io::Read;
use std::time::Duration;

use keldra_consensus::{DecisionRaft, NodeId};
use keldra_store::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionCheckpoint, DefinitionConsumerKind, DefinitionDeletion, DefinitionKind,
    DefinitionLocator, DefinitionLocatorCursor, DefinitionLocatorPage, DefinitionOperation,
    DefinitionTransition, JournalRoute, LocalChange, MAX_DEFINITION_STATE_SCAN_RECORDS,
    PlacementLogId, RoutedJournalError, SourceId, Store, VersionId,
};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::ClusterPeerTransport;
use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::DataPeerTransport;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::catalog::{CatalogDefinition, CatalogIdentity, IndexCatalog};
use super::events::MAX_INDEX_EVENT_PAGE_BYTES;
use super::placement::{IndexIdentity, IndexPlacement};

#[path = "coordination/assignment_recovery.rs"]
mod assignment_recovery;
#[path = "coordination/delivery_checkpoint.rs"]
mod delivery_checkpoint;
mod reconcile;
use assignment_recovery::AssignmentInventoryRecovery;
use delivery_checkpoint::{
    DeliveryProgress, advance_assignment_checkpoints, commit_delivery_progress,
    require_membership_assignment_baseline,
};

const DELIVERY_IDLE_INTERVAL: Duration = Duration::from_millis(100);
const DELIVERY_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const MEMBERSHIP_POLL_INTERVAL: Duration = Duration::from_millis(500);
const ASSIGNMENT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const MAX_DEFINITION_BYTES: u64 = 64 * 1024 * 1024;
const LOCATOR_MERGE_SOURCE_PAGE: u32 = 64;

#[derive(Clone)]
pub(crate) struct ClusterDefinitionLocatorScanner {
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    peers: DataPeerTransport,
}

impl ClusterDefinitionLocatorScanner {
    pub(crate) fn new(
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: DataPeerTransport,
    ) -> Self {
        Self {
            local_node,
            decisions,
            store,
            peers,
        }
    }

    pub(crate) fn begin_bucket(
        &self,
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<ClusterDefinitionLocatorScan, Status> {
        if tenant_id == 0 || bucket_id == 0 {
            return Err(Status::invalid_argument(
                "definition locator bucket identity must be non-zero",
            ));
        }
        let placement = current_placement(&self.decisions)?;
        let fence = placement.fence();
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| {
                        Status::unavailable("ACTIVE definition locator source has no address")
                    })?
                    .0
                    .clone();
                Ok(LocatorSource {
                    node,
                    address,
                    cursor: None,
                    buffered: VecDeque::new(),
                    exhausted: false,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(ClusterDefinitionLocatorScan {
            scanner: self.clone(),
            scope: LocatorScanScope::Bucket {
                kind,
                tenant_id,
                bucket_id,
            },
            fence,
            sources,
            finished: false,
        })
    }

    fn begin_kind(
        &self,
        kind: DefinitionKind,
        placement: &ClusterPlacement,
    ) -> Result<ClusterDefinitionLocatorScan, Status> {
        let fence = placement.fence();
        let sources = placement
            .active_node_ids()
            .into_iter()
            .map(|node| {
                let address = placement
                    .address(node)
                    .ok_or_else(|| {
                        Status::unavailable("ACTIVE definition locator source has no address")
                    })?
                    .0
                    .clone();
                Ok(LocatorSource {
                    node,
                    address,
                    cursor: None,
                    buffered: VecDeque::new(),
                    exhausted: false,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        Ok(ClusterDefinitionLocatorScan {
            scanner: self.clone(),
            scope: LocatorScanScope::Kind(kind),
            fence,
            sources,
            finished: false,
        })
    }
}

struct LocatorSource {
    node: NodeId,
    address: String,
    cursor: Option<DefinitionLocatorCursor>,
    buffered: VecDeque<DefinitionLocator>,
    exhausted: bool,
}

pub(crate) struct ClusterDefinitionLocatorScan {
    scanner: ClusterDefinitionLocatorScanner,
    scope: LocatorScanScope,
    fence: PlacementLogId,
    sources: Vec<LocatorSource>,
    finished: bool,
}

#[derive(Clone, Copy)]
enum LocatorScanScope {
    Kind(DefinitionKind),
    Bucket {
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
    },
}

impl ClusterDefinitionLocatorScan {
    /// Return one bounded, globally ordered page. Replicated copies are
    /// adjacent by canonical definition path and collapse to the highest
    /// observed ordinary-object version before the caller exact-reads it.
    pub(crate) async fn next_page(
        &mut self,
        limit: usize,
    ) -> Result<Option<Vec<DefinitionLocator>>, Status> {
        if self.finished {
            return Ok(None);
        }
        let limit = limit.min(MAX_DEFINITION_STATE_SCAN_RECORDS as usize);
        if limit == 0 {
            return Err(Status::invalid_argument(
                "definition locator merge page limit must be positive",
            ));
        }
        let mut output = Vec::with_capacity(limit);
        while output.len() < limit {
            for index in 0..self.sources.len() {
                if self.fill_source(index).await? == LocatorSourceFill::YieldQuantum {
                    require_placement(&self.scanner.decisions, self.fence)?;
                    return Ok(Some(output));
                }
            }
            let Some(next_key) = self
                .sources
                .iter()
                .filter_map(|source| source.buffered.front())
                .min_by(|left, right| locator_sort_key(left).cmp(&locator_sort_key(right)))
                .map(|locator| (locator.tenant_id, locator.bucket_id, locator.path.clone()))
            else {
                require_placement(&self.scanner.decisions, self.fence)?;
                self.finished = true;
                break;
            };
            let mut selected: Option<DefinitionLocator> = None;
            for source in &mut self.sources {
                if source.buffered.front().is_some_and(|locator| {
                    locator.tenant_id == next_key.0
                        && locator.bucket_id == next_key.1
                        && locator.path == next_key.2
                }) {
                    let locator = source.buffered.pop_front().expect("front was present");
                    select_replica_locator(&mut selected, locator)?;
                }
            }
            output.push(selected.expect("a minimum locator key had a source"));
        }
        require_placement(&self.scanner.decisions, self.fence)?;
        if output.is_empty() && self.finished {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }

    async fn fill_source(&mut self, index: usize) -> Result<LocatorSourceFill, Status> {
        if self.sources[index].exhausted || !self.sources[index].buffered.is_empty() {
            return Ok(LocatorSourceFill::Ready);
        }
        let node = self.sources[index].node;
        let address = self.sources[index].address.clone();
        let cursor = self.sources[index].cursor.clone();
        let page = if node == self.scanner.local_node {
            let store = self.scanner.store.clone();
            let scope = self.scope;
            tokio::task::spawn_blocking(move || match scope {
                LocatorScanScope::Kind(kind) => store.scan_definition_locators(
                    Some(kind),
                    cursor.as_ref(),
                    LOCATOR_MERGE_SOURCE_PAGE,
                ),
                LocatorScanScope::Bucket {
                    kind,
                    tenant_id,
                    bucket_id,
                } => store.scan_definition_locators_by_bucket(
                    kind,
                    tenant_id,
                    bucket_id,
                    cursor.as_ref(),
                    LOCATOR_MERGE_SOURCE_PAGE,
                ),
            })
            .await
            .map_err(join_status)?
            .map_err(internal_status)?
        } else {
            match self.scope {
                LocatorScanScope::Kind(kind) => {
                    self.scanner
                        .peers
                        .scan_definition_locators_by_kind(
                            node,
                            &address,
                            kind,
                            cursor.as_ref(),
                            LOCATOR_MERGE_SOURCE_PAGE,
                        )
                        .await?
                }
                LocatorScanScope::Bucket {
                    kind,
                    tenant_id,
                    bucket_id,
                } => {
                    self.scanner
                        .peers
                        .scan_definition_locators_by_bucket(
                            node,
                            &address,
                            kind,
                            tenant_id,
                            bucket_id,
                            cursor.as_ref(),
                            LOCATOR_MERGE_SOURCE_PAGE,
                        )
                        .await?
                }
            }
        };
        let fill = install_locator_page(&mut self.sources[index], page);
        require_placement(&self.scanner.decisions, self.fence)?;
        Ok(fill)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocatorSourceFill {
    Ready,
    YieldQuantum,
}

fn install_locator_page(
    source: &mut LocatorSource,
    page: DefinitionLocatorPage,
) -> LocatorSourceFill {
    source.cursor = page.next_cursor;
    source.exhausted = source.cursor.is_none();
    source.buffered = page.locators.into();
    if source.buffered.is_empty() && !source.exhausted {
        LocatorSourceFill::YieldQuantum
    } else {
        LocatorSourceFill::Ready
    }
}

fn locator_sort_key(locator: &DefinitionLocator) -> (u64, u64, &str) {
    (locator.tenant_id, locator.bucket_id, &locator.path)
}

fn select_replica_locator(
    selected: &mut Option<DefinitionLocator>,
    candidate: DefinitionLocator,
) -> Result<(), Status> {
    match selected {
        None => *selected = Some(candidate),
        Some(current) if candidate.object_version > current.object_version => {
            *current = candidate;
        }
        Some(current)
            if candidate.object_version == current.object_version && candidate != *current =>
        {
            return Err(Status::data_loss(
                "definition locator replicas disagree at the same object version",
            ));
        }
        Some(_) => {}
    }
    Ok(())
}

pub(crate) struct DefinitionObject {
    pub(crate) object_version: VersionId,
    pub(crate) bytes: Vec<u8>,
}

/// Exact-read one ordinary definition through stable numeric identity. Mutable
/// names never participate in placement; the placeholder names only satisfy
/// `ObjectKey`'s public structural validation.
pub(crate) async fn load_assigned_definition_object(
    reader: &ClusterObjectReader,
    assignment: &DefinitionAssignment,
) -> Result<Option<DefinitionObject>, Status> {
    assignment
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    load_definition_object(
        reader,
        assignment.tenant_id,
        assignment.bucket_id,
        &assignment.definition_path,
        assignment.object_version,
    )
    .await
}

pub(crate) async fn load_definition_locator_object(
    reader: &ClusterObjectReader,
    locator: &DefinitionLocator,
) -> Result<Option<DefinitionObject>, Status> {
    locator
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    load_definition_object(
        reader,
        locator.tenant_id,
        locator.bucket_id,
        &locator.path,
        locator.object_version,
    )
    .await
}

/// Verify that a locator still names the authoritative current live or deleted
/// definition version without reconstructing its opaque payload. The selected
/// owner reads payload bytes only when installing a live definition.
pub(crate) async fn definition_reference_matches(
    reader: &ClusterObjectReader,
    tenant_id: u64,
    bucket_id: u64,
    definition_path: &str,
    object_version: VersionId,
    operation: DefinitionOperation,
) -> Result<bool, Status> {
    let key = keldra_store::ObjectKey::new("system", "definitions", definition_path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    Ok(reader
        .current_head_snapshot_stable(&key, tenant_id, bucket_id)
        .await?
        .is_some_and(|current| {
            current.version.id == object_version
                && current.version.deleted == (operation == DefinitionOperation::Delete)
        }))
}

pub(crate) async fn load_definition_object(
    reader: &ClusterObjectReader,
    tenant_id: u64,
    bucket_id: u64,
    definition_path: &str,
    object_version: VersionId,
) -> Result<Option<DefinitionObject>, Status> {
    let key = keldra_store::ObjectKey::new("system", "definitions", definition_path)
        .map_err(|error| Status::data_loss(error.to_string()))?;
    let Some(opened) = reader
        .open_current_stable(&key, tenant_id, bucket_id)
        .await?
    else {
        return Ok(None);
    };
    if opened.version.deleted || opened.version.id != object_version {
        return Ok(None);
    }
    let Some(payload) = opened.payload else {
        return Err(Status::data_loss("live assigned definition has no payload"));
    };
    let mut bytes = Vec::new();
    payload
        .take(MAX_DEFINITION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| Status::internal(format!("read assigned definition: {error}")))?;
    if bytes.len() as u64 > MAX_DEFINITION_BYTES {
        return Err(Status::resource_exhausted(
            "assigned definition exceeds the runtime bound",
        ));
    }
    Ok(Some(DefinitionObject {
        object_version: opened.version.id,
        bytes,
    }))
}

pub(crate) struct DefinitionCoordinationTask {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl DefinitionCoordinationTask {
    pub(crate) fn start(
        local_node: NodeId,
        decisions: DecisionRaft,
        store: Store,
        peers: DataPeerTransport,
        cluster_peers: ClusterPeerTransport,
        reader: ClusterObjectReader,
        catalog: IndexCatalog,
    ) -> Self {
        let mut tasks = Vec::new();
        for kind in [DefinitionKind::Index, DefinitionKind::Accounting] {
            tasks.push(tokio::spawn(run_source_delivery(
                kind,
                local_node,
                decisions.clone(),
                store.clone(),
                peers.clone(),
                cluster_peers.clone(),
                reader.clone(),
            )));
        }
        tasks.push(tokio::spawn(run_membership_transfer(
            local_node,
            decisions.clone(),
            store.clone(),
            peers,
            cluster_peers,
            reader.clone(),
        )));
        tasks.push(tokio::spawn(run_index_assignments(
            local_node, decisions, store, reader, catalog,
        )));
        Self { tasks }
    }
}

impl Drop for DefinitionCoordinationTask {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn run_source_delivery(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    peers: DataPeerTransport,
    cluster_peers: ClusterPeerTransport,
    reader: ClusterObjectReader,
) {
    let mut progress = DeliveryProgress::default();
    loop {
        match deliver_source_page(
            kind,
            local_node,
            &decisions,
            &store,
            &peers,
            &cluster_peers,
            &reader,
            &mut progress,
        )
        .await
        {
            Ok(true) => tokio::task::yield_now().await,
            Ok(false) => tokio::time::sleep(DELIVERY_IDLE_INTERVAL).await,
            Err(error) => {
                tracing::warn!(definition.kind = ?kind, %error, "sparse definition delivery will retry");
                tokio::time::sleep(DELIVERY_RETRY_INTERVAL).await;
            }
        }
    }
}

async fn deliver_source_page(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    reader: &ClusterObjectReader,
    progress: &mut DeliveryProgress,
) -> Result<bool, Status> {
    let status = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || store.local_watch_status())
            .await
            .map_err(join_status)?
            .map_err(internal_status)?
    };
    let placement = current_placement(decisions)?;
    if !placement.active_node_ids().contains(&local_node) {
        return Err(Status::failed_precondition(
            "local definition source is not ACTIVE",
        ));
    }
    // Normal delivery must not create a current-fence assignment checkpoint
    // before membership reconciliation has made that assignment inventory
    // complete. The derived-retention runtime treats these checkpoints as its
    // inventory barrier, so advancing early could release source history for a
    // definition which has not reached its new rank-zero owner yet.
    require_membership_assignment_baseline(kind, &placement, store).await?;
    if progress.reset_required(status.source_id, placement.fence(), status.retention_floor) {
        let after_offset = initialize_delivery_epoch(
            kind,
            local_node,
            decisions,
            store,
            peers,
            cluster_peers,
            reader,
            &placement,
            status.source_id,
            status.retention_floor,
            status.settled_through,
        )
        .await?;
        commit_delivery_progress(
            kind,
            local_node,
            decisions,
            &placement,
            store,
            peers,
            status.source_id,
            after_offset,
            progress,
        )
        .await?;
    }
    if progress.after_offset >= status.settled_through {
        return Ok(false);
    }

    let route = JournalRoute::Definition(kind);
    let source = status.source_id;
    let after = progress.after_offset;
    let target = status.settled_through;
    let page = {
        let scan_store = store.clone();
        let result = tokio::task::spawn_blocking(move || {
            scan_store.scan_routed_local_changes(
                route,
                source,
                after,
                target,
                MAX_DEFINITION_STATE_SCAN_RECORDS as usize,
                MAX_INDEX_EVENT_PAGE_BYTES,
            )
        })
        .await
        .map_err(join_status)?;
        match result {
            Ok(page) => page,
            Err(error) if routed_failure_requires_reconciliation(&error) => {
                let through = reconcile_delivery_epoch(
                    kind,
                    local_node,
                    decisions,
                    store,
                    peers,
                    cluster_peers,
                    reader,
                    &placement,
                    source,
                )
                .await?;
                commit_delivery_progress(
                    kind, local_node, decisions, &placement, store, peers, source, through,
                    progress,
                )
                .await?;
                return Ok(true);
            }
            Err(error) => return Err(Status::out_of_range(error.to_string())),
        }
    };
    if page.source_id != source
        || page.through_offset <= after
        || page.through_offset > target
        || page.oversize.is_some()
    {
        return Err(Status::data_loss(
            "sparse definition route returned invalid advancement",
        ));
    }
    for change in &page.changes {
        let transition = definition_transition(kind, change)?;
        deliver_transition(
            local_node,
            &placement,
            store,
            peers,
            cluster_peers,
            source,
            change.offset(),
            transition,
            &mut progress.destination_next,
        )
        .await?;
    }
    // A routed page proves every skipped source position irrelevant to this
    // definition kind. Publish that proof to every ACTIVE derived consumer
    // only after all assignment mutations in the page are durable there.
    advance_assignment_checkpoints(
        kind,
        local_node,
        &placement,
        store,
        peers,
        source,
        page.through_offset,
        &mut progress.destination_next,
    )
    .await?;
    require_placement(decisions, placement.fence())?;
    persist_delivery_checkpoint(kind, store, source, page.through_offset, placement.fence())
        .await?;
    progress.after_offset = page.through_offset;
    Ok(true)
}

async fn initialize_delivery_epoch(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
    source: SourceId,
    retention_floor: u64,
    tail: u64,
) -> Result<u64, Status> {
    let checkpoint = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.definition_checkpoint(delivery_consumer_kind(kind), source.node_id)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    let plan = source_delivery_start(source, placement.fence(), retention_floor, tail, checkpoint)?;
    if plan.requires_inventory {
        return reconcile_delivery_epoch(
            kind,
            local_node,
            decisions,
            store,
            peers,
            cluster_peers,
            reader,
            placement,
            source,
        )
        .await;
    }
    require_placement(decisions, placement.fence())?;
    Ok(plan.resume_after)
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_delivery_epoch(
    kind: DefinitionKind,
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
    source: SourceId,
) -> Result<u64, Status> {
    let reconciled_through = reconcile::reconcile_kind(
        kind,
        local_node,
        decisions,
        store,
        peers,
        cluster_peers,
        reader,
        placement,
    )
    .await?;
    let through = reconciled_through
        .get(&source.node_id)
        .copied()
        .ok_or_else(|| Status::data_loss("reconciliation omitted the local source"))?;
    require_placement(decisions, placement.fence())?;
    Ok(through)
}

fn routed_failure_requires_reconciliation(error: &RoutedJournalError) -> bool {
    matches!(
        error,
        RoutedJournalError::CursorExpired { .. }
            | RoutedJournalError::MissingPrimary { .. }
            | RoutedJournalError::RouteMismatch { .. }
    )
}

async fn persist_delivery_checkpoint(
    kind: DefinitionKind,
    store: &Store,
    source: SourceId,
    through_offset: u64,
    fence: PlacementLogId,
) -> Result<(), Status> {
    let checkpoint = delivery_checkpoint(kind, source, through_offset, fence)?;
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.apply_definition_assignment_page(&[], &checkpoint))
        .await
        .map_err(join_status)?
        .map_err(internal_status)
}

fn delivery_checkpoint(
    kind: DefinitionKind,
    source: SourceId,
    through_offset: u64,
    fence: PlacementLogId,
) -> Result<DefinitionCheckpoint, Status> {
    Ok(DefinitionCheckpoint {
        consumer_kind: delivery_consumer_kind(kind),
        source_id: source,
        next_offset: through_offset
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("definition delivery offset exhausted"))?,
        observed_fence: fence,
    })
}

#[derive(Debug, Eq, PartialEq)]
struct SourceDeliveryStart {
    requires_inventory: bool,
    resume_after: u64,
}

fn source_delivery_start(
    source: SourceId,
    fence: PlacementLogId,
    retention_floor: u64,
    tail: u64,
    checkpoint: Option<DefinitionCheckpoint>,
) -> Result<SourceDeliveryStart, Status> {
    let tail_next = tail
        .checked_add(1)
        .ok_or_else(|| Status::resource_exhausted("definition source tail exhausted"))?;
    let matching = checkpoint.filter(|checkpoint| checkpoint.source_id == source);
    if matching.is_some_and(|checkpoint| fence_after(checkpoint.observed_fence, fence)) {
        return Err(Status::data_loss(
            "definition delivery checkpoint is from a future placement",
        ));
    }
    let resume_after = match matching {
        Some(checkpoint) if checkpoint.next_offset > 0 && checkpoint.next_offset <= tail_next => {
            checkpoint.next_offset - 1
        }
        Some(_) => {
            return Err(Status::data_loss(
                "definition delivery checkpoint is outside the source journal",
            ));
        }
        None => 0,
    };
    let requires_inventory = resume_after < retention_floor;
    Ok(SourceDeliveryStart {
        requires_inventory,
        resume_after: if requires_inventory {
            retention_floor
        } else {
            resume_after
        },
    })
}

fn definition_transition(
    expected_kind: DefinitionKind,
    change: &LocalChange,
) -> Result<&DefinitionTransition, Status> {
    let LocalChange::ObjectHead(head) = change else {
        return Err(Status::data_loss(
            "definition route referenced a non-object change",
        ));
    };
    let transition = head
        .definition_transition
        .as_ref()
        .ok_or_else(|| Status::data_loss("definition route referenced an untyped object change"))?;
    transition
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if transition.kind != expected_kind
        || transition.tenant_id != head.tenant_id
        || transition.bucket_id != head.bucket_id
        || transition.path != head.exact_path
        || transition.object_version != head.path_version
    {
        return Err(Status::data_loss(
            "definition transition disagrees with its source event",
        ));
    }
    Ok(transition)
}

#[allow(clippy::too_many_arguments)]
async fn deliver_transition(
    local_node: NodeId,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    source: SourceId,
    source_offset: u64,
    transition: &DefinitionTransition,
    destination_next: &mut BTreeMap<NodeId, u64>,
) -> Result<(), Status> {
    let assignment = assignment_placement(
        transition.tenant_id,
        transition.bucket_id,
        transition.definition_id,
        placement,
    )?;
    let consumer_kind = consumer_kind(transition.kind);
    for (rank, destination) in assignment.query_replicas().iter().copied().enumerate() {
        let next = match destination_next.get(&destination).copied() {
            Some(next) => next,
            None => {
                let checkpoint = read_destination_checkpoint(
                    local_node,
                    destination,
                    placement,
                    store,
                    peers,
                    consumer_kind,
                    source,
                )
                .await?;
                let next = checkpoint.map_or(0, |checkpoint| checkpoint.next_offset);
                destination_next.insert(destination, next);
                next
            }
        };
        if next > source_offset {
            continue;
        }
        let mutation = transition_mutation(transition, placement.fence(), rank as u8);
        let checkpoint = DefinitionCheckpoint {
            consumer_kind,
            source_id: source,
            next_offset: source_offset
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("definition source offset exhausted"))?,
            observed_fence: placement.fence(),
        };
        apply_assignment_page(
            local_node,
            destination,
            placement,
            store,
            peers,
            std::slice::from_ref(&mutation),
            checkpoint,
        )
        .await?;
        destination_next.insert(destination, checkpoint.next_offset);
    }
    if transition.kind == DefinitionKind::Accounting {
        invalidate_accounting_matcher_bucket(
            cluster_peers,
            placement,
            transition.tenant_id,
            transition.bucket_id,
        )
        .await?;
    }
    Ok(())
}

async fn invalidate_accounting_matcher_bucket(
    peers: &ClusterPeerTransport,
    placement: &ClusterPlacement,
    tenant_id: u64,
    bucket_id: u64,
) -> Result<(), Status> {
    let target = crate::accounting::matcher_node(placement, tenant_id, bucket_id)?;
    let address = placement
        .address(target)
        .ok_or_else(|| Status::unavailable("accounting matcher has no peer address"))?;
    peers
        .invalidate_accounting_matcher_bucket(
            target,
            &address.0,
            tenant_id,
            bucket_id,
            placement.fence(),
        )
        .await
}

async fn clear_accounting_matcher_caches(
    peers: &ClusterPeerTransport,
    placement: &ClusterPlacement,
) -> Result<(), Status> {
    for target in placement.active_node_ids() {
        let address = placement.address(target).ok_or_else(|| {
            Status::unavailable("ACTIVE accounting matcher cache target has no peer address")
        })?;
        peers
            .clear_accounting_matcher_cache(target, &address.0, placement.fence())
            .await?;
    }
    Ok(())
}

async fn read_destination_checkpoint(
    local_node: NodeId,
    destination: NodeId,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    consumer_kind: DefinitionConsumerKind,
    source: SourceId,
) -> Result<Option<DefinitionCheckpoint>, Status> {
    let checkpoint = if destination == local_node {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.definition_checkpoint(consumer_kind, source.node_id)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    } else {
        let address = placement.address(destination).ok_or_else(|| {
            Status::unavailable("definition assignment destination has no peer address")
        })?;
        peers
            .definition_checkpoint(destination, &address.0, consumer_kind, source.node_id)
            .await?
    };
    let Some(checkpoint) = checkpoint else {
        return Ok(None);
    };
    if checkpoint.consumer_kind != consumer_kind
        || checkpoint.source_id.node_id != source.node_id
        || fence_after(checkpoint.observed_fence, placement.fence())
    {
        return Err(Status::data_loss(
            "definition destination checkpoint has invalid identity or fence",
        ));
    }
    if checkpoint.source_id.source_epoch != source.source_epoch {
        return Ok(None);
    }
    Ok(Some(checkpoint))
}

async fn apply_assignment_page(
    local_node: NodeId,
    destination: NodeId,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    mutations: &[DefinitionAssignmentMutation],
    checkpoint: DefinitionCheckpoint,
) -> Result<(), Status> {
    if destination == local_node {
        let store = store.clone();
        let mutations = mutations.to_vec();
        tokio::task::spawn_blocking(move || {
            store.apply_definition_assignment_page(&mutations, &checkpoint)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)
    } else {
        let address = placement.address(destination).ok_or_else(|| {
            Status::unavailable("definition assignment destination has no peer address")
        })?;
        peers
            .apply_definition_assignment_page(destination, &address.0, mutations, checkpoint)
            .await
    }
}

fn transition_mutation(
    transition: &DefinitionTransition,
    fence: PlacementLogId,
    rank: u8,
) -> DefinitionAssignmentMutation {
    match transition.operation {
        DefinitionOperation::Upsert => DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
            kind: transition.kind,
            tenant_id: transition.tenant_id,
            bucket_id: transition.bucket_id,
            definition_id: transition.definition_id,
            definition_path: transition.path.clone(),
            object_version: transition.object_version,
            observed_fence: fence,
            rank,
        }),
        DefinitionOperation::Delete => DefinitionAssignmentMutation::Delete(DefinitionDeletion {
            kind: transition.kind,
            tenant_id: transition.tenant_id,
            bucket_id: transition.bucket_id,
            definition_id: transition.definition_id,
            definition_path: transition.path.clone(),
            object_version: transition.object_version,
            observed_fence: fence,
            rank,
        }),
    }
}

async fn run_membership_transfer(
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    peers: DataPeerTransport,
    cluster_peers: ClusterPeerTransport,
    reader: ClusterObjectReader,
) {
    let mut progress = MembershipTransferProgress::default();
    loop {
        let placement = match current_placement(&decisions) {
            Ok(placement) => placement,
            Err(error) => {
                tracing::warn!(%error, "definition assignment membership is unavailable");
                tokio::time::sleep(DELIVERY_RETRY_INTERVAL).await;
                continue;
            }
        };
        if progress.is_complete(placement.fence()) {
            tokio::time::sleep(MEMBERSHIP_POLL_INTERVAL).await;
            continue;
        }
        let reconcile = if is_membership_reconciliation_coordinator(local_node, &placement) {
            match membership_reconciliation_required(&store, placement.fence()).await {
                Ok(required) => required,
                Err(error) => {
                    tracing::warn!(%error, "definition membership reconciliation marker is invalid");
                    tokio::time::sleep(DELIVERY_RETRY_INTERVAL).await;
                    continue;
                }
            }
        } else {
            false
        };
        match apply_membership_change(
            local_node,
            &decisions,
            &store,
            &peers,
            &cluster_peers,
            &reader,
            &placement,
            reconcile,
        )
        .await
        {
            Ok(()) => progress.complete(placement.fence()),
            Err(error) => {
                tracing::warn!(%error, "definition assignment membership transfer will retry");
                tokio::time::sleep(DELIVERY_RETRY_INTERVAL).await;
            }
        }
    }
}

#[derive(Default)]
struct MembershipTransferProgress {
    completed_fence: Option<PlacementLogId>,
}

impl MembershipTransferProgress {
    fn is_complete(&self, fence: PlacementLogId) -> bool {
        self.completed_fence == Some(fence)
    }

    fn complete(&mut self, fence: PlacementLogId) {
        self.completed_fence = Some(fence);
    }
}

async fn membership_reconciliation_required(
    store: &Store,
    current: PlacementLogId,
) -> Result<bool, Status> {
    let store = store.clone();
    let completed = tokio::task::spawn_blocking(move || store.definition_reconciliation_fence())
        .await
        .map_err(join_status)?
        .map_err(internal_status)?;
    match completed {
        None => Ok(true),
        Some(fence) if fence == current => Ok(false),
        Some(fence) if fence_after(fence, current) => Err(Status::data_loss(
            "definition reconciliation marker is from a future placement",
        )),
        Some(_) => Ok(true),
    }
}

async fn mark_membership_reconciliation_complete(
    store: &Store,
    fence: PlacementLogId,
) -> Result<(), Status> {
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.complete_definition_reconciliation(fence))
        .await
        .map_err(join_status)?
        .map_err(internal_status)
}

#[allow(clippy::too_many_arguments)]
async fn apply_membership_change(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    cluster_peers: &ClusterPeerTransport,
    reader: &ClusterObjectReader,
    placement: &ClusterPlacement,
    reconcile_locators: bool,
) -> Result<(), Status> {
    transfer_assignments(local_node, decisions, store, peers, placement).await?;
    if reconcile_locators {
        for kind in [DefinitionKind::Index, DefinitionKind::Accounting] {
            reconcile::reconcile_kind(
                kind,
                local_node,
                decisions,
                store,
                peers,
                cluster_peers,
                reader,
                placement,
            )
            .await?;
        }
        require_placement(decisions, placement.fence())?;
        mark_membership_reconciliation_complete(store, placement.fence()).await?;
    }
    require_placement(decisions, placement.fence())
}

fn is_membership_reconciliation_coordinator(
    local_node: NodeId,
    placement: &ClusterPlacement,
) -> bool {
    // Reconciliation already inventories every ACTIVE source, so one stable
    // coordinator avoids multiplying that bounded work by cluster size.
    placement.active_node_ids().into_iter().min() == Some(local_node)
}

async fn transfer_assignments(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    peers: &DataPeerTransport,
    placement: &ClusterPlacement,
) -> Result<(), Status> {
    for kind in [DefinitionKind::Index, DefinitionKind::Accounting] {
        let mut cursor: Option<DefinitionAssignmentCursor> = None;
        loop {
            let page = {
                let store = store.clone();
                let cursor = cursor.clone();
                tokio::task::spawn_blocking(move || {
                    store.scan_definition_assignments_by_kind(
                        kind,
                        cursor.as_ref(),
                        MAX_DEFINITION_STATE_SCAN_RECORDS,
                    )
                })
                .await
                .map_err(join_status)?
                .map_err(internal_status)?
            };
            let mut by_destination = BTreeMap::<NodeId, Vec<DefinitionAssignmentMutation>>::new();
            for existing in page.assignments {
                let owners = assignment_placement(
                    existing.tenant_id,
                    existing.bucket_id,
                    existing.definition_id,
                    placement,
                )?;
                if assignment_matches_placement(local_node, &existing, &owners, placement.fence()) {
                    continue;
                }
                for (rank, destination) in owners.query_replicas().iter().copied().enumerate() {
                    by_destination.entry(destination).or_default().push(
                        DefinitionAssignmentMutation::Upsert(DefinitionAssignment {
                            observed_fence: placement.fence(),
                            rank: rank as u8,
                            ..existing.clone()
                        }),
                    );
                }
                if !owners.query_replicas().contains(&local_node) {
                    by_destination.entry(local_node).or_default().push(
                        DefinitionAssignmentMutation::Remove {
                            kind: existing.kind,
                            tenant_id: existing.tenant_id,
                            bucket_id: existing.bucket_id,
                            definition_id: existing.definition_id,
                            object_version: existing.object_version,
                            observed_fence: placement.fence(),
                        },
                    );
                }
            }

            // Apply the local fence/rank update last. If the process stops
            // after a peer accepted its page, retry is harmless. If it stops
            // before every peer accepted, the still-old local record causes
            // the bounded page to be retransmitted after restart.
            let local_mutations = by_destination.remove(&local_node);
            for (destination, mutations) in by_destination {
                apply_assignment_transfer(
                    local_node,
                    destination,
                    placement,
                    store,
                    peers,
                    &mutations,
                )
                .await?;
            }
            if let Some(mutations) = local_mutations {
                apply_assignment_transfer(
                    local_node, local_node, placement, store, peers, &mutations,
                )
                .await?;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            require_placement(decisions, placement.fence())?;
        }
    }
    require_placement(decisions, placement.fence())
}

fn assignment_matches_placement(
    local_node: NodeId,
    existing: &DefinitionAssignment,
    owners: &IndexPlacement,
    fence: PlacementLogId,
) -> bool {
    owners
        .rank_of(local_node)
        .is_some_and(|rank| existing.observed_fence == fence && existing.rank == rank)
}

async fn apply_assignment_transfer(
    local_node: NodeId,
    destination: NodeId,
    placement: &ClusterPlacement,
    store: &Store,
    peers: &DataPeerTransport,
    mutations: &[DefinitionAssignmentMutation],
) -> Result<(), Status> {
    if destination == local_node {
        let store = store.clone();
        let mutations = mutations.to_vec();
        tokio::task::spawn_blocking(move || store.apply_definition_assignment_mutations(&mutations))
            .await
            .map_err(join_status)?
            .map_err(internal_status)
    } else {
        let address = placement.address(destination).ok_or_else(|| {
            Status::unavailable("definition assignment destination has no peer address")
        })?;
        peers
            .apply_definition_assignments(destination, &address.0, mutations)
            .await
    }
}

async fn run_index_assignments(
    local_node: NodeId,
    decisions: DecisionRaft,
    store: Store,
    reader: ClusterObjectReader,
    catalog: IndexCatalog,
) {
    let mut changes = store.subscribe_definition_assignment_changes();
    let mut recovery = Some(AssignmentInventoryRecovery::startup());
    loop {
        let recovery_at = recovery.as_ref().map(|recovery| recovery.due);
        let recovery_due = async move {
            match recovery_at {
                Some(due) => tokio::time::sleep_until(due).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            received = changes.recv() => match received {
                Ok(mutations) => {
                    for mutation in mutations {
                        if mutation.kind() != DefinitionKind::Index {
                            continue;
                        }
                        let identity = mutation_identity(&mutation);
                        let result = match &mutation {
                            DefinitionAssignmentMutation::Delete(deletion) => catalog.delete_wait(
                                CatalogIdentity { tenant_id: identity.0, bucket_id: identity.1, index_id: identity.2 },
                                deletion.object_version.0,
                            ).await,
                            _ => refresh_index_assignment(local_node, &decisions, &store, &reader, &catalog, identity).await,
                        };
                        if let Err(error) = result {
                            tracing::warn!(definition.id = identity.2, %error, "assigned index refresh will retry");
                            recovery = Some(AssignmentInventoryRecovery::retry());
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    recovery = Some(AssignmentInventoryRecovery::immediate());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            _ = recovery_due => {
                let cursor = recovery.as_ref().and_then(|recovery| recovery.cursor.as_ref());
                match scan_index_assignment_page(
                    local_node,
                    &decisions,
                    &store,
                    &reader,
                    &catalog,
                    cursor,
                ).await {
                    Ok(next) => {
                        // Normal source changes are delivered by the sparse
                        // derived-consumer wake path. A completed inventory is
                        // not revisited until a notification gap, membership
                        // recovery, or retry explicitly requires it.
                        let continued = next.is_some();
                        recovery = AssignmentInventoryRecovery::after_page(next);
                        if continued {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "paged assigned index inventory will retry");
                        let cursor = recovery.take().and_then(|recovery| recovery.cursor);
                        recovery = Some(AssignmentInventoryRecovery::retry_from(cursor));
                    }
                }
            }
        }
    }
}

async fn scan_index_assignment_page(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    reader: &ClusterObjectReader,
    catalog: &IndexCatalog,
    cursor: Option<&DefinitionAssignmentCursor>,
) -> Result<Option<DefinitionAssignmentCursor>, Status> {
    let page = {
        let store = store.clone();
        let cursor = cursor.cloned();
        tokio::task::spawn_blocking(move || {
            store.scan_definition_assignments_by_kind(
                DefinitionKind::Index,
                cursor.as_ref(),
                MAX_DEFINITION_STATE_SCAN_RECORDS,
            )
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    for assignment in page.assignments {
        if assignment.rank != 0 {
            continue;
        }
        match load_index_assignment(local_node, decisions, reader, &assignment).await {
            Ok(Some(definition)) => catalog.upsert(definition)?,
            Ok(None) => remove_stale_assignment(store, &assignment).await?,
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) =>
            {
                return Err(error);
            }
            Err(error) => tracing::warn!(
                definition.id = assignment.definition_id,
                %error,
                "invalid assigned index definition was isolated"
            ),
        }
    }
    Ok(page.next_cursor)
}

async fn refresh_index_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    store: &Store,
    reader: &ClusterObjectReader,
    catalog: &IndexCatalog,
    identity: (u64, u64, u64),
) -> Result<(), Status> {
    catalog.remove(identity.0, identity.1, identity.2)?;
    let assignment = {
        let store = store.clone();
        tokio::task::spawn_blocking(move || {
            store.definition_assignment(DefinitionKind::Index, identity.0, identity.1, identity.2)
        })
        .await
        .map_err(join_status)?
        .map_err(internal_status)?
    };
    let Some(assignment) = assignment else {
        return Ok(());
    };
    if assignment.rank != 0 {
        return Ok(());
    }
    match load_index_assignment(local_node, decisions, reader, &assignment).await? {
        Some(definition) => catalog.upsert(definition)?,
        None => remove_stale_assignment(store, &assignment).await?,
    }
    Ok(())
}

async fn remove_stale_assignment(
    store: &Store,
    assignment: &DefinitionAssignment,
) -> Result<(), Status> {
    let assignment = assignment.clone();
    let store = store.clone();
    tokio::task::spawn_blocking(move || store.remove_definition_assignment_if_matches(&assignment))
        .await
        .map_err(join_status)?
        .map(|_| ())
        .map_err(internal_status)
}

pub(crate) async fn load_index_assignment(
    local_node: NodeId,
    decisions: &DecisionRaft,
    reader: &ClusterObjectReader,
    assignment: &DefinitionAssignment,
) -> Result<Option<CatalogDefinition>, Status> {
    assignment
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if assignment.kind != DefinitionKind::Index {
        return Ok(None);
    }
    let placement = current_placement(decisions)?;
    let owners = assignment_placement(
        assignment.tenant_id,
        assignment.bucket_id,
        assignment.definition_id,
        &placement,
    )?;
    if owners.rank_of(local_node) != Some(assignment.rank)
        || assignment.observed_fence != placement.fence()
    {
        return Ok(None);
    }
    let Some(opened) = load_assigned_definition_object(reader, assignment).await? else {
        return Ok(None);
    };
    let stored = StoredIndexDefinition::decode(&opened.bytes)?;
    if stored.index_id != assignment.definition_id
        || definition_path(&stored.name)? != assignment.definition_path
    {
        return Err(Status::data_loss(
            "assigned index identity disagrees with the ordinary definition",
        ));
    }
    require_placement(decisions, placement.fence())?;
    Ok(Some(CatalogDefinition::new(
        assignment.tenant_id,
        assignment.bucket_id,
        opened.object_version.0,
        stored,
    )?))
}

fn mutation_identity(mutation: &DefinitionAssignmentMutation) -> (u64, u64, u64) {
    match mutation {
        DefinitionAssignmentMutation::Upsert(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Delete(value) => {
            (value.tenant_id, value.bucket_id, value.definition_id)
        }
        DefinitionAssignmentMutation::Remove {
            tenant_id,
            bucket_id,
            definition_id,
            ..
        } => (*tenant_id, *bucket_id, *definition_id),
    }
}

fn assignment_placement(
    tenant_id: u64,
    bucket_id: u64,
    _definition_id: u64,
    placement: &ClusterPlacement,
) -> Result<IndexPlacement, Status> {
    let identity = IndexIdentity::projection_partition(tenant_id, bucket_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    IndexPlacement::derive(identity, placement)
        .map_err(|error| Status::unavailable(error.to_string()))
}

fn consumer_kind(kind: DefinitionKind) -> DefinitionConsumerKind {
    match kind {
        DefinitionKind::Index => DefinitionConsumerKind::IndexAssignments,
        DefinitionKind::Accounting => DefinitionConsumerKind::AccountingAssignments,
    }
}

fn delivery_consumer_kind(kind: DefinitionKind) -> DefinitionConsumerKind {
    match kind {
        DefinitionKind::Index => DefinitionConsumerKind::IndexDelivery,
        DefinitionKind::Accounting => DefinitionConsumerKind::AccountingDelivery,
    }
}

pub(crate) fn current_placement(decisions: &DecisionRaft) -> Result<ClusterPlacement, Status> {
    let state = decisions
        .state()
        .map_err(|_| Status::unavailable("applied cluster membership is unavailable"))?;
    ClusterPlacement::from_applied(&state).map_err(|error| Status::unavailable(error.to_string()))
}

fn require_placement(decisions: &DecisionRaft, expected: PlacementLogId) -> Result<(), Status> {
    if current_placement(decisions)?.fence() == expected {
        Ok(())
    } else {
        Err(Status::unavailable(
            "cluster placement changed during definition coordination",
        ))
    }
}

fn fence_after(left: PlacementLogId, right: PlacementLogId) -> bool {
    (left.term, left.index) > (right.term, right.index)
}

fn join_status(error: tokio::task::JoinError) -> Status {
    Status::internal(format!("definition coordination task failed: {error}"))
}

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_delivery_is_typed_and_ranked() {
        let transition = DefinitionTransition {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id: 2,
            definition_id: 3,
            path: definition_path("one").unwrap(),
            object_version: VersionId(4),
            operation: DefinitionOperation::Upsert,
        };
        let mutation = transition_mutation(&transition, PlacementLogId { term: 5, index: 6 }, 2);
        let DefinitionAssignmentMutation::Upsert(assignment) = mutation else {
            panic!("expected assignment upsert");
        };
        assert_eq!(assignment.definition_id, 3);
        assert_eq!(assignment.object_version, VersionId(4));
        assert_eq!(assignment.rank, 2);
    }

    #[test]
    fn deletion_never_carries_a_synthetic_assignment_path() {
        let transition = DefinitionTransition {
            kind: DefinitionKind::Accounting,
            tenant_id: 1,
            bucket_id: 2,
            definition_id: 3,
            path: "_keldra/accounting/3/definition.json".into(),
            object_version: VersionId(5),
            operation: DefinitionOperation::Delete,
        };
        assert!(matches!(
            transition_mutation(&transition, PlacementLogId { term: 6, index: 7 }, 0,),
            DefinitionAssignmentMutation::Delete(DefinitionDeletion {
                definition_id: 3,
                object_version: VersionId(5),
                rank: 0,
                ..
            })
        ));
    }

    #[test]
    fn locator_merge_order_matches_the_persisted_cross_bucket_key_order() {
        let mut locators = [
            DefinitionLocator {
                kind: DefinitionKind::Index,
                tenant_id: 2,
                bucket_id: 1,
                definition_id: 1,
                path: "a".into(),
                object_version: VersionId(1),
                operation: DefinitionOperation::Upsert,
            },
            DefinitionLocator {
                kind: DefinitionKind::Index,
                tenant_id: 1,
                bucket_id: 2,
                definition_id: 2,
                path: "a".into(),
                object_version: VersionId(1),
                operation: DefinitionOperation::Upsert,
            },
            DefinitionLocator {
                kind: DefinitionKind::Index,
                tenant_id: 1,
                bucket_id: 1,
                definition_id: 3,
                path: "z".into(),
                object_version: VersionId(1),
                operation: DefinitionOperation::Upsert,
            },
        ];
        locators.sort_by(|left, right| locator_sort_key(left).cmp(&locator_sort_key(right)));
        assert_eq!(
            locators
                .iter()
                .map(|locator| (locator.tenant_id, locator.bucket_id, locator.path.as_str()))
                .collect::<Vec<_>>(),
            [(1, 1, "z"), (1, 2, "a"), (2, 1, "a")]
        );
    }

    #[test]
    fn locator_replicas_cannot_disagree_at_one_persisted_key_and_version() {
        let mut selected = Some(DefinitionLocator {
            kind: DefinitionKind::Index,
            tenant_id: 1,
            bucket_id: 2,
            definition_id: 3,
            path: definition_path("example").unwrap(),
            object_version: VersionId(4),
            operation: DefinitionOperation::Upsert,
        });
        let error = select_replica_locator(
            &mut selected,
            DefinitionLocator {
                kind: DefinitionKind::Index,
                tenant_id: 1,
                bucket_id: 2,
                definition_id: 9,
                path: definition_path("example").unwrap(),
                object_version: VersionId(4),
                operation: DefinitionOperation::Upsert,
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn empty_nonterminal_locator_page_yields_the_current_merge_quantum() {
        let cursor = DefinitionLocatorCursor::from_bytes(vec![1, b'L', 1]).unwrap();
        let mut source = LocatorSource {
            node: NodeId(1),
            address: "unused".into(),
            cursor: None,
            buffered: VecDeque::new(),
            exhausted: false,
        };
        assert_eq!(
            install_locator_page(
                &mut source,
                DefinitionLocatorPage {
                    locators: Vec::new(),
                    next_cursor: Some(cursor.clone()),
                },
            ),
            LocatorSourceFill::YieldQuantum
        );
        assert_eq!(source.cursor, Some(cursor));
        assert!(source.buffered.is_empty());
        assert!(!source.exhausted);
    }

    #[tokio::test]
    async fn failed_exact_revalidation_removes_a_delayed_pre_delete_transfer() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(keldra_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let fence = PlacementLogId { term: 7, index: 8 };
        let assignment = DefinitionAssignment {
            kind: DefinitionKind::Index,
            tenant_id: 2,
            bucket_id: 3,
            definition_id: 4,
            definition_path: definition_path("example").unwrap(),
            object_version: VersionId(5),
            observed_fence: fence,
            rank: 0,
        };
        store
            .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Remove {
                kind: assignment.kind,
                tenant_id: assignment.tenant_id,
                bucket_id: assignment.bucket_id,
                definition_id: assignment.definition_id,
                object_version: VersionId(6),
                observed_fence: fence,
            }])
            .unwrap();

        // A cursorless membership transfer captured before the delete can
        // arrive afterward because ASSIGNED intentionally has no tombstones.
        store
            .apply_definition_assignment_mutations(&[DefinitionAssignmentMutation::Upsert(
                assignment.clone(),
            )])
            .unwrap();
        assert!(
            store
                .definition_assignment(DefinitionKind::Index, 2, 3, 4)
                .unwrap()
                .is_some()
        );

        // The runtime exact-reads the ordinary definition before catalog use;
        // a missing/deleted/version-mismatched object drives this removal.
        assert!(
            store
                .remove_definition_assignment_if_matches(&assignment)
                .unwrap()
        );
        assert!(
            store
                .definition_assignment(DefinitionKind::Index, 2, 3, 4)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn clean_restart_uses_the_source_cursor_without_consulting_non_owners() {
        let source = SourceId {
            node_id: 1,
            source_epoch: [2; 32],
        };
        let plan = source_delivery_start(
            source,
            PlacementLogId { term: 3, index: 4 },
            40,
            50,
            Some(DefinitionCheckpoint {
                consumer_kind: DefinitionConsumerKind::IndexDelivery,
                source_id: source,
                next_offset: 47,
                observed_fence: PlacementLogId { term: 2, index: 9 },
            }),
        )
        .unwrap();
        assert_eq!(
            plan,
            SourceDeliveryStart {
                requires_inventory: false,
                resume_after: 46,
            }
        );
    }

    #[test]
    fn missing_cursor_at_retained_beginning_replays_without_inventory() {
        let source = SourceId {
            node_id: 1,
            source_epoch: [2; 32],
        };
        let plan = source_delivery_start(source, PlacementLogId { term: 3, index: 4 }, 0, 50, None)
            .unwrap();
        assert_eq!(
            plan,
            SourceDeliveryStart {
                requires_inventory: false,
                resume_after: 0,
            }
        );
    }

    #[test]
    fn missing_or_pruned_cursor_reconciles_then_replays_the_retained_suffix() {
        let source = SourceId {
            node_id: 1,
            source_epoch: [2; 32],
        };
        for checkpoint in [
            None,
            Some(DefinitionCheckpoint {
                consumer_kind: DefinitionConsumerKind::IndexDelivery,
                source_id: source,
                next_offset: 20,
                observed_fence: PlacementLogId { term: 3, index: 4 },
            }),
        ] {
            let plan = source_delivery_start(
                source,
                PlacementLogId { term: 3, index: 4 },
                40,
                50,
                checkpoint,
            )
            .unwrap();
            assert_eq!(
                plan,
                SourceDeliveryStart {
                    requires_inventory: true,
                    resume_after: 40,
                }
            );
        }
    }

    #[test]
    fn retained_route_evidence_loss_triggers_true_gap_reconciliation() {
        assert!(routed_failure_requires_reconciliation(
            &RoutedJournalError::MissingPrimary { offset: 12 }
        ));
        assert!(routed_failure_requires_reconciliation(
            &RoutedJournalError::RouteMismatch { offset: 12 }
        ));
        assert!(routed_failure_requires_reconciliation(
            &RoutedJournalError::CursorExpired {
                cursor: 3,
                retention_floor: 4,
            }
        ));
        assert!(!routed_failure_requires_reconciliation(
            &RoutedJournalError::Storage("temporary".into())
        ));
    }

    #[test]
    fn successful_reconciliation_checkpoint_resumes_after_its_source_tail() {
        let source = SourceId {
            node_id: 2,
            source_epoch: [3; 32],
        };
        let checkpoint = delivery_checkpoint(
            DefinitionKind::Index,
            source,
            41,
            PlacementLogId { term: 5, index: 6 },
        )
        .unwrap();
        assert_eq!(checkpoint.source_id, source);
        assert_eq!(checkpoint.next_offset, 42);
        assert_eq!(
            checkpoint.consumer_kind,
            DefinitionConsumerKind::IndexDelivery
        );
    }

    #[test]
    fn completed_assignment_inventory_has_no_periodic_revisit() {
        assert!(AssignmentInventoryRecovery::after_page(None).is_none());

        let cursor = DefinitionAssignmentCursor::from_bytes(vec![1, b'A', 1]).unwrap();
        let continued = AssignmentInventoryRecovery::after_page(Some(cursor.clone())).unwrap();
        assert_eq!(continued.cursor, Some(cursor));
        assert!(continued.due <= tokio::time::Instant::now());
    }

    #[tokio::test]
    async fn durable_membership_marker_drives_first_start_restart_and_future_rejection() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(keldra_store::StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let current = PlacementLogId { term: 3, index: 8 };
        assert!(
            membership_reconciliation_required(&store, current)
                .await
                .unwrap()
        );
        store.complete_definition_reconciliation(current).unwrap();
        assert!(
            !membership_reconciliation_required(&store, current)
                .await
                .unwrap()
        );

        let later = PlacementLogId { term: 3, index: 9 };
        assert!(
            membership_reconciliation_required(&store, later)
                .await
                .unwrap()
        );
        store.complete_definition_reconciliation(later).unwrap();
        assert_eq!(
            membership_reconciliation_required(&store, current)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }
}
