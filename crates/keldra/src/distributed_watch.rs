//! Transport-neutral cluster-wide public watch aggregation.
//!
//! The existing source-local journal remains the only event authority. This
//! module fans one bounded bucket-routed read out to every ACTIVE source,
//! validates the full response set, filters public output to object-head
//! invalidations, and then advances an opaque vector checkpoint. It persists
//! no cursor, acknowledgement, emission, or second log.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use keldra_consensus::{ClusterId, NodeId};
use keldra_store::{
    InvalidationStateHint, LocalChange, LocalInvalidation, MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
    ObjectHeadChange, ObjectHeadChangeKind, ObjectKey, PlacementLogId, SourceId,
    WatchJournalStatus, WatchScope,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authentication::Caller;

const CHECKPOINT_FORMAT: u16 = 1;
pub(crate) const CHECKPOINT_AUDIENCE: &str = "keldra-watch-checkpoint";
pub(crate) const CHECKPOINT_PURPOSE: &str = "keldra-watch-vector";
pub(crate) const MAX_WATCH_SOURCE_PAGE_BYTES: u64 = 64 * 1024 * 1024;

/// The canonical public scope plus the stable IDs used in source journals.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DistributedWatchScope {
    tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    prefix: String,
}

impl DistributedWatchScope {
    pub(crate) fn new(
        scope: &WatchScope,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Self, DistributedWatchError> {
        if tenant_id == 0 || bucket_id == 0 {
            return Err(DistributedWatchError::InvalidScope(
                "stable tenant and bucket IDs must be non-zero".into(),
            ));
        }
        Ok(Self {
            tenant: scope.tenant().to_owned(),
            bucket: scope.bucket().to_owned(),
            tenant_id,
            bucket_id,
            prefix: scope.prefix().to_owned(),
        })
    }

    fn contains_path(&self, path: &str) -> bool {
        self.prefix.is_empty()
            || path == self.prefix
            || path
                .strip_prefix(&self.prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    }

    pub(crate) fn tenant(&self) -> &str {
        &self.tenant
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) const fn tenant_id(&self) -> u64 {
        self.tenant_id
    }

    pub(crate) const fn bucket_id(&self) -> u64 {
        self.bucket_id
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// One ACTIVE membership view. The Raft placement fence is its revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchPlacement {
    cluster_id: ClusterId,
    membership_revision: PlacementLogId,
    sources: BTreeMap<NodeId, String>,
}

impl WatchPlacement {
    pub(crate) fn new(
        cluster_id: ClusterId,
        membership_revision: PlacementLogId,
        sources: BTreeMap<NodeId, String>,
    ) -> Result<Self, DistributedWatchError> {
        if cluster_id.0 == [0; 16] {
            return Err(DistributedWatchError::Placement(
                "cluster identity is unavailable".into(),
            ));
        }
        if membership_revision.term == 0 || membership_revision.index == 0 {
            return Err(DistributedWatchError::Placement(
                "ACTIVE membership revision is unavailable".into(),
            ));
        }
        if sources.is_empty() {
            return Err(DistributedWatchError::Placement(
                "ACTIVE membership contains no watch source".into(),
            ));
        }
        for (node, address) in &sources {
            if node.0 == 0 || u16::try_from(node.0).is_err() {
                return Err(DistributedWatchError::Placement(format!(
                    "ACTIVE watch source {} cannot be represented by SourceId",
                    node.0
                )));
            }
            if address.is_empty() {
                return Err(DistributedWatchError::Placement(format!(
                    "ACTIVE watch source {} has no peer address",
                    node.0
                )));
            }
        }
        Ok(Self {
            cluster_id,
            membership_revision,
            sources,
        })
    }

    fn source_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.sources.keys().copied()
    }
}

/// Supplies one fresh, applied ACTIVE membership view.
pub(crate) trait WatchPlacementAuthority: Send + Sync + 'static {
    fn current(&self) -> Result<WatchPlacement, String>;
}

/// Source-local read request. `next_offset` is the first journal position not
/// represented by the checkpoint, rather than the last position consumed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchSourceQuery {
    pub membership_revision: PlacementLogId,
    pub expected_source: SourceId,
    pub scope: DistributedWatchScope,
    pub next_offset: u64,
    pub max_records: usize,
}

/// Source identity and journal status under one exact ACTIVE membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WatchSourceStatus {
    pub source_node: NodeId,
    pub membership_revision: PlacementLogId,
    pub status: WatchJournalStatus,
}

/// A filtered bounded source page. Non-public journal variants are omitted,
/// but their positions are represented by `next_offset`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchSourcePage {
    pub source_node: NodeId,
    pub membership_revision: PlacementLogId,
    pub status: WatchJournalStatus,
    pub next_offset: u64,
    pub object_heads: Vec<ObjectHeadChange>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum WatchSourceError {
    #[error("RESUME_EXPIRED")]
    ResumeExpired,
    #[error("watch authorization was revoked")]
    AccessRevoked,
    #[error("{0}")]
    Unavailable(String),
}

/// Mandatory peer transport boundary. Its eventual adapter uses the existing
/// mTLS data-peer listener and the same local source-journal implementation.
#[tonic::async_trait]
pub(crate) trait ClusterWatchSources: Send + Sync + 'static {
    async fn status(
        &self,
        target: NodeId,
        address: &str,
        membership_revision: PlacementLogId,
        caller: Caller,
        scope: DistributedWatchScope,
    ) -> Result<WatchSourceStatus, WatchSourceError>;

    async fn read_page(
        &self,
        target: NodeId,
        address: &str,
        caller: Caller,
        query: WatchSourceQuery,
    ) -> Result<WatchSourcePage, WatchSourceError>;
}

/// Claims sealed by the existing JWT subsystem in the production adapter.
/// The core revalidates every semantic binding after opening the token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WatchCheckpointClaims {
    pub format: u16,
    pub aud: String,
    pub purpose: String,
    pub cluster_id: ClusterId,
    pub scope: DistributedWatchScope,
    pub membership_revision: PlacementLogId,
    pub sources: Vec<WatchVectorEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WatchVectorEntry {
    pub source: SourceId,
    pub next_offset: u64,
}

/// Integrity and opacity belong to the already-approved JWT mechanism. The
/// aggregator neither knows nor retains signing material.
pub(crate) trait WatchCheckpointCodec: Send + Sync + 'static {
    fn seal(&self, claims: &WatchCheckpointClaims) -> Result<Vec<u8>, String>;
    fn open(&self, token: &[u8]) -> Result<WatchCheckpointClaims, String>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DistributedWatchBatch {
    pub invalidations: Vec<LocalInvalidation>,
    pub checkpoint: Vec<u8>,
}

/// One stateless ingress aggregator. Any ingress using the same cluster JWT
/// verifier can resume a checkpoint; no cursor is kept on this node.
pub(crate) struct DistributedWatch {
    placement: Arc<dyn WatchPlacementAuthority>,
    sources: Arc<dyn ClusterWatchSources>,
    checkpoints: Arc<dyn WatchCheckpointCodec>,
    page_size: usize,
}

impl DistributedWatch {
    pub(crate) fn new(
        placement: Arc<dyn WatchPlacementAuthority>,
        sources: Arc<dyn ClusterWatchSources>,
        checkpoints: Arc<dyn WatchCheckpointCodec>,
    ) -> Self {
        Self {
            placement,
            sources,
            checkpoints,
            page_size: MAX_LOCAL_INVALIDATION_SCAN_RECORDS,
        }
    }

    #[cfg(test)]
    fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.clamp(1, MAX_LOCAL_INVALIDATION_SCAN_RECORDS);
        self
    }

    /// Capture every required ACTIVE source tail. No event preceding this
    /// checkpoint is part of a `Now` watch.
    pub(crate) async fn start_now(
        &self,
        scope: DistributedWatchScope,
        caller: Caller,
    ) -> Result<Vec<u8>, DistributedWatchError> {
        self.start(scope, caller, WatchInitialPosition::Now).await
    }

    /// Capture the first position still retained by every ACTIVE source.
    pub(crate) async fn start_retained_beginning(
        &self,
        scope: DistributedWatchScope,
        caller: Caller,
    ) -> Result<Vec<u8>, DistributedWatchError> {
        self.start(scope, caller, WatchInitialPosition::RetainedBeginning)
            .await
    }

    async fn start(
        &self,
        scope: DistributedWatchScope,
        caller: Caller,
        position: WatchInitialPosition,
    ) -> Result<Vec<u8>, DistributedWatchError> {
        let placement = self.current_placement()?;
        let mut tasks = tokio::task::JoinSet::new();
        for (node, address) in placement.sources.clone() {
            let sources = self.sources.clone();
            let revision = placement.membership_revision;
            let caller = caller.clone();
            let scope = scope.clone();
            tasks.spawn(async move {
                let status = sources
                    .status(node, &address, revision, caller, scope)
                    .await;
                (node, status)
            });
        }

        let mut cursors = BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (node, result) =
                joined.map_err(|error| DistributedWatchError::SourceUnavailable {
                    node_id: None,
                    message: format!("watch source task failed: {error}"),
                })?;
            let response = map_source_result(node, result)?;
            if response.source_node != node
                || response.membership_revision != placement.membership_revision
            {
                return Err(DistributedWatchError::InvalidSource {
                    node_id: node,
                    message: "source identity or membership revision differs from the request"
                        .into(),
                });
            }
            validate_status(node, &response.status)?;
            let cursor = match position {
                WatchInitialPosition::Now => response.status.settled_through,
                WatchInitialPosition::RetainedBeginning => response.status.retention_floor,
            };
            let next_offset =
                cursor
                    .checked_add(1)
                    .ok_or_else(|| DistributedWatchError::InvalidSource {
                        node_id: node,
                        message: "source journal cursor cannot advance".into(),
                    })?;
            if cursors
                .insert(
                    node,
                    WatchVectorEntry {
                        source: response.status.source_id,
                        next_offset,
                    },
                )
                .is_some()
            {
                return Err(DistributedWatchError::InvalidSource {
                    node_id: node,
                    message: "source responded more than once".into(),
                });
            }
        }
        require_complete_sources(&placement, cursors.keys().copied())?;
        self.require_unchanged(&placement)?;
        self.seal_checkpoint(&placement, scope, cursors.into_values().collect())
    }

    /// Read at most one bounded page from every source. Events are accumulated
    /// in source-completion order, deliberately imposing no cross-source order.
    /// The returned checkpoint is only constructed after every source succeeds.
    pub(crate) async fn poll_once(
        &self,
        scope: DistributedWatchScope,
        checkpoint: &[u8],
        caller: Caller,
    ) -> Result<DistributedWatchBatch, DistributedWatchError> {
        let placement = self.current_placement()?;
        let claims = self.open_checkpoint(checkpoint, &scope, placement.cluster_id)?;
        let cursors = resume_cursors(&placement, &claims)?;
        let mut tasks = tokio::task::JoinSet::new();
        for (node, address) in placement.sources.clone() {
            let cursor = cursors[&node];
            let sources = self.sources.clone();
            let caller = caller.clone();
            let query = WatchSourceQuery {
                membership_revision: placement.membership_revision,
                expected_source: cursor.source,
                scope: scope.clone(),
                next_offset: cursor.next_offset,
                max_records: self.page_size,
            };
            tasks.spawn(async move {
                let page = sources
                    .read_page(node, &address, caller, query.clone())
                    .await;
                (node, query, page)
            });
        }

        let mut next_cursors = BTreeMap::new();
        let mut invalidations = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            let (node, query, result) =
                joined.map_err(|error| DistributedWatchError::SourceUnavailable {
                    node_id: None,
                    message: format!("watch source task failed: {error}"),
                })?;
            let page = map_source_result(node, result)?;
            let events = validate_page(node, &query, page)?;
            let next = WatchVectorEntry {
                source: query.expected_source,
                next_offset: events.next_offset,
            };
            if next_cursors.insert(node, next).is_some() {
                return Err(DistributedWatchError::InvalidSource {
                    node_id: node,
                    message: "source responded more than once".into(),
                });
            }
            invalidations.extend(events.invalidations);
        }
        require_complete_sources(&placement, next_cursors.keys().copied())?;
        self.require_unchanged(&placement)?;
        let checkpoint =
            self.seal_checkpoint(&placement, scope, next_cursors.into_values().collect())?;
        Ok(DistributedWatchBatch {
            invalidations,
            checkpoint,
        })
    }

    fn current_placement(&self) -> Result<WatchPlacement, DistributedWatchError> {
        self.placement
            .current()
            .map_err(DistributedWatchError::Placement)
    }

    fn require_unchanged(&self, expected: &WatchPlacement) -> Result<(), DistributedWatchError> {
        if self.current_placement()? != *expected {
            return Err(DistributedWatchError::MembershipChanged);
        }
        Ok(())
    }

    fn seal_checkpoint(
        &self,
        placement: &WatchPlacement,
        scope: DistributedWatchScope,
        sources: Vec<WatchVectorEntry>,
    ) -> Result<Vec<u8>, DistributedWatchError> {
        self.checkpoints
            .seal(&WatchCheckpointClaims {
                format: CHECKPOINT_FORMAT,
                aud: CHECKPOINT_AUDIENCE.into(),
                purpose: CHECKPOINT_PURPOSE.into(),
                cluster_id: placement.cluster_id,
                scope,
                membership_revision: placement.membership_revision,
                sources,
            })
            .map_err(DistributedWatchError::CheckpointCodec)
    }

    fn open_checkpoint(
        &self,
        token: &[u8],
        scope: &DistributedWatchScope,
        cluster_id: ClusterId,
    ) -> Result<WatchCheckpointClaims, DistributedWatchError> {
        if token.is_empty() {
            return Err(DistributedWatchError::InvalidCheckpoint);
        }
        let claims = self
            .checkpoints
            .open(token)
            .map_err(|_| DistributedWatchError::InvalidCheckpoint)?;
        if claims.format != CHECKPOINT_FORMAT
            || claims.aud != CHECKPOINT_AUDIENCE
            || claims.purpose != CHECKPOINT_PURPOSE
            || claims.cluster_id != cluster_id
            || claims.scope != *scope
        {
            return Err(DistributedWatchError::InvalidCheckpoint);
        }
        validate_claim_vector(&claims)?;
        Ok(claims)
    }
}

#[derive(Clone, Copy)]
enum WatchInitialPosition {
    Now,
    RetainedBeginning,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum DistributedWatchError {
    #[error("invalid distributed watch scope: {0}")]
    InvalidScope(String),
    #[error("invalid watch checkpoint")]
    InvalidCheckpoint,
    #[error("RESUME_EXPIRED")]
    ResumeExpired,
    #[error("watch authorization was revoked")]
    AccessRevoked,
    #[error("ACTIVE watch placement is unavailable: {0}")]
    Placement(String),
    #[error("required watch source {node_id:?} is unavailable: {message}")]
    SourceUnavailable {
        node_id: Option<NodeId>,
        message: String,
    },
    #[error("required watch source {node_id:?} returned invalid evidence: {message}")]
    InvalidSource { node_id: NodeId, message: String },
    #[error("watch membership changed while collecting all required sources")]
    MembershipChanged,
    #[error("watch checkpoint codec failed: {0}")]
    CheckpointCodec(String),
}

struct ValidatedPage {
    next_offset: u64,
    invalidations: Vec<LocalInvalidation>,
}

fn map_source_result<T>(
    node: NodeId,
    result: Result<T, WatchSourceError>,
) -> Result<T, DistributedWatchError> {
    match result {
        Ok(value) => Ok(value),
        Err(WatchSourceError::ResumeExpired) => Err(DistributedWatchError::ResumeExpired),
        Err(WatchSourceError::AccessRevoked) => Err(DistributedWatchError::AccessRevoked),
        Err(WatchSourceError::Unavailable(message)) => {
            Err(DistributedWatchError::SourceUnavailable {
                node_id: Some(node),
                message,
            })
        }
    }
}

fn validate_status(node: NodeId, status: &WatchJournalStatus) -> Result<(), DistributedWatchError> {
    let expected_node =
        u16::try_from(node.0).map_err(|_| DistributedWatchError::InvalidSource {
            node_id: node,
            message: "node identity cannot be represented by SourceId".into(),
        })?;
    if status.source_id.node_id != expected_node {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source epoch belongs to another node".into(),
        });
    }
    if status.retention_floor > status.tail
        || status.settled_through < status.retention_floor
        || status.settled_through > status.tail
        || status.retained_entries != status.tail - status.retention_floor
    {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source retention metadata is inconsistent".into(),
        });
    }
    Ok(())
}

fn validate_page(
    node: NodeId,
    query: &WatchSourceQuery,
    page: WatchSourcePage,
) -> Result<ValidatedPage, DistributedWatchError> {
    if page.source_node != node || page.membership_revision != query.membership_revision {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source identity or membership revision differs from the request".into(),
        });
    }
    validate_status(node, &page.status)?;
    if page.status.source_id != query.expected_source {
        return Err(DistributedWatchError::ResumeExpired);
    }
    let after_tail = page.status.settled_through.checked_add(1).ok_or_else(|| {
        DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source journal tail cannot advance".into(),
        }
    })?;
    let floor_next = page.status.retention_floor.checked_add(1).ok_or_else(|| {
        DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source retention floor cannot advance".into(),
        }
    })?;
    if query.next_offset < floor_next || query.next_offset > after_tail {
        return Err(DistributedWatchError::ResumeExpired);
    }
    if page.next_offset < query.next_offset || page.next_offset > after_tail {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source returned an invalid next offset".into(),
        });
    }
    if page.object_heads.len() > query.max_records {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source returned more events than the bounded journal page".into(),
        });
    }
    if query.next_offset < after_tail && page.next_offset == query.next_offset {
        return Err(DistributedWatchError::InvalidSource {
            node_id: node,
            message: "source made no progress despite retained records".into(),
        });
    }

    let mut previous = None;
    let mut invalidations = Vec::with_capacity(page.object_heads.len());
    for head in page.object_heads {
        if head.offset < query.next_offset || head.offset >= page.next_offset {
            return Err(DistributedWatchError::InvalidSource {
                node_id: node,
                message: "source returned an event outside its represented cursor range".into(),
            });
        }
        if previous.is_some_and(|offset| head.offset <= offset) {
            return Err(DistributedWatchError::InvalidSource {
                node_id: node,
                message: "source event offsets are not strictly increasing".into(),
            });
        }
        previous = Some(head.offset);
        invalidations.push(head_to_invalidation(&query.scope, head).map_err(|message| {
            DistributedWatchError::InvalidSource {
                node_id: node,
                message,
            }
        })?);
    }
    Ok(ValidatedPage {
        next_offset: page.next_offset,
        invalidations,
    })
}

fn head_to_invalidation(
    scope: &DistributedWatchScope,
    head: ObjectHeadChange,
) -> Result<LocalInvalidation, String> {
    if head.tenant_id != scope.tenant_id
        || head.bucket_id != scope.bucket_id
        || !scope.contains_path(&head.exact_path)
        || contains_reserved_segment(&head.exact_path)
    {
        return Err("source returned an object head outside the public watch scope".into());
    }
    let key = ObjectKey::new(&scope.tenant, &scope.bucket, head.exact_path)
        .map_err(|error| error.to_string())?;
    Ok(LocalInvalidation {
        offset: head.offset,
        key,
        minimum_path_version: head.path_version,
        state_hint: match head.kind {
            ObjectHeadChangeKind::Put => InvalidationStateHint::Present,
            ObjectHeadChangeKind::Delete => InvalidationStateHint::Deleted,
        },
    })
}

fn contains_reserved_segment(path: &str) -> bool {
    path.split('/').any(|segment| segment == "_keldra")
}

/// Source adapters use this helper after resolving bucket-routed source
/// records. It prevents internal paths and later journal variants from leaking
/// through public WatchPrefix.
pub(crate) fn filter_public_changes(
    scope: &DistributedWatchScope,
    changes: Vec<LocalChange>,
) -> Vec<ObjectHeadChange> {
    changes
        .into_iter()
        .filter_map(LocalChange::into_object_head)
        .filter(|head| {
            head.tenant_id == scope.tenant_id
                && head.bucket_id == scope.bucket_id
                && scope.contains_path(&head.exact_path)
                && !contains_reserved_segment(&head.exact_path)
        })
        .collect()
}

fn validate_claim_vector(claims: &WatchCheckpointClaims) -> Result<(), DistributedWatchError> {
    let mut nodes = BTreeSet::new();
    let mut previous = None;
    for entry in &claims.sources {
        let node = NodeId(u64::from(entry.source.node_id));
        if entry.source.node_id == 0
            || entry.next_offset == 0
            || previous.is_some_and(|prior| node <= prior)
            || !nodes.insert(node)
        {
            return Err(DistributedWatchError::InvalidCheckpoint);
        }
        previous = Some(node);
    }
    if claims.sources.is_empty()
        || claims.membership_revision.term == 0
        || claims.membership_revision.index == 0
    {
        return Err(DistributedWatchError::InvalidCheckpoint);
    }
    Ok(())
}

fn resume_cursors(
    placement: &WatchPlacement,
    claims: &WatchCheckpointClaims,
) -> Result<BTreeMap<NodeId, WatchVectorEntry>, DistributedWatchError> {
    let claimed = claims
        .sources
        .iter()
        .copied()
        .map(|entry| (NodeId(u64::from(entry.source.node_id)), entry))
        .collect::<BTreeMap<_, _>>();
    if claims.membership_revision == placement.membership_revision
        && claimed.keys().copied().collect::<Vec<_>>() != placement.source_ids().collect::<Vec<_>>()
    {
        return Err(DistributedWatchError::InvalidCheckpoint);
    }
    placement
        .source_ids()
        .map(|node| {
            claimed
                .get(&node)
                .copied()
                .map(|entry| (node, entry))
                .ok_or(DistributedWatchError::ResumeExpired)
        })
        .collect()
}

fn require_complete_sources(
    placement: &WatchPlacement,
    returned: impl Iterator<Item = NodeId>,
) -> Result<(), DistributedWatchError> {
    let returned = returned.collect::<Vec<_>>();
    let required = placement.source_ids().collect::<Vec<_>>();
    if returned != required {
        return Err(DistributedWatchError::SourceUnavailable {
            node_id: None,
            message: "not every ACTIVE source returned a complete response".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "distributed_watch/tests.rs"]
mod tests;
