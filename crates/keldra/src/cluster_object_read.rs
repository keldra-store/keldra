//! Authoritative object reads over distributed metadata and immutable bytes.
//!
//! Metadata is selected from the exact complete-record quorum. Payload bytes
//! are reconstructed into an anonymous, non-authoritative file and are not
//! exposed until the placement fence is checked again.

use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use keldra_store::{
    BlobRef, CurrentObjectSnapshot, ErasureError, ErasureProfile, ObjectKey, ObjectPathSnapshot,
    PlacementLogId, SMALL_BLOB_MAX_BYTES, Version, VersionId,
};
use tonic::Status;

use crate::cluster_placement::ClusterPlacement;
use crate::object_distribution::ObjectDistribution;
use crate::payload_read::{
    AnonymousPayloadReadSpools, DistributedPayloadReader, PayloadReadError,
    PayloadReadPlacementView, PayloadReadSpool, PayloadReadSpoolFactory, PayloadReadTransport,
};

#[tonic::async_trait]
trait ObjectReadMetadata: Send + Sync {
    async fn reconciled_snapshot(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectPathSnapshot>, Status>;

    async fn reconciled_snapshot_stable(
        &self,
        key: &ObjectKey,
        _tenant_id: u64,
        _bucket_id: u64,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        self.reconciled_snapshot(key).await
    }

    async fn reconciled_current_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<CurrentObjectSnapshot>, Status>;

    async fn reconciled_current_snapshots_stable(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
        let mut snapshots = Vec::with_capacity(keys.len());
        for key in keys {
            snapshots.push(
                self.reconciled_current_snapshot_stable(key, tenant_id, bucket_id)
                    .await?,
            );
        }
        Ok(snapshots)
    }

    async fn wait_for_program_cursors(
        &self,
        _cursors: &[u64],
        _budget: Duration,
    ) -> Result<bool, Status> {
        Ok(false)
    }

    fn current_placement(&self) -> Result<Arc<dyn PayloadReadPlacementView>, Status>;

    fn require_current_fence(&self, expected: PlacementLogId) -> Result<(), Status>;
}

#[tonic::async_trait]
impl ObjectReadMetadata for ObjectDistribution {
    async fn reconciled_snapshot(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        self.reconciled_object_snapshot(key).await
    }

    async fn reconciled_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        self.reconciled_object_snapshot_stable(key, tenant_id, bucket_id)
            .await
    }

    async fn reconciled_current_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        self.reconciled_current_object_snapshot_stable(key, tenant_id, bucket_id)
            .await
    }

    async fn reconciled_current_snapshots_stable(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
        self.reconciled_current_object_snapshots_stable(keys, tenant_id, bucket_id)
            .await
    }

    async fn wait_for_program_cursors(
        &self,
        cursors: &[u64],
        budget: Duration,
    ) -> Result<bool, Status> {
        ObjectDistribution::wait_for_program_cursors(self, cursors, budget).await
    }

    fn current_placement(&self) -> Result<Arc<dyn PayloadReadPlacementView>, Status> {
        Ok(Arc::new(self.current_read_placement()?))
    }

    fn require_current_fence(&self, expected: PlacementLogId) -> Result<(), Status> {
        self.require_current_read_fence(expected)
    }
}

impl PayloadReadPlacementView for ClusterPlacement {
    fn cluster_id(&self) -> keldra_consensus::ClusterId {
        self.cluster_id()
    }

    fn fence(&self) -> PlacementLogId {
        self.fence()
    }

    fn placement_nodes(&self) -> &[crate::placement::PlacementNode] {
        self.placement_nodes()
    }

    fn address(&self, node: keldra_consensus::NodeId) -> Option<&str> {
        self.address(node).map(|address| address.0.as_str())
    }
}

#[derive(Clone)]
pub(crate) struct ClusterObjectReader {
    metadata: Arc<dyn ObjectReadMetadata>,
    payload: DistributedPayloadReader,
    spools: Arc<dyn PayloadReadSpoolFactory>,
}

impl ClusterObjectReader {
    pub(crate) fn new(
        distribution: ObjectDistribution,
        profile: ErasureProfile,
        transport: Arc<dyn PayloadReadTransport>,
        spool_directory: impl AsRef<Path>,
    ) -> Result<Self, ErasureError> {
        let spools: Arc<dyn PayloadReadSpoolFactory> =
            Arc::new(AnonymousPayloadReadSpools::new(spool_directory)?);
        Self::with_components(Arc::new(distribution), profile, transport, spools)
    }

    fn with_components(
        metadata: Arc<dyn ObjectReadMetadata>,
        profile: ErasureProfile,
        transport: Arc<dyn PayloadReadTransport>,
        spools: Arc<dyn PayloadReadSpoolFactory>,
    ) -> Result<Self, ErasureError> {
        let payload = DistributedPayloadReader::new(profile, transport, spools.clone())?;
        Ok(Self {
            metadata,
            payload,
            spools,
        })
    }

    /// Selects the authoritative current descriptor without reading bytes.
    /// A tombstone is returned as a deleted descriptor; only a never-created
    /// path returns `None`.
    pub(crate) async fn head(&self, key: &ObjectKey) -> Result<Option<Version>, Status> {
        Ok(self.head_with_program_cursor(key).await?.0)
    }

    pub(crate) async fn head_with_program_cursor(
        &self,
        key: &ObjectKey,
    ) -> Result<(Option<Version>, Option<u64>), Status> {
        let (placement, snapshot) = self.stable_snapshot(key).await?;
        let selected = select_descriptor(snapshot.as_ref(), key, Selection::Current)?;
        let cursor = selected_program_cursor(snapshot.as_ref(), Selection::Current);
        self.metadata.require_current_fence(placement.fence())?;
        Ok((selected, cursor))
    }

    pub(crate) async fn head_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<Version>, Status> {
        let (placement, snapshot) = self
            .stable_snapshot_with_ids(key, tenant_id, bucket_id)
            .await?;
        let selected = select_descriptor(snapshot.as_ref(), key, Selection::Current)?;
        self.metadata.require_current_fence(placement.fence())?;
        Ok(selected)
    }

    /// Returns the reconciled stable-ID metadata snapshot under one placement
    /// fence without fetching payload bytes. Derived-view builders use its
    /// mutation stamp to prove a reread does not pass their captured journal
    /// target before opening the immutable BlobRef directly.
    pub(crate) async fn current_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<ObjectPathSnapshot>, Status> {
        let (placement, snapshot) = self
            .stable_snapshot_with_ids(key, tenant_id, bucket_id)
            .await?;
        if let Some(snapshot) = &snapshot {
            snapshot
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
        }
        self.metadata.require_current_fence(placement.fence())?;
        Ok(snapshot)
    }

    /// Returns only the authoritative current head and the immutable version
    /// descriptor it names. Retained historical descriptors are deliberately
    /// outside this bounded read.
    pub(crate) async fn current_head_snapshot_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<CurrentObjectSnapshot>, Status> {
        let placement = self.metadata.current_placement()?;
        let snapshot = self
            .metadata
            .reconciled_current_snapshot_stable(key, tenant_id, bucket_id)
            .await?;
        if let Some(snapshot) = &snapshot {
            snapshot
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            if snapshot.exact_path != key.path() {
                return Err(Status::data_loss(
                    "current object quorum returned another exact path",
                ));
            }
        }
        self.metadata.require_current_fence(placement.fence())?;
        Ok(snapshot)
    }

    /// Returns one authoritative exact-current descriptor per key under one
    /// placement fence. Production metadata reads are grouped into bounded
    /// replica batches; this boundary additionally validates result ordering
    /// and identity before exposing any descriptor to an index query.
    pub(crate) async fn current_head_snapshots_stable(
        &self,
        keys: &[ObjectKey],
        tenant_id: u64,
        bucket_id: u64,
        budget: Duration,
    ) -> Result<Vec<Option<CurrentObjectSnapshot>>, Status> {
        let placement = self.metadata.current_placement()?;
        let deadline = tokio::time::Instant::now()
            .checked_add(budget)
            .ok_or_else(|| Status::invalid_argument("live-version deadline overflowed"))?;
        loop {
            let snapshots = self
                .metadata
                .reconciled_current_snapshots_stable(keys, tenant_id, bucket_id)
                .await?;
            if snapshots.len() != keys.len() {
                return Err(Status::data_loss(
                    "current object batch returned the wrong result count",
                ));
            }
            let mut maximum_program_cursor = None::<u64>;
            for (key, snapshot) in keys.iter().zip(&snapshots) {
                let Some(snapshot) = snapshot else {
                    continue;
                };
                snapshot
                    .validate()
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                if snapshot.tenant_id != tenant_id
                    || snapshot.bucket_id != bucket_id
                    || snapshot.exact_path != key.path()
                {
                    return Err(Status::data_loss(
                        "current object batch returned another object identity",
                    ));
                }
                if let Some(cursor) = snapshot
                    .head
                    .mutation_stamp
                    .and_then(|stamp| stamp.program_commit_cursor)
                {
                    maximum_program_cursor =
                        Some(maximum_program_cursor.map_or(cursor, |current| current.max(cursor)));
                }
            }
            let waited = self
                .metadata
                .wait_for_program_cursors(
                    maximum_program_cursor.as_slice(),
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await?;
            self.metadata.require_current_fence(placement.fence())?;
            if waited {
                continue;
            }
            return Ok(snapshots);
        }
    }

    /// Opens only the authoritative current descriptor through the bounded
    /// current-head quorum path. Retained historical descriptors are neither
    /// transferred nor decoded.
    pub(crate) async fn open_current_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<Option<ClusterOpenedObject>, Status> {
        let placement = self.metadata.current_placement()?;
        let snapshot = self
            .metadata
            .reconciled_current_snapshot_stable(key, tenant_id, bucket_id)
            .await?;
        let Some(snapshot) = snapshot else {
            self.metadata.require_current_fence(placement.fence())?;
            return Ok(None);
        };
        snapshot
            .validate()
            .map_err(|error| Status::data_loss(error.to_string()))?;
        if snapshot.tenant_id != tenant_id
            || snapshot.bucket_id != bucket_id
            || snapshot.exact_path != key.path()
        {
            return Err(Status::data_loss(
                "current object quorum returned another object identity",
            ));
        }
        self.metadata.require_current_fence(placement.fence())?;

        let program_commit_cursor = snapshot
            .head
            .mutation_stamp
            .and_then(|stamp| stamp.program_commit_cursor);
        let version = snapshot.version;
        let payload = match (&version.blob, version.deleted) {
            (None, true) => None,
            (Some(reference), false) => {
                let shared =
                    SharedOutputSpool::new(self.spools.create().map_err(|error| {
                        Status::internal(format!("create read spool: {error}"))
                    })?);
                self.payload
                    .read(placement.as_ref(), reference, shared.clone())
                    .await
                    .map_err(payload_status)?;
                Some(shared.into_payload()?)
            }
            _ => return Err(Status::data_loss("version has an invalid payload shape")),
        };
        self.metadata.require_current_fence(placement.fence())?;
        Ok(Some(ClusterOpenedObject {
            version,
            payload,
            program_commit_cursor,
        }))
    }

    /// Selects current or exact-version metadata and reconstructs live bytes
    /// into an anonymous file. The file remains private until the final fence
    /// check succeeds.
    pub(crate) async fn open(
        &self,
        key: &ObjectKey,
        requested_version: Option<VersionId>,
    ) -> Result<Option<ClusterOpenedObject>, Status> {
        let (placement, snapshot) = self.stable_snapshot(key).await?;
        self.open_selected(key, requested_version, placement, snapshot)
            .await
    }

    pub(crate) async fn open_stable(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
        requested_version: Option<VersionId>,
    ) -> Result<Option<ClusterOpenedObject>, Status> {
        let (placement, snapshot) = self
            .stable_snapshot_with_ids(key, tenant_id, bucket_id)
            .await?;
        self.open_selected(key, requested_version, placement, snapshot)
            .await
    }

    async fn open_selected(
        &self,
        key: &ObjectKey,
        requested_version: Option<VersionId>,
        placement: Arc<dyn PayloadReadPlacementView>,
        snapshot: Option<ObjectPathSnapshot>,
    ) -> Result<Option<ClusterOpenedObject>, Status> {
        let selection = requested_version.map_or(Selection::Current, Selection::Exact);
        let Some(version) = select_descriptor(snapshot.as_ref(), key, selection)? else {
            self.metadata.require_current_fence(placement.fence())?;
            return Ok(None);
        };
        let program_commit_cursor = selected_program_cursor(snapshot.as_ref(), selection);

        let payload = match (&version.blob, version.deleted) {
            (None, true) => None,
            (Some(reference), false) => {
                let shared =
                    SharedOutputSpool::new(self.spools.create().map_err(|error| {
                        Status::internal(format!("create read spool: {error}"))
                    })?);
                self.payload
                    .read(placement.as_ref(), reference, shared.clone())
                    .await
                    .map_err(payload_status)?;
                Some(shared.into_payload()?)
            }
            _ => return Err(Status::data_loss("version has an invalid payload shape")),
        };
        self.metadata.require_current_fence(placement.fence())?;
        Ok(Some(ClusterOpenedObject {
            version,
            payload,
            program_commit_cursor,
        }))
    }

    /// Reconstruct one ordinary content-addressed blob without inventing an
    /// object path. Atomic recovery uses this for the complete prepared bundle
    /// named by Raft.
    pub(crate) async fn read_blob_bytes(&self, reference: &BlobRef) -> Result<Vec<u8>, Status> {
        let mut payload = self.open_blob_payload(reference).await?;
        let mut bytes = Vec::new();
        payload
            .read_to_end(&mut bytes)
            .map_err(|error| Status::internal(format!("read reconstructed blob: {error}")))?;
        Ok(bytes)
    }

    /// Reconstructs one immutable blob. Verified inline values remain in memory;
    /// large values retain the anonymous spool so memory stays corpus-independent.
    pub(crate) async fn open_blob_payload(
        &self,
        reference: &BlobRef,
    ) -> Result<ClusterReadPayload, Status> {
        let placement = self.metadata.current_placement()?;
        if reference.length <= SMALL_BLOB_MAX_BYTES as u64 {
            let capacity = usize::try_from(reference.length)
                .map_err(|_| Status::resource_exhausted("small payload exceeds platform"))?;
            let shared = SharedOutputBytes::new(capacity);
            self.payload
                .read(placement.as_ref(), reference, shared.clone())
                .await
                .map_err(payload_status)?;
            self.metadata.require_current_fence(placement.fence())?;
            return shared.into_payload();
        }
        let shared = SharedOutputSpool::new(
            self.spools
                .create()
                .map_err(|error| Status::internal(format!("create read spool: {error}")))?,
        );
        self.payload
            .read(placement.as_ref(), reference, shared.clone())
            .await
            .map_err(payload_status)?;
        self.metadata.require_current_fence(placement.fence())?;
        shared.into_payload()
    }

    async fn stable_snapshot_with_ids(
        &self,
        key: &ObjectKey,
        tenant_id: u64,
        bucket_id: u64,
    ) -> Result<
        (
            Arc<dyn PayloadReadPlacementView>,
            Option<ObjectPathSnapshot>,
        ),
        Status,
    > {
        let placement = self.metadata.current_placement()?;
        let snapshot = self
            .metadata
            .reconciled_snapshot_stable(key, tenant_id, bucket_id)
            .await?;
        self.metadata.require_current_fence(placement.fence())?;
        Ok((placement, snapshot))
    }

    async fn stable_snapshot(
        &self,
        key: &ObjectKey,
    ) -> Result<
        (
            Arc<dyn PayloadReadPlacementView>,
            Option<ObjectPathSnapshot>,
        ),
        Status,
    > {
        // Capture placement first. If reconciliation observes a later view,
        // the immediate fence check rejects mixing its metadata with this byte
        // placement.
        let placement = self.metadata.current_placement()?;
        let snapshot = self.metadata.reconciled_snapshot(key).await?;
        self.metadata.require_current_fence(placement.fence())?;
        Ok((placement, snapshot))
    }
}

pub(crate) struct ClusterOpenedObject {
    pub(crate) version: Version,
    pub(crate) payload: Option<ClusterReadPayload>,
    pub(crate) program_commit_cursor: Option<u64>,
}

pub(crate) struct ClusterReadPayload {
    spool: Box<dyn PayloadReadSpool>,
}

impl ClusterReadPayload {
    pub(crate) fn into_spool(self) -> Box<dyn PayloadReadSpool> {
        self.spool
    }
}

impl Read for ClusterReadPayload {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.spool.read(bytes)
    }
}

impl Seek for ClusterReadPayload {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.spool.seek(position)
    }
}

#[derive(Clone, Copy)]
enum Selection {
    Current,
    Exact(VersionId),
}

fn select_descriptor(
    snapshot: Option<&ObjectPathSnapshot>,
    key: &ObjectKey,
    selection: Selection,
) -> Result<Option<Version>, Status> {
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    snapshot
        .validate()
        .map_err(|error| Status::data_loss(error.to_string()))?;
    if snapshot.exact_path != key.path() {
        return Err(Status::data_loss(
            "object quorum returned another exact path",
        ));
    }
    let selected_id = match selection {
        Selection::Current => snapshot.head.version,
        Selection::Exact(version) => version,
    };
    Ok(snapshot
        .versions
        .iter()
        .find(|version| version.id == selected_id)
        .cloned())
}

fn selected_program_cursor(
    snapshot: Option<&ObjectPathSnapshot>,
    selection: Selection,
) -> Option<u64> {
    let snapshot = snapshot?;
    let selected = match selection {
        Selection::Current => snapshot.head.version,
        Selection::Exact(version) => version,
    };
    (selected == snapshot.head.version)
        .then(|| snapshot.head.mutation_stamp?.program_commit_cursor)
        .flatten()
}

#[derive(Clone)]
struct SharedOutputBytes(Arc<Mutex<Option<Vec<u8>>>>);

impl SharedOutputBytes {
    fn new(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(Some(Vec::with_capacity(capacity)))))
    }

    fn into_payload(self) -> Result<ClusterReadPayload, Status> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| Status::internal("read bytes lock is poisoned"))?;
        let bytes = guard
            .take()
            .ok_or_else(|| Status::internal("read bytes are unavailable"))?;
        Ok(ClusterReadPayload {
            spool: Box::new(Cursor::new(bytes)),
        })
    }
}

impl Write for SharedOutputBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| io::Error::other("read bytes lock is poisoned"))?;
        guard
            .as_mut()
            .ok_or_else(|| io::Error::other("read bytes are unavailable"))?
            .write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct SharedOutputSpool(Arc<Mutex<Option<Box<dyn PayloadReadSpool>>>>);

impl SharedOutputSpool {
    fn new(spool: Box<dyn PayloadReadSpool>) -> Self {
        Self(Arc::new(Mutex::new(Some(spool))))
    }

    fn into_payload(self) -> Result<ClusterReadPayload, Status> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| Status::internal("read spool lock is poisoned"))?;
        let mut spool = guard
            .take()
            .ok_or_else(|| Status::internal("read spool is unavailable"))?;
        spool
            .seek(SeekFrom::Start(0))
            .map_err(|error| Status::internal(format!("rewind read spool: {error}")))?;
        Ok(ClusterReadPayload { spool })
    }
}

impl Write for SharedOutputSpool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| io::Error::other("read spool lock is poisoned"))?;
        guard
            .as_mut()
            .ok_or_else(|| io::Error::other("read spool is unavailable"))?
            .write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| io::Error::other("read spool lock is poisoned"))?;
        guard
            .as_mut()
            .ok_or_else(|| io::Error::other("read spool is unavailable"))?
            .flush()
    }
}

fn payload_status(error: PayloadReadError) -> Status {
    match error {
        PayloadReadError::Unavailable { .. } => Status::unavailable(error.to_string()),
        PayloadReadError::Erasure(_) => Status::data_loss(error.to_string()),
        PayloadReadError::Spool(_) | PayloadReadError::Output(_) | PayloadReadError::Task(_) => {
            Status::internal(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests;
