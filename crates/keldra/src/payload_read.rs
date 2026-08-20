//! Bounded-memory reads from the current content placement.
//!
//! Small values and large values in an undersized cluster are read from one
//! verified current complete-copy owner. Once membership can place every
//! erasure ordinal, large values are reconstructed from independently verified
//! shard owners. Anonymous spool files hold only non-authoritative working
//! bytes and disappear on every success or failure path.

use std::collections::HashSet;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use keldra_consensus::{ClusterId, NodeId};
use keldra_store::{
    BlobRef, ErasureCodec, ErasureError, ErasureProfile, PlacementLogId, ShardIdentity,
};
use thiserror::Error;

use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::placement::PlacementNode;

pub(crate) const PAYLOAD_READ_FRAME_BYTES: usize = 64 * 1024;

/// The immutable placement inputs used for one read attempt.
pub(crate) trait PayloadReadPlacementView: Send + Sync {
    fn cluster_id(&self) -> ClusterId;
    fn fence(&self) -> PlacementLogId;
    fn placement_nodes(&self) -> &[PlacementNode];
    fn address(&self, node: NodeId) -> Option<&str>;
}

/// A typed byte-plane transport. Implementations may serve `target` locally or
/// over the private peer connection, but must apply the supplied exact fence.
///
/// `get_*` implementations stream frames into `destination`; a destination
/// rejects any individual frame larger than [`PAYLOAD_READ_FRAME_BYTES`].
/// `put_*` is idempotent for an exact immutable identity and atomically replaces
/// an existing corrupt artifact only after the supplied bytes verify.
#[tonic::async_trait]
pub(crate) trait PayloadReadTransport: Send + Sync {
    async fn get_small(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError>;

    async fn put_small(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), PayloadReadTransportError>;

    /// Fetch one complete large-object copy. The default keeps existing test
    /// transports source-compatible while failing closed until their complete
    /// byte-plane adapter is supplied.
    async fn get_complete(
        &self,
        _fence: PlacementLogId,
        _target: NodeId,
        _address: &str,
        _reference: &BlobRef,
        _destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        Err(PayloadReadTransportError::NotFound)
    }

    async fn get_shard(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError>;

    async fn put_shard(
        &self,
        fence: PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        source: Box<dyn Read + Send>,
    ) -> Result<(), PayloadReadTransportError>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum PayloadReadTransportError {
    #[error("artifact is absent")]
    NotFound,
    #[error("owner is unavailable: {0}")]
    Unavailable(String),
    #[error("owner returned an invalid artifact: {0}")]
    InvalidArtifact(String),
    #[error("the local destination failed: {0}")]
    Destination(String),
}

pub(crate) trait PayloadReadSpool: Read + Write + Seek + Send {}

impl<T: Read + Write + Seek + Send> PayloadReadSpool for T {}

/// Creates anonymous non-authoritative files. Closing the returned handle must
/// remove the working bytes without a separate cleanup operation.
pub(crate) trait PayloadReadSpoolFactory: Send + Sync {
    fn create(&self) -> io::Result<Box<dyn PayloadReadSpool>>;
}

/// Linux `O_TMPFILE` spool files have no directory entry and are removed when
/// their final descriptor closes.
#[derive(Clone, Debug)]
pub(crate) struct AnonymousPayloadReadSpools {
    directory: Arc<PathBuf>,
}

impl AnonymousPayloadReadSpools {
    pub(crate) fn new(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory)?;
        Ok(Self {
            directory: Arc::new(directory),
        })
    }
}

impl PayloadReadSpoolFactory for AnonymousPayloadReadSpools {
    fn create(&self) -> io::Result<Box<dyn PayloadReadSpool>> {
        anonymous_file(&self.directory).map(|file| Box::new(file) as Box<dyn PayloadReadSpool>)
    }
}

#[cfg(target_os = "linux")]
fn anonymous_file(directory: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_TMPFILE | libc::O_CLOEXEC)
        .open(directory)
}

#[cfg(not(target_os = "linux"))]
fn anonymous_file(_directory: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "anonymous payload-read spools require Linux O_TMPFILE",
    ))
}

#[derive(Clone)]
pub(crate) struct DistributedPayloadReader {
    profile: ErasureProfile,
    codec: Arc<ErasureCodec>,
    transport: Arc<dyn PayloadReadTransport>,
    spools: Arc<dyn PayloadReadSpoolFactory>,
}

impl DistributedPayloadReader {
    pub(crate) fn new(
        profile: ErasureProfile,
        transport: Arc<dyn PayloadReadTransport>,
        spools: Arc<dyn PayloadReadSpoolFactory>,
    ) -> Result<Self, ErasureError> {
        Ok(Self {
            profile,
            codec: Arc::new(ErasureCodec::new(profile)?),
            transport,
            spools,
        })
    }

    /// Streams one fully verified value to `output`. Nothing is written to the
    /// caller until a complete copy or reconstructed value has passed its
    /// BLAKE3 and length checks.
    pub(crate) async fn read<W>(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        output: W,
    ) -> Result<PayloadReadReport, PayloadReadError>
    where
        W: Write + Send + 'static,
    {
        let desired = select_payload_placement(
            placement.cluster_id(),
            reference,
            self.profile,
            placement.placement_nodes(),
        );
        match desired {
            PayloadPlacement::Small(small) => {
                let mut seen = HashSet::with_capacity(placement.placement_nodes().len());
                let mut candidates = small.owners().to_vec();
                seen.extend(candidates.iter().copied());
                candidates.extend(
                    placement
                        .placement_nodes()
                        .iter()
                        .map(|node| node.node_id())
                        .filter(|node| seen.insert(*node)),
                );
                self.read_small(placement, reference, small.owners(), &candidates, output)
                    .await
            }
            PayloadPlacement::LargeComplete(complete) => {
                let mut seen = HashSet::with_capacity(placement.placement_nodes().len());
                let mut candidates = complete.owners().to_vec();
                seen.extend(candidates.iter().copied());
                candidates.extend(
                    placement
                        .placement_nodes()
                        .iter()
                        .map(|node| node.node_id())
                        .filter(|node| seen.insert(*node)),
                );
                self.read_complete(placement, reference, &candidates, output)
                    .await
            }
            PayloadPlacement::Large(large) => {
                self.read_large(placement, reference, large.shards(), output)
                    .await
            }
        }
    }

    async fn read_complete<W>(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        owners: &[NodeId],
        output: W,
    ) -> Result<PayloadReadReport, PayloadReadError>
    where
        W: Write + Send + 'static,
    {
        let (mut complete, states) = self.fetch_complete(placement, reference, owners).await?;
        let expected_length = reference.length;
        tokio::task::spawn_blocking(move || stream_exact(&mut complete, output, expected_length))
            .await
            .map_err(|error| PayloadReadError::Task(error.to_string()))??;
        Ok(PayloadReadReport::new(&states))
    }

    async fn fetch_complete(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        owners: &[NodeId],
    ) -> Result<(Box<dyn PayloadReadSpool>, Vec<OwnerState>), PayloadReadError> {
        let mut states = Vec::with_capacity(owners.len());
        for owner in owners {
            let Some(address) = placement.address(*owner) else {
                states.push(OwnerState::Unavailable);
                continue;
            };
            let mut spool = self.spools.create().map_err(PayloadReadError::Spool)?;
            let mut bounded = FrameBoundedWriter::new(spool.as_mut(), reference.length);
            let fetched = self
                .transport
                .get_complete(placement.fence(), *owner, address, reference, &mut bounded)
                .await;
            let violated = bounded.violated;
            drop(bounded);
            let state = classify_complete_fetch(reference, fetched, violated, spool.as_mut())?;
            states.push(state);
            if state == OwnerState::Healthy {
                spool.seek(SeekFrom::Start(0))?;
                return Ok((spool, states));
            }
        }
        Err(PayloadReadError::Unavailable {
            kind: "complete copy",
            required: 1,
            summary: StateSummary::from_states(&states),
        })
    }

    async fn read_small<W>(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        selected_owners: &[NodeId],
        candidates: &[NodeId],
        mut output: W,
    ) -> Result<PayloadReadReport, PayloadReadError>
    where
        W: Write + Send + 'static,
    {
        let mut valid = None;
        let mut states = Vec::with_capacity(candidates.len());
        for owner in candidates {
            let Some(address) = placement.address(*owner) else {
                states.push(OwnerState::Unavailable);
                continue;
            };
            let mut bytes = Vec::with_capacity(reference.length as usize);
            let mut bounded = FrameBoundedWriter::new(&mut bytes, reference.length);
            let fetched = self
                .transport
                .get_small(placement.fence(), *owner, address, reference, &mut bounded)
                .await;
            let violated = bounded.violated;
            drop(bounded);
            let state = classify_small_fetch(reference, fetched, violated, &bytes)?;
            if valid.is_none() && state == OwnerState::Healthy {
                valid = Some(bytes.clone());
            }
            states.push(state);
        }
        let bytes = valid.ok_or_else(|| PayloadReadError::Unavailable {
            kind: "small copy",
            required: 1,
            summary: StateSummary::from_states(&states),
        })?;
        write_bounded(&mut output, &bytes)?;
        output.flush().map_err(PayloadReadError::Output)?;

        let mut report = PayloadReadReport::new(&states);
        for owner in selected_owners {
            let index = candidates
                .iter()
                .position(|candidate| candidate == owner)
                .expect("selected small owner is included in read candidates");
            let state = states[index];
            if !state.needs_repair() {
                continue;
            }
            report.repairs_attempted += 1;
            let Some(address) = placement.address(*owner) else {
                report.repairs_failed += 1;
                continue;
            };
            match self
                .transport
                .put_small(placement.fence(), *owner, address, reference, &bytes)
                .await
            {
                Ok(()) => report.repairs_completed += 1,
                Err(_) => report.repairs_failed += 1,
            }
        }
        Ok(report)
    }

    async fn read_large<W>(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        owners: &[crate::payload_placement::ShardPlacement],
        output: W,
    ) -> Result<PayloadReadReport, PayloadReadError>
    where
        W: Write + Send + 'static,
    {
        let mut states = Vec::with_capacity(owners.len());
        let mut valid = Vec::with_capacity(owners.len());
        for owner in owners {
            let Some(address) = placement.address(owner.owner()) else {
                states.push(OwnerState::Unavailable);
                continue;
            };
            let identity = ShardIdentity::new(reference.clone(), owner.ordinal());
            let mut spool = self.spools.create().map_err(PayloadReadError::Spool)?;
            let maximum = self
                .codec
                .encoded_shard_length(reference, owner.ordinal())?;
            let mut bounded = FrameBoundedWriter::new(spool.as_mut(), maximum);
            let fetched = self
                .transport
                .get_shard(
                    placement.fence(),
                    owner.owner(),
                    address,
                    &identity,
                    &mut bounded,
                )
                .await;
            let violated = bounded.violated;
            drop(bounded);
            let state = classify_shard_fetch(
                self.codec.as_ref(),
                &identity,
                fetched,
                violated,
                spool.as_mut(),
            )?;
            if state == OwnerState::Healthy {
                spool.seek(SeekFrom::Start(0))?;
                valid.push((owner.ordinal(), spool));
            }
            states.push(state);
        }
        let required = usize::from(self.profile.data_shards());
        if valid.len() < required {
            let unavailable = PayloadReadError::Unavailable {
                kind: "shard",
                required,
                summary: StateSummary::from_states(&states),
            };
            let mut seen = HashSet::with_capacity(placement.placement_nodes().len());
            let complete_candidates = placement
                .placement_nodes()
                .iter()
                .map(|node| node.node_id())
                .filter(|node| seen.insert(*node))
                .collect::<Vec<_>>();
            let Ok((mut reconstructed, _)) = self
                .fetch_complete(placement, reference, &complete_candidates)
                .await
            else {
                return Err(unavailable);
            };
            let expected_length = reference.length;
            reconstructed = tokio::task::spawn_blocking(move || {
                stream_exact(&mut reconstructed, output, expected_length)?;
                reconstructed.seek(SeekFrom::Start(0))?;
                Ok::<_, PayloadReadError>(reconstructed)
            })
            .await
            .map_err(|error| PayloadReadError::Task(error.to_string()))??;

            let mut report = PayloadReadReport::new(&states);
            let repair_ordinals = states
                .iter()
                .enumerate()
                .filter_map(|(ordinal, state)| state.needs_repair().then_some(ordinal))
                .collect::<Vec<_>>();
            if !repair_ordinals.is_empty() {
                report.repairs_attempted = repair_ordinals.len();
                self.repair_large(
                    placement,
                    reference,
                    owners,
                    repair_ordinals,
                    reconstructed,
                    &mut report,
                )
                .await;
            }
            return Ok(report);
        }

        let codec = self.codec.clone();
        let expected = reference.clone();
        let mut reconstructed = self.spools.create().map_err(PayloadReadError::Spool)?;
        reconstructed = tokio::task::spawn_blocking(move || {
            codec.reconstruct_available(&expected, valid, &mut reconstructed)?;
            reconstructed.seek(SeekFrom::Start(0))?;
            Ok::<_, PayloadReadError>(reconstructed)
        })
        .await
        .map_err(|error| PayloadReadError::Task(error.to_string()))??;

        let expected_length = reference.length;
        reconstructed = tokio::task::spawn_blocking(move || {
            stream_exact(&mut reconstructed, output, expected_length)?;
            reconstructed.seek(SeekFrom::Start(0))?;
            Ok::<_, PayloadReadError>(reconstructed)
        })
        .await
        .map_err(|error| PayloadReadError::Task(error.to_string()))??;

        let mut report = PayloadReadReport::new(&states);
        let repair_ordinals = states
            .iter()
            .enumerate()
            .filter_map(|(ordinal, state)| state.needs_repair().then_some(ordinal))
            .collect::<Vec<_>>();
        if repair_ordinals.is_empty() {
            return Ok(report);
        }
        report.repairs_attempted = repair_ordinals.len();
        self.repair_large(
            placement,
            reference,
            owners,
            repair_ordinals,
            reconstructed,
            &mut report,
        )
        .await;
        Ok(report)
    }

    async fn repair_large(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        owners: &[crate::payload_placement::ShardPlacement],
        repair_ordinals: Vec<usize>,
        mut reconstructed: Box<dyn PayloadReadSpool>,
        report: &mut PayloadReadReport,
    ) {
        let mut writers = Vec::with_capacity(usize::from(self.profile.total_shards()));
        for ordinal in 0..usize::from(self.profile.total_shards()) {
            if repair_ordinals.contains(&ordinal) {
                match self.spools.create() {
                    Ok(spool) => writers.push(RepairWriter::Spool(spool)),
                    Err(_) => {
                        report.repairs_failed += 1;
                        writers.push(RepairWriter::Discard(io::sink()));
                    }
                }
            } else {
                writers.push(RepairWriter::Discard(io::sink()));
            }
        }
        let codec = self.codec.clone();
        let expected = reference.clone();
        let encoded = tokio::task::spawn_blocking(move || {
            reconstructed.seek(SeekFrom::Start(0))?;
            codec.encode(&mut reconstructed, &expected, &mut writers)?;
            Ok::<_, PayloadReadError>(writers)
        })
        .await;
        let Ok(Ok(writers)) = encoded else {
            report.repairs_failed = report.repairs_attempted;
            return;
        };
        let mut encoded_spools = writers
            .into_iter()
            .map(|writer| match writer {
                RepairWriter::Spool(spool) => Some(spool),
                RepairWriter::Discard(_) => None,
            })
            .collect::<Vec<_>>();

        for ordinal in repair_ordinals {
            let Some(mut spool) = encoded_spools[ordinal].take() else {
                continue;
            };
            if spool.seek(SeekFrom::Start(0)).is_err() {
                report.repairs_failed += 1;
                continue;
            }
            let owner = owners[ordinal];
            let Some(address) = placement.address(owner.owner()) else {
                report.repairs_failed += 1;
                continue;
            };
            let identity = ShardIdentity::new(reference.clone(), owner.ordinal());
            match self
                .transport
                .put_shard(placement.fence(), owner.owner(), address, &identity, spool)
                .await
            {
                Ok(()) => report.repairs_completed += 1,
                Err(_) => report.repairs_failed += 1,
            }
        }
    }
}

fn classify_small_fetch(
    reference: &BlobRef,
    fetched: Result<(), PayloadReadTransportError>,
    violated: bool,
    bytes: &[u8],
) -> Result<OwnerState, PayloadReadError> {
    if violated {
        return Ok(OwnerState::Corrupt);
    }
    Ok(match fetched {
        Ok(())
            if bytes.len() as u64 == reference.length
                && blake3::hash(bytes).as_bytes() == &reference.hash =>
        {
            OwnerState::Healthy
        }
        Ok(()) | Err(PayloadReadTransportError::InvalidArtifact(_)) => OwnerState::Corrupt,
        Err(PayloadReadTransportError::NotFound) => OwnerState::Missing,
        Err(PayloadReadTransportError::Unavailable(_)) => OwnerState::Unavailable,
        Err(PayloadReadTransportError::Destination(reason)) => {
            return Err(PayloadReadError::Spool(io::Error::other(reason)));
        }
    })
}

fn classify_complete_fetch(
    reference: &BlobRef,
    fetched: Result<(), PayloadReadTransportError>,
    violated: bool,
    spool: &mut dyn PayloadReadSpool,
) -> Result<OwnerState, PayloadReadError> {
    if violated {
        return Ok(OwnerState::Corrupt);
    }
    match fetched {
        Ok(()) => {
            spool.seek(SeekFrom::Start(0))?;
            let mut hasher = blake3::Hasher::new();
            let mut observed = 0_u64;
            let mut buffer = [0_u8; PAYLOAD_READ_FRAME_BYTES];
            loop {
                let read = spool.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                observed = observed.saturating_add(read as u64);
                hasher.update(&buffer[..read]);
            }
            spool.seek(SeekFrom::Start(0))?;
            Ok(
                if observed == reference.length && hasher.finalize().as_bytes() == &reference.hash {
                    OwnerState::Healthy
                } else {
                    OwnerState::Corrupt
                },
            )
        }
        Err(PayloadReadTransportError::NotFound) => Ok(OwnerState::Missing),
        Err(PayloadReadTransportError::InvalidArtifact(_)) => Ok(OwnerState::Corrupt),
        Err(PayloadReadTransportError::Unavailable(_)) => Ok(OwnerState::Unavailable),
        Err(PayloadReadTransportError::Destination(reason)) => {
            Err(PayloadReadError::Spool(io::Error::other(reason)))
        }
    }
}

fn classify_shard_fetch(
    codec: &ErasureCodec,
    identity: &ShardIdentity,
    fetched: Result<(), PayloadReadTransportError>,
    violated: bool,
    spool: &mut dyn PayloadReadSpool,
) -> Result<OwnerState, PayloadReadError> {
    if violated {
        return Ok(OwnerState::Corrupt);
    }
    match fetched {
        Ok(()) => {
            spool.seek(SeekFrom::Start(0))?;
            Ok(
                if codec
                    .validate_shard(identity.blob(), identity.ordinal(), spool)
                    .is_ok()
                {
                    OwnerState::Healthy
                } else {
                    OwnerState::Corrupt
                },
            )
        }
        Err(PayloadReadTransportError::NotFound) => Ok(OwnerState::Missing),
        Err(PayloadReadTransportError::InvalidArtifact(_)) => Ok(OwnerState::Corrupt),
        Err(PayloadReadTransportError::Unavailable(_)) => Ok(OwnerState::Unavailable),
        Err(PayloadReadTransportError::Destination(reason)) => {
            Err(PayloadReadError::Spool(io::Error::other(reason)))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerState {
    Healthy,
    Missing,
    Corrupt,
    Unavailable,
}

impl OwnerState {
    const fn needs_repair(self) -> bool {
        matches!(self, Self::Missing | Self::Corrupt)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct StateSummary {
    pub(crate) healthy: usize,
    pub(crate) missing: usize,
    pub(crate) corrupt: usize,
    pub(crate) unavailable: usize,
}

impl StateSummary {
    fn from_states(states: &[OwnerState]) -> Self {
        let mut summary = Self::default();
        for state in states {
            match state {
                OwnerState::Healthy => summary.healthy += 1,
                OwnerState::Missing => summary.missing += 1,
                OwnerState::Corrupt => summary.corrupt += 1,
                OwnerState::Unavailable => summary.unavailable += 1,
            }
        }
        summary
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PayloadReadReport {
    pub(crate) sources: StateSummary,
    pub(crate) repairs_attempted: usize,
    pub(crate) repairs_completed: usize,
    pub(crate) repairs_failed: usize,
}

impl PayloadReadReport {
    fn new(states: &[OwnerState]) -> Self {
        Self {
            sources: StateSummary::from_states(states),
            ..Self::default()
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum PayloadReadError {
    #[error("insufficient valid {kind} owners: need {required}; {summary}")]
    Unavailable {
        kind: &'static str,
        required: usize,
        summary: StateSummary,
    },
    #[error("payload read spool failed: {0}")]
    Spool(#[from] io::Error),
    #[error("payload reconstruction failed: {0}")]
    Erasure(#[from] ErasureError),
    #[error("payload output failed: {0}")]
    Output(io::Error),
    #[error("payload blocking task failed: {0}")]
    Task(String),
}

struct FrameBoundedWriter<'a> {
    inner: &'a mut (dyn Write + Send),
    maximum: u64,
    observed: u64,
    violated: bool,
}

impl<'a> FrameBoundedWriter<'a> {
    fn new(inner: &'a mut (dyn Write + Send), maximum: u64) -> Self {
        Self {
            inner,
            maximum,
            observed: 0,
            violated: false,
        }
    }
}

impl Write for FrameBoundedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self.observed.saturating_add(bytes.len() as u64);
        if bytes.len() > PAYLOAD_READ_FRAME_BYTES || next > self.maximum {
            self.violated = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload owner exceeded the bounded transfer",
            ));
        }
        let written = self.inner.write(bytes)?;
        self.observed += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

enum RepairWriter {
    Discard(io::Sink),
    Spool(Box<dyn PayloadReadSpool>),
}

impl Write for RepairWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Discard(writer) => writer.write(bytes),
            Self::Spool(writer) => writer.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Discard(writer) => writer.flush(),
            Self::Spool(writer) => writer.flush(),
        }
    }
}

fn write_bounded(output: &mut dyn Write, mut bytes: &[u8]) -> Result<(), PayloadReadError> {
    while !bytes.is_empty() {
        let length = bytes.len().min(PAYLOAD_READ_FRAME_BYTES);
        output
            .write_all(&bytes[..length])
            .map_err(PayloadReadError::Output)?;
        bytes = &bytes[length..];
    }
    Ok(())
}

fn stream_exact<W: Write>(
    source: &mut dyn PayloadReadSpool,
    mut output: W,
    expected: u64,
) -> Result<(), PayloadReadError> {
    let mut buffer = vec![0_u8; PAYLOAD_READ_FRAME_BYTES];
    let mut written = 0_u64;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(PayloadReadError::Output)?;
        written += read as u64;
    }
    if written != expected {
        return Err(PayloadReadError::Spool(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("verified payload length changed from {expected} to {written}"),
        )));
    }
    output.flush().map_err(PayloadReadError::Output)
}

impl fmt::Display for StateSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "healthy={}, missing={}, corrupt={}, unavailable={}",
            self.healthy, self.missing, self.corrupt, self.unavailable
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::num::NonZeroU32;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use keldra_store::SMALL_BLOB_MAX_BYTES;

    #[derive(Clone)]
    struct TestPlacement {
        nodes: Vec<PlacementNode>,
        addresses: BTreeMap<NodeId, String>,
    }

    impl TestPlacement {
        fn new(ids: &[u64]) -> Self {
            let nodes = ids
                .iter()
                .copied()
                .map(|id| PlacementNode::new(NodeId(id), NonZeroU32::new(1_000_000).unwrap()))
                .collect::<Vec<_>>();
            let addresses = ids
                .iter()
                .copied()
                .map(|id| (NodeId(id), format!("node-{id}:50052")))
                .collect();
            Self { nodes, addresses }
        }
    }

    impl PayloadReadPlacementView for TestPlacement {
        fn cluster_id(&self) -> ClusterId {
            ClusterId(*b"payload-read-tst")
        }

        fn fence(&self) -> PlacementLogId {
            PlacementLogId { term: 3, index: 8 }
        }

        fn placement_nodes(&self) -> &[PlacementNode] {
            &self.nodes
        }

        fn address(&self, node: NodeId) -> Option<&str> {
            self.addresses.get(&node).map(String::as_str)
        }
    }

    #[derive(Default)]
    struct MemorySpools;

    impl PayloadReadSpoolFactory for MemorySpools {
        fn create(&self) -> io::Result<Box<dyn PayloadReadSpool>> {
            Ok(Box::new(Cursor::new(Vec::new())))
        }
    }

    #[derive(Default)]
    struct TestTransport {
        small: BTreeMap<NodeId, Vec<u8>>,
        complete: BTreeMap<NodeId, Vec<u8>>,
        small_repairs: Mutex<Vec<NodeId>>,
        shard_repairs: AtomicUsize,
    }

    #[tonic::async_trait]
    impl PayloadReadTransport for TestTransport {
        async fn get_small(
            &self,
            _fence: PlacementLogId,
            target: NodeId,
            _address: &str,
            _reference: &BlobRef,
            destination: &mut (dyn Write + Send),
        ) -> Result<(), PayloadReadTransportError> {
            let bytes = self
                .small
                .get(&target)
                .ok_or(PayloadReadTransportError::NotFound)?;
            destination
                .write_all(bytes)
                .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))
        }

        async fn put_small(
            &self,
            _fence: PlacementLogId,
            target: NodeId,
            _address: &str,
            _reference: &BlobRef,
            _bytes: &[u8],
        ) -> Result<(), PayloadReadTransportError> {
            self.small_repairs.lock().unwrap().push(target);
            Ok(())
        }

        async fn get_complete(
            &self,
            _fence: PlacementLogId,
            target: NodeId,
            _address: &str,
            _reference: &BlobRef,
            destination: &mut (dyn Write + Send),
        ) -> Result<(), PayloadReadTransportError> {
            let bytes = self
                .complete
                .get(&target)
                .ok_or(PayloadReadTransportError::NotFound)?;
            for frame in bytes.chunks(PAYLOAD_READ_FRAME_BYTES) {
                destination
                    .write_all(frame)
                    .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            }
            Ok(())
        }

        async fn get_shard(
            &self,
            _fence: PlacementLogId,
            _target: NodeId,
            _address: &str,
            _identity: &ShardIdentity,
            _destination: &mut (dyn Write + Send),
        ) -> Result<(), PayloadReadTransportError> {
            Err(PayloadReadTransportError::NotFound)
        }

        async fn put_shard(
            &self,
            _fence: PlacementLogId,
            _target: NodeId,
            _address: &str,
            _identity: &ShardIdentity,
            mut source: Box<dyn Read + Send>,
        ) -> Result<(), PayloadReadTransportError> {
            io::copy(&mut source, &mut io::sink())
                .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            self.shard_repairs.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct SharedOutput(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn large_bytes() -> Vec<u8> {
        (0..SMALL_BLOB_MAX_BYTES + 7_777)
            .map(|index| (index % 251) as u8)
            .collect()
    }

    fn reference(bytes: &[u8]) -> BlobRef {
        BlobRef {
            hash: *blake3::hash(bytes).as_bytes(),
            length: bytes.len() as u64,
        }
    }

    #[tokio::test]
    async fn small_local_source_outside_selected_owners_remains_readable_and_repairs_only_owners() {
        let bytes = b"LOCAL ingress source outside selected owners".to_vec();
        let reference = reference(&bytes);
        let placement = TestPlacement::new(&[1, 2, 3]);
        let desired = select_payload_placement(
            placement.cluster_id(),
            &reference,
            ErasureProfile::default(),
            placement.placement_nodes(),
        );
        let PayloadPlacement::Small(selected) = desired else {
            panic!("expected small payload placement")
        };
        let source = placement
            .placement_nodes()
            .iter()
            .map(|node| node.node_id())
            .find(|node| !selected.owners().contains(node))
            .expect("three ACTIVE nodes include one outside a two-copy placement");
        let transport = Arc::new(TestTransport {
            small: BTreeMap::from([(source, bytes.clone())]),
            ..TestTransport::default()
        });
        let reader = DistributedPayloadReader::new(
            ErasureProfile::default(),
            transport.clone(),
            Arc::new(MemorySpools),
        )
        .unwrap();
        let output = SharedOutput::default();

        let report = reader
            .read(&placement, &reference, output.clone())
            .await
            .unwrap();

        assert_eq!(*output.0.lock().unwrap(), bytes);
        assert_eq!(report.sources.healthy, 1);
        assert_eq!(report.repairs_completed, selected.owners().len());
        let mut repaired = transport.small_repairs.lock().unwrap().clone();
        repaired.sort();
        let mut expected = selected.owners().to_vec();
        expected.sort();
        assert_eq!(repaired, expected);
        assert!(!repaired.contains(&source));
    }

    #[tokio::test]
    async fn one_node_large_read_uses_verified_complete_copy() {
        let bytes = large_bytes();
        let reference = reference(&bytes);
        let transport = Arc::new(TestTransport {
            complete: BTreeMap::from([(NodeId(1), bytes.clone())]),
            ..TestTransport::default()
        });
        let reader = DistributedPayloadReader::new(
            ErasureProfile::default(),
            transport,
            Arc::new(MemorySpools),
        )
        .unwrap();
        let output = SharedOutput::default();

        let report = reader
            .read(&TestPlacement::new(&[1]), &reference, output.clone())
            .await
            .unwrap();

        assert_eq!(*output.0.lock().unwrap(), bytes);
        assert_eq!(report.sources.healthy, 1);
    }

    #[tokio::test]
    async fn erasure_read_falls_back_to_verified_complete_source_and_repairs_shards() {
        let bytes = large_bytes();
        let reference = reference(&bytes);
        let transport = Arc::new(TestTransport {
            complete: BTreeMap::from([(NodeId(2), bytes.clone())]),
            ..TestTransport::default()
        });
        let reader = DistributedPayloadReader::new(
            ErasureProfile::default(),
            transport.clone(),
            Arc::new(MemorySpools),
        )
        .unwrap();
        let output = SharedOutput::default();

        let report = reader
            .read(&TestPlacement::new(&[1, 2, 3]), &reference, output.clone())
            .await
            .unwrap();

        assert_eq!(*output.0.lock().unwrap(), bytes);
        assert_eq!(report.sources.missing, 3);
        assert_eq!(report.repairs_completed, 3);
        assert_eq!(transport.shard_repairs.load(Ordering::Relaxed), 3);
    }
}
