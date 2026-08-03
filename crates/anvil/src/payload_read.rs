//! Bounded-memory reads from the current content placement.
//!
//! Small values are read from one verified current complete-copy owner. Large
//! values are reconstructed from independently verified current shard owners.
//! Complete large values exist here only as anonymous, non-authoritative spool
//! files and disappear on every success or failure path.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anvil_consensus::{ClusterId, NodeId};
use anvil_store::{
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
    pub(crate) fn new(directory: impl AsRef<Path>) -> Self {
        Self {
            directory: Arc::new(directory.as_ref().to_path_buf()),
        }
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
    /// caller until a small copy or the complete reconstructed large value has
    /// passed its BLAKE3 and length checks.
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
                self.read_small(placement, reference, small.owners(), output)
                    .await
            }
            PayloadPlacement::Large(large) => {
                self.read_large(placement, reference, large.shards(), output)
                    .await
            }
        }
    }

    async fn read_small<W>(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        owners: &[NodeId],
        mut output: W,
    ) -> Result<PayloadReadReport, PayloadReadError>
    where
        W: Write + Send + 'static,
    {
        let mut valid = None;
        let mut states = Vec::with_capacity(owners.len());
        for owner in owners {
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
        for (index, state) in states.iter().enumerate() {
            if !state.needs_repair() {
                continue;
            }
            report.repairs_attempted += 1;
            let owner = owners[index];
            let Some(address) = placement.address(owner) else {
                report.repairs_failed += 1;
                continue;
            };
            match self
                .transport
                .put_small(placement.fence(), owner, address, reference, &bytes)
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
            return Err(PayloadReadError::Unavailable {
                kind: "shard",
                required,
                summary: StateSummary::from_states(&states),
            });
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
