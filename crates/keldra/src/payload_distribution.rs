//! Immediate payload preparation and evidence verification for one cluster.
//!
//! The upload source is explicit and remains the sole complete source needed
//! for `LOCAL`. Placement is derived from the current fenced membership; no
//! placement record, source-location record, reference inventory, or side
//! persistence is created here.

use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

use keldra_consensus::{ClusterId, NodeId};
use keldra_store::{
    BlobReader, BlobRef, Durability, ErasureCodec, ErasureProfile, PayloadArtifactState,
    ShardIdentity, ShardSealOutcome, Store,
};
use thiserror::Error;
use tonic::{Code, Status};

use crate::cluster_placement::ClusterPlacement;
use crate::data_peer::{DATA_PEER_FRAME_BYTES, DATA_PEER_SCHEMA_VERSION, DataPeerTransport};
use crate::payload_placement::{
    NodePayloadEvidence, PayloadPlacement, PayloadReadinessError, select_payload_placement,
};
use crate::placement::PlacementNode;

mod peer;

pub(crate) use peer::{PayloadPeerService, PayloadPeerTransport};

pub(crate) const PAYLOAD_EVIDENCE_FORMAT: u16 = 1;
const SHARD_PIPE_DEPTH: usize = 2;

pub(crate) trait PayloadPlacementView: Send + Sync {
    fn cluster_id(&self) -> ClusterId;
    fn fence(&self) -> keldra_store::PlacementLogId;
    fn placement_nodes(&self) -> &[PlacementNode];
    fn active_node_ids(&self) -> Vec<NodeId>;
    fn address(&self, node: NodeId) -> Option<&str>;
}

impl PayloadPlacementView for ClusterPlacement {
    fn cluster_id(&self) -> ClusterId {
        self.cluster_id()
    }

    fn fence(&self) -> keldra_store::PlacementLogId {
        self.fence()
    }

    fn placement_nodes(&self) -> &[PlacementNode] {
        self.placement_nodes()
    }

    fn active_node_ids(&self) -> Vec<NodeId> {
        self.active_node_ids()
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.address(node).map(|address| address.0.as_str())
    }
}

/// Bounded proof returned directly by the authenticated upload-source node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreparedPayloadEvidence {
    format: u16,
    placement_fence: keldra_store::PlacementLogId,
    blob: BlobRef,
    upload_source: NodeId,
    artifacts: Vec<NodePayloadEvidence>,
}

impl PreparedPayloadEvidence {
    pub(crate) const fn format(&self) -> u16 {
        self.format
    }

    pub(crate) const fn placement_fence(&self) -> keldra_store::PlacementLogId {
        self.placement_fence
    }

    pub(crate) fn blob(&self) -> &BlobRef {
        &self.blob
    }

    pub(crate) const fn upload_source(&self) -> NodeId {
        self.upload_source
    }

    pub(crate) fn artifacts(&self) -> &[NodePayloadEvidence] {
        &self.artifacts
    }

    pub(crate) fn from_parts(
        format: u16,
        placement_fence: keldra_store::PlacementLogId,
        blob: BlobRef,
        upload_source: NodeId,
        artifacts: Vec<NodePayloadEvidence>,
    ) -> Self {
        Self {
            format,
            placement_fence,
            blob,
            upload_source,
            artifacts,
        }
    }
}

#[tonic::async_trait]
pub(crate) trait PayloadArtifactPeers: Send + Sync {
    async fn put_small(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), Status>;

    async fn small_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status>;

    async fn put_complete(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        source: BlobReader,
    ) -> Result<(), Status>;

    async fn complete_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status>;

    async fn put_shard(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        source: Box<dyn Read + Send>,
    ) -> Result<ShardSealOutcome, Status>;

    async fn shard_exists(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<bool, Status>;
}

#[tonic::async_trait]
impl PayloadArtifactPeers for DataPeerTransport {
    async fn put_small(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), Status> {
        self.put_small_content(target, address, reference, bytes)
            .await
    }

    async fn small_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status> {
        self.small_content_exists(target, address, reference).await
    }

    async fn put_complete(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        source: BlobReader,
    ) -> Result<(), Status> {
        self.put_complete_source(target, address, reference, source)
            .await
            .map(|_| ())
    }

    async fn complete_exists(
        &self,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
    ) -> Result<bool, Status> {
        let mut stream = match self.get_complete_source(target, address, reference).await {
            Ok(stream) => stream,
            Err(status) if status.code() == Code::NotFound => return Ok(false),
            Err(status) => return Err(status),
        };
        let mut offset = 0_u64;
        let mut hasher = blake3::Hasher::new();
        while let Some(frame) = stream.message().await? {
            let next = offset
                .checked_add(frame.content.len() as u64)
                .ok_or_else(|| Status::data_loss("complete-copy response offset overflowed"))?;
            if frame.schema_version != DATA_PEER_SCHEMA_VERSION
                || frame.offset != offset
                || frame.content.len() > DATA_PEER_FRAME_BYTES
                || next > reference.length
            {
                return Err(Status::data_loss(
                    "complete-copy response is not a bounded contiguous stream",
                ));
            }
            hasher.update(&frame.content);
            offset = next;
            if frame.end {
                return Ok(
                    offset == reference.length && hasher.finalize().as_bytes() == &reference.hash
                );
            }
        }
        Err(Status::data_loss(
            "complete-copy response ended without a final frame",
        ))
    }

    async fn put_shard(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        source: Box<dyn Read + Send>,
    ) -> Result<ShardSealOutcome, Status> {
        DataPeerTransport::put_shard(self, target, address, identity, source).await
    }

    async fn shard_exists(
        &self,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
    ) -> Result<bool, Status> {
        DataPeerTransport::shard_exists(self, target, address, identity).await
    }
}

/// Payload work performed by the explicit upload source and path coordinator.
#[derive(Clone)]
pub(crate) struct PayloadDistribution {
    local_node: NodeId,
    store: Store,
    peers: Arc<dyn PayloadArtifactPeers>,
    profile: ErasureProfile,
}

impl PayloadDistribution {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        peers: Arc<dyn PayloadArtifactPeers>,
        profile: ErasureProfile,
    ) -> Self {
        Self {
            local_node,
            store,
            peers,
            profile,
        }
    }

    /// Prepare payload evidence on the node named by the ready capability.
    ///
    /// `LOCAL` only verifies this node's already sealed complete source.
    /// `REPLICATED` additionally installs the required final copies or shards.
    pub(crate) async fn prepare_on_upload_source(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        reference: &BlobRef,
        durability: Durability,
    ) -> Result<PreparedPayloadEvidence, PayloadDistributionError> {
        match self.store.complete_copy_state(reference).await? {
            PayloadArtifactState::Valid => {}
            PayloadArtifactState::Missing => {
                return Err(PayloadDistributionError::UploadSourceMissing);
            }
            PayloadArtifactState::Corrupt => {
                return Err(PayloadDistributionError::UploadSourceCorrupt);
            }
        }

        let desired = select_payload_placement(
            placement.cluster_id(),
            reference,
            self.profile,
            placement.placement_nodes(),
        );
        let mut artifacts = BTreeMap::from([(self.local_node, (true, None))]);
        if durability == Durability::Replicated {
            match &desired {
                PayloadPlacement::Small(small) => {
                    let bytes = self.store.read_small_copy(reference)?;
                    for owner in small.owners() {
                        self.install_small(placement, *owner, reference, &bytes)
                            .await?;
                        artifacts
                            .entry(*owner)
                            .and_modify(|entry| entry.0 = true)
                            .or_insert((true, None));
                    }
                }
                PayloadPlacement::LargeComplete(complete) => {
                    for owner in complete.owners() {
                        if self
                            .install_complete(placement, *owner, reference)
                            .await
                            .is_ok()
                        {
                            artifacts
                                .entry(*owner)
                                .and_modify(|entry| entry.0 = true)
                                .or_insert((true, None));
                        }
                    }
                }
                PayloadPlacement::Large(large) => {
                    for (node, ordinal) in self
                        .encode_and_install_shards(placement, reference, large.shards())
                        .await?
                    {
                        artifacts
                            .entry(node)
                            .and_modify(|entry| entry.1 = Some(ordinal))
                            .or_insert((false, Some(ordinal)));
                    }
                }
            }
        }
        let artifacts = evidence_from_map(artifacts);
        desired.require_ready(durability, true, self.local_node, &artifacts)?;
        Ok(PreparedPayloadEvidence {
            format: PAYLOAD_EVIDENCE_FORMAT,
            placement_fence: placement.fence(),
            blob: reference.clone(),
            upload_source: self.local_node,
            artifacts,
        })
    }

    /// Independently validates evidence obtained directly from the upload
    /// source over mTLS and probes every final owner needed by the requested
    /// durability. Metadata quorum remains a separate caller obligation.
    pub(crate) async fn verify_on_path_coordinator(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        reference: &BlobRef,
        durability: Durability,
        upload_source: NodeId,
        evidence: &PreparedPayloadEvidence,
    ) -> Result<(), PayloadDistributionError> {
        if evidence.format != PAYLOAD_EVIDENCE_FORMAT
            || evidence.placement_fence != placement.fence()
            || evidence.blob != *reference
            || evidence.upload_source != upload_source
            || !placement.active_node_ids().contains(&upload_source)
        {
            return Err(PayloadDistributionError::InvalidEvidence);
        }
        let desired = select_payload_placement(
            placement.cluster_id(),
            reference,
            self.profile,
            placement.placement_nodes(),
        );
        desired.require_ready(durability, true, upload_source, &evidence.artifacts)?;
        if durability == Durability::Local {
            return Ok(());
        }

        match &desired {
            PayloadPlacement::Small(small) => {
                for owner in small.owners() {
                    if !self.small_exists(placement, *owner, reference).await? {
                        return Err(PayloadDistributionError::OwnerArtifactMissing {
                            node: *owner,
                        });
                    }
                }
            }
            PayloadPlacement::LargeComplete(complete) => {
                let mut verified = 0_usize;
                for owner in complete.owners() {
                    if evidence
                        .artifacts
                        .iter()
                        .any(|entry| entry.node_id() == *owner && entry.complete_copy())
                        && self.complete_exists(placement, *owner, reference).await?
                    {
                        verified += 1;
                    }
                }
                if verified < 2 {
                    return Err(PayloadDistributionError::OwnerArtifactThreshold {
                        kind: "complete copies",
                        required: 2,
                        verified,
                    });
                }
            }
            PayloadPlacement::Large(large) => {
                let mut verified = 0_usize;
                for shard in large.shards() {
                    if evidence.artifacts.iter().any(|entry| {
                        entry.node_id() == shard.owner()
                            && entry.shard_ordinal() == Some(shard.ordinal())
                    }) && self
                        .shard_exists(
                            placement,
                            shard.owner(),
                            &ShardIdentity::new(reference.clone(), shard.ordinal()),
                        )
                        .await?
                    {
                        verified += 1;
                    }
                }
                if verified < usize::from(self.profile.data_shards()) + 1 {
                    return Err(PayloadDistributionError::OwnerArtifactThreshold {
                        kind: "shards",
                        required: usize::from(self.profile.data_shards()) + 1,
                        verified,
                    });
                }
            }
        }
        Ok(())
    }

    /// Explicit fail-closed seam for restart recovery when the ready
    /// capability carrying the upload source is no longer available.
    ///
    /// Source discovery or a durable source-location field requires an
    /// architectural decision and is intentionally absent.
    pub(crate) fn resume_without_upload_source(
        &self,
        _placement: &(impl PayloadPlacementView + ?Sized),
        _reference: &BlobRef,
    ) -> Result<(), PayloadDistributionError> {
        Err(PayloadDistributionError::SourceLocationUnavailable)
    }

    async fn install_small(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        owner: NodeId,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), PayloadDistributionError> {
        if owner == self.local_node {
            self.store.seal_small_copy(reference, bytes).await?;
            return Ok(());
        }
        let address = peer_address(placement, owner)?;
        self.peers
            .put_small(owner, address, reference, bytes)
            .await
            .map_err(|status| peer_error(owner, status))?;
        Ok(())
    }

    async fn install_complete(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        owner: NodeId,
        reference: &BlobRef,
    ) -> Result<(), PayloadDistributionError> {
        if owner == self.local_node {
            return match self.store.complete_copy_state(reference).await? {
                PayloadArtifactState::Valid => Ok(()),
                PayloadArtifactState::Missing => Err(PayloadDistributionError::UploadSourceMissing),
                PayloadArtifactState::Corrupt => Err(PayloadDistributionError::UploadSourceCorrupt),
            };
        }
        let source = self
            .store
            .open_blob(reference)
            .await
            .map_err(|error| PayloadDistributionError::CompleteSource(error.to_string()))?;
        self.peers
            .put_complete(owner, peer_address(placement, owner)?, reference, source)
            .await
            .map_err(|status| peer_error(owner, status))
    }

    async fn small_exists(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        owner: NodeId,
        reference: &BlobRef,
    ) -> Result<bool, PayloadDistributionError> {
        if owner == self.local_node {
            return Ok(
                self.store.complete_copy_state(reference).await? == PayloadArtifactState::Valid
            );
        }
        self.peers
            .small_exists(owner, peer_address(placement, owner)?, reference)
            .await
            .map_err(|status| peer_error(owner, status))
    }

    async fn complete_exists(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        owner: NodeId,
        reference: &BlobRef,
    ) -> Result<bool, PayloadDistributionError> {
        if owner == self.local_node {
            return Ok(
                self.store.complete_copy_state(reference).await? == PayloadArtifactState::Valid
            );
        }
        self.peers
            .complete_exists(owner, peer_address(placement, owner)?, reference)
            .await
            .map_err(|status| peer_error(owner, status))
    }

    async fn shard_exists(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        owner: NodeId,
        identity: &ShardIdentity,
    ) -> Result<bool, PayloadDistributionError> {
        if owner == self.local_node {
            return Ok(self
                .store
                .get_shard(&ErasureCodec::new(self.profile)?, identity)
                .is_ok());
        }
        self.peers
            .shard_exists(owner, peer_address(placement, owner)?, identity)
            .await
            .map_err(|status| peer_error(owner, status))
    }

    async fn encode_and_install_shards(
        &self,
        placement: &(impl PayloadPlacementView + ?Sized),
        reference: &BlobRef,
        shards: &[crate::payload_placement::ShardPlacement],
    ) -> Result<Vec<(NodeId, u16)>, PayloadDistributionError> {
        let mut writers = Vec::with_capacity(shards.len());
        let mut receivers = Vec::with_capacity(shards.len());
        for shard in shards {
            let (sender, receiver) = mpsc::sync_channel(SHARD_PIPE_DEPTH);
            let open = Arc::new(AtomicBool::new(true));
            writers.push(ShardPipeWriter {
                sender,
                open: open.clone(),
            });
            receivers.push((*shard, ShardPipeReader::new(receiver), open));
        }

        let runtime = tokio::runtime::Handle::current();
        let encoding_store = self.store.clone();
        let encoding_reference = reference.clone();
        let profile = self.profile;
        let encoding = tokio::task::spawn_blocking(move || {
            let codec = ErasureCodec::new(profile)?;
            runtime.block_on(encoding_store.encode_sealed_source(
                &codec,
                &encoding_reference,
                &mut writers,
            ))
        });

        let mut installs = Vec::with_capacity(receivers.len());
        for (shard, reader, open) in receivers {
            let identity = ShardIdentity::new(reference.clone(), shard.ordinal());
            if shard.owner() == self.local_node {
                let runtime = tokio::runtime::Handle::current();
                let store = self.store.clone();
                let profile = self.profile;
                installs.push(tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let codec = ErasureCodec::new(profile)?;
                        runtime.block_on(store.seal_shard(&codec, &identity, reader))
                    })
                    .await
                    .map_err(|error| error.to_string())?
                    .map(|_| (shard.owner(), shard.ordinal()))
                    .map_err(|error| error.to_string());
                    if result.is_err() {
                        open.store(false, Ordering::Release);
                    }
                    result
                }));
            } else {
                let peers = self.peers.clone();
                let address = peer_address(placement, shard.owner())?.to_owned();
                installs.push(tokio::spawn(async move {
                    let result = peers
                        .put_shard(shard.owner(), &address, &identity, Box::new(reader))
                        .await
                        .map(|_| (shard.owner(), shard.ordinal()))
                        .map_err(|error| error.to_string());
                    if result.is_err() {
                        open.store(false, Ordering::Release);
                    }
                    result
                }));
            }
        }

        encoding
            .await
            .map_err(|error| PayloadDistributionError::Encoding(error.to_string()))??;
        let mut installed = Vec::new();
        for install in installs {
            if let Ok(Ok(shard)) = install.await {
                installed.push(shard);
            }
        }
        Ok(installed)
    }
}

fn evidence_from_map(artifacts: BTreeMap<NodeId, (bool, Option<u16>)>) -> Vec<NodePayloadEvidence> {
    artifacts
        .into_iter()
        .map(|(node, (complete, ordinal))| NodePayloadEvidence::new(node, complete, ordinal))
        .collect()
}

fn peer_address(
    placement: &(impl PayloadPlacementView + ?Sized),
    node: NodeId,
) -> Result<&str, PayloadDistributionError> {
    placement
        .address(node)
        .ok_or(PayloadDistributionError::OwnerAddressMissing { node })
}

fn peer_error(node: NodeId, status: Status) -> PayloadDistributionError {
    PayloadDistributionError::Peer {
        node,
        message: status.to_string(),
    }
}

struct ShardPipeWriter {
    sender: mpsc::SyncSender<Vec<u8>>,
    open: Arc<AtomicBool>,
}

impl Write for ShardPipeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.open.load(Ordering::Acquire) && self.sender.send(bytes.to_vec()).is_err() {
            self.open.store(false, Ordering::Release);
        }
        // One failed destination must not prevent other independent shard
        // writers from reaching K+1.
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ShardPipeReader {
    receiver: mpsc::Receiver<Vec<u8>>,
    current: Cursor<Vec<u8>>,
}

impl ShardPipeReader {
    fn new(receiver: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            receiver,
            current: Cursor::new(Vec::new()),
        }
    }
}

impl Read for ShardPipeReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        loop {
            let read = self.current.read(output)?;
            if read != 0 {
                return Ok(read);
            }
            let Ok(bytes) = self.receiver.recv() else {
                return Ok(0);
            };
            self.current = Cursor::new(bytes);
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum PayloadDistributionError {
    #[error("the ready capability's upload source no longer has the complete payload")]
    UploadSourceMissing,
    #[error("the ready capability's upload source contains corrupt payload bytes")]
    UploadSourceCorrupt,
    #[error("prepared payload evidence is stale or does not match the request")]
    InvalidEvidence,
    #[error("payload owner node {node:?} has no committed peer address")]
    OwnerAddressMissing { node: NodeId },
    #[error("payload owner node {node:?} is missing its required artifact")]
    OwnerArtifactMissing { node: NodeId },
    #[error("only {verified} of {required} required {kind} verified their artifacts")]
    OwnerArtifactThreshold {
        kind: &'static str,
        required: usize,
        verified: usize,
    },
    #[error("payload peer node {node:?} failed: {message}")]
    Peer { node: NodeId, message: String },
    #[error("payload encoding failed: {0}")]
    Encoding(String),
    #[error("complete payload source failed: {0}")]
    CompleteSource(String),
    #[error("crash continuation cannot locate an upload source without an approved mechanism")]
    SourceLocationUnavailable,
    #[error(transparent)]
    Readiness(#[from] PayloadReadinessError),
    #[error(transparent)]
    Store(#[from] keldra_store::PayloadStoreError),
    #[error(transparent)]
    Erasure(#[from] keldra_store::ErasureError),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::sync::RwLock;

    use keldra_store::{ErasureProfile, SMALL_BLOB_MAX_BYTES, StoreOptions};

    use super::*;

    #[derive(Clone)]
    struct TestPlacement {
        nodes: Vec<PlacementNode>,
        addresses: BTreeMap<NodeId, String>,
    }

    impl TestPlacement {
        fn three_nodes() -> Self {
            Self::with_nodes(&[NodeId(1), NodeId(2), NodeId(3)])
        }

        fn with_nodes(ids: &[NodeId]) -> Self {
            Self {
                nodes: ids
                    .iter()
                    .copied()
                    .map(|node| PlacementNode::new(node, NonZeroU32::new(1_000_000).unwrap()))
                    .collect(),
                addresses: ids
                    .iter()
                    .copied()
                    .map(|node| (node, format!("node-{}:50052", node.0)))
                    .collect(),
            }
        }
    }

    impl PayloadPlacementView for TestPlacement {
        fn cluster_id(&self) -> ClusterId {
            ClusterId(*b"payload-dist-tst")
        }

        fn fence(&self) -> keldra_store::PlacementLogId {
            keldra_store::PlacementLogId { term: 4, index: 9 }
        }

        fn placement_nodes(&self) -> &[PlacementNode] {
            &self.nodes
        }

        fn active_node_ids(&self) -> Vec<NodeId> {
            self.nodes.iter().map(|node| node.node_id()).collect()
        }

        fn address(&self, node: NodeId) -> Option<&str> {
            self.addresses.get(&node).map(String::as_str)
        }
    }

    struct TestPeers {
        stores: BTreeMap<NodeId, Store>,
        unavailable: RwLock<BTreeSet<NodeId>>,
        profile: ErasureProfile,
    }

    impl TestPeers {
        fn new(stores: BTreeMap<NodeId, Store>) -> Self {
            Self {
                stores,
                unavailable: RwLock::new(BTreeSet::new()),
                profile: ErasureProfile::default(),
            }
        }

        fn fail(&self, node: NodeId) {
            self.unavailable.write().unwrap().insert(node);
        }

        fn available(&self, node: NodeId) -> Result<Store, Status> {
            if self.unavailable.read().unwrap().contains(&node) {
                return Err(Status::unavailable("test owner is unavailable"));
            }
            self.stores
                .get(&node)
                .cloned()
                .ok_or_else(|| Status::unavailable("test owner is missing"))
        }
    }

    #[tonic::async_trait]
    impl PayloadArtifactPeers for TestPeers {
        async fn put_small(
            &self,
            target: NodeId,
            _address: &str,
            reference: &BlobRef,
            bytes: &[u8],
        ) -> Result<(), Status> {
            self.available(target)?
                .seal_small_copy(reference, bytes)
                .await
                .map(|_| ())
                .map_err(|error| Status::internal(error.to_string()))
        }

        async fn small_exists(
            &self,
            target: NodeId,
            _address: &str,
            reference: &BlobRef,
        ) -> Result<bool, Status> {
            self.available(target)?
                .complete_copy_state(reference)
                .await
                .map(|state| state == PayloadArtifactState::Valid)
                .map_err(|error| Status::internal(error.to_string()))
        }

        async fn put_complete(
            &self,
            target: NodeId,
            _address: &str,
            reference: &BlobRef,
            mut source: BlobReader,
        ) -> Result<(), Status> {
            let mut bytes = Vec::new();
            let mut frame = [0_u8; 16 * 1024];
            loop {
                let read = source
                    .read(&mut frame)
                    .await
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&frame[..read]);
            }
            let stored = self
                .available(target)?
                .stage_blob(&bytes)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            if stored != *reference {
                return Err(Status::data_loss("complete-copy identity changed"));
            }
            Ok(())
        }

        async fn complete_exists(
            &self,
            target: NodeId,
            _address: &str,
            reference: &BlobRef,
        ) -> Result<bool, Status> {
            self.available(target)?
                .complete_copy_state(reference)
                .await
                .map(|state| state == PayloadArtifactState::Valid)
                .map_err(|error| Status::internal(error.to_string()))
        }

        async fn put_shard(
            &self,
            target: NodeId,
            _address: &str,
            identity: &ShardIdentity,
            source: Box<dyn Read + Send>,
        ) -> Result<ShardSealOutcome, Status> {
            let store = self.available(target)?;
            let identity = identity.clone();
            let profile = self.profile;
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let codec = ErasureCodec::new(profile)
                    .map_err(|error| Status::internal(error.to_string()))?;
                runtime
                    .block_on(store.seal_shard(&codec, &identity, source))
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        }

        async fn shard_exists(
            &self,
            target: NodeId,
            _address: &str,
            identity: &ShardIdentity,
        ) -> Result<bool, Status> {
            let codec = ErasureCodec::new(self.profile)
                .map_err(|error| Status::internal(error.to_string()))?;
            Ok(self
                .available(target)?
                .validate_shard(&codec, identity)
                .is_ok())
        }
    }

    async fn stores(root: &Path) -> BTreeMap<NodeId, Store> {
        let mut stores = BTreeMap::new();
        for node in [NodeId(1), NodeId(2), NodeId(3)] {
            stores.insert(
                node,
                Store::open(StoreOptions::new(
                    root.join(node.0.to_string()),
                    node.0 as u16,
                ))
                .await
                .unwrap(),
            );
        }
        stores
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_small_local_and_replicated_use_explicit_source() {
        let temporary = tempfile::tempdir().unwrap();
        let stores = stores(temporary.path()).await;
        let source = stores[&NodeId(2)].clone();
        let reference = source
            .stage_blob(b"small three-node payload")
            .await
            .unwrap();
        let peers = Arc::new(TestPeers::new(stores.clone()));
        let placement = TestPlacement::three_nodes();
        let source_distribution =
            PayloadDistribution::new(NodeId(2), source, peers.clone(), ErasureProfile::default());
        let path_distribution = PayloadDistribution::new(
            NodeId(1),
            stores[&NodeId(1)].clone(),
            peers,
            ErasureProfile::default(),
        );

        let local = source_distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Local)
            .await
            .unwrap();
        assert_eq!(local.upload_source(), NodeId(2));
        assert_eq!(local.artifacts().len(), 1);
        path_distribution
            .verify_on_path_coordinator(
                &placement,
                &reference,
                Durability::Local,
                NodeId(2),
                &local,
            )
            .await
            .unwrap();

        let replicated = source_distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Replicated)
            .await
            .unwrap();
        path_distribution
            .verify_on_path_coordinator(
                &placement,
                &reference,
                Durability::Replicated,
                NodeId(2),
                &replicated,
            )
            .await
            .unwrap();
        let desired = select_payload_placement(
            placement.cluster_id(),
            &reference,
            ErasureProfile::default(),
            placement.placement_nodes(),
        );
        let PayloadPlacement::Small(desired) = desired else {
            panic!("expected small placement")
        };
        for owner in desired.owners() {
            assert_eq!(
                stores[owner].complete_copy_state(&reference).await.unwrap(),
                PayloadArtifactState::Valid
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn three_node_large_replicated_streams_one_distinct_shard_per_owner() {
        let temporary = tempfile::tempdir().unwrap();
        let stores = stores(temporary.path()).await;
        let bytes = (0..SMALL_BLOB_MAX_BYTES + 33_333)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let source = stores[&NodeId(2)].clone();
        let reference = source.stage_blob(&bytes).await.unwrap();
        let peers = Arc::new(TestPeers::new(stores.clone()));
        let placement = TestPlacement::three_nodes();
        let source_distribution =
            PayloadDistribution::new(NodeId(2), source, peers.clone(), ErasureProfile::default());
        let path_distribution = PayloadDistribution::new(
            NodeId(1),
            stores[&NodeId(1)].clone(),
            peers,
            ErasureProfile::default(),
        );

        let evidence = source_distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Replicated)
            .await
            .unwrap();
        path_distribution
            .verify_on_path_coordinator(
                &placement,
                &reference,
                Durability::Replicated,
                NodeId(2),
                &evidence,
            )
            .await
            .unwrap();

        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut ordinals = BTreeSet::new();
        for (node, store) in stores {
            let presence = store
                .local_payload_presence(&codec, &reference)
                .await
                .unwrap();
            assert_eq!(presence.shard_ordinals().len(), 1, "node {}", node.0);
            ordinals.insert(presence.shard_ordinals()[0]);
        }
        assert_eq!(ordinals, BTreeSet::from([0, 1, 2]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_node_large_local_uses_complete_copy_and_replicated_fails_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let stores = stores(temporary.path()).await;
        let source = stores[&NodeId(1)].clone();
        let reference = source
            .stage_blob(&vec![0x5a; SMALL_BLOB_MAX_BYTES + 1])
            .await
            .unwrap();
        let peers = Arc::new(TestPeers::new(stores));
        let distribution =
            PayloadDistribution::new(NodeId(1), source, peers, ErasureProfile::default());
        let placement = TestPlacement::with_nodes(&[NodeId(1)]);

        distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Local)
            .await
            .unwrap();
        assert!(matches!(
            distribution
                .prepare_on_upload_source(&placement, &reference, Durability::Replicated)
                .await,
            Err(PayloadDistributionError::Readiness(
                PayloadReadinessError::CompleteCopies {
                    required: 2,
                    durable: 1,
                }
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_node_large_replicated_installs_and_verifies_two_complete_copies() {
        let temporary = tempfile::tempdir().unwrap();
        let stores = stores(temporary.path()).await;
        let bytes = vec![0x39; SMALL_BLOB_MAX_BYTES + 1];
        let source = stores[&NodeId(2)].clone();
        let reference = source.stage_blob(&bytes).await.unwrap();
        let peers = Arc::new(TestPeers::new(stores.clone()));
        let placement = TestPlacement::with_nodes(&[NodeId(1), NodeId(2)]);
        let source_distribution =
            PayloadDistribution::new(NodeId(2), source, peers.clone(), ErasureProfile::default());
        let coordinator = PayloadDistribution::new(
            NodeId(1),
            stores[&NodeId(1)].clone(),
            peers,
            ErasureProfile::default(),
        );

        let evidence = source_distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Replicated)
            .await
            .unwrap();
        coordinator
            .verify_on_path_coordinator(
                &placement,
                &reference,
                Durability::Replicated,
                NodeId(2),
                &evidence,
            )
            .await
            .unwrap();
        for node in [NodeId(1), NodeId(2)] {
            assert_eq!(
                stores[&node].complete_copy_state(&reference).await.unwrap(),
                PayloadArtifactState::Valid
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn missing_owner_rejects_replicated_but_not_local() {
        let temporary = tempfile::tempdir().unwrap();
        let stores = stores(temporary.path()).await;
        let source = stores[&NodeId(2)].clone();
        let reference = source
            .stage_blob(&vec![0x5a; SMALL_BLOB_MAX_BYTES + 1])
            .await
            .unwrap();
        let peers = Arc::new(TestPeers::new(stores));
        peers.fail(NodeId(3));
        let distribution =
            PayloadDistribution::new(NodeId(2), source, peers, ErasureProfile::default());
        let placement = TestPlacement::three_nodes();

        distribution
            .prepare_on_upload_source(&placement, &reference, Durability::Local)
            .await
            .unwrap();
        assert!(
            distribution
                .prepare_on_upload_source(&placement, &reference, Durability::Replicated)
                .await
                .is_err()
        );
        assert!(matches!(
            distribution.resume_without_upload_source(&placement, &reference),
            Err(PayloadDistributionError::SourceLocationUnavailable)
        ));
    }
}
