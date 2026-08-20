//! Byte-plane and lifecycle transfer for one ADD transition.
//!
//! Final copies and shards use their ordinary storage paths. Reconstruction
//! uses anonymous files only; it creates no durable handoff inventory.

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use keldra_consensus::NodeId;
use keldra_store::{
    BlobRef, BlobReferenceState, ErasureCodec, ErasureProfile, PayloadArtifactCursor,
    PayloadArtifactIdentity, PayloadArtifactSnapshot, ShardIdentity,
};
use tonic::{Code, Status};

use super::HandoffTopology;
use crate::data_peer::{DATA_PEER_FRAME_BYTES, DATA_PEER_SCHEMA_VERSION, DataPeerTransport};
use crate::join_peer::handoff::merge::{MergeSource, next_key};
use crate::payload_placement::{PayloadPlacement, select_payload_placement};
use crate::payload_read::{
    DistributedPayloadReader, PayloadReadPlacementView, PayloadReadSpool, PayloadReadSpoolFactory,
    PayloadReadTransport, PayloadReadTransportError,
};

#[derive(Default)]
struct BlobArtifacts {
    reference: Option<BlobRef>,
    complete: BTreeMap<NodeId, PayloadArtifactSnapshot>,
    shards: BTreeMap<(NodeId, u16), PayloadArtifactSnapshot>,
}

impl BlobArtifacts {
    fn observe(
        &mut self,
        node: NodeId,
        artifact: PayloadArtifactSnapshot,
        profile: ErasureProfile,
    ) -> Result<(), Status> {
        let reference = artifact.identity.blob().clone();
        match self.reference.as_ref() {
            Some(existing) if existing != &reference => {
                return Err(Status::data_loss(
                    "payload identities collide during handoff enumeration",
                ));
            }
            Some(_) => {}
            None => self.reference = Some(reference),
        }
        match &artifact.identity {
            PayloadArtifactIdentity::Complete(_) => {
                if self.complete.insert(node, artifact).is_some() {
                    return Err(Status::data_loss(
                        "node exported one complete payload lifecycle twice",
                    ));
                }
            }
            PayloadArtifactIdentity::Shard(identity) => {
                if identity.ordinal() >= u16::from(profile.total_shards()) {
                    return Err(Status::data_loss(
                        "node exported a shard outside the configured erasure profile",
                    ));
                }
                if self
                    .shards
                    .insert((node, identity.ordinal()), artifact)
                    .is_some()
                {
                    return Err(Status::data_loss("node exported one shard lifecycle twice"));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn evidence_len(&self) -> usize {
        self.complete.len() + self.shards.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleValue {
    ref_count: u64,
    flags: u8,
}

pub(super) async fn transfer_all(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    profile: ErasureProfile,
    spools: Arc<dyn PayloadReadSpoolFactory>,
) -> Result<(), Status> {
    let mut sources = topology
        .discovery_endpoints()
        .cloned()
        .map(MergeSource::<PayloadArtifactSnapshot, PayloadArtifactCursor>::new)
        .collect::<Vec<_>>();
    loop {
        refill_artifact_sources(&mut sources, peers).await?;
        let Some(first_key) = next_key(&sources) else {
            return Ok(());
        };
        let identity = blob_key_from_artifact_key(&first_key)?;
        let mut artifacts = BlobArtifacts::default();
        loop {
            let drained = drain_blob_identity(&mut sources, identity, &mut artifacts, profile)?;
            if !drained.needs_page {
                break;
            }
            refill_artifact_sources(&mut sources, peers).await?;
        }
        let reference = artifacts
            .reference
            .clone()
            .ok_or_else(|| Status::data_loss("payload observation has no content identity"))?;
        let old = select_payload_placement(
            topology.cluster_id(),
            &reference,
            profile,
            topology.old_nodes(),
        );
        let new = select_payload_placement(
            topology.cluster_id(),
            &reference,
            profile,
            topology.new_nodes(),
        );
        match (old, new) {
            (PayloadPlacement::Small(old), PayloadPlacement::Small(new)) => {
                transfer_small(
                    topology,
                    peers,
                    &reference,
                    &artifacts,
                    old.owners(),
                    new.owners(),
                )
                .await?;
            }
            (PayloadPlacement::LargeComplete(old), PayloadPlacement::LargeComplete(new)) => {
                transfer_large_complete(
                    topology,
                    peers,
                    &reference,
                    &artifacts,
                    old.owners(),
                    new.owners(),
                )
                .await?;
            }
            (PayloadPlacement::LargeComplete(old), PayloadPlacement::Large(new)) => {
                transfer_complete_to_shards(
                    topology,
                    peers,
                    profile,
                    &reference,
                    &artifacts,
                    old.owners(),
                    new.shards(),
                    spools.clone(),
                )
                .await?;
            }
            (PayloadPlacement::Large(old), PayloadPlacement::Large(new)) => {
                transfer_large(
                    topology,
                    peers,
                    profile,
                    &reference,
                    &artifacts,
                    old.shards(),
                    new.shards(),
                    spools.clone(),
                )
                .await?;
            }
            (PayloadPlacement::Large(_), PayloadPlacement::LargeComplete(_)) => {
                return Err(Status::failed_precondition(
                    "an ADD handoff cannot reduce erasure placement to complete copies",
                ));
            }
            _ => {
                return Err(Status::data_loss(
                    "payload size selected contradictory placement classes",
                ));
            }
        }
    }
}

async fn refill_artifact_sources(
    sources: &mut [MergeSource<PayloadArtifactSnapshot, PayloadArtifactCursor>],
    peers: &DataPeerTransport,
) -> Result<(), Status> {
    for source in sources.iter_mut() {
        if !source.needs_page() {
            continue;
        }
        let node = source.node_id();
        let address = source.address().to_owned();
        let cursor = source.cursor().cloned();
        let page = peers
            .export_payload_artifacts(node, &address, cursor.as_ref())
            .await?;
        source.install_page(page.artifacts, page.next_cursor, |artifact| {
            artifact
                .validate()
                .map_err(|error| Status::data_loss(error.to_string()))?;
            Ok(artifact.identity.handoff_order_key())
        })?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IdentityDrain {
    needs_page: bool,
    consumed: usize,
}

fn drain_blob_identity(
    sources: &mut [MergeSource<PayloadArtifactSnapshot, PayloadArtifactCursor>],
    identity: [u8; 40],
    observed: &mut BlobArtifacts,
    profile: ErasureProfile,
) -> Result<IdentityDrain, Status> {
    let mut consumed = 0;
    for source in sources.iter_mut() {
        loop {
            let Some(front) = source.front_key() else {
                break;
            };
            let front_identity = blob_key_from_artifact_key(front)?;
            if front_identity < identity {
                return Err(Status::data_loss(
                    "payload merge advanced past an unconsumed identity",
                ));
            }
            if front_identity != identity {
                break;
            }
            let full_key = front.to_vec();
            let artifact = source.take_if(&full_key).ok_or_else(|| {
                Status::internal("payload merge front changed while consuming an identity")
            })?;
            observed.observe(source.node_id(), artifact, profile)?;
            consumed += 1;
        }
    }
    Ok(IdentityDrain {
        needs_page: sources.iter().any(MergeSource::needs_page),
        consumed,
    })
}

fn blob_key_from_artifact_key(key: &[u8]) -> Result<[u8; 40], Status> {
    let prefix = key.get(..40).ok_or_else(|| {
        Status::data_loss("payload handoff order key is shorter than a content identity")
    })?;
    let mut identity = [0_u8; 40];
    identity.copy_from_slice(prefix);
    Ok(identity)
}

async fn transfer_small(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    old_owners: &[NodeId],
    new_owners: &[NodeId],
) -> Result<(), Status> {
    let (source, source_artifact) = select_complete_source(topology, artifacts, old_owners)?;
    let source_value = lifecycle_value(source_artifact.lifecycle);
    require_matching_complete_lifecycle(artifacts, old_owners, source_value)?;
    let lifecycle = merge_lifecycle_timestamps(
        source_artifact.lifecycle,
        artifacts
            .complete
            .values()
            .map(|artifact| artifact.lifecycle),
    );
    if lifecycle.ref_count == 0 {
        return reconcile_zero_complete(
            topology, peers, reference, artifacts, new_owners, lifecycle,
        )
        .await;
    }
    let bytes = peers
        .get_small_content(
            source,
            topology
                .address(source)
                .ok_or_else(|| Status::data_loss("small payload source has no address"))?,
            reference,
        )
        .await?;

    for target in new_owners {
        let address = topology
            .address(*target)
            .ok_or_else(|| Status::data_loss("small payload target has no address"))?;
        if artifacts
            .complete
            .get(target)
            .is_some_and(|entry| entry.lifecycle == lifecycle)
        {
            peers
                .install_payload_lifecycle(
                    *target,
                    address,
                    &PayloadArtifactSnapshot {
                        identity: PayloadArtifactIdentity::Complete(reference.clone()),
                        lifecycle,
                    },
                )
                .await?;
            continue;
        }
        peers
            .put_small_content(*target, address, reference, &bytes)
            .await?;
        peers
            .install_payload_lifecycle(
                *target,
                address,
                &PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Complete(reference.clone()),
                    lifecycle,
                },
            )
            .await?;
    }
    Ok(())
}

async fn transfer_large_complete(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    old_owners: &[NodeId],
    new_owners: &[NodeId],
) -> Result<(), Status> {
    let (source, source_artifact) = select_complete_source(topology, artifacts, old_owners)?;
    let source_value = lifecycle_value(source_artifact.lifecycle);
    require_matching_complete_lifecycle(artifacts, old_owners, source_value)?;
    let lifecycle = merge_lifecycle_timestamps(
        source_artifact.lifecycle,
        artifacts
            .complete
            .values()
            .map(|artifact| artifact.lifecycle),
    );
    if lifecycle.ref_count == 0 {
        return reconcile_zero_complete(
            topology, peers, reference, artifacts, new_owners, lifecycle,
        )
        .await;
    }
    let source_address = topology
        .address(source)
        .ok_or_else(|| Status::data_loss("large complete source has no address"))?;

    for target in new_owners {
        let target_address = topology
            .address(*target)
            .ok_or_else(|| Status::data_loss("large complete target has no address"))?;
        let exact = artifacts
            .complete
            .get(target)
            .is_some_and(|entry| entry.lifecycle == lifecycle);
        if !exact && *target != source {
            peers
                .copy_complete_source(source, source_address, *target, target_address, reference)
                .await?;
        }
        peers
            .install_payload_lifecycle(
                *target,
                target_address,
                &PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Complete(reference.clone()),
                    lifecycle,
                },
            )
            .await?;
    }
    Ok(())
}

async fn transfer_complete_to_shards(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    profile: ErasureProfile,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    old_owners: &[NodeId],
    new_shards: &[crate::payload_placement::ShardPlacement],
    spools: Arc<dyn PayloadReadSpoolFactory>,
) -> Result<(), Status> {
    let (source, source_artifact) = select_complete_source(topology, artifacts, old_owners)?;
    let selected = lifecycle_value(source_artifact.lifecycle);
    require_matching_complete_lifecycle(artifacts, old_owners, selected)?;
    let lifecycle = merge_lifecycle_timestamps(
        source_artifact.lifecycle,
        artifacts
            .complete
            .values()
            .map(|artifact| artifact.lifecycle),
    );
    if lifecycle.ref_count == 0 {
        return reconcile_zero_shards(topology, peers, reference, artifacts, new_shards, lifecycle)
            .await;
    }
    let missing = new_shards
        .iter()
        .copied()
        .filter(|placement| {
            !artifacts
                .shards
                .contains_key(&(placement.owner(), placement.ordinal()))
        })
        .collect::<Vec<_>>();
    let mut encoded = if missing.is_empty() {
        BTreeMap::new()
    } else {
        encode_complete_source(
            topology, peers, profile, reference, source, &missing, spools,
        )
        .await?
    };

    for placement in new_shards {
        let target = placement.owner();
        let ordinal = placement.ordinal();
        let identity = ShardIdentity::new(reference.clone(), ordinal);
        let target_address = topology
            .address(target)
            .ok_or_else(|| Status::data_loss("large payload target has no address"))?;
        if let Some(spool) = encoded.remove(&ordinal) {
            peers
                .put_shard(
                    target,
                    target_address,
                    &identity,
                    Box::new(OwnedSpoolReader(spool)),
                )
                .await?;
        }
        peers
            .install_payload_lifecycle(
                target,
                target_address,
                &PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Shard(identity),
                    lifecycle,
                },
            )
            .await?;
    }
    if !encoded.is_empty() {
        return Err(Status::internal(
            "complete-to-erasure handoff retained an unassigned shard",
        ));
    }
    Ok(())
}

async fn encode_complete_source(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    profile: ErasureProfile,
    reference: &BlobRef,
    source: NodeId,
    missing: &[crate::payload_placement::ShardPlacement],
    spools: Arc<dyn PayloadReadSpoolFactory>,
) -> Result<BTreeMap<u16, Box<dyn PayloadReadSpool>>, Status> {
    let mut complete = spools
        .create()
        .map_err(|error| Status::internal(format!("create handoff source spool: {error}")))?;
    let source_address = topology
        .address(source)
        .ok_or_else(|| Status::data_loss("large complete source has no address"))?;
    let mut stream = peers
        .get_complete_source(source, source_address, reference)
        .await?;
    let mut offset = 0_u64;
    let mut hasher = blake3::Hasher::new();
    let mut ended = false;
    while let Some(frame) = stream.message().await? {
        if frame.schema_version != DATA_PEER_SCHEMA_VERSION
            || frame.offset != offset
            || frame.content.len() > DATA_PEER_FRAME_BYTES
            || (frame.content.is_empty() && !frame.end)
        {
            return Err(Status::data_loss(
                "complete handoff source stream is malformed",
            ));
        }
        let next = offset
            .checked_add(frame.content.len() as u64)
            .filter(|next| *next <= reference.length)
            .ok_or_else(|| Status::data_loss("complete handoff source length overflow"))?;
        complete
            .write_all(&frame.content)
            .map_err(|error| Status::internal(format!("write handoff source spool: {error}")))?;
        hasher.update(&frame.content);
        offset = next;
        if frame.end {
            ended = true;
            break;
        }
    }
    if !ended || offset != reference.length || hasher.finalize().as_bytes() != &reference.hash {
        return Err(Status::data_loss(
            "complete handoff source failed immutable identity verification",
        ));
    }

    let mut outputs = (0..usize::from(profile.total_shards()))
        .map(|_| EncodeOutput::Discard(io::sink()))
        .collect::<Vec<_>>();
    for placement in missing {
        let ordinal = usize::from(placement.ordinal());
        if ordinal >= outputs.len() || matches!(&outputs[ordinal], EncodeOutput::Target(_)) {
            return Err(Status::data_loss(
                "complete-to-erasure handoff has an invalid shard assignment",
            ));
        }
        outputs[ordinal] =
            EncodeOutput::Target(Some(spools.create().map_err(|error| {
                Status::internal(format!("create encoded shard spool: {error}"))
            })?));
    }
    let codec = ErasureCodec::new(profile)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let expected = reference.clone();
    tokio::task::spawn_blocking(move || {
        complete.seek(SeekFrom::Start(0))?;
        codec
            .encode(&mut *complete, &expected, &mut outputs)
            .map_err(io::Error::other)?;
        let mut encoded = BTreeMap::new();
        for (ordinal, output) in outputs.into_iter().enumerate() {
            if let EncodeOutput::Target(Some(mut spool)) = output {
                spool.seek(SeekFrom::Start(0))?;
                encoded.insert(ordinal as u16, spool);
            }
        }
        Ok::<_, io::Error>(encoded)
    })
    .await
    .map_err(|error| Status::internal(format!("join shard encoder: {error}")))?
    .map_err(|error| Status::data_loss(error.to_string()))
}

fn select_complete_source<'a>(
    topology: &HandoffTopology,
    artifacts: &'a BlobArtifacts,
    old_owners: &[NodeId],
) -> Result<(NodeId, &'a PayloadArtifactSnapshot), Status> {
    old_owners
        .iter()
        .chain(topology.active().iter().map(|endpoint| &endpoint.node_id))
        .find_map(|node| {
            artifacts
                .complete
                .get(node)
                .map(|artifact| (*node, artifact))
        })
        .ok_or_else(|| Status::unavailable("payload has no complete ACTIVE source"))
}

fn require_matching_complete_lifecycle(
    artifacts: &BlobArtifacts,
    owners: &[NodeId],
    selected: LifecycleValue,
) -> Result<(), Status> {
    for owner in owners {
        let Some(artifact) = artifacts.complete.get(owner) else {
            continue;
        };
        if lifecycle_value(artifact.lifecycle) != selected {
            return Err(Status::data_loss(
                "complete payload owners disagree on reference count or flags",
            ));
        }
    }
    Ok(())
}

async fn reconcile_zero_complete(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    new_owners: &[NodeId],
    lifecycle: BlobReferenceState,
) -> Result<(), Status> {
    for (target, artifact) in existing_complete_targets(reference, artifacts, new_owners, lifecycle)
    {
        let address = topology
            .address(target)
            .ok_or_else(|| Status::data_loss("zero-count complete target has no address"))?;
        peers
            .install_payload_lifecycle(target, address, &artifact)
            .await?;
    }
    Ok(())
}

fn existing_complete_targets(
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    owners: &[NodeId],
    lifecycle: BlobReferenceState,
) -> Vec<(NodeId, PayloadArtifactSnapshot)> {
    owners
        .iter()
        .copied()
        .filter(|owner| artifacts.complete.contains_key(owner))
        .map(|owner| {
            (
                owner,
                PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Complete(reference.clone()),
                    lifecycle,
                },
            )
        })
        .collect()
}

async fn reconcile_zero_shards(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    new_shards: &[crate::payload_placement::ShardPlacement],
    lifecycle: BlobReferenceState,
) -> Result<(), Status> {
    for (target, artifact) in existing_shard_targets(reference, artifacts, new_shards, lifecycle) {
        let address = topology
            .address(target)
            .ok_or_else(|| Status::data_loss("zero-count shard target has no address"))?;
        peers
            .install_payload_lifecycle(target, address, &artifact)
            .await?;
    }
    Ok(())
}

fn existing_shard_targets(
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    placements: &[crate::payload_placement::ShardPlacement],
    lifecycle: BlobReferenceState,
) -> Vec<(NodeId, PayloadArtifactSnapshot)> {
    placements
        .iter()
        .copied()
        .filter(|placement| {
            artifacts
                .shards
                .contains_key(&(placement.owner(), placement.ordinal()))
        })
        .map(|placement| {
            (
                placement.owner(),
                PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Shard(ShardIdentity::new(
                        reference.clone(),
                        placement.ordinal(),
                    )),
                    lifecycle,
                },
            )
        })
        .collect()
}

async fn transfer_large(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    profile: ErasureProfile,
    reference: &BlobRef,
    artifacts: &BlobArtifacts,
    old_shards: &[crate::payload_placement::ShardPlacement],
    new_shards: &[crate::payload_placement::ShardPlacement],
    spools: Arc<dyn PayloadReadSpoolFactory>,
) -> Result<(), Status> {
    let selected_lifecycle = select_shard_lifecycle(artifacts, old_shards)?;
    let selected = lifecycle_value(selected_lifecycle);
    require_matching_shard_lifecycle(artifacts, old_shards, selected)?;
    let lifecycle = merge_lifecycle_timestamps(
        selected_lifecycle,
        artifacts.shards.values().map(|artifact| artifact.lifecycle),
    );
    if lifecycle.ref_count == 0 {
        return reconcile_zero_shards(topology, peers, reference, artifacts, new_shards, lifecycle)
            .await;
    }

    for placement in new_shards {
        let target = placement.owner();
        let ordinal = placement.ordinal();
        let identity = ShardIdentity::new(reference.clone(), ordinal);
        let target_address = topology
            .address(target)
            .ok_or_else(|| Status::data_loss("large payload target has no address"))?;
        let exact = artifacts
            .shards
            .get(&(target, ordinal))
            .is_some_and(|entry| entry.lifecycle == lifecycle);
        if !exact {
            if let Some(source) = topology.active().iter().find(|endpoint| {
                artifacts
                    .shards
                    .get(&(endpoint.node_id, ordinal))
                    .is_some_and(|entry| lifecycle_value(entry.lifecycle) == selected)
            }) {
                peers
                    .copy_shard(
                        source.node_id,
                        &source.address,
                        target,
                        target_address,
                        &identity,
                    )
                    .await?;
            } else {
                rebuild_shard(
                    topology,
                    peers,
                    profile,
                    reference,
                    target,
                    target_address,
                    &identity,
                    spools.clone(),
                )
                .await?;
            }
        }
        peers
            .install_payload_lifecycle(
                target,
                target_address,
                &PayloadArtifactSnapshot {
                    identity: PayloadArtifactIdentity::Shard(identity),
                    lifecycle,
                },
            )
            .await?;
    }
    Ok(())
}

fn select_shard_lifecycle(
    artifacts: &BlobArtifacts,
    old_shards: &[crate::payload_placement::ShardPlacement],
) -> Result<BlobReferenceState, Status> {
    old_shards
        .iter()
        .find_map(|placement| {
            artifacts
                .shards
                .get(&(placement.owner(), placement.ordinal()))
                .map(|artifact| artifact.lifecycle)
        })
        .ok_or_else(|| Status::unavailable("large payload has no current shard lifecycle"))
}

fn require_matching_shard_lifecycle(
    artifacts: &BlobArtifacts,
    old_shards: &[crate::payload_placement::ShardPlacement],
    selected: LifecycleValue,
) -> Result<(), Status> {
    for placement in old_shards {
        let Some(artifact) = artifacts
            .shards
            .get(&(placement.owner(), placement.ordinal()))
        else {
            continue;
        };
        if lifecycle_value(artifact.lifecycle) != selected {
            return Err(Status::data_loss(
                "large payload owners disagree on reference count or flags",
            ));
        }
    }
    Ok(())
}

async fn rebuild_shard(
    topology: &HandoffTopology,
    peers: &DataPeerTransport,
    profile: ErasureProfile,
    reference: &BlobRef,
    target: NodeId,
    target_address: &str,
    identity: &ShardIdentity,
    spools: Arc<dyn PayloadReadSpoolFactory>,
) -> Result<(), Status> {
    let complete = spools
        .create()
        .map_err(|error| Status::internal(format!("create handoff spool: {error}")))?;
    let complete = Arc::new(Mutex::new(complete));
    let reader = DistributedPayloadReader::new(
        profile,
        Arc::new(HandoffPayloadTransport {
            peers: peers.clone(),
        }),
        spools.clone(),
    )
    .map_err(|error| Status::failed_precondition(error.to_string()))?;
    reader
        .read(topology, reference, SharedSpool(complete.clone()))
        .await
        .map_err(|error| Status::unavailable(format!("reconstruct handoff payload: {error}")))?;

    let codec = ErasureCodec::new(profile)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let expected = reference.clone();
    let ordinal = usize::from(identity.ordinal());
    let mut target_spool = spools
        .create()
        .map_err(|error| Status::internal(format!("create encoded shard spool: {error}")))?;
    target_spool
        .seek(SeekFrom::Start(0))
        .map_err(|error| Status::internal(error.to_string()))?;
    let encoded = tokio::task::spawn_blocking(move || {
        let mut source = complete
            .lock()
            .map_err(|_| io::Error::other("handoff source spool lock is poisoned"))?;
        source.seek(SeekFrom::Start(0))?;
        let mut outputs = (0..usize::from(profile.total_shards()))
            .map(|index| {
                if index == ordinal {
                    EncodeOutput::Target(None)
                } else {
                    EncodeOutput::Discard(io::sink())
                }
            })
            .collect::<Vec<_>>();
        outputs[ordinal] = EncodeOutput::Target(Some(target_spool));
        codec
            .encode(&mut **source, &expected, &mut outputs)
            .map_err(io::Error::other)?;
        match std::mem::replace(&mut outputs[ordinal], EncodeOutput::Discard(io::sink())) {
            EncodeOutput::Target(Some(mut spool)) => {
                spool.seek(SeekFrom::Start(0))?;
                Ok(spool)
            }
            _ => Err(io::Error::other("encoded handoff shard was not retained")),
        }
    })
    .await
    .map_err(|error| Status::internal(format!("join shard encoder: {error}")))?
    .map_err(|error| Status::data_loss(error.to_string()))?;
    peers
        .put_shard(target, target_address, identity, encoded)
        .await?;
    Ok(())
}

fn lifecycle_value(state: BlobReferenceState) -> LifecycleValue {
    LifecycleValue {
        ref_count: state.ref_count,
        flags: state.flags,
    }
}

fn merge_lifecycle_timestamps(
    selected: BlobReferenceState,
    observed: impl IntoIterator<Item = BlobReferenceState>,
) -> BlobReferenceState {
    let logical = lifecycle_value(selected);
    observed
        .into_iter()
        .filter(|state| lifecycle_value(*state) == logical)
        .fold(selected, |merged, state| BlobReferenceState {
            ref_count: selected.ref_count,
            flags: selected.flags,
            created_at: merged.created_at.min(state.created_at),
            updated_at: merged.updated_at.max(state.updated_at),
        })
}

fn blob_key(reference: &BlobRef) -> [u8; 40] {
    let mut key = [0_u8; 40];
    key[..32].copy_from_slice(&reference.hash);
    key[32..].copy_from_slice(&reference.length.to_be_bytes());
    key
}

#[derive(Clone)]
struct HandoffPayloadTransport {
    peers: DataPeerTransport,
}

impl PayloadReadPlacementView for HandoffTopology {
    fn cluster_id(&self) -> keldra_consensus::ClusterId {
        self.cluster_id()
    }

    fn fence(&self) -> keldra_store::PlacementLogId {
        self.fence()
    }

    fn placement_nodes(&self) -> &[crate::placement::PlacementNode] {
        self.old_nodes()
    }

    fn address(&self, node: NodeId) -> Option<&str> {
        self.address(node)
    }
}

#[tonic::async_trait]
impl PayloadReadTransport for HandoffPayloadTransport {
    async fn get_small(
        &self,
        _fence: keldra_store::PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        let bytes = self
            .peers
            .get_small_content(target, address, reference)
            .await
            .map_err(read_transport_error)?;
        destination
            .write_all(&bytes)
            .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))
    }

    async fn put_small(
        &self,
        _fence: keldra_store::PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), PayloadReadTransportError> {
        self.peers
            .put_small_content(target, address, reference, bytes)
            .await
            .map_err(read_transport_error)
    }

    async fn get_shard(
        &self,
        _fence: keldra_store::PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        let mut stream = self
            .peers
            .get_shard(target, address, identity)
            .await
            .map_err(read_transport_error)?;
        let mut offset = 0_u64;
        while let Some(frame) = stream.message().await.map_err(read_transport_error)? {
            if frame.schema_version != DATA_PEER_SCHEMA_VERSION
                || frame.offset != offset
                || frame.content.len() > DATA_PEER_FRAME_BYTES
            {
                return Err(PayloadReadTransportError::InvalidArtifact(
                    "peer shard stream is malformed".into(),
                ));
            }
            destination
                .write_all(&frame.content)
                .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            offset = offset
                .checked_add(frame.content.len() as u64)
                .ok_or_else(|| {
                    PayloadReadTransportError::InvalidArtifact("peer shard stream overflow".into())
                })?;
            if frame.end {
                return Ok(());
            }
        }
        Err(PayloadReadTransportError::InvalidArtifact(
            "peer shard stream ended without a final frame".into(),
        ))
    }

    async fn put_shard(
        &self,
        _fence: keldra_store::PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        source: Box<dyn Read + Send>,
    ) -> Result<(), PayloadReadTransportError> {
        self.peers
            .put_shard(target, address, identity, source)
            .await
            .map(|_| ())
            .map_err(read_transport_error)
    }
}

fn read_transport_error(status: Status) -> PayloadReadTransportError {
    match status.code() {
        Code::NotFound => PayloadReadTransportError::NotFound,
        Code::DataLoss | Code::FailedPrecondition | Code::InvalidArgument => {
            PayloadReadTransportError::InvalidArtifact(status.to_string())
        }
        _ => PayloadReadTransportError::Unavailable(status.to_string()),
    }
}

#[derive(Clone)]
struct SharedSpool(Arc<Mutex<Box<dyn PayloadReadSpool>>>);

impl Write for SharedSpool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("handoff spool lock is poisoned"))?
            .write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("handoff spool lock is poisoned"))?
            .flush()
    }
}

struct OwnedSpoolReader(Box<dyn PayloadReadSpool>);

impl Read for OwnedSpoolReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.0.read(bytes)
    }
}

enum EncodeOutput {
    Target(Option<Box<dyn PayloadReadSpool>>),
    Discard(io::Sink),
}

impl Write for EncodeOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Target(Some(spool)) => spool.write(bytes),
            Self::Target(None) => Err(io::Error::other("target shard spool is missing")),
            Self::Discard(sink) => sink.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Target(Some(spool)) => spool.flush(),
            Self::Target(None) => Err(io::Error::other("target shard spool is missing")),
            Self::Discard(sink) => sink.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::join_peer::handoff::HandoffEndpoint;
    use crate::placement::PlacementNode;

    fn reference(byte: u8) -> BlobRef {
        BlobRef {
            hash: [byte; 32],
            length: u64::from(byte),
        }
    }

    fn lifecycle() -> BlobReferenceState {
        BlobReferenceState {
            ref_count: 1,
            flags: 0,
            created_at: 1,
            updated_at: 1,
        }
    }

    fn complete(reference: &BlobRef) -> PayloadArtifactSnapshot {
        PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Complete(reference.clone()),
            lifecycle: lifecycle(),
        }
    }

    fn shard(reference: &BlobRef, ordinal: u16) -> PayloadArtifactSnapshot {
        PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Shard(ShardIdentity::new(
                reference.clone(),
                ordinal,
            )),
            lifecycle: lifecycle(),
        }
    }

    fn source(node: u64) -> MergeSource<PayloadArtifactSnapshot, PayloadArtifactCursor> {
        MergeSource::new(HandoffEndpoint {
            node_id: NodeId(node),
            address: format!("node-{node}"),
        })
    }

    fn install_page(
        source: &mut MergeSource<PayloadArtifactSnapshot, PayloadArtifactCursor>,
        artifacts: Vec<PayloadArtifactSnapshot>,
        has_more: bool,
    ) -> usize {
        let count = artifacts.len();
        let next_cursor = has_more.then(|| {
            PayloadArtifactCursor::from_key(
                artifacts
                    .last()
                    .expect("continued test page is non-empty")
                    .identity
                    .handoff_order_key(),
            )
            .unwrap()
        });
        source
            .install_page(artifacts, next_cursor, |artifact| {
                Ok(artifact.identity.handoff_order_key())
            })
            .unwrap();
        count
    }

    #[test]
    fn one_blob_is_grouped_across_page_boundaries() {
        let a = reference(1);
        let b = reference(2);
        let mut sources = vec![source(1), source(2)];
        install_page(&mut sources[0], vec![complete(&a)], true);
        install_page(&mut sources[1], vec![shard(&a, 0), complete(&b)], false);
        let identity = blob_key(&a);
        assert_eq!(
            blob_key_from_artifact_key(&next_key(&sources).unwrap()).unwrap(),
            identity
        );

        let mut observed = BlobArtifacts::default();
        let first = drain_blob_identity(
            &mut sources,
            identity,
            &mut observed,
            ErasureProfile::default(),
        )
        .unwrap();
        assert_eq!(
            first,
            IdentityDrain {
                needs_page: true,
                consumed: 2
            }
        );
        install_page(&mut sources[0], vec![shard(&a, 1), complete(&b)], false);
        let second = drain_blob_identity(
            &mut sources,
            identity,
            &mut observed,
            ErasureProfile::default(),
        )
        .unwrap();
        assert_eq!(
            second,
            IdentityDrain {
                needs_page: false,
                consumed: 1
            }
        );
        assert_eq!(observed.reference.as_ref(), Some(&a));
        assert_eq!(observed.evidence_len(), 3);
        assert_eq!(
            blob_key_from_artifact_key(&next_key(&sources).unwrap()).unwrap(),
            blob_key(&b)
        );
    }

    #[test]
    fn duplicate_artifact_identities_from_distinct_sources_form_one_group() {
        let a = reference(1);
        let mut sources = vec![source(1), source(2)];
        for source in &mut sources {
            install_page(source, vec![complete(&a), shard(&a, 0)], false);
        }
        let mut observed = BlobArtifacts::default();
        let drained = drain_blob_identity(
            &mut sources,
            blob_key(&a),
            &mut observed,
            ErasureProfile::default(),
        )
        .unwrap();

        assert_eq!(
            drained,
            IdentityDrain {
                needs_page: false,
                consumed: 4
            }
        );
        assert_eq!(observed.complete.len(), 2);
        assert_eq!(observed.shards.len(), 2);
        assert!(next_key(&sources).is_none());
    }

    #[test]
    fn an_absent_source_does_not_hide_another_sources_identity() {
        let a = reference(1);
        let mut sources = vec![source(1), source(2)];
        install_page(&mut sources[0], Vec::new(), false);
        install_page(&mut sources[1], vec![complete(&a)], false);
        let mut observed = BlobArtifacts::default();
        let drained = drain_blob_identity(
            &mut sources,
            blob_key(&a),
            &mut observed,
            ErasureProfile::default(),
        )
        .unwrap();

        assert_eq!(
            drained,
            IdentityDrain {
                needs_page: false,
                consumed: 1
            }
        );
        assert_eq!(
            observed.complete.keys().copied().collect::<Vec<_>>(),
            [NodeId(2)]
        );
    }

    #[test]
    fn resident_evidence_is_bounded_by_one_page_per_source_plus_one_blob() {
        const SOURCE_COUNT: usize = 3;
        const PAGE_SIZE: usize = 2;
        let a = reference(1);
        let b = reference(2);
        let mut sources = (1..=SOURCE_COUNT as u64).map(source).collect::<Vec<_>>();
        let mut resident_pages = 0;
        for source in &mut sources {
            resident_pages += install_page(source, vec![complete(&a), shard(&a, 0)], true);
        }
        assert_eq!(resident_pages, SOURCE_COUNT * PAGE_SIZE);

        let mut current = BlobArtifacts::default();
        let first = drain_blob_identity(
            &mut sources,
            blob_key(&a),
            &mut current,
            ErasureProfile::default(),
        )
        .unwrap();
        resident_pages -= first.consumed;
        assert!(first.needs_page);
        assert!(resident_pages <= SOURCE_COUNT * PAGE_SIZE);
        for source in &mut sources {
            resident_pages += install_page(source, vec![shard(&a, 1), complete(&b)], false);
        }
        assert!(resident_pages <= SOURCE_COUNT * PAGE_SIZE);
        assert!(
            resident_pages + current.evidence_len()
                <= SOURCE_COUNT * PAGE_SIZE + current.evidence_len()
        );

        let second = drain_blob_identity(
            &mut sources,
            blob_key(&a),
            &mut current,
            ErasureProfile::default(),
        )
        .unwrap();
        resident_pages -= second.consumed;
        assert!(!second.needs_page);
        assert_eq!(current.evidence_len(), SOURCE_COUNT * 3);
        assert!(resident_pages <= SOURCE_COUNT * PAGE_SIZE);

        drop(current);
        let mut next = BlobArtifacts::default();
        let next_drain = drain_blob_identity(
            &mut sources,
            blob_key(&b),
            &mut next,
            ErasureProfile::default(),
        )
        .unwrap();
        resident_pages -= next_drain.consumed;
        assert_eq!(next.evidence_len(), SOURCE_COUNT);
        assert_eq!(resident_pages, 0);
    }

    #[test]
    fn lifecycle_merge_keeps_logical_state_and_safe_timestamp_extremes() {
        let selected = BlobReferenceState {
            ref_count: 7,
            flags: 0,
            created_at: 20,
            updated_at: 30,
        };
        let merged = merge_lifecycle_timestamps(
            selected,
            [
                BlobReferenceState {
                    created_at: 10,
                    updated_at: 25,
                    ..selected
                },
                BlobReferenceState {
                    created_at: 15,
                    updated_at: 90,
                    ..selected
                },
                BlobReferenceState {
                    ref_count: 8,
                    created_at: 1,
                    updated_at: 100,
                    ..selected
                },
            ],
        );

        assert_eq!(merged.ref_count, 7);
        assert_eq!(merged.flags, 0);
        assert_eq!(merged.created_at, 10);
        assert_eq!(merged.updated_at, 90);
    }

    #[test]
    fn zero_count_retry_reconciles_only_artifacts_already_created() {
        let reference = BlobRef {
            hash: [9; 32],
            length: keldra_store::SMALL_BLOB_MAX_BYTES as u64 + 1,
        };
        let stale = PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Complete(reference.clone()),
            lifecycle: BlobReferenceState {
                ref_count: 1,
                flags: 0,
                created_at: 1,
                updated_at: 2,
            },
        };
        let mut artifacts = BlobArtifacts::default();
        artifacts.reference = Some(reference.clone());
        artifacts.complete.insert(NodeId(2), stale);
        let zero = BlobReferenceState {
            ref_count: 0,
            flags: 0,
            created_at: 1,
            updated_at: 3,
        };

        let complete =
            existing_complete_targets(&reference, &artifacts, &[NodeId(1), NodeId(2)], zero);
        assert_eq!(complete.len(), 1);
        assert_eq!(complete[0].0, NodeId(2));
        assert_eq!(complete[0].1.lifecycle, zero);

        let nodes = [NodeId(1), NodeId(2), NodeId(3)]
            .map(|node| PlacementNode::new(node, NonZeroU32::new(1_000_000).unwrap()));
        let PayloadPlacement::Large(placement) = select_payload_placement(
            keldra_consensus::ClusterId(*b"handoff-retry-v1"),
            &reference,
            ErasureProfile::default(),
            &nodes,
        ) else {
            panic!("three nodes must select default erasure placement")
        };
        let present = placement.shards()[1];
        artifacts.shards.insert(
            (present.owner(), present.ordinal()),
            PayloadArtifactSnapshot {
                identity: PayloadArtifactIdentity::Shard(ShardIdentity::new(
                    reference.clone(),
                    present.ordinal(),
                )),
                lifecycle: BlobReferenceState {
                    ref_count: 1,
                    ..zero
                },
            },
        );
        let shards = existing_shard_targets(&reference, &artifacts, placement.shards(), zero);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].0, present.owner());
        assert_eq!(shards[0].1.lifecycle, zero);
        assert_eq!(
            shards[0].1.identity,
            PayloadArtifactIdentity::Shard(ShardIdentity::new(reference, present.ordinal()))
        );
    }
}
