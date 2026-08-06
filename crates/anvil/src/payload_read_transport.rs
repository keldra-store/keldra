//! Production byte-plane adapter for distributed object reads.
//!
//! The adapter keeps local reads local and uses the existing typed, mandatory-
//! mTLS data-peer transport for every other current placement owner. It adds no
//! persistence and does not make placement decisions.

use std::io::{Read, Write};
use std::sync::Arc;

use anvil_consensus::NodeId;
use anvil_store::{
    BlobRef, ErasureCodec, ErasureError, ErasureProfile, PayloadStoreError, PlacementLogId,
    ShardIdentity, ShardStoreError, Store,
};
use tonic::{Code, Status};

use crate::data_peer::{DATA_PEER_FRAME_BYTES, DATA_PEER_SCHEMA_VERSION, DataPeerTransport};
use crate::payload_read::{PayloadReadTransport, PayloadReadTransportError};

#[derive(Clone)]
pub(crate) struct StorePayloadReadTransport {
    local_node: NodeId,
    store: Store,
    peers: DataPeerTransport,
    codec: Arc<ErasureCodec>,
}

impl StorePayloadReadTransport {
    pub(crate) fn new(
        local_node: NodeId,
        store: Store,
        peers: DataPeerTransport,
        profile: ErasureProfile,
    ) -> Result<Self, ErasureError> {
        Ok(Self {
            local_node,
            store,
            peers,
            codec: Arc::new(ErasureCodec::new(profile)?),
        })
    }

    fn is_local(&self, target: NodeId) -> bool {
        target == self.local_node
    }
}

#[tonic::async_trait]
impl PayloadReadTransport for StorePayloadReadTransport {
    async fn get_small(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        let bytes = if self.is_local(target) {
            self.store
                .read_small_copy(reference)
                .map_err(map_payload_error)?
        } else {
            self.peers
                .get_small_content(target, address, reference)
                .await
                .map_err(map_peer_error)?
        };
        destination
            .write_all(&bytes)
            .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))
    }

    async fn put_small(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), PayloadReadTransportError> {
        if self.is_local(target) {
            self.store
                .seal_small_copy(reference, bytes)
                .await
                .map(|_| ())
                .map_err(map_payload_error)
        } else {
            self.peers
                .put_small_content(target, address, reference, bytes)
                .await
                .map_err(map_peer_error)
        }
    }

    async fn get_complete(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        address: &str,
        reference: &BlobRef,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        if self.is_local(target) {
            match self
                .store
                .complete_copy_state(reference)
                .await
                .map_err(map_payload_error)?
            {
                anvil_store::PayloadArtifactState::Valid => {}
                anvil_store::PayloadArtifactState::Missing => {
                    return Err(PayloadReadTransportError::NotFound);
                }
                anvil_store::PayloadArtifactState::Corrupt => {
                    return Err(PayloadReadTransportError::InvalidArtifact(
                        "local complete payload failed integrity verification".into(),
                    ));
                }
            }
            let mut reader = self
                .store
                .open_blob(reference)
                .await
                .map_err(|error| PayloadReadTransportError::Unavailable(error.to_string()))?;
            let mut frame = [0_u8; DATA_PEER_FRAME_BYTES];
            loop {
                let read = reader.read(&mut frame).await.map_err(|error| {
                    PayloadReadTransportError::InvalidArtifact(error.to_string())
                })?;
                if read == 0 {
                    return Ok(());
                }
                destination
                    .write_all(&frame[..read])
                    .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            }
        }

        let mut stream = self
            .peers
            .get_complete_source(target, address, reference)
            .await
            .map_err(map_peer_error)?;
        let mut offset = 0_u64;
        while let Some(frame) = stream.message().await.map_err(map_peer_error)? {
            if frame.schema_version != DATA_PEER_SCHEMA_VERSION
                || frame.offset != offset
                || frame.content.len() > DATA_PEER_FRAME_BYTES
                || (frame.content.is_empty() && !frame.end)
            {
                return Err(PayloadReadTransportError::InvalidArtifact(
                    "peer complete-payload stream is malformed".into(),
                ));
            }
            let next = offset
                .checked_add(frame.content.len() as u64)
                .filter(|next| *next <= reference.length)
                .ok_or_else(|| {
                    PayloadReadTransportError::InvalidArtifact(
                        "peer complete-payload stream exceeds its declared length".into(),
                    )
                })?;
            destination
                .write_all(&frame.content)
                .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            offset = next;
            if frame.end {
                return if offset == reference.length {
                    Ok(())
                } else {
                    Err(PayloadReadTransportError::InvalidArtifact(
                        "peer complete-payload stream ended at another length".into(),
                    ))
                };
            }
        }
        Err(PayloadReadTransportError::InvalidArtifact(
            "peer complete-payload stream ended without a final frame".into(),
        ))
    }

    async fn get_shard(
        &self,
        _fence: PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        destination: &mut (dyn Write + Send),
    ) -> Result<(), PayloadReadTransportError> {
        if self.is_local(target) {
            let mut reader = self
                .store
                .get_shard(&self.codec, identity)
                .map_err(map_shard_error)?;
            let mut frame = [0_u8; DATA_PEER_FRAME_BYTES];
            loop {
                let read = reader
                    .read(&mut frame)
                    .map_err(|error| PayloadReadTransportError::Unavailable(error.to_string()))?;
                if read == 0 {
                    return Ok(());
                }
                destination
                    .write_all(&frame[..read])
                    .map_err(|error| PayloadReadTransportError::Destination(error.to_string()))?;
            }
        }

        let mut stream = self
            .peers
            .get_shard(target, address, identity)
            .await
            .map_err(map_peer_error)?;
        let mut offset = 0_u64;
        while let Some(frame) = stream.message().await.map_err(map_peer_error)? {
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
                    PayloadReadTransportError::InvalidArtifact(
                        "peer shard stream offset overflowed".into(),
                    )
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
        _fence: PlacementLogId,
        target: NodeId,
        address: &str,
        identity: &ShardIdentity,
        source: Box<dyn Read + Send>,
    ) -> Result<(), PayloadReadTransportError> {
        if self.is_local(target) {
            self.store
                .seal_shard(&self.codec, identity, source)
                .await
                .map(|_| ())
                .map_err(map_shard_error)
        } else {
            self.peers
                .put_shard(target, address, identity, source)
                .await
                .map(|_| ())
                .map_err(map_peer_error)
        }
    }
}

fn map_payload_error(error: PayloadStoreError) -> PayloadReadTransportError {
    match error {
        PayloadStoreError::CompleteCopyMissing => PayloadReadTransportError::NotFound,
        PayloadStoreError::CompleteCopyCorrupt
        | PayloadStoreError::NotSmall
        | PayloadStoreError::NotLarge
        | PayloadStoreError::Erasure(_)
        | PayloadStoreError::Shard(ShardStoreError::MalformedIdentity)
        | PayloadStoreError::Shard(ShardStoreError::UnsupportedFragmentFormat(_))
        | PayloadStoreError::Shard(ShardStoreError::Erasure(_)) => {
            PayloadReadTransportError::InvalidArtifact(error.to_string())
        }
        PayloadStoreError::Shard(ShardStoreError::NotFound) => PayloadReadTransportError::NotFound,
        PayloadStoreError::Mutation(_)
        | PayloadStoreError::Storage(_)
        | PayloadStoreError::Shard(ShardStoreError::Storage(_)) => {
            PayloadReadTransportError::Unavailable(error.to_string())
        }
    }
}

fn map_shard_error(error: ShardStoreError) -> PayloadReadTransportError {
    match error {
        ShardStoreError::NotFound => PayloadReadTransportError::NotFound,
        ShardStoreError::MalformedIdentity
        | ShardStoreError::UnsupportedFragmentFormat(_)
        | ShardStoreError::Erasure(_) => {
            PayloadReadTransportError::InvalidArtifact(error.to_string())
        }
        ShardStoreError::Storage(_) => PayloadReadTransportError::Unavailable(error.to_string()),
    }
}

fn map_peer_error(status: Status) -> PayloadReadTransportError {
    match status.code() {
        Code::NotFound => PayloadReadTransportError::NotFound,
        Code::DataLoss | Code::FailedPrecondition | Code::InvalidArgument => {
            PayloadReadTransportError::InvalidArtifact(status.to_string())
        }
        _ => PayloadReadTransportError::Unavailable(status.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_failures_have_stable_read_classifications() {
        assert_eq!(
            map_peer_error(Status::not_found("missing")),
            PayloadReadTransportError::NotFound
        );
        assert!(matches!(
            map_peer_error(Status::data_loss("corrupt")),
            PayloadReadTransportError::InvalidArtifact(_)
        ));
        assert!(matches!(
            map_peer_error(Status::unavailable("offline")),
            PayloadReadTransportError::Unavailable(_)
        ));
    }
}
