//! Authenticated, fenced bounded reads of immutable payload extents.
use super::*;

impl DataPeerService {
    pub(super) async fn serve_payload_range(
        &self,
        mut request: Request<wire::PayloadRangeRequest>,
    ) -> Result<Response<wire::PayloadRangeResponse>, Status> {
        let peer = request.get_ref().peer.clone();
        self.authorize(&mut request, peer.as_ref(), PeerRpcKind::DataPlane)?;
        let reference = parse_blob(request.get_ref().blob.as_ref())?;
        if reference.length > self.max_blob_bytes {
            return Err(Status::resource_exhausted(
                "range blob exceeds configured maximum",
            ));
        }
        let fence = keldra_store::PlacementLogId {
            term: request.get_ref().placement_fence_term,
            index: request.get_ref().placement_fence_index,
        };
        self.mutation_admission.require_fence(fence)?;
        let offset = request.get_ref().offset;
        let length = request.get_ref().length;
        let ordinal = request.get_ref().shard_ordinal;
        let maximum = MAX_DATA_PEER_MESSAGE_BYTES.saturating_sub(1024);
        if length > maximum as u64
            || offset
                .checked_add(length)
                .is_none_or(|end| end > reference.length)
        {
            return Err(Status::resource_exhausted(
                "payload range exceeds admitted bound",
            ));
        }
        let metadata = request.metadata().clone();
        let store = self.store.clone();
        let codec = self.codec.clone();
        let bytes = self
            .bounded(&metadata, async move {
                match ordinal {
                    None => tokio::task::spawn_blocking(move || {
                        store.read_complete_copy_range(&reference, offset, length, maximum)
                    })
                    .await
                    .map_err(|error| Status::internal(format!("join complete range: {error}")))?
                    .map_err(map_mutation_error),
                    Some(ordinal) => {
                        let ordinal = u16::try_from(ordinal).map_err(|_| {
                            Status::invalid_argument("shard range ordinal is invalid")
                        })?;
                        let identity = ShardIdentity::new(reference, ordinal);
                        tokio::task::spawn_blocking(move || {
                            store.get_shard_range(&codec, &identity, offset, length, maximum)
                        })
                        .await
                        .map_err(|error| Status::internal(format!("join shard range: {error}")))?
                        .map_err(map_shard_error)
                    }
                }
            })
            .await?;
        self.mutation_admission.require_fence(fence)?;
        Ok(Response::new(wire::PayloadRangeResponse {
            schema_version: DATA_PEER_SCHEMA_VERSION,
            content: bytes,
        }))
    }
}
