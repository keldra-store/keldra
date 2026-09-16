use super::*;

impl DataPeerTransport {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn get_payload_range(
        &self,
        target: NodeId,
        address: &str,
        fence: keldra_store::PlacementLogId,
        reference: &BlobRef,
        offset: u64,
        length: u64,
        ordinal: Option<u16>,
        deadline: Option<tokio::time::Instant>,
        expected: u64,
    ) -> Result<Vec<u8>, Status> {
        let expected = usize::try_from(expected)
            .map_err(|_| Status::resource_exhausted("range extent exceeds platform"))?;
        let decode_bound = expected
            .checked_add(1024)
            .filter(|bound| *bound <= MAX_DATA_PEER_MESSAGE_BYTES)
            .ok_or_else(|| Status::resource_exhausted("range extent exceeds peer message bound"))?;
        let mut request = Request::new(wire::PayloadRangeRequest {
            peer: Some(self.context()),
            blob: Some(wire_blob(reference)),
            placement_fence_term: fence.term,
            placement_fence_index: fence.index,
            offset,
            length,
            shard_ordinal: ordinal.map(u32::from),
        });
        if let Some(deadline) = deadline {
            request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
        }
        let response = {
            let mut client = self
                .client(target, address)?
                .max_decoding_message_size(decode_bound);
            let read = client.get_payload_range(request);
            match deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(deadline, read)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded("payload range caller deadline exceeded")
                        })??
                }
                None => read.await?,
            }
        }
        .into_inner();
        require_response_schema(response.schema_version)?;
        if response.content.len() != expected {
            return Err(Status::data_loss(
                "peer range length differs from requested extent",
            ));
        }
        Ok(response.content)
    }
}
