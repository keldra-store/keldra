//! Verified child ranges, independent of whole-object reconstruction.
use super::*;
use tonic::Status;

impl DistributedPayloadReader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_verified_range(
        &self,
        placement: &(impl PayloadReadPlacementView + ?Sized),
        reference: &BlobRef,
        offset: u64,
        length: u64,
        maximum_bytes: usize,
        child_hash: [u8; 32],
        deadline: Option<tokio::time::Instant>,
        memory: Arc<dyn PayloadRangeMemory>,
    ) -> Result<Vec<u8>, Status> {
        let count = usize::try_from(length)
            .map_err(|_| Status::resource_exhausted("range exceeds platform"))?;
        if count > maximum_bytes
            || offset
                .checked_add(length)
                .is_none_or(|end| end > reference.length)
        {
            return Err(Status::resource_exhausted("range exceeds admitted bound"));
        }
        if length == 0 {
            if blake3::hash(&[]).as_bytes() != &child_hash {
                return Err(Status::data_loss("empty child range hash mismatch"));
            }
            return Ok(Vec::new());
        }
        let desired = select_payload_placement(
            placement.cluster_id(),
            reference,
            self.profile,
            placement.placement_nodes(),
        );
        let complete_owners = match &desired {
            PayloadPlacement::Small(small) => Some(small.owners().to_vec()),
            PayloadPlacement::LargeComplete(complete) => Some(complete.owners().to_vec()),
            PayloadPlacement::Large(_) => None,
        };
        if let Some(mut owners) = complete_owners {
            let mut seen = owners.iter().copied().collect::<HashSet<_>>();
            owners.extend(
                placement
                    .placement_nodes()
                    .iter()
                    .map(|node| node.node_id())
                    .filter(|node| seen.insert(*node)),
            );
            let mut reads = tokio::task::JoinSet::new();
            let mut pending = std::collections::VecDeque::from(owners);
            let mut denied = false;
            let mut corrupt = false;
            loop {
                while let Some(owner) = pending.pop_front() {
                    let Some(address) = placement.address(owner) else {
                        continue;
                    };
                    let address = address.to_owned();
                    let transport = self.transport.clone();
                    let reference = reference.clone();
                    let fence = placement.fence();
                    let attempt = count
                        .checked_mul(2)
                        .and_then(|bytes| bytes.checked_add(1024))
                        .ok_or_else(|| {
                            Status::resource_exhausted("range transport admission overflow")
                        })?;
                    let lease = match memory.try_reserve(attempt) {
                        Ok(lease) => Arc::new(lease),
                        Err(_) => {
                            pending.push_front(owner);
                            denied = true;
                            break;
                        }
                    };
                    reads.spawn(async move {
                        let bytes = transport
                            .get_range(
                                fence,
                                owner,
                                &address,
                                &reference,
                                offset,
                                length,
                                None,
                                deadline,
                                lease.clone(),
                            )
                            .await?;
                        Ok::<_, PayloadReadTransportError>((bytes, lease))
                    });
                }
                if let Some(result) = reads.join_next().await {
                    if let Ok(Ok((bytes, _lease))) = result {
                        if bytes.len() == count && blake3::hash(&bytes).as_bytes() == &child_hash {
                            reads.abort_all();
                            return Ok(bytes);
                        }
                        corrupt = true;
                    } else if matches!(
                        result,
                        Ok(Err(PayloadReadTransportError::InvalidArtifact(_)))
                    ) {
                        corrupt = true;
                    }
                } else {
                    if denied && !pending.is_empty() {
                        return Err(Status::resource_exhausted(
                            "complete child range scratch admission unavailable",
                        ));
                    }
                    break;
                }
            }
            return Err(if corrupt {
                Status::data_loss("complete child range failed published identity verification")
            } else {
                Status::unavailable("no complete owner supplied a verified child range")
            });
        }
        let PayloadPlacement::Large(large) = desired else {
            unreachable!()
        };
        let mut chunks = (0..self.profile.total_shards())
            .map(|_| None)
            .collect::<Vec<Option<std::io::Cursor<Vec<u8>>>>>();
        let mut leases = (0..self.profile.total_shards())
            .map(|_| None)
            .collect::<Vec<Option<PayloadRangeLease>>>();
        // Decoding holds one encoded chunk and one reconstructed chunk per
        // shard at a time; the separately-admitted caller owns final output.
        let stripe_scratch = usize::from(self.profile.total_shards())
            .checked_mul(self.profile.stripe_unit() as usize)
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| Status::resource_exhausted("EC stripe scratch admission overflow"))?;
        let _reconstruction = memory.try_reserve(stripe_scratch)?;
        let mut reads = tokio::task::JoinSet::new();
        let mut pending = large
            .shards()
            .iter()
            .collect::<std::collections::VecDeque<_>>();
        let mut denied = false;
        let mut corrupt = false;
        loop {
            while let Some(shard) = pending.pop_front() {
                let owner = shard.owner();
                let ordinal = shard.ordinal();
                let Some(address) = placement.address(owner) else {
                    continue;
                };
                let address = address.to_owned();
                let transport = self.transport.clone();
                let reference = reference.clone();
                let fence = placement.fence();
                let (header, _, extent, _) = self
                    .codec
                    .range_extent(&reference, ordinal, offset, length)
                    .map_err(|error| Status::data_loss(error.to_string()))?;
                let encoded_length = header
                    .checked_add(extent)
                    .ok_or_else(|| Status::resource_exhausted("encoded range length overflow"))?;
                let expected = usize::try_from(encoded_length)
                    .map_err(|_| Status::resource_exhausted("encoded range exceeds platform"))?;
                let attempt = expected
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(1024))
                    .ok_or_else(|| Status::resource_exhausted("EC transport admission overflow"))?;
                let lease = match memory.try_reserve(attempt) {
                    Ok(lease) => Arc::new(lease),
                    Err(_) => {
                        pending.push_front(shard);
                        denied = true;
                        break;
                    }
                };
                reads.spawn(async move {
                    let bytes = transport
                        .get_range(
                            fence,
                            owner,
                            &address,
                            &reference,
                            offset,
                            length,
                            Some(ordinal),
                            deadline,
                            lease.clone(),
                        )
                        .await?;
                    if bytes.len() != expected {
                        return Err(PayloadReadTransportError::InvalidArtifact(
                            "encoded shard range length mismatch".into(),
                        ));
                    }
                    Ok::<_, PayloadReadTransportError>((ordinal, bytes, lease))
                });
            }
            if let Some(result) = reads.join_next().await {
                match result {
                    Ok(Ok((ordinal, bytes, lease))) => {
                        chunks[usize::from(ordinal)] = Some(std::io::Cursor::new(bytes));
                        leases[usize::from(ordinal)] = Some(lease);
                    }
                    Ok(Err(PayloadReadTransportError::InvalidArtifact(_))) => corrupt = true,
                    _ => {}
                }
                if chunks.iter().flatten().count() >= usize::from(self.profile.data_shards()) {
                    for reader in chunks.iter_mut().flatten() {
                        reader.set_position(0);
                    }
                    if let Ok(bytes) =
                        self.codec
                            .reconstruct_range(reference, offset, length, &mut chunks)
                    {
                        if bytes.len() == count && blake3::hash(&bytes).as_bytes() == &child_hash {
                            reads.abort_all();
                            return Ok(bytes);
                        }
                    }
                    corrupt = true;
                    for (chunk, lease) in chunks.iter().zip(&mut leases) {
                        if chunk.is_none() {
                            *lease = None;
                        }
                    }
                }
            } else {
                if denied && !pending.is_empty() {
                    return Err(Status::resource_exhausted(
                        "insufficient admitted EC child range quorum",
                    ));
                }
                break;
            }
        }
        for reader in chunks.iter_mut().flatten() {
            reader.set_position(0);
        }
        let bytes = self
            .codec
            .reconstruct_range(reference, offset, length, &mut chunks)
            .map_err(|error| match error {
                ErasureError::TooFewValidChunks { .. } if !corrupt => {
                    Status::unavailable(format!("insufficient valid child range shards: {error}"))
                }
                _ => Status::data_loss(format!("child range reconstruction: {error}")),
            })?;
        if bytes.len() != count || blake3::hash(&bytes).as_bytes() != &child_hash {
            return Err(Status::data_loss("reconstructed child range hash mismatch"));
        }
        Ok(bytes)
    }
}
