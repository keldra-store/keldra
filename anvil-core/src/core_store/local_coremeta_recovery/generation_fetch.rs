use super::*;

impl CoreStore {
    pub(super) async fn exchange_coremeta_inventory(
        &self,
        peer: &RecoveryPeer,
        root_key_hash: &str,
        after: Option<CoreMetaInventoryCursor>,
        through_generation: u64,
        max_entries: u32,
    ) -> Result<CoreMetaInventory> {
        let bearer = self.coremeta_recovery_bearer()?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreMeta recovery bearer token")?;
        let request_body = CoreMetaInventoryRequest {
            header: Some(self.internal_request_header("coremeta.exchange_inventory")?),
            root_key_hash: root_key_hash.to_string(),
            after,
            through_generation,
            max_entries,
            max_bytes: RECOVERY_PAGE_BYTES,
        };
        let operation = self.internal_grpc_request(
            &peer.public_api_addr,
            "exchange CoreMeta recovery inventory",
            move |channel| {
                let mut client = CoreMetaReplicationInternalClient::new(channel);
                let body = request_body.clone();
                let authorization = authorization.clone();
                async move {
                    let mut request = tonic::Request::new(body);
                    request
                        .metadata_mut()
                        .insert("authorization", authorization);
                    client
                        .exchange_core_meta_inventory(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
        );
        let inventory = tokio::time::timeout(RECOVERY_RPC_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow!("CoreMeta inventory request timed out"))??;
        self.validate_coremeta_recovery_inventory(root_key_hash, &inventory)?;
        Ok(inventory)
    }

    pub(super) async fn fetch_coremeta_generation(
        &self,
        source: &RecoverySource,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Vec<u8>> {
        self.fetch_coremeta_generation_inner(source, root_key_hash, generation, None)
            .await
    }

    pub(super) async fn fetch_committed_coremeta_generation(
        &self,
        source: &RecoverySource,
        root_key_hash: &str,
        generation: u64,
        committed_certificate_hash: &str,
        committed_publication_bundle: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        self.fetch_coremeta_generation_inner(
            source,
            root_key_hash,
            generation,
            Some((committed_certificate_hash, committed_publication_bundle)),
        )
        .await
    }

    async fn fetch_coremeta_generation_inner(
        &self,
        source: &RecoverySource,
        root_key_hash: &str,
        generation: u64,
        committed: Option<(&str, Option<&[u8]>)>,
    ) -> Result<Vec<u8>> {
        let mut cursor = self.coremeta_recovery_cursor(root_key_hash)?;
        let mut publication_bundle = None;
        for _ in 0..RECOVERY_MAX_PAGES_PER_GENERATION {
            let frames = self
                .catch_up_coremeta_page(&source.peer, root_key_hash, cursor.clone(), generation)
                .await?;
            if frames.is_empty() {
                bail!("CoreMeta recovery catch-up returned no frames");
            }
            let mut history_complete = false;
            for frame in frames {
                let descriptor = frame
                    .descriptor
                    .as_ref()
                    .ok_or_else(|| anyhow!("CoreMeta recovery frame has no descriptor"))?;
                if descriptor.root_key_hash != root_key_hash || descriptor.generation != generation
                {
                    bail!("CoreMeta recovery frame skipped or changed generation");
                }
                if frame.final_generation != generation
                    || frame.retention_floor_generation == 0
                    || frame.retention_floor_generation > generation
                {
                    bail!("CoreMeta recovery frame changed its captured history bounds");
                }
                if let Some((certificate_hash, expected_bundle)) = committed {
                    if descriptor.certificate_hash != certificate_hash {
                        bail!("CoreMeta recovery source has a different committed certificate");
                    }
                    if let Some(expected_bundle) = expected_bundle
                        && descriptor.publication_bundle != expected_bundle
                    {
                        bail!("CoreMeta recovery source has a different publication bundle");
                    }
                }
                let required_evidence = self
                    .default_coremeta_quorum_profile()?
                    .certificate_persist_quorum;
                if descriptor.certificate_persist_evidence.len() < required_evidence {
                    let replica_node_ids = descriptor
                        .certificate_persist_evidence
                        .iter()
                        .filter_map(|evidence| {
                            decode_deterministic_proto::<
                                crate::anvil_api::CoreMetaCertificatePersistReceipt,
                            >(
                                &evidence.evidence,
                                "CoreMeta recovery certificate persistence receipt",
                            )
                            .ok()
                            .map(|receipt| receipt.replica_node_id)
                        })
                        .collect::<Vec<_>>();
                    tracing::warn!(
                        source_node_id = %source.peer.node_id,
                        root_key_hash,
                        generation,
                        transaction_id = %descriptor.transaction_id,
                        evidence_count = descriptor.certificate_persist_evidence.len(),
                        required_evidence,
                        replica_node_ids = %replica_node_ids.join(","),
                        "CoreMeta recovery source returned a generation without certificate persistence quorum"
                    );
                }
                let bundle =
                    decode_coremeta_recovery_publication_bundle(&descriptor.publication_bundle)?;
                if bundle.transaction_id != descriptor.transaction_id
                    || !bundle
                        .scopes
                        .iter()
                        .any(|(bundle_root, bundle_generation)| {
                            bundle_root == root_key_hash && *bundle_generation == generation
                        })
                {
                    bail!("CoreMeta recovery descriptor is outside its publication bundle");
                }
                match &publication_bundle {
                    Some(existing) if existing != &descriptor.publication_bundle => {
                        bail!("CoreMeta recovery generation changed publication bundle")
                    }
                    None => publication_bundle = Some(descriptor.publication_bundle.clone()),
                    _ => {}
                }
                cursor = frame.next_cursor.clone();
                history_complete = frame.history_complete;
                match committed {
                    Some((certificate_hash, expected_bundle)) => {
                        self.install_committed_coremeta_generation_frame(
                            &frame,
                            certificate_hash,
                            expected_bundle,
                        )
                        .await?;
                    }
                    None => {
                        self.install_coremeta_generation_frame(&frame).await?;
                    }
                }
            }
            if history_complete {
                return publication_bundle
                    .ok_or_else(|| anyhow!("CoreMeta recovery generation has no bundle"));
            }
        }
        bail!("CoreMeta recovery generation exceeded its bounded page count")
    }

    async fn catch_up_coremeta_page(
        &self,
        peer: &RecoveryPeer,
        root_key_hash: &str,
        after: Option<CoreMetaHistoryCursor>,
        through_generation: u64,
    ) -> Result<Vec<CoreMetaBatchFrame>> {
        let bearer = self.coremeta_recovery_bearer()?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreMeta recovery bearer token")?;
        let request_body = CoreMetaCatchUpRequest {
            header: Some(self.internal_request_header("coremeta.catch_up_partition")?),
            root_key_hash: root_key_hash.to_string(),
            after,
            through_generation,
            max_rows: RECOVERY_PAGE_ROWS,
            max_bytes: RECOVERY_PAGE_BYTES,
        };
        let operation = self.internal_grpc_request(
            &peer.public_api_addr,
            "catch up CoreMeta recovery generation",
            move |channel| {
                let mut client = CoreMetaReplicationInternalClient::new(channel);
                let body = request_body.clone();
                let authorization = authorization.clone();
                async move {
                    let mut request = tonic::Request::new(body);
                    request
                        .metadata_mut()
                        .insert("authorization", authorization);
                    let mut stream = client.catch_up_partition(request).await?.into_inner();
                    let mut frames = Vec::new();
                    while let Some(frame) = stream.message().await? {
                        frames.push(frame);
                    }
                    Ok(frames)
                }
            },
        );
        tokio::time::timeout(RECOVERY_RPC_TIMEOUT, operation)
            .await
            .map_err(|_| anyhow!("CoreMeta catch-up request timed out"))?
    }
}
