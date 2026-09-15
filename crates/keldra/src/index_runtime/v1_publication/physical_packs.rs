use super::*;

impl V1ProjectionPublisher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_component_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: &[keldra_index::v1::SealedProjectionDeltaPack],
    ) -> Result<ArtifactPackTable, Status> {
        let inputs = packs
            .iter()
            .map(|pack| {
                (
                    pack.ordinal,
                    projection_pack_path(partition, pack.hash),
                    keldra_index::v1::ProjectionArtifactKind::Pack,
                    pack.hash,
                    pack.bytes.as_slice(),
                )
            })
            .collect();
        self.publish_physical_packs(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            inputs,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_query_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: &[keldra_index::v1::UnpublishedArtifactPack],
    ) -> Result<ArtifactPackTable, Status> {
        let inputs = packs
            .iter()
            .map(|pack| {
                (
                    pack.ordinal,
                    projection_query_run_pack_path(partition, pack.hash),
                    keldra_index::v1::ProjectionArtifactKind::QueryRunPack,
                    pack.hash,
                    pack.bytes.as_slice(),
                )
            })
            .collect();
        self.publish_physical_packs(
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            partition,
            inputs,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_physical_packs(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        partition: ProjectionPartitionIdentity,
        packs: Vec<(
            u32,
            String,
            keldra_index::v1::ProjectionArtifactKind,
            [u8; 32],
            &[u8],
        )>,
    ) -> Result<ArtifactPackTable, Status> {
        if packs.is_empty() {
            return ArtifactPackTable::new(Vec::new()).map_err(index_status);
        }
        let mut publications = Vec::with_capacity(packs.len());
        for (_, path, kind, hash, bytes) in &packs {
            let blob = self.stage(bytes).await?;
            if blob.hash != *hash || blob.length != bytes.len() as u64 {
                return Err(Status::data_loss(
                    "staged v1 physical pack changed its exact bytes",
                ));
            }
            publications.push(request(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                projection_artifact_routing_id(partition.family_id, *kind, *hash)
                    .map_err(index_status)?,
                path.clone(),
                blob,
                None,
            ));
        }
        let outcomes = self.artifacts.publish_immutable_many(publications).await?;
        if outcomes.len() != packs.len() {
            return Err(Status::data_loss(
                "v1 physical pack outcomes differ from their inputs",
            ));
        }
        let mut references = Vec::with_capacity(packs.len());
        for ((ordinal, path, _, hash, bytes), outcome) in packs.into_iter().zip(outcomes) {
            let outcome = outcome?;
            references.push(ArtifactPackReference {
                ordinal,
                canonical_path: path.into(),
                object_version: outcome.version.0,
                hash,
                length: bytes.len() as u64,
            });
        }
        ArtifactPackTable::new(references).map_err(index_status)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_exact_artifact_pack(
        &self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        reference: &ArtifactPackReference,
    ) -> Result<Bytes, Status> {
        reference.validate().map_err(index_status)?;
        let parsed =
            keldra_index::v1::parse_projection_artifact_path(reference.canonical_path.as_ref())
                .map_err(index_status)?;
        if !matches!(
            parsed.kind,
            keldra_index::v1::ProjectionArtifactKind::Pack
                | keldra_index::v1::ProjectionArtifactKind::QueryRunPack
        ) || parsed.content_hash != Some(reference.hash)
        {
            return Err(Status::data_loss(
                "v1 artifact pack path differs from its exact content identity",
            ));
        }
        let maximum_bytes = usize::try_from(reference.length)
            .map_err(|_| Status::data_loss("v1 artifact pack length is unbounded"))?;
        self.immutable_cache
            .get_or_load(
                tenant_id,
                bucket_id,
                &reference.canonical_path,
                reference.hash,
                Some(reference.object_version),
                maximum_bytes,
                || async {
                    let key =
                        ObjectKey::new(storage_tenant, bucket, reference.canonical_path.as_ref())
                            .map_err(|error| Status::internal(error.to_string()))?;
                    let version = self
                        .reader
                        .exact_version_descriptor_stable(
                            &key,
                            tenant_id,
                            bucket_id,
                            VersionId(reference.object_version),
                        )
                        .await?
                        .ok_or_else(|| Status::data_loss("v1 artifact pack is absent"))?;
                    if version.id.0 != reference.object_version || version.deleted {
                        return Err(Status::data_loss(
                            "v1 artifact pack object version differs from its root reference",
                        ));
                    }
                    let blob = version.blob.as_ref().ok_or_else(|| {
                        Status::data_loss("v1 artifact pack version has no payload")
                    })?;
                    if blob.hash != reference.hash || blob.length != reference.length {
                        return Err(Status::data_loss(
                            "v1 artifact pack object version differs from its root identity",
                        ));
                    }
                    let bytes = self.read_blob_local_first(blob, maximum_bytes).await?;
                    Ok(Some(Bytes::from(bytes)))
                },
            )
            .await?
            .ok_or_else(|| Status::data_loss("v1 artifact pack is absent"))
    }
}
