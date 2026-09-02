//! Bounded format-v6 immutable publication through ordinary object storage.

use super::*;

impl IndexArtifactCoordinator {
    pub(super) async fn publish_v6_immutable_many(
        &self,
        authenticated_publisher: NodeId,
        placement: ClusterPlacement,
        requests: Vec<IndexArtifactPublish>,
    ) -> Result<Vec<IndexArtifactPublicationOutcome>, Status> {
        validate_immutable_batch(&requests)?;
        let first = &requests[0];
        let admission = first.admission;
        let governance = self
            .governance
            .resolve(&first.storage_tenant, &first.bucket)
            .await?;
        if (governance.tenant_id, governance.bucket_id) != (first.tenant_id, first.bucket_id) {
            return Err(Status::failed_precondition(
                "projection artifact names no longer bind the supplied stable IDs",
            ));
        }
        let first_key = first.key()?;
        let group = self.objects.object_replica_group_stable(
            &placement,
            &first_key,
            first.tenant_id,
            first.bucket_id,
        )?;
        if group.coordinator() != self.objects.local_node() {
            return Err(Status::failed_precondition(
                "grouped projection artifacts reached the wrong object coordinator",
            ));
        }
        for request in &requests {
            let key = request.key()?;
            self.validate_active_publisher(
                authenticated_publisher,
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            let candidate = self.objects.object_replica_group_stable(
                &placement,
                &key,
                request.tenant_id,
                request.bucket_id,
            )?;
            if candidate != group {
                return Err(Status::invalid_argument(
                    "grouped projection artifacts span metadata replica groups",
                ));
            }
        }
        let durability = artifact_durability(
            ArtifactPathKind::ProjectionImmutable,
            placement.placement_nodes().len(),
        );
        let publishes = requests
            .into_iter()
            .map(|request| {
                Ok(PublishRequest {
                    key: request.key()?,
                    blob: request.blob,
                    content_type: Some(INDEX_ARTIFACT_CONTENT_TYPE.into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some(request.command_id),
                    durability,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let results = if admission.is_publication_progress() {
            self.objects
                .publish_many_derived_progress_from_source_with_governance(
                    publishes,
                    authenticated_publisher,
                    governance,
                    placement,
                )
                .await?
        } else {
            self.objects
                .publish_many_from_source_with_governance(
                    publishes,
                    authenticated_publisher,
                    governance,
                    placement,
                )
                .await?
        };
        Ok(results
            .into_iter()
            .map(|outcome| {
                outcome.map(|receipt| IndexArtifactOutcome {
                    version: receipt.version,
                    replayed: receipt.replayed,
                })
            })
            .collect())
    }
}
