use anvil_store::{Store, VersionId};
use tonic::Status;

use crate::index_runtime::publication::{
    IndexArtifactDelete, IndexArtifactOutcome, IndexArtifactPublish, IndexArtifactRouter,
};

use super::{
    StoredAccountingDefinition, StoredAccountingRollup, StoredTrafficSource, current_path,
    definition_path, outbound_source_path,
};

#[derive(Clone)]
pub(crate) struct AccountingPublisher {
    store: Store,
    artifacts: IndexArtifactRouter,
}

impl AccountingPublisher {
    pub(crate) fn new(store: Store, artifacts: IndexArtifactRouter) -> Self {
        Self { store, artifacts }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_definition(
        &self,
        definition: &StoredAccountingDefinition,
        tenant_id: u64,
        bucket_id: u64,
        expected_version: Option<VersionId>,
        command_id: String,
    ) -> Result<IndexArtifactOutcome, Status> {
        self.publish_bytes(
            definition,
            tenant_id,
            bucket_id,
            definition_path(definition.accounting_id)?,
            definition.encode()?,
            expected_version,
            command_id,
        )
        .await
    }

    pub(crate) async fn publish_rollup(
        &self,
        definition: &StoredAccountingDefinition,
        tenant_id: u64,
        bucket_id: u64,
        rollup: &StoredAccountingRollup,
        expected_version: Option<VersionId>,
        command_id: String,
    ) -> Result<IndexArtifactOutcome, Status> {
        if rollup.accounting_id != definition.accounting_id {
            return Err(Status::invalid_argument(
                "accounting rollup and definition identities differ",
            ));
        }
        self.publish_bytes(
            definition,
            tenant_id,
            bucket_id,
            current_path(definition.accounting_id)?,
            rollup.encode()?,
            expected_version,
            command_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_outbound_source(
        &self,
        definition: &StoredAccountingDefinition,
        tenant_id: u64,
        bucket_id: u64,
        source: &StoredTrafficSource,
        expected_version: Option<VersionId>,
        command_id: String,
    ) -> Result<IndexArtifactOutcome, Status> {
        if source.accounting_id != definition.accounting_id {
            return Err(Status::invalid_argument(
                "accounting outbound source and definition identities differ",
            ));
        }
        self.publish_bytes(
            definition,
            tenant_id,
            bucket_id,
            outbound_source_path(definition.accounting_id, source.node_id)?,
            source.encode()?,
            expected_version,
            command_id,
        )
        .await
    }

    pub(crate) async fn delete_definition(
        &self,
        definition: &StoredAccountingDefinition,
        tenant_id: u64,
        bucket_id: u64,
        expected_version: VersionId,
        command_id: String,
    ) -> Result<IndexArtifactOutcome, Status> {
        self.artifacts
            .delete(IndexArtifactDelete {
                storage_tenant: definition.storage_tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.accounting_id,
                exact_path: definition_path(definition.accounting_id)?,
                expected_version,
                command_id,
            })
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_bytes(
        &self,
        definition: &StoredAccountingDefinition,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: String,
        bytes: Vec<u8>,
        expected_version: Option<VersionId>,
        command_id: String,
    ) -> Result<IndexArtifactOutcome, Status> {
        let blob = self
            .store
            .stage_blob(&bytes)
            .await
            .map_err(|error| Status::internal(format!("stage accounting artifact: {error}")))?;
        self.artifacts
            .publish(IndexArtifactPublish {
                storage_tenant: definition.storage_tenant.clone(),
                bucket: definition.bucket.clone(),
                tenant_id,
                bucket_id,
                index_id: definition.accounting_id,
                exact_path,
                blob,
                expected_version,
                command_id,
            })
            .await
    }
}
