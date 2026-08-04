use std::io::Read;
use std::time::Duration;

use anvil_store::ObjectKey;
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_runtime::scanner::ClusterIndexScanner;

use super::{
    LoadedAccountingDefinition, StoredAccountingDefinition, catalog::AccountingCatalog,
    definition_path,
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct AccountingDiscovery {
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    catalog: AccountingCatalog,
}

impl AccountingDiscovery {
    pub(crate) fn new(
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        catalog: AccountingCatalog,
    ) -> Self {
        Self {
            scanner,
            reader,
            catalog,
        }
    }

    pub(crate) async fn refresh(&self) -> Result<(), Status> {
        let candidates = self
            .scanner
            .scan(IndexHeadScanScope::AccountingDefinitions)
            .await?;
        let mut definitions = Vec::new();
        for candidate in candidates {
            if candidate.head.deleted || candidate.version.deleted {
                continue;
            }
            let blob = candidate.version.blob.as_ref().ok_or_else(|| {
                Status::data_loss("live accounting definition has no payload reference")
            })?;
            let hinted =
                StoredAccountingDefinition::decode(&self.reader.read_blob_bytes(blob).await?)?;
            if candidate.exact_path != definition_path(hinted.accounting_id)? {
                return Err(Status::data_loss(
                    "accounting definition payload and object path disagree",
                ));
            }
            let key = ObjectKey::new(
                &hinted.storage_tenant,
                &hinted.bucket,
                &candidate.exact_path,
            )
            .map_err(|error| Status::data_loss(error.to_string()))?;
            let Some(opened) = self
                .reader
                .open_stable(&key, candidate.tenant_id, candidate.bucket_id, None)
                .await?
            else {
                continue;
            };
            if opened.version.deleted {
                continue;
            }
            let mut payload = opened.payload.ok_or_else(|| {
                Status::data_loss("accounting definition quorum read returned no payload")
            })?;
            let mut encoded = Vec::new();
            payload.read_to_end(&mut encoded).map_err(|error| {
                Status::internal(format!("read accounting definition: {error}"))
            })?;
            let stored = StoredAccountingDefinition::decode(&encoded)?;
            if stored.accounting_id != hinted.accounting_id
                || stored.storage_tenant != hinted.storage_tenant
                || stored.bucket != hinted.bucket
                || stored.path_prefix != hinted.path_prefix
            {
                return Err(Status::data_loss(
                    "authoritative accounting definition changed immutable identity",
                ));
            }
            definitions.push(LoadedAccountingDefinition {
                tenant_id: candidate.tenant_id,
                bucket_id: candidate.bucket_id,
                version: opened.version.id,
                stored,
            });
        }
        self.catalog.replace(definitions)
    }

    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = self.refresh().await {
                    tracing::warn!(%error, "accounting definition discovery refresh failed");
                }
            }
        })
    }
}
