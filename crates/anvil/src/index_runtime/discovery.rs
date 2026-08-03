//! Cold discovery of ordinary index-definition objects.

use std::io::Read;
use std::time::Duration;

use anvil_store::ObjectKey;
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::catalog::{CatalogDefinition, IndexCatalog};
use super::scanner::ClusterIndexScanner;

const DEFINITION_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct IndexDefinitionDiscovery {
    scanner: ClusterIndexScanner,
    reader: ClusterObjectReader,
    catalog: IndexCatalog,
}

impl IndexDefinitionDiscovery {
    pub(crate) fn new(
        scanner: ClusterIndexScanner,
        reader: ClusterObjectReader,
        catalog: IndexCatalog,
    ) -> Self {
        Self {
            scanner,
            reader,
            catalog,
        }
    }

    pub(crate) async fn refresh(&self) -> Result<(), Status> {
        let candidates = self.scanner.scan(IndexHeadScanScope::Definitions).await?;
        let mut definitions = Vec::new();
        for candidate in candidates {
            if candidate.head.deleted || candidate.version.deleted {
                continue;
            }
            let Some(candidate_blob) = candidate.version.blob.as_ref() else {
                return Err(Status::data_loss(
                    "live index definition has no payload reference",
                ));
            };
            let candidate_bytes = self.reader.read_blob_bytes(candidate_blob).await?;
            let candidate_definition = StoredIndexDefinition::decode(&candidate_bytes)?;
            if candidate.exact_path != definition_path(&candidate_definition.name)? {
                return Err(Status::data_loss(
                    "index definition payload and object path disagree",
                ));
            }

            // The scan discovers a path. The normal quorum read, not an
            // individual metadata replica, selects its authoritative value.
            let key = ObjectKey::new(
                &candidate_definition.tenant,
                &candidate_definition.bucket,
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
            let Some(mut payload) = opened.payload else {
                return Err(Status::data_loss(
                    "live index definition quorum read returned no payload",
                ));
            };
            let mut encoded = Vec::new();
            payload
                .read_to_end(&mut encoded)
                .map_err(|error| Status::internal(format!("read index definition: {error}")))?;
            let stored = StoredIndexDefinition::decode(&encoded)?;
            if stored.tenant != candidate_definition.tenant
                || stored.bucket != candidate_definition.bucket
                || stored.name != candidate_definition.name
                || candidate.exact_path != definition_path(&stored.name)?
            {
                return Err(Status::data_loss(
                    "authoritative index definition changed immutable identity",
                ));
            }
            definitions.push(CatalogDefinition {
                tenant_id: candidate.tenant_id,
                bucket_id: candidate.bucket_id,
                object_version: opened.version.id.0,
                stored,
                encoded,
            });
        }
        self.catalog.replace(definitions)
    }

    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DEFINITION_REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = self.refresh().await {
                    tracing::warn!(%error, "index definition discovery refresh failed");
                }
            }
        })
    }
}
