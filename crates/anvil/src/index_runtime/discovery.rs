//! Cold definition discovery followed by ordered journal refresh.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anvil_store::{LocalChange, ObjectKey};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::cluster_peer::IndexHeadScanScope;
use crate::index_service::{StoredIndexDefinition, definition_path};

use super::catalog::{CatalogDefinition, IndexCatalog};
use super::events::{IndexBarrier, IndexEventError, IndexEventJournal, MAX_INDEX_EVENT_PAGE_BYTES};
use super::publication::index_definition_name;
use super::scanner::ClusterIndexScanner;

const DEFINITION_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFINITION_RETRY_INTERVAL: Duration = Duration::from_secs(1);

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

    /// One complete inventory scan. This is used only at cold start and after
    /// a retained-journal gap; the steady state is driven by source events.
    pub(crate) async fn refresh(&self) -> Result<(), Status> {
        let candidates = self.scanner.scan(IndexHeadScanScope::Definitions).await?;
        let mut definitions = Vec::new();
        for candidate in candidates {
            if let Some(definition) = self.load_candidate(candidate).await? {
                definitions.push(definition);
            }
        }
        self.catalog.replace(definitions)
    }

    async fn load_candidate(
        &self,
        candidate: crate::cluster_peer::IndexCurrentHead,
    ) -> Result<Option<CatalogDefinition>, Status> {
        if candidate.head.deleted || candidate.version.deleted {
            return Ok(None);
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
        self.load_exact(
            candidate.tenant_id,
            candidate.bucket_id,
            &candidate.exact_path,
            &candidate_definition.name,
        )
        .await
    }

    async fn load_exact(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        exact_path: &str,
        expected_name: &str,
    ) -> Result<Option<CatalogDefinition>, Status> {
        // Stable-ID reads use the mutable names only to construct a validated
        // ObjectKey; placement and metadata identity come from the supplied IDs.
        let key = ObjectKey::new("system", "indexes", exact_path)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let Some(opened) = self
            .reader
            .open_stable(&key, tenant_id, bucket_id, None)
            .await?
        else {
            return Ok(None);
        };
        if opened.version.deleted {
            return Ok(None);
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
        if stored.name != expected_name || exact_path != definition_path(&stored.name)? {
            return Err(Status::data_loss(
                "authoritative index definition changed immutable identity",
            ));
        }
        Ok(Some(CatalogDefinition {
            tenant_id,
            bucket_id,
            object_version: opened.version.id.0,
            stored,
            encoded,
        }))
    }

    async fn apply_page(&self, page: &super::events::IndexJournalPage) -> Result<(), Status> {
        for event in &page.changes {
            let LocalChange::ObjectHead(change) = &event.change else {
                continue;
            };
            let Some(name) = index_definition_name(&change.exact_path) else {
                continue;
            };
            match self
                .load_exact(change.tenant_id, change.bucket_id, &change.exact_path, name)
                .await?
            {
                Some(definition) => self.catalog.upsert(definition)?,
                None => self
                    .catalog
                    .remove(change.tenant_id, change.bucket_id, name)?,
            }
        }
        Ok(())
    }

    pub(crate) fn spawn(
        self,
        journal: Arc<IndexEventJournal>,
        initial: IndexBarrier,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut cursor = initial;
            loop {
                match self.catch_up_once(&journal, &mut cursor).await {
                    Ok(()) => tokio::time::sleep(DEFINITION_EVENT_POLL_INTERVAL).await,
                    Err(error) => {
                        tracing::warn!(%error, "index definition journal refresh failed; rebuilding its disposable catalog");
                        match capture_then_refresh(&self, &journal).await {
                            Ok(rebased) => cursor = rebased,
                            Err(rebuild_error) => tracing::warn!(
                                %rebuild_error,
                                "index definition catalog rebuild failed"
                            ),
                        }
                        tokio::time::sleep(DEFINITION_RETRY_INTERVAL).await;
                    }
                }
            }
        })
    }

    async fn catch_up_once(
        &self,
        journal: &IndexEventJournal,
        cursor: &mut IndexBarrier,
    ) -> Result<(), Status> {
        let target = journal.capture_barrier().await.map_err(event_status)?;
        while let Some(page) = journal
            .next_page(cursor, &target, MAX_INDEX_EVENT_PAGE_BYTES)
            .await
            .map_err(event_status)?
        {
            self.apply_page(&page).await?;
            *cursor = page.through;
        }
        Ok(())
    }
}

pub(crate) async fn capture_then_refresh(
    discovery: &IndexDefinitionDiscovery,
    journal: &IndexEventJournal,
) -> Result<IndexBarrier, Status> {
    let before = journal.capture_barrier().await.map_err(event_status)?;
    discovery.refresh().await?;
    Ok(before)
}

fn event_status(error: IndexEventError) -> Status {
    Status::unavailable(error.to_string())
}
