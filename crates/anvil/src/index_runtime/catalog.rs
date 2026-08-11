//! Bounded process-local handoff for index definitions assigned to this node.
//!
//! Durable `ASSIGNED` records and ordinary definition objects remain the
//! authorities. This queue only coalesces work between the paged assignment
//! walker and the bounded builder scheduler. Losing it merely causes the next
//! assignment walk to offer the definition again.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tonic::Status;

use crate::index_service::{StoredIndexDefinition, definition_path};

const MAX_PENDING_CATALOG_CHANGES: usize = 1_024;

#[derive(Clone, Debug)]
pub(crate) struct CatalogDefinition {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) object_version: u64,
    pub(crate) stored: StoredIndexDefinition,
}

impl CatalogDefinition {
    pub(crate) fn identity(&self) -> CatalogIdentity {
        CatalogIdentity {
            tenant_id: self.tenant_id,
            bucket_id: self.bucket_id,
            index_id: self.stored.index_id,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Status> {
        if self.tenant_id == 0 || self.bucket_id == 0 || self.object_version == 0 {
            return Err(Status::data_loss(
                "assigned index definition has a zero stable identity",
            ));
        }
        if definition_path(&self.stored.name)?
            != format!("_anvil/indexes/v2/definitions/{}", self.stored.name)
        {
            return Err(Status::data_loss(
                "assigned index definition path is not canonical",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CatalogChange {
    Upsert(CatalogDefinition),
    Remove(CatalogIdentity),
}

impl CatalogChange {
    pub(crate) fn identity(&self) -> CatalogIdentity {
        match self {
            Self::Upsert(definition) => definition.identity(),
            Self::Remove(identity) => *identity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CatalogIdentity {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) index_id: u64,
}

#[derive(Clone)]
pub(crate) struct IndexCatalog {
    inner: Arc<Mutex<CatalogState>>,
    changes: tokio::sync::broadcast::Sender<CatalogIdentity>,
}

struct CatalogState {
    pending: BTreeMap<CatalogIdentity, CatalogChange>,
    capacity: usize,
}

impl Default for IndexCatalog {
    fn default() -> Self {
        Self::with_capacity(MAX_PENDING_CATALOG_CHANGES)
    }
}

impl IndexCatalog {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "index catalog capacity must be positive");
        let (changes, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(CatalogState {
                pending: BTreeMap::new(),
                capacity,
            })),
            changes,
        }
    }

    pub(crate) fn upsert(&self, definition: CatalogDefinition) -> Result<(), Status> {
        definition.validate()?;
        self.enqueue(CatalogChange::Upsert(definition))
    }

    pub(crate) fn remove(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<(), Status> {
        self.enqueue(CatalogChange::Remove(CatalogIdentity {
            tenant_id,
            bucket_id,
            index_id,
        }))
    }

    fn enqueue(&self, change: CatalogChange) -> Result<(), Status> {
        let identity = change.identity();
        let is_remove = matches!(change, CatalogChange::Remove(_));
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        if !state.pending.contains_key(&identity) && state.pending.len() >= state.capacity {
            if is_remove {
                // Removal is correctness-sensitive for a running local worker.
                // Discarding one pending upsert is safe because durable
                // assignment rediscovery will offer it again.
                let evicted = state.pending.iter().find_map(|(key, value)| {
                    matches!(value, CatalogChange::Upsert(_)).then_some(*key)
                });
                if let Some(evicted) = evicted {
                    state.pending.remove(&evicted);
                }
            }
            if state.pending.len() >= state.capacity {
                return Err(Status::resource_exhausted(
                    "assigned index handoff is at its bounded capacity",
                ));
            }
        }
        state.pending.insert(identity, change);
        drop(state);
        let _ = self.changes.send(identity);
        Ok(())
    }

    pub(crate) fn take(
        &self,
        identity: CatalogIdentity,
        admit_upsert: bool,
    ) -> Result<Option<CatalogChange>, Status> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        if state
            .pending
            .get(&identity)
            .is_some_and(|change| matches!(change, CatalogChange::Upsert(_)) && !admit_upsert)
        {
            return Ok(None);
        }
        Ok(state.pending.remove(&identity))
    }

    pub(crate) fn take_page(
        &self,
        limit: usize,
        mut admit_upsert: impl FnMut(CatalogIdentity) -> bool,
    ) -> Result<Vec<CatalogChange>, Status> {
        if limit == 0 {
            return Err(Status::invalid_argument(
                "assigned index handoff page must be positive",
            ));
        }
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?;
        let identities = state
            .pending
            .iter()
            .filter_map(|(identity, change)| match change {
                CatalogChange::Remove(_) => Some(*identity),
                CatalogChange::Upsert(_) if admit_upsert(*identity) => Some(*identity),
                CatalogChange::Upsert(_) => None,
            })
            .take(limit)
            .collect::<Vec<_>>();
        Ok(identities
            .into_iter()
            .filter_map(|identity| state.pending.remove(&identity))
            .collect())
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<CatalogIdentity> {
        self.changes.subscribe()
    }

    #[cfg(test)]
    fn pending_len(&self) -> Result<usize, Status> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| Status::internal("assigned index handoff lock is poisoned"))?
            .pending
            .len())
    }
}

#[cfg(test)]
mod tests {
    use anvil_api::v1::{CreateIndexRequest, IndexSpecification, PathIndexSpec};

    use super::*;

    fn definition(tenant_id: u64, bucket_id: u64, index_id: u64) -> CatalogDefinition {
        CatalogDefinition {
            tenant_id,
            bucket_id,
            object_version: 1,
            stored: StoredIndexDefinition::create(
                "tenant".into(),
                CreateIndexRequest {
                    bucket: "bucket".into(),
                    name: format!("index-{index_id}"),
                    path_prefix: String::new(),
                    content_type: String::new(),
                    specification: Some(IndexSpecification {
                        specification: Some(
                            anvil_api::v1::index_specification::Specification::Path(
                                PathIndexSpec {},
                            ),
                        ),
                    }),
                    command_id: format!("create-{index_id}"),
                },
                index_id,
            )
            .unwrap(),
        }
    }

    #[test]
    fn changes_are_coalesced_and_consumed() {
        let catalog = IndexCatalog::with_capacity(2);
        let first = definition(1, 2, 9);
        let mut replacement = first.clone();
        replacement.object_version = 2;
        catalog.upsert(first).unwrap();
        catalog.upsert(replacement.clone()).unwrap();
        assert_eq!(catalog.pending_len().unwrap(), 1);
        let change = catalog.take(replacement.identity(), true).unwrap().unwrap();
        assert!(matches!(change, CatalogChange::Upsert(value) if value.object_version == 2));
        assert_eq!(catalog.pending_len().unwrap(), 0);
    }

    #[test]
    fn bounded_queue_rejects_extra_upserts_but_prioritizes_removal() {
        let catalog = IndexCatalog::with_capacity(1);
        let first = definition(1, 2, 9);
        let second = definition(3, 4, 10);
        catalog.upsert(first.clone()).unwrap();
        assert_eq!(
            catalog.upsert(second).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        catalog
            .remove(first.tenant_id, first.bucket_id, first.stored.index_id)
            .unwrap();
        assert!(matches!(
            catalog.take(first.identity(), true).unwrap(),
            Some(CatalogChange::Remove(_))
        ));
    }

    #[test]
    fn upserts_remain_pending_while_builder_leases_are_full() {
        let catalog = IndexCatalog::with_capacity(1);
        catalog.upsert(definition(1, 2, 9)).unwrap();
        assert!(catalog.take_page(1, |_| false).unwrap().is_empty());
        assert_eq!(catalog.pending_len().unwrap(), 1);
        assert_eq!(catalog.take_page(1, |_| true).unwrap().len(), 1);
    }
}
