//! Bounded process-local handoff for assigned accounting definitions.
//!
//! Durable `ASSIGNED` records and ordinary definition objects remain
//! authoritative. This queue coalesces assignment work; it is not a registry
//! and is safe to discard on restart.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tonic::Status;

use super::{LoadedAccountingDefinition, StoredAccountingDefinition};

const MAX_PENDING_ACCOUNTING_CHANGES: usize = 1_024;

pub(crate) type AccountingIdentity = (u64, u64, u64);

#[derive(Clone)]
pub(crate) enum AccountingCatalogChange {
    Upsert(LoadedAccountingDefinition),
    Remove(AccountingIdentity),
}

impl AccountingCatalogChange {
    pub(crate) fn identity(&self) -> AccountingIdentity {
        match self {
            Self::Upsert(definition) => identity(definition),
            Self::Remove(identity) => *identity,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AccountingCatalog {
    inner: Arc<Mutex<CatalogState>>,
    changes: tokio::sync::broadcast::Sender<AccountingIdentity>,
}

struct CatalogState {
    pending: BTreeMap<AccountingIdentity, AccountingCatalogChange>,
    capacity: usize,
}

impl Default for AccountingCatalog {
    fn default() -> Self {
        Self::with_capacity(MAX_PENDING_ACCOUNTING_CHANGES)
    }
}

impl AccountingCatalog {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "accounting handoff capacity must be positive");
        let (changes, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            inner: Arc::new(Mutex::new(CatalogState {
                pending: BTreeMap::new(),
                capacity,
            })),
            changes,
        }
    }

    pub(crate) fn upsert(&self, definition: LoadedAccountingDefinition) -> Result<(), Status> {
        validate(&definition)?;
        self.enqueue(AccountingCatalogChange::Upsert(definition))
    }

    pub(crate) fn remove(&self, identity: AccountingIdentity) -> Result<(), Status> {
        self.enqueue(AccountingCatalogChange::Remove(identity))
    }

    fn enqueue(&self, change: AccountingCatalogChange) -> Result<(), Status> {
        let identity = change.identity();
        let is_remove = matches!(change, AccountingCatalogChange::Remove(_));
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("accounting handoff lock is poisoned"))?;
        if !state.pending.contains_key(&identity) && state.pending.len() >= state.capacity {
            if is_remove {
                let evicted = state.pending.iter().find_map(|(key, value)| {
                    matches!(value, AccountingCatalogChange::Upsert(_)).then_some(*key)
                });
                if let Some(evicted) = evicted {
                    state.pending.remove(&evicted);
                }
            }
            if state.pending.len() >= state.capacity {
                return Err(Status::resource_exhausted(
                    "accounting assignment handoff is at its bounded capacity",
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
        identity: AccountingIdentity,
        admit_upsert: bool,
    ) -> Result<Option<AccountingCatalogChange>, Status> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("accounting handoff lock is poisoned"))?;
        if state.pending.get(&identity).is_some_and(|change| {
            matches!(change, AccountingCatalogChange::Upsert(_)) && !admit_upsert
        }) {
            return Ok(None);
        }
        Ok(state.pending.remove(&identity))
    }

    pub(crate) fn take_page(
        &self,
        limit: usize,
        mut admit_upsert: impl FnMut(AccountingIdentity) -> bool,
    ) -> Result<Vec<AccountingCatalogChange>, Status> {
        if limit == 0 {
            return Err(Status::invalid_argument(
                "accounting handoff page must be positive",
            ));
        }
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Status::internal("accounting handoff lock is poisoned"))?;
        let identities = state
            .pending
            .iter()
            .filter_map(|(identity, change)| match change {
                AccountingCatalogChange::Remove(_) => Some(*identity),
                AccountingCatalogChange::Upsert(_) if admit_upsert(*identity) => Some(*identity),
                AccountingCatalogChange::Upsert(_) => None,
            })
            .take(limit)
            .collect::<Vec<_>>();
        Ok(identities
            .into_iter()
            .filter_map(|identity| state.pending.remove(&identity))
            .collect())
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<AccountingIdentity> {
        self.changes.subscribe()
    }

    #[cfg(test)]
    fn pending_len(&self) -> Result<usize, Status> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| Status::internal("accounting handoff lock is poisoned"))?
            .pending
            .len())
    }
}

fn identity(definition: &LoadedAccountingDefinition) -> AccountingIdentity {
    (
        definition.tenant_id,
        definition.bucket_id,
        definition.stored.accounting_id,
    )
}

fn validate(definition: &LoadedAccountingDefinition) -> Result<(), Status> {
    if definition.tenant_id == 0 || definition.bucket_id == 0 || definition.version.0 == 0 {
        return Err(Status::data_loss(
            "accounting handoff definition has a zero stable identity or version",
        ));
    }
    let round_trip = StoredAccountingDefinition::decode(&definition.stored.encode()?)?;
    if round_trip != definition.stored {
        return Err(Status::data_loss(
            "accounting handoff definition is not canonical",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use keldra_store::VersionId;

    use super::*;

    fn definition(tenant_id: u64, bucket_id: u64, prefix: &str) -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
            tenant_id,
            bucket_id,
            version: VersionId(1),
            stored: StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                prefix.into(),
                tenant_id,
                bucket_id,
            )
            .unwrap(),
        }
    }

    #[test]
    fn changes_are_coalesced_and_consumed() {
        let catalog = AccountingCatalog::with_capacity(2);
        let value = definition(7, 9, "users/7");
        let identity = identity(&value);
        catalog.upsert(value.clone()).unwrap();
        catalog.upsert(value).unwrap();
        assert_eq!(catalog.pending_len().unwrap(), 1);
        assert!(matches!(
            catalog.take(identity, true).unwrap(),
            Some(AccountingCatalogChange::Upsert(_))
        ));
    }

    #[test]
    fn bounded_queue_prioritizes_removal_over_rediscoverable_upsert() {
        let catalog = AccountingCatalog::with_capacity(1);
        let value = definition(7, 9, "users/7");
        let identity = identity(&value);
        catalog.upsert(value).unwrap();
        catalog.remove(identity).unwrap();
        assert!(matches!(
            catalog.take(identity, true).unwrap(),
            Some(AccountingCatalogChange::Remove(_))
        ));
    }

    #[test]
    fn upserts_remain_pending_while_worker_leases_are_full() {
        let catalog = AccountingCatalog::with_capacity(1);
        catalog.upsert(definition(7, 9, "users/7")).unwrap();
        assert!(catalog.take_page(1, |_| false).unwrap().is_empty());
        assert_eq!(catalog.pending_len().unwrap(), 1);
        assert_eq!(catalog.take_page(1, |_| true).unwrap().len(), 1);
    }
}
