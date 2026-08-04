use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use tonic::Status;

use super::{LoadedAccountingDefinition, StoredAccountingDefinition};

#[derive(Clone, Default)]
pub(crate) struct AccountingCatalog {
    inner: Arc<RwLock<CatalogState>>,
}

#[derive(Default)]
struct CatalogState {
    definitions: BTreeMap<u64, LoadedAccountingDefinition>,
    by_named_bucket: BTreeMap<(String, String), Vec<u64>>,
}

impl AccountingCatalog {
    pub(crate) fn replace(
        &self,
        definitions: Vec<LoadedAccountingDefinition>,
    ) -> Result<(), Status> {
        let mut replacement = BTreeMap::new();
        for definition in definitions {
            validate(&definition)?;
            if replacement
                .insert(definition.stored.accounting_id, definition)
                .is_some()
            {
                return Err(Status::data_loss(
                    "accounting discovery returned a duplicate stable identity",
                ));
            }
        }
        let mut by_named_bucket = BTreeMap::<(String, String), Vec<u64>>::new();
        for definition in replacement.values() {
            by_named_bucket
                .entry((
                    definition.stored.storage_tenant.clone(),
                    definition.stored.bucket.clone(),
                ))
                .or_default()
                .push(definition.stored.accounting_id);
        }
        *self
            .inner
            .write()
            .map_err(|_| Status::internal("accounting catalog lock is poisoned"))? = CatalogState {
            definitions: replacement,
            by_named_bucket,
        };
        Ok(())
    }

    pub(crate) fn all(&self) -> Result<Vec<LoadedAccountingDefinition>, Status> {
        Ok(self
            .inner
            .read()
            .map_err(|_| Status::internal("accounting catalog lock is poisoned"))?
            .definitions
            .values()
            .cloned()
            .collect())
    }

    pub(crate) fn get(
        &self,
        accounting_id: u64,
    ) -> Result<Option<LoadedAccountingDefinition>, Status> {
        Ok(self
            .inner
            .read()
            .map_err(|_| Status::internal("accounting catalog lock is poisoned"))?
            .definitions
            .get(&accounting_id)
            .cloned())
    }

    pub(crate) fn matching_names(
        &self,
        storage_tenant: &str,
        bucket: &str,
        path: &str,
    ) -> Result<Vec<LoadedAccountingDefinition>, Status> {
        let state = self
            .inner
            .read()
            .map_err(|_| Status::internal("accounting catalog lock is poisoned"))?;
        Ok(state
            .by_named_bucket
            .get(&(storage_tenant.to_owned(), bucket.to_owned()))
            .into_iter()
            .flatten()
            .filter_map(|id| state.definitions.get(id))
            .filter(|definition| super::includes_path(&definition.stored.path_prefix, path))
            .cloned()
            .collect())
    }
}

fn validate(definition: &LoadedAccountingDefinition) -> Result<(), Status> {
    if definition.tenant_id == 0 || definition.bucket_id == 0 || definition.version.0 == 0 {
        return Err(Status::data_loss(
            "accounting catalog definition has a zero stable identity or version",
        ));
    }
    let round_trip = StoredAccountingDefinition::decode(&definition.stored.encode()?)?;
    if round_trip != definition.stored {
        return Err(Status::data_loss(
            "accounting catalog definition is not canonical",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use anvil_store::VersionId;

    use super::*;

    fn definition(id_prefix: &str) -> LoadedAccountingDefinition {
        LoadedAccountingDefinition {
            tenant_id: 7,
            bucket_id: 9,
            version: VersionId(1),
            stored: StoredAccountingDefinition::create(
                "tenant".into(),
                "bucket".into(),
                id_prefix.into(),
                7,
                9,
            )
            .unwrap(),
        }
    }

    #[test]
    fn matching_uses_stable_bucket_ids_and_segment_prefixes() {
        let catalog = AccountingCatalog::default();
        catalog
            .replace(vec![definition("users/7"), definition("")])
            .unwrap();

        assert_eq!(
            catalog
                .matching_names("tenant", "bucket", "users/7/a")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            catalog
                .matching_names("tenant", "bucket", "users/70/a")
                .unwrap()
                .len(),
            1
        );
    }
}
