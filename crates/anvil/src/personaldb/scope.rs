use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use personaldb_core::DatabaseId;
use personaldb_server_core::ObjectStoreError;

use crate::authentication::Caller;
use crate::distributed_list::OriginalBearer;

/// Stable identity of one ordinary Anvil storage scope. Mutable tenant and
/// bucket names are deliberately excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PersonalDbStorageId {
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
}

impl PersonalDbStorageId {
    pub(crate) const fn new(tenant_id: u64, bucket_id: u64) -> Self {
        Self {
            tenant_id,
            bucket_id,
        }
    }

    pub(crate) fn group(self, database_id: DatabaseId) -> PersonalDbGroupScope {
        PersonalDbGroupScope {
            storage: self,
            database_id,
        }
    }
}

/// Complete stable identity used for PersonalDB placement and process-local
/// group state. The canonical PersonalDB database ID is not rewritten.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PersonalDbGroupScope {
    pub(crate) storage: PersonalDbStorageId,
    pub(crate) database_id: DatabaseId,
}

impl PersonalDbGroupScope {
    pub(crate) fn placement_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(17 + self.database_id.0.len());
        key.push(0); // scoped PersonalDB placement-key format v0
        key.extend_from_slice(&self.storage.tenant_id.to_be_bytes());
        key.extend_from_slice(&self.storage.bucket_id.to_be_bytes());
        key.extend_from_slice(self.database_id.0.as_bytes());
        key
    }
}

#[derive(Clone)]
pub(crate) struct PersonalDbStorageScope {
    pub(crate) tenant: String,
    pub(crate) bucket: String,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) caller: Caller,
    pub(crate) bearer: OriginalBearer,
}

impl PersonalDbStorageScope {
    pub(crate) const fn storage_id(&self) -> PersonalDbStorageId {
        PersonalDbStorageId::new(self.tenant_id, self.bucket_id)
    }

    pub(crate) fn group(&self, database_id: DatabaseId) -> PersonalDbGroupScope {
        self.storage_id().group(database_id)
    }

    fn same_storage(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.bucket == other.bucket
            && self.tenant_id == other.tenant_id
            && self.bucket_id == other.bucket_id
            && self.caller == other.caller
            && self.bearer.signed_token() == other.bearer.signed_token()
    }
}

#[derive(Clone, Default)]
pub(crate) struct PersonalDbScopes {
    inner: Arc<Mutex<HashMap<String, ScopeEntry>>>,
}

struct ScopeEntry {
    scope: PersonalDbStorageScope,
    users: usize,
}

pub(crate) struct PersonalDbScopeLease {
    database_id: String,
    scopes: PersonalDbScopes,
}

impl PersonalDbScopes {
    pub(crate) fn enter(
        &self,
        database_id: &DatabaseId,
        scope: PersonalDbStorageScope,
    ) -> Result<PersonalDbScopeLease, ObjectStoreError> {
        let mut entries = self
            .inner
            .lock()
            .map_err(|_| unavailable("scope lock poisoned"))?;
        match entries.get_mut(&database_id.0) {
            Some(entry) if entry.scope.same_storage(&scope) => {
                entry.users = entry.users.saturating_add(1);
            }
            Some(_) => {
                return Err(unavailable(
                    "the same PersonalDB group is concurrently bound to another bucket or caller",
                ));
            }
            None => {
                entries.insert(database_id.0.clone(), ScopeEntry { scope, users: 1 });
            }
        }
        Ok(PersonalDbScopeLease {
            database_id: database_id.0.clone(),
            scopes: self.clone(),
        })
    }

    pub(crate) fn for_key(
        &self,
        key: &str,
    ) -> Result<(String, PersonalDbStorageScope), ObjectStoreError> {
        let entries = self
            .inner
            .lock()
            .map_err(|_| unavailable("scope lock poisoned"))?;
        entries
            .iter()
            .filter(|(database_id, _)| key_belongs_to(key, database_id))
            .max_by_key(|(database_id, _)| database_id.len())
            .map(|(database_id, entry)| (database_id.clone(), entry.scope.clone()))
            .ok_or_else(|| unavailable("PersonalDB storage access has no active request scope"))
    }

    pub(crate) fn for_database(
        &self,
        database_id: &DatabaseId,
    ) -> Result<PersonalDbStorageScope, ObjectStoreError> {
        self.inner
            .lock()
            .map_err(|_| unavailable("scope lock poisoned"))?
            .get(&database_id.0)
            .map(|entry| entry.scope.clone())
            .ok_or_else(|| unavailable("PersonalDB request has no active storage scope"))
    }
}

impl Drop for PersonalDbScopeLease {
    fn drop(&mut self) {
        let Ok(mut entries) = self.scopes.inner.lock() else {
            return;
        };
        let remove = entries.get_mut(&self.database_id).is_some_and(|entry| {
            entry.users = entry.users.saturating_sub(1);
            entry.users == 0
        });
        if remove {
            entries.remove(&self.database_id);
        }
    }
}

fn key_belongs_to(key: &str, database_id: &str) -> bool {
    let prefix = format!("groups/{database_id}");
    key == prefix
        || key
            .strip_prefix(&prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

fn unavailable(message: impl Into<String>) -> ObjectStoreError {
    ObjectStoreError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_store::StorageTenantId;

    fn scope() -> PersonalDbStorageScope {
        PersonalDbStorageScope {
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            tenant_id: 1,
            bucket_id: 2,
            caller: Caller::from_authenticated_application(
                StorageTenantId::parse("tenant").unwrap(),
                "app",
            )
            .unwrap(),
            bearer: OriginalBearer::from_signed_token("token"),
        }
    }

    #[test]
    fn lease_exposes_only_the_matching_group() {
        let scopes = PersonalDbScopes::default();
        let lease = scopes.enter(&DatabaseId::new("db"), scope()).unwrap();
        assert!(scopes.for_key("groups/db/manifest.json").is_ok());
        assert!(scopes.for_key("groups/db2/manifest.json").is_err());
        drop(lease);
        assert!(scopes.for_key("groups/db/manifest.json").is_err());
    }

    #[test]
    fn identical_database_ids_in_distinct_storage_scopes_do_not_share_state() {
        let first_scopes = PersonalDbScopes::default();
        let second_scopes = PersonalDbScopes::default();
        let database_id = DatabaseId::new("same-external-id");
        let first_scope = scope();
        let mut second_scope = scope();
        second_scope.tenant = "other-tenant".into();
        second_scope.bucket = "other-bucket".into();
        second_scope.tenant_id = 10;
        second_scope.bucket_id = 20;
        second_scope.caller = Caller::from_authenticated_application(
            StorageTenantId::parse("other-tenant").unwrap(),
            "app",
        )
        .unwrap();

        let _first = first_scopes
            .enter(&database_id, first_scope.clone())
            .unwrap();
        let _second = second_scopes
            .enter(&database_id, second_scope.clone())
            .unwrap();

        assert_eq!(
            first_scopes
                .for_database(&database_id)
                .unwrap()
                .storage_id(),
            first_scope.storage_id()
        );
        assert_eq!(
            second_scopes
                .for_database(&database_id)
                .unwrap()
                .storage_id(),
            second_scope.storage_id()
        );
        assert_ne!(first_scope.storage_id(), second_scope.storage_id());
        assert_ne!(
            first_scope.group(database_id.clone()).placement_key(),
            second_scope.group(database_id).placement_key()
        );
    }
}
