use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anvil_consensus::NodeId;
use personaldb_core::{
    Ed25519ProtocolSigner, KeyGeneration, KeyTrustPolicy, ProtocolSigner, PublicKeyTrustStore,
    SignaturePurpose,
};
use personaldb_server::{PersonalDbServer, ServerConfig, ServerError, TransportKind};

use crate::authentication::JwtManager;
use crate::authoritative_system::AuthoritativeSystemAuthorization;
use crate::distributed_list::DistributedObjectLister;
use crate::serving_fence::ServingAuthority;
use crate::v05::ObjectServiceImpl;

use super::authorization::PersonalDbAuthorizer;
use super::object_store::AnvilPersonalDbObjectStore;
use super::placement::HrwPrimaryResolver;
use super::runtime::PersonalDbRuntime;
use super::scope::{PersonalDbScopes, PersonalDbStorageId};

pub(crate) struct PersonalDbInstance {
    pub(crate) runtime: PersonalDbRuntime,
    pub(crate) scopes: PersonalDbScopes,
    pub(crate) authorizer: PersonalDbAuthorizer,
}

/// Disposable process-local cache. Each stable tenant/bucket scope gets one
/// upstream server whose native maps can hold all of that bucket's database
/// groups. Nothing in this cache is authoritative or written to Raft.
#[derive(Clone)]
pub(crate) struct PersonalDbInstances {
    local_node: NodeId,
    resolver: HrwPrimaryResolver,
    serving: ServingAuthority,
    objects: ObjectServiceImpl,
    lister: DistributedObjectLister,
    authorization: AuthoritativeSystemAuthorization,
    witness_signer: Arc<dyn ProtocolSigner>,
    witness_trust: Arc<PublicKeyTrustStore>,
    cache: StorageScopeCache<PersonalDbInstance>,
}

impl PersonalDbInstances {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        local_node: NodeId,
        resolver: HrwPrimaryResolver,
        serving: ServingAuthority,
        objects: ObjectServiceImpl,
        lister: DistributedObjectLister,
        authorization: AuthoritativeSystemAuthorization,
        tokens: &JwtManager,
    ) -> anyhow::Result<Self> {
        let witness_signer = Arc::new(
            Ed25519ProtocolSigner::from_pkcs8_der(
                &tokens.personaldb_witness_pkcs8_der()?,
                KeyTrustPolicy::new(
                    KeyGeneration::new(1).map_err(|error| anyhow::anyhow!(error.to_string()))?,
                    SignaturePurpose::Witness,
                    0,
                ),
            )
            .map_err(|error| anyhow::anyhow!(error.to_string()))?,
        );
        let witness_trust = Arc::new(
            PublicKeyTrustStore::from_records([witness_signer.trust_record().clone()])
                .map_err(|error| anyhow::anyhow!(error.to_string()))?,
        );
        Ok(Self {
            local_node,
            resolver,
            serving,
            objects,
            lister,
            authorization,
            witness_signer,
            witness_trust,
            cache: StorageScopeCache::default(),
        })
    }

    pub(crate) fn get(
        &self,
        storage: PersonalDbStorageId,
    ) -> Result<Arc<PersonalDbInstance>, ServerError> {
        self.cache.get_or_try_insert_with(storage, || {
            let scopes = PersonalDbScopes::default();
            let object_store = Arc::new(AnvilPersonalDbObjectStore::new(
                self.objects.clone(),
                self.lister.clone(),
                scopes.clone(),
            ));
            let authorizer = PersonalDbAuthorizer::new(scopes.clone(), self.authorization.clone());
            let resolver = self.resolver.scoped(storage);
            let server = PersonalDbServer::new(
                ServerConfig::new(
                    resolver.server_id(self.local_node),
                    object_store.clone(),
                    personaldb_core::ConsistencyPolicy::strict_witnessed(),
                )
                .with_primary_resolver(Arc::new(resolver.clone()))
                .with_authorizer(Arc::new(authorizer.clone()))
                .with_witness_signer(self.witness_signer.clone(), self.witness_trust.clone()),
            );
            let runtime = PersonalDbRuntime::new(
                self.local_node,
                resolver,
                self.serving.clone(),
                server,
                object_store,
            )?;
            Ok(PersonalDbInstance {
                runtime,
                scopes,
                authorizer,
            })
        })
    }
}

struct StorageScopeCache<T> {
    inner: Arc<Mutex<HashMap<PersonalDbStorageId, Arc<T>>>>,
}

impl<T> Clone for StorageScopeCache<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for StorageScopeCache<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T> StorageScopeCache<T> {
    fn get_or_try_insert_with(
        &self,
        storage: PersonalDbStorageId,
        create: impl FnOnce() -> Result<T, ServerError>,
    ) -> Result<Arc<T>, ServerError> {
        let mut entries = self
            .inner
            .lock()
            .map_err(|_| ServerError::TransportUnavailable {
                transport: TransportKind::InternalRpc,
                message: "PersonalDB instance cache lock poisoned".into(),
            })?;
        if let Some(instance) = entries.get(&storage) {
            return Ok(instance.clone());
        }
        let instance = Arc::new(create()?);
        entries.insert(storage, instance.clone());
        Ok(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_database_ids_cannot_reuse_another_storage_scopes_state() {
        #[derive(Default)]
        struct State(Mutex<Vec<String>>);

        let cache = StorageScopeCache::default();
        let first_storage = PersonalDbStorageId::new(1, 10);
        let second_storage = PersonalDbStorageId::new(2, 20);
        let database_id = personaldb_core::DatabaseId::new("same-external-id");

        let first = cache
            .get_or_try_insert_with(first_storage, || Ok(State::default()))
            .unwrap();
        first.0.lock().unwrap().push(database_id.0.clone());
        let second = cache
            .get_or_try_insert_with(second_storage, || Ok(State::default()))
            .unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
        assert!(second.0.lock().unwrap().is_empty());
        let first_again = cache
            .get_or_try_insert_with(first_storage, || Ok(State::default()))
            .unwrap();
        assert!(Arc::ptr_eq(&first, &first_again));
        assert_eq!(
            first_again.0.lock().unwrap().as_slice(),
            ["same-external-id"]
        );
    }
}
