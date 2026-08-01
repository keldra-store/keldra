use std::fmt;

use anvil_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, RelationDefinition, RewriteRule, Schema,
};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::store::{CF_CREDENTIALS, CF_METADATA};
use crate::{AuthzStoreError, SchemaId, StorageTenantId, Store};

pub const SYSTEM_BOOTSTRAP_VERSION: u16 = 1;
pub const SYSTEM_SCHEMA_ID: &str = "anvil-system";
const SYSTEM_BOOTSTRAP_MARKER_KEY: &[u8] = b"system_bootstrap_complete";
const CREDENTIAL_FORMAT_VERSION: u16 = 1;
const MIN_CLIENT_SECRET_BYTES: usize = 32;
const MAX_CLIENT_SECRET_BYTES: usize = 4 * 1024;
const MAX_CLIENT_ID_BYTES: usize = 256;

const SYSTEM_NAMESPACE: &str = "system";
const STORAGE_TENANT_NAMESPACE: &str = "storage_tenant";
const BUCKET_NAMESPACE: &str = "bucket";
const OBJECT_NAMESPACE: &str = "object";
const AUTHZ_REALM_NAMESPACE: &str = "authz_realm";
const APP_NAMESPACE: &str = "app";

#[derive(Clone, PartialEq, Eq)]
pub struct SystemBootstrapRequest {
    pub app_id: String,
    pub client_id: String,
    pub client_secret: String,
}

impl fmt::Debug for SystemBootstrapRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemBootstrapRequest")
            .field("app_id", &self.app_id)
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemBootstrapState {
    Missing,
    Complete { version: u16 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationCredential {
    pub app_id: String,
    pub client_id: String,
    pub storage_tenant: StorageTenantId,
    pub active: bool,
}

#[derive(Debug, Error)]
pub enum SystemBootstrapError {
    #[error("system bootstrap has already completed")]
    AlreadyBootstrapped,
    #[error("invalid system bootstrap input: {0}")]
    InvalidInput(String),
    #[error("system bootstrap could not obtain operating-system entropy: {0}")]
    Entropy(String),
    #[error(transparent)]
    Authorization(#[from] AuthzStoreError),
    #[error("system bootstrap storage failed: {0}")]
    Storage(String),
}

#[derive(Clone)]
pub struct CredentialRepository {
    store: Store,
}

impl fmt::Debug for CredentialRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialRepository")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BootstrapMarker {
    version: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredApplicationCredential {
    format_version: u16,
    app_id: String,
    client_id: String,
    storage_tenant: StorageTenantId,
    active: bool,
    salt: [u8; 32],
    verifier: [u8; 32],
}

impl Store {
    pub fn credentials(&self) -> CredentialRepository {
        CredentialRepository {
            store: self.clone(),
        }
    }

    pub fn system_bootstrap_state(&self) -> Result<SystemBootstrapState, SystemBootstrapError> {
        self.credentials().system_bootstrap_state()
    }

    pub fn bootstrap_system(
        &self,
        request: SystemBootstrapRequest,
    ) -> Result<(), SystemBootstrapError> {
        self.credentials().bootstrap_system(request)
    }

    pub fn credential(
        &self,
        client_id: &str,
    ) -> Result<Option<ApplicationCredential>, SystemBootstrapError> {
        self.credentials().credential(client_id)
    }

    pub fn verify_credential(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<Option<ApplicationCredential>, SystemBootstrapError> {
        self.credentials()
            .verify_credential(client_id, client_secret)
    }
}

impl CredentialRepository {
    pub fn system_bootstrap_state(&self) -> Result<SystemBootstrapState, SystemBootstrapError> {
        match self.read_marker()? {
            None => Ok(SystemBootstrapState::Missing),
            Some(marker) if marker.version == SYSTEM_BOOTSTRAP_VERSION => {
                Ok(SystemBootstrapState::Complete {
                    version: marker.version,
                })
            }
            Some(marker) => Err(SystemBootstrapError::Storage(format!(
                "unsupported system bootstrap marker version {}",
                marker.version
            ))),
        }
    }

    pub fn bootstrap_system(
        &self,
        request: SystemBootstrapRequest,
    ) -> Result<(), SystemBootstrapError> {
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        match self.system_bootstrap_state()? {
            SystemBootstrapState::Missing => {}
            SystemBootstrapState::Complete { .. } => {
                return Err(SystemBootstrapError::AlreadyBootstrapped);
            }
        }
        let bootstrap_application = validate_bootstrap_request(&request)?;
        if self.read_stored_credential(&request.client_id)?.is_some() {
            return Err(SystemBootstrapError::InvalidInput(
                "client id is already registered".into(),
            ));
        }

        let mut salt = [0_u8; 32];
        getrandom::fill(&mut salt)
            .map_err(|error| SystemBootstrapError::Entropy(error.to_string()))?;
        let stored_credential = StoredApplicationCredential {
            format_version: CREDENTIAL_FORMAT_VERSION,
            app_id: request.app_id,
            client_id: request.client_id,
            storage_tenant: StorageTenantId::system(),
            active: true,
            salt,
            verifier: credential_verifier(&salt, request.client_secret.as_bytes()),
        };

        let mut batch = WriteBatch::default();
        authz.stage_initial_system_realm(
            &mut batch,
            SchemaId::parse(SYSTEM_SCHEMA_ID)?,
            system_schema(),
            bootstrap_application,
        )?;
        batch.put_cf(
            self.cf(CF_CREDENTIALS)?,
            credential_key(&stored_credential.client_id),
            encode_json(&stored_credential)?,
        );
        batch.put_cf(
            self.cf(CF_METADATA)?,
            SYSTEM_BOOTSTRAP_MARKER_KEY,
            encode_json(&BootstrapMarker {
                version: SYSTEM_BOOTSTRAP_VERSION,
            })?,
        );
        authz.write(batch)?;
        Ok(())
    }

    pub fn credential(
        &self,
        client_id: &str,
    ) -> Result<Option<ApplicationCredential>, SystemBootstrapError> {
        validate_client_id(client_id)?;
        self.read_stored_credential(client_id)?
            .map(|stored| validate_stored_credential(stored, client_id))
            .transpose()
    }

    pub fn verify_credential(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<Option<ApplicationCredential>, SystemBootstrapError> {
        validate_client_id(client_id)?;
        if client_secret.len() > MAX_CLIENT_SECRET_BYTES {
            return Ok(None);
        }
        let Some(stored) = self.read_stored_credential(client_id)? else {
            return Ok(None);
        };
        let credential = validate_stored_credential(stored.clone(), client_id)?;
        if !credential.active {
            return Ok(None);
        }
        let candidate = credential_verifier(&stored.salt, client_secret.as_bytes());
        if bool::from(candidate.ct_eq(&stored.verifier)) {
            Ok(Some(credential))
        } else {
            Ok(None)
        }
    }

    fn read_marker(&self) -> Result<Option<BootstrapMarker>, SystemBootstrapError> {
        self.read_json(CF_METADATA, SYSTEM_BOOTSTRAP_MARKER_KEY)
    }

    fn read_stored_credential(
        &self,
        client_id: &str,
    ) -> Result<Option<StoredApplicationCredential>, SystemBootstrapError> {
        self.read_json(CF_CREDENTIALS, &credential_key(client_id))
    }

    fn read_json<T: DeserializeOwned>(
        &self,
        cf: &'static str,
        key: &[u8],
    ) -> Result<Option<T>, SystemBootstrapError> {
        self.store
            .db
            .get_cf(self.cf(cf)?, key)
            .map_err(storage_error)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(storage_error))
            .transpose()
    }

    fn cf(&self, name: &'static str) -> Result<&rocksdb::ColumnFamily, SystemBootstrapError> {
        self.store.db.cf_handle(name).ok_or_else(|| {
            SystemBootstrapError::Storage(format!("missing bootstrap column family {name}"))
        })
    }
}

fn validate_bootstrap_request(
    request: &SystemBootstrapRequest,
) -> Result<ObjectRef, SystemBootstrapError> {
    let application = ObjectRef::opaque(APP_NAMESPACE, &request.app_id)
        .map_err(|error| SystemBootstrapError::InvalidInput(error.to_string()))?;
    if application.is_public() {
        return Err(SystemBootstrapError::InvalidInput(
            "bootstrap application cannot be the public subject".into(),
        ));
    }
    validate_client_id(&request.client_id)?;
    if request.client_secret.len() < MIN_CLIENT_SECRET_BYTES {
        return Err(SystemBootstrapError::InvalidInput(format!(
            "client secret must contain at least {MIN_CLIENT_SECRET_BYTES} UTF-8 bytes"
        )));
    }
    if request.client_secret.len() > MAX_CLIENT_SECRET_BYTES {
        return Err(SystemBootstrapError::InvalidInput(format!(
            "client secret exceeds {MAX_CLIENT_SECRET_BYTES} UTF-8 bytes"
        )));
    }
    Ok(application)
}

fn validate_client_id(client_id: &str) -> Result<(), SystemBootstrapError> {
    if client_id.is_empty()
        || client_id.len() > MAX_CLIENT_ID_BYTES
        || matches!(client_id, "." | "..")
        || client_id
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | ':' | '#'))
    {
        return Err(SystemBootstrapError::InvalidInput(
            "client id must be one non-empty canonical component of at most 256 bytes".into(),
        ));
    }
    Ok(())
}

fn validate_stored_credential(
    stored: StoredApplicationCredential,
    expected_client_id: &str,
) -> Result<ApplicationCredential, SystemBootstrapError> {
    if stored.format_version != CREDENTIAL_FORMAT_VERSION {
        return Err(SystemBootstrapError::Storage(format!(
            "unsupported credential format version {}",
            stored.format_version
        )));
    }
    validate_client_id(&stored.client_id).map_err(|error| {
        SystemBootstrapError::Storage(format!("persisted credential is invalid: {error}"))
    })?;
    if stored.client_id != expected_client_id {
        return Err(SystemBootstrapError::Storage(
            "persisted credential key does not match its client id".into(),
        ));
    }
    let application = ObjectRef::opaque(APP_NAMESPACE, &stored.app_id).map_err(|error| {
        SystemBootstrapError::Storage(format!("persisted application id is invalid: {error}"))
    })?;
    if stored.storage_tenant != StorageTenantId::system() || application.is_public() {
        return Err(SystemBootstrapError::Storage(
            "persisted bootstrap credential has an invalid identity".into(),
        ));
    }
    Ok(ApplicationCredential {
        app_id: stored.app_id,
        client_id: stored.client_id,
        storage_tenant: stored.storage_tenant,
        active: stored.active,
    })
}

fn credential_verifier(salt: &[u8; 32], secret: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("anvil.application-credential.verifier.v1");
    hasher.update(salt);
    hasher.update(&(secret.len() as u64).to_be_bytes());
    hasher.update(secret);
    *hasher.finalize().as_bytes()
}

fn credential_key(client_id: &str) -> Vec<u8> {
    client_id.as_bytes().to_vec()
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, SystemBootstrapError> {
    serde_json::to_vec(value).map_err(storage_error)
}

fn storage_error(error: impl fmt::Display) -> SystemBootstrapError {
    SystemBootstrapError::Storage(error.to_string())
}

/// The protected system realm is persisted and evaluated through the same
/// schema and tuple primitives as every application realm.
pub fn system_schema() -> Schema {
    Schema::new([
        NamespaceDefinition::new(
            SYSTEM_NAMESPACE,
            [
                direct("bootstrap_admin", APP_NAMESPACE),
                direct("admin", APP_NAMESPACE),
                permission("manage_system", ["bootstrap_admin", "admin"]),
            ],
        ),
        NamespaceDefinition::new(
            STORAGE_TENANT_NAMESPACE,
            [
                direct("owner", APP_NAMESPACE),
                direct("admin", APP_NAMESPACE),
                direct("reader", APP_NAMESPACE),
                direct("manage_tenant_grant", APP_NAMESPACE),
                direct("read_tenant_grant", APP_NAMESPACE),
                direct("manage_buckets_grant", APP_NAMESPACE),
                direct("manage_authz_grant", APP_NAMESPACE),
                permission("manage_tenant", ["owner", "admin", "manage_tenant_grant"]),
                permission(
                    "read_tenant",
                    ["owner", "admin", "reader", "read_tenant_grant"],
                ),
                permission("manage_buckets", ["owner", "admin", "manage_buckets_grant"]),
                permission("manage_authz", ["owner", "admin", "manage_authz_grant"]),
            ],
        ),
        NamespaceDefinition::new(
            BUCKET_NAMESPACE,
            [
                direct("owner", APP_NAMESPACE),
                direct("admin", APP_NAMESPACE),
                direct("reader", APP_NAMESPACE),
                direct("writer", APP_NAMESPACE),
                direct("get_object_grant", APP_NAMESPACE),
                direct("put_object_grant", APP_NAMESPACE),
                direct("delete_object_grant", APP_NAMESPACE),
                direct("manage_policy_grant", APP_NAMESPACE),
                permission("get_object", ["owner", "reader", "get_object_grant"]),
                permission("put_object", ["owner", "writer", "put_object_grant"]),
                permission("delete_object", ["owner", "writer", "delete_object_grant"]),
                permission("manage_policy", ["owner", "admin", "manage_policy_grant"]),
            ],
        ),
        NamespaceDefinition::new(
            OBJECT_NAMESPACE,
            [
                direct("owner", APP_NAMESPACE),
                direct("reader", APP_NAMESPACE),
                direct("writer", APP_NAMESPACE),
                direct("get_grant", APP_NAMESPACE),
                direct("put_grant", APP_NAMESPACE),
                direct("delete_grant", APP_NAMESPACE),
                permission("get", ["owner", "reader", "get_grant"]),
                permission("put", ["owner", "writer", "put_grant"]),
                permission("delete", ["owner", "writer", "delete_grant"]),
            ],
        ),
        NamespaceDefinition::new(
            AUTHZ_REALM_NAMESPACE,
            [
                RelationDefinition::direct(
                    "parent_tenant",
                    [AllowedSubject::any_object(STORAGE_TENANT_NAMESPACE)],
                ),
                direct("owner", APP_NAMESPACE),
                direct("schema_admin", APP_NAMESPACE),
                direct("tuple_writer", APP_NAMESPACE),
                direct("checker", APP_NAMESPACE),
                direct("auditor", APP_NAMESPACE),
                permission_via_parent(
                    "bind_schema",
                    ["owner", "schema_admin"],
                    "parent_tenant",
                    "manage_authz",
                ),
                permission_via_parent(
                    "write_tuples",
                    ["owner", "tuple_writer"],
                    "parent_tenant",
                    "manage_authz",
                ),
                permission_via_parent(
                    "check",
                    ["owner", "checker", "auditor"],
                    "parent_tenant",
                    "read_tenant",
                ),
                permission_via_parent("list", ["owner", "auditor"], "parent_tenant", "read_tenant"),
            ],
        ),
    ])
}

fn direct(name: &str, subject_namespace: &str) -> RelationDefinition {
    RelationDefinition::direct(name, [AllowedSubject::any_object(subject_namespace)])
}

fn permission<const N: usize>(name: &str, inherited: [&str; N]) -> RelationDefinition {
    RelationDefinition::permission(
        name,
        inherited.map(|relation| RewriteRule::Inherit {
            relation: relation.to_owned(),
        }),
    )
}

fn permission_via_parent<const N: usize>(
    name: &str,
    inherited: [&str; N],
    tuple_relation: &str,
    target_relation: &str,
) -> RelationDefinition {
    let mut rules = inherited
        .map(|relation| RewriteRule::Inherit {
            relation: relation.to_owned(),
        })
        .to_vec();
    rules.push(RewriteRule::computed(tuple_relation, target_relation));
    RelationDefinition::permission(name, rules)
}

#[cfg(test)]
mod tests {
    use anvil_authz::{ObjectId, RealmId, TupleSubject};
    use rocksdb::IteratorMode;
    use tempfile::TempDir;

    use super::*;
    use crate::{AuthzConsistency, AuthzRevision, AuthzScope, StoreOptions};

    const SECRET: &str = "secret-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    async fn store() -> (TempDir, Store) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 7))
            .await
            .unwrap();
        (directory, store)
    }

    fn request(app_id: &str, client_id: &str) -> SystemBootstrapRequest {
        SystemBootstrapRequest {
            app_id: app_id.into(),
            client_id: client_id.into(),
            client_secret: SECRET.into(),
        }
    }

    #[test]
    fn bootstrap_request_debug_redacts_the_secret() {
        let request = request("bootstrap-app", "bootstrap-client");
        let debug = format!("{request:?}");
        assert!(debug.contains("bootstrap-app"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(SECRET));
    }

    #[tokio::test]
    async fn bootstrap_installs_one_complete_system_state_batch() {
        let (_directory, store) = store().await;
        assert_eq!(
            store.system_bootstrap_state().unwrap(),
            SystemBootstrapState::Missing
        );

        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        assert_eq!(
            store.system_bootstrap_state().unwrap(),
            SystemBootstrapState::Complete {
                version: SYSTEM_BOOTSTRAP_VERSION,
            }
        );
        assert_eq!(
            store
                .authz()
                .tenant_revision(&StorageTenantId::system())
                .unwrap(),
            AuthzRevision(3)
        );
        let snapshot = store
            .authz()
            .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
            .unwrap();
        assert_eq!(snapshot.binding.generation, 1);
        assert_eq!(snapshot.binding.authz_revision, AuthzRevision(2));
        assert_eq!(snapshot.binding.tuple_count, 1);
        assert_eq!(snapshot.tuples.len(), 1);
        let tuple = &snapshot.tuples[0];
        assert_eq!(tuple.relation, "bootstrap_admin");
        assert_eq!(tuple.object.namespace, SYSTEM_NAMESPACE);
        assert_eq!(tuple.object.id, ObjectId::Opaque("_anvil".into()));
        assert_eq!(
            tuple.subject,
            TupleSubject::Object(ObjectRef::opaque(APP_NAMESPACE, "bootstrap-app").unwrap())
        );
    }

    #[tokio::test]
    async fn repeat_bootstrap_is_rejected_without_minting_another_administrator() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        assert!(matches!(
            store.bootstrap_system(request("other-app", "other-client")),
            Err(SystemBootstrapError::AlreadyBootstrapped)
        ));
        assert!(store.credential("other-client").unwrap().is_none());
        assert_eq!(
            store
                .authz()
                .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
                .unwrap()
                .tuples
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn credential_lookup_and_verification_return_the_stable_application() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        let expected = ApplicationCredential {
            app_id: "bootstrap-app".into(),
            client_id: "bootstrap-client".into(),
            storage_tenant: StorageTenantId::system(),
            active: true,
        };
        assert_eq!(
            store.credential("bootstrap-client").unwrap(),
            Some(expected.clone())
        );
        assert_eq!(
            store.verify_credential("bootstrap-client", SECRET).unwrap(),
            Some(expected)
        );
        assert!(
            store
                .verify_credential("bootstrap-client", "wrong-secret-with-at-least-32-bytes")
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .verify_credential("missing-client", SECRET)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn plaintext_secret_is_not_persisted_in_credential_values() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        let secret = SECRET.as_bytes();
        for name in crate::store::COLUMN_FAMILIES {
            let column_family = store.db.cf_handle(name).unwrap();
            for item in store.db.iterator_cf(column_family, IteratorMode::Start) {
                let (_key, value) = item.unwrap();
                assert!(!value.windows(secret.len()).any(|window| window == secret));
            }
        }
    }

    #[tokio::test]
    async fn invalid_input_leaves_every_bootstrap_state_absent() {
        let (_directory, store) = store().await;
        let mut invalid = request("bootstrap-app", "bootstrap-client");
        invalid.client_secret = "too-short".into();

        assert!(matches!(
            store.bootstrap_system(invalid),
            Err(SystemBootstrapError::InvalidInput(_))
        ));
        assert_eq!(
            store.system_bootstrap_state().unwrap(),
            SystemBootstrapState::Missing
        );
        assert!(store.credential("bootstrap-client").unwrap().is_none());
        assert_eq!(
            store
                .authz()
                .tenant_revision(&StorageTenantId::system())
                .unwrap(),
            AuthzRevision::ZERO
        );
        assert!(
            store
                .authz()
                .get_binding(&AuthzScope::system())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn bootstrap_marker_survives_reopen() {
        let (directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        drop(store);

        let reopened = Store::open(StoreOptions::new(directory.path(), 7))
            .await
            .unwrap();
        assert_eq!(
            reopened.system_bootstrap_state().unwrap(),
            SystemBootstrapState::Complete {
                version: SYSTEM_BOOTSTRAP_VERSION,
            }
        );
        assert!(matches!(
            reopened.bootstrap_system(request("other-app", "other-client")),
            Err(SystemBootstrapError::AlreadyBootstrapped)
        ));
    }

    #[test]
    fn schema_uses_the_protected_system_realm_id() {
        assert!(RealmId::system().is_system());
        assert!(
            system_schema()
                .namespaces
                .iter()
                .any(|namespace| namespace.name == SYSTEM_NAMESPACE)
        );
    }
}
