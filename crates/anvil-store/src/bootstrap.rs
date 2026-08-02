use std::fmt;

use anvil_authz::{
    AllowedSubject, NamespaceDefinition, ObjectId, ObjectRef, RelationDefinition, RewriteRule,
    Schema, Tuple,
};
use argon2::{Algorithm, Argon2, Params, Version};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::key::{
    BUCKET_NAME_TYPE, BucketId, BucketIdentity, TENANT_NAME_TYPE, TenantId, bucket_name_key,
    encode_identity_value, tenant_name_key,
};
use crate::store::{CF_CREDENTIALS, CF_METADATA, CF_NAMES, VERSION_HIGH_WATERMARK_KEY};
use crate::{
    AuthzConsistency, AuthzRevision, AuthzScope, AuthzStoreError, ObjectKey, ObjectVersioning,
    SchemaId, StorageTenantId, Store, TupleBatchRequest, TupleMutation, TupleMutationKind,
    VersionId,
};

pub const SYSTEM_BOOTSTRAP_VERSION: u16 = 1;
pub const SYSTEM_SCHEMA_ID: &str = "anvil-system";
const SYSTEM_BOOTSTRAP_MARKER_KEY: &[u8] = b"system_bootstrap_complete";
const CREDENTIAL_FORMAT_VERSION: u16 = 2;
const APPLICATION_FORMAT_VERSION: u16 = 1;
const PROVISIONING_FORMAT_VERSION: u16 = 1;
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

#[derive(Clone, PartialEq, Eq)]
pub struct ApplicationCredentialRequest {
    pub storage_tenant: StorageTenantId,
    pub app_id: String,
    pub client_id: String,
    pub client_secret: String,
}

impl fmt::Debug for ApplicationCredentialRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationCredentialRequest")
            .field("storage_tenant", &self.storage_tenant)
            .field("app_id", &self.app_id)
            .field("client_id", &self.client_id)
            .field("client_secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialMutationReceipt {
    pub credential: ApplicationCredential,
    pub replayed: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ProvisionTenantRequest {
    pub storage_tenant: StorageTenantId,
    pub owner_app_id: String,
    pub owner_client_id: String,
    pub owner_client_secret: String,
    pub principal: ObjectRef,
    pub expected_authorization_revision: AuthzRevision,
    pub expected_binding_generation: u64,
}

impl fmt::Debug for ProvisionTenantRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProvisionTenantRequest")
            .field("storage_tenant", &self.storage_tenant)
            .field("owner_app_id", &self.owner_app_id)
            .field("owner_client_id", &self.owner_client_id)
            .field("owner_client_secret", &"[REDACTED]")
            .field("principal", &self.principal)
            .field(
                "expected_authorization_revision",
                &self.expected_authorization_revision,
            )
            .field(
                "expected_binding_generation",
                &self.expected_binding_generation,
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvisionTenantReceipt {
    pub credential: ApplicationCredential,
    pub authorization_revision: AuthzRevision,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateBucketRequest {
    pub storage_tenant: StorageTenantId,
    pub bucket: String,
    pub owner: ObjectRef,
    pub principal: ObjectRef,
    pub expected_authorization_revision: AuthzRevision,
    pub expected_binding_generation: u64,
    pub versioning: ObjectVersioning,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateBucketReceipt {
    pub storage_tenant: StorageTenantId,
    pub bucket: String,
    pub authorization_revision: AuthzRevision,
    pub versioning: ObjectVersioning,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SystemApplicationRole {
    Admin,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantApplicationRole {
    Owner,
    Admin,
    Reader,
    ManageTenant,
    ReadTenant,
    ManageBuckets,
    ManageAuthz,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketApplicationRole {
    Owner,
    Admin,
    Reader,
    Writer,
    GetObject,
    PutObject,
    DeleteObject,
    ManagePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplicationRoleTarget {
    System(SystemApplicationRole),
    Tenant(TenantApplicationRole),
    Bucket {
        bucket: String,
        role: BucketApplicationRole,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetApplicationRoleRequest {
    pub storage_tenant: StorageTenantId,
    pub app_id: String,
    pub target: ApplicationRoleTarget,
    pub granted: bool,
    pub principal: ObjectRef,
    pub expected_authorization_revision: AuthzRevision,
    pub expected_binding_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetApplicationRoleReceipt {
    pub authorization_revision: AuthzRevision,
    pub replayed: bool,
}

#[derive(Debug, Error)]
pub enum CredentialRepositoryError {
    #[error("system bootstrap has already completed")]
    AlreadyBootstrapped,
    #[error("invalid credential or provisioning input: {0}")]
    InvalidInput(String),
    #[error("credential or resource already exists: {0}")]
    AlreadyExists(String),
    #[error("credential or resource was not found: {0}")]
    NotFound(String),
    #[error("credential or provisioning state conflicts with the request: {0}")]
    Conflict(String),
    #[error("credential storage could not obtain operating-system entropy: {0}")]
    Entropy(String),
    #[error(transparent)]
    Authorization(#[from] AuthzStoreError),
    #[error("credential or provisioning storage failed: {0}")]
    Storage(String),
}

pub type SystemBootstrapError = CredentialRepositoryError;

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
    verifier: StoredCredentialVerifier,
}

/// KDF identity and costs are durable data so a later release can add an
/// explicit migration branch without guessing which verifier produced a
/// credential record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "algorithm", rename_all = "snake_case")]
enum StoredCredentialVerifier {
    Argon2id {
        version: u32,
        memory_cost_kib: u32,
        time_cost: u32,
        parallelism: u32,
        output_length: u32,
        salt: [u8; 32],
        output: [u8; 32],
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredApplication {
    format_version: u16,
    app_id: String,
    client_id: String,
    storage_tenant: StorageTenantId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredTenant {
    format_version: u16,
    tenant_id: TenantId,
    storage_tenant: StorageTenantId,
    owner_app_id: String,
    owner_client_id: String,
    authorization_revision: AuthzRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredBucket {
    format_version: u16,
    tenant_id: TenantId,
    bucket_id: BucketId,
    storage_tenant: StorageTenantId,
    bucket: String,
    owner: ObjectRef,
    authorization_revision: AuthzRevision,
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

    pub fn application(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
    ) -> Result<Option<ApplicationCredential>, CredentialRepositoryError> {
        self.credentials().application(storage_tenant, app_id)
    }

    pub fn provision_tenant(
        &self,
        request: ProvisionTenantRequest,
    ) -> Result<ProvisionTenantReceipt, CredentialRepositoryError> {
        self.credentials().provision_tenant(request)
    }

    pub fn create_application(
        &self,
        request: ApplicationCredentialRequest,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        self.credentials()
            .create_application(request, expected_authorization_revision)
    }

    pub fn rotate_application_credential(
        &self,
        request: ApplicationCredentialRequest,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        self.credentials()
            .rotate_application_credential(request, expected_authorization_revision)
    }

    pub fn disable_application_credential(
        &self,
        storage_tenant: StorageTenantId,
        app_id: String,
        client_id: String,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        self.credentials().disable_application_credential(
            storage_tenant,
            app_id,
            client_id,
            expected_authorization_revision,
        )
    }

    pub fn create_bucket(
        &self,
        request: CreateBucketRequest,
    ) -> Result<CreateBucketReceipt, CredentialRepositoryError> {
        self.credentials().create_bucket(request)
    }

    pub fn set_application_role(
        &self,
        request: SetApplicationRoleRequest,
    ) -> Result<SetApplicationRoleReceipt, CredentialRepositoryError> {
        self.credentials().set_application_role(request)
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
        let application_request = ApplicationCredentialRequest {
            storage_tenant: StorageTenantId::system(),
            app_id: request.app_id,
            client_id: request.client_id,
            client_secret: request.client_secret,
        };

        let mut batch = WriteBatch::default();
        authz.stage_initial_system_realm(
            &mut batch,
            SchemaId::parse(SYSTEM_SCHEMA_ID)?,
            system_schema(),
            bootstrap_application,
        )?;
        let staged = self.stage_new_application(&mut batch, &application_request)?;
        if staged.replayed {
            return Err(SystemBootstrapError::Storage(
                "bootstrap application unexpectedly existed before bootstrap".into(),
            ));
        }
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

    pub fn provision_tenant(
        &self,
        request: ProvisionTenantRequest,
    ) -> Result<ProvisionTenantReceipt, CredentialRepositoryError> {
        if request.storage_tenant.is_system() {
            return Err(CredentialRepositoryError::InvalidInput(
                "the protected system tenant cannot be provisioned".into(),
            ));
        }
        validate_principal(&request.principal)?;
        let owner = application_ref(&request.owner_app_id)?;
        let application_request = ApplicationCredentialRequest {
            storage_tenant: request.storage_tenant.clone(),
            app_id: request.owner_app_id.clone(),
            client_id: request.owner_client_id.clone(),
            client_secret: request.owner_client_secret.clone(),
        };
        validate_application_request(&application_request)?;
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;

        if let Some(tenant_id) = self
            .store
            .tenant_id_by_name(request.storage_tenant.as_str())
            .map_err(storage_error)?
        {
            let marker_key = tenant_record_key(tenant_id);
            let existing = self
                .read_json::<StoredTenant>(CF_METADATA, &marker_key)?
                .ok_or_else(|| {
                    CredentialRepositoryError::Storage(
                        "tenant name points to a missing stable-ID record".into(),
                    )
                })?;
            validate_stored_tenant(&existing, tenant_id, &request.storage_tenant)?;
            if existing.owner_app_id != request.owner_app_id
                || existing.owner_client_id != request.owner_client_id
            {
                return Err(CredentialRepositoryError::AlreadyExists(format!(
                    "storage tenant {}",
                    request.storage_tenant
                )));
            }
            let credential = self.require_matching_application(&application_request)?;
            return Ok(ProvisionTenantReceipt {
                credential,
                authorization_revision: existing.authorization_revision,
                replayed: true,
            });
        }

        require_system_revision(&authz, request.expected_authorization_revision)?;
        let _commit_guard = lock_object_commits(&self.store)?;
        let tenant_id = TenantId(self.store.clock.next().map_err(storage_error)?.0);
        let mut batch = WriteBatch::default();
        let staged = self.stage_new_application(&mut batch, &application_request)?;
        if staged.replayed {
            return Err(CredentialRepositoryError::Conflict(
                "tenant owner application exists without a tenant marker".into(),
            ));
        }
        let tuple_receipt = authz.prepare_tuple_batch(
            &TupleBatchRequest {
                scope: AuthzScope::system(),
                principal: request.principal,
                expected_revision: Some(request.expected_authorization_revision),
                expected_binding_generation: request.expected_binding_generation,
                operation_id: None,
                mutations: vec![TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(tenant_resource(&request.storage_tenant)?, "owner", owner),
                }],
            },
            &mut batch,
        )?;
        batch.put_cf(
            self.cf(CF_METADATA)?,
            tenant_record_key(tenant_id),
            encode_json(&StoredTenant {
                format_version: PROVISIONING_FORMAT_VERSION,
                tenant_id,
                storage_tenant: request.storage_tenant,
                owner_app_id: request.owner_app_id,
                owner_client_id: request.owner_client_id,
                authorization_revision: tuple_receipt.authz_revision,
            })?,
        );
        batch.put_cf(
            self.cf(CF_NAMES)?,
            tenant_name_key(application_request.storage_tenant.as_str()),
            encode_identity_value(tenant_id.0),
        );
        stage_identity_high_watermark(&self.store, &mut batch, tenant_id.0)?;
        authz.write(batch)?;
        Ok(ProvisionTenantReceipt {
            credential: staged.credential,
            authorization_revision: tuple_receipt.authz_revision,
            replayed: false,
        })
    }

    pub fn create_application(
        &self,
        request: ApplicationCredentialRequest,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        validate_application_request(&request)?;
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        require_system_revision(&authz, expected_authorization_revision)?;
        let mut batch = WriteBatch::default();
        let receipt = self.stage_new_application(&mut batch, &request)?;
        if !receipt.replayed {
            authz.write(batch)?;
        }
        Ok(receipt)
    }

    pub fn rotate_application_credential(
        &self,
        request: ApplicationCredentialRequest,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        validate_application_request(&request)?;
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        require_system_revision(&authz, expected_authorization_revision)?;
        let mut stored = self.require_stored_application_credential(
            &request.storage_tenant,
            &request.app_id,
            &request.client_id,
        )?;
        if stored.active && credential_matches(&stored.verifier, request.client_secret.as_bytes())?
        {
            return Ok(CredentialMutationReceipt {
                credential: credential_from_stored(&stored)?,
                replayed: true,
            });
        }
        stored.verifier = new_credential_verifier(request.client_secret.as_bytes())?;
        stored.active = true;
        let credential = credential_from_stored(&stored)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_CREDENTIALS)?,
            credential_key(&stored.client_id),
            encode_json(&stored)?,
        );
        authz.write(batch)?;
        Ok(CredentialMutationReceipt {
            credential,
            replayed: false,
        })
    }

    pub fn disable_application_credential(
        &self,
        storage_tenant: StorageTenantId,
        app_id: String,
        client_id: String,
        expected_authorization_revision: AuthzRevision,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        application_ref(&app_id)?;
        validate_client_id(&client_id)?;
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        require_system_revision(&authz, expected_authorization_revision)?;
        let mut stored =
            self.require_stored_application_credential(&storage_tenant, &app_id, &client_id)?;
        if !stored.active {
            return Ok(CredentialMutationReceipt {
                credential: credential_from_stored(&stored)?,
                replayed: true,
            });
        }
        stored.active = false;
        let credential = credential_from_stored(&stored)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(
            self.cf(CF_CREDENTIALS)?,
            credential_key(&stored.client_id),
            encode_json(&stored)?,
        );
        authz.write(batch)?;
        Ok(CredentialMutationReceipt {
            credential,
            replayed: false,
        })
    }

    pub fn create_bucket(
        &self,
        request: CreateBucketRequest,
    ) -> Result<CreateBucketReceipt, CredentialRepositoryError> {
        if request.storage_tenant.is_system() {
            return Err(CredentialRepositoryError::InvalidInput(
                "buckets cannot be created in the protected system tenant".into(),
            ));
        }
        validate_principal(&request.principal)?;
        validate_application_for_tenant(&request.owner, &request.storage_tenant)?;
        validate_bucket(&request.storage_tenant, &request.bucket)?;
        self.require_application(&request.storage_tenant, app_id(&request.owner)?)?;
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        let _commit_guard = lock_object_commits(&self.store)?;
        let _bucket_options_guard = self.store.bucket_options_lock.lock().map_err(|_| {
            CredentialRepositoryError::Storage("bucket-options lock is poisoned".into())
        })?;
        let tenant_id = self
            .store
            .tenant_id_by_name(request.storage_tenant.as_str())
            .map_err(storage_error)?
            .ok_or_else(|| {
                CredentialRepositoryError::NotFound(format!(
                    "storage tenant {}",
                    request.storage_tenant
                ))
            })?;
        if let Some(bucket_id) = self
            .store
            .bucket_id_by_name(tenant_id, &request.bucket)
            .map_err(storage_error)?
        {
            let existing = self
                .read_json::<StoredBucket>(
                    CF_METADATA,
                    &bucket_record_key(BucketIdentity {
                        tenant_id,
                        bucket_id,
                    }),
                )?
                .ok_or_else(|| {
                    CredentialRepositoryError::Storage(
                        "bucket name points to a missing stable-ID record".into(),
                    )
                })?;
            validate_stored_bucket(
                &existing,
                BucketIdentity {
                    tenant_id,
                    bucket_id,
                },
                &request.storage_tenant,
                &request.bucket,
            )?;
            if existing.owner != request.owner {
                return Err(CredentialRepositoryError::AlreadyExists(format!(
                    "bucket {}/{}",
                    request.storage_tenant, request.bucket
                )));
            }
            return Ok(CreateBucketReceipt {
                versioning: self
                    .store
                    .bucket_versioning(existing.storage_tenant.as_str(), &existing.bucket)
                    .map_err(storage_error)?,
                storage_tenant: existing.storage_tenant,
                bucket: existing.bucket,
                authorization_revision: existing.authorization_revision,
                replayed: true,
            });
        }
        require_system_revision(&authz, request.expected_authorization_revision)?;
        let bucket_id = BucketId(self.store.clock.next().map_err(storage_error)?.0);
        let identity = BucketIdentity {
            tenant_id,
            bucket_id,
        };
        let mut batch = WriteBatch::default();
        let tuple_receipt = authz.prepare_tuple_batch(
            &TupleBatchRequest {
                scope: AuthzScope::system(),
                principal: request.principal,
                expected_revision: Some(request.expected_authorization_revision),
                expected_binding_generation: request.expected_binding_generation,
                operation_id: None,
                mutations: vec![TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(
                        bucket_resource(&request.storage_tenant, &request.bucket)?,
                        "owner",
                        request.owner.clone(),
                    ),
                }],
            },
            &mut batch,
        )?;
        let stored = StoredBucket {
            format_version: PROVISIONING_FORMAT_VERSION,
            tenant_id,
            bucket_id,
            storage_tenant: request.storage_tenant,
            bucket: request.bucket,
            owner: request.owner,
            authorization_revision: tuple_receipt.authz_revision,
        };
        self.store
            .stage_bucket_versioning(&mut batch, identity, request.versioning)
            .map_err(storage_error)?;
        batch.put_cf(
            self.cf(CF_NAMES)?,
            bucket_name_key(tenant_id, &stored.bucket),
            encode_identity_value(bucket_id.0),
        );
        batch.put_cf(
            self.cf(CF_METADATA)?,
            bucket_record_key(identity),
            encode_json(&stored)?,
        );
        stage_identity_high_watermark(&self.store, &mut batch, bucket_id.0)?;
        authz.write(batch)?;
        Ok(CreateBucketReceipt {
            versioning: request.versioning,
            storage_tenant: stored.storage_tenant,
            bucket: stored.bucket,
            authorization_revision: tuple_receipt.authz_revision,
            replayed: false,
        })
    }

    pub fn set_application_role(
        &self,
        request: SetApplicationRoleRequest,
    ) -> Result<SetApplicationRoleReceipt, CredentialRepositoryError> {
        validate_principal(&request.principal)?;
        let application = application_ref(&request.app_id)?;
        self.require_application(&request.storage_tenant, &request.app_id)?;
        if let ApplicationRoleTarget::Bucket { bucket, .. } = &request.target {
            let identity = self
                .store
                .resolve_bucket_identity(request.storage_tenant.as_str(), bucket)
                .map_err(|_| {
                    CredentialRepositoryError::NotFound(format!(
                        "bucket {}/{}",
                        request.storage_tenant, bucket
                    ))
                })?;
            let existing = self
                .read_json::<StoredBucket>(CF_METADATA, &bucket_record_key(identity))?
                .ok_or_else(|| {
                    CredentialRepositoryError::NotFound(format!(
                        "bucket {}/{}",
                        request.storage_tenant, bucket
                    ))
                })?;
            validate_stored_bucket(&existing, identity, &request.storage_tenant, bucket)?;
        }
        let (resource, relation) = role_tuple_parts(&request.storage_tenant, &request.target)?;
        let tuple = Tuple::new(resource, relation, application);
        let authz = self.store.authz();
        let _guard = authz.lock_writes()?;
        require_system_revision(&authz, request.expected_authorization_revision)?;
        let snapshot = authz.realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)?;
        let present = snapshot.tuples.iter().any(|existing| existing == &tuple);
        if present == request.granted {
            return Ok(SetApplicationRoleReceipt {
                authorization_revision: snapshot.revision,
                replayed: true,
            });
        }
        let mut batch = WriteBatch::default();
        let receipt = authz.prepare_tuple_batch(
            &TupleBatchRequest {
                scope: AuthzScope::system(),
                principal: request.principal,
                expected_revision: Some(request.expected_authorization_revision),
                expected_binding_generation: request.expected_binding_generation,
                operation_id: None,
                mutations: vec![TupleMutation {
                    kind: if request.granted {
                        TupleMutationKind::Add
                    } else {
                        TupleMutationKind::Remove
                    },
                    tuple,
                }],
            },
            &mut batch,
        )?;
        authz.write(batch)?;
        Ok(SetApplicationRoleReceipt {
            authorization_revision: receipt.authz_revision,
            replayed: false,
        })
    }

    pub fn credential(
        &self,
        client_id: &str,
    ) -> Result<Option<ApplicationCredential>, SystemBootstrapError> {
        validate_client_id(client_id)?;
        let Some(stored) = self.read_stored_credential(client_id)? else {
            return Ok(None);
        };
        let credential = credential_from_stored(&stored)?;
        self.require_application_record(
            &credential.storage_tenant,
            &credential.app_id,
            &credential.client_id,
        )?;
        Ok(Some(credential))
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
            burn_dummy_credential_verification(client_secret.as_bytes())?;
            return Ok(None);
        };
        let credential = credential_from_stored(&stored)?;
        if credential.client_id != client_id {
            return Err(SystemBootstrapError::Storage(
                "persisted credential key does not match its client id".into(),
            ));
        }
        self.require_application_record(
            &credential.storage_tenant,
            &credential.app_id,
            &credential.client_id,
        )?;
        let matches = credential_matches(&stored.verifier, client_secret.as_bytes())?;
        if credential.active && matches {
            Ok(Some(credential))
        } else {
            Ok(None)
        }
    }

    pub fn application(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
    ) -> Result<Option<ApplicationCredential>, CredentialRepositoryError> {
        application_ref(app_id)?;
        let Some(application) = self.read_stored_application(storage_tenant, app_id)? else {
            return Ok(None);
        };
        validate_stored_application(&application, storage_tenant, app_id)?;
        let stored = self
            .read_stored_credential(&application.client_id)?
            .ok_or_else(|| {
                CredentialRepositoryError::Storage("persisted application has no credential".into())
            })?;
        let credential = credential_from_stored(&stored)?;
        if credential.storage_tenant != *storage_tenant || credential.app_id != app_id {
            return Err(CredentialRepositoryError::Storage(
                "persisted application and credential disagree".into(),
            ));
        }
        Ok(Some(credential))
    }

    fn stage_new_application(
        &self,
        batch: &mut WriteBatch,
        request: &ApplicationCredentialRequest,
    ) -> Result<CredentialMutationReceipt, CredentialRepositoryError> {
        validate_application_request(request)?;
        let existing_application =
            self.read_stored_application(&request.storage_tenant, &request.app_id)?;
        let existing_credential = self.read_stored_credential(&request.client_id)?;
        match (existing_application, existing_credential) {
            (None, None) => {}
            (Some(application), Some(credential)) => {
                validate_stored_application(
                    &application,
                    &application.storage_tenant,
                    &application.app_id,
                )?;
                let matches = application.storage_tenant == request.storage_tenant
                    && application.app_id == request.app_id
                    && application.client_id == request.client_id
                    && credential.storage_tenant == request.storage_tenant
                    && credential.app_id == request.app_id
                    && credential.client_id == request.client_id
                    && credential.active
                    && credential_matches(&credential.verifier, request.client_secret.as_bytes())?;
                if matches {
                    return Ok(CredentialMutationReceipt {
                        credential: credential_from_stored(&credential)?,
                        replayed: true,
                    });
                }
                return Err(CredentialRepositoryError::AlreadyExists(format!(
                    "application {} or client id {}",
                    request.app_id, request.client_id
                )));
            }
            (Some(_), None) => {
                return Err(CredentialRepositoryError::AlreadyExists(format!(
                    "application {}",
                    request.app_id
                )));
            }
            (None, Some(_)) => {
                return Err(CredentialRepositoryError::AlreadyExists(format!(
                    "client id {}",
                    request.client_id
                )));
            }
        }

        let stored_application = StoredApplication {
            format_version: APPLICATION_FORMAT_VERSION,
            app_id: request.app_id.clone(),
            client_id: request.client_id.clone(),
            storage_tenant: request.storage_tenant.clone(),
        };
        let stored_credential = StoredApplicationCredential {
            format_version: CREDENTIAL_FORMAT_VERSION,
            app_id: request.app_id.clone(),
            client_id: request.client_id.clone(),
            storage_tenant: request.storage_tenant.clone(),
            active: true,
            verifier: new_credential_verifier(request.client_secret.as_bytes())?,
        };
        batch.put_cf(
            self.cf(CF_CREDENTIALS)?,
            application_key(&request.app_id),
            encode_json(&stored_application)?,
        );
        batch.put_cf(
            self.cf(CF_CREDENTIALS)?,
            credential_key(&request.client_id),
            encode_json(&stored_credential)?,
        );
        Ok(CredentialMutationReceipt {
            credential: credential_from_stored(&stored_credential)?,
            replayed: false,
        })
    }

    fn require_matching_application(
        &self,
        request: &ApplicationCredentialRequest,
    ) -> Result<ApplicationCredential, CredentialRepositoryError> {
        let stored = self.require_stored_application_credential(
            &request.storage_tenant,
            &request.app_id,
            &request.client_id,
        )?;
        if !stored.active
            || !credential_matches(&stored.verifier, request.client_secret.as_bytes())?
        {
            return Err(CredentialRepositoryError::Conflict(
                "the supplied tenant owner credential does not match persisted state".into(),
            ));
        }
        credential_from_stored(&stored)
    }

    fn require_application(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
    ) -> Result<ApplicationCredential, CredentialRepositoryError> {
        self.application(storage_tenant, app_id)?
            .ok_or_else(|| CredentialRepositoryError::NotFound(format!("application {app_id}")))
    }

    fn require_stored_application_credential(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
        client_id: &str,
    ) -> Result<StoredApplicationCredential, CredentialRepositoryError> {
        self.require_application_record(storage_tenant, app_id, client_id)?;
        let stored = self
            .read_stored_credential(client_id)?
            .ok_or_else(|| CredentialRepositoryError::NotFound(format!("client id {client_id}")))?;
        let credential = credential_from_stored(&stored)?;
        if credential.storage_tenant != *storage_tenant
            || credential.app_id != app_id
            || credential.client_id != client_id
        {
            return Err(CredentialRepositoryError::Conflict(
                "application and client ID do not identify the same credential".into(),
            ));
        }
        Ok(stored)
    }

    fn require_application_record(
        &self,
        storage_tenant: &StorageTenantId,
        app_id: &str,
        client_id: &str,
    ) -> Result<StoredApplication, CredentialRepositoryError> {
        let application = self
            .read_stored_application(storage_tenant, app_id)?
            .ok_or_else(|| CredentialRepositoryError::NotFound(format!("application {app_id}")))?;
        validate_stored_application(&application, storage_tenant, app_id)?;
        if application.client_id != client_id {
            return Err(CredentialRepositoryError::Conflict(
                "application does not own the supplied client ID".into(),
            ));
        }
        Ok(application)
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

    fn read_stored_application(
        &self,
        _storage_tenant: &StorageTenantId,
        app_id: &str,
    ) -> Result<Option<StoredApplication>, CredentialRepositoryError> {
        self.read_json(CF_CREDENTIALS, &application_key(app_id))
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
    let application_request = ApplicationCredentialRequest {
        storage_tenant: StorageTenantId::system(),
        app_id: request.app_id.clone(),
        client_id: request.client_id.clone(),
        client_secret: request.client_secret.clone(),
    };
    validate_application_request(&application_request)?;
    application_ref(&request.app_id)
}

fn validate_application_request(
    request: &ApplicationCredentialRequest,
) -> Result<(), CredentialRepositoryError> {
    application_ref(&request.app_id)?;
    validate_client_id(&request.client_id)?;
    validate_client_secret(&request.client_secret)
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

fn validate_client_secret(client_secret: &str) -> Result<(), CredentialRepositoryError> {
    if client_secret.len() < MIN_CLIENT_SECRET_BYTES {
        return Err(CredentialRepositoryError::InvalidInput(format!(
            "client secret must contain at least {MIN_CLIENT_SECRET_BYTES} UTF-8 bytes"
        )));
    }
    if client_secret.len() > MAX_CLIENT_SECRET_BYTES {
        return Err(CredentialRepositoryError::InvalidInput(format!(
            "client secret exceeds {MAX_CLIENT_SECRET_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn credential_from_stored(
    stored: &StoredApplicationCredential,
) -> Result<ApplicationCredential, CredentialRepositoryError> {
    if stored.format_version != CREDENTIAL_FORMAT_VERSION {
        return Err(CredentialRepositoryError::Storage(format!(
            "unsupported credential format version {}",
            stored.format_version
        )));
    }
    validate_stored_credential_verifier(&stored.verifier)?;
    validate_client_id(&stored.client_id).map_err(|error| {
        CredentialRepositoryError::Storage(format!("persisted credential is invalid: {error}"))
    })?;
    let application = application_ref(&stored.app_id).map_err(|error| {
        CredentialRepositoryError::Storage(format!("persisted application id is invalid: {error}"))
    })?;
    debug_assert!(!application.is_public());
    Ok(ApplicationCredential {
        app_id: stored.app_id.clone(),
        client_id: stored.client_id.clone(),
        storage_tenant: stored.storage_tenant.clone(),
        active: stored.active,
    })
}

fn validate_stored_application(
    stored: &StoredApplication,
    expected_tenant: &StorageTenantId,
    expected_app_id: &str,
) -> Result<(), CredentialRepositoryError> {
    if stored.format_version != APPLICATION_FORMAT_VERSION {
        return Err(CredentialRepositoryError::Storage(format!(
            "unsupported application format version {}",
            stored.format_version
        )));
    }
    application_ref(&stored.app_id).map_err(|error| {
        CredentialRepositoryError::Storage(format!("persisted application is invalid: {error}"))
    })?;
    validate_client_id(&stored.client_id).map_err(|error| {
        CredentialRepositoryError::Storage(format!("persisted application is invalid: {error}"))
    })?;
    if stored.storage_tenant != *expected_tenant || stored.app_id != expected_app_id {
        return Err(CredentialRepositoryError::Storage(
            "persisted application key does not match its identity".into(),
        ));
    }
    Ok(())
}

fn validate_stored_tenant(
    stored: &StoredTenant,
    expected_id: TenantId,
    expected: &StorageTenantId,
) -> Result<(), CredentialRepositoryError> {
    if stored.format_version != PROVISIONING_FORMAT_VERSION
        || stored.tenant_id != expected_id
        || stored.storage_tenant != *expected
        || stored.storage_tenant.is_system()
        || stored.authorization_revision == AuthzRevision::ZERO
    {
        return Err(CredentialRepositoryError::Storage(
            "persisted tenant marker is invalid".into(),
        ));
    }
    application_ref(&stored.owner_app_id).map_err(storage_error)?;
    validate_client_id(&stored.owner_client_id).map_err(storage_error)
}

fn validate_stored_bucket(
    stored: &StoredBucket,
    expected_identity: BucketIdentity,
    expected_tenant: &StorageTenantId,
    expected_bucket: &str,
) -> Result<(), CredentialRepositoryError> {
    if stored.format_version != PROVISIONING_FORMAT_VERSION
        || stored.tenant_id != expected_identity.tenant_id
        || stored.bucket_id != expected_identity.bucket_id
        || stored.storage_tenant != *expected_tenant
        || stored.bucket != expected_bucket
        || stored.authorization_revision == AuthzRevision::ZERO
    {
        return Err(CredentialRepositoryError::Storage(
            "persisted bucket marker is invalid".into(),
        ));
    }
    validate_bucket(expected_tenant, expected_bucket)?;
    validate_application_for_tenant(&stored.owner, expected_tenant)
}

fn application_ref(app_id: &str) -> Result<ObjectRef, CredentialRepositoryError> {
    let application = ObjectRef::opaque(APP_NAMESPACE, app_id)
        .map_err(|error| CredentialRepositoryError::InvalidInput(error.to_string()))?;
    if application.is_public() {
        return Err(CredentialRepositoryError::InvalidInput(
            "application cannot be the reserved public subject".into(),
        ));
    }
    Ok(application)
}

fn app_id(application: &ObjectRef) -> Result<&str, CredentialRepositoryError> {
    if application.namespace != APP_NAMESPACE {
        return Err(CredentialRepositoryError::InvalidInput(
            "application principal must use the app namespace".into(),
        ));
    }
    let ObjectId::Opaque(app_id) = &application.id else {
        return Err(CredentialRepositoryError::InvalidInput(
            "application principal must use an opaque ID".into(),
        ));
    };
    if application_ref(app_id)? != *application {
        return Err(CredentialRepositoryError::InvalidInput(
            "application principal is not canonical".into(),
        ));
    }
    Ok(app_id)
}

fn validate_principal(principal: &ObjectRef) -> Result<(), CredentialRepositoryError> {
    app_id(principal).map(|_| ())
}

fn validate_application_for_tenant(
    application: &ObjectRef,
    _storage_tenant: &StorageTenantId,
) -> Result<(), CredentialRepositoryError> {
    app_id(application).map(|_| ())
}

fn validate_bucket(
    storage_tenant: &StorageTenantId,
    bucket: &str,
) -> Result<(), CredentialRepositoryError> {
    ObjectKey::new(storage_tenant.as_str(), bucket, "_anvil/resource-check")
        .map(|_| ())
        .map_err(|error| CredentialRepositoryError::InvalidInput(error.to_string()))
}

fn require_system_revision(
    authz: &crate::AuthzRepository,
    expected: AuthzRevision,
) -> Result<(), CredentialRepositoryError> {
    let current = authz.tenant_revision(&StorageTenantId::system())?;
    if current != expected {
        return Err(AuthzStoreError::RevisionConflict { expected, current }.into());
    }
    Ok(())
}

fn tenant_resource(
    storage_tenant: &StorageTenantId,
) -> Result<ObjectRef, CredentialRepositoryError> {
    ObjectRef::opaque(STORAGE_TENANT_NAMESPACE, storage_tenant.as_str())
        .map_err(|error| CredentialRepositoryError::InvalidInput(error.to_string()))
}

fn bucket_resource(
    storage_tenant: &StorageTenantId,
    bucket: &str,
) -> Result<ObjectRef, CredentialRepositoryError> {
    validate_bucket(storage_tenant, bucket)?;
    ObjectRef::opaque(
        BUCKET_NAMESPACE,
        format!("{}/{bucket}", storage_tenant.as_str()),
    )
    .map_err(|error| CredentialRepositoryError::InvalidInput(error.to_string()))
}

fn role_tuple_parts(
    storage_tenant: &StorageTenantId,
    target: &ApplicationRoleTarget,
) -> Result<(ObjectRef, &'static str), CredentialRepositoryError> {
    match target {
        ApplicationRoleTarget::System(role) => {
            if !storage_tenant.is_system() {
                return Err(CredentialRepositoryError::InvalidInput(
                    "system roles may be assigned only to system applications".into(),
                ));
            }
            let relation = match role {
                SystemApplicationRole::Admin => "admin",
            };
            Ok((
                ObjectRef::opaque(SYSTEM_NAMESPACE, crate::SYSTEM_STORAGE_TENANT_ID)
                    .map_err(|error| CredentialRepositoryError::InvalidInput(error.to_string()))?,
                relation,
            ))
        }
        ApplicationRoleTarget::Tenant(role) => {
            if storage_tenant.is_system() {
                return Err(CredentialRepositoryError::InvalidInput(
                    "the protected system tenant has no tenant roles".into(),
                ));
            }
            let relation = match role {
                TenantApplicationRole::Owner => "owner",
                TenantApplicationRole::Admin => "admin",
                TenantApplicationRole::Reader => "reader",
                TenantApplicationRole::ManageTenant => "manage_tenant_grant",
                TenantApplicationRole::ReadTenant => "read_tenant_grant",
                TenantApplicationRole::ManageBuckets => "manage_buckets_grant",
                TenantApplicationRole::ManageAuthz => "manage_authz_grant",
            };
            Ok((tenant_resource(storage_tenant)?, relation))
        }
        ApplicationRoleTarget::Bucket { bucket, role } => {
            if storage_tenant.is_system() {
                return Err(CredentialRepositoryError::InvalidInput(
                    "the protected system tenant has no buckets".into(),
                ));
            }
            let relation = match role {
                BucketApplicationRole::Owner => "owner",
                BucketApplicationRole::Admin => "admin",
                BucketApplicationRole::Reader => "reader",
                BucketApplicationRole::Writer => "writer",
                BucketApplicationRole::GetObject => "get_object_grant",
                BucketApplicationRole::PutObject => "put_object_grant",
                BucketApplicationRole::DeleteObject => "delete_object_grant",
                BucketApplicationRole::ManagePolicy => "manage_policy_grant",
            };
            Ok((bucket_resource(storage_tenant, bucket)?, relation))
        }
    }
}

fn new_credential_verifier(
    secret: &[u8],
) -> Result<StoredCredentialVerifier, CredentialRepositoryError> {
    let mut salt = [0_u8; 32];
    fill_salt(&mut salt)?;
    let output = derive_current_credential_output(&salt, secret)?;
    Ok(StoredCredentialVerifier::Argon2id {
        version: Version::V0x13.into(),
        memory_cost_kib: Params::DEFAULT_M_COST,
        time_cost: Params::DEFAULT_T_COST,
        parallelism: Params::DEFAULT_P_COST,
        output_length: Params::DEFAULT_OUTPUT_LEN as u32,
        salt,
        output,
    })
}

fn credential_matches(
    verifier: &StoredCredentialVerifier,
    secret: &[u8],
) -> Result<bool, CredentialRepositoryError> {
    validate_stored_credential_verifier(verifier)?;
    let StoredCredentialVerifier::Argon2id { salt, output, .. } = verifier;
    let candidate = derive_current_credential_output(salt, secret)?;
    Ok(bool::from(candidate.ct_eq(output)))
}

fn validate_stored_credential_verifier(
    verifier: &StoredCredentialVerifier,
) -> Result<(), CredentialRepositoryError> {
    match verifier {
        StoredCredentialVerifier::Argon2id {
            version,
            memory_cost_kib,
            time_cost,
            parallelism,
            output_length,
            ..
        } if *version == u32::from(Version::V0x13)
            && *memory_cost_kib == Params::DEFAULT_M_COST
            && *time_cost == Params::DEFAULT_T_COST
            && *parallelism == Params::DEFAULT_P_COST
            && *output_length == Params::DEFAULT_OUTPUT_LEN as u32 =>
        {
            Ok(())
        }
        StoredCredentialVerifier::Argon2id { .. } => Err(CredentialRepositoryError::Storage(
            "persisted Argon2id credential uses unsupported version or parameters".into(),
        )),
    }
}

fn derive_current_credential_output(
    salt: &[u8; 32],
    secret: &[u8],
) -> Result<[u8; 32], CredentialRepositoryError> {
    let mut output = [0_u8; Params::DEFAULT_OUTPUT_LEN];
    Argon2::new(Algorithm::Argon2id, Version::V0x13, Params::default())
        .hash_password_into(secret, salt, &mut output)
        .map_err(|error| {
            CredentialRepositoryError::Storage(format!(
                "Argon2id credential derivation failed: {error}"
            ))
        })?;
    Ok(output)
}

fn burn_dummy_credential_verification(secret: &[u8]) -> Result<(), CredentialRepositoryError> {
    // A valid but unknown client ID must pay the same memory-hard work as a
    // known credential. The global and client-ID rate limiters run before this
    // function, so random-ID attacks cannot create unbounded KDF work.
    let output = derive_current_credential_output(&[0xA5; 32], secret)?;
    let _ = std::hint::black_box(output);
    Ok(())
}

fn credential_key(client_id: &str) -> Vec<u8> {
    let mut key = b"client\0".to_vec();
    key.extend_from_slice(client_id.as_bytes());
    key
}

fn application_key(app_id: &str) -> Vec<u8> {
    let mut key = b"application\0".to_vec();
    key.extend_from_slice(app_id.as_bytes());
    key
}

fn tenant_record_key(tenant_id: TenantId) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + size_of::<u64>());
    key.extend_from_slice(&[crate::key::STORAGE_KEY_FORMAT_VERSION, TENANT_NAME_TYPE]);
    key.extend_from_slice(&tenant_id.0.to_be_bytes());
    key
}

fn bucket_record_key(identity: BucketIdentity) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 2 * size_of::<u64>());
    key.extend_from_slice(&[crate::key::STORAGE_KEY_FORMAT_VERSION, BUCKET_NAME_TYPE]);
    key.extend_from_slice(&identity.tenant_id.0.to_be_bytes());
    key.extend_from_slice(&identity.bucket_id.0.to_be_bytes());
    key
}

fn lock_object_commits(
    store: &Store,
) -> Result<tokio::sync::MutexGuard<'_, ()>, CredentialRepositoryError> {
    match store.commit_lock.try_lock() {
        Ok(guard) => Ok(guard),
        Err(_) => Ok(store.commit_lock.blocking_lock()),
    }
}

fn stage_identity_high_watermark(
    store: &Store,
    batch: &mut WriteBatch,
    allocated_id: u64,
) -> Result<(), CredentialRepositoryError> {
    batch.put_cf(
        store.db.cf_handle(CF_METADATA).ok_or_else(|| {
            CredentialRepositoryError::Storage(
                "missing metadata column family while allocating identity".into(),
            )
        })?,
        VERSION_HIGH_WATERMARK_KEY,
        encode_json(&VersionId(allocated_id))?,
    );
    Ok(())
}

fn fill_salt(salt: &mut [u8; 32]) -> Result<(), CredentialRepositoryError> {
    getrandom::fill(salt).map_err(|error| CredentialRepositoryError::Entropy(error.to_string()))
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
    use crate::store::CF_BUCKET_OPTIONS;
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

    fn app(app_id: &str) -> ObjectRef {
        ObjectRef::opaque(APP_NAMESPACE, app_id).unwrap()
    }

    fn tenant(value: &str) -> StorageTenantId {
        StorageTenantId::parse(value).unwrap()
    }

    fn provision_request(
        storage_tenant: &str,
        owner_app_id: &str,
        owner_client_id: &str,
        revision: u64,
    ) -> ProvisionTenantRequest {
        ProvisionTenantRequest {
            storage_tenant: tenant(storage_tenant),
            owner_app_id: owner_app_id.into(),
            owner_client_id: owner_client_id.into(),
            owner_client_secret: SECRET.into(),
            principal: app("bootstrap-app"),
            expected_authorization_revision: AuthzRevision(revision),
            expected_binding_generation: 1,
        }
    }

    #[test]
    fn bootstrap_request_debug_redacts_the_secret() {
        let request = request("bootstrap-app", "bootstrap-client");
        let debug = format!("{request:?}");
        assert!(debug.contains("bootstrap-app"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(SECRET));

        let application = ApplicationCredentialRequest {
            storage_tenant: tenant("acme"),
            app_id: "app".into(),
            client_id: "client".into(),
            client_secret: SECRET.into(),
        };
        assert!(!format!("{application:?}").contains(SECRET));
        let provision = provision_request("acme", "owner", "owner-client", 3);
        assert!(!format!("{provision:?}").contains(SECRET));
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
    async fn tenant_provisioning_commits_owner_credential_marker_and_tuple_together() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        let receipt = store
            .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
            .unwrap();

        assert!(!receipt.replayed);
        assert_eq!(receipt.authorization_revision, AuthzRevision(4));
        assert_eq!(receipt.credential.storage_tenant, tenant("acme"));
        assert_eq!(receipt.credential.app_id, "owner-app");
        assert_eq!(
            store.verify_credential("owner-client", SECRET).unwrap(),
            Some(receipt.credential.clone())
        );
        let tenant_id = store.tenant_id_by_name("acme").unwrap().unwrap();
        let marker = store
            .credentials()
            .read_json::<StoredTenant>(CF_METADATA, &tenant_record_key(tenant_id))
            .unwrap()
            .unwrap();
        assert_eq!(marker.tenant_id, tenant_id);
        assert_eq!(marker.authorization_revision, AuthzRevision(4));
        let snapshot = store
            .authz()
            .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
            .unwrap();
        assert!(snapshot.tuples.contains(&Tuple::new(
            tenant_resource(&tenant("acme")).unwrap(),
            "owner",
            app("owner-app"),
        )));

        let replay = store
            .provision_tenant(provision_request("acme", "owner-app", "owner-client", 4))
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.authorization_revision, AuthzRevision(4));
        assert_eq!(
            store
                .authz()
                .tenant_revision(&StorageTenantId::system())
                .unwrap(),
            AuthzRevision(4)
        );
    }

    #[tokio::test]
    async fn failed_tenant_provisioning_leaves_no_partial_owner_state() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        let mut invalid = provision_request("acme", "owner-app", "owner-client", 3);
        invalid.owner_client_secret = "too-short".into();

        assert!(matches!(
            store.provision_tenant(invalid),
            Err(CredentialRepositoryError::InvalidInput(_))
        ));
        assert!(store.credential("owner-client").unwrap().is_none());
        assert!(store.tenant_id_by_name("acme").unwrap().is_none());
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
    async fn application_credentials_create_rotate_disable_and_replay_without_plaintext() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        store
            .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
            .unwrap();
        let application = ApplicationCredentialRequest {
            storage_tenant: tenant("acme"),
            app_id: "worker-app".into(),
            client_id: "worker-client".into(),
            client_secret: SECRET.into(),
        };

        let created = store
            .create_application(application.clone(), AuthzRevision(4))
            .unwrap();
        assert!(!created.replayed);
        assert!(
            store
                .create_application(application.clone(), AuthzRevision(4))
                .unwrap()
                .replayed
        );
        assert!(
            store
                .verify_credential("worker-client", SECRET)
                .unwrap()
                .is_some()
        );

        let replacement = "replacement-0123456789abcdef0123456789abcdef0123456789abcdef";
        let rotated = store
            .rotate_application_credential(
                ApplicationCredentialRequest {
                    client_secret: replacement.into(),
                    ..application.clone()
                },
                AuthzRevision(4),
            )
            .unwrap();
        assert!(!rotated.replayed);
        assert!(
            store
                .verify_credential("worker-client", SECRET)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_some()
        );

        let disabled = store
            .disable_application_credential(
                tenant("acme"),
                "worker-app".into(),
                "worker-client".into(),
                AuthzRevision(4),
            )
            .unwrap();
        assert!(!disabled.credential.active);
        assert!(
            store
                .verify_credential("worker-client", replacement)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .disable_application_credential(
                    tenant("acme"),
                    "worker-app".into(),
                    "worker-client".into(),
                    AuthzRevision(4),
                )
                .unwrap()
                .replayed
        );
    }

    #[tokio::test]
    async fn application_ids_and_client_ids_are_globally_unique_authentication_subjects() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        store
            .provision_tenant(provision_request("acme", "acme-owner", "acme-client", 3))
            .unwrap();
        store
            .provision_tenant(provision_request("other", "other-owner", "other-client", 4))
            .unwrap();

        let duplicate_app = store.create_application(
            ApplicationCredentialRequest {
                storage_tenant: tenant("other"),
                app_id: "acme-owner".into(),
                client_id: "different-client".into(),
                client_secret: SECRET.into(),
            },
            AuthzRevision(5),
        );
        assert!(matches!(
            duplicate_app,
            Err(CredentialRepositoryError::AlreadyExists(_))
        ));
        let duplicate_client = store.create_application(
            ApplicationCredentialRequest {
                storage_tenant: tenant("other"),
                app_id: "different-app".into(),
                client_id: "acme-client".into(),
                client_secret: SECRET.into(),
            },
            AuthzRevision(5),
        );
        assert!(matches!(
            duplicate_client,
            Err(CredentialRepositoryError::AlreadyExists(_))
        ));
    }

    #[tokio::test]
    async fn bucket_creation_and_role_changes_are_system_realm_tuple_batches() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        store
            .provision_tenant(provision_request("acme", "owner-app", "owner-client", 3))
            .unwrap();
        let bucket = CreateBucketRequest {
            storage_tenant: tenant("acme"),
            bucket: "objects".into(),
            owner: app("owner-app"),
            principal: app("owner-app"),
            expected_authorization_revision: AuthzRevision(4),
            expected_binding_generation: 1,
            versioning: ObjectVersioning::Enabled,
        };

        let created = store.create_bucket(bucket.clone()).unwrap();
        assert_eq!(created.authorization_revision, AuthzRevision(5));
        assert_eq!(created.versioning, ObjectVersioning::Enabled);
        assert_eq!(
            store.bucket_versioning("acme", "objects").unwrap(),
            ObjectVersioning::Enabled
        );
        let tenant_id = store.tenant_id_by_name("acme").unwrap().unwrap();
        let bucket_id = store
            .bucket_id_by_name(tenant_id, "objects")
            .unwrap()
            .unwrap();
        let identity = BucketIdentity {
            tenant_id,
            bucket_id,
        };
        assert_eq!(
            store
                .db
                .get_cf(store.cf(CF_NAMES).unwrap(), tenant_name_key("acme"))
                .unwrap()
                .unwrap()
                .as_ref(),
            tenant_id.0.to_be_bytes()
        );
        assert_eq!(
            store
                .db
                .get_cf(
                    store.cf(CF_NAMES).unwrap(),
                    bucket_name_key(tenant_id, "objects"),
                )
                .unwrap()
                .unwrap()
                .as_ref(),
            bucket_id.0.to_be_bytes()
        );
        let stored = store
            .credentials()
            .read_json::<StoredBucket>(CF_METADATA, &bucket_record_key(identity))
            .unwrap()
            .unwrap();
        assert_eq!(stored.tenant_id, tenant_id);
        assert_eq!(stored.bucket_id, bucket_id);
        assert!(
            store
                .db
                .get_cf(store.cf(CF_BUCKET_OPTIONS).unwrap(), identity.encode())
                .unwrap()
                .is_some()
        );
        assert!(!created.replayed);
        let replayed = store.create_bucket(bucket).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.versioning, ObjectVersioning::Enabled);
        let role = SetApplicationRoleRequest {
            storage_tenant: tenant("acme"),
            app_id: "owner-app".into(),
            target: ApplicationRoleTarget::Bucket {
                bucket: "objects".into(),
                role: BucketApplicationRole::Writer,
            },
            granted: true,
            principal: app("owner-app"),
            expected_authorization_revision: AuthzRevision(5),
            expected_binding_generation: 1,
        };
        let granted = store.set_application_role(role.clone()).unwrap();
        assert_eq!(granted.authorization_revision, AuthzRevision(6));
        assert!(!granted.replayed);
        let mut replay = role.clone();
        replay.expected_authorization_revision = AuthzRevision(6);
        assert!(store.set_application_role(replay).unwrap().replayed);
        let mut remove = role;
        remove.granted = false;
        remove.expected_authorization_revision = AuthzRevision(6);
        assert_eq!(
            store
                .set_application_role(remove)
                .unwrap()
                .authorization_revision,
            AuthzRevision(7)
        );
        let snapshot = store
            .authz()
            .realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)
            .unwrap();
        assert!(snapshot.tuples.contains(&Tuple::new(
            bucket_resource(&tenant("acme"), "objects").unwrap(),
            "owner",
            app("owner-app"),
        )));
        assert!(!snapshot.tuples.contains(&Tuple::new(
            bucket_resource(&tenant("acme"), "objects").unwrap(),
            "writer",
            app("owner-app"),
        )));
    }

    #[tokio::test]
    async fn stale_authorization_revision_prevents_credential_mutation() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();
        let candidate = ApplicationCredentialRequest {
            storage_tenant: StorageTenantId::system(),
            app_id: "system-worker".into(),
            client_id: "system-worker-client".into(),
            client_secret: SECRET.into(),
        };

        assert!(matches!(
            store.create_application(candidate, AuthzRevision(2)),
            Err(CredentialRepositoryError::Authorization(
                AuthzStoreError::RevisionConflict { .. }
            ))
        ));
        assert!(store.credential("system-worker-client").unwrap().is_none());
    }

    #[test]
    fn every_typed_role_maps_to_one_declared_direct_system_relation() {
        let acme = tenant("acme");
        let cases = [
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::Owner),
                "owner",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::Admin),
                "admin",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::Reader),
                "reader",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageTenant),
                "manage_tenant_grant",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::ReadTenant),
                "read_tenant_grant",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageBuckets),
                "manage_buckets_grant",
            ),
            (
                ApplicationRoleTarget::Tenant(TenantApplicationRole::ManageAuthz),
                "manage_authz_grant",
            ),
        ];
        for (target, expected) in cases {
            assert_eq!(role_tuple_parts(&acme, &target).unwrap().1, expected);
        }
        let bucket_roles = [
            (BucketApplicationRole::Owner, "owner"),
            (BucketApplicationRole::Admin, "admin"),
            (BucketApplicationRole::Reader, "reader"),
            (BucketApplicationRole::Writer, "writer"),
            (BucketApplicationRole::GetObject, "get_object_grant"),
            (BucketApplicationRole::PutObject, "put_object_grant"),
            (BucketApplicationRole::DeleteObject, "delete_object_grant"),
            (BucketApplicationRole::ManagePolicy, "manage_policy_grant"),
        ];
        for (role, expected) in bucket_roles {
            assert_eq!(
                role_tuple_parts(
                    &acme,
                    &ApplicationRoleTarget::Bucket {
                        bucket: "objects".into(),
                        role,
                    },
                )
                .unwrap()
                .1,
                expected
            );
        }
        assert_eq!(
            role_tuple_parts(
                &StorageTenantId::system(),
                &ApplicationRoleTarget::System(SystemApplicationRole::Admin),
            )
            .unwrap()
            .1,
            "admin"
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
    async fn credential_record_persists_approved_argon2id_identity_and_costs() {
        let (_directory, store) = store().await;
        store
            .bootstrap_system(request("bootstrap-app", "bootstrap-client"))
            .unwrap();

        let stored = store
            .credentials()
            .read_stored_credential("bootstrap-client")
            .unwrap()
            .unwrap();
        assert_eq!(stored.format_version, CREDENTIAL_FORMAT_VERSION);
        let StoredCredentialVerifier::Argon2id {
            version,
            memory_cost_kib,
            time_cost,
            parallelism,
            output_length,
            salt: _,
            output: _,
        } = &stored.verifier;
        assert_eq!(*version, u32::from(Version::V0x13));
        assert_eq!(*memory_cost_kib, Params::DEFAULT_M_COST);
        assert_eq!(*time_cost, Params::DEFAULT_T_COST);
        assert_eq!(*parallelism, Params::DEFAULT_P_COST);
        assert_eq!(*output_length, Params::DEFAULT_OUTPUT_LEN as u32);
        assert!(credential_matches(&stored.verifier, SECRET.as_bytes()).unwrap());
        assert!(
            !credential_matches(&stored.verifier, b"wrong-secret-with-at-least-32-bytes",).unwrap()
        );
    }

    #[test]
    fn credential_verifier_uses_fresh_salts_and_rejects_unapproved_costs() {
        let first = new_credential_verifier(SECRET.as_bytes()).unwrap();
        let second = new_credential_verifier(SECRET.as_bytes()).unwrap();
        let (
            StoredCredentialVerifier::Argon2id {
                salt: first_salt, ..
            },
            StoredCredentialVerifier::Argon2id {
                salt: second_salt, ..
            },
        ) = (&first, &second);
        assert_ne!(first_salt, second_salt);

        let mut unsupported = first.clone();
        let StoredCredentialVerifier::Argon2id {
            memory_cost_kib, ..
        } = &mut unsupported;
        *memory_cost_kib = memory_cost_kib.saturating_add(1);
        assert!(matches!(
            validate_stored_credential_verifier(&unsupported),
            Err(CredentialRepositoryError::Storage(_))
        ));
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
