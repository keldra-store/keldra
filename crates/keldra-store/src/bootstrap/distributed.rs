use super::*;
use crate::LogicalCredentialRecord;

pub(super) fn public_bucket_reader_tuple(
    storage_tenant: &StorageTenantId,
    bucket: &str,
) -> Result<Tuple, CredentialRepositoryError> {
    Ok(Tuple::new(
        bucket_resource(storage_tenant, bucket)?,
        "reader",
        ObjectRef::anonymous(),
    ))
}

impl Store {
    /// Constructs a new application and credential without changing storage.
    pub fn prepare_application_creation(
        &self,
        request: ApplicationCredentialRequest,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        self.credentials().prepare_application_creation(request)
    }

    /// Constructs a replacement credential verifier without changing storage.
    pub fn prepare_credential_rotation(
        &self,
        request: ApplicationCredentialRequest,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        self.credentials().prepare_credential_rotation(request)
    }

    /// Constructs the disabled form of an existing credential without changing storage.
    pub fn prepare_credential_disable(
        &self,
        current: LogicalCredentialRecord,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        self.credentials().prepare_credential_disable(current)
    }

    /// Constructs one system-realm role mutation without changing storage.
    pub fn prepare_application_role_change(
        &self,
        request: SetApplicationRoleRequest,
    ) -> Result<TupleBatchRequest, CredentialRepositoryError> {
        self.credentials().prepare_application_role_change(request)
    }

    /// Constructs the one protected-system tuple that controls anonymous
    /// reads for a bucket without creating a credentialed application.
    pub fn prepare_bucket_public_read_change(
        &self,
        request: SetBucketPublicReadRequest,
    ) -> Result<TupleBatchRequest, CredentialRepositoryError> {
        self.credentials()
            .prepare_bucket_public_read_change(request)
    }
}

impl CredentialRepository {
    pub fn prepare_application_creation(
        &self,
        request: ApplicationCredentialRequest,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        validate_application_request(&request)?;
        let application = StoredApplication {
            format_version: APPLICATION_FORMAT_VERSION,
            app_id: request.app_id.clone(),
            client_id: request.client_id.clone(),
            storage_tenant: request.storage_tenant.clone(),
        };
        let credential = stored_credential(&request)?;
        let public_credential = credential_from_stored(&credential)?;
        Ok(PreparedApplicationCredential {
            credential: public_credential,
            logical_records: vec![
                LogicalRecordValue::Application(application.into()),
                LogicalRecordValue::Credential(credential.into()),
            ],
        })
    }

    pub fn prepare_credential_rotation(
        &self,
        request: ApplicationCredentialRequest,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        validate_application_request(&request)?;
        let credential = stored_credential(&request)?;
        let public_credential = credential_from_stored(&credential)?;
        Ok(PreparedApplicationCredential {
            credential: public_credential,
            logical_records: vec![LogicalRecordValue::Credential(credential.into())],
        })
    }

    pub fn prepare_credential_disable(
        &self,
        current: LogicalCredentialRecord,
    ) -> Result<PreparedApplicationCredential, CredentialRepositoryError> {
        let mut stored = StoredApplicationCredential::from(current);
        credential_from_stored(&stored)?;
        stored.active = false;
        let credential = credential_from_stored(&stored)?;
        Ok(PreparedApplicationCredential {
            credential,
            logical_records: vec![LogicalRecordValue::Credential(stored.into())],
        })
    }

    pub fn prepare_application_role_change(
        &self,
        request: SetApplicationRoleRequest,
    ) -> Result<TupleBatchRequest, CredentialRepositoryError> {
        validate_principal(&request.principal)?;
        let application = application_ref(&request.app_id)?;
        let (resource, relation) = role_tuple_parts(&request.storage_tenant, &request.target)?;
        let revision = request.expected_authorization_revision.0.to_be_bytes();
        let granted = [u8::from(request.granted)];
        let resource_bytes = serde_json::to_vec(&resource)
            .map_err(|error| CredentialRepositoryError::Storage(error.to_string()))?;
        Ok(TupleBatchRequest {
            scope: AuthzScope::system(),
            principal: request.principal,
            expected_revision: Some(request.expected_authorization_revision),
            expected_binding_generation: request.expected_binding_generation,
            operation_id: Some(mutation_operation_id(
                "application-role",
                &[
                    request.storage_tenant.as_str().as_bytes(),
                    request.app_id.as_bytes(),
                    &resource_bytes,
                    relation.as_bytes(),
                    &granted,
                    &revision,
                ],
            )),
            mutations: vec![TupleMutation {
                kind: if request.granted {
                    TupleMutationKind::Add
                } else {
                    TupleMutationKind::Remove
                },
                tuple: Tuple::new(resource, relation, application),
            }],
        })
    }

    pub fn prepare_bucket_public_read_change(
        &self,
        request: SetBucketPublicReadRequest,
    ) -> Result<TupleBatchRequest, CredentialRepositoryError> {
        validate_principal(&request.principal)?;
        if request.storage_tenant.is_system() {
            return Err(CredentialRepositoryError::InvalidInput(
                "the protected system tenant has no buckets".into(),
            ));
        }
        Ok(TupleBatchRequest {
            scope: AuthzScope::system(),
            principal: request.principal,
            expected_revision: Some(request.expected_authorization_revision),
            expected_binding_generation: request.expected_binding_generation,
            operation_id: Some(mutation_operation_id(
                "bucket-public-read",
                &[
                    request.storage_tenant.as_str().as_bytes(),
                    request.bucket.as_bytes(),
                    &[u8::from(request.enabled)],
                    &request.expected_authorization_revision.0.to_be_bytes(),
                ],
            )),
            mutations: vec![TupleMutation {
                kind: if request.enabled {
                    TupleMutationKind::Add
                } else {
                    TupleMutationKind::Remove
                },
                tuple: public_bucket_reader_tuple(&request.storage_tenant, &request.bucket)?,
            }],
        })
    }
}

fn mutation_operation_id(prefix: &str, parts: &[&[u8]]) -> String {
    let mut hash = blake3::Hasher::new_derive_key("keldra.system-mutation-operation-id/v1");
    hash.update(prefix.as_bytes());
    for part in parts {
        hash.update(&(part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    format!("{prefix}-{}", hash.finalize().to_hex())
}

fn stored_credential(
    request: &ApplicationCredentialRequest,
) -> Result<StoredApplicationCredential, CredentialRepositoryError> {
    Ok(StoredApplicationCredential {
        format_version: CREDENTIAL_FORMAT_VERSION,
        app_id: request.app_id.clone(),
        client_id: request.client_id.clone(),
        storage_tenant: request.storage_tenant.clone(),
        active: true,
        verifier: new_credential_verifier(request.client_secret.as_bytes())?,
        sigv4_secret: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (tempfile::TempDir, Store) {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(crate::StoreOptions::new(root.path(), 1))
            .await
            .unwrap();
        (root, store)
    }

    fn request() -> ApplicationCredentialRequest {
        ApplicationCredentialRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            app_id: "worker".into(),
            client_id: "worker-client".into(),
            client_secret: "a-long-enough-secret-for-argon2id".into(),
        }
    }

    #[tokio::test]
    async fn application_planning_is_no_write_and_verifier_backed() {
        let (_root, store) = store().await;
        let request = request();
        let prepared = store.prepare_application_creation(request.clone()).unwrap();
        assert_eq!(prepared.logical_records.len(), 2);
        for value in &prepared.logical_records {
            assert!(
                store
                    .logical_record_candidate(&value.id())
                    .unwrap()
                    .is_none()
            );
        }
        let credential = prepared
            .logical_records
            .iter()
            .find_map(|value| match value {
                LogicalRecordValue::Credential(record) => Some(record),
                _ => None,
            })
            .unwrap();
        assert!(
            credential
                .verify_secret(&request.client_secret)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn disable_planning_preserves_identity_and_does_not_write() {
        let (_root, store) = store().await;
        let created = store.prepare_application_creation(request()).unwrap();
        let current = created
            .logical_records
            .into_iter()
            .find_map(|value| match value {
                LogicalRecordValue::Credential(record) => Some(record),
                _ => None,
            })
            .unwrap();
        let prepared = store.prepare_credential_disable(current).unwrap();
        assert!(!prepared.credential.active);
        let value = prepared.logical_records.first().unwrap();
        let LogicalRecordValue::Credential(disabled) = value else {
            panic!("disable must produce one typed credential record")
        };
        assert!(!disabled.active());
        assert!(
            store
                .logical_record_candidate(&value.id())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn public_read_toggle_operation_identity_tracks_its_predecessor() {
        let (_root, store) = store().await;
        let request = |revision, enabled| SetBucketPublicReadRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            bucket: "objects".into(),
            enabled,
            principal: keldra_authz::ObjectRef::opaque("app", "owner").unwrap(),
            expected_authorization_revision: crate::AuthzRevision(revision),
            expected_binding_generation: 1,
        };
        let first_enable = store
            .prepare_bucket_public_read_change(request(7, true))
            .unwrap()
            .operation_id
            .unwrap();
        let disable = store
            .prepare_bucket_public_read_change(request(8, false))
            .unwrap()
            .operation_id
            .unwrap();
        let second_enable = store
            .prepare_bucket_public_read_change(request(9, true))
            .unwrap()
            .operation_id
            .unwrap();

        assert_ne!(first_enable, disable);
        assert_ne!(first_enable, second_enable);
        assert_ne!(disable, second_enable);
    }

    #[tokio::test]
    async fn application_role_operation_identity_tracks_target_action_and_predecessor() {
        let (_root, store) = store().await;
        let request = |revision, granted, role| SetApplicationRoleRequest {
            storage_tenant: StorageTenantId::parse("acme").unwrap(),
            app_id: "worker".into(),
            target: ApplicationRoleTarget::Tenant(role),
            granted,
            principal: keldra_authz::ObjectRef::opaque("app", "owner").unwrap(),
            expected_authorization_revision: crate::AuthzRevision(revision),
            expected_binding_generation: 1,
        };
        let reader = store
            .prepare_application_role_change(request(7, true, TenantApplicationRole::Reader))
            .unwrap()
            .operation_id
            .unwrap();
        let revoked = store
            .prepare_application_role_change(request(8, false, TenantApplicationRole::Reader))
            .unwrap()
            .operation_id
            .unwrap();
        let admin = store
            .prepare_application_role_change(request(9, true, TenantApplicationRole::Admin))
            .unwrap()
            .operation_id
            .unwrap();

        assert_ne!(reader, revoked);
        assert_ne!(reader, admin);
        assert_ne!(revoked, admin);
    }
}
