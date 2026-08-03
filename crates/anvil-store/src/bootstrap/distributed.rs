use super::*;
use crate::LogicalCredentialRecord;

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
        Ok(TupleBatchRequest {
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
                tuple: Tuple::new(resource, relation, application),
            }],
        })
    }
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
}
