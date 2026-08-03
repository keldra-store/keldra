use anvil_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, RealmId, RelationDefinition, RewriteRule,
    Schema, Tuple,
};
use anvil_store::{
    AuthzRevision, AuthzScope, AuthzStoreError, BindSchemaRequest, PublishSchemaRequest,
    RealmBinding, SchemaId, SchemaRef, StorageTenantId, TupleBatchRequest, TupleMutation,
    TupleMutationKind,
};

use crate::authorization::{authz_realm_resource, storage_tenant_resource};

pub(crate) const PERSONALDB_REALM: &str = "personaldb";
pub(crate) const DATABASE_GROUP_NAMESPACE: &str = "database_group";
pub(crate) const PERSONALDB_TENANT_NAMESPACE: &str = "personaldb_tenant";

const APP_NAMESPACE: &str = "app";
const SCHEMA_ID: &str = "anvil-personaldb-v0";

#[cfg(test)]
const PERSONALDB_PERMISSIONS: [&str; 6] = [
    "open",
    "witness_sensitive_submit",
    "sync",
    "snapshot",
    "attach",
    "administer",
];

/// One deterministic plan for provisioning the built-in PersonalDB realm in a
/// tenant's ordinary Zanzibar group. The schema, binding, and owner tuple are
/// normal Zanzibar records; this plan is not persisted separately.
#[derive(Clone, Debug)]
pub(crate) struct PersonalDbAuthorizationBootstrap {
    stable_tenant_id: u64,
    storage_tenant: StorageTenantId,
    owner: ObjectRef,
    scope: AuthzScope,
}

impl PersonalDbAuthorizationBootstrap {
    pub(crate) fn new(
        stable_tenant_id: u64,
        storage_tenant: StorageTenantId,
        owner_app_id: &str,
    ) -> Result<Self, AuthzStoreError> {
        if stable_tenant_id == 0 {
            return Err(AuthzStoreError::InvalidInput(
                "stable PersonalDB tenant ID must be non-zero".into(),
            ));
        }
        let realm = RealmId::parse(PERSONALDB_REALM)?;
        let scope = AuthzScope::new(storage_tenant.clone(), realm)?;
        let owner = ObjectRef::opaque(APP_NAMESPACE, owner_app_id)?;
        Ok(Self {
            stable_tenant_id,
            storage_tenant,
            owner,
            scope,
        })
    }

    pub(crate) fn publish_request(&self) -> Result<PublishSchemaRequest, AuthzStoreError> {
        Ok(PublishSchemaRequest {
            storage_tenant: self.storage_tenant.clone(),
            schema_id: SchemaId::parse(SCHEMA_ID)?,
            schema: personaldb_schema(),
            // The built-in schema may be installed after another application
            // schema was published. Its immutable digest supplies replay.
            expected_revision: None,
        })
    }

    pub(crate) fn bind_request(&self, schema_ref: SchemaRef) -> BindSchemaRequest {
        BindSchemaRequest {
            scope: self.scope.clone(),
            schema_ref,
            expected_generation: Some(0),
            // Tenant owners may concurrently publish unrelated immutable
            // schemas. First-binding CAS, rather than a tenant-wide revision,
            // is the relevant condition here.
            expected_revision: None,
        }
    }

    pub(crate) fn owner_request(&self, binding: &RealmBinding) -> TupleBatchRequest {
        TupleBatchRequest {
            scope: self.scope.clone(),
            principal: self.owner.clone(),
            expected_revision: None,
            expected_binding_generation: binding.generation,
            operation_id: Some(format!(
                "provision-personaldb-owner-{}",
                self.stable_tenant_id
            )),
            mutations: vec![TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(self.tenant_authority(), "owner", self.owner.clone()),
            }],
        }
    }

    pub(crate) fn protected_realm_grant(
        &self,
        principal: ObjectRef,
        expected_revision: AuthzRevision,
        expected_binding_generation: u64,
    ) -> Result<TupleBatchRequest, AuthzStoreError> {
        let realm_resource = authz_realm_resource(self.storage_tenant.as_str(), &self.scope.realm)?;
        let parent_tenant = storage_tenant_resource(self.storage_tenant.as_str())?;
        Ok(TupleBatchRequest {
            scope: AuthzScope::system(),
            principal,
            expected_revision: Some(expected_revision),
            expected_binding_generation,
            operation_id: Some(format!(
                "provision-personaldb-realm-{}",
                self.stable_tenant_id
            )),
            mutations: vec![
                TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(realm_resource.clone(), "parent_tenant", parent_tenant),
                },
                TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(realm_resource, "owner", self.owner.clone()),
                },
            ],
        })
    }

    pub(crate) fn tenant_authority(&self) -> ObjectRef {
        ObjectRef::opaque(
            PERSONALDB_TENANT_NAMESPACE,
            self.stable_tenant_id.to_string(),
        )
        .expect("a non-zero decimal tenant ID is a valid Zanzibar object ID")
    }

    #[cfg(test)]
    fn scope(&self) -> &AuthzScope {
        &self.scope
    }
}

pub(crate) fn personaldb_schema() -> Schema {
    Schema::new([
        personaldb_namespace(PERSONALDB_TENANT_NAMESPACE),
        personaldb_namespace(DATABASE_GROUP_NAMESPACE),
    ])
}

fn personaldb_namespace(name: &str) -> NamespaceDefinition {
    NamespaceDefinition::new(
        name,
        [
            direct("owner"),
            direct("open_grant"),
            direct("submit_grant"),
            direct("sync_grant"),
            direct("snapshot_grant"),
            direct("attach_grant"),
            direct("administer_grant"),
            permission("open", "open_grant"),
            permission("witness_sensitive_submit", "submit_grant"),
            permission("sync", "sync_grant"),
            permission("snapshot", "snapshot_grant"),
            permission("attach", "attach_grant"),
            permission("administer", "administer_grant"),
        ],
    )
}

fn direct(name: &str) -> RelationDefinition {
    RelationDefinition::direct(name, [AllowedSubject::any_object(APP_NAMESPACE)])
}

fn permission(name: &str, grant: &str) -> RelationDefinition {
    RelationDefinition::permission(
        name,
        ["owner", grant].map(|relation| RewriteRule::Inherit {
            relation: relation.to_owned(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use anvil_authz::{Authorization, AuthorizationCheck, AuthorizationLimits};
    use anvil_store::{
        AuthzConsistency, CoordinatedAuthzRealmResult, PlacementLogId, Store, StoreOptions,
    };

    use super::*;

    #[test]
    fn tenant_owner_receives_every_group_permission_without_per_database_tuples() {
        let plan = PersonalDbAuthorizationBootstrap::new(
            42,
            StorageTenantId::parse("acme").unwrap(),
            "owner-app",
        )
        .unwrap();
        let owner_tuple = plan.owner_request(&binding(&plan)).mutations[0]
            .tuple
            .clone();
        let authorization = Authorization::new(
            RealmId::parse(PERSONALDB_REALM).unwrap(),
            personaldb_schema(),
            [owner_tuple],
            AuthorizationLimits::default(),
        )
        .unwrap();
        let owner = ObjectRef::opaque(APP_NAMESPACE, "owner-app").unwrap();

        for permission in PERSONALDB_PERMISSIONS {
            assert!(
                authorization
                    .check(&AuthorizationCheck::new(
                        owner.clone(),
                        plan.tenant_authority(),
                        permission,
                    ))
                    .unwrap(),
                "owner lacks {permission}"
            );
        }
    }

    #[test]
    fn bootstrap_requests_are_stable_and_separate_between_tenants() {
        let acme = PersonalDbAuthorizationBootstrap::new(
            42,
            StorageTenantId::parse("acme").unwrap(),
            "owner-app",
        )
        .unwrap();
        let retry = PersonalDbAuthorizationBootstrap::new(
            42,
            StorageTenantId::parse("acme").unwrap(),
            "owner-app",
        )
        .unwrap();
        let other = PersonalDbAuthorizationBootstrap::new(
            43,
            StorageTenantId::parse("other").unwrap(),
            "owner-app",
        )
        .unwrap();

        assert_eq!(
            acme.publish_request().unwrap(),
            retry.publish_request().unwrap()
        );
        assert_eq!(
            acme.owner_request(&binding(&acme)),
            retry.owner_request(&binding(&retry))
        );
        assert_ne!(acme.tenant_authority(), other.tenant_authority());
        assert_ne!(
            acme.owner_request(&binding(&acme)).operation_id,
            other.owner_request(&binding(&other)).operation_id
        );
    }

    #[tokio::test]
    async fn journaled_bootstrap_retries_without_another_revision_or_event() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(root.path(), 1))
            .await
            .unwrap();
        let plan = PersonalDbAuthorizationBootstrap::new(
            42,
            StorageTenantId::parse("acme").unwrap(),
            "owner-app",
        )
        .unwrap();
        let fence = PlacementLogId { term: 1, index: 1 };

        let published = store
            .coordinate_journaled_authz_schema_publication(
                42,
                plan.publish_request().unwrap(),
                fence,
                1,
            )
            .await
            .unwrap();
        assert!(!published.result.replayed);
        let bound = store
            .coordinate_journaled_authz_schema_binding(
                42,
                plan.bind_request(published.result.schema_ref.clone()),
                fence,
                1,
            )
            .await
            .unwrap();
        let CoordinatedAuthzRealmResult::Bound(bound) = bound.result else {
            panic!("binding returned a tuple receipt");
        };
        assert!(!bound.replayed);
        let owner_request = plan.owner_request(&bound.binding);
        let owner = store
            .coordinate_journaled_authz_tuple_mutation(42, owner_request.clone(), fence, 1)
            .await
            .unwrap();
        let CoordinatedAuthzRealmResult::Tuples(owner) = owner.result else {
            panic!("owner grant returned a binding");
        };
        assert!(!owner.replayed);
        let first_status = store.local_watch_status().unwrap();
        let first_revision = store
            .authz()
            .tenant_revision(&StorageTenantId::parse("acme").unwrap())
            .unwrap();

        let published_retry = store
            .coordinate_journaled_authz_schema_publication(
                42,
                plan.publish_request().unwrap(),
                fence,
                1,
            )
            .await
            .unwrap();
        assert!(published_retry.result.replayed);
        let bound_retry = store
            .coordinate_journaled_authz_schema_binding(
                42,
                plan.bind_request(published.result.schema_ref),
                fence,
                1,
            )
            .await
            .unwrap();
        let CoordinatedAuthzRealmResult::Bound(bound_retry) = bound_retry.result else {
            panic!("binding retry returned a tuple receipt");
        };
        assert!(bound_retry.replayed);
        let owner_retry = store
            .coordinate_journaled_authz_tuple_mutation(42, owner_request, fence, 1)
            .await
            .unwrap();
        let CoordinatedAuthzRealmResult::Tuples(owner_retry) = owner_retry.result else {
            panic!("owner retry returned a binding");
        };
        assert!(owner_retry.replayed);
        assert_eq!(store.local_watch_status().unwrap(), first_status);
        assert_eq!(
            store
                .authz()
                .tenant_revision(&StorageTenantId::parse("acme").unwrap())
                .unwrap(),
            first_revision
        );
        let snapshot = store
            .authz()
            .realm_snapshot(plan.scope(), AuthzConsistency::Latest)
            .unwrap();
        assert_eq!(snapshot.tuples.len(), 1);
    }

    fn binding(plan: &PersonalDbAuthorizationBootstrap) -> RealmBinding {
        RealmBinding {
            scope: plan.scope().clone(),
            schema_ref: SchemaRef {
                schema_id: SchemaId::parse(SCHEMA_ID).unwrap(),
                schema_revision: 1,
                schema_digest: anvil_store::SchemaDigest([7; 32]),
            },
            generation: 1,
            authz_revision: AuthzRevision(2),
            tuple_count: 0,
        }
    }
}
