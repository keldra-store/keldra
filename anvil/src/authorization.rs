use anvil_authz::{
    AllowedSubject, Authorization, AuthorizationCheck, ExactPath, NamespaceDefinition, ObjectRef,
    RelationDefinition, RewriteRule, Schema,
};
use std::sync::{Arc, RwLock};

use anvil_store::{
    AuthzConsistency, AuthzRepository, AuthzRevision, AuthzScope, AuthzStoreError,
    BindSchemaRequest, ObjectKey, PublishSchemaRequest, RealmSnapshot, SchemaId, StorageTenantId,
};

pub(crate) const SYSTEM_STORAGE_TENANT: &str = "_anvil";
pub(crate) const SYSTEM_SCHEMA_ID: &str = "anvil-system";
pub(crate) const SYSTEM_NAMESPACE: &str = "system";
pub(crate) const SYSTEM_OBJECT_ID: &str = "_anvil";
pub(crate) const STORAGE_TENANT_NAMESPACE: &str = "storage_tenant";
pub(crate) const BUCKET_NAMESPACE: &str = "bucket";
pub(crate) const OBJECT_NAMESPACE: &str = "object";
pub(crate) const AUTHZ_REALM_NAMESPACE: &str = "authz_realm";
pub(crate) const APP_NAMESPACE: &str = "app";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectPermission {
    Get,
    Put,
    Delete,
}

/// Realm-scoped operations only. Schema publication and lookup are
/// tenant-scoped because their requests do not name a realm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RealmPermission {
    BindSchema,
    WriteTuples,
    Check,
    List,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageTenantPermission {
    Read,
    ManageAuthz,
}

impl StorageTenantPermission {
    fn relation(self) -> &'static str {
        match self {
            Self::Read => "read_tenant",
            Self::ManageAuthz => "manage_authz",
        }
    }
}

/// One current protected-realm view used for every authorization decision in
/// a request. Loading it once is important for bounded bulk calls: permissions
/// must not move between individual items in the same request.
#[derive(Clone, Debug)]
pub(crate) struct SystemAuthorization {
    authorization: Authorization,
    pub(crate) revision: AuthzRevision,
    pub(crate) binding_generation: u64,
}

/// Revision-keyed cache for Anvil's own protected realm. Customer realms use
/// the same repository and evaluator; this cache only avoids recompiling the
/// one graph consulted by nearly every public request.
#[derive(Clone, Debug)]
pub(crate) struct SystemAuthorizer {
    repository: AuthzRepository,
    cached: Arc<RwLock<Option<SystemAuthorization>>>,
}

impl SystemAuthorizer {
    pub(crate) fn new(repository: AuthzRepository) -> Self {
        Self {
            repository,
            cached: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) fn load(&self) -> Result<SystemAuthorization, AuthzStoreError> {
        let revision = self
            .repository
            .tenant_revision(&StorageTenantId::system())?;
        if let Some(cached) = self
            .cached
            .read()
            .map_err(|_| AuthzStoreError::Storage("system authorization cache poisoned".into()))?
            .as_ref()
            .filter(|cached| cached.revision == revision)
        {
            return Ok(cached.clone());
        }

        let loaded = SystemAuthorization::load(&self.repository)?;
        *self.cached.write().map_err(|_| {
            AuthzStoreError::Storage("system authorization cache poisoned".into())
        })? = Some(loaded.clone());
        Ok(loaded)
    }
}

impl SystemAuthorization {
    pub(crate) fn load(repository: &AuthzRepository) -> Result<Self, AuthzStoreError> {
        let snapshot =
            repository.realm_snapshot(&AuthzScope::system(), AuthzConsistency::Latest)?;
        Self::from_snapshot(snapshot, repository)
    }

    fn from_snapshot(
        snapshot: RealmSnapshot,
        repository: &AuthzRepository,
    ) -> Result<Self, AuthzStoreError> {
        let binding_generation = snapshot.binding.generation;
        let revision = snapshot.revision;
        let authorization = Authorization::new(
            snapshot.scope.realm,
            snapshot.schema,
            snapshot.tuples,
            repository.limits().evaluator,
        )?;
        Ok(Self {
            authorization,
            revision,
            binding_generation,
        })
    }

    pub(crate) fn allows_object(
        &self,
        subject: &ObjectRef,
        key: &ObjectKey,
        permission: ObjectPermission,
    ) -> anvil_authz::Result<bool> {
        allows_object(&self.authorization, subject, key, permission)
    }

    pub(crate) fn allows_bucket_policy(
        &self,
        subject: &ObjectRef,
        tenant: &str,
        bucket: &str,
    ) -> anvil_authz::Result<bool> {
        allows_bucket_policy(&self.authorization, subject, tenant, bucket)
    }

    pub(crate) fn allows_realm(
        &self,
        subject: &ObjectRef,
        tenant: &str,
        realm: &anvil_authz::RealmId,
        permission: RealmPermission,
    ) -> anvil_authz::Result<bool> {
        allows_authz_realm(&self.authorization, subject, tenant, realm, permission)
    }

    pub(crate) fn allows_storage_tenant(
        &self,
        subject: &ObjectRef,
        tenant: &str,
        permission: StorageTenantPermission,
    ) -> anvil_authz::Result<bool> {
        self.authorization.check(&AuthorizationCheck::new(
            subject.clone(),
            storage_tenant_resource(tenant)?,
            permission.relation(),
        ))
    }
}

/// Installs or advances the protected realm through the exact same immutable
/// schema publication and binding operations used by customer realms. This is
/// bootstrap authority, not an authorization bypass.
pub(crate) fn ensure_system_realm(repository: &AuthzRepository) -> Result<(), AuthzStoreError> {
    let system_tenant = StorageTenantId::system();
    let published = repository.publish_schema(PublishSchemaRequest {
        storage_tenant: system_tenant.clone(),
        schema_id: SchemaId::parse(SYSTEM_SCHEMA_ID)?,
        schema: system_schema(),
        expected_revision: None,
    })?;
    let scope = AuthzScope::system();
    let existing = repository.get_binding(&scope)?;
    if existing
        .as_ref()
        .is_some_and(|binding| binding.schema_ref == published.schema_ref)
    {
        return Ok(());
    }
    repository.bind_schema(BindSchemaRequest {
        scope,
        schema_ref: published.schema_ref,
        expected_generation: existing.map(|binding| binding.generation).or(Some(0)),
        expected_revision: Some(repository.tenant_revision(&system_tenant)?),
    })?;
    Ok(())
}

impl RealmPermission {
    fn relation(self) -> &'static str {
        match self {
            Self::BindSchema => "bind_schema",
            Self::WriteTuples => "write_tuples",
            Self::Check => "check",
            Self::List => "list",
        }
    }
}

impl ObjectPermission {
    fn object_relation(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }

    fn bucket_relation(self) -> &'static str {
        match self {
            Self::Get => "get_object",
            Self::Put => "put_object",
            Self::Delete => "delete_object",
        }
    }
}

pub(crate) fn system_schema() -> Schema {
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

pub(crate) fn caller_subject(subject_id: &str) -> anvil_authz::Result<ObjectRef> {
    ObjectRef::opaque(APP_NAMESPACE, subject_id)
}

pub(crate) fn storage_tenant_resource(tenant: &str) -> anvil_authz::Result<ObjectRef> {
    ObjectRef::opaque(STORAGE_TENANT_NAMESPACE, tenant)
}

pub(crate) fn bucket_resource(tenant: &str, bucket: &str) -> anvil_authz::Result<ObjectRef> {
    ObjectRef::opaque(BUCKET_NAMESPACE, format!("{tenant}/{bucket}"))
}

pub(crate) fn object_resource(key: &ObjectKey) -> anvil_authz::Result<ObjectRef> {
    ObjectRef::exact_path(
        OBJECT_NAMESPACE,
        ExactPath::new(key.tenant(), key.bucket(), key.path())?,
    )
}

pub(crate) fn authz_realm_resource(
    tenant: &str,
    realm: &anvil_authz::RealmId,
) -> anvil_authz::Result<ObjectRef> {
    ObjectRef::opaque(AUTHZ_REALM_NAMESPACE, format!("{tenant}/{realm}"))
}

/// Checks the exact object and then the bucket already present in its address.
/// There is deliberately no per-object parent tuple.
pub(crate) fn allows_object(
    authorization: &Authorization,
    subject: &ObjectRef,
    key: &ObjectKey,
    permission: ObjectPermission,
) -> anvil_authz::Result<bool> {
    if authorization.check(&AuthorizationCheck::new(
        subject.clone(),
        object_resource(key)?,
        permission.object_relation(),
    ))? {
        return Ok(true);
    }
    authorization.check(&AuthorizationCheck::new(
        subject.clone(),
        bucket_resource(key.tenant(), key.bucket())?,
        permission.bucket_relation(),
    ))
}

pub(crate) fn allows_bucket_policy(
    authorization: &Authorization,
    subject: &ObjectRef,
    tenant: &str,
    bucket: &str,
) -> anvil_authz::Result<bool> {
    authorization.check(&AuthorizationCheck::new(
        subject.clone(),
        bucket_resource(tenant, bucket)?,
        "manage_policy",
    ))
}

pub(crate) fn allows_authz_realm(
    authorization: &Authorization,
    subject: &ObjectRef,
    tenant: &str,
    realm: &anvil_authz::RealmId,
    permission: RealmPermission,
) -> anvil_authz::Result<bool> {
    authorization.check(&AuthorizationCheck::new(
        subject.clone(),
        authz_realm_resource(tenant, realm)?,
        permission.relation(),
    ))
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
    use anvil_authz::{AuthorizationLimits, RealmId, Tuple};
    use anvil_store::{
        AuthzScope, Store, StoreOptions, TupleBatchRequest, TupleMutation, TupleMutationKind,
    };

    use super::*;

    fn key(path: &str) -> ObjectKey {
        ObjectKey::new("acme", "objects", path).unwrap()
    }

    #[test]
    fn built_in_system_schema_is_an_ordinary_valid_schema() {
        system_schema()
            .validate(AuthorizationLimits::default())
            .unwrap();
    }

    #[test]
    fn exact_object_grant_and_bucket_fallback_use_no_parent_tuple() {
        let alice = caller_subject("alice").unwrap();
        let exact = key("private/exact.json");
        let inherited = key("bucket-wide.json");
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [
                Tuple::new(object_resource(&exact).unwrap(), "reader", alice.clone()),
                Tuple::new(
                    bucket_resource("acme", "objects").unwrap(),
                    "reader",
                    alice.clone(),
                ),
            ],
            AuthorizationLimits::default(),
        )
        .unwrap();

        assert!(allows_object(&authorization, &alice, &exact, ObjectPermission::Get).unwrap());
        assert!(allows_object(&authorization, &alice, &inherited, ObjectPermission::Get).unwrap());
        assert!(!allows_object(&authorization, &alice, &inherited, ObjectPermission::Put).unwrap());
        assert!(
            !allows_object(
                &authorization,
                &alice,
                &ObjectKey::new("acme", "other", "bucket-wide.json").unwrap(),
                ObjectPermission::Get,
            )
            .unwrap()
        );
    }

    #[test]
    fn object_authority_is_exact_and_does_not_follow_path_prefixes() {
        let alice = caller_subject("alice").unwrap();
        let parent = key("reports");
        let child = key("reports/one.json");
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [Tuple::new(
                object_resource(&parent).unwrap(),
                "reader",
                alice.clone(),
            )],
            AuthorizationLimits::default(),
        )
        .unwrap();

        assert!(allows_object(&authorization, &alice, &parent, ObjectPermission::Get).unwrap());
        assert!(!allows_object(&authorization, &alice, &child, ObjectPermission::Get).unwrap());
    }

    #[test]
    fn object_write_and_delete_use_the_same_exact_then_bucket_order() {
        let alice = caller_subject("alice").unwrap();
        let bob = caller_subject("bob").unwrap();
        let exact = key("exact.json");
        let bucket_wide = key("bucket-wide.json");
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [
                Tuple::new(object_resource(&exact).unwrap(), "writer", alice.clone()),
                Tuple::new(
                    bucket_resource("acme", "objects").unwrap(),
                    "writer",
                    bob.clone(),
                ),
            ],
            AuthorizationLimits::default(),
        )
        .unwrap();

        for permission in [ObjectPermission::Put, ObjectPermission::Delete] {
            assert!(allows_object(&authorization, &alice, &exact, permission).unwrap());
            assert!(!allows_object(&authorization, &alice, &bucket_wide, permission).unwrap());
            assert!(allows_object(&authorization, &bob, &bucket_wide, permission).unwrap());
        }
    }

    #[test]
    fn bucket_policy_uses_only_the_bucket_policy_permission() {
        let owner = caller_subject("owner").unwrap();
        let writer = caller_subject("writer").unwrap();
        let policy_manager = caller_subject("policy-manager").unwrap();
        let bucket = bucket_resource("acme", "objects").unwrap();
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [
                Tuple::new(bucket.clone(), "owner", owner.clone()),
                Tuple::new(bucket.clone(), "writer", writer.clone()),
                Tuple::new(bucket, "manage_policy_grant", policy_manager.clone()),
            ],
            AuthorizationLimits::default(),
        )
        .unwrap();

        assert!(allows_bucket_policy(&authorization, &owner, "acme", "objects").unwrap());
        assert!(allows_bucket_policy(&authorization, &policy_manager, "acme", "objects").unwrap());
        assert!(!allows_bucket_policy(&authorization, &writer, "acme", "objects").unwrap());
        assert!(!allows_bucket_policy(&authorization, &owner, "acme", "other").unwrap());
    }

    #[test]
    fn program_definitions_are_ordinary_exact_objects() {
        let alice = caller_subject("alice").unwrap();
        let bucket_writer = caller_subject("bucket-writer").unwrap();
        let first = key("_anvil/programs/import_osv@1");
        let second = key("_anvil/programs/import_osv@2");
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [
                Tuple::new(object_resource(&first).unwrap(), "writer", alice.clone()),
                Tuple::new(object_resource(&first).unwrap(), "reader", alice.clone()),
                Tuple::new(
                    bucket_resource("acme", "objects").unwrap(),
                    "writer",
                    bucket_writer.clone(),
                ),
            ],
            AuthorizationLimits::default(),
        )
        .unwrap();

        assert!(allows_object(&authorization, &alice, &first, ObjectPermission::Put).unwrap());
        assert!(allows_object(&authorization, &alice, &first, ObjectPermission::Get).unwrap());
        assert!(!allows_object(&authorization, &alice, &second, ObjectPermission::Put).unwrap());
        assert!(!allows_object(&authorization, &alice, &second, ObjectPermission::Get).unwrap());
        assert!(
            allows_object(
                &authorization,
                &bucket_writer,
                &second,
                ObjectPermission::Put,
            )
            .unwrap()
        );
    }

    #[test]
    fn realm_roles_and_parent_tenant_permissions_are_bounded_unions() {
        let realm_id = RealmId::custom("finance").unwrap();
        let realm_resource = authz_realm_resource("acme", &realm_id).unwrap();
        let tenant_resource = storage_tenant_resource("acme").unwrap();
        let tenant_owner = caller_subject("tenant-owner").unwrap();
        let tenant_reader = caller_subject("tenant-reader").unwrap();
        let authz_manager = caller_subject("authz-manager").unwrap();
        let schema_admin = caller_subject("schema-admin").unwrap();
        let tuple_writer = caller_subject("tuple-writer").unwrap();
        let checker = caller_subject("checker").unwrap();
        let auditor = caller_subject("auditor").unwrap();
        let authorization = Authorization::new(
            RealmId::system(),
            system_schema(),
            [
                Tuple::new(
                    realm_resource.clone(),
                    "parent_tenant",
                    tenant_resource.clone(),
                ),
                Tuple::new(tenant_resource.clone(), "owner", tenant_owner.clone()),
                Tuple::new(tenant_resource.clone(), "reader", tenant_reader.clone()),
                Tuple::new(tenant_resource, "manage_authz_grant", authz_manager.clone()),
                Tuple::new(realm_resource.clone(), "schema_admin", schema_admin.clone()),
                Tuple::new(realm_resource.clone(), "tuple_writer", tuple_writer.clone()),
                Tuple::new(realm_resource.clone(), "checker", checker.clone()),
                Tuple::new(realm_resource, "auditor", auditor.clone()),
            ],
            AuthorizationLimits::default(),
        )
        .unwrap();

        for permission in [
            RealmPermission::BindSchema,
            RealmPermission::WriteTuples,
            RealmPermission::Check,
            RealmPermission::List,
        ] {
            assert!(
                allows_authz_realm(&authorization, &tenant_owner, "acme", &realm_id, permission,)
                    .unwrap()
            );
        }
        for subject in [&tenant_reader, &auditor] {
            assert!(
                allows_authz_realm(
                    &authorization,
                    subject,
                    "acme",
                    &realm_id,
                    RealmPermission::Check,
                )
                .unwrap()
            );
            assert!(
                allows_authz_realm(
                    &authorization,
                    subject,
                    "acme",
                    &realm_id,
                    RealmPermission::List,
                )
                .unwrap()
            );
            assert!(
                !allows_authz_realm(
                    &authorization,
                    subject,
                    "acme",
                    &realm_id,
                    RealmPermission::WriteTuples,
                )
                .unwrap()
            );
        }
        assert!(
            allows_authz_realm(
                &authorization,
                &schema_admin,
                "acme",
                &realm_id,
                RealmPermission::BindSchema,
            )
            .unwrap()
        );
        for permission in [RealmPermission::BindSchema, RealmPermission::WriteTuples] {
            assert!(
                allows_authz_realm(
                    &authorization,
                    &authz_manager,
                    "acme",
                    &realm_id,
                    permission,
                )
                .unwrap()
            );
        }
        assert!(
            !authorization
                .check(&AuthorizationCheck::new(
                    authz_manager,
                    storage_tenant_resource("acme").unwrap(),
                    "manage_tenant",
                ))
                .unwrap()
        );
        assert!(
            allows_authz_realm(
                &authorization,
                &tuple_writer,
                "acme",
                &realm_id,
                RealmPermission::WriteTuples,
            )
            .unwrap()
        );
        assert!(
            allows_authz_realm(
                &authorization,
                &checker,
                "acme",
                &realm_id,
                RealmPermission::Check,
            )
            .unwrap()
        );
        assert!(
            !allows_authz_realm(
                &authorization,
                &checker,
                "acme",
                &realm_id,
                RealmPermission::List,
            )
            .unwrap()
        );
    }

    #[tokio::test]
    async fn system_cache_reuses_one_revision_and_refreshes_after_a_tuple_write() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(directory.path(), 1))
            .await
            .unwrap();
        let repository = store.authz();
        ensure_system_realm(&repository).unwrap();
        let authorizer = SystemAuthorizer::new(repository.clone());
        let alice = caller_subject("alice").unwrap();

        let before = authorizer.load().unwrap();
        assert_eq!(before.revision, AuthzRevision(2));
        assert!(
            !before
                .allows_storage_tenant(&alice, "acme", StorageTenantPermission::ManageAuthz)
                .unwrap()
        );

        repository
            .mutate_tuples(TupleBatchRequest {
                scope: AuthzScope::system(),
                principal: caller_subject("_anvil/bootstrap").unwrap(),
                expected_revision: Some(before.revision),
                expected_binding_generation: before.binding_generation,
                operation_id: Some("cache-refresh".into()),
                mutations: vec![TupleMutation {
                    kind: TupleMutationKind::Add,
                    tuple: Tuple::new(
                        storage_tenant_resource("acme").unwrap(),
                        "owner",
                        alice.clone(),
                    ),
                }],
            })
            .unwrap();

        let after = authorizer.load().unwrap();
        assert_eq!(after.revision, AuthzRevision(3));
        assert!(
            after
                .allows_storage_tenant(&alice, "acme", StorageTenantPermission::ManageAuthz)
                .unwrap()
        );
    }
}
