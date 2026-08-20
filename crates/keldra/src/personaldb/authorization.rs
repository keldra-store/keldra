use keldra_authz::{
    AllowedSubject, NamespaceDefinition, ObjectRef, PERSONALDB_REALM_ID, RealmId,
    RelationDefinition, RewriteRule, Schema,
};
use keldra_store::{
    AuthzScope, BindSchemaRequest, CoordinatedAuthzRealmResult, PublishSchemaRequest, RealmBinding,
    SchemaId, StorageTenantId, Store,
};
use tonic::Status;

use crate::authz_distribution::ZanzibarDistribution;

use super::model::GroupScope;

const SCHEMA_ID: &str = "_keldra-personaldb-v1";
const GROUP_NAMESPACE: &str = "personaldb_group";
const APP_NAMESPACE: &str = "app";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GroupPermission {
    Read,
    Write,
    Materialize,
    Manage,
}

impl GroupPermission {
    pub(super) const fn relation(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Materialize => "materialize",
            Self::Manage => "manage",
        }
    }
}

pub(super) fn realm_scope(tenant: &StorageTenantId) -> Result<AuthzScope, Status> {
    AuthzScope::new(
        tenant.clone(),
        RealmId::parse(PERSONALDB_REALM_ID).map_err(input_status)?,
    )
    .map_err(input_status)
}

pub(super) fn group_resource(scope: &GroupScope) -> Result<ObjectRef, Status> {
    ObjectRef::opaque(
        GROUP_NAMESPACE,
        format!(
            "{:016x}/{}/{}",
            scope.bucket_id,
            hex::encode(scope.database_id.0.as_bytes()),
            hex::encode(scope.group_id.as_bytes())
        ),
    )
    .map_err(input_status)
}

pub(super) fn role_relation(role: keldra_api::v1::PersonalDbGroupRole) -> &'static str {
    match role {
        keldra_api::v1::PersonalDbGroupRole::Reader => "reader",
        keldra_api::v1::PersonalDbGroupRole::Writer => "writer",
        keldra_api::v1::PersonalDbGroupRole::Materializer => "materializer",
        keldra_api::v1::PersonalDbGroupRole::Manager => "manager",
        keldra_api::v1::PersonalDbGroupRole::Unspecified => unreachable!("role was validated"),
    }
}

pub(super) async fn ensure_realm(
    zanzibar: &ZanzibarDistribution,
    store: &Store,
    tenant_id: u64,
    tenant: &StorageTenantId,
) -> Result<RealmBinding, Status> {
    let scope = realm_scope(tenant)?;
    let published = zanzibar
        .publish_schema_journaled(
            tenant_id,
            store,
            PublishSchemaRequest {
                storage_tenant: tenant.clone(),
                schema_id: SchemaId::parse(SCHEMA_ID).map_err(input_status)?,
                schema: schema(),
                expected_revision: None,
            },
        )
        .await?;

    if let Some(binding) = zanzibar
        .repository()
        .get_binding(&scope)
        .map_err(input_status)?
    {
        return require_expected_binding(binding, &published.result.schema_ref);
    }

    let request = BindSchemaRequest {
        scope: scope.clone(),
        schema_ref: published.result.schema_ref.clone(),
        expected_generation: Some(0),
        expected_revision: None,
    };
    match zanzibar
        .bind_schema_journaled(tenant_id, store, request)
        .await
    {
        Ok(coordinated) => match coordinated.result {
            CoordinatedAuthzRealmResult::Bound(bound) => Ok(bound.binding),
            CoordinatedAuthzRealmResult::Tuples(_) => Err(Status::internal(
                "PersonalDB realm binding returned a tuple result",
            )),
        },
        Err(error) => match zanzibar.repository().get_binding(&scope) {
            Ok(Some(binding)) => require_expected_binding(binding, &published.result.schema_ref),
            Ok(None) | Err(_) => Err(error),
        },
    }
}

fn require_expected_binding(
    binding: RealmBinding,
    expected: &keldra_store::SchemaRef,
) -> Result<RealmBinding, Status> {
    if &binding.schema_ref == expected {
        Ok(binding)
    } else {
        Err(Status::failed_precondition(
            "the protected PersonalDB realm is bound to an incompatible schema",
        ))
    }
}

fn schema() -> Schema {
    let direct =
        |name| RelationDefinition::direct(name, [AllowedSubject::any_object(APP_NAMESPACE)]);
    let permission = |name, inherited: &[&str]| {
        RelationDefinition::permission(
            name,
            inherited.iter().map(|relation| RewriteRule::Inherit {
                relation: (*relation).to_owned(),
            }),
        )
    };
    Schema::new([NamespaceDefinition::new(
        GROUP_NAMESPACE,
        [
            direct("reader"),
            direct("writer"),
            direct("materializer"),
            direct("manager"),
            permission("read", &["reader", "manager"]),
            permission("write", &["writer", "manager"]),
            permission("materialize", &["materializer", "manager"]),
            permission("manage", &["manager"]),
        ],
    )])
}

fn input_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!(
        "invalid protected PersonalDB authorization state: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use keldra_authz::{Authorization, AuthorizationCheck, AuthorizationLimits};

    use super::*;

    #[test]
    fn manager_inherits_every_group_permission() {
        let group = ObjectRef::opaque(GROUP_NAMESPACE, "0000000000000001/db/group").unwrap();
        let manager = ObjectRef::opaque(APP_NAMESPACE, "manager").unwrap();
        let authorization = Authorization::new(
            RealmId::parse(PERSONALDB_REALM_ID).unwrap(),
            schema(),
            [keldra_authz::Tuple::new(
                group.clone(),
                "manager",
                manager.clone(),
            )],
            AuthorizationLimits::default(),
        )
        .unwrap();
        for permission in [
            GroupPermission::Read,
            GroupPermission::Write,
            GroupPermission::Materialize,
            GroupPermission::Manage,
        ] {
            assert!(
                authorization
                    .check(&AuthorizationCheck::new(
                        manager.clone(),
                        group.clone(),
                        permission.relation(),
                    ))
                    .unwrap()
            );
        }
    }
}
