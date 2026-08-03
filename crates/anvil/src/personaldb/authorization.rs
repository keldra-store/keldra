use anvil_authz::{AuthorizationCheck, ObjectRef, RealmId};
use anvil_store::AuthzScope;
use personaldb_server::{
    AuthContext, AuthorizationDecision, Authorizer, ResourceRef, ServerActionKind, ServerError,
};

use crate::authoritative_system::AuthoritativeSystemAuthorization;

use super::bootstrap::{DATABASE_GROUP_NAMESPACE, PERSONALDB_REALM, PERSONALDB_TENANT_NAMESPACE};
use super::scope::PersonalDbScopes;

/// Maps PersonalDB's group-level actions onto one ordinary tenant Zanzibar
/// realm. Every database is one object inside that realm, so millions of
/// databases do not create millions of distributed authorization groups.
#[derive(Clone)]
pub(crate) struct PersonalDbAuthorizer {
    scopes: PersonalDbScopes,
    authorization: AuthoritativeSystemAuthorization,
}

impl PersonalDbAuthorizer {
    pub(crate) fn new(
        scopes: PersonalDbScopes,
        authorization: AuthoritativeSystemAuthorization,
    ) -> Self {
        Self {
            scopes,
            authorization,
        }
    }
}

#[tonic::async_trait]
impl Authorizer for PersonalDbAuthorizer {
    async fn authorize(
        &self,
        _auth: &AuthContext,
        action: ServerActionKind,
        resource: ResourceRef,
    ) -> Result<AuthorizationDecision, ServerError> {
        let ResourceRef::DatabaseGroup(database_id) = resource else {
            // Hello and Ping still require Anvil bearer authentication at the
            // public or routed ingress. They name no tenant resource to check.
            return Ok(AuthorizationDecision::allow());
        };
        let scope = self
            .scopes
            .for_database(&database_id)
            .map_err(store_error)?;
        let realm = RealmId::parse(PERSONALDB_REALM).map_err(authz_error)?;
        let authz_scope = AuthzScope::new(scope.caller.storage_tenant().clone(), realm)
            .map_err(|error| transport_error(error.to_string()))?;
        let database_group = database_group_resource(scope.bucket_id, &database_id)?;
        let tenant_authority =
            ObjectRef::opaque(PERSONALDB_TENANT_NAMESPACE, scope.tenant_id.to_string())
                .map_err(authz_error)?;
        let permission = permission(action);
        let checks = vec![
            AuthorizationCheck::new(scope.caller.subject().clone(), database_group, permission),
            AuthorizationCheck::new(scope.caller.subject().clone(), tenant_authority, permission),
        ];
        let checked = self
            .authorization
            .fresh_tenant_checks(scope.tenant_id, authz_scope, checks)
            .await
            .map_err(|error| transport_error(error.message()))?;
        Ok(if checked.allowed.into_iter().any(|allowed| allowed) {
            AuthorizationDecision::allow()
        } else {
            AuthorizationDecision::deny("PersonalDB database-group permission denied")
        })
    }
}

fn database_group_resource(
    stable_bucket_id: u64,
    database_id: &personaldb_core::DatabaseId,
) -> Result<ObjectRef, ServerError> {
    if stable_bucket_id == 0 {
        return Err(transport_error(
            "PersonalDB authorization has a zero stable bucket ID",
        ));
    }
    ObjectRef::opaque(
        DATABASE_GROUP_NAMESPACE,
        format!("{stable_bucket_id}:{}", database_id.0),
    )
    .map_err(authz_error)
}

fn permission(action: ServerActionKind) -> &'static str {
    match action {
        ServerActionKind::OpenOrJoinDatabaseGroup | ServerActionKind::ResolveRoute => "open",
        ServerActionKind::SubmitWriteProposal => "witness_sensitive_submit",
        ServerActionKind::ServeCatchUp => "sync",
        ServerActionKind::ServeSnapshot => "snapshot",
        ServerActionKind::AttachDatabaseGroup => "attach",
        ServerActionKind::ReadOrMutateGroupPolicy => "administer",
        ServerActionKind::Ping => "open",
    }
}

fn store_error(error: personaldb_server_core::ObjectStoreError) -> ServerError {
    transport_error(error.to_string())
}

fn authz_error(error: impl std::fmt::Display) -> ServerError {
    transport_error(error.to_string())
}

fn transport_error(message: impl Into<String>) -> ServerError {
    ServerError::TransportUnavailable {
        transport: personaldb_server::TransportKind::Grpc,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use anvil_authz::{Authorization, AuthorizationLimits, Tuple};

    use super::super::bootstrap::personaldb_schema;
    use super::*;

    #[test]
    fn actions_map_to_spec_group_permissions() {
        assert_eq!(
            permission(ServerActionKind::OpenOrJoinDatabaseGroup),
            "open"
        );
        assert_eq!(
            permission(ServerActionKind::SubmitWriteProposal),
            "witness_sensitive_submit"
        );
        assert_eq!(permission(ServerActionKind::ServeCatchUp), "sync");
    }

    #[test]
    fn exact_database_grants_are_independent_between_stable_buckets() {
        let database_id = personaldb_core::DatabaseId::new("shared-id");
        let first = database_group_resource(10, &database_id).unwrap();
        let second = database_group_resource(20, &database_id).unwrap();
        assert_ne!(first, second);

        let principal = ObjectRef::opaque("app", "reader").unwrap();
        let authorization = Authorization::new(
            RealmId::parse(PERSONALDB_REALM).unwrap(),
            personaldb_schema(),
            [Tuple::new(first.clone(), "open_grant", principal.clone())],
            AuthorizationLimits::default(),
        )
        .unwrap();

        assert!(
            authorization
                .check(&AuthorizationCheck::new(principal.clone(), first, "open",))
                .unwrap()
        );
        assert!(
            !authorization
                .check(&AuthorizationCheck::new(principal, second, "open"))
                .unwrap()
        );
    }
}
