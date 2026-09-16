use keldra_authz::{ObjectId, ObjectRef, Tuple, TupleSubject};
use rocksdb::{Direction, IteratorMode, WriteBatch};

use super::{
    AuthzRepository, AuthzScope, AuthzStoreError, BindSchemaRequest, ProtectedRealmOwnership,
    StorageTenantId, StoredTuple, TupleBatchRequest, TupleMutation, TupleMutationKind, decode_json,
    storage_error, tuple_key,
};
use crate::store::CF_AUTHZ_TUPLES;

impl AuthzRepository {
    /// Validate the exact first-binding preconditions without publishing state.
    pub fn validate_first_binding(
        &self,
        request: &BindSchemaRequest,
    ) -> Result<(), AuthzStoreError> {
        if request.scope.realm.is_system() || request.expected_generation.unwrap_or(0) != 0 {
            return Err(AuthzStoreError::InvalidInput(
                "first binding requires one custom realm at generation zero".into(),
            ));
        }
        let _guard = self.lock_writes()?;
        let mut discarded = WriteBatch::default();
        self.prepare_binding(request, true, &mut discarded)
            .map(drop)
    }

    /// Enforce permanent owner identity when retrying after the prerequisite
    /// system tuples were committed but the custom binding was not.
    pub fn normalize_first_realm_owner_grant(
        &self,
        request: &mut TupleBatchRequest,
    ) -> Result<bool, AuthzStoreError> {
        if request.scope != AuthzScope::system() || request.mutations.len() != 2 {
            return Err(AuthzStoreError::InvalidInput(
                "first realm owner grant has an invalid scope or mutation count".into(),
            ));
        }
        let owner = request
            .mutations
            .iter()
            .find(|mutation| mutation.tuple.relation == "owner")
            .ok_or_else(|| {
                AuthzStoreError::InvalidInput("first realm owner grant omits owner".into())
            })?;
        let parent = request
            .mutations
            .iter()
            .find(|mutation| mutation.tuple.relation == "parent_tenant")
            .ok_or_else(|| {
                AuthzStoreError::InvalidInput("first realm owner grant omits parent".into())
            })?;
        let ObjectId::Opaque(realm_id) = &owner.tuple.object.id else {
            return Err(AuthzStoreError::InvalidInput(
                "first realm owner grant requires an opaque realm identity".into(),
            ));
        };
        let Some((storage_tenant, realm)) = realm_id.split_once('/') else {
            return Err(AuthzStoreError::InvalidInput(
                "first realm owner grant has an invalid realm identity".into(),
            ));
        };
        let expected_parent = ObjectRef::opaque("storage_tenant", storage_tenant)?;
        if owner.kind != TupleMutationKind::Add
            || parent.kind != TupleMutationKind::Add
            || owner.tuple.object != parent.tuple.object
            || owner.tuple.object.namespace != "authz_realm"
            || storage_tenant.is_empty()
            || realm.is_empty()
            || realm.contains('/')
            || owner.tuple.subject != request.principal.clone().into()
            || parent.tuple.subject != TupleSubject::Object(expected_parent)
        {
            return Err(AuthzStoreError::InvalidInput(
                "first realm owner grant has an invalid tuple shape".into(),
            ));
        }
        let scope = AuthzScope::system();
        let owner_exact = self
            .read_json::<StoredTuple>(CF_AUTHZ_TUPLES, &tuple_key(&scope, &owner.tuple)?)?
            .map(|stored| stored.tuple);
        let parent_exact = self
            .read_json::<StoredTuple>(CF_AUTHZ_TUPLES, &tuple_key(&scope, &parent.tuple)?)?
            .map(|stored| stored.tuple);
        // Canonical keys include the subject, so bounded userset scans are
        // still required to reject a different or additional permanent owner
        // after the short-lived operation receipt expires.
        let persisted_owner = self.read_userset_tuples(&scope, &owner.tuple)?;
        let persisted_parent = self.read_userset_tuples(&scope, &parent.tuple)?;
        if owner_exact.as_ref() == Some(&owner.tuple)
            && parent_exact.as_ref() == Some(&parent.tuple)
            && persisted_owner.as_slice() == std::slice::from_ref(&owner.tuple)
            && persisted_parent.as_slice() == std::slice::from_ref(&parent.tuple)
        {
            let current = self.tenant_revision(&StorageTenantId::system())?;
            let expected = request.expected_revision.ok_or_else(|| {
                AuthzStoreError::InvalidInput(
                    "first realm owner retry requires the authorization revision".into(),
                )
            })?;
            if expected != current {
                return Err(AuthzStoreError::RevisionConflict { expected, current });
            }
            request.operation_id = None;
            return Ok(true);
        }
        if persisted_owner.is_empty() && persisted_parent.is_empty() {
            return Ok(false);
        }
        Err(AuthzStoreError::InvalidInput(
            "custom realm already has a different or incomplete protected owner grant".into(),
        ))
    }

    fn read_userset_tuples(
        &self,
        scope: &AuthzScope,
        requested: &Tuple,
    ) -> Result<Vec<Tuple>, AuthzStoreError> {
        let prefix =
            super::leopard::canonical_userset_prefix(scope, &requested.object, &requested.relation);
        let mut tuples = Vec::new();
        for item in self.db.iterator_cf(
            self.cf(CF_AUTHZ_TUPLES)?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(storage_error)?;
            if !key.starts_with(&prefix) {
                break;
            }
            let tuple = decode_json::<StoredTuple>(&value)?.tuple;
            if tuple_key(scope, &tuple)?.as_slice() != key.as_ref() {
                return Err(AuthzStoreError::Storage(
                    "persisted authorization tuple key is inconsistent".into(),
                ));
            }
            tuples.push(tuple);
        }
        Ok(tuples)
    }
}

/// Construct the fixed protected-system grant used by local and distributed
/// first binding so their tuple shape cannot drift.
pub fn protected_realm_owner_request(
    custom_scope: &AuthzScope,
    protected: ProtectedRealmOwnership,
    operation_id: Option<String>,
) -> Result<TupleBatchRequest, AuthzStoreError> {
    let realm_resource = ObjectRef::opaque(
        "authz_realm",
        format!(
            "{}/{}",
            custom_scope.storage_tenant.as_str(),
            custom_scope.realm.as_str()
        ),
    )?;
    let parent_tenant = ObjectRef::opaque(
        "storage_tenant",
        custom_scope.storage_tenant.as_str().to_owned(),
    )?;
    let owner = protected.principal.clone();
    Ok(TupleBatchRequest {
        scope: AuthzScope::system(),
        principal: protected.principal,
        expected_revision: Some(protected.expected_revision),
        expected_binding_generation: protected.expected_binding_generation,
        operation_id,
        mutations: vec![
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(realm_resource.clone(), "parent_tenant", parent_tenant),
            },
            TupleMutation {
                kind: TupleMutationKind::Add,
                tuple: Tuple::new(realm_resource, "owner", owner),
            },
        ],
    })
}
