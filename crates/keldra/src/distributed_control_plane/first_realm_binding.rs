use keldra_authz::ObjectRef;
use keldra_store::{
    BindSchemaRequest, BoundRealm, CoordinatedAuthzRealmResult, LogicalRecordId,
    LogicalRecordValue, ProtectedRealmOwnership, TupleBatchReceipt, TupleBatchRequest,
    protected_realm_owner_request,
};
use tonic::Status;

use super::{
    CONTROL_OPERATION_TIMEOUT, ControlTarget, DistributedControlPlane, authz_evaluation_status,
};
use crate::authentication::Caller;
use crate::authorization::{SYSTEM_STABLE_TENANT_ID, storage_tenant_resource};
use crate::distributed_list::OriginalBearer;
use crate::mutable_record_replica_group::MutableRecordReplicaGroup;
use crate::placement::PlacementKind;

impl DistributedControlPlane {
    pub(super) fn system_realm_target(&self) -> Result<Option<ControlTarget>, Status> {
        self.realm_target(SYSTEM_STABLE_TENANT_ID)
    }

    pub(super) fn realm_target(
        &self,
        stable_tenant_id: u64,
    ) -> Result<Option<ControlTarget>, Status> {
        let placement = self.placement()?;
        let group = MutableRecordReplicaGroup::select(
            PlacementKind::ZanzibarRealm,
            placement.cluster_id(),
            &stable_tenant_id.to_be_bytes(),
            placement.placement_nodes(),
        )
        .ok_or_else(|| Status::unavailable("cluster has no Zanzibar replica"))?;
        let node = group.coordinator();
        if node == self.local_node {
            return Ok(None);
        }
        let address = placement
            .address(node)
            .ok_or_else(|| Status::unavailable("Zanzibar coordinator has no address"))?;
        Ok(Some(ControlTarget {
            node_id: node,
            address: address.0.clone(),
            placement_fence: placement.fence(),
        }))
    }

    pub(super) fn require_local_realm_coordinator(
        &self,
        stable_tenant_id: u64,
    ) -> Result<(), Status> {
        if self.realm_target(stable_tenant_id)?.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "realm binding did not reach its Zanzibar coordinator",
            ))
        }
    }

    pub(super) fn require_system_realm_coordinator(&self) -> Result<(), Status> {
        if self.system_realm_target()?.is_none() {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "administration grant did not reach the system Zanzibar coordinator",
            ))
        }
    }

    pub(super) fn require_same_system_realm_target(
        &self,
        expected: &ControlTarget,
    ) -> Result<(), Status> {
        if self.system_realm_target()?.as_ref() == Some(expected) {
            Ok(())
        } else {
            Err(Status::unavailable(
                "system Zanzibar placement changed during administration",
            ))
        }
    }

    pub(super) fn require_same_realm_target(
        &self,
        stable_tenant_id: u64,
        expected: &ControlTarget,
    ) -> Result<(), Status> {
        if self.realm_target(stable_tenant_id)?.as_ref() == Some(expected) {
            Ok(())
        } else {
            Err(Status::unavailable(
                "Zanzibar placement changed during first realm binding",
            ))
        }
    }

    /// Route first custom-realm publication through the Raft-nominated
    /// administration executor. That executor is the single authority which
    /// orders the protected owner prerequisite before the custom binding.
    pub(crate) async fn bind_first_custom_realm(
        &self,
        caller: Caller,
        bearer: OriginalBearer,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
    ) -> Result<BoundRealm, Status> {
        if let Some(target) = self.executor_target()? {
            return self
                .peers
                .route_first_realm_binding(
                    target.node_id,
                    &target.address,
                    bearer.signed_token(),
                    stable_tenant_id,
                    &request,
                    CONTROL_OPERATION_TIMEOUT,
                )
                .await;
        }
        self.execute_first_realm_binding(caller, stable_tenant_id, request)
            .await
    }

    pub(crate) async fn execute_routed_first_realm_binding(
        &self,
        bearer: &str,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
    ) -> Result<BoundRealm, Status> {
        let caller = self.verify_routed_bearer(bearer)?;
        self.require_local_executor().map_err(|_| {
            Status::unavailable("first realm binding executor changed before execution")
        })?;
        self.execute_first_realm_binding(caller, stable_tenant_id, request)
            .await
    }

    async fn execute_first_realm_binding(
        &self,
        caller: Caller,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
    ) -> Result<BoundRealm, Status> {
        self.require_local_executor()?;
        if request.scope.realm.is_system()
            || request.scope.storage_tenant != *caller.storage_tenant()
        {
            return Err(Status::invalid_argument(
                "first binding must target one custom realm owned by the caller's tenant",
            ));
        }
        match self
            .read_record(&LogicalRecordId::TenantNameClaim {
                storage_tenant: caller.storage_tenant().clone(),
            })
            .await?
        {
            Some(LogicalRecordValue::TenantNameClaim { tenant_id, .. })
                if tenant_id == stable_tenant_id => {}
            Some(_) => {
                return Err(Status::failed_precondition(
                    "first realm binding stable tenant identity is inconsistent",
                ));
            }
            None => return Err(Status::not_found("storage tenant does not exist")),
        }

        let _serial = self.administration_serial.lock().await;
        self.require_local_executor()?;
        let system = self
            .authorize_system(
                caller.subject(),
                storage_tenant_resource(caller.storage_tenant().as_str())
                    .map_err(authz_evaluation_status)?,
                "manage_authz",
                "first realm binding is not authorized",
            )
            .await?;

        if use_atomic_single_node_binding(self.active_node_count()?) {
            let repository = self.zanzibar.repository().clone();
            let ownership = ProtectedRealmOwnership {
                principal: caller.subject().clone(),
                expected_revision: system.revision,
                expected_binding_generation: system.binding_generation,
            };
            return tokio::task::spawn_blocking(move || {
                repository.bind_schema_with_protected_owner(request, ownership)
            })
            .await
            .map_err(|_| Status::internal("first realm binding worker failed"))?
            .map(|result| result.realm)
            .map_err(crate::authz_service::authz_store_status);
        }

        // Preflight is deliberately read-only. The protected system owner is
        // published first, so the custom binding is never observable without
        // its authorization parent and owner.
        self.apply_first_realm_binding(stable_tenant_id, &request, true)
            .await?;
        self.require_local_executor().map_err(|_| {
            Status::unavailable("first realm binding executor changed after owner publication")
        })?;
        let owner_grant = protected_realm_owner_request(
            &request.scope,
            ProtectedRealmOwnership {
                principal: caller.subject().clone(),
                expected_revision: system.revision,
                expected_binding_generation: system.binding_generation,
            },
            Some(first_realm_binding_operation_id(
                &request,
                caller.subject(),
            )?),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.apply_first_realm_owner_grant(owner_grant).await?;

        // A moved executor must not finish an operation under a stale fence.
        // The durable owner prerequisite makes the exact retry convergent.
        self.require_local_executor()?;
        self.apply_first_realm_binding(stable_tenant_id, &request, false)
            .await?
            .ok_or_else(|| Status::internal("first realm binding did not publish a binding"))
    }

    pub(crate) async fn coordinate_first_realm_binding(
        &self,
        stable_tenant_id: u64,
        request: BindSchemaRequest,
        validate_only: bool,
    ) -> Result<Option<BoundRealm>, Status> {
        self.require_local_realm_coordinator(stable_tenant_id)?;
        if validate_only {
            self.zanzibar
                .repository()
                .validate_first_binding(&request)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            return Ok(None);
        }
        let coordinated = self
            .zanzibar
            .bind_schema_journaled(stable_tenant_id, &self.store, request)
            .await?;
        match coordinated.result {
            CoordinatedAuthzRealmResult::Bound(bound) => Ok(Some(bound)),
            CoordinatedAuthzRealmResult::Tuples(_) => Err(Status::internal(
                "first realm binding returned a tuple result",
            )),
        }
    }

    async fn apply_first_realm_owner_grant(
        &self,
        request: TupleBatchRequest,
    ) -> Result<TupleBatchReceipt, Status> {
        let Some(target) = self.system_realm_target()? else {
            return self.coordinate_system_grant(request, true).await;
        };
        let receipt = self
            .peers
            .coordinate_system_grant(
                target.node_id,
                &target.address,
                &request,
                true,
                CONTROL_OPERATION_TIMEOUT,
            )
            .await?;
        self.require_same_system_realm_target(&target)?;
        Ok(receipt)
    }

    async fn apply_first_realm_binding(
        &self,
        stable_tenant_id: u64,
        request: &BindSchemaRequest,
        validate_only: bool,
    ) -> Result<Option<BoundRealm>, Status> {
        let Some(target) = self.realm_target(stable_tenant_id)? else {
            return self
                .coordinate_first_realm_binding(stable_tenant_id, request.clone(), validate_only)
                .await;
        };
        let result = self
            .peers
            .coordinate_first_realm_binding(
                target.node_id,
                &target.address,
                stable_tenant_id,
                request,
                validate_only,
                CONTROL_OPERATION_TIMEOUT,
            )
            .await?;
        self.require_same_realm_target(stable_tenant_id, &target)?;
        Ok(result)
    }
}

fn use_atomic_single_node_binding(active_node_count: usize) -> bool {
    active_node_count == 1
}

fn first_realm_binding_operation_id(
    request: &BindSchemaRequest,
    principal: &ObjectRef,
) -> Result<String, Status> {
    let fingerprint = serde_json::to_vec(&(
        "keldra.first-realm-binding.v1",
        &request.scope,
        &request.schema_ref,
        request.expected_generation,
        principal,
    ))
    .map_err(|error| {
        Status::internal(format!("first realm binding fingerprint failed: {error}"))
    })?;
    Ok(format!(
        "first-realm-binding-{}",
        hex::encode(blake3::hash(&fingerprint).as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use keldra_authz::RealmId;
    use keldra_store::{SchemaDigest, SchemaId, SchemaRef, StorageTenantId};

    use super::*;

    #[test]
    fn operation_identity_binds_principal_and_public_binding_input() {
        let request = BindSchemaRequest {
            scope: keldra_store::AuthzScope::new(
                StorageTenantId::parse("tenant").unwrap(),
                RealmId::parse("orders").unwrap(),
            )
            .unwrap(),
            schema_ref: SchemaRef {
                schema_id: SchemaId::parse("v1").unwrap(),
                schema_revision: 1,
                schema_digest: SchemaDigest([7; 32]),
            },
            expected_generation: Some(0),
            expected_revision: None,
        };
        let alice = ObjectRef::opaque("app", "alice").unwrap();
        let bob = ObjectRef::opaque("app", "bob").unwrap();
        let first = first_realm_binding_operation_id(&request, &alice).unwrap();
        assert_eq!(
            first,
            first_realm_binding_operation_id(&request, &alice).unwrap()
        );
        assert_ne!(
            first,
            first_realm_binding_operation_id(&request, &bob).unwrap()
        );
    }

    #[test]
    fn only_one_active_node_uses_the_atomic_local_binding() {
        assert!(use_atomic_single_node_binding(1));
        assert!(!use_atomic_single_node_binding(2));
        assert!(!use_atomic_single_node_binding(3));
    }
}
