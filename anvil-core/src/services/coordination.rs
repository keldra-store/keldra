use crate::anvil_api::coordination_service_server::CoordinationService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, partition_fence, permissions::AnvilAction, task_lease,
};
use anyhow::{Result, anyhow};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl CoordinationService for AppState {
    async fn acquire_task_lease(
        &self,
        request: Request<AcquireTaskLeaseRequest>,
    ) -> Result<Response<TaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::CoordinationLeaseWrite,
            &resource,
        )
        .await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let ttl_nanos =
            capped_public_ttl_nanos(req.requested_ttl_nanos, self.config.task_lease_ttl_secs)?;
        let owner = lease_owner_from_claims(&claims, &req.owner_label);
        let lease = self
            .persistence
            .acquire_named_task_lease(task_lease::TaskLeaseAcquire {
                task_id: req.task_id,
                task_kind: req.task_kind,
                partition_family: req.partition_family,
                partition_id: req.partition_id,
                owner,
                source_cursor: join_u128(req.source_cursor_low, req.source_cursor_high),
                now_nanos,
                ttl_nanos,
            })
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(TaskLeaseResponse {
            lease: Some(task_lease_response(lease)),
        }))
    }

    async fn checkpoint_task_lease(
        &self,
        request: Request<CheckpointTaskLeaseRequest>,
    ) -> Result<Response<TaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::CoordinationLeaseWrite,
            &resource,
        )
        .await?;

        let owner = lease_owner_from_claims(&claims, "");
        let expected = self
            .persistence
            .read_expected_named_task_lease(
                &owner,
                &req.task_id,
                req.fence_token,
                req.expected_root_generation,
                req.expected_lease_epoch,
                req.expected_expires_at_nanos,
                &req.expected_lease_hash,
            )
            .await
            .map_err(lease_error_status)?;
        let lease = self
            .persistence
            .checkpoint_named_task_lease(
                &expected,
                join_u128(req.checkpoint_cursor_low, req.checkpoint_cursor_high),
            )
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(TaskLeaseResponse {
            lease: Some(task_lease_response(lease)),
        }))
    }

    async fn commit_task_lease(
        &self,
        request: Request<CommitTaskLeaseRequest>,
    ) -> Result<Response<CommitTaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::CoordinationLeaseWrite,
            &resource,
        )
        .await?;

        let owner = lease_owner_from_claims(&claims, "");
        let expected = self
            .persistence
            .read_expected_named_task_lease(
                &owner,
                &req.task_id,
                req.fence_token,
                req.expected_root_generation,
                req.expected_lease_epoch,
                req.expected_expires_at_nanos,
                &req.expected_lease_hash,
            )
            .await
            .map_err(lease_error_status)?;
        let lease = self
            .persistence
            .commit_named_task_lease(
                &expected,
                join_u128(req.committed_cursor_low, req.committed_cursor_high),
            )
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(CommitTaskLeaseResponse {
            committed: true,
            previous_lease: Some(task_lease_response(lease)),
        }))
    }

    async fn read_task_lease(
        &self,
        request: Request<ReadTaskLeaseRequest>,
    ) -> Result<Response<ReadTaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::CoordinationLeaseRead,
            &resource,
        )
        .await?;

        let lease = self
            .persistence
            .read_named_task_lease(claims.tenant_id, &req.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ReadTaskLeaseResponse {
            found: lease.is_some(),
            lease: lease.map(task_lease_response),
        }))
    }

    async fn force_release_task_lease(
        &self,
        request: Request<ForceReleaseTaskLeaseRequest>,
    ) -> Result<Response<ForceReleaseTaskLeaseResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_task_lease_id(&req.task_id)?;
        let resource = task_lease_resource(&req.task_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::CoordinationLeaseAdmin,
            &resource,
        )
        .await?;

        let released = self
            .persistence
            .force_release_named_task_lease(claims.tenant_id, &req.task_id)
            .await
            .map_err(lease_error_status)?;

        Ok(Response::new(ForceReleaseTaskLeaseResponse {
            released: released.is_some(),
            previous_lease: released.map(task_lease_response),
        }))
    }

    async fn acquire_ownership(
        &self,
        request: Request<AcquireOwnershipRequest>,
    ) -> Result<Response<OwnershipFenceResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = ownership_resource_from_proto(req.resource)?;
        ensure_ownership_authorized(
            self,
            &claims,
            &resource,
            AnvilAction::CoordinationLeaseWrite,
        )
        .await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let ttl_nanos = ownership_ttl_nanos(req.requested_lease_ms)?;
        let signing_key = ownership_signing_key(self)?;
        let owner = ownership_owner_from_claims(
            &claims,
            &req.owner_label,
            &self.region,
            &self.config.cell_id,
        )?;
        let outcome = partition_fence::acquire_ownership(
            &self.storage,
            partition_fence::AcquireOwnership {
                request_id: req.request_id.clone(),
                idempotency_key: req.idempotency_key,
                resource,
                owner,
                now_nanos,
                ttl_nanos,
            },
            &signing_key,
        )
        .await
        .map_err(ownership_error_status)?;

        Ok(Response::new(ownership_fence_response(
            req.request_id,
            outcome,
        )))
    }

    async fn renew_ownership(
        &self,
        request: Request<RenewOwnershipRequest>,
    ) -> Result<Response<OwnershipFenceResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = ownership_resource_from_proto(req.resource)?;
        ensure_ownership_authorized(
            self,
            &claims,
            &resource,
            AnvilAction::CoordinationLeaseWrite,
        )
        .await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let ttl_nanos = ownership_ttl_nanos(req.requested_lease_ms)?;
        let signing_key = ownership_signing_key(self)?;
        let owner = ownership_owner_from_claims(
            &claims,
            &req.owner_label,
            &self.region,
            &self.config.cell_id,
        )?;
        let outcome = partition_fence::renew_ownership(
            &self.storage,
            partition_fence::RenewOwnership {
                request_id: req.request_id.clone(),
                resource,
                owner,
                current_fence: req.current_fence,
                now_nanos,
                ttl_nanos,
            },
            &signing_key,
        )
        .await
        .map_err(ownership_error_status)?;

        Ok(Response::new(ownership_fence_response(
            req.request_id,
            outcome,
        )))
    }

    async fn transfer_ownership(
        &self,
        request: Request<TransferOwnershipRequest>,
    ) -> Result<Response<OwnershipFenceResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = ownership_resource_from_proto(req.resource)?;
        ensure_ownership_authorized(
            self,
            &claims,
            &resource,
            AnvilAction::CoordinationLeaseWrite,
        )
        .await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let ttl_nanos = ownership_ttl_nanos(partition_fence::MAX_OWNERSHIP_LEASE_MS)?;
        let signing_key = ownership_signing_key(self)?;
        let owner = ownership_owner_from_claims(
            &claims,
            &req.owner_label,
            &self.region,
            &self.config.cell_id,
        )?;
        let new_owner = ownership_transfer_target_from_request(
            &claims,
            &req.new_owner_principal_kind,
            &req.new_owner_principal_id,
            &req.new_owner_actor_instance_id,
            &req.new_owner_label,
            &req.new_owner_region,
            &req.new_owner_cell,
            &self.region,
            &self.config.cell_id,
        )?;
        let outcome = partition_fence::transfer_ownership(
            &self.storage,
            partition_fence::TransferOwnership {
                request_id: req.request_id.clone(),
                idempotency_key: req.idempotency_key,
                resource,
                current_owner: owner,
                new_owner,
                current_fence: req.current_fence,
                now_nanos,
                ttl_nanos,
            },
            &signing_key,
        )
        .await
        .map_err(ownership_error_status)?;

        Ok(Response::new(ownership_fence_response(
            req.request_id,
            outcome,
        )))
    }

    async fn release_ownership(
        &self,
        request: Request<ReleaseOwnershipRequest>,
    ) -> Result<Response<OwnershipFenceResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = ownership_resource_from_proto(req.resource)?;
        let action = ownership_release_action(req.administrative_force);
        ensure_ownership_authorized(self, &claims, &resource, action).await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let signing_key = ownership_signing_key(self)?;
        let owner = ownership_owner_from_claims(
            &claims,
            &req.owner_label,
            &self.region,
            &self.config.cell_id,
        )?;
        let outcome = partition_fence::release_ownership(
            &self.storage,
            partition_fence::ReleaseOwnership {
                request_id: req.request_id.clone(),
                idempotency_key: req.idempotency_key,
                resource,
                owner,
                current_fence: req.current_fence,
                administrative_force: req.administrative_force,
                now_nanos,
            },
            &signing_key,
        )
        .await
        .map_err(ownership_error_status)?;

        Ok(Response::new(ownership_fence_response(
            req.request_id,
            outcome,
        )))
    }

    async fn force_expire_ownership(
        &self,
        request: Request<ForceExpireOwnershipRequest>,
    ) -> Result<Response<OwnershipFenceResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let resource = ownership_resource_from_proto(req.resource)?;
        ensure_ownership_authorized(
            self,
            &claims,
            &resource,
            AnvilAction::CoordinationLeaseAdmin,
        )
        .await?;

        let now_nanos = current_time_nanos().map_err(|e| Status::internal(e.to_string()))?;
        let signing_key = ownership_signing_key(self)?;
        let admin = ownership_owner_from_claims(&claims, "", &self.region, &self.config.cell_id)?;
        let outcome = partition_fence::force_expire_ownership(
            &self.storage,
            partition_fence::ForceExpireOwnership {
                request_id: req.request_id.clone(),
                idempotency_key: req.idempotency_key,
                resource,
                admin,
                reason: req.reason,
                now_nanos,
            },
            &signing_key,
        )
        .await
        .map_err(ownership_error_status)?;

        Ok(Response::new(ownership_fence_response(
            req.request_id,
            outcome,
        )))
    }
}

fn task_lease_response(lease: task_lease::TaskLease) -> TaskLease {
    let (source_cursor_low, source_cursor_high) = split_u128(lease.source_cursor);
    let (checkpoint_cursor_low, checkpoint_cursor_high) = split_u128(lease.checkpoint_cursor);
    let owner = lease.owner;
    TaskLease {
        task_id: lease.task_id,
        task_kind: lease.task_kind,
        partition_family: lease.partition_family,
        partition_id: lease.partition_id,
        owner_label: owner.display_name,
        fence_token: lease.fence_token,
        source_cursor_low,
        source_cursor_high,
        checkpoint_cursor_low,
        checkpoint_cursor_high,
        lease_epoch: lease.lease_epoch,
        acquired_at_nanos: lease.acquired_at_nanos,
        expires_at_nanos: lease.expires_at_nanos,
        updated_at_nanos: lease.updated_at_nanos,
        lease_hash: lease.lease_hash.unwrap_or_default(),
        lease_signature: lease.lease_signature.unwrap_or_default(),
        owner_tenant_id: owner.tenant_id,
        owner_principal_kind: owner.principal_kind,
        owner_principal_id: owner.principal_id,
        owner_actor_instance_id: owner.actor_instance_id,
        root_generation: lease.root_generation,
    }
}

fn lease_owner_from_claims(claims: &auth::Claims, owner_label: &str) -> task_lease::TaskLeaseOwner {
    let actor_instance_id = claims.jti.clone().unwrap_or_else(|| claims.sub.clone());
    task_lease::TaskLeaseOwner {
        tenant_id: claims.tenant_id,
        principal_kind: "app".to_string(),
        principal_id: claims.sub.clone(),
        actor_instance_id,
        display_name: if owner_label.is_empty() {
            claims.sub.clone()
        } else {
            owner_label.to_string()
        },
    }
}

fn ownership_owner_from_claims(
    claims: &auth::Claims,
    owner_label: &str,
    region: &str,
    cell: &str,
) -> Result<partition_fence::OwnershipPrincipal, Status> {
    let actor_instance_id = claims.jti.clone().unwrap_or_else(|| claims.sub.clone());
    let display_name = if owner_label.is_empty() {
        claims.sub.clone()
    } else {
        owner_label.to_string()
    };
    if display_name.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(Status::invalid_argument(
            "owner_label must not contain control characters",
        ));
    }
    Ok(partition_fence::OwnershipPrincipal {
        tenant_id: claims.tenant_id,
        principal_kind: "app".to_string(),
        principal_id: claims.sub.clone(),
        actor_instance_id,
        display_name,
        region: if region.is_empty() {
            "default".to_string()
        } else {
            region.to_string()
        },
        cell: if cell.is_empty() {
            "default".to_string()
        } else {
            cell.to_string()
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn ownership_transfer_target_from_request(
    claims: &auth::Claims,
    principal_kind: &str,
    principal_id: &str,
    actor_instance_id: &str,
    owner_label: &str,
    region: &str,
    cell: &str,
    default_region: &str,
    default_cell: &str,
) -> Result<partition_fence::OwnershipPrincipal, Status> {
    let principal_kind = principal_kind.trim();
    let principal_id = principal_id.trim();
    let actor_instance_id = actor_instance_id.trim();
    if principal_kind.is_empty() || principal_id.is_empty() || actor_instance_id.is_empty() {
        return Err(Status::invalid_argument(
            "transfer target principal kind, id, and actor instance id are required",
        ));
    }
    for (field, value) in [
        ("new_owner_principal_kind", principal_kind),
        ("new_owner_principal_id", principal_id),
        ("new_owner_actor_instance_id", actor_instance_id),
        ("new_owner_label", owner_label),
        ("new_owner_region", region),
        ("new_owner_cell", cell),
    ] {
        if value.chars().any(|ch| ch == '\0' || ch.is_control()) {
            return Err(Status::invalid_argument(format!(
                "{field} must not contain control characters"
            )));
        }
    }
    let display_name = if owner_label.trim().is_empty() {
        principal_id.to_string()
    } else {
        owner_label.to_string()
    };
    Ok(partition_fence::OwnershipPrincipal {
        tenant_id: claims.tenant_id,
        principal_kind: principal_kind.to_string(),
        principal_id: principal_id.to_string(),
        actor_instance_id: actor_instance_id.to_string(),
        display_name,
        region: if region.trim().is_empty() {
            default_region.to_string()
        } else {
            region.to_string()
        },
        cell: if cell.trim().is_empty() {
            default_cell.to_string()
        } else {
            cell.to_string()
        },
    })
}

fn ownership_resource_from_proto(
    resource: Option<OwnershipResource>,
) -> Result<partition_fence::OwnershipResource, Status> {
    let resource = resource.ok_or_else(|| Status::invalid_argument("resource is required"))?;
    let resource_kind =
        match crate::anvil_api::OwnershipResourceKind::try_from(resource.resource_kind)
            .map_err(|_| Status::invalid_argument("resource_kind is invalid"))?
        {
            crate::anvil_api::OwnershipResourceKind::ControlPartition => {
                partition_fence::OwnershipResourceKind::ControlPartition
            }
            crate::anvil_api::OwnershipResourceKind::BucketPrimary => {
                partition_fence::OwnershipResourceKind::BucketPrimary
            }
            crate::anvil_api::OwnershipResourceKind::ObjectPartition => {
                partition_fence::OwnershipResourceKind::ObjectPartition
            }
            crate::anvil_api::OwnershipResourceKind::IndexPartition => {
                partition_fence::OwnershipResourceKind::IndexPartition
            }
            crate::anvil_api::OwnershipResourceKind::PersonaldbGroup => {
                partition_fence::OwnershipResourceKind::PersonalDbGroup
            }
            crate::anvil_api::OwnershipResourceKind::TaskQueue => {
                partition_fence::OwnershipResourceKind::TaskQueue
            }
            crate::anvil_api::OwnershipResourceKind::WatchPartition => {
                partition_fence::OwnershipResourceKind::WatchPartition
            }
            crate::anvil_api::OwnershipResourceKind::Unspecified => {
                return Err(Status::invalid_argument("resource_kind is required"));
            }
        };
    validate_ownership_resource_id(&resource.resource_id)?;
    Ok(partition_fence::OwnershipResource {
        resource_kind,
        resource_id: resource.resource_id,
    })
}

fn ownership_resource_to_proto(resource: &partition_fence::OwnershipResource) -> OwnershipResource {
    OwnershipResource {
        resource_kind: match resource.resource_kind {
            partition_fence::OwnershipResourceKind::ControlPartition => {
                crate::anvil_api::OwnershipResourceKind::ControlPartition as i32
            }
            partition_fence::OwnershipResourceKind::BucketPrimary => {
                crate::anvil_api::OwnershipResourceKind::BucketPrimary as i32
            }
            partition_fence::OwnershipResourceKind::ObjectPartition => {
                crate::anvil_api::OwnershipResourceKind::ObjectPartition as i32
            }
            partition_fence::OwnershipResourceKind::IndexPartition => {
                crate::anvil_api::OwnershipResourceKind::IndexPartition as i32
            }
            partition_fence::OwnershipResourceKind::PersonalDbGroup => {
                crate::anvil_api::OwnershipResourceKind::PersonaldbGroup as i32
            }
            partition_fence::OwnershipResourceKind::TaskQueue => {
                crate::anvil_api::OwnershipResourceKind::TaskQueue as i32
            }
            partition_fence::OwnershipResourceKind::WatchPartition => {
                crate::anvil_api::OwnershipResourceKind::WatchPartition as i32
            }
        },
        resource_id: resource.resource_id.clone(),
    }
}

fn ownership_state_to_proto(state: partition_fence::OwnershipFenceState) -> i32 {
    match state {
        partition_fence::OwnershipFenceState::Active => {
            crate::anvil_api::OwnershipFenceState::Active as i32
        }
        partition_fence::OwnershipFenceState::Transferring => {
            crate::anvil_api::OwnershipFenceState::Transferring as i32
        }
        partition_fence::OwnershipFenceState::Draining => {
            crate::anvil_api::OwnershipFenceState::Draining as i32
        }
        partition_fence::OwnershipFenceState::Expired => {
            crate::anvil_api::OwnershipFenceState::Expired as i32
        }
        partition_fence::OwnershipFenceState::Released => {
            crate::anvil_api::OwnershipFenceState::Released as i32
        }
    }
}

fn ownership_fence_response(
    request_id: String,
    outcome: partition_fence::OwnershipFenceOutcome,
) -> OwnershipFenceResponse {
    let record = outcome.record;
    let owner = record.owner;
    OwnershipFenceResponse {
        request_id,
        resource: Some(ownership_resource_to_proto(&record.resource)),
        owner_node_id: owner.display_name,
        owner_region: owner.region,
        owner_cell: owner.cell,
        fence: record.fence,
        state: ownership_state_to_proto(record.state),
        lease_expires_at: rfc3339_from_nanos(record.lease_expires_at_nanos),
        generation: record.generation,
        idempotent_replay: outcome.idempotent_replay,
        owner_tenant_id: owner.tenant_id,
        owner_principal_kind: owner.principal_kind,
        owner_principal_id: owner.principal_id,
        owner_actor_instance_id: owner.actor_instance_id,
    }
}

fn ownership_ttl_nanos(requested_lease_ms: u64) -> Result<i64, Status> {
    let max_ms = partition_fence::MAX_OWNERSHIP_LEASE_MS;
    let lease_ms = if requested_lease_ms == 0 {
        max_ms
    } else if requested_lease_ms > max_ms {
        return Err(Status::invalid_argument(
            "requested_lease_ms exceeds max ownership lease",
        ));
    } else {
        requested_lease_ms
    };
    let nanos = lease_ms
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| Status::internal("ownership lease ttl exceeds supported range"))?;
    Ok(nanos)
}

fn ownership_signing_key(state: &AppState) -> Result<Vec<u8>, Status> {
    hex::decode(&state.config.anvil_secret_encryption_key)
        .map_err(|_| Status::internal("ownership signing key is invalid"))
}

fn ownership_release_action(administrative_force: bool) -> AnvilAction {
    if administrative_force {
        AnvilAction::CoordinationLeaseAdmin
    } else {
        AnvilAction::CoordinationLeaseWrite
    }
}

async fn ensure_ownership_authorized(
    state: &AppState,
    claims: &auth::Claims,
    resource: &partition_fence::OwnershipResource,
    action: AnvilAction,
) -> Result<(), Status> {
    let auth_resource = ownership_auth_resource(claims.tenant_id, resource);
    access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        action,
        &auth_resource,
    )
    .await
}

fn ownership_auth_resource(
    tenant_id: i64,
    resource: &partition_fence::OwnershipResource,
) -> String {
    format!(
        "ownership/tenant-{}/{}/{}",
        tenant_id,
        resource.resource_kind.as_str(),
        resource.resource_id
    )
}

fn validate_ownership_resource_id(value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument("resource_id must not be empty"));
    }
    if value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(Status::invalid_argument(
            "resource_id must not contain control characters",
        ));
    }
    Ok(())
}

fn ownership_error_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains(partition_fence::OWNERSHIP_HELD) {
        Status::failed_precondition(partition_fence::OWNERSHIP_HELD)
    } else if message.contains(partition_fence::OWNERSHIP_EXPIRED) {
        Status::failed_precondition(partition_fence::OWNERSHIP_EXPIRED)
    } else if message.contains(partition_fence::OWNERSHIP_NOT_FOUND) {
        Status::not_found(partition_fence::OWNERSHIP_NOT_FOUND)
    } else if message.contains(partition_fence::OWNERSHIP_OWNER_MISMATCH) {
        Status::permission_denied(partition_fence::OWNERSHIP_OWNER_MISMATCH)
    } else if message.contains(partition_fence::OWNERSHIP_STALE_FENCE) {
        Status::failed_precondition(partition_fence::OWNERSHIP_STALE_FENCE)
    } else if message.contains(partition_fence::OWNERSHIP_CAS_CONFLICT) {
        Status::aborted(partition_fence::OWNERSHIP_CAS_CONFLICT)
    } else {
        Status::failed_precondition(message)
    }
}

fn rfc3339_from_nanos(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let sub_nanos = nanos.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub_nanos)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}

fn capped_public_ttl_nanos(requested_ttl_nanos: i64, max_ttl_secs: u64) -> Result<i64, Status> {
    let max_secs = if max_ttl_secs == 0 { 300 } else { max_ttl_secs };
    let max_nanos = i64::try_from(max_secs)
        .ok()
        .and_then(|secs| secs.checked_mul(1_000_000_000))
        .ok_or_else(|| Status::internal("task lease ttl cap exceeds supported range"))?;
    if requested_ttl_nanos <= 0 {
        return Ok(max_nanos);
    }
    Ok(requested_ttl_nanos.min(max_nanos))
}

fn lease_error_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains(task_lease::LEASE_HELD) {
        Status::failed_precondition(task_lease::LEASE_HELD)
    } else if message.contains(task_lease::LEASE_EXPIRED) {
        Status::failed_precondition(task_lease::LEASE_EXPIRED)
    } else if message.contains(task_lease::STALE_FENCE) {
        Status::failed_precondition(task_lease::STALE_FENCE)
    } else if message.contains(task_lease::LEASE_OWNER_MISMATCH) {
        Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH)
    } else if message.contains(task_lease::LEASE_CAS_CONFLICT) {
        Status::aborted(task_lease::LEASE_CAS_CONFLICT)
    } else {
        Status::failed_precondition(message)
    }
}

fn task_lease_resource(task_id: &str) -> String {
    format!("task_lease/{task_id}")
}

fn validate_task_lease_id(value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument("task_id must not be empty"));
    }
    if value == "." || value == ".." || value.contains('/') || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(
            "task_id must be a safe path component",
        ));
    }
    Ok(())
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn ownership_ttl_above_cap_is_rejected() {
        assert_eq!(
            ownership_ttl_nanos(partition_fence::MAX_OWNERSHIP_LEASE_MS + 1)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            ownership_ttl_nanos(partition_fence::MAX_OWNERSHIP_LEASE_MS).unwrap(),
            (partition_fence::MAX_OWNERSHIP_LEASE_MS as i64) * 1_000_000
        );
    }

    #[test]
    fn ownership_auth_resource_is_bound_to_authenticated_tenant() {
        let resource = ownership_resource();
        assert_eq!(
            ownership_auth_resource(1, &resource),
            "ownership/tenant-1/bucket_primary/tenant-acme/releases"
        );
        assert_eq!(
            ownership_auth_resource(2, &resource),
            "ownership/tenant-2/bucket_primary/tenant-acme/releases"
        );
    }

    #[test]
    fn force_release_and_expire_require_admin_action() {
        assert_eq!(
            ownership_release_action(false),
            AnvilAction::CoordinationLeaseWrite
        );
        assert_eq!(
            ownership_release_action(true),
            AnvilAction::CoordinationLeaseAdmin
        );
    }

    #[test]
    fn owner_label_does_not_replace_claim_identity() {
        let claims = claims(7);
        let owner =
            ownership_owner_from_claims(&claims, "node-shared", "eu-west-1", "cell-a").unwrap();
        assert_eq!(owner.tenant_id, 7);
        assert_eq!(owner.principal_id, "app-a");
        assert_eq!(owner.actor_instance_id, "token-a");
        assert_eq!(owner.display_name, "node-shared");
    }

    fn ownership_resource() -> partition_fence::OwnershipResource {
        partition_fence::OwnershipResource {
            resource_kind: partition_fence::OwnershipResourceKind::BucketPrimary,
            resource_id: "tenant-acme/releases".to_string(),
        }
    }

    fn claims(tenant_id: i64) -> auth::Claims {
        auth::Claims {
            sub: "app-a".to_string(),
            exp: usize::MAX,
            tenant_id,
            jti: Some("token-a".to_string()),
        }
    }
}
