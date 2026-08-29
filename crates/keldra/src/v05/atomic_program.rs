use std::time::{Duration, UNIX_EPOCH};

use keldra_api::v1::{
    InvokeProgramRequest, InvokeProgramResponse, ObjectAddress, ProgramPathReceipt,
};
use tonic::{Request, Response, Status};

use super::*;
use crate::authentication::PluginObjectScope;

pub(super) async fn invoke(
    service: &ObjectServiceImpl,
    request: Request<InvokeProgramRequest>,
) -> Result<Response<InvokeProgramResponse>, Status> {
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let deadline = tokio::time::Instant::now()
        .checked_add(effective_request_timeout(
            request.metadata(),
            service.atomic_program_timeout,
        ))
        .ok_or_else(|| Status::internal("configured atomic program timeout exceeds clock"))?;
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let plugin_scope = plugin_object_scope(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let api_request = request.into_inner();
    let durability = durability(api_request.durability)?;
    let program_address = api_request
        .program
        .clone()
        .ok_or_else(|| Status::invalid_argument("program address is required"))?;
    let program = object_key(Some(program_address.clone()))?;
    object_path_access::require_public_key(&program)?;
    let expected_program_hash = required_hash(&api_request.program_hash, "program_hash")?;
    require_caller_tenant(&caller, &program)?;
    require_authorized(
        service
            .authoritative_system
            .allows_object(&caller, &program, ObjectPermission::Get)
            .await?,
        "program definition read is not authorized",
    )?;

    let clustered = service.programs.is_clustered()?;
    if clustered {
        if let Some((target, address)) = service.programs.executor_routing_target()? {
            if peer_routed {
                return Err(Status::failed_precondition(
                    "a routed InvokeProgram reached a node that is not the nominated executor",
                ));
            }
            return service
                .cluster_peers
                .route_invoke_program(
                    target,
                    &address,
                    bearer.signed_token(),
                    api_request,
                    deadline_remaining(deadline)?,
                )
                .await
                .map(Response::new);
        }
    }

    let invocation_id = api_request.invocation_id.clone();
    let result = if clustered {
        let authorization = service.authoritative_system.clone();
        let governance = service.bucket_governance.clone();
        let dependency_caller = caller.clone();
        let logical_caller = caller.clone();
        let logical_access = path_access.clone();
        let logical_scope = plugin_scope.clone();
        let canonical_access = path_access.clone();
        let canonical_scope = plugin_scope.clone();
        run_atomic_program_until(
            deadline,
            service.programs.invoke_distributed(
                program,
                expected_program_hash,
                api_request.invocation_id,
                &api_request.input_json,
                durability_name(durability),
                deadline_remaining(deadline)?,
                move |dependencies| {
                    let caller = logical_caller.clone();
                    let access = logical_access.clone();
                    let scope = logical_scope.clone();
                    async move {
                        for dependency in dependencies {
                            authorize_program_dependency_capability(
                                &caller,
                                &access,
                                scope.as_ref(),
                                &dependency,
                            )?;
                        }
                        Ok(())
                    }
                },
                move |dependencies| {
                    let authorization = authorization.clone();
                    let governance = governance.clone();
                    let caller = dependency_caller.clone();
                    let access = canonical_access.clone();
                    let scope = canonical_scope.clone();
                    async move {
                        authorize_program_dependencies_authoritatively(
                            &authorization,
                            &governance,
                            &caller,
                            &access,
                            scope.as_ref(),
                            dependencies,
                        )
                        .await
                    }
                },
            ),
        )
        .await?
    } else {
        let authorization = service.system_authorization().await?;
        run_atomic_program_until(
            deadline,
            service.programs.invoke(
                program,
                expected_program_hash,
                api_request.invocation_id,
                &api_request.input_json,
                durability_name(durability),
                |dependency| {
                    authorize_program_dependency_capability(
                        &caller,
                        &path_access,
                        plugin_scope.as_ref(),
                        dependency,
                    )
                    .map(|_| ())
                },
                |dependency| {
                    authorize_program_dependency(
                        &authorization,
                        &caller,
                        &path_access,
                        plugin_scope.as_ref(),
                        dependency,
                    )
                },
            ),
        )
        .await?
    };
    let mut path_receipts = Vec::with_capacity(result.published_versions.len());
    for (path, published) in result.published_versions {
        path_receipts.push(ProgramPathReceipt {
            address: Some(ObjectAddress {
                tenant: path.tenant,
                bucket: path.bucket,
                path: path.path,
            }),
            version: published.version.0,
            deleted: published.deleted,
        });
    }
    let output_json = serde_json::to_vec(&result.receipt.outputs)
        .map_err(|error| internal(format!("encode atomic program output: {error}")))?;
    let replay_expiration = UNIX_EPOCH
        .checked_add(Duration::from_millis(
            result.replay_guarantee_expires_at_unix_millis,
        ))
        .ok_or_else(|| Status::internal("atomic replay receipt expiry is out of range"))?;
    Ok(Response::new(InvokeProgramResponse {
        invocation_id,
        program: Some(program_address),
        program_hash: result.program_hash.to_vec(),
        executor_nomination_log_index: result.executor_nomination_log_index,
        commit_log_index: result.commit_log_index,
        path_receipts,
        output_json,
        replayed: result.replayed,
        replay_guarantee_expires_at: Some(replay_expiration.into()),
    }))
}

fn authorize_program_dependency(
    authorization: &SystemAuthorization,
    caller: &Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&PluginObjectScope>,
    dependency: &ExpandedProgramPath,
) -> Result<(), Status> {
    let key =
        authorize_program_dependency_capability(caller, path_access, plugin_scope, dependency)?;
    for (required, permission, message) in [
        (
            dependency.intent.get,
            ObjectPermission::Get,
            "atomic program dependency read is not authorized",
        ),
        (
            dependency.intent.put,
            ObjectPermission::Put,
            "atomic program dependency put is not authorized",
        ),
        (
            dependency.intent.delete,
            ObjectPermission::Delete,
            "atomic program dependency delete is not authorized",
        ),
    ] {
        if required {
            require_authorized(
                authorization
                    .allows_object(caller.subject(), &key, permission)
                    .map_err(crate::authz_api::authz_status)?,
                message,
            )?;
        }
    }
    Ok(())
}

fn authorize_program_dependency_capability(
    caller: &Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&PluginObjectScope>,
    dependency: &ExpandedProgramPath,
) -> Result<ObjectKey, Status> {
    let key = ObjectKey::new(
        &dependency.path.tenant,
        &dependency.path.bucket,
        &dependency.path.path,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    object_path_access::require_key(path_access, &key)?;
    require_plugin_key_scope(plugin_scope, &key)?;
    require_caller_tenant(caller, &key)?;
    Ok(key)
}

async fn authorize_program_dependencies_authoritatively(
    authorization: &AuthoritativeSystemAuthorization,
    _governance: &BucketGovernance,
    caller: &Caller,
    path_access: &object_path_access::ObjectPathAccess,
    plugin_scope: Option<&PluginObjectScope>,
    dependencies: Vec<ExpandedProgramPath>,
) -> Result<(), Status> {
    let mut requests = Vec::new();
    for dependency in dependencies {
        let key = authorize_program_dependency_capability(
            caller,
            path_access,
            plugin_scope,
            &dependency,
        )?;
        for (required, permission) in [
            (dependency.intent.get, ObjectPermission::Get),
            (dependency.intent.put, ObjectPermission::Put),
            (dependency.intent.delete, ObjectPermission::Delete),
        ] {
            if required {
                requests.push((key.clone(), permission));
            }
        }
    }
    let allowed = authorization.allows_objects(caller, &requests).await?;
    if allowed.into_iter().all(|allowed| allowed) {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "atomic program dependency is not authorized",
        ))
    }
}
