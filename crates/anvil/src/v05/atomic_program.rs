use std::time::{Duration, UNIX_EPOCH};

use anvil_api::v1::{
    InvokeProgramRequest, InvokeProgramResponse, ObjectAddress, ProgramPathReceipt,
};
use tonic::{Request, Response, Status};

use super::*;

pub(super) async fn invoke(
    service: &ObjectServiceImpl,
    request: Request<InvokeProgramRequest>,
) -> Result<Response<InvokeProgramResponse>, Status> {
    let peer_routed = request
        .extensions()
        .get::<routed_writes::RoutedDestination>()
        .is_some();
    let deadline = tokio::time::Instant::now()
        .checked_add(effective_atomic_program_timeout(
            request.metadata(),
            service.atomic_program_timeout,
        ))
        .ok_or_else(|| Status::internal("configured atomic program timeout exceeds clock"))?;
    let caller = authenticated_caller(&request)?;
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
                    let authorization = authorization.clone();
                    let governance = governance.clone();
                    let caller = dependency_caller.clone();
                    async move {
                        authorize_program_dependencies_authoritatively(
                            &authorization,
                            &governance,
                            &caller,
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
                |dependency| authorize_program_dependency(&authorization, &caller, dependency),
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
