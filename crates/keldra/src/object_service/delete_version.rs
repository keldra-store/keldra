use super::*;

pub(super) async fn delete_version(
    service: &ObjectServiceImpl,
    request: Request<DeleteVersionRequest>,
) -> Result<Response<DeleteVersionResponse>, Status> {
    let plugin_scope = plugin_object_scope(&request);
    let routed_alias = request
        .extensions()
        .get::<routed_writes::DeleteVersionOriginalAlias>()
        .map(|alias| alias.0.clone());
    let peer_routed = routed_writes::is_routed(&request);
    let caller = authenticated_caller(&request)?;
    let path_access = object_path_access::access_for(&request);
    let bearer = OriginalBearer::from_metadata(request.metadata())?;
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let mut api_request = request.into_inner();
    let durability = durability(api_request.durability)?;
    let routed_key = object_key(api_request.address.clone())?;
    let requested_key = routed_alias.as_ref().unwrap_or(&routed_key);
    object_path_access::require_key(&path_access, requested_key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), requested_key)?;
    let resolution = object_link::resolve_current(service, requested_key.clone()).await?;
    let linked = matches!(resolution, object_link::ResolvedAddress::Link(_));
    let key = resolution.canonical().clone();
    object_path_access::require_key(&path_access, &key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &key)?;
    if routed_alias.is_some() && key != routed_key {
        return Err(Status::failed_precondition(
            "routed DeleteVersion alias no longer resolves to its canonical target",
        ));
    }
    service
        .authorize_object(&caller, &key, ObjectPermission::Delete)
        .await?;
    object_link::require_public_version(service, &key, VersionId(api_request.version)).await?;
    let (current, _) = service.reader.head_with_program_cursor(&key).await?;
    if current.is_some_and(|version| version.id == VersionId(api_request.version)) {
        if linked {
            return Err(Status::failed_precondition(
                "the current target version cannot be deleted while the object link exists",
            ));
        }
        object_link::require_no_inbound_links(service, &key).await?;
    }
    let governance = service
        .bucket_governance
        .resolve(key.tenant(), key.bucket())
        .await?;
    require_governance_versioning_enabled(&governance)?;
    service
        .distribution
        .wait_for_durability_available(durability, deadline)
        .await?;
    let original_alias = linked.then(|| api_address(requested_key));
    api_request.address = Some(api_address(&key));
    let outcome = match service.distribution.routing_target_stable(
        &key,
        governance.tenant_id,
        governance.bucket_id,
    )? {
        Some(_) if peer_routed => {
            return Err(Status::failed_precondition(
                "a routed DeleteVersion reached a node that is not its coordinator",
            ));
        }
        Some((target, address)) => {
            return service
                .cluster_peers
                .route_delete_version(
                    target,
                    &address,
                    bearer.signed_token(),
                    api_request,
                    original_alias,
                    deadline_remaining(deadline)?,
                )
                .await
                .map(Response::new);
        }
        None => {
            run_request_until(
                deadline,
                service
                    .distribution
                    .delete_retained_version_with_governance(
                        &key,
                        VersionId(api_request.version),
                        governance,
                    ),
                "delete version deadline exceeded",
            )
            .await?
        }
    };
    Ok(Response::new(api_delete_version_outcome(outcome)))
}
