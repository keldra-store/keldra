use super::*;

pub(super) async fn create_object_link(
    state: &AppState,
    request: Request<CreateObjectLinkRequest>,
) -> Result<Response<ObjectLinkResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    let context = public_link_context(req.context.as_ref(), true)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    require_object_link_scope(
        state,
        &claims,
        &req.bucket_name,
        &req.link_key,
        AnvilAction::ObjectWrite,
    )
    .await?;
    let bucket = public_link_bucket(state, &claims, &req.bucket_name).await?;
    let resolution = object_link_resolution_from_proto(req.resolution)?;
    let target_version = parse_optional_uuid("target_version", req.target_version)?;
    let mutation = state
        .persistence
        .put_object_link(object_links::PutObjectLinkRequest {
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            link_key: req.link_key,
            target_key: req.target_key,
            target_version,
            resolution,
            expected_generation: None,
            create_only: true,
            allow_dangling: req.allow_dangling,
            idempotency_key: context.idempotency_key.clone(),
            created_by: format!("app:{}", claims.sub),
            transaction_id: transaction_id.map(ToOwned::to_owned),
            transaction_principal: transaction_principal.clone(),
        })
        .await
        .map_err(object_link_status)?;
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, mutation.descriptor.link_key),
            "object_link.create",
            serde_json::json!({
                "target_key": mutation.descriptor.target_key.clone(),
                "generation": mutation.descriptor.generation
            }),
        )
        .await?
    };

    Ok(Response::new(ObjectLinkResponse {
        request_id: context.request_id.clone(),
        link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
        audit_event_id,
    }))
}
pub(super) async fn update_object_link(
    state: &AppState,
    request: Request<UpdateObjectLinkRequest>,
) -> Result<Response<ObjectLinkResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    let context = public_link_context(req.context.as_ref(), false)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    require_object_link_scope(
        state,
        &claims,
        &req.bucket_name,
        &req.link_key,
        AnvilAction::ObjectWrite,
    )
    .await?;
    let bucket = public_link_bucket(state, &claims, &req.bucket_name).await?;
    let resolution = object_link_resolution_from_proto(req.resolution)?;
    let target_version = parse_optional_uuid("target_version", req.target_version)?;
    let mutation = state
        .persistence
        .put_object_link(object_links::PutObjectLinkRequest {
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            link_key: req.link_key,
            target_key: req.target_key,
            target_version,
            resolution,
            expected_generation: Some(context.expected_generation),
            create_only: false,
            allow_dangling: req.allow_dangling,
            idempotency_key: context.idempotency_key.clone(),
            created_by: format!("app:{}", claims.sub),
            transaction_id: transaction_id.map(ToOwned::to_owned),
            transaction_principal: transaction_principal.clone(),
        })
        .await
        .map_err(object_link_status)?;
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, mutation.descriptor.link_key),
            "object_link.update",
            serde_json::json!({
                "target_key": mutation.descriptor.target_key.clone(),
                "generation": mutation.descriptor.generation
            }),
        )
        .await?
    };

    Ok(Response::new(ObjectLinkResponse {
        request_id: context.request_id.clone(),
        link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
        audit_event_id,
    }))
}
pub(super) async fn delete_object_link(
    state: &AppState,
    request: Request<DeleteObjectLinkRequest>,
) -> Result<Response<MutationResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    let context = public_link_context(req.context.as_ref(), false)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    require_object_link_scope(
        state,
        &claims,
        &req.bucket_name,
        &req.link_key,
        AnvilAction::ObjectDelete,
    )
    .await?;
    let bucket = public_link_bucket(state, &claims, &req.bucket_name).await?;
    let deleted = state
        .persistence
        .delete_object_link(object_links::DeleteObjectLinkRequest {
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            link_key: req.link_key,
            expected_generation: context.expected_generation,
            idempotency_key: context.idempotency_key.clone(),
            transaction_id: transaction_id.map(ToOwned::to_owned),
            transaction_principal: transaction_principal.clone(),
        })
        .await
        .map_err(object_link_status)?;
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, deleted.link_key),
            "object_link.delete",
            serde_json::json!({ "generation": deleted.generation }),
        )
        .await?
    };

    Ok(Response::new(MutationResponse {
        request_id: context.request_id.clone(),
        resource_id: deleted.link_key,
        generation: deleted.generation,
        audit_event_id,
        idempotent_replay: false,
    }))
}
pub(super) async fn read_object_link(
    state: &AppState,
    request: Request<ReadObjectLinkRequest>,
) -> Result<Response<ObjectLinkResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    require_object_link_scope(
        state,
        &claims,
        &req.bucket_name,
        &req.link_key,
        AnvilAction::ObjectRead,
    )
    .await?;
    let consistency = object_read_consistency(req.consistency.as_ref())?;
    let descriptor = state
        .object_manager
        .read_object_link_for_tenant(
            Some(claims.clone()),
            Some(claims.tenant_id),
            &req.bucket_name,
            &req.link_key,
            None,
            consistency,
        )
        .await
        .map_err(|status| status)?;

    Ok(Response::new(ObjectLinkResponse {
        request_id: req.request_id,
        link: Some(object_link_descriptor_to_proto(descriptor)),
        audit_event_id: String::new(),
    }))
}
pub(super) async fn list_object_links(
    state: &AppState,
    request: Request<ListObjectLinksRequest>,
) -> Result<Response<ListObjectLinksResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    let bucket = public_link_bucket(state, &claims, &req.bucket_name).await?;
    let _consistency = object_read_consistency(req.consistency.as_ref())?;
    crate::access_control::require_action(
        &state.storage,
        &state.persistence,
        &claims,
        AnvilAction::ObjectList,
        &format!("{}/{}", bucket.name, req.prefix),
    )
    .await?;
    let links = state
        .persistence
        .list_object_links(bucket.id, Some(&req.prefix))
        .await
        .map_err(object_link_status)?;
    let mut authorized_links = Vec::new();
    for link in links {
        if crate::access_control::action_allows(
            &state.storage,
            &state.persistence,
            &claims,
            AnvilAction::ObjectRead,
            &format!("{}/{}", bucket.name, link.link_key),
        )
        .await?
        {
            authorized_links.push(link);
        }
    }
    let links = authorized_links
        .into_iter()
        .map(object_link_descriptor_to_proto)
        .collect::<Vec<_>>();
    let filters = [
        ("tenant_id", req.tenant_id.as_str()),
        ("bucket_name", req.bucket_name.as_str()),
        ("prefix", req.prefix.as_str()),
    ];
    let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
    let (links, page) = crate::services::collection_cursor::paginate(
        links,
        req.page.as_ref(),
        "anvil.ObjectService/ListObjectLinks",
        &filters,
        &principal_scope,
        "link_key.asc",
        state.config.jwt_secret.as_bytes(),
        |link| link.link_key.as_str(),
        |link| link.generation,
    )?;

    Ok(Response::new(ListObjectLinksResponse {
        page: Some(page),
        links,
    }))
}
pub(super) async fn create_host_alias(
    state: &AppState,
    request: Request<CreateHostAliasRequest>,
) -> Result<Response<HostAliasResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_public_tenant_locator(&claims, &req.tenant_id)?;
    let context = public_link_context(req.context.as_ref(), true)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    let bucket = public_host_alias_bucket(state, &claims, &req.bucket_name).await?;
    require_bucket_scope(state, &claims, &bucket.name, AnvilAction::BucketWrite).await?;

    let region = if req.region.trim().is_empty() {
        bucket.region.clone()
    } else {
        req.region
    };
    let routing_config = public_routing_config_for_region(state, &region).await?;
    let input = CreateHostAliasDescriptor {
        hostname: req.hostname,
        tenant_id: claims.tenant_id.to_string(),
        bucket_name: bucket.name,
        region,
        prefix: req.prefix,
    };
    let host_alias = if let (Some(transaction_id), Some(principal)) =
        (transaction_id, transaction_principal.as_deref())
    {
        state
            .persistence
            .create_host_alias_descriptor_in_transaction(
                &routing_config,
                input,
                transaction_id,
                principal,
            )
            .await
            .map_err(lifecycle_status)?
    } else {
        state
            .persistence
            .create_host_alias_descriptor(&routing_config, input)
            .await
            .map_err(lifecycle_status)?
    };
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.create",
            serde_json::json!({
                "bucket_name": host_alias.bucket_name.clone(),
                "region": host_alias.region.clone(),
                "prefix": host_alias.prefix.clone()
            }),
        )
        .await?
    };

    Ok(Response::new(HostAliasResponse {
        request_id: context.request_id.clone(),
        host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
        audit_event_id,
    }))
}
pub(super) async fn verify_host_alias(
    state: &AppState,
    request: Request<VerifyHostAliasRequest>,
) -> Result<Response<HostAliasResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let context = public_link_context(req.context.as_ref(), false)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    let current = public_host_alias_descriptor(state, &claims, &req.hostname).await?;
    require_bucket_scope(
        state,
        &claims,
        &current.bucket_name,
        AnvilAction::BucketWrite,
    )
    .await?;
    let expected_challenge = host_alias_verification_challenge(&current);
    if req.observed_challenge.trim() != expected_challenge {
        return Err(Status::failed_precondition(
            "Host alias verification challenge did not match",
        ));
    }
    let host_alias = if let (Some(transaction_id), Some(principal)) =
        (transaction_id, transaction_principal.as_deref())
    {
        state
            .persistence
            .transition_host_alias_descriptor_in_transaction(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Active,
                transaction_id,
                principal,
            )
            .await
            .map_err(lifecycle_status)?
    } else {
        state
            .persistence
            .transition_host_alias_descriptor(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Active,
            )
            .await
            .map_err(lifecycle_status)?
    };
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.verify",
            serde_json::json!({ "generation": host_alias.generation }),
        )
        .await?
    };

    Ok(Response::new(HostAliasResponse {
        request_id: context.request_id.clone(),
        host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
        audit_event_id,
    }))
}
pub(super) async fn delete_host_alias(
    state: &AppState,
    request: Request<DeleteHostAliasRequest>,
) -> Result<Response<MutationResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let context = public_link_context(req.context.as_ref(), false)?;
    let transaction_id = public_context_transaction_id(context)?;
    let transaction_principal =
        transaction_id.map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
    let current = public_host_alias_descriptor(state, &claims, &req.hostname).await?;
    require_bucket_scope(
        state,
        &claims,
        &current.bucket_name,
        AnvilAction::BucketWrite,
    )
    .await?;
    let host_alias = if let (Some(transaction_id), Some(principal)) =
        (transaction_id, transaction_principal.as_deref())
    {
        state
            .persistence
            .transition_host_alias_descriptor_in_transaction(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Deleted,
                transaction_id,
                principal,
            )
            .await
            .map_err(lifecycle_status)?
    } else {
        state
            .persistence
            .transition_host_alias_descriptor(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Deleted,
            )
            .await
            .map_err(lifecycle_status)?
    };
    let audit_event_id = if transaction_id.is_some() {
        String::new()
    } else {
        crate::services::audit::record_tenant_audit_event(
            state,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.delete",
            serde_json::json!({ "generation": host_alias.generation }),
        )
        .await?
    };

    Ok(Response::new(MutationResponse {
        request_id: context.request_id.clone(),
        resource_id: host_alias.hostname,
        generation: host_alias.generation,
        audit_event_id,
        idempotent_replay: false,
    }))
}
pub(super) async fn read_host_alias(
    state: &AppState,
    request: Request<ReadHostAliasRequest>,
) -> Result<Response<HostAliasResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let host_alias = public_host_alias_descriptor(state, &claims, &req.hostname).await?;
    require_bucket_scope(
        state,
        &claims,
        &host_alias.bucket_name,
        AnvilAction::BucketRead,
    )
    .await?;

    Ok(Response::new(HostAliasResponse {
        request_id: req.request_id,
        host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
        audit_event_id: String::new(),
    }))
}
pub(super) async fn list_host_aliases(
    state: &AppState,
    request: Request<ListHostAliasesRequest>,
) -> Result<Response<ListHostAliasesResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let tenant_id = claims.tenant_id.to_string();
    let aliases = state
        .persistence
        .list_host_alias_descriptors(none_if_empty(&req.region))
        .await
        .map_err(lifecycle_status)?;
    let mut host_aliases = Vec::new();
    for alias in aliases
        .into_iter()
        .filter(|alias| alias.tenant_id == tenant_id)
    {
        if crate::access_control::action_allows(
            &state.storage,
            &state.persistence,
            &claims,
            AnvilAction::BucketRead,
            &alias.bucket_name,
        )
        .await?
        {
            host_aliases.push(alias);
        }
    }
    let host_aliases = host_aliases
        .into_iter()
        .map(host_alias_descriptor_to_proto)
        .collect::<Vec<_>>();
    let filters = [("region", req.region.as_str())];
    let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
    let (host_aliases, page) = crate::services::collection_cursor::paginate(
        host_aliases,
        req.page.as_ref(),
        "anvil.ObjectService/ListHostAliases",
        &filters,
        &principal_scope,
        "hostname.asc",
        state.config.jwt_secret.as_bytes(),
        |alias| alias.hostname.as_str(),
        |alias| alias.generation,
    )?;

    Ok(Response::new(ListHostAliasesResponse {
        page: Some(page),
        host_aliases,
    }))
}
