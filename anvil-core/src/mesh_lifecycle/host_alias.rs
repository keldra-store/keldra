use super::*;

pub async fn create_host_alias(
    storage: &Storage,
    config: &RoutingConfig,
    input: CreateHostAliasDescriptor,
) -> LifecycleResult<HostAliasDescriptor> {
    require_identifier(&input.tenant_id, "tenant id")?;
    require_identifier(&input.bucket_name, "bucket name")?;
    require_identifier(&input.region, "region")?;
    let hostname = routing::normalize_alias_hostname(&input.hostname)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;

    let mut state = read_state(storage).await?;
    match state.regions.get(&input.region) {
        Some(region) if region.state == LifecycleState::Active => {}
        Some(_) => {
            return Err(LifecycleError::InvalidArgument(
                "host alias region must be active".to_string(),
            ));
        }
        None => {
            return Err(LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
    }
    if state.host_aliases.contains_key(&hostname) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "host alias",
            resource_id: hostname,
        });
    }

    let mut descriptor = HostAliasDescriptor::active(
        hostname,
        input.tenant_id,
        input.bucket_name,
        input.region,
        input.prefix,
        config,
    )
    .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    descriptor.state = HostAliasState::PendingVerification;
    let out = descriptor.clone();
    state.host_aliases.insert(out.hostname.clone(), descriptor);
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn transition_host_alias(
    storage: &Storage,
    hostname: &str,
    expected_generation: u64,
    target: HostAliasState,
) -> LifecycleResult<HostAliasDescriptor> {
    let hostname = routing::normalize_alias_hostname(hostname)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    let mut state = read_state(storage).await?;
    let descriptor =
        state
            .host_aliases
            .get_mut(&hostname)
            .ok_or_else(|| LifecycleError::NotFound {
                resource_kind: "host alias",
                resource_id: hostname.clone(),
            })?;
    ensure_generation(
        "host alias",
        &hostname,
        descriptor.generation,
        expected_generation,
    )?;
    validate_host_alias_transition(descriptor.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "host alias",
            resource_id: hostname.clone(),
            from: lifecycle_state_for_host_alias(descriptor.state),
            to: lifecycle_state_for_host_alias(target),
        }
    })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn create_host_alias_in_transaction(
    _storage: &Storage,
    _config: &RoutingConfig,
    _input: CreateHostAliasDescriptor,
    _transaction_id: &str,
    _principal: &str,
) -> LifecycleResult<HostAliasDescriptor> {
    Err(topology_transaction_error("host alias"))
}

pub async fn transition_host_alias_in_transaction(
    _storage: &Storage,
    _hostname: &str,
    _expected_generation: u64,
    _target: HostAliasState,
    _transaction_id: &str,
    _principal: &str,
) -> LifecycleResult<HostAliasDescriptor> {
    Err(topology_transaction_error("host alias"))
}

pub async fn list_host_aliases(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<HostAliasDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    Ok(read_state(storage)
        .await?
        .host_aliases
        .into_values()
        .filter(|alias| {
            region_filter.is_none_or(|region| region.is_empty() || alias.region == region)
        })
        .collect())
}

pub fn validate_host_alias_transition(
    from: HostAliasState,
    to: HostAliasState,
) -> LifecycleResult<()> {
    use HostAliasState::*;
    if matches!(
        (from, to),
        (PendingVerification, Active)
            | (PendingVerification, Deleted)
            | (Active, Suspended)
            | (Active, Deleted)
            | (Suspended, Active)
            | (Suspended, Deleted)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "host alias",
            resource_id: String::new(),
            from: lifecycle_state_for_host_alias(from),
            to: lifecycle_state_for_host_alias(to),
        })
    }
}
