use super::*;

pub(super) async fn head_object(
    service: &ObjectServiceImpl,
    request: Request<HeadObjectRequest>,
) -> Result<Response<ObjectHead>, Status> {
    let plugin_scope = plugin_object_scope(&request);
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let identity = ObjectReadIdentity::from_request(&request)?;
    let path_access = object_path_access::access_for(&request);
    let requested_key = object_key(request.into_inner().address)?;
    let caller = identity.caller_for_tenant(requested_key.tenant())?;
    object_path_access::require_key(&path_access, &requested_key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &requested_key)?;
    let resolution = object_link::resolve_current(service, requested_key).await?;
    let key = resolution.canonical();
    object_path_access::require_key(&path_access, key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), key)?;
    service
        .authorize_object(&caller, key, ObjectPermission::Get)
        .await?;
    loop {
        let (version, cursor) = service.reader.head_with_program_cursor(key).await?;
        match cursor {
            Some(cursor) if !service.programs.cursor_is_visible(cursor)? => {
                service
                    .programs
                    .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                    .await?;
            }
            _ => {
                if !object_link::revalidate(service, &resolution).await? {
                    return Err(Status::aborted(
                        "object-link binding changed during HeadObject",
                    ));
                }
                return version.map_or_else(
                    || Ok(Response::new(never_existed())),
                    |version| api_head(&version).map(Response::new),
                );
            }
        }
    }
}

pub(super) async fn get_object(
    service: &ObjectServiceImpl,
    request: Request<GetObjectRequest>,
) -> Result<Response<GetObjectStream>, Status> {
    let plugin_scope = plugin_object_scope(&request);
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let identity = ObjectReadIdentity::from_request(&request)?;
    let path_access = object_path_access::access_for(&request);
    let request = request.into_inner();
    let requested_key = object_key(request.address)?;
    let caller = identity.caller_for_tenant(requested_key.tenant())?;
    object_path_access::require_key(&path_access, &requested_key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &requested_key)?;
    let resolution = if object_path_access::is_internal(&path_access) {
        object_link::ResolvedAddress::Ordinary(requested_key)
    } else {
        object_link::resolve_current(service, requested_key).await?
    };
    let key = resolution.canonical();
    object_path_access::require_key(&path_access, key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), key)?;
    if !object_path_access::is_internal(&path_access) {
        service
            .authorize_object(&caller, key, ObjectPermission::Get)
            .await?;
    }
    if request.version.is_some() {
        require_versioning_enabled(&service.store, key)?;
    }
    let requested_version = request.version.map(VersionId);
    let meter_public = !object_path_access::is_internal(&path_access);
    loop {
        let selected = service.reader.open(key, requested_version).await?;
        let cursor = selected
            .as_ref()
            .and_then(|object| object.program_commit_cursor);
        match cursor {
            Some(cursor) if !service.programs.cursor_is_visible(cursor)? => {
                service
                    .programs
                    .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                    .await?;
            }
            _ => {
                if !object_link::revalidate(service, &resolution).await? {
                    return Err(Status::aborted(
                        "object-link binding changed during GetObject",
                    ));
                }
                if !object_path_access::is_internal(&path_access)
                    && selected
                        .as_ref()
                        .is_some_and(|object| !is_public_version(&object.version))
                {
                    return Err(Status::not_found("requested version was not found"));
                }
                if meter_public
                    && let Some(bytes) = selected
                        .as_ref()
                        .and_then(|object| object.version.blob.as_ref())
                        .map(|blob| blob.length)
                {
                    service.record_accounting_outbound(key, bytes);
                }
                return distributed_reads::get_object_response(
                    selected,
                    requested_version.is_some(),
                );
            }
        }
    }
}

pub(super) async fn list_object_versions(
    service: &ObjectServiceImpl,
    request: Request<ListObjectVersionsRequest>,
) -> Result<Response<ListObjectVersionsStream>, Status> {
    let plugin_scope = plugin_object_scope(&request);
    let deadline = request_deadline(request.metadata(), service.atomic_program_timeout)?;
    let identity = ObjectReadIdentity::from_request(&request)?;
    let path_access = object_path_access::access_for(&request);
    let requested_key = object_key(request.into_inner().address)?;
    let caller = identity.caller_for_tenant(requested_key.tenant())?;
    object_path_access::require_key(&path_access, &requested_key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), &requested_key)?;
    let resolution = object_link::resolve_current(service, requested_key).await?;
    let key = resolution.canonical();
    object_path_access::require_key(&path_access, key)?;
    require_plugin_key_scope(plugin_scope.as_ref(), key)?;
    service
        .authorize_object(&caller, key, ObjectPermission::Get)
        .await?;
    let governance = service
        .bucket_governance
        .resolve(key.tenant(), key.bucket())
        .await?;
    require_governance_versioning_enabled(&governance)?;

    loop {
        let mut snapshot = service.distribution.reconciled_object_snapshot(key).await?;
        let cursor = snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .head
                .mutation_stamp
                .and_then(|stamp| stamp.program_commit_cursor)
        });
        match cursor {
            Some(cursor) if !service.programs.cursor_is_visible(cursor)? => {
                service
                    .programs
                    .wait_for_cursor(cursor, deadline_remaining(deadline)?)
                    .await?;
            }
            _ => {
                if !object_link::revalidate(service, &resolution).await? {
                    return Err(Status::aborted(
                        "object-link binding changed during ListObjectVersions",
                    ));
                }
                if let Some(snapshot) = snapshot.as_mut() {
                    retain_public_versions(&mut snapshot.versions);
                }
                return distributed_reads::list_object_versions_response(snapshot, key);
            }
        }
    }
}

fn is_public_version(version: &keldra_store::Version) -> bool {
    !version.protected_link_descriptor
}

fn retain_public_versions(versions: &mut Vec<keldra_store::Version>) {
    versions.retain(is_public_version);
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, Version, VersionId};

    use super::*;

    fn mime_version(id: u64, protected_link_descriptor: bool) -> Version {
        Version {
            id: VersionId(id),
            blob: Some(BlobRef {
                hash: [id as u8; 32],
                length: 1,
            }),
            content_type: Some(keldra_store::OBJECT_LINK_CONTENT_TYPE.into()),
            deleted: false,
            committed_at_unix_millis: 1,
            protected_link_descriptor,
        }
    }

    #[test]
    fn historical_mime_alone_remains_public_but_sealed_descriptor_is_hidden() {
        let ordinary = mime_version(1, false);
        let protected = mime_version(2, true);
        assert!(is_public_version(&ordinary));
        assert!(!is_public_version(&protected));

        let mut versions = vec![ordinary.clone(), protected];
        retain_public_versions(&mut versions);
        assert_eq!(versions, [ordinary]);
    }
}
