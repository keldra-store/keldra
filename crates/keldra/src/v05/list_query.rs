use keldra_api::v1::{ListObjectsRequest, ListObjectsResponse};
use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use keldra_store::{MAX_LIST_OBJECTS, ObjectKey};
use tonic::Status;

use super::DEFAULT_LIST_OBJECTS_LIMIT;
use super::object_link::resolve_current;
use super::{ObjectServiceImpl, OriginalBearer};
use crate::authentication::PluginObjectScope;
use crate::object_path_access;

#[derive(Debug)]
pub(super) struct ListObjectsQuery {
    pub(super) tenant: String,
    pub(super) bucket: String,
    pub(super) prefix: String,
    pub(super) start_after: Option<String>,
    pub(super) limit: usize,
}

pub(super) fn list_objects_query(request: ListObjectsRequest) -> Result<ListObjectsQuery, Status> {
    if request.prefix.len() > MAX_OBJECT_PATH_BYTES {
        return Err(Status::invalid_argument(format!(
            "list prefix exceeds {MAX_OBJECT_PATH_BYTES} UTF-8 bytes"
        )));
    }
    let validation_path = request.start_after.as_deref().unwrap_or("_list");
    ObjectKey::new(&request.tenant, &request.bucket, validation_path)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let limit = match request.limit as usize {
        0 => DEFAULT_LIST_OBJECTS_LIMIT,
        limit if limit <= MAX_LIST_OBJECTS => limit,
        _ => {
            return Err(Status::invalid_argument(format!(
                "list limit must not exceed {MAX_LIST_OBJECTS}"
            )));
        }
    };
    Ok(ListObjectsQuery {
        tenant: request.tenant,
        bucket: request.bucket,
        prefix: request.prefix,
        start_after: request.start_after,
        limit,
    })
}

pub(super) async fn list_objects_scoped(
    service: &ObjectServiceImpl,
    bearer: OriginalBearer,
    tenant_id: u64,
    bucket_id: u64,
    query: ListObjectsQuery,
    plugin_scope: Option<&PluginObjectScope>,
) -> Result<ListObjectsResponse, Status> {
    if plugin_scope.is_none() {
        let page = service
            .lister
            .list_objects(
                bearer,
                &query.tenant,
                &query.bucket,
                tenant_id,
                bucket_id,
                &query.prefix,
                query.start_after.as_deref(),
                query.limit,
            )
            .await?;
        return Ok(ListObjectsResponse {
            paths: page.paths,
            has_more: page.has_more,
        });
    }

    let scope = plugin_scope.expect("checked above");
    let mut start_after = query.start_after;
    let mut visible = Vec::with_capacity(query.limit);
    loop {
        let prior_cursor = start_after.clone();
        let page = service
            .lister
            .list_objects(
                bearer.clone(),
                &query.tenant,
                &query.bucket,
                tenant_id,
                bucket_id,
                &query.prefix,
                start_after.as_deref(),
                query.limit,
            )
            .await?;
        let page_has_more = page.has_more;
        for path in page.paths {
            start_after = Some(path.clone());
            let requested = ObjectKey::new(&query.tenant, &query.bucket, &path)
                .map_err(|_| Status::data_loss("object listing returned an invalid path"))?;
            object_path_access::require_public_key(&requested)?;
            if !scope.allows(requested.tenant(), requested.bucket(), requested.path()) {
                continue;
            }
            let resolved = resolve_current(service, requested).await?;
            let canonical = resolved.canonical();
            if object_path_access::require_public_key(canonical).is_ok()
                && scope.allows(canonical.tenant(), canonical.bucket(), canonical.path())
            {
                if visible.len() == query.limit {
                    return Ok(ListObjectsResponse {
                        paths: visible,
                        has_more: true,
                    });
                }
                visible.push(path);
            }
        }
        if !page_has_more {
            return Ok(ListObjectsResponse {
                paths: visible,
                has_more: false,
            });
        }
        if start_after == prior_cursor {
            return Err(Status::data_loss(
                "object listing did not advance its continuation cursor",
            ));
        }
    }
}
