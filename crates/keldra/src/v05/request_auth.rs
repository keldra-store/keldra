use keldra_store::ObjectKey;
use tonic::{Request, Status};

use crate::authentication::{Caller, PluginObjectScope};

pub(super) fn authenticated_caller<T>(request: &Request<T>) -> Result<Caller, Status> {
    request
        .extensions()
        .get::<Caller>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("authenticated caller identity is missing"))
}

pub(super) fn plugin_object_scope<T>(request: &Request<T>) -> Option<PluginObjectScope> {
    request.extensions().get::<PluginObjectScope>().cloned()
}

pub(super) fn require_plugin_key_scope(
    scope: Option<&PluginObjectScope>,
    key: &ObjectKey,
) -> Result<(), Status> {
    if scope.is_none_or(|scope| scope.allows(key.tenant(), key.bucket(), key.path())) {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "plugin object token does not cover the addressed path",
        ))
    }
}

pub(super) fn require_plugin_list_scope(
    scope: Option<&PluginObjectScope>,
    tenant: &str,
    bucket: &str,
    prefix: &str,
) -> Result<(), Status> {
    if scope.is_none_or(|scope| scope.allows_prefix(tenant, bucket, prefix)) {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "plugin object token does not cover the listed prefix",
        ))
    }
}

pub(super) fn reject_plugin_token<T>(
    request: &Request<T>,
    operation: &'static str,
) -> Result<(), Status> {
    if request.extensions().get::<PluginObjectScope>().is_some() {
        Err(Status::permission_denied(format!(
            "plugin object tokens cannot call {operation}"
        )))
    } else {
        Ok(())
    }
}

pub(super) fn require_caller_tenant(caller: &Caller, key: &ObjectKey) -> Result<(), Status> {
    if caller.storage_tenant().as_str() == key.tenant() {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "object address does not belong to the authenticated tenant",
        ))
    }
}

pub(super) fn require_authorized(allowed: bool, message: &'static str) -> Result<(), Status> {
    if allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(message))
    }
}
