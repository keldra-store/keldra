use std::sync::Arc;

use keldra_api::v1::object_service_server::ObjectService;
use keldra_api::v1::{
    BucketPolicy, BulkWriteRequest, BulkWriteResponse, CloneObjectRequest, DeleteIfVersionRequest,
    DeleteRequest, DeleteVersionRequest, DeleteVersionResponse, InvokeProgramRequest,
    InvokeProgramResponse, LinkObjectRequest, MutationReceipt, PutToken, SetBucketPolicyRequest,
    UnlinkObjectRequest,
};
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use super::ObjectServiceImpl;
use crate::cluster_peer::{RoutedCall, RoutedPublicHandler};
use crate::object_path_access;

/// Marks a request delivered by the authenticated peer listener.
///
/// This marker is local process state, never a protobuf or trusted caller
/// claim. Most object-service paths permit only one hop; Delete may make the
/// narrowly authenticated executor-to-path-owner hop described below.
#[derive(Clone, Copy)]
pub(super) struct RoutedDestination;

pub(super) fn is_routed<T>(request: &Request<T>) -> bool {
    request.extensions().get::<RoutedDestination>().is_some()
}

/// Authenticated peer evidence that the global atomic executor performed the
/// exact Unlink replay lookup before forwarding an ordinary delete.
#[derive(Clone, Copy)]
pub(super) struct AtomicExecutorReplayChecked {
    pub(super) source_node: keldra_consensus::NodeId,
}

pub(super) fn atomic_executor_replay_marker<T>(
    request: &Request<T>,
) -> Option<AtomicExecutorReplayChecked> {
    request
        .extensions()
        .get::<AtomicExecutorReplayChecked>()
        .copied()
}

#[derive(Clone)]
pub(super) struct DeleteVersionOriginalAlias(pub(super) keldra_store::ObjectKey);

#[derive(Clone)]
struct RoutedObjectWrites {
    service: ObjectServiceImpl,
}

impl ObjectServiceImpl {
    pub(crate) fn routed_public_handler(&self) -> Arc<dyn RoutedPublicHandler> {
        Arc::new(RoutedObjectWrites {
            service: self.clone(),
        })
    }
}

impl RoutedObjectWrites {
    fn authenticated_request<T>(
        &self,
        call: RoutedCall<T>,
        internal: bool,
    ) -> Result<Request<T>, Status> {
        let definition_intents = call.definition_intents().to_vec();
        let replay_checked = call.atomic_executor_replay_checked();
        let delete_version_original_alias = call.delete_version_original_alias().cloned();
        let source_node = call.source_node();
        let bearer = call.bearer().to_owned();
        let (caller, plugin_scope) = self
            .service
            .jwt_manager
            .verify_object_bearer(&bearer)
            .map_err(|_| Status::unauthenticated("the bearer token is invalid or expired"))?;
        let authorization = format!("Bearer {bearer}")
            .parse::<MetadataValue<_>>()
            .map_err(|_| Status::unauthenticated("the bearer token is malformed"))?;
        let mut request = Request::new(call.into_request());
        request
            .metadata_mut()
            .insert("authorization", authorization);
        request.extensions_mut().insert(caller);
        if let Some(scope) = plugin_scope {
            request.extensions_mut().insert(scope);
        }
        request.extensions_mut().insert(RoutedDestination);
        if replay_checked {
            request
                .extensions_mut()
                .insert(AtomicExecutorReplayChecked { source_node });
        }
        if let Some(alias) = delete_version_original_alias {
            request
                .extensions_mut()
                .insert(DeleteVersionOriginalAlias(alias));
        }
        if internal {
            if definition_intents.is_empty() {
                object_path_access::mark_internal_peer_route(&mut request);
            } else {
                object_path_access::mark_internal_peer_definition_route(
                    &mut request,
                    definition_intents,
                );
            }
        }
        Ok(request)
    }
}

#[tonic::async_trait]
impl RoutedPublicHandler for RoutedObjectWrites {
    async fn put_end(&self, call: RoutedCall<PutToken>) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::put_end(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn delete(&self, call: RoutedCall<DeleteRequest>) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::delete(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn delete_if_version(
        &self,
        call: RoutedCall<DeleteIfVersionRequest>,
    ) -> Result<MutationReceipt, Status> {
        Ok(ObjectService::delete_if_version(
            &self.service,
            self.authenticated_request(call, false)?,
        )
        .await?
        .into_inner())
    }

    async fn clone_object(
        &self,
        call: RoutedCall<CloneObjectRequest>,
    ) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::clone_object(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn link_object(
        &self,
        call: RoutedCall<LinkObjectRequest>,
    ) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::link_object(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn unlink_object(
        &self,
        call: RoutedCall<UnlinkObjectRequest>,
    ) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::unlink_object(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn bulk_write(
        &self,
        call: RoutedCall<BulkWriteRequest>,
    ) -> Result<BulkWriteResponse, Status> {
        Ok(
            ObjectService::bulk_write(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn internal_delete_if_version(
        &self,
        call: RoutedCall<DeleteIfVersionRequest>,
    ) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::delete_if_version(
                &self.service,
                self.authenticated_request(call, true)?,
            )
            .await?
            .into_inner(),
        )
    }

    async fn internal_put_end(
        &self,
        call: RoutedCall<PutToken>,
    ) -> Result<MutationReceipt, Status> {
        Ok(
            ObjectService::put_end(&self.service, self.authenticated_request(call, true)?)
                .await?
                .into_inner(),
        )
    }

    async fn internal_bulk_write(
        &self,
        call: RoutedCall<BulkWriteRequest>,
    ) -> Result<BulkWriteResponse, Status> {
        Ok(
            ObjectService::bulk_write(&self.service, self.authenticated_request(call, true)?)
                .await?
                .into_inner(),
        )
    }

    async fn set_bucket_policy(
        &self,
        call: RoutedCall<SetBucketPolicyRequest>,
    ) -> Result<BucketPolicy, Status> {
        Ok(ObjectService::set_bucket_policy(
            &self.service,
            self.authenticated_request(call, false)?,
        )
        .await?
        .into_inner())
    }

    async fn delete_version(
        &self,
        call: RoutedCall<DeleteVersionRequest>,
    ) -> Result<DeleteVersionResponse, Status> {
        Ok(
            ObjectService::delete_version(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn invoke_program(
        &self,
        call: RoutedCall<InvokeProgramRequest>,
    ) -> Result<InvokeProgramResponse, Status> {
        Ok(
            ObjectService::invoke_program(&self.service, self.authenticated_request(call, false)?)
                .await?
                .into_inner(),
        )
    }

    async fn replay_builtin_batch(
        &self,
        lookups: Vec<crate::programs::BuiltInReplayLookup>,
    ) -> Result<Vec<Result<Option<crate::programs::InvokedProgramResult>, Status>>, Status> {
        self.service
            .programs
            .replay_builtin_object_transactions(&lookups)
            .await
    }
}

#[cfg(test)]
mod tests {
    use keldra_store::ObjectKey;

    use super::*;

    #[test]
    fn ordinary_routed_public_marker_does_not_grant_reserved_access() {
        let mut request = Request::new(());
        request.extensions_mut().insert(RoutedDestination);
        let access = object_path_access::access_for(&request);
        let reserved = ObjectKey::new("tenant", "bucket", "_keldra/internal/00").unwrap();

        assert_eq!(
            object_path_access::require_key(&access, &reserved)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}
