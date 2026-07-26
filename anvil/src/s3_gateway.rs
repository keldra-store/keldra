use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use anvil_core::anvil_api::{
    ProxyHeader, ProxyRequestChunk, ProxyRequestHeader, ProxyResponseHeader, proxy_request_chunk,
    proxy_response_chunk,
};
use anvil_core::bucket_journal;
use anvil_core::mesh_directory::{BucketLocatorStatus, TenantNameStatus};
use anvil_core::mesh_lifecycle::{LifecycleState, NodeCapability};
use anvil_core::mvcc_open_transactions::OpenTransactionHandle;
use anvil_core::mvcc_transaction::{DurabilityLevel, ReadConsistency};
use anvil_core::object_links;
use anvil_core::object_manager::{
    ObjectLinkReadMode, ObjectReadConsistency, ObjectWriteOptions, ObjectWriteVisibility,
};
use anvil_core::observability::RESERVED_NAMESPACE_REJECTION_COUNT;
use anvil_core::permissions::AnvilAction;
use anvil_core::persistence::Object;
use anvil_core::routing::{
    self as core_routing, CrossRegionRoutingPolicy, HostAliasDescriptor, ObjectRoute, RouteRequest,
    RouteSource, RoutingConfig, RoutingError,
};
use anvil_core::validation;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{self, HeaderMap, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_core::Stream;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod bucket;
mod guard;
mod multipart;
mod object;
mod preconditions;
mod proxy;
mod routing;
mod util;

#[allow(unused_imports)]
use bucket::*;
#[allow(unused_imports)]
use guard::*;
#[allow(unused_imports)]
use multipart::*;
#[allow(unused_imports)]
use object::*;
#[allow(unused_imports)]
use preconditions::*;
#[allow(unused_imports)]
use proxy::*;
#[allow(unused_imports)]
use routing::*;
#[allow(unused_imports)]
use util::*;

fn s3_write_durability(state: &AppState) -> Result<DurabilityLevel, tonic::Status> {
    match state.config.mvcc_default_durability.as_str() {
        "local" => Ok(DurabilityLevel::Local),
        "quorum" => Ok(DurabilityLevel::Quorum),
        "erasure" => Ok(DurabilityLevel::Erasure),
        _ => Err(tonic::Status::failed_precondition(
            "mvcc_default_durability must be local, quorum, or erasure",
        )),
    }
}

fn s3_now_unix_ms() -> Result<u64, tonic::Status> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| tonic::Status::internal("system clock precedes Unix epoch"))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| tonic::Status::internal("system time exceeds u64"))
}

async fn begin_s3_write_transaction(
    state: &AppState,
    claims: &Claims,
    operation: &str,
    bucket: &str,
    object_key: &str,
) -> Result<(OpenTransactionHandle, String), tonic::Status> {
    let principal = anvil_core::object_manager::transaction_principal_from_claims(claims);
    let idempotency_key = format!(
        "s3/{operation}/{bucket}/{object_key}/{}",
        uuid::Uuid::new_v4()
    );
    let handle = state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.mvcc.cluster_id().to_string(),
            &principal,
            idempotency_key,
            Duration::from_secs(60),
            s3_write_durability(state)?,
            ReadConsistency::Linearized,
            s3_now_unix_ms()?,
        )
        .await
        .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
    Ok((handle, principal))
}

async fn commit_s3_write_transaction(
    state: &AppState,
    handle: &OpenTransactionHandle,
    principal: &str,
) -> Result<(), tonic::Status> {
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            s3_now_unix_ms()?,
        )
        .await
        .map_err(|error| tonic::Status::unavailable(error.to_string()))?;
    if !matches!(
        outcome.certification,
        anvil_core::mvcc_transaction::CertificationResult::Committed { .. }
    ) {
        return Err(tonic::Status::aborted(
            "S3 object transaction was not certified",
        ));
    }
    Ok(())
}

pub fn app(state: AppState) -> Router {
    let public = Router::new()
        .route("/ready", get(readiness_check))
        .with_state(state.clone());

    let s3_routes = Router::new()
        .route("/", get(list_buckets)) // ListBuckets
        .route(
            "/{bucket}",
            put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
                .post(post_bucket)
                .get(list_objects),
        )
        .route(
            "/{bucket}/",
            get(list_objects)
                .put(create_bucket)
                .delete(delete_bucket)
                .post(post_bucket)
                .head(head_bucket),
        )
        .route(
            "/{bucket}/{*path}",
            get(get_object)
                .put(put_object)
                .post(post_object)
                .delete(delete_object)
                .head(head_object),
        )
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            reserved_namespace_guard,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            s3_host_routing,
        ))
        .layer(middleware::from_fn(aws_chunked_decoder))
        .layer(middleware::from_fn_with_state(state.clone(), sigv4_auth))
        .layer(middleware::from_fn_with_state(
            state,
            reserved_namespace_guard,
        ));

    public.merge(s3_routes)
}

#[cfg(test)]
mod tests;
