use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{CreateHostAliasDescriptor, LifecycleError};
use crate::native_idempotency::{self, NativeIdempotencyTarget};
use crate::object_links;
use crate::object_manager::{
    AuthzMaterializationVisibility, AuthzRevisionVisibility, BoundaryExtractionVisibility,
    IndexMaintenanceVisibility, IndexPolicySnapshotVisibility, ObjectWriteOptions,
    ObjectWriteVisibility, WatchVisibility,
};
use crate::permissions::AnvilAction;
use crate::routing::{
    self, HostAliasDescriptor as CoreHostAliasDescriptor, HostAliasState as CoreHostAliasState,
    RoutingConfig,
};
use crate::{
    AppState, auth, authz_journal, bucket_journal,
    services::watch_envelope::{self, WatchEnvelopeParts},
    task_lease, watch_log,
};
use futures_util::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::OwnedMutexGuard;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

mod batch_helpers;
mod boundary_rpc;
mod common;
mod link_helpers;
mod link_rpc;
mod mutation_batch_rpc;
mod native_mutation;
mod native_put_rpc;
mod page_token;
mod peer_proxy;
mod rpc;
mod stream_rpc;
mod watch;

pub(crate) use batch_helpers::enforce_write_precondition;
use batch_helpers::*;
use boundary_rpc::*;
use common::*;
use link_helpers::*;
use mutation_batch_rpc::execute_mutation_batch;
use native_mutation::*;
pub(crate) use native_put_rpc::execute_native_put;
use native_put_rpc::native_put_data_chunk;
use page_token::*;
use peer_proxy::*;
use stream_rpc::*;
use watch::*;
