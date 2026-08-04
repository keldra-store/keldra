use std::sync::{Arc, OnceLock};

use anvil_api::v1::{
    AppendPersonalDbEntryRequest, ChangePersonalDbGroupRoleRequest, CreatePersonalDbGroupRequest,
    MaterializePersonalDbProjectionRequest, PersonalDbCommit, PersonalDbGroup,
    PersonalDbGroupRoleChange, PersonalDbMaterialization, PersonalDbSnapshot,
    RegisterPersonalDbSnapshotRequest,
};
use anvil_consensus::NodeId;
use anvil_store::PlacementLogId;
use tonic::Status;

#[derive(Clone, Debug)]
pub(crate) enum RoutedPersonalDbRequest {
    Create(CreatePersonalDbGroupRequest),
    ChangeRole {
        request: ChangePersonalDbGroupRoleRequest,
        granted: bool,
    },
    Append(AppendPersonalDbEntryRequest),
    Materialize(MaterializePersonalDbProjectionRequest),
    RegisterSnapshot(RegisterPersonalDbSnapshotRequest),
}

#[derive(Clone, Debug)]
pub(crate) enum RoutedPersonalDbResponse {
    Group(PersonalDbGroup),
    RoleChange(PersonalDbGroupRoleChange),
    Commit(PersonalDbCommit),
    Materialization(PersonalDbMaterialization),
    Snapshot(PersonalDbSnapshot),
}

pub(crate) struct RoutedPersonalDbCall {
    bearer: Arc<str>,
    placement_fence: PlacementLogId,
    request: RoutedPersonalDbRequest,
}

impl RoutedPersonalDbCall {
    pub(crate) fn new(
        bearer: Arc<str>,
        placement_fence: PlacementLogId,
        request: RoutedPersonalDbRequest,
    ) -> Self {
        Self {
            bearer,
            placement_fence,
            request,
        }
    }

    pub(crate) fn bearer(&self) -> &str {
        &self.bearer
    }

    pub(crate) const fn placement_fence(&self) -> PlacementLogId {
        self.placement_fence
    }

    pub(crate) fn into_request(self) -> RoutedPersonalDbRequest {
        self.request
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ApplyPersonalDbRoleCall {
    pub(crate) bearer: Arc<str>,
    pub(crate) source_node: NodeId,
    pub(crate) placement_fence: PlacementLogId,
    pub(crate) tenant_id: u64,
    pub(crate) bucket_id: u64,
    pub(crate) request: ChangePersonalDbGroupRoleRequest,
    pub(crate) granted: bool,
    pub(crate) creator_owner: bool,
}

#[tonic::async_trait]
pub(crate) trait RoutedPersonalDbHandler: Send + Sync + 'static {
    async fn execute(&self, call: RoutedPersonalDbCall)
    -> Result<RoutedPersonalDbResponse, Status>;

    async fn apply_role(
        &self,
        call: ApplyPersonalDbRoleCall,
    ) -> Result<PersonalDbGroupRoleChange, Status>;
}

#[derive(Clone, Default)]
pub(crate) struct RoutedPersonalDbHandlers {
    inner: Arc<OnceLock<Arc<dyn RoutedPersonalDbHandler>>>,
}

impl RoutedPersonalDbHandlers {
    pub(crate) fn install(
        &self,
        handler: Arc<dyn RoutedPersonalDbHandler>,
    ) -> Result<(), Arc<dyn RoutedPersonalDbHandler>> {
        self.inner.set(handler)
    }

    pub(crate) fn get(&self) -> Result<Arc<dyn RoutedPersonalDbHandler>, Status> {
        self.inner
            .get()
            .cloned()
            .ok_or_else(|| Status::unavailable("routed PersonalDB handler is not ready"))
    }
}
