use super::*;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipResourceKind {
    ControlPartition,
    BucketPrimary,
    ObjectPartition,
    IndexPartition,
    PersonalDbGroup,
    TaskQueue,
    WatchPartition,
}

impl OwnershipResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlPartition => "control_partition",
            Self::BucketPrimary => "bucket_primary",
            Self::ObjectPartition => "object_partition",
            Self::IndexPartition => "index_partition",
            Self::PersonalDbGroup => "personaldb_group",
            Self::TaskQueue => "task_queue",
            Self::WatchPartition => "watch_partition",
        }
    }
}

fn ownership_resource_kind_from_str(value: &str) -> Result<OwnershipResourceKind> {
    Ok(match value {
        "control_partition" => OwnershipResourceKind::ControlPartition,
        "bucket_primary" => OwnershipResourceKind::BucketPrimary,
        "object_partition" => OwnershipResourceKind::ObjectPartition,
        "index_partition" => OwnershipResourceKind::IndexPartition,
        "personaldb_group" => OwnershipResourceKind::PersonalDbGroup,
        "task_queue" => OwnershipResourceKind::TaskQueue,
        "watch_partition" => OwnershipResourceKind::WatchPartition,
        _ => bail!("unsupported ownership resource kind {value}"),
    })
}

pub(super) fn ownership_resource_hash(
    tenant_id: i64,
    resource: &OwnershipResource,
) -> Result<String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(tenant_id.to_string().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(resource.resource_kind.as_str().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(resource.resource_id.as_bytes());
    Ok(hex::encode(hash32(&bytes)))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipResource {
    pub resource_kind: OwnershipResourceKind,
    pub resource_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipPrincipal {
    pub tenant_id: i64,
    pub principal_kind: String,
    pub principal_id: String,
    pub actor_instance_id: String,
    pub display_name: String,
    pub region: String,
    pub cell: String,
}

impl OwnershipPrincipal {
    pub fn node(owner_node_id: impl Into<String>) -> Self {
        let owner_node_id = owner_node_id.into();
        Self {
            tenant_id: 0,
            principal_kind: "node".to_string(),
            principal_id: owner_node_id.clone(),
            actor_instance_id: owner_node_id.clone(),
            display_name: owner_node_id,
            region: "default".to_string(),
            cell: "default".to_string(),
        }
    }

    pub fn same_security_owner(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.principal_kind == other.principal_kind
            && self.principal_id == other.principal_id
            && self.actor_instance_id == other.actor_instance_id
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipFenceState {
    Active,
    Transferring,
    Draining,
    Expired,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipFenceRecord {
    pub format_version: u16,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub fence: u64,
    pub state: OwnershipFenceState,
    pub lease_expires_at_nanos: i64,
    pub last_heartbeat_at_nanos: i64,
    pub generation: u64,
    pub last_operation: Option<String>,
    pub last_idempotency_key: Option<String>,
    #[serde(default)]
    pub last_actor: Option<OwnershipPrincipal>,
    pub ownership_hash: Option<String>,
    pub ownership_signature: Option<String>,
}

impl OwnershipFenceRecord {
    pub fn seal(mut self, _signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_ownership_fence(&self)?;
        self.ownership_hash = Some(hash_ownership_fence(&self)?);
        self.ownership_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _signing_key: &[u8]) -> Result<()> {
        validate_unsigned_ownership_fence(self)?;
        let expected_hash = hash_ownership_fence(self)?;
        if self.ownership_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("ownership fence hash mismatch"));
        }
        Ok(())
    }

    pub fn is_active_unexpired(&self, now_nanos: i64) -> bool {
        matches!(
            self.state,
            OwnershipFenceState::Active
                | OwnershipFenceState::Transferring
                | OwnershipFenceState::Draining
        ) && self.lease_expires_at_nanos > now_nanos
    }
}

impl OwnershipFenceState {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Transferring => "transferring",
            Self::Draining => "draining",
            Self::Expired => "expired",
            Self::Released => "released",
        }
    }
}

fn ownership_fence_state_from_str(value: &str) -> Result<OwnershipFenceState> {
    Ok(match value {
        "active" => OwnershipFenceState::Active,
        "transferring" => OwnershipFenceState::Transferring,
        "draining" => OwnershipFenceState::Draining,
        "expired" => OwnershipFenceState::Expired,
        "released" => OwnershipFenceState::Released,
        _ => bail!("unsupported ownership fence state {value}"),
    })
}

#[derive(Clone, PartialEq, Message)]
struct OwnershipResourceProto {
    #[prost(string, tag = "1")]
    resource_kind: String,
    #[prost(string, tag = "2")]
    resource_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct OwnershipPrincipalProto {
    #[prost(int64, tag = "1")]
    tenant_id: i64,
    #[prost(string, tag = "2")]
    principal_kind: String,
    #[prost(string, tag = "3")]
    principal_id: String,
    #[prost(string, tag = "4")]
    actor_instance_id: String,
    #[prost(string, tag = "5")]
    display_name: String,
    #[prost(string, tag = "6")]
    region: String,
    #[prost(string, tag = "7")]
    cell: String,
}

#[derive(Clone, PartialEq, Message)]
struct OwnershipFenceRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(message, optional, tag = "3")]
    resource: Option<OwnershipResourceProto>,
    #[prost(message, optional, tag = "4")]
    owner: Option<OwnershipPrincipalProto>,
    #[prost(uint64, tag = "5")]
    fence: u64,
    #[prost(string, tag = "6")]
    state: String,
    #[prost(int64, tag = "7")]
    lease_expires_at_nanos: i64,
    #[prost(int64, tag = "8")]
    last_heartbeat_at_nanos: i64,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(string, optional, tag = "10")]
    last_operation: Option<String>,
    #[prost(string, optional, tag = "11")]
    last_idempotency_key: Option<String>,
    #[prost(message, optional, tag = "12")]
    last_actor: Option<OwnershipPrincipalProto>,
    #[prost(string, optional, tag = "13")]
    ownership_hash: Option<String>,
    #[prost(string, optional, tag = "14")]
    ownership_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct PartitionOwnerRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(string, tag = "3")]
    partition_family: String,
    #[prost(string, tag = "4")]
    partition_id: String,
    #[prost(string, tag = "5")]
    owner_node_id: String,
    #[prost(uint64, tag = "6")]
    fence_token: u64,
    #[prost(uint64, tag = "7")]
    recovery_epoch: u64,
    #[prost(string, tag = "8")]
    status: String,
    #[prost(uint64, tag = "9")]
    recovered_through_sequence: u64,
    #[prost(string, tag = "10")]
    recovered_manifest_hash: String,
    #[prost(int64, tag = "11")]
    updated_at_nanos: i64,
    #[prost(string, optional, tag = "12")]
    owner_hash: Option<String>,
    #[prost(string, optional, tag = "13")]
    owner_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipFenceOutcome {
    pub record: OwnershipFenceRecord,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone)]
pub struct OwnershipFenceWritePlan {
    pub outcome: OwnershipFenceOutcome,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    assignment_identity: String,
}

impl OwnershipFenceWritePlan {
    pub async fn stage_into_transaction(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        self.stage_into_transaction_with_assignment(
            mvcc,
            transaction_id,
            principal,
            now_unix_ms,
            None,
        )
        .await
    }

    async fn stage_into_transaction_with_assignment(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
        assignment: Option<&crate::mvcc_worker_authority::AssignmentGuard>,
    ) -> Result<()> {
        mvcc.open_transactions.binding(transaction_id, principal)?;
        let reconciled;
        let assignment = if let Some(assignment) = assignment {
            mvcc.validate_assignment(assignment)?;
            assignment
        } else {
            reconciled = mvcc
                .reconcile_work_assignment("ownership-fence", &self.assignment_identity)
                .await?
                .ok_or_else(|| anyhow!("local node does not own the ownership-fence assignment"))?;
            &reconciled
        };
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for (key, predicate) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        mvcc.stage_assignment_guard(transaction_id, principal, assignment, now_unix_ms)
    }
}

pub(crate) async fn commit_implicit_ownership_plan(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    principal: &str,
    idempotency_key: &str,
    now_nanos: i64,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
    planner: impl FnOnce(&str) -> Result<OwnershipFenceWritePlan>,
) -> Result<OwnershipFenceOutcome> {
    commit_implicit_ownership_plan_with_assignment(
        mvcc,
        principal,
        idempotency_key,
        now_nanos,
        tenant_id,
        resource,
        signing_key,
        None,
        planner,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_implicit_ownership_plan_with_assignment(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    principal: &str,
    idempotency_key: &str,
    now_nanos: i64,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
    assignment: Option<&crate::mvcc_worker_authority::AssignmentGuard>,
    planner: impl FnOnce(&str) -> Result<OwnershipFenceWritePlan>,
) -> Result<OwnershipFenceOutcome> {
    let now_unix_ms = u64::try_from(now_nanos / 1_000_000).unwrap_or_default();
    let assignment_scoped_idempotency_key = assignment.map(|assignment| {
        format!(
            "{idempotency_key}:assignment-{}-{}",
            assignment.partition_id, assignment.assignment_epoch
        )
    });
    let transaction_idempotency_key = assignment_scoped_idempotency_key
        .as_deref()
        .unwrap_or(idempotency_key);
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            principal,
            transaction_idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now_unix_ms,
        )
        .await?;
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, principal, now_unix_ms)?;
    let planned_outcome = if status.state == "open" {
        let plan = planner(&handle.transaction_id)?;
        let outcome = plan.outcome.clone();
        plan.stage_into_transaction_with_assignment(
            mvcc,
            &handle.transaction_id,
            principal,
            now_unix_ms,
            assignment,
        )
        .await?;
        Some(outcome)
    } else {
        None
    };
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            now_unix_ms,
        )
        .await?;
    if let crate::mvcc_transaction::CertificationResult::Aborted { reason } = outcome.certification
    {
        bail!("{OWNERSHIP_CAS_CONFLICT}: ownership fence MVCC transaction aborted: {reason:?}");
    }
    if let Some(outcome) = planned_outcome {
        return Ok(outcome);
    }
    let record =
        read_ownership_fence_mvcc(mvcc, tenant_id, resource, signing_key)?.ok_or_else(|| {
            anyhow!("{OWNERSHIP_NOT_FOUND}: resolved ownership transaction has no row")
        })?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: true,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewOwnership {
    pub request_id: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub current_owner: OwnershipPrincipal,
    pub new_owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub administrative_force: bool,
    pub now_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceExpireOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub admin: OwnershipPrincipal,
    pub reason: String,
    pub now_nanos: i64,
}

impl PartitionOwnerState {
    pub fn seal(mut self, _signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_owner(&self)?;
        self.owner_hash = Some(hash_partition_owner(&self)?);
        self.owner_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _signing_key: &[u8]) -> Result<()> {
        validate_unsigned_owner(self)?;
        let expected_hash = hash_partition_owner(self)?;
        if self.owner_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("partition owner hash mismatch"));
        }
        Ok(())
    }

    pub fn write_permit(&self) -> Result<PartitionWritePermit, FenceRejection> {
        if self.status != PartitionOwnerStatus::Ready {
            return Err(FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner has not completed recovery",
            });
        }
        Ok(PartitionWritePermit {
            partition_family: self.partition_family.clone(),
            partition_id: self.partition_id.clone(),
            owner_node_id: self.owner_node_id.clone(),
            fence_token: self.fence_token,
        })
    }
}

pub fn hash_partition_owner(owner: &PartitionOwnerState) -> Result<String> {
    let mut unsigned = owner.clone();
    unsigned.owner_hash = None;
    unsigned.owner_signature = None;
    Ok(hex::encode(hash32(&encode_partition_owner_record(
        &unsigned,
    )?)))
}

pub fn hash_ownership_fence(record: &OwnershipFenceRecord) -> Result<String> {
    let mut unsigned = record.clone();
    unsigned.ownership_hash = None;
    unsigned.ownership_signature = None;
    Ok(hex::encode(hash32(&encode_ownership_fence_record(
        &unsigned,
    )?)))
}

pub(super) fn encode_partition_owner_record(owner: &PartitionOwnerState) -> Result<Vec<u8>> {
    Ok(partition_owner_to_proto(owner).encode_to_vec())
}

pub(super) fn decode_partition_owner_record(bytes: &[u8]) -> Result<PartitionOwnerState> {
    let proto = PartitionOwnerRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("partition owner record is not deterministic protobuf");
    }
    partition_owner_from_proto(proto)
}

fn partition_owner_to_proto(owner: &PartitionOwnerState) -> PartitionOwnerRecordProto {
    PartitionOwnerRecordProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&format!(
                "partition-owner/{}/{}",
                owner.partition_family, owner.partition_id
            )),
            owner.generation,
            coremeta::partition_owner_transaction_id(owner),
            owner.updated_at_nanos.max(0) as u64,
        )),
        format_version: u32::from(owner.format_version),
        partition_family: owner.partition_family.clone(),
        partition_id: owner.partition_id.clone(),
        owner_node_id: owner.owner_node_id.clone(),
        fence_token: owner.fence_token,
        recovery_epoch: owner.recovery_epoch,
        status: owner.status.as_str().to_string(),
        recovered_through_sequence: owner.recovered_through_sequence,
        recovered_manifest_hash: owner.recovered_manifest_hash.clone(),
        updated_at_nanos: owner.updated_at_nanos,
        owner_hash: owner.owner_hash.clone(),
        owner_signature: owner.owner_signature.clone(),
    }
}

fn partition_owner_from_proto(proto: PartitionOwnerRecordProto) -> Result<PartitionOwnerState> {
    let generation = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("partition owner row missing CoreMeta common"))?
        .root_generation;
    Ok(PartitionOwnerState {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("partition owner format version exceeds u16"))?,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        owner_node_id: proto.owner_node_id,
        fence_token: proto.fence_token,
        recovery_epoch: proto.recovery_epoch,
        generation,
        status: partition_owner_status_from_str(&proto.status)?,
        recovered_through_sequence: proto.recovered_through_sequence,
        recovered_manifest_hash: proto.recovered_manifest_hash,
        updated_at_nanos: proto.updated_at_nanos,
        owner_hash: proto.owner_hash,
        owner_signature: proto.owner_signature,
    })
}

pub(super) fn encode_ownership_fence_record(record: &OwnershipFenceRecord) -> Result<Vec<u8>> {
    let proto = ownership_fence_record_to_proto(record)?;
    Ok(proto.encode_to_vec())
}

pub(super) fn decode_ownership_fence_record(bytes: &[u8]) -> Result<OwnershipFenceRecord> {
    let proto = OwnershipFenceRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("ownership fence record is not deterministic protobuf");
    }
    ownership_fence_record_from_proto(proto)
}

pub(super) fn ownership_fence_record_to_proto(
    record: &OwnershipFenceRecord,
) -> Result<OwnershipFenceRecordProto> {
    let root_key_hash = match record.format_version {
        1 => core_meta_root_key_hash(&format!(
            "ownership-fence/{}/{}",
            record.resource.resource_kind.as_str(),
            record.resource.resource_id
        )),
        2 => core_meta_root_key_hash(&format!(
            "ownership-fence/v2/tenant:{}/{}/{}",
            record.owner.tenant_id,
            record.resource.resource_kind.as_str(),
            ownership_resource_hash(record.owner.tenant_id, &record.resource)?,
        )),
        _ => return Err(anyhow!("unsupported ownership fence version")),
    };
    Ok(OwnershipFenceRecordProto {
        common: Some(core_meta_committed_row_common(
            format!("tenant/{}", record.owner.tenant_id),
            root_key_hash,
            record.generation,
            coremeta::ownership_fence_transaction_id(record)?,
            record.last_heartbeat_at_nanos.max(0) as u64,
        )),
        format_version: u32::from(record.format_version),
        resource: Some(ownership_resource_to_proto(&record.resource)),
        owner: Some(ownership_principal_to_proto(&record.owner)),
        fence: record.fence,
        state: record.state.as_str().to_string(),
        lease_expires_at_nanos: record.lease_expires_at_nanos,
        last_heartbeat_at_nanos: record.last_heartbeat_at_nanos,
        generation: record.generation,
        last_operation: record.last_operation.clone(),
        last_idempotency_key: record.last_idempotency_key.clone(),
        last_actor: record.last_actor.as_ref().map(ownership_principal_to_proto),
        ownership_hash: record.ownership_hash.clone(),
        ownership_signature: record.ownership_signature.clone(),
    })
}

fn ownership_fence_record_from_proto(
    proto: OwnershipFenceRecordProto,
) -> Result<OwnershipFenceRecord> {
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("ownership fence row missing CoreMeta common"))?;
    Ok(OwnershipFenceRecord {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("ownership fence format version exceeds u16"))?,
        resource: ownership_resource_from_proto(
            proto
                .resource
                .ok_or_else(|| anyhow!("ownership fence record is missing resource"))?,
        )?,
        owner: ownership_principal_from_proto(
            proto
                .owner
                .ok_or_else(|| anyhow!("ownership fence record is missing owner"))?,
        ),
        fence: proto.fence,
        state: ownership_fence_state_from_str(&proto.state)?,
        lease_expires_at_nanos: proto.lease_expires_at_nanos,
        last_heartbeat_at_nanos: proto.last_heartbeat_at_nanos,
        generation: proto.generation,
        last_operation: proto.last_operation,
        last_idempotency_key: proto.last_idempotency_key,
        last_actor: proto.last_actor.map(ownership_principal_from_proto),
        ownership_hash: proto.ownership_hash,
        ownership_signature: proto.ownership_signature,
    })
}

fn ownership_resource_to_proto(resource: &OwnershipResource) -> OwnershipResourceProto {
    OwnershipResourceProto {
        resource_kind: resource.resource_kind.as_str().to_string(),
        resource_id: resource.resource_id.clone(),
    }
}

fn ownership_resource_from_proto(proto: OwnershipResourceProto) -> Result<OwnershipResource> {
    Ok(OwnershipResource {
        resource_kind: ownership_resource_kind_from_str(&proto.resource_kind)?,
        resource_id: proto.resource_id,
    })
}

fn ownership_principal_to_proto(principal: &OwnershipPrincipal) -> OwnershipPrincipalProto {
    OwnershipPrincipalProto {
        tenant_id: principal.tenant_id,
        principal_kind: principal.principal_kind.clone(),
        principal_id: principal.principal_id.clone(),
        actor_instance_id: principal.actor_instance_id.clone(),
        display_name: principal.display_name.clone(),
        region: principal.region.clone(),
        cell: principal.cell.clone(),
    }
}

fn ownership_principal_from_proto(proto: OwnershipPrincipalProto) -> OwnershipPrincipal {
    OwnershipPrincipal {
        tenant_id: proto.tenant_id,
        principal_kind: proto.principal_kind,
        principal_id: proto.principal_id,
        actor_instance_id: proto.actor_instance_id,
        display_name: proto.display_name,
        region: proto.region,
        cell: proto.cell,
    }
}

pub fn plan_acquire_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: AcquireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceWritePlan> {
    validate_acquire_ownership(&request)?;
    let existing = read_ownership_fence_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )?;
    if let Some(existing) = existing.as_ref() {
        if ownership_idempotency_matches(
            existing,
            "acquire",
            &request.idempotency_key,
            &request.owner,
        ) && existing.is_active_unexpired(request.now_nanos)
        {
            return ownership_fence_plan(existing.clone(), Some(existing), true);
        }
        // Time only admits planning a successor. Certification uses the exact
        // prior row predicate produced below.
        if existing.is_active_unexpired(request.now_nanos) {
            bail!("{OWNERSHIP_HELD}: ownership fence is held by an active principal");
        }
    }
    let record = OwnershipFenceRecord {
        format_version: existing
            .as_ref()
            .map(|record| record.format_version)
            .unwrap_or(2),
        resource: request.resource,
        owner: request.owner.clone(),
        fence: existing
            .as_ref()
            .map(|record| increment_counter(record.fence, "ownership fence token"))
            .transpose()?
            .unwrap_or(1),
        state: OwnershipFenceState::Active,
        lease_expires_at_nanos: request.now_nanos.saturating_add(request.ttl_nanos),
        last_heartbeat_at_nanos: request.now_nanos,
        generation: existing
            .as_ref()
            .map(|record| increment_counter(record.generation, "ownership fence generation"))
            .transpose()?
            .unwrap_or(1),
        last_operation: Some("acquire".to_string()),
        last_idempotency_key: nonempty_idempotency_key(request.idempotency_key),
        last_actor: Some(request.owner),
        ownership_hash: None,
        ownership_signature: None,
    }
    .seal(signing_key)?;
    ownership_fence_plan(record, existing.as_ref(), false)
}

pub fn plan_renew_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: RenewOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceWritePlan> {
    validate_renew_ownership(&request)?;
    let mut record = require_ownership_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )?;
    require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
    if !record.is_active_unexpired(request.now_nanos) {
        bail!("{OWNERSHIP_EXPIRED}: ownership fence is not active");
    }
    let expected = record.clone();
    record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.generation = increment_counter(record.generation, "ownership fence generation")?;
    record.last_operation = Some("renew".to_string());
    record.last_idempotency_key = None;
    record.last_actor = Some(request.owner);
    ownership_fence_plan(record.seal(signing_key)?, Some(&expected), false)
}

pub fn plan_transfer_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: TransferOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceWritePlan> {
    validate_transfer_ownership(&request)?;
    if request.new_owner.tenant_id != request.current_owner.tenant_id {
        bail!("{OWNERSHIP_OWNER_MISMATCH}: transfer target is outside the owner tenant");
    }
    let mut record = require_ownership_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.current_owner.tenant_id,
        &request.resource,
        signing_key,
    )?;
    if ownership_idempotency_matches(
        &record,
        "transfer",
        &request.idempotency_key,
        &request.current_owner,
    ) {
        return ownership_fence_plan(record.clone(), Some(&record), true);
    }
    require_current_owner_and_fence(&record, &request.current_owner, request.current_fence)?;
    if !record.is_active_unexpired(request.now_nanos) {
        bail!("{OWNERSHIP_EXPIRED}: ownership fence is not active");
    }
    let expected = record.clone();
    record.fence = increment_counter(record.fence, "ownership fence token")?;
    record.generation = increment_counter(record.generation, "ownership fence generation")?;
    record.owner = request.new_owner;
    record.state = OwnershipFenceState::Active;
    record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("transfer".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.current_owner);
    ownership_fence_plan(record.seal(signing_key)?, Some(&expected), false)
}

pub fn plan_release_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: ReleaseOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceWritePlan> {
    validate_release_ownership(&request)?;
    let mut record = require_ownership_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )?;
    if ownership_idempotency_matches(&record, "release", &request.idempotency_key, &request.owner) {
        return ownership_fence_plan(record.clone(), Some(&record), true);
    }
    if !request.administrative_force {
        require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
    }
    let expected = record.clone();
    record.fence = increment_counter(record.fence, "ownership fence token")?;
    record.generation = increment_counter(record.generation, "ownership fence generation")?;
    record.state = OwnershipFenceState::Released;
    record.lease_expires_at_nanos = request.now_nanos;
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("release".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.owner);
    ownership_fence_plan(record.seal(signing_key)?, Some(&expected), false)
}

pub fn plan_force_expire_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: ForceExpireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceWritePlan> {
    validate_force_expire_ownership(&request)?;
    let mut record = require_ownership_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.admin.tenant_id,
        &request.resource,
        signing_key,
    )?;
    if ownership_idempotency_matches(
        &record,
        "force_expire",
        &request.idempotency_key,
        &request.admin,
    ) {
        return ownership_fence_plan(record.clone(), Some(&record), true);
    }
    let expected = record.clone();
    record.fence = increment_counter(record.fence, "ownership fence token")?;
    record.generation = increment_counter(record.generation, "ownership fence generation")?;
    record.state = OwnershipFenceState::Expired;
    record.lease_expires_at_nanos = request.now_nanos;
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("force_expire".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.admin);
    ownership_fence_plan(record.seal(signing_key)?, Some(&expected), false)
}

fn require_ownership_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<OwnershipFenceRecord> {
    read_ownership_fence_in_transaction(
        mvcc,
        transaction_id,
        principal,
        tenant_id,
        resource,
        signing_key,
    )?
    .ok_or_else(|| anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"))
}

fn read_ownership_fence_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    let key = ownership_fence_logical_key(tenant_id, resource)?;
    mvcc.read_transaction_value(transaction_id, principal, &key)?
        .map(|payload| {
            let record = decode_ownership_fence_record(&payload)?;
            record.verify(signing_key)?;
            Ok(record)
        })
        .transpose()
}

fn ownership_fence_logical_key(
    tenant_id: i64,
    resource: &OwnershipResource,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_LEASES_FENCES,
        TABLE_OWNERSHIP_FENCE_ROW,
        &ownership_fence_row_key(tenant_id, resource)?,
    )
}

fn ownership_fence_projection_logical_key(
    record: &OwnershipFenceRecord,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_LEASES_FENCES,
        TABLE_OWNERSHIP_FENCE_ROW,
        &ownership_fence_by_node_key(record)?,
    )
}

fn ownership_fence_plan(
    record: OwnershipFenceRecord,
    expected: Option<&OwnershipFenceRecord>,
    idempotent_replay: bool,
) -> Result<OwnershipFenceWritePlan> {
    let key = ownership_fence_logical_key(record.owner.tenant_id, &record.resource)?;
    let payload = encode_ownership_fence_record(&record)?;
    let main_predicate = expected
        .map(encode_ownership_fence_record)
        .transpose()?
        .map(|payload| {
            crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes())
        })
        .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
    let mut mutations = Vec::new();
    let mut predicates = vec![(key.clone(), main_predicate)];
    if !idempotent_replay {
        mutations.push(crate::mvcc_product::ProductMutation::put(
            key,
            payload.clone(),
        ));
    }
    let old_projection = expected
        .filter(|record| record.owner.principal_kind == "node")
        .map(ownership_fence_projection_logical_key)
        .transpose()?;
    let new_projection = (record.owner.principal_kind == "node")
        .then(|| ownership_fence_projection_logical_key(&record))
        .transpose()?;
    if old_projection != new_projection {
        if let Some(old_key) = old_projection {
            if !idempotent_replay {
                mutations.push(crate::mvcc_product::ProductMutation::delete(
                    old_key.clone(),
                ));
            }
            let old_payload = encode_ownership_fence_record(
                expected.ok_or_else(|| anyhow!("old ownership projection lacks prior row"))?,
            )?;
            predicates.push((
                old_key,
                crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&old_payload).as_bytes(),
                ),
            ));
        }
        if let Some(new_key) = new_projection {
            if !idempotent_replay {
                mutations.push(crate::mvcc_product::ProductMutation::put(
                    new_key.clone(),
                    payload,
                ));
            }
            predicates.push((new_key, crate::mvcc_transaction::PredicateKind::Absent));
        }
    } else if let Some(projection_key) = new_projection {
        let projection_predicate = expected
            .map(encode_ownership_fence_record)
            .transpose()?
            .map(|payload| {
                crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&payload).as_bytes(),
                )
            })
            .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
        if !idempotent_replay {
            mutations.push(crate::mvcc_product::ProductMutation::put(
                projection_key.clone(),
                payload,
            ));
        }
        predicates.push((projection_key, projection_predicate));
    }
    Ok(OwnershipFenceWritePlan {
        assignment_identity: ownership_resource_hash(record.owner.tenant_id, &record.resource)?,
        outcome: OwnershipFenceOutcome {
            record,
            idempotent_replay,
        },
        mutations,
        predicates,
    })
}

pub async fn acquire_ownership(
    storage: &Storage,
    request: AcquireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_acquire_ownership(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let existing = read_ownership_fence_state(
            storage,
            request.owner.tenant_id,
            &request.resource,
            signing_key,
        )
        .await?;
        let existing_record = existing.as_ref().map(|(_, record)| record);
        if let Some(existing) = existing_record {
            if ownership_idempotency_matches(
                existing,
                "acquire",
                &request.idempotency_key,
                &request.owner,
            ) && existing.is_active_unexpired(request.now_nanos)
            {
                return Ok(OwnershipFenceOutcome {
                    record: existing.clone(),
                    idempotent_replay: true,
                });
            }
            if existing.is_active_unexpired(request.now_nanos) {
                return Err(anyhow!(
                    "{OWNERSHIP_HELD}: ownership fence is held by an active principal"
                ));
            }
        }

        let fence = match existing_record {
            Some(record) => increment_counter(record.fence, "ownership fence token")?,
            None => 1,
        };
        let generation = match existing_record {
            Some(record) => increment_counter(record.generation, "ownership fence generation")?,
            None => 1,
        };
        let record = OwnershipFenceRecord {
            format_version: existing_record
                .map(|record| record.format_version)
                .unwrap_or(2),
            resource: request.resource.clone(),
            owner: request.owner.clone(),
            fence,
            state: OwnershipFenceState::Active,
            lease_expires_at_nanos: request.now_nanos.saturating_add(request.ttl_nanos),
            last_heartbeat_at_nanos: request.now_nanos,
            generation,
            last_operation: Some("acquire".to_string()),
            last_idempotency_key: nonempty_idempotency_key(request.idempotency_key.clone()),
            last_actor: Some(request.owner.clone()),
            ownership_hash: None,
            ownership_signature: None,
        }
        .seal(signing_key)?;
        match write_ownership_fence_state(
            storage,
            &record,
            existing.as_ref().map(|(ref_value, _)| ref_value),
        )
        .await
        {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence CAS retries exhausted"
    ))
}

pub async fn renew_ownership(
    storage: &Storage,
    request: RenewOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_renew_ownership(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut record)) = read_ownership_fence_state(
            storage,
            request.owner.tenant_id,
            &request.resource,
            signing_key,
        )
        .await?
        else {
            return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
        };
        require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
        if !record.is_active_unexpired(request.now_nanos) {
            return Err(anyhow!(
                "{OWNERSHIP_EXPIRED}: ownership fence is not active"
            ));
        }
        record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
        record.last_heartbeat_at_nanos = request.now_nanos;
        record.generation = increment_counter(record.generation, "ownership fence generation")?;
        record.last_operation = Some("renew".to_string());
        record.last_idempotency_key = None;
        record.last_actor = Some(request.owner.clone());
        record = record.seal(signing_key)?;
        match write_ownership_fence_state(storage, &record, Some(&ref_value)).await {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence renew CAS retries exhausted"
    ))
}

pub async fn transfer_ownership(
    storage: &Storage,
    request: TransferOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_transfer_ownership(&request)?;
    if request.new_owner.tenant_id != request.current_owner.tenant_id {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: transfer target is outside the owner tenant"
        ));
    }
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut record)) = read_ownership_fence_state(
            storage,
            request.current_owner.tenant_id,
            &request.resource,
            signing_key,
        )
        .await?
        else {
            return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
        };
        if ownership_idempotency_matches(
            &record,
            "transfer",
            &request.idempotency_key,
            &request.current_owner,
        ) {
            return Ok(OwnershipFenceOutcome {
                record,
                idempotent_replay: true,
            });
        }
        require_current_owner_and_fence(&record, &request.current_owner, request.current_fence)?;
        if !record.is_active_unexpired(request.now_nanos) {
            return Err(anyhow!(
                "{OWNERSHIP_EXPIRED}: ownership fence is not active"
            ));
        }

        record.state = OwnershipFenceState::Transferring;
        record.fence = increment_counter(record.fence, "ownership fence token")?;
        record.generation = increment_counter(record.generation, "ownership fence generation")?;
        record.owner = request.new_owner.clone();
        record.state = OwnershipFenceState::Active;
        record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
        record.last_heartbeat_at_nanos = request.now_nanos;
        record.last_operation = Some("transfer".to_string());
        record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key.clone());
        record.last_actor = Some(request.current_owner.clone());
        record = record.seal(signing_key)?;
        match write_ownership_fence_state(storage, &record, Some(&ref_value)).await {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence transfer CAS retries exhausted"
    ))
}

pub async fn release_ownership(
    storage: &Storage,
    request: ReleaseOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_release_ownership(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut record)) = read_ownership_fence_state(
            storage,
            request.owner.tenant_id,
            &request.resource,
            signing_key,
        )
        .await?
        else {
            return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
        };
        if ownership_idempotency_matches(
            &record,
            "release",
            &request.idempotency_key,
            &request.owner,
        ) {
            return Ok(OwnershipFenceOutcome {
                record,
                idempotent_replay: true,
            });
        }
        if !request.administrative_force {
            require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
        }
        record.fence = increment_counter(record.fence, "ownership fence token")?;
        record.generation = increment_counter(record.generation, "ownership fence generation")?;
        record.state = OwnershipFenceState::Released;
        record.lease_expires_at_nanos = request.now_nanos;
        record.last_heartbeat_at_nanos = request.now_nanos;
        record.last_operation = Some("release".to_string());
        record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key.clone());
        record.last_actor = Some(request.owner.clone());
        record = record.seal(signing_key)?;
        match write_ownership_fence_state(storage, &record, Some(&ref_value)).await {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence release CAS retries exhausted"
    ))
}

pub async fn force_expire_ownership(
    storage: &Storage,
    request: ForceExpireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_force_expire_ownership(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut record)) = read_ownership_fence_state(
            storage,
            request.admin.tenant_id,
            &request.resource,
            signing_key,
        )
        .await?
        else {
            return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
        };
        if ownership_idempotency_matches(
            &record,
            "force_expire",
            &request.idempotency_key,
            &request.admin,
        ) {
            return Ok(OwnershipFenceOutcome {
                record,
                idempotent_replay: true,
            });
        }
        record.fence = increment_counter(record.fence, "ownership fence token")?;
        record.generation = increment_counter(record.generation, "ownership fence generation")?;
        record.state = OwnershipFenceState::Expired;
        record.lease_expires_at_nanos = request.now_nanos;
        record.last_heartbeat_at_nanos = request.now_nanos;
        record.last_operation = Some("force_expire".to_string());
        record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key.clone());
        record.last_actor = Some(request.admin.clone());
        record = record.seal(signing_key)?;
        match write_ownership_fence_state(storage, &record, Some(&ref_value)).await {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence force-expire CAS retries exhausted"
    ))
}

pub async fn read_ownership_fence(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    Ok(
        read_ownership_fence_state(storage, tenant_id, resource, signing_key)
            .await?
            .map(|(_, record)| record),
    )
}

pub fn read_ownership_fence_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    Ok(
        read_ownership_fence_state_mvcc(mvcc, tenant_id, resource, signing_key)?
            .map(|(_, record)| record),
    )
}

pub fn ownership_fence_predicate_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    resource: &OwnershipResource,
    expected_owner: &OwnershipPrincipal,
    expected_fence: u64,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<(
    crate::mvcc_transaction::LogicalKey,
    crate::mvcc_transaction::PredicateKind,
)> {
    if now_nanos < 0 {
        bail!("ownership fence validation timestamp must be nonnegative");
    }
    let Some((payload, record)) =
        read_ownership_fence_state_mvcc(mvcc, tenant_id, resource, signing_key)?
    else {
        bail!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent");
    };
    if !record.is_active_unexpired(now_nanos) {
        bail!("{OWNERSHIP_EXPIRED}: ownership fence is not active");
    }
    if !record.owner.same_security_owner(expected_owner) {
        bail!("{OWNERSHIP_OWNER_MISMATCH}: ownership fence owner mismatch");
    }
    if record.fence != expected_fence {
        bail!("{OWNERSHIP_STALE_FENCE}: ownership fence token mismatch");
    }
    Ok((
        ownership_fence_logical_key(tenant_id, resource)?,
        crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()),
    ))
}

/// Returns an exact CoreMeta CAS fence for an active ownership lease.
///
/// Callers include this precondition in the same mutation batch as the
/// authoritative state they publish. A validate-then-write check alone cannot
/// prevent a stale owner from committing after ownership changes.
pub async fn ownership_fence_precondition(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    expected_owner: &OwnershipPrincipal,
    expected_fence: u64,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    if now_nanos < 0 {
        bail!("ownership fence validation timestamp must be nonnegative");
    }
    let Some((payload, record)) =
        read_ownership_fence_state(storage, tenant_id, resource, signing_key).await?
    else {
        bail!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent");
    };
    if !record.is_active_unexpired(now_nanos) {
        bail!("{OWNERSHIP_EXPIRED}: ownership fence is not active");
    }
    if !record.owner.same_security_owner(expected_owner) {
        bail!("{OWNERSHIP_OWNER_MISMATCH}: ownership fence owner mismatch");
    }
    if record.fence != expected_fence {
        bail!("{OWNERSHIP_STALE_FENCE}: ownership fence token mismatch");
    }
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_OWNERSHIP_FENCE_ROW,
        tuple_key: ownership_fence_row_key(tenant_id, resource)?,
        expected_payload_hash: Some(core_meta_payload_digest(
            TABLE_OWNERSHIP_FENCE_ROW,
            &payload,
        )),
        require_absent: false,
        require_present: true,
    })
}
