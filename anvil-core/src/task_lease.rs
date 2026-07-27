use crate::{
    core_store::{CF_LEASES_FENCES, CoreMetaTuplePart, TABLE_TASK_LEASE_ROW, core_meta_tuple_key},
    formats::hash32,
};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use serde::{Deserialize, Serialize};

pub const LEASE_HELD: &str = "LeaseHeld";
pub const LEASE_EXPIRED: &str = "LeaseExpired";
pub const STALE_FENCE: &str = "StaleFence";
pub const LEASE_OWNER_MISMATCH: &str = "LeaseOwnerMismatch";
pub const LEASE_CAS_CONFLICT: &str = "LeaseCasConflict";

const LOCK_RETRY_ATTEMPTS: usize = 200;
const TASK_LEASE_ROW_PREFIX: &str = "task_lease";
const TASK_LEASE_OWNER_PREFIX: &str = "task_lease_owner";
const TASK_LEASE_LIST_PAGE_MAX: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLeaseOwner {
    pub tenant_id: i64,
    pub principal_kind: String,
    pub principal_id: String,
    pub actor_instance_id: String,
    pub display_name: String,
}

impl TaskLeaseOwner {
    pub fn node(owner_node_id: impl Into<String>) -> Self {
        let owner_node_id = owner_node_id.into();
        Self::node_instance(owner_node_id.clone(), owner_node_id)
    }

    pub fn node_instance(
        owner_node_id: impl Into<String>,
        actor_instance_id: impl Into<String>,
    ) -> Self {
        let owner_node_id = owner_node_id.into();
        Self {
            tenant_id: 0,
            principal_kind: "node".to_string(),
            principal_id: owner_node_id.clone(),
            actor_instance_id: actor_instance_id.into(),
            display_name: owner_node_id,
        }
    }

    pub fn same_security_owner(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.principal_kind == other.principal_kind
            && self.principal_id == other.principal_id
            && self.actor_instance_id == other.actor_instance_id
    }
}

#[derive(Clone, PartialEq, Message)]
struct TaskLeaseOwnerProto {
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
}

#[derive(Clone, PartialEq, Message)]
struct TaskLeaseRecordProto {
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(string, tag = "3")]
    task_id: String,
    #[prost(string, tag = "4")]
    task_kind: String,
    #[prost(string, tag = "5")]
    partition_family: String,
    #[prost(string, tag = "6")]
    partition_id: String,
    #[prost(message, optional, tag = "7")]
    owner: Option<TaskLeaseOwnerProto>,
    #[prost(uint64, tag = "8")]
    fence_token: u64,
    #[prost(bytes, tag = "9")]
    source_cursor_be: Vec<u8>,
    #[prost(bytes, tag = "10")]
    checkpoint_cursor_be: Vec<u8>,
    #[prost(uint64, tag = "11")]
    lease_epoch: u64,
    #[prost(int64, tag = "12")]
    acquired_at_nanos: i64,
    #[prost(int64, tag = "13")]
    expires_at_nanos: i64,
    #[prost(int64, tag = "14")]
    updated_at_nanos: i64,
    #[prost(string, optional, tag = "15")]
    lease_hash: Option<String>,
    #[prost(string, optional, tag = "16")]
    lease_signature: Option<String>,
    #[prost(uint64, tag = "17")]
    root_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLease {
    pub format_version: u16,
    pub root_generation: u64,
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner: TaskLeaseOwner,
    pub fence_token: u64,
    pub source_cursor: u128,
    pub checkpoint_cursor: u128,
    pub lease_epoch: u64,
    pub acquired_at_nanos: i64,
    pub expires_at_nanos: i64,
    pub updated_at_nanos: i64,
    pub lease_hash: Option<String>,
    pub lease_signature: Option<String>,
}

impl TaskLease {
    pub fn owner_node_id(&self) -> &str {
        &self.owner.display_name
    }

    pub fn seal(mut self, _signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_lease(&self)?;
        self.lease_hash = Some(hash_task_lease(&self)?);
        // Cluster-local lease authority is the certified MVCC row plus its
        // compact-Raft assignment guard. A second per-record MAC adds no
        // authority and was removed from the active path.
        self.lease_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _signing_key: &[u8]) -> Result<()> {
        validate_unsigned_lease(self)?;
        let expected_hash = hash_task_lease(self)?;
        if self.lease_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("task lease hash mismatch"));
        }
        Ok(())
    }

    pub fn require_expected_version(
        &self,
        fence_token: u64,
        root_generation: u64,
        lease_epoch: u64,
        expires_at_nanos: i64,
        lease_hash: &str,
    ) -> Result<()> {
        if lease_hash.is_empty()
            || self.fence_token != fence_token
            || self.root_generation != root_generation
            || self.lease_epoch != lease_epoch
            || self.expires_at_nanos != expires_at_nanos
            || self.lease_hash.as_deref() != Some(lease_hash)
        {
            return Err(anyhow!(
                "{STALE_FENCE}: task lease version expectation does not match"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLeaseAcquire {
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner: TaskLeaseOwner,
    pub source_cursor: u128,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLeasePage {
    pub leases: Vec<TaskLease>,
    pub next_tuple_key: Option<Vec<u8>>,
    pub snapshot_version: u64,
}

/// One cluster-local lease transition ready to join either a caller-owned
/// transaction or the implicit single-operation transaction.
///
/// `now_nanos` is deliberately absent from the certified predicates.  Time is
/// used while planning a successor claim, never as authority in the
/// deterministic state machine.
#[derive(Debug, Clone)]
pub struct TaskLeaseWritePlan {
    pub lease: TaskLease,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    assignment_identity: String,
}

impl TaskLeaseWritePlan {
    pub async fn stage_into_transaction(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        mvcc.open_transactions.binding(transaction_id, principal)?;
        let assignment = mvcc
            .reconcile_work_assignment("task-lease", &self.assignment_identity)
            .await?
            .ok_or_else(|| anyhow!("local node does not own the task lease assignment"))?;
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for (key, predicate) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, predicate, now_unix_ms)?;
        }
        mvcc.stage_assignment_guard(transaction_id, principal, &assignment, now_unix_ms)
    }
}

pub fn hash_task_lease(lease: &TaskLease) -> Result<String> {
    let mut unsigned = lease.clone();
    unsigned.lease_hash = None;
    unsigned.lease_signature = None;
    Ok(hex::encode(hash32(
        &task_lease_to_proto(&unsigned).encode_to_vec(),
    )))
}

fn decode_task_lease_record(bytes: &[u8]) -> Result<TaskLease> {
    let proto = TaskLeaseRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("task lease record is not deterministic protobuf");
    }
    task_lease_from_proto(proto)
}

fn encode_task_lease_record(lease: &TaskLease) -> Result<Vec<u8>> {
    validate_unsigned_lease(lease)?;
    Ok(task_lease_to_proto(lease).encode_to_vec())
}

fn task_lease_to_proto(lease: &TaskLease) -> TaskLeaseRecordProto {
    TaskLeaseRecordProto {
        format_version: u32::from(lease.format_version),
        task_id: lease.task_id.clone(),
        task_kind: lease.task_kind.clone(),
        partition_family: lease.partition_family.clone(),
        partition_id: lease.partition_id.clone(),
        owner: Some(task_lease_owner_to_proto(&lease.owner)),
        fence_token: lease.fence_token,
        source_cursor_be: lease.source_cursor.to_be_bytes().to_vec(),
        checkpoint_cursor_be: lease.checkpoint_cursor.to_be_bytes().to_vec(),
        lease_epoch: lease.lease_epoch,
        acquired_at_nanos: lease.acquired_at_nanos,
        expires_at_nanos: lease.expires_at_nanos,
        updated_at_nanos: lease.updated_at_nanos,
        lease_hash: lease.lease_hash.clone(),
        lease_signature: lease.lease_signature.clone(),
        root_generation: lease.root_generation,
    }
}

fn task_lease_from_proto(proto: TaskLeaseRecordProto) -> Result<TaskLease> {
    Ok(TaskLease {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("task lease format version exceeds u16"))?,
        root_generation: proto.root_generation,
        task_id: proto.task_id,
        task_kind: proto.task_kind,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        owner: task_lease_owner_from_proto(
            proto
                .owner
                .ok_or_else(|| anyhow!("task lease record is missing owner"))?,
        ),
        fence_token: proto.fence_token,
        source_cursor: u128_from_be(&proto.source_cursor_be, "source_cursor")?,
        checkpoint_cursor: u128_from_be(&proto.checkpoint_cursor_be, "checkpoint_cursor")?,
        lease_epoch: proto.lease_epoch,
        acquired_at_nanos: proto.acquired_at_nanos,
        expires_at_nanos: proto.expires_at_nanos,
        updated_at_nanos: proto.updated_at_nanos,
        lease_hash: proto.lease_hash,
        lease_signature: proto.lease_signature,
    })
}

fn task_lease_owner_to_proto(owner: &TaskLeaseOwner) -> TaskLeaseOwnerProto {
    TaskLeaseOwnerProto {
        tenant_id: owner.tenant_id,
        principal_kind: owner.principal_kind.clone(),
        principal_id: owner.principal_id.clone(),
        actor_instance_id: owner.actor_instance_id.clone(),
        display_name: owner.display_name.clone(),
    }
}

fn task_lease_owner_from_proto(proto: TaskLeaseOwnerProto) -> TaskLeaseOwner {
    TaskLeaseOwner {
        tenant_id: proto.tenant_id,
        principal_kind: proto.principal_kind,
        principal_id: proto.principal_id,
        actor_instance_id: proto.actor_instance_id,
        display_name: proto.display_name,
    }
}

fn u128_from_be(bytes: &[u8], field: &str) -> Result<u128> {
    let array: [u8; 16] = bytes
        .try_into()
        .map_err(|_| anyhow!("task lease {field} must be 16 bytes"))?;
    Ok(u128::from_be_bytes(array))
}

pub async fn acquire_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: TaskLeaseAcquire,
    signing_key: &[u8],
) -> Result<TaskLease> {
    validate_acquire_request(&request)?;
    let existing =
        read_task_lease_mvcc(mvcc, request.owner.tenant_id, &request.task_id, signing_key)?;
    if let Some(existing) = existing.as_ref()
        && existing.expires_at_nanos > request.now_nanos
        && !existing.owner.same_security_owner(&request.owner)
    {
        return Err(anyhow!(
            "{LEASE_HELD}: task lease is owned by another active principal"
        ));
    }
    let fence_token = existing
        .as_ref()
        .map(|lease| {
            lease
                .fence_token
                .checked_add(1)
                .context("task lease fence token overflow")
        })
        .transpose()?
        .unwrap_or(1);
    let lease_epoch = existing
        .as_ref()
        .map(|lease| {
            lease
                .lease_epoch
                .checked_add(1)
                .context("task lease epoch overflow")
        })
        .transpose()?
        .unwrap_or(1);
    let root_generation = existing
        .as_ref()
        .map(|lease| {
            lease
                .root_generation
                .checked_add(1)
                .context("task lease root generation overflow")
        })
        .transpose()?
        .unwrap_or(1);
    let checkpoint_cursor = existing
        .as_ref()
        .map(|lease| lease.checkpoint_cursor)
        .unwrap_or(0)
        .max(request.source_cursor);
    let lease = TaskLease {
        format_version: 3,
        root_generation,
        task_id: request.task_id.clone(),
        task_kind: request.task_kind,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner: request.owner,
        fence_token,
        source_cursor: request.source_cursor,
        checkpoint_cursor,
        lease_epoch,
        acquired_at_nanos: request.now_nanos,
        expires_at_nanos: request
            .now_nanos
            .checked_add(request.ttl_nanos)
            .context("task lease expiry overflow")?,
        updated_at_nanos: request.now_nanos,
        lease_hash: None,
        lease_signature: None,
    }
    .seal(signing_key)?;
    write_task_lease_mvcc(mvcc, &lease, existing.as_ref()).await?;
    Ok(lease)
}

pub async fn plan_acquire_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    request: TaskLeaseAcquire,
    signing_key: &[u8],
) -> Result<TaskLeaseWritePlan> {
    validate_acquire_request(&request)?;
    let existing = read_task_lease_in_transaction(
        mvcc,
        transaction_id,
        principal,
        request.owner.tenant_id,
        &request.task_id,
        signing_key,
    )?;
    // Expiry permits planning a successor.  The exact prior-value predicate
    // below is the authority check which eventually certifies.
    if let Some(existing) = existing.as_ref()
        && existing.expires_at_nanos > request.now_nanos
        && !existing.owner.same_security_owner(&request.owner)
    {
        bail!("{LEASE_HELD}: task lease is owned by another active principal");
    }
    let fence_token = successor_counter(
        existing.as_ref().map(|lease| lease.fence_token),
        "task lease fence token",
    )?;
    let lease_epoch = successor_counter(
        existing.as_ref().map(|lease| lease.lease_epoch),
        "task lease epoch",
    )?;
    let root_generation = successor_counter(
        existing.as_ref().map(|lease| lease.root_generation),
        "task lease root generation",
    )?;
    let checkpoint_cursor = existing
        .as_ref()
        .map(|lease| lease.checkpoint_cursor)
        .unwrap_or_default()
        .max(request.source_cursor);
    let lease = TaskLease {
        format_version: 3,
        root_generation,
        task_id: request.task_id,
        task_kind: request.task_kind,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner: request.owner,
        fence_token,
        source_cursor: request.source_cursor,
        checkpoint_cursor,
        lease_epoch,
        acquired_at_nanos: request.now_nanos,
        expires_at_nanos: request
            .now_nanos
            .checked_add(request.ttl_nanos)
            .context("task lease expiry overflow")?,
        updated_at_nanos: request.now_nanos,
        lease_hash: None,
        lease_signature: None,
    }
    .seal(signing_key)?;
    task_lease_put_plan(&lease, existing.as_ref())
}

#[allow(clippy::too_many_arguments)]
pub fn plan_checkpoint_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    authenticated_owner: &TaskLeaseOwner,
    task_id: &str,
    fence_token: u64,
    root_generation: u64,
    lease_epoch: u64,
    expires_at_nanos: i64,
    lease_hash: &str,
    checkpoint_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLeaseWritePlan> {
    let mut lease = require_expected_task_lease_in_transaction(
        mvcc,
        transaction_id,
        principal,
        authenticated_owner,
        task_id,
        fence_token,
        root_generation,
        lease_epoch,
        expires_at_nanos,
        lease_hash,
        now_nanos,
        signing_key,
    )?;
    if checkpoint_cursor < lease.checkpoint_cursor {
        bail!("{STALE_FENCE}: task lease checkpoint cannot move backwards");
    }
    let expected = lease.clone();
    if checkpoint_cursor != lease.checkpoint_cursor {
        lease.root_generation = lease
            .root_generation
            .checked_add(1)
            .context("task lease root generation overflow")?;
        lease.checkpoint_cursor = checkpoint_cursor;
        lease.updated_at_nanos = now_nanos;
        lease = lease.seal(signing_key)?;
    }
    task_lease_put_plan(&lease, Some(&expected))
}

#[allow(clippy::too_many_arguments)]
pub fn plan_commit_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    authenticated_owner: &TaskLeaseOwner,
    task_id: &str,
    fence_token: u64,
    root_generation: u64,
    lease_epoch: u64,
    expires_at_nanos: i64,
    lease_hash: &str,
    committed_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLeaseWritePlan> {
    let mut lease = require_expected_task_lease_in_transaction(
        mvcc,
        transaction_id,
        principal,
        authenticated_owner,
        task_id,
        fence_token,
        root_generation,
        lease_epoch,
        expires_at_nanos,
        lease_hash,
        now_nanos,
        signing_key,
    )?;
    if committed_cursor < lease.checkpoint_cursor {
        bail!("{STALE_FENCE}: task lease commit cannot move backwards");
    }
    let expected = lease.clone();
    lease.root_generation = lease
        .root_generation
        .checked_add(1)
        .context("task lease root generation overflow")?;
    lease.checkpoint_cursor = committed_cursor;
    lease.updated_at_nanos = now_nanos;
    let committed = lease.seal(signing_key)?;
    let mut plan = task_lease_delete_plan(&expected)?;
    plan.lease = committed;
    Ok(plan)
}

pub fn plan_force_release_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLeaseWritePlan>> {
    read_task_lease_in_transaction(
        mvcc,
        transaction_id,
        principal,
        tenant_id,
        task_id,
        signing_key,
    )?
    .as_ref()
    .map(task_lease_delete_plan)
    .transpose()
}

fn successor_counter(current: Option<u64>, label: &str) -> Result<u64> {
    current
        .map(|value| value.checked_add(1).context(format!("{label} overflow")))
        .transpose()
        .map(|value| value.unwrap_or(1))
}

fn read_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    let payload = mvcc.read_transaction_value(
        transaction_id,
        principal,
        &task_lease_mvcc_key(tenant_id, task_id)?,
    )?;
    payload
        .map(|payload| {
            let lease = decode_task_lease_record(&payload)?;
            lease.verify(signing_key)?;
            Ok(lease)
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
fn require_expected_task_lease_in_transaction(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    owner: &TaskLeaseOwner,
    task_id: &str,
    fence_token: u64,
    root_generation: u64,
    lease_epoch: u64,
    expires_at_nanos: i64,
    lease_hash: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let lease = read_task_lease_in_transaction(
        mvcc,
        transaction_id,
        principal,
        owner.tenant_id,
        task_id,
        signing_key,
    )?
    .ok_or_else(|| anyhow!("{STALE_FENCE}: task lease does not exist"))?;
    if !lease.owner.same_security_owner(owner) {
        bail!("{LEASE_OWNER_MISMATCH}: task lease owner mismatch");
    }
    lease.require_expected_version(
        fence_token,
        root_generation,
        lease_epoch,
        expires_at_nanos,
        lease_hash,
    )?;
    if lease.expires_at_nanos <= now_nanos {
        bail!("{LEASE_EXPIRED}: task lease expired and is eligible for reclaim");
    }
    Ok(lease)
}

pub fn read_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    read_task_lease_mvcc_at_snapshot(
        mvcc,
        tenant_id,
        task_id,
        signing_key,
        mvcc.runtime.applied_version()?,
    )
}

pub fn read_task_lease_mvcc_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
    snapshot_version: u64,
) -> Result<Option<TaskLease>> {
    let logical_key = task_lease_mvcc_key(tenant_id, task_id)?;
    mvcc.runtime
        .read_at(&logical_key, snapshot_version)?
        .map(|version| version.value)
        .map(|payload| {
            let lease = decode_task_lease_record(&payload)?;
            lease.verify(signing_key)?;
            Ok(lease)
        })
        .transpose()
}

pub async fn renew_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
    now_nanos: i64,
    ttl_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    if ttl_nanos <= 0 {
        bail!("task lease ttl must be positive");
    }
    let mut lease = check_task_lease_mvcc(mvcc, expected, now_nanos, signing_key)?;
    lease.root_generation = lease
        .root_generation
        .checked_add(1)
        .context("task lease root generation overflow")?;
    lease.lease_epoch = lease
        .lease_epoch
        .checked_add(1)
        .context("task lease epoch overflow")?;
    lease.acquired_at_nanos = now_nanos;
    lease.expires_at_nanos = now_nanos
        .checked_add(ttl_nanos)
        .context("task lease expiry overflow")?;
    lease.updated_at_nanos = now_nanos;
    lease = lease.seal(signing_key)?;
    write_task_lease_mvcc(mvcc, &lease, Some(expected)).await?;
    Ok(lease)
}

pub async fn checkpoint_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
    checkpoint_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let mut lease = require_exact_task_lease_mvcc(mvcc, expected, now_nanos, signing_key)?;
    if checkpoint_cursor < lease.checkpoint_cursor {
        bail!("{STALE_FENCE}: task lease checkpoint cannot move backwards");
    }
    if checkpoint_cursor == lease.checkpoint_cursor {
        return Ok(lease);
    }
    lease.root_generation = lease
        .root_generation
        .checked_add(1)
        .context("task lease root generation overflow")?;
    lease.checkpoint_cursor = checkpoint_cursor;
    lease.updated_at_nanos = now_nanos;
    lease = lease.seal(signing_key)?;
    write_task_lease_mvcc(mvcc, &lease, Some(expected)).await?;
    Ok(lease)
}

pub fn require_exact_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
    _now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let current = read_task_lease_mvcc(
        mvcc,
        expected.owner.tenant_id,
        &expected.task_id,
        signing_key,
    )?
    .ok_or_else(|| anyhow!("{STALE_FENCE}: task lease does not exist"))?;
    if current != *expected {
        bail!("{STALE_FENCE}: task lease holder or epoch changed");
    }
    Ok(current)
}

pub fn check_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let current = require_exact_task_lease_mvcc(mvcc, expected, now_nanos, signing_key)?;
    if current.expires_at_nanos <= now_nanos {
        bail!("{LEASE_EXPIRED}: task lease expired and is eligible for reclaim");
    }
    Ok(current)
}

pub fn task_lease_mvcc_key(
    tenant_id: i64,
    task_id: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_LEASES_FENCES,
        TABLE_TASK_LEASE_ROW,
        &task_lease_row_key(tenant_id, task_id)?,
    )
}

pub(crate) fn task_lease_mvcc_predicate(
    lease: &TaskLease,
) -> Result<(
    crate::mvcc_transaction::LogicalKey,
    crate::mvcc_transaction::PredicateKind,
)> {
    Ok((
        task_lease_mvcc_key(lease.owner.tenant_id, &lease.task_id)?,
        crate::mvcc_transaction::PredicateKind::ValueHash(
            *blake3::hash(&encode_task_lease_record(lease)?).as_bytes(),
        ),
    ))
}

async fn write_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    lease: &TaskLease,
    expected: Option<&TaskLease>,
) -> Result<()> {
    let plan = task_lease_put_plan(lease, expected)?;
    let idempotency_key = lease
        .lease_hash
        .as_deref()
        .ok_or_else(|| anyhow!("sealed task lease is missing its hash"))?;
    commit_task_lease_plan(
        mvcc,
        &format!("task-lease:{}", lease.owner.principal_id),
        idempotency_key,
        plan,
        u64::try_from(lease.updated_at_nanos / 1_000_000).unwrap_or_default(),
    )
    .await
}

fn task_lease_put_plan(
    lease: &TaskLease,
    expected: Option<&TaskLease>,
) -> Result<TaskLeaseWritePlan> {
    let key = task_lease_mvcc_key(lease.owner.tenant_id, &lease.task_id)?;
    let owner_key = task_lease_owner_mvcc_key(lease)?;
    let payload = encode_task_lease_record(lease)?;
    let predicate = match expected {
        Some(expected) => crate::mvcc_transaction::PredicateKind::ValueHash(
            *blake3::hash(&encode_task_lease_record(expected)?).as_bytes(),
        ),
        None => crate::mvcc_transaction::PredicateKind::Absent,
    };
    let mut mutations = vec![
        crate::mvcc_product::ProductMutation::put(key.clone(), payload.clone()),
        crate::mvcc_product::ProductMutation::put(owner_key.clone(), payload),
    ];
    let current_owner_tuple = task_lease_owner_key(lease)?;
    let same_owner = expected
        .map(|previous| task_lease_owner_key(previous))
        .transpose()?
        .is_some_and(|previous_owner| previous_owner == current_owner_tuple);
    let owner_predicate = match expected.filter(|_| same_owner) {
        Some(previous) => crate::mvcc_transaction::PredicateKind::ValueHash(
            *blake3::hash(&encode_task_lease_record(previous)?).as_bytes(),
        ),
        None => crate::mvcc_transaction::PredicateKind::Absent,
    };
    let mut predicates = vec![(key, predicate), (owner_key, owner_predicate)];
    if let Some(previous) = expected
        && task_lease_owner_key(previous)? != task_lease_owner_key(lease)?
    {
        let previous_owner_key = task_lease_owner_mvcc_key(previous)?;
        mutations.push(crate::mvcc_product::ProductMutation::delete(
            previous_owner_key.clone(),
        ));
        predicates.push((
            previous_owner_key,
            crate::mvcc_transaction::PredicateKind::ValueHash(
                *blake3::hash(&encode_task_lease_record(previous)?).as_bytes(),
            ),
        ));
    }
    Ok(TaskLeaseWritePlan {
        lease: lease.clone(),
        mutations,
        predicates,
        assignment_identity: format!("{}:{}", lease.owner.tenant_id, lease.task_id),
    })
}

pub async fn commit_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
    committed_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let mut lease = require_exact_task_lease_mvcc(mvcc, expected, now_nanos, signing_key)?;
    if committed_cursor < lease.checkpoint_cursor {
        bail!("{STALE_FENCE}: task lease commit cannot move backwards");
    }
    lease.root_generation = lease
        .root_generation
        .checked_add(1)
        .context("task lease root generation overflow")?;
    lease.checkpoint_cursor = committed_cursor;
    lease.updated_at_nanos = now_nanos;
    let committed = lease.seal(signing_key)?;
    delete_task_lease_mvcc(mvcc, expected).await?;
    Ok(committed)
}

pub async fn force_release_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    let Some(lease) = read_task_lease_mvcc(mvcc, tenant_id, task_id, signing_key)? else {
        return Ok(None);
    };
    delete_task_lease_mvcc(mvcc, &lease).await?;
    Ok(Some(lease))
}

async fn delete_task_lease_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    expected: &TaskLease,
) -> Result<()> {
    let plan = task_lease_delete_plan(expected)?;
    commit_task_lease_plan(
        mvcc,
        &format!("task-lease:{}", expected.owner.principal_id),
        &format!(
            "task-lease-release:{}:{}:{}",
            expected.owner.tenant_id, expected.task_id, expected.lease_epoch
        ),
        plan,
        u64::try_from(expected.updated_at_nanos / 1_000_000).unwrap_or_default(),
    )
    .await
}

fn task_lease_delete_plan(expected: &TaskLease) -> Result<TaskLeaseWritePlan> {
    let key = task_lease_mvcc_key(expected.owner.tenant_id, &expected.task_id)?;
    let owner_key = task_lease_owner_mvcc_key(expected)?;
    let predicate = crate::mvcc_transaction::PredicateKind::ValueHash(
        *blake3::hash(&encode_task_lease_record(expected)?).as_bytes(),
    );
    Ok(TaskLeaseWritePlan {
        lease: expected.clone(),
        mutations: vec![
            crate::mvcc_product::ProductMutation::delete(key.clone()),
            crate::mvcc_product::ProductMutation::delete(owner_key.clone()),
        ],
        predicates: vec![(key, predicate.clone()), (owner_key, predicate)],
        assignment_identity: format!("{}:{}", expected.owner.tenant_id, expected.task_id),
    })
}

async fn commit_task_lease_plan(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    principal: &str,
    idempotency_key: &str,
    plan: TaskLeaseWritePlan,
    now_unix_ms: u64,
) -> Result<()> {
    for attempt in 0..5u8 {
        let assignment = mvcc
            .reconcile_work_assignment("task-lease", &plan.assignment_identity)
            .await?
            .ok_or_else(|| anyhow!("local node does not own the task lease assignment"))?;
        let attempt_key = format!("{idempotency_key}:{attempt}");
        let handle = mvcc
            .open_transactions
            .begin(
                mvcc.runtime.as_ref(),
                mvcc.cluster_id(),
                principal,
                &attempt_key,
                std::time::Duration::from_secs(30),
                crate::mvcc_transaction::DurabilityLevel::Quorum,
                crate::mvcc_transaction::ReadConsistency::Linearized,
                now_unix_ms,
            )
            .await?;
        let status =
            mvcc.open_transactions
                .status(&handle.transaction_id, principal, now_unix_ms)?;
        if status.state == "open" {
            mvcc.stage_product_mutations(
                &handle.transaction_id,
                principal,
                plan.mutations.clone(),
                now_unix_ms,
            )?;
            for (key, kind) in plan.predicates.clone() {
                mvcc.stage_predicate(&handle.transaction_id, principal, key, kind, now_unix_ms)?;
            }
            mvcc.stage_assignment_guard(
                &handle.transaction_id,
                principal,
                &assignment,
                now_unix_ms,
            )?;
        }
        let outcome = match mvcc
            .open_transactions
            .commit(
                mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_unix_ms,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error)
                if attempt < 4
                    && format!("{error:#}")
                        .contains("assignment predicate violates applied Raft control state") =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(
                    25 * u64::from(attempt + 1),
                ))
                .await;
                continue;
            }
            Err(error) => return Err(error),
        };
        match outcome.certification {
            crate::mvcc_transaction::CertificationResult::Committed { .. } => return Ok(()),
            crate::mvcc_transaction::CertificationResult::Aborted { reason }
                if attempt < 4
                    && format!("{reason:?}")
                        .contains("assignment predicate violates applied Raft control state") =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(
                    25 * u64::from(attempt + 1),
                ))
                .await;
            }
            crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                bail!("{LEASE_CAS_CONFLICT}: task lease MVCC transaction aborted: {reason:?}")
            }
        }
    }
    bail!("{LEASE_CAS_CONFLICT}: task lease assignment kept changing")
}

fn task_lease_owner_mvcc_key(lease: &TaskLease) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_LEASES_FENCES,
        TABLE_TASK_LEASE_ROW,
        &task_lease_owner_key(lease)?,
    )
}

pub fn list_active_task_leases_for_node_page_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    limit: usize,
) -> Result<TaskLeasePage> {
    list_active_task_leases_for_node_page_at_snapshot(
        mvcc,
        owner_node_id,
        now_nanos,
        signing_key,
        after_tuple_key,
        limit,
        mvcc.runtime.applied_version()?,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn list_active_task_leases_for_node_page_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    limit: usize,
    snapshot_version: u64,
) -> Result<TaskLeasePage> {
    if !(1..=TASK_LEASE_LIST_PAGE_MAX).contains(&limit) {
        bail!("task lease page limit must be between 1 and {TASK_LEASE_LIST_PAGE_MAX}");
    }
    let tuple_prefix = task_lease_owner_prefix(owner_node_id)?;
    if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(&tuple_prefix)) {
        bail!("task lease page cursor is outside the owner scope");
    }
    let prefix = crate::mvcc_product::coremeta_application_prefix(CF_LEASES_FENCES, &tuple_prefix)?;
    let mut rows =
        mvcc.runtime
            .scan_table_prefix_at(TABLE_TASK_LEASE_ROW, &prefix, snapshot_version)?;
    if let Some(after) = after_tuple_key {
        rows.retain(|(key, _)| {
            crate::mvcc_product::coremeta_tuple_from_logical_key(key, CF_LEASES_FENCES)
                .is_ok_and(|tuple| tuple > after)
        });
    }
    let has_more = rows.len() > limit;
    if has_more {
        rows.truncate(limit);
    }
    let next_tuple_key = if has_more {
        Some(
            crate::mvcc_product::coremeta_tuple_from_logical_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("task lease page lost its final row"))?
                    .0,
                CF_LEASES_FENCES,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    let mut leases = Vec::with_capacity(rows.len());
    for (key, row) in rows {
        let lease = decode_task_lease_record(&row.value)?;
        lease.verify(signing_key)?;
        if lease.owner_node_id() != owner_node_id
            || crate::mvcc_product::coremeta_tuple_from_logical_key(&key, CF_LEASES_FENCES)?
                != task_lease_owner_key(&lease)?
        {
            bail!("task lease owner projection scope mismatch");
        }
        if lease.expires_at_nanos > now_nanos {
            leases.push(lease);
        }
    }
    Ok(TaskLeasePage {
        leases,
        next_tuple_key,
        snapshot_version,
    })
}

fn validate_acquire_request(request: &TaskLeaseAcquire) -> Result<()> {
    require_nonempty(&request.task_id, "task_id")?;
    require_nonempty(&request.task_kind, "task_kind")?;
    require_nonempty(&request.partition_family, "partition_family")?;
    validate_hex32(&request.partition_id, "partition_id")?;
    validate_owner(&request.owner)?;
    if request.ttl_nanos <= 0 {
        return Err(anyhow!("task lease ttl must be positive"));
    }
    if request.now_nanos < 0 {
        return Err(anyhow!("task lease timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_lease(lease: &TaskLease) -> Result<()> {
    if lease.format_version != 3 {
        return Err(anyhow!("unsupported task lease version"));
    }
    require_nonempty(&lease.task_id, "task_id")?;
    require_nonempty(&lease.task_kind, "task_kind")?;
    require_nonempty(&lease.partition_family, "partition_family")?;
    validate_hex32(&lease.partition_id, "partition_id")?;
    validate_owner(&lease.owner)?;
    if lease.root_generation == 0 || lease.fence_token == 0 || lease.lease_epoch == 0 {
        return Err(anyhow!(
            "task lease root generation, fence, and epoch must be nonzero"
        ));
    }
    if lease.expires_at_nanos <= lease.acquired_at_nanos {
        return Err(anyhow!("task lease expiry must be after acquisition"));
    }
    if lease.updated_at_nanos < lease.acquired_at_nanos {
        return Err(anyhow!("task lease update timestamp is before acquisition"));
    }
    Ok(())
}

fn validate_owner(owner: &TaskLeaseOwner) -> Result<()> {
    if owner.tenant_id < 0 {
        return Err(anyhow!("task lease owner tenant_id must be nonnegative"));
    }
    require_nonempty(&owner.principal_kind, "owner.principal_kind")?;
    require_nonempty(&owner.principal_id, "owner.principal_id")?;
    require_nonempty(&owner.actor_instance_id, "owner.actor_instance_id")?;
    require_nonempty(&owner.display_name, "owner.display_name")?;
    Ok(())
}

fn task_lease_owner_prefix(owner_node_id: &str) -> Result<Vec<u8>> {
    require_nonempty(owner_node_id, "owner_node_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(TASK_LEASE_OWNER_PREFIX),
        CoreMetaTuplePart::Utf8(owner_node_id),
    ])
}

fn task_lease_owner_key(lease: &TaskLease) -> Result<Vec<u8>> {
    let mut parts = vec![
        CoreMetaTuplePart::Utf8(TASK_LEASE_OWNER_PREFIX),
        CoreMetaTuplePart::Utf8(lease.owner_node_id()),
        CoreMetaTuplePart::I64(lease.owner.tenant_id),
        CoreMetaTuplePart::Utf8(&lease.task_id),
    ];
    if lease.owner.principal_kind != "node" {
        parts.insert(2, CoreMetaTuplePart::Utf8(&lease.owner.principal_kind));
    }
    core_meta_tuple_key(&parts)
}

fn task_lease_row_key(tenant_id: i64, task_id: &str) -> Result<Vec<u8>> {
    if tenant_id < 0 {
        return Err(anyhow!("task lease tenant id must be nonnegative"));
    }
    require_nonempty(task_id, "task_id")?;
    if task_id.contains('\0') || task_id.contains("..") || task_id.chars().any(char::is_control) {
        return Err(anyhow!("task_id contains an invalid component"));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(TASK_LEASE_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(&format!("tenant:{tenant_id}")),
        CoreMetaTuplePart::Utf8(task_id),
    ])
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}
