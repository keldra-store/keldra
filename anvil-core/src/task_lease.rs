use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart,
        CoreMetaVisibilityState, CoreMutationBatch, CoreMutationOperation,
        CoreMutationPrecondition, CoreMutationRootPublication, CoreStore, TABLE_TASK_LEASE_ROW,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_record_tuple_key,
        core_meta_root_key_hash, core_meta_tuple_key,
    },
    formats::{hash32, writer::WriterFamily},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub const LEASE_HELD: &str = "LeaseHeld";
pub const LEASE_EXPIRED: &str = "LeaseExpired";
pub const STALE_FENCE: &str = "StaleFence";
pub const LEASE_OWNER_MISMATCH: &str = "LeaseOwnerMismatch";
pub const LEASE_CAS_CONFLICT: &str = "LeaseCasConflict";

const LOCK_RETRY_ATTEMPTS: usize = 200;
const TASK_LEASE_ROW_PREFIX: &str = "task_lease";
const TASK_LEASE_OWNER_PREFIX: &str = "task_lease_owner";
const TASK_LEASE_LIST_PAGE_MAX: usize = 1_000;

type HmacSha256 = Hmac<Sha256>;

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
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
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

    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_lease(&self)?;
        let hash = hash_task_lease(&self)?;
        let signature = sign_lease_hash(
            signing_key,
            &hash,
            &[
                &self.task_id,
                &self.owner.tenant_id.to_string(),
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence_token.to_string(),
            ],
        )?;
        self.lease_hash = Some(hash);
        self.lease_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_lease(self)?;
        let expected_hash = hash_task_lease(self)?;
        if self.lease_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("task lease hash mismatch"));
        }
        let expected_signature = sign_lease_hash(
            signing_key,
            &expected_hash,
            &[
                &self.task_id,
                &self.owner.tenant_id.to_string(),
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.lease_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("task lease signature mismatch"));
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
}

pub fn hash_task_lease(lease: &TaskLease) -> Result<String> {
    let mut unsigned = lease.clone();
    unsigned.lease_hash = None;
    unsigned.lease_signature = None;
    Ok(hex::encode(hash32(
        &task_lease_to_proto(&unsigned, None).encode_to_vec(),
    )))
}

fn decode_task_lease_record(bytes: &[u8]) -> Result<TaskLease> {
    let proto = TaskLeaseRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("task lease record is not deterministic protobuf");
    }
    task_lease_from_proto(proto)
}

fn task_lease_to_proto(lease: &TaskLease, transaction_id: Option<&str>) -> TaskLeaseRecordProto {
    TaskLeaseRecordProto {
        common: transaction_id.map(|transaction_id| task_lease_common(lease, transaction_id)),
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
    }
}

fn task_lease_from_proto(proto: TaskLeaseRecordProto) -> Result<TaskLease> {
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("task lease record missing CoreMeta common"))?;
    validate_task_lease_common(&proto, &common)?;
    Ok(TaskLease {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("task lease format version exceeds u16"))?,
        root_generation: common.root_generation,
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

fn task_lease_common(lease: &TaskLease, transaction_id: &str) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("tenant/{}", lease.owner.tenant_id),
        task_lease_root_key_hash(lease.owner.tenant_id, &lease.task_id),
        lease.root_generation,
        transaction_id,
        u64::try_from(lease.updated_at_nanos).unwrap_or_default(),
    )
}

fn validate_task_lease_common(
    proto: &TaskLeaseRecordProto,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    let owner = proto
        .owner
        .as_ref()
        .ok_or_else(|| anyhow!("task lease record is missing owner"))?;
    if common.realm_id != format!("tenant/{}", owner.tenant_id) {
        return Err(anyhow!("task lease CoreMeta realm mismatch"));
    }
    if common.root_key_hash != task_lease_root_key_hash(owner.tenant_id, &proto.task_id) {
        return Err(anyhow!("task lease CoreMeta root mismatch"));
    }
    if common.root_generation == 0 {
        return Err(anyhow!("task lease CoreMeta generation must be nonzero"));
    }
    if !common.transaction_id.starts_with("task-lease:") {
        return Err(anyhow!("task lease CoreMeta transaction mismatch"));
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        return Err(anyhow!("task lease CoreMeta row is not committed"));
    }
    Ok(())
}

fn task_lease_root_key_hash(tenant_id: i64, task_id: &str) -> String {
    core_meta_root_key_hash(&task_lease_root_anchor_key(tenant_id, task_id))
}

fn task_lease_root_anchor_key(tenant_id: i64, task_id: &str) -> String {
    format!("task-lease/tenant/{tenant_id}/task/{task_id}")
}

async fn next_task_lease_root_generation(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    existing: Option<&TaskLease>,
) -> Result<u64> {
    let current = match existing {
        Some(lease) => lease.root_generation,
        None => {
            let root_hash = task_lease_root_key_hash(tenant_id, task_id);
            match CoreStore::new(storage.clone())
                .await?
                .read_internal_root_anchor_by_hash(&root_hash, 0)
                .await
            {
                Ok(root) => root.generation,
                Err(error) if is_missing_root_anchor(&error) => 0,
                Err(error) => {
                    return Err(error).context("read task lease root generation");
                }
            }
        }
    };
    current
        .checked_add(1)
        .ok_or_else(|| anyhow!("task lease root generation overflow"))
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

pub async fn acquire_task_lease(
    storage: &Storage,
    request: TaskLeaseAcquire,
    signing_key: &[u8],
) -> Result<TaskLease> {
    validate_acquire_request(&request)?;
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let existing = match read_task_lease_state(
            storage,
            request.owner.tenant_id,
            &request.task_id,
            signing_key,
        )
        .await
        {
            Ok(existing) => existing,
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        };
        let existing_lease = existing.as_ref().map(|(_, lease)| lease);
        if let Some(existing) = existing_lease
            && existing.expires_at_nanos > request.now_nanos
            && !existing.owner.same_security_owner(&request.owner)
        {
            return Err(anyhow!(
                "{LEASE_HELD}: task lease is owned by another active principal"
            ));
        }

        // Every acquisition starts a new execution incarnation. Renewal is the
        // only operation that preserves a fence; otherwise an older same-owner
        // worker could continue publishing through a reacquired lease.
        let fence_token = existing_lease
            .map(|lease| {
                lease
                    .fence_token
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("task lease fence token overflow"))
            })
            .transpose()?
            .unwrap_or(1);
        let lease_epoch = existing_lease
            .map(|lease| {
                lease
                    .lease_epoch
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("task lease epoch overflow"))
            })
            .transpose()?
            .unwrap_or(1);
        let root_generation = next_task_lease_root_generation(
            storage,
            request.owner.tenant_id,
            &request.task_id,
            existing_lease,
        )
        .await?;
        let checkpoint_cursor = existing_lease
            .map(|lease| lease.checkpoint_cursor)
            .unwrap_or(0)
            .max(request.source_cursor);
        let lease = TaskLease {
            format_version: 3,
            root_generation,
            task_id: request.task_id.clone(),
            task_kind: request.task_kind.clone(),
            partition_family: request.partition_family.clone(),
            partition_id: request.partition_id.clone(),
            owner: request.owner.clone(),
            fence_token,
            source_cursor: request.source_cursor,
            checkpoint_cursor,
            lease_epoch,
            acquired_at_nanos: request.now_nanos,
            expires_at_nanos: request
                .now_nanos
                .checked_add(request.ttl_nanos)
                .ok_or_else(|| anyhow!("task lease expiry overflow"))?,
            updated_at_nanos: request.now_nanos,
            lease_hash: None,
            lease_signature: None,
        }
        .seal(signing_key)?;
        match write_task_lease_state(storage, &lease, existing.as_ref().map(|(row, _)| row)).await {
            Ok(()) => return Ok(lease),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
}

pub async fn check_task_lease(
    storage: &Storage,
    expected: &TaskLease,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let (_, lease) =
        read_expected_task_lease_state(storage, expected, now_nanos, signing_key).await?;
    Ok(lease)
}

pub async fn renew_task_lease(
    storage: &Storage,
    expected: &TaskLease,
    now_nanos: i64,
    ttl_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    if now_nanos < 0 {
        return Err(anyhow!("task lease timestamp must be nonnegative"));
    }
    if ttl_nanos <= 0 {
        return Err(anyhow!("task lease ttl must be positive"));
    }

    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let (row, mut lease) =
            read_expected_task_lease_state(storage, expected, now_nanos, signing_key).await?;
        lease.root_generation = lease
            .root_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("task lease root generation overflow"))?;
        lease.lease_epoch = lease
            .lease_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow!("task lease epoch overflow"))?;
        lease.acquired_at_nanos = now_nanos;
        lease.expires_at_nanos = now_nanos
            .checked_add(ttl_nanos)
            .ok_or_else(|| anyhow!("task lease expiry overflow"))?;
        lease.updated_at_nanos = now_nanos;
        lease = lease.seal(signing_key)?;
        match write_task_lease_state(storage, &lease, Some(&row)).await {
            Ok(()) => return Ok(lease),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease renewal CAS retries exhausted"
    ))
}

pub async fn checkpoint_task_lease(
    storage: &Storage,
    expected: &TaskLease,
    checkpoint_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let (row, mut lease) =
            read_expected_task_lease_state(storage, expected, now_nanos, signing_key).await?;
        if checkpoint_cursor < lease.checkpoint_cursor {
            return Err(anyhow!(
                "{STALE_FENCE}: task lease checkpoint cannot move backwards"
            ));
        }
        if checkpoint_cursor == lease.checkpoint_cursor {
            return Ok(lease);
        }
        lease.root_generation = lease
            .root_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("task lease root generation overflow"))?;
        lease.checkpoint_cursor = checkpoint_cursor;
        lease.updated_at_nanos = now_nanos;
        lease = lease.seal(signing_key)?;
        match write_task_lease_state(storage, &lease, Some(&row)).await {
            Ok(()) => return Ok(lease),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
}

pub async fn commit_task_lease(
    storage: &Storage,
    expected: &TaskLease,
    committed_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let (row, mut lease) =
            read_expected_task_lease_state(storage, expected, now_nanos, signing_key).await?;
        if committed_cursor < lease.checkpoint_cursor {
            return Err(anyhow!(
                "{STALE_FENCE}: task lease commit cannot move backwards"
            ));
        }
        lease.root_generation = lease
            .root_generation
            .checked_add(1)
            .ok_or_else(|| anyhow!("task lease root generation overflow"))?;
        lease.checkpoint_cursor = committed_cursor;
        lease.updated_at_nanos = now_nanos;
        let committed = lease.seal(signing_key)?;
        match delete_task_lease_state(
            storage,
            expected.owner.tenant_id,
            &expected.task_id,
            Some(&row),
        )
        .await
        {
            Ok(_) => return Ok(committed),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
}

pub async fn read_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    Ok(
        read_task_lease_state(storage, tenant_id, task_id, signing_key)
            .await?
            .map(|(_, lease)| lease),
    )
}

pub fn task_lease_precondition(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
) -> Result<CoreMutationPrecondition> {
    let row_key = task_lease_row_key(tenant_id, task_id)?;
    let payload = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_LEASES_FENCES,
        TABLE_TASK_LEASE_ROW,
        &row_key,
    )?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: row_key,
        expected_payload_hash: payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_TASK_LEASE_ROW, payload)),
        require_absent: payload.is_none(),
        require_present: payload.is_some(),
    })
}

pub async fn task_lease_exact_precondition(
    storage: &Storage,
    expected: &TaskLease,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    expected.verify(signing_key)?;
    let Some((payload, current)) = read_task_lease_state(
        storage,
        expected.owner.tenant_id,
        &expected.task_id,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{STALE_FENCE}: task lease does not exist"));
    };
    if current != *expected {
        return Err(anyhow!("{STALE_FENCE}: task lease version changed"));
    }
    Ok(task_lease_row_precondition(
        &task_lease_row_key(expected.owner.tenant_id, &expected.task_id)?,
        Some(&payload),
    ))
}

/// Returns the exact row-CAS precondition for an unexpired execution lease.
///
/// The caller must attach it to the same atomic mutation as the lease-protected
/// completion effects; checking it separately does not fence that mutation.
/// Commit admission must also compare `expected.expires_at_nanos` with its own
/// commit-time clock so an unchanged but expired row cannot pass.
pub async fn task_lease_fenced_precondition(
    storage: &Storage,
    expected: &TaskLease,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    let (payload, _) =
        read_expected_task_lease_state(storage, expected, now_nanos, signing_key).await?;
    let row_key = task_lease_row_key(expected.owner.tenant_id, &expected.task_id)?;
    Ok(CoreMutationPrecondition::CoreMetaLease {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: row_key,
        expected_payload_hash: core_meta_payload_digest(TABLE_TASK_LEASE_ROW, &payload),
        expires_at_unix_nanos: u64::try_from(expected.expires_at_nanos)
            .context("task lease expiry is before the unix epoch")?,
    })
}

pub async fn list_active_task_leases_for_node_page(
    storage: &Storage,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
    after_tuple_key: Option<&[u8]>,
    limit: usize,
) -> Result<TaskLeasePage> {
    if !(1..=TASK_LEASE_LIST_PAGE_MAX).contains(&limit) {
        bail!("task lease page limit must be between 1 and {TASK_LEASE_LIST_PAGE_MAX}");
    }
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = task_lease_owner_prefix(owner_node_id)?;
    if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
        bail!("task lease page cursor is outside the owner scope");
    }
    let rows = store.scan_coremeta_prefix_page(
        CF_LEASES_FENCES,
        TABLE_TASK_LEASE_ROW,
        &prefix,
        after_tuple_key,
        limit,
    )?;
    let mut leases = Vec::with_capacity(rows.len());
    for record in &rows {
        let lease = decode_task_lease_record(&record.payload)?;
        lease.verify(signing_key)?;
        if lease.owner_node_id() != owner_node_id {
            bail!("task lease owner projection is outside its key scope");
        }
        let tuple_key = core_meta_record_tuple_key(&record.key)?;
        if tuple_key != task_lease_owner_key(&lease)? {
            bail!("task lease owner projection key does not match its payload");
        }
        if lease.expires_at_nanos > now_nanos {
            leases.push(lease);
        }
    }
    let next_tuple_key = if rows.len() == limit {
        Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("task lease page lost its final row"))?
                    .key,
            )?
            .to_vec(),
        )
    } else {
        None
    };
    Ok(TaskLeasePage {
        leases,
        next_tuple_key,
    })
}

pub async fn force_release_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let Some((row, lease)) =
            read_task_lease_state(storage, tenant_id, task_id, signing_key).await?
        else {
            return Ok(None);
        };
        match delete_task_lease_state(storage, tenant_id, task_id, Some(&row)).await {
            Ok(_) => return Ok(Some(lease)),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
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

fn sign_lease_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("task lease signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"task_lease");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

async fn read_expected_task_lease_state(
    storage: &Storage,
    expected: &TaskLease,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<(Vec<u8>, TaskLease)> {
    if now_nanos < 0 {
        return Err(anyhow!("task lease timestamp must be nonnegative"));
    }
    expected.verify(signing_key)?;
    let Some((row, lease)) = read_task_lease_state(
        storage,
        expected.owner.tenant_id,
        &expected.task_id,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{STALE_FENCE}: task lease does not exist"));
    };
    if !lease.owner.same_security_owner(&expected.owner) {
        return Err(anyhow!("{LEASE_OWNER_MISMATCH}: task lease owner mismatch"));
    }
    if lease.fence_token != expected.fence_token {
        return Err(anyhow!("{STALE_FENCE}: task lease fence token mismatch"));
    }
    if lease != *expected {
        return Err(anyhow!("{STALE_FENCE}: task lease version changed"));
    }
    if lease.expires_at_nanos <= now_nanos {
        return Err(anyhow!("{LEASE_EXPIRED}: task lease expired"));
    }
    Ok((row, lease))
}

async fn read_task_lease_state(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, TaskLease)>> {
    let row_key = task_lease_row_key(tenant_id, task_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(bytes) = store.read_coremeta_row(CF_LEASES_FENCES, TABLE_TASK_LEASE_ROW, &row_key)?
    else {
        return Ok(None);
    };
    let lease = decode_task_lease_record(&bytes)?;
    lease.verify(signing_key)?;
    if lease.owner.tenant_id != tenant_id || lease.task_id != task_id {
        return Err(anyhow!("task lease row scope mismatch"));
    }
    Ok(Some((bytes, lease)))
}

async fn write_task_lease_state(
    storage: &Storage,
    lease: &TaskLease,
    expected_row: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = task_lease_row_key(lease.owner.tenant_id, &lease.task_id)?;
    let owner_key = task_lease_owner_key(lease)?;
    let previous = expected_row
        .map(|row| decode_task_lease_record(row))
        .transpose()?;
    let partition_id = task_lease_partition_id(lease.owner.tenant_id, &lease.task_id);
    let transaction_id = format!("task-lease:{}", uuid::Uuid::new_v4());
    let bytes = task_lease_to_proto(lease, Some(&transaction_id)).encode_to_vec();
    let mut preconditions = vec![task_lease_row_precondition(&row_key, expected_row)];
    let mut operations = vec![CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.clone(),
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: row_key,
        payload: bytes.clone(),
    }];
    match previous.as_ref().map(task_lease_owner_key).transpose()? {
        Some(previous_owner_key) if previous_owner_key == owner_key => {
            preconditions.push(task_lease_row_precondition(&owner_key, expected_row));
        }
        Some(previous_owner_key) => {
            preconditions.push(task_lease_row_precondition(
                &previous_owner_key,
                expected_row,
            ));
            preconditions.push(task_lease_row_precondition(&owner_key, None));
            operations.push(CoreMutationOperation::CoreMetaDelete {
                partition_id: partition_id.clone(),
                cf: CF_LEASES_FENCES.to_string(),
                table_id: TABLE_TASK_LEASE_ROW,
                tuple_key: previous_owner_key,
            });
        }
        None => preconditions.push(task_lease_row_precondition(&owner_key, None)),
    }
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.clone(),
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: owner_key,
        payload: bytes,
    });
    let root_publications = task_lease_root_publications(
        task_lease_root_anchor_key(lease.owner.tenant_id, &lease.task_id),
        partition_id.clone(),
    );
    let receipt = CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id.clone(),
            committed_by_principal: task_lease_principal(&lease.owner),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    ensure_task_lease_mutation_committed(&receipt, "write")?;
    Ok(())
}

async fn delete_task_lease_state(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    expected_row: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = task_lease_row_key(tenant_id, task_id)?;
    let lease = expected_row
        .map(|row| decode_task_lease_record(row))
        .transpose()?;
    let owner = lease
        .as_ref()
        .map(|lease| lease.owner.clone())
        .unwrap_or(TaskLeaseOwner {
            tenant_id,
            principal_kind: "system".to_string(),
            principal_id: "task-lease-admin".to_string(),
            actor_instance_id: "task-lease-admin".to_string(),
            display_name: "Task lease admin".to_string(),
        });
    let partition_id = task_lease_partition_id(tenant_id, task_id);
    let mut preconditions = vec![task_lease_row_precondition(&row_key, expected_row)];
    let mut operations = vec![CoreMutationOperation::CoreMetaDelete {
        partition_id: partition_id.clone(),
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: row_key,
    }];
    if let Some(lease) = lease {
        let owner_key = task_lease_owner_key(&lease)?;
        preconditions.push(task_lease_row_precondition(&owner_key, expected_row));
        operations.push(CoreMutationOperation::CoreMetaDelete {
            partition_id: partition_id.clone(),
            cf: CF_LEASES_FENCES.to_string(),
            table_id: TABLE_TASK_LEASE_ROW,
            tuple_key: owner_key,
        });
    }
    let root_publications = task_lease_root_publications(
        task_lease_root_anchor_key(tenant_id, task_id),
        partition_id.clone(),
    );
    let receipt = CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("task-lease-delete:{}", uuid::Uuid::new_v4()),
            scope_partition: partition_id.clone(),
            committed_by_principal: task_lease_principal(&owner),
            root_publications,
            preconditions,
            operations,
        })
        .await?;
    ensure_task_lease_mutation_committed(&receipt, "delete")?;
    Ok(())
}

fn ensure_task_lease_mutation_committed(
    receipt: &crate::core_store::CoreMutationBatchReceipt,
    operation: &str,
) -> Result<()> {
    if receipt.state == crate::core_store::CoreTransactionState::Committed {
        return Ok(());
    }
    bail!(
        "task lease {operation} mutation {} did not commit: {}",
        receipt.transaction_id,
        receipt
            .finalisation_error
            .as_deref()
            .unwrap_or("unknown finalisation failure")
    )
}

fn task_lease_row_precondition(
    row_key: &[u8],
    expected_row: Option<&Vec<u8>>,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_LEASE_ROW,
        tuple_key: row_key.to_vec(),
        expected_payload_hash: expected_row
            .map(|payload| core_meta_payload_digest(TABLE_TASK_LEASE_ROW, payload)),
        require_absent: expected_row.is_none(),
        require_present: expected_row.is_some(),
    }
}

fn task_lease_partition_id(tenant_id: i64, task_id: &str) -> String {
    hex::encode(hash32(
        task_lease_root_anchor_key(tenant_id, task_id).as_bytes(),
    ))
}

fn task_lease_root_publications(
    data_root: String,
    coordinator_root: String,
) -> Vec<CoreMutationRootPublication> {
    let coordinator = CoreMutationRootPublication::new(
        coordinator_root.clone(),
        WriterFamily::CoreControl.as_str(),
    )
    .coordinator();
    if data_root == coordinator_root {
        vec![coordinator]
    } else {
        vec![
            coordinator,
            CoreMutationRootPublication::new(data_root, WriterFamily::CoreControl.as_str()),
        ]
    }
}

fn task_lease_principal(_owner: &TaskLeaseOwner) -> String {
    "task-lease-coordinator".to_string()
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

fn is_core_ref_cas_conflict(err: &anyhow::Error) -> bool {
    if crate::core_store::is_retryable_mutation_conflict(err) {
        return true;
    }
    err.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("generation mismatch")
            || message.contains("target mismatch")
            || message.contains("must be absent")
            || message.contains("must be present")
            || message.contains("CAS lock was not acquired")
            || message.contains("CoreStore stream idempotency conflict")
            || message.contains("CoreStore task lease CAS conflict")
            || message.contains("CoreStore root CAS expected generation mismatch")
            || message.contains("CoreStore root CAS expected generation missing")
    })
}

fn is_missing_root_anchor(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .to_string()
            .contains("CoreStore root anchor not found")
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_store::TABLE_TASK_LEASE_ROW;
    use tempfile::tempdir;

    const KEY: &[u8] = b"task lease signing key";

    #[tokio::test]
    async fn task_lease_acquires_reads_and_checkpoints() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let lease = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();
        assert_eq!(lease.fence_token, 1);
        assert_eq!(lease.lease_epoch, 1);
        assert_eq!(lease.root_generation, 1);
        assert_eq!(lease.checkpoint_cursor, 10);
        assert_eq!(lease.owner_node_id(), "node-a");
        assert!(lease.lease_hash.as_deref().unwrap().len() == 64);
        let row_key = task_lease_row_key(0, "index-build-alpha").unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let row = meta
            .get(CF_LEASES_FENCES, TABLE_TASK_LEASE_ROW, &row_key)
            .unwrap()
            .expect("task lease must be stored in CoreMeta");
        assert_ne!(row.first().copied(), Some(b'{'));
        let owner_row = meta
            .get(
                CF_LEASES_FENCES,
                TABLE_TASK_LEASE_ROW,
                &task_lease_owner_key(&lease).unwrap(),
            )
            .unwrap()
            .expect("task lease owner projection");
        assert_eq!(owner_row, row);
        let checkpointed = checkpoint_task_lease(&storage, &lease, 99, 200, KEY)
            .await
            .unwrap();
        assert_eq!(checkpointed.checkpoint_cursor, 99);
        assert_eq!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .unwrap()
                .unwrap(),
            checkpointed
        );
    }

    #[tokio::test]
    async fn task_lease_acquire_rejects_expiry_overflow() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let error = acquire_task_lease(
            &storage,
            acquire(TaskLeaseOwner::node("node-a"), i64::MAX - 1, 2),
            KEY,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("task lease expiry overflow"));
    }

    #[tokio::test]
    async fn task_lease_commit_requires_owner_and_fence_then_removes_lease() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let other = TaskLeaseOwner::node("node-b");
        let lease = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();

        let mut wrong_owner_lease = lease.clone();
        wrong_owner_lease.owner = other;
        wrong_owner_lease.lease_hash = None;
        wrong_owner_lease.lease_signature = None;
        let wrong_owner_lease = wrong_owner_lease.seal(KEY).unwrap();
        let wrong_owner = commit_task_lease(&storage, &wrong_owner_lease, 10, 200, KEY)
            .await
            .unwrap_err();
        assert!(wrong_owner.to_string().contains(LEASE_OWNER_MISMATCH));

        let mut stale_lease = lease.clone();
        stale_lease.fence_token += 1;
        stale_lease.lease_hash = None;
        stale_lease.lease_signature = None;
        let stale_lease = stale_lease.seal(KEY).unwrap();
        let stale = commit_task_lease(&storage, &stale_lease, 10, 200, KEY)
            .await
            .unwrap_err();
        assert!(stale.to_string().contains(STALE_FENCE));

        let committed = commit_task_lease(&storage, &lease, 12, 200, KEY)
            .await
            .unwrap();
        assert_eq!(committed.checkpoint_cursor, 12);
        assert_eq!(committed.root_generation, lease.root_generation + 1);
        assert!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
        let reacquired = acquire_task_lease(&storage, acquire(owner, 300, 500), KEY)
            .await
            .unwrap();
        assert_eq!(reacquired.root_generation, committed.root_generation + 1);
    }

    #[tokio::test]
    async fn active_lease_blocks_other_owner_until_expired_then_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner_a = TaskLeaseOwner::node("node-a");
        let owner_b = TaskLeaseOwner::node("node-b");
        let first = acquire_task_lease(&storage, acquire(owner_a.clone(), 100, 500), KEY)
            .await
            .unwrap();
        assert!(
            acquire_task_lease(&storage, acquire(owner_b.clone(), 200, 500), KEY)
                .await
                .unwrap_err()
                .to_string()
                .contains(LEASE_HELD)
        );

        let second = acquire_task_lease(&storage, acquire(owner_b, 700, 500), KEY)
            .await
            .unwrap();
        assert_eq!(second.fence_token, first.fence_token + 1);
        assert_eq!(second.lease_epoch, first.lease_epoch + 1);
        assert!(
            list_active_task_leases_for_node_page(&storage, "node-a", 700, KEY, None, 10)
                .await
                .unwrap()
                .leases
                .is_empty()
        );
        assert_eq!(
            list_active_task_leases_for_node_page(&storage, "node-b", 700, KEY, None, 10)
                .await
                .unwrap(),
            TaskLeasePage {
                leases: vec![second.clone()],
                next_tuple_key: None,
            }
        );
        assert!(
            checkpoint_task_lease(&storage, &first, 20, 750, KEY,)
                .await
                .unwrap_err()
                .to_string()
                .contains(LEASE_OWNER_MISMATCH)
        );
    }

    #[tokio::test]
    async fn same_owner_reacquisition_starts_a_new_fenced_execution() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let first = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();
        let renewed = acquire_task_lease(&storage, acquire(owner, 200, 500), KEY)
            .await
            .unwrap();
        assert_eq!(renewed.fence_token, first.fence_token + 1);
        assert_eq!(renewed.lease_epoch, first.lease_epoch + 1);
        assert_eq!(renewed.root_generation, first.root_generation + 1);
        assert_eq!(renewed.expires_at_nanos, 700);
        assert!(
            checkpoint_task_lease(&storage, &first, 10, 250, KEY)
                .await
                .unwrap_err()
                .to_string()
                .contains(STALE_FENCE)
        );
        assert_eq!(
            check_task_lease(&storage, &renewed, 250, KEY)
                .await
                .unwrap(),
            renewed
        );
    }

    #[tokio::test]
    async fn checked_renewal_fences_old_versions_and_refuses_expired_leases() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_task_lease(
            &storage,
            acquire(TaskLeaseOwner::node("node-a"), 100, 500),
            KEY,
        )
        .await
        .unwrap();

        let precondition = task_lease_fenced_precondition(&storage, &first, 150, KEY)
            .await
            .unwrap();
        assert!(matches!(
            precondition,
            CoreMutationPrecondition::CoreMetaLease {
                expires_at_unix_nanos: 600,
                ..
            }
        ));

        let renewed = renew_task_lease(&storage, &first, 200, 500, KEY)
            .await
            .unwrap();
        assert_eq!(renewed.fence_token, first.fence_token);
        assert_eq!(renewed.lease_epoch, first.lease_epoch + 1);
        assert_eq!(renewed.expires_at_nanos, 700);
        assert!(
            check_task_lease(&storage, &first, 250, KEY)
                .await
                .unwrap_err()
                .to_string()
                .contains(STALE_FENCE)
        );
        assert_eq!(
            check_task_lease(&storage, &renewed, 250, KEY)
                .await
                .unwrap(),
            renewed
        );
        assert!(
            renew_task_lease(&storage, &renewed, 700, 500, KEY)
                .await
                .unwrap_err()
                .to_string()
                .contains(LEASE_EXPIRED)
        );
    }

    #[tokio::test]
    async fn concurrent_acquire_has_exactly_one_active_owner() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut tasks = Vec::new();
        for idx in 0..16 {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                acquire_task_lease(
                    &storage,
                    acquire(TaskLeaseOwner::node(format!("node-{idx}")), 100, 500),
                    KEY,
                )
                .await
            }));
        }
        let mut successes = 0;
        let mut held = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(_) => successes += 1,
                Err(err) if err.to_string().contains(LEASE_HELD) => held += 1,
                Err(err) => panic!("unexpected lease error: {err}"),
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(held, 15);
    }

    #[tokio::test]
    async fn independent_task_leases_commit_without_a_process_wide_lock() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut acquisitions = Vec::new();
        for idx in 0..16 {
            let storage = storage.clone();
            acquisitions.push(tokio::spawn(async move {
                let mut request = acquire(TaskLeaseOwner::node(format!("node-{idx}")), 100, 500);
                request.task_id = format!("independent-{idx}");
                acquire_task_lease(&storage, request, KEY).await
            }));
        }
        for acquisition in acquisitions {
            let lease = acquisition.await.unwrap().unwrap();
            assert_eq!(lease.fence_token, 1);
            assert_eq!(lease.lease_epoch, 1);
        }
    }

    #[tokio::test]
    async fn checkpoint_requires_the_exact_current_lease_version() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let lease = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();
        let first = checkpoint_task_lease(&storage, &lease, 99, 200, KEY)
            .await
            .unwrap();
        let stale = checkpoint_task_lease(&storage, &lease, 99, 200, KEY)
            .await
            .unwrap_err();
        assert!(stale.to_string().contains(STALE_FENCE));
        let second = checkpoint_task_lease(&storage, &first, 99, 250, KEY)
            .await
            .unwrap();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn task_lease_rejects_tamper_and_invalid_inputs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        acquire_task_lease(
            &storage,
            acquire(TaskLeaseOwner::node("node-a"), 100, 500),
            KEY,
        )
        .await
        .unwrap();
        let (row, _) = read_task_lease_state(&storage, 0, "index-build-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        let mut tampered = TaskLeaseRecordProto::decode(row.as_slice()).unwrap();
        tampered.checkpoint_cursor_be = 1234_u128.to_be_bytes().to_vec();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        meta.put(
            CF_LEASES_FENCES,
            TABLE_TASK_LEASE_ROW,
            &task_lease_row_key(0, "index-build-alpha").unwrap(),
            &tampered.encode_to_vec(),
        )
        .unwrap();
        assert!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .is_err()
        );
        assert!(task_lease_row_key(0, "../escape").is_err());
        let mut invalid = acquire(TaskLeaseOwner::node("node-a"), 100, 0);
        invalid.partition_id = "not-hex".to_string();
        assert!(acquire_task_lease(&storage, invalid, KEY).await.is_err());
    }

    fn acquire(owner: TaskLeaseOwner, now_nanos: i64, ttl_nanos: i64) -> TaskLeaseAcquire {
        TaskLeaseAcquire {
            task_id: "index-build-alpha".to_string(),
            task_kind: "full_text_index_build".to_string(),
            partition_family: "full_text_index".to_string(),
            partition_id: hex::encode([4; 32]),
            owner,
            source_cursor: 10,
            now_nanos,
            ttl_nanos,
        }
    }
}
