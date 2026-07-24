use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMutationPrecondition, TABLE_OWNERSHIP_FENCE_ROW,
        TABLE_PARTITION_OWNER_ROW, core_meta_committed_row_common, core_meta_payload_digest,
        core_meta_root_key_hash,
    },
    error_codes::AnvilErrorCode,
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fmt;

mod coremeta;

pub use coremeta::{
    MAX_PARTITION_FENCE_PAGE_SIZE, OwnershipFencePage, OwnershipFencePageCursor,
    PartitionOwnerPage, PartitionOwnerPageCursor, list_active_ownership_fences_for_node_page,
    list_ownership_fences_page, list_partition_owners_for_node_page, list_partition_owners_page,
};
use coremeta::{
    is_partition_fence_cas_conflict, ownership_fence_row_key, partition_owner_row_key,
    read_ownership_fence_state, read_partition_owner_state, write_ownership_fence_state,
    write_partition_owner_state,
};

type HmacSha256 = Hmac<Sha256>;

pub const OWNERSHIP_HELD: &str = "OwnershipHeld";
pub const OWNERSHIP_EXPIRED: &str = "OwnershipExpired";
pub const OWNERSHIP_NOT_FOUND: &str = "OwnershipNotFound";
pub const OWNERSHIP_OWNER_MISMATCH: &str = "OwnershipOwnerMismatch";
pub const OWNERSHIP_STALE_FENCE: &str = "StaleFence";
pub const OWNERSHIP_CAS_CONFLICT: &str = "OwnershipCasConflict";
pub const MAX_OWNERSHIP_LEASE_MS: u64 = 120_000;

const MAX_PARTITION_FENCE_CAS_ATTEMPTS: usize = 64;
const EXPIRED_PARTITION_OWNER_NODE_PREFIX: &str = "__anvil_expired_partition_owner__:";

pub(crate) fn partition_owner_root_key_hash(partition_family: &str, partition_id: &str) -> String {
    core_meta_root_key_hash(&format!(
        "partition-owner/{partition_family}/{partition_id}"
    ))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionOwnerStatus {
    Recovering,
    Ready,
}

impl PartitionOwnerStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recovering => "recovering",
            Self::Ready => "ready",
        }
    }
}

fn partition_owner_status_from_str(value: &str) -> Result<PartitionOwnerStatus> {
    Ok(match value {
        "recovering" => PartitionOwnerStatus::Recovering,
        "ready" => PartitionOwnerStatus::Ready,
        _ => bail!("unsupported partition owner status {value}"),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOwnerState {
    pub format_version: u16,
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
    pub recovery_epoch: u64,
    pub generation: u64,
    pub status: PartitionOwnerStatus,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub updated_at_nanos: i64,
    pub owner_hash: Option<String>,
    pub owner_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRecoveryAcquire {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub now_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWritePermit {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceRejection {
    pub code: AnvilErrorCode,
    pub reason: &'static str,
}

impl fmt::Display for FenceRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

impl std::error::Error for FenceRejection {}

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

fn ownership_resource_hash(tenant_id: i64, resource: &OwnershipResource) -> Result<String> {
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
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_ownership_fence(&self)?;
        let hash = hash_ownership_fence(&self)?;
        let signature = sign_ownership_hash(
            signing_key,
            &hash,
            &[
                &self.owner.tenant_id.to_string(),
                self.resource.resource_kind.as_str(),
                &self.resource.resource_id,
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence.to_string(),
            ],
        )?;
        self.ownership_hash = Some(hash);
        self.ownership_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_ownership_fence(self)?;
        let expected_hash = hash_ownership_fence(self)?;
        if self.ownership_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("ownership fence hash mismatch"));
        }
        let expected_signature = sign_ownership_hash(
            signing_key,
            &expected_hash,
            &[
                &self.owner.tenant_id.to_string(),
                self.resource.resource_kind.as_str(),
                &self.resource.resource_id,
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence.to_string(),
            ],
        )?;
        if self.ownership_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("ownership fence signature mismatch"));
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
    fn as_str(self) -> &'static str {
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
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_owner(&self)?;
        let hash = hash_partition_owner(&self)?;
        let signature = sign_owner_hash(
            signing_key,
            &hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        self.owner_hash = Some(hash);
        self.owner_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_owner(self)?;
        let expected_hash = hash_partition_owner(self)?;
        if self.owner_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("partition owner hash mismatch"));
        }
        let expected_signature = sign_owner_hash(
            signing_key,
            &expected_hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.owner_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("partition owner signature mismatch"));
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

fn encode_partition_owner_record(owner: &PartitionOwnerState) -> Result<Vec<u8>> {
    Ok(partition_owner_to_proto(owner).encode_to_vec())
}

fn decode_partition_owner_record(bytes: &[u8]) -> Result<PartitionOwnerState> {
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

fn encode_ownership_fence_record(record: &OwnershipFenceRecord) -> Result<Vec<u8>> {
    let proto = ownership_fence_record_to_proto(record)?;
    Ok(proto.encode_to_vec())
}

fn decode_ownership_fence_record(bytes: &[u8]) -> Result<OwnershipFenceRecord> {
    let proto = OwnershipFenceRecordProto::decode(bytes)?;
    if proto.encode_to_vec() != bytes {
        bail!("ownership fence record is not deterministic protobuf");
    }
    ownership_fence_record_from_proto(proto)
}

fn ownership_fence_record_to_proto(
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

pub async fn force_expire_partition_owner_for_node(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    let failover_started_at = std::time::Instant::now();
    if now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut owner)) =
            read_partition_owner_state(storage, partition_family, partition_id, signing_key)
                .await?
        else {
            crate::perf::record_partition_failover_duration(
                "unknown",
                "unknown",
                "owner_absent",
                failover_started_at.elapsed(),
            );
            return Ok(None);
        };
        if owner.owner_node_id != owner_node_id {
            crate::perf::record_partition_failover_duration(
                "unknown",
                "unknown",
                "owner_mismatch",
                failover_started_at.elapsed(),
            );
            return Ok(None);
        }
        owner.owner_node_id = expired_partition_owner_node_id(owner_node_id);
        owner.fence_token = increment_counter(owner.fence_token, "partition owner fence token")?;
        owner.recovery_epoch =
            increment_counter(owner.recovery_epoch, "partition owner recovery epoch")?;
        owner.generation = increment_counter(owner.generation, "partition owner generation")?;
        owner.status = PartitionOwnerStatus::Recovering;
        owner.updated_at_nanos = now_nanos;
        owner = owner.seal(signing_key)?;
        match write_partition_owner_state(storage, &owner, Some(&ref_value)).await {
            Ok(()) => {
                crate::perf::record_root_generation_in_doubt(
                    "partition_owner",
                    partition_id_hash(partition_id),
                );
                crate::perf::record_partition_failover_duration(
                    "unknown",
                    "unknown",
                    "forced_expired",
                    failover_started_at.elapsed(),
                );
                return Ok(Some(owner));
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: partition owner force-expire CAS retries exhausted"
    ))
}

pub async fn acquire_partition_recovery(
    storage: &Storage,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    let failover_started_at = std::time::Instant::now();
    validate_recovery_acquire(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let existing = read_partition_owner_state(
            storage,
            &request.partition_family,
            &request.partition_id,
            signing_key,
        )
        .await?;
        let existing_state = existing.as_ref().map(|(_, state)| state);
        if let Some(existing_state) = existing_state {
            if partition_owner_is_current_for_node(existing_state, &request.owner_node_id) {
                if existing_state.status == PartitionOwnerStatus::Ready
                    || (existing_state.recovered_through_sequence
                        == request.recovered_through_sequence
                        && existing_state.recovered_manifest_hash
                            == request.recovered_manifest_hash)
                {
                    return Ok(existing_state.clone());
                }
                return Err(anyhow!(
                    "{OWNERSHIP_HELD}: partition owner recovery state already exists with a different recovery basis"
                ));
            }
            if !partition_owner_is_force_expired(existing_state) {
                return Err(anyhow!(
                    "{OWNERSHIP_HELD}: partition owner is held by active node {}",
                    existing_state.owner_node_id
                ));
            }
        }

        let fence_token = match existing_state {
            Some(state) => increment_counter(state.fence_token, "partition owner fence token")?,
            None => 1,
        };
        let recovery_epoch = match existing_state {
            Some(state) => {
                increment_counter(state.recovery_epoch, "partition owner recovery epoch")?
            }
            None => 1,
        };
        let generation = match existing_state {
            Some(state) => increment_counter(state.generation, "partition owner generation")?,
            None => 1,
        };
        let state = PartitionOwnerState {
            format_version: existing_state
                .map(|state| state.format_version)
                .unwrap_or(1),
            partition_family: request.partition_family.clone(),
            partition_id: request.partition_id.clone(),
            owner_node_id: request.owner_node_id.clone(),
            fence_token,
            recovery_epoch,
            generation,
            status: PartitionOwnerStatus::Recovering,
            recovered_through_sequence: request.recovered_through_sequence,
            recovered_manifest_hash: request.recovered_manifest_hash.clone(),
            updated_at_nanos: request.now_nanos,
            owner_hash: None,
            owner_signature: None,
        }
        .seal(signing_key)?;
        match write_partition_owner_state(
            storage,
            &state,
            existing.as_ref().map(|(ref_value, _)| ref_value),
        )
        .await
        {
            Ok(()) => {
                if existing.is_some() {
                    crate::perf::record_root_generation_in_doubt(
                        "partition_owner",
                        partition_id_hash(&state.partition_id),
                    );
                }
                crate::perf::record_partition_failover_duration(
                    "unknown",
                    "unknown",
                    "recovery_acquired",
                    failover_started_at.elapsed(),
                );
                return Ok(state);
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: partition owner recovery CAS retries exhausted"
    ))
}

pub async fn publish_partition_ready(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    fence_token: u64,
    recovered_through_sequence: u64,
    recovered_manifest_hash: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    let failover_started_at = std::time::Instant::now();
    validate_hex32(recovered_manifest_hash, "recovered manifest hash")?;
    if now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let Some((ref_value, mut state)) =
            read_partition_owner_state(storage, partition_family, partition_id, signing_key)
                .await?
        else {
            return Err(FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner state is absent",
            }
            .into());
        };
        validate_write_permit_for_state(
            &state,
            &PartitionWritePermit {
                partition_family: partition_family.to_string(),
                partition_id: partition_id.to_string(),
                owner_node_id: owner_node_id.to_string(),
                fence_token,
            },
            false,
        )?;
        if state.status == PartitionOwnerStatus::Ready {
            if state.recovered_through_sequence == recovered_through_sequence
                && state.recovered_manifest_hash == recovered_manifest_hash
            {
                return Ok(state);
            }
            return Err(anyhow!(
                "{OWNERSHIP_HELD}: partition owner is already ready with different recovery state"
            ));
        }
        state.status = PartitionOwnerStatus::Ready;
        state.recovered_through_sequence = recovered_through_sequence;
        state.recovered_manifest_hash = recovered_manifest_hash.to_string();
        state.updated_at_nanos = now_nanos;
        state.generation = increment_counter(state.generation, "partition owner generation")?;
        state = state.seal(signing_key)?;
        match write_partition_owner_state(storage, &state, Some(&ref_value)).await {
            Ok(()) => {
                crate::perf::record_partition_failover_duration(
                    "unknown",
                    "unknown",
                    "ready_published",
                    failover_started_at.elapsed(),
                );
                return Ok(state);
            }
            Err(err) if is_partition_fence_cas_conflict(&err) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: partition owner ready CAS retries exhausted"
    ))
}

#[cfg(test)]
pub async fn ready_partition_owner_for_test(
    storage: &Storage,
    partition_family: String,
    partition_id: String,
    owner_node_id: &str,
    recovered_through_sequence: u64,
    recovered_manifest_hash: String,
    ready_manifest_hash: String,
    signing_key: &[u8],
) -> PartitionOwnerState {
    if let Some(existing) =
        read_partition_owner(storage, &partition_family, &partition_id, signing_key)
            .await
            .unwrap()
    {
        if existing.owner_node_id != owner_node_id && !partition_owner_is_force_expired(&existing) {
            force_expire_partition_owner_for_node(
                storage,
                &partition_family,
                &partition_id,
                &existing.owner_node_id,
                existing.updated_at_nanos.saturating_add(1),
                signing_key,
            )
            .await
            .unwrap();
        }
    }

    let recovering = acquire_partition_recovery(
        storage,
        PartitionRecoveryAcquire {
            partition_family: partition_family.clone(),
            partition_id: partition_id.clone(),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence,
            recovered_manifest_hash,
            now_nanos: 100,
        },
        signing_key,
    )
    .await
    .unwrap();

    publish_partition_ready(
        storage,
        &partition_family,
        &partition_id,
        owner_node_id,
        recovering.fence_token,
        recovered_through_sequence,
        &ready_manifest_hash,
        200,
        signing_key,
    )
    .await
    .unwrap()
}

fn partition_id_hash(partition_id: &str) -> u64 {
    let hash = hash32(partition_id.as_bytes());
    u64::from_le_bytes(
        hash[..8]
            .try_into()
            .expect("hash32 is at least eight bytes"),
    )
}

fn expired_partition_owner_node_id(owner_node_id: &str) -> String {
    format!("{EXPIRED_PARTITION_OWNER_NODE_PREFIX}{owner_node_id}")
}

pub fn partition_owner_is_force_expired(owner: &PartitionOwnerState) -> bool {
    owner.status == PartitionOwnerStatus::Recovering
        && owner
            .owner_node_id
            .starts_with(EXPIRED_PARTITION_OWNER_NODE_PREFIX)
}

fn partition_owner_is_current_for_node(owner: &PartitionOwnerState, owner_node_id: &str) -> bool {
    owner.owner_node_id == owner_node_id
        && matches!(
            owner.status,
            PartitionOwnerStatus::Recovering | PartitionOwnerStatus::Ready
        )
}

pub async fn validate_partition_write(
    storage: &Storage,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<(), FenceRejection> {
    let owner = read_partition_owner(
        storage,
        &permit.partition_family,
        &permit.partition_id,
        signing_key,
    )
    .await
    .map_err(|_| FenceRejection {
        code: AnvilErrorCode::PartitionNotOwned,
        reason: "partition owner state cannot be read",
    })?;
    let Some(owner) = owner else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        });
    };
    validate_write_permit_for_state(&owner, permit, true)
}

pub async fn partition_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition, FenceRejection> {
    let state = read_partition_owner_state(
        storage,
        &permit.partition_family,
        &permit.partition_id,
        signing_key,
    )
    .await
    .map_err(|_| FenceRejection {
        code: AnvilErrorCode::PartitionNotOwned,
        reason: "partition owner state cannot be read",
    })?;
    let Some((payload, owner)) = state else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        });
    };
    validate_write_permit_for_state(&owner, permit, true)?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_PARTITION_OWNER_ROW,
        tuple_key: partition_owner_row_key(&permit.partition_family, &permit.partition_id)
            .map_err(|_| FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner row cannot be addressed",
            })?,
        expected_payload_hash: Some(core_meta_payload_digest(
            TABLE_PARTITION_OWNER_ROW,
            &payload,
        )),
        require_absent: false,
        require_present: true,
    })
}

pub fn validate_write_permit_for_state(
    owner: &PartitionOwnerState,
    permit: &PartitionWritePermit,
    require_ready: bool,
) -> Result<(), FenceRejection> {
    if owner.partition_family != permit.partition_family
        || owner.partition_id != permit.partition_id
    {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit targets a different partition",
        });
    }
    if require_ready && owner.status != PartitionOwnerStatus::Ready {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner has not completed recovery",
        });
    }
    if owner.owner_node_id != permit.owner_node_id {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit owner is not current",
        });
    }
    if owner.fence_token != permit.fence_token {
        return Err(FenceRejection {
            code: AnvilErrorCode::StaleFenceToken,
            reason: "write permit fence token is stale",
        });
    }
    Ok(())
}

pub async fn read_partition_owner(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    Ok(
        read_partition_owner_state(storage, partition_family, partition_id, signing_key)
            .await?
            .map(|(_, owner)| owner),
    )
}

fn validate_acquire_ownership(request: &AcquireOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_renew_ownership(request: &RenewOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    validate_ownership_fence_value(request.current_fence)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_transfer_ownership(request: &TransferOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    require_nonempty(&request.idempotency_key, "idempotency_key")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.current_owner)?;
    validate_ownership_principal(&request.new_owner)?;
    validate_ownership_fence_value(request.current_fence)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_release_ownership(request: &ReleaseOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    if !request.administrative_force {
        validate_ownership_fence_value(request.current_fence)?;
    }
    if request.now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_force_expire_ownership(request: &ForceExpireOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.admin)?;
    if request.now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    if request.reason.chars().any(char::is_control) {
        return Err(anyhow!(
            "ownership force-expire reason must not contain control characters"
        ));
    }
    Ok(())
}

fn validate_ownership_resource(resource: &OwnershipResource) -> Result<()> {
    require_nonempty(&resource.resource_id, "resource_id")?;
    if resource
        .resource_id
        .chars()
        .any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("resource_id must not contain control characters"));
    }
    Ok(())
}

fn validate_ownership_principal(owner: &OwnershipPrincipal) -> Result<()> {
    if owner.tenant_id < 0 {
        return Err(anyhow!("ownership owner tenant_id must be nonnegative"));
    }
    require_nonempty(&owner.principal_kind, "owner.principal_kind")?;
    require_nonempty(&owner.principal_id, "owner.principal_id")?;
    require_nonempty(&owner.actor_instance_id, "owner.actor_instance_id")?;
    require_nonempty(&owner.display_name, "owner.display_name")?;
    require_nonempty(&owner.region, "owner.region")?;
    require_nonempty(&owner.cell, "owner.cell")?;
    validate_optional_label(&owner.display_name, "owner.display_name")?;
    validate_optional_label(&owner.region, "owner.region")?;
    validate_optional_label(&owner.cell, "owner.cell")?;
    Ok(())
}

fn validate_ownership_time(now_nanos: i64, ttl_nanos: i64) -> Result<()> {
    if now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    if ttl_nanos <= 0 {
        return Err(anyhow!("ownership fence ttl must be positive"));
    }
    Ok(())
}

fn validate_ownership_fence_value(fence: u64) -> Result<()> {
    if fence == 0 {
        return Err(anyhow!("ownership fence token must be nonzero"));
    }
    Ok(())
}

fn validate_optional_label(value: &str, field: &'static str) -> Result<()> {
    if value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(anyhow!("{field} must not contain control characters"));
    }
    Ok(())
}

fn validate_unsigned_ownership_fence(record: &OwnershipFenceRecord) -> Result<()> {
    if !matches!(record.format_version, 1 | 2) {
        return Err(anyhow!("unsupported ownership fence version"));
    }
    validate_ownership_resource(&record.resource)?;
    validate_ownership_principal(&record.owner)?;
    validate_ownership_fence_value(record.fence)?;
    if record.generation == 0 {
        return Err(anyhow!("ownership fence generation must be nonzero"));
    }
    if record.generation < record.fence {
        return Err(anyhow!(
            "ownership fence generation must not precede the fence token"
        ));
    }
    if record.last_heartbeat_at_nanos < 0 || record.lease_expires_at_nanos < 0 {
        return Err(anyhow!("ownership fence timestamps must be nonnegative"));
    }
    if matches!(
        record.state,
        OwnershipFenceState::Active
            | OwnershipFenceState::Transferring
            | OwnershipFenceState::Draining
    ) && record.lease_expires_at_nanos <= record.last_heartbeat_at_nanos
    {
        return Err(anyhow!(
            "active ownership fence expiry must be after heartbeat"
        ));
    }
    Ok(())
}

fn require_current_owner_and_fence(
    record: &OwnershipFenceRecord,
    owner: &OwnershipPrincipal,
    current_fence: u64,
) -> Result<()> {
    if !record.owner.same_security_owner(owner) {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: ownership fence owner mismatch"
        ));
    }
    if record.fence != current_fence {
        return Err(anyhow!(
            "{OWNERSHIP_STALE_FENCE}: ownership fence token mismatch"
        ));
    }
    Ok(())
}

fn ownership_idempotency_matches(
    record: &OwnershipFenceRecord,
    operation: &str,
    idempotency_key: &str,
    owner: &OwnershipPrincipal,
) -> bool {
    !idempotency_key.is_empty()
        && record.last_operation.as_deref() == Some(operation)
        && record.last_idempotency_key.as_deref() == Some(idempotency_key)
        && record
            .last_actor
            .as_ref()
            .unwrap_or(&record.owner)
            .same_security_owner(owner)
}

fn nonempty_idempotency_key(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn validate_recovery_acquire(request: &PartitionRecoveryAcquire) -> Result<()> {
    require_nonempty(&request.partition_family, "partition family")?;
    validate_hex32(&request.partition_id, "partition id")?;
    require_nonempty(&request.owner_node_id, "owner node id")?;
    if request
        .owner_node_id
        .starts_with(EXPIRED_PARTITION_OWNER_NODE_PREFIX)
    {
        return Err(anyhow!("owner node id uses an Anvil-reserved prefix"));
    }
    validate_hex32(&request.recovered_manifest_hash, "recovered manifest hash")?;
    if request.now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_owner(owner: &PartitionOwnerState) -> Result<()> {
    if owner.format_version != 1 {
        return Err(anyhow!("unsupported partition owner version"));
    }
    require_nonempty(&owner.partition_family, "partition family")?;
    validate_hex32(&owner.partition_id, "partition id")?;
    require_nonempty(&owner.owner_node_id, "owner node id")?;
    validate_hex32(&owner.recovered_manifest_hash, "recovered manifest hash")?;
    if owner.fence_token == 0 || owner.recovery_epoch == 0 || owner.generation == 0 {
        return Err(anyhow!(
            "partition owner fence, epoch, and generation must be nonzero"
        ));
    }
    if owner.generation < owner.fence_token || owner.generation < owner.recovery_epoch {
        return Err(anyhow!(
            "partition owner generation must not precede its fence or recovery epoch"
        ));
    }
    if owner.updated_at_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn increment_counter(value: u64, label: &'static str) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| anyhow!("{label} overflow"))
}

fn sign_owner_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("partition owner signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"partition_owner");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn sign_ownership_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("ownership fence signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"ownership_fence");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be 32 bytes encoded as hex"));
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
mod tests;
