use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreStore, TABLE_MESH_PARTITION_ROW, core_meta_payload_digest, core_meta_tuple_key,
};
use crate::mesh_control_stream::{
    self, ControlMutationHeaderInput, ControlRecordDigest, ControlStreamFrame,
};
use crate::partition_fence::{self, PartitionWritePermit};
use crate::storage::Storage;
use crate::{routing, validation};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use thiserror::Error;

mod helpers;
mod listing;
mod record_proto;
use helpers::*;
pub use listing::{
    BucketLocatorPage, RoutingRecordPage, page_bucket_locators, page_projected_routing_records,
};
use record_proto::{
    DESCRIPTOR_FILE_EXTENSION, DecodeRoutingRecord, StoredRoutingRecord,
    routing_record_descriptor_from_proto, routing_record_descriptor_from_record,
};

pub const MESH_DIRECTORY_ROOT: &str = "_anvil/control/v1/mesh";
pub const TENANT_NAME_SCHEMA: &str = "anvil.mesh.tenant_name.v1";
pub const TENANT_LOCATOR_SCHEMA: &str = "anvil.mesh.tenant_locator.v1";
pub const BUCKET_LOCATOR_SCHEMA: &str = "anvil.mesh.bucket_locator.v1";
pub const CONTROL_MUTATION_SCHEMA: &str = "anvil.mesh.control_mutation.v1";
pub const CONTROL_PARTITION_FAMILY: &str = "control_partition";
const MESH_DIRECTORY_PROJECTION_PARTITION_ID: &str = "mesh-directory-projection";

const TENANT_NAME_PARTITION_DOMAIN: &str = "tenant-name";
const TENANT_LOCATOR_PARTITION_DOMAIN: &str = "tenant-locator";
const BUCKET_LOCATOR_PARTITION_DOMAIN: &str = "bucket-locator";
const HOST_ALIAS_PARTITION_DOMAIN: &str = "host-alias";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RoutingRecordFamily {
    TenantName,
    TenantLocator,
    BucketLocator,
    HostAlias,
}

impl RoutingRecordFamily {
    pub fn all() -> [Self; 4] {
        [
            Self::TenantName,
            Self::TenantLocator,
            Self::BucketLocator,
            Self::HostAlias,
        ]
    }

    pub fn stream_family(self) -> &'static str {
        match self {
            Self::TenantName => "tenant_name",
            Self::TenantLocator => "tenant_locator",
            Self::BucketLocator => "bucket_locator",
            Self::HostAlias => "host_alias",
        }
    }

    pub fn from_stream_family(value: &str) -> Option<Self> {
        match value {
            "tenant_name" => Some(Self::TenantName),
            "tenant_locator" => Some(Self::TenantLocator),
            "bucket_locator" => Some(Self::BucketLocator),
            "host_alias" => Some(Self::HostAlias),
            _ => None,
        }
    }

    pub fn directory_segment(self) -> &'static str {
        match self {
            Self::TenantName => "tenant-names",
            Self::TenantLocator => "tenants",
            Self::BucketLocator => "buckets",
            Self::HostAlias => "host-aliases",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutingRecordDescriptor {
    pub family: RoutingRecordFamily,
    pub record_key: String,
    pub partition: String,
    pub descriptor_key: String,
    pub generation: u64,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutingRecordSource {
    descriptor: RoutingRecordDescriptor,
    payload_proto: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct MeshControlWriteAuthority<'a> {
    pub permit: &'a PartitionWritePermit,
    pub signing_key: &'a [u8],
    /// Cluster-local authoritative storage and consensus boundary.
    ///
    /// The optional shape is temporarily retained for source compatibility,
    /// but authority-less writes are rejected. There is no CoreStore fallback.
    pub mvcc: Option<&'a crate::mvcc_bootstrap::MvccSubsystem>,
}

const ROUTING_CONTROL_HEAD_KIND: &str = "routing_control_head";

#[derive(Serialize, Deserialize)]
struct RoutingControlHead {
    next_sequence: u64,
    next_byte_offset: u64,
}

pub fn control_partition_id(stream_family: &str, partition: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(stream_family.as_bytes());
    hasher.update(b"/");
    hasher.update(partition.as_bytes());
    hasher.finalize().to_hex().to_string()
}

#[derive(Debug, Error)]
pub enum MeshDirectoryError {
    #[error("invalid tenant name: {0}")]
    InvalidTenantName(String),
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("invalid {field}: {value}")]
    InvalidIdentifier { field: &'static str, value: String },
    #[error("bucket locator already exists for tenant {tenant_id} bucket {bucket_name}")]
    DuplicateBucketLocator {
        tenant_id: String,
        bucket_name: String,
    },
    #[error("tenant name already exists: {tenant_name}")]
    TenantNameAlreadyExists { tenant_name: String },
    #[error(
        "mesh directory generation conflict for {descriptor_key}: expected {expected}, actual {actual}"
    )]
    GenerationConflict {
        descriptor_key: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid mesh directory state for {descriptor_key}: {state}")]
    InvalidState {
        descriptor_key: String,
        state: String,
    },
    #[error("invalid RFC3339 timestamp in {field}: {value}")]
    InvalidTimestamp { field: &'static str, value: String },
    #[error("mesh directory record not found: {0}")]
    NotFound(String),
    #[error("invalid mesh control write permit for {stream_family}/{partition}: {reason}")]
    InvalidControlWritePermit {
        stream_family: String,
        partition: String,
        reason: String,
    },
    #[error("mesh control write fence rejected for {stream_family}/{partition}: {code}: {reason}")]
    ControlFenceRejected {
        stream_family: String,
        partition: String,
        code: &'static str,
        reason: &'static str,
    },
    #[error("mesh control stream write failed for {stream_family}/{partition}: {message}")]
    ControlStreamWrite {
        stream_family: String,
        partition: String,
        message: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type MeshDirectoryResult<T> = Result<T, MeshDirectoryError>;

fn routing_transaction_rejected() -> MeshDirectoryError {
    MeshDirectoryError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "mesh routing projections are control-plane state and cannot participate in a cluster transaction",
    ))
}

impl PartialEq for MeshDirectoryError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InvalidTenantName(a), Self::InvalidTenantName(b)) => a == b,
            (Self::InvalidBucketName(a), Self::InvalidBucketName(b)) => a == b,
            (
                Self::InvalidIdentifier {
                    field: field_a,
                    value: value_a,
                },
                Self::InvalidIdentifier {
                    field: field_b,
                    value: value_b,
                },
            ) => field_a == field_b && value_a == value_b,
            (
                Self::DuplicateBucketLocator {
                    tenant_id: tenant_a,
                    bucket_name: bucket_a,
                },
                Self::DuplicateBucketLocator {
                    tenant_id: tenant_b,
                    bucket_name: bucket_b,
                },
            ) => tenant_a == tenant_b && bucket_a == bucket_b,
            (
                Self::TenantNameAlreadyExists {
                    tenant_name: name_a,
                },
                Self::TenantNameAlreadyExists {
                    tenant_name: name_b,
                },
            ) => name_a == name_b,
            (
                Self::GenerationConflict {
                    descriptor_key: key_a,
                    expected: expected_a,
                    actual: actual_a,
                },
                Self::GenerationConflict {
                    descriptor_key: key_b,
                    expected: expected_b,
                    actual: actual_b,
                },
            ) => key_a == key_b && expected_a == expected_b && actual_a == actual_b,
            (
                Self::InvalidState {
                    descriptor_key: key_a,
                    state: state_a,
                },
                Self::InvalidState {
                    descriptor_key: key_b,
                    state: state_b,
                },
            ) => key_a == key_b && state_a == state_b,
            (
                Self::InvalidTimestamp {
                    field: field_a,
                    value: value_a,
                },
                Self::InvalidTimestamp {
                    field: field_b,
                    value: value_b,
                },
            ) => field_a == field_b && value_a == value_b,
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            (
                Self::InvalidControlWritePermit {
                    stream_family: family_a,
                    partition: partition_a,
                    reason: reason_a,
                },
                Self::InvalidControlWritePermit {
                    stream_family: family_b,
                    partition: partition_b,
                    reason: reason_b,
                },
            ) => family_a == family_b && partition_a == partition_b && reason_a == reason_b,
            (
                Self::ControlFenceRejected {
                    stream_family: family_a,
                    partition: partition_a,
                    code: code_a,
                    reason: reason_a,
                },
                Self::ControlFenceRejected {
                    stream_family: family_b,
                    partition: partition_b,
                    code: code_b,
                    reason: reason_b,
                },
            ) => {
                family_a == family_b
                    && partition_a == partition_b
                    && code_a == code_b
                    && reason_a == reason_b
            }
            (
                Self::ControlStreamWrite {
                    stream_family: family_a,
                    partition: partition_a,
                    message: source_a,
                },
                Self::ControlStreamWrite {
                    stream_family: family_b,
                    partition: partition_b,
                    message: source_b,
                },
            ) => family_a == family_b && partition_a == partition_b && source_a == source_b,
            _ => false,
        }
    }
}

impl Eq for MeshDirectoryError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MeshId(String);

impl MeshId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "mesh id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MeshId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "tenant id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(TENANT_LOCATOR_PARTITION_DOMAIN, &[self.as_str()])
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "tenants",
            &self.partition(),
            &format!("{}{}", self.as_str(), DESCRIPTOR_FILE_EXTENSION),
        ])
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantName(String);

impl TenantName {
    pub fn canonicalize(value: impl AsRef<str>) -> MeshDirectoryResult<Self> {
        let raw = value.as_ref();
        if raw.contains('.') || !raw.is_ascii() {
            return Err(MeshDirectoryError::InvalidTenantName(raw.to_string()));
        }
        let canonical = raw.to_ascii_lowercase();
        validate_dns_label_name(&canonical)
            .map_err(|_| MeshDirectoryError::InvalidTenantName(raw.to_string()))?;
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(TENANT_NAME_PARTITION_DOMAIN, &[self.as_str()])
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "tenant-names",
            &self.partition(),
            &format!("{}{}", self.as_str(), DESCRIPTOR_FILE_EXTENSION),
        ])
    }
}

impl fmt::Display for TenantName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketName(String);

impl BucketName {
    pub fn canonicalize(value: impl AsRef<str>) -> MeshDirectoryResult<Self> {
        let raw = value.as_ref();
        if !raw.is_ascii() {
            return Err(MeshDirectoryError::InvalidBucketName(raw.to_string()));
        }
        let canonical = raw.to_ascii_lowercase();
        if !validation::is_valid_bucket_name(&canonical) {
            return Err(MeshDirectoryError::InvalidBucketName(raw.to_string()));
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketId(String);

impl BucketId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "bucket id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RegionName(String);

impl RegionName {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        if !validation::is_valid_region_name(&value) {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "region",
                value,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RegionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CellId(String);

impl CellId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "cell id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TenantNameStatus {
    Reserved,
    Active,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TenantLocatorStatus {
    Creating,
    Active,
    Suspended,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketLocatorStatus {
    Creating,
    Active,
    ReadOnly,
    Moving,
    Draining,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantNameDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_name: TenantName,
    pub tenant_id: TenantId,
    pub status: TenantNameStatus,
    pub idempotency_key: Option<String>,
    pub reservation_expires_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl TenantNameDescriptor {
    pub fn reserved(
        mesh_id: MeshId,
        tenant_name: TenantName,
        tenant_id: TenantId,
        idempotency_key: impl Into<String>,
        reservation_expires_at: impl Into<String>,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let idempotency_key = idempotency_key.into();
        let reservation_expires_at = reservation_expires_at.into();
        let now = now.into();
        require_nonempty(&idempotency_key, "idempotency key")?;
        require_nonempty(&reservation_expires_at, "reservation expiry")?;
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_NAME_SCHEMA.to_string(),
            mesh_id,
            tenant_name,
            tenant_id,
            status: TenantNameStatus::Reserved,
            idempotency_key: Some(idempotency_key),
            reservation_expires_at: Some(reservation_expires_at),
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn active(
        mesh_id: MeshId,
        tenant_name: TenantName,
        tenant_id: TenantId,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_NAME_SCHEMA.to_string(),
            mesh_id,
            tenant_name,
            tenant_id,
            status: TenantNameStatus::Active,
            idempotency_key: None,
            reservation_expires_at: None,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn activate(&self, now: impl Into<String>) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        if self.status != TenantNameStatus::Reserved {
            return Err(MeshDirectoryError::InvalidState {
                descriptor_key: self.descriptor_key(),
                state: format!("{:?}", self.status),
            });
        }
        let mut active = self.clone();
        active.status = TenantNameStatus::Active;
        active.reservation_expires_at = None;
        active.updated_at = now;
        active.generation += 1;
        Ok(active)
    }

    pub fn tombstone(&self, now: impl Into<String>) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        let mut tombstone = self.clone();
        tombstone.status = TenantNameStatus::Tombstoned;
        tombstone.updated_at = now;
        tombstone.generation += 1;
        Ok(tombstone)
    }

    pub fn descriptor_key(&self) -> String {
        self.tenant_name.descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.tenant_name.partition_key()
    }

    pub fn partition(&self) -> String {
        self.tenant_name.partition()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantLocatorDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_id: TenantId,
    pub tenant_name: TenantName,
    pub home_region: RegionName,
    pub status: TenantLocatorStatus,
    pub profile_revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl TenantLocatorDescriptor {
    pub fn active(
        mesh_id: MeshId,
        tenant_id: TenantId,
        tenant_name: TenantName,
        home_region: RegionName,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_LOCATOR_SCHEMA.to_string(),
            mesh_id,
            tenant_id,
            tenant_name,
            home_region,
            status: TenantLocatorStatus::Active,
            profile_revision: 1,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn descriptor_key(&self) -> String {
        self.tenant_id.descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.tenant_id.partition_key()
    }

    pub fn partition(&self) -> String {
        self.tenant_id.partition()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BucketLocatorKey {
    pub tenant_id: TenantId,
    pub bucket_name: BucketName,
}

impl BucketLocatorKey {
    pub fn new(tenant_id: TenantId, bucket_name: BucketName) -> Self {
        Self {
            tenant_id,
            bucket_name,
        }
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(
            BUCKET_LOCATOR_PARTITION_DOMAIN,
            &[self.tenant_id.as_str(), self.bucket_name.as_str()],
        )
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "buckets",
            &self.partition(),
            self.tenant_id.as_str(),
            &format!("{}{}", self.bucket_name.as_str(), DESCRIPTOR_FILE_EXTENSION),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketLocatorDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_id: TenantId,
    pub bucket_name: BucketName,
    pub bucket_id: BucketId,
    pub home_region: RegionName,
    pub home_cell: CellId,
    pub status: BucketLocatorStatus,
    pub placement_policy: String,
    pub object_prefix: String,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl BucketLocatorDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        mesh_id: MeshId,
        tenant_id: TenantId,
        bucket_name: BucketName,
        bucket_id: BucketId,
        home_region: RegionName,
        home_cell: CellId,
        placement_policy: impl Into<String>,
        object_prefix: impl Into<String>,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let placement_policy = placement_policy.into();
        require_nonempty(&placement_policy, "placement policy")?;
        let object_prefix = object_prefix.into();
        require_control_path_fragment(&object_prefix, "object prefix")?;
        let now = now.into();
        require_nonempty(&now, "timestamp")?;

        Ok(Self {
            schema: BUCKET_LOCATOR_SCHEMA.to_string(),
            mesh_id,
            tenant_id,
            bucket_name,
            bucket_id,
            home_region,
            home_cell,
            status: BucketLocatorStatus::Active,
            placement_policy,
            object_prefix,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn key(&self) -> BucketLocatorKey {
        BucketLocatorKey::new(self.tenant_id.clone(), self.bucket_name.clone())
    }

    pub fn descriptor_key(&self) -> String {
        self.key().descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.key().partition_key()
    }

    pub fn partition(&self) -> String {
        self.key().partition()
    }
}

pub fn host_alias_partition_key(hostname: &str) -> MeshDirectoryResult<Vec<u8>> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    Ok(partition_key_bytes(
        HOST_ALIAS_PARTITION_DOMAIN,
        &[&hostname],
    ))
}

pub fn host_alias_partition(hostname: &str) -> MeshDirectoryResult<String> {
    Ok(stable_partition_prefix(&host_alias_partition_key(
        hostname,
    )?))
}

pub fn host_alias_descriptor_key(hostname: &str) -> MeshDirectoryResult<String> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    let partition = host_alias_partition(&hostname)?;
    Ok(join_mesh_key(&[
        "host-aliases",
        &partition,
        &format!("{hostname}{DESCRIPTOR_FILE_EXTENSION}"),
    ]))
}

pub async fn write_host_alias_descriptor_in_transaction(
    _storage: &Storage,
    _descriptor: &routing::HostAliasDescriptor,
    _require_absent: bool,
    _transaction_id: &str,
    _principal: &str,
) -> MeshDirectoryResult<()> {
    Err(routing_transaction_rejected())
}

pub async fn write_host_alias_descriptor(
    storage: &Storage,
    descriptor: &routing::HostAliasDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    let hostname = routing::normalize_alias_hostname(&descriptor.hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: descriptor.hostname.clone(),
        }
    })?;
    let partition = host_alias_partition(&hostname)?;
    let existing = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::HostAlias,
        &hostname,
        authority,
    )
    .await?;
    if let Some(existing) = existing
        && existing == *descriptor
    {
        return Ok(());
    }
    append_control_mutation(
        storage,
        RoutingRecordFamily::HostAlias,
        &partition,
        &hostname,
        "upsert",
        descriptor
            .generation
            .checked_sub(1)
            .filter(|generation| *generation > 0),
        descriptor.generation,
        None,
        descriptor,
        authority,
    )
    .await?;
    Ok(())
}

pub async fn read_host_alias_descriptor(
    storage: &Storage,
    hostname: &str,
) -> MeshDirectoryResult<Option<routing::HostAliasDescriptor>> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    read_typed_routing_descriptor(storage, RoutingRecordFamily::HostAlias, &hostname).await
}

pub(crate) fn read_host_alias_descriptor_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    hostname: &str,
) -> MeshDirectoryResult<Option<routing::HostAliasDescriptor>> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    let descriptor_key = host_alias_descriptor_key(&hostname)?;
    let Some(payload_proto) = read_descriptor_projection_payload_proto_mvcc(mvcc, &descriptor_key)?
    else {
        return Ok(None);
    };
    let descriptor: routing::HostAliasDescriptor =
        record_proto::decode_typed_routing_descriptor(&payload_proto)?;
    if descriptor.routing_record_key() != hostname {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "host alias record key",
            value: format!(
                "expected {hostname}, got {}",
                descriptor.routing_record_key()
            ),
        });
    }
    Ok(Some(descriptor))
}

pub(crate) fn list_host_alias_descriptors_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
) -> MeshDirectoryResult<Vec<routing::HostAliasDescriptor>> {
    let tuple_prefix = routing_projection_row_prefix(RoutingRecordFamily::HostAlias)?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let mut aliases = Vec::new();
    for (_, row) in mvcc.runtime.scan_table_prefix_at(
        TABLE_MESH_PARTITION_ROW,
        &application_prefix,
        snapshot,
    )? {
        let projection = record_proto::decode_routing_projection_row(&row.value)?;
        if projection.descriptor.family != RoutingRecordFamily::HostAlias {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "host alias projection family",
                value: format!("{:?}", projection.descriptor.family),
            });
        }
        let descriptor: routing::HostAliasDescriptor =
            record_proto::decode_typed_routing_descriptor(&projection.payload_proto)?;
        if descriptor.routing_record_key() != projection.descriptor.record_key {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "host alias projection record key",
                value: format!(
                    "expected {}, got {}",
                    projection.descriptor.record_key,
                    descriptor.routing_record_key()
                ),
            });
        }
        aliases.push(descriptor);
    }
    Ok(aliases)
}

pub(crate) fn list_bucket_locators_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
) -> MeshDirectoryResult<Vec<BucketLocatorDescriptor>> {
    let tuple_prefix = routing_projection_row_prefix(RoutingRecordFamily::BucketLocator)?;
    let application_prefix =
        crate::mvcc_product::coremeta_application_prefix(CF_MESH, &tuple_prefix)?;
    let snapshot = mvcc.runtime.applied_version()?;
    let mut locators = Vec::new();
    for (_, row) in mvcc.runtime.scan_table_prefix_at(
        TABLE_MESH_PARTITION_ROW,
        &application_prefix,
        snapshot,
    )? {
        let projection = record_proto::decode_routing_projection_row(&row.value)?;
        let descriptor: BucketLocatorDescriptor =
            record_proto::decode_typed_routing_descriptor(&projection.payload_proto)?;
        if projection.descriptor.family != RoutingRecordFamily::BucketLocator
            || descriptor.routing_record_key() != projection.descriptor.record_key
        {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "bucket locator projection record key",
                value: projection.descriptor.record_key,
            });
        }
        locators.push(descriptor);
    }
    Ok(locators)
}

#[derive(Debug, Clone, Default)]
pub struct BucketLocatorDirectory {
    locators: BTreeMap<BucketLocatorKey, BucketLocatorDescriptor>,
}

impl BucketLocatorDirectory {
    pub fn insert(&mut self, locator: BucketLocatorDescriptor) -> MeshDirectoryResult<()> {
        let key = locator.key();
        match self.locators.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(locator);
                Ok(())
            }
            Entry::Occupied(entry) => Err(MeshDirectoryError::DuplicateBucketLocator {
                tenant_id: entry.key().tenant_id.to_string(),
                bucket_name: entry.key().bucket_name.to_string(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.locators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }
}

pub fn stable_partition_prefix(canonical_key: &[u8]) -> String {
    let digest = blake3::hash(canonical_key);
    let bytes = digest.as_bytes();
    format!("{:02x}{:02x}", bytes[0], bytes[1])
}

async fn append_control_mutation<T: StoredRoutingRecord>(
    _storage: &Storage,
    family: RoutingRecordFamily,
    partition: &str,
    record_key: &str,
    operation: &str,
    expected_generation: Option<u64>,
    new_generation: u64,
    idempotency_key: Option<&str>,
    payload: &T,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    let mvcc = authority.mvcc.ok_or_else(|| {
        MeshDirectoryError::Other(anyhow::anyhow!(
            "mesh control writes require cluster MVCC authority"
        ))
    })?;
    let stream_family = family.stream_family();
    let expected_partition_id = control_partition_id(stream_family, partition);
    if authority.permit.partition_family != CONTROL_PARTITION_FAMILY {
        return Err(MeshDirectoryError::InvalidControlWritePermit {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            reason: format!(
                "expected partition family {CONTROL_PARTITION_FAMILY}, got {}",
                authority.permit.partition_family
            ),
        });
    }
    if authority.permit.partition_id != expected_partition_id {
        return Err(MeshDirectoryError::InvalidControlWritePermit {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            reason: "permit partition id does not match control stream partition".to_string(),
        });
    }
    let mvcc_fence = partition_fence::partition_write_predicate_mvcc(
        mvcc,
        authority.permit,
        authority.signing_key,
    )
    .map_err(|rejection| MeshDirectoryError::ControlFenceRejected {
        stream_family: stream_family.to_string(),
        partition: partition.to_string(),
        code: rejection.code.as_str(),
        reason: rejection.reason,
    })?;

    let head_tuple_key = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(ROUTING_CONTROL_HEAD_KIND),
        CoreMetaTuplePart::Utf8(stream_family),
        CoreMetaTuplePart::Utf8(partition),
    ])?;
    let head_key = crate::mvcc_product::coremeta_logical_key(
        CF_MESH,
        TABLE_MESH_PARTITION_ROW,
        &head_tuple_key,
    )?;
    let current_head = mvcc.read_latest_value(&head_key)?;
    let head = current_head
        .as_deref()
        .map(serde_json::from_slice::<RoutingControlHead>)
        .transpose()
        .map_err(|error| MeshDirectoryError::Other(error.into()))?
        .unwrap_or(RoutingControlHead {
            next_sequence: 1,
            next_byte_offset: 0,
        });
    let cursor = mesh_control_stream::ControlStreamAppendCursor {
        sequence: mesh_control_stream::ControlStreamSequence::new(head.next_sequence).map_err(
            |error| MeshDirectoryError::ControlStreamWrite {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                message: error.to_string(),
            },
        )?,
        byte_offset: head.next_byte_offset,
    };
    let payload_proto = payload.encode_routing_payload_proto()?;
    let digest = ControlRecordDigest::blake3(&payload_proto);
    let stable_idempotency = idempotency_key.map(str::to_string).unwrap_or_else(|| {
        format!(
            "routing:{stream_family}:{partition}:{record_key}:{operation}:{new_generation}:{}",
            digest
        )
    });
    let created_at = Utc::now().to_rfc3339();
    let mesh_id = payload.routing_mesh_id();
    let header_proto =
        mesh_control_stream::encode_control_mutation_header(ControlMutationHeaderInput {
            schema: CONTROL_MUTATION_SCHEMA,
            mesh_id: &mesh_id,
            stream_family,
            partition,
            sequence: cursor.sequence,
            record_key,
            operation,
            expected_generation,
            new_generation,
            writer_node_id: authority.permit.owner_node_id.as_str(),
            writer_fence: authority.permit.fence_token,
            idempotency_key: Some(&stable_idempotency),
            record_digest: &digest,
            created_at: &created_at,
            byte_offset: cursor.byte_offset,
        });
    let frame = ControlStreamFrame::new(header_proto, payload_proto);
    let descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let projection_tuple_key = routing_projection_row_key(family, record_key)?;
    let projection_key = crate::mvcc_product::coremeta_logical_key(
        CF_MESH,
        TABLE_MESH_PARTITION_ROW,
        &projection_tuple_key,
    )?;
    let current_projection = mvcc.read_latest_value(&projection_key)?;
    let projection_payload = record_proto::encode_routing_projection_row(&descriptor_key, payload)?;
    let encoded_len = u64::try_from(frame.encoded_len().map_err(|error| {
        MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: error.to_string(),
        }
    })?)
    .map_err(|error| MeshDirectoryError::Other(error.into()))?;
    let next_sequence = cursor.sequence.get().checked_add(1).ok_or_else(|| {
        MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: "control stream sequence overflow".to_string(),
        }
    })?;
    let next_byte_offset = cursor.byte_offset.checked_add(encoded_len).ok_or_else(|| {
        MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: "control stream byte offset overflow".to_string(),
        }
    })?;
    let prepared = mesh_control_stream::prepare_control_stream_append_at_cursor(
        stream_family,
        partition,
        &frame,
        None,
        MESH_DIRECTORY_PROJECTION_PARTITION_ID,
        cursor,
        None,
    )
    .await
    .map_err(|err| MeshDirectoryError::ControlStreamWrite {
        stream_family: stream_family.to_string(),
        partition: partition.to_string(),
        message: format!("{err:#}"),
    })?;
    let mut operations = prepared.operations;
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_MESH_PARTITION_ROW,
        tuple_key: projection_tuple_key,
        payload: projection_payload,
    });
    operations.push(CoreMutationOperation::CoreMetaPut {
        partition_id: MESH_DIRECTORY_PROJECTION_PARTITION_ID.to_string(),
        cf: CF_MESH.to_string(),
        table_id: TABLE_MESH_PARTITION_ROW,
        tuple_key: head_tuple_key.clone(),
        payload: serde_json::to_vec(&RoutingControlHead {
            next_sequence,
            next_byte_offset,
        })
        .map_err(|error| MeshDirectoryError::Other(error.into()))?,
    });
    let mut plan = crate::mvcc_product::product_mutations_and_outbox_from_operations(operations)?;
    plan.predicates.push((
        projection_key,
        current_projection
            .as_ref()
            .map(|value| {
                crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(value).as_bytes())
            })
            .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent),
    ));
    plan.predicates.push((
        head_key,
        current_head
            .as_ref()
            .map(|value| {
                crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(value).as_bytes())
            })
            .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent),
    ));
    plan.predicates.push(mvcc_fence);
    mvcc.autocommit_product_mutations_with_predicates_and_outbox(
        &format!("partition-owner:{}", authority.permit.owner_node_id),
        &stable_idempotency,
        plan.mutations,
        plan.predicates,
        plan.outbox_events,
        crate::mvcc_transaction::DurabilityLevel::Quorum,
        Utc::now().timestamp_millis().max(0) as u64,
    )
    .await
    .map_err(MeshDirectoryError::Other)?;
    Ok(())
}

pub async fn reserve_tenant_name(
    storage: &Storage,
    descriptor: &TenantNameDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    if descriptor.status != TenantNameStatus::Reserved {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: descriptor.descriptor_key(),
            state: format!("{:?}", descriptor.status),
        });
    }
    if descriptor
        .idempotency_key
        .as_deref()
        .unwrap_or_default()
        .is_empty()
        || descriptor
            .reservation_expires_at
            .as_deref()
            .unwrap_or_default()
            .is_empty()
    {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: descriptor.descriptor_key(),
            state: "reserved tenant-name requires idempotency_key and reservation_expires_at"
                .to_string(),
        });
    }

    if let Some(existing) = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::TenantName,
        descriptor.tenant_name.as_str(),
        authority,
    )
    .await?
    {
        if existing.tenant_id == descriptor.tenant_id
            && (existing.status == TenantNameStatus::Active
                || existing.idempotency_key == descriptor.idempotency_key)
        {
            return Ok(existing);
        }
        return Err(MeshDirectoryError::TenantNameAlreadyExists {
            tenant_name: descriptor.tenant_name.as_str().to_string(),
        });
    }

    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &descriptor.partition(),
        descriptor.tenant_name.as_str(),
        "create",
        None,
        descriptor.generation,
        descriptor.idempotency_key.as_deref(),
        descriptor,
        authority,
    )
    .await?;
    Ok(descriptor.clone())
}

pub async fn create_tenant_locator(
    storage: &Storage,
    locator: &TenantLocatorDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantLocatorDescriptor> {
    if let Some(existing) = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::TenantLocator,
        locator.tenant_id.as_str(),
        authority,
    )
    .await?
    {
        if existing.tenant_id == locator.tenant_id
            && existing.tenant_name == locator.tenant_name
            && existing.home_region == locator.home_region
        {
            return Ok(existing);
        }
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: locator.descriptor_key(),
            expected: 0,
            actual: existing.generation,
        });
    }

    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantLocator,
        &locator.partition(),
        locator.tenant_id.as_str(),
        "create",
        None,
        locator.generation,
        None,
        locator,
        authority,
    )
    .await?;
    Ok(locator.clone())
}

pub async fn activate_tenant_name(
    storage: &Storage,
    tenant_name: &TenantName,
    tenant_id: &TenantId,
    expected_generation: u64,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    let now = now.into();
    let existing = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::TenantName,
        tenant_name.as_str(),
        authority,
    )
    .await?
    .ok_or_else(|| MeshDirectoryError::NotFound(tenant_name.descriptor_key()))?;
    if existing.tenant_id != *tenant_id {
        return Err(MeshDirectoryError::TenantNameAlreadyExists {
            tenant_name: tenant_name.as_str().to_string(),
        });
    }
    if existing.status == TenantNameStatus::Active {
        return Ok(existing);
    }
    if existing.status != TenantNameStatus::Reserved {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: existing.descriptor_key(),
            state: format!("{:?}", existing.status),
        });
    }
    if existing.generation != expected_generation {
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: existing.descriptor_key(),
            expected: expected_generation,
            actual: existing.generation,
        });
    }
    let active = existing.activate(now)?;
    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &active.partition(),
        active.tenant_name.as_str(),
        "upsert",
        Some(expected_generation),
        active.generation,
        active.idempotency_key.as_deref(),
        &active,
        authority,
    )
    .await?;
    Ok(active)
}

pub async fn tombstone_tenant_name(
    storage: &Storage,
    tenant_name: &TenantName,
    expected_generation: u64,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    let existing = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::TenantName,
        tenant_name.as_str(),
        authority,
    )
    .await?
    .ok_or_else(|| MeshDirectoryError::NotFound(tenant_name.descriptor_key()))?;
    if existing.generation != expected_generation {
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: existing.descriptor_key(),
            expected: expected_generation,
            actual: existing.generation,
        });
    }
    let tombstone = existing.tombstone(now)?;
    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &tombstone.partition(),
        tombstone.tenant_name.as_str(),
        "tombstone",
        Some(expected_generation),
        tombstone.generation,
        tombstone.idempotency_key.as_deref(),
        &tombstone,
        authority,
    )
    .await?;
    Ok(tombstone)
}

pub async fn recover_tenant_name_reservation(
    storage: &Storage,
    tenant_name: &TenantName,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<Option<TenantNameDescriptor>> {
    let now = now.into();
    let Some(existing) = read_tenant_name_descriptor(storage, tenant_name).await? else {
        return Ok(None);
    };
    if existing.status != TenantNameStatus::Reserved {
        return Ok(Some(existing));
    }

    if let Some(locator) = read_tenant_locator_descriptor(storage, &existing.tenant_id).await?
        && locator.tenant_id == existing.tenant_id
        && locator.tenant_name == existing.tenant_name
    {
        return activate_tenant_name(
            storage,
            tenant_name,
            &existing.tenant_id,
            existing.generation,
            now,
            authority,
        )
        .await
        .map(Some);
    }

    let expires_at = existing.reservation_expires_at.as_deref().ok_or_else(|| {
        MeshDirectoryError::InvalidState {
            descriptor_key: existing.descriptor_key(),
            state: "reserved tenant-name missing reservation_expires_at".to_string(),
        }
    })?;
    let expires_at = parse_rfc3339(expires_at, "reservation_expires_at")?;
    let now_dt = parse_rfc3339(&now, "now")?;
    if expires_at <= now_dt {
        return tombstone_tenant_name(storage, tenant_name, existing.generation, now, authority)
            .await
            .map(Some);
    }

    Ok(Some(existing))
}

pub async fn write_bucket_locator(
    storage: &Storage,
    locator: &BucketLocatorDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    let locator_record_key = format!(
        "{}/{}",
        locator.tenant_id.as_str(),
        locator.bucket_name.as_str()
    );
    if let Some(existing) = read_typed_routing_descriptor_for_authority(
        storage,
        RoutingRecordFamily::BucketLocator,
        &locator_record_key,
        authority,
    )
    .await?
    {
        if existing == *locator {
            return Ok(());
        }
        if existing.bucket_id != locator.bucket_id
            && existing.status != BucketLocatorStatus::Deleted
        {
            return Err(MeshDirectoryError::DuplicateBucketLocator {
                tenant_id: locator.tenant_id.to_string(),
                bucket_name: locator.bucket_name.to_string(),
            });
        }
    }
    append_control_mutation(
        storage,
        RoutingRecordFamily::BucketLocator,
        &locator.partition(),
        &locator_record_key,
        "upsert",
        locator
            .generation
            .checked_sub(1)
            .filter(|generation| *generation > 0),
        locator.generation,
        None,
        locator,
        authority,
    )
    .await?;
    Ok(())
}

pub async fn write_bucket_locator_in_transaction(
    _storage: &Storage,
    _locator: &BucketLocatorDescriptor,
    _require_absent: bool,
    _transaction_id: &str,
    _principal: &str,
) -> MeshDirectoryResult<()> {
    Err(routing_transaction_rejected())
}

pub async fn read_tenant_name_descriptor(
    storage: &Storage,
    tenant_name: &TenantName,
) -> MeshDirectoryResult<Option<TenantNameDescriptor>> {
    read_typed_routing_descriptor(
        storage,
        RoutingRecordFamily::TenantName,
        tenant_name.as_str(),
    )
    .await
}

pub async fn read_tenant_locator_descriptor(
    storage: &Storage,
    tenant_id: &TenantId,
) -> MeshDirectoryResult<Option<TenantLocatorDescriptor>> {
    read_typed_routing_descriptor(
        storage,
        RoutingRecordFamily::TenantLocator,
        tenant_id.as_str(),
    )
    .await
}

pub async fn read_bucket_locator(
    storage: &Storage,
    key: &BucketLocatorKey,
) -> MeshDirectoryResult<Option<BucketLocatorDescriptor>> {
    let record_key = format!("{}/{}", key.tenant_id.as_str(), key.bucket_name.as_str());
    read_typed_routing_descriptor(storage, RoutingRecordFamily::BucketLocator, &record_key).await
}

pub fn routing_record_partition_for_key(
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<String> {
    match family {
        RoutingRecordFamily::TenantName => Ok(TenantName::canonicalize(record_key)?.partition()),
        RoutingRecordFamily::TenantLocator => Ok(TenantId::new(record_key)?.partition()),
        RoutingRecordFamily::BucketLocator => {
            let (tenant_id, bucket_name) = bucket_record_key(record_key)?;
            Ok(BucketLocatorKey::new(tenant_id, bucket_name).partition())
        }
        RoutingRecordFamily::HostAlias => host_alias_partition(record_key),
    }
}

pub fn routing_record_descriptor_key_for_key(
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<String> {
    match family {
        RoutingRecordFamily::TenantName => {
            Ok(TenantName::canonicalize(record_key)?.descriptor_key())
        }
        RoutingRecordFamily::TenantLocator => Ok(TenantId::new(record_key)?.descriptor_key()),
        RoutingRecordFamily::BucketLocator => {
            let (tenant_id, bucket_name) = bucket_record_key(record_key)?;
            Ok(BucketLocatorKey::new(tenant_id, bucket_name).descriptor_key())
        }
        RoutingRecordFamily::HostAlias => host_alias_descriptor_key(record_key),
    }
}

pub async fn read_routing_record_descriptor(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    read_routing_record_from_source_of_truth(storage, family, record_key)
        .await?
        .ok_or_else(|| MeshDirectoryError::NotFound(record_key.to_string()))
}

pub(crate) fn control_payload_operator_json(
    family: RoutingRecordFamily,
    record_key: &str,
    payload_proto: &[u8],
) -> MeshDirectoryResult<Vec<u8>> {
    record_proto::control_payload_operator_json(family, record_key, payload_proto)
}

pub(crate) fn encode_control_payload_from_operator_json(
    family: RoutingRecordFamily,
    payload_json: &[u8],
) -> MeshDirectoryResult<Vec<u8>> {
    record_proto::encode_control_payload_from_operator_json(family, payload_json)
}

async fn read_routing_record_from_source_of_truth(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingRecordDescriptor>> {
    let projected = read_projected_routing_record_source(storage, family, record_key).await?;
    let streamed = latest_routing_record_from_control_stream(storage, family, record_key).await?;
    let streamed = match streamed {
        None => return Ok(projected.map(|source| source.descriptor)),
        Some(RoutingControlStreamState::Deleted) => return Ok(None),
        Some(RoutingControlStreamState::Present(streamed)) => streamed,
    };
    if projected.as_ref().is_none_or(|projected| {
        projected.descriptor.generation != streamed.descriptor.generation
            || projected.payload_proto != streamed.payload_proto
    }) {
        rebuild_routing_record_projection_from_proto(
            storage,
            family,
            record_key,
            &streamed.payload_proto,
        )
        .await?;
    }
    Ok(Some(streamed.descriptor))
}

enum RoutingControlStreamState {
    Present(RoutingRecordSource),
    Deleted,
}

async fn latest_routing_record_from_control_stream(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingControlStreamState>> {
    let partition = routing_record_partition_for_key(family, record_key)?;
    let stream_family = family.stream_family();
    let latest = mesh_control_stream::latest_projected_record_from_control_stream(
        storage,
        stream_family,
        &partition,
        record_key,
    )
    .await
    .map_err(|err| MeshDirectoryError::ControlStreamWrite {
        stream_family: stream_family.to_string(),
        partition: partition.clone(),
        message: format!("{err:#}"),
    })?;
    let Some(latest) = latest else {
        return Ok(None);
    };
    if latest.deleted {
        return Ok(Some(RoutingControlStreamState::Deleted));
    }
    let payload_proto = encode_control_payload_from_operator_json(family, &latest.payload_json)?;
    let descriptor = routing_record_descriptor_from_proto(family, record_key, &payload_proto)?;
    Ok(Some(RoutingControlStreamState::Present(
        RoutingRecordSource {
            descriptor,
            payload_proto,
        },
    )))
}

async fn read_projected_routing_record_source(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingRecordSource>> {
    let descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let Some(payload_proto) =
        read_descriptor_projection_payload_proto(storage, &descriptor_key).await?
    else {
        return Ok(None);
    };
    Ok(Some(RoutingRecordSource {
        descriptor: routing_record_descriptor_from_proto(family, record_key, &payload_proto)?,
        payload_proto,
    }))
}

async fn read_typed_routing_descriptor<T: DecodeRoutingRecord + StoredRoutingRecord>(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<T>> {
    let Some(_record) =
        read_routing_record_from_source_of_truth(storage, family, record_key).await?
    else {
        return Ok(None);
    };
    let descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let Some(payload_proto) =
        read_descriptor_projection_payload_proto(storage, &descriptor_key).await?
    else {
        return Ok(None);
    };
    let descriptor: T = record_proto::decode_typed_routing_descriptor(&payload_proto)?;
    if descriptor.routing_record_key() != record_key {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record protobuf record key",
            value: format!(
                "expected {record_key}, got {}",
                descriptor.routing_record_key()
            ),
        });
    }
    Ok(Some(descriptor))
}

async fn read_typed_routing_descriptor_for_authority<
    T: DecodeRoutingRecord + StoredRoutingRecord,
>(
    _storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<Option<T>> {
    let mvcc = authority.mvcc.ok_or_else(|| {
        MeshDirectoryError::Other(anyhow::anyhow!(
            "mesh control reads require cluster MVCC authority"
        ))
    })?;
    let descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let Some(payload_proto) = read_descriptor_projection_payload_proto_mvcc(mvcc, &descriptor_key)?
    else {
        return Ok(None);
    };
    let descriptor: T = record_proto::decode_typed_routing_descriptor(&payload_proto)?;
    if descriptor.routing_record_key() != record_key {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record protobuf record key",
            value: format!(
                "expected {record_key}, got {}",
                descriptor.routing_record_key()
            ),
        });
    }
    Ok(Some(descriptor))
}

pub async fn rebuild_routing_record_projection_from_payload_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: RoutingRecordFamily,
    record_key: &str,
    payload_json: &[u8],
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let descriptor = match family {
        RoutingRecordFamily::TenantName => {
            let descriptor: TenantNameDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor_mvcc(mvcc, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::TenantLocator => {
            let descriptor: TenantLocatorDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor_mvcc(mvcc, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::BucketLocator => {
            let descriptor: BucketLocatorDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor_mvcc(mvcc, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::HostAlias => {
            let descriptor: routing::HostAliasDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(
                &host_alias_descriptor_key(&descriptor.hostname)?,
                &expected_descriptor_key,
            )?;
            write_descriptor_mvcc(mvcc, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
    };
    Ok(descriptor)
}

async fn rebuild_routing_record_projection_from_proto(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
    payload_proto: &[u8],
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let descriptor = match family {
        RoutingRecordFamily::TenantName => {
            let descriptor: TenantNameDescriptor =
                record_proto::decode_typed_routing_descriptor(payload_proto)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::TenantLocator => {
            let descriptor: TenantLocatorDescriptor =
                record_proto::decode_typed_routing_descriptor(payload_proto)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::BucketLocator => {
            let descriptor: BucketLocatorDescriptor =
                record_proto::decode_typed_routing_descriptor(payload_proto)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
        RoutingRecordFamily::HostAlias => {
            let descriptor: routing::HostAliasDescriptor =
                record_proto::decode_typed_routing_descriptor(payload_proto)?;
            ensure_descriptor_key_matches(
                &host_alias_descriptor_key(&descriptor.hostname)?,
                &expected_descriptor_key,
            )?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
            routing_record_descriptor_from_record(&descriptor)?
        }
    };
    Ok(descriptor)
}

async fn write_descriptor<T: StoredRoutingRecord>(
    storage: &Storage,
    descriptor_key: &str,
    descriptor: &T,
) -> MeshDirectoryResult<()> {
    write_descriptor_projection(storage, descriptor_key, descriptor, false).await?;
    Ok(())
}

async fn write_descriptor_mvcc<T: StoredRoutingRecord>(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    descriptor_key: &str,
    descriptor: &T,
) -> MeshDirectoryResult<()> {
    write_descriptor_projection_mvcc(mvcc, descriptor_key, descriptor, false).await
}

fn bucket_record_key(record_key: &str) -> MeshDirectoryResult<(TenantId, BucketName)> {
    let (tenant_id, bucket_name) =
        record_key
            .split_once('/')
            .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
                field: "bucket routing record key",
                value: record_key.to_string(),
            })?;
    Ok((
        TenantId::new(tenant_id)?,
        BucketName::canonicalize(bucket_name)?,
    ))
}

fn ensure_descriptor_key_matches(actual: &str, expected: &str) -> MeshDirectoryResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record payload descriptor key",
            value: format!("expected {expected}, got {actual}"),
        })
    }
}

#[cfg(test)]
mod tests;
