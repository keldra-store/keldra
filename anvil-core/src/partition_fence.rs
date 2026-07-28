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
use prost::Message;
use serde::{Deserialize, Serialize};
use std::fmt;

mod coremeta;

pub use coremeta::{
    MAX_PARTITION_FENCE_PAGE_SIZE, OwnershipFencePage, OwnershipFencePageCursor,
    PartitionOwnerPage, PartitionOwnerPageCursor, list_active_ownership_fences_for_node_page,
    list_active_ownership_fences_for_node_page_mvcc, list_ownership_fences_page,
    list_partition_owners_for_node_page, list_partition_owners_for_node_page_mvcc,
    list_partition_owners_page,
};
use coremeta::{
    is_partition_fence_cas_conflict, ownership_fence_by_node_key, ownership_fence_row_key,
    partition_owner_row_key, read_ownership_fence_state, read_ownership_fence_state_mvcc,
    read_partition_owner_state, read_partition_owner_state_mvcc, write_ownership_fence_state,
    write_partition_owner_state, write_partition_owner_state_mvcc,
};

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

mod ownership;
pub use ownership::*;
use ownership::{
    decode_ownership_fence_record, decode_partition_owner_record, encode_ownership_fence_record,
    encode_partition_owner_record, ownership_fence_record_to_proto,
};

pub async fn force_expire_partition_owner_for_node(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    force_expire_partition_owner_for_node_inner(
        storage,
        None,
        partition_family,
        partition_id,
        owner_node_id,
        now_nanos,
        signing_key,
    )
    .await
}

pub(crate) async fn force_expire_partition_owner_for_node_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    force_expire_partition_owner_for_node_inner(
        storage,
        Some(mvcc),
        partition_family,
        partition_id,
        owner_node_id,
        now_nanos,
        signing_key,
    )
    .await
}

async fn force_expire_partition_owner_for_node_inner(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
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
        let Some((ref_value, mut owner)) = read_partition_owner_state_backend(
            storage,
            mvcc,
            partition_family,
            partition_id,
            signing_key,
        )
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
        match write_partition_owner_state_backend(storage, mvcc, &owner, Some(&ref_value)).await {
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
    acquire_partition_recovery_inner(storage, None, request, signing_key).await
}

pub(crate) async fn acquire_partition_recovery_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    acquire_partition_recovery_inner(storage, Some(mvcc), request, signing_key).await
}

async fn acquire_partition_recovery_inner(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    let failover_started_at = std::time::Instant::now();
    validate_recovery_acquire(&request)?;
    for _ in 0..MAX_PARTITION_FENCE_CAS_ATTEMPTS {
        let existing = read_partition_owner_state_backend(
            storage,
            mvcc,
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
        match write_partition_owner_state_backend(
            storage,
            mvcc,
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
    publish_partition_ready_inner(
        storage,
        None,
        partition_family,
        partition_id,
        owner_node_id,
        fence_token,
        recovered_through_sequence,
        recovered_manifest_hash,
        now_nanos,
        signing_key,
    )
    .await
}

pub(crate) async fn publish_partition_ready_mvcc(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    fence_token: u64,
    recovered_through_sequence: u64,
    recovered_manifest_hash: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    publish_partition_ready_inner(
        storage,
        Some(mvcc),
        partition_family,
        partition_id,
        owner_node_id,
        fence_token,
        recovered_through_sequence,
        recovered_manifest_hash,
        now_nanos,
        signing_key,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn publish_partition_ready_inner(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
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
        let Some((ref_value, mut state)) = read_partition_owner_state_backend(
            storage,
            mvcc,
            partition_family,
            partition_id,
            signing_key,
        )
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
        match write_partition_owner_state_backend(storage, mvcc, &state, Some(&ref_value)).await {
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

pub fn partition_write_predicate_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<
    (
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    ),
    FenceRejection,
> {
    let state = read_partition_owner_state_mvcc(
        mvcc,
        &permit.partition_family,
        &permit.partition_id,
        signing_key,
    )
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
    let row_key =
        partition_owner_row_key(&permit.partition_family, &permit.partition_id).map_err(|_| {
            FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner row cannot be addressed",
            }
        })?;
    let key = crate::mvcc_product::coremeta_logical_key(
        CF_LEASES_FENCES,
        TABLE_PARTITION_OWNER_ROW,
        &row_key,
    )
    .map_err(|_| FenceRejection {
        code: AnvilErrorCode::PartitionNotOwned,
        reason: "partition owner row cannot be addressed",
    })?;
    Ok((
        key,
        crate::mvcc_transaction::PredicateKind::ValueHash(*blake3::hash(&payload).as_bytes()),
    ))
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

pub(crate) fn read_partition_owner_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    Ok(
        read_partition_owner_state_mvcc(mvcc, partition_family, partition_id, signing_key)?
            .map(|(_, owner)| owner),
    )
}

async fn read_partition_owner_state_backend(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, PartitionOwnerState)>> {
    match mvcc {
        Some(mvcc) => {
            read_partition_owner_state_mvcc(mvcc, partition_family, partition_id, signing_key)
        }
        None => {
            read_partition_owner_state(storage, partition_family, partition_id, signing_key).await
        }
    }
}

async fn write_partition_owner_state_backend(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    owner: &PartitionOwnerState,
    expected_ref: Option<&Vec<u8>>,
) -> Result<()> {
    match mvcc {
        Some(mvcc) => write_partition_owner_state_mvcc(mvcc, owner, expected_ref).await,
        None => write_partition_owner_state(storage, owner, expected_ref).await,
    }
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
