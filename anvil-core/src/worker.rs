use crate::auth::JwtManager;
use crate::core_store::CoreStore;
use crate::crypto::EncryptionKeyring;
use crate::object_manager::ObjectManager;
use crate::partition_fence::{
    OWNERSHIP_CAS_CONFLICT, OWNERSHIP_HELD, OWNERSHIP_OWNER_MISMATCH, OWNERSHIP_STALE_FENCE,
};
use crate::persistence::Object;
use crate::persistence::Persistence;
use crate::task_execution_guard::TaskExecutionGuard;
use crate::task_lease::{
    self, LEASE_CAS_CONFLICT, LEASE_EXPIRED, LEASE_HELD, LEASE_OWNER_MISMATCH, STALE_FENCE,
    TaskLease,
};
use crate::tasks::{
    HFIngestionItemState, HFIngestionState, RebalanceShardTaskPayload, TaskStatus, TaskType,
};
use anyhow::{Result, anyhow};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::boxed::Box;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tonic::Status;
use tracing::{debug, error, info, warn};

mod hf_index;

type Task = crate::persistence::TaskRecord;

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(500);
const CLAIM_CONTENTION_BASE_DELAY: Duration = Duration::from_millis(250);
const CLAIM_CONTENTION_MAX_DELAY: Duration = Duration::from_secs(8);
const CLAIM_TRANSIENT_MAX_DELAY: Duration = Duration::from_secs(2);
const CLAIM_FATAL_DELAY: Duration = Duration::from_secs(5);
const INTERRUPTED_TASK_RECOVERY_INTERVAL: Duration = Duration::from_secs(30);
const UNLEASED_RUNNING_TASK_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerClaimError {
    OwnershipContention,
    TransientFence,
    Fatal,
}

#[derive(Debug, Default)]
struct WorkerClaimBackoff {
    consecutive_contentions: u32,
}

impl WorkerClaimBackoff {
    fn reset(&mut self) {
        self.consecutive_contentions = 0;
    }

    fn next_delay(&mut self, node_id: &str, error: WorkerClaimError) -> Duration {
        match error {
            WorkerClaimError::OwnershipContention => {
                let exponent = self.consecutive_contentions.min(5);
                self.consecutive_contentions = self.consecutive_contentions.saturating_add(1);
                let base_ms =
                    (CLAIM_CONTENTION_BASE_DELAY.as_millis() as u64).saturating_mul(1 << exponent);
                let capped_ms = base_ms.min(CLAIM_CONTENTION_MAX_DELAY.as_millis() as u64);
                Duration::from_millis(capped_ms.saturating_add(stable_jitter_ms(
                    node_id,
                    self.consecutive_contentions,
                    capped_ms / 2,
                )))
            }
            WorkerClaimError::TransientFence => {
                let exponent = self.consecutive_contentions.min(3);
                self.consecutive_contentions = self.consecutive_contentions.saturating_add(1);
                let base_ms = 50_u64.saturating_mul(1 << exponent);
                let capped_ms = base_ms.min(CLAIM_TRANSIENT_MAX_DELAY.as_millis() as u64);
                Duration::from_millis(capped_ms.saturating_add(stable_jitter_ms(
                    node_id,
                    self.consecutive_contentions,
                    capped_ms,
                )))
            }
            WorkerClaimError::Fatal => CLAIM_FATAL_DELAY,
        }
    }
}

fn stable_jitter_ms(node_id: &str, attempt: u32, max_jitter_ms: u64) -> u64 {
    if max_jitter_ms == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    node_id.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % max_jitter_ms.saturating_add(1)
}

fn classify_worker_claim_error(error: &anyhow::Error) -> WorkerClaimError {
    if error_chain_contains(
        error,
        &[
            OWNERSHIP_HELD,
            OWNERSHIP_OWNER_MISMATCH,
            LEASE_HELD,
            "partition owner row exists but is not committed-visible",
        ],
    ) {
        return WorkerClaimError::OwnershipContention;
    }
    if error_chain_contains(
        error,
        &[
            OWNERSHIP_CAS_CONFLICT,
            OWNERSHIP_STALE_FENCE,
            LEASE_CAS_CONFLICT,
            LEASE_OWNER_MISMATCH,
            STALE_FENCE,
            "generation mismatch",
            "stale",
            "CAS conflict",
        ],
    ) {
        return WorkerClaimError::TransientFence;
    }
    WorkerClaimError::Fatal
}

fn error_chain_contains(error: &anyhow::Error, needles: &[&str]) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string();
        needles.iter().any(|needle| message.contains(needle))
    })
}

fn is_task_lease_fencing_error(error: &anyhow::Error) -> bool {
    error_chain_contains(
        error,
        &[
            LEASE_CAS_CONFLICT,
            LEASE_EXPIRED,
            LEASE_HELD,
            LEASE_OWNER_MISMATCH,
            STALE_FENCE,
        ],
    )
}

fn current_time_nanos() -> Result<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

fn duration_nanos(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos()).map_err(|_| anyhow!("duration exceeds i64 nanoseconds"))
}

async fn wait_for_task_or_delay(task_notify: &Arc<tokio::sync::Notify>, delay: Duration) {
    tokio::select! {
        _ = task_notify.notified() => {}
        _ = tokio::time::sleep(delay) => {}
    }
}

#[derive(Deserialize)]
struct DeleteObjectPayload {
    object_id: i64,
}

#[derive(Deserialize)]
struct DeleteBucketPayload {
    bucket_id: i64,
}

#[derive(Deserialize)]
struct ObjectMetadataCompactionPayload {
    bucket_id: i64,
}

#[derive(Deserialize)]
struct IndexBuildPayload {
    tenant_id: i64,
    bucket_id: i64,
    index_id: i64,
    index_version: i64,
    source_cursor: u128,
}

pub async fn run(
    persistence: Persistence,
    core_store: CoreStore,
    jwt_manager: Arc<JwtManager>,
    object_manager: ObjectManager,
    keyring: Arc<EncryptionKeyring>,
    concurrency: usize,
) -> Result<()> {
    core_store.wait_for_coremeta_recovery_ready().await;
    while let Err(error) = recover_interrupted_tasks(&persistence).await {
        warn!(%error, "Failed to recover interrupted background tasks; retrying");
        tokio::time::sleep(CLAIM_FATAL_DELAY).await;
        core_store.wait_for_coremeta_recovery_ready().await;
    }
    let mut next_recovery = tokio::time::Instant::now() + INTERRUPTED_TASK_RECOVERY_INTERVAL;
    let task_notify = persistence.task_notify();
    let mut claim_backoff = WorkerClaimBackoff::default();
    let task_slots = Arc::new(Semaphore::new(concurrency.max(1)));
    loop {
        core_store.wait_for_coremeta_recovery_ready().await;
        if tokio::time::Instant::now() >= next_recovery {
            if let Err(error) = recover_interrupted_tasks(&persistence).await {
                warn!(%error, "Failed to recover expired background tasks");
            }
            next_recovery = tokio::time::Instant::now() + INTERRUPTED_TASK_RECOVERY_INTERVAL;
        }
        if task_slots.available_permits() == 0 {
            tokio::select! {
                permit = task_slots.acquire() => {
                    drop(permit.map_err(|_| anyhow!("background task semaphore closed"))?);
                }
                _ = tokio::time::sleep_until(next_recovery) => {}
            }
            continue;
        }

        match persistence.has_due_task_work().await {
            Ok(true) => {}
            Ok(false) => {
                claim_backoff.reset();
                wait_for_task_or_delay(&task_notify, IDLE_POLL_INTERVAL).await;
                continue;
            }
            Err(error) => {
                warn!("Failed to inspect due tasks before claiming: {error}");
                wait_for_task_or_delay(&task_notify, CLAIM_FATAL_DELAY).await;
                continue;
            }
        }

        let claim_limit = task_slots.available_permits().min(10) as i64;
        let tasks = match persistence.claim_pending_tasks(claim_limit).await {
            Ok(tasks) => {
                claim_backoff.reset();
                tasks
            }
            Err(error) => {
                let claim_error = classify_worker_claim_error(&error);
                let delay = claim_backoff.next_delay(persistence.owner_node_id(), claim_error);
                match claim_error {
                    WorkerClaimError::OwnershipContention => {
                        debug!(
                            node_id = persistence.owner_node_id(),
                            delay_ms = delay.as_millis(),
                            error = %error,
                            "Task worker is not the current task-queue owner; backing off"
                        );
                    }
                    WorkerClaimError::TransientFence => {
                        debug!(
                            node_id = persistence.owner_node_id(),
                            delay_ms = delay.as_millis(),
                            error = %error,
                            "Task worker saw transient task-queue fence contention; backing off"
                        );
                    }
                    WorkerClaimError::Fatal => {
                        error!("Failed to fetch tasks: {}", error);
                    }
                }
                wait_for_task_or_delay(&task_notify, delay).await;
                continue;
            }
        };

        if tasks.is_empty() {
            wait_for_task_or_delay(&task_notify, IDLE_POLL_INTERVAL).await;
            continue;
        }

        for task in tasks {
            let p = persistence.clone();
            let core_store = core_store.clone();
            let jm = jwt_manager.clone();
            let om = object_manager.clone();
            let keyring = keyring.clone();
            let permit = task_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow!("background task semaphore closed"))?;
            tokio::spawn(async move {
                let _permit = permit;
                let result =
                    execute_task_with_lease(&p, &core_store, &jm, &om, &task, &keyring).await;

                match result {
                    Err(failure) => {
                        error!("Task {} failed: {:?}", task.id, failure.error);
                        if is_task_lease_fencing_error(&failure.error) {
                            warn!(
                                task_id = task.id,
                                error = %failure.error,
                                "Stale task execution stopped without mutating queue state"
                            );
                        } else if let Some(lease) = failure.lease {
                            match execution_lease_precondition(&p, &core_store, &lease).await {
                                Ok(precondition) => {
                                    if let Err(fail_err) = p
                                        .fail_task_with_execution_guard(
                                            task.id,
                                            task.attempts,
                                            &failure.error.to_string(),
                                            precondition,
                                        )
                                        .await
                                    {
                                        warn!(
                                            task_id = task.id,
                                            %fail_err,
                                            "Task failure queue mutation was rejected"
                                        );
                                    }
                                }
                                Err(fence_error) => {
                                    warn!(
                                        task_id = task.id,
                                        %fence_error,
                                        "Task failure was not recorded because its lease is stale"
                                    );
                                }
                            }
                        } else {
                            warn!(
                                task_id = task.id,
                                "Task failed before acquiring an execution lease; queue recovery will retry it"
                            );
                        }
                    }
                    Ok(lease) => {
                        match execution_lease_precondition(&p, &core_store, &lease).await {
                            Err(error) => {
                                warn!(
                                    task_id = task.id,
                                    %error,
                                    "Task completion rejected because its execution lease is stale"
                                );
                            }
                            Ok(precondition) => {
                                if let Err(complete_err) = p
                                    .update_task_status_with_execution_guard(
                                        task.id,
                                        task.attempts,
                                        TaskStatus::Completed,
                                        precondition,
                                    )
                                    .await
                                {
                                    error!(
                                        "Failed to mark task {} as completed: {}",
                                        task.id, complete_err
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}

async fn recover_interrupted_tasks(persistence: &Persistence) -> Result<()> {
    recover_interrupted_tasks_at(
        persistence,
        current_time_nanos()?,
        duration_nanos(UNLEASED_RUNNING_TASK_GRACE)?,
    )
    .await
}

async fn recover_interrupted_tasks_at(
    persistence: &Persistence,
    now_nanos: i64,
    unleased_grace_nanos: i64,
) -> Result<()> {
    if now_nanos < 0 || unleased_grace_nanos < 0 {
        return Err(anyhow!("task recovery timestamps must be nonnegative"));
    }
    let mut recovered = 0_usize;
    let mut after_tuple_key = None;

    loop {
        let page = persistence
            .list_tasks_page(after_tuple_key.as_deref(), 256)
            .await?;
        for task in page
            .tasks
            .into_iter()
            .filter(|task| task.status == TaskStatus::Running)
        {
            let lease = match persistence.read_task_execution_lease(task.id).await {
                Ok(lease) => lease,
                Err(error) => {
                    warn!(
                        task_id = task.id,
                        %error,
                        "Failed to inspect an interrupted background task lease"
                    );
                    continue;
                }
            };
            let recovery_reason = match lease.as_ref() {
                Some(lease) if lease.expires_at_nanos <= now_nanos => {
                    "background task execution lease expired before completion"
                }
                Some(_) => continue,
                None => {
                    let updated_at_nanos =
                        task.updated_at.timestamp_nanos_opt().ok_or_else(|| {
                            anyhow!("task {} update time exceeds nanosecond range", task.id)
                        })?;
                    if updated_at_nanos > now_nanos.saturating_sub(unleased_grace_nanos) {
                        continue;
                    }
                    "background task remained unleased after being claimed"
                }
            };
            let lease_precondition = match lease.as_ref() {
                Some(lease) => {
                    task_lease::task_lease_exact_precondition(
                        persistence.storage(),
                        lease,
                        persistence.partition_owner_signing_key(),
                    )
                    .await
                }
                None => task_lease::task_lease_precondition(
                    persistence.storage(),
                    0,
                    &format!("task-{}", task.id),
                ),
            };
            let lease_precondition = match lease_precondition {
                Ok(precondition) => precondition,
                Err(error) if is_task_lease_fencing_error(&error) => continue,
                Err(error) => return Err(error),
            };
            if let Err(error) = persistence
                .fail_task_with_execution_guard(
                    task.id,
                    task.attempts,
                    recovery_reason,
                    lease_precondition,
                )
                .await
            {
                warn!(
                    task_id = task.id,
                    %error,
                    "Failed to recover an interrupted background task"
                );
                continue;
            }
            recovered = recovered.saturating_add(1);
            warn!(
                task_id = task.id,
                task_type = ?task.task_type,
                recovery_reason,
                "Recovered a background task without a valid execution lease"
            );
        }
        let Some(next) = page.next_tuple_key else {
            break;
        };
        after_tuple_key = Some(next);
    }

    if recovered > 0 {
        info!(recovered, "Recovered interrupted background tasks");
    }
    Ok(())
}

#[derive(Debug)]
struct TaskExecutionFailure {
    lease: Option<TaskLease>,
    error: anyhow::Error,
}

async fn execute_task_with_lease(
    persistence: &Persistence,
    core_store: &CoreStore,
    _jwt_manager: &Arc<JwtManager>,
    object_manager: &ObjectManager,
    task: &Task,
    keyring: &Arc<EncryptionKeyring>,
) -> std::result::Result<TaskLease, TaskExecutionFailure> {
    let lease = persistence
        .acquire_task_execution_lease(task)
        .await
        .map_err(|error| TaskExecutionFailure { lease: None, error })?;
    if task.task_type == TaskType::RebalanceShard {
        let mut lease = lease;
        handle_rebalance_shard(persistence, core_store, task, &mut lease)
            .await
            .map_err(|error| TaskExecutionFailure {
                lease: Some(lease.clone()),
                error,
            })?;
        return check_execution_lease(persistence, core_store, &lease)
            .await
            .map_err(|error| TaskExecutionFailure {
                lease: Some(lease),
                error,
            });
    }

    let guard = TaskExecutionGuard::new(
        core_store.storage().clone(),
        persistence.partition_owner_signing_key().to_vec(),
        lease.clone(),
    )
    .map_err(|error| TaskExecutionFailure {
        lease: Some(lease),
        error,
    })?;
    if let Err(error) =
        execute_task_handler_with_lease_renewal(persistence, object_manager, task, keyring, &guard)
            .await
    {
        return Err(TaskExecutionFailure {
            lease: Some(guard.snapshot().await),
            error,
        });
    }
    let source_cursor = guard.snapshot().await.source_cursor;
    if let Err(error) = guard.checkpoint(source_cursor).await {
        return Err(TaskExecutionFailure {
            lease: Some(guard.snapshot().await),
            error,
        });
    }
    match guard.check().await {
        Ok(lease) => Ok(lease),
        Err(error) => Err(TaskExecutionFailure {
            lease: Some(guard.snapshot().await),
            error,
        }),
    }
}

async fn execute_task_handler_with_lease_renewal(
    persistence: &Persistence,
    object_manager: &ObjectManager,
    task: &Task,
    keyring: &EncryptionKeyring,
    guard: &TaskExecutionGuard,
) -> Result<()> {
    let execution = async {
        match task.task_type {
            TaskType::DeleteObject => handle_delete_object(persistence, task, guard).await?,
            TaskType::DeleteBucket => handle_delete_bucket(persistence, task, guard).await?,
            TaskType::ObjectMetadataCompaction => {
                handle_object_metadata_compaction(persistence, task, guard).await?
            }
            TaskType::IndexBuild => handle_index_build(persistence, task, guard).await?,
            TaskType::AuthzMaterialization => {
                handle_authz_materialization(persistence, task, guard).await?
            }
            TaskType::HFIngestion => {
                handle_hf_ingestion(persistence, object_manager, task, keyring, guard).await?
            }
            TaskType::RebalanceShard => {
                return Err(anyhow!(
                    "RebalanceShard must use its lease-aware repair executor"
                ));
            }
        }
        Ok(())
    };
    tokio::pin!(execution);

    loop {
        let renewal_delay = guard.renewal_delay().await?;
        let renewal = async {
            tokio::time::sleep(renewal_delay).await;
            guard.renew().await
        };
        tokio::pin!(renewal);
        tokio::select! {
            biased;
            renewed = &mut renewal => {
                renewed?;
            }
            result = &mut execution => {
                guard.check().await?;
                return result;
            }
        }
    }
}

async fn check_execution_lease(
    persistence: &Persistence,
    core_store: &CoreStore,
    lease: &TaskLease,
) -> Result<TaskLease> {
    task_lease::check_task_lease(
        core_store.storage(),
        lease,
        current_time_nanos()?,
        persistence.partition_owner_signing_key(),
    )
    .await
}

async fn execution_lease_precondition(
    persistence: &Persistence,
    core_store: &CoreStore,
    lease: &TaskLease,
) -> Result<crate::core_store::CoreMutationPrecondition> {
    task_lease::task_lease_fenced_precondition(
        core_store.storage(),
        lease,
        current_time_nanos()?,
        persistence.partition_owner_signing_key(),
    )
    .await
}

async fn renew_execution_lease(
    persistence: &Persistence,
    core_store: &CoreStore,
    lease: &TaskLease,
    ttl_nanos: i64,
) -> Result<TaskLease> {
    task_lease::renew_task_lease(
        core_store.storage(),
        lease,
        current_time_nanos()?,
        ttl_nanos,
        persistence.partition_owner_signing_key(),
    )
    .await
}

fn task_lease_ttl_nanos(lease: &TaskLease) -> Result<i64> {
    lease
        .expires_at_nanos
        .checked_sub(lease.acquired_at_nanos)
        .filter(|ttl| *ttl > 0)
        .ok_or_else(|| anyhow!("task lease has no positive renewal window"))
}

fn task_lease_renewal_delay(lease: &TaskLease, now_nanos: i64) -> Result<Duration> {
    let remaining = lease.expires_at_nanos.saturating_sub(now_nanos);
    if remaining <= 0 {
        return Err(anyhow!("{LEASE_EXPIRED}: task lease expired"));
    }
    let delay_nanos = (remaining / 3).max(1);
    Ok(Duration::from_nanos(
        u64::try_from(delay_nanos).map_err(|_| anyhow!("task lease delay exceeds u64"))?,
    ))
}

#[derive(Debug, Deserialize)]
struct AuthzMaterializationPayload {
    tenant_id: i64,
    target_revision: u64,
}

async fn handle_rebalance_shard(
    persistence: &Persistence,
    core_store: &CoreStore,
    task: &Task,
    lease: &mut TaskLease,
) -> anyhow::Result<()> {
    let payload: RebalanceShardTaskPayload = serde_json::from_value(task.payload.clone())
        .map_err(|error| anyhow!("invalid RebalanceShard task {} payload: {error}", task.id))?;
    payload.validate()?;
    let attempt_started_at_nanos = lease.acquired_at_nanos;
    let ttl_nanos = task_lease_ttl_nanos(&lease)?;

    *lease = check_execution_lease(persistence, core_store, lease).await?;
    let open_finding_precondition =
        execution_lease_precondition(persistence, core_store, lease).await?;
    let open_finding = persistence
        .write_rebalance_shard_finding(
            &payload,
            task.id,
            lease.fence_token,
            lease.lease_epoch,
            attempt_started_at_nanos,
            crate::repair_finding::RepairFindingStatus::Open,
            open_finding_precondition,
        )
        .await?;
    *lease = check_execution_lease(persistence, core_store, lease).await?;
    let repair_finding_id = open_finding.finding_id;

    let preparation = core_store.prepare_shard_repair_for_task(&payload, &repair_finding_id);
    tokio::pin!(preparation);
    let prepared = loop {
        let renewal_delay = task_lease_renewal_delay(&lease, current_time_nanos()?)?;
        tokio::select! {
            biased;
            _ = tokio::time::sleep(renewal_delay) => {
                *lease = renew_execution_lease(
                    persistence,
                    core_store,
                    lease,
                    ttl_nanos,
                ).await?;
            }
            result = &mut preparation => {
                *lease = check_execution_lease(persistence, core_store, lease).await?;
                break result?;
            }
        }
    };
    *lease = renew_execution_lease(persistence, core_store, lease, ttl_nanos).await?;
    let lease_precondition = task_lease::task_lease_fenced_precondition(
        core_store.storage(),
        lease,
        current_time_nanos()?,
        persistence.partition_owner_signing_key(),
    )
    .await?;
    let outcome = core_store
        .publish_prepared_shard_repair(prepared, lease_precondition)
        .await?;
    if outcome.requires_retry() {
        let unresolved = outcome
            .unresolved_placements()
            .iter()
            .map(|placement| {
                format!(
                    "{}:{}:{:?}",
                    placement.shard_index, placement.expected_node_id, placement.reason
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        return Err(anyhow!(
            "CoreStore shard repair remains incomplete and must retry: {unresolved}"
        ));
    }
    let overlays_published = outcome.overlays_published();
    let completed_status = rebalance_shard_completion_status(overlays_published);
    *lease = check_execution_lease(persistence, core_store, lease).await?;
    let completed_finding_precondition =
        execution_lease_precondition(persistence, core_store, lease).await?;
    let completed_finding = persistence
        .write_rebalance_shard_finding(
            &payload,
            task.id,
            lease.fence_token,
            lease.lease_epoch,
            attempt_started_at_nanos,
            completed_status,
            completed_finding_precondition,
        )
        .await;
    *lease = renew_execution_lease(persistence, core_store, lease, ttl_nanos).await?;
    completed_finding?;

    info!(
        task_id = task.id,
        object_hash = %payload.object_hash,
        block_id = %payload.block_id,
        lease_fence_token = lease.fence_token,
        overlays_published,
        "RebalanceShard task completed"
    );
    Ok(())
}

fn rebalance_shard_completion_status(
    overlays_published: bool,
) -> crate::repair_finding::RepairFindingStatus {
    if overlays_published {
        crate::repair_finding::RepairFindingStatus::RepairedObjectShards
    } else {
        crate::repair_finding::RepairFindingStatus::VerifiedHealthy
    }
}

async fn handle_authz_materialization(
    persistence: &Persistence,
    task: &Task,
    guard: &TaskExecutionGuard,
) -> anyhow::Result<()> {
    let payload: AuthzMaterializationPayload = serde_json::from_value(task.payload.clone())?;
    let outcome = persistence
        .run_authz_materialization_task(payload.tenant_id, payload.target_revision, guard)
        .await?;
    info!(
        tenant_id = payload.tenant_id,
        requested_revision = payload.target_revision,
        processed_revision = outcome.processed_revision,
        source_record_count = outcome.source_record_count,
        generation = outcome.generation,
        segment_ref = %outcome.segment_ref,
        "Authorization materialization task completed"
    );
    Ok(())
}

async fn handle_index_build(
    persistence: &Persistence,
    task: &Task,
    guard: &TaskExecutionGuard,
) -> anyhow::Result<()> {
    let payload: IndexBuildPayload = serde_json::from_value(task.payload.clone())?;
    match persistence
        .build_index_task(
            payload.tenant_id,
            payload.bucket_id,
            payload.index_id,
            payload.index_version,
            payload.source_cursor,
            guard,
        )
        .await?
    {
        Some(outcome) => {
            info!(
                index_id = payload.index_id,
                index_storage_id = %outcome.index_storage_id,
                index_kind = %outcome.index_kind,
                generation = outcome.generation,
                item_count = outcome.item_count,
                source_cursor = outcome.source_cursor,
                segment_hashes = ?outcome.segment_hashes,
                diagnostic_count = outcome.diagnostics.len(),
                "Index build task completed"
            );
        }
        None => {
            info!(
                index_id = payload.index_id,
                index_version = payload.index_version,
                "Index build task skipped because the index is absent, disabled, stale, or unsupported"
            );
        }
    }
    Ok(())
}

async fn handle_object_metadata_compaction(
    persistence: &Persistence,
    task: &Task,
    guard: &TaskExecutionGuard,
) -> anyhow::Result<()> {
    let payload: ObjectMetadataCompactionPayload = serde_json::from_value(task.payload.clone())?;
    let Some(sealed) = persistence
        .compact_object_metadata_for_task(payload.bucket_id, guard)
        .await?
    else {
        info!(
            bucket_id = payload.bucket_id,
            "Object metadata compaction skipped; bucket or journal did not exist"
        );
        return Ok(());
    };
    info!(
        bucket_id = payload.bucket_id,
        generation = sealed.generation,
        metadata_records = sealed.metadata_record_count,
        directory_records = sealed.directory_record_count,
        manifest_hash = %sealed.manifest_hash,
        "Object metadata compaction sealed partition"
    );
    Ok(())
}

async fn handle_hf_ingestion(
    persistence: &Persistence,
    object_manager: &ObjectManager,
    task: &Task,
    keyring: &EncryptionKeyring,
    _guard: &TaskExecutionGuard,
) -> anyhow::Result<()> {
    use globset::{Glob, GlobSetBuilder};
    use hf_hub::{Repo, RepoType, api::sync::ApiBuilder};

    let ingestion_id: i64 = task
        .payload
        .get("ingestion_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("missing ingestion_id"))?;

    // Wrap the main logic in a closure to ensure we can catch errors and update the final status.
    let result = async {
        info!(ingestion_id, "Starting ingestion task.");

        persistence
            .hf_update_ingestion_state(ingestion_id, HFIngestionState::Running, None)
            .await?;

        let job = persistence
            .hf_get_ingestion_job(ingestion_id)
            .await?
            .ok_or_else(|| anyhow!("ingestion job not found"))?;
        let key_id = job.key_id;
        let tenant_id = job.tenant_id;
        let requester_app_id = job.requester_app_id;
        let repo_str = job.repo;
        let revision = job.revision;
        let target_bucket = job.target_bucket;
        let _target_region = job.target_region;
        let target_prefix = job.target_prefix;
        let include_globs = job.include_globs;
        let exclude_globs = job.exclude_globs;
        let requester_claims = crate::auth::Claims {
            sub: requester_app_id.to_string(),
            exp: usize::MAX,
            tenant_id,
            jti: None,
        };
        info!(
            repo = %repo_str,
            revision = %revision,
            "Fetched job details."
        );

        let token_encrypted = persistence
            .hf_get_key_encrypted_by_id(tenant_id, key_id)
            .await?
            .ok_or_else(|| anyhow!("hugging face key not found"))?;
        let token_bytes = keyring.decrypt(&token_encrypted)?;
        let token = String::from_utf8(token_bytes)?;
        debug!("Decrypted token.");

        // Local ingestion cache only; model files are durable after ObjectManager uploads to CoreStore.
        let cache_dir = tempfile::tempdir()?;
        let api = ApiBuilder::new()
            .with_cache_dir(cache_dir.path().to_path_buf())
            .with_token(Some(token))
            .build()?;

        // --- Blocking File Listing ---
        info!("Getting repo file list (blocking)...");
        let repo_details = (repo_str.clone(), revision.clone());
        let api_clone = api.clone();
        let siblings = tokio::task::spawn_blocking(move || {
            let repo = Repo::with_revision(repo_details.0, RepoType::Model, repo_details.1);
            let repo_client = api_clone.repo(repo);
            repo_client.info().map(|info| info.siblings)
        })
        .await??;
        info!(num_files = siblings.len(), "Got files from repo.");
        // --- End Blocking ---

        let mut inc_builder = GlobSetBuilder::new();
        if include_globs.is_empty() {
            inc_builder.add(Glob::new("**/*")?);
        } else {
            for g in include_globs {
                inc_builder.add(Glob::new(&g)?);
            }
        }
        let include = inc_builder.build()?;
        let mut exc_builder = GlobSetBuilder::new();
        for g in exclude_globs {
            exc_builder.add(Glob::new(&g)?);
        }
        let exclude = exc_builder.build()?;

        'outer: for e in siblings {
            let path = e.rfilename.clone();
            debug!(path = %path, "Processing file");
            let path_buf = std::path::PathBuf::from(path.clone());
            if !include.is_match(path_buf.as_path()) {
                continue;
            }
            if exclude.is_match(path_buf.as_path()) {
                continue;
            }
            let size = None; // hf-hub RepoSibling does not include size; will be known after download
            let item_id = persistence
                .hf_add_item(ingestion_id, &path, size, None)
                .await?;
            persistence
                .hf_update_item_state(item_id, HFIngestionItemState::Downloading, None)
                .await?;
            debug!(item_id, "Item state set to downloading.");

            if let Ok(bucket_opt) = persistence
                .get_bucket_by_name(tenant_id, &target_bucket)
                .await
            {
                if let Some(bucket) = bucket_opt {
                    if let Ok(obj_opt) = persistence.get_object(bucket.id, &path).await {
                        if obj_opt.is_some() {
                            info!(path = %path, "Skipping existing file");
                            persistence
                                .hf_update_item_state(item_id, HFIngestionItemState::Skipped, None)
                                .await?;
                            continue 'outer;
                        }
                    }
                }
            }

            // --- Blocking File Download ---
            info!(
                file = %e.rfilename,
                "Downloading file (blocking)..."
            );
            let repo_details_clone = (repo_str.clone(), revision.clone());
            let api_clone_2 = api.clone();
            let filename = e.rfilename.clone();
            let local_path_buf;
            info!("Downloading from Hugging Face");
            local_path_buf = tokio::task::spawn_blocking(move || {
                let repo = Repo::with_revision(
                    repo_details_clone.0,
                    RepoType::Model,
                    repo_details_clone.1,
                );
                let repo_client = api_clone_2.repo(repo);
                repo_client.get(&filename)
            })
            .await??;

            let local_path = &local_path_buf;
            debug!(path = ?local_path, "Downloaded to");
            // --- End Blocking ---

            let _bucket = persistence
                .get_bucket_by_name(tenant_id, &target_bucket)
                .await?
                .ok_or_else(|| anyhow!("target bucket not found"))?;
            let full_key = if target_prefix.is_empty() {
                path.clone()
            } else {
                format!("{}/{}", target_prefix.trim_end_matches('/'), path)
            };

            info!(
                bucket = %target_bucket,
                key = %full_key,
                "Uploading to Anvil"
            );
            let make_reader = || async {
                let f = tokio::fs::File::open(&local_path).await;
                f.map(|file| {
                    use futures_util::StreamExt as _;
                    use tokio_util::io::ReaderStream;
                    ReaderStream::new(file).map(|r: Result<bytes::Bytes, std::io::Error>| {
                        r.map(|b| b.to_vec())
                            .map_err(|e| tonic::Status::internal(e.to_string()))
                    })
                })
            };

            let mut reader = make_reader().await?;
            let mut attempt = 0;
            loop {
                attempt += 1;
                info!("Putting object, attempt {}", attempt);
                let res = object_manager
                    .put_object(
                        &requester_claims,
                        &target_bucket,
                        &full_key,
                        reader,
                        crate::object_manager::ObjectWriteOptions::default(),
                    )
                    .await;
                match res {
                    Ok(obj) => {
                        info!(key = %full_key, "Upload successful");
                        persistence
                            .hf_update_item_success(item_id, obj.size, &obj.etag)
                            .await?;
                        break;
                    }
                    Err(e) if attempt < 3 => {
                        warn!(
                            attempt,
                            key = %full_key,
                            error = %e.to_string(),
                            "Upload attempt failed. Retrying..."
                        );
                        let jitter = (rand::random::<u64>() % 200) as u64;
                        tokio::time::sleep(std::time::Duration::from_millis(
                            500 * attempt as u64 + jitter,
                        ))
                        .await;
                        reader = make_reader().await?;
                        continue;
                    }
                    Err(e) => {
                        error!(
                            key = %full_key,
                            error = %e,
                            "Upload failed permanently"
                        );
                        return Err(anyhow::anyhow!(e.to_string()));
                    }
                }
            }
        }

        info!(ingestion_id, "Ingestion task completed successfully.");

        // --- Generate and upload anvil-index.json ---
        let index_key = if target_prefix.is_empty() {
            "anvil-index.json".to_string()
        } else {
            format!("{}/anvil-index.json", target_prefix.trim_end_matches('/'))
        };

        // The index output is necessarily proportional to the target contents,
        // but paging and streaming keep memory independent of target cardinality.
        let index_path = hf_index::write_target_index(
            persistence,
            tenant_id,
            &target_bucket,
            &target_prefix,
            &repo_str,
            &revision,
            cache_dir.path(),
        )
        .await?;
        info!(index_key = %index_key, "Uploading anvil-index.json");

        // Upload index file, using retry logic adapted from above for robustness
        let mut attempt = 0;
        loop {
            attempt += 1;
            info!("Putting anvil-index.json, attempt {}", attempt);
            let index_file = tokio::fs::File::open(&index_path).await?;
            let index_stream: Pin<
                Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>,
            > = Box::pin(tokio_util::io::ReaderStream::new(index_file).map(|result| {
                result
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| Status::internal(error.to_string()))
            }));

            let res: Result<Object, Status> = object_manager
                .put_object(
                    &requester_claims,
                    &target_bucket,
                    &index_key,
                    index_stream,
                    crate::object_manager::ObjectWriteOptions {
                        content_type: Some("application/json".to_string()),
                        user_metadata: None,
                        transaction_id: None,
                        transaction_principal: None,
                        storage_class_id: None,
                        ..Default::default()
                    },
                )
                .await;
            match res {
                Ok(_) => {
                    info!(key = %index_key, "anvil-index.json upload successful");
                    break;
                }
                Err(e) if attempt < 3 => {
                    warn!(
                        attempt,
                        key = %index_key,
                        error = %e.to_string(),
                        "anvil-index.json upload attempt failed. Retrying..."
                    );
                    let jitter = (rand::random::<u64>() % 200) as u64;
                    tokio::time::sleep(std::time::Duration::from_millis(
                        500 * attempt as u64 + jitter,
                    ))
                    .await;
                    continue;
                }
                Err(e) => {
                    error!(
                        key = %index_key,
                        error = %e,
                        "anvil-index.json upload failed permanently"
                    );
                    return Err(anyhow::anyhow!(e.to_string()));
                }
            }
        }
        // --- End anvil-index.json upload ---

        info!(ingestion_id, "Updating ingestion state to completed.");
        persistence
            .hf_update_ingestion_state(ingestion_id, HFIngestionState::Completed, None)
            .await?;
        info!(ingestion_id, "Ingestion state set to completed.");

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = &result {
        error!(ingestion_id, error = %e, "HF Ingestion task failed");
    }
    result
}

async fn handle_delete_object(
    persistence: &Persistence,
    task: &Task,
    _guard: &TaskExecutionGuard,
) -> Result<()> {
    let payload: DeleteObjectPayload = serde_json::from_value(task.payload.clone())?;

    // Finally, hard delete the object metadata.
    persistence.hard_delete_object(payload.object_id).await?;

    info!(
        "Successfully processed DeleteObject task for object {}",
        payload.object_id
    );
    Ok(())
}

async fn handle_delete_bucket(
    persistence: &Persistence,
    task: &Task,
    _guard: &TaskExecutionGuard,
) -> Result<()> {
    let payload: DeleteBucketPayload = serde_json::from_value(task.payload.clone())?;
    let deleted = persistence
        .hard_delete_bucket_if_empty(payload.bucket_id)
        .await?;

    if deleted {
        info!(
            "Successfully processed DeleteBucket task for bucket {}",
            payload.bucket_id
        );
    } else {
        info!(
            "DeleteBucket task for bucket {} was already applied",
            payload.bucket_id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, storage::Storage};
    use chrono::Utc;
    use tempfile::tempdir;

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "worker-test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    #[test]
    fn worker_claim_error_classification_treats_queue_ownership_as_contention() {
        let error = anyhow!("{OWNERSHIP_HELD}: partition task_queue is owned by active node");
        assert_eq!(
            classify_worker_claim_error(&error),
            WorkerClaimError::OwnershipContention
        );
        let error = anyhow!("partition owner row exists but is not committed-visible");
        assert_eq!(
            classify_worker_claim_error(&error),
            WorkerClaimError::OwnershipContention
        );

        let error = anyhow!("{LEASE_HELD}: task lease is owned by another active principal");
        assert_eq!(
            classify_worker_claim_error(&error),
            WorkerClaimError::OwnershipContention
        );
    }

    #[test]
    fn worker_claim_backoff_is_bounded_and_jittered() {
        let mut first = WorkerClaimBackoff::default();
        let first_delay = first.next_delay("node-a", WorkerClaimError::OwnershipContention);
        assert!(first_delay >= CLAIM_CONTENTION_BASE_DELAY);
        assert!(first_delay <= CLAIM_CONTENTION_BASE_DELAY + CLAIM_CONTENTION_BASE_DELAY / 2);

        let mut backoff = WorkerClaimBackoff::default();
        let mut max_seen = Duration::ZERO;
        for _ in 0..16 {
            max_seen = backoff
                .next_delay("node-a", WorkerClaimError::OwnershipContention)
                .max(max_seen);
        }
        assert!(max_seen <= CLAIM_CONTENTION_MAX_DELAY + CLAIM_CONTENTION_MAX_DELAY / 2);
    }

    #[test]
    fn rebalance_completion_finding_reflects_overlay_publication() {
        assert_eq!(
            rebalance_shard_completion_status(true),
            crate::repair_finding::RepairFindingStatus::RepairedObjectShards
        );
        assert_eq!(
            rebalance_shard_completion_status(false),
            crate::repair_finding::RepairFindingStatus::VerifiedHealthy
        );
    }

    #[tokio::test]
    async fn unleased_running_task_is_recovered_only_after_the_grace_period() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let persistence = Persistence::new(&config).unwrap();

        persistence
            .enqueue_task(TaskType::DeleteObject, json!({ "object_id": 1 }), 0)
            .await
            .unwrap();
        let claimed = persistence.claim_pending_tasks(1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, TaskStatus::Running);
        let claimed_at = claimed[0].updated_at.timestamp_nanos_opt().unwrap();
        let grace = duration_nanos(UNLEASED_RUNNING_TASK_GRACE).unwrap();

        recover_interrupted_tasks_at(&persistence, claimed_at + grace - 1, grace)
            .await
            .unwrap();
        let active = persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks;
        assert_eq!(active[0].status, TaskStatus::Running);

        recover_interrupted_tasks_at(&persistence, claimed_at + grace, grace)
            .await
            .unwrap();

        let tasks = persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].status, TaskStatus::Failed);
        assert_eq!(tasks[0].attempts, 1);
        assert_eq!(
            tasks[0].last_error.as_deref(),
            Some("background task remained unleased after being claimed")
        );
    }

    #[tokio::test]
    async fn running_task_is_recovered_only_after_its_lease_expires() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let persistence = Persistence::new(&config).unwrap();

        persistence
            .enqueue_task(TaskType::DeleteObject, json!({ "object_id": 1 }), 0)
            .await
            .unwrap();
        let claimed = persistence.claim_pending_tasks(1).await.unwrap();
        let lease = persistence
            .acquire_task_execution_lease(&claimed[0])
            .await
            .unwrap();
        let grace = duration_nanos(UNLEASED_RUNNING_TASK_GRACE).unwrap();

        recover_interrupted_tasks_at(&persistence, lease.expires_at_nanos - 1, grace)
            .await
            .unwrap();
        let active = persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks;
        assert_eq!(active[0].status, TaskStatus::Running);

        recover_interrupted_tasks_at(&persistence, lease.expires_at_nanos, grace)
            .await
            .unwrap();

        let tasks = persistence
            .list_tasks_page(None, 1_000)
            .await
            .unwrap()
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].status, TaskStatus::Failed);
        assert_eq!(
            tasks[0].last_error.as_deref(),
            Some("background task execution lease expired before completion")
        );
    }

    #[tokio::test]
    async fn object_metadata_compaction_task_seals_manifest() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let persistence = Persistence::new(&config).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "task-compact-bucket", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                1,
                bucket.id,
                "docs/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let now = Utc::now();
        let task = Task {
            id: 1,
            task_type: TaskType::ObjectMetadataCompaction,
            payload: json!({ "bucket_id": bucket.id }),
            priority: 0,
            status: TaskStatus::Running,
            attempts: 1,
            last_error: None,
            scheduled_at: now,
            created_at: now,
            updated_at: now,
        };
        let storage = Storage::new_at_sync(&config.storage_path).unwrap();
        let core_store = crate::core_store::CoreStore::new(storage.clone())
            .await
            .unwrap();
        let jwt_manager = Arc::new(JwtManager::new(config.jwt_secret.clone()));
        let object_manager = ObjectManager::new(
            persistence.clone(),
            storage.clone(),
            core_store.clone(),
            config.region.clone(),
            config.cross_region_routing_policy,
            hex::decode(&config.anvil_secret_encryption_key).unwrap(),
            crate::observability::Observability::default(),
        );
        let keyring = Arc::new(config.secret_keyring().unwrap());
        execute_task_with_lease(
            &persistence,
            &core_store,
            &jwt_manager,
            &object_manager,
            &task,
            &keyring,
        )
        .await
        .unwrap();

        assert!(
            crate::metadata_journal::read_latest_partition_manifest(
                &storage,
                &bucket,
                &hex::decode(&config.anvil_secret_encryption_key).unwrap()
            )
            .await
            .unwrap()
            .is_some()
        );
        let replayed = persistence
            .get_object(bucket.id, "docs/a.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed.version_id, object.version_id);
        let lease = persistence
            .read_task_execution_lease(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.partition_family, "object_metadata");
        assert_eq!(lease.checkpoint_cursor, lease.source_cursor);
    }
}
