use super::*;
use crate::task_execution_guard::TaskExecutionGuard;
use anyhow::Context;

const AUTHZ_MATERIALIZATION_DERIVED_INDEX_KIND: &str = "userset";
const AUTHZ_MATERIALIZATION_MAX_STEPS_PER_TASK: usize = 256;
const REBALANCE_SHARD_PARTITION_FAMILY: &str = "object_shard_repair";

fn task_queue_is_owned_elsewhere(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains(OWNERSHIP_HELD)
        || message.contains("partition owner row exists but is not committed-visible")
}

impl Persistence {
    pub async fn hard_delete_object(&self, _object_id: i64) -> Result<()> {
        // Object metadata is append-only in the native journal. Physical shard cleanup
        // must not erase the metadata history needed for watches, indexes, and audit.
        Ok(())
    }

    pub async fn enqueue_task(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<()> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::enqueue_task_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.notify_task_enqueued();
        Ok(())
    }

    pub async fn enqueue_task_if_absent(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<bool> {
        let permit = self.task_queue_write_permit().await?;
        let enqueued = task_journal::enqueue_task_if_absent_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        if enqueued {
            self.notify_task_enqueued();
        }
        Ok(enqueued)
    }

    pub async fn enqueue_rebalance_shard_task(
        &self,
        payload: &crate::tasks::RebalanceShardTaskPayload,
        priority: i32,
    ) -> Result<bool> {
        payload.validate()?;
        self.enqueue_task_if_absent(
            crate::tasks::TaskType::RebalanceShard,
            serde_json::to_value(payload).context("serialize RebalanceShard task payload")?,
            priority,
        )
        .await
    }

    pub(crate) async fn owns_rebalance_shard_scheduler(&self) -> Result<bool> {
        match self.task_queue_write_permit().await {
            Ok(permit) => Ok(permit.owner_node_id == self.owner_node_id),
            Err(error)
                if is_retryable_partition_fence_error(&error)
                    || task_queue_is_owned_elsewhere(&error) =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn write_rebalance_shard_finding(
        &self,
        payload: &crate::tasks::RebalanceShardTaskPayload,
        task_id: i64,
        lease_fence_token: u64,
        lease_epoch: u64,
        attempt_started_at_nanos: i64,
        status: repair_finding::RepairFindingStatus,
        lease_precondition: crate::core_store::CoreMutationPrecondition,
    ) -> Result<repair_finding::RepairFinding> {
        payload.validate()?;
        if lease_fence_token == 0 || lease_epoch == 0 {
            return Err(anyhow!(
                "shard repair finding lease fence and epoch must be nonzero"
            ));
        }

        let scope_id = payload.object_digest()?.to_string();
        let repair_task_id = task_lease_id(task_id)?;
        let (stage, severity, code, message, overlays_published) = match status {
            repair_finding::RepairFindingStatus::Open => (
                "open",
                repair_finding::RepairFindingSeverity::Warning,
                "ObjectShardRepairStarted",
                "CoreStore object block shard repair task started",
                None,
            ),
            repair_finding::RepairFindingStatus::RepairedObjectShards => (
                "completed",
                repair_finding::RepairFindingSeverity::Info,
                "ObjectShardRepairCompleted",
                "CoreStore object block shard repair task published at least one overlay",
                Some(true),
            ),
            repair_finding::RepairFindingStatus::VerifiedHealthy => (
                "completed",
                repair_finding::RepairFindingSeverity::Info,
                "ObjectShardsVerifiedHealthy",
                "CoreStore object block placements were already healthy after re-probe",
                Some(false),
            ),
            _ => {
                return Err(anyhow!(
                    "shard repair finding status must be Open, RepairedObjectShards, or VerifiedHealthy"
                ));
            }
        };

        repair_finding::write_repair_finding_with_lease(
            &self.storage,
            repair_finding::RepairFindingWrite {
                finding_id: rebalance_shard_audit_finding_id(
                    task_id,
                    lease_fence_token,
                    lease_epoch,
                    stage,
                ),
                scope_kind: "object_shard".to_string(),
                scope_id: scope_id.clone(),
                repair_task_id,
                lease_fence_token,
                severity,
                status,
                code: code.to_string(),
                message: message.to_string(),
                subjects: vec![
                    repair_finding::RepairSubjectRef {
                        subject_kind: "core_object".to_string(),
                        subject_id: payload.object_hash.clone(),
                        generation: None,
                        cursor: None,
                        expected_hash: Some(scope_id),
                        actual_hash: None,
                    },
                    repair_finding::RepairSubjectRef {
                        subject_kind: "core_block".to_string(),
                        subject_id: payload.block_id.clone(),
                        generation: None,
                        cursor: None,
                        expected_hash: None,
                        actual_hash: None,
                    },
                    repair_finding::RepairSubjectRef {
                        subject_kind: "core_manifest".to_string(),
                        subject_id: payload.manifest_ref.clone(),
                        generation: Some(payload.manifest_root_generation),
                        cursor: None,
                        expected_hash: Some(payload.manifest_payload_digest_hex()?.to_string()),
                        actual_hash: None,
                    },
                ],
                proposed_action: repair_finding::RepairActionKind::RepairObjectShards,
                evidence: serde_json::json!({
                    "object_hash": payload.object_hash,
                    "logical_size": payload.logical_size,
                    "manifest_ref": payload.manifest_ref,
                    "manifest_root_key_hash": payload.manifest_root_key_hash,
                    "manifest_root_generation": payload.manifest_root_generation,
                    "manifest_transaction_id": payload.manifest_transaction_id,
                    "manifest_payload_digest": payload.manifest_payload_digest,
                    "block_id": payload.block_id,
                    "task_id": task_id,
                    "lease_fence_token": lease_fence_token,
                    "lease_epoch": lease_epoch,
                    "stage": stage,
                    "overlays_published": overlays_published,
                }),
                created_at_nanos: attempt_started_at_nanos,
            },
            &self.partition_owner_signing_key,
            lease_precondition,
        )
        .await
    }

    pub(super) async fn enqueue_index_build_task(
        &self,
        payload: JsonValue,
        priority: i32,
    ) -> Result<bool> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::enqueue_index_build_task_with_permit(
                &self.storage,
                payload.clone(),
                priority,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(result) => {
                    if result {
                        self.notify_task_enqueued();
                    }
                    return Ok(result);
                }
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("index build task enqueue retry exhausted")))
    }

    pub(crate) async fn enqueue_authz_materialization(
        &self,
        tenant_id: i64,
        target_revision: u64,
    ) -> Result<bool> {
        let payload = serde_json::json!({
            "tenant_id": tenant_id,
            "target_revision": target_revision,
        });
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::enqueue_authz_materialization_task_with_permit(
                &self.storage,
                payload.clone(),
                30,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(result) => {
                    if result {
                        self.notify_task_enqueued();
                    }
                    return Ok(result);
                }
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("authz materialization enqueue retry exhausted")))
    }

    pub(crate) async fn run_authz_materialization_task(
        &self,
        tenant_id: i64,
        requested_revision: u64,
        guard: &TaskExecutionGuard,
    ) -> Result<authz_journal::AuthzMaterializationOutcome> {
        let latest_revision =
            authz_journal::latest_authz_tuple_revision(&self.storage, tenant_id).await?;
        let latest_revision = u64::try_from(latest_revision.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        let target_revision = requested_revision.max(latest_revision);
        let source_permit = self.authz_write_permit(tenant_id).await?;
        let source_partition_precondition = crate::partition_fence::partition_write_precondition(
            &self.storage,
            &source_permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_fence_token =
            authz_journal::latest_authz_journal_fence_token(&self.storage, tenant_id).await?;

        let mut steps = 0usize;
        let mut source_rows_visited = 0usize;
        let mut step_target =
            if crate::authz_segment::latest_authz_tuple_segment_record(&self.storage, tenant_id)
                .await?
                .is_none()
            {
                1
            } else {
                target_revision
            };
        let outcome = loop {
            let mut outcome =
                authz_journal::AuthzMaterializationOutcome::materialize_for_task_at_revision(
                    &self.storage,
                    tenant_id,
                    step_target,
                    source_fence_token,
                    guard,
                    &source_partition_precondition,
                )
                .await?;
            steps = steps.saturating_add(1);
            source_rows_visited = source_rows_visited.saturating_add(outcome.source_rows_visited);
            outcome.source_rows_visited = source_rows_visited;
            if outcome.processed_revision >= target_revision
                || steps >= AUTHZ_MATERIALIZATION_MAX_STEPS_PER_TASK
            {
                break outcome;
            }
            step_target = target_revision;
        };

        let latest_after =
            authz_journal::latest_authz_tuple_revision(&self.storage, tenant_id).await?;
        let latest_after = u64::try_from(latest_after.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        append_authz_materialization_lag_watch(
            &self.storage,
            tenant_id,
            latest_after,
            &outcome,
            guard,
            &source_partition_precondition,
        )
        .await?;
        if latest_after > outcome.processed_revision {
            self.enqueue_authz_materialization(tenant_id, latest_after)
                .await?;
        }

        Ok(outcome)
    }

    pub async fn acquire_task_execution_lease(
        &self,
        task: &TaskRecord,
    ) -> Result<task_lease::TaskLease> {
        let target = self.task_lease_target(task).await?;
        let now_nanos = current_time_nanos()?;
        let ttl_nanos = self.task_lease_ttl_nanos()?;
        task_lease::acquire_task_lease(
            &self.storage,
            task_lease::TaskLeaseAcquire {
                task_id: task_lease_id(task.id)?,
                task_kind: task.task_type.as_str().to_string(),
                partition_family: target.partition_family,
                partition_id: target.partition_id,
                owner: task_lease::TaskLeaseOwner::node_instance(
                    self.owner_node_id.clone(),
                    self.task_actor_instance_id.clone(),
                ),
                source_cursor: target.source_cursor,
                now_nanos,
                ttl_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn checkpoint_task_execution_lease(
        &self,
        lease: &task_lease::TaskLease,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            lease,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn acquire_named_task_lease(
        &self,
        request: task_lease::TaskLeaseAcquire,
    ) -> Result<task_lease::TaskLease> {
        task_lease::acquire_task_lease(&self.storage, request, &self.partition_owner_signing_key)
            .await
    }

    pub async fn checkpoint_named_task_lease(
        &self,
        expected: &task_lease::TaskLease,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            expected,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn commit_named_task_lease(
        &self,
        expected: &task_lease::TaskLease,
        committed_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::commit_task_lease(
            &self.storage,
            expected,
            committed_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub(crate) async fn named_task_lease_fenced_precondition(
        &self,
        lease: &task_lease::TaskLease,
        now_nanos: i64,
    ) -> Result<crate::core_store::CoreMutationPrecondition> {
        task_lease::task_lease_fenced_precondition(
            &self.storage,
            lease,
            now_nanos,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_expected_named_task_lease(
        &self,
        authenticated_owner: &task_lease::TaskLeaseOwner,
        task_id: &str,
        expected_fence_token: u64,
        expected_root_generation: u64,
        expected_lease_epoch: u64,
        expected_expires_at_nanos: i64,
        expected_lease_hash: &str,
    ) -> Result<task_lease::TaskLease> {
        let lease = self
            .read_named_task_lease(authenticated_owner.tenant_id, task_id)
            .await?
            .ok_or_else(|| anyhow!("{}: task lease does not exist", task_lease::STALE_FENCE))?;
        if !lease.owner.same_security_owner(authenticated_owner) {
            return Err(anyhow!(
                "{}: task lease owner mismatch",
                task_lease::LEASE_OWNER_MISMATCH
            ));
        }
        lease.require_expected_version(
            expected_fence_token,
            expected_root_generation,
            expected_lease_epoch,
            expected_expires_at_nanos,
            expected_lease_hash,
        )?;
        if lease.expires_at_nanos <= current_time_nanos()? {
            return Err(anyhow!("{}: task lease expired", task_lease::LEASE_EXPIRED));
        }
        Ok(lease)
    }

    pub async fn force_release_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::force_release_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_task_execution_lease(
        &self,
        task_id: i64,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            0,
            &task_lease_id(task_id)?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub(super) async fn task_lease_target(&self, task: &TaskRecord) -> Result<TaskLeaseTarget> {
        match task.task_type {
            crate::tasks::TaskType::ObjectMetadataCompaction => {
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
                    .await?
                    .ok_or_else(|| anyhow!("object metadata compaction bucket not found"))?;
                let stats = metadata_journal::active_object_journal_stats(
                    &self.storage,
                    &bucket,
                    &self.partition_owner_signing_key,
                )
                .await?;
                Ok(TaskLeaseTarget {
                    partition_family: "object_metadata".to_string(),
                    partition_id: hex::encode(metadata_journal::object_metadata_partition_id(
                        bucket.tenant_id,
                        bucket.id,
                    )),
                    source_cursor: u128::from(stats.last_sequence),
                })
            }
            crate::tasks::TaskType::IndexBuild => {
                let tenant_id = task_payload_i64(task, "tenant_id")?;
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let index_id = task_payload_i64(task, "index_id")?;
                let source_cursor = task_payload_u128(task, "source_cursor")?;
                Ok(TaskLeaseTarget {
                    partition_family: "index".to_string(),
                    partition_id: hex::encode(crate::formats::hash32(
                        format!("tenant/{tenant_id}/bucket/{bucket_id}/index/{index_id}")
                            .as_bytes(),
                    )),
                    source_cursor,
                })
            }
            crate::tasks::TaskType::AuthzMaterialization => {
                let tenant_id = task_payload_i64(task, "tenant_id")?;
                let source_cursor = task_payload_u128(task, "target_revision")?;
                Ok(TaskLeaseTarget {
                    partition_family: "authz_materialization".to_string(),
                    partition_id: hex::encode(crate::formats::hash32(
                        format!("tenant/{tenant_id}/authz").as_bytes(),
                    )),
                    source_cursor,
                })
            }
            crate::tasks::TaskType::RebalanceShard => {
                let payload = serde_json::from_value(task.payload.clone())
                    .with_context(|| format!("decode RebalanceShard task {} payload", task.id))?;
                rebalance_shard_lease_target(&payload)
            }
            _ => Ok(TaskLeaseTarget {
                partition_family: "task_queue".to_string(),
                partition_id: hex::encode(task_journal::task_queue_partition_id()),
                source_cursor: task.id.max(0) as u128,
            }),
        }
    }

    pub(super) fn task_lease_ttl_nanos(&self) -> Result<i64> {
        if self.task_lease_ttl_secs == 0 {
            return Err(anyhow!("task lease ttl must be nonzero"));
        }
        let ttl = self
            .task_lease_ttl_secs
            .checked_mul(1_000_000_000)
            .ok_or_else(|| anyhow!("task lease ttl overflow"))?;
        i64::try_from(ttl).map_err(|_| anyhow!("task lease ttl cannot fit i64 nanoseconds"))
    }

    pub async fn claim_pending_tasks(&self, limit: i64) -> Result<Vec<TaskRecord>> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::claim_pending_tasks_with_permit(
                &self.storage,
                limit,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(tasks) => return Ok(tasks),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task claim retry exhausted")))
    }

    pub async fn has_due_task_work(&self) -> Result<bool> {
        task_journal::has_due_tasks(&self.storage).await
    }

    pub async fn list_tasks_page(
        &self,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<TaskPage> {
        task_journal::list_tasks_page(&self.storage, after_tuple_key, page_size).await
    }

    pub async fn update_task_status(
        &self,
        task_id: i64,
        status: crate::tasks::TaskStatus,
    ) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::update_task_status_with_permit(
                &self.storage,
                task_id,
                status,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task status update retry exhausted")))
    }

    pub async fn update_task_status_with_execution_guard(
        &self,
        task_id: i64,
        expected_attempts: i32,
        status: crate::tasks::TaskStatus,
        lease_precondition: crate::core_store::CoreMutationPrecondition,
    ) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::update_task_status_with_execution_guard(
                &self.storage,
                task_id,
                expected_attempts,
                status,
                &permit,
                &self.partition_owner_signing_key,
                lease_precondition.clone(),
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("guarded task status update retry exhausted")))
    }

    pub async fn fail_task(&self, task_id: i64, error: &str) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::fail_task_with_permit(
                &self.storage,
                task_id,
                error,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task failure update retry exhausted")))
    }

    pub async fn fail_task_with_execution_guard(
        &self,
        task_id: i64,
        expected_attempts: i32,
        error: &str,
        lease_precondition: crate::core_store::CoreMutationPrecondition,
    ) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(failure) if is_retryable_partition_fence_error(&failure) => {
                    last_error = Some(failure);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(failure) => return Err(failure),
            };
            match task_journal::fail_task_with_execution_guard(
                &self.storage,
                task_id,
                expected_attempts,
                error,
                &permit,
                &self.partition_owner_signing_key,
                lease_precondition.clone(),
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(failure) if is_retryable_partition_fence_error(&failure) => {
                    last_error = Some(failure);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(failure) => return Err(failure),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("guarded task failure update retry exhausted")))
    }

    pub async fn hf_create_key(
        &self,
        tenant_id: i64,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::create_key_with_permit(
            &self.storage,
            tenant_id,
            name,
            token_encrypted,
            note,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_delete_key(&self, tenant_id: i64, name: &str) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::delete_key_with_permit(
            &self.storage,
            tenant_id,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_key_encrypted(
        &self,
        tenant_id: i64,
        name: &str,
    ) -> Result<Option<(i64, Vec<u8>)>> {
        hf_journal::get_key_encrypted(&self.storage, tenant_id, name).await
    }

    pub async fn hf_get_key_encrypted_by_id(
        &self,
        tenant_id: i64,
        id: i64,
    ) -> Result<Option<Vec<u8>>> {
        hf_journal::get_key_encrypted_by_id(&self.storage, tenant_id, id).await
    }

    pub(crate) async fn hf_list_encrypted_key_page(
        &self,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfKeyPage> {
        hf_journal::list_encrypted_key_page(&self.storage, after_cursor, limit).await
    }

    pub async fn hf_update_key_encrypted(&self, id: i64, token_encrypted: &[u8]) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_key_encrypted_with_permit(
            &self.storage,
            id,
            token_encrypted,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub(crate) async fn hf_list_key_page(
        &self,
        tenant_id: i64,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfKeyPage> {
        hf_journal::list_key_page(&self.storage, tenant_id, after_cursor, limit).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn hf_create_ingestion(
        &self,
        key_id: i64,
        tenant_id: i64,
        requester_app_id: i64,
        repo: &str,
        revision: Option<&str>,
        target_bucket: &str,
        target_region: &str,
        target_prefix: Option<&str>,
        include_globs: &[String],
        exclude_globs: &[String],
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::create_ingestion_with_permit(
            &self.storage,
            key_id,
            tenant_id,
            requester_app_id,
            repo,
            revision,
            target_bucket,
            target_region,
            target_prefix,
            include_globs,
            exclude_globs,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_ingestion_job(&self, id: i64) -> Result<Option<HfIngestionJob>> {
        hf_journal::get_ingestion_job(&self.storage, id).await
    }

    pub async fn hf_update_ingestion_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_ingestion_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_cancel_ingestion(&self, id: i64) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::cancel_ingestion_with_permit(
            &self.storage,
            id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_add_item(
        &self,
        ingestion_id: i64,
        path: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::add_item_with_permit(
            &self.storage,
            ingestion_id,
            path,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionItemState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_success(&self, id: i64, size: i64, etag: &str) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_success_with_permit(
            &self.storage,
            id,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_list_stored_ingestion_item_page(
        &self,
        ingestion_id: i64,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfStoredItemPage> {
        hf_journal::list_stored_ingestion_item_page(
            &self.storage,
            ingestion_id,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn hf_list_stored_target_item_page(
        &self,
        tenant_id: i64,
        bucket: &str,
        prefix: &str,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfStoredItemPage> {
        hf_journal::list_stored_target_item_page(
            &self.storage,
            tenant_id,
            bucket,
            prefix,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn hf_get_ingestion_status(&self, id: i64) -> Result<hf_journal::HfIngestionStatus> {
        hf_journal::get_ingestion_status(&self.storage, id).await
    }
}

async fn append_authz_materialization_lag_watch(
    storage: &Storage,
    tenant_id: i64,
    latest_revision: u64,
    outcome: &authz_journal::AuthzMaterializationOutcome,
    guard: &TaskExecutionGuard,
    source_partition_precondition: &crate::core_store::CoreMutationPrecondition,
) -> Result<()> {
    let derived_index_id = crate::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID.to_string();
    if let Some(latest_event) =
        crate::authz_derived_lag_watch::latest_authz_derived_lag_watch_event(
            storage,
            tenant_id,
            &derived_index_id,
        )
        .await?
        && (latest_event.payload.processed_revision > outcome.processed_revision
            || (latest_event.payload.processed_revision == outcome.processed_revision
                && latest_event.payload.latest_revision >= latest_revision))
    {
        return Ok(());
    }
    let mutation_id = authz_materialization_mutation_id(
        tenant_id,
        outcome.processed_revision,
        latest_revision,
        &outcome.source_records_hash,
    );
    let payload =
        authz_materialization_lag_watch_payload(derived_index_id, latest_revision, outcome);
    let source_partition_precondition = source_partition_precondition.clone();
    guard
        .publication_permit()
        .await?
        .publish_with(move |task_lease_precondition| async move {
            let preconditions = [source_partition_precondition, task_lease_precondition];
            crate::authz_derived_lag_watch::append_authz_derived_lag_watch_record(
                storage,
                tenant_id,
                mutation_id,
                payload,
                &preconditions,
            )
            .await
            .map(|_| ())
        })
        .await
}

fn authz_materialization_lag_watch_payload(
    derived_index_id: String,
    latest_revision: u64,
    outcome: &authz_journal::AuthzMaterializationOutcome,
) -> crate::authz_derived_lag_watch::AuthzDerivedLagWatchPayload {
    crate::authz_derived_lag_watch::AuthzDerivedLagWatchPayload {
        derived_index_id,
        derived_index_kind: AUTHZ_MATERIALIZATION_DERIVED_INDEX_KIND.to_string(),
        processed_revision: outcome.processed_revision,
        latest_revision,
        source_cursor: u128::from(outcome.source_cursor),
        source_manifest_hash: outcome.source_records_hash.clone(),
        generation: outcome.generation,
        emitted_at: outcome.materialized_at.clone(),
    }
}

fn authz_materialization_mutation_id(
    tenant_id: i64,
    processed_revision: u64,
    latest_revision: u64,
    source_records_hash: &str,
) -> [u8; 16] {
    let hash = crate::formats::hash32(
        format!(
            "authz-materialization:{tenant_id}:{processed_revision}:{latest_revision}:{source_records_hash}"
        )
        .as_bytes(),
    );
    let mut mutation_id = [0; 16];
    mutation_id.copy_from_slice(&hash[..16]);
    mutation_id
}

fn rebalance_shard_lease_target(
    payload: &crate::tasks::RebalanceShardTaskPayload,
) -> Result<TaskLeaseTarget> {
    payload.validate()?;
    Ok(TaskLeaseTarget {
        partition_family: REBALANCE_SHARD_PARTITION_FAMILY.to_string(),
        partition_id: hex::encode(crate::formats::hash32(&payload.immutable_identity_bytes())),
        source_cursor: 0,
    })
}

fn rebalance_shard_audit_finding_id(
    task_id: i64,
    lease_fence_token: u64,
    lease_epoch: u64,
    stage: &str,
) -> String {
    format!("object-shards-{task_id}-{lease_fence_token}-{lease_epoch}-{stage}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repair_scheduler_defers_to_the_current_task_queue_owner() {
        assert!(task_queue_is_owned_elsewhere(&anyhow!(
            "{OWNERSHIP_HELD}: task queue is owned by another equal peer"
        )));
        assert!(task_queue_is_owned_elsewhere(&anyhow!(
            "partition owner row exists but is not committed-visible"
        )));
        assert!(!task_queue_is_owned_elsewhere(&anyhow!(
            "partition owner signature mismatch"
        )));
    }

    #[test]
    fn rebalance_shard_lease_target_is_stable_and_block_scoped() {
        let digest = "12".repeat(32);
        let payload = crate::tasks::RebalanceShardTaskPayload {
            object_hash: format!("sha256:{digest}"),
            logical_size: 8_192,
            manifest_ref: format!("core-manifest-sha256:{digest}:profile:ec-4-2"),
            block_id: "block-a".to_string(),
            manifest_root_key_hash: format!("sha256:{}", "34".repeat(32)),
            manifest_root_generation: 7,
            manifest_transaction_id: "manifest-mutation-a".to_string(),
            manifest_payload_digest: format!("blake3:{}", "56".repeat(32)),
        };

        let target = rebalance_shard_lease_target(&payload).unwrap();
        assert_eq!(target.partition_family, REBALANCE_SHARD_PARTITION_FAMILY);
        assert_eq!(target.partition_id.len(), 64);
        assert_eq!(target.source_cursor, 0);
        assert_eq!(target, rebalance_shard_lease_target(&payload).unwrap());

        for changed in [
            crate::tasks::RebalanceShardTaskPayload {
                object_hash: format!("sha256:{}", "34".repeat(32)),
                ..payload.clone()
            },
            crate::tasks::RebalanceShardTaskPayload {
                logical_size: payload.logical_size + 1,
                ..payload.clone()
            },
            crate::tasks::RebalanceShardTaskPayload {
                manifest_ref: format!("{}-next", payload.manifest_ref),
                ..payload.clone()
            },
            crate::tasks::RebalanceShardTaskPayload {
                block_id: "block-b".to_string(),
                ..payload.clone()
            },
            crate::tasks::RebalanceShardTaskPayload {
                manifest_root_generation: payload.manifest_root_generation + 1,
                ..payload.clone()
            },
        ] {
            assert_ne!(
                target.partition_id,
                rebalance_shard_lease_target(&changed).unwrap().partition_id
            );
        }
    }

    #[test]
    fn rebalance_shard_audit_finding_identity_changes_with_lease_epoch() {
        let first = rebalance_shard_audit_finding_id(41, 7, 11, "open");
        let retried = rebalance_shard_audit_finding_id(41, 7, 12, "open");

        assert_ne!(first, retried);
        assert_eq!(first, "object-shards-41-7-11-open");
        assert_eq!(retried, "object-shards-41-7-12-open");
    }

    #[test]
    fn authz_lag_watch_payload_and_identity_are_derived_from_immutable_inputs() {
        let outcome = authz_journal::AuthzMaterializationOutcome {
            processed_revision: 7,
            source_cursor: 41,
            source_record_count: 3,
            source_records_hash: hex::encode([9; 32]),
            generation: 7,
            segment_ref: "authz_tuple_segment:tenant:11:generation:7".to_string(),
            materialized_at: "2026-07-21T00:00:00.000000000Z".to_string(),
            source_rows_visited: 1,
        };
        let first = authz_materialization_lag_watch_payload(
            "derived-userset-primary".to_string(),
            9,
            &outcome,
        );
        let second = authz_materialization_lag_watch_payload(
            "derived-userset-primary".to_string(),
            9,
            &outcome,
        );
        assert_eq!(first, second);
        assert_eq!(first.source_cursor, 41);
        assert_eq!(first.emitted_at, outcome.materialized_at);
        assert_eq!(
            authz_materialization_mutation_id(
                11,
                outcome.processed_revision,
                9,
                &outcome.source_records_hash,
            ),
            authz_materialization_mutation_id(
                11,
                outcome.processed_revision,
                9,
                &outcome.source_records_hash,
            )
        );
        assert_ne!(
            authz_materialization_mutation_id(
                11,
                outcome.processed_revision,
                9,
                &outcome.source_records_hash,
            ),
            authz_materialization_mutation_id(
                11,
                outcome.processed_revision,
                10,
                &outcome.source_records_hash,
            )
        );
    }
}
