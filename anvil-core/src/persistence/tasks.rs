use super::*;
use crate::task_execution_guard::TaskExecutionGuard;
use anyhow::Context;

const AUTHZ_MATERIALIZATION_DERIVED_INDEX_KIND: &str = "userset";
const AUTHZ_MATERIALIZATION_MAX_STEPS_PER_TASK: usize = 256;
const REBALANCE_SHARD_PARTITION_FAMILY: &str = "object_shard_repair";

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
            self.mvcc()?,
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
            self.mvcc()?,
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

    pub async fn enqueue_repair_run(
        &self,
        payload: &crate::tasks::RepairRunTaskPayload,
        priority: i32,
        audit_event: &crate::admin_audit::AdminAuditEvent,
    ) -> Result<TaskRecord> {
        payload.validate()?;
        let permit = self.task_queue_write_permit().await?;
        let task = task_journal::enqueue_repair_run_with_permit(
            self.mvcc()?,
            payload,
            priority,
            &permit,
            audit_event,
        )
        .await?;
        self.notify_task_enqueued();
        Ok(task)
    }

    pub fn repair_run_status(&self, repair_task_id: &str) -> Result<Option<TaskRecord>> {
        let task_id = repair_task_id
            .strip_prefix("repair-run-")
            .ok_or_else(|| anyhow!("repair task id must use repair-run-<id>"))?
            .parse::<i64>()
            .context("repair task id has an invalid numeric id")?;
        let task = task_journal::get_task(self.mvcc()?, task_id)?;
        match task {
            Some(task) if task.task_type == crate::tasks::TaskType::RepairRun => Ok(Some(task)),
            Some(_) => Err(anyhow!("repair task id names a different task type")),
            None => Ok(None),
        }
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
                self.mvcc()?,
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
                self.mvcc()?,
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
        let latest_revision = authz_journal::latest_authz_tuple_revision(self.mvcc()?, tenant_id)?;
        let latest_revision = u64::try_from(latest_revision.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        let target_revision = requested_revision.max(latest_revision);
        let source_permit = self.authz_write_permit(tenant_id).await?;
        let source_head_predicate =
            crate::authz_head::latest_mvcc_predicate(self.mvcc()?, tenant_id)?;
        let source_fence_token =
            authz_journal::latest_authz_journal_fence_token(self.mvcc()?, tenant_id)?;

        let mut steps = 0usize;
        let mut source_rows_visited = 0usize;
        let mut step_target =
            if crate::authz_segment::latest_authz_tuple_segment_record(self.mvcc()?, tenant_id)
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
                    self.mvcc()?,
                    tenant_id,
                    step_target,
                    source_fence_token,
                    guard,
                    &source_head_predicate,
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

        let latest_after = authz_journal::latest_authz_tuple_revision(self.mvcc()?, tenant_id)?;
        let latest_after = u64::try_from(latest_after.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        append_authz_materialization_lag_watch(
            self.mvcc()?,
            tenant_id,
            latest_after,
            &outcome,
            guard,
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
        task_lease::acquire_task_lease_mvcc(
            self.mvcc()?,
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
        task_lease::checkpoint_task_lease_mvcc(
            self.mvcc()?,
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
        task_lease::acquire_task_lease_mvcc(
            self.mvcc()?,
            request,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn checkpoint_named_task_lease(
        &self,
        expected: &task_lease::TaskLease,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease_mvcc(
            self.mvcc()?,
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
        task_lease::commit_task_lease_mvcc(
            self.mvcc()?,
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
        task_lease::read_task_lease_mvcc(
            self.mvcc()?,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
    }

    pub(crate) async fn named_task_lease_fenced_precondition(
        &self,
        lease: &task_lease::TaskLease,
        now_nanos: i64,
    ) -> Result<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )> {
        task_lease::check_task_lease_mvcc(
            self.mvcc()?,
            lease,
            now_nanos,
            &self.partition_owner_signing_key,
        )?;
        task_lease::task_lease_mvcc_predicate(lease)
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
        task_lease::force_release_task_lease_mvcc(
            self.mvcc()?,
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
        task_lease::read_task_lease_mvcc(
            self.mvcc()?,
            0,
            &task_lease_id(task_id)?,
            &self.partition_owner_signing_key,
        )
    }

    pub(super) async fn task_lease_target(&self, task: &TaskRecord) -> Result<TaskLeaseTarget> {
        match task.task_type {
            crate::tasks::TaskType::ObjectMetadataCompaction => {
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let bucket =
                    bucket_journal::read_current_bucket_by_id_mvcc(self.mvcc()?, bucket_id)?
                        .ok_or_else(|| anyhow!("object metadata compaction bucket not found"))?;
                let stats = metadata_journal::active_object_journal_stats(
                    &self.storage,
                    self.mvcc()?,
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
                self.mvcc()?,
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
        task_journal::has_due_tasks(self.mvcc()?).await
    }

    pub async fn list_tasks_page(
        &self,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<TaskPage> {
        task_journal::list_tasks_page(self.mvcc()?, after_tuple_key, page_size).await
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
                self.mvcc()?,
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
        lease_predicate: (
            crate::mvcc_transaction::LogicalKey,
            crate::mvcc_transaction::PredicateKind,
        ),
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
                self.mvcc()?,
                task_id,
                expected_attempts,
                status,
                &permit,
                &self.partition_owner_signing_key,
                lease_predicate.clone(),
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
                self.mvcc()?,
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
        lease_predicate: (
            crate::mvcc_transaction::LogicalKey,
            crate::mvcc_transaction::PredicateKind,
        ),
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
                self.mvcc()?,
                task_id,
                expected_attempts,
                error,
                &permit,
                &self.partition_owner_signing_key,
                lease_predicate.clone(),
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
            self.mvcc()?,
            tenant_id,
            name,
            token_encrypted,
            note,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn hf_stage_create_key(
        &self,
        tenant_id: i64,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::stage_create_key_with_permit(
            &self.storage,
            self.mvcc()?,
            tenant_id,
            name,
            token_encrypted,
            note,
            transaction_id,
            principal,
            now_unix_ms,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_delete_key(&self, tenant_id: i64, name: &str) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::delete_key_with_permit(
            &self.storage,
            self.mvcc()?,
            tenant_id,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_stage_delete_key(
        &self,
        tenant_id: i64,
        name: &str,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::stage_delete_key_with_permit(
            &self.storage,
            self.mvcc()?,
            tenant_id,
            name,
            transaction_id,
            principal,
            now_unix_ms,
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
        hf_journal::get_key_encrypted(self.mvcc()?, tenant_id, name).await
    }

    pub async fn hf_get_key_encrypted_by_id(
        &self,
        tenant_id: i64,
        id: i64,
    ) -> Result<Option<Vec<u8>>> {
        hf_journal::get_key_encrypted_by_id(self.mvcc()?, tenant_id, id).await
    }

    pub(crate) async fn hf_list_encrypted_key_page(
        &self,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfKeyPage> {
        hf_journal::list_encrypted_key_page(self.mvcc()?, after_cursor, limit).await
    }

    pub async fn hf_update_key_encrypted(&self, id: i64, token_encrypted: &[u8]) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_key_encrypted_with_permit(
            &self.storage,
            self.mvcc()?,
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
        hf_journal::list_key_page(self.mvcc()?, tenant_id, after_cursor, limit).await
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
            self.mvcc()?,
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

    #[allow(clippy::too_many_arguments)]
    pub async fn hf_stage_create_ingestion(
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
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::stage_create_ingestion_with_permit(
            &self.storage,
            self.mvcc()?,
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
            transaction_id,
            principal,
            now_unix_ms,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_ingestion_job(&self, id: i64) -> Result<Option<HfIngestionJob>> {
        hf_journal::get_ingestion_job(self.mvcc()?, id).await
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
            self.mvcc()?,
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
            self.mvcc()?,
            id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_stage_cancel_ingestion(
        &self,
        id: i64,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::stage_cancel_ingestion_with_permit(
            &self.storage,
            self.mvcc()?,
            id,
            transaction_id,
            principal,
            now_unix_ms,
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
            self.mvcc()?,
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
            self.mvcc()?,
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
            self.mvcc()?,
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
        hf_journal::list_stored_ingestion_item_page(self.mvcc()?, ingestion_id, after_cursor, limit)
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
            self.mvcc()?,
            tenant_id,
            bucket,
            prefix,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn hf_get_ingestion_status(&self, id: i64) -> Result<hf_journal::HfIngestionStatus> {
        hf_journal::get_ingestion_status(self.mvcc()?, id).await
    }
}

async fn append_authz_materialization_lag_watch(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    latest_revision: u64,
    outcome: &authz_journal::AuthzMaterializationOutcome,
    guard: &TaskExecutionGuard,
) -> Result<()> {
    let derived_index_id = crate::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID.to_string();
    if let Some(latest_event) =
        crate::authz_derived_lag_watch::latest_authz_derived_lag_watch_event(
            mvcc,
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
    guard
        .publish_mvcc_with(move |_task_lease_predicate| async move {
            crate::authz_derived_lag_watch::append_authz_derived_lag_watch_record(
                mvcc,
                tenant_id,
                mutation_id,
                payload,
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

#[cfg(test)]
mod tests {
    use super::*;

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
