use super::*;

impl AppState {
    pub async fn run_personaldb_postcommit_loop(self) {
        loop {
            if let Err(error) = self.run_personaldb_postcommit_once().await {
                tracing::warn!(%error, "PersonalDB postcommit attempt failed");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub(super) async fn run_personaldb_postcommit_once(&self) -> anyhow::Result<bool> {
        let worker_id = format!("personaldb-postcommit/{}", self.persistence.owner_node_id());
        let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        let Some((job_id, record)) = self
            .mvcc
            .runtime
            .local_store()
            .claim_personaldb_postcommit_authorized(&worker_id, now, 30_000, |record| {
                self.mvcc
                    .claim_assignment(
                        "personaldb-postcommit",
                        &record.job.target_logical_identity(),
                    )
                    .ok()
                    .flatten()
                    .map(|guard| guard.lease_owner(&worker_id))
            })?
        else {
            return Ok(false);
        };
        let guard = self
            .mvcc
            .claim_assignment(
                "personaldb-postcommit",
                &record.job.target_logical_identity(),
            )?
            .ok_or_else(|| anyhow::anyhow!("PersonalDB postcommit assignment changed"))?;
        let lease_owner = guard.lease_owner(&worker_id);
        let result = self
            .execute_personaldb_postcommit(&record.job, record.commit_version)
            .await;
        match result {
            Ok(()) => {
                self.mvcc.validate_assignment(&guard)?;
                self.mvcc
                    .runtime
                    .local_store()
                    .complete_personaldb_postcommit(&job_id, &lease_owner)?;
                Ok(true)
            }
            Err(error) => {
                let delay =
                    250_u64.saturating_mul(1_u64 << record.attempts.saturating_sub(1).min(10));
                self.mvcc
                    .runtime
                    .local_store()
                    .retry_personaldb_postcommit(
                        &job_id,
                        &lease_owner,
                        now.saturating_add(delay),
                        &error.to_string(),
                    )?;
                Err(error)
            }
        }
    }

    pub(super) async fn execute_personaldb_postcommit(
        &self,
        job: &crate::personaldb_postcommit_job::PersonalDbPostCommitJob,
        commit_version: u64,
    ) -> anyhow::Result<()> {
        crate::mvcc_fault_injection::hit(
            crate::mvcc_fault_injection::FaultPoint::PersonalDbPostCommitBeforeEffects,
        )?;
        let head = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            job.tenant_id,
            &job.database_id,
            self.personaldb_protocol_keyring.trust_store(),
            commit_version,
        )?
        .ok_or_else(|| anyhow::anyhow!("PersonalDB postcommit head is missing"))?;
        if head.log_index != job.log_index
            || head.log_hash != job.log_hash
            || head.head_hash.as_deref() != Some(job.committed_head_hash.as_str())
        {
            anyhow::bail!(
                "certified PersonalDB postcommit head does not match immutable job identity"
            );
        }
        maybe_build_personaldb_snapshot(
            &self.storage,
            &self.mvcc,
            PersonalDbSnapshotBuildRequest {
                tenant_id: job.tenant_id,
                database_id: &job.database_id,
                schema_sql: &job.schema_sql,
                created_by_node: &job.principal,
                policy: configured_personaldb_snapshot_policy(&self.config),
            },
            self.personaldb_protocol_keyring.as_ref(),
        )
        .await?;
        self.build_personaldb_projections_for_source_commit(
            job.tenant_id,
            &job.database_id,
            &job.changeset_bytes,
            job.log_index,
            &job.log_hash,
            job.authz_revision,
            &job.excluded_projection_ids,
        )
        .await
        .map_err(|status| anyhow::anyhow!(status.to_string()))?;
        crate::mvcc_fault_injection::hit(
            crate::mvcc_fault_injection::FaultPoint::PersonalDbPostCommitAfterEffects,
        )?;
        Ok(())
    }

    pub(super) fn personaldb_node_id(&self) -> String {
        if !self.config.node_id.is_empty() {
            return self.config.node_id.clone();
        }
        if !self.config.public_api_addr.is_empty() {
            return self.config.public_api_addr.clone();
        }
        if !self.config.api_listen_addr.is_empty() {
            return self.config.api_listen_addr.clone();
        }
        if !self.config.region.is_empty() {
            return self.config.region.clone();
        }
        "local-anvil-node".to_string()
    }

    pub(super) async fn personaldb_commit_guard(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> OwnedMutexGuard<()> {
        let key = format!("{tenant_id}:{database_id}");
        let lock = {
            let mut locks = self.personaldb_commit_locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
                lock
            } else {
                let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(key, std::sync::Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    pub(super) async fn personaldb_write_assignment(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<crate::mvcc_worker_authority::AssignmentGuard, Status> {
        let logical_identity = format!("tenant/{tenant_id}/personaldb/{database_id}");
        let guard = self
            .mvcc
            .reconcile_work_assignment("personaldb-write", &logical_identity)
            .await
            .map_err(internal_status)?
            .ok_or_else(|| {
                Status::failed_precondition("PersonalDB write is assigned to another cluster node")
            })?;
        Ok(guard)
    }

    pub(super) async fn handle_personaldb_projection_writeback(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
        definitions: Vec<ProjectionDefinition>,
        caller_transaction: Option<(&str, &str)>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        validate_claim_tenant(actor.tenant_id, request.tenant_id)?;
        validate_database_id(&request.database_id)?;
        if let Some(bearer_token) = actor.bearer_token.as_deref() {
            bind_personaldb_submit_session(&request, &actor, bearer_token)?;
        }
        if !personaldb_actor_access_allowed(
            &self.storage,
            &self.mvcc,
            &actor,
            &request.database_id,
            AnvilAction::PersonalDbCommit,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let validated = validate_submit_personaldb_changeset(request, default_max_changeset_size())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        iterate_changeset(&validated.request.changeset_bytes)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if definitions.len() != 1 {
            return Err(projection_writeback_rejected(
                "projection write-back has ambiguous projection bindings",
            ));
        }
        let definition = definitions.into_iter().next().ok_or_else(|| {
            projection_writeback_rejected("projection write-back binding missing")
        })?;
        match definition.writeback_policy {
            WriteBackPolicy::Deny => Err(projection_writeback_rejected(
                "projection write-back is denied by projection policy",
            )),
            WriteBackPolicy::AllowMappedColumns { .. } => {
                self.commit_personaldb_projection_writeback(
                    validated.request,
                    actor,
                    definition,
                    caller_transaction,
                )
                .await
            }
        }
    }

    pub(super) async fn commit_personaldb_projection_writeback(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
        definition: ProjectionDefinition,
        caller_transaction: Option<(&str, &str)>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        if caller_transaction.is_none() {
            let now = u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("PersonalDB timestamp predates Unix epoch"))?;
            let handle = self
                .mvcc
                .open_transactions
                .begin(
                    self.mvcc.runtime.as_ref(),
                    self.mvcc.cluster_id().to_string(),
                    actor.principal.clone(),
                    &format!(
                        "personaldb-projection-writeback:{}:{}",
                        request.database_id, request.idempotency_key
                    ),
                    std::time::Duration::from_secs(300),
                    crate::mvcc_transaction::DurabilityLevel::Quorum,
                    crate::mvcc_transaction::ReadConsistency::Linearized,
                    now,
                )
                .await
                .map_err(internal_status)?;
            let status = self
                .mvcc
                .open_transactions
                .status(&handle.transaction_id, &actor.principal, now)
                .map_err(internal_status)?;
            if matches!(status.state, "committed" | "committing") {
                let outcome = self
                    .mvcc
                    .open_transactions
                    .commit(
                        self.mvcc.runtime.as_ref(),
                        &handle.transaction_id,
                        &actor.principal,
                        now,
                    )
                    .await
                    .map_err(internal_status)?;
                let commit_version = match outcome.certification {
                    crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
                        commit_version
                    }
                    crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                        return Err(Status::aborted(format!(
                            "PersonalDB projection writeback aborted: {reason:?}"
                        )));
                    }
                };
                let result = self
                    .mvcc
                    .open_transactions
                    .resolved_idempotency_result(
                        &handle.transaction_id,
                        &actor.principal,
                        PERSONALDB_PROJECTION_WRITEBACK_RESULT_NAMESPACE,
                        &projection_writeback_result_key(&request),
                    )
                    .map_err(internal_status)?
                    .ok_or_else(|| {
                        Status::failed_precondition(
                            "committed projection writeback is missing its response record",
                        )
                    })?;
                let mut response =
                    SubmitPersonalDbChangesetResponse::decode(result.payload.as_slice())
                        .map_err(internal_status)?;
                response.write_state = WriteState::Committed as i32;
                response.watch_cursor_low = commit_version;
                response.watch_cursor_high = 0;
                return Ok(Response::new(response));
            }
            if status.state == "aborted" {
                return Err(Status::aborted(
                    "PersonalDB projection writeback previously aborted",
                ));
            }
            let mut response = Box::pin(self.commit_personaldb_projection_writeback(
                request,
                actor.clone(),
                definition,
                Some((&handle.transaction_id, &actor.principal)),
            ))
            .await?;
            let outcome = self
                .mvcc
                .open_transactions
                .commit(
                    self.mvcc.runtime.as_ref(),
                    &handle.transaction_id,
                    &actor.principal,
                    now,
                )
                .await
                .map_err(internal_status)?;
            let commit_version = match outcome.certification {
                crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
                    commit_version
                }
                crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                    return Err(Status::aborted(format!(
                        "PersonalDB projection writeback aborted: {reason:?}"
                    )));
                }
            };
            response.get_mut().write_state = WriteState::Committed as i32;
            response.get_mut().watch_cursor_low = commit_version;
            response.get_mut().watch_cursor_high = 0;
            return Ok(response);
        }
        let snapshot_version = match caller_transaction {
            Some((transaction_id, principal)) => {
                self.mvcc
                    .open_transactions
                    .binding(transaction_id, principal)
                    .map_err(internal_status)?;
                let handle = self
                    .mvcc
                    .open_transactions
                    .handle(transaction_id)
                    .map_err(internal_status)?;
                handle.snapshot_version
            }
            None => self
                .mvcc
                .runtime
                .applied_version()
                .map_err(internal_status)?,
        };
        let target_request = request.clone();
        let projection_manifest = read_personaldb_group_manifest(
            &self.storage,
            &self.mvcc,
            actor.tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        let projection_head = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            actor.tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection head missing"))?;
        if projection_head.log_index != request.base_log_index
            || projection_head.log_hash != request.base_log_hash
        {
            return Err(projection_writeback_rejected(
                "projection write-back base does not match projection head",
            ));
        }
        let target_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            &self.mvcc,
            actor.tenant_id,
            &definition.database_id,
            &projection_manifest.schema_hash,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection schema SQL missing"))?;
        let source_database_id = single_projection_writeback_source(&definition)?;
        let source_manifest = read_personaldb_group_manifest(
            &self.storage,
            &self.mvcc,
            actor.tenant_id,
            &source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB source group not found"))?;
        let source_head = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            actor.tenant_id,
            &source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source head missing"))?;
        let source_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            &self.mvcc,
            actor.tenant_id,
            &source_database_id,
            &source_manifest.schema_hash,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source schema SQL missing"))?;
        let writeback = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: &source_schema_sql,
            target_schema_sql: &target_schema_sql,
            definition: &definition,
            projection_changeset_bytes: &request.changeset_bytes,
        })
        .map_err(|err| projection_writeback_rejected_owned(err.to_string()))?;
        if writeback.source_database_id != source_database_id {
            return Err(projection_writeback_rejected(
                "projection write-back source binding changed during translation",
            ));
        }
        let payload_hash = hash32(&writeback.changeset_bytes);
        let source_request = CoreSubmitChangeset {
            tenant_id: actor.tenant_id,
            database_id: source_database_id.clone(),
            principal: request.principal,
            session_token: request.session_token,
            request_id: format!(
                "projection-writeback:{}:{}",
                definition.projection_id, request.request_id
            ),
            idempotency_key: format!(
                "projection-writeback:{}:{}",
                definition.projection_id, request.idempotency_key
            ),
            base_log_index: source_head.log_index,
            base_log_hash: source_head.log_hash.clone(),
            client_log_epoch: source_head.log_index.saturating_add(1),
            membership_epoch: source_manifest.active_membership_epoch,
            policy_epoch: source_manifest.active_policy_epoch,
            leader_replica_id: request.leader_replica_id,
            voter_acks: vec![crate::personaldb_submit::PersonalDbVoterAck {
                replica_id: "projection-writeback".to_string(),
                log_index: source_head.log_index.saturating_add(1),
                log_hash: hex::encode(payload_hash),
                signature: "projection-writeback".to_string(),
            }],
            changeset_payload_hash: hex::encode(payload_hash),
            changeset_bytes: writeback.changeset_bytes,
            client_debug_metadata: request.client_debug_metadata,
        };
        let result_key = projection_writeback_result_key(&target_request);
        if let Some(caller_transaction) = caller_transaction {
            self.commit_personaldb_changeset(
                target_request,
                actor.clone(),
                Some(caller_transaction),
                &[],
            )
            .await?;
            let excluded_projection_ids = vec![format!(
                "{}/{}",
                definition.database_id, definition.projection_id
            )];
            let staged_source = self
                .commit_personaldb_changeset(
                    source_request,
                    actor,
                    Some(caller_transaction),
                    &excluded_projection_ids,
                )
                .await?;
            let response = submit_changeset_response(staged_source, WriteState::Staged);
            self.mvcc
                .open_transactions
                .add_idempotency_result(
                    caller_transaction.0,
                    caller_transaction.1,
                    crate::mvcc_transaction::IdempotencyResult {
                        namespace: PERSONALDB_PROJECTION_WRITEBACK_RESULT_NAMESPACE.to_string(),
                        key: result_key,
                        payload: response.get_ref().encode_to_vec(),
                    },
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .map_err(internal_status)?;
            return Ok(response);
        }
        Err(Status::internal(
            "PersonalDB projection writeback reached commit without a transaction",
        ))
    }

    pub(super) async fn reconstruct_personaldb_submit_retry(
        &self,
        request: &CoreSubmitChangeset,
        commit_version: u64,
    ) -> Result<CommittedPersonalDbChangeset, Status> {
        let protocol_keyring = self.personaldb_protocol_keyring.as_ref();
        let committed_head = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            request.tenant_id,
            &request.database_id,
            protocol_keyring.trust_store(),
            commit_version,
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::internal("Committed PersonalDB retry head is missing"))?;
        let expected_log_index = request
            .base_log_index
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("PersonalDB log index overflow"))?;
        let segment = read_personaldb_log_segment(
            &self.storage,
            &self.mvcc,
            &committed_head.segment_ref,
            commit_version,
        )
        .await
        .map_err(internal_status)?;
        let record = segment
            .records
            .into_iter()
            .find(|record| {
                record.log_index == expected_log_index
                    && hex::encode(record.previous_log_hash) == request.base_log_hash
            })
            .ok_or_else(|| Status::internal("Committed PersonalDB retry record is missing"))?;
        let requested_payload_hash =
            hex32_status(&request.changeset_payload_hash, "changeset payload hash")?;
        if record.changeset_payload_hash != requested_payload_hash {
            return Err(Status::failed_precondition(
                "PersonalDB idempotency key was already used for a different changeset",
            ));
        }
        let certificate_ref = std::str::from_utf8(&record.certificate_ref)
            .map_err(|_| Status::internal("Committed PersonalDB certificate ref is invalid"))?;
        let certificate = read_personaldb_commit_certificate_ref(
            &self.storage,
            &self.mvcc,
            certificate_ref,
            protocol_keyring.trust_store(),
            commit_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::internal("Committed PersonalDB retry certificate is missing"))?;
        Ok(CommittedPersonalDbChangeset {
            log_index: record.log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(record.changeset_payload_hash),
            verified_envelope_hash: hex::encode(record.verified_envelope_hash),
            certificate_hash: hex::encode(record.certificate_hash),
            authz_revision: certificate.authz_revision,
            certificate,
            committed_head,
            watch_cursor: u128::from(commit_version),
        })
    }

    pub(super) async fn commit_personaldb_changeset(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
        caller_transaction: Option<(&str, &str)>,
        excluded_projection_ids: &[String],
    ) -> Result<CommittedPersonalDbChangeset, Status> {
        if caller_transaction.is_none() {
            let idempotency_key = format!(
                "personaldb-submit:{}:{}",
                request.database_id, request.idempotency_key
            );
            let now = u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("PersonalDB timestamp predates Unix epoch"))?;
            let handle = self
                .mvcc
                .open_transactions
                .begin(
                    self.mvcc.runtime.as_ref(),
                    self.mvcc.cluster_id().to_string(),
                    actor.principal.clone(),
                    &idempotency_key,
                    std::time::Duration::from_secs(300),
                    crate::mvcc_transaction::DurabilityLevel::Quorum,
                    crate::mvcc_transaction::ReadConsistency::Linearized,
                    now,
                )
                .await
                .map_err(internal_status)?;
            let status = self
                .mvcc
                .open_transactions
                .status(&handle.transaction_id, &actor.principal, now)
                .map_err(internal_status)?;
            if matches!(status.state, "committed" | "committing") {
                if status.state == "committing" {
                    self.mvcc
                        .open_transactions
                        .commit(
                            self.mvcc.runtime.as_ref(),
                            &handle.transaction_id,
                            &actor.principal,
                            now,
                        )
                        .await
                        .map_err(internal_status)?;
                }
                let commit_version = PersonalDbWritePlan::resolved_commit_version(
                    &self.mvcc,
                    &actor.principal,
                    &idempotency_key,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "committed PersonalDB transaction is missing its commit result",
                    )
                })?;
                return self
                    .reconstruct_personaldb_submit_retry(&request, commit_version)
                    .await;
            }
            if status.state == "aborted" {
                return Err(Status::aborted("PersonalDB transaction previously aborted"));
            }
            Box::pin(self.commit_personaldb_changeset(
                request.clone(),
                actor.clone(),
                Some((&handle.transaction_id, &actor.principal)),
                excluded_projection_ids,
            ))
            .await?;
            let outcome = self
                .mvcc
                .open_transactions
                .commit(
                    self.mvcc.runtime.as_ref(),
                    &handle.transaction_id,
                    &actor.principal,
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .await
                .map_err(internal_status)?;
            let commit_version = match outcome.certification {
                crate::mvcc_transaction::CertificationResult::Committed { commit_version } => {
                    commit_version
                }
                crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
                    return Err(Status::aborted(format!(
                        "PersonalDB transaction aborted: {reason:?}"
                    )));
                }
            };
            return self
                .reconstruct_personaldb_submit_retry(&request, commit_version)
                .await;
        }
        let snapshot_version = match caller_transaction {
            Some((transaction_id, principal)) => {
                self.mvcc
                    .open_transactions
                    .binding(transaction_id, principal)
                    .map_err(internal_status)?;
                let handle = self
                    .mvcc
                    .open_transactions
                    .handle(transaction_id)
                    .map_err(internal_status)?;
                handle.snapshot_version
            }
            None => self
                .mvcc
                .runtime
                .applied_version()
                .map_err(internal_status)?,
        };
        validate_claim_tenant(actor.tenant_id, request.tenant_id)?;
        validate_database_id(&request.database_id)?;
        if actor.require_public_commit_authorization
            && !personaldb_actor_access_allowed(
                &self.storage,
                &self.mvcc,
                &actor,
                &request.database_id,
                AnvilAction::PersonalDbCommit,
            )
            .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }

        let validated = validate_submit_personaldb_changeset(request, default_max_changeset_size())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let deterministic_operation_hash = caller_transaction.map(|(transaction_id, _)| {
            hash32(
                format!(
                    "personaldb-transaction-operation:{transaction_id}:{}:{}",
                    validated.request.database_id, validated.request.idempotency_key
                )
                .as_bytes(),
            )
        });
        let operation_timestamp = deterministic_operation_hash
            .as_ref()
            .map(|hash| {
                let offset = u64::from_be_bytes(hash[..8].try_into().expect("eight hash bytes"))
                    % 3_155_760_000_000_000_000;
                chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(
                    1_577_836_800_000_000_000_i64
                        .saturating_add(i64::try_from(offset).unwrap_or_default()),
                )
            })
            .unwrap_or_else(chrono::Utc::now);
        let operation_timestamp_rfc3339 =
            operation_timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let mutation_id = deterministic_operation_hash
            .map(|hash| hash[..16].try_into().expect("sixteen hash bytes"))
            .unwrap_or_else(|| *uuid::Uuid::new_v4().as_bytes());
        if let Some(bearer_token) = actor.bearer_token.as_deref() {
            bind_personaldb_submit_session(&validated.request, &actor, bearer_token)?;
        }
        let _commit_guard = self
            .personaldb_commit_guard(actor.tenant_id, &validated.request.database_id)
            .await;
        let protocol_keyring = self.personaldb_protocol_keyring.as_ref();
        let manifest = match caller_transaction {
            Some((transaction_id, principal)) => {
                read_personaldb_group_manifest_in_transaction(
                    &self.storage,
                    &self.mvcc,
                    transaction_id,
                    principal,
                    actor.tenant_id,
                    &validated.request.database_id,
                    protocol_keyring.trust_store(),
                )
                .await
            }
            None => {
                read_personaldb_group_manifest(
                    &self.storage,
                    &self.mvcc,
                    actor.tenant_id,
                    &validated.request.database_id,
                    protocol_keyring.trust_store(),
                    snapshot_version,
                )
                .await
            }
        }
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let previous_head = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
            snapshot_version,
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;

        if previous_head.log_index != validated.request.base_log_index
            || previous_head.log_hash != validated.request.base_log_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB base log position does not match committed head",
            ));
        }
        if manifest.active_membership_epoch != validated.request.membership_epoch
            || manifest.active_policy_epoch != validated.request.policy_epoch
            || previous_head.schema_hash != manifest.schema_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB submit epochs or schema do not match the active group",
            ));
        }
        let assignment = self
            .personaldb_write_assignment(actor.tenant_id, &validated.request.database_id)
            .await?;
        let current_head_after_fence = read_personaldb_committed_head_at_snapshot(
            &self.mvcc,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
            snapshot_version,
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;
        if current_head_after_fence.log_index != previous_head.log_index
            || current_head_after_fence.log_hash != previous_head.log_hash
            || current_head_after_fence.head_hash != previous_head.head_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB committed head changed during partition handoff",
            ));
        }
        let changes = iterate_changeset(&validated.request.changeset_bytes)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let schema_sql = read_personaldb_schema_sql(
            &self.storage,
            &self.mvcc,
            actor.tenant_id,
            &validated.request.database_id,
            &manifest.schema_hash,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB schema SQL missing"))?;
        validate_changeset_tables_registered(&changes, &schema_sql)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let authz_revision = authz_journal::authz_revision_at_snapshot(
            &self.mvcc,
            actor.tenant_id,
            snapshot_version,
        )
        .map_err(internal_status)
        .and_then(|revision| {
            u64::try_from(revision).map_err(|_| Status::internal("Invalid authorization revision"))
        })?;
        let proposed_log_index = validated
            .request
            .base_log_index
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("PersonalDB log index overflow"))?;
        let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
            tenant_id: actor.tenant_id,
            database_id: &validated.request.database_id,
            principal: &validated.request.principal,
            base_log_index: validated.request.base_log_index,
            proposed_log_index,
            changeset_payload_hash: validated.changeset_payload_hash,
            schema_hash: &manifest.schema_hash,
            policy_epoch: manifest.active_policy_epoch,
            authz_revision,
            changes: &changes,
            updated_at_nanos: operation_timestamp
                .timestamp_nanos_opt()
                .ok_or_else(|| Status::internal("Invalid current timestamp"))?,
        })
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        authorize_personaldb_row_effects(&self.storage, &self.mvcc, &envelope, &actor).await?;
        let envelope_hash = envelope.envelope_hash32().map_err(internal_status)?;
        let previous_log_hash = hex32_status(&previous_head.log_hash, "committed head log hash")?;
        let schema_hash = hex32_status(&manifest.schema_hash, "schema hash")?;
        let root_generation = snapshot_version
            .checked_add(1)
            .ok_or_else(|| Status::internal("PersonalDB root generation overflow"))?;
        let mut write_plan = PersonalDbWritePlan::new(
            actor.tenant_id,
            &validated.request.database_id,
            caller_transaction
                .map(|(_, principal)| principal)
                .unwrap_or(&actor.principal),
            format!(
                "personaldb-submit:{}:{}",
                validated.request.database_id, validated.request.idempotency_key
            ),
        )
        .map_err(internal_status)?
        .with_assignment_guard(assignment.clone());
        let payload_paths = prepare_and_stage_personaldb_changeset_payload(
            &self.storage,
            &mut write_plan,
            actor.tenant_id,
            &validated.request.database_id,
            proposed_log_index,
            root_generation,
            validated.changeset_payload_hash,
            &validated.request.changeset_bytes,
        )
        .await
        .map_err(internal_status)?;
        let payload_ref = payload_paths.by_index_ref.clone().into_bytes();

        let provisional_record = CorePersonalDbLogRecord::new(
            proposed_log_index,
            validated.request.client_log_epoch,
            validated.request.membership_epoch,
            validated.request.policy_epoch,
            previous_log_hash,
            validated.changeset_payload_hash,
            envelope_hash,
            [0; 32],
            payload_ref.clone(),
            Vec::new(),
            Vec::new(),
        );
        let unsigned_certificate = PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: actor.tenant_id.to_string(),
            database_id: validated.request.database_id.clone(),
            log_index: proposed_log_index,
            previous_log_hash: hex::encode(previous_log_hash),
            entry_hash: hex::encode(provisional_record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            verified_envelope_hash: hex::encode(envelope_hash),
            client_log_epoch: validated.request.client_log_epoch,
            membership_epoch: validated.request.membership_epoch,
            policy_epoch: validated.request.policy_epoch,
            leader_replica_id: validated.request.leader_replica_id.clone(),
            voter_acks_hash: hex::encode(validated.voter_acks_hash),
            authz_revision,
            witness_node_id: actor.principal.clone(),
            witnessed_at: operation_timestamp_rfc3339.clone(),
            certificate_hash: None,
            witness_signature: None,
        };
        let row_index_records = envelope.row_index_upserts().map_err(internal_status)?;
        let row_index_generation = if row_index_records.is_empty() {
            previous_head.row_index_generation
        } else {
            previous_head
                .row_index_generation
                .checked_add(1)
                .ok_or_else(|| Status::failed_precondition("PersonalDB row index overflow"))?
        };
        let certificate = unsigned_certificate
            .seal(protocol_keyring)
            .await
            .map_err(internal_status)?;
        let certificate_ref = prepare_and_stage_personaldb_commit_certificate(
            &self.storage,
            &mut write_plan,
            actor.tenant_id,
            &validated.request.database_id,
            root_generation,
            &certificate,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;
        let certificate_hash = hex32_status(
            certificate
                .certificate_hash
                .as_deref()
                .ok_or_else(|| Status::internal("PersonalDB certificate hash missing"))?,
            "certificate hash",
        )?;
        let record = CorePersonalDbLogRecord::new(
            proposed_log_index,
            validated.request.client_log_epoch,
            validated.request.membership_epoch,
            validated.request.policy_epoch,
            previous_log_hash,
            validated.changeset_payload_hash,
            envelope_hash,
            certificate_hash,
            payload_ref,
            certificate_ref.into_bytes(),
            Vec::new(),
        );
        let segment_ref = prepare_and_stage_personaldb_log_segment(
            &self.storage,
            &mut write_plan,
            root_generation,
            PersonalDbLogSegmentWrite {
                tenant_id: actor.tenant_id,
                database_id: &validated.request.database_id,
                schema_hash,
                source_fence_token: assignment.assignment_epoch,
                records: std::slice::from_ref(&record),
            },
        )
        .await
        .map_err(internal_status)?;
        let committed_head = PersonalDbCommittedHead {
            format_version: 2,
            tenant_id: actor.tenant_id.to_string(),
            database_id: validated.request.database_id.clone(),
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            segment_ref,
            row_index_generation,
            policy_epoch: manifest.active_policy_epoch,
            membership_epoch: manifest.active_membership_epoch,
            schema_hash: manifest.schema_hash.clone(),
            updated_at: operation_timestamp_rfc3339.clone(),
            updated_by_node: actor.principal.clone(),
            head_hash: None,
            head_signature: None,
        }
        .seal(protocol_keyring)
        .await
        .map_err(internal_status)?;
        if !row_index_records.is_empty() {
            prepare_and_stage_personaldb_row_index(
                &self.storage,
                &mut write_plan,
                root_generation,
                PersonalDbRowIndexWrite {
                    tenant_id: actor.tenant_id,
                    database_id: &validated.request.database_id,
                    generation: row_index_generation,
                    source_hash: record.entry_hash,
                    records: &row_index_records,
                },
            )
            .await
            .map_err(internal_status)?;
        }
        prepare_and_stage_personaldb_committed_head(
            &self.storage,
            &mut write_plan,
            actor.tenant_id,
            &validated.request.database_id,
            root_generation,
            &committed_head,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;
        stage_personaldb_committed_head_mvcc(
            &mut write_plan,
            &self.mvcc,
            actor.tenant_id,
            &validated.request.database_id,
            &previous_head,
            &committed_head,
            protocol_keyring.trust_store(),
        )
        .map_err(internal_status)?;

        let watch_payload = PersonalDbGroupWatchPayload {
            database_id: validated.request.database_id.clone(),
            event_type: "commit".to_string(),
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            certificate_hash: hex::encode(certificate_hash),
            committed_head_hash: committed_head.head_hash.clone().unwrap_or_default(),
            emitted_at: operation_timestamp_rfc3339,
        };
        stage_personaldb_group_watch_record(
            &mut write_plan,
            actor.tenant_id,
            &validated.request.database_id,
            mutation_id,
            authz_revision,
            watch_payload,
        )
        .map_err(internal_status)?;
        if let Some((transaction_id, transaction_principal)) = caller_transaction {
            stage_personaldb_row_owner_grants(
                &self.persistence,
                &envelope,
                &actor,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(internal_status)?;
            let job = crate::personaldb_postcommit_job::PersonalDbPostCommitJob {
                schema: crate::personaldb_postcommit_job::PersonalDbPostCommitJob::SCHEMA.into(),
                cluster_id: self.mvcc.cluster_id().to_string(),
                transaction_id: transaction_id.to_string(),
                tenant_id: actor.tenant_id,
                database_id: validated.request.database_id.clone(),
                principal: actor.principal.clone(),
                log_index: proposed_log_index,
                log_hash: committed_head.log_hash.clone(),
                authz_revision,
                schema_sql,
                changeset_bytes: validated.request.changeset_bytes.clone(),
                envelope: serde_json::to_value(&envelope).map_err(internal_status)?,
                committed_head_hash: committed_head.head_hash.clone().unwrap_or_default(),
                excluded_projection_ids: excluded_projection_ids.to_vec(),
            };
            write_plan
                .stage_into_transaction(
                    &self.mvcc,
                    transaction_id,
                    transaction_principal,
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .await
                .map_err(internal_status)?;
            self.mvcc
                .open_transactions
                .add_job(
                    transaction_id,
                    job.encode().map_err(internal_status)?,
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .map_err(internal_status)?;
            return Ok(CommittedPersonalDbChangeset {
                log_index: proposed_log_index,
                log_hash: hex::encode(record.entry_hash),
                changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
                verified_envelope_hash: hex::encode(envelope_hash),
                certificate_hash: hex::encode(certificate_hash),
                certificate,
                committed_head,
                watch_cursor: 0,
                authz_revision,
            });
        }
        Err(Status::internal(
            "PersonalDB changeset reached commit without a caller or internal transaction",
        ))
    }

    pub(super) async fn build_personaldb_projections_for_source_commit(
        &self,
        tenant_id: i64,
        source_database_id: &str,
        source_changeset_bytes: &[u8],
        source_log_index: u64,
        source_log_hash: &str,
        authz_revision: u64,
        excluded_projection_ids: &[String],
    ) -> Result<(), Status> {
        let snapshot_version = self
            .mvcc
            .runtime
            .applied_version()
            .map_err(internal_status)?;
        let source_manifest = read_personaldb_group_manifest(
            &self.storage,
            &self.mvcc,
            tenant_id,
            source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB source group not found"))?;
        let source_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            &self.mvcc,
            tenant_id,
            source_database_id,
            &source_manifest.schema_hash,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source schema SQL missing"))?;
        let definitions = list_projection_definitions_for_source(
            &self.storage,
            &self.mvcc,
            tenant_id,
            source_database_id,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?;
        for definition in definitions {
            if excluded_projection_ids
                .binary_search(&format!(
                    "{}/{}",
                    definition.database_id, definition.projection_id
                ))
                .is_ok()
            {
                continue;
            }
            self.build_one_personaldb_projection(
                tenant_id,
                source_database_id,
                &source_schema_sql,
                source_changeset_bytes,
                source_log_index,
                source_log_hash,
                authz_revision,
                &definition,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn build_one_personaldb_projection(
        &self,
        tenant_id: i64,
        source_database_id: &str,
        source_schema_sql: &str,
        source_changeset_bytes: &[u8],
        source_log_index: u64,
        source_log_hash: &str,
        authz_revision: u64,
        definition: &ProjectionDefinition,
    ) -> Result<(), Status> {
        let snapshot_version = self
            .mvcc
            .runtime
            .applied_version()
            .map_err(internal_status)?;
        if definition.target_database_id != definition.database_id {
            return Err(Status::failed_precondition(
                "PersonalDB projection target database scope mismatch",
            ));
        }
        let target_manifest = read_personaldb_group_manifest(
            &self.storage,
            &self.mvcc,
            tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        let target_head = read_personaldb_committed_head_mvcc(
            &self.mvcc,
            tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection head missing"))?;
        let target_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            &self.mvcc,
            tenant_id,
            &definition.database_id,
            &target_manifest.schema_hash,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection schema SQL missing"))?;
        let build_input = ProjectionBuildInput {
            source_database_id,
            source_schema_sql,
            target_schema_sql: &target_schema_sql,
            definition,
            source_changeset_bytes,
        };
        let authorization_checks =
            collect_projection_authorization_checks(build_input).map_err(internal_status)?;
        let authorization = self
            .evaluate_projection_authorization_checks(
                tenant_id,
                &definition.target_actor_or_scope,
                authorization_checks,
                authz_revision,
            )
            .await?;
        let Some(projection_changeset) =
            build_projection_changeset_with_authorization(build_input, &authorization)
                .map_err(internal_status)?
        else {
            return Ok(());
        };
        if projection_changeset.changeset_bytes.is_empty() {
            return Ok(());
        }
        let internal_actor = "anvil-projection-builder".to_string();
        let payload_hash = hash32(&projection_changeset.changeset_bytes);
        let projection_request = CoreSubmitChangeset {
            tenant_id,
            database_id: definition.database_id.clone(),
            principal: internal_actor.clone(),
            session_token: "internal-projection-builder".to_string(),
            request_id: format!(
                "projection:{}:{}:{}",
                source_database_id, source_log_index, definition.projection_id
            ),
            idempotency_key: format!(
                "projection:{}:{}:{}",
                source_database_id, source_log_hash, definition.projection_id
            ),
            base_log_index: target_head.log_index,
            base_log_hash: target_head.log_hash,
            client_log_epoch: target_head.log_index.saturating_add(1),
            membership_epoch: target_manifest.active_membership_epoch,
            policy_epoch: target_manifest.active_policy_epoch,
            leader_replica_id: internal_actor.clone(),
            voter_acks: vec![crate::personaldb_submit::PersonalDbVoterAck {
                replica_id: internal_actor.clone(),
                log_index: target_head.log_index.saturating_add(1),
                log_hash: hex::encode(payload_hash),
                signature: "internal-projection-builder".to_string(),
            }],
            changeset_payload_hash: hex::encode(payload_hash),
            changeset_bytes: projection_changeset.changeset_bytes,
            client_debug_metadata: None,
        };
        let projection_idempotency_key = format!(
            "personaldb-submit:{}:{}",
            projection_request.database_id, projection_request.idempotency_key
        );
        let projection_commit = if let Some(commit_version) =
            PersonalDbWritePlan::resolved_commit_version(
                &self.mvcc,
                &internal_actor,
                &projection_idempotency_key,
            )
            .await
            .map_err(internal_status)?
        {
            self.reconstruct_personaldb_submit_retry(&projection_request, commit_version)
                .await?
        } else {
            self.commit_personaldb_changeset(
                projection_request,
                PersonalDbCommitActor {
                    tenant_id,
                    principal: internal_actor.clone(),
                    bearer_token: None,
                    require_public_commit_authorization: false,
                },
                None,
                &[],
            )
            .await?
        };
        let payload = PersonalDbProjectionWatchPayload {
            database_id: definition.database_id.clone(),
            projection_id: definition.projection_id.clone(),
            event_type: "projection_committed".to_string(),
            source_database_id: source_database_id.to_string(),
            source_log_index,
            source_log_hash: source_log_hash.to_string(),
            projection_log_index: projection_commit.log_index,
            projection_log_hash: projection_commit.log_hash.clone(),
            definition_hash: definition.definition_hash.clone().unwrap_or_default(),
            emitted_at: now_rfc3339(),
        };
        let mutation_hash = hash32(
            format!(
                "projection-watch:{tenant_id}:{source_database_id}:{source_log_hash}:{}",
                definition.projection_id
            )
            .as_bytes(),
        );
        let mut mutation_id = [0_u8; 16];
        mutation_id.copy_from_slice(&mutation_hash[..16]);
        let cursor = append_personaldb_projection_watch_record(
            &self.mvcc,
            tenant_id,
            &definition.database_id,
            &definition.projection_id,
            mutation_id,
            authz_revision,
            payload.clone(),
        )
        .await
        .map_err(internal_status)?;
        Ok(())
    }

    pub(super) async fn evaluate_projection_authorization_checks(
        &self,
        tenant_id: i64,
        target_actor: &str,
        checks: std::collections::BTreeSet<ProjectionAuthorizationCheck>,
        authz_revision: u64,
    ) -> Result<ProjectionAuthorizationDecisions, Status> {
        let revision = i64::try_from(authz_revision)
            .map_err(|_| Status::internal("Invalid projection authorization revision"))?;
        let mut allowed = Vec::new();
        for check in checks {
            let scoped_namespace = encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, &check.namespace);
            let is_allowed = authz_journal::resolve_permission_at_revision(
                &self.storage,
                &self.mvcc,
                tenant_id,
                &scoped_namespace,
                &check.object_id,
                &check.relation,
                access_control::APP_SUBJECT_KIND,
                target_actor,
                "",
                revision,
            )
            .await
            .map_err(internal_status)?;
            if is_allowed {
                allowed.push(check);
            }
        }
        Ok(ProjectionAuthorizationDecisions::new(allowed))
    }
}
