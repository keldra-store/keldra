use super::*;

impl ProgramCoordinator {
    pub(super) fn spawn_recovery_worker(&self) {
        let coordinator = self.clone();
        tokio::spawn(async move {
            let mut next_sweep = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if Instant::now() >= next_sweep {
                    if let Err(error) = coordinator.sweep_stale_local_reservations().await {
                        tracing::warn!(%error, "stale atomic reservation sweep will retry");
                    }
                    next_sweep = Instant::now() + Duration::from_secs(1);
                }
                let Ok(state) = coordinator.decisions.state() else {
                    continue;
                };
                if state
                    .executor()
                    .is_none_or(|nomination| nomination.executor != coordinator.node)
                    || (state.preparing_batch().is_none() && state.unfinalized_commit_len() == 0)
                {
                    continue;
                }
                drop(state);
                let Ok(_guard) = coordinator.commit_gate.try_lock() else {
                    continue;
                };
                let result = if coordinator.distributed.get().is_some() {
                    coordinator
                        .recover_distributed_tail_locked()
                        .await
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                } else {
                    coordinator.recover_committed_tail_locked().await
                };
                if let Err(error) = result {
                    tracing::warn!(%error, "durable atomic recovery worker will retry");
                }
            }
        });
    }

    pub(super) async fn sweep_stale_local_reservations(&self) -> Result<()> {
        let state = self
            .decisions
            .state()
            .context("read reservation authority")?;
        let preparing_begin = state.preparing_batch().map(|batch| batch.begin_cursor);
        let active_begins = state
            .unfinalized_invocations()
            .map(|invocation| invocation.committed_batch.begin_cursor)
            .collect::<std::collections::BTreeSet<_>>();
        drop(state);
        for reservation in self
            .store
            .program_reservations()
            .context("enumerate durable atomic reservations")?
        {
            if preparing_begin == Some(reservation.begin_cursor())
                || active_begins.contains(&reservation.begin_cursor())
            {
                continue;
            }
            let finalized_commit_cursor = match &reservation {
                ProgramReservation::Object(value) => match value.state {
                    keldra_store::ProgramReservationState::Prepared => None,
                    keldra_store::ProgramReservationState::Committed { commit_cursor } => {
                        Some(commit_cursor)
                    }
                },
                ProgramReservation::Governance(value) => match value.state {
                    keldra_store::ProgramReservationState::Prepared => None,
                    keldra_store::ProgramReservationState::Committed { commit_cursor } => {
                        Some(commit_cursor)
                    }
                },
            };
            self.store
                .release_program_participant(&reservation, finalized_commit_cursor)
                .await
                .context("clear stale durable atomic reservation")?;
        }
        Ok(())
    }

    pub(super) fn require_generalized_atomic_paths(&self) -> Result<(), Status> {
        let state = self.decisions.state().map_err(decision_status)?;
        if crate::cluster_capabilities::generalized_atomic_paths_active(&state) {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "generalized atomic path reservations are not active for this cluster",
            ))
        }
    }

    pub(super) async fn reserve_local_participants(
        &self,
        reservations: &[ProgramReservation],
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.store
                .reserve_program_participant(reservation)
                .await
                .map_err(mutation_status)?;
        }
        Ok(())
    }

    pub(super) async fn commit_local_participants(
        &self,
        reservations: &[ProgramReservation],
        commit_cursor: u64,
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.store
                .commit_program_participant(reservation, commit_cursor)
                .await
                .map_err(mutation_status)?;
        }
        Ok(())
    }

    pub(super) async fn release_local_participants(
        &self,
        reservations: &[ProgramReservation],
        finalized_commit_cursor: Option<u64>,
    ) -> Result<(), Status> {
        for reservation in reservations {
            self.store
                .release_program_participant(reservation, finalized_commit_cursor)
                .await
                .map_err(mutation_status)?;
        }
        Ok(())
    }

    pub(super) fn current_nomination(&self) -> Result<ExecutorNomination, Status> {
        let nomination = self
            .decisions
            .state()
            .map_err(decision_status)?
            .executor()
            .ok_or_else(|| {
                Status::unavailable("EXECUTOR_MOVED: no atomic executor is nominated")
            })?;
        if nomination.executor != self.node {
            return Err(Status::unavailable(format!(
                "EXECUTOR_MOVED: atomic request belongs on nominated executor node {}",
                nomination.executor.0
            )));
        }
        Ok(nomination)
    }

    pub(super) fn one_node_mutation_context(
        &self,
        nomination: ExecutorNomination,
    ) -> Result<ObjectMutationContext, Status> {
        let state = self.decisions.state().map_err(decision_status)?;
        let placement = state
            .cluster_control()
            .active_placement_log_id()
            .ok_or_else(|| Status::unavailable("ACTIVE placement has no committed log identity"))?;
        Ok(ObjectMutationContext {
            active_placement_log_id: PlacementLogId {
                term: placement.leader_id.term,
                index: placement.index,
            },
            serving_fence_term: nomination.nomination_log_index,
        })
    }

    pub(super) async fn recover_committed_tail(&self) -> Result<()> {
        let _guard = self.commit_gate.lock().await;
        self.recover_committed_tail_locked().await
    }

    pub(super) async fn recover_committed_tail_locked(&self) -> Result<()> {
        let nomination = self
            .current_nomination()
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let mutation_context = self
            .one_node_mutation_context(nomination)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let state = self.decisions.state().context("read decision state")?;
        let preparing = state.preparing_batch();
        drop(state);
        if let Some(batch) = preparing {
            let request = batch.request;
            let prepared = self
                .store
                .prepared_program_bundle(
                    PreparedBundleRef {
                        hash: request.bundle_ref.hash,
                        length: request.bundle_ref.length,
                    },
                    PreparedBundleHash(request.bundle_hash.0),
                    ProgramDurabilityEvidenceHash(request.durability_evidence_hash.0),
                )
                .await
                .context("load durably begun atomic bundle")?
                .ok_or_else(|| anyhow::anyhow!("durably begun atomic bundle is unavailable"))?;
            if decision_bundle_authority(prepared.authority) != request.authority
                || prepared.participant_manifest_hash != request.participant_manifest_hash.0
            {
                bail!("durably begun atomic bundle authority or participant manifest changed");
            }
            let record = self
                .store
                .prepared_program_record(&prepared)
                .await
                .context("load durably begun atomic participant manifest")?;
            let reservations = record
                .reservations(
                    batch.begin_cursor,
                    request.invocation_id.0,
                    prepared.hash,
                    nomination.executor.0,
                    nomination.nomination_log_index,
                    mutation_context.active_placement_log_id,
                )
                .context("reconstruct durable atomic reservations")?;
            if let Err(error) = self.reserve_local_participants(&reservations).await {
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument
                        | tonic::Code::FailedPrecondition
                        | tonic::Code::DataLoss
                ) {
                    self.abort_prepared_batch(batch)
                        .await
                        .map_err(|abort| anyhow::anyhow!(abort.to_string()))?;
                    self.release_local_participants(&reservations, None)
                        .await
                        .map_err(|release| anyhow::anyhow!(release.to_string()))?;
                }
                return Err(anyhow::anyhow!(error.to_string()));
            }
            let committed = self
                .decisions
                .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
                    executor: nomination.executor,
                    nomination_log_index: nomination.nomination_log_index,
                    begin_cursor: batch.begin_cursor,
                    invocation_id: request.invocation_id,
                    participant_manifest_hash: request.participant_manifest_hash,
                }))
                .await
                .context("commit durably prepared atomic batch")?;
            let committed = expect_batch_committed(committed.result)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            self.commit_local_participants(
                &reservations,
                committed.invocation.committed_batch.commit_cursor,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        let state = self.decisions.state().context("read decision state")?;
        let mut applied = self
            .store
            .applied_program_commit_cursor()
            .context("read applied atomic commit cursor")?;
        if let Some(finalized) = state.finalized_through()
            && applied.is_none_or(|cursor| cursor < finalized)
        {
            bail!(
                "local atomic view is behind finalized cursor {finalized}; compacted recovery data is unavailable"
            );
        }
        if let (Some(applied), Some(last)) = (applied, state.last_commit_cursor())
            && applied > last
        {
            bail!("local atomic cursor {applied} is ahead of consensus cursor {last}");
        }

        let mut finalized_through = None;
        let mut completed_reservations = Vec::new();
        for invocation in state.unfinalized_invocations() {
            tracing::info!(
                monotonic_counter.keldra_atomic_program_finalization_retries_total = 1_u64,
                "retry atomic program finalization"
            );
            let batch = invocation.committed_batch;
            let prepared = self
                .store
                .prepared_program_bundle(
                    PreparedBundleRef {
                        hash: batch.bundle_ref.hash,
                        length: batch.bundle_ref.length,
                    },
                    PreparedBundleHash(batch.bundle_hash.0),
                    ProgramDurabilityEvidenceHash(batch.durability_evidence_hash.0),
                )
                .await
                .context("load committed atomic bundle for reservation recovery")?
                .ok_or_else(|| anyhow::anyhow!("committed atomic bundle is unavailable"))?;
            let record = self
                .store
                .prepared_program_record(&prepared)
                .await
                .context("load committed atomic participant manifest")?;
            let reservations = record
                .reservations(
                    batch.begin_cursor,
                    invocation.invocation_id.0,
                    prepared.hash,
                    nomination.executor.0,
                    nomination.nomination_log_index,
                    mutation_context.active_placement_log_id,
                )
                .context("reconstruct committed atomic reservations")?;
            self.reserve_local_participants(&reservations)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            self.commit_local_participants(&reservations, batch.commit_cursor)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            if applied.is_some_and(|cursor| cursor >= batch.commit_cursor) {
                finalized_through = Some(batch.commit_cursor);
                completed_reservations.push((reservations, batch.commit_cursor));
                continue;
            }
            let result = self
                .store
                .recover_program_bundle(program_commit(applied, batch), mutation_context)
                .await
                .with_context(|| format!("finalize atomic commit {}", batch.commit_cursor))?;
            require_result_matches_consensus(&result, invocation)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            applied = Some(batch.commit_cursor);
            finalized_through = Some(batch.commit_cursor);
            completed_reservations.push((reservations, batch.commit_cursor));
        }
        if let Some(through_commit_cursor) = finalized_through {
            self.advance_finalized_through(nomination, through_commit_cursor)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        for (reservations, commit_cursor) in completed_reservations {
            self.release_local_participants(&reservations, Some(commit_cursor))
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        Ok(())
    }

    pub(super) async fn advance_finalized_through(
        &self,
        nomination: ExecutorNomination,
        through_commit_cursor: u64,
    ) -> Result<(), Status> {
        let result = self
            .decisions
            .submit(Command::FinalizedThrough {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                through_commit_cursor,
            })
            .await;
        self.emit_bounded_state_metrics();
        let result = result.map_err(finalization_decision_status)?;
        expect_finalization(result.result, through_commit_cursor)
    }

    pub(super) async fn abort_prepared_batch(
        &self,
        prepared: keldra_consensus::PreparedBatch,
    ) -> Result<(), Status> {
        let result = self
            .decisions
            .submit(Command::AbortPreparedBatch(
                keldra_consensus::AbortPreparedBatch {
                    executor: prepared.request.executor,
                    nomination_log_index: prepared.request.nomination_log_index,
                    begin_cursor: prepared.begin_cursor,
                    invocation_id: prepared.request.invocation_id,
                    participant_manifest_hash: prepared.request.participant_manifest_hash,
                },
            ))
            .await
            .map_err(decision_status)?;
        match result.result {
            ApplyResult::BatchAborted { begin_cursor } if begin_cursor == prepared.begin_cursor => {
                Ok(())
            }
            other => Err(Status::internal(format!(
                "unexpected atomic abort response: {other:?}"
            ))),
        }
    }
}
