use super::*;
use keldra_store::BuiltInObjectTransactionPlan;

impl ProgramCoordinator {
    /// Resolve a bounded set of built-in replay identities under one executor
    /// fence and one recovery pass. Results preserve request order; a miss is
    /// never treated as authority to bypass normal live-state validation.
    pub(crate) async fn replay_builtin_object_transactions(
        &self,
        lookups: &[BuiltInReplayLookup],
    ) -> Result<Vec<Result<Option<InvokedProgramResult>, Status>>, Status> {
        if lookups.len() > keldra_store::MAX_ATOMIC_BATCH_MUTATIONS {
            return Err(Status::resource_exhausted(
                "built-in replay batch exceeds the atomic mutation bound",
            ));
        }
        self.require_generalized_atomic_paths()?;
        let nomination = self.current_nomination()?;
        if nomination.executor != self.node {
            return Err(Status::failed_precondition(
                "built-in replay must execute on the nominated executor",
            ));
        }
        let clustered = self.is_clustered()?;
        let _gate = self.commit_gate.lock().await;
        if clustered {
            self.recover_distributed_tail_locked().await?;
        } else {
            self.recover_committed_tail_locked()
                .await
                .map_err(internal)?;
        }
        if self.current_nomination()? != nomination {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: atomic executor changed during built-in replay",
            ));
        }
        let replay_clock = current_unix_millis().map_err(internal)?;
        let state = self.decisions.state().map_err(decision_status)?;
        let mut committed = Vec::with_capacity(lookups.len());
        for lookup in lookups {
            let Some(entry) = state.replay_entry(InvocationId(lookup.invocation_id), replay_clock)
            else {
                committed.push(None);
                continue;
            };
            if entry.input_fingerprint.0 != lookup.input_fingerprint
                || entry.committed_batch.authority
                    != (AtomicBundleAuthority::BuiltInObjectTransaction {
                        kind: lookup.authority_kind,
                        contract_version: lookup.contract_version,
                    })
            {
                committed.push(Some(Err(Status::already_exists(
                    "IDEMPOTENCY_INPUT_MISMATCH",
                ))));
                continue;
            }
            committed.push(Some(Ok(entry)));
        }
        drop(state);
        let mut results = Vec::with_capacity(committed.len());
        for entry in committed {
            results.push(match entry {
                Some(Ok(entry)) if clustered => {
                    self.load_distributed_result(entry, true).await.map(Some)
                }
                Some(Ok(entry)) => self
                    .load_committed_invocation_result(entry, true)
                    .await
                    .map(Some),
                Some(Err(error)) => Err(error),
                None => Ok(None),
            });
        }
        Ok(results)
    }

    /// Resolve an already-committed built-in command before a caller rebuilds
    /// its exact participant plan. This is required for destructive built-ins:
    /// after a successful transaction, the state used to reconstruct the plan
    /// may intentionally no longer exist. A miss is not authority to weaken
    /// validation of a new plan.
    pub(crate) async fn replay_builtin_object_transaction(
        &self,
        authority_kind: u16,
        contract_version: u16,
        invocation_id: [u8; 32],
        input_fingerprint: [u8; 32],
    ) -> Result<Option<InvokedProgramResult>, Status> {
        self.require_generalized_atomic_paths()?;
        let nomination = self.current_nomination()?;
        if nomination.executor != self.node {
            return Err(Status::failed_precondition(
                "built-in replay must execute on the nominated executor",
            ));
        }
        let clustered = self.is_clustered()?;
        let _gate = self.commit_gate.lock().await;
        if clustered {
            self.recover_distributed_tail_locked().await?;
        } else {
            self.recover_committed_tail_locked()
                .await
                .map_err(internal)?;
        }
        if self.current_nomination()? != nomination {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: atomic executor changed during built-in replay",
            ));
        }
        let replay_clock = current_unix_millis().map_err(internal)?;
        let invocation_id = InvocationId(invocation_id);
        let Some(committed) = self
            .decisions
            .state()
            .map_err(decision_status)?
            .replay_entry(invocation_id, replay_clock)
        else {
            return Ok(None);
        };
        if committed.input_fingerprint.0 != input_fingerprint
            || committed.committed_batch.authority
                != (AtomicBundleAuthority::BuiltInObjectTransaction {
                    kind: authority_kind,
                    contract_version,
                })
        {
            return Err(Status::already_exists("IDEMPOTENCY_INPUT_MISMATCH"));
        }
        if clustered {
            self.load_distributed_result(committed, true)
                .await
                .map(Some)
        } else {
            self.load_committed_invocation_result(committed, true)
                .await
                .map(Some)
        }
    }

    /// Execute one already-authorized sealed internal transaction through the
    /// same durable Begin/reservation/commit/finalization authority as stored
    /// programs. The caller remains responsible for public namespace auth;
    /// the manifest supplies exact storage/governance conditions.
    pub(crate) async fn invoke_builtin_object_transaction(
        &self,
        plan: BuiltInObjectTransactionPlan,
        invocation_id: [u8; 32],
        input_fingerprint: [u8; 32],
        durability_class: &str,
        budget: Duration,
    ) -> Result<InvokedProgramResult, Status> {
        self.require_generalized_atomic_paths()?;
        let nomination = self.current_nomination()?;
        let clustered = self.is_clustered()?;
        if !clustered
            && plan.writes.iter().any(|write| {
                matches!(
                    write.payload,
                    keldra_store::BuiltInWritePayload::StagedReference {
                        upload_source_node_id,
                        ..
                    } if upload_source_node_id != self.node.0
                )
            })
        {
            return Err(Status::failed_precondition(
                "a one-node built-in staged reference must originate on the local node",
            ));
        }
        let (prepared, record) = if clustered {
            self.distributed()?
                .prepare_builtin(&plan, durability_class)
                .await?
        } else {
            let prepared = self
                .store
                .prepare_builtin_object_transaction(&plan)
                .await
                .map_err(program_store_status)?;
            let record = self
                .store
                .prepared_program_record(&prepared)
                .await
                .map_err(program_store_status)?;
            (prepared, record)
        };
        let evidence_hash = accepted_program_evidence_hash(
            &prepared.durability.scope,
            prepared.durability_evidence_hash,
            durability_class,
            self.node,
        )?;
        let _gate = self.commit_gate.lock().await;
        if clustered {
            self.recover_distributed_tail_locked().await?;
        } else {
            self.recover_committed_tail_locked()
                .await
                .map_err(internal)?;
        }
        let current = self.current_nomination()?;
        if current != nomination {
            return Err(Status::unavailable(
                "EXECUTOR_MOVED: atomic executor changed during built-in preparation",
            ));
        }
        let mutation_context = if clustered {
            self.distributed()?.mutation_context()?
        } else {
            self.one_node_mutation_context(nomination)?
        };
        let replay_clock = current_unix_millis().map_err(internal)?;
        let invocation_id = InvocationId(invocation_id);
        if let Some(committed) = self
            .decisions
            .state()
            .map_err(decision_status)?
            .replay_entry(invocation_id, replay_clock)
        {
            if committed.input_fingerprint.0 != input_fingerprint
                || store_bundle_authority(committed.committed_batch.authority) != prepared.authority
            {
                return Err(Status::already_exists("IDEMPOTENCY_INPUT_MISMATCH"));
            }
            return if clustered {
                self.load_distributed_result(committed, true).await
            } else {
                self.load_committed_invocation_result(committed, true).await
            };
        }
        let replay_expires_at_unix_millis = replay_clock
            .checked_add(ATOMIC_REPLAY_RETENTION_MILLIS)
            .ok_or_else(|| Status::internal("atomic replay expiry overflow"))?;
        let begun = self
            .decisions
            .submit(Command::BeginBatch(BeginBatch {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                authority: decision_bundle_authority(prepared.authority),
                invocation_id,
                input_fingerprint: InvocationFingerprint(input_fingerprint),
                bundle_ref: BundleRef {
                    hash: prepared.bundle.hash,
                    length: prepared.bundle.length,
                },
                bundle_hash: BundleHash(prepared.hash.0),
                durability_class: DurabilityClass(
                    ProgramDurabilityClassHash::for_class(durability_class).0,
                ),
                durability_evidence_hash: DurabilityEvidenceHash(evidence_hash.0),
                participant_manifest_hash: ParticipantManifestHash(
                    prepared.participant_manifest_hash,
                ),
                proposal_at_unix_millis: replay_clock,
                replay_expires_at_unix_millis,
            }))
            .await
            .map_err(decision_status)?;
        let prepared_batch = match expect_batch_begun(begun.result)? {
            keldra_consensus::BeginResult::AlreadyCommitted(committed) => {
                return if clustered {
                    self.load_distributed_result(committed.invocation, true)
                        .await
                } else {
                    self.load_committed_invocation_result(committed.invocation, true)
                        .await
                };
            }
            keldra_consensus::BeginResult::Prepared { batch, .. } => batch,
        };
        let reservations = record
            .reservations(
                prepared_batch.begin_cursor,
                invocation_id.0,
                prepared.hash,
                self.node.0,
                nomination.nomination_log_index,
                mutation_context.active_placement_log_id,
            )
            .map_err(program_store_status)?;
        let stages = if clustered {
            match self
                .distributed()?
                .stage_prepared(
                    &prepared,
                    &record,
                    prepared_batch.begin_cursor,
                    nomination,
                    budget,
                )
                .await
            {
                Ok(stages) => Some(stages),
                Err(error) => {
                    // Once this call returns a definitive failure, recovery
                    // must never later commit the same durable Begin. Orphaned
                    // immutable stage blobs remain ordinary GC candidates.
                    self.abort_prepared_batch(prepared_batch).await?;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let reserve = if clustered {
            self.distributed()?
                .reserve_participants(&reservations, nomination, budget)
                .await
        } else {
            self.reserve_local_participants(&reservations).await
        };
        if let Err(error) = reserve {
            self.abort_prepared_batch(prepared_batch).await?;
            if clustered {
                self.distributed()?
                    .release_participants(&reservations, None, nomination, budget)
                    .await?;
            } else {
                self.release_local_participants(&reservations, None).await?;
            }
            return Err(error);
        }
        let committed = self
            .decisions
            .submit(Command::CommitPreparedBatch(CommitPreparedBatch {
                executor: self.node,
                nomination_log_index: nomination.nomination_log_index,
                begin_cursor: prepared_batch.begin_cursor,
                invocation_id,
                participant_manifest_hash: ParticipantManifestHash(
                    prepared.participant_manifest_hash,
                ),
            }))
            .await
            .map_err(decision_status)?;
        let committed = expect_batch_committed(committed.result)?;
        let commit_cursor = committed.invocation.committed_batch.commit_cursor;
        if clustered {
            let distributed = self.distributed()?;
            distributed
                .commit_participants(&reservations, commit_cursor, nomination, budget)
                .await?;
            let stages = stages.expect("clustered preparation has path stages");
            let finalized = distributed
                .finalize(&stages, commit_cursor, nomination, budget)
                .await?;
            self.store
                .publish_atomic_batch(
                    keldra_store::SealedAtomicBatchPublication::from_prepared(
                        commit_cursor,
                        prepared.bundle,
                        prepared.hash,
                        &record,
                        &stages.paths,
                        &finalized.paths,
                        &finalized.alias_registries,
                    )
                    .map_err(program_store_status)?,
                )
                .await
                .map_err(program_store_status)?;
        } else {
            self.commit_local_participants(&reservations, commit_cursor)
                .await?;
            self.store
                .recover_program_bundle(
                    program_commit(
                        self.store
                            .applied_program_commit_cursor()
                            .map_err(program_store_status)?,
                        committed.invocation.committed_batch,
                    ),
                    mutation_context,
                )
                .await
                .map_err(program_store_status)?;
        }
        self.advance_finalized_through(nomination, commit_cursor)
            .await?;
        if clustered {
            self.distributed()?
                .release_participants(&reservations, Some(commit_cursor), nomination, budget)
                .await?;
            Ok(distributed::result_from_record(
                &record,
                committed.invocation,
                false,
            ))
        } else {
            self.release_local_participants(&reservations, Some(commit_cursor))
                .await?;
            self.load_committed_invocation_result(committed.invocation, false)
                .await
        }
    }
}

pub(crate) fn builtin_invocation_identity(kind: u16, command_id: &str) -> [u8; 32] {
    tagged_hash(
        b"keldra.builtin-object-invocation.v1",
        &[&kind.to_be_bytes(), command_id.as_bytes()],
    )
}
